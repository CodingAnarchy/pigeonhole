//! The scaling gate's driver (D204): Pigeonhole in application-owned mode, each shard
//! driven by a thread of its own that also issues the writes to the rows its shard owns,
//! keeping a fixed number of commits in flight (`commit_async`) between turns of its shard.
//! No client thread competes with the shards for cores: the spec's thread-per-core
//! embedding.

use std::collections::VecDeque;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use pigeonhole::nonblocking::CommitFuture;
use pigeonhole::{Pigeonhole, Shard, Table};

use super::pigeonhole::{create_table, err, options};
use crate::{
    BenchOp, EngineCommit, Histogram, PigeonholeRunner, RunDetail, RunOptions, RunRecord, Runner,
    ShardShare, Stalls, Workload, WorkloadConfig,
};

/// Commits each shard thread keeps in flight (D204).
pub const INLINE_IN_FLIGHT: usize = 16;

/// Longest the warmup runs while waiting for the balancer to spread the table (D204).
const SPREAD_CAP: Duration = Duration::from_secs(30);

/// How long the table must go without a split, merge or move before the warmup ends: longer
/// than the balancer's dwell (10 passes of 100 ms, D146), so a quiet stretch means the
/// balancer settled, not that it is waiting out a dwell.
const SETTLED: Duration = Duration::from_secs(2);

/// How long a shard thread's turn may run before it polls its commits again.
const TURN: Duration = Duration::from_micros(50);

/// How a shard thread sleeps between turns, and how its commits' wakers and the shard's
/// wakeup callback wake it.
///
/// With `IoBackend::Uring` a shard's I/O completes only when its thread runs it again, so
/// the thread waits on the shard's completion fd ([`Shard::io_fd`], #408) and an eventfd the
/// wakes signal, as an application's event loop would. Without the fd (taking it is the
/// opt-in), `next_wakeup` is zero while any I/O is in flight and the thread could only spin:
/// with commits always in flight (D213) every driver spun on its core, and the kernel's
/// fsync workers queued behind them. Elsewhere (pread, other systems) it parks.
struct Sleeper {
    thread: std::thread::Thread,
    /// Set by a wake, taken by the next [`Sleeper::idle`]: a wake that lands while the
    /// thread runs its turn ends the next sleep at once.
    pending: AtomicBool,
    #[cfg(target_os = "linux")]
    event: Option<std::os::fd::OwnedFd>,
    /// The thread waits on `event` (its shard has a completion fd): only then do wakes write
    /// it. Elsewhere (pread) it parks, and the write would be a wasted system call.
    #[cfg(target_os = "linux")]
    polls: AtomicBool,
}

impl Sleeper {
    /// The calling thread's sleeper.
    fn current() -> Arc<Self> {
        Arc::new(Self {
            thread: std::thread::current(),
            pending: AtomicBool::new(false),
            #[cfg(target_os = "linux")]
            event: rustix::event::eventfd(
                0,
                rustix::event::EventfdFlags::NONBLOCK | rustix::event::EventfdFlags::CLOEXEC,
            )
            .ok(),
            #[cfg(target_os = "linux")]
            polls: AtomicBool::new(false),
        })
    }

    /// Wakes the thread from another one (or marks a wake from its own turn).
    fn wake(&self) {
        self.pending.store(true, Ordering::SeqCst);
        if std::thread::current().id() == self.thread.id() {
            return;
        }
        #[cfg(target_os = "linux")]
        if let Some(event) = &self.event
            && self.polls.load(Ordering::SeqCst)
        {
            let _ = rustix::io::write(event, &1u64.to_ne_bytes());
        }
        self.thread.unpark();
    }

    /// Sleeps until a wake, the shard's I/O completion, its [`next_wakeup`](Shard::next_wakeup)
    /// or `cap`, whichever comes first.
    fn idle(&self, shard: &Shard, cap: Duration) {
        // `io_fd` first: taking it is what stops `next_wakeup` reporting in-flight I/O as due
        // now. `polls` is published before `pending` is checked (each side stores, then
        // loads the other's flag, SeqCst), so a wake either lands before the check or sees
        // `polls` and writes the eventfd.
        #[cfg(target_os = "linux")]
        let ring = self
            .event
            .as_ref()
            .and_then(|event| shard.io_fd().map(|fd| (fd, event)));
        #[cfg(target_os = "linux")]
        if ring.is_some() && !self.polls.load(Ordering::Relaxed) {
            self.polls.store(true, Ordering::SeqCst);
        }
        if self.pending.swap(false, Ordering::SeqCst) {
            return;
        }
        #[cfg(target_os = "linux")]
        if let Some((fd, event)) = ring {
            let wait = shard.next_wakeup().unwrap_or(cap).min(cap);
            if !wait.is_zero() {
                poll_ring(fd, event, wait);
            }
            return;
        }
        let wait = shard.next_wakeup().unwrap_or(cap).min(cap);
        if !wait.is_zero() {
            std::thread::park_timeout(wait);
        }
    }
}

/// Waits up to `wait` for the shard's completion fd `fd` or the wake eventfd `event` to turn
/// readable, then resets `event`.
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn poll_ring(fd: i32, event: &std::os::fd::OwnedFd, wait: Duration) {
    use rustix::event::{PollFd, PollFlags, Timespec};
    // SAFETY: `fd` is this thread's completion fd from `Shard::io_fd`, owned by the thread's
    // I/O ring, which lives as long as the thread; the borrow ends with this call.
    let ring = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) };
    let mut fds = [
        PollFd::new(&ring, PollFlags::IN),
        PollFd::new(event, PollFlags::IN),
    ];
    let timeout = Timespec {
        tv_sec: i64::try_from(wait.as_secs()).unwrap_or(i64::MAX),
        tv_nsec: wait.subsec_nanos().into(),
    };
    let _ = rustix::event::poll(&mut fds, Some(&timeout));
    let mut count = [0u8; 8];
    let _ = rustix::io::read(event, &mut count);
}

impl Wake for Sleeper {
    fn wake(self: Arc<Self>) {
        Sleeper::wake(&self);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        Sleeper::wake(self);
    }
}

/// One write of the stream: a row and its cells, in one family.
struct Write {
    row: Vec<u8>,
    family: &'static str,
    cells: Vec<(Vec<u8>, Vec<u8>)>,
}

fn writes(ops: impl Iterator<Item = BenchOp>) -> Result<Vec<Write>, String> {
    ops.map(|op| match op {
        BenchOp::Put { row, family, cells } => Ok(Write { row, family, cells }),
        other => Err(format!(
            "the inline scaling driver runs writes only, not {other:?}"
        )),
    })
    .collect()
}

/// Keys that cut the loaded rows into `pieces` runs of about equal size (D213): the
/// `i * len / pieces`-th distinct row, for `i` in `1..pieces`.
fn split_keys(load: &[Write], pieces: usize) -> Vec<Vec<u8>> {
    let mut rows: Vec<&[u8]> = load.iter().map(|w| w.row.as_slice()).collect();
    rows.sort_unstable();
    rows.dedup();
    if rows.len() < 2 {
        return Vec::new();
    }
    let mut keys: Vec<Vec<u8>> = (1..pieces)
        .map(|i| rows[i * rows.len() / pieces].to_vec())
        .filter(|k| !k.is_empty())
        .collect();
    keys.dedup();
    keys
}

/// Fails unless every shard owns exactly one tablet (D213: the pre-split's placement, which
/// the measured phase must run on unchanged).
fn one_tablet_each(db: &Pigeonhole, when: &str) -> Result<(), String> {
    let tablets: Vec<u64> = db.shard_stats().iter().map(|s| s.tablets).collect();
    if tablets.iter().all(|&t| t == 1) {
        Ok(())
    } else {
        Err(format!(
            "D213: {when}, the shards own {tablets:?} tablets, not one each; the measured \
             phase would not run on the pre-split placement"
        ))
    }
}

fn submit(t: &Table, w: &Write) -> CommitFuture {
    let mut m = t.mutate(&w.row);
    for (q, v) in &w.cells {
        m = m.put(w.family, q, v);
    }
    m.commit_async()
}

/// Pipelines `writes` (up to `in_flight` at a time), giving `shard` a turn between polls;
/// records each commit's latency (submit to resolution) in `hist` when given.
fn pipeline(
    t: &Table,
    writes: &[&Write],
    in_flight: usize,
    shard: &mut Shard,
    sleeper: &Arc<Sleeper>,
    mut hist: Option<&mut Histogram>,
) -> Result<(), String> {
    let waker = Waker::from(Arc::clone(sleeper));
    let mut cx = Context::from_waker(&waker);
    let mut flight: VecDeque<(Instant, CommitFuture)> = VecDeque::with_capacity(in_flight);
    let mut next = writes.iter();
    loop {
        while flight.len() < in_flight {
            match next.next() {
                Some(w) => flight.push_back((Instant::now(), submit(t, w))),
                None => break,
            }
        }
        if flight.is_empty() {
            return Ok(());
        }
        let mut progressed = false;
        let mut failed = None;
        flight.retain_mut(|(at, f)| match Pin::new(f).poll(&mut cx) {
            Poll::Ready(r) => {
                progressed = true;
                match r {
                    Ok(_) => {
                        if let Some(h) = hist.as_deref_mut() {
                            h.record(at.elapsed());
                        }
                    }
                    Err(e) => failed = Some(err(e)),
                }
                false
            }
            Poll::Pending => true,
        });
        if let Some(e) = failed {
            return Err(e);
        }
        let more = shard.run_once(TURN);
        if !progressed && !more {
            // Woken by a commit's completion or by the shard's wakeup callback.
            sleeper.idle(shard, Duration::from_micros(200));
        }
    }
}

/// Drives every shard on a helper thread while `f` runs (setup and teardown, which use the
/// blocking API), then gives the shards back.
fn with_drivers<T>(shards: Vec<Shard>, f: impl FnOnce() -> T) -> (Vec<Shard>, T) {
    let stop = Arc::new(AtomicBool::new(false));
    let threads: Vec<_> = shards
        .into_iter()
        .map(|mut shard| {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let sleeper = Sleeper::current();
                let me = Arc::clone(&sleeper);
                shard.set_wakeup(Box::new(move || Sleeper::wake(&me)));
                while !stop.load(Ordering::Acquire) {
                    if !shard.run_once(Duration::from_millis(1)) {
                        sleeper.idle(&shard, Duration::from_micros(500));
                    }
                }
                // Handed back to the caller's next phase: nothing of the shard's may stay on
                // this thread's ring (#492).
                shard.release();
                shard
            })
        })
        .collect();
    let out = f();
    stop.store(true, Ordering::Release);
    let shards = threads
        .into_iter()
        .map(|t| t.join().expect("a driver thread panicked"))
        .collect();
    (shards, out)
}

/// One phase on the shard threads: each thread drives one shard and pipelines the writes to
/// rows that shard owns ([`Table::shard_of`]); it keeps driving its shard until every
/// thread is done (other threads' commits may need it). Returns the shards, the wall time
/// and the merged latencies.
fn phase(
    t: &Table,
    shards: Vec<Shard>,
    writes: &[Write],
    in_flight: usize,
    record: bool,
) -> Result<(Vec<Shard>, Duration, Histogram), String> {
    let n = shards.len();
    let done = AtomicUsize::new(0);
    let start = Instant::now();
    let results: Vec<Result<(Shard, Histogram), String>> = std::thread::scope(|s| {
        let threads: Vec<_> = shards
            .into_iter()
            .map(|mut shard| {
                let done = &done;
                // The rows this thread's shard owns as the phase starts (D204): its writes
                // then commit on its own shard. A row the balancer moves during the phase
                // is still committed correctly, routed to its new owner.
                let i = shard.index();
                let mine: Vec<&Write> = writes
                    .iter()
                    .filter(|w| t.shard_of(&w.row).unwrap_or(0) == i)
                    .collect();
                s.spawn(move || {
                    let sleeper = Sleeper::current();
                    let me = Arc::clone(&sleeper);
                    shard.set_wakeup(Box::new(move || Sleeper::wake(&me)));
                    let mut hist = Histogram::new();
                    let r = pipeline(
                        t,
                        &mine,
                        in_flight,
                        &mut shard,
                        &sleeper,
                        record.then_some(&mut hist),
                    );
                    done.fetch_add(1, Ordering::AcqRel);
                    // Other threads' commits may still need this shard: serve them, parked
                    // between turns until one arrives (the wakeup unparks this thread).
                    while done.load(Ordering::Acquire) < n {
                        if !shard.run_once(TURN) {
                            sleeper.idle(&shard, Duration::from_micros(200));
                        }
                    }
                    // The next phase drives it on another thread (#492).
                    shard.release();
                    r.map(|()| (shard, hist))
                })
            })
            .collect();
        threads
            .into_iter()
            .map(|t| t.join().expect("a shard thread panicked"))
            .collect()
    });
    let elapsed = start.elapsed();
    let mut shards = Vec::with_capacity(n);
    let mut hist = Histogram::new();
    for r in results {
        let (shard, h) = r?;
        shards.push(shard);
        hist.merge(&h);
    }
    Ok((shards, elapsed, hist))
}

impl PigeonholeRunner {
    /// Runs `config` (writes only) the way the scaling gate measures it (D204): application-
    /// owned shards, each thread issuing its shard's share of the writes inline with
    /// [`INLINE_IN_FLIGHT`] commits in flight. The load is applied through the blocking API
    /// first; the warmup and the measured writes run inline, the warmup unrecorded.
    pub fn run_inline(
        &self,
        config: &WorkloadConfig,
        dir: &Path,
        options_: &RunOptions,
    ) -> Result<RunRecord, String> {
        let warmup = options_.warmup_ops(config);
        let mut workload = Workload::new(WorkloadConfig {
            operations: config.operations + warmup,
            ..config.clone()
        });
        let (db, shards) =
            Pigeonhole::open_application_owned(dir.join("bench.phdb"), options(&self.settings))
                .map_err(err)?;
        let n = shards.len();
        let load_ops = writes(workload.load_ops())?;
        let load_start = Instant::now();
        let presplit = self.settings.presplit;
        let (shards, setup) = with_drivers(shards, || -> Result<Table, String> {
            let t = create_table(&db)?;
            let keys = if presplit && n > 1 {
                split_keys(&load_ops, n)
            } else {
                Vec::new()
            };
            if !keys.is_empty() {
                // Every shard spends its one balancer pass now, before any piece arrives
                // (D213). With the balancer off, a shard still runs a single pass at its
                // first turn, and a pass that is a shard's first counts the tablets it then
                // owns as settled: a shard idle until the split would see its cold piece as
                // settled and consolidate it onto its left neighbour's shard, as on the AX102
                // at 32 shards. A flush sends every shard a message, so each takes that turn
                // here; the pause lets the last of them finish it.
                db.flush().map_err(err)?;
                std::thread::sleep(Duration::from_millis(100));
                t.split_over_shards(keys).map_err(err)?;
                one_tablet_each(&db, "after the pre-split")?;
            }
            for w in &load_ops {
                let mut m = t.mutate(&w.row);
                for (q, v) in &w.cells {
                    m = m.put(w.family, q, v);
                }
                m.commit().map_err(err)?;
            }
            Ok(t)
        });
        let t = setup?;
        let load = load_start.elapsed();
        let run = writes(workload.run_ops())?;
        let (warm, measured) = run.split_at((warmup as usize).min(run.len()));
        // Warm up until the balancer has spread the table and settled: every shard owns a
        // tablet and nothing split, merged or moved for `SETTLED` (a fixed warmup can end
        // before the first split, and the measured phase would then be one tablet on one
        // shard). Passes repeat the warmup writes; the cap bounds a table that never settles,
        // reported as a warning.
        let started = Instant::now();
        let mut last_change = started;
        let mut shards = shards;
        loop {
            let before = db.shard_stats();
            shards = phase(&t, shards, warm, INLINE_IN_FLIGHT, false)?.0;
            let after = db.shard_stats();
            let changed = before
                .iter()
                .zip(&after)
                .any(|(b, a)| (a.splits, a.merges, a.moves) != (b.splits, b.merges, b.moves));
            if changed {
                last_change = Instant::now();
            }
            let spread = after.iter().all(|s| s.tablets > 0);
            // Pre-split, the balancer never runs (D213): one warmup pass is enough.
            if presplit || (spread && last_change.elapsed() >= SETTLED) || warm.is_empty() {
                break;
            }
            if started.elapsed() > SPREAD_CAP {
                eprintln!(
                    "warning: the table did not spread over {n} shards within {SPREAD_CAP:?}; \
                     the measured phase may use fewer"
                );
                break;
            }
        }
        if presplit && n > 1 {
            one_tablet_each(&db, "before the measured phase")?;
        }
        let (before, stalls_before) = (db.shard_stats(), super::pigeonhole::stalls(&db));
        let latency_before = db.commit_latency_buckets();
        // The measured writes, repeated until `min_measured` has been measured (D219): a
        // fixed count measures a faster store for less time, too short for its flushes and
        // compactions to show.
        let (mut shards, mut elapsed, mut hist) = (shards, Duration::ZERO, Histogram::new());
        loop {
            let (s, e, h) = phase(&t, shards, measured, INLINE_IN_FLIGHT, true)?;
            shards = s;
            elapsed += e;
            hist.merge(&h);
            if elapsed >= options_.min_measured || measured.is_empty() {
                break;
            }
        }
        let (after, stalls_after) = (db.shard_stats(), super::pigeonhole::stalls(&db));
        let engine_commit = EngineCommit::between(
            &latency_before,
            &db.commit_latency_buckets(),
            Pigeonhole::commit_latency_floor,
        );
        let detail_shards = before
            .iter()
            .zip(&after)
            .map(|(b, a)| ShardShare {
                commits: a.commits - b.commits,
                tablets_start: b.tablets,
                tablets_end: a.tablets,
                splits: a.splits - b.splits,
                merges: a.merges - b.merges,
                moves: a.moves - b.moves,
                busy_nanos: a.busy_nanos - b.busy_nanos,
            })
            .collect();
        drop(t);
        let (_, closed) = with_drivers(shards, || db.close().map_err(err));
        closed?;
        let ops = hist.count();
        Ok(RunRecord {
            store: self.name().to_owned(),
            store_config: format!("{} inline in-flight={INLINE_IN_FLIGHT}", self.describe()),
            workload: config.kind.name().to_owned(),
            seed: config.seed,
            records: config.records,
            operations: ops,
            value_len: config.value_len,
            threads: n,
            load_secs: load.as_secs_f64(),
            run_secs: elapsed.as_secs_f64(),
            throughput: ops as f64 / elapsed.as_secs_f64().max(1e-9),
            p50_ns: hist.percentile(0.50).as_nanos() as u64,
            p99_ns: hist.percentile(0.99).as_nanos() as u64,
            p999_ns: hist.percentile(0.999).as_nanos() as u64,
            mean_ns: hist.mean().as_nanos() as u64,
            max_ns: hist.max().as_nanos() as u64,
            warmup_ops: warmup,
            detail: RunDetail {
                shards: detail_shards,
                stalls: Some(Stalls::between(&stalls_before, &stalls_after)),
                engine_commit,
                ..RunDetail::default()
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole::Options;

    /// A one-shard application-owned store, driven on this thread until idle.
    fn idle_shard(tag: &str) -> (Pigeonhole, Shard, std::path::PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("phdb-bench-inline-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (db, mut shards) = Pigeonhole::open_application_owned(
            dir.join("s.phdb"),
            Options::default().shards(1).tablet_changes(false),
        )
        .unwrap();
        let mut shard = shards.pop().unwrap();
        while shard.run_once(TURN) {}
        (db, shard, dir)
    }

    fn close(db: Pigeonhole, mut shard: Shard, dir: &Path) {
        db.close().unwrap();
        while shard.closed().is_none() {
            shard.run_once(TURN);
        }
        shard.closed().unwrap().unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_wake_during_the_turn_ends_the_next_sleep_at_once() {
        let (db, shard, dir) = idle_shard("self");
        let sleeper = Sleeper::current();
        let started = Instant::now();
        Sleeper::wake(&sleeper);
        sleeper.idle(&shard, Duration::from_secs(30));
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
        close(db, shard, &dir);
    }

    #[test]
    fn a_wake_from_another_thread_ends_the_sleep() {
        let (db, shard, dir) = idle_shard("other");
        let sleeper = Sleeper::current();
        let idle_for = shard.next_wakeup();
        let waker = Arc::clone(&sleeper);
        let woke = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            Sleeper::wake(&waker);
        });
        let started = Instant::now();
        sleeper.idle(&shard, Duration::from_secs(30));
        let slept = started.elapsed();
        woke.join().unwrap();
        // Nothing else is due for longer than the wake takes (a sleep cut short by the shard's
        // own deadline would prove nothing).
        if idle_for.is_none_or(|d| d >= Duration::from_secs(10)) {
            assert!(slept < Duration::from_secs(5), "slept {slept:?}");
        }
        close(db, shard, &dir);
    }
}
