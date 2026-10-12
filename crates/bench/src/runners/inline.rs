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
use std::task::{Context, Poll};
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

/// Writes a shard thread checks for its rows between turns of its shard, as a phase starts.
const PICK_CHUNK: usize = 1 << 16;

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

/// The threads of one phase (#578): they start together, and if one of them panics the
/// others stop at once instead of waiting for it (its shard is gone, so commits routed to it
/// never resolve).
struct Crew {
    threads: usize,
    started: AtomicUsize,
    done: AtomicUsize,
    failed: AtomicBool,
}

/// What a crew thread that stopped because another one failed returns.
const ANOTHER_FAILED: &str = "another shard thread failed";

impl Crew {
    fn new(threads: usize) -> Self {
        Self {
            threads,
            started: AtomicUsize::new(0),
            done: AtomicUsize::new(0),
            failed: AtomicBool::new(false),
        }
    }

    /// Waits until every thread and the caller are ready to start. False if a thread failed
    /// meanwhile.
    fn start(&self) -> bool {
        self.started.fetch_add(1, Ordering::AcqRel);
        while self.started.load(Ordering::Acquire) <= self.threads {
            if self.failed() {
                return false;
            }
            std::thread::yield_now();
        }
        !self.failed()
    }

    /// Whether a thread of the crew panicked.
    fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// This thread's own work is done.
    fn finish(&self) {
        self.done.fetch_add(1, Ordering::AcqRel);
    }

    /// Every thread's own work is done, or one failed: nobody needs this thread's shard.
    fn over(&self) -> bool {
        self.done.load(Ordering::Acquire) >= self.threads || self.failed()
    }
}

/// Marks the crew failed if its thread unwinds.
struct FailOnPanic<'a>(&'a Crew);

impl Drop for FailOnPanic<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.failed.store(true, Ordering::Release);
        }
    }
}

/// Runs `body` on a thread per item, as a [`Crew`], and returns the results and when the
/// crew started (every thread ready). A panicking thread fails the whole run with its
/// message, promptly: the others see the crew failed and stop.
fn run_crew<I: Send, R: Send>(
    items: Vec<I>,
    body: impl Fn(I, &Crew) -> R + Sync,
) -> Result<(Vec<R>, Instant), String> {
    let crew = Crew::new(items.len());
    let mut start = Instant::now();
    let joined: Vec<std::thread::Result<R>> = std::thread::scope(|s| {
        let threads: Vec<_> = items
            .into_iter()
            .map(|item| {
                let (crew, body) = (&crew, &body);
                s.spawn(move || {
                    let _guard = FailOnPanic(crew);
                    body(item, crew)
                })
            })
            .collect();
        crew.start();
        start = Instant::now();
        threads.into_iter().map(|t| t.join()).collect()
    });
    let mut results = Vec::with_capacity(joined.len());
    let mut panicked = None;
    for (i, r) in joined.into_iter().enumerate() {
        match r {
            Ok(r) => results.push(r),
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_owned())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "(no message)".to_owned());
                panicked.get_or_insert(format!("shard thread {i} panicked: {msg}"));
            }
        }
    }
    match panicked {
        Some(e) => Err(e),
        None => Ok((results, start)),
    }
}

/// Pipelines `writes` (up to `in_flight` at a time), giving `shard` a turn between polls;
/// records each commit's latency (submit to resolution) in `hist` when given. Stops with an
/// error if another thread of `crew` fails.
fn pipeline(
    t: &Table,
    writes: &[&Write],
    in_flight: usize,
    shard: &mut Shard,
    mut hist: Option<&mut Histogram>,
    crew: &Crew,
) -> Result<(), String> {
    // A commit can resolve on another shard's thread: its wake must end this thread's wait.
    let waker = shard.waker();
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
        if crew.failed() {
            return Err(ANOTHER_FAILED.to_owned());
        }
        let more = shard.run_once(TURN);
        if !progressed && !more {
            // Until a commit resolves, work arrives, or this thread's own I/O completes (#553).
            shard.wait(Duration::from_micros(200));
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
                while !stop.load(Ordering::Acquire) {
                    if !shard.run_once(Duration::from_millis(1)) {
                        shard.wait(Duration::from_micros(500));
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
    let (results, start) = run_crew(shards, |mut shard, crew| {
        // The rows this thread's shard owns as the phase starts (D204): its writes then
        // commit on its own shard. A row the balancer moves during the phase is still
        // committed correctly, routed to its new owner. Every thread picks its rows, then
        // they all start together: picked one shard after another, the shards started (and
        // finished) staggered, and the balancer saw the idle ones' loads as skew (D223). The
        // shard takes a turn between chunks, so its balancer keeps publishing its load
        // meanwhile (a shard that misses its passes counts as idle).
        let i = shard.index();
        let mut mine: Vec<&Write> = Vec::new();
        for chunk in writes.chunks(PICK_CHUNK) {
            mine.extend(
                chunk
                    .iter()
                    .filter(|w| t.shard_of(&w.row).unwrap_or(0) == i),
            );
            shard.run_once(TURN);
        }
        if !crew.start() {
            return Err(ANOTHER_FAILED.to_owned());
        }
        let mut hist = Histogram::new();
        let r = pipeline(
            t,
            &mine,
            in_flight,
            &mut shard,
            record.then_some(&mut hist),
            crew,
        );
        crew.finish();
        // Other threads' commits may still need this shard: serve them, waiting between
        // turns until one arrives.
        while !crew.over() {
            if !shard.run_once(TURN) {
                shard.wait(Duration::from_micros(200));
            }
        }
        if crew.failed() {
            return Err(ANOTHER_FAILED.to_owned());
        }
        // The next phase drives it on another thread (#492).
        shard.release();
        r.map(|()| (shard, hist))
    })?;
    let elapsed = start.elapsed();
    let mut shards = Vec::with_capacity(results.len());
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
    use super::{ANOTHER_FAILED, run_crew};
    use std::sync::mpsc;
    use std::time::Duration;

    /// Runs `f` on a thread and fails the test if it takes longer than a few seconds: the
    /// bug was a hang, so a hang must fail rather than stall the suite.
    fn promptly<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || tx.send(f()).unwrap());
        rx.recv_timeout(Duration::from_secs(10))
            .expect("the crew hung instead of failing")
    }

    /// A worker like a phase's: starts with the crew, does its own work (`work`), then
    /// serves the others until the crew is over.
    fn worker(
        i: usize,
        panic_at: Option<(usize, bool)>,
        crew: &super::Crew,
    ) -> Result<usize, String> {
        if panic_at == Some((i, false)) {
            panic!("worker {i} broke before the start");
        }
        if !crew.start() {
            return Err(ANOTHER_FAILED.to_owned());
        }
        if panic_at == Some((i, true)) {
            panic!("worker {i} broke mid-phase");
        }
        crew.finish();
        while !crew.over() {
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(i)
    }

    #[test]
    fn a_crew_runs_every_worker_and_returns_their_results() {
        let (results, _) = promptly(|| run_crew((0..4).collect(), |i, c| worker(i, None, c)))
            .expect("no worker failed");
        assert_eq!(results, [Ok(0), Ok(1), Ok(2), Ok(3)]);
    }

    #[test]
    fn a_worker_panicking_mid_phase_fails_the_run_with_its_message() {
        let err = promptly(|| run_crew((0..4).collect(), |i, c| worker(i, Some((2, true)), c)))
            .expect_err("a worker panicked");
        assert_eq!(err, "shard thread 2 panicked: worker 2 broke mid-phase");
    }

    #[test]
    fn a_worker_panicking_before_the_start_fails_the_run_with_its_message() {
        let err = promptly(|| run_crew((0..4).collect(), |i, c| worker(i, Some((1, false)), c)))
            .expect_err("a worker panicked");
        assert_eq!(
            err,
            "shard thread 1 panicked: worker 1 broke before the start"
        );
    }
}
