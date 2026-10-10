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
    BenchOp, Histogram, PigeonholeRunner, RunDetail, RunOptions, RunRecord, Runner, ShardShare,
    Stalls, Workload, WorkloadConfig,
};

/// Commits each shard thread keeps in flight (D204).
pub const INLINE_IN_FLIGHT: usize = 16;

/// Longest the warmup runs while waiting for the balancer to spread the table (D204).
const SPREAD_CAP: Duration = Duration::from_secs(30);

/// How long the table must go without a split, merge or move before the warmup ends: longer
/// than the balancer's dwell (10 passes of 100 ms, D146), so a quiet stretch means the
/// balancer settled, not that it is waiting out a dwell.
const SETTLED: Duration = Duration::from_secs(2);

/// Sleeps until the shard's wakeup or completion fires, but no longer than its
/// [`next_wakeup`](Shard::next_wakeup) or `cap`: with `IoBackend::Uring` a shard's I/O
/// completes only when its thread runs it again, and `next_wakeup` is zero while any is in
/// flight, so a loop that slept anyway would sleep through its own WAL writes (the #154
/// Linux anomaly).
fn idle(shard: &Shard, cap: Duration) {
    let wait = shard.next_wakeup().unwrap_or(cap).min(cap);
    if !wait.is_zero() {
        std::thread::park_timeout(wait);
    }
}

/// How long a shard thread's turn may run before it polls its commits again.
const TURN: Duration = Duration::from_micros(50);

struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
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
    mut hist: Option<&mut Histogram>,
) -> Result<(), String> {
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
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
            idle(shard, Duration::from_micros(200));
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
                let me = std::thread::current();
                shard.set_wakeup(Box::new(move || me.unpark()));
                while !stop.load(Ordering::Acquire) {
                    if !shard.run_once(Duration::from_millis(1)) {
                        idle(&shard, Duration::from_micros(500));
                    }
                }
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
                    let me = std::thread::current();
                    shard.set_wakeup(Box::new(move || me.unpark()));
                    let mut hist = Histogram::new();
                    let r = pipeline(t, &mine, in_flight, &mut shard, record.then_some(&mut hist));
                    done.fetch_add(1, Ordering::AcqRel);
                    // Other threads' commits may still need this shard: serve them, parked
                    // between turns until one arrives (the wakeup unparks this thread).
                    while done.load(Ordering::Acquire) < n {
                        if !shard.run_once(TURN) {
                            idle(&shard, Duration::from_micros(200));
                        }
                    }
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
        let (shards, setup) = with_drivers(shards, || -> Result<Table, String> {
            let t = create_table(&db)?;
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
            if (spread && last_change.elapsed() >= SETTLED) || warm.is_empty() {
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
        let (before, stalls_before) = (db.shard_stats(), super::pigeonhole::stalls(&db));
        let (shards, elapsed, hist) = phase(&t, shards, measured, INLINE_IN_FLIGHT, true)?;
        let (after, stalls_after) = (db.shard_stats(), super::pigeonhole::stalls(&db));
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
                ..RunDetail::default()
            },
        })
    }
}
