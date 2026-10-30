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

use pigeonhole::nonblocking::{CommitFuture, TransactionCommitFuture};
use pigeonhole::{ErrorCode, Pigeonhole, RetryPolicy, Shard, Table};
use pigeonhole_io::sys::PinPlan;

use super::pigeonhole::{create_table, err, options, put_cells, retry_policy};
use crate::{
    BenchOp, EngineCommit, Histogram, LatencyStats, OpTypeStats, PigeonholeRunner, RunDetail,
    RunOptions, RunRecord, Runner, ShardShare, Stalls, WARMUP_CAP, WARMUP_CAP_AFTER_SWITCH,
    Workload, WorkloadConfig,
};

/// Commits each shard thread keeps in flight (D204).
pub const INLINE_IN_FLIGHT: usize = 16;

/// How long a shard thread's turn may run before it polls its commits again.
const TURN: Duration = Duration::from_micros(50);

/// Writes a shard thread checks for its rows between turns of its shard, as a phase starts.
const PICK_CHUNK: usize = 1 << 16;

/// What one of a phase's threads drives between polls of its commits: an application-owned
/// shard, or, engine-owned (D280's comparator, [`PigeonholeRunner::inline_engine_owned`]),
/// nothing: the engine's own shard threads run the commits, and the thread only issues the
/// same share of the writes and waits for their wakes.
enum Driver {
    // Boxed: a `Shard` is large, and the engine-owned variant is a number.
    Owned(Box<Shard>),
    Engine {
        /// The shard whose rows this thread issues ([`Table::shard_of`]).
        index: usize,
    },
}

/// Wakes the thread that made it: an engine-owned driver's commits resolve on the engine's
/// threads.
struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

impl Driver {
    fn index(&self) -> usize {
        match self {
            Driver::Owned(shard) => shard.index(),
            Driver::Engine { index } => *index,
        }
    }

    /// The waker for this thread's commits.
    fn waker(&self) -> Waker {
        match self {
            Driver::Owned(shard) => shard.waker(),
            Driver::Engine { .. } => Waker::from(Arc::new(Unpark(std::thread::current()))),
        }
    }

    /// A turn of the shard: whether it has more work at once (never, with none to drive).
    fn run_once(&mut self, turn: Duration) -> bool {
        match self {
            Driver::Owned(shard) => shard.run_once(turn),
            Driver::Engine { .. } => false,
        }
    }

    /// Waits at most `d` for work or a wake.
    fn wait(&mut self, d: Duration) {
        match self {
            Driver::Owned(shard) => shard.wait(d),
            Driver::Engine { .. } => std::thread::park_timeout(d),
        }
    }

    /// Before the shard moves to another thread (#492).
    fn release(&mut self) {
        if let Driver::Owned(shard) = self {
            shard.release();
        }
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

/// Keys that cut the measured writes into `pieces` contiguous runs whose heaviest is as light
/// as it can be (D231): each row weighs its writes. A row heavier than a fair share (the
/// skewed workload's hottest takes 6.5%) gets a piece of its own, cut just before it and just
/// after it, and the other cuts go by write weight over the rest. Exactly `pieces - 1` keys
/// when there are enough distinct rows, so every shard gets one piece.
fn split_keys_by_writes(writes: &[Write], pieces: usize) -> Vec<Vec<u8>> {
    let mut sorted: Vec<&[u8]> = writes.iter().map(|w| w.row.as_slice()).collect();
    sorted.sort_unstable();
    let mut rows: Vec<(&[u8], u64)> = Vec::new();
    for r in sorted {
        match rows.last_mut() {
            Some((last, n)) if *last == r => *n += 1,
            _ => rows.push((r, 1)),
        }
    }
    weighted_cuts(&rows, pieces)
}

/// Piece boundaries (the index of each piece's first row, after the first piece) over `rows`
/// (sorted, distinct, weighted) for the smallest cap on a piece's weight that a greedy pass
/// fits in `pieces`; then the heaviest pieces of two rows or more split at their weighted
/// middle until there are `pieces` (or no piece can split). Returns the cut rows.
fn weighted_cuts(rows: &[(&[u8], u64)], pieces: usize) -> Vec<Vec<u8>> {
    if pieces < 2 || rows.len() < 2 {
        return Vec::new();
    }
    let starts = |cap: u64| {
        let mut out: Vec<usize> = Vec::new();
        let mut acc = 0;
        for (i, (_, w)) in rows.iter().enumerate() {
            if acc > 0 && acc + w > cap {
                out.push(i);
                acc = *w;
            } else {
                acc += w;
            }
        }
        out
    };
    let (mut lo, mut hi) = (
        rows.iter().map(|(_, w)| *w).max().unwrap_or(0),
        rows.iter().map(|(_, w)| *w).sum::<u64>(),
    );
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if starts(mid).len() < pieces {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    let mut cuts = starts(lo);
    // One piece per shard: split the heaviest piece that has two rows or more.
    while cuts.len() + 1 < pieces {
        let bounds: Vec<(usize, usize)> = std::iter::once(0)
            .chain(cuts.iter().copied())
            .zip(cuts.iter().copied().chain(std::iter::once(rows.len())))
            .collect();
        let Some(&(a, b)) = bounds
            .iter()
            .filter(|(a, b)| b - a >= 2)
            .max_by_key(|(a, b)| rows[*a..*b].iter().map(|(_, w)| w).sum::<u64>())
        else {
            break;
        };
        let half = rows[a..b].iter().map(|(_, w)| w).sum::<u64>() / 2;
        let (mut acc, mut at) = (0, a + 1);
        for (i, (_, w)) in rows.iter().enumerate().take(b - 1).skip(a) {
            acc += w;
            at = i + 1;
            if acc >= half {
                break;
            }
        }
        cuts.push(at.clamp(a + 1, b - 1));
        cuts.sort_unstable();
        cuts.dedup();
    }
    cuts.into_iter().map(|i| rows[i].0.to_vec()).collect()
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

/// A commit in flight: a write or batch commit, or a reading transaction's, whose commit
/// future is its own type (ICR 0060).
enum Flight {
    Commit(CommitFuture),
    Txn(TransactionCommitFuture),
}

impl std::future::Future for Flight {
    type Output = pigeonhole::Result<pigeonhole::CommitInfo>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Flight::Commit(f) => Pin::new(f).poll(cx),
            Flight::Txn(f) => Pin::new(f).poll(cx),
        }
    }
}

fn submit(t: &Table, w: &Write) -> CommitFuture {
    put_cells(t.mutate(&w.row), w.family, &w.cells).commit_async()
}

/// Every this-many commits of a thread with cross-shard batches (D247's app-owned row), one is
/// a batch: about 20% of the commits.
const CROSS_BATCH_EVERY: usize = 5;

/// The rows a batch at commit `j` adds to its own write: 1-3, picked across the stream.
fn batch_extras(j: usize, all: &[Write]) -> impl Iterator<Item = &Write> {
    let extra = 1 + (j / CROSS_BATCH_EVERY) % 3;
    (1..=extra).map(move |m| &all[(j.wrapping_mul(7919) + m.wrapping_mul(104_729)) % all.len()])
}

/// D247's app-owned row: commit `j` of a thread's share is a batch of `w` and 1-3 rows picked
/// across the whole stream (`all`), most of them owned by other shards; other commits are `w`
/// alone.
/// Whether commit `j` of a thread's share is a batch (D247's app-owned row), given the
/// whole stream's length.
fn is_batch(j: usize, all: usize) -> bool {
    j.is_multiple_of(CROSS_BATCH_EVERY) && all >= 2
}

/// With readers on, whether batch `j` is a reading transaction: every other one.
fn is_rmw(j: usize, all: usize, readers: bool) -> bool {
    readers && is_batch(j, all) && (j / CROSS_BATCH_EVERY) % 2 == 1
}

/// The mixed row's reading commit (#816, D267): a transaction that reads a cold companion
/// row of each of its rows (`<row>/r`, never written) and writes the batch's rows, so it
/// meets the blind batches on their hot rows as a commit that reads (it cannot chain behind
/// them) without conflicting on the hot rows themselves: a hot row is written every few
/// microseconds, so a transaction reading one would conflict on nearly every attempt.
fn submit_rmw(
    db: &Pigeonhole,
    t: &Table,
    w: &Write,
    j: usize,
    all: &[Write],
) -> Result<TransactionCommitFuture, String> {
    let mut txn = db.transaction().map_err(err)?;
    let rows: Vec<&Write> = std::iter::once(w).chain(batch_extras(j, all)).collect();
    for x in &rows {
        if let Some((q, _)) = x.cells.first() {
            let mut cold = x.row.clone();
            cold.extend_from_slice(b"/r");
            txn.get(t, &cold, x.family, q).map_err(err)?;
        }
    }
    for x in &rows {
        for (q, v) in &x.cells {
            txn.put(t, &x.row, x.family, q, v);
        }
    }
    Ok(txn.commit_async())
}

fn submit_with_batches(
    db: &Pigeonhole,
    t: &Table,
    w: &Write,
    j: usize,
    all: &[Write],
) -> CommitFuture {
    if !is_batch(j, all.len()) {
        return submit(t, w);
    }
    let mut batch = db.write_batch();
    for x in std::iter::once(w).chain(batch_extras(j, all)) {
        for (q, v) in &x.cells {
            batch.put(t, &x.row, x.family, q, v);
        }
    }
    batch.commit_async()
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

/// Commit latencies (submit to resolution) by kind: single-row commits, D247's batches and
/// the mixed row's read-modify-write transactions.
#[derive(Default)]
struct KindHists {
    single: Histogram,
    batch: Histogram,
    rmw: Histogram,
    /// Read-modify-write transactions retried after a conflict.
    conflicts: u64,
}

impl KindHists {
    fn merge(&mut self, other: &KindHists) {
        self.single.merge(&other.single);
        self.batch.merge(&other.batch);
        self.rmw.merge(&other.rmw);
        self.conflicts += other.conflicts;
    }

    fn all(&self) -> Histogram {
        let mut h = Histogram::new();
        h.merge(&self.single);
        h.merge(&self.batch);
        h.merge(&self.rmw);
        h
    }
}

/// A commit of [`pipeline`]'s in flight.
struct InFlight<'w> {
    /// Its first submission: the latency counts from here across retries.
    at: Instant,
    /// Its index in the thread's writes.
    j: usize,
    w: &'w Write,
    /// Retries so far, after `Busy` or a conflict.
    tries: u32,
    future: Flight,
}

/// A conflicted transaction of [`pipeline`]'s waiting out its `RetryPolicy` pause.
struct Paused<'w> {
    /// When it is submitted again.
    due: Instant,
    at: Instant,
    j: usize,
    w: &'w Write,
    /// Attempts so far.
    tries: u32,
}

/// Pipelines `writes` (up to `in_flight` at a time), giving `shard` a turn between polls;
/// records each commit's latency (submit to resolution) in `hist` when given, by kind. Stops with an
/// error if another thread of `crew` fails.
fn pipeline(
    t: &Table,
    writes: &[&Write],
    in_flight: usize,
    shard: &mut Driver,
    mut hist: Option<&mut KindHists>,
    crew: &Crew,
    extras: PipelineExtras<'_>,
) -> Result<u64, String> {
    let PipelineExtras {
        batches,
        readers,
        deadline,
        retry: policy,
    } = extras;
    // A commit can resolve on another shard's thread: its wake must end this thread's wait.
    let waker = shard.waker();
    let mut cx = Context::from_waker(&waker);
    let submit_one = |j: usize, w: &Write| match batches {
        Some((db, all)) if is_rmw(j, all.len(), readers) => {
            submit_rmw(db, t, w, j, all).map(Flight::Txn)
        }
        Some((db, all)) => Ok(Flight::Commit(submit_with_batches(db, t, w, j, all))),
        None => Ok(Flight::Commit(submit(t, w))),
    };
    let mut flight: VecDeque<InFlight<'_>> = VecDeque::with_capacity(in_flight);
    // Conflicted transactions waiting out their `RetryPolicy` pause. They hold their
    // pipeline slot.
    let mut paused: Vec<Paused<'_>> = Vec::new();
    let mut next = writes.iter().enumerate();
    let mut busy = 0u64;
    let mut conflicts = 0u64;
    loop {
        // Past the deadline (D247) no commit starts; those in flight finish.
        let open = deadline.is_none_or(|d| Instant::now() < d);
        let now = Instant::now();
        let mut i = 0;
        while i < paused.len() {
            if paused[i].due <= now {
                let Paused {
                    at, j, w, tries, ..
                } = paused.swap_remove(i);
                let future = submit_one(j, w)?;
                flight.push_back(InFlight {
                    at,
                    j,
                    w,
                    tries,
                    future,
                });
            } else {
                i += 1;
            }
        }
        while open && flight.len() + paused.len() < in_flight {
            match next.next() {
                Some((j, w)) => {
                    let future = submit_one(j, w)?;
                    flight.push_back(InFlight {
                        at: Instant::now(),
                        j,
                        w,
                        tries: 0,
                        future,
                    });
                }
                None => break,
            }
        }
        if flight.is_empty() && paused.is_empty() {
            if let Some(h) = hist.as_deref_mut() {
                h.conflicts += conflicts;
            }
            return Ok(busy);
        }
        let mut progressed = false;
        let mut failed = None;
        let mut retry = Vec::new();
        flight.retain_mut(|c| match Pin::new(&mut c.future).poll(&mut cx) {
            Poll::Ready(r) => {
                progressed = true;
                match r {
                    Ok(_) => {
                        if let Some(h) = hist.as_deref_mut() {
                            let kind = match batches {
                                Some((_, all)) if is_rmw(c.j, all.len(), readers) => &mut h.rmw,
                                Some((_, all)) if is_batch(c.j, all.len()) => &mut h.batch,
                                _ => &mut h.single,
                            };
                            kind.record(c.at.elapsed());
                        }
                    }
                    // The mixed row's read-modify-write transaction lost to a write after its
                    // snapshot: run again with a new snapshot, as the transfer clients do.
                    Err(e) if e.code() == ErrorCode::Conflict && c.tries < CONFLICT_RETRIES => {
                        conflicts += 1;
                        // Attempts count from 1; `tries` counts the retries so far. Past the
                        // policy's deadline it runs again at once, as before.
                        match policy
                            .next_pause(e.code(), c.tries + 1, c.at.elapsed())
                            .filter(|p| !p.is_zero())
                        {
                            Some(p) => paused.push(Paused {
                                due: Instant::now() + p,
                                at: c.at,
                                j: c.j,
                                w: c.w,
                                tries: c.tries + 1,
                            }),
                            None => retry.push((c.at, c.j, c.w, c.tries + 1, false)),
                        }
                    }
                    // A stall past the engine's timeout: retried and counted, as the client
                    // runners do (D247's app-owned row reports it instead of stopping).
                    Err(e) if e.code() == ErrorCode::Busy && c.tries < BUSY_RETRIES => {
                        retry.push((c.at, c.j, c.w, c.tries + 1, true));
                    }
                    Err(e) => failed = Some(err(e)),
                }
                false
            }
            Poll::Pending => true,
        });
        for (at, j, w, tries, stalled) in retry {
            busy += u64::from(stalled);
            let future = submit_one(j, w)?;
            flight.push_back(InFlight {
                at,
                j,
                w,
                tries,
                future,
            });
        }
        if let Some(e) = failed {
            return Err(e);
        }
        if crew.failed() {
            return Err(ANOTHER_FAILED.to_owned());
        }
        let more = shard.run_once(TURN);
        if !progressed && !more {
            // Until a commit resolves, work arrives, or this thread's own I/O completes (#553),
            // or a paused retry is due.
            let due = paused
                .iter()
                .map(|p| p.due.saturating_duration_since(Instant::now()))
                .min();
            shard.wait(due.map_or(Duration::from_micros(200), |d| {
                d.min(Duration::from_micros(200))
            }));
        }
    }
}

/// What a phase's threads do beyond pipelining their writes: D247's cross-shard batches (the
/// database to batch with) and the time bound past which no commit starts.
#[derive(Clone, Copy)]
struct PhaseExtras<'a> {
    batches: Option<&'a Pigeonhole>,
    /// Every other batch reads its rows first (the mixed row).
    readers: bool,
    deadline: Option<Instant>,
    /// How the mixed row's transactions retry a conflict (D274): `RetryPolicy::next_pause`
    /// with this thread's own timer, as an async caller drives it.
    retry: &'a RetryPolicy,
}

/// [`PhaseExtras`] for one thread's pipeline, the batches with the whole stream to pick from.
#[derive(Clone, Copy)]
struct PipelineExtras<'a> {
    batches: Option<(&'a Pigeonhole, &'a [Write])>,
    readers: bool,
    deadline: Option<Instant>,
    retry: &'a RetryPolicy,
}

/// How many times the inline driver reruns a read-modify-write transaction that conflicted
/// before the run fails.
const CONFLICT_RETRIES: u32 = 1000;

/// How many times the inline driver resubmits a commit refused with `Busy` before the run
/// fails (as the client runners' `retry_busy`).
const BUSY_RETRIES: u32 = 20;

/// Drives every shard on a helper thread while `f` runs (setup and teardown, which use the
/// blocking API), then gives the shards back.
fn with_drivers<T>(shards: Vec<Driver>, f: impl FnOnce() -> T) -> (Vec<Driver>, T) {
    // Engine-owned, the engine drives its own shards.
    if shards.iter().all(|d| matches!(d, Driver::Engine { .. })) {
        return (shards, f());
    }
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
    shards: Vec<Driver>,
    writes: &[Write],
    in_flight: usize,
    record: bool,
    pins: Option<&PinPlan>,
    extras: PhaseExtras<'_>,
) -> Result<(Vec<Driver>, Duration, KindHists, u64), String> {
    let PhaseExtras {
        batches,
        readers,
        deadline,
        retry,
    } = extras;
    let (results, start) = run_crew(shards, |mut shard, crew| {
        // D235: each shard's thread on a CPU of its own, the same one every phase. A pin the
        // OS refuses leaves the thread unpinned (the plan was checked when it was made).
        if let Some(cpu) = pins.and_then(|p| p.cpu(shard.index())) {
            let _ = pigeonhole_io::sys::pin_current_thread_to(cpu);
        }
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
        let mut hist = KindHists::default();
        let r = pipeline(
            t,
            &mine,
            in_flight,
            &mut shard,
            record.then_some(&mut hist),
            crew,
            PipelineExtras {
                batches: batches.map(|db| (db, writes)),
                readers,
                deadline,
                retry,
            },
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
        r.map(|busy| (shard, hist, busy))
    })?;
    let elapsed = start.elapsed();
    let mut shards = Vec::with_capacity(results.len());
    let mut hist = KindHists::default();
    let mut busy = 0;
    for r in results {
        let (shard, h, b) = r?;
        shards.push(shard);
        hist.merge(&h);
        busy += b;
    }
    Ok((shards, elapsed, hist, busy))
}

/// Where the run pins its shard threads (D235): one CPU of its own per shard thread, one per
/// physical core first (D220), and how its settings label that. Where pinning is refused the
/// run goes on unpinned, and its settings say so.
fn pin_plan(n: usize, pin: bool) -> (Option<PinPlan>, String) {
    if !pin {
        return (None, "unpinned".to_owned());
    }
    match pigeonhole_io::sys::pin_plan(n) {
        Ok(plan) => (Some(plan), "pinned".to_owned()),
        Err(why) => {
            eprintln!("warning: running unpinned: {}", why.as_str());
            (None, format!("unpinned ({})", why.as_str()))
        }
    }
}

/// How the warmup went (D242).
struct Warmup {
    secs: f64,
    /// Until the last split, merge, move or pending pack.
    settle_secs: f64,
    settled: bool,
    busy_retries: u64,
}

/// What the measured phase measured, and the shards' counters over it.
struct MeasuredPhase {
    elapsed: Duration,
    kinds: KindHists,
    busy_retries: u64,
    capped: bool,
    shares: Vec<ShardShare>,
    stalls: Stalls,
    engine_commit: Option<EngineCommit>,
}

/// What every phase of one inline run shares.
struct Inline<'a> {
    db: &'a Pigeonhole,
    t: &'a Table,
    in_flight: usize,
    pins: Option<&'a PinPlan>,
    /// D247's app-owned row: about 20% of the commits are batches across shards.
    batches: Option<&'a Pigeonhole>,
    readers: bool,
    retry: &'a RetryPolicy,
}

impl Inline<'_> {
    fn phase(
        &self,
        shards: Vec<Driver>,
        writes: &[Write],
        record: bool,
        deadline: Option<Instant>,
    ) -> Result<(Vec<Driver>, Duration, KindHists, u64), String> {
        phase(
            self.t,
            shards,
            writes,
            self.in_flight,
            record,
            self.pins,
            PhaseExtras {
                batches: self.batches,
                readers: self.readers,
                deadline,
                retry: self.retry,
            },
        )
    }

    /// Warms up until the balancer has spread the table and settled: every shard owns a
    /// tablet and nothing split, merged or moved for `quiet` (a fixed warmup can end before
    /// the first split, and the measured phase would then be one tablet on one shard).
    /// Passes repeat the warmup writes; `cap` bounds a table that never settles, reported as
    /// a warning. Pre-split, the balancer never runs (D213): one pass is enough.
    fn warm_up(
        &self,
        mut shards: Vec<Driver>,
        warm: &[Write],
        quiet: Duration,
        cap: Duration,
        presplit: bool,
    ) -> Result<(Vec<Driver>, Warmup), String> {
        let n = shards.len();
        let started = Instant::now();
        let mut last_change = started;
        let mut settled = true;
        let mut busy_retries = 0;
        loop {
            let before = self.db.shard_stats();
            let (s, _, _, busy) = self.phase(shards, warm, false, Some(started + cap))?;
            shards = s;
            busy_retries += busy;
            let after = self.db.shard_stats();
            let changed = before
                .iter()
                .zip(&after)
                .any(|(b, a)| (a.splits, a.merges, a.moves) != (b.splits, b.merges, b.moves));
            // A pending growth or first pack counts as a change too (D242's amendment,
            // #983): its moves have not started yet.
            let growing = self.db.pack_target().is_some_and(|(k, target)| target != k);
            if changed || growing {
                last_change = Instant::now();
            }
            let spread = after.iter().all(|s| s.tablets > 0);
            if presplit || (spread && last_change.elapsed() >= quiet) || warm.is_empty() {
                break;
            }
            if started.elapsed() > cap {
                eprintln!(
                    "warning: the table did not spread over {n} shards within {cap:?}; \
                     the measured phase may use fewer"
                );
                settled = false;
                break;
            }
        }
        let warmup = Warmup {
            secs: started.elapsed().as_secs_f64(),
            settle_secs: (last_change - started).as_secs_f64(),
            settled,
            busy_retries,
        };
        Ok((shards, warmup))
    }

    /// The measured writes, repeated until `min_measured` has been measured (D219): a fixed
    /// count measures a faster store for less time, too short for its flushes and compactions
    /// to show. Past `max_measured` no commit starts, and the phase is capped (D247).
    fn measure(
        &self,
        mut shards: Vec<Driver>,
        measured: &[Write],
        min_measured: Duration,
        max_measured: Option<Duration>,
    ) -> Result<(Vec<Driver>, MeasuredPhase), String> {
        let (before, stalls_before) = (
            super::pigeonhole::shard_shares(self.db),
            super::pigeonhole::stalls(self.db),
        );
        let latency_before = self.db.commit_latency_buckets();
        let (mut elapsed, mut kinds, mut busy_retries) = (Duration::ZERO, KindHists::default(), 0);
        let deadline = max_measured.map(|m| Instant::now() + m);
        let mut capped = false;
        loop {
            let (s, e, h, b) = self.phase(shards, measured, true, deadline)?;
            shards = s;
            busy_retries += b;
            elapsed += e;
            kinds.merge(&h);
            if deadline.is_some_and(|d| Instant::now() >= d) {
                capped = true;
                break;
            }
            if elapsed >= min_measured || measured.is_empty() {
                break;
            }
        }
        let (after, stalls_after) = (
            super::pigeonhole::shard_shares(self.db),
            super::pigeonhole::stalls(self.db),
        );
        let engine_commit = EngineCommit::between(
            &latency_before,
            &self.db.commit_latency_buckets(),
            Pigeonhole::commit_latency_floor,
        );
        let phase = MeasuredPhase {
            elapsed,
            kinds,
            busy_retries,
            capped,
            shares: before
                .iter()
                .zip(&after)
                .map(|(b, a)| ShardShare::between(b, a))
                .collect(),
            stalls: Stalls::between(&stalls_before, &stalls_after),
            engine_commit,
        };
        Ok((shards, phase))
    }
}

impl PigeonholeRunner {
    /// Opens the store for an inline run: engine-owned, its shards driven by the engine
    /// ([`Driver::Engine`]), or application-owned, its shards handed to the phases' threads
    /// and, with `background_workers` (D284), flush and compaction on worker threads of
    /// their own (none by default: on the shards). Returns the workers to join after close.
    #[allow(clippy::type_complexity)]
    fn open_inline(
        &self,
        dir: &Path,
    ) -> Result<(Pigeonhole, Vec<Driver>, Vec<std::thread::JoinHandle<()>>), String> {
        let path = dir.join("bench.phdb");
        if self.settings.inline_engine_owned {
            let db = Pigeonhole::open(path, options(&self.settings)).map_err(err)?;
            let n = db.shard_stats().len();
            let shards = (0..n).map(|index| Driver::Engine { index }).collect();
            return Ok((db, shards, Vec::new()));
        }
        let mut owned =
            Pigeonhole::open_application_owned(path, options(&self.settings)).map_err(err)?;
        let workers = owned.spawn_background_threads().map_err(err)?;
        let shards = std::mem::take(&mut owned.shards)
            .into_iter()
            .map(|s| Driver::Owned(Box::new(s)))
            .collect();
        Ok((owned.db, shards, workers))
    }

    /// Creates the table, pre-splits it when the settings say so, and applies the load
    /// through the blocking API, the shards driven on helper threads meanwhile.
    fn load_inline(
        &self,
        db: &Pigeonhole,
        shards: Vec<Driver>,
        load_ops: &[Write],
        run: &[Write],
    ) -> (Vec<Driver>, Result<Table, String>) {
        let n = shards.len();
        let presplit = self.settings.presplit;
        let by_writes = self.settings.presplit_writes;
        let load_buffered = self.settings.load_buffered;
        with_drivers(shards, || -> Result<Table, String> {
            let t = create_table(db)?;
            // The measured writes are the same sequence whenever they are generated, so the
            // pre-split can cut at their quantiles before the load (D231).
            let keys = if presplit && n > 1 && by_writes {
                split_keys_by_writes(run, n)
            } else if presplit && n > 1 {
                split_keys(load_ops, n)
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
                one_tablet_each(db, "after the pre-split")?;
            }
            for w in load_ops {
                let mut m = t.mutate(&w.row);
                // D247: a durable run loads at buffered durability, as the client runs do.
                if load_buffered {
                    m = m.durability(pigeonhole::Durability::Buffered);
                }
                put_cells(m, w.family, &w.cells).commit().map_err(err)?;
            }
            Ok(t)
        })
    }

    /// Runs `config` (writes only) the way the scaling gate measures it (D204): application-
    /// owned shards, each thread issuing its shard's share of the writes inline with
    /// [`RunOptions::in_flight`] commits in flight ([`INLINE_IN_FLIGHT`] by default). The
    /// load is applied through the blocking API first; the warmup and the measured writes
    /// run inline, the warmup unrecorded.
    pub fn run_inline(
        &self,
        config: &WorkloadConfig,
        dir: &Path,
        options_: &RunOptions,
    ) -> Result<RunRecord, String> {
        let in_flight = options_.in_flight.max(1);
        let warmup = options_.warmup_ops(config);
        let mut workload = Workload::new(WorkloadConfig {
            operations: config.operations + warmup,
            ..config.clone()
        });
        let (db, shards, worker_threads) = self.open_inline(dir)?;
        let n = shards.len();
        let (pins, placement) = pin_plan(n, options_.pin);
        let load_ops = writes(workload.load_ops())?;
        let run = writes(workload.run_ops())?;
        let load_start = Instant::now();
        let (shards, setup) = self.load_inline(&db, shards, &load_ops, &run);
        let t = setup?;
        let load = load_start.elapsed();
        let readers = self.settings.cross_readers;
        let retry = retry_policy(&self.settings);
        let inline = Inline {
            db: &db,
            t: &t,
            in_flight,
            pins: pins.as_ref(),
            batches: self.settings.cross_batches.then_some(&db),
            readers,
            retry: &retry,
        };
        let (warm, measured) = run.split_at((warmup as usize).min(run.len()));
        // D242, D247: after a buffered load the run commits at its durable level, and packing
        // re-places the tablets for it over tens of seconds; the warmup waits longer then.
        let (quiet, cap) = if self.settings.load_buffered {
            (config.quiet.warmup_after_switch, WARMUP_CAP_AFTER_SWITCH)
        } else {
            (config.quiet.warmup, WARMUP_CAP)
        };
        let presplit = self.settings.presplit;
        let (shards, warmed) = inline.warm_up(shards, warm, quiet, cap, presplit)?;
        if presplit && n > 1 {
            one_tablet_each(&db, "before the measured phase")?;
        }
        let (shards, phase) = inline.measure(
            shards,
            measured,
            options_.min_measured,
            options_.max_measured,
        )?;
        drop(t);
        let (_, closed) = with_drivers(shards, || db.close().map_err(err));
        closed?;
        for worker in worker_threads {
            worker
                .join()
                .map_err(|_| "a background worker thread panicked".to_owned())?;
        }
        let kinds = &phase.kinds;
        let hist = kinds.all();
        let ops = hist.count();
        let elapsed = phase.elapsed;
        // D247's app-owned row: singles and batches apart (a single there lands on a hot
        // row as often as the batches do, #816).
        let by_type = if self.settings.cross_batches {
            [
                ("single-commit", &kinds.single),
                ("batch-commit", &kinds.batch),
                ("reading-commit", &kinds.rmw),
            ]
            .into_iter()
            .filter(|(_, h)| h.count() > 0)
            .map(|(op, h)| OpTypeStats {
                op: op.to_owned(),
                stats: LatencyStats::of(h),
                mean_ns: h.mean().as_nanos() as u64,
            })
            .collect()
        } else {
            Vec::new()
        };
        Ok(RunRecord {
            store: self.name().to_owned(),
            store_config: format!(
                "{} {} in-flight={in_flight} {placement}{}",
                self.describe(),
                if self.settings.inline_engine_owned {
                    "engine-owned"
                } else {
                    "inline"
                },
                match (self.settings.cross_batches, readers) {
                    (true, true) => " cross-batches=20% (half read-modify-write)",
                    (true, false) => " cross-batches=20%",
                    _ => "",
                }
            ),
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
                shards: phase.shares,
                stalls: Some(phase.stalls),
                engine_commit: phase.engine_commit,
                warmup_secs: Some(warmed.secs),
                warmup_settle_secs: Some(warmed.settle_secs),
                warmup_settled: Some(warmed.settled),
                busy_retries: warmed.busy_retries + phase.busy_retries,
                capped: options_.max_measured.map(|_| phase.capped),
                by_type,
                txn_conflicts: readers.then_some(phase.kinds.conflicts),
                ..RunDetail::default()
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ANOTHER_FAILED, CROSS_BATCH_EVERY, Write, batch_extras, run_crew, split_keys_by_writes,
    };

    #[test]
    fn every_fifth_commit_batches_one_to_three_rows_from_across_the_stream() {
        // D247's app-owned row: about 20% of the commits are batches, of 2-4 rows.
        let all = writes_of(
            &(0..1000)
                .map(|i| (format!("r{i:04}"), 1))
                .collect::<Vec<_>>(),
        );
        let sizes: Vec<usize> = (0..300)
            .filter(|j| j % CROSS_BATCH_EVERY == 0)
            .map(|j| batch_extras(j, &all).count())
            .collect();
        assert_eq!(sizes.len(), 60);
        assert!(sizes.iter().all(|n| (1..=3).contains(n)));
        assert!((1..=3).all(|n| sizes.contains(&n)), "{sizes:?}");
        // The extra rows spread over the stream rather than repeating one row.
        let rows: std::collections::BTreeSet<&[u8]> = (0..300)
            .step_by(CROSS_BATCH_EVERY)
            .flat_map(|j| batch_extras(j, &all).map(|w| w.row.as_slice()))
            .collect();
        assert!(rows.len() > 100, "{}", rows.len());
    }

    fn writes_of(rows: &[(String, u64)]) -> Vec<Write> {
        rows.iter()
            .flat_map(|(r, n)| {
                (0..*n).map(move |_| Write {
                    row: r.clone().into_bytes(),
                    family: "f",
                    cells: Vec::new(),
                })
            })
            .collect()
    }

    /// The pieces' write counts for `keys` over `rows` (sorted), and which piece holds `row`.
    fn pieces(rows: &[(String, u64)], keys: &[Vec<u8>], row: &str) -> (Vec<u64>, usize) {
        let mut out = vec![0u64; keys.len() + 1];
        let mut at = 0;
        for (r, n) in rows {
            let p = keys.iter().filter(|k| k.as_slice() <= r.as_bytes()).count();
            out[p] += n;
            if r == row {
                at = p;
            }
        }
        (out, at)
    }

    #[test]
    fn the_write_split_gives_a_hot_row_a_piece_of_its_own() {
        // D231 (perf2's 16-shard finding): 1,000 rows of 10 writes and one with 700, 6.5% of
        // the writes, about a fair share. Cutting at the writes' quantiles gave its piece the
        // hot row plus a normal slice (10.3% at 16 shards).
        let mut rows: Vec<(String, u64)> = (0..1_000).map(|i| (format!("r{i:04}"), 10)).collect();
        rows[300].1 = 700;
        let keys = split_keys_by_writes(&writes_of(&rows), 16);
        assert_eq!(keys.len(), 15, "one piece per shard");
        let (weights, hot) = pieces(&rows, &keys, "r0300");
        assert_eq!(weights[hot], 700, "the hot row sits alone: {weights:?}");
        let total: u64 = weights.iter().sum();
        let bound = (700f64).max(1.2 * total as f64 / 16.0);
        assert!(
            weights.iter().all(|w| *w as f64 <= bound),
            "a piece over {bound}: {weights:?}"
        );
        // Without a hot row the pieces come out even.
        rows[300].1 = 10;
        let keys = split_keys_by_writes(&writes_of(&rows), 16);
        let (weights, _) = pieces(&rows, &keys, "r0000");
        assert_eq!(keys.len(), 15);
        // A fair share is 625 writes; rows of 10 make the heaviest piece 630.
        assert!(weights.iter().all(|w| *w <= 630), "{weights:?}");
        // As many pieces as distinct rows allow.
        let few: Vec<(String, u64)> = vec![("a".into(), 5), ("b".into(), 90), ("c".into(), 5)];
        assert_eq!(split_keys_by_writes(&writes_of(&few), 3).len(), 2);
    }
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
