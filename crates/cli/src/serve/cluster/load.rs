//! A cluster partition's load, for the balancer (D332 PR 1, ICR 0142; perf287's L1 and
//! Amendment 1 of `.coord/cli692-d332-plan.md`).
//!
//! **What's measured, per partition, on its owner:**
//! - **Request CPU**, sampled: one service call in [`CPU_EVERY`] per thread (a thread-local
//!   countdown, so an unsampled call touches nothing shared) runs as [`sampled_call`]:
//!   - [`Timed`] reads the thread's CPU clock around each `poll` (the call is async: it may
//!     wait, and resume on another thread);
//!   - the work it hands to the server's read pools (the light pool, the blocking pool)
//!     is timed on the thread that runs it ([`run_sampled`]), since a read's CPU is mostly
//!     there (simp724's M1 on #2063). The two spans never overlap: a poll that waits on the
//!     pool spends no CPU while it waits.
//!   - The sum × [`CPU_EVERY`] is added to the partition when the call ends, or is dropped.
//! - **Read keys**, sampled: one read in [`READ_EVERY`] per thread offers its table and
//!   first key to the partition's reservoir ([`READ_SLOTS`] keys, a uniform sample of the
//!   sampled reads since open: Algorithm R). Nothing on a read looks at tablet bounds: the
//!   keys are matched to `tablets()` only when the balancer asks, so a split or flush
//!   mid-window loses nothing (perf287's follow-up).
//! - **From the database:** its shard busy and group-sync nanoseconds (`Metrics`, read only
//!   when an interval is folded), and its device bytes (`Pigeonhole::device_bytes`).
//!
//! **The load fields** (`LocalPartition.load_cpu`, `.load_device`, `.load`) are
//! exponentially weighted moving averages over `--balance-window`: each interval of the
//! node's load interval (30 s) weighs `1 − e^(−interval/window)` ([`alpha`]), and a fold
//! that finds `k` intervals passed weighs its rates as `k` of them. The node's device
//! watcher folds every partition once a second if an interval has ended, so the averages
//! don't depend on how often `Status` is read. `load_device` divides by the node's ceiling
//! ([`DeviceCeiling`]): the highest one-second rate of the same counters summed over the
//! node's partitions within `--balance-window`, never an OS figure, so the counters'
//! over-count (page-cache hits) cancels out (perf287's must).

use std::cell::Cell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;

/// One call in this many, per thread, has its CPU time measured (perf287: 1 in 16).
pub(crate) const CPU_EVERY: u32 = 16;
/// One read in this many, per thread, has its key sampled (perf287: 1 in 64).
pub(crate) const READ_EVERY: u32 = 64;
/// Sampled read keys kept per partition.
pub(crate) const READ_SLOTS: usize = 256;
/// A load interval: how often the averages take a new reading.
pub(crate) const INTERVAL: Duration = Duration::from_secs(30);
/// The least ceiling a partition's `load_device` is taken over, in bytes a second (#2208):
/// the share is `rate / max(ceiling, CEILING_FLOOR)`. The ceiling is the node's own highest
/// second, so a node that only ever trickles (its map, lease and decision writes, and
/// Status reads: a few KiB/s) had a ceiling near its partitions' own rates, and a lone such
/// partition read as near 1.0: too hot, never cold (#2174). Over at least 32 MiB/s a
/// trickle reads near 0 (1 MiB/s is 0.03), the share rises smoothly from 0 with no step,
/// and `partition_max_load` (0.5) takes 16 MiB/s sustained, real work on any device. A
/// device that has run faster than this keeps its own ceiling, as before.
pub(crate) const CEILING_FLOOR: f64 = (32u64 << 20) as f64;
/// The longest `--balance-window`, in seconds (a day): it bounds the ceiling's seconds kept.
pub(crate) const MAX_WINDOW_SECS: u64 = 86_400;

thread_local! {
    static CPU_LEFT: Cell<u32> = const { Cell::new(0) };
    static READ_LEFT: Cell<u32> = const { Cell::new(0) };
    /// The reservoir's random draws (xorshift64), seeded per thread.
    static DRAW: Cell<u64> = const { Cell::new(0) };
}

tokio::task_local! {
    /// The partition a sampled call's CPU goes to, while it runs ([`sampled_call`]).
    static SAMPLED: Arc<PartitionLoad>;
}

/// A new thread's place in its sampling periods: threads take turns, so they don't all
/// sample in step.
fn phase() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed).wrapping_mul(7)
}

/// Whether this call is the thread's sampled one (`every`'s countdown in `left`).
fn take(left: &'static std::thread::LocalKey<Cell<u32>>, every: u32) -> bool {
    left.with(|c| {
        let n = c.get();
        if n == 0 {
            c.set(phase() % every + 1);
            return false;
        }
        if n == 1 {
            c.set(every);
            true
        } else {
            c.set(n - 1);
            false
        }
    })
}

/// Whether this thread's next service call is measured.
#[inline]
pub(crate) fn sample_cpu() -> bool {
    take(&CPU_LEFT, CPU_EVERY)
}

/// Whether this thread's next read's key is sampled.
#[inline]
pub(crate) fn sample_read() -> bool {
    take(&READ_LEFT, READ_EVERY)
}

/// A random number below `n` (`n` > 0), for the reservoir.
fn draw_below(n: u64) -> u64 {
    DRAW.with(|d| {
        let mut x = d.get();
        if x == 0 {
            x = u64::from(phase()).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        d.set(x);
        x % n
    })
}

/// The newest interval's weight in a moving average over `window` of readings each
/// `interval` apart: `1 − e^(−interval/window)`.
pub(crate) fn alpha(interval: Duration, window: Duration) -> f64 {
    1.0 - (-(interval.as_secs_f64() / window.as_secs_f64().max(1e-9))).exp()
}

/// A measurement seam (tests): every partition of the node in each listed cluster directory
/// reports this load in `Status` (cpu, device and their larger), in place of its measured
/// one. A balancer test that isn't about load pins it, so its decisions don't hang on how
/// busy the runner is: on a slow one, a lone partition's steady I/O measured "too hot"
/// against its node's own peak, and never cold enough to merge (#2164's Windows red). An
/// entry of `None` keeps the measured load: a test that covers the measured path says so.
#[cfg(test)]
pub(crate) static LOAD_FOR: std::sync::Mutex<Vec<(std::path::PathBuf, Option<f64>)>> =
    std::sync::Mutex::new(Vec::new());

/// The load [`LOAD_FOR`] pins for the node in `dir`, if any.
#[cfg(test)]
pub(crate) fn load_for(dir: &std::path::Path) -> Option<f64> {
    LOAD_FOR
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .find(|(d, _)| d == dir)
        .and_then(|(_, l)| *l)
}

/// The number of cores the loads' CPU share is over: read once per process.
fn cores() -> usize {
    static CORES: OnceLock<usize> = OnceLock::new();
    *CORES.get_or_init(pigeonhole_io::sys::available_cpus)
}

/// A sampled read: its table and its first key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadKey {
    pub table: String,
    pub key: Vec<u8>,
}

/// One partition's sampled measures. Shared by its service calls (the sampled ones only),
/// `Status` and the node's device watcher.
#[derive(Debug)]
pub(crate) struct PartitionLoad {
    /// This open's id: unique per open, so the device ceiling never takes a reopen's
    /// counters for an earlier open's (simp724's nit on #2063).
    pub(crate) id: u64,
    /// Sampled request CPU, already scaled by [`CPU_EVERY`], in nanoseconds since open.
    cpu_nanos: AtomicU64,
    /// Two reservoirs of sampled read keys, the current one and the window before it
    /// (perf287 on #2063): [`Self::rotate`] swaps them once a `--balance-window`, so split
    /// keys come from the window, not from every read since open.
    reservoirs: [Reservoir; 2],
    /// Which of `reservoirs` takes reads now (0 or 1).
    current: AtomicUsize,
    window: Mutex<Window>,
}

/// One window's sampled read keys (Algorithm R) and how many sampled reads it saw.
#[derive(Debug)]
struct Reservoir {
    reads: AtomicU64,
    slots: Box<[ArcSwapOption<ReadKey>]>,
}

impl Default for Reservoir {
    fn default() -> Self {
        Self {
            reads: AtomicU64::new(0),
            slots: (0..READ_SLOTS).map(|_| ArcSwapOption::empty()).collect(),
        }
    }
}

/// The reads a current reservoir needs before [`PartitionLoad::read_keys`] takes it over
/// the window before (perf287).
const ENOUGH_READS: u64 = 1_024;

impl Default for PartitionLoad {
    fn default() -> Self {
        static OPENS: AtomicU64 = AtomicU64::new(1);
        Self {
            id: OPENS.fetch_add(1, Ordering::Relaxed),
            cpu_nanos: AtomicU64::new(0),
            reservoirs: [Reservoir::default(), Reservoir::default()],
            current: AtomicUsize::new(0),
            window: Mutex::new(Window::default()),
        }
    }
}

/// The counters as one interval ends.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Reading {
    /// The database's shard busy nanoseconds (summed over its shards).
    pub busy_nanos: u64,
    /// Its group-sync nanoseconds (summed over its shards).
    pub sync_nanos: u64,
    /// Its device bytes, read plus written.
    pub device_bytes: u64,
    /// The written ones alone (D331 PR 5's plan, `LocalPartition.write_bytes_rate`).
    pub written_bytes: u64,
}

#[derive(Debug, Default)]
struct Window {
    /// When the current interval began, and the counters then (`None` before the first).
    start: Option<(Instant, Reading, u64)>,
    cpu: f64,
    device_rate: f64,
    write_rate: f64,
    intervals: u64,
    /// When the current reservoir began (perf287's nit on #2103: by time, so a fold after a
    /// gap doesn't stretch the window).
    rotated_at: Option<Instant>,
}

/// A partition's folded load (`LocalPartition`'s fields).
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub(crate) struct Load {
    pub cpu: f64,
    pub device: f64,
    /// Its device bytes written a second, the same average: `None` until the first
    /// interval ends (unknown, where 0 is idle).
    pub write_rate: Option<f64>,
}

impl Load {
    pub(crate) fn load(&self) -> f64 {
        self.cpu.max(self.device)
    }
}

impl PartitionLoad {
    /// Adds a sampled call's CPU nanoseconds, scaled.
    pub(crate) fn add_cpu(&self, nanos: u64) {
        self.cpu_nanos.fetch_add(
            nanos.saturating_mul(u64::from(CPU_EVERY)),
            Ordering::Relaxed,
        );
    }

    /// Offers a sampled read's key to the reservoir (Algorithm R): the first
    /// [`READ_SLOTS`] fill it, and the `n`th replaces a random one with chance
    /// `READ_SLOTS / n`, so the keys kept are a uniform sample of every sampled read.
    pub(crate) fn add_read(&self, table: &str, key: &[u8]) {
        // Acquire, with rotate's Release: a read that sees the new reservoir sees it emptied
        // (perf287's nit on #2103). A read that took the old one just before can still land
        // there: one sampled key, in the window it belongs to.
        let r = &self.reservoirs[self.current.load(Ordering::Acquire) & 1];
        let n = r.reads.fetch_add(1, Ordering::Relaxed);
        let slot = if n < READ_SLOTS as u64 {
            n
        } else {
            draw_below(n + 1)
        };
        if let Some(s) = r.slots.get(usize::try_from(slot).unwrap_or(usize::MAX)) {
            s.store(Some(Arc::new(ReadKey {
                table: table.to_owned(),
                key: key.to_vec(),
            })));
        }
    }

    /// Starts a new window's reservoir: the current one becomes the window before, and the
    /// one before is emptied to take reads (perf287). Called by the fold once a
    /// `--balance-window`.
    fn rotate(&self) {
        let next = (self.current.load(Ordering::Acquire) + 1) & 1;
        let r = &self.reservoirs[next];
        for s in &r.slots {
            s.store(None);
        }
        r.reads.store(0, Ordering::Relaxed);
        self.current.store(next, Ordering::Release);
    }

    /// The read keys of the current window, or of the window before while the current one
    /// has seen fewer than [`ENOUGH_READS`] (perf287), and how many reads each stands for
    /// (its window's sampled reads, times [`READ_EVERY`], over the keys kept).
    pub(crate) fn read_keys(&self) -> (Vec<ReadKey>, f64) {
        let cur = self.current.load(Ordering::Acquire) & 1;
        let r = if self.reservoirs[cur].reads.load(Ordering::Relaxed) >= ENOUGH_READS {
            &self.reservoirs[cur]
        } else {
            &self.reservoirs[cur ^ 1]
        };
        let keys: Vec<ReadKey> = r
            .slots
            .iter()
            .filter_map(|s| s.load_full().map(|k| (*k).clone()))
            .collect();
        let reads = r.reads.load(Ordering::Relaxed) as f64 * f64::from(READ_EVERY);
        let each = if keys.is_empty() {
            0.0
        } else {
            reads / keys.len() as f64
        };
        (keys, each)
    }

    /// Whether an interval of `interval` has ended by `now` (or none began): a fold would
    /// take a new reading.
    pub(crate) fn due(&self, now: Instant, interval: Duration) -> bool {
        self.window
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .start
            .is_none_or(|(at, _, _)| now.saturating_duration_since(at) >= interval)
    }

    /// Folds the intervals of `interval` that have ended by `now` into the averages, from
    /// `reading` (the database's counters now): the `k` intervals passed since the last
    /// reading weigh as `k`, each `alpha`. With the node's `cores` and device `ceiling`
    /// (bytes per second, 0 if none measured yet); returns the averages. A call within an
    /// interval changes nothing.
    pub(crate) fn fold(
        &self,
        now: Instant,
        reading: Reading,
        cores: usize,
        ceiling: f64,
        interval: Duration,
        alpha: f64,
    ) -> Load {
        let cpu_now = self.cpu_nanos.load(Ordering::Relaxed);
        let mut w = self.window.lock().unwrap_or_else(PoisonError::into_inner);
        match w.start {
            None => w.start = Some((now, reading, cpu_now)),
            Some((at, then, cpu_then)) if now.saturating_duration_since(at) >= interval => {
                let elapsed = now.saturating_duration_since(at);
                let secs = elapsed.as_secs_f64();
                let ns = secs * 1e9;
                let busy = reading.busy_nanos.saturating_sub(then.busy_nanos) as f64 / ns;
                let sync = reading.sync_nanos.saturating_sub(then.sync_nanos) as f64 / ns;
                let req = cpu_now.saturating_sub(cpu_then) as f64 / ns;
                let cpu = (busy.max(sync) + req) / cores.max(1) as f64;
                let rate = reading.device_bytes.saturating_sub(then.device_bytes) as f64 / secs;
                let written =
                    reading.written_bytes.saturating_sub(then.written_bytes) as f64 / secs;
                if w.intervals == 0 {
                    (w.cpu, w.device_rate, w.write_rate) = (cpu, rate, written);
                } else {
                    let k = (secs / interval.as_secs_f64()).floor().max(1.0);
                    let a = 1.0 - (1.0 - alpha).powf(k);
                    w.cpu = a * cpu + (1.0 - a) * w.cpu;
                    w.device_rate = a * rate + (1.0 - a) * w.device_rate;
                    w.write_rate = a * written + (1.0 - a) * w.write_rate;
                }
                w.intervals += 1;
                w.start = Some((now, reading, cpu_now));
                // A new window's reservoir once a window has passed (perf287). alpha is
                // 1 - e^(-interval/window): the window, back from it.
                let ratio = -(1.0 - alpha).ln();
                if ratio > 0.0 {
                    let window = interval.mul_f64(1.0 / ratio);
                    let began = *w.rotated_at.get_or_insert(now);
                    if now.saturating_duration_since(began) >= window {
                        self.rotate();
                        w.rotated_at = Some(now);
                    }
                }
            }
            Some(_) => {}
        }
        Self::averages(&w, ceiling)
    }

    /// The averages as last folded, over `ceiling`.
    pub(crate) fn current(&self, ceiling: f64) -> Load {
        Self::averages(
            &self.window.lock().unwrap_or_else(PoisonError::into_inner),
            ceiling,
        )
    }

    fn averages(w: &Window, ceiling: f64) -> Load {
        if w.intervals == 0 {
            return Load::default();
        }
        Load {
            cpu: w.cpu,
            // Over the ceiling, but never one under the floor (#2208).
            device: (w.device_rate / ceiling.max(CEILING_FLOOR)).min(1.0),
            write_rate: Some(w.write_rate),
        }
    }
}

/// Runs a sampled call (`inner`) for partition `load`: its polls timed ([`Timed`]), and the
/// work it hands to the read pools timed where it runs ([`run_sampled`]).
pub(crate) fn sampled_call<F: Future>(
    load: Arc<PartitionLoad>,
    inner: F,
) -> impl Future<Output = F::Output> {
    SAMPLED.scope(Arc::clone(&load), Timed::new(inner, load))
}

/// The partition of the sampled call running on this task, if it's one: what a read pool
/// times its work for. Taken where the work is handed off, on the call's task.
pub(crate) fn sampled() -> Option<Arc<PartitionLoad>> {
    SAMPLED.try_with(Arc::clone).ok()
}

/// Runs `f`, adding its thread CPU time to `load` if a sampled call handed it here
/// ([`sampled`]); just runs it otherwise.
pub(crate) fn run_sampled<T>(load: Option<&Arc<PartitionLoad>>, f: impl FnOnce() -> T) -> T {
    let Some(load) = load else { return f() };
    let t0 = pigeonhole_io::sys::thread_cpu_nanos();
    let out = f();
    load.add_cpu(pigeonhole_io::sys::thread_cpu_nanos().saturating_sub(t0));
    out
}

/// A future whose `poll`s' thread CPU time is summed, then added to a partition when it's
/// ready, or dropped unfinished (a cancelled call's CPU was spent all the same) (perf287's
/// must: per poll, since the call may wait and resume on another thread).
pub(crate) struct Timed<F> {
    inner: Pin<Box<F>>,
    load: Arc<PartitionLoad>,
    nanos: u64,
    added: bool,
}

impl<F> Timed<F> {
    pub(crate) fn new(inner: F, load: Arc<PartitionLoad>) -> Self {
        Self {
            inner: Box::pin(inner),
            load,
            nanos: 0,
            added: false,
        }
    }
}

impl<F: Future> Future for Timed<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let t0 = pigeonhole_io::sys::thread_cpu_nanos();
        let out = self.inner.as_mut().poll(cx);
        let spent = pigeonhole_io::sys::thread_cpu_nanos().saturating_sub(t0);
        self.nanos = self.nanos.saturating_add(spent);
        if out.is_ready() && !self.added {
            self.added = true;
            self.load.add_cpu(self.nanos);
        }
        out
    }
}

impl<F> Drop for Timed<F> {
    fn drop(&mut self) {
        if !self.added && self.nanos > 0 {
            self.load.add_cpu(self.nanos);
        }
    }
}

/// The node's device ceiling (perf287's must): the highest one-second rate of its
/// partitions' device bytes, summed, within the window. Each reading adds the partitions'
/// rises since the last one, by partition open: one opened since adds nothing until its
/// next reading, so its open (the manifest's reads, a WAL replay) never counts as a
/// second's rate (simp724's S1 on #2063).
#[derive(Debug)]
pub(crate) struct DeviceCeiling {
    window: Duration,
    inner: Mutex<Ceiling>,
}

#[derive(Debug, Default)]
struct Ceiling {
    /// When the last rate was taken (`None` before the first reading).
    last_at: Option<Instant>,
    /// Bytes risen since `last_at`, not yet in a rate.
    pending: u64,
    /// Each open partition's total at the last reading, by its open (an id the caller
    /// keeps unique per open).
    totals: HashMap<u64, u64>,
    /// Each second's rate, with when it ended, oldest first.
    rates: std::collections::VecDeque<(Instant, f64)>,
}

impl DeviceCeiling {
    pub(crate) fn new(window: Duration) -> Self {
        Self {
            window,
            inner: Mutex::new(Ceiling::default()),
        }
    }

    /// Takes the device bytes of every partition open now (`(open, total)`) at `now`,
    /// called about once a second: a second or more after the last rate, the rises since
    /// join the window as one.
    pub(crate) fn note(&self, now: Instant, parts: &[(u64, u64)]) {
        let mut c = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let risen: u64 = parts
            .iter()
            .filter_map(|(open, total)| c.totals.get(open).map(|t| total.saturating_sub(*t)))
            .sum();
        c.totals = parts.iter().copied().collect();
        match c.last_at {
            None => c.last_at = Some(now),
            Some(at) => {
                c.pending = c.pending.saturating_add(risen);
                let elapsed = now.saturating_duration_since(at);
                if elapsed >= Duration::from_secs(1) {
                    let rate = c.pending as f64 / elapsed.as_secs_f64();
                    c.rates.push_back((now, rate));
                    c.pending = 0;
                    c.last_at = Some(now);
                }
            }
        }
        while c
            .rates
            .front()
            .is_some_and(|(at, _)| now.saturating_duration_since(*at) > self.window)
        {
            c.rates.pop_front();
        }
    }

    /// The highest one-second rate in the window, in bytes per second (0 before any).
    pub(crate) fn get(&self) -> f64 {
        let c = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        c.rates.iter().map(|(_, r)| *r).fold(0.0, f64::max)
    }
}

impl super::Node {
    /// A partition's load now (`LocalPartition.load_*`): folded first if an interval has
    /// ended, reading the database's counters only then (D332).
    pub(crate) fn partition_load(&self, part: &crate::serve::service::Partition) -> Load {
        let now = Instant::now();
        let ceiling = self.device_ceiling.get();
        let interval = self.peers.load_interval;
        if !part.load.due(now, interval) {
            return part.load.current(ceiling);
        }
        let m = part.db.engine_metrics();
        let (read, written) = part.db.device_bytes();
        let reading = Reading {
            busy_nanos: m.shard_busy_nanos,
            sync_nanos: m.group_sync_nanos,
            device_bytes: read.saturating_add(written),
            written_bytes: written,
        };
        part.load.fold(
            now,
            reading,
            cores(),
            ceiling,
            interval,
            alpha(interval, self.peers.balance_window),
        )
    }

    /// Once a second for as long as the node runs: the device ceiling's reading of every
    /// partition open here, and each partition's fold if its interval has ended (D332). One
    /// loop per node; it holds the node only while it reads.
    pub(crate) fn watch_device(self: &Arc<Self>) {
        if self
            .watching_device
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        // The thread CPU clock's first read measures its rate on Windows (a 10 ms spin):
        // done here, once at the node's start, never on a sampled request (perf287 on #2063).
        let _ = pigeonhole_io::sys::thread_cpu_nanos();
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            self.watching_device
                .store(false, std::sync::atomic::Ordering::Release);
            return;
        };
        let me = Arc::downgrade(self);
        rt.spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(node) = me.upgrade() else { return };
                let Some(shared) = node.server.get().and_then(|o| o.shared.upgrade()) else {
                    continue;
                };
                let parts = shared.router.partitions();
                // Each partition by its open's id (simp724's nit on #2063).
                let totals: Vec<(u64, u64)> = parts
                    .iter()
                    .map(|(_, p)| {
                        let (r, w) = p.db.device_bytes();
                        (p.load.id, r.saturating_add(w))
                    })
                    .collect();
                node.device_ceiling.note(Instant::now(), &totals);
                for (_, p) in &parts {
                    node.partition_load(p);
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: f64 = 0.2;

    #[test]
    fn a_thread_samples_one_call_in_its_period() {
        let n = (0..CPU_EVERY * 100).filter(|_| sample_cpu()).count();
        assert!((99..=100).contains(&n), "{n}");
        let n = (0..READ_EVERY * 10).filter(|_| sample_read()).count();
        assert!((9..=10).contains(&n), "{n}");
    }

    #[test]
    fn the_window_sets_each_intervals_weight() {
        // simp724's S2: a 300 s window of 30 s intervals weighs each new one 1 − e^−0.1.
        let a = alpha(INTERVAL, Duration::from_secs(300));
        assert!((a - (1.0 - (-0.1f64).exp())).abs() < 1e-12, "{a}");
        assert!(alpha(INTERVAL, Duration::from_secs(60)) > a);
    }

    #[test]
    fn the_load_folds_once_an_interval_and_averages() {
        let l = PartitionLoad::default();
        let t0 = Instant::now();
        let fold = |at, r, ceiling| l.fold(at, r, 4, ceiling, INTERVAL, A);
        assert_eq!(fold(t0, Reading::default(), 0.0), Load::default());
        // 2 s of busy in a 30 s interval over 4 cores, plus 1 s of sampled request CPU.
        l.add_cpu(1_000_000_000 / u64::from(CPU_EVERY));
        let r = Reading {
            busy_nanos: 2_000_000_000,
            sync_nanos: 1_000_000_000,
            device_bytes: 1920 << 20,
            written_bytes: 15 << 20,
        };
        let t1 = t0 + INTERVAL;
        // Within the interval: nothing yet.
        assert!(!l.due(t1 - Duration::from_secs(1), INTERVAL));
        assert_eq!(fold(t1 - Duration::from_secs(1), r, 0.0), Load::default());
        assert!(l.due(t1, INTERVAL));
        let ceiling = 256.0 * (1 << 20) as f64;
        let got = fold(t1, r, ceiling);
        assert!((got.cpu - 3.0 / 30.0 / 4.0).abs() < 1e-9, "{got:?}");
        // 64 MiB/s over a 256 MiB/s ceiling (over `CEILING_FLOOR`).
        assert!((got.device - 0.25).abs() < 1e-9, "{got:?}");
        // D331 PR 5: half of it written, in bytes a second (no ceiling).
        assert_eq!(got.write_rate, Some(512.0 * 1024.0), "{got:?}");
        assert_eq!(got.load(), got.device);
        assert_eq!(l.current(ceiling), got);
        // An idle interval pulls the average down by α.
        let idle = fold(t1 + INTERVAL, r, ceiling);
        assert!((idle.device - 0.25 * (1.0 - A)).abs() < 1e-9, "{idle:?}");
        let w = idle.write_rate.unwrap();
        assert!((w - 512.0 * 1024.0 * (1.0 - A)).abs() < 1e-6, "{idle:?}");
        // Three idle intervals in one fold weigh as three (simp724's S3), not one.
        let three = fold(t1 + INTERVAL * 4, r, ceiling);
        assert!(
            (three.device - 0.25 * (1.0 - A).powi(4)).abs() < 1e-9,
            "{three:?}"
        );
    }

    /// A partition's device share after one interval at a steady `rate`, on a node whose
    /// ceiling is `ceiling` (bytes a second).
    fn share_at(rate: f64, ceiling: f64) -> Load {
        let l = PartitionLoad::default();
        let t0 = Instant::now();
        l.fold(t0, Reading::default(), 4, 0.0, INTERVAL, A);
        let r = Reading {
            device_bytes: (rate * INTERVAL.as_secs_f64()) as u64,
            ..Reading::default()
        };
        l.fold(t0 + INTERVAL, r, 4, ceiling, INTERVAL, A)
    }

    /// #2208 (#2174's cliff): a partition alone on its node whose steady trickle is the
    /// node's whole ceiling reads near 0, over `CEILING_FLOOR`, not near 1.0; and the share
    /// rises smoothly through 1 MiB/s, where 1.3.1's rate floor stepped from 0 to the whole
    /// ceiling.
    #[test]
    fn a_trickle_at_its_own_ceiling_reads_near_zero_and_the_share_has_no_step() {
        // Its own steady rate is the node's highest second.
        let own = |rate: f64| share_at(rate, rate).device;
        let trickle = own(8.0 * 1024.0);
        assert!(trickle > 0.0 && trickle < 0.001, "{trickle}");
        let mib = (1u64 << 20) as f64;
        let (under, over) = (own(mib - 4096.0), own(mib + 4096.0));
        assert!(
            (under - (mib - 4096.0) / CEILING_FLOOR).abs() < 1e-12,
            "{under}"
        );
        assert!(over - under < 0.001, "no step: {under} then {over}");
        assert!(over < 0.04, "1 MiB/s is a small share: {over}");
        // `partition_max_load` (0.5) is 16 MiB/s sustained at the floor.
        assert!((own(16.0 * mib) - 0.5).abs() < 1e-12);
        // At the floor and over it, the whole share, as before.
        assert!((own(CEILING_FLOOR) - 1.0).abs() < 1e-12);
        assert!((own(4.0 * CEILING_FLOOR) - 1.0).abs() < 1e-12);
    }

    /// #2208: a node whose device has run faster than `CEILING_FLOOR` keeps its own
    /// ceiling: its partitions' shares are exactly `rate / ceiling`, as before the floor.
    #[test]
    fn a_fast_devices_shares_are_over_its_own_ceiling_as_before() {
        let mib = (1u64 << 20) as f64;
        for (rate, ceiling) in [
            (256.0 * mib, 1024.0 * mib),
            (1.0 * mib, 2048.0 * mib),
            (512.0 * mib, 512.0 * mib),
            (40.0 * mib, 33.0 * mib),
        ] {
            let got = share_at(rate, ceiling);
            assert_eq!(
                got.device,
                (rate / ceiling).min(1.0),
                "{rate} over {ceiling}"
            );
            assert_eq!(got.load(), got.device);
        }
    }

    #[test]
    fn the_reservoir_keeps_a_uniform_sample_of_every_read() {
        // Algorithm R: of 256 × 40 reads, those kept come from the whole run, not only the
        // last 256; each stands for its share of every read.
        let l = PartitionLoad::default();
        let n = READ_SLOTS * 40;
        for i in 0..n {
            l.add_read("t", format!("k{i:06}").as_bytes());
        }
        let (keys, each) = l.read_keys();
        assert_eq!(keys.len(), READ_SLOTS);
        let total = n as f64 * f64::from(READ_EVERY);
        assert!((each * keys.len() as f64 - total).abs() < 1e-6);
        let first_half = keys
            .iter()
            .filter(|k| k.key.as_slice() < format!("k{:06}", n / 2).as_bytes())
            .count();
        assert!(
            (64..=192).contains(&first_half),
            "{first_half} of {READ_SLOTS} from the first half"
        );
    }

    #[test]
    fn a_late_hot_key_fills_the_reservoir_within_two_windows() {
        // perf287 on #2063: the reservoir is per window, so a key that turns hot late is the
        // read keys' majority once its window has enough reads, not diluted by every read
        // since open.
        let l = PartitionLoad::default();
        for i in 0..READ_SLOTS * 40 {
            l.add_read("t", format!("k{i:06}").as_bytes());
        }
        l.rotate();
        // A new window with too few reads yet: the window before stands.
        for _ in 0..10 {
            l.add_read("t", b"hot");
        }
        let (keys, _) = l.read_keys();
        assert!(keys.iter().all(|k| k.key != b"hot"), "the window before");
        for i in 0..ENOUGH_READS * 2 {
            let key = if i % 10 == 0 {
                format!("k{i:06}").into_bytes()
            } else {
                b"hot".to_vec()
            };
            l.add_read("t", &key);
        }
        let (keys, each) = l.read_keys();
        let hot = keys.iter().filter(|k| k.key == b"hot").count();
        assert!(hot * 4 > keys.len() * 3, "{hot} of {} keys hot", keys.len());
        let reads = (ENOUGH_READS * 2 + 10) as f64 * f64::from(READ_EVERY);
        assert!(
            (each * keys.len() as f64 - reads).abs() < 1e-6,
            "this window's reads"
        );
    }

    /// `n` steps of work the optimiser can't fold away.
    fn busy(n: u64) -> u64 {
        let mut x = 0u64;
        for i in 0..n {
            x = std::hint::black_box(x.wrapping_add(i.wrapping_mul(i)));
        }
        x
    }

    #[test]
    fn a_timed_future_adds_its_polls_cpu_when_ready_or_dropped() {
        let l = Arc::new(PartitionLoad::default());
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(Timed::new(async { busy(2_000_000) }, Arc::clone(&l)));
        let done = l.cpu_nanos.load(Ordering::Relaxed);
        assert!(done > 0);
        // Dropped after one poll: its CPU still counts.
        let mut fut = Box::pin(Timed::new(
            async {
                busy(2_000_000);
                std::future::pending::<()>().await;
            },
            Arc::clone(&l),
        ));
        let waker = std::task::Waker::noop();
        assert!(
            fut.as_mut()
                .poll(&mut Context::from_waker(waker))
                .is_pending()
        );
        drop(fut);
        assert!(l.cpu_nanos.load(Ordering::Relaxed) > done);
    }

    #[test]
    fn a_sampled_calls_pool_work_counts_once_where_it_runs() {
        // simp724's M1: a sampled call's read on a pool thread counts there, and the
        // call's own polls (which only wait for it) add next to nothing: the total is about
        // the closure's CPU, never twice it.
        let l = Arc::new(PartitionLoad::default());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let closure_ns = Arc::new(AtomicU64::new(0));
        let ns = Arc::clone(&closure_ns);
        rt.block_on(sampled_call(Arc::clone(&l), async move {
            let load = sampled();
            assert!(load.is_some(), "the call's task carries its partition");
            tokio::task::spawn_blocking(move || {
                run_sampled(load.as_ref(), || {
                    let t0 = pigeonhole_io::sys::thread_cpu_nanos();
                    busy(20_000_000);
                    ns.store(
                        pigeonhole_io::sys::thread_cpu_nanos() - t0,
                        Ordering::Relaxed,
                    );
                });
            })
            .await
            .unwrap();
        }));
        assert!(sampled().is_none(), "outside a sampled call");
        let total = l.cpu_nanos.load(Ordering::Relaxed) as f64 / f64::from(CPU_EVERY);
        let closure = closure_ns.load(Ordering::Relaxed) as f64;
        assert!(closure > 0.0);
        assert!(
            total >= closure * 0.9 && total < closure * 1.5,
            "counted {total} ns for a closure of {closure} ns"
        );
    }

    #[test]
    fn the_ceiling_is_the_highest_second_within_the_window() {
        let c = DeviceCeiling::new(Duration::from_secs(10));
        let t = Instant::now();
        c.note(t, &[(1, 0)]);
        c.note(t + Duration::from_secs(1), &[(1, 5 << 20)]);
        c.note(t + Duration::from_secs(2), &[(1, 6 << 20)]);
        assert!((c.get() - (5 << 20) as f64).abs() < 1.0);
        // Past the window the fast second is forgotten.
        c.note(t + Duration::from_secs(13), &[(1, 7 << 20)]);
        assert!(c.get() < (5 << 20) as f64, "{}", c.get());
    }

    #[test]
    fn a_partition_opened_since_adds_nothing_until_its_next_reading() {
        // simp724's S1: a partition that opens with 500 MiB already read (its manifest, a
        // replayed WAL) brings no spike; its rise after counts.
        let c = DeviceCeiling::new(Duration::from_secs(60));
        let t = Instant::now();
        c.note(t, &[(1, 0)]);
        c.note(t + Duration::from_secs(1), &[(1, 1 << 20), (2, 500 << 20)]);
        assert!((c.get() - (1 << 20) as f64).abs() < 1.0, "{}", c.get());
        c.note(
            t + Duration::from_secs(2),
            &[(1, 2 << 20), (2, (500 << 20) + (3 << 20))],
        );
        assert!((c.get() - (4 << 20) as f64).abs() < 1.0, "{}", c.get());
        // Closed and opened again (a new open id): its total starts over, no spike.
        c.note(t + Duration::from_secs(3), &[(1, 3 << 20), (3, 900 << 20)]);
        assert!((c.get() - (4 << 20) as f64).abs() < 1.0, "{}", c.get());
    }
}
