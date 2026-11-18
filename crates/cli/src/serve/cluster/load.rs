//! A cluster partition's load, for the balancer (D332 PR 1, ICR 0142; perf287's L1 and
//! Amendment 1 of `.coord/cli692-d332-plan.md`).
//!
//! **What's measured, per partition, on its owner:**
//! - **Request CPU**, sampled: one service call in [`CPU_EVERY`] per thread (a thread-local
//!   countdown, so an unsampled call touches nothing shared) runs under [`Timed`], which reads
//!   the thread's CPU clock around each `poll` (the call is async: it may wait, and resume on
//!   another thread) and, when it's done, adds the sum × [`CPU_EVERY`] to the partition.
//! - **Read keys**, sampled: one read in [`READ_EVERY`] per thread puts its table and first
//!   key in the partition's reservoir ([`READ_SLOTS`] slots, overwritten in turn). Nothing on
//!   a read looks at tablet bounds: the keys are matched to `tablets()` only when the balancer
//!   asks, so a split or flush mid-window loses nothing (perf287's follow-up).
//! - **From the database:** its shard busy and group-sync nanoseconds (`Metrics`), and its
//!   device bytes (`Pigeonhole::device_bytes`), read when an interval is folded.
//!
//! **The load fields** (`LocalPartition.load_cpu`, `.load_device`, `.load`) are moving
//! averages over intervals of [`INTERVAL`], the newest weighted [`ALPHA`] (D214's), folded
//! lazily when `Status` reads them. `load_device` divides by the node's ceiling
//! ([`DeviceCeiling`]): the highest one-second rate of the same counters summed over the
//! node's partitions within `--balance-window`, never an OS figure, so the counters'
//! over-count (page-cache hits) cancels out (perf287's must).

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;

/// One call in this many, per thread, has its CPU time measured (perf287: 1 in 16).
pub(crate) const CPU_EVERY: u32 = 16;
/// One read in this many, per thread, has its key sampled (perf287: 1 in 64).
pub(crate) const READ_EVERY: u32 = 64;
/// Sampled read keys kept per partition.
pub(crate) const READ_SLOTS: usize = 256;
/// A load interval.
pub(crate) const INTERVAL: Duration = Duration::from_secs(30);
/// The newest interval's weight in the moving averages (D214's 0.2).
pub(crate) const ALPHA: f64 = 0.2;

thread_local! {
    static CPU_LEFT: Cell<u32> = const { Cell::new(0) };
    static READ_LEFT: Cell<u32> = const { Cell::new(0) };
}

/// Whether this call is the thread's sampled one (`every`'s countdown in `left`). The first
/// call on a thread starts the countdown somewhere in its period, so threads don't all sample
/// in step.
fn take(left: &'static std::thread::LocalKey<Cell<u32>>, every: u32) -> bool {
    left.with(|c| {
        let n = c.get();
        if n == 0 {
            // A thread's first call: start at a point derived from its stack address.
            let seed = (std::ptr::from_ref(c) as usize >> 4) as u32;
            c.set(seed % every + 1);
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

/// A sampled read: its table and its first key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadKey {
    pub table: String,
    pub key: Vec<u8>,
}

/// One partition's sampled measures. Shared by its service calls (the sampled ones only)
/// and `Status`.
#[derive(Debug)]
pub(crate) struct PartitionLoad {
    /// Sampled request CPU, already scaled by [`CPU_EVERY`], in nanoseconds since open.
    cpu_nanos: AtomicU64,
    /// Sampled reads since open (each standing for [`READ_EVERY`]).
    reads: AtomicU64,
    slots: Box<[ArcSwapOption<ReadKey>]>,
    window: Mutex<Window>,
}

impl Default for PartitionLoad {
    fn default() -> Self {
        Self {
            cpu_nanos: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            slots: (0..READ_SLOTS).map(|_| ArcSwapOption::empty()).collect(),
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
}

#[derive(Debug, Default)]
struct Window {
    /// When the current interval began, and the counters then (`None` before the first).
    start: Option<(Instant, Reading, u64)>,
    cpu: f64,
    device_rate: f64,
    intervals: u64,
}

/// A partition's folded load (`LocalPartition`'s fields).
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub(crate) struct Load {
    pub cpu: f64,
    pub device: f64,
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

    /// Keeps a sampled read's key.
    pub(crate) fn add_read(&self, table: &str, key: &[u8]) {
        let n = self.reads.fetch_add(1, Ordering::Relaxed);
        self.slots[(n % READ_SLOTS as u64) as usize].store(Some(Arc::new(ReadKey {
            table: table.to_owned(),
            key: key.to_vec(),
        })));
    }

    /// The read keys kept now, and how many reads each stands for (all sampled reads since
    /// open, times [`READ_EVERY`], over the keys kept).
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "D332 PR 3's balancer picks split keys from it")
    )]
    pub(crate) fn read_keys(&self) -> (Vec<ReadKey>, f64) {
        let keys: Vec<ReadKey> = self
            .slots
            .iter()
            .filter_map(|s| s.load_full().map(|k| (*k).clone()))
            .collect();
        let reads = self.reads.load(Ordering::Relaxed) as f64 * f64::from(READ_EVERY);
        let each = if keys.is_empty() {
            0.0
        } else {
            reads / keys.len() as f64
        };
        (keys, each)
    }

    /// Folds the intervals that have ended by `now` into the averages, from `reading` (the
    /// database's counters now), with the node's `cores` and device `ceiling` (bytes per
    /// second, 0 if none measured yet); returns the averages. A call within an interval
    /// changes nothing.
    pub(crate) fn fold(&self, now: Instant, reading: Reading, cores: usize, ceiling: f64) -> Load {
        let cpu_now = self.cpu_nanos.load(Ordering::Relaxed);
        let mut w = self.window.lock().unwrap_or_else(PoisonError::into_inner);
        match w.start {
            None => w.start = Some((now, reading, cpu_now)),
            Some((at, then, cpu_then)) if now.duration_since(at) >= INTERVAL => {
                let secs = now.duration_since(at).as_secs_f64();
                let ns = secs * 1e9;
                let busy = reading.busy_nanos.saturating_sub(then.busy_nanos) as f64 / ns;
                let sync = reading.sync_nanos.saturating_sub(then.sync_nanos) as f64 / ns;
                let req = cpu_now.saturating_sub(cpu_then) as f64 / ns;
                let cpu = (busy.max(sync) + req) / cores.max(1) as f64;
                let rate = reading.device_bytes.saturating_sub(then.device_bytes) as f64 / secs;
                if w.intervals == 0 {
                    (w.cpu, w.device_rate) = (cpu, rate);
                } else {
                    w.cpu = ALPHA * cpu + (1.0 - ALPHA) * w.cpu;
                    w.device_rate = ALPHA * rate + (1.0 - ALPHA) * w.device_rate;
                }
                w.intervals += 1;
                w.start = Some((now, reading, cpu_now));
            }
            Some(_) => {}
        }
        if w.intervals == 0 {
            return Load::default();
        }
        Load {
            cpu: w.cpu,
            device: if ceiling > 0.0 {
                (w.device_rate / ceiling).min(1.0)
            } else {
                0.0
            },
        }
    }
}

/// A future whose `poll`s' thread CPU time is summed, then added to a partition when it's
/// ready (perf287's must: per poll, since the call may wait and resume on another thread).
pub(crate) struct Timed<F> {
    inner: Pin<Box<F>>,
    load: Arc<PartitionLoad>,
    nanos: u64,
}

impl<F> Timed<F> {
    pub(crate) fn new(inner: F, load: Arc<PartitionLoad>) -> Self {
        Self {
            inner: Box::pin(inner),
            load,
            nanos: 0,
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
        if out.is_ready() {
            self.load.add_cpu(self.nanos);
        }
        out
    }
}

/// The node's device ceiling (perf287's must): the highest one-second rate of its
/// partitions' device bytes, summed, within the window.
#[derive(Debug)]
pub(crate) struct DeviceCeiling {
    window: Duration,
    inner: Mutex<Ceiling>,
}

#[derive(Debug, Default)]
struct Ceiling {
    /// The last one-second reading: when, and the summed bytes then.
    last: Option<(Instant, u64)>,
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

    /// Takes the node's summed device bytes at `now` (called about once a second): a
    /// second or more after the last reading, its rate joins the window.
    pub(crate) fn note(&self, now: Instant, bytes: u64) {
        let mut c = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        match c.last {
            Some((at, then)) if now.duration_since(at) >= Duration::from_secs(1) => {
                let rate = bytes.saturating_sub(then) as f64 / now.duration_since(at).as_secs_f64();
                c.rates.push_back((now, rate));
                c.last = Some((now, bytes));
            }
            Some(_) => {}
            None => c.last = Some((now, bytes)),
        }
        while c
            .rates
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > self.window)
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
    /// A partition's load now (`LocalPartition.load_*`): its sampled measures and its
    /// database's counters, folded if an interval has ended (D332).
    pub(crate) fn partition_load(&self, part: &crate::serve::service::Partition) -> Load {
        let m = part.db.engine_metrics();
        let (read, written) = part.db.device_bytes();
        let reading = Reading {
            busy_nanos: m.shard_busy_nanos,
            sync_nanos: m.group_sync_nanos,
            device_bytes: read.saturating_add(written),
        };
        part.load.fold(
            Instant::now(),
            reading,
            pigeonhole_io::sys::available_cpus(),
            self.device_ceiling.get(),
        )
    }

    /// The node's device ceiling's readings, once a second for as long as the node runs: the
    /// device bytes of every partition open here, summed (D332). One loop per node; it holds
    /// the node only while it reads.
    pub(crate) fn watch_device(self: &Arc<Self>) {
        if self
            .watching_device
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
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
                let bytes: u64 = shared
                    .router
                    .partitions()
                    .iter()
                    .map(|(_, p)| {
                        let (r, w) = p.db.device_bytes();
                        r.saturating_add(w)
                    })
                    .sum();
                node.device_ceiling.note(Instant::now(), bytes);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_thread_samples_one_call_in_its_period() {
        let n = (0..CPU_EVERY * 100).filter(|_| sample_cpu()).count();
        assert!((99..=100).contains(&n), "{n}");
        let n = (0..READ_EVERY * 10).filter(|_| sample_read()).count();
        assert!((9..=10).contains(&n), "{n}");
    }

    #[test]
    fn the_load_folds_once_an_interval_and_averages() {
        let l = PartitionLoad::default();
        let t0 = Instant::now();
        assert_eq!(l.fold(t0, Reading::default(), 4, 0.0), Load::default());
        // 2 s of busy in a 30 s interval over 4 cores, plus 1 s of sampled request CPU.
        l.add_cpu(1_000_000_000 / u64::from(CPU_EVERY));
        let r = Reading {
            busy_nanos: 2_000_000_000,
            sync_nanos: 1_000_000_000,
            device_bytes: 30 << 20,
        };
        let t1 = t0 + INTERVAL;
        // Within the interval: nothing yet.
        assert_eq!(
            l.fold(t1 - Duration::from_secs(1), r, 4, 0.0),
            Load::default()
        );
        let got = l.fold(t1, r, 4, 4.0 * (1 << 20) as f64);
        assert!((got.cpu - 3.0 / 30.0 / 4.0).abs() < 1e-9, "{got:?}");
        // 1 MiB/s over a 4 MiB/s ceiling.
        assert!((got.device - 0.25).abs() < 1e-9, "{got:?}");
        assert_eq!(got.load(), got.device);
        // An idle interval pulls the average down by α.
        let idle = l.fold(t1 + INTERVAL, r, 4, 4.0 * (1 << 20) as f64);
        assert!(
            (idle.device - 0.25 * (1.0 - ALPHA)).abs() < 1e-9,
            "{idle:?}"
        );
    }

    #[test]
    fn read_keys_stand_for_every_sampled_read_whatever_the_bounds() {
        // perf287's follow-up: keys, not per-tablet counts, so bounds changing mid-window
        // lose nothing; the balancer matches them to the bounds it has when it asks.
        let l = PartitionLoad::default();
        for i in 0..(READ_SLOTS as u32 + 10) {
            l.add_read("t", format!("k{i:04}").as_bytes());
        }
        let (keys, each) = l.read_keys();
        assert_eq!(keys.len(), READ_SLOTS);
        let total = (READ_SLOTS + 10) as f64 * f64::from(READ_EVERY);
        assert!((each * keys.len() as f64 - total).abs() < 1e-6);
        // The newest overwrote the oldest.
        assert!(keys.iter().any(|k| k.key == b"k0265"));
        assert!(!keys.iter().any(|k| k.key == b"k0000"));
    }

    #[test]
    fn a_timed_future_adds_its_polls_cpu_when_ready() {
        let l = Arc::new(PartitionLoad::default());
        let fut = Timed::new(
            async {
                let mut x = 0u64;
                for i in 0..2_000_000u64 {
                    x = x.wrapping_add(i * i);
                }
                std::hint::black_box(x)
            },
            Arc::clone(&l),
        );
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(fut);
        assert!(l.cpu_nanos.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn the_ceiling_is_the_highest_second_within_the_window() {
        let c = DeviceCeiling::new(Duration::from_secs(10));
        let t = Instant::now();
        c.note(t, 0);
        c.note(t + Duration::from_secs(1), 5 << 20);
        c.note(t + Duration::from_secs(2), 6 << 20);
        assert!((c.get() - (5 << 20) as f64).abs() < 1.0);
        // Past the window the fast second is forgotten.
        c.note(t + Duration::from_secs(13), 7 << 20);
        assert!(c.get() < (5 << 20) as f64, "{}", c.get());
    }
}
