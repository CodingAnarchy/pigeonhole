//! Engine-owned driver threads (D289). Each runs the turns of the shards it holds, one
//! after another, then spins and parks once for all of them. With the load controller
//! (PR 2) the shards move between drivers: a light load runs on few threads, which park
//! less, and a heavy one on up to one per shard.
//!
//! # Moves
//! A shard moves only between its turns, and only when its driver's own ring is quiet
//! (nothing of the thread's in flight): its I/O completes only on the ring of the thread
//! that submitted it (#402), and a turn may wait on that I/O. So nothing a moved shard
//! submitted is left on the old ring, and no wait inside a turn can come to depend on
//! another driver's ring through a move (#1002). The source takes the shard out of its
//! list (it announces no further sleep for it), pushes it onto the adopter's handoff queue,
//! and wakes the adopter. The adopter drains its queue at the start of every pass, points
//! the shard's wakeups at itself, and runs the shard's turn before it next parks: a producer
//! that read the old target woke the old driver, and the adopter's turn finds its message.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

use pigeonhole_io::VfsRef;

use crate::signal::WakeTarget;
use crate::{EnterShard, RING_IDLE_WAIT, ShardCore, ShardHandler, ShardId, Submitter, sched};

/// A pass longer than this kept a message waiting too long behind its siblings' turns (#997's
/// A/B: heavy rows at one driver had pass p99s of 128-256 µs, light ones far under). More
/// than [`SLOW_PASSES`] of them hold off a shrink.
const PASS_BUDGET_NANOS: u64 = 100_000;
/// The controller's window.
const WINDOW: Duration = Duration::from_millis(100);
/// Windows a grow condition must hold over (1 s).
const GROW_WINDOWS: usize = 10;
/// Windows the sibling-wait rule sums over (2 s): over 10, one spike of sparse-wide's at 2
/// drivers (0.16) grew it to 4 for 40 s, while over 20 its highest was 0.13, and the loads
/// that need more drivers held (ycsb-f at 8: 0.27; transfer at 8: 0.15 median; #1015's
/// first round).
const WAIT_WINDOWS: usize = 20;
/// Windows a shrink condition must hold over at first (5 s); doubled by a shrink a grow
/// undid within [`UNDO`], up to [`MAX_SHRINK_WINDOWS`] (D255's damping).
const SHRINK_WINDOWS: usize = 50;
const MAX_SHRINK_WINDOWS: usize = 800;
/// No resize within this long after another.
const HOLD: Duration = Duration::from_secs(2);
const UNDO: Duration = Duration::from_secs(60);
/// Awake-busy (turn time over the window) above which a driver grows the set: a backstop
/// for a saturated driver. Sibling wait is the grow signal; in #1015's first round it fired
/// wherever busy did, and busy at 0.75 added no grow it missed.
const GROW_BUSY: f64 = 0.9;
/// A shrink keeps the remaining drivers at most this busy on average.
const SHRINK_BUSY: f64 = 0.5;
/// Producer wakes per message above which fewer drivers would park less. #1015's first
/// round, over the shrink windows of the measured phases: sparse-wide 0.107-0.82 (where
/// fewer drivers keep its put tail at 33-43 µs, against 553 at 16), and ycsb-f, transfer and
/// ycsb-a at most 0.082. At 0.05 ycsb-f shrank 16 -> 8 and back (-4% at 8).
const SHRINK_WAKES_PER_OP: f64 = 0.10;
/// Slow passes (over [`PASS_BUDGET_NANOS`]) above this fraction hold off a shrink. Not a
/// grow signal: in #1015's first round it read 50-100% in the settle step on a few ops a
/// window, and fired in load phases on flush and compaction slices with nothing waiting
/// (ycsb-a 4 -> 8, wait 0.03); in the measured phases it never fired without sibling wait.
const SLOW_PASSES: f64 = 0.01;
/// The fewest drivers the shrink goes to. One driver never paid off: the controller took
/// sparse-wide to 1 twice in #1015's first round and went back up both times within 2 s
/// (sibling wait 0.27), while 2 holds its tail gain.
const MIN_DRIVERS: usize = 2;
/// A shrink needs at least this many ops per window on average: an idle store's drivers
/// park and cost nothing, while shrinking it in a gap between bursts left the next burst
/// growing back through its first seconds (#1006's third A/B: every heavy run started 16 -> 2).
const SHRINK_MIN_OPS: u64 = 100;
/// Sibling wait over busy, summed over the active drivers, above which the set grows: their
/// shards' work waits behind other shards' turns for this share of the time it is served.
/// Transfer's hops at few drivers cost throughput with neither a busy driver nor slow passes
/// (#1006's fourth A/B, -6%); this is the cost itself. #1006's fixed-k round put the line
/// between the loads that gain from more drivers (transfer 1.6 at 2 drivers and 0.55 at 4,
/// ycsb-f 0.23-0.27 at 8: +50%, +22% and +5% ops a doubling) and sparse-wide at 2 (0.04-0.08,
/// where its tail gain is). Summed, not the highest driver's: a nearly idle driver's ratio
/// is noise (0.57-0.64 for sparse-wide's at 8 of 16, at 2% busy). Summed over the grow
/// windows too, not averaged per window, and only with at least [`SHRINK_MIN_OPS`] per window:
/// one nearly idle window's ratio (1.1-2.2 at 0.1% busy, between a bench's load and its run)
/// grew ycsb-a and sparse-wide in #1006's fifth A/B.
const GROW_WAIT: f64 = 0.15;
/// A window longer than this ran while every driver that could tick it was parked (an idle
/// store): it is dropped, not averaged in, so its diluted busy can't read as a light load.
const IDLE_WINDOW: Duration = Duration::from_millis(200);
/// Windows a trace keeps before its reader drains them ([`Drivers::trace`]): an hour's.
const TRACE_WINDOWS: usize = 36_000;

/// One driver's counters, read by the controller (Relaxed: windowed estimates).
#[derive(Default)]
pub(crate) struct DriverStats {
    /// Nanoseconds in turns (not spinning or parked), on the runtime's clock.
    busy: AtomicU64,
    /// Passes that ran at least one turn.
    passes: AtomicU64,
    /// Of those, passes longer than [`PASS_BUDGET_NANOS`].
    slow: AtomicU64,
    /// Messages handled and background slices run by its shards.
    ops: AtomicU64,
    /// Nanoseconds from each pass's start to the start of each of its turns that handled
    /// work: how long that work waited behind sibling turns (at most; work that arrived
    /// mid-pass waited less). Zero for a lone shard.
    sibling_wait: AtomicU64,
    /// Shards it holds that belong to another driver at the current target (not yet moved:
    /// its ring was never quiet).
    misplaced: AtomicUsize,
}

impl DriverStats {
    /// Busy, passes, slow, ops and sibling wait, as the controller reads them.
    fn counts(&self) -> [u64; 5] {
        [
            self.busy.load(Ordering::Relaxed),
            self.passes.load(Ordering::Relaxed),
            self.slow.load(Ordering::Relaxed),
            self.ops.load(Ordering::Relaxed),
            self.sibling_wait.load(Ordering::Relaxed),
        ]
    }
}

struct Handoff<H: ShardHandler> {
    cores: Vec<ShardCore<H>>,
    /// The driver returned (shutdown): nothing may be handed to it.
    exited: bool,
}

struct Slot<H: ShardHandler> {
    handoff: Mutex<Handoff<H>>,
    /// The driver's thread and its ring's waker, to wake it for a handoff.
    thread: Mutex<Option<(Thread, Option<pigeonhole_io::OwnIoWaker>)>>,
    stats: DriverStats,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The driver threads' shared state.
pub(crate) struct Drivers<H: ShardHandler> {
    slots: Box<[Slot<H>]>,
    /// The drivers the shards run on now: shard `i` belongs to driver `i % target`.
    target: AtomicUsize,
    /// Whether the controller sizes `target` (else it stays as started).
    adaptive: bool,
    /// A test's fixed target (0: none), overriding the controller.
    forced: AtomicUsize,
    /// Set by shutdown before the inboxes close: no more moves, and drivers that hold no
    /// shard exit.
    closing: AtomicBool,
    /// Shards handed from one driver to another.
    moves: AtomicU64,
    /// The load controller (`adaptive` only), ticked by whichever driver finds its window
    /// over: driver 0 may be parked while another driver's shards carry the load.
    controller: Mutex<Option<Controller>>,
    /// When the controller's window started, in nanoseconds since `epoch`: one Relaxed load
    /// per pass decides whether to try the controller's lock.
    window_at: AtomicU64,
    epoch: Instant,
    /// A trace was asked for ([`Drivers::trace`], #1015): the controller runs, deciding
    /// without resizing unless `adaptive`, and logs every window.
    tracing: AtomicBool,
    vfs: VfsRef,
    submitters: Arc<[Submitter<H::Msg>]>,
}

impl<H: ShardHandler> Drivers<H> {
    pub(crate) fn new(
        drivers: usize,
        adaptive: bool,
        vfs: VfsRef,
        submitters: Arc<[Submitter<H::Msg>]>,
    ) -> Self {
        let controller = adaptive.then(|| {
            let wakes = submitters.iter().map(|s| s.idle_counts().1).sum();
            Controller::new(drivers, wakes)
        });
        Self {
            slots: (0..drivers)
                .map(|_| Slot {
                    handoff: Mutex::new(Handoff {
                        cores: Vec::new(),
                        exited: false,
                    }),
                    thread: Mutex::new(None),
                    stats: DriverStats::default(),
                })
                .collect(),
            target: AtomicUsize::new(drivers),
            adaptive,
            forced: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
            moves: AtomicU64::new(0),
            controller: Mutex::new(controller),
            window_at: AtomicU64::new(0),
            epoch: Instant::now(),
            tracing: AtomicBool::new(false),
            vfs,
            submitters,
        }
    }

    /// Runs the controller if its window is over and no other driver is running it.
    fn tick(&self) {
        let window = WINDOW.as_nanos() as u64;
        let now = self.epoch.elapsed().as_nanos() as u64;
        if now.saturating_sub(self.window_at.load(Ordering::Relaxed)) < window {
            return;
        }
        let mut c = match self.controller.try_lock() {
            Ok(c) => c,
            Err(std::sync::TryLockError::Poisoned(p)) => p.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return,
        };
        if let Some(c) = c.as_mut() {
            c.tick(self);
        }
        self.window_at.store(now, Ordering::Relaxed);
    }

    /// The driver shard `id` belongs to at `k` drivers. Round-robin, not blocks of n/k:
    /// packing (D217) keeps shards `0..active` busy, and round-robin spreads those over the
    /// drivers where blocks would put them all on driver 0.
    pub(crate) fn owner(id: ShardId, k: usize) -> usize {
        usize::from(id.0) % k
    }

    pub(crate) fn target(&self) -> usize {
        self.target.load(Ordering::Acquire)
    }

    /// Whether every shard is on the driver the target puts it on.
    pub(crate) fn settled(&self) -> bool {
        self.slots
            .iter()
            .all(|s| s.stats.misplaced.load(Ordering::Relaxed) == 0)
    }

    /// Fixes the target at `k` (clamped to the drivers started), or with `None` hands it
    /// back to the controller, or without one back to the drivers started. For tests and
    /// benchmarks.
    pub(crate) fn force_target(&self, k: Option<usize>) {
        let k = k.map_or(0, |k| k.clamp(1, self.slots.len()));
        self.forced.store(k, Ordering::Release);
        if k != 0 {
            self.set_target(k);
        } else if !self.adaptive {
            self.set_target(self.slots.len());
        }
    }

    fn set_target(&self, k: usize) {
        if self.target.swap(k, Ordering::AcqRel) != k {
            // Every driver looks again: those above `k` give their shards away.
            for d in 0..self.slots.len() {
                self.wake(d);
            }
        }
    }

    /// Shutdown, before the inboxes close: no more moves, and every driver wakes to finish.
    pub(crate) fn close(&self) {
        self.closing.store(true, Ordering::Release);
        for d in 0..self.slots.len() {
            self.wake(d);
        }
    }

    fn wake(&self, d: usize) {
        let target = lock(&self.slots[d].thread).clone();
        if let Some((t, w)) = target {
            if let Some(w) = w {
                w.wake();
            }
            t.unpark();
        }
    }

    /// Hands `core` to driver `to`, or gives it back when `to` exited or shutdown began.
    fn hand_off(&self, mut core: ShardCore<H>, to: usize) -> Option<ShardCore<H>> {
        let mut h = lock(&self.slots[to].handoff);
        if h.exited || self.closing.load(Ordering::Acquire) {
            return Some(core);
        }
        core.mark_released();
        h.cores.push(core);
        drop(h);
        self.moves.fetch_add(1, Ordering::Relaxed);
        self.wake(to);
        None
    }

    pub(crate) fn moves(&self) -> u64 {
        self.moves.load(Ordering::Relaxed)
    }

    /// The controller's windows since the last call, one JSON object each (#1015). The
    /// first call starts the log, and without the load controller starts an observe-only
    /// one: it reads the same windows and logs what it would decide, but never resizes. For
    /// benchmarks: not an API.
    pub(crate) fn trace(&self) -> Vec<String> {
        let wakes = self.wakes();
        let mut c = lock(&self.controller);
        let c = c.get_or_insert_with(|| {
            let mut c = Controller::new(self.slots.len(), wakes);
            for (last, slot) in c.last.iter_mut().zip(self.slots.iter()) {
                *last = slot.stats.counts();
            }
            c
        });
        let out = c.log.get_or_insert_with(VecDeque::new).drain(..).collect();
        self.tracing.store(true, Ordering::Relaxed);
        out
    }

    /// Producer wakes of every shard so far (the sum of their `(parks, wakes)` second half).
    fn wakes(&self) -> u64 {
        self.submitters.iter().map(|s| s.idle_counts().1).sum()
    }
}

/// One window of the controller's view. Only drivers below the target count towards
/// `max_busy`, `sum_busy`, `slow` and `wait`: during a shrink's moves, misplaced shards' time
/// on drivers at or above it is missed for a window or two.
#[derive(Clone, Copy, Debug)]
struct Window {
    /// The busiest driver's awake-busy.
    max_busy: f64,
    /// Awake-busy summed over the active drivers.
    sum_busy: f64,
    /// The highest fraction of slow passes over the active drivers.
    slow: f64,
    /// The active drivers' sibling wait and busy, in nanoseconds.
    wait_nanos: u64,
    busy_nanos: u64,
    wakes: u64,
    ops: u64,
}

/// Which rule resized the set. A shrink that cost throughput is undone by the grow rule,
/// on the sibling wait it causes, not by comparing ops per window before and after it:
/// sparse-wide's ops per window move 3% in a second about half the time at a fixed driver
/// count, so that comparison undid its shrink to 2 at random and held it at 4, where its
/// tail gain is lost (#1015's second round).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Why {
    Grow,
    Shrink,
}

/// The load controller (D289 PR 2), run between passes by any driver whose pass finds the
/// window over.
struct Controller {
    at: Instant,
    /// Per driver: busy, passes, slow, ops, sibling wait at the window's start.
    last: Vec<[u64; 5]>,
    last_wakes: u64,
    windows: VecDeque<Window>,
    hold_until: Instant,
    shrink_windows: usize,
    last_shrink: Option<Instant>,
    /// The trace's windows not yet read ([`Drivers::trace`]), and how many it has logged.
    log: Option<VecDeque<String>>,
    logged: u64,
}

impl Controller {
    fn new(drivers: usize, wakes: u64) -> Self {
        let now = Instant::now();
        Self {
            at: now,
            last: vec![[0; 5]; drivers],
            last_wakes: wakes,
            windows: VecDeque::new(),
            hold_until: now + HOLD,
            shrink_windows: SHRINK_WINDOWS,
            last_shrink: None,
            log: None,
            logged: 0,
        }
    }

    fn tick<H: ShardHandler>(&mut self, drivers: &Drivers<H>) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.at);
        if elapsed < WINDOW {
            return;
        }
        self.at = now;
        let k = drivers.target();
        let window_nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX).max(1) as f64;
        let (mut max_busy, mut sum_busy, mut slow, mut ops) = (0f64, 0f64, 0f64, 0u64);
        let (mut wait_nanos, mut busy_nanos) = (0u64, 0u64);
        let mut deltas = Vec::new();
        for (d, slot) in drivers.slots.iter().enumerate() {
            let now_counts = slot.stats.counts();
            let last = std::mem::replace(&mut self.last[d], now_counts);
            let delta = |i: usize| now_counts[i].saturating_sub(last[i]);
            if self.log.is_some() {
                deltas.push(std::array::from_fn::<u64, 5, _>(delta));
            }
            ops += delta(3);
            if d < k {
                let busy = delta(0) as f64 / window_nanos;
                max_busy = max_busy.max(busy);
                sum_busy += busy;
                if delta(1) > 0 {
                    slow = slow.max(delta(2) as f64 / delta(1) as f64);
                }
                wait_nanos += delta(4);
                busy_nanos += delta(0);
            }
        }
        let wakes_now = drivers.wakes();
        let wakes = wakes_now.saturating_sub(std::mem::replace(&mut self.last_wakes, wakes_now));
        if elapsed > IDLE_WINDOW {
            // Every driver parked through most of it: the counters above are the new
            // baseline, and the window isn't one.
            self.log_window(drivers, elapsed, k, &deltas, wakes, None, false);
            return;
        }
        self.windows.push_back(Window {
            max_busy,
            sum_busy,
            slow,
            wait_nanos,
            busy_nanos,
            wakes,
            ops,
        });
        while self.windows.len() > MAX_SHRINK_WINDOWS {
            self.windows.pop_front();
        }
        let held = drivers.forced.load(Ordering::Acquire) != 0 || now < self.hold_until;
        let decision = if held && self.log.is_none() {
            None
        } else {
            self.decide(k, drivers.slots.len())
        };
        let apply = drivers.adaptive && !held;
        if self.log.is_some() {
            let logged = decision.map(|d| (d, apply));
            self.log_window(drivers, elapsed, k, &deltas, wakes, logged, true);
        }
        if !apply {
            return;
        }
        if let Some((next, _)) = decision {
            if next < k {
                self.last_shrink = Some(now);
            } else if self
                .last_shrink
                .is_some_and(|at| now.duration_since(at) < UNDO)
            {
                // A shrink undone: wait twice as long before the next one (D255).
                self.shrink_windows = (self.shrink_windows * 2).min(MAX_SHRINK_WINDOWS);
            }
            self.windows.clear();
            self.hold_until = now + HOLD;
            drivers.set_target(next);
        }
    }

    /// Logs one window: each driver's busy, passes, slow passes, ops and sibling wait over
    /// it, the producer wakes, and what the controller decided and whether it resized.
    #[allow(clippy::too_many_arguments)]
    fn log_window<H: ShardHandler>(
        &mut self,
        drivers: &Drivers<H>,
        elapsed: Duration,
        k: usize,
        deltas: &[[u64; 5]],
        wakes: u64,
        decision: Option<((usize, Why), bool)>,
        counted: bool,
    ) {
        let Some(log) = self.log.as_mut() else {
            return;
        };
        let column = |i: usize| {
            let v: Vec<String> = deltas.iter().map(|d| d[i].to_string()).collect();
            v.join(",")
        };
        let at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_micros());
        let decided = match decision {
            None => "null".to_owned(),
            Some(((to, why), applied)) => {
                format!(r#"{{"to":{to},"why":"{why:?}","applied":{applied}}}"#)
            }
        };
        log.push_back(format!(
            r#"{{"seq":{},"at_us":{at},"window_us":{},"k":{k},"ceiling":{},"adaptive":{},"forced":{},"counted":{counted},"busy_ns":[{}],"passes":[{}],"slow":[{}],"ops":[{}],"wait_ns":[{}],"wakes":{wakes},"decided":{decided}}}"#,
            self.logged,
            elapsed.as_micros(),
            drivers.slots.len(),
            drivers.adaptive,
            drivers.forced.load(Ordering::Acquire),
            column(0),
            column(1),
            column(2),
            column(3),
            column(4),
        ));
        self.logged += 1;
        while log.len() > TRACE_WINDOWS {
            log.pop_front();
        }
    }

    /// The next target and the rule that set it, if the windows since the last resize call
    /// for one.
    fn decide(&self, k: usize, ceiling: usize) -> Option<(usize, Why)> {
        let recent = |n: usize| self.windows.iter().rev().take(n);
        if k < ceiling && self.windows.len() >= GROW_WINDOWS {
            let n = GROW_WINDOWS as f64;
            let busy = recent(GROW_WINDOWS).map(|w| w.max_busy).sum::<f64>() / n;
            let busy_ops: u64 = recent(GROW_WINDOWS).map(|w| w.ops).sum();
            let saturated = busy_ops >= SHRINK_MIN_OPS * GROW_WINDOWS as u64 && busy > GROW_BUSY;
            let waiting = self.windows.len() >= WAIT_WINDOWS && {
                let (wait, served, ops) = recent(WAIT_WINDOWS).fold((0, 0, 0), |(a, b, o), w| {
                    (a + w.wait_nanos, b + w.busy_nanos, o + w.ops)
                });
                ops >= SHRINK_MIN_OPS * WAIT_WINDOWS as u64
                    && wait as f64 > GROW_WAIT * served as f64
            };
            // Doubling, not one at a time: a driver's busy saturates at 1, so it can't say how
            // many more the work needs, and stepping up one per 3 s left transfer at 2-12 of 16
            // drivers through its measured phase (-10%; #1006's second A/B).
            if saturated || waiting {
                return Some(((2 * k).min(ceiling), Why::Grow));
            }
        }
        if k > 1 && self.windows.len() >= self.shrink_windows {
            let n = self.shrink_windows as f64;
            let sum_busy = recent(self.shrink_windows).map(|w| w.sum_busy).sum::<f64>() / n;
            let slow = recent(self.shrink_windows).map(|w| w.slow).sum::<f64>() / n;
            let wakes: u64 = recent(self.shrink_windows).map(|w| w.wakes).sum();
            let ops: u64 = recent(self.shrink_windows).map(|w| w.ops).sum();
            let wakes_per_op = wakes as f64 / ops.max(1) as f64;
            let busy_enough = ops >= SHRINK_MIN_OPS * self.shrink_windows as u64;
            // Straight to the drivers the work needs at SHRINK_BUSY each, not one at a time:
            // stepping down one driver per 5 s took a light store about 100 s from 16 to 2,
            // past the bench's warm-up and most of its measured phase (#1006's first A/B).
            let needed = ((sum_busy / SHRINK_BUSY).ceil() as usize).max(MIN_DRIVERS);
            if wakes_per_op > SHRINK_WAKES_PER_OP
                && busy_enough
                && needed < k
                && slow <= SLOW_PASSES
            {
                return Some((needed, Why::Shrink));
            }
        }
        None
    }
}

/// Body of an engine-owned driver thread `d` (D289): the turns of the shards it holds, one
/// after another, then one spin and one park for all of them. With one shard it is that
/// shard's thread, as before D289. Returns the handlers of the shards it held at shutdown,
/// with their ids.
pub(crate) fn driver_main<H: ShardHandler>(
    drivers: Arc<Drivers<H>>,
    d: usize,
    mut cores: Vec<ShardCore<H>>,
) -> Vec<(ShardId, H)> {
    let mut idle_park = sched::IdlePark::default();
    let mut done = Vec::new();
    let vfs = Arc::clone(&drivers.vfs);
    // The driver reads the spin window from its first shard: every shard of a runtime
    // shares it.
    debug_assert!(cores.iter().all(|c| Arc::ptr_eq(&c.vfs, &vfs)
        && (c.spin.window, c.spin.cap) == (cores[0].spin.window, cores[0].spin.cap)));
    // Sized from this thread's idle gaps (#1176); made at the first spin, from a shard's
    // settings (a driver may start with none).
    let mut gaps: Option<GapWindow> = None;
    vfs.attach_thread();
    for core in &mut cores {
        core.attach();
    }
    // Every shard this driver holds wakes this thread: its park, or its wait in its ring.
    let waker = pigeonhole_io::own_io_waker();
    let target = || match &waker {
        Some(w) => WakeTarget::ThreadRing(thread::current(), w.clone()),
        None => WakeTarget::Thread(thread::current()),
    };
    *lock(&drivers.slots[d].thread) = Some((thread::current(), waker.clone()));
    for core in &cores {
        core.signal().set_target(target());
    }
    let stats = &drivers.slots[d].stats;
    loop {
        // Shards handed over since the last pass: their wakeups now reach this thread, and
        // their turns run in this pass, before any park.
        {
            let mut h = lock(&drivers.slots[d].handoff);
            for core in h.cores.drain(..) {
                core.signal().set_target(target());
                cores.push(core);
            }
        }
        let pass_start = vfs.monotonic_nanos();
        let mut more = false;
        let mut sibling_wait = 0;
        let mut i = 0;
        while i < cores.len() {
            let core = &mut cores[i];
            let start = vfs.monotonic_nanos();
            let deadline = start.saturating_add(core.time_slice);
            let work = core.work_done;
            more |= core.run_once(deadline);
            let turn = vfs.monotonic_nanos().saturating_sub(start);
            // The turn's time, for the balancer's view of how busy this shard is (D217) and
            // the controller's of this driver.
            core.signal().add_busy(turn);
            stats.busy.fetch_add(turn, Ordering::Relaxed);
            stats
                .ops
                .fetch_add(core.work_done.saturating_sub(work), Ordering::Relaxed);
            if core.work_done > work {
                sibling_wait += start.saturating_sub(pass_start);
            }
            if core.inbox().is_closed() {
                let mut core = cores.swap_remove(i);
                let _shard = EnterShard::new(core.id);
                core.finish();
                done.push((core.id, core.handler));
                continue;
            }
            i += 1;
        }
        if !cores.is_empty() {
            stats.passes.fetch_add(1, Ordering::Relaxed);
            if sibling_wait > 0 {
                stats
                    .sibling_wait
                    .fetch_add(sibling_wait, Ordering::Relaxed);
            }
            if vfs.monotonic_nanos().saturating_sub(pass_start) > PASS_BUDGET_NANOS {
                stats.slow.fetch_add(1, Ordering::Relaxed);
            }
        }
        if drivers.adaptive || drivers.tracing.load(Ordering::Relaxed) {
            drivers.tick();
        }
        // Shards that belong elsewhere at the current target move once this thread's ring
        // is quiet (see the module doc); until then they stay and run here.
        let k = drivers.target();
        let misplaced = cores
            .iter()
            .filter(|c| Drivers::<H>::owner(c.id, k) != d)
            .count();
        if misplaced > 0 && !pigeonhole_io::own_io_in_flight() {
            let mut kept = Vec::with_capacity(cores.len());
            for core in cores.drain(..) {
                let to = Drivers::<H>::owner(core.id, k);
                if to == d {
                    kept.push(core);
                } else if let Some(core) = drivers.hand_off(core, to) {
                    kept.push(core);
                }
            }
            cores = kept;
        }
        stats.misplaced.store(
            cores
                .iter()
                .filter(|c| Drivers::<H>::owner(c.id, k) != d)
                .count(),
            Ordering::Relaxed,
        );
        if cores.is_empty() && drivers.closing.load(Ordering::Acquire) {
            let mut h = lock(&drivers.slots[d].handoff);
            if h.cores.is_empty() {
                h.exited = true;
                return done;
            }
            continue;
        }
        if more {
            continue;
        }
        // I/O only this thread completes (its ring, #402), for any of its shards: spin
        // first, reaping without blocking, then wait on the ring until a completion, a wake
        // (the ring's eventfd) or the earliest deadline.
        let idle_from = Instant::now();
        if gaps.is_none()
            && let Some(c) = cores.first()
        {
            gaps = Some(GapWindow::new(c.spin.window, c.spin.cap));
        }
        let window = gaps.as_ref().map_or(Duration::ZERO, GapWindow::window);
        if pigeonhole_io::own_io_in_flight() {
            if spin_before_park(&mut cores, window, true) {
                record_gap(&mut gaps, idle_from);
                continue;
            }
            let now = vfs.monotonic_nanos();
            let until = next_deadline(&cores)
                .map_or(RING_IDLE_WAIT, |d| {
                    Duration::from_nanos(d.saturating_sub(now))
                })
                .min(RING_IDLE_WAIT);
            pigeonhole_io::reap_own_io(Some(until));
            record_gap(&mut gaps, idle_from);
            continue;
        }
        if spin_before_park(&mut cores, window, false) {
            record_gap(&mut gaps, idle_from);
            continue;
        }
        // Every shard announced sleep at the end of its turn (or of the spin), so a message
        // sent since wakes this thread, as does a handoff. Until then, or the earliest
        // sleeping task's deadline (re-checked sooner while the clock has not shown it keeps
        // real time: a simulated clock moves without waking anyone).
        if !lock(&drivers.slots[d].handoff).cores.is_empty() {
            continue;
        }
        for core in &cores {
            core.signal().parked();
        }
        match next_deadline(&cores) {
            None => thread::park(),
            Some(at) => idle_park.park(&*vfs, at),
        }
        record_gap(&mut gaps, idle_from);
    }
}

fn record_gap(gaps: &mut Option<GapWindow>, idle_from: Instant) {
    if let Some(g) = gaps {
        g.record(idle_from.elapsed());
    }
}

/// A driver's spin window sized from its recent idle gaps (#1176): each gap runs from the
/// end of a pass with nothing left to the next pass, whether a spin, a ring wait or a park
/// ended it. A pass a timer starts (a balance pass, a flush deadline, a ring wait's timeout)
/// ends a gap too, and a parked gap includes the wake: both count gaps as no shorter than
/// the commits' own, so they bias the window short, never long. The window is the longest, up to the cap, whose expected poll per gap is at
/// most the base window: a poll ends when the gap does or the window runs out, so it costs
/// `min(gap, window)`. The base window always qualifies (it costs at most itself), so the
/// window is never shorter than the base. Gaps mostly short (a steady stream, however
/// bursty) allow the cap and catch the rare long gap a fixed base would park through;
/// gaps mostly long allow little more than the base, which then polls no longer per gap
/// than a fixed one.
#[derive(Debug)]
struct GapWindow {
    base: Duration,
    cap: Duration,
    /// Gaps by power-of-two microseconds (bucket `b`: from 2^b to 2^(b+1) µs, bucket 0 from
    /// 0), and their sum in nanoseconds, halved every [`Self::DECAY`] gaps.
    counts: [u32; Self::BUCKETS],
    sums: [u64; Self::BUCKETS],
    /// Gaps since the last halving, and since the window was last chosen.
    seen: u32,
    since: u32,
    window: Duration,
}

impl GapWindow {
    const BUCKETS: usize = 16;
    /// Gaps between choices of the window: the first comes after this many too.
    const CHOOSE: u32 = 32;
    const DECAY: u32 = 1024;

    fn new(base: Duration, cap: Duration) -> Self {
        Self {
            base,
            cap,
            counts: [0; Self::BUCKETS],
            sums: [0; Self::BUCKETS],
            seen: 0,
            since: 0,
            window: base,
        }
    }

    fn window(&self) -> Duration {
        self.window
    }

    fn record(&mut self, gap: Duration) {
        if self.cap <= self.base {
            return;
        }
        let nanos = u64::try_from(gap.as_nanos()).unwrap_or(u64::MAX);
        let b = (nanos / 1000).checked_ilog2().unwrap_or(0) as usize;
        let b = b.min(Self::BUCKETS - 1);
        self.counts[b] += 1;
        self.sums[b] = self.sums[b].saturating_add(nanos);
        self.seen += 1;
        if self.seen >= Self::DECAY {
            self.seen /= 2;
            for (c, s) in self.counts.iter_mut().zip(&mut self.sums) {
                *c /= 2;
                *s /= 2;
            }
        }
        self.since += 1;
        if self.since >= Self::CHOOSE {
            self.since = 0;
            self.window = self.choose();
        }
    }

    /// The expected poll per gap of a `window`, in nanoseconds, from the buckets: exact
    /// at a bucket edge, and over the bucket it falls inside an upper bound.
    fn cost(&self, window: u64) -> u64 {
        let n: u64 = self.counts.iter().map(|&c| u64::from(c)).sum();
        if n == 0 {
            return 0;
        }
        let mut total: u64 = 0;
        for (b, (&c, &s)) in self.counts.iter().zip(&self.sums).enumerate() {
            let below = if b + 1 < Self::BUCKETS {
                (1000u64 << (b + 1)) <= window
            } else {
                false
            };
            let poll = if below {
                s
            } else {
                s.min(window.saturating_mul(u64::from(c)))
            };
            total = total.saturating_add(poll);
        }
        total / n
    }

    fn choose(&self) -> Duration {
        let nanos = |d: Duration| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        let (base, cap) = (nanos(self.base), nanos(self.cap));
        let mut best = base;
        // The cap, then each bucket edge above the base and below the cap.
        let edges = (0..Self::BUCKETS).map(|b| 1000u64 << b);
        for w in std::iter::once(cap).chain(edges.filter(|&e| e > base && e < cap)) {
            if w > best && self.cost(w) <= base {
                best = w;
            }
        }
        Duration::from_nanos(best)
    }
}

/// The earliest deadline of a sleeping background task on any of `cores`.
fn next_deadline<H: ShardHandler>(cores: &[ShardCore<H>]) -> Option<u64> {
    cores.iter().filter_map(ShardCore::next_deadline).min()
}

/// Before parking (engine-owned mode, D198): if any of a driver's shards handled a message
/// since the last park, polls all their queues for up to the spin window, so a client's
/// next message is taken without a wakeup. With `reap`, own-ring I/O is in flight: the poll
/// also completes it without blocking, so a commit waiting on a sync is answered without a
/// ring wait (#997's A/B: ring waits ended 67-78% of ycsb-f's passes). Returns whether work
/// arrived (the loop runs again at once). The window is measured on the real clock: a
/// simulated clock may not move while polling.
fn spin_before_park<H: ShardHandler>(
    cores: &mut [ShardCore<H>],
    window: Duration,
    reap: bool,
) -> bool {
    if cores.is_empty() {
        return false;
    }
    // `due` consumes each shard's "handled a message" mark and its backoff.
    let due: Vec<bool> = cores.iter_mut().map(|c| c.spin.due()).collect();
    let n_due = due.iter().filter(|&&d| d).count() as u64;
    if n_due == 0 {
        return false;
    }
    // A poll that runs counts as busy (D256): the thread is awake and polling, kept so by
    // its clients. On the VFS clock, as the turns are (a simulated clock does not move
    // while polling, and counts nothing), split evenly over the shards that made it due.
    // A skipped poll (the window is zero, no message since the last park, or the backoff)
    // adds nothing: timing the skip counted two clock reads' worth on Linux and macOS and
    // nothing on Windows, where consecutive reads of the clock can be equal.
    let spun_from = cores[0].vfs.monotonic_nanos();
    let hit = spin_poll(cores, window, reap);
    let spun = cores[0].vfs.monotonic_nanos().saturating_sub(spun_from);
    let share = spun / n_due;
    for (core, _) in cores.iter_mut().zip(&due).filter(|(_, d)| **d) {
        core.spin.record(hit);
        core.signal().add_spin(share);
    }
    hit
}

/// The poll itself: every shard's queue for up to the spin window, then the sleep
/// handshake on a miss. Returns whether work arrived.
fn spin_poll<H: ShardHandler>(cores: &mut [ShardCore<H>], window: Duration, reap: bool) -> bool {
    // Awake while polling, so a producer does not wake the thread (on a ring backend that
    // is a syscall); sleep is announced again, and checked once more, on a miss.
    for core in cores.iter() {
        core.signal().awake();
    }
    let end = std::time::Instant::now() + window;
    let mut polls = 0u32;
    let mut hit = 'poll: loop {
        for core in cores.iter_mut() {
            if core.stash.is_none() {
                core.stash = core.rx.try_recv().ok();
            }
            if core.stash.is_some() || core.signal().task_woken() || core.inbox().is_closed() {
                break 'poll true;
            }
        }
        polls = polls.wrapping_add(1);
        if polls.is_multiple_of(64) {
            if std::time::Instant::now() >= end {
                break false;
            }
            // Completions resolve here, waking the tasks waiting on them (`task_woken`
            // above): in userspace while the completion queue is empty.
            if reap {
                pigeonhole_io::reap_own_io(None);
            }
            // Let another runnable thread in (an oversubscribed machine).
            thread::yield_now();
        } else {
            std::hint::spin_loop();
        }
    };
    if !hit {
        // The sleep handshake again, for every shard: whatever a producer published
        // before it read the flag is found here, and anything later wakes the thread.
        for core in cores.iter_mut() {
            hit |= core.check_before_sleep();
        }
    }
    if hit {
        for core in cores.iter() {
            core.signal().awake();
        }
    }
    hit
}

#[cfg(test)]
mod tests {
    use super::{Controller, GROW_WINDOWS, GapWindow, SHRINK_WINDOWS, WAIT_WINDOWS, Why, Window};
    use std::collections::VecDeque;
    use std::time::Duration;
    use std::time::Instant;

    fn controller(windows: impl IntoIterator<Item = Window>) -> Controller {
        let now = Instant::now();
        Controller {
            at: now,
            last: Vec::new(),
            last_wakes: 0,
            windows: windows.into_iter().collect::<VecDeque<_>>(),
            hold_until: now,
            shrink_windows: SHRINK_WINDOWS,
            last_shrink: None,
            log: None,
            logged: 0,
        }
    }

    fn window(max_busy: f64, sum_busy: f64, slow: f64, wakes: u64, ops: u64) -> Window {
        Window {
            max_busy,
            sum_busy,
            slow,
            wait_nanos: 0,
            busy_nanos: 0,
            wakes,
            ops,
        }
    }

    /// A light, wake-heavy load shrinks straight to the drivers its work needs at 0.5 busy
    /// each, not one at a time.
    #[test]
    fn a_light_load_shrinks_to_what_it_needs_in_one_step() {
        let c = controller(vec![window(0.1, 1.4, 0.0, 200, 1000); SHRINK_WINDOWS]);
        assert_eq!(c.decide(16, 16), Some((3, Why::Shrink)));
        // Too little evidence yet: no change.
        let c = controller(vec![window(0.1, 1.4, 0.0, 200, 1000); SHRINK_WINDOWS - 1]);
        assert_eq!(c.decide(16, 16), None);
        // Few producer wakes: the drivers aren't parking, so fewer wouldn't help. 0.08 a
        // message is ycsb-f's and transfer's at 8-16 drivers (#1015), which lose 4% at 8.
        let c = controller(vec![window(0.1, 1.4, 0.0, 80, 1000); SHRINK_WINDOWS]);
        assert_eq!(c.decide(16, 16), None);
        // Slow passes: no shrink, even with wakes.
        let c = controller(vec![window(0.1, 1.4, 0.05, 200, 1000); SHRINK_WINDOWS]);
        assert_eq!(c.decide(16, 16), None);
    }

    /// The shrink stops at two drivers, however light the load: one never paid off (#1015).
    #[test]
    fn the_shrink_stops_at_two_drivers() {
        let c = controller(vec![window(0.05, 0.1, 0.0, 500, 1000); SHRINK_WINDOWS]);
        assert_eq!(c.decide(16, 16), Some((2, Why::Shrink)));
        assert_eq!(c.decide(2, 16), None);
    }

    /// An idle store doesn't shrink, whatever its wakes per op: its drivers park.
    #[test]
    fn an_idle_store_does_not_shrink() {
        let c = controller(vec![window(0.0, 0.0, 0.0, 5, 3); SHRINK_WINDOWS]);
        assert_eq!(c.decide(16, 16), None);
    }

    /// A shrink stands through a dip in ops per window that comes without sibling wait: at
    /// a fixed driver count sparse-wide's ops move 3% in a second about half the time
    /// (#1015's second round). The sibling wait a shrink that cost throughput causes grows
    /// the set back instead.
    #[test]
    fn a_shrink_stands_through_an_ops_dip_and_sibling_wait_undoes_it() {
        let before = window(0.3, 0.6, 0.0, 200, 10_000);
        let dip = window(0.3, 0.6, 0.0, 200, 7_000);
        let mut windows = vec![before; SHRINK_WINDOWS];
        windows.extend(vec![dip; WAIT_WINDOWS]);
        assert_eq!(controller(windows).decide(2, 16), None);
        let waiting = Window {
            wait_nanos: 20,
            busy_nanos: 100,
            ..dip
        };
        let c = controller(vec![waiting; WAIT_WINDOWS]);
        assert_eq!(c.decide(2, 16), Some((4, Why::Grow)));
    }

    /// A saturated driver doubles the set, up to the ceiling. Slow passes alone don't: they
    /// come with flush and compaction slices that nothing waits behind (#1015).
    #[test]
    fn a_saturated_driver_grows_the_set_and_slow_passes_alone_do_not() {
        let c = controller(vec![window(0.95, 1.9, 0.0, 0, 1000); GROW_WINDOWS]);
        assert_eq!(c.decide(2, 16), Some((4, Why::Grow)));
        let c = controller(vec![window(0.95, 9.5, 0.0, 0, 1000); GROW_WINDOWS]);
        assert_eq!(c.decide(12, 16), Some((16, Why::Grow)));
        // At the ceiling: no growth.
        let c = controller(vec![window(0.95, 1.9, 0.0, 0, 1000); GROW_WINDOWS]);
        assert_eq!(c.decide(16, 16), None);
        // Busy but not saturated, or saturated on a few ops: no growth.
        let c = controller(vec![window(0.8, 1.6, 0.0, 0, 1000); WAIT_WINDOWS]);
        assert_eq!(c.decide(2, 16), None);
        let c = controller(vec![window(0.95, 1.9, 0.0, 0, 50); WAIT_WINDOWS]);
        assert_eq!(c.decide(2, 16), None);
        // Slow passes alone.
        let c = controller(vec![window(0.3, 0.6, 0.5, 0, 1000); WAIT_WINDOWS]);
        assert_eq!(c.decide(2, 16), None);
    }

    /// Messages waiting behind sibling turns about as long as they're served double the set,
    /// with neither a busy driver nor slow passes (transfer's hops at few drivers).
    #[test]
    fn sibling_wait_grows_the_set() {
        let waiting = Window {
            wait_nanos: 64_000_000,
            busy_nanos: 80_000_000,
            ..window(0.4, 0.8, 0.0, 0, 1000)
        };
        let c = controller(vec![waiting; WAIT_WINDOWS]);
        assert_eq!(c.decide(2, 16), Some((4, Why::Grow)));
        // Over two seconds, not one: a second of waiting is not enough.
        let c = controller(vec![waiting; WAIT_WINDOWS - 1]);
        assert_eq!(c.decide(2, 16), None);
        let c = controller(vec![
            Window {
                wait_nanos: 8_000_000,
                ..waiting
            };
            WAIT_WINDOWS
        ]);
        assert_eq!(c.decide(2, 16), None);
    }

    /// A nearly idle window's high ratio doesn't grow the set: the windows' nanoseconds are
    /// summed, so it weighs as little as it served, and without enough ops none counts.
    #[test]
    fn idle_windows_do_not_grow_the_set_on_sibling_wait() {
        let quiet = Window {
            wait_nanos: 8_000_000,
            busy_nanos: 80_000_000,
            ..window(0.4, 0.8, 0.0, 0, 1000)
        };
        let idle = Window {
            wait_nanos: 2_000_000,
            busy_nanos: 1_000_000,
            ..window(0.01, 0.01, 0.0, 0, 50)
        };
        let mut windows = vec![quiet; WAIT_WINDOWS - 1];
        windows.push(idle);
        assert_eq!(controller(windows).decide(2, 16), None);
        // All idle: too few ops for the ratio to count, however high.
        assert_eq!(controller(vec![idle; WAIT_WINDOWS]).decide(2, 16), None);
    }

    fn gaps(base_us: u64, cap_us: u64, gaps_us: impl IntoIterator<Item = u64>) -> GapWindow {
        let mut g = GapWindow::new(
            Duration::from_micros(base_us),
            Duration::from_micros(cap_us),
        );
        for us in gaps_us {
            g.record(Duration::from_micros(us));
        }
        g
    }

    #[test]
    fn the_spin_window_starts_at_the_base_and_never_falls_below_it() {
        assert_eq!(gaps(200, 1000, []).window(), Duration::from_micros(200));
        // Gaps far over the cap: nothing longer than the base pays for itself.
        let sparse = gaps(200, 1000, std::iter::repeat_n(5000, 256));
        assert_eq!(sparse.window(), Duration::from_micros(200));
    }

    #[test]
    fn a_steady_stream_spins_to_the_cap() {
        // sparse-wide's shape: mostly short gaps and the odd long one, which a 200 µs window
        // parks through. Polling 1 ms costs 0.98 × 20 + 0.02 × 1000 = 40 µs a gap.
        let g = (0..512).map(|i| if i % 50 == 0 { 700 } else { 20 });
        assert_eq!(gaps(200, 1000, g).window(), Duration::from_micros(1000));
    }

    #[test]
    fn gaps_near_the_base_extend_the_window_only_as_far_as_it_pays() {
        // Every gap 300 µs: a window of 256 µs or more costs at least 256 µs a gap, over the
        // base, so the window stays at the base; at 150 µs gaps, polling to the cap costs
        // 150 µs a gap and catches them all.
        let at = |us| gaps(200, 1000, std::iter::repeat_n(us, 256)).window();
        assert_eq!(at(300), Duration::from_micros(200));
        assert_eq!(at(150), Duration::from_micros(1000));
    }

    #[test]
    fn the_window_follows_a_change_of_load() {
        let mut g = gaps(200, 1000, std::iter::repeat_n(20, 256));
        assert_eq!(g.window(), Duration::from_micros(1000));
        for _ in 0..2048 {
            g.record(Duration::from_micros(5000));
        }
        assert_eq!(g.window(), Duration::from_micros(200));
    }

    #[test]
    fn the_window_grows_back_when_the_load_returns() {
        // Slow to fast: the long gaps still weigh until the short ones outnumber them (and
        // the decay halves them), then the window is the cap again.
        let mut g = gaps(200, 1000, std::iter::repeat_n(5000, 256));
        assert_eq!(g.window(), Duration::from_micros(200));
        for _ in 0..32 {
            g.record(Duration::from_micros(20));
        }
        assert_eq!(g.window(), Duration::from_micros(200), "one choice in");
        for _ in 0..2048 {
            g.record(Duration::from_micros(20));
        }
        assert_eq!(g.window(), Duration::from_micros(1000));
    }

    #[test]
    fn a_cap_at_or_under_the_base_keeps_the_window_fixed() {
        let g = gaps(200, 200, std::iter::repeat_n(20, 256));
        assert_eq!(g.window(), Duration::from_micros(200));
        assert_eq!(
            gaps(0, 1000, std::iter::repeat_n(20, 256)).window(),
            Duration::ZERO
        );
    }
}
