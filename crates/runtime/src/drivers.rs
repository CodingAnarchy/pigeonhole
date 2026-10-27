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

/// A pass longer than this kept a message waiting too long behind its siblings' turns: the
/// controller grows when more than 1% of a driver's passes run over it (#997's A/B: heavy
/// rows at one driver had pass p99s of 128-256 µs, light ones far under).
const PASS_BUDGET_NANOS: u64 = 100_000;
/// The controller's window.
const WINDOW: Duration = Duration::from_millis(100);
/// Windows a grow condition must hold over (1 s).
const GROW_WINDOWS: usize = 10;
/// Windows a shrink condition must hold over at first (5 s); doubled by a shrink a grow
/// undid within [`UNDO`], up to [`MAX_SHRINK_WINDOWS`] (D255's damping).
const SHRINK_WINDOWS: usize = 50;
const MAX_SHRINK_WINDOWS: usize = 800;
/// No resize within this long after another.
const HOLD: Duration = Duration::from_secs(2);
const UNDO: Duration = Duration::from_secs(60);
/// Awake-busy (turn time over the window) above which a driver grows the set.
const GROW_BUSY: f64 = 0.75;
/// A shrink keeps the remaining drivers at most this busy on average.
const SHRINK_BUSY: f64 = 0.5;
/// Producer wakes per message above which fewer drivers would park less.
const SHRINK_WAKES_PER_OP: f64 = 0.05;
/// Slow passes (over [`PASS_BUDGET_NANOS`]) above this fraction grow the set.
const SLOW_PASSES: f64 = 0.01;
/// A shrink needs at least this many ops per window on average: an idle store's drivers
/// park and cost nothing, while shrinking it in a gap between bursts left the next burst
/// growing back through its first seconds (#1006's third A/B: every heavy run started 16 -> 2).
const SHRINK_MIN_OPS: u64 = 100;
/// A shrink after which ops per window fall by more than this fraction is undone (the work
/// needed the drivers after all: ycsb-f went 16 -> 8 -> 16 at -3%, and loses 8% at a fixed 8).
const SHRINK_REGRET: f64 = 0.03;

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
    /// Shards it holds that belong to another driver at the current target (not yet moved:
    /// its ring was never quiet).
    misplaced: AtomicUsize,
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
            vfs,
            submitters,
        }
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
    /// back to the controller. For tests and benchmarks.
    pub(crate) fn force_target(&self, k: Option<usize>) {
        let k = k.map_or(0, |k| k.clamp(1, self.slots.len()));
        self.forced.store(k, Ordering::Release);
        if k != 0 {
            self.set_target(k);
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

    /// Producer wakes of every shard so far (the sum of their `(parks, wakes)` second half).
    fn wakes(&self) -> u64 {
        self.submitters.iter().map(|s| s.idle_counts().1).sum()
    }
}

/// One window of the controller's view.
#[derive(Clone, Copy, Debug)]
struct Window {
    /// The busiest driver's awake-busy.
    max_busy: f64,
    /// Awake-busy summed over the active drivers.
    sum_busy: f64,
    /// The highest fraction of slow passes over the active drivers.
    slow: f64,
    wakes: u64,
    ops: u64,
}

/// The load controller (D289 PR 2), run by driver 0 between its passes.
struct Controller {
    at: Instant,
    /// Per driver: busy, passes, slow, ops at the window's start.
    last: Vec<[u64; 4]>,
    last_wakes: u64,
    windows: VecDeque<Window>,
    hold_until: Instant,
    shrink_windows: usize,
    last_shrink: Option<Instant>,
    /// After a shrink: the target before it and the ops per window that led to it, to undo
    /// the shrink if throughput falls ([`SHRINK_REGRET`]).
    regret: Option<(usize, f64)>,
}

impl Controller {
    fn new<H: ShardHandler>(drivers: &Drivers<H>) -> Self {
        let now = Instant::now();
        Self {
            at: now,
            last: vec![[0; 4]; drivers.slots.len()],
            last_wakes: drivers.wakes(),
            windows: VecDeque::new(),
            hold_until: now + HOLD,
            shrink_windows: SHRINK_WINDOWS,
            last_shrink: None,
            regret: None,
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
        for (d, slot) in drivers.slots.iter().enumerate() {
            let s = &slot.stats;
            let now_counts = [
                s.busy.load(Ordering::Relaxed),
                s.passes.load(Ordering::Relaxed),
                s.slow.load(Ordering::Relaxed),
                s.ops.load(Ordering::Relaxed),
            ];
            let last = std::mem::replace(&mut self.last[d], now_counts);
            let delta = |i: usize| now_counts[i].saturating_sub(last[i]);
            ops += delta(3);
            if d < k {
                let busy = delta(0) as f64 / window_nanos;
                max_busy = max_busy.max(busy);
                sum_busy += busy;
                if delta(1) > 0 {
                    slow = slow.max(delta(2) as f64 / delta(1) as f64);
                }
            }
        }
        let wakes_now = drivers.wakes();
        let wakes = wakes_now.saturating_sub(std::mem::replace(&mut self.last_wakes, wakes_now));
        self.windows.push_back(Window {
            max_busy,
            sum_busy,
            slow,
            wakes,
            ops,
        });
        while self.windows.len() > MAX_SHRINK_WINDOWS {
            self.windows.pop_front();
        }
        if drivers.forced.load(Ordering::Acquire) != 0 || now < self.hold_until {
            return;
        }
        if let Some(next) = self.decide(k, drivers.slots.len()) {
            if next < k {
                self.last_shrink = Some(now);
                let n = self.shrink_windows.min(self.windows.len()).max(1);
                let before = self
                    .windows
                    .iter()
                    .rev()
                    .take(n)
                    .map(|w| w.ops)
                    .sum::<u64>();
                self.regret = Some((k, before as f64 / n as f64));
            } else if self
                .last_shrink
                .is_some_and(|at| now.duration_since(at) < UNDO)
            {
                // A shrink undone: wait twice as long before the next one (D255).
                self.shrink_windows = (self.shrink_windows * 2).min(MAX_SHRINK_WINDOWS);
            }
            if next > k {
                self.regret = None;
            }
            self.windows.clear();
            self.hold_until = now + HOLD;
            drivers.set_target(next);
        } else if self.windows.len() >= GROW_WINDOWS {
            // The shrink held its throughput for a second: it stands.
            self.regret = None;
        }
    }

    /// The next target, if the windows since the last resize call for one.
    fn decide(&self, k: usize, ceiling: usize) -> Option<usize> {
        let recent = |n: usize| self.windows.iter().rev().take(n);
        // A shrink that cost throughput comes back first (the windows were cleared at it, so
        // these are all after it).
        if let Some((before_k, before_ops)) = self.regret
            && before_k > k
            && self.windows.len() >= GROW_WINDOWS
        {
            let after =
                recent(GROW_WINDOWS).map(|w| w.ops).sum::<u64>() as f64 / GROW_WINDOWS as f64;
            if after < before_ops * (1.0 - SHRINK_REGRET) {
                return Some(before_k);
            }
        }
        if k < ceiling && self.windows.len() >= GROW_WINDOWS {
            let n = GROW_WINDOWS as f64;
            let busy = recent(GROW_WINDOWS).map(|w| w.max_busy).sum::<f64>() / n;
            let slow = recent(GROW_WINDOWS).map(|w| w.slow).sum::<f64>() / n;
            // Doubling, not one at a time: a driver's busy saturates at 1, so it can't say how
            // many more the work needs, and stepping up one per 3 s left transfer at 2-12 of 16
            // drivers through its measured phase (-10%; #1006's second A/B).
            if busy > GROW_BUSY || slow > SLOW_PASSES {
                return Some((2 * k).min(ceiling));
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
            let needed = ((sum_busy / SHRINK_BUSY).ceil() as usize).max(1);
            if wakes_per_op > SHRINK_WAKES_PER_OP
                && busy_enough
                && needed < k
                && slow <= SLOW_PASSES
            {
                return Some(needed);
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
    debug_assert!(
        cores
            .iter()
            .all(|c| Arc::ptr_eq(&c.vfs, &vfs) && c.spin.window == cores[0].spin.window)
    );
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
    let mut controller = (d == 0 && drivers.adaptive).then(|| Controller::new(&drivers));
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
            if vfs.monotonic_nanos().saturating_sub(pass_start) > PASS_BUDGET_NANOS {
                stats.slow.fetch_add(1, Ordering::Relaxed);
            }
        }
        if let Some(c) = controller.as_mut() {
            c.tick(&drivers);
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
        if pigeonhole_io::own_io_in_flight() {
            if spin_before_park(&mut cores, true) {
                continue;
            }
            let now = vfs.monotonic_nanos();
            let until = next_deadline(&cores)
                .map_or(RING_IDLE_WAIT, |d| {
                    Duration::from_nanos(d.saturating_sub(now))
                })
                .min(RING_IDLE_WAIT);
            pigeonhole_io::reap_own_io(Some(until));
            continue;
        }
        if spin_before_park(&mut cores, false) {
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
fn spin_before_park<H: ShardHandler>(cores: &mut [ShardCore<H>], reap: bool) -> bool {
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
    let window = cores[0].spin.window;
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
    use super::{Controller, GROW_WINDOWS, SHRINK_WINDOWS, Window};
    use std::collections::VecDeque;
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
            regret: None,
        }
    }

    fn window(max_busy: f64, sum_busy: f64, slow: f64, wakes: u64, ops: u64) -> Window {
        Window {
            max_busy,
            sum_busy,
            slow,
            wakes,
            ops,
        }
    }

    /// A light, wake-heavy load shrinks straight to the drivers its work needs at 0.5 busy
    /// each, not one at a time.
    #[test]
    fn a_light_load_shrinks_to_what_it_needs_in_one_step() {
        let c = controller(vec![window(0.1, 0.6, 0.0, 100, 1000); SHRINK_WINDOWS]);
        assert_eq!(c.decide(16, 16), Some(2));
        // Too little evidence yet: no change.
        let c = controller(vec![window(0.1, 0.6, 0.0, 100, 1000); SHRINK_WINDOWS - 1]);
        assert_eq!(c.decide(16, 16), None);
        // Few producer wakes: the drivers aren't parking, so fewer wouldn't help.
        let c = controller(vec![window(0.1, 0.6, 0.0, 10, 1000); SHRINK_WINDOWS]);
        assert_eq!(c.decide(16, 16), None);
        // Slow passes: no shrink, even with wakes.
        let c = controller(vec![window(0.1, 0.6, 0.05, 100, 1000); SHRINK_WINDOWS]);
        assert_eq!(c.decide(16, 16), None);
    }

    /// An idle store doesn't shrink, whatever its wakes per op: its drivers park.
    #[test]
    fn an_idle_store_does_not_shrink() {
        let c = controller(vec![window(0.0, 0.0, 0.0, 5, 3); SHRINK_WINDOWS]);
        assert_eq!(c.decide(16, 16), None);
    }

    /// A shrink whose next second runs more than 3% fewer ops is undone.
    #[test]
    fn a_shrink_that_costs_throughput_is_undone() {
        let mut c = controller(vec![window(0.5, 2.0, 0.0, 0, 9_000); GROW_WINDOWS]);
        c.regret = Some((16, 10_000.0));
        assert_eq!(c.decide(8, 16), Some(16));
        // Throughput held: the shrink stands.
        let mut c = controller(vec![window(0.5, 2.0, 0.0, 0, 9_900); GROW_WINDOWS]);
        c.regret = Some((16, 10_000.0));
        assert_eq!(c.decide(8, 16), None);
    }

    /// A busy driver or slow passes double the set, up to the ceiling.
    #[test]
    fn a_busy_driver_or_slow_passes_grow_the_set() {
        let c = controller(vec![window(0.9, 1.8, 0.0, 0, 1000); GROW_WINDOWS]);
        assert_eq!(c.decide(2, 16), Some(4));
        let c = controller(vec![window(0.3, 0.6, 0.02, 0, 1000); GROW_WINDOWS]);
        assert_eq!(c.decide(2, 16), Some(4));
        let c = controller(vec![window(0.9, 9.0, 0.0, 0, 1000); GROW_WINDOWS]);
        assert_eq!(c.decide(12, 16), Some(16));
        // At the ceiling: no growth.
        let c = controller(vec![window(0.9, 1.8, 0.0, 0, 1000); GROW_WINDOWS]);
        assert_eq!(c.decide(16, 16), None);
    }
}
