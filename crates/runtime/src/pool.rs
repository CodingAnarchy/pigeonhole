//! Background workers (D284): the drivers flush and compaction run on when they leave the
//! shards.
//!
//! A shard hands a task it spawns to the pool's injector ([`Pool::submit`]); any
//! [`PoolDriver`] takes it from there, and keeps it (its sleeping state and wakes) until it
//! finishes or is taken back. Engine-owned mode starts `compaction_threads` threads, each a
//! loop over a driver ([`worker_main`]); application-owned mode hands the drivers to the
//! application, which runs them from threads it owns (D40 holds: the runtime starts none).
//!
//! # Taking tasks back
//! A task a driver holds but is not running a slice of is reclaimable: its state is in the
//! pool's table, not on the driver's thread. An application driver runs every slice inside
//! an [`inline_io`] scope, so when a slice ends nothing it submitted is still in flight
//! anywhere: the task can resume on any thread. Tasks go back to the shard that spawned
//! them ([`Origin`]), which calls [`Task::reclaim`] and runs what it returns:
//! - [`WorkerPool::close`] (the engine's close): every held and queued task, waiting only for
//!   slices running at that moment. The only condvar wait in the pool (`slice_done`); the
//!   caller holds no lock a slice could take (concurrency.md).
//! - a room wait on the shard ([`ShardContext::reclaim_room`](crate::ShardContext)): its own
//!   commit-path tasks (flushes, the manifest pump, WAL spares) that could run but have not for [`ROOM_RECLAIM`], never
//!   waiting: a flush a worker is running stays there.
//! - a dropped driver: everything it held. With no driver left, queued tasks too, and later
//!   spawns stay on the shards.
//! - a slice that panicked: its task, as failed ([`Task::reclaim`]'s `failed`), when one of
//!   the above takes it.
//! - tasks queued for [`FALLBACK`] while no driver made a pass: the shard takes its own back
//!   (counted, [`Submitter::background_take_backs`](crate::Submitter::background_take_backs)).
//!
//! Engine-owned threads' drivers are never taken from: the engine drives them until
//! shutdown, and their I/O goes to their own rings (D252), which only they reap.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, PoisonError};
use std::thread;
use std::time::Duration;

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
#[cfg(loom)]
use loom::sync::{Condvar, Mutex, MutexGuard};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::{Condvar, Mutex, MutexGuard};

use pigeonhole_io::{VfsRef, inline_io};

use crate::Wakes;
use crate::sched::{IdlePark, Moved, Scheduler, Task, TaskPoll};
use crate::signal::{Signal, WakeTarget};

/// How long a shard's commit-path task (a flush) may sit runnable on a worker without a
/// slice before a room wait on that shard takes it back (D284): four slices.
pub(crate) const ROOM_RECLAIM: u64 = 2_000_000;

/// How long a task may sit in the injector, with no driver making a pass, before the shard
/// that spawned it takes it back (D284): workers configured but not driven.
pub(crate) const FALLBACK: u64 = 10_000_000;

/// How long an application's driver may make no pass (no turn started, no slice ended)
/// before the shards take back the tasks it holds that could run but don't (D284): long
/// enough that a driven worker in one slow slice (an inline sync on a slow device) does not
/// usually look stopped, since a compaction taken back is dropped and redone. Its blocked
/// tasks stay: their wake is what they wait for, not the driver (#1012).
pub(crate) const STOPPED: u64 = 100_000_000;

/// Tasks on their way back to a shard (D284), and whether the shard is gone.
#[derive(Default)]
struct Back {
    tasks: Vec<Moved>,
    /// The shard's spawner was dropped: nothing will take tasks back, so they are dropped
    /// instead of kept. A kept one would hold the `Origin` it names (`Moved::origin`) and,
    /// through the task, the engine's state and VFS, a cycle nothing breaks (#1070).
    gone: bool,
}

/// Where a shard's tasks go back to (D284), and its share of the pool's accounting.
pub(crate) struct Origin {
    /// The shard's signal: slices' time counts to it (D256), and it is notified when tasks
    /// come back.
    pub(crate) signal: Arc<Signal>,
    back: Mutex<Back>,
    has_back: AtomicBool,
    /// This shard's tasks in the injector.
    queued: AtomicUsize,
    /// This shard's tasks a driver holds (D284): its turns look for a stopped driver only
    /// then.
    pub(crate) held: AtomicUsize,
}

impl fmt::Debug for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Origin")
            .field("queued", &self.queued.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Origin {
    pub(crate) fn new(signal: Arc<Signal>) -> Arc<Self> {
        Arc::new(Self {
            signal,
            back: Mutex::new(Back::default()),
            has_back: AtomicBool::new(false),
            queued: AtomicUsize::new(0),
            held: AtomicUsize::new(0),
        })
    }

    /// Whether tasks are waiting to come back (one relaxed load; the shard's sleep handshake
    /// fences before it).
    pub(crate) fn has_back(&self) -> bool {
        self.has_back.load(Ordering::Relaxed)
    }

    /// Takes the tasks that came back.
    pub(crate) fn take_back(&self) -> Vec<Moved> {
        if !self.has_back.swap(false, Ordering::Acquire) {
            return Vec::new();
        }
        std::mem::take(
            &mut self
                .back
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .tasks,
        )
    }

    /// The shard's spawner is dropped (#1070): drops the tasks waiting to come back, and
    /// every later one. Their destructors run after the lock is released (a task's drop
    /// runs engine code).
    pub(crate) fn shard_gone(&self) {
        let tasks = {
            let mut back = self.back.lock().unwrap_or_else(PoisonError::into_inner);
            back.gone = true;
            std::mem::take(&mut back.tasks)
        };
        self.has_back.store(false, Ordering::Relaxed);
        drop(tasks);
    }

    /// Whether this shard has tasks waiting in the injector.
    pub(crate) fn queued(&self) -> bool {
        self.queued.load(Ordering::Relaxed) > 0
    }

    /// Whether a driver holds tasks of this shard's.
    pub(crate) fn held(&self) -> bool {
        self.held.load(Ordering::Relaxed) > 0
    }

    fn give_back(&self, moved: Moved) {
        {
            let mut back = self.back.lock().unwrap_or_else(PoisonError::into_inner);
            if !back.gone {
                back.tasks.push(moved);
                drop(back);
                self.has_back.store(true, Ordering::Release);
                // The shard checks `has_back` after announcing sleep: either it sees this or
                // the notify sees it asleep and wakes it.
                self.signal.notify();
                return;
            }
        }
        // Nobody will take it back: dropped here, outside the lock (#1070).
        drop(moved);
    }
}

/// A task waiting in the injector.
struct Queued {
    task: Box<dyn Task>,
    origin: Arc<Origin>,
    /// When it was queued (VFS clock).
    at: u64,
}

struct PoolState {
    /// Queued commit-path tasks (flushes, the manifest pump, WAL spares): taken ahead of the rest.
    room: VecDeque<Queued>,
    rest: VecDeque<Queued>,
    /// Each driver's tasks; `None` once the driver is dropped.
    drivers: Vec<Option<Scheduler>>,
}

impl PoolState {
    fn queued(&self) -> usize {
        self.room.len() + self.rest.len()
    }

    /// Takes every queued task `pick` selects (by origin and room) queued at or before
    /// `before`.
    fn take_queued(
        &mut self,
        before: u64,
        pick: impl Fn(&Arc<Origin>, bool) -> bool,
        out: &mut Vec<Moved>,
    ) {
        for (q, room) in [(&mut self.room, true), (&mut self.rest, false)] {
            let mut keep = VecDeque::with_capacity(q.len());
            for e in q.drain(..) {
                if e.at <= before && pick(&e.origin, room) {
                    e.origin.queued.fetch_sub(1, Ordering::Relaxed);
                    out.push(Moved {
                        task: e.task,
                        cell: None,
                        origin: e.origin,
                        failed: false,
                    });
                } else {
                    keep.push_back(e);
                }
            }
            *q = keep;
        }
    }
}

/// The background workers' shared state: the injector and every driver's tasks (D284).
pub(crate) struct Pool {
    /// A leaf lock, except that `WorkerPool::close` waits on `slice_done` with it.
    state: Mutex<PoolState>,
    /// Signalled when a slice ends (a driver puts its task back).
    slice_done: Condvar,
    /// Each driver's signal, by driver id (fixed at creation).
    signals: Vec<Arc<Signal>>,
    queued: AtomicUsize,
    closed: AtomicBool,
    /// Drivers not dropped.
    live: AtomicUsize,
    /// The VFS clock at the last pass any driver made: queued tasks fall back to their
    /// shards only when nobody drives (`FALLBACK`).
    last_pass: AtomicU64,
    /// Each driver's last pass (VFS clock: a turn's start or a slice's end), by driver id: a
    /// driver that made none for `STOPPED` gives back the held tasks it could run but doesn't
    /// (application drivers only).
    passes: Vec<AtomicU64>,
    /// Engine-owned threads: never taken from.
    builtin: bool,
    vfs: VfsRef,
    time_slice: u64,
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("drivers", &self.signals.len())
            .field("queued", &self.queued.load(Ordering::Relaxed))
            .field("builtin", &self.builtin)
            .finish_non_exhaustive()
    }
}

impl Pool {
    /// A pool of `k` drivers (`builtin`: engine-owned threads will run them).
    pub(crate) fn new(
        k: usize,
        builtin: bool,
        vfs: VfsRef,
        time_slice: u64,
    ) -> (Arc<Self>, Vec<PoolDriver>) {
        let signals: Vec<Arc<Signal>> = (0..k).map(|_| Arc::new(Signal::new())).collect();
        let pool = Arc::new(Self {
            state: Mutex::new(PoolState {
                room: VecDeque::new(),
                rest: VecDeque::new(),
                drivers: signals
                    .iter()
                    .map(|s| Some(Scheduler::new(Arc::clone(s))))
                    .collect(),
            }),
            slice_done: Condvar::new(),
            signals,
            queued: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            live: AtomicUsize::new(k),
            last_pass: AtomicU64::new(0),
            passes: (0..k).map(|_| AtomicU64::new(0)).collect(),
            builtin,
            vfs,
            time_slice,
        });
        let drivers = (0..k)
            .map(|id| PoolDriver {
                pool: Arc::clone(&pool),
                id,
                idle_at: None,
                wakes: Wakes::default(),
            })
            .collect();
        (pool, drivers)
    }

    fn lock(&self) -> MutexGuard<'_, PoolState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn now(&self) -> u64 {
        self.vfs.monotonic_nanos()
    }

    /// Queues `task` from `origin`'s shard, or hands it back when it must stay on the shard:
    /// it does not offload ([`Task::offload`]), the pool is closed, or no driver is left.
    pub(crate) fn submit(
        &self,
        task: Box<dyn Task>,
        origin: &Arc<Origin>,
    ) -> Result<(), Box<dyn Task>> {
        if !task.offload() {
            return Err(task);
        }
        let room = task.commit_path();
        let at = self.now();
        {
            let mut st = self.lock();
            if self.closed.load(Ordering::Acquire) || self.live.load(Ordering::Acquire) == 0 {
                return Err(task);
            }
            let q = Queued {
                task,
                origin: Arc::clone(origin),
                at,
            };
            if room {
                st.room.push_back(q);
            } else {
                st.rest.push_back(q);
            }
            origin.queued.fetch_add(1, Ordering::Relaxed);
            self.queued.fetch_add(1, Ordering::Release);
        }
        // Every idle driver looks; one takes it (a driver's sleep handshake checks
        // `queued` after announcing sleep).
        for s in &self.signals {
            s.notify();
        }
        Ok(())
    }

    /// Moves a queued task into driver `id`'s scheduler if the take policy allows: a
    /// commit-path task always (one per call), any other only while the driver has nothing
    /// runnable or holds fewer than two tasks (so the first driver to look does not take
    /// everything), or always on engine-owned threads, which are always driven.
    fn take_new(&self, st: &mut PoolState, id: usize, now: u64) {
        if st.queued() == 0 {
            return;
        }
        let Some(sched) = st.drivers[id].as_ref() else {
            return;
        };
        let q = if !st.room.is_empty() {
            st.room.pop_front()
        } else if self.builtin || !sched.has_runnable() || sched.held() < 2 {
            st.rest.pop_front()
        } else {
            None
        };
        let Some(q) = q else {
            return;
        };
        self.queued.fetch_sub(1, Ordering::Relaxed);
        q.origin.queued.fetch_sub(1, Ordering::Relaxed);
        if let Some(sched) = st.drivers[id].as_mut() {
            sched.adopt(q.task, None, Some(q.origin), now);
        }
    }

    /// Closes the pool and takes every task back to its shard: queued ones, and those the
    /// application's drivers hold, waiting only for slices running now (D284). Later
    /// spawns stay on the shards, and the drivers' `run_once` returns `false`.
    ///
    /// The one condvar wait in the pool: the caller must hold no lock a slice could take.
    pub(crate) fn close_and_reclaim(&self) {
        let mut moved = Vec::new();
        let mut st = self.lock();
        self.closed.store(true, Ordering::Release);
        st.take_queued(u64::MAX, |_, _| true, &mut moved);
        self.queued.store(0, Ordering::Relaxed);
        if !self.builtin {
            loop {
                for sched in st.drivers.iter_mut().flatten() {
                    sched.take_idle(u64::MAX, false, 0, |_, _| true, &mut moved);
                }
                if !st.drivers.iter().flatten().any(|s| s.running(|_, _| true)) {
                    break;
                }
                st = self
                    .slice_done
                    .wait(st)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }
        drop(st);
        for s in &self.signals {
            s.force_wake();
        }
        deliver(moved);
    }

    /// Engine-owned shutdown, after the shards are joined: the threads stop; what they
    /// still hold is dropped unfinished, as tasks on shards are.
    pub(crate) fn close_builtin(&self) {
        let st = self.lock();
        self.closed.store(true, Ordering::Release);
        drop(st);
        for s in &self.signals {
            s.force_wake();
        }
    }

    pub(crate) fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// A room wait on `origin`'s shard (D284): takes back its commit-path tasks that could
    /// run but have not for [`ROOM_RECLAIM`] (held by an application's driver that is not
    /// running them, or queued with no driver taking them). Never waits. Returns them, and
    /// when to look again for one not yet that idle.
    pub(crate) fn reclaim_room(&self, origin: &Arc<Origin>, now: u64) -> (Vec<Moved>, Option<u64>) {
        let mut moved = Vec::new();
        if self.builtin {
            return (moved, None);
        }
        // Nothing can have been idle that long before the clock got that far (the takes
        // below are skipped then).
        let before = now.saturating_sub(ROOM_RECLAIM);
        let mine = |o: &Arc<Origin>, room: bool| room && Arc::ptr_eq(o, origin);
        let mut st = self.lock();
        if now >= ROOM_RECLAIM && self.last_pass.load(Ordering::Relaxed) <= before {
            st.take_queued(before, mine, &mut moved);
            self.queued.fetch_sub(moved.len(), Ordering::Relaxed);
        }
        if now >= ROOM_RECLAIM {
            for sched in st.drivers.iter_mut().flatten() {
                sched.take_idle(before, true, 0, mine, &mut moved);
            }
        }
        let held = st
            .drivers
            .iter()
            .flatten()
            .filter_map(|s| s.stalled_since(mine))
            .min();
        let queued = st
            .room
            .iter()
            .filter(|q| Arc::ptr_eq(&q.origin, origin))
            .map(|q| q.at)
            .min();
        drop(st);
        let next = held
            .into_iter()
            .chain(queued)
            .min()
            .map(|t| t.saturating_add(ROOM_RECLAIM).max(now.saturating_add(1)));
        (moved, next)
    }

    /// What `origin`'s shard takes back because nobody drives it (workers configured but
    /// not driven, or one the application stopped driving without dropping it): its tasks
    /// queued for [`FALLBACK`] while no driver made a pass, and every task of its an
    /// application driver holds that could run (runnable, woken, or asleep past its
    /// deadline) without running it, once that driver has made no pass for [`STOPPED`] (a
    /// compaction is then dropped and picked again, rather than keep its inputs busy until
    /// the close). A blocked task stays until it is woken (#1012).
    pub(crate) fn take_stale(&self, origin: &Arc<Origin>, now: u64) -> Vec<Moved> {
        let mut moved = Vec::new();
        let Some(before) = now.checked_sub(FALLBACK) else {
            return moved;
        };
        let mine = |o: &Arc<Origin>, _: bool| Arc::ptr_eq(o, origin);
        let mut st = self.lock();
        if self.last_pass.load(Ordering::Relaxed) <= before {
            st.take_queued(before, mine, &mut moved);
            self.queued.fetch_sub(moved.len(), Ordering::Relaxed);
        }
        if !self.builtin
            && let Some(stopped_before) = now.checked_sub(STOPPED)
        {
            for (id, sched) in st.drivers.iter_mut().enumerate() {
                // A driver with a slice running is being driven, however long the slice
                // (perf287's review of #1014): its runnable tasks wait for that slice, not
                // for a driver that stopped. The close still waits for the slice.
                if let Some(sched) = sched
                    && self.passes[id].load(Ordering::Relaxed) <= stopped_before
                    && !sched.running(|_, _| true)
                {
                    // Only what it could run: a blocked task waits on a wake its driver
                    // would hear anyway (`notify_task` wakes a sleeping driver), and a
                    // driver deep in one slow slice (an inline sync on a slow device: over
                    // 100 ms on Windows CI) has made no pass without having stopped (#1012).
                    // Once woken, the task is taken at the next look if the driver still
                    // makes none.
                    sched.take_idle(u64::MAX, true, now, mine, &mut moved);
                }
            }
        }
        drop(st);
        moved
    }

    /// Engine-owned threads' pool (never taken from).
    pub(crate) fn builtin(&self) -> bool {
        self.builtin
    }

    /// Driver `id` is dropped: everything it held goes back to its shards (dropped, on a
    /// closed engine-owned pool). With no driver left, queued tasks go back too and later
    /// spawns stay on the shards.
    fn retire(&self, id: usize) {
        let mut moved = Vec::new();
        let mut st = self.lock();
        if let Some(mut sched) = st.drivers[id].take() {
            sched.take_idle(u64::MAX, false, 0, |_, _| true, &mut moved);
        }
        if self.live.fetch_sub(1, Ordering::AcqRel) == 1 {
            st.take_queued(u64::MAX, |_, _| true, &mut moved);
            self.queued.store(0, Ordering::Relaxed);
        }
        drop(st);
        if self.builtin && self.closed() {
            return;
        }
        deliver(moved);
    }
}

/// Hands tasks back to the shards that spawned them.
fn deliver(moved: Vec<Moved>) {
    for m in moved {
        let origin = Arc::clone(&m.origin);
        origin.give_back(m);
    }
}

/// Runs one slice's task back into its scheduler on every exit, and on a panic marks it
/// failed so a reclaim takes it back rather than waiting for it (D284).
struct Slice<'a> {
    pool: &'a Pool,
    id: usize,
    slot: usize,
    task: Option<Box<dyn Task>>,
}

impl Drop for Slice<'_> {
    fn drop(&mut self) {
        // Still here: the slice unwound.
        if let Some(task) = self.task.take() {
            let now = self.pool.now();
            let mut st = self.pool.lock();
            if let Some(sched) = st.drivers[self.id].as_mut() {
                sched.poison(self.slot, task, now);
            }
            drop(st);
            self.pool.slice_done.notify_all();
        }
    }
}

/// Drives one background worker: flushes and compactions the shards hand to the pool (D284).
///
/// Engine-owned mode runs each on a thread of its own; application-owned mode
/// ([`Runtime::application_owned_with_workers`](crate::Runtime::application_owned_with_workers))
/// hands them to the application, which calls [`run_once`](PoolDriver::run_once) from a
/// thread it owns, like a [`ShardDriver`](crate::ShardDriver)'s. Each of its slices runs its
/// I/O inline ([`pigeonhole_io::inline_io`]), so a task it holds can always be taken back:
/// the engine's close never waits for the application to drive it, only for a slice running
/// at that moment.
///
/// Dropping it hands what it holds back to the shards.
pub struct PoolDriver {
    pool: Arc<Pool>,
    id: usize,
    /// The clock when it last went idle with sleeping tasks (see `ShardCore::idle_at`).
    idle_at: Option<u64>,
    wakes: Wakes,
}

impl fmt::Debug for PoolDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolDriver")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl PoolDriver {
    fn signal(&self) -> &Arc<Signal> {
        &self.pool.signals[self.id]
    }

    /// Runs the worker's tasks until `deadline_nanos` (VFS monotonic clock) or until none is
    /// runnable, taking new ones from the shards as its policy allows. Returns whether work
    /// remains (call again soon); with `false`, wait for the wakeup or
    /// [`next_deadline`](Self::next_deadline). Returns `false` once the pool is closed.
    pub fn run_once(&mut self, deadline_nanos: u64) -> bool {
        if self.turn(deadline_nanos, !self.pool.builtin) {
            return true;
        }
        // A closed pool runs nothing: `false` for good, whatever its queue count says.
        if self.pool.closed() {
            return false;
        }
        // As a shard's turn ends (`check_before_sleep`), and as `worker_main` does after its
        // turns: announce sleep, then look again. A task queued or woken from here on finds
        // the worker asleep and wakes its target, the `set_wakeup` callback, so an
        // application that sleeps on its own terms after a `false` hears of new work as it
        // does for a shard. Before, only `wait` announced sleep, and a worker parked in the
        // application's own loop slept through new tasks until the shards took them back
        // (found by D284 PR 2's hand-driven worker test).
        let signal = self.signal();
        signal.prepare_sleep();
        // As `worker_main` and `wait` re-check: a close landing between the check above and
        // the announce returns `false` here, whatever the application's wake does with a
        // `force_wake` that came before its wait (perf287's review of #1010).
        if self.pool.closed() {
            return false;
        }
        if self.pool.queued.load(Ordering::Acquire) > 0 || signal.task_woken() {
            signal.awake();
            return true;
        }
        false
    }

    fn turn(&mut self, deadline: u64, inline: bool) -> bool {
        let pool = Arc::clone(&self.pool);
        if pool.closed() {
            return false;
        }
        let mut now = pool.now();
        pool.last_pass.store(now, Ordering::Relaxed);
        pool.passes[self.id].store(now, Ordering::Relaxed);
        self.signal().awake();
        let mut st = pool.lock();
        loop {
            if pool.closed() {
                return false;
            }
            {
                let Some(sched) = st.drivers[self.id].as_mut() else {
                    return false;
                };
                sched.collect_woken();
                if self.idle_at.take() == Some(now) && pool.vfs.clock_is_simulated() {
                    // A simulated clock has not moved since we went idle: sleepers check
                    // for themselves (#263).
                    sched.wake_sleepers();
                }
                sched.wake_due(now);
            }
            pool.take_new(&mut st, self.id, now);
            let Some(sched) = st.drivers[self.id].as_mut() else {
                return false;
            };
            let Some(begun) = sched.begin_one(true) else {
                break;
            };
            drop(st);
            let mut slice = Slice {
                pool: &pool,
                id: self.id,
                slot: begun.slot,
                task: Some(begun.task),
            };
            let end = now.saturating_add(pool.time_slice);
            let started = pool.now();
            let poll = match slice.task.as_mut() {
                Some(task) if inline => inline_io(|| task.run(end, &begun.waker)),
                Some(task) => task.run(end, &begun.waker),
                None => TaskPoll::Done,
            };
            let task = slice.task.take();
            drop(slice);
            let after = pool.now();
            // The slice's time counts to the shard that spawned the task (D256): the task's
            // run alone, not the pool lock or the takes before it.
            if let Some(origin) = &begun.origin {
                origin.signal.add_pool_busy(after.saturating_sub(started));
            }
            // A finished task is dropped here, before the pool lock: its destructor may run
            // engine code (settle callbacks, the release of a view it held), which must not
            // nest inside `Pool::state` (lock order item 12).
            let task = match poll {
                TaskPoll::Done => {
                    drop(task);
                    None
                }
                _ => task,
            };
            pool.passes[self.id].store(after, Ordering::Relaxed);
            st = pool.lock();
            if let Some(sched) = st.drivers[self.id].as_mut() {
                sched.end_one(begun.slot, task, poll, after);
            }
            pool.slice_done.notify_all();
            now = after;
            if now >= deadline {
                break;
            }
        }
        let more = st.drivers[self.id].as_ref().is_some_and(|s| {
            s.has_runnable()
                || (st.queued() > 0 && (!st.room.is_empty() || !s.has_runnable() || s.held() < 2))
        });
        if !more
            && st.drivers[self.id]
                .as_ref()
                .is_some_and(|s| s.next_deadline().is_some())
        {
            self.idle_at = Some(now);
        }
        more
    }

    /// The earliest deadline (VFS monotonic clock) of a task sleeping on this worker, or
    /// `None`: after [`run_once`](Self::run_once) returns `false`, call it again by then even
    /// if no wakeup arrives.
    pub fn next_deadline(&self) -> Option<u64> {
        self.pool.lock().drivers[self.id]
            .as_ref()
            .and_then(Scheduler::next_deadline)
    }

    /// Whether the pool is closed (the engine closed, D284): `run_once` does nothing from
    /// now on, and the worker can be dropped.
    pub fn closed(&self) -> bool {
        self.pool.closed()
    }

    /// Registers a waker called when this worker has work again (a task queued or woken),
    /// as [`ShardDriver::set_wakeup`](crate::ShardDriver::set_wakeup). Called once right
    /// away.
    pub fn set_wakeup(&mut self, wake: Box<dyn Fn() + Send + Sync>) {
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::from(wake);
        self.wakes.callback = Some(Arc::clone(&wake));
        self.signal().set_target(self.wakes.target());
        wake();
    }

    /// Waits until this worker has work again (a task queued or woken, a
    /// [`waker`](Self::waker) wake, the pool's close), the next sleeping task's deadline, or
    /// `timeout`, on the thread that runs it. A spurious return is possible: loop on
    /// [`run_once`](Self::run_once).
    pub fn wait(&mut self, timeout: Duration) {
        let me = thread::current();
        if self.wakes.waiter.as_ref().map(|(t, _)| t.id()) != Some(me.id()) {
            // Its slices' I/O runs inline: nothing waits in a ring of this thread's.
            self.wakes.waiter = Some((me, None));
            self.signal().set_target(self.wakes.target());
        }
        let now = self.pool.now();
        let wait = self.next_deadline().map_or(timeout, |d| {
            Duration::from_nanos(d.saturating_sub(now)).min(timeout)
        });
        if wait.is_zero() {
            return;
        }
        let pool = &self.pool;
        self.signal().sleep_unless(
            || pool.queued.load(Ordering::Acquire) > 0 || pool.closed(),
            || {
                debug_assert!(
                    !pigeonhole_io::in_async_poll(),
                    "a pool worker parks inside an async poll (a lost wake, #1226)"
                );
                thread::park_timeout(wait)
            },
        );
    }

    /// A waker that ends this worker's [`wait`](Self::wait) from any thread.
    pub fn waker(&self) -> std::task::Waker {
        std::task::Waker::from(Arc::new(crate::WaitWaker(Arc::clone(self.signal()))))
    }
}

impl Drop for PoolDriver {
    fn drop(&mut self) {
        self.pool.retire(self.id);
    }
}

/// The handle the engine keeps to an application-owned mode's workers (D284): closes them
/// at the engine's close.
#[derive(Debug, Clone)]
pub struct WorkerPool(pub(crate) Arc<Pool>);

impl WorkerPool {
    /// Closes the workers and takes every task they hold or have queued back to the shards
    /// that spawned them, which run them from then on (flushes resume; compactions are
    /// dropped at their next slice, see [`Task::reclaim`]). Waits only for slices running
    /// at this moment, never for the application to drive a worker. Later spawns stay on
    /// the shards. Call it holding no lock a task's slice could take.
    pub fn close(&self) {
        self.0.close_and_reclaim();
    }
}

/// Body of an engine-owned compaction thread (D252): a loop over its driver, with a ring of
/// its own where the backend has them.
pub(crate) fn worker_main(mut driver: PoolDriver, vfs: VfsRef) {
    // A ring of this thread's own where the backend has them (io_uring, #402), as a shard
    // thread has: what its tasks submit (a compaction's block reads, a flush's SST writes,
    // the manifest pump's root commit) completes here, on the thread that waits for it,
    // not on the backend's shared ring and its one reaper thread, which every other
    // thread's reads already queue on (D252's stall under io_uring).
    vfs.attach_thread();
    let own_ring = pigeonhole_io::own_io_waker().is_some();
    driver
        .signal()
        .set_target(match pigeonhole_io::own_io_waker() {
            Some(w) => WakeTarget::ThreadRing(thread::current(), w),
            None => WakeTarget::Thread(thread::current()),
        });
    let pool = Arc::clone(&driver.pool);
    let slice = pool.time_slice;
    let mut idle_park = IdlePark::default();
    loop {
        if pool.closed() {
            return;
        }
        // What completed on this thread's ring resolves now, before the wakes it causes
        // are collected, so the tasks it unblocks run this pass.
        if own_ring {
            pigeonhole_io::reap_own_io(None);
        }
        let now = vfs.monotonic_nanos();
        // One slice per pass, as before D284: the ring is reaped between slices.
        if driver.turn(now.saturating_add(slice), false) {
            continue;
        }
        let signal = Arc::clone(driver.signal());
        signal.prepare_sleep();
        if pool.queued.load(Ordering::Acquire) > 0 || signal.task_woken() || pool.closed() {
            signal.awake();
            continue;
        }
        if own_ring && pigeonhole_io::own_io_in_flight() {
            // I/O only this thread completes (its ring): wait on it rather than park, until
            // a completion, a wake (the ring's eventfd) or the next sleeper's deadline.
            let until = driver
                .next_deadline()
                .map_or(RING_IDLE_WAIT, |d| {
                    Duration::from_nanos(d.saturating_sub(now))
                })
                .min(RING_IDLE_WAIT);
            pigeonhole_io::reap_own_io(Some(until));
            signal.awake();
            continue;
        }
        match driver.next_deadline() {
            None => {
                debug_assert!(
                    !pigeonhole_io::in_async_poll(),
                    "an idle pool worker parks inside an async poll (a lost wake, #1226)"
                );
                thread::park()
            }
            Some(deadline) => idle_park.park(&*vfs, deadline),
        }
        signal.awake();
    }
}

/// Longest an idle compaction thread waits on its own I/O ring before it looks again (a
/// wake or a completion ends the wait sooner).
const RING_IDLE_WAIT: Duration = Duration::from_millis(100);

#[cfg(all(test, loom))]
mod loom_tests {
    use super::{Origin, Pool};
    use crate::sched::{Task, TaskPoll, TaskWaker};
    use crate::signal::{Signal, WakeTarget};
    use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use loom::thread;
    use std::sync::Arc;

    /// Runs forever, one `Pending` slice at a time, counting its slices.
    struct Forever(Arc<AtomicUsize>);

    impl Task for Forever {
        fn run(&mut self, _deadline: u64, _waker: &TaskWaker) -> TaskPoll {
            self.0.fetch_add(1, Ordering::SeqCst);
            TaskPoll::Pending
        }
        fn name(&self) -> &'static str {
            "forever"
        }
    }

    fn pool(k: usize) -> (Arc<Pool>, Vec<super::PoolDriver>, Arc<Origin>) {
        let (pool, drivers) = Pool::new(k, false, pigeonhole_io::sim::SimVfs::new(1), 1);
        (pool, drivers, Origin::new(Arc::new(Signal::new())))
    }

    #[test]
    fn loom_close_waits_for_a_running_slice_and_takes_the_task_back_once() {
        // A worker runs a slice while the engine's close reclaims: the close never takes the
        // task while its slice runs, waits for it on the condvar, and hands it back exactly
        // once; the worker runs nothing after the close.
        loom::model(|| {
            let (pool, mut drivers, origin) = pool(1);
            let runs = Arc::new(AtomicUsize::new(0));
            assert!(
                pool.submit(Box::new(Forever(Arc::clone(&runs))), &origin)
                    .is_ok()
            );
            let mut driver = drivers.pop().unwrap();
            let worker = thread::spawn(move || {
                driver.run_once(0);
                driver
            });
            pool.close_and_reclaim();
            let mut driver = worker.join().unwrap();
            let back = origin.take_back();
            assert_eq!(back.len(), 1);
            let after = runs.load(Ordering::SeqCst);
            assert!(after <= 1);
            assert!(!driver.run_once(0));
            assert_eq!(runs.load(Ordering::SeqCst), after);
        });
    }

    #[test]
    fn loom_a_submit_racing_a_workers_sleep_wakes_it() {
        // The worker's sleep handshake against a shard's submit: either the worker sees the
        // queued task after announcing sleep, or the submit sees it asleep and wakes it.
        loom::model(|| {
            let (pool, mut drivers, origin) = pool(1);
            let mut driver = drivers.pop().unwrap();
            let woken = Arc::new(AtomicBool::new(false));
            {
                let woken = Arc::clone(&woken);
                driver
                    .signal()
                    .set_target(WakeTarget::Callback(Arc::new(move || {
                        woken.store(true, Ordering::SeqCst);
                    })));
            }
            let runs = Arc::new(AtomicUsize::new(0));
            let producer = {
                let (pool, origin, runs) =
                    (Arc::clone(&pool), Arc::clone(&origin), Arc::clone(&runs));
                thread::spawn(move || {
                    assert!(pool.submit(Box::new(Forever(runs)), &origin).is_ok());
                })
            };
            if !driver.run_once(0) {
                let signal = Arc::clone(driver.signal());
                let slept = signal.sleep_unless(
                    || pool.queued.load(Ordering::SeqCst) > 0,
                    || {
                        // Asleep: only the producer's wake may end it.
                        while !woken.load(Ordering::SeqCst) {
                            thread::yield_now();
                        }
                    },
                );
                let _ = slept;
                producer.join().unwrap();
                driver.run_once(0);
            } else {
                // `true` says work remains: either the turn ran the task, or the sleep
                // handshake's re-check found it queued, and the next call runs it.
                producer.join().unwrap();
                driver.run_once(0);
            }
            assert!(runs.load(Ordering::SeqCst) >= 1);
            pool.close_and_reclaim();
        });
    }

    #[test]
    fn loom_the_last_drivers_drop_racing_a_submit_loses_no_task() {
        // The last worker is dropped while a shard submits: the task is either refused (the
        // shard runs it) or queued and handed back by the drop, never left behind.
        loom::model(|| {
            let (pool, mut drivers, origin) = pool(1);
            let driver = drivers.pop().unwrap();
            let dropper = thread::spawn(move || drop(driver));
            let runs = Arc::new(AtomicUsize::new(0));
            let refused = pool.submit(Box::new(Forever(runs)), &origin).is_err();
            dropper.join().unwrap();
            let back = origin.take_back().len();
            assert_eq!(usize::from(refused) + back, 1);
            assert_eq!(pool.queued.load(Ordering::SeqCst), 0);
        });
    }
}
