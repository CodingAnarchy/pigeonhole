//! Cooperative background tasks: the scheduler shards and background workers run them on.

use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use pigeonhole_io::Vfs;

use crate::pool::{Origin, Pool};
use crate::signal::Signal;

/// What a background task reports after a time slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskPoll {
    /// More work remains; run me again when foreground work allows.
    Pending,
    /// Waiting for I/O or another event; the task is woken by its [`TaskWaker`].
    Blocked,
    /// Nothing to do until the VFS clock (`monotonic_nanos`) reaches this deadline: the task
    /// runs again then, or earlier if its [`TaskWaker`] is woken. A shard with only sleeping
    /// tasks is idle: engine-owned threads park until the earliest deadline (or the next
    /// message), and [`ShardDriver::run_once`](crate::ShardDriver::run_once) returns
    /// `false` with the deadline in
    /// [`ShardDriver::next_deadline`](crate::ShardDriver::next_deadline).
    ///
    /// When the clock has not moved since the shard last went idle (a simulated clock only
    /// the application advances), the runtime runs sleeping tasks early so they can notice:
    /// a sleeping task must tolerate running before its deadline.
    SleepUntil(u64),
    /// Finished.
    Done,
}

/// A cooperative background task (flush, compaction, manifest write, rebalancing).
///
/// The scheduler calls [`Task::run`] with a deadline; the task does a bounded amount of work,
/// checks the clock, and returns before the deadline so queued foreground messages run next.
///
/// ```
/// use pigeonhole_runtime::{Task, TaskPoll, TaskWaker};
///
/// /// Counts down one step per slice.
/// struct Countdown(u32);
///
/// impl Task for Countdown {
///     fn run(&mut self, _deadline_nanos: u64, _waker: &TaskWaker) -> TaskPoll {
///         self.0 -= 1;
///         if self.0 == 0 { TaskPoll::Done } else { TaskPoll::Pending }
///     }
///     fn name(&self) -> &'static str {
///         "countdown"
///     }
/// }
/// # let _ = Countdown(3);
/// ```
pub trait Task: Send + 'static {
    /// Runs until `deadline_nanos` (monotonic) or until it would block, then returns.
    fn run(&mut self, deadline_nanos: u64, waker: &TaskWaker) -> TaskPoll;

    /// A short label for metrics.
    fn name(&self) -> &'static str;

    /// Whether this task is on its shard's commit path: commits wait for it, directly or
    /// for the room it frees (a flush, the manifest pump that commits a flush's output, a
    /// WAL spare a segment rollover waits for). A background worker takes and runs such tasks
    /// ahead of others (a compaction may hold it for seconds), and a room wait on the shard
    /// takes one back from a worker that is not running it (D284).
    fn commit_path(&self) -> bool {
        false
    }

    /// Whether this task may run on a background worker. `false` keeps it on the shard
    /// that spawned it: a timer, which has nothing to offload and must fire whether or not
    /// any worker is driven (D284).
    fn offload(&self) -> bool {
        true
    }

    /// Called on the shard that spawned this task when the runtime takes it back from a
    /// background worker that held it (D284): the worker was dropped, the engine closed or
    /// a room wait took it (with `failed: false`), or a slice of it panicked (`failed: true`,
    /// its state then half-updated). Returns whether to run it on the shard from now on
    /// (`false`: drop it); whatever it reports to the shard, it reports from here or from
    /// its next slice there.
    ///
    /// The default resumes the task on the shard, or drops it if it failed.
    fn reclaim(&mut self, failed: bool) -> bool {
        !failed
    }
}

/// Wakes a blocked background task. Cheap to clone; may be called from any thread, any number
/// of times, before or after the task reports [`TaskPoll::Blocked`].
///
/// ```
/// use std::sync::{Arc, Mutex};
/// use pigeonhole_runtime::{Task, TaskPoll, TaskWaker};
///
/// /// Blocks until someone outside the shard calls `wake`.
/// struct WaitForSignal {
///     waker_out: Arc<Mutex<Option<TaskWaker>>>,
///     woken: bool,
/// }
///
/// impl Task for WaitForSignal {
///     fn run(&mut self, _deadline: u64, waker: &TaskWaker) -> TaskPoll {
///         if self.woken {
///             return TaskPoll::Done;
///         }
///         self.woken = true;
///         *self.waker_out.lock().unwrap() = Some(waker.clone()); // e.g. hand to an I/O backend
///         TaskPoll::Blocked
///     }
///     fn name(&self) -> &'static str {
///         "wait"
///     }
/// }
/// # let _ = WaitForSignal { waker_out: Arc::default(), woken: false };
/// ```
#[derive(Debug, Clone)]
pub struct TaskWaker {
    cell: Arc<TaskCell>,
}

/// A task's woken flag (loom's in the `loom` models).
#[cfg(loom)]
type WokenFlag = loom::sync::atomic::AtomicBool;
#[cfg(not(loom))]
type WokenFlag = AtomicBool;

/// The lock over a task's current thread signal (loom's in the `loom` models).
#[cfg(loom)]
type TargetLock<T> = loom::sync::Mutex<T>;
#[cfg(not(loom))]
type TargetLock<T> = std::sync::Mutex<T>;

/// A task's wake state, kept for the task's whole life: futures it awaits hold wakers built
/// from it (once), so when a background worker hands the task to another scheduler (D284)
/// the cell stays and only its signal changes.
#[derive(Debug)]
pub(crate) struct TaskCell {
    woken: WokenFlag,
    /// The signal of the scheduler that holds the task now. A leaf lock: held only to clone
    /// or replace the `Arc`.
    signal: TargetLock<Arc<Signal>>,
}

impl TaskCell {
    pub(crate) fn new(signal: Arc<Signal>) -> Arc<Self> {
        Arc::new(Self {
            woken: WokenFlag::new(false),
            signal: TargetLock::new(signal),
        })
    }

    fn signal(&self) -> Arc<Signal> {
        Arc::clone(
            &self
                .signal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Points later wakes at `signal`, the new holder's. A wake that read the old signal
    /// went to the old holder; the new one runs the task once on adoption anyway, so the
    /// event behind that wake is seen.
    fn retarget(&self, signal: Arc<Signal>) {
        *self
            .signal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = signal;
    }
}

impl TaskWaker {
    /// Marks the task runnable.
    pub fn wake(&self) {
        self.cell.woken.store(true, Ordering::Release);
        self.cell.signal().notify_task();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Runnable,
    Blocked,
    /// Sleeping until this deadline (also listed in `Scheduler::sleeping`).
    Sleeping(u64),
}

/// Longest an idle thread with sleeping tasks parks before it looks at the clock again,
/// until the clock has been seen to keep pace with real time: a simulated clock moves
/// without waking anyone, and a frozen one must still be noticed.
const MAX_PARK: Duration = Duration::from_millis(10);
/// The same once the clock has kept pace with real time (a real clock): a safety net, not
/// polling.
const MAX_PARK_REAL: Duration = Duration::from_secs(1);

struct Entry {
    /// `None` while a background worker runs a slice of it outside the pool's lock (D284).
    task: Option<Box<dyn Task>>,
    waker: TaskWaker,
    state: State,
    /// The shard that spawned this task onto the pool (a pool scheduler's entries only):
    /// its slices' time is counted back to that shard (D256), and it goes back there if
    /// taken back (D284).
    origin: Option<Arc<Origin>>,
    /// The task is on its shard's commit path ([`Task::commit_path`]).
    room: bool,
    /// When it last ended a slice, or was taken (VFS clock; pool schedulers only).
    idle_since: u64,
    /// A slice of it panicked: it never runs here again (D284).
    poisoned: bool,
}

/// A task taken out of a scheduler, with its wake cell, to be adopted elsewhere (D284).
pub(crate) struct Moved {
    pub(crate) task: Box<dyn Task>,
    pub(crate) cell: Option<Arc<TaskCell>>,
    pub(crate) origin: Arc<Origin>,
    pub(crate) failed: bool,
}

/// A slice taken out of a scheduler to run without its lock (D284).
pub(crate) struct Begun {
    pub(crate) slot: usize,
    pub(crate) task: Box<dyn Task>,
    pub(crate) waker: TaskWaker,
    pub(crate) origin: Option<Arc<Origin>>,
}

/// Round-robin scheduler over one thread's background tasks. Deterministic: tasks run in
/// spawn order, woken tasks rejoin in slot order.
pub(crate) struct Scheduler {
    slots: Vec<Option<Entry>>,
    free: Vec<usize>,
    runnable: VecDeque<usize>,
    blocked: usize,
    /// Sleeping tasks by `(deadline, slot)`.
    sleeping: BTreeSet<(u64, usize)>,
    signal: Arc<Signal>,
}

impl fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scheduler")
            .field("runnable", &self.runnable.len())
            .field("blocked", &self.blocked)
            .field("sleeping", &self.sleeping.len())
            .finish_non_exhaustive()
    }
}

impl Scheduler {
    pub(crate) fn new(signal: Arc<Signal>) -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            runnable: VecDeque::new(),
            blocked: 0,
            sleeping: BTreeSet::new(),
            signal,
        }
    }

    pub(crate) fn spawn(&mut self, task: Box<dyn Task>) {
        self.adopt(task, None, None, 0);
    }

    /// Takes `task` in, runnable, with the wake cell it already has if it ran elsewhere
    /// (pointed at this scheduler from now on, D284), counting its slices' time to `origin`
    /// (the shard that handed it to the pool, D256) when given. `now`: the VFS clock.
    pub(crate) fn adopt(
        &mut self,
        task: Box<dyn Task>,
        cell: Option<Arc<TaskCell>>,
        origin: Option<Arc<Origin>>,
        now: u64,
    ) {
        let cell = match cell {
            Some(cell) => {
                cell.retarget(Arc::clone(&self.signal));
                cell
            }
            None => TaskCell::new(Arc::clone(&self.signal)),
        };
        if let Some(o) = &origin {
            o.held.fetch_add(1, Ordering::Relaxed);
        }
        let entry = Entry {
            room: task.commit_path(),
            task: Some(task),
            waker: TaskWaker { cell },
            state: State::Runnable,
            origin,
            idle_since: now,
            poisoned: false,
        };
        let idx = match self.free.pop() {
            Some(i) => {
                self.slots[i] = Some(entry);
                i
            }
            None => {
                self.slots.push(Some(entry));
                self.slots.len() - 1
            }
        };
        self.runnable.push_back(idx);
    }

    pub(crate) fn has_runnable(&self) -> bool {
        !self.runnable.is_empty()
    }

    /// The earliest deadline of a sleeping task.
    pub(crate) fn next_deadline(&self) -> Option<u64> {
        self.sleeping.first().map(|&(d, _)| d)
    }

    /// Makes every sleeping task whose deadline is at or before `now` runnable.
    pub(crate) fn wake_due(&mut self, now: u64) {
        while let Some(&(d, i)) = self.sleeping.first() {
            if d > now {
                break;
            }
            self.sleeping.pop_first();
            self.make_runnable(i);
        }
    }

    /// Makes every sleeping task runnable, due or not (the clock did not move).
    pub(crate) fn wake_sleepers(&mut self) {
        while let Some((_, i)) = self.sleeping.pop_first() {
            self.make_runnable(i);
        }
    }

    fn make_runnable(&mut self, i: usize) {
        if let Some(e) = self.slots[i].as_mut() {
            e.state = State::Runnable;
            self.runnable.push_back(i);
        }
    }

    /// Moves woken blocked tasks back to the run queue.
    pub(crate) fn collect_woken(&mut self) {
        // Always clear the flag, even with nothing blocked, so a stale wake cannot keep the
        // owner from sleeping.
        if !self.signal.take_task_woken() || (self.blocked == 0 && self.sleeping.is_empty()) {
            return;
        }
        // O(slots) per wake batch. A shard has a handful of live tasks (flush, compaction
        // jobs, the manifest task), so a scan beats a shared woken-list; revisit if that
        // changes.
        for (i, slot) in self.slots.iter_mut().enumerate() {
            let Some(e) = slot else { continue };
            if e.state == State::Runnable || !e.waker.cell.woken.swap(false, Ordering::Acquire) {
                continue;
            }
            match e.state {
                State::Blocked => self.blocked -= 1,
                State::Sleeping(d) => {
                    self.sleeping.remove(&(d, i));
                }
                State::Runnable => {}
            }
            e.state = State::Runnable;
            self.runnable.push_back(i);
        }
    }

    /// Runs the next runnable task for one slice ending at `deadline` (a shard's own
    /// scheduler).
    pub(crate) fn run_one(&mut self, deadline: u64) {
        let Some(b) = self.begin_one(false) else {
            return;
        };
        let mut task = b.task;
        let poll = task.run(deadline, &b.waker);
        self.end_one(b.slot, Some(task), poll, 0);
    }

    /// Takes the next runnable task out for one slice, commit-path tasks first when
    /// `room_first` (a worker, D284); its slot stays, marked running, until
    /// [`Scheduler::end_one`].
    pub(crate) fn begin_one(&mut self, room_first: bool) -> Option<Begun> {
        let pick = if room_first {
            self.runnable
                .iter()
                .position(|&i| self.slots[i].as_ref().is_some_and(|e| e.room))
                .unwrap_or(0)
        } else {
            0
        };
        let i = self.runnable.remove(pick)?;
        let e = self.slots[i].as_mut()?;
        e.waker.cell.woken.store(false, Ordering::Relaxed);
        Some(Begun {
            slot: i,
            task: e.task.take()?,
            waker: e.waker.clone(),
            origin: e.origin.clone(),
        })
    }

    /// Puts a task back after its slice, as `poll` says (`now`: the VFS clock). A finished
    /// task is not passed back (`None`): its owner drops it, a worker with no pool lock
    /// held, since a task's destructor may run engine code (a settle callback, the release
    /// of the last view it pinned).
    pub(crate) fn end_one(
        &mut self,
        i: usize,
        task: Option<Box<dyn Task>>,
        poll: TaskPoll,
        now: u64,
    ) {
        let Some(e) = self.slots[i].as_mut() else {
            return;
        };
        e.idle_since = now;
        if poll != TaskPoll::Done {
            e.task = task;
        }
        match poll {
            TaskPoll::Pending => self.runnable.push_back(i),
            TaskPoll::Blocked => {
                // A wake that raced with the slice keeps the task runnable.
                if e.waker.cell.woken.swap(false, Ordering::Acquire) {
                    self.runnable.push_back(i);
                } else {
                    e.state = State::Blocked;
                    self.blocked += 1;
                }
            }
            TaskPoll::SleepUntil(deadline) => {
                if e.waker.cell.woken.swap(false, Ordering::Acquire) {
                    self.runnable.push_back(i);
                } else {
                    e.state = State::Sleeping(deadline);
                    self.sleeping.insert((deadline, i));
                }
            }
            TaskPoll::Done => {
                if let Some(o) = self.slots[i].take().and_then(|e| e.origin) {
                    o.held.fetch_sub(1, Ordering::Relaxed);
                }
                self.free.push(i);
            }
        }
    }

    /// A slice of the task in slot `i` panicked (D284): it goes back, never to run here
    /// again, and is taken back as failed.
    pub(crate) fn poison(&mut self, i: usize, task: Box<dyn Task>, now: u64) {
        if let Some(e) = self.slots[i].as_mut() {
            e.task = Some(task);
            e.poisoned = true;
            e.idle_since = now;
            // Neither runnable nor sleeping: blocked for good, until taken back.
            e.state = State::Blocked;
            self.blocked += 1;
        }
    }

    /// Tasks held: running, runnable, blocked or sleeping.
    pub(crate) fn held(&self) -> usize {
        self.slots.len() - self.free.len()
    }

    /// Whether a slice of a task `pick` selects is running outside the lock.
    pub(crate) fn running(&self, pick: impl Fn(&Arc<Origin>, bool) -> bool) -> bool {
        self.slots
            .iter()
            .flatten()
            .any(|e| e.task.is_none() && e.origin.as_ref().is_some_and(|o| pick(o, e.room)))
    }

    /// Takes out every task not running that `pick` selects (by origin and
    /// [`Task::commit_path`]) and that has been idle since `idle_before` or earlier (VFS
    /// clock; `u64::MAX`: any), for adoption elsewhere (D284). Only pool schedulers' tasks
    /// (with an origin) are taken.
    pub(crate) fn take_idle(
        &mut self,
        idle_before: u64,
        could_run_only: bool,
        pick: impl Fn(&Arc<Origin>, bool) -> bool,
        out: &mut Vec<Moved>,
    ) {
        for i in 0..self.slots.len() {
            let take = self.slots[i].as_ref().is_some_and(|e| {
                e.task.is_some()
                    && e.idle_since <= idle_before
                    && (!could_run_only || e.could_run())
                    && e.origin.as_ref().is_some_and(|o| pick(o, e.room))
            });
            if !take {
                continue;
            }
            let Some(e) = self.slots[i].take() else {
                continue;
            };
            self.free.push(i);
            match e.state {
                State::Runnable => self.runnable.retain(|&r| r != i),
                State::Blocked => self.blocked -= 1,
                State::Sleeping(d) => {
                    self.sleeping.remove(&(d, i));
                }
            }
            if let (Some(task), Some(origin)) = (e.task, e.origin) {
                origin.held.fetch_sub(1, Ordering::Relaxed);
                out.push(Moved {
                    task,
                    cell: Some(e.waker.cell),
                    origin,
                    failed: e.poisoned,
                });
            }
        }
    }

    /// The earliest time (VFS clock) a task `pick` selects that could run but is not
    /// running became idle: when a room wait could take it back (D284).
    pub(crate) fn stalled_since(&self, pick: impl Fn(&Arc<Origin>, bool) -> bool) -> Option<u64> {
        self.slots
            .iter()
            .flatten()
            .filter(|e| {
                e.task.is_some()
                    && e.could_run()
                    && e.origin.as_ref().is_some_and(|o| pick(o, e.room))
            })
            .map(|e| e.idle_since)
            .min()
    }
}

impl Entry {
    /// Whether the task could run now if its scheduler were driven: runnable, woken while
    /// nobody collected the wake, or failed (taken back as such).
    fn could_run(&self) -> bool {
        self.poisoned
            || self.state == State::Runnable
            || self.waker.cell.woken.load(Ordering::Acquire)
    }
}

/// Parks an idle thread whose earliest sleeping task is due at a VFS-clock deadline.
#[derive(Debug, Default)]
pub(crate) struct IdlePark {
    /// The VFS clock kept pace with real time over the last timed park.
    real: bool,
}

impl IdlePark {
    /// Parks until `deadline` on `vfs`'s clock, an unpark (new work), or the re-check cap.
    pub(crate) fn park(&mut self, vfs: &dyn Vfs, deadline: u64) {
        let now = vfs.monotonic_nanos();
        let cap = if self.real { MAX_PARK_REAL } else { MAX_PARK };
        let want = Duration::from_nanos(deadline.saturating_sub(now)).min(cap);
        let started = Instant::now();
        thread::park_timeout(want);
        let slept = started.elapsed();
        // An early unpark says nothing about the clock.
        if slept >= Duration::from_millis(1) {
            let moved = u128::from(vfs.monotonic_nanos().saturating_sub(now));
            self.real = moved * 2 >= slept.as_nanos();
        }
    }
}

/// Where a shard's spawned tasks go: its own scheduler, or the background workers' pool
/// (D284), from which tasks also come back.
#[derive(Debug)]
pub(crate) struct Spawner {
    pub(crate) local: Scheduler,
    pool: Option<Arc<Pool>>,
    origin: Arc<Origin>,
    /// When to look again for a stalled commit-path task of this shard's on a worker (a
    /// room wait, D284).
    room_check: Option<u64>,
    /// When this shard's tasks queued on the pool may fall back to it (`pool::FALLBACK`).
    stale_check: Option<u64>,
}

impl Spawner {
    pub(crate) fn new(local: Scheduler, pool: Option<Arc<Pool>>, origin: Arc<Origin>) -> Self {
        Self {
            local,
            pool,
            origin,
            room_check: None,
            stale_check: None,
        }
    }

    pub(crate) fn spawn(&mut self, task: Box<dyn Task>) {
        let Some(pool) = &self.pool else {
            self.local.spawn(task);
            return;
        };
        match pool.submit(task, &self.origin) {
            Ok(()) => {
                if self.stale_check.is_none() {
                    self.stale_check = Some(pool.now().saturating_add(crate::pool::FALLBACK));
                }
            }
            Err(task) => self.local.spawn(task),
        }
    }

    /// Whether the shard has background workers (D284). Without them, the turn and its
    /// sleep checks skip the take-back work in one branch (#983's per-turn checks, which
    /// the commit shapes paid as calls on every turn).
    #[inline]
    pub(crate) fn has_pool(&self) -> bool {
        self.pool.is_some()
    }

    /// Whether tasks came back from the workers (for the sleep re-check, after its fence).
    #[inline]
    pub(crate) fn has_back(&self) -> bool {
        self.has_pool() && self.origin.has_back()
    }

    /// At the start of a turn: adopts tasks that came back from the workers, takes back
    /// tasks queued while nobody drives them, and re-checks a room wait's stalled flush.
    /// Callers check [`has_pool`](Spawner::has_pool) first.
    #[inline(never)]
    pub(crate) fn take_back(&mut self, now: u64) {
        let Some(pool) = &self.pool else {
            return;
        };
        let mut moved = Vec::new();
        if self.origin.has_back() {
            moved = self.origin.take_back();
        }
        if self.stale_check.is_some_and(|t| t <= now) {
            self.stale_check = None;
            let stale = pool.take_stale(&self.origin, now);
            self.origin.signal.add_take_backs(stale.len() as u64);
            moved.extend(stale);
            // Look again while tasks wait in the queue, or an application's driver holds
            // some (it may stop being driven; engine-owned threads never do).
            if self.origin.queued() || (!pool.builtin() && self.origin.held()) {
                self.stale_check = Some(now.saturating_add(crate::pool::FALLBACK));
            }
        }
        if self.room_check.is_some_and(|t| t <= now) {
            self.room_check = None;
            let (room, next) = pool.reclaim_room(&self.origin, now);
            self.origin.signal.add_room_take_backs(room.len() as u64);
            moved.extend(room);
            self.room_check = next;
        }
        self.adopt_back(moved, now);
    }

    /// A room wait on this shard (D284): takes back its flushes a worker holds but has not
    /// run for `pool::ROOM_RECLAIM`, never waiting for one that runs. Returns how many.
    pub(crate) fn reclaim_room(&mut self, now: u64) -> usize {
        let Some(pool) = &self.pool else {
            return 0;
        };
        let (moved, next) = pool.reclaim_room(&self.origin, now);
        let n = moved.len();
        self.origin.signal.add_room_take_backs(n as u64);
        self.room_check = next;
        self.adopt_back(moved, now);
        n
    }

    fn adopt_back(&mut self, moved: Vec<Moved>, now: u64) {
        for mut m in moved {
            // A task a worker held (it has a wake cell) is told it was taken back; one that
            // only waited in the queue never ran elsewhere and simply runs here.
            if m.cell.is_none() || m.task.reclaim(m.failed) {
                self.local.adopt(m.task, m.cell, None, now);
            }
        }
    }

    /// The earliest of the shard's own sleepers' deadlines and the pool checks above.
    #[inline]
    pub(crate) fn next_deadline(&self) -> Option<u64> {
        let local = self.local.next_deadline();
        if !self.has_pool() {
            return local;
        }
        self.pool_deadline(local)
    }

    #[inline(never)]
    fn pool_deadline(&self, local: Option<u64>) -> Option<u64> {
        [local, self.room_check, self.stale_check]
            .into_iter()
            .flatten()
            .min()
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::{TaskCell, TaskWaker};
    use crate::signal::{Signal, WakeTarget};
    use loom::sync::{Condvar, Mutex};
    use loom::thread;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    /// The shard thread's park, with a token.
    #[derive(Default)]
    struct Parker {
        token: Mutex<bool>,
        cv: Condvar,
    }

    impl Parker {
        fn park(&self) {
            let mut t = self.token.lock().unwrap();
            while !*t {
                t = self.cv.wait(t).unwrap();
            }
            *t = false;
        }
    }

    #[test]
    #[ignore = "loom 0.7.2 lets an RMW read past a store (tokio-rs/loom#254; the woken flag's \
                swap): passes with tokio-rs/loom#391 patched in; un-ignore with #533"]
    fn loom_a_wake_during_a_slice_keeps_the_task_runnable() {
        // A task blocks waiting for an event; another thread publishes the event and wakes
        // it, at any point of the task's slice (`run_one`: the woken flag is cleared, the
        // task runs, then a wake that raced the slice keeps it runnable) or of the shard's
        // idle handshake (`collect_woken`, then the sleep re-check): the task always runs
        // again and sees the event.
        loom::model(|| {
            let parker = Arc::new(Parker::default());
            let signal = Arc::new(Signal::new());
            {
                let p = Arc::clone(&parker);
                signal.set_target(WakeTarget::Callback(Arc::new(move || {
                    *p.token.lock().unwrap() = true;
                    p.cv.notify_one();
                })));
            }
            let waker = TaskWaker {
                cell: TaskCell::new(Arc::clone(&signal)),
            };
            let event = Arc::new(Mutex::new(false));
            let other = {
                let (waker, event) = (waker.clone(), Arc::clone(&event));
                thread::spawn(move || {
                    *event.lock().unwrap() = true;
                    waker.wake();
                })
            };
            let cell = &waker.cell;
            loop {
                // `run_one`: one slice of the task.
                cell.woken.store(false, Ordering::Relaxed);
                if *event.lock().unwrap() {
                    break;
                }
                if cell.woken.swap(false, Ordering::Acquire) {
                    continue;
                }
                // Blocked: the shard runs its loop until `collect_woken` finds it woken.
                loop {
                    signal.awake();
                    if signal.take_task_woken() && cell.woken.swap(false, Ordering::Acquire) {
                        break;
                    }
                    signal.prepare_sleep();
                    if signal.task_woken() {
                        continue;
                    }
                    parker.park();
                }
            }
            other.join().unwrap();
        });
    }
}
