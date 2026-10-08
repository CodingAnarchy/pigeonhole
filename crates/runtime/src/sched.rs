//! Cooperative background tasks: the per-shard scheduler and the compaction-thread pool.

use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use pigeonhole_io::{Vfs, VfsRef};

use crate::signal::{Signal, WakeTarget};

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

#[derive(Debug)]
struct TaskCell {
    woken: AtomicBool,
    signal: Arc<Signal>,
}

impl TaskWaker {
    /// Marks the task runnable.
    pub fn wake(&self) {
        self.cell.woken.store(true, Ordering::Release);
        self.cell.signal.notify_task();
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
    task: Box<dyn Task>,
    waker: TaskWaker,
    state: State,
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
        let waker = TaskWaker {
            cell: Arc::new(TaskCell {
                woken: AtomicBool::new(false),
                signal: Arc::clone(&self.signal),
            }),
        };
        let entry = Entry {
            task,
            waker,
            state: State::Runnable,
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

    /// Runs the next runnable task for one slice ending at `deadline`.
    pub(crate) fn run_one(&mut self, deadline: u64) {
        let Some(i) = self.runnable.pop_front() else {
            return;
        };
        let Some(e) = self.slots[i].as_mut() else {
            return;
        };
        e.waker.cell.woken.store(false, Ordering::Relaxed);
        match e.task.run(deadline, &e.waker) {
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
                self.slots[i] = None;
                self.free.push(i);
            }
        }
    }
}

/// One dedicated compaction thread's inbox.
#[derive(Debug)]
pub(crate) struct PoolShared {
    tx: Sender<Box<dyn Task>>,
    closed: AtomicBool,
    signal: Arc<Signal>,
}

impl PoolShared {
    pub(crate) fn new() -> (Arc<Self>, Receiver<Box<dyn Task>>) {
        let (tx, rx) = mpsc::channel();
        let shared = Arc::new(Self {
            tx,
            closed: AtomicBool::new(false),
            signal: Arc::new(Signal::new()),
        });
        (shared, rx)
    }

    pub(crate) fn submit(&self, task: Box<dyn Task>) {
        // After shutdown the receiver is gone and the task is dropped, like any unfinished
        // background task.
        if self.tx.send(task).is_ok() {
            self.signal.notify();
        }
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.signal.force_wake();
    }
}

/// Body of a dedicated compaction thread: back-to-back slices, no foreground queue.
pub(crate) fn pool_main(
    shared: Arc<PoolShared>,
    rx: Receiver<Box<dyn Task>>,
    vfs: VfsRef,
    time_slice: u64,
) {
    shared
        .signal
        .set_target(WakeTarget::Thread(thread::current()));
    let mut sched = Scheduler::new(Arc::clone(&shared.signal));
    // The clock when this thread last went idle with sleeping tasks.
    let mut idle_at = None;
    let mut idle_park = IdlePark::default();
    loop {
        if shared.closed.load(Ordering::SeqCst) {
            return;
        }
        while let Ok(task) = rx.try_recv() {
            sched.spawn(task);
        }
        sched.collect_woken();
        let now = vfs.monotonic_nanos();
        if idle_at.take() == Some(now) && vfs.clock_is_simulated() {
            // A simulated clock has not moved since we went idle: sleepers check for
            // themselves (a real clock that reads the same only ticks coarsely, #263).
            sched.wake_sleepers();
        }
        sched.wake_due(now);
        if sched.has_runnable() {
            sched.run_one(now.saturating_add(time_slice));
            continue;
        }
        shared.signal.prepare_sleep();
        if let Ok(task) = rx.try_recv() {
            shared.signal.awake();
            sched.spawn(task);
            continue;
        }
        if shared.signal.task_woken() {
            shared.signal.awake();
            continue;
        }
        if !shared.closed.load(Ordering::SeqCst) {
            match sched.next_deadline() {
                None => thread::park(),
                Some(deadline) => {
                    idle_at = Some(now);
                    idle_park.park(&*vfs, deadline);
                }
            }
        }
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

/// Where a shard's spawned tasks go: its own scheduler, or round-robin over the pool.
#[derive(Debug)]
pub(crate) struct Spawner {
    pub(crate) local: Scheduler,
    pool: Arc<[Arc<PoolShared>]>,
    next: usize,
}

impl Spawner {
    pub(crate) fn new(local: Scheduler, pool: Arc<[Arc<PoolShared>]>) -> Self {
        Self {
            local,
            pool,
            next: 0,
        }
    }

    pub(crate) fn spawn(&mut self, task: Box<dyn Task>) {
        if self.pool.is_empty() {
            self.local.spawn(task);
        } else {
            let p = &self.pool[self.next % self.pool.len()];
            self.next = self.next.wrapping_add(1);
            p.submit(task);
        }
    }
}
