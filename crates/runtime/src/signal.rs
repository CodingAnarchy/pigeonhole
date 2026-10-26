//! Idle/wake handshake shared by a shard (or compaction thread) and everyone who gives it work.
//!
//! The consumer announces it is about to sleep ([`Signal::prepare_sleep`]) and then re-checks
//! its work sources; a producer publishes work and then calls [`Signal::notify`]. Both sides
//! put a `SeqCst` fence between their write and their read, so either the consumer sees the
//! new work or the producer sees `sleeping` and wakes it. A busy consumer costs producers one
//! fence and one relaxed load: no lock, no syscall.

use std::fmt;
use std::sync::{Arc, PoisonError};
use std::thread::Thread;

#[cfg(loom)]
use loom::sync::Mutex;
#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
#[cfg(not(loom))]
use std::sync::Mutex;
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};

/// How to wake a sleeping consumer.
pub(crate) enum WakeTarget {
    /// Nobody to wake yet (an application-owned shard without a registered wakeup).
    None,
    /// An engine-owned thread parked in its loop.
    Thread(Thread),
    /// An engine-owned thread that may instead wait on its own I/O ring (#402): unparked,
    /// and its ring wait interrupted.
    ThreadRing(Thread, pigeonhole_io::OwnIoWaker),
    /// The application's event-loop wakeup.
    Callback(Arc<dyn Fn() + Send + Sync>),
}

pub(crate) struct Signal {
    sleeping: AtomicBool,
    task_woken: AtomicBool,
    /// An application wake ([`ShardDriver::waker`](crate::ShardDriver::waker)) not yet seen
    /// by [`Signal::sleep_unless`]. Apart from `task_woken`, which the scheduler clears as it
    /// collects woken tasks: a wake that arrives during a turn (a commit the driver's own
    /// shard resolved) must still end the driver's next wait (#592).
    app_woken: AtomicBool,
    /// An application wake not yet seen by a turn (D237): a future the driving thread awaits
    /// was woken, so the turn skips its background slices and hands the thread back. Apart
    /// from `app_woken`, which only `sleep_unless` clears: a turn that takes this one leaves
    /// the next `wait`'s answer unchanged (#592). A hint: a missed or stale one costs at most
    /// one turn's slices.
    turn_woken: AtomicBool,
    target: Mutex<WakeTarget>,
    /// Times the consumer's engine-owned thread parked idle (ICR 0027).
    parks: AtomicU64,
    /// Notifies that found the consumer announced asleep and woke it (ICR 0027).
    wakes: AtomicU64,
    /// Nanoseconds the consumer's engine-owned thread spent in work turns (D217).
    busy: AtomicU64,
    /// Nanoseconds the pool's threads spent in slices of tasks this consumer (a shard)
    /// spawned onto the pool (D256): flush and compaction time counted back to the shard
    /// whose tablets they serve, as it was inside its turns before D252.
    pool_busy: AtomicU64,
    /// Nanoseconds the consumer's engine-owned thread spent in its spin before parking
    /// (D198, D256): awake and polling for the next message, counted as busy by the
    /// balancer, since a shard its clients keep awake has no idle time to give packing.
    spin: AtomicU64,
}

impl fmt::Debug for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Signal")
            .field("sleeping", &self.sleeping.load(Ordering::Relaxed))
            .field("task_woken", &self.task_woken.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Signal {
    pub(crate) fn new() -> Self {
        Self {
            sleeping: AtomicBool::new(false),
            task_woken: AtomicBool::new(false),
            app_woken: AtomicBool::new(false),
            turn_woken: AtomicBool::new(false),
            target: Mutex::new(WakeTarget::None),
            parks: AtomicU64::new(0),
            wakes: AtomicU64::new(0),
            busy: AtomicU64::new(0),
            pool_busy: AtomicU64::new(0),
            spin: AtomicU64::new(0),
        }
    }

    pub(crate) fn set_target(&self, target: WakeTarget) {
        *self.target.lock().unwrap_or_else(PoisonError::into_inner) = target;
    }

    /// Producer side: call after publishing work.
    pub(crate) fn notify(&self) {
        fence(Ordering::SeqCst);
        if self.sleeping.load(Ordering::Relaxed) && self.sleeping.swap(false, Ordering::AcqRel) {
            self.wakes.fetch_add(1, Ordering::Relaxed);
            self.wake_target();
        }
    }

    /// Records that a blocked background task was woken, then notifies.
    pub(crate) fn notify_task(&self) {
        self.task_woken.store(true, Ordering::Release);
        self.notify();
    }

    /// Records an application wake (`ShardDriver::waker`), then notifies: the consumer's next
    /// [`Signal::sleep_unless`] returns at once if it is not asleep already (#592).
    pub(crate) fn notify_app(&self) {
        self.turn_woken.store(true, Ordering::Relaxed);
        self.app_woken.store(true, Ordering::Release);
        self.notify();
    }

    /// Wakes the consumer whether or not it announced sleep (shutdown).
    pub(crate) fn force_wake(&self) {
        self.sleeping.store(false, Ordering::Relaxed);
        self.wake_target();
    }

    /// Consumer side: announce sleep. The caller must re-check every work source afterwards
    /// and sleep only if all are empty.
    pub(crate) fn prepare_sleep(&self) {
        self.sleeping.store(true, Ordering::Relaxed);
        fence(Ordering::SeqCst);
    }

    /// Consumer side, for a thread that sleeps on its own terms (`ShardDriver::wait`,
    /// #553): announces sleep, re-checks with `has_work`, `task_woken` and the application
    /// wake (which it clears), and calls `sleep` only if none found anything. A producer that
    /// publishes work after the announcement sees the consumer asleep and wakes its target,
    /// which must end `sleep`. Returns whether it slept.
    pub(crate) fn sleep_unless(
        &self,
        has_work: impl FnOnce() -> bool,
        sleep: impl FnOnce(),
    ) -> bool {
        self.prepare_sleep();
        if has_work() || self.task_woken() || self.take_app_woken() {
            self.awake();
            return false;
        }
        sleep();
        self.awake();
        // The wake that ended the sleep is spent: the caller looks at everything next.
        self.take_app_woken();
        true
    }

    /// Whether an application wake arrived since a turn last asked (clears it; D237).
    pub(crate) fn take_turn_woken(&self) -> bool {
        self.turn_woken.load(Ordering::Relaxed) && self.turn_woken.swap(false, Ordering::Relaxed)
    }

    /// Whether an application wake arrived since the last call (clears it).
    fn take_app_woken(&self) -> bool {
        self.app_woken.load(Ordering::Relaxed) && self.app_woken.swap(false, Ordering::Acquire)
    }

    /// Consumer side: back to work (producers need not wake us).
    pub(crate) fn awake(&self) {
        self.sleeping.store(false, Ordering::Relaxed);
    }

    /// Consumer side: counts a park of an engine-owned thread with nothing to do.
    pub(crate) fn parked(&self) {
        self.parks.fetch_add(1, Ordering::Relaxed);
    }

    /// `(parks, wakes)` since the signal was made: see [`Signal::parked`] and
    /// [`Signal::notify`].
    pub(crate) fn idle_counts(&self) -> (u64, u64) {
        (
            self.parks.load(Ordering::Relaxed),
            self.wakes.load(Ordering::Relaxed),
        )
    }

    /// Consumer side: adds a work turn's nanoseconds (engine-owned threads only; the one
    /// writer is the shard's thread).
    pub(crate) fn add_busy(&self, nanos: u64) {
        self.busy.fetch_add(nanos, Ordering::Relaxed);
    }

    /// Nanoseconds counted by [`Signal::add_busy`].
    pub(crate) fn busy_nanos(&self) -> u64 {
        self.busy.load(Ordering::Relaxed)
    }

    /// A pool thread adds a slice's nanoseconds for a task this consumer spawned onto the
    /// pool (D256; several pool threads may write, each after its own slice).
    pub(crate) fn add_pool_busy(&self, nanos: u64) {
        self.pool_busy.fetch_add(nanos, Ordering::Relaxed);
    }

    /// Nanoseconds counted by [`Signal::add_pool_busy`].
    pub(crate) fn pool_busy_nanos(&self) -> u64 {
        self.pool_busy.load(Ordering::Relaxed)
    }

    /// Consumer side: adds a spin's nanoseconds (engine-owned threads only; the one writer
    /// is the shard's thread, D256).
    pub(crate) fn add_spin(&self, nanos: u64) {
        self.spin.fetch_add(nanos, Ordering::Relaxed);
    }

    /// Nanoseconds counted by [`Signal::add_spin`].
    pub(crate) fn spin_nanos(&self) -> u64 {
        self.spin.load(Ordering::Relaxed)
    }

    /// Whether some task was woken since the last call (clears the flag).
    pub(crate) fn take_task_woken(&self) -> bool {
        self.task_woken.load(Ordering::Relaxed) && self.task_woken.swap(false, Ordering::Acquire)
    }

    /// Whether some task was woken, without clearing (for the sleep re-check).
    pub(crate) fn task_woken(&self) -> bool {
        self.task_woken.load(Ordering::Relaxed)
    }

    fn wake_target(&self) {
        enum Act {
            Thread(Thread),
            ThreadRing(Thread, pigeonhole_io::OwnIoWaker),
            Callback(Arc<dyn Fn() + Send + Sync>),
        }
        // Clone the target out so the callback runs without the lock held.
        let act = match &*self.target.lock().unwrap_or_else(PoisonError::into_inner) {
            WakeTarget::None => return,
            WakeTarget::Thread(t) => Act::Thread(t.clone()),
            WakeTarget::ThreadRing(t, w) => Act::ThreadRing(t.clone(), w.clone()),
            WakeTarget::Callback(f) => Act::Callback(Arc::clone(f)),
        };
        match act {
            Act::Thread(t) => t.unpark(),
            Act::ThreadRing(t, w) => {
                t.unpark();
                w.wake();
            }
            Act::Callback(f) => f(),
        }
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::{Signal, WakeTarget};
    use loom::sync::{Condvar, Mutex};
    use loom::thread;
    use std::collections::VecDeque;
    use std::sync::Arc;

    /// `thread::park`/`unpark` with a token (the consumer's wake target). Loom's own park
    /// drops an unpark that reaches a thread blocked on a loom `Mutex`.
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

        fn unpark(&self) {
            *self.token.lock().unwrap() = true;
            self.cv.notify_one();
        }
    }

    /// A shard's queues and its signal, as `Inbox` and `ShardCore` hold them: the main lane
    /// and D285's priority lane.
    struct Shard {
        signal: Signal,
        queue: Mutex<VecDeque<u32>>,
        priority: Mutex<VecDeque<u32>>,
    }

    impl Shard {
        fn new() -> (std::sync::Arc<Shard>, std::sync::Arc<Parker>) {
            let parker = std::sync::Arc::new(Parker::default());
            let shard = std::sync::Arc::new(Shard {
                signal: Signal::new(),
                queue: Mutex::new(VecDeque::new()),
                priority: Mutex::new(VecDeque::new()),
            });
            let p = std::sync::Arc::clone(&parker);
            shard
                .signal
                .set_target(WakeTarget::Callback(Arc::new(move || p.unpark())));
            (shard, parker)
        }

        /// `Submitter::submit`: send, then notify.
        fn submit(&self, msg: u32) {
            self.queue.lock().unwrap().push_back(msg);
            self.signal.notify();
        }

        /// `Submitter::submit` of a message `ShardHandler::priority` routes to that lane.
        fn submit_priority(&self, msg: u32) {
            self.priority.lock().unwrap().push_back(msg);
            self.signal.notify();
        }

        /// `Lanes::try_recv`: the priority lane first.
        fn pop(&self) -> Option<u32> {
            let p = self.priority.lock().unwrap().pop_front();
            p.or_else(|| self.queue.lock().unwrap().pop_front())
        }

        fn any_queued(&self) -> bool {
            !self.priority.lock().unwrap().is_empty() || !self.queue.lock().unwrap().is_empty()
        }

        /// `ShardCore::check_before_sleep`.
        fn check_before_sleep(&self) -> bool {
            self.signal.prepare_sleep();
            if self.any_queued() || self.signal.task_woken() {
                self.signal.awake();
                return true;
            }
            false
        }

        /// One poll of `spin_before_park` (D198), awake, then the handshake on a miss.
        fn spin_once(&self) -> bool {
            self.signal.awake();
            if self.any_queued() || self.signal.task_woken() {
                return true;
            }
            let hit = self.check_before_sleep();
            if hit {
                self.signal.awake();
            }
            hit
        }

        /// `shard_main`: runs turns (`run_turn` is awake, drain, collect the woken task)
        /// until it has handled `msgs` messages and one task wake, parking when idle.
        fn run(&self, parker: &Parker, msgs: usize, spin: bool) {
            let (mut handled, mut task) = (0, false);
            loop {
                self.signal.awake();
                while self.pop().is_some() {
                    handled += 1;
                }
                task |= self.signal.take_task_woken();
                if handled == msgs && task {
                    return;
                }
                if self.check_before_sleep() || (spin && self.spin_once()) {
                    continue;
                }
                self.signal.parked();
                parker.park();
            }
        }
    }

    fn model(f: impl Fn() + Sync + Send + 'static) {
        let mut b = loom::model::Builder::new();
        b.preemption_bound = Some(3);
        b.check(f);
    }

    #[test]
    #[ignore = "loom 0.7.2 lets an RMW read past a store (tokio-rs/loom#254; `notify`'s swap): \
                passes with tokio-rs/loom#391 patched in; un-ignore with #533"]
    fn loom_a_message_or_task_wake_never_finds_the_shard_asleep_unwoken() {
        // A submit and a background task's wake race the shard going idle: it never parks
        // for good with a message queued or a task woken.
        model(|| {
            let (shard, parker) = Shard::new();
            let producer = {
                let shard = std::sync::Arc::clone(&shard);
                thread::spawn(move || shard.submit(1))
            };
            let task = {
                let shard = std::sync::Arc::clone(&shard);
                thread::spawn(move || shard.signal.notify_task())
            };
            shard.run(&parker, 1, false);
            producer.join().unwrap();
            task.join().unwrap();
        });
    }

    #[test]
    #[ignore = "loom 0.7.2 lets an RMW read past a store (tokio-rs/loom#254; `notify`'s swap): \
                passes with tokio-rs/loom#391 patched in; un-ignore with #533"]
    fn loom_an_application_wait_is_never_slept_through() {
        // `ShardDriver::wait` (#553): the application thread looks at its commit, runs a
        // turn (which collects woken background tasks, clearing their flag), then announces
        // sleep, re-checks and parks (`sleep_unless`). A submit and a `waker()` wake for its
        // commit, from other threads, race it; neither leaves it parked for good, including
        // a wake that lands between its look and the turn's collection (#592: with the wake
        // in `task_woken`, the turn cleared it and the wait slept through it). The park is
        // untimed here (a timeout would hide a lost wake).
        model(|| {
            let (shard, parker) = Shard::new();
            let resolved = std::sync::Arc::new(loom::sync::atomic::AtomicBool::new(false));
            let producer = {
                let shard = std::sync::Arc::clone(&shard);
                thread::spawn(move || shard.submit(1))
            };
            let waker = {
                let shard = std::sync::Arc::clone(&shard);
                let resolved = std::sync::Arc::clone(&resolved);
                thread::spawn(move || {
                    resolved.store(true, std::sync::atomic::Ordering::Release);
                    shard.signal.notify_app();
                })
            };
            let mut handled = 0;
            loop {
                shard.signal.awake();
                // Poll the commit, then run a turn: drain, and collect woken tasks.
                let done = resolved.load(std::sync::atomic::Ordering::Acquire);
                while shard.queue.lock().unwrap().pop_front().is_some() {
                    handled += 1;
                }
                shard.signal.take_task_woken();
                if handled == 1 && done {
                    break;
                }
                shard
                    .signal
                    .sleep_unless(|| !shard.queue.lock().unwrap().is_empty(), || parker.park());
            }
            producer.join().unwrap();
            waker.join().unwrap();
        });
    }

    #[test]
    #[ignore = "loom 0.7.2 lets an RMW read past a store (tokio-rs/loom#254; `notify`'s swap): \
                passes with tokio-rs/loom#391 patched in; un-ignore with #533"]
    fn loom_the_idle_spin_keeps_the_handshake() {
        // As above with D198's poll before parking: awake while it polls, then the same
        // handshake on a miss.
        model(|| {
            let (shard, parker) = Shard::new();
            let producer = {
                let shard = std::sync::Arc::clone(&shard);
                thread::spawn(move || {
                    shard.submit(1);
                    shard.submit(2);
                })
            };
            let task = {
                let shard = std::sync::Arc::clone(&shard);
                thread::spawn(move || shard.signal.notify_task())
            };
            shard.run(&parker, 2, true);
            producer.join().unwrap();
            task.join().unwrap();
        });
    }
    #[test]
    #[ignore = "loom 0.7.2 lets an RMW read past a store (tokio-rs/loom#254; `notify`'s swap): \
                passes with tokio-rs/loom#391 patched in; un-ignore with #533"]
    fn loom_a_priority_message_left_by_a_bounded_take_is_never_slept_through() {
        // D285: a main-lane submit and two priority-lane submits race the shard. Each turn
        // drains (the priority lane first), then a safe point takes at most one priority
        // message mid-batch, leaving any other queued: the shard never parks for good with
        // either lane non-empty.
        model(|| {
            let (shard, parker) = Shard::new();
            let main = {
                let shard = std::sync::Arc::clone(&shard);
                thread::spawn(move || shard.submit(1))
            };
            let peer = {
                let shard = std::sync::Arc::clone(&shard);
                thread::spawn(move || {
                    shard.submit_priority(10);
                    shard.submit_priority(11);
                })
            };
            let mut handled = 0;
            loop {
                shard.signal.awake();
                while shard.pop().is_some() {
                    handled += 1;
                }
                // The safe point's bounded take (`ShardContext::take_priority`, bound 1).
                if shard.priority.lock().unwrap().pop_front().is_some() {
                    handled += 1;
                }
                if handled == 3 {
                    break;
                }
                if shard.check_before_sleep() {
                    continue;
                }
                shard.signal.parked();
                parker.park();
            }
            main.join().unwrap();
            peer.join().unwrap();
        });
    }
}
