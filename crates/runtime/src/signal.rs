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
    target: Mutex<WakeTarget>,
    /// Times the consumer's engine-owned thread parked idle (ICR 0027).
    parks: AtomicU64,
    /// Notifies that found the consumer announced asleep and woke it (ICR 0027).
    wakes: AtomicU64,
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
            target: Mutex::new(WakeTarget::None),
            parks: AtomicU64::new(0),
            wakes: AtomicU64::new(0),
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

    /// A shard's queue and its signal, as `Inbox` and `ShardCore` hold them.
    struct Shard {
        signal: Signal,
        queue: Mutex<VecDeque<u32>>,
    }

    impl Shard {
        fn new() -> (std::sync::Arc<Shard>, std::sync::Arc<Parker>) {
            let parker = std::sync::Arc::new(Parker::default());
            let shard = std::sync::Arc::new(Shard {
                signal: Signal::new(),
                queue: Mutex::new(VecDeque::new()),
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

        /// `ShardCore::check_before_sleep`.
        fn check_before_sleep(&self) -> bool {
            self.signal.prepare_sleep();
            if !self.queue.lock().unwrap().is_empty() || self.signal.task_woken() {
                self.signal.awake();
                return true;
            }
            false
        }

        /// One poll of `spin_before_park` (D198), awake, then the handshake on a miss.
        fn spin_once(&self) -> bool {
            self.signal.awake();
            if !self.queue.lock().unwrap().is_empty() || self.signal.task_woken() {
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
                while self.queue.lock().unwrap().pop_front().is_some() {
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
}
