//! Idle/wake handshake shared by a shard (or compaction thread) and everyone who gives it work.
//!
//! The consumer announces it is about to sleep ([`Signal::prepare_sleep`]) and then re-checks
//! its work sources; a producer publishes work and then calls [`Signal::notify`]. Both sides
//! put a `SeqCst` fence between their write and their read, so either the consumer sees the
//! new work or the producer sees `sleeping` and wakes it. A busy consumer costs producers one
//! fence and one relaxed load: no lock, no syscall.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering, fence};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::Thread;

/// How to wake a sleeping consumer.
pub(crate) enum WakeTarget {
    /// Nobody to wake yet (an application-owned shard without a registered wakeup).
    None,
    /// An engine-owned thread parked in its loop.
    Thread(Thread),
    /// The application's event-loop wakeup.
    Callback(Arc<dyn Fn() + Send + Sync>),
}

pub(crate) struct Signal {
    sleeping: AtomicBool,
    task_woken: AtomicBool,
    target: Mutex<WakeTarget>,
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
        }
    }

    pub(crate) fn set_target(&self, target: WakeTarget) {
        *self.target.lock().unwrap_or_else(PoisonError::into_inner) = target;
    }

    /// Producer side: call after publishing work.
    pub(crate) fn notify(&self) {
        fence(Ordering::SeqCst);
        if self.sleeping.load(Ordering::Relaxed) && self.sleeping.swap(false, Ordering::AcqRel) {
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
            Callback(Arc<dyn Fn() + Send + Sync>),
        }
        // Clone the target out so the callback runs without the lock held.
        let act = match &*self.target.lock().unwrap_or_else(PoisonError::into_inner) {
            WakeTarget::None => return,
            WakeTarget::Thread(t) => Act::Thread(t.clone()),
            WakeTarget::Callback(f) => Act::Callback(Arc::clone(f)),
        };
        match act {
            Act::Thread(t) => t.unpark(),
            Act::Callback(f) => f(),
        }
    }
}
