//! Whether a wake must interrupt a thread's ring wait (#402; the Phase 3 gate analysis).
//!
//! A shard thread with its own io_uring ring sleeps in one of two ways. With nothing of its
//! own in flight it parks, and a waker's unpark ends that. With its own I/O in flight it
//! waits in the ring, and only a write to the eventfd the ring polls ends that wait. Writing
//! the eventfd on every wake cost each wake a system call, and the woken ring a drain and a
//! re-arm: about three system calls per write on a busy shard, on top of the futex wake.
//! So the eventfd is written only while the owner waits in the ring.
//!
//! The handshake is store buffering with a SeqCst fence on each side. The waker stores
//! `pending`, fences, and writes the eventfd only if it then sees `waiting`. The owner
//! stores `waiting`, fences, and waits only if it then does not see `pending`. Either the
//! owner sees the wake and does not wait, or the waker sees the owner waiting and writes the
//! eventfd. A wake while the owner runs leaves `pending` set, so the owner's next ring wait
//! returns at once, as the eventfd's stale count made it before.

#[cfg(io_loom)]
use loom::sync::atomic::{AtomicBool, Ordering, fence};
#[cfg(not(io_loom))]
use std::sync::atomic::{AtomicBool, Ordering, fence};

/// One ring's wake state, shared by its owner and the threads that wake it.
#[derive(Debug, Default)]
pub(crate) struct RingWake {
    /// A wake arrived since the owner last checked.
    pending: AtomicBool,
    /// The owner is in (or about to enter) a ring wait.
    waiting: AtomicBool,
}

impl RingWake {
    /// Waker side: records the wake. Returns whether the owner may be waiting in the ring,
    /// so the caller must write the eventfd the ring polls.
    pub(crate) fn wake(&self) -> bool {
        self.pending.store(true, Ordering::Relaxed);
        fence(Ordering::SeqCst);
        self.waiting.load(Ordering::Relaxed)
    }

    /// Owner side, before a ring wait: returns whether to wait, `false` if a wake is pending
    /// (taken). Call [`RingWake::end_wait`] afterwards either way.
    pub(crate) fn begin_wait(&self) -> bool {
        self.waiting.store(true, Ordering::Relaxed);
        fence(Ordering::SeqCst);
        // A load, then a store only when set: a wake that lands in between finds the owner
        // already returning to its turn, which re-checks its work.
        if self.pending.load(Ordering::Relaxed) {
            self.pending.store(false, Ordering::Relaxed);
            return false;
        }
        true
    }

    /// Owner side: the ring wait is over.
    pub(crate) fn end_wait(&self) {
        self.waiting.store(false, Ordering::Relaxed);
    }
}

#[cfg(all(test, io_loom))]
mod loom_tests {
    use super::RingWake;
    use loom::sync::{Arc, Condvar, Mutex};
    use loom::thread;

    /// The ring's eventfd, as a token: a write before the wait ends the wait at once.
    #[derive(Default)]
    struct EventFd {
        count: Mutex<bool>,
        cv: Condvar,
    }

    impl EventFd {
        fn signal(&self) {
            *self.count.lock().unwrap() = true;
            self.cv.notify_one();
        }

        fn wait(&self) {
            let mut c = self.count.lock().unwrap();
            while !*c {
                c = self.cv.wait(c).unwrap();
            }
            *c = false;
        }
    }

    /// A wake racing the owner's decision to wait in the ring is never lost: the owner either
    /// skips the wait or is woken from it (a lost wake leaves it waiting for ever, which
    /// loom reports as a deadlock).
    #[test]
    fn loom_a_wake_racing_a_ring_wait_is_never_lost() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(|| {
            let wake = Arc::new(RingWake::default());
            let event = Arc::new(EventFd::default());
            let waker = {
                let (wake, event) = (Arc::clone(&wake), Arc::clone(&event));
                thread::spawn(move || {
                    if wake.wake() {
                        event.signal();
                    }
                })
            };
            if wake.begin_wait() {
                event.wait();
            }
            wake.end_wait();
            waker.join().unwrap();
        });
    }
}
