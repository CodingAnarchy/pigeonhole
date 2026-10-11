//! A clock timer's state, shared between the shard and its `ClockTimer` task (#84, #93,
//! #244; #134 step 3: loom-modelled).
//!
//! Two handshakes:
//!
//! - **Cancel against sleep.** The shard stores `cancel`, then takes the task's waker under
//!   the lock and wakes it ([`TimerState::cancel`]). The task stores its waker under the
//!   lock, then looks at `cancel` again ([`TimerState::sleep_with`]). Whichever takes the
//!   lock second sees the other's step: a cancel never leaves the task asleep.
//! - **Giving up is published before done.** A timer that gives up on a stopped clock
//!   stores the reading (`frozen_at`) before `done` ([`TimerState::give_up`]). A shard that
//!   sees `done` therefore sees the reading too, and never takes a timer that gave up for
//!   one that fired.

#[cfg(engine_loom)]
use loom::sync::Mutex;
#[cfg(engine_loom)]
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(not(engine_loom))]
use std::sync::Mutex;
#[cfg(not(engine_loom))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use std::sync::{Arc, PoisonError};

use pigeonhole_runtime::TaskWaker;

/// What wakes the sleeping timer task.
pub(crate) trait TimerWake {
    fn wake_timer(&self);
}

impl TimerWake for TaskWaker {
    fn wake_timer(&self) {
        self.wake();
    }
}

/// The state (see the module documentation).
#[derive(Debug)]
pub(crate) struct TimerState<W = TaskWaker> {
    /// Set by the shard to cancel the timer.
    cancel: AtomicBool,
    /// Set by the timer when it fired or gave up.
    done: AtomicBool,
    /// The clock reading at which the timer gave up because the clock stopped moving
    /// (`u64::MAX`: it did not give up).
    frozen_at: AtomicU64,
    /// The timer task's waker while it sleeps, so a cancel ends the sleep at once.
    waker: Mutex<Option<W>>,
}

impl<W: TimerWake> TimerState<W> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            cancel: AtomicBool::new(false),
            done: AtomicBool::new(false),
            frozen_at: AtomicU64::new(u64::MAX),
            waker: Mutex::new(None),
        })
    }

    /// The shard cancels the timer, waking it if it sleeps.
    pub(crate) fn cancel(&self) {
        self.cancel.store(true, Ordering::Release);
        let waker = self
            .waker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(w) = waker {
            w.wake_timer();
        }
    }

    /// The task: whether the shard cancelled it.
    pub(crate) fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::Acquire)
    }

    /// The task, before it sleeps: registers `waker`, then looks at `cancel` again.
    /// Returns whether it may sleep (`false`: it was cancelled, and ends).
    pub(crate) fn sleep_with(&self, waker: W) -> bool {
        *self.waker.lock().unwrap_or_else(PoisonError::into_inner) = Some(waker);
        !self.cancelled()
    }

    /// The task: the deadline passed.
    pub(crate) fn fire(&self) {
        self.done.store(true, Ordering::Release);
    }

    /// The task: the clock read `now` too many times in a row; it gives up, publishing the
    /// reading before `done`.
    pub(crate) fn give_up(&self, now: u64) {
        self.frozen_at.store(now, Ordering::Release);
        self.done.store(true, Ordering::Release);
    }

    /// The timer is no longer running.
    pub(crate) fn finished(&self) -> bool {
        self.done.load(Ordering::Acquire) || self.cancel.load(Ordering::Acquire)
    }

    /// The timer fired (its deadline passed).
    pub(crate) fn fired(&self) -> bool {
        self.done.load(Ordering::Acquire) && self.frozen_at.load(Ordering::Acquire) == u64::MAX
    }

    /// The timer gave up on a stopped clock and the clock still reads `now`: only events
    /// can end the wait (the frozen-clock fallbacks of issue #70 apply).
    pub(crate) fn frozen(&self, now: u64) -> bool {
        self.frozen_at.load(Ordering::Acquire) == now
    }
}

#[cfg(all(test, engine_loom))]
mod loom_tests {
    use super::{TimerState, TimerWake};
    use loom::sync::{Condvar, Mutex};
    use loom::thread;
    use std::sync::Arc;

    /// The timer task's waker: a token the task waits on (its scheduler parking it).
    #[derive(Debug, Default)]
    struct Token {
        woken: Mutex<bool>,
        cv: Condvar,
    }

    #[derive(Debug, Clone)]
    struct Waker(Arc<Token>);

    impl TimerWake for Waker {
        fn wake_timer(&self) {
            *self.0.woken.lock().unwrap() = true;
            self.0.cv.notify_one();
        }
    }

    #[test]
    fn loom_a_cancel_never_leaves_the_timer_asleep() {
        // The shard cancels while the timer task goes to sleep (its deadline never passes):
        // the task ends, at once or after the cancel wakes it.
        loom::model(|| {
            let state = TimerState::<Waker>::new();
            let token = Arc::new(Token::default());
            let task = {
                let (state, token) = (Arc::clone(&state), Arc::clone(&token));
                thread::spawn(move || {
                    // `ClockTimer::run`, run again by the scheduler each time it is woken.
                    loop {
                        if state.cancelled() {
                            return;
                        }
                        if !state.sleep_with(Waker(Arc::clone(&token))) {
                            return;
                        }
                        let mut woken = token.woken.lock().unwrap();
                        while !*woken {
                            woken = token.cv.wait(woken).unwrap();
                        }
                        *woken = false;
                    }
                })
            };
            state.cancel();
            task.join().unwrap();
            assert!(state.finished());
        });
    }

    #[test]
    fn loom_a_timer_that_gave_up_is_never_seen_as_fired() {
        // The timer gives up on a clock stuck at 5 while the shard looks: once the shard
        // sees it done, it sees the reading, so it applies the frozen-clock fallback rather
        // than taking the timer as fired.
        loom::model(|| {
            let state = TimerState::<Waker>::new();
            let timer = {
                let state = Arc::clone(&state);
                thread::spawn(move || state.give_up(5))
            };
            for _ in 0..2 {
                assert!(!state.fired(), "a timer that gave up was taken as fired");
                if state.finished() {
                    assert!(state.frozen(5));
                }
            }
            timer.join().unwrap();
            assert!(state.frozen(5) && !state.fired());
        });
    }
}
