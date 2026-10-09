//! One-shot completions that wake either a blocked thread or an async task.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

/// A callback that sees the outcome before the waiter does ([`Notifier::on_resolve`]).
type Hook<T> = Box<dyn FnOnce(Option<&T>) + Send>;

/// One lock for everything, and the condition variable signalled only when a thread blocks
/// on it: on platforms whose locks are boxed lazily (macOS), a completion then allocates
/// one lock and nothing else (#320).
struct Slot<T> {
    state: Mutex<SlotState<T>>,
    cv: Condvar,
}

struct SlotState<T> {
    value: Option<T>,
    done: bool,
    waker: Option<Waker>,
    hook: Option<Hook<T>>,
    /// A thread waits on `cv` (`Waiter::wait`).
    blocked: bool,
}

impl<T> Slot<T> {
    fn lock(&self) -> MutexGuard<'_, SlotState<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn resolve(&self, value: Option<T>) {
        // The hook runs outside the lock, before the value is visible.
        let hook = self.lock().hook.take();
        if let Some(hook) = hook {
            hook(value.as_ref());
        }
        let (waker, blocked) = {
            let mut s = self.lock();
            s.value = value;
            s.done = true;
            (s.waker.take(), s.blocked)
        };
        // `blocked` is set under the lock before a waiter sleeps, and the waiter checks `done`
        // under the same lock first, so a waiter that sleeps is always signalled.
        if blocked {
            self.cv.notify_one();
        }
        if let Some(w) = waker {
            w.wake();
        }
    }
}

/// Creates a one-shot completion: the shard resolves the [`Notifier`]; the caller blocks on,
/// or awaits, the [`Waiter`].
///
/// ```
/// let (notifier, waiter) = pigeonhole_runtime::completion::<u32>();
/// std::thread::spawn(move || notifier.notify(7));
/// assert_eq!(waiter.wait(), Some(7));
///
/// // A dropped notifier resolves the waiter with `None`.
/// let (notifier, waiter) = pigeonhole_runtime::completion::<u32>();
/// drop(notifier);
/// assert_eq!(waiter.wait(), None);
/// ```
pub fn completion<T: Send>() -> (Notifier<T>, Waiter<T>) {
    let slot = Arc::new(Slot {
        state: Mutex::new(SlotState {
            value: None,
            done: false,
            waker: None,
            hook: None,
            blocked: false,
        }),
        cv: Condvar::new(),
    });
    (
        Notifier {
            slot: Some(Arc::clone(&slot)),
        },
        Waiter { slot },
    )
}

/// The resolving half of a completion. Dropping it unresolved resolves the waiter with
/// `None`. See [`completion`].
pub struct Notifier<T> {
    slot: Option<Arc<Slot<T>>>,
}

impl<T> fmt::Debug for Notifier<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Notifier").finish_non_exhaustive()
    }
}

impl<T: Send> Notifier<T> {
    /// Calls `hook` with the outcome when the completion resolves, before the waiter is
    /// woken: `Some` with the value passed to [`notify`](Self::notify), or `None` if the
    /// notifier is dropped unresolved. It runs on the resolving thread whether or not anyone
    /// still waits, so it suits cleanup that must follow the outcome. A later call replaces
    /// an earlier hook.
    ///
    /// ```
    /// use std::sync::atomic::{AtomicBool, Ordering};
    /// use std::sync::Arc;
    ///
    /// let failed = Arc::new(AtomicBool::new(false));
    /// let (notifier, waiter) = pigeonhole_runtime::completion::<Result<u32, ()>>();
    /// let seen = Arc::clone(&failed);
    /// notifier.on_resolve(move |r| seen.store(!matches!(r, Some(Ok(_))), Ordering::Relaxed));
    /// drop(waiter); // nobody waits; the hook still runs
    /// notifier.notify(Err(()));
    /// assert!(failed.load(Ordering::Relaxed));
    /// ```
    pub fn on_resolve(&self, hook: impl FnOnce(Option<&T>) + Send + 'static) {
        if let Some(slot) = &self.slot {
            slot.lock().hook = Some(Box::new(hook));
        }
    }

    /// Resolves the completion and wakes the waiter (thread unpark or task waker).
    pub fn notify(mut self, value: T) {
        if let Some(slot) = self.slot.take() {
            slot.resolve(Some(value));
        }
    }
}

impl<T> Drop for Notifier<T> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            slot.resolve(None);
        }
    }
}

/// The waiting half of a completion. Sync callers call [`Waiter::wait`]; async callers await
/// it. Dropping it is always safe.
///
/// The future works on any executor: it depends only on [`std::task::Waker`].
///
/// ```
/// use std::future::Future;
/// use std::pin::pin;
/// use std::task::{Context, Poll, Waker};
///
/// let (notifier, waiter) = pigeonhole_runtime::completion::<&str>();
/// let mut fut = pin!(waiter);
/// let mut cx = Context::from_waker(Waker::noop());
/// assert!(fut.as_mut().poll(&mut cx).is_pending());
/// notifier.notify("done");
/// assert_eq!(fut.poll(&mut cx), Poll::Ready(Some("done")));
/// ```
pub struct Waiter<T> {
    slot: Arc<Slot<T>>,
}

impl<T> fmt::Debug for Waiter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Waiter").finish_non_exhaustive()
    }
}

impl<T: Send> Waiter<T> {
    /// Blocks until notified. Returns `None` if the notifier was dropped unresolved.
    pub fn wait(self) -> Option<T> {
        let mut s = self.slot.lock();
        while !s.done {
            s.blocked = true;
            s = self.slot.cv.wait(s).unwrap_or_else(PoisonError::into_inner);
        }
        s.value.take()
    }
}

impl<T: Send> Future for Waiter<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut s = self.slot.lock();
        if s.done {
            return Poll::Ready(s.value.take());
        }
        match &s.waker {
            Some(w) if w.will_wake(cx.waker()) => {}
            _ => s.waker = Some(cx.waker().clone()),
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_blocked_waiter_is_always_woken() {
        // The condition variable is signalled only when a waiter blocks: race the waiter
        // going to sleep against the notification, both ways, many times. A lost wakeup
        // hangs the test.
        for i in 0..20_000u32 {
            let (notifier, waiter) = completion::<u32>();
            let t = std::thread::spawn(move || {
                if i % 2 == 0 {
                    std::thread::yield_now();
                }
                notifier.notify(i);
            });
            assert_eq!(waiter.wait(), Some(i));
            t.join().unwrap();
        }
    }

    #[test]
    fn the_hook_runs_before_the_waiter_sees_the_value() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let ran = Arc::new(AtomicBool::new(false));
        let (notifier, waiter) = completion::<u32>();
        let seen = Arc::clone(&ran);
        notifier.on_resolve(move |v| {
            assert_eq!(v, Some(&7));
            seen.store(true, Ordering::SeqCst);
        });
        let t = std::thread::spawn(move || notifier.notify(7));
        assert_eq!(waiter.wait(), Some(7));
        assert!(ran.load(Ordering::SeqCst));
        t.join().unwrap();
    }
}
