//! One-shot completions that wake either a blocked thread or an async task.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

struct Slot<T> {
    state: Mutex<SlotState<T>>,
    cv: Condvar,
}

struct SlotState<T> {
    value: Option<T>,
    done: bool,
    waker: Option<Waker>,
}

impl<T> Slot<T> {
    fn lock(&self) -> MutexGuard<'_, SlotState<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn resolve(&self, value: Option<T>) {
        let waker = {
            let mut s = self.lock();
            s.value = value;
            s.done = true;
            s.waker.take()
        };
        self.cv.notify_one();
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
