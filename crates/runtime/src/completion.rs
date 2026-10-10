//! One-shot completions that wake either a blocked thread or an async task.

use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::PoisonError;
use std::task::{Context, Poll, Waker};

#[cfg(loom)]
use loom::sync::{Arc, Condvar, Mutex, MutexGuard};
#[cfg(not(loom))]
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

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
    pair(new_slot())
}

fn new_slot<T>() -> Arc<Slot<T>> {
    Arc::new(Slot {
        state: Mutex::new(SlotState {
            value: None,
            done: false,
            waker: None,
            hook: None,
            blocked: false,
        }),
        cv: Condvar::new(),
    })
}

fn pair<T>(slot: Arc<Slot<T>>) -> (Notifier<T>, Waiter<T>) {
    (
        Notifier {
            slot: Some(Arc::clone(&slot)),
        },
        Waiter { slot },
    )
}

/// Up to [`SlotCache::CAP`] finished completions' slots, kept by one thread for its next
/// ones (#320): a completion made with [`completion_from`] reuses one instead of allocating,
/// and on platforms whose locks are boxed lazily (macOS) keeps its lock too. Not `Sync`: a
/// thread keeps its own, typically in a `thread_local!`, and the slots are freed with it.
///
/// ```
/// use pigeonhole_runtime::{SlotCache, completion_from};
///
/// let cache = SlotCache::new();
/// let (notifier, waiter) = completion_from(&cache);
/// notifier.notify(7);
/// let mut waiter = std::pin::pin!(waiter);
/// let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
/// assert_eq!(waiter.as_mut().poll(&mut cx), std::task::Poll::Ready(Some(7)));
/// waiter.recycle_into(&cache); // the notifier is gone: the slot goes back, reset
/// let (notifier, waiter) = completion_from::<u32>(&cache); // reuses it
/// drop(notifier);
/// assert_eq!(waiter.wait(), None);
/// ```
pub struct SlotCache<T> {
    slots: RefCell<Vec<Arc<Slot<T>>>>,
}

impl<T> SlotCache<T> {
    /// Slots a cache keeps at most: a thread that holds many completions at once does not
    /// keep them all afterwards.
    pub const CAP: usize = 8;

    /// An empty cache.
    pub const fn new() -> Self {
        Self {
            slots: RefCell::new(Vec::new()),
        }
    }
}

impl<T> Default for SlotCache<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> fmt::Debug for SlotCache<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlotCache")
            .field("slots", &self.slots.try_borrow().map_or(0, |s| s.len()))
            .finish()
    }
}

/// [`completion`], reusing a slot from `cache` when it has one.
pub fn completion_from<T: Send>(cache: &SlotCache<T>) -> (Notifier<T>, Waiter<T>) {
    let reused = cache.slots.try_borrow_mut().ok().and_then(|mut s| s.pop());
    pair(reused.unwrap_or_else(new_slot))
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

impl<T> Waiter<T> {
    /// Gives this completion's slot to `cache` for a later [`completion_from`], when nothing
    /// else holds it: the notifier has resolved or dropped and released it. The slot is reset
    /// first (no value, not done, no waker, no hook), so the next completion starts fresh. Does
    /// nothing while the notifier still holds the slot, or when the cache is full.
    pub fn recycle_into(&self, cache: &SlotCache<T>) {
        if Arc::strong_count(&self.slot) != 1 {
            return;
        }
        let Ok(mut slots) = cache.slots.try_borrow_mut() else {
            return;
        };
        if slots.len() >= SlotCache::<T>::CAP {
            return;
        }
        // Taken out under the lock and dropped after it: a value's or waker's drop may run
        // arbitrary code.
        let (value, waker, hook) = {
            let mut s = self.slot.lock();
            s.done = false;
            s.blocked = false;
            (s.value.take(), s.waker.take(), s.hook.take())
        };
        drop((value, waker, hook));
        slots.push(Arc::clone(&self.slot));
    }
}

impl<T: Send> Waiter<T> {
    /// [`Waiter::wait`] without consuming the waiter (the loom models recycle it after).
    #[cfg(all(test, loom))]
    fn wait_ref(&self) -> Option<T> {
        let mut s = self.slot.lock();
        while !s.done {
            s.blocked = true;
            s = self.slot.cv.wait(s).unwrap_or_else(PoisonError::into_inner);
        }
        s.value.take()
    }

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

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    /// A waker that counts its wakes.
    struct Counting(AtomicUsize);

    impl Wake for Counting {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn poll<T: Send>(w: &mut Waiter<T>, waker: &Waker) -> Poll<Option<T>> {
        Pin::new(w).poll(&mut Context::from_waker(waker))
    }

    #[test]
    fn a_recycled_slot_starts_fresh() {
        let cache = SlotCache::new();
        let first = Arc::new(Counting(AtomicUsize::new(0)));
        let first_waker = Waker::from(Arc::clone(&first));
        let hooked = Arc::new(AtomicUsize::new(0));

        // A completion that registered a waker and a hook, resolved with 1, and whose value
        // the waiter never took.
        let (n, mut w) = completion_from::<u32>(&cache);
        let h = Arc::clone(&hooked);
        n.on_resolve(move |_| {
            h.fetch_add(1, Ordering::SeqCst);
        });
        assert!(poll(&mut w, &first_waker).is_pending());
        n.notify(1);
        assert_eq!(first.0.load(Ordering::SeqCst), 1);
        let slot = Arc::downgrade(&w.slot);
        w.recycle_into(&cache);
        drop(w);

        // The next completion reuses that slot, and carries none of its state.
        let (n, mut w) = completion_from::<u32>(&cache);
        assert!(
            slot.upgrade().is_some_and(|s| Arc::ptr_eq(&s, &w.slot)),
            "slot not reused"
        );
        let second = Arc::new(Counting(AtomicUsize::new(0)));
        let second_waker = Waker::from(Arc::clone(&second));
        assert!(
            poll(&mut w, &second_waker).is_pending(),
            "a stale value was seen"
        );
        n.notify(2);
        assert_eq!(poll(&mut w, &second_waker), Poll::Ready(Some(2)));
        assert_eq!(first.0.load(Ordering::SeqCst), 1, "the old waker was woken");
        assert_eq!(second.0.load(Ordering::SeqCst), 1);
        assert_eq!(hooked.load(Ordering::SeqCst), 1, "the old hook ran again");

        // Blocking waits on a reused slot block until notified, as on a fresh one.
        w.recycle_into(&cache);
        drop(w);
        let (n, w) = completion_from::<u32>(&cache);
        let t = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            n.notify(3);
        });
        assert_eq!(w.wait(), Some(3));
        t.join().unwrap();
    }

    #[test]
    fn recycling_drops_a_value_nobody_took() {
        // A resolved completion whose waiter never read the value: recycling must drop it,
        // not keep it alive in the cache until the slot's next use.
        let cache = SlotCache::new();
        let value = Arc::new(());
        let (n, w) = completion_from::<Arc<()>>(&cache);
        n.notify(Arc::clone(&value));
        assert_eq!(Arc::strong_count(&value), 2);
        w.recycle_into(&cache);
        assert_eq!(
            Arc::strong_count(&value),
            1,
            "the recycled slot kept the old value"
        );
    }

    #[test]
    fn a_slot_still_held_by_its_notifier_is_not_recycled() {
        let cache = SlotCache::new();
        let (n, w) = completion_from::<u32>(&cache);
        w.recycle_into(&cache);
        assert_eq!(cache.slots.borrow().len(), 0);
        drop(n);
        w.recycle_into(&cache);
        assert_eq!(cache.slots.borrow().len(), 1);
    }

    #[test]
    fn the_cache_keeps_at_most_cap_slots() {
        let cache = SlotCache::new();
        let pairs: Vec<_> = (0..SlotCache::<u32>::CAP + 3)
            .map(|_| completion_from::<u32>(&cache))
            .collect();
        for (n, w) in pairs {
            drop(n);
            w.recycle_into(&cache);
        }
        assert_eq!(cache.slots.borrow().len(), SlotCache::<u32>::CAP);
    }

    #[test]
    fn a_threads_cache_is_freed_when_it_exits() {
        thread_local! {
            static CACHE: SlotCache<u32> = const { SlotCache::new() };
        }
        let slot = std::thread::spawn(|| {
            CACHE.with(|cache| {
                let (n, w) = completion_from(cache);
                n.notify(1);
                w.recycle_into(cache);
                Arc::downgrade(&w.slot)
            })
        })
        .join()
        .unwrap();
        assert!(
            slot.upgrade().is_none(),
            "the exited thread's slot was not freed"
        );
    }

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

/// Loom models of slot reuse (#320): a recycled slot is never handed out while its notifier
/// still uses it, and a reused slot starts fresh. `RUSTFLAGS="--cfg loom" cargo test
/// --release -p pigeonhole-runtime --lib loom`.
#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use loom::thread;

    fn poll_once<T: Send>(w: &mut Waiter<T>) -> Poll<Option<T>> {
        Pin::new(w).poll(&mut Context::from_waker(Waker::noop()))
    }

    /// The next completion from `cache` is fresh: pending until notified, then its own value.
    fn fresh(cache: &SlotCache<u32>) {
        let (n, mut w) = completion_from(cache);
        assert!(
            poll_once(&mut w).is_pending(),
            "a reused slot was already resolved"
        );
        n.notify(2);
        assert_eq!(poll_once(&mut w), Poll::Ready(Some(2)));
    }

    #[test]
    fn loom_recycle_races_the_notifier_releasing_the_slot() {
        loom::model(|| {
            let cache = SlotCache::new();
            let (n, w) = completion_from::<u32>(&cache);
            let notifier = thread::spawn(move || n.notify(1));
            assert_eq!(w.wait_ref(), Some(1));
            // The notifier may still hold the slot here: then it is not recycled.
            w.recycle_into(&cache);
            drop(w);
            fresh(&cache);
            notifier.join().unwrap();
        });
    }

    #[test]
    fn loom_a_waiter_dropped_on_another_thread_recycles_there() {
        loom::model(|| {
            let (n, mut w) = completion::<u32>();
            assert!(poll_once(&mut w).is_pending());
            let notifier = thread::spawn(move || n.notify(1));
            let other = thread::spawn(move || {
                // An async waiter dropped elsewhere, whether or not it saw the value.
                let _ = poll_once(&mut w);
                let cache = SlotCache::new();
                w.recycle_into(&cache);
                drop(w);
                fresh(&cache);
            });
            notifier.join().unwrap();
            other.join().unwrap();
        });
    }
}
