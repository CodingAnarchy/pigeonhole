//! One-shot completions that wake either a blocked thread or an async task.

use std::cell::{OnceCell, RefCell};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::PoisonError;
use std::task::{Context, Poll, Waker};

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicU8, Ordering};
#[cfg(loom)]
use loom::sync::{Arc, Condvar, Mutex, MutexGuard};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
#[cfg(not(loom))]
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

/// A callback that sees the outcome before the waiter does ([`Notifier::on_resolve`]).
type Hook<T> = Box<dyn FnOnce(Option<&T>) + Send>;

/// Told when a completion resolves ([`Waiter::on_settled`]), with the argument it was
/// registered with: cleanup the waiting side owns, without an allocation per completion.
pub trait Settle: Send + Sync {
    /// The completion resolved (or its notifier was dropped).
    fn settled(&self, arg: u64);
}

/// One lock for everything, and the condition variable signalled only when a thread blocks
/// on it: on platforms whose locks are boxed lazily (macOS), a completion then allocates
/// one lock and nothing else (#320).
struct Slot<T> {
    state: Mutex<SlotState<T>>,
    cv: Condvar,
    /// The completion's two holders, the notifier and the waiter, each release the slot once
    /// (`AcqRel`), after their last use of it. The one that releases last returns it to
    /// its owner (#1795): no side's last use can race the slot's reuse, whichever side
    /// finishes first.
    holders: AtomicU8,
    /// Where the slot goes back to once both holders released it: the [`SlotCache`] it was
    /// made from, through its shared half. `None` for [`completion`]'s slots.
    owner: Option<Arc<Returns<T>>>,
}

/// A [`SlotCache`]'s shared half: slots released last on a thread other than the cache's
/// own come back here, and the owner takes them when its own list runs out (#1795). A leaf
/// lock: never held with the slot lock, and nothing that can run user code (a value, a
/// waker or a hook) is dropped while it is held.
struct Returns<T> {
    inner: Mutex<ReturnsInner<T>>,
    /// Set and cleared under `inner`, read without it: a stale `false` costs only a fresh
    /// slot.
    pending: AtomicBool,
}

struct ReturnsInner<T> {
    slots: Vec<Arc<Slot<T>>>,
    /// The owner's cache was dropped (its thread exited): a slot released afterwards is
    /// freed, not kept, so a slot and its owner never keep each other alive.
    gone: bool,
}

struct SlotState<T> {
    value: Option<T>,
    done: bool,
    waker: Option<Waker>,
    hook: Option<Hook<T>>,
    /// [`Waiter::on_settled`]'s callee and argument.
    /// A std `Arc` under loom too (its `Arc` does not coerce to a trait object).
    settle: Option<(std::sync::Arc<dyn Settle>, u64)>,
    /// The resolution has begun: its hook and settle (if any) are taken, so a later settle
    /// runs at once.
    resolving: bool,
    /// A thread waits on `cv` (`Waiter::wait`).
    blocked: bool,
}

impl<T> Slot<T> {
    fn lock(&self) -> MutexGuard<'_, SlotState<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn resolve(&self, value: Option<T>) {
        // The hook runs outside the lock, before the value is visible.
        let (hook, settle) = {
            let mut s = self.lock();
            s.resolving = true;
            let settle = if s.settle.is_some() {
                s.settle.take()
            } else {
                None
            };
            (s.hook.take(), settle)
        };
        if let Some(hook) = hook {
            hook(value.as_ref());
        }
        if let Some((callee, arg)) = settle {
            callee.settled(arg);
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
    pair(new_slot(None))
}

fn new_slot<T>(owner: Option<Arc<Returns<T>>>) -> Arc<Slot<T>> {
    Arc::new(Slot {
        state: Mutex::new(SlotState {
            value: None,
            done: false,
            waker: None,
            hook: None,
            settle: None,
            resolving: false,
            blocked: false,
        }),
        cv: Condvar::new(),
        holders: AtomicU8::new(2),
        owner,
    })
}

/// A completion on `slot`, which nothing else holds (fresh, or both holders of its last
/// completion released it).
fn pair<T>(slot: Arc<Slot<T>>) -> (Notifier<T>, Waiter<T>) {
    // Ordered before the other side's use by however the notifier reaches it.
    slot.holders.store(2, Ordering::Relaxed);
    (
        Notifier {
            slot: Some(Arc::clone(&slot)),
        },
        Waiter {
            slot,
            released: AtomicBool::new(false),
        },
    )
}

impl<T> Slot<T> {
    /// Releases one holder's claim, after its last use of the slot. Whether it was the
    /// last: then the caller is the slot's only user, and returns it.
    fn release(&self) -> bool {
        self.holders.fetch_sub(1, Ordering::AcqRel) == 1
    }

    /// Clears a finished completion's state for the slot's next use. The caller is the
    /// slot's only user and holds no other lock: what comes out (a value nobody took, a
    /// waker, a hook) is returned to be dropped after, as its drop may run arbitrary code.
    fn reset(&self) -> Leftovers<T> {
        let mut s = self.lock();
        // The resolution took it, or `on_settled` ran it at once: never left behind.
        debug_assert!(s.settle.is_none());
        s.done = false;
        s.resolving = false;
        s.blocked = false;
        (s.value.take(), s.waker.take(), s.hook.take())
    }
}

/// What [`Slot::reset`] took out, dropped by its caller once it holds no lock or borrow.
type Leftovers<T> = (Option<T>, Option<Waker>, Option<Hook<T>>);

/// Returns a slot both holders released to its owner's shared half, from any thread: reset
/// first, then kept if the owner is there and has room, else freed. Nothing is dropped
/// under the `Returns` lock (simp724's M3).
fn return_to_owner<T>(slot: Arc<Slot<T>>) {
    let Some(owner) = slot.owner.clone() else {
        return;
    };
    drop(slot.reset());
    let refused = {
        let mut r = owner.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if r.gone || r.slots.len() >= SlotCache::<T>::CAP {
            Some(slot)
        } else {
            r.slots.push(slot);
            owner.pending.store(true, Ordering::Relaxed);
            None
        }
    };
    drop(refused);
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
    /// The shared half, made on first use: where this cache's slots come back when the
    /// last to release one isn't this thread's waiter (#1795).
    returns: OnceCell<Arc<Returns<T>>>,
}

impl<T> SlotCache<T> {
    /// Slots a cache keeps at most in each of its two lists (its own, and the one other
    /// threads return slots to), so up to twice this in all: a thread that holds many
    /// completions at once does not keep them all afterwards.
    pub const CAP: usize = 8;

    /// An empty cache.
    pub const fn new() -> Self {
        Self {
            slots: RefCell::new(Vec::new()),
            returns: OnceCell::new(),
        }
    }

    fn returns(&self) -> &Arc<Returns<T>> {
        self.returns.get_or_init(|| {
            Arc::new(Returns {
                inner: Mutex::new(ReturnsInner {
                    slots: Vec::new(),
                    gone: false,
                }),
                pending: AtomicBool::new(false),
            })
        })
    }

    /// Whether `slot` came from this cache.
    fn owns(&self, slot: &Slot<T>) -> bool {
        match (self.returns.get(), &slot.owner) {
            (Some(mine), Some(its)) => Arc::ptr_eq(mine, its),
            _ => false,
        }
    }

    /// A slot to reuse: this thread's own list first, then what other threads returned.
    fn take(&self) -> Option<Arc<Slot<T>>> {
        let mut slots = self.slots.try_borrow_mut().ok()?;
        if let Some(slot) = slots.pop() {
            return Some(slot);
        }
        let returns = self.returns.get()?;
        if !returns.pending.load(Ordering::Relaxed) {
            return None;
        }
        let mut r = returns.inner.lock().unwrap_or_else(PoisonError::into_inner);
        returns.pending.store(false, Ordering::Relaxed);
        // Moves only `Arc`s: nothing is dropped under the lock. Both lists cap at `CAP`, and
        // this one is empty, so everything fits.
        slots.append(&mut r.slots);
        drop(r);
        slots.pop()
    }
}

impl<T> Drop for SlotCache<T> {
    /// Breaks the slot-to-owner link at the one moment it matters: a slot already returned
    /// (or released later) would otherwise keep the shared half alive, and it the slot.
    fn drop(&mut self) {
        if let Some(returns) = self.returns.get() {
            let kept = {
                let mut r = returns.inner.lock().unwrap_or_else(PoisonError::into_inner);
                r.gone = true;
                std::mem::take(&mut r.slots)
            };
            drop(kept);
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
    let slot = cache
        .take()
        .unwrap_or_else(|| new_slot(Some(Arc::clone(cache.returns()))));
    pair(slot)
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
            release_notifier(slot);
        }
    }
}

impl<T> Drop for Notifier<T> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            slot.resolve(None);
            release_notifier(slot);
        }
    }
}

/// The notifier's release, after `resolve`'s last touch of the slot (its condvar signal and
/// wake). Released last (its waiter recycled or dropped first), it returns the slot.
fn release_notifier<T>(slot: Arc<Slot<T>>) {
    if slot.release() {
        return_to_owner(slot);
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
    /// [`recycle_into`](Waiter::recycle_into) released the slot: the waiter may no longer
    /// use it, and its drop releases nothing. Atomic (and only read on its own thread) so
    /// the waiter stays `Sync`.
    released: AtomicBool,
}

impl<T> fmt::Debug for Waiter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Waiter").finish_non_exhaustive()
    }
}

impl<T> Waiter<T> {
    /// Releases this completion's slot, its last use by the waiter: the waiter must not be
    /// polled, waited on or given `on_settled` afterwards, as the slot may already be
    /// another completion's. Whichever of the waiter and the notifier releases last returns
    /// the slot, reset, to the [`SlotCache`] it was made from (#1795): to `cache` itself
    /// when that is it and this is its thread, else to the cache's shared half. It releases
    /// whatever the notifier's state, before resolution too (a pending future dropped by
    /// its owner). A slot from [`completion`] is freed instead.
    pub fn recycle_into(&self, cache: &SlotCache<T>) {
        if self.released.swap(true, Ordering::Relaxed) || !self.slot.release() {
            return;
        }
        if !cache.owns(&self.slot) {
            return_to_owner(Arc::clone(&self.slot));
            return;
        }
        // Reset with no borrow held: the leftovers' drops may run arbitrary code.
        drop(self.slot.reset());
        let reentered = match cache.slots.try_borrow_mut() {
            Ok(mut slots) => {
                // A full list keeps nothing: the slot is freed with the waiter's `Arc`,
                // after the borrow.
                if slots.len() < SlotCache::<T>::CAP {
                    slots.push(Arc::clone(&self.slot));
                }
                false
            }
            Err(_) => true,
        };
        if reentered {
            // A leftover's drop is recycling another completion: the shared half.
            return_to_owner(Arc::clone(&self.slot));
        }
    }
}

impl<T> Drop for Waiter<T> {
    /// A waiter dropped without [`recycle_into`](Waiter::recycle_into) releases the slot
    /// here, and returns it if the notifier already released it.
    fn drop(&mut self) {
        if !self.released.load(Ordering::Relaxed) && self.slot.release() {
            return_to_owner(Arc::clone(&self.slot));
        }
    }
}

impl<T: Send> Waiter<T> {
    /// Calls `callee.settled(arg)` once the completion resolves (or its notifier is
    /// dropped): on the resolving thread, or at once on this one if the resolution has
    /// begun. For cleanup the waiting side owns, such as returning what a submission was
    /// charged, which must not wait for anyone to poll or drop the waiter. One per
    /// completion (a later one replaces it). Its order against a hook the notifier set is
    /// unspecified.
    ///
    /// ```
    /// use std::sync::atomic::{AtomicU64, Ordering};
    /// use std::sync::Arc;
    /// use pigeonhole_runtime::Settle;
    ///
    /// struct Sum(AtomicU64);
    /// impl Settle for Sum {
    ///     fn settled(&self, arg: u64) {
    ///         self.0.fetch_add(arg, Ordering::Relaxed);
    ///     }
    /// }
    /// let sum = Arc::new(Sum(AtomicU64::new(0)));
    /// let (notifier, waiter) = pigeonhole_runtime::completion::<u32>();
    /// waiter.on_settled(Arc::clone(&sum) as _, 5);
    /// assert_eq!(sum.0.load(Ordering::Relaxed), 0);
    /// notifier.notify(7);
    /// assert_eq!(sum.0.load(Ordering::Relaxed), 5);
    /// let (notifier, waiter) = pigeonhole_runtime::completion::<u32>();
    /// drop(notifier);
    /// waiter.on_settled(Arc::clone(&sum) as _, 2); // already resolved: runs now
    /// assert_eq!(sum.0.load(Ordering::Relaxed), 7);
    /// ```
    pub fn on_settled(&self, callee: std::sync::Arc<dyn Settle>, arg: u64) {
        debug_assert!(
            !self.released.load(Ordering::Relaxed),
            "used after recycle_into"
        );
        let mut s = self.slot.lock();
        if s.resolving || s.done {
            drop(s);
            callee.settled(arg);
            return;
        }
        s.settle = Some((callee, arg));
    }
}

impl<T: Send> Waiter<T> {
    /// [`Waiter::wait`] without consuming the waiter (the loom models recycle it after).
    #[cfg(all(test, loom))]
    fn wait_ref(&self) -> Option<T> {
        debug_assert!(
            !self.released.load(Ordering::Relaxed),
            "used after recycle_into"
        );
        let mut s = self.slot.lock();
        while !s.done {
            s.blocked = true;
            s = self.slot.cv.wait(s).unwrap_or_else(PoisonError::into_inner);
        }
        s.value.take()
    }

    /// Blocks until notified. Returns `None` if the notifier was dropped unresolved.
    pub fn wait(self) -> Option<T> {
        debug_assert!(
            !self.released.load(Ordering::Relaxed),
            "used after recycle_into"
        );
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
        debug_assert!(
            !self.released.load(Ordering::Relaxed),
            "used after recycle_into"
        );
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
    use std::sync::atomic::AtomicUsize;
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
    fn a_slot_its_notifier_releases_last_returns_to_its_owner() {
        // The waiter recycles first (before the notifier is done with the slot), so the
        // notifier is the last to release it: the slot goes to the cache's shared half, and
        // the next completion takes it from there, fresh (#1795).
        let cache = SlotCache::new();
        let (n, w) = completion_from::<u32>(&cache);
        let slot = Arc::downgrade(&w.slot);
        w.recycle_into(&cache);
        drop(w);
        assert_eq!(
            cache.slots.borrow().len(),
            0,
            "recycled while the notifier held it"
        );
        n.notify(1);
        assert_eq!(cache.returns().inner.lock().unwrap().slots.len(), 1);
        let (n, mut w) = completion_from::<u32>(&cache);
        assert!(
            slot.upgrade().is_some_and(|s| Arc::ptr_eq(&s, &w.slot)),
            "the returned slot was not reused"
        );
        assert!(
            poll(&mut w, Waker::noop()).is_pending(),
            "a stale value was seen"
        );
        n.notify(2);
        assert_eq!(poll(&mut w, Waker::noop()), Poll::Ready(Some(2)));
    }

    #[test]
    fn a_slot_released_last_on_another_thread_comes_back() {
        let cache = SlotCache::new();
        let (n, w) = completion_from::<u32>(&cache);
        let slot = Arc::downgrade(&w.slot);
        w.recycle_into(&cache);
        drop(w);
        std::thread::spawn(move || n.notify(1)).join().unwrap();
        let (n, w) = completion_from::<u32>(&cache);
        assert!(slot.upgrade().is_some_and(|s| Arc::ptr_eq(&s, &w.slot)));
        drop(n);
        assert_eq!(w.wait(), None);
    }

    /// Counts its drops.
    struct Dropped(Arc<AtomicUsize>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn an_exited_threads_returned_slots_are_freed() {
        // simp724's M1: a slot returned to a thread's shared half, or released after the
        // thread exited, is freed with it, not kept alive by the slot-to-owner link.
        thread_local! {
            static CACHE: SlotCache<Dropped> = const { SlotCache::new() };
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let (returned, late, late_slot) = std::thread::spawn(|| {
            CACHE.with(|cache| {
                let (n1, w1) = completion_from::<Dropped>(cache);
                let (n2, w2) = completion_from::<Dropped>(cache);
                // Released last by its notifier: in the shared half when the thread exits.
                let returned = Arc::downgrade(&w1.slot);
                w1.recycle_into(cache);
                drop(w1);
                drop(n1);
                // Released by its notifier only after the thread exits.
                let late_slot = Arc::downgrade(&w2.slot);
                w2.recycle_into(cache);
                drop(w2);
                (returned, n2, late_slot)
            })
        })
        .join()
        .unwrap();
        assert!(
            returned.upgrade().is_none(),
            "a returned slot outlived its thread"
        );
        late.notify(Dropped(Arc::clone(&drops)));
        assert!(
            late_slot.upgrade().is_none(),
            "a slot released after its thread exited lived on"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "the value nobody took was not dropped"
        );
    }

    #[test]
    fn a_leftover_whose_drop_uses_the_cache_does_not_deadlock() {
        // simp724's M3: a value nobody took is dropped outside the shared half's lock and the
        // cache's borrow, so a drop that makes and recycles a completion from the same cache
        // works, on both the notifier-last and the waiter-last path, with the cache full.
        thread_local! {
            static CACHE: SlotCache<Reentrant> = const { SlotCache::new() };
        }
        struct Reentrant;
        impl Drop for Reentrant {
            fn drop(&mut self) {
                CACHE.with(|c| {
                    let (n, w) = completion_from::<Reentrant>(c);
                    drop(n);
                    w.recycle_into(c);
                });
            }
        }
        CACHE.with(|cache| {
            let fill: Vec<_> = (0..2 * SlotCache::<Reentrant>::CAP)
                .map(|_| completion_from::<Reentrant>(cache))
                .collect();
            for (n, w) in fill {
                drop(n);
                w.recycle_into(cache);
            }
            // Notifier last, its value untaken.
            let (n, w) = completion_from::<Reentrant>(cache);
            w.recycle_into(cache);
            drop(w);
            n.notify(Reentrant);
            // Waiter last, its value untaken.
            let (n, w) = completion_from::<Reentrant>(cache);
            n.notify(Reentrant);
            w.recycle_into(cache);
        });
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

    /// Slots the cache holds in both lists.
    fn kept(cache: &SlotCache<u32>) -> usize {
        let shared = cache
            .returns
            .get()
            .map_or(0, |r| r.inner.lock().unwrap().slots.len());
        cache.slots.borrow().len() + shared
    }

    /// The notifier and the waiter release in either order (#1795): the slot comes back
    /// exactly once, to the local list or the shared half, and the next completion reuses
    /// it fresh.
    #[test]
    fn loom_either_side_releases_last() {
        loom::model(|| {
            let cache = SlotCache::new();
            let (n, w) = completion_from::<u32>(&cache);
            let notifier = thread::spawn(move || n.notify(1));
            assert_eq!(w.wait_ref(), Some(1));
            w.recycle_into(&cache);
            drop(w);
            notifier.join().unwrap();
            assert_eq!(kept(&cache), 1, "the slot came back other than once");
            fresh(&cache);
        });
    }

    /// A waiter dropped on a third thread while the notifier resolves: the slot comes back
    /// once, to the owner's shared half, whichever releases last.
    #[test]
    fn loom_a_waiter_dropped_on_a_third_thread() {
        loom::model(|| {
            let cache = SlotCache::new();
            let (n, mut w) = completion_from::<u32>(&cache);
            assert!(poll_once(&mut w).is_pending());
            let notifier = thread::spawn(move || n.notify(1));
            let other = thread::spawn(move || {
                let _ = poll_once(&mut w);
                drop(w);
            });
            notifier.join().unwrap();
            other.join().unwrap();
            assert_eq!(kept(&cache), 1);
            fresh(&cache);
        });
    }

    /// The owner drains its shared half while the notifier, last, pushes the slot there:
    /// the slot is reused or still kept, never lost or kept twice.
    #[test]
    fn loom_the_drain_races_a_remote_return() {
        loom::model(|| {
            let cache = SlotCache::new();
            let (n, w) = completion_from::<u32>(&cache);
            let first = Arc::clone(&w.slot);
            w.recycle_into(&cache);
            drop(w);
            let notifier = thread::spawn(move || n.notify(1));
            let (n2, w2) = completion_from::<u32>(&cache);
            let reused = Arc::ptr_eq(&first, &w2.slot);
            drop(first);
            notifier.join().unwrap();
            assert_eq!(usize::from(reused) + kept(&cache), 1);
            drop(n2);
            assert_eq!(w2.wait_ref(), None);
        });
    }

    /// The owner's cache is dropped (its thread exiting) while the notifier releases last
    /// (simp724's M1): the slot isn't left in the shared half.
    #[test]
    fn loom_the_owner_exits_while_a_slot_returns() {
        loom::model(|| {
            let cache = SlotCache::new();
            let (n, w) = completion_from::<u32>(&cache);
            let returns = Arc::clone(cache.returns());
            w.recycle_into(&cache);
            drop(w);
            let notifier = thread::spawn(move || n.notify(1));
            drop(cache);
            notifier.join().unwrap();
            let r = returns.inner.lock().unwrap();
            assert!(r.gone);
            assert!(r.slots.is_empty(), "a slot outlived its owner's cache");
        });
    }

    /// `on_settled` set from the waiting side while the notifier resolves on another
    /// thread (D210): the callback and the notifier's hook each run exactly once, whichever
    /// side gets there first.
    #[test]
    fn loom_on_settled_races_the_resolution() {
        use loom::sync::atomic::{AtomicU64, Ordering};
        struct Runs(AtomicU64);
        impl Settle for Runs {
            fn settled(&self, arg: u64) {
                self.0.fetch_add(arg, Ordering::SeqCst);
            }
        }
        loom::model(|| {
            let runs = std::sync::Arc::new(Runs(AtomicU64::new(0)));
            let (n, w) = completion::<u32>();
            let h = std::sync::Arc::clone(&runs);
            n.on_resolve(move |_| {
                h.0.fetch_add(10, Ordering::SeqCst);
            });
            let notifier = thread::spawn(move || n.notify(1));
            w.on_settled(
                std::sync::Arc::clone(&runs) as std::sync::Arc<dyn Settle>,
                1,
            );
            notifier.join().unwrap();
            assert_eq!(w.wait_ref(), Some(1));
            assert_eq!(runs.0.load(Ordering::SeqCst), 11);
        });
    }
}
