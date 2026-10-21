//! A shared `Arc<T>` a rare writer replaces and every operation reads (D265: the engine
//! behind a database handle, replaced by a recovery): a reader pays one thread-local store
//! and two loads, no read-modify-write on a line other threads touch, and the writer waits
//! until no reader still uses the old value before it drops it (a hazard pointer with one
//! slot per thread).
//!
//! Why not `arc_swap`: its load costs about 80 instructions (a debt slot search in a
//! thread-local list, a fence and a re-check, then the debt's repayment), which the read
//! shapes' instruction ceilings do not have room for twice per operation (the engine already
//! pays one for its view). This load is about a fifth of that.
//!
//! The protocol (the standard hazard-pointer argument): a reader publishes the pointer it
//! read in its slot (`SeqCst`) and then reads the current pointer again (`SeqCst`); the
//! writer publishes the new pointer (`SeqCst`) and then reads every slot (`SeqCst`). In the
//! total order of `SeqCst` operations, either the reader's second read sees the new pointer
//! (it retries) or the writer's scan sees the reader's slot (it waits). So a pointer a
//! reader holds is never freed under it.
//!
//! A thread's slot is taken from a free list on the thread's first load and given back when
//! the thread ends; slots are leaked only if a thread never ends. A nested load on the same
//! thread (one operation inside another, which the public API does not do but a future
//! change might) returns the outer load's pointer, which the slot already protects.

use std::cell::Cell;
use std::ops::Deref;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

/// One thread's hazard: the pointer it is using, or null.
struct Slot {
    hazard: AtomicPtr<()>,
    /// Taken by a live thread; a thread that ends gives its slot back.
    in_use: AtomicBool,
}

/// Every slot ever made (threads take and return them; never freed).
fn slots() -> &'static Mutex<Vec<&'static Slot>> {
    static SLOTS: OnceLock<Mutex<Vec<&'static Slot>>> = OnceLock::new();
    SLOTS.get_or_init(Mutex::default)
}

/// A thread's slot and how many guards it holds (nesting).
struct Local {
    slot: &'static Slot,
    depth: Cell<u32>,
}

impl Local {
    fn new() -> Self {
        let mut all = slots().lock().unwrap_or_else(PoisonError::into_inner);
        let slot = match all.iter().find(|s| {
            s.in_use
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        }) {
            Some(s) => *s,
            None => {
                let s: &'static Slot = Box::leak(Box::new(Slot {
                    hazard: AtomicPtr::new(ptr::null_mut()),
                    in_use: AtomicBool::new(true),
                }));
                all.push(s);
                s
            }
        };
        Self {
            slot,
            depth: Cell::new(0),
        }
    }
}

impl Drop for Local {
    fn drop(&mut self) {
        self.slot.hazard.store(ptr::null_mut(), Ordering::Release);
        self.slot.in_use.store(false, Ordering::Release);
    }
}

thread_local! {
    static LOCAL: Local = Local::new();
}

/// The shared value: one strong count of the `Arc` it holds.
pub(crate) struct Swap<T> {
    current: AtomicPtr<T>,
}

impl<T> Swap<T> {
    pub(crate) fn new(value: Arc<T>) -> Self {
        Self {
            current: AtomicPtr::new(Arc::into_raw(value).cast_mut()),
        }
    }

    /// The current value, protected while the guard lives. Hold it for one operation.
    #[inline]
    pub(crate) fn load(&self) -> Guard<'_, T> {
        LOCAL.with(|local| {
            let depth = local.depth.get();
            let ptr = if depth == 0 {
                loop {
                    let p = self.current.load(Ordering::SeqCst);
                    local.slot.hazard.store(p.cast(), Ordering::SeqCst);
                    if self.current.load(Ordering::SeqCst) == p {
                        break p;
                    }
                }
            } else {
                // Nested: the outer guard's pointer, which the slot protects.
                local.slot.hazard.load(Ordering::Relaxed).cast()
            };
            local.depth.set(depth + 1);
            Guard {
                ptr,
                slot: local.slot,
                depth: &local.depth,
                _swap: std::marker::PhantomData,
            }
        })
    }

    /// Replaces the value and returns the previous one, once no reader uses it any more
    /// (it waits for in-flight loads to end: microseconds, since guards live for one
    /// operation). Not for the hot path.
    pub(crate) fn swap(&self, value: Arc<T>) -> Arc<T> {
        let new = Arc::into_raw(value).cast_mut();
        let old = self.current.swap(new, Ordering::SeqCst);
        loop {
            let busy = slots()
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .any(|s| s.hazard.load(Ordering::SeqCst) == old.cast());
            if !busy {
                break;
            }
            std::thread::yield_now();
        }
        // SAFETY: `old` came from `Arc::into_raw` in `new` or an earlier `swap`, and no
        // reader holds it (the scan above, under the protocol in the module doc).
        unsafe { Arc::from_raw(old) }
    }
}

impl<T> Drop for Swap<T> {
    fn drop(&mut self) {
        // SAFETY: the strong count `new`/`swap` left in `current`; `&mut self` means no guard
        // can be taken on it any more.
        unsafe { drop(Arc::from_raw(*self.current.get_mut())) }
    }
}

impl<T> std::fmt::Debug for Swap<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Swap")
            .field("current", &self.current.load(Ordering::Relaxed))
            .finish()
    }
}

/// A loaded value, protected until dropped. Not `Send`: the protection is this thread's slot.
pub(crate) struct Guard<'a, T> {
    ptr: *const T,
    slot: &'static Slot,
    /// The thread's nesting depth; the `Local` it lives in outlives every guard on the thread.
    depth: *const Cell<u32>,
    _swap: std::marker::PhantomData<&'a Swap<T>>,
}

impl<T> Guard<'_, T> {
    /// A strong reference of its own, for a caller that must outlive the guard.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn to_arc(&self) -> Arc<T> {
        // SAFETY: the pointer is a live `Arc` allocation while this guard lives.
        unsafe {
            Arc::increment_strong_count(self.ptr);
            Arc::from_raw(self.ptr)
        }
    }
}

impl<T> Deref for Guard<'_, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        // SAFETY: protected by the slot (or by the outer guard's slot entry when nested).
        unsafe { &*self.ptr }
    }
}

impl<T> Drop for Guard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: `depth` points into this thread's `Local`, alive while any guard is.
        let depth = unsafe { &*self.depth };
        let d = depth.get() - 1;
        depth.set(d);
        if d == 0 {
            self.slot.hazard.store(ptr::null_mut(), Ordering::Release);
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Guard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn a_swap_waits_for_the_readers_of_the_old_value_and_frees_it_after() {
        struct Counted(&'static AtomicUsize);
        impl Drop for Counted {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        let swap = Arc::new(Swap::new(Arc::new(Counted(&DROPS))));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let reader = {
            let swap = Arc::clone(&swap);
            std::thread::spawn(move || {
                let guard = swap.load();
                let nested = swap.load();
                assert!(ptr::eq(&*guard, &*nested));
                entered_tx.send(()).unwrap();
                go_rx.recv().unwrap();
                drop(nested);
                std::thread::sleep(std::time::Duration::from_millis(50));
                drop(guard);
            })
        };
        entered_rx.recv().unwrap();
        let swapper = {
            let swap = Arc::clone(&swap);
            std::thread::spawn(move || drop(swap.swap(Arc::new(Counted(&DROPS)))))
        };
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert!(
            !swapper.is_finished(),
            "the swap returned under a live guard"
        );
        assert_eq!(DROPS.load(Ordering::SeqCst), 0);
        go_tx.send(()).unwrap();
        reader.join().unwrap();
        swapper.join().unwrap();
        assert_eq!(
            DROPS.load(Ordering::SeqCst),
            1,
            "the old value dropped after the swap"
        );
        drop(swap);
        assert_eq!(DROPS.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn many_readers_and_swaps_never_read_a_freed_value() {
        let swap = Arc::new(Swap::new(Arc::new(Box::new(0u64))));
        let stop = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let (swap, stop) = (Arc::clone(&swap), Arc::clone(&stop));
                std::thread::spawn(move || {
                    let mut seen = 0;
                    while !stop.load(Ordering::Relaxed) {
                        let g = swap.load();
                        assert!(**g < 1_000_000, "a freed or garbage value");
                        seen += 1;
                    }
                    seen
                })
            })
            .collect();
        for i in 1..=2_000u64 {
            drop(swap.swap(Arc::new(Box::new(i))));
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            assert!(r.join().unwrap() > 0);
        }
    }
}
