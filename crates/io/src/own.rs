//! I/O only the thread that submitted it completes (#207).
//!
//! A backend can complete a thread's submitted operations only when that thread reaps them:
//! an io_uring ring owned by a shard thread (Phase 3), or [`SimVfs`](crate::sim::SimVfs) in
//! owner-reaps mode. A thread that blocks on something its own I/O must finish first (the
//! manifest writer's exclusion, a WAL stream's older syncs) would then wait for ever: such
//! waits call [`reap_own_io`] instead of parking while [`own_io_in_flight`] says the thread
//! has I/O only it can complete.
//!
//! ```
//! use std::time::Duration;
//!
//! // No backend here completes I/O only on its submitting thread.
//! assert!(!pigeonhole_io::own_io_in_flight());
//! assert!(!pigeonhole_io::reap_own_io(Some(Duration::from_millis(1))));
//! ```

use std::cell::RefCell;
use std::sync::{Arc, Weak};
use std::time::Duration;

/// A backend whose submitted operations a thread completes by reaping them itself.
pub(crate) trait OwnIo: Send + Sync {
    /// Whether the calling thread has operations in flight that only it can complete.
    fn in_flight_here(&self) -> bool;
    /// Completes the calling thread's finished operations, waiting up to `wait` for one to
    /// finish when none has (`None`: take only what has already finished). Returns whether
    /// any completed.
    fn reap_here(&self, wait: Option<Duration>) -> bool;
    /// A handle that interrupts the calling thread's wait in [`OwnIo::reap_here`], if the
    /// backend's waits can be interrupted.
    fn waker_here(&self) -> Option<OwnIoWaker> {
        None
    }
    /// A descriptor that turns readable when the calling thread has completions to reap
    /// (#408), if the backend has one.
    fn fd_here(&self) -> Option<i32> {
        None
    }
}

/// Interrupts its thread's wait in [`reap_own_io`] (an io_uring ring's wait for completions,
/// #402) from any thread: the scheduler's wakeup for a shard thread that waits on its ring
/// rather than parking.
#[derive(Clone)]
pub struct OwnIoWaker(pub(crate) Arc<dyn Fn() + Send + Sync>);

impl OwnIoWaker {
    /// Wakes the thread, now if it waits in [`reap_own_io`], or else at its next such wait.
    pub fn wake(&self) {
        (self.0)();
    }
}

impl std::fmt::Debug for OwnIoWaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OwnIoWaker")
    }
}

thread_local! {
    /// The backends this thread has submitted owner-reaped operations to.
    static OWN: RefCell<Vec<Weak<dyn OwnIo>>> = const { RefCell::new(Vec::new()) };
}

/// Records that the calling thread submitted owner-reaped operations to `own`.
pub(crate) fn register(own: Weak<dyn OwnIo>) {
    let _ = OWN.try_with(|o| {
        let mut o = o.borrow_mut();
        o.retain(|w| w.strong_count() > 0);
        if !o
            .iter()
            .any(|w| std::ptr::addr_eq(w.as_ptr(), own.as_ptr()))
        {
            o.push(own);
        }
    });
}

/// The live backends of this thread, taken out of the registry's borrow (reaping runs
/// continuations, which may submit and register again). Allocates nothing for a thread with
/// none, the common case.
fn backends() -> Vec<Arc<dyn OwnIo>> {
    OWN.try_with(|o| {
        let o = o.borrow();
        if o.is_empty() {
            Vec::new()
        } else {
            o.iter().filter_map(Weak::upgrade).collect()
        }
    })
    .unwrap_or_default()
}

/// Whether the calling thread has submitted operations in flight that only it can complete
/// (by [`reap_own_io`]). Always `false` on backends whose completions arrive on other
/// threads, such as [`PreadVfs`](crate::pread::PreadVfs).
pub fn own_io_in_flight() -> bool {
    backends().iter().any(|b| b.in_flight_here())
}

/// Completes the calling thread's finished operations on every backend that leaves them to
/// it, waiting up to `wait` for one to finish when none has. `None` takes only what has
/// already finished, without waiting (a shard's turn); the simulator's device finishes its
/// operations only for a thread that waits. Their completions resolve, and their
/// continuations run, on this thread. Returns whether any completed.
pub fn reap_own_io(wait: Option<Duration>) -> bool {
    let mut any = false;
    for b in backends() {
        any |= b.reap_here(wait);
    }
    any
}

/// A handle that interrupts the calling thread's waits in [`reap_own_io`], from the first of
/// its backends that offers one (`None` when no backend's wait can be interrupted: a
/// scheduler then parks the thread as usual).
pub fn own_io_waker() -> Option<OwnIoWaker> {
    backends().iter().find_map(|b| b.waker_here())
}

/// A file descriptor that turns readable when the calling thread has completions to reap
/// (an io_uring ring's registered eventfd, #408), for a thread that waits in its own event
/// loop (`poll`, `epoll`) rather than in [`reap_own_io`]: wait until it is readable, then
/// reap. Reaping resets it. `None` when the thread's backends offer no such descriptor (the
/// thread then has to reap again soon while [`own_io_in_flight`] says it has I/O in flight).
pub fn own_io_fd() -> Option<i32> {
    backends().iter().find_map(|b| b.fd_here())
}

// ---- rings no thread reaps on its own (#408) ----

/// A backend whose completions no thread of its own takes (an io_uring shared ring without a
/// reaper, in application-owned mode): any thread blocked on a completion may take them.
pub(crate) trait OrphanIo: Send + Sync {
    /// Takes what has completed, waiting up to `wait` for something to when nothing has.
    /// Returns whether an operation completed.
    fn reap_orphan(&self, wait: Duration) -> bool;
}

static ORPHANS: std::sync::Mutex<Vec<Weak<dyn OrphanIo>>> = std::sync::Mutex::new(Vec::new());
/// Live entries of `ORPHANS` (checked without the lock by every blocked wait).
static ORPHAN_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Registers a backend no thread reaps; it stays registered until dropped.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn register_orphan(orphan: Weak<dyn OrphanIo>) {
    let mut o = ORPHANS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    o.retain(|w| w.strong_count() > 0);
    o.push(orphan);
    ORPHAN_COUNT.store(o.len(), std::sync::atomic::Ordering::Release);
}

/// Reaps every backend no thread reaps (application-owned io_uring's shared ring, #408),
/// waiting up to `wait` for something to complete: for a thread blocked on an outcome chained
/// after I/O it holds no completion of, such as a WAL sync waiting for the stream's older
/// syncs (ICR 0028), which would otherwise wait for ever. `None` when no such backend is
/// registered (one relaxed load: then wait as usual); otherwise whether anything completed.
///
/// ```
/// // With no reaper-less backend in the process, nothing to do.
/// assert_eq!(pigeonhole_io::reap_orphan_io(std::time::Duration::ZERO), None);
/// ```
pub fn reap_orphan_io(wait: Duration) -> Option<bool> {
    reap_orphans(wait)
}

/// What a blocked [`Completion::wait`](crate::Completion::wait) with no drive of its own does
/// while it waits (#408): reaps every registered orphan backend, so a completion chained after
/// their operations (a WAL's ordered sync, say) resolves without a reaper thread. `None` when
/// none is registered (wait as usual); otherwise whether anything completed.
pub(crate) fn reap_orphans(wait: Duration) -> Option<bool> {
    if ORPHAN_COUNT.load(std::sync::atomic::Ordering::Acquire) == 0 {
        return None;
    }
    let live: Vec<Arc<dyn OrphanIo>> = {
        let mut o = ORPHANS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        o.retain(|w| w.strong_count() > 0);
        ORPHAN_COUNT.store(o.len(), std::sync::atomic::Ordering::Release);
        o.iter().filter_map(Weak::upgrade).collect()
    };
    if live.is_empty() {
        return None;
    }
    let mut any = false;
    for orphan in live {
        any |= orphan.reap_orphan(wait);
    }
    Some(any)
}
