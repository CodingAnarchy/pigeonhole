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
    /// finish when none has. Returns whether any completed.
    fn reap_here(&self, wait: Option<Duration>) -> bool;
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
/// continuations, which may submit and register again).
fn backends() -> Vec<Arc<dyn OwnIo>> {
    OWN.try_with(|o| o.borrow().iter().filter_map(Weak::upgrade).collect())
        .unwrap_or_default()
}

/// Whether the calling thread has submitted operations in flight that only it can complete
/// (by [`reap_own_io`]). Always `false` on backends whose completions arrive on other
/// threads, such as [`PreadVfs`](crate::pread::PreadVfs).
pub fn own_io_in_flight() -> bool {
    backends().iter().any(|b| b.in_flight_here())
}

/// Completes the calling thread's finished operations on every backend that leaves them to
/// it, waiting up to `wait` for one to finish when none has (`None`: do not wait). Their
/// completions resolve, and their continuations run, on this thread. Returns whether any
/// completed.
pub fn reap_own_io(wait: Option<Duration>) -> bool {
    let mut any = false;
    for b in backends() {
        any |= b.reap_here(wait);
    }
    any
}
