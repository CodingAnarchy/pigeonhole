//! `std` or `loom` synchronization primitives, so the loom tests check the real code.
//! (`loom` is a dev-dependency, so only the `--cfg loom` test build switches.)

#[cfg(all(loom, test))]
pub(crate) use loom::sync::{Arc, Mutex, MutexGuard};
#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::{Arc, Mutex, MutexGuard};

/// Locks `m`, ignoring poison: every critical section leaves the shard consistent before it
/// can panic (panics only come from allocation failure inside `HashMap`/`VecDeque` growth).
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
