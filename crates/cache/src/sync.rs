//! `std` or `loom` synchronization primitives, so the loom tests check the real code.
//! (`loom` is a dev-dependency, so only the `--cfg loom` test build switches.)

#[cfg(all(loom, test))]
pub(crate) use loom::sync::{Arc, Mutex, MutexGuard};
#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::{Arc, Mutex, MutexGuard};

/// Locks `m`, ignoring poison. A panic inside a shard's critical section (in practice a
/// debug-build arithmetic overflow in the usage counters, which would be a bug) can leave
/// that shard's accounting off, but never its bytes: every entry is an `Arc` that stays valid,
/// so a poisoned shard keeps serving correct blocks rather than failing every read.
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
