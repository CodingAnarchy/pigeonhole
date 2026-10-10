//! `std` or `loom` synchronization primitives, so the loom tests check the real code.
//! (`loom` is a dev-dependency, so only the `--cfg loom` test build switches.)

#[cfg(all(loom, test))]
pub(crate) use loom::sync::atomic::{AtomicU8, AtomicU64};
#[cfg(all(loom, test))]
pub(crate) use loom::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::atomic::{AtomicU8, AtomicU64};
#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Locks a shard for changes (insert, eviction, removal), ignoring poison. A panic inside a
/// shard's critical section (in practice a debug-build arithmetic overflow in the usage
/// counters, which would be a bug) can leave that shard's accounting off, but never its
/// bytes: every entry is an `Arc` that stays valid, so a poisoned shard keeps serving correct
/// blocks rather than failing every read.
pub(crate) fn lock<T>(m: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    m.write().unwrap_or_else(|e| e.into_inner())
}

/// Locks a shard for a hit, shared with other hits (#18), ignoring poison as [`lock`] does.
pub(crate) fn read<T>(m: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    m.read().unwrap_or_else(|e| e.into_inner())
}
