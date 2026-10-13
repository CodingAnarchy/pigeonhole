//! Whether an open database instance has failed, and why (ICR 0040).
//!
//! A shard that panics, whose application-owned driver is dropped before the close, or that
//! poisons itself (a WAL write or sync failed, or its state cannot be trusted) leaves the
//! instance unable to commit on that shard; a failed manifest commit (the main file) leaves
//! it unable to commit at all. Applications learn it here without waiting for a commit to
//! fail. The first failure is kept; later ones do not replace it. The types are plain data
//! (`repr(C)`, `repr(u8)`) for the C ABI.

use std::sync::atomic::{AtomicU64, Ordering};

/// Why the instance failed. The numbers are stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub enum FailureCause {
    /// A shard's thread panicked.
    Panic = 1,
    /// An application-owned shard's driver (`EngineShard`) was dropped before the close
    /// finished.
    DriverDropped = 2,
    /// A write or sync to the shard's WAL stream, or (for [`Failure::MAIN_FILE`]) a manifest
    /// commit to the main file, failed.
    Io = 3,
    /// The shard's in-memory state could not be updated consistently (a failed apply, view
    /// publication or memtable freeze): it stops rather than extend a state it cannot trust.
    Internal = 4,
}

/// The first failure of an instance (ICR 0040).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct Failure {
    /// The shard that failed, or [`Failure::MAIN_FILE`] when a manifest commit to the main
    /// file failed (no single shard).
    pub shard: u32,
    /// Why it failed.
    pub cause: FailureCause,
}

impl Failure {
    /// The `shard` of a main-file failure.
    pub const MAIN_FILE: u32 = u32::MAX;
}

/// The first failure recorded, packed into one word: 0 while healthy, else
/// `shard << 8 | cause`.
#[derive(Debug, Default)]
pub(crate) struct FailureRecord(AtomicU64);

impl FailureRecord {
    /// Records `cause` for `shard` unless a failure is already recorded.
    pub(crate) fn record(&self, shard: u32, cause: FailureCause) {
        let packed = (u64::from(shard) << 8) | u64::from(cause as u8);
        let _ = self
            .0
            .compare_exchange(0, packed, Ordering::AcqRel, Ordering::Acquire);
    }

    /// The first failure, if any.
    pub(crate) fn get(&self) -> Option<Failure> {
        let packed = self.0.load(Ordering::Acquire);
        if packed == 0 {
            return None;
        }
        let cause = match packed as u8 {
            1 => FailureCause::Panic,
            2 => FailureCause::DriverDropped,
            3 => FailureCause::Io,
            _ => FailureCause::Internal,
        };
        Some(Failure {
            shard: (packed >> 8) as u32,
            cause,
        })
    }
}
