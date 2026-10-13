//! Whether an open database instance has failed, and why (ICR 0040).
//!
//! A shard that panics, whose application-owned driver is dropped before the close, or that
//! poisons itself (a WAL write or sync failed, or its state cannot be trusted) leaves the
//! instance unable to commit on that shard; a failed manifest commit (the main file) leaves
//! it unable to commit at all. Applications learn it here without waiting for a commit to
//! fail. The first failure is kept; later ones do not replace it. The types are plain data
//! (`repr(C)`, `repr(u8)`) for the C ABI.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, PoisonError};
use std::time::Instant;

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
/// `shard << 8 | cause`, and a wake for the threads that wait for one (#646).
#[derive(Debug, Default)]
pub(crate) struct FailureRecord {
    first: AtomicU64,
    /// Pairs a waiter's look with [`FailureRecord::notify`], so a wake can't fall between
    /// the look and the wait. Guards no data, and nothing is locked while it is held.
    lock: Mutex<()>,
    changed: Condvar,
}

impl FailureRecord {
    /// Records `cause` for `shard` unless a failure is already recorded, and wakes the
    /// waiters. Runs while a shard's thread unwinds, so it must not panic: it takes only
    /// [`FailureRecord::lock`], which a holder never panics under.
    pub(crate) fn record(&self, shard: u32, cause: FailureCause) {
        let packed = (u64::from(shard) << 8) | u64::from(cause as u8);
        if self
            .first
            .compare_exchange(0, packed, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.notify();
        }
    }

    /// Wakes the threads in [`FailureRecord::wait`] to look again: after a failure is
    /// recorded, or once the instance's close has finished (`Shared::closed`).
    pub(crate) fn notify(&self) {
        drop(self.lock.lock().unwrap_or_else(PoisonError::into_inner));
        self.changed.notify_all();
    }

    /// Waits until a failure is recorded (`true`), or until `stop` holds or `deadline`
    /// passes (`false`). `stop` must turn true only before a [`FailureRecord::notify`].
    pub(crate) fn wait(&self, deadline: Instant, stop: impl Fn() -> bool) -> bool {
        let mut guard = self.lock.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if self.get().is_some() {
                return true;
            }
            let now = Instant::now();
            if stop() || now >= deadline {
                return false;
            }
            guard = self
                .changed
                .wait_timeout(guard, deadline - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// The first failure, if any.
    pub(crate) fn get(&self) -> Option<Failure> {
        let packed = self.first.load(Ordering::Acquire);
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use super::*;

    /// Far past any test's run: a wait that returns before it was woken, not timed out.
    const LONG: Duration = Duration::from_secs(60);

    #[test]
    fn a_waiter_wakes_when_a_failure_is_recorded() {
        let record = Arc::new(FailureRecord::default());
        let recorder = {
            let record = Arc::clone(&record);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                record.record(3, FailureCause::Panic);
            })
        };
        let start = Instant::now();
        assert!(record.wait(start + LONG, || false));
        assert!(start.elapsed() < LONG / 2, "woken, not timed out");
        assert_eq!(
            record.get(),
            Some(Failure {
                shard: 3,
                cause: FailureCause::Panic
            })
        );
        recorder.join().unwrap();
    }

    #[test]
    fn a_waiter_wakes_when_its_stop_condition_is_notified() {
        let record = Arc::new(FailureRecord::default());
        let stop = Arc::new(AtomicBool::new(false));
        let closer = {
            let (record, stop) = (Arc::clone(&record), Arc::clone(&stop));
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                stop.store(true, Ordering::Release);
                record.notify();
            })
        };
        let start = Instant::now();
        assert!(!record.wait(start + LONG, || stop.load(Ordering::Acquire)));
        assert!(start.elapsed() < LONG / 2, "woken, not timed out");
        closer.join().unwrap();
    }

    #[test]
    fn a_wait_with_nothing_to_wake_it_ends_at_its_deadline() {
        let record = FailureRecord::default();
        let start = Instant::now();
        assert!(!record.wait(start + Duration::from_millis(20), || false));
        assert!(start.elapsed() >= Duration::from_millis(20));
        // A failure recorded before the wait is seen at once.
        record.record(Failure::MAIN_FILE, FailureCause::Io);
        assert!(record.wait(Instant::now(), || false));
    }
}
