//! A database's device bytes (D332, ICR 0142): the bytes its files read and write, counted in
//! each backend's [`File`](crate::File) at its read and write entry points, once a counter
//! set is given to the file ([`File::count_into`](crate::File::count_into)).
//!
//! Counted inside the backend, not by a wrapping `File`: a wrapper put an extra dynamic call
//! on every I/O, the WAL's included, and cost the commit paths about 50 instructions (#2063's
//! callgrind run). Here an I/O costs a load, a branch and an add.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// One counter on a cache line of its own.
#[derive(Debug, Default)]
#[repr(align(64))]
struct Line(AtomicU64);

/// One database's bytes read and written by its files since the counters were given to them.
///
/// Two plain counters (perf287 on #2063): a data read reaches a file only on a cache miss,
/// and WAL writes are mostly the group-commit leader's, so a shared line costs nothing
/// measurable next to the I/O itself.
#[derive(Debug, Default)]
pub struct IoCounters {
    read: Line,
    written: Line,
}

impl IoCounters {
    /// Counters at 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bytes read and written so far.
    pub fn totals(&self) -> (u64, u64) {
        (
            self.read.0.load(Ordering::Relaxed),
            self.written.0.load(Ordering::Relaxed),
        )
    }
}

/// A backend file's counters: empty until [`File::count_into`](crate::File::count_into).
#[derive(Debug, Default)]
pub(crate) struct FileCounts(OnceLock<Arc<IoCounters>>);

impl FileCounts {
    /// Counts into `counters` from now on. A second call with the same counters does nothing;
    /// with other counters it's a bug (a file handed to a second database), caught in debug
    /// builds and ignored otherwise.
    pub(crate) fn set(&self, counters: Arc<IoCounters>) {
        if let Err(other) = self.0.set(counters) {
            debug_assert!(
                self.0.get().is_some_and(|c| Arc::ptr_eq(c, &other)),
                "a file's I/O counters were given twice, to different databases"
            );
        }
    }

    /// A read of `bytes`, at its submission: its length asked for, so a short or failed read
    /// counts in full.
    #[inline]
    pub(crate) fn read(&self, bytes: usize) {
        if let Some(c) = self.0.get() {
            c.read.0.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    /// A write of `bytes`, at its submission.
    #[inline]
    pub(crate) fn written(&self, bytes: usize) {
        if let Some(c) = self.0.get() {
            c.written.0.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }
}
