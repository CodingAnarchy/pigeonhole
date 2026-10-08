//! Compaction pickers, jobs, merge operators and GC for Pigeonhole.
//!
//! - [`CompactionPicker`] chooses work for one `(tablet, family)` from its [`Levels`].
//! - [`CompactionJob`] runs it as a cooperative, time-sliced job: merges inputs, applies
//!   version/TTL/tombstone GC that preserves every live snapshot, resolves merge operands,
//!   separates large values into blob files, and returns a [`CompactionOutput`] the engine
//!   turns into manifest edits.
//! - [`MergingCursor`], [`FilteredCursor`] and [`CellResolver`] are the shared read machinery: the engine's read
//!   path runs the same resolver over memtables and SSTs, so reads and compaction can never
//!   disagree about visibility.
//! - [`MergeOperator`]s are identified by name in the file.
//!
//! Scheduling is not here: the engine decides when, the runtime decides where.
//!
//! **Scope.** Leveled and tiered picking (FIFO-by-time returns no work yet), no
//! value separation into blob files and no blob GC (the output types carry them for
//! Phase 2), and merge operands are combined only within one `(column, timestamp)`.
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]

mod cursor;
mod gc;
mod job;
mod merge;
mod picker;
mod resolver;

use std::fmt;

pub use cursor::{FilteredCursor, MergingCursor, VecCursor};
pub use job::{CompactionJob, CompactionOutput, GcPolicy, JobContext, JobPoll, NewBlobFile};
pub use merge::{I64Add, MergeError, MergeOperator, MergeRegistry};
pub use picker::{CompactionPicker, CompactionTask, KeyRange, Levels, PickerOptions, TaskKind};
pub use resolver::{CellResolver, ResolveOptions, ResolvedCell, ValuePredicate};

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Compaction errors.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Reading or writing an SST failed.
    Sst(pigeonhole_sst::Error),
    /// Allocating an extent failed.
    Pager(pigeonhole_pager::Error),
    /// A merge operator failed.
    Merge(MergeError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sst(e) => write!(f, "compaction: {e}"),
            Self::Pager(e) => write!(f, "compaction: {e}"),
            Self::Merge(e) => write!(f, "compaction: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sst(e) => Some(e),
            Self::Pager(e) => Some(e),
            Self::Merge(e) => Some(e),
        }
    }
}

impl From<pigeonhole_sst::Error> for Error {
    fn from(e: pigeonhole_sst::Error) -> Self {
        Self::Sst(e)
    }
}

impl From<pigeonhole_pager::Error> for Error {
    fn from(e: pigeonhole_pager::Error) -> Self {
        Self::Pager(e)
    }
}

impl From<MergeError> for Error {
    fn from(e: MergeError) -> Self {
        Self::Merge(e)
    }
}
