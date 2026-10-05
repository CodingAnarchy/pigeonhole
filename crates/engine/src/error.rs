use std::fmt;

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Engine errors. Each variant maps to exactly one public `ErrorCode`.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// An I/O failure.
    Io(pigeonhole_io::Error),
    /// On-disk or shared-memory data failed validation.
    Corruption(String),
    /// Another process holds the writer lock.
    WriterLocked,
    /// A live shared-memory region has a different layout version.
    ShmVersionMismatch {
        /// Version in the region.
        found: u32,
        /// Version this build uses.
        expected: u32,
    },
    /// The shared-memory region could not be created at the configured size.
    ShmUnavailable,
    /// The file's format version is not readable by this build.
    UnsupportedFormat(u32),
    /// The database is on a network filesystem.
    NetworkFilesystem,
    /// No such table.
    TableNotFound(String),
    /// A table with that name exists.
    TableExists(String),
    /// No such family in the table.
    FamilyNotFound(String),
    /// A family with that name exists in the table.
    FamilyExists(String),
    /// A family names a merge operator this process has not registered.
    UnknownMergeOperator(String),
    /// A merge operator failed.
    Merge(pigeonhole_compaction::MergeError),
    /// An optimistic transaction conflicted and was aborted.
    Conflict,
    /// The handle is read-only.
    ReadOnly,
    /// A row key or qualifier exceeds 64 KiB.
    KeyTooLarge,
    /// A value exceeds the limit.
    ValueTooLarge,
    /// The device is full.
    NoSpace,
    /// An argument is invalid.
    InvalidArgument(String),
    /// The feature is not available in this build or phase.
    Unsupported(&'static str),
    /// The database is closed.
    Closed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl std::error::Error for Error {}

impl From<pigeonhole_io::Error> for Error {
    fn from(e: pigeonhole_io::Error) -> Self {
        todo!()
    }
}

impl From<pigeonhole_format::Error> for Error {
    fn from(e: pigeonhole_format::Error) -> Self {
        todo!()
    }
}

impl From<pigeonhole_pager::Error> for Error {
    fn from(e: pigeonhole_pager::Error) -> Self {
        todo!()
    }
}

impl From<pigeonhole_wal::Error> for Error {
    fn from(e: pigeonhole_wal::Error) -> Self {
        todo!()
    }
}

impl From<pigeonhole_memtable::Error> for Error {
    fn from(e: pigeonhole_memtable::Error) -> Self {
        todo!()
    }
}

impl From<pigeonhole_shm::Error> for Error {
    fn from(e: pigeonhole_shm::Error) -> Self {
        todo!()
    }
}

impl From<pigeonhole_sst::Error> for Error {
    fn from(e: pigeonhole_sst::Error) -> Self {
        todo!()
    }
}

impl From<pigeonhole_compaction::Error> for Error {
    fn from(e: pigeonhole_compaction::Error) -> Self {
        todo!()
    }
}

impl From<pigeonhole_compaction::MergeError> for Error {
    fn from(e: pigeonhole_compaction::MergeError) -> Self {
        Self::Merge(e)
    }
}

impl From<pigeonhole_runtime::Error> for Error {
    fn from(e: pigeonhole_runtime::Error) -> Self {
        todo!()
    }
}
