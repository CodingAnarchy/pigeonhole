use std::fmt;

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A flat, stable error code. Values never change meaning and are never reused, so they map
/// one-to-one onto a future C enum and onto exceptions in any binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
#[non_exhaustive]
pub enum ErrorCode {
    /// An I/O failure.
    Io = 1,
    /// Stored data failed validation (checksum, structure).
    Corruption = 2,
    /// Another process holds the writer lock.
    WriterLocked = 3,
    /// A live shared-memory region has a different layout version.
    ShmVersionMismatch = 4,
    /// The shared-memory region could not be created at the configured size.
    ShmUnavailable = 5,
    /// The file's format version is not supported by this build.
    UnsupportedFormat = 6,
    /// The database is on a network filesystem.
    NetworkFilesystem = 7,
    /// No such table.
    TableNotFound = 8,
    /// The table already exists.
    TableExists = 9,
    /// No such family.
    FamilyNotFound = 10,
    /// The family already exists.
    FamilyExists = 11,
    /// A family names a merge operator this process has not registered.
    UnknownMergeOperator = 12,
    /// A merge operator failed.
    MergeFailed = 13,
    /// A transaction conflicted and was aborted.
    Conflict = 14,
    /// The handle or database is read-only.
    ReadOnly = 15,
    /// A row key or qualifier exceeds 64 KiB.
    KeyTooLarge = 16,
    /// A value exceeds the size limit.
    ValueTooLarge = 17,
    /// The device is full.
    NoSpace = 18,
    /// An argument is invalid.
    InvalidArgument = 19,
    /// The feature is not available in this build.
    Unsupported = 20,
    /// The database is closed.
    Closed = 21,
}

/// An error: a stable [`ErrorCode`] and a human-readable message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    code: ErrorCode,
    message: String,
}

impl Error {
    /// The stable code.
    pub fn code(&self) -> ErrorCode {
        self.code
    }

    /// The message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl std::error::Error for Error {}

impl From<pigeonhole_engine::Error> for Error {
    fn from(e: pigeonhole_engine::Error) -> Self {
        todo!()
    }
}
