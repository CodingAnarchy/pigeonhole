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
    /// Every reader slot in the shared-memory region is taken.
    NoReaderSlot = 22,
    /// A commit is too large for one WAL record.
    RecordTooLarge = 23,
    /// Writes are stalled. A write that finds the memtable arena full waits while a flush
    /// frees room; this code means the wait outlasted the engine's stall timeout (30 s by
    /// default), a transient condition: back off and retry. It also means one batch is
    /// larger than a shard's arena, which never succeeds: split it or raise
    /// `Options::memtable_budget`.
    Busy = 24,
    /// A reader process's snapshot was taken before a writer restart, which may have reused
    /// the space it reads. Take a new snapshot and redo the read.
    SnapshotExpired = 25,
    /// A submitted commit's outcome was awaited on a thread that drives a shard
    /// (application-owned mode), where blocking could deadlock. The commit was submitted
    /// and will apply; poll its future from the event loop instead.
    WouldDeadlock = 26,
}

/// An error: a stable [`ErrorCode`] and a human-readable message.
///
/// Branch on [`Error::code`]; the message is for people and may change between releases.
///
/// ```
/// use pigeonhole::{ErrorCode, Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default())?;
/// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
/// let err = t.mutate(b"row").put("nope", b"q", b"v").commit().unwrap_err();
/// assert_eq!(err.code(), ErrorCode::FamilyNotFound);
/// assert!(err.message().contains("nope"));
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    code: ErrorCode,
    message: String,
}

impl Error {
    pub(crate) fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

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
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<pigeonhole_engine::Error> for Error {
    /// Every engine error maps to exactly one code; the message is the engine's.
    fn from(e: pigeonhole_engine::Error) -> Self {
        use pigeonhole_engine::Error as E;
        let code = match &e {
            E::Io(_) => ErrorCode::Io,
            E::Corruption(_) => ErrorCode::Corruption,
            E::WriterLocked => ErrorCode::WriterLocked,
            E::ShmVersionMismatch { .. } => ErrorCode::ShmVersionMismatch,
            E::ShmUnavailable => ErrorCode::ShmUnavailable,
            E::UnsupportedFormat(_) => ErrorCode::UnsupportedFormat,
            E::NetworkFilesystem => ErrorCode::NetworkFilesystem,
            E::TableNotFound(_) => ErrorCode::TableNotFound,
            E::TableExists(_) => ErrorCode::TableExists,
            E::FamilyNotFound(_) => ErrorCode::FamilyNotFound,
            E::FamilyExists(_) => ErrorCode::FamilyExists,
            E::UnknownMergeOperator(_) => ErrorCode::UnknownMergeOperator,
            E::Merge(_) => ErrorCode::MergeFailed,
            E::Conflict => ErrorCode::Conflict,
            E::ReadOnly => ErrorCode::ReadOnly,
            E::KeyTooLarge => ErrorCode::KeyTooLarge,
            E::ValueTooLarge => ErrorCode::ValueTooLarge,
            E::NoSpace => ErrorCode::NoSpace,
            E::InvalidArgument(_) => ErrorCode::InvalidArgument,
            E::Unsupported(_) => ErrorCode::Unsupported,
            E::Closed => ErrorCode::Closed,
            E::NoReaderSlot => ErrorCode::NoReaderSlot,
            E::RecordTooLarge => ErrorCode::RecordTooLarge,
            E::Busy => ErrorCode::Busy,
            E::SnapshotExpired => ErrorCode::SnapshotExpired,
            E::WouldDeadlock => ErrorCode::WouldDeadlock,
            // `engine::Error` is `#[non_exhaustive]`. A variant added there without a code
            // here surfaces as `Io` (the message names it) until it gets its own code; the
            // mapping test lists every current variant.
            _ => ErrorCode::Io,
        };
        let message = match &e {
            // Built from the fields: the message does not depend on the operator's `Display`.
            E::Merge(m) => format!("merge operator {:?} failed: {}", m.operator, m.message),
            // A write stall that outlasted the engine's timeout, or a batch larger than the
            // arena: transient unless the batch itself cannot fit.
            E::Busy => "memtable arena full: a flush did not free room within the write-stall \
                        timeout, or one batch is larger than the arena; retry, or raise \
                        Options::memtable_budget"
                .to_owned(),
            _ => e.to_string(),
        };
        Self::new(code, message)
    }
}
