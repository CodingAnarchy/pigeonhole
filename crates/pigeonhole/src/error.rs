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
    /// The feature is not available in this build, or the call is not supported here: a
    /// reader used in a process forked from the one that opened it (#693, ICR 0065).
    Unsupported = 20,
    /// The database is closed, or a shard failed and the database now refuses every new
    /// commit (reads still work; reopen it). A commit that fails with it was not committed
    /// (one whose outcome is not known fails with [`ErrorCode::OutcomeUnknown`]).
    Closed = 21,
    /// Every reader slot in the shared-memory region is taken.
    NoReaderSlot = 22,
    /// A commit is too large for one WAL record.
    RecordTooLarge = 23,
    /// A write, `flush` or `compact` waited past
    /// [`Options::write_stall_timeout`](crate::Options::write_stall_timeout) (30 s by
    /// default): for memtable room, counted from when that call began waiting, or for a
    /// tablet split, merge or move, counted from when the commit was submitted. A commit
    /// that fails with it was not committed. Transient: back off and retry. A batch that can
    /// never fit gets [`ErrorCode::BatchTooLarge`] instead. A reader process's `snapshot()`
    /// also returns it when the writer has not published a manifest change for about a
    /// second.
    Busy = 24,
    /// A reader process's snapshot was taken before a writer restart, which may have reused
    /// the space it reads. Take a new snapshot and redo the read.
    SnapshotExpired = 25,
    /// A submitted commit's outcome was awaited on a thread that drives a shard
    /// (application-owned mode), where blocking could deadlock. The commit was submitted
    /// and will apply; poll its future from the event loop instead.
    WouldDeadlock = 26,
    /// A batch can never fit a shard's memtable arena, even an empty one: its cells need
    /// more than about half of `Options::memtable_budget`. Retrying never succeeds: split
    /// the batch, or raise the budget.
    BatchTooLarge = 27,
    /// A commit's outcome is not known in this process: a shard failed or the database
    /// closed while the commit was in flight, after its records may have reached the log.
    /// The next open decides it: reopen and read to see whether it landed. `Closed`, by
    /// contrast, means not committed.
    OutcomeUnknown = 28,
    /// A reader process found no running writer to attach to
    /// ([`Pigeonhole::open_reader`](crate::Pigeonhole::open_reader), or a reader's next read
    /// after its writer's shared memory went away): a reader attaches to a running writer.
    /// Open the reader again once a writer runs (ICR 0057).
    NoWriter = 29,
    /// The file uses a feature this build does not support: a value written by a newer
    /// version (a value type, family kind, compression codec, block or log record kind) that
    /// this one does not know. The message names it. Open the file with a newer version;
    /// the data is intact, not corrupt (#496, ICR 0058). [`ErrorCode::UnsupportedFormat`], by
    /// contrast, means the whole file's format version is unreadable.
    UnsupportedFeature = 30,
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
#[derive(Clone)]
pub struct Error {
    code: ErrorCode,
    message: String,
    /// A commit the failed engine refused before logging anything (the engine's
    /// `ShardFailed`, code `Io`): with `Options::auto_recover` covering the failure it is
    /// treated as a `Closed` refusal (#921, ICR 0060). Not part of equality or `Debug`.
    unlogged: bool,
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code && self.message == other.message
    }
}

impl Eq for Error {}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("code", &self.code)
            .field("message", &self.message)
            .finish()
    }
}

impl Error {
    pub(crate) fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            unlogged: false,
        }
    }

    /// Whether a commit got this from a failed engine that refused it before logging it
    /// (see the `unlogged` field).
    pub(crate) fn refused_unlogged(&self) -> bool {
        self.unlogged
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
            E::ReadOnly | E::ReadOnlyOpen(_) => ErrorCode::ReadOnly,
            E::KeyTooLarge => ErrorCode::KeyTooLarge,
            E::ValueTooLarge => ErrorCode::ValueTooLarge,
            E::NoSpace => ErrorCode::NoSpace,
            E::InvalidArgument(_) => ErrorCode::InvalidArgument,
            E::Unsupported(_) => ErrorCode::Unsupported,
            E::Closed => ErrorCode::Closed,
            E::NoReaderSlot => ErrorCode::NoReaderSlot,
            E::RecordTooLarge => ErrorCode::RecordTooLarge,
            E::Busy => ErrorCode::Busy,
            E::BatchTooLarge => ErrorCode::BatchTooLarge,
            E::SnapshotExpired => ErrorCode::SnapshotExpired,
            E::WouldDeadlock => ErrorCode::WouldDeadlock,
            E::OutcomeUnknown => ErrorCode::OutcomeUnknown,
            // A failed shard's refusal of a commit it never logged: still `Io` to the caller.
            E::ShardFailed => ErrorCode::Io,
            E::NoWriter => ErrorCode::NoWriter,
            E::UnsupportedFeature(_) => ErrorCode::UnsupportedFeature,
            // `engine::Error` is `#[non_exhaustive]`. A variant added there without a code
            // here surfaces as `Io` (the message names it) until it gets its own code; the
            // mapping test lists every current variant.
            _ => ErrorCode::Io,
        };
        let message = match &e {
            // Built from the fields: the message does not depend on the operator's `Display`.
            E::Merge(m) => format!("merge operator {:?} failed: {}", m.operator, m.message),
            // A write stall that outlasted the engine's timeout: transient.
            E::Busy => {
                "stalled past the write-stall timeout (Options::write_stall_timeout, 30 s by \
                        default): no flush or compaction freed room in time, or a tablet split \
                        or move kept the commit waiting; back off and retry, and drop old \
                        snapshots"
                    .to_owned()
            }
            E::BatchTooLarge => "the batch can never fit a shard's memtable arena (about half \
                                 of Options::memtable_budget); split it or raise the budget"
                .to_owned(),
            _ => e.to_string(),
        };
        let mut out = Self::new(code, message);
        out.unlogged = matches!(e, E::ShardFailed);
        out
    }
}
