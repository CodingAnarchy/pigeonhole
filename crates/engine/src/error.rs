use std::fmt;

use pigeonhole_io::ErrorKind;

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An `Io` error carrying a dynamic message (a background failure reported to a caller).
pub(crate) fn io_other(context: &'static str, message: impl Into<String>) -> Error {
    Error::Io(pigeonhole_io::Error::os(
        context,
        std::io::Error::other(message.into()),
    ))
}

/// Engine errors. Each variant maps to exactly one public `ErrorCode`.
///
/// ```
/// use pigeonhole_engine::Error;
///
/// let e: Error = pigeonhole_format::Error::KeyTooLarge.into();
/// assert!(matches!(e, Error::KeyTooLarge));
/// assert_eq!(e.to_string(), "row key or qualifier exceeds 64 KiB");
/// ```
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
    /// Every reader slot in the shared-memory region is taken.
    NoReaderSlot,
    /// A commit's WAL record is larger than a WAL segment can hold.
    RecordTooLarge,
    /// Writes are stalled (L0 too deep, or the memtable arena full) and the caller asked not
    /// to wait.
    Busy,
    /// A reader process's snapshot was taken before a writer restart: the new writer may
    /// have reused the space it names. Take a new snapshot.
    SnapshotExpired,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Corruption(what) => write!(f, "corruption: {what}"),
            Error::WriterLocked => f.write_str("another process holds the writer lock"),
            Error::ShmVersionMismatch { found, expected } => write!(
                f,
                "shared-memory layout version {found} differs from this build's {expected}"
            ),
            Error::ShmUnavailable => {
                f.write_str("the shared-memory region could not be created at the configured size")
            }
            Error::UnsupportedFormat(v) => write!(f, "unsupported file format version {v}"),
            Error::NetworkFilesystem => f.write_str("the database is on a network filesystem"),
            Error::TableNotFound(t) => write!(f, "no such table {t:?}"),
            Error::TableExists(t) => write!(f, "table {t:?} already exists"),
            Error::FamilyNotFound(x) => write!(f, "no such family {x:?}"),
            Error::FamilyExists(x) => write!(f, "family {x:?} already exists"),
            Error::UnknownMergeOperator(op) => {
                write!(f, "merge operator {op:?} is not registered in this process")
            }
            Error::Merge(e) => write!(f, "merge failed: {e}"),
            Error::Conflict => f.write_str("the transaction conflicted with a later commit"),
            Error::ReadOnly => f.write_str("the handle is read-only"),
            Error::KeyTooLarge => f.write_str("row key or qualifier exceeds 64 KiB"),
            Error::ValueTooLarge => f.write_str("value exceeds the size limit"),
            Error::NoSpace => f.write_str("no space left on device"),
            Error::InvalidArgument(what) => write!(f, "invalid argument: {what}"),
            Error::Unsupported(what) => write!(f, "unsupported: {what}"),
            Error::Closed => f.write_str("the database is closed"),
            Error::NoReaderSlot => f.write_str("every reader slot is taken"),
            Error::RecordTooLarge => {
                f.write_str("the commit's WAL record is larger than a segment can hold")
            }
            Error::Busy => f.write_str(
                "writes stalled past the write-stall timeout (transient: retry later), or a \
                 batch larger than the arena (never fits: split it or raise memtable_budget)",
            ),
            Error::SnapshotExpired => f.write_str(
                "the snapshot was taken before a writer restart and can no longer be read; \
                 take a new snapshot",
            ),
        }
    }
}

impl Error {
    /// An equal error (same variant and message) for a second reader of one outcome (the
    /// final close's, issue #135). The I/O source is carried as its kind and text.
    pub(crate) fn duplicate(&self) -> Error {
        match self {
            Error::Io(e) => Error::Io(pigeonhole_io::Error {
                kind: e.kind,
                context: e.context,
                source: e
                    .source
                    .as_ref()
                    .map(|s| std::io::Error::new(s.kind(), s.to_string())),
            }),
            Error::Corruption(s) => Error::Corruption(s.clone()),
            Error::WriterLocked => Error::WriterLocked,
            Error::ShmVersionMismatch { found, expected } => Error::ShmVersionMismatch {
                found: *found,
                expected: *expected,
            },
            Error::ShmUnavailable => Error::ShmUnavailable,
            Error::UnsupportedFormat(v) => Error::UnsupportedFormat(*v),
            Error::NetworkFilesystem => Error::NetworkFilesystem,
            Error::TableNotFound(s) => Error::TableNotFound(s.clone()),
            Error::TableExists(s) => Error::TableExists(s.clone()),
            Error::FamilyNotFound(s) => Error::FamilyNotFound(s.clone()),
            Error::FamilyExists(s) => Error::FamilyExists(s.clone()),
            Error::UnknownMergeOperator(s) => Error::UnknownMergeOperator(s.clone()),
            Error::Merge(e) => Error::Merge(e.clone()),
            Error::Conflict => Error::Conflict,
            Error::ReadOnly => Error::ReadOnly,
            Error::KeyTooLarge => Error::KeyTooLarge,
            Error::ValueTooLarge => Error::ValueTooLarge,
            Error::NoSpace => Error::NoSpace,
            Error::InvalidArgument(s) => Error::InvalidArgument(s.clone()),
            Error::Unsupported(s) => Error::Unsupported(s),
            Error::Closed => Error::Closed,
            Error::NoReaderSlot => Error::NoReaderSlot,
            Error::RecordTooLarge => Error::RecordTooLarge,
            Error::Busy => Error::Busy,
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Merge(e) => Some(e),
            _ => None,
        }
    }
}

impl From<pigeonhole_io::Error> for Error {
    fn from(e: pigeonhole_io::Error) -> Self {
        match e.kind {
            ErrorKind::NoSpace => Error::NoSpace,
            _ => Error::Io(e),
        }
    }
}

impl From<pigeonhole_format::Error> for Error {
    /// Every variant is mapped explicitly (ICR 0001, issue #14). `format::Error` is
    /// `#[non_exhaustive]`, so the compiler insists on a wildcard arm for variants that do not
    /// exist yet; it is unreachable today and maps to `Corruption`, the conservative choice.
    fn from(e: pigeonhole_format::Error) -> Self {
        use pigeonhole_format::Error as F;
        match e {
            F::Truncated { what }
            | F::BadMagic { what }
            | F::Checksum { what }
            | F::Corrupt { what } => Error::Corruption(what.to_owned()),
            F::UnsupportedVersion { found, .. } => Error::UnsupportedFormat(found),
            F::KeyTooLarge => Error::KeyTooLarge,
            F::ValueTooLarge => Error::ValueTooLarge,
            F::UnsupportedCompression(c) => {
                Error::Corruption(format!("unsupported compression codec {c}"))
            }
            F::InvalidArgument { what } => Error::InvalidArgument(what.to_owned()),
            _ => Error::Corruption(format!("format: {e}")),
        }
    }
}

impl From<pigeonhole_pager::Error> for Error {
    fn from(e: pigeonhole_pager::Error) -> Self {
        use pigeonhole_pager::Error as P;
        match e {
            P::Io(e) => e.into(),
            P::Format(e) => e.into(),
            P::NoSpace => Error::NoSpace,
            P::TooLarge => Error::ValueTooLarge,
            P::UnsupportedVersion(v) => Error::UnsupportedFormat(v),
            _ => Error::Corruption(format!("pager: {e}")),
        }
    }
}

impl From<pigeonhole_wal::Error> for Error {
    fn from(e: pigeonhole_wal::Error) -> Self {
        use pigeonhole_wal::Error as W;
        match e {
            W::Io(e) => e.into(),
            W::Format(e) => e.into(),
            W::ForeignSegment => {
                Error::Corruption("WAL segment belongs to another database".to_owned())
            }
            W::RecordTooLarge => Error::RecordTooLarge,
            W::Poisoned => Error::Io(pigeonhole_io::Error::new(
                ErrorKind::Other,
                "WAL stream poisoned by an earlier write or sync failure; reopen the database",
            )),
            W::InvalidArgument { what } => Error::InvalidArgument(what.to_owned()),
            _ => Error::Corruption(format!("wal: {e}")),
        }
    }
}

impl From<pigeonhole_memtable::Error> for Error {
    fn from(e: pigeonhole_memtable::Error) -> Self {
        use pigeonhole_memtable::Error as M;
        match e {
            M::ArenaFull => Error::Busy,
            M::EntryTooLarge => Error::ValueTooLarge,
            M::Corrupt(what) => Error::Corruption(format!("memtable: {what}")),
            _ => Error::Corruption(format!("memtable: {e}")),
        }
    }
}

impl From<pigeonhole_shm::Error> for Error {
    fn from(e: pigeonhole_shm::Error) -> Self {
        use pigeonhole_shm::Error as S;
        match e {
            S::Io(e) => e.into(),
            S::WriterLocked => Error::WriterLocked,
            S::VersionMismatch { found, expected } => Error::ShmVersionMismatch { found, expected },
            S::Corrupt(what) => Error::Corruption(format!("shared memory: {what}")),
            S::NoReaderSlot => Error::NoReaderSlot,
            S::Unavailable => Error::ShmUnavailable,
            S::ViewTooLarge { needed, capacity } => Error::InvalidArgument(format!(
                "the view ({needed} bytes) does not fit the shared-memory view buffer ({capacity} bytes)"
            )),
            S::Stale => Error::Io(pigeonhole_io::Error::new(
                ErrorKind::Other,
                "shared-memory mapping is stale; take a new snapshot",
            )),
            S::ViewVersionNotNewer { .. } => {
                Error::Corruption("view version published out of order".to_owned())
            }
            S::InvalidConfig(what) => Error::InvalidArgument(what.to_owned()),
            _ => Error::Corruption(format!("shared memory: {e}")),
        }
    }
}

impl From<pigeonhole_sst::Error> for Error {
    fn from(e: pigeonhole_sst::Error) -> Self {
        use pigeonhole_sst::Error as S;
        match e {
            S::Io(e) => e.into(),
            S::Format(e) => e.into(),
            _ => Error::Corruption(format!("sst: {e:?}")),
        }
    }
}

impl From<pigeonhole_compaction::Error> for Error {
    fn from(e: pigeonhole_compaction::Error) -> Self {
        use pigeonhole_compaction::Error as C;
        match e {
            C::Sst(e) => e.into(),
            C::Pager(e) => e.into(),
            C::Merge(e) => Error::Merge(e),
            _ => Error::Corruption(format!("compaction: {e:?}")),
        }
    }
}

impl From<pigeonhole_compaction::MergeError> for Error {
    fn from(e: pigeonhole_compaction::MergeError) -> Self {
        Self::Merge(e)
    }
}

impl From<pigeonhole_runtime::Error> for Error {
    fn from(e: pigeonhole_runtime::Error) -> Self {
        use pigeonhole_runtime::Error as R;
        match e {
            R::Closed => Error::Closed,
            R::Spawn(e) => Error::Io(pigeonhole_io::Error::os("start shard thread", e)),
            R::InvalidConfig(what) => Error::InvalidArgument(what.to_owned()),
            _ => Error::InvalidArgument(format!("runtime: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_format_variant_maps_to_one_code() {
        use pigeonhole_format::Error as F;
        type Check = fn(&Error) -> bool;
        let cases: Vec<(F, Check)> = vec![
            (F::Truncated { what: "x" }, |e| {
                matches!(e, Error::Corruption(_))
            }),
            (F::BadMagic { what: "x" }, |e| {
                matches!(e, Error::Corruption(_))
            }),
            (F::Checksum { what: "x" }, |e| {
                matches!(e, Error::Corruption(_))
            }),
            (F::Corrupt { what: "x" }, |e| {
                matches!(e, Error::Corruption(_))
            }),
            (
                F::UnsupportedVersion {
                    what: "x",
                    found: 9,
                },
                |e| matches!(e, Error::UnsupportedFormat(9)),
            ),
            (F::KeyTooLarge, |e| matches!(e, Error::KeyTooLarge)),
            (F::ValueTooLarge, |e| matches!(e, Error::ValueTooLarge)),
            (F::UnsupportedCompression(7), |e| {
                matches!(e, Error::Corruption(_))
            }),
            (F::InvalidArgument { what: "x" }, |e| {
                matches!(e, Error::InvalidArgument(_))
            }),
        ];
        for (f, check) in cases {
            let e: Error = f.clone().into();
            assert!(check(&e), "{f:?} mapped to {e:?}");
        }
        // Code 19 is InvalidArgument in the public ErrorCode (ICR 0001, issue #14).
        let e: Error = F::InvalidArgument { what: "misuse" }.into();
        assert_eq!(e.to_string(), "invalid argument: misuse");
    }

    #[test]
    fn io_no_space_is_no_space() {
        let e: Error = pigeonhole_io::Error::new(ErrorKind::NoSpace, "write").into();
        assert!(matches!(e, Error::NoSpace));
        let e: Error = pigeonhole_io::Error::new(ErrorKind::Other, "write").into();
        assert!(matches!(e, Error::Io(_)));
    }

    #[test]
    fn memtable_arena_full_is_busy() {
        let e: Error = pigeonhole_memtable::Error::ArenaFull.into();
        assert!(matches!(e, Error::Busy));
    }
}
