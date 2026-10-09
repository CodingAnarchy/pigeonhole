//! Per-shard write-ahead log streams for Pigeonhole: recycled segments, group commit, 2PC
//! records.
//!
//! One [`Wal`] per shard. The shard thread is the only user of its stream, so the trait takes
//! `&mut self` and needs no locks. Group commit is the shard loop calling
//! [`Wal::append`] for every commit it drained, then [`Wal::write`] once and, if any member
//! asked for `GroupSync`, [`Wal::submit_sync`] once. The sync runs on the I/O backend while
//! the shard builds the next group; its committers resolve when the completion does.
//!
//! Segment rules (FORMAT §10, decision D25): when a segment fills, the writer syncs it before
//! writing the next segment's header, whose `prev_end` records where it ended. After
//! recovery the writer never appends to the last replayed segment: it starts a new one with
//! an epoch above every epoch in any header, chained to the recovered end.
//!
//! Implementations: [`WalStream`] (sidecar files `data.phdb-wal-N`, preallocated recycled
//! segments) and [`MemWal`] (the in-memory mock for engine tests). An in-file ring can be
//! added later behind the same trait.
//!
//! ```
//! use std::path::Path;
//! use pigeonhole_format::wal::{BatchBuilder, WalRecord};
//! use pigeonhole_format::{Durability, FamilyId, Kind, StreamId, TableId};
//! use pigeonhole_io::sim::SimVfs;
//! use pigeonhole_wal::{Recovery, Wal, WalOptions, WalStream};
//!
//! # fn main() -> pigeonhole_wal::Result<()> {
//! let vfs: pigeonhole_io::VfsRef = SimVfs::new(1);
//! let db = Path::new("/db/data.phdb");
//! let mut opts = WalOptions::default();
//! opts.segment_size = 4 * 32 * 1024; // tiny segments for the example
//!
//! // A shard's group commit: append every member, one write, one submitted sync.
//! let mut wal = WalStream::create(&vfs, db, StreamId(0), [7; 16], opts)?;
//! let mut batch = BatchBuilder::new();
//! batch.push(TableId(1), FamilyId(1), Kind::Put, b"row", b"q", None, b"\x00v").unwrap();
//! let rec = WalRecord::Batch { seqno: 1, commit_ts: 10, batch: batch.batch() };
//! let ticket = wal.append(&rec, Durability::GroupSync)?;
//! wal.write()?;
//! assert!(!wal.satisfies(&ticket));
//! wal.submit_sync()?.wait()?;
//! assert!(wal.satisfies(&ticket));
//! drop(wal);
//!
//! // Recovery replays from the manifest's checkpoint and never appends to the torn segment.
//! let mut rec = Recovery::open(&vfs, db, StreamId(0), [7; 16], Default::default())?;
//! let (end, record) = rec.next_record()?.expect("one record");
//! assert_eq!(end, ticket.end);
//! assert!(matches!(record, WalRecord::Batch { seqno: 1, .. }));
//! assert!(rec.next_record()?.is_none());
//! assert_eq!(rec.max_seqno(), 1);
//! let wal = rec.into_stream(opts)?;
//! assert!(wal.written().epoch() > ticket.end.epoch());
//! # Ok(())
//! # }
//! ```
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]

mod mem;
mod recovery;
mod stream;

use std::fmt;
use std::path::{Path, PathBuf};

use pigeonhole_format::wal::WalRecord;
use pigeonhole_format::{Durability, Lsn, StreamId};
use pigeonhole_io::{Completion, VfsRef};

pub use mem::MemWal;
pub use recovery::Recovery;
pub use stream::{SpareSegments, WalCounters, WalStream};

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// WAL errors.
///
/// ```
/// use pigeonhole_wal::Error;
///
/// let e: Error = pigeonhole_format::Error::Corrupt { what: "wal segment chain" }.into();
/// assert!(matches!(e, Error::Format(_)));
/// assert_eq!(e.to_string(), "wal: wal segment chain: corrupt");
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The underlying file failed.
    Io(pigeonhole_io::Error),
    /// A segment header or record failed to decode (outside the torn tail).
    Format(pigeonhole_format::Error),
    /// A segment belongs to another database (db id mismatch).
    ForeignSegment,
    /// The record is larger than a segment can hold.
    RecordTooLarge,
    /// A write or sync failed earlier; the stream refuses further appends, writes and syncs
    /// until it is reopened through [`Recovery`] (see [`Wal`]).
    Poisoned,
    /// A caller error: an option out of range.
    InvalidArgument {
        /// What was wrong.
        what: &'static str,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "wal: {e}"),
            Error::Format(e) => write!(f, "wal: {e}"),
            Error::ForeignSegment => f.write_str("wal segment belongs to another database"),
            Error::RecordTooLarge => f.write_str("wal record larger than a segment can hold"),
            Error::Poisoned => {
                f.write_str("wal stream poisoned by an earlier write or sync failure")
            }
            Error::InvalidArgument { what } => write!(f, "wal: invalid argument: {what}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Format(e) => Some(e),
            Error::ForeignSegment
            | Error::RecordTooLarge
            | Error::Poisoned
            | Error::InvalidArgument { .. } => None,
        }
    }
}

impl From<pigeonhole_io::Error> for Error {
    fn from(e: pigeonhole_io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<pigeonhole_format::Error> for Error {
    fn from(e: pigeonhole_format::Error) -> Self {
        Self::Format(e)
    }
}

/// Stream configuration.
///
/// `segment_size` applies when a stream file is created; an existing file keeps the slot size
/// its headers record. Invalid sizes (not a multiple of 32 KiB, under two frames, or above
/// 4 GiB minus one frame so `prev_end` always fits) are refused with
/// [`Error::InvalidArgument`]. `spare_segments` is the number of free slots
/// [`SpareSegments::prepare`] keeps ready ahead of the writer (its
/// [`target`](SpareSegments::target)).
///
/// ```
/// use pigeonhole_wal::WalOptions;
///
/// let mut opts = WalOptions::default();
/// assert_eq!(opts.segment_size, 64 << 20);
/// opts.spare_segments = 1;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct WalOptions {
    /// Segment size in bytes; a multiple of 32 KiB, at most 4 GiB − 32 KiB (decision D43).
    /// Default 64 MiB.
    pub segment_size: u64,
    /// Segments kept preallocated ahead of the writer. Default 2.
    pub spare_segments: u32,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            segment_size: 64 * 1024 * 1024,
            spare_segments: 2,
        }
    }
}

/// Proof that a commit's record was appended, and how durable it must become. The engine
/// resolves a commit when [`Wal::satisfies`] returns true for its ticket.
///
/// ```
/// use pigeonhole_format::{Durability, Lsn, StreamId};
/// use pigeonhole_wal::{MemWal, Wal};
///
/// let mut wal = MemWal::new(StreamId(3));
/// let rec = pigeonhole_format::wal::WalRecord::Commit {
///     seqno: 5,
///     participants: pigeonhole_format::wal::StreamList::new(&[0, 0]).unwrap(),
/// };
/// let ticket = wal.append(&rec, Durability::Buffered).unwrap();
/// assert_eq!(ticket.stream, StreamId(3));
/// assert!(!wal.satisfies(&ticket));
/// wal.write().unwrap();
/// assert!(wal.satisfies(&ticket));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitTicket {
    /// Stream the record went to.
    pub stream: StreamId,
    /// Position just past the record.
    pub end: Lsn,
    /// Level the commit asked for.
    pub durability: Durability,
}

impl CommitTicket {
    /// Whether a stream whose written and durable positions are `written` and `durable`
    /// meets this ticket's level. Shared by every [`Wal`] implementation.
    fn met_by(&self, stream: StreamId, written: Lsn, durable: Lsn) -> bool {
        if self.stream != stream {
            return false;
        }
        match self.durability {
            Durability::None => true,
            Durability::Buffered => self.end <= written,
            Durability::GroupSync | Durability::Sync => self.end <= durable,
        }
    }
}

/// One shard's log stream. Single-threaded: owned by the shard thread.
///
/// **Failure rule.** Once any write or sync of a stream has failed (a `write`, `sync`,
/// `submit_sync` completion, or the write and sync a full segment triggers inside `append`),
/// the stream is poisoned: every later `append`, `write`, `sync` and `submit_sync` fails with
/// [`Error::Poisoned`]. The kernel's view of the file is unknown after such a failure, so a
/// later successful sync must never acknowledge commits behind a hole that replay would drop.
/// The engine reopens the stream through [`Recovery`] to continue. Argument errors
/// ([`Error::RecordTooLarge`], [`Error::InvalidArgument`]) do not poison.
pub trait Wal: Send + fmt::Debug {
    /// This stream's id.
    fn stream(&self) -> StreamId;

    /// Encodes and frames `record` into the stream's buffer (no syscall). Returns the ticket
    /// for `durability`. Opens a new segment when the current one is full.
    ///
    /// A `Durability::None` commit's record stays in the buffer with no I/O of its own
    /// (its ticket is satisfied at once); the next [`Wal::write`] or sync, which a stronger
    /// commit triggers, carries it to the file (FORMAT §10.3, decision #50). Until then it
    /// survives nothing.
    fn append(&mut self, record: &WalRecord<'_>, durability: Durability) -> Result<CommitTicket>;

    /// Hands every appended byte to the kernel with one `write()`. After this, every ticket
    /// appended so far satisfies `Buffered`.
    fn write(&mut self) -> Result<Lsn>;

    /// Writes (if needed) and fdatasyncs, blocking. After this, every ticket appended so far
    /// satisfies `GroupSync` and `Sync`. For tests and shutdown; shards use
    /// [`Wal::submit_sync`].
    fn sync(&mut self) -> Result<Lsn>;

    /// Writes (if needed) and submits an fdatasync covering everything appended so far.
    /// Returns at once; the completion resolves with the position that sync made durable,
    /// after which [`Wal::durable`] is at least that. Completions may finish in any order (the
    /// I/O backend has several workers); `durable` only ever moves forward, and a later sync's
    /// position covers every earlier one's, so waiting on any completion is enough for the
    /// tickets at or below its position.
    fn submit_sync(&mut self) -> Result<Completion<Lsn>>;

    /// Position past the last byte handed to the kernel.
    fn written(&self) -> Lsn;

    /// Position past the last durable byte.
    fn durable(&self) -> Lsn;

    /// Whether `ticket`'s record meets its requested durability.
    fn satisfies(&self, ticket: &CommitTicket) -> bool;

    /// Everything before `upto` is no longer needed (the manifest records the checkpoint);
    /// whole segments below it are recycled under a new epoch.
    ///
    /// Preconditions, enforced by the engine: the manifest edit recording `upto` as this
    /// stream's checkpoint is durable before this is called (a recycled segment is gone for
    /// good, so a manifest that still named an older checkpoint could not be replayed from),
    /// and `upto` never passes a COMMIT record while a participant's PREPARE for it may
    /// still need replay (decision D24). This crate checks neither.
    fn checkpoint(&mut self, upto: Lsn) -> Result<()>;

    /// A handle for preparing spare segments on a background task, if the implementation
    /// has segments to prepare (`None` for mocks).
    fn spares(&self) -> Option<SpareSegments> {
        None
    }

    /// Removes this stream's files (clean close by the last process, after checkpoint).
    fn remove(self: Box<Self>) -> Result<()>;
}

/// Path of stream `stream`'s file: `<db path>-wal-<stream>`.
///
/// ```
/// use std::path::Path;
/// use pigeonhole_format::StreamId;
///
/// let p = pigeonhole_wal::stream_path(Path::new("/data/db.phdb"), StreamId(3));
/// assert_eq!(p, Path::new("/data/db.phdb-wal-3"));
/// ```
pub fn stream_path(db_path: &Path, stream: StreamId) -> PathBuf {
    let mut name = db_path.as_os_str().to_owned();
    name.push(format!("-wal-{}", stream.0));
    PathBuf::from(name)
}

/// Every stream file present for `db_path`, in stream order (used at open to replay all
/// streams regardless of the current shard count).
///
/// ```
/// use std::path::Path;
/// use pigeonhole_format::StreamId;
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_wal::{WalOptions, WalStream, discover_streams};
///
/// # fn main() -> pigeonhole_wal::Result<()> {
/// let vfs: pigeonhole_io::VfsRef = SimVfs::new(1);
/// let db = Path::new("/db/data.phdb");
/// let mut opts = WalOptions::default();
/// opts.segment_size = 2 * 32 * 1024;
/// for s in [2, 0] {
///     WalStream::create(&vfs, db, StreamId(s), [1; 16], opts)?;
/// }
/// assert_eq!(discover_streams(&vfs, db)?, [StreamId(0), StreamId(2)]);
/// # Ok(())
/// # }
/// ```
pub fn discover_streams(vfs: &VfsRef, db_path: &Path) -> Result<Vec<StreamId>> {
    let dir = match db_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let Some(db_name) = db_path.file_name().and_then(|n| n.to_str()) else {
        return Ok(Vec::new());
    };
    let prefix = format!("{db_name}-wal-");
    let mut streams: Vec<StreamId> = vfs
        .list_dir(dir)?
        .iter()
        .filter_map(|p| {
            p.file_name()?
                .to_str()?
                .strip_prefix(&prefix)?
                .parse::<u32>()
                .ok()
        })
        .map(StreamId)
        .collect();
    streams.sort_unstable();
    streams.dedup();
    Ok(streams)
}

/// Syncs the directory holding `path` (after creating or removing a stream file).
fn sync_parent(vfs: &VfsRef, path: &Path) -> Result<()> {
    let dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    vfs.sync_dir(dir)?;
    Ok(())
}
