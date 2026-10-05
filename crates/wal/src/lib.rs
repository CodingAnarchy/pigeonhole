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
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]
// Interface freeze: bodies are `todo!()`. Remove this allow when implementing.
#![allow(unused_variables, clippy::ptr_arg)]

use std::fmt;
use std::path::{Path, PathBuf};

use pigeonhole_format::wal::WalRecord;
use pigeonhole_format::{Durability, Lsn, StreamId};
use pigeonhole_io::{Completion, VfsRef};

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// WAL errors.
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
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl std::error::Error for Error {}

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct WalOptions {
    /// Segment size in bytes; a multiple of 32 KiB, at most 4 GiB. Default 64 MiB.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitTicket {
    /// Stream the record went to.
    pub stream: StreamId,
    /// Position just past the record.
    pub end: Lsn,
    /// Level the commit asked for.
    pub durability: Durability,
}

/// One shard's log stream. Single-threaded: owned by the shard thread.
pub trait Wal: Send + fmt::Debug {
    /// This stream's id.
    fn stream(&self) -> StreamId;

    /// Encodes and frames `record` into the stream's buffer (no syscall). Returns the ticket
    /// for `durability`. Opens a new segment when the current one is full.
    fn append(&mut self, record: &WalRecord<'_>, durability: Durability) -> Result<CommitTicket>;

    /// Hands every appended byte to the kernel with one `write()`. After this, every ticket
    /// appended so far satisfies `Buffered`.
    fn write(&mut self) -> Result<Lsn>;

    /// Writes (if needed) and fdatasyncs, blocking. After this, every ticket appended so far
    /// satisfies `GroupSync` and `Sync`. For tests and shutdown; shards use
    /// [`Wal::submit_sync`].
    fn sync(&mut self) -> Result<Lsn>;

    /// Writes (if needed) and submits an fdatasync covering everything appended so far.
    /// Returns at once; the completion resolves with the newly durable position, after which
    /// [`Wal::durable`] reflects it. Syncs complete in submission order.
    fn submit_sync(&mut self) -> Result<Completion<Lsn>>;

    /// Position past the last byte handed to the kernel.
    fn written(&self) -> Lsn;

    /// Position past the last durable byte.
    fn durable(&self) -> Lsn;

    /// Whether `ticket`'s record meets its requested durability.
    fn satisfies(&self, ticket: &CommitTicket) -> bool;

    /// Everything before `upto` is no longer needed (the manifest records the checkpoint);
    /// whole segments below it are recycled under a new epoch.
    fn checkpoint(&mut self, upto: Lsn) -> Result<()>;

    /// Removes this stream's files (clean close by the last process, after checkpoint).
    fn remove(self: Box<Self>) -> Result<()>;
}

/// Path of stream `stream`'s file: `<db path>-wal-<stream>`.
pub fn stream_path(db_path: &Path, stream: StreamId) -> PathBuf {
    todo!()
}

/// Every stream file present for `db_path`, in stream order (used at open to replay all
/// streams regardless of the current shard count).
pub fn discover_streams(vfs: &VfsRef, db_path: &Path) -> Result<Vec<StreamId>> {
    todo!()
}

/// The sidecar-file implementation: one file per stream, made of preallocated segments that
/// are recycled after checkpoint.
#[derive(Debug)]
pub struct WalStream {
    _priv: (),
}

impl WalStream {
    /// Creates stream `stream` for a new database, or after its recovery found nothing.
    pub fn create(
        vfs: &VfsRef,
        db_path: &Path,
        stream: StreamId,
        db_id: [u8; 16],
        opts: WalOptions,
    ) -> Result<WalStream> {
        todo!()
    }
}

impl Wal for WalStream {
    fn stream(&self) -> StreamId {
        todo!()
    }

    fn append(&mut self, record: &WalRecord<'_>, durability: Durability) -> Result<CommitTicket> {
        todo!()
    }

    fn write(&mut self) -> Result<Lsn> {
        todo!()
    }

    fn sync(&mut self) -> Result<Lsn> {
        todo!()
    }

    fn submit_sync(&mut self) -> Result<Completion<Lsn>> {
        todo!()
    }

    fn written(&self) -> Lsn {
        todo!()
    }

    fn durable(&self) -> Lsn {
        todo!()
    }

    fn satisfies(&self, ticket: &CommitTicket) -> bool {
        todo!()
    }

    fn checkpoint(&mut self, upto: Lsn) -> Result<()> {
        todo!()
    }

    fn remove(self: Box<Self>) -> Result<()> {
        todo!()
    }
}

/// Replays one stream from its checkpoint. A lending reader: each record borrows the reader's
/// buffer until the next call.
///
/// Within a segment, replay stops at the first fragment that is unused, stale, bad or
/// incomplete. If another segment's header names this segment and exactly this offset as its
/// predecessor, replay continues there (end of segment); otherwise the log ends (torn tail).
/// A successor naming this segment with a different offset means synced data was lost and
/// fails with [`Error::Format`] rather than silently dropping it.
#[derive(Debug)]
pub struct Recovery {
    _priv: (),
}

impl Recovery {
    /// Opens `stream` for replay starting at `checkpoint` (from the manifest).
    pub fn open(
        vfs: &VfsRef,
        db_path: &Path,
        stream: StreamId,
        db_id: [u8; 16],
        checkpoint: Lsn,
    ) -> Result<Recovery> {
        todo!()
    }

    /// The next record and its end position, or `None` at the end of the log.
    pub fn next_record(&mut self) -> Result<Option<(Lsn, WalRecord<'_>)>> {
        todo!()
    }

    /// Where the valid log ends (after `next_record` returned `None`).
    pub fn end(&self) -> Lsn {
        todo!()
    }

    /// Every seqno seen in any record, including PREPAREs later discarded, is at most this;
    /// the engine starts `next_seqno` above it so a seqno is never reused.
    pub fn max_seqno(&self) -> pigeonhole_format::Seqno {
        todo!()
    }

    /// Reopens the stream for appending: starts a fresh segment with epoch one above the
    /// largest epoch in any segment header, chained (`prev_epoch`, `prev_end`) to where replay
    /// ended. The torn segment is never appended to; its slot is recycled after checkpoint.
    pub fn into_stream(self, opts: WalOptions) -> Result<WalStream> {
        todo!()
    }
}

/// In-memory mock of a stream for engine tests: keeps appended, written and synced records
/// separately so a test can drop what a crash would lose.
#[derive(Debug, Default)]
pub struct MemWal {
    _priv: (),
}

impl MemWal {
    /// An empty in-memory stream.
    pub fn new(stream: StreamId) -> Self {
        todo!()
    }

    /// Simulates a crash: keeps written records for a process crash, synced records for
    /// power loss.
    pub fn crash(&mut self, power_loss: bool) {
        todo!()
    }

    /// The surviving records, encoded, in order (feed them to the engine's replay).
    pub fn records(&self) -> Vec<(Lsn, Vec<u8>)> {
        todo!()
    }
}

impl Wal for MemWal {
    fn stream(&self) -> StreamId {
        todo!()
    }

    fn append(&mut self, record: &WalRecord<'_>, durability: Durability) -> Result<CommitTicket> {
        todo!()
    }

    fn write(&mut self) -> Result<Lsn> {
        todo!()
    }

    fn sync(&mut self) -> Result<Lsn> {
        todo!()
    }

    fn submit_sync(&mut self) -> Result<Completion<Lsn>> {
        todo!()
    }

    fn written(&self) -> Lsn {
        todo!()
    }

    fn durable(&self) -> Lsn {
        todo!()
    }

    fn satisfies(&self, ticket: &CommitTicket) -> bool {
        todo!()
    }

    fn checkpoint(&mut self, upto: Lsn) -> Result<()> {
        todo!()
    }

    fn remove(self: Box<Self>) -> Result<()> {
        todo!()
    }
}
