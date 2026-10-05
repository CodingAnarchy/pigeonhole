//! WAL segments, 32 KiB frames and log records. See `FORMAT.md` §10.
//!
//! A segment is a preallocated file region made of 32 KiB frames. Frame 0 holds the segment
//! header (magic, stream, epoch, db id, and the predecessor's epoch and end offset). Every
//! later frame holds fragments: a 12-byte header (CRC32C, the segment epoch, length, fragment
//! type) and payload. A record larger than the space left in a frame is split into
//! First/Middle/Last fragments.
//!
//! Replay of a segment stops at the first fragment that is unused, stale (another epoch), bad
//! or incomplete. That stop is the **end of the segment** if some segment's header names this
//! segment and this offset as its predecessor, and the **end of the log** otherwise. A writer
//! never appends to a segment after recovery; it starts a new one whose header records where
//! the old one ended (decision D25).

use crate::{FamilyId, FormatVersion, Seqno, StreamId, TableId, Timestamp};

/// Size of a WAL frame.
pub const FRAME_SIZE: usize = 32 * 1024;

/// Size of a fragment header.
pub const FRAGMENT_HEADER_LEN: usize = 12;

/// Fragment types. `0` marks unused (zeroed) space. Numbers are frozen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FragmentType {
    /// A whole record.
    Full = 1,
    /// The first piece of a record.
    First = 2,
    /// A middle piece.
    Middle = 3,
    /// The last piece.
    Last = 4,
}

/// The header in frame 0 of every segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    /// Format version.
    pub version: FormatVersion,
    /// Stream this segment belongs to.
    pub stream: StreamId,
    /// Epoch of this use of the segment: one more than the largest epoch in any segment
    /// header of the stream when it was started, so it never repeats.
    pub epoch: u32,
    /// Epoch of the segment this one continues, or 0 for the first segment of a new stream.
    pub prev_epoch: u32,
    /// Offset in the predecessor where its valid log ends. Replay of the predecessor must stop
    /// exactly here for this segment to continue it.
    pub prev_end: u32,
    /// Database id from the superblock; a segment from another database is rejected.
    pub db_id: [u8; 16],
    /// Segment size in bytes: a multiple of [`FRAME_SIZE`], at most 4 GiB (LSN offsets are
    /// `u32`).
    pub segment_size: u64,
}

impl SegmentHeader {
    /// Encodes into a frame-0 buffer (unused bytes zero).
    pub fn encode(&self, frame: &mut [u8; FRAME_SIZE]) {
        todo!()
    }

    /// Decodes and verifies magic, CRC32C and version.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        todo!()
    }
}

/// Splits records into fragments, tracking the position within the current frame across
/// calls so consecutive group commits pack frames densely.
#[derive(Debug)]
pub struct FrameEncoder {
    _priv: (),
}

impl FrameEncoder {
    /// An encoder for a segment with `epoch`, starting at byte `offset` of the segment
    /// (which must be past frame 0).
    pub fn new(epoch: u32, offset: u64) -> Self {
        todo!()
    }

    /// Appends the fragments of `record` to `out`, zero-padding a frame tail too short for a
    /// header. Returns the segment offset just past the record.
    pub fn encode(&mut self, record: &[u8], out: &mut Vec<u8>) -> u64 {
        todo!()
    }

    /// Bytes that encoding a record of `len` bytes would append (for segment-full checks).
    pub fn encoded_len(&self, len: usize) -> usize {
        todo!()
    }
}

/// What the decoder found at a position.
#[derive(Debug, PartialEq, Eq)]
pub enum Decoded {
    /// A complete record; its bytes are in the decoder's buffer.
    Record {
        /// Segment offset of the record's first fragment.
        offset: u64,
    },
    /// The segment's valid data ends here: an unused fragment, a stale epoch, a bad CRC or an
    /// incomplete record. Whether this is also the end of the log depends on whether a
    /// successor segment names this offset as its `prev_end`.
    Stop {
        /// Segment offset where valid data ends.
        offset: u64,
    },
}

/// Reassembles records from a segment's frames.
#[derive(Debug)]
pub struct FrameDecoder {
    _priv: (),
}

impl FrameDecoder {
    /// A decoder for a segment with `epoch`, starting at `offset`.
    pub fn new(epoch: u32, offset: u64) -> Self {
        todo!()
    }

    /// Feeds the next frame (exactly [`FRAME_SIZE`] bytes, frame-aligned) and decodes until
    /// a record completes, the frame is exhausted (`Ok(None)`), or the segment's data stops.
    /// A frame tail shorter than a fragment header is skipped whatever its bytes.
    pub fn decode(&mut self, frame: &[u8]) -> crate::Result<Option<Decoded>> {
        todo!()
    }

    /// The last complete record.
    pub fn record(&self) -> &[u8] {
        todo!()
    }
}

/// Record type byte. Numbers are frozen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RecordType {
    /// A single-shard commit.
    Batch = 1,
    /// A participant's share of a cross-shard commit.
    Prepare = 2,
    /// The coordinator's decision for a cross-shard commit.
    Commit = 3,
}

/// A decoded WAL record, borrowing its batch bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecord<'a> {
    /// Single-shard commit: apply at `seqno`.
    Batch {
        /// Commit seqno.
        seqno: Seqno,
        /// Commit timestamp for mutations that did not supply one.
        commit_ts: Timestamp,
        /// The mutations.
        batch: BatchRef<'a>,
    },
    /// Cross-shard participant record: apply at `seqno` only if the coordinator's stream holds
    /// a [`WalRecord::Commit`] with the same seqno. The reserved seqno is the commit's id.
    Prepare {
        /// Commit seqno, reserved by the coordinator; identifies the commit.
        seqno: Seqno,
        /// Commit timestamp.
        commit_ts: Timestamp,
        /// Stream holding the decision.
        coordinator: StreamId,
        /// This participant's mutations.
        batch: BatchRef<'a>,
    },
    /// Cross-shard decision: every PREPARE with this seqno is committed.
    Commit {
        /// Commit seqno.
        seqno: Seqno,
        /// Participant streams (diagnostics and recovery cross-checks).
        participants: StreamList<'a>,
    },
}

impl WalRecord<'_> {
    /// Appends the record encoding.
    pub fn encode(&self, out: &mut Vec<u8>) {
        todo!()
    }

    /// Decodes a record. Never panics.
    pub fn decode(bytes: &[u8]) -> crate::Result<WalRecord<'_>> {
        todo!()
    }
}

/// Builds the mutation list of a batch in its final encoding, so a write batch becomes a WAL
/// record without re-encoding. Used by the engine's `WriteBatch`.
#[derive(Debug, Default, Clone)]
pub struct BatchBuilder {
    _priv: (),
}

impl BatchBuilder {
    /// An empty batch.
    pub fn new() -> Self {
        todo!()
    }

    /// Appends one mutation. `ts == None` means "use the commit timestamp". `value` is an
    /// already-encoded stored value ([`crate::value`]); empty for deletes.
    #[allow(clippy::too_many_arguments)]
    pub fn push(
        &mut self,
        table: TableId,
        family: FamilyId,
        kind: crate::Kind,
        row: &[u8],
        qualifier: &[u8],
        ts: Option<Timestamp>,
        value: &[u8],
    ) -> crate::Result<()> {
        todo!()
    }

    /// Number of mutations.
    pub fn len(&self) -> usize {
        todo!()
    }

    /// Whether the batch has no mutations.
    pub fn is_empty(&self) -> bool {
        todo!()
    }

    /// The encoded batch.
    pub fn batch(&self) -> BatchRef<'_> {
        todo!()
    }

    /// Clears the batch, keeping its allocation.
    pub fn clear(&mut self) {
        todo!()
    }
}

/// An encoded batch (`count u32` then mutations), borrowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchRef<'a> {
    _bytes: &'a [u8],
}

impl<'a> BatchRef<'a> {
    /// Wraps encoded batch bytes, validating the count. Never panics.
    pub fn new(bytes: &'a [u8]) -> crate::Result<Self> {
        todo!()
    }

    /// The encoded bytes.
    pub fn as_bytes(&self) -> &'a [u8] {
        todo!()
    }

    /// Number of mutations.
    pub fn len(&self) -> usize {
        todo!()
    }

    /// Whether the batch has no mutations.
    pub fn is_empty(&self) -> bool {
        todo!()
    }

    /// Iterates the mutations in insertion order.
    pub fn iter(&self) -> BatchIter<'a> {
        todo!()
    }
}

/// One decoded mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mutation<'a> {
    /// Table.
    pub table: TableId,
    /// Family.
    pub family: FamilyId,
    /// Kind.
    pub kind: crate::Kind,
    /// Row (unescaped).
    pub row: &'a [u8],
    /// Qualifier (unescaped); empty for [`crate::Kind::FamilyDelete`].
    pub qualifier: &'a [u8],
    /// Explicit timestamp, or `None` for the commit timestamp.
    pub ts: Option<Timestamp>,
    /// Encoded stored value; empty for deletes.
    pub value: &'a [u8],
}

/// Iterator over a batch's mutations.
#[derive(Debug, Clone)]
pub struct BatchIter<'a> {
    _rest: &'a [u8],
}

impl<'a> Iterator for BatchIter<'a> {
    type Item = crate::Result<Mutation<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

/// A stream list borrowed from a COMMIT record: `count u16 LE`, then `count` stream ids
/// as `u32 LE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamList<'a> {
    _bytes: &'a [u8],
}

impl<'a> StreamList<'a> {
    /// Wraps the encoded list; fails unless `bytes.len() == 2 + 4 * count`.
    pub fn new(bytes: &'a [u8]) -> crate::Result<Self> {
        todo!()
    }

    /// Appends `streams` in the same encoding.
    pub fn encode(streams: &[StreamId], out: &mut Vec<u8>) {
        todo!()
    }

    /// Iterates the stream ids.
    pub fn iter(&self) -> impl Iterator<Item = StreamId> + 'a {
        std::iter::empty()
    }
}
