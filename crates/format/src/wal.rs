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
//!
//! ```
//! use pigeonhole_format::wal::{Decoded, FRAME_SIZE, FrameDecoder, FrameEncoder};
//!
//! // Encode two records starting at frame 1 of a segment with epoch 3.
//! let mut enc = FrameEncoder::new(3, FRAME_SIZE as u64);
//! let mut bytes = Vec::new();
//! enc.encode(b"first", &mut bytes);
//! enc.encode(&[9u8; 40_000], &mut bytes); // spans two frames
//! bytes.resize(3 * FRAME_SIZE, 0); // frames 1 and 2, zero-padded
//!
//! let mut dec = FrameDecoder::new(3, FRAME_SIZE as u64);
//! let mut records = Vec::new();
//! 'frames: for frame in bytes.chunks(FRAME_SIZE) {
//!     loop {
//!         match dec.decode(frame).unwrap() {
//!             Some(Decoded::Record { .. }) => records.push(dec.record().to_vec()),
//!             Some(Decoded::Stop { .. }) => break 'frames,
//!             None => break,
//!         }
//!     }
//! }
//! assert_eq!(records, [b"first".to_vec(), vec![9u8; 40_000]]);
//! ```

use crate::bytes::{Reader, le_u32, le_u64};
use crate::key::{Kind, MAX_KEY_PART};
use crate::value::MAX_VALUE_LEN;
use crate::version::WAL_SEGMENT_MAGIC;
use crate::{Error, FamilyId, FormatVersion, Seqno, StreamId, TableId, Timestamp};

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
        frame.fill(0);
        frame[0..8].copy_from_slice(&WAL_SEGMENT_MAGIC);
        frame[8..12].copy_from_slice(&self.version.0.to_le_bytes());
        frame[12..16].copy_from_slice(&self.stream.0.to_le_bytes());
        frame[16..20].copy_from_slice(&self.epoch.to_le_bytes());
        frame[20..24].copy_from_slice(&self.prev_epoch.to_le_bytes());
        frame[24..40].copy_from_slice(&self.db_id);
        frame[40..48].copy_from_slice(&self.segment_size.to_le_bytes());
        frame[48..52].copy_from_slice(&(FRAME_SIZE as u32).to_le_bytes());
        frame[52..56].copy_from_slice(&self.prev_end.to_le_bytes());
        let crc = crate::checksum::crc32c(&frame[..56]);
        frame[56..60].copy_from_slice(&crc.to_le_bytes());
    }

    /// Decodes and verifies magic, CRC32C and version. Also checks the frame size and that
    /// the segment size is a whole number (at least two) of frames and at most 4 GiB.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        const WHAT: &str = "wal segment header";
        let Some(b) = bytes.get(..SEGMENT_HEADER_LEN) else {
            return Err(Error::Truncated { what: WHAT });
        };
        if b[..8] != WAL_SEGMENT_MAGIC {
            return Err(Error::BadMagic { what: WHAT });
        }
        if crate::checksum::crc32c(&b[..56]) != le_u32(b, 56) {
            return Err(Error::Checksum { what: WHAT });
        }
        let version = FormatVersion(le_u32(b, 8));
        version.check(WHAT)?;
        let segment_size = le_u64(b, 40);
        let frames = segment_size / FRAME_SIZE as u64;
        if le_u32(b, 48) as usize != FRAME_SIZE
            || !segment_size.is_multiple_of(FRAME_SIZE as u64)
            || frames < 2
            || segment_size > 1 << 32
        {
            return Err(Error::Corrupt { what: WHAT });
        }
        Ok(Self {
            version,
            stream: StreamId(le_u32(b, 12)),
            epoch: le_u32(b, 16),
            prev_epoch: le_u32(b, 20),
            prev_end: le_u32(b, 52),
            db_id: b[24..40].try_into().expect("16 bytes"),
            segment_size,
        })
    }
}

/// Splits records into fragments, tracking the position within the current frame across
/// calls so consecutive group commits pack frames densely.
#[derive(Debug)]
pub struct FrameEncoder {
    epoch: u32,
    /// Segment offset of the next byte to write.
    pos: u64,
}

/// Size of the meaningful part of a segment header (the rest of frame 0 is zero).
const SEGMENT_HEADER_LEN: usize = 60;

/// A fragment is only started where more than a header's worth of bytes remains, so every
/// fragment except an empty record's carries payload. Shorter frame tails are skipped.
const MIN_FRAGMENT_ROOM: usize = FRAGMENT_HEADER_LEN + 1;

/// Bytes left in the frame at segment offset `pos`.
fn frame_room(pos: u64) -> usize {
    FRAME_SIZE - (pos % FRAME_SIZE as u64) as usize
}

fn fragment_type(first: bool, last: bool) -> FragmentType {
    match (first, last) {
        (true, true) => FragmentType::Full,
        (true, false) => FragmentType::First,
        (false, false) => FragmentType::Middle,
        (false, true) => FragmentType::Last,
    }
}

impl FrameEncoder {
    /// An encoder for a segment with `epoch`, starting at byte `offset` of the segment
    /// (which must be past frame 0).
    pub fn new(epoch: u32, offset: u64) -> Self {
        Self {
            epoch,
            pos: offset.max(FRAME_SIZE as u64),
        }
    }

    /// Appends the fragments of `record` to `out`, zero-padding a frame tail of 12 bytes or
    /// fewer (too short for a header and any payload). Returns the segment offset just past the record.
    ///
    /// The tail of a frame left too short for a header is padded lazily, at the start of the
    /// next record, so the returned offset is exactly where the record's data ends: the
    /// `prev_end` a successor segment records when this segment fills up.
    pub fn encode(&mut self, record: &[u8], out: &mut Vec<u8>) -> u64 {
        let mut rest = record;
        let mut first = true;
        loop {
            let room = frame_room(self.pos);
            if room < MIN_FRAGMENT_ROOM {
                out.resize(out.len() + room, 0);
                self.pos += room as u64;
                continue;
            }
            let n = rest.len().min(room - FRAGMENT_HEADER_LEN);
            let last = n == rest.len();
            let mut header = [0u8; FRAGMENT_HEADER_LEN];
            header[4..8].copy_from_slice(&self.epoch.to_le_bytes());
            header[8..10].copy_from_slice(&(n as u16).to_le_bytes());
            header[10] = fragment_type(first, last) as u8;
            let crc =
                crate::checksum::crc32c_append(crate::checksum::crc32c(&header[4..]), &rest[..n]);
            header[..4].copy_from_slice(&crc.to_le_bytes());
            out.extend_from_slice(&header);
            out.extend_from_slice(&rest[..n]);
            self.pos += (FRAGMENT_HEADER_LEN + n) as u64;
            rest = &rest[n..];
            first = false;
            if last {
                return self.pos;
            }
        }
    }

    /// Bytes that encoding a record of `len` bytes would append (for segment-full checks).
    pub fn encoded_len(&self, len: usize) -> usize {
        let mut pos = self.pos;
        let mut rest = len;
        loop {
            let room = frame_room(pos);
            if room < MIN_FRAGMENT_ROOM {
                pos += room as u64;
                continue;
            }
            let n = rest.min(room - FRAGMENT_HEADER_LEN);
            pos += (FRAGMENT_HEADER_LEN + n) as u64;
            rest -= n;
            if rest == 0 {
                return (pos - self.pos) as usize;
            }
        }
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
        /// Segment offset where valid data ends: just past the last complete record (or the
        /// starting offset if none), never past a skipped frame tail. A writer that fills a
        /// segment records the same offset as its successor's `prev_end`.
        offset: u64,
    },
}

/// Reassembles records from a segment's frames.
#[derive(Debug)]
pub struct FrameDecoder {
    epoch: u32,
    /// Segment offset of the frame the caller feeds next (or is feeding).
    frame_start: u64,
    /// Segment offset of the next fragment header to read.
    pos: u64,
    /// Just past the last complete record.
    record_end: u64,
    /// Offset of the first fragment of the record being assembled, if any.
    partial_start: Option<u64>,
    partial: Vec<u8>,
    record: Vec<u8>,
    stopped: bool,
}

impl FrameDecoder {
    /// A decoder for a segment with `epoch`, starting at `offset`.
    pub fn new(epoch: u32, offset: u64) -> Self {
        // Frame 0 holds the segment header, never fragments (as in `FrameEncoder::new`).
        let offset = offset.max(FRAME_SIZE as u64);
        Self {
            epoch,
            frame_start: offset - offset % FRAME_SIZE as u64,
            pos: offset,
            record_end: offset,
            partial_start: None,
            partial: Vec::new(),
            record: Vec::new(),
            stopped: false,
        }
    }

    /// Feeds the next frame (exactly [`FRAME_SIZE`] bytes, frame-aligned) and decodes until
    /// a record completes, the frame is exhausted (`Ok(None)`), or the segment's data stops.
    /// A frame tail of 12 bytes or fewer is skipped whatever its bytes.
    ///
    /// After a `Record`, call again with the same frame; after `Ok(None)`, feed the next
    /// frame. Once it has returned `Stop`, it keeps returning the same `Stop`. If the segment
    /// ends without a `Stop`, its data stops at the end of the last record (a record still
    /// being assembled is incomplete).
    pub fn decode(&mut self, frame: &[u8]) -> crate::Result<Option<Decoded>> {
        if frame.len() != FRAME_SIZE {
            return Err(Error::Corrupt {
                what: "wal frame length",
            });
        }
        if self.stopped {
            return Ok(Some(Decoded::Stop {
                offset: self.record_end,
            }));
        }
        loop {
            let at = (self.pos - self.frame_start) as usize;
            if at + MIN_FRAGMENT_ROOM > FRAME_SIZE {
                self.frame_start += FRAME_SIZE as u64;
                self.pos = self.frame_start;
                return Ok(None);
            }
            let h = &frame[at..at + FRAGMENT_HEADER_LEN];
            let len = usize::from(u16::from_le_bytes([h[8], h[9]]));
            let end = at + FRAGMENT_HEADER_LEN + len;
            let ty = h[10];
            let sequenced = match ty {
                1 | 2 => self.partial_start.is_none(),
                3 | 4 => self.partial_start.is_some(),
                _ => false,
            };
            if !sequenced
                || le_u32(h, 4) != self.epoch
                || end > FRAME_SIZE
                || crate::checksum::crc32c_append(
                    crate::checksum::crc32c(&h[4..]),
                    &frame[at + FRAGMENT_HEADER_LEN..end],
                ) != le_u32(h, 0)
            {
                self.stopped = true;
                return Ok(Some(Decoded::Stop {
                    offset: self.record_end,
                }));
            }
            let start = self.pos;
            let payload = &frame[at + FRAGMENT_HEADER_LEN..end];
            self.pos += (FRAGMENT_HEADER_LEN + len) as u64;
            if ty == FragmentType::First as u8 || ty == FragmentType::Full as u8 {
                self.partial.clear();
                self.partial_start = Some(start);
            }
            self.partial.extend_from_slice(payload);
            if ty == FragmentType::Full as u8 || ty == FragmentType::Last as u8 {
                std::mem::swap(&mut self.partial, &mut self.record);
                self.partial.clear();
                self.record_end = self.pos;
                let offset = self.partial_start.take().unwrap_or(start);
                return Ok(Some(Decoded::Record { offset }));
            }
        }
    }

    /// The last complete record.
    pub fn record(&self) -> &[u8] {
        &self.record
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
        match self {
            WalRecord::Batch {
                seqno,
                commit_ts,
                batch,
            } => {
                out.push(RecordType::Batch as u8);
                out.extend_from_slice(&seqno.to_le_bytes());
                out.extend_from_slice(&commit_ts.to_le_bytes());
                out.extend_from_slice(batch.as_bytes());
            }
            WalRecord::Prepare {
                seqno,
                commit_ts,
                coordinator,
                batch,
            } => {
                out.push(RecordType::Prepare as u8);
                out.extend_from_slice(&seqno.to_le_bytes());
                out.extend_from_slice(&commit_ts.to_le_bytes());
                out.extend_from_slice(&coordinator.0.to_le_bytes());
                out.extend_from_slice(batch.as_bytes());
            }
            WalRecord::Commit {
                seqno,
                participants,
            } => {
                out.push(RecordType::Commit as u8);
                out.extend_from_slice(&seqno.to_le_bytes());
                out.extend_from_slice(participants.bytes);
            }
        }
    }

    /// Decodes a record. Never panics.
    ///
    /// ```
    /// use pigeonhole_format::wal::{BatchBuilder, WalRecord};
    /// use pigeonhole_format::{FamilyId, Kind, TableId};
    ///
    /// let mut b = BatchBuilder::new();
    /// b.push(TableId(1), FamilyId(2), Kind::Put, b"row", b"q", None, b"\x00v").unwrap();
    /// let rec = WalRecord::Batch { seqno: 9, commit_ts: 100, batch: b.batch() };
    /// let mut bytes = Vec::new();
    /// rec.encode(&mut bytes);
    /// assert_eq!(WalRecord::decode(&bytes).unwrap(), rec);
    /// ```
    pub fn decode(bytes: &[u8]) -> crate::Result<WalRecord<'_>> {
        let mut r = Reader::new(bytes, "wal record");
        match r.u8()? {
            1 => Ok(WalRecord::Batch {
                seqno: r.u64()?,
                commit_ts: r.u64()?,
                batch: BatchRef::new(r.rest())?,
            }),
            2 => Ok(WalRecord::Prepare {
                seqno: r.u64()?,
                commit_ts: r.u64()?,
                coordinator: StreamId(r.u32()?),
                batch: BatchRef::new(r.rest())?,
            }),
            3 => Ok(WalRecord::Commit {
                seqno: r.u64()?,
                participants: StreamList::new(r.rest())?,
            }),
            _ => Err(Error::Corrupt {
                what: "wal record type",
            }),
        }
    }
}

/// Builds the mutation list of a batch in its final encoding, so a write batch becomes a WAL
/// record without re-encoding. Used by the engine's `WriteBatch`.
#[derive(Debug, Clone)]
pub struct BatchBuilder {
    /// `count u32 LE` followed by the encoded mutations.
    buf: Vec<u8>,
    count: u32,
}

impl Default for BatchBuilder {
    fn default() -> Self {
        Self {
            buf: vec![0; 4],
            count: 0,
        }
    }
}

/// Bit of `kind_flags` set when the mutation carries its own timestamp.
const EXPLICIT_TS: u8 = 0x80;

/// The rules every mutation obeys, checked when it is pushed and again when a batch is
/// decoded: row and qualifier at most [`MAX_KEY_PART`], value at most [`MAX_VALUE_LEN`],
/// deletes carry no value, and a family delete carries no qualifier.
fn check_mutation(kind: Kind, row: &[u8], qualifier: &[u8], value: &[u8]) -> crate::Result<()> {
    if row.len() > MAX_KEY_PART || qualifier.len() > MAX_KEY_PART {
        return Err(Error::KeyTooLarge);
    }
    if value.len() as u64 > MAX_VALUE_LEN {
        return Err(Error::ValueTooLarge);
    }
    if (kind.is_delete() && !value.is_empty())
        || (kind == Kind::FamilyDelete && !qualifier.is_empty())
    {
        return Err(Error::Corrupt {
            what: "mutation: delete with value or qualifier",
        });
    }
    Ok(())
}

impl BatchBuilder {
    /// An empty batch.
    pub fn new() -> Self {
        Self::default()
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
        check_mutation(kind, row, qualifier, value)?;
        let count = self.count.checked_add(1).ok_or(Error::ValueTooLarge)?;
        let b = &mut self.buf;
        b.extend_from_slice(&table.0.to_le_bytes());
        b.extend_from_slice(&family.0.to_le_bytes());
        match ts {
            Some(ts) => {
                b.push(kind as u8 | EXPLICIT_TS);
                b.extend_from_slice(&ts.to_le_bytes());
            }
            None => b.push(kind as u8),
        }
        crate::varint::put_bytes(b, row);
        crate::varint::put_bytes(b, qualifier);
        crate::varint::put_bytes(b, value);
        self.count = count;
        b[..4].copy_from_slice(&count.to_le_bytes());
        Ok(())
    }

    /// Number of mutations.
    pub fn len(&self) -> usize {
        self.count as usize
    }

    /// Whether the batch has no mutations.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The encoded batch.
    pub fn batch(&self) -> BatchRef<'_> {
        BatchRef {
            bytes: &self.buf,
            count: self.count,
        }
    }

    /// Clears the batch, keeping its allocation.
    pub fn clear(&mut self) {
        self.buf.truncate(4);
        self.buf.fill(0);
        self.count = 0;
    }
}

/// An encoded batch (`count u32` then mutations), borrowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchRef<'a> {
    bytes: &'a [u8],
    count: u32,
}

impl<'a> BatchRef<'a> {
    /// Wraps encoded batch bytes, validating the count. Never panics.
    ///
    /// Every mutation is parsed once here, and trailing bytes are rejected, so iterating a
    /// `BatchRef` built by this function never yields an error.
    pub fn new(bytes: &'a [u8]) -> crate::Result<Self> {
        let mut r = Reader::new(bytes, "wal batch");
        let count = r.u32()?;
        let mut it = BatchIter {
            rest: r.rest(),
            remaining: count,
        };
        for m in &mut it {
            m?;
        }
        if !it.rest.is_empty() {
            return Err(Error::Corrupt {
                what: "wal batch: trailing bytes",
            });
        }
        Ok(Self { bytes, count })
    }

    /// The encoded bytes.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// Number of mutations.
    pub fn len(&self) -> usize {
        self.count as usize
    }

    /// Whether the batch has no mutations.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Iterates the mutations in insertion order.
    pub fn iter(&self) -> BatchIter<'a> {
        BatchIter {
            rest: self.bytes.get(4..).unwrap_or_default(),
            remaining: self.count,
        }
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
    rest: &'a [u8],
    remaining: u32,
}

impl<'a> BatchIter<'a> {
    fn parse(&mut self) -> crate::Result<Mutation<'a>> {
        let mut r = Reader::new(self.rest, "wal mutation");
        let table = TableId(r.u32()?);
        let family = FamilyId(r.u32()?);
        let flags = r.u8()?;
        let kind = Kind::from_u8(flags & !EXPLICIT_TS)?;
        let ts = if flags & EXPLICIT_TS != 0 {
            Some(r.u64()?)
        } else {
            None
        };
        let m = Mutation {
            table,
            family,
            kind,
            ts,
            row: r.bytes()?,
            qualifier: r.bytes()?,
            value: r.bytes()?,
        };
        check_mutation(m.kind, m.row, m.qualifier, m.value)?;
        self.rest = r.rest();
        Ok(m)
    }
}

impl<'a> Iterator for BatchIter<'a> {
    type Item = crate::Result<Mutation<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let m = self.parse();
        if m.is_err() {
            self.remaining = 0;
        }
        Some(m)
    }
}

/// A stream list borrowed from a COMMIT record: `count u16 LE`, then `count` stream ids
/// as `u32 LE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamList<'a> {
    bytes: &'a [u8],
}

impl<'a> StreamList<'a> {
    /// Wraps the encoded list; fails unless `bytes.len() == 2 + 4 * count`.
    pub fn new(bytes: &'a [u8]) -> crate::Result<Self> {
        let Some(count) = bytes.get(..2) else {
            return Err(Error::Truncated {
                what: "wal stream list",
            });
        };
        let count = usize::from(u16::from_le_bytes([count[0], count[1]]));
        if bytes.len() != 2 + 4 * count {
            return Err(Error::Corrupt {
                what: "wal stream list length",
            });
        }
        Ok(Self { bytes })
    }

    /// Appends `streams` in the same encoding.
    ///
    /// # Panics
    ///
    /// If there are more than `u16::MAX` streams. Participants are WAL streams, which number
    /// at most the shard count, so this is a caller bug; returning an error instead would
    /// change the frozen signature (see ICR 0001).
    pub fn encode(streams: &[StreamId], out: &mut Vec<u8>) {
        assert!(
            streams.len() <= usize::from(u16::MAX),
            "a COMMIT record lists at most 65535 participant streams"
        );
        out.extend_from_slice(&(streams.len() as u16).to_le_bytes());
        for s in streams {
            out.extend_from_slice(&s.0.to_le_bytes());
        }
    }

    /// Iterates the stream ids.
    pub fn iter(&self) -> impl Iterator<Item = StreamId> + 'a {
        let (ids, _) = self.bytes[2..].as_chunks::<4>();
        ids.iter().map(|c| StreamId(u32::from_le_bytes(*c)))
    }
}
