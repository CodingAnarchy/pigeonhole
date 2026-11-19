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

/// Fragment type 5, padding (D327, FORMAT §10.2): zeros the decoder steps over, written only
/// directly before a record's first fragment, in the same write, so a stream's next group
/// starts on a fresh page while a sync holds the page before it (#1708). Its payload is
/// zeros, covered by the CRC like any fragment's. Needs [`INCOMPAT_WAL_PAD`]. A constant
/// rather than a [`FragmentType`] variant: that enum is exhaustive, and a pad is never part
/// of a record.
pub const PAD_FRAGMENT: u8 = 5;

/// Superblock `incompat` bit 1 (D327): the database's WAL may hold [`PAD_FRAGMENT`]
/// fragments, which a build without D327 would refuse mid-replay. Set in both superblock
/// pages before the first segment that may hold one is written, and never cleared: retained
/// feed segments (D310) can keep pads long after a checkpoint.
pub const INCOMPAT_WAL_PAD: u64 = 1 << 1;

/// `incompat` bit 2 (D329): the WAL may hold [`WalRecord::ExternalPrepare`] and
/// [`WalRecord::ExternalAbort`] records, which a build without D329 would refuse mid-replay
/// or mid-feed. Set in both superblock pages before the first external PREPARE is appended,
/// and never cleared: as with [`INCOMPAT_WAL_PAD`], retained feed segments (D310) can keep
/// such records long after a checkpoint, for a feed resumed from before them.
pub const INCOMPAT_EXTERNAL_PREPARE: u64 = 1 << 2;

/// `incompat` bit 3 (D331, ICR 0141): the WAL may hold feed-silent batches, whose `count`
/// has [`SILENT_BATCH`] set. A build without D331 would read the count as two billion
/// mutations and report the record corrupt. Set in both superblock pages before the first
/// silent batch is appended, and never cleared (retained feed segments can keep such
/// records long after a checkpoint). A record decoder refuses the mark unless the bit is
/// set ([`WalRecord::decode_with`]), so a stray bit is never read as a mark.
pub const INCOMPAT_FEED_SILENT: u64 = 1 << 3;

/// The bit of a batch's `count u32` that marks it feed-silent (D331, ICR 0141, FORMAT
/// §10.3): logged and applied as usual, skipped by change-feed subscriptions, carried by
/// replication. The mutation count is the low 31 bits.
pub const SILENT_BATCH: u32 = 1 << 31;

/// The bit of a batch's `count u32` that says the batch ends in an xid trailer (D329 2b,
/// FORMAT §10.3): after the mutations, the xid's bytes and then its length (one byte,
/// 1–255), so a reader finds it from the end. The transaction a spanning commit's share
/// belongs to, which the change feed delivers with it. Written only by a database with
/// `incompat` bit 2 ([`INCOMPAT_EXTERNAL_PREPARE`]); a build without it reads the count as
/// over a billion mutations and reports the record corrupt.
pub const XID_BATCH: u32 = 1 << 30;

/// A batch count's flag bits ([`SILENT_BATCH`], [`XID_BATCH`]); the mutation count is the
/// rest ([`COUNT_MASK`]). Every reader of a count masks through it.
pub const COUNT_FLAGS: u32 = SILENT_BATCH | XID_BATCH;

/// A batch count's mutation count bits: everything below the flags.
pub const COUNT_MASK: u32 = !COUNT_FLAGS;

/// The `Corrupt` reason [`WalRecord::decode_with`] gives a feed-silent batch, or a Range
/// Drop, read without `incompat` bit 3 (D331): a reader that may have opened before the bit
/// was set asks again on this one only.
pub const SILENT_WITHOUT_BIT: &str = "wal record: a feed-silent batch without incompat bit 3";

/// Whether a record's batch carries the [`SILENT_BATCH`] mark, read at its place in the
/// record's bytes (FORMAT §10.3): a Batch's `count` ends at byte 21 (type, seqno and
/// timestamp before it), a Prepare's at byte 25 (and the coordinator). An External Prepare's
/// batch follows its variable-length share, so it's decoded (a rare record). A record too
/// short to hold its count isn't marked here: decoding then reports it.
#[inline(always)]
fn marked(bytes: &[u8]) -> bool {
    let top = |at: usize| bytes.get(at).is_some_and(|b| b & 0x80 != 0);
    match bytes.first() {
        Some(1) => top(20),
        Some(2) => top(24),
        Some(5) => external_marked(bytes),
        // A Range Drop needs the bit too (D331), and is refused without it alike.
        Some(7) => true,
        _ => false,
    }
}

/// The `Corrupt` reason [`WalRecord::decode_flags`] gives a batch with an xid trailer read
/// without `incompat` bit 2 (D329 2b): a stray bit is never read as a trailer.
pub const XID_WITHOUT_BIT: &str = "wal record: an xid-tagged batch without incompat bit 2";

/// Whether a record's batch has [`XID_BATCH`] set, at the same fixed places as [`marked`]
/// (the count's top byte, bit 6). An External Prepare's batch never may (its share names
/// its xid), which decoding refuses. Out of line: only a reader that may meet a stray bit
/// asks.
#[cold]
#[inline(never)]
fn xid_marked(bytes: &[u8]) -> bool {
    let bit = |at: usize| bytes.get(at).is_some_and(|b| b & 0x40 != 0);
    match bytes.first() {
        Some(1) => bit(20),
        Some(2) => bit(24),
        _ => false,
    }
}

/// [`marked`] for an External Prepare: its share decoded, then its batch's mark.
#[cold]
#[inline(never)]
fn external_marked(bytes: &[u8]) -> bool {
    WalRecord::decode_any(bytes).is_ok_and(|r| r.batch().is_some_and(|b| b.is_silent()))
}

/// [`SILENT_WITHOUT_BIT`]'s error, out of the decoder's way.
#[cold]
#[inline(never)]
fn silent_without_bit() -> Error {
    Error::Corrupt {
        what: SILENT_WITHOUT_BIT,
    }
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
    /// Incompatible features the segment's records use (format 4; 0 before): a build that
    /// does not know a bit refuses the segment ([`Error::UnsupportedFeature`]) rather than
    /// misread its records. No bit is defined yet.
    pub features: u32,
    /// The change-feed session that wrote this segment (format 4, bytes 76..84; D310), or 0
    /// for a session that fed nothing (and every segment of an older build, which writes zero
    /// there). Covered by the header's CRC, but never checked by [`SegmentHeader::decode`]:
    /// builds that do not know the field accept a marked segment and write zero in theirs,
    /// which ends the feed's history (FORMAT §10.2). A header before format 4 has no room
    /// for it and reads as 0.
    pub feed_session: u64,
}

/// The segment features this build knows (none yet).
pub const KNOWN_SEGMENT_FEATURES: u32 = 0;

impl SegmentHeader {
    /// Encodes into a frame-0 buffer (unused bytes zero): format 4's 128-byte layout for a
    /// version 4 header, the 60-byte one before (FORMAT §10.2).
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
        if self.version >= FormatVersion(4) {
            // 60..76: the per-database salt's slot (#176), zero; 76..84 the feed session
            // (D310); 84..124 reserved, zero.
            frame[56..60].copy_from_slice(&self.features.to_le_bytes());
            frame[FEED_SESSION_AT..FEED_SESSION_AT + 8]
                .copy_from_slice(&self.feed_session.to_le_bytes());
            let crc = crate::checksum::crc32c(&frame[..V4_CRC_AT]);
            frame[V4_CRC_AT..SEGMENT_HEADER_LEN].copy_from_slice(&crc.to_le_bytes());
        } else {
            let crc = crate::checksum::crc32c(&frame[..V3_CRC_AT]);
            frame[V3_CRC_AT..V3_CRC_AT + 4].copy_from_slice(&crc.to_le_bytes());
        }
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
        let crc_ok = |at: usize| crate::checksum::crc32c(&b[..at]) == le_u32(b, at);
        let version = FormatVersion(le_u32(b, 8));
        // The layout follows the version: 128 bytes from format 4, 60 before. A version this
        // build does not know is a newer build's header if either layout's CRC passes (a
        // newer segment must be refused, never skipped as torn, #496); otherwise torn.
        let intact = if version > FormatVersion::CURRENT {
            crc_ok(V4_CRC_AT) || crc_ok(V3_CRC_AT)
        } else if version >= FormatVersion(4) {
            crc_ok(V4_CRC_AT)
        } else {
            crc_ok(V3_CRC_AT)
        };
        if !intact {
            return Err(Error::Checksum { what: WHAT });
        }
        version.check(WHAT)?;
        let (features, feed_session) = if version >= FormatVersion(4) {
            (le_u32(b, 56), le_u64(b, FEED_SESSION_AT))
        } else {
            (0, 0)
        };
        if features & !KNOWN_SEGMENT_FEATURES != 0 {
            return Err(Error::UnsupportedFeature {
                what: "wal segment feature bits",
                value: u64::from(features & !KNOWN_SEGMENT_FEATURES),
            });
        }
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
            features,
            feed_session,
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

/// Size of the meaningful part of a segment header (the rest of frame 0 is zero): format 4's
/// 128 bytes; a header before format 4 uses the first 60, the rest zero.
pub const SEGMENT_HEADER_LEN: usize = 128;
/// Where the CRC32C sits (over the bytes before it) in a format 4 header.
const V4_CRC_AT: usize = 124;
/// Where a format 4 header keeps its feed session (D310).
const FEED_SESSION_AT: usize = 76;
/// Where it sits in a header before format 4.
const V3_CRC_AT: usize = 56;

/// A fragment is only started where more than a header's worth of bytes remains, so every
/// fragment except an empty record's carries payload. Shorter frame tails are skipped.
const MIN_FRAGMENT_ROOM: usize = FRAGMENT_HEADER_LEN + 1;

/// The smallest unit [`FrameEncoder::pad`] aligns to: room for a pad's header twice over.
const MIN_PAD_UNIT: usize = 32;

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
        self.encode_parts(&[record], out)
    }

    /// [`FrameEncoder::encode`] for a record given as consecutive `parts` (a record's head and
    /// its batch, [`WalRecord::split`]), so a caller frames it without first copying it into
    /// one buffer (#320). The bytes appended are those of encoding the parts' concatenation.
    pub fn encode_parts(&mut self, parts: &[&[u8]], out: &mut Vec<u8>) -> u64 {
        let mut left: usize = parts.iter().map(|p| p.len()).sum();
        // The next byte to frame: `parts[part][off]`.
        let (mut part, mut off) = (0, 0);
        let mut first = true;
        loop {
            let room = frame_room(self.pos);
            if room < MIN_FRAGMENT_ROOM {
                out.resize(out.len() + room, 0);
                self.pos += room as u64;
                continue;
            }
            let n = left.min(room - FRAGMENT_HEADER_LEN);
            let last = n == left;
            let mut header = [0u8; FRAGMENT_HEADER_LEN];
            header[4..8].copy_from_slice(&self.epoch.to_le_bytes());
            header[8..10].copy_from_slice(&(n as u16).to_le_bytes());
            header[10] = fragment_type(first, last) as u8;
            let at = out.len();
            out.extend_from_slice(&header);
            let mut take = n;
            while take > 0 {
                let p = &parts[part][off..];
                let k = p.len().min(take);
                out.extend_from_slice(&p[..k]);
                take -= k;
                off += k;
                if off == parts[part].len() {
                    part += 1;
                    off = 0;
                }
            }
            // One checksum over the fragment as written (header after the CRC, then payload),
            // not one per part: a record's head is a few bytes, and each call has a set-up
            // cost.
            let crc = crate::checksum::crc32c(&out[at + 4..]);
            out[at..at + 4].copy_from_slice(&crc.to_le_bytes());
            self.pos += (FRAGMENT_HEADER_LEN + n) as u64;
            left -= n;
            first = false;
            if last {
                return self.pos;
            }
        }
    }

    /// Bytes [`pad`](Self::pad) would append to start the next record at a multiple of `unit`
    /// (0 when it already would). See [`pad`](Self::pad) for `unit`'s bounds.
    pub fn pad_len(&self, unit: usize) -> usize {
        assert!(
            unit.is_power_of_two() && (MIN_PAD_UNIT..=FRAME_SIZE).contains(&unit),
            "a pad unit is a power of two from {MIN_PAD_UNIT} to a frame, not {unit}"
        );
        let room = frame_room(self.pos);
        // A frame tail too short for a fragment: the next record starts the next frame,
        // which is aligned already.
        if room < MIN_FRAGMENT_ROOM {
            return 0;
        }
        let gap = match (self.pos % unit as u64) as usize {
            0 => return 0,
            r => unit - r,
        };
        // A pad needs its header: a gap too short for one pads to the boundary after. `unit`
        // divides a frame, so a boundary short of the frame's end has a whole unit after it.
        if gap < FRAGMENT_HEADER_LEN {
            gap + unit
        } else {
            gap
        }
    }

    /// Appends a [`PAD_FRAGMENT`] (D327) so the next record starts at a multiple
    /// of `unit` bytes of the segment, and returns the bytes appended (0 when it already
    /// would). `unit` is a power of two from 32 bytes to a frame, so a pad never crosses a
    /// frame. Call it only directly before encoding a record into the same buffer: a pad is
    /// never the last thing written, so the segment's stop offset and `prev_end` stay the end
    /// of the last record (FORMAT §10.2).
    pub fn pad(&mut self, unit: usize, out: &mut Vec<u8>) -> usize {
        let n = self.pad_len(unit);
        if n == 0 {
            return 0;
        }
        let payload = n - FRAGMENT_HEADER_LEN;
        let mut header = [0u8; FRAGMENT_HEADER_LEN];
        header[4..8].copy_from_slice(&self.epoch.to_le_bytes());
        header[8..10].copy_from_slice(&(payload as u16).to_le_bytes());
        header[10] = PAD_FRAGMENT;
        let at = out.len();
        out.extend_from_slice(&header);
        out.resize(at + n, 0);
        let crc = crate::checksum::crc32c(&out[at + 4..]);
        out[at..at + 4].copy_from_slice(&crc.to_le_bytes());
        self.pos += n as u64;
        n
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

/// What [`FrameDecoder::decode_within`] found: what [`decode`](FrameDecoder::decode) would
/// return, or that the next fragment reaches past the frame's fresh bytes.
#[derive(Debug, PartialEq, Eq)]
pub enum Within {
    /// As [`FrameDecoder::decode`] returns, decided on fresh bytes only.
    Decoded(Option<Decoded>),
    /// The next fragment's header or payload reaches past the fresh bytes: call again, with
    /// the same frame and more of it fresh. Nothing about that fragment was consumed or
    /// decided.
    More,
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
        match self.decode_within(frame, FRAME_SIZE)? {
            Within::Decoded(d) => Ok(d),
            Within::More => unreachable!("a whole frame is fresh"),
        }
    }

    /// [`decode`](Self::decode) where only the frame's first `fresh` bytes may be trusted:
    /// the rest may predate the writer (a feed cursor that re-read only the tail past where
    /// it stopped, #1901). Nothing is decided on a byte at or past `fresh`. A fragment whose
    /// header or payload reaches there returns [`Within::More`] with the decoder as it was
    /// before that fragment, and a call with more of the frame fresh goes on from it. The
    /// skipped frame tail (12 bytes or fewer) needs no fresh bytes. With `fresh` the whole
    /// frame or more, this is `decode`.
    pub fn decode_within(&mut self, frame: &[u8], fresh: usize) -> crate::Result<Within> {
        if frame.len() != FRAME_SIZE {
            return Err(Error::Corrupt {
                what: "wal frame length",
            });
        }
        if self.stopped {
            return Ok(Within::Decoded(Some(Decoded::Stop {
                offset: self.record_end,
            })));
        }
        loop {
            let at = (self.pos - self.frame_start) as usize;
            if at + MIN_FRAGMENT_ROOM > FRAME_SIZE {
                self.frame_start += FRAME_SIZE as u64;
                self.pos = self.frame_start;
                return Ok(Within::Decoded(None));
            }
            let h = &frame[at..at + FRAGMENT_HEADER_LEN];
            let len = usize::from(u16::from_le_bytes([h[8], h[9]]));
            let end = at + FRAGMENT_HEADER_LEN + len;
            // One compare on the usual path: `end` is at least the header's end, so a fragment
            // wholly fresh passes both checks. Past `fresh`, a header not wholly fresh is never
            // read; a fresh one's payload must be too before anything that reads it is decided,
            // but a header of another epoch, or one running past the frame, is a stop on its own
            // bytes.
            if end > fresh
                && (at + FRAGMENT_HEADER_LEN > fresh
                    || (le_u32(h, 4) == self.epoch && end <= FRAME_SIZE))
            {
                return Ok(Within::More);
            }
            let ty = h[10];
            let sequenced = match ty {
                // A pad (D327) sits only between records, never inside one.
                1 | 2 | 5 => self.partial_start.is_none(),
                3 | 4 => self.partial_start.is_some(),
                _ => false,
            };
            let intact = le_u32(h, 4) == self.epoch
                && end <= FRAME_SIZE
                && crate::checksum::crc32c_append(
                    crate::checksum::crc32c(&h[4..]),
                    &frame[at + FRAGMENT_HEADER_LEN..end],
                ) == le_u32(h, 0);
            // A fragment of this segment's epoch whose CRC passes is real data, not a torn
            // tail: a type this build does not know means a newer build wrote it (#496).
            // Zeroed and recycled frames fail the epoch or the CRC and stay a stop.
            if intact && !(1..=5).contains(&ty) {
                return Err(Error::UnsupportedFeature {
                    what: "wal fragment type",
                    value: u64::from(ty),
                });
            }
            if !sequenced || !intact {
                self.stopped = true;
                return Ok(Within::Decoded(Some(Decoded::Stop {
                    offset: self.record_end,
                })));
            }
            let start = self.pos;
            let payload = &frame[at + FRAGMENT_HEADER_LEN..end];
            self.pos += (FRAGMENT_HEADER_LEN + len) as u64;
            // A pad is stepped over: the stop offset stays the end of the last record, so a
            // pad whose record was torn off is not counted as data.
            if ty == PAD_FRAGMENT {
                continue;
            }
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
                return Ok(Within::Decoded(Some(Decoded::Record { offset })));
            }
        }
    }

    /// The last complete record.
    pub fn record(&self) -> &[u8] {
        &self.record
    }
}

/// Record type byte. Numbers are frozen. Non-exhaustive: a later format may add record
/// types (ICR 0046).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub enum RecordType {
    /// A single-shard commit.
    Batch = 1,
    /// A participant's share of a cross-shard commit.
    Prepare = 2,
    /// The coordinator's decision for a cross-shard commit.
    Commit = 3,
    /// The coordinator's decision, naming the seqno and timestamp the commit applies at
    /// (D263).
    Decision = 4,
    /// A participant's share of a transaction prepared for an external coordinator, with its
    /// xid, every participant stream and its read set (D329, `incompat` bit 2).
    ExternalPrepare = 5,
    /// An externally prepared transaction aborted, at a seqno of its own (D329).
    ExternalAbort = 6,
    /// A table's key range dropped, at a seqno of its own (D331, `incompat` bit 3).
    RangeDrop = 7,
}

/// A decoded WAL record, borrowing its batch bytes. Non-exhaustive: a later format may add
/// record types (ICR 0046).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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
    /// Cross-shard decision (D263): every PREPARE whose id is `seqno` is
    /// committed, applied at `apply_seqno` with `commit_ts` for mutations without a timestamp.
    /// The id is a seqno reserved but never applied; `apply_seqno` is reserved at the decision.
    Decision {
        /// The commit's id (the PREPAREs' seqno field).
        seqno: Seqno,
        /// The seqno the commit applies at.
        apply_seqno: Seqno,
        /// The commit timestamp.
        commit_ts: Timestamp,
        /// Participant streams.
        participants: StreamList<'a>,
    },
    /// A share of a transaction prepared for an external coordinator (D329): like
    /// [`WalRecord::Prepare`], but in doubt until a [`WalRecord::Decision`] (commit) or a
    /// [`WalRecord::ExternalAbort`] on the `coordinator` stream decides it, across a crash
    /// and a reopen. Its read set is logged, so a reopen holds the rows it read again.
    ExternalPrepare {
        /// The commit's id, reserved by the coordinator.
        seqno: Seqno,
        /// The stream that holds the decision.
        coordinator: StreamId,
        /// The xid, the participants and the read set, then this participant's mutations.
        share: ExternalShare<'a>,
    },
    /// An externally prepared transaction aborted (D329): its PREPAREs are dropped. Written
    /// on the coordinator's stream, at a seqno of its own (`abort_seqno`), so a feed's
    /// frontier passes it and an idle reader looks again.
    ExternalAbort {
        /// The commit's id (the PREPAREs' seqno field).
        seqno: Seqno,
        /// The seqno the abort was reserved at: nothing applies there.
        abort_seqno: Seqno,
        /// The xid and the participants (no read set, no mutations).
        share: ExternalShare<'a>,
    },
    /// A table's key range dropped (D331, ICR 0141; `incompat` bit 3): every entry of the
    /// range at or below `seqno` is gone, from the memtables and the SSTs alike, with no
    /// delete markers, so a later write of the range at an older timestamp is visible. A
    /// range move's source writes it at its cleanup, and its target at an abort. Written on
    /// every stream at one seqno, which nothing else applies at; a replica runs the same drop
    /// on its own tablets. Feed-silent.
    RangeDrop {
        /// The drop's seqno.
        seqno: Seqno,
        /// The table and the range.
        range: DroppedRange<'a>,
    },
}

/// A [`WalRecord::RangeDrop`]'s range (FORMAT §10.3): table `u32 LE`, the range's first
/// row (varint-length bytes), then the end: `0` for the table's end, or `1` followed by
/// the row after the range (varint-length bytes). Rows are at most [`MAX_KEY_PART`], and a
/// bounded end is after the start. Kept as its body, read on access, so the record stays
/// [`WalRecord`]'s size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DroppedRange<'a> {
    body: &'a [u8],
}

/// A [`DroppedRange`]'s fields: the table, the range's first row, and the row after it
/// (`None`: the table's end).
pub type RangeFields<'a> = (TableId, &'a [u8], Option<&'a [u8]>);

impl<'a> DroppedRange<'a> {
    /// Encodes a range into `out`. `InvalidArgument` for a row over [`MAX_KEY_PART`] or an
    /// end at or before the start.
    pub fn encode(
        out: &mut Vec<u8>,
        table: TableId,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> crate::Result<()> {
        if start.len() > MAX_KEY_PART || end.is_some_and(|e| e.len() > MAX_KEY_PART) {
            return Err(Error::KeyTooLarge);
        }
        if end.is_some_and(|e| e <= start) {
            return Err(Error::InvalidArgument {
                what: "a dropped range's end is after its start",
            });
        }
        out.extend_from_slice(&table.0.to_le_bytes());
        crate::varint::put_bytes(out, start);
        match end {
            None => out.push(0),
            Some(e) => {
                out.push(1);
                crate::varint::put_bytes(out, e);
            }
        }
        Ok(())
    }

    /// Decodes and checks a body. Never panics. Out of line, so the record decoder around it
    /// keeps its inlining (#2089's callgrind).
    #[inline(never)]
    pub fn decode(bytes: &'a [u8]) -> crate::Result<Self> {
        Self::parse(bytes)?;
        Ok(Self { body: bytes })
    }

    fn parse(bytes: &'a [u8]) -> crate::Result<RangeFields<'a>> {
        let corrupt = |what| Error::Corrupt { what };
        let mut r = Reader::new(bytes, "wal range drop");
        let table = TableId(r.u32()?);
        let start = r.bytes()?;
        let end = match r.u8()? {
            0 => None,
            1 => Some(r.bytes()?),
            _ => return Err(corrupt("wal range drop end flag")),
        };
        if r.remaining() != 0 {
            return Err(corrupt("wal range drop length"));
        }
        if start.len() > MAX_KEY_PART || end.is_some_and(|e| e.len() > MAX_KEY_PART) {
            return Err(corrupt("wal range drop row length"));
        }
        if end.is_some_and(|e| e <= start) {
            return Err(corrupt("wal range drop bounds"));
        }
        Ok((table, start, end))
    }

    /// The fields: table, first row, and the row after the range (`None`: the table's end).
    pub fn fields(&self) -> RangeFields<'a> {
        Self::parse(self.body).expect("checked when decoded")
    }

    /// The encoded body, re-encoded unchanged by [`WalRecord::split`].
    pub fn as_bytes(&self) -> &'a [u8] {
        self.body
    }
}

/// An external share's encoded fields (D329, FORMAT §10.3): `xid` (varint-length bytes),
/// `participants` (a [`StreamList`], length-prefixed by a `u32 LE`), the read set (`count
/// u32 LE`, then each read as table `u32 LE`, family `u32 LE`, row as varint-length bytes,
/// and a qualifier flag `u8`, 0 for a whole family or 1 followed by varint-length bytes),
/// then, in an [`WalRecord::ExternalPrepare`], the batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExternalShare<'a> {
    /// The whole encoded body, checked by [`ExternalShare::decode`]; the fields are read
    /// from it on access. Only the body is kept: every [`WalRecord`] is sized for its largest
    /// variant, and a share's decoded fields grew it from 48 to 120 bytes, which every
    /// commit's record paid for (#2000's callgrind).
    body: &'a [u8],
    /// Whether the body ends with a batch ([`WalRecord::ExternalPrepare`]).
    with_batch: bool,
}

// Every WAL record a commit builds is a `WalRecord`, sized for its largest variant: a variant
// that grows it costs every commit (#2000's callgrind). A growth fails the build here instead.
#[cfg(target_pointer_width = "64")]
const _: () = assert!(core::mem::size_of::<WalRecord<'static>>() == 48);

/// An [`ExternalShare`]'s fields, read from its body.
struct ShareFields<'a> {
    xid: &'a [u8],
    participants: StreamList<'a>,
    reads: &'a [u8],
    read_count: u32,
    batch: Option<BatchRef<'a>>,
}

/// A read in an [`ExternalShare`]'s read set: a cell (`qualifier` set) or a whole family
/// of a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExternalRead<'a> {
    /// The table.
    pub table: TableId,
    /// The family.
    pub family: FamilyId,
    /// The row.
    pub row: &'a [u8],
    /// The cell's qualifier, or `None` for the whole family.
    pub qualifier: Option<&'a [u8]>,
}

impl<'a> ExternalShare<'a> {
    /// The fields, read again from the body [`decode`](Self::decode) checked.
    fn fields(&self) -> ShareFields<'a> {
        Self::parse(self.body, self.with_batch).expect("checked by ExternalShare::decode")
    }

    /// The transaction's external id.
    pub fn xid(&self) -> &'a [u8] {
        self.fields().xid
    }

    /// Every participant stream of the transaction, the coordinator's included.
    pub fn participants(&self) -> StreamList<'a> {
        self.fields().participants
    }

    /// The batch: `Some` in an [`WalRecord::ExternalPrepare`], `None` in an abort.
    pub fn batch(&self) -> Option<BatchRef<'a>> {
        self.fields().batch
    }

    /// The reads this participant validated, in order.
    pub fn reads(&self) -> impl Iterator<Item = crate::Result<ExternalRead<'a>>> + 'a {
        let ShareFields {
            reads, read_count, ..
        } = self.fields();
        let mut r = Reader::new(reads, "wal external read set");
        (0..read_count).map(move |_| {
            let table = TableId(r.u32()?);
            let family = FamilyId(r.u32()?);
            let row = r.bytes()?;
            let qualifier = match r.u8()? {
                0 => None,
                1 => Some(r.bytes()?),
                _ => {
                    return Err(Error::Corrupt {
                        what: "wal external read qualifier flag",
                    });
                }
            };
            Ok(ExternalRead {
                table,
                family,
                row,
                qualifier,
            })
        })
    }

    /// Encodes a share's fields into `out`: `xid`, `participants`, `reads`, then `batch`'s
    /// bytes if it has one. The result is a record body for [`ExternalShare::decode`].
    pub fn encode<'r>(
        xid: &[u8],
        participants: &[StreamId],
        reads: impl ExactSizeIterator<Item = ExternalRead<'r>>,
        batch: Option<BatchRef<'_>>,
        out: &mut Vec<u8>,
    ) -> crate::Result<()> {
        if xid.is_empty() || xid.len() > MAX_KEY_PART {
            return Err(Error::InvalidArgument {
                what: "an external xid is 1 to MAX_KEY_PART bytes",
            });
        }
        crate::varint::put_bytes(out, xid);
        let mut list = Vec::with_capacity(2 + 4 * participants.len());
        StreamList::encode(participants, &mut list)?;
        out.extend_from_slice(&(list.len() as u32).to_le_bytes());
        out.extend_from_slice(&list);
        let count = u32::try_from(reads.len()).map_err(|_| Error::InvalidArgument {
            what: "an external share's read set is at most u32::MAX reads",
        })?;
        out.extend_from_slice(&count.to_le_bytes());
        for read in reads {
            out.extend_from_slice(&read.table.0.to_le_bytes());
            out.extend_from_slice(&read.family.0.to_le_bytes());
            crate::varint::put_bytes(out, read.row);
            match read.qualifier {
                None => out.push(0),
                Some(q) => {
                    out.push(1);
                    crate::varint::put_bytes(out, q);
                }
            }
        }
        if let Some(b) = batch {
            out.extend_from_slice(b.as_bytes());
        }
        Ok(())
    }

    /// Decodes a share's fields from a record body; `with_batch` for an
    /// [`WalRecord::ExternalPrepare`], whose batch is the rest. Never panics.
    pub fn decode(bytes: &'a [u8], with_batch: bool) -> crate::Result<Self> {
        Self::parse(bytes, with_batch)?;
        Ok(Self {
            body: bytes,
            with_batch,
        })
    }

    /// Reads and checks a body's fields.
    fn parse(bytes: &'a [u8], with_batch: bool) -> crate::Result<ShareFields<'a>> {
        let mut r = Reader::new(bytes, "wal external share");
        let xid = r.bytes()?;
        let list_len = r.u32()? as usize;
        let participants = StreamList::new(r.take(list_len)?)?;
        let read_count = r.u32()?;
        let start = r.pos();
        // Walk the reads once to find where they end (and check them).
        for _ in 0..read_count {
            r.u32()?;
            r.u32()?;
            r.bytes()?;
            match r.u8()? {
                0 => {}
                1 => {
                    r.bytes()?;
                }
                _ => {
                    return Err(Error::Corrupt {
                        what: "wal external read qualifier flag",
                    });
                }
            }
        }
        let reads = &bytes[start..r.pos()];
        let batch = if with_batch {
            let b = BatchRef::new(r.rest())?;
            // An External Prepare's batch never carries an xid trailer: its share names the
            // xid (D329 2b; FORMAT §10.3). Refused here, on every read (feed1248's must on
            // #2076: `xid_marked` doesn't look inside a share).
            if b.xid().is_some() {
                return Err(Error::Corrupt {
                    what: "wal external prepare: an xid trailer on its batch",
                });
            }
            Some(b)
        } else if r.remaining() != 0 {
            return Err(Error::Corrupt {
                what: "wal external abort length",
            });
        } else {
            None
        };
        Ok(ShareFields {
            xid,
            participants,
            reads,
            read_count,
            batch,
        })
    }

    /// The encoded body this share was decoded from, re-encoded unchanged by
    /// [`WalRecord::split`].
    pub fn as_bytes(&self) -> &'a [u8] {
        self.body
    }
}

/// The longest record head [`WalRecord::split`] writes: a DECISION's type, id, seqno and
/// commit timestamp.
pub const RECORD_HEAD_MAX: usize = 1 + 8 + 8 + 8;

impl WalRecord<'_> {
    /// Appends the record encoding.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let mut head = [0u8; RECORD_HEAD_MAX];
        let (head, body) = self.split(&mut head);
        out.extend_from_slice(head);
        out.extend_from_slice(body);
    }

    /// The record encoding as two parts, without copying the batch: its head (type, seqno,
    /// and the type's other fixed fields), written into `head`, then its body (the batch, or a
    /// COMMIT's participant list). [`FrameEncoder::encode_parts`] frames them as one record.
    pub fn split<'b>(&'b self, head: &'b mut [u8; RECORD_HEAD_MAX]) -> (&'b [u8], &'b [u8]) {
        let (len, body) = match self {
            WalRecord::Batch {
                seqno,
                commit_ts,
                batch,
            } => {
                head[0] = RecordType::Batch as u8;
                head[1..9].copy_from_slice(&seqno.to_le_bytes());
                head[9..17].copy_from_slice(&commit_ts.to_le_bytes());
                (17, batch.as_bytes())
            }
            WalRecord::Prepare {
                seqno,
                commit_ts,
                coordinator,
                batch,
            } => {
                head[0] = RecordType::Prepare as u8;
                head[1..9].copy_from_slice(&seqno.to_le_bytes());
                head[9..17].copy_from_slice(&commit_ts.to_le_bytes());
                head[17..21].copy_from_slice(&coordinator.0.to_le_bytes());
                (21, batch.as_bytes())
            }
            WalRecord::Commit {
                seqno,
                participants,
            } => {
                head[0] = RecordType::Commit as u8;
                head[1..9].copy_from_slice(&seqno.to_le_bytes());
                (9, participants.bytes)
            }
            WalRecord::Decision {
                seqno,
                apply_seqno,
                commit_ts,
                participants,
            } => {
                head[0] = RecordType::Decision as u8;
                head[1..9].copy_from_slice(&seqno.to_le_bytes());
                head[9..17].copy_from_slice(&apply_seqno.to_le_bytes());
                head[17..25].copy_from_slice(&commit_ts.to_le_bytes());
                (25, participants.bytes)
            }
            WalRecord::ExternalPrepare {
                seqno,
                coordinator,
                share,
            } => {
                head[0] = RecordType::ExternalPrepare as u8;
                head[1..9].copy_from_slice(&seqno.to_le_bytes());
                head[9..13].copy_from_slice(&coordinator.0.to_le_bytes());
                (13, share.as_bytes())
            }
            WalRecord::ExternalAbort {
                seqno,
                abort_seqno,
                share,
            } => {
                head[0] = RecordType::ExternalAbort as u8;
                head[1..9].copy_from_slice(&seqno.to_le_bytes());
                head[9..17].copy_from_slice(&abort_seqno.to_le_bytes());
                (17, share.as_bytes())
            }
            WalRecord::RangeDrop { seqno, range } => {
                head[0] = RecordType::RangeDrop as u8;
                head[1..9].copy_from_slice(&seqno.to_le_bytes());
                (9, range.as_bytes())
            }
        };
        (&head[..len], body)
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
    ///
    /// A batch marked [`SILENT_BATCH`] is refused (`Corrupt`), as a build without D331 reads
    /// it: use [`decode_with`](Self::decode_with) where the superblock has
    /// [`INCOMPAT_FEED_SILENT`].
    #[inline]
    pub fn decode(bytes: &[u8]) -> crate::Result<WalRecord<'_>> {
        Self::decode_with(bytes, false)
    }

    /// As [`decode`](Self::decode), taking a batch's [`SILENT_BATCH`] mark when `silent_ok`
    /// (the database's superblock has [`INCOMPAT_FEED_SILENT`], D331). Without it a marked
    /// batch is `Corrupt`, so a stray bit is never read as the mark.
    ///
    /// The mark is found at its fixed place in the record's bytes before the record is
    /// decoded, and the decoding itself is what it was before D331, so a record costs a byte
    /// test more (#2055's feed shapes).
    #[inline]
    pub fn decode_with(bytes: &[u8], silent_ok: bool) -> crate::Result<WalRecord<'_>> {
        if !silent_ok && marked(bytes) {
            return Err(silent_without_bit());
        }
        let record = Self::decode_any(bytes);
        // The mark's places in the bytes agree with the decoded batch (perf2's nit on #2055).
        debug_assert!(
            record
                .as_ref()
                .map_or(true, |r| matches!(r, WalRecord::RangeDrop { .. })
                    || r.batch().is_some_and(|b| b.is_silent()) == marked(bytes)),
            "a feed-silent mark at a place the decoder doesn't read"
        );
        record
    }

    /// [`decode_with`](Self::decode_with), refusing a batch's [`XID_BATCH`] trailer unless
    /// `xid_ok` (the superblock has [`INCOMPAT_EXTERNAL_PREPARE`], D329 2b): a reader that
    /// knows the database's bits passes both (recovery, the check tool), so a stray bit is
    /// never read as a trailer. An External Prepare's batch refuses the bit whatever
    /// `xid_ok` says.
    #[inline]
    pub fn decode_flags(
        bytes: &[u8],
        silent_ok: bool,
        xid_ok: bool,
    ) -> crate::Result<WalRecord<'_>> {
        // Only a reader that knows the database lacks bit 2 asks: the feed's cursor, the
        // hottest reader, goes straight to `decode_with` (#2076's feed shapes).
        if !xid_ok && xid_marked(bytes) {
            return Err(Error::Corrupt {
                what: XID_WITHOUT_BIT,
            });
        }
        Self::decode_with(bytes, silent_ok)
    }

    /// Whether the encoded record `bytes` carries a batch marked [`SILENT_BATCH`] (D331),
    /// read at the mark's place without decoding the record: a reader that doesn't take
    /// the mark yet tests it first, then decodes with [`decode_with`](Self::decode_with).
    #[inline(always)]
    pub fn is_marked(bytes: &[u8]) -> bool {
        marked(bytes)
    }

    /// The batch a record carries, if any: a Batch's, a Prepare's, or an external PREPARE's.
    pub fn batch(&self) -> Option<BatchRef<'_>> {
        match self {
            WalRecord::Batch { batch, .. } | WalRecord::Prepare { batch, .. } => Some(*batch),
            WalRecord::ExternalPrepare { share, .. } => share.batch(),
            WalRecord::Commit { .. }
            | WalRecord::Decision { .. }
            | WalRecord::ExternalAbort { .. }
            | WalRecord::RangeDrop { .. } => None,
        }
    }

    /// [`decode_any`](Self::decode_any)'s record of type `t`, `rest` the bytes after its
    /// type byte: a Range Drop (D331), or an error.
    #[cold]
    #[inline(never)]
    fn decode_other(t: u8, rest: &[u8]) -> crate::Result<WalRecord<'_>> {
        if t != RecordType::RangeDrop as u8 {
            return Err(Error::UnsupportedFeature {
                what: "wal record type",
                value: u64::from(t),
            });
        }
        let mut r = Reader::new(rest, "wal record");
        Ok(WalRecord::RangeDrop {
            seqno: r.u64()?,
            range: DroppedRange::decode(r.rest())?,
        })
    }

    fn decode_any(bytes: &[u8]) -> crate::Result<WalRecord<'_>> {
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
            4 => Ok(WalRecord::Decision {
                seqno: r.u64()?,
                apply_seqno: r.u64()?,
                commit_ts: r.u64()?,
                participants: StreamList::new(r.rest())?,
            }),
            5 => Ok(WalRecord::ExternalPrepare {
                seqno: r.u64()?,
                coordinator: StreamId(r.u32()?),
                share: ExternalShare::decode(r.rest(), true)?,
            }),
            6 => Ok(WalRecord::ExternalAbort {
                seqno: r.u64()?,
                abort_seqno: r.u64()?,
                share: ExternalShare::decode(r.rest(), false)?,
            }),
            // A Range Drop (D331, gated by its bit in `decode_with`), or a type this build
            // doesn't know: out of line, so the arms above inline as they did (#2089's
            // callgrind).
            t => Self::decode_other(t, r.rest()),
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
    /// A mark for the engine, not encoded: the batch holds a primary's logged mutations (a
    /// replica's apply, D315 PR 3). Here rather than beside the builder, so it sits in the
    /// padding after `count` and a batch moved by value on every commit doesn't grow.
    logged: bool,
    /// Feed-silent (D331): encoded, as [`SILENT_BATCH`] in the count.
    silent: bool,
}

impl Default for BatchBuilder {
    fn default() -> Self {
        Self {
            buf: vec![0; 4],
            count: 0,
            logged: false,
            silent: false,
        }
    }
}

/// Bit of `kind_flags` set when the mutation carries its own timestamp.
const EXPLICIT_TS: u8 = 0x80;

/// The rules every mutation obeys, checked when it is pushed and again when a batch is
/// decoded: row and qualifier at most [`MAX_KEY_PART`], value at most [`MAX_VALUE_LEN`],
/// deletes carry no value, and a family delete carries no qualifier. A shape violation is
/// reported with `shape`: `InvalidArgument` when pushing, `Corrupt` when decoding.
fn check_mutation(
    kind: Kind,
    row: &[u8],
    qualifier: &[u8],
    value_len: usize,
    shape: fn(&'static str) -> Error,
) -> crate::Result<()> {
    if row.len() > MAX_KEY_PART || qualifier.len() > MAX_KEY_PART {
        return Err(Error::KeyTooLarge);
    }
    if value_len as u64 > MAX_VALUE_LEN {
        return Err(Error::ValueTooLarge);
    }
    if (kind.is_delete() && value_len != 0) || (kind == Kind::FamilyDelete && !qualifier.is_empty())
    {
        return Err(shape("mutation: delete with value or qualifier"));
    }
    Ok(())
}

impl BatchBuilder {
    /// An empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// A builder holding a copy of `batch`'s mutations, as it was logged: a share replay
    /// reinstalls from its record (D329's in-doubt shares).
    pub fn from_batch(batch: BatchRef<'_>) -> Self {
        Self {
            buf: batch.as_bytes().to_vec(),
            count: batch.len() as u32,
            logged: false,
            silent: batch.is_silent(),
        }
    }

    /// An empty batch with room for about `bytes` of encoded mutations before it grows
    /// (one allocation, where [`BatchBuilder::new`] allocates its header and grows on the
    /// first push, #320).
    pub fn with_capacity(bytes: usize) -> Self {
        let mut buf = Vec::with_capacity(4 + bytes);
        buf.extend_from_slice(&[0; 4]);
        Self {
            buf,
            count: 0,
            logged: false,
            silent: false,
        }
    }

    /// Marks the batch as a primary's logged mutations (a replica's apply, D315 PR 3): a mark
    /// for the engine, not encoded, kept in the padding after the count.
    pub fn mark_logged(&mut self) {
        self.logged = true;
    }

    /// Whether [`mark_logged`](Self::mark_logged) marked it (since the last `clear`).
    pub fn is_logged(&self) -> bool {
        self.logged
    }

    /// Marks the batch feed-silent (D331, ICR 0141): encoded as [`SILENT_BATCH`] in its
    /// count, so its record carries the mark. Its writer sets [`INCOMPAT_FEED_SILENT`]
    /// before appending it.
    pub fn mark_silent(&mut self) {
        self.silent = true;
        self.write_count();
        if self.tagged() {
            // `write_count` wrote the count without the trailer's bit: put it back.
            self.buf[3] |= (XID_BATCH >> 24) as u8;
        }
    }

    /// Ends the batch with an xid trailer (D329 2b, [`XID_BATCH`]): the transaction a
    /// spanning commit's share belongs to. The batch's last step: a push after it is a bug
    /// (the trailer would sit inside the mutations), and the engine tags a share only once
    /// it's whole, so `push` needn't look. `InvalidArgument` for an empty xid or one over
    /// 255 bytes, or for a batch already tagged. Its writer sets `incompat` bit 2 first.
    pub fn tag_xid(&mut self, xid: &[u8]) -> crate::Result<()> {
        let len = u8::try_from(xid.len()).map_err(|_| Error::InvalidArgument {
            what: "an xid over 255 bytes",
        })?;
        if len == 0 || self.tagged() {
            return Err(Error::InvalidArgument {
                what: "an empty xid, or a batch already tagged",
            });
        }
        self.buf.extend_from_slice(xid);
        self.buf.push(len);
        self.buf[3] |= (XID_BATCH >> 24) as u8;
        Ok(())
    }

    /// Whether [`tag_xid`](Self::tag_xid) tagged it (since the last `clear`).
    /// Whether [`tag_xid`](Self::tag_xid) tagged it: one byte's test, for callers that copy
    /// the tag through a rebuild (D329 2b).
    #[inline]
    pub fn is_tagged(&self) -> bool {
        self.tagged()
    }

    #[inline]
    fn tagged(&self) -> bool {
        self.buf
            .get(3)
            .is_some_and(|b| b & (XID_BATCH >> 24) as u8 != 0)
    }

    /// Whether [`mark_silent`](Self::mark_silent) marked it (since the last `clear`).
    pub fn is_silent(&self) -> bool {
        self.silent
    }

    /// Writes the encoded count: the mutations, with the silent mark.
    #[inline]
    fn write_count(&mut self) {
        let mark = u32::from(self.silent) << 31;
        self.buf[..4].copy_from_slice(&(self.count | mark).to_le_bytes());
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
        self.push_parts(table, family, kind, row, qualifier, ts, &[], value)
    }

    /// As [`BatchBuilder::push`], for a stored value of `tag` followed by `payload` (a
    /// [`crate::value`] encoding), written straight into the batch: the caller needs no
    /// buffer holding the encoded value (#320).
    #[allow(clippy::too_many_arguments)]
    pub fn push_tagged(
        &mut self,
        table: TableId,
        family: FamilyId,
        kind: crate::Kind,
        row: &[u8],
        qualifier: &[u8],
        ts: Option<Timestamp>,
        tag: u8,
        payload: &[u8],
    ) -> crate::Result<()> {
        self.push_parts(table, family, kind, row, qualifier, ts, &[tag], payload)
    }

    /// Appends one mutation whose value is `head` followed by `tail`.
    #[allow(clippy::too_many_arguments)]
    fn push_parts(
        &mut self,
        table: TableId,
        family: FamilyId,
        kind: crate::Kind,
        row: &[u8],
        qualifier: &[u8],
        ts: Option<Timestamp>,
        head: &[u8],
        tail: &[u8],
    ) -> crate::Result<()> {
        // The xid trailer is the batch's last step (D329 2b): a push after it would land past
        // the trailer and clear bit 30 (`write_count`), silently corrupting the record
        // (perf2's S1 on #2076). Every engine path tags last; this catches any that doesn't.
        debug_assert!(!self.tagged(), "a push after the batch's xid trailer");
        let value_len = head.len() + tail.len();
        check_mutation(kind, row, qualifier, value_len, |what| {
            Error::InvalidArgument { what }
        })?;
        let count = self
            .count
            .checked_add(1)
            .filter(|&c| c & COUNT_FLAGS == 0)
            .ok_or(Error::ValueTooLarge)?;
        let b = &mut self.buf;
        // One growth for the whole mutation, not one per field.
        b.reserve(4 + 4 + 1 + 8 + 3 * 10 + row.len() + qualifier.len() + value_len);
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
        crate::varint::put_u64(b, value_len as u64);
        b.extend_from_slice(head);
        b.extend_from_slice(tail);
        self.count = count;
        self.write_count();
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

    /// The encoded batch. Valid as [`BatchRef::new`] would find it: every push checked its
    /// mutation before writing it, so a reader of this needs no validating decode.
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
        self.logged = false;
        self.silent = false;
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
    /// `BatchRef` built by this function never yields an error. A [`SILENT_BATCH`] mark is
    /// taken as the mark, so this is for batches built in memory: bytes read from a WAL file
    /// go through [`WalRecord::decode_with`] (or the WAL crate's readers), which refuse the
    /// mark without `incompat` bit 3, never straight through here (perf2's N2 on #2055).
    pub fn new(bytes: &'a [u8]) -> crate::Result<Self> {
        let mut r = Reader::new(bytes, "wal batch");
        let raw = r.u32()?;
        let count = raw & COUNT_MASK;
        let mut rest = r.rest();
        if raw & XID_BATCH != 0 {
            // The xid trailer (D329 2b): its length last, then the xid before it. The
            // mutations end where it begins, and anything else past them is still refused.
            let (&len, before) = rest.split_last().ok_or(Error::Corrupt {
                what: "wal batch: an xid trailer without its length",
            })?;
            let len = usize::from(len);
            if len == 0 || before.len() < len {
                return Err(Error::Corrupt {
                    what: "wal batch: a malformed xid trailer",
                });
            }
            rest = &before[..before.len() - len];
        }
        let mut it = BatchIter {
            rest,
            remaining: count,
            validate: true,
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

    /// The count's top byte, where its flags live ([`SILENT_BATCH`] as 0x80, [`XID_BATCH`]
    /// as 0x40): a reader that tests both reads it once (#2076's feed shapes).
    #[inline]
    pub fn count_flags(&self) -> u8 {
        self.bytes.get(3).copied().unwrap_or(0)
    }

    /// Whether the batch is feed-silent ([`SILENT_BATCH`], D331): read from its count's
    /// top byte, so a `BatchRef` stays two words and a count (#2055's feed shapes).
    #[inline]
    pub fn is_silent(&self) -> bool {
        self.bytes.get(3).is_some_and(|b| b & 0x80 != 0)
    }

    /// The transaction this batch is a share of ([`XID_BATCH`], D329 2b): its trailer, read
    /// from the end. `None` for an untagged batch: one bit test.
    #[inline]
    pub fn xid(&self) -> Option<&'a [u8]> {
        if self
            .bytes
            .get(3)
            .is_none_or(|b| b & (XID_BATCH >> 24) as u8 == 0)
        {
            return None;
        }
        xid_trailer(self.bytes)
    }

    /// Iterates the mutations in insertion order.
    pub fn iter(&self) -> BatchIter<'a> {
        BatchIter {
            rest: self.bytes.get(4..).unwrap_or_default(),
            remaining: self.count,
            validate: false,
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
    /// Whether each mutation's fields are checked: only in [`BatchRef::new`]'s pass. A
    /// `BatchRef` exists only once its bytes passed that check (or were built by
    /// [`BatchBuilder`], which checks each mutation as it is pushed), so its own iterators
    /// skip it.
    validate: bool,
}

impl<'a> BatchIter<'a> {
    fn parse(&mut self) -> crate::Result<Mutation<'a>> {
        // Decoded without a `Reader`: one bounds check for the fixed fields, and the
        // length-prefixed parts taken inline (a batch is decoded on every commit's path).
        // The errors are the same.
        let truncated = || Error::Truncated { what: MUTATION };
        let Some((head, mut rest)) = self.rest.split_first_chunk::<9>() else {
            return Err(truncated());
        };
        let table = TableId(u32::from_le_bytes([head[0], head[1], head[2], head[3]]));
        let family = FamilyId(u32::from_le_bytes([head[4], head[5], head[6], head[7]]));
        let flags = head[8];
        let kind = Kind::from_u8(flags & !EXPLICIT_TS)?;
        let ts = if flags & EXPLICIT_TS != 0 {
            let Some((ts, after)) = rest.split_first_chunk::<8>() else {
                return Err(truncated());
            };
            rest = after;
            Some(u64::from_le_bytes(*ts))
        } else {
            None
        };
        let m = Mutation {
            table,
            family,
            kind,
            ts,
            row: take_bytes(&mut rest)?,
            qualifier: take_bytes(&mut rest)?,
            value: take_bytes(&mut rest)?,
        };
        if self.validate {
            check_mutation(m.kind, m.row, m.qualifier, m.value.len(), |what| {
                Error::Corrupt { what }
            })?;
        }
        self.rest = rest;
        Ok(m)
    }
}

/// What a batch's decode errors name.
const MUTATION: &str = "wal mutation";

/// Takes a varint-length-prefixed part off the front of `rest`; a length below 128 (one
/// byte) is read inline. A short part is `Truncated` and a bad length `Corrupt`, both
/// naming the mutation, as `Reader::bytes` reports them.
#[inline]
fn take_bytes<'a>(rest: &mut &'a [u8]) -> crate::Result<&'a [u8]> {
    let (len, n) = match rest.first() {
        Some(&b) if b < 0x80 => (usize::from(b), 1),
        _ => {
            let (len, n) = crate::varint::get_u64(rest).map_err(|e| match e {
                Error::Truncated { .. } => Error::Truncated { what: MUTATION },
                _ => Error::Corrupt { what: MUTATION },
            })?;
            let len = usize::try_from(len).map_err(|_| Error::Truncated { what: MUTATION })?;
            (len, n)
        }
    };
    let body = &rest[n..];
    if len > body.len() {
        return Err(Error::Truncated { what: MUTATION });
    }
    let (part, after) = body.split_at(len);
    *rest = after;
    Ok(part)
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

    /// Appends `streams` in the same encoding. Fails with
    /// [`Error::InvalidArgument`] (writing nothing) if there are more than `u16::MAX`.
    pub fn encode(streams: &[StreamId], out: &mut Vec<u8>) -> crate::Result<()> {
        if streams.len() > usize::from(u16::MAX) {
            return Err(Error::InvalidArgument {
                what: "a COMMIT record lists at most 65535 participant streams",
            });
        }
        out.extend_from_slice(&(streams.len() as u16).to_le_bytes());
        for s in streams {
            out.extend_from_slice(&s.0.to_le_bytes());
        }
        Ok(())
    }

    /// Iterates the stream ids.
    pub fn iter(&self) -> impl Iterator<Item = StreamId> + 'a {
        let (ids, _) = self.bytes[2..].as_chunks::<4>();
        ids.iter().map(|c| StreamId(u32::from_le_bytes(*c)))
    }
}

/// The xid trailer of encoded batch `bytes` whose count has [`XID_BATCH`]: out of line, as
/// only spanning commits' shares have one.
#[cold]
#[inline(never)]
fn xid_trailer(bytes: &[u8]) -> Option<&[u8]> {
    let (&len, before) = bytes.get(4..)?.split_last()?;
    let len = usize::from(len);
    before.get(before.len().checked_sub(len)?..)
}
