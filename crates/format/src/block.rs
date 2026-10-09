//! Blocks: the unit of I/O, caching, compression and checksumming inside an SST.
//!
//! A *physical* block is `payload ++ trailer` where the trailer (16 bytes) records the block
//! kind, codec, uncompressed length and an xxh3-64 checksum. The *logical* (uncompressed)
//! block for data and index blocks is:
//!
//! ```text
//! entry*  restart_offset u32*R  row_start_offset u32*S  R u32  S u32
//! entry = shared varint | unshared varint | value_len varint | key[shared..] | value
//! ```
//!
//! Restart entries (every `restart_interval` entries) store the full key. The row-start
//! table lists the offset of the first entry of every row that begins in the block, so a scan
//! can skip the rest of a row with a binary search instead of decoding cells. See `FORMAT.md` §4.
//!
//! ```
//! use pigeonhole_format::Cursor;
//! use pigeonhole_format::block::{Block, BlockBuilder};
//! use pigeonhole_format::key::{Kind, encode_key};
//!
//! let mut b = BlockBuilder::data(16);
//! for row in [&b"a"[..], b"b", b"c"] {
//!     for q in [&b"x"[..], b"y"] {
//!         let mut k = Vec::new();
//!         encode_key(&mut k, row, q, 1, 1, Kind::Put).unwrap();
//!         b.add(&k, b"\x00v").unwrap();
//!     }
//! }
//! let block = Block::new(b.finish().to_vec()).unwrap();
//! assert_eq!(block.row_start_count(), 3);
//! let mut it = block.into_cursor();
//! it.seek_to_first().unwrap();
//! it.skip_row().unwrap(); // jumps from row a to row b without decoding a's second cell
//! assert!(it.key().starts_with(b"b\x00\x01"));
//! ```

use std::ops::Deref;

use crate::Error;
use crate::bytes::le_u32;
use crate::compress::Compression;
use crate::cursor::Cursor;

/// Default restart interval for data blocks.
pub const DEFAULT_RESTART_INTERVAL: usize = 16;

/// Default target size of an uncompressed data block (16 KiB).
pub const DEFAULT_BLOCK_SIZE: usize = 16 * 1024;

/// Size of the physical block trailer.
pub const TRAILER_LEN: usize = 16;

/// Largest logical block accepted (FORMAT §4.1): twice the largest extent, so a block holding
/// one maximal cell always fits, while a crafted trailer can never force a huge allocation.
const MAX_BLOCK_LEN: usize = 128 << 20;

/// LZ4 cannot expand input by more than this factor, which bounds a believable
/// `uncompressed_len` by the payload length.
const MAX_LZ4_RATIO: usize = 255;

/// What a block holds. Numbers are frozen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum BlockKind {
    /// Cells: internal key to stored value.
    Data = 1,
    /// An index partition: separator key to data-block [`BlockAddr`].
    Index = 2,
    /// The top-level index: separator key to index-partition [`BlockAddr`].
    TopIndex = 3,
    /// A filter (see [`crate::filter`]).
    Filter = 4,
    /// SST properties (see [`crate::sst::Properties`]).
    Properties = 5,
}

/// Where a physical block lives inside an SST: offset from the SST start and length
/// including the trailer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct BlockAddr {
    /// Byte offset from the start of the SST.
    pub offset: u64,
    /// Physical length in bytes, trailer included.
    pub len: u32,
}

impl BlockAddr {
    /// Appends the varint encoding used as an index-entry value.
    pub fn encode_varint(&self, out: &mut Vec<u8>) {
        crate::varint::put_u64(out, self.offset);
        crate::varint::put_u64(out, u64::from(self.len));
    }

    /// Decodes the varint encoding.
    pub fn decode_varint(input: &[u8]) -> crate::Result<Self> {
        let (offset, n) = crate::varint::get_u64(input)?;
        let (len, m) = crate::varint::get_u32(&input[n..])?;
        if n + m != input.len() {
            return Err(Error::Corrupt {
                what: "block address",
            });
        }
        Ok(Self { offset, len })
    }
}

/// The decoded 16-byte trailer of a physical block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockTrailer {
    /// Block kind.
    pub kind: BlockKind,
    /// Codec of the payload.
    pub compression: Compression,
    /// Length of the logical block after decompression.
    pub uncompressed_len: u32,
    /// xxh3-64 of the payload followed by the first 8 trailer bytes.
    pub checksum: u64,
}

impl BlockKind {
    fn from_u8(b: u8) -> crate::Result<Self> {
        match b {
            1 => Ok(Self::Data),
            2 => Ok(Self::Index),
            3 => Ok(Self::TopIndex),
            4 => Ok(Self::Filter),
            5 => Ok(Self::Properties),
            _ => Err(Error::Corrupt { what: "block kind" }),
        }
    }
}

/// Compresses `logical`, appends `payload ++ trailer` to `out`, and returns the trailer.
///
/// ```
/// use pigeonhole_format::block::{BlockKind, seal, verify};
/// use pigeonhole_format::compress::Compression;
///
/// let mut physical = Vec::new();
/// let trailer = seal(BlockKind::Data, Compression::Lz4, &[0u8; 1000], &mut physical).unwrap();
/// let (checked, payload) = verify(&physical).unwrap();
/// assert_eq!(checked, trailer);
/// assert_eq!(checked.uncompressed_len, 1000);
/// assert!(payload.len() < 1000);
/// ```
pub fn seal(
    kind: BlockKind,
    codec: Compression,
    logical: &[u8],
    out: &mut Vec<u8>,
) -> crate::Result<BlockTrailer> {
    seal_with_level(
        kind,
        codec,
        crate::compress::DEFAULT_ZSTD_LEVEL,
        logical,
        out,
    )
}

/// [`seal`] with a zstd compression level ([`crate::compress::compress_with_level`]).
pub fn seal_with_level(
    kind: BlockKind,
    codec: Compression,
    level: i8,
    logical: &[u8],
    out: &mut Vec<u8>,
) -> crate::Result<BlockTrailer> {
    if logical.len() > MAX_BLOCK_LEN {
        return Err(Error::ValueTooLarge);
    }
    let uncompressed_len = logical.len() as u32;
    let start = out.len();
    let compression = crate::compress::compress_with_level(codec, level, logical, out)?;
    out.extend_from_slice(&[kind as u8, compression as u8, 0, 0]);
    out.extend_from_slice(&uncompressed_len.to_le_bytes());
    let checksum = crate::checksum::xxh3_64(&out[start..]);
    out.extend_from_slice(&checksum.to_le_bytes());
    Ok(BlockTrailer {
        kind,
        compression,
        uncompressed_len,
        checksum,
    })
}

/// Verifies a physical block's checksum and returns its trailer and payload. Done on reads
/// from disk; skipped on cache hits.
pub fn verify(physical: &[u8]) -> crate::Result<(BlockTrailer, &[u8])> {
    let Some(payload_len) = physical.len().checked_sub(TRAILER_LEN) else {
        return Err(Error::Truncated { what: "block" });
    };
    let t = &physical[payload_len..];
    let checksum = crate::bytes::le_u64(t, 8);
    if crate::checksum::xxh3_64(&physical[..payload_len + 8]) != checksum {
        return Err(Error::Checksum { what: "block" });
    }
    let trailer = BlockTrailer {
        kind: BlockKind::from_u8(t[0])?,
        compression: Compression::from_u8(t[1])?,
        uncompressed_len: le_u32(t, 4),
        checksum,
    };
    let payload = &physical[..payload_len];
    let len = trailer.uncompressed_len as usize;
    let plausible = match trailer.compression {
        Compression::None => len == payload_len,
        _ => len <= payload_len.saturating_mul(MAX_LZ4_RATIO),
    };
    if !plausible || len > MAX_BLOCK_LEN {
        return Err(Error::Corrupt {
            what: "uncompressed block length",
        });
    }
    Ok((trailer, payload))
}

/// Builds one logical data or index block. Reused across blocks via [`BlockBuilder::reset`].
///
/// A data builder needs keys that are internal keys (or at least contain a row terminator);
/// an index builder takes any strictly increasing byte strings (separators).
#[derive(Debug)]
pub struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    row_starts: Vec<u32>,
    restart_interval: usize,
    data: bool,
    entries: usize,
    last_key: Vec<u8>,
    /// Row-prefix length of `last_key` (data blocks). Kept across [`reset`](Self::reset) so the
    /// first entry of the next block is listed as a row start only if it starts a new row.
    last_row_len: Option<usize>,
    finished: bool,
}

impl BlockBuilder {
    fn new(restart_interval: usize, data: bool) -> Self {
        Self {
            buf: Vec::new(),
            restarts: Vec::new(),
            row_starts: Vec::new(),
            restart_interval: restart_interval.max(1),
            data,
            entries: 0,
            last_key: Vec::new(),
            last_row_len: None,
            finished: false,
        }
    }

    /// A data-block builder: maintains the row-start table.
    pub fn data(restart_interval: usize) -> Self {
        Self::new(restart_interval, true)
    }

    /// An index-block builder: restart interval 1, no row-start table.
    pub fn index() -> Self {
        Self::new(1, false)
    }

    /// Appends an entry. Keys must be strictly increasing.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> crate::Result<()> {
        if self.finished {
            return Err(Error::InvalidArgument {
                what: "block builder: add after finish",
            });
        }
        if self.entries > 0 && crate::key::compare(key, &self.last_key).is_le() {
            return Err(Error::InvalidArgument {
                what: "block builder: keys out of order",
            });
        }
        if self.buf.len() > MAX_BLOCK_LEN {
            return Err(Error::ValueTooLarge);
        }
        let offset = self.buf.len() as u32;
        let row_len = if self.data {
            Some(crate::key::row_prefix_len(key)?)
        } else {
            None
        };
        let shared = if self.entries.is_multiple_of(self.restart_interval) {
            self.restarts.push(offset);
            0
        } else {
            key.iter()
                .zip(&self.last_key)
                .take_while(|(a, b)| a == b)
                .count()
        };
        if self.data {
            let same_row =
                matches!(self.last_row_len, Some(n) if key.starts_with(&self.last_key[..n]));
            if !same_row {
                self.row_starts.push(offset);
            }
        }
        crate::varint::put_u64(&mut self.buf, shared as u64);
        crate::varint::put_u64(&mut self.buf, (key.len() - shared) as u64);
        crate::varint::put_u64(&mut self.buf, value.len() as u64);
        self.buf.extend_from_slice(&key[shared..]);
        self.buf.extend_from_slice(value);
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.last_row_len = row_len;
        self.entries += 1;
        Ok(())
    }

    /// Size the logical block would have if finished now.
    pub fn estimated_len(&self) -> usize {
        if self.finished {
            return self.buf.len();
        }
        self.buf.len() + 4 * (self.restarts.len() + self.row_starts.len()) + 8
    }

    /// Whether no entry has been added since the last reset.
    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }

    /// The last key added.
    pub fn last_key(&self) -> &[u8] {
        &self.last_key
    }

    /// Finishes the logical block and returns its bytes; valid until the next call.
    pub fn finish(&mut self) -> &[u8] {
        if !self.finished {
            for r in &self.restarts {
                self.buf.extend_from_slice(&r.to_le_bytes());
            }
            for r in &self.row_starts {
                self.buf.extend_from_slice(&r.to_le_bytes());
            }
            self.buf
                .extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
            self.buf
                .extend_from_slice(&(self.row_starts.len() as u32).to_le_bytes());
            self.finished = true;
        }
        &self.buf
    }

    /// Clears the builder for the next block, keeping its allocations.
    pub fn reset(&mut self) {
        self.buf.clear();
        self.restarts.clear();
        self.row_starts.clear();
        self.entries = 0;
        self.finished = false;
    }
}

/// A parsed logical block over any byte owner: `&[u8]`, a cache `BlockHandle`, a
/// decompression buffer. Owning the bytes lets a cursor live beside whatever keeps them alive
/// (an `Arc<SstReader>`, a pinned cache entry) without borrowing from it.
///
/// [`Block::new`] is O(1): it reads the table counts and checks the first restart. Offsets
/// in the restart and row-start tables are checked as a cursor uses them, so a cursor never
/// panics on bad bytes (it returns `Corrupt`); [`Block::validate`] checks both tables up
/// front for paths that want the whole block vetted (verification, fuzzing).
#[derive(Debug, Clone)]
pub struct Block<B> {
    bytes: B,
    /// End of the entries (start of the restart table).
    data_end: usize,
    restart_count: usize,
    row_start_count: usize,
}

/// One decoded entry header.
struct Entry {
    shared: usize,
    unshared: std::ops::Range<usize>,
    value: std::ops::Range<usize>,
}

/// Reads the table counts at the end of a logical block and checks that the tables fit and
/// the first restart is at offset 0. Returns `(data_end, restarts, row_starts)`.
fn parse_tail(b: &[u8]) -> crate::Result<(usize, usize, usize)> {
    let Some(tail) = b.len().checked_sub(8) else {
        return Err(Error::Truncated { what: "block" });
    };
    let r = le_u32(b, tail) as usize;
    let s = le_u32(b, tail + 4) as usize;
    let tables = r.checked_add(s).and_then(|n| n.checked_mul(4));
    let Some(data_end) = tables.and_then(|t| tail.checked_sub(t)) else {
        return Err(Error::Corrupt {
            what: "block tables",
        });
    };
    let first_ok = if r == 0 {
        data_end == 0
    } else {
        le_u32(b, data_end) == 0
    };
    if !first_ok {
        return Err(Error::Corrupt {
            what: "block tables",
        });
    }
    Ok((data_end, r, s))
}

impl<B: Deref<Target = [u8]>> Block<B> {
    /// Parses the table counts: O(1), never panics. See [`Block::validate`].
    pub fn new(logical: B) -> crate::Result<Self> {
        let (data_end, restart_count, row_start_count) = parse_tail(&logical)?;
        Ok(Self {
            data_end,
            restart_count,
            row_start_count,
            bytes: logical,
        })
    }

    /// Checks that both offset tables are strictly ascending and point inside the entries.
    /// O(restarts + row starts).
    pub fn validate(&self) -> crate::Result<()> {
        let b: &[u8] = &self.bytes;
        let ascending = |start: usize, n: usize| {
            let mut prev = None;
            (0..n).all(|i| {
                let v = le_u32(b, start + 4 * i) as usize;
                let ok = v < self.data_end && prev.is_none_or(|p| v > p);
                prev = Some(v);
                ok
            })
        };
        let r = self.restart_count;
        if !ascending(self.data_end, r) || !ascending(self.data_end + 4 * r, self.row_start_count) {
            return Err(Error::Corrupt {
                what: "block tables",
            });
        }
        Ok(())
    }

    /// Number of restart points.
    pub fn restart_count(&self) -> usize {
        self.restart_count
    }

    /// Number of rows that begin in this block.
    pub fn row_start_count(&self) -> usize {
        self.row_start_count
    }

    /// A cursor over the block, positioned before the first entry. Takes the block (and so
    /// its byte owner) by value; clone a cheap owner such as a `BlockHandle` to keep both.
    pub fn into_cursor(self) -> BlockIter<B> {
        let end = self.data_end;
        BlockIter {
            block: self,
            cur: end,
            next: end,
            key: KeySrc::Block(0..0),
            key_buf: KeyBuf::new(),
            value: 0..0,
        }
    }

    /// Entry `i` of the table starting at `table`, checked to point inside the entries.
    fn table(&self, table: usize, i: usize, what: &'static str) -> crate::Result<usize> {
        let v = le_u32(&self.bytes, table + 4 * i) as usize;
        if v >= self.data_end {
            return Err(Error::Corrupt { what });
        }
        Ok(v)
    }

    fn restart(&self, i: usize) -> crate::Result<usize> {
        self.table(self.data_end, i, "block restart offset")
    }

    fn row_start(&self, i: usize) -> crate::Result<usize> {
        self.table(
            self.data_end + 4 * self.restart_count,
            i,
            "block row-start offset",
        )
    }

    fn entry(&self, offset: usize) -> crate::Result<Entry> {
        let b = &self.bytes[..self.data_end];
        let corrupt = || Error::Corrupt {
            what: "block entry",
        };
        // Most entries have all three fields under 128: one byte each, decoded without the
        // general varint loop (#287).
        if let Some(&[shared, unshared, value_len]) = b.get(offset..offset + 3)
            && (shared | unshared | value_len) < 0x80
        {
            let key_start = offset + 3;
            let key_end = key_start + usize::from(unshared);
            let value_end = key_end + usize::from(value_len);
            if value_end > b.len() {
                return Err(corrupt());
            }
            return Ok(Entry {
                shared: usize::from(shared),
                unshared: key_start..key_end,
                value: key_end..value_end,
            });
        }
        let mut pos = offset;
        let mut field = || -> crate::Result<usize> {
            let (v, n) = crate::varint::get_u64(b.get(pos..).ok_or_else(corrupt)?)?;
            pos += n;
            usize::try_from(v).map_err(|_| corrupt())
        };
        let shared = field()?;
        let unshared = field()?;
        let value_len = field()?;
        let key_end = pos.checked_add(unshared).ok_or_else(corrupt)?;
        let value_end = key_end.checked_add(value_len).ok_or_else(corrupt)?;
        if value_end > b.len() {
            return Err(corrupt());
        }
        Ok(Entry {
            shared,
            unshared: pos..key_end,
            value: key_end..value_end,
        })
    }
}

#[derive(Debug, Clone)]
enum KeySrc {
    /// The key is a range of the block (restart entries).
    Block(std::ops::Range<usize>),
    /// The key was rebuilt in `key_buf`.
    Buf,
}

/// Keys up to this long are rebuilt inline, so a cursor allocates nothing for them, even
/// when one is created per lookup.
const INLINE_KEY: usize = 128;

/// A key buffer that lives inline until a key outgrows it.
#[derive(Debug, Clone)]
struct KeyBuf {
    inline: [u8; INLINE_KEY],
    len: usize,
    heap: Vec<u8>,
    spilled: bool,
}

impl KeyBuf {
    fn new() -> Self {
        Self {
            inline: [0; INLINE_KEY],
            len: 0,
            heap: Vec::new(),
            spilled: false,
        }
    }

    fn as_slice(&self) -> &[u8] {
        if self.spilled {
            &self.heap
        } else {
            &self.inline[..self.len]
        }
    }

    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn clear(&mut self) {
        self.len = 0;
        self.heap.clear();
        self.spilled = false;
    }

    fn truncate(&mut self, n: usize) {
        if self.spilled {
            self.heap.truncate(n);
        } else {
            self.len = self.len.min(n);
        }
    }

    fn extend_from_slice(&mut self, b: &[u8]) {
        if !self.spilled {
            if self.len + b.len() <= INLINE_KEY {
                self.inline[self.len..self.len + b.len()].copy_from_slice(b);
                self.len += b.len();
                return;
            }
            self.heap.clear();
            self.heap.extend_from_slice(&self.inline[..self.len]);
            self.spilled = true;
        }
        self.heap.extend_from_slice(b);
    }
}

/// A zero-copy cursor over one logical block that owns its byte owner `B`. Keys at restart
/// points are borrowed from the block; other keys are rebuilt in a buffer that is inline for
/// keys up to 128 bytes, so iterating allocates nothing for them. [`BlockIter::reset`] moves
/// the cursor onto another block, keeping its buffer.
#[derive(Debug, Clone)]
pub struct BlockIter<B> {
    block: Block<B>,
    /// Offset of the current entry; `data_end` when not valid.
    cur: usize,
    /// Offset of the entry after the current one.
    next: usize,
    key: KeySrc,
    key_buf: KeyBuf,
    value: std::ops::Range<usize>,
}

impl<B: Deref<Target = [u8]>> BlockIter<B> {
    /// Moves the cursor onto another logical block, unpositioned, with the O(1) checks of
    /// [`Block::new`]. On error the cursor holds `bytes` as an empty block (so it pins
    /// nothing else) and is invalid.
    ///
    /// ```
    /// use pigeonhole_format::Cursor;
    /// use pigeonhole_format::block::{Block, BlockBuilder};
    /// use pigeonhole_format::key::{Kind, encode_key};
    ///
    /// let mut blocks = Vec::new();
    /// for row in [&b"a"[..], b"b"] {
    ///     let mut b = BlockBuilder::data(16);
    ///     let mut k = Vec::new();
    ///     encode_key(&mut k, row, b"q", 1, 1, Kind::Put).unwrap();
    ///     b.add(&k, b"\x00v").unwrap();
    ///     blocks.push(b.finish().to_vec());
    /// }
    /// let mut it = Block::new(blocks[0].as_slice()).unwrap().into_cursor();
    /// it.seek_to_first().unwrap();
    /// assert!(it.key().starts_with(b"a\x00\x01"));
    /// it.reset(blocks[1].as_slice()).unwrap();
    /// assert!(!it.valid());
    /// it.seek_to_first().unwrap();
    /// assert!(it.key().starts_with(b"b\x00\x01"));
    /// ```
    pub fn reset(&mut self, bytes: B) -> crate::Result<()> {
        let parsed = parse_tail(&bytes);
        let (data_end, r, s) = *parsed.as_ref().unwrap_or(&(0, 0, 0));
        self.block = Block {
            bytes,
            data_end,
            restart_count: r,
            row_start_count: s,
        };
        self.invalidate();
        parsed.map(|_| ())
    }

    /// The block this cursor reads.
    pub fn block(&self) -> &Block<B> {
        &self.block
    }

    /// Byte offset of the current entry within the block (for row-start lookups).
    pub fn entry_offset(&self) -> usize {
        self.cur
    }

    /// The byte owner (to hand out pinned value ranges).
    pub fn bytes(&self) -> &B {
        &self.block.bytes
    }

    /// The current value's byte range within the block.
    pub fn value_range(&self) -> std::ops::Range<u32> {
        self.value.start as u32..self.value.end as u32
    }

    fn invalidate(&mut self) {
        self.cur = self.block.data_end;
        self.next = self.block.data_end;
        self.key = KeySrc::Block(0..0);
        self.value = 0..0;
    }

    /// Passes `r` through, invalidating the cursor if it is an error.
    fn or_invalidate<T>(&mut self, r: crate::Result<T>) -> crate::Result<T> {
        if r.is_err() {
            self.invalidate();
        }
        r
    }

    /// Decodes the entry at `offset`, taking its shared prefix from the current key.
    fn load(&mut self, offset: usize) -> crate::Result<()> {
        if offset >= self.block.data_end {
            self.invalidate();
            return Ok(());
        }
        let e = match self.block.entry(offset) {
            Ok(e) => e,
            Err(err) => {
                self.invalidate();
                return Err(err);
            }
        };
        let bytes: &[u8] = &self.block.bytes;
        if e.shared == 0 {
            self.key = KeySrc::Block(e.unshared.clone());
        } else {
            match &self.key {
                KeySrc::Buf if e.shared <= self.key_buf.len() => self.key_buf.truncate(e.shared),
                KeySrc::Block(r) if e.shared <= r.len() => {
                    let prefix = r.start..r.start + e.shared;
                    self.key_buf.clear();
                    self.key_buf.extend_from_slice(&bytes[prefix]);
                }
                _ => {
                    self.invalidate();
                    return Err(Error::Corrupt {
                        what: "block entry shared prefix",
                    });
                }
            }
            self.key_buf.extend_from_slice(&bytes[e.unshared.clone()]);
            self.key = KeySrc::Buf;
        }
        self.cur = offset;
        self.next = e.value.end;
        self.value = e.value;
        Ok(())
    }

    /// Positions on the restart entry at index `i` (its key is stored whole).
    fn load_restart(&mut self, i: usize) -> crate::Result<()> {
        let at = self.block.restart(i);
        let at = self.or_invalidate(at)?;
        self.key = KeySrc::Block(0..0);
        self.load(at)
    }

    fn restart_key(&self, i: usize) -> crate::Result<&[u8]> {
        let e = self.block.entry(self.block.restart(i)?)?;
        if e.shared != 0 {
            return Err(Error::Corrupt {
                what: "block restart entry",
            });
        }
        Ok(&self.block.bytes[e.unshared])
    }

    /// The first row start after the current entry, if any.
    fn next_row_start(&self) -> crate::Result<Option<usize>> {
        let (mut lo, mut hi) = (0, self.block.row_start_count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.block.row_start(mid)? <= self.cur {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == self.block.row_start_count {
            return Ok(None);
        }
        let at = self.block.row_start(lo)?;
        // The search only stops on an entry it saw above `cur`, so this cannot fire; it is
        // kept so a future change to the search can never make `skip_row` move backwards
        // (and loop) on an unsorted, corrupt table.
        if at <= self.cur {
            return Err(Error::Corrupt {
                what: "block row-start table",
            });
        }
        Ok(Some(at))
    }
}

impl<B: Deref<Target = [u8]>> Cursor for BlockIter<B> {
    type Error = crate::Error;

    fn valid(&self) -> bool {
        self.cur < self.block.data_end
    }

    fn key(&self) -> &[u8] {
        match &self.key {
            KeySrc::Block(r) => &self.block.bytes[r.clone()],
            KeySrc::Buf => self.key_buf.as_slice(),
        }
    }

    fn value(&self) -> &[u8] {
        &self.block.bytes[self.value.clone()]
    }

    fn seek_to_first(&mut self) -> crate::Result<()> {
        if self.block.restart_count == 0 {
            self.invalidate();
            return Ok(());
        }
        self.load_restart(0)
    }

    fn seek(&mut self, target: &[u8]) -> crate::Result<()> {
        if self.block.restart_count == 0 {
            self.invalidate();
            return Ok(());
        }
        // Last restart whose key is < target; scanning forward from it finds the answer.
        let (mut lo, mut hi) = (0, self.block.restart_count);
        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            match self.restart_key(mid) {
                Ok(k) if k < target => lo = mid,
                Ok(_) => hi = mid,
                Err(e) => {
                    self.invalidate();
                    return Err(e);
                }
            }
        }
        self.load_restart(lo)?;
        while self.valid() && self.key() < target {
            self.next()?;
        }
        Ok(())
    }

    fn next(&mut self) -> crate::Result<()> {
        if !self.valid() {
            return Ok(());
        }
        self.load(self.next)
    }

    /// Uses the row-start table: one binary search, no cell decoding. Becomes invalid if no
    /// later row starts in this block.
    fn skip_row(&mut self) -> crate::Result<()> {
        if !self.valid() {
            return Ok(());
        }
        let next = self.next_row_start();
        match self.or_invalidate(next)? {
            None => {
                self.invalidate();
                Ok(())
            }
            // The row start's shared prefix lies within the current key's row prefix.
            Some(at) => self.load(at),
        }
    }
}
