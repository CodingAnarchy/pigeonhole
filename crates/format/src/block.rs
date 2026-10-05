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
//! can skip the rest of a row with a binary search instead of decoding cells. See `FORMAT.md` §3.

use crate::compress::Compression;
use crate::cursor::Cursor;

/// Default restart interval for data blocks.
pub const DEFAULT_RESTART_INTERVAL: usize = 16;

/// Default target size of an uncompressed data block (16 KiB).
pub const DEFAULT_BLOCK_SIZE: usize = 16 * 1024;

/// Size of the physical block trailer.
pub const TRAILER_LEN: usize = 16;

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
        todo!()
    }

    /// Decodes the varint encoding.
    pub fn decode_varint(input: &[u8]) -> crate::Result<Self> {
        todo!()
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

/// Compresses `logical`, appends `payload ++ trailer` to `out`, and returns the trailer.
pub fn seal(
    kind: BlockKind,
    codec: Compression,
    logical: &[u8],
    out: &mut Vec<u8>,
) -> crate::Result<BlockTrailer> {
    todo!()
}

/// Verifies a physical block's checksum and returns its trailer and payload. Done on reads
/// from disk; skipped on cache hits.
pub fn verify(physical: &[u8]) -> crate::Result<(BlockTrailer, &[u8])> {
    todo!()
}

/// Builds one logical data or index block. Reused across blocks via [`BlockBuilder::reset`].
#[derive(Debug)]
pub struct BlockBuilder {
    _priv: (),
}

impl BlockBuilder {
    /// A data-block builder: maintains the row-start table.
    pub fn data(restart_interval: usize) -> Self {
        todo!()
    }

    /// An index-block builder: restart interval 1, no row-start table.
    pub fn index() -> Self {
        todo!()
    }

    /// Appends an entry. Keys must be strictly increasing.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> crate::Result<()> {
        todo!()
    }

    /// Size the logical block would have if finished now.
    pub fn estimated_len(&self) -> usize {
        todo!()
    }

    /// Whether no entry has been added since the last reset.
    pub fn is_empty(&self) -> bool {
        todo!()
    }

    /// The last key added.
    pub fn last_key(&self) -> &[u8] {
        todo!()
    }

    /// Finishes the logical block and returns its bytes; valid until the next call.
    pub fn finish(&mut self) -> &[u8] {
        todo!()
    }

    /// Clears the builder for the next block, keeping its allocations.
    pub fn reset(&mut self) {
        todo!()
    }
}

/// A parsed logical block borrowing its bytes (a cache buffer or a decompression buffer).
#[derive(Debug, Clone, Copy)]
pub struct Block<'a> {
    _bytes: &'a [u8],
}

impl<'a> Block<'a> {
    /// Parses the restart and row-start tables. Never panics.
    pub fn new(logical: &'a [u8]) -> crate::Result<Self> {
        todo!()
    }

    /// Number of restart points.
    pub fn restart_count(&self) -> usize {
        todo!()
    }

    /// Number of rows that begin in this block.
    pub fn row_start_count(&self) -> usize {
        todo!()
    }

    /// A cursor over the block, positioned before the first entry.
    pub fn iter(&self) -> BlockIter<'a> {
        todo!()
    }
}

/// A zero-copy cursor over one logical block. Keys at restart points are borrowed; other keys
/// are rebuilt in a small reused buffer.
#[derive(Debug)]
pub struct BlockIter<'a> {
    _block: Block<'a>,
}

impl BlockIter<'_> {
    /// Byte offset of the current entry within the block (for row-start lookups).
    pub fn entry_offset(&self) -> usize {
        todo!()
    }
}

impl Cursor for BlockIter<'_> {
    type Error = crate::Error;

    fn valid(&self) -> bool {
        todo!()
    }

    fn key(&self) -> &[u8] {
        todo!()
    }

    fn value(&self) -> &[u8] {
        todo!()
    }

    fn seek_to_first(&mut self) -> crate::Result<()> {
        todo!()
    }

    fn seek(&mut self, target: &[u8]) -> crate::Result<()> {
        todo!()
    }

    fn next(&mut self) -> crate::Result<()> {
        todo!()
    }

    /// Uses the row-start table: one binary search, no cell decoding.
    fn skip_row(&mut self) -> crate::Result<()> {
        todo!()
    }
}
