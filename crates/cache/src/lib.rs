//! Block and row caches with pinned, ref-counted handles for Pigeonhole.
//!
//! Per-process caches (never shared across processes). A [`BlockHandle`] pins its block:
//! eviction skips pinned entries, and the bytes stay valid until the last handle drops. The
//! hit path takes a shard lock, bumps a reference count and allocates nothing.
//!
//! The mock for the layer above is the real cache, small or [`BlockCache::disabled`].
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
// `unsafe` is permitted in this crate; every block carries a `// SAFETY:` argument.
#![deny(unsafe_op_in_unsafe_fn)]
// Interface freeze: bodies are `todo!()`. Remove this allow when implementing.
#![allow(unused_variables, clippy::ptr_arg)]

use std::ops::{Deref, Range};

use pigeonhole_io::IoBuf;

/// Identifies a cached block: a namespace (an SST id or a blob file id, tagged by the caller
/// so the two never collide) and the block's offset within it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockKey {
    /// SST or blob-file namespace.
    pub file: u64,
    /// Offset of the block within the file.
    pub offset: u64,
}

/// Eviction priority. Higher survives longer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Priority {
    /// Evicted first (scans, compaction reads).
    Low,
    /// The default.
    #[default]
    Normal,
    /// Evicted last (hot families).
    High,
}

/// Bytes handed to the cache: a disk buffer (uncompressed blocks are cached as read) or a
/// heap buffer (decompressed blocks).
#[derive(Debug)]
pub enum BlockData {
    /// An aligned I/O buffer.
    Io(IoBuf),
    /// A heap buffer.
    Heap(Box<[u8]>),
}

impl From<IoBuf> for BlockData {
    fn from(b: IoBuf) -> Self {
        Self::Io(b)
    }
}

impl From<Vec<u8>> for BlockData {
    fn from(v: Vec<u8>) -> Self {
        Self::Heap(v.into_boxed_slice())
    }
}

/// A sharded block cache (S3-FIFO or CLOCK-Pro, chosen at implementation).
#[derive(Debug)]
pub struct BlockCache {
    _priv: (),
}

impl BlockCache {
    /// A cache holding up to `capacity` bytes across `shards` shards (0 picks a default).
    pub fn new(capacity: usize, shards: usize) -> Self {
        todo!()
    }

    /// A cache that stores nothing: every insert returns an unshared handle.
    pub fn disabled() -> Self {
        todo!()
    }

    /// Looks up a block and pins it. Allocation-free.
    pub fn get(&self, key: BlockKey) -> Option<BlockHandle> {
        todo!()
    }

    /// Inserts a block (replacing any entry with the same key) and returns it pinned.
    pub fn insert(&self, key: BlockKey, data: BlockData, priority: Priority) -> BlockHandle {
        todo!()
    }

    /// Drops every unpinned block of `file` (after its SST or blob file is deleted).
    pub fn erase_file(&self, file: u64) {
        todo!()
    }

    /// Bytes currently cached.
    pub fn usage(&self) -> usize {
        todo!()
    }

    /// Configured capacity.
    pub fn capacity(&self) -> usize {
        todo!()
    }
}

/// A pinned, ref-counted cached block. Clone is a reference-count increment.
#[derive(Debug, Clone)]
pub struct BlockHandle {
    _priv: (),
}

impl BlockHandle {
    /// The block's bytes.
    pub fn bytes(&self) -> &[u8] {
        todo!()
    }
}

impl Deref for BlockHandle {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        todo!()
    }
}

/// A value slice that pins its source: a range of a cached block, or a small owned buffer
/// (resolved merges, blob reads). Cheap to clone; safe to hold across `.await`.
#[derive(Debug, Clone)]
pub struct Cell {
    _priv: (),
}

impl Cell {
    /// A cell over `range` of `block`.
    pub fn in_block(block: BlockHandle, range: Range<u32>) -> Self {
        todo!()
    }

    /// A cell owning its bytes.
    pub fn owned(bytes: Vec<u8>) -> Self {
        todo!()
    }

    /// The value bytes.
    pub fn bytes(&self) -> &[u8] {
        todo!()
    }
}

impl Deref for Cell {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        todo!()
    }
}

/// A per-family cache of small, very hot rows, keyed by `(family, row, epoch)` where the
/// epoch changes whenever the row may have changed (the engine supplies it).
#[derive(Debug)]
pub struct RowCache {
    _priv: (),
}

impl RowCache {
    /// A row cache of up to `capacity` bytes.
    pub fn new(capacity: usize) -> Self {
        todo!()
    }

    /// Looks up a row. Allocation-free.
    pub fn get(&self, family: u64, row: &[u8], epoch: u64) -> Option<RowHandle> {
        todo!()
    }

    /// Caches the encoded row (an engine-defined encoding of its visible cells).
    pub fn insert(&self, family: u64, row: &[u8], epoch: u64, encoded: Vec<u8>) -> RowHandle {
        todo!()
    }

    /// Drops a row (on write to it).
    pub fn invalidate(&self, family: u64, row: &[u8]) {
        todo!()
    }
}

/// A pinned cached row.
#[derive(Debug, Clone)]
pub struct RowHandle {
    _priv: (),
}

impl Deref for RowHandle {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        todo!()
    }
}
