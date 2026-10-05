//! Block and row caches with pinned, ref-counted handles for Pigeonhole.
//!
//! Per-process caches (never shared across processes). A [`BlockHandle`] pins its block:
//! eviction skips pinned entries, and the bytes stay valid until the last handle drops. The
//! hit path takes a shard lock, bumps a reference count and allocates nothing.
//!
//! Both caches are sharded S3-FIFO (small, main and ghost FIFOs per shard); see the
//! `s3fifo` module docs for the policy, pins and how [`Priority`] changes it.
//!
//! The mock for the layer above is the real cache, small or [`BlockCache::disabled`].
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
// The crate needs no `unsafe` today: pins are `Arc`s. CONTRIBUTING permits it here, with a
// `// SAFETY:` argument per block, should Phase 3 tuning need it.
#![deny(unsafe_op_in_unsafe_fn)]

mod hash;
mod s3fifo;
mod sync;

use std::fmt;
use std::ops::{Deref, Range};

use pigeonhole_io::IoBuf;

use crate::hash::hash_of;
use crate::s3fifo::Sharded;
use crate::sync::{Arc, lock};

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
///
/// `High` blocks skip the probationary queue and get extra lives; `Low` blocks (scans,
/// compaction reads) churn only the probationary queue, so a large scan cannot flush the
/// working set.
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

impl BlockData {
    #[inline]
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Io(b) => b,
            Self::Heap(b) => b,
        }
    }

    /// Bytes of memory held (an `IoBuf` holds its whole aligned capacity).
    fn charge(&self) -> usize {
        match self {
            Self::Io(b) => b.capacity(),
            Self::Heap(b) => b.len(),
        }
    }
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

/// A sharded S3-FIFO block cache.
///
/// Capacity is in bytes of block memory (an [`IoBuf`] counts its aligned capacity, a heap
/// buffer its length) and is split evenly across shards. A block larger than a shard is
/// returned pinned but not cached. Pinned blocks are never evicted; if everything in a shard
/// is pinned, the shard runs over capacity until handles drop.
///
/// ```
/// use pigeonhole_cache::{BlockCache, BlockKey, Priority};
///
/// let cache = BlockCache::new(1 << 20, 0);
/// let key = BlockKey { file: 7, offset: 4096 };
/// let pinned = cache.insert(key, vec![1, 2, 3].into(), Priority::Normal);
/// assert_eq!(&pinned[..], &[1, 2, 3]);
///
/// let hit = cache.get(key).expect("cached");
/// assert_eq!(hit.bytes(), &[1, 2, 3]);
///
/// // Eviction and `erase_file` skip pinned blocks; once unpinned, the block can go.
/// cache.erase_file(7);
/// assert_eq!(&hit[..], &[1, 2, 3]);
/// drop((pinned, hit));
/// cache.erase_file(7);
/// assert!(cache.get(key).is_none());
/// assert_eq!(cache.usage(), 0);
/// ```
pub struct BlockCache {
    shards: Sharded<BlockKey, BlockData>,
}

impl BlockCache {
    /// Smallest shard the default shard count will create.
    const MIN_SHARD_BYTES: usize = 256 << 10;

    /// A cache holding up to `capacity` bytes across `shards` shards (0 picks a default:
    /// up to 64, at least 256 KiB each).
    pub fn new(capacity: usize, shards: usize) -> Self {
        Self {
            shards: Sharded::new(capacity, shards, Self::MIN_SHARD_BYTES),
        }
    }

    /// A cache that stores nothing: every insert returns an unshared handle.
    pub fn disabled() -> Self {
        Self {
            shards: Sharded::empty(),
        }
    }

    /// Looks up a block and pins it. Allocation-free.
    #[inline]
    pub fn get(&self, key: BlockKey) -> Option<BlockHandle> {
        let shard = self.shards.shard(hash_of(&key))?;
        lock(shard).get(&key).map(|block| BlockHandle { block })
    }

    /// Inserts a block (replacing any entry with the same key) and returns it pinned.
    pub fn insert(&self, key: BlockKey, data: BlockData, priority: Priority) -> BlockHandle {
        let charge = data.charge();
        let block = Arc::new(data);
        if let Some(shard) = self.shards.shard(hash_of(&key)) {
            lock(shard).insert(key, Arc::clone(&block), charge, priority);
        }
        BlockHandle { block }
    }

    /// Drops every unpinned block of `file` (after its SST or blob file is deleted).
    ///
    /// Pinned blocks of `file` stay indexed until a later eviction; file ids are never reused,
    /// so they can only be found by holders of the old file's keys.
    pub fn erase_file(&self, file: u64) {
        self.erase_files(&[file]);
    }

    /// [`erase_file`](Self::erase_file) for a batch of files (one pass over each shard, so a
    /// compaction that removes many SSTs pays one scan, not one per file).
    ///
    /// ```
    /// use pigeonhole_cache::{BlockCache, BlockKey, Priority};
    ///
    /// let cache = BlockCache::new(1 << 20, 0);
    /// for file in 1..=3 {
    ///     drop(cache.insert(BlockKey { file, offset: 0 }, vec![0; 8].into(), Priority::Normal));
    /// }
    /// cache.erase_files(&[1, 3]);
    /// assert!(cache.get(BlockKey { file: 1, offset: 0 }).is_none());
    /// assert!(cache.get(BlockKey { file: 2, offset: 0 }).is_some());
    /// assert!(cache.get(BlockKey { file: 3, offset: 0 }).is_none());
    /// ```
    pub fn erase_files(&self, files: &[u64]) {
        if files.len() <= 8 {
            self.shards
                .for_each(|s| s.remove_unpinned_where(|k| files.contains(&k.file)));
        } else {
            let mut sorted = files.to_vec();
            sorted.sort_unstable();
            self.shards
                .for_each(|s| s.remove_unpinned_where(|k| sorted.binary_search(&k.file).is_ok()));
        }
    }

    /// Bytes currently cached.
    pub fn usage(&self) -> usize {
        self.shards.usage()
    }

    /// Configured capacity.
    pub fn capacity(&self) -> usize {
        self.shards.capacity()
    }
}

impl fmt::Debug for BlockCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockCache")
            .field("shards", &self.shards.len())
            .field("capacity", &self.capacity())
            .field("usage", &self.usage())
            .finish()
    }
}

/// A pinned, ref-counted cached block. Clone is a reference-count increment.
///
/// ```
/// use pigeonhole_cache::{BlockCache, BlockKey, Priority};
///
/// let cache = BlockCache::disabled();
/// let h = cache.insert(BlockKey { file: 1, offset: 0 }, b"abc".to_vec().into(), Priority::Low);
/// let h2 = h.clone();
/// assert_eq!(h2.bytes(), b"abc");
/// ```
#[derive(Clone)]
pub struct BlockHandle {
    block: Arc<BlockData>,
}

impl BlockHandle {
    /// The block's bytes.
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        self.block.bytes()
    }
}

impl Deref for BlockHandle {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.bytes()
    }
}

impl fmt::Debug for BlockHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockHandle")
            .field("len", &self.bytes().len())
            .finish()
    }
}

/// A value slice that pins its source: a range of a cached block, or a small owned buffer
/// (resolved merges, blob reads). Cheap to clone; safe to hold across `.await`.
///
/// ```
/// use pigeonhole_cache::{BlockCache, BlockKey, Cell, Priority};
///
/// let cache = BlockCache::new(1 << 20, 1);
/// let block = cache.insert(BlockKey { file: 1, offset: 0 }, b"key=value".to_vec().into(), Priority::Normal);
/// let cell = Cell::in_block(block, 4..9);
/// assert_eq!(&cell[..], b"value");
///
/// assert_eq!(Cell::owned(b"merged".to_vec()).bytes(), b"merged");
/// ```
#[derive(Clone)]
pub struct Cell {
    repr: CellRepr,
}

#[derive(Clone)]
enum CellRepr {
    Block {
        block: BlockHandle,
        start: u32,
        end: u32,
    },
    Owned(Arc<Vec<u8>>),
}

impl Cell {
    /// A cell over `range` of `block`.
    ///
    /// # Panics
    /// If `range` is reversed or extends past the block.
    pub fn in_block(block: BlockHandle, range: Range<u32>) -> Self {
        assert!(
            range.start <= range.end && range.end as usize <= block.bytes().len(),
            "cell range {range:?} outside a block of {} bytes",
            block.bytes().len()
        );
        Self {
            repr: CellRepr::Block {
                block,
                start: range.start,
                end: range.end,
            },
        }
    }

    /// A cell owning its bytes.
    pub fn owned(bytes: Vec<u8>) -> Self {
        Self {
            repr: CellRepr::Owned(Arc::new(bytes)),
        }
    }

    /// The value bytes.
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        match &self.repr {
            CellRepr::Block { block, start, end } => &block.bytes()[*start as usize..*end as usize],
            CellRepr::Owned(v) => v,
        }
    }
}

impl Deref for Cell {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.bytes()
    }
}

impl fmt::Debug for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let source = match self.repr {
            CellRepr::Block { .. } => "block",
            CellRepr::Owned(_) => "owned",
        };
        f.debug_struct("Cell")
            .field("source", &source)
            .field("len", &self.bytes().len())
            .finish()
    }
}

/// A per-family cache of small, very hot rows, keyed by `(family, row, epoch)` where the
/// epoch changes whenever the row may have changed (the engine supplies it).
///
/// One entry per `(family, row)`: a lookup with a different epoch misses, and inserting a new
/// epoch replaces the old one. Entries are indexed by a 64-bit hash of `(family, row)` and
/// verified against the stored family and row, so a hash collision costs a miss, never a
/// wrong row.
///
/// ```
/// use pigeonhole_cache::RowCache;
///
/// let rows = RowCache::new(1 << 20);
/// rows.insert(3, b"user:42", 10, b"encoded cells".to_vec());
/// assert_eq!(&rows.get(3, b"user:42", 10).unwrap()[..], b"encoded cells");
/// assert!(rows.get(3, b"user:42", 11).is_none()); // the row may have changed
///
/// rows.invalidate(3, b"user:42");
/// assert!(rows.get(3, b"user:42", 10).is_none());
/// ```
pub struct RowCache {
    shards: Sharded<u64, RowEntry>,
}

struct RowEntry {
    family: u64,
    epoch: u64,
    row: Box<[u8]>,
    encoded: Vec<u8>,
}

impl RowEntry {
    #[inline]
    fn is(&self, family: u64, row: &[u8]) -> bool {
        self.family == family && *self.row == *row
    }
}

impl RowCache {
    /// Smallest shard the row cache will create.
    const MIN_SHARD_BYTES: usize = 64 << 10;

    /// Bytes charged per entry on top of its row and encoding (index slot, `Arc`, queues).
    const ENTRY_OVERHEAD: usize = 96;

    /// A row cache of up to `capacity` bytes.
    pub fn new(capacity: usize) -> Self {
        Self {
            shards: Sharded::new(capacity, 0, Self::MIN_SHARD_BYTES),
        }
    }

    #[inline]
    fn hash(family: u64, row: &[u8]) -> u64 {
        hash_of(&(family, row))
    }

    /// Looks up a row. Allocation-free.
    #[inline]
    pub fn get(&self, family: u64, row: &[u8], epoch: u64) -> Option<RowHandle> {
        let h = Self::hash(family, row);
        lock(self.shards.shard(h)?)
            .get_if(&h, |e| e.is(family, row) && e.epoch == epoch)
            .map(|entry| RowHandle { entry })
    }

    /// Caches the encoded row (an engine-defined encoding of its visible cells).
    pub fn insert(&self, family: u64, row: &[u8], epoch: u64, encoded: Vec<u8>) -> RowHandle {
        let charge = row.len() + encoded.len() + Self::ENTRY_OVERHEAD;
        let entry = Arc::new(RowEntry {
            family,
            epoch,
            row: row.into(),
            encoded,
        });
        let h = Self::hash(family, row);
        if let Some(shard) = self.shards.shard(h) {
            lock(shard).insert(h, Arc::clone(&entry), charge, Priority::Normal);
        }
        RowHandle { entry }
    }

    /// Drops a row (on write to it).
    pub fn invalidate(&self, family: u64, row: &[u8]) {
        let h = Self::hash(family, row);
        let Some(shard) = self.shards.shard(h) else {
            return;
        };
        let mut shard = lock(shard);
        if shard.peek(&h).is_some_and(|e| e.is(family, row)) {
            shard.remove(&h);
        }
    }

    /// Bytes currently cached (rows, encodings and a fixed per-entry overhead).
    #[cfg(all(test, not(loom)))]
    fn usage(&self) -> usize {
        self.shards.usage()
    }
}

impl fmt::Debug for RowCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowCache")
            .field("shards", &self.shards.len())
            .field("capacity", &self.shards.capacity())
            .field("usage", &self.shards.usage())
            .finish()
    }
}

/// A pinned cached row.
///
/// ```
/// use pigeonhole_cache::RowCache;
///
/// let rows = RowCache::new(1 << 16);
/// let h = rows.insert(1, b"r", 0, vec![9, 9]);
/// rows.invalidate(1, b"r");
/// assert_eq!(&h[..], &[9, 9]); // still pinned
/// ```
#[derive(Clone)]
pub struct RowHandle {
    entry: Arc<RowEntry>,
}

impl Deref for RowHandle {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.entry.encoded
    }
}

impl fmt::Debug for RowHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowHandle")
            .field("family", &self.entry.family)
            .field("epoch", &self.entry.epoch)
            .field("len", &self.entry.encoded.len())
            .finish()
    }
}

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, loom))]
mod loom_tests;
