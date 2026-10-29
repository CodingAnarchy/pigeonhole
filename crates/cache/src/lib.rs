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
// Pins are `Arc`s. The one `unsafe` is `BlockHandle`'s cached view of its block's bytes
// (CONTRIBUTING permits it here, with a `// SAFETY:` argument per block).
#![deny(unsafe_op_in_unsafe_fn)]

mod hash;
mod s3fifo;
mod sync;

use std::fmt;
use std::ops::{Deref, Range};
use std::sync::atomic::Ordering;

use pigeonhole_io::IoBuf;

use crate::hash::hash_of;
use crate::s3fifo::Sharded;
use crate::sync::{Arc, AtomicU8, AtomicU64, lock, read};

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
/// // Eviction and `erase_files` skip pinned blocks; once unpinned, the block can go.
/// cache.erase_files(&[7]);
/// assert_eq!(&hit[..], &[1, 2, 3]);
/// drop((pinned, hit));
/// cache.erase_files(&[7]);
/// assert!(cache.get(key).is_none());
/// assert_eq!(cache.usage(), 0);
/// ```
pub struct BlockCache {
    shards: Sharded<BlockKey, BlockData>,
    /// Bytes of blocks their readers keep pinned for as long as they are open (an SST's top
    /// index and filters; [`add_pinned`](Self::add_pinned)), D215.
    pinned: AtomicU64,
    /// 1 while the next rise of `pinned` past half the capacity counts a warning; re-armed
    /// once `pinned` falls below 40% of it.
    pinned_armed: AtomicU8,
    /// Rises of `pinned` past half the capacity.
    pinned_warnings: AtomicU64,
}

impl BlockCache {
    /// Smallest shard the default shard count will create.
    const MIN_SHARD_BYTES: usize = 256 << 10;

    /// A cache holding up to `capacity` bytes across `shards` shards (0 picks a default:
    /// up to 64, at least 256 KiB each).
    pub fn new(capacity: usize, shards: usize) -> Self {
        Self::with_shards(Sharded::new(capacity, shards, Self::MIN_SHARD_BYTES))
    }

    /// A cache that stores nothing: every insert returns an unshared handle.
    pub fn disabled() -> Self {
        Self::with_shards(Sharded::empty())
    }

    fn with_shards(shards: Sharded<BlockKey, BlockData>) -> Self {
        Self {
            shards,
            pinned: AtomicU64::new(0),
            pinned_armed: AtomicU8::new(1),
            pinned_warnings: AtomicU64::new(0),
        }
    }

    /// Looks up a block and pins it, counting a hit or a miss
    /// ([`hits_and_misses`](Self::hits_and_misses)). Allocation-free.
    #[inline]
    pub fn get(&self, key: BlockKey) -> Option<BlockHandle> {
        let shard = self.shards.padded(hash_of(&key))?;
        let found = read(&shard.lock).get(&key).map(BlockHandle::new);
        let counter = if found.is_some() {
            &shard.hits
        } else {
            &shard.misses
        };
        counter.fetch_add(1, Ordering::Relaxed);
        found
    }

    /// Lookups ([`get`](Self::get)) that found their block, and lookups that did not, since
    /// the cache was made (ICR 0024). A disabled cache counts nothing.
    ///
    /// ```
    /// use pigeonhole_cache::{BlockCache, BlockKey, Priority};
    ///
    /// let cache = BlockCache::new(1 << 20, 0);
    /// let key = BlockKey { file: 1, offset: 0 };
    /// assert!(cache.get(key).is_none());
    /// drop(cache.insert(key, vec![0; 8].into(), Priority::Normal));
    /// assert!(cache.get(key).is_some());
    /// assert_eq!(cache.hits_and_misses(), (1, 1));
    /// ```
    pub fn hits_and_misses(&self) -> (u64, u64) {
        self.shards.lookups()
    }

    /// Inserts a block (replacing any entry with the same key) and returns it pinned.
    pub fn insert(&self, key: BlockKey, data: BlockData, priority: Priority) -> BlockHandle {
        let charge = data.charge();
        let block = Arc::new(data);
        if let Some(shard) = self.shards.shard(hash_of(&key)) {
            lock(shard).insert(key, Arc::clone(&block), charge, priority);
        }
        BlockHandle::new(block)
    }

    /// Drops every unpinned block of `files` (after their SSTs or blob files are deleted), in
    /// one pass over each shard, so a compaction that removes many SSTs pays one scan, not
    /// one per file.
    ///
    /// Pinned blocks stay indexed until a later eviction; file ids are never reused, so they
    /// can only be found by holders of the old file's keys.
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

    /// Drops every unpinned block and every ghost, and returns the index's spare capacity
    /// to the allocator: for an engine releasing a closed instance's memory (D265). Pinned
    /// blocks stay indexed until their holders go and a later `clear` or eviction drops
    /// them; the capacity and the lookup counters are unchanged.
    ///
    /// ```
    /// use pigeonhole_cache::{BlockCache, BlockKey, Priority};
    ///
    /// let cache = BlockCache::new(1 << 20, 0);
    /// let held = cache.insert(BlockKey { file: 1, offset: 0 }, vec![0; 8].into(), Priority::Normal);
    /// drop(cache.insert(BlockKey { file: 2, offset: 0 }, vec![0; 8].into(), Priority::Normal));
    /// cache.clear();
    /// assert!(cache.get(BlockKey { file: 2, offset: 0 }).is_none());
    /// assert!(cache.get(BlockKey { file: 1, offset: 0 }).is_some(), "pinned by `held`");
    /// drop(held);
    /// cache.clear();
    /// assert_eq!(cache.usage(), 0);
    /// ```
    pub fn clear(&self) {
        self.shards.for_each(|s| s.clear());
    }

    /// Counts `bytes` of blocks a reader keeps pinned while it is open (an SST's top index
    /// and filters), until a matching [`sub_pinned`](Self::sub_pinned). Pinned blocks count
    /// against the capacity and are never evicted, so once they pass half of it the blocks
    /// reads come back for (index partitions, data) no longer stay cached (D215): such a rise
    /// counts a warning ([`pinned_warnings`](Self::pinned_warnings)), at most once until the
    /// pinned bytes fall below 40% of the capacity again.
    ///
    /// ```
    /// use pigeonhole_cache::BlockCache;
    ///
    /// let cache = BlockCache::new(1 << 20, 1);
    /// cache.add_pinned(400 << 10);
    /// assert_eq!((cache.pinned(), cache.pinned_warnings()), (400 << 10, 0));
    /// cache.add_pinned(200 << 10); // past half of 1 MiB
    /// cache.add_pinned(100 << 10);
    /// assert_eq!(cache.pinned_warnings(), 1);
    /// cache.sub_pinned(400 << 10); // below 40%: the next rise counts again
    /// cache.add_pinned(300 << 10);
    /// assert_eq!(cache.pinned_warnings(), 2);
    /// ```
    pub fn add_pinned(&self, bytes: usize) {
        let after = self.pinned.fetch_add(bytes as u64, Ordering::Relaxed) + bytes as u64;
        let cap = self.capacity() as u64;
        if cap > 0 && after > cap / 2 && self.pinned_armed.swap(0, Ordering::Relaxed) == 1 {
            self.pinned_warnings.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Ends the count of `bytes` an [`add_pinned`](Self::add_pinned) began.
    pub fn sub_pinned(&self, bytes: usize) {
        let after = self.pinned.fetch_sub(bytes as u64, Ordering::Relaxed) - bytes as u64;
        if after < self.capacity() as u64 / 10 * 4 {
            self.pinned_armed.store(1, Ordering::Relaxed);
        }
    }

    /// Bytes counted by [`add_pinned`](Self::add_pinned) and not yet ended.
    pub fn pinned(&self) -> usize {
        self.pinned.load(Ordering::Relaxed) as usize
    }

    /// Times the pinned bytes rose past half the capacity
    /// ([`add_pinned`](Self::add_pinned)).
    pub fn pinned_warnings(&self) -> u64 {
        self.pinned_warnings.load(Ordering::Relaxed)
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
    /// `block`'s bytes, resolved once: a block reader derefs the handle for every entry it
    /// decodes, and this skips the `Arc` and the `BlockData` variant each time.
    ptr: *const u8,
    len: usize,
}

// SAFETY: `ptr`/`len` are a shared view of bytes `block` owns and never changes (a
// `BlockData` is not mutated once in an `Arc`), so the handle is as `Send` and `Sync` as the
// `Arc<BlockData>` it holds and a `&[u8]` into it.
unsafe impl Send for BlockHandle {}
// SAFETY: as for `Send`.
unsafe impl Sync for BlockHandle {}

impl BlockHandle {
    fn new(block: Arc<BlockData>) -> Self {
        let bytes = block.bytes();
        let (ptr, len) = (bytes.as_ptr(), bytes.len());
        Self { block, ptr, len }
    }

    /// Bytes the block counts against a cache's capacity (an aligned I/O buffer's capacity,
    /// a heap buffer's length).
    pub fn charge(&self) -> usize {
        self.block.charge()
    }

    /// The block's bytes.
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        // SAFETY: `ptr`/`len` were taken from `self.block.bytes()`, which the `Arc` keeps
        // alive while `self` does. The bytes never change and never move: a `BlockData` is
        // immutable once shared, and its buffer (an `IoBuf`'s allocation or a boxed slice)
        // stays where it is when the `Arc` or the handle moves.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
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
    /// Read since it was stored or moved to `epoch` (1) or not (0): what
    /// [`RowCache::stale_unused`] asks. A flag of its own rather than the queue's hit
    /// count, which eviction passes age back to 0 for rows read all the time.
    read: AtomicU8,
}

impl Clone for RowEntry {
    fn clone(&self) -> Self {
        Self {
            family: self.family,
            epoch: self.epoch,
            row: self.row.clone(),
            encoded: self.encoded.clone(),
            read: AtomicU8::new(self.read.load(Ordering::Relaxed)),
        }
    }
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
        let entry =
            read(self.shards.shard(h)?).get_if(&h, |e| e.is(family, row) && e.epoch == epoch)?;
        // Written once, not on every hit: a hot row's line stays clean in other cores'
        // caches (#18).
        if entry.read.load(Ordering::Relaxed) == 0 {
            entry.read.store(1, Ordering::Relaxed);
        }
        Some(RowHandle { entry })
    }

    /// Caches the encoded row (an engine-defined encoding of its visible cells).
    pub fn insert(&self, family: u64, row: &[u8], epoch: u64, encoded: Vec<u8>) -> RowHandle {
        let charge = row.len() + encoded.len() + Self::ENTRY_OVERHEAD;
        let entry = Arc::new(RowEntry {
            family,
            epoch,
            row: row.into(),
            encoded,
            read: AtomicU8::new(0),
        });
        let h = Self::hash(family, row);
        if let Some(shard) = self.shards.shard(h) {
            lock(shard).insert(h, Arc::clone(&entry), charge, Priority::Normal);
        }
        RowHandle { entry }
    }

    /// Moves the row cached at epoch `from` to epoch `to`, editing its encoding in place,
    /// if `check` accepts the current encoding; otherwise leaves the entry as it is. Returns
    /// whether the row is now cached at `to`. Nothing changes when it is not cached at
    /// `from`. A [`RowHandle`] held meanwhile keeps the old encoding: the entry is copied
    /// before the edit while one is held.
    ///
    /// ```
    /// use pigeonhole_cache::RowCache;
    ///
    /// let rows = RowCache::new(1 << 20);
    /// let old = rows.insert(3, b"user:42", 10, vec![1, 2, 3]);
    /// assert!(rows.update(3, b"user:42", 10, 11, |_| true, |e| e[0] = 9));
    /// assert_eq!(&rows.get(3, b"user:42", 11).unwrap()[..], &[9, 2, 3]);
    /// assert_eq!(&old[..], &[1, 2, 3]); // the handle kept its copy
    /// assert!(rows.get(3, b"user:42", 10).is_none());
    /// // Refused: the entry stays at 11, unchanged.
    /// assert!(!rows.update(3, b"user:42", 11, 12, |_| false, |e| e[0] = 0));
    /// assert_eq!(&rows.get(3, b"user:42", 11).unwrap()[..], &[9, 2, 3]);
    /// assert!(!rows.update(3, b"user:42", 10, 12, |_| true, |_| {})); // not cached at 10
    /// ```
    pub fn update(
        &self,
        family: u64,
        row: &[u8],
        from: u64,
        to: u64,
        check: impl FnOnce(&[u8]) -> bool,
        edit: impl FnOnce(&mut Vec<u8>),
    ) -> bool {
        let h = Self::hash(family, row);
        let Some(shard) = self.shards.shard(h) else {
            return false;
        };
        let mut shard = lock(shard);
        if !shard
            .peek(&h)
            .is_some_and(|e| e.is(family, row) && e.epoch == from && check(&e.encoded))
        {
            return false;
        }
        shard.update(&h, |entry| {
            // Copied first while a handle holds it (`Arc::make_mut`, written out: loom's
            // `Arc` has no `make_mut`).
            if Arc::get_mut(entry).is_none() {
                *entry = Arc::new(RowEntry::clone(entry));
            }
            let e = Arc::get_mut(entry)?;
            edit(&mut e.encoded);
            e.epoch = to;
            e.read = AtomicU8::new(0);
            Some(e.row.len() + e.encoded.len() + Self::ENTRY_OVERHEAD)
        })
    }

    /// Whether the row is cached at an epoch other than `epoch` and was not read since it
    /// was stored or [`update`](Self::update)d to that epoch: the row changed before its
    /// cached copy served anyone, so storing it again would likely be wasted.
    ///
    /// ```
    /// use pigeonhole_cache::RowCache;
    ///
    /// let rows = RowCache::new(1 << 20);
    /// drop(rows.insert(3, b"user:42", 10, vec![1]));
    /// assert!(rows.stale_unused(3, b"user:42", 11)); // stored at 10, never read, now at 11
    /// assert!(!rows.stale_unused(3, b"user:42", 10)); // current
    /// assert!(rows.get(3, b"user:42", 10).is_some());
    /// assert!(!rows.stale_unused(3, b"user:42", 11)); // read once before it went stale
    /// assert!(rows.update(3, b"user:42", 10, 11, |_| true, |_| {}));
    /// assert!(rows.stale_unused(3, b"user:42", 12)); // moved to 11, not read there
    /// assert!(!rows.stale_unused(3, b"other", 11)); // not cached at all
    /// ```
    pub fn stale_unused(&self, family: u64, row: &[u8], epoch: u64) -> bool {
        let h = Self::hash(family, row);
        let Some(shard) = self.shards.shard(h) else {
            return false;
        };
        read(shard).peek(&h).is_some_and(|e| {
            e.is(family, row) && e.epoch != epoch && e.read.load(Ordering::Relaxed) == 0
        })
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

    /// Drops every unpinned row and every ghost, and returns the index's spare capacity to
    /// the allocator: for an engine releasing a closed instance's memory (D265). Rows a
    /// [`RowHandle`] still holds stay indexed until it goes and a later `clear` or eviction
    /// drops them.
    ///
    /// ```
    /// use pigeonhole_cache::RowCache;
    ///
    /// let rows = RowCache::new(1 << 20);
    /// drop(rows.insert(3, b"user:42", 10, vec![1, 2, 3]));
    /// rows.clear();
    /// assert!(rows.get(3, b"user:42", 10).is_none());
    /// assert_eq!(rows.usage(), 0);
    /// ```
    pub fn clear(&self) {
        self.shards.for_each(|s| s.clear());
    }

    /// Bytes currently cached (rows, encodings and a fixed per-entry overhead).
    pub fn usage(&self) -> usize {
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
