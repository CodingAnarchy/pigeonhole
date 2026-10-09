//! Offset-linked single-writer multi-reader skiplist memtable for Pigeonhole.
//!
//! One memtable per `(tablet, family)`, written only by the owning shard thread and read
//! concurrently by any thread, and by reader processes. Nodes live in the shard's arena (a
//! part of the shared-memory region) and link by `u32` offsets relative to the arena base,
//! never by pointer, so any process that maps the region can traverse them. The writer
//! publishes each node with a release store; readers load with acquire.
//!
//! Keys are complete internal keys from `pigeonhole_format::key` and values are stored
//! values, so comparison is `memcmp` and flushing is a copy. The node and header layout is in
//! `FORMAT.md` §11.6.
//!
//! Until `shm` lands (and in unit tests) use [`ArenaRegion::heap`] as the arena.
//!
//! ```
//! use pigeonhole_format::{Cursor, Kind, encode_key};
//! use pigeonhole_memtable::{ArenaRegion, Memtable, ShardArena};
//!
//! let mut arena = ShardArena::new(ArenaRegion::heap(1 << 20), 64 * 1024);
//! let mut memtable = Memtable::create(&mut arena)?;
//!
//! let mut key = Vec::new();
//! encode_key(&mut key, b"row", b"qual", 10, 1, Kind::Put).unwrap();
//! memtable.insert(&mut arena, &key, b"\x00hello")?;
//!
//! let reader = memtable.reader(); // usable from any thread
//! let mut it = reader.iter();
//! it.seek_to_first()?;
//! assert!(it.valid());
//! assert_eq!(it.key(), &key[..]);
//! assert_eq!(it.value(), b"\x00hello");
//! # Ok::<(), pigeonhole_memtable::Error>(())
//! ```
//!
//! # Memory ordering
//!
//! The writer fully writes a node (header, tower, key, value) with plain stores, then links
//! it bottom-up with a release store into each predecessor's `next[i]`, and finally bumps the
//! header's `count` with a release store. A reader acquires `next[i]` (or `count`) and may
//! then read the node's bytes plainly: they are immutable until the memtable is retired and
//! its chunks reclaimed, which the engine does only once no view that lists the memtable is
//! pinned anywhere. The same discipline holds across processes, since a mapping of the
//! region is ordinary memory.
//!
//! # Reclamation
//!
//! A retired memtable's chunks go back to the arena through [`ShardArena::reclaim`], which
//! is safe to call at any time: reuse is deferred while any handle of that memtable created
//! in this process (a [`MemtableReader`], [`MemIter`] or [`ArenaSlice`], from
//! [`Memtable::reader`] or [`MemtableReader::open`]) is alive, so a `&[u8]` borrowed from
//! the arena can never be overwritten. Reader *processes* are protected by the shared-memory
//! protocol instead: the engine calls `reclaim` only after no reader slot pins a view that
//! lists the memtable. A reader process that nevertheless reads reused memory sees garbage
//! but never faults: every node a cursor reaches is bounds-checked, a link above a node's
//! height is rejected, and a traversal longer than the entry count is reported as a cycle,
//! all as [`Error::Corrupt`].
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
// `unsafe` is permitted in this crate; every block carries a `// SAFETY:` argument.
#![deny(unsafe_op_in_unsafe_fn)]

// Arena integers are native-endian in memory while FORMAT.md §11.6 fixes them as
// little-endian; refuse to build where the two differ (decision D56).
#[cfg(not(target_endian = "little"))]
compile_error!("pigeonhole-memtable supports little-endian targets only (FORMAT.md §11.6)");

use std::cmp::Ordering as Cmp;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};

use pigeonhole_format::key::split_suffix;
use pigeonhole_format::shm::memtable as layout;
use pigeonhole_format::version::MEMTABLE_MAGIC;
use pigeonhole_format::{Cursor, Seqno};
use pigeonhole_io::SharedRegion;

mod mem;

use mem::{Mem, Ordering};

#[cfg(all(test, loom))]
mod loom_tests;
#[cfg(all(test, not(loom)))]
mod tests;

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Memtable errors.
///
/// ```
/// use pigeonhole_memtable::Error;
///
/// assert_eq!(Error::ArenaFull.to_string(), "shard arena is full");
/// assert_eq!(
///     Error::Corrupt("node height").to_string(),
///     "corrupt memtable: node height"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// The shard arena has no chunk left: the engine must flush or stall writes.
    ArenaFull,
    /// One entry is larger than the arena could ever hold.
    EntryTooLarge,
    /// A reader found an offset or length outside the arena, or a bad header (possible
    /// only with a buggy or crashed writer; checked so a reader never faults).
    Corrupt(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ArenaFull => f.write_str("shard arena is full"),
            Error::EntryTooLarge => f.write_str("entry is larger than the arena can hold"),
            Error::Corrupt(what) => write!(f, "corrupt memtable: {what}"),
        }
    }
}

impl std::error::Error for Error {}

/// Maximum skiplist height (the head node's height).
const MAX_HEIGHT: usize = layout::MAX_HEIGHT;

/// Levels a cursor records for its forward seeks' finger search: with towers growing by 4
/// per level, the lowest 4 reach about 64 entries ahead (a row's columns and versions, the
/// forward seeks a read makes); a forward seek farther than that searches from the top. Few
/// levels keep the cursor small: it is built and moved for every read.
const FINGER_LEVELS: usize = 4;

/// A search's step count, against a budget that catches a link cycle.
struct Walk {
    steps: usize,
    budget: usize,
}
/// Bytes reserved at the start of every arena; offset `0` is null.
const RESERVED: usize = 64;
/// Length of the head node: a tower of [`MAX_HEIGHT`] links and no key or value.
const HEAD_LEN: usize = layout::N_TOWER + 4 * MAX_HEIGHT;
/// Offsets are `u32`, so an arena holds at most this many bytes.
const MAX_ARENA_LEN: usize = 1 << 32;
/// Header layout version written to every memtable header.
const HEADER_VERSION: u16 = 1;
/// Header flag bit: frozen.
const FLAG_FROZEN: u8 = 1;
/// The null offset.
const NULL: u32 = 0;
/// Extra traversal steps allowed beyond the entry count before a walk is called a cycle
/// (one level stop per level, plus nodes linked but not yet counted).
const STEP_SLACK: usize = MAX_HEIGHT + 16;

/// One strong reference per live in-process handle of a memtable (the writer, each reader,
/// cursor and slice, and the `Retired` token). `ShardArena::reclaim` reuses the chunks only
/// once the token holds the last reference.
#[derive(Debug)]
struct Pin;

/// Shared by every clone of an [`ArenaRegion`]: the pins of the memtables this process
/// created, by root, so a reader opened by root in the writer process pins the same memtable.
#[derive(Debug, Default)]
struct Registry {
    pins: Mutex<HashMap<u32, Weak<Pin>>>,
}

impl Registry {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u32, Weak<Pin>>> {
        self.pins.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn register(&self, root: u32, pin: &Arc<Pin>) {
        self.lock().insert(root, Arc::downgrade(pin));
    }

    /// The pin of the memtable at `root` if it was created in this process, else a fresh one
    /// (a reader process: nothing reclaims there).
    fn pin_for(&self, root: u32) -> Arc<Pin> {
        self.lock()
            .get(&root)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| Arc::new(Pin))
    }

    fn forget(&self, root: u32, pin: &Arc<Pin>) {
        let mut pins = self.lock();
        if pins
            .get(&root)
            .is_some_and(|w| Weak::ptr_eq(w, &Arc::downgrade(pin)))
        {
            pins.remove(&root);
        }
    }
}

/// Offset of link `level` in the tower of the node at `node`.
#[inline]
fn tower(node: u32, level: usize) -> usize {
    node as usize + layout::N_TOWER + 4 * level
}

/// A byte range of a [`SharedRegion`] holding one shard's arena. Cheap to clone; keeps the
/// mapping alive.
///
/// ```
/// use pigeonhole_io::SharedRegion;
/// use pigeonhole_memtable::ArenaRegion;
///
/// // The engine hands the memtable its shard's slice of the shared-memory region.
/// let region = SharedRegion::heap(4 << 20);
/// let arena = ArenaRegion::new(region, 2 << 20, 2 << 20).unwrap();
/// assert_eq!(arena.len(), 2 << 20);
///
/// // Tests and the engine before `shm` lands use a private heap arena.
/// let mock = ArenaRegion::heap(1 << 20);
/// assert!(!mock.is_empty());
/// ```
#[derive(Debug, Clone)]
pub struct ArenaRegion {
    mem: Mem,
    registry: Arc<Registry>,
}

impl ArenaRegion {
    /// A private heap-backed arena of `len` bytes (the mock until `shm` lands).
    pub fn heap(len: usize) -> Self {
        assert!(len <= MAX_ARENA_LEN, "arena longer than 4 GiB");
        Self {
            mem: Mem::heap(len),
            registry: Arc::default(),
        }
    }

    /// `[offset, offset + len)` of `region`. `offset` must be 64-byte aligned and `len` at
    /// most 4 GiB (offsets are `u32`).
    ///
    /// # Errors
    /// [`Error::Corrupt`] if the range is misaligned, too long or outside the region.
    #[cfg(not(loom))]
    pub fn new(region: SharedRegion, offset: usize, len: usize) -> Result<Self> {
        if !offset.is_multiple_of(64) {
            return Err(Error::Corrupt("arena offset is not 64-byte aligned"));
        }
        if len > MAX_ARENA_LEN {
            return Err(Error::Corrupt("arena longer than 4 GiB"));
        }
        if !offset
            .checked_add(len)
            .is_some_and(|end| end <= region.len())
        {
            return Err(Error::Corrupt("arena extends past its region"));
        }
        Ok(Self {
            mem: Mem::new(region, offset, len),
            registry: Arc::default(),
        })
    }

    /// Shared regions are not modeled under loom; only [`ArenaRegion::heap`] works there.
    #[cfg(loom)]
    pub fn new(region: SharedRegion, offset: usize, len: usize) -> Result<Self> {
        let _ = (region, offset, len);
        Err(Error::Corrupt("shared regions are not modeled under loom"))
    }

    /// Length in bytes.
    pub fn len(&self) -> usize {
        self.mem.len()
    }

    /// Whether the arena is empty.
    pub fn is_empty(&self) -> bool {
        self.mem.len() == 0
    }

    /// Whether `other` is a clone of this arena (the same mapping and pin registry).
    fn is_same(&self, other: &ArenaRegion) -> bool {
        Arc::ptr_eq(&self.registry, &other.registry)
    }
}

/// A contiguous run of chunks: `[first, first + count)` chunk indices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Run {
    first: u32,
    count: u32,
}

/// The writer-side chunk allocator over one shard's arena. Owned by the shard thread; its
/// free list is private memory (a restarted writer rebuilds the region anyway).
///
/// ```
/// use pigeonhole_memtable::{ArenaRegion, Memtable, ShardArena};
///
/// let mut arena = ShardArena::new(ArenaRegion::heap(1 << 20), ShardArena::DEFAULT_CHUNK);
/// assert_eq!(arena.free_bytes(), 1 << 20);
///
/// let memtable = Memtable::create(&mut arena)?;
/// assert_eq!(arena.free_bytes(), (1 << 20) - ShardArena::DEFAULT_CHUNK);
///
/// // After its flush is in the manifest and no view pins it, its chunks come back.
/// arena.reclaim(memtable.retire());
/// assert_eq!(arena.free_bytes(), 1 << 20);
///
/// // Reuse waits for in-process readers: a cursor keeps the chunks it borrows from.
/// let mut memtable = Memtable::create(&mut arena)?;
/// let reader = memtable.reader();
/// arena.reclaim(memtable.retire());
/// assert_eq!(arena.free_bytes(), (1 << 20) - ShardArena::DEFAULT_CHUNK);
/// drop(reader);
/// let next = Memtable::create(&mut arena)?; // the deferred chunk is free again
/// assert_eq!(next.root(), 64);
/// # Ok::<(), pigeonhole_memtable::Error>(())
/// ```
#[derive(Debug)]
pub struct ShardArena {
    region: ArenaRegion,
    chunk_size: usize,
    /// `true` for each free chunk.
    free: Vec<bool>,
    free_count: usize,
    /// Retired memtables whose chunks some in-process handle still borrows.
    deferred: Vec<Retired>,
}

impl ShardArena {
    /// Default chunk size (256 KiB).
    pub const DEFAULT_CHUNK: usize = 256 * 1024;

    /// Smallest chunk size accepted.
    const MIN_CHUNK: usize = 1024;

    /// An allocator handing out `chunk_size` chunks of `region`. Entries larger than a chunk
    /// take a contiguous run of chunks.
    ///
    /// # Panics
    /// If `chunk_size` is below 1 KiB or not a multiple of 64.
    pub fn new(region: ArenaRegion, chunk_size: usize) -> Self {
        assert!(
            chunk_size >= Self::MIN_CHUNK && chunk_size.is_multiple_of(64),
            "chunk size must be a multiple of 64 and at least 1 KiB"
        );
        let chunks = region.len() / chunk_size;
        Self {
            region,
            chunk_size,
            free: vec![true; chunks],
            free_count: chunks,
            deferred: Vec::new(),
        }
    }

    /// The region (for building readers).
    pub fn region(&self) -> &ArenaRegion {
        &self.region
    }

    /// Bytes not allocated to any memtable. Chunks of a retired memtable count once no
    /// in-process handle of it is left (see [`ShardArena::reclaim`]).
    pub fn free_bytes(&self) -> usize {
        self.free_count * self.chunk_size
    }

    /// Usable bytes of the largest run of contiguous free chunks: the largest entry an
    /// allocation can place now. An entry larger than a chunk needs one such run, so free
    /// bytes elsewhere do not help it.
    pub fn largest_free_run(&self) -> usize {
        let mut best = 0;
        self.free_runs_each(|first, count| best = best.max(self.usable(first, count)));
        best
    }

    /// How many blocks of `chunks` contiguous free chunks the free runs are sure to hold
    /// after any `removed` chunks are allocated: `Σ ⌊run / chunks⌋` minus the most blocks
    /// that many chunks could break.
    ///
    /// Every allocation takes its chunks from the start of a free run (the first one long
    /// enough). Taking chunks from a run's start breaks its first block only once its spare
    /// chunks (`run mod chunks`) are gone, and each further block costs `chunks` more, so
    /// the most blocks `removed` chunks can break is found by spending them on the cheapest
    /// breaks first. While at least one block is left, an allocation of up to `chunks`
    /// chunks finds a run (issue #141).
    pub fn blocks_left(&self, chunks: usize, removed: usize) -> usize {
        let chunks = chunks.max(1);
        let mut blocks = 0;
        // The cost of breaking each run's first block; later ones cost `chunks` each.
        let mut first_costs = Vec::new();
        self.free_runs_each(|_, count| {
            if count >= chunks {
                blocks += count / chunks;
                first_costs.push(count % chunks + 1);
            }
        });
        first_costs.sort_unstable();
        let (mut budget, mut broken) = (removed, 0);
        for cost in first_costs {
            if cost > budget {
                break;
            }
            budget -= cost;
            broken += 1;
        }
        broken += budget / chunks;
        blocks.saturating_sub(broken)
    }

    /// The chunk size in bytes.
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Calls `f(first, count)` for each run of free chunks.
    fn free_runs_each(&self, mut f: impl FnMut(usize, usize)) {
        let n = self.free.len();
        let mut first = 0;
        while first < n {
            if !self.free[first] {
                first += 1;
                continue;
            }
            let mut count = 1;
            while first + count < n && self.free[first + count] {
                count += 1;
            }
            f(first, count);
            first += count;
        }
    }

    /// Returns a retired memtable's chunks to the free list. Call only once no reader slot
    /// pins a view that lists it (reader processes have no other protection). Handles in
    /// this process need no care: while any [`MemtableReader`], [`MemIter`] or
    /// [`ArenaSlice`] of the memtable is alive, its chunks stay out of use and become free
    /// on a later `reclaim` or allocation, after the last handle is dropped.
    ///
    /// # Panics
    /// If a chunk is already free (a double reclaim, or a token from another arena).
    pub fn reclaim(&mut self, retired: Retired) {
        self.deferred.push(retired);
        self.release_unreferenced();
    }

    /// Frees every deferred memtable whose token holds the last in-process reference.
    fn release_unreferenced(&mut self) {
        let mut i = 0;
        while i < self.deferred.len() {
            if Arc::strong_count(&self.deferred[i].pin) == 1 {
                // Pairs with the release decrement of the last handle's drop, so its reads
                // of the chunks happen before the writer reuses them.
                mem::acquire_fence();
                let retired = self.deferred.swap_remove(i);
                self.free_runs(&retired);
            } else {
                i += 1;
            }
        }
    }

    fn free_runs(&mut self, retired: &Retired) {
        for run in &retired.runs {
            for c in run.first..run.first + run.count {
                let slot = &mut self.free[c as usize];
                assert!(!*slot, "chunk {c} reclaimed twice");
                *slot = true;
                self.free_count += 1;
            }
        }
        self.region.registry.forget(retired.root, &retired.pin);
    }

    /// Usable bytes of a run of `count` chunks starting at chunk `first` (the arena's first
    /// 64 bytes are reserved).
    fn usable(&self, first: usize, count: usize) -> usize {
        let bytes = count * self.chunk_size;
        if first == 0 { bytes - RESERVED } else { bytes }
    }

    /// Allocates the lowest run of chunks with at least `bytes` usable bytes.
    fn allocate(&mut self, bytes: usize) -> Result<Run> {
        self.release_unreferenced();
        let n = self.free.len();
        if n == 0 || bytes > self.usable(0, n) {
            return Err(Error::EntryTooLarge);
        }
        let mut first = 0;
        while first < n {
            if !self.free[first] {
                first += 1;
                continue;
            }
            let mut count = 1;
            while self.usable(first, count) < bytes {
                if first + count < n && self.free[first + count] {
                    count += 1;
                } else {
                    break;
                }
            }
            if self.usable(first, count) >= bytes {
                for c in first..first + count {
                    self.free[c] = false;
                }
                self.free_count -= count;
                return Ok(Run {
                    first: first as u32,
                    count: count as u32,
                });
            }
            first += count;
        }
        Err(Error::ArenaFull)
    }

    /// Byte range `[start, end)` of `run`, excluding the reserved prefix.
    fn span(&self, run: Run) -> (usize, usize) {
        let start = run.first as usize * self.chunk_size;
        let end = start + run.count as usize * self.chunk_size;
        (start.max(RESERVED), end)
    }
}

/// The writer's handle to one memtable.
///
/// ```
/// use pigeonhole_format::{Kind, encode_key};
/// use pigeonhole_memtable::{ArenaRegion, Memtable, ShardArena};
///
/// let mut arena = ShardArena::new(ArenaRegion::heap(1 << 20), 64 * 1024);
/// let mut memtable = Memtable::create(&mut arena)?;
///
/// let mut key = Vec::new();
/// for seqno in 1..=3 {
///     key.clear();
///     encode_key(&mut key, b"row", b"q", 100 + seqno, seqno, Kind::Put).unwrap();
///     memtable.insert(&mut arena, &key, b"\x00v")?;
/// }
/// assert_eq!(memtable.len(), 3);
/// assert_eq!(memtable.seqno_range(), Some((1, 3)));
/// assert_eq!(memtable.allocated_bytes(), 64 * 1024); // one chunk so far
///
/// memtable.freeze();
/// assert!(memtable.is_frozen());
/// let root = memtable.root(); // goes into the published view
/// let retired = memtable.retire(); // once its SST is in the manifest
/// arena.reclaim(retired); // once no view listing `root` is pinned
/// # Ok::<(), pigeonhole_memtable::Error>(())
/// ```
#[derive(Debug)]
pub struct Memtable {
    region: ArenaRegion,
    root: u32,
    head: u32,
    /// Chunk runs owned, in allocation order.
    runs: Vec<Run>,
    /// Next free byte of the current run and its end.
    bump: usize,
    limit: usize,
    count: u32,
    /// Node and header bytes used (the header's `bytes` field).
    used: u64,
    /// Bytes of chunks owned.
    allocated: usize,
    min_seqno: Seqno,
    max_seqno: Seqno,
    frozen: bool,
    rng: u64,
    /// The insert splice: per level, the node a key sorting right after the last one
    /// inserted links behind. Below the last inserted node's height it is that node itself;
    /// at and above it, the last node at that level that sorts before it (the head when
    /// none does, and at every level before any insert). A key between the last node and its
    /// level-0 successor has exactly these predecessors, so inserts in key order (a row's
    /// cells, rising row keys) skip the search. Only this writer links nodes and none is
    /// removed, so the splice stays exact.
    splice: [u32; MAX_HEIGHT],
    pin: Arc<Pin>,
}

impl Memtable {
    /// Creates an empty memtable in `arena`.
    ///
    /// # Errors
    /// [`Error::ArenaFull`] if no chunk is free.
    pub fn create(arena: &mut ShardArena) -> Result<Memtable> {
        let run = arena.allocate(layout::HEADER_LEN + HEAD_LEN)?;
        let (start, end) = arena.span(run);
        let region = arena.region.clone();
        let mem = &region.mem;
        let root = start;
        let head = root + layout::HEADER_LEN;
        debug_assert!(root.is_multiple_of(64));

        // Header. Plain writes: the root is not published until this returns.
        mem.write_u32(root + layout::H_MAGIC, MEMTABLE_MAGIC);
        let [v0, v1] = HEADER_VERSION.to_le_bytes();
        mem.write(root + layout::H_VERSION, &[v0, v1, 0, 0]);
        mem.write_u32(root + layout::H_HEAD, head as u32);
        mem.store_u32(root + layout::H_COUNT, 0, Ordering::Relaxed);
        let used = (layout::HEADER_LEN + HEAD_LEN) as u64;
        mem.store_u64(root + layout::H_BYTES, used, Ordering::Relaxed);
        mem.store_u64(root + layout::H_MAX_SEQNO, 0, Ordering::Relaxed);
        mem.store_u64(root + layout::H_MIN_SEQNO, 0, Ordering::Relaxed);
        mem.write(
            root + layout::H_MIN_SEQNO + 8,
            &[0; layout::HEADER_LEN - 40],
        );

        // Head node: no key or value, a tower of null links (the chunk may be reused memory).
        mem.write_u32(head + layout::N_KEY_LEN, 0);
        mem.write_u32(head + layout::N_VALUE_LEN, 0);
        mem.write(head + layout::N_HEIGHT, &[MAX_HEIGHT as u8, 0, 0, 0]);
        for level in 0..MAX_HEIGHT {
            mem.write_u32(tower(head as u32, level), NULL);
        }

        let pin = Arc::new(Pin);
        region.registry.register(root as u32, &pin);
        Ok(Memtable {
            region,
            root: root as u32,
            head: head as u32,
            runs: vec![run],
            bump: head + HEAD_LEN,
            limit: end,
            count: 0,
            used,
            allocated: run.count as usize * arena.chunk_size,
            min_seqno: 0,
            max_seqno: 0,
            frozen: false,
            rng: 0x9E37_79B9_7F4A_7C15 ^ (root as u64),
            splice: [head as u32; MAX_HEIGHT],
            pin,
        })
    }

    /// Inserts one entry. `key` is a full internal key (unique: it contains the seqno);
    /// `value` is a stored value. Visible to readers when this returns.
    ///
    /// A duplicate key is linked as another entry; readers see both, in unspecified order.
    ///
    /// # Errors
    /// [`Error::ArenaFull`] if the entry does not fit in the chunks left,
    /// [`Error::EntryTooLarge`] if it could never fit, [`Error::Corrupt`] if `key` is shorter
    /// than an internal key suffix.
    ///
    /// # Panics
    /// If the memtable is frozen, or `arena` is not the arena it was created in.
    pub fn insert(&mut self, arena: &mut ShardArena, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!self.frozen, "insert into a frozen memtable");
        assert!(
            self.region.is_same(&arena.region),
            "memtable inserted through a different shard arena"
        );
        let (_, _, seqno, _) =
            split_suffix(key).map_err(|_| Error::Corrupt("insert key is not an internal key"))?;
        let key_len = u32::try_from(key.len()).map_err(|_| Error::EntryTooLarge)?;
        let value_len = u32::try_from(value.len()).map_err(|_| Error::EntryTooLarge)?;
        let height = self.random_height();
        let node_len = (layout::N_TOWER + 4 * height)
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()))
            .ok_or(Error::EntryTooLarge)?
            .next_multiple_of(4);

        if self.limit - self.bump < node_len {
            let run = arena.allocate(node_len)?;
            let (start, end) = arena.span(run);
            self.runs.push(run);
            self.allocated += run.count as usize * arena.chunk_size;
            self.bump = start;
            self.limit = end;
        }
        let node = self.bump as u32;
        debug_assert!(
            node != NULL && node.is_multiple_of(4) && (node as usize) < self.region.len()
        );

        // Predecessors at every level: the last insert's when the key sorts right after
        // the last key inserted (between it and its successor, two comparisons), else from a
        // search down from the top of the head tower.
        let mem = &self.region.mem;
        let mut prev = self.splice;
        let after_last = {
            let last = prev[0];
            (last == self.head || self.key_cmp(last, key) == Cmp::Less) && {
                let next = mem.load_u32(tower(last, 0), Ordering::Relaxed);
                next == NULL || self.key_cmp(next, key) != Cmp::Less
            }
        };
        if !after_last {
            let mut cur = self.head;
            for level in (0..MAX_HEIGHT).rev() {
                loop {
                    let next = mem.load_u32(tower(cur, level), Ordering::Relaxed);
                    if next == NULL || self.key_cmp(next, key) != Cmp::Less {
                        break;
                    }
                    cur = next;
                }
                prev[level] = cur;
            }
        }

        // Fill the node; nothing links to it yet, so these are plain writes.
        let off = node as usize;
        mem.write_u32(off + layout::N_KEY_LEN, key_len);
        mem.write_u32(off + layout::N_VALUE_LEN, value_len);
        mem.write(off + layout::N_HEIGHT, &[height as u8, 0, 0, 0]);
        for (level, &p) in prev.iter().enumerate().take(height) {
            let next = mem.load_u32(tower(p, level), Ordering::Relaxed);
            mem.write_u32(tower(node, level), next);
        }
        let key_off = off + layout::N_TOWER + 4 * height;
        mem.write(key_off, key);
        mem.write(key_off + key.len(), value);

        // Publish: link bottom-up, each with release so a reader that acquires the link
        // sees the whole node.
        for (level, &p) in prev.iter().enumerate().take(height) {
            debug_assert!(p != NULL && p.is_multiple_of(4) && (p as usize) < self.region.len());
            mem.store_u32(tower(p, level), node, Ordering::Release);
        }

        for (level, p) in prev.iter().enumerate() {
            self.splice[level] = if level < height { node } else { *p };
        }
        self.bump += node_len;
        self.used += node_len as u64;
        self.count += 1;
        if self.count == 1 || seqno < self.min_seqno {
            self.min_seqno = seqno;
            mem.store_u64(
                self.root as usize + layout::H_MIN_SEQNO,
                seqno,
                Ordering::Relaxed,
            );
        }
        if seqno > self.max_seqno {
            self.max_seqno = seqno;
            mem.store_u64(
                self.root as usize + layout::H_MAX_SEQNO,
                seqno,
                Ordering::Relaxed,
            );
        }
        mem.store_u64(
            self.root as usize + layout::H_BYTES,
            self.used,
            Ordering::Relaxed,
        );
        // Release: a reader that sees this count sees every node counted.
        mem.store_u32(
            self.root as usize + layout::H_COUNT,
            self.count,
            Ordering::Release,
        );
        Ok(())
    }

    /// Compares the key of the (writer-trusted) node at `node` with `key`.
    #[inline]
    fn key_cmp(&self, node: u32, key: &[u8]) -> Cmp {
        let mem = &self.region.mem;
        let off = node as usize;
        let height = mem.read_u8(off + layout::N_HEIGHT) as usize;
        let key_len = mem.read_u32(off + layout::N_KEY_LEN) as usize;
        mem.cmp(off + layout::N_TOWER + 4 * height, key_len, key)
    }

    /// A geometric height with ratio 1/4, from a private xorshift generator.
    fn random_height(&mut self) -> usize {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        let mut height = 1;
        while height < MAX_HEIGHT && x & 3 == 0 {
            height += 1;
            x >>= 2;
        }
        height
    }

    /// Marks the memtable immutable (sets the frozen flag readers can see).
    pub fn freeze(&mut self) {
        self.frozen = true;
        self.region.mem.fetch_or_u8(
            self.root as usize + layout::H_FLAGS,
            FLAG_FROZEN,
            Ordering::Release,
        );
    }

    /// Whether frozen.
    pub fn is_frozen(&self) -> bool {
        self.frozen
    }

    /// Entries inserted.
    pub fn len(&self) -> usize {
        self.count as usize
    }

    /// Whether no entry was inserted.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Bytes allocated in the arena (drives freezing): the chunks this memtable owns, which
    /// is what the shard's arena budget loses to it. The header's `bytes` field counts the
    /// bytes actually written.
    pub fn allocated_bytes(&self) -> usize {
        self.allocated
    }

    /// Smallest and largest seqno inserted, or `None` if empty.
    pub fn seqno_range(&self) -> Option<(Seqno, Seqno)> {
        (self.count > 0).then_some((self.min_seqno, self.max_seqno))
    }

    /// Offset of the header within the arena; published in views.
    pub fn root(&self) -> u32 {
        self.root
    }

    /// A reader for other threads. While it (or a cursor or slice taken from it) is alive,
    /// the memtable's chunks are not reused.
    pub fn reader(&self) -> MemtableReader {
        MemtableReader {
            region: self.region.clone(),
            root: self.root,
            head: self.head,
            pin: Arc::clone(&self.pin),
        }
    }

    /// Stops using the memtable after its flush is in the manifest. The returned token goes
    /// to [`ShardArena::reclaim`] when no reader slot pins a view that lists it.
    pub fn retire(self) -> Retired {
        Retired {
            runs: self.runs,
            root: self.root,
            pin: self.pin,
        }
    }
}

/// A retired memtable's chunks, awaiting reclamation.
#[derive(Debug)]
#[must_use = "retired chunks leak unless passed to ShardArena::reclaim"]
pub struct Retired {
    runs: Vec<Run>,
    root: u32,
    pin: Arc<Pin>,
}

/// A validated node: where its parts are.
#[derive(Debug, Clone, Copy)]
struct Node {
    off: u32,
    height: usize,
    key_off: usize,
    key_len: usize,
    value_off: usize,
    value_len: usize,
}

/// A read handle to a memtable, usable from any thread or process. Cheap to clone.
///
/// ```
/// use pigeonhole_format::{Cursor, Kind, encode_key};
/// use pigeonhole_memtable::{ArenaRegion, Memtable, MemtableReader, ShardArena};
///
/// let mut arena = ShardArena::new(ArenaRegion::heap(1 << 20), 64 * 1024);
/// let mut memtable = Memtable::create(&mut arena)?;
/// let mut key = Vec::new();
/// encode_key(&mut key, b"row", b"q", 1, 1, Kind::Put).unwrap();
/// memtable.insert(&mut arena, &key, b"\x00v")?;
///
/// // A reader process opens the root it finds in the published view.
/// let reader = MemtableReader::open(arena.region().clone(), memtable.root())?;
/// assert_eq!(reader.len(), 1);
/// let mut it = reader.iter();
/// it.seek(&key)?;
/// assert!(it.valid() && it.key() == &key[..]);
/// # Ok::<(), pigeonhole_memtable::Error>(())
/// ```
#[derive(Debug, Clone)]
pub struct MemtableReader {
    region: ArenaRegion,
    root: u32,
    head: u32,
    /// Keeps the memtable's chunks from being reused while this handle lives (writer
    /// process only; in a reader process it pins nothing).
    pin: Arc<Pin>,
}

impl MemtableReader {
    /// Opens the memtable whose header is at `root` in `region` (reader processes use the
    /// root from the published view). Validates the header. In the writer process the
    /// reader pins the memtable like [`Memtable::reader`] does, provided `region` is a clone
    /// of the arena the memtable was created in.
    ///
    /// # Errors
    /// [`Error::Corrupt`] if the header is misplaced, has the wrong magic or version, or
    /// names an invalid head node.
    pub fn open(region: ArenaRegion, root: u32) -> Result<MemtableReader> {
        let off = root as usize;
        if root == NULL || !off.is_multiple_of(8) {
            return Err(Error::Corrupt("memtable root is misaligned"));
        }
        if off + layout::HEADER_LEN > region.len() {
            return Err(Error::Corrupt("memtable header outside arena"));
        }
        let mem = &region.mem;
        if mem.read_u32(off + layout::H_MAGIC) != MEMTABLE_MAGIC {
            return Err(Error::Corrupt("memtable header magic"));
        }
        let version = u16::from_le_bytes([
            mem.read_u8(off + layout::H_VERSION),
            mem.read_u8(off + layout::H_VERSION + 1),
        ]);
        if version != HEADER_VERSION {
            return Err(Error::Corrupt("memtable header version"));
        }
        let head = mem.read_u32(off + layout::H_HEAD);
        let pin = region.registry.pin_for(root);
        let reader = MemtableReader {
            region,
            root,
            head,
            pin,
        };
        let node = reader.node(head)?;
        if node.height != MAX_HEIGHT || node.key_len != 0 || node.value_len != 0 {
            return Err(Error::Corrupt("memtable head node"));
        }
        Ok(reader)
    }

    /// Entries visible now.
    pub fn len(&self) -> usize {
        self.region
            .mem
            .load_u32(self.root as usize + layout::H_COUNT, Ordering::Acquire) as usize
    }

    /// Whether no entry is visible.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// A cursor, initially unpositioned. Holds its own clone of the reader, so it can be
    /// stored beside the view that keeps the arena memory alive.
    pub fn iter(&self) -> MemIter {
        MemIter {
            reader: self.clone(),
            node: None,
            preds: [NULL; FINGER_LEVELS],
            steps: 0,
            budget: 0,
            #[cfg(loom)]
            key_buf: Vec::new(),
            #[cfg(loom)]
            value_buf: Vec::new(),
        }
    }

    /// The arena this memtable lives in.
    pub fn region(&self) -> &ArenaRegion {
        &self.region
    }

    /// Validates the node at `off` (which must be non-null) and locates its parts, so that
    /// a bad link from a buggy or crashed writer surfaces as [`Error::Corrupt`], never as a
    /// fault.
    #[inline]
    fn node(&self, off: u32) -> Result<Node> {
        let len = self.region.len() as u64;
        let start = u64::from(off);
        if off == NULL || !off.is_multiple_of(4) {
            return Err(Error::Corrupt("node offset"));
        }
        if start + layout::N_TOWER as u64 > len {
            return Err(Error::Corrupt("node header outside arena"));
        }
        let mem = &self.region.mem;
        let height = mem.read_u8(off as usize + layout::N_HEIGHT) as usize;
        if height == 0 || height > MAX_HEIGHT {
            return Err(Error::Corrupt("node height"));
        }
        let key_len = mem.read_u32(off as usize + layout::N_KEY_LEN);
        let value_len = mem.read_u32(off as usize + layout::N_VALUE_LEN);
        let key_off = start + (layout::N_TOWER + 4 * height) as u64;
        let value_off = key_off + u64::from(key_len);
        if value_off + u64::from(value_len) > len {
            return Err(Error::Corrupt("node outside arena"));
        }
        Ok(Node {
            off,
            height,
            key_off: key_off as usize,
            key_len: key_len as usize,
            value_off: value_off as usize,
            value_len: value_len as usize,
        })
    }

    /// The first node whose key is `>= target`, or `None`, recording in `preds` the last
    /// node before `target` at each of the lowest [`FINGER_LEVELS`] levels. With `finger`,
    /// `preds` holds those of an earlier search, used when they are all before `target` (a
    /// finger search, for a forward seek; otherwise a full search): it climbs from level 0 to the first level whose next
    /// node is at or past `target`, and descends from there, so its cost grows with how far
    /// the target is, not with the memtable's size; a target past the reach of the recorded
    /// levels gets a full search. Only reads: nodes are never unlinked, so `preds` stay
    /// valid lower bounds while writers insert, and a node linked after the climb read its
    /// predecessor's link is met by the descent like any other.
    ///
    /// Returns the level-0 node it stopped at rather than re-reading the predecessor's link:
    /// the writer may link a new node below `target` there between the two loads.
    fn search(
        &self,
        target: &[u8],
        preds: &mut [u32; FINGER_LEVELS],
        finger: bool,
    ) -> Result<Option<Node>> {
        let mut walk = Walk {
            steps: 0,
            budget: self.len() + STEP_SLACK,
        };
        // The recorded predecessors are lower bounds only if they are all before `target`:
        // the level-0 one is the last of them, so it alone is compared. A target before the
        // last seek's (allowed when the cursor has moved off its current key or run out)
        // gets a full search.
        if finger && self.before(preds[0], target)? {
            // The lowest recorded level whose next node is at or past `target`.
            let mut start = None;
            for (l, &pred) in preds.iter().enumerate() {
                let next = self.region.mem.load_u32(tower(pred, l), Ordering::Acquire);
                let past = next == NULL || {
                    let n = self.node(next)?;
                    walk.steps += 1;
                    self.region.mem.cmp(n.key_off, n.key_len, target) != Cmp::Less
                };
                if past {
                    start = Some(l);
                    break;
                }
            }
            if let Some(top) = start {
                // Each level starts from its recorded predecessor (every node between is
                // before the earlier target) until the walk first moves forward, after which
                // it is past every recorded predecessor below.
                let mut cur = preds[top];
                let mut moved = false;
                for level in (1..=top).rev() {
                    if !moved {
                        cur = preds[level];
                    }
                    let from = cur;
                    self.walk_level(&mut walk, &mut cur, level, target)?;
                    moved |= cur != from;
                    preds[level] = cur;
                }
                if !moved {
                    cur = preds[0];
                }
                let found = self.walk_level(&mut walk, &mut cur, 0, target)?;
                preds[0] = cur;
                return Ok(found);
            }
            // Farther than the recorded levels reach: a full search.
        }
        let mut cur = self.head;
        for level in (FINGER_LEVELS..MAX_HEIGHT).rev() {
            self.walk_level(&mut walk, &mut cur, level, target)?;
        }
        for level in (1..FINGER_LEVELS).rev() {
            self.walk_level(&mut walk, &mut cur, level, target)?;
            preds[level] = cur;
        }
        let found = self.walk_level(&mut walk, &mut cur, 0, target)?;
        preds[0] = cur;
        Ok(found)
    }

    /// Whether node `off` (the head, or a linked node) sorts before `target`.
    fn before(&self, off: u32, target: &[u8]) -> Result<bool> {
        if off == self.head {
            return Ok(true);
        }
        let n = self.node(off)?;
        Ok(self.region.mem.cmp(n.key_off, n.key_len, target) == Cmp::Less)
    }

    /// Advances `cur` along `level` while the next node is before `target`; returns that
    /// next node (the first at or past `target` on this level), or `None` at the level's end.
    #[inline(always)]
    fn walk_level(
        &self,
        walk: &mut Walk,
        cur: &mut u32,
        level: usize,
        target: &[u8],
    ) -> Result<Option<Node>> {
        let mem = &self.region.mem;
        loop {
            let next = mem.load_u32(tower(*cur, level), Ordering::Acquire);
            if next == NULL {
                return Ok(None);
            }
            let n = self.node(next)?;
            if level >= n.height {
                return Err(Error::Corrupt("node linked above its height"));
            }
            walk.steps += 1;
            if walk.steps > walk.budget {
                // Entries may have been published meanwhile; re-read before deciding.
                walk.budget = self.len() + STEP_SLACK;
                if walk.steps > walk.budget {
                    return Err(Error::Corrupt("link cycle"));
                }
            }
            if mem.cmp(n.key_off, n.key_len, target) == Cmp::Less {
                *cur = next;
            } else {
                return Ok(Some(n));
            }
        }
    }

    /// The node after `node` at level 0, or `None`.
    #[inline]
    fn successor(&self, node: u32) -> Result<Option<Node>> {
        let next = self.region.mem.load_u32(tower(node, 0), Ordering::Acquire);
        if next == NULL {
            Ok(None)
        } else {
            self.node(next).map(Some)
        }
    }
}

/// A zero-copy cursor over a memtable: keys and values borrow arena memory, which the view
/// pin keeps alive. Owns a [`MemtableReader`] clone. Sees every entry published before each
/// move.
///
/// ```
/// use pigeonhole_format::{Cursor, Kind, encode_key};
/// use pigeonhole_memtable::{ArenaRegion, Memtable, ShardArena};
///
/// let mut arena = ShardArena::new(ArenaRegion::heap(1 << 20), 64 * 1024);
/// let mut memtable = Memtable::create(&mut arena)?;
/// let mut keys = Vec::new();
/// for (row, seqno) in [(b"b", 1), (b"a", 2), (b"c", 3)] {
///     let mut key = Vec::new();
///     encode_key(&mut key, row, b"q", 0, seqno, Kind::Put).unwrap();
///     memtable.insert(&mut arena, &key, row)?;
///     keys.push(key);
/// }
///
/// let mut it = memtable.reader().iter();
/// it.seek(&keys[0])?; // row "b"
/// assert_eq!(it.value(), b"b");
/// it.next()?;
/// assert_eq!(it.value(), b"c");
/// it.next()?;
/// assert!(!it.valid());
///
/// it.seek_to_first()?;
/// let big = it.value_slice(); // outlives the cursor
/// drop(it);
/// assert_eq!(&*big, b"a");
/// # Ok::<(), pigeonhole_memtable::Error>(())
/// ```
#[derive(Debug)]
pub struct MemIter {
    reader: MemtableReader,
    /// The current node, or `None` when unpositioned or past the end.
    node: Option<Node>,
    /// The last node before the last seek's target at each of the lowest levels, where a
    /// forward seek starts its finger search; `NULL` at level 0 until a seek sets them.
    preds: [u32; FINGER_LEVELS],
    /// Nodes stepped to since the last seek, and how many are plausible before the walk is
    /// declared a cycle.
    steps: usize,
    budget: usize,
    #[cfg(loom)]
    key_buf: Vec<u8>,
    #[cfg(loom)]
    value_buf: Vec<u8>,
}

impl MemIter {
    /// The current value as an [`ArenaSlice`] that outlives the cursor, so a caller can hand
    /// out a large value without copying it (it must also hold the view pin that keeps the
    /// memtable from being reclaimed).
    ///
    /// # Panics
    /// If the cursor is not valid.
    pub fn value_slice(&self) -> ArenaSlice {
        let node = self.node.expect("value_slice on an invalid cursor");
        ArenaSlice {
            region: self.reader.region.clone(),
            offset: node.value_off,
            len: node.value_len,
            _pin: Arc::clone(&self.reader.pin),
            #[cfg(loom)]
            bytes: self.value_buf.clone(),
        }
    }

    #[inline]
    fn set(&mut self, node: Option<Node>) {
        self.node = node;
        #[cfg(loom)]
        if let Some(n) = node {
            self.key_buf = self.reader.region.mem.copy(n.key_off, n.key_len);
            self.value_buf = self.reader.region.mem.copy(n.value_off, n.value_len);
        }
    }
}

/// A byte range of an arena, readable as `&[u8]`. Holds an [`ArenaRegion`] clone (keeping the
/// mapping alive); the memory stays meaningful only while a view listing its memtable is
/// pinned, which the holder guarantees.
///
/// ```
/// use pigeonhole_format::{Cursor, Kind, encode_key};
/// use pigeonhole_memtable::{ArenaRegion, ArenaSlice, Memtable, ShardArena};
///
/// let mut arena = ShardArena::new(ArenaRegion::heap(1 << 20), 64 * 1024);
/// let mut memtable = Memtable::create(&mut arena)?;
/// let mut key = Vec::new();
/// encode_key(&mut key, b"row", b"q", 0, 1, Kind::Put).unwrap();
/// let value = vec![7u8; 4096];
/// memtable.insert(&mut arena, &key, &value)?;
///
/// let slice: ArenaSlice = {
///     let mut it = memtable.reader().iter();
///     it.seek_to_first()?;
///     it.value_slice()
/// };
/// assert_eq!(slice.len(), 4096);
/// assert_eq!(&slice[..3], &[7, 7, 7]);
/// # Ok::<(), pigeonhole_memtable::Error>(())
/// ```
#[derive(Debug, Clone)]
pub struct ArenaSlice {
    region: ArenaRegion,
    offset: usize,
    len: usize,
    /// Keeps the memtable's chunks from being reused while the slice lives (held for its
    /// drop).
    _pin: Arc<Pin>,
    #[cfg(loom)]
    bytes: Vec<u8>,
}

impl std::ops::Deref for ArenaSlice {
    type Target = [u8];

    #[cfg(not(loom))]
    fn deref(&self) -> &[u8] {
        self.region.mem.slice(self.offset, self.len)
    }

    #[cfg(loom)]
    fn deref(&self) -> &[u8] {
        let _ = (&self.region, self.offset, self.len);
        &self.bytes
    }
}

impl Cursor for MemIter {
    type Error = Error;

    #[inline]
    fn valid(&self) -> bool {
        self.node.is_some()
    }

    #[cfg(not(loom))]
    #[inline]
    fn key(&self) -> &[u8] {
        let node = self.node.expect("key on an invalid cursor");
        self.reader.region.mem.slice(node.key_off, node.key_len)
    }

    #[cfg(loom)]
    fn key(&self) -> &[u8] {
        assert!(self.node.is_some(), "key on an invalid cursor");
        &self.key_buf
    }

    #[cfg(not(loom))]
    #[inline]
    fn value(&self) -> &[u8] {
        let node = self.node.expect("value on an invalid cursor");
        self.reader.region.mem.slice(node.value_off, node.value_len)
    }

    #[cfg(loom)]
    fn value(&self) -> &[u8] {
        assert!(self.node.is_some(), "value on an invalid cursor");
        &self.value_buf
    }

    fn seek_to_first(&mut self) -> Result<()> {
        let first = self.reader.successor(self.reader.head)?;
        self.steps = 0;
        self.preds[0] = NULL;
        self.set(first);
        Ok(())
    }

    fn seek(&mut self, target: &[u8]) -> Result<()> {
        let found = self.reader.search(target, &mut self.preds, false)?;
        self.steps = 0;
        self.set(found);
        Ok(())
    }

    /// A finger search from the last seek's position: cost grows with the distance to
    /// `target`. Any `target` gives the first entry `>= target`, as [`Cursor::seek`] does: one
    /// before the last seek's (the cursor exhausted, or moved past it) gets a full search.
    fn seek_forward(&mut self, target: &[u8]) -> Result<()> {
        let finger = self.preds[0] != NULL;
        let found = self.reader.search(target, &mut self.preds, finger)?;
        self.steps = 0;
        self.set(found);
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        let Some(node) = self.node else {
            return Ok(());
        };
        self.steps += 1;
        if self.steps > self.budget {
            // Entries may have been published since the last check; re-read the count.
            self.budget = self.reader.len() + STEP_SLACK;
            if self.steps > self.budget {
                return Err(Error::Corrupt("link cycle"));
            }
        }
        let next = self.reader.successor(node.off)?;
        self.set(next);
        Ok(())
    }
}
