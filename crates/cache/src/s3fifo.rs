//! The eviction core shared by [`BlockCache`](crate::BlockCache) and
//! [`RowCache`](crate::RowCache): a sharded S3-FIFO with pins and priorities.
//!
//! Each shard is a `RwLock` around three FIFOs (Yang et al., SOSP '23). A hit takes it shared
//! and bumps an atomic hit count; inserts, evictions and removals take it exclusively (#18):
//!
//! - **small** (~10% of the bytes): new entries land here. An entry not hit while in small is
//!   evicted ("one-hit wonders" never reach main) and its key goes to the ghost queue; an
//!   entry hit at least once moves to main.
//! - **main**: a CLOCK-like FIFO. An entry with a nonzero hit count is reinserted at the tail
//!   with the count decremented; an entry at zero is evicted.
//! - **ghost**: keys only. Re-inserting a ghost key goes straight to main.
//!
//! Pins: the shard owns one `Arc` per entry, and every handle is another, so an entry is pinned
//! exactly when `Arc::strong_count > 1`. Handles are only created under the shard lock
//! (`get`/`insert`) or cloned from an existing handle, so a count of 1 observed under the lock
//! cannot grow until the lock is released. Pinned entries are skipped (rotated to the tail);
//! if every entry is pinned the shard runs over capacity rather than evicting a pin. Even an
//! entry dropped from the index while pinned (replacement, invalidation) stays valid: its
//! bytes are owned by the `Arc` and freed by the last handle.
//!
//! Priorities ([`Priority`]): `High` enters main directly with two lives and can bank up to
//! seven (`Normal` three); `Normal` is plain S3-FIFO; `Low` (scans, compaction) enters small,
//! never leaves a ghost, and keeps at most one life in main. So a scan of `Low` blocks churns
//! only the small queue, and a `High` block outlives an equally hot `Normal` one.
//!
//! Memory safety (no use-after-evict) comes from `Arc` ownership, not from any locking
//! protocol: eviction only drops the shard's own reference.
//!
//! **No frees under the lock** (#1559). An entry leaving the index (evicted, replaced or
//! removed) is usually unpinned, so the shard's reference is its last, and dropping it frees
//! the value: a glibc free, or an `IoBuf` going back to another thread's pool behind a mutex.
//! Done under the shard's write lock, that wait became the lock's, and readers queued behind
//! it (up to ms at 32 readers). So a change moves what it takes out into a [`Taken`] that the
//! caller declared before taking the lock, and it's dropped after the guard is. The loom models check the
//! accounting protocol around it (a handle `get` returns is never an entry already evicted
//! or erased from the shard's books).

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

use crate::Priority;
use crate::hash::BuildKeyHasher;
use std::sync::atomic::Ordering;

use crate::sync::{Arc, AtomicU8, AtomicU64, AtomicUsize, Mutex, RwLock, lock};

/// Maximum hit count per priority: S3-FIFO's 2 bits for `Normal`, one life for `Low`, and
/// more lives for `High`, so a hot `High` block outlasts an equally hot `Normal` one in main.
const fn max_freq(priority: Priority) -> u8 {
    match priority {
        Priority::Low => 1,
        Priority::Normal => 3,
        Priority::High => 7,
    }
}

/// The small queue's share of a shard's bytes, in percent.
const SMALL_PERCENT: usize = 10;

/// Minimum number of ghost keys kept per shard.
const MIN_GHOSTS: usize = 16;

/// Most values one change takes out of a shard ([`Taken`]). An insert takes out the entry it
/// replaces and what it evicts, usually one or two. A pass that would take more stops early,
/// leaving the shard over capacity until its next change evicts again (as a shard whose
/// entries are all pinned does), and counts it ([`Shard::deferred_evictions`]): never an
/// allocation under the lock (perf287's R1 on #1559).
const TAKEN_MAX: usize = 16;

/// The values a change took out of a shard's index under its lock, to drop after the lock is
/// released (#1559): stack storage, never allocating. Declare it before the guard, so the
/// guard is dropped first.
#[must_use = "drop it after the shard's lock is released"]
pub(crate) struct Taken<V> {
    items: [Option<Arc<V>>; TAKEN_MAX],
    n: usize,
}

impl<V> Taken<V> {
    pub(crate) fn new() -> Self {
        Self {
            items: std::array::from_fn(|_| None),
            n: 0,
        }
    }

    fn is_full(&self) -> bool {
        self.n == TAKEN_MAX
    }

    /// Keeps `value` to drop later; a full buffer (which callers check first) drops it now.
    fn push(&mut self, value: Arc<V>) {
        if let Some(slot) = self.items.get_mut(self.n) {
            *slot = Some(value);
            self.n += 1;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Queue {
    Small,
    Main,
}

struct Slot<V> {
    value: Arc<V>,
    /// Distinguishes this slot from earlier slots with the same key, so queue entries left
    /// behind by removals are recognized as stale.
    id: u64,
    charge: usize,
    /// Hits since the last eviction pass, bumped under a shared lock (a hit) and changed
    /// otherwise only under the exclusive one; a lost concurrent bump only ages the entry a
    /// pass sooner.
    freq: AtomicU8,
    priority: Priority,
    queue: Queue,
}

impl<V> Slot<V> {
    fn pinned(&self) -> bool {
        Arc::strong_count(&self.value) > 1
    }
}

/// What one eviction step did.
enum Step {
    Evicted,
    Pinned,
    /// Aged, promoted or stale: progress that frees nothing.
    Other,
}

/// One shard: an index plus the three queues.
pub(crate) struct Shard<K, V> {
    map: HashMap<K, Slot<V>, BuildKeyHasher>,
    small: VecDeque<(K, u64)>,
    main: VecDeque<(K, u64)>,
    ghost: VecDeque<(K, u64)>,
    ghost_set: HashMap<K, u64, BuildKeyHasher>,
    capacity: usize,
    small_target: usize,
    usage: usize,
    small_usage: usize,
    next_id: u64,
    /// Eviction passes that stopped with a full [`Taken`] ([`TAKEN_MAX`]).
    deferred_evictions: u64,
}

impl<K: Copy + Eq + Hash, V> Shard<K, V> {
    /// A shard of `capacity` bytes, its index sized for `capacity / entry_hint` entries up
    /// front (none when `entry_hint` is 0), so filling it doesn't grow the index under the
    /// lock (#1559).
    fn new(capacity: usize, entry_hint: usize) -> Self {
        let entries = capacity.checked_div(entry_hint).unwrap_or(0);
        Self {
            map: HashMap::with_capacity_and_hasher(entries, BuildKeyHasher),
            small: VecDeque::new(),
            main: VecDeque::new(),
            ghost: VecDeque::new(),
            ghost_set: HashMap::with_hasher(BuildKeyHasher),
            capacity,
            small_target: capacity / (100 / SMALL_PERCENT),
            usage: 0,
            small_usage: 0,
            next_id: 0,
            deferred_evictions: 0,
        }
    }

    /// Looks up `key` and pins it. Allocation-free: a hash probe, a counter bump and an
    /// `Arc` increment.
    #[inline]
    pub(crate) fn get(&self, key: &K) -> Option<Arc<V>> {
        self.get_if(key, |_| true)
    }

    /// Like [`get`](Self::get), but only when `pred` accepts the value (one hash probe; a
    /// rejected entry's hit count is untouched).
    #[inline]
    pub(crate) fn get_if(&self, key: &K, pred: impl FnOnce(&V) -> bool) -> Option<Arc<V>> {
        let slot = self.map.get(key)?;
        if !pred(&slot.value) {
            return None;
        }
        // Saturated counts are left unwritten: a hot entry's slot then stays clean in other
        // cores' caches (#18).
        let freq = slot.freq.load(Ordering::Relaxed);
        if freq < max_freq(slot.priority) {
            slot.freq.store(freq + 1, Ordering::Relaxed);
        }
        Some(Arc::clone(&slot.value))
    }

    /// The entry for `key` without touching its hit count.
    pub(crate) fn peek(&self, key: &K) -> Option<&Arc<V>> {
        self.map.get(key).map(|s| &s.value)
    }

    /// Inserts `value` (the caller keeps a clone, so it is pinned while the caller holds it),
    /// replacing any entry for `key`, then evicts down to capacity, moving what it takes out
    /// into `taken`. An entry larger than the shard is not cached.
    pub(crate) fn insert(
        &mut self,
        key: K,
        value: Arc<V>,
        charge: usize,
        priority: Priority,
        taken: &mut Taken<V>,
    ) {
        self.remove(&key, taken);
        if charge > self.capacity {
            return;
        }
        let from_ghost = self.ghost_set.remove(&key).is_some();
        let (queue, freq) = match priority {
            Priority::High => (Queue::Main, 2),
            Priority::Normal if from_ghost => (Queue::Main, 0),
            _ => (Queue::Small, 0),
        };
        let id = self.next_id;
        self.next_id += 1;
        match queue {
            Queue::Small => {
                self.small.push_back((key, id));
                self.small_usage += charge;
            }
            Queue::Main => self.main.push_back((key, id)),
        }
        self.usage += charge;
        self.map.insert(
            key,
            Slot {
                value,
                id,
                charge,
                freq: AtomicU8::new(freq),
                priority,
                queue,
            },
        );
        self.evict(taken);
    }

    /// Changes the entry for `key` in place, keeping its queue and hit count: `f` gets the
    /// value and returns its new charge, or `None` to drop the entry. Then evicts down to
    /// capacity, moving what it takes out into `taken`. Returns whether the entry is still
    /// cached.
    pub(crate) fn update(
        &mut self,
        key: &K,
        f: impl FnOnce(&mut Arc<V>) -> Option<usize>,
        taken: &mut Taken<V>,
    ) -> bool {
        let Some(slot) = self.map.get_mut(key) else {
            return false;
        };
        let Some(charge) = f(&mut slot.value) else {
            self.remove(key, taken);
            return false;
        };
        let (old, small) = (slot.charge, slot.queue == Queue::Small);
        slot.charge = charge;
        self.usage = self.usage - old + charge;
        if small {
            self.small_usage = self.small_usage - old + charge;
        }
        if charge > self.capacity {
            self.remove(key, taken);
            return false;
        }
        self.evict(taken);
        self.map.contains_key(key)
    }

    /// Drops `key` from the index (pinned or not), moving its value into `taken`.
    /// Outstanding handles stay valid.
    pub(crate) fn remove(&mut self, key: &K, taken: &mut Taken<V>) -> bool {
        let Some(slot) = self.map.remove(key) else {
            return false;
        };
        self.forget(&slot);
        taken.push(slot.value);
        self.maybe_compact();
        true
    }

    /// Drops every unpinned entry whose key matches `pred`, moving their values into `out`
    /// to drop after the lock (#1559). Rare (deleted files, a closed instance), so `out` may
    /// grow under the lock; its caller reserves it first and reuses it across shards.
    pub(crate) fn remove_unpinned_where(
        &mut self,
        mut pred: impl FnMut(&K) -> bool,
        out: &mut Vec<Arc<V>>,
    ) {
        let mut freed = (0, 0);
        for (_, slot) in self.map.extract_if(|k, slot| pred(k) && !slot.pinned()) {
            freed.0 += slot.charge;
            if slot.queue == Queue::Small {
                freed.1 += slot.charge;
            }
            out.push(slot.value);
        }
        self.usage -= freed.0;
        self.small_usage -= freed.1;
        self.maybe_compact();
    }

    /// Drops every unpinned entry and every ghost, and returns the index's and the queues'
    /// spare capacity to the allocator: an engine releasing a closed instance's memory
    /// (D265). Pinned entries stay indexed, as after `remove_unpinned_where`, until their
    /// holders go and a later `clear` or eviction drops them.
    pub(crate) fn clear(&mut self, out: &mut Vec<Arc<V>>) {
        self.remove_unpinned_where(|_| true, out);
        self.ghost.clear();
        self.ghost_set.clear();
        let map = &self.map;
        self.small.retain(|(k, id)| {
            map.get(k)
                .is_some_and(|s| s.id == *id && s.queue == Queue::Small)
        });
        self.main.retain(|(k, id)| {
            map.get(k)
                .is_some_and(|s| s.id == *id && s.queue == Queue::Main)
        });
        self.map.shrink_to_fit();
        self.small.shrink_to_fit();
        self.main.shrink_to_fit();
        self.ghost.shrink_to_fit();
        self.ghost_set.shrink_to_fit();
    }

    /// Whether `key` is cached in the main queue.
    #[cfg(all(test, not(loom)))]
    pub(crate) fn in_main(&self, key: &K) -> bool {
        self.map.get(key).is_some_and(|s| s.queue == Queue::Main)
    }

    pub(crate) fn usage(&self) -> usize {
        self.usage
    }

    /// Sets the shard's capacity (and its small queue's share of it), evicting nothing:
    /// [`Sharded::set_capacity`] evicts down to it in batches, outside the lock between them
    /// (#1761, simp724's S1).
    fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
        self.small_target = capacity / (100 / SMALL_PERCENT);
    }

    /// Eviction passes that stopped early with a full [`Taken`] ([`TAKEN_MAX`]).
    pub(crate) fn deferred_evictions(&self) -> u64 {
        self.deferred_evictions
    }

    fn forget(&mut self, slot: &Slot<V>) {
        self.usage -= slot.charge;
        if slot.queue == Queue::Small {
            self.small_usage -= slot.charge;
        }
    }

    /// Whether the queue entry `(key, id)` still names a live slot in `queue`.
    fn live(&self, key: &K, id: u64, queue: Queue) -> bool {
        self.map
            .get(key)
            .is_some_and(|s| s.id == id && s.queue == queue)
    }

    /// Removals leave stale queue entries behind; drop them once they dominate.
    fn maybe_compact(&mut self) {
        let live = self.map.len();
        if self.small.len() + self.main.len() > 2 * live + 32 {
            let map = &self.map;
            self.small.retain(|(k, id)| {
                map.get(k)
                    .is_some_and(|s| s.id == *id && s.queue == Queue::Small)
            });
            self.main.retain(|(k, id)| {
                map.get(k)
                    .is_some_and(|s| s.id == *id && s.queue == Queue::Main)
            });
        }
        if self.ghost.len() > 2 * self.ghost_set.len() + 32 {
            let set = &self.ghost_set;
            self.ghost.retain(|(k, id)| set.get(k) == Some(id));
        }
    }

    fn evict(&mut self, taken: &mut Taken<V>) {
        if self.evict_pass(taken) {
            self.deferred_evictions += 1;
        }
    }

    /// Evicts down to capacity, or until `taken` is full: then it returns true, with the
    /// shard still over capacity. False when it got under capacity, or can make no further
    /// progress (every entry left pinned).
    fn evict_pass(&mut self, taken: &mut Taken<V>) -> bool {
        // A pinned entry is rotated to its queue's tail; once a whole queue has been skipped
        // without any progress, stop using it. Progress is an eviction or anything else that
        // changes what a later pass finds (aging, promotion, dropping a stale entry); each of
        // those is bounded, so this terminates. Not resetting on aging would give up on a
        // queue whose unpinned entries still hold hit counts.
        let mut skipped_small = 0;
        let mut skipped_main = 0;
        while self.usage > self.capacity {
            if taken.is_full() {
                return true;
            }
            let small_ok = !self.small.is_empty() && skipped_small < self.small.len();
            let main_ok = !self.main.is_empty() && skipped_main < self.main.len();
            let use_small = match (small_ok, main_ok) {
                (true, true) => self.small_usage > self.small_target,
                (true, false) => true,
                (false, true) => false,
                (false, false) => break,
            };
            let step = if use_small {
                self.evict_small(taken)
            } else {
                self.evict_main(taken)
            };
            match step {
                Step::Evicted => {
                    skipped_small = 0;
                    skipped_main = 0;
                }
                Step::Pinned if use_small => skipped_small += 1,
                Step::Pinned => skipped_main += 1,
                Step::Other => {
                    skipped_small = 0;
                    skipped_main = 0;
                }
            }
        }
        false
    }

    fn evict_small(&mut self, taken: &mut Taken<V>) -> Step {
        let Some((key, id)) = self.small.pop_front() else {
            return Step::Other;
        };
        if !self.live(&key, id, Queue::Small) {
            return Step::Other;
        }
        let Some(slot) = self.map.get_mut(&key) else {
            return Step::Other;
        };
        if slot.pinned() {
            self.small.push_back((key, id));
            return Step::Pinned;
        }
        let freq = slot.freq.load(Ordering::Relaxed);
        if freq > 0 {
            slot.freq.store(freq - 1, Ordering::Relaxed);
            slot.queue = Queue::Main;
            self.small_usage -= slot.charge;
            self.main.push_back((key, id));
            return Step::Other;
        }
        let Some(slot) = self.map.remove(&key) else {
            return Step::Other;
        };
        self.forget(&slot);
        let low = slot.priority == Priority::Low;
        taken.push(slot.value);
        if !low {
            self.add_ghost(key);
        }
        Step::Evicted
    }

    fn evict_main(&mut self, taken: &mut Taken<V>) -> Step {
        let Some((key, id)) = self.main.pop_front() else {
            return Step::Other;
        };
        if !self.live(&key, id, Queue::Main) {
            return Step::Other;
        }
        let Some(slot) = self.map.get_mut(&key) else {
            return Step::Other;
        };
        if slot.pinned() {
            self.main.push_back((key, id));
            return Step::Pinned;
        }
        let freq = slot.freq.load(Ordering::Relaxed);
        if freq > 0 {
            slot.freq.store(freq - 1, Ordering::Relaxed);
            self.main.push_back((key, id));
            return Step::Other;
        }
        if let Some(slot) = self.map.remove(&key) {
            self.forget(&slot);
            taken.push(slot.value);
        }
        Step::Evicted
    }

    fn add_ghost(&mut self, key: K) {
        let id = self.next_id;
        self.next_id += 1;
        self.ghost_set.insert(key, id);
        self.ghost.push_back((key, id));
        // Remember about as many evicted keys as the shard holds live entries (small and main
        // together, the paper's "as many as main" rounded up), at least `MIN_GHOSTS`.
        let cap = self.map.len().max(MIN_GHOSTS);
        while self.ghost_set.len() > cap {
            let Some((k, id)) = self.ghost.pop_front() else {
                break;
            };
            if self.ghost_set.get(&k) == Some(&id) {
                self.ghost_set.remove(&k);
            }
        }
        self.maybe_compact();
    }
}

/// Keeps each shard's lock word on its own cache line (128 bytes covers adjacent-line
/// prefetch on x86 and the line size on Apple silicon), with its lookup counters beside it
/// (first, so they share the lock word's 64-byte line: a lookup that took the lock adds to
/// a line it already holds).
#[repr(C, align(128))]
pub(crate) struct Padded<K, V> {
    /// Lookups that found their entry (the block cache counts them; [`Sharded::lookups`]).
    pub(crate) hits: AtomicU64,
    /// Lookups that did not.
    pub(crate) misses: AtomicU64,
    pub(crate) lock: RwLock<Shard<K, V>>,
}

/// Shard `i`'s part of `capacity` over `n` shards: equal, the remainder one byte each to the
/// first.
fn share(capacity: usize, n: usize, i: usize) -> usize {
    capacity / n + usize::from(i < capacity % n)
}

/// A fixed set of shards, picked by key hash.
pub(crate) struct Sharded<K, V> {
    shards: Box<[Padded<K, V>]>,
    /// The shards' capacities together: read without a lock (the block cache's pinned-bytes
    /// thresholds, metrics), changed only by [`Sharded::set_capacity`] (#1761). Stored before
    /// the shards are resized, so it leads them while a resize runs.
    capacity: AtomicUsize,
    /// Held across a whole [`Sharded::set_capacity`], so two resizes can't interleave their
    /// per-shard passes and leave the shards at a mix of both totals (perf2's S1 on #1890).
    /// Off the get and insert paths.
    resizing: Mutex<()>,
}

impl<K: Copy + Eq + Hash, V> Sharded<K, V> {
    /// `shards == 0` picks `min(64, capacity / min_shard_bytes)`, at least 1. Each shard's
    /// index is sized up front for `entry_hint`-byte entries (0: not sized).
    pub(crate) fn new(
        capacity: usize,
        shards: usize,
        min_shard_bytes: usize,
        entry_hint: usize,
    ) -> Self {
        let n = if shards == 0 {
            (capacity / min_shard_bytes).clamp(1, 64)
        } else {
            shards
        };
        let shards = (0..n)
            .map(|i| Padded {
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                lock: RwLock::new(Shard::new(share(capacity, n, i), entry_hint)),
            })
            .collect();
        Self {
            shards,
            capacity: AtomicUsize::new(capacity),
            resizing: Mutex::new(()),
        }
    }

    /// No shards: stores nothing.
    pub(crate) fn empty() -> Self {
        Self {
            shards: Box::new([]),
            capacity: AtomicUsize::new(0),
            resizing: Mutex::new(()),
        }
    }

    /// The shard for `hash`, or `None` when there are no shards.
    #[inline]
    pub(crate) fn shard(&self, hash: u64) -> Option<&RwLock<Shard<K, V>>> {
        self.padded(hash).map(|s| &s.lock)
    }

    /// [`Sharded::shard`] with its lookup counters.
    #[inline]
    pub(crate) fn padded(&self, hash: u64) -> Option<&Padded<K, V>> {
        let n = self.shards.len() as u64;
        // Lemire's multiply-shift range reduction over hash bits 16..48. The shard's HashMap
        // hashes the same key to the same value and uses the low bits for the bucket and the
        // top 7 bits as its probe tag; taking the shard from the top bits would give every
        // key in a shard the same tag prefix and defeat probe filtering.
        let mid = u64::from((hash >> 16) as u32);
        self.shards.get(((mid * n) >> 32) as usize)
    }

    /// Runs `f` on each shard under its lock. What `f` moves into the buffer is dropped
    /// after that shard's lock is released (#1559); the buffer is reserved before the first
    /// lock and reused.
    pub(crate) fn for_each(&self, mut f: impl FnMut(&mut Shard<K, V>, &mut Vec<Arc<V>>)) {
        let mut out = Vec::with_capacity(64);
        for s in self.shards.iter() {
            f(&mut lock(&s.lock), &mut out);
            out.clear();
        }
    }

    /// Eviction passes that stopped early, over every shard ([`Shard::deferred_evictions`]).
    pub(crate) fn deferred_evictions(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| lock(&s.lock).deferred_evictions())
            .sum()
    }

    pub(crate) fn usage(&self) -> usize {
        self.shards.iter().map(|s| lock(&s.lock).usage()).sum()
    }

    /// Hits and misses counted in every shard's [`Padded`] counters.
    pub(crate) fn lookups(&self) -> (u64, u64) {
        self.shards.iter().fold((0, 0), |(h, m), s| {
            (
                h + s.hits.load(Ordering::Relaxed),
                m + s.misses.load(Ordering::Relaxed),
            )
        })
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    /// Resizes to `capacity` bytes over the same shards, each its share as at
    /// [`new`](Self::new) (#1761: a node re-splitting its engine memory as partitions open and
    /// close). A shard over its new share evicts down to it in passes of at most
    /// [`TAKEN_MAX`] entries, its lock released and the taken values dropped between them, so
    /// a large shrink never holds one shard's lock for the whole eviction (simp724's S1). It
    /// returns once every shard is at or under its share, or holds only pinned entries over
    /// it: those stay until their handles drop, as at any capacity. A grow evicts nothing.
    /// No shards (a disabled cache): nothing changes. Concurrent resizes run one after the
    /// other, the last to take the lock deciding the capacity.
    pub(crate) fn set_capacity(&self, capacity: usize) {
        let n = self.shards.len();
        if n == 0 {
            return;
        }
        let _one = self
            .resizing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.capacity.store(capacity, Ordering::Relaxed);
        for (i, s) in self.shards.iter().enumerate() {
            let mut first = true;
            loop {
                // Declared before the guard, so the values drop after it (#1559).
                let mut taken = Taken::new();
                let mut shard = lock(&s.lock);
                if first {
                    shard.set_capacity(share(capacity, n, i));
                    first = false;
                }
                let more = shard.evict_pass(&mut taken);
                drop(shard);
                drop(taken);
                if !more {
                    break;
                }
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.shards.len()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    /// Loop length, shortened under Miri.
    const fn n(full: u32) -> u32 {
        if cfg!(miri) { full / 20 } else { full }
    }

    fn shard(cap: usize) -> Shard<u32, Vec<u8>> {
        Shard::new(cap, 0)
    }

    fn put(s: &mut Shard<u32, Vec<u8>>, k: u32, len: usize, p: Priority) {
        let mut taken = Taken::new();
        s.insert(k, Arc::new(vec![k as u8; len]), len, p, &mut taken);
    }

    #[test]
    fn evicts_to_capacity() {
        let mut s = shard(1000);
        for k in 0..100 {
            put(&mut s, k, 100, Priority::Normal);
            assert!(s.usage() <= 1000);
        }
        assert_eq!(s.map.len(), 10);
    }

    #[test]
    fn one_hit_wonders_do_not_displace_hot_entries() {
        let mut s = shard(1000);
        for k in 0..5 {
            put(&mut s, k, 100, Priority::Normal);
            s.get(&k);
        }
        for k in 100..100 + n(900) {
            put(&mut s, k, 100, Priority::Normal);
            for hot in 0..5 {
                s.get(&hot);
            }
        }
        for k in 0..5 {
            assert!(s.peek(&k).is_some(), "hot key {k} evicted");
        }
    }

    #[test]
    fn ghost_hit_goes_to_main() {
        let mut s = shard(1000);
        put(&mut s, 1, 100, Priority::Normal);
        for k in 10..30 {
            put(&mut s, k, 100, Priority::Normal);
        }
        assert!(s.peek(&1).is_none());
        assert!(s.ghost_set.contains_key(&1));
        put(&mut s, 1, 100, Priority::Normal);
        assert_eq!(s.map[&1].queue, Queue::Main);
    }

    #[test]
    fn low_priority_leaves_no_ghost() {
        let mut s = shard(1000);
        put(&mut s, 1, 100, Priority::Low);
        for k in 10..30 {
            put(&mut s, k, 100, Priority::Low);
        }
        assert!(s.peek(&1).is_none());
        assert!(!s.ghost_set.contains_key(&1));
    }

    #[test]
    fn all_pinned_runs_over_capacity() {
        let mut s = shard(300);
        let pins: Vec<_> = (0..5)
            .map(|k| {
                let v = Arc::new(vec![0u8; 100]);
                let mut taken = Taken::new();
                s.insert(k, Arc::clone(&v), 100, Priority::Normal, &mut taken);
                v
            })
            .collect();
        assert_eq!(s.usage(), 500);
        assert_eq!(s.map.len(), 5);
        drop(pins);
        put(&mut s, 99, 100, Priority::Normal);
        assert!(s.usage() <= 300);
    }

    /// Regression for #76: a pinned entry rotated through a queue must not exhaust the skip
    /// budget while unpinned entries behind it still need aging.
    #[test]
    fn pinned_entry_does_not_stop_aging_of_unpinned_ones() {
        let mut s = shard(300);
        put(&mut s, 1, 100, Priority::High);
        s.get(&1);
        s.get(&1);
        // Main is [1 (hits banked), 2 (pinned, just inserted)]; 1 must age out, not be kept.
        let v = Arc::new(vec![0u8; 250]);
        let mut taken = Taken::new();
        s.insert(2, Arc::clone(&v), 250, Priority::High, &mut taken);
        assert!(s.usage() <= 300, "usage {}", s.usage());
        assert!(s.peek(&1).is_none());
    }

    #[test]
    fn stale_queue_entries_are_compacted() {
        let mut s = shard(1 << 20);
        for _ in 0..n(10_000) {
            put(&mut s, 7, 10, Priority::Normal);
        }
        assert_eq!(s.map.len(), 1);
        assert!(s.small.len() + s.main.len() <= 2 + 32 + 1);
    }

    /// What leaves the index goes into the caller's `Taken`, not freed under the lock
    /// (#1559): the replaced entry and every evicted one, each its value's last reference.
    #[test]
    fn evicted_and_replaced_values_are_taken_not_dropped() {
        let mut s = shard(300);
        let weak: Vec<_> = (0..3u32)
            .map(|k| {
                let v = Arc::new(vec![k as u8; 100]);
                let w = Arc::downgrade(&v);
                let mut taken = Taken::new();
                s.insert(k, v, 100, Priority::Normal, &mut taken);
                w
            })
            .collect();
        let mut taken = Taken::new();
        // Replaces key 0 and evicts down to capacity.
        s.insert(0, Arc::new(vec![9; 250]), 250, Priority::Normal, &mut taken);
        let evicted: Vec<usize> = (1..3).filter(|&k| s.peek(&(k as u32)).is_none()).collect();
        assert!(!evicted.is_empty(), "the insert evicted something");
        // Out of the index, but alive in `taken` until it's dropped.
        assert!(
            weak[0].upgrade().is_some(),
            "the replaced value lives in `taken`"
        );
        for &k in &evicted {
            assert!(weak[k].upgrade().is_some(), "evicted {k} lives in `taken`");
        }
        drop(taken);
        assert!(weak[0].upgrade().is_none(), "freed with `taken`");
        for &k in &evicted {
            assert!(
                weak[k].upgrade().is_none(),
                "evicted {k} freed with `taken`"
            );
        }
    }

    /// One insert that would evict more than `TAKEN_MAX` entries stops at a full buffer and
    /// counts it (perf287's R1 on #1559): the shard runs over capacity until its next insert.
    #[test]
    fn a_full_taken_stops_eviction_and_counts_it() {
        let mut s = shard(1000);
        for k in 0..40u32 {
            put(&mut s, k, 25, Priority::Normal);
        }
        assert_eq!(s.deferred_evictions(), 0);
        let mut taken = Taken::new();
        s.insert(
            100,
            Arc::new(vec![0; 900]),
            900,
            Priority::Normal,
            &mut taken,
        );
        assert!(taken.is_full());
        assert_eq!(s.deferred_evictions(), 1);
        assert!(s.usage() > 1000, "over capacity until the next inserts");
        drop(taken);
        // Each later insert evicts up to a buffer's worth more, back under capacity.
        for k in 0..5u32 {
            put(&mut s, 200 + k, 10, Priority::Normal);
        }
        assert!(s.usage() <= 1000, "usage {}", s.usage());
    }

    #[test]
    fn oversized_entry_is_not_cached() {
        let mut s = shard(100);
        put(&mut s, 1, 50, Priority::Normal);
        put(&mut s, 1, 200, Priority::Normal);
        assert!(s.peek(&1).is_none());
        assert_eq!(s.usage(), 0);
    }

    #[test]
    fn default_shard_count() {
        let s: Sharded<u32, ()> = Sharded::new(1 << 30, 0, 1 << 18, 0);
        assert_eq!(s.len(), 64);
        let s: Sharded<u32, ()> = Sharded::new(1000, 0, 1 << 18, 0);
        assert_eq!(s.len(), 1);
        let s: Sharded<u32, ()> = Sharded::new(10, 3, 1 << 18, 0);
        assert_eq!(s.len(), 3);
        let mut total = 0;
        s.for_each(|sh, _| total += sh.capacity);
        assert_eq!(total, 10);
    }

    /// A `Sharded` of `shards` shards and `cap` bytes, filled with `entries` 10-byte entries
    /// (keys 0..entries), none pinned.
    fn filled(cap: usize, shards: usize, entries: u32) -> Sharded<u32, Vec<u8>> {
        let s: Sharded<u32, Vec<u8>> = Sharded::new(cap, shards, 1, 0);
        for k in 0..entries {
            let mut taken = Taken::new();
            let h = crate::hash::hash_of(&k);
            let shard = s.shard(h).unwrap();
            lock(shard).insert(k, Arc::new(vec![0; 10]), 10, Priority::Normal, &mut taken);
        }
        s
    }

    /// #1761 (simp724's S1): a shrink far past one pass's `TAKEN_MAX` evicts every shard down
    /// to its new share before it returns, in passes that are not counted as deferred.
    #[test]
    fn a_shrink_evicts_every_shard_down_to_its_share_in_passes() {
        let entries = n(2000);
        // Room for every entry however the keys hash over the shards.
        let s = filled(40 * entries as usize, 4, entries);
        assert_eq!(s.usage(), 10 * entries as usize);
        let deferred = s.deferred_evictions();
        // About a quarter of the entries per shard, down to 2 or 3: many passes each.
        s.set_capacity(100);
        assert_eq!(s.capacity(), 100);
        let mut caps = Vec::new();
        s.for_each(|sh, _| {
            assert!(
                sh.usage() <= sh.capacity,
                "{} over {}",
                sh.usage(),
                sh.capacity
            );
            caps.push(sh.capacity);
        });
        assert_eq!(caps, [25, 25, 25, 25], "each shard its share");
        assert!(s.usage() <= 100);
        assert_eq!(
            s.deferred_evictions(),
            deferred,
            "a resize's passes aren't deferrals"
        );
    }

    /// #1761 (simp724's S2): the small queue's target follows the capacity.
    #[test]
    fn a_resize_recomputes_the_small_target() {
        let s: Sharded<u32, Vec<u8>> = Sharded::new(1000, 1, 1, 0);
        s.set_capacity(5000);
        s.for_each(|sh, _| assert_eq!((sh.capacity, sh.small_target), (5000, 500)));
        s.set_capacity(100);
        s.for_each(|sh, _| assert_eq!((sh.capacity, sh.small_target), (100, 10)));
    }

    /// #1761 (simp724's S2): an entry larger than the new capacity is evicted with the rest,
    /// and one inserted afterwards isn't cached, as for any oversized entry.
    #[test]
    fn a_shrink_below_an_entry_evicts_it_and_a_later_one_isnt_cached() {
        let s: Sharded<u32, Vec<u8>> = Sharded::new(1000, 1, 1, 0);
        let shard = s.shard(0).unwrap();
        let mut taken = Taken::new();
        lock(shard).insert(1, Arc::new(vec![0; 500]), 500, Priority::Normal, &mut taken);
        drop(taken);
        s.set_capacity(100);
        assert!(
            lock(shard).peek(&1).is_none(),
            "the 500-byte entry is evicted"
        );
        assert_eq!(s.usage(), 0);
        let mut taken = Taken::new();
        lock(shard).insert(2, Arc::new(vec![0; 500]), 500, Priority::Normal, &mut taken);
        assert!(lock(shard).peek(&2).is_none(), "nor is a new one cached");
    }

    /// #1761: a shrink can't evict what's pinned; those entries stay over the new share until
    /// their handles drop, and the resize returns rather than spinning on them.
    #[test]
    fn a_shrink_stops_at_pinned_entries() {
        let s: Sharded<u32, Vec<u8>> = Sharded::new(1000, 1, 1, 0);
        let shard = s.shard(0).unwrap();
        let mut held = Vec::new();
        for k in 0..10u32 {
            let v = Arc::new(vec![0u8; 50]);
            held.push(Arc::clone(&v));
            let mut taken = Taken::new();
            lock(shard).insert(k, v, 50, Priority::Normal, &mut taken);
        }
        for k in 10..20u32 {
            let mut taken = Taken::new();
            lock(shard).insert(k, Arc::new(vec![0u8; 50]), 50, Priority::Normal, &mut taken);
        }
        assert_eq!(s.usage(), 1000);
        s.set_capacity(100);
        assert_eq!(s.usage(), 500, "only the ten pinned entries are left");
        drop(held);
        // The next change evicts down to the new capacity.
        let mut taken = Taken::new();
        lock(shard).insert(
            99,
            Arc::new(vec![0u8; 10]),
            10,
            Priority::Normal,
            &mut taken,
        );
        drop(taken);
        assert!(s.usage() <= 100, "usage {}", s.usage());
    }

    /// #1761: a grow evicts nothing, and the cache fills to the new capacity.
    #[test]
    fn a_grow_keeps_everything_and_fills_further() {
        let s = filled(500, 1, 100);
        assert_eq!(s.usage(), 500);
        s.set_capacity(2000);
        assert_eq!(s.usage(), 500, "nothing evicted");
        let shard = s.shard(0).unwrap();
        for k in 100..300u32 {
            let mut taken = Taken::new();
            lock(shard).insert(k, Arc::new(vec![0; 10]), 10, Priority::Normal, &mut taken);
        }
        assert_eq!(s.usage(), 2000);
    }

    /// perf2's S1 on #1890: two resizes at once end at one of the two totals, every shard at
    /// its share of it, never a mix.
    #[test]
    fn concurrent_resizes_end_at_one_total() {
        for _ in 0..n(200) {
            let s = Arc::new(filled(4000, 4, 100));
            let (a, b) = (Arc::clone(&s), Arc::clone(&s));
            let one = std::thread::spawn(move || a.set_capacity(400));
            let two = std::thread::spawn(move || b.set_capacity(2000));
            one.join().unwrap();
            two.join().unwrap();
            let total = s.capacity();
            assert!(total == 400 || total == 2000, "{total}");
            let mut sum = 0;
            s.for_each(|sh, _| {
                assert_eq!(
                    sh.capacity,
                    total / 4,
                    "every shard at its share of {total}"
                );
                sum += sh.capacity;
            });
            assert_eq!(sum, total);
        }
    }

    /// A disabled (shardless) cache stays disabled.
    #[test]
    fn a_resize_of_no_shards_changes_nothing() {
        let s: Sharded<u32, Vec<u8>> = Sharded::empty();
        s.set_capacity(1 << 20);
        assert_eq!(s.capacity(), 0);
    }
}
