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
//! protocol: eviction only drops the shard's own reference. The loom models check the
//! accounting protocol around it (a handle `get` returns is never an entry already evicted
//! or erased from the shard's books).

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

use crate::Priority;
use crate::hash::BuildKeyHasher;
use std::sync::atomic::Ordering;

use crate::sync::{Arc, AtomicU8, AtomicU64, RwLock, lock};

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
}

impl<K: Copy + Eq + Hash, V> Shard<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::with_hasher(BuildKeyHasher),
            small: VecDeque::new(),
            main: VecDeque::new(),
            ghost: VecDeque::new(),
            ghost_set: HashMap::with_hasher(BuildKeyHasher),
            capacity,
            small_target: capacity / (100 / SMALL_PERCENT),
            usage: 0,
            small_usage: 0,
            next_id: 0,
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
    /// replacing any entry for `key`, then evicts down to capacity. An entry larger than the
    /// shard is not cached.
    pub(crate) fn insert(&mut self, key: K, value: Arc<V>, charge: usize, priority: Priority) {
        self.remove(&key);
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
        self.evict();
    }

    /// Drops `key` from the index (pinned or not). Outstanding handles stay valid.
    pub(crate) fn remove(&mut self, key: &K) -> bool {
        let Some(slot) = self.map.remove(key) else {
            return false;
        };
        self.forget(&slot);
        self.maybe_compact();
        true
    }

    /// Drops every unpinned entry whose key matches `pred`.
    pub(crate) fn remove_unpinned_where(&mut self, mut pred: impl FnMut(&K) -> bool) {
        let mut freed = (0, 0);
        self.map.retain(|k, slot| {
            if !pred(k) || slot.pinned() {
                return true;
            }
            freed.0 += slot.charge;
            if slot.queue == Queue::Small {
                freed.1 += slot.charge;
            }
            false
        });
        self.usage -= freed.0;
        self.small_usage -= freed.1;
        self.maybe_compact();
    }

    /// Whether `key` is cached in the main queue.
    #[cfg(all(test, not(loom)))]
    pub(crate) fn in_main(&self, key: &K) -> bool {
        self.map.get(key).is_some_and(|s| s.queue == Queue::Main)
    }

    pub(crate) fn usage(&self) -> usize {
        self.usage
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

    fn evict(&mut self) {
        // A pinned entry is rotated to its queue's tail; once a whole queue has been skipped
        // without any progress, stop using it. Progress is an eviction or anything else that
        // changes what a later pass finds (aging, promotion, dropping a stale entry); each of
        // those is bounded, so this terminates. Not resetting on aging would give up on a
        // queue whose unpinned entries still hold hit counts.
        let mut skipped_small = 0;
        let mut skipped_main = 0;
        while self.usage > self.capacity {
            let small_ok = !self.small.is_empty() && skipped_small < self.small.len();
            let main_ok = !self.main.is_empty() && skipped_main < self.main.len();
            let use_small = match (small_ok, main_ok) {
                (true, true) => self.small_usage > self.small_target,
                (true, false) => true,
                (false, true) => false,
                (false, false) => break,
            };
            let step = if use_small {
                self.evict_small()
            } else {
                self.evict_main()
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
    }

    fn evict_small(&mut self) -> Step {
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
        if slot.priority != Priority::Low {
            self.add_ghost(key);
        }
        Step::Evicted
    }

    fn evict_main(&mut self) -> Step {
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

/// A fixed set of shards, picked by key hash.
pub(crate) struct Sharded<K, V> {
    shards: Box<[Padded<K, V>]>,
    capacity: usize,
}

impl<K: Copy + Eq + Hash, V> Sharded<K, V> {
    /// `shards == 0` picks `min(64, capacity / min_shard_bytes)`, at least 1.
    pub(crate) fn new(capacity: usize, shards: usize, min_shard_bytes: usize) -> Self {
        let n = if shards == 0 {
            (capacity / min_shard_bytes).clamp(1, 64)
        } else {
            shards
        };
        let shards = (0..n)
            .map(|i| Padded {
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                lock: RwLock::new(Shard::new(capacity / n + usize::from(i < capacity % n))),
            })
            .collect();
        Self { shards, capacity }
    }

    /// No shards: stores nothing.
    pub(crate) fn empty() -> Self {
        Self {
            shards: Box::new([]),
            capacity: 0,
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

    pub(crate) fn for_each(&self, mut f: impl FnMut(&mut Shard<K, V>)) {
        for s in self.shards.iter() {
            f(&mut lock(&s.lock));
        }
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
        self.capacity
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
        Shard::new(cap)
    }

    fn put(s: &mut Shard<u32, Vec<u8>>, k: u32, len: usize, p: Priority) {
        s.insert(k, Arc::new(vec![k as u8; len]), len, p);
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
                s.insert(k, Arc::clone(&v), 100, Priority::Normal);
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
        s.insert(2, Arc::clone(&v), 250, Priority::High);
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
        let s: Sharded<u32, ()> = Sharded::new(1 << 30, 0, 1 << 18);
        assert_eq!(s.len(), 64);
        let s: Sharded<u32, ()> = Sharded::new(1000, 0, 1 << 18);
        assert_eq!(s.len(), 1);
        let s: Sharded<u32, ()> = Sharded::new(10, 3, 1 << 18);
        assert_eq!(s.len(), 3);
        let mut total = 0;
        s.for_each(|sh| total += sh.capacity);
        assert_eq!(total, 10);
    }
}
