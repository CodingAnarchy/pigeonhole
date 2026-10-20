//! Who waits for the visible watermark (#134 step 2, item 2): one registry for async
//! commits waiting for their seqno to become visible and shards with a freeze deferred
//! until the watermark passes their memtable.
//!
//! An entry is a seqno, a key that names its waiter, and a wake. A shard publishing a
//! watermark wakes, and forgets, the entries whose seqno is visible, outside the lock.
//!
//! The registry is split into parts (one per shard), each with its own lock, so waiters on
//! different shards' threads rarely meet (gate analysis, 2026-10-11: one list at 32 shards
//! was found held 60K times in 0.33 s). A key always lands in the same part. In each part:
//! - a map from key to its listed seqno, generation and wake: registering again (a spurious
//!   poll, a new waker, a raised seqno) replaces the wake in place, with no scan of the
//!   list;
//! - a heap of `(seqno, key, generation)`, smallest seqno first: a wake pass pops only the
//!   entries that are ready. An entry whose generation is no longer the key's (its seqno
//!   was changed) is dropped when it surfaces;
//! - `min_needed`, the heap's smallest seqno (`u64::MAX` when empty). A publish below it
//!   takes no lock.
//!
//! Across the parts, `listed` has one bit per part, set while its heap is non-empty. A bit
//! changes only under its part's lock, on the heap's empty/non-empty transitions, so a
//! publish with nobody registered is one fence and one load (the first 64 parts' word is
//! inline; more parts add a load per 64), and a publish visits only the parts that list
//! someone (#564's per-part scan cost every commit).
//!
//! Registration pairs with publication through SeqCst fences: a registrant stores its
//! part's `min_needed` (and sets its bit if the part was empty), fences, then re-reads the
//! watermark; a publisher stores the watermark, fences, then loads `listed` and each listed
//! part's `min_needed`. Either the publisher sees the registration or the registrant sees
//! the watermark, so no wake is lost. A registrant joining a part that already lists
//! someone sets no bit: the bit was set by an earlier registration under the same lock, so
//! it happens before this registrant's fence, and a bit is cleared only while its part is
//! empty, never while this registrant's entry is listed.
//!
//! Generic over the wake `W`, so the loom models below run it alone.

#[cfg(engine_loom)]
use loom::sync::atomic::{AtomicU64, Ordering, fence};
#[cfg(engine_loom)]
use loom::sync::{Mutex, MutexGuard};
#[cfg(not(engine_loom))]
use std::sync::atomic::{AtomicU64, Ordering, fence};
#[cfg(not(engine_loom))]
use std::sync::{Mutex, MutexGuard};

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::{PoisonError, TryLockError};

use pigeonhole_format::Seqno;
use pigeonhole_format::hash::FastMap;
use smallvec::SmallVec;

/// The registry (see the module documentation).
#[derive(Debug)]
pub(crate) struct WatermarkWaiters<W> {
    parts: Box<[Part<W>]>,
    /// Bit `i` of `listed` (parts 0-63), or bit `i % 64` of `more[i / 64 - 1]`: part `i`'s
    /// heap is non-empty. Changed only under part `i`'s lock. The first word is inline, so
    /// a publish with nobody registered loads it and returns.
    listed: Listed,
    more: Box<[Listed]>,
}

/// A word of [`WatermarkWaiters::listed`], on its own cache line (each bit flips on a
/// part's empty/non-empty transition, from any thread).
#[derive(Debug)]
#[repr(align(64))]
struct Listed(AtomicU64);

/// One part of the registry, on its own cache line.
#[derive(Debug)]
#[repr(align(64))]
struct Part<W> {
    /// The heap's smallest seqno (`u64::MAX` when empty); an entry left behind by a changed
    /// registration can hold it lower until it surfaces. Stored under `list`.
    min_needed: AtomicU64,
    list: Mutex<List<W>>,
    /// Registrations and wake passes that found `list` held (ICR 0023).
    contended: AtomicU64,
}

#[derive(Debug)]
struct List<W> {
    /// Key -> (listed seqno, generation, wake).
    keys: FastMap<u64, (Seqno, u64, W)>,
    /// `(seqno, key, generation)`, smallest seqno first.
    heap: BinaryHeap<Reverse<(Seqno, u64, u64)>>,
    next_generation: u64,
}

impl<W> Default for List<W> {
    fn default() -> Self {
        Self {
            keys: FastMap::default(),
            heap: BinaryHeap::new(),
            next_generation: 0,
        }
    }
}

impl<W> List<W> {
    fn min(&self) -> u64 {
        self.heap.peek().map_or(u64::MAX, |Reverse((s, _, _))| *s)
    }
}

impl<W> Default for WatermarkWaiters<W> {
    /// One part (the registry's tests and loom models).
    fn default() -> Self {
        Self::new(1)
    }
}

impl<W> WatermarkWaiters<W> {
    /// A registry of `parts` parts (at least one): the engine makes one per shard.
    pub(crate) fn new(parts: usize) -> Self {
        let parts = parts.max(1);
        Self {
            listed: Listed(AtomicU64::new(0)),
            more: (1..parts.div_ceil(64))
                .map(|_| Listed(AtomicU64::new(0)))
                .collect(),
            parts: (0..parts)
                .map(|_| Part {
                    min_needed: AtomicU64::new(u64::MAX),
                    list: Mutex::new(List::default()),
                    contended: AtomicU64::new(0),
                })
                .collect(),
        }
    }

    fn part_index(&self, key: u64) -> usize {
        // A power-of-two count would make this a mask; shard counts are not always one.
        (key % self.parts.len() as u64) as usize
    }

    /// Word `w` of the `listed` bits.
    fn word(&self, w: usize) -> &AtomicU64 {
        match w {
            0 => &self.listed.0,
            _ => &self.more[w - 1].0,
        }
    }

    /// Marks part `i` as listing someone or not. Call it under part `i`'s lock, on a
    /// transition only.
    fn mark(&self, i: usize, listed: bool) {
        let word = self.word(i / 64);
        let bit = 1u64 << (i % 64);
        if listed {
            word.fetch_or(bit, Ordering::SeqCst);
        } else {
            word.fetch_and(!bit, Ordering::SeqCst);
        }
    }

    /// Registrations and wake passes that found a part's lock held.
    pub(crate) fn contended(&self) -> u64 {
        self.parts
            .iter()
            .map(|p| p.contended.load(Ordering::Relaxed))
            .sum()
    }

    /// Registers `key`'s wake until `seqno` is visible. A key already listed takes the new
    /// wake, and `seqno` if it differs, so a waiter registered again is never listed twice;
    /// each waiter needs a key of its own ([`commit_key`], [`shard_key`]). Returns the
    /// entries listed in the key's part before it. The caller then re-reads the watermark: a
    /// publish that missed this registration is seen there.
    pub(crate) fn register(&self, key: u64, seqno: Seqno, wake: W) -> usize {
        self.register_in(self.part_index(key), key, seqno, wake)
    }

    /// [`register`](Self::register) in part `i` (chosen by the caller: a shard's part for a
    /// commit waiting on that shard's component, #717 (b2)).
    pub(crate) fn register_in(&self, i: usize, key: u64, seqno: Seqno, wake: W) -> usize {
        let part = &self.parts[i];
        let mut list = part.lock();
        let listed = list.keys.len();
        let was_empty = list.heap.is_empty();
        let generation = match list.keys.get_mut(&key) {
            Some(entry) if entry.0 == seqno => {
                entry.2 = wake;
                None
            }
            _ => {
                let generation = list.next_generation;
                list.next_generation += 1;
                list.keys.insert(key, (seqno, generation, wake));
                Some(generation)
            }
        };
        if let Some(generation) = generation {
            list.heap.push(Reverse((seqno, key, generation)));
        }
        // Stored even when unchanged: the fence below pairs this store with the publisher's
        // load.
        part.min_needed.store(list.min(), Ordering::SeqCst);
        if was_empty && !list.heap.is_empty() {
            self.mark(i, true);
        }
        drop(list);
        // Pairs with the fence in `wake_ready`: either the publisher sees this registration
        // or the caller's re-read sees its watermark.
        fence(Ordering::SeqCst);
        listed
    }

    /// After a watermark publish: hands `wake` every entry whose seqno is at or below what
    /// `visible` reads, forgotten, and runs it outside the locks. Returns whether the pass
    /// took a lock. With nobody registered it is one fence and one load of `listed` (plus
    /// one per further 64 parts), and it visits only the parts that list someone. The engine
    /// fences once for both registries and calls `wake_ready_fenced`; this stays for tests.
    #[cfg(test)]
    pub(crate) fn wake_ready(&self, visible: impl Fn() -> Seqno, wake: impl FnMut(W)) -> bool {
        // Pairs with the fence in `register`.
        fence(Ordering::SeqCst);
        self.wake_ready_fenced(visible, wake)
    }

    /// `wake_ready` for a caller that has already issued the SeqCst fence after its
    /// watermark publish: one fence then serves every registry it wakes.
    #[inline]
    pub(crate) fn wake_ready_fenced(
        &self,
        visible: impl Fn() -> Seqno,
        mut wake: impl FnMut(W),
    ) -> bool {
        let first = self.listed.0.load(Ordering::SeqCst);
        if first == 0 && self.more.is_empty() {
            return false;
        }
        self.wake_listed(first, &visible, &mut wake)
    }

    /// [`wake_ready_fenced`](Self::wake_ready_fenced) for one part, against `visible`:
    /// the publisher of one shard's component wakes only that shard's part.
    #[inline]
    pub(crate) fn wake_part_fenced(
        &self,
        i: usize,
        visible: Seqno,
        mut wake: impl FnMut(W),
    ) -> bool {
        let (word, bit) = (i / 64, i % 64);
        let listed = if word == 0 {
            self.listed.0.load(Ordering::SeqCst)
        } else {
            self.more[word - 1].0.load(Ordering::SeqCst)
        };
        if listed & (1u64 << bit) == 0 {
            return false;
        }
        let mut now = Some(visible);
        self.wake_part(i, &mut now, &|| visible, &mut wake)
    }

    /// `wake_ready` past its check: visits every part the `listed` bits mark, starting
    /// from the first word's bits as `wake_ready` loaded them. Kept out of line, so a
    /// publish with nobody registered stays a fence and a load.
    #[inline(never)]
    fn wake_listed(
        &self,
        first: u64,
        visible: &impl Fn() -> Seqno,
        wake: &mut impl FnMut(W),
    ) -> bool {
        let mut visible_now = None;
        let mut passed = false;
        let (mut bits, mut w) = (first, 0);
        loop {
            while bits != 0 {
                let i = w * 64 + bits.trailing_zeros() as usize;
                bits &= bits - 1;
                passed |= self.wake_part(i, &mut visible_now, visible, wake);
            }
            if w == self.more.len() {
                return passed;
            }
            bits = self.more[w].0.load(Ordering::SeqCst);
            w += 1;
        }
    }

    /// `wake_ready`'s pass over part `i`, which `listed` says lists someone. Returns
    /// whether it took the part's lock.
    fn wake_part(
        &self,
        i: usize,
        visible_now: &mut Option<Seqno>,
        visible: &impl Fn() -> Seqno,
        wake: &mut impl FnMut(W),
    ) -> bool {
        let part = &self.parts[i];
        let min = part.min_needed.load(Ordering::SeqCst);
        if min == u64::MAX {
            return false;
        }
        let visible = *visible_now.get_or_insert_with(visible);
        if visible < min {
            return false;
        }
        let mut ready: SmallVec<[W; 8]> = SmallVec::new();
        {
            let mut list = part.lock();
            let was_empty = list.heap.is_empty();
            while let Some(&Reverse((seqno, key, generation))) = list.heap.peek() {
                if seqno > visible {
                    break;
                }
                list.heap.pop();
                if list.keys.get(&key).is_some_and(|e| e.1 == generation) {
                    let (_, _, w) = list.keys.remove(&key).expect("checked");
                    ready.push(w);
                }
            }
            part.min_needed.store(list.min(), Ordering::SeqCst);
            if !was_empty && list.heap.is_empty() {
                self.mark(i, false);
            }
        }
        for w in ready {
            wake(w);
        }
        true
    }

    /// Hands `wake` every entry, whatever its seqno, forgotten (visibility ended: each
    /// re-checks and ends), outside the locks.
    pub(crate) fn wake_all(&self, mut wake: impl FnMut(W)) {
        for (i, part) in self.parts.iter().enumerate() {
            let all = {
                let mut list = part.list.lock().unwrap_or_else(PoisonError::into_inner);
                part.min_needed.store(u64::MAX, Ordering::SeqCst);
                if !list.heap.is_empty() {
                    list.heap.clear();
                    self.mark(i, false);
                }
                std::mem::take(&mut list.keys)
            };
            all.into_values().map(|(_, _, w)| w).for_each(&mut wake);
        }
    }

    /// The smallest registered seqno as published, and as the lists say (test check).
    /// Under each part's lock, also checks that its `listed` bit says whether its heap is
    /// non-empty: the bit changes only under that lock.
    #[cfg(all(test, not(engine_loom)))]
    fn published_and_actual_min(&self) -> (Seqno, Seqno) {
        let mut published = u64::MAX;
        let mut actual = u64::MAX;
        for (i, part) in self.parts.iter().enumerate() {
            let list = part.list.lock().unwrap_or_else(PoisonError::into_inner);
            let bit = self.word(i / 64).load(Ordering::SeqCst) >> (i % 64) & 1 == 1;
            assert_eq!(bit, !list.heap.is_empty(), "part {i}'s listed bit");
            published = published.min(part.min_needed.load(Ordering::SeqCst));
            actual = actual.min(list.keys.values().map(|e| e.0).min().unwrap_or(u64::MAX));
        }
        (published, actual)
    }
}

impl<W> Part<W> {
    /// The list, locked; a lock already held counts as contention.
    fn lock(&self) -> MutexGuard<'_, List<W>> {
        match self.list.try_lock() {
            Ok(g) => g,
            Err(TryLockError::Poisoned(e)) => e.into_inner(),
            Err(TryLockError::WouldBlock) => {
                self.contended.fetch_add(1, Ordering::Relaxed);
                self.list.lock().unwrap_or_else(PoisonError::into_inner)
            }
        }
    }
}

/// The key of a commit waiting for its seqno to become visible: the seqno itself. A commit
/// has one waiter (its future, or the one blocking wait that consumes it), so no two
/// waiters share it. Seqnos stay below 2^63, so it never meets a [`shard_key`].
pub(crate) fn commit_key(seqno: Seqno) -> u64 {
    debug_assert!(seqno < 1 << 63);
    seqno
}

/// The key of a shard waiting for the watermark (a deferred freeze): one per shard, with
/// the top bit set so it never meets a [`commit_key`].
pub(crate) fn shard_key(shard: u16) -> u64 {
    (1 << 63) | u64::from(shard)
}

#[cfg(all(test, engine_loom))]
mod loom_tests {
    use super::WatermarkWaiters;
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use loom::thread;

    /// A watermark and the registry, as `Shared` holds them: the shm watermark is an
    /// atomic published with Release (`publish_pending`).
    struct Shared {
        visible: AtomicU64,
        waiters: WatermarkWaiters<usize>,
        woken: [AtomicUsize; 4],
    }

    impl Shared {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                visible: AtomicU64::new(0),
                // Two parts: keys 0 and 2 register in part 0, keys 1 and 3 in part 1.
                waiters: WatermarkWaiters::new(2),
                woken: std::array::from_fn(|_| AtomicUsize::new(0)),
            })
        }

        /// `publish_watermark`: publish, then wake the ready. The watermark never goes back
        /// (it is the smallest of the shards' pending seqnos).
        fn publish(&self, v: u64) {
            self.visible.fetch_max(v, Ordering::Release);
            self.waiters.wake_ready(
                || self.visible.load(Ordering::Acquire),
                |w| {
                    self.woken[w].fetch_add(1, Ordering::Relaxed);
                },
            );
        }

        /// `wait_visible`: register, then look again. True if the waiter saw it visible.
        fn wait(&self, id: usize, seqno: u64) -> bool {
            self.waiters.register(id as u64, seqno, id);
            self.visible.load(Ordering::Acquire) >= seqno
        }
    }

    fn model(f: impl Fn() + Sync + Send + 'static) {
        let mut b = loom::model::Builder::new();
        b.preemption_bound = Some(3);
        b.check(f);
    }

    #[test]
    fn loom_a_waiter_registering_as_the_watermark_passes_is_never_lost() {
        // A commit waiting for seqno 1 and a shard waiting for seqno 2 register while two
        // shards publish 1 and 2: each waiter sees its seqno visible on its second look or
        // is woken, and is woken at most once.
        model(|| {
            let s = Shared::new();
            let a = {
                let s = Arc::clone(&s);
                thread::spawn(move || s.publish(1))
            };
            let b = {
                let s = Arc::clone(&s);
                thread::spawn(move || s.publish(2))
            };
            let saw = [s.wait(0, 1), s.wait(1, 2)];
            a.join().unwrap();
            b.join().unwrap();
            for (id, saw) in saw.into_iter().enumerate() {
                let woken = s.woken[id].load(Ordering::Relaxed);
                assert!(woken <= 1, "waiter {id} woken {woken} times");
                assert!(saw || woken == 1, "waiter {id} lost its wake");
            }
        });
    }

    #[test]
    fn loom_a_waiter_joining_a_listed_part_is_never_lost() {
        // Two waiters register into part 1 while a shard publishes 2: whichever comes
        // second finds the part listed and sets no `listed` bit, relying on the first one's
        // bit (set under the same lock, so before its own fence). Waiter 3 needs 2: it sees
        // it visible or is woken. Waiter 1 needs 5 and is never woken.
        model(|| {
            let s = Shared::new();
            let first = {
                let s = Arc::clone(&s);
                thread::spawn(move || s.wait(1, 5))
            };
            let publisher = {
                let s = Arc::clone(&s);
                thread::spawn(move || s.publish(2))
            };
            let saw = s.wait(3, 2);
            assert!(!first.join().unwrap());
            publisher.join().unwrap();
            let woken = s.woken[3].load(Ordering::Relaxed);
            assert!(woken <= 1, "waiter 3 woken {woken} times");
            assert!(saw || woken == 1, "waiter 3 lost its wake");
            assert_eq!(
                s.woken[1].load(Ordering::Relaxed),
                0,
                "waiter 1 woken early"
            );
        });
    }

    #[test]
    fn loom_a_part_drained_while_a_waiter_joins_it_still_wakes_the_waiter() {
        // Waiter 1 needs 1; a shard publishes 1 (draining part 1 to empty, which clears its
        // `listed` bit), then 2. Waiter 3 joins part 1 meanwhile, needing 2: whichever way
        // the drain's clear and its registration interleave, the publish of 2 finds it.
        model(|| {
            let s = Shared::new();
            assert!(!s.wait(1, 1));
            let publisher = {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    s.publish(1);
                    s.publish(2);
                })
            };
            let saw = s.wait(3, 2);
            publisher.join().unwrap();
            let woken = s.woken[3].load(Ordering::Relaxed);
            assert!(saw || woken == 1, "waiter 3 lost its wake");
            assert_eq!(s.woken[1].load(Ordering::Relaxed), 1);
        });
    }

    #[test]
    fn loom_a_waiter_registered_again_with_a_higher_seqno_still_wakes() {
        // A shard that defers again raises its seqno while a publish takes a pass: it is
        // woken once its new seqno is visible, however the pass and the raise interleave.
        model(|| {
            let s = Shared::new();
            assert!(!s.wait(0, 1));
            let p = {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    s.publish(1);
                    s.publish(2);
                })
            };
            let saw = s.wait(0, 2);
            p.join().unwrap();
            let woken = s.woken[0].load(Ordering::Relaxed);
            assert!(saw || woken >= 1, "the raised registration lost its wake");
        });
    }
}

#[cfg(all(test, not(engine_loom)))]
mod tests {
    use super::WatermarkWaiters;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn take(w: &WatermarkWaiters<u16>, visible: u64) -> Vec<u16> {
        let mut out = Vec::new();
        w.wake_ready(|| visible, |s| out.push(s));
        out.sort_unstable();
        quiescent(w);
        out
    }

    fn register(w: &WatermarkWaiters<u16>, shard: u16, seqno: u64) {
        w.register(u64::from(shard), seqno, shard);
        quiescent(w);
    }

    /// At rest, every part's `listed` bit says whether its heap is non-empty (checked in
    /// `published_and_actual_min`), and the published minimum is the lists' own.
    fn quiescent(w: &WatermarkWaiters<u16>) {
        let (published, actual) = w.published_and_actual_min();
        assert!(published <= actual, "published {published} above {actual}");
    }

    #[test]
    fn a_waiter_wakes_only_once_its_seqno_is_visible() {
        let w = WatermarkWaiters::default();
        register(&w, 3, 10);
        register(&w, 1, 5);
        // A watermark that did not reach either seqno wakes nobody.
        assert!(take(&w, 4).is_empty());
        assert_eq!(take(&w, 7), [1]);
        assert!(take(&w, 9).is_empty());
        // Registering again replaces the seqno.
        register(&w, 3, 12);
        assert!(take(&w, 10).is_empty());
        assert_eq!(take(&w, 12), [3]);
        assert!(take(&w, u64::MAX - 1).is_empty());
        // A waiter that registers again after its wake is woken again.
        register(&w, 3, 20);
        assert_eq!(take(&w, 20), [3]);
    }

    #[test]
    fn a_pass_with_nobody_ready_takes_no_lock() {
        let w = WatermarkWaiters::default();
        assert!(!w.wake_ready(|| 100, |_: u16| unreachable!()));
        register(&w, 1, 50);
        assert!(!w.wake_ready(|| 49, |_| unreachable!()));
        assert!(w.wake_ready(|| 50, |_| {}));
        assert!(!w.wake_ready(|| 100, |_| unreachable!()), "forgotten");
    }

    #[test]
    fn a_duplicate_registration_is_listed_once_and_every_entry_wakes_all_at_once() {
        // A key registered again is listed once, with its latest wake and seqno.
        let w = WatermarkWaiters::default();
        assert_eq!(w.register(7, 5, 1), 0);
        assert_eq!(w.register(7, 5, 2), 1, "listed once, wake replaced");
        assert_eq!(w.register(7, 6, 3), 1, "seqno raised");
        assert!(take(&w, 5).is_empty(), "the raised entry waits for 6");
        assert_eq!(take(&w, 6), [3]);
        assert!(take(&w, 6).is_empty(), "woken once");
        // Two keys with the same seqno are two waiters.
        w.register(8, 9, 8);
        w.register(9, 9, 9);
        assert_eq!(take(&w, 9), [8, 9]);
        // More ready than the inline buffer holds.
        for id in 0..20 {
            register(&w, 100 + id, 3);
        }
        register(&w, 7, 4);
        let mut all = Vec::new();
        w.wake_all(|id| all.push(id));
        quiescent(&w);
        assert_eq!(all.len(), 21);
        assert!(take(&w, u64::MAX - 1).is_empty());
    }

    #[test]
    fn every_part_wakes_its_ready_waiters_and_only_those() {
        let w = WatermarkWaiters::new(4);
        for key in 0..16u16 {
            // Part key % 4; seqnos 10, 11, ... so each part has early and late waiters.
            w.register(u64::from(key), 10 + u64::from(key), key);
        }
        assert!(!w.wake_ready(|| 9, |_| unreachable!()));
        assert_eq!(take(&w, 13), [0, 1, 2, 3]);
        assert_eq!(take(&w, 20), [4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(take(&w, 100), [11, 12, 13, 14, 15]);
        assert!(!w.wake_ready(|| 100, |_| unreachable!()), "all forgotten");
        assert_ne!(super::commit_key(5) >> 63, super::shard_key(0) >> 63);
    }

    #[test]
    fn parts_past_the_first_listed_word_wake_too() {
        // 130 parts: three `listed` words, the last one partly used.
        let w = WatermarkWaiters::new(130);
        for key in [0u16, 63, 64, 127, 128, 129] {
            register(&w, key, 10 + u64::from(key));
        }
        assert!(!w.wake_ready(|| 9, |_| unreachable!()));
        assert_eq!(take(&w, 73), [0, 63]);
        assert_eq!(take(&w, 137), [64, 127]);
        assert_eq!(take(&w, 139), [128, 129]);
        assert!(
            !w.wake_ready(|| u64::MAX - 1, |_| unreachable!()),
            "all forgotten"
        );
        register(&w, 129, 200);
        let mut all = Vec::new();
        w.wake_all(|id| all.push(id));
        quiescent(&w);
        assert_eq!(all, [129]);
    }

    #[test]
    fn a_waiter_registering_during_a_pass_is_never_hidden() {
        // Registrants race a waker while a checker looks: under the lock the published
        // minimum is never above the list's smallest seqno, so a registration racing a pass
        // is never hidden from later publishers.
        const WAITERS: u16 = 4;
        let w = WatermarkWaiters::default();
        let stop = AtomicUsize::new(0);
        let failure = Mutex::new(None);
        std::thread::scope(|s| {
            for id in 0..WAITERS {
                let (w, stop) = (&w, &stop);
                s.spawn(move || {
                    let mut n = 0u64;
                    while stop.load(Ordering::Acquire) == 0 {
                        n += 1;
                        register(w, id, n);
                    }
                });
            }
            s.spawn(|| {
                let mut v = 0u64;
                while stop.load(Ordering::Acquire) == 0 {
                    v += 1;
                    w.wake_ready(|| v, |_| {});
                }
            });
            for _ in 0..200_000 {
                let (published, actual) = w.published_and_actual_min();
                if published > actual {
                    *failure.lock().unwrap() = Some((published, actual));
                    break;
                }
            }
            stop.store(1, Ordering::Release);
        });
        assert_eq!(
            *failure.lock().unwrap(),
            None,
            "(published, actual) minimum"
        );
    }
}
