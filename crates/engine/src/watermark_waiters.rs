//! Who waits for the visible watermark (#134 step 2, item 2): one registry for async
//! commits waiting for their seqno to become visible and shards with a freeze deferred
//! until the watermark passes their memtable.
//!
//! An entry is a seqno and a wake. A shard publishing a watermark wakes, and forgets, the
//! entries whose seqno is visible, outside the lock. `min_needed` holds the smallest
//! registered seqno, so a publish with nobody registered costs one load, and one below
//! every registered seqno takes no lock.
//!
//! Registration pairs with publication through SeqCst fences: a registrant stores
//! `min_needed`, fences, then re-reads the watermark; a publisher stores the watermark,
//! fences, then loads `min_needed`. Either the publisher sees the registration or the
//! registrant sees the watermark, so no wake is lost.
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

use std::sync::{PoisonError, TryLockError};

use pigeonhole_format::Seqno;
use smallvec::SmallVec;

/// The registry (see the module documentation).
#[derive(Debug)]
pub(crate) struct WatermarkWaiters<W> {
    /// The smallest seqno registered (`u64::MAX` when none), or lower: an entry whose seqno
    /// was raised leaves it low until a wake pass recomputes it. Stored under `list`.
    min_needed: AtomicU64,
    list: Mutex<Vec<(Seqno, W)>>,
    /// Registrations and wake passes that found the lock held (ICR 0023).
    contended: AtomicU64,
}

impl<W> Default for WatermarkWaiters<W> {
    fn default() -> Self {
        Self {
            min_needed: AtomicU64::new(u64::MAX),
            list: Mutex::new(Vec::new()),
            contended: AtomicU64::new(0),
        }
    }
}

impl<W> WatermarkWaiters<W> {
    /// The list, locked; a lock already held counts as contention.
    fn lock(&self) -> MutexGuard<'_, Vec<(Seqno, W)>> {
        match self.list.try_lock() {
            Ok(g) => g,
            Err(TryLockError::Poisoned(e)) => e.into_inner(),
            Err(TryLockError::WouldBlock) => {
                self.contended.fetch_add(1, Ordering::Relaxed);
                self.list.lock().unwrap_or_else(PoisonError::into_inner)
            }
        }
    }

    /// Registrations and wake passes that found the lock held.
    pub(crate) fn contended(&self) -> u64 {
        self.contended.load(Ordering::Relaxed)
    }

    /// Registers a wake (built by `wake` only when needed) until `seqno` is visible. An
    /// entry `same` matches takes `seqno` instead, so a waiter registered again is not
    /// listed twice. Returns the entries scanned. The caller then re-reads the watermark: a
    /// publish that missed this registration is seen there.
    pub(crate) fn register(
        &self,
        seqno: Seqno,
        wake: impl FnOnce() -> W,
        same: impl Fn(Seqno, &W) -> bool,
    ) -> usize {
        let mut list = self.lock();
        let scanned = list.len();
        match list.iter_mut().find(|(s, w)| same(*s, w)) {
            Some(entry) => entry.0 = seqno,
            None => list.push((seqno, wake())),
        }
        // Stored even when unchanged: the fence below pairs this store with the publisher's
        // load.
        let min = self.min_needed.load(Ordering::Relaxed).min(seqno);
        self.min_needed.store(min, Ordering::SeqCst);
        drop(list);
        // Pairs with the fence in `wake_ready`: either the publisher sees this registration
        // or the caller's re-read sees its watermark.
        fence(Ordering::SeqCst);
        scanned
    }

    /// After a watermark publish: hands `wake` every entry whose seqno is at or below what
    /// `visible` reads, forgotten, and runs it outside the lock. Returns whether the pass
    /// took the lock. With nobody registered it is one fence and one load.
    pub(crate) fn wake_ready(&self, visible: impl Fn() -> Seqno, mut wake: impl FnMut(W)) -> bool {
        // Pairs with the fence in `register`.
        fence(Ordering::SeqCst);
        let min = self.min_needed.load(Ordering::SeqCst);
        if min == u64::MAX {
            return false;
        }
        let visible = visible();
        if visible < min {
            return false;
        }
        let mut ready: SmallVec<[W; 8]> = SmallVec::new();
        {
            let mut list = self.lock();
            let mut min = u64::MAX;
            let mut i = 0;
            while i < list.len() {
                if list[i].0 <= visible {
                    ready.push(list.swap_remove(i).1);
                } else {
                    min = min.min(list[i].0);
                    i += 1;
                }
            }
            self.min_needed.store(min, Ordering::SeqCst);
        }
        for w in ready {
            wake(w);
        }
        true
    }

    /// Hands `wake` every entry, whatever its seqno, forgotten (visibility ended: each
    /// re-checks and ends), outside the lock.
    pub(crate) fn wake_all(&self, wake: impl FnMut(W)) {
        let all = {
            let mut list = self.list.lock().unwrap_or_else(PoisonError::into_inner);
            self.min_needed.store(u64::MAX, Ordering::SeqCst);
            std::mem::take(&mut *list)
        };
        all.into_iter().map(|(_, w)| w).for_each(wake);
    }

    /// The smallest registered seqno as published, and as the list says (test check).
    #[cfg(all(test, not(engine_loom)))]
    fn published_and_actual_min(&self) -> (Seqno, Seqno) {
        let list = self.list.lock().unwrap_or_else(PoisonError::into_inner);
        let min = list.iter().map(|(s, _)| *s).min().unwrap_or(u64::MAX);
        (self.min_needed.load(Ordering::SeqCst), min)
    }
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
        woken: [AtomicUsize; 2],
    }

    impl Shared {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                visible: AtomicU64::new(0),
                waiters: WatermarkWaiters::default(),
                woken: [AtomicUsize::new(0), AtomicUsize::new(0)],
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
            self.waiters.register(seqno, || id, |_, w| *w == id);
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
        out
    }

    fn register(w: &WatermarkWaiters<u16>, shard: u16, seqno: u64) {
        w.register(seqno, || shard, |_, s| *s == shard);
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
        // Commits: an entry is the same only with the same seqno and waker.
        let w = WatermarkWaiters::default();
        let same = |seqno| move |s: u64, id: &u16| s == seqno && *id == 7;
        assert_eq!(w.register(5, || 7, same(5)), 0);
        assert_eq!(w.register(5, || 7, same(5)), 1, "scanned the one entry");
        assert_eq!(w.register(6, || 7, same(6)), 1);
        // More ready than the inline buffer holds.
        for id in 0..20 {
            register(&w, 100 + id, 3);
        }
        let mut all = Vec::new();
        w.wake_all(|id| all.push(id));
        assert_eq!(all.len(), 22);
        assert!(take(&w, u64::MAX - 1).is_empty());
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
