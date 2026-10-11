//! Flushed memtables against the published view (#131; #134 step 3: loom-modelled).
//!
//! A shard reads its own memtables beside the published view's SSTs. Once a memtable's SST
//! is in the manifest, the view has the SST and the memtable's root joins this set, which
//! the shard reads to leave that memtable out until it retires it. The two must change
//! together: a reader that saw the new view with the old set would count the memtable's
//! entries twice (a merge operand, #131), and one that saw the old view with the new set
//! would miss them.
//!
//! So the publisher adds the roots and stores the view under this set's lock
//! ([`FlushedRoots::publish`]), and a reader that pairs the two loads the view under it
//! too ([`FlushedRoots::read`]). The owning shard retires roots once its memtables are
//! gone ([`FlushedRoots::retire`]).

#[cfg(engine_loom)]
use loom::sync::Mutex;
#[cfg(not(engine_loom))]
use std::sync::Mutex;

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::PoisonError;

/// The set (see the module documentation).
#[derive(Debug)]
pub(crate) struct FlushedRoots<K> {
    roots: Mutex<HashSet<K>>,
}

impl<K> Default for FlushedRoots<K> {
    fn default() -> Self {
        Self {
            roots: Mutex::new(HashSet::new()),
        }
    }
}

impl<K: Eq + Hash + Copy> FlushedRoots<K> {
    /// Adds `roots` and runs `store`, which stores the view with their SSTs, under the
    /// lock: no reader sees one without the other.
    pub(crate) fn publish(&self, roots: &[K], store: impl FnOnce()) {
        let mut set = self.roots.lock().unwrap_or_else(PoisonError::into_inner);
        set.extend(roots.iter().copied());
        store();
    }

    /// Runs `f` with the set held. A caller that pairs the set with the view loads the
    /// view inside `f`.
    pub(crate) fn read<R>(&self, f: impl FnOnce(&HashSet<K>) -> R) -> R {
        f(&self.roots.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// The owning shard dropped these memtables: their roots leave the set.
    pub(crate) fn retire(&self, roots: impl IntoIterator<Item = K>) {
        let mut set = self.roots.lock().unwrap_or_else(PoisonError::into_inner);
        for root in roots {
            set.remove(&root);
        }
    }
}

#[cfg(all(test, engine_loom))]
mod loom_tests {
    use super::FlushedRoots;
    use loom::sync::{Arc, Mutex};
    use loom::thread;

    /// The published view as far as one memtable goes: whether it has that memtable's SST.
    /// A `Mutex` stands in for the engine's `ArcSwap` (both are linearizable stores and
    /// loads of one value).
    struct Shared {
        roots: FlushedRoots<u32>,
        view_has_sst: Mutex<bool>,
    }

    /// Where the shard finds memtable 7's entries: in the memtable (unless its root is
    /// flushed) and in the view's SSTs. `read_latest`'s pairing.
    fn copies(s: &Shared) -> u32 {
        s.roots.read(|flushed| {
            let view_has_sst = *s.view_has_sst.lock().unwrap();
            u32::from(!flushed.contains(&7)) + u32::from(view_has_sst)
        })
    }

    #[test]
    fn loom_a_reader_sees_a_flushed_memtable_exactly_once() {
        // The manifest commit publishes memtable 7's SST while the shard reads twice, then
        // learns of the flush and retires the root (its memtable is gone). Every read finds
        // the entries exactly once: never in both the memtable and the SST, never in neither.
        loom::model(|| {
            let s = Arc::new(Shared {
                roots: FlushedRoots::default(),
                view_has_sst: Mutex::new(false),
            });
            let publisher = {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    s.roots
                        .publish(&[7], || *s.view_has_sst.lock().unwrap() = true);
                })
            };
            assert_eq!(copies(&s), 1);
            assert_eq!(copies(&s), 1);
            publisher.join().unwrap();
            // `on_flushed`: the memtable is dropped and its root retired; the view keeps the SST.
            s.roots.retire([7]);
            assert!(*s.view_has_sst.lock().unwrap());
            assert_eq!(s.roots.read(|flushed| flushed.len()), 0);
        });
    }

    #[test]
    fn loom_two_flushes_published_at_once() {
        // Two flushes' commits (memtables 7 and 8) publish while the shard reads: each
        // memtable's entries are found exactly once in every read.
        loom::model(|| {
            let roots = Arc::new(FlushedRoots::default());
            let ssts = Arc::new(Mutex::new([false, false]));
            let publish = |root: u32, i: usize| {
                let (roots, ssts) = (Arc::clone(&roots), Arc::clone(&ssts));
                thread::spawn(move || {
                    roots.publish(&[root], || ssts.lock().unwrap()[i] = true);
                })
            };
            let a = publish(7, 0);
            let b = publish(8, 1);
            let read = || {
                roots.read(|flushed| {
                    let ssts = *ssts.lock().unwrap();
                    [7u32, 8].map(|r| {
                        u32::from(!flushed.contains(&r)) + u32::from(ssts[(r - 7) as usize])
                    })
                })
            };
            assert_eq!(read(), [1, 1]);
            a.join().unwrap();
            assert_eq!(read(), [1, 1]);
            b.join().unwrap();
            assert_eq!(read(), [1, 1]);
        });
    }
}

#[cfg(all(test, not(engine_loom)))]
mod tests {
    use super::FlushedRoots;

    #[test]
    fn roots_join_with_the_store_and_leave_when_retired() {
        let roots = FlushedRoots::default();
        let mut stored = false;
        roots.publish(&[(0u16, 7u32), (1, 9)], || stored = true);
        assert!(stored);
        assert!(roots.read(|f| f.contains(&(0, 7)) && f.contains(&(1, 9))));
        roots.retire([(0, 7)]);
        assert!(roots.read(|f| !f.contains(&(0, 7)) && f.contains(&(1, 9))));
    }
}
