//! Where a commit's commit-time blob files stand until its outcome decides whether they
//! stay (#230; #134 step 3: loom-modelled).
//!
//! `large::separate` writes a batch's long values into new blob files and commits them
//! before the batch is submitted. From then on each file is:
//!
//! - **pending** until its commit settles, or until its release (`DropBlobFile`) commits:
//!   published, but nothing may count on what points into it yet;
//! - **logged** once the shard handed a record pointing into it to the WAL (the commit
//!   point: from there the batch may be applied, or replayed);
//! - **dropped** if the same-commit collapse (D34) dropped its value at apply.
//!
//! When the commit resolves, [`LargeSettlement::settle`] decides, on whichever thread
//! resolves it:
//! - a dropped file is released;
//! - after success a file stays (no longer pending);
//! - after failure a logged file stays pending (the next open sweeps it or finds the
//!   replayed pointers), and one never logged is released.
//!
//! Lock order: `settle` holds logged → dropped → pending together; everyone else takes one
//! set at a time.

#[cfg(engine_loom)]
use loom::sync::{Mutex, MutexGuard};
#[cfg(not(engine_loom))]
use std::sync::{Mutex, MutexGuard};

use std::collections::HashSet;
use std::sync::PoisonError;

use pigeonhole_format::BlobFileId;

/// The three sets (see the module documentation).
#[derive(Debug)]
pub(crate) struct LargeSettlement {
    pending: Mutex<HashSet<BlobFileId>>,
    logged: Mutex<HashSet<BlobFileId>>,
    dropped: Mutex<HashSet<BlobFileId>>,
}

impl Default for LargeSettlement {
    fn default() -> Self {
        Self {
            pending: Mutex::new(HashSet::new()),
            logged: Mutex::new(HashSet::new()),
            dropped: Mutex::new(HashSet::new()),
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl LargeSettlement {
    /// New files join before the commit that publishes them.
    pub(crate) fn add_pending(&self, ids: impl IntoIterator<Item = BlobFileId>) {
        lock(&self.pending).extend(ids);
    }

    /// Files whose publishing commit failed, or whose release committed.
    pub(crate) fn remove_pending(&self, ids: impl IntoIterator<Item = BlobFileId>) {
        let mut pending = lock(&self.pending);
        for id in ids {
            pending.remove(&id);
        }
    }

    /// The files pending now (a test check).
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn pending(&self) -> HashSet<BlobFileId> {
        lock(&self.pending).clone()
    }

    /// The shard hands a record pointing into `ids` to the WAL: those still pending are
    /// logged. Takes `pending`, then, after releasing it, `logged`.
    pub(crate) fn note_logged(&self, mut ids: HashSet<BlobFileId>) {
        let pending = lock(&self.pending);
        ids.retain(|id| pending.contains(id));
        drop(pending);
        lock(&self.logged).extend(ids);
    }

    /// The collapse dropped a value in `id` at apply.
    pub(crate) fn note_dropped(&self, id: BlobFileId) {
        lock(&self.dropped).insert(id);
    }

    /// The commit holding `files` resolved: leaves in `files` those to release.
    pub(crate) fn settle(&self, files: &mut Vec<BlobFileId>, succeeded: bool) {
        let mut logged = lock(&self.logged);
        let mut dropped = lock(&self.dropped);
        let mut pending = lock(&self.pending);
        files.retain(|id| {
            let was_logged = logged.remove(id);
            if dropped.remove(id) {
                return true;
            }
            if succeeded {
                // Applied: the batch points into it.
                pending.remove(id);
            } else if !was_logged {
                // Never logged: nothing points into it, now or after a replay.
                return true;
            }
            // Logged but failed (applied, half applied or not): kept, and left pending, so
            // nothing counts on what points into it until the next open sweeps it or finds
            // the replayed pointers.
            false
        });
    }
}

#[cfg(all(test, engine_loom))]
mod loom_tests {
    use super::LargeSettlement;
    use loom::sync::Arc;
    use loom::thread;
    use pigeonhole_format::BlobFileId;
    use std::collections::HashSet;

    fn ids(v: &[u32]) -> HashSet<BlobFileId> {
        v.iter().map(|&i| BlobFileId(i)).collect()
    }

    #[test]
    fn loom_each_commits_files_are_kept_or_released_by_where_it_stopped() {
        // Three commits settle at once on different threads, each with its own file:
        // - commit 1 was logged by its shard, then failed: its file stays, still pending;
        // - commit 2 failed before its commit point: its file is released, and the release's
        //   reply then takes it out of pending, racing commit 1's shard noting its own;
        // - commit 3 succeeded with its value collapsed at apply: its file is released;
        // - commit 4 was logged and succeeded: its file stays, no longer pending.
        // In every interleaving the decisions hold and the lock order never deadlocks.
        let mut b = loom::model::Builder::new();
        // Every interleaving with up to three preemptions (unbounded: about a minute).
        b.preemption_bound = Some(3);
        b.check(|| {
            let s = Arc::new(LargeSettlement::default());
            s.add_pending([BlobFileId(1), BlobFileId(2), BlobFileId(3), BlobFileId(4)]);
            let one = {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    // The shard logs the batch, then the commit resolves (failed).
                    s.note_logged(ids(&[1]));
                    let mut files = vec![BlobFileId(1)];
                    s.settle(&mut files, false);
                    files
                })
            };
            let two = {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    let mut files = vec![BlobFileId(2)];
                    s.settle(&mut files, false);
                    // `release`: its `DropBlobFile` committed.
                    s.remove_pending(files.iter().copied());
                    files
                })
            };
            // Commit 3: the shard's apply drops the value, then the commit resolves (ok).
            s.note_dropped(BlobFileId(3));
            let mut three = vec![BlobFileId(3)];
            s.settle(&mut three, true);
            s.note_logged(ids(&[4]));
            let mut four = vec![BlobFileId(4)];
            s.settle(&mut four, true);
            assert_eq!(one.join().unwrap(), vec![], "a logged file was released");
            assert_eq!(two.join().unwrap(), vec![BlobFileId(2)]);
            assert_eq!(three, vec![BlobFileId(3)], "a collapsed file was kept");
            assert_eq!(four, vec![], "an applied file was released");
            let mut pending: Vec<_> = s.pending().into_iter().collect();
            pending.sort_by_key(|id| id.0);
            assert_eq!(pending, vec![BlobFileId(1), BlobFileId(3)]);
        });
    }
}

#[cfg(all(test, not(engine_loom)))]
mod tests {
    use super::LargeSettlement;
    use pigeonhole_format::BlobFileId;

    #[test]
    fn the_decision_table() {
        let s = LargeSettlement::default();
        let f = |i| BlobFileId(i);
        s.add_pending([f(1), f(2), f(3), f(4)]);
        s.note_logged([f(1), f(9)].into_iter().collect());
        s.note_dropped(f(4));
        let mut failed = vec![f(1), f(2)];
        s.settle(&mut failed, false);
        assert_eq!(failed, vec![f(2)], "logged stays, never logged goes");
        let mut ok = vec![f(3), f(4)];
        s.settle(&mut ok, true);
        assert_eq!(ok, vec![f(4)], "applied stays, collapsed goes");
        let pending = s.pending();
        assert!(pending.contains(&f(1)) && pending.contains(&f(2)) && pending.contains(&f(4)));
        assert!(!pending.contains(&f(3)));
    }
}
