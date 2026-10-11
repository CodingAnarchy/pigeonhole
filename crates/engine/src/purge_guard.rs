//! A purge guard (D191, D192): settles a purge in flight against the writes that would
//! invalidate it (#134 step 3: loom-modelled).
//!
//! A flush may drop versions beyond `max_versions` only if no delete in its family arrived
//! while it was in flight (#287); a bottommost compaction may drop entries below its bound
//! only if no write below that bound arrived (#316). The purge's commit and the shard
//! admitting such a write race on one three-state cell:
//!
//! - **In flight:** neither has happened yet.
//! - **Voided:** the shard admitted a write that invalidates the purge first
//!   ([`PurgeGuard::void`]). The commit's catalog closure finds it voided and refuses
//!   ([`PurgeGuard::claim`]), and the shard flushes or compacts again without the purge.
//! - **Installing:** the commit claimed the purge first. A write that would invalidate it
//!   waits until the shard learns the commit's outcome, so it becomes visible only after
//!   the install.
//!
//! Each side moves the cell out of in-flight with one compare-and-swap, so exactly one wins,
//! and each accepts its own state again: a write that touches the guard twice, or a closure
//! the writer runs again, finds its own move.

#[cfg(engine_loom)]
use loom::sync::atomic::{AtomicU8, Ordering};
#[cfg(not(engine_loom))]
use std::sync::atomic::{AtomicU8, Ordering};

const IN_FLIGHT: u8 = 0;
const VOIDED: u8 = 1;
const INSTALLING: u8 = 2;

/// The guard's state (see the module documentation).
#[derive(Debug)]
pub(crate) struct PurgeGuard {
    state: AtomicU8,
}

impl PurgeGuard {
    /// A guard in flight.
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU8::new(IN_FLIGHT),
        }
    }

    /// The shard admitting a write that invalidates the purge: voids it unless its commit
    /// has claimed it. Returns whether the write may be admitted now (`false`: it waits for
    /// the commit's outcome).
    #[inline]
    pub(crate) fn void(&self) -> bool {
        match self
            .state
            .compare_exchange(IN_FLIGHT, VOIDED, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) | Err(VOIDED) => true,
            Err(_) => false,
        }
    }

    /// The purge's commit (its catalog closure, under the manifest writer): claims the
    /// purge unless a write voided it. Returns whether it may install (`false`: refuse).
    #[inline]
    pub(crate) fn claim(&self) -> bool {
        match self.state.compare_exchange(
            IN_FLIGHT,
            INSTALLING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(INSTALLING) => true,
            Err(_) => false,
        }
    }

    /// Whether a write voided the purge (its commit is or will be refused).
    #[inline]
    pub(crate) fn voided(&self) -> bool {
        self.state.load(Ordering::Acquire) == VOIDED
    }
}

#[cfg(all(test, engine_loom))]
mod loom_tests {
    use super::PurgeGuard;
    use loom::sync::{Arc, Mutex};
    use loom::thread;

    fn model(f: impl Fn() + Sync + Send + 'static) {
        let mut b = loom::model::Builder::new();
        b.preemption_bound = Some(3);
        b.check(f);
    }

    /// What became visible, in order: the writes the shard admitted and the purge, if it
    /// installed.
    #[derive(Debug, Default)]
    struct History {
        admitted: Vec<u32>,
        installed: bool,
        /// Writes admitted before the install became visible.
        before_install: usize,
    }

    #[test]
    fn loom_a_purge_and_a_write_that_invalidates_it_never_both_take_effect() {
        // The shard admits two writes touching the guard (one of them touching it twice,
        // as a member with two deletes in the family does) while the purge's commit runs its
        // closure, run twice as the writer may. In every interleaving exactly one side wins:
        // a write admitted while the purge was in flight refuses the install, and an
        // install makes every later write wait for its outcome, so no admitted write is
        // ordered before an installed purge.
        model(|| {
            let guard = Arc::new(PurgeGuard::new());
            let history = Arc::new(Mutex::new(History::default()));
            // The commit's outcome, which the shard waits for (`on_flushed`).
            let outcome = Arc::new(Mutex::new(None::<bool>));

            let commit = {
                let (guard, history, outcome) = (
                    Arc::clone(&guard),
                    Arc::clone(&history),
                    Arc::clone(&outcome),
                );
                thread::spawn(move || {
                    let claimed = guard.claim();
                    // A re-run of the closure finds its own claim.
                    assert_eq!(guard.claim(), claimed, "a re-run changed the decision");
                    if claimed {
                        let mut h = history.lock().unwrap();
                        h.installed = true;
                        h.before_install = h.admitted.len();
                    }
                    *outcome.lock().unwrap() = Some(claimed);
                })
            };

            let mut waited = Vec::new();
            for (write, touches) in [(1u32, 2), (2, 1)] {
                let mut admit = true;
                for _ in 0..touches {
                    admit &= guard.void();
                }
                if admit {
                    history.lock().unwrap().admitted.push(write);
                } else {
                    waited.push(write);
                }
            }
            commit.join().unwrap();
            let installed = outcome.lock().unwrap().expect("the commit decided");
            // The shard learns the outcome and admits the writes that waited.
            for write in waited {
                assert!(installed, "write {write} waited, yet nothing installed");
                history.lock().unwrap().admitted.push(write);
            }
            // What the shard reads to decide whether to flush again without the purge.
            assert_eq!(guard.voided(), !installed);

            let h = history.lock().unwrap();
            assert_eq!(h.admitted.len(), 2, "every write admitted once");
            if h.installed {
                assert_eq!(
                    h.before_install, 0,
                    "a write invalidating the purge landed first"
                );
            }
        });
    }

    #[test]
    fn loom_one_write_against_a_flush_and_a_compaction_guard() {
        // A write touching both a flush's guard and a running compaction's guard
        // (`admit_against_guards` voids each in turn, waiting if one is installing) while
        // both commits run: the write is admitted only if it voided both, and each purge
        // installs only if it was not voided, so nothing the write invalidates installs
        // before it is visible.
        model(|| {
            let guards = [Arc::new(PurgeGuard::new()), Arc::new(PurgeGuard::new())];
            let installs: Vec<_> = guards
                .iter()
                .map(|g| {
                    let g = Arc::clone(g);
                    thread::spawn(move || g.claim())
                })
                .collect();
            let admitted = guards.iter().all(|g| g.void());
            let installed: Vec<bool> = installs.into_iter().map(|t| t.join().unwrap()).collect();
            if admitted {
                assert!(
                    installed.iter().all(|i| !i),
                    "an install beat an admitted write"
                );
            } else {
                assert!(installed.iter().any(|i| *i), "the write waited for nothing");
            }
            for (g, i) in guards.iter().zip(&installed) {
                assert_eq!(g.voided(), !i);
            }
        });
    }
}

#[cfg(all(test, not(engine_loom)))]
mod tests {
    use super::PurgeGuard;

    #[test]
    fn the_first_move_wins_and_each_side_accepts_its_own_again() {
        let g = PurgeGuard::new();
        assert!(!g.voided());
        assert!(g.void(), "a write voids a purge in flight");
        assert!(g.void(), "and again");
        assert!(!g.claim(), "the commit is refused");
        assert!(g.voided());

        let g = PurgeGuard::new();
        assert!(g.claim(), "the commit claims a purge in flight");
        assert!(g.claim(), "a re-run finds its own claim");
        assert!(!g.void(), "a write waits for the outcome");
        assert!(!g.voided());
    }
}
