//! Who runs the final close, and when (#134 step 2): one lock over the close's state.
//!
//! Every shard reports once its part of the close is done (its WAL checkpointed, synced and
//! dropped, or the shard died). Once all have, the final close is pending. It runs once
//! nobody is inside maintenance (`shrink`, `backup`, a catalog change: `MaintenanceGuard`)
//! and the manifest writer's exclusion is free, on whichever thread finds both: the last
//! reporter, the last thread to leave maintenance, or the thread releasing the exclusion
//! (`manifest::release`). The caller of `Engine::close` registers a waiter, answered when
//! the final close finishes, or at once if it already has.
//!
//! The decision is made under one lock, so it needs no fence pairings. The exclusion is a
//! try-lock taken under it: a releaser stores the exclusion free before it takes this lock,
//! so whichever of the releaser and the reporter (or the leaving maintainer) takes the lock
//! second sees the other's step and runs the close. The final close itself runs outside the
//! lock, under the exclusion.
//!
//! Generic over the outcome `T` and the waiter `W`, so the loom models below run it alone.

#[cfg(engine_loom)]
use loom::sync::{Mutex, MutexGuard};
#[cfg(not(engine_loom))]
use std::sync::{Mutex, MutexGuard};

use std::sync::PoisonError;

/// Where the close is.
#[derive(Debug)]
enum Stage<T> {
    /// Shards still to report (`Gate::remaining > 0`), or no close requested yet.
    Open,
    /// Every shard reported; the final close waits for maintenance and the exclusion.
    Pending,
    /// A thread is running the final close.
    Running,
    /// The final close finished with this outcome.
    Done(T),
}

#[derive(Debug)]
struct Gate<T, W> {
    /// Shards that have not reported.
    remaining: usize,
    /// A shard's final sync, flush or checkpoint failed, or a shard died: the close is not
    /// clean.
    failed: bool,
    /// Application threads inside maintenance.
    maintenance: usize,
    stage: Stage<T>,
    /// The `Engine::close` caller's waiter, until the outcome is known.
    waiter: Option<W>,
}

/// The close's state behind one lock (see the module documentation).
#[derive(Debug)]
pub(crate) struct CloseGate<T, W> {
    state: Mutex<Gate<T, W>>,
}

impl<T, W> CloseGate<T, W> {
    /// A gate waiting for `shards` reports (0 for a reader, which never closes this way).
    pub(crate) fn new(shards: usize) -> Self {
        Self {
            state: Mutex::new(Gate {
                remaining: shards,
                failed: false,
                maintenance: 0,
                stage: Stage::Open,
                waiter: None,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Gate<T, W>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Marks the close unclean; returns whether this was the first time.
    pub(crate) fn fail(&self) -> bool {
        let mut g = self.lock();
        !std::mem::replace(&mut g.failed, true)
    }

    /// Whether the close is unclean.
    pub(crate) fn failed(&self) -> bool {
        self.lock().failed
    }

    /// A shard reports its part of the close done. A second report of the same shard (a
    /// dropped driver after a panic, say) finds nothing left to count: no-op. Returns
    /// whether the final close became pending: the caller then tries to run it.
    pub(crate) fn report(&self) -> bool {
        let mut g = self.lock();
        if g.remaining == 0 {
            return false;
        }
        g.remaining -= 1;
        if g.remaining > 0 {
            return false;
        }
        g.stage = Stage::Pending;
        true
    }

    /// An application thread enters maintenance; the final close waits for it.
    pub(crate) fn enter_maintenance(&self) {
        self.lock().maintenance += 1;
    }

    /// It leaves; the caller then tries to run the final close.
    pub(crate) fn leave_maintenance(&self) {
        let mut g = self.lock();
        g.maintenance -= 1;
    }

    /// Starts the final close if it is pending, nobody is inside maintenance and `claim`
    /// takes the manifest writer's exclusion (it must not block). Returns whether the
    /// close is unclean when it did: the caller runs the final close, then calls
    /// [`CloseGate::finish`]. `None`: not now (someone else will, or already did).
    pub(crate) fn try_start(&self, claim: impl FnOnce() -> bool) -> Option<bool> {
        let mut g = self.lock();
        if !matches!(g.stage, Stage::Pending) || g.maintenance > 0 || !claim() {
            return None;
        }
        g.stage = Stage::Running;
        Some(g.failed)
    }

    /// Records the final close's outcome; returns the waiter to answer, if one registered.
    pub(crate) fn finish(&self, outcome: T) -> Option<W> {
        let mut g = self.lock();
        debug_assert!(matches!(g.stage, Stage::Running), "finish without a start");
        g.stage = Stage::Done(outcome);
        g.waiter.take()
    }

    /// Registers the `Engine::close` caller's waiter. If the final close already finished,
    /// returns it with a copy of the outcome (`dup`) to answer at once.
    pub(crate) fn register(&self, waiter: W, dup: impl FnOnce(&T) -> T) -> Option<(W, T)> {
        let mut g = self.lock();
        if let Stage::Done(outcome) = &g.stage {
            let outcome = dup(outcome);
            return Some((waiter, outcome));
        }
        g.waiter = Some(waiter);
        None
    }

    /// A copy of the outcome (`dup`) once the final close finished.
    pub(crate) fn outcome(&self, dup: impl FnOnce(&T) -> T) -> Option<T> {
        match &self.lock().stage {
            Stage::Done(outcome) => Some(dup(outcome)),
            _ => None,
        }
    }

    /// Whether the final close is pending (test hook).
    pub(crate) fn pending(&self) -> bool {
        matches!(self.lock().stage, Stage::Pending)
    }
}

#[cfg(all(test, engine_loom))]
mod loom_tests {
    use super::CloseGate;
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use loom::sync::{Arc, Mutex};
    use loom::thread;

    /// The engine's side of the protocol, as `Shared::try_final_close` runs it: start, the
    /// final close (counted), finish, release the exclusion and look again. The manifest
    /// writer's exclusion (`manifest_busy`, an atomic try-lock) is a `Mutex<bool>` here:
    /// the model needs it linearizable, and loom 0.7 lets a swap read a value older than
    /// the latest store, which no real atomic does.
    struct Engine {
        gate: CloseGate<u32, u32>,
        busy: Mutex<bool>,
        runs: AtomicUsize,
        answered: AtomicUsize,
    }

    /// Every interleaving with up to three preemptions (the unbounded four-thread model
    /// takes half an hour; the bugs it is for need two).
    fn model(f: impl Fn() + Sync + Send + 'static) {
        let mut b = loom::model::Builder::new();
        b.preemption_bound = Some(3);
        b.check(f);
    }

    impl Engine {
        fn new(shards: usize, busy: bool) -> Arc<Self> {
            Arc::new(Self {
                gate: CloseGate::new(shards),
                busy: Mutex::new(busy),
                runs: AtomicUsize::new(0),
                answered: AtomicUsize::new(0),
            })
        }

        fn claim(&self) -> bool {
            let mut b = self.busy.lock().unwrap();
            !std::mem::replace(&mut *b, true)
        }

        fn release(&self) {
            *self.busy.lock().unwrap() = false;
            self.try_final_close();
        }

        fn try_final_close(&self) {
            if self.gate.try_start(|| self.claim()).is_none() {
                return;
            }
            self.runs.fetch_add(1, Ordering::Relaxed);
            let waiter = self.gate.finish(7);
            self.release();
            if waiter.is_some() {
                self.answered.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[test]
    fn loom_the_final_close_runs_exactly_once_whoever_finishes_last() {
        // Two shards report, a thread holding the manifest exclusion releases it, and an
        // application thread passes through maintenance, in every interleaving: the final
        // close runs exactly once.
        model(|| {
            let e = Engine::new(2, true);
            let report = |e: Arc<Engine>| {
                thread::spawn(move || {
                    if e.gate.report() {
                        e.try_final_close();
                    }
                })
            };
            let a = report(Arc::clone(&e));
            let b = report(Arc::clone(&e));
            let holder = {
                let e = Arc::clone(&e);
                thread::spawn(move || e.release())
            };
            e.gate.enter_maintenance();
            e.gate.leave_maintenance();
            e.try_final_close();
            for t in [a, b, holder] {
                t.join().unwrap();
            }
            assert_eq!(
                e.runs.load(Ordering::Relaxed),
                1,
                "the final close ran once"
            );
            assert_eq!(e.gate.outcome(|o| *o), Some(7));
        });
    }

    #[test]
    fn loom_the_close_caller_is_answered_exactly_once() {
        // `Engine::close` registers its waiter while the last shard reports and the final
        // close runs: the caller is answered once, by the final close or at registration.
        model(|| {
            let e = Engine::new(1, false);
            let shard = {
                let e = Arc::clone(&e);
                thread::spawn(move || {
                    if e.gate.report() {
                        e.try_final_close();
                    }
                })
            };
            if let Some((_, outcome)) = e.gate.register(1, |o| *o) {
                assert_eq!(outcome, 7);
                e.answered.fetch_add(1, Ordering::Relaxed);
            }
            shard.join().unwrap();
            assert_eq!(e.runs.load(Ordering::Relaxed), 1);
            assert_eq!(e.answered.load(Ordering::Relaxed), 1, "answered once");
        });
    }
}

#[cfg(test)]
mod tests {
    use super::CloseGate;

    #[test]
    fn a_maintainer_holds_the_final_close_until_it_leaves() {
        let gate: CloseGate<u32, ()> = CloseGate::new(1);
        gate.enter_maintenance();
        assert!(gate.report(), "the last report makes it pending");
        assert_eq!(gate.try_start(|| true), None, "maintenance holds it");
        gate.leave_maintenance();
        assert_eq!(gate.try_start(|| false), None, "the exclusion is held");
        assert_eq!(gate.try_start(|| true), Some(false));
        assert_eq!(gate.try_start(|| true), None, "it runs once");
        assert!(gate.finish(3).is_none());
        assert_eq!(gate.outcome(|o| *o), Some(3));
        assert!(!gate.report(), "a late report is a no-op");
    }

    #[test]
    fn a_failed_close_reports_unclean_and_the_first_failure_is_told() {
        let gate: CloseGate<u32, ()> = CloseGate::new(2);
        assert!(gate.fail());
        assert!(!gate.fail());
        assert!(!gate.report());
        assert!(gate.report());
        assert_eq!(gate.try_start(|| true), Some(true), "unclean");
    }
}
