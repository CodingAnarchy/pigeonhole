//! The light blocking pool (#1880): sync calls whose work per call is small and bounded,
//! run on the network workers' CPUs (the free cores and the shards' SMT siblings) instead of
//! the blocking pool's (the free cores only).
//!
//! Under the mask, `read_row` on the blocking pool saturated the free cores while the
//! siblings sat idle: ReadRow ran 13–17% below `--affinity off`, where Get, which never
//! leaves the workers, didn't (perf287's g119 part A). Heavy calls (scans, text and vector
//! calls, `/mcp`, the replica's and cluster's sync calls) stay on the blocking pool, off the
//! siblings, since a heavy call beside a shard's driver doubled its p99 (#1244).
//!
//! simp724's conditions on the design (`.coord/simp724-review-design-1880.md`):
//! - **C1, a cap of its own:** `places` places, as many as the network workers. A call that
//!   finds none free doesn't wait for one: it runs on the blocking pool, as before
//!   ([`Lite::try_run`] hands it back). So the siblings never carry more light calls than
//!   there are network workers, however many the blocking pool would take.
//! - **C2, light closures only:** the threads are plain `std` threads, outside any Tokio
//!   runtime, so a closure that reaches for one (`Handle::current`, `spawn_blocking`,
//!   `block_in_place`, `tokio::fs`) panics instead of starting heavy work on these CPUs.
//!   The panic is caught, and the call fails as `Internal`.
//! - **C3, a thread for every place:** one thread per place, so a call holding a place
//!   never waits for a thread.
//!
//! The threads start at the first call, each masked as a network worker
//! ([`Placement::enter_lite`]), and named `phdb-serve-lite`. Unmasked (`--affinity off`,
//! nothing pinned, or not Linux), the blocking pool already runs anywhere, so there is no
//! light pool ([`Lite::off`]): a second pool would only add concurrency (simp724's S1 on
//! #1972). At shutdown the server waits for the calls running here ([`Lite::drain`]) before
//! it closes the databases, as it does for the blocking pool's; the threads end when the
//! last [`Lite`] goes.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::{Semaphore, oneshot};

use super::errors::ApiError;
use super::placement::Placement;

type Job = Box<dyn FnOnce() + Send>;

/// The pool: its places, and its threads once a call has started them. A cluster node's
/// partitions share one ([`Service::for_partition`](super::service::Service::for_partition)).
pub(crate) struct Lite {
    places: Arc<Semaphore>,
    threads: usize,
    placement: Option<Arc<Placement>>,
    /// The queue the threads read, once the first call has started them; `None` if none
    /// started, or for [`Lite::off`]: every call then runs on the blocking pool.
    jobs: OnceLock<Option<mpsc::Sender<Job>>>,
    /// Threads running: the places [`Lite::drain`] waits to see free.
    live: AtomicUsize,
}

impl Lite {
    /// A pool of `threads` places (at least 1), its threads masked by `placement`.
    pub(crate) fn new(threads: usize, placement: Option<Arc<Placement>>) -> Self {
        let threads = threads.max(1);
        Self {
            places: Arc::new(Semaphore::new(threads)),
            threads,
            placement,
            jobs: OnceLock::new(),
            live: AtomicUsize::new(0),
        }
    }

    /// No light pool: every call is handed back, to run on the blocking pool.
    pub(crate) fn off() -> Self {
        let lite = Self::new(1, None);
        let _ = lite.jobs.set(None);
        lite
    }

    /// Runs `f` on the pool if a place is free now, and resolves to its outcome; hands `f`
    /// back, unrun, if none is (C1).
    pub(crate) fn try_run<T, F>(&self, f: F) -> Result<oneshot::Receiver<Result<T, ApiError>>, F>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, ApiError> + Send + 'static,
    {
        // Started before a place is taken, so a thread that fails to start can take its
        // place with it (simp724's nit on #1972).
        let Some(jobs) = self.jobs.get_or_init(|| self.start()) else {
            return Err(f);
        };
        let Ok(place) = Arc::clone(&self.places).try_acquire_owned() else {
            return Err(f);
        };
        let (tx, rx) = oneshot::channel();
        // A sampled call's work is timed where it runs (D332, simp724's M1 on #2063).
        let sampled = super::cluster::load::sampled();
        let job: Job = Box::new(move || {
            // A mask published since the thread started (D317 ticket 6).
            super::placement::refresh();
            let out = catch_unwind(AssertUnwindSafe(|| {
                super::cluster::load::run_sampled(sampled.as_ref(), f)
            }))
            .unwrap_or_else(|_| Err(ApiError::internal("a call on the light pool panicked")));
            // The place first, so a caller that has its reply finds it free.
            drop(place);
            let _ = tx.send(out);
        });
        // The receiver lives as long as a thread does, and threads end only when the sender
        // goes, so the send can't fail.
        let _ = jobs.send(job);
        Ok(rx)
    }

    /// Waits up to `timeout` for the calls running on the pool to finish; whether they did.
    /// For shutdown, once nothing can start a new call.
    pub(crate) fn drain(&self, timeout: Duration) -> bool {
        let live = self.live.load(Ordering::Acquire);
        let until = Instant::now() + timeout;
        while self.places.available_permits() < live {
            if Instant::now() >= until {
                return false;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        true
    }

    /// Starts the threads, all reading one queue. A thread that fails to start takes its
    /// place with it, so C3 holds with fewer places; if none starts, there's no queue.
    fn start(&self) -> Option<mpsc::Sender<Job>> {
        let (tx, rx) = mpsc::channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        let mut failed = 0;
        for _ in 0..self.threads {
            let rx = Arc::clone(&rx);
            let placement = self.placement.clone();
            let started = std::thread::Builder::new()
                .name("phdb-serve-lite".into())
                .spawn(move || {
                    if let Some(p) = &placement {
                        p.enter_lite();
                    }
                    loop {
                        let job = rx.lock().unwrap_or_else(PoisonError::into_inner).recv();
                        match job {
                            Ok(job) => job(),
                            Err(_) => return,
                        }
                    }
                });
            if started.is_err() {
                failed += 1;
            }
        }
        self.places.forget_permits(failed);
        let started = self.threads - failed;
        self.live.store(started, Ordering::Release);
        (started > 0).then_some(tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_call_runs_on_a_lite_thread_outside_any_runtime() {
        let lite = Lite::new(2, None);
        let rx = lite
            .try_run(|| {
                Ok((
                    std::thread::current().name().map(str::to_owned),
                    tokio::runtime::Handle::try_current().is_ok(),
                ))
            })
            .ok()
            .unwrap();
        let (name, in_runtime) = rx.await.unwrap().unwrap();
        assert_eq!(name.as_deref(), Some("phdb-serve-lite"));
        assert!(
            !in_runtime,
            "a lite thread is outside any Tokio runtime (C2)"
        );
    }

    #[tokio::test]
    async fn a_call_finding_no_free_place_comes_back_unrun() {
        let lite = Lite::new(1, None);
        let (hold, held) = std::sync::mpsc::channel::<()>();
        let (started, running) = std::sync::mpsc::channel::<()>();
        let first = lite
            .try_run(move || {
                started.send(()).unwrap();
                held.recv().unwrap();
                Ok(1)
            })
            .ok()
            .unwrap();
        running.recv().unwrap();
        let second = lite.try_run(|| Ok(2));
        let Err(back) = second else {
            panic!("the second call took a place the first holds");
        };
        assert_eq!(back().unwrap(), 2);
        hold.send(()).unwrap();
        assert_eq!(first.await.unwrap().unwrap(), 1);
        // The place is free again once the first call has replied.
        let third = lite.try_run(|| Ok(3)).ok().unwrap();
        assert_eq!(third.await.unwrap().unwrap(), 3);
    }

    #[test]
    fn with_no_light_pool_every_call_comes_back_unrun() {
        let Err(back) = Lite::off().try_run(|| Ok(5)) else {
            panic!("an off pool ran a call");
        };
        assert_eq!(back().unwrap(), 5);
    }

    #[tokio::test]
    async fn drain_waits_for_a_running_call_and_gives_up_at_its_timeout() {
        let lite = Arc::new(Lite::new(2, None));
        assert!(
            lite.drain(Duration::ZERO),
            "nothing started, nothing to wait for"
        );
        let (hold, held) = std::sync::mpsc::channel::<()>();
        let (started, running) = std::sync::mpsc::channel::<()>();
        let call = lite
            .try_run(move || {
                started.send(()).unwrap();
                held.recv().unwrap();
                Ok(())
            })
            .ok()
            .unwrap();
        running.recv().unwrap();
        assert!(
            !lite.drain(Duration::from_millis(20)),
            "a held call isn't drained"
        );
        let draining = {
            let lite = Arc::clone(&lite);
            std::thread::spawn(move || lite.drain(Duration::from_secs(30)))
        };
        hold.send(()).unwrap();
        assert!(
            draining.join().unwrap(),
            "drain returns once the call has finished"
        );
        call.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_panicking_call_fails_as_internal_and_the_thread_lives_on() {
        let lite = Lite::new(1, None);
        let rx = lite
            .try_run(|| -> Result<(), ApiError> {
                // C2: Tokio from a lite thread panics instead of running.
                let _ = tokio::runtime::Handle::current();
                Ok(())
            })
            .ok()
            .unwrap();
        assert!(rx.await.unwrap().is_err());
        let rx = lite.try_run(|| Ok(7)).ok().unwrap();
        assert_eq!(rx.await.unwrap().unwrap(), 7);
    }
}
