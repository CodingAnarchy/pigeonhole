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
//! ([`Placement::enter_lite`]), and named `phdb-serve-lite`. They end when the last
//! [`Lite`] goes (each finishes its call first), as the services sharing it do at shutdown.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

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
    jobs: OnceLock<mpsc::Sender<Job>>,
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
        }
    }

    /// Runs `f` on the pool if a place is free now, and resolves to its outcome; hands `f`
    /// back, unrun, if none is (C1).
    pub(crate) fn try_run<T, F>(&self, f: F) -> Result<oneshot::Receiver<Result<T, ApiError>>, F>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, ApiError> + Send + 'static,
    {
        let Ok(place) = Arc::clone(&self.places).try_acquire_owned() else {
            return Err(f);
        };
        let (tx, rx) = oneshot::channel();
        let job: Job = Box::new(move || {
            // A mask published since the thread started (D317 ticket 6).
            super::placement::refresh();
            let out = catch_unwind(AssertUnwindSafe(f))
                .unwrap_or_else(|_| Err(ApiError::internal("a call on the light pool panicked")));
            // The place first, so a caller that has its reply finds it free.
            drop(place);
            let _ = tx.send(out);
        });
        // If every thread failed to start, the job comes back and is dropped with its place,
        // and the caller sees its reply channel closed.
        let _ = self.jobs.get_or_init(|| self.start()).send(job);
        Ok(rx)
    }

    /// Starts the threads, all reading one queue. A thread that fails to start takes its
    /// place with it, so C3 holds with fewer places.
    fn start(&self) -> mpsc::Sender<Job> {
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
        tx
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
