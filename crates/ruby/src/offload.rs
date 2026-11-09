//! The worker-thread jobs under a fiber scheduler (#1574 PR 3, §3.5 of its design note):
//! memory `recall`, the near-dedup lookup before a `remember`, and an IVF store's `settle`.
//! They have no future of the engine's, so they run on `pigeonhole-ruby-offload`, a small
//! pool of the binding's own (at most `min(available_parallelism, 8)` threads, started as
//! jobs need them), and complete through a fiber op like any other waiting call.
//!
//! A job is queued when its op starts. Dropping the op (its fiber stopped, say) before the
//! job starts skips it; once started, it runs to its end and its result is dropped. A job
//! that panics completes with the panic (the fiber raises), and its thread lives on
//! (cli692's R1 on #1614).
//!
//! After a fork the child's first job finds a different pid and starts a fresh pool: the
//! parent's threads don't exist there, and its queue is left behind.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};

use crate::convert::lock;

/// The pool's thread cap.
fn cap() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get().min(8))
}

type Job = Box<dyn FnOnce() + Send>;

struct Queue {
    pid: u32,
    jobs: VecDeque<Job>,
    /// Threads started, and how many of them wait for a job.
    threads: usize,
    idle: usize,
}

struct Pool {
    queue: Mutex<Queue>,
    more: Condvar,
}

fn pool() -> &'static Mutex<Arc<Pool>> {
    static POOL: OnceLock<Mutex<Arc<Pool>>> = OnceLock::new();
    POOL.get_or_init(|| Mutex::new(new_pool()))
}

fn new_pool() -> Arc<Pool> {
    Arc::new(Pool {
        queue: Mutex::new(Queue {
            pid: std::process::id(),
            jobs: VecDeque::new(),
            threads: 0,
            idle: 0,
        }),
        more: Condvar::new(),
    })
}

/// Queues `job`, starting a thread if none is idle and the pool is under its cap. If no
/// thread can be started and none exists, runs it here (the caller is never left waiting
/// for a job no thread will run).
fn submit(job: Job) {
    let p = {
        let mut g = lock(pool());
        if lock(&g.queue).pid != std::process::id() {
            *g = new_pool();
        }
        Arc::clone(&g)
    };
    let mut q = lock(&p.queue);
    q.jobs.push_back(job);
    if q.idle > 0 {
        p.more.notify_one();
    }
    // A thread more unless the idle ones cover every queued job (cli692's N1 on #1614), so
    // a short job doesn't wait behind a long one while the pool is under its cap.
    if q.idle < q.jobs.len() && q.threads < cap() {
        q.threads += 1;
        drop(q);
        let worker = Arc::clone(&p);
        let started = std::thread::Builder::new()
            .name("pigeonhole-ruby-offload".into())
            .spawn(move || run(&worker));
        if started.is_ok() {
            return;
        }
        q = lock(&p.queue);
        q.threads -= 1;
        if q.threads > 0 {
            return;
        }
        let job = q.jobs.pop_back();
        drop(q);
        if let Some(job) = job {
            job();
        }
    }
}

/// A pool thread: runs jobs for as long as the process does.
fn run(p: &Pool) {
    let mut q = lock(&p.queue);
    loop {
        if let Some(job) = q.jobs.pop_front() {
            drop(q);
            #[cfg(feature = "test-hooks")]
            hooks::wait();
            job();
            q = lock(&p.queue);
            continue;
        }
        q.idle += 1;
        q = p
            .more
            .wait(q)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        q.idle -= 1;
    }
}

/// A job's result (or its panic), and the waker of the op waiting for it.
struct Slot<T> {
    out: Option<std::thread::Result<T>>,
    waker: Option<Waker>,
}

struct Shared<T> {
    slot: Mutex<Slot<T>>,
    /// Set when the op is dropped: a job not yet started is skipped.
    dropped: AtomicBool,
}

/// The future of one job: `Ready` with its result, or its panic, once the pool has run it.
pub(crate) struct JobFuture<T>(Arc<Shared<T>>);

/// Runs `f` on the pool; the returned future is ready with its result, or `Err` with its
/// panic. `f` is queued here, so an op's first poll (at its call) has already submitted it.
pub(crate) fn job<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> JobFuture<T> {
    let shared = Arc::new(Shared {
        slot: Mutex::new(Slot {
            out: None,
            waker: None,
        }),
        dropped: AtomicBool::new(false),
    });
    let s = Arc::clone(&shared);
    submit(Box::new(move || {
        if s.dropped.load(Ordering::Acquire) {
            return;
        }
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        let waker = {
            let mut g = lock(&s.slot);
            g.out = Some(out);
            g.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }));
    JobFuture(shared)
}

impl<T> Future for JobFuture<T> {
    type Output = std::thread::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut g = lock(&self.0.slot);
        match g.out.take() {
            Some(out) => Poll::Ready(out),
            None => {
                g.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl<T> Drop for JobFuture<T> {
    fn drop(&mut self) {
        self.0.dropped.store(true, Ordering::Release);
    }
}

/// A hold seam for the tests (feature `test-hooks`): while held, the pool's threads wait
/// before each job, so a test can show a fiber parked on a job while others run.
#[cfg(feature = "test-hooks")]
pub(crate) mod hooks {
    use std::sync::{Condvar, Mutex};

    static HELD: Mutex<bool> = Mutex::new(false);
    static OPENED: Condvar = Condvar::new();

    /// Waits while the pool is held.
    pub(super) fn wait() {
        let mut g = HELD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while *g {
            g = OPENED
                .wait(g)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    /// `Pigeonhole::Native.offload_hold(on)`: holds or releases the pool's jobs.
    pub(crate) fn hold(on: bool) {
        *HELD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = on;
        if !on {
            OPENED.notify_all();
        }
    }
}
