//! Bridges `pigeonhole-runtime`'s [`TaskWaker`] to `std::task::Waker`, so a cooperative task
//! can poll a [`Waiter`](pigeonhole_runtime::Waiter) or any future and report
//! [`TaskPoll::Blocked`](pigeonhole_runtime::TaskPoll::Blocked) until it is woken.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use pigeonhole_runtime::TaskWaker;

struct Adapter(TaskWaker);

impl Wake for Adapter {
    fn wake(self: Arc<Self>) {
        self.0.wake();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.wake();
    }
}

/// A cached `std` waker for one task.
#[derive(Debug, Default)]
pub(crate) struct StdWaker {
    waker: Option<Waker>,
}

impl StdWaker {
    /// The waker for `task`, built on first use.
    pub(crate) fn get(&mut self, task: &TaskWaker) -> &Waker {
        self.waker
            .get_or_insert_with(|| Waker::from(Arc::new(Adapter(task.clone()))))
    }

    /// Polls `fut` once with the task's waker.
    pub(crate) fn poll<F: Future + Unpin>(
        &mut self,
        task: &TaskWaker,
        fut: &mut F,
    ) -> Poll<F::Output> {
        let waker = self.get(task).clone();
        let mut cx = Context::from_waker(&waker);
        Pin::new(fut).poll(&mut cx)
    }
}

/// Unparks one thread.
struct ThreadWake(std::thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// A waker that unparks the calling thread, for a blocking wait that parks between checks.
pub(crate) fn thread_waker() -> Waker {
    Waker::from(Arc::new(ThreadWake(std::thread::current())))
}

/// Which thread last drove each application-owned shard: a thread that drives a shard must
/// not block on work only it can run (decision D88, issue #135).
#[derive(Debug, Default)]
pub(crate) struct Drivers(Box<[std::sync::atomic::AtomicU64]>);

/// The slot of a shard whose `EngineShard` was dropped (it reported closed).
const DROPPED: u64 = u64::MAX;

thread_local! {
    /// This thread's driver token (never 0 or `DROPPED`).
    static TOKEN: u64 = {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    };
}

impl Drivers {
    pub(crate) fn new(shards: usize) -> Self {
        Self((0..shards).map(|_| Default::default()).collect())
    }

    /// The calling thread runs `shard` (`EngineShard::run_once`).
    pub(crate) fn enter(&self, shard: usize) {
        use std::sync::atomic::Ordering;
        let me = TOKEN.with(|t| *t);
        if let Some(slot) = self.0.get(shard)
            && slot.load(Ordering::Relaxed) != me
        {
            slot.store(me, Ordering::Release);
        }
    }

    /// `shard`'s `EngineShard` was dropped: nobody drives it any more.
    pub(crate) fn dropped(&self, shard: usize) {
        if let Some(slot) = self.0.get(shard) {
            slot.store(DROPPED, std::sync::atomic::Ordering::Release);
        }
    }

    /// Whether the calling thread is the last one that ran some shard still alive: it
    /// must not block on shard work.
    pub(crate) fn current_drives(&self) -> bool {
        let me = TOKEN.with(|t| *t);
        self.0
            .iter()
            .any(|s| s.load(std::sync::atomic::Ordering::Acquire) == me)
    }

    /// Whether every shard is dropped or was last run by another thread, so the calling
    /// thread may wait for the shards to finish a close.
    pub(crate) fn driven_elsewhere(&self) -> bool {
        let me = TOKEN.with(|t| *t);
        self.0.iter().all(|s| {
            let v = s.load(std::sync::atomic::Ordering::Acquire);
            v != 0 && v != me
        })
    }
}
