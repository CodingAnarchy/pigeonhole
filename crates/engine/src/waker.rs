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
