use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{IoBuf, Result};

/// The pending result of a submitted read or write. Resolves to the buffer (filled, for a
/// read) or an error.
///
/// Usable from both worlds: [`Completion::wait`] blocks a sync caller; as a [`Future`] it
/// registers a waker that the backend wakes on completion (no `spawn_blocking`). Dropping an
/// unfinished completion is safe: the backend keeps the buffer until the kernel is done.
#[derive(Debug)]
#[must_use = "a completion does nothing unless waited on or polled"]
pub struct Completion {
    _priv: (),
}

impl Completion {
    /// A completion that is already resolved (used by synchronous backends and cache hits).
    pub fn ready(result: Result<IoBuf>) -> Self {
        todo!()
    }

    /// Blocks the calling thread until the operation finishes.
    pub fn wait(self) -> Result<IoBuf> {
        todo!()
    }

    /// Whether the operation has finished (never blocks).
    pub fn is_ready(&self) -> bool {
        todo!()
    }
}

impl Future for Completion {
    type Output = Result<IoBuf>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}
