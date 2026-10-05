use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{IoBuf, Result};

/// The pending result of a submitted operation: a read or write (resolving to its buffer),
/// a sync (resolving to `()`), or a higher-level operation built on them (a WAL group sync
/// resolving to an `Lsn`, a root commit).
///
/// Usable from both worlds: [`Completion::wait`] blocks a sync caller; as a [`Future`] it
/// registers a waker that the backend wakes on completion (no `spawn_blocking`). Dropping an
/// unfinished completion is safe: the backend keeps the buffer until the kernel is done.
#[derive(Debug)]
#[must_use = "a completion does nothing unless waited on or polled"]
pub struct Completion<T = IoBuf> {
    _priv: std::marker::PhantomData<T>,
}

impl<T: Send + 'static> Completion<T> {
    /// A completion that is already resolved (used by synchronous backends and cache hits).
    pub fn ready(result: Result<T>) -> Self {
        todo!()
    }

    /// A completion and the handle that resolves it, for layers that build their own
    /// asynchronous operations (WAL group sync, root commit) on top of the backend.
    pub fn pair() -> (Self, Resolver<T>) {
        todo!()
    }

    /// Transforms the result once it arrives (runs on the resolving thread).
    pub fn map<U: Send + 'static>(
        self,
        f: impl FnOnce(Result<T>) -> Result<U> + Send + 'static,
    ) -> Completion<U> {
        todo!()
    }

    /// Blocks the calling thread until the operation finishes.
    pub fn wait(self) -> Result<T> {
        todo!()
    }

    /// Whether the operation has finished (never blocks).
    pub fn is_ready(&self) -> bool {
        todo!()
    }
}

impl<T: Send + 'static> Future for Completion<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}

/// Resolves a [`Completion`] created with [`Completion::pair`].
#[derive(Debug)]
pub struct Resolver<T> {
    _priv: std::marker::PhantomData<T>,
}

impl<T: Send + 'static> Resolver<T> {
    /// Resolves the completion and wakes its waiter.
    pub fn resolve(self, result: Result<T>) {
        todo!()
    }
}
