//! Thread-per-core shard runtime for Pigeonhole.
//!
//! The runtime runs N shard loops. Each loop owns a [`ShardHandler`] (the engine's per-shard
//! state), drains its MPSC queue of messages, and runs cooperative background [`Task`]s in
//! short time slices that always yield to queued foreground work.
//!
//! Two embedding modes share every line of the loop:
//! - **Engine-owned:** [`Runtime::start`] spawns and pins one thread per shard.
//! - **Application-owned:** [`Runtime::application_owned`] returns one [`ShardDriver`] per
//!   shard; the application calls [`ShardDriver::run_once`] from its own core threads. The
//!   simulator uses this mode to run shards deterministically.
//!
//! Messages are a type the engine defines (`H::Msg`), so the hot path has no boxing and no
//! virtual calls. Background tasks are boxed (`Box<dyn Task>`): they are few and long-lived.
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]
// Interface freeze: bodies are `todo!()`. Remove this allow when implementing.
#![allow(unused_variables, clippy::ptr_arg)]

use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use pigeonhole_io::VfsRef;

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Runtime errors.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The shard's queue is closed (shutdown).
    Closed,
    /// A thread could not be spawned or pinned.
    Spawn(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl std::error::Error for Error {}

/// A shard's index, `0..shard_count`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardId(pub u16);

/// Runtime configuration.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Number of shards (default: available CPUs).
    pub shards: usize,
    /// Pin shard threads to CPUs (engine-owned mode).
    pub pin_threads: bool,
    /// Extra pinned threads dedicated to background tasks; 0 runs them on the shards.
    pub compaction_threads: usize,
    /// Longest a background task runs before yielding.
    pub time_slice: Duration,
    /// Clock source (simulated under test).
    pub vfs: VfsRef,
}

/// The engine's per-shard state. One instance per shard, moved onto its thread.
pub trait ShardHandler: Send + 'static {
    /// The message type submitted to this shard.
    type Msg: Send + 'static;

    /// Handles one foreground message. `ctx` gives access to other shards and background
    /// spawning.
    fn handle(&mut self, ctx: &mut ShardContext<'_, Self::Msg>, msg: Self::Msg);

    /// Called after each drained batch of messages (the group-commit point: write the WAL
    /// once, sync once, publish, wake committers).
    fn end_batch(&mut self, ctx: &mut ShardContext<'_, Self::Msg>);
}

/// What a background task reports after a time slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskPoll {
    /// More work remains; run me again when foreground work allows.
    Pending,
    /// Waiting for I/O or another event; the task is woken by its [`TaskWaker`].
    Blocked,
    /// Finished.
    Done,
}

/// A cooperative background task (flush, compaction, manifest write, rebalancing).
pub trait Task: Send + 'static {
    /// Runs until `deadline_nanos` (monotonic) or until it would block, then returns.
    fn run(&mut self, deadline_nanos: u64, waker: &TaskWaker) -> TaskPoll;

    /// A short label for metrics.
    fn name(&self) -> &'static str;
}

/// Wakes a blocked background task.
#[derive(Debug, Clone)]
pub struct TaskWaker {
    _priv: (),
}

impl TaskWaker {
    /// Marks the task runnable.
    pub fn wake(&self) {
        todo!()
    }
}

/// The handler's view of the runtime while it runs on its shard.
#[derive(Debug)]
pub struct ShardContext<'a, M> {
    _priv: PhantomData<&'a M>,
}

impl<M: Send + 'static> ShardContext<'_, M> {
    /// This shard.
    pub fn shard(&self) -> ShardId {
        todo!()
    }

    /// Number of shards.
    pub fn shard_count(&self) -> usize {
        todo!()
    }

    /// Submitter for another shard.
    pub fn submitter(&self, shard: ShardId) -> &Submitter<M> {
        todo!()
    }

    /// Schedules a background task on this shard (or the compaction pool, if configured).
    pub fn spawn(&mut self, task: Box<dyn Task>) {
        todo!()
    }

    /// Monotonic time in nanoseconds.
    pub fn now_nanos(&self) -> u64 {
        todo!()
    }
}

/// Sends messages to one shard's lock-free MPSC queue and wakes it. Cheap to clone.
#[derive(Debug)]
pub struct Submitter<M> {
    _priv: PhantomData<fn(M)>,
}

impl<M> Clone for Submitter<M> {
    fn clone(&self) -> Self {
        todo!()
    }
}

impl<M: Send + 'static> Submitter<M> {
    /// The target shard.
    pub fn shard(&self) -> ShardId {
        todo!()
    }

    /// Enqueues `msg`. Fails only after shutdown.
    pub fn submit(&self, msg: M) -> Result<()> {
        todo!()
    }
}

/// Engine-owned mode: the runtime's own pinned shard threads.
#[derive(Debug)]
pub struct Runtime<H: ShardHandler> {
    _priv: PhantomData<H>,
}

impl<H: ShardHandler> Runtime<H> {
    /// Spawns one thread per handler (`handlers.len()` must equal `config.shards`).
    pub fn start(config: RuntimeConfig, handlers: Vec<H>) -> Result<Self> {
        todo!()
    }

    /// Application-owned mode: no threads; one driver per shard.
    pub fn application_owned(
        config: RuntimeConfig,
        handlers: Vec<H>,
    ) -> Result<Vec<ShardDriver<H>>> {
        todo!()
    }

    /// Submitter for `shard`.
    pub fn submitter(&self, shard: ShardId) -> Submitter<H::Msg> {
        todo!()
    }

    /// Number of shards.
    pub fn shard_count(&self) -> usize {
        todo!()
    }

    /// Whether the calling thread is a shard thread, and which (inline-write fast path).
    pub fn current_shard() -> Option<ShardId> {
        todo!()
    }

    /// Closes every queue, lets each loop finish its queued messages, and joins the threads.
    pub fn shutdown(self) -> Result<Vec<H>> {
        todo!()
    }
}

/// Application-owned mode: drives one shard from a thread the application owns.
#[derive(Debug)]
pub struct ShardDriver<H: ShardHandler> {
    _priv: PhantomData<H>,
}

impl<H: ShardHandler> ShardDriver<H> {
    /// This shard.
    pub fn shard(&self) -> ShardId {
        todo!()
    }

    /// Submitter for any shard.
    pub fn submitter(&self, shard: ShardId) -> Submitter<H::Msg> {
        todo!()
    }

    /// Drains queued messages, calls `end_batch`, then runs background tasks until
    /// `deadline_nanos`. Returns whether work remains.
    pub fn run_once(&mut self, deadline_nanos: u64) -> bool {
        todo!()
    }

    /// Runs `f` on this shard's handler inline (the application's own writes on its own
    /// core, without a queue hop).
    pub fn with_handler<R>(
        &mut self,
        f: impl FnOnce(&mut H, &mut ShardContext<'_, H::Msg>) -> R,
    ) -> R {
        todo!()
    }

    /// Registers a waker the runtime calls when work arrives (for the application's event
    /// loop).
    pub fn set_wakeup(&mut self, wake: Box<dyn Fn() + Send + Sync>) {
        todo!()
    }
}

/// Creates a one-shot completion: the shard resolves the [`Notifier`]; the caller blocks on,
/// or awaits, the [`Waiter`].
pub fn completion<T: Send>() -> (Notifier<T>, Waiter<T>) {
    todo!()
}

/// The resolving half of a completion.
#[derive(Debug)]
pub struct Notifier<T> {
    _priv: PhantomData<T>,
}

impl<T: Send> Notifier<T> {
    /// Resolves the completion and wakes the waiter (thread unpark or task waker).
    pub fn notify(self, value: T) {
        todo!()
    }
}

/// The waiting half of a completion. Sync callers call [`Waiter::wait`]; async callers await
/// it. Dropping it is always safe.
#[derive(Debug)]
pub struct Waiter<T> {
    _priv: PhantomData<T>,
}

impl<T: Send> Waiter<T> {
    /// Blocks until notified. Returns `None` if the notifier was dropped unresolved.
    pub fn wait(self) -> Option<T> {
        todo!()
    }
}

impl<T: Send> Future for Waiter<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}
