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
//! # Shared state
//! Shards share nothing mutable except their documented queues: each shard's message queue
//! (a lock-free `std::sync::mpsc` channel plus a closed flag and in-flight counter), its
//! idle/wake flag, and the task queues of the optional compaction threads. A
//! [`Notifier`]/[`Waiter`] pair shares one completion slot between two parties.
//!
//! # Example
//! ```
//! use std::sync::Arc;
//! use pigeonhole_io::pread::PreadVfs;
//! use pigeonhole_runtime::{
//!     Notifier, Runtime, RuntimeConfig, ShardContext, ShardHandler, ShardId, completion,
//! };
//!
//! /// Each shard keeps a running sum and replies with it.
//! struct Adder(u64);
//!
//! impl ShardHandler for Adder {
//!     type Msg = (u64, Notifier<u64>);
//!     fn handle(&mut self, _ctx: &mut ShardContext<'_, Self::Msg>, (n, reply): Self::Msg) {
//!         self.0 += n;
//!         reply.notify(self.0);
//!     }
//!     fn end_batch(&mut self, _ctx: &mut ShardContext<'_, Self::Msg>) {}
//! }
//!
//! let mut config = RuntimeConfig::new(PreadVfs::new(1));
//! config.shards = 2;
//! config.pin_threads = false;
//! let rt = Runtime::start(config, vec![Adder(0), Adder(100)])?;
//! let (tx, rx) = completion();
//! rt.submitter(ShardId(1)).submit((5, tx))?;
//! assert_eq!(rx.wait(), Some(105));
//! let handlers = rt.shutdown()?;
//! assert_eq!(handlers[1].0, 105);
//! # Ok::<(), pigeonhole_runtime::Error>(())
//! ```
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]

mod completion;
mod sched;
mod signal;
mod submit_gate;

use std::cell::Cell;
use std::fmt;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use pigeonhole_io::{Vfs, VfsRef};

pub use completion::{Notifier, Settle, SlotCache, Waiter, completion, completion_from};
pub use sched::{Task, TaskPoll, TaskWaker};

use sched::{PoolShared, Scheduler, Spawner};
use signal::{Signal, WakeTarget};

/// Most messages handled before `end_batch` runs, so a flooded queue still commits groups.
const MAX_BATCH: usize = 1024;

/// The most turns in a row that skip their background slices because a future of the
/// driving thread was woken (D237): the next turn runs them, so flush and compaction keep
/// at least one turn in `MAX_TURN_YIELDS + 1`.
const MAX_TURN_YIELDS: u32 = 8;

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
    /// The configuration is not valid for the chosen mode; names the field.
    InvalidConfig(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Closed => f.write_str("shard queue is closed"),
            Error::Spawn(e) => write!(f, "could not start runtime thread: {e}"),
            Error::InvalidConfig(what) => write!(f, "invalid runtime configuration: {what}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Closed | Error::InvalidConfig(_) => None,
            Error::Spawn(e) => Some(e),
        }
    }
}

/// A shard's index, `0..shard_count`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardId(pub u16);

/// Runtime configuration.
///
/// ```
/// use std::time::Duration;
/// use pigeonhole_io::pread::PreadVfs;
/// use pigeonhole_runtime::RuntimeConfig;
///
/// let mut config = RuntimeConfig::new(PreadVfs::new(1));
/// assert!(config.shards >= 1);
/// config.time_slice = Duration::from_micros(200);
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RuntimeConfig {
    /// Number of shards (default: one per physical core, D220).
    pub shards: usize,
    /// Pin shard threads to CPUs (engine-owned mode).
    pub pin_threads: bool,
    /// Extra pinned threads dedicated to background tasks; 0 runs them on the shards.
    /// Engine-owned mode only: application-owned mode starts no threads, so
    /// [`Runtime::application_owned`] refuses a nonzero value with
    /// [`Error::InvalidConfig`] (decision D40). As with tasks on shards, tasks still queued
    /// or running on these threads at shutdown are dropped unfinished.
    pub compaction_threads: usize,
    /// Longest a background task runs before yielding.
    pub time_slice: Duration,
    /// Engine-owned mode: how long a shard thread that just handled a message keeps
    /// polling its queue before it parks (D198). A message that arrives meanwhile is
    /// handled without the producer waking the thread. Zero (the default) parks at once.
    /// A poll that finds nothing backs off: the thread skips the next 1, 2, 4, ... up to 64
    /// polls, and an idle shard (no message since its last park) never polls.
    pub idle_spin: Duration,
    /// Clock source (simulated under test).
    pub vfs: VfsRef,
}

impl RuntimeConfig {
    /// Defaults: one shard per physical core (D220), pinned, no compaction threads, 500 µs
    /// slices.
    pub fn new(vfs: VfsRef) -> Self {
        Self {
            shards: pigeonhole_io::sys::default_shards(),
            pin_threads: true,
            compaction_threads: 0,
            time_slice: Duration::from_micros(500),
            idle_spin: Duration::ZERO,
            vfs,
        }
    }

    fn slice_nanos(&self) -> u64 {
        u64::try_from(self.time_slice.as_nanos())
            .unwrap_or(u64::MAX)
            .max(1)
    }
}

/// The engine's per-shard state. One instance per shard, moved onto its thread.
///
/// See the [crate example](crate#example).
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

/// The handler's view of the runtime while it runs on its shard.
///
/// ```
/// use pigeonhole_runtime::{ShardContext, ShardHandler, ShardId};
///
/// /// Forwards every message to the next shard until its hop count runs out.
/// struct Relay;
///
/// impl ShardHandler for Relay {
///     type Msg = u32;
///     fn handle(&mut self, ctx: &mut ShardContext<'_, u32>, hops: u32) {
///         if hops > 0 {
///             let next = ShardId(((ctx.shard().0 as usize + 1) % ctx.shard_count()) as u16);
///             let _ = ctx.submitter(next).submit(hops - 1);
///         }
///     }
///     fn end_batch(&mut self, _ctx: &mut ShardContext<'_, u32>) {}
/// }
/// ```
pub struct ShardContext<'a, M> {
    shard: ShardId,
    submitters: &'a [Submitter<M>],
    spawner: &'a mut Spawner,
    vfs: &'a dyn Vfs,
}

impl<M> fmt::Debug for ShardContext<'_, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardContext")
            .field("shard", &self.shard)
            .field("shard_count", &self.submitters.len())
            .finish_non_exhaustive()
    }
}

impl<M: Send + 'static> ShardContext<'_, M> {
    /// This shard.
    pub fn shard(&self) -> ShardId {
        self.shard
    }

    /// Number of shards.
    pub fn shard_count(&self) -> usize {
        self.submitters.len()
    }

    /// Submitter for another shard.
    ///
    /// # Panics
    /// If `shard` is out of range.
    pub fn submitter(&self, shard: ShardId) -> &Submitter<M> {
        &self.submitters[usize::from(shard.0)]
    }

    /// Schedules a background task on this shard (or the compaction pool, if configured).
    pub fn spawn(&mut self, task: Box<dyn Task>) {
        self.spawner.spawn(task);
    }

    /// Monotonic time in nanoseconds.
    pub fn now_nanos(&self) -> u64 {
        self.vfs.monotonic_nanos()
    }
}

/// One shard's queue. Producers share it through [`Submitter`]s.
struct Inbox<M> {
    shard: ShardId,
    tx: Sender<M>,
    /// Submits against shutdown (`submit_gate`).
    gate: submit_gate::SubmitGate,
    signal: Arc<Signal>,
}

impl<M> Inbox<M> {
    fn close(&self) {
        self.gate.close();
        self.signal.force_wake();
    }

    fn is_closed(&self) -> bool {
        self.gate.is_closed()
    }
}

/// Sends messages to one shard's lock-free MPSC queue and wakes it. Cheap to clone.
///
/// Messages from one submitter (or one thread) arrive in the order they were submitted.
/// See the [crate example](crate#example).
pub struct Submitter<M> {
    inbox: Arc<Inbox<M>>,
}

impl<M> fmt::Debug for Submitter<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Submitter")
            .field("shard", &self.inbox.shard)
            .finish_non_exhaustive()
    }
}

impl<M> Clone for Submitter<M> {
    fn clone(&self) -> Self {
        Self {
            inbox: Arc::clone(&self.inbox),
        }
    }
}

impl<M: Send + 'static> Submitter<M> {
    /// The target shard.
    pub fn shard(&self) -> ShardId {
        self.inbox.shard
    }

    /// `(parks, wakes)` of the target shard since the runtime started (ICR 0027): times its
    /// engine-owned thread parked with nothing to do (after its idle spin), and submits or
    /// notifications that found it announced asleep and woke it. An application-owned
    /// shard's loop sleeps in the application, so only its wakes are counted. Relaxed: a
    /// reading taken while the shard runs may lag it.
    pub fn idle_counts(&self) -> (u64, u64) {
        self.inbox.signal.idle_counts()
    }

    /// Nanoseconds the target shard's engine-owned thread has spent in its work turns since
    /// the runtime started (D217, ICR 0033): handling messages and running its tasks, not
    /// spinning (D198) or parked. Its rise over an interval, divided by the interval, is how
    /// busy the shard was. An application-owned shard's loop is the application's, so
    /// nothing is counted. Relaxed: a reading taken during a turn lags it.
    pub fn busy_nanos(&self) -> u64 {
        self.inbox.signal.busy_nanos()
    }

    /// Nanoseconds the compaction pool's threads have spent in slices of the tasks the
    /// target shard spawned onto the pool (its flushes and compactions, D256), since the
    /// runtime started. With `compaction_threads = 0` the shard runs those slices inside
    /// its own turns and nothing is counted here. Relaxed, as [`Submitter::busy_nanos`].
    pub fn pool_busy_nanos(&self) -> u64 {
        self.inbox.signal.pool_busy_nanos()
    }

    /// Nanoseconds the target shard's engine-owned thread has spent in its spin before
    /// parking (D198: polling for the next message after handling one), since the runtime
    /// started (D256). Not part of [`Submitter::busy_nanos`]; the balancer adds the two.
    /// Relaxed, as that one.
    pub fn spin_nanos(&self) -> u64 {
        self.inbox.signal.spin_nanos()
    }

    /// Enqueues `msg`. Fails only after shutdown.
    pub fn submit(&self, msg: M) -> Result<()> {
        let inbox = &*self.inbox;
        // Either shutdown sees us enter and waits for the send, or we see it closed and
        // refuse (`submit_gate`).
        if !inbox.gate.enter() {
            return Err(Error::Closed);
        }
        let sent = inbox.tx.send(msg);
        inbox.gate.leave();
        sent.map_err(|_| Error::Closed)?;
        inbox.signal.notify();
        Ok(())
    }
}

thread_local! {
    static CURRENT_SHARD: Cell<Option<ShardId>> = const { Cell::new(None) };
}

/// Marks the calling thread as running `shard` until dropped.
struct EnterShard(Option<ShardId>);

impl EnterShard {
    fn new(shard: ShardId) -> Self {
        Self(CURRENT_SHARD.with(|c| c.replace(Some(shard))))
    }
}

impl Drop for EnterShard {
    fn drop(&mut self) {
        CURRENT_SHARD.with(|c| c.set(self.0));
    }
}

/// A small id for the calling thread, nonzero: cheaper to compare each turn than
/// `thread::current().id()`, which counts a reference.
fn thread_token() -> u64 {
    thread_local! {
        static TOKEN: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    TOKEN.with(|t| match t.get() {
        0 => {
            let v = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            t.set(v);
            v
        }
        v => v,
    })
}

/// The shard loop, shared by both embedding modes.
struct ShardCore<H: ShardHandler> {
    id: ShardId,
    handler: H,
    rx: Receiver<H::Msg>,
    /// A message taken while checking for work before sleeping; handled first.
    stash: Option<H::Msg>,
    submitters: Arc<[Submitter<H::Msg>]>,
    spawner: Spawner,
    vfs: VfsRef,
    time_slice: u64,
    /// The clock when the loop last went idle with sleeping tasks: if it reads the same at
    /// the next pass, the clock is not moving on its own and sleepers run early to notice.
    idle_at: Option<u64>,
    /// The thread ([`thread_token`]) last given its own I/O ring (`Vfs::attach_thread`, at
    /// its first turn of this shard; 0: none yet), and whether it got one: a turn reaps only
    /// then, so a backend without rings costs a turn nothing (#402). Per thread, not once: an
    /// application may move a shard to another thread, which needs a ring of its own, or its
    /// I/O would go to a ring no thread reaps (the #473 scaling hang).
    attached: u64,
    own_ring: bool,
    /// Debug builds: whether the thread's ring still had I/O in flight when its last turn of
    /// this shard ended, and whether `release` ran since. A shard that turns up on another
    /// thread with I/O left behind unreleased is a bug (#492).
    #[cfg(debug_assertions)]
    left_io: bool,
    #[cfg(debug_assertions)]
    released: bool,
    /// Polling the queue before parking (D198; engine-owned mode only).
    spin: IdleSpin,
    /// Turns in a row that ended before their background slices because a future of the
    /// driving thread was woken (D237; application-owned mode only, at most
    /// [`MAX_TURN_YIELDS`]).
    yields: u32,
}

/// An engine-owned shard's poll of its queue before it parks (D198).
#[derive(Debug)]
struct IdleSpin {
    window: Duration,
    /// A message was handled since the thread last parked or polled.
    worked: bool,
    /// Polls still to skip after one found nothing, and the backoff exponent.
    skip: u32,
    level: u32,
}

impl IdleSpin {
    /// The most polls skipped after polls kept finding nothing.
    const MAX_SKIP: u32 = 64;

    fn new(window: Duration) -> Self {
        Self {
            window,
            worked: false,
            skip: 0,
            level: 0,
        }
    }

    /// Whether to poll before parking now; consumes the "handled a message" mark.
    fn due(&mut self) -> bool {
        if self.window.is_zero() || !std::mem::take(&mut self.worked) {
            return false;
        }
        if self.skip > 0 {
            self.skip -= 1;
            return false;
        }
        true
    }

    /// Records a poll's outcome: a hit resets the backoff, a miss doubles it.
    fn record(&mut self, hit: bool) {
        if hit {
            self.level = 0;
        } else {
            self.level = (self.level + 1).min(Self::MAX_SKIP.trailing_zeros());
            self.skip = 1 << (self.level - 1);
        }
    }
}

impl<H: ShardHandler> ShardCore<H> {
    fn inbox(&self) -> &Inbox<H::Msg> {
        &self.submitters[usize::from(self.id.0)].inbox
    }

    fn signal(&self) -> &Signal {
        &self.inbox().signal
    }

    /// Splits `self` into the handler and its context.
    fn parts(&mut self) -> (&mut H, ShardContext<'_, H::Msg>) {
        let ctx = ShardContext {
            shard: self.id,
            submitters: &self.submitters,
            spawner: &mut self.spawner,
            vfs: &*self.vfs,
        };
        (&mut self.handler, ctx)
    }

    /// Handles up to [`MAX_BATCH`] queued messages, then calls `end_batch` if any ran.
    fn drain(&mut self) -> usize {
        let stash = self.stash.take();
        let ShardCore {
            id,
            handler,
            rx,
            submitters,
            spawner,
            vfs,
            ..
        } = self;
        let mut ctx = ShardContext {
            shard: *id,
            submitters,
            spawner,
            vfs: &**vfs,
        };
        let mut n = 0;
        if let Some(msg) = stash {
            handler.handle(&mut ctx, msg);
            n += 1;
        }
        while n < MAX_BATCH {
            match rx.try_recv() {
                Ok(msg) => {
                    handler.handle(&mut ctx, msg);
                    n += 1;
                }
                Err(_) => break,
            }
        }
        if n > 0 {
            handler.end_batch(&mut ctx);
            self.spin.worked = true;
        }
        n
    }

    /// Before parking (engine-owned mode, D198): polls the queue for up to the spin window
    /// if a message was handled since the last park, so a client's next message is taken
    /// without a wakeup. Returns whether work arrived (the loop runs again at once). The
    /// window is measured on the real clock: a simulated clock may not move while polling.
    fn spin_before_park(&mut self) -> bool {
        if !self.spin.due() {
            return false;
        }
        // A poll that runs counts as busy (D256): the thread is awake and polling, kept so
        // by its clients. On the VFS clock, as the turns are (a simulated clock does not
        // move while polling, and counts nothing). A skipped poll (the window is zero, no
        // message since the last park, or the backoff) adds nothing: timing the skip
        // counted two clock reads' worth on Linux and macOS and nothing on Windows, where
        // consecutive reads of the clock can be equal.
        let spun_from = self.vfs.monotonic_nanos();
        let hit = self.spin_poll();
        self.signal()
            .add_spin(self.vfs.monotonic_nanos().saturating_sub(spun_from));
        hit
    }

    /// The poll itself: the queue for up to the spin window, then the sleep handshake on a
    /// miss. Returns whether work arrived.
    fn spin_poll(&mut self) -> bool {
        // Awake while polling, so a producer does not wake the thread (on a ring backend
        // that is a syscall); sleep is announced again, and checked once more, on a miss.
        self.signal().awake();
        let end = std::time::Instant::now() + self.spin.window;
        let mut polls = 0u32;
        let mut hit = loop {
            if self.stash.is_none() {
                self.stash = self.rx.try_recv().ok();
            }
            if self.stash.is_some() || self.signal().task_woken() || self.inbox().is_closed() {
                break true;
            }
            polls = polls.wrapping_add(1);
            if polls.is_multiple_of(64) {
                if std::time::Instant::now() >= end {
                    break false;
                }
                // Let another runnable thread in (an oversubscribed machine).
                thread::yield_now();
            } else {
                std::hint::spin_loop();
            }
        };
        if !hit {
            // The sleep handshake again: whatever a producer published before it read the
            // flag is found here, and anything later wakes the thread.
            hit = self.check_before_sleep();
        }
        self.spin.record(hit);
        if hit {
            self.signal().awake();
        }
        hit
    }

    fn run_once(&mut self, deadline: u64) -> bool {
        let more = self.run_turn(deadline);
        #[cfg(debug_assertions)]
        {
            self.left_io = self.own_ring && pigeonhole_io::own_io_in_flight();
        }
        more
    }

    /// Hands the shard over cleanly: drives it until the calling thread's ring has nothing
    /// in flight, so nothing of the shard's waits on a ring only this thread may reap (#492,
    /// ICR 0029). Without per-thread rings it returns at once.
    fn release(&mut self) {
        if !self.own_ring || self.attached != thread_token() {
            return;
        }
        let slice = std::time::Duration::from_millis(1);
        while pigeonhole_io::own_io_in_flight() {
            pigeonhole_io::reap_own_io(Some(slice));
            let deadline = self.vfs.monotonic_nanos().saturating_add(1_000_000);
            self.run_turn(deadline);
        }
        #[cfg(debug_assertions)]
        {
            self.left_io = false;
            self.released = true;
        }
    }

    fn run_turn(&mut self, deadline: u64) -> bool {
        let _shard = EnterShard::new(self.id);
        self.signal().awake();
        // A backend with a ring per driving thread (#402) gives each thread that runs this
        // shard its own (a shard moved to another thread needs one there, #473); what
        // completed on it resolves now, so the work it unblocks runs this turn. Without rings
        // nothing depends on the thread: a turn pays the two branches it always did, and only
        // a ring backend reads the thread token (#485's instruction ceilings).
        if self.own_ring {
            if self.attached != thread_token() {
                #[cfg(debug_assertions)]
                {
                    assert!(
                        !self.left_io || self.released,
                        "shard {} moved to another thread while its I/O was in flight on the \
                         old thread's ring: call release() on the old thread first (#492)",
                        self.id.0
                    );
                    self.released = false;
                }
                self.attach();
            }
            pigeonhole_io::reap_own_io(None);
        } else if self.attached == 0 {
            self.attach();
        }
        self.drain();
        self.spawner.local.collect_woken();
        let start = self.vfs.monotonic_nanos();
        if self.idle_at.take() == Some(start) && self.vfs.clock_is_simulated() {
            // A simulated clock has not moved since the loop went idle: sleepers check for
            // themselves (a real clock that reads the same only ticks coarsely, #263).
            self.spawner.local.wake_sleepers();
        }
        self.spawner.local.wake_due(start);
        if self.spawner.local.has_runnable() {
            // D237: the drain woke a future the driving thread awaits (a commit it submitted
            // settled): hand the thread back before the background slices, so it sees that
            // now rather than at the deadline. The slices run next turn, and after
            // `MAX_TURN_YIELDS` such turns in a row one runs them anyway. Only a
            // `ShardDriver::waker` wakes this, so engine-owned threads never skip. Checked
            // only when there are slices to skip: a turn without background work pays
            // nothing (the D193 commit ceilings), and a wake left for a later turn costs
            // that turn's slices at most.
            if self.signal().take_turn_woken() && self.yields < MAX_TURN_YIELDS {
                self.yields += 1;
                return true;
            }
            self.yields = 0;
            loop {
                let now = self.vfs.monotonic_nanos();
                if now >= deadline {
                    break;
                }
                self.spawner
                    .local
                    .run_one(now.saturating_add(self.time_slice).min(deadline));
                // Foreground first: whatever queued during the slice runs before the next
                // one.
                self.drain();
                self.spawner.local.collect_woken();
                self.spawner.local.wake_due(self.vfs.monotonic_nanos());
                if !self.spawner.local.has_runnable() {
                    break;
                }
            }
        }
        if self.spawner.local.has_runnable() {
            return true;
        }
        let more = self.check_before_sleep();
        if !more && self.spawner.local.next_deadline().is_some() {
            self.idle_at = Some(self.vfs.monotonic_nanos());
        }
        more
    }

    /// Gives the calling thread its own I/O ring if the backend has them, once per thread.
    #[cold]
    fn attach(&mut self) {
        self.attached = thread_token();
        self.vfs.attach_thread();
        self.own_ring = pigeonhole_io::own_io_waker().is_some();
    }

    /// The earliest deadline of a sleeping background task on this shard.
    fn next_deadline(&self) -> Option<u64> {
        self.spawner.local.next_deadline()
    }

    /// Announces sleep, then re-checks for work. Returns whether work remains.
    fn check_before_sleep(&mut self) -> bool {
        self.signal().prepare_sleep();
        if self.stash.is_none() {
            self.stash = self.rx.try_recv().ok();
        }
        if self.stash.is_some() || self.signal().task_woken() {
            self.signal().awake();
            return true;
        }
        false
    }

    /// An application thread's wait between turns (`ShardDriver::wait`, #553): until work
    /// arrives, a woken task, this thread's own I/O completing, the next background
    /// deadline, or `timeout`. It waits in the thread's ring while its own I/O is in flight
    /// (only a reap completes that), and parks otherwise.
    fn wait(&mut self, timeout: Duration) {
        let now = self.vfs.monotonic_nanos();
        let wait = self.next_deadline().map_or(timeout, |d| {
            Duration::from_nanos(d.saturating_sub(now)).min(timeout)
        });
        if wait.is_zero() {
            return;
        }
        let ring = self.own_ring && pigeonhole_io::own_io_in_flight();
        let signal = &self.submitters[usize::from(self.id.0)].inbox.signal;
        let (stash, rx) = (&mut self.stash, &self.rx);
        signal.sleep_unless(
            || {
                if stash.is_none() {
                    *stash = rx.try_recv().ok();
                }
                stash.is_some()
            },
            || {
                if ring {
                    pigeonhole_io::reap_own_io(Some(wait));
                } else {
                    thread::park_timeout(wait);
                }
            },
        );
    }

    /// Shutdown: wait out in-flight submits, then handle everything queued.
    fn finish(&mut self) {
        self.inbox().gate.wait_drained();
        while self.drain() > 0 {}
    }
}

/// Longest an idle shard thread waits on its own I/O ring before it looks again (a wake or a
/// completion ends the wait sooner).
const RING_IDLE_WAIT: std::time::Duration = std::time::Duration::from_millis(100);

/// Body of an engine-owned shard thread.
fn shard_main<H: ShardHandler>(mut core: ShardCore<H>) -> H {
    let _shard = EnterShard::new(core.id);
    let mut idle_park = sched::IdlePark::default();
    core.attach();
    core.signal()
        .set_target(match pigeonhole_io::own_io_waker() {
            Some(w) => WakeTarget::ThreadRing(thread::current(), w),
            None => WakeTarget::Thread(thread::current()),
        });
    loop {
        let start = core.vfs.monotonic_nanos();
        let deadline = start.saturating_add(core.time_slice);
        let more = core.run_once(deadline);
        // The turn's time, for the balancer's view of how busy this shard is (D217).
        core.signal()
            .add_busy(core.vfs.monotonic_nanos().saturating_sub(start));
        if core.inbox().is_closed() {
            core.finish();
            return core.handler;
        }
        // No spin while I/O only this thread reaps is in flight: it waits on that below.
        if !more && !(core.own_ring && pigeonhole_io::own_io_in_flight()) && core.spin_before_park()
        {
            continue;
        }
        if !more {
            if core.own_ring && pigeonhole_io::own_io_in_flight() {
                // I/O only this thread completes (its ring, #402): wait on it rather than
                // park, until a completion, a wake (the ring's eventfd) or the deadline.
                let now = core.vfs.monotonic_nanos();
                let until = core
                    .next_deadline()
                    .map_or(RING_IDLE_WAIT, |d| {
                        std::time::Duration::from_nanos(d.saturating_sub(now))
                    })
                    .min(RING_IDLE_WAIT);
                pigeonhole_io::reap_own_io(Some(until));
                continue;
            }
            // Until the next message, or the earliest sleeping task's deadline (re-checked
            // sooner while the clock has not shown it keeps real time: a simulated clock
            // moves without waking anyone).
            core.signal().parked();
            match core.next_deadline() {
                None => thread::park(),
                Some(d) => idle_park.park(&*core.vfs, d),
            }
        }
    }
}

/// Every shard's submitter, indexed by shard.
type Submitters<M> = Arc<[Submitter<M>]>;

/// Builds every shard's queue and loop state.
fn build<H: ShardHandler>(
    config: &RuntimeConfig,
    handlers: Vec<H>,
    pool: Arc<[Arc<PoolShared>]>,
) -> (Submitters<H::Msg>, Vec<ShardCore<H>>) {
    let n = handlers.len();
    assert_eq!(n, config.shards, "handlers.len() must equal config.shards");
    assert!(
        (1..=1 << 16).contains(&n),
        "shard count must be in 1..=65536"
    );
    let mut rxs = Vec::with_capacity(n);
    let submitters: Arc<[Submitter<H::Msg>]> = (0..n)
        .map(|i| {
            let (tx, rx) = mpsc::channel();
            rxs.push(rx);
            Submitter {
                inbox: Arc::new(Inbox {
                    shard: ShardId(i as u16),
                    tx,
                    gate: submit_gate::SubmitGate::new(),
                    signal: Arc::new(Signal::new()),
                }),
            }
        })
        .collect();
    let cores = handlers
        .into_iter()
        .zip(rxs)
        .enumerate()
        .map(|(i, (handler, rx))| ShardCore {
            id: ShardId(i as u16),
            handler,
            rx,
            stash: None,
            spawner: Spawner::new(
                Scheduler::new(Arc::clone(&submitters[i].inbox.signal)),
                Arc::clone(&pool),
            ),
            submitters: Arc::clone(&submitters),
            vfs: Arc::clone(&config.vfs),
            time_slice: config.slice_nanos(),
            idle_at: None,
            yields: 0,
            attached: 0,
            #[cfg(debug_assertions)]
            left_io: false,
            #[cfg(debug_assertions)]
            released: false,
            own_ring: false,
            spin: IdleSpin::new(config.idle_spin),
        })
        .collect();
    (submitters, cores)
}

/// How a runtime's threads were pinned (D235): see [`Runtime::pinning`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Pinning {
    /// Threads the runtime started: shards and compaction threads.
    pub threads: usize,
    /// Threads pinned to a CPU of their own.
    pub pinned: usize,
    /// Why threads the runtime asked to pin were planned no CPU: it pinned none, or, with
    /// too few free CPUs for every thread, only the shards (D252).
    pub unpinned: Option<pigeonhole_io::sys::Unpinned>,
    /// Threads the OS refused to pin; they run unpinned.
    pub failed: usize,
}

/// Spawns a named thread that pins itself to CPU id `cpu` if given, reports whether it is
/// pinned (a refused pin leaves it unpinned, never stops it), then runs `f`.
fn spawn_pinned<T: Send + 'static>(
    name: String,
    cpu: Option<usize>,
    f: impl FnOnce() -> T + Send + 'static,
) -> Result<(JoinHandle<T>, Receiver<bool>)> {
    let (ack_tx, ack_rx) = mpsc::channel();
    let handle = thread::Builder::new()
        .name(name)
        .spawn(move || {
            let pinned = cpu.is_some_and(|c| pigeonhole_io::sys::pin_current_thread_to(c).is_ok());
            let _ = ack_tx.send(pinned);
            f()
        })
        .map_err(Error::Spawn)?;
    Ok((handle, ack_rx))
}

/// Engine-owned mode: the runtime's own pinned shard threads.
///
/// See the [crate example](crate#example). Dropping a `Runtime` shuts it down like
/// [`Runtime::shutdown`], discarding the handlers.
pub struct Runtime<H: ShardHandler> {
    submitters: Arc<[Submitter<H::Msg>]>,
    shards: Vec<JoinHandle<H>>,
    pool: Vec<(Arc<PoolShared>, JoinHandle<()>)>,
    /// The CPUs this runtime's threads hold, released once they are joined (D235).
    plan: Option<pigeonhole_io::sys::PinPlan>,
    pinning: Pinning,
}

impl<H: ShardHandler> fmt::Debug for Runtime<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("shards", &self.submitters.len())
            .field("compaction_threads", &self.pool.len())
            .finish_non_exhaustive()
    }
}

impl<H: ShardHandler> Runtime<H> {
    /// Spawns one thread per handler (`handlers.len()` must equal `config.shards`).
    ///
    /// With `config.pin_threads`, each shard and compaction thread gets a CPU of its own from
    /// the opening thread's affinity set, one per physical core first and then their SMT
    /// siblings (D220, D235), skipping CPUs other runtimes in this process hold
    /// ([`pigeonhole_io::sys::pin_plan`]). Where that plan refuses (no hard affinity on the
    /// platform, a CPU quota narrower than the affinity set, too few free CPUs) or the OS
    /// refuses a pin, threads run unpinned: pinning never fails the start. With too few free
    /// CPUs for the compaction threads but enough for the shards, only the compaction
    /// threads run unpinned (D252).
    /// [`pinning`](Runtime::pinning) says what happened.
    ///
    /// # Panics
    /// If `handlers.len() != config.shards` or the count is not in `1..=65536`.
    pub fn start(config: RuntimeConfig, handlers: Vec<H>) -> Result<Self> {
        let slice = config.slice_nanos();
        let mut acks = Vec::new();
        let mut pool = Vec::with_capacity(config.compaction_threads);
        let mut pool_threads = Vec::with_capacity(config.compaction_threads);
        let mut pool_rxs = Vec::with_capacity(config.compaction_threads);
        for _ in 0..config.compaction_threads {
            let (shared, rx) = PoolShared::new();
            pool.push(shared);
            pool_rxs.push(rx);
        }
        let pool: Arc<[Arc<PoolShared>]> = pool.into();
        let (submitters, cores) = build(&config, handlers, Arc::clone(&pool));
        let threads = config.shards + config.compaction_threads;
        let (plan, unpinned) = if config.pin_threads {
            match pigeonhole_io::sys::pin_plan(threads) {
                Ok(plan) => (Some(plan), None),
                // Too few CPUs for the compaction threads too: pin the shards, which carry
                // the commits, and leave the compaction threads unpinned (D252).
                Err(why @ pigeonhole_io::sys::Unpinned::TooFewCpus)
                    if config.compaction_threads > 0 =>
                {
                    (pigeonhole_io::sys::pin_plan(config.shards).ok(), Some(why))
                }
                Err(why) => (None, Some(why)),
            }
        } else {
            (None, None)
        };
        let cpu = |k: usize| plan.as_ref().and_then(|p| p.cpu(k));
        let mut rt = Runtime {
            submitters,
            shards: Vec::with_capacity(cores.len()),
            pool: Vec::new(),
            plan: None,
            pinning: Pinning {
                threads,
                unpinned,
                ..Pinning::default()
            },
        };
        for (j, rx) in pool_rxs.into_iter().enumerate() {
            let shared = Arc::clone(&pool[j]);
            let vfs = Arc::clone(&config.vfs);
            let (handle, ack) = spawn_pinned(
                format!("pigeonhole-compaction-{j}"),
                cpu(config.shards + j),
                {
                    let shared = Arc::clone(&shared);
                    move || sched::pool_main(shared, rx, vfs, slice)
                },
            )?;
            pool_threads.push((shared, handle));
            acks.push(ack);
        }
        rt.pool = pool_threads;
        for (i, core) in cores.into_iter().enumerate() {
            let (handle, ack) = spawn_pinned(format!("pigeonhole-shard-{i}"), cpu(i), move || {
                shard_main(core)
            })?;
            rt.shards.push(handle);
            acks.push(ack);
        }
        let planned = plan.as_ref().map_or(0, pigeonhole_io::sys::PinPlan::len);
        // A thread that died before reporting surfaces its panic at shutdown.
        let pinned = acks.into_iter().filter(|a| a.recv() == Ok(true)).count();
        rt.pinning.pinned = pinned;
        rt.pinning.failed = planned.saturating_sub(pinned);
        rt.plan = plan;
        Ok(rt)
    }

    /// Application-owned mode: no threads; one driver per shard.
    ///
    /// The application owns every thread and background tasks run on the shards, so
    /// `config.pin_threads` is ignored and a nonzero `config.compaction_threads` fails with
    /// [`Error::InvalidConfig`] (decision D40) rather than being silently dropped.
    ///
    /// # Panics
    /// If `handlers.len() != config.shards` or the count is not in `1..=65536`.
    pub fn application_owned(
        config: RuntimeConfig,
        handlers: Vec<H>,
    ) -> Result<Vec<ShardDriver<H>>> {
        if config.compaction_threads != 0 {
            return Err(Error::InvalidConfig(
                "compaction_threads must be 0 in application-owned mode",
            ));
        }
        let (_, cores) = build(&config, handlers, Arc::from(Vec::new()));
        Ok(cores
            .into_iter()
            .map(|core| ShardDriver {
                _close: CloseOnDrop(core.submitters[usize::from(core.id.0)].clone()),
                core,
                wakes: Wakes::default(),
            })
            .collect())
    }

    /// Submitter for `shard`.
    ///
    /// # Panics
    /// If `shard` is out of range.
    pub fn submitter(&self, shard: ShardId) -> Submitter<H::Msg> {
        self.submitters[usize::from(shard.0)].clone()
    }

    /// Number of shards.
    pub fn shard_count(&self) -> usize {
        self.submitters.len()
    }

    /// Whether the calling thread is a shard thread, and which (inline-write fast path).
    ///
    /// True on engine-owned shard threads, and on an application thread while it is inside
    /// [`ShardDriver::run_once`] or [`ShardDriver::with_handler`].
    pub fn current_shard() -> Option<ShardId> {
        CURRENT_SHARD.with(Cell::get)
    }

    /// Closes every queue, lets each loop finish its queued messages, and joins the threads.
    ///
    /// Returns the handlers in shard order. Unfinished background tasks are dropped. A
    /// message a shard sends to an already-closed shard during shutdown fails with
    /// [`Error::Closed`]. If a shard thread panicked, the panic resumes here.
    pub fn shutdown(mut self) -> Result<Vec<H>> {
        self.stop()
    }

    /// How this runtime's threads were pinned (D235): how many it started, how many got a
    /// CPU of their own, and why none did when pinning was asked for and refused.
    pub fn pinning(&self) -> Pinning {
        self.pinning
    }

    fn stop(&mut self) -> Result<Vec<H>> {
        for s in self.submitters.iter() {
            s.inbox.close();
        }
        let mut handlers = Vec::with_capacity(self.shards.len());
        let mut panic = None;
        for t in self.shards.drain(..) {
            match t.join() {
                Ok(h) => handlers.push(h),
                Err(p) => panic = panic.or(Some(p)),
            }
        }
        // Shards are gone, so nothing spawns onto the pool any more.
        for (shared, _) in &self.pool {
            shared.close();
        }
        for (_, t) in self.pool.drain(..) {
            if let Err(p) = t.join() {
                panic = panic.or(Some(p));
            }
        }
        // Every thread is gone: its CPU is free for another runtime in this process.
        self.plan = None;
        if let Some(p) = panic {
            std::panic::resume_unwind(p);
        }
        Ok(handlers)
    }
}

impl<H: ShardHandler> Drop for Runtime<H> {
    fn drop(&mut self) {
        if !self.shards.is_empty() || !self.pool.is_empty() {
            if thread::panicking() {
                // Close without joining: never double-panic.
                for s in self.submitters.iter() {
                    s.inbox.close();
                }
                for (shared, _) in &self.pool {
                    shared.close();
                }
            } else {
                let _ = self.stop();
            }
        }
    }
}

/// Application-owned mode: drives one shard from a thread the application owns.
///
/// Call [`run_once`](ShardDriver::run_once) until it returns `false`, then wait for the
/// wakeup registered with [`set_wakeup`](ShardDriver::set_wakeup) (which fires once per
/// idle period, when work arrives) before calling it again.
///
/// [`ShardDriver::shutdown`] closes the shard, handles every queued message and returns the
/// handler, like [`Runtime::shutdown`]. Dropping the driver instead closes the shard and
/// **discards** what is queued: queued messages are dropped unhandled (their [`Notifier`]s
/// resolve waiters with `None`) and background tasks are dropped. Either way, later submits
/// fail with [`Error::Closed`].
///
/// ```
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_runtime::{Runtime, RuntimeConfig, ShardContext, ShardHandler, ShardId};
///
/// struct Log(Vec<u32>);
///
/// impl ShardHandler for Log {
///     type Msg = u32;
///     fn handle(&mut self, _ctx: &mut ShardContext<'_, u32>, msg: u32) {
///         self.0.push(msg);
///     }
///     fn end_batch(&mut self, _ctx: &mut ShardContext<'_, u32>) {}
/// }
///
/// let mut config = RuntimeConfig::new(SimVfs::new(7));
/// config.shards = 1;
/// let mut drivers = Runtime::application_owned(config, vec![Log(Vec::new())])?;
/// let shard = &mut drivers[0];
/// shard.submitter(ShardId(0)).submit(1)?;
/// shard.submitter(ShardId(0)).submit(2)?;
/// assert!(!shard.run_once(u64::MAX)); // drained; no work remains
/// // The application's own write, inline on its own core:
/// shard.with_handler(|log, _ctx| log.0.push(3));
/// assert_eq!(shard.with_handler(|log, _| log.0.clone()), [1, 2, 3]);
/// # Ok::<(), pigeonhole_runtime::Error>(())
/// ```
pub struct ShardDriver<H: ShardHandler> {
    core: ShardCore<H>,
    _close: CloseOnDrop<H::Msg>,
    /// Who a wake reaches: the application's callback ([`ShardDriver::set_wakeup`]) and the
    /// thread in [`ShardDriver::wait`].
    wakes: Wakes,
}

/// What a wake of an application-owned shard calls, composed into one wake target.
#[derive(Default)]
struct Wakes {
    callback: Option<Arc<dyn Fn() + Send + Sync>>,
    /// The thread that last waited, and its ring's waker if it has a ring.
    waiter: Option<(thread::Thread, Option<pigeonhole_io::OwnIoWaker>)>,
}

impl Wakes {
    /// The wake target: the callback, then the waiting thread (its ring's waker and an
    /// unpark: it waits in one or the other). Without a callback, the waiting thread is the
    /// engine-owned thread's own target, so a commit's wake costs no closure call.
    fn target(&self) -> WakeTarget {
        let callback = self.callback.clone();
        let waiter = self.waiter.clone();
        match (callback, waiter) {
            (None, None) => WakeTarget::None,
            (Some(cb), None) => WakeTarget::Callback(cb),
            (None, Some((thread, None))) => WakeTarget::Thread(thread),
            (None, Some((thread, Some(ring)))) => WakeTarget::ThreadRing(thread, ring),
            (Some(cb), Some((thread, ring))) => WakeTarget::Callback(Arc::new(move || {
                cb();
                if let Some(ring) = &ring {
                    ring.wake();
                }
                thread.unpark();
            })),
        }
    }
}

/// Ends a [`ShardDriver::wait`] from any thread ([`ShardDriver::waker`]).
struct WaitWaker(Arc<Signal>);

impl std::task::Wake for WaitWaker {
    fn wake(self: Arc<Self>) {
        self.0.notify_app();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.notify_app();
    }
}

/// Refuses further submits once the driver is gone (shut down or dropped).
struct CloseOnDrop<M>(Submitter<M>);

impl<M> Drop for CloseOnDrop<M> {
    fn drop(&mut self) {
        self.0.inbox.gate.close();
    }
}

impl<H: ShardHandler> fmt::Debug for ShardDriver<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardDriver")
            .field("shard", &self.core.id)
            .field("tasks", &self.core.spawner.local)
            .finish_non_exhaustive()
    }
}

impl<H: ShardHandler> ShardDriver<H> {
    /// This shard.
    pub fn shard(&self) -> ShardId {
        self.core.id
    }

    /// Submitter for any shard.
    ///
    /// # Panics
    /// If `shard` is out of range.
    pub fn submitter(&self, shard: ShardId) -> Submitter<H::Msg> {
        self.core.submitters[usize::from(shard.0)].clone()
    }

    /// Drains queued messages, calls `end_batch`, then runs background tasks until
    /// `deadline_nanos`. Returns whether work remains.
    ///
    /// Between task slices (each at most `time_slice` long) it drains the queue again, so
    /// foreground messages never wait behind more than one slice.
    ///
    /// When a [`waker`](Self::waker) wake arrived since the last turn (typically a message
    /// this turn handled settled a future the calling thread awaits), it returns `true` right
    /// after the drain, before the background tasks, so the caller can poll that future now
    /// rather than at the deadline (D237). The tasks run on the next call; after eight such
    /// turns in a row, a turn runs them anyway. The wake still ends the next
    /// [`wait`](Self::wait) at once.
    ///
    /// Tasks sleeping until a deadline ([`TaskPoll::SleepUntil`]) are not work that remains:
    /// with only those left it returns `false`, and the application should call it again
    /// by [`next_deadline`](ShardDriver::next_deadline) even if no wakeup arrives.
    pub fn run_once(&mut self, deadline_nanos: u64) -> bool {
        self.core.run_once(deadline_nanos)
    }

    /// Before handing this shard to another thread: drives it until the calling thread's
    /// I/O ring has nothing in flight (#492, ICR 0029). A backend with a ring per driving
    /// thread (io_uring in application-owned mode) lets only that thread complete what was
    /// submitted to it, so a shard moved with I/O still there would wait on it for ever if
    /// this thread stopped running turns. Returns at once without such rings (the pread
    /// and synchronous backends), or on a thread that has not run this shard.
    pub fn release(&mut self) {
        self.core.release();
    }

    /// The earliest deadline (VFS `monotonic_nanos`) of a background task sleeping on this
    /// shard, or `None`. After [`run_once`](ShardDriver::run_once) returns `false`, the
    /// application sleeps until the wakeup registered with
    /// [`set_wakeup`](ShardDriver::set_wakeup) fires or this deadline passes, whichever
    /// comes first.
    ///
    /// ```
    /// use pigeonhole_io::Vfs;
    /// use pigeonhole_io::sim::SimVfs;
    /// use pigeonhole_runtime::{
    ///     Runtime, RuntimeConfig, ShardContext, ShardHandler, Task, TaskPoll, TaskWaker,
    /// };
    ///
    /// /// Fires once the clock reaches `at`.
    /// struct Alarm {
    ///     vfs: SimVfsRef,
    ///     at: u64,
    /// }
    /// type SimVfsRef = std::sync::Arc<SimVfs>;
    ///
    /// impl Task for Alarm {
    ///     fn run(&mut self, _deadline: u64, _waker: &TaskWaker) -> TaskPoll {
    ///         if self.vfs.monotonic_nanos() >= self.at {
    ///             TaskPoll::Done
    ///         } else {
    ///             TaskPoll::SleepUntil(self.at)
    ///         }
    ///     }
    ///     fn name(&self) -> &'static str {
    ///         "alarm"
    ///     }
    /// }
    ///
    /// struct Idle;
    /// impl ShardHandler for Idle {
    ///     type Msg = ();
    ///     fn handle(&mut self, _ctx: &mut ShardContext<'_, ()>, _msg: ()) {}
    ///     fn end_batch(&mut self, _ctx: &mut ShardContext<'_, ()>) {}
    /// }
    ///
    /// let vfs = SimVfs::new(1);
    /// let mut config = RuntimeConfig::new(vfs.clone());
    /// config.shards = 1;
    /// let mut drivers = Runtime::application_owned(config, vec![Idle])?;
    /// let shard = &mut drivers[0];
    /// let at = vfs.monotonic_nanos() + 5_000;
    /// shard.with_handler(|_, ctx| ctx.spawn(Box::new(Alarm { vfs: vfs.clone(), at })));
    /// assert!(!shard.run_once(u64::MAX)); // only a sleeping task: idle
    /// assert_eq!(shard.next_deadline(), Some(at));
    /// vfs.advance(5_000);
    /// assert!(!shard.run_once(u64::MAX)); // due: it ran and finished
    /// assert_eq!(shard.next_deadline(), None);
    /// # Ok::<(), pigeonhole_runtime::Error>(())
    /// ```
    pub fn next_deadline(&self) -> Option<u64> {
        self.core.next_deadline()
    }

    /// Runs `f` on this shard's handler inline (the application's own writes on its own
    /// core, without a queue hop).
    pub fn with_handler<R>(
        &mut self,
        f: impl FnOnce(&mut H, &mut ShardContext<'_, H::Msg>) -> R,
    ) -> R {
        let _shard = EnterShard::new(self.core.id);
        let (handler, mut ctx) = self.core.parts();
        f(handler, &mut ctx)
    }

    /// Registers a waker the runtime calls when work arrives (for the application's event
    /// loop).
    ///
    /// It is called from the submitting thread, at most once per idle period (after
    /// [`run_once`](ShardDriver::run_once) returned `false`), so it should be cheap: set a
    /// flag, unpark a thread, or write an eventfd. It is also called once right away, since
    /// work may have arrived before it was registered.
    pub fn set_wakeup(&mut self, wake: Box<dyn Fn() + Send + Sync>) {
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::from(wake);
        self.wakes.callback = Some(Arc::clone(&wake));
        self.core.signal().set_target(self.wakes.target());
        // A submit that found the shard idle before the target existed woke nobody; a
        // spurious wake is harmless, a lost one is not.
        wake();
    }

    /// Waits until this shard has work again: a message arrives, a [`waker`](Self::waker)
    /// fires, the calling thread's own I/O completes, the next background deadline
    /// ([`next_deadline`](Self::next_deadline)) passes, or `timeout` passes (#553, ICR
    /// 0035). Call it after [`run_once`](Self::run_once) returns `false`, on the thread
    /// that runs the shard; a spurious return is possible, so loop on `run_once`. It needs no
    /// completion fd: with io_uring it waits in the thread's ring while its own I/O is in
    /// flight, and parks otherwise. It composes with [`set_wakeup`](Self::set_wakeup), whose
    /// callback still fires. A thread that drives several shards from one loop waits in its
    /// own event loop instead (the wakeup callbacks and the I/O completion fd).
    pub fn wait(&mut self, timeout: Duration) {
        let me = thread::current();
        if self.wakes.waiter.as_ref().map(|(t, _)| t.id()) != Some(me.id()) {
            self.wakes.waiter = Some((me, pigeonhole_io::own_io_waker()));
            self.core.signal().set_target(self.wakes.target());
        }
        self.core.wait(timeout);
    }

    /// A waker that ends this shard's [`wait`](Self::wait) from any thread: for futures the
    /// driving thread polls itself (a commit's future, resolved on another shard's thread
    /// or by this shard during [`run_once`](Self::run_once)). Waking it when nobody waits
    /// only makes the next `wait` return at once, even if a turn ran in between (#592).
    pub fn waker(&self) -> std::task::Waker {
        std::task::Waker::from(Arc::new(WaitWaker(Arc::clone(&self.core.inbox().signal))))
    }

    /// Closes the shard, waits for in-flight submits, handles every queued message (calling
    /// `end_batch` per batch) and returns the handler. Unfinished background tasks are
    /// dropped. Later submits fail with [`Error::Closed`].
    pub fn shutdown(self) -> H {
        let ShardDriver {
            mut core, _close, ..
        } = self;
        let _shard = EnterShard::new(core.id);
        core.inbox().gate.close();
        core.finish();
        core.handler
    }
}
