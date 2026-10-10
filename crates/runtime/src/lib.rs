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

use std::cell::Cell;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use pigeonhole_io::{ErrorKind, Vfs, VfsRef};

pub use completion::{Notifier, Waiter, completion};
pub use sched::{Task, TaskPoll, TaskWaker};

use sched::{PoolShared, Scheduler, Spawner};
use signal::{Signal, WakeTarget};

/// Most messages handled before `end_batch` runs, so a flooded queue still commits groups.
const MAX_BATCH: usize = 1024;

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
    /// Number of shards (default: available CPUs).
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
    /// Defaults: one shard per available CPU, pinned, no compaction threads, 500 µs slices.
    pub fn new(vfs: VfsRef) -> Self {
        Self {
            shards: pigeonhole_io::sys::available_cpus().max(1),
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
    closed: AtomicBool,
    /// Submits between their closed check and their send; shutdown waits for zero.
    inflight: AtomicUsize,
    signal: Arc<Signal>,
}

impl<M> Inbox<M> {
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.signal.force_wake();
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
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

    /// Enqueues `msg`. Fails only after shutdown.
    pub fn submit(&self, msg: M) -> Result<()> {
        let inbox = &*self.inbox;
        // `SeqCst` on both sides: either shutdown sees our increment and waits for the send,
        // or we see `closed` and refuse.
        inbox.inflight.fetch_add(1, Ordering::SeqCst);
        if inbox.closed.load(Ordering::SeqCst) {
            inbox.inflight.fetch_sub(1, Ordering::Release);
            return Err(Error::Closed);
        }
        let sent = inbox.tx.send(msg);
        inbox.inflight.fetch_sub(1, Ordering::Release);
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
    /// Polling the queue before parking (D198; engine-owned mode only).
    spin: IdleSpin,
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
        let _shard = EnterShard::new(self.id);
        self.signal().awake();
        // A backend with a ring per driving thread (#402) gives each thread that runs this
        // shard its own (a shard moved to another thread needs one there, #473); what
        // completed on it resolves now, so the work it unblocks runs this turn. Without rings
        // nothing depends on the thread: a turn pays the two branches it always did, and only
        // a ring backend reads the thread token (#485's instruction ceilings).
        if self.own_ring {
            if self.attached != thread_token() {
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
        while self.spawner.local.has_runnable() {
            let now = self.vfs.monotonic_nanos();
            if now >= deadline {
                break;
            }
            self.spawner
                .local
                .run_one(now.saturating_add(self.time_slice).min(deadline));
            // Foreground first: whatever queued during the slice runs before the next one.
            self.drain();
            self.spawner.local.collect_woken();
            self.spawner.local.wake_due(self.vfs.monotonic_nanos());
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

    /// Shutdown: wait out in-flight submits, then handle everything queued.
    fn finish(&mut self) {
        while self.inbox().inflight.load(Ordering::SeqCst) != 0 {
            thread::yield_now();
        }
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
        let deadline = core.vfs.monotonic_nanos().saturating_add(core.time_slice);
        let more = core.run_once(deadline);
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
                    closed: AtomicBool::new(false),
                    inflight: AtomicUsize::new(0),
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
            attached: 0,
            own_ring: false,
            spin: IdleSpin::new(config.idle_spin),
        })
        .collect();
    (submitters, cores)
}

/// Pins the calling thread to `cpu` (modulo the available CPUs). Platforms without affinity
/// control (macOS) are best-effort: `Unsupported` is not an error.
fn pin(cpu: usize) -> std::io::Result<()> {
    let cpus = pigeonhole_io::sys::available_cpus().max(1);
    match pigeonhole_io::sys::pin_current_thread(cpu % cpus) {
        Ok(()) => Ok(()),
        Err(e) if e.kind == ErrorKind::Unsupported => Ok(()),
        Err(e) => Err(std::io::Error::other(e)),
    }
}

/// Spawns a named thread that optionally pins itself, reports the pin result, then runs `f`.
fn spawn_pinned<T: Send + 'static>(
    name: String,
    cpu: Option<usize>,
    f: impl FnOnce() -> T + Send + 'static,
) -> Result<(JoinHandle<T>, Receiver<std::io::Result<()>>)> {
    let (ack_tx, ack_rx) = mpsc::channel();
    let handle = thread::Builder::new()
        .name(name)
        .spawn(move || {
            let _ = ack_tx.send(cpu.map_or(Ok(()), pin));
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
    /// Shard `i` is pinned to CPU `i` and compaction thread `j` to CPU `shards + j` (modulo
    /// the available CPUs) when `config.pin_threads` is set; pinning is best-effort where the
    /// OS has no affinity control (macOS).
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
        let mut rt = Runtime {
            submitters,
            shards: Vec::with_capacity(cores.len()),
            pool: Vec::new(),
        };
        for (j, rx) in pool_rxs.into_iter().enumerate() {
            let shared = Arc::clone(&pool[j]);
            let vfs = Arc::clone(&config.vfs);
            let cpu = config.pin_threads.then_some(config.shards + j);
            let (handle, ack) = spawn_pinned(format!("pigeonhole-compaction-{j}"), cpu, {
                let shared = Arc::clone(&shared);
                move || sched::pool_main(shared, rx, vfs, slice)
            })?;
            pool_threads.push((shared, handle));
            acks.push(ack);
        }
        rt.pool = pool_threads;
        for (i, core) in cores.into_iter().enumerate() {
            let cpu = config.pin_threads.then_some(i);
            let (handle, ack) = spawn_pinned(format!("pigeonhole-shard-{i}"), cpu, move || {
                shard_main(core)
            })?;
            rt.shards.push(handle);
            acks.push(ack);
        }
        for ack in acks {
            // A thread that died before reporting will surface its panic at shutdown.
            if let Ok(Err(e)) = ack.recv() {
                return Err(Error::Spawn(e));
            }
        }
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
}

/// Refuses further submits once the driver is gone (shut down or dropped).
struct CloseOnDrop<M>(Submitter<M>);

impl<M> Drop for CloseOnDrop<M> {
    fn drop(&mut self) {
        self.0.inbox.closed.store(true, Ordering::SeqCst);
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
    /// Tasks sleeping until a deadline ([`TaskPoll::SleepUntil`]) are not work that remains:
    /// with only those left it returns `false`, and the application should call it again
    /// by [`next_deadline`](ShardDriver::next_deadline) even if no wakeup arrives.
    pub fn run_once(&mut self, deadline_nanos: u64) -> bool {
        self.core.run_once(deadline_nanos)
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
        self.core
            .signal()
            .set_target(WakeTarget::Callback(Arc::clone(&wake)));
        // A submit that found the shard idle before the target existed woke nobody; a
        // spurious wake is harmless, a lost one is not.
        wake();
    }

    /// Closes the shard, waits for in-flight submits, handles every queued message (calling
    /// `end_batch` per batch) and returns the handler. Unfinished background tasks are
    /// dropped. Later submits fail with [`Error::Closed`].
    pub fn shutdown(self) -> H {
        let ShardDriver { mut core, _close } = self;
        let _shard = EnterShard::new(core.id);
        core.inbox().closed.store(true, Ordering::SeqCst);
        core.finish();
        core.handler
    }
}
