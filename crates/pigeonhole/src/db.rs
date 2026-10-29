use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use arc_swap::ArcSwap;
use pigeonhole_engine::{Engine, EngineShard};

use crate::Durability;
use crate::Failure;
use pigeonhole_io::VfsRef;

use crate::table::TableCore;
use crate::{
    Error, ErrorCode, IoRings, Options, ReadTable, ReaderOptions, Recovery, RecoveryStats, Result,
    TableBuilder, Transaction, WriteBatch,
};

/// What every handle derived from one open database shares: the engine, whether
/// [`Pigeonhole::close`] ran, and the largest value a commit may carry.
///
/// The engine can be replaced (D265: automatic recovery reopens the file and swaps the new
/// engine in behind the handles). Every operation loads the current one through
/// [`Db::engine`]; what was taken from an earlier engine (a snapshot, a transaction, an
/// iterator) carries the [`generation`](Db::generation) it was taken in and expires with
/// `SnapshotExpired` once the generation moved on.
#[derive(Debug)]
pub(crate) struct Db {
    engine: ArcSwap<Engine>,
    /// Opens of the engine behind this handle: 1 at open, +1 per swap, with [`CLOSED`] set
    /// once the database was closed through any handle. One word, so a scan step checks
    /// both with one load ([`Db::check_generation`]): #827's per-row `Closed` check and
    /// D265's generation check together cost what either does alone.
    generation: AtomicU64,
    /// The I/O backend the engine runs on (for [`Pigeonhole::io_rings`]).
    vfs: VfsRef,
    /// Largest value accepted at commit (decision D16), for error messages; 0 for readers.
    pub(crate) max_value: usize,
    /// Automatic recovery (D265): the supervisor's state and thread, with
    /// `Options::auto_recover` on; `None` otherwise, and for readers.
    supervision: Option<crate::recovery::Supervision>,
    /// What a reopen needs: the options and the path (writers with the option on).
    reopen: Option<(Options, PathBuf)>,
    /// `Options::write_stall_timeout`: how long an operation waits for a recovery's swap.
    stall_timeout: Duration,
    /// Transaction outcomes since open (ICR 0061).
    pub(crate) txn_counts: crate::retry::TxnCounts,
    /// With the option on, this database's entry in the watch registry ([`watched`]; 0
    /// otherwise): how a commit that resolves later finds it again on its error path only
    /// (#966).
    watch_id: u64,
}

/// The supervised databases of one process, by watch id: a commit that resolves after its
/// call returned (an async commit, a ticket) keeps only the id and looks its database up
/// here if the failed engine refused it, so its success path takes no reference (perf287's
/// P1 on #968).
struct Watched {
    /// The process it belongs to ([`pigeonhole_io::sys::process_id`]).
    pid: u32,
    list: std::sync::Mutex<Vec<(u64, std::sync::Weak<Db>)>>,
}

static WATCHED: arc_swap::ArcSwapOption<Watched> = arc_swap::ArcSwapOption::const_empty();
static NEXT_WATCH_ID: AtomicU64 = AtomicU64::new(1);

/// This process's watch registry. A forked child gets a fresh one: the parent's lock may
/// have been copied held into it (perf287's S3 on #968; ICR 0067's forked-child rule), and
/// none of the parent's databases is the child's to watch. Taken only at a supervised open,
/// a supervised database's drop, and a refusal's error path.
fn watched() -> Arc<Watched> {
    let pid = pigeonhole_io::sys::process_id();
    WATCHED.rcu(|cur| match cur {
        Some(w) if w.pid == pid => Some(Arc::clone(w)),
        _ => Some(Arc::new(Watched {
            pid,
            list: std::sync::Mutex::new(Vec::new()),
        })),
    });
    WATCHED.load_full().expect("installed just now")
}

/// Runs `f` while another thread holds this process's watch registry locked (the fork test's
/// seam: a fork inside `f` copies the lock held, by a thread the child does not have, into
/// the child; perf287's S3 on #968).
#[cfg(feature = "__unstable-hooks")]
pub(crate) fn with_watch_registry_locked_elsewhere<R>(f: impl FnOnce() -> R) -> R {
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        let registry = watched();
        let _held = registry
            .list
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = locked_tx.send(());
        let _ = done_rx.recv();
    });
    let _ = locked_rx.recv();
    let pid = pigeonhole_io::sys::process_id();
    let r = f();
    if pigeonhole_io::sys::process_id() != pid {
        // A forked child: the holder is not in this process.
        std::mem::forget((holder, done_tx));
        return r;
    }
    let _ = done_tx.send(());
    let _ = holder.join();
    r
}

impl Drop for Db {
    /// Leaves the watch registry (perf287's N1 on #968).
    fn drop(&mut self) {
        if self.watch_id == 0 {
            return;
        }
        let registry = watched();
        registry
            .list
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|(id, d)| *id != self.watch_id && d.strong_count() > 0);
    }
}

/// What a commit that resolves later needs to map a refusal by the failed engine
/// ([`Db::refused_later`]): its database's watch id (0 without the option) and the
/// generation it was submitted under. Copied, never counted.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Watch {
    id: u64,
    generation: u64,
    transaction: bool,
}

impl Watch {
    /// For a transaction's commit: `SnapshotExpired` once the swap is in, as its sync
    /// commit gets.
    pub(crate) fn transaction(mut self) -> Self {
        self.transaction = true;
        self
    }

    /// `e`, or what the sync path reports for a refusal by the failed engine of the watched
    /// database (#966). Only a refusal looks the database up: one the failed shard made
    /// before logging, or a `Closed` from the failed engine's close (a slow commit meets it),
    /// as the sync path's `after_commit_closed` takes both.
    pub(crate) fn refused(self, e: Error) -> Error {
        if self.id == 0 || (e.code() != ErrorCode::Closed && !e.refused_unlogged()) {
            return e;
        }
        let db = watched()
            .list
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|(id, _)| *id == self.id)
            .and_then(|(_, w)| w.upgrade());
        match db {
            Some(db) => db.refused_later(e, self),
            None => e,
        }
    }
}

/// The engine an operation runs on, held for the operation: the handle's cached `Arc`
/// (one generation load to validate it), or, after a swap left the handle stale, the
/// current engine loaded from the `ArcSwap` (the slow path).
pub(crate) enum EngineRef<'a> {
    Cached(&'a Arc<Engine>),
    /// The slow path's own strong reference (an `Arc` clone, not an `arc_swap` guard, so
    /// the fast path's drop glue is a discriminant check and nothing else).
    Loaded(Arc<Engine>),
}

impl std::ops::Deref for EngineRef<'_> {
    type Target = Engine;
    #[inline(always)]
    fn deref(&self) -> &Engine {
        match self {
            EngineRef::Cached(e) => e,
            EngineRef::Loaded(e) => e,
        }
    }
}

impl EngineRef<'_> {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn to_arc(&self) -> Arc<Engine> {
        match self {
            EngineRef::Cached(e) => Arc::clone(e),
            EngineRef::Loaded(e) => Arc::clone(e),
        }
    }
}

/// A handle's cached engine and the generation it was taken in (D265). Valid while the
/// database's generation is still that one: the check is one atomic load. A handle made
/// before a swap keeps the old engine alive (a light shell once its close released its
/// memory) and takes the slow path on every operation until a new handle is opened.
#[derive(Debug, Clone)]
pub(crate) struct Cached {
    engine: Arc<Engine>,
    generation: u64,
}

impl Cached {
    pub(crate) fn new(db: &Db) -> Self {
        // The generation first: a swap in between leaves a cache that fails its check.
        let generation = db.generation();
        let engine = Arc::clone(&db.engine.load());
        Self { engine, generation }
    }

    /// The engine for one operation.
    #[inline(always)]
    pub(crate) fn get<'a>(&'a self, db: &'a Db) -> EngineRef<'a> {
        if db.generation() == self.generation {
            EngineRef::Cached(&self.engine)
        } else {
            Self::stale(db)
        }
    }

    /// The slow path, out of line and cold: inlined into every operation it made the fast
    /// function large enough to spoil the codegen of the engine calls inlined after it
    /// (about 30 instructions on a get, measured).
    #[cold]
    #[inline(never)]
    fn stale(db: &Db) -> EngineRef<'_> {
        db.engine()
    }

    /// Runs `f` on the engine for one operation without materializing a guard: the fast
    /// path is two loads, a compare and the call (a materialized `EngineRef`, constructed,
    /// dereferenced and drop-checked, measured about 25 instructions more per operation).
    #[inline(always)]
    pub(crate) fn with<R>(&self, db: &Db, f: impl FnOnce(&Engine) -> R) -> R {
        if db.generation() == self.generation {
            f(&self.engine)
        } else {
            Self::stale_with(db, f)
        }
    }

    #[cold]
    #[inline(never)]
    fn stale_with<R>(db: &Db, f: impl FnOnce(&Engine) -> R) -> R {
        f(&db.engine())
    }

    /// A cache for a handle derived from this one: this one while it is current.
    pub(crate) fn fresh(&self, db: &Db) -> Self {
        if db.generation() == self.generation {
            self.clone()
        } else {
            Self::new(db)
        }
    }
}

/// The bit of [`Db::generation`] that says the database was closed (`Pigeonhole::close`).
const CLOSED: u64 = 1 << 63;

impl Db {
    fn new(engine: Arc<Engine>, vfs: VfsRef, max_value: usize) -> Arc<Self> {
        Self::with_recovery(engine, vfs, max_value, None, Duration::from_secs(30))
    }

    /// `new`, supervised when `reopen` names the options and path to recover with and the
    /// options ask for it.
    fn with_recovery(
        engine: Arc<Engine>,
        vfs: VfsRef,
        max_value: usize,
        reopen: Option<(Options, PathBuf)>,
        stall_timeout: Duration,
    ) -> Arc<Self> {
        let supervision = reopen
            .as_ref()
            .map(|(o, _)| o.recovery())
            .filter(|r| *r != Recovery::Off)
            .map(crate::recovery::Supervision::new);
        let watch_id = if supervision.is_some() {
            NEXT_WATCH_ID.fetch_add(1, Ordering::Relaxed)
        } else {
            0
        };
        let db = Arc::new(Self {
            engine: ArcSwap::from(engine),
            generation: AtomicU64::new(1),
            vfs,
            max_value,
            supervision,
            reopen,
            stall_timeout,
            txn_counts: Default::default(),
            watch_id,
        });
        if watch_id != 0 {
            let registry = watched();
            let mut w = registry
                .list
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            w.retain(|(_, d)| d.strong_count() > 0);
            w.push((watch_id, Arc::downgrade(&db)));
        }
        db
    }

    /// What a commit submitted now needs to map a refusal by the failed engine when it
    /// resolves later (#966): nothing without the option.
    ///
    /// Take it before choosing the engine the commit runs on (perf287's S2 on #968): taken
    /// after, a swap in between records the new generation, and a refusal by the failed
    /// engine then looks as if nothing swapped (`Closed` instead of `Busy`).
    #[inline]
    pub(crate) fn watch(&self) -> Watch {
        self.watch_at(self.generation())
    }

    /// [`Db::watch`] for a commit submitted under `generation` (a transaction's).
    #[inline]
    pub(crate) fn watch_at(&self, generation: u64) -> Watch {
        if self.watch_id == 0 {
            return Watch::default();
        }
        Watch {
            id: self.watch_id,
            generation,
            transaction: false,
        }
    }

    /// The supervisor's state, with the option on.
    pub(crate) fn supervision(&self) -> Option<&crate::recovery::Supervision> {
        self.supervision.as_ref()
    }

    /// The current engine, as an `Arc` (the supervisor's wait holds it).
    pub(crate) fn engine_arc(&self) -> Arc<Engine> {
        Arc::clone(&self.engine.load())
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.generation.load(Ordering::Acquire) & CLOSED != 0
    }

    pub(crate) fn stall_timeout(&self) -> Duration {
        self.stall_timeout
    }

    /// The engine options a recovery reopens with (the handle's own, re-resolved).
    pub(crate) fn reopen_options(&self) -> Result<pigeonhole_engine::EngineOptions> {
        let (options, _) = self.reopen.as_ref().expect("a supervised writer");
        Ok(options.to_engine()?)
    }

    pub(crate) fn path(&self) -> &Path {
        let (_, path) = self.reopen.as_ref().expect("a supervised writer");
        path
    }

    /// Runs `op`, and once more on the current engine if it got `Closed` from an engine a
    /// recovery was replacing (D265): the wait for the swap is bounded by
    /// `Options::write_stall_timeout` (`Busy` past it; `Closed` once recovery gave up or
    /// the handle is closed). Costs nothing on the success path.
    /// Whether a recovery supervisor runs for this database (`Options::auto_recover`): the
    /// hot read paths take `retry_closed` only then, and the engine call as before the
    /// option otherwise. A closure handing the engine's result back costs a move of it
    /// (`CellData` carries up to 128 inline bytes: about 60 instructions per point get on
    /// callgrind, #836 round 6), which the option-off path must not pay.
    #[inline(always)]
    pub(crate) fn supervised(&self) -> bool {
        self.supervision.is_some()
    }

    #[inline(always)]
    pub(crate) fn retry_closed<T>(&self, mut op: impl FnMut() -> Result<T>) -> Result<T> {
        // Option off: the operation as it was, nothing around it (the instructions run on
        // #836 measured the version below with a load and a match around every get at
        // about 120 instructions more per point get).
        if self.supervision.is_none() {
            return op();
        }
        // The generation before the operation (one load): a `Closed` is then judged against
        // the engine the operation ran on, not against whatever is current once the thread
        // runs again (perf287's R3 on #836).
        let generation = self.generation();
        match op() {
            Err(e) if e.code() == ErrorCode::Closed => self.retry_after_closed(generation, op),
            r => r,
        }
    }

    /// The retry, out of line: after the wait, the operation again on the current engine.
    #[cold]
    #[inline(never)]
    fn retry_after_closed<T>(
        &self,
        generation: u64,
        mut op: impl FnMut() -> Result<T>,
    ) -> Result<T> {
        self.after_closed(generation)?;
        op()
    }

    /// An operation got `Closed`: whether to run it again (`Ok`), after waiting for a running
    /// recovery's swap, or to fail (`Closed`, or `Busy` past the wait's bound).
    #[cold]
    #[inline(never)]
    pub(crate) fn after_closed(&self, generation: u64) -> Result<()> {
        let closed = || Err(pigeonhole_engine::Error::Closed.into());
        if self.is_closed() {
            return closed();
        }
        let Some(sup) = &self.supervision else {
            return closed();
        };
        // A recovery either runs (wait for it) or completed since the operation loaded its
        // engine (the failed one, closed meanwhile: run again on the new one). With neither,
        // the `Closed` is the instance's own (a failure the option does not cover, or one
        // the supervisor gave up on).
        match sup.wait_settled(self.stall_timeout, || self.engine_arc().is_failed()) {
            crate::recovery::Settled::Swapped if self.generation() != generation => Ok(()),
            crate::recovery::Settled::Swapped => closed(),
            crate::recovery::Settled::Failed => closed(),
            crate::recovery::Settled::TimedOut => Err(Error::new(
                ErrorCode::Busy,
                "recovering after a shard failure (Options::auto_recover): the reopen did not \
                 finish within Options::write_stall_timeout; not committed, retry",
            )),
        }
    }

    /// Before a commit is submitted: with a recovery running, waits for its swap (bounded),
    /// so the commit runs on the new engine rather than being refused by the closing one.
    /// Returns the generation the commit submits under, for `after_commit_closed`.
    #[inline(always)]
    pub(crate) fn before_commit(&self) -> Result<u64> {
        match &self.supervision {
            // Option off: no load at all (`after_commit_closed` returns early without it).
            None => Ok(0),
            Some(sup) if sup.recovering() => {
                self.wait_for_swap()?;
                Ok(self.generation())
            }
            Some(_) => Ok(self.generation()),
        }
    }

    #[cold]
    #[inline(never)]
    fn wait_for_swap(&self) -> Result<()> {
        let sup = self.supervision.as_ref().expect("checked");
        match sup.wait_settled(self.stall_timeout, || self.engine_arc().is_failed()) {
            crate::recovery::Settled::Swapped => Ok(()),
            crate::recovery::Settled::Failed => Err(pigeonhole_engine::Error::Closed.into()),
            crate::recovery::Settled::TimedOut => Err(Error::new(
                ErrorCode::Busy,
                "recovering after a shard failure (Options::auto_recover): the reopen did not \
                 finish within Options::write_stall_timeout; not committed, retry",
            )),
        }
    }

    /// A commit got `Closed` from the engine, or a failed shard's refusal of a commit it never
    /// logged (`Io`, before the supervisor began closing that engine: #921): with a recovery
    /// running or just completed, the commit was refused by the failing engine before
    /// anything was written, and the batch is gone with it: `Busy` (not committed, retry)
    /// once the swap is in, or the error as it is if the recovery gave up. Without a
    /// recovery, the error as it is.
    #[cold]
    #[inline(never)]
    pub(crate) fn after_commit_closed(&self, generation: u64, e: Error) -> Error {
        self.after_commit_closed_as(generation, e, false)
    }

    /// [`after_commit_closed`](Self::after_commit_closed) for a transaction's commit: refused
    /// by the failed engine before the swap, it gets `SnapshotExpired` rather than `Busy`
    /// (perf287's S1 on #877, ICR 0060): its snapshot expired with the swap, as its reads
    /// would have, and `SnapshotExpired` is what `Pigeonhole::transact` retries by default.
    /// A recovery that times out is still `Busy`.
    #[cold]
    #[inline(never)]
    pub(crate) fn after_transaction_commit_closed(&self, generation: u64, e: Error) -> Error {
        self.after_commit_closed_as(generation, e, true)
    }

    #[cold]
    #[inline(never)]
    fn after_commit_closed_as(&self, generation: u64, e: Error, transaction: bool) -> Error {
        if e.code() != ErrorCode::Closed && !e.refused_unlogged() {
            return e;
        }
        let Some(sup) = &self.supervision else {
            return e;
        };
        // Closed through any handle: `Closed`, also for a refusal the failed shard made
        // unlogged, as an async commit or a ticket gets it (`refused_later`; #970, N1).
        if self.is_closed() {
            return pigeonhole_engine::Error::Closed.into();
        }
        match sup.wait_settled(self.stall_timeout, || self.engine_arc().is_failed()) {
            crate::recovery::Settled::Swapped if self.generation() != generation => {
                if transaction {
                    Error::new(
                        ErrorCode::SnapshotExpired,
                        "refused by the failed engine while the database recovered \
                         (Options::auto_recover): not committed; the transaction's snapshot \
                         expired with the swap, so begin it again",
                    )
                } else {
                    Error::new(
                        ErrorCode::Busy,
                        "refused by the failed engine while the database recovered \
                         (Options::auto_recover): not committed, retry on the recovered one",
                    )
                }
            }
            crate::recovery::Settled::TimedOut => Error::new(
                ErrorCode::Busy,
                "recovering after a shard failure (Options::auto_recover): the reopen did not \
                 finish within Options::write_stall_timeout; not committed, retry",
            ),
            // The supervisor gave up on a failure it covers: the handle stays failed and
            // refuses commits with `Closed` (ICR 0060's rule for a covered failure), also one
            // the failed shard refused before logging it, which can meet it before the engine
            // is marked dead (#966). A failure it does not cover is as without the option.
            crate::recovery::Settled::Failed if e.refused_unlogged() && sup.gave_up() => {
                pigeonhole_engine::Error::Closed.into()
            }
            _ => e,
        }
    }

    /// A commit resolved after its call returned (an async commit, a ticket), refused by the
    /// failed engine before it logged anything (the engine's `ShardFailed`, code `Io`, or a
    /// `Closed` from its close), reported as the sync path reports it once it has waited, but
    /// without waiting (#966):
    /// `Busy` (`SnapshotExpired` for a transaction) once the swap is in, `Closed` while the
    /// recovery runs or after a give-up. Not committed either way. A failure the option
    /// does not cover is as without it.
    #[cold]
    #[inline(never)]
    fn refused_later(&self, e: Error, watch: Watch) -> Error {
        let Some(sup) = &self.supervision else {
            return e;
        };
        if self.is_closed() {
            return pigeonhole_engine::Error::Closed.into();
        }
        let failure = self
            .engine_arc()
            .failure()
            .map(crate::recovery::Failure::from_engine);
        if !sup.covers_refusal(failure) {
            return e;
        }
        if self.generation() != watch.generation {
            return if watch.transaction {
                Error::new(
                    ErrorCode::SnapshotExpired,
                    "refused by the failed engine while the database recovered \
                     (Options::auto_recover): not committed; the transaction's snapshot \
                     expired with the swap, so begin it again",
                )
            } else {
                Error::new(
                    ErrorCode::Busy,
                    "refused by the failed engine while the database recovered \
                     (Options::auto_recover): not committed, retry on the recovered one",
                )
            };
        }
        if sup.gave_up() {
            return Error::new(
                ErrorCode::Closed,
                "refused by the failed engine, and automatic recovery gave up \
                 (recovery_stats().gave_up): not committed; reopen the database",
            );
        }
        Error::new(
            ErrorCode::Closed,
            "refused by the failed engine while the database recovers \
             (Options::auto_recover): not committed; retry once it recovered",
        )
    }

    /// The current engine, loaded from the `ArcSwap` (the slow path; handles use their
    /// `Cached`). Hold the guard only for the operation.
    #[inline]
    pub(crate) fn engine(&self) -> EngineRef<'_> {
        EngineRef::Loaded(Arc::clone(&self.engine.load()))
    }

    /// The generation the current engine was opened in (see [`Db::swap_engine`]).
    #[inline]
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire) & !CLOSED
    }

    /// Fails unless `generation` is the current one and the database is open: with
    /// `SnapshotExpired` when a snapshot, a transaction or an iterator from an earlier
    /// engine would read through its old view (the reopen freed the extents it names:
    /// D148's reasoning, D265), with `Closed` after `close()` (#826). One load.
    #[inline]
    pub(crate) fn check_generation(&self, generation: u64) -> Result<()> {
        let current = self.generation.load(Ordering::Acquire);
        if current == generation {
            Ok(())
        } else if current & CLOSED != 0 {
            Err(pigeonhole_engine::Error::Closed.into())
        } else {
            Err(pigeonhole_engine::Error::SnapshotExpired.into())
        }
    }

    /// Replaces the engine (D265): later operations run on `engine`, the generation moves
    /// on, and the previous engine is returned for the caller to drop. The order matters: the
    /// new engine is in place before the generation moves, so an operation that passed its
    /// generation check loads either the old engine (still open, or closing: it then fails
    /// with `Closed`) or the new one, never a mix.
    pub(crate) fn swap_engine(&self, engine: Arc<Engine>) -> Arc<Engine> {
        let previous = self.engine.swap(engine);
        self.generation.fetch_add(1, Ordering::AcqRel);
        previous
    }

    fn io_rings(&self) -> Option<IoRings> {
        self.vfs.ring_stats().map(|s| IoRings {
            rings: s.rings,
            attach_failures: s.attach_failures,
            inline: s.inline,
        })
    }

    /// Fails with `Closed` once the database was closed through any handle.
    #[inline]
    pub(crate) fn check_open(&self) -> Result<()> {
        if self.generation.load(Ordering::Acquire) & CLOSED != 0 {
            Err(pigeonhole_engine::Error::Closed.into())
        } else {
            Ok(())
        }
    }

    pub(crate) fn snapshot(self: &Arc<Self>, cached: &Cached) -> Result<Snapshot> {
        self.check_open()?;
        // The generation before the engine, inside the retry (perf287's R2 on #836): a swap
        // in between leaves a snapshot that fails its check rather than one that passes it
        // with an old view, and a snapshot retried on the new engine carries that one's.
        let (inner, generation) = self.retry_closed(|| {
            let generation = self.generation();
            let inner = cached.with(self, Engine::snapshot)?;
            Ok((inner, generation))
        })?;
        Ok(Snapshot {
            inner,
            db: Arc::downgrade(self),
            generation,
        })
    }

    pub(crate) fn durable_snapshot(self: &Arc<Self>, cached: &Cached) -> Result<Snapshot> {
        self.check_open()?;
        let (inner, generation) = self.retry_closed(|| {
            let generation = self.generation();
            let inner = cached.with(self, Engine::durable_snapshot)?;
            Ok((inner, generation))
        })?;
        Ok(Snapshot {
            inner,
            db: Arc::downgrade(self),
            generation,
        })
    }

    /// Refuses a snapshot taken from another database, or from an earlier engine of this one
    /// (`SnapshotExpired`, D265).
    #[inline]
    pub(crate) fn check_snapshot(self: &Arc<Self>, snapshot: &Snapshot) -> Result<()> {
        if !std::ptr::eq(Weak::as_ptr(&snapshot.db), Arc::as_ptr(self)) {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "the snapshot belongs to another database",
            ));
        }
        self.check_generation(snapshot.generation)
    }
}

/// An open database: the one writer process's handle. Cheap to clone; every clone shares the
/// same engine and shard threads.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let users = db
///     .table("users")?
///     .family("profile", Family::default().max_versions(1))
///     .create_if_missing()?;
/// users.mutate(b"user:42").put("profile", b"name", b"Ada").commit()?;
/// assert_eq!(db.tables(), ["users"]);
/// db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Pigeonhole {
    pub(crate) db: Arc<Db>,
    pub(crate) cached: Cached,
}

impl Pigeonhole {
    /// The engine for one operation (the cached one while this handle is current).
    #[inline]
    pub(crate) fn engine(&self) -> EngineRef<'_> {
        self.cached.get(&self.db)
    }
}

impl Pigeonhole {
    /// Opens (or creates) a database as the writer. A second writer, in this or another
    /// process, fails with [`ErrorCode::WriterLocked`](crate::ErrorCode::WriterLocked).
    /// Opening replays the WAL sidecars; there is no full-file recovery scan.
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Pigeonhole> {
        let engine_options = options.to_engine()?;
        let vfs = Arc::clone(&engine_options.vfs);
        let max_value = max_value(&engine_options);
        let shm = ShmFootprint::of(&engine_options);
        let engine = Engine::open(path.as_ref(), engine_options).map_err(|e| shm.explain(e))?;
        let stall_timeout = options.stall_timeout();
        let supervised = options.recovery() != Recovery::Off;
        let db = Db::with_recovery(
            engine,
            vfs,
            max_value,
            Some((options, path.as_ref().to_path_buf())),
            stall_timeout,
        );
        if supervised {
            crate::recovery::Supervision::start(&db)?;
        }
        let cached = Cached::new(&db);
        Ok(Pigeonhole { db, cached })
    }

    /// Opens a read-only handle (Phase 4). Any number of reader processes may open the same
    /// file while a writer runs; they see each commit as soon as the writer publishes it.
    ///
    /// **Readers need write access to the file.** The handle never writes, but the process
    /// opens the `.phdb` file read-write, because the coordination locks it takes are
    /// exclusive byte-range locks, which POSIX grants only on a writable descriptor (the same
    /// requirement as SQLite in WAL mode). A file on read-only media, or one the reader's
    /// user cannot write, cannot be opened this way (decision D36).
    pub fn open_reader(path: impl AsRef<Path>, options: ReaderOptions) -> Result<PigeonholeReader> {
        let engine_options = options.to_engine()?;
        let vfs = Arc::clone(&engine_options.vfs);
        let engine = Engine::open_reader(path.as_ref(), engine_options)?;
        let db = Db::new(engine, vfs, 0);
        let cached = Cached::new(&db);
        Ok(PigeonholeReader { db, cached })
    }

    /// Opens as the writer in application-owned mode (ICR 0088): no shard or compaction
    /// threads are started; drive each [`Shard`] in the returned [`AppOwned`] from one of your
    /// own (typically pinned) threads. The default I/O backend still runs a pool of 2-16 I/O threads for WAL syncs
    /// and reads; a threadless mode is planned for Phase 3. Because no compaction thread
    /// is started, [`Options::compaction_cores`] with `k > 0` is refused with
    /// [`ErrorCode::InvalidArgument`](crate::ErrorCode::InvalidArgument) (decision D40).
    ///
    /// Every blocking call (`commit`, `flush`, `close`) waits for the shards to run, so make
    /// sure each shard is being driven before calling one. A thread that drives a shard (it
    /// last called [`Shard::run_once`]) must not block on shard work: there `commit`,
    /// `check_and_mutate`, transaction commits, `flush` and `compact` fail with
    /// [`ErrorCode::InvalidArgument`](crate::ErrorCode::InvalidArgument) before doing
    /// anything, instead of deadlocking. A thread holding a shard it has never run is not
    /// detected, so run each shard before committing from its thread. Table creation, other
    /// catalog changes and `shrink` may be called from any thread.
    ///
    /// The loop for each shard: call [`Shard::run_once`] until it returns `false`, then sleep
    /// until the [`Shard::set_wakeup`] callback fires (work arrived or I/O completed) or
    /// [`Shard::next_wakeup`] passes (background work is due), whichever comes first. An idle
    /// shard then uses no CPU, even while a write stall or a failed compaction's retry is
    /// pending, beyond a short tablet-balancer pass: every 100 ms after writes, backing off to
    /// one every 10 s while idle.
    ///
    /// To close, call [`Pigeonhole::close`] and keep driving each shard the same way until
    /// [`Shard::closed`] returns `Some`, then drop it: the close's flush and syncs run on
    /// the shards, and `run_once` returns `false` while their I/O is in flight. Called on a
    /// thread that drives no shard (once every shard has been run), `close` waits for this
    /// and returns the close's result. Called on a thread that drives a shard, or before
    /// every shard has been run, it cannot wait: it returns `Ok(())` once the shards are
    /// told to close, and the close's result (a failed or unclean close) is then reported
    /// only by [`Shard::closed`].
    ///
    /// ```
    /// use std::thread;
    /// use std::time::Duration;
    /// use pigeonhole::{AppOwned, Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let AppOwned { db, shards, .. } =
    ///     Pigeonhole::open_application_owned(dir.join("app.phdb"), Options::default().shards(2))?;
    /// let threads: Vec<_> = shards
    ///     .into_iter()
    ///     .map(|mut shard| {
    ///         thread::spawn(move || -> pigeonhole::Result<()> {
    ///             let me = thread::current();
    ///             shard.set_wakeup(Box::new(move || me.unpark()));
    ///             loop {
    ///                 // Your event loop: run the shard until it is idle, do your own work...
    ///                 while shard.run_once(Duration::from_micros(200)) {}
    ///                 // ...and stop once the database's close has finished on every shard.
    ///                 if let Some(closed) = shard.closed() {
    ///                     return closed;
    ///                 }
    ///                 // ...then sleep until work arrives or background work is due.
    ///                 match shard.next_wakeup() {
    ///                     Some(due) => thread::park_timeout(due),
    ///                     None => thread::park(),
    ///                 }
    ///             }
    ///         })
    ///     })
    ///     .collect();
    ///
    /// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
    /// t.mutate(b"row").put("f", b"q", b"v").commit()?;
    /// assert_eq!(t.get(b"row", "f", b"q")?.unwrap().value(), b"v");
    ///
    /// drop(t);
    /// // Waits for the shard threads to finish the close, and returns its result.
    /// db.close()?;
    /// for thread in threads {
    ///     thread.join().unwrap()?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Background workers
    /// With [`Options::background_workers`] above 0 (the default is 0), [`AppOwned::workers`]
    /// holds that many background workers (D284) for flush and compaction, which then run on
    /// threads you choose rather than in slices on the shards.
    ///
    /// - **Why:** a flush or compaction slice on a shard holds up every commit queued
    ///   behind it for up to half a millisecond. With workers, the shards only commit.
    /// - **Driving:** a worker runs as a shard does: [`BackgroundWorker::run_once`] until it
    ///   returns `false`, then sleep until its [`set_wakeup`](BackgroundWorker::set_wakeup)
    ///   callback fires or [`next_wakeup`](BackgroundWorker::next_wakeup) passes. Any thread
    ///   may drive one, including a thread that drives shards, and a worker may move between
    ///   threads: its I/O completes inside `run_once`. Or call
    ///   [`AppOwned::spawn_background_threads`] to give each a thread of its own.
    /// - **Close:** [`Pigeonhole::close`] never waits for you to drive a worker. It takes
    ///   back what the workers hold (a flush finishes on its shard; a compaction is dropped
    ///   and runs again after the next open), waiting only for a slice a worker is running at
    ///   that moment. Then [`BackgroundWorker::closed`] is `true` and the worker can be
    ///   dropped.
    /// - **A worker you stop driving** delays its tasks until the shards take them back: a
    ///   queued task no worker took, once no worker has looked for 10 ms; the tasks a worker
    ///   holds that could run, once it has made no pass for 100 ms (never inside a slice);
    ///   and a flush a worker holds but isn't running, 2 ms into a commit's wait for memtable
    ///   room.
    ///   [`EngineMetrics::background_fallback_take_backs`](crate::EngineMetrics::background_fallback_take_backs)
    ///   counts the first two, so a loop that forgot its workers shows up.
    ///
    /// ```
    /// use std::thread;
    /// use std::time::Duration;
    /// use pigeonhole::{Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let options = Options::default().shards(2).background_workers(1);
    /// let mut owned = Pigeonhole::open_application_owned(dir.join("app.phdb"), options)?;
    /// // Flush and compaction get a thread each; the shards run on yours, as above.
    /// let workers = owned.spawn_background_threads()?;
    /// let shards: Vec<_> = owned
    ///     .shards
    ///     .drain(..)
    ///     .map(|mut shard| {
    ///         thread::spawn(move || -> pigeonhole::Result<()> {
    ///             let me = thread::current();
    ///             shard.set_wakeup(Box::new(move || me.unpark()));
    ///             loop {
    ///                 while shard.run_once(Duration::from_micros(200)) {}
    ///                 if let Some(closed) = shard.closed() {
    ///                     return closed;
    ///                 }
    ///                 match shard.next_wakeup() {
    ///                     Some(due) => thread::park_timeout(due),
    ///                     None => thread::park(),
    ///                 }
    ///             }
    ///         })
    ///     })
    ///     .collect();
    ///
    /// let db = owned.db;
    /// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
    /// t.mutate(b"row").put("f", b"q", b"v").commit()?;
    /// db.flush()?; // runs on the worker
    /// assert_eq!(t.get(b"row", "f", b"q")?.unwrap().value(), b"v");
    ///
    /// drop(t);
    /// db.close()?;
    /// for thread in shards {
    ///     thread.join().unwrap()?;
    /// }
    /// for thread in workers {
    ///     thread.join().unwrap();
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn open_application_owned(path: impl AsRef<Path>, options: Options) -> Result<AppOwned> {
        Self::open_app(path.as_ref(), options)
    }

    /// The application-owned open: the shards and any background workers are handed out.
    fn open_app(path: &Path, options: Options) -> Result<AppOwned> {
        if options.recovery() != Recovery::Off {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "Options::auto_recover needs engine-owned shards: an application-owned engine's \
                 shards are driven by the application, which closes and reopens it itself",
            ));
        }
        let engine_options = options.to_engine_for(true)?;
        let vfs = Arc::clone(&engine_options.vfs);
        let max_value = max_value(&engine_options);
        let shm = ShmFootprint::of(&engine_options);
        let owned = Engine::open_application_owned_with(path, engine_options)
            .map_err(|e| shm.explain(e))?;
        let (engine, shards, workers) = (owned.engine, owned.shards, owned.workers);
        let workers = workers
            .into_iter()
            .map(|inner| BackgroundWorker {
                inner,
                vfs: Arc::clone(&vfs),
            })
            .collect();
        let shards = shards
            .into_iter()
            .map(|inner| Shard {
                inner,
                vfs: Arc::clone(&vfs),
                io_fd_used: std::sync::atomic::AtomicBool::new(false),
            })
            .collect();
        let db = Db::new(engine, Arc::clone(&vfs), max_value);
        let cached = Cached::new(&db);
        Ok(AppOwned {
            db: Pigeonhole { db, cached },
            shards,
            workers,
        })
    }

    /// Starts defining or opening a table: chain `.family(..)` and finish with
    /// `.create_if_missing()`, `.create()` or `.open()`.
    pub fn table(&self, name: &str) -> Result<TableBuilder<'_>> {
        Ok(TableBuilder::new(self, name))
    }

    /// Names of every table.
    pub fn tables(&self) -> Vec<String> {
        self.db
            .engine()
            .tables()
            .iter()
            .map(|t| t.name.clone())
            .collect()
    }

    /// Drops a table and all its data.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let info = self
            .db
            .engine()
            .table(name)
            .ok_or_else(|| pigeonhole_engine::Error::TableNotFound(name.to_owned()))?;
        self.db
            .retry_closed(|| self.engine().drop_table(info.id).map_err(Error::from))
    }

    /// A batch of writes across any rows and tables, committed with one durability point.
    pub fn write_batch(&self) -> WriteBatch {
        WriteBatch::new(Arc::clone(&self.db), self.cached.fresh(&self.db))
    }

    /// Starts an optimistic multi-row transaction (Phase 4).
    pub fn transaction(&self) -> Result<Transaction> {
        self.db.check_open()?;
        let generation = self.db.generation();
        Ok(Transaction::new(
            Arc::clone(&self.db),
            self.engine().begin()?,
            generation,
        ))
    }

    /// Whether this instance has failed (ICR 0040): a shard's thread panicked, an
    /// application-owned shard's driver was dropped before the close finished, a shard
    /// stopped after a WAL write or sync failed (or its in-memory state could not be
    /// updated), or a manifest commit to the main file failed. A failed instance refuses
    /// some or all commits until it is closed and reopened: see the user guide's
    /// "Recovering from a failed shard". [`failure`](Self::failure) says which shard and
    /// why. Cheap (one atomic load), so it can back a health endpoint.
    pub fn is_failed(&self) -> bool {
        self.engine().is_failed() || self.db.supervision().is_some_and(|s| s.stood_down())
    }

    /// What automatic recovery ([`Options::auto_recover`](crate::Options::auto_recover))
    /// did since this handle opened: the engine's generation (1 at open, +1 per recovery),
    /// how many recoveries completed, whether the supervisor gave up, the last failure it
    /// handled and the last recovery's duration. All zero and `false` with the option off.
    /// Cheap (one lock, no I/O). Engine metrics restart from zero at each recovery;
    /// `generation` shows the reset.
    pub fn recovery_stats(&self) -> RecoveryStats {
        match self.db.supervision() {
            Some(s) => s.stats(self.db.generation()),
            None => RecoveryStats {
                generation: self.db.generation(),
                ..RecoveryStats::default()
            },
        }
    }

    /// The instance's first failure, if any (see [`is_failed`](Self::is_failed)): the shard
    /// ([`Failure::MAIN_FILE`] for the main file) and the
    /// [`FailureCause`](crate::FailureCause). Later failures do not replace it.
    pub fn failure(&self) -> Option<Failure> {
        self.engine()
            .failure()
            .map(Failure::from_engine)
            .or_else(|| {
                self.db
                    .supervision()
                    .filter(|s| s.gave_up())
                    .and_then(|s| s.last_failure())
            })
    }

    /// A consistent snapshot of everything committed so far. It sees a commit once the commit
    /// is applied, possibly before its fsync (D227): a power loss can remove what it read.
    pub fn snapshot(&self) -> Result<Snapshot> {
        self.db.snapshot(&self.cached)
    }

    /// A consistent snapshot that sees a commit only once it is durable at its commit's level
    /// (D227, ICR 0037): of what it reads, `GroupSync` and `Sync` commits survive a power
    /// loss, `Buffered` ones a process crash, and `None` ones nothing. It may lag
    /// [`snapshot`](Self::snapshot) by the fsyncs in flight (typically well under a
    /// millisecond). Use it when what you read must never be undone: deriving a write on
    /// another shard from it, or a cursor you persist.
    ///
    /// ```
    /// use pigeonhole::{Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
    /// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
    /// t.mutate(b"row").put("f", b"q", b"v").commit()?; // durable once it returns
    /// let durable = db.durable_snapshot()?;
    /// assert_eq!(t.get_at(&durable, b"row", "f", b"q")?.unwrap().value(), b"v");
    /// # drop(t);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn durable_snapshot(&self) -> Result<Snapshot> {
        self.db.durable_snapshot(&self.cached)
    }

    /// Per-shard commits, tablets and tablet changes since open, indexed by shard: a bench
    /// hook (ICR 0010) that shows whether one table's writes spread over the shards.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn shard_stats(&self) -> Vec<crate::ShardStats> {
        self.engine()
            .shard_stats()
            .into_iter()
            .map(crate::ShardStats::from_engine)
            .collect()
    }

    /// The active shard count the balancer published and the count it is heading for (the
    /// same unless a growth or the first pack from all shards is pending), `None` before its
    /// first decision: a bench hook (D242's amendment, #983) so a warm-up does not end inside
    /// a regrow or before the first pack.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn pack_target(&self) -> Option<(usize, usize)> {
        self.engine().pack_target()
    }

    /// The driver threads the shards run on now and whether every shard is on its driver
    /// (D289 PR 2), `None` in application-owned mode: a bench hook, so a warm-up does not end
    /// while the load controller resizes.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn driver_target(&self) -> Option<(usize, bool)> {
        self.engine().driver_target()
    }

    /// Fixes the driver target at `k`, or with `None` gives it back to the controller (D289
    /// PR 2): for tests that move shards between drivers under load.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn force_driver_target(&self, k: Option<usize>) {
        self.engine().force_driver_target(k);
    }

    /// The load controller's windows since the last call, one JSON object each (D289,
    /// #1015): every driver's busy, passes, slow passes, ops and sibling wait, the producer
    /// wakes, and what the controller decided. Empty in application-owned mode. The first
    /// call starts the log; with
    /// [`Options::adaptive_shard_threads`](crate::Options::adaptive_shard_threads) off it starts
    /// an observe-only controller that logs what it would decide but never resizes. A bench
    /// hook.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn driver_trace(&self) -> Vec<String> {
        self.engine().driver_trace()
    }

    /// What the row cache did since open: hits, misses and fills (D201). All zero when
    /// [`Options::row_cache`](crate::Options::row_cache) is 0.
    pub fn row_cache_stats(&self) -> crate::RowCacheStats {
        crate::RowCacheStats::from_engine(&self.engine())
    }

    /// The writer's counters since this instance opened: commits and their latency,
    /// flushes, compactions, write stalls, the block cache, file growth, the shard threads,
    /// reader slots, transactions, and cross-shard commits' rounds and fast failures
    /// (ICR 0044, ICR 0087). Most are cumulative
    /// from 0 at open, so take two readings and subtract for an interval; see
    /// [`EngineMetrics`](crate::EngineMetrics) for each field. Cheap and non-blocking.
    pub fn engine_metrics(&self) -> crate::EngineMetrics {
        let mut m = crate::EngineMetrics::from_engine(self.engine().metrics());
        self.db.txn_counts.fill(&mut m);
        m
    }

    /// Commit latency histograms per durability level since open, summed over shards: a
    /// bench hook (D213). Bucket `b` starts at
    /// [`commit_latency_floor(b)`](Self::commit_latency_floor) nanoseconds; subtract two readings for a phase.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn commit_latency_buckets(&self) -> [[u64; 64]; 4] {
        self.engine().commit_latency_buckets()
    }

    /// The lower bound in nanoseconds of [`commit_latency_buckets`](Self::commit_latency_buckets)'
    /// bucket `b`.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn commit_latency_floor(b: usize) -> u64 {
        pigeonhole_engine::Engine::commit_latency_floor(b)
    }

    /// Tablet changes refused for lack of room since open, as `(slot budget, view size)`:
    /// see the engine's `tablet_refusals`. A bench hook (#123).
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn tablet_refusals(&self) -> (u64, u64) {
        self.engine().tablet_refusals()
    }

    /// Bytes of async commits each shard holds submitted and not yet resolved (D210): a
    /// test hook. At most half `memtable_budget` a shard, or one commit larger than that
    /// alone.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn async_commit_inflight(&self) -> Vec<u64> {
        (0..self.engine().shard_stats().len())
            .map(|s| self.engine().admission_inflight(s))
            .collect()
    }

    /// With io_uring (#402; the default on Linux, D218): how many io_uring rings the
    /// database has now (a shared one, plus one per shard thread that got its own), how
    /// often a thread could not get one, and how much I/O ran inline on the calling thread
    /// instead. `None` on other backends.
    ///
    /// A ring registers a buffer pool on its first read that can use one. Pools are pinned
    /// memory, charged to the user's locked-memory limit (`RLIMIT_MEMLOCK`, often 8 MiB)
    /// unless the process has `CAP_IPC_LOCK`; a process registers them up to half that
    /// limit, 1 MiB a ring. Rings past it use plain buffers: correct, but each read pins its
    /// pages.
    pub fn io_rings(&self) -> Option<IoRings> {
        self.db.io_rings()
    }

    /// D292's inline page-cache reads on this database's I/O backend so far (hits, misses
    /// and their times, refusals, breaker trips), or `None` on a backend without them. For
    /// benchmarks (ICR 0083); not part of the API.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn nowait_reads(&self) -> Option<pigeonhole_io::NowaitReads> {
        self.db.vfs.nowait_reads()
    }

    /// The counters [`engine_metrics`](Self::engine_metrics) leaves out, by name (ICR 0087):
    /// the engine's mechanisms (shard parks and wakes, the WAL's unpin passes, visibility
    /// waits, cross-shard refusals and share waits, io_uring buffer pools, D292's inline
    /// page-cache reads, and the like), for benchmarks and tests. Not part of the API: names,
    /// and whether a name is present at all, may change in any release, so treat a missing
    /// name as absent.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn internal_metrics(&self) -> Vec<(&'static str, u64)> {
        crate::metrics::internal(
            &self.engine().metrics(),
            self.db.vfs.ring_stats().map(|s| s.pooled),
            self.db.vfs.nowait_reads(),
        )
    }

    /// What the database holds and how it is stored: format version, file and free bytes,
    /// and per table its tablets, its families' stored options and their SSTs per level and
    /// blob files (ICR 0047). Reads the current catalog; cheap, no data I/O.
    pub fn info(&self) -> Result<crate::DatabaseInfo> {
        Ok(crate::DatabaseInfo::from_engine(self.engine().info()?))
    }

    /// The writer default durability.
    pub fn default_durability(&self) -> Durability {
        Durability::from_engine(self.engine().default_durability())
    }

    /// Changes the writer default; applies to commits that start afterwards.
    pub fn set_default_durability(&self, durability: Durability) {
        self.engine().set_default_durability(durability.engine());
    }

    /// Writes every memtable into the file and returns once the data is there (the SSTs are
    /// in the manifest). Afterwards even [`Durability::None`] commits survive a crash, and
    /// the WAL behind the flushed data is checkpointed. Background flushes run on their own
    /// as memtables fill; call this to make everything so far durable in the file.
    ///
    /// Each flushed memtable needs a fresh one from the memtable arena. When snapshots pin
    /// the arena's memory, the flush waits as a stalled write does, and fails with
    /// [`ErrorCode::Busy`](crate::ErrorCode::Busy) if no room frees up within the
    /// write-stall timeout. Nothing is lost: drop old snapshots and retry.
    ///
    /// ```
    /// use pigeonhole::{Durability, Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let path = dir.join("app.phdb");
    /// let db = Pigeonhole::open(&path, Options::default().shards(1))?;
    /// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
    /// t.mutate(b"row").put("f", b"q", b"v").durability(Durability::None).commit()?;
    /// db.flush()?; // now in the file, whatever the commit's durability level was
    /// # drop(t);
    /// db.close()?;
    ///
    /// let db = Pigeonhole::open(&path, Options::default().shards(1))?;
    /// let t = db.table("t")?.open()?;
    /// assert_eq!(t.get(b"row", "f", b"q")?.unwrap().value(), b"v");
    /// # drop(t);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn flush(&self) -> Result<()> {
        self.db
            .retry_closed(|| self.engine().flush().map_err(Error::from))
    }

    /// Compacts every table fully: flushes, then merges every level into the last one.
    /// Versions beyond a family's `max_versions`, expired cells and tombstones that no
    /// snapshot can still need are dropped. Compaction otherwise runs in the background;
    /// call this after a bulk load or delete to reclaim space and speed up reads. Its flush
    /// can fail with [`ErrorCode::Busy`](crate::ErrorCode::Busy) as
    /// [`flush`](Self::flush) does.
    ///
    /// ```
    /// use pigeonhole::{Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
    /// let t = db
    ///     .table("t")?
    ///     .family("f", Family::default().max_versions(1))
    ///     .create_if_missing()?;
    /// t.mutate(b"row").put("f", b"q", b"old").commit()?;
    /// t.mutate(b"row").put("f", b"q", b"new").commit()?;
    /// db.compact()?;
    /// assert_eq!(t.get(b"row", "f", b"q")?.unwrap().value(), b"new");
    /// # drop(t);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn compact(&self) -> Result<()> {
        self.db
            .retry_closed(|| self.engine().compact(None).map_err(Error::from))
    }

    /// Writes a consistent single-file copy to `dest`, which must not exist, while writes
    /// continue. The copy holds exactly the commits visible when this is called and opens
    /// on its own, without WAL replay or sidecar files. Visible is not durable: the copy can
    /// hold commits whose fsync had not finished, which a power loss then removes from this
    /// database (see the durability guide). The snapshot's memtables are written into the
    /// copy, and its SST and blob files are copied byte for byte: the copy is about the size
    /// of the live data, keeps the source's level shape, and holds a blob file only if the
    /// snapshot references a value in it.
    ///
    /// ```
    /// use pigeonhole::{Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
    /// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
    /// t.mutate(b"row").put("f", b"q", b"v").commit()?;
    ///
    /// let copy_path = dir.join("backup.phdb");
    /// db.backup(&copy_path)?;
    /// t.mutate(b"row").put("f", b"q", b"later").commit()?; // not in the backup
    ///
    /// let copy = Pigeonhole::open(&copy_path, Options::default().shards(1).create_if_missing(false))?;
    /// let ct = copy.table("t")?.open()?;
    /// assert_eq!(ct.get(b"row", "f", b"q")?.unwrap().value(), b"v");
    /// # drop(ct);
    /// # copy.close()?;
    /// # drop(t);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()> {
        self.db
            .retry_closed(|| self.engine().backup(dest.as_ref()).map_err(Error::from))
    }

    /// As [`backup`](Self::backup), at a durable snapshot (D227, ICR 0037): the copy holds
    /// only commits durable here at their level. A `GroupSync` or `Sync` commit it holds
    /// survives a power loss here; a `Buffered` or `None` one may not.
    pub fn backup_durable(&self, dest: impl AsRef<Path>) -> Result<()> {
        self.db.retry_closed(|| {
            self.engine()
                .backup_durable(dest.as_ref())
                .map_err(Error::from)
        })
    }

    /// Returns free space at the end of the file to the filesystem and reports the bytes
    /// released. The file never shrinks by itself: space freed by compaction or by deleting
    /// rows is reused by later writes, but the file keeps its length. Call this after a
    /// large delete followed by [`compact`](Self::compact) when you need
    /// the disk space back (for example before copying the file, or on a small device).
    ///
    /// It cuts off the free space at the end of the file, moves the live data that sits past
    /// the point where the file could end into free space nearer the start, and truncates
    /// again. The cost is reading and rewriting that data (at most the live bytes in the
    /// file's tail) and a manifest commit per round, so run it rarely. It runs online: reads,
    /// writes, flushes and compactions continue on other threads, and in-flight flush or
    /// compaction output is never moved. It returns `0` when there is nothing to release,
    /// for example before `compact` has freed anything. Space still held by an open
    /// snapshot or scan, a backup in progress or a reader process's snapshot is not released
    /// until that handle is dropped. While one is held, `shrink` never grows the file, even
    /// for a while, and so may give back less; call it again afterwards for the rest.
    ///
    /// The file can end no earlier than its live data packed toward the start. Data lives in
    /// extents of a power of two from 64 KiB to 64 MiB, each aligned to its size, and the
    /// first 64 KiB of the file is the header: a file holding one 64 MiB extent (a large
    /// SST) is at least 128 MiB, however little else it holds. Data with no free extent of
    /// its size below it stays where it is; that is not an error.
    ///
    /// Errors: [`ErrorCode::Closed`](crate::ErrorCode::Closed) after `close`;
    /// [`ErrorCode::ReadOnly`](crate::ErrorCode::ReadOnly) if this handle cannot write;
    /// [`ErrorCode::NoSpace`](crate::ErrorCode::NoSpace) when the disk is full and moving
    /// the manifest needs the file to grow by its few KiB first (free space and retry);
    /// [`ErrorCode::Io`](crate::ErrorCode::Io) on a disk failure. A failed shrink loses no
    /// data; if it fails while committing the manifest, reopen the database.
    ///
    /// ```
    /// use pigeonhole::{Durability, Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let path = dir.join("app.phdb");
    /// let db = Pigeonhole::open(&path, Options::default().shards(1))?;
    /// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
    /// t.mutate(b"keep").put("f", b"q", b"v").commit()?;
    /// let junk = db.table("junk")?.family("f", Family::default()).create_if_missing()?;
    /// for i in 0..2_000u32 {
    ///     junk.mutate(&i.to_be_bytes()).put("f", b"q", &[7u8; 512]).durability(Durability::None).commit()?;
    /// }
    /// db.flush()?;
    /// for i in 0..2_000u32 {
    ///     junk.mutate(&i.to_be_bytes()).delete_row().durability(Durability::None).commit()?;
    /// }
    /// # drop(junk);
    /// db.compact()?;
    ///
    /// let before = std::fs::metadata(&path).unwrap().len();
    /// let released = db.shrink()?;
    /// let after = std::fs::metadata(&path).unwrap().len();
    /// assert_eq!(before - after, released);
    /// assert_eq!(t.get(b"keep", "f", b"q")?.unwrap().value(), b"v");
    /// # drop(t);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn shrink(&self) -> Result<u64> {
        self.db
            .retry_closed(|| self.engine().shrink().map_err(Error::from))
    }

    /// Closes this handle's database. Flushes every memtable and checkpoints the WAL; if
    /// this is the last process with the database open, also removes the sidecar files and
    /// shared-memory region, leaving one file. Dropping the last clone does the same,
    /// ignoring errors.
    ///
    /// A crash instead of a clean close leaves the sidecars; the next open replays them.
    /// Every handle derived from this database (tables, batches) fails with
    /// [`ErrorCode::Closed`](crate::ErrorCode::Closed) afterwards.
    pub fn close(self) -> Result<()> {
        self.db.generation.fetch_or(CLOSED, Ordering::Release);
        // A running recovery settles first (its swap, or its give-up; bounded by its own
        // deadline), so the engine closed is the one every handle ends on. A failure the
        // supervisor has not marked yet is not waited for (perf287's R5 on #836): the
        // supervisor, once it wakes, finds the handle closed and ends, so there is no
        // recovery to wait for, and this closes the failed engine as a close without the
        // option would (unclean; the next open replays the log).
        if let Some(sup) = self.db.supervision() {
            sup.wait_settled(self.db.stall_timeout().saturating_mul(5), || false);
        }
        let result = self.db.engine().close();
        if let Some(sup) = self.db.supervision() {
            sup.join();
        }
        Ok(result?)
    }
}

/// A read-only handle, typically in another process. Has no write methods, so misuse does
/// not compile. Reads run on the caller's thread.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole, ReaderOptions};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let path = dir.join("shared.phdb");
/// let db = Pigeonhole::open(&path, Options::default().shards(1))?;
/// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
/// t.mutate(b"row").put("f", b"q", b"v").commit()?;
///
/// // Normally in another process.
/// let reader = Pigeonhole::open_reader(&path, ReaderOptions::default())?;
/// let rt = reader.table("t")?;
/// assert_eq!(rt.get(b"row", "f", b"q")?.unwrap().value(), b"v");
/// # drop(rt);
/// # drop(reader);
/// # drop(t);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PigeonholeReader {
    db: Arc<Db>,
    cached: Cached,
}

impl PigeonholeReader {
    #[inline]
    pub(crate) fn engine(&self) -> EngineRef<'_> {
        self.cached.get(&self.db)
    }
}

impl PigeonholeReader {
    /// File reads made synchronously inside async reads since open (D196, #398): a
    /// separated value too large to cache, or a block the cache could not keep. Zero means
    /// no async read blocked its executor thread on the file.
    #[cfg(feature = "async")]
    pub fn async_sync_reads(&self) -> u64 {
        self.engine().metrics().async_sync_reads
    }

    /// As [`Pigeonhole::io_rings`].
    pub fn io_rings(&self) -> Option<IoRings> {
        self.db.io_rings()
    }

    /// As [`Pigeonhole::info`], from the catalog the writer last published.
    /// [`DatabaseInfo::free_bytes`](crate::DatabaseInfo::free_bytes) is `None`: a reader
    /// does not track allocations.
    pub fn info(&self) -> Result<crate::DatabaseInfo> {
        Ok(crate::DatabaseInfo::from_engine(self.engine().info()?))
    }

    /// What this reader's row cache did since it last attached to a writer: hits, misses
    /// and fills (D281). All zero when
    /// [`ReaderOptions::row_cache`](crate::ReaderOptions::row_cache) is 0. A re-attach (the
    /// writer restarted or recovered) empties the cache and starts the counts again.
    pub fn row_cache_stats(&self) -> crate::RowCacheStats {
        crate::RowCacheStats::from_engine(&self.engine())
    }

    /// Opens an existing table for reading.
    pub fn table(&self, name: &str) -> Result<ReadTable> {
        let info = self
            .db
            .engine()
            .table(name)
            .ok_or_else(|| pigeonhole_engine::Error::TableNotFound(name.to_owned()))?;
        Ok(ReadTable {
            core: Arc::new(TableCore::new(
                Arc::clone(&self.db),
                info,
                self.cached.fresh(&self.db),
            )),
        })
    }

    /// Names of every table.
    pub fn tables(&self) -> Vec<String> {
        self.db
            .engine()
            .tables()
            .iter()
            .map(|t| t.name.clone())
            .collect()
    }

    /// A consistent snapshot of what the writer has published. May do I/O: re-attach after
    /// a writer restart and reload the manifest if it changed. It sees commits once applied,
    /// possibly before their fsync (D227).
    pub fn snapshot(&self) -> Result<Snapshot> {
        self.db.snapshot(&self.cached)
    }

    /// A snapshot that sees only commits durable at their level (D227, ICR 0037), as
    /// [`Pigeonhole::durable_snapshot`]. May do I/O, as [`snapshot`](Self::snapshot).
    pub fn durable_snapshot(&self) -> Result<Snapshot> {
        self.db.durable_snapshot(&self.cached)
    }

    /// As [`Pigeonhole::backup`], from this reader process while the writer runs (ICR 0049):
    /// a consistent single-file copy of everything visible at a snapshot taken now. `dest`
    /// must not exist.
    ///
    /// The snapshot's pin in shared memory covers the whole view, memtables included, so
    /// the writer frees none of that view's memtables until the copy is done (its own
    /// backup releases them after copying them): a long backup of a write-heavy database
    /// can fill the writer's memtable arenas, and its commits then wait, up to
    /// [`Options::write_stall_timeout`](crate::Options::write_stall_timeout). A writer
    /// restart while the copy runs fails it with
    /// [`ErrorCode::SnapshotExpired`](crate::ErrorCode::SnapshotExpired), removing the
    /// partial copy, even one during the copy's own final commit, when the data was
    /// complete.
    pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()> {
        Ok(self.engine().backup(dest.as_ref())?)
    }

    /// As [`PigeonholeReader::backup`], at a durable snapshot (as
    /// [`Pigeonhole::backup_durable`]): the copy holds only commits durable at their level.
    pub fn backup_durable(&self, dest: impl AsRef<Path>) -> Result<()> {
        Ok(self.engine().backup_durable(dest.as_ref())?)
    }
}

/// A point-in-time view for reads. Holding it pins the data it can see; drop it promptly.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
/// t.mutate(b"row").put("f", b"q", b"old").commit()?;
///
/// let snap = db.snapshot()?;
/// t.mutate(b"row").put("f", b"q", b"new").commit()?;
///
/// assert_eq!(t.get_at(&snap, b"row", "f", b"q")?.unwrap().value(), b"old");
/// assert_eq!(t.get(b"row", "f", b"q")?.unwrap().value(), b"new");
/// drop(snap);
/// # drop(t);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub(crate) inner: pigeonhole_engine::Snapshot,
    /// The database it was taken from (weak: a snapshot does not keep the database open).
    db: Weak<Db>,
    /// The engine generation it was taken in (D265): reads through it fail with
    /// `SnapshotExpired` once the database recovered onto a newer engine.
    generation: u64,
}

impl Snapshot {
    /// The snapshot's sequence number.
    pub fn seqno(&self) -> u64 {
        self.inner.seqno()
    }
}

/// What [`Pigeonhole::open_application_owned`] hands you: the database, its shards and its
/// background workers (D284; none unless [`Options::background_workers`] asks for them). Move the shards and workers out to the threads that
/// drive them.
#[derive(Debug)]
#[non_exhaustive]
pub struct AppOwned {
    /// The database.
    pub db: Pigeonhole,
    /// One per shard, to drive as [`Pigeonhole::open_application_owned`] describes.
    pub shards: Vec<Shard>,
    /// [`Options::background_workers`] of them, for flush and compaction.
    pub workers: Vec<BackgroundWorker>,
}

impl AppOwned {
    /// Gives each background worker a thread of its own (named `pigeonhole-worker-<i>`),
    /// which drives it until the database's close finishes and then ends. Takes the workers
    /// out of [`workers`](Self::workers); join the handles after [`Pigeonhole::close`].
    ///
    /// For an application that wants its shards on its own threads but has no use for the
    /// background work's: the workers' threads sleep while there is no flush or compaction.
    ///
    /// # Errors
    /// [`ErrorCode::Io`](crate::ErrorCode::Io) when the operating system refuses a thread
    /// (a thread limit, a container's pids cap). The workers that got no thread are back in
    /// [`workers`](Self::workers), to drive yourself or drop; those already started keep
    /// running, and their handles are lost with the error, so join nothing: they end at the
    /// close.
    pub fn spawn_background_threads(&mut self) -> Result<Vec<std::thread::JoinHandle<()>>> {
        let mut handles = Vec::with_capacity(self.workers.len());
        let mut workers = std::mem::take(&mut self.workers).into_iter().enumerate();
        while let Some((i, worker)) = workers.next() {
            // The worker reaches its thread only once the thread exists: a refused spawn
            // leaves it here to hand back.
            let (tx, rx) = std::sync::mpsc::channel::<BackgroundWorker>();
            let spawned = std::thread::Builder::new()
                .name(format!("pigeonhole-worker-{i}"))
                .spawn(move || {
                    let Ok(mut worker) = rx.recv() else {
                        return;
                    };
                    while !worker.closed() {
                        while worker.run_once(Duration::from_millis(1)) {}
                        // Wakes for new work, the next deadline, or the close.
                        worker.wait(Duration::from_secs(1));
                    }
                });
            match spawned {
                Ok(handle) => {
                    // The thread holds `rx` until it receives.
                    let _ = tx.send(worker);
                    handles.push(handle);
                }
                Err(e) => {
                    self.workers.push(worker);
                    self.workers.extend(workers.map(|(_, w)| w));
                    return Err(Error::new(
                        crate::ErrorCode::Io,
                        format!("could not start background worker thread {i}: {e}"),
                    ));
                }
            }
        }
        Ok(handles)
    }
}

/// A background worker in application-owned mode (D284): it runs flushes and compactions
/// for every shard. Drive it from any thread with [`run_once`](Self::run_once), as you drive
/// a [`Shard`]. See [`Pigeonhole::open_application_owned`].
///
/// Dropping it before the close hands what it holds back to the shards, which run it.
#[derive(Debug)]
pub struct BackgroundWorker {
    inner: pigeonhole_engine::BackgroundWorker,
    vfs: VfsRef,
}

impl BackgroundWorker {
    /// Runs flush and compaction slices for up to `budget`. Returns whether work remains.
    ///
    /// A worker takes flushes ahead of compactions, and at most two tasks at a time; the
    /// others stay queued for the other workers. Its I/O completes inside the call, so the
    /// worker holds none in flight between calls and may move to another thread at any time.
    /// After the close it does nothing and returns `false`.
    pub fn run_once(&mut self, budget: Duration) -> bool {
        let budget = u64::try_from(budget.as_nanos()).unwrap_or(u64::MAX);
        let deadline = self.vfs.monotonic_nanos().saturating_add(budget);
        self.inner.run_once(deadline)
    }

    /// How long until a task this worker holds is due (one sleeping until a time), or
    /// `None` when none is. After [`run_once`](Self::run_once) returns `false`, sleep until
    /// the [`set_wakeup`](Self::set_wakeup) callback fires or this much time passes.
    pub fn next_wakeup(&self) -> Option<Duration> {
        let deadline = self.inner.next_deadline()?;
        let now = self.vfs.monotonic_nanos();
        Some(Duration::from_nanos(deadline.saturating_sub(now)))
    }

    /// Waits until this worker has work again, a task it holds falls due, the database
    /// closes, or `timeout` passes. It may return early: loop on [`run_once`](Self::run_once).
    pub fn wait(&mut self, timeout: Duration) {
        self.inner.wait(timeout);
    }

    /// A waker that ends this worker's [`wait`](Self::wait) from any thread.
    pub fn waker(&self) -> std::task::Waker {
        self.inner.waker()
    }

    /// Registers a callback run (on another thread) when work arrives for this worker or the
    /// database closes: wake the thread that drives it. Called once right away. Keep it short
    /// and non-blocking.
    pub fn set_wakeup(&mut self, wake: Box<dyn Fn() + Send + Sync>) {
        self.inner.set_wakeup(wake);
    }

    /// Whether the database's close has taken this worker's tasks back: from then on it has
    /// nothing to do, and can be dropped.
    pub fn closed(&self) -> bool {
        self.inner.closed()
    }
}

/// One shard in application-owned mode. Move it to the thread that should run it and call
/// [`Shard::run_once`] from that thread's event loop.
///
/// See [`Pigeonhole::open_application_owned`] for an example.
#[derive(Debug)]
pub struct Shard {
    inner: EngineShard,
    vfs: VfsRef,
    /// The application asked for [`Shard::io_fd`] and got one, so it waits on it: then
    /// `next_wakeup` need not report in-flight I/O as due now (#408).
    io_fd_used: std::sync::atomic::AtomicBool,
}

impl Shard {
    /// Shard index.
    pub fn index(&self) -> usize {
        usize::from(self.inner.index())
    }

    /// Call before handing this shard to another thread: drives it until the calling
    /// thread has none of its I/O in flight (#492, ICR 0029).
    ///
    /// With io_uring (the default on Linux, D218) in application-owned mode each
    /// driving thread has its own I/O ring, and only that thread can complete what was
    /// submitted to it. A shard moved with I/O still on the old thread's ring would wait on
    /// it for ever once that thread stops running turns. So: `release()` on the old thread,
    /// then move the shard, and take [`io_fd`](Shard::io_fd) again on the new thread (the
    /// descriptor belongs to the thread's ring).
    ///
    /// A no-op on the pread backend (completions arrive on its pool threads) and in
    /// engine-owned mode (shards never move). Debug builds panic when a shard turns up on
    /// another thread with I/O left on the old one unreleased.
    ///
    /// ```
    /// use pigeonhole::{Options, Pigeonhole};
    ///
    /// let dir = std::env::temp_dir().join(format!("phdb-release-{}", std::process::id()));
    /// std::fs::create_dir_all(&dir).unwrap();
    /// let pigeonhole::AppOwned { db, mut shards, .. } =
    ///     Pigeonhole::open_application_owned(dir.join("db.phdb"), Options::default().shards(1))?;
    /// let mut shard = shards.pop().unwrap();
    /// shard.run_once(std::time::Duration::from_millis(1));
    /// // Hand the shard to another thread.
    /// shard.release();
    /// let driver = std::thread::spawn(move || {
    ///     while shard.closed().is_none() {
    ///         shard.run_once(std::time::Duration::from_millis(1));
    ///     }
    /// });
    /// db.close()?;
    /// driver.join().unwrap();
    /// # let _ = std::fs::remove_dir_all(&dir);
    /// # Ok::<(), pigeonhole::Error>(())
    /// ```
    pub fn release(&mut self) {
        self.inner.release();
    }

    /// Runs queued writes, group commit and background work for up to `budget`. Returns
    /// whether work remains.
    ///
    /// A call first handles the queued messages (up to 1,024) and commits them as one group,
    /// whatever `budget` says. Then it runs background work (flush, compaction, the tablet
    /// balancer) in slices of at most 500 µs until `budget` is spent, handling newly queued
    /// messages between slices.
    ///
    /// Background work waiting for a time (a write stall's pacing, a wait for memtable room,
    /// a failed compaction's retry, the tablet balancer's next pass) is not work that
    /// remains: with only that left it returns `false`, and
    /// [`next_wakeup`](Shard::next_wakeup) says when to call it again. The callback
    /// registered with [`set_wakeup`](Shard::set_wakeup) does not fire for it.
    ///
    /// A call that completed a future woken through [`waker`](Shard::waker) (a commit this
    /// thread awaits) returns `true` before its background work, so the loop can poll that
    /// future sooner; the background work runs on the next call, and at the latest every
    /// ninth (D237).
    pub fn run_once(&mut self, budget: std::time::Duration) -> bool {
        let budget = u64::try_from(budget.as_nanos()).unwrap_or(u64::MAX);
        let deadline = self.vfs.monotonic_nanos().saturating_add(budget);
        self.inner.run_once(deadline)
    }

    /// How long until background work on this shard is due (a write stall's pacing, a wait
    /// for memtable room, a failed compaction's retry, the tablet balancer's next pass), or
    /// `None` when none is waiting for a time. `Some(Duration::ZERO)` means it is due now.
    /// The tablet balancer's next pass is always pending, so this is never `None`; it is
    /// bounded by the balancer's current interval:
    /// 100 ms after a write or a tablet change, doubling with each idle pass up to 10 s.
    ///
    /// With io_uring (the default on Linux, D218), I/O the shard submitted goes to
    /// a ring of the thread that drives it and completes only when that thread runs a shard
    /// again, so this is `Some(Duration::ZERO)` while any is in flight (#402), unless the
    /// application waits on [`io_fd`](Shard::io_fd), which turns readable when some has
    /// finished: once `io_fd` has returned a descriptor, in-flight I/O no longer counts as due
    /// (#408).
    ///
    /// After [`run_once`](Shard::run_once) returns `false`, sleep until the
    /// [`set_wakeup`](Shard::set_wakeup) callback fires or this much time passes, whichever
    /// comes first, then call `run_once` again. See [`Pigeonhole::open_application_owned`]
    /// for the loop.
    ///
    /// ```
    /// use std::time::Duration;
    /// use pigeonhole::{Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let pigeonhole::AppOwned { db, mut shards, .. } = Pigeonhole::open_application_owned(dir.join("w.phdb"), Options::default().shards(1))?;
    /// let shard = &mut shards[0];
    /// while shard.run_once(Duration::from_micros(200)) {}
    /// // Idle: only the tablet balancer's next pass is waiting for a time (at most 10 s).
    /// assert!(shard.next_wakeup().is_some_and(|d| d <= Duration::from_secs(10)));
    /// db.close()?;
    /// // This thread drives the shard, so `close` cannot wait: drive it until it has closed
    /// // (a real loop sleeps between calls as above).
    /// while shard.closed().is_none() {
    ///     shard.run_once(Duration::from_micros(200));
    /// }
    /// shard.closed().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn next_wakeup(&self) -> Option<std::time::Duration> {
        // I/O on this thread's own ring (`IoBackend::Uring`, #402) completes only when the
        // thread runs a shard again: due now while any is in flight, unless the thread has a
        // completion fd to wait on (`io_fd`, #408).
        // An application that never asked for the fd keeps polling (a loop that only sleeps
        // for `next_wakeup` would otherwise sleep through its own I/O).
        if pigeonhole_io::own_io_in_flight()
            && !(self.io_fd_used.load(std::sync::atomic::Ordering::Relaxed)
                && pigeonhole_io::own_io_fd().is_some())
        {
            return Some(std::time::Duration::ZERO);
        }
        let deadline = self.inner.next_deadline()?;
        let now = self.vfs.monotonic_nanos();
        Some(std::time::Duration::from_nanos(
            deadline.saturating_sub(now),
        ))
    }

    /// The database's close result once the close has finished (every shard closed, and the
    /// clean close recorded), or `None` before (including before [`Pigeonhole::close`]).
    /// After `close`, keep driving the shard until this is `Some`, then drop it. Every shard
    /// reports the same result.
    pub fn closed(&self) -> Option<Result<()>> {
        self.inner.closed().map(|r| r.map_err(Into::into))
    }

    /// A file descriptor that turns readable when I/O this thread submitted has finished and
    /// waits for [`run_once`](Shard::run_once) to complete it (#408), for an application
    /// that waits in its own event loop: wait until it is readable, the
    /// [`set_wakeup`](Shard::set_wakeup) callback fires, or [`next_wakeup`](Shard::next_wakeup)
    /// passes, then call `run_once`, which resets it. Call it on the thread that drives the
    /// shard, after its first `run_once`. Asking for it is the opt-in: from then on
    /// `next_wakeup` no longer reports this thread's in-flight I/O as due now, so a loop that
    /// takes the descriptor must wait on it. `None` where that thread has none: with
    /// [`IoBackend::Pread`](crate::IoBackend::Pread), on systems without io_uring, or with a
    /// custom VFS. A thread that drives several shards gets the same descriptor from each.
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use pigeonhole::{IoBackend, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let options = Options::default().shards(1).io_backend(IoBackend::Auto);
    /// let pigeonhole::AppOwned { db, mut shards, .. } = Pigeonhole::open_application_owned(dir.join("w.phdb"), options)?;
    /// let shard = &mut shards[0];
    /// while shard.run_once(Duration::from_micros(200)) {}
    /// if let Some(fd) = shard.io_fd() {
    ///     // Add `fd` (readable) to the loop's poll set, with `next_wakeup` as the timeout.
    ///     # let _ = fd;
    /// }
    /// # drop(db);
    /// # Ok(())
    /// # }
    /// ```
    pub fn io_fd(&self) -> Option<i32> {
        let fd = pigeonhole_io::own_io_fd();
        if fd.is_some() {
            self.io_fd_used
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        fd
    }

    /// Waits until this shard has work again: a commit or message arrives for it, a
    /// [`waker`](Shard::waker) fires, I/O this thread submitted completes, background work
    /// falls due ([`next_wakeup`](Shard::next_wakeup)), or `timeout` passes (#553). The loop
    /// for one shard per thread:
    ///
    /// ```
    /// use std::time::Duration;
    /// use pigeonhole::{Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let pigeonhole::AppOwned { db, mut shards, .. } =
    ///     Pigeonhole::open_application_owned(dir.join("w.phdb"), Options::default().shards(1))?;
    /// let shard = &mut shards[0];
    /// for _ in 0..3 {
    ///     while shard.run_once(Duration::from_micros(200)) {}
    ///     // Poll your own futures here, with `shard.waker()` as their waker.
    ///     shard.wait(Duration::from_millis(10));
    /// }
    /// db.close()?;
    /// while shard.closed().is_none() {
    ///     shard.run_once(Duration::from_micros(200));
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Call it on the thread that drives the shard, after [`run_once`](Shard::run_once)
    /// returns `false`. It may return early, so loop on `run_once`. It needs no completion fd
    /// and works on every backend: with io_uring it waits in the thread's ring while its own
    /// I/O is in flight, and parks otherwise. The [`set_wakeup`](Shard::set_wakeup) callback
    /// still fires. A thread that drives several shards, or waits on other sources too,
    /// uses its own event loop instead: [`io_fd`](Shard::io_fd), `set_wakeup` and
    /// `next_wakeup`.
    pub fn wait(&mut self, timeout: std::time::Duration) {
        self.inner.wait(timeout);
    }

    /// A waker that ends this shard's [`wait`](Shard::wait) from any thread. Give it to the
    /// futures the driving thread polls itself (`commit_async` and the other async calls):
    /// a commit can complete on another shard's thread, and its wake must reach this one.
    pub fn waker(&self) -> std::task::Waker {
        self.inner.waker()
    }

    /// Registers a callback invoked (from any thread) when work arrives for this shard. It
    /// does not fire when background work falls due: see [`next_wakeup`](Shard::next_wakeup).
    pub fn set_wakeup(&mut self, wake: Box<dyn Fn() + Send + Sync>) {
        self.inner.set_wakeup(wake);
    }
}

/// The shared-memory region a writer open creates, kept to explain `ShmUnavailable`: what it
/// needed, where, and how to make it fit.
struct ShmFootprint {
    shards: u64,
    budget: u64,
    /// The whole region: every arena (each rounded up to 2 MiB) plus the header, the two view
    /// buffers, the reader slots and the row-epoch table, laid out as the engine lays it out.
    region_len: u64,
    dir: Option<std::path::PathBuf>,
}

impl ShmFootprint {
    fn of(o: &pigeonhole_engine::EngineOptions) -> Self {
        // As the engine resolves it: 0 shards means one per physical core (D220).
        let shards = match o.shards {
            0 => pigeonhole_io::sys::default_shards(),
            n => n,
        };
        let count = u32::try_from(shards).unwrap_or(u32::MAX);
        let config = pigeonhole_shm::ShmConfig::new(count);
        let layout = pigeonhole_format::shm::ShmHeader::layout(
            [0; 16],
            count,
            o.reader_slots.max(1),
            config.view_buffer_bytes,
            o.memtable_budget,
            pigeonhole_engine::row_epoch_slots(o.row_cache_bytes),
            0,
            0,
        );
        Self {
            shards: shards as u64,
            budget: o.memtable_budget,
            region_len: layout.region_len,
            dir: o.shm_dir.clone(),
        }
    }

    /// `e` as an [`Error`], with the footprint and remedies when it is `ShmUnavailable`.
    fn explain(&self, e: pigeonhole_engine::Error) -> Error {
        if !matches!(e, pigeonhole_engine::Error::ShmUnavailable) {
            return e.into();
        }
        let arenas = self.shards.saturating_mul(self.budget);
        let need = format!(
            "{} ({} shards × {} memtable_budget, plus {} for views, reader slots and row epochs)",
            bytes(self.region_len),
            self.shards,
            bytes(self.budget),
            bytes(self.region_len.saturating_sub(arenas)),
        );
        let fix = "or lower Options::memtable_budget or Options::shards";
        let message = match &self.dir {
            Some(dir) => format!(
                "the shared-memory region could not be created in {}: it needs {need}. The \
                 directory must exist and have that much free space; free some, point \
                 Options::shm_dir at a larger one, {fix}",
                dir.display()
            ),
            None if cfg!(any(target_os = "linux", target_os = "android")) => format!(
                "the shared-memory region could not be created in /dev/shm: it needs {need}, \
                 and /dev/shm is too small or missing (Docker and Kubernetes default it to 64 \
                 MiB). Enlarge it (docker run --shm-size; in Kubernetes, mount an emptyDir \
                 with medium: Memory at /dev/shm), point Options::shm_dir at a larger tmpfs, \
                 {fix}"
            ),
            None => format!(
                "the shared-memory region could not be created: it needs {need} of shared \
                 memory, more than the system would commit. Free memory, point \
                 Options::shm_dir at a directory with room, {fix}"
            ),
        };
        Error::new(ErrorCode::ShmUnavailable, message)
    }
}

/// `n` bytes in MiB when whole, else KiB (rounded up).
fn bytes(n: u64) -> String {
    if n.is_multiple_of(1 << 20) {
        format!("{} MiB", n >> 20)
    } else {
        format!("{} KiB", n.div_ceil(1 << 10))
    }
}

/// The largest value a commit carries inline (decision D16), as `pigeonhole-engine` computes
/// it at open: the WAL segment payload, 64 MiB, and half a shard's memtable arena. A longer
/// put is separated into a blob file at commit time; a longer merge operand is refused.
fn max_value(o: &pigeonhole_engine::EngineOptions) -> usize {
    (o.wal.segment_size as usize)
        .saturating_sub(64 * 1024)
        .min(64 << 20)
        .min((o.memtable_budget / 2) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Instant;

    /// D265 (perf287's R5 on #836): a `close()` that reaches a failed engine before the
    /// supervisor marked the failure does not wait for a recovery that never starts (the
    /// supervisor finds the handle closed and ends). It closes the failed engine as a close
    /// without the option would, and returns well under the stall timeout; the next open
    /// replays the log. The supervisor is held on its wake by a test hook, so the close
    /// wins the race for certain.
    #[test]
    fn a_close_racing_an_unmarked_failure_closes_the_failed_engine_at_once() {
        let vfs: VfsRef = pigeonhole_io::sim::SimVfs::new(2651);
        let options = Options::default()
            .vfs(Arc::clone(&vfs))
            .shards(1)
            .pin_threads(false)
            .write_stall_timeout(Duration::from_secs(10))
            .auto_recover(crate::Recovery::Any);
        let path = Path::new("/db/race.phdb");
        let db = Pigeonhole::open(path, options.clone()).unwrap();
        let t = db
            .table("t")
            .unwrap()
            .family("f", crate::Family::default())
            .create_if_missing()
            .unwrap();
        for i in 0..8u8 {
            t.mutate(&[b'r', i]).put("f", b"q", b"v").commit().unwrap();
        }
        // The supervisor, woken by the failure, is held before it looks at it.
        let (entered_tx, entered_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let engine = db.engine().to_arc();
        engine.on_next(
            pigeonhole_engine::HookSite::FailureWake,
            Box::new(move || {
                let _ = entered_tx.send(());
                let _ = go_rx.recv();
            }),
        );
        // The next apply fails after its WAL append: the shard poisons itself (its record
        // replays at the next open) and the commit gets `OutcomeUnknown`.
        engine.inject(pigeonhole_engine::Fault::NextApply, true);
        let e = t
            .mutate(b"unknown")
            .put("f", b"q", b"v")
            .commit()
            .unwrap_err();
        assert_eq!(e.code(), crate::ErrorCode::OutcomeUnknown);
        entered_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the supervisor woke on the failure");
        assert!(db.is_failed());
        // `close()` on its own thread (it joins the supervisor at its end): once it has
        // marked the handle closed, the supervisor is let go, finds the handle closed and
        // ends without marking the failure.
        let inner = Arc::clone(&db.db);
        let closer = std::thread::spawn(move || {
            let started = Instant::now();
            (db.close(), started.elapsed())
        });
        while !inner.is_closed() {
            std::thread::sleep(Duration::from_millis(1));
        }
        let _ = go_tx.send(());
        let (result, took) = closer.join().unwrap();
        assert!(
            took < Duration::from_secs(2),
            "the close took {took:?} ({result:?}); it must not wait for a recovery"
        );
        drop(t);
        // The next open replays the log: every row, the unknown commit's included.
        let db = Pigeonhole::open(path, options.auto_recover(crate::Recovery::Off)).unwrap();
        let t = db.table("t").unwrap().open().unwrap();
        for i in 0..8u8 {
            assert!(t.get(&[b'r', i], "f", b"q").unwrap().is_some(), "row {i}");
        }
        assert!(t.get(b"unknown", "f", b"q").unwrap().is_some());
        db.close().unwrap();
    }

    /// D265: the engine behind the handle is swapped for one reopened on the first one's
    /// handover, as the recovery supervisor does. Table handles keep working on the new
    /// engine; a snapshot, a scan and a transaction from the old one expire.
    #[test]
    fn a_swapped_engine_serves_table_handles_and_expires_what_the_old_one_gave_out() {
        let vfs: VfsRef = pigeonhole_io::sim::SimVfs::new(265);
        let options = Options::default()
            .vfs(Arc::clone(&vfs))
            .shards(2)
            .pin_threads(false);
        let path = Path::new("/db/swap.phdb");
        let db = Pigeonhole::open(path, options.clone()).unwrap();
        let t = db
            .table("t")
            .unwrap()
            .family("f", crate::Family::default())
            .create_if_missing()
            .unwrap();
        t.mutate(b"row").put("f", b"q", b"old").commit().unwrap();
        let snap = db.snapshot().unwrap();
        let mut rows = t
            .scan_bounds(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
            .iter()
            .unwrap();
        let mut txn = db.transaction().unwrap();
        txn.put(&t, b"row2", "f", b"q", b"txn");
        assert_eq!(db.db.generation(), 1);

        // The swap: close the engine keeping its locks, reopen on them, swap it in.
        let old = db.engine().to_arc();
        let (closed, handover) = old.close_for_reopen();
        closed.unwrap();
        let reopened = Engine::open_with(path, options.to_engine().unwrap(), handover.unwrap())
            .map_err(|(e, _)| e)
            .unwrap();
        let previous = db.db.swap_engine(reopened);
        assert!(Arc::ptr_eq(&previous, &old));
        drop((previous, old));
        assert_eq!(db.db.generation(), 2);

        // Table handles keep working, on the new engine.
        assert_eq!(t.get(b"row", "f", b"q").unwrap().unwrap().value(), b"old");
        t.mutate(b"row").put("f", b"q", b"new").commit().unwrap();
        assert_eq!(db.tables(), ["t"]);
        assert!(!db.is_failed());

        // What the old engine gave out expires before it touches an engine.
        let expired = |e: &Error| e.code() == ErrorCode::SnapshotExpired;
        let r = t.get_at(&snap, b"row", "f", b"q");
        assert!(matches!(&r, Err(e) if expired(e)), "{r:?}");
        let r = rows.next().expect("an item, the error");
        assert!(matches!(&r, Err(e) if expired(e)), "{r:?}");
        let r = txn.commit();
        assert!(matches!(&r, Err(e) if expired(e)), "{r:?}");

        // A snapshot of the new generation reads the new engine.
        let snap2 = db.snapshot().unwrap();
        assert_eq!(
            t.get_at(&snap2, b"row", "f", b"q")
                .unwrap()
                .unwrap()
                .value(),
            b"new"
        );
        drop(rows);
        drop((snap, snap2));
        drop(t);
        db.close().unwrap();
    }

    fn footprint(shards: usize, budget: u64, dir: Option<&str>) -> ShmFootprint {
        let mut o = pigeonhole_engine::EngineOptions::new(pigeonhole_io::sim::SimVfs::new(1));
        o.shards = shards;
        o.memtable_budget = budget;
        o.shm_dir = dir.map(Into::into);
        ShmFootprint::of(&o)
    }

    #[test]
    fn shm_unavailable_names_the_footprint_and_the_remedies() {
        let e = footprint(4, 64 << 20, None).explain(pigeonhole_engine::Error::ShmUnavailable);
        assert_eq!(e.code(), ErrorCode::ShmUnavailable);
        let m = e.message();
        assert!(
            m.contains("268 MiB (4 shards × 64 MiB memtable_budget, plus 12 MiB for views"),
            "{m}"
        );
        assert!(
            m.contains("Options::memtable_budget or Options::shards"),
            "{m}"
        );
        assert!(m.contains("Options::shm_dir"), "{m}");
        if cfg!(target_os = "linux") {
            assert!(m.contains("/dev/shm") && m.contains("--shm-size"), "{m}");
        }

        let e = footprint(3, 192 << 10, Some("/mnt/small"))
            .explain(pigeonhole_engine::Error::ShmUnavailable);
        assert_eq!(e.code(), ErrorCode::ShmUnavailable);
        let m = e.message();
        assert!(
            // Each arena rounds up to 2 MiB.
            m.contains("in /mnt/small: it needs 18 MiB (3 shards × 192 KiB memtable_budget"),
            "{m}"
        );
    }

    #[test]
    fn other_open_errors_pass_through() {
        let e = footprint(1, 1 << 20, None).explain(pigeonhole_engine::Error::WriterLocked);
        assert_eq!(e.code(), ErrorCode::WriterLocked);
        assert!(!e.message().contains("shared-memory region"));
    }
}
