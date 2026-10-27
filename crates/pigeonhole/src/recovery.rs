//! Automatic recovery (D265, ICR 0050, #620): a supervisor thread per writer handle with
//! [`Options::auto_recover`](crate::Options::auto_recover) on. It parks in the engine's
//! failure wait and, on a failure the setting covers, closes the failed engine keeping its
//! writer lock and presence, waits the backoff, reopens the same path on them and swaps the
//! new engine in behind every handle ([`Db::swap_engine`]), then releases what the old engine
//! still holds ([`Engine::release_memory`]) and drops it. A handle made before the swap keeps
//! working through its slow path; what it took from the old engine expires.
//!
//! The close may block (a device that never answers holds the close's I/O wait, #824): it
//! runs on the supervisor thread under the recovery's deadline (four write-stall timeouts
//! from the failure), and a second instance is never opened beside a close that has not
//! returned. Past the deadline the recovery is given up (the handle stays failed, as without
//! the option), and whatever the close returns later is dropped, which releases the locks.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use pigeonhole_engine::Engine;

use crate::db::Db;
use crate::options::Recovery;
use crate::{Error, ErrorCode, Result};

/// Recoveries allowed in [`WINDOW`] (D265: constants in 0.4, no knob).
const MAX_RECOVERIES: usize = 3;
/// The sliding window of the budget.
const WINDOW: Duration = Duration::from_secs(10 * 60);
/// The backoff before the first, second and third reopen in the window.
const BACKOFF: [Duration; MAX_RECOVERIES] = [
    Duration::ZERO,
    Duration::from_secs(1),
    Duration::from_secs(4),
];
/// How long the supervisor waits for a failure before it checks whether the handle closed.
const POLL: Duration = Duration::from_millis(500);
/// The recovery's deadline, in write-stall timeouts (two minutes by default): a close that
/// has not returned by then, or reopens still failing, end the recovery.
const DEADLINE_STALLS: u32 = 4;

/// Why the instance failed ([`Failure::cause`]). The numbers are stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub enum FailureCause {
    /// A shard's thread panicked.
    Panic = 1,
    /// An application-owned shard's driver ([`Shard`](crate::Shard)) was dropped before the
    /// close finished.
    DriverDropped = 2,
    /// A write or sync to the shard's log, or (for [`Failure::MAIN_FILE`]) a manifest commit
    /// to the main file, failed.
    Io = 3,
    /// The shard's in-memory state could not be updated consistently: it stops rather than
    /// extend a state it cannot trust.
    Internal = 4,
}

/// The first failure of an instance ([`Pigeonhole::failure`](crate::Pigeonhole::failure),
/// ICR 0040).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
#[non_exhaustive]
pub struct Failure {
    /// The shard that failed, or [`Failure::MAIN_FILE`] when a manifest commit to the main
    /// file failed (no single shard).
    pub shard: u32,
    /// Why it failed.
    pub cause: FailureCause,
}

impl Failure {
    /// The `shard` of a main-file failure.
    pub const MAIN_FILE: u32 = u32::MAX;

    /// The public failure of an engine failure (D282: the public type is this crate's own).
    pub(crate) fn from_engine(f: pigeonhole_engine::Failure) -> Self {
        use pigeonhole_engine::FailureCause as C;
        let cause = match f.cause {
            C::Panic => FailureCause::Panic,
            C::DriverDropped => FailureCause::DriverDropped,
            C::Io => FailureCause::Io,
            // `Internal`, and any cause a later engine adds: a state it cannot trust.
            _ => FailureCause::Internal,
        };
        Self {
            shard: f.shard,
            cause,
        }
    }
}

/// What the supervisor did since the handle opened
/// ([`Pigeonhole::recovery_stats`](crate::Pigeonhole::recovery_stats)). Plain data.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecoveryStats {
    /// Opens of the engine behind the handle: 1 at open, +1 per recovery. Engine metrics
    /// restart from zero at each.
    pub generation: u64,
    /// Recoveries that completed.
    pub recoveries: u64,
    /// The supervisor gave up: the budget was spent, the reopen failed for good, or the
    /// failed engine's close did not return in time. The instance stays failed. Also set
    /// when `close()` landed while a reopen ran: the engine that reopen produced was closed
    /// instead of swapped in (the handle is closed then, so it changes nothing).
    pub gave_up: bool,
    /// The failure the last recovery (or give-up) was for.
    pub last_failure: Option<Failure>,
    /// How long the last completed recovery took, from the failure to the swap.
    pub last_recovery: Duration,
}

/// The handle's recovery state (D265): three states and the waiters on them.
#[derive(Debug)]
enum Phase {
    Healthy,
    /// A recovery runs; past `deadline` it is given up (a close that does not return, say).
    Recovering {
        deadline: Instant,
    },
    /// The supervisor gave up: the budget, a fatal reopen error or the deadline.
    GaveUp,
    /// The instance failed for a cause the setting does not cover: the supervisor stands
    /// down, and the handle is failed as without the option.
    Uncovered,
}

#[derive(Debug)]
struct State {
    phase: Phase,
    stats: RecoveryStats,
    /// When each recovery in the current window completed (the budget).
    recent: Vec<Instant>,
}

/// What an operation that would otherwise get `Closed` finds after waiting for the swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Settled {
    /// The engine was swapped (or no recovery was running): run on the current engine.
    Swapped,
    /// The recovery was given up, or the failure is not covered: the instance is failed.
    Failed,
    /// The wait's bound passed with the recovery still running.
    TimedOut,
}

/// The supervisor's state and thread, in `Db`.
pub(crate) struct Supervision {
    mode: Recovery,
    /// A recovery is running: the commit path's one load before it submits.
    recovering: AtomicBool,
    state: Mutex<State>,
    changed: Condvar,
    thread: Mutex<Option<JoinHandle<()>>>,
    /// Unit tests only: holds the supervisor back after a failure until an operation waits for
    /// it (#921).
    #[cfg(test)]
    pub(crate) test_hold: TestHold,
    /// The process that created it ([`pigeonhole_io::sys::process_id`]). A forked child has
    /// no supervisor thread: it waits for no recovery and joins nothing (#901).
    started_in: u32,
}

/// Unit tests only: while `armed`, the supervisor, woken by a failure, starts its recovery
/// only once an operation has entered [`Supervision::wait_settled`] (or after 10 s), so an
/// operation meets the failed engine before its close begins (#921's window).
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct TestHold {
    pub(crate) armed: AtomicBool,
    waited: AtomicBool,
}

#[cfg(test)]
impl TestHold {
    /// Lets a held supervisor go, as an operation entering `wait_settled` would.
    pub(crate) fn release(&self) {
        self.waited.store(true, Ordering::SeqCst);
    }

    fn hold(&self) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while !self.waited.load(Ordering::SeqCst) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

impl std::fmt::Debug for Supervision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Supervision")
            .field("mode", &self.mode)
            .field("recovering", &self.recovering.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Supervision {
    pub(crate) fn new(mode: Recovery) -> Self {
        Self {
            mode,
            recovering: AtomicBool::new(false),
            state: Mutex::new(State {
                phase: Phase::Healthy,
                stats: RecoveryStats::default(),
                recent: Vec::new(),
            }),
            changed: Condvar::new(),
            thread: Mutex::new(None),
            #[cfg(test)]
            test_hold: TestHold::default(),
            started_in: pigeonhole_io::sys::process_id(),
        }
    }

    /// In a forked child: the supervisor thread is the parent's.
    fn forked(&self) -> bool {
        pigeonhole_io::sys::process_id() != self.started_in
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        let mut st = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        // A recovery past its deadline is given up here, by whoever looks: the supervisor
        // may be blocked in the close.
        if let Phase::Recovering { deadline, .. } = st.phase
            && Instant::now() >= deadline
        {
            st.phase = Phase::GaveUp;
            st.stats.gave_up = true;
            self.recovering.store(false, Ordering::Release);
            self.changed.notify_all();
        }
        st
    }

    /// Whether a recovery is running (one load; the commit path's check).
    #[inline]
    pub(crate) fn recovering(&self) -> bool {
        self.recovering.load(Ordering::Acquire)
    }

    pub(crate) fn gave_up(&self) -> bool {
        // In a forked child the state's lock may have been copied held: inert (#901).
        if self.forked() {
            return false;
        }
        matches!(self.lock().phase, Phase::GaveUp)
    }

    /// The supervisor stood down: it gave up, or the failure is not one it covers.
    pub(crate) fn stood_down(&self) -> bool {
        if self.forked() {
            return false;
        }
        matches!(self.lock().phase, Phase::GaveUp | Phase::Uncovered)
    }

    /// Whether a commit the failed engine refused before logging it falls under the option
    /// (ICR 0060): not once the supervisor stood down on a failure it does not cover, nor for
    /// such a failure it has not looked at yet (`failure`, the current engine's). A swapped
    /// engine (no failure) means the refusal's failure was recovered from.
    pub(crate) fn covers_refusal(&self, failure: Option<Failure>) -> bool {
        if self.forked() {
            return false;
        }
        match self.lock().phase {
            Phase::Uncovered => false,
            Phase::Healthy => failure.is_none_or(|f| covers(self.mode, f.cause)),
            Phase::Recovering { .. } | Phase::GaveUp => true,
        }
    }

    pub(crate) fn stats(&self, generation: u64) -> RecoveryStats {
        if self.forked() {
            return RecoveryStats {
                generation,
                ..RecoveryStats::default()
            };
        }
        let mut s = self.lock().stats;
        s.generation = generation;
        s
    }

    pub(crate) fn last_failure(&self) -> Option<Failure> {
        if self.forked() {
            return None;
        }
        self.lock().stats.last_failure
    }

    /// Waits up to `timeout` for a running recovery to end. While no recovery is marked but
    /// `still_failing` says the engine is failed, waits too: the supervisor marks the
    /// recovery on its own thread after the failure wait wakes it, and an operation can get
    /// the failed engine's `Closed` before that.
    pub(crate) fn wait_settled(
        &self,
        timeout: Duration,
        still_failing: impl Fn() -> bool,
    ) -> Settled {
        if self.forked() {
            return Settled::Failed;
        }
        #[cfg(test)]
        self.test_hold.waited.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + timeout;
        let mut st = self.lock();
        loop {
            match st.phase {
                Phase::Healthy if !still_failing() => return Settled::Swapped,
                Phase::Healthy | Phase::Recovering { .. } => {}
                Phase::GaveUp | Phase::Uncovered => return Settled::Failed,
            }
            let now = Instant::now();
            if now >= deadline {
                return Settled::TimedOut;
            }
            drop(st);
            // Bounded, so a recovery given up by its own deadline is noticed by `lock`.
            let wait = (deadline - now).min(POLL);
            let guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let _ = self
                .changed
                .wait_timeout(guard, wait)
                .unwrap_or_else(PoisonError::into_inner);
            st = self.lock();
        }
    }

    /// Starts the supervisor thread for `db` (a writer with the option on).
    pub(crate) fn start(db: &Arc<Db>) -> Result<()> {
        let weak = Arc::downgrade(db);
        let thread = std::thread::Builder::new()
            .name("pigeonhole-recovery".to_owned())
            .spawn(move || run(weak))
            .map_err(|e| {
                Error::new(ErrorCode::Io, format!("spawn the recovery supervisor: {e}"))
            })?;
        *db.supervision()
            .expect("started only with the option on")
            .thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(thread);
        Ok(())
    }

    /// Joins the supervisor thread (at close, once the engine is closed so the thread ends).
    pub(crate) fn join(&self) {
        let thread = self
            .thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        // A forked child's copy of the handle names a thread that runs only in the parent:
        // joining would wait for ever, so it is left alone (#901).
        if self.forked() {
            std::mem::forget(thread);
            return;
        }
        if let Some(t) = thread
            && t.thread().id() != std::thread::current().id()
        {
            let _ = t.join();
        }
    }
}

/// The supervisor thread: waits for a failure of the current engine; on one the setting
/// covers, recovers; ends when the handle closes or goes, or once it gave up.
fn run(weak: Weak<Db>) {
    loop {
        let Some(db) = weak.upgrade() else { return };
        if db.is_closed() {
            return;
        }
        let engine = db.engine_arc();
        let failed = engine.wait_failed(Instant::now() + POLL);
        if db.is_closed() {
            return;
        }
        if !failed {
            drop(engine);
            drop(db);
            continue;
        }
        #[cfg(test)]
        if let Some(sup) = db.supervision() {
            sup.test_hold.hold();
        }
        if !recover(&db, engine) {
            return;
        }
    }
}

/// Whether `cause` is one `mode` recovers from.
fn covers(mode: Recovery, cause: FailureCause) -> bool {
    match mode {
        Recovery::Off => false,
        Recovery::Io => cause == FailureCause::Io,
        Recovery::Any => cause != FailureCause::DriverDropped,
    }
}

/// A reopen error that ends recovery at once (D265).
fn fatal(e: &pigeonhole_engine::Error) -> bool {
    use pigeonhole_engine::Error as E;
    matches!(
        e,
        E::Corruption(_)
            | E::UnsupportedFormat(_)
            | E::NoSpace
            | E::WriterLocked
            | E::ShmVersionMismatch { .. }
    )
}

/// Recovers `db` from the failure of `failed` (its current engine): whether the supervisor
/// goes on (a swap happened) or ends (the failure is not covered, or it gave up).
fn recover(db: &Arc<Db>, failed: Arc<Engine>) -> bool {
    let sup = db
        .supervision()
        .expect("the supervisor runs only with the option on");
    let failure = failed.failure().map(Failure::from_engine);
    if !failure.is_some_and(|f| covers(sup.mode, f.cause)) {
        // Not covered: the handle is failed as without the option; the waiters learn it.
        let mut st = sup.lock();
        if matches!(st.phase, Phase::Healthy) {
            st.phase = Phase::Uncovered;
            st.stats.last_failure = failure;
            sup.changed.notify_all();
        }
        return false;
    }
    let (since, deadline, attempt) = {
        let mut st = sup.lock();
        if !matches!(st.phase, Phase::Healthy) {
            return false;
        }
        let now = Instant::now();
        st.recent.retain(|t| now.duration_since(*t) < WINDOW);
        st.stats.last_failure = failure;
        if st.recent.len() >= MAX_RECOVERIES {
            st.phase = Phase::GaveUp;
            st.stats.gave_up = true;
            sup.changed.notify_all();
            return false;
        }
        // Past this the recovery is given up (a close that does not return, say): well above
        // the operations' own wait, so a waiter gets `Busy` and retries before the handle
        // turns failed for good.
        let deadline = now + db.stall_timeout().saturating_mul(DEADLINE_STALLS);
        st.phase = Phase::Recovering { deadline };
        sup.recovering.store(true, Ordering::Release);
        sup.changed.notify_all();
        (now, deadline, st.recent.len())
    };
    let give_up = |sup: &Supervision| {
        let mut st = sup.lock();
        st.phase = Phase::GaveUp;
        st.stats.gave_up = true;
        sup.recovering.store(false, Ordering::Release);
        sup.changed.notify_all();
        false
    };
    // The close may block (its I/O wait, #824): nothing is opened beside it until it returns.
    let (_closed, handover) = failed.close_for_reopen();
    drop(failed);
    if sup.gave_up() || db.is_closed() {
        // The deadline passed, or the handle was closed, while the close was out: the locks
        // go with the handover, and nothing is reopened.
        drop(handover);
        return give_up(sup);
    }
    let mut handover = handover;
    let mut tries = attempt;
    loop {
        let backoff = BACKOFF[tries.min(MAX_RECOVERIES - 1)];
        if !backoff.is_zero() {
            let now = Instant::now();
            if now + backoff >= deadline {
                return give_up(sup);
            }
            std::thread::sleep(backoff);
        }
        let options = match db.reopen_options() {
            Ok(o) => o,
            Err(_) => return give_up(sup),
        };
        let path = db.path();
        let result = match handover.take() {
            Some(h) => Engine::open_with(path, options, h).map_err(|(e, h)| {
                handover = Some(h);
                e
            }),
            // The final close had released the locks (every shard was dead before the close
            // ran): a plain open.
            None => Engine::open(path, options),
        };
        match result {
            Ok(engine) => {
                // The swap is decided under the state lock (perf287's R1 on #836): the
                // deadline may have passed during the backoff or the reopen, and whoever
                // looked then turned the phase to `GaveUp` and told the waiters `Closed`,
                // which is terminal; `close()` may have run meanwhile too. Then the new
                // engine is closed instead (which releases the locks cleanly) and the phase
                // stays as it is. The swap itself is an `ArcSwap` store and a `fetch_add`.
                let mut engine = Some(engine);
                let old = {
                    let mut st = sup.lock();
                    if matches!(st.phase, Phase::Recovering { .. }) && !db.is_closed() {
                        let old = db.swap_engine(engine.take().expect("not swapped yet"));
                        let now = Instant::now();
                        st.recent.push(now);
                        st.stats.recoveries += 1;
                        st.stats.last_recovery = now.duration_since(since);
                        st.phase = Phase::Healthy;
                        sup.recovering.store(false, Ordering::Release);
                        sup.changed.notify_all();
                        Some(old)
                    } else {
                        None
                    }
                };
                match old {
                    Some(old) => {
                        // The old engine's view, caches and (once its region is superseded,
                        // as it is now) arena pages go; a stale handle keeps a light shell
                        // (#821).
                        old.release_memory();
                        drop(old);
                        return true;
                    }
                    None => {
                        let engine = engine.take().expect("not swapped");
                        let _ = engine.close();
                        drop(engine);
                        return give_up(sup);
                    }
                }
            }
            Err(e) => {
                tries += 1;
                if fatal(&e) || tries >= MAX_RECOVERIES || Instant::now() >= deadline {
                    drop(handover);
                    return give_up(sup);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use pigeonhole_io::sim::SimVfs;

    use crate::{ErrorCode, Family, Options, Pigeonhole, Recovery, RetryPolicy};

    /// #921: a transaction whose commit reaches a shard after it failed, but before the
    /// supervisor began closing the engine, is refused there, never logged (the engine's
    /// `ShardFailed`, code `Io`). With `auto_recover` covering the failure it is treated as a
    /// `Closed` refusal: `SnapshotExpired` once the swap is in, which `transact` retries (ICR
    /// 0060). On main it got `Io` and `transact` failed.
    ///
    /// Deterministic: a failed apply (`Fault::NextApply`) poisons the shard without marking
    /// the engine dead, so the next commit always passes the engine's open check and reaches
    /// the poisoned shard. (A failed WAL sync marks the engine dead in the same shard turn
    /// that answers the failing commit, so only a racing commit meets the shard there, as on
    /// CI.) The supervisor is held until the commit waits for it.
    #[test]
    fn a_commit_refused_by_a_failed_shard_before_the_close_recovers() {
        let vfs = SimVfs::new(9211);
        let options = Options::default()
            .shards(2)
            .pin_threads(false)
            .memtable_budget(4 << 20)
            .write_stall_timeout(Duration::from_secs(10))
            .vfs(Arc::clone(&vfs) as _)
            .auto_recover(Recovery::Any);
        let db = Pigeonhole::open("/db/r921.phdb", options).unwrap();
        let t = db
            .table("t")
            .unwrap()
            .family("f", Family::default())
            .create_if_missing()
            .unwrap();
        t.mutate(b"n").put("f", b"q", b"0").commit().unwrap();
        db.db
            .supervision()
            .unwrap()
            .test_hold
            .armed
            .store(true, Ordering::SeqCst);
        let mut attempts = 0;
        let (value, _) = db
            .transact(&RetryPolicy::default(), |txn| {
                attempts += 1;
                let n: u64 = txn.get(&t, b"n", "f", b"q")?.map_or(0, |c| {
                    std::str::from_utf8(c.value()).unwrap().parse().unwrap()
                });
                if attempts == 1 {
                    // The shard holding the rows fails (a new table is one tablet on one
                    // shard); this attempt's commit then meets the poisoned shard.
                    db.engine()
                        .to_arc()
                        .inject(pigeonhole_engine::Fault::NextApply, true);
                    let e = t.mutate(b"x").put("f", b"q", b"1").commit().unwrap_err();
                    assert_eq!(e.code(), ErrorCode::OutcomeUnknown);
                }
                txn.put(&t, b"n", "f", b"q", (n + 1).to_string().as_bytes());
                Ok::<_, crate::Error>(n)
            })
            .unwrap();
        assert_eq!(attempts, 2, "{:?}", db.recovery_stats());
        assert_eq!(value, 0);
        assert_eq!(t.get(b"n", "f", b"q").unwrap().unwrap().value(), b"1");
        assert_eq!(db.recovery_stats().recoveries, 1);
        drop(t);
        db.close().unwrap();
    }

    /// #966, perf287's S1 on #968: a commit that resolves after its call returned (a ticket,
    /// an async commit), refused by the failed engine before it logged anything and looked
    /// at only after the swap, is `Busy` (not committed, retry on the recovered engine), as a
    /// sync commit refused there is once it waited for the swap.
    #[test]
    fn a_ticket_or_async_commit_refused_before_the_swap_is_busy_after_it() {
        let vfs = SimVfs::new(9662);
        let options = Options::default()
            .shards(1)
            .pin_threads(false)
            .memtable_budget(4 << 20)
            .write_stall_timeout(Duration::from_secs(10))
            .vfs(Arc::clone(&vfs) as _)
            .auto_recover(Recovery::Any);
        let db = Pigeonhole::open("/db/r9662.phdb", options).unwrap();
        let t = db
            .table("t")
            .unwrap()
            .family("f", Family::default())
            .create_if_missing()
            .unwrap();
        // Held until the refusals are in, so neither commit meets the closing engine.
        db.db
            .supervision()
            .unwrap()
            .test_hold
            .armed
            .store(true, Ordering::SeqCst);
        db.engine()
            .to_arc()
            .inject(pigeonhole_engine::Fault::NextApply, true);
        let e = t.mutate(b"x").put("f", b"q", b"1").commit().unwrap_err();
        assert_eq!(e.code(), ErrorCode::OutcomeUnknown);
        // A refusal the failed engine's close makes instead (a `Closed`, as a slow commit meets
        // on CI, #968's macOS flake) is mapped the same way: taken now, looked at after.
        let watch = db.db.watch();
        // Both reach the poisoned shard and are refused there, unlogged.
        let mut wb = db.write_batch();
        wb.put(&t, b"w", "f", b"q", b"2");
        let ticket = wb.commit_with_ticket(crate::Durability::Buffered).unwrap();
        #[cfg(feature = "async")]
        let fut = t.mutate(b"z").put("f", b"q", b"3").commit_async();
        // Let the supervisor go, and wait for the swap.
        db.db.supervision().unwrap().test_hold.release();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while db.recovery_stats().recoveries < 1 {
            assert!(
                std::time::Instant::now() < deadline,
                "{:?}",
                db.recovery_stats()
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        let e = ticket.wait().unwrap_err();
        assert_eq!(e.code(), ErrorCode::Busy, "ticket: {e}");
        let closed: crate::Error = pigeonhole_engine::Error::Closed.into();
        let e = watch.refused(closed);
        assert_eq!(e.code(), ErrorCode::Busy, "a Closed refusal: {e}");
        #[cfg(feature = "async")]
        {
            let e = crate::doc_support::block_on(fut).unwrap_err();
            assert_eq!(e.code(), ErrorCode::Busy, "async: {e}");
        }
        drop(t);
        db.close().unwrap();
    }

    /// #966: once the supervisor gave up, a commit is `Closed` (the recovery guide: "the
    /// handle stays failed exactly as with the option off (`is_failed()` true, commits
    /// `Closed`)"), also when it reaches the failed shard before the engine is marked dead
    /// and is refused there unlogged (the engine's `ShardFailed`, code `Io`). On Windows the
    /// budget's give-up, which is immediate, left that window open (#966).
    ///
    /// Deterministic: a failed apply (`Fault::NextApply`) poisons the shard without marking
    /// the engine dead, so after the fourth failure (past the budget of three) the next
    /// commit always passes the engine's open check and reaches the poisoned shard.
    #[test]
    fn a_commit_after_a_give_up_is_closed_even_when_the_failed_shard_refuses_it() {
        let vfs = SimVfs::new(9661);
        let options = Options::default()
            .shards(1)
            .pin_threads(false)
            .memtable_budget(4 << 20)
            .write_stall_timeout(Duration::from_secs(10))
            .vfs(Arc::clone(&vfs) as _)
            .auto_recover(Recovery::Any);
        let db = Pigeonhole::open("/db/r966.phdb", options).unwrap();
        let t = db
            .table("t")
            .unwrap()
            .family("f", Family::default())
            .create_if_missing()
            .unwrap();
        let fail = || {
            db.engine()
                .to_arc()
                .inject(pigeonhole_engine::Fault::NextApply, true);
            let e = t.mutate(b"x").put("f", b"q", b"1").commit().unwrap_err();
            assert_eq!(e.code(), ErrorCode::OutcomeUnknown);
        };
        for n in 1..=3 {
            fail();
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            while db.recovery_stats().recoveries < n {
                assert!(
                    std::time::Instant::now() < deadline,
                    "{:?}",
                    db.recovery_stats()
                );
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        fail();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !db.recovery_stats().gave_up {
            assert!(
                std::time::Instant::now() < deadline,
                "{:?}",
                db.recovery_stats()
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(db.is_failed());
        let e = t.mutate(b"y").put("f", b"q", b"2").commit().unwrap_err();
        assert_eq!(e.code(), ErrorCode::Closed, "{e}");
        // The same for a commit resolved after its call returned, which does not wait: a
        // ticket, and an async commit (the guide: "they return `Closed` or `Busy` at once").
        let mut wb = db.write_batch();
        wb.put(&t, b"w", "f", b"q", b"3");
        let e = wb
            .commit_with_ticket(crate::Durability::Buffered)
            .and_then(crate::CommitTicket::wait)
            .unwrap_err();
        assert_eq!(e.code(), ErrorCode::Closed, "ticket: {e}");
        assert!(e.to_string().contains("gave up"), "ticket: {e}");
        #[cfg(feature = "async")]
        {
            let fut = t.mutate(b"z").put("f", b"q", b"4").commit_async();
            let e = crate::doc_support::block_on(fut).unwrap_err();
            assert_eq!(e.code(), ErrorCode::Closed, "async: {e}");
        }
        drop(t);
        let _ = db.close();
    }

    /// D286 (perf287's review on #708): a Buffered cross-shard commit whose `Decide` went
    /// out before its DECISION was written, on a coordinator then poisoned before the
    /// DECISION's group, keeps its seqno held: nothing of it is visible, and the hold would
    /// stall every later commit's visibility in that instance. The poison's recovery swap
    /// ends that: the commit answers `OutcomeUnknown`, is absent after the swap, and a later
    /// commit on the other shard commits and is visible on the recovered engine.
    #[test]
    fn an_early_decided_commit_held_on_a_poisoned_coordinator_ends_with_the_swap() {
        let vfs = SimVfs::new(2860);
        let options = Options::default()
            .shards(2)
            .pin_threads(false)
            .memtable_budget(4 << 20)
            .write_stall_timeout(Duration::from_secs(10))
            .vfs(Arc::clone(&vfs) as _)
            .auto_recover(Recovery::Any);
        let db = Pigeonhole::open("/db/r2860.phdb", options).unwrap();
        // Two tables on different shards (a new table is one tablet on one shard).
        let mut tables: Vec<crate::Table> = Vec::new();
        for i in 0..64 {
            let t = db
                .table(&format!("t{i}"))
                .unwrap()
                .family("f", Family::default())
                .create_if_missing()
                .unwrap();
            if tables.iter().all(|o| o.shard_of(b"r") != t.shard_of(b"r")) {
                tables.push(t);
            }
            if tables.len() == 2 {
                break;
            }
        }
        let (a, b) = (tables[0].clone(), tables[1].clone());
        let engine = db.engine().to_arc();
        db.db
            .supervision()
            .unwrap()
            .test_hold
            .armed
            .store(true, Ordering::SeqCst);
        // A failed assertion releases the held DECISIONs as it unwinds: the close in the
        // database's drop would otherwise wait for them for ever, and the test hang (#998).
        struct ReleaseOnPanic(Arc<pigeonhole_engine::Engine>);
        impl Drop for ReleaseOnPanic {
            fn drop(&mut self) {
                if std::thread::panicking() {
                    self.0.release_held_decisions();
                }
            }
        }
        let _release = ReleaseOnPanic(Arc::clone(&engine));
        // The cross-shard commit, coordinated by `a`'s shard (its first row): its DECISION
        // is held, so it blocks with its shares applied and its seqno held.
        engine.inject(pigeonhole_engine::Fault::HoldDecisionRecords, true);
        let rounds = engine.metrics().cross_shard.rounds;
        let cross = {
            let (db, a, b) = (db.clone(), a.clone(), b.clone());
            std::thread::spawn(move || {
                let mut wb = db.write_batch();
                wb.put(&a, b"x", "f", b"q", b"v");
                wb.put(&b, b"x", "f", b"q", b"v");
                wb.commit_with(crate::Durability::Buffered)
            })
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while engine.metrics().cross_shard.rounds == rounds {
            assert!(std::time::Instant::now() < deadline, "never decided");
            std::thread::sleep(Duration::from_millis(1));
        }
        // The coordinator fails on an unrelated single (its apply), then the DECISION is
        // let go: it reaches a poisoned shard and settles unlogged. Only a single's apply
        // takes the fault: `rounds` counts at the decision, before the shares apply, and a
        // share taking it would poison a shard while `s` applied and waited for ever to be
        // visible behind the held seqno (#998).
        engine.inject(pigeonhole_engine::Fault::NextSingleApply, true);
        let e = a.mutate(b"s").put("f", b"q", b"s").commit().unwrap_err();
        assert_eq!(e.code(), ErrorCode::OutcomeUnknown);
        engine.release_held_decisions();
        let e = cross.join().unwrap().unwrap_err();
        assert_eq!(e.code(), ErrorCode::OutcomeUnknown);
        assert_eq!(engine.abort_decides_sent(a.shard_of(b"r").unwrap()), 0);
        // Let the supervisor go, and wait for the swap.
        db.db.supervision().unwrap().test_hold.release();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while db.recovery_stats().recoveries < 1 || db.is_failed() {
            assert!(
                std::time::Instant::now() < deadline,
                "{:?}",
                db.recovery_stats()
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        // The early-decided commit is absent; a later commit on the other shard is visible.
        assert!(a.get(b"x", "f", b"q").unwrap().is_none());
        assert!(b.get(b"x", "f", b"q").unwrap().is_none());
        b.mutate(b"later").put("f", b"q", b"l").commit().unwrap();
        assert!(b.get(b"later", "f", b"q").unwrap().is_some());
        drop((a, b, tables));
        db.close().unwrap();
    }
}
