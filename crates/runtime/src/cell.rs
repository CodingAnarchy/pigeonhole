//! A shard's state behind its run token (#1176, D198's C2; the design is revision 3.1 of
//! #1176's note).
//!
//! An engine-owned shard's [`ShardCore`] lives in a [`ShardCell`], and only the thread that
//! holds the cell's run token may touch it: its driver, for a turn, or (with caller-runs on)
//! a committing client, for a caller's turn. The token is the cell's mutex, taken only with
//! `try_lock`: nothing ever waits for it, and a thread that finds it held moves on. So it is
//! the outermost entry in the lock order (docs/design/concurrency.md) and never held across
//! a wait on another shard. Its lock and unlock order each turn's use of the core after the
//! previous holder's.
//!
//! - **Failed:** a turn that panics poisons the mutex, and a poisoned token is never taken
//!   again: no thread runs a core whose turn unwound halfway.
//! - **Closed:** shutdown takes the core out of the cell (`ShardCell::close`), after which
//!   the token, though free, holds nothing.
//!
//! Beside the mutex, the run word is an advisory copy of who holds the token, on a cache
//! line of its own (#1176 rev 3.1, C3): polling clients load it before they try the lock,
//! so they don't touch the mutex while a turn runs, and the lines the inbox and wakeup flags
//! share stay free of their traffic.
//!
//! Application-owned shards keep their plain `ShardCore`: their loop is the application's,
//! and caller-runs is engine-owned only. So do engine-owned shards with caller-runs off
//! (D321): a driver holds its shards as a [`DriverShard`], chosen by type when the runtime
//! starts, and the plain `ShardCore` one is the driver loop as it was before caller-runs,
//! with no token, no backstop and no branch on the option (zero off-cost by type).

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering, fence};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

use crate::signal::Signal;
use crate::{Inbox, ShardCore, ShardHandler, ShardId};

/// How long `ShardCell::check_backstop` waits for a notify already under way (a producer
/// between its publish and its notify) before it counts a lost wake.
const BACKSTOP_GRACE: std::time::Duration = std::time::Duration::from_millis(1);

/// No thread holds the token.
pub(crate) const FREE: u8 = 0;
/// The shard's driver holds it, for a turn.
pub(crate) const DRIVER: u8 = 1;
/// A committing client holds it, for a caller's turn (#1176).
pub(crate) const CALLER: u8 = 2;
/// Terminal: a turn panicked and poisoned the token.
pub(crate) const FAILED: u8 = 3;
/// Terminal: shutdown took the core out (`ShardCell::close`).
pub(crate) const CLOSED: u8 = 4;

/// The run word and its flags, on a line of their own.
#[repr(align(128))]
struct RunWord {
    /// Who holds the token, advisory: the mutex decides.
    state: AtomicU8,
    /// The driver has work a caller's turn may not run (#1176 rev 3.1, C2): callers leave
    /// the token alone while it is set. Set by a caller's turn that stopped at a message it
    /// may not run, and by a driver turn that left work; cleared when the driver takes the
    /// token to look at the shard (`ShardCell::driver_look`).
    driver_wants: AtomicBool,
    /// The driver found the token held by a caller and went on without the shard (#1176's
    /// skip-then-notify pair): the caller's release, which loads it after its fence, wakes
    /// the driver. Cleared only when the driver takes the token to look at the shard
    /// (`ShardCell::driver_look`): a take that doesn't look must leave it.
    skipped: AtomicBool,
    /// Time callers' turns spent on the shard since the hosting driver last collected it
    /// (rev 3.1: charged to that driver's busy time, so the controller sizes it for the
    /// moments it takes over).
    caller_busy: AtomicU64,
    /// Lost wakes the caller-runs park backstop (`CALLER_RUNS_PARK_BACKSTOP`) recovered on
    /// this shard: when the hosting driver's park ran out the backstop, the shard had work
    /// and no notify had found it announced asleep, even after a grace for one in flight
    /// (`ShardCell::check_backstop`). Should stay 0. Written by the hosting driver only;
    /// read through the inbox's caller entry, so 0 with caller-runs off.
    backstop_wakes: AtomicU64,
}

/// A shard a committing client may run (#1176): the runtime's entry, on the shard's inbox.
pub(crate) trait CallerTurn: Send + Sync {
    /// Runs a caller's turn if the token is free and the driver doesn't want it. Returns
    /// whether a turn ran.
    fn try_run(&self, own_done: &mut dyn FnMut() -> bool) -> bool;

    /// Lost wakes the park backstop recovered on the shard (see `RunWord::backstop_wakes`).
    /// Relaxed.
    fn backstop_wakes(&self) -> u64;
}

/// An engine-owned shard: its loop state behind the run token.
pub(crate) struct ShardCell<H: ShardHandler> {
    run: RunWord,
    id: ShardId,
    /// The shard's wakeup signal, readable without the token.
    signal: Arc<Signal>,
    /// `None` once shutdown took the core out (which also drops the core's submitters,
    /// breaking the inbox's reference back to this cell).
    core: Mutex<Option<ShardCore<H>>>,
}

impl<H: ShardHandler> ShardCell<H> {
    pub(crate) fn new(core: ShardCore<H>) -> Self {
        let id = core.id;
        let signal = Arc::clone(&core.submitters[usize::from(id.0)].inbox.signal);
        Self {
            run: RunWord {
                state: AtomicU8::new(FREE),
                driver_wants: AtomicBool::new(false),
                skipped: AtomicBool::new(false),
                caller_busy: AtomicU64::new(0),
                backstop_wakes: AtomicU64::new(0),
            },
            id,
            signal,
            core: Mutex::new(Some(core)),
        }
    }

    pub(crate) fn id(&self) -> ShardId {
        self.id
    }

    /// The shard's wakeup signal, without the token.
    pub(crate) fn signal(&self) -> &Signal {
        &self.signal
    }

    /// Takes the token for `holder` if no thread holds it and the shard is neither failed
    /// nor closed. Never waits.
    fn try_take(&self, holder: u8) -> Option<Turn<'_, H>> {
        let guard = match self.core.try_lock() {
            Ok(g) => g,
            // Held by another thread, or poisoned by a turn that panicked (terminal).
            Err(TryLockError::WouldBlock | TryLockError::Poisoned(_)) => return None,
        };
        guard.as_ref()?;
        self.run.state.store(holder, Ordering::Relaxed);
        Some(Turn { cell: self, guard })
    }

    /// The driver's take, without waiting. When a caller holds the token, the driver marks
    /// the shard skipped, fences, and tries once more: either it gets the token now (the
    /// caller released in between), or the caller's release sees `skipped` and wakes it
    /// (the Dekker pair of #1176's note, §4).
    pub(crate) fn driver(&self) -> Option<Turn<'_, H>> {
        if let Some(t) = self.try_take(DRIVER) {
            return Some(t);
        }
        self.run.skipped.store(true, Ordering::Relaxed);
        fence(Ordering::SeqCst);
        self.try_take(DRIVER)
    }

    /// [`ShardCell::driver`] for a take that looks at the shard's work (a turn, the spin's
    /// looks, its last check before sleeping): clears `skipped` and `driver_wants`, which
    /// that look answers. Only such a take may: one that doesn't look (`next_deadline`, the
    /// spin's bookkeeping, a move) leaves them, or a caller's release that loads `skipped`
    /// after it finds it clear and doesn't notify, while a message queued during its turn
    /// waits unread with the driver parked (#1502's box run; the loom model
    /// `loom_a_take_that_does_not_look_keeps_the_skip_handshake`).
    pub(crate) fn driver_look(&self) -> Option<Turn<'_, H>> {
        let t = self.driver()?;
        self.run.skipped.store(false, Ordering::Relaxed);
        self.run.driver_wants.store(false, Ordering::Relaxed);
        Some(t)
    }

    /// The driver's take where it must have the shard (start, a move, close): waits out a
    /// caller's turn, which is bounded (`CALLER_BATCH` messages, no blocking I/O). `None`
    /// only for a failed shard.
    pub(crate) fn driver_wait(&self) -> Option<Turn<'_, H>> {
        loop {
            if let Some(t) = self.try_take(DRIVER) {
                return Some(t);
            }
            if self.core.is_poisoned() {
                return None;
            }
            std::thread::yield_now();
        }
    }

    /// The driver has work a caller may not take (rev 3.1, C2).
    pub(crate) fn want(&self) {
        self.run.driver_wants.store(true, Ordering::Relaxed);
    }

    /// The hosting driver's park timed out (the caller-runs backstop, or a sleeping task's
    /// deadline): counts a lost wake if the shard has work, other than what the deadline was
    /// for, that no notify signalled. `wakes_before` is the signal's wake count
    /// (`Signal::idle_counts`) read before the park.
    ///
    /// A notify that finds the shard announced asleep counts a wake as it wakes the thread,
    /// so a count that moved since `wakes_before` means the wake was delivered: it only
    /// landed after the timer had already fired, which the elapsed time alone can't tell
    /// apart (#1502's re-run counted those; the cross-shard stress test reproduced them, every
    /// one with its wake counted). Work found with the count unchanged, and still unchanged
    /// after a grace for a producer between its publish and its notify, is work whose wake
    /// was lost. A caller holding the token is left alone: its release looks at the shard.
    #[cold]
    pub(crate) fn check_backstop(&self, wakes_before: u64) {
        let wakes = || self.signal.idle_counts().1;
        if wakes() != wakes_before {
            return;
        }
        if !self.try_take(DRIVER).is_some_and(|mut t| t.has_work()) {
            return;
        }
        let grace = std::time::Instant::now() + BACKSTOP_GRACE;
        while std::time::Instant::now() < grace {
            if wakes() != wakes_before {
                return;
            }
            std::thread::yield_now();
        }
        if wakes() == wakes_before {
            self.run.backstop_wakes.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Callers' turn time since the last call, for the hosting driver's busy count.
    pub(crate) fn take_caller_busy(&self) -> u64 {
        if self.run.caller_busy.load(Ordering::Relaxed) == 0 {
            return 0;
        }
        self.run.caller_busy.swap(0, Ordering::Relaxed)
    }

    /// Runs `f` on the shard's inbox (its caller-run entry is set there at start).
    pub(crate) fn with_inbox(&self, f: impl FnOnce(&Inbox<H::Msg>)) {
        if let Some(core) = self.driver_wait() {
            f(core.inbox());
        }
    }

    /// Shutdown: takes the core out for good, once any caller's turn ends. The cell holds
    /// nothing from then on. `None` for a shard whose turn panicked.
    pub(crate) fn close(&self) -> Option<ShardCore<H>> {
        let mut turn = self.driver_wait()?;
        let core = turn.guard.take();
        // The empty core refuses every later take; the run word says why.
        drop(turn);
        self.run.state.store(CLOSED, Ordering::Release);
        core
    }
}

impl<H: ShardHandler> CallerTurn for ShardCell<H> {
    fn backstop_wakes(&self) -> u64 {
        self.run.backstop_wakes.load(Ordering::Relaxed)
    }

    fn try_run(&self, own_done: &mut dyn FnMut() -> bool) -> bool {
        let run = &self.run;
        if run.state.load(Ordering::Relaxed) != FREE || run.driver_wants.load(Ordering::Relaxed) {
            return false;
        }
        let Some(mut turn) = self.try_take(CALLER) else {
            return false;
        };
        let vfs = Arc::clone(&turn.vfs);
        let start = vfs.monotonic_nanos();
        // No ring: any I/O the turn starts completes inline (#1176 rev 3, §4).
        let (notify, handled) =
            pigeonhole_io::inline_io(|| turn.caller_turn(own_done, &run.driver_wants));
        let spent = vfs.monotonic_nanos().saturating_sub(start);
        self.signal.add_busy(spent);
        if handled > 0 {
            self.signal.add_caller_turn();
        }
        run.caller_busy.fetch_add(spent, Ordering::Relaxed);
        // Work the turn left for the driver (a refused message, a task it spawned or woke, a
        // deadline it moved) is flagged as a woken task before the release. The notify
        // below wakes the driver only while it's announced asleep, and a driver polling
        // before it parks (`spin_poll`) is announced awake: its looks at the shard, and its
        // last check before sleeping, read `task_woken`, so they see the flag once they take
        // the token, and the turn they then run finds the work (#1502 Phase B's
        // cross-shard-contention hang: the notify alone was lost there, and the driver
        // parked untimed).
        if notify {
            self.signal.mark_task_woken();
        }
        // Release, then the re-check: whatever the driver must see (work left, a task woken,
        // a deadline moved, a driver that skipped the shard meanwhile) wakes it.
        drop(turn);
        fence(Ordering::SeqCst);
        if notify || run.skipped.load(Ordering::Relaxed) || self.signal.task_woken() {
            self.signal.notify();
        }
        true
    }
}

/// How a driver holds a shard (D321): plainly, when caller-runs is off and nothing but the
/// driver may run it, or in a [`ShardCell`] behind the run token. The runtime picks one at
/// start for every driver, so the driver loop is compiled once per kind, and the plain kind
/// is main's loop exactly: its takes are a borrow, and the caller-runs steps, on
/// [`CALLER_RUNS`](Self::CALLER_RUNS), are compiled out.
pub(crate) trait DriverShard<H: ShardHandler>: Send + Sized + 'static {
    /// Clients may run the shard's turns: the token, the skip handshake and the backstop.
    const CALLER_RUNS: bool;

    /// A driver's hold on the shard's core.
    type Turn<'a>: DriverTurn<H>
    where
        Self: 'a;

    fn id(&self) -> ShardId;

    /// The shard's wakeup signal, without a turn.
    fn signal(&self) -> &Signal;

    /// A take that looks at the shard's work ([`ShardCell::driver_look`]).
    fn look(&mut self) -> Option<Self::Turn<'_>>;

    /// A take that doesn't look ([`ShardCell::driver`]).
    fn take(&mut self) -> Option<Self::Turn<'_>>;

    /// A take that must have the shard: start, a move, close ([`ShardCell::driver_wait`]).
    fn wait(&mut self) -> Option<Self::Turn<'_>>;

    /// Callers' turn time since the last call ([`ShardCell::take_caller_busy`]).
    fn take_caller_busy(&self) -> u64;

    /// After a park that timed out ([`ShardCell::check_backstop`]).
    fn check_backstop(&self, wakes_before: u64);

    /// Shutdown: the core, out for good ([`ShardCell::close`]).
    fn close(self) -> Option<ShardCore<H>>;

    /// For a move to another driver, through its handoff.
    fn into_handed(self) -> Handed<H>;

    /// A shard handed to this driver: always of this runtime's kind.
    fn from_handed(h: Handed<H>) -> Self;
}

/// A driver's turn on a shard: the core, and the token's flags.
pub(crate) trait DriverTurn<H: ShardHandler>: DerefMut<Target = ShardCore<H>> {
    /// The driver has work a caller may not take ([`ShardCell::want`]).
    fn want(&self);
}

/// A shard on its way to another driver (a move is rare: boxed, it keeps the handoff small).
pub(crate) enum Handed<H: ShardHandler> {
    Plain(Box<ShardCore<H>>),
    Cell(Arc<ShardCell<H>>),
}

impl<H: ShardHandler> DriverShard<H> for ShardCore<H> {
    const CALLER_RUNS: bool = false;
    type Turn<'a> = &'a mut ShardCore<H>;

    fn id(&self) -> ShardId {
        self.id
    }

    fn signal(&self) -> &Signal {
        ShardCore::signal(self)
    }

    #[inline(always)]
    fn look(&mut self) -> Option<&mut ShardCore<H>> {
        Some(self)
    }

    #[inline(always)]
    fn take(&mut self) -> Option<&mut ShardCore<H>> {
        Some(self)
    }

    #[inline(always)]
    fn wait(&mut self) -> Option<&mut ShardCore<H>> {
        Some(self)
    }

    fn take_caller_busy(&self) -> u64 {
        0
    }

    fn check_backstop(&self, _wakes_before: u64) {}

    fn close(self) -> Option<ShardCore<H>> {
        Some(self)
    }

    fn into_handed(self) -> Handed<H> {
        Handed::Plain(Box::new(self))
    }

    fn from_handed(h: Handed<H>) -> Self {
        match h {
            Handed::Plain(core) => *core,
            Handed::Cell(_) => unreachable!("a runtime's drivers all hold one kind of shard"),
        }
    }
}

impl<H: ShardHandler> DriverTurn<H> for &mut ShardCore<H> {
    #[inline(always)]
    fn want(&self) {}
}

impl<H: ShardHandler> DriverShard<H> for Arc<ShardCell<H>> {
    const CALLER_RUNS: bool = true;
    type Turn<'a> = Turn<'a, H>;

    fn id(&self) -> ShardId {
        ShardCell::id(self)
    }

    fn signal(&self) -> &Signal {
        ShardCell::signal(self)
    }

    fn look(&mut self) -> Option<Turn<'_, H>> {
        self.driver_look()
    }

    fn take(&mut self) -> Option<Turn<'_, H>> {
        self.driver()
    }

    fn wait(&mut self) -> Option<Turn<'_, H>> {
        self.driver_wait()
    }

    fn take_caller_busy(&self) -> u64 {
        ShardCell::take_caller_busy(self)
    }

    fn check_backstop(&self, wakes_before: u64) {
        ShardCell::check_backstop(self, wakes_before);
    }

    fn close(self) -> Option<ShardCore<H>> {
        ShardCell::close(&self)
    }

    fn into_handed(self) -> Handed<H> {
        Handed::Cell(self)
    }

    fn from_handed(h: Handed<H>) -> Self {
        match h {
            Handed::Cell(cell) => cell,
            Handed::Plain(_) => unreachable!("a runtime's drivers all hold one kind of shard"),
        }
    }
}

impl<H: ShardHandler> DriverTurn<H> for Turn<'_, H> {
    fn want(&self) {
        self.cell.want();
    }
}

/// A turn's hold on its shard: the core, until it drops and frees the token.
pub(crate) struct Turn<'a, H: ShardHandler> {
    cell: &'a ShardCell<H>,
    /// Holds `Some` core: `try_take` checked it (`close` takes it out).
    guard: MutexGuard<'a, Option<ShardCore<H>>>,
}

impl<H: ShardHandler> Deref for Turn<'_, H> {
    type Target = ShardCore<H>;

    fn deref(&self) -> &ShardCore<H> {
        self.guard.as_ref().expect("a turn holds its core")
    }
}

impl<H: ShardHandler> DerefMut for Turn<'_, H> {
    fn deref_mut(&mut self) -> &mut ShardCore<H> {
        self.guard.as_mut().expect("a turn holds its core")
    }
}

impl<H: ShardHandler> Drop for Turn<'_, H> {
    fn drop(&mut self) {
        // The advisory word first, then the unlock (the guard's own drop, after this).
        if std::thread::panicking() {
            // A turn that unwinds poisons the token for good, and the core goes with it,
            // here, while the thread still unwinds: its handler reports the panic as it
            // drops (the engine's failure record), and the replies it held drop with it,
            // so their waiters resolve rather than wait on a core no thread may run again.
            // Before the token, a shard thread's panic dropped its core the same way; the
            // cell, which the inbox's caller-run entry keeps alive, must not hold it.
            self.cell.run.state.store(FAILED, Ordering::Release);
            drop(self.guard.take());
        } else {
            self.cell.run.state.store(FREE, Ordering::Release);
        }
    }
}

/// The skip-then-notify handshake (#1176 rev 3.1, §4) under loom. Loom can't run a whole
/// shard (its queue is std's channel), so this models the cell's exact operations and
/// orderings around the shipped `Signal`: the token as the CAS `try_lock` is (Acquire on
/// take, Release on release), `skipped`, and the driver's order on a failed take (announce
/// sleep, then mark skipped, fence and retry) against the caller's release (Free, fence, load
/// `skipped`, notify). A message submitted while the caller holds the token, after its last
/// look at the queue, must be taken by the driver's retry or wake the driver; never neither.
#[cfg(all(test, loom))]
mod loom_tests {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering, fence};
    use loom::thread;

    use crate::signal::{Signal, WakeTarget};

    struct Model {
        token: AtomicBool,
        skipped: AtomicBool,
        queued: AtomicBool,
        handled: AtomicBool,
        woken: Arc<AtomicBool>,
        signal: Signal,
    }

    impl Model {
        fn try_take(&self) -> bool {
            self.token
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
        }

        fn release(&self) {
            self.token.store(false, Ordering::Release);
        }

        /// The driver's look at the queue with the token held (`check_before_sleep`).
        fn drain(&self) {
            if self.queued.swap(false, Ordering::Relaxed) {
                self.handled.store(true, Ordering::Relaxed);
            }
        }
    }

    #[test]
    fn loom_a_message_queued_during_a_callers_turn_reaches_the_driver() {
        loom::model(|| {
            let woken = Arc::new(AtomicBool::new(false));
            let m = Arc::new(Model {
                // The caller holds the token, past its last look at the queue.
                token: AtomicBool::new(true),
                skipped: AtomicBool::new(false),
                queued: AtomicBool::new(false),
                handled: AtomicBool::new(false),
                woken: Arc::clone(&woken),
                signal: Signal::new(),
            });
            {
                let woken = Arc::clone(&woken);
                m.signal
                    .set_target(WakeTarget::Callback(std::sync::Arc::new(move || {
                        woken.store(true, Ordering::SeqCst);
                    })));
            }
            // A submitter: publishes, then notifies (`Submitter::submit`).
            let submitter = {
                let m = Arc::clone(&m);
                thread::spawn(move || {
                    m.queued.store(true, Ordering::Release);
                    m.signal.notify();
                })
            };
            // The caller's release, with nothing left of its own (`CallerTurn::try_run`).
            let caller = {
                let m = Arc::clone(&m);
                thread::spawn(move || {
                    m.release();
                    fence(Ordering::SeqCst);
                    if m.skipped.load(Ordering::Relaxed) {
                        m.signal.notify();
                    }
                })
            };
            // The driver, awake, looks once: a failed take announces sleep, then marks the
            // shard skipped, fences and retries (`ShardCell::driver`).
            let mut asleep = false;
            if m.try_take() {
                m.drain();
                m.release();
            } else {
                m.signal.prepare_sleep();
                m.skipped.store(true, Ordering::Relaxed);
                fence(Ordering::SeqCst);
                if m.try_take() {
                    m.drain();
                    m.release();
                } else {
                    asleep = true;
                }
            }
            submitter.join().unwrap();
            caller.join().unwrap();
            // A driver that went to sleep must have been woken, or the message must have
            // been taken already: then its next turn takes what is queued.
            let pending = m.queued.load(Ordering::Relaxed);
            if pending {
                assert!(
                    !asleep || m.woken.load(Ordering::SeqCst),
                    "a message waits with the driver asleep and unwoken"
                );
            }
            let _ = m.handled.load(Ordering::Relaxed);
        });
    }

    /// A driver take that doesn't look at the shard (`next_deadline`, the spin's `due` and
    /// `record`, the gap window's setup) after the sleep handshake found the token held: the
    /// handshake's `skipped` must survive it, or the caller's release, loading `skipped` after
    /// that take cleared it, doesn't notify, and a message submitted while the driver polled
    /// awake (its notify a no-op) waits with the driver parked.
    #[test]
    fn loom_a_take_that_does_not_look_keeps_the_skip_handshake() {
        loom::model(|| {
            let woken = Arc::new(AtomicBool::new(false));
            let m = Arc::new(Model {
                token: AtomicBool::new(true),
                skipped: AtomicBool::new(false),
                queued: AtomicBool::new(false),
                handled: AtomicBool::new(false),
                woken: Arc::clone(&woken),
                signal: Signal::new(),
            });
            {
                let woken = Arc::clone(&woken);
                m.signal
                    .set_target(WakeTarget::Callback(std::sync::Arc::new(move || {
                        woken.store(true, Ordering::SeqCst);
                    })));
            }
            // Polling (`spin_poll`): announced awake.
            m.signal.awake();
            let submitter = {
                let m = Arc::clone(&m);
                thread::spawn(move || {
                    m.queued.store(true, Ordering::Release);
                    m.signal.notify();
                })
            };
            let caller = {
                let m = Arc::clone(&m);
                thread::spawn(move || {
                    m.release();
                    fence(Ordering::SeqCst);
                    if m.skipped.load(Ordering::Relaxed) {
                        m.signal.notify();
                    }
                })
            };
            // The poll's last check, as `spin_poll`: the token held, announce sleep, then the
            // driver's take (skipped, fence, retry).
            let mut hit = false;
            m.signal.prepare_sleep();
            if m.try_take() {
                m.drain();
                hit = m.handled.load(Ordering::Relaxed);
                m.release();
            } else {
                m.skipped.store(true, Ordering::Relaxed);
                fence(Ordering::SeqCst);
                if m.try_take() {
                    m.drain();
                    hit = m.handled.load(Ordering::Relaxed);
                    m.release();
                }
            }
            // A take that doesn't look (`record`, `next_deadline`): as `ShardCell::driver`,
            // leaving `skipped` (clearing it here fails this model: the lost wake).
            if !hit && m.try_take() {
                m.release();
            }
            submitter.join().unwrap();
            caller.join().unwrap();
            if m.queued.load(Ordering::Relaxed) {
                assert!(
                    hit || m.woken.load(Ordering::SeqCst),
                    "a message waits with the driver parked and unwoken"
                );
            }
        });
    }

    /// The driver's poll before it parks (`spin_poll`) against a caller's turn that leaves
    /// work only a driver turn runs (a task its batch spawned, #1502 Phase B's hang). The
    /// poll announces the shard awake, so the caller's notify alone may find it awake and
    /// do nothing; the poll's looks and its last check before sleeping must then see the
    /// work through `task_woken`, which the caller sets before it releases the token.
    /// Without that flag, the interleaving "awake; the caller releases and notifies; the
    /// driver's looks find nothing; it announces sleep and parks" leaves the work with no
    /// thread to run it.
    #[test]
    fn loom_work_a_callers_turn_leaves_reaches_a_polling_driver() {
        loom::model(|| {
            let woken = Arc::new(AtomicBool::new(false));
            let m = Arc::new(Model {
                // The caller holds the token, in its turn.
                token: AtomicBool::new(true),
                skipped: AtomicBool::new(false),
                queued: AtomicBool::new(false),
                handled: AtomicBool::new(false),
                woken: Arc::clone(&woken),
                signal: Signal::new(),
            });
            {
                let woken = Arc::clone(&woken);
                m.signal
                    .set_target(WakeTarget::Callback(std::sync::Arc::new(move || {
                        woken.store(true, Ordering::SeqCst);
                    })));
            }
            // The caller's turn leaves work, then releases (`CallerTurn::try_run` with
            // `notify` set): flag, release, fence, notify.
            let caller = {
                let m = Arc::clone(&m);
                thread::spawn(move || {
                    m.signal.mark_task_woken();
                    m.release();
                    fence(Ordering::SeqCst);
                    let _ = m.skipped.load(Ordering::Relaxed);
                    m.signal.notify();
                })
            };
            // The driver's take, as `ShardCell::driver`: on failure, mark skipped, fence and
            // retry.
            let driver = |m: &Model| {
                if m.try_take() {
                    return true;
                }
                m.skipped.store(true, Ordering::Relaxed);
                fence(Ordering::SeqCst);
                m.try_take()
            };
            // `spin_poll`: awake, one look (the poll loop's: `task_woken` with the token),
            // then on a miss the sleep handshake (`check_before_sleep`, or announce sleep and
            // retry the take when a caller holds it).
            m.signal.awake();
            let mut hit = false;
            if driver(&m) {
                hit = m.signal.task_woken();
                m.release();
            }
            if !hit {
                if driver(&m) {
                    m.signal.prepare_sleep();
                    hit = m.signal.task_woken();
                    m.release();
                } else {
                    m.signal.prepare_sleep();
                    if driver(&m) {
                        m.signal.prepare_sleep();
                        hit = m.signal.task_woken();
                        m.release();
                    }
                }
            }
            caller.join().unwrap();
            // The driver either found the work (its next turn runs it) or, parking, was
            // woken.
            assert!(
                hit || m.woken.load(Ordering::SeqCst),
                "a caller's turn left work with the driver parked and unwoken"
            );
        });
    }
}
