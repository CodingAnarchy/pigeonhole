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
//! and caller-runs is engine-owned only.

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering, fence};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

use crate::signal::Signal;
use crate::{Inbox, ShardCore, ShardHandler, ShardId};

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
    /// token.
    driver_wants: AtomicBool,
    /// The driver found the token held by a caller and went on without the shard (#1176's
    /// skip-then-notify pair): the caller's release, which loads it after its fence, wakes
    /// the driver. Cleared when the driver takes the token.
    skipped: AtomicBool,
    /// Time callers' turns spent on the shard since the hosting driver last collected it
    /// (rev 3.1: charged to that driver's busy time, so the controller sizes it for the
    /// moments it takes over).
    caller_busy: AtomicU64,
}

/// A shard a committing client may run (#1176): the runtime's entry, on the shard's inbox.
pub(crate) trait CallerTurn: Send + Sync {
    /// Runs a caller's turn if the token is free and the driver doesn't want it. Returns
    /// whether a turn ran.
    fn try_run(&self, own_done: &mut dyn FnMut() -> bool) -> bool;
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
        if holder == DRIVER {
            self.run.skipped.store(false, Ordering::Relaxed);
            self.run.driver_wants.store(false, Ordering::Relaxed);
        }
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
        let notify = pigeonhole_io::inline_io(|| turn.caller_turn(own_done, &run.driver_wants));
        let spent = vfs.monotonic_nanos().saturating_sub(start);
        self.signal.add_busy(spent);
        self.signal.add_caller_turn();
        run.caller_busy.fetch_add(spent, Ordering::Relaxed);
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
        // The advisory word first, then the unlock (the guard's own drop, after this). A
        // turn that unwinds poisons the token for good; the word says so.
        let state = if std::thread::panicking() {
            FAILED
        } else {
            FREE
        };
        self.cell.run.state.store(state, Ordering::Release);
    }
}
