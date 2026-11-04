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
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Mutex, MutexGuard, TryLockError};

use crate::signal::Signal;
use crate::{ShardCore, ShardHandler, ShardId, Submitter};

/// No thread holds the token.
pub(crate) const FREE: u8 = 0;
/// The shard's driver holds it, for a turn.
pub(crate) const DRIVER: u8 = 1;
/// A committing client holds it, for a caller's turn (#1176).
#[allow(dead_code)] // Taken by the caller's turn, the next step of #1176.
pub(crate) const CALLER: u8 = 2;
/// Terminal: a turn panicked and poisoned the token.
pub(crate) const FAILED: u8 = 3;
/// Terminal: shutdown took the core out (`ShardCell::close`).
pub(crate) const CLOSED: u8 = 4;

/// The run word and its flag, on a line of their own.
#[repr(align(128))]
struct RunWord {
    /// Who holds the token, advisory: the mutex decides.
    state: AtomicU8,
    /// The driver has work a caller's turn may not run (#1176 rev 3.1, C2): callers leave
    /// the token alone while it is set. Used from the caller's turn on.
    #[allow(dead_code)]
    driver_wants: AtomicBool,
}

/// An engine-owned shard: its loop state behind the run token.
pub(crate) struct ShardCell<H: ShardHandler> {
    run: RunWord,
    id: ShardId,
    /// The shard's own submitter: its inbox and wakeup signal, readable without the token.
    submitter: Submitter<H::Msg>,
    /// `None` once shutdown took the core out.
    core: Mutex<Option<ShardCore<H>>>,
}

impl<H: ShardHandler> ShardCell<H> {
    pub(crate) fn new(core: ShardCore<H>) -> Self {
        let id = core.id;
        let submitter = core.submitters[usize::from(id.0)].clone();
        Self {
            run: RunWord {
                state: AtomicU8::new(FREE),
                driver_wants: AtomicBool::new(false),
            },
            id,
            submitter,
            core: Mutex::new(Some(core)),
        }
    }

    pub(crate) fn id(&self) -> ShardId {
        self.id
    }

    /// The shard's submitter (its inbox and signal), without the token.
    #[allow(dead_code)] // The caller's turn submits through it (the next step of #1176).
    pub(crate) fn submitter(&self) -> &Submitter<H::Msg> {
        &self.submitter
    }

    /// The shard's wakeup signal, without the token.
    pub(crate) fn signal(&self) -> &Signal {
        &self.submitter.inbox.signal
    }

    /// Takes the token for `holder` if no thread holds it and the shard is neither failed
    /// nor closed. Never waits.
    pub(crate) fn try_take(&self, holder: u8) -> Option<Turn<'_, H>> {
        debug_assert!(holder == DRIVER || holder == CALLER);
        let guard = match self.core.try_lock() {
            Ok(g) => g,
            // Held by another thread, or poisoned by a turn that panicked (terminal).
            Err(TryLockError::WouldBlock | TryLockError::Poisoned(_)) => return None,
        };
        guard.as_ref()?;
        self.run.state.store(holder, Ordering::Relaxed);
        Some(Turn { cell: self, guard })
    }

    /// The driver's take. Until callers run turns (the next step of #1176) nothing else ever
    /// holds the token, so it is always free here.
    pub(crate) fn driver(&self) -> Turn<'_, H> {
        self.try_take(DRIVER)
            .expect("a shard's token is held while only its driver runs it")
    }

    /// Shutdown: takes the core out for good. The cell holds nothing from then on.
    pub(crate) fn close(&self) -> ShardCore<H> {
        let mut guard = self
            .core
            .try_lock()
            .expect("a shard's token is held while its driver closes it");
        let core = guard.take().expect("a shard is closed once");
        // The empty core refuses every later take; the run word says why.
        self.run.state.store(CLOSED, Ordering::Relaxed);
        core
    }
}

/// A turn's hold on its shard: the core, until it drops and frees the token.
pub(crate) struct Turn<'a, H: ShardHandler> {
    cell: &'a ShardCell<H>,
    /// Holds `Some` core: `try_take` checked it.
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
