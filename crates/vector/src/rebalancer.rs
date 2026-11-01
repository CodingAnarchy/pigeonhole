//! Background rebalancing (D306 §3, the owner's §6.4 call): with
//! [`Rebalance::Background`](crate::Rebalance::Background), inserts and deletes only mark
//! the lists that left their bounds, and a [`Rebalancer`] splits and merges them. The crate
//! runs one on a thread of its own ([`Index::open`](crate::Index::open)), or hands it to the
//! application to run on a thread of its choosing
//! ([`Index::open_with_rebalancer`](crate::Index::open_with_rebalancer)), as D284 hands out
//! `BackgroundWorker`s.

use std::sync::{Arc, Condvar, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

use crate::Result;
use crate::index::{Index, Inner};

type Wakeup = Arc<dyn Fn() + Send + Sync>;

#[derive(Debug, Default)]
struct State {
    /// Work was queued since the last wait.
    notified: bool,
    /// Every `Index` handle is gone.
    closed: bool,
}

/// Between an index's writers and its rebalancer: the writers say when they queue work,
/// and the index's last handle says when it's gone.
#[derive(Default)]
pub(crate) struct Signal {
    state: Mutex<State>,
    cv: Condvar,
    wakeup: Mutex<Option<Wakeup>>,
}

impl Signal {
    /// Work was queued: wakes a waiting rebalancer, and calls the application's wakeup.
    pub(crate) fn notify(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .notified = true;
        self.cv.notify_all();
        self.wake();
    }

    /// Calls the application's wakeup, outside the lock that holds it (cli692 on #1271): a
    /// callback that sets another wakeup, or blocks, can't deadlock a writer.
    fn wake(&self) {
        let f = self
            .wakeup
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(f) = f {
            f();
        }
    }

    /// The index's last handle dropped.
    pub(crate) fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closed = true;
        self.cv.notify_all();
        self.wake();
    }

    fn wait(&self, timeout: Duration) {
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !s.notified && !s.closed {
            s = self
                .cv
                .wait_timeout(s, timeout)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        s.notified = false;
    }
}

/// Splits and merges the lists an index's writes marked, when the index rebalances in the
/// background ([`Rebalance::Background`](crate::Rebalance::Background)).
///
/// [`Index::open_with_rebalancer`](crate::Index::open_with_rebalancer) hands one to an
/// application that runs its threads itself (an application-owned database, D284). Run it
/// in a loop on a thread of your choosing:
///
/// ```
/// # use std::time::Duration;
/// # fn run(mut rebalancer: pigeonhole_vector::Rebalancer) -> pigeonhole_vector::Result<()> {
/// while !rebalancer.closed() {
///     if !rebalancer.run_once(Duration::from_millis(50))? {
///         rebalancer.wait(Duration::from_secs(1));
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// It holds the index weakly: once every [`Index`](crate::Index) handle is gone,
/// [`closed`](Self::closed) is true and the loop ends. Each split or merge is one or a few
/// transactions, so stopping between calls is always safe; lists left marked are found
/// again by the next [`Index::open`](crate::Index::open).
pub struct Rebalancer {
    index: Weak<Inner>,
    /// The application's handles (simp724's R1 on #1271): once none is left, stop between
    /// lists, so a handle this rebalancer upgraded doesn't keep the database open long.
    user: Weak<()>,
    signal: Arc<Signal>,
}

impl std::fmt::Debug for Rebalancer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rebalancer")
            .field("closed", &self.closed())
            .finish_non_exhaustive()
    }
}

impl Rebalancer {
    pub(crate) fn new(index: &Index, signal: Arc<Signal>) -> Self {
        Self {
            index: Arc::downgrade(index.inner()),
            user: Arc::downgrade(index.user()),
            signal,
        }
    }

    /// Splits and merges marked lists until none is left or `budget` has passed, always
    /// taking at least one (a split in progress finishes first). Returns whether to call
    /// again soon: it did some work, or marked lists are left because the budget ran out.
    /// When it returns `false`, nothing is left until a write marks a list, so
    /// [`wait`](Self::wait). An error leaves the list marked for a later call.
    pub fn run_once(&mut self, budget: Duration) -> Result<bool> {
        if self.closed() {
            return Ok(false);
        }
        let Some(inner) = self.index.upgrade() else {
            return Ok(false);
        };
        let index = Index::from_inner(inner);
        let start = Instant::now();
        let mut did = false;
        loop {
            match index.maintain_one()? {
                Some(worked) => did |= worked,
                None => return Ok(did),
            }
            if self.closed() {
                return Ok(false);
            }
            if start.elapsed() >= budget {
                return Ok(did || index.has_pending());
            }
        }
    }

    /// Waits until a write marks a list, the index's last handle drops, or `timeout`
    /// passes.
    pub fn wait(&mut self, timeout: Duration) {
        self.signal.wait(timeout);
    }

    /// Whether every [`Index`](crate::Index) handle is gone: nothing is left to do.
    pub fn closed(&self) -> bool {
        self.user.strong_count() == 0
    }

    /// Calls `wakeup` whenever a write marks a list (and once when the index closes), for an
    /// application whose threads wait in an event loop rather than in
    /// [`wait`](Self::wait). It runs on the writing thread: keep it short.
    pub fn set_wakeup(&mut self, wakeup: Box<dyn Fn() + Send + Sync>) {
        *self
            .signal
            .wakeup
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Arc::from(wakeup));
    }
}

/// The crate's own rebalancing thread (engine-owned mode): runs `rebalancer` until the
/// index's last handle drops or the database closes.
pub(crate) fn spawn(mut rebalancer: Rebalancer) -> std::io::Result<()> {
    std::thread::Builder::new()
        .name("pigeonhole-vector".into())
        .spawn(move || {
            while !rebalancer.closed() {
                match rebalancer.run_once(Duration::from_millis(50)) {
                    Ok(true) => {}
                    Ok(false) => rebalancer.wait(Duration::from_secs(1)),
                    Err(e) if e.code() == pigeonhole::ErrorCode::Closed => break,
                    // Left marked; try again after a pause.
                    Err(_) => rebalancer.wait(Duration::from_secs(1)),
                }
            }
        })
        .map(|_| ())
}
