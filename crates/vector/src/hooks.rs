//! Hold and fault seams for tests (`__unstable-hooks` only). They pause or skip; they never run
//! another path.

use std::sync::{Arc, Condvar, Mutex, PoisonError};

/// Where a test can hold an index operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Hold {
    /// A split, after its 2-means snapshot read and before its transaction (rev 4 N1).
    SplitBeforeCommit,
}

type Gate = Arc<(Mutex<State>, Condvar)>;

#[derive(Debug, Default)]
struct State {
    reached: bool,
    released: bool,
}

/// An armed hold: [`wait`](Self::wait) until an operation reaches it, then
/// [`release`](Self::release) it. Dropping it releases too.
#[derive(Debug)]
pub struct Armed {
    gate: Gate,
}

static ARMED: Mutex<Option<(Hold, Gate)>> = Mutex::new(None);
static SKIP_REBALANCE: Mutex<bool> = Mutex::new(false);

/// Holds the next operation that reaches `at` (one at a time, process-wide).
pub fn arm(at: Hold) -> Armed {
    let gate = Arc::new((Mutex::new(State::default()), Condvar::new()));
    *ARMED.lock().unwrap_or_else(PoisonError::into_inner) = Some((at, Arc::clone(&gate)));
    Armed { gate }
}

impl Armed {
    /// Waits until an operation is held here.
    pub fn wait(&self) {
        let (m, cv) = &*self.gate;
        let mut s = m.lock().unwrap_or_else(PoisonError::into_inner);
        while !s.reached {
            s = cv.wait(s).unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Lets the held operation go on.
    pub fn release(&self) {
        let (m, cv) = &*self.gate;
        m.lock().unwrap_or_else(PoisonError::into_inner).released = true;
        cv.notify_all();
    }
}

impl Drop for Armed {
    fn drop(&mut self) {
        self.release();
    }
}

pub(crate) fn hold(at: Hold) {
    let gate = {
        let mut armed = ARMED.lock().unwrap_or_else(PoisonError::into_inner);
        match armed.as_ref() {
            Some((h, _)) if *h == at => armed.take().map(|(_, g)| g),
            _ => None,
        }
    };
    let Some(gate) = gate else {
        return;
    };
    let (m, cv) = &*gate;
    let mut s = m.lock().unwrap_or_else(PoisonError::into_inner);
    s.reached = true;
    cv.notify_all();
    while !s.released {
        s = cv.wait(s).unwrap_or_else(PoisonError::into_inner);
    }
}

/// While set, inserts and deletes commit without splitting or merging afterwards: the state
/// a crash between a write and its rebalance leaves (rev 3 M3).
pub fn skip_rebalance(skip: bool) {
    *SKIP_REBALANCE
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = skip;
}

pub(crate) fn skipping_rebalance() -> bool {
    *SKIP_REBALANCE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}
