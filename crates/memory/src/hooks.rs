//! Hold and fault seams for tests (`__unstable-hooks` only, ICR 0085's convention). They
//! pause or fail; they never run another path.

use std::sync::{Arc, Condvar, Mutex, PoisonError};

/// Where a test can hold a store operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Hold {
    /// A `remember` with near dedup, after its near-duplicate check and before its
    /// transaction (simp724's F1 on #1386).
    RememberAfterNear,
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

static FAIL_LOOP_SAVE: Mutex<Option<String>> = Mutex::new(None);

/// Fails the next save of the consolidation loop named `name` at its commit, with an error
/// that isn't a conflict (simp724's L1 on #1432: a failed save mustn't fence the loop out).
/// By name, so tests running in parallel don't take each other's.
pub fn fail_next_loop_save(name: &str) {
    *FAIL_LOOP_SAVE
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(name.to_owned());
}

pub(crate) fn take_fail_loop_save(name: &str) -> bool {
    let mut armed = FAIL_LOOP_SAVE
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if armed.as_deref() == Some(name) {
        *armed = None;
        return true;
    }
    false
}
