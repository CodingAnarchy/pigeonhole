//! Handles and `fork` (simp724's R1 on #697): pre-fork servers (Puma, Unicorn, Pitchfork
//! with `preload_app`) fork after the app has opened its database.
//!
//! Every Ruby object that holds an engine handle keeps it in a [`ForkSafe`], which records
//! the process that created it. In any other process (a forked child):
//! - using it raises `Pigeonhole::Unsupported`, before the engine is reached (the engine
//!   refuses too, ICR 0067, but some of its accessors are inert rather than failing);
//! - dropping it (Ruby's GC, or the process exiting) **forgets** the handle instead of
//!   dropping it: no close, flush or lock release runs on the parent's database. A `flock`
//!   belongs to the open file description, which the child shares with the parent.

use magnus::{Error, Ruby};
use pigeonhole::ErrorCode;

use crate::error::with_code;

/// A value owned by the process that created it.
pub(crate) struct ForkSafe<T> {
    /// The creating process.
    pid: u32,
    /// The value; `None` only while dropping.
    val: Option<T>,
}

impl<T> ForkSafe<T> {
    /// Wraps `val`, owned by this process.
    pub(crate) fn new(val: T) -> Self {
        Self {
            pid: std::process::id(),
            val: Some(val),
        }
    }

    /// Whether this is a process other than the one that created the value.
    pub(crate) fn forked(&self) -> bool {
        std::process::id() != self.pid
    }

    /// The value, or `Unsupported` in a forked child.
    pub(crate) fn get(&self, ruby: &Ruby) -> Result<&T, Error> {
        if self.forked() {
            return Err(forked_error(ruby));
        }
        Ok(self.val.as_ref().expect("present until dropped"))
    }
}

impl<T> Drop for ForkSafe<T> {
    fn drop(&mut self) {
        if self.forked() {
            // The parent's handle: leave it alone (no close, no unlock, no thread join).
            std::mem::forget(self.val.take());
        }
    }
}

/// The error a handle raises in a forked child.
pub(crate) fn forked_error(ruby: &Ruby) -> Error {
    with_code(
        ruby,
        ErrorCode::Unsupported,
        "this database was opened in another process (fork); open it again here",
    )
}
