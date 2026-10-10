//! The foreground guard (#19, D200): a shard's group commit must never block on a sync.
//!
//! The engine marks its shard threads' WAL calls with [`StrictForeground::enter`]. Inside
//! that, [`Wal::append`](crate::Wal::append), [`Wal::write`](crate::Wal::write) and
//! [`Wal::submit_sync`](crate::Wal::submit_sync) run in a foreground scope, and in debug
//! builds any blocking sync or wait reached from one panics, except the counted fallbacks
//! (`exempt`: an inline rollover sync or growth with no spare slot, a group larger than what
//! [`Wal::blocked`](crate::Wal::blocked) leaves room for). Release builds compile it all out.
//!
//! ```
//! let _strict = pigeonhole_wal::StrictForeground::enter();
//! ```

use std::cell::Cell;

thread_local! {
    static STRICT: Cell<u32> = const { Cell::new(0) };
    static FOREGROUND: Cell<u32> = const { Cell::new(0) };
    static EXEMPT: Cell<u32> = const { Cell::new(0) };
}

fn bump(c: &'static std::thread::LocalKey<Cell<u32>>, by: i32) {
    c.with(|v| v.set(v.get().wrapping_add_signed(by)));
}

/// Marks this thread's WAL calls as a shard's foreground loop until dropped (#19): in debug
/// builds, a blocking sync or wait inside `append`, `write` or `submit_sync` then panics.
#[must_use = "the scope ends when this is dropped"]
#[derive(Debug)]
pub struct StrictForeground(());

impl StrictForeground {
    /// Enters the scope (nests).
    pub fn enter() -> Self {
        if cfg!(debug_assertions) {
            bump(&STRICT, 1);
        }
        StrictForeground(())
    }
}

impl Drop for StrictForeground {
    fn drop(&mut self) {
        if cfg!(debug_assertions) {
            bump(&STRICT, -1);
        }
    }
}

/// A foreground WAL call (`append`, `write`, `submit_sync`) in progress on this thread.
pub(crate) struct Foreground(());

impl Foreground {
    pub(crate) fn enter() -> Self {
        if cfg!(debug_assertions) {
            bump(&FOREGROUND, 1);
        }
        Foreground(())
    }
}

impl Drop for Foreground {
    fn drop(&mut self) {
        if cfg!(debug_assertions) {
            bump(&FOREGROUND, -1);
        }
    }
}

/// Runs a counted fallback that may block (see the module docs).
pub(crate) fn exempt<T>(f: impl FnOnce() -> T) -> T {
    if cfg!(debug_assertions) {
        bump(&EXEMPT, 1);
    }
    let r = f();
    if cfg!(debug_assertions) {
        bump(&EXEMPT, -1);
    }
    r
}

/// Called before anything that blocks on I/O: panics (debug builds) inside a strict
/// foreground call outside an exempt fallback.
pub(crate) fn may_block(what: &str) {
    if cfg!(debug_assertions)
        && STRICT.with(Cell::get) > 0
        && FOREGROUND.with(Cell::get) > 0
        && EXEMPT.with(Cell::get) == 0
    {
        panic!("{what} on a shard's foreground loop (#19, D200)");
    }
}

#[cfg(all(test, debug_assertions))]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "foreground loop")]
    fn a_blocking_sync_in_a_strict_foreground_call_panics() {
        let _strict = StrictForeground::enter();
        let _fg = Foreground::enter();
        may_block("a blocking WAL sync");
    }

    #[test]
    fn exempt_fallbacks_and_calls_outside_the_scopes_may_block() {
        may_block("outside any scope");
        let _strict = StrictForeground::enter();
        may_block("strict, not in a foreground call");
        let _fg = Foreground::enter();
        exempt(|| may_block("a counted fallback"));
    }
}
