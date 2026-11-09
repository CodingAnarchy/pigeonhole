//! Whether this thread is inside an async poll (debug builds only), so that code which parks
//! the thread can assert it is not: a `std::thread::park` inside a poll can use up the unpark
//! a waker delivered meanwhile, and an executor that parks its thread on `Pending` then sleeps
//! for ever (a lost wake; #1226). Engine poll entry points enter the scope; the engine's and
//! runtime's park sites `debug_assert!(!in_async_poll())`.

#[cfg(debug_assertions)]
thread_local! {
    static IN_ASYNC_POLL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Marks this thread as inside an async poll until dropped ([`async_poll_scope`]).
#[must_use = "the scope ends when dropped"]
#[derive(Debug)]
pub struct AsyncPollScope {
    #[cfg(debug_assertions)]
    was: bool,
}

/// Enters an async poll's scope (nesting allowed). A no-op in release builds.
#[inline]
pub fn async_poll_scope() -> AsyncPollScope {
    AsyncPollScope {
        #[cfg(debug_assertions)]
        was: IN_ASYNC_POLL.with(|c| c.replace(true)),
    }
}

impl Drop for AsyncPollScope {
    #[inline]
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        IN_ASYNC_POLL.with(|c| c.set(self.was));
    }
}

/// Whether this thread is inside an async poll; always `false` in release builds.
#[inline]
pub fn in_async_poll() -> bool {
    #[cfg(debug_assertions)]
    return IN_ASYNC_POLL.with(std::cell::Cell::get);
    #[cfg(not(debug_assertions))]
    false
}

thread_local! {
    static FILE_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Counts a block or value read from a file on this thread (called where the read happens,
/// never on a cache hit): what an async cell step reads synchronously is the difference of
/// [`thread_file_reads`] around it (#1625, perf287: two thread-local loads per step, not a
/// counting scope).
#[cold]
pub fn note_thread_file_read() {
    FILE_READS.with(|c| c.set(c.get().wrapping_add(1)));
}

/// The file reads this thread has made ([`note_thread_file_read`]), wrapping.
#[inline]
pub fn thread_file_reads() -> u64 {
    FILE_READS.with(std::cell::Cell::get)
}
