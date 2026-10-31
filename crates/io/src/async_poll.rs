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
