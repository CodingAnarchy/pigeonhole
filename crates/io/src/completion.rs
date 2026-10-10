use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use crate::{Error, ErrorKind, IoBuf, Result};

/// The pending result of a submitted operation: a read or write (resolving to its buffer),
/// a sync (resolving to `()`), or a higher-level operation built on them (a WAL group sync
/// resolving to an `Lsn`, a root commit).
///
/// Usable from both worlds: [`Completion::wait`] blocks a sync caller; as a [`Future`] it
/// registers a waker that the backend wakes on completion (no `spawn_blocking`). Dropping an
/// unfinished completion is safe: the backend keeps the buffer until the kernel is done.
///
/// ```
/// use pigeonhole_io::Completion;
///
/// let (done, resolver) = Completion::<u64>::pair();
/// let doubled = done.map(|r| r.map(|n| n * 2));
/// assert!(!doubled.is_ready());
/// std::thread::spawn(move || resolver.resolve(Ok(21)));
/// assert_eq!(doubled.wait().unwrap(), 42);
///
/// assert!(Completion::ready(Ok(())).is_ready());
/// ```
#[must_use = "a completion does nothing unless waited on or polled"]
pub struct Completion<T = IoBuf> {
    inner: Inner<T>,
}

enum Inner<T> {
    /// Resolved at creation; `None` once taken.
    Ready(Option<Result<T>>),
    Pending(Arc<Shared<T>>),
}

type Then<T> = Box<dyn FnOnce(Result<T>) + Send>;

/// Makes progress on an operation's I/O when a thread blocks on its completion, where nothing
/// else may ever complete it: the simulator's deferred mode runs the operation; an io_uring
/// ring owned by the waiting thread reaps it (#402). Returns whether calling it again may
/// make more progress (`false`: wait for another thread to resolve it). Calling it when the
/// operation already completed does nothing.
/// How long a blocked wait with no drive reaps orphan rings, or sleeps between reaps (#408).
const ORPHAN_SLICE: std::time::Duration = std::time::Duration::from_millis(1);

pub(crate) type Drive = Arc<dyn Fn() -> bool + Send + Sync>;

struct Shared<T> {
    state: Mutex<State<T>>,
    cond: Condvar,
    /// Set for the simulator's deferred operations and every completion mapped from one.
    drive: Option<Drive>,
}

enum State<T> {
    Pending {
        waker: Option<Waker>,
        /// Installed by `map`: receives the result on the resolving thread.
        then: Option<Then<T>>,
    },
    Done(Result<T>),
    Taken,
}

impl<T> Shared<T> {
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

// The result is never pinned in place: polling moves it out, so `Completion` is `Unpin` for
// any `T`.
impl<T> Unpin for Completion<T> {}

impl<T: Send + 'static> Completion<T> {
    /// A completion that is already resolved (used by synchronous backends and cache hits).
    pub fn ready(result: Result<T>) -> Self {
        Self {
            inner: Inner::Ready(Some(result)),
        }
    }

    /// A completion and the handle that resolves it, for layers that build their own
    /// asynchronous operations (WAL group sync, root commit) on top of the backend.
    pub fn pair() -> (Self, Resolver<T>) {
        Self::driven_pair(None)
    }

    /// A pair whose blocking [`Completion::wait`] calls `drive` first (see [`Drive`]).
    pub(crate) fn driven_pair(drive: Option<Drive>) -> (Self, Resolver<T>) {
        let shared = Arc::new(Shared {
            state: Mutex::new(State::Pending {
                waker: None,
                then: None,
            }),
            cond: Condvar::new(),
            drive,
        });
        (
            Self {
                inner: Inner::Pending(Arc::clone(&shared)),
            },
            Resolver {
                shared: Some(shared),
            },
        )
    }

    /// Transforms the result once it arrives (runs on the resolving thread).
    pub fn map<U: Send + 'static>(
        self,
        f: impl FnOnce(Result<T>) -> Result<U> + Send + 'static,
    ) -> Completion<U> {
        let shared = match self.inner {
            Inner::Ready(result) => return Completion::ready(f(taken(result))),
            Inner::Pending(shared) => shared,
        };
        let mut state = shared.lock();
        match std::mem::replace(&mut *state, State::Taken) {
            State::Done(result) => {
                drop(state);
                Completion::ready(f(result))
            }
            State::Pending { .. } => {
                let (next, resolver) = Completion::driven_pair(shared.drive.clone());
                *state = State::Pending {
                    waker: None,
                    then: Some(Box::new(move |result| resolver.resolve(f(result)))),
                };
                next
            }
            State::Taken => unreachable!("completion result taken twice"),
        }
    }

    /// Blocks the calling thread until the operation finishes.
    pub fn wait(self) -> Result<T> {
        let shared = match self.inner {
            Inner::Ready(result) => return taken(result),
            Inner::Pending(shared) => shared,
        };
        let mut state = shared.lock();
        let mut drive = shared.drive.as_ref();
        loop {
            match std::mem::replace(&mut *state, State::Taken) {
                State::Done(result) => return result,
                pending @ State::Pending { .. } => {
                    *state = pending;
                    if let Some(run) = drive {
                        drop(state);
                        if !run() {
                            drive = None;
                        }
                        state = shared.lock();
                        continue;
                    }
                    // No drive: with rings no thread reaps in this process (application-owned
                    // io_uring, #408), take their completions while waiting, in short slices.
                    drop(state);
                    let reaped = crate::own::reap_orphans(ORPHAN_SLICE);
                    state = shared.lock();
                    if reaped == Some(true) {
                        continue;
                    }
                    if !matches!(*state, State::Pending { .. }) {
                        continue;
                    }
                    state = match reaped {
                        None => shared
                            .cond
                            .wait(state)
                            .unwrap_or_else(PoisonError::into_inner),
                        Some(_) => {
                            shared
                                .cond
                                .wait_timeout(state, ORPHAN_SLICE)
                                .unwrap_or_else(PoisonError::into_inner)
                                .0
                        }
                    };
                }
                State::Taken => unreachable!("completion result taken twice"),
            }
        }
    }

    /// Whether the operation has finished (never blocks).
    pub fn is_ready(&self) -> bool {
        match &self.inner {
            Inner::Ready(_) => true,
            Inner::Pending(shared) => matches!(*shared.lock(), State::Done(_)),
        }
    }
}

impl Completion<()> {
    /// `n` completions that each resolve with this one's outcome, for one operation several
    /// waiters depend on (one directory sync covering several new files, #158). A blocking
    /// [`Completion::wait`] on any of them makes progress on this one's I/O as a wait on it
    /// would. A failure reaches the first with its OS error, and the others as an error of
    /// the same kind and context.
    ///
    /// ```
    /// use pigeonhole_io::Completion;
    ///
    /// let (c, resolver) = Completion::<()>::pair();
    /// let fanned = c.fan_out(3);
    /// assert!(fanned.iter().all(|c| !c.is_ready()));
    /// resolver.resolve(Ok(()));
    /// assert!(fanned.into_iter().all(|c| c.wait().is_ok()));
    /// ```
    pub fn fan_out(self, n: usize) -> Vec<Completion<()>> {
        let drive = match &self.inner {
            Inner::Pending(shared) => shared.drive.clone(),
            Inner::Ready(_) => None,
        };
        let (out, resolvers): (Vec<_>, Vec<_>) = (0..n)
            .map(|_| Completion::driven_pair(drive.clone()))
            .unzip();
        drop(self.map(move |r| {
            let copy = r.as_ref().err().map(|e| (e.kind, e.context));
            let mut first = Some(r);
            for resolver in resolvers {
                resolver.resolve(match (first.take(), copy) {
                    (Some(r), _) => r,
                    (None, Some((kind, context))) => Err(Error::new(kind, context)),
                    (None, None) => Ok(()),
                });
            }
            Ok(())
        }));
        out
    }
}

fn taken<T>(result: Option<Result<T>>) -> Result<T> {
    result.expect("completion polled after it finished")
}

impl<T: Send + 'static> Future for Completion<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.get_mut().inner {
            Inner::Ready(result) => Poll::Ready(taken(result.take())),
            Inner::Pending(shared) => {
                let mut state = shared.lock();
                match &mut *state {
                    State::Done(_) => match std::mem::replace(&mut *state, State::Taken) {
                        State::Done(result) => Poll::Ready(result),
                        _ => unreachable!(),
                    },
                    State::Pending { waker, .. } => {
                        match waker {
                            Some(w) if w.will_wake(cx.waker()) => {}
                            _ => *waker = Some(cx.waker().clone()),
                        }
                        Poll::Pending
                    }
                    State::Taken => panic!("completion polled after it finished"),
                }
            }
        }
    }
}

impl<T> fmt::Debug for Completion<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ready = match &self.inner {
            Inner::Ready(_) => true,
            Inner::Pending(shared) => matches!(*shared.lock(), State::Done(_)),
        };
        f.debug_struct("Completion").field("ready", &ready).finish()
    }
}

/// Resolves a [`Completion`] created with [`Completion::pair`].
///
/// Dropping a resolver without calling [`Resolver::resolve`] resolves its completion with an
/// `Other` error, so a waiter never hangs on an abandoned operation.
///
/// ```
/// use pigeonhole_io::{Completion, ErrorKind};
///
/// let (done, resolver) = Completion::<()>::pair();
/// drop(resolver);
/// assert_eq!(done.wait().unwrap_err().kind, ErrorKind::Other);
/// ```
pub struct Resolver<T> {
    shared: Option<Arc<Shared<T>>>,
}

impl<T: Send + 'static> Resolver<T> {
    /// Resolves the completion and wakes its waiter.
    pub fn resolve(mut self, result: Result<T>) {
        if let Some(shared) = self.shared.take() {
            complete(&shared, result);
        }
    }
}

fn complete<T>(shared: &Shared<T>, result: Result<T>) {
    let mut state = shared.lock();
    match std::mem::replace(&mut *state, State::Taken) {
        State::Pending {
            then: Some(then), ..
        } => {
            drop(state);
            then(result);
        }
        State::Pending { waker, then: None } => {
            *state = State::Done(result);
            drop(state);
            shared.cond.notify_all();
            if let Some(waker) = waker {
                waker.wake();
            }
        }
        State::Done(_) | State::Taken => unreachable!("completion resolved twice"),
    }
}

impl<T> Drop for Resolver<T> {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.take() {
            complete(
                &shared,
                Err(Error::new(ErrorKind::Other, "operation abandoned")),
            );
        }
    }
}

impl<T> fmt::Debug for Resolver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Resolver").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    struct CountWaker(AtomicUsize);

    impl Wake for CountWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn poll_registers_waker_and_resolves() {
        let counter = Arc::new(CountWaker(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&counter));
        let mut cx = Context::from_waker(&waker);
        let (mut c, r) = Completion::<u32>::pair();
        assert!(Pin::new(&mut c).poll(&mut cx).is_pending());
        assert!(!c.is_ready());
        r.resolve(Ok(7));
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
        assert!(c.is_ready());
        match Pin::new(&mut c).poll(&mut cx) {
            Poll::Ready(Ok(7)) => {}
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn map_on_ready_and_on_resolved() {
        let c = Completion::ready(Ok(1u8)).map(|r| r.map(u16::from));
        assert_eq!(c.wait().unwrap(), 1u16);

        let (c, r) = Completion::<u8>::pair();
        r.resolve(Ok(2));
        assert_eq!(c.map(|r| r.map(|x| x + 1)).wait().unwrap(), 3);
    }

    #[test]
    fn map_runs_on_resolving_thread() {
        let (c, r) = Completion::<()>::pair();
        let c = c.map(|r| r.map(|()| std::thread::current().id()));
        let resolver_thread = std::thread::spawn(move || {
            r.resolve(Ok(()));
            std::thread::current().id()
        });
        let resolved_on = resolver_thread.join().unwrap();
        assert_eq!(c.wait().unwrap(), resolved_on);
    }

    #[test]
    fn dropped_completion_is_harmless() {
        let (c, r) = Completion::<u8>::pair();
        drop(c);
        r.resolve(Ok(1));
    }

    #[test]
    fn errors_pass_through() {
        let (c, r) = Completion::<u8>::pair();
        r.resolve(Err(Error::new(ErrorKind::NoSpace, "write")));
        assert_eq!(c.wait().unwrap_err().kind, ErrorKind::NoSpace);
    }
}
