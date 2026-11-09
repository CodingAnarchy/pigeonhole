//! A stream read by sync calls and by one fiber wait at a time (#1574 PR 2, ICR 0126): the
//! change feed's `Feed` and the memory store's `Watch` share it. Python's `waits.rs` (#1479)
//! is the model.
//!
//! - **A sync read** (`next`, `try_next`) takes the source out of the state and waits on it
//!   with the lock and the GVL released, so no engine wait ever holds the lock the fiber
//!   bridge's drain takes. Other sync calls (`token`, `close`, a second consumer) wait their
//!   turn, and a consumer looping in slices gives way to them between slices (#1411). A guard
//!   puts the source back even if the wait panics.
//! - **One fiber wait at a time** ([`Waits::next_op`]). Its future, polled by the bridge's
//!   drain without the GVL, puts an item it takes in a one-item slot, with the source's token
//!   from before it; the fiber claims it when it resumes. A fiber stopped before it claims
//!   (or whose op is dropped) leaves the item there: the slot's item is the next one any call
//!   returns, and `token` gives the token from before it, so a stop never loses an item.
//! - **A generation counter** supersedes a wait that was dropped, so it never touches the
//!   source or the slot again.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use magnus::{Error, Ruby, Value};
use pigeonhole::ErrorCode;

use crate::error::with_code;
use crate::gvl::without_gvl;

/// The longest a sync wait runs with the GVL released before it checks for an interrupt
/// (`Thread#raise`, Ctrl-C), so a consumer blocked in `next` stays interruptible.
const SLICE: Duration = Duration::from_millis(100);

/// What a stream reads: the change feed, or the memory store's watch.
pub(crate) trait Source: Send + 'static {
    type Item: Send + 'static;
    type Error: Send + 'static;
    /// For messages: "feed" or "watch".
    const NAME: &'static str;
    fn next(&mut self, d: Duration) -> Result<Option<Self::Item>, Self::Error>;
    fn try_next(&mut self) -> Result<Option<Self::Item>, Self::Error>;
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Self::Item, Self::Error>>;
    fn token(&self) -> Vec<u8>;
    fn oldest_retained(&self) -> Option<u64>;
    /// An item as its Ruby value.
    fn to_rb(ruby: &Ruby, item: Self::Item) -> Result<Value, Error>;
    fn error(ruby: &Ruby, e: Self::Error) -> Error;
}

struct State<S: Source> {
    /// `None` once closed, or while a sync read has it out.
    source: Option<S>,
    out: bool,
    closed: bool,
    slot: Option<(S::Item, Vec<u8>)>,
    /// The generation of the fiber wait in progress, if one is.
    waiting: Option<u64>,
    generation: u64,
}

/// A stream's state, shared with its fiber waits.
pub(crate) struct Waits<S: Source> {
    state: Arc<Mutex<State<S>>>,
    turn: Condvar,
    /// Threads waiting for the source: a sync consumer gives way to them between slices.
    wanted: AtomicUsize,
}

/// What a sync read can't do.
enum Refused<E> {
    Source(E),
    /// A fiber's wait is in progress.
    Waiting,
    Closed,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<S: Source> Waits<S> {
    pub(crate) fn new(source: S) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                source: Some(source),
                out: false,
                closed: false,
                slot: None,
                waiting: None,
                generation: 0,
            })),
            turn: Condvar::new(),
            wanted: AtomicUsize::new(0),
        }
    }

    fn closed_err(ruby: &Ruby) -> Error {
        with_code(
            ruby,
            ErrorCode::Closed,
            &format!("the {} is closed", S::NAME),
        )
    }

    fn waiting_err(ruby: &Ruby) -> Error {
        crate::error::uncoded(
            ruby,
            "Error",
            &format!(
                "another fiber's next is waiting on this {}: one consumer at a time",
                S::NAME
            ),
        )
    }

    fn refused(ruby: &Ruby, r: Refused<S::Error>) -> Error {
        match r {
            Refused::Source(e) => S::error(ruby, e),
            Refused::Waiting => Self::waiting_err(ruby),
            Refused::Closed => Self::closed_err(ruby),
        }
    }

    /// The state, once no sync read has the source out: counted in `wanted` meanwhile. Call
    /// without the GVL (a read holds the source for up to a slice).
    fn lock_unread(&self) -> MutexGuard<'_, State<S>> {
        self.wanted.fetch_add(1, Ordering::SeqCst);
        let mut g = lock(&self.state);
        while g.out {
            g = self.turn.wait(g).unwrap_or_else(PoisonError::into_inner);
        }
        self.wanted.fetch_sub(1, Ordering::SeqCst);
        g
    }

    /// One read of at most `slice` (`None`: no wait), without the GVL: the slot's item first.
    fn poll(&self, ruby: &Ruby, slice: Option<Duration>) -> Result<Option<S::Item>, Error> {
        let r = without_gvl(ruby, || -> Result<Option<S::Item>, Refused<S::Error>> {
            let mut g = self.lock_unread();
            if g.waiting.is_some() {
                return Err(Refused::Waiting);
            }
            if let Some((item, _)) = g.slot.take() {
                return Ok(Some(item));
            }
            let source = g.source.take().ok_or(Refused::Closed)?;
            g.out = true;
            drop(g);
            let mut out = Out {
                source: Some(source),
                home: self,
            };
            let source = out.source.as_mut().expect("just taken");
            let next = match slice {
                Some(d) => source.next(d),
                None => source.try_next(),
            };
            drop(out);
            next.map_err(Refused::Source)
        })?;
        r.map_err(|e| Self::refused(ruby, e))
    }

    /// `next(timeout)`: in slices, checking for interrupts between them and letting other
    /// threads that want the stream go first.
    pub(crate) fn wait(
        &self,
        ruby: &Ruby,
        timeout: Option<Duration>,
    ) -> Result<Option<S::Item>, Error> {
        let deadline = timeout.and_then(|t| Instant::now().checked_add(t));
        loop {
            let left = || {
                deadline.map_or(SLICE, |d| {
                    d.saturating_duration_since(Instant::now()).min(SLICE)
                })
            };
            self.give_way(ruby, left())?;
            ruby.thread_check_ints()?;
            if let Some(item) = self.poll(ruby, Some(left()))? {
                return Ok(Some(item));
            }
            if deadline.is_some_and(|d| Instant::now() >= d) {
                return Ok(None);
            }
        }
    }

    /// `try_next`: the next item if one is ready. Never waits for one.
    pub(crate) fn try_next(&self, ruby: &Ruby) -> Result<Option<S::Item>, Error> {
        self.poll(ruby, None)
    }

    /// Waits, without the GVL, until no other thread wants the stream or `limit` passes.
    fn give_way(&self, ruby: &Ruby, limit: Duration) -> Result<(), Error> {
        if self.wanted.load(Ordering::SeqCst) == 0 {
            return Ok(());
        }
        without_gvl(ruby, || {
            let start = Instant::now();
            while self.wanted.load(Ordering::SeqCst) > 0 && start.elapsed() < limit {
                std::thread::sleep(Duration::from_millis(1));
            }
        })
    }

    /// Where the stream is: the token from before a slot's item, else the source's.
    pub(crate) fn token(&self, ruby: &Ruby) -> Result<Vec<u8>, Error> {
        let token = without_gvl(ruby, || {
            let g = self.lock_unread();
            match (&g.slot, &g.source) {
                (Some((_, t)), _) => Some(t.clone()),
                (None, Some(s)) => Some(s.token()),
                (None, None) => None,
            }
        })?;
        token.ok_or_else(|| Self::closed_err(ruby))
    }

    pub(crate) fn oldest_retained(&self, ruby: &Ruby) -> Result<Option<u64>, Error> {
        without_gvl(ruby, || {
            self.lock_unread()
                .source
                .as_ref()
                .and_then(S::oldest_retained)
        })
    }

    pub(crate) fn is_closed(&self, ruby: &Ruby) -> Result<bool, Error> {
        without_gvl(ruby, || lock(&self.state).closed)
    }

    /// Closes the stream: the source is dropped without the GVL. A sync read that has it out
    /// drops it when it comes back; a fiber wait finds it gone and raises `Closed`.
    pub(crate) fn close(&self, ruby: &Ruby) -> Result<(), Error> {
        without_gvl(ruby, || {
            let (source, slot) = {
                let mut g = lock(&self.state);
                g.closed = true;
                (g.source.take(), g.slot.take())
            };
            drop((source, slot));
        })
    }

    /// `native_next_op(bridge)`: the next item as a fiber op. One at a time per stream, and
    /// not while a sync read has the source out.
    #[cfg(unix)]
    pub(crate) fn next_op(
        &self,
        ruby: &Ruby,
        bridge: &crate::fiber::FiberBridge,
    ) -> Result<crate::fiber::FiberOp, Error> {
        let state = Arc::clone(&self.state);
        let begun = {
            let mut g = lock(&state);
            if g.out {
                return Err(crate::error::uncoded(
                    ruby,
                    "Error",
                    &format!(
                        "a thread's next is waiting on this {}: one consumer at a time",
                        S::NAME
                    ),
                ));
            }
            if g.waiting.is_some() {
                return Err(Self::waiting_err(ruby));
            }
            if g.slot.is_none() && g.source.is_none() {
                return Err(Self::closed_err(ruby));
            }
            g.generation += 1;
            let generation = g.generation;
            g.waiting = Some(generation);
            // Taken with the lock held: the token from before the item this wait takes.
            let token = g.source.as_ref().map(S::token);
            (generation, token)
        };
        let (generation, token) = begun;
        crate::fiber::FiberBridge::start(ruby, bridge, move || {
            Box::pin(NextWait {
                state,
                generation,
                token,
                finished: false,
            })
        })
    }
}

/// A sync read's hold on the source, out of the state: dropped (even by a panic), it puts
/// the source back unless the stream was closed meanwhile, and wakes the callers waiting a
/// turn.
struct Out<'a, S: Source> {
    source: Option<S>,
    home: &'a Waits<S>,
}

impl<S: Source> Drop for Out<'_, S> {
    fn drop(&mut self) {
        let mut g = lock(&self.home.state);
        g.out = false;
        let late = if g.closed { self.source.take() } else { None };
        if !g.closed {
            g.source = self.source.take();
        }
        drop(g);
        self.home.turn.notify_all();
        drop(late);
    }
}

/// A fiber wait's future, polled by the bridge's drain without the GVL. An item it takes
/// goes to the slot; the fiber claims it when it resumes, or leaves it for the next caller if
/// it was stopped (its op dropped).
struct NextWait<S: Source> {
    state: Arc<Mutex<State<S>>>,
    generation: u64,
    token: Option<Vec<u8>>,
    finished: bool,
}

/// The fiber's claim on the slot's item: run, it takes the item; dropped unrun (the fiber
/// was stopped after the item came), it ends the wait and leaves the item in the slot.
struct Claim<S: Source> {
    state: Arc<Mutex<State<S>>>,
    generation: u64,
    used: bool,
}

impl<S: Source> Claim<S> {
    fn take(mut self, ruby: &Ruby) -> Result<Value, Error> {
        self.used = true;
        let item = {
            let mut g = lock(&self.state);
            if g.generation != self.generation {
                None
            } else {
                g.waiting = None;
                g.slot.take().map(|(item, _)| item)
            }
        };
        match item {
            Some(item) => S::to_rb(ruby, item),
            None => Err(Waits::<S>::closed_err(ruby)),
        }
    }
}

impl<S: Source> Drop for Claim<S> {
    fn drop(&mut self) {
        if self.used {
            return;
        }
        let mut g = lock(&self.state);
        if g.waiting == Some(self.generation) {
            g.waiting = None;
        }
    }
}

impl<S: Source> NextWait<S> {
    fn claim(&self) -> crate::fiber::Out {
        let claim = Claim {
            state: Arc::clone(&self.state),
            generation: self.generation,
            used: false,
        };
        Box::new(move |ruby: &Ruby| claim.take(ruby))
    }
}

impl<S: Source> Future for NextWait<S> {
    type Output = crate::fiber::Out;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let state = Arc::clone(&this.state);
        let mut g = lock(&state);
        if g.waiting != Some(this.generation) {
            // Superseded: the wait was dropped and a newer one (or a sync read) went on.
            this.finished = true;
            return Poll::Ready(Box::new(|ruby: &Ruby| Err(Waits::<S>::closed_err(ruby))));
        }
        if g.slot.is_some() {
            // An item a stopped wait left: this wait takes it.
            this.finished = true;
            return Poll::Ready(this.claim());
        }
        let Some(source) = g.source.as_mut() else {
            g.waiting = None;
            this.finished = true;
            return Poll::Ready(Box::new(|ruby: &Ruby| Err(Waits::<S>::closed_err(ruby))));
        };
        match source.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(item)) => {
                let token = this.token.take().unwrap_or_default();
                g.slot = Some((item, token));
                this.finished = true;
                Poll::Ready(this.claim())
            }
            Poll::Ready(Err(e)) => {
                g.waiting = None;
                this.finished = true;
                Poll::Ready(Box::new(move |ruby: &Ruby| Err(S::error(ruby, e))))
            }
        }
    }
}

impl<S: Source> Drop for NextWait<S> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Dropped unfinished (the fiber was stopped or timed out): nothing was taken.
        let mut g = lock(&self.state);
        if g.waiting == Some(self.generation) {
            g.waiting = None;
        }
    }
}
