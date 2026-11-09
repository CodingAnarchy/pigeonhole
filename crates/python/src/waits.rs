//! A stream of items read by sync calls and by one `next_async` wait (#1479): the change
//! feed's `Feed` and the memory store's `Watch` share it.
//!
//! - **A sync read** (`next`, `try_next`) takes the source out of the state and waits on it
//!   with the lock released, so no engine wait ever holds the lock the bridge and the loop
//!   take (cli692's P1 on #1547). Other sync calls wait their turn on the condvar; a guard
//!   puts the source back even if the wait panics (N3). `next_async` meanwhile refuses.
//! - **One `next_async` wait at a time.** Its Rust future, polled on the bridge without the
//!   GIL, puts an item it takes in a one-item slot, with the source's token from before it;
//!   the loop thread then claims it for the wait's future, or leaves it there if that future
//!   was cancelled meanwhile (B5). The slot's item is the next one any call returns, and
//!   `token()` gives the token from before it, so a cancel never loses an item.
//! - **A generation counter** supersedes a cancelled wait still being dropped, so it never
//!   touches the source or the slot again.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::error::with_code;

/// The longest stretch `next` waits with the GIL released before it checks for a signal
/// (Ctrl-C), so a blocked consumer can still be interrupted.
const SLICE: Duration = Duration::from_millis(100);

/// What a stream reads: the change feed, or the memory store's watch.
pub(crate) trait Source: Send + 'static {
    type Item: Clone + Send + 'static;
    type Error: Send + 'static;
    /// For messages: "feed" or "watch".
    const NAME: &'static str;
    fn next(&mut self, d: Duration) -> Result<Option<Self::Item>, Self::Error>;
    fn try_next(&mut self) -> Result<Option<Self::Item>, Self::Error>;
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Self::Item, Self::Error>>;
    fn token(&self) -> Vec<u8>;
    fn oldest_retained(&self) -> Option<u64>;
    /// An item as its Python object.
    fn to_py(py: Python<'_>, item: Self::Item) -> PyResult<Py<PyAny>>;
    fn error(e: Self::Error) -> PyErr;
}

struct State<S: Source> {
    /// `None` once closed, or while a sync read has it out.
    source: Option<S>,
    out: bool,
    closed: bool,
    slot: Option<(S::Item, Vec<u8>)>,
    wait: Option<Wait>,
    /// The `next_async` wait's waker while its future is pending: `close` wakes it, so the
    /// wait finds the source gone and fails with `Closed` (the source's own wakers go with it,
    /// unwoken; cli692's R1 on the Ruby port, #1605).
    waker: Option<Waker>,
    generation: u64,
}

struct Wait {
    /// Set by the wait's asyncio future's done callback: a cancelled wait doesn't block a
    /// new one, even before its Rust future is dropped.
    over: Arc<AtomicBool>,
    /// That future, asked `done()` (with the GIL) before a call is refused: a future
    /// cancelled and awaited at once is done before its callback has run.
    owner: Arc<Py<PyAny>>,
}

impl<S: Source> State<S> {
    /// Whether a `next_async` wait is still in progress.
    fn waiting(&mut self) -> bool {
        match &self.wait {
            Some(w) if !w.over.load(Ordering::SeqCst) => true,
            _ => {
                self.wait = None;
                false
            }
        }
    }
}

/// What a sync read can't do.
enum Refused<E> {
    Source(E),
    /// A wait looks in progress: its flag and future, to ask whether it's done.
    Waiting(Arc<AtomicBool>, Arc<Py<PyAny>>),
    Closed,
}

/// Whether the wait's future is done (completed or cancelled); if so, marks it over.
fn owner_done(py: Python<'_>, over: &AtomicBool, owner: &Py<PyAny>) -> PyResult<bool> {
    let done = owner.bind(py).call_method0("done")?.is_truthy()?;
    if done {
        over.store(true, Ordering::SeqCst);
    }
    Ok(done)
}

/// A stream's state, shared with its waits.
pub(crate) struct Waits<S: Source>(Arc<Mutex<State<S>>>, Arc<Condvar>);

impl<S: Source> Waits<S> {
    pub(crate) fn new(source: S) -> Self {
        Self(
            Arc::new(Mutex::new(State {
                source: Some(source),
                out: false,
                closed: false,
                slot: None,
                wait: None,
                waker: None,
                generation: 0,
            })),
            Arc::new(Condvar::new()),
        )
    }

    fn waiting_err() -> PyErr {
        PyValueError::new_err(format!(
            "a next_async wait is in progress on this {}",
            S::NAME
        ))
    }

    fn closed_err() -> PyErr {
        with_code(
            pigeonhole::ErrorCode::Closed,
            &format!("the {} is closed", S::NAME),
        )
    }

    fn refused(r: Refused<S::Error>) -> PyErr {
        match r {
            Refused::Source(e) => S::error(e),
            Refused::Waiting(..) => Self::waiting_err(),
            Refused::Closed => Self::closed_err(),
        }
    }

    /// One wait of at most `slice` (`None`: don't wait), with the GIL released: the slot's
    /// item first.
    pub(crate) fn poll(
        &self,
        py: Python<'_>,
        slice: Option<Duration>,
    ) -> PyResult<Option<S::Item>> {
        match self.poll_once(py, slice) {
            // The wait may be over already (cancelled and awaited): ask, then try again.
            Err(Refused::Waiting(over, owner)) if owner_done(py, &over, &owner)? => {
                self.poll_once(py, slice).map_err(Self::refused)
            }
            r => r.map_err(Self::refused),
        }
    }

    fn poll_once(
        &self,
        py: Python<'_>,
        slice: Option<Duration>,
    ) -> Result<Option<S::Item>, Refused<S::Error>> {
        py.detach(|| {
            let mut g = self.lock_unread();
            if g.waiting() {
                let w = g.wait.as_ref().expect("waiting");
                return Err(Refused::Waiting(Arc::clone(&w.over), Arc::clone(&w.owner)));
            }
            if let Some((item, _)) = g.slot.take() {
                return Ok(Some(item));
            }
            let source = g.source.take().ok_or(Refused::Closed)?;
            g.out = true;
            // A cancelled wait still being dropped is superseded: it never polls the source.
            g.generation += 1;
            drop(g);
            // Puts the source back and wakes the sync callers taking turns, even if the wait
            // panics (cli692's N3).
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
        })
    }

    /// The state, once no sync read has the source out (without the GIL: a read waits up to
    /// a slice).
    fn lock_unread(&self) -> std::sync::MutexGuard<'_, State<S>> {
        let mut g = self.0.lock().expect("not poisoned");
        while g.out {
            g = self.1.wait(g).expect("not poisoned");
        }
        g
    }

    /// `next` with an optional timeout: in slices, checking for signals between them.
    pub(crate) fn wait(
        &self,
        py: Python<'_>,
        timeout: Option<Duration>,
    ) -> PyResult<Option<S::Item>> {
        let deadline = timeout.and_then(|t| Instant::now().checked_add(t));
        loop {
            let slice = match deadline {
                None => SLICE,
                Some(d) => d.saturating_duration_since(Instant::now()).min(SLICE),
            };
            if let Some(item) = self.poll(py, Some(slice))? {
                return Ok(Some(item));
            }
            py.check_signals()?;
            if deadline.is_some_and(|d| Instant::now() >= d) {
                return Ok(None);
            }
        }
    }

    /// The next item, as an `asyncio.Future` of the running loop.
    pub(crate) fn next_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (event_loop, fut) = crate::aio::new_future(py)?;
        // Brief: no engine wait holds this lock (P1).
        let mut g = self.0.lock().expect("not poisoned");
        if g.out {
            return Err(PyValueError::new_err(format!(
                "a sync next is in progress on this {}",
                S::NAME
            )));
        }
        if g.waiting() {
            let w = g.wait.as_ref().expect("waiting");
            if !owner_done(py, &w.over, &w.owner)? {
                return Err(Self::waiting_err());
            }
            g.wait = None;
        }
        if let Some((item, _)) = g.slot.take() {
            drop(g);
            let v = S::to_py(py, item)?;
            crate::aio::complete_now(py, &fut, crate::aio::Entry::Value(v))?;
            return Ok(fut);
        }
        let token = g.source.as_ref().ok_or_else(Self::closed_err)?.token();
        g.generation += 1;
        let generation = g.generation;
        let over = Arc::new(AtomicBool::new(false));
        g.wait = Some(Wait {
            over: Arc::clone(&over),
            owner: Arc::new(fut.clone().unbind()),
        });
        drop(g);
        fut.call_method1("add_done_callback", (Over(over),))?;
        let state = Arc::clone(&self.0);
        crate::aio::start(py, &event_loop, &fut, move || NextWait {
            state,
            generation,
            token: Some(token),
            finished: false,
        })?;
        Ok(fut)
    }

    /// Where the stream is: the token from before a slot's item, else the source's.
    pub(crate) fn token(&self, py: Python<'_>) -> PyResult<Vec<u8>> {
        // The lock may be held by a thread waiting in `next`, for up to a slice: wait for
        // it without the GIL.
        let token = py.detach(|| {
            let g = self.lock_unread();
            // An item in the slot isn't delivered yet: the token from before it (B5).
            match (&g.slot, &g.source) {
                (Some((_, t)), _) => Some(t.clone()),
                (None, Some(s)) => Some(s.token()),
                (None, None) => None,
            }
        });
        token.ok_or_else(Self::closed_err)
    }

    pub(crate) fn oldest_retained(&self, py: Python<'_>) -> Option<u64> {
        py.detach(|| {
            self.lock_unread()
                .source
                .as_ref()
                .and_then(S::oldest_retained)
        })
    }

    pub(crate) fn closed(&self) -> bool {
        self.0.lock().expect("not poisoned").closed
    }

    pub(crate) fn close(&self, py: Python<'_>) {
        py.detach(|| {
            let (source, slot, waker) = {
                let mut g = self.0.lock().expect("not poisoned");
                // A sync read that has the source out drops it when it comes back.
                g.closed = true;
                (g.source.take(), g.slot.take(), g.waker.take())
            };
            drop((source, slot));
            // A pending `next_async` is polled again, finds the source gone and fails with
            // `Closed`, rather than waiting for an item that can't come.
            if let Some(w) = waker {
                w.wake();
            }
        });
    }
}

/// A sync read's hold on the source, out of the state: dropped (even by a panic), it puts
/// the source back, unless the stream was closed meanwhile, and wakes the callers waiting a
/// turn.
struct Out<'a, S: Source> {
    source: Option<S>,
    home: &'a Waits<S>,
}

impl<S: Source> Drop for Out<'_, S> {
    fn drop(&mut self) {
        let mut g = self
            .home
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        g.out = false;
        if !g.closed {
            g.source = self.source.take();
        }
        drop(g);
        self.home.1.notify_all();
    }
}

/// A `next_async` wait's Rust future, polled on the bridge without the GIL. An item it takes
/// goes to the slot first; the loop thread then claims it for the wait's future, or leaves
/// it for the next caller if that future was cancelled meanwhile.
struct NextWait<S: Source> {
    state: Arc<Mutex<State<S>>>,
    generation: u64,
    token: Option<Vec<u8>>,
    finished: bool,
}

impl<S: Source> NextWait<S> {
    fn claim(&self) -> crate::aio::Output {
        let state = Arc::clone(&self.state);
        let generation = self.generation;
        let take = move |py: Python<'_>, take: bool| -> PyResult<Option<Py<PyAny>>> {
            let mut g = state.lock().expect("not poisoned");
            if g.generation != generation {
                return Ok(None);
            }
            g.wait = None;
            match (take, g.slot.take()) {
                (true, Some((item, _))) => {
                    drop(g);
                    S::to_py(py, item).map(Some)
                }
                (true, None) => Ok(None),
                (false, slot) => {
                    // Cancelled: the item stays for the next caller.
                    g.slot = slot;
                    Ok(None)
                }
            }
        };
        Box::new(move |py| match Py::new(py, Claim(Box::new(take))) {
            Ok(c) => crate::aio::Entry::Claim(c.into_any()),
            Err(e) => crate::aio::Entry::Error(e),
        })
    }
}

impl<S: Source> Future for NextWait<S> {
    type Output = crate::aio::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let state = Arc::clone(&this.state);
        let mut g = state.lock().expect("not poisoned");
        if g.generation != this.generation {
            // Superseded: the future was cancelled and a newer wait owns the source. The
            // stored waker is that wait's, so it stays (perf2's R1 on #1610).
            this.finished = true;
            return Poll::Ready(Box::new(|py| crate::aio::Entry::Value(py.None())));
        }
        g.waker = None;
        if g.slot.is_some() {
            // Left by a superseded wait that took it after its cancel.
            this.finished = true;
            return Poll::Ready(this.claim());
        }
        if g.out {
            // A sync read has the source: only a superseded wait gets here, and the
            // generation check above has already ended it.
            return Poll::Pending;
        }
        let Some(source) = g.source.as_mut() else {
            g.wait = None;
            this.finished = true;
            return Poll::Ready(Box::new(|_| {
                crate::aio::Entry::Error(Waits::<S>::closed_err())
            }));
        };
        match source.poll_next(cx) {
            Poll::Pending => {
                g.waker = Some(cx.waker().clone());
                Poll::Pending
            }
            Poll::Ready(Ok(item)) => {
                let token = this.token.take().unwrap_or_default();
                g.slot = Some((item, token));
                this.finished = true;
                Poll::Ready(this.claim())
            }
            Poll::Ready(Err(e)) => {
                g.wait = None;
                this.finished = true;
                Poll::Ready(Box::new(move |_| crate::aio::Entry::Error(S::error(e))))
            }
        }
    }
}

impl<S: Source> Drop for NextWait<S> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Dropped unfinished (its future was cancelled): nothing was taken from the source.
        let mut g = self.state.lock().expect("not poisoned");
        if g.generation == self.generation {
            g.wait = None;
            g.waker = None;
        }
    }
}

type ClaimFn = dyn Fn(Python<'_>, bool) -> PyResult<Option<Py<PyAny>>> + Send + Sync;

/// On the loop: hands the slot's item to the wait's future (`claim(True)`), or ends the wait
/// and leaves the item for the next caller (`claim(False)`, the future was cancelled).
#[pyclass(module = "pigeonhole", frozen)]
struct Claim(Box<ClaimFn>);

#[pymethods]
impl Claim {
    fn __call__(&self, py: Python<'_>, take: bool) -> PyResult<Option<Py<PyAny>>> {
        (self.0)(py, take)
    }
}

/// A wait's asyncio done callback: the wait is over, whether it completed or was cancelled.
#[pyclass(module = "pigeonhole", frozen)]
struct Over(Arc<AtomicBool>);

#[pymethods]
impl Over {
    fn __call__(&self, _fut: &Bound<'_, PyAny>) {
        self.0.store(true, Ordering::SeqCst);
    }
}
