//! The change feed (D310, #1322): `Table.feed(...)`, `Feed`, `Commit` and `Change`.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use pigeonhole::ChangeKind;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::error::with_code;

/// The longest stretch `next` waits with the GIL released before it checks for a signal
/// (Ctrl-C), so a blocked consumer can still be interrupted.
const SLICE: Duration = Duration::from_millis(100);

/// A subscription to some tables' committed changes: whole commits, once each, in commit
/// order. Get one from `Table.feed(...)`.
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Feed(Arc<Mutex<FeedState>>, Arc<Condvar>);

/// The feed, and its one `next_async` wait (#1479).
struct FeedState {
    /// `None` once closed, or while a sync read has it out.
    feed: Option<pigeonhole::Feed>,
    /// A sync `next`/`try_next` has the feed out and waits on it with this lock released
    /// (cli692's P1: no engine wait ever holds this lock, which the bridge and the loop
    /// take). Other sync calls wait for it on the condvar; `next_async` refuses.
    out: bool,
    closed: bool,
    /// A commit a `next_async` took from the feed, not yet handed to its future, with the
    /// feed's token from before it (B5). It's the next commit every caller gets, and
    /// `token()` gives that token, so a resume from it replays the commit.
    slot: Option<(pigeonhole::Commit, Vec<u8>)>,
    /// The wait in progress, if any.
    wait: Option<Wait>,
    /// Counts `next_async` waits: a wait whose generation isn't the latest was superseded
    /// (its future was cancelled), and never touches the feed or the slot again.
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

impl FeedState {
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

/// What a sync read of the feed can't do.
enum Refused {
    Db(pigeonhole::Error),
    /// A wait looks in progress: its flag and future, to ask whether it's done.
    Waiting(Arc<AtomicBool>, Arc<Py<PyAny>>),
    Closed,
}

fn refused(r: Refused) -> PyErr {
    match r {
        Refused::Db(e) => crate::error::OrPy::or_py(Err::<(), _>(e)).unwrap_err(),
        Refused::Waiting(..) => waiting(),
        Refused::Closed => closed(),
    }
}

/// Whether the wait's future is done (completed or cancelled); if so, marks it over.
fn owner_done(py: Python<'_>, over: &AtomicBool, owner: &Py<PyAny>) -> PyResult<bool> {
    let done = owner.bind(py).call_method0("done")?.is_truthy()?;
    if done {
        over.store(true, Ordering::SeqCst);
    }
    Ok(done)
}

fn waiting() -> PyErr {
    PyValueError::new_err("a next_async wait is in progress on this feed")
}

fn closed() -> PyErr {
    with_code(pigeonhole::ErrorCode::Closed, "the feed is closed")
}

impl Feed {
    pub(crate) fn new(feed: pigeonhole::Feed) -> Self {
        Self(
            Arc::new(Mutex::new(FeedState {
                feed: Some(feed),
                out: false,
                closed: false,
                slot: None,
                wait: None,
                generation: 0,
            })),
            Arc::new(Condvar::new()),
        )
    }

    /// One wait of at most `slice`, with the GIL released: the slot's commit first.
    fn poll(&self, py: Python<'_>, slice: Option<Duration>) -> PyResult<Option<Commit>> {
        match self.poll_once(py, slice) {
            // The wait may be over already (cancelled and awaited): ask, then try again.
            Err(Refused::Waiting(over, owner)) if owner_done(py, &over, &owner)? => self
                .poll_once(py, slice)
                .map(|c| c.map(Commit))
                .map_err(refused),
            r => r.map(|c| c.map(Commit)).map_err(refused),
        }
    }

    fn poll_once(
        &self,
        py: Python<'_>,
        slice: Option<Duration>,
    ) -> Result<Option<pigeonhole::Commit>, Refused> {
        py.detach(|| {
            let mut g = self.lock_unread();
            if g.waiting() {
                let w = g.wait.as_ref().expect("waiting");
                return Err(Refused::Waiting(Arc::clone(&w.over), Arc::clone(&w.owner)));
            }
            if let Some((c, _)) = g.slot.take() {
                return Ok(Some(c));
            }
            let feed = g.feed.take().ok_or(Refused::Closed)?;
            g.out = true;
            // A cancelled wait still being dropped is superseded: it never polls the feed.
            g.generation += 1;
            drop(g);
            // Puts the feed back and wakes the sync callers taking turns, even if the wait
            // panics (cli692's N3).
            let mut out = Out {
                feed: Some(feed),
                home: self,
            };
            let feed = out.feed.as_mut().expect("just taken");
            let next = match slice {
                Some(d) => feed.next(d),
                None => feed.try_next(),
            }
            .map(|c| c.cloned());
            drop(out);
            next.map_err(Refused::Db)
        })
    }

    /// The state, once no sync read has the feed out (without the GIL: a read waits up to
    /// a slice).
    fn lock_unread(&self) -> std::sync::MutexGuard<'_, FeedState> {
        let mut g = self.0.lock().expect("not poisoned");
        while g.out {
            g = self.1.wait(g).expect("not poisoned");
        }
        g
    }

    /// `next` with an optional timeout: in slices, checking for signals between them.
    fn wait(&self, py: Python<'_>, timeout: Option<Duration>) -> PyResult<Option<Commit>> {
        let deadline = timeout.and_then(|t| Instant::now().checked_add(t));
        loop {
            let slice = match deadline {
                None => SLICE,
                Some(d) => d.saturating_duration_since(Instant::now()).min(SLICE),
            };
            if let Some(c) = self.poll(py, Some(slice))? {
                return Ok(Some(c));
            }
            py.check_signals()?;
            if deadline.is_some_and(|d| Instant::now() >= d) {
                return Ok(None);
            }
        }
    }
}

/// A sync read's hold on the feed, out of the state: dropped (even by a panic), it puts the
/// feed back, unless the feed was closed meanwhile, and wakes the callers waiting a turn.
struct Out<'a> {
    feed: Option<pigeonhole::Feed>,
    home: &'a Feed,
}

impl Drop for Out<'_> {
    fn drop(&mut self) {
        let mut g = self
            .home
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        g.out = false;
        if !g.closed {
            g.feed = self.feed.take();
        }
        drop(g);
        self.home.1.notify_all();
    }
}

/// A `next_async` wait's Rust future, polled on the bridge without the GIL. A commit it
/// takes goes to the slot first; the loop thread then claims it for the wait's future, or
/// leaves it for the next caller if that future was cancelled meanwhile.
struct FeedWait {
    state: Arc<Mutex<FeedState>>,
    generation: u64,
    token: Option<Vec<u8>>,
    finished: bool,
}

impl FeedWait {
    fn claim(&self) -> crate::aio::Output {
        let claim = Claim {
            state: Arc::clone(&self.state),
            generation: self.generation,
        };
        Box::new(move |py| match Py::new(py, claim) {
            Ok(c) => crate::aio::Entry::Claim(c.into_any()),
            Err(e) => crate::aio::Entry::Error(e),
        })
    }
}

impl Future for FeedWait {
    type Output = crate::aio::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let state = Arc::clone(&this.state);
        let mut g = state.lock().expect("not poisoned");
        if g.generation != this.generation {
            // Superseded: the future was cancelled and a newer wait owns the feed.
            this.finished = true;
            return Poll::Ready(Box::new(|py| crate::aio::Entry::Value(py.None())));
        }
        if g.slot.is_some() {
            // Left by a superseded wait that took it after its cancel.
            this.finished = true;
            return Poll::Ready(this.claim());
        }
        if g.out {
            // A sync read has the feed: only a superseded wait gets here, and the generation
            // check above has already ended it.
            return Poll::Pending;
        }
        let Some(feed) = g.feed.as_mut() else {
            g.wait = None;
            this.finished = true;
            return Poll::Ready(Box::new(|_| crate::aio::Entry::Error(closed())));
        };
        match feed.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(c)) => {
                let c = c.clone();
                let token = this.token.take().unwrap_or_default();
                g.slot = Some((c, token));
                this.finished = true;
                Poll::Ready(this.claim())
            }
            Poll::Ready(Err(e)) => {
                g.wait = None;
                this.finished = true;
                Poll::Ready(crate::aio::output(Err::<(), _>(e), |py, ()| Ok(py.None())))
            }
        }
    }
}

impl Drop for FeedWait {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Dropped unfinished (its future was cancelled): nothing was taken from the feed.
        let mut g = self.state.lock().expect("not poisoned");
        if g.generation == self.generation {
            g.wait = None;
        }
    }
}

/// On the loop: hands the slot's commit to the wait's future (`claim(True)`), or ends the
/// wait and leaves the commit for the next caller (`claim(False)`, the future was cancelled).
#[pyclass(module = "pigeonhole", frozen)]
struct Claim {
    state: Arc<Mutex<FeedState>>,
    generation: u64,
}

#[pymethods]
impl Claim {
    fn __call__(&self, take: bool) -> Option<Commit> {
        let mut g = self.state.lock().expect("not poisoned");
        if g.generation != self.generation {
            return None;
        }
        g.wait = None;
        if take {
            g.slot.take().map(|(c, _)| Commit(c))
        } else {
            None
        }
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

#[pymethods]
impl Feed {
    /// The next commit, waiting up to `timeout` (seconds or a `timedelta`; `None` waits
    /// until one comes); `None` if none came in time. The GIL is released while it waits.
    /// Raises `FeedExpired` once the history the feed needs is gone, and from then on.
    #[pyo3(signature = (timeout=None))]
    fn next(&self, py: Python<'_>, timeout: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Commit>> {
        let timeout = timeout.map(crate::convert::duration).transpose()?;
        self.wait(py, timeout)
    }

    /// The next commit if one is ready, else `None`. Never waits.
    fn try_next(&self, py: Python<'_>) -> PyResult<Option<Commit>> {
        self.poll(py, None)
    }

    /// The next commit, as an `asyncio.Future` of the running loop (#1479): no thread
    /// waits for it. For a timeout, `asyncio.wait_for(feed.next_async(), t)`. One wait at a
    /// time: a second `next_async`, or a sync `next`, during one raises `ValueError`.
    /// Cancelling it loses nothing: the commit it would have returned is the next one any
    /// call returns.
    fn next_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (event_loop, fut) = crate::aio::new_future(py)?;
        // Brief: no engine wait holds this lock (P1).
        let mut g = self.0.lock().expect("not poisoned");
        if g.out {
            return Err(PyValueError::new_err(
                "a sync next is in progress on this feed",
            ));
        }
        if g.waiting() {
            let w = g.wait.as_ref().expect("waiting");
            if !owner_done(py, &w.over, &w.owner)? {
                return Err(waiting());
            }
            g.wait = None;
        }
        if let Some((c, _)) = g.slot.take() {
            drop(g);
            let c = Py::new(py, Commit(c))?.into_any();
            crate::aio::complete_now(py, &fut, crate::aio::Entry::Value(c))?;
            return Ok(fut);
        }
        let token = g
            .feed
            .as_ref()
            .ok_or_else(closed)?
            .token()
            .as_bytes()
            .to_vec();
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
        crate::aio::start(py, &event_loop, &fut, move || FeedWait {
            state,
            generation,
            token: Some(token),
            finished: false,
        })?;
        Ok(fut)
    }

    /// Where the feed is, as bytes: `Table.feed(resume=...)` continues after the last
    /// commit delivered, in this open of the database or a later one. Store it with what
    /// the commits fed.
    fn token<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        // The lock may be held by a thread waiting in `next`, for up to a slice: wait for
        // it without the GIL.
        let token = py.detach(|| {
            let g = self.lock_unread();
            // A commit in the slot isn't delivered yet: the token from before it (B5).
            match (&g.slot, &g.feed) {
                (Some((_, t)), _) => Some(t.clone()),
                (None, Some(f)) => Some(f.token().as_bytes().to_vec()),
                (None, None) => None,
            }
        });
        let token = token.ok_or_else(closed)?;
        Ok(PyBytes::new(py, &token))
    }

    /// Once the feed expired: the oldest commit seqno its history still holds, when known.
    #[getter]
    fn oldest_retained(&self, py: Python<'_>) -> Option<u64> {
        py.detach(|| {
            let g = self.lock_unread();
            g.feed.as_ref().and_then(pigeonhole::Feed::oldest_retained)
        })
    }

    /// The commits as they come: `for commit in feed:` waits for each (`None` never ends
    /// it). `feed.iter(timeout=...)` stops once one doesn't come in time.
    #[pyo3(signature = (timeout=None))]
    fn iter(slf: Py<Self>, timeout: Option<&Bound<'_, PyAny>>) -> PyResult<FeedIter> {
        Ok(FeedIter {
            feed: slf,
            timeout: timeout.map(crate::convert::duration).transpose()?,
        })
    }

    fn __iter__(slf: Py<Self>) -> FeedIter {
        FeedIter {
            feed: slf,
            timeout: None,
        }
    }

    /// Ends the subscription now rather than when the feed is collected. Calls afterwards
    /// raise `Closed`; closing twice does nothing.
    fn close(&self, py: Python<'_>) {
        py.detach(|| {
            let mut g = self.0.lock().expect("not poisoned");
            // A sync read that has the feed out drops it when it comes back.
            g.closed = true;
            g.feed = None;
            g.slot = None;
        });
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &self,
        _ty: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _tb: &Bound<'_, PyAny>,
    ) -> bool {
        self.close(_ty.py());
        false
    }
}

/// A feed's commits as an iterator (`iter(feed)`, `feed.iter(timeout=...)`).
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct FeedIter {
    feed: Py<Feed>,
    timeout: Option<Duration>,
}

#[pymethods]
impl FeedIter {
    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Commit>> {
        self.feed.get().wait(py, self.timeout)
    }
}

/// One whole commit: its changes in the feed's tables and row prefix.
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Commit(pigeonhole::Commit);

#[pymethods]
impl Commit {
    /// Its sequence number: the order commits arrive in, as `Snapshot.seqno` counts.
    #[getter]
    fn seqno(&self) -> u64 {
        self.0.seqno()
    }

    /// Its commit timestamp, in microseconds since the Unix epoch.
    #[getter]
    fn timestamp(&self) -> u64 {
        self.0.timestamp()
    }

    /// Its changes, in the order the commit wrote them.
    #[getter]
    fn changes(&self) -> Vec<Change> {
        self.0.changes().iter().cloned().map(Change).collect()
    }

    fn __len__(&self) -> usize {
        self.0.changes().len()
    }

    fn __repr__(&self) -> String {
        format!(
            "Commit(seqno={}, timestamp={}, changes={})",
            self.0.seqno(),
            self.0.timestamp(),
            self.0.changes().len()
        )
    }
}

/// One mutation of a commit.
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Change(pigeonhole::Change);

#[pymethods]
impl Change {
    /// The table's name.
    #[getter]
    fn table(&self) -> &str {
        self.0.table()
    }

    /// The family's name.
    #[getter]
    fn family(&self) -> &str {
        self.0.family()
    }

    /// What it did: `"put"`, `"merge"` (a merge operand, a counter increment or an
    /// aggregate input), `"delete_cell"`, `"delete_column"` or `"delete_family"` (a row
    /// delete is one `"delete_family"` per family).
    #[getter]
    fn kind(&self) -> &'static str {
        kind_name(self.0.kind())
    }

    /// The row key.
    #[getter]
    fn row<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.0.row())
    }

    /// The qualifier (empty for `"delete_family"`).
    #[getter]
    fn qualifier<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.0.qualifier())
    }

    /// Its timestamp, in microseconds since the Unix epoch.
    #[getter]
    fn timestamp(&self) -> u64 {
        self.0.timestamp()
    }

    /// The value written or merged, typed as `Cell.typed` types it; `None` for deletes.
    #[getter]
    fn value<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        self.0
            .value()
            .map(|v| crate::read::typed(py, v))
            .transpose()
    }

    fn __repr__(&self) -> String {
        format!(
            "Change(table={:?}, family={:?}, kind={:?}, row={:?}, timestamp={})",
            self.0.table(),
            self.0.family(),
            kind_name(self.0.kind()),
            String::from_utf8_lossy(self.0.row()),
            self.0.timestamp()
        )
    }
}

fn kind_name(kind: ChangeKind) -> &'static str {
    match kind {
        ChangeKind::Put => "put",
        ChangeKind::Merge => "merge",
        ChangeKind::DeleteCell => "delete_cell",
        ChangeKind::DeleteColumn => "delete_column",
        ChangeKind::DeleteFamily => "delete_family",
        // A kind this build doesn't name.
        _ => "unknown",
    }
}
