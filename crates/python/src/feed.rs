//! The change feed (D310, #1322): `Table.feed(...)`, `Feed`, `Commit` and `Change`.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use pigeonhole::ChangeKind;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::error::{OrPy, with_code};

/// The longest stretch `next` waits with the GIL released before it checks for a signal
/// (Ctrl-C), so a blocked consumer can still be interrupted.
const SLICE: Duration = Duration::from_millis(100);

/// A subscription to some tables' committed changes: whole commits, once each, in commit
/// order. Get one from `Table.feed(...)`.
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Feed(Mutex<Option<pigeonhole::Feed>>);

impl Feed {
    pub(crate) fn new(feed: pigeonhole::Feed) -> Self {
        Self(Mutex::new(Some(feed)))
    }

    /// One wait of at most `slice`, with the GIL released.
    fn poll(&self, py: Python<'_>, slice: Option<Duration>) -> PyResult<Option<Commit>> {
        // `None` for a closed feed.
        let polled = py.detach(|| {
            let mut guard = self.0.lock().expect("not poisoned");
            let Some(feed) = guard.as_mut() else {
                return Ok(None);
            };
            let next = match slice {
                Some(d) => feed.next(d)?,
                None => feed.try_next()?,
            };
            Ok::<_, pigeonhole::Error>(Some(next.cloned().map(Commit)))
        });
        polled
            .or_py()?
            .ok_or_else(|| with_code(pigeonhole::ErrorCode::Closed, "the feed is closed"))
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

    /// Where the feed is, as bytes: `Table.feed(resume=...)` continues after the last
    /// commit delivered, in this open of the database or a later one. Store it with what
    /// the commits fed.
    fn token<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        // The lock may be held by a thread waiting in `next`, for up to a slice: wait for
        // it without the GIL.
        let token = py.detach(|| {
            let guard = self.0.lock().expect("not poisoned");
            guard.as_ref().map(|f| f.token())
        });
        let token =
            token.ok_or_else(|| with_code(pigeonhole::ErrorCode::Closed, "the feed is closed"))?;
        Ok(PyBytes::new(py, token.as_bytes()))
    }

    /// Once the feed expired: the oldest commit seqno its history still holds, when known.
    #[getter]
    fn oldest_retained(&self, py: Python<'_>) -> Option<u64> {
        py.detach(|| {
            let guard = self.0.lock().expect("not poisoned");
            guard.as_ref().and_then(pigeonhole::Feed::oldest_retained)
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
        py.detach(|| self.0.lock().expect("not poisoned").take());
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
