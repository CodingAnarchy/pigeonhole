//! D308's byte-budgeted reads (#1308): a scan's or a multi-get's page within a byte budget,
//! the positions to resume them from, and a nearest search's page.

use crate::Maybe;
use std::ops::Bound as KeyBound;

use pigeonhole::KeySet;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::convert;
use crate::error::OrPy;
use crate::read::{Row, Snapshot, snapshot_arg};
use crate::table::Handle;

/// Runs `$body` with `$t` bound to either kind of table.
macro_rules! either {
    ($h:expr, |$t:ident| $body:expr) => {
        match $h {
            Handle::Writer($t) => $body,
            Handle::Reader($t) => $body,
        }
    };
}

/// A position's Python class: its bytes, `from_bytes`, its key, equality and hashing.
macro_rules! position {
    ($(#[$doc:meta])* $name:ident, $inner:ty, $key:ident, $key_doc:literal) => {
        $(#[$doc])*
        #[pyclass(module = "pigeonhole", frozen)]
        #[derive(Clone)]
        pub(crate) struct $name(pub(crate) $inner);

        #[pymethods]
        impl $name {
            /// The position as bytes: versioned and stable, so a client can hand them back
            /// later.
            fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
                PyBytes::new(py, &self.0.to_bytes())
            }

            /// The position `to_bytes` wrote; anything else (another kind of position
            /// included) raises `InvalidArgument`.
            #[staticmethod]
            fn from_bytes(b: &Bound<'_, PyAny>) -> PyResult<Self> {
                <$inner>::from_bytes(&convert::bytes(b)?).or_py().map(Self)
            }

            #[doc = $key_doc]
            #[getter]
            fn $key<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
                PyBytes::new(py, self.0.$key())
            }

            fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
                other.cast::<$name>().is_ok_and(|o| o.get().0 == self.0)
            }

            fn __hash__(&self) -> u64 {
                use std::hash::{Hash, Hasher};
                let mut h = std::hash::DefaultHasher::new();
                self.0.hash(&mut h);
                h.finish()
            }

            fn __repr__(&self) -> String {
                format!(
                    concat!(stringify!($name), "({}={:?})"),
                    stringify!($key),
                    String::from_utf8_lossy(self.0.$key())
                )
            }
        }
    };
}

position!(
    /// Where a `scan_within` page stopped, to resume from with `after=`: after its last row,
    /// after a column inside it when the page cut the row, or (an empty page) at its own
    /// start. It names its table: a read of another table refuses it (`InvalidArgument`).
    ScanPosition,
    pigeonhole::ScanPosition,
    row,
    "The row the position is in or after."
);

position!(
    /// Where a `multi_get_within` page stopped, to resume from with `after=`. It names the
    /// last key returned (and the column when the page cut its row), not an index into the
    /// keys, so keys added or removed between pages resume at the right key. It names its
    /// table: a read of another table refuses it (`InvalidArgument`).
    KeysPosition,
    pigeonhole::KeysPosition,
    key,
    "The key the position is at or after."
);

/// A page's class: its rows, size, whether the budget cut it, the next column's size, and
/// where to resume.
macro_rules! page {
    ($(#[$doc:meta])* $name:ident, $pos:ident) => {
        $(#[$doc])*
        #[pyclass(module = "pigeonhole", frozen)]
        pub(crate) struct $name {
            rows: Vec<Py<Row>>,
            bytes: u64,
            cut_by_budget: bool,
            next_bytes: Option<u64>,
            resume: Option<$pos>,
        }

        #[pymethods]
        impl $name {
            /// The page's rows, in key order. A row the budget cut holds the columns that
            /// fit.
            #[getter]
            fn rows(&self, py: Python<'_>) -> Vec<Py<Row>> {
                self.rows.iter().map(|r| r.clone_ref(py)).collect()
            }

            /// The page's size by D308's rule: each cell's qualifier, value and 16 bytes, and
            /// each row's key once. Never more than the budget.
            #[getter]
            fn bytes(&self) -> u64 {
                self.bytes
            }

            /// Whether the budget stopped the read (a `limit` stopping it first does not
            /// count).
            #[getter]
            fn cut_by_budget(&self) -> bool {
                self.cut_by_budget
            }

            /// When the budget cut the page, the size of the first column that didn't fit,
            /// its row key included: resumed with a budget of this many bytes, the read
            /// returns it.
            #[getter]
            fn next_bytes(&self) -> Option<u64> {
                self.next_bytes
            }

            /// Where to resume (`after=`): set whenever the read stopped before its input
            /// ended, `None` once it's exhausted.
            #[getter]
            fn resume(&self) -> Option<$pos> {
                self.resume.clone()
            }

            fn __len__(&self) -> usize {
                self.rows.len()
            }

            fn __repr__(&self) -> String {
                format!(
                    concat!(stringify!($name), "(rows={}, bytes={}, cut_by_budget={})"),
                    self.rows.len(),
                    self.bytes,
                    if self.cut_by_budget { "True" } else { "False" }
                )
            }
        }
    };
}

page!(
    /// A `scan_within` page (D308).
    ScanPage,
    ScanPosition
);

page!(
    /// A `multi_get_within` page (D308).
    KeysPage,
    KeysPosition
);

fn rows(py: Python<'_>, rows: Vec<pigeonhole::Row>) -> PyResult<Vec<Py<Row>>> {
    rows.into_iter().map(|r| Py::new(py, Row(r))).collect()
}

/// `Scan.read_within`, resumed `after` a position.
#[allow(clippy::too_many_arguments)]
pub(crate) fn scan_within(
    py: Python<'_>,
    h: &Handle,
    bytes: u64,
    prefix: Option<&Bound<'_, PyAny>>,
    start: Option<&Bound<'_, PyAny>>,
    end: Option<&Bound<'_, PyAny>>,
    families: Option<Vec<String>>,
    qualifier_prefix: Option<&Bound<'_, PyAny>>,
    versions: u32,
    limit: Option<u64>,
    column_limit: Option<u32>,
    snapshot: Option<&Bound<'_, Snapshot>>,
    after: Option<&ScanPosition>,
) -> PyResult<ScanPage> {
    let prefix = prefix.map(convert::bytes).transpose()?;
    let start = start.map(convert::bytes).transpose()?;
    let end = end.map(convert::bytes).transpose()?;
    if prefix.is_some() && (start.is_some() || end.is_some()) {
        return Err(PyValueError::new_err(
            "scan_within takes a prefix or start/end, not both",
        ));
    }
    let qp = qualifier_prefix.map(convert::bytes).transpose()?;
    let snap = snapshot_arg(snapshot)?;
    let after = after.map(|a| a.0.clone());
    let h = h.clone();
    let page = py
        .detach(move || {
            either!(&h, |t| {
                let mut s = match &prefix {
                    Some(p) => t.scan_prefix(p),
                    None => t.scan_bounds(
                        start
                            .as_deref()
                            .map_or(KeyBound::Unbounded, KeyBound::Included),
                        end.as_deref()
                            .map_or(KeyBound::Unbounded, KeyBound::Excluded),
                    ),
                }
                .versions(versions);
                s = s.maybe(families.as_ref(), |s, f| {
                    s.families(f.iter().map(String::as_str))
                });
                s = s.maybe(qp.as_ref(), |s, q| s.qualifier_prefix(q));
                s = s.maybe(limit, |s, n| s.limit(n));
                s = s.maybe(column_limit, |s, n| s.column_limit(n));
                s = s.maybe(snap.as_ref(), |s, sn| s.snapshot(sn));
                s = s.maybe(after.as_ref(), |s, a| s.resume_after(a));
                s.read_within(bytes)
            })
        })
        .or_py()?;
    let (bytes, cut, next, resume) = (
        page.bytes(),
        page.cut_by_budget(),
        page.next_bytes(),
        page.resume().cloned().map(ScanPosition),
    );
    Ok(ScanPage {
        rows: rows(py, page.into_rows())?,
        bytes,
        cut_by_budget: cut,
        next_bytes: next,
        resume,
    })
}

/// `KeysRead.read_within`, resumed `after` a position.
#[allow(clippy::too_many_arguments)]
pub(crate) fn multi_get_within(
    py: Python<'_>,
    h: &Handle,
    keys: &[Bound<'_, PyAny>],
    bytes: u64,
    families: Option<Vec<String>>,
    qualifier_prefix: Option<&Bound<'_, PyAny>>,
    versions: u32,
    column_limit: Option<u32>,
    snapshot: Option<&Bound<'_, Snapshot>>,
    after: Option<&KeysPosition>,
) -> PyResult<KeysPage> {
    let given: Vec<Vec<u8>> = keys.iter().map(convert::bytes).collect::<PyResult<_>>()?;
    let qp = qualifier_prefix.map(convert::bytes).transpose()?;
    let snap = snapshot_arg(snapshot)?;
    let after = after.map(|a| a.0.clone());
    let h = h.clone();
    let page = py
        .detach(move || {
            let set = KeySet::from_unsorted(&given);
            either!(&h, |t| {
                let mut r = t.keys(&set).versions(versions);
                r = r.maybe(families.as_ref(), |r, f| {
                    r.families(f.iter().map(String::as_str))
                });
                r = r.maybe(qp.as_ref(), |r, q| r.qualifier_prefix(q));
                r = r.maybe(column_limit, |r, n| r.column_limit(n));
                r = r.maybe(snap.as_ref(), |r, s| r.snapshot(s));
                r = r.maybe(after.as_ref(), |r, a| r.resume_after(a));
                r.read_within(bytes)
            })
        })
        .or_py()?;
    let (bytes, cut, next, resume) = (
        page.bytes(),
        page.cut_by_budget(),
        page.next_bytes(),
        page.resume().cloned().map(KeysPosition),
    );
    Ok(KeysPage {
        rows: rows(py, page.into_rows())?,
        bytes,
        cut_by_budget: cut,
        next_bytes: next,
        resume,
    })
}

/// A `nearest_within` page (D308 PR 3): the nearest neighbours, nearest first, that fit a
/// byte budget. There's no resume position: ask again with a larger budget or `k`.
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct NearestPage {
    pub(crate) neighbours: Vec<Py<crate::walk::Neighbour>>,
    pub(crate) bytes: u64,
    pub(crate) next_bytes: Option<u64>,
}

#[pymethods]
impl NearestPage {
    /// The neighbours that fit, nearest first, in `nearest`'s order.
    #[getter]
    fn neighbours(&self, py: Python<'_>) -> Vec<Py<crate::walk::Neighbour>> {
        self.neighbours.iter().map(|n| n.clone_ref(py)).collect()
    }

    /// The page's size: each neighbour's row key, qualifier and 16 bytes. Never more than
    /// the budget. The budget bounds the result, not the work: the search reads and ranks as
    /// `nearest` does, then keeps what fits.
    #[getter]
    fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Whether the budget left out a neighbour the search found (not `k` or the input
    /// running out).
    #[getter]
    fn cut_by_budget(&self) -> bool {
        self.next_bytes.is_some()
    }

    /// When the budget cut the page, the size of the first neighbour left out: a budget of
    /// `bytes + next_bytes` includes it.
    #[getter]
    fn next_bytes(&self) -> Option<u64> {
        self.next_bytes
    }

    fn __len__(&self) -> usize {
        self.neighbours.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "NearestPage(neighbours={}, bytes={})",
            self.neighbours.len(),
            self.bytes
        )
    }
}
