//! Read results: cells, rows, snapshots and the scan iterator.

use std::collections::VecDeque;
use std::sync::Mutex;

use pigeonhole::Value;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::error::{OrPy, with_code};

/// One version of a cell: its value and timestamp (microseconds since the Unix epoch).
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Cell(pub(crate) pigeonhole::Cell);

#[pymethods]
impl Cell {
    /// The value's bytes, as stored (an `i64`/`f64` cell's 8 bytes, little-endian).
    #[getter]
    fn value<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.0.value())
    }

    /// The timestamp, in microseconds since the Unix epoch.
    #[getter]
    fn ts(&self) -> u64 {
        self.0.timestamp()
    }

    /// The value as its type: `bytes`, `int` (an `i64` cell) or `float` (an `f64` cell).
    #[getter]
    fn typed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        typed(py, self.0.typed())
    }

    /// The value as an `int`, if it is an `i64` cell (counters included).
    fn as_int(&self) -> Option<i64> {
        self.0.as_i64()
    }

    /// The value's 8 bytes as an unsigned `int` (bitwise aggregate families, `put_u64`).
    fn as_u64(&self) -> Option<u64> {
        self.0.as_u64()
    }

    /// The value as a `float`, if it is an `f64` cell.
    fn as_float(&self) -> Option<f64> {
        self.0.as_f64()
    }

    /// An `hll` family's distinct-count estimate.
    fn hll_estimate(&self) -> Option<u64> {
        self.0.hll_estimate()
    }

    /// A `sum_f64` family's exact sum, rounded once.
    fn sum_f64(&self) -> Option<f64> {
        self.0.sum_f64()
    }

    /// A `mean_var` family's `(count, mean, variance)`.
    fn mean_var(&self) -> Option<(u64, f64, f64)> {
        self.0
            .mean_var()
            .map(|m| (m.count(), m.mean(), m.variance()))
    }

    /// A `histogram` family's bucket counts.
    fn histogram_counts(&self) -> Option<Vec<u64>> {
        self.0.histogram_counts()
    }

    /// A `quantiles` family's estimate of quantile `q` (0 to 1).
    fn quantile(&self, q: f64) -> Option<f64> {
        self.0.quantile(q)
    }

    /// Whether a `bloom` family may contain `element` (an `int`, or bytes).
    fn bloom_contains(&self, element: &Bound<'_, PyAny>) -> PyResult<Option<bool>> {
        if let Ok(n) = element.extract::<i64>() {
            return Ok(self.0.bloom_contains(n));
        }
        Ok(self
            .0
            .bloom_contains_bytes(&crate::convert::bytes(element)?))
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "Cell(ts={}, value={})",
            self.0.timestamp(),
            typed(py, self.0.typed())?.repr()?
        ))
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        other.cast::<Cell>().is_ok_and(|o| {
            o.get().0.value() == self.0.value() && o.get().0.timestamp() == self.0.timestamp()
        })
    }
}

pub(crate) fn typed<'py>(py: Python<'py>, v: Value<'_>) -> PyResult<Bound<'py, PyAny>> {
    Ok(match v {
        Value::I64(n) | Value::Varint(n) => n.into_pyobject(py)?.into_any(),
        Value::F64(x) => x.into_pyobject(py)?.into_any(),
        Value::Bytes(b) => PyBytes::new(py, b).into_any(),
        // A value type this build doesn't know: its bytes.
        _ => py.None().into_bound(py),
    })
}

/// A row: its key and cells, by family (in the read's family order), then qualifier, then
/// newest version first. Iterating gives `(family, qualifier, cell)` tuples.
#[pyclass(module = "pigeonhole", frozen, sequence)]
pub(crate) struct Row(pub(crate) pigeonhole::Row);

#[pymethods]
impl Row {
    /// The row key.
    #[getter]
    fn key<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.0.key())
    }

    /// The newest version of `family:qualifier` in the row, if the read returned it.
    fn get(&self, family: &str, qualifier: &Bound<'_, PyAny>) -> PyResult<Option<Cell>> {
        let q = crate::convert::bytes(qualifier)?;
        Ok(self.0.get(family, &q).map(|c| Cell(c.clone())))
    }

    /// The cells as `(family, qualifier, cell)` tuples.
    fn cells<'py>(&self, py: Python<'py>) -> Vec<(String, Bound<'py, PyBytes>, Cell)> {
        (0..self.0.len())
            .filter_map(|i| self.0.entry(i))
            .map(|e| {
                (
                    e.family.to_owned(),
                    PyBytes::new(py, e.qualifier),
                    Cell(e.cell.to_owned()),
                )
            })
            .collect()
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.cells(py).into_pyobject(py)?.call_method0("__iter__")
    }

    fn __repr__(&self) -> String {
        format!(
            "Row(key={:?}, cells={})",
            String::from_utf8_lossy(self.0.key()),
            self.0.len()
        )
    }
}

/// A consistent view of the database as of one point. Reads that take it see exactly what
/// was committed then. It pins data until released: use it as a context manager, or call
/// `release()`.
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Snapshot(Mutex<Option<pigeonhole::Snapshot>>);

impl Snapshot {
    pub(crate) fn new(s: pigeonhole::Snapshot) -> Self {
        Self(Mutex::new(Some(s)))
    }

    /// The snapshot, or `SnapshotExpired` once released.
    pub(crate) fn get(&self) -> PyResult<pigeonhole::Snapshot> {
        self.0.lock().expect("not poisoned").clone().ok_or_else(|| {
            with_code(
                pigeonhole::ErrorCode::SnapshotExpired,
                "the snapshot was released",
            )
        })
    }
}

#[pymethods]
impl Snapshot {
    /// The snapshot's sequence number.
    #[getter]
    fn seqno(&self) -> PyResult<u64> {
        Ok(self.get()?.seqno())
    }

    /// Releases the snapshot: reads with it fail with `SnapshotExpired` afterwards.
    fn release(&self) {
        self.0.lock().expect("not poisoned").take();
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
        self.release();
        false
    }
}

/// An optional `Snapshot` argument.
pub(crate) fn snapshot_arg(
    s: Option<&Bound<'_, Snapshot>>,
) -> PyResult<Option<pigeonhole::Snapshot>> {
    s.map(|s| s.get().get()).transpose()
}

/// The rows of a scan, in key order. Rows are read in chunks with the GIL released, then
/// handed out one at a time.
#[pyclass(module = "pigeonhole")]
pub(crate) struct ScanIter {
    rows: Mutex<Option<pigeonhole::RowIter<'static>>>,
    buffered: VecDeque<pigeonhole::Row>,
    chunk: usize,
}

impl ScanIter {
    pub(crate) fn new(scan: pigeonhole::Scan<'static>, chunk: usize) -> PyResult<Self> {
        if chunk == 0 {
            return Err(PyValueError::new_err("chunk must be at least 1"));
        }
        Ok(Self {
            rows: Mutex::new(Some(scan.iter().or_py()?)),
            buffered: VecDeque::new(),
            chunk,
        })
    }
}

#[pymethods]
impl ScanIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Row>> {
        if self.buffered.is_empty() {
            let chunk = self.chunk;
            let rows = &self.rows;
            let fetched = py.detach(|| -> pigeonhole::Result<Vec<pigeonhole::Row>> {
                let mut guard = rows.lock().expect("not poisoned");
                let Some(it) = guard.as_mut() else {
                    return Ok(Vec::new());
                };
                let mut out = Vec::with_capacity(chunk);
                while out.len() < chunk {
                    match it.next_ref()? {
                        Some(r) => out.push(r.to_owned()),
                        None => {
                            *guard = None;
                            break;
                        }
                    }
                }
                Ok(out)
            });
            self.buffered.extend(fetched.or_py()?);
        }
        Ok(self.buffered.pop_front().map(Row))
    }
}
