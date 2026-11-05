//! A scan's cells as Arrow batches (#1480, ICR 0120): [`ArrowStream`], from
//! `ScanIter.to_arrow()`, hands them to pyarrow, Polars, DuckDB or anything else that takes
//! the Arrow PyCapsule protocol (`__arrow_c_stream__`), through the Arrow C stream interface.
//! The batches are `pigeonhole-arrow`'s, so the schema is the Rust crate's and `phdb dump
//! --format arrow`'s.
//!
//! The capsule holds the `FFI_ArrowArrayStream` by value (pyo3 boxes it and points the
//! capsule at the box). A consumer moves the struct out, which nulls its `release`; the
//! capsule's destructor drops what's left, and the struct's own `Drop` calls `release` only
//! if it's still set, so the stream is released exactly once either way. No `unsafe` here.

use std::ffi::CStr;
use std::sync::Mutex;

use pigeonhole_arrow::arrow_schema::SchemaRef;
use pigeonhole_arrow::arrow_schema::ffi::FFI_ArrowSchema;
use pigeonhole_arrow::{ArrowOptions, ArrowScan, TsType};
use pyo3::exceptions::{PyImportError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

/// The protocol's capsule names.
const STREAM: &CStr = c"arrow_array_stream";
const SCHEMA: &CStr = c"arrow_schema";

/// `to_arrow`'s options as `pigeonhole-arrow`'s.
pub(crate) fn options(
    batch_cells: usize,
    batch_bytes: usize,
    row_dictionary: bool,
    utf8_keys: bool,
    typed_values: bool,
    ts: &str,
    vector_dim: Option<u32>,
) -> PyResult<ArrowOptions> {
    let ts = match ts {
        "timestamp" => TsType::Timestamp,
        "uint64" => TsType::UInt64,
        other => {
            return Err(PyValueError::new_err(format!(
                "ts is \"timestamp\" or \"uint64\", not {other:?}"
            )));
        }
    };
    if batch_cells == 0 || batch_bytes == 0 {
        return Err(PyValueError::new_err(
            "batch_cells and batch_bytes are at least 1",
        ));
    }
    let o = ArrowOptions::default()
        .batch_cells(batch_cells)
        .batch_bytes(batch_bytes)
        .row_dictionary(row_dictionary)
        .utf8_keys(utf8_keys)
        .typed_values(typed_values)
        .ts(ts);
    Ok(match vector_dim {
        Some(0) | Some(4097..) => {
            return Err(PyValueError::new_err("vector_dim is 1 to 4096"));
        }
        Some(d) => o.vector_dim(d),
        None => o,
    })
}

/// A scan's cells as Arrow batches, for one consumer: pass it to `pyarrow`, Polars or
/// DuckDB (the Arrow PyCapsule protocol), or call `to_pyarrow()`.
#[pyclass(module = "pigeonhole")]
pub(crate) struct ArrowStream {
    scan: Mutex<Option<ArrowScan<'static>>>,
    schema: SchemaRef,
}

impl ArrowStream {
    pub(crate) fn new(scan: ArrowScan<'static>) -> Self {
        Self {
            schema: scan.schema(),
            scan: Mutex::new(Some(scan)),
        }
    }
}

#[pymethods]
impl ArrowStream {
    /// The Arrow C stream, as a PyCapsule (the Arrow PyCapsule protocol). Once only: the
    /// consumer owns the stream, and the scan with it. `requested_schema` is ignored: the
    /// stream has this export's schema, which the consumer may cast.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let scan = self
            .scan
            .lock()
            .expect("not poisoned")
            .take()
            .ok_or_else(|| PyValueError::new_err("this Arrow stream was already read or closed"))?;
        PyCapsule::new_with_value(py, scan.into_ffi_stream(), STREAM)
    }

    /// The stream's schema, as a PyCapsule (the Arrow PyCapsule protocol).
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let schema = FFI_ArrowSchema::try_from(self.schema.as_ref())
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        PyCapsule::new_with_value(py, schema, SCHEMA)
    }

    /// The columns' names, in order.
    #[getter]
    fn schema_names(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// The stream as a `pyarrow.RecordBatchReader`. Needs `pyarrow` (the `arrow` extra:
    /// `pip install pigeonhole-db[arrow]`).
    fn to_pyarrow<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let pa = py.import("pyarrow").map_err(|_| {
            PyImportError::new_err(
                "to_pyarrow() needs pyarrow: pip install pigeonhole-db[arrow], or pass the \
                 stream to another Arrow consumer",
            )
        })?;
        pa.getattr("RecordBatchReader")?
            .call_method1("from_stream", (slf,))
    }

    /// Ends the scan if the stream wasn't handed to a consumer: its read (and the snapshot
    /// it holds, a reader process's slot included) is released now. Closing twice, or after
    /// the stream was read, does nothing.
    fn close(&self) {
        self.scan.lock().expect("not poisoned").take();
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        _ty: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _tb: &Bound<'_, PyAny>,
    ) -> bool {
        self.close();
        false
    }
}
