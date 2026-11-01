//! Python arguments to Rust: byte strings, cell values, durations and durability.

use std::time::Duration;

use pyo3::exceptions::{PyOverflowError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyDelta, PyString};

/// A row key, qualifier or byte value: `bytes`, `bytearray`, `memoryview`, or `str` (as
/// UTF-8).
pub(crate) fn bytes(ob: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(b) = ob.cast::<PyBytes>() {
        return Ok(b.as_bytes().to_vec());
    }
    if let Ok(s) = ob.cast::<PyString>() {
        return Ok(s.to_str()?.as_bytes().to_vec());
    }
    if let Ok(b) = ob.cast::<PyByteArray>() {
        return Ok(b.to_vec());
    }
    if ob.hasattr("tobytes")? {
        // A memoryview, or another buffer that can copy itself out.
        let b = ob.call_method0("tobytes")?;
        if let Ok(b) = b.cast::<PyBytes>() {
            return Ok(b.as_bytes().to_vec());
        }
    }
    Err(PyTypeError::new_err(format!(
        "expected bytes, bytearray, memoryview or str, not {}",
        ob.get_type().name()?
    )))
}

/// A cell value as written: the Python type picks the cell type.
#[derive(Debug, Clone)]
pub(crate) enum Val {
    Bytes(Vec<u8>),
    I64(i64),
    F64(f64),
}

/// `bytes`-like and `str` are byte values, `int` an `i64`, `float` an `f64`. `bool` is
/// refused: `True` is an `int` in Python, and a stored 1 is rarely what was meant.
pub(crate) fn value(ob: &Bound<'_, PyAny>) -> PyResult<Val> {
    if ob.is_instance_of::<pyo3::types::PyBool>() {
        return Err(PyTypeError::new_err(
            "a bool is not a cell value: write 1 or 0, or bytes",
        ));
    }
    if ob.is_instance_of::<pyo3::types::PyInt>() {
        return ob.extract::<i64>().map(Val::I64).map_err(|_| {
            PyOverflowError::new_err("an int value must fit in an i64 (put_u64 writes a u64)")
        });
    }
    if ob.is_instance_of::<pyo3::types::PyFloat>() {
        return Ok(Val::F64(ob.extract()?));
    }
    bytes(ob).map(Val::Bytes)
}

/// A duration: a `timedelta`, or seconds as an `int` or `float`.
pub(crate) fn duration(ob: &Bound<'_, PyAny>) -> PyResult<Duration> {
    let secs: f64 = if let Ok(d) = ob.cast::<PyDelta>() {
        d.call_method0("total_seconds")?.extract()?
    } else {
        ob.extract()?
    };
    Duration::try_from_secs_f64(secs)
        .map_err(|_| PyValueError::new_err("a duration must be a non-negative, finite time"))
}
