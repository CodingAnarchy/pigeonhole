//! Vectors (D303): vector families' specs, values written from Python buffers or sequences,
//! and vector cells read back as `array.array` (`'f'`, `'b'`) or `bytes` (bits).

use pigeonhole::{Element, Metric};
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::error::OrPy;

/// `pigeonhole.Element` (an `enum.StrEnum` in the package) or its value.
pub(crate) fn element(ob: &Bound<'_, PyAny>) -> PyResult<Element> {
    match &*ob.str()?.to_cow()? {
        "f32" => Ok(Element::F32),
        "i8" => Ok(Element::I8),
        "bit" => Ok(Element::Bit),
        other => Err(PyValueError::new_err(format!(
            "unknown vector element {other:?} (f32, i8 or bit)"
        ))),
    }
}

/// `pigeonhole.Metric` (an `enum.StrEnum` in the package) or its value.
pub(crate) fn metric(ob: &Bound<'_, PyAny>) -> PyResult<Metric> {
    match &*ob.str()?.to_cow()? {
        "l2" => Ok(Metric::L2),
        "cosine" => Ok(Metric::Cosine),
        "inner_product" => Ok(Metric::InnerProduct),
        "hamming" => Ok(Metric::Hamming),
        other => Err(PyValueError::new_err(format!(
            "unknown metric {other:?} (l2, cosine, inner_product or hamming)"
        ))),
    }
}

fn element_name(e: Element) -> &'static str {
    match e {
        Element::F32 => "f32",
        Element::I8 => "i8",
        Element::Bit => "bit",
        _ => "unknown",
    }
}

fn metric_name(m: Metric) -> &'static str {
    match m {
        Metric::L2 => "l2",
        Metric::Cosine => "cosine",
        Metric::InnerProduct => "inner_product",
        Metric::Hamming => "hamming",
        _ => "unknown",
    }
}

/// A vector family's spec: its dimension, element type and metric.
#[pyclass(module = "pigeonhole", frozen)]
#[derive(Clone)]
pub(crate) struct VectorSpec(pub(crate) pigeonhole::VectorSpec);

impl VectorSpec {
    pub(crate) fn new_spec(
        dim: u32,
        element: &Bound<'_, PyAny>,
        metric: &Bound<'_, PyAny>,
    ) -> PyResult<pigeonhole::VectorSpec> {
        pigeonhole::VectorSpec::new(dim, self::element(element)?, self::metric(metric)?).or_py()
    }
}

#[pymethods]
impl VectorSpec {
    /// A spec: `dim` elements of `element` (`Element.F32`, `I8` or `BIT`), compared by
    /// `metric` (`Metric.L2`, `COSINE`, `INNER_PRODUCT` or `HAMMING`).
    #[new]
    fn new(dim: u32, element: &Bound<'_, PyAny>, metric: &Bound<'_, PyAny>) -> PyResult<Self> {
        Self::new_spec(dim, element, metric).map(Self)
    }

    /// The dimension.
    #[getter]
    fn dim(&self) -> u32 {
        self.0.dim()
    }

    /// The element type's value (`"f32"`, `"i8"` or `"bit"`; `pigeonhole.Element`).
    #[getter]
    fn element(&self) -> &'static str {
        element_name(self.0.element())
    }

    /// The metric's value (`"l2"`, `"cosine"`, `"inner_product"` or `"hamming"`;
    /// `pigeonhole.Metric`).
    #[getter]
    fn metric(&self) -> &'static str {
        metric_name(self.0.metric())
    }

    fn __repr__(&self) -> String {
        format!(
            "VectorSpec(dim={}, element={:?}, metric={:?})",
            self.0.dim(),
            self.element(),
            self.metric()
        )
    }
}

/// A vector as written: the family's element type decides how a Python value is read.
#[derive(Debug, Clone)]
pub(crate) enum Vector {
    F32(Vec<f32>),
    I8(Vec<i8>),
    Bits(Vec<u8>),
}

/// `ob` as a vector of `element`: for `F32`, a buffer of `float32` (a NumPy `float32`
/// array, `array.array('f')`) or a sequence of numbers; for `I8`, a buffer of `int8` or a
/// sequence of ints; for `Bit`, the packed bits as bytes-like. The family checks the
/// dimension at commit.
pub(crate) fn vector(ob: &Bound<'_, PyAny>, element: Element) -> PyResult<Vector> {
    let py = ob.py();
    match element {
        Element::F32 => {
            if let Ok(b) = PyBuffer::<f32>::get(ob) {
                return Ok(Vector::F32(b.to_vec(py)?));
            }
            Ok(Vector::F32(ob.extract::<Vec<f32>>().map_err(|_| {
                PyTypeError::new_err("an f32 vector is a float32 buffer or a sequence of numbers")
            })?))
        }
        Element::I8 => {
            if let Ok(b) = PyBuffer::<i8>::get(ob) {
                return Ok(Vector::I8(b.to_vec(py)?));
            }
            Ok(Vector::I8(ob.extract::<Vec<i8>>().map_err(|_| {
                PyTypeError::new_err("an i8 vector is an int8 buffer or a sequence of ints")
            })?))
        }
        Element::Bit => Ok(Vector::Bits(crate::convert::bytes(ob).map_err(|_| {
            PyTypeError::new_err("a bit vector is its packed bits, bytes-like")
        })?)),
        _ => Err(PyTypeError::new_err(
            "a vector element this binding doesn't know",
        )),
    }
}

/// A vector cell as Python: `array.array('f')` or `array.array('b')` (a buffer: NumPy wraps
/// it without a copy), or the packed bits as `bytes`.
pub(crate) fn to_py<'py>(
    py: Python<'py>,
    v: pigeonhole::VectorRef<'_>,
) -> PyResult<Bound<'py, PyAny>> {
    let code = match v.element() {
        Element::F32 => "f",
        Element::I8 => "b",
        _ => return Ok(PyBytes::new(py, v.as_bytes()).into_any()),
    };
    let arr = py.import("array")?.getattr("array")?.call1((code,))?;
    // Little-endian payload; every platform the bindings build for is little-endian.
    arr.call_method1("frombytes", (PyBytes::new(py, v.as_bytes()),))?;
    Ok(arr)
}
