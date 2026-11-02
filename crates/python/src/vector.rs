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
        "f16" => Ok(Element::F16),
        "bf16" => Ok(Element::BF16),
        other => Err(PyValueError::new_err(format!(
            "unknown vector element {other:?} (f32, i8, bit, f16 or bf16)"
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
        Element::F16 => "f16",
        Element::BF16 => "bf16",
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
    /// A spec: `dim` elements of `element` (`Element.F32`, `I8`, `BIT`, `F16` or `BF16`), compared by
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

/// `ob` as a vector of `element`:
/// - `F32`: a `float32` buffer (a NumPy `float32` array, `array.array('f')`; the fast path),
///   a `float64` buffer (NumPy's default dtype; narrowed to `f32`), or a sequence of numbers;
/// - `I8`: an `int8` buffer or a sequence of ints;
/// - `Bit`: the packed bits, bytes-like;
/// - `F16`, `BF16` (#1263): read as `F32`, as above. `put_vector_f32` rounds each component
///   to the family's element (nearest, ties to even; an overflow is refused), and
///   `nearest_f32` takes the query as `f32`. A NumPy `float16` array goes through the
///   sequence path, and its values convert back exactly;
/// - an element this build doesn't name: read as `F32` too.
///
/// A bytes-like object is refused for `F32` and `I8` (cli692's S2 on #1276): `bytes` is a
/// sequence of ints in Python, so it would otherwise be stored as one element per byte.
/// The family checks the dimension at commit.
pub(crate) fn vector(ob: &Bound<'_, PyAny>, element: Element) -> PyResult<Vector> {
    let py = ob.py();
    match element {
        Element::I8 => {
            if let Ok(b) = PyBuffer::<i8>::get(ob) {
                return Ok(Vector::I8(b.to_vec(py)?));
            }
            refuse_bytes(ob, "an i8 vector is an int8 buffer or a sequence of ints")?;
            Ok(Vector::I8(ob.extract::<Vec<i8>>().map_err(|_| {
                PyTypeError::new_err("an i8 vector is an int8 buffer or a sequence of ints")
            })?))
        }
        Element::Bit => Ok(Vector::Bits(crate::convert::bytes(ob).map_err(|_| {
            PyTypeError::new_err("a bit vector is its packed bits, bytes-like")
        })?)),
        // F32, F16 and BF16 (all written and queried as f32), and any element this build
        // doesn't name.
        _ => {
            if let Ok(b) = PyBuffer::<f32>::get(ob) {
                return Ok(Vector::F32(b.to_vec(py)?));
            }
            if let Ok(b) = PyBuffer::<f64>::get(ob) {
                return Ok(Vector::F32(
                    b.to_vec(py)?.into_iter().map(|x| x as f32).collect(),
                ));
            }
            const WANT: &str =
                "an f32 vector is a float32 or float64 buffer, or a sequence of numbers";
            refuse_bytes(ob, WANT)?;
            Ok(Vector::F32(
                ob.extract::<Vec<f32>>()
                    .map_err(|_| PyTypeError::new_err(WANT))?,
            ))
        }
    }
}

/// Refuses `bytes`, `bytearray` and a `memoryview` (whose typed buffer didn't match) for a
/// numeric vector.
fn refuse_bytes(ob: &Bound<'_, PyAny>, want: &str) -> PyResult<()> {
    use pyo3::types::{PyByteArray, PyMemoryView};
    if ob.is_instance_of::<PyBytes>()
        || ob.is_instance_of::<PyByteArray>()
        || ob.is_instance_of::<PyMemoryView>()
    {
        return Err(PyTypeError::new_err(format!(
            "{want}, not {} (bytes are a bit vector's form)",
            ob.get_type().name()?
        )));
    }
    Ok(())
}

/// A vector cell as Python: `array.array('f')` (f32) or `array.array('b')` (i8), buffers
/// NumPy wraps without a copy (`numpy.frombuffer`); the packed bits as `bytes`; and `f16` or
/// `bf16` (#1263), or an element this build doesn't name, widened exactly to
/// `array.array('f')` (`array` has no half-precision typecode).
pub(crate) fn to_py<'py>(
    py: Python<'py>,
    v: pigeonhole::VectorRef<'_>,
) -> PyResult<Bound<'py, PyAny>> {
    let array = py.import("array")?.getattr("array")?;
    let arr = match v.element() {
        Element::Bit => return Ok(PyBytes::new(py, v.as_bytes()).into_any()),
        Element::F32 => array.call1(("f",))?,
        Element::I8 => array.call1(("b",))?,
        _ => {
            return match v.to_f32_vec() {
                Some(x) => array.call1(("f", x)),
                None => Ok(PyBytes::new(py, v.as_bytes()).into_any()),
            };
        }
    };
    // Little-endian payload; every platform the bindings build for is little-endian.
    arr.call_method1("frombytes", (PyBytes::new(py, v.as_bytes()),))?;
    Ok(arr)
}
