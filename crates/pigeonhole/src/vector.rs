//! Vector families (D303, #490 B): their spec, stored vectors and distances.

use pigeonhole_format::vector as fmt;

use crate::{Error, ErrorCode, Result};

/// A vector's element type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Element {
    /// IEEE-754 `f32`.
    F32,
    /// Signed bytes, raw: the application quantises.
    I8,
    /// Bits, packed eight to a byte, least significant bit first.
    Bit,
}

/// A vector family's distance metric. Smaller is nearer for every metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Metric {
    /// Squared Euclidean distance (`f32` and `i8`).
    L2,
    /// The negated inner product, so the largest product ranks first (`f32` and `i8`).
    InnerProduct,
    /// `1 - cosine similarity` (`f32` and `i8`; vectors with a zero norm are refused).
    Cosine,
    /// The number of differing bits (bit vectors only).
    Hamming,
}

impl Element {
    fn to_format(self) -> fmt::Element {
        match self {
            Element::F32 => fmt::Element::F32,
            Element::I8 => fmt::Element::I8,
            Element::Bit => fmt::Element::Bit,
        }
    }

    fn from_format(e: fmt::Element) -> Self {
        match e {
            fmt::Element::F32 => Element::F32,
            fmt::Element::I8 => Element::I8,
            fmt::Element::Bit => Element::Bit,
        }
    }
}

impl Metric {
    fn to_format(self) -> fmt::Metric {
        match self {
            Metric::L2 => fmt::Metric::L2,
            Metric::InnerProduct => fmt::Metric::InnerProduct,
            Metric::Cosine => fmt::Metric::Cosine,
            Metric::Hamming => fmt::Metric::Hamming,
        }
    }

    fn from_format(m: fmt::Metric) -> Self {
        match m {
            fmt::Metric::L2 => Metric::L2,
            fmt::Metric::InnerProduct => Metric::InnerProduct,
            fmt::Metric::Cosine => Metric::Cosine,
            fmt::Metric::Hamming => Metric::Hamming,
        }
    }
}

/// A vector family's shape (D303, #490 B): every value of the family is a vector of one element
/// type, dimension and distance metric, fixed when the family is created
/// ([`Family::vector`](crate::Family::vector)).
///
/// Write vectors with [`put_vector_f32`](crate::RowMutation::put_vector_f32),
/// [`put_vector_i8`](crate::RowMutation::put_vector_i8) or
/// [`put_vector_bits`](crate::RowMutation::put_vector_bits) (and their `_at` forms, on
/// [`WriteBatch`](crate::WriteBatch) and [`Transaction`](crate::Transaction) too). A vector
/// of another element type or dimension, an `f32` component that isn't finite or exceeds
/// 2^56 in magnitude, or (under cosine) a vector with a zero norm is refused with
/// `InvalidArgument` when the write is built. A vector family takes nothing else: a plain put, a merge or an aggregate input fails with
/// `InvalidArgument`. Cells version and delete as in an ordinary family.
///
/// Read them back as [`Value::Vector`](crate::Value::Vector) or with
/// [`CellRef::as_vector`](crate::CellRef::as_vector).
///
/// ```
/// use pigeonhole::{Element, Family, Metric, Options, Pigeonhole, VectorSpec};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default())?;
/// let spec = VectorSpec::new(3, Element::F32, Metric::Cosine)?;
/// let t = db.table("docs")?.family("emb", Family::vector(spec)).create_if_missing()?;
/// t.mutate(b"doc1").put_vector_f32("emb", b"", &[0.1, 0.2, 0.3]).commit()?;
///
/// let cell = t.get(b"doc1", "emb", b"")?.expect("written");
/// let v = cell.as_vector().expect("a vector");
/// assert_eq!(v.to_f32_vec(), Some(vec![0.1, 0.2, 0.3]));
/// assert!(spec.distance_f32(&[0.1, 0.2, 0.3], v)?.abs() < 1e-6);
///
/// // Another dimension, or a plain put, is refused.
/// assert!(t.mutate(b"doc2").put_vector_f32("emb", b"", &[1.0]).commit().is_err());
/// assert!(t.mutate(b"doc2").put("emb", b"", b"bytes").commit().is_err());
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VectorSpec {
    params: fmt::Params,
}

impl VectorSpec {
    /// A spec of `dim` elements (1 to 4096; a multiple of 8 for bit vectors) under `metric`.
    /// L2, inner product and cosine take `f32` or `i8` vectors; Hamming takes bit vectors,
    /// and bit vectors take only Hamming. Anything else is `InvalidArgument`.
    pub fn new(dim: u32, element: Element, metric: Metric) -> Result<Self> {
        fmt::Params::new(element.to_format(), dim, metric.to_format())
            .map(|params| Self { params })
            .map_err(invalid)
    }

    /// The dimension.
    pub fn dim(&self) -> u32 {
        self.params.dim
    }

    /// The element type.
    pub fn element(&self) -> Element {
        Element::from_format(self.params.element)
    }

    /// The metric.
    pub fn metric(&self) -> Metric {
        Metric::from_format(self.params.metric)
    }

    /// The distance from the `f32` vector `query` to `stored` under this spec's metric, as
    /// the engine ranks vectors (exact for `i8` and bit vectors, and the `f32` kernels'
    /// result widened exactly). `query` must have this spec's dimension and finite
    /// components a write takes (finite, at most 2^56 in magnitude, and a nonzero norm under
    /// cosine), and `stored` must be of this spec;
    /// otherwise `InvalidArgument`.
    pub fn distance_f32(&self, query: &[f32], stored: VectorRef<'_>) -> Result<f64> {
        self.distance(fmt::Query::F32(query), stored)
    }

    /// [`distance_f32`](Self::distance_f32) for an `i8` spec.
    pub fn distance_i8(&self, query: &[i8], stored: VectorRef<'_>) -> Result<f64> {
        self.distance(fmt::Query::I8(query), stored)
    }

    /// [`distance_f32`](Self::distance_f32) for a bit spec: `query` is packed bits.
    pub fn distance_bits(&self, query: &[u8], stored: VectorRef<'_>) -> Result<f64> {
        self.distance(fmt::Query::Bits(query), stored)
    }

    fn distance(&self, q: fmt::Query<'_>, stored: VectorRef<'_>) -> Result<f64> {
        self.params.check(q).map_err(invalid)?;
        if !self.params.accepts(&stored.inner) {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "the stored vector is not of this spec",
            ));
        }
        Ok(self.params.distance(q, &stored.inner))
    }

    /// The spec of a vector family's stored parameters, if they're one.
    pub(crate) fn from_kind_params(params: &[u8]) -> Option<Self> {
        fmt::Params::parse(params)
            .ok()
            .map(|params| Self { params })
    }

    pub(crate) fn kind_params(&self) -> Vec<u8> {
        self.params.encode()
    }

    pub(crate) fn params(&self) -> &fmt::Params {
        &self.params
    }
}

/// A stored vector, borrowed from its cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VectorRef<'a> {
    inner: fmt::VectorRef<'a>,
}

impl<'a> VectorRef<'a> {
    pub(crate) fn new(inner: fmt::VectorRef<'a>) -> Self {
        Self { inner }
    }

    /// The element type.
    pub fn element(&self) -> Element {
        Element::from_format(self.inner.element())
    }

    /// The dimension.
    pub fn dim(&self) -> u32 {
        self.inner.dim()
    }

    /// The raw payload: `f32` little-endian, `i8` bytes, or packed bits. A cell's
    /// [`value`](crate::CellRef::value) is the element byte and dimension followed by this.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.inner.as_bytes()
    }

    /// The components as `f32`s: exact for `f32` and `i8` vectors, `None` for bit vectors.
    pub fn to_f32_vec(&self) -> Option<Vec<f32>> {
        let p = self.inner.as_bytes();
        match self.inner.element() {
            fmt::Element::F32 => Some(
                p.as_chunks::<4>()
                    .0
                    .iter()
                    .map(|c| f32::from_le_bytes(*c))
                    .collect(),
            ),
            fmt::Element::I8 => Some(p.iter().map(|b| f32::from(*b as i8)).collect()),
            fmt::Element::Bit => None,
        }
    }
}

fn invalid(e: fmt::VectorError) -> Error {
    Error::new(ErrorCode::InvalidArgument, e.0)
}

/// Checks `q` against a vector family's spec and encodes its payload into `out`.
pub(crate) fn payload(spec: &VectorSpec, q: fmt::Query<'_>, out: &mut Vec<u8>) -> Result<()> {
    spec.params().payload(q, out).map_err(invalid)
}
