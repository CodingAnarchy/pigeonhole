//! Vectors, the values of a `Vector` family (decision D303, #490 B; FORMAT §3.7), and the
//! family's parameters (`kind_params`, FORMAT §9.2.1).
//!
//! A family fixes every value's element type, dimension and distance metric. A value is
//! value tag `0x04`, then:
//!
//! | Offset | Size | Field |
//! |---|---|---|
//! | 0 | 1 | element: 1 `f32`, 2 `i8`, 3 bit |
//! | 1 | varint | `dim` (1 to 4096; a multiple of 8 for bit) |
//! | 1 + varint | payload | `dim` × 4 bytes of `f32` LE, `dim` bytes of `i8`, or `dim / 8` bytes of bits (bit `j` in byte `j / 8`, bit `j % 8`, LSB first) |
//!
//! There is no alignment padding: the distance kernels read unaligned little-endian bytes.
//! The largest value is 16 KiB plus 4 bytes, so vectors stay inline and are never
//! separated into blob files (`FamilyKind::values_stay_inline`).
//!
//! ```
//! use pigeonhole_format::value::{ValueRef, decode_value, encode_value};
//! use pigeonhole_format::vector::{Element, Metric, Params, VectorRef};
//!
//! let p = Params::new(Element::F32, 2, Metric::Cosine).unwrap();
//! assert_eq!(Params::parse(&p.encode()).unwrap(), p);
//!
//! let payload: Vec<u8> = [1.0f32, -2.0].iter().flat_map(|x| x.to_le_bytes()).collect();
//! let v = VectorRef::new(Element::F32, 2, &payload).unwrap();
//! let mut stored = Vec::new();
//! encode_value(&mut stored, ValueRef::Vector(v));
//! assert_eq!(decode_value(&stored).unwrap(), ValueRef::Vector(v));
//! assert!(p.accepts(&v));
//! ```

use std::fmt;

use crate::{Error, kind_params};

/// `kind_params` tag of the element type (u8: 1 `f32`, 2 `i8`, 3 bit).
pub const TAG_ELEMENT: u8 = 1;
/// `kind_params` tag of the dimension (u32 LE).
pub const TAG_DIM: u8 = 2;
/// `kind_params` tag of the metric (u8: 1 L2, 2 inner product, 3 cosine, 4 Hamming).
pub const TAG_METRIC: u8 = 3;
/// The largest dimension.
pub const MAX_DIM: u32 = 4096;

/// A vector's element type. Numbers are frozen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Element {
    /// IEEE-754 `f32`, little-endian.
    F32 = 1,
    /// Signed bytes (raw: the application quantises).
    I8 = 2,
    /// Bits, packed eight to a byte, LSB first.
    Bit = 3,
    /// IEEE binary16 (`f16`) bits, little-endian (#1263).
    F16 = 4,
    /// bfloat16 (`bf16`) bits, little-endian (#1263).
    BF16 = 5,
}

impl Element {
    /// Whether the element is a half-precision float (`f16` or `bf16`, #1263).
    #[inline]
    pub fn is_half(self) -> bool {
        // One compare on the code (4 and 5 are the half elements): `score` tests it per
        // vector.
        self as u8 >= Element::F16 as u8
    }

    /// From its byte.
    pub fn from_u8(b: u8) -> Option<Self> {
        Some(match b {
            1 => Element::F32,
            2 => Element::I8,
            3 => Element::Bit,
            4 => Element::F16,
            5 => Element::BF16,
            _ => return None,
        })
    }

    /// The payload length of a `dim`-element vector.
    pub fn payload_len(self, dim: u32) -> usize {
        let dim = dim as usize;
        match self {
            Element::F32 => 4 * dim,
            Element::I8 => dim,
            Element::Bit => dim / 8,
            Element::F16 | Element::BF16 => 2 * dim,
        }
    }
}

/// A family's distance metric. Smaller is nearer for every metric. Numbers are frozen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Metric {
    /// Squared Euclidean distance (`f32`, `i8`).
    L2 = 1,
    /// The negated inner product (`f32`, `i8`).
    InnerProduct = 2,
    /// `1 - cosine similarity` (`f32`, `i8`; zero vectors are refused).
    Cosine = 3,
    /// The count of differing bits (bit).
    Hamming = 4,
}

impl Metric {
    /// From its byte.
    pub fn from_u8(b: u8) -> Option<Self> {
        Some(match b {
            1 => Metric::L2,
            2 => Metric::InnerProduct,
            3 => Metric::Cosine,
            4 => Metric::Hamming,
            _ => return None,
        })
    }

    /// Whether the metric measures vectors of `element`: L2, inner product and cosine take
    /// `f32` or `i8`, and Hamming takes bit, the only metric bit takes.
    pub fn takes(self, element: Element) -> bool {
        match self {
            Metric::L2 | Metric::InnerProduct | Metric::Cosine => element != Element::Bit,
            Metric::Hamming => element == Element::Bit,
        }
    }
}

/// A malformed parameter list, or parameters out of range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorError(pub &'static str);

impl fmt::Display for VectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for VectorError {}

/// A vector family's parameters: every value's element type, dimension and metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Params {
    /// The element type.
    pub element: Element,
    /// The dimension.
    pub dim: u32,
    /// The metric.
    pub metric: Metric,
}

impl Params {
    /// Checked parameters: `dim` in 1 to 4096 (a multiple of 8 for bit), and a metric that
    /// takes the element.
    pub fn new(element: Element, dim: u32, metric: Metric) -> Result<Self, VectorError> {
        if !(1..=MAX_DIM).contains(&dim) {
            return Err(VectorError("a vector's dimension must be 1 to 4096"));
        }
        if element == Element::Bit && !dim.is_multiple_of(8) {
            return Err(VectorError(
                "a bit vector's dimension must be a multiple of 8",
            ));
        }
        if !metric.takes(element) {
            return Err(VectorError(
                "L2, inner product and cosine take f32 or i8 vectors; Hamming takes bit vectors",
            ));
        }
        Ok(Self {
            element,
            dim,
            metric,
        })
    }

    /// From `kind_params`: all three tags required, checked as [`Params::new`] does. A
    /// skippable tag (128 and up) is ignored; any other is refused.
    pub fn parse(params: &[u8]) -> Result<Self, VectorError> {
        let (mut element, mut dim, mut metric) = (None, None, None);
        for (tag, v) in
            kind_params::parse(params).map_err(|_| VectorError("malformed parameters"))?
        {
            match (tag, v) {
                (TAG_ELEMENT, [b]) => {
                    element = Some(Element::from_u8(*b).ok_or(VectorError(
                        "the element must be 1 (f32), 2 (i8), 3 (bit), 4 (f16) or 5 (bf16)",
                    ))?);
                }
                (TAG_ELEMENT, _) => return Err(VectorError("the element is one byte")),
                (TAG_DIM, v) => {
                    let v: [u8; 4] = v
                        .try_into()
                        .map_err(|_| VectorError("the dimension is a u32"))?;
                    dim = Some(u32::from_le_bytes(v));
                }
                (TAG_METRIC, [b]) => {
                    metric = Some(Metric::from_u8(*b).ok_or(VectorError(
                        "the metric must be 1 (L2), 2 (inner product), 3 (cosine) or 4 (Hamming)",
                    ))?);
                }
                (TAG_METRIC, _) => return Err(VectorError("the metric is one byte")),
                (t, _) if kind_params::must_understand(t) => {
                    return Err(VectorError("an unknown vector parameter"));
                }
                _ => {}
            }
        }
        let (Some(element), Some(dim), Some(metric)) = (element, dim, metric) else {
            return Err(VectorError(
                "a vector family needs its element (tag 1), dimension (tag 2) and metric (tag 3)",
            ));
        };
        Self::new(element, dim, metric)
    }

    /// The canonical `kind_params` holding all three.
    pub fn encode(&self) -> Vec<u8> {
        kind_params::encode(&[
            (TAG_ELEMENT, &[self.element as u8]),
            (TAG_DIM, &self.dim.to_le_bytes()),
            (TAG_METRIC, &[self.metric as u8]),
        ])
    }

    /// Whether `v` has the family's element type and dimension.
    pub fn accepts(&self, v: &VectorRef<'_>) -> bool {
        v.element == self.element && v.dim == self.dim
    }
}

/// A vector given by a caller (a write's value, or a query), in its element's own type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Query<'a> {
    /// `f32` components.
    F32(&'a [f32]),
    /// `i8` components.
    I8(&'a [i8]),
    /// Bits, packed eight to a byte, LSB first.
    Bits(&'a [u8]),
    /// `f16` bit patterns (a write's value).
    F16(&'a [u16]),
    /// `bf16` bit patterns (a write's value).
    BF16(&'a [u16]),
}

impl Query<'_> {
    /// The element type.
    pub fn element(&self) -> Element {
        match self {
            Query::F32(_) => Element::F32,
            Query::I8(_) => Element::I8,
            Query::Bits(_) => Element::Bit,
            Query::F16(_) => Element::F16,
            Query::BF16(_) => Element::BF16,
        }
    }

    /// The dimension (eight per byte for bits).
    pub fn dim(&self) -> usize {
        match self {
            Query::F32(q) => q.len(),
            Query::I8(q) => q.len(),
            Query::Bits(q) => 8 * q.len(),
            Query::F16(q) | Query::BF16(q) => q.len(),
        }
    }
}

impl Params {
    /// Checks a caller's vector against the family (D303): its element type and dimension,
    /// and its components as [`VectorRef::check_components`] does.
    ///
    /// An `f32` vector also fits a half-precision family (#1263): as a query it stays `f32`,
    /// and as a write [`Params::payload`] converts it and checks the result.
    pub fn check(&self, q: Query<'_>) -> Result<(), VectorError> {
        let converts = matches!(q, Query::F32(_)) && self.element.is_half();
        if (q.element() != self.element && !converts) || q.dim() != self.dim as usize {
            return Err(VectorError(
                "the vector's element type or dimension is not the family's",
            ));
        }
        match q {
            Query::F32(q) => check_f32(q, self.metric),
            Query::I8(q) => check_bytes(q.iter().all(|x| *x == 0), self.metric),
            Query::Bits(q) => check_bytes(q.iter().all(|x| *x == 0), self.metric),
            Query::F16(q) | Query::BF16(q) => {
                let bytes: Vec<u8> = q.iter().flat_map(|h| h.to_le_bytes()).collect();
                check_half(self.element, &bytes, self.metric, COSINE_NORM)
            }
        }
    }

    /// [`Params::check`], then the vector's payload (`f32` LE, `i8` or bits) appended to
    /// `out`: what a put of it stores after the element and dimension.
    ///
    /// An `f32` vector written to a half-precision family is converted (to nearest, ties to
    /// even; a component that overflows the element is refused), and the converted vector
    /// is checked as stored: an `f32` vector can round to a zero vector (#1263).
    pub fn payload(&self, q: Query<'_>, out: &mut Vec<u8>) -> Result<(), VectorError> {
        if let (Query::F32(xs), true) = (q, self.element.is_half()) {
            if xs.len() != self.dim as usize {
                return Err(VectorError(
                    "the vector's element type or dimension is not the family's",
                ));
            }
            let start = out.len();
            for x in xs {
                let h = if self.element == Element::F16 {
                    crate::half::f32_to_f16(*x)
                } else {
                    crate::half::f32_to_bf16(*x)
                }
                .ok_or(VectorError(
                    "a component isn't finite or overflows the family's half-precision element",
                ))?;
                out.extend_from_slice(&h.to_le_bytes());
            }
            let checked = check_half(
                self.element,
                &out[start..],
                self.metric,
                "the vector rounds to a zero vector in the family's half-precision element",
            );
            if checked.is_err() {
                out.truncate(start);
            }
            return checked;
        }
        self.check(q)?;
        match q {
            Query::F32(q) => out.extend(q.iter().flat_map(|x| x.to_le_bytes())),
            Query::I8(q) => out.extend(q.iter().map(|x| *x as u8)),
            Query::Bits(q) => out.extend_from_slice(q),
            Query::F16(q) | Query::BF16(q) => out.extend(q.iter().flat_map(|h| h.to_le_bytes())),
        }
        Ok(())
    }

    /// The distance from `q` to the stored vector `v` under the family's metric, smaller
    /// nearer, with the `distance` kernels the engine ranks by (D296): `f32` kernels widened
    /// exactly, `i8` and Hamming distances as exact integers. The caller has checked `q`
    /// ([`Params::check`]) and `v` is the family's ([`Params::accepts`]); a mismatched
    /// element pairing is NaN. To score many vectors against one query, [`Params::prepare`]
    /// it once: the same result, with the query's norm computed once.
    pub fn distance(&self, q: Query<'_>, v: &VectorRef<'_>) -> f64 {
        score(self.metric, q, query_norm(self.metric, q), v)
    }

    /// `q` checked against the family ([`Params::check`]) and made ready to score many
    /// stored vectors: its norm, which cosine needs, is computed here once
    /// ([`Prepared::distance`] equals [`Params::distance`] bit for bit).
    pub fn prepare(&self, q: QueryBuf) -> Result<Prepared, VectorError> {
        self.check(q.as_query())?;
        let norm = query_norm(self.metric, q.as_query());
        Ok(Prepared {
            params: *self,
            query: q,
            norm,
        })
    }
}

/// An owned query, in the vector family's element type: what a search holds while it
/// scores ([`Params::prepare`]).
#[derive(Debug, Clone, PartialEq)]
pub enum QueryBuf {
    /// `f32` components.
    F32(std::sync::Arc<[f32]>),
    /// `i8` components.
    I8(std::sync::Arc<[i8]>),
    /// Bits, packed eight to a byte, LSB first.
    Bits(std::sync::Arc<[u8]>),
}

impl QueryBuf {
    /// The query, borrowed.
    pub fn as_query(&self) -> Query<'_> {
        match self {
            QueryBuf::F32(q) => Query::F32(q),
            QueryBuf::I8(q) => Query::I8(q),
            QueryBuf::Bits(q) => Query::Bits(q),
        }
    }
}

/// A checked query with its norm, ready to score stored vectors of one family
/// ([`Params::prepare`]). Cheap to clone: the query is shared.
#[derive(Debug, Clone)]
pub struct Prepared {
    params: Params,
    query: QueryBuf,
    norm: QueryNorm,
}

impl Prepared {
    /// The family's parameters.
    pub fn params(&self) -> &Params {
        &self.params
    }

    /// The distance to the stored vector `v`, which must be the family's
    /// ([`Params::accepts`]); [`Params::distance`]'s result, bit for bit.
    pub fn distance(&self, v: &VectorRef<'_>) -> f64 {
        score(self.params.metric, self.query.as_query(), self.norm, v)
    }
}

/// The query norm cosine needs, in the type its formula uses.
#[derive(Debug, Clone, Copy)]
enum QueryNorm {
    None,
    F32(f32),
    I8(f64),
}

fn query_norm(metric: Metric, q: Query<'_>) -> QueryNorm {
    use crate::distance as k;
    match (q, metric) {
        (Query::F32(q), Metric::Cosine) => QueryNorm::F32(k::norm2_f32(q)),
        (Query::I8(q), Metric::Cosine) => QueryNorm::I8(f64::from(k::dot_i8(q, q))),
        _ => QueryNorm::None,
    }
}

/// The one scoring function: [`Params::distance`] and [`Prepared::distance`] both call it.
fn score(metric: Metric, q: Query<'_>, norm: QueryNorm, v: &VectorRef<'_>) -> f64 {
    use crate::distance as k;
    // Half-precision stored vectors (#1263) leave the hot path with one test, so the `f32`,
    // `i8` and bit arms below compile as they did before (perf287 on #1285: the f32 row
    // must hold its ceiling).
    if v.element.is_half() {
        return score_half(metric, q, norm, v);
    }
    let p = v.payload;
    match (q, metric, norm) {
        (Query::F32(q), Metric::L2, _) => f64::from(k::l2_f32_le(q, p)),
        (Query::F32(q), Metric::InnerProduct, _) => -f64::from(k::dot_norm_f32_le(q, p).0),
        (Query::F32(q), Metric::Cosine, QueryNorm::F32(nq)) => {
            let (dot, n2) = k::dot_norm_f32_le(q, p);
            f64::from(k::cosine_from(dot, nq, n2))
        }
        (Query::I8(q), Metric::L2, _) => f64::from(k::l2_i8(q, p)),
        (Query::I8(q), Metric::InnerProduct, _) => -f64::from(k::dot_i8(q, p)),
        (Query::I8(q), Metric::Cosine, QueryNorm::I8(nq)) => {
            let dot = f64::from(k::dot_i8(q, p));
            let nv = f64::from(k::dot_i8(p, p));
            1.0 - dot / (nq.sqrt() * nv.sqrt())
        }
        (Query::Bits(q), Metric::Hamming, _) => f64::from(k::hamming(q, p)),
        _ => f64::NAN,
    }
}

/// [`score`] for an `f16` or `bf16` stored vector: an `f32` query through the widening
/// kernels, which equal the `f32` kernels over the widened vector bit for bit. Out of line,
/// so it costs the other elements' path nothing.
#[cold]
#[inline(never)]
fn score_half(metric: Metric, q: Query<'_>, norm: QueryNorm, v: &VectorRef<'_>) -> f64 {
    use crate::distance as k;
    let (Query::F32(q), p) = (q, v.payload) else {
        return f64::NAN;
    };
    let f16 = v.element == Element::F16;
    match (metric, norm) {
        (Metric::L2, _) => f64::from(if f16 {
            k::l2_f16_le(q, p)
        } else {
            k::l2_bf16_le(q, p)
        }),
        (Metric::InnerProduct, _) => {
            let (dot, _) = if f16 {
                k::dot_norm_f16_le(q, p)
            } else {
                k::dot_norm_bf16_le(q, p)
            };
            -f64::from(dot)
        }
        (Metric::Cosine, QueryNorm::F32(nq)) => {
            let (dot, n2) = if f16 {
                k::dot_norm_f16_le(q, p)
            } else {
                k::dot_norm_bf16_le(q, p)
            };
            f64::from(k::cosine_from(dot, nq, n2))
        }
        _ => f64::NAN,
    }
}

/// The largest `f32` component magnitude a vector may hold: 2^56. With at most 4096
/// components, every kernel's sum stays below `f32::MAX` (Σ(2x)² ≤ 4096 × 4 × 2^112 = 2^126),
/// so no distance overflows to infinity or NaN (perf287's B1 on #1255).
pub const MAX_F32_COMPONENT: f32 = 72_057_594_037_927_936.0;

const F32_RANGE: &str = "a vector's components must be finite and at most 2^56 in magnitude";
const COSINE_NORM: &str =
    "a cosine vector needs a nonzero norm (not all zeros, nor so small its square underflows)";

/// The `f32` components a vector may hold under `metric` (D303): finite and at most
/// [`MAX_F32_COMPONENT`] in magnitude, and under cosine a squared norm (as the kernels sum
/// it) of at least `f32::MIN_POSITIVE`, so the norms' product can't round to zero. Every
/// distance between two such vectors is finite.
fn check_f32(q: &[f32], metric: Metric) -> Result<(), VectorError> {
    if q.iter()
        .any(|x| !x.is_finite() || x.abs() > MAX_F32_COMPONENT)
    {
        return Err(VectorError(F32_RANGE));
    }
    if metric == Metric::Cosine && crate::distance::norm2_f32(q) < f32::MIN_POSITIVE {
        return Err(VectorError(COSINE_NORM));
    }
    Ok(())
}

/// The components a half-precision vector may hold (#1263): finite (an `f16`'s largest,
/// 65,504, bounds it; a `bf16` keeps `f32`'s 2^56 bound, having its range), and under cosine
/// a squared norm, as the widening kernels sum it, of at least `f32::MIN_POSITIVE`.
/// `zero_norm` is the refusal's message (a converted `f32` vector "rounds to" zero).
fn check_half(
    element: Element,
    payload: &[u8],
    metric: Metric,
    zero_norm: &'static str,
) -> Result<(), VectorError> {
    let widen = if element == Element::F16 {
        crate::half::f16_to_f32
    } else {
        crate::half::bf16_to_f32
    };
    let bad = payload
        .as_chunks::<2>()
        .0
        .iter()
        .map(|c| widen(u16::from_le_bytes(*c)))
        .any(|x| !x.is_finite() || x.abs() > MAX_F32_COMPONENT);
    if bad {
        return Err(VectorError(F32_RANGE));
    }
    let norm = if element == Element::F16 {
        crate::distance::norm2_f16_le(payload)
    } else {
        crate::distance::norm2_bf16_le(payload)
    };
    if metric == Metric::Cosine && norm < f32::MIN_POSITIVE {
        return Err(VectorError(zero_norm));
    }
    Ok(())
}

/// [`check_f32`]'s cosine rule for `i8` and bit vectors, whose norm is zero only when every
/// component is.
fn check_bytes(all_zero: bool, metric: Metric) -> Result<(), VectorError> {
    if metric == Metric::Cosine && all_zero {
        return Err(VectorError("a cosine vector must not be all zeros"));
    }
    Ok(())
}

/// A decoded vector value, borrowing its payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorRef<'a> {
    element: Element,
    dim: u32,
    payload: &'a [u8],
}

impl<'a> VectorRef<'a> {
    /// A vector of `dim` `element`s in `payload`: `dim` in 1 to 4096 (a multiple of 8 for
    /// bit) and `payload` exactly [`Element::payload_len`] bytes. Components are not checked
    /// (a writer refuses non-finite ones, D303).
    pub fn new(element: Element, dim: u32, payload: &'a [u8]) -> Result<Self, VectorError> {
        if !(1..=MAX_DIM).contains(&dim) || (element == Element::Bit && !dim.is_multiple_of(8)) {
            return Err(VectorError(
                "a vector's dimension must be 1 to 4096, a multiple of 8 for bit",
            ));
        }
        // `f32` first: the common element skips the match, once per scored vector.
        let want = if element == Element::F32 {
            4 * dim as usize
        } else {
            element.payload_len(dim)
        };
        if payload.len() != want {
            return Err(VectorError(
                "a vector's payload must match its element and dimension",
            ));
        }
        Ok(Self {
            element,
            dim,
            payload,
        })
    }

    /// The element type.
    pub fn element(&self) -> Element {
        self.element
    }

    /// The dimension.
    pub fn dim(&self) -> u32 {
        self.dim
    }

    /// The raw payload: `f32` LE, `i8` or packed bits.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.payload
    }

    /// Checks the components a writer must refuse (D303): an `f32` that isn't finite or is
    /// above 2^56 in magnitude, and under cosine a zero norm (all zeros, `-0.0` included, or an
    /// `f32` norm whose square underflows). With these refused, every distance is finite.
    pub fn check_components(&self, metric: Metric) -> Result<(), VectorError> {
        let p = self.payload;
        match self.element {
            Element::F32 => {
                let mut xs = p.as_chunks::<4>().0.iter().map(|c| f32::from_le_bytes(*c));
                if xs.any(|x| !x.is_finite() || x.abs() > MAX_F32_COMPONENT) {
                    return Err(VectorError(F32_RANGE));
                }
                // The norm the kernels sum, read from the stored bytes (no copy per put).
                if metric == Metric::Cosine && crate::distance::norm2_f32_le(p) < f32::MIN_POSITIVE
                {
                    return Err(VectorError(COSINE_NORM));
                }
                Ok(())
            }
            Element::I8 | Element::Bit => check_bytes(p.iter().all(|b| *b == 0), metric),
            Element::F16 | Element::BF16 => check_half(self.element, p, metric, COSINE_NORM),
        }
    }

    /// Appends the encoding after the value tag: element, `dim` varint, payload.
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        out.push(self.element as u8);
        crate::varint::put_u64(out, u64::from(self.dim));
        out.extend_from_slice(self.payload);
    }

    /// Decodes the bytes after the value tag; anything malformed is `Corrupt`. An unknown
    /// element is too, as in `kind_params`: a new element would come under a new `incompat`
    /// bit, so a reader that meets one has a damaged file.
    // Inlined into `decode_value`, once per scored vector: #1263's larger element matches
    // pushed it out of line, at about 7 instructions a vector (#1285's callgrind).
    #[inline]
    pub(crate) fn decode(bytes: &'a [u8]) -> crate::Result<Self> {
        let Some((&e, rest)) = bytes.split_first() else {
            return Err(Error::Truncated { what: "vector" });
        };
        let element = Element::from_u8(e).ok_or(Error::Corrupt {
            what: "vector element",
        })?;
        let (dim, used) = crate::varint::get_u32(rest)?;
        Self::new(element, dim, &rest[used..]).map_err(|_| Error::Corrupt { what: "vector" })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn le_bytes(xs: &[f32]) -> Vec<u8> {
        xs.iter().flat_map(|x| x.to_le_bytes()).collect()
    }

    #[test]
    fn params_round_trip_and_refuse_bad_ranges_and_pairings() {
        for (e, d, m) in [
            (Element::F32, 1, Metric::L2),
            (Element::F32, 4096, Metric::Cosine),
            (Element::I8, 768, Metric::InnerProduct),
            (Element::Bit, 8, Metric::Hamming),
            (Element::Bit, 4096, Metric::Hamming),
        ] {
            let p = Params::new(e, d, m).unwrap();
            assert_eq!(Params::parse(&p.encode()).unwrap(), p);
        }
        for (e, d, m) in [
            (Element::F32, 0, Metric::L2),
            (Element::F32, 4097, Metric::L2),
            (Element::Bit, 12, Metric::Hamming),
            (Element::Bit, 64, Metric::L2),
            (Element::Bit, 64, Metric::Cosine),
            (Element::F32, 64, Metric::Hamming),
            (Element::I8, 64, Metric::Hamming),
        ] {
            assert!(Params::new(e, d, m).is_err(), "{e:?} {d} {m:?}");
        }
    }

    #[test]
    fn parse_needs_all_three_tags_and_known_values() {
        let ok = |e: u8, d: u32, m: u8| {
            kind_params::encode(&[(1, &[e]), (2, &d.to_le_bytes()), (3, &[m])])
        };
        assert!(Params::parse(&ok(1, 16, 1)).is_ok());
        for bad in [
            ok(0, 16, 1),
            ok(6, 16, 1),
            ok(1, 16, 0),
            ok(1, 16, 5),
            ok(1, 0, 1),
            kind_params::encode(&[(1, &[1]), (2, &16u32.to_le_bytes())]),
            kind_params::encode(&[(1, &[1, 0]), (2, &16u32.to_le_bytes()), (3, &[1])]),
            kind_params::encode(&[(1, &[1]), (2, &16u16.to_le_bytes()), (3, &[1])]),
            kind_params::encode(&[(1, &[1]), (2, &16u32.to_le_bytes()), (3, &[1]), (4, &[])]),
            Vec::new(),
            vec![1, 1],
        ] {
            assert!(Params::parse(&bad).is_err(), "{bad:?}");
        }
        let hint =
            kind_params::encode(&[(1, &[1]), (2, &16u32.to_le_bytes()), (3, &[1]), (200, &[7])]);
        assert!(Params::parse(&hint).is_ok(), "a skippable tag is ignored");
    }

    #[test]
    fn values_decode_strictly() {
        let mut out = Vec::new();
        VectorRef::new(Element::Bit, 16, &[0xff, 0x01])
            .unwrap()
            .encode(&mut out);
        assert_eq!(out, [3, 16, 0xff, 0x01]);
        let v = VectorRef::decode(&out).unwrap();
        assert_eq!(
            (v.element(), v.dim(), v.as_bytes()),
            (Element::Bit, 16, &[0xff, 0x01][..])
        );
        // short, long, zero dim, bit dim not a multiple of 8, overlong varint, empty.
        for bad in [
            &[3u8, 16, 0xff][..],
            &[3, 16, 0xff, 1, 2],
            &[2, 0],
            &[3, 12, 0xff, 0x0f],
            &[2, 0x81, 0x00, 7],
            &[],
            &[1],
        ] {
            assert!(VectorRef::decode(bad).is_err(), "{bad:?}");
        }
        assert!(matches!(
            VectorRef::decode(&[9, 1, 0]),
            Err(Error::Corrupt {
                what: "vector element"
            })
        ));
        // Components: non-finite f32 refused; cosine refuses all zeros (-0.0 too).
        let f = |xs: &[f32]| -> Vec<u8> { xs.iter().flat_map(|x| x.to_le_bytes()).collect() };
        let ok = f(&[0.0, 1.0]);
        let zero = f(&[0.0, -0.0]);
        let nan = f(&[1.0, f32::NAN]);
        let inf = f(&[f32::INFINITY, 1.0]);
        let v = |b| VectorRef::new(Element::F32, 2, b).unwrap();
        assert!(v(&ok).check_components(Metric::Cosine).is_ok());
        assert!(v(&zero).check_components(Metric::Cosine).is_err());
        assert!(v(&zero).check_components(Metric::L2).is_ok());
        assert!(v(&nan).check_components(Metric::L2).is_err());
        assert!(v(&inf).check_components(Metric::InnerProduct).is_err());
        let i8z = VectorRef::new(Element::I8, 2, &[0, 0]).unwrap();
        assert!(i8z.check_components(Metric::Cosine).is_err());
        assert!(i8z.check_components(Metric::L2).is_ok());
        // A caller's vector: checked, encoded, scored with the shared kernels.
        let p = Params::new(Element::F32, 2, Metric::Cosine).unwrap();
        assert!(p.check(Query::F32(&[1.0])).is_err(), "wrong dimension");
        assert!(p.check(Query::I8(&[1, 2])).is_err(), "wrong element");
        assert!(p.check(Query::F32(&[0.0, -0.0])).is_err(), "cosine zero");
        assert!(p.check(Query::F32(&[f32::NAN, 1.0])).is_err(), "NaN");
        let mut payload = Vec::new();
        p.payload(Query::F32(&[3.0, 4.0]), &mut payload).unwrap();
        let v = VectorRef::new(Element::F32, 2, &payload).unwrap();
        assert!(p.distance(Query::F32(&[3.0, 4.0]), &v).abs() < 1e-7);
        let q8 = Params::new(Element::I8, 2, Metric::L2).unwrap();
        let mut b8 = Vec::new();
        q8.payload(Query::I8(&[-128, 127]), &mut b8).unwrap();
        let v8 = VectorRef::new(Element::I8, 2, &b8).unwrap();
        assert_eq!(
            q8.distance(Query::I8(&[127, -128]), &v8),
            2.0 * 255.0 * 255.0
        );
        let qb = Params::new(Element::Bit, 8, Metric::Hamming).unwrap();
        let vb = VectorRef::new(Element::Bit, 8, &[0b1010_1010]).unwrap();
        assert_eq!(qb.distance(Query::Bits(&[0b0101_0101]), &vb), 8.0);
        // perf287's B1: finite components that would overflow a kernel, or a cosine norm that
        // underflows, are refused; at the bound every distance is finite.
        let ip = Params::new(Element::F32, 2, Metric::InnerProduct).unwrap();
        assert!(ip.check(Query::F32(&[1e30, 1e30])).is_err(), "above 2^56");
        let cos1 = Params::new(Element::F32, 1, Metric::Cosine).unwrap();
        assert!(cos1.check(Query::F32(&[1e30])).is_err(), "above 2^56");
        let cos4 = Params::new(Element::F32, 4, Metric::Cosine).unwrap();
        assert!(
            cos4.check(Query::F32(&[1e-45, 0.0, 0.0, 0.0])).is_err(),
            "a nonzero vector whose squared norm underflows"
        );
        let tiny = le_bytes(&[1e-45, 0.0, 0.0, 0.0]);
        assert!(
            VectorRef::new(Element::F32, 4, &tiny)
                .unwrap()
                .check_components(Metric::Cosine)
                .is_err(),
            "the same rule for a stored vector"
        );
        let big = MAX_F32_COMPONENT;
        let wide = Params::new(Element::F32, 4096, Metric::InnerProduct).unwrap();
        let (q, v) = (vec![big; 4096], vec![-big; 4096]);
        let vb = le_bytes(&v);
        let vr = VectorRef::new(Element::F32, 4096, &vb).unwrap();
        wide.check(Query::F32(&q)).unwrap();
        vr.check_components(Metric::InnerProduct).unwrap();
        for m in [Metric::L2, Metric::InnerProduct, Metric::Cosine] {
            let p = Params::new(Element::F32, 4096, m).unwrap();
            assert!(
                p.distance(Query::F32(&q), &vr).is_finite(),
                "{m:?} at the bound"
            );
        }
        let small = Params::new(Element::F32, 1, Metric::Cosine).unwrap();
        let s1 = le_bytes(&[1.1e-19]);
        let sv = VectorRef::new(Element::F32, 1, &s1).unwrap();
        small.check(Query::F32(&[1.1e-19])).unwrap();
        sv.check_components(Metric::Cosine).unwrap();
        assert!(
            small.distance(Query::F32(&[1.1e-19]), &sv).is_finite(),
            "at the norm floor"
        );
        // A prepared query scores as Params::distance, bit for bit, for every element.
        for (p, q, v) in [
            (
                Params::new(Element::F32, 2, Metric::Cosine).unwrap(),
                QueryBuf::F32(vec![0.3, -0.4].into()),
                le_bytes(&[1.5, 2.5]),
            ),
            (
                Params::new(Element::I8, 2, Metric::Cosine).unwrap(),
                QueryBuf::I8(vec![3, -7].into()),
                vec![5u8, 250],
            ),
            (
                Params::new(Element::Bit, 8, Metric::Hamming).unwrap(),
                QueryBuf::Bits(vec![0x0f].into()),
                vec![0x3c],
            ),
        ] {
            let v = VectorRef::new(p.element, p.dim, &v).unwrap();
            let prepared = p.prepare(q.clone()).unwrap();
            assert_eq!(
                prepared.distance(&v).to_bits(),
                p.distance(q.as_query(), &v).to_bits()
            );
        }
        assert!(
            Params::new(Element::F32, 2, Metric::Cosine)
                .unwrap()
                .prepare(QueryBuf::F32(vec![0.0, 0.0].into()))
                .is_err(),
            "prepare checks"
        );
        // #1263: half-precision elements.
        for e in [Element::F16, Element::BF16] {
            for m in [Metric::L2, Metric::InnerProduct, Metric::Cosine] {
                let p = Params::new(e, 3, m).unwrap();
                assert_eq!(Params::parse(&p.encode()).unwrap(), p);
            }
            assert!(
                Params::new(e, 8, Metric::Hamming).is_err(),
                "Hamming is bit-only"
            );
            assert_eq!(e.payload_len(4096), 8192, "half the bytes of f32");
        }
        // Raw bits are stored as given; an f32 write converts to nearest-even.
        let f16c = Params::new(Element::F16, 2, Metric::Cosine).unwrap();
        let mut out = Vec::new();
        f16c.payload(Query::F16(&[0x3c00, 0xc000]), &mut out)
            .unwrap();
        assert_eq!(out, [0x00, 0x3c, 0x00, 0xc0]);
        out.clear();
        f16c.payload(Query::F32(&[1.0, -2.0]), &mut out).unwrap();
        assert_eq!(
            out,
            [0x00, 0x3c, 0x00, 0xc0],
            "f32 converts to the same halves"
        );
        out.clear();
        assert!(
            f16c.payload(Query::F32(&[70_000.0, 1.0]), &mut out)
                .is_err(),
            "overflows f16"
        );
        let zero = f16c
            .payload(Query::F32(&[1e-9, -1e-9]), &mut out)
            .unwrap_err();
        assert!(zero.0.contains("rounds to a zero vector"), "{}", zero.0);
        assert!(
            out.is_empty(),
            "nothing is left in the payload of a refused write"
        );
        assert!(
            f16c.check(Query::F16(&[0x7c00, 0x3c00])).is_err(),
            "an f16 infinity"
        );
        assert!(
            f16c.check(Query::F16(&[0x0000, 0x8000])).is_err(),
            "a cosine zero vector"
        );
        assert!(f16c.check(Query::I8(&[1, 2])).is_err(), "i8 into f16");
        let bf = Params::new(Element::BF16, 1, Metric::L2).unwrap();
        assert!(bf.check(Query::BF16(&[0x7f00])).is_err(), "bf16 above 2^56");
        assert!(bf.check(Query::BF16(&[0x5b00])).is_ok(), "bf16 at 2^55");
        // A stored half vector scores as the same vector widened into an f32 family.
        let halves = [0x3555u16, 0xbc00, 0x0001, 0x7bff];
        let widened: Vec<f32> = halves.iter().map(|h| crate::half::f16_to_f32(*h)).collect();
        let hb: Vec<u8> = halves.iter().flat_map(|h| h.to_le_bytes()).collect();
        let fb = le_bytes(&widened);
        let q = [0.25f32, -1.5, 3.0, 0.125];
        for m in [Metric::L2, Metric::InnerProduct, Metric::Cosine] {
            let ph = Params::new(Element::F16, 4, m).unwrap();
            let pf = Params::new(Element::F32, 4, m).unwrap();
            let vh = VectorRef::new(Element::F16, 4, &hb).unwrap();
            let vf = VectorRef::new(Element::F32, 4, &fb).unwrap();
            assert_eq!(
                ph.prepare(QueryBuf::F32(q.to_vec().into()))
                    .unwrap()
                    .distance(&vh)
                    .to_bits(),
                pf.distance(Query::F32(&q), &vf).to_bits(),
                "{m:?}"
            );
        }
        // 4096 dimensions fit; 4097 do not.
        let big = vec![0u8; 4 * 4096];
        assert!(VectorRef::new(Element::F32, 4096, &big).is_ok());
        let mut huge = vec![1u8, 0x81, 0x20];
        huge.extend_from_slice(&[0; 4 * 4097]);
        assert!(VectorRef::decode(&huge).is_err());
    }
}
