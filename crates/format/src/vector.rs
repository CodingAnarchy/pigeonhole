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
}

impl Element {
    /// From its byte.
    pub fn from_u8(b: u8) -> Option<Self> {
        Some(match b {
            1 => Element::F32,
            2 => Element::I8,
            3 => Element::Bit,
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
                        "the element must be 1 (f32), 2 (i8) or 3 (bit)",
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
}

impl Query<'_> {
    /// The element type.
    pub fn element(&self) -> Element {
        match self {
            Query::F32(_) => Element::F32,
            Query::I8(_) => Element::I8,
            Query::Bits(_) => Element::Bit,
        }
    }

    /// The dimension (eight per byte for bits).
    pub fn dim(&self) -> usize {
        match self {
            Query::F32(q) => q.len(),
            Query::I8(q) => q.len(),
            Query::Bits(q) => 8 * q.len(),
        }
    }
}

impl Params {
    /// Checks a caller's vector against the family (D303): its element type and dimension,
    /// finite components, and not all zeros under cosine.
    pub fn check(&self, q: Query<'_>) -> Result<(), VectorError> {
        if q.element() != self.element || q.dim() != self.dim as usize {
            return Err(VectorError(
                "the vector's element type or dimension is not the family's",
            ));
        }
        match q {
            Query::F32(q) if q.iter().any(|x| !x.is_finite()) => Err(VectorError(
                "a vector's components must be finite (no NaN or infinity)",
            )),
            _ if self.metric == Metric::Cosine && is_zero(q) => {
                Err(VectorError("a cosine vector must not be all zeros"))
            }
            _ => Ok(()),
        }
    }

    /// [`Params::check`], then the vector's payload (`f32` LE, `i8` or bits) appended to
    /// `out`: what a put of it stores after the element and dimension.
    pub fn payload(&self, q: Query<'_>, out: &mut Vec<u8>) -> Result<(), VectorError> {
        self.check(q)?;
        match q {
            Query::F32(q) => out.extend(q.iter().flat_map(|x| x.to_le_bytes())),
            Query::I8(q) => out.extend(q.iter().map(|x| *x as u8)),
            Query::Bits(q) => out.extend_from_slice(q),
        }
        Ok(())
    }

    /// The distance from `q` to the stored vector `v` under the family's metric, smaller
    /// nearer, with the `distance` kernels the engine ranks by (D296): `f32` kernels widened
    /// exactly, `i8` and Hamming distances as exact integers. The caller has checked `q`
    /// ([`Params::check`]) and `v` is the family's ([`Params::accepts`]); a mismatched
    /// element pairing is NaN.
    pub fn distance(&self, q: Query<'_>, v: &VectorRef<'_>) -> f64 {
        use crate::distance as k;
        let p = v.payload;
        match (q, self.metric) {
            (Query::F32(q), Metric::L2) => f64::from(k::l2_f32_le(q, p)),
            (Query::F32(q), Metric::InnerProduct) => -f64::from(k::dot_norm_f32_le(q, p).0),
            (Query::F32(q), Metric::Cosine) => {
                let (dot, n2) = k::dot_norm_f32_le(q, p);
                f64::from(k::cosine_from(dot, k::norm2_f32(q), n2))
            }
            (Query::I8(q), Metric::L2) => f64::from(k::l2_i8(q, p)),
            (Query::I8(q), Metric::InnerProduct) => -f64::from(k::dot_i8(q, p)),
            (Query::I8(q), Metric::Cosine) => {
                let dot = f64::from(k::dot_i8(q, p));
                let (nq, nv) = (f64::from(k::dot_i8(q, q)), f64::from(k::dot_i8(p, p)));
                1.0 - dot / (nq.sqrt() * nv.sqrt())
            }
            (Query::Bits(q), Metric::Hamming) => f64::from(k::hamming(q, p)),
            _ => f64::NAN,
        }
    }
}

/// Whether every component is zero (`-0.0` included).
fn is_zero(q: Query<'_>) -> bool {
    match q {
        Query::F32(q) => q.iter().all(|x| *x == 0.0),
        Query::I8(q) => q.iter().all(|x| *x == 0),
        Query::Bits(q) => q.iter().all(|x| *x == 0),
    }
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
        if payload.len() != element.payload_len(dim) {
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

    /// Checks the components a writer must refuse (D303): a non-finite `f32`, and under
    /// cosine an all-zero vector (`-0.0` counts as zero), which has no direction. Bit and
    /// `i8` components are always finite.
    pub fn check_components(&self, metric: Metric) -> Result<(), VectorError> {
        let p = self.payload;
        if self.element == Element::F32
            && p.as_chunks::<4>()
                .0
                .iter()
                .any(|c| !f32::from_le_bytes(*c).is_finite())
        {
            return Err(VectorError(
                "a vector's components must be finite (no NaN or infinity)",
            ));
        }
        if metric == Metric::Cosine {
            let zero = match self.element {
                Element::F32 => p
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .all(|c| f32::from_le_bytes(*c) == 0.0),
                Element::I8 | Element::Bit => p.iter().all(|b| *b == 0),
            };
            if zero {
                return Err(VectorError("a cosine vector must not be all zeros"));
            }
        }
        Ok(())
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
            ok(4, 16, 1),
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
        // 4096 dimensions fit; 4097 do not.
        let big = vec![0u8; 4 * 4096];
        assert!(VectorRef::new(Element::F32, 4096, &big).is_ok());
        let mut huge = vec![1u8, 0x81, 0x20];
        huge.extend_from_slice(&[0; 4 * 4097]);
        assert!(VectorRef::decode(&huge).is_err());
    }
}
