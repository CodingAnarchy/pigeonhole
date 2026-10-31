//! HyperLogLog sketches, the values of an HLL aggregate family (decision D294; FORMAT §3.1).
//!
//! A sketch has 4096 registers (precision 12, about 1.6% standard error). An `i64` element
//! is hashed with [`crate::checksum::xxh3_64`] (xxh3-64, seed 0: the format fixes it, so a
//! change to that function breaks every sketch) over its 8 little-endian bytes: the top 12
//! bits pick the register, and the rank is one plus the leading zeros of the remaining 52
//! bits, at most 53. A register keeps the largest rank it has seen.
//!
//! **Encoding**, a 4-byte header then the registers:
//!
//! | Offset | Size | Field |
//! |---|---|---|
//! | 0 | 1 | encoding: 1 |
//! | 1 | 1 | precision: 12 |
//! | 2 | 1 | representation: 0 sparse, 1 dense |
//! | 3 | 1 | reserved: 0 |
//! | 4 | 4·n | sparse: `n` (1..=767) `u32` LE entries `index << 8 \| rank`, sorted by index, unique, rank 1..=53 |
//! | 4 | 3072 | dense: 4096 six-bit registers, packed little-endian (register `i` in bits `6i .. 6i+6`) |
//!
//! It is **canonical**: a sketch is sparse if and only if at most 767 of its registers are
//! nonzero, so the same registers always give the same bytes. A sketch only grows (a merge
//! keeps the larger rank), so it turns dense once and never back.
//!
//! **Merging** is in place: into a dense sketch it writes the larger ranks straight into the
//! packed bytes; into a sparse one it merges the entry lists from the end, and converts to
//! dense (through a stack copy of its entries) only when the union passes 767. A growing
//! sparse sketch reserves the dense length once, so a fold allocates at most once.
//!
//! **The estimate** is Ertl's improved estimator ("New cardinality estimation algorithms for
//! HyperLogLog sketches", 2017, algorithm 6), from the register histogram, with no bias
//! tables. It uses only `+`, `×`, `÷`, halving and `√`, which IEEE-754 rounds the same on
//! every platform, and one constant, [`ALPHA_INF`], given as its bit pattern: the estimate of
//! a sketch is the same everywhere.
//!
//! ```
//! use pigeonhole_format::hll;
//!
//! let mut acc = Vec::new();
//! hll::encode_element(&mut acc, 1);
//! let mut other = Vec::new();
//! for v in 2..=1000 {
//!     hll::encode_element(&mut other, v);
//!     hll::merge(&mut acc, &other).unwrap();
//! }
//! let n = hll::estimate(&acc).unwrap();
//! assert!((950..=1050).contains(&n), "{n}");
//! hll::merge(&mut acc, &other.clone()).unwrap(); // idempotent
//! assert_eq!(hll::estimate(&acc).unwrap(), n);
//! ```

use std::fmt;

/// Register-index bits.
pub const PRECISION: u8 = 12;
/// Registers per sketch.
pub const REGISTERS: usize = 1 << PRECISION;
/// The largest rank: one plus the 52 bits below the index, all zero.
pub const MAX_RANK: u8 = 64 - PRECISION + 1;
/// Header bytes.
pub const HEADER: usize = 4;
/// Bytes of a dense sketch (the largest a sketch gets).
pub const DENSE_LEN: usize = HEADER + REGISTERS * 6 / 8;
/// The most nonzero registers a sparse sketch holds (`4 · 767 < 3072`).
pub const MAX_SPARSE: usize = (DENSE_LEN - HEADER) / 4 - 1;
/// The largest sketch, in bytes.
pub const MAX_LEN: usize = DENSE_LEN;
/// `α∞ = 1 / (2 ln 2)`, as an exact bit pattern (no runtime `ln`).
pub const ALPHA_INF: f64 = f64::from_bits(0x3fe7_1547_652b_82fe);

const ENCODING: u8 = 1;
const SPARSE: u8 = 0;
const DENSE: u8 = 1;

/// A malformed sketch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SketchError(pub &'static str);

impl fmt::Display for SketchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "malformed HLL sketch: {}", self.0)
    }
}

impl std::error::Error for SketchError {}

type Result<T> = std::result::Result<T, SketchError>;

/// The register and rank of the `i64` element `v`.
#[inline]
pub fn register_of(v: i64) -> (u16, u8) {
    register_of_hash(hash_i64(v))
}

/// The domain tag of a bytes element (D294 reserved it, D298 uses it): its hash is over the tag
/// then the bytes, so it never collides with an `i64` element's 8 untagged bytes.
pub const BYTES_DOMAIN: u8 = 0x01;

/// The hash of an `i64` element: xxh3-64 over its 8 little-endian bytes. HLL and bloom
/// families hash an element the same way.
pub fn hash_i64(v: i64) -> u64 {
    crate::checksum::xxh3_64(&v.to_le_bytes())
}

/// The hash of a bytes element: xxh3-64 over [`BYTES_DOMAIN`] then the bytes (D298). An
/// element under 256 bytes is hashed from a stack buffer; a longer one allocates once.
pub fn hash_bytes(b: &[u8]) -> u64 {
    let mut buf = [0u8; 256];
    if b.len() < buf.len() {
        buf[0] = BYTES_DOMAIN;
        buf[1..=b.len()].copy_from_slice(b);
        return crate::checksum::xxh3_64(&buf[..=b.len()]);
    }
    crate::checksum::xxh3_64(&[&[BYTES_DOMAIN][..], b].concat())
}

/// The register and rank of an element hash `h`.
pub fn register_of_hash(h: u64) -> (u16, u8) {
    let index = (h >> (64 - PRECISION)) as u16;
    let rank = ((h << PRECISION).leading_zeros() + 1).min(u32::from(MAX_RANK)) as u8;
    (index, rank)
}

/// Writes the sketch of the one element `v` into `out` (cleared first): a sparse sketch of
/// one entry, 8 bytes.
pub fn encode_element(out: &mut Vec<u8>, v: i64) {
    out.clear();
    append_element(out, v);
}

/// [`encode_element`], appended to `out` (after a tag byte, say).
pub fn append_element(out: &mut Vec<u8>, v: i64) {
    append_hashed(out, hash_i64(v));
}

/// The one-element sketch of the bytes element `b` (D298), appended to `out`.
pub fn append_bytes_element(out: &mut Vec<u8>, b: &[u8]) {
    append_hashed(out, hash_bytes(b));
}

/// The one-element sketch of the element hash `h` ([`hash_i64`], [`hash_bytes`]), appended
/// to `out`.
pub fn append_hashed(out: &mut Vec<u8>, h: u64) {
    let (index, rank) = register_of_hash(h);
    out.extend_from_slice(&[ENCODING, PRECISION, SPARSE, 0]);
    out.extend_from_slice(&entry(index, rank).to_le_bytes());
}

#[inline]
fn entry(index: u16, rank: u8) -> u32 {
    (u32::from(index) << 8) | u32::from(rank)
}

#[inline]
fn unpack(e: u32) -> (u16, u8) {
    ((e >> 8) as u16, e as u8)
}

#[inline]
fn entry_at(b: &[u8], i: usize) -> u32 {
    let at = HEADER + 4 * i;
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

#[inline]
fn set_entry(b: &mut [u8], i: usize, e: u32) {
    let at = HEADER + 4 * i;
    b[at..at + 4].copy_from_slice(&e.to_le_bytes());
}

/// Register `i` of a dense register area.
#[inline]
fn reg(area: &[u8], i: usize) -> u8 {
    let bit = 6 * i;
    let (byte, shift) = (bit / 8, bit % 8);
    let lo = u16::from(area[byte]);
    let hi = area.get(byte + 1).copied().map_or(0, u16::from);
    (((hi << 8 | lo) >> shift) & 0x3f) as u8
}

/// Raises register `i` of a dense register area to `rank` if that is larger.
#[inline]
fn raise(area: &mut [u8], i: usize, rank: u8) {
    if reg(area, i) >= rank {
        return;
    }
    let bit = 6 * i;
    let (byte, shift) = (bit / 8, bit % 8);
    let mask = 0x3fu16 << shift;
    let value = u16::from(rank) << shift;
    area[byte] = (u16::from(area[byte]) & !mask | value) as u8;
    if byte + 1 < area.len() {
        area[byte + 1] = ((u16::from(area[byte + 1]) << 8 & !mask | value) >> 8) as u8;
    }
}

/// A checked view of a sketch.
enum View<'a> {
    /// The sparse entries (each 4 bytes).
    Sparse(&'a [u8], usize),
    /// The dense register area.
    Dense(&'a [u8]),
}

/// Checks the header and length only.
fn header(s: &[u8]) -> Result<View<'_>> {
    if s.len() < HEADER {
        return Err(SketchError("shorter than its header"));
    }
    if s[0] != ENCODING {
        return Err(SketchError("unknown encoding"));
    }
    if s[1] != PRECISION {
        return Err(SketchError("unknown precision"));
    }
    if s[3] != 0 {
        return Err(SketchError("reserved byte set"));
    }
    match s[2] {
        SPARSE => {
            let body = s.len() - HEADER;
            if body == 0 || !body.is_multiple_of(4) || body / 4 > MAX_SPARSE {
                return Err(SketchError("bad sparse length"));
            }
            Ok(View::Sparse(s, body / 4))
        }
        DENSE if s.len() == DENSE_LEN => Ok(View::Dense(&s[HEADER..])),
        DENSE => Err(SketchError("bad dense length")),
        _ => Err(SketchError("unknown representation")),
    }
}

/// Checks a sketch completely: header, length, entry order and ranks, canonical form.
fn view(s: &[u8]) -> Result<View<'_>> {
    let v = header(s)?;
    match &v {
        View::Sparse(b, n) => {
            let mut last: Option<u16> = None;
            for i in 0..*n {
                let (index, rank) = unpack(entry_at(b, i));
                if entry_at(b, i) >> 20 != 0 || index as usize >= REGISTERS {
                    return Err(SketchError("register index out of range"));
                }
                if rank == 0 || rank > MAX_RANK {
                    return Err(SketchError("rank out of range"));
                }
                if last.is_some_and(|l| l >= index) {
                    return Err(SketchError("sparse entries out of order"));
                }
                last = Some(index);
            }
        }
        View::Dense(area) => {
            let mut nonzero = 0usize;
            for i in 0..REGISTERS {
                let r = reg(area, i);
                if r > MAX_RANK {
                    return Err(SketchError("rank out of range"));
                }
                nonzero += usize::from(r != 0);
            }
            if nonzero <= MAX_SPARSE {
                return Err(SketchError("dense with few registers set (not canonical)"));
            }
        }
    }
    Ok(v)
}

/// Checks that `s` is a well-formed, canonical sketch.
pub fn validate(s: &[u8]) -> Result<()> {
    view(s).map(|_| ())
}

/// Merges the sketch `other` into the sketch `acc[start..]`, in place: afterwards it holds
/// the larger rank of every register, in canonical form. `other` is checked completely, and
/// so is a sparse `acc` (its entries are read anyway: a bad index would otherwise reach the
/// dense area, perf287's S1 on #1200). A dense `acc` is checked by its header and length:
/// any six-bit register is in bounds, and its ranks are checked when it is read
/// ([`estimate`], [`validate`]). On error `acc` is unspecified.
pub fn merge_at(acc: &mut Vec<u8>, start: usize, other: &[u8]) -> Result<()> {
    let other = view(other)?;
    let (n, dense) = match header(&acc[start..])? {
        View::Sparse(..) => match view(&acc[start..])? {
            View::Sparse(_, n) => (n, false),
            View::Dense(_) => unreachable!("the header said sparse"),
        },
        View::Dense(_) => (0, true),
    };
    if dense {
        let area = &mut acc[start + HEADER..];
        match other {
            View::Sparse(b, m) => {
                for i in 0..m {
                    let (index, rank) = unpack(entry_at(b, i));
                    raise(area, usize::from(index), rank);
                }
            }
            View::Dense(o) => {
                for i in 0..REGISTERS {
                    raise(area, i, reg(o, i));
                }
            }
        }
        return Ok(());
    }
    match other {
        View::Dense(o) => {
            to_dense(acc, start, n);
            let area = &mut acc[start + HEADER..];
            for i in 0..REGISTERS {
                raise(area, i, reg(o, i));
            }
            Ok(())
        }
        View::Sparse(b, m) => {
            // The union's size, by a merge walk.
            let (mut i, mut j, mut union) = (0, 0, 0);
            {
                let a = &acc[start..];
                while i < n || j < m {
                    let ai = (i < n).then(|| unpack(entry_at(a, i)).0);
                    let bj = (j < m).then(|| unpack(entry_at(b, j)).0);
                    match (ai, bj) {
                        (Some(x), Some(y)) if x == y => (i, j) = (i + 1, j + 1),
                        (Some(x), Some(y)) if x < y => i += 1,
                        (Some(_), None) => i += 1,
                        _ => j += 1,
                    }
                    union += 1;
                }
            }
            if union > MAX_SPARSE {
                to_dense(acc, start, n);
                let area = &mut acc[start + HEADER..];
                for k in 0..m {
                    let (index, rank) = unpack(entry_at(b, k));
                    raise(area, usize::from(index), rank);
                }
                return Ok(());
            }
            // Merge from the end into the grown list: the output's end is past the input's.
            let len = start + HEADER + 4 * union;
            if acc.capacity() < start + MAX_LEN {
                acc.reserve(start + MAX_LEN - acc.len());
            }
            acc.resize(len, 0);
            let a = &mut acc[start..];
            let (mut i, mut j, mut out) = (n, m, union);
            while out > 0 {
                let ai = (i > 0).then(|| entry_at(a, i - 1));
                let bj = (j > 0).then(|| entry_at(b, j - 1));
                let e = match (ai, bj) {
                    (Some(x), Some(y)) if unpack(x).0 == unpack(y).0 => {
                        (i, j) = (i - 1, j - 1);
                        x.max(y)
                    }
                    (Some(x), Some(y)) if unpack(x).0 > unpack(y).0 => {
                        i -= 1;
                        x
                    }
                    (Some(x), None) => {
                        i -= 1;
                        x
                    }
                    (_, Some(y)) => {
                        j -= 1;
                        y
                    }
                    (None, None) => unreachable!("the union counts every entry"),
                };
                out -= 1;
                set_entry(a, out, e);
            }
            Ok(())
        }
    }
}

/// [`merge_at`] for a sketch that starts at byte 0.
pub fn merge(acc: &mut Vec<u8>, other: &[u8]) -> Result<()> {
    merge_at(acc, 0, other)
}

/// Turns the sparse sketch of `n` entries at `acc[start..]` dense, in place (its entries go
/// through a stack copy).
fn to_dense(acc: &mut Vec<u8>, start: usize, n: usize) {
    let mut entries = [0u32; MAX_SPARSE];
    for (i, e) in entries.iter_mut().enumerate().take(n) {
        *e = entry_at(&acc[start..], i);
    }
    if acc.capacity() < start + MAX_LEN {
        acc.reserve(start + MAX_LEN - acc.len());
    }
    acc.truncate(start + HEADER);
    acc.resize(start + DENSE_LEN, 0);
    acc[start + 2] = DENSE;
    let area = &mut acc[start + HEADER..];
    for &e in &entries[..n] {
        let (index, rank) = unpack(e);
        raise(area, usize::from(index), rank);
    }
}

/// The estimated number of distinct elements merged into the sketch `s`.
pub fn estimate(s: &[u8]) -> Result<u64> {
    let mut histogram = [0u32; MAX_RANK as usize + 1];
    match view(s)? {
        View::Sparse(b, n) => {
            histogram[0] = (REGISTERS - n) as u32;
            for i in 0..n {
                histogram[usize::from(unpack(entry_at(b, i)).1)] += 1;
            }
        }
        View::Dense(area) => {
            for i in 0..REGISTERS {
                histogram[usize::from(reg(area, i))] += 1;
            }
        }
    }
    Ok(ertl(&histogram))
}

/// Ertl's improved estimator over the register histogram `c` (`c[k]`: registers of rank
/// `k`, `k` in `0..=q+1`, `q = 64 - precision`).
fn ertl(c: &[u32; MAX_RANK as usize + 1]) -> u64 {
    let m = REGISTERS as f64;
    let q = usize::from(MAX_RANK) - 1;
    if c[0] as usize == REGISTERS {
        return 0;
    }
    let mut z = m * tau((m - f64::from(c[q + 1])) / m);
    for k in (1..=q).rev() {
        z = 0.5 * (z + f64::from(c[k]));
    }
    z += m * sigma(f64::from(c[0]) / m);
    (ALPHA_INF * m * m / z).round() as u64
}

/// Ertl's `σ(x) = x + Σ_{k≥1} x^(2^k) 2^(k-1)`, for `x` in `[0, 1)`.
fn sigma(mut x: f64) -> f64 {
    if x >= 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0;
    let mut z = x;
    loop {
        x *= x;
        let previous = z;
        z += x * y;
        y += y;
        if z == previous {
            return z;
        }
    }
}

/// Ertl's `τ(x) = (1/3)(1 - x - Σ_{k≥1} (1 - x^(2^-k))² 2^-k)`, for `x` in `[0, 1]`.
fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let previous = z;
        y *= 0.5;
        let d = 1.0 - x;
        z -= d * d * y;
        if z == previous {
            return z / 3.0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_constants_hold() {
        assert_eq!(DENSE_LEN, 3076);
        assert_eq!(MAX_SPARSE, 767);
        assert_eq!(MAX_RANK, 53);
        assert!((ALPHA_INF - 1.0 / (2.0 * std::f64::consts::LN_2)).abs() < 1e-16);
    }

    #[test]
    fn packed_registers_round_trip() {
        let mut area = vec![0u8; DENSE_LEN - HEADER];
        for i in 0..REGISTERS {
            raise(&mut area, i, (i % 53 + 1) as u8);
        }
        for i in 0..REGISTERS {
            assert_eq!(reg(&area, i), (i % 53 + 1) as u8, "register {i}");
        }
        raise(&mut area, 7, 1); // lower: no change
        assert_eq!(reg(&area, 7), 8);
    }
}
