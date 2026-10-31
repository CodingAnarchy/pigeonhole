//! Bloom filters, the values of a `Bloom` aggregate family (decision D298; FORMAT §3.6), and
//! their parameters (`kind_params`, FORMAT §9.2.1).
//!
//! A family fixes `m` bits (a power of two, 2^6 to 2^24) and `k` hash positions (1 to 16). An
//! element's hash `h` is the HLL family's ([`crate::hll::hash_i64`], or
//! [`crate::hll::hash_bytes`] for a bytes element, behind its domain tag); its positions are
//! `(h1 + i·h2) mod m` for `i` in `0..k` (Kirsch–Mitzenmacher double hashing), with `h1` the
//! hash's low 32 bits and `h2` its high 32 bits with bit 0 set. They are exact integers, so
//! every platform sets the same bits. Merging is a bitwise OR: associative, commutative and
//! idempotent. A lookup never misses an element that was added (no false negatives).
//!
//! | Offset | Size | Field |
//! |---|---|---|
//! | 0 | 1 | encoding: 6 |
//! | 1 | 1 | `log2 m` (6 to 24) |
//! | 2 | 1 | `k` (1 to 16) |
//! | 3 | 1 | representation: 0 sparse, 1 dense |
//! | 4 | 4·n | sparse: the `n` set bits, `u32` LE, strictly increasing |
//! | 4 | m/8 | dense: the bitmap, bit `j` in byte `j / 8`, bit `j % 8` |
//!
//! It is **canonical**: sparse if and only if `4·n < m/8` (the sparse form is the smaller),
//! and never empty (a state holds at least one element). A filter only gains bits, so it
//! turns dense once and never back.
//!
//! ```
//! use pigeonhole_format::bloom::{self, BloomParams};
//!
//! let p = BloomParams { log2_bits: 10, hashes: 3 };
//! let mut s = Vec::new();
//! for v in 0..20 {
//!     bloom::add(&mut s, p, pigeonhole_format::hll::hash_i64(v)).unwrap();
//! }
//! assert!((0..20).all(|v| bloom::contains(&s, pigeonhole_format::hll::hash_i64(v)).unwrap()));
//! ```

use std::cmp::Ordering;
use std::fmt;

use crate::kind_params;

/// The encoding byte of a bloom state.
pub const ENCODING: u8 = 6;
/// `kind_params` tag of the bit count `m` (u32 LE, a power of two).
pub const TAG_BITS: u8 = 1;
/// `kind_params` tag of the hash positions per element (u8).
pub const TAG_HASHES: u8 = 2;
const HEADER: usize = 4;

/// A malformed state or parameter list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BloomError(pub &'static str);

impl fmt::Display for BloomError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for BloomError {}

/// The result type of this module.
pub type Result<T> = std::result::Result<T, BloomError>;

/// A bloom family's parameters: `2^log2_bits` bits (6 to 24) and `hashes` positions per
/// element (1 to 16).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BloomParams {
    /// `log2 m`.
    pub log2_bits: u8,
    /// `k`.
    pub hashes: u8,
}

impl BloomParams {
    /// From `kind_params`: both tags required.
    pub fn parse(params: &[u8]) -> Result<Self> {
        let (mut bits, mut hashes) = (None, None);
        for (tag, v) in
            kind_params::parse(params).map_err(|_| BloomError("malformed parameters"))?
        {
            match tag {
                TAG_BITS => {
                    let v: [u8; 4] = v.try_into().map_err(|_| BloomError("bits is a u32"))?;
                    bits = Some(u32::from_le_bytes(v));
                }
                TAG_HASHES => match v {
                    [k] => hashes = Some(*k),
                    _ => return Err(BloomError("hashes is one byte")),
                },
                t if kind_params::must_understand(t) => {
                    return Err(BloomError("an unknown bloom parameter"));
                }
                _ => {}
            }
        }
        let (Some(bits), Some(hashes)) = (bits, hashes) else {
            return Err(BloomError(
                "a bloom filter needs its bits (tag 1) and hashes (tag 2)",
            ));
        };
        if !bits.is_power_of_two() {
            return Err(BloomError("bloom bits must be a power of two, 2^6 to 2^24"));
        }
        let p = BloomParams {
            log2_bits: bits.trailing_zeros() as u8,
            hashes,
        };
        p.check()?;
        Ok(p)
    }

    /// Checks the ranges.
    pub fn check(&self) -> Result<()> {
        if !(6..=24).contains(&self.log2_bits) || !(1..=16).contains(&self.hashes) {
            return Err(BloomError(
                "bloom bits must be a power of two, 2^6 to 2^24, and hashes 1 to 16",
            ));
        }
        Ok(())
    }

    /// The `kind_params` holding both.
    pub fn encode(&self) -> Vec<u8> {
        kind_params::encode(&[
            (TAG_BITS, &(1u32 << self.log2_bits).to_le_bytes()),
            (TAG_HASHES, &[self.hashes]),
        ])
    }

    fn bits(&self) -> u32 {
        1 << self.log2_bits
    }
}

/// The bit positions of hash `h`.
fn positions(h: u64, log2_bits: u8, hashes: u8) -> impl Iterator<Item = u32> {
    let mask = (1u64 << log2_bits) - 1;
    let (h1, h2) = (h & 0xffff_ffff, (h >> 32) | 1);
    (0..u64::from(hashes)).map(move |i| (h1.wrapping_add(i.wrapping_mul(h2)) & mask) as u32)
}

/// A state's body: the sparse list's bytes, or the dense bitmap.
enum View<'a> {
    Sparse(&'a [u8]),
    Dense(&'a [u8]),
}

/// The `i`th bit of a sparse body.
fn sparse_at(body: &[u8], i: usize) -> u32 {
    u32::from_le_bytes(body[4 * i..4 * i + 4].try_into().expect("4 bytes"))
}

fn set_bit(bitmap: &mut [u8], j: u32) {
    bitmap[j as usize / 8] |= 1 << (j % 8);
}

/// The header and the body's shape (its length for the representation): O(1). The bits
/// themselves are not looked at.
fn header(s: &[u8], params: Option<BloomParams>) -> Result<(BloomParams, View<'_>)> {
    if s.len() < HEADER || s[0] != ENCODING {
        return Err(BloomError("not a bloom state"));
    }
    let p = BloomParams {
        log2_bits: s[1],
        hashes: s[2],
    };
    p.check()?;
    if params.is_some_and(|q| q != p) {
        return Err(BloomError("a bloom state of other parameters"));
    }
    let m = p.bits() as usize;
    let body = &s[HEADER..];
    match s[3] {
        0 if !body.is_empty() && body.len().is_multiple_of(4) && body.len() < m / 8 => {
            Ok((p, View::Sparse(body)))
        }
        1 if body.len() == m / 8 => Ok((p, View::Dense(body))),
        0 | 1 => Err(BloomError("bloom state not in canonical form")),
        _ => Err(BloomError("unknown bloom representation")),
    }
}

/// [`header`], then the bits: sparse ones strictly increasing and in range, a dense bitmap
/// with enough bits set to be canonical. O(the state's size).
fn view(s: &[u8], params: Option<BloomParams>) -> Result<(BloomParams, View<'_>)> {
    let (p, v) = header(s, params)?;
    match v {
        View::Sparse(b) => {
            let n = b.len() / 4;
            if (1..n).any(|i| sparse_at(b, i - 1) >= sparse_at(b, i))
                || sparse_at(b, n - 1) >= p.bits()
            {
                return Err(BloomError("bloom bits not increasing or out of range"));
            }
        }
        View::Dense(b) => {
            if 4 * set_bits(b) < b.len() {
                return Err(BloomError("bloom state not in canonical form"));
            }
        }
    }
    Ok((p, v))
}

fn set_bits(bitmap: &[u8]) -> usize {
    bitmap.iter().map(|b| b.count_ones() as usize).sum()
}

/// Writes the state of the sorted, distinct bits `set` (a new filter) to `out`.
fn encode_new(out: &mut Vec<u8>, p: BloomParams, set: &[u32]) {
    let m = p.bits() as usize;
    out.extend_from_slice(&[ENCODING, p.log2_bits, p.hashes]);
    if 4 * set.len() < m / 8 {
        out.push(0);
        for &j in set {
            out.extend_from_slice(&j.to_le_bytes());
        }
    } else {
        out.push(1);
        let at = out.len();
        out.resize(at + m / 8, 0);
        for &j in set {
            set_bit(&mut out[at..], j);
        }
    }
}

/// The sorted, distinct positions of hash `h`, in `buf`.
fn sorted_positions(buf: &mut [u32; 16], p: BloomParams, h: u64) -> &[u32] {
    let k = usize::from(p.hashes);
    for (slot, j) in buf.iter_mut().zip(positions(h, p.log2_bits, p.hashes)) {
        *slot = j;
    }
    let set = &mut buf[..k];
    set.sort_unstable();
    let mut n = 0;
    for i in 0..k {
        if n == 0 || set[i] != set[n - 1] {
            set[n] = set[i];
            n += 1;
        }
    }
    &buf[..n]
}

/// ORs the bits of `other` (a checked body of `p`) into the state at `acc[start..]`, whose
/// header was checked. A dense state is updated in place; a sparse one by an in-place merge
/// from the back, or turned dense once the union is large enough. The sparse state's own bits
/// are checked on the way (its union is walked anyway); a dense one's are not.
fn or_into(acc: &mut Vec<u8>, start: usize, p: BloomParams, other: View<'_>) -> Result<()> {
    let m = p.bits() as usize;
    let base = start + HEADER;
    if acc[start + 3] == 1 {
        let bitmap = &mut acc[base..];
        match other {
            View::Sparse(b) => (0..b.len() / 4).for_each(|i| set_bit(bitmap, sparse_at(b, i))),
            View::Dense(b) => bitmap.iter_mut().zip(b).for_each(|(x, y)| *x |= y),
        }
        return Ok(());
    }
    let n = (acc.len() - base) / 4;
    let bad = || BloomError("bloom bits not increasing or out of range");
    {
        let a = &acc[base..];
        if (1..n).any(|i| sparse_at(a, i - 1) >= sparse_at(a, i))
            || sparse_at(a, n - 1) as usize >= m
        {
            return Err(bad());
        }
    }
    let b = match other {
        View::Dense(b) => {
            let mut bitmap = b.to_vec();
            (0..n).for_each(|i| set_bit(&mut bitmap, sparse_at(&acc[base..], i)));
            acc.truncate(start + 3);
            acc.push(1);
            acc.extend_from_slice(&bitmap);
            return Ok(());
        }
        View::Sparse(b) => b,
    };
    let j = b.len() / 4;
    // The union's size, by a merge walk.
    let (mut x, mut y, mut union) = (0, 0, 0);
    while x < n || y < j {
        if x == n {
            y += 1;
        } else if y == j {
            x += 1;
        } else {
            match sparse_at(&acc[base..], x).cmp(&sparse_at(b, y)) {
                Ordering::Less => x += 1,
                Ordering::Greater => y += 1,
                Ordering::Equal => (x, y) = (x + 1, y + 1),
            }
        }
        union += 1;
    }
    if 4 * union >= m / 8 {
        let mut bitmap = vec![0u8; m / 8];
        (0..n).for_each(|i| set_bit(&mut bitmap, sparse_at(&acc[base..], i)));
        (0..j).for_each(|i| set_bit(&mut bitmap, sparse_at(b, i)));
        acc.truncate(start + 3);
        acc.push(1);
        acc.extend_from_slice(&bitmap);
        return Ok(());
    }
    // Merge from the back into the grown list: each write lands at or past the read point.
    acc.resize(base + 4 * union, 0);
    let (mut x, mut y, mut w) = (n, j, union);
    while y > 0 {
        let by = sparse_at(b, y - 1);
        let ax = (x > 0).then(|| sparse_at(&acc[base..], x - 1));
        let v = match ax {
            Some(a) if a > by => {
                x -= 1;
                a
            }
            Some(a) if a == by => {
                (x, y) = (x - 1, y - 1);
                a
            }
            _ => {
                y -= 1;
                by
            }
        };
        w -= 1;
        acc[base + 4 * w..base + 4 * w + 4].copy_from_slice(&v.to_le_bytes());
    }
    debug_assert_eq!(w, x, "the rest of the list is in place");
    Ok(())
}

/// Starts or extends `acc` (a state, or empty for none yet) with the element of hash `h`.
/// O(k) for a new or dense filter, O(the sparse list) otherwise.
pub fn add(acc: &mut Vec<u8>, params: BloomParams, h: u64) -> Result<()> {
    params.check()?;
    let mut buf = [0u32; 16];
    let set = sorted_positions(&mut buf, params, h);
    if acc.is_empty() {
        encode_new(acc, params, set);
        return Ok(());
    }
    header(acc, Some(params))?;
    let mut body = [0u8; 64];
    for (i, &j) in set.iter().enumerate() {
        body[4 * i..4 * i + 4].copy_from_slice(&j.to_le_bytes());
    }
    or_into(acc, 0, params, View::Sparse(&body[..4 * set.len()]))
}

/// The one-element state of hash `h`, appended to `out` (after a tag byte, say). O(k), unless
/// `m` is so small that k bits make it dense.
pub fn append_element(out: &mut Vec<u8>, params: BloomParams, h: u64) {
    debug_assert!(params.check().is_ok(), "checked parameters");
    let mut buf = [0u32; 16];
    encode_new(out, params, sorted_positions(&mut buf, params, h));
}

/// Merges the state `other` into `acc` (`acc` may be empty for none), both of `params`.
pub fn merge(acc: &mut Vec<u8>, params: BloomParams, other: &[u8]) -> Result<()> {
    if acc.is_empty() {
        view(other, Some(params))?;
        acc.extend_from_slice(other);
        return Ok(());
    }
    merge_at(acc, 0, params, other)
}

/// Merges the state `other` into the state at `acc[start..]`, both of `params`. `other` is
/// checked in full; of the accumulator only its header and, while sparse, its list (a dense
/// accumulator's bits are not re-read: [`validate`] it once, after a fold). O(the operand)
/// once the accumulator is dense, with no allocation.
pub fn merge_at(acc: &mut Vec<u8>, start: usize, params: BloomParams, other: &[u8]) -> Result<()> {
    let (_, o) = view(other, Some(params))?;
    header(&acc[start..], Some(params))?;
    or_into(acc, start, params, o)
}

/// Checks a state against a family's parameters.
pub fn validate(s: &[u8], params: BloomParams) -> Result<()> {
    view(s, Some(params)).map(|_| ())
}

/// Whether the element of hash `h` may be in the filter: `false` means certainly not. The
/// state carries its own parameters.
pub fn contains(s: &[u8], h: u64) -> Result<bool> {
    let (p, v) = view(s, None)?;
    let set = |j: u32| match v {
        View::Sparse(b) => {
            let (mut lo, mut hi) = (0, b.len() / 4);
            while lo < hi {
                let mid = (lo + hi) / 2;
                match sparse_at(b, mid).cmp(&j) {
                    Ordering::Less => lo = mid + 1,
                    Ordering::Greater => hi = mid,
                    Ordering::Equal => return true,
                }
            }
            false
        }
        View::Dense(b) => b[j as usize / 8] >> (j % 8) & 1 == 1,
    };
    Ok(positions(h, p.log2_bits, p.hashes).all(set))
}

/// The fraction of bits set (the false-positive rate is about this to the power `k`).
pub fn fill(s: &[u8]) -> Result<f64> {
    let (p, v) = view(s, None)?;
    let n = match v {
        View::Sparse(b) => b.len() / 4,
        View::Dense(b) => set_bits(b),
    };
    Ok(n as f64 / f64::from(p.bits()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_until_the_dense_form_is_smaller() {
        let p = BloomParams {
            log2_bits: 8, // 32 bytes dense: sparse while fewer than 8 bits are set
            hashes: 1,
        };
        let mut s = Vec::new();
        let mut v = 0;
        while s.get(3) != Some(&1) {
            add(&mut s, p, crate::hll::hash_i64(v)).unwrap();
            validate(&s, p).unwrap();
            v += 1;
        }
        assert_eq!(s.len(), HEADER + 32);
        // A dense state with fewer than 8 bits set is not canonical.
        let mut few = vec![ENCODING, 8, 1, 1];
        few.extend_from_slice(&[0u8; 32]);
        few[HEADER] = 1;
        assert!(validate(&few, p).is_err());
    }

    #[test]
    fn parameters() {
        let p = BloomParams {
            log2_bits: 20,
            hashes: 7,
        };
        assert_eq!(BloomParams::parse(&p.encode()).unwrap(), p);
        for (bits, k) in [(100u32, 3u8), (32, 3), (1 << 25, 3), (1024, 0), (1024, 17)] {
            let raw = kind_params::encode(&[(TAG_BITS, &bits.to_le_bytes()), (TAG_HASHES, &[k])]);
            assert!(BloomParams::parse(&raw).is_err(), "{bits} {k}");
        }
        assert!(BloomParams::parse(&[]).is_err(), "both required");
    }
}
