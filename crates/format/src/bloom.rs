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

/// A decoded filter: its parameters and set bits.
enum Bits {
    Sparse(Vec<u32>),
    Dense(Vec<u8>),
}

fn decode(s: &[u8], params: Option<BloomParams>) -> Result<(BloomParams, Bits)> {
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
    let bits = match s[3] {
        0 => {
            if body.is_empty() || !body.len().is_multiple_of(4) || body.len() >= m / 8 {
                return Err(BloomError("bloom state not in canonical form"));
            }
            let set: Vec<u32> = body
                .chunks(4)
                .map(|c| u32::from_le_bytes(c.try_into().expect("4 bytes")))
                .collect();
            if set.windows(2).any(|w| w[0] >= w[1]) || set.last().is_some_and(|&b| b as usize >= m)
            {
                return Err(BloomError("bloom bits not increasing or out of range"));
            }
            Bits::Sparse(set)
        }
        1 => {
            let n: u32 = body.iter().map(|b| b.count_ones()).sum();
            if body.len() != m / 8 || 4 * (n as usize) < m / 8 {
                return Err(BloomError("bloom state not in canonical form"));
            }
            Bits::Dense(body.to_vec())
        }
        _ => return Err(BloomError("unknown bloom representation")),
    };
    Ok((p, bits))
}

/// Writes the canonical encoding of `bitmap` (a dense bitmap of `p`'s size).
fn encode(out: &mut Vec<u8>, p: BloomParams, bitmap: &[u8]) {
    out.clear();
    out.extend_from_slice(&[ENCODING, p.log2_bits, p.hashes]);
    let n: usize = bitmap.iter().map(|b| b.count_ones() as usize).sum();
    if 4 * n < bitmap.len() {
        out.push(0);
        for (i, byte) in bitmap.iter().enumerate() {
            for bit in 0..8 {
                if byte >> bit & 1 == 1 {
                    out.extend_from_slice(&((8 * i + bit) as u32).to_le_bytes());
                }
            }
        }
    } else {
        out.push(1);
        out.extend_from_slice(bitmap);
    }
}

fn bitmap(p: BloomParams, bits: &Bits) -> Vec<u8> {
    match bits {
        Bits::Dense(b) => b.clone(),
        Bits::Sparse(set) => {
            let mut b = vec![0u8; p.bits() as usize / 8];
            for &j in set {
                b[j as usize / 8] |= 1 << (j % 8);
            }
            b
        }
    }
}

/// Starts or extends `acc` (a state, or empty for none yet) with the element of hash `h`.
pub fn add(acc: &mut Vec<u8>, params: BloomParams, h: u64) -> Result<()> {
    params.check()?;
    let mut b = if acc.is_empty() {
        vec![0u8; params.bits() as usize / 8]
    } else {
        let (_, bits) = decode(acc, Some(params))?;
        bitmap(params, &bits)
    };
    for j in positions(h, params.log2_bits, params.hashes) {
        b[j as usize / 8] |= 1 << (j % 8);
    }
    encode(acc, params, &b);
    Ok(())
}

/// The one-element state of hash `h`, appended to `out` (after a tag byte, say).
pub fn append_element(out: &mut Vec<u8>, params: BloomParams, h: u64) {
    let mut s = Vec::new();
    add(&mut s, params, h).expect("checked parameters");
    out.extend_from_slice(&s);
}

/// Merges the state `other` into `acc` (`acc` may be empty for none), both of `params`.
pub fn merge(acc: &mut Vec<u8>, params: BloomParams, other: &[u8]) -> Result<()> {
    let (_, o) = decode(other, Some(params))?;
    let mut b = if acc.is_empty() {
        vec![0u8; params.bits() as usize / 8]
    } else {
        let (_, bits) = decode(acc, Some(params))?;
        bitmap(params, &bits)
    };
    for (x, y) in b.iter_mut().zip(bitmap(params, &o)) {
        *x |= y;
    }
    encode(acc, params, &b);
    Ok(())
}

/// Checks a state against a family's parameters.
pub fn validate(s: &[u8], params: BloomParams) -> Result<()> {
    decode(s, Some(params)).map(|_| ())
}

/// Whether the element of hash `h` may be in the filter: `false` means certainly not. The
/// state carries its own parameters.
pub fn contains(s: &[u8], h: u64) -> Result<bool> {
    let (p, bits) = decode(s, None)?;
    let set = |j: u32| match &bits {
        Bits::Sparse(v) => v.binary_search(&j).is_ok(),
        Bits::Dense(b) => b[j as usize / 8] >> (j % 8) & 1 == 1,
    };
    Ok(positions(h, p.log2_bits, p.hashes).all(set))
}

/// The fraction of bits set (the false-positive rate is about this to the power `k`).
pub fn fill(s: &[u8]) -> Result<f64> {
    let (p, bits) = decode(s, None)?;
    let n = match &bits {
        Bits::Sparse(v) => v.len() as u64,
        Bits::Dense(b) => b.iter().map(|x| u64::from(x.count_ones())).sum(),
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
