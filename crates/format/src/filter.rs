//! Per-SST filters: a cache-line-blocked bloom filter over row keys and another over
//! row+qualifier. Pinned in memory while the SST is open. See `FORMAT.md` §6.
//!
//! ```
//! use pigeonhole_format::filter::{Filter, FilterBuilder, row_hash};
//!
//! let mut b = FilterBuilder::new(10);
//! b.add_hash(row_hash(b"alice"));
//! let mut block = Vec::new();
//! b.finish(&mut block);
//! let f = Filter::new(block).unwrap();
//! assert!(f.may_contain(row_hash(b"alice")));
//! ```
//!
//! The filter block's first byte names the filter kind so a ribbon filter can be added later
//! without breaking old files.

use std::ops::Deref;

use crate::Error;

/// Filter kinds. Numbers are frozen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FilterKind {
    /// 512-bit blocked bloom filter with double hashing inside one cache line.
    BlockedBloom = 1,
}

/// Size of the fixed header of a filter block.
const HEADER_LEN: usize = 8;
/// Bits per filter line (one cache line).
const LINE_BITS: u64 = 512;
const LINE_BYTES: usize = 64;

/// Hash of a row key for the row filter: xxh3-64 of the escaped row bytes.
pub fn row_hash(escaped_row: &[u8]) -> u64 {
    crate::checksum::xxh3_64(escaped_row)
}

/// Hash of a column for the column filter: xxh3-64 of the column prefix (escaped row,
/// terminator, escaped qualifier, terminator). For a row's family markers the key is the
/// marker prefix ([`crate::key::encode_marker_prefix`]); a point get probes both.
pub fn column_hash(column_prefix: &[u8]) -> u64 {
    crate::checksum::xxh3_64(column_prefix)
}

/// The probe sequence of FORMAT §6: the line, then `k` bit positions within it.
fn probe(hash: u64, num_lines: u32) -> (usize, u32, u32) {
    let h1 = hash as u32;
    let h2 = (hash >> 32) as u32;
    let line = ((u64::from(h2) * u64::from(num_lines)) >> 32) as usize;
    (line, h1, h1.rotate_right(17))
}

/// Builds a filter from hashes.
#[derive(Debug, Default)]
pub struct FilterBuilder {
    bits_per_key: u8,
    hashes: Vec<u64>,
}

impl FilterBuilder {
    /// A builder targeting `bits_per_key` bits per distinct key.
    pub fn new(bits_per_key: u8) -> Self {
        Self {
            bits_per_key,
            hashes: Vec::new(),
        }
    }

    /// Adds a key hash. Duplicate consecutive hashes are ignored.
    pub fn add_hash(&mut self, hash: u64) {
        if self.hashes.last() != Some(&hash) {
            self.hashes.push(hash);
        }
    }

    /// Appends the logical filter block to `out` and resets the builder.
    pub fn finish(&mut self, out: &mut Vec<u8>) {
        let bpk = u64::from(self.bits_per_key);
        // floor(bpk * 0.69) in integers, so every build computes the same k.
        let probes = (bpk * 69 / 100).clamp(1, 16) as u32;
        let bits = self.hashes.len() as u64 * bpk;
        let num_lines = bits.div_ceil(LINE_BITS).clamp(1, u64::from(u32::MAX)) as u32;
        out.extend_from_slice(&[FilterKind::BlockedBloom as u8, probes as u8, 0, 0]);
        out.extend_from_slice(&num_lines.to_le_bytes());
        let start = out.len();
        out.resize(start + num_lines as usize * LINE_BYTES, 0);
        let lines = &mut out[start..];
        for &h in &self.hashes {
            let (line, mut x, delta) = probe(h, num_lines);
            let line = &mut lines[line * LINE_BYTES..(line + 1) * LINE_BYTES];
            for _ in 0..probes {
                let bit = (x & 511) as usize;
                line[bit / 8] |= 1 << (bit % 8);
                x = x.wrapping_add(delta);
            }
        }
        self.hashes.clear();
    }
}

/// A filter over its logical block bytes, held by any byte owner (zero-copy; an SST reader
/// keeps it pinned beside the rest of its state).
#[derive(Debug, Clone)]
pub struct Filter<B> {
    bytes: B,
    probes: u32,
    num_lines: u32,
}

impl<B: Deref<Target = [u8]>> Filter<B> {
    /// Parses a logical filter block. Never panics.
    pub fn new(logical: B) -> crate::Result<Self> {
        let b: &[u8] = &logical;
        if b.len() < HEADER_LEN {
            return Err(Error::Truncated { what: "filter" });
        }
        if b[0] != FilterKind::BlockedBloom as u8 {
            return Err(Error::Corrupt {
                what: "filter kind",
            });
        }
        let probes = u32::from(b[1]);
        let num_lines = crate::bytes::le_u32(b, 4);
        let expected = (num_lines as usize)
            .checked_mul(LINE_BYTES)
            .and_then(|n| n.checked_add(HEADER_LEN));
        if !(1..=16).contains(&probes) || num_lines == 0 || expected != Some(b.len()) {
            return Err(Error::Corrupt {
                what: "filter header",
            });
        }
        Ok(Self {
            bytes: logical,
            probes,
            num_lines,
        })
    }

    /// `false` means the key is definitely absent; `true` means it may be present.
    pub fn may_contain(&self, hash: u64) -> bool {
        let (line, mut x, delta) = probe(hash, self.num_lines);
        let start = HEADER_LEN + line * LINE_BYTES;
        let line = &self.bytes[start..start + LINE_BYTES];
        for _ in 0..self.probes {
            let bit = (x & 511) as usize;
            if line[bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
            x = x.wrapping_add(delta);
        }
        true
    }
}
