//! LEB128 unsigned varints, as used inside blocks, WAL batches and manifest edits.
//!
//! ```
//! use pigeonhole_format::varint;
//!
//! let mut out = Vec::new();
//! varint::put_u64(&mut out, 300);
//! assert_eq!(out, [0xAC, 0x02]);
//! assert_eq!(varint::get_u64(&out).unwrap(), (300, 2));
//! ```

use crate::Error;

/// Maximum encoded length of a `u64` varint.
pub const MAX_VARINT_LEN: usize = 10;

/// Appends `v` as a LEB128 varint.
pub fn put_u64(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Decodes a varint from the front of `input`; returns the value and the bytes consumed.
/// Only the canonical (shortest) encoding is accepted, so every value has one encoding.
#[inline]
pub fn get_u64(input: &[u8]) -> crate::Result<(u64, usize)> {
    // One byte (a value below 128): most lengths and counts, decoded inline.
    if let Some(&b) = input.first()
        && b < 0x80
    {
        return Ok((u64::from(b), 1));
    }
    get_u64_long(input)
}

/// [`get_u64`] past its one-byte case.
fn get_u64_long(input: &[u8]) -> crate::Result<(u64, usize)> {
    let mut v = 0u64;
    for (i, &b) in input.iter().take(MAX_VARINT_LEN).enumerate() {
        // The tenth byte may only carry the top bit of a u64.
        if i == MAX_VARINT_LEN - 1 && b > 1 {
            return Err(Error::Corrupt { what: "varint" });
        }
        v |= u64::from(b & 0x7F) << (7 * i);
        if b < 0x80 {
            // A zero final byte after the first means an overlong (non-canonical) encoding.
            if b == 0 && i > 0 {
                return Err(Error::Corrupt { what: "varint" });
            }
            return Ok((v, i + 1));
        }
    }
    if input.len() >= MAX_VARINT_LEN {
        Err(Error::Corrupt { what: "varint" })
    } else {
        Err(Error::Truncated { what: "varint" })
    }
}

/// Decodes a varint that must fit in a `u32`.
pub fn get_u32(input: &[u8]) -> crate::Result<(u32, usize)> {
    let (v, n) = get_u64(input)?;
    let v = u32::try_from(v).map_err(|_| Error::Corrupt { what: "varint u32" })?;
    Ok((v, n))
}

/// Appends a varint length prefix followed by `bytes`.
pub fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u64(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// Decodes a varint-length-prefixed byte string; returns it and the bytes consumed.
#[inline]
pub fn get_bytes(input: &[u8]) -> crate::Result<(&[u8], usize)> {
    let (len, n) = get_u64(input)?;
    let rest = &input[n..];
    match usize::try_from(len) {
        Ok(len) if len <= rest.len() => Ok((&rest[..len], n + len)),
        _ => Err(Error::Truncated {
            what: "length-prefixed bytes",
        }),
    }
}
