//! Value encoding: a tag byte followed by the payload. Deletes have an empty value.

use crate::bytes::{le_u32, le_u64};
use crate::{BlobFileId, Error};

/// Largest value accepted, in bytes (`2^32 - 1`).
pub const MAX_VALUE_LEN: u64 = u32::MAX as u64;

/// The first byte of every non-empty stored value. Numbers are frozen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ValueTag {
    /// Opaque bytes.
    Bytes = 0x00,
    /// A little-endian `i64` (8 bytes); the operand type of the built-in counter.
    I64 = 0x01,
    /// A little-endian IEEE-754 `f64` bit pattern (8 bytes).
    F64 = 0x02,
    /// A zigzag LEB128 signed integer.
    Varint = 0x03,
    /// A 16-byte [`BlobPointer`]; the bytes live in a blob extent.
    Blob = 0x80,
}

/// A decoded stored value, borrowing from the input.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueRef<'a> {
    /// Opaque bytes.
    Bytes(&'a [u8]),
    /// A signed 64-bit integer.
    I64(i64),
    /// A 64-bit float.
    F64(f64),
    /// A varint-encoded signed integer.
    Varint(i64),
    /// A pointer to a separated blob.
    Blob(BlobPointer),
}

/// Appends an encoded value to `out`.
///
/// ```
/// use pigeonhole_format::value::{ValueRef, decode_value, encode_value};
///
/// let mut out = Vec::new();
/// encode_value(&mut out, ValueRef::I64(-5));
/// assert_eq!(decode_value(&out).unwrap(), ValueRef::I64(-5));
/// ```
pub fn encode_value(out: &mut Vec<u8>, value: ValueRef<'_>) {
    match value {
        ValueRef::Bytes(b) => {
            out.push(ValueTag::Bytes as u8);
            out.extend_from_slice(b);
        }
        ValueRef::I64(v) => {
            out.push(ValueTag::I64 as u8);
            out.extend_from_slice(&v.to_le_bytes());
        }
        ValueRef::F64(v) => {
            out.push(ValueTag::F64 as u8);
            out.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        ValueRef::Varint(v) => {
            out.push(ValueTag::Varint as u8);
            crate::varint::put_u64(out, ((v << 1) ^ (v >> 63)) as u64);
        }
        ValueRef::Blob(p) => {
            out.push(ValueTag::Blob as u8);
            out.extend_from_slice(&p.encode());
        }
    }
}

/// Decodes a stored value. An empty input is an error (deletes carry no value).
pub fn decode_value(stored: &[u8]) -> crate::Result<ValueRef<'_>> {
    let Some((&tag, payload)) = stored.split_first() else {
        return Err(Error::Truncated { what: "value" });
    };
    let fixed8 = |payload: &[u8]| -> crate::Result<[u8; 8]> {
        payload.try_into().map_err(|_| Error::Corrupt {
            what: "value length",
        })
    };
    match tag {
        0x00 => Ok(ValueRef::Bytes(payload)),
        0x01 => Ok(ValueRef::I64(i64::from_le_bytes(fixed8(payload)?))),
        0x02 => Ok(ValueRef::F64(f64::from_bits(u64::from_le_bytes(fixed8(
            payload,
        )?)))),
        0x03 => {
            let (z, n) = crate::varint::get_u64(payload)?;
            if n != payload.len() {
                return Err(Error::Corrupt {
                    what: "value length",
                });
            }
            Ok(ValueRef::Varint(((z >> 1) as i64) ^ -((z & 1) as i64)))
        }
        0x80 => Ok(ValueRef::Blob(BlobPointer::decode(payload)?)),
        _ => Err(Error::Corrupt { what: "value tag" }),
    }
}

/// The 16-byte pointer an LSM entry stores in place of a separated value.
///
/// ```text
/// 0  blob_file u32 LE   4  len u32 LE   8  offset u64 LE (logical offset in the blob file)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlobPointer {
    /// The logical blob file.
    pub blob_file: BlobFileId,
    /// Value length in bytes.
    pub len: u32,
    /// Logical offset of the blob record (its header) within the blob file.
    pub offset: u64,
}

impl BlobPointer {
    /// Encoded size.
    pub const LEN: usize = 16;

    /// Encodes to 16 bytes.
    pub fn encode(&self) -> [u8; 16] {
        let mut out = [0; 16];
        out[0..4].copy_from_slice(&self.blob_file.0.to_le_bytes());
        out[4..8].copy_from_slice(&self.len.to_le_bytes());
        out[8..16].copy_from_slice(&self.offset.to_le_bytes());
        out
    }

    /// Decodes from exactly 16 bytes.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        if bytes.len() != Self::LEN {
            return Err(Error::Corrupt {
                what: "blob pointer length",
            });
        }
        Ok(Self {
            blob_file: BlobFileId(le_u32(bytes, 0)),
            len: le_u32(bytes, 4),
            offset: le_u64(bytes, 8),
        })
    }
}
