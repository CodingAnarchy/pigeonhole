//! Value encoding: a tag byte followed by the payload. Deletes have an empty value.

use crate::BlobFileId;

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
pub fn encode_value(out: &mut Vec<u8>, value: ValueRef<'_>) {
    todo!()
}

/// Decodes a stored value. An empty input is an error (deletes carry no value).
pub fn decode_value(stored: &[u8]) -> crate::Result<ValueRef<'_>> {
    todo!()
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
        todo!()
    }

    /// Decodes from exactly 16 bytes.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        todo!()
    }
}
