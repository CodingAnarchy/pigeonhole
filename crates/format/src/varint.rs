//! LEB128 unsigned varints, as used inside blocks, WAL batches and manifest edits.

/// Maximum encoded length of a `u64` varint.
pub const MAX_VARINT_LEN: usize = 10;

/// Appends `v` as a LEB128 varint.
pub fn put_u64(out: &mut Vec<u8>, v: u64) {
    todo!()
}

/// Decodes a varint from the front of `input`; returns the value and the bytes consumed.
pub fn get_u64(input: &[u8]) -> crate::Result<(u64, usize)> {
    todo!()
}

/// Decodes a varint that must fit in a `u32`.
pub fn get_u32(input: &[u8]) -> crate::Result<(u32, usize)> {
    todo!()
}

/// Appends a varint length prefix followed by `bytes`.
pub fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    todo!()
}

/// Decodes a varint-length-prefixed byte string; returns it and the bytes consumed.
pub fn get_bytes(input: &[u8]) -> crate::Result<(&[u8], usize)> {
    todo!()
}
