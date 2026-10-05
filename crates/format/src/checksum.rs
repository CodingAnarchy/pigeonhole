//! Checksums: CRC32C for the WAL, xxh3-64 for everything in the main file.

/// CRC32C (Castagnoli) of `bytes`. Used by WAL frames and WAL segment headers.
pub fn crc32c(bytes: &[u8]) -> u32 {
    todo!()
}

/// Extends a running CRC32C with `bytes`.
pub fn crc32c_append(crc: u32, bytes: &[u8]) -> u32 {
    todo!()
}

/// xxh3-64 (seed 0) of `bytes`. Used by blocks, the SST footer, superblocks, manifest blocks,
/// blob records and filter hashing.
pub fn xxh3_64(bytes: &[u8]) -> u64 {
    todo!()
}
