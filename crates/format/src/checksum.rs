//! Checksums: CRC32C for the WAL, xxh3-64 for everything in the main file.
//!
//! ```
//! use pigeonhole_format::checksum::{crc32c, crc32c_append};
//!
//! assert_eq!(crc32c_append(crc32c(b"hello "), b"world"), crc32c(b"hello world"));
//! ```

/// CRC32C (Castagnoli) of `bytes`. Used by WAL frames and WAL segment headers.
pub fn crc32c(bytes: &[u8]) -> u32 {
    crc32c_append(0, bytes)
}

/// Extends a running CRC32C with `bytes`.
pub fn crc32c_append(crc: u32, bytes: &[u8]) -> u32 {
    // The crc32c crate uses inline assembly, which Miri cannot run.
    if cfg!(miri) {
        crc32c_soft(crc, bytes)
    } else {
        ::crc32c::crc32c_append(crc, bytes)
    }
}

/// Bitwise CRC32C, used under Miri (and checked against the crate in tests).
fn crc32c_soft(crc: u32, bytes: &[u8]) -> u32 {
    let mut crc = !crc;
    for &b in bytes {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// xxh3-64 (seed 0) of `bytes`. Used by blocks, the SST footer, superblocks, manifest blocks,
/// blob records and filter hashing.
pub fn xxh3_64(bytes: &[u8]) -> u64 {
    twox_hash::XxHash3_64::oneshot(bytes)
}

/// xxh3-64 (seed 0) of the concatenation of `parts`, without copying them together.
pub(crate) fn xxh3_64_parts(parts: &[&[u8]]) -> u64 {
    use std::hash::Hasher;
    let mut h = twox_hash::XxHash3_64::with_seed(0);
    for p in parts {
        h.write(p);
    }
    h.finish()
}

#[cfg(test)]
mod tests {
    #[test]
    fn soft_crc_matches() {
        let data: Vec<u8> = (0..1000u32).map(|i| (i * 7 + i / 3) as u8).collect();
        // The standard CRC32C check value.
        assert_eq!(super::crc32c_soft(0, b"123456789"), 0xE306_9283);
        for split in [0, 1, 17, 999, 1000] {
            let (a, b) = data.split_at(split);
            assert_eq!(
                super::crc32c_soft(super::crc32c_soft(0, a), b),
                ::crc32c::crc32c(&data)
            );
        }
    }
}
