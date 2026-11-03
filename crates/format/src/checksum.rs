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
    // The SIMD paths are intrinsics Miri cannot run.
    if cfg!(miri) {
        crc32c_soft(crc, bytes)
    } else if crc == 0 {
        // The common case (every frame, header and record): crc-fast's whole-buffer entry,
        // which skips the digest's set-up (#1206 lever 4).
        crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32Iscsi, bytes) as u32
    } else {
        // Continuing a finished CRC: the digest's state is the register before the final
        // inversion, so it starts at `!crc`.
        let mut d = crc_fast::Digest::new_with_init_state(
            crc_fast::CrcAlgorithm::Crc32Iscsi,
            u64::from(!crc),
        );
        d.update(bytes);
        d.finalize() as u32
    }
}

/// Table-driven CRC32C (reflected polynomial `0x82F63B78`), used under Miri and checked
/// against the crate in tests.
fn crc32c_soft(crc: u32, bytes: &[u8]) -> u32 {
    let mut crc = !crc;
    for &b in bytes {
        crc = CRC32C_TABLE[((crc ^ u32::from(b)) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

/// CRC32C of each byte value, computed at compile time.
const CRC32C_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// xxh3-64 (seed 0) of `bytes`. Used by blocks, the SST footer, superblocks, manifest blocks,
/// blob records and filter hashing.
pub fn xxh3_64(bytes: &[u8]) -> u64 {
    twox_hash::XxHash3_64::oneshot(bytes)
}

/// xxh3-64 (seed 0) of the concatenation of `parts`, without copying them together.
pub(crate) fn xxh3_64_parts(parts: &[&[u8]]) -> u64 {
    use std::hash::Hasher;
    use twox_hash::xxhash3_64::{RawHasher, SecretBuffer};
    // The default seed and secret, borrowed: `XxHash3_64::with_seed` boxes a copy of the
    // secret per hash (#499).
    let mut h = RawHasher::new(SecretBuffer::default());
    for p in parts {
        h.write(p);
    }
    h.finish()
}

#[cfg(test)]
mod tests {
    #[test]
    fn xxh3_of_parts_is_xxh3_of_their_concatenation() {
        let data: Vec<u8> = (0..5000u32).map(|i| (i * 31 + i / 7) as u8).collect();
        for len in [0, 1, 16, 240, 241, 1024, 5000] {
            let whole = super::xxh3_64(&data[..len]);
            for split in [0, len / 3, len / 2, len] {
                let (a, b) = data[..len].split_at(split);
                assert_eq!(
                    super::xxh3_64_parts(&[a, b]),
                    whole,
                    "len {len} split {split}"
                );
            }
        }
    }

    #[test]
    fn soft_crc_matches() {
        let data: Vec<u8> = (0..1000u32).map(|i| (i * 7 + i / 3) as u8).collect();
        // The standard CRC32C check value, on both paths.
        assert_eq!(super::crc32c_soft(0, b"123456789"), 0xE306_9283);
        assert_eq!(super::crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(super::crc32c(b""), 0);
        // Every length a frame takes (the small-input path below 16 bytes and around the
        // SIMD block sizes), and continuing a finished CRC at any split.
        for len in [
            0, 1, 7, 8, 15, 16, 17, 63, 64, 65, 127, 128, 129, 255, 256, 257, 1000,
        ] {
            let whole = super::crc32c(&data[..len]);
            assert_eq!(whole, super::crc32c_soft(0, &data[..len]), "len {len}");
            for split in [0, 1, len / 2, len.saturating_sub(1), len] {
                let (a, b) = data[..len].split_at(split.min(len));
                assert_eq!(
                    super::crc32c_soft(super::crc32c_soft(0, a), b),
                    whole,
                    "soft len {len} split {split}"
                );
                assert_eq!(
                    super::crc32c_append(super::crc32c(a), b),
                    whole,
                    "len {len} split {split}"
                );
            }
        }
    }
}
