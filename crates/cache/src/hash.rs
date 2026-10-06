//! A small, fast, deterministic hasher for cache keys (no allocation, no random state).
//!
//! Keys are not attacker-controlled hash-table keys in the DoS sense: block keys are file ids
//! and offsets chosen by the engine, and row-cache collisions only cost a miss (see
//! [`RowCache`](crate::RowCache)).

use std::hash::{BuildHasher, Hasher};

const K: u64 = 0x9e37_79b9_7f4a_7c15;

/// Builds [`KeyHasher`]s.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BuildKeyHasher;

impl BuildHasher for BuildKeyHasher {
    type Hasher = KeyHasher;

    fn build_hasher(&self) -> KeyHasher {
        KeyHasher(0)
    }
}

/// Multiply-rotate mixing per word, with a full avalanche in `finish` (block offsets are
/// multiples of 4096, so their low bits would otherwise all be zero).
#[derive(Debug, Clone, Copy)]
pub(crate) struct KeyHasher(u64);

impl KeyHasher {
    #[inline]
    fn add(&mut self, w: u64) {
        self.0 = (self.0.rotate_left(5) ^ w).wrapping_mul(K);
    }
}

impl Hasher for KeyHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let (words, rest) = bytes.as_chunks::<8>();
        for w in words {
            self.add(u64::from_le_bytes(*w));
        }
        if !rest.is_empty() {
            let mut tail = [0u8; 8];
            tail[..rest.len()].copy_from_slice(rest);
            self.add(u64::from_le_bytes(tail));
        }
        self.add(bytes.len() as u64);
    }

    #[inline]
    fn write_u64(&mut self, n: u64) {
        self.add(n);
    }

    #[inline]
    fn write_u32(&mut self, n: u32) {
        self.add(u64::from(n));
    }

    #[inline]
    fn write_usize(&mut self, n: usize) {
        self.add(n as u64);
    }

    #[inline]
    fn finish(&self) -> u64 {
        fmix64(self.0)
    }
}

/// MurmurHash3's 64-bit finalizer.
#[inline]
pub(crate) fn fmix64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

/// Hashes any `Hash` value with [`KeyHasher`].
#[inline]
pub(crate) fn hash_of<T: std::hash::Hash + ?Sized>(v: &T) -> u64 {
    BuildKeyHasher.hash_one(v)
}
