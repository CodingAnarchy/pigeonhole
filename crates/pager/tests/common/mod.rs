//! Shared test helpers.
#![allow(dead_code)]

use pigeonhole_pager::Extent;
use proptest::test_runner::{Config, RngSeed};

/// A proptest config with a fixed seed: `PROPTEST_RNG_SEED` if set, otherwise a fresh random
/// one. The seed is printed so a failing run (whose output cargo shows) can be replayed.
pub fn config(cases: u32) -> Config {
    let seed = std::env::var("PROPTEST_RNG_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(random_seed);
    println!("proptest seed {seed}; replay with PROPTEST_RNG_SEED={seed}");
    Config {
        cases,
        rng_seed: RngSeed::Fixed(seed),
        failure_persistence: None,
        ..Config::default()
    }
}

/// A seed from `PIGEONHOLE_SEED` or a fresh random one, printed for replay.
pub fn seed() -> u64 {
    let seed = std::env::var("PIGEONHOLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(random_seed);
    println!("seed {seed}; replay with PIGEONHOLE_SEED={seed}");
    seed
}

fn random_seed() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    h.finish()
}

/// Byte range of an extent.
pub fn span(e: Extent) -> (u64, u64) {
    (e.offset(), e.offset() + e.len())
}

/// Whether two extents share a byte.
pub fn overlaps(a: Extent, b: Extent) -> bool {
    let (a0, a1) = span(a);
    let (b0, b1) = span(b);
    a0 < b1 && b0 < a1
}

/// SplitMix64, for deterministic workloads.
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}
