//! Seeded randomness and the YCSB request distributions.
//!
//! Everything here is deterministic for a given seed, so two runs with the same
//! [`WorkloadConfig::seed`](crate::WorkloadConfig) generate the same operations.

/// SplitMix64: small, fast and good enough for workload generation.
#[derive(Debug, Clone)]
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)`.
    pub(crate) fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Uniform in `[0, n)`; `n` must be non-zero.
    pub(crate) fn below(&mut self, n: u64) -> u64 {
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    /// Uniform in `[lo, hi]`.
    pub(crate) fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi - lo + 1)
    }

    pub(crate) fn bytes(&mut self, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            let word = self.next_u64().to_le_bytes();
            let take = (len - out.len()).min(8);
            out.extend_from_slice(&word[..take]);
        }
        out
    }
}

/// 64-bit FNV-1a of an integer, as YCSB uses to scatter keys.
pub(crate) fn fnv64(v: u64) -> u64 {
    let mut h: u64 = 0xCBF2_9CE4_8422_2325;
    for b in v.to_le_bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}

/// YCSB's Zipfian constant.
pub(crate) const ZIPF_THETA: f64 = 0.99;

/// Zipfian over `[0, n)`, item 0 most popular (Gray et al., "Quickly generating
/// billion-record synthetic databases", as in YCSB's `ZipfianGenerator`).
#[derive(Debug, Clone)]
pub(crate) struct Zipf {
    n: u64,
    zetan: f64,
    alpha: f64,
    eta: f64,
    half_pow_theta: f64,
}

impl Zipf {
    pub(crate) fn new(n: u64) -> Self {
        let n = n.max(1);
        let theta = ZIPF_THETA;
        let zetan: f64 = (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum();
        let zeta2 = 1.0 + 0.5f64.powf(theta);
        let eta = if n <= 2 {
            0.0
        } else {
            (1.0 - (2.0 / n as f64).powf(1.0 - theta)) / (1.0 - zeta2 / zetan)
        };
        Self {
            n,
            zetan,
            alpha: 1.0 / (1.0 - theta),
            eta,
            half_pow_theta: 0.5f64.powf(theta),
        }
    }

    pub(crate) fn sample(&self, rng: &mut Rng) -> u64 {
        let u = rng.next_f64();
        let uz = u * self.zetan;
        if uz < 1.0 || self.n == 1 {
            return 0;
        }
        if uz < 1.0 + self.half_pow_theta || self.n == 2 {
            return 1;
        }
        let v = (self.n as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64;
        v.min(self.n - 1)
    }

    /// Zipfian popularity, but the popular items are scattered over `[0, n)` instead
    /// of clustered at the low end (YCSB's `ScrambledZipfianGenerator`).
    pub(crate) fn sample_scrambled(&self, rng: &mut Rng) -> u64 {
        fnv64(self.sample(rng)) % self.n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rng_is_deterministic() {
        let a: Vec<u64> = (0..8)
            .map({
                let mut r = Rng::new(7);
                move |_| r.next_u64()
            })
            .collect();
        let b: Vec<u64> = (0..8)
            .map({
                let mut r = Rng::new(7);
                move |_| r.next_u64()
            })
            .collect();
        assert_eq!(a, b);
    }

    #[test]
    fn zipf_stays_in_range_and_is_skewed() {
        let seed = 42;
        let z = Zipf::new(1000);
        let mut rng = Rng::new(seed);
        let mut counts = vec![0u32; 1000];
        for _ in 0..100_000 {
            let v = z.sample(&mut rng);
            assert!(v < 1000, "seed {seed}");
            counts[v as usize] += 1;
        }
        // Item 0 is the most popular and much more popular than the median item.
        assert!(counts[0] > counts[1], "seed {seed}");
        assert!(counts[0] > 20 * counts[500].max(1), "seed {seed}");
        let s = z.sample_scrambled(&mut rng);
        assert!(s < 1000);
    }

    #[test]
    fn tiny_domains() {
        let mut rng = Rng::new(1);
        for n in 1..4 {
            let z = Zipf::new(n);
            for _ in 0..100 {
                assert!(z.sample(&mut rng) < n);
            }
        }
    }

    #[test]
    fn range_bounds() {
        let mut rng = Rng::new(3);
        for _ in 0..1000 {
            let v = rng.range(5, 9);
            assert!((5..=9).contains(&v));
        }
        assert_eq!(rng.bytes(13).len(), 13);
    }
}
