//! D298 (FORMAT §3.6): bloom filter states. No false negatives; a false-positive rate near
//! `(1 − e^(−kn/m))^k`; merges in any grouping give the bytes of one-shot adding (sparse to
//! dense included); bytes and `i64` elements hash apart; malformed states are refused.
//! Seeded; the seed prints on failure.

use pigeonhole_format::bloom::{self, BloomParams};
use pigeonhole_format::hll::{hash_bytes, hash_i64};

/// xorshift64*, seeded.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn state(p: BloomParams, hashes: &[u64]) -> Vec<u8> {
    let mut s = Vec::new();
    for &h in hashes {
        bloom::add(&mut s, p, h).unwrap();
    }
    s
}

#[test]
fn no_false_negatives_and_the_expected_false_positive_rate() {
    let p = BloomParams {
        log2_bits: 14, // 16384 bits
        hashes: 4,
    };
    let n = 2000u64;
    let s = state(p, &(0..n as i64).map(hash_i64).collect::<Vec<_>>());
    assert!((0..n as i64).all(|v| bloom::contains(&s, hash_i64(v)).unwrap()));
    let probes = 20_000i64;
    let fp = (n as i64..n as i64 + probes)
        .filter(|&v| bloom::contains(&s, hash_i64(v)).unwrap())
        .count() as f64
        / probes as f64;
    let (k, m) = (4.0, 16384.0);
    let expected = (1.0 - (-k * n as f64 / m).exp()).powf(k);
    assert!(
        (fp - expected).abs() < 0.5 * expected,
        "fp {fp} vs {expected}"
    );
    let fill = bloom::fill(&s).unwrap();
    assert!(
        (fill - (1.0 - (-k * n as f64 / m).exp())).abs() < 0.02,
        "fill {fill}"
    );
}

#[test]
fn merges_in_any_grouping_give_the_same_bytes() {
    for seed in 1..=200u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let p = BloomParams {
            log2_bits: 7 + rng.below(4) as u8,
            hashes: 1 + rng.below(5) as u8,
        };
        let hs: Vec<u64> = (0..1 + rng.below(80))
            .map(|_| hash_i64(rng.next() as i64 % 500))
            .collect();
        let one = state(p, &hs);
        bloom::validate(&one, p).unwrap();
        let mut parts: Vec<Vec<u8>> = hs
            .chunks(1 + rng.below(5) as usize)
            .map(|c| state(p, c))
            .collect();
        while parts.len() > 1 {
            let a = parts.swap_remove(rng.below(parts.len() as u64) as usize);
            let k = rng.below(parts.len() as u64) as usize;
            bloom::merge(&mut parts[k], p, &a).unwrap();
        }
        assert_eq!(parts.pop().unwrap(), one, "seed {seed}");
        // Idempotent.
        let mut twice = one.clone();
        bloom::merge(&mut twice, p, &one).unwrap();
        assert_eq!(twice, one, "seed {seed}");
    }
}

#[test]
fn bytes_and_i64_elements_hash_apart() {
    // The domain tag: the bytes of an i64's little-endian form are another element.
    for v in [0i64, 1, -1, 1 << 40] {
        assert_ne!(hash_i64(v), hash_bytes(&v.to_le_bytes()), "{v}");
    }
    let p = BloomParams {
        log2_bits: 16,
        hashes: 3,
    };
    let s = state(p, &[hash_bytes(b"alice"), hash_bytes(b"bob")]);
    assert!(bloom::contains(&s, hash_bytes(b"alice")).unwrap());
    assert!(!bloom::contains(&s, hash_bytes(b"carol")).unwrap());
}

#[test]
fn malformed_states_are_refused() {
    let p = BloomParams {
        log2_bits: 8,
        hashes: 2,
    };
    let s = state(p, &[hash_i64(1)]);
    // Other parameters; an empty sparse list; unsorted or out-of-range bits; a dense form
    // that should be sparse; a truncated bitmap.
    let other = BloomParams { hashes: 3, ..p };
    assert!(bloom::validate(&s, other).is_err());
    assert!(bloom::validate(&[6, 8, 2, 0], p).is_err());
    let mut unsorted = vec![6, 8, 2, 0];
    unsorted.extend_from_slice(&5u32.to_le_bytes());
    unsorted.extend_from_slice(&3u32.to_le_bytes());
    assert!(bloom::validate(&unsorted, p).is_err());
    let mut out_of_range = vec![6, 8, 2, 0];
    out_of_range.extend_from_slice(&256u32.to_le_bytes());
    assert!(bloom::validate(&out_of_range, p).is_err());
    let mut dense_few = vec![6, 8, 2, 1];
    dense_few.extend_from_slice(&[0u8; 32]);
    dense_few[4] = 0b11;
    assert!(bloom::validate(&dense_few, p).is_err());
    assert!(bloom::validate(&[6, 8, 2, 1, 0xff], p).is_err());
    assert!(
        bloom::contains(&[7, 8, 2, 0], 1).is_err(),
        "another encoding"
    );
}
