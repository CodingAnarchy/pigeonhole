//! D298 (FORMAT §3.4, §3.5): histograms and quantile sketches. Merges in random groupings
//! give the same bytes as adding every input to one state, collapses included (revision 1's
//! S3 test); quantiles stay within 2^-p of the exact order statistic; histogram counts match
//! a direct count. Seeded; the seed prints on failure.

use pigeonhole_format::buckets::{self, QuantileParams};

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

    /// A finite value over 30 binades (fewer buckets than a store keeps at 7 mantissa bits,
    /// so nothing collapses), either sign, or zero.
    fn narrow(&mut self) -> f64 {
        match self.below(10) {
            0 => 0.0,
            _ => {
                let m = 1.0 + (self.next() >> 11) as f64 / (1u64 << 53) as f64;
                let x = m * 2f64.powi(self.below(30) as i32 - 15);
                if self.next() & 1 == 1 { -x } else { x }
            }
        }
    }

    /// A finite value over many binades, either sign, or zero.
    fn value(&mut self) -> f64 {
        match self.below(10) {
            0 => 0.0,
            1 => -0.0,
            _ => {
                let m = 1.0 + (self.next() >> 11) as f64 / (1u64 << 53) as f64;
                let x = m * 2f64.powi(self.below(200) as i32 - 100);
                if self.next() & 1 == 1 { -x } else { x }
            }
        }
    }
}

/// The bytes of folding `xs` in a random grouping: random runs added apart, then merged in a
/// random order.
fn grouped(
    xs: &[f64],
    rng: &mut Rng,
    add: &dyn Fn(&mut Vec<u8>, f64),
    merge: &dyn Fn(&mut Vec<u8>, &[u8]),
) -> Vec<u8> {
    let mut parts: Vec<Vec<u8>> = Vec::new();
    let mut i = 0;
    while i < xs.len() {
        let len = 1 + rng.below(6) as usize;
        let mut p = Vec::new();
        for &x in &xs[i..(i + len).min(xs.len())] {
            add(&mut p, x);
        }
        parts.push(p);
        i += len;
    }
    while parts.len() > 1 {
        let a = parts.swap_remove(rng.below(parts.len() as u64) as usize);
        let k = rng.below(parts.len() as u64) as usize;
        merge(&mut parts[k], &a);
    }
    parts.pop().unwrap_or_default()
}

#[test]
fn quantile_merges_are_associative_through_collapses() {
    for seed in 1..=200u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        // Few buckets and coarse indexes, so values over 200 binades collapse often.
        let params = QuantileParams {
            mantissa_bits: 1 + rng.below(4) as u8,
            max_buckets: 16,
        };
        let xs: Vec<f64> = (0..1 + rng.below(300)).map(|_| rng.value()).collect();
        let mut one = Vec::new();
        for &x in &xs {
            buckets::quantiles_add(&mut one, params, x).unwrap();
        }
        let other = grouped(
            &xs,
            &mut rng,
            &|a, x| buckets::quantiles_add(a, params, x).unwrap(),
            &|a, b| buckets::quantiles_merge(a, params, b).unwrap(),
        );
        assert_eq!(one, other, "seed {seed}");
        buckets::quantiles_validate(&one, params).unwrap();
        assert_eq!(buckets::quantiles_count(&one).unwrap(), xs.len() as u64);
    }
}

#[test]
fn quantiles_stay_within_the_relative_error() {
    for seed in 1..=100u64 {
        let mut rng = Rng(seed | 1);
        let params = QuantileParams {
            mantissa_bits: 7,
            max_buckets: 4096,
        };
        let mut xs: Vec<f64> = (0..1 + rng.below(500)).map(|_| rng.narrow()).collect();
        let mut s = Vec::new();
        for &x in &xs {
            buckets::quantiles_add(&mut s, params, x).unwrap();
        }
        xs.sort_by(f64::total_cmp);
        for q in [0.0, 0.01, 0.25, 0.5, 0.9, 0.99, 1.0] {
            let rank = ((q * (xs.len() - 1) as f64).floor() as usize).min(xs.len() - 1);
            let exact = xs[rank];
            let got = buckets::quantile(&s, q).unwrap();
            if exact == 0.0 {
                assert_eq!(got, 0.0, "seed {seed} q {q}");
            } else {
                // A bucket spans a factor 1 + 2^-p; its midpoint is within 2^-(p+1) of any
                // member, relative to the bucket's low edge.
                let err = (got - exact).abs() / exact.abs();
                assert!(err <= 2f64.powi(-7), "seed {seed} q {q}: {got} vs {exact}");
                assert_eq!(got.signum(), exact.signum(), "seed {seed} q {q}");
            }
        }
        assert_eq!(buckets::quantile(&s, 1.5), None);
    }
}

#[test]
fn histograms_count_and_merge_in_any_grouping() {
    let bounds =
        buckets::histogram_bounds(&buckets::histogram_params(&[-10.0, 0.0, 1.0, 100.0])).unwrap();
    for seed in 1..=100u64 {
        let mut rng = Rng(seed | 1);
        let xs: Vec<f64> = (0..1 + rng.below(200))
            .map(|_| match rng.below(8) {
                0 => f64::INFINITY,
                1 => f64::NEG_INFINITY,
                _ => rng.value() * 50.0,
            })
            .collect();
        let mut one = Vec::new();
        for &x in &xs {
            buckets::histogram_add(&mut one, &bounds, x).unwrap();
        }
        let other = grouped(
            &xs,
            &mut rng,
            &|a, x| buckets::histogram_add(a, &bounds, x).unwrap(),
            &|a, b| buckets::histogram_merge(a, &bounds, b).unwrap(),
        );
        assert_eq!(one, other, "seed {seed}");
        let mut want = vec![0u64; 5];
        for &x in &xs {
            let b = bounds.iter().filter(|b| b.total_cmp(&x).is_le()).count();
            want[b] += 1;
        }
        assert_eq!(
            buckets::histogram_counts(&one, Some(5)).unwrap(),
            want,
            "seed {seed}"
        );
    }
}

#[test]
fn malformed_states_and_inputs_are_refused() {
    let p = QuantileParams::default();
    let mut s = Vec::new();
    assert!(buckets::quantiles_add(&mut s, p, f64::NAN).is_err());
    assert!(buckets::quantiles_add(&mut s, p, f64::INFINITY).is_err());
    buckets::quantiles_add(&mut s, p, 3.0).unwrap();
    // Another family's mantissa bits.
    let other = QuantileParams {
        mantissa_bits: 5,
        ..p
    };
    assert!(buckets::quantiles_validate(&s, other).is_err());
    // A sketch of no inputs; a zero-count bucket; an index gap of 0.
    assert!(buckets::quantiles_validate(&[5, 7, 0, 0, 0], p).is_err());
    assert!(buckets::quantiles_validate(&[5, 7, 0, 1, 3, 0, 0], p).is_err());
    assert!(buckets::quantiles_validate(&[5, 7, 0, 2, 3, 1, 0, 1, 0], p).is_err());
    // Histograms: NaN refused; a trailing zero count, or the wrong bucket count, malformed.
    let b = [0.0, 1.0];
    let mut h = Vec::new();
    assert!(buckets::histogram_add(&mut h, &b, f64::NAN).is_err());
    buckets::histogram_add(&mut h, &b, 0.5).unwrap();
    assert_eq!(h, [4, 3, 0, 1]);
    assert!(buckets::histogram_counts(&[4, 3, 0, 1, 0], Some(3)).is_err());
    assert!(buckets::histogram_counts(&h, Some(4)).is_err());
    // Parameters: out of range, or an unknown must-understand tag.
    for bad in [
        QuantileParams {
            mantissa_bits: 0,
            ..p
        },
        QuantileParams {
            mantissa_bits: 13,
            ..p
        },
        QuantileParams {
            max_buckets: 15,
            ..p
        },
    ] {
        assert!(QuantileParams::parse(&bad.encode()).is_err(), "{bad:?}");
    }
    let unknown = pigeonhole_format::kind_params::encode(&[(9, &[1])]);
    assert!(QuantileParams::parse(&unknown).is_err());
    assert_eq!(QuantileParams::parse(&[]).unwrap(), p, "defaults");
}
