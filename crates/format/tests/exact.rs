//! D298 (FORMAT §3.2, §3.3): exact `f64` sums and mean/variance states, against references
//! written apart from the codec. Sums are checked against Shewchuk's exactly rounded
//! summation (the algorithm of Python's `math.fsum`); mean and variance over small integer
//! inputs, whose exact values are ratios of integers that `f64` division rounds correctly.
//! Merges are checked to give the same bytes under any grouping. Seeded; the seed prints on
//! failure.

use pigeonhole_format::exact;

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

    /// A finite `f64`: random bits (any exponent, subnormals included), or an adversarial
    /// value (±max, the smallest subnormal, signed zeros, powers of two).
    fn finite(&mut self) -> f64 {
        loop {
            let x = match self.below(8) {
                0 => f64::MAX,
                1 => -f64::MAX,
                2 => 5e-324,
                3 => -0.0,
                4 => f64::from_bits(self.next() & 0x000f_ffff_ffff_ffff), // subnormal
                5 => (2.0f64).powi(self.below(2000) as i32 - 1000),
                _ => f64::from_bits(self.next()),
            };
            if x.is_finite() {
                return if self.next() & 1 == 1 { -x } else { x };
            }
        }
    }

    /// A finite `f64` below 2^600 in magnitude (so sums of a few stay in range): a random
    /// significand at a random binary exponent down to the subnormals, with either sign.
    fn moderate(&mut self) -> f64 {
        let m = (self.next() >> 11) as f64; // 53 random bits
        let x = m * 2f64.powi(self.below(1700) as i32 - 1127);
        if self.next() & 1 == 1 { -x } else { x }
    }
}

/// Shewchuk's exactly rounded sum (Python's `math.fsum`), or `None` when an intermediate
/// overflows (the codec reads ±∞ there; the reference cannot say which).
fn fsum(xs: &[f64]) -> Option<f64> {
    let mut partials: Vec<f64> = Vec::new();
    for &x0 in xs {
        let mut x = x0;
        let mut i = 0;
        for j in 0..partials.len() {
            let mut y = partials[j];
            if x.abs() < y.abs() {
                std::mem::swap(&mut x, &mut y);
            }
            let hi = x + y;
            if !hi.is_finite() {
                return None;
            }
            let lo = y - (hi - x);
            if lo != 0.0 {
                partials[i] = lo;
                i += 1;
            }
            x = hi;
        }
        partials.truncate(i);
        partials.push(x);
    }
    let mut n = partials.len();
    let mut hi = 0.0;
    if n > 0 {
        n -= 1;
        hi = partials[n];
        let mut lo = 0.0;
        while n > 0 {
            let x = hi;
            n -= 1;
            let y = partials[n];
            hi = x + y;
            let yr = hi - x;
            lo = y - yr;
            if lo != 0.0 {
                break;
            }
        }
        // Half-way cases: correct the rounding by the next partial's sign.
        if n > 0 && ((lo < 0.0 && partials[n - 1] < 0.0) || (lo > 0.0 && partials[n - 1] > 0.0)) {
            let y = lo * 2.0;
            let x = hi + y;
            let yr = x - hi;
            if y == yr {
                hi = x;
            }
        }
    }
    Some(hi + 0.0)
}

fn sum_of(xs: &[f64]) -> Vec<u8> {
    let mut acc = Vec::new();
    for &x in xs {
        exact::sum_add(&mut acc, x).unwrap();
    }
    acc
}

#[test]
fn sums_round_exactly_like_fsum() {
    let mut compared = 0;
    for seed in 1..=400u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let n = 1 + rng.below(40) as usize;
        // Odd seeds: any finite values (overflow is frequent); even seeds: values that mix
        // scales and cancel but stay in range.
        let xs: Vec<f64> = (0..n)
            .map(|_| {
                if seed % 2 == 1 {
                    rng.finite()
                } else {
                    rng.moderate()
                }
            })
            .collect();
        let Some(want) = fsum(&xs) else {
            continue;
        };
        let got = exact::sum_read(&sum_of(&xs)).unwrap();
        assert_eq!(got.to_bits(), want.to_bits(), "seed {seed}: {xs:?}");
        compared += 1;
    }
    // Most seeds stay in range for the reference.
    assert!(compared >= 200, "only {compared} of 400 seeds compared");
}

#[test]
fn cancellation_and_ties() {
    // Floating-point addition loses the small terms; the exact sum keeps them.
    let xs = [1e308, 1.0, -1e308, 1e-300, 2.0, -1e-300];
    assert_eq!(exact::sum_read(&sum_of(&xs)).unwrap(), 3.0);
    // Inputs that cancel exactly read +0.0, whatever their signs.
    assert_eq!(
        exact::sum_read(&sum_of(&[-0.0, -0.0])).unwrap().to_bits(),
        0
    );
    assert_eq!(exact::sum_read(&sum_of(&[5.5, -5.5])).unwrap().to_bits(), 0);
    // The smallest subnormal, many times: exact.
    let tiny = vec![5e-324; 1000];
    assert_eq!(exact::sum_read(&sum_of(&tiny)).unwrap(), 5e-324 * 1000.0);
    // Overflow reads as infinity, of the sum's sign.
    assert_eq!(
        exact::sum_read(&sum_of(&[f64::MAX; 3])).unwrap(),
        f64::INFINITY
    );
    assert_eq!(
        exact::sum_read(&sum_of(&[-f64::MAX; 3])).unwrap(),
        f64::NEG_INFINITY
    );
    // NaN and infinities are refused, and leave the state alone.
    let mut acc = sum_of(&[1.0]);
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(exact::sum_add(&mut acc, bad).is_err());
    }
    assert_eq!(acc, sum_of(&[1.0]));
}

/// The bytes of folding `xs` in a random grouping: random runs summed apart, then merged in
/// a random order.
fn grouped<F: Fn(&mut Vec<u8>, f64), M: Fn(&mut Vec<u8>, &[u8])>(
    xs: &[f64],
    rng: &mut Rng,
    add: F,
    merge: M,
) -> Vec<u8> {
    let mut parts: Vec<Vec<u8>> = Vec::new();
    let mut i = 0;
    while i < xs.len() {
        let len = 1 + rng.below(4) as usize;
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
fn any_grouping_gives_the_same_bytes() {
    for seed in 1..=200u64 {
        let mut rng = Rng(seed.wrapping_mul(0x2545_f491_4f6c_dd1d) | 1);
        let xs: Vec<f64> = (0..1 + rng.below(30)).map(|_| rng.finite()).collect();
        let one = sum_of(&xs);
        let other = grouped(
            &xs,
            &mut rng,
            |a, x| exact::sum_add(a, x).unwrap(),
            |a, b| exact::sum_merge(a, b).unwrap(),
        );
        assert_eq!(one, other, "seed {seed}");
        let mut mv = Vec::new();
        for &x in &xs {
            exact::mean_var_add(&mut mv, x).unwrap();
        }
        let mv2 = grouped(
            &xs,
            &mut rng,
            |a, x| exact::mean_var_add(a, x).unwrap(),
            |a, b| exact::mean_var_merge(a, b).unwrap(),
        );
        assert_eq!(mv, mv2, "seed {seed}");
        exact::sum_validate(&one).unwrap();
        exact::mean_var_validate(&mv).unwrap();
        assert!(one.len() <= exact::SUM_MAX_LEN && mv.len() <= exact::MEAN_VAR_MAX_LEN);
    }
}

#[test]
fn mean_and_variance_of_integers_round_correctly() {
    for seed in 1..=300u64 {
        let mut rng = Rng(seed | 1);
        let n = 1 + rng.below(50);
        let xs: Vec<i64> = (0..n)
            .map(|_| rng.below(2_000_001) as i64 - 1_000_000)
            .collect();
        let mut mv = Vec::new();
        for &x in &xs {
            exact::mean_var_add(&mut mv, x as f64).unwrap();
        }
        let r = exact::mean_var_read(&mv).unwrap();
        let s: i128 = xs.iter().map(|&x| i128::from(x)).sum();
        let q: i128 = xs.iter().map(|&x| i128::from(x) * i128::from(x)).sum();
        let n = i128::from(n);
        let d = n * q - s * s;
        // Each ratio's numerator and denominator are exact in f64 (below 2^53 here), so one
        // IEEE division rounds it correctly.
        assert!(
            s.abs() < 1 << 53 && d < 1 << 53 && n * n < 1 << 53,
            "seed {seed}"
        );
        assert_eq!(r.count, n as u64);
        assert_eq!(r.mean, s as f64 / n as f64, "seed {seed}: mean");
        assert_eq!(
            r.variance,
            d as f64 / (n * n) as f64,
            "seed {seed}: variance"
        );
        let sample = (n >= 2).then(|| d as f64 / (n * (n - 1)) as f64);
        assert_eq!(r.sample_variance, sample, "seed {seed}: sample variance");
    }
}

#[test]
fn mean_and_variance_at_the_extremes() {
    // Squares past the f64 range stay exact: the variance of ±max is max², an infinity.
    let mut mv = Vec::new();
    for x in [f64::MAX, -f64::MAX] {
        exact::mean_var_add(&mut mv, x).unwrap();
    }
    let r = exact::mean_var_read(&mv).unwrap();
    assert_eq!((r.mean, r.variance), (0.0, f64::INFINITY));
    // Subnormal inputs: the variance is far below the f64 range, and reads 0.
    let mut mv = Vec::new();
    for x in [5e-324, 1e-323] {
        exact::mean_var_add(&mut mv, x).unwrap();
    }
    let r = exact::mean_var_read(&mv).unwrap();
    // The mean, 1.5 units of 2^-1074, rounds to even: 2 units.
    assert_eq!((r.mean, r.variance), (1e-323, 0.0));
    // One input: mean x, variance 0, no sample variance.
    let mut mv = Vec::new();
    exact::mean_var_add(&mut mv, -3.5).unwrap();
    let r = exact::mean_var_read(&mv).unwrap();
    assert_eq!(
        (r.count, r.mean, r.variance, r.sample_variance),
        (1, -3.5, 0.0, None)
    );
}
