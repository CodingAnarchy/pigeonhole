//! A log-linear latency histogram in the style of HdrHistogram.

use std::time::Duration;

/// Sub-buckets per power of two: values are kept to within 1/128 (< 0.8%).
const SUB_BITS: u32 = 7;
const SUB: u64 = 1 << SUB_BITS;
/// Values below `SUB` get one bucket each; then 128 buckets per power of two up to 2^64.
const BUCKETS: usize = (SUB + (64 - SUB_BITS as u64) * SUB) as usize;

/// Latency histogram over nanoseconds with < 0.8% relative error, like an HdrHistogram
/// with two significant digits. Recording is allocation-free; histograms from several
/// client threads merge with [`Histogram::merge`].
///
/// ```
/// use pigeonhole_bench::Histogram;
/// use std::time::Duration;
///
/// let mut h = Histogram::new();
/// for us in 1..=100 {
///     h.record(Duration::from_micros(us));
/// }
/// let p50 = h.percentile(0.50).as_micros();
/// assert!((49..=51).contains(&p50));
/// assert_eq!(h.count(), 100);
/// ```
#[derive(Clone)]
pub struct Histogram {
    counts: Box<[u64]>,
    count: u64,
    sum_ns: u128,
    min_ns: u64,
    max_ns: u64,
}

impl std::fmt::Debug for Histogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Histogram")
            .field("count", &self.count)
            .field("min_ns", &self.min_ns)
            .field("max_ns", &self.max_ns)
            .finish_non_exhaustive()
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

fn index(v: u64) -> usize {
    if v < SUB {
        return v as usize;
    }
    let e = 63 - v.leading_zeros(); // >= SUB_BITS
    let sub = (v >> (e - SUB_BITS)) & (SUB - 1);
    (SUB + u64::from(e - SUB_BITS) * SUB + sub) as usize
}

/// The highest value that maps to bucket `i`.
fn highest(i: usize) -> u64 {
    let i = i as u64;
    if i < SUB {
        return i;
    }
    let e = (i - SUB) / SUB + u64::from(SUB_BITS);
    let sub = (i - SUB) % SUB;
    let shift = e - u64::from(SUB_BITS);
    let low = (SUB + sub) << shift;
    low + ((1u64 << shift) - 1)
}

impl Histogram {
    /// An empty histogram.
    pub fn new() -> Self {
        Self {
            counts: vec![0; BUCKETS].into_boxed_slice(),
            count: 0,
            sum_ns: 0,
            min_ns: u64::MAX,
            max_ns: 0,
        }
    }

    /// Records one latency.
    pub fn record(&mut self, latency: Duration) {
        let ns = u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX);
        self.record_ns(ns);
    }

    /// Records one latency in nanoseconds.
    pub fn record_ns(&mut self, ns: u64) {
        self.counts[index(ns)] += 1;
        self.count += 1;
        self.sum_ns += u128::from(ns);
        self.min_ns = self.min_ns.min(ns);
        self.max_ns = self.max_ns.max(ns);
    }

    /// Adds every sample of `other`.
    pub fn merge(&mut self, other: &Histogram) {
        for (a, b) in self.counts.iter_mut().zip(other.counts.iter()) {
            *a += b;
        }
        self.count += other.count;
        self.sum_ns += other.sum_ns;
        self.min_ns = self.min_ns.min(other.min_ns);
        self.max_ns = self.max_ns.max(other.max_ns);
    }

    /// Samples recorded.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// The latency at quantile `q` in `[0, 1]` (0.99 for p99): the highest value
    /// equivalent to the sample at that rank. Zero when empty.
    pub fn percentile(&self, q: f64) -> Duration {
        if self.count == 0 {
            return Duration::ZERO;
        }
        let rank = ((q.clamp(0.0, 1.0) * self.count as f64).ceil() as u64).max(1);
        let mut seen = 0;
        for (i, &c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= rank {
                return Duration::from_nanos(highest(i).clamp(self.min_ns, self.max_ns));
            }
        }
        Duration::from_nanos(self.max_ns)
    }

    /// Mean latency. Zero when empty.
    pub fn mean(&self) -> Duration {
        if self.count == 0 {
            return Duration::ZERO;
        }
        Duration::from_nanos((self.sum_ns / u128::from(self.count)) as u64)
    }

    /// Largest latency recorded. Zero when empty.
    pub fn max(&self) -> Duration {
        Duration::from_nanos(self.max_ns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn buckets_are_contiguous() {
        let mut prev = None;
        for v in (0..100_000u64).chain([u64::MAX / 3, u64::MAX - 1, u64::MAX]) {
            let i = index(v);
            assert!(i < BUCKETS);
            assert!(highest(i) >= v);
            if let Some(p) = prev {
                assert!(i >= p);
            }
            prev = Some(i);
        }
        assert_eq!(index(u64::MAX), BUCKETS - 1);
    }

    #[test]
    fn empty() {
        let h = Histogram::new();
        assert_eq!(h.percentile(0.99), Duration::ZERO);
        assert_eq!(h.mean(), Duration::ZERO);
    }

    proptest! {
        #[test]
        fn percentiles_within_one_percent(
            mut samples in proptest::collection::vec(1u64..10_000_000_000, 1..2000),
            q in 0.0f64..=1.0,
        ) {
            let mut h = Histogram::new();
            for &s in &samples {
                h.record_ns(s);
            }
            samples.sort_unstable();
            let rank = ((q * samples.len() as f64).ceil() as usize).max(1);
            let exact = samples[rank - 1] as f64;
            let got = h.percentile(q).as_nanos() as f64;
            prop_assert!(got >= exact && got <= exact * 1.008 + 1.0, "exact {exact} got {got}");
        }

        #[test]
        fn merge_equals_single(a in proptest::collection::vec(0u64..1_000_000, 0..500),
                               b in proptest::collection::vec(0u64..1_000_000, 0..500)) {
            let (mut ha, mut hb, mut all) = (Histogram::new(), Histogram::new(), Histogram::new());
            for &v in &a { ha.record_ns(v); all.record_ns(v); }
            for &v in &b { hb.record_ns(v); all.record_ns(v); }
            ha.merge(&hb);
            for q in [0.5, 0.99, 0.999] {
                prop_assert_eq!(ha.percentile(q), all.percentile(q));
            }
            prop_assert_eq!(ha.count(), all.count());
        }
    }
}
