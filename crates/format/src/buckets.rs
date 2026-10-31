//! Fixed-bucket histograms and log-linear quantile sketches, the values of `Histogram` and
//! `Quantiles` aggregate families (decision D298; FORMAT §3.4, §3.5), and their parameters
//! (`kind_params`, FORMAT §9.2.1).
//!
//! **Histogram.** A family fixes `n` bounds `b0 < b1 < … < b(n-1)` (finite, `1 ≤ n ≤ 1023`,
//! never `-0.0`), so `n + 1` buckets: `(-∞, b0)`, `[b0, b1)`, …, `[b(n-1), +∞)`. An input's
//! bucket is the number of bounds at or below it by `f64::total_cmp` (so `-0.0` lies below a
//! bound of `0.0`); infinities fall in the end buckets and NaN is refused. A state is the
//! bucket count and the per-bucket counts; merging adds them.
//!
//! **Quantiles.** A positive finite `x` maps to the index `bits(x) >> (52 - p)`: its binary
//! exponent and top `p` mantissa bits, so a bucket's values lie within a factor of
//! `1 + 2^-p` of each other (log-linear, as HdrHistogram's and DDSketch's linear mapping),
//! and no logarithm is computed. Negative inputs map their magnitude into a second store;
//! zeros (either sign) are counted apart. Past `max_buckets`, each store collapses (D298
//! revision 1's S3): with `t = max_index − max_buckets + 1`, every bucket below `t` folds
//! into bucket `t`. `t` depends only on the store's largest index, which only grows, so
//! collapsing after every merge gives what one collapse at the end would: merges are
//! associative and commutative. A read walks the counts to rank `⌊q·(n−1)⌋` and returns the
//! bucket's midpoint, built from its bits (exact).
//!
//! | State | Bytes |
//! |---|---|
//! | Histogram | `4` (encoding), the bucket count `n + 1` (varint), the counts (varints, trailing zeros dropped, so the last is nonzero) |
//! | Quantiles | `5` (encoding), `p` (u8), the zero count (varint), the positive store, the negative store |
//!
//! A store is its bucket count `k` (varint), then `k` pairs: the first bucket's index and
//! count, then each later bucket's index gap from the one before (at least 1) and count (at
//! least 1), all varints. A state holds at least one input.
//!
//! ```
//! use pigeonhole_format::buckets::{self, QuantileParams};
//!
//! let p = QuantileParams::default();
//! let mut s = Vec::new();
//! for x in 1..=1000 {
//!     buckets::quantiles_add(&mut s, p, f64::from(x)).unwrap();
//! }
//! let median = buckets::quantile(&s, 0.5).unwrap();
//! assert!((median - 500.0).abs() / 500.0 < 1.0 / 128.0, "{median}");
//! ```

use std::fmt;

use crate::kind_params;

/// The encoding byte of a histogram state.
pub const HISTOGRAM_ENCODING: u8 = 4;
/// The encoding byte of a quantile sketch.
pub const QUANTILES_ENCODING: u8 = 5;
/// The most bounds a histogram takes.
pub const MAX_BOUNDS: usize = 1023;
/// `kind_params` tag of a histogram's bounds (`n` `f64` LE).
pub const TAG_BOUNDS: u8 = 1;
/// `kind_params` tag of a quantile sketch's mantissa bits (u8).
pub const TAG_MANTISSA_BITS: u8 = 1;
/// `kind_params` tag of a quantile sketch's bucket limit per store (u16 LE).
pub const TAG_MAX_BUCKETS: u8 = 2;

/// A malformed state or parameter list, or an input these kinds refuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketError(pub &'static str);

impl fmt::Display for BucketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for BucketError {}

/// The result type of this module.
pub type Result<T> = std::result::Result<T, BucketError>;

fn get_varint(s: &[u8], at: &mut usize) -> Result<u64> {
    let (v, used) = crate::varint::get_u64(&s[(*at).min(s.len())..])
        .map_err(|_| BucketError("malformed varint"))?;
    *at += used;
    Ok(v)
}

/// A histogram's bounds, from its `kind_params`: tag 1, required.
pub fn histogram_bounds(params: &[u8]) -> Result<Vec<f64>> {
    let entries = kind_params::parse(params).map_err(|_| BucketError("malformed parameters"))?;
    let mut bounds = None;
    for (tag, v) in entries {
        match tag {
            TAG_BOUNDS => bounds = Some(v),
            t if kind_params::must_understand(t) => {
                return Err(BucketError("an unknown histogram parameter"));
            }
            _ => {}
        }
    }
    let v = bounds.ok_or(BucketError("a histogram needs its bounds (tag 1)"))?;
    if v.is_empty() || v.len() % 8 != 0 || v.len() / 8 > MAX_BOUNDS {
        return Err(BucketError("a histogram takes 1 to 1023 bounds"));
    }
    let b: Vec<f64> = v
        .chunks(8)
        .map(|c| f64::from_le_bytes(c.try_into().expect("8 bytes")))
        .collect();
    let ok = b
        .iter()
        .all(|x| x.is_finite() && x.to_bits() != (-0.0f64).to_bits())
        && b.windows(2).all(|w| w[0] < w[1]);
    if !ok {
        return Err(BucketError(
            "histogram bounds must be finite, strictly increasing, and not -0.0",
        ));
    }
    Ok(b)
}

/// The `kind_params` of a histogram with `bounds` (not checked here).
pub fn histogram_params(bounds: &[f64]) -> Vec<u8> {
    let bytes: Vec<u8> = bounds.iter().flat_map(|b| b.to_le_bytes()).collect();
    kind_params::encode(&[(TAG_BOUNDS, &bytes)])
}

/// The bucket of `x` among `bounds`: how many bounds lie at or below it (`total_cmp`).
pub fn bucket_of(bounds: &[f64], x: f64) -> usize {
    bounds.partition_point(|b| b.total_cmp(&x).is_le())
}

/// The counts of a histogram state, one per bucket (trailing zeros included), and validates
/// it: `buckets` buckets if given, canonical, at least one input.
pub fn histogram_counts(s: &[u8], buckets: Option<usize>) -> Result<Vec<u64>> {
    if s.first() != Some(&HISTOGRAM_ENCODING) {
        return Err(BucketError("not a histogram state"));
    }
    let mut at = 1;
    let n = get_varint(s, &mut at)? as usize;
    if !(2..=MAX_BOUNDS + 1).contains(&n) || buckets.is_some_and(|b| b != n) {
        return Err(BucketError("a histogram state of the wrong bucket count"));
    }
    let mut counts = Vec::with_capacity(n);
    while at < s.len() {
        counts.push(get_varint(s, &mut at)?);
    }
    if counts.len() > n || counts.last().is_none_or(|&c| c == 0) {
        return Err(BucketError("histogram state not in canonical form"));
    }
    counts.resize(n, 0);
    Ok(counts)
}

fn encode_histogram(out: &mut Vec<u8>, counts: &[u64]) {
    out.clear();
    out.push(HISTOGRAM_ENCODING);
    crate::varint::put_u64(out, counts.len() as u64);
    let end = counts.iter().rposition(|&c| c != 0).map_or(0, |i| i + 1);
    for &c in &counts[..end] {
        crate::varint::put_u64(out, c);
    }
}

/// Starts or extends `acc` (a histogram state, or empty for none yet) with the input `x`.
pub fn histogram_add(acc: &mut Vec<u8>, bounds: &[f64], x: f64) -> Result<()> {
    if x.is_nan() {
        return Err(BucketError("a histogram refuses NaN"));
    }
    let mut counts = if acc.is_empty() {
        vec![0; bounds.len() + 1]
    } else {
        histogram_counts(acc, Some(bounds.len() + 1))?
    };
    let b = bucket_of(bounds, x);
    counts[b] = counts[b].wrapping_add(1);
    encode_histogram(acc, &counts);
    Ok(())
}

/// Merges the histogram state `other` into `acc` (`acc` may be empty for none).
pub fn histogram_merge(acc: &mut Vec<u8>, bounds: &[f64], other: &[u8]) -> Result<()> {
    let n = bounds.len() + 1;
    let mut counts = if acc.is_empty() {
        vec![0; n]
    } else {
        histogram_counts(acc, Some(n))?
    };
    for (c, o) in counts.iter_mut().zip(histogram_counts(other, Some(n))?) {
        *c = c.wrapping_add(o);
    }
    encode_histogram(acc, &counts);
    Ok(())
}

/// A quantile sketch's parameters: `p` mantissa bits (1 to 12, so a relative error of at
/// most 2^-p; 7 by default) and a bucket limit per store (16 to 4096, 2048 by default).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QuantileParams {
    /// Mantissa bits kept in an index.
    pub mantissa_bits: u8,
    /// Buckets each store keeps before it collapses.
    pub max_buckets: u16,
}

impl Default for QuantileParams {
    fn default() -> Self {
        QuantileParams {
            mantissa_bits: 7,
            max_buckets: 2048,
        }
    }
}

impl QuantileParams {
    /// From `kind_params` (both tags optional, defaulted).
    pub fn parse(params: &[u8]) -> Result<Self> {
        let mut out = QuantileParams::default();
        for (tag, v) in
            kind_params::parse(params).map_err(|_| BucketError("malformed parameters"))?
        {
            match tag {
                TAG_MANTISSA_BITS => match v {
                    [p] => out.mantissa_bits = *p,
                    _ => return Err(BucketError("mantissa bits are one byte")),
                },
                TAG_MAX_BUCKETS => match v {
                    [a, b] => out.max_buckets = u16::from_le_bytes([*a, *b]),
                    _ => return Err(BucketError("max buckets is a u16")),
                },
                t if kind_params::must_understand(t) => {
                    return Err(BucketError("an unknown quantile parameter"));
                }
                _ => {}
            }
        }
        out.check()?;
        Ok(out)
    }

    /// Checks the ranges.
    pub fn check(&self) -> Result<()> {
        if !(1..=12).contains(&self.mantissa_bits) || !(16..=4096).contains(&self.max_buckets) {
            return Err(BucketError(
                "quantiles take 1 to 12 mantissa bits and 16 to 4096 buckets",
            ));
        }
        Ok(())
    }

    /// The `kind_params` holding both parameters.
    pub fn encode(&self) -> Vec<u8> {
        kind_params::encode(&[
            (TAG_MANTISSA_BITS, &[self.mantissa_bits]),
            (TAG_MAX_BUCKETS, &self.max_buckets.to_le_bytes()),
        ])
    }
}

/// A store: `(index, count)`, indexes strictly increasing, counts at least 1.
type Store = Vec<(u64, u64)>;

struct Sketch {
    p: u8,
    zero: u64,
    pos: Store,
    neg: Store,
}

fn decode_store(s: &[u8], at: &mut usize) -> Result<Store> {
    let k = get_varint(s, at)?;
    if k > (s.len() as u64) {
        return Err(BucketError("malformed store"));
    }
    let mut out = Vec::with_capacity(k as usize);
    let mut index = 0u64;
    for i in 0..k {
        let d = get_varint(s, at)?;
        if i > 0 && d == 0 {
            return Err(BucketError("store indexes not increasing"));
        }
        index = index.checked_add(d).ok_or(BucketError("malformed store"))?;
        let c = get_varint(s, at)?;
        if c == 0 {
            return Err(BucketError("an empty bucket in a store"));
        }
        out.push((index, c));
    }
    Ok(out)
}

fn encode_store(out: &mut Vec<u8>, store: &Store) {
    crate::varint::put_u64(out, store.len() as u64);
    let mut prev = 0;
    for &(i, c) in store {
        crate::varint::put_u64(out, i - prev);
        crate::varint::put_u64(out, c);
        prev = i;
    }
}

impl Sketch {
    fn decode(s: &[u8], params: Option<QuantileParams>) -> Result<Self> {
        if s.first() != Some(&QUANTILES_ENCODING) || s.len() < 2 {
            return Err(BucketError("not a quantile sketch"));
        }
        let p = s[1];
        if !(1..=12).contains(&p) || params.is_some_and(|q| q.mantissa_bits != p) {
            return Err(BucketError("a quantile sketch of the wrong mantissa bits"));
        }
        let mut at = 2;
        let zero = get_varint(s, &mut at)?;
        let pos = decode_store(s, &mut at)?;
        let neg = decode_store(s, &mut at)?;
        if at != s.len() {
            return Err(BucketError("bytes after a quantile sketch"));
        }
        let sk = Sketch { p, zero, pos, neg };
        if sk.count() == 0 {
            return Err(BucketError("a quantile sketch of no inputs"));
        }
        let limit = 1u64 << (11 + u32::from(p)); // indexes below 2^(11 + p)
        if sk.pos.iter().chain(&sk.neg).any(|&(i, _)| i >= limit) {
            return Err(BucketError("a quantile index past the f64 range"));
        }
        if let Some(q) = params
            && (!collapsed(&sk.pos, q.max_buckets) || !collapsed(&sk.neg, q.max_buckets))
        {
            return Err(BucketError("a quantile store not collapsed"));
        }
        Ok(sk)
    }

    fn count(&self) -> u64 {
        let sum = |s: &Store| s.iter().map(|&(_, c)| c).fold(0u64, u64::wrapping_add);
        self.zero
            .wrapping_add(sum(&self.pos))
            .wrapping_add(sum(&self.neg))
    }

    fn encode(&self, out: &mut Vec<u8>) {
        out.clear();
        out.push(QUANTILES_ENCODING);
        out.push(self.p);
        crate::varint::put_u64(out, self.zero);
        encode_store(out, &self.pos);
        encode_store(out, &self.neg);
    }
}

/// Whether no index lies below the store's threshold.
fn collapsed(store: &Store, max: u16) -> bool {
    let Some(&(top, _)) = store.last() else {
        return true;
    };
    let t = top.saturating_sub(u64::from(max) - 1);
    store.first().is_some_and(|&(i, _)| i >= t)
}

/// Folds every bucket below the threshold into it (S3).
fn collapse(store: &mut Store, max: u16) {
    if collapsed(store, max) {
        return;
    }
    let top = store.last().expect("not empty").0;
    let t = top - (u64::from(max) - 1);
    let below: u64 = store
        .iter()
        .take_while(|&&(i, _)| i < t)
        .map(|&(_, c)| c)
        .fold(0, u64::wrapping_add);
    store.retain(|&(i, _)| i >= t);
    match store.first_mut() {
        Some((i, c)) if *i == t => *c = c.wrapping_add(below),
        _ => store.insert(0, (t, below)),
    }
}

fn merge_stores(a: &Store, b: &Store) -> Store {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() || j < b.len() {
        match (a.get(i), b.get(j)) {
            (Some(&(x, c)), Some(&(y, d))) if x == y => {
                out.push((x, c.wrapping_add(d)));
                i += 1;
                j += 1;
            }
            (Some(&(x, c)), Some(&(y, _))) if x < y => {
                out.push((x, c));
                i += 1;
            }
            (Some(&(x, c)), None) => {
                out.push((x, c));
                i += 1;
            }
            (_, Some(&(y, d))) => {
                out.push((y, d));
                j += 1;
            }
            (None, None) => unreachable!(),
        }
    }
    out
}

/// The index of a positive finite `x` with `p` mantissa bits.
fn index_of(x: f64, p: u8) -> u64 {
    x.to_bits() >> (52 - p)
}

/// The midpoint of bucket `i`: its lower edge's bits with the next bit below the kept ones
/// set, which is the bucket's midpoint since values are linear within a binade.
fn midpoint(i: u64, p: u8) -> f64 {
    f64::from_bits((i << (52 - p)) | (1 << (51 - p)))
}

/// Starts or extends `acc` (a sketch, or empty for none yet) with the finite input `x`.
pub fn quantiles_add(acc: &mut Vec<u8>, params: QuantileParams, x: f64) -> Result<()> {
    if !x.is_finite() {
        return Err(BucketError("a quantile sketch takes finite f64s only"));
    }
    let mut sk = if acc.is_empty() {
        Sketch {
            p: params.mantissa_bits,
            zero: 0,
            pos: Vec::new(),
            neg: Vec::new(),
        }
    } else {
        Sketch::decode(acc, Some(params))?
    };
    if x == 0.0 {
        sk.zero = sk.zero.wrapping_add(1);
    } else {
        let store = if x > 0.0 { &mut sk.pos } else { &mut sk.neg };
        let i = index_of(x.abs(), sk.p);
        match store.binary_search_by_key(&i, |&(k, _)| k) {
            Ok(at) => store[at].1 = store[at].1.wrapping_add(1),
            Err(at) => store.insert(at, (i, 1)),
        }
        collapse(store, params.max_buckets);
    }
    sk.encode(acc);
    Ok(())
}

/// Merges the sketch `other` into `acc` (`acc` may be empty for none).
pub fn quantiles_merge(acc: &mut Vec<u8>, params: QuantileParams, other: &[u8]) -> Result<()> {
    let b = Sketch::decode(other, Some(params))?;
    if acc.is_empty() {
        b.encode(acc);
        return Ok(());
    }
    let a = Sketch::decode(acc, Some(params))?;
    let mut pos = merge_stores(&a.pos, &b.pos);
    let mut neg = merge_stores(&a.neg, &b.neg);
    collapse(&mut pos, params.max_buckets);
    collapse(&mut neg, params.max_buckets);
    Sketch {
        p: a.p,
        zero: a.zero.wrapping_add(b.zero),
        pos,
        neg,
    }
    .encode(acc);
    Ok(())
}

/// Checks a sketch against a family's parameters.
pub fn quantiles_validate(s: &[u8], params: QuantileParams) -> Result<()> {
    Sketch::decode(s, Some(params)).map(|_| ())
}

/// The number of inputs a sketch holds.
pub fn quantiles_count(s: &[u8]) -> Result<u64> {
    Sketch::decode(s, None).map(|sk| sk.count())
}

/// The `q`-quantile (`0 ≤ q ≤ 1`) of a sketch: the midpoint of the bucket holding the input
/// of rank `⌊q·(n−1)⌋` (0-based, in increasing order), within a relative error of 2^-p of
/// it while its store has not collapsed; `0.0` for a zero. A collapse folds the smallest
/// magnitudes into the threshold bucket, so inputs that small (the store's lowest buckets,
/// near zero) read as that bucket's midpoint. `None` if `q` is outside `[0, 1]`.
pub fn quantile(s: &[u8], q: f64) -> Option<f64> {
    if !(0.0..=1.0).contains(&q) {
        return None;
    }
    let sk = Sketch::decode(s, None).ok()?;
    let n = sk.count();
    let mut rank = ((q * (n - 1) as f64).floor() as u64).min(n - 1);
    for &(i, c) in sk.neg.iter().rev() {
        if rank < c {
            return Some(-midpoint(i, sk.p));
        }
        rank -= c;
    }
    if rank < sk.zero {
        return Some(0.0);
    }
    rank -= sk.zero;
    for &(i, c) in &sk.pos {
        if rank < c {
            return Some(midpoint(i, sk.p));
        }
        rank -= c;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexes_and_midpoints() {
        // 1.0 and its bucket: [1, 1 + 2^-p), midpoint 1 + 2^-(p+1).
        let p = 7;
        let i = index_of(1.0, p);
        assert_eq!(midpoint(i, p), 1.0 + 2f64.powi(-8));
        // Monotone: a larger value never maps lower.
        let mut last = 0;
        for x in [5e-324, 1e-300, 0.5, 1.0, 1.5, 2.0, 1e300, f64::MAX] {
            let j = index_of(x, p);
            assert!(j >= last, "{x}");
            last = j;
        }
        assert!(index_of(f64::MAX, 12) < 1 << 23);
    }

    #[test]
    fn collapse_folds_into_the_threshold() {
        let mut s: Store = vec![(1, 1), (5, 2), (30, 1), (40, 3)];
        collapse(&mut s, 16);
        // t = 40 - 15 = 25: 1 and 5 fold into a new bucket 25.
        assert_eq!(s, vec![(25, 3), (30, 1), (40, 3)]);
        collapse(&mut s, 16);
        assert_eq!(s, vec![(25, 3), (30, 1), (40, 3)], "idempotent");
    }

    #[test]
    fn bounds_and_buckets() {
        let b = histogram_bounds(&histogram_params(&[0.0, 10.0, 100.0])).unwrap();
        assert_eq!(bucket_of(&b, -0.0), 0); // -0.0 lies below a bound of 0.0
        assert_eq!(bucket_of(&b, 0.0), 1);
        assert_eq!(bucket_of(&b, 9.99), 1);
        assert_eq!(bucket_of(&b, 10.0), 2);
        assert_eq!(bucket_of(&b, f64::INFINITY), 3);
        assert_eq!(bucket_of(&b, f64::NEG_INFINITY), 0);
        for bad in [&[][..], &[1.0, 1.0], &[2.0, 1.0], &[-0.0], &[f64::NAN]] {
            assert!(histogram_bounds(&histogram_params(bad)).is_err(), "{bad:?}");
        }
        assert!(histogram_bounds(&[]).is_err(), "bounds are required");
    }
}
