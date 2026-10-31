//! The model's oracle for D298's histogram and quantile kinds (FORMAT §3.4–§3.5), written from
//! the encodings' description rather than with the store's code
//! (`pigeonhole_format::buckets`): counts kept in plain vectors and a `BTreeMap`, collapse
//! applied once at the end over every input (which S3 says equals collapsing after each
//! merge), and its own encoder and decoder.

use std::collections::BTreeMap;

/// The model's histogram families' bounds (`fhist*`): buckets below -100, then up to -1, 0, 1,
/// 100 and above.
pub const MODEL_HISTOGRAM_BOUNDS: [f64; 5] = [-100.0, -1.0, 0.0, 1.0, 100.0];
/// The model's quantile families' mantissa bits (`fq*`): coarse, so buckets merge often.
pub const MODEL_QUANTILE_BITS: u8 = 3;
/// The model's quantile families' bucket limit: the smallest allowed, so the workload's
/// magnitudes (1e-300 to 1e300) collapse.
pub const MODEL_QUANTILE_MAX: u16 = 16;

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_varint(s: &[u8], at: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    for i in 0..10 {
        let b = *s.get(*at)?;
        *at += 1;
        v |= u64::from(b & 0x7f) << (7 * i);
        if b & 0x80 == 0 {
            return Some(v);
        }
    }
    None
}

/// The bucket of `x`: the number of bounds `b` with `b ≤ x` in the total order.
fn bucket(x: f64) -> usize {
    MODEL_HISTOGRAM_BOUNDS
        .iter()
        .filter(|b| b.total_cmp(&x).is_le())
        .count()
}

fn encode_histogram(counts: &[u64]) -> Vec<u8> {
    let mut out = vec![4];
    put_varint(&mut out, counts.len() as u64);
    let mut end = counts.len();
    while end > 0 && counts[end - 1] == 0 {
        end -= 1;
    }
    for &c in &counts[..end] {
        put_varint(&mut out, c);
    }
    out
}

fn decode_histogram(s: &[u8]) -> Option<Vec<u64>> {
    if s.first() != Some(&4) {
        return None;
    }
    let mut at = 1;
    let n = get_varint(s, &mut at)? as usize;
    if n != MODEL_HISTOGRAM_BOUNDS.len() + 1 {
        return None;
    }
    let mut counts = Vec::new();
    while at < s.len() {
        counts.push(get_varint(s, &mut at)?);
    }
    if counts.len() > n || counts.last().is_none_or(|&c| c == 0) {
        return None;
    }
    counts.resize(n, 0);
    (encode_histogram(&counts) == s).then_some(counts)
}

/// A quantile sketch as the model keeps it: the zero count, and each sign's buckets by
/// index (before collapse).
#[derive(Default)]
struct Sketch {
    zero: u64,
    pos: BTreeMap<u64, u64>,
    neg: BTreeMap<u64, u64>,
}

fn index(x: f64) -> u64 {
    x.abs().to_bits() >> (52 - MODEL_QUANTILE_BITS)
}

/// The store with every bucket below `max_index - max + 1` folded into that threshold.
fn collapsed(store: &BTreeMap<u64, u64>) -> BTreeMap<u64, u64> {
    let Some((&top, _)) = store.iter().next_back() else {
        return BTreeMap::new();
    };
    let t = top.saturating_sub(u64::from(MODEL_QUANTILE_MAX) - 1);
    let mut out = BTreeMap::new();
    for (&i, &c) in store {
        *out.entry(i.max(t)).or_insert(0) += c;
    }
    out
}

fn encode_store(out: &mut Vec<u8>, store: &BTreeMap<u64, u64>) {
    put_varint(out, store.len() as u64);
    let mut prev = 0;
    for (&i, &c) in store {
        put_varint(out, i - prev);
        put_varint(out, c);
        prev = i;
    }
}

fn encode_sketch(sk: &Sketch) -> Vec<u8> {
    let mut out = vec![5, MODEL_QUANTILE_BITS];
    put_varint(&mut out, sk.zero);
    encode_store(&mut out, &collapsed(&sk.pos));
    encode_store(&mut out, &collapsed(&sk.neg));
    out
}

fn decode_store(s: &[u8], at: &mut usize) -> Option<BTreeMap<u64, u64>> {
    let k = get_varint(s, at)?;
    let mut out = BTreeMap::new();
    let mut i = 0u64;
    for n in 0..k {
        let d = get_varint(s, at)?;
        if n > 0 && d == 0 {
            return None;
        }
        i = i.checked_add(d)?;
        let c = get_varint(s, at)?;
        if c == 0 {
            return None;
        }
        out.insert(i, c);
    }
    Some(out)
}

fn decode_sketch(s: &[u8]) -> Option<Sketch> {
    if s.len() < 2 || s[0] != 5 || s[1] != MODEL_QUANTILE_BITS {
        return None;
    }
    let mut at = 2;
    let zero = get_varint(s, &mut at)?;
    let pos = decode_store(s, &mut at)?;
    let neg = decode_store(s, &mut at)?;
    if at != s.len() {
        return None;
    }
    let sk = Sketch { zero, pos, neg };
    let total = sk.zero + sk.pos.values().sum::<u64>() + sk.neg.values().sum::<u64>();
    // Canonical only if it is already collapsed and re-encodes to itself.
    (total > 0 && encode_sketch(&sk) == s).then_some(sk)
}

/// The state of `base` (if any) with the inputs `inputs` (`f64` bits) folded in, for a
/// histogram (`quantiles` false) or quantile family of the model's parameters. `None` if
/// `base` is malformed, an input is refused (NaN; for quantiles any non-finite value), or
/// there is neither a base nor an input.
pub fn bucket_state(
    quantiles: bool,
    base: Option<&[u8]>,
    inputs: impl IntoIterator<Item = i64>,
) -> Option<Vec<u8>> {
    let inputs: Vec<f64> = inputs
        .into_iter()
        .map(|b| f64::from_bits(b as u64))
        .collect();
    if base.is_none() && inputs.is_empty() {
        return None;
    }
    if quantiles {
        let mut sk = match base {
            Some(b) => decode_sketch(b)?,
            None => Sketch::default(),
        };
        for x in inputs {
            if !x.is_finite() {
                return None;
            }
            if x == 0.0 {
                sk.zero += 1;
            } else {
                let store = if x > 0.0 { &mut sk.pos } else { &mut sk.neg };
                *store.entry(index(x)).or_insert(0) += 1;
            }
        }
        Some(encode_sketch(&sk))
    } else {
        let mut counts = match base {
            Some(b) => decode_histogram(b)?,
            None => vec![0; MODEL_HISTOGRAM_BOUNDS.len() + 1],
        };
        for x in inputs {
            if x.is_nan() {
                return None;
            }
            counts[bucket(x)] += 1;
        }
        Some(encode_histogram(&counts))
    }
}

/// Whether `s` is a well-formed, canonical state of the model's parameters.
pub fn bucket_state_valid(quantiles: bool, s: &[u8]) -> bool {
    if quantiles {
        decode_sketch(s).is_some()
    } else {
        decode_histogram(s).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_format::buckets::{self, QuantileParams};

    #[test]
    fn matches_the_format_codec() {
        // The oracle is independent of the store's code; here, the two agree, collapse
        // included.
        let xs = [
            1e300,
            -1e300,
            5e-324,
            0.0,
            -0.0,
            1.5,
            -2.5,
            1e-300,
            7.0,
            f64::MAX,
        ];
        let q = QuantileParams {
            mantissa_bits: MODEL_QUANTILE_BITS,
            max_buckets: MODEL_QUANTILE_MAX,
        };
        let mut fq = Vec::new();
        let mut fh = Vec::new();
        for &x in &xs {
            buckets::quantiles_add(&mut fq, q, x).unwrap();
            buckets::histogram_add(&mut fh, &MODEL_HISTOGRAM_BOUNDS, x).unwrap();
        }
        let bits = || xs.iter().map(|x| x.to_bits() as i64);
        assert_eq!(bucket_state(true, None, bits()).as_deref(), Some(&fq[..]));
        assert_eq!(bucket_state(false, None, bits()).as_deref(), Some(&fh[..]));
        assert!(bucket_state_valid(true, &fq) && bucket_state_valid(false, &fh));
    }
}
