//! Exact `f64` sums and mean/variance states, the values of `SumF64` and `MeanVar` aggregate
//! families (decision D298; FORMAT §3.2, §3.3).
//!
//! Every finite `f64` is `m·2^e` with an integer `|m| < 2^53` and `e ≥ -1074`, so it is an
//! integer multiple of 2^-1074 below 2^1024. A sum of up to 2^64 of them is an integer
//! number of 2^-1074 units below 2^2112: a **2176-bit two's-complement accumulator** (34
//! little-endian `u64` limbs) holds it exactly. A square `x² = m²·2^2e` is an integer number
//! of 2^-2148 units below 2^2048, so the sum of squares takes a **4288-bit** accumulator (67
//! limbs) scaled by 2^-2148. Adding an input and merging two states are integer additions:
//! exact, so commutative and associative, and a state does not depend on how flushes and
//! compactions grouped its inputs. Reads round once, to nearest with ties to even.
//!
//! **Accumulator encoding:** a limb count `n` (u8), then `n` limbs (`u64` LE), least
//! significant first. High limbs that are pure sign extension of the limb below are trimmed
//! (a top limb of 0 above a limb with bit 63 clear, or of `u64::MAX` above one with it set),
//! and zero is no limbs at all, so every value has one encoding.
//!
//! | State | Bytes |
//! |---|---|
//! | `SumF64` | `2` (encoding), the Σx accumulator (34 limbs at most) |
//! | `MeanVar` | `3` (encoding), the count (varint, at least 1), the Σx accumulator, the Σx² accumulator (67 limbs at most, never negative) |
//!
//! A state is at most [`SUM_MAX_LEN`] or [`MEAN_VAR_MAX_LEN`] bytes. The encoding byte keeps
//! the shapes apart from an HLL sketch's (encoding 1, FORMAT §3.1).
//!
//! ```
//! use pigeonhole_format::exact;
//!
//! let mut acc = Vec::new();
//! for x in [1e100, 1.0, -1e100] {
//!     exact::sum_add(&mut acc, x).unwrap();
//! }
//! assert_eq!(exact::sum_read(&acc).unwrap(), 1.0); // floating-point addition gives 0.0
//! let mut mv = Vec::new();
//! for x in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
//!     exact::mean_var_add(&mut mv, x).unwrap();
//! }
//! let s = exact::mean_var_read(&mv).unwrap();
//! assert_eq!((s.count, s.mean, s.variance), (8, 5.0, 4.0));
//! ```

use std::fmt;

/// The encoding byte of a `SumF64` state.
pub const SUM_ENCODING: u8 = 2;
/// The encoding byte of a `MeanVar` state.
pub const MEAN_VAR_ENCODING: u8 = 3;
/// Limbs of the Σx accumulator: 2176 bits.
pub const SUM_LIMBS: usize = 34;
/// Limbs of the Σx² accumulator: 4288 bits.
pub const SQUARE_LIMBS: usize = 67;
/// The longest `SumF64` state.
pub const SUM_MAX_LEN: usize = 2 + 8 * SUM_LIMBS;
/// The longest `MeanVar` state: encoding, a 10-byte varint, both accumulators.
pub const MEAN_VAR_MAX_LEN: usize = 1 + 10 + (1 + 8 * SUM_LIMBS) + (1 + 8 * SQUARE_LIMBS);

/// A malformed state, or an input these kinds refuse (NaN, an infinity).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateError(pub &'static str);

impl fmt::Display for StateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for StateError {}

/// The result type of this module.
pub type Result<T> = std::result::Result<T, StateError>;

/// A two's-complement integer of `N` limbs.
#[derive(Clone, Copy)]
struct Acc<const N: usize>([u64; N]);

impl<const N: usize> Acc<N> {
    const ZERO: Self = Acc([0; N]);

    fn negative(&self) -> bool {
        self.0[N - 1] >> 63 == 1
    }

    /// Adds (or with `neg` subtracts) `m << shift`, wrapping at `N` limbs.
    fn add_shifted(&mut self, m: u128, shift: usize, neg: bool) {
        let (limb, bits) = (shift / 64, shift % 64);
        // `m` (at most 106 bits) shifted by up to 63 bits spans three limbs.
        let lo = m << bits;
        let hi = if bits == 0 {
            0
        } else {
            (m >> (128 - bits)) as u64
        };
        let mut t = Self::ZERO;
        for (k, p) in [lo as u64, (lo >> 64) as u64, hi].into_iter().enumerate() {
            if let Some(l) = t.0.get_mut(limb + k) {
                *l = p;
            }
        }
        if neg {
            let mut carry = true;
            for l in &mut t.0 {
                let (v, c) = (!*l).overflowing_add(u64::from(carry));
                *l = v;
                carry = c;
            }
        }
        self.add(&t);
    }

    fn add(&mut self, other: &Self) {
        let mut carry = false;
        for (a, b) in self.0.iter_mut().zip(other.0) {
            let (s1, c1) = a.overflowing_add(b);
            let (s2, c2) = s1.overflowing_add(u64::from(carry));
            *a = s2;
            carry = c1 || c2;
        }
    }

    /// Appends the encoding: the limb count, then the limbs not trimmed.
    fn encode(&self, out: &mut Vec<u8>) {
        let mut n = N;
        while n > 0 {
            let top = self.0[n - 1];
            let below_neg = n >= 2 && self.0[n - 2] >> 63 == 1;
            let trim = if n == 1 {
                top == 0
            } else {
                (top == 0 && !below_neg) || (top == u64::MAX && below_neg)
            };
            if !trim {
                break;
            }
            n -= 1;
        }
        out.push(n as u8);
        for l in &self.0[..n] {
            out.extend_from_slice(&l.to_le_bytes());
        }
    }

    /// Reads an encoding from the front of `s`; returns it and the bytes it took.
    fn decode(s: &[u8]) -> Result<(Self, usize)> {
        let malformed = StateError("malformed accumulator");
        let n = usize::from(*s.first().ok_or(malformed)?);
        if n > N || s.len() < 1 + 8 * n {
            return Err(malformed);
        }
        let mut a = Self::ZERO;
        for (i, l) in a.0[..n].iter_mut().enumerate() {
            *l = u64::from_le_bytes(s[1 + 8 * i..9 + 8 * i].try_into().expect("8 bytes"));
        }
        if n > 0 {
            let top = a.0[n - 1];
            let below_neg = n >= 2 && a.0[n - 2] >> 63 == 1;
            let trimmable = if n == 1 {
                top == 0
            } else {
                (top == 0 && !below_neg) || (top == u64::MAX && below_neg)
            };
            if trimmable {
                return Err(StateError("accumulator not in canonical form"));
            }
            if top >> 63 == 1 {
                for l in &mut a.0[n..] {
                    *l = u64::MAX;
                }
            }
        }
        Ok((a, 1 + 8 * n))
    }

    /// Sign and magnitude, as limbs (least significant first, no high zeros).
    fn magnitude(&self) -> (bool, Vec<u64>) {
        let neg = self.negative();
        let mut m = self.0.to_vec();
        if neg {
            let mut carry = true;
            for l in &mut m {
                let (v, c) = (!*l).overflowing_add(u64::from(carry));
                *l = v;
                carry = c;
            }
        }
        trim(&mut m);
        (neg, m)
    }
}

/// The significand and binary shift of a finite `x`: `|x| = m · 2^(shift - 1074)`.
fn split(x: f64) -> Result<(bool, u64, usize)> {
    if !x.is_finite() {
        return Err(StateError("an exact sum takes finite f64s only"));
    }
    let bits = x.to_bits();
    let exp = ((bits >> 52) & 0x7ff) as usize;
    let frac = bits & ((1 << 52) - 1);
    let m = if exp == 0 { frac } else { frac | 1 << 52 };
    Ok((bits >> 63 == 1, m, exp.max(1) - 1))
}

/// Starts or extends `acc` (a `SumF64` state, or empty for none yet) with the input `x`.
pub fn sum_add(acc: &mut Vec<u8>, x: f64) -> Result<()> {
    let (neg, m, shift) = split(x)?;
    let mut s = sum_state(acc)?;
    s.add_shifted(u128::from(m), shift, neg);
    acc.clear();
    acc.push(SUM_ENCODING);
    s.encode(acc);
    Ok(())
}

/// The Σx accumulator of a `SumF64` state (zero for an empty `s`).
fn sum_state(s: &[u8]) -> Result<Acc<SUM_LIMBS>> {
    if s.is_empty() {
        return Ok(Acc::ZERO);
    }
    if s[0] != SUM_ENCODING {
        return Err(StateError("not an exact-sum state"));
    }
    let (a, used) = Acc::decode(&s[1..])?;
    if 1 + used != s.len() {
        return Err(StateError("bytes after an exact-sum state"));
    }
    Ok(a)
}

/// Checks that `s` is a well-formed, canonical `SumF64` state.
pub fn sum_validate(s: &[u8]) -> Result<()> {
    if s.is_empty() {
        return Err(StateError("an empty exact-sum state"));
    }
    sum_state(s).map(|_| ())
}

/// Merges the `SumF64` state `other` into `acc` (either may be empty for none).
pub fn sum_merge(acc: &mut Vec<u8>, other: &[u8]) -> Result<()> {
    let mut a = sum_state(acc)?;
    a.add(&sum_state(other)?);
    acc.clear();
    acc.push(SUM_ENCODING);
    a.encode(acc);
    Ok(())
}

/// The sum a `SumF64` state holds, rounded once to the nearest `f64` (ties to even), or an
/// infinity past the `f64` range. Inputs that cancel exactly read as `+0.0`.
pub fn sum_read(s: &[u8]) -> Result<f64> {
    sum_validate(s)?;
    let (neg, mag) = sum_state(s)?.magnitude();
    Ok(round_ratio(neg, &mag, &[], -1074))
}

/// The `MeanVar` state's parts.
struct MeanVarState {
    count: u64,
    sum: Acc<SUM_LIMBS>,
    squares: Acc<SQUARE_LIMBS>,
}

impl MeanVarState {
    fn decode(s: &[u8]) -> Result<Self> {
        if s.is_empty() {
            return Ok(MeanVarState {
                count: 0,
                sum: Acc::ZERO,
                squares: Acc::ZERO,
            });
        }
        if s[0] != MEAN_VAR_ENCODING {
            return Err(StateError("not a mean-variance state"));
        }
        let (count, used) =
            crate::varint::get_u64(&s[1..]).map_err(|_| StateError("malformed count"))?;
        if count == 0 {
            return Err(StateError("a mean-variance state of no inputs"));
        }
        let mut at = 1 + used;
        let (sum, used) = Acc::decode(&s[at..])?;
        at += used;
        let (squares, used) = Acc::decode(&s[at..])?;
        at += used;
        if at != s.len() {
            return Err(StateError("bytes after a mean-variance state"));
        }
        if squares.negative() {
            return Err(StateError("a negative sum of squares"));
        }
        Ok(MeanVarState {
            count,
            sum,
            squares,
        })
    }

    fn encode(&self, out: &mut Vec<u8>) {
        out.clear();
        out.push(MEAN_VAR_ENCODING);
        crate::varint::put_u64(out, self.count);
        self.sum.encode(out);
        self.squares.encode(out);
    }
}

/// Starts or extends `acc` (a `MeanVar` state, or empty for none yet) with the input `x`.
pub fn mean_var_add(acc: &mut Vec<u8>, x: f64) -> Result<()> {
    let (neg, m, shift) = split(x)?;
    let mut s = MeanVarState::decode(acc)?;
    s.count = s.count.wrapping_add(1);
    s.sum.add_shifted(u128::from(m), shift, neg);
    // x² = m² · 2^(2·shift - 2148): exact, at the squares' scale of 2^-2148.
    s.squares
        .add_shifted(u128::from(m) * u128::from(m), 2 * shift, false);
    s.encode(acc);
    Ok(())
}

/// Checks that `s` is a well-formed, canonical `MeanVar` state.
pub fn mean_var_validate(s: &[u8]) -> Result<()> {
    if s.is_empty() {
        return Err(StateError("an empty mean-variance state"));
    }
    MeanVarState::decode(s).map(|_| ())
}

/// Merges the `MeanVar` state `other` into `acc` (either may be empty for none).
pub fn mean_var_merge(acc: &mut Vec<u8>, other: &[u8]) -> Result<()> {
    let mut a = MeanVarState::decode(acc)?;
    let b = MeanVarState::decode(other)?;
    if b.count == 0 {
        return if a.count == 0 {
            Ok(())
        } else {
            a.encode(acc);
            Ok(())
        };
    }
    a.count = a.count.wrapping_add(b.count);
    a.sum.add(&b.sum);
    a.squares.add(&b.squares);
    a.encode(acc);
    Ok(())
}

/// What a `MeanVar` state reads as: each figure computed exactly from the accumulators and
/// rounded once.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MeanVar {
    /// The number of inputs.
    pub count: u64,
    /// Σx / n.
    pub mean: f64,
    /// The population variance, (n·Σx² − (Σx)²) / n².
    pub variance: f64,
    /// The sample variance, (n·Σx² − (Σx)²) / (n(n−1)); `None` for a single input.
    pub sample_variance: Option<f64>,
}

/// Reads a `MeanVar` state.
pub fn mean_var_read(s: &[u8]) -> Result<MeanVar> {
    mean_var_validate(s)?;
    let st = MeanVarState::decode(s)?;
    let n = st.count;
    let (neg, sum) = st.sum.magnitude();
    let mean = round_ratio(neg, &sum, &[n], -1074);
    // n·Σx² − (Σx)², both at 2^-2148: never negative (Cauchy–Schwarz).
    let (_, squares) = st.squares.magnitude();
    let mut d = mul_small(&squares, n);
    let s2 = mul(&sum, &sum);
    sub_in_place(&mut d, &s2);
    let variance = round_ratio(false, &d, &[n, n], -2148);
    let sample_variance = (n >= 2).then(|| round_ratio(false, &d, &[n, n - 1], -2148));
    Ok(MeanVar {
        count: n,
        mean,
        variance,
        sample_variance,
    })
}

fn trim(m: &mut Vec<u64>) {
    while m.last() == Some(&0) {
        m.pop();
    }
}

fn mul_small(a: &[u64], k: u64) -> Vec<u64> {
    let mut out = Vec::with_capacity(a.len() + 1);
    let mut carry = 0u128;
    for &l in a {
        let p = u128::from(l) * u128::from(k) + carry;
        out.push(p as u64);
        carry = p >> 64;
    }
    out.push(carry as u64);
    trim(&mut out);
    out
}

fn mul(a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut out = vec![0u64; a.len() + b.len()];
    for (i, &x) in a.iter().enumerate() {
        let mut carry = 0u128;
        for (j, &y) in b.iter().enumerate() {
            let t = u128::from(out[i + j]) + u128::from(x) * u128::from(y) + carry;
            out[i + j] = t as u64;
            carry = t >> 64;
        }
        out[i + b.len()] = carry as u64;
    }
    trim(&mut out);
    out
}

/// `a -= b`, clamped at zero (`a < b` happens only for a malformed state's figures).
fn sub_in_place(a: &mut Vec<u64>, b: &[u64]) {
    if cmp(a, b).is_lt() {
        a.clear();
        return;
    }
    let mut borrow = false;
    for (i, x) in a.iter_mut().enumerate() {
        let y = b.get(i).copied().unwrap_or(0);
        let (d1, b1) = x.overflowing_sub(y);
        let (d2, b2) = d1.overflowing_sub(u64::from(borrow));
        *x = d2;
        borrow = b1 || b2;
    }
    trim(a);
}

fn cmp(a: &[u64], b: &[u64]) -> std::cmp::Ordering {
    a.len()
        .cmp(&b.len())
        .then_with(|| a.iter().rev().cmp(b.iter().rev()))
}

fn bit_len(a: &[u64]) -> usize {
    a.last().map_or(0, |&top| {
        64 * (a.len() - 1) + 64 - top.leading_zeros() as usize
    })
}

fn shl(a: &[u64], s: usize) -> Vec<u64> {
    let (limbs, bits) = (s / 64, s % 64);
    let mut out = vec![0u64; limbs];
    let mut carry = 0u64;
    for &l in a {
        out.push(if bits == 0 { l } else { (l << bits) | carry });
        carry = if bits == 0 { 0 } else { l >> (64 - bits) };
    }
    out.push(carry);
    trim(&mut out);
    out
}

/// `a / d` (floor) in place; returns whether a remainder was left.
fn div_small(a: &mut Vec<u64>, d: u64) -> bool {
    let mut rem = 0u128;
    for l in a.iter_mut().rev() {
        let cur = (rem << 64) | u128::from(*l);
        *l = (cur / u128::from(d)) as u64;
        rem = cur % u128::from(d);
    }
    trim(a);
    rem != 0
}

/// Bits `[lo, lo + 64)` of `a`, and whether any bit below `lo` is set.
fn bits_at(a: &[u64], lo: usize) -> (u64, bool) {
    let (limb, bits) = (lo / 64, lo % 64);
    let get = |i: usize| a.get(i).copied().unwrap_or(0);
    let v = if bits == 0 {
        get(limb)
    } else {
        (get(limb) >> bits) | (get(limb + 1) << (64 - bits))
    };
    let below = a[..limb.min(a.len())].iter().any(|&l| l != 0)
        || (bits != 0 && get(limb) & ((1 << bits) - 1) != 0);
    (v, below)
}

/// `±mag / Π divisors · 2^scale`, rounded to the nearest `f64` (ties to even), or an
/// infinity past the range; `+0.0` for zero.
fn round_ratio(neg: bool, mag: &[u64], divisors: &[u64], scale: i32) -> f64 {
    if mag.is_empty() {
        return 0.0;
    }
    // Enough quotient bits to round: 55 and more above the divisors' size.
    let div_bits: usize = divisors
        .iter()
        .map(|&d| 64 - d.leading_zeros() as usize)
        .sum();
    let s = (div_bits + 58).saturating_sub(bit_len(mag));
    let mut q = shl(mag, s);
    let mut sticky = false;
    for &d in divisors {
        sticky |= div_small(&mut q, d);
    }
    // value = (q + sticky·ε) · 2^exp, with q at least 56 bits long.
    let exp = i64::from(scale) - s as i64;
    let h = bit_len(&q) as i64 - 1;
    // The result's unit in the last place: 53 bits below the top, never below 2^-1074. At
    // least 3 bits of q lie below it.
    let lsb = (h + exp - 52).max(-1074);
    let drop = (lsb - exp) as usize;
    debug_assert!(drop >= 3);
    let (v, _) = bits_at(&q, drop);
    let (half, rest) = bits_at(&q, drop - 1);
    let mut m = v & ((1 << 54) - 1);
    if half & 1 == 1 && (rest || sticky || m & 1 == 1) {
        m += 1;
    }
    let mut lsb = lsb;
    if m >= 1 << 53 {
        m >>= 1;
        lsb += 1;
    }
    let bits = if m >= 1 << 52 {
        let be = lsb + 1075;
        if be >= 2047 {
            0x7ff << 52
        } else {
            ((be as u64) << 52) | (m - (1 << 52))
        }
    } else {
        m
    };
    f64::from_bits(bits | (u64::from(neg) << 63))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounding_matches_f64_where_it_is_exact() {
        for x in [
            0.0,
            1.0,
            -1.0,
            0.5,
            1e-310,
            -5e-324,
            f64::MAX,
            f64::MIN_POSITIVE,
            3.25e200,
        ] {
            let mut acc = Vec::new();
            sum_add(&mut acc, x).unwrap();
            assert_eq!(
                sum_read(&acc).unwrap().to_bits(),
                (x + 0.0).to_bits(),
                "{x:e}"
            );
        }
        // Past the range: infinity.
        let mut acc = Vec::new();
        sum_add(&mut acc, f64::MAX).unwrap();
        sum_add(&mut acc, f64::MAX).unwrap();
        assert_eq!(sum_read(&acc).unwrap(), f64::INFINITY);
        // Ties to even: 2^53 + 1 rounds down to 2^53, 2^53 + 3 up to 2^53 + 4.
        for (extra, want) in [(1.0, 9007199254740992.0), (3.0, 9007199254740996.0)] {
            let mut acc = Vec::new();
            sum_add(&mut acc, 9007199254740992.0).unwrap();
            sum_add(&mut acc, extra).unwrap();
            assert_eq!(sum_read(&acc).unwrap(), want, "+{extra}");
        }
    }

    #[test]
    fn a_division_remainder_breaks_a_tie() {
        // q = 2^58 + 2^5 with remainder 1 (mag = 3q + 1): the kept bits are 2^52 (even), the
        // half bit is set and the bits below it are clear, so only the remainder says the
        // value is past the tie and rounds up.
        let q = (1u64 << 58) + (1 << 5);
        let mag = 3 * q + 1;
        let want = ((1u64 << 52) + 1) as f64 * 64.0;
        assert_eq!(round_ratio(false, &[mag], &[3], 0), want);
        // Without a remainder it is a tie, to even.
        assert_eq!(round_ratio(false, &[3 * q], &[3], 0), (1u64 << 58) as f64);
    }

    #[test]
    fn non_canonical_states_are_refused() {
        // Zero is no limbs; a top limb that is sign extension is refused.
        assert!(sum_validate(&[SUM_ENCODING, 0]).is_ok());
        assert!(sum_validate(&[SUM_ENCODING, 1, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
        let mut minus_one = vec![SUM_ENCODING, 1];
        minus_one.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(sum_validate(&minus_one).is_ok());
        let mut two = vec![SUM_ENCODING, 2];
        two.extend_from_slice(&u64::MAX.to_le_bytes());
        two.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(sum_validate(&two).is_err());
        assert!(sum_validate(&[SUM_ENCODING, 35]).is_err());
        assert!(sum_validate(&[1, 0]).is_err());
        assert!(mean_var_validate(&[MEAN_VAR_ENCODING, 0, 0, 0]).is_err()); // no inputs
    }
}
