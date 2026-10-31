//! The model's oracle for D298's exact `f64` kinds (`SumF64`, `MeanVar`; FORMAT §3.2–§3.3),
//! written from the encoding's description rather than with the store's code
//! (`pigeonhole_format::exact`): its own big integers (32-bit limbs), its own trimming and
//! its own decoder, so a slip in one is not repeated in the other.
//!
//! A state holds exact integers: Σx in units of 2^-1074 (2176-bit two's complement), and for
//! mean/variance the count and Σx² in units of 2^-2148 (4288 bits). An input `x = ±m·2^(e)`
//! (finite) adds `±m·2^(e+1074)` to Σx and `m²·2^(2(e+1074))` to Σx².

/// Bits of the Σx accumulator.
const SUM_BITS: usize = 2176;
/// Bits of the Σx² accumulator.
const SQUARE_BITS: usize = 4288;

/// A two's-complement integer of `bits / 32` limbs of 32 bits.
#[derive(Clone, PartialEq, Debug)]
struct Wide(Vec<u32>);

impl Wide {
    fn zero(bits: usize) -> Self {
        Wide(vec![0; bits / 32])
    }

    /// Adds `±v · 2^shift` (`v` as 32-bit limbs).
    fn add(&mut self, v: &[u32], shift: usize, neg: bool) {
        let n = self.0.len();
        let mut t = vec![0u32; n];
        let (limb, bit) = (shift / 32, shift % 32);
        let mut carry = 0u64;
        for (i, &x) in v.iter().enumerate() {
            let w = (u64::from(x) << bit) | carry;
            if limb + i < n {
                t[limb + i] = w as u32;
            }
            carry = w >> 32;
        }
        if limb + v.len() < n {
            t[limb + v.len()] = carry as u32;
        }
        if neg {
            for x in &mut t {
                *x = !*x;
            }
            add_one(&mut t);
        }
        let mut c = 0u64;
        for (a, b) in self.0.iter_mut().zip(t) {
            let s = u64::from(*a) + u64::from(b) + c;
            *a = s as u32;
            c = s >> 32;
        }
    }

    fn add_wide(&mut self, other: &Wide) {
        let mut c = 0u64;
        for (a, &b) in self.0.iter_mut().zip(&other.0) {
            let s = u64::from(*a) + u64::from(b) + c;
            *a = s as u32;
            c = s >> 32;
        }
    }

    fn negative(&self) -> bool {
        self.0.last().is_some_and(|&t| t >> 31 == 1)
    }

    /// The encoding: a count of 64-bit limbs, then those limbs (LE), high sign-extension
    /// limbs trimmed (zero is no limbs).
    fn encode(&self, out: &mut Vec<u8>) {
        let limbs64: Vec<u64> = self
            .0
            .chunks(2)
            .map(|c| u64::from(c[0]) | u64::from(c[1]) << 32)
            .collect();
        let mut n = limbs64.len();
        let sign = if self.negative() { u64::MAX } else { 0 };
        // Drop a top limb while it equals the sign and the limb below already carries it.
        while n > 0 {
            let top = limbs64[n - 1];
            let below_sign = if n >= 2 {
                if limbs64[n - 2] >> 63 == 1 {
                    u64::MAX
                } else {
                    0
                }
            } else {
                0
            };
            if top == sign && top == below_sign {
                n -= 1;
            } else {
                break;
            }
        }
        out.push(n as u8);
        for l in &limbs64[..n] {
            out.extend_from_slice(&l.to_le_bytes());
        }
    }

    /// Decodes an accumulator of `bits` from the front of `s`, `None` unless canonical.
    fn decode(s: &[u8], bits: usize) -> Option<(Wide, usize)> {
        let n = usize::from(*s.first()?);
        if n > bits / 64 || s.len() < 1 + 8 * n {
            return None;
        }
        let mut limbs: Vec<u64> = (0..n)
            .map(|i| u64::from_le_bytes(s[1 + 8 * i..9 + 8 * i].try_into().unwrap()))
            .collect();
        let neg = limbs.last().is_some_and(|&t| t >> 63 == 1);
        limbs.resize(bits / 64, if neg { u64::MAX } else { 0 });
        let mut w = Wide::zero(bits);
        for (i, l) in limbs.iter().enumerate() {
            w.0[2 * i] = *l as u32;
            w.0[2 * i + 1] = (*l >> 32) as u32;
        }
        // Canonical only if re-encoding gives the same bytes.
        let mut again = Vec::new();
        w.encode(&mut again);
        (again[..] == s[..1 + 8 * n]).then_some((w, 1 + 8 * n))
    }
}

fn add_one(t: &mut [u32]) {
    for x in t {
        let (v, c) = x.overflowing_add(1);
        *x = v;
        if !c {
            return;
        }
    }
}

/// `x`'s sign, its significand as 32-bit limbs and its shift above 2^-1074; `None` unless
/// finite.
fn split(x: f64) -> Option<(bool, u64, usize)> {
    if !x.is_finite() {
        return None;
    }
    let bits = x.to_bits();
    let exp = ((bits >> 52) & 0x7ff) as usize;
    let frac = bits & ((1 << 52) - 1);
    let (m, shift) = if exp == 0 {
        (frac, 0)
    } else {
        (frac | 1 << 52, exp - 1)
    };
    Some((bits >> 63 == 1, m, shift))
}

fn limbs32(v: u128) -> Vec<u32> {
    (0..4).map(|i| (v >> (32 * i)) as u32).collect()
}

/// The parts of a state: the count (mean/variance only), Σx and Σx².
struct Parts {
    count: u64,
    sum: Wide,
    squares: Wide,
}

fn read_varint(s: &[u8]) -> Option<(u64, usize)> {
    let mut v = 0u64;
    for (i, &b) in s.iter().enumerate().take(10) {
        v |= u64::from(b & 0x7f) << (7 * i);
        if b & 0x80 == 0 {
            return Some((v, i + 1));
        }
    }
    None
}

fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn decode(mean_var: bool, s: &[u8]) -> Option<Parts> {
    let (count, mut at) = if mean_var {
        if s.first() != Some(&3) {
            return None;
        }
        let (c, used) = read_varint(&s[1..])?;
        (c, 1 + used)
    } else {
        if s.first() != Some(&2) {
            return None;
        }
        (0, 1)
    };
    let (sum, used) = Wide::decode(&s[at..], SUM_BITS)?;
    at += used;
    let squares = if mean_var {
        let (q, used) = Wide::decode(&s[at..], SQUARE_BITS)?;
        at += used;
        if q.negative() || count == 0 {
            return None;
        }
        q
    } else {
        Wide::zero(SQUARE_BITS)
    };
    (at == s.len()).then_some(Parts {
        count,
        sum,
        squares,
    })
}

fn encode(mean_var: bool, p: &Parts) -> Vec<u8> {
    let mut out = Vec::new();
    if mean_var {
        out.push(3);
        write_varint(&mut out, p.count);
    } else {
        out.push(2);
    }
    p.sum.encode(&mut out);
    if mean_var {
        p.squares.encode(&mut out);
    }
    out
}

/// The state of `base` (if any) with the inputs `inputs` (`f64` bits) folded in, for an
/// exact sum (`mean_var` false) or a mean/variance family. `None` if `base` is malformed,
/// an input is not finite, or there is neither a base nor an input.
pub fn exact_state(
    mean_var: bool,
    base: Option<&[u8]>,
    inputs: impl IntoIterator<Item = i64>,
) -> Option<Vec<u8>> {
    let mut p = match base {
        Some(b) => decode(mean_var, b)?,
        None => Parts {
            count: 0,
            sum: Wide::zero(SUM_BITS),
            squares: Wide::zero(SQUARE_BITS),
        },
    };
    let mut any = base.is_some();
    for bits in inputs {
        let (neg, m, shift) = split(f64::from_bits(bits as u64))?;
        p.count += 1;
        p.sum.add(&limbs32(u128::from(m)), shift, neg);
        p.squares
            .add(&limbs32(u128::from(m) * u128::from(m)), 2 * shift, false);
        any = true;
    }
    any.then(|| encode(mean_var, &p))
}

/// Whether `s` is a well-formed, canonical state of its kind.
pub fn exact_state_valid(mean_var: bool, s: &[u8]) -> bool {
    decode(mean_var, s).is_some()
}

/// Two states merged (the union of their inputs), `None` if either is malformed.
pub fn exact_merge(mean_var: bool, a: &[u8], b: &[u8]) -> Option<Vec<u8>> {
    let mut p = decode(mean_var, a)?;
    let q = decode(mean_var, b)?;
    p.count += q.count;
    p.sum.add_wide(&q.sum);
    p.squares.add_wide(&q.squares);
    Some(encode(mean_var, &p))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_format_codec() {
        // The oracle is independent of the store's code; here, the two agree.
        let xs = [1.5f64, -2.25, 1e300, -1e300, 5e-324, 0.0, -0.0, 123456.789];
        for mean_var in [false, true] {
            let mut fmt = Vec::new();
            for &x in &xs {
                if mean_var {
                    pigeonhole_format::exact::mean_var_add(&mut fmt, x).unwrap();
                } else {
                    pigeonhole_format::exact::sum_add(&mut fmt, x).unwrap();
                }
            }
            let model = exact_state(mean_var, None, xs.iter().map(|x| x.to_bits() as i64));
            assert_eq!(model.as_deref(), Some(&fmt[..]), "mean_var {mean_var}");
            assert!(exact_state_valid(mean_var, &fmt));
        }
    }
}
