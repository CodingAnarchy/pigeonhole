//! Half-precision vector components (#1263, ICR 0104): IEEE binary16 (`f16`) and bfloat16
//! (`bf16`), as their raw `u16` bit patterns, little-endian in a vector's payload.
//!
//! - **Widening** (`f16_to_f32`, `bf16_to_f32`) is exact and total: every bit pattern widens to
//!   the `f32` with the same value (an infinity or NaN to an infinity or NaN), without a branch,
//!   so the distance kernels can widen each lane in their hot loops.
//! - **Narrowing** (`f32_to_f16`, `f32_to_bf16`) rounds to nearest, ties to even, and refuses a
//!   value that isn't finite or that rounds past the element's largest finite value.
//!
//! ```
//! use pigeonhole_format::half::{bf16_to_f32, f16_to_f32, f32_to_bf16, f32_to_f16};
//!
//! assert_eq!(f32_to_f16(1.5), Some(0x3e00));
//! assert_eq!(f16_to_f32(0x3e00), 1.5);
//! assert_eq!(f32_to_f16(65_504.0), Some(0x7bff)); // the largest finite f16
//! assert_eq!(f32_to_f16(70_000.0), None); // overflows
//! assert_eq!(f32_to_bf16(1.0), Some(0x3f80));
//! assert_eq!(bf16_to_f32(0x3f80), 1.0);
//! ```

/// The `f16` with bits `h`, widened exactly to `f32`: normal and subnormal values, ±0, and
/// infinities and NaNs (which a write refuses, but corrupt bytes may hold) as infinities and
/// NaNs. Branch-free: the exponent and mantissa shift into place and the exponent is rebiased,
/// a subnormal takes one exact float subtraction, and masks choose the result.
#[inline(always)]
pub fn f16_to_f32(h: u16) -> f32 {
    const SHIFTED_EXP: u32 = 0x7c00 << 13;
    let h = u32::from(h);
    let sign = (h & 0x8000) << 16;
    // Exponent and mantissa in an f32's place, the exponent still biased for f16.
    let x = (h & 0x7fff) << 13;
    let exp = x & SHIFTED_EXP;
    // A normal value: rebias the exponent from 15 to 127.
    let normal = x + ((127 - 15) << 23);
    // An infinity or NaN: the exponent all ones (rebias again, to 255).
    let special = normal + ((128 - 16) << 23);
    // A subnormal (or zero): the mantissa under an exponent of 2^-14, minus 2^-14, which is
    // exact; the value is mantissa × 2^-24.
    let sub = (f32::from_bits(x + (113 << 23)) - f32::from_bits(113 << 23)).to_bits();
    let is_special = 0u32.wrapping_sub(u32::from(exp == SHIFTED_EXP));
    let is_sub = 0u32.wrapping_sub(u32::from(exp == 0));
    let is_normal = !(is_special | is_sub);
    f32::from_bits(sign | (special & is_special) | (sub & is_sub) | (normal & is_normal))
}

/// The `bf16` with bits `h`, widened exactly to `f32`: its bits in an `f32`'s high half.
#[inline(always)]
pub fn bf16_to_f32(h: u16) -> f32 {
    f32::from_bits(u32::from(h) << 16)
}

/// `x` rounded to the nearest `f16` (ties to even), or `None` if `x` isn't finite or rounds
/// past 65,504 (the largest finite `f16`: values from 65,520 up round to infinity).
pub fn f32_to_f16(x: f32) -> Option<u16> {
    if !x.is_finite() {
        return None;
    }
    let bits = x.to_bits();
    let sign = ((bits >> 16) & 0x8000) as u16;
    let abs = bits & 0x7fff_ffff;
    // 65,520: the midpoint between 65,504 and 65,536; a tie rounds to even, which is 65,536.
    if abs >= 0x477f_f000 {
        return None;
    }
    if abs < 0x3880_0000 {
        // Below 2^-14: an f16 subnormal (or zero), mantissa × 2^-24. Scaling by 2^24 is exact,
        // and rounding ties to even gives the mantissa (1,024 is the smallest normal's bits).
        let m = (f32::from_bits(abs) * 16_777_216.0).round_ties_even() as u16;
        return Some(sign | m);
    }
    let exp = (abs >> 23) + 15 - 127;
    let mant = abs & 0x7f_ffff;
    let mut h = (exp << 10) | (mant >> 13);
    let rest = mant & 0x1fff;
    // Ties to even; a carry out of the mantissa rounds into the exponent, as it should.
    if rest > 0x1000 || (rest == 0x1000 && h & 1 == 1) {
        h += 1;
    }
    Some(sign | h as u16)
}

/// `x` rounded to the nearest `bf16` (ties to even), or `None` if `x` isn't finite or rounds
/// past the largest finite `bf16`.
pub fn f32_to_bf16(x: f32) -> Option<u16> {
    if !x.is_finite() {
        return None;
    }
    let bits = x.to_bits();
    let rounded = bits + 0x7fff + ((bits >> 16) & 1);
    let h = (rounded >> 16) as u16;
    // Rounding past the largest finite value reaches the infinity's exponent.
    if h & 0x7f80 == 0x7f80 {
        return None;
    }
    Some(h)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn same(a: f32, b: f32) -> bool {
        a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan())
    }

    #[test]
    fn widening_matches_the_half_crate_for_every_pattern() {
        for h in 0..=u16::MAX {
            let want = half::f16::from_bits(h).to_f32();
            assert!(same(f16_to_f32(h), want), "f16 {h:#06x}");
            let want = half::bf16::from_bits(h).to_f32();
            assert!(same(bf16_to_f32(h), want), "bf16 {h:#06x}");
        }
    }

    #[test]
    fn narrowing_round_trips_rounds_to_even_and_refuses_overflow() {
        for h in 0..=u16::MAX {
            let x = f16_to_f32(h);
            if x.is_finite() {
                // Every finite f16 round-trips (−0 included).
                assert_eq!(f32_to_f16(x), Some(h), "f16 {h:#06x}");
            } else {
                assert_eq!(f32_to_f16(x), None, "f16 {h:#06x}");
            }
            let y = bf16_to_f32(h);
            if y.is_finite() {
                assert_eq!(f32_to_bf16(y), Some(h), "bf16 {h:#06x}");
            } else {
                assert_eq!(f32_to_bf16(y), None, "bf16 {h:#06x}");
            }
        }
        // Midpoints between neighbouring f16s land on the even one.
        for h in 0x0000u16..0x7bff {
            let (lo, hi) = (f16_to_f32(h), f16_to_f32(h + 1));
            let mid = ((f64::from(lo) + f64::from(hi)) / 2.0) as f32;
            if f64::from(mid) == (f64::from(lo) + f64::from(hi)) / 2.0 {
                let even = if h % 2 == 0 { h } else { h + 1 };
                assert_eq!(f32_to_f16(mid), Some(even), "midpoint after {h:#06x}");
            }
        }
        // The overflow boundary: below 65,520 rounds to 65,504; 65,520 and up overflow.
        assert_eq!(f32_to_f16(65_519.996), Some(0x7bff));
        assert_eq!(f32_to_f16(65_520.0), None);
        assert_eq!(f32_to_f16(-65_520.0), None);
        assert_eq!(f32_to_f16(f32::NAN), None);
        assert_eq!(f32_to_bf16(f32::INFINITY), None);
        assert_eq!(
            f32_to_bf16(f32::MAX),
            None,
            "rounds up past the largest bf16"
        );
        // Tiny values flush to (signed) zero.
        assert_eq!(f32_to_f16(1e-9), Some(0x0000));
        assert_eq!(f32_to_f16(-1e-9), Some(0x8000));
    }

    #[test]
    fn narrowing_matches_the_half_crate_over_a_seeded_sweep() {
        let mut s = 0x1263_u64 | 1;
        for _ in 0..200_000 {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            let x = f32::from_bits((s >> 32) as u32);
            if !x.is_finite() {
                continue;
            }
            let want = half::f16::from_f32(x);
            let want = want.is_finite().then(|| want.to_bits());
            assert_eq!(f32_to_f16(x), want, "f16 of {x:e} ({:#010x})", x.to_bits());
            let want = half::bf16::from_f32(x);
            let want = want.is_finite().then(|| want.to_bits());
            assert_eq!(
                f32_to_bf16(x),
                want,
                "bf16 of {x:e} ({:#010x})",
                x.to_bits()
            );
        }
    }
}
