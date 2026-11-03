//! Half-precision widening (#1268 item 7): IEEE binary16 and bfloat16 bit patterns to `f32`,
//! exactly, into a caller's buffer. Every finite half is exactly an `f32`, so these agree
//! bit for bit with `pigeonhole`'s own widening (`VectorRef::widen_into`), which a test
//! checks over all 65,536 patterns. Centroid math and the half queries use them; a bulk
//! load can widen each vector into one reused buffer.

use pigeonhole::Element;

/// One IEEE binary16 value as `f32`.
pub(crate) fn f16_to_f32(h: u16) -> f32 {
    let sign = u32::from(h >> 15) << 31;
    let exp = u32::from((h >> 10) & 0x1f);
    let man = u32::from(h & 0x3ff);
    let bits = match (exp, man) {
        (0, 0) => sign,
        // Subnormal: normalise the mantissa into an f32 normal.
        (0, m) => {
            let shift = m.leading_zeros() - 21;
            let m = (m << shift) & 0x3ff;
            sign | ((127 - 15 + 1 - shift) << 23) | (m << 13)
        }
        (0x1f, 0) => sign | 0x7f80_0000,
        (0x1f, m) => sign | 0x7f80_0000 | (m << 13),
        (e, m) => sign | ((e + 127 - 15) << 23) | (m << 13),
    };
    f32::from_bits(bits)
}

/// One bfloat16 value as `f32`: its bits are an `f32`'s top half.
pub(crate) fn bf16_to_f32(b: u16) -> f32 {
    f32::from_bits(u32::from(b) << 16)
}

/// Appends the half vector `bits` of `element` (`F16` or `BF16`) to `out` as `f32`s.
pub(crate) fn widen_into(element: Element, bits: &[u16], out: &mut Vec<f32>) {
    match element {
        Element::BF16 => out.extend(bits.iter().map(|b| bf16_to_f32(*b))),
        Element::F16 => out.extend(bits.iter().map(|h| f16_to_f32(*h))),
        other => unreachable!("half::widen_into for a {other:?} vector"),
    }
}

/// `x` rounded to `element` (`F16` or `BF16`), ties to even, as `put_vector_f32` rounds it
/// into a half family; `None` if `x` isn't finite or rounds past the element's largest
/// finite value (a write the family refuses).
pub(crate) fn narrow(element: Element, x: f32) -> Option<u16> {
    match element {
        Element::BF16 => f32_to_bf16(x),
        Element::F16 => f32_to_f16(x),
        other => unreachable!("half::narrow to a {other:?} vector"),
    }
}

/// `x` as the `f32` a half family stores for it: rounded to `element`, then widened. `x`
/// itself if the family would refuse it (the write then fails, so nothing is placed).
pub(crate) fn round_trip(element: Element, x: f32) -> f32 {
    match narrow(element, x) {
        Some(h) if element == Element::BF16 => bf16_to_f32(h),
        Some(h) => f16_to_f32(h),
        None => x,
    }
}

/// `x` rounded to the nearest `f16`, ties to even.
fn f32_to_f16(x: f32) -> Option<u16> {
    if !x.is_finite() {
        return None;
    }
    let bits = x.to_bits();
    let sign = ((bits >> 16) & 0x8000) as u16;
    let abs = bits & 0x7fff_ffff;
    // 65,520, the midpoint between 65,504 (the largest finite f16) and 65,536, rounds up.
    if abs >= 0x477f_f000 {
        return None;
    }
    if abs < 0x3880_0000 {
        // Below 2^-14: a subnormal (or zero), mantissa x 2^-24. Scaling by 2^24 is exact.
        let m = (f32::from_bits(abs) * 16_777_216.0).round_ties_even() as u16;
        return Some(sign | m);
    }
    let exp = (abs >> 23) + 15 - 127;
    let mant = abs & 0x7f_ffff;
    let mut h = (exp << 10) | (mant >> 13);
    let rest = mant & 0x1fff;
    // A carry out of the mantissa rounds into the exponent, as it should.
    if rest > 0x1000 || (rest == 0x1000 && h & 1 == 1) {
        h += 1;
    }
    Some(sign | h as u16)
}

/// `x` rounded to the nearest `bf16`, ties to even.
fn f32_to_bf16(x: f32) -> Option<u16> {
    if !x.is_finite() {
        return None;
    }
    let bits = x.to_bits();
    let h = ((bits + 0x7fff + ((bits >> 16) & 1)) >> 16) as u16;
    // Rounding past the largest finite value reaches the infinity's exponent.
    (h & 0x7f80 != 0x7f80).then_some(h)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole::Vector;

    /// Every finite pattern widens as `pigeonhole` widens it (its stored halves refuse
    /// non-finite components, so those aren't compared there; they're checked by class).
    #[test]
    fn widening_matches_pigeonhole_for_every_pattern() {
        for bits in 0..=u16::MAX {
            for (element, ours) in [
                (Element::F16, f16_to_f32(bits)),
                (Element::BF16, bf16_to_f32(bits)),
            ] {
                let v = match element {
                    Element::F16 => Vector::from_f16_bits(&[bits]),
                    _ => Vector::from_bf16_bits(&[bits]),
                };
                match v {
                    Ok(v) => {
                        let mut want = Vec::new();
                        assert!(v.as_ref().widen_into(&mut want));
                        assert_eq!(ours.to_bits(), want[0].to_bits(), "{element:?} {bits:#06x}");
                    }
                    Err(_) => assert!(!ours.is_finite(), "{element:?} {bits:#06x} is finite"),
                }
            }
        }
        assert!(f16_to_f32(0x7c00).is_infinite() && f16_to_f32(0x7e00).is_nan());
        assert_eq!(f16_to_f32(0x0001), 2f32.powi(-24));
        assert_eq!(f16_to_f32(0x3c00), 1.0);
    }

    /// Narrowing agrees with the rounding `put_vector_f32` applies into a half family
    /// (`pigeonhole-format`'s), at every f32 exponent and sign, with mantissas that hit each
    /// rounding case: exact, just below, at and just above a tie, for both elements.
    #[test]
    fn narrowing_matches_the_stored_rounding() {
        use pigeonhole_format::half as fmt;
        let tails = [
            0u32, 1, 0x0fff, 0x1000, 0x1001, 0x1fff, 0x2000, 0x3000, 0x7fff, 0x8000, 0x8001,
            0xffff, 0x1_0000, 0x1_8000, 0x7f_e000, 0x7f_f000, 0x7f_ffff,
        ];
        for sign_exp in 0..=0x1ffu32 {
            for top in [0u32, 0x10_0000, 0x40_0000, 0x7e_0000] {
                for tail in tails {
                    let bits = (sign_exp << 23) | ((top | tail) & 0x7f_ffff);
                    let x = f32::from_bits(bits);
                    assert_eq!(
                        narrow(Element::F16, x),
                        fmt::f32_to_f16(x),
                        "f16 {bits:#010x}"
                    );
                    assert_eq!(
                        narrow(Element::BF16, x),
                        fmt::f32_to_bf16(x),
                        "bf16 {bits:#010x}"
                    );
                }
            }
        }
        assert_eq!(round_trip(Element::F16, 1.0001), 1.0);
        assert_eq!(round_trip(Element::F16, 1.0e6), 1.0e6, "refused, kept");
    }
}
