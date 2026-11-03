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
}
