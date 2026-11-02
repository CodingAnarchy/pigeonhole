//! Distance kernels for vector search (#490 C, D296).
//!
//! Safe Rust over fixed-size lane groups: each kernel keeps 16 independent accumulators,
//! which the compiler turns into vector instructions on every target without `unsafe` or a
//! dependency. On an M-series core that measured about 65 GB/s for `f32` L2, memory
//! bandwidth, and reading the vector from unaligned little-endian bytes cost nothing more
//! (#490, c6537123400).
//!
//! **Deterministic:** the summation order is fixed (lane `i` takes elements `16k + i`; the
//! lanes are then summed in a fixed pairwise tree; a tail shorter than 16 is added last, in
//! order), and Rust never contracts `a * b + c` into a fused multiply-add. So a kernel gives
//! the same bits for the same inputs on every run, thread and fan-out, and a model check can
//! score with these very functions and compare results exactly (D296 R2). A faster kernel
//! added later must be tested against these within a stated relative error.

/// Lanes per accumulator group.
const LANES: usize = 16;

/// Sums 16 lane accumulators in a fixed pairwise order.
#[inline(always)]
fn tree(acc: [f32; LANES]) -> f32 {
    let mut a = acc;
    let mut n = LANES;
    while n > 1 {
        n /= 2;
        for i in 0..n {
            a[i] += a[i + n];
        }
    }
    a[0]
}

/// The `i`-th little-endian `f32` of `bytes`.
#[inline(always)]
fn le(bytes: &[u8], i: usize) -> f32 {
    f32::from_le_bytes([
        bytes[4 * i],
        bytes[4 * i + 1],
        bytes[4 * i + 2],
        bytes[4 * i + 3],
    ])
}

/// Full lane groups of a query and of its little-endian stored vector, and their tails.
type Groups<'a> = (
    &'a [[f32; LANES]],
    &'a [[u8; 4 * LANES]],
    &'a [f32],
    &'a [u8],
);

/// `q` and the little-endian `f32` vector `v` as full lane groups and their tails.
#[inline(always)]
fn groups<'a>(q: &'a [f32], v: &'a [u8]) -> Groups<'a> {
    let (gq, tq) = q.as_chunks::<LANES>();
    let (gv, tv) = v.as_chunks::<{ 4 * LANES }>();
    (gq, gv, tq, tv)
}

/// Squared Euclidean distance between `q` and the vector `v` stored as `q.len()`
/// little-endian `f32`s (`v.len() == 4 * q.len()`, checked by the caller).
pub fn l2_f32_le(q: &[f32], v: &[u8]) -> f32 {
    debug_assert_eq!(v.len(), 4 * q.len());
    let (gq, gv, tq, tv) = groups(q, v);
    let mut acc = [0f32; LANES];
    for (cq, cv) in gq.iter().zip(gv) {
        for i in 0..LANES {
            let d = cq[i] - le(cv, i);
            acc[i] += d * d;
        }
    }
    let mut sum = tree(acc);
    for (j, x) in tq.iter().enumerate() {
        let d = x - le(tv, j);
        sum += d * d;
    }
    sum
}

/// The inner product of `q` and the little-endian `f32` vector `v`, and `v`'s squared norm:
/// one pass for inner-product and cosine distances.
pub fn dot_norm_f32_le(q: &[f32], v: &[u8]) -> (f32, f32) {
    debug_assert_eq!(v.len(), 4 * q.len());
    let (gq, gv, tq, tv) = groups(q, v);
    let (mut dot, mut norm) = ([0f32; LANES], [0f32; LANES]);
    for (cq, cv) in gq.iter().zip(gv) {
        for i in 0..LANES {
            let x = le(cv, i);
            dot[i] += cq[i] * x;
            norm[i] += x * x;
        }
    }
    let (mut d, mut n) = (tree(dot), tree(norm));
    for (j, y) in tq.iter().enumerate() {
        let x = le(tv, j);
        d += y * x;
        n += x * x;
    }
    (d, n)
}

/// The squared norm of `q`, in the same order as [`dot_norm_f32_le`]'s norm.
pub fn norm2_f32(q: &[f32]) -> f32 {
    let (g, t) = q.as_chunks::<LANES>();
    let mut acc = [0f32; LANES];
    for c in g {
        for i in 0..LANES {
            acc[i] += c[i] * c[i];
        }
    }
    let mut sum = tree(acc);
    for x in t {
        sum += x * x;
    }
    sum
}

/// The squared norm of the little-endian `f32` vector `v`, in [`norm2_f32`]'s order: the
/// same bits as `norm2_f32` of its components, and as [`dot_norm_f32_le`]'s norm.
pub fn norm2_f32_le(v: &[u8]) -> f32 {
    let (g, t) = v.as_chunks::<{ 4 * LANES }>();
    let mut acc = [0f32; LANES];
    for c in g {
        for (i, a) in acc.iter_mut().enumerate() {
            let x = le(c, i);
            *a += x * x;
        }
    }
    let mut sum = tree(acc);
    for c in t.as_chunks::<4>().0 {
        let x = f32::from_le_bytes(*c);
        sum += x * x;
    }
    sum
}

/// The `i`-th little-endian `u16` of `bytes`.
#[inline(always)]
fn le16(bytes: &[u8], i: usize) -> u16 {
    u16::from_le_bytes([bytes[2 * i], bytes[2 * i + 1]])
}

/// The half-precision kernels (#1263): the `f32` kernels with each stored lane widened to
/// `f32` (`$widen`) where they read an `f32`, the same 16 lanes, pairwise tree and tail order.
/// So each equals its `f32` kernel over the widened vector, bit for bit.
macro_rules! half_kernels {
    ($elem:literal, $widen:path, $l2:ident, $dot_norm:ident, $norm2:ident) => {
        #[doc = concat!("[`l2_f32_le`] over a vector of little-endian `", $elem, "` bits.")]
        pub fn $l2(q: &[f32], v: &[u8]) -> f32 {
            debug_assert_eq!(v.len(), 2 * q.len());
            let (gq, tq) = q.as_chunks::<LANES>();
            let (gv, tv) = v.as_chunks::<{ 2 * LANES }>();
            let mut acc = [0f32; LANES];
            for (cq, cv) in gq.iter().zip(gv) {
                for i in 0..LANES {
                    let d = cq[i] - $widen(le16(cv, i));
                    acc[i] += d * d;
                }
            }
            let mut sum = tree(acc);
            for (j, x) in tq.iter().enumerate() {
                let d = x - $widen(le16(tv, j));
                sum += d * d;
            }
            sum
        }

        #[doc = concat!("[`dot_norm_f32_le`] over a vector of little-endian `", $elem, "` bits.")]
        pub fn $dot_norm(q: &[f32], v: &[u8]) -> (f32, f32) {
            debug_assert_eq!(v.len(), 2 * q.len());
            let (gq, tq) = q.as_chunks::<LANES>();
            let (gv, tv) = v.as_chunks::<{ 2 * LANES }>();
            let (mut dot, mut norm) = ([0f32; LANES], [0f32; LANES]);
            for (cq, cv) in gq.iter().zip(gv) {
                for i in 0..LANES {
                    let x = $widen(le16(cv, i));
                    dot[i] += cq[i] * x;
                    norm[i] += x * x;
                }
            }
            let (mut d, mut n) = (tree(dot), tree(norm));
            for (j, y) in tq.iter().enumerate() {
                let x = $widen(le16(tv, j));
                d += y * x;
                n += x * x;
            }
            (d, n)
        }

        #[doc = concat!("[`norm2_f32`] of a vector of little-endian `", $elem, "` bits, widened.")]
        pub fn $norm2(v: &[u8]) -> f32 {
            let (g, t) = v.as_chunks::<{ 2 * LANES }>();
            let mut acc = [0f32; LANES];
            for c in g {
                for (i, a) in acc.iter_mut().enumerate() {
                    let x = $widen(le16(c, i));
                    *a += x * x;
                }
            }
            let mut sum = tree(acc);
            for c in t.as_chunks::<2>().0 {
                let x = $widen(u16::from_le_bytes(*c));
                sum += x * x;
            }
            sum
        }
    };
}

half_kernels!(
    "f16",
    crate::half::f16_to_f32,
    l2_f16_le,
    dot_norm_f16_le,
    norm2_f16_le
);
half_kernels!(
    "bf16",
    crate::half::bf16_to_f32,
    l2_bf16_le,
    dot_norm_bf16_le,
    norm2_bf16_le
);

/// Cosine distance `1 - (q·v) / (|q| |v|)` from [`dot_norm_f32_le`]'s parts and `q`'s
/// squared norm. NaN when either norm is zero (the caller refuses or skips those).
pub fn cosine_from(dot: f32, q_norm2: f32, v_norm2: f32) -> f32 {
    1.0 - dot / (q_norm2.sqrt() * v_norm2.sqrt())
}

/// A signed byte: an `i8`, or a stored byte read as one (a vector's `i8` payload), so the
/// exact integer kernels take either without reinterpreting a slice.
pub trait SignedByte: Copy {
    /// The byte's value.
    fn value(self) -> i32;
}

impl SignedByte for i8 {
    #[inline(always)]
    fn value(self) -> i32 {
        i32::from(self)
    }
}

impl SignedByte for u8 {
    #[inline(always)]
    fn value(self) -> i32 {
        i32::from(self as i8)
    }
}

/// Squared Euclidean distance between two `i8` vectors (either as `i8`s or as the bytes of a
/// stored `i8` vector), exact.
pub fn l2_i8<A: SignedByte, B: SignedByte>(a: &[A], b: &[B]) -> u32 {
    debug_assert_eq!(a.len(), b.len());
    let ((ga, ta), (gb, tb)) = (a.as_chunks::<LANES>(), b.as_chunks::<LANES>());
    let sq = |x: A, y: B| {
        let d = (x.value() - y.value()).unsigned_abs();
        d * d
    };
    let mut acc = [0u32; LANES];
    for (ca, cb) in ga.iter().zip(gb) {
        for i in 0..LANES {
            acc[i] += sq(ca[i], cb[i]);
        }
    }
    acc.iter().sum::<u32>() + ta.iter().zip(tb).map(|(x, y)| sq(*x, *y)).sum::<u32>()
}

/// The inner product of two `i8` vectors (either as `i8`s or as stored bytes), exact.
pub fn dot_i8<A: SignedByte, B: SignedByte>(a: &[A], b: &[B]) -> i32 {
    debug_assert_eq!(a.len(), b.len());
    let ((ga, ta), (gb, tb)) = (a.as_chunks::<LANES>(), b.as_chunks::<LANES>());
    let mut acc = [0i32; LANES];
    for (ca, cb) in ga.iter().zip(gb) {
        for i in 0..LANES {
            acc[i] += ca[i].value() * cb[i].value();
        }
    }
    acc.iter().sum::<i32>()
        + ta.iter()
            .zip(tb)
            .map(|(x, y)| x.value() * y.value())
            .sum::<i32>()
}

/// Hamming distance between two bit vectors of equal length (bytes, any bit order).
pub fn hamming(a: &[u8], b: &[u8]) -> u32 {
    debug_assert_eq!(a.len(), b.len());
    let (wa, ra) = a.as_chunks::<8>();
    let (wb, rb) = b.as_chunks::<8>();
    let words: u32 = wa
        .iter()
        .zip(wb)
        .map(|(x, y)| (u64::from_le_bytes(*x) ^ u64::from_le_bytes(*y)).count_ones())
        .sum();
    words
        + ra.iter()
            .zip(rb)
            .map(|(x, y)| (x ^ y).count_ones())
            .sum::<u32>()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(v: &[f32]) -> Vec<u8> {
        v.iter().flat_map(|x| x.to_le_bytes()).collect()
    }

    /// An `f64` reference, for the accuracy bounds.
    fn l2_ref(a: &[f32], b: &[f32]) -> f64 {
        a.iter()
            .zip(b)
            .map(|(x, y)| (f64::from(*x) - f64::from(*y)).powi(2))
            .sum()
    }

    fn rnd(seed: &mut u64, n: usize) -> Vec<f32> {
        (0..n)
            .map(|_| {
                *seed ^= *seed << 13;
                *seed ^= *seed >> 7;
                *seed ^= *seed << 17;
                ((*seed >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
            })
            .collect()
    }

    #[test]
    fn f32_kernels_agree_with_an_f64_reference_for_every_length_and_are_deterministic() {
        let mut seed = 0x9E37_79B9_7F4A_7C15;
        for n in [1usize, 3, 15, 16, 17, 31, 64, 100, 128, 769] {
            let (q, v) = (rnd(&mut seed, n), rnd(&mut seed, n));
            let vb = bytes(&v);
            let l2 = l2_f32_le(&q, &vb);
            let want = l2_ref(&q, &v);
            assert!(
                (f64::from(l2) - want).abs() <= 1e-5 * want.max(1.0),
                "n = {n}"
            );
            assert_eq!(
                l2.to_bits(),
                l2_f32_le(&q, &vb).to_bits(),
                "same bits again"
            );
            let (dot, norm) = dot_norm_f32_le(&q, &vb);
            let dot_ref: f64 = q
                .iter()
                .zip(&v)
                .map(|(a, b)| f64::from(*a) * f64::from(*b))
                .sum();
            assert!((f64::from(dot) - dot_ref).abs() <= 1e-4, "n = {n}");
            assert_eq!(norm.to_bits(), norm2_f32(&v).to_bits(), "one norm order");
            assert_eq!(
                norm2_f32_le(&vb).to_bits(),
                norm.to_bits(),
                "from the stored bytes"
            );
            let cos = cosine_from(dot, norm2_f32(&q), norm);
            assert!((-1e-6..=2.0 + 1e-6).contains(&cos), "n = {n}: {cos}");
        }
        assert_eq!(l2_f32_le(&[1.0, 2.0], &bytes(&[1.0, 2.0])), 0.0);
        assert!(
            cosine_from(0.0, 0.0, 1.0).is_nan(),
            "a zero norm is NaN, for the caller"
        );
    }

    /// #1263: each half kernel equals its `f32` kernel over the widened vector, bit for bit,
    /// for random and boundary payloads (subnormals, ±0, the extremes) at dims around the lane
    /// width and at 4096.
    #[test]
    fn half_kernels_equal_the_f32_kernels_over_widened_vectors() {
        use crate::half::{bf16_to_f32, f16_to_f32};
        let mut seed = 0x1263_0001_u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let boundary: [u16; 8] = [
            0x0000, 0x8000, 0x0001, 0x03ff, 0x0400, 0x7bff, 0xfbff, 0x3c00,
        ];
        for n in [1usize, 15, 16, 17, 64, 4096] {
            for (is_f16, widen, lo, hi) in [
                (true, f16_to_f32 as fn(u16) -> f32, 0x0000u16, 0x7bffu16),
                (false, bf16_to_f32 as fn(u16) -> f32, 0x0000u16, 0x7f7fu16),
            ] {
                let bits: Vec<u16> = (0..n)
                    .map(|i| {
                        if i < boundary.len() && is_f16 {
                            boundary[i]
                        } else {
                            let mag = lo + (next() % u64::from(hi - lo)) as u16;
                            mag | if next() & 1 == 0 { 0 } else { 0x8000 }
                        }
                    })
                    .collect();
                let half: Vec<u8> = bits.iter().flat_map(|b| b.to_le_bytes()).collect();
                let wide: Vec<f32> = bits.iter().map(|b| widen(*b)).collect();
                let wide_le = bytes(&wide);
                let q = rnd(&mut next(), n);
                let (l2, dn, n2) = if is_f16 {
                    (
                        l2_f16_le(&q, &half),
                        dot_norm_f16_le(&q, &half),
                        norm2_f16_le(&half),
                    )
                } else {
                    (
                        l2_bf16_le(&q, &half),
                        dot_norm_bf16_le(&q, &half),
                        norm2_bf16_le(&half),
                    )
                };
                let want_dn = dot_norm_f32_le(&q, &wide_le);
                assert_eq!(l2.to_bits(), l2_f32_le(&q, &wide_le).to_bits(), "l2 n={n}");
                assert_eq!(
                    (dn.0.to_bits(), dn.1.to_bits()),
                    (want_dn.0.to_bits(), want_dn.1.to_bits()),
                    "dot/norm n={n}"
                );
                assert_eq!(n2.to_bits(), norm2_f32(&wide).to_bits(), "norm2 n={n}");
            }
        }
    }

    #[test]
    fn integer_and_bit_kernels_are_exact() {
        let a: Vec<i8> = (0..37).map(|i| (i * 7 - 100) as i8).collect();
        let b: Vec<i8> = (0..37).map(|i| (100 - i * 5) as i8).collect();
        let l2: u32 = a
            .iter()
            .zip(&b)
            .map(|(x, y)| (i32::from(*x) - i32::from(*y)).pow(2) as u32)
            .sum();
        let dot: i32 = a
            .iter()
            .zip(&b)
            .map(|(x, y)| i32::from(*x) * i32::from(*y))
            .sum();
        assert_eq!((l2_i8(&a, &b), dot_i8(&a, &b)), (l2, dot));
        let stored: Vec<u8> = b.iter().map(|x| *x as u8).collect();
        assert_eq!(
            (l2_i8(&a, &stored), dot_i8(&a, &stored)),
            (l2, dot),
            "a stored i8 vector's bytes score as its i8s"
        );
        let extremes = [i8::MIN; 4096];
        let other = [i8::MAX; 4096];
        assert_eq!(
            l2_i8(&extremes, &other),
            4096 * 255 * 255,
            "no overflow at 4096 dims"
        );
        let x: Vec<u8> = (0..19).map(|i| i * 13).collect();
        let y: Vec<u8> = (0..19).map(|i| !(i * 13) ^ (i & 1)).collect();
        let want: u32 = x.iter().zip(&y).map(|(p, q)| (p ^ q).count_ones()).sum();
        assert_eq!(hamming(&x, &y), want);
    }
}
