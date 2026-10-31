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

/// Cosine distance `1 - (q·v) / (|q| |v|)` from [`dot_norm_f32_le`]'s parts and `q`'s
/// squared norm. NaN when either norm is zero (the caller refuses or skips those).
pub fn cosine_from(dot: f32, q_norm2: f32, v_norm2: f32) -> f32 {
    1.0 - dot / (q_norm2.sqrt() * v_norm2.sqrt())
}

/// Squared Euclidean distance between two `i8` vectors, exact.
pub fn l2_i8(a: &[i8], b: &[i8]) -> u32 {
    debug_assert_eq!(a.len(), b.len());
    let ((ga, ta), (gb, tb)) = (a.as_chunks::<LANES>(), b.as_chunks::<LANES>());
    let sq = |x: i8, y: i8| {
        let d = (i32::from(x) - i32::from(y)).unsigned_abs();
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

/// The inner product of two `i8` vectors, exact.
pub fn dot_i8(a: &[i8], b: &[i8]) -> i32 {
    debug_assert_eq!(a.len(), b.len());
    let ((ga, ta), (gb, tb)) = (a.as_chunks::<LANES>(), b.as_chunks::<LANES>());
    let mut acc = [0i32; LANES];
    for (ca, cb) in ga.iter().zip(gb) {
        for i in 0..LANES {
            acc[i] += i32::from(ca[i]) * i32::from(cb[i]);
        }
    }
    acc.iter().sum::<i32>()
        + ta.iter()
            .zip(tb)
            .map(|(x, y)| i32::from(*x) * i32::from(*y))
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
            let cos = cosine_from(dot, norm2_f32(&q), norm);
            assert!((-1e-6..=2.0 + 1e-6).contains(&cos), "n = {n}: {cos}");
        }
        assert_eq!(l2_f32_le(&[1.0, 2.0], &bytes(&[1.0, 2.0])), 0.0);
        assert!(
            cosine_from(0.0, 0.0, 1.0).is_nan(),
            "a zero norm is NaN, for the caller"
        );
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
