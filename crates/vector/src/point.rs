//! Centroid math (D306 §3, rev 2's S1 and S2): vectors as points, means, and the 2-means a
//! split runs. Every distance is [`VectorSpec`]'s, the one `nearest_*` ranks by.

use pigeonhole::{Element, Metric, Table, Transaction, Vector, VectorRef, VectorSpec};

use crate::{Error, Result};

/// A vector as centroid math sees it: `f32` components (an `i8` vector widened, which is
/// exact), or packed bits.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Point {
    F32(Vec<f32>),
    Bits(Vec<u8>),
}

impl Point {
    pub(crate) fn of(v: VectorRef<'_>) -> Self {
        match v.to_f32_vec() {
            Some(x) => Point::F32(x),
            None => Point::Bits(v.as_bytes().to_vec()),
        }
    }

    /// The point as a centroid: an `f32` vector, or a bit vector of `dim` bits.
    pub(crate) fn to_vector(&self, dim: u32) -> Result<Vector> {
        Ok(match self {
            Point::F32(x) => Vector::from_f32(x)?,
            Point::Bits(b) => Vector::from_bits(dim, b)?,
        })
    }
}

/// A caller's vector, borrowed.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Input<'a> {
    F32(&'a [f32]),
    I8(&'a [i8]),
    Bits(&'a [u8]),
}

impl Input<'_> {
    /// Refuses a vector of another element type or dimension than `spec`'s. The components
    /// are checked by the write (or the search) itself.
    pub(crate) fn check(&self, spec: &VectorSpec) -> Result<()> {
        let (element, dim) = match self {
            Input::F32(x) => (Element::F32, x.len()),
            Input::I8(x) => (Element::I8, x.len()),
            Input::Bits(x) => (Element::Bit, 8 * x.len()),
        };
        if element != spec.element() || dim != spec.dim() as usize {
            return Err(Error::InvalidArgument(format!(
                "the index holds vectors of {} {:?}, not {dim} {element:?}",
                spec.dim(),
                spec.element()
            )));
        }
        Ok(())
    }

    pub(crate) fn point(&self) -> Point {
        match self {
            Input::F32(x) => Point::F32(x.to_vec()),
            Input::I8(x) => Point::F32(x.iter().map(|v| f32::from(*v)).collect()),
            Input::Bits(x) => Point::Bits(x.to_vec()),
        }
    }

    pub(crate) fn put(&self, tx: &mut Transaction, t: &Table, row: &[u8], id: &[u8]) {
        match self {
            Input::F32(x) => tx.put_vector_f32(t, row, crate::layout::VEC, id, x),
            Input::I8(x) => tx.put_vector_i8(t, row, crate::layout::VEC, id, x),
            Input::Bits(x) => tx.put_vector_bits(t, row, crate::layout::VEC, id, x),
        };
    }
}

/// A stored vector, copied out of its cell so it can be written elsewhere (a move).
#[derive(Debug, Clone)]
pub(crate) enum Stored {
    F32(Vec<f32>),
    I8(Vec<i8>),
    Bits(Vec<u8>),
}

impl Stored {
    pub(crate) fn of(v: VectorRef<'_>) -> Result<Self> {
        Ok(match v.element() {
            Element::F32 => Stored::F32(v.to_f32_vec().unwrap_or_default()),
            Element::I8 => Stored::I8(v.as_bytes().iter().map(|b| *b as i8).collect()),
            Element::Bit => Stored::Bits(v.as_bytes().to_vec()),
            other => {
                return Err(Error::Corruption(format!(
                    "a list holds a vector of element {other:?}"
                )));
            }
        })
    }

    pub(crate) fn input(&self) -> Input<'_> {
        match self {
            Stored::F32(x) => Input::F32(x),
            Stored::I8(x) => Input::I8(x),
            Stored::Bits(x) => Input::Bits(x),
        }
    }
}

/// `p`'s distance to the centroid `c`, under the centroid family's spec (`f32` for `f32`
/// and `i8` indexes, bits for bit indexes; the index's metric).
pub(crate) fn distance(cen: &VectorSpec, p: &Point, c: &Vector) -> Result<f64> {
    Ok(match p {
        Point::F32(q) => cen.distance_f32(q, c.as_ref())?,
        Point::Bits(q) => cen.distance_bits(q, c.as_ref())?,
    })
}

/// A mean whose norm is below this is degenerate under cosine (opposed clusters, rev 2 S1).
const MIN_COSINE_NORM: f64 = 1e-6;

fn norm(x: &[f32]) -> f64 {
    x.iter()
        .map(|v| f64::from(*v) * f64::from(*v))
        .sum::<f64>()
        .sqrt()
}

fn scaled(x: &[f32], by: f64) -> Point {
    Point::F32(x.iter().map(|v| (f64::from(*v) * by) as f32).collect())
}

/// The point as a centroid under `metric`: scaled to unit length under cosine (spherical
/// k-means), else as is. A stored cosine vector's norm is never zero (D303).
pub(crate) fn unit(metric: Metric, p: Point) -> Point {
    match (metric, p) {
        (Metric::Cosine, Point::F32(x)) if norm(&x) > 0.0 => scaled(&x, 1.0 / norm(&x)),
        (_, p) => p,
    }
}

/// The centroid of `members` under `metric`: the mean (of unit vectors under cosine, then
/// normalised), or for bits the per-bit majority with ties going to 0. `None` for no
/// members, or a cosine mean too short to give a direction.
pub(crate) fn mean<'p>(metric: Metric, members: impl Iterator<Item = &'p Point>) -> Option<Point> {
    let mut sum: Vec<f64> = Vec::new();
    let mut bits: Vec<u32> = Vec::new();
    let mut n = 0u32;
    let mut is_bits = false;
    for p in members {
        n += 1;
        match p {
            Point::F32(x) => {
                sum.resize(x.len(), 0.0);
                let scale = match metric {
                    Metric::Cosine if norm(x) > 0.0 => 1.0 / norm(x),
                    _ => 1.0,
                };
                for (s, v) in sum.iter_mut().zip(x) {
                    *s += f64::from(*v) * scale;
                }
            }
            Point::Bits(b) => {
                is_bits = true;
                bits.resize(8 * b.len(), 0);
                for (i, c) in bits.iter_mut().enumerate() {
                    *c += u32::from(b[i / 8] >> (i % 8) & 1);
                }
            }
        }
    }
    if n == 0 {
        return None;
    }
    if is_bits {
        let mut out = vec![0u8; bits.len() / 8];
        for (i, c) in bits.iter().enumerate() {
            if 2 * c > n {
                out[i / 8] |= 1 << (i % 8);
            }
        }
        return Some(Point::Bits(out));
    }
    let m: Vec<f32> = sum.iter().map(|s| (s / f64::from(n)) as f32).collect();
    if metric != Metric::Cosine {
        return Some(Point::F32(m));
    }
    let n = norm(&m);
    (n >= MIN_COSINE_NORM).then(|| scaled(&m, 1.0 / n))
}

/// A split's plan: the two centroids, and which members go to the new list.
#[derive(Debug)]
pub(crate) struct Plan {
    /// The split list's new centroid.
    pub(crate) stay: Point,
    /// The new list's centroid.
    pub(crate) go: Point,
    /// Indexes into the members of those that move to the new list.
    pub(crate) moving: Vec<usize>,
}

/// Lloyd iterations of a split's 2-means.
const ITERATIONS: usize = 10;

/// Splits `members` (in qualifier order) into two clusters with 2-means (D306 §3), seeded
/// deterministically by farthest points from `current`, the list's centroid. Members
/// nearer the second centroid move; a tie stays. A side that empties, or a cosine mean
/// with no direction, is re-seeded from the member farthest from the other centroid.
///
/// Each side ends with at least `min` members (the merge bound), so a split never makes a
/// list that merges straight back: when 2-means leaves a side short (an outlier cluster),
/// the other side's members with the smallest margin move to it.
///
/// When every member is at the same point, or the clusters don't separate, it splits by
/// order instead (the second half moves, both lists keep `current`), so a list of
/// duplicates still stays within its bound.
pub(crate) fn two_means(
    cen: &VectorSpec,
    members: &[Point],
    current: &Point,
    min: usize,
) -> Result<Plan> {
    let metric = cen.metric();
    let dim = cen.dim();
    let by_order = || Plan {
        stay: current.clone(),
        go: current.clone(),
        moving: (members.len() / 2..members.len()).collect(),
    };
    if members.len() < 2 {
        return Ok(by_order());
    }
    let farthest = |from: &Point| -> Result<(usize, f64)> {
        let c = from.to_vector(dim)?;
        let mut best = (0, f64::NEG_INFINITY);
        for (i, p) in members.iter().enumerate() {
            let d = distance(cen, p, &c)?;
            if d > best.1 {
                best = (i, d);
            }
        }
        Ok(best)
    };
    let seed = |i: usize| unit(metric, members[i].clone());
    let (a, _) = farthest(current)?;
    let (b, d) = farthest(&members[a])?;
    let (mut stay, mut go) = (seed(a), seed(b));
    if d <= 0.0 || stay == go {
        return Ok(by_order());
    }
    let mut side = vec![false; members.len()];
    for round in 0..ITERATIONS {
        let (cs, cg) = (stay.to_vector(dim)?, go.to_vector(dim)?);
        let mut changed = round == 0;
        for (i, p) in members.iter().enumerate() {
            let moves = distance(cen, p, &cg)? < distance(cen, p, &cs)?;
            changed |= moves != side[i];
            side[i] = moves;
        }
        if !changed {
            break;
        }
        let pick = |go_side: bool| {
            members
                .iter()
                .zip(&side)
                .filter(move |(_, s)| **s == go_side)
                .map(|(p, _)| p)
        };
        stay = match mean(metric, pick(false)) {
            Some(m) => m,
            None => seed(farthest(&go)?.0),
        };
        go = match mean(metric, pick(true)) {
            Some(m) => m,
            None => seed(farthest(&stay)?.0),
        };
    }
    let going = side.iter().filter(|s| **s).count();
    if 2 * min > members.len() || going == 0 || going == members.len() {
        return Ok(by_order());
    }
    let short = if going < min {
        Some(true)
    } else {
        (members.len() - going < min).then_some(false)
    };
    if let Some(to) = short {
        // Move the long side's members nearest the short side's centroid, by margin.
        let (ct, cf) = if to {
            (go.to_vector(dim)?, stay.to_vector(dim)?)
        } else {
            (stay.to_vector(dim)?, go.to_vector(dim)?)
        };
        let mut margins = Vec::new();
        for (i, p) in members.iter().enumerate().filter(|(i, _)| side[*i] != to) {
            margins.push((distance(cen, p, &ct)? - distance(cen, p, &cf)?, i));
        }
        margins.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let have = if to { going } else { members.len() - going };
        for (_, i) in margins.into_iter().take(min - have) {
            side[i] = to;
        }
        let pick = |go_side: bool| {
            members
                .iter()
                .zip(&side)
                .filter(move |(_, s)| **s == go_side)
                .map(|(p, _)| p)
        };
        if let Some(m) = mean(metric, pick(false)) {
            stay = m;
        }
        if let Some(m) = mean(metric, pick(true)) {
            go = m;
        }
    }
    let moving: Vec<usize> = (0..members.len()).filter(|i| side[*i]).collect();
    Ok(Plan { stay, go, moving })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bit_majority_tie_goes_to_zero() {
        let a = Point::Bits(vec![0b0000_0011]);
        let b = Point::Bits(vec![0b0000_0001]);
        // Bit 0: 2 of 2 → 1. Bit 1: 1 of 2, a tie → 0.
        assert_eq!(
            mean(Metric::Hamming, [&a, &b].into_iter()),
            Some(Point::Bits(vec![1]))
        );
    }

    #[test]
    fn opposed_cosine_clusters_get_unit_non_zero_centroids() {
        let cen = VectorSpec::new(2, Element::F32, Metric::Cosine).unwrap();
        let mut members = Vec::new();
        for i in 0..20 {
            let e = 0.01 * i as f32;
            members.push(Point::F32(vec![1.0, e]));
            members.push(Point::F32(vec![-1.0, -e]));
        }
        // The mean of the whole list has no direction; the split still separates it.
        assert_eq!(mean(Metric::Cosine, members.iter()), None);
        let plan = two_means(&cen, &members, &Point::F32(vec![0.0, 1.0]), 1).unwrap();
        assert_eq!(plan.moving.len(), 20);
        for c in [&plan.stay, &plan.go] {
            let Point::F32(x) = c else { panic!() };
            let n: f64 = x.iter().map(|v| f64::from(*v).powi(2)).sum();
            assert!((n - 1.0).abs() < 1e-6, "{x:?}");
        }
    }

    #[test]
    fn an_outlier_cluster_is_topped_up_to_the_merge_bound() {
        let cen = VectorSpec::new(1, Element::F32, Metric::L2).unwrap();
        // Nineteen near 0 and one far outlier: plain 2-means moves the outlier alone.
        let mut members: Vec<Point> = (0..19).map(|i| Point::F32(vec![i as f32 * 0.01])).collect();
        members.push(Point::F32(vec![100.0]));
        let plan = two_means(&cen, &members, &Point::F32(vec![0.0]), 5).unwrap();
        // One side is the outlier and the four members nearest it, whichever side moves.
        let small: Vec<usize> = if plan.moving.len() == 5 {
            plan.moving.clone()
        } else {
            (0..20).filter(|i| !plan.moving.contains(i)).collect()
        };
        assert_eq!(small, vec![15, 16, 17, 18, 19], "{:?}", plan.moving);
    }

    #[test]
    fn duplicates_split_by_order() {
        let cen = VectorSpec::new(2, Element::F32, Metric::L2).unwrap();
        let members = vec![Point::F32(vec![1.0, 1.0]); 9];
        let plan = two_means(&cen, &members, &Point::F32(vec![1.0, 1.0]), 1).unwrap();
        assert_eq!(plan.moving, (4..9).collect::<Vec<_>>());
    }

    #[test]
    fn two_means_separates_two_clusters() {
        let cen = VectorSpec::new(2, Element::F32, Metric::L2).unwrap();
        let mut members = Vec::new();
        for i in 0..10 {
            let e = i as f32 * 0.01;
            members.push(Point::F32(vec![e, 0.0]));
            members.push(Point::F32(vec![10.0 + e, 10.0]));
        }
        let plan = two_means(&cen, &members, &Point::F32(vec![5.0, 5.0]), 1).unwrap();
        let moved: Vec<bool> = (0..20).map(|i| plan.moving.contains(&i)).collect();
        // One cluster moves whole, the other stays whole.
        assert!(moved.iter().step_by(2).all(|m| *m == moved[0]));
        assert!(moved.iter().skip(1).step_by(2).all(|m| *m == moved[1]));
        assert_ne!(moved[0], moved[1]);
    }
}
