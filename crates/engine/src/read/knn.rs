//! Exact k-nearest-neighbour search, D296's first consumer (#490 A and C): a [`KnnVisitor`]
//! scores each resolved cell's vector against a query with the `pigeonhole_format::distance`
//! kernels, and a [`super::reduce::TopK`] of [`Score`]s keeps the k nearest.
//!
//! A typed visitor ([`KnnVisitor::typed`]) reads a vector family's values (D303, tag `0x04`)
//! and scores them with [`Params::distance`], the same function `VectorSpec::distance_*`
//! uses, so a ranking and a client's own scoring agree exactly. The engine-level fallback
//! ([`KnnVisitor::new`]) reads `Bytes` values of little-endian `f32`s (D296 §2.4; never part
//! of the public API). A cell whose value doesn't fit is skipped and counted, as is one whose
//! distance is NaN, so every ranked distance is finite and ordered.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pigeonhole_format::distance::{cosine_from, dot_norm_f32_le, l2_f32_le, norm2_f32};
use pigeonhole_format::value::{ValueRef, decode_value};
use pigeonhole_format::vector::{Params, Query};
use pigeonhole_format::{FamilyId, Timestamp};

use super::reduce::{CellVisitor, Visit};

/// How a distance is measured. Smaller is nearer for every metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Metric {
    /// Squared Euclidean distance.
    L2,
    /// The negated inner product (the largest product ranks first).
    InnerProduct,
    /// `1 - cosine similarity`.
    Cosine,
}

/// A distance as a totally ordered value: [`f64::total_cmp`] order (D296 R1). The visitor
/// never emits NaN, so this is the numeric order of the distances it ranks. `f64`, so `i8`
/// and bit distances stay exact integers and an `f32` distance widens exactly (#490 D).
#[derive(Debug, Clone, Copy)]
pub struct Score(pub f64);

impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0).is_eq()
    }
}

impl Eq for Score {}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Cells a KNN read skipped, over every group of the read.
#[derive(Debug, Default)]
pub struct KnnSkips {
    /// Values that are not a vector of the query's shape.
    pub shape: AtomicU64,
    /// Distances that came out NaN (a zero vector under cosine, a NaN component).
    pub nan: AtomicU64,
}

impl KnnSkips {
    /// `(shape, nan)`.
    pub fn counts(&self) -> (u64, u64) {
        (
            self.shape.load(Ordering::Relaxed),
            self.nan.load(Ordering::Relaxed),
        )
    }
}

/// Scores each cell's vector against a query: a [`CellVisitor`] for
/// [`Engine::reduce`](crate::Engine::reduce) with a `TopK<Score>`. Not `Clone`: each
/// group's visitor folds its own skip counts into the shared [`KnnSkips`] when it drops.
#[derive(Debug)]
pub struct KnnVisitor {
    query: Target,
    skips: Arc<KnnSkips>,
    /// Skips counted locally, folded into `skips` when the visitor drops (one atomic add
    /// per group, not per cell).
    shape: u64,
    nan: u64,
}

/// A typed query, in the vector family's element type.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum KnnQuery {
    /// `f32` components.
    F32(Arc<[f32]>),
    /// `i8` components.
    I8(Arc<[i8]>),
    /// Packed bits.
    Bits(Arc<[u8]>),
}

impl KnnQuery {
    fn as_query(&self) -> Query<'_> {
        match self {
            KnnQuery::F32(q) => Query::F32(q),
            KnnQuery::I8(q) => Query::I8(q),
            KnnQuery::Bits(q) => Query::Bits(q),
        }
    }
}

/// What a visitor scores against.
#[derive(Debug)]
enum Target {
    /// The engine-level fallback: `Bytes` values of little-endian `f32`s.
    Bytes {
        query: Arc<[f32]>,
        q_norm2: f32,
        metric: Metric,
    },
    /// A vector family's values (D303).
    Typed { params: Params, query: KnnQuery },
}

impl KnnVisitor {
    /// A visitor for `query` under `metric` over `Bytes` values of little-endian `f32`s (the
    /// engine-level fallback), counting what it skips into `skips` (shared by every group's
    /// visitor).
    pub fn new(query: Arc<[f32]>, metric: Metric, skips: Arc<KnnSkips>) -> Self {
        let q_norm2 = norm2_f32(&query);
        Self::with(
            Target::Bytes {
                query,
                q_norm2,
                metric,
            },
            skips,
        )
    }

    /// A visitor for a vector family of `params` (D303): `query` must fit them (its element
    /// type and dimension, finite, and not all zeros under cosine), else `InvalidArgument`.
    pub fn typed(params: Params, query: KnnQuery, skips: Arc<KnnSkips>) -> crate::Result<Self> {
        params
            .check(query.as_query())
            .map_err(|e| crate::Error::InvalidArgument(format!("the query: {e}")))?;
        Ok(Self::with(Target::Typed { params, query }, skips))
    }

    fn with(query: Target, skips: Arc<KnnSkips>) -> Self {
        Self {
            query,
            skips,
            shape: 0,
            nan: 0,
        }
    }

    /// The distance from the query to the stored value `value` (tag included), or `None` if
    /// the value is not a vector of the query's shape.
    pub fn distance(&self, value: &[u8]) -> Option<f64> {
        match &self.query {
            Target::Bytes {
                query,
                q_norm2,
                metric,
            } => {
                let Ok(ValueRef::Bytes(v)) = decode_value(value) else {
                    return None;
                };
                if v.len() != 4 * query.len() {
                    return None;
                }
                Some(f64::from(match metric {
                    Metric::L2 => l2_f32_le(query, v),
                    Metric::InnerProduct => -dot_norm_f32_le(query, v).0,
                    Metric::Cosine => {
                        let (dot, n2) = dot_norm_f32_le(query, v);
                        cosine_from(dot, *q_norm2, n2)
                    }
                }))
            }
            Target::Typed { params, query } => {
                let Ok(ValueRef::Vector(v)) = decode_value(value) else {
                    return None;
                };
                params
                    .accepts(&v)
                    .then(|| params.distance(query.as_query(), &v))
            }
        }
    }
}

impl Drop for KnnVisitor {
    fn drop(&mut self) {
        if self.shape > 0 {
            self.skips.shape.fetch_add(self.shape, Ordering::Relaxed);
        }
        if self.nan > 0 {
            self.skips.nan.fetch_add(self.nan, Ordering::Relaxed);
        }
    }
}

impl CellVisitor for KnnVisitor {
    type Item = Score;

    fn visit(
        &mut self,
        _: &[u8],
        _: FamilyId,
        _: &[u8],
        _: Timestamp,
        value: &[u8],
    ) -> Visit<Score> {
        match self.distance(value) {
            None => {
                self.shape += 1;
                Visit::Skip
            }
            Some(d) if d.is_nan() => {
                self.nan += 1;
                Visit::Skip
            }
            Some(d) => Visit::Emit(Score(d)),
        }
    }
}
