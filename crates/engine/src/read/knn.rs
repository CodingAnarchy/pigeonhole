//! Exact k-nearest-neighbour search, D296's first consumer (#490 A and C): a [`KnnVisitor`]
//! scores each resolved cell's vector against a query with the `pigeonhole_format::distance`
//! kernels, and a [`super::reduce::TopK`] of [`Score`]s keeps the k nearest.
//!
//! Until typed vector families (#490 B, D303) land, a vector is a `Bytes` value of `dim`
//! little-endian `f32`s (D296 §2.4's engine-level form; never part of the public API). A cell
//! whose value is not one is skipped and counted, as is one whose distance is NaN, so every
//! ranked distance is finite and ordered.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pigeonhole_format::distance::{cosine_from, dot_norm_f32_le, l2_f32_le, norm2_f32};
use pigeonhole_format::value::{ValueRef, decode_value};
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

/// A distance as a totally ordered value: [`f32::total_cmp`] order (D296 R1). The visitor
/// never emits NaN, so this is the numeric order of the distances it ranks.
#[derive(Debug, Clone, Copy)]
pub struct Score(pub f32);

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
    /// Values that are not `dim` little-endian `f32`s.
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
    query: Arc<[f32]>,
    q_norm2: f32,
    metric: Metric,
    skips: Arc<KnnSkips>,
    /// Skips counted locally, folded into `skips` when the visitor drops (one atomic add
    /// per group, not per cell).
    shape: u64,
    nan: u64,
}

impl KnnVisitor {
    /// A visitor for `query` under `metric`, counting what it skips into `skips` (shared by
    /// every group's visitor).
    pub fn new(query: Arc<[f32]>, metric: Metric, skips: Arc<KnnSkips>) -> Self {
        let q_norm2 = norm2_f32(&query);
        Self {
            query,
            q_norm2,
            metric,
            skips,
            shape: 0,
            nan: 0,
        }
    }

    /// The distance from the query to the stored value `value` (tag included), or `None` if
    /// the value is not a vector of the query's dimension.
    pub fn distance(&self, value: &[u8]) -> Option<f32> {
        let Ok(ValueRef::Bytes(v)) = decode_value(value) else {
            return None;
        };
        if v.len() != 4 * self.query.len() {
            return None;
        }
        Some(match self.metric {
            Metric::L2 => l2_f32_le(&self.query, v),
            Metric::InnerProduct => -dot_norm_f32_le(&self.query, v).0,
            Metric::Cosine => {
                let (dot, n2) = dot_norm_f32_le(&self.query, v);
                cosine_from(dot, self.q_norm2, n2)
            }
        })
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
