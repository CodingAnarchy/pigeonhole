//! Exact k-nearest-neighbour search, D296's first consumer (#490 A and C): a [`KnnVisitor`]
//! scores each resolved cell's vector against a query with the `pigeonhole_format::distance`
//! kernels, and a [`super::reduce::TopK`] of [`Score`]s keeps the k nearest.
//!
//! A typed visitor ([`KnnVisitor::typed`]) reads a vector family's values (D303, tag `0x04`)
//! and scores them with `Params::distance`, the same function `VectorSpec::distance_*`
//! uses, so a ranking and a client's own scoring agree exactly. The engine-level fallback
//! ([`KnnVisitor::new`]) reads `Bytes` values of little-endian `f32`s (D296 §2.4; never part
//! of the public API). A cell whose value doesn't fit is skipped and counted, as is one whose
//! distance is NaN, so every ranked distance is finite and ordered.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pigeonhole_format::distance::{cosine_from, dot_norm_f32_le, l2_f32_le, norm2_f32};
use pigeonhole_format::value::{ValueRef, decode_value};
use pigeonhole_format::vector::Prepared;
use pigeonhole_format::{FamilyId, Timestamp};

use super::reduce::{CellVisitor, TopK, Visit};

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

/// What a visitor scores against.
#[derive(Debug)]
enum Target {
    /// The engine-level fallback: `Bytes` values of little-endian `f32`s.
    Bytes {
        query: Arc<[f32]>,
        q_norm2: f32,
        metric: Metric,
    },
    /// A vector family's values (D303), against a query checked and prepared once.
    Typed(Prepared),
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

    /// A visitor for a vector family, scoring with a query prepared for it once
    /// ([`Params::prepare`](pigeonhole_format::vector::Params::prepare): checked, its norm
    /// computed), shared by every group's visitor.
    pub fn typed(query: Prepared, skips: Arc<KnnSkips>) -> Self {
        Self::with(Target::Typed(query), skips)
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
            Target::Typed(query) => {
                let Ok(ValueRef::Vector(v)) = decode_value(value) else {
                    return None;
                };
                query.params().accepts(&v).then(|| query.distance(&v))
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

/// Cells an async nearest-neighbour search reads in one poll before it yields, as an async
/// traversal does ([`TRAVERSE_POLL_CELLS`](crate::TRAVERSE_POLL_CELLS)).
pub const NEAREST_POLL_CELLS: u32 = 4_096;

/// What an async search reads.
enum Source {
    /// A range, through an async scan ([`ScanCursor::poll_next_row`]).
    Scan(super::ScanCursor),
    /// A key set, through a multi-get cursor ([`KeyCursor::poll_next_key`]).
    Keys {
        cursor: super::keys::KeyCursor,
        keys: Arc<super::keys::KeySet>,
    },
}

/// An async exact nearest-neighbour search
/// ([`Engine::nearest_async`](crate::Engine::nearest_async), D296): the `k` nearest
/// vectors of one vector family over a range or a key set, read on the polling task (rows
/// positioned from the cache, their blocks fetched asynchronously) and scored inline with
/// a prepared query. It never fans out; one `TopK` over the input in order gives the sync
/// read's result. A poll yields after [`NEAREST_POLL_CELLS`] cells (a value filter's misses
/// included: it is tested here, not in the read), so a wide row never holds the executor;
/// a row's cells past its first block are read synchronously where the cache misses
/// (counted in `async_sync_reads`). Dropping it releases its pins and its snapshot.
#[must_use = "a future does nothing unless polled"]
pub struct PendingNearest {
    source: Source,
    visitor: KnnVisitor,
    top: TopK<Score>,
    /// The read's value predicate, tested here before the visitor rather than in the read,
    /// so the cells it drops count toward a poll's budget (#1237's rule), with the SSTs to
    /// read a separated value from.
    filter: Option<Filter>,
    /// The row being read (its key in a buffer reused from row to row), if one is.
    row: Vec<u8>,
    in_row: bool,
    done: bool,
}

/// A value predicate and the SSTs its separated values are read from.
type Filter = (super::ValuePredicate, Arc<crate::snapshot::SstSet>);

impl std::fmt::Debug for PendingNearest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingNearest")
            .field("reading", &self.in_row)
            .finish_non_exhaustive()
    }
}

impl PendingNearest {
    pub(crate) fn scan(
        cursor: super::ScanCursor,
        query: Prepared,
        k: usize,
        filter: Option<Filter>,
    ) -> Self {
        Self::with(Source::Scan(cursor), query, k, filter)
    }

    pub(crate) fn keys(
        cursor: super::keys::KeyCursor,
        keys: Arc<super::keys::KeySet>,
        query: Prepared,
        k: usize,
        filter: Option<Filter>,
    ) -> Self {
        Self::with(Source::Keys { cursor, keys }, query, k, filter)
    }

    fn with(source: Source, query: Prepared, k: usize, filter: Option<Filter>) -> Self {
        Self {
            source,
            visitor: KnnVisitor::typed(query, Arc::new(KnnSkips::default())),
            top: TopK::new(k),
            filter,
            row: Vec::new(),
            in_row: false,
            done: false,
        }
    }

    fn step(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<crate::Result<()>> {
        use std::task::Poll;
        let mut budget = NEAREST_POLL_CELLS;
        loop {
            if !self.in_row {
                let row = &mut self.row;
                let found = match &mut self.source {
                    Source::Scan(c) => match c.poll_next_row(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(r) => r?.then(|| {
                            row.clear();
                            row.extend_from_slice(c.row());
                        }),
                    },
                    Source::Keys { cursor, keys } => match cursor.poll_next_key(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(r) => r?.map(|i| {
                            row.clear();
                            row.extend_from_slice(keys.get(i));
                        }),
                    },
                };
                if found.is_none() {
                    return Poll::Ready(Ok(()));
                }
                self.in_row = true;
            }
            let row = self.row.as_slice();
            let (visitor, top, filter) = (&mut self.visitor, &mut self.top, self.filter.as_ref());
            let ended = match &mut self.source {
                Source::Scan(c) => {
                    c.counted(|c| read_row(c, row, &mut budget, visitor, top, filter))
                }
                Source::Keys { cursor, .. } => {
                    cursor.counted(|c| read_row(c, row, &mut budget, visitor, top, filter))
                }
            }?;
            if ended {
                self.in_row = false;
            } else {
                // The budget ran out mid-row: yield, keeping the row's place.
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
    }
}

/// A cursor that lends the current row's cells one at a time.
trait RowCells {
    fn next_cell(&mut self) -> crate::Result<Option<super::ScanCell<'_>>>;
}

impl RowCells for super::ScanCursor {
    fn next_cell(&mut self) -> crate::Result<Option<super::ScanCell<'_>>> {
        super::ScanCursor::next_cell(self)
    }
}

impl RowCells for super::keys::KeyCursor {
    fn next_cell(&mut self) -> crate::Result<Option<super::ScanCell<'_>>> {
        super::keys::KeyCursor::next_cell(self)
    }
}

/// Scores the current row's cells into `top` within `budget` cells: whether the row ended.
/// Cells `filter` drops count toward the budget too, and never reach the visitor.
fn read_row(
    cells: &mut impl RowCells,
    row: &[u8],
    budget: &mut u32,
    visitor: &mut KnnVisitor,
    top: &mut TopK<Score>,
    filter: Option<&Filter>,
) -> crate::Result<bool> {
    use super::reduce::{CellRef, Reducer};
    loop {
        if *budget == 0 {
            return Ok(false);
        }
        let Some(c) = cells.next_cell()? else {
            return Ok(true);
        };
        *budget -= 1;
        if let Some((p, ssts)) = filter {
            let kept = match pigeonhole_compaction::blob_pointer(c.stored) {
                Some(ptr) => p.matches(&ssts.read_pointer(&ptr)?),
                None => p.matches(c.stored),
            };
            if !kept {
                continue;
            }
        }
        if let Visit::Emit(score) = visitor.visit(row, c.family, c.qualifier, c.ts, c.stored) {
            let cell = CellRef {
                row,
                family_rank: 0,
                family: c.family,
                qualifier: c.qualifier,
                ts: c.ts,
            };
            let _ = top.push(cell, score);
        }
    }
}

impl std::future::Future for PendingNearest {
    type Output = crate::Result<Vec<(super::reduce::CellKey, Score)>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use super::reduce::Reducer;
        use std::task::Poll;
        let _poll = pigeonhole_io::async_poll_scope();
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(Err(crate::Error::InvalidArgument(
                "a search polled after it resolved".to_owned(),
            )));
        }
        match this.step(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(r) => {
                this.done = true;
                this.in_row = false;
                Poll::Ready(r.map(|()| std::mem::replace(&mut this.top, TopK::new(0)).finish()))
            }
        }
    }
}
