//! An async bounded read (#1286, ICR 0122): a visitor and a reducer driven over an async
//! scan or multi-get on the polling task, as [`PendingNearest`](super::knn::PendingNearest)
//! drives its search. Rows are positioned from the block cache, and the blocks they lack
//! are fetched asynchronously; a row's cells past its first block that miss, and separated
//! values (read from their blob files as the cursor fetches them), are read synchronously
//! and counted in `Metrics::async_sync_reads`.
//!
//! It reads the input in order with one reducer, never in groups: by D296 rev 1.2 a group's
//! result is a function of its own key range, so merging groups gives the sequential result,
//! and this one pass returns what [`reduce_into`](super::reduce::reduce_into) returns at
//! any fan-out.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::task::{Context, Poll};

use pigeonhole_format::{FamilyId, TableId};

use super::keys::{KeyCursor, KeySet};
use super::reduce::{CellRef, CellVisitor, Reducer, Resume, Visit};
use super::{ScanCell, ScanCursor, ValuePredicate};
use crate::Result;

/// Cells an async bounded read reads in one poll before it yields (a value predicate's
/// misses included), as an async search does
/// ([`NEAREST_POLL_CELLS`](crate::NEAREST_POLL_CELLS)).
pub const REDUCE_POLL_CELLS: u32 = 4_096;

/// Value bytes an async bounded read takes in one poll before it yields, every cell's
/// counted whether or not it reaches the visitor (a separated one is read from its blob file
/// as the cursor takes it): a visitor that copies each value (a byte-budgeted read's), or a
/// run of predicate misses over large separated values, would otherwise hold the executor
/// for [`REDUCE_POLL_CELLS`] large values.
pub const REDUCE_POLL_BYTES: u64 = 1 << 20;

/// What an async bounded read reads.
enum Source {
    /// One range, through an async scan ([`ScanCursor::poll_next_row`]).
    Scan(ScanCursor),
    /// A key set, through a multi-get cursor ([`KeyCursor::poll_next_key`]).
    Keys {
        cursor: KeyCursor,
        keys: Arc<KeySet>,
    },
}

/// A cursor that lends the current row's cells one at a time.
trait RowCells {
    fn next_cell(&mut self) -> Result<Option<ScanCell<'_>>>;
}

impl RowCells for ScanCursor {
    fn next_cell(&mut self) -> Result<Option<ScanCell<'_>>> {
        ScanCursor::next_cell(self)
    }
}

impl RowCells for KeyCursor {
    fn next_cell(&mut self) -> Result<Option<ScanCell<'_>>> {
        KeyCursor::next_cell(self)
    }
}

/// The value predicate, taken out of the read so its misses count toward a poll's bound
/// (#1237's rule), and the column limit with it: the resolver counts only the columns whose
/// newest version passes (simp724's A1 on #1286).
struct Filter {
    predicate: ValuePredicate,
    column_limit: u32,
}

/// An async bounded read ([`Engine::reduce_async`](crate::Engine::reduce_async)): `visitor`
/// on each resolved cell of one range or a key set, in cell order, `reducer` keeping the
/// result; it resolves to the reducer. A poll yields after [`REDUCE_POLL_CELLS`] cells or
/// [`REDUCE_POLL_BYTES`] value bytes. Dropping it releases its pins and its snapshot.
#[must_use = "a future does nothing unless polled"]
pub struct PendingReduce<V, R> {
    source: Source,
    visitor: V,
    reducer: Option<R>,
    /// The read's families, in its order (a cell's `family_rank`).
    families: Box<[FamilyId]>,
    filter: Option<Filter>,
    resume: Option<Resume>,
    row: Row,
}

/// What [`PendingReduce`] reads with, besides its cursor.
pub(crate) struct Read<V, R> {
    pub visitor: V,
    pub reducer: R,
    /// The read's families, in its order.
    pub families: Box<[FamilyId]>,
    /// The value predicate taken out of the read, and the column limit taken with it.
    pub predicate: Option<(ValuePredicate, u32)>,
    /// The table, which a resume position must be from.
    pub table: TableId,
    /// D308's resume inside a row ([`ReduceSpec::resume`](crate::ReduceSpec)).
    pub resume: Option<(Vec<u8>, crate::ColumnPosition)>,
}

/// The row being read, kept across polls.
#[derive(Default)]
struct Row {
    /// Its key (a buffer reused from row to row).
    key: Vec<u8>,
    reading: bool,
    /// The family last seen, with its rank, and the columns of it that passed the predicate.
    family: Option<(FamilyId, u32)>,
    columns: u32,
    /// The family's column limit is reached: the rest of its cells are skipped.
    family_full: bool,
    /// The column last seen (its qualifier), and whether its newest version passed.
    column: Vec<u8>,
    column_ok: bool,
    in_column: bool,
}

/// What a row's read did.
enum Step {
    /// The poll's bound ran out: the row goes on next poll.
    Yield,
    /// The row ended: on to the next.
    RowDone,
    /// The rest of the row is dropped unread (a visitor's `NextRow`).
    SkipRow,
    /// The reducer broke, or the visitor stopped: the read is done.
    Done,
}

impl<V, R> std::fmt::Debug for PendingReduce<V, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingReduce")
            .field("reading", &self.row.reading)
            .finish_non_exhaustive()
    }
}

impl<V, R> PendingReduce<V, R>
where
    V: CellVisitor<Item = R::Item>,
    R: Reducer,
{
    /// Over an async scan of one range.
    pub(crate) fn scan(cursor: ScanCursor, read: Read<V, R>) -> Result<Self> {
        Self::with(Source::Scan(cursor), read)
    }

    /// Over a multi-get of `keys`.
    pub(crate) fn keys(cursor: KeyCursor, keys: Arc<KeySet>, read: Read<V, R>) -> Result<Self> {
        Self::with(Source::Keys { cursor, keys }, read)
    }

    fn with(source: Source, read: Read<V, R>) -> Result<Self> {
        let resume = read
            .resume
            .as_ref()
            .map(|r| Resume::new(read.table, &read.families, r))
            .transpose()?;
        Ok(Self {
            source,
            visitor: read.visitor,
            reducer: Some(read.reducer),
            families: read.families,
            filter: read.predicate.map(|(predicate, column_limit)| Filter {
                predicate,
                column_limit,
            }),
            resume,
            row: Row::default(),
        })
    }

    fn step(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut cells = REDUCE_POLL_CELLS;
        let mut bytes = REDUCE_POLL_BYTES;
        loop {
            if !self.row.reading {
                let key = &mut self.row.key;
                let found = match &mut self.source {
                    Source::Scan(c) => match c.poll_next_row(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(r) => r?.then(|| {
                            key.clear();
                            key.extend_from_slice(c.row());
                        }),
                    },
                    Source::Keys { cursor, keys } => match cursor.poll_next_key(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(r) => r?.map(|i| {
                            key.clear();
                            key.extend_from_slice(keys.get(i));
                        }),
                    },
                };
                if found.is_none() {
                    return Poll::Ready(Ok(()));
                }
                // D308: the resume row's reads start past the position's column.
                if let Some(r) = &self.resume
                    && r.is(&self.row.key)
                {
                    let any = match &mut self.source {
                        Source::Scan(c) => c.resume_in_row(r)?,
                        Source::Keys { cursor, .. } => cursor.resume_in_row(r)?,
                    };
                    if !any {
                        continue;
                    }
                }
                self.row.reading = true;
                self.row.family = None;
                self.row.in_column = false;
            }
            let mut read = RowRead {
                visitor: &mut self.visitor,
                reducer: self.reducer.as_mut().expect("until resolved"),
                families: &self.families,
                filter: self.filter.as_ref(),
                row: &mut self.row,
                cells: &mut cells,
                bytes: &mut bytes,
            };
            let step = match &mut self.source {
                Source::Scan(c) => c.counted(|c| read.read(c)),
                Source::Keys { cursor, .. } => cursor.counted(|c| read.read(c)),
            }?;
            match step {
                Step::Yield => {
                    // The bound ran out mid-row: yield, keeping the row's place.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Step::RowDone => self.row.reading = false,
                Step::SkipRow => {
                    // A scan's next row drops the rest itself (#1550); a key's is dropped here.
                    if let Source::Keys { cursor, .. } = &mut self.source {
                        cursor.skip_rest_of_key();
                    }
                    self.row.reading = false;
                }
                Step::Done => return Poll::Ready(Ok(())),
            }
        }
    }
}

/// One row's read, within the poll's bounds.
struct RowRead<'a, V, R> {
    visitor: &'a mut V,
    reducer: &'a mut R,
    families: &'a [FamilyId],
    filter: Option<&'a Filter>,
    row: &'a mut Row,
    cells: &'a mut u32,
    bytes: &'a mut u64,
}

impl<V, R> RowRead<'_, V, R>
where
    V: CellVisitor<Item = R::Item>,
    R: Reducer,
{
    fn read(&mut self, cursor: &mut impl RowCells) -> Result<Step> {
        loop {
            if *self.cells == 0 || *self.bytes == 0 {
                return Ok(Step::Yield);
            }
            let Some(c) = cursor.next_cell()? else {
                return Ok(Step::RowDone);
            };
            *self.cells -= 1;
            // Every taken cell's value counts, skipped or not: the lane already read it, a
            // separated one from its blob file (simp724's B1 on #1556).
            *self.bytes = self.bytes.saturating_sub(c.stored.len() as u64);
            let rank = match self.row.family {
                Some((f, r)) if f == c.family => r,
                _ => {
                    let r = self
                        .families
                        .iter()
                        .position(|f| *f == c.family)
                        .unwrap_or(usize::MAX) as u32;
                    self.row.family = Some((c.family, r));
                    self.row.columns = 0;
                    self.row.family_full = false;
                    self.row.in_column = false;
                    r
                }
            };
            if self.row.family_full {
                continue;
            }
            // A separated value is already the value here: the scan's and the multi-get's
            // lanes read it from its blob file as they fetch the cell (inside `counted`), so
            // the predicate and the visitor see what the sync read's lanes hand over
            // (simp724's A2 on #1286).
            let value = c.stored;
            if let Some(f) = self.filter {
                // D22: the predicate tests a column's newest version; the column's older
                // versions follow it.
                let new_column = !self.row.in_column || self.row.column.as_slice() != c.qualifier;
                if new_column {
                    self.row.in_column = true;
                    self.row.column.clear();
                    self.row.column.extend_from_slice(c.qualifier);
                    self.row.column_ok = f.predicate.matches(value);
                    if self.row.column_ok && f.column_limit != 0 {
                        if self.row.columns == f.column_limit {
                            // The family's limit: its remaining columns are skipped.
                            self.row.family_full = true;
                            continue;
                        }
                        self.row.columns += 1;
                    }
                }
                if !self.row.column_ok {
                    continue;
                }
            }
            let visit = self
                .visitor
                .visit(&self.row.key, c.family, c.qualifier, c.ts, value);
            let (item, next_row) = match visit {
                Visit::Skip => (None, false),
                Visit::Emit(item) => (Some(item), false),
                Visit::EmitAndNextRow(item) => (Some(item), true),
                Visit::NextRow => (None, true),
                Visit::Stop => return Ok(Step::Done),
            };
            if let Some(item) = item
                && self.reducer.admits(&item)
            {
                let cell = CellRef {
                    row: &self.row.key,
                    family_rank: rank,
                    family: c.family,
                    qualifier: c.qualifier,
                    ts: c.ts,
                };
                if let ControlFlow::Break(()) = self.reducer.push(cell, item) {
                    return Ok(Step::Done);
                }
            }
            if next_row {
                return Ok(Step::SkipRow);
            }
        }
    }
}

impl<V, R> std::future::Future for PendingReduce<V, R>
where
    V: CellVisitor<Item = R::Item> + Unpin,
    R: Reducer + Unpin,
{
    type Output = Result<R>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _poll = pigeonhole_io::async_poll_scope();
        let this = self.get_mut();
        if this.reducer.is_none() {
            return Poll::Ready(Err(crate::Error::InvalidArgument(
                "a bounded read polled after it resolved".to_owned(),
            )));
        }
        match this.step(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(r) => {
                this.row.reading = false;
                Poll::Ready(r.map(|()| this.reducer.take().expect("checked above")))
            }
        }
    }
}
