use std::ops::Bound;

use pigeonhole_compaction::ValuePredicate;
use pigeonhole_format::value::ValueRef;
use pigeonhole_format::{FamilyId, Timestamp};
use pigeonhole_sst::QualifierFilter;

/// A resolved cell value that pins its storage instead of copying it: a range of a cached
/// block (`cache::Cell`), or a range of a memtable arena plus the view pin that keeps it alive
/// (`memtable::ArenaSlice` + `Arc<View>`). Memtable values of at most
/// [`CellData::INLINE_MAX`] bytes, merge results and blob reads are copied into the cell
/// instead (decision D29), so a hot small get never takes a view reference. Holds no
/// lifetime and clones cheaply.
#[derive(Debug, Clone)]
pub struct CellData {
    _priv: (),
}

impl CellData {
    /// Memtable values up to this many bytes are copied rather than pinned.
    pub const INLINE_MAX: usize = 128;

    /// Timestamp of this version.
    pub fn timestamp(&self) -> Timestamp {
        todo!()
    }

    /// The stored value (tag byte included).
    pub fn stored(&self) -> &[u8] {
        todo!()
    }

    /// The decoded value. Never [`ValueRef::Blob`]: separated values are resolved on read.
    pub fn value(&self) -> ValueRef<'_> {
        todo!()
    }
}

/// What part of a row (or of each scanned row) to read. Built once per read.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ReadSpec {
    /// Families to read, in this order; empty means every family of the table.
    pub families: Vec<FamilyId>,
    /// Qualifier selection (pushed into the block decoder).
    pub qualifiers: QualifierFilter,
    /// Versions per column (1 = latest; 0 = all retained).
    pub versions: u32,
    /// Keep versions with `min <= ts < max` (pushed into the block decoder).
    pub time_range: Option<(Timestamp, Timestamp)>,
    /// Columns per row and family (0 = unlimited); the rest of the row is skipped.
    pub columns_per_row: u32,
    /// Keep only cells whose value matches.
    pub value: Option<ValuePredicate>,
}

/// A row-range scan.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ScanSpec {
    /// Start of the row range.
    pub start: Bound<Vec<u8>>,
    /// End of the row range.
    pub end: Bound<Vec<u8>>,
    /// Per-row selection.
    pub read: ReadSpec,
    /// Stop after this many rows (0 = unlimited).
    pub limit: u64,
}

impl ScanSpec {
    /// Every row in `[start, end)` with the default projection.
    pub fn new(start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> Self {
        Self {
            start,
            end,
            read: ReadSpec::default(),
            limit: 0,
        }
    }
}

/// One cell of a row read. Its qualifier is a range of [`RowData::qualifiers`], so a row
/// read allocates a few buffers, not one per cell.
#[derive(Debug, Clone)]
pub struct RowCell {
    /// Family.
    pub family: FamilyId,
    /// Qualifier bytes (unescaped) within [`RowData::qualifiers`].
    pub qualifier: std::ops::Range<u32>,
    /// Version and value.
    pub data: CellData,
}

/// The result of a row read: cells ordered by family (in [`ReadSpec::families`] order), then
/// qualifier, then newest version first.
#[derive(Debug, Clone, Default)]
pub struct RowData {
    /// Row key.
    pub row: Vec<u8>,
    /// Every qualifier of the row, concatenated; cells refer to ranges of it.
    pub qualifiers: Vec<u8>,
    /// Cells.
    pub cells: Vec<RowCell>,
}

impl RowData {
    /// The qualifier of `cell`.
    pub fn qualifier(&self, cell: &RowCell) -> &[u8] {
        todo!()
    }
}

/// A cell borrowed from a [`ScanCursor`]; valid until the cursor moves.
#[derive(Debug, Clone, Copy)]
pub struct ScanCell<'a> {
    /// Family.
    pub family: FamilyId,
    /// Qualifier (unescaped; may borrow the cursor's scratch buffer).
    pub qualifier: &'a [u8],
    /// Timestamp.
    pub ts: Timestamp,
    /// Stored value (tag byte included).
    pub stored: &'a [u8],
}

/// An ordered scan over a snapshot: walks tablets in key order and, per family, a
/// `CellResolver` over a `MergingCursor` of owning sources (`FilteredCursor<MemIter>`,
/// `SstIter`) stored inside the cursor beside the snapshot that keeps them valid. Rows come out in order; within
/// a row, cells come out by family, qualifier, newest first.
#[derive(Debug)]
pub struct ScanCursor {
    _priv: (),
}

impl ScanCursor {
    /// Advances to the next row with at least one visible cell. Returns `false` at the end.
    pub fn next_row(&mut self) -> crate::Result<bool> {
        todo!()
    }

    /// The current row key.
    pub fn row(&self) -> &[u8] {
        todo!()
    }

    /// The next cell of the current row, or `None` when the row is done.
    pub fn next_cell(&mut self) -> crate::Result<Option<ScanCell<'_>>> {
        todo!()
    }

    /// The cell last returned by [`ScanCursor::next_cell`], as a pinned [`CellData`] (no
    /// copy of the value).
    pub fn current_data(&self) -> CellData {
        todo!()
    }
}
