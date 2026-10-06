use std::ops::Bound;
use std::sync::Arc;

use pigeonhole_compaction::ValuePredicate;
use pigeonhole_format::key::{Escaped, encode_row_prefix, row_prefix_len};
use pigeonhole_format::value::{ValueRef, decode_value};
use pigeonhole_format::{FamilyId, Seqno, TableId, Timestamp};
use pigeonhole_memtable::ArenaSlice;
use pigeonhole_sst::QualifierFilter;

use crate::catalog::FamilyMeta;
use crate::resolve::{Hold, Merge, ResolveOpts, ResolvedCell, Resolver, SourceCursor};
use crate::snapshot::{Snapshot, TabletEntry, View};
use crate::{Error, Result};

/// How a [`CellData`] keeps its value alive.
#[derive(Clone)]
enum CellValue {
    /// A copy (small memtable values, merge results, blob reads).
    Inline {
        len: u8,
        bytes: [u8; CellData::INLINE_MAX],
    },
    /// A pinned memtable range plus the view that keeps the memtable from being reclaimed.
    Arena { slice: ArenaSlice, _view: Arc<View> },
    /// A heap copy larger than the inline limit (merge results).
    Owned(Vec<u8>),
}

impl std::fmt::Debug for CellValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CellValue::Inline { len, .. } => write!(f, "Inline({len} bytes)"),
            CellValue::Arena { slice, .. } => write!(f, "Arena({} bytes)", slice.len()),
            CellValue::Owned(v) => write!(f, "Owned({} bytes)", v.len()),
        }
    }
}

/// A resolved cell value that pins its storage instead of copying it: a range of a cached
/// block (`cache::Cell`), or a range of a memtable arena plus the view pin that keeps it alive
/// (`memtable::ArenaSlice` + `Arc<View>`). Memtable values of at most
/// [`CellData::INLINE_MAX`] bytes, merge results and blob reads are copied into the cell
/// instead (decision D29), so a hot small get never takes a view reference. Holds no
/// lifetime and clones cheaply.
#[derive(Debug, Clone)]
pub struct CellData {
    ts: Timestamp,
    value: CellValue,
}

impl CellData {
    /// Memtable values up to this many bytes are copied rather than pinned.
    pub const INLINE_MAX: usize = 128;

    /// Builds a cell from a resolved one. `pin` is called only when the value is large and
    /// borrowed from a memtable, so a hot small get never touches a view reference count.
    pub(crate) fn from_resolved(cell: &ResolvedCell<'_>, pin: impl FnOnce() -> Arc<View>) -> Self {
        let stored = cell.value;
        let value = if stored.len() <= Self::INLINE_MAX {
            let mut bytes = [0u8; Self::INLINE_MAX];
            bytes[..stored.len()].copy_from_slice(stored);
            CellValue::Inline {
                len: stored.len() as u8,
                bytes,
            }
        } else {
            match cell.hold {
                Some(Hold::Arena(slice)) => CellValue::Arena {
                    slice: slice.clone(),
                    _view: pin(),
                },
                Some(Hold::Owned(v)) => CellValue::Owned(v.clone()),
                None => CellValue::Owned(stored.to_vec()),
            }
        };
        Self { ts: cell.ts, value }
    }

    /// Timestamp of this version.
    pub fn timestamp(&self) -> Timestamp {
        self.ts
    }

    /// The stored value (tag byte included).
    pub fn stored(&self) -> &[u8] {
        match &self.value {
            CellValue::Inline { len, bytes } => &bytes[..usize::from(*len)],
            CellValue::Arena { slice, .. } => slice,
            CellValue::Owned(v) => v,
        }
    }

    /// The decoded value. Never [`ValueRef::Blob`]: separated values are resolved on read.
    pub fn value(&self) -> ValueRef<'_> {
        decode_value(self.stored()).unwrap_or(ValueRef::Bytes(&[]))
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

impl ReadSpec {
    /// The resolver options for one family under this spec.
    pub(crate) fn resolve_opts(
        &self,
        meta: &FamilyMeta,
        snapshot: Seqno,
        now: Timestamp,
    ) -> ResolveOpts {
        let mut opts = ResolveOpts::new(snapshot, now);
        opts.ttl_micros = meta.options.ttl_micros;
        opts.max_versions = meta.options.max_versions;
        opts.versions = self.versions;
        opts.columns_per_row = self.columns_per_row;
        opts.value = self.value.clone();
        opts.merge = meta.merge;
        opts.filter.qualifiers = self.qualifiers.clone();
        // D82: on a family with a merge operator the time range applies to resolved
        // versions (pushing it to puts could drop a counter's base but keep its operands).
        if meta.merge == crate::catalog::MergeKind::None {
            opts.filter.time_range = self.time_range;
        } else {
            opts.resolved_time_range = self.time_range;
        }
        opts
    }
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
        &self.qualifiers[cell.qualifier.start as usize..cell.qualifier.end as usize]
    }

    /// Appends a resolved cell (its qualifier unescaped into the shared buffer).
    pub(crate) fn push(&mut self, family: FamilyId, cell: &ResolvedCell<'_>, view: &Arc<View>) {
        let start = self.qualifiers.len() as u32;
        qualifier_of(cell.column).unescape_into(&mut self.qualifiers);
        self.cells.push(RowCell {
            family,
            qualifier: start..self.qualifiers.len() as u32,
            data: CellData::from_resolved(cell, || Arc::clone(view)),
        });
    }
}

/// The escaped qualifier inside a column prefix (escaped row, terminator, escaped qualifier,
/// terminator).
pub(crate) fn qualifier_of(column: &[u8]) -> Escaped<'_> {
    let n = row_prefix_len(column).unwrap_or(column.len());
    let end = column.len().saturating_sub(2).max(n);
    Escaped::new(&column[n..end])
}

/// The escaped row inside a column prefix (without its terminator).
pub(crate) fn row_of(column: &[u8]) -> &[u8] {
    let n = row_prefix_len(column).unwrap_or(column.len());
    &column[..n.saturating_sub(2)]
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

/// The resolver of one family within one tablet, plus whether it holds a cell not yet handed
/// out.
struct Lane {
    family: FamilyId,
    resolver: Resolver<Merge<SourceCursor>>,
    pending: bool,
}

impl std::fmt::Debug for Lane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lane")
            .field("family", &self.family)
            .field("pending", &self.pending)
            .finish()
    }
}

/// An ordered scan over a snapshot: walks tablets in key order and, per family, a resolver
/// over a merge of owning sources (memtables now, SSTs from Milestone B) stored inside the
/// cursor beside the snapshot that keeps them valid. Rows come out in order; within a row,
/// cells come out by family, qualifier, newest first.
#[derive(Debug)]
pub struct ScanCursor {
    snapshot: Snapshot,
    spec: ScanSpec,
    families: Vec<FamilyId>,
    now: Timestamp,
    /// Tablets not yet scanned, in row order.
    tablets: std::collections::VecDeque<TabletEntry>,
    lanes: Vec<Lane>,
    /// Escaped and unescaped current row.
    row_esc: Vec<u8>,
    row: Vec<u8>,
    in_row: bool,
    rows_emitted: u64,
    /// Next lane to look at for the current row.
    lane_idx: usize,
    /// The lane whose cell was handed out last (advanced before the next cell).
    last_lane: Option<usize>,
    qual_buf: Vec<u8>,
    done: bool,
}

impl ScanCursor {
    pub(crate) fn new(
        snapshot: Snapshot,
        table: TableId,
        spec: ScanSpec,
        families: Vec<FamilyId>,
        now: Timestamp,
    ) -> Self {
        let tablets = snapshot
            .view
            .tablets()
            .tablets_of(table)
            .iter()
            .cloned()
            .collect();
        Self {
            snapshot,
            spec,
            families,
            now,
            tablets,
            lanes: Vec::new(),
            row_esc: Vec::new(),
            row: Vec::new(),
            in_row: false,
            rows_emitted: 0,
            lane_idx: 0,
            last_lane: None,
            qual_buf: Vec::new(),
            done: false,
        }
    }

    /// Builds the lanes for the next tablet, seeking each to the scan start. Returns false
    /// when no tablet is left.
    fn open_next_tablet(&mut self) -> Result<bool> {
        let Some(tablet) = self.tablets.pop_front() else {
            return Ok(false);
        };
        let view = &self.snapshot.view;
        let mut start_key = Vec::new();
        match &self.spec.start {
            Bound::Included(s) | Bound::Excluded(s) => encode_row_prefix(&mut start_key, s)?,
            Bound::Unbounded => {}
        }
        self.lanes.clear();
        for &family in &self.families {
            let Some(meta) = view.catalog.family(family) else {
                continue;
            };
            let sources = sources_for(view, tablet.shard, tablet.id, family);
            let opts = self
                .spec
                .read
                .resolve_opts(meta, self.snapshot.seqno, self.now);
            let mut resolver = Resolver::new(Merge::new(sources), opts);
            resolver.seek(&start_key)?;
            self.lanes.push(Lane {
                family,
                resolver,
                pending: false,
            });
        }
        Ok(true)
    }

    /// Makes sure every lane either holds a pending cell or is exhausted.
    fn fill_lanes(&mut self) -> Result<()> {
        for lane in &mut self.lanes {
            if !lane.pending {
                lane.pending = lane.resolver.next_cell()?.is_some();
            }
        }
        Ok(())
    }

    /// Whether `row` (unescaped) is past the end bound.
    fn past_end(&self, row: &[u8]) -> bool {
        match &self.spec.end {
            Bound::Included(e) => row > e.as_slice(),
            Bound::Excluded(e) => row >= e.as_slice(),
            Bound::Unbounded => false,
        }
    }

    /// Advances to the next row with at least one visible cell. Returns `false` at the end.
    pub fn next_row(&mut self) -> Result<bool> {
        if self.done {
            return Ok(false);
        }
        // Drop whatever is left of the current row.
        if self.in_row {
            if let Some(i) = self.last_lane.take() {
                self.lanes[i].pending = false;
            }
            for lane in &mut self.lanes {
                if lane.pending && row_of(lane.resolver.current_column()) == self.row_esc {
                    lane.pending = false;
                    lane.resolver.skip_row()?;
                }
            }
            self.in_row = false;
        }
        loop {
            if self.lanes.is_empty() && !self.open_next_tablet()? {
                self.done = true;
                return Ok(false);
            }
            self.fill_lanes()?;
            // The smallest pending row across lanes.
            let mut best: Option<&[u8]> = None;
            for lane in &self.lanes {
                if !lane.pending {
                    continue;
                }
                let row = row_of(lane.resolver.current_column());
                if best.is_none_or(|b| row < b) {
                    best = Some(row);
                }
            }
            let Some(best) = best else {
                // This tablet is exhausted.
                self.lanes.clear();
                continue;
            };
            self.row_esc.clear();
            self.row_esc.extend_from_slice(best);
            self.row.clear();
            Escaped::new(&self.row_esc).unescape_into(&mut self.row);
            if matches!(&self.spec.start, Bound::Excluded(s) if s.as_slice() == self.row.as_slice())
            {
                for lane in &mut self.lanes {
                    if lane.pending && row_of(lane.resolver.current_column()) == self.row_esc {
                        lane.pending = false;
                        lane.resolver.skip_row()?;
                    }
                }
                continue;
            }
            if self.past_end(&self.row)
                || (self.spec.limit != 0 && self.rows_emitted >= self.spec.limit)
            {
                self.done = true;
                self.lanes.clear();
                return Ok(false);
            }
            self.rows_emitted += 1;
            self.in_row = true;
            self.lane_idx = 0;
            self.last_lane = None;
            return Ok(true);
        }
    }

    /// The current row key.
    pub fn row(&self) -> &[u8] {
        &self.row
    }

    /// The next cell of the current row, or `None` when the row is done.
    pub fn next_cell(&mut self) -> Result<Option<ScanCell<'_>>> {
        if !self.in_row {
            return Ok(None);
        }
        if let Some(i) = self.last_lane.take() {
            let lane = &mut self.lanes[i];
            lane.pending = lane.resolver.next_cell()?.is_some();
        }
        while self.lane_idx < self.lanes.len() {
            let i = self.lane_idx;
            let lane = &self.lanes[i];
            if lane.pending && row_of(lane.resolver.current_column()) == self.row_esc {
                self.last_lane = Some(i);
                let lane = &self.lanes[i];
                let cell = lane.resolver.current().expect("pending lane has a cell");
                self.qual_buf.clear();
                qualifier_of(cell.column).unescape_into(&mut self.qual_buf);
                return Ok(Some(ScanCell {
                    family: lane.family,
                    qualifier: &self.qual_buf,
                    ts: cell.ts,
                    stored: cell.value,
                }));
            }
            self.lane_idx += 1;
        }
        Ok(None)
    }

    /// The cell last returned by [`ScanCursor::next_cell`], as a pinned [`CellData`] (no
    /// copy of the value).
    ///
    /// # Panics
    /// If no cell has been returned for the current row.
    pub fn current_data(&self) -> CellData {
        let i = self.last_lane.expect("current_data before next_cell");
        let cell = self.lanes[i]
            .resolver
            .current()
            .expect("the last lane holds a cell");
        CellData::from_resolved(&cell, || Arc::clone(&self.snapshot.view))
    }
}

/// The sources of `(tablet, family)` on `shard` in `view`, newest first.
pub(crate) fn sources_for(
    view: &View,
    shard: pigeonhole_runtime::ShardId,
    tablet: pigeonhole_format::TabletId,
    family: FamilyId,
) -> Vec<SourceCursor> {
    match view.memtables(shard, tablet, family) {
        Some(set) => set
            .readers
            .iter()
            .map(|r| SourceCursor::Mem(r.iter()))
            .collect(),
        None => Vec::new(),
    }
}

impl<S: crate::resolve::Source> Resolver<S> {
    /// The cell last emitted, if any.
    pub(crate) fn current(&self) -> Option<ResolvedCell<'_>> {
        self.has_cell().then(|| self.current_cell())
    }

    /// The column prefix of the cell last emitted.
    ///
    /// # Panics
    /// If no cell is held.
    pub(crate) fn current_column(&self) -> &[u8] {
        self.current_cell().column
    }
}

/// Reads one row through `view`: every family in `families` order.
pub(crate) fn read_row(
    snapshot: &Snapshot,
    table: TableId,
    row: &[u8],
    families: &[FamilyId],
    spec: &ReadSpec,
    now: Timestamp,
) -> Result<Option<RowData>> {
    let view = &snapshot.view;
    let Some((tablet, shard)) = view.tablets().route(table, row) else {
        return Err(Error::TableNotFound(format!("table {}", table.0)));
    };
    let mut prefix = Vec::new();
    encode_row_prefix(&mut prefix, row)?;
    let mut out = RowData {
        row: row.to_vec(),
        ..RowData::default()
    };
    for &family in families {
        let Some(meta) = view.catalog.family(family) else {
            continue;
        };
        let sources = sources_for(view, shard, tablet, family);
        if sources.is_empty() {
            continue;
        }
        let opts = spec.resolve_opts(meta, snapshot.seqno, now);
        let mut resolver = Resolver::new(Merge::new(sources), opts);
        resolver.seek(&prefix)?;
        while let Some(cell) = resolver.next_cell()? {
            if !cell.column.starts_with(&prefix) {
                break;
            }
            out.push(family, &cell, view);
        }
    }
    Ok((!out.cells.is_empty()).then_some(out))
}
