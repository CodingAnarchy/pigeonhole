//! The read path: point gets, row reads and ordered scans over a snapshot's view, through
//! `pigeonhole-compaction`'s `CellResolver` (snapshot visibility, deletes, TTL, versions,
//! filters, merge folding) over a `MergingCursor` of the engine's [`Source`]s.

use std::cell::RefCell;
use std::ops::Bound;
use std::sync::{Arc, Mutex, PoisonError};

use pigeonhole_compaction::{
    BlobFetch, MergingCursor, ResolveOptions, ResolvedCell, ValuePredicate, blob_pointer,
};
use pigeonhole_format::key::{
    Escaped, Kind, SUFFIX_LEN, TERMINATOR, decode_key, encode_row_prefix, row_prefix_len,
};
use pigeonhole_format::manifest::FamilyKind;
use pigeonhole_format::scan::ScanFilter;
use pigeonhole_format::value::{BlobPointer, ValueRef, decode_value};
use pigeonhole_format::{Cursor, FamilyId, Seqno, TableId, Timestamp};
use pigeonhole_memtable::ArenaSlice;
use pigeonhole_sst::QualifierFilter;

use crate::catalog::{FamilyMeta, I64_ADD, MergeKind};
use crate::snapshot::{Snapshot, SstSet, TabletEntry, View};
use crate::source::{ColumnKey, Pinned, Resolver, Source, row_prefix};
use crate::{Error, Result};

/// How a [`CellData`] keeps its value alive.
#[derive(Clone)]
enum CellValue {
    /// A copy (small values, merge results, small separated values).
    Inline {
        len: u8,
        bytes: [u8; CellData::INLINE_MAX],
    },
    /// A pinned memtable range plus the view that keeps the memtable from being reclaimed.
    Arena { slice: ArenaSlice, _view: Arc<View> },
    /// A pinned range of a cached SST block.
    Block(pigeonhole_cache::Cell),
    /// A heap copy larger than the inline limit (merge results, mid-sized values).
    Owned(Vec<u8>),
}

impl std::fmt::Debug for CellValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CellValue::Inline { len, .. } => write!(f, "Inline({len} bytes)"),
            CellValue::Arena { slice, .. } => write!(f, "Arena({} bytes)", slice.len()),
            CellValue::Block(c) => write!(f, "Block({} bytes)", c.len()),
            CellValue::Owned(v) => write!(f, "Owned({} bytes)", v.len()),
        }
    }
}

/// A resolved cell value that pins its storage instead of copying it: a range of a cached
/// block (`cache::Cell`), or a range of a memtable arena plus the view pin that keeps it alive
/// (`memtable::ArenaSlice` + `Arc<View>`). Values of at most [`CellData::INLINE_MAX`] bytes,
/// merge results and values the resolver had to buffer (up to 4 KiB, decision D81) are copied
/// into the cell instead (decision D29), so a hot small get never takes a view reference.
/// Holds no lifetime and clones cheaply.
#[derive(Debug, Clone)]
pub struct CellData {
    ts: Timestamp,
    value: CellValue,
}

impl CellData {
    /// Values up to this many bytes are copied rather than pinned.
    pub const INLINE_MAX: usize = 128;

    /// An empty inline value at timestamp 0, to be overwritten in place with
    /// [`CellData::set_inline`].
    pub const EMPTY: CellData = CellData {
        ts: 0,
        value: CellValue::Inline {
            len: 0,
            bytes: [0; Self::INLINE_MAX],
        },
    };

    /// Makes this cell a copy of `stored` (a stored value of at most
    /// [`CellData::INLINE_MAX`] bytes) at `ts`, reusing its inline bytes when it has them.
    #[inline]
    pub fn set_inline(&mut self, ts: Timestamp, stored: &[u8]) {
        debug_assert!(stored.len() <= Self::INLINE_MAX);
        self.ts = ts;
        match &mut self.value {
            CellValue::Inline { len, bytes } => {
                bytes[..stored.len()].copy_from_slice(stored);
                *len = stored.len() as u8;
            }
            v => {
                let mut bytes = [0u8; Self::INLINE_MAX];
                bytes[..stored.len()].copy_from_slice(stored);
                *v = CellValue::Inline {
                    len: stored.len() as u8,
                    bytes,
                };
            }
        }
    }

    /// Builds a cell from a resolved one. `source` is the source the merged cursor is on
    /// (to pin a large value without copying); `pin` is called only when the value is large
    /// and borrowed from a memtable, so a hot small get never touches a view reference count.
    pub(crate) fn from_cell(
        cell: &ResolvedCell<'_>,
        source: Option<&Source>,
        pin: impl FnOnce() -> Arc<View>,
    ) -> Self {
        let stored = cell.value;
        let value = if stored.len() <= Self::INLINE_MAX {
            let mut bytes = [0u8; Self::INLINE_MAX];
            bytes[..stored.len()].copy_from_slice(stored);
            CellValue::Inline {
                len: stored.len() as u8,
                bytes,
            }
        } else if cell.from_source {
            match source.map(Source::pin_value) {
                Some(Pinned::Arena(slice)) => CellValue::Arena {
                    slice,
                    _view: pin(),
                },
                Some(Pinned::Block(c)) => CellValue::Block(c),
                #[cfg(test)]
                Some(Pinned::Owned(v)) => CellValue::Owned(v),
                None => CellValue::Owned(stored.to_vec()),
            }
        } else {
            CellValue::Owned(stored.to_vec())
        };
        Self { ts: cell.ts, value }
    }

    /// A separated value read from its blob file: pinned in the block cache, or copied when
    /// small.
    fn from_blob(ts: Timestamp, value: pigeonhole_cache::Cell) -> Self {
        let bytes: &[u8] = &value;
        if bytes.len() <= Self::INLINE_MAX {
            let mut inline = [0u8; Self::INLINE_MAX];
            inline[..bytes.len()].copy_from_slice(bytes);
            return Self {
                ts,
                value: CellValue::Inline {
                    len: bytes.len() as u8,
                    bytes: inline,
                },
            };
        }
        Self {
            ts,
            value: CellValue::Block(value),
        }
    }

    /// Builds a cell from a resolved one that the caller would copy (small, or buffered by
    /// the resolver), reading a separated value from its blob file first.
    pub(crate) fn resolved(
        cell: &ResolvedCell<'_>,
        ssts: &SstSet,
        pin: impl FnOnce() -> Arc<View>,
    ) -> Result<Self> {
        Ok(match ssts.read_blob(cell.value)? {
            Some(v) => Self::from_blob(cell.ts, v),
            None => Self::from_cell(cell, None, pin),
        })
    }

    fn from_pinned(ts: Timestamp, value: &LaneValue, pin: impl FnOnce() -> Arc<View>) -> Self {
        let bytes: &[u8] = value;
        if bytes.len() <= Self::INLINE_MAX {
            let mut inline = [0u8; Self::INLINE_MAX];
            inline[..bytes.len()].copy_from_slice(bytes);
            return Self {
                ts,
                value: CellValue::Inline {
                    len: bytes.len() as u8,
                    bytes: inline,
                },
            };
        }
        let value = match value {
            LaneValue::Copied(v) => CellValue::Owned(v.clone()),
            LaneValue::Pinned(Pinned::Arena(slice)) => CellValue::Arena {
                slice: slice.clone(),
                _view: pin(),
            },
            LaneValue::Pinned(Pinned::Block(c)) => CellValue::Block(c.clone()),
            #[cfg(test)]
            LaneValue::Pinned(Pinned::Owned(v)) => CellValue::Owned(v.clone()),
        };
        Self { ts, value }
    }

    /// A copy of `stored` at `ts` (the row cache's hits, D201).
    pub(crate) fn copied(ts: Timestamp, stored: &[u8]) -> Self {
        let mut data = Self::EMPTY;
        if stored.len() <= Self::INLINE_MAX {
            data.set_inline(ts, stored);
        } else {
            data.ts = ts;
            data.value = CellValue::Owned(stored.to_vec());
        }
        data
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
            CellValue::Block(c) => c,
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
    /// Keep versions with `min <= ts < max` (pushed into the block decoder, or applied to
    /// resolved versions on a family with a merge operator, decision D82).
    pub time_range: Option<(Timestamp, Timestamp)>,
    /// Columns per row and family (0 = unlimited); the rest of the row is skipped.
    pub columns_per_row: u32,
    /// Keep only cells whose value matches.
    pub value: Option<ValuePredicate>,
}

impl ReadSpec {
    /// The resolver options and entry filter for one family under this spec.
    pub(crate) fn resolve_opts(
        &self,
        meta: &FamilyMeta,
        snapshot: Seqno,
        now: Timestamp,
    ) -> (ResolveOptions, ScanFilter) {
        let mut opts = ResolveOptions::new(snapshot, now);
        opts.ttl_micros = meta.options.ttl_micros;
        // Decision D76: the caller folds the family's max_versions in (0 = unlimited).
        opts.versions = match (meta.options.max_versions, self.versions) {
            (0, v) => v,
            (m, 0) => m,
            (m, v) => m.min(v),
        };
        opts.columns_per_row = self.columns_per_row;
        opts.value = self.value.clone();
        opts.merge = meta.merge_op.clone();
        opts.counter = meta.options.kind == FamilyKind::Counter;
        let mut filter = ScanFilter::all();
        filter.qualifiers = self.qualifiers.clone();
        opts.route_time_range(&mut filter, self.time_range);
        (opts, filter)
    }
}

/// Reads separated values for the resolver (`ResolveOptions::blobs`): a value predicate
/// tests the value, and a merge operator folds onto the value, not its pointer. The resolver
/// cannot fail through the hook, so the first error is kept here and the read reports it
/// after each resolver step ([`ResolverBlobs::check`]).
#[derive(Debug)]
pub(crate) struct ResolverBlobs {
    ssts: Arc<SstSet>,
    error: Mutex<Option<Error>>,
}

impl ResolverBlobs {
    /// Sets `opts.blobs` when the resolver may need a separated value and `ssts` names blob
    /// files; returns the handle to check after each resolver step. It may need one for a
    /// value predicate, or to fold operands onto a separated base. The built-in `i64` add
    /// is the exception: it rejects every base that is not a stored `i64`, with the same
    /// error, and a separated value never is one (only `Bytes` are separated), so loading
    /// the base could not change its result. Skipping it keeps a default family's reads
    /// free of the hook.
    pub(crate) fn attach(opts: &mut ResolveOptions, ssts: &Arc<SstSet>) -> Option<Arc<Self>> {
        let folds = opts.merge.as_ref().is_some_and(|m| m.name() != I64_ADD);
        if !(opts.value.is_some() || folds) || !ssts.has_blobs() {
            return None;
        }
        let blobs = Arc::new(Self {
            ssts: Arc::clone(ssts),
            error: Mutex::new(None),
        });
        opts.blobs = Some(Arc::clone(&blobs) as Arc<dyn BlobFetch>);
        Some(blobs)
    }

    /// Fails with the first error a fetch hit.
    pub(crate) fn check(blobs: Option<&Arc<Self>>) -> Result<()> {
        match blobs.and_then(|b| {
            b.error
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
        }) {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl BlobFetch for ResolverBlobs {
    fn fetch(&self, ptr: &BlobPointer) -> Option<pigeonhole_cache::Cell> {
        match self.ssts.read_pointer(ptr) {
            Ok(v) => Some(v),
            Err(e) => {
                let mut slot = self.error.lock().unwrap_or_else(PoisonError::into_inner);
                if slot.is_none() {
                    *slot = Some(e);
                }
                None
            }
        }
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
}

/// Where a row read puts the cells it resolves ([`Engine::read_row_into`](crate::Engine::read_row_into)),
/// in order: by family (in [`ReadSpec::families`] order), then qualifier, newest version
/// first. A caller's own row buffer can take them directly, without an intermediate
/// [`RowData`] (#287).
pub trait RowSink {
    /// The buffer qualifiers are unescaped into, one after another.
    fn qualifiers(&mut self) -> &mut Vec<u8>;
    /// Appends a cell of `family` whose qualifier is `qualifier` within
    /// [`RowSink::qualifiers`].
    fn push(&mut self, family: FamilyId, qualifier: std::ops::Range<usize>, data: CellData);
    /// Appends a cell whose value is a copy of `stored` (at most [`CellData::INLINE_MAX`]
    /// bytes). Sinks that store cells in a vector override this to build the cell in place
    /// rather than move it (#287).
    fn push_inline(
        &mut self,
        family: FamilyId,
        qualifier: std::ops::Range<usize>,
        ts: Timestamp,
        stored: &[u8],
    ) {
        let mut data = CellData::EMPTY;
        data.set_inline(ts, stored);
        self.push(family, qualifier, data);
    }

    /// How many cells this sink holds, for the row cache to read back what a read pushed
    /// (D201). `None` (the default): the sink cannot report them, and its reads are not
    /// stored in the row cache.
    fn cell_count(&self) -> Option<usize> {
        None
    }

    /// Cell `i`: its qualifier and data (see [`RowSink::cell_count`]).
    fn cell(&self, i: usize) -> Option<(&[u8], &CellData)> {
        let _ = i;
        None
    }
}

impl RowSink for RowData {
    fn qualifiers(&mut self) -> &mut Vec<u8> {
        &mut self.qualifiers
    }

    fn push(&mut self, family: FamilyId, qualifier: std::ops::Range<usize>, data: CellData) {
        self.cells.push(RowCell {
            family,
            qualifier: qualifier.start as u32..qualifier.end as u32,
            data,
        });
    }

    fn push_inline(
        &mut self,
        family: FamilyId,
        qualifier: std::ops::Range<usize>,
        ts: Timestamp,
        stored: &[u8],
    ) {
        self.cells.push(RowCell {
            family,
            qualifier: qualifier.start as u32..qualifier.end as u32,
            data: CellData::EMPTY,
        });
        if let Some(c) = self.cells.last_mut() {
            c.data.set_inline(ts, stored);
        }
    }

    fn cell_count(&self) -> Option<usize> {
        Some(self.cells.len())
    }

    fn cell(&self, i: usize) -> Option<(&[u8], &CellData)> {
        let c = self.cells.get(i)?;
        Some((self.qualifier(c), &c.data))
    }
}

/// The column prefix of a resolved cell's key (escaped row, terminator, escaped qualifier,
/// terminator).
pub(crate) fn column_of(key: &[u8]) -> &[u8] {
    &key[..key.len().saturating_sub(SUFFIX_LEN)]
}

/// The escaped row inside a column prefix (without its terminator).
pub(crate) fn row_of(column: &[u8]) -> &[u8] {
    let n = row_prefix_len(column).unwrap_or(column.len());
    &column[..n.saturating_sub(2)]
}

/// The smallest key past every key of the row whose prefix is `prefix` (the terminator's
/// last byte bumped), appended to `out`.
pub(crate) fn past_row(prefix: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(prefix);
    if let Some(last) = out.last_mut() {
        *last += 1;
    }
}

/// Maps a resolver error for `meta`: the shared resolver reports a missing operator as a
/// merge error; the engine promises `UnknownMergeOperator` for such reads.
pub(crate) fn read_error(e: Error, meta: &FamilyMeta) -> Error {
    match (e, meta.merge) {
        (Error::Merge(_), MergeKind::Unknown) => {
            Error::UnknownMergeOperator(meta.options.merge_operator.clone())
        }
        (e, _) => e,
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

/// A scan's `[start, end)` as encoded row prefixes (`None` = unbounded).
type RowBounds = (Option<Vec<u8>>, Option<Vec<u8>>);

/// A lane's held value.
#[derive(Debug)]
enum LaneValue {
    Copied(Vec<u8>),
    Pinned(Pinned),
}

impl std::ops::Deref for LaneValue {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            LaneValue::Copied(v) => v,
            LaneValue::Pinned(p) => p,
        }
    }
}

/// The resolver of one family within one tablet, plus the cell it holds but has not handed
/// out yet.
struct Lane {
    family: FamilyId,
    /// The family's merge operator name when the build does not know it (to name it in the
    /// error a merge then fails with), `None` otherwise: no copy of the family's metadata.
    unknown_merge: Option<String>,
    resolver: Resolver,
    /// A value the resolver buffered was too large to keep its buffers for the next scan.
    grew: bool,
    /// The view's SSTs and blob files, to read separated values.
    ssts: Arc<SstSet>,
    /// The resolver's blob reads, if it may need any.
    resolver_blobs: Option<Arc<ResolverBlobs>>,
    /// The held cell is the resolver's current one (`CellResolver::current`, valid while
    /// `pending`), not a copy: its timestamp, and its value pinned when it is separated or a
    /// large value borrowed from a source.
    ts: Timestamp,
    pinned: Option<LaneValue>,
    pending: bool,
    done: bool,
}

impl std::fmt::Debug for Lane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lane")
            .field("family", &self.family)
            .field("pending", &self.pending)
            .finish()
    }
}

impl Lane {
    /// Moves to the next visible cell. The lane holds it where the resolver has it; only a
    /// separated value (read from its blob file) or a large one borrowed from a source is
    /// pinned.
    fn fetch(&mut self) -> Result<()> {
        self.pending = false;
        self.pinned = None;
        if self.done {
            return Ok(());
        }
        let (from_source, len) = {
            let next = self.resolver.next_cell();
            ResolverBlobs::check(self.resolver_blobs.as_ref())?;
            let Some(cell) = next.map_err(|e| match (e, &self.unknown_merge) {
                (Error::Merge(_), Some(name)) => Error::UnknownMergeOperator(name.clone()),
                (e, _) => e,
            })?
            else {
                self.done = true;
                return Ok(());
            };
            self.ts = cell.ts;
            if let Some(v) = self.ssts.read_blob(cell.value)? {
                self.pinned = Some(LaneValue::Pinned(Pinned::Block(v)));
            }
            (cell.from_source, cell.value.len())
        };
        // Large values only (one branch per cell): a source's is pinned where it lies; one
        // the resolver buffered marks its buffers too large to keep for the next scan.
        if len > CellData::INLINE_MAX {
            if !from_source {
                self.grew |= len > KEEP_BUFFERS_BELOW;
            } else if self.pinned.is_none() {
                let src = self
                    .resolver
                    .cursor()
                    .current()
                    .expect("the merged cursor is on the returned entry");
                self.pinned = Some(LaneValue::Pinned(src.pin_value()));
            }
        }
        self.pending = true;
        Ok(())
    }

    /// The held cell's column prefix (only while `pending`).
    fn col(&self) -> &[u8] {
        self.resolver.current().map_or(&[], |c| column_of(c.key))
    }

    /// The held cell's stored value (only while `pending`).
    fn value(&self) -> &[u8] {
        match &self.pinned {
            Some(v) => v,
            None => self.resolver.current().map_or(&[], |c| c.value),
        }
    }

    /// The held cell as a [`CellData`]: a large value stays pinned when it is, and is
    /// copied otherwise (a large merge result the resolver buffered).
    fn data(&self, pin: impl FnOnce() -> Arc<View>) -> CellData {
        match &self.pinned {
            Some(v) => CellData::from_pinned(self.ts, v, pin),
            None => CellData::from_pinned(self.ts, &LaneValue::Copied(self.value().to_vec()), pin),
        }
    }
}

/// An ordered scan over a snapshot: walks tablets in key order and, per family, a resolver
/// over a merge of the owning sources (memtables and SSTs) stored inside the cursor beside
/// the snapshot that keeps them valid. Rows come out in order; within a row, cells come out
/// by family, qualifier, newest first.
#[derive(Debug)]
pub struct ScanCursor {
    snapshot: Snapshot,
    spec: ScanSpec,
    families: Vec<FamilyId>,
    now: Timestamp,
    table: TableId,
    /// Index of the next tablet to scan among the table's tablets in the snapshot's view.
    next_tablet: usize,
    /// The scan's start and end as row-prefix bounds, encoded on the first tablet.
    bounds: Option<RowBounds>,
    /// The bounds clamped to the current tablet (reused buffers).
    lo: Vec<u8>,
    hi: Vec<u8>,
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
    /// An async scan's state (`Engine::scan_async`), `None` for a sync one.
    nb: Option<Box<AsyncScan>>,
}

/// What an async scan keeps between polls (D196, #398).
struct AsyncScan {
    shared: Arc<crate::shard::Shared>,
    fetch: Option<(pigeonhole_io::Completion, Box<pigeonhole_sst::Fetch>)>,
    /// Blocks it fetched and has not stepped past yet.
    pinned: Vec<pigeonhole_cache::BlockHandle>,
    /// Fetches while positioning the current tablet: past `MAX_POSITION_FETCHES`, or when
    /// the cache keeps nothing, it positions synchronously (counted).
    fetches: u32,
    sync: bool,
}

impl std::fmt::Debug for AsyncScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncScan")
            .field("fetching", &self.fetch.is_some())
            .field("pinned", &self.pinned.len())
            .field("sync", &self.sync)
            .finish()
    }
}

/// Fetches an async scan makes positioning one tablet before it positions synchronously.
const MAX_POSITION_FETCHES: u32 = 64;

impl ScanCursor {
    pub(crate) fn new(
        snapshot: Snapshot,
        table: TableId,
        spec: ScanSpec,
        families: Vec<FamilyId>,
        now: Timestamp,
    ) -> Self {
        Self {
            snapshot,
            spec,
            families,
            now,
            table,
            next_tablet: 0,
            bounds: None,
            lo: Vec::new(),
            hi: Vec::new(),
            lanes: Vec::new(),
            row_esc: Vec::new(),
            row: Vec::new(),
            in_row: false,
            rows_emitted: 0,
            lane_idx: 0,
            last_lane: None,
            qual_buf: Vec::new(),
            done: false,
            nb: None,
        }
    }

    /// The next row of an async scan ([`Engine::scan_async`](crate::Engine::scan_async)):
    /// `Ready(Ok(true))` when the cursor is on a row (read its cells, inside
    /// [`ScanCursor::counted`]), `Ready(Ok(false))` at the end.
    ///
    /// Each tablet is positioned cache-only: its cursors' seeks and first cells read only
    /// cached blocks, and what they miss is fetched asynchronously before positioning again
    /// (a position can be redone, as a get can). Then each step first fetches the blocks the
    /// SST cursors will read next, if uncached (one ahead per cursor, only as the scan is
    /// polled), and steps synchronously: a block it still misses (not predicted) is read
    /// synchronously and counted in `Metrics::async_sync_reads` (D196, #398). On a sync
    /// cursor this is `next_row`.
    pub fn poll_next_row(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<bool>> {
        use std::future::Future;
        use std::sync::atomic::Ordering;
        use std::task::Poll;
        let Some(mut nb) = self.nb.take() else {
            return Poll::Ready(self.next_row());
        };
        let out = loop {
            if let Some((completion, fetch)) = &mut nb.fetch {
                let buf = match std::pin::Pin::new(completion).poll(cx) {
                    Poll::Pending => {
                        self.nb = Some(nb);
                        return Poll::Pending;
                    }
                    Poll::Ready(r) => r,
                };
                let admitted = buf.map_err(Error::from).and_then(|b| Ok(fetch.admit(b)?));
                let kept = fetch.is_kept();
                nb.fetch = None;
                match admitted {
                    Ok(h) => nb.pinned.extend(h),
                    Err(e) => break Err(e),
                }
                if !kept {
                    // The cache keeps nothing: prefetching or positioning cache-only would
                    // fetch the same block again; read synchronously from now on (counted).
                    nb.sync = true;
                    nb.shared.async_sync_reads.fetch_add(1, Ordering::Relaxed);
                }
            }
            if self.done {
                break Ok(false);
            }
            if self.lanes.is_empty() && !self.in_row && !nb.sync {
                // Position the next tablet cache-only; on a miss, undo and fetch.
                let tablet = self.next_tablet;
                let positioned = self.open_next_tablet::<true>().and_then(|more| {
                    if more {
                        self.fill_lanes()?;
                    }
                    Ok(more)
                });
                match positioned {
                    Err(Error::WouldBlock(fetch)) => {
                        self.recycle_lanes();
                        self.next_tablet = tablet;
                        nb.fetches += 1;
                        if nb.fetches > MAX_POSITION_FETCHES {
                            nb.sync = true;
                            nb.shared.async_sync_reads.fetch_add(1, Ordering::Relaxed);
                        } else {
                            let completion = fetch.submit();
                            nb.fetch = Some((completion, fetch));
                        }
                        continue;
                    }
                    Err(e) => break Err(e),
                    Ok(false) => {
                        self.done = true;
                        break Ok(false);
                    }
                    Ok(true) => {
                        nb.fetches = 0;
                        nb.pinned.clear();
                        for lane in &mut self.lanes {
                            for source in lane.resolver.cursor_mut().sources_mut() {
                                source.set_cache_only(false);
                            }
                        }
                    }
                }
            }
            if !nb.sync {
                // One block ahead per cursor; fetched one at a time.
                let mut upcoming = Vec::new();
                for lane in &self.lanes {
                    for source in lane.resolver.cursor().sources() {
                        source.upcoming(1, &mut upcoming);
                        if !upcoming.is_empty() {
                            break;
                        }
                    }
                    if !upcoming.is_empty() {
                        break;
                    }
                }
                if let Some(fetch) = upcoming.into_iter().next() {
                    let completion = fetch.submit();
                    nb.fetch = Some((completion, Box::new(fetch)));
                    continue;
                }
            }
            let ((stepped, blob_reads), file_reads) = pigeonhole_sst::counting_file_reads(|| {
                as_async_read(false, || {
                    if nb.sync {
                        self.advance_row_impl::<true>()
                    } else {
                        self.advance_row_impl::<false>()
                    }
                })
            });
            if blob_reads + file_reads > 0 {
                nb.shared
                    .async_sync_reads
                    .fetch_add(blob_reads + file_reads, Ordering::Relaxed);
            }
            match stepped {
                // The tablet ran out: position the next one.
                Ok(None) => continue,
                Ok(Some(row)) => {
                    // Blocks stepped past need not stay pinned.
                    nb.pinned.clear();
                    break Ok(row);
                }
                Err(e) => break Err(e),
            }
        };
        self.nb = Some(nb);
        Poll::Ready(self.snapshot.checked(out))
    }

    /// Runs `f` on this cursor counting the file reads it makes synchronously into
    /// `Metrics::async_sync_reads` (an async scan reading a row's cells after
    /// [`ScanCursor::poll_next_row`]); on a sync cursor it just runs `f`.
    pub fn counted<T>(&mut self, f: impl FnOnce(&mut Self) -> T) -> T {
        let Some(shared) = self.nb.as_ref().map(|nb| Arc::clone(&nb.shared)) else {
            return f(self);
        };
        let ((out, blob_reads), file_reads) =
            pigeonhole_sst::counting_file_reads(|| as_async_read(false, || f(self)));
        if blob_reads + file_reads > 0 {
            shared.async_sync_reads.fetch_add(
                blob_reads + file_reads,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        out
    }

    /// The cursor of an async scan ([`ScanCursor::poll_next_row`]).
    pub(crate) fn into_async(mut self, shared: Arc<crate::shard::Shared>) -> Self {
        self.nb = Some(Box::new(AsyncScan {
            shared,
            fetch: None,
            pinned: Vec::new(),
            fetches: 0,
            sync: false,
        }));
        self
    }

    /// The scan's start and end as row-prefix bounds (`None` = unbounded).
    fn bounds(&self) -> Result<RowBounds> {
        let start = match &self.spec.start {
            Bound::Included(s) | Bound::Excluded(s) => {
                let mut k = Vec::new();
                encode_row_prefix(&mut k, s)?;
                Some(k)
            }
            Bound::Unbounded => None,
        };
        let end = match &self.spec.end {
            Bound::Excluded(e) => {
                let mut k = Vec::new();
                encode_row_prefix(&mut k, e)?;
                Some(k)
            }
            Bound::Included(e) => {
                let mut k = Vec::new();
                encode_row_prefix(&mut k, e)?;
                let mut past = Vec::new();
                past_row(&k, &mut past);
                Some(past)
            }
            Bound::Unbounded => None,
        };
        Ok((start, end))
    }

    /// Builds the lanes for the next tablet, seeking each to the scan start. Returns false
    /// when no tablet is left.
    fn open_next_tablet<const CACHE_ONLY: bool>(&mut self) -> Result<bool> {
        self.recycle_lanes();
        let view = Arc::clone(&self.snapshot.view);
        let Some(tablet) = view.tablets().tablets_of(self.table).get(self.next_tablet) else {
            return Ok(false);
        };
        self.next_tablet += 1;
        if self.bounds.is_none() {
            self.bounds = Some(self.bounds()?);
        }
        let (start, end) = self.bounds.as_ref().expect("encoded above");
        // Clamp to the tablet: after a split, children share their parent's SSTs (D13), which
        // hold rows of the sibling too.
        let (has_lo, has_hi) = clamp_into(
            tablet,
            start.as_deref(),
            end.as_deref(),
            &mut self.lo,
            &mut self.hi,
        )?;
        let (start, end) = (
            has_lo.then_some(&self.lo[..]),
            has_hi.then_some(&self.hi[..]),
        );
        if matches!((start, end), (Some(s), Some(e)) if s >= e) {
            // No row of the scan is in this tablet: it opens with no lanes.
            return Ok(true);
        }
        for &family in &self.families {
            // Nothing of this family in the tablet: no resolver, options or sources for it (a
            // table's empty families, #406). Located once, for the sources below too.
            let located = view.locate(tablet.shard, tablet.id, family);
            if !located.has_data() {
                continue;
            }
            let Some(meta) = view.catalog.family(family) else {
                continue;
            };
            let (mut opts, filter) =
                self.spec
                    .read
                    .resolve_opts(meta, self.snapshot.seqno, self.now);
            let resolver_blobs = ResolverBlobs::attach(&mut opts, &view.ssts);
            let mut resolver = take_scan_resolver();
            let filled = view.scan_sources_located::<CACHE_ONLY>(
                &located,
                &filter,
                start,
                end,
                resolver.cursor_mut().sources_mut(),
            );
            if let Err(e) = filled {
                // The sources filled so far go now, so the resolver pins nothing.
                give_back(resolver);
                return Err(e);
            }
            if resolver.cursor_mut().sources().is_empty() {
                // Nothing of this family in the tablet's range: no lane, so the scan does not
                // set one up, seek it or poll it per row (a table's empty families, #406).
                give_back(resolver);
                continue;
            }
            opts.skip_columns = skips_columns(resolver.cursor_mut().sources());
            resolver.cursor_mut().reset();
            resolver.reset(opts);
            resolver.set_upper_bound(end);
            self.lanes.push(Lane {
                family,
                unknown_merge: (meta.merge == MergeKind::Unknown)
                    .then(|| meta.options.merge_operator.clone()),
                resolver,
                grew: false,
                ssts: Arc::clone(&view.ssts),
                resolver_blobs,
                ts: 0,
                pinned: None,
                pending: false,
                done: false,
            });
            // A failed seek leaves the lane in place; its sources go with the others.
            let lane = self.lanes.last_mut().expect("pushed above");
            match start {
                Some(s) => lane.resolver.seek(s)?,
                None => lane.resolver.seek(&[])?,
            }
        }
        Ok(true)
    }

    /// Ends the current lanes: each resolver goes back to the thread's pool for the next
    /// lanes, unless a large value grew its buffers.
    fn recycle_lanes(&mut self) {
        for lane in self.lanes.drain(..) {
            if !lane.grew {
                give_back(lane.resolver);
            }
        }
    }

    /// Makes sure every lane either holds a pending cell or is exhausted.
    fn fill_lanes(&mut self) -> Result<()> {
        for lane in &mut self.lanes {
            if !lane.pending && !lane.done {
                lane.fetch()?;
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

    /// Drops the rest of the current row in every lane.
    fn skip_current_row(&mut self) -> Result<()> {
        // A column of the current row starts with its escaped row and the terminator (which
        // never occurs inside an escaped row): a prefix compare, no walk to find the row.
        let row_len = self.row_esc.len() + TERMINATOR.len();
        for lane in &mut self.lanes {
            let col = if lane.pending { lane.col() } else { &[] };
            if lane.pending
                && col.len() >= row_len
                && col.starts_with(&self.row_esc)
                && col[self.row_esc.len()..row_len] == TERMINATOR
            {
                lane.pending = false;
                lane.resolver.skip_row()?;
            }
        }
        Ok(())
    }

    /// Advances to the next row with at least one visible cell. Returns `false` at the end.
    /// In a reader process, fails with [`Error::SnapshotExpired`] once a writer restarted
    /// after the snapshot was taken.
    pub fn next_row(&mut self) -> Result<bool> {
        let advanced = self.advance_row();
        self.snapshot.checked(advanced)
    }

    fn advance_row(&mut self) -> Result<bool> {
        Ok(self.advance_row_impl::<true>()?.unwrap_or(false))
    }

    /// Advances to the next row; with `OPEN`, opening the next tablets as it needs them;
    /// without, `None` where it would open one (an async scan positions it cache-only first).
    // Inlined: as a separate function a sync scan measured about 3.5 instructions more per
    // row (#42).
    #[inline(always)]
    fn advance_row_impl<const OPEN: bool>(&mut self) -> Result<Option<bool>> {
        if self.done {
            return Ok(Some(false));
        }
        // A scan at its row limit is done: never read the next row just to stop there.
        if self.spec.limit != 0 && self.rows_emitted >= self.spec.limit {
            self.done = true;
            self.recycle_lanes();
            return Ok(Some(false));
        }
        // Drop whatever is left of the current row.
        if self.in_row {
            if let Some(i) = self.last_lane.take() {
                self.lanes[i].pending = false;
            }
            self.skip_current_row()?;
            self.in_row = false;
        }
        loop {
            if self.lanes.is_empty() {
                if !OPEN {
                    return Ok(None);
                }
                if !self.open_next_tablet::<false>()? {
                    self.done = true;
                    return Ok(Some(false));
                }
            }
            self.fill_lanes()?;
            // The smallest pending row across lanes.
            let mut best: Option<&[u8]> = None;
            for lane in &self.lanes {
                if !lane.pending {
                    continue;
                }
                let row = row_of(lane.col());
                if best.is_none_or(|b| row < b) {
                    best = Some(row);
                }
            }
            let Some(best) = best else {
                // This tablet is exhausted.
                self.recycle_lanes();
                continue;
            };
            self.row_esc.clear();
            self.row_esc.extend_from_slice(best);
            self.row.clear();
            Escaped::new(&self.row_esc).unescape_long_into(&mut self.row);
            if matches!(&self.spec.start, Bound::Excluded(s) if s.as_slice() == self.row.as_slice())
            {
                self.skip_current_row()?;
                continue;
            }
            if self.past_end(&self.row)
                || (self.spec.limit != 0 && self.rows_emitted >= self.spec.limit)
            {
                self.done = true;
                self.recycle_lanes();
                return Ok(Some(false));
            }
            self.rows_emitted += 1;
            self.in_row = true;
            self.lane_idx = 0;
            self.last_lane = None;
            return Ok(Some(true));
        }
    }

    /// The current row key.
    pub fn row(&self) -> &[u8] {
        &self.row
    }

    /// The next cell of the current row, or `None` when the row is done.
    pub fn next_cell(&mut self) -> Result<Option<ScanCell<'_>>> {
        let mut qual = std::mem::take(&mut self.qual_buf);
        qual.clear();
        let next = self.next_cell_into(&mut qual);
        self.qual_buf = qual;
        Ok(next?.map(|family| {
            let lane = &self.lanes[self.last_lane.expect("set by next_cell_into")];
            ScanCell {
                family,
                qualifier: &self.qual_buf,
                ts: lane.ts,
                stored: lane.value(),
            }
        }))
    }

    /// Like [`ScanCursor::next_cell`], but appends the cell's unescaped qualifier to
    /// `qualifiers` (a caller's row buffer, saving a copy) and returns only its family;
    /// [`ScanCursor::current_data`] has its version and value.
    pub fn next_cell_into(&mut self, qualifiers: &mut Vec<u8>) -> Result<Option<FamilyId>> {
        if !self.in_row {
            return Ok(None);
        }
        if let Some(i) = self.last_lane.take() {
            let fetched = self.lanes[i].fetch();
            self.snapshot.checked(fetched)?;
        }
        // A column of the current row starts with its escaped row and the terminator (which
        // never occurs inside an escaped row).
        let row_len = self.row_esc.len() + TERMINATOR.len();
        while self.lane_idx < self.lanes.len() {
            let i = self.lane_idx;
            let lane = &self.lanes[i];
            let col = if lane.pending { lane.col() } else { &[] };
            if lane.pending
                && col.len() >= row_len
                && col.starts_with(&self.row_esc)
                && col[self.row_esc.len()..row_len] == TERMINATOR
            {
                self.last_lane = Some(i);
                let end = col.len().saturating_sub(2).max(row_len);
                Escaped::new(&col[row_len..end]).unescape_into(qualifiers);
                return Ok(Some(lane.family));
            }
            self.lane_idx += 1;
        }
        Ok(None)
    }

    /// The cell last returned by [`ScanCursor::next_cell`], as a pinned [`CellData`] (no
    /// copy of a large value).
    ///
    /// # Panics
    /// If no cell has been returned for the current row.
    pub fn current_data(&self) -> CellData {
        let i = self.last_lane.expect("current_data before next_cell");
        self.lanes[i].data(|| Arc::clone(&self.snapshot.view))
    }

    /// Appends the cell last returned by [`ScanCursor::next_cell`] to `sink`, as `family`
    /// with `qualifier` (a range of the sink's qualifiers): the cell of
    /// [`ScanCursor::current_data`], but a small value is copied straight into the sink's
    /// cell rather than moved there in a [`CellData`] (#287).
    ///
    /// # Panics
    /// If no cell has been returned for the current row.
    pub fn push_current(
        &self,
        family: FamilyId,
        qualifier: std::ops::Range<usize>,
        sink: &mut impl RowSink,
    ) {
        let i = self.last_lane.expect("push_current before next_cell");
        let lane = &self.lanes[i];
        let bytes = lane.value();
        if bytes.len() <= CellData::INLINE_MAX {
            sink.push_inline(family, qualifier, lane.ts, bytes);
        } else {
            sink.push(family, qualifier, self.current_data());
        }
    }
}

impl Drop for ScanCursor {
    fn drop(&mut self) {
        self.recycle_lanes();
    }
}

thread_local! {
    /// Resolvers scans refill in place, kept by each scanning thread (#287) as row reads and
    /// point gets keep theirs: a scan takes one per lane and gives it back when the lane ends
    /// (its tablet or the scan does, or the cursor is dropped). Each comes back with no source
    /// and default options, so the pool pins no memtable, SST or blob reader.
    static SCAN_RESOLVERS: RefCell<Vec<Resolver>> = const { RefCell::new(Vec::new()) };
}

/// Resolvers a thread keeps for its scans (one per family a scan reads, for a few families).
const SCAN_RESOLVERS_KEPT: usize = 4;

/// A resolver for a scan lane: one of the thread's, or a new one. The pool is sized here,
/// when a scan starts, so giving a resolver back (at its end) never allocates.
fn take_scan_resolver() -> Resolver {
    SCAN_RESOLVERS
        .try_with(|pool| {
            let mut pool = pool.borrow_mut();
            pool.reserve(SCAN_RESOLVERS_KEPT);
            pool.pop()
        })
        .ok()
        .flatten()
        .unwrap_or_else(point_resolver)
}

/// Returns a lane's resolver to the thread's pool: its sources (each pins its memtable or
/// SST) and options (merge operator, blob reader) go now, its allocations stay. Past
/// [`SCAN_RESOLVERS_KEPT`] it is dropped.
fn give_back(mut resolver: Resolver) {
    let cursor = resolver.cursor_mut();
    cursor.sources_mut().clear();
    cursor.reset();
    resolver.reset(ResolveOptions::new(0, 0));
    let _ = SCAN_RESOLVERS.try_with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.len() < SCAN_RESOLVERS_KEPT {
            pool.push(resolver);
        }
    });
}

/// [`clamp_to_tablet`] into reused buffers: `lo` and `hi` receive the clamped start and end,
/// and the result says which are set (`false` = unbounded).
fn clamp_into(
    tablet: &TabletEntry,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    lo: &mut Vec<u8>,
    hi: &mut Vec<u8>,
) -> Result<(bool, bool)> {
    lo.clear();
    let has_lo = if tablet.start.is_empty() {
        if let Some(s) = start {
            lo.extend_from_slice(s);
        }
        start.is_some()
    } else {
        encode_row_prefix(lo, &tablet.start)?;
        if let Some(s) = start
            && s > lo.as_slice()
        {
            lo.clear();
            lo.extend_from_slice(s);
        }
        true
    };
    hi.clear();
    let has_hi = match &tablet.end {
        None => {
            if let Some(e) = end {
                hi.extend_from_slice(e);
            }
            end.is_some()
        }
        Some(e) => {
            encode_row_prefix(hi, e)?;
            if let Some(x) = end
                && x < hi.as_slice()
            {
                hi.clear();
                hi.extend_from_slice(x);
            }
            true
        }
    };
    Ok((has_lo, has_hi))
}

/// Narrows `[start, end)` (row prefixes, `None` = unbounded) to the rows of `tablet`.
pub(crate) fn clamp_to_tablet(
    tablet: &TabletEntry,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
) -> Result<RowBounds> {
    let start = if tablet.start.is_empty() {
        start
    } else {
        let mut k = Vec::new();
        encode_row_prefix(&mut k, &tablet.start)?;
        Some(match start {
            Some(s) if s > k => s,
            _ => k,
        })
    };
    let end = match &tablet.end {
        None => end,
        Some(e) => {
            let mut k = Vec::new();
            encode_row_prefix(&mut k, e)?;
            Some(match end {
                Some(x) if x < k => x,
                _ => k,
            })
        }
    };
    Ok((start, end))
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
    let mut out = RowData {
        row: row.to_vec(),
        ..RowData::default()
    };
    let any = read_row_into(
        &snapshot.view,
        snapshot.seqno,
        table,
        row,
        families,
        spec,
        now,
        false,
        &mut out,
    )?;
    Ok(any.then_some(out))
}

thread_local! {
    /// The resolver row reads refill in place, one per reading thread (#287), as point gets
    /// refill theirs: its merging cursor's source list and heap and its scratch are reused.
    /// Between reads it holds no source and default options, so it pins no memtable, SST or
    /// blob reader. Taken while in use, so a read nested in another gets a resolver of its
    /// own.
    static ROW_RESOLVER: std::cell::Cell<Option<Resolver>> = const { std::cell::Cell::new(None) };
}

/// Reads one row through `view` at `seqno` into `sink`: every family in `families` order.
/// Returns whether any cell was found. A value pinned rather than copied pins `view`.
#[allow(clippy::too_many_arguments)]
///
/// `cache_only` (an async read, ICR 0014) changes only how each family's sources are
/// gathered, so it is a runtime flag rather than a second instance: with two instances the
/// per-cell loop stopped inlining `CellResolver::next_cell` into the sync one (#430, +2%
/// instructions per row).
pub(crate) fn read_row_into(
    view: &Arc<View>,
    seqno: Seqno,
    table: TableId,
    row: &[u8],
    families: &[FamilyId],
    spec: &ReadSpec,
    now: Timestamp,
    cache_only: bool,
    sink: &mut impl RowSink,
) -> Result<bool> {
    let mut resolver = ROW_RESOLVER
        .try_with(std::cell::Cell::take)
        .ok()
        .flatten()
        .unwrap_or_else(point_resolver);
    let mut large = false;
    let read = read_row_with(
        &mut resolver,
        view,
        seqno,
        table,
        row,
        families,
        spec,
        now,
        cache_only,
        sink,
        &mut large,
    );
    // Whatever happened, the sources (each pins its memtable or SST) and the options (merge
    // operator, blob reader) go now; only the allocations stay, unless a large value grew
    // them.
    let cursor = resolver.cursor_mut();
    cursor.sources_mut().clear();
    cursor.reset();
    resolver.reset(ResolveOptions::new(0, 0));
    if !large {
        let _ = ROW_RESOLVER.try_with(|slot| slot.set(Some(resolver)));
    }
    read
}

/// [`read_row_into`] with the thread's resolver. Sets `large` when a value the resolver
/// buffered was too large to keep its buffers for.
#[allow(clippy::too_many_arguments)]
// Inlined into `read_row_into`: called through a separate frame, a hot row's per-cell loop
// measured about 1.5% more instructions.
#[inline(always)]
fn read_row_with(
    resolver: &mut Resolver,
    view: &Arc<View>,
    seqno: Seqno,
    table: TableId,
    row: &[u8],
    families: &[FamilyId],
    spec: &ReadSpec,
    now: Timestamp,
    cache_only: bool,
    sink: &mut impl RowSink,
    large: &mut bool,
) -> Result<bool> {
    let Some((tablet, shard)) = view.tablets().route(table, row) else {
        return Err(Error::TableNotFound(format!("table {}", table.0)));
    };
    // Inline for rows of usual length: the setup allocates nothing per read.
    let prefix = row_prefix(row)?;
    let mut past = prefix.clone();
    if let Some(last) = past.last_mut() {
        *last += 1;
    }
    let mut any = false;
    #[cfg(feature = "test-hooks")]
    let mut first = true;
    for &family in families {
        #[cfg(feature = "test-hooks")]
        {
            if !first {
                run_between_row_families();
            }
            first = false;
        }
        let Some(meta) = view.catalog.family(family) else {
            continue;
        };
        let (mut opts, filter) = spec.resolve_opts(meta, seqno, now);
        let resolver_blobs = ResolverBlobs::attach(&mut opts, &view.ssts);
        let cursor = resolver.cursor_mut();
        cursor.sources_mut().clear();
        if cache_only {
            row_sources_cache_only(
                view,
                shard,
                tablet,
                family,
                &filter,
                &prefix,
                cursor.sources_mut(),
            )?;
        } else {
            view.row_sources_into::<false>(
                shard,
                tablet,
                family,
                &filter,
                &prefix,
                cursor.sources_mut(),
            )?;
        }
        if cursor.sources().is_empty() {
            continue;
        }
        opts.skip_columns = skips_columns(cursor.sources());
        cursor.reset();
        resolver.reset(opts);
        resolver.set_upper_bound(Some(&past));
        resolver.seek(&prefix)?;
        loop {
            let (data, qualifier) = {
                let next = resolver.next_cell();
                ResolverBlobs::check(resolver_blobs.as_ref())?;
                let Some(cell) = next.map_err(|e| read_error(e, meta))? else {
                    break;
                };
                if !cell.key.starts_with(&prefix) {
                    break;
                }
                *large |= !cell.from_source && cell.value.len() > KEEP_BUFFERS_BELOW;
                // The qualifier goes straight into the sink, unescaped from the key.
                let column = column_of(cell.key);
                let end = column.len().saturating_sub(2).max(prefix.len());
                let qualifiers = sink.qualifiers();
                let start = qualifiers.len();
                Escaped::new(&column[prefix.len()..end]).unescape_into(qualifiers);
                let qualifier = start..qualifiers.len();
                let data = if cell.from_source && cell.value.len() > CellData::INLINE_MAX {
                    // Pinned below, once the borrow of the resolver ends.
                    None
                } else if cell.value.len() <= CellData::INLINE_MAX
                    && blob_pointer(cell.value).is_none()
                {
                    // Small and not separated: copied straight into the sink's cell.
                    sink.push_inline(family, qualifier, cell.ts, cell.value);
                    any = true;
                    continue;
                } else {
                    Some(CellData::resolved(&cell, &view.ssts, || Arc::clone(view))?)
                };
                (data, qualifier)
            };
            let data = match data {
                Some(d) => d,
                None => {
                    let src = resolver
                        .cursor()
                        .current()
                        .expect("the merged cursor is on the returned entry");
                    let ts = {
                        let (_, ts, _, _) = pigeonhole_format::key::split_suffix(src.key())?;
                        ts
                    };
                    let value = LaneValue::Pinned(src.pin_value());
                    CellData::from_pinned(ts, &value, || Arc::clone(view))
                }
            };
            sink.push(family, qualifier, data);
            any = true;
        }
    }
    Ok(any)
}

/// A row read's sources for one family, gathered cache-only (an async read): out of the
/// sync row read's way.
#[cold]
#[inline(never)]
fn row_sources_cache_only(
    view: &View,
    shard: pigeonhole_runtime::ShardId,
    tablet: pigeonhole_format::TabletId,
    family: FamilyId,
    filter: &ScanFilter,
    prefix: &[u8],
    out: &mut Vec<Source>,
) -> Result<()> {
    view.row_sources_into::<true>(shard, tablet, family, filter, prefix, out)
}

#[cfg(feature = "test-hooks")]
thread_local! {
    /// Runs once on this thread, in the next row read, before it reads its second family
    /// (`Engine::between_row_read_families`; test hook).
    pub(crate) static BETWEEN_ROW_FAMILIES: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(feature = "test-hooks")]
fn run_between_row_families() {
    let f = BETWEEN_ROW_FAMILIES.with(|h| h.borrow_mut().take());
    if let Some(f) = f {
        f();
    }
}

thread_local! {
    /// The resolver point gets refill in place, one per reading thread (#46): its merging
    /// cursor's source list and heap and its scratch are reused, and nothing is moved. Between
    /// gets it holds no source and default options, so it pins no memtable, SST or blob
    /// reader.
    static POINT_RESOLVER: RefCell<Option<Resolver>> = const { RefCell::new(None) };
}

/// A point get returning a longer value drops the thread's resolver (and the buffers that
/// value may have grown) instead of keeping it for the next get.
const KEEP_BUFFERS_BELOW: usize = 64 << 10;

/// An empty resolver for point gets.
fn point_resolver() -> Resolver {
    Resolver::new(MergingCursor::new(Vec::new()), ResolveOptions::new(0, 0))
}

/// A point get through `view` at `seqno`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn get_in<const CACHE_ONLY: bool>(
    view: &View,
    seqno: Seqno,
    now: impl FnOnce() -> Timestamp,
    table: TableId,
    family: FamilyId,
    row: &[u8],
    qualifier: &[u8],
    pin: impl FnOnce() -> Arc<View>,
) -> Result<Option<CellData>> {
    let mut run = Some(|resolver: &mut Resolver| {
        let got = get_with::<CACHE_ONLY>(
            resolver, view, seqno, now, table, family, row, qualifier, pin,
        );
        // Whatever happened, the sources (each pins its memtable or SST) and the options
        // (merge operator, blob reader) go now; only the allocations stay.
        let cursor = resolver.cursor_mut();
        cursor.sources_mut().clear();
        cursor.reset();
        resolver.reset(ResolveOptions::new(0, 0));
        got
    });
    // A get nested in another on the same thread, or one during thread teardown, uses a
    // resolver of its own.
    let in_place = POINT_RESOLVER.try_with(|cell| {
        let mut slot = cell.try_borrow_mut().ok()?;
        let run = run.take().expect("not run yet");
        let got = run(slot.get_or_insert_with(point_resolver));
        // A large result may have been folded in the resolver's buffers (merge operands are
        // copied whatever their size): not worth keeping that much per thread.
        if got.as_ref().is_ok_and(|c| {
            c.as_ref()
                .is_some_and(|c| c.stored().len() > KEEP_BUFFERS_BELOW)
        }) {
            *slot = None;
        }
        Some(got)
    });
    match in_place {
        Ok(Some(got)) => got,
        _ => (run.take().expect("not run yet"))(&mut point_resolver()),
    }
}

/// Whether a read over `sources` should pass finished columns with `skip_column` (D194):
/// only when the index is on and a memtable source can jump, so a read of SSTs alone never
/// pays for a try that cannot succeed.
fn skips_columns(sources: &[Source]) -> bool {
    #[cfg(feature = "test-hooks")]
    if crate::shard::READS_STEP.load(std::sync::atomic::Ordering::Relaxed) {
        return false;
    }
    sources
        .iter()
        .any(|s| matches!(s, Source::Mem(m) if m.inner().skips_columns()))
}

/// A point get's resolver options: as `ReadSpec { versions: 1, .. }.resolve_opts`, built
/// directly (no spec, filter or time range to make and drop on every get).
fn point_opts(meta: &FamilyMeta, seqno: Seqno, now: Timestamp) -> ResolveOptions {
    let mut opts = ResolveOptions::new(seqno, now);
    opts.ttl_micros = meta.options.ttl_micros;
    // `versions: 1` folded with the family's `max_versions` (0 = unlimited): always 1.
    opts.versions = 1;
    opts.merge = meta.merge_op.clone();
    opts.counter = meta.options.kind == FamilyKind::Counter;
    opts
}

/// How a point get's resolution ended.
enum Resolved {
    Done(Option<CellData>),
    /// A large put the resolver copied: re-found by its key (in the caller's buffers) and
    /// pinned, at this timestamp.
    Refind(Timestamp),
}

#[allow(clippy::too_many_arguments)]
fn get_with<const CACHE_ONLY: bool>(
    resolver: &mut Resolver,
    view: &View,
    seqno: Seqno,
    now: impl FnOnce() -> Timestamp,
    table: TableId,
    family: FamilyId,
    row: &[u8],
    qualifier: &[u8],
    pin: impl FnOnce() -> Arc<View>,
) -> Result<Option<CellData>> {
    let Some((tablet, shard)) = view.tablets().route(table, row) else {
        return Err(Error::TableNotFound(format!("table {}", table.0)));
    };
    let Some(meta) = view.catalog.family(family) else {
        return Err(Error::FamilyNotFound(format!("family {}", family.0)));
    };
    if meta.table != table {
        return Err(Error::FamilyNotFound(format!("family {}", family.0)));
    }
    // Encoded once, for the filter probes and the resolver.
    let mut key = ColumnKey::new(row, qualifier);
    // The caller clears the sources afterwards, whatever this returns.
    let sources = resolver.cursor_mut().sources_mut();
    let markers = match view.point_sources::<CACHE_ONLY>(shard, tablet, family, &mut key, sources) {
        Ok(_) if sources.is_empty() => return Ok(None),
        Ok(markers) => markers,
        Err(e) => return Err(e),
    };
    // The clock only for a family with a TTL, the one thing a point get uses it for.
    let now = if meta.options.ttl_micros == 0 {
        0
    } else {
        now()
    };
    let mut opts = point_opts(meta, seqno, now);
    let resolver_blobs = ResolverBlobs::attach(&mut opts, &view.ssts);
    resolver.cursor_mut().reset();
    resolver.reset(opts);
    let mut key_buf = [0u8; 512];
    let mut key_vec: Vec<u8> = Vec::new();
    let mut key_len = 0;
    let mut pin = Some(pin);
    let resolved = resolve_point(
        resolver,
        view,
        meta,
        resolver_blobs.as_ref(),
        &key,
        markers,
        (&mut key_buf, &mut key_vec, &mut key_len),
        &mut pin,
    );
    let cursor = resolver.cursor_mut();
    match resolved {
        Ok(Resolved::Done(cell)) => Ok(cell),
        Err(e) => Err(e),
        Ok(Resolved::Refind(ts)) => {
            let key: &[u8] = if key_vec.is_empty() {
                &key_buf[..key_len]
            } else {
                &key_vec
            };
            match cursor.seek(key) {
                Err(e) => Err(e),
                Ok(()) if cursor.valid() && cursor.key() == key => match cursor.current() {
                    Some(src) => {
                        let value = LaneValue::Pinned(src.pin_value());
                        let pin = pin.take().expect("not used yet");
                        Ok(Some(CellData::from_pinned(ts, &value, pin)))
                    }
                    None => Err(missing_entry()),
                },
                Ok(()) => Err(missing_entry()),
            }
        }
    }
}

fn missing_entry() -> Error {
    Error::Corruption("point get: a resolved entry is not in its sources".into())
}

/// Resolves a point get's column. A value above the inline threshold is pinned, not copied
/// (D29). The resolver copies values up to its own limit while it reads the group; a copy of
/// a put (the output key names exactly that entry; a fold's key is an operand's) is re-found
/// with one seek and pinned, so the pinned bytes are the version itself: its key goes to
/// `key` and the caller seeks.
#[allow(clippy::too_many_arguments)]
fn resolve_point<P: FnOnce() -> Arc<View>>(
    resolver: &mut Resolver,
    view: &View,
    meta: &FamilyMeta,
    resolver_blobs: Option<&Arc<ResolverBlobs>>,
    column: &ColumnKey,
    markers: bool,
    key: (&mut [u8; 512], &mut Vec<u8>, &mut usize),
    pin: &mut Option<P>,
) -> Result<Resolved> {
    let (key_buf, key_vec, key_len) = key;
    if markers {
        resolver.seek_column_encoded(column.prefix(), column.row_len())?;
    } else {
        // No source can hold a delete marker: straight to the column (ICR 0020).
        resolver.seek_column_encoded_unmarked(column.prefix(), column.row_len())?;
    }
    let (ts, refind) = {
        let next = resolver.next_cell();
        ResolverBlobs::check(resolver_blobs)?;
        let Some(cell) = next.map_err(|e| read_error(e, meta))? else {
            return Ok(Resolved::Done(None));
        };
        if cell.value.len() <= CellData::INLINE_MAX {
            let pin = pin.take().expect("not used yet");
            return Ok(Resolved::Done(Some(CellData::resolved(
                &cell, &view.ssts, pin,
            )?)));
        }
        if cell.from_source {
            (cell.ts, false)
        } else if decode_key(cell.key).is_ok_and(|k| k.kind == Kind::Put) {
            *key_len = cell.key.len();
            if *key_len <= key_buf.len() {
                key_buf[..*key_len].copy_from_slice(cell.key);
            } else {
                key_vec.extend_from_slice(cell.key);
            }
            (cell.ts, true)
        } else {
            let pin = pin.take().expect("not used yet");
            return Ok(Resolved::Done(Some(CellData::from_cell(&cell, None, pin))));
        }
    };
    if refind {
        return Ok(Resolved::Refind(ts));
    }
    let src = resolver
        .cursor()
        .current()
        .expect("the merged cursor is on the returned entry");
    let value = LaneValue::Pinned(src.pin_value());
    let pin = pin.take().expect("not used yet");
    Ok(Resolved::Done(Some(CellData::from_pinned(ts, &value, pin))))
}

/// Whether a stored value satisfies a predicate (decision D77).
pub(crate) fn predicate_matches(p: &ValuePredicate, stored: &[u8]) -> bool {
    p.matches(stored)
}

#[cfg(test)]
mod tests {
    //! The resolver rules as the engine relies on them (snapshot visibility, the delete rules
    //! D9/D38, TTL, merge folding D41, limits and filters), run through the engine's
    //! `Source` enum against `pigeonhole-compaction`'s shared `CellResolver`.

    use std::sync::Arc;

    use pigeonhole_compaction::{I64Add, MergingCursor, ResolveOptions, VecCursor};
    use pigeonhole_format::key::{Kind, decode_key, encode_key, encode_marker_key};
    use pigeonhole_format::value::{ValueRef, decode_value, encode_value};

    use crate::source::{Resolver, Source};
    use crate::{Error, ValuePredicate};

    fn put(row: &str, q: &str, ts: u64, seqno: u64, v: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let mut k = Vec::new();
        encode_key(&mut k, row.as_bytes(), q.as_bytes(), ts, seqno, Kind::Put).unwrap();
        let mut val = Vec::new();
        // Eight raw bytes are written as a tagged `i64` (what `put_i64` does).
        match <[u8; 8]>::try_from(v) {
            Ok(b) => encode_value(&mut val, ValueRef::I64(i64::from_le_bytes(b))),
            Err(_) => encode_value(&mut val, ValueRef::Bytes(v)),
        }
        (k, val)
    }

    fn incr(row: &str, q: &str, ts: u64, seqno: u64, d: i64) -> (Vec<u8>, Vec<u8>) {
        let mut k = Vec::new();
        encode_key(&mut k, row.as_bytes(), q.as_bytes(), ts, seqno, Kind::Merge).unwrap();
        let mut val = Vec::new();
        encode_value(&mut val, ValueRef::I64(d));
        (k, val)
    }

    fn del(row: &str, q: &str, ts: u64, seqno: u64, kind: Kind) -> (Vec<u8>, Vec<u8>) {
        let mut k = Vec::new();
        encode_key(&mut k, row.as_bytes(), q.as_bytes(), ts, seqno, kind).unwrap();
        (k, Vec::new())
    }

    fn marker(row: &str, ts: u64, seqno: u64) -> (Vec<u8>, Vec<u8>) {
        let mut k = Vec::new();
        encode_marker_key(&mut k, row.as_bytes(), ts, seqno).unwrap();
        (k, Vec::new())
    }

    type Cell = (String, String, u64, Vec<u8>);

    fn cell(key: &[u8], ts: u64, value: &[u8]) -> Cell {
        let parts = decode_key(key).unwrap();
        let (mut r, mut q) = (Vec::new(), Vec::new());
        parts.row.unescape_into(&mut r);
        parts.qualifier.unwrap().unescape_into(&mut q);
        let v = match decode_value(value).unwrap() {
            ValueRef::Bytes(b) => b.to_vec(),
            ValueRef::I64(x) => x.to_le_bytes().to_vec(),
            other => panic!("{other:?}"),
        };
        (
            String::from_utf8(r).unwrap(),
            String::from_utf8(q).unwrap(),
            ts,
            v,
        )
    }

    fn resolver(sources: Vec<Vec<(Vec<u8>, Vec<u8>)>>, opts: ResolveOptions) -> Resolver {
        let sources = sources
            .into_iter()
            .map(|e| Source::Vec(VecCursor::new(e)))
            .collect();
        Resolver::new(MergingCursor::new(sources), opts)
    }

    fn collect(src: Vec<(Vec<u8>, Vec<u8>)>, opts: ResolveOptions) -> Vec<Cell> {
        let mut r = resolver(vec![src], opts);
        r.seek(b"").unwrap();
        let mut out = Vec::new();
        while let Some(c) = r.next_cell().unwrap() {
            out.push(cell(c.key, c.ts, c.value));
        }
        out
    }

    fn opts(snapshot: u64) -> ResolveOptions {
        let mut o = ResolveOptions::new(snapshot, 1_000);
        o.versions = 0;
        o.merge = Some(Arc::new(I64Add));
        o
    }

    fn c(row: &str, q: &str, ts: u64, v: &[u8]) -> Cell {
        (row.into(), q.into(), ts, v.to_vec())
    }

    #[test]
    fn newest_first_and_snapshot() {
        let src = vec![
            put("r", "q", 10, 1, b"a"),
            put("r", "q", 20, 2, b"b"),
            put("r", "q", 15, 3, b"c"),
        ];
        assert_eq!(
            collect(src.clone(), opts(3)),
            vec![
                c("r", "q", 20, b"b"),
                c("r", "q", 15, b"c"),
                c("r", "q", 10, b"a")
            ]
        );
        assert_eq!(collect(src.clone(), opts(1)), vec![c("r", "q", 10, b"a")]);
        let mut one = opts(3);
        one.versions = 1;
        assert_eq!(collect(src, one), vec![c("r", "q", 20, b"b")]);
    }

    #[test]
    fn cell_delete_hides_exact_timestamp_whatever_the_seqno() {
        let src = vec![
            put("r", "q", 10, 1, b"a"),
            del("r", "q", 10, 2, Kind::CellDelete),
            put("r", "q", 10, 3, b"later"),
            put("r", "q", 9, 4, b"old"),
        ];
        assert_eq!(collect(src.clone(), opts(4)), vec![c("r", "q", 9, b"old")]);
        assert_eq!(collect(src, opts(1)), vec![c("r", "q", 10, b"a")]);
    }

    #[test]
    fn column_delete_hides_by_timestamp_not_seqno() {
        let src = vec![
            put("r", "q", 10, 1, b"a"),
            put("r", "q", 30, 2, b"c"),
            del("r", "q", 20, 3, Kind::ColumnDelete),
            put("r", "q", 15, 4, b"late-old"),
            put("r", "q", 20, 5, b"at-delete"),
            put("r", "z", 1, 6, b"next-column"),
        ];
        assert_eq!(
            collect(src, opts(6)),
            vec![c("r", "q", 30, b"c"), c("r", "z", 1, b"next-column")]
        );
    }

    #[test]
    fn family_marker_hides_row_cells_at_or_below_it() {
        let src = vec![
            put("r", "a", 10, 1, b"a"),
            put("r", "b", 30, 1, b"b"),
            marker("r", 20, 2),
            put("r", "a", 20, 3, b"a2"),
            put("s", "a", 5, 1, b"other-row"),
        ];
        assert_eq!(
            collect(src.clone(), opts(3)),
            vec![c("r", "b", 30, b"b"), c("s", "a", 5, b"other-row")]
        );
        assert_eq!(
            collect(src, opts(1)),
            vec![
                c("r", "a", 10, b"a"),
                c("r", "b", 30, b"b"),
                c("s", "a", 5, b"other-row")
            ]
        );
    }

    #[test]
    fn ttl_expires_at_the_boundary() {
        let src = vec![put("r", "q", 900, 1, b"old"), put("r", "q", 950, 2, b"new")];
        let mut o = opts(2);
        o.ttl_micros = 100; // now = 1000: 900 + 100 <= 1000 expired, 950 lives
        assert_eq!(collect(src, o), vec![c("r", "q", 950, b"new")]);
    }

    #[test]
    fn merge_folds_onto_base_and_runs() {
        let base = 5i64.to_le_bytes();
        let src = vec![
            put("r", "c", 10, 1, &base),
            incr("r", "c", 20, 2, 3),
            incr("r", "c", 30, 3, 4),
            put("r", "d", 10, 1, &base),
            incr("r", "d", 10, 2, 1),
            incr("r", "e", 7, 1, 2),
        ];
        assert_eq!(
            collect(src.clone(), opts(3)),
            vec![
                c("r", "c", 30, &12i64.to_le_bytes()),
                c("r", "d", 10, &6i64.to_le_bytes()),
                c("r", "e", 7, &2i64.to_le_bytes()),
            ]
        );
        // A snapshot before the operands returns the base as written.
        assert_eq!(collect(src, opts(1))[0], c("r", "c", 10, &base));
    }

    #[test]
    fn merge_onto_non_i64_base_fails_only_when_returned() {
        let src = vec![
            put("r", "c", 10, 1, b"bad"),
            incr("r", "c", 20, 2, 3),
            put("r", "c", 30, 3, b"newest"),
        ];
        let mut o = opts(3);
        o.versions = 1;
        assert_eq!(collect(src.clone(), o), vec![c("r", "c", 30, b"newest")]);
        let mut r = resolver(vec![src], opts(3));
        r.seek(b"").unwrap();
        assert!(r.next_cell().unwrap().is_some());
        assert!(matches!(r.next_cell(), Err(Error::Merge(_))));
    }

    #[test]
    fn point_get_sees_markers_before_the_column() {
        let src = vec![
            put("r", "q", 10, 1, b"a"),
            marker("r", 10, 2),
            put("r", "q", 11, 3, b"b"),
            put("r", "q", 9, 4, b"hidden"),
            put("r", "r", 50, 5, b"other-column"),
        ];
        let mut r = resolver(vec![src], opts(5));
        r.seek_column(b"r", b"q").unwrap();
        let ts = r.next_cell().unwrap().unwrap().ts;
        assert_eq!(ts, 11);
        assert!(r.next_cell().unwrap().is_none());
        assert!(r.next_cell().unwrap().is_none());
        r.seek_column(b"r", b"zz").unwrap();
        assert!(r.next_cell().unwrap().is_none());
        r.seek_column(b"r", b"r").unwrap();
        assert_eq!(r.next_cell().unwrap().unwrap().ts, 50);
    }

    #[test]
    fn columns_per_row_and_max_versions() {
        let src = vec![
            put("r", "a", 1, 1, b"1"),
            put("r", "a", 2, 2, b"2"),
            put("r", "b", 1, 1, b"x"),
            put("r", "c", 1, 1, b"y"),
            put("s", "a", 1, 1, b"s"),
        ];
        let mut o = opts(2);
        o.versions = 1; // the family's max_versions folded in (D76)
        o.columns_per_row = 2;
        assert_eq!(
            collect(src, o),
            vec![
                c("r", "a", 2, b"2"),
                c("r", "b", 1, b"x"),
                c("s", "a", 1, b"s")
            ]
        );
    }

    #[test]
    fn value_predicate_tests_the_newest_version() {
        let src = vec![
            put("r", "a", 1, 1, b"yes"),
            put("r", "a", 2, 2, b"no"),
            put("r", "b", 1, 1, b"yes"),
        ];
        let mut o = opts(2);
        o.value = Some(ValuePredicate::Equals(b"yes".to_vec()));
        assert_eq!(collect(src, o), vec![c("r", "b", 1, b"yes")]);
    }

    #[test]
    fn qualifier_filter_skips_columns() {
        let src = vec![
            put("r", "a", 1, 1, b"1"),
            put("r", "meta:x", 1, 1, b"2"),
            put("r", "meta:y", 1, 1, b"3"),
            put("r", "z", 1, 1, b"4"),
            put("s", "meta:z", 1, 1, b"5"),
        ];
        // The filter is applied by the sources (FilteredCursor / SstIter), not the resolver:
        // wrap the in-memory source the way memtables are wrapped.
        let mut filter = pigeonhole_format::scan::ScanFilter::all();
        filter.qualifiers = pigeonhole_format::scan::QualifierFilter::Prefix(b"meta:".to_vec());
        let filtered = pigeonhole_compaction::FilteredCursor::new(VecCursor::new(src), filter);
        let mut out = Vec::new();
        let mut r = pigeonhole_compaction::CellResolver::new(filtered, opts(2));
        r.seek(b"").unwrap();
        while let Some(cell_) = r.next_cell().unwrap() {
            out.push(cell(cell_.key, cell_.ts, cell_.value));
        }
        assert_eq!(
            out,
            vec![
                c("r", "meta:x", 1, b"2"),
                c("r", "meta:y", 1, b"3"),
                c("s", "meta:z", 1, b"5")
            ]
        );
    }

    #[test]
    fn merged_sources_interleave_in_key_order() {
        let a = vec![put("r", "q", 20, 2, b"new"), put("s", "q", 1, 4, b"s")];
        let b = vec![put("r", "q", 10, 1, b"old"), put("r", "z", 5, 3, b"z")];
        let mut r = resolver(vec![a, b], opts(4));
        r.seek(b"").unwrap();
        let mut out = Vec::new();
        while let Some(c) = r.next_cell().unwrap() {
            out.push(cell(c.key, c.ts, c.value));
        }
        assert_eq!(
            out,
            vec![
                c("r", "q", 20, b"new"),
                c("r", "q", 10, b"old"),
                c("r", "z", 5, b"z"),
                c("s", "q", 1, b"s")
            ]
        );
    }

    #[test]
    fn set_inline_overwrites_any_cell_in_place() {
        use super::{CellData, CellValue};
        let stored = |v: &[u8]| {
            let mut out = Vec::new();
            encode_value(&mut out, ValueRef::Bytes(v));
            out
        };
        // From the empty cell, as `RowSink::push_inline` builds it.
        let mut d = CellData::EMPTY;
        d.set_inline(7, &stored(b"abc"));
        assert_eq!((d.timestamp(), d.value()), (7, ValueRef::Bytes(b"abc")));
        // A shorter value over a longer one keeps only its own bytes.
        d.set_inline(8, &stored(b"x"));
        assert_eq!((d.timestamp(), d.value()), (8, ValueRef::Bytes(b"x")));
        // Over a cell that held a heap value.
        let mut d = CellData {
            ts: 1,
            value: CellValue::Owned(stored(&[9; 300])),
        };
        let full = vec![5; CellData::INLINE_MAX - 1];
        d.set_inline(2, &stored(&full));
        assert!(matches!(d.value, CellValue::Inline { .. }));
        assert_eq!((d.timestamp(), d.value()), (2, ValueRef::Bytes(&full[..])));
    }
}

thread_local! {
    /// While an async read's attempt runs on this thread: whether it is cache-only, and the
    /// file reads it made synchronously so far (D196), counted into
    /// `Metrics::async_sync_reads`.
    static ASYNC_READ: std::cell::Cell<Option<(bool, u64)>> =
        const { std::cell::Cell::new(None) };
}

/// The running async read's mode on this thread: `Some(cache_only)`, or `None` outside one.
pub(crate) fn async_read_mode() -> Option<bool> {
    ASYNC_READ.with(|c| c.get().map(|(cache_only, _)| cache_only))
}

/// Counts a file read the running async read makes synchronously.
pub(crate) fn note_sync_read() {
    ASYNC_READ.with(|c| {
        if let Some((cache_only, n)) = c.get() {
            c.set(Some((cache_only, n + 1)));
        }
    });
}

/// Runs `f` as an async read's attempt (`cache_only`, or a synchronous fallback); returns
/// its result and the file reads it made synchronously. The flag is restored even if `f`
/// panics.
pub(crate) fn as_async_read<T>(cache_only: bool, f: impl FnOnce() -> T) -> (T, u64) {
    struct Restore(Option<(bool, u64)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            ASYNC_READ.with(|c| c.set(self.0));
        }
    }
    let restore = Restore(ASYNC_READ.with(|c| c.replace(Some((cache_only, 0)))));
    let out = f();
    let n = ASYNC_READ.with(|c| c.get()).map_or(0, |(_, n)| n);
    drop(restore);
    (out, n)
}
