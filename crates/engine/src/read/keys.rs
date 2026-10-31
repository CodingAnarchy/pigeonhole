//! Frontier multi-get (D299, #489): many rows read in key order under one snapshot, with the
//! read set up once per tablet rather than once per row.
//!
//! A [`KeySet`] is sorted and distinct. [`key_groups`] cuts it into runs of keys that route
//! to the same tablet. For each run, per family, a [`KeyCursor`] keeps its memtable sources
//! and one SST iterator per SST it has read from, and reuses them for every key of the run:
//!
//! - an SST takes part in a key's read only if the row is within its key range and its row
//!   filter may hold the row, as for a point row read (`sst_sources_row`);
//! - a source that takes part moves forward to the row: `Cursor::seek_forward` from where the
//!   previous key left it (usually inside the block it already decoded), a full seek if it is
//!   new or ran past its end;
//! - the merge is rebuilt over just those sources ([`MergingCursor::rebuild_positioned`]),
//!   and the family's resolver reads the row as `read_row_with` does.
//!
//! The scan's and the row read's own code are untouched (D296's R3): this is a sibling that
//! drives the same [`Lane`]s.
//!
//! [`MergingCursor::rebuild_positioned`]: pigeonhole_compaction::MergingCursor::rebuild_positioned

use std::ops::Range;
use std::sync::Arc;

use pigeonhole_format::Cursor;
use pigeonhole_format::filter::row_hash;
use pigeonhole_format::key::{Escaped, TERMINATOR};
use pigeonhole_format::scan::ScanFilter;
use pigeonhole_format::{FamilyId, TableId, TabletId, Timestamp};
use pigeonhole_runtime::ShardId;
use pigeonhole_sst::SstReader;

use super::{
    CellData, FORK_CHECK_ROWS, Lane, MergeKind, ReadSpec, ResolverBlobs, RowSink, ScanCell,
    give_back, skips_columns, take_scan_resolver,
};
use crate::snapshot::{OpenSst, Snapshot};
use crate::source::{Source, mem_sources, read_options, row_prefix};
use crate::{Error, Result};

/// A sorted set of distinct row keys in one flat buffer: the input of a multi-get (D299) and
/// of a bounded read's `Keys` input (D296).
///
/// ```
/// use pigeonhole_engine::KeySet;
///
/// let keys = KeySet::from_unsorted([&b"c"[..], b"a", b"c", b"b"]);
/// assert_eq!(keys.len(), 3);
/// assert_eq!(keys.iter().collect::<Vec<_>>(), [&b"a"[..], b"b", b"c"]);
/// assert!(KeySet::from_sorted([&b"b"[..], b"a"]).is_err());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct KeySet {
    bytes: Vec<u8>,
    /// Where each key ends in `bytes`.
    ends: Vec<usize>,
}

impl KeySet {
    /// The set of `keys`, sorted and deduplicated.
    pub fn from_unsorted<K: AsRef<[u8]>>(keys: impl IntoIterator<Item = K>) -> Self {
        let mut flat = Vec::new();
        let mut spans: Vec<Range<usize>> = Vec::new();
        for k in keys {
            let start = flat.len();
            flat.extend_from_slice(k.as_ref());
            spans.push(start..flat.len());
        }
        spans.sort_unstable_by(|a, b| flat[a.clone()].cmp(&flat[b.clone()]));
        spans.dedup_by(|a, b| flat[a.clone()] == flat[b.clone()]);
        let mut out = KeySet {
            bytes: Vec::with_capacity(spans.iter().map(|s| s.len()).sum()),
            ends: Vec::with_capacity(spans.len()),
        };
        for s in spans {
            out.bytes.extend_from_slice(&flat[s]);
            out.ends.push(out.bytes.len());
        }
        out
    }

    /// The set of `keys`, which must already be strictly increasing (`InvalidArgument`
    /// otherwise).
    pub fn from_sorted<K: AsRef<[u8]>>(keys: impl IntoIterator<Item = K>) -> Result<Self> {
        let mut out = KeySet::default();
        for k in keys {
            let k = k.as_ref();
            if !out.is_empty() && out.get(out.len() - 1) >= k {
                return Err(Error::InvalidArgument(
                    "a key set must be strictly increasing (KeySet::from_unsorted sorts)"
                        .to_owned(),
                ));
            }
            out.bytes.extend_from_slice(k);
            out.ends.push(out.bytes.len());
        }
        Ok(out)
    }

    /// The number of keys.
    pub fn len(&self) -> usize {
        self.ends.len()
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    /// The `i`th key (panics past the end).
    pub fn get(&self, i: usize) -> &[u8] {
        let start = if i == 0 { 0 } else { self.ends[i - 1] };
        &self.bytes[start..self.ends[i]]
    }

    /// The keys, in order.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> + '_ {
        (0..self.len()).map(|i| self.get(i))
    }
}

/// A run of keys that route to one tablet in a view: `keys` indexes the [`KeySet`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyGroup {
    pub(crate) tablet: TabletId,
    pub(crate) shard: ShardId,
    pub(crate) keys: Range<usize>,
}

/// Cuts `keys` into runs of keys that route to the same tablet of `table` in `snapshot`'s
/// view, in key order.
pub(crate) fn key_groups(
    snapshot: &Snapshot,
    table: TableId,
    keys: &KeySet,
) -> Result<Vec<KeyGroup>> {
    let mut out: Vec<KeyGroup> = Vec::new();
    for i in 0..keys.len() {
        let (tablet, shard) = snapshot
            .view
            .tablets()
            .route(table, keys.get(i))
            .ok_or_else(|| Error::TableNotFound(format!("table {}", table.0)))?;
        match out.last_mut() {
            Some(g) if g.tablet == tablet => g.keys.end = i + 1,
            _ => out.push(KeyGroup {
                tablet,
                shard,
                keys: i..i + 1,
            }),
        }
    }
    Ok(out)
}

/// One SST a family's read in the current group may use, with its reader and iterator kept
/// from key to key.
struct SstSlot {
    sst: Arc<OpenSst>,
    reader: Option<Arc<SstReader>>,
    source: Option<Source>,
}

/// Where a source in a lane's merge came from, to put it back after the key.
#[derive(Clone, Copy)]
enum Slot {
    Mem(usize),
    Sst(usize),
}

/// A family's read within one group: its lane (resolver and held cell) and the sources it
/// keeps across keys.
struct FamilyRead {
    lane: Lane,
    filter: ScanFilter,
    opts: pigeonhole_compaction::ResolveOptions,
    priority: pigeonhole_cache::Priority,
    mems: Vec<Option<Source>>,
    /// The family's SSTs in the tablet, by level (level 0 newest first), flattened; `levels`
    /// gives each level's range.
    ssts: Vec<SstSlot>,
    levels: Vec<Range<usize>>,
    /// Per level, `FamilySsts::level_start`'s answer needs the level's files: kept here.
    fam: Option<Arc<crate::snapshot::FamilySsts>>,
    /// The sources in the lane's merge for the current key, in order.
    in_merge: Vec<Slot>,
}

/// A multi-get over a snapshot (D299): positioned on one key at a time ([`KeyCursor::next_key`]),
/// its cells handed out one at a time like a scan's, by family, then qualifier, then newest
/// version first.
pub struct KeyCursor {
    snapshot: Snapshot,
    families: Vec<FamilyId>,
    spec: ReadSpec,
    now: Timestamp,
    keys: KeySet,
    groups: Vec<KeyGroup>,
    group: usize,
    /// The next key to read.
    next: usize,
    /// The key the cursor is on, if any.
    current: Option<usize>,
    reads: Vec<FamilyRead>,
    /// The current row, escaped with its terminator (a row prefix), and unescaped.
    prefix: Vec<u8>,
    /// The first key past the current row (the resolvers' upper bound).
    past: Vec<u8>,
    row_esc_len: usize,
    lane_idx: usize,
    last_lane: Option<usize>,
    qual_buf: Vec<u8>,
    rows_read: u64,
}

impl std::fmt::Debug for KeyCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyCursor")
            .field("keys", &self.keys.len())
            .field("next", &self.next)
            .finish_non_exhaustive()
    }
}

impl KeyCursor {
    pub(crate) fn new(
        snapshot: Snapshot,
        table: TableId,
        families: Vec<FamilyId>,
        spec: ReadSpec,
        now: Timestamp,
        keys: KeySet,
    ) -> Result<Self> {
        let groups = key_groups(&snapshot, table, &keys)?;
        Ok(Self {
            snapshot,
            families,
            spec,
            now,
            keys,
            groups,
            group: 0,
            next: 0,
            current: None,
            reads: Vec::new(),
            prefix: Vec::new(),
            past: Vec::new(),
            row_esc_len: 0,
            lane_idx: 0,
            last_lane: None,
            qual_buf: Vec::new(),
            rows_read: 0,
        })
    }

    /// The key set.
    pub fn keys(&self) -> &KeySet {
        &self.keys
    }

    /// Opens the reads of group `g` (its tablet), one per family with data there.
    fn open_group(&mut self, g: usize) -> Result<()> {
        self.close_reads();
        let (tablet, shard) = (self.groups[g].tablet, self.groups[g].shard);
        let view = Arc::clone(&self.snapshot.view);
        for &family in &self.families {
            let located = view.locate(shard, tablet, family);
            if !located.has_data() {
                continue;
            }
            let Some(meta) = view.catalog.family(family) else {
                continue;
            };
            let (mut opts, filter) = self.spec.resolve_opts(meta, self.snapshot.seqno, self.now);
            let resolver_blobs = ResolverBlobs::attach(&mut opts, &view.ssts);
            let mut mems = Vec::new();
            if let Some(set) = located.mems {
                let mut out = Vec::new();
                mem_sources(set, &filter, &mut out);
                mems.extend(out.into_iter().map(Some));
            }
            let (mut ssts, mut levels) = (Vec::new(), Vec::new());
            let fam = located.ssts.cloned();
            if let Some(f) = &fam {
                for files in &f.levels {
                    let start = ssts.len();
                    ssts.extend(files.iter().map(|sst| SstSlot {
                        sst: Arc::clone(sst),
                        reader: None,
                        source: None,
                    }));
                    levels.push(start..ssts.len());
                }
            }
            self.reads.push(FamilyRead {
                lane: Lane {
                    family,
                    unknown_merge: (meta.merge == MergeKind::Unknown)
                        .then(|| meta.options.merge_operator.clone()),
                    resolver: take_scan_resolver(),
                    grew: false,
                    ssts: Arc::clone(&view.ssts),
                    resolver_blobs,
                    ts: 0,
                    pinned: None,
                    pending: false,
                    done: true,
                },
                filter,
                opts,
                priority: located.priority,
                mems,
                ssts,
                levels,
                fam,
                in_merge: Vec::new(),
            });
        }
        Ok(())
    }

    /// Ends the current group's reads, giving their resolvers back.
    fn close_reads(&mut self) {
        for mut r in self.reads.drain(..) {
            r.lane.resolver.cursor_mut().sources_mut().clear();
            if !r.lane.grew {
                give_back(r.lane.resolver);
            }
        }
    }

    /// Positions on the next key with a cell to read, and returns its index in the key set;
    /// `None` after the last.
    pub fn next_key(&mut self) -> Result<Option<usize>> {
        self.current = None;
        while self.next < self.keys.len() {
            let i = self.next;
            self.next += 1;
            while self.groups[self.group].keys.end <= i {
                self.group += 1;
            }
            if self.groups[self.group].keys.start == i {
                let g = self.group;
                let opened = self.open_group(g);
                self.snapshot.checked(opened)?;
            }
            self.rows_read += 1;
            if self.rows_read.is_multiple_of(FORK_CHECK_ROWS) {
                self.snapshot.check_process()?;
            }
            let read = self.position(i);
            if self.snapshot.checked(read)? {
                self.current = Some(i);
                self.lane_idx = 0;
                self.last_lane = None;
                return Ok(Some(i));
            }
        }
        self.close_reads();
        Ok(None)
    }

    /// Positions every family's read on key `i`: whether any has a cell in its row.
    fn position(&mut self, i: usize) -> Result<bool> {
        let prefix = row_prefix(self.keys.get(i))?;
        self.prefix.clear();
        self.prefix.extend_from_slice(&prefix);
        self.row_esc_len = prefix.len() - TERMINATOR.len();
        let hash = row_hash(&self.prefix[..self.row_esc_len]);
        self.past.clear();
        self.past.extend_from_slice(&self.prefix);
        if let Some(last) = self.past.last_mut() {
            *last += 1;
        }
        let mut any = false;
        for r in &mut self.reads {
            r.position(&self.prefix, hash, &self.past)?;
            if r.lane.pending && in_row(&r.lane, &self.prefix) {
                any = true;
            }
        }
        Ok(any)
    }

    /// The current row key (escaped prefix aside: the key as given).
    pub fn row(&self) -> &[u8] {
        self.current.map_or(&[], |i| self.keys.get(i))
    }

    /// The next cell of the current key's row, or `None` when the row is done.
    pub fn next_cell(&mut self) -> Result<Option<ScanCell<'_>>> {
        let mut qual = std::mem::take(&mut self.qual_buf);
        qual.clear();
        let next = self.next_cell_into(&mut qual);
        self.qual_buf = qual;
        Ok(next?.map(|family| {
            let read = &self.reads[self.last_lane.expect("set by next_cell_into")];
            ScanCell {
                family,
                qualifier: &self.qual_buf,
                ts: read.lane.ts,
                stored: read.lane.value(),
            }
        }))
    }

    /// Like [`KeyCursor::next_cell`], appending the cell's unescaped qualifier to
    /// `qualifiers` and returning its family; [`KeyCursor::current_data`] has its version.
    pub fn next_cell_into(&mut self, qualifiers: &mut Vec<u8>) -> Result<Option<FamilyId>> {
        if self.current.is_none() {
            return Ok(None);
        }
        if let Some(i) = self.last_lane.take() {
            let fetched = self.reads[i].lane.fetch();
            self.snapshot.checked(fetched)?;
        }
        let row_len = self.prefix.len();
        while self.lane_idx < self.reads.len() {
            let i = self.lane_idx;
            let lane = &self.reads[i].lane;
            if in_row(lane, &self.prefix) {
                self.last_lane = Some(i);
                let col = lane.col();
                let end = col.len().saturating_sub(2).max(row_len);
                Escaped::new(&col[row_len..end]).unescape_into(qualifiers);
                return Ok(Some(lane.family));
            }
            self.lane_idx += 1;
        }
        Ok(None)
    }

    /// Appends the current row's next cell to `sink` (its qualifier unescaped into
    /// [`RowSink::qualifiers`]); `false` when the row is done.
    pub fn push_next_cell(&mut self, sink: &mut impl RowSink) -> Result<bool> {
        if self.current.is_none() {
            return Ok(false);
        }
        if let Some(i) = self.last_lane.take() {
            let fetched = self.reads[i].lane.fetch();
            self.snapshot.checked(fetched)?;
        }
        let row_len = self.prefix.len();
        while self.lane_idx < self.reads.len() {
            let i = self.lane_idx;
            let lane = &self.reads[i].lane;
            if in_row(lane, &self.prefix) {
                let col = lane.col();
                let end = col.len().saturating_sub(2).max(row_len);
                let qualifiers = sink.qualifiers();
                let start = qualifiers.len();
                Escaped::new(&col[row_len..end]).unescape_into(qualifiers);
                let qualifier = start..qualifiers.len();
                let bytes = lane.value();
                if bytes.len() <= CellData::INLINE_MAX {
                    sink.push_inline(lane.family, qualifier, lane.ts, bytes);
                } else {
                    let view = Arc::clone(&self.snapshot.view);
                    sink.push(lane.family, qualifier, lane.data(|| view));
                }
                self.last_lane = Some(i);
                return Ok(true);
            }
            self.lane_idx += 1;
        }
        Ok(false)
    }

    /// The cell last returned by [`KeyCursor::next_cell`], as a [`CellData`].
    ///
    /// # Panics
    /// If no cell has been returned for the current key.
    pub fn current_data(&self) -> CellData {
        let i = self.last_lane.expect("current_data before next_cell");
        let view = Arc::clone(&self.snapshot.view);
        self.reads[i].lane.data(|| view)
    }

    /// Drops the current key's remaining cells without reading them: the next
    /// [`KeyCursor::next_key`] moves every source on from where it is (D296's `NextRow`).
    pub fn skip_rest_of_key(&mut self) {
        for r in &mut self.reads {
            r.lane.pending = false;
        }
        self.last_lane = None;
        self.lane_idx = self.reads.len();
    }
}

impl Drop for KeyCursor {
    fn drop(&mut self) {
        self.close_reads();
    }
}

/// Whether the lane's held cell is in the row whose prefix (escaped row and terminator) is
/// `prefix`.
fn in_row(lane: &Lane, prefix: &[u8]) -> bool {
    lane.pending && lane.col().starts_with(prefix)
}

impl FamilyRead {
    /// Returns the sources of the last key's merge to their slots, then gathers and
    /// positions this key's (the row filter prunes SSTs, as a point row read does), and
    /// reads its first cell.
    fn position(&mut self, prefix: &[u8], hash: u64, past: &[u8]) -> Result<()> {
        let FamilyRead {
            lane,
            filter,
            opts,
            priority,
            mems,
            ssts,
            levels,
            fam,
            in_merge,
        } = self;
        let sources = lane.resolver.cursor_mut().sources_mut();
        for (src, slot) in sources.drain(..).zip(in_merge.drain(..)) {
            match slot {
                Slot::Mem(i) => mems[i] = Some(src),
                Slot::Sst(i) => ssts[i].source = Some(src),
            }
        }
        for (i, m) in mems.iter_mut().enumerate() {
            let mut src = m.take().expect("a memtable source is in its slot");
            move_to(&mut src, prefix, false)?;
            sources.push(src);
            in_merge.push(Slot::Mem(i));
        }
        if let Some(f) = fam.as_deref() {
            // A point read's options without `one_row`: a cursor that stopped at its row's
            // end could not move forward to the next key, and would seek from the top.
            let mut read_opts = read_options(*priority, false, false, false);
            read_opts.one_row = false;
            for (l, range) in levels.iter().enumerate() {
                let (from, search) = f.level_start(l, prefix);
                for (i, slot) in ssts
                    .iter_mut()
                    .enumerate()
                    .take(range.end)
                    .skip(range.start + from)
                {
                    if search && slot.sst.first_row() > prefix {
                        break;
                    }
                    if !(slot.sst.first_row() <= prefix && prefix <= slot.sst.last_row()) {
                        continue;
                    }
                    let reader = match &slot.reader {
                        Some(r) => Arc::clone(r),
                        None => {
                            let r = slot.sst.reader(&lane.ssts, *priority)?;
                            slot.reader = Some(Arc::clone(&r));
                            r
                        }
                    };
                    if !reader.may_contain_row(hash) {
                        continue;
                    }
                    let fresh = slot.source.is_none();
                    let mut src = slot.source.take().unwrap_or_else(|| {
                        Source::Sst(reader.iter(filter.clone(), read_opts).into())
                    });
                    move_to(&mut src, prefix, fresh)?;
                    sources.push(src);
                    in_merge.push(Slot::Sst(i));
                }
            }
        }
        lane.pending = false;
        lane.pinned = None;
        if sources.is_empty() {
            lane.done = true;
            return Ok(());
        }
        lane.done = false;
        let mut o = opts.clone();
        o.skip_columns = skips_columns(sources);
        let resolver = &mut lane.resolver;
        resolver.cursor_mut().reset();
        resolver.reset(o);
        resolver.set_upper_bound(Some(past));
        resolver.seek_with(|cursor| cursor.rebuild_positioned())?;
        lane.fetch()
    }
}

/// Moves `src` to the first entry at or past `target` (a row prefix): a full seek if it is
/// `fresh` or has run past its end, a forward one from where it is if it is behind, nothing
/// if it is already there (keys come in increasing order).
fn move_to(src: &mut Source, target: &[u8], fresh: bool) -> Result<()> {
    if fresh || !src.valid() {
        src.seek(target)
    } else if src.key() < target {
        src.seek_forward(target)
    } else {
        Ok(())
    }
}
