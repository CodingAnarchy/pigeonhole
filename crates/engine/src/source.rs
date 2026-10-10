//! The engine's sorted sources: memtables (behind `pigeonhole-compaction`'s `FilteredCursor`
//! so they filter exactly as `SstIter` does, decision D22) and SSTs, as one enum so the
//! per-entry path has no virtual calls. Merging and MVCC resolution are
//! `pigeonhole-compaction`'s `MergingCursor` and `CellResolver`: reads and compaction share
//! one implementation of the delete, TTL, version and merge rules.

use std::cell::Cell as StdCell;
use std::ops::Deref;
use std::sync::Arc;

use pigeonhole_cache::{Cell, Priority};
use pigeonhole_compaction::{CellResolver, FilteredCursor, MergingCursor};
use pigeonhole_format::Cursor;
use pigeonhole_format::filter::{column_hash, row_hash};
use pigeonhole_format::key::{
    MARKER_QUALIFIER, MAX_KEY_PART, TERMINATOR, escape_into, row_prefix_len,
};
use pigeonhole_format::scan::ScanFilter;
use pigeonhole_format::{FamilyId, TabletId};
use pigeonhole_memtable::{ArenaSlice, MemIter, MemtableReader};
use pigeonhole_runtime::ShardId;
use pigeonhole_sst::{ReadOptions, SstIter};
use smallvec::SmallVec;

use crate::snapshot::{FamilySsts, MemSet, OpenSst, SstSet, View};
use crate::{Error, Result};

/// A sorted source of one family's entries. The variants differ in size (an SST cursor
/// holds three block cursors); they stay unboxed so the per-entry path has no indirection.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Source {
    /// A memtable (active or frozen), filtered like an SST.
    Mem(FilteredCursor<MemIter>),
    /// An SST, or the SSTs of one level below 0 (the filter runs inside the block decoder).
    Sst(SstSource),
    /// An in-memory source for the resolver tests.
    #[cfg(test)]
    Vec(pigeonhole_compaction::VecCursor),
}

impl Cursor for Source {
    type Error = Error;

    #[inline]
    fn valid(&self) -> bool {
        match self {
            Source::Mem(it) => it.valid(),
            Source::Sst(it) => it.valid(),
            #[cfg(test)]
            Source::Vec(it) => it.valid(),
        }
    }

    #[inline]
    fn key(&self) -> &[u8] {
        match self {
            Source::Mem(it) => it.key(),
            Source::Sst(it) => it.key(),
            #[cfg(test)]
            Source::Vec(it) => it.key(),
        }
    }

    #[inline]
    fn value(&self) -> &[u8] {
        match self {
            Source::Mem(it) => it.value(),
            Source::Sst(it) => it.value(),
            #[cfg(test)]
            Source::Vec(it) => it.value(),
        }
    }

    fn seek_to_first(&mut self) -> Result<()> {
        match self {
            Source::Mem(it) => Ok(it.seek_to_first()?),
            Source::Sst(it) => it.seek_to_first(),
            #[cfg(test)]
            Source::Vec(it) => Ok(it.seek_to_first()?),
        }
    }

    fn seek(&mut self, target: &[u8]) -> Result<()> {
        match self {
            Source::Mem(it) => Ok(it.seek(target)?),
            Source::Sst(it) => it.seek(target),
            #[cfg(test)]
            Source::Vec(it) => Ok(it.seek(target)?),
        }
    }

    fn seek_forward(&mut self, target: &[u8]) -> Result<()> {
        match self {
            // A finger search from the memtable cursor's last seek.
            Source::Mem(it) => Ok(it.seek_forward(target)?),
            Source::Sst(it) => it.seek(target),
            #[cfg(test)]
            Source::Vec(it) => Ok(it.seek_forward(target)?),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self {
            Source::Mem(it) => Ok(it.next()?),
            Source::Sst(it) => it.next(),
            #[cfg(test)]
            Source::Vec(it) => Ok(it.next()?),
        }
    }

    fn skip_row(&mut self) -> Result<()> {
        match self {
            Source::Mem(it) => Ok(it.skip_row()?),
            Source::Sst(it) => it.skip_row(),
            #[cfg(test)]
            Source::Vec(it) => Ok(it.skip_row()?),
        }
    }

    /// A memtable with a stale-tail index jumps (D194); an SST steps as before.
    #[inline]
    fn skip_column(&mut self, column: &[u8]) -> Result<bool> {
        match self {
            Source::Mem(it) => {
                let skipped = it.skip_column(column)?;
                #[cfg(feature = "test-hooks")]
                if skipped {
                    crate::shard::TAIL_SKIPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Ok(skipped)
            }
            Source::Sst(_) => Ok(false),
            #[cfg(test)]
            Source::Vec(it) => Ok(it.skip_column(column)?),
        }
    }
}

/// A value pinned in its storage, outliving the cursor position it came from.
#[derive(Debug, Clone)]
pub(crate) enum Pinned {
    /// A range of a memtable arena (the caller also holds the view that keeps it alive).
    Arena(ArenaSlice),
    /// A range of a cached block.
    Block(Cell),
    /// An owned copy (the resolver tests' in-memory sources).
    #[cfg(test)]
    Owned(Vec<u8>),
}

impl Deref for Pinned {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            Pinned::Arena(s) => s,
            Pinned::Block(c) => c,
            #[cfg(test)]
            Pinned::Owned(v) => v,
        }
    }
}

impl Source {
    /// Switches an SST source's cache-only reads on or off (async scans, ICR 0014).
    pub(crate) fn set_cache_only(&mut self, on: bool) {
        if let Source::Sst(s) = self {
            s.set_cache_only(on);
        }
    }

    /// The uncached blocks an SST source's next steps read, up to `max`, appended to `out`
    /// ([`SstSource::upcoming`]; nothing for other sources).
    pub(crate) fn upcoming(&self, max: usize, out: &mut Vec<pigeonhole_sst::Fetch>) {
        if let Source::Sst(s) = self {
            s.upcoming(max, out);
        }
    }

    /// The current value, pinned without a copy where the source allows it.
    ///
    /// # Panics
    /// If the source is not valid.
    pub(crate) fn pin_value(&self) -> Pinned {
        match self {
            Source::Mem(it) => Pinned::Arena(it.inner().value_slice()),
            Source::Sst(it) => Pinned::Block(it.iter.value_cell()),
            #[cfg(test)]
            Source::Vec(it) => Pinned::Owned(it.value().to_vec()),
        }
    }
}

/// The merged sources of one family within one tablet.
pub(crate) type Merged = MergingCursor<Source>;
/// The MVCC resolver over them.
pub(crate) type Resolver = CellResolver<Merged>;

/// Memtable sources of `set`, newest first.
pub(crate) fn mem_sources(set: &MemSet, filter: &ScanFilter, out: &mut Vec<Source>) {
    mem_sources_from(&set.readers, filter, out);
}

/// Memtable sources over `readers`, in their order.
pub(crate) fn mem_sources_from(
    readers: &[MemtableReader],
    filter: &ScanFilter,
    out: &mut Vec<Source>,
) {
    for r in readers {
        out.push(Source::Mem(FilteredCursor::new(r.iter(), filter.clone())));
    }
}

/// Whether an SST holds any row of `[start, end)` (row prefixes; `None` is unbounded).
fn overlaps_range(sst: &OpenSst, start: Option<&[u8]>, end: Option<&[u8]>) -> bool {
    start.is_none_or(|s| sst.last_row() >= s) && end.is_none_or(|e| sst.first_row() < e)
}

/// Whether an SST's range covers `row` (a row prefix).
fn covers_row(sst: &OpenSst, row: &[u8]) -> bool {
    sst.first_row() <= row && row <= sst.last_row()
}

/// Data blocks a scan fetches ahead (`ReadOptions::readahead_blocks`), and whether adjacent
/// ones go as one read: 4 and no, unless `PIGEONHOLE_READAHEAD=N[,merge]` (a bench and test
/// variable, read once per process) says otherwise, until #431 decides both from the gate
/// measurement (#405).
fn scan_readahead() -> (u32, bool) {
    static READAHEAD: std::sync::OnceLock<(u32, bool)> = std::sync::OnceLock::new();
    *READAHEAD.get_or_init(|| {
        let Ok(v) = std::env::var("PIGEONHOLE_READAHEAD") else {
            return (4, false);
        };
        let mut parts = v.split(',');
        let blocks = parts
            .next()
            .and_then(|n| n.trim().parse().ok())
            .unwrap_or(4);
        (blocks, parts.any(|p| p.trim() == "merge"))
    })
}

/// Read options for a scan or a point read.
fn read_options(priority: Priority, scan: bool, cache_only: bool) -> ReadOptions {
    let mut o = ReadOptions::default();
    o.priority = priority;
    if scan {
        (o.readahead_blocks, o.readahead_merge) = scan_readahead();
    }
    o.cache_only = cache_only;
    o
}

/// SST sources for a scan of `[start, end)` (row prefixes), newest first: one per SST of
/// level 0 (they overlap), and one per deeper level (its SSTs are sorted and disjoint, so a
/// [`LevelIter`] opens and seeks only the ones the scan reaches).
pub(crate) fn sst_sources_range<const CACHE_ONLY: bool>(
    fam: &FamilySsts,
    set: &Arc<SstSet>,
    filter: &ScanFilter,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    priority: Priority,
    out: &mut Vec<Source>,
) -> Result<()> {
    let opts = read_options(priority, true, CACHE_ONLY);
    let reader = |sst: &OpenSst| {
        if CACHE_ONLY {
            sst.reader_cache_only(set, priority)
        } else {
            sst.reader(set, priority)
        }
    };
    for (level, files) in fam.levels.iter().enumerate() {
        let mut picked = files.iter().filter(|sst| overlaps_range(sst, start, end));
        if level == 0 {
            for sst in picked {
                let reader = reader(sst)?;
                out.push(Source::Sst(reader.iter(filter.clone(), opts).into()));
            }
            continue;
        }
        let Some(first) = picked.next() else {
            continue;
        };
        let rest: Vec<_> = picked.cloned().collect();
        if rest.is_empty() {
            let reader = reader(first)?;
            out.push(Source::Sst(reader.iter(filter.clone(), opts).into()));
            continue;
        }
        let mut level_files = Vec::with_capacity(rest.len() + 1);
        level_files.push(Arc::clone(first));
        level_files.extend(rest);
        out.push(Source::Sst(SstSource::level(
            level_files,
            Arc::clone(set),
            filter.clone(),
            opts,
        )?));
    }
    Ok(())
}

/// A point read's column prefix (escaped row, terminator, escaped qualifier, terminator),
/// encoded once per read for both its filter probes and its resolver (#46). Held in the
/// reading thread's scratch, so a point read allocates nothing for it.
pub(crate) struct ColumnKey {
    buf: Vec<u8>,
    /// Length of the row prefix (escaped row and terminator) at the start of `buf`.
    row_len: usize,
    /// Length of the column prefix; `buf` may hold scratch past it.
    len: usize,
    /// The row or qualifier is longer than a key part may be: no SST can hold it.
    oversized: bool,
}

thread_local! {
    /// Scratch for a point read's [`ColumnKey`], kept by each reading thread (#46). Taken
    /// while in use, so a nested read on the same thread (none today) gets a fresh one.
    static KEY_SCRATCH: StdCell<Vec<u8>> = const { StdCell::new(Vec::new()) };
}

impl ColumnKey {
    pub(crate) fn new(row: &[u8], qualifier: &[u8]) -> Self {
        let mut buf = KEY_SCRATCH.try_with(StdCell::take).unwrap_or_default();
        buf.clear();
        escape_into(&mut buf, row);
        buf.extend_from_slice(&TERMINATOR);
        let row_len = buf.len();
        escape_into(&mut buf, qualifier);
        buf.extend_from_slice(&TERMINATOR);
        Self {
            len: buf.len(),
            buf,
            row_len,
            oversized: row.len() > MAX_KEY_PART || qualifier.len() > MAX_KEY_PART,
        }
    }

    /// The column prefix.
    pub(crate) fn prefix(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    /// Length of the row prefix at the start of [`ColumnKey::prefix`].
    pub(crate) fn row_len(&self) -> usize {
        self.row_len
    }
}

impl Drop for ColumnKey {
    fn drop(&mut self) {
        let buf = std::mem::take(&mut self.buf);
        // A key long enough to grow the scratch past 64 KiB is not worth keeping.
        if buf.capacity() <= 64 << 10 {
            let _ = KEY_SCRATCH.try_with(|s| s.set(buf));
        }
    }
}

/// A row read's row prefix (escaped row and terminator, as
/// [`encode_row_prefix`](pigeonhole_format::key::encode_row_prefix) writes it), inline for
/// rows of usual length so a row read allocates nothing for it (#287).
pub(crate) fn row_prefix(row: &[u8]) -> Result<SmallVec<[u8; 96]>> {
    if row.len() > MAX_KEY_PART {
        return Err(pigeonhole_format::Error::KeyTooLarge.into());
    }
    let mut out = SmallVec::new();
    let mut rest = row;
    while let Some(i) = rest.iter().position(|&b| b == 0) {
        out.extend_from_slice(&rest[..=i]);
        out.push(0xFF);
        rest = &rest[i + 1..];
    }
    out.extend_from_slice(rest);
    out.extend_from_slice(&TERMINATOR);
    Ok(out)
}

/// An SST's cursor, or, for a level below 0 (its SSTs sorted by key and disjoint) with more
/// than one SST in range, the cursor of the level: it opens and seeks an SST only when it
/// reaches it (as RocksDB's `LevelIterator` does), so a scan bounded by a limit rather than
/// an end key touches the one or two SSTs it reads, not every SST to the right of its start.
/// The open SST's cursor serves every entry directly; the level only acts when it runs out
/// or seeks.
///
/// A level opens an SST's reader when it reaches it, so a failure to open one (I/O,
/// corruption) surfaces from a scan's `next` or seek, not from creating the scan.
#[derive(Debug)]
pub(crate) struct SstSource {
    /// The open SST's cursor.
    iter: SstIter,
    /// The level, when there is more than one SST in it.
    level: Option<Box<Level>>,
}

/// The SSTs of a level an [`SstSource`] walks.
#[derive(Debug)]
struct Level {
    files: Vec<Arc<OpenSst>>,
    set: Arc<SstSet>,
    filter: ScanFilter,
    opts: ReadOptions,
    /// Index in `files` of the open SST.
    at: usize,
    /// A seek has positioned the cursor: before one, the open SST's cursor is invalid
    /// without having run out, and `next` must not move on to the next SST.
    positioned: bool,
    /// Scratch for `skip_row`'s seek past a row.
    past_row: Vec<u8>,
}

impl From<SstIter> for SstSource {
    fn from(iter: SstIter) -> Self {
        Self { iter, level: None }
    }
}

impl Level {
    /// The cursor of the SST at `i`, unpositioned; `at` moves only if it opened.
    fn open(&mut self, i: usize) -> Result<SstIter> {
        let reader = if self.opts.cache_only {
            self.files[i].reader_cache_only(&self.set, self.opts.priority)?
        } else {
            self.files[i].reader(&self.set, self.opts.priority)?
        };
        self.at = i;
        Ok(reader.iter(self.filter.clone(), self.opts))
    }
}

impl SstSource {
    /// Switches cache-only reads on or off for the open SST and the ones the level opens
    /// next (an async scan positions cache-only, then steps reading normally; ICR 0014).
    pub(crate) fn set_cache_only(&mut self, on: bool) {
        self.iter.set_cache_only(on);
        if let Some(level) = &mut self.level {
            level.opts.cache_only = on;
        }
    }

    /// The uncached blocks the next steps past the current one read, up to `max` of the open
    /// SST's ([`SstIter::upcoming`]), appended to `out`; on the open SST's last block, what
    /// opening the level's next SST and reading its first block need (ICR 0017).
    pub(crate) fn upcoming(&self, max: usize, out: &mut Vec<pigeonhole_sst::Fetch>) {
        let before = out.len();
        self.iter.upcoming(max, out);
        if out.len() > before {
            return;
        }
        let Some(level) = self.level.as_ref().filter(|l| l.positioned) else {
            return;
        };
        if !self.iter.at_last_block() {
            return;
        }
        let Some(next) = level.files.get(level.at + 1) else {
            return;
        };
        match next.reader_cache_only(&level.set, level.opts.priority) {
            Ok(reader) => out.extend(reader.first_fetch(level.opts.priority)),
            Err(Error::WouldBlock(fetch)) => out.push(*fetch),
            Err(_) => {}
        }
    }

    /// The cursor of a level's `files` (in key order, at least one), with the first opened.
    fn level(
        files: Vec<Arc<OpenSst>>,
        set: Arc<SstSet>,
        filter: ScanFilter,
        opts: ReadOptions,
    ) -> Result<Self> {
        // The level cursor is correct only over disjoint SSTs in key order, which every level
        // below 0 keeps (and `tablets.rs` checks when merging tablets).
        debug_assert!(
            files
                .windows(2)
                .all(|w| w[0].meta.largest_key < w[1].meta.smallest_key),
            "the SSTs of a level below 0 overlap or are out of order"
        );
        let mut level = Box::new(Level {
            files,
            set,
            filter,
            opts,
            at: 0,
            positioned: false,
            past_row: Vec::new(),
        });
        let iter = level.open(0)?;
        Ok(Self {
            iter,
            level: Some(level),
        })
    }

    /// While the open SST is exhausted and the level has more, moves to the first entry of
    /// the next one.
    #[cold]
    fn next_file(&mut self) -> Result<()> {
        let Some(level) = self.level.as_mut().filter(|l| l.positioned) else {
            return Ok(());
        };
        while !self.iter.valid() && level.at + 1 < level.files.len() {
            self.iter = level.open(level.at + 1)?;
            self.iter.seek_to_first()?;
        }
        Ok(())
    }

    #[inline]
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    #[inline]
    fn key(&self) -> &[u8] {
        self.iter.key()
    }

    #[inline]
    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn seek_to_first(&mut self) -> Result<()> {
        if let Some(level) = &mut self.level {
            level.positioned = true;
            if level.at != 0 {
                self.iter = level.open(0)?;
            }
        }
        self.iter.seek_to_first()?;
        self.next_file()
    }

    /// Also the forward seek: an SST cursor's forward seek is a seek. The level's seek reuses
    /// the open SST when the target is still in it.
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        if let Some(level) = &mut self.level {
            level.positioned = true;
            // The first SST whose largest key is at or past the target holds the first entry
            // `>= target`, or (if the filter hides the rest of it) precedes the SST that does;
            // past every SST, the last one's seek runs out.
            let i = level
                .files
                .partition_point(|f| f.meta.largest_key.as_slice() < target)
                .min(level.files.len() - 1);
            if i != level.at {
                self.iter = level.open(i)?;
            }
        }
        self.iter.seek(target)?;
        if self.level.is_some() && !self.iter.valid() {
            self.next_file()?;
        }
        Ok(())
    }

    #[inline]
    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        if self.level.is_some() && !self.iter.valid() {
            self.next_file()?;
        }
        Ok(())
    }

    fn skip_row(&mut self) -> Result<()> {
        let Some(level) = self.level.as_mut().filter(|_| self.iter.valid()) else {
            return Ok(self.iter.skip_row()?);
        };
        let Ok(n) = row_prefix_len(self.iter.key()) else {
            return self.next();
        };
        let row = &self.iter.key()[..n];
        if !level.files[level.at].meta.largest_key.starts_with(row) {
            // The row ends within this SST, so the next SST starts with a later row.
            self.iter.skip_row()?;
            return self.next_file();
        }
        // The row may continue into the next SST: seek past it. `escaped row ++ 00 02` sorts
        // after every key of the row and before every later row (as `SstIter::skip_row`).
        let mut past = std::mem::take(&mut level.past_row);
        past.clear();
        past.extend_from_slice(row);
        if let Some(last) = past.last_mut() {
            *last = 0x02;
        }
        let r = self.seek(&past);
        if let Some(level) = &mut self.level {
            level.past_row = past;
        }
        r
    }
}

/// The filter probes of a point read: hashes of the row, the column and the row's marker
/// key (FORMAT §6), computed once per read from its [`ColumnKey`].
pub(crate) struct Probe<'a> {
    /// The row prefix (escaped row and terminator).
    pub row_prefix: &'a [u8],
    row: u64,
    column: u64,
    marker: u64,
}

impl<'a> Probe<'a> {
    pub(crate) fn new(key: &'a mut ColumnKey) -> Result<Self> {
        if key.oversized {
            return Err(pigeonhole_format::Error::KeyTooLarge.into());
        }
        let (row_len, len) = (key.row_len, key.len);
        let row = row_hash(&key.buf[..row_len - TERMINATOR.len()]);
        let column = column_hash(&key.buf[..len]);
        // The marker prefix (row prefix and marker qualifier), built past the column prefix.
        key.buf.truncate(len);
        key.buf.extend_from_within(..row_len);
        key.buf.extend_from_slice(&MARKER_QUALIFIER);
        let marker = column_hash(&key.buf[len..]);
        let key: &'a ColumnKey = key;
        Ok(Self {
            row_prefix: &key.buf[..row_len],
            row,
            column,
            marker,
        })
    }
}

/// SST sources for a point read of one column: every SST of a level whose range covers the
/// row (decision D78) and whose filters admit the row and either the column or the row's
/// family markers (decision D9), newest first.
pub(crate) fn sst_sources_point<const CACHE_ONLY: bool>(
    fam: &FamilySsts,
    set: &SstSet,
    probe: &Probe<'_>,
    priority: Priority,
    out: &mut Vec<Source>,
) -> Result<()> {
    let opts = read_options(priority, false, CACHE_ONLY);
    for sst in fam.iter() {
        if !covers_row(sst, probe.row_prefix) {
            continue;
        }
        let reader = if CACHE_ONLY {
            sst.reader_cache_only(set, priority)?
        } else {
            sst.reader(set, priority)?
        };
        if !reader.may_contain_row(probe.row)
            || !(reader.may_contain_column(probe.column) || reader.may_contain_column(probe.marker))
        {
            continue;
        }
        out.push(Source::Sst(reader.iter(ScanFilter::all(), opts).into()));
    }
    Ok(())
}

/// SST sources for reading one whole row (the row filter applies), newest first.
#[allow(clippy::too_many_arguments)]
pub(crate) fn sst_sources_row<const CACHE_ONLY: bool>(
    fam: &FamilySsts,
    set: &SstSet,
    filter: &ScanFilter,
    escaped_row: &[u8],
    row_prefix: &[u8],
    priority: Priority,
    out: &mut Vec<Source>,
) -> Result<()> {
    let opts = read_options(priority, false, CACHE_ONLY);
    let hash = row_hash(escaped_row);
    for sst in fam.iter() {
        if !covers_row(sst, row_prefix) {
            continue;
        }
        let reader = if CACHE_ONLY {
            sst.reader_cache_only(set, priority)?
        } else {
            sst.reader(set, priority)?
        };
        if !reader.may_contain_row(hash) {
            continue;
        }
        out.push(Source::Sst(reader.iter(filter.clone(), opts).into()));
    }
    Ok(())
}

/// What a read needs to know about where a family's data lives in a view.
pub(crate) struct Located<'a> {
    pub mems: Option<&'a Arc<MemSet>>,
    pub ssts: Option<&'a Arc<FamilySsts>>,
    pub priority: Priority,
}

impl View {
    /// Where `(tablet, family)` lives in this view.
    pub(crate) fn locate(&self, shard: ShardId, tablet: TabletId, family: FamilyId) -> Located<'_> {
        let priority = self.catalog.family(family).map_or(Priority::Normal, |m| {
            SstSet::priority(m.options.cache_priority)
        });
        Located {
            mems: self.memtables(shard, tablet, family),
            ssts: self.ssts.family(tablet, family),
            priority,
        }
    }

    /// Sources for a scan of `[start, end)` (row prefixes), newest first.
    pub(crate) fn scan_sources(
        &self,
        shard: ShardId,
        tablet: TabletId,
        family: FamilyId,
        filter: &ScanFilter,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<Source>> {
        let mut out = Vec::new();
        self.scan_sources_into::<false>(shard, tablet, family, filter, start, end, &mut out)?;
        Ok(out)
    }

    /// [`View::scan_sources`] appended to `out` (a reused source list).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn scan_sources_into<const CACHE_ONLY: bool>(
        &self,
        shard: ShardId,
        tablet: TabletId,
        family: FamilyId,
        filter: &ScanFilter,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        out: &mut Vec<Source>,
    ) -> Result<()> {
        let l = self.locate(shard, tablet, family);
        if let Some(set) = l.mems {
            mem_sources(set, filter, out);
        }
        if let Some(fam) = l.ssts {
            sst_sources_range::<CACHE_ONLY>(fam, &self.ssts, filter, start, end, l.priority, out)?;
        }
        Ok(())
    }

    /// Sources for a point read of one column, newest first. Returns whether one of them may
    /// hold a family or row delete marker (ICR 0020): an SST, or a memtable whose writer
    /// noted one. Taken after the read point, so a marker visible to the read is never
    /// missed.
    pub(crate) fn point_sources<const CACHE_ONLY: bool>(
        &self,
        shard: ShardId,
        tablet: TabletId,
        family: FamilyId,
        key: &mut ColumnKey,
        out: &mut Vec<Source>,
    ) -> Result<bool> {
        let l = self.locate(shard, tablet, family);
        // Room for the memtables and a couple of SSTs: a source is about 1 KiB, and a `Vec`
        // grown from empty would hold four (#46). `out` is normally a reused buffer that
        // already has it.
        let mems = l.mems.map_or(0, |set| set.readers.len());
        let ssts = l.ssts.map_or(0, |fam| fam.iter().take(2).count());
        out.reserve(mems + ssts);
        let all = ScanFilter::all();
        let mut markers = false;
        if let Some(set) = l.mems {
            markers = set.readers.iter().any(MemtableReader::may_have_markers);
            mem_sources(set, &all, out);
        }
        if let Some(fam) = l.ssts
            && !fam.is_empty()
        {
            let probe = Probe::new(key)?;
            let before = out.len();
            sst_sources_point::<CACHE_ONLY>(fam, &self.ssts, &probe, l.priority, out)?;
            markers |= out.len() > before;
        }
        Ok(markers)
    }

    /// Sources for reading one row, newest first, appended to `out` (a reused source list).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn row_sources_into<const CACHE_ONLY: bool>(
        &self,
        shard: ShardId,
        tablet: TabletId,
        family: FamilyId,
        filter: &ScanFilter,
        row_prefix: &[u8],
        out: &mut Vec<Source>,
    ) -> Result<()> {
        let l = self.locate(shard, tablet, family);
        if let Some(set) = l.mems {
            mem_sources(set, filter, out);
        }
        if let Some(fam) = l.ssts
            && !fam.is_empty()
        {
            // The escaped row: the row prefix without its terminator.
            let escaped = &row_prefix[..row_prefix.len() - TERMINATOR.len()];
            sst_sources_row::<CACHE_ONLY>(
                fam, &self.ssts, filter, escaped, row_prefix, l.priority, out,
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod row_prefix_tests {
    use pigeonhole_format::key::{MAX_KEY_PART, encode_row_prefix};

    use super::row_prefix;

    #[test]
    fn a_row_prefix_is_the_formats_encoding() {
        let long = vec![7u8; 300];
        let rows: [&[u8]; 6] = [
            b"",
            b"r",
            b"\x00",
            b"a\x00b\x00\x00",
            b"\xff\x00\xff",
            &long,
        ];
        for row in rows {
            let mut want = Vec::new();
            encode_row_prefix(&mut want, row).unwrap();
            assert_eq!(&row_prefix(row).unwrap()[..], &want[..], "row {row:?}");
        }
        assert!(row_prefix(&vec![1u8; MAX_KEY_PART + 1]).is_err());
        assert!(row_prefix(&vec![1u8; MAX_KEY_PART]).is_ok());
    }
}

#[cfg(test)]
mod level_tests {
    //! The lazy level cursor against the eager merge of its SSTs, over random disjoint
    //! SSTs (rows may span two) and random moves.

    use std::sync::Arc;

    use pigeonhole_cache::{BlockCache, Priority};
    use pigeonhole_compaction::MergingCursor;
    use pigeonhole_format::Cursor;
    use pigeonhole_format::key::{Kind, encode_key, encode_row_prefix};
    use pigeonhole_format::manifest::FamilyOptions;
    use pigeonhole_format::scan::ScanFilter;
    use pigeonhole_format::superblock::ExtentRef;
    use pigeonhole_format::{FamilyId, SstId, TableId, TabletId};
    use pigeonhole_io::sim::SimVfs;
    use pigeonhole_io::{OpenOptions, Vfs};
    use pigeonhole_sst::{SstReader, SstWriter, SstWriterOptions};

    use super::{Source, SstSource, read_options, sst_sources_range};
    use crate::snapshot::{FamilySsts, OpenSst, SstSet};

    /// xorshift64*, seeded per case.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    type Entries = Vec<(Vec<u8>, Vec<u8>)>;

    fn entries(rng: &mut Rng) -> Entries {
        let n = 1 + rng.below(160);
        let mut out = Vec::new();
        for seqno in 1..=n {
            // Rows with a zero byte exercise the escaping of the row prefix.
            let row = [b'r', rng.below(3) as u8, b'0' + rng.below(6) as u8];
            let qualifier = [b'q', rng.below(5) as u8];
            let kind = if rng.below(5) == 0 {
                Kind::CellDelete
            } else {
                Kind::Put
            };
            let mut key = Vec::new();
            encode_key(&mut key, &row, &qualifier, 1 + rng.below(5), seqno, kind).unwrap();
            let value = if kind == Kind::Put {
                vec![0, seqno as u8, row[2]]
            } else {
                Vec::new()
            };
            out.push((key, value));
        }
        out.sort();
        out.dedup_by(|a, b| a.0 == b.0);
        out
    }

    /// Writes `chunks` as SSTs of one file, each opened.
    fn ssts(chunks: &[&[(Vec<u8>, Vec<u8>)]]) -> (Arc<SstSet>, Vec<Arc<OpenSst>>) {
        let vfs = SimVfs::new(1);
        let file = vfs
            .open("/db".as_ref(), OpenOptions::read_write_create())
            .unwrap();
        let cache = Arc::new(BlockCache::new(1 << 20, 0));
        // Small blocks: an SST of a few entries still has several.
        let family = FamilyOptions::default().block_size(128);
        let files = chunks
            .iter()
            .enumerate()
            .map(|(i, chunk)| {
                let options =
                    SstWriterOptions::for_family(&family, TableId(1), FamilyId(1), TabletId(1));
                let extent = ExtentRef {
                    page: 1024 * (i as u64 + 1),
                    size_class: 6,
                };
                let mut w = SstWriter::new(file.clone(), extent, SstId(i as u64 + 1), options);
                for (k, v) in *chunk {
                    w.add(k, v).unwrap();
                }
                let meta = w.finish().unwrap();
                let reader =
                    SstReader::open(file.clone(), &meta, Arc::clone(&cache), Priority::Normal)
                        .unwrap();
                Arc::new(OpenSst::new(Arc::new(meta), Some(Arc::new(reader))))
            })
            .collect();
        (Arc::new(SstSet::empty(file, cache)), files)
    }

    /// A seek target: an entry's key, a row prefix, or a key just past an entry.
    fn target(rng: &mut Rng, all: &Entries) -> Vec<u8> {
        let (k, _) = &all[rng.below(all.len() as u64) as usize];
        match rng.below(3) {
            0 => k.clone(),
            1 => {
                let mut p = Vec::new();
                let row = [b'r', rng.below(3) as u8, b'0' + rng.below(7) as u8];
                encode_row_prefix(&mut p, &row).unwrap();
                p
            }
            _ => {
                let mut p = k.clone();
                p.push(0);
                p
            }
        }
    }

    fn same(lazy: &Source, eager: &MergingCursor<Source>, case: &str) {
        assert_eq!(lazy.valid(), eager.valid(), "{case}");
        if lazy.valid() {
            assert_eq!(lazy.key(), eager.key(), "{case}");
            assert_eq!(lazy.value(), eager.value(), "{case}");
            assert_eq!(&*lazy.pin_value(), eager.value(), "{case}");
        }
    }

    #[test]
    fn a_level_cursor_moves_as_the_merge_of_its_ssts() {
        let base = std::env::var("PH_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0x1e7e1_u64);
        let cases = if cfg!(miri) { 4 } else { 400 };
        for case in 0..cases {
            let seed = base.wrapping_add(case);
            let mut rng = Rng(seed | 1);
            let all = entries(&mut rng);
            // Cut into 2..=6 non-empty SSTs, anywhere (a row may span a cut).
            let parts = (2 + rng.below(5) as usize).min(all.len());
            if parts < 2 {
                continue;
            }
            let mut cuts: Vec<usize> = (0..parts - 1)
                .map(|_| 1 + rng.below(all.len() as u64 - 1) as usize)
                .collect();
            cuts.sort_unstable();
            cuts.dedup();
            let mut chunks = Vec::new();
            let mut from = 0;
            for &c in cuts.iter().chain([all.len()].iter()) {
                chunks.push(&all[from..c]);
                from = c;
            }
            let (set, files) = ssts(&chunks);
            let filter = if rng.below(2) == 0 {
                ScanFilter::all()
            } else {
                let mut f = ScanFilter::all();
                f.time_range = Some((2, 4));
                f
            };
            let opts = read_options(Priority::Normal, true, false);
            let mut lazy = Source::Sst(
                SstSource::level(files.clone(), Arc::clone(&set), filter.clone(), opts).unwrap(),
            );
            let mut eager = MergingCursor::new(
                files
                    .iter()
                    .map(|f| {
                        let r = f.reader(&set, Priority::Normal).unwrap();
                        Source::Sst(r.iter(filter.clone(), opts).into())
                    })
                    .collect(),
            );
            let case = format!("seed {seed}");
            for _ in 0..40 {
                match rng.below(6) {
                    0 => {
                        lazy.seek_to_first().unwrap();
                        eager.seek_to_first().unwrap();
                    }
                    1 | 2 => {
                        let t = target(&mut rng, &all);
                        lazy.seek(&t).unwrap();
                        eager.seek(&t).unwrap();
                    }
                    3 => {
                        // A forward seek only from a valid position, to a target at or past it.
                        if !eager.valid() {
                            continue;
                        }
                        let t = target(&mut rng, &all);
                        if t.as_slice() < eager.key() {
                            continue;
                        }
                        lazy.seek_forward(&t).unwrap();
                        eager.seek_forward(&t).unwrap();
                    }
                    4 => {
                        lazy.next().unwrap();
                        eager.next().unwrap();
                    }
                    _ => {
                        lazy.skip_row().unwrap();
                        eager.skip_row().unwrap();
                    }
                }
                same(&lazy, &eager, &case);
            }
            // And a full walk from the start.
            lazy.seek_to_first().unwrap();
            eager.seek_to_first().unwrap();
            while eager.valid() {
                same(&lazy, &eager, &case);
                lazy.next().unwrap();
                eager.next().unwrap();
            }
            same(&lazy, &eager, &case);
        }
    }

    /// The row prefix of `row`.
    fn prefix(row: &[u8]) -> Vec<u8> {
        let mut p = Vec::new();
        encode_row_prefix(&mut p, row).unwrap();
        p
    }

    /// A range (row starts) and the sources and level cursors it should get.
    type RangeCase<'a> = (Option<&'a [u8]>, Option<&'a [u8]>, usize, usize);

    /// `sst_sources_range` picks the SSTs a range overlaps: one source per SST of level 0,
    /// one per deeper level (a plain SST source when only one of its SSTs is in range).
    #[test]
    fn a_range_gets_one_source_per_level_below_0() {
        // Rows `a`..`f`, one cell each, cut into three SSTs: [a b] [c d] [e f].
        let all: Entries = [b"a", b"b", b"c", b"d", b"e", b"f"]
            .iter()
            .enumerate()
            .map(|(i, row)| {
                let mut key = Vec::new();
                encode_key(&mut key, &row[..], b"q", 1, i as u64 + 1, Kind::Put).unwrap();
                (key, vec![0, i as u8])
            })
            .collect();
        let (set, files) = ssts(&[&all[0..2], &all[2..4], &all[4..6]]);
        let l0 = ssts(&[&all[0..6]]).1;
        let fam = FamilySsts {
            levels: vec![l0, files],
        };
        // (start, end) -> (sources, of which levels)
        let cases: [RangeCase<'_>; 6] = [
            (None, None, 2, 1),
            (Some(b"b"), None, 2, 1),
            (Some(b"c"), Some(b"d"), 2, 0), // only [c d] in level 1
            (Some(b"e"), None, 2, 0),       // only [e f]
            (Some(b"b"), Some(b"d"), 2, 1), // [a b] and [c d]
            (Some(b"g"), None, 0, 0),
        ];
        for (start, end, sources, levels) in cases {
            let (start, end) = (start.map(prefix), end.map(prefix));
            let mut out = Vec::new();
            sst_sources_range::<false>(
                &fam,
                &set,
                &ScanFilter::all(),
                start.as_deref(),
                end.as_deref(),
                Priority::Normal,
                &mut out,
            )
            .unwrap();
            let lazy = out
                .iter()
                .filter(|s| matches!(s, Source::Sst(s) if s.level.is_some()))
                .count();
            assert_eq!((out.len(), lazy), (sources, levels), "{start:?}..{end:?}");
        }
    }
}
