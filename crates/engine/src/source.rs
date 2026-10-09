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
use pigeonhole_format::key::{MARKER_QUALIFIER, MAX_KEY_PART, TERMINATOR, escape_into};
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
    /// An SST (the filter runs inside its block decoder).
    Sst(SstIter),
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
            Source::Sst(it) => Ok(it.seek_to_first()?),
            #[cfg(test)]
            Source::Vec(it) => Ok(it.seek_to_first()?),
        }
    }

    fn seek(&mut self, target: &[u8]) -> Result<()> {
        match self {
            Source::Mem(it) => Ok(it.seek(target)?),
            Source::Sst(it) => Ok(it.seek(target)?),
            #[cfg(test)]
            Source::Vec(it) => Ok(it.seek(target)?),
        }
    }

    fn seek_forward(&mut self, target: &[u8]) -> Result<()> {
        match self {
            // A finger search from the memtable cursor's last seek.
            Source::Mem(it) => Ok(it.seek_forward(target)?),
            Source::Sst(it) => Ok(it.seek_forward(target)?),
            #[cfg(test)]
            Source::Vec(it) => Ok(it.seek_forward(target)?),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self {
            Source::Mem(it) => Ok(it.next()?),
            Source::Sst(it) => Ok(it.next()?),
            #[cfg(test)]
            Source::Vec(it) => Ok(it.next()?),
        }
    }

    fn skip_row(&mut self) -> Result<()> {
        match self {
            Source::Mem(it) => Ok(it.skip_row()?),
            Source::Sst(it) => Ok(it.skip_row()?),
            #[cfg(test)]
            Source::Vec(it) => Ok(it.skip_row()?),
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
    /// The current value, pinned without a copy where the source allows it.
    ///
    /// # Panics
    /// If the source is not valid.
    pub(crate) fn pin_value(&self) -> Pinned {
        match self {
            Source::Mem(it) => Pinned::Arena(it.inner().value_slice()),
            Source::Sst(it) => Pinned::Block(it.value_cell()),
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

/// Read options for a scan or a point read.
fn read_options(priority: Priority, scan: bool) -> ReadOptions {
    let mut o = ReadOptions::default();
    o.priority = priority;
    o.readahead_blocks = if scan { 4 } else { 0 };
    o
}

/// SST sources for a scan of `[start, end)` (row prefixes), newest first.
pub(crate) fn sst_sources_range(
    fam: &FamilySsts,
    set: &SstSet,
    filter: &ScanFilter,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    priority: Priority,
    out: &mut Vec<Source>,
) -> Result<()> {
    let opts = read_options(priority, true);
    for sst in fam.iter() {
        if !overlaps_range(sst, start, end) {
            continue;
        }
        let reader = sst.reader(set, priority)?;
        out.push(Source::Sst(reader.iter(filter.clone(), opts)));
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
pub(crate) fn sst_sources_point(
    fam: &FamilySsts,
    set: &SstSet,
    probe: &Probe<'_>,
    priority: Priority,
    out: &mut Vec<Source>,
) -> Result<()> {
    let opts = read_options(priority, false);
    for sst in fam.iter() {
        if !covers_row(sst, probe.row_prefix) {
            continue;
        }
        let reader = sst.reader(set, priority)?;
        if !reader.may_contain_row(probe.row)
            || !(reader.may_contain_column(probe.column) || reader.may_contain_column(probe.marker))
        {
            continue;
        }
        out.push(Source::Sst(reader.iter(ScanFilter::all(), opts)));
    }
    Ok(())
}

/// SST sources for reading one whole row (the row filter applies), newest first.
pub(crate) fn sst_sources_row(
    fam: &FamilySsts,
    set: &SstSet,
    filter: &ScanFilter,
    escaped_row: &[u8],
    row_prefix: &[u8],
    priority: Priority,
    out: &mut Vec<Source>,
) -> Result<()> {
    let opts = read_options(priority, false);
    let hash = row_hash(escaped_row);
    for sst in fam.iter() {
        if !covers_row(sst, row_prefix) {
            continue;
        }
        let reader = sst.reader(set, priority)?;
        if !reader.may_contain_row(hash) {
            continue;
        }
        out.push(Source::Sst(reader.iter(filter.clone(), opts)));
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
        let l = self.locate(shard, tablet, family);
        let mut out = Vec::new();
        if let Some(set) = l.mems {
            mem_sources(set, filter, &mut out);
        }
        if let Some(fam) = l.ssts {
            sst_sources_range(fam, &self.ssts, filter, start, end, l.priority, &mut out)?;
        }
        Ok(out)
    }

    /// Sources for a point read of one column, newest first.
    pub(crate) fn point_sources(
        &self,
        shard: ShardId,
        tablet: TabletId,
        family: FamilyId,
        key: &mut ColumnKey,
        out: &mut Vec<Source>,
    ) -> Result<()> {
        let l = self.locate(shard, tablet, family);
        // Room for the memtables and a couple of SSTs: a source is about 1 KiB, and a `Vec`
        // grown from empty would hold four (#46). `out` is normally a reused buffer that
        // already has it.
        let mems = l.mems.map_or(0, |set| set.readers.len());
        let ssts = l.ssts.map_or(0, |fam| fam.iter().take(2).count());
        out.reserve(mems + ssts);
        let all = ScanFilter::all();
        if let Some(set) = l.mems {
            mem_sources(set, &all, out);
        }
        if let Some(fam) = l.ssts
            && !fam.is_empty()
        {
            let probe = Probe::new(key)?;
            sst_sources_point(fam, &self.ssts, &probe, l.priority, out)?;
        }
        Ok(())
    }

    /// Sources for reading one row, newest first, appended to `out` (a reused source list).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn row_sources_into(
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
            sst_sources_row(
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
