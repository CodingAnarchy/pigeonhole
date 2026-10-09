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
use pigeonhole_format::key::{encode_column_prefix, encode_marker_prefix, escape_into};
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

/// The filter probes of a point read: hashes of the row, the column and the row's marker
/// key (FORMAT §6), computed once per read.
pub(crate) struct Probe {
    /// The escaped row prefix: inline for rows of usual length, so a point read allocates
    /// nothing for it (#46).
    pub row_prefix: SmallVec<[u8; 96]>,
    row: u64,
    column: u64,
    marker: u64,
}

thread_local! {
    /// Scratch for encoding a probe's keys, kept by each reading thread (#46). Taken while
    /// in use, so a nested probe on the same thread (none today) would get a fresh one.
    static PROBE_SCRATCH: StdCell<Vec<u8>> = const { StdCell::new(Vec::new()) };
}

impl Probe {
    pub(crate) fn new(row: &[u8], qualifier: &[u8]) -> Result<Self> {
        let mut buf = PROBE_SCRATCH.take();
        buf.clear();
        let probe = Self::encode(&mut buf, row, qualifier);
        // A key long enough to grow the scratch past 64 KiB is not worth keeping.
        if buf.capacity() <= 64 << 10 {
            PROBE_SCRATCH.set(buf);
        }
        probe
    }

    fn encode(buf: &mut Vec<u8>, row: &[u8], qualifier: &[u8]) -> Result<Self> {
        escape_into(buf, row);
        let row_h = row_hash(buf);
        buf.clear();
        encode_column_prefix(buf, row, qualifier)?;
        let column = column_hash(buf);
        buf.clear();
        encode_marker_prefix(buf, row)?;
        let marker = column_hash(buf);
        let row_prefix = SmallVec::from_slice(&buf[..buf.len() - 2]);
        Ok(Self {
            row_prefix,
            row: row_h,
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
    probe: &Probe,
    priority: Priority,
    out: &mut Vec<Source>,
) -> Result<()> {
    let opts = read_options(priority, false);
    for sst in fam.iter() {
        if !covers_row(sst, &probe.row_prefix) {
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
        row: &[u8],
        qualifier: &[u8],
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
            let probe = Probe::new(row, qualifier)?;
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
        row: &[u8],
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
            // Inline for rows of usual length: nothing allocated per read.
            let mut escaped = SmallVec::<[u8; 96]>::new();
            let mut rest = row;
            while let Some(i) = rest.iter().position(|&b| b == 0) {
                escaped.extend_from_slice(&rest[..=i]);
                escaped.push(0xFF);
                rest = &rest[i + 1..];
            }
            escaped.extend_from_slice(rest);
            sst_sources_row(
                fam, &self.ssts, filter, &escaped, row_prefix, l.priority, out,
            )?;
        }
        Ok(())
    }
}
