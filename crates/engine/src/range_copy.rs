//! A range move's copy (D331 PR 4a, ICR 0141): one chunk of a table's range at a snapshot,
//! as exactly what a compaction would write for it at that snapshot. Every version and
//! marker a reader at the snapshot could still need is kept, and a counter or aggregate
//! family's operands are folded into their base as compaction folds them (simp724's Q1 on
//! the PR 4 design), so a target that writes the chunk back
//! ([`WriteBatch::push_logged`](crate::WriteBatch::push_logged)) reads as the source did.
//!
//! - **Sources:** every source of each tablet the range overlaps (its memtables and SSTs),
//!   read without filling the block cache (perf287's should-fix), with entries above the
//!   snapshot passed over.
//! - **GC:** compaction's ([`StreamGc::folding`]), with the snapshot as the only read point,
//!   not bottommost (markers stay), and no other source of the slot (every one is read).
//! - **Chunks** end at a row boundary, once `max_bytes` is reached (simp724): a row's
//!   versions are never split between two snapshots.

use std::collections::VecDeque;
use std::sync::Arc;

use pigeonhole_compaction::{GcPolicy, MergeOperator, MergingCursor, StreamGc};
use pigeonhole_format::Cursor;
use pigeonhole_format::key::{Kind, decode_key, encode_row_prefix, split_suffix};
use pigeonhole_format::scan::ScanFilter;
use pigeonhole_format::{FamilyId, Seqno, TableId, Timestamp};
use pigeonhole_sst::ReadOptions;

use crate::error::{Error, Result};
use crate::snapshot::{Snapshot, TabletEntry, View};
use crate::source::Source;

/// One cell a copy writes back, as its source logged it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopiedCell {
    /// The family.
    pub family: FamilyId,
    /// Put, merge (a counter family's base), or a delete marker.
    pub kind: Kind,
    /// The row.
    pub row: Vec<u8>,
    /// The qualifier; empty for a family marker.
    pub qualifier: Vec<u8>,
    /// The timestamp.
    pub ts: Timestamp,
    /// The stored value, tag byte first, a separated value read back inline; empty for a
    /// marker.
    pub stored: Vec<u8>,
}

/// One chunk of a range copy ([`Engine::copy_range`](crate::Engine::copy_range)).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RangeChunk {
    /// The chunk's cells, in row order, each row whole.
    pub cells: Vec<CopiedCell>,
    /// The row to ask for next; `None`: the range is done.
    pub next: Option<Vec<u8>>,
}

/// One family's GC output, kept until its row is taken.
struct FamilyStream {
    family: FamilyId,
    /// A counter or aggregate family: its base is per `(column, ts)`.
    aggregate: bool,
    merge: Option<Arc<dyn MergeOperator>>,
    cursor: MergingCursor<AtOrBelow<Source>>,
    gc: StreamGc,
    /// Cells read and not yet taken, in key order.
    buf: VecDeque<CopiedCell>,
    done: bool,
}

impl FamilyStream {
    /// Fills `buf` until it holds a cell, or the source ends.
    fn fill(&mut self, end: Option<&[u8]>, view: &View) -> Result<()> {
        while self.buf.is_empty() && !self.done {
            if !self.gc.step(&mut self.cursor)? {
                self.done = true;
            }
            let mut out = Vec::new();
            self.gc.drain(|k, v| {
                out.push((k.to_vec(), v.to_vec()));
                Ok::<(), Error>(())
            })?;
            // One step's output is one unit: a family marker, or one `(column, ts)` group.
            let mut group = Vec::with_capacity(out.len());
            for (k, v) in out {
                let parts = decode_key(&k)?;
                let mut row = Vec::new();
                parts.row.unescape_into(&mut row);
                if end.is_some_and(|e| row.as_slice() >= e) {
                    self.done = true;
                    break;
                }
                let mut qualifier = Vec::new();
                if let Some(q) = parts.qualifier {
                    q.unescape_into(&mut qualifier);
                }
                // A separated value is copied as the value, not its pointer: the target's
                // blob files aren't the source's.
                let stored = match view.ssts.read_blob(&v)? {
                    Some(b) => b.to_vec(),
                    None => v,
                };
                group.push(CopiedCell {
                    family: self.family,
                    kind: parts.kind,
                    row,
                    qualifier,
                    ts: parts.ts,
                    stored,
                });
            }
            self.settle(&mut group);
            self.buf.extend(group);
        }
        Ok(())
    }

    /// Puts one `(column, ts)` group in the order a target applies it, and makes a lone
    /// aggregate operand a base.
    /// - **Several entries** (an aggregate value newer than a delete kept beside it, a fold
    ///   that failed): one commit keeps only the last mutation of a cell and timestamp
    ///   (D34), so the target commits them apart, oldest first, as the source's seqnos
    ///   ordered them (key order is newest first).
    /// - **A lone aggregate operand** (the fold of every source, simp724's (d)): written as
    ///   a put of its finished value, which an aggregate family reads as the base its later
    ///   operands apply to, per `(column, ts)`, as D291's checkpoints are. A put is
    ///   idempotent, so a chunk copied twice reads the same. A standard family's operands
    ///   run across timestamps (D41), so they stay operands.
    fn settle(&self, group: &mut [CopiedCell]) {
        if group.len() > 1 {
            group.reverse();
            return;
        }
        let ([c], true, Some(op)) = (group, self.aggregate, &self.merge) else {
            return;
        };
        if c.kind != Kind::Merge {
            return;
        }
        let mut base = c.stored.clone();
        if op.finish(None, &mut base).is_ok() {
            c.kind = Kind::Put;
            c.stored = base;
        }
    }
}

/// [`Engine::copy_range`](crate::Engine::copy_range).
pub(crate) fn copy_range(
    snapshot: &Snapshot,
    now: Timestamp,
    table: TableId,
    after: &[u8],
    end: Option<&[u8]>,
    max_bytes: usize,
) -> Result<RangeChunk> {
    let view = &snapshot.view;
    let families = view.catalog.family_ids_of(table);
    if view.catalog.table(table).is_none() {
        return Err(Error::TableNotFound(format!("table {}", table.0)));
    }
    let mut tablets: Vec<&TabletEntry> = view
        .tablets()
        .tablets_of(table)
        .iter()
        .filter(|t| {
            t.end.as_deref().is_none_or(|e| after < e) && end.is_none_or(|e| t.start.as_slice() < e)
        })
        .collect();
    tablets.sort_by(|a, b| a.start.cmp(&b.start));
    let mut chunk = RangeChunk::default();
    let mut bytes = 0usize;
    for t in tablets {
        // The tablet's part of the range: from the later of its start and `after`, to the
        // earlier of its end and `end`.
        let from = if t.start.as_slice() > after {
            t.start.as_slice()
        } else {
            after
        };
        let to = match (t.end.as_deref(), end) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let mut streams = families
            .iter()
            .map(|&f| open(view, snapshot.seqno, now, t, f, from, to))
            .collect::<Result<Vec<_>>>()?;
        loop {
            for s in &mut streams {
                s.fill(to, view)?;
            }
            // The smallest row any family still holds.
            let Some(row) = streams
                .iter()
                .filter_map(|s| s.buf.front().map(|c| c.row.clone()))
                .min()
            else {
                break;
            };
            if bytes >= max_bytes && !chunk.cells.is_empty() {
                chunk.next = Some(row);
                return Ok(chunk);
            }
            // That row whole, from every family.
            for s in &mut streams {
                loop {
                    s.fill(to, view)?;
                    match s.buf.front() {
                        Some(c) if c.row == row => {
                            let c = s.buf.pop_front().expect("checked");
                            bytes += c.row.len() + c.qualifier.len() + c.stored.len() + 32;
                            chunk.cells.push(c);
                        }
                        _ => break,
                    }
                }
            }
        }
    }
    Ok(chunk)
}

/// One family's stream over tablet `t` from row `from` to `to`: its sources at `seqno`, under
/// compaction's GC.
fn open(
    view: &View,
    seqno: u64,
    now: Timestamp,
    t: &TabletEntry,
    family: FamilyId,
    from: &[u8],
    to: Option<&[u8]>,
) -> Result<FamilyStream> {
    let meta = view
        .catalog
        .family(family)
        .ok_or_else(|| Error::FamilyNotFound(format!("family {}", family.0)))?;
    let all = ScanFilter::all();
    let l = view.locate(t.shard, t.id, family);
    let mut sources = Vec::new();
    if let Some(set) = l.mems {
        crate::source::mem_sources(set, &all, false, &mut sources);
    }
    if let Some(fam) = l.ssts {
        let mut read = ReadOptions::default();
        read.fill_cache = false;
        read.readahead_blocks = 4;
        crate::source::sst_sources_with::<false>(
            fam,
            &view.ssts,
            &all,
            // Only the SSTs the chunk's rows can be in (perf287's should-fix on B2): every
            // other one's index would be read again for each chunk, uncached.
            (!from.is_empty()).then_some(from),
            to,
            l.priority,
            read,
            &mut sources,
        )?;
    }
    let sources = sources
        .into_iter()
        .map(|inner| AtOrBelow { inner, seqno })
        .collect();
    let mut cursor = MergingCursor::new(sources);
    if from.is_empty() {
        cursor.seek_to_first()?;
    } else {
        let mut k = Vec::new();
        encode_row_prefix(&mut k, from)?;
        cursor.seek(&k)?;
    }
    let mut policy = GcPolicy::new(vec![seqno], now, false);
    // Every source of the slot is read, so a counter family's operands combine.
    policy.other_sources = Some(Vec::new());
    let base_blobs: Arc<dyn pigeonhole_compaction::BlobFetch> =
        Arc::new(crate::compact::BaseBlobs(Arc::clone(&view.ssts)));
    let gc = StreamGc::folding(
        &policy,
        &meta.options,
        meta.merge_op.clone(),
        Some(base_blobs),
    );
    Ok(FamilyStream {
        family,
        aggregate: meta.options.kind.is_aggregate(),
        merge: meta.merge_op.clone(),
        cursor,
        gc,
        buf: VecDeque::new(),
        done: false,
    })
}

/// A source with its entries above `seqno` passed over: a snapshot's sources include
/// memtable entries committed after it, which neither the scan filter nor the GC drops (a
/// version newer than every read point is kept).
struct AtOrBelow<C> {
    inner: C,
    seqno: Seqno,
}

impl<C: Cursor> AtOrBelow<C> {
    fn pass_newer(&mut self) -> std::result::Result<(), C::Error> {
        // A key whose suffix doesn't decode is left to the GC, which reports it.
        while self.inner.valid()
            && split_suffix(self.inner.key()).is_ok_and(|(_, _, s, _)| s > self.seqno)
        {
            self.inner.next()?;
        }
        Ok(())
    }
}

impl<C: Cursor> Cursor for AtOrBelow<C> {
    type Error = C::Error;

    fn valid(&self) -> bool {
        self.inner.valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn seek_to_first(&mut self) -> std::result::Result<(), Self::Error> {
        self.inner.seek_to_first()?;
        self.pass_newer()
    }

    fn seek(&mut self, target: &[u8]) -> std::result::Result<(), Self::Error> {
        self.inner.seek(target)?;
        self.pass_newer()
    }

    fn next(&mut self) -> std::result::Result<(), Self::Error> {
        self.inner.next()?;
        self.pass_newer()
    }
}
