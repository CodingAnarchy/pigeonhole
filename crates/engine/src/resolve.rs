//! The read machinery: a k-way merge over sources and the MVCC resolver that applies snapshot
//! visibility, the delete rules (D9, D10, D38), TTL (D11), version limits, filters and `i64`
//! merge folding (D41) to an ordered cursor, exactly as `pigeonhole_sim::Model` does.
//!
//! The resolver is a lending iterator: each resolved cell borrows the resolver until the next
//! call. A resolved put is *held* (an `ArenaSlice` pin, no copy) rather than borrowed from
//! the cursor, because the rules need one entry of lookahead: a cell delete committed after a
//! put at the same timestamp sorts behind it yet hides it.

use std::ops::Deref;

use pigeonhole_format::key::{
    Kind, MARKER_QUALIFIER, encode_column_prefix, encode_marker_prefix, row_prefix_len,
    split_suffix,
};
use pigeonhole_format::scan::ScanFilter;
use pigeonhole_format::value::{ValueRef, decode_value, encode_value};
use pigeonhole_format::{Cursor, Seqno, Timestamp};
use pigeonhole_memtable::{ArenaSlice, MemIter};

use crate::catalog::MergeKind;
use crate::{Error, Result, ValuePredicate};

/// A pinned stored value that outlives the cursor position it came from.
#[derive(Debug, Clone)]
pub(crate) enum Hold {
    /// A range of a memtable arena (an `Arc` pin, no copy).
    Arena(ArenaSlice),
    /// An owned copy (blob reads from Milestone B; tests).
    #[cfg_attr(not(test), allow(dead_code))]
    Owned(Vec<u8>),
}

impl Deref for Hold {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            Hold::Arena(s) => s,
            Hold::Owned(v) => v,
        }
    }
}

/// A sorted source the resolver can pin values from.
pub(crate) trait Source: Cursor<Error = Error> {
    /// The current value, pinned.
    fn hold_value(&self) -> Hold;
}

/// The engine's sources, as an enum so the per-entry path has no virtual calls.
#[derive(Debug)]
pub(crate) enum SourceCursor {
    /// A memtable (active or frozen).
    Mem(MemIter),
}

impl Cursor for SourceCursor {
    type Error = Error;

    #[inline]
    fn valid(&self) -> bool {
        match self {
            SourceCursor::Mem(it) => it.valid(),
        }
    }

    #[inline]
    fn key(&self) -> &[u8] {
        match self {
            SourceCursor::Mem(it) => it.key(),
        }
    }

    #[inline]
    fn value(&self) -> &[u8] {
        match self {
            SourceCursor::Mem(it) => it.value(),
        }
    }

    fn seek_to_first(&mut self) -> Result<()> {
        match self {
            SourceCursor::Mem(it) => Ok(it.seek_to_first()?),
        }
    }

    fn seek(&mut self, target: &[u8]) -> Result<()> {
        match self {
            SourceCursor::Mem(it) => Ok(it.seek(target)?),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self {
            SourceCursor::Mem(it) => Ok(it.next()?),
        }
    }

    fn skip_row(&mut self) -> Result<()> {
        match self {
            SourceCursor::Mem(it) => Ok(it.skip_row()?),
        }
    }
}

impl Source for SourceCursor {
    fn hold_value(&self) -> Hold {
        match self {
            SourceCursor::Mem(it) => Hold::Arena(it.value_slice()),
        }
    }
}

/// A k-way merge of sources into one ordered cursor. Sources are ordered newest first; since
/// internal keys are unique (they contain the seqno) ties cannot occur, and if one ever did
/// the newest source wins. Sources are few (a handful of memtables), so the minimum is found
/// by a linear scan rather than a heap.
#[derive(Debug)]
pub(crate) struct Merge<S> {
    sources: Vec<S>,
    cur: Option<usize>,
}

impl<S: Source> Merge<S> {
    pub(crate) fn new(sources: Vec<S>) -> Self {
        Self { sources, cur: None }
    }

    fn pick(&mut self) {
        let mut best: Option<usize> = None;
        for (i, s) in self.sources.iter().enumerate() {
            if !s.valid() {
                continue;
            }
            match best {
                None => best = Some(i),
                Some(b) if s.key() < self.sources[b].key() => best = Some(i),
                _ => {}
            }
        }
        self.cur = best;
    }
}

impl<S: Source> Cursor for Merge<S> {
    type Error = Error;

    #[inline]
    fn valid(&self) -> bool {
        self.cur.is_some()
    }

    #[inline]
    fn key(&self) -> &[u8] {
        self.sources[self.cur.expect("key on an invalid cursor")].key()
    }

    #[inline]
    fn value(&self) -> &[u8] {
        self.sources[self.cur.expect("value on an invalid cursor")].value()
    }

    fn seek_to_first(&mut self) -> Result<()> {
        for s in &mut self.sources {
            s.seek_to_first()?;
        }
        self.pick();
        Ok(())
    }

    fn seek(&mut self, target: &[u8]) -> Result<()> {
        for s in &mut self.sources {
            s.seek(target)?;
        }
        self.pick();
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        if let Some(i) = self.cur {
            self.sources[i].next()?;
            self.pick();
        }
        Ok(())
    }
}

impl<S: Source> Source for Merge<S> {
    fn hold_value(&self) -> Hold {
        self.sources[self.cur.expect("hold on an invalid cursor")].hold_value()
    }
}

/// What the read path asks of the resolver.
#[derive(Debug, Clone)]
pub(crate) struct ResolveOpts {
    /// Ignore entries with a newer seqno.
    pub snapshot: Seqno,
    /// Current time in microseconds, for TTL.
    pub now: Timestamp,
    /// Family TTL in microseconds (0 = none).
    pub ttl_micros: u64,
    /// The family's `max_versions` (0 = all).
    pub max_versions: u32,
    /// Versions the caller wants per column (0 = all retained).
    pub versions: u32,
    /// Columns per row (0 = unlimited).
    pub columns_per_row: u32,
    /// Keep only columns whose newest visible value matches.
    pub value: Option<ValuePredicate>,
    /// The family's merge operator.
    pub merge: MergeKind,
    /// Entry-level filter (qualifier selection, time range on puts).
    pub filter: ScanFilter,
}

impl ResolveOpts {
    pub(crate) fn new(snapshot: Seqno, now: Timestamp) -> Self {
        Self {
            snapshot,
            now,
            ttl_micros: 0,
            max_versions: 0,
            versions: 1,
            columns_per_row: 0,
            value: None,
            merge: MergeKind::None,
            filter: ScanFilter::all(),
        }
    }

    /// Versions returned per column (0 = unlimited).
    fn cap(&self) -> u32 {
        match (self.max_versions, self.versions) {
            (0, v) => v,
            (m, 0) => m,
            (m, v) => m.min(v),
        }
    }
}

/// The value of a resolved cell.
#[derive(Debug)]
enum OutValue {
    /// A held put.
    Held(Hold),
    /// A merge result in the resolver's buffer.
    Merged,
}

/// A resolved cell, borrowed from the resolver.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ResolvedCell<'a> {
    /// The column prefix of the version (escaped row, terminator, escaped qualifier,
    /// terminator): decode with `pigeonhole_format::decode_key` after appending any suffix,
    /// or split the row with `row_prefix_len`.
    pub column: &'a [u8],
    /// Timestamp (of the newest operand for a folded counter).
    pub ts: Timestamp,
    /// The stored value, tag byte included.
    pub value: &'a [u8],
    /// The pinned value, when it is a held put (not a merge result).
    pub hold: Option<&'a Hold>,
}

/// The entries at one timestamp of one column, newest seqno first.
#[derive(Debug, Default)]
struct Group {
    ts: Timestamp,
    /// The newest-seqno put at this timestamp, once seen.
    base: Option<Hold>,
    /// Sum of merge operands newer than the base.
    operands: i64,
    has_operands: bool,
    /// An operand that was not an `i64`.
    bad_operand: bool,
    cell_delete: bool,
    column_delete: bool,
}

/// Applies MVCC visibility to an ordered cursor of one family's entries: snapshot seqno,
/// cell, column and family deletes, TTL, version limits, columns per row, filters, value
/// predicates and `i64` merge folding. No allocation per cell: keys are copied into reusable
/// buffers and values are pinned.
#[derive(Debug)]
pub(crate) struct Resolver<S> {
    cur: S,
    opts: ResolveOpts,
    cap: u32,
    /// Escaped row plus terminator of the current row; empty when not in a row.
    row_prefix: Vec<u8>,
    /// Highest timestamp a visible family marker of the current row covers.
    family_covered: Option<Timestamp>,
    columns_emitted: u32,
    /// Column prefix of the column being resolved; empty when between columns.
    col_prefix: Vec<u8>,
    /// The rest of the column is hidden (column delete, family marker, cap, predicate).
    col_hidden: bool,
    versions_emitted: u32,
    /// A run of operands without a base yet: `(timestamp of the newest, sum)`.
    run: Option<(Timestamp, i64)>,
    run_bad: bool,
    group: Option<Group>,
    /// Point-read mode: resolve one column, then stop.
    column_only: bool,
    column_done: bool,
    /// The emitted cell.
    out_key: Vec<u8>,
    out_ts: Timestamp,
    out_value: Option<OutValue>,
    merge_buf: Vec<u8>,
    seek_buf: Vec<u8>,
}

impl<S: Source> Resolver<S> {
    pub(crate) fn new(cur: S, opts: ResolveOpts) -> Self {
        let cap = opts.cap();
        Self {
            cur,
            opts,
            cap,
            row_prefix: Vec::new(),
            family_covered: None,
            columns_emitted: 0,
            col_prefix: Vec::new(),
            col_hidden: false,
            versions_emitted: 0,
            run: None,
            run_bad: false,
            group: None,
            column_only: false,
            column_done: false,
            out_key: Vec::new(),
            out_ts: 0,
            out_value: None,
            merge_buf: Vec::new(),
            seek_buf: Vec::new(),
        }
    }

    fn reset_row(&mut self) {
        self.row_prefix.clear();
        self.family_covered = None;
        self.columns_emitted = 0;
        self.reset_column();
    }

    fn reset_column(&mut self) {
        self.col_prefix.clear();
        self.col_hidden = false;
        self.versions_emitted = 0;
        self.run = None;
        self.run_bad = false;
        self.group = None;
    }

    /// Positions at the first entry `>= key` (an encoded row prefix for scans).
    pub(crate) fn seek(&mut self, key: &[u8]) -> Result<()> {
        self.column_only = false;
        self.column_done = false;
        self.reset_row();
        self.out_value = None;
        self.cur.seek(key)
    }

    /// Positions for a point read of one column: seeks to the row's marker prefix, records
    /// the family markers visible at the snapshot, then seeks to the column (one extra seek
    /// per source, decision D9). Only that column is resolved.
    pub(crate) fn seek_column(&mut self, row: &[u8], qualifier: &[u8]) -> Result<()> {
        self.reset_row();
        self.out_value = None;
        self.column_only = true;
        self.column_done = false;
        let mut buf = std::mem::take(&mut self.seek_buf);
        buf.clear();
        encode_marker_prefix(&mut buf, row)?;
        self.cur.seek(&buf)?;
        self.row_prefix
            .extend_from_slice(&buf[..buf.len() - MARKER_QUALIFIER.len()]);
        self.consume_markers()?;
        buf.clear();
        encode_column_prefix(&mut buf, row, qualifier)?;
        self.cur.seek(&buf)?;
        self.col_prefix.extend_from_slice(&buf);
        self.seek_buf = buf;
        Ok(())
    }

    /// Skips the rest of the current row.
    pub(crate) fn skip_row(&mut self) -> Result<()> {
        if self.row_prefix.is_empty() {
            return Ok(());
        }
        while self.cur.valid() && self.cur.key().starts_with(&self.row_prefix) {
            self.cur.skip_row()?;
        }
        self.reset_row();
        Ok(())
    }

    /// Reads the family markers at the front of the current row (the cursor is at or past
    /// the row's marker prefix) and records the newest visible one.
    fn consume_markers(&mut self) -> Result<()> {
        while self.cur.valid() {
            let key = self.cur.key();
            let n = self.row_prefix.len();
            if !key.starts_with(&self.row_prefix) || key.get(n..n + 2) != Some(&MARKER_QUALIFIER) {
                break;
            }
            let (_, ts, seqno, kind) = split_suffix(key)?;
            if seqno <= self.opts.snapshot && kind == Kind::FamilyDelete {
                self.family_covered = Some(self.family_covered.map_or(ts, |c| c.max(ts)));
            }
            self.cur.next()?;
        }
        Ok(())
    }

    /// Whether a cell is held (emitted by the last `next_cell`).
    pub(crate) fn has_cell(&self) -> bool {
        self.out_value.is_some()
    }

    /// The emitted cell, after a step reported one.
    pub(crate) fn current_cell(&self) -> ResolvedCell<'_> {
        match &self.out_value {
            Some(OutValue::Held(h)) => ResolvedCell {
                column: &self.out_key,
                ts: self.out_ts,
                value: h,
                hold: Some(h),
            },
            Some(OutValue::Merged) => ResolvedCell {
                column: &self.out_key,
                ts: self.out_ts,
                value: &self.merge_buf,
                hold: None,
            },
            None => unreachable!("no cell emitted"),
        }
    }

    /// The next visible cell, or `None` at the end of the cursor (or of the column, for a
    /// point read).
    pub(crate) fn next_cell(&mut self) -> Result<Option<ResolvedCell<'_>>> {
        self.out_value = None;
        loop {
            if self.column_done {
                return Ok(None);
            }
            if !self.cur.valid() {
                let emitted = self.finalize_column()?;
                return Ok(emitted.then(|| self.current_cell()));
            }
            if !self.col_prefix.is_empty() {
                if self.cur.key().starts_with(&self.col_prefix) {
                    if self.col_hidden {
                        self.skip_column()?;
                        continue;
                    }
                    if self.step_entry()? {
                        return Ok(Some(self.current_cell()));
                    }
                    continue;
                }
                // The cursor left the column.
                if self.finalize_column()? {
                    return Ok(Some(self.current_cell()));
                }
                continue;
            }
            if self.column_only {
                // The single column has been resolved.
                self.column_done = true;
                return Ok(None);
            }
            // Between columns: row boundaries first.
            if self.row_prefix.is_empty() || !self.cur.key().starts_with(&self.row_prefix) {
                let n = row_prefix_len(self.cur.key())?;
                self.reset_row();
                self.row_prefix.extend_from_slice(&self.cur.key()[..n]);
                self.consume_markers()?;
                continue;
            }
            if self.opts.columns_per_row != 0 && self.columns_emitted >= self.opts.columns_per_row {
                self.skip_row()?;
                continue;
            }
            // Entry-level filter: skip excluded columns with a seek hint.
            if !self.opts.filter.is_all() && !self.opts.filter.admits(self.cur.key()) {
                let mut hint = std::mem::take(&mut self.seek_buf);
                hint.clear();
                let has_hint = self.opts.filter.next_admissible(self.cur.key(), &mut hint);
                let past = hint.as_slice() > self.cur.key();
                if !has_hint {
                    self.seek_buf = hint;
                    self.skip_row()?;
                } else if past {
                    self.cur.seek(&hint)?;
                    self.seek_buf = hint;
                } else {
                    self.seek_buf = hint;
                    self.cur.next()?;
                }
                continue;
            }
            // Start the column under the cursor.
            let (prefix, ..) = split_suffix(self.cur.key())?;
            self.col_prefix.extend_from_slice(prefix);
        }
    }

    /// Advances past every remaining entry of the current column and ends it.
    fn skip_column(&mut self) -> Result<()> {
        // The column prefix ends with the terminator `00 01`; bumping its last byte gives the
        // smallest key past every entry of the column.
        let mut target = std::mem::take(&mut self.seek_buf);
        target.clear();
        target.extend_from_slice(&self.col_prefix);
        if let Some(last) = target.last_mut() {
            *last += 1;
        }
        self.cur.seek(&target)?;
        self.seek_buf = target;
        self.reset_column();
        if self.column_only {
            self.column_done = true;
        }
        Ok(())
    }

    /// Consumes the entry under the cursor (which belongs to the current column, and is not
    /// hidden). Returns whether a cell was emitted.
    fn step_entry(&mut self) -> Result<bool> {
        let key = self.cur.key();
        let (_, ts, seqno, kind) = split_suffix(key)?;
        if seqno > self.opts.snapshot {
            self.cur.next()?;
            return Ok(false);
        }
        if !self.opts.filter.is_all() && !self.opts.filter.admits(key) {
            self.cur.next()?;
            return Ok(false);
        }
        if self.family_covered.is_some_and(|c| ts <= c) {
            // Everything from here on in this column is older and hidden by the marker; the
            // group and run above it still count.
            self.col_hidden = true;
            if self.finish_group()? {
                return Ok(true);
            }
            return self.flush_run();
        }
        if self.group.as_ref().is_some_and(|g| g.ts != ts) {
            // The cursor stays on this entry; it starts the next group afterwards.
            return self.finish_group();
        }
        let group = self.group.get_or_insert_with(|| Group {
            ts,
            ..Group::default()
        });
        match kind {
            Kind::ColumnDelete => group.column_delete = true,
            Kind::CellDelete => group.cell_delete = true,
            Kind::Put | Kind::Merge => {
                let expired = self.opts.ttl_micros != 0
                    && ts.saturating_add(self.opts.ttl_micros) <= self.opts.now;
                if !expired && group.base.is_none() {
                    if kind == Kind::Put {
                        group.base = Some(self.cur.hold_value());
                    } else {
                        group.has_operands = true;
                        match as_i64(self.cur.value()) {
                            Some(d) => group.operands = group.operands.wrapping_add(d),
                            None => group.bad_operand = true,
                        }
                    }
                }
            }
            Kind::FamilyDelete => {}
        }
        self.cur.next()?;
        Ok(false)
    }

    /// Ends the current column (the cursor is past it): finalizes the open group and the
    /// pending run, then clears the column state. Returns whether a cell was emitted; when
    /// it did, the column state is cleared on the next call instead.
    fn finalize_column(&mut self) -> Result<bool> {
        if self.col_prefix.is_empty() {
            return Ok(false);
        }
        if self.finish_group()? {
            return Ok(true);
        }
        let emitted = self.flush_run()?;
        self.reset_column();
        if self.column_only {
            self.column_done = true;
        }
        Ok(emitted)
    }

    /// Emits the pending operand run (no base below it) as a version.
    fn flush_run(&mut self) -> Result<bool> {
        let Some((ts, sum)) = self.run.take() else {
            return Ok(false);
        };
        if std::mem::take(&mut self.run_bad) {
            return Err(self.merge_failed());
        }
        self.emit_merged(ts, sum)
    }

    fn merge_failed(&self) -> Error {
        match self.opts.merge {
            MergeKind::Unknown => Error::UnknownMergeOperator(String::new()),
            _ => Error::Merge(pigeonhole_compaction::MergeError {
                operator: crate::catalog::I64_ADD.to_owned(),
                message: "the base value is not an i64".to_owned(),
            }),
        }
    }

    /// Finalizes the open timestamp group. Returns whether a cell was emitted.
    fn finish_group(&mut self) -> Result<bool> {
        let Some(group) = self.group.take() else {
            return Ok(false);
        };
        if group.column_delete {
            // This group and everything older is hidden; the run above it still counts.
            self.col_hidden = true;
            return self.flush_run();
        }
        if group.cell_delete {
            return Ok(false);
        }
        match group.base {
            Some(base) if self.run.is_none() && !group.has_operands => {
                self.out_key.clear();
                self.out_key.extend_from_slice(&self.col_prefix);
                self.out_ts = group.ts;
                self.out_value = Some(OutValue::Held(base));
                Ok(self.accept())
            }
            Some(base) => {
                let (run_ts, run_sum) = self.run.take().unwrap_or((group.ts, 0));
                let bad = std::mem::take(&mut self.run_bad) || group.bad_operand;
                match (bad, as_i64(&base)) {
                    (false, Some(b)) => {
                        let total = run_sum.wrapping_add(group.operands).wrapping_add(b);
                        self.emit_merged(run_ts, total)
                    }
                    _ => Err(self.merge_failed()),
                }
            }
            None => {
                if group.has_operands {
                    let (run_ts, sum) = self.run.unwrap_or((group.ts, 0));
                    self.run = Some((run_ts, sum.wrapping_add(group.operands)));
                    self.run_bad |= group.bad_operand;
                }
                Ok(false)
            }
        }
    }

    /// Emits a folded counter version.
    fn emit_merged(&mut self, ts: Timestamp, total: i64) -> Result<bool> {
        self.merge_buf.clear();
        encode_value(&mut self.merge_buf, ValueRef::I64(total));
        self.out_key.clear();
        self.out_key.extend_from_slice(&self.col_prefix);
        self.out_ts = ts;
        self.out_value = Some(OutValue::Merged);
        Ok(self.accept())
    }

    /// Applies the value predicate (newest version of the column only) and the version cap
    /// to the staged cell. Returns whether it is emitted.
    fn accept(&mut self) -> bool {
        if self.versions_emitted == 0 {
            if let Some(p) = &self.opts.value {
                let value = match &self.out_value {
                    Some(OutValue::Held(h)) => &h[..],
                    _ => &self.merge_buf[..],
                };
                if !predicate_matches(p, value) {
                    // The column is excluded as a whole.
                    self.out_value = None;
                    self.col_hidden = true;
                    self.run = None;
                    return false;
                }
            }
            self.columns_emitted += 1;
        }
        self.versions_emitted += 1;
        if self.cap != 0 && self.versions_emitted >= self.cap {
            self.col_hidden = true;
            self.run = None;
        }
        true
    }
}

/// A stored value as an `i64`: a tagged `I64`, or raw bytes of length 8 (the model's rule).
fn as_i64(stored: &[u8]) -> Option<i64> {
    match decode_value(stored).ok()? {
        ValueRef::I64(v) => Some(v),
        ValueRef::Bytes(b) => <[u8; 8]>::try_from(b).ok().map(i64::from_le_bytes),
        _ => None,
    }
}

/// Whether a stored value satisfies a predicate.
pub(crate) fn predicate_matches(p: &ValuePredicate, stored: &[u8]) -> bool {
    let Ok(value) = decode_value(stored) else {
        return false;
    };
    if let ValuePredicate::I64(ord, n) = p {
        let got = match value {
            ValueRef::I64(x) | ValueRef::Varint(x) => x,
            ValueRef::Bytes(b) => match <[u8; 8]>::try_from(b) {
                Ok(b) => i64::from_le_bytes(b),
                Err(_) => return false,
            },
            _ => return false,
        };
        return got.cmp(n) == *ord;
    }
    let buf;
    let bytes: &[u8] = match value {
        ValueRef::Bytes(b) => b,
        ValueRef::I64(x) | ValueRef::Varint(x) => {
            buf = x.to_le_bytes();
            &buf
        }
        ValueRef::F64(x) => {
            buf = x.to_bits().to_le_bytes();
            &buf
        }
        ValueRef::Blob(_) => return false,
    };
    match p {
        ValuePredicate::Equals(e) => bytes == e.as_slice(),
        ValuePredicate::Prefix(pre) => bytes.starts_with(pre),
        ValuePredicate::Range(lo, hi) => {
            let lo_ok = match lo {
                std::ops::Bound::Included(l) => bytes >= l.as_slice(),
                std::ops::Bound::Excluded(l) => bytes > l.as_slice(),
                std::ops::Bound::Unbounded => true,
            };
            let hi_ok = match hi {
                std::ops::Bound::Included(h) => bytes <= h.as_slice(),
                std::ops::Bound::Excluded(h) => bytes < h.as_slice(),
                std::ops::Bound::Unbounded => true,
            };
            lo_ok && hi_ok
        }
        ValuePredicate::I64(..) => unreachable!("handled above"),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use pigeonhole_format::key::{encode_key, encode_marker_key};

    /// An in-memory source for resolver tests: sorted `(key, value)` pairs.
    #[derive(Debug, Clone, Default)]
    pub(crate) struct VecSource {
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        pos: usize,
    }

    impl VecSource {
        pub(crate) fn new(mut entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
            entries.sort();
            Self {
                entries,
                pos: usize::MAX,
            }
        }
    }

    impl Cursor for VecSource {
        type Error = Error;
        fn valid(&self) -> bool {
            self.pos < self.entries.len()
        }
        fn key(&self) -> &[u8] {
            &self.entries[self.pos].0
        }
        fn value(&self) -> &[u8] {
            &self.entries[self.pos].1
        }
        fn seek_to_first(&mut self) -> Result<()> {
            self.pos = 0;
            Ok(())
        }
        fn seek(&mut self, target: &[u8]) -> Result<()> {
            self.pos = self.entries.partition_point(|(k, _)| k.as_slice() < target);
            Ok(())
        }
        fn next(&mut self) -> Result<()> {
            if self.valid() {
                self.pos += 1;
            }
            Ok(())
        }
    }

    impl Source for VecSource {
        fn hold_value(&self) -> Hold {
            Hold::Owned(self.value().to_vec())
        }
    }

    fn put(row: &str, q: &str, ts: u64, seqno: u64, v: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let mut k = Vec::new();
        encode_key(&mut k, row.as_bytes(), q.as_bytes(), ts, seqno, Kind::Put).unwrap();
        let mut val = Vec::new();
        encode_value(&mut val, ValueRef::Bytes(v));
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

    fn cell(c: ResolvedCell<'_>) -> (String, String, u64, Vec<u8>) {
        let n = row_prefix_len(c.column).unwrap();
        let row = pigeonhole_format::key::Escaped::new(&c.column[..n - 2]);
        let qual = pigeonhole_format::key::Escaped::new(&c.column[n..c.column.len() - 2]);
        let (mut r, mut q) = (Vec::new(), Vec::new());
        row.unescape_into(&mut r);
        qual.unescape_into(&mut q);
        let v = match decode_value(c.value).unwrap() {
            ValueRef::Bytes(b) => b.to_vec(),
            ValueRef::I64(x) => x.to_le_bytes().to_vec(),
            other => panic!("{other:?}"),
        };
        (
            String::from_utf8(r).unwrap(),
            String::from_utf8(q).unwrap(),
            c.ts,
            v,
        )
    }

    fn collect(src: VecSource, opts: ResolveOpts) -> Vec<(String, String, u64, Vec<u8>)> {
        let mut r = Resolver::new(Merge::new(vec![src]), opts);
        r.seek(b"").unwrap();
        let mut out = Vec::new();
        while let Some(c) = r.next_cell().unwrap() {
            out.push(cell(c));
        }
        out
    }

    fn opts(snapshot: u64) -> ResolveOpts {
        let mut o = ResolveOpts::new(snapshot, 1_000);
        o.versions = 0;
        o.merge = MergeKind::I64Add;
        o
    }

    fn c(row: &str, q: &str, ts: u64, v: &[u8]) -> (String, String, u64, Vec<u8>) {
        (row.into(), q.into(), ts, v.to_vec())
    }

    #[test]
    fn newest_first_and_snapshot() {
        let src = VecSource::new(vec![
            put("r", "q", 10, 1, b"a"),
            put("r", "q", 20, 2, b"b"),
            put("r", "q", 15, 3, b"c"),
        ]);
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
        let src = VecSource::new(vec![
            put("r", "q", 10, 1, b"a"),
            del("r", "q", 10, 2, Kind::CellDelete),
            put("r", "q", 10, 3, b"later"),
            put("r", "q", 9, 4, b"old"),
        ]);
        assert_eq!(collect(src.clone(), opts(4)), vec![c("r", "q", 9, b"old")]);
        assert_eq!(collect(src, opts(1)), vec![c("r", "q", 10, b"a")]);
    }

    #[test]
    fn column_delete_hides_by_timestamp_not_seqno() {
        let src = VecSource::new(vec![
            put("r", "q", 10, 1, b"a"),
            put("r", "q", 30, 2, b"c"),
            del("r", "q", 20, 3, Kind::ColumnDelete),
            put("r", "q", 15, 4, b"late-old"),
            put("r", "q", 20, 5, b"at-delete"),
            put("r", "z", 1, 6, b"next-column"),
        ]);
        assert_eq!(
            collect(src, opts(6)),
            vec![c("r", "q", 30, b"c"), c("r", "z", 1, b"next-column")]
        );
    }

    #[test]
    fn family_marker_hides_row_cells_at_or_below_it() {
        let src = VecSource::new(vec![
            put("r", "a", 10, 1, b"a"),
            put("r", "b", 30, 1, b"b"),
            marker("r", 20, 2),
            put("r", "a", 20, 3, b"a2"),
            put("s", "a", 5, 1, b"other-row"),
        ]);
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
        let src = VecSource::new(vec![
            put("r", "q", 900, 1, b"old"),
            put("r", "q", 950, 2, b"new"),
        ]);
        let mut o = opts(2);
        o.ttl_micros = 100; // now = 1000: 900 + 100 <= 1000 expired, 950 lives
        assert_eq!(collect(src, o), vec![c("r", "q", 950, b"new")]);
    }

    #[test]
    fn merge_folds_onto_base_and_runs() {
        let base = 5i64.to_le_bytes();
        let src = VecSource::new(vec![
            put("r", "c", 10, 1, &base),
            incr("r", "c", 20, 2, 3),
            incr("r", "c", 30, 3, 4),
            put("r", "d", 10, 1, &base),
            incr("r", "d", 10, 2, 1),
            incr("r", "e", 7, 1, 2),
        ]);
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
        let src = VecSource::new(vec![
            put("r", "c", 10, 1, b"bad"),
            incr("r", "c", 20, 2, 3),
            put("r", "c", 30, 3, b"newest"),
        ]);
        let mut o = opts(3);
        o.versions = 1;
        assert_eq!(collect(src.clone(), o), vec![c("r", "c", 30, b"newest")]);
        let mut r = Resolver::new(Merge::new(vec![src]), opts(3));
        r.seek(b"").unwrap();
        assert!(r.next_cell().unwrap().is_some());
        assert!(matches!(r.next_cell(), Err(Error::Merge(_))));
    }

    #[test]
    fn point_get_sees_markers_before_the_column() {
        let src = VecSource::new(vec![
            put("r", "q", 10, 1, b"a"),
            marker("r", 10, 2),
            put("r", "q", 11, 3, b"b"),
            put("r", "q", 9, 4, b"hidden"),
            put("r", "r", 50, 5, b"other-column"),
        ]);
        let mut r = Resolver::new(Merge::new(vec![src]), opts(5));
        r.seek_column(b"r", b"q").unwrap();
        let c = r.next_cell().unwrap().unwrap();
        assert_eq!(c.ts, 11);
        assert!(r.next_cell().unwrap().is_none());
        assert!(r.next_cell().unwrap().is_none());
        r.seek_column(b"r", b"zz").unwrap();
        assert!(r.next_cell().unwrap().is_none());
        r.seek_column(b"r", b"r").unwrap();
        assert_eq!(r.next_cell().unwrap().unwrap().ts, 50);
    }

    #[test]
    fn columns_per_row_and_max_versions() {
        let src = VecSource::new(vec![
            put("r", "a", 1, 1, b"1"),
            put("r", "a", 2, 2, b"2"),
            put("r", "b", 1, 1, b"x"),
            put("r", "c", 1, 1, b"y"),
            put("s", "a", 1, 1, b"s"),
        ]);
        let mut o = opts(2);
        o.max_versions = 1;
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
        let src = VecSource::new(vec![
            put("r", "a", 1, 1, b"yes"),
            put("r", "a", 2, 2, b"no"),
            put("r", "b", 1, 1, b"yes"),
        ]);
        let mut o = opts(2);
        o.value = Some(ValuePredicate::Equals(b"yes".to_vec()));
        assert_eq!(collect(src, o), vec![c("r", "b", 1, b"yes")]);
    }

    #[test]
    fn qualifier_filter_skips_columns() {
        let src = VecSource::new(vec![
            put("r", "a", 1, 1, b"1"),
            put("r", "meta:x", 1, 1, b"2"),
            put("r", "meta:y", 1, 1, b"3"),
            put("r", "z", 1, 1, b"4"),
            put("s", "meta:z", 1, 1, b"5"),
        ]);
        let mut o = opts(2);
        o.filter.qualifiers = pigeonhole_format::scan::QualifierFilter::Prefix(b"meta:".to_vec());
        assert_eq!(
            collect(src, o),
            vec![
                c("r", "meta:x", 1, b"2"),
                c("r", "meta:y", 1, b"3"),
                c("s", "meta:z", 1, b"5")
            ]
        );
    }

    #[test]
    fn merged_sources_interleave_in_key_order() {
        let a = VecSource::new(vec![
            put("r", "q", 20, 2, b"new"),
            put("s", "q", 1, 4, b"s"),
        ]);
        let b = VecSource::new(vec![
            put("r", "q", 10, 1, b"old"),
            put("r", "z", 5, 3, b"z"),
        ]);
        let mut r = Resolver::new(Merge::new(vec![a, b]), opts(4));
        r.seek(b"").unwrap();
        let mut out = Vec::new();
        while let Some(c) = r.next_cell().unwrap() {
            out.push(cell(c));
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
}
