//! MVCC resolution over an ordered cursor: the read path's half of the shared machinery.

use std::ops::Bound;
use std::sync::Arc;

use pigeonhole_format::key::{
    Kind, MARKER_QUALIFIER, SUFFIX_LEN, TERMINATOR, escape_into, row_prefix_len, split_suffix,
};
use pigeonhole_format::scan::ScanFilter;
use pigeonhole_format::value::{ValueRef, ValueTag, decode_value};
use pigeonhole_format::{Cursor, Seqno, Timestamp};

use crate::blob::{BlobFetch, blob_pointer};
use crate::merge::{MergeError, MergeOperator};

/// Values up to this size are copied out of the source while the resolver looks at the rest
/// of their `(column, timestamp)` group; larger ones are re-found with a seek so they can be
/// returned zero-copy.
const COPY_LIMIT: usize = 4096;

/// A predicate on a resolved value.
///
/// Byte predicates compare the value's payload (the stored value without its tag byte);
/// `I64` matches `i64` and varint values only. A separated value is tested on the value its
/// blob pointer names when [`ResolveOptions::blobs`] is set; [`ValuePredicate::matches`]
/// alone matches no byte predicate against a pointer.
#[derive(Debug, Clone, PartialEq)]
pub enum ValuePredicate {
    /// Value bytes equal.
    Equals(Vec<u8>),
    /// Value bytes start with.
    Prefix(Vec<u8>),
    /// Value bytes within the range.
    Range(Bound<Vec<u8>>, Bound<Vec<u8>>),
    /// The value is an `i64` and compares to the operand.
    I64(std::cmp::Ordering, i64),
}

impl ValuePredicate {
    /// Whether the stored value `stored` (tag byte included) matches.
    pub fn matches(&self, stored: &[u8]) -> bool {
        match self {
            Self::I64(ord, operand) => match decode_value(stored) {
                Ok(ValueRef::I64(v) | ValueRef::Varint(v)) => v.cmp(operand) == *ord,
                _ => false,
            },
            _ => {
                let payload = match stored.split_first() {
                    Some((&tag, rest)) if tag != ValueTag::Blob as u8 => rest,
                    _ => return false,
                };
                match self {
                    Self::Equals(v) => payload == v.as_slice(),
                    Self::Prefix(p) => payload.starts_with(p),
                    Self::Range(lo, hi) => {
                        let above = match lo {
                            Bound::Included(l) => payload >= l.as_slice(),
                            Bound::Excluded(l) => payload > l.as_slice(),
                            Bound::Unbounded => true,
                        };
                        let below = match hi {
                            Bound::Included(h) => payload <= h.as_slice(),
                            Bound::Excluded(h) => payload < h.as_slice(),
                            Bound::Unbounded => true,
                        };
                        above && below
                    }
                    Self::I64(..) => unreachable!("handled above"),
                }
            }
        }
    }
}

/// What the read path asks of the resolver.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ResolveOptions {
    /// Ignore entries with a newer seqno.
    pub snapshot: Seqno,
    /// Current time in microseconds, for TTL.
    pub now: Timestamp,
    /// Family TTL in microseconds (0 = none).
    pub ttl_micros: u64,
    /// Versions to return per column (1 = latest only; 0 = all retained). The caller folds
    /// the family's `max_versions` in (`min` of the two, 0 meaning unlimited), because reads
    /// must not depend on whether compaction has run yet.
    pub versions: u32,
    /// Columns to return per row (0 = unlimited); the rest of the row is skipped.
    pub columns_per_row: u32,
    /// Keep only cells whose resolved value matches. Tested on the newest visible version
    /// of each column (decision D22): if it matches, the column's versions are returned,
    /// otherwise none of them.
    pub value: Option<ValuePredicate>,
    /// The family's merge operator, if any.
    pub merge: Option<Arc<dyn MergeOperator>>,
    /// Keep only resolved versions with `min <= ts < max`. Applied after deletes, TTL and
    /// merge folding and before the value predicate and version limits, so for a family
    /// without merge operands it equals pushing the range down to puts (D22). Families with
    /// a merge operator must use this instead of `ScanFilter::time_range`, which could drop
    /// a counter's base while keeping its operands; [`ResolveOptions::route_time_range`]
    /// picks the right place.
    pub time_range: Option<(Timestamp, Timestamp)>,
    /// Reads separated values, so that a value predicate (and a merge base folded under
    /// operands) sees the value rather than its blob pointer. Without it a pointer matches
    /// no byte predicate.
    pub blobs: Option<Arc<dyn BlobFetch>>,
    /// Counter-family semantics (`FamilyKind::Counter`, decision D179): operands combine
    /// only within one `(column, timestamp)`, so every timestamp is its own version (no
    /// run folds across timestamps), and a delete hides only entries with a lower seqno
    /// within its timestamp scope (a later write at a covered timestamp stays visible).
    pub counter: bool,
}

impl ResolveOptions {
    /// Latest version only, no TTL, no limits, no predicate, no merge operator.
    pub fn new(snapshot: Seqno, now: Timestamp) -> Self {
        Self {
            snapshot,
            now,
            ttl_micros: 0,
            versions: 1,
            columns_per_row: 0,
            value: None,
            merge: None,
            time_range: None,
            blobs: None,
            counter: false,
        }
    }

    /// Puts a scan's time range where it belongs: pushed down into `filter` for a family
    /// without a merge operator, or onto resolved versions (`self.time_range`) for one with
    /// an operator (proposed D22 amendment). Set [`ResolveOptions::merge`] first.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use pigeonhole_compaction::{I64Add, ResolveOptions};
    /// use pigeonhole_format::scan::ScanFilter;
    ///
    /// let mut filter = ScanFilter::all();
    /// let mut counters = ResolveOptions::new(9, 0);
    /// counters.merge = Some(Arc::new(I64Add));
    /// counters.route_time_range(&mut filter, Some((10, 20)));
    /// assert_eq!((filter.time_range, counters.time_range), (None, Some((10, 20))));
    ///
    /// let mut plain = ResolveOptions::new(9, 0);
    /// plain.route_time_range(&mut filter, Some((10, 20)));
    /// assert_eq!((filter.time_range, plain.time_range), (Some((10, 20)), None));
    /// ```
    pub fn route_time_range(
        &mut self,
        filter: &mut ScanFilter,
        range: Option<(Timestamp, Timestamp)>,
    ) {
        if self.merge.is_some() {
            filter.time_range = None;
            self.time_range = range;
        } else {
            filter.time_range = range;
            self.time_range = None;
        }
    }

    fn in_time_range(&self, ts: Timestamp) -> bool {
        self.time_range
            .is_none_or(|(min, max)| min <= ts && ts < max)
    }
}

/// A cell as resolved: visible at the snapshot, not deleted, not expired, merges applied.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResolvedCell<'a> {
    /// The cell's internal key (row, qualifier, version); decode with
    /// `pigeonhole_format::decode_key`. For a merged version, the key of its newest operand.
    pub key: &'a [u8],
    /// Timestamp.
    pub ts: Timestamp,
    /// Stored value (tag byte included): borrowed from the source, or from the resolver's
    /// merge buffer.
    pub value: &'a [u8],
    /// Whether `value` borrows the current source entry (so the caller can pin it instead of
    /// copying) rather than the merge buffer.
    pub from_source: bool,
}

/// Where the cell being returned lives.
#[derive(Debug, Clone, Copy)]
enum Out {
    /// The cursor's current entry.
    Source(Timestamp),
    /// `out_key` / `out_val`.
    Buffer(Timestamp),
}

/// The newest put of the group being examined.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Base {
    None,
    /// Copied into `base_val`.
    Copied,
    /// Too large to copy: re-found by seeking to `base_key`.
    Large,
}

/// Applies MVCC visibility to an ordered cursor: snapshot seqno, cell/column/family deletes,
/// TTL, version limits, columns per row, value predicates and merge resolution. A lending
/// iterator; no allocation per cell except merge results (buffers are reused).
///
/// Semantics are those of `pigeonhole_sim::Model` (decisions D9, D34, D38, D41): a column or
/// family delete at `T` hides every version with timestamp `<= T`, a cell delete at `T`
/// every version at exactly `T`, whatever their seqnos; at one timestamp the newest put is the
/// base and newer operands fold onto it; a run of operands over several timestamps folds into
/// one version at its newest timestamp, consuming the next older put; deletes and TTL apply
/// before folding, version limits after.
///
/// Because a delete can follow the put it hides inside one `(column, timestamp)` group (it
/// was committed earlier), the resolver reads a whole group before returning its version.
/// Values up to 4 KiB are copied meanwhile (`from_source == false`); a larger value is
/// re-found with one seek and returned borrowed from the source (`from_source == true`).
///
/// ```
/// use pigeonhole_compaction::{CellResolver, ResolveOptions, VecCursor};
/// use pigeonhole_format::key::{Kind, encode_key, encode_marker_key};
///
/// let key = |q: &[u8], ts, seqno, kind| {
///     let mut k = Vec::new();
///     encode_key(&mut k, b"row", q, ts, seqno, kind).unwrap();
///     k
/// };
/// let mut marker = Vec::new();
/// encode_marker_key(&mut marker, b"row", 15, 3).unwrap(); // delete_family at ts 15
/// let src = VecCursor::new(vec![
///     (marker, vec![]),
///     (key(b"a", 10, 1, Kind::Put), b"\x00old".to_vec()),
///     (key(b"b", 20, 2, Kind::Put), b"\x00new".to_vec()),
/// ]);
///
/// let mut r = CellResolver::new(src, ResolveOptions::new(3, 100));
/// r.seek(b"").unwrap();
/// let cell = r.next_cell().unwrap().unwrap();
/// assert_eq!((cell.ts, cell.value), (20, &b"\x00new"[..])); // `a` is hidden by the marker
/// assert!(r.next_cell().unwrap().is_none());
///
/// // A snapshot before the delete still sees `a`.
/// let mut r = CellResolver::new(r.into_cursor(), ResolveOptions::new(2, 100));
/// r.seek_column(b"row", b"a").unwrap();
/// assert_eq!(r.next_cell().unwrap().unwrap().value, b"\x00old");
/// ```
#[derive(Debug)]
pub struct CellResolver<C> {
    cursor: C,
    opts: ResolveOptions,

    /// Row prefix (escaped row and terminator) of the current row; empty before the first.
    row: Vec<u8>,
    /// Newest timestamp a visible family marker of the row covers.
    family_cover: Option<Timestamp>,
    /// Counter families: `(ts, seqno)` of the row's visible family markers.
    markers: Vec<(Timestamp, Seqno)>,
    /// Columns of the row that returned a version.
    columns_in_row: u32,

    /// Column prefix of the current column; empty when none.
    col: Vec<u8>,
    /// Newest timestamp a visible column delete of the column covers.
    col_cover: Option<Timestamp>,
    /// Counter families: newest seqno of a visible column delete of the column seen so far
    /// (all at timestamps at or above the current group; 0 = none).
    col_cover_seqno: Seqno,
    /// Versions of the column returned so far.
    col_versions: u32,
    /// Skip the rest of the column (version limit reached or predicate failed).
    col_skip: bool,
    /// Entries of the column stepped over while skipping it; past [`SKIP_STEPS`] the
    /// resolver seeks past the column instead.
    col_skipped: u32,
    /// After `seek_column`: stop at the end of `col`.
    column_bound: bool,
    /// Every key of `col` is below `upper` (or there is none), so entries inside the column
    /// skip the bound check.
    col_below_upper: bool,
    /// Every key of `row` is below `upper` (or there is none): a new column of the row skips
    /// the bound check too.
    row_below_upper: bool,
    /// The cursor's current entry is known not to be in `col` (the group loop stopped on it):
    /// whether it is in `row`. Taken by the next step; cleared whenever the cursor moves.
    peek: Option<bool>,
    /// Where the cell last returned by `next_cell` lives, until the cursor moves again.
    last: Option<Out>,
    /// Stop at keys `>= upper`.
    upper: Option<Vec<u8>>,

    /// A pending run of merge operands (no base found yet).
    run: bool,
    run_ts: Timestamp,
    run_key: Vec<u8>,
    run_acc: Vec<u8>,
    run_err: Option<MergeError>,

    /// The cursor sits on a returned source entry; skip its group on the next call.
    skip_group: Option<Timestamp>,

    // Reused scratch.
    g_acc: Vec<u8>,
    g_key: Vec<u8>,
    base_key: Vec<u8>,
    base_val: Vec<u8>,
    out_key: Vec<u8>,
    out_val: Vec<u8>,
    past_col: Vec<u8>,
}

/// Entries of a skipped column the resolver steps over before it seeks past the column. A
/// column with many versions (a hot cell overwritten hundreds of times, all still in the
/// memtable) is then passed with a forward seek of the sources inside it (usually one)
/// instead of a step per version; a column with a few versions keeps the cheaper steps.
const SKIP_STEPS: u32 = 8;

/// Whether internal key `k` is in column `col` and in row `row` (`col` starts with `row`
/// when set), from one comparison with `col`.
fn locate_key(k: &[u8], col: &[u8], row: &[u8]) -> (bool, bool) {
    if col.is_empty() {
        let same_row = !row.is_empty() && common_prefix_len(k, row) == row.len();
        return (false, same_row);
    }
    let cp = common_prefix_len(k, col);
    let in_col = cp == col.len() && k.len() == col.len() + SUFFIX_LEN;
    let same_row = !row.is_empty() && cp >= row.len();
    (in_col, same_row)
}

/// The length of the longest common prefix of `a` and `b`, eight bytes at a time.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i + 8 <= n {
        let x = u64::from_le_bytes(a[i..i + 8].try_into().expect("8 bytes"));
        let y = u64::from_le_bytes(b[i..i + 8].try_into().expect("8 bytes"));
        if x != y {
            return i + ((x ^ y).trailing_zeros() / 8) as usize;
        }
        i += 8;
    }
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i
}

/// `max(a, b)` over optional timestamps.
fn raise(cover: &mut Option<Timestamp>, ts: Timestamp) {
    *cover = Some(cover.map_or(ts, |c| c.max(ts)));
}

impl<C: Cursor> CellResolver<C>
where
    C::Error: From<MergeError>,
{
    /// A resolver over `cursor`.
    pub fn new(cursor: C, options: ResolveOptions) -> Self {
        Self {
            cursor,
            opts: options,
            row: Vec::new(),
            family_cover: None,
            markers: Vec::new(),
            columns_in_row: 0,
            col: Vec::new(),
            col_cover: None,
            col_cover_seqno: 0,
            col_versions: 0,
            col_skip: false,
            col_skipped: 0,
            column_bound: false,
            col_below_upper: false,
            row_below_upper: false,
            peek: None,
            last: None,
            upper: None,
            run: false,
            run_ts: 0,
            run_key: Vec::new(),
            run_acc: Vec::new(),
            run_err: None,
            skip_group: None,
            g_acc: Vec::new(),
            g_key: Vec::new(),
            base_key: Vec::new(),
            base_val: Vec::new(),
            out_key: Vec::new(),
            out_val: Vec::new(),
            past_col: Vec::new(),
        }
    }

    /// Stops the resolver at the first key `>= end` (the end of a scan range, normally an
    /// encoded row prefix), so a scan over deleted rows does not run past its range. `None`
    /// removes the bound. Kept across seeks.
    pub fn set_upper_bound(&mut self, end: Option<&[u8]>) {
        match end {
            Some(e) => {
                let u = self.upper.get_or_insert_with(Vec::new);
                u.clear();
                u.extend_from_slice(e);
            }
            None => self.upper = None,
        }
        self.col_below_upper = false;
        self.row_below_upper = false;
    }

    /// Records whether every key of the column `col` sorts below `upper`. Exact unless
    /// `upper` starts with `col` (a bound inside the column), which is then checked per entry:
    /// otherwise a key `col + suffix` compares with `upper` as `col` does.
    fn note_column_bound(&mut self) {
        self.col_below_upper = self.row_below_upper
            || match self.upper.as_deref() {
                None => true,
                Some(u) => !u.starts_with(&self.col) && self.col.as_slice() < u,
            };
    }

    /// Records whether every key of the row `row` sorts below `upper`, as for a column.
    fn note_row_bound(&mut self) {
        self.row_below_upper = match self.upper.as_deref() {
            None => true,
            Some(u) => !u.starts_with(&self.row) && self.row.as_slice() < u,
        };
    }

    fn reset(&mut self) {
        self.row.clear();
        self.family_cover = None;
        self.markers.clear();
        self.columns_in_row = 0;
        self.reset_column();
        self.col.clear();
        self.column_bound = false;
        self.col_below_upper = false;
        self.row_below_upper = false;
        self.peek = None;
        self.skip_group = None;
        self.last = None;
    }

    fn reset_column(&mut self) {
        self.col_cover = None;
        self.col_cover_seqno = 0;
        self.col_versions = 0;
        self.col_skip = false;
        self.col_skipped = 0;
        self.run = false;
        self.run_err = None;
    }

    /// Positions at the first entry `>= key` (an encoded row prefix, for scans). Markers of
    /// a row are met before its cells, so a row-ordered walk sees every delete it needs. A
    /// `key` inside a row would miss that row's markers; use [`CellResolver::seek_column`]
    /// for point reads.
    pub fn seek(&mut self, key: &[u8]) -> Result<(), C::Error> {
        self.reset();
        self.cursor.seek(key)
    }

    /// Positions for a point read of one column: first seeks the merged cursor to the row's
    /// marker prefix and records any family markers visible at the snapshot, then seeks to
    /// the column. Costs a second seek per source (usually inside the block the first seek
    /// already loaded); the row filter has already excluded SSTs without the row.
    ///
    /// Afterwards [`CellResolver::next_cell`] returns only versions of that column.
    pub fn seek_column(&mut self, row: &[u8], qualifier: &[u8]) -> Result<(), C::Error> {
        self.reset();
        // Row prefix, then the marker prefix in the column scratch.
        escape_into(&mut self.row, row);
        self.row.extend_from_slice(&TERMINATOR);
        self.col.extend_from_slice(&self.row);
        self.col.extend_from_slice(&MARKER_QUALIFIER);
        self.cursor.seek(&self.col)?;
        while self.cursor.valid() && self.cursor.key().starts_with(&self.col) {
            if let Ok((_, ts, seqno, Kind::FamilyDelete)) = split_suffix(self.cursor.key())
                && seqno <= self.opts.snapshot
            {
                raise(&mut self.family_cover, ts);
                if self.opts.counter {
                    self.markers.push((ts, seqno));
                }
            }
            self.cursor.next()?;
        }
        self.col.truncate(self.row.len());
        escape_into(&mut self.col, qualifier);
        self.col.extend_from_slice(&TERMINATOR);
        self.column_bound = true;
        self.note_row_bound();
        self.note_column_bound();
        self.cursor.seek(&self.col)
    }

    /// The next visible cell, or `None` at the end of the cursor (or of the column after
    /// [`CellResolver::seek_column`], or at the upper bound).
    ///
    /// Fails with the cursor's error, or with a [`MergeError`] when a version that would be
    /// returned folds operands onto a base the operator rejects (D41).
    pub fn next_cell(&mut self) -> Result<Option<ResolvedCell<'_>>, C::Error> {
        self.last = None;
        self.last = self.advance()?;
        Ok(self.current())
    }

    /// The cell [`CellResolver::next_cell`] last returned, again: valid until the resolver
    /// next moves (`next_cell`, a seek, `skip_row`), so a caller can hold it without copying.
    pub fn current(&self) -> Option<ResolvedCell<'_>> {
        Some(match self.last? {
            Out::Source(ts) => ResolvedCell {
                key: self.cursor.key(),
                ts,
                value: self.cursor.value(),
                from_source: true,
            },
            Out::Buffer(ts) => ResolvedCell {
                key: &self.out_key,
                ts,
                value: &self.out_val,
                from_source: false,
            },
        })
    }

    /// Skips the rest of the current row.
    pub fn skip_row(&mut self) -> Result<(), C::Error> {
        self.last = None;
        self.skip_group = None;
        self.peek = None;
        self.run = false;
        self.col_skip = true;
        if self.cursor.valid() && !self.row.is_empty() && self.cursor.key().starts_with(&self.row) {
            self.cursor.skip_row()?;
        }
        Ok(())
    }

    /// The underlying cursor (to pin the current block for a zero-copy value).
    pub fn cursor(&self) -> &C {
        &self.cursor
    }

    /// Gives the cursor back.
    pub fn into_cursor(self) -> C {
        self.cursor
    }

    /// As [`CellResolver::new`], starting from `buffers`' allocations: the state is that of a
    /// new resolver.
    pub fn reuse(cursor: C, options: ResolveOptions, buffers: ResolverBuffers) -> Self {
        let mut r = Self::new(cursor, options);
        let ResolverBuffers {
            row,
            markers,
            col,
            run_key,
            run_acc,
            g_acc,
            g_key,
            base_key,
            base_val,
            out_key,
            out_val,
            past_col,
        } = buffers;
        // Cleared when they were taken out (`into_parts`), as `new`'s are empty.
        r.row = row;
        r.markers = markers;
        r.col = col;
        r.run_key = run_key;
        r.run_acc = run_acc;
        r.g_acc = g_acc;
        r.g_key = g_key;
        r.base_key = base_key;
        r.base_val = base_val;
        r.out_key = out_key;
        r.out_val = out_val;
        r.past_col = past_col;
        r
    }

    /// The cursor, and the resolver's allocations cleared for the next
    /// [`CellResolver::reuse`].
    pub fn into_parts(self) -> (C, ResolverBuffers) {
        let mut buffers = ResolverBuffers {
            row: self.row,
            markers: self.markers,
            col: self.col,
            run_key: self.run_key,
            run_acc: self.run_acc,
            g_acc: self.g_acc,
            g_key: self.g_key,
            base_key: self.base_key,
            base_val: self.base_val,
            out_key: self.out_key,
            out_val: self.out_val,
            past_col: self.past_col,
        };
        buffers.clear();
        (self.cursor, buffers)
    }

    fn expired(&self, ts: Timestamp) -> bool {
        self.opts.ttl_micros != 0 && ts.saturating_add(self.opts.ttl_micros) <= self.opts.now
    }

    fn covered(&self, ts: Timestamp) -> bool {
        self.family_cover.is_some_and(|c| ts <= c) || self.col_cover.is_some_and(|c| ts <= c)
    }

    /// Counter families: entries at `ts` with a seqno below this are hidden by a column
    /// delete or family marker (0 = none).
    fn cover_seqno(&self, ts: Timestamp) -> Seqno {
        self.markers
            .iter()
            .filter(|m| m.0 >= ts)
            .map(|m| m.1)
            .fold(self.col_cover_seqno, Seqno::max)
    }

    /// Whether the cursor's current key is in the current column.
    fn in_column(&self) -> bool {
        let k = self.cursor.key();
        !self.col.is_empty() && k.len() == self.col.len() + SUFFIX_LEN && k.starts_with(&self.col)
    }

    /// Whether the cursor's entry is past the column bound or the upper bound; `in_col` and
    /// `same_row` are [`Self::locate`]'s for it.
    fn past_bounds(&self, in_col: bool, same_row: bool) -> bool {
        if self.column_bound && !in_col {
            return true;
        }
        if (in_col && self.col_below_upper) || (same_row && self.row_below_upper) {
            return false;
        }
        self.upper
            .as_deref()
            .is_some_and(|u| self.cursor.key() >= u)
    }

    fn advance(&mut self) -> Result<Option<Out>, C::Error> {
        if let Some(ts) = self.skip_group.take() {
            self.skip_rest_of_group(ts)?;
        }
        loop {
            // One key fetch per step, and one comparison of the key with the column (or none:
            // the group loop already made it for this entry).
            let peeked = self.peek.take();
            let k = if self.cursor.valid() {
                Some(self.cursor.key())
            } else {
                None
            };
            let (in_col, same_row) = match (k, peeked) {
                (None, _) => (false, false),
                (Some(_), Some(same_row)) => (false, same_row),
                (Some(k), None) => locate_key(k, &self.col, &self.row),
            };
            let k = match k {
                Some(k) if !self.past_bounds(in_col, same_row) => k,
                _ => {
                    if self.run {
                        if let Some(out) = self.flush_run()? {
                            return Ok(Some(out));
                        }
                        continue;
                    }
                    return Ok(None);
                }
            };
            let Ok((_, ts, seqno, kind)) = split_suffix(k) else {
                debug_assert!(false, "malformed internal key from a source");
                self.cursor.next()?;
                continue;
            };
            if !in_col {
                // The column ends: a pending run is its last version.
                if self.run {
                    if let Some(out) = self.flush_run()? {
                        return Ok(Some(out));
                    }
                    continue;
                }
                // The new row and column are copied from the same key fetch.
                if !same_row {
                    let Ok(n) = row_prefix_len(k) else {
                        debug_assert!(false, "malformed internal key from a source");
                        self.cursor.next()?;
                        continue;
                    };
                    self.row.clear();
                    self.row.extend_from_slice(&k[..n]);
                }
                self.col.clear();
                if kind != Kind::FamilyDelete {
                    self.col.extend_from_slice(&k[..k.len() - SUFFIX_LEN]);
                }
                if !same_row {
                    self.family_cover = None;
                    self.markers.clear();
                    self.columns_in_row = 0;
                    self.note_row_bound();
                }
                if kind == Kind::FamilyDelete {
                    if seqno <= self.opts.snapshot {
                        raise(&mut self.family_cover, ts);
                        if self.opts.counter {
                            self.markers.push((ts, seqno));
                        }
                    }
                    self.cursor.next()?;
                    continue;
                }
                self.reset_column();
                self.note_column_bound();
                let limit = self.opts.columns_per_row;
                if limit != 0 && self.columns_in_row >= limit {
                    self.col.clear();
                    self.cursor.skip_row()?;
                    continue;
                }
            }
            if self.col_skip {
                self.col_skipped += 1;
                if self.col_skipped > SKIP_STEPS {
                    // The column prefix ends with the terminator `00 01`: bumping its last
                    // byte gives the smallest key past every version of the column, and
                    // before the next column (whose qualifier continues past this one's
                    // with a byte above `00 02`, or differs earlier).
                    self.past_col.clear();
                    self.past_col.extend_from_slice(&self.col);
                    if let Some(last) = self.past_col.last_mut() {
                        *last += 1;
                    }
                    self.cursor.seek_forward(&self.past_col)?;
                } else {
                    self.cursor.next()?;
                }
                continue;
            }
            if let Some(out) = self.group(ts, seqno, kind)? {
                return Ok(Some(out));
            }
        }
    }

    /// Skips the remaining entries of the current column at timestamp `ts`.
    fn skip_rest_of_group(&mut self, ts: Timestamp) -> Result<(), C::Error> {
        while self.cursor.valid() && self.in_column() {
            match split_suffix(self.cursor.key()) {
                Ok((_, t, _, _)) if t == ts => self.cursor.next()?,
                _ => break,
            }
        }
        Ok(())
    }

    /// Reads the whole `(column, ts)` group the cursor is on and returns its version, if it
    /// produces one now (a group of operands only extends the pending run instead).
    fn group(&mut self, ts: Timestamp, seqno: Seqno, kind: Kind) -> Result<Option<Out>, C::Error> {
        let snapshot = self.opts.snapshot;
        let counter = self.opts.counter;
        let expired = self.expired(ts);
        // Timestamp deletes hide the whole group whatever the order. In a counter family a
        // delete hides only older entries: those below `floor`, and those after it in the
        // group (seqno descending), which `hidden` then skips.
        let mut hidden = expired || (!counter && self.covered(ts));
        let floor = if counter { self.cover_seqno(ts) } else { 0 };
        let mut base = Base::None;
        let mut ops = false;
        let mut ops_err: Option<MergeError> = None;
        // Copy the base whenever it will be folded (operands above it), not only when small.
        let fold_ready = self.run;
        // The first entry is in the column (the caller checked); the loop stops on the first
        // that is not, and leaves what it found for the next step.
        let mut first = Some((seqno, kind));
        while self.cursor.valid() {
            let (seqno, kind) = match first.take() {
                Some(sk) => sk,
                None => {
                    // One key fetch: the column check and the suffix.
                    let k = self.cursor.key();
                    let (in_col, same_row) = locate_key(k, &self.col, &self.row);
                    if !in_col {
                        self.peek = Some(same_row);
                        break;
                    }
                    let Ok((_, t, seqno, kind)) = split_suffix(k) else {
                        break;
                    };
                    if t != ts {
                        break;
                    }
                    (seqno, kind)
                }
            };
            if seqno <= snapshot && seqno >= floor {
                match kind {
                    Kind::ColumnDelete => {
                        raise(&mut self.col_cover, ts);
                        self.col_cover_seqno = self.col_cover_seqno.max(seqno);
                        hidden = true;
                    }
                    Kind::CellDelete => hidden = true,
                    Kind::Put if !hidden && base == Base::None => {
                        let value = self.cursor.value();
                        self.base_key.clear();
                        self.base_key.extend_from_slice(self.cursor.key());
                        if value.len() <= COPY_LIMIT || ops || fold_ready {
                            self.base_val.clear();
                            self.base_val.extend_from_slice(value);
                            base = Base::Copied;
                        } else {
                            base = Base::Large;
                        }
                    }
                    Kind::Merge if !hidden && base == Base::None => {
                        let value = self.cursor.value();
                        if !ops {
                            ops = true;
                            self.g_acc.clear();
                            self.g_acc.extend_from_slice(value);
                            self.g_key.clear();
                            self.g_key.extend_from_slice(self.cursor.key());
                        } else if ops_err.is_none() {
                            ops_err = match &self.opts.merge {
                                Some(op) => op.merge(&mut self.g_acc, value).err(),
                                None => Some(MergeError::no_operator()),
                            };
                        }
                    }
                    _ => {}
                }
            }
            self.cursor.next()?;
        }
        if (if counter { expired } else { hidden }) || (base == Base::None && !ops) {
            return Ok(None);
        }

        if self.run {
            if ops && self.run_err.is_none() {
                self.run_err = ops_err.or_else(|| match &self.opts.merge {
                    Some(op) => op.merge(&mut self.run_acc, &self.g_acc).err(),
                    None => Some(MergeError::no_operator()),
                });
            }
            if base == Base::None {
                return Ok(None);
            }
            // The run ends on this base.
            self.run = false;
            self.load_base();
            let err = self.run_err.take().or_else(|| match &self.opts.merge {
                Some(op) => op.finish(Some(&self.base_val), &mut self.run_acc).err(),
                None => Some(MergeError::no_operator()),
            });
            std::mem::swap(&mut self.out_key, &mut self.run_key);
            std::mem::swap(&mut self.out_val, &mut self.run_acc);
            return self.emit_buffer(self.run_ts, err);
        }

        match (base, ops) {
            (Base::Copied, false) => {
                std::mem::swap(&mut self.out_key, &mut self.base_key);
                std::mem::swap(&mut self.out_val, &mut self.base_val);
                self.emit_buffer(ts, None)
            }
            (Base::Large, false) => {
                if !self.opts.in_time_range(ts) {
                    return Ok(None);
                }
                self.cursor.seek(&self.base_key)?;
                self.peek = None;
                debug_assert!(self.cursor.valid() && self.cursor.key() == self.base_key);
                if !self.admit_source() {
                    return Ok(None);
                }
                self.skip_group = Some(ts);
                Ok(Some(Out::Source(ts)))
            }
            (_, true) if base != Base::None => {
                self.load_base();
                let err = ops_err.or_else(|| match &self.opts.merge {
                    Some(op) => op.finish(Some(&self.base_val), &mut self.g_acc).err(),
                    None => Some(MergeError::no_operator()),
                });
                std::mem::swap(&mut self.out_key, &mut self.g_key);
                std::mem::swap(&mut self.out_val, &mut self.g_acc);
                self.emit_buffer(ts, err)
            }
            _ if counter => {
                // Operands only, in a counter family: the bucket's version is their sum.
                let err = ops_err.or_else(|| match &self.opts.merge {
                    Some(op) => op.finish(None, &mut self.g_acc).err(),
                    None => Some(MergeError::no_operator()),
                });
                std::mem::swap(&mut self.out_key, &mut self.g_key);
                std::mem::swap(&mut self.out_val, &mut self.g_acc);
                self.emit_buffer(ts, err)
            }
            _ => {
                // Operands only: start a run that later timestamps may extend.
                self.run = true;
                self.run_ts = ts;
                self.run_err = ops_err;
                std::mem::swap(&mut self.run_key, &mut self.g_key);
                std::mem::swap(&mut self.run_acc, &mut self.g_acc);
                Ok(None)
            }
        }
    }

    /// Emits the pending run with no base below it.
    fn flush_run(&mut self) -> Result<Option<Out>, C::Error> {
        self.run = false;
        let err = self.run_err.take().or_else(|| match &self.opts.merge {
            Some(op) => op.finish(None, &mut self.run_acc).err(),
            None => Some(MergeError::no_operator()),
        });
        std::mem::swap(&mut self.out_key, &mut self.run_key);
        std::mem::swap(&mut self.out_val, &mut self.run_acc);
        self.emit_buffer(self.run_ts, err)
    }

    /// Counts a version about to be returned from the buffers, applying the predicate and
    /// limits; a failed fold is an error only now, when the version would be returned.
    fn emit_buffer(
        &mut self,
        ts: Timestamp,
        err: Option<MergeError>,
    ) -> Result<Option<Out>, C::Error> {
        if !self.opts.in_time_range(ts) {
            return Ok(None);
        }
        if let Some(e) = err {
            return Err(e.into());
        }
        let first_ok = self.col_versions != 0 || self.predicate_ok(&self.out_val);
        Ok(self.count_version(first_ok).then_some(Out::Buffer(ts)))
    }

    fn admit_source(&mut self) -> bool {
        let first_ok = self.col_versions != 0 || self.predicate_ok(self.cursor.value());
        self.count_version(first_ok)
    }

    /// Whether `stored` passes the value predicate, reading it from its blob file first if
    /// it is separated (and the options can).
    fn predicate_ok(&self, stored: &[u8]) -> bool {
        let Some(p) = &self.opts.value else {
            return true;
        };
        match (&self.opts.blobs, blob_pointer(stored)) {
            (Some(blobs), Some(ptr)) => blobs.fetch(&ptr).is_some_and(|v| p.matches(&v)),
            _ => p.matches(stored),
        }
    }

    /// Replaces a separated merge base (copied into `base_val`) by its value, so the
    /// operator folds onto the value rather than its pointer.
    fn load_base(&mut self) {
        if let (Some(blobs), Some(ptr)) = (&self.opts.blobs, blob_pointer(&self.base_val))
            && let Some(v) = blobs.fetch(&ptr)
        {
            self.base_val.clear();
            self.base_val.extend_from_slice(&v);
        }
    }

    fn count_version(&mut self, first_ok: bool) -> bool {
        if !first_ok {
            self.col_skip = true;
            return false;
        }
        if self.col_versions == 0 {
            self.columns_in_row += 1;
        }
        self.col_versions += 1;
        if self.opts.versions != 0 && self.col_versions >= self.opts.versions {
            self.col_skip = true;
        }
        true
    }
}

/// The allocations a [`CellResolver`] keeps between uses: its key, value and marker scratch,
/// each cleared ([`CellResolver::reuse`], [`CellResolver::into_parts`]). A caller that
/// resolves many times (a point get per call) keeps one and allocates nothing for it.
#[derive(Debug, Default)]
pub struct ResolverBuffers {
    row: Vec<u8>,
    markers: Vec<(Timestamp, Seqno)>,
    col: Vec<u8>,
    run_key: Vec<u8>,
    run_acc: Vec<u8>,
    g_acc: Vec<u8>,
    g_key: Vec<u8>,
    base_key: Vec<u8>,
    base_val: Vec<u8>,
    out_key: Vec<u8>,
    out_val: Vec<u8>,
    past_col: Vec<u8>,
}

impl ResolverBuffers {
    fn clear(&mut self) {
        self.row.clear();
        self.markers.clear();
        self.col.clear();
        self.run_key.clear();
        self.run_acc.clear();
        self.g_acc.clear();
        self.g_key.clear();
        self.base_key.clear();
        self.base_val.clear();
        self.out_key.clear();
        self.out_val.clear();
        self.past_col.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::common_prefix_len;

    #[test]
    fn common_prefix_len_finds_the_first_difference() {
        let a: Vec<u8> = (0..40).collect();
        for at in 0..40 {
            let mut b = a.clone();
            b[at] ^= 0x80;
            assert_eq!(common_prefix_len(&a, &b), at, "differ at {at}");
        }
        assert_eq!(common_prefix_len(&a, &a), 40);
        assert_eq!(common_prefix_len(&a[..13], &a), 13);
        assert_eq!(common_prefix_len(&a, &a[..21]), 21);
        assert_eq!(common_prefix_len(&[], &a), 0);
    }
}
