//! Compaction's garbage collection: which input entries the output must keep so that no read
//! at any live snapshot changes.
//!
//! Read points are the live snapshots plus "latest" (`u64::MAX`). An entry with seqno `q` is
//! visible at every read point `>= q`; its *stripe* is the index of the first such point. Two
//! entries in one stripe are seen by exactly the same snapshots. Per `(column, timestamp)`
//! group (read whole, since a delete may follow the put it hides) an entry is dropped when:
//!
//! - its timestamp has expired (`ts + ttl <= now`): everything it could affect expired too;
//! - it is a put or operand hidden by a delete at every read point that sees it (the delete's
//!   stripe is not above its own), or shadowed by a newer put at the same timestamp in its
//!   stripe;
//! - it is a delete made redundant by another delete covering at least its scope at every
//!   read point that sees it;
//! - at the bottommost level only, and only for timestamps below `min_ts_above` (so nothing
//!   newer above the inputs can be uncovered or have its versions recounted): a delete
//!   visible at every read point (nothing below can need it, and everything it hides here is
//!   dropped by the rule above), or a put or operand outside the newest `max_versions`
//!   versions at every read point that sees it.
//!
//! Consecutive kept operands of one group and stripe are combined into one operand.
//!
//! Operands are folded across timestamps (issue #34) only where no read point can tell: at
//! the bottommost level, in a family with a merge operator and no TTL, in a column whose
//! input entries are all below `min_ts_above`, and only in the column's *plain prefix*: the
//! kept entries, newest first, up to the first that is not a put or operand visible at every
//! read point (stripe 0) and covered by no delete or marker at any of them. Each run of
//! operands there becomes one operand at the newest one's key, or, folded onto the put right
//! below it, one put there. A base the operator refuses or a blob base stays unfolded (#21),
//! and an operand the operator refuses ends folding for the column, so a read that failed
//! still fails. The run is streamed (one accumulator), never buffered.
//! `pigeonhole_sim::Model::purge` applies the same rule, so the model oracle stays strict.

use std::sync::Arc;

use pigeonhole_format::key::{Kind, SUFFIX_LEN, row_prefix_len, split_suffix};
use pigeonhole_format::value::{BlobPointer, ValueTag};
use pigeonhole_format::{BlobFileId, Cursor, Seqno, Timestamp};

use crate::MergeOperator;

/// No stripe (no delete seen).
const NONE: usize = usize::MAX;

/// Bytes a blob record occupies in its blob file: the 16-byte record header and the value.
const BLOB_RECORD_HEADER: i64 = 16;

#[derive(Debug, Clone, Copy)]
struct GEntry {
    key_start: usize,
    val_start: usize,
    end: usize,
    kind: Kind,
    stripe: usize,
}

/// Records that a dropped entry's blob value is no longer referenced from this output.
fn account_drop(delta: &mut Vec<(BlobFileId, i64)>, kind: Kind, value: &[u8]) {
    if kind != Kind::Put {
        return;
    }
    if let [tag, ptr @ ..] = value
        && *tag == ValueTag::Blob as u8
        && let Ok(p) = BlobPointer::decode(ptr)
    {
        let bytes = BLOB_RECORD_HEADER + i64::from(p.len);
        match delta.iter_mut().find(|d| d.0 == p.blob_file) {
            Some(d) => d.1 -= bytes,
            None => delta.push((p.blob_file, -bytes)),
        }
    }
}

/// Kept entries, appended in key order.
#[derive(Debug, Default)]
pub(crate) struct OutBuf {
    data: Vec<u8>,
    items: Vec<(usize, usize, usize)>,
}

impl OutBuf {
    fn push(&mut self, key: &[u8], value: &[u8]) {
        let k = self.data.len();
        self.data.extend_from_slice(key);
        let v = self.data.len();
        self.data.extend_from_slice(value);
        self.items.push((k, v, self.data.len()));
    }

    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn get(&self, i: usize) -> (&[u8], &[u8]) {
        let (k, v, e) = self.items[i];
        (&self.data[k..v], &self.data[v..e])
    }

    pub(crate) fn clear(&mut self) {
        self.data.clear();
        self.items.clear();
    }
}

/// What GC needs to know.
#[derive(Debug, Clone)]
pub(crate) struct GcConfig {
    /// Live snapshots, ascending.
    pub snapshots: Vec<Seqno>,
    pub now: Timestamp,
    pub bottommost: bool,
    pub min_ts_above: Timestamp,
    pub ttl_micros: u64,
    pub max_versions: u32,
    pub merge: Option<Arc<dyn MergeOperator>>,
}

/// Streaming GC state over one ordered input.
#[derive(Debug)]
pub(crate) struct Gc {
    /// Read points: snapshots ascending, then `u64::MAX`.
    points: Vec<Seqno>,
    now: Timestamp,
    ttl: u64,
    bottommost: bool,
    min_ts_above: Timestamp,
    max_versions: u32,
    merge: Option<Arc<dyn MergeOperator>>,

    row: Vec<u8>,
    /// `(ts, stripe)` of the row's family markers, newest first.
    markers: Vec<(Timestamp, usize)>,
    col: Vec<u8>,
    /// Lowest stripe of the column's column deletes seen so far (all at newer timestamps).
    col_stripe: usize,
    /// Timestamp of the column's newest group.
    col_newest: Timestamp,
    /// Per read point: versions started, and whether an operand run is open.
    counts: Vec<u32>,
    run_open: Vec<bool>,

    group_data: Vec<u8>,
    group: Vec<GEntry>,
    /// Per read point, scratch: group version index (0 = none) and base position.
    gidx: Vec<u32>,
    gbase: Vec<usize>,
    acc: Vec<u8>,
    acc_key: Vec<u8>,
    scratch: Vec<u8>,

    /// Whether the current column is still in its plain prefix (may fold), the open run's
    /// newest key and accumulator, and the last group's hiding stripe.
    fold_col: bool,
    fold_open: bool,
    fold_key: Vec<u8>,
    fold_acc: Vec<u8>,
    group_hide: usize,
    /// One group's kept entries, before folding.
    gbuf: OutBuf,

    /// Live-byte change per blob file from dropped values.
    pub(crate) blob_delta: Vec<(BlobFileId, i64)>,
    /// Entries read and kept (statistics).
    pub(crate) read: u64,
    pub(crate) kept: u64,
}

impl Gc {
    pub(crate) fn new(config: GcConfig) -> Self {
        let mut points = config.snapshots;
        points.sort_unstable();
        points.dedup();
        if points.last() != Some(&Seqno::MAX) {
            points.push(Seqno::MAX);
        }
        let n = points.len();
        Self {
            points,
            now: config.now,
            ttl: config.ttl_micros,
            bottommost: config.bottommost,
            min_ts_above: config.min_ts_above,
            max_versions: config.max_versions,
            merge: config.merge,
            row: Vec::new(),
            markers: Vec::new(),
            col: Vec::new(),
            col_stripe: NONE,
            col_newest: 0,
            counts: vec![0; n],
            run_open: vec![false; n],
            group_data: Vec::new(),
            group: Vec::new(),
            gidx: vec![0; n],
            gbase: vec![0; n],
            acc: Vec::new(),
            acc_key: Vec::new(),
            scratch: Vec::new(),
            fold_col: false,
            fold_open: false,
            fold_key: Vec::new(),
            fold_acc: Vec::new(),
            group_hide: NONE,
            gbuf: OutBuf::default(),
            blob_delta: Vec::new(),
            read: 0,
            kept: 0,
        }
    }

    /// Forgets the current row (at a range boundary).
    pub(crate) fn reset(&mut self) {
        self.row.clear();
        self.markers.clear();
        self.col.clear();
    }

    /// Writes the open run's folded operand, if any.
    fn flush_run(&mut self, out: &mut OutBuf) {
        if self.fold_open {
            out.push(&self.fold_key, &self.fold_acc);
            self.fold_open = false;
        }
    }

    /// Ends the current column (or a stretch the fold must not cross).
    fn end_column(&mut self, out: &mut OutBuf) {
        self.flush_run(out);
        self.fold_col = false;
    }

    /// Passes one group's kept entries (`gbuf`) to `out`, folding them into the open run
    /// while the column is in its plain prefix.
    fn fold_group(&mut self, out: &mut OutBuf) {
        let gbuf = std::mem::take(&mut self.gbuf);
        for i in 0..gbuf.len() {
            let (key, value) = gbuf.get(i);
            let Ok((_, _, seqno, kind)) = split_suffix(key) else {
                self.end_column(out);
                out.push(key, value);
                continue;
            };
            let plain = self.fold_col
                && self.group_hide == NONE
                && self.stripe(seqno) == 0
                && matches!(kind, Kind::Put | Kind::Merge);
            let op = match (&self.merge, plain) {
                (Some(op), true) => Arc::clone(op),
                _ => {
                    self.end_column(out);
                    out.push(key, value);
                    continue;
                }
            };
            if kind == Kind::Merge {
                if !self.fold_open {
                    self.fold_open = true;
                    self.fold_key.clear();
                    self.fold_key.extend_from_slice(key);
                    self.fold_acc.clear();
                    self.fold_acc.extend_from_slice(value);
                    continue;
                }
                self.scratch.clear();
                self.scratch.extend_from_slice(&self.fold_acc);
                if op.merge(&mut self.scratch, value).is_ok() {
                    std::mem::swap(&mut self.fold_acc, &mut self.scratch);
                    self.kept -= 1;
                } else {
                    // The read fails here: fold nothing below it, so it still does.
                    self.end_column(out);
                    out.push(key, value);
                }
                continue;
            }
            // A put: the open run's base, unless it is a blob or the operator refuses it.
            let is_blob = value.first() == Some(&(ValueTag::Blob as u8));
            if self.fold_open && !is_blob {
                self.scratch.clear();
                self.scratch.extend_from_slice(&self.fold_acc);
                if op.finish(Some(value), &mut self.scratch).is_ok() {
                    if let Some(k) = self.fold_key.last_mut() {
                        *k = Kind::Put as u8;
                    }
                    out.push(&self.fold_key, &self.scratch);
                    self.fold_open = false;
                    self.kept -= 1;
                    continue;
                }
            }
            self.flush_run(out);
            out.push(key, value);
        }
        self.gbuf = gbuf;
        self.gbuf.clear();
    }

    fn stripe(&self, seqno: Seqno) -> usize {
        self.points.partition_point(|&p| p < seqno)
    }

    /// Whether a bottommost purge may touch timestamp `ts`.
    fn purgeable(&self, ts: Timestamp) -> bool {
        self.bottommost && ts < self.min_ts_above
    }

    fn expired(&self, ts: Timestamp) -> bool {
        self.ttl != 0 && ts.saturating_add(self.ttl) <= self.now
    }

    /// Lowest stripe of the row's markers covering timestamp `ts`.
    fn marker_stripe(&self, ts: Timestamp) -> usize {
        self.markers
            .iter()
            .filter(|m| m.0 >= ts)
            .map(|m| m.1)
            .min()
            .unwrap_or(NONE)
    }

    /// Processes the next unit at the cursor (one family marker or one `(column, ts)` group),
    /// appending kept entries to `out`. Returns false at the end of the input or at `end`.
    pub(crate) fn step<C: Cursor>(
        &mut self,
        cursor: &mut C,
        end: Option<&[u8]>,
        out: &mut OutBuf,
    ) -> Result<bool, C::Error> {
        if !cursor.valid() || end.is_some_and(|e| cursor.key() >= e) {
            self.end_column(out);
            return Ok(false);
        }
        let key = cursor.key();
        let Ok((body, ts, seqno, kind)) = split_suffix(key) else {
            // A malformed key cannot be interpreted; keep it as is, after any open run.
            self.end_column(out);
            out.push(key, cursor.value());
            cursor.next()?;
            return Ok(true);
        };
        if self.row.is_empty() || !key.starts_with(&self.row) {
            self.end_column(out);
            let n = row_prefix_len(key).unwrap_or(body.len());
            self.row.clear();
            self.row.extend_from_slice(&key[..n]);
            self.markers.clear();
            self.col.clear();
        }
        self.read += 1;
        if kind == Kind::FamilyDelete {
            let stripe = self.stripe(seqno);
            let redundant = self.markers.iter().any(|m| m.1 <= stripe);
            let keep = !self.expired(ts) && !redundant && !(self.purgeable(ts) && stripe == 0);
            self.markers.push((ts, stripe));
            if keep {
                out.push(key, cursor.value());
                self.kept += 1;
            }
            cursor.next()?;
            return Ok(true);
        }
        if body != self.col.as_slice() {
            self.end_column(out);
            self.fold_col = self.merge.is_some() && self.ttl == 0 && self.purgeable(ts);
            self.col.clear();
            self.col.extend_from_slice(body);
            self.col_stripe = NONE;
            self.col_newest = ts;
            self.counts.fill(0);
            self.run_open.fill(false);
        }
        self.read_group(cursor, ts)?;
        if self.fold_col {
            let mut gbuf = std::mem::take(&mut self.gbuf);
            self.decide(ts, &mut gbuf);
            self.gbuf = gbuf;
            self.fold_group(out);
        } else {
            self.decide(ts, out);
        }
        Ok(true)
    }

    /// Copies the `(column, ts)` group at the cursor into the group buffer.
    fn read_group<C: Cursor>(&mut self, cursor: &mut C, ts: Timestamp) -> Result<(), C::Error> {
        self.group.clear();
        self.group_data.clear();
        let col_len = self.col.len();
        while cursor.valid() {
            let key = cursor.key();
            if key.len() != col_len + SUFFIX_LEN || !key.starts_with(&self.col) {
                break;
            }
            let Ok((_, t, seqno, kind)) = split_suffix(key) else {
                break;
            };
            if t != ts {
                break;
            }
            let key_start = self.group_data.len();
            self.group_data.extend_from_slice(key);
            let val_start = self.group_data.len();
            self.group_data.extend_from_slice(cursor.value());
            self.group.push(GEntry {
                key_start,
                val_start,
                end: self.group_data.len(),
                kind,
                stripe: self.points.partition_point(|&p| p < seqno),
            });
            cursor.next()?;
        }
        // The first entry was counted by `step`.
        self.read += self.group.len().saturating_sub(1) as u64;
        Ok(())
    }

    fn decide(&mut self, ts: Timestamp, out: &mut OutBuf) {
        let n_points = self.points.len();
        if self.expired(ts) {
            for e in &self.group {
                account_drop(
                    &mut self.blob_delta,
                    e.kind,
                    &self.group_data[e.val_start..e.end],
                );
            }
            return;
        }
        let cover = self.col_stripe.min(self.marker_stripe(ts));
        let mut cell_min = NONE;
        let mut coldel_min = NONE;
        for e in &self.group {
            match e.kind {
                Kind::CellDelete => cell_min = cell_min.min(e.stripe),
                Kind::ColumnDelete => coldel_min = coldel_min.min(e.stripe),
                _ => {}
            }
        }
        let hide = cover.min(cell_min).min(coldel_min);
        self.group_hide = hide;

        // Upper cell deletes can only hit timestamps >= min_ts_above, so if the whole column
        // is below it, versions counted here stay versions.
        let versions_gc = self.max_versions > 0 && self.purgeable(self.col_newest);
        let purge_deletes = self.purgeable(ts);
        if versions_gc {
            for j in 0..n_points {
                self.gidx[j] = 0;
                if hide <= j {
                    continue;
                }
                let visible =
                    |e: &GEntry| matches!(e.kind, Kind::Put | Kind::Merge) && e.stripe <= j;
                if !self.group.iter().any(visible) {
                    continue;
                }
                let base = self
                    .group
                    .iter()
                    .position(|e| e.kind == Kind::Put && e.stripe <= j);
                if !self.run_open[j] {
                    self.counts[j] += 1;
                }
                self.gidx[j] = self.counts[j];
                self.run_open[j] = base.is_none();
                self.gbase[j] = base.unwrap_or(usize::MAX);
            }
        }

        let mut put_min = NONE;
        let mut earlier_cell = NONE;
        let mut earlier_coldel = NONE;
        // A pending combined operand: its stripe.
        let mut pending: Option<usize> = None;
        for idx in 0..self.group.len() {
            let e = self.group[idx];
            let i = e.stripe;
            let keep = match e.kind {
                Kind::Put | Kind::Merge => {
                    let mut k = hide > i && put_min > i;
                    if k && versions_gc {
                        k = (i..n_points).any(|j| {
                            hide > j
                                && self.gidx[j] != 0
                                && self.gidx[j] <= self.max_versions
                                && idx <= self.gbase[j]
                        });
                    }
                    if e.kind == Kind::Put {
                        put_min = put_min.min(i);
                    }
                    k
                }
                Kind::CellDelete => {
                    let redundant = cover.min(coldel_min).min(earlier_cell) <= i;
                    earlier_cell = earlier_cell.min(i);
                    !redundant && !(purge_deletes && i == 0)
                }
                Kind::ColumnDelete => {
                    let redundant = cover.min(earlier_coldel) <= i;
                    earlier_coldel = earlier_coldel.min(i);
                    !redundant && !(purge_deletes && i == 0)
                }
                // Markers never share a group with cells.
                Kind::FamilyDelete => true,
            };
            let key = e.key_start..e.val_start;
            let value = e.val_start..e.end;
            if !keep {
                account_drop(&mut self.blob_delta, e.kind, &self.group_data[value]);
                continue;
            }
            self.kept += 1;
            // Combine consecutive kept operands of one stripe (no put between them, or it
            // would shadow the older one).
            if e.kind == Kind::Merge
                && let Some(op) = &self.merge
            {
                if pending == Some(i) {
                    // Merge into a scratch copy: a failed merge may leave its accumulator
                    // half-written, and the operands are then kept apart unchanged.
                    self.scratch.clear();
                    self.scratch.extend_from_slice(&self.acc);
                    if op
                        .merge(&mut self.scratch, &self.group_data[value.clone()])
                        .is_ok()
                    {
                        std::mem::swap(&mut self.acc, &mut self.scratch);
                        self.kept -= 1;
                        continue;
                    }
                }
                if pending.is_some() {
                    out.push(&self.acc_key, &self.acc);
                }
                pending = Some(i);
                self.acc_key.clear();
                self.acc_key.extend_from_slice(&self.group_data[key]);
                self.acc.clear();
                self.acc.extend_from_slice(&self.group_data[value]);
                continue;
            }
            if pending.take().is_some() {
                out.push(&self.acc_key, &self.acc);
            }
            out.push(&self.group_data[key], &self.group_data[value]);
        }
        if pending.is_some() {
            out.push(&self.acc_key, &self.acc);
        }
        self.col_stripe = self.col_stripe.min(coldel_min);
    }
}
