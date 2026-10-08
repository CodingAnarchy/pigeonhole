use std::collections::BTreeMap;
use std::fmt;
use std::ops::{Bound, RangeBounds};

use pigeonhole_format::{Durability, Seqno, Timestamp};
use pigeonhole_io::sim::CrashKind;

/// Policy of a model family (mirrors the persisted family options that change semantics).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ModelFamily {
    /// Family name.
    pub name: String,
    /// Versions kept per column; 0 keeps all.
    pub max_versions: u32,
    /// TTL in microseconds; 0 disables it.
    pub ttl_micros: u64,
    /// Whether merge operands use the built-in `i64` add operator (a 0.1.0-style family:
    /// operands at the commit timestamp, folded across timestamps, D41).
    pub i64_add: bool,
    /// A counter family (decision D179): `i64` values only, operands combine per timestamp,
    /// deletes hide only older writes. Implies the `i64` add operator.
    pub counter: bool,
}

/// The timestamp of a counter family's counter: puts and `Incr`s without a timestamp land
/// here (decision D179; the engine's `COUNTER_TS`).
pub const COUNTER_TS: Timestamp = 0;

/// One mutation in a model commit. Rows, families and qualifiers are plain values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelOp {
    /// Put a value.
    Put {
        /// Table name.
        table: String,
        /// Row.
        row: Vec<u8>,
        /// Family name.
        family: String,
        /// Qualifier.
        qualifier: Vec<u8>,
        /// Timestamp; `None` uses the commit timestamp.
        ts: Option<Timestamp>,
        /// Value.
        value: Vec<u8>,
    },
    /// Add `delta` with the `i64` merge operator.
    Incr {
        /// Table name.
        table: String,
        /// Row.
        row: Vec<u8>,
        /// Family name.
        family: String,
        /// Qualifier.
        qualifier: Vec<u8>,
        /// Bucket timestamp (counter families only, `incr_at`); `None` is [`COUNTER_TS`] in
        /// a counter family and the commit timestamp elsewhere.
        ts: Option<Timestamp>,
        /// Amount.
        delta: i64,
    },
    /// Delete one version.
    DeleteCell {
        /// Table name.
        table: String,
        /// Row.
        row: Vec<u8>,
        /// Family name.
        family: String,
        /// Qualifier.
        qualifier: Vec<u8>,
        /// Version timestamp.
        ts: Timestamp,
    },
    /// Delete all versions of a column at or below the commit timestamp.
    DeleteColumn {
        /// Table name.
        table: String,
        /// Row.
        row: Vec<u8>,
        /// Family name.
        family: String,
        /// Qualifier.
        qualifier: Vec<u8>,
    },
    /// Delete a family within a row.
    DeleteFamily {
        /// Table name.
        table: String,
        /// Row.
        row: Vec<u8>,
        /// Family name.
        family: String,
    },
    /// Delete a whole row (every family).
    DeleteRow {
        /// Table name.
        table: String,
        /// Row.
        row: Vec<u8>,
    },
}

/// Why a commit was rejected. Nothing is applied when a commit fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelError {
    /// The table does not exist.
    NoSuchTable(String),
    /// The family does not exist in the table.
    NoSuchFamily(String),
    /// `Incr` on a family without the `i64` add merge operator.
    NoMergeOperator(String),
    /// A read had to fold merge operands onto a base put whose value is not an 8-byte `i64`
    /// (decision D41); the family is named. Returned by the `try_` read methods.
    MergeFailed(String),
    /// A write a counter family refuses (a put that is not an 8-byte `i64`, or a
    /// fixed-timestamp write into one with a TTL), or an `Incr` with a timestamp outside a
    /// counter family (decision D179); the family is named. `InvalidArgument` in the store.
    CounterWrite(String),
}

impl fmt::Display for ModelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoSuchTable(t) => write!(f, "no such table {t:?}"),
            Self::NoSuchFamily(x) => write!(f, "no such family {x:?}"),
            Self::NoMergeOperator(x) => write!(f, "family {x:?} has no merge operator"),
            Self::MergeFailed(x) => write!(f, "merge failed in family {x:?}: base is not an i64"),
            Self::CounterWrite(x) => write!(f, "write refused by the counter rules of {x:?}"),
        }
    }
}

impl std::error::Error for ModelError {}

/// A visible cell as the model returns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelCell {
    /// Family name.
    pub family: String,
    /// Qualifier.
    pub qualifier: Vec<u8>,
    /// Timestamp.
    pub ts: Timestamp,
    /// Resolved value.
    pub value: Vec<u8>,
}

/// What may survive a crash: every commit with seqno `<= must_survive` is present after
/// recovery, nothing above `may_survive` is, and the survivors form a prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrashWindow {
    /// Highest seqno that must survive.
    pub must_survive: Seqno,
    /// Highest seqno that may survive.
    pub may_survive: Seqno,
}

/// A stored entry of one column. Newest-first order is `(ts, seqno)` descending.
#[derive(Debug, Clone)]
struct Entry {
    ts: Timestamp,
    seqno: Seqno,
    kind: Kind,
}

#[derive(Debug, Clone)]
enum Kind {
    Put(Vec<u8>),
    Merge(i64),
    /// A `CellDelete`: hides every version with exactly this timestamp, any seqno (D38).
    CellDelete,
    /// A `ColumnDelete`: hides every version with `ts <=` this timestamp, any seqno.
    ColumnDelete,
}

/// `(family, qualifier)` within a row.
type ColumnKey = (String, Vec<u8>);

/// Scan results: each row with its cells.
type Rows = Vec<(Vec<u8>, Vec<ModelCell>)>;

/// `(ts, seqno)` of a family-in-row marker.
type Marker = (Timestamp, Seqno);

#[derive(Debug, Default)]
struct Table {
    families: BTreeMap<String, ModelFamily>,
    /// Family names in creation order (D39).
    order: Vec<String>,
    /// Column entries, including cell and column delete markers.
    columns: BTreeMap<Vec<u8>, BTreeMap<ColumnKey, Vec<Entry>>>,
    /// Family-in-row delete markers.
    family_deletes: BTreeMap<(Vec<u8>, String), Vec<Marker>>,
}

/// The reference model: an in-memory `BTreeMap` implementation of Pigeonhole's semantics.
///
/// Semantics, in one place (the spec and decisions D9–D11 are the source):
///
/// - Every commit gets the next seqno (from 1) and applies atomically. A snapshot is a seqno;
///   a read at snapshot `s` sees only entries with seqno `<= s`.
/// - A column's versions are ordered newest first by timestamp, one version per timestamp.
/// - **Same-commit collapse (D34).** Within one commit, mutations to the same
///   `(row, family, qualifier, ts)` collapse to the last one written (a `Put`'s `ts` is its
///   explicit or the commit timestamp; `Incr` and `DeleteColumn` use the commit timestamp;
///   `DeleteCell` its own). So after a commit no column holds two entries at one `(ts, seqno)`.
///   Family and row markers are separate keys and never collapse with column entries.
///   In a counter family an `Incr` first combines into the latest earlier put or `Incr` of
///   its cell in the commit ([`combine_counter_writes`], D186 / #295), so `incr(1).incr(2)`
///   adds 3 and `put(5).incr(1)` writes 6.
/// - **Same timestamp across commits.** At one timestamp the newest-seqno put is the
///   version's base; merge operands with a newer seqno fold onto it; every older entry at
///   that timestamp is shadowed. A timestamp with operands only folds them into the run
///   described under `Incr` below.
/// - `DeleteColumn` and `DeleteFamily` (and `DeleteRow`, one family marker per family) take
///   the commit timestamp `T` and hide every version in scope with timestamp `<= T`,
///   whatever its seqno, including a put in the same commit with timestamp `<= T` and a later
///   put with an older timestamp. `DeleteCell` is timestamp-only too (D38): it hides every
///   version at exactly its timestamp whatever its seqno, so a put at that timestamp in a
///   later commit stays hidden (until compaction purges the marker; [`Model::purge`] applies
///   the same purge, HBase semantics).
/// - TTL: a version is expired when `ts + ttl_micros <= now` (timestamps are microseconds).
///   Expired versions are dropped before merge operands are folded and versions counted.
/// - `Incr` (only on `i64_add` families, else [`ModelError::NoMergeOperator`]) is a merge
///   operand at the commit timestamp. Going newest to oldest, a run of operands folds into
///   one cell at the newest operand's timestamp: its value is the wrapping sum of the
///   operands plus the `i64` in the next older put, which the fold consumes. A base put
///   whose value is not 8 bytes makes the read fail with [`ModelError::MergeFailed`] (D41);
///   a put no operand folds onto is returned as written. Without a base the sum is the
///   value. An expired base is dropped before folding, so the run then has no base.
/// - **Counter families (D179)** differ: puts and `Incr`s without a timestamp land at
///   [`COUNTER_TS`] (a family with a TTL refuses them, and refuses non-8-byte puts), every
///   timestamp is its own version (operands never fold across timestamps; at one timestamp
///   the newest put is the base and newer operands add to it), and a delete hides only
///   entries with a lower seqno within its timestamp scope, so a later write at a covered
///   timestamp stays visible. Purges never change a counter family's reads.
/// - `max_versions` keeps the newest N resolved versions (after deletes, TTL and folding).
/// - Reads order cells by family, then qualifier, then timestamp descending. Families come
///   in creation order, or in the caller's order when the read lists families (D39).
///
/// ```
/// use pigeonhole_format::Durability;
/// use pigeonhole_sim::{Model, ModelFamily, ModelOp};
///
/// let mut m = Model::new();
/// m.create_table("t", vec![ModelFamily { name: "f".into(), ..Default::default() }]);
/// let put = |v: &[u8]| ModelOp::Put {
///     table: "t".into(), row: b"r".to_vec(), family: "f".into(),
///     qualifier: b"q".to_vec(), ts: None, value: v.to_vec(),
/// };
/// m.commit(&[put(b"a")], 10, Durability::Sync);
/// m.commit(&[put(b"b")], 20, Durability::Sync);
/// let cell = m.get("t", b"r", "f", b"q", m.snapshot(), 30).unwrap();
/// assert_eq!(cell.value, b"b");
/// assert_eq!(m.get("t", b"r", "f", b"q", 1, 30).unwrap().value, b"a");
/// ```
#[derive(Debug, Default)]
pub struct Model {
    tables: BTreeMap<String, Table>,
    /// Acknowledged durability per seqno; index `i` is seqno `i + 1`.
    commits: Vec<Durability>,
}

impl Model {
    /// An empty model.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a table with families.
    pub fn create_table(&mut self, name: &str, families: Vec<ModelFamily>) {
        let table = self.tables.entry(name.to_owned()).or_default();
        for f in families {
            let name = f.name.clone();
            if table.families.insert(name.clone(), f).is_none() {
                table.order.push(name);
            }
        }
    }

    /// Applies a commit atomically at the next seqno with timestamp `commit_ts`, remembering
    /// the durability level it was acknowledged with. Returns the seqno.
    ///
    /// For a commit that was in flight at a crash (never acknowledged), pass
    /// [`Durability::None`]: nothing is promised, but it may still have survived.
    ///
    /// # Panics
    /// If the commit is invalid; use [`Model::try_commit`] to get the error instead.
    pub fn commit(
        &mut self,
        ops: &[ModelOp],
        commit_ts: Timestamp,
        durability: Durability,
    ) -> Seqno {
        match self.try_commit(ops, commit_ts, durability) {
            Ok(seqno) => seqno,
            Err(e) => panic!("invalid model commit: {e}"),
        }
    }

    /// Like [`Model::commit`], but an invalid commit is a typed error and changes nothing:
    /// no seqno is consumed and no mutation is applied.
    ///
    /// Within one commit, mutations to the same column at the same timestamp collapse to the
    /// last one written (decision D34); see the type docs.
    pub fn try_commit(
        &mut self,
        ops: &[ModelOp],
        commit_ts: Timestamp,
        durability: Durability,
    ) -> Result<Seqno, ModelError> {
        for op in ops {
            self.validate(op)?;
        }
        let combined = combine_counter_writes(ops, |table, family| self.is_counter(table, family));
        let ops = combined.as_slice();
        self.commits.push(durability);
        let seqno = self.commits.len() as Seqno;
        // Keep only the last column-level mutation per (row, family, qualifier, ts).
        let mut last: BTreeMap<CollapseKey<'_>, usize> = BTreeMap::new();
        let keys: Vec<Option<CollapseKey<'_>>> = ops
            .iter()
            .map(|op| self.collapse_key(op, commit_ts))
            .collect();
        for (i, key) in keys.iter().enumerate() {
            if let Some(key) = key {
                last.insert(*key, i);
            }
        }
        let collapsed: Vec<bool> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| k.is_some_and(|k| last[&k] != i))
            .collect();
        for (op, collapsed) in ops.iter().zip(collapsed) {
            if !collapsed {
                self.apply(op, commit_ts, seqno);
            }
        }
        Ok(seqno)
    }

    fn validate(&self, op: &ModelOp) -> Result<(), ModelError> {
        let (table, family) = match op {
            ModelOp::Put { table, family, .. }
            | ModelOp::Incr { table, family, .. }
            | ModelOp::DeleteCell { table, family, .. }
            | ModelOp::DeleteColumn { table, family, .. }
            | ModelOp::DeleteFamily { table, family, .. } => (table, Some(family)),
            ModelOp::DeleteRow { table, .. } => (table, None),
        };
        let t = self
            .tables
            .get(table)
            .ok_or_else(|| ModelError::NoSuchTable(table.clone()))?;
        let Some(family) = family else {
            return Ok(());
        };
        let f = t
            .families
            .get(family)
            .ok_or_else(|| ModelError::NoSuchFamily(family.clone()))?;
        let refused = || Err(ModelError::CounterWrite(family.clone()));
        // A fixed-timestamp write into a counter family with a TTL would expire at once.
        let fixed_with_ttl = |ts: &Option<Timestamp>| ts.is_none() && f.ttl_micros != 0;
        match op {
            ModelOp::Incr { ts, .. } if f.counter && fixed_with_ttl(ts) => refused(),
            ModelOp::Incr { .. } if f.counter => Ok(()),
            ModelOp::Incr { ts: Some(_), .. } => refused(),
            ModelOp::Incr { .. } if !f.i64_add => Err(ModelError::NoMergeOperator(family.clone())),
            ModelOp::Put { ts, value, .. }
                if f.counter && (value.len() != 8 || fixed_with_ttl(ts)) =>
            {
                refused()
            }
            _ => Ok(()),
        }
    }

    /// Whether `family` of `table` is a counter family.
    fn is_counter(&self, table: &str, family: &str) -> bool {
        self.tables
            .get(table)
            .and_then(|t| t.families.get(family))
            .is_some_and(|f| f.counter)
    }

    /// The collapse key of a column-level mutation (see [`column_key`]), with counter-family
    /// puts and operands at [`COUNTER_TS`].
    fn collapse_key<'a>(&self, op: &'a ModelOp, commit_ts: Timestamp) -> Option<CollapseKey<'a>> {
        let default_ts = match op {
            ModelOp::Put { table, family, .. } | ModelOp::Incr { table, family, .. }
                if self.is_counter(table, family) =>
            {
                COUNTER_TS
            }
            _ => commit_ts,
        };
        column_key(op, commit_ts, default_ts)
    }

    /// Applies one validated mutation.
    fn apply(&mut self, op: &ModelOp, commit_ts: Timestamp, seqno: Seqno) {
        let (table, row) = match op {
            ModelOp::Put { table, row, .. }
            | ModelOp::Incr { table, row, .. }
            | ModelOp::DeleteCell { table, row, .. }
            | ModelOp::DeleteColumn { table, row, .. }
            | ModelOp::DeleteFamily { table, row, .. }
            | ModelOp::DeleteRow { table, row } => (table, row),
        };
        let t = self.tables.get_mut(table).expect("validated");
        // Puts and operands without a timestamp: the counter's in a counter family.
        let default_ts = match op {
            ModelOp::Put { family, .. } | ModelOp::Incr { family, .. }
                if t.families[family].counter =>
            {
                COUNTER_TS
            }
            _ => commit_ts,
        };
        let mut column = |family: &String, qualifier: &Vec<u8>, ts: Timestamp, kind: Kind| {
            t.columns
                .entry(row.clone())
                .or_default()
                .entry((family.clone(), qualifier.clone()))
                .or_default()
                .push(Entry { ts, seqno, kind });
        };
        match op {
            ModelOp::Put {
                family,
                qualifier,
                ts,
                value,
                ..
            } => column(
                family,
                qualifier,
                ts.unwrap_or(default_ts),
                Kind::Put(value.clone()),
            ),
            ModelOp::Incr {
                family,
                qualifier,
                ts,
                delta,
                ..
            } => {
                column(
                    family,
                    qualifier,
                    ts.unwrap_or(default_ts),
                    Kind::Merge(*delta),
                );
            }
            ModelOp::DeleteCell {
                family,
                qualifier,
                ts,
                ..
            } => {
                column(family, qualifier, *ts, Kind::CellDelete);
            }
            ModelOp::DeleteColumn {
                family, qualifier, ..
            } => {
                column(family, qualifier, commit_ts, Kind::ColumnDelete);
            }
            ModelOp::DeleteFamily { family, .. } => {
                t.family_deletes
                    .entry((row.clone(), family.clone()))
                    .or_default()
                    .push((commit_ts, seqno));
            }
            ModelOp::DeleteRow { .. } => {
                for family in t.families.keys() {
                    t.family_deletes
                        .entry((row.clone(), family.clone()))
                        .or_default()
                        .push((commit_ts, seqno));
                }
            }
        }
    }

    /// The latest seqno (a snapshot of "everything so far").
    pub fn snapshot(&self) -> Seqno {
        self.commits.len() as Seqno
    }

    /// The newest visible version of a cell at `snapshot`, with TTL evaluated at `now`.
    ///
    /// # Panics
    /// On [`ModelError::MergeFailed`]; use [`Model::try_get`] to get the error instead.
    pub fn get(
        &self,
        table: &str,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        snapshot: Seqno,
        now: Timestamp,
    ) -> Option<ModelCell> {
        expect_read(self.try_get(table, row, family, qualifier, snapshot, now))
    }

    /// Like [`Model::get`], but a failed merge is a typed error.
    pub fn try_get(
        &self,
        table: &str,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        snapshot: Seqno,
        now: Timestamp,
    ) -> Result<Option<ModelCell>, ModelError> {
        let Some(t) = self.tables.get(table) else {
            return Ok(None);
        };
        let Some(fam) = t.families.get(family) else {
            return Ok(None);
        };
        let Some(entries) = t
            .columns
            .get(row)
            .and_then(|c| c.get(&(family.to_owned(), qualifier.to_vec())))
        else {
            return Ok(None);
        };
        let markers = t.family_deletes.get(&(row.to_vec(), family.to_owned()));
        let mut versions = resolve(fam, entries, markers, snapshot, now, 1)?;
        Ok(versions.pop().map(|(ts, value)| ModelCell {
            family: family.to_owned(),
            qualifier: qualifier.to_vec(),
            ts,
            value,
        }))
    }

    /// Up to `versions` visible versions of every cell of `row` in `families` (all if empty).
    /// `versions == 0` means every visible version. Families come in the order listed, or in
    /// creation order when `families` is empty (D39); a name listed twice or not in the table
    /// is skipped.
    ///
    /// # Panics
    /// On [`ModelError::MergeFailed`]; use [`Model::try_read_row`] to get the error instead.
    pub fn read_row(
        &self,
        table: &str,
        row: &[u8],
        families: &[&str],
        versions: u32,
        snapshot: Seqno,
        now: Timestamp,
    ) -> Vec<ModelCell> {
        expect_read(self.try_read_row(table, row, families, versions, snapshot, now))
    }

    /// Like [`Model::read_row`], but a failed merge is a typed error.
    pub fn try_read_row(
        &self,
        table: &str,
        row: &[u8],
        families: &[&str],
        versions: u32,
        snapshot: Seqno,
        now: Timestamp,
    ) -> Result<Vec<ModelCell>, ModelError> {
        let Some(t) = self.tables.get(table) else {
            return Ok(Vec::new());
        };
        let Some(columns) = t.columns.get(row) else {
            return Ok(Vec::new());
        };
        let order: Vec<&str> = if families.is_empty() {
            t.order.iter().map(String::as_str).collect()
        } else {
            families.to_vec()
        };
        let mut out = Vec::new();
        for (i, &family) in order.iter().enumerate() {
            let Some(fam) = t.families.get(family) else {
                continue;
            };
            if order[..i].contains(&family) {
                continue;
            }
            let markers = t.family_deletes.get(&(row.to_vec(), family.to_owned()));
            let in_family = columns
                .range((family.to_owned(), Vec::new())..)
                .take_while(|((f, _), _)| f == family);
            for ((_, qualifier), entries) in in_family {
                for (ts, value) in resolve(fam, entries, markers, snapshot, now, versions)? {
                    out.push(ModelCell {
                        family: family.to_owned(),
                        qualifier: qualifier.clone(),
                        ts,
                        value,
                    });
                }
            }
        }
        Ok(out)
    }

    /// Rows in `[start, end)` with their latest visible cells, in order. Rows with no visible
    /// cell are omitted. Cells within a row are ordered as by [`Model::read_row`].
    ///
    /// # Panics
    /// On [`ModelError::MergeFailed`]; use [`Model::try_scan`] to get the error instead.
    pub fn scan(
        &self,
        table: &str,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        families: &[&str],
        snapshot: Seqno,
        now: Timestamp,
    ) -> Vec<(Vec<u8>, Vec<ModelCell>)> {
        expect_read(self.try_scan(table, start, end, families, snapshot, now))
    }

    /// Like [`Model::scan`], but a failed merge is a typed error.
    pub fn try_scan(
        &self,
        table: &str,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        families: &[&str],
        snapshot: Seqno,
        now: Timestamp,
    ) -> Result<Rows, ModelError> {
        let Some(t) = self.tables.get(table) else {
            return Ok(Vec::new());
        };
        let to_owned = |b: Bound<&[u8]>| match b {
            Bound::Included(k) => Bound::Included(k.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let (start, end) = (to_owned(start), to_owned(end));
        // `BTreeMap::range` panics on an empty or inverted range; the model returns nothing.
        let inverted = match (&start, &end) {
            (Bound::Included(s), Bound::Included(e)) => s > e,
            (Bound::Included(s) | Bound::Excluded(s), Bound::Excluded(e))
            | (Bound::Excluded(s), Bound::Included(e)) => s >= e,
            _ => false,
        };
        if inverted {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for row in t.columns.range((start, end)).map(|(row, _)| row) {
            let cells = self.try_read_row(table, row, families, 1, snapshot, now)?;
            if !cells.is_empty() {
                out.push((row.clone(), cells));
            }
        }
        Ok(out)
    }

    /// What a crash of `kind` may lose, given each commit's acknowledged durability.
    ///
    /// This is the single-stream special case (D84): durability is per WAL stream, so with
    /// several streams (one per shard) there is no global prefix and a later `GroupSync`
    /// commit on one stream says nothing about an earlier `Buffered` one on another. Use
    /// [`recovered_commits`](crate::recovered_commits) and
    /// [`check_acknowledged_survive`](crate::check_acknowledged_survive) for that.
    ///
    /// Every stream is ordered, so a commit at level `L` also makes every earlier commit
    /// durable at `L` (the spec's "a `GroupSync` commit also makes earlier `Buffered` or
    /// `None` records durable"); the survivors are always a prefix. After a process crash
    /// everything handed to the kernel (`Buffered` and up) survives; after power loss only
    /// `GroupSync` and `Sync` commits are promised. Anything may survive.
    pub fn crash_window(&self, kind: CrashKind) -> CrashWindow {
        let floor = match kind {
            CrashKind::Process => Durability::Buffered,
            CrashKind::Power => Durability::GroupSync,
        };
        let must_survive = self
            .commits
            .iter()
            .rposition(|d| *d >= floor)
            .map_or(0, |i| i as Seqno + 1);
        CrashWindow {
            must_survive,
            may_survive: self.snapshot(),
        }
    }

    /// Discards every commit above `seqno` (after recovery revealed which prefix survived).
    pub fn truncate(&mut self, seqno: Seqno) {
        self.commits.truncate(seqno as usize);
        for t in self.tables.values_mut() {
            t.columns.retain(|_, cols| {
                cols.retain(|_, entries| {
                    entries.retain(|e| e.seqno <= seqno);
                    !entries.is_empty()
                });
                !cols.is_empty()
            });
            t.family_deletes.retain(|_, ms| {
                ms.retain(|m| m.1 <= seqno);
                !ms.is_empty()
            });
        }
    }

    /// Records that a crash of `kind` left exactly the commits up to `survivors`: truncates
    /// to it, and after power loss marks the survivors durable, because they are now what
    /// the disk holds.
    pub fn recover(&mut self, kind: CrashKind, survivors: Seqno) {
        self.truncate(survivors);
        if kind == CrashKind::Power {
            self.commits.fill(Durability::Sync);
        }
    }

    /// Applies what a bottommost compaction may purge from one family (the owner's HBase
    /// rule), so the model keeps matching the store after later writes with older explicit
    /// timestamps. Reads at the live snapshots are unchanged; afterwards a later write that
    /// a purged marker or version limit would have hidden becomes visible, as in the store.
    ///
    /// Only *input* entries (`seqno <= max_seqno`, rows in `rows`) are touched, and only
    /// timestamps below `min_ts_above`. In order:
    /// 1. A delete (cell, column or family) visible at every read point (`seqno <=` the
    ///    oldest snapshot, or any seqno with no snapshot) is removed, with every put and
    ///    operand it covers, and every delete it makes redundant: one of narrower scope at
    ///    or below its timestamp, or of the same scope and timestamp with a lower seqno.
    /// 2. If the family has `max_versions`, in each column whose input entries are all
    ///    below `min_ts_above`, every put and operand that contributes to none of the newest
    ///    `max_versions` versions at any read point (live snapshots and latest, input
    ///    entries only, TTL at `now`) is removed.
    pub fn purge(&mut self, p: &ModelPurge) {
        self.purge_inner(p, None);
    }

    /// Applies what a flush may purge under the guard of #287: only step 2 of
    /// [`Model::purge`], over the entries of `p.rows` and `p.family` whose seqno is in
    /// `inputs` (the flushed memtable's commits), with no `min_ts_above` condition. No delete
    /// is purged, and nothing at all unless the model agrees the guard held: no delete of
    /// the family in those rows outside `inputs` with a seqno at or below `installed` (the
    /// newest seqno committed before the purge was installed) or the newest input, unless
    /// expired at `p.now`. A delete committed after the install is a later write (D74).
    /// `p.min_ts_above` and `p.max_seqno` are ignored.
    pub fn purge_versions(&mut self, p: &ModelPurge, inputs: &[Seqno], installed: Seqno) {
        let mut inputs = inputs.to_vec();
        inputs.sort_unstable();
        self.purge_inner(p, Some((&inputs, installed)));
    }

    /// [`Model::purge`], or with `inputs` (sorted) [`Model::purge_versions`].
    fn purge_inner(&mut self, p: &ModelPurge, flush: Option<(&[Seqno], Seqno)>) {
        let inputs = flush.map(|f| f.0);
        let Some(t) = self.tables.get_mut(&p.table) else {
            return;
        };
        let Some(fam) = t.families.get(&p.family).cloned() else {
            return;
        };
        if fam.counter {
            // A counter family's deletes hide only older writes, which a purge drops with
            // them, so purges never change its reads.
            return;
        }
        let first = p.snapshots.iter().copied().min().unwrap_or(Seqno::MAX);
        let mut points: Vec<Seqno> = p.snapshots.clone();
        points.push(Seqno::MAX);
        let versions_only = inputs.is_some();
        if let Some((set, installed)) = flush
            && !Self::flush_guard_holds(t, &fam, p, set, installed)
        {
            return;
        }
        let input = |seqno: Seqno| {
            inputs.map_or(seqno <= p.max_seqno, |set| {
                set.binary_search(&seqno).is_ok()
            })
        };
        let purgeable = |ts: Timestamp, seqno: Seqno| {
            !versions_only && input(seqno) && seqno <= first && ts < p.min_ts_above
        };
        let in_range = |row: &Vec<u8>| p.rows.contains(row);
        let rows: Vec<Vec<u8>> = t
            .columns
            .keys()
            .chain(t.family_deletes.keys().map(|(r, _)| r))
            .filter(|r| in_range(r))
            .cloned()
            .collect();
        for row in rows {
            let mkey = (row.clone(), p.family.clone());
            let markers = t.family_deletes.get(&mkey).cloned().unwrap_or_default();
            let purged_markers: Vec<Marker> = markers
                .iter()
                .copied()
                .filter(|m| purgeable(m.0, m.1))
                .collect();
            // Markers: purged, or redundant (an earlier purged marker at or above).
            let marker_dropped = |m: &Marker| {
                purgeable(m.0, m.1)
                    || (input(m.1)
                        && purged_markers
                            .iter()
                            .any(|d| d.0 > m.0 || (d.0 == m.0 && d.1 > m.1)))
            };
            let kept_markers: Vec<Marker> = markers
                .iter()
                .copied()
                .filter(|m| !marker_dropped(m))
                .collect();
            if kept_markers.is_empty() {
                t.family_deletes.remove(&mkey);
            } else {
                t.family_deletes.insert(mkey.clone(), kept_markers.clone());
            }
            let marker_ts = purged_markers.iter().map(|m| m.0).max();
            let Some(cols) = t.columns.get_mut(&row) else {
                continue;
            };
            for ((family, _), entries) in cols.iter_mut() {
                if *family != p.family {
                    continue;
                }
                // Step 1: purged deletes and what they cover.
                let col_dels: Vec<(Timestamp, Seqno)> = entries
                    .iter()
                    .filter(|e| matches!(e.kind, Kind::ColumnDelete) && purgeable(e.ts, e.seqno))
                    .map(|e| (e.ts, e.seqno))
                    .collect();
                let cell_dels: Vec<(Timestamp, Seqno)> = entries
                    .iter()
                    .filter(|e| matches!(e.kind, Kind::CellDelete) && purgeable(e.ts, e.seqno))
                    .map(|e| (e.ts, e.seqno))
                    .collect();
                let by_marker = |ts: Timestamp| marker_ts.is_some_and(|m| ts <= m);
                entries.retain(|e| {
                    if !input(e.seqno) {
                        return true;
                    }
                    let dropped = match e.kind {
                        Kind::Put(_) | Kind::Merge(_) => {
                            by_marker(e.ts)
                                || col_dels.iter().any(|d| e.ts <= d.0)
                                || cell_dels.iter().any(|d| e.ts == d.0)
                        }
                        Kind::ColumnDelete => {
                            purgeable(e.ts, e.seqno)
                                || by_marker(e.ts)
                                || col_dels
                                    .iter()
                                    .any(|d| d.0 > e.ts || (d.0 == e.ts && d.1 > e.seqno))
                        }
                        Kind::CellDelete => {
                            purgeable(e.ts, e.seqno)
                                || by_marker(e.ts)
                                || col_dels.iter().any(|d| d.0 >= e.ts)
                                || cell_dels.iter().any(|d| d.0 == e.ts && d.1 > e.seqno)
                        }
                    };
                    !dropped
                });
                // Step 2: versions beyond the limit at every read point.
                if fam.max_versions == 0
                    || (!versions_only
                        && entries
                            .iter()
                            .any(|e| input(e.seqno) && e.ts >= p.min_ts_above))
                {
                    continue;
                }
                let inputs: Vec<Entry> =
                    entries.iter().filter(|e| input(e.seqno)).cloned().collect();
                let input_markers: Vec<Marker> = kept_markers
                    .iter()
                    .copied()
                    .filter(|m| input(m.1))
                    .collect();
                let mut needed = vec![false; inputs.len()];
                for &point in &points {
                    let versions = contributors(&fam, &inputs, &input_markers, point, p.now);
                    for v in versions.iter().take(fam.max_versions as usize) {
                        for &i in v {
                            needed[i] = true;
                        }
                    }
                }
                let mut i = 0;
                entries.retain(|e| {
                    if !input(e.seqno) {
                        return true;
                    }
                    let keep = needed[i] || !matches!(e.kind, Kind::Put(_) | Kind::Merge(_));
                    i += 1;
                    keep
                });
            }
            cols.retain(|_, entries| !entries.is_empty());
            if cols.is_empty() {
                t.columns.remove(&row);
            }
        }
    }
}

impl Model {
    /// [`Model::purge_versions`]'s guard over table `t`.
    fn flush_guard_holds(
        t: &Table,
        fam: &ModelFamily,
        p: &ModelPurge,
        inputs: &[Seqno],
        installed: Seqno,
    ) -> bool {
        let Some(&newest) = inputs.last() else {
            return true;
        };
        let before_install = newest.max(installed);
        let outside = |ts: Timestamp, seqno: Seqno| {
            seqno <= before_install
                && inputs.binary_search(&seqno).is_err()
                && !(fam.ttl_micros != 0 && ts.saturating_add(fam.ttl_micros) <= p.now)
        };
        let markers = t
            .family_deletes
            .iter()
            .filter(|((row, f), _)| *f == p.family && p.rows.contains(row))
            .flat_map(|(_, ms)| ms.iter())
            .any(|m| outside(m.0, m.1));
        let cells = t
            .columns
            .iter()
            .filter(|(row, _)| p.rows.contains(*row))
            .flat_map(|(_, cols)| cols.iter())
            .filter(|((f, _), _)| *f == p.family)
            .flat_map(|(_, entries)| entries.iter())
            .any(|e| {
                matches!(e.kind, Kind::CellDelete | Kind::ColumnDelete) && outside(e.ts, e.seqno)
            });
        !markers && !cells
    }
}

/// What [`Model::purge`] removes: the bottommost-compaction purge of one family's input
/// entries (owner decision on purges vs later writes, HBase semantics).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelPurge {
    /// Table name.
    pub table: String,
    /// Family name.
    pub family: String,
    /// Rows the compaction covered.
    pub rows: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    /// Live snapshot seqnos (the compaction's `GcPolicy::snapshots`).
    pub snapshots: Vec<Seqno>,
    /// The compaction's `now` (TTL).
    pub now: Timestamp,
    /// The compaction's `GcPolicy::min_ts_above`: nothing at or above it is purged.
    pub min_ts_above: Timestamp,
    /// The newest seqno among the compaction's inputs; newer entries are above them.
    pub max_seqno: Seqno,
}

/// Indices into `entries` of the entries each visible version is made of (base and the
/// operands folded onto it), newest version first: the same grouping as [`resolve`].
fn contributors(
    family: &ModelFamily,
    entries: &[Entry],
    family_markers: &[Marker],
    snapshot: Seqno,
    now: Timestamp,
) -> Vec<Vec<usize>> {
    let visible = |seqno: Seqno| seqno <= snapshot;
    let covered = entries
        .iter()
        .filter(|e| visible(e.seqno) && matches!(e.kind, Kind::ColumnDelete))
        .map(|e| e.ts)
        .chain(family_markers.iter().filter(|m| visible(m.1)).map(|m| m.0))
        .max();
    let cell_deleted = |ts: Timestamp| {
        entries
            .iter()
            .any(|d| visible(d.seqno) && matches!(d.kind, Kind::CellDelete) && d.ts == ts)
    };
    let mut live: Vec<usize> = (0..entries.len())
        .filter(|&i| {
            let e = &entries[i];
            visible(e.seqno)
                && matches!(e.kind, Kind::Put(_) | Kind::Merge(_))
                && covered.is_none_or(|c| e.ts > c)
                && !cell_deleted(e.ts)
                && (family.ttl_micros == 0 || e.ts.saturating_add(family.ttl_micros) > now)
        })
        .collect();
    live.sort_by_key(|&i| std::cmp::Reverse((entries[i].ts, entries[i].seqno)));
    let mut out = Vec::new();
    let mut run: Option<Vec<usize>> = None;
    let mut i = 0;
    while i < live.len() {
        let ts = entries[live[i]].ts;
        let end = live[i..]
            .iter()
            .position(|&j| entries[j].ts != ts)
            .map_or(live.len(), |n| i + n);
        let group = &live[i..end];
        i = end;
        match group
            .iter()
            .position(|&j| matches!(entries[j].kind, Kind::Put(_)))
        {
            Some(b) => {
                let mut v = run.take().unwrap_or_default();
                v.extend_from_slice(&group[..=b]);
                out.push(v);
            }
            None => run.get_or_insert_with(Vec::new).extend_from_slice(group),
        }
    }
    out.extend(run);
    out
}

/// Unwraps a read for the panicking read methods.
fn expect_read<T>(r: Result<T, ModelError>) -> T {
    r.unwrap_or_else(|e| panic!("model read failed: {e}"))
}

/// Resolves one column to its visible versions, newest first, at most `limit` (0 = all).
/// Fails if a version within the limit folds operands onto a base that is not an `i64`.
fn resolve(
    family: &ModelFamily,
    entries: &[Entry],
    family_markers: Option<&Vec<Marker>>,
    snapshot: Seqno,
    now: Timestamp,
    limit: u32,
) -> Result<Vec<(Timestamp, Vec<u8>)>, ModelError> {
    if family.counter {
        return resolve_counter(family, entries, family_markers, snapshot, now, limit);
    }
    let visible = |seqno: Seqno| seqno <= snapshot;
    // Highest timestamp a visible column or family delete covers.
    let covered = entries
        .iter()
        .filter(|e| visible(e.seqno) && matches!(e.kind, Kind::ColumnDelete))
        .map(|e| e.ts)
        .chain(
            family_markers
                .into_iter()
                .flatten()
                .filter(|m| visible(m.1))
                .map(|m| m.0),
        )
        .max();
    // Deletes and the survivors are found with linear scans, so resolving a column costs
    // O(entries x cell deletes); columns in model runs are tiny, so clarity wins.
    let cell_deletes: Vec<&Entry> = entries
        .iter()
        .filter(|e| visible(e.seqno) && matches!(e.kind, Kind::CellDelete))
        .collect();
    let mut live: Vec<&Entry> = entries
        .iter()
        .filter(|e| {
            visible(e.seqno)
                && matches!(e.kind, Kind::Put(_) | Kind::Merge(_))
                && covered.is_none_or(|c| e.ts > c)
                && !cell_deletes.iter().any(|d| d.ts == e.ts)
                && (family.ttl_micros == 0 || e.ts.saturating_add(family.ttl_micros) > now)
        })
        .collect();
    // Within one commit a (column, ts) holds one entry (D34), so this order is total.
    live.sort_by_key(|e| std::cmp::Reverse((e.ts, e.seqno)));

    // One version per timestamp. At a timestamp, entries are taken newest seqno first: the
    // newest put is the base, merge operands newer than it fold onto it, and everything
    // older at that timestamp is shadowed. A timestamp with operands but no put adds them to
    // a pending run; the run folds onto the first older timestamp that has a put (whose
    // newer-than-base operands join the sum), and the version carries the timestamp of the
    // run's newest operand.
    // A version whose fold failed is `None`; it is an error only if it is returned.
    let mut out: Vec<(Timestamp, Option<Vec<u8>>)> = Vec::new();
    let mut run: Option<(Timestamp, i64)> = None;
    let mut i = 0;
    while i < live.len() {
        let ts = live[i].ts;
        let end = live[i..]
            .iter()
            .position(|e| e.ts != ts)
            .map_or(live.len(), |n| i + n);
        let group = &live[i..end];
        i = end;
        let base = group.iter().position(|e| matches!(e.kind, Kind::Put(_)));
        let operands = group[..base.unwrap_or(group.len())]
            .iter()
            .fold(0i64, |sum, e| match e.kind {
                Kind::Merge(d) => sum.wrapping_add(d),
                _ => sum,
            });
        match base.map(|b| &group[b].kind) {
            Some(Kind::Put(value)) if run.is_none() && base == Some(0) => {
                out.push((ts, Some(value.clone())));
            }
            Some(Kind::Put(value)) => {
                let (run_ts, run_sum) = run.take().unwrap_or((ts, 0));
                let total =
                    as_i64(value).map(|base| run_sum.wrapping_add(operands).wrapping_add(base));
                out.push((run_ts, total.map(|t| t.to_le_bytes().to_vec())));
            }
            _ => {
                run = Some(match run {
                    Some((run_ts, sum)) => (run_ts, sum.wrapping_add(operands)),
                    None => (ts, operands),
                });
            }
        }
    }
    if let Some((run_ts, sum)) = run {
        out.push((run_ts, Some(sum.to_le_bytes().to_vec())));
    }
    let mut cap = if family.max_versions == 0 {
        usize::MAX
    } else {
        family.max_versions as usize
    };
    if limit != 0 {
        cap = cap.min(limit as usize);
    }
    out.truncate(cap);
    out.into_iter()
        .map(|(ts, value)| {
            value
                .map(|v| (ts, v))
                .ok_or_else(|| ModelError::MergeFailed(family.name.clone()))
        })
        .collect()
}

/// [`resolve`] for a counter family (D179): a delete hides only entries with a lower seqno
/// in its scope, and every timestamp is one version: its newest put plus the operands newer
/// than that put, or the sum of its operands.
fn resolve_counter(
    family: &ModelFamily,
    entries: &[Entry],
    family_markers: Option<&Vec<Marker>>,
    snapshot: Seqno,
    now: Timestamp,
    limit: u32,
) -> Result<Vec<(Timestamp, Vec<u8>)>, ModelError> {
    let visible = |seqno: Seqno| seqno <= snapshot;
    let hidden = |e: &Entry| {
        let by_entry = entries.iter().any(|d| {
            visible(d.seqno)
                && d.seqno > e.seqno
                && match d.kind {
                    Kind::ColumnDelete => e.ts <= d.ts,
                    Kind::CellDelete => e.ts == d.ts,
                    Kind::Put(_) | Kind::Merge(_) => false,
                }
        });
        let by_marker = family_markers
            .into_iter()
            .flatten()
            .any(|m| visible(m.1) && m.1 > e.seqno && e.ts <= m.0);
        by_entry || by_marker
    };
    let mut live: Vec<&Entry> = entries
        .iter()
        .filter(|e| {
            visible(e.seqno)
                && matches!(e.kind, Kind::Put(_) | Kind::Merge(_))
                && !hidden(e)
                && (family.ttl_micros == 0 || e.ts.saturating_add(family.ttl_micros) > now)
        })
        .collect();
    live.sort_by_key(|e| std::cmp::Reverse((e.ts, e.seqno)));
    let mut out = Vec::new();
    let mut i = 0;
    while i < live.len() {
        let ts = live[i].ts;
        let end = live[i..]
            .iter()
            .position(|e| e.ts != ts)
            .map_or(live.len(), |n| i + n);
        let group = &live[i..end];
        i = end;
        let mut sum = 0i64;
        let mut value = None;
        for e in group {
            match &e.kind {
                Kind::Merge(d) => sum = sum.wrapping_add(*d),
                Kind::Put(v) => {
                    value = as_i64(v).map(|base| sum.wrapping_add(base));
                    break;
                }
                _ => {}
            }
        }
        let base_put = group.iter().any(|e| matches!(e.kind, Kind::Put(_)));
        out.push((ts, if base_put { value } else { Some(sum) }));
    }
    let mut cap = if family.max_versions == 0 {
        usize::MAX
    } else {
        family.max_versions as usize
    };
    if limit != 0 {
        cap = cap.min(limit as usize);
    }
    out.truncate(cap);
    out.into_iter()
        .map(|(ts, v)| {
            v.map(|v| (ts, v.to_le_bytes().to_vec()))
                .ok_or_else(|| ModelError::MergeFailed(family.name.clone()))
        })
        .collect()
}

/// `ops` with the writes of counter families combined as the store combines them within
/// one commit (decision D186, #295): an `Incr` of a cell (table, row, family, qualifier and
/// timestamp, [`COUNTER_TS`] when it has none) that an earlier put or `Incr` of `ops` wrote
/// is added to that write (wrapping), in order, and dropped. Deletes do not stop a
/// combination; D34 then collapses what is left of a cell to the last write. `counter`
/// tells whether `(table, family)` is a counter family; other families' ops are unchanged.
pub fn combine_counter_writes(
    ops: &[ModelOp],
    counter: impl Fn(&str, &str) -> bool,
) -> Vec<ModelOp> {
    let mut out: Vec<ModelOp> = Vec::with_capacity(ops.len());
    let mut latest: BTreeMap<CollapseKey<'_>, usize> = BTreeMap::new();
    for op in ops {
        let (table, row, family, qualifier, ts) = match op {
            ModelOp::Put {
                table,
                row,
                family,
                qualifier,
                ts,
                ..
            }
            | ModelOp::Incr {
                table,
                row,
                family,
                qualifier,
                ts,
                ..
            } if counter(table, family) => (table, row, family, qualifier, ts),
            _ => {
                out.push(op.clone());
                continue;
            }
        };
        let key = (
            table.as_str(),
            row.as_slice(),
            family.as_str(),
            qualifier.as_slice(),
            ts.unwrap_or(COUNTER_TS),
        );
        if let ModelOp::Incr { delta, .. } = op
            && let Some(&i) = latest.get(&key)
        {
            match &mut out[i] {
                ModelOp::Incr { delta: d, .. } => *d = d.wrapping_add(*delta),
                ModelOp::Put { value, .. } => {
                    let base = as_i64(value).unwrap_or(0);
                    *value = base.wrapping_add(*delta).to_le_bytes().to_vec();
                }
                _ => unreachable!("only puts and increments are combined into"),
            }
            continue;
        }
        latest.insert(key, out.len());
        out.push(op.clone());
    }
    out
}

/// `(row, family, qualifier, ts)` of a column-level mutation, borrowed.
type CollapseKey<'a> = (&'a str, &'a [u8], &'a str, &'a [u8], Timestamp);

/// The collapse key of a column-level mutation: `(row, family, qualifier, ts)`. Family and row
/// markers live in their own key space and never collapse with column entries.
/// `default_ts` is where a put or operand without a timestamp lands.
fn column_key(
    op: &ModelOp,
    commit_ts: Timestamp,
    default_ts: Timestamp,
) -> Option<CollapseKey<'_>> {
    Some(match op {
        ModelOp::Put {
            table,
            row,
            family,
            qualifier,
            ts,
            ..
        }
        | ModelOp::Incr {
            table,
            row,
            family,
            qualifier,
            ts,
            ..
        } => (table, row, family, qualifier, ts.unwrap_or(default_ts)),
        ModelOp::DeleteColumn {
            table,
            row,
            family,
            qualifier,
        } => (table, row, family, qualifier, commit_ts),
        ModelOp::DeleteCell {
            table,
            row,
            family,
            qualifier,
            ts,
        } => (table, row, family, qualifier, *ts),
        ModelOp::DeleteFamily { .. } | ModelOp::DeleteRow { .. } => return None,
    })
    .map(|(t, r, f, q, ts)| (t.as_str(), r.as_slice(), f.as_str(), q.as_slice(), ts))
}

/// A merge base as an `i64`, or `None` if it is not 8 bytes.
fn as_i64(bytes: &[u8]) -> Option<i64> {
    <[u8; 8]>::try_from(bytes).ok().map(i64::from_le_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fam(name: &str, max_versions: u32, ttl_micros: u64, i64_add: bool) -> ModelFamily {
        ModelFamily {
            name: name.into(),
            max_versions,
            ttl_micros,
            i64_add,
            counter: false,
        }
    }

    /// A counter family (D179).
    fn sum_fam(name: &str, max_versions: u32, ttl_micros: u64) -> ModelFamily {
        ModelFamily {
            counter: true,
            ..fam(name, max_versions, ttl_micros, false)
        }
    }

    fn put(row: &str, q: &str, ts: Option<u64>, v: &str) -> ModelOp {
        ModelOp::Put {
            table: "t".into(),
            row: row.into(),
            family: "f".into(),
            qualifier: q.into(),
            ts,
            value: v.into(),
        }
    }

    fn incr(row: &str, d: i64) -> ModelOp {
        ModelOp::Incr {
            table: "t".into(),
            row: row.into(),
            family: "c".into(),
            qualifier: b"n".to_vec(),
            ts: None,
            delta: d,
        }
    }

    fn model() -> Model {
        let mut m = Model::new();
        m.create_table(
            "t",
            vec![
                fam("f", 0, 0, false),
                fam("c", 0, 0, true),
                fam("g", 2, 0, false),
            ],
        );
        m
    }

    fn values(m: &Model, row: &str, versions: u32, snap: Seqno, now: u64) -> Vec<(u64, String)> {
        m.read_row("t", row.as_bytes(), &["f"], versions, snap, now)
            .into_iter()
            .map(|c| (c.ts, String::from_utf8(c.value).unwrap()))
            .collect()
    }

    fn purge_all(m: &mut Model, family: &str, snapshots: Vec<Seqno>) {
        let now = 1_000;
        m.purge(&ModelPurge {
            table: "t".into(),
            family: family.into(),
            rows: (Bound::Unbounded, Bound::Unbounded),
            snapshots,
            now,
            min_ts_above: u64::MAX,
            max_seqno: m.snapshot(),
        });
    }

    #[test]
    fn purge_follows_hbase_semantics() {
        // A cell delete hides a later put at its timestamp until it is purged.
        let mut m = model();
        m.commit(&[put("r", "q", Some(5), "a")], 10, Durability::Sync);
        let del = ModelOp::DeleteCell {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "f".into(),
            qualifier: b"q".to_vec(),
            ts: 5,
        };
        m.commit(std::slice::from_ref(&del), 20, Durability::Sync);
        // A snapshot that still sees the put keeps the delete.
        purge_all(&mut m, "f", vec![1]);
        assert_eq!(values(&m, "r", 0, 1, 100), [(5, "a".into())]);
        assert!(values(&m, "r", 0, 2, 100).is_empty());
        // With no snapshot, the delete and the put it hides go; a later put at 5 shows.
        purge_all(&mut m, "f", vec![]);
        m.commit(&[put("r", "q", Some(5), "later")], 30, Durability::Sync);
        assert_eq!(values(&m, "r", 0, 3, 100), [(5, "later".into())]);

        // Versions beyond max_versions go, so deleting the newest leaves nothing.
        m.commit(&[put("r", "v", Some(1), "x")], 40, Durability::Sync);
        let g = |m: &Model, s| m.read_row("t", b"r", &["g"], 0, s, 100);
        let gput = |ts, v: &str| ModelOp::Put {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "g".into(),
            qualifier: b"q".to_vec(),
            ts: Some(ts),
            value: v.into(),
        };
        for (i, ts) in [10, 20, 30].into_iter().enumerate() {
            m.commit(&[gput(ts, "v")], 50 + i as u64, Durability::Sync);
        }
        purge_all(&mut m, "g", vec![]);
        let del = ModelOp::DeleteCell {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "g".into(),
            qualifier: b"q".to_vec(),
            ts: 30,
        };
        m.commit(&[del], 60, Durability::Sync);
        let s = m.snapshot();
        assert_eq!(g(&m, s).iter().map(|c| c.ts).collect::<Vec<_>>(), [20]);
    }

    /// #287: a flush's version purge runs only when no delete outside its input could hide
    /// one of the versions it counts.
    #[test]
    fn purge_versions_respects_the_flush_guard() {
        let gput = |ts, v: &str| ModelOp::Put {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "g".into(),
            qualifier: b"q".to_vec(),
            ts: Some(ts),
            value: v.into(),
        };
        let gdel = |ts| ModelOp::DeleteCell {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "g".into(),
            qualifier: b"q".to_vec(),
            ts,
        };
        let flush = || ModelPurge {
            table: "t".into(),
            family: "g".into(),
            rows: (Bound::Unbounded, Bound::Unbounded),
            snapshots: vec![],
            now: 1_000,
            min_ts_above: 0,
            max_seqno: 0,
        };
        let g = |m: &Model| -> Vec<u64> {
            m.read_row("t", b"r", &["g"], 0, m.snapshot(), 1_000)
                .iter()
                .map(|c| c.ts)
                .collect()
        };

        // An older cell delete at 30 (outside the flush) hides the put at 30: 20 and 10 are
        // the two versions read. Counting the flushed puts alone would purge 10.
        let mut m = model();
        m.commit(&[gdel(30)], 10, Durability::Sync);
        let inputs: Vec<Seqno> = [10, 20, 30]
            .into_iter()
            .enumerate()
            .map(|(i, ts)| m.commit(&[gput(ts, "v")], 20 + i as u64, Durability::Sync))
            .collect();
        assert_eq!(g(&m), [20, 10]);
        m.purge_versions(&flush(), &inputs, 0);
        assert_eq!(
            g(&m),
            [20, 10],
            "the guard keeps the version the delete shows"
        );

        // Without it the flush purges 10; a later delete of 30 shows nothing older (D70).
        let mut m = model();
        let inputs: Vec<Seqno> = [10, 20, 30]
            .into_iter()
            .enumerate()
            .map(|(i, ts)| m.commit(&[gput(ts, "v")], 20 + i as u64, Durability::Sync))
            .collect();
        m.purge_versions(&flush(), &inputs, 0);
        assert_eq!(g(&m), [30, 20]);
        m.commit(&[gdel(30)], 40, Durability::Sync);
        assert_eq!(g(&m), [20]);
    }

    #[test]
    fn versions_newest_first_and_snapshots() {
        let mut m = model();
        m.commit(&[put("r", "q", None, "a")], 10, Durability::Sync);
        m.commit(&[put("r", "q", None, "b")], 20, Durability::Sync);
        m.commit(&[put("r", "q", Some(5), "old")], 30, Durability::Sync);
        let all = values(&m, "r", 0, 3, 100);
        assert_eq!(all, [(20, "b".into()), (10, "a".into()), (5, "old".into())]);
        assert_eq!(values(&m, "r", 2, 3, 100).len(), 2);
        assert_eq!(values(&m, "r", 0, 1, 100), [(10, "a".into())]);
        assert_eq!(values(&m, "r", 0, 0, 100), []);
    }

    #[test]
    fn same_timestamp_highest_seqno_wins() {
        let mut m = model();
        m.commit(&[put("r", "q", Some(7), "a")], 10, Durability::Sync);
        m.commit(&[put("r", "q", Some(7), "b")], 11, Durability::Sync);
        assert_eq!(values(&m, "r", 0, 2, 100), [(7, "b".into())]);
    }

    #[test]
    fn max_versions_applies_per_column() {
        let mut m = model();
        for i in 1..=4 {
            let op = ModelOp::Put {
                table: "t".into(),
                row: b"r".to_vec(),
                family: "g".into(),
                qualifier: b"q".to_vec(),
                ts: None,
                value: vec![i],
            };
            m.commit(&[op], u64::from(i) * 10, Durability::Sync);
        }
        let cells = m.read_row("t", b"r", &[], 0, 4, 100);
        assert_eq!(cells.iter().map(|c| c.ts).collect::<Vec<_>>(), [40, 30]);
    }

    #[test]
    fn ttl_expires_at_boundary() {
        let mut m = Model::new();
        m.create_table("t", vec![fam("f", 0, 100, false)]);
        m.commit(&[put("r", "q", None, "a")], 1000, Durability::Sync);
        assert!(m.get("t", b"r", "f", b"q", 1, 1099).is_some());
        assert!(m.get("t", b"r", "f", b"q", 1, 1100).is_none());
        assert_eq!(
            m.scan("t", Bound::Unbounded, Bound::Unbounded, &[], 1, 1100),
            []
        );
    }

    #[test]
    fn cell_delete_hits_exact_timestamp_only() {
        let mut m = model();
        m.commit(
            &[put("r", "q", Some(5), "a"), put("r", "q", Some(6), "b")],
            10,
            Durability::Sync,
        );
        let del = ModelOp::DeleteCell {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "f".into(),
            qualifier: b"q".to_vec(),
            ts: 6,
        };
        m.commit(&[del], 11, Durability::Sync);
        assert_eq!(values(&m, "r", 0, 2, 100), [(5, "a".into())]);
        assert_eq!(values(&m, "r", 0, 1, 100).len(), 2);
        // Timestamp-only (D38): a later put at the deleted timestamp stays hidden...
        m.commit(&[put("r", "q", Some(6), "c")], 12, Durability::Sync);
        assert_eq!(values(&m, "r", 0, 3, 100), [(5, "a".into())]);
        // ...while one at another timestamp is visible, and a snapshot from before the
        // delete still sees the original version.
        m.commit(&[put("r", "q", Some(7), "d")], 13, Durability::Sync);
        assert_eq!(
            values(&m, "r", 0, 4, 100),
            [(7, "d".into()), (5, "a".into())]
        );
        assert_eq!(values(&m, "r", 0, 1, 100)[0], (6, "b".into()));
    }

    #[test]
    fn cell_delete_hides_operands_at_its_timestamp() {
        let mut m = model();
        let del = ModelOp::DeleteCell {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "c".into(),
            qualifier: b"n".to_vec(),
            ts: 20,
        };
        m.commit(&[incr("r", 1)], 10, Durability::Sync);
        m.commit(&[del], 15, Durability::Sync);
        m.commit(&[incr("r", 5)], 20, Durability::Sync);
        let v = m.get("t", b"r", "c", b"n", 3, 100).unwrap();
        assert_eq!((v.ts, v.value), (10, 1i64.to_le_bytes().to_vec()));
    }

    #[test]
    fn families_in_creation_or_requested_order() {
        let mut m = Model::new();
        m.create_table("t", vec![fam("z", 0, 0, false), fam("a", 0, 0, false)]);
        m.create_table("t", vec![fam("m", 0, 0, false), fam("z", 0, 0, false)]);
        let p = |family: &str, q: &str| ModelOp::Put {
            table: "t".into(),
            row: b"r".to_vec(),
            family: family.into(),
            qualifier: q.into(),
            ts: None,
            value: b"v".to_vec(),
        };
        m.commit(
            &[p("a", "2"), p("a", "1"), p("m", "x"), p("z", "y")],
            10,
            Durability::Sync,
        );
        let order = |families: &[&str]| {
            m.read_row("t", b"r", families, 0, 1, 100)
                .into_iter()
                .map(|c| format!("{}:{}", c.family, String::from_utf8(c.qualifier).unwrap()))
                .collect::<Vec<_>>()
        };
        assert_eq!(order(&[]), ["z:y", "a:1", "a:2", "m:x"]);
        assert_eq!(order(&["m", "a", "m", "nope"]), ["m:x", "a:1", "a:2"]);
        let scanned = m.scan("t", Bound::Unbounded, Bound::Unbounded, &["a", "z"], 1, 100);
        let fams: Vec<_> = scanned[0].1.iter().map(|c| c.family.as_str()).collect();
        assert_eq!(fams, ["a", "a", "z"]);
    }

    #[test]
    fn column_delete_hides_by_timestamp_not_seqno() {
        let mut m = model();
        m.commit(&[put("r", "q", Some(5), "a")], 10, Durability::Sync);
        let del = ModelOp::DeleteColumn {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "f".into(),
            qualifier: b"q".to_vec(),
        };
        m.commit(&[del], 20, Durability::Sync);
        // Later seqno, older-than-marker timestamp: still hidden. Newer timestamp: visible.
        m.commit(&[put("r", "q", Some(15), "hidden")], 21, Durability::Sync);
        m.commit(&[put("r", "q", Some(25), "shown")], 22, Durability::Sync);
        assert_eq!(values(&m, "r", 0, 4, 100), [(25, "shown".into())]);
        assert_eq!(values(&m, "r", 0, 1, 100), [(5, "a".into())]);
    }

    #[test]
    fn family_and_row_deletes() {
        let mut m = model();
        let other = ModelOp::Put {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "g".into(),
            qualifier: b"q".to_vec(),
            ts: None,
            value: b"x".to_vec(),
        };
        m.commit(
            &[put("r", "a", None, "1"), put("r", "b", None, "2"), other],
            10,
            Durability::Sync,
        );
        let fd = ModelOp::DeleteFamily {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "f".into(),
        };
        m.commit(&[fd], 20, Durability::Sync);
        assert_eq!(values(&m, "r", 0, 2, 100), []);
        assert_eq!(m.read_row("t", b"r", &["g"], 0, 2, 100).len(), 1);
        let rd = ModelOp::DeleteRow {
            table: "t".into(),
            row: b"r".to_vec(),
        };
        m.commit(&[rd], 30, Durability::Sync);
        assert_eq!(m.read_row("t", b"r", &[], 0, 3, 100), []);
        assert_eq!(m.read_row("t", b"r", &[], 0, 2, 100).len(), 1);
    }

    #[test]
    fn merge_folds_into_base_and_wraps() {
        let mut m = model();
        let base = ModelOp::Put {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "c".into(),
            qualifier: b"n".to_vec(),
            ts: None,
            value: 40i64.to_le_bytes().to_vec(),
        };
        m.commit(&[incr("r", 1)], 5, Durability::Sync);
        m.commit(&[base], 10, Durability::Sync);
        m.commit(&[incr("r", 1)], 20, Durability::Sync);
        m.commit(&[incr("r", 1)], 20, Durability::Sync);
        let n = |snap| {
            m.get("t", b"r", "c", b"n", snap, 100)
                .map(|c| (c.ts, i64::from_le_bytes(c.value.try_into().unwrap())))
        };
        assert_eq!(n(1), Some((5, 1)));
        assert_eq!(n(2), Some((10, 40)));
        assert_eq!(n(3), Some((20, 41)));
        assert_eq!(n(4), Some((20, 42)));
        let mut w = model();
        w.commit(&[incr("r", i64::MAX)], 1, Durability::Sync);
        w.commit(&[incr("r", 1)], 2, Durability::Sync);
        let v = w.get("t", b"r", "c", b"n", 2, 2).unwrap().value;
        assert_eq!(i64::from_le_bytes(v.try_into().unwrap()), i64::MIN);
    }

    #[test]
    fn merge_onto_a_non_i64_base_fails() {
        let mut m = model();
        let base = ModelOp::Put {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "c".into(),
            qualifier: b"n".to_vec(),
            ts: None,
            value: b"abc".to_vec(),
        };
        m.commit(&[base], 10, Durability::Sync);
        // No operand on top: the put is returned as written.
        assert_eq!(m.get("t", b"r", "c", b"n", 1, 100).unwrap().value, b"abc");
        m.commit(&[incr("r", 1)], 20, Durability::Sync);
        let failed = Err(ModelError::MergeFailed("c".into()));
        assert_eq!(m.try_get("t", b"r", "c", b"n", 2, 100), failed);
        assert_eq!(
            m.try_read_row("t", b"r", &[], 0, 2, 100).map(|_| ()),
            Err(ModelError::MergeFailed("c".into()))
        );
        assert_eq!(
            m.try_scan("t", Bound::Unbounded, Bound::Unbounded, &[], 2, 100)
                .map(|_| ()),
            Err(ModelError::MergeFailed("c".into()))
        );
        // A newer plain put shadows the fold when only the newest version is read.
        m.commit(&[put_c(30, 7)], 30, Durability::Sync);
        let v = m.try_get("t", b"r", "c", b"n", 3, 100).unwrap().unwrap();
        assert_eq!((v.ts, v.value), (30, 7i64.to_le_bytes().to_vec()));
        assert!(m.try_read_row("t", b"r", &[], 0, 3, 100).is_err());
    }

    #[test]
    #[should_panic(expected = "merge failed")]
    fn panicking_read_reports_a_failed_merge() {
        let mut m = model();
        m.commit(&[put_c(10, 0)], 10, Durability::Sync);
        let mut bad = put_c(10, 0);
        if let ModelOp::Put { value, .. } = &mut bad {
            value.truncate(3);
        }
        m.commit(&[bad], 11, Durability::Sync);
        m.commit(&[incr("r", 1)], 20, Durability::Sync);
        let _ = m.get("t", b"r", "c", b"n", 3, 100);
    }

    fn put_c(ts: u64, v: i64) -> ModelOp {
        ModelOp::Put {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "c".into(),
            qualifier: b"n".to_vec(),
            ts: Some(ts),
            value: v.to_le_bytes().to_vec(),
        }
    }

    #[test]
    fn scan_bounds_order_and_inverted_range() {
        let mut m = model();
        for r in ["a", "b", "c", "d"] {
            m.commit(&[put(r, "q", None, r)], 10, Durability::Sync);
        }
        let rows = |s, e| {
            m.scan("t", s, e, &[], 4, 100)
                .into_iter()
                .map(|(r, _)| String::from_utf8(r).unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            rows(Bound::Included(b"b"), Bound::Excluded(b"d")),
            ["b", "c"]
        );
        assert_eq!(
            rows(Bound::Unbounded, Bound::Unbounded),
            ["a", "b", "c", "d"]
        );
        assert_eq!(
            rows(Bound::Included(b"c"), Bound::Excluded(b"b")),
            Vec::<String>::new()
        );
        assert_eq!(
            rows(Bound::Excluded(b"c"), Bound::Excluded(b"c")),
            Vec::<String>::new()
        );
    }

    #[test]
    fn crash_windows_by_durability() {
        let mut m = model();
        let w = |m: &Model, k| m.crash_window(k);
        assert_eq!(
            w(&m, CrashKind::Power),
            CrashWindow {
                must_survive: 0,
                may_survive: 0
            }
        );
        m.commit(&[put("r", "q", None, "1")], 1, Durability::None); // 1
        m.commit(&[put("r", "q", None, "2")], 2, Durability::Buffered); // 2
        m.commit(&[put("r", "q", None, "3")], 3, Durability::None); // 3
        assert_eq!(
            w(&m, CrashKind::Process),
            CrashWindow {
                must_survive: 2,
                may_survive: 3
            }
        );
        assert_eq!(
            w(&m, CrashKind::Power),
            CrashWindow {
                must_survive: 0,
                may_survive: 3
            }
        );
        m.commit(&[put("r", "q", None, "4")], 4, Durability::GroupSync); // 4
        m.commit(&[put("r", "q", None, "5")], 5, Durability::Buffered); // 5
        assert_eq!(
            w(&m, CrashKind::Power),
            CrashWindow {
                must_survive: 4,
                may_survive: 5
            }
        );
        assert_eq!(
            w(&m, CrashKind::Process),
            CrashWindow {
                must_survive: 5,
                may_survive: 5
            }
        );
    }

    #[test]
    fn truncate_and_recover() {
        let mut m = model();
        m.commit(&[put("r", "q", None, "1")], 1, Durability::Buffered);
        m.commit(&[put("r", "q", None, "2")], 2, Durability::Buffered);
        m.commit(&[put("s", "q", None, "3")], 3, Durability::Buffered);
        m.recover(CrashKind::Power, 2);
        assert_eq!(m.snapshot(), 2);
        assert_eq!(
            m.scan("t", Bound::Unbounded, Bound::Unbounded, &[], 9, 9)
                .len(),
            1
        );
        // Survivors of a power loss are on disk now.
        assert_eq!(m.crash_window(CrashKind::Power).must_survive, 2);
        // New commits continue from the truncated seqno.
        assert_eq!(
            m.commit(&[put("s", "q", None, "4")], 4, Durability::None),
            3
        );
    }

    fn counter_put(row: &str, ts: Option<u64>, v: i64) -> ModelOp {
        ModelOp::Put {
            table: "t".into(),
            row: row.into(),
            family: "c".into(),
            qualifier: b"n".to_vec(),
            ts,
            value: v.to_le_bytes().to_vec(),
        }
    }

    fn counter(m: &Model, row: &str, snap: Seqno) -> Vec<(u64, i64)> {
        m.read_row("t", row.as_bytes(), &["c"], 0, snap, 1_000)
            .into_iter()
            .map(|c| (c.ts, i64::from_le_bytes(c.value.try_into().unwrap())))
            .collect()
    }

    #[test]
    fn merge_and_put_at_one_timestamp_are_one_version() {
        // Operand older than the put: shadowed by it.
        let mut m = model();
        m.commit(&[incr("r", 1)], 10, Durability::Sync);
        m.commit(&[counter_put("r", Some(10), 5)], 11, Durability::Sync);
        assert_eq!(counter(&m, "r", 2), [(10, 5)]);
        // Operand newer than the put: folds onto it.
        let mut m = model();
        m.commit(&[counter_put("r", Some(10), 5)], 9, Durability::Sync);
        m.commit(&[incr("r", 1)], 10, Durability::Sync);
        assert_eq!(counter(&m, "r", 2), [(10, 6)]);
        assert_eq!(counter(&m, "r", 1), [(10, 5)]);
    }

    #[test]
    fn same_timestamp_newest_put_is_the_base() {
        let mut m = model();
        m.commit(&[counter_put("r", Some(10), 100)], 1, Durability::Sync); // 1: shadowed
        m.commit(&[incr("r", 1000)], 10, Durability::Sync); // 2: shadowed (older than put 3)
        m.commit(&[counter_put("r", Some(10), 5)], 10, Durability::Sync); // 3: base
        m.commit(&[incr("r", 7)], 10, Durability::Sync); // 4: folds
        assert_eq!(counter(&m, "r", 4), [(10, 12)]);
        assert_eq!(counter(&m, "r", 3), [(10, 5)]);
        assert_eq!(counter(&m, "r", 2), [(10, 1100)]);
    }

    #[test]
    fn operand_run_folds_onto_older_timestamp_base_and_same_ts_operands() {
        let mut m = model();
        m.commit(&[counter_put("r", Some(5), 40)], 5, Durability::Sync);
        m.commit(&[incr("r", 1)], 8, Durability::Sync);
        m.commit(&[incr("r", 2)], 10, Durability::Sync);
        // Operands at 10 and 8 fold onto the base at 5: one version at the newest operand's ts.
        assert_eq!(counter(&m, "r", 3), [(10, 43)]);
        // A put at 12 starts a new version above the run.
        m.commit(&[counter_put("r", Some(12), 1)], 12, Durability::Sync);
        assert_eq!(counter(&m, "r", 4), [(12, 1), (10, 43)]);
    }

    #[test]
    fn same_commit_mutations_collapse_to_the_last_write() {
        let mut m = model();
        // Two puts at one column and timestamp: the last one is the cell.
        m.commit(
            &[put("r", "q", Some(7), "a"), put("r", "q", Some(7), "b")],
            10,
            Durability::Sync,
        );
        assert_eq!(values(&m, "r", 0, 1, 100), [(7, "b".into())]);
        // Put (default ts) then incr at the commit ts: only the incr survives, no base.
        let mut m = model();
        m.commit(
            &[counter_put("r", None, 50), incr("r", 3)],
            10,
            Durability::Sync,
        );
        assert_eq!(counter(&m, "r", 1), [(10, 3)]);
        // Incr then put: only the put survives.
        let mut m = model();
        m.commit(
            &[incr("r", 3), counter_put("r", None, 50)],
            10,
            Durability::Sync,
        );
        assert_eq!(counter(&m, "r", 1), [(10, 50)]);
        // Different timestamps in one commit do not collapse.
        let mut m = model();
        m.commit(
            &[put("r", "q", Some(1), "a"), put("r", "q", Some(2), "b")],
            10,
            Durability::Sync,
        );
        assert_eq!(values(&m, "r", 0, 1, 100).len(), 2);
        // A column delete and a put at the commit timestamp are the same key: last wins.
        let del = ModelOp::DeleteColumn {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "f".into(),
            qualifier: b"q".to_vec(),
        };
        let mut m = model();
        m.commit(&[put("r", "q", Some(3), "old")], 5, Durability::Sync);
        m.commit(
            &[del.clone(), put("r", "q", None, "new")],
            10,
            Durability::Sync,
        );
        assert_eq!(
            values(&m, "r", 0, 2, 100),
            [(10, "new".into()), (3, "old".into())]
        );
        let mut m = model();
        m.commit(&[put("r", "q", Some(3), "old")], 5, Durability::Sync);
        m.commit(&[put("r", "q", None, "new"), del], 10, Durability::Sync);
        assert_eq!(values(&m, "r", 0, 2, 100), []);
    }

    #[test]
    fn family_marker_hides_same_commit_puts_at_or_below_its_timestamp() {
        let fd = ModelOp::DeleteFamily {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "f".into(),
        };
        for ops in [
            vec![put("r", "q", Some(5), "x"), fd.clone()],
            vec![fd.clone(), put("r", "q", Some(5), "x")],
            vec![fd.clone(), put("r", "q", None, "x")],
            vec![put("r", "q", None, "x"), fd.clone()],
        ] {
            let mut m = model();
            m.commit(&ops, 10, Durability::Sync);
            assert_eq!(values(&m, "r", 0, 1, 100), [], "{ops:?}");
        }
        // A put newer than the marker survives.
        let mut m = model();
        m.commit(&[fd, put("r", "q", Some(11), "x")], 10, Durability::Sync);
        assert_eq!(values(&m, "r", 0, 1, 100), [(11, "x".into())]);
    }

    #[test]
    fn invalid_commits_are_typed_errors_and_change_nothing() {
        let mut m = model();
        let bad_incr = ModelOp::Incr {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "f".into(),
            qualifier: b"q".to_vec(),
            ts: None,
            delta: 1,
        };
        let ok = put("r", "q", None, "x");
        assert_eq!(
            m.try_commit(&[ok.clone(), bad_incr], 1, Durability::Sync),
            Err(ModelError::NoMergeOperator("f".into()))
        );
        let mut nofam = put("r", "q", None, "x");
        if let ModelOp::Put { family, .. } = &mut nofam {
            *family = "zzz".into();
        }
        assert_eq!(
            m.try_commit(&[nofam], 1, Durability::Sync),
            Err(ModelError::NoSuchFamily("zzz".into()))
        );
        let mut notable = put("r", "q", None, "x");
        if let ModelOp::Put { table, .. } = &mut notable {
            *table = "nope".into();
        }
        assert_eq!(
            m.try_commit(&[notable], 1, Durability::Sync),
            Err(ModelError::NoSuchTable("nope".into()))
        );
        assert_eq!(m.snapshot(), 0);
        assert_eq!(values(&m, "r", 0, 0, 100), []);
        assert_eq!(m.try_commit(&[ok], 1, Durability::Sync), Ok(1));
    }

    #[test]
    fn ttl_boundary_with_explicit_timestamps_and_versions() {
        let mut m = Model::new();
        m.create_table("t", vec![fam("f", 2, 100, false)]);
        // Explicit timestamps: 1000 expires at now=1100, 1050 at 1150.
        m.commit(
            &[
                put("r", "q", Some(1000), "a"),
                put("r", "q", Some(1050), "b"),
                put("r", "q", Some(1060), "c"),
            ],
            1060,
            Durability::Sync,
        );
        let at = |now| m.read_row("t", b"r", &[], 0, 1, now).len();
        // max_versions=2 keeps the newest two; expiry then shrinks it.
        assert_eq!(at(1099), 2);
        assert_eq!(at(1149), 2);
        assert_eq!(at(1150), 1);
        assert_eq!(at(1160), 0);
    }

    #[test]
    fn delete_by_timestamp_boundaries_and_max_versions_after_deletes() {
        let mut m = model();
        let col = ModelOp::DeleteColumn {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "f".into(),
            qualifier: b"q".to_vec(),
        };
        m.commit(
            &[
                put("r", "q", Some(19), "a"),
                put("r", "q", Some(20), "b"),
                put("r", "q", Some(21), "c"),
            ],
            5,
            Durability::Sync,
        );
        m.commit(&[col], 20, Durability::Sync);
        // The marker at 20 hides 19 and 20 but not 21.
        assert_eq!(values(&m, "r", 0, 2, 100), [(21, "c".into())]);
        // max_versions counts versions that survive deletes: family g keeps 2 of the rest.
        let mut m = model();
        for ts in 1..=4u64 {
            let op = ModelOp::Put {
                table: "t".into(),
                row: b"r".to_vec(),
                family: "g".into(),
                qualifier: b"q".to_vec(),
                ts: Some(ts),
                value: vec![ts as u8],
            };
            m.commit(&[op], ts, Durability::Sync);
        }
        let del = ModelOp::DeleteCell {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "g".into(),
            qualifier: b"q".to_vec(),
            ts: 4,
        };
        m.commit(&[del], 5, Durability::Sync);
        let ts: Vec<_> = m
            .read_row("t", b"r", &[], 0, 5, 100)
            .iter()
            .map(|c| c.ts)
            .collect();
        assert_eq!(ts, [3, 2]);
    }

    // ---- Counter families (D179) ----

    fn sum_model() -> Model {
        let mut m = Model::new();
        m.create_table(
            "t",
            vec![
                sum_fam("s", 0, 0),
                sum_fam("v", 2, 0),
                sum_fam("ttl", 0, 100),
            ],
        );
        m
    }

    fn sum_incr(family: &str, ts: Option<u64>, d: i64) -> ModelOp {
        ModelOp::Incr {
            table: "t".into(),
            row: b"r".to_vec(),
            family: family.into(),
            qualifier: b"n".to_vec(),
            ts,
            delta: d,
        }
    }

    fn sum_put(family: &str, ts: Option<u64>, v: &[u8]) -> ModelOp {
        ModelOp::Put {
            table: "t".into(),
            row: b"r".to_vec(),
            family: family.into(),
            qualifier: b"n".to_vec(),
            ts,
            value: v.to_vec(),
        }
    }

    fn sums(m: &Model, family: &str, snap: Seqno, now: u64) -> Vec<(u64, i64)> {
        m.read_row("t", b"r", &[family], 0, snap, now)
            .into_iter()
            .map(|c| (c.ts, i64::from_le_bytes(c.value.try_into().unwrap())))
            .collect()
    }

    #[test]
    fn counter_increments_share_the_fixed_timestamp() {
        let mut m = sum_model();
        m.commit(&[sum_incr("s", None, 1)], 10, Durability::Sync);
        m.commit(&[sum_incr("s", None, 2)], 20, Durability::Sync);
        assert_eq!(sums(&m, "s", 2, 30), [(COUNTER_TS, 3)]);
        assert_eq!(sums(&m, "s", 1, 30), [(COUNTER_TS, 1)]);
        // put_i64 sets it; later increments add to it.
        m.commit(
            &[sum_put("s", None, &10i64.to_le_bytes())],
            30,
            Durability::Sync,
        );
        m.commit(&[sum_incr("s", None, 5)], 40, Durability::Sync);
        assert_eq!(sums(&m, "s", 4, 50), [(COUNTER_TS, 15)]);
        assert_eq!(sums(&m, "s", 3, 50), [(COUNTER_TS, 10)]);
    }

    #[test]
    fn counter_buckets_are_versions() {
        let mut m = sum_model();
        for (ts, d) in [(100, 1), (200, 2), (100, 3), (300, 4)] {
            m.commit(&[sum_incr("s", Some(ts), d)], 1_000, Durability::Sync);
            m.commit(&[sum_incr("v", Some(ts), d)], 1_000, Durability::Sync);
        }
        assert_eq!(sums(&m, "s", 8, 1_000), [(300, 4), (200, 2), (100, 4)]);
        // max_versions applies per bucket.
        assert_eq!(sums(&m, "v", 8, 1_000), [(300, 4), (200, 2)]);
        // A bucket set with put_i64 at its timestamp.
        m.commit(
            &[sum_put("s", Some(200), &7i64.to_le_bytes())],
            1_000,
            Durability::Sync,
        );
        assert_eq!(sums(&m, "s", 9, 1_000), [(300, 4), (200, 7), (100, 4)]);
    }

    #[test]
    fn counter_deletes_hide_only_older_writes() {
        let column = |q: &[u8]| ModelOp::DeleteColumn {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "s".into(),
            qualifier: q.to_vec(),
        };
        let cell = |ts| ModelOp::DeleteCell {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "s".into(),
            qualifier: b"n".to_vec(),
            ts,
        };
        let family = ModelOp::DeleteFamily {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "s".into(),
        };
        let row = ModelOp::DeleteRow {
            table: "t".into(),
            row: b"r".to_vec(),
        };
        for del in [column(b"n"), cell(COUNTER_TS), family, row] {
            let mut m = sum_model();
            m.commit(&[sum_incr("s", None, 5)], 10, Durability::Sync);
            m.commit(std::slice::from_ref(&del), 20, Durability::Sync);
            assert_eq!(sums(&m, "s", 2, 30), [], "{del:?}");
            m.commit(&[sum_incr("s", None, 3)], 30, Durability::Sync);
            assert_eq!(sums(&m, "s", 3, 40), [(COUNTER_TS, 3)], "{del:?}");
            assert_eq!(sums(&m, "s", 1, 40), [(COUNTER_TS, 5)], "{del:?}");
            // Within one commit the delete hides nothing written with it.
            m.commit(
                &[sum_incr("s", Some(5), 1), del.clone()],
                40,
                Durability::Sync,
            );
            assert_eq!(sums(&m, "s", 4, 50), [(5, 1)], "{del:?}");
            // Purges never change a counter family's reads.
            let before = sums(&m, "s", 4, 50);
            purge_all(&mut m, "s", vec![]);
            assert_eq!(sums(&m, "s", 4, 50), before);
        }
        // A cell delete at one bucket leaves the others.
        let mut m = sum_model();
        m.commit(&[sum_incr("s", Some(5), 1)], 10, Durability::Sync);
        m.commit(&[sum_incr("s", Some(6), 2)], 10, Durability::Sync);
        m.commit(&[cell(5)], 20, Durability::Sync);
        assert_eq!(sums(&m, "s", 3, 30), [(6, 2)]);
    }

    #[test]
    fn counter_ttl_applies_per_bucket_and_refuses_the_fixed_timestamp() {
        let mut m = sum_model();
        for bad in [
            sum_incr("ttl", None, 1),
            sum_put("ttl", None, &1i64.to_le_bytes()),
            sum_put("s", None, b"bytes"),
            sum_put("s", Some(3), b"bytes"),
        ] {
            assert_eq!(
                m.try_commit(std::slice::from_ref(&bad), 10, Durability::Sync),
                Err(ModelError::CounterWrite(match &bad {
                    ModelOp::Incr { family, .. } | ModelOp::Put { family, .. } => family.clone(),
                    _ => unreachable!(),
                })),
                "{bad:?}"
            );
        }
        assert_eq!(m.snapshot(), 0, "refused commits change nothing");
        m.commit(&[sum_incr("ttl", Some(100), 1)], 10, Durability::Sync);
        m.commit(&[sum_incr("ttl", Some(150), 2)], 10, Durability::Sync);
        assert_eq!(sums(&m, "ttl", 2, 199), [(150, 2), (100, 1)]);
        assert_eq!(sums(&m, "ttl", 2, 200), [(150, 2)]);
        // A bucket timestamp is for counter families only.
        let mut c = model();
        let mut at = incr("r", 1);
        if let ModelOp::Incr { ts, .. } = &mut at {
            *ts = Some(5);
        }
        assert_eq!(
            c.try_commit(&[at], 10, Durability::Sync),
            Err(ModelError::CounterWrite("c".into()))
        );
    }

    #[test]
    fn counter_writes_in_one_commit_combine() {
        let mut m = sum_model();
        m.commit(
            &[sum_incr("s", None, 1), sum_incr("s", None, 2)],
            10,
            Durability::Sync,
        );
        assert_eq!(sums(&m, "s", 1, 20), [(COUNTER_TS, 3)]);
        let five = 5i64.to_le_bytes();
        m.commit(
            &[sum_put("s", None, &five), sum_incr("s", None, 1)],
            20,
            Durability::Sync,
        );
        assert_eq!(sums(&m, "s", 2, 30), [(COUNTER_TS, 6)]);
        // A put after an increment sets the cell (D34: the last write of a cell wins).
        m.commit(
            &[sum_incr("s", None, 7), sum_put("s", None, &five)],
            30,
            Durability::Sync,
        );
        assert_eq!(sums(&m, "s", 3, 40), [(COUNTER_TS, 5)]);
        // Buckets combine on their own; `None` and `Some(COUNTER_TS)` are one cell.
        m.commit(
            &[
                sum_incr("s", Some(100), 1),
                sum_incr("s", Some(COUNTER_TS), 2),
                sum_incr("s", Some(100), 4),
                sum_incr("s", None, 8),
            ],
            40,
            Durability::Sync,
        );
        assert_eq!(sums(&m, "s", 4, 50), [(100, 5), (COUNTER_TS, 15)]);
        // A delete in the commit does not split a combination, and hides only earlier
        // commits (D186).
        let del = ModelOp::DeleteColumn {
            table: "t".into(),
            row: b"r".to_vec(),
            family: "s".into(),
            qualifier: b"n".to_vec(),
        };
        m.commit(
            &[sum_incr("s", None, 1), del, sum_incr("s", None, 2)],
            50,
            Durability::Sync,
        );
        assert_eq!(sums(&m, "s", 5, 60), [(100, 5), (COUNTER_TS, 3)]);
        // Other families keep D34: the last write of a cell wins.
        let mut c = model();
        c.commit(&[incr("r", 1), incr("r", 2)], 10, Durability::Sync);
        assert_eq!(counter(&c, "r", 1), [(10, 2)]);
    }
}
