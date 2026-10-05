use std::collections::BTreeMap;
use std::fmt;
use std::ops::Bound;

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
    /// Whether merge operands use the built-in `i64` add operator.
    pub i64_add: bool,
}

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
}

impl fmt::Display for ModelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoSuchTable(t) => write!(f, "no such table {t:?}"),
            Self::NoSuchFamily(x) => write!(f, "no such family {x:?}"),
            Self::NoMergeOperator(x) => write!(f, "family {x:?} has no merge operator"),
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
    /// A `CellDelete`: hides earlier-written versions with exactly this timestamp.
    CellDelete,
    /// A `ColumnDelete`: hides every version with `ts <=` this timestamp, any seqno.
    ColumnDelete,
}

/// `(family, qualifier)` within a row.
type ColumnKey = (String, Vec<u8>);

/// `(ts, seqno)` of a family-in-row marker.
type Marker = (Timestamp, Seqno);

#[derive(Debug, Default)]
struct Table {
    families: BTreeMap<String, ModelFamily>,
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
/// - **Same timestamp across commits.** At one timestamp the newest-seqno put is the
///   version's base; merge operands with a newer seqno fold onto it; every older entry at
///   that timestamp is shadowed. A timestamp with operands only folds them into the run
///   described under `Incr` below.
/// - `DeleteColumn` and `DeleteFamily` (and `DeleteRow`, one family marker per family) take
///   the commit timestamp `T` and hide every version in scope with timestamp `<= T`,
///   whatever its seqno, including a put in the same commit with timestamp `<= T` and a later
///   put with an older timestamp. `DeleteCell` hides the versions at exactly its timestamp
///   with an older seqno.
/// - TTL: a version is expired when `ts + ttl_micros <= now` (timestamps are microseconds).
///   Expired versions are dropped before merge operands are folded and versions counted.
/// - `Incr` (only on `i64_add` families, else [`ModelError::NoMergeOperator`]) is a merge
///   operand at the commit timestamp. Going newest to oldest, a run of operands folds into
///   one cell at the newest operand's timestamp: its value is the wrapping sum of the
///   operands plus the `i64` in the next older put, which the fold consumes (a put value
///   that is not 8 bytes counts as 0). Without a base the sum is the value.
/// - `max_versions` keeps the newest N resolved versions (after deletes, TTL and folding).
/// - Reads order cells by family name, then qualifier, then timestamp descending.
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
            table.families.insert(f.name.clone(), f);
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
        self.commits.push(durability);
        let seqno = self.commits.len() as Seqno;
        // Keep only the last column-level mutation per (row, family, qualifier, ts).
        let mut last: BTreeMap<CollapseKey<'_>, usize> = BTreeMap::new();
        for (i, op) in ops.iter().enumerate() {
            if let Some(key) = column_key(op, commit_ts) {
                last.insert(key, i);
            }
        }
        for (i, op) in ops.iter().enumerate() {
            let collapsed = column_key(op, commit_ts).is_some_and(|k| last[&k] != i);
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
        if matches!(op, ModelOp::Incr { .. }) && !f.i64_add {
            return Err(ModelError::NoMergeOperator(family.clone()));
        }
        Ok(())
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
                ts.unwrap_or(commit_ts),
                Kind::Put(value.clone()),
            ),
            ModelOp::Incr {
                family,
                qualifier,
                delta,
                ..
            } => {
                column(family, qualifier, commit_ts, Kind::Merge(*delta));
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
    pub fn get(
        &self,
        table: &str,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        snapshot: Seqno,
        now: Timestamp,
    ) -> Option<ModelCell> {
        let t = self.tables.get(table)?;
        let fam = t.families.get(family)?;
        let entries = t
            .columns
            .get(row)?
            .get(&(family.to_owned(), qualifier.to_vec()))?;
        let markers = t.family_deletes.get(&(row.to_vec(), family.to_owned()));
        let mut versions = resolve(fam, entries, markers, snapshot, now, 1);
        let (ts, value) = versions.pop()?;
        Some(ModelCell {
            family: family.to_owned(),
            qualifier: qualifier.to_vec(),
            ts,
            value,
        })
    }

    /// Up to `versions` visible versions of every cell of `row` in `families` (all if empty).
    /// `versions == 0` means every visible version.
    pub fn read_row(
        &self,
        table: &str,
        row: &[u8],
        families: &[&str],
        versions: u32,
        snapshot: Seqno,
        now: Timestamp,
    ) -> Vec<ModelCell> {
        let Some(t) = self.tables.get(table) else {
            return Vec::new();
        };
        let Some(columns) = t.columns.get(row) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for ((family, qualifier), entries) in columns {
            if !families.is_empty() && !families.contains(&family.as_str()) {
                continue;
            }
            let fam = &t.families[family];
            let markers = t.family_deletes.get(&(row.to_vec(), family.clone()));
            for (ts, value) in resolve(fam, entries, markers, snapshot, now, versions) {
                out.push(ModelCell {
                    family: family.clone(),
                    qualifier: qualifier.clone(),
                    ts,
                    value,
                });
            }
        }
        out
    }

    /// Rows in `[start, end)` with their latest visible cells, in order. Rows with no visible
    /// cell are omitted.
    pub fn scan(
        &self,
        table: &str,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        families: &[&str],
        snapshot: Seqno,
        now: Timestamp,
    ) -> Vec<(Vec<u8>, Vec<ModelCell>)> {
        let Some(t) = self.tables.get(table) else {
            return Vec::new();
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
            return Vec::new();
        }
        t.columns
            .range((start, end))
            .filter_map(|(row, _)| {
                let cells = self.read_row(table, row, families, 1, snapshot, now);
                (!cells.is_empty()).then(|| (row.clone(), cells))
            })
            .collect()
    }

    /// What a crash of `kind` may lose, given each commit's acknowledged durability.
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
}

/// Resolves one column to its visible versions, newest first, at most `limit` (0 = all).
fn resolve(
    family: &ModelFamily,
    entries: &[Entry],
    family_markers: Option<&Vec<Marker>>,
    snapshot: Seqno,
    now: Timestamp,
    limit: u32,
) -> Vec<(Timestamp, Vec<u8>)> {
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
                && !cell_deletes
                    .iter()
                    .any(|d| d.ts == e.ts && d.seqno > e.seqno)
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
    let mut out: Vec<(Timestamp, Vec<u8>)> = Vec::new();
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
                out.push((ts, value.clone()));
            }
            Some(Kind::Put(value)) => {
                let (run_ts, run_sum) = run.take().unwrap_or((ts, 0));
                let total = run_sum.wrapping_add(operands).wrapping_add(as_i64(value));
                out.push((run_ts, total.to_le_bytes().to_vec()));
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
        out.push((run_ts, sum.to_le_bytes().to_vec()));
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
    out
}

/// `(row, family, qualifier, ts)` of a column-level mutation, borrowed.
type CollapseKey<'a> = (&'a str, &'a [u8], &'a str, &'a [u8], Timestamp);

/// The collapse key of a column-level mutation: `(row, family, qualifier, ts)`. Family and row
/// markers live in their own key space and never collapse with column entries.
fn column_key(op: &ModelOp, commit_ts: Timestamp) -> Option<CollapseKey<'_>> {
    Some(match op {
        ModelOp::Put {
            table,
            row,
            family,
            qualifier,
            ts,
            ..
        } => (table, row, family, qualifier, ts.unwrap_or(commit_ts)),
        ModelOp::Incr {
            table,
            row,
            family,
            qualifier,
            ..
        }
        | ModelOp::DeleteColumn {
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

fn as_i64(bytes: &[u8]) -> i64 {
    <[u8; 8]>::try_from(bytes).map_or(0, i64::from_le_bytes)
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
        // A later put at the deleted timestamp is visible again.
        m.commit(&[put("r", "q", Some(6), "c")], 12, Durability::Sync);
        assert_eq!(
            values(&m, "r", 0, 3, 100),
            [(6, "c".into()), (5, "a".into())]
        );
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
}
