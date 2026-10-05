use std::collections::BTreeMap;
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
/// - A column's versions are ordered newest first by timestamp. Two puts at one timestamp
///   are one version: the higher seqno wins.
/// - `DeleteColumn` and `DeleteFamily` (and `DeleteRow`, one family marker per family) take
///   the commit timestamp `T` and hide every version in scope with timestamp `<= T`,
///   whatever its seqno; a later put with an older timestamp stays hidden. `DeleteCell`
///   hides the versions at exactly its timestamp written before it.
/// - TTL: a version is expired when `ts + ttl_micros <= now` (timestamps are microseconds).
///   Expired versions are dropped before merge operands are folded and versions counted.
/// - `Incr` is a merge operand at the commit timestamp. Going newest to oldest, a run of
///   operands folds into one cell at the newest operand's timestamp: its value is the
///   wrapping sum of the operands plus the `i64` in the next older put, which the fold
///   consumes (a put value that is not 8 bytes counts as 0). Without a base the sum is the
///   value.
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
    /// If an op names a table or family that does not exist.
    pub fn commit(
        &mut self,
        ops: &[ModelOp],
        commit_ts: Timestamp,
        durability: Durability,
    ) -> Seqno {
        self.commits.push(durability);
        let seqno = self.commits.len() as Seqno;
        for op in ops {
            self.apply(op, commit_ts, seqno);
        }
        seqno
    }

    fn apply(&mut self, op: &ModelOp, commit_ts: Timestamp, seqno: Seqno) {
        let (table, row) = match op {
            ModelOp::Put { table, row, .. }
            | ModelOp::Incr { table, row, .. }
            | ModelOp::DeleteCell { table, row, .. }
            | ModelOp::DeleteColumn { table, row, .. }
            | ModelOp::DeleteFamily { table, row, .. }
            | ModelOp::DeleteRow { table, row } => (table, row),
        };
        let t = self
            .tables
            .get_mut(table)
            .unwrap_or_else(|| panic!("no table {table:?}"));
        let column = |t: &mut Table, family: &String, qualifier: &Vec<u8>, entry: Entry| {
            assert!(t.families.contains_key(family), "no family {family:?}");
            t.columns
                .entry(row.clone())
                .or_default()
                .entry((family.clone(), qualifier.clone()))
                .or_default()
                .push(entry);
        };
        match op {
            ModelOp::Put {
                family,
                qualifier,
                ts,
                value,
                ..
            } => column(
                t,
                family,
                qualifier,
                Entry {
                    ts: ts.unwrap_or(commit_ts),
                    seqno,
                    kind: Kind::Put(value.clone()),
                },
            ),
            ModelOp::Incr {
                family,
                qualifier,
                delta,
                ..
            } => column(
                t,
                family,
                qualifier,
                Entry {
                    ts: commit_ts,
                    seqno,
                    kind: Kind::Merge(*delta),
                },
            ),
            ModelOp::DeleteCell {
                family,
                qualifier,
                ts,
                ..
            } => column(
                t,
                family,
                qualifier,
                Entry {
                    ts: *ts,
                    seqno,
                    kind: Kind::CellDelete,
                },
            ),
            ModelOp::DeleteColumn {
                family, qualifier, ..
            } => column(
                t,
                family,
                qualifier,
                Entry {
                    ts: commit_ts,
                    seqno,
                    kind: Kind::ColumnDelete,
                },
            ),
            ModelOp::DeleteFamily { family, .. } => {
                assert!(t.families.contains_key(family), "no family {family:?}");
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
    // Entries are in write order, so a position also orders entries inside one commit.
    let cell_deletes: Vec<(usize, &Entry)> = entries
        .iter()
        .enumerate()
        .filter(|(_, e)| visible(e.seqno) && matches!(e.kind, Kind::CellDelete))
        .collect();

    let mut live: Vec<(usize, &Entry)> = entries
        .iter()
        .enumerate()
        .filter(|(i, e)| {
            visible(e.seqno)
                && matches!(e.kind, Kind::Put(_) | Kind::Merge(_))
                && covered.is_none_or(|c| e.ts > c)
                && !cell_deletes.iter().any(|(d, de)| de.ts == e.ts && d > i)
                && (family.ttl_micros == 0 || e.ts.saturating_add(family.ttl_micros) > now)
        })
        .collect();
    live.sort_by(|(ia, a), (ib, b)| (b.ts, b.seqno, ib).cmp(&(a.ts, a.seqno, ia)));
    let live: Vec<&Entry> = live.into_iter().map(|(_, e)| e).collect();
    // One version per timestamp: the highest seqno wins, except that merge operands at one
    // timestamp all stay (they fold together).
    let mut out: Vec<(Timestamp, Vec<u8>)> = Vec::new();
    let mut i = 0;
    while i < live.len() {
        match &live[i].kind {
            Kind::Put(value) => {
                let ts = live[i].ts;
                out.push((ts, value.clone()));
                while i < live.len() && live[i].ts == ts && matches!(live[i].kind, Kind::Put(_)) {
                    i += 1;
                }
            }
            Kind::Merge(_) => {
                let ts = live[i].ts;
                let mut sum = 0i64;
                while let Some(Kind::Merge(d)) = live.get(i).map(|e| &e.kind) {
                    sum = sum.wrapping_add(*d);
                    i += 1;
                }
                if let Some(Kind::Put(base)) = live.get(i).map(|e| &e.kind) {
                    sum = sum.wrapping_add(as_i64(base));
                    let base_ts = live[i].ts;
                    while i < live.len()
                        && live[i].ts == base_ts
                        && matches!(live[i].kind, Kind::Put(_))
                    {
                        i += 1;
                    }
                }
                out.push((ts, sum.to_le_bytes().to_vec()));
            }
            _ => unreachable!("deletes were filtered out"),
        }
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
        m.commit(&[incr("r", 1), incr("r", 1)], 20, Durability::Sync);
        let n = |snap| {
            m.get("t", b"r", "c", b"n", snap, 100)
                .map(|c| (c.ts, i64::from_le_bytes(c.value.try_into().unwrap())))
        };
        assert_eq!(n(1), Some((5, 1)));
        assert_eq!(n(2), Some((10, 40)));
        assert_eq!(n(3), Some((20, 42)));
        let mut w = model();
        w.commit(&[incr("r", i64::MAX), incr("r", 1)], 1, Durability::Sync);
        let v = w.get("t", b"r", "c", b"n", 1, 1).unwrap().value;
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
}
