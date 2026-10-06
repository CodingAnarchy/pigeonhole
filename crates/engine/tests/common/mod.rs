//! The engine's model checker: drives `pigeonhole-engine` in application-owned mode under
//! the `Sim` scheduler (shards as tasks, so their interleaving varies by seed) and compares
//! every read, scan and recovered state against `pigeonhole_sim::Model`.
//!
//! **Commits.** One commit is in flight at a time: the client submits it, the scheduler
//! interleaves shard steps until it resolves, and the model applies it at the engine's
//! seqno. The simulated clock advances one microsecond per operation, so a default commit
//! timestamp is the clock at submission (decision D11) and the model gets the same value.
//!
//! **Crashes.** Power loss and process crashes between operations, and power loss in the
//! middle of a commit (`FaultPlan::crash_after_ops`). Recovery is checked against the WAL
//! itself: the harness reads every stream's surviving records (a Batch, or a Prepare whose
//! coordinator stream holds the Commit) to learn which commits the engine must have
//! recovered, checks the per-stream durability promises (every acknowledged commit at or
//! below a stream's last `GroupSync`/`Sync` commit, or `Buffered` after a process crash,
//! must be there, and survivors are a prefix per stream), rebuilds the model from those
//! commits, and compares the reopened engine with it.
#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::future::Future;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, Error, FamilyId, FamilyOptions, PendingCommit, Predicate,
    ScanSpec, Snapshot, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::wal::WalRecord;
use pigeonhole_format::{Durability, Lsn, Seqno, StreamId, Timestamp};
use pigeonhole_io::sim::{CrashKind, FaultPlan, SimVfs};
use pigeonhole_io::{ErrorKind, FileRef, OpenOptions, Vfs};
use pigeonhole_sim::{
    Model, ModelCell, ModelFamily, ModelOp, Op, Rng, Sim, Step, Workload, WorkloadSpec,
};

pub const TABLE: &str = "t";
pub const DB: &str = "/db/data.phdb";

/// A table is one tablet on one shard until splits exist, so the checker spreads rows over
/// several tables (deterministically by row) to get cross-shard commits.
pub const TABLES: [&str; 4] = ["t0", "t1", "t2", "t3"];

pub fn table_of(row: &[u8]) -> &'static str {
    let h = row.iter().fold(0usize, |h, b| {
        h.wrapping_mul(31).wrapping_add(usize::from(*b))
    });
    TABLES[h % TABLES.len()]
}

/// Rewrites a generated op (table `t`) onto the table its row belongs to.
pub fn place(op: &mut ModelOp) {
    match op {
        ModelOp::Put { table, row, .. }
        | ModelOp::Incr { table, row, .. }
        | ModelOp::DeleteCell { table, row, .. }
        | ModelOp::DeleteColumn { table, row, .. }
        | ModelOp::DeleteFamily { table, row, .. }
        | ModelOp::DeleteRow { table, row, .. } => *table = table_of(row).to_owned(),
    }
}

fn op_table(op: &ModelOp) -> &str {
    match op {
        ModelOp::Put { table, .. }
        | ModelOp::Incr { table, .. }
        | ModelOp::DeleteCell { table, .. }
        | ModelOp::DeleteColumn { table, .. }
        | ModelOp::DeleteFamily { table, .. }
        | ModelOp::DeleteRow { table, .. } => table,
    }
}

fn op_row(op: &ModelOp) -> &[u8] {
    match op {
        ModelOp::Put { row, .. }
        | ModelOp::Incr { row, .. }
        | ModelOp::DeleteCell { row, .. }
        | ModelOp::DeleteColumn { row, .. }
        | ModelOp::DeleteFamily { row, .. }
        | ModelOp::DeleteRow { row, .. } => row,
    }
}

pub fn new_model() -> Model {
    let mut m = Model::new();
    for t in TABLES {
        m.create_table(t, families());
    }
    m
}

/// The families every model-check run uses.
pub fn families() -> Vec<ModelFamily> {
    let f = |name: &str, max_versions, ttl_micros, i64_add| ModelFamily {
        name: name.into(),
        max_versions,
        ttl_micros,
        i64_add,
    };
    vec![
        f("f", 0, 0, false),
        f("g", 2, 0, false),
        f("ttl", 0, 40, false),
        f("counter", 3, 0, true),
    ]
}

fn family_options(f: &ModelFamily) -> FamilyOptions {
    FamilyOptions {
        max_versions: f.max_versions,
        ttl_micros: f.ttl_micros,
        merge_operator: if f.i64_add {
            "pigeonhole.i64_add".to_owned()
        } else {
            String::new()
        },
        ..FamilyOptions::default()
    }
}

#[derive(Clone)]
pub struct Config {
    /// Operations in the run.
    pub ops: usize,
    pub faults: FaultPlan,
    /// Per-op probability (ppm) of crashing between operations.
    pub crash_ppm: u32,
    /// Per-commit probability (ppm) of a power loss in the middle of the commit.
    pub mid_commit_crash_ppm: u32,
    pub spec: WorkloadSpec,
    /// Shard count at the first open.
    pub shards: usize,
    /// Shard counts to use at each reopen (cycled); empty keeps `shards`.
    pub reopen_shards: Vec<usize>,
    /// Memtable arena per shard.
    pub memtable_budget: u64,
    /// Every commit uses this level instead of the workload's.
    pub durability: Option<Durability>,
    /// Crash exactly after this many mutating operations (a crash-at-every-point sweep).
    pub crash_at: Option<u64>,
    /// How often (ppm) a scan is followed by a full-state comparison.
    pub dump_ppm: u32,
    /// Concurrent plain commits submitted together (real groups on the shards).
    pub tasks: usize,
    /// How often (ppm) a commit runs as a `check_and_mutate` / a transaction.
    pub cas_ppm: u32,
    pub txn_ppm: u32,
    /// `final_dump` only: a process crash and reopen every this many ops.
    pub crash_every: Option<usize>,
}

impl Config {
    pub fn standard(ops: usize) -> Self {
        let mut faults = FaultPlan::none();
        faults.torn_writes = true;
        faults.reorder_unsynced = true;
        let mut spec = WorkloadSpec::default();
        spec.rows = 24;
        spec.qualifiers = 3;
        spec.families = families().into_iter().map(|f| f.name).collect();
        spec.max_value_len = 160;
        spec.max_batch = 5;
        Config {
            ops,
            faults,
            crash_ppm: 15_000,
            mid_commit_crash_ppm: 25_000,
            spec,
            shards: 2,
            reopen_shards: Vec::new(),
            memtable_budget: 4 << 20,
            durability: None,
            crash_at: None,
            dump_ppm: 100_000,
            tasks: 4,
            cas_ppm: 80_000,
            txn_ppm: 80_000,
            crash_every: None,
        }
    }

    /// No faults, no crashes.
    pub fn quiet(ops: usize) -> Self {
        let mut c = Self::standard(ops);
        c.faults = FaultPlan::none();
        c.crash_ppm = 0;
        c.mid_commit_crash_ppm = 0;
        c
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Stats {
    pub ops: usize,
    pub submitted: usize,
    pub commits: usize,
    pub cross_shard: usize,
    pub batched: usize,
    pub cas_refused: usize,
    pub conflicts: usize,
    pub io_errors: usize,
    pub crashes: usize,
    pub mid_commit_crashes: usize,
    pub busy: usize,
}

/// What kind of divergence the checker saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// Recovery lost a commit the durability level promised.
    LostAckedCommit,
    /// A stream's survivors are not a prefix, or a seqno the client never committed survived.
    RecoveredFromTheFuture,
    /// The recovered state is not the model's state for the surviving commits.
    RecoveredStateMismatch,
    /// A live read or scan disagreed with the model.
    LiveReadMismatch,
    /// The engine and model disagree about a seqno, or an unexpected error occurred.
    Protocol,
}

struct Fail {
    class: FailureClass,
    message: String,
}

fn fail<T>(class: FailureClass, message: String) -> Result<T, Fail> {
    Err(Fail { class, message })
}

/// A checker failure with everything needed to replay it.
pub struct Failure {
    pub seed: u64,
    pub class: FailureClass,
    pub op_index: usize,
    pub message: String,
    pub trace: Vec<String>,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "checker failure ({:?}): seed={} at op #{}: {}",
            self.class, self.seed, self.op_index, self.message
        )?;
        const SHOWN: usize = 40;
        let skip = self.trace.len().saturating_sub(SHOWN);
        if skip > 0 {
            writeln!(
                f,
                "  ... {skip} earlier trace lines omitted; replay with the seed"
            )?;
        }
        for line in &self.trace[skip..] {
            writeln!(f, "  {line}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn show(op: &ModelOp) -> String {
    match op {
        ModelOp::Put {
            row,
            family,
            qualifier,
            ts,
            value,
            ..
        } => format!(
            "put {}/{family}:{} ts={ts:?} {}B",
            text(row),
            text(qualifier),
            value.len()
        ),
        ModelOp::Incr {
            row,
            family,
            qualifier,
            delta,
            ..
        } => format!("incr {}/{family}:{} {delta:+}", text(row), text(qualifier)),
        ModelOp::DeleteCell {
            row,
            family,
            qualifier,
            ts,
            ..
        } => format!(
            "del-cell {}/{family}:{} ts={ts}",
            text(row),
            text(qualifier)
        ),
        ModelOp::DeleteColumn {
            row,
            family,
            qualifier,
            ..
        } => format!("del-col {}/{family}:{}", text(row), text(qualifier)),
        ModelOp::DeleteFamily { row, family, .. } => format!("del-fam {}/{family}", text(row)),
        ModelOp::DeleteRow { row, .. } => format!("del-row {}", text(row)),
    }
}

fn show_cell(c: &ModelCell) -> String {
    let head: Vec<u8> = c.value.iter().copied().take(6).collect();
    format!(
        "{}:{}@{}={}B{head:02x?}",
        c.family,
        text(&c.qualifier),
        c.ts,
        c.value.len()
    )
}

fn show_row(r: Option<&(Vec<u8>, Vec<ModelCell>)>) -> String {
    r.map_or("<no row>".into(), |(row, cells)| {
        format!(
            "{} [{}]",
            text(row),
            cells.iter().map(show_cell).collect::<Vec<_>>().join(", ")
        )
    })
}

type Rows = Vec<(Vec<u8>, Vec<ModelCell>)>;

fn first_diff(store: &Rows, model: &Rows) -> Option<String> {
    (0..store.len().max(model.len())).find_map(|i| {
        (store.get(i) != model.get(i)).then(|| {
            format!(
                "first difference at row #{i}: engine {} vs model {}",
                show_row(store.get(i)),
                show_row(model.get(i))
            )
        })
    })
}

// ---------------------------------------------------------------------------------------
// The engine adapter
// ---------------------------------------------------------------------------------------

/// The engine as the checker drives it: application-owned shards.
pub struct Store {
    pub engine: Arc<Engine>,
    pub shards: Vec<EngineShard>,
    pub tables: HashMap<String, Arc<TableInfo>>,
    /// `(table, family) -> id`.
    pub family_ids: HashMap<(String, String), FamilyId>,
    pub family_names: HashMap<FamilyId, String>,
}

pub fn options(vfs: Arc<SimVfs>, shards: usize, memtable_budget: u64) -> EngineOptions {
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = shards;
    o.pin_threads = false;
    o.memtable_budget = memtable_budget;
    o.memtable_freeze_bytes = memtable_budget / 4;
    o.reader_slots = 8;
    o.wal.segment_size = 8 * 32 * 1024;
    o.wal.spare_segments = 1;
    o
}

impl Store {
    /// Opens (creating the table on a fresh database).
    pub fn open(
        vfs: &Arc<SimVfs>,
        shards: usize,
        budget: u64,
        fams: &[ModelFamily],
    ) -> Result<Self, Error> {
        let (engine, shards) = Engine::open_application_owned(
            Path::new(DB),
            options(Arc::clone(vfs), shards, budget),
        )?;
        let mut tables = HashMap::new();
        let mut family_ids = HashMap::new();
        let mut family_names = HashMap::new();
        for name in TABLES {
            let table = match engine.table(name) {
                Some(t) => t,
                None => {
                    let defs: Vec<(String, FamilyOptions)> = fams
                        .iter()
                        .map(|f| (f.name.clone(), family_options(f)))
                        .collect();
                    engine.create_table(name, &defs)?
                }
            };
            for f in &table.families {
                family_ids.insert((name.to_owned(), f.name.clone()), f.id);
                family_names.insert(f.id, f.name.clone());
            }
            tables.insert(name.to_owned(), table);
        }
        Ok(Store {
            engine,
            shards,
            tables,
            family_ids,
            family_names,
        })
    }

    /// Encodes model ops into an engine batch, mapping names to ids.
    pub fn batch(&self, ops: &[ModelOp]) -> Result<WriteBatch, Error> {
        let mut wb = WriteBatch::new();
        for op in ops {
            let t = self.tables[op_table(op)].id;
            let fam = |name: &str| self.family_ids[&(op_table(op).to_owned(), name.to_owned())];
            match op {
                ModelOp::Put {
                    row,
                    family,
                    qualifier,
                    ts,
                    value,
                    ..
                } => {
                    // Counter families hold tagged `i64`s (what `put_i64` writes).
                    let value = match (
                        family.starts_with("counter"),
                        <[u8; 8]>::try_from(value.as_slice()),
                    ) {
                        (true, Ok(b)) => ValueRef::I64(i64::from_le_bytes(b)),
                        _ => ValueRef::Bytes(value),
                    };
                    wb.put(t, fam(family), row, qualifier, *ts, value)?
                }
                ModelOp::Incr {
                    row,
                    family,
                    qualifier,
                    delta,
                    ..
                } => wb.merge(t, fam(family), row, qualifier, ValueRef::I64(*delta))?,
                ModelOp::DeleteCell {
                    row,
                    family,
                    qualifier,
                    ts,
                    ..
                } => wb.delete_cell(t, fam(family), row, qualifier, *ts)?,
                ModelOp::DeleteColumn {
                    row,
                    family,
                    qualifier,
                    ..
                } => wb.delete_column(t, fam(family), row, qualifier, None)?,
                ModelOp::DeleteFamily { row, family, .. } => {
                    wb.delete_family(t, fam(family), row, None)?
                }
                ModelOp::DeleteRow { row, .. } => wb.delete_row(t, row, None)?,
            }
        }
        Ok(wb)
    }

    /// Runs every shard once.
    pub fn step_shards(&mut self, now: u64) {
        for s in &mut self.shards {
            s.run_once(now + 1_000);
        }
    }

    pub fn get(
        &self,
        snap: &Snapshot,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> Result<Option<ModelCell>, Error> {
        let table = table_of(row);
        let fam = self.family_ids[&(table.to_owned(), family.to_owned())];
        Ok(self
            .engine
            .get(snap, self.tables[table].id, fam, row, qualifier)?
            .map(|c| ModelCell {
                family: family.to_owned(),
                qualifier: qualifier.to_vec(),
                ts: c.timestamp(),
                value: value_bytes(c.value()),
            }))
    }

    /// Rows in `[start, end)` with the latest version of each column (`versions = 1`), or
    /// every version (`versions = 0`).
    pub fn scan(
        &self,
        snap: &Snapshot,
        table: &str,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        versions: u32,
    ) -> Result<Rows, Error> {
        let own = |b: Bound<&[u8]>| match b {
            Bound::Included(k) => Bound::Included(k.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let mut spec = ScanSpec::new(own(start), own(end));
        spec.read.versions = versions;
        let mut cursor = self.engine.scan(snap, self.tables[table].id, spec)?;
        let mut rows = Vec::new();
        while cursor.next_row()? {
            let row = cursor.row().to_vec();
            let mut cells = Vec::new();
            while let Some(c) = cursor.next_cell()? {
                cells.push(ModelCell {
                    family: self.family_names[&c.family].clone(),
                    qualifier: c.qualifier.to_vec(),
                    ts: c.ts,
                    value: value_bytes(
                        pigeonhole_format::value::decode_value(c.stored).expect("stored value"),
                    ),
                });
            }
            rows.push((row, cells));
        }
        Ok(rows)
    }

    /// Every row of every table with every visible version (rows prefixed by their table),
    /// for state comparisons.
    pub fn dump(&self, snap: &Snapshot) -> Result<Rows, Error> {
        let mut out = Vec::new();
        for t in TABLES {
            for (row, cells) in self.scan(snap, t, Bound::Unbounded, Bound::Unbounded, 0)? {
                out.push(([t.as_bytes(), b"/", &row].concat(), cells));
            }
        }
        Ok(out)
    }
}

pub fn value_bytes(v: ValueRef<'_>) -> Vec<u8> {
    match v {
        ValueRef::Bytes(b) => b.to_vec(),
        ValueRef::I64(x) | ValueRef::Varint(x) => x.to_le_bytes().to_vec(),
        ValueRef::F64(x) => x.to_le_bytes().to_vec(),
        ValueRef::Blob(_) => panic!("blob pointer returned"),
    }
}

/// Polls a pending commit without a real waker.
pub fn poll_commit(pc: &mut PendingCommit) -> Poll<Result<pigeonhole_engine::CommitInfo, Error>> {
    let mut cx = Context::from_waker(Waker::noop());
    Pin::new(pc).poll(&mut cx)
}

/// The model's dump: every row of every table with every version.
pub fn model_dump(
    model: &Model,
    snapshot: Seqno,
    now: Timestamp,
) -> Result<Rows, pigeonhole_sim::ModelError> {
    let mut out = Vec::new();
    for t in TABLES {
        let rows = model.try_scan(t, Bound::Unbounded, Bound::Unbounded, &[], snapshot, now)?;
        for (row, _) in rows {
            let cells = model.try_read_row(t, &row, &[], 0, snapshot, now)?;
            out.push(([t.as_bytes(), b"/", &row].concat(), cells));
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------
// WAL survivors
// ---------------------------------------------------------------------------------------

/// What the WAL holds after a crash.
#[derive(Debug, Default)]
pub struct Survivors {
    /// Commits recovery must apply, by seqno.
    pub commits: BTreeMap<Seqno, Survivor>,
    /// Each stream's record seqnos (Batch records and applied Prepares), sorted.
    pub order: BTreeMap<StreamId, Vec<Seqno>>,
    /// Decided cross-shard commits (COMMIT present) with at least one PREPARE missing:
    /// recovery must apply nothing of them.
    pub ambiguous: Vec<Seqno>,
}

/// A surviving commit as the WAL holds it.
#[derive(Debug, Clone)]
pub struct Survivor {
    pub seqno: Seqno,
    pub commit_ts: Timestamp,
    /// Every stream that holds a record of it (one for a single-shard commit).
    pub streams: Vec<StreamId>,
    /// The encoded mutations, per stream.
    pub batches: Vec<Vec<u8>>,
}

/// Reads every stream of `db` and returns the commits recovery must apply: Batch records,
/// and Prepare records whose coordinator stream holds the matching Commit. Also returns the
/// per-stream list of record seqnos in log order (for the prefix check).
pub fn wal_survivors(vfs: &Arc<SimVfs>, db: &Path) -> Result<Survivors, String> {
    let vfs_ref: pigeonhole_io::VfsRef = Arc::clone(vfs) as pigeonhole_io::VfsRef;
    let opened =
        pigeonhole_pager::Pager::open(&vfs_ref, db, false).map_err(|e| format!("pager: {e}"))?;
    let db_id = opened.db_id();
    drop(opened);
    let streams =
        pigeonhole_wal::discover_streams(&vfs_ref, db).map_err(|e| format!("discover: {e}"))?;
    let mut survivors: BTreeMap<Seqno, Survivor> = BTreeMap::new();
    let mut prepares: Vec<(StreamId, Seqno, Timestamp, StreamId, Vec<u8>)> = Vec::new();
    let mut commits: BTreeMap<(StreamId, Seqno), Vec<StreamId>> = BTreeMap::new();
    let mut order: BTreeMap<StreamId, Vec<Seqno>> = BTreeMap::new();
    for stream in streams {
        let mut rec = pigeonhole_wal::Recovery::open(&vfs_ref, db, stream, db_id, Lsn::default())
            .map_err(|e| format!("recovery open {}: {e}", stream.0))?;
        loop {
            let next = rec
                .next_record()
                .map_err(|e| format!("replay {}: {e}", stream.0))?;
            let Some((_, record)) = next else { break };
            match record {
                WalRecord::Batch {
                    seqno,
                    commit_ts,
                    batch,
                } => {
                    order.entry(stream).or_default().push(seqno);
                    survivors.insert(
                        seqno,
                        Survivor {
                            seqno,
                            commit_ts,
                            streams: vec![stream],
                            batches: vec![batch.as_bytes().to_vec()],
                        },
                    );
                }
                WalRecord::Prepare {
                    seqno,
                    commit_ts,
                    coordinator,
                    batch,
                } => {
                    prepares.push((
                        stream,
                        seqno,
                        commit_ts,
                        coordinator,
                        batch.as_bytes().to_vec(),
                    ));
                }
                WalRecord::Commit {
                    seqno,
                    participants,
                } => {
                    commits.insert((stream, seqno), participants.iter().collect());
                }
            }
        }
    }
    // All or nothing: a decided commit counts only if every listed participant holds its
    // PREPARE (the engine's recovery rule).
    let prepared: BTreeSet<(StreamId, Seqno)> = prepares.iter().map(|p| (p.0, p.1)).collect();
    let mut ambiguous: Vec<Seqno> = commits
        .iter()
        .filter(|((_, seqno), ps)| !ps.iter().all(|p| prepared.contains(&(*p, *seqno))))
        .map(|((_, seqno), _)| *seqno)
        .collect();
    ambiguous.sort_unstable();
    ambiguous.dedup();
    for (stream, seqno, commit_ts, coordinator, bytes) in prepares {
        let decided = commits
            .get(&(coordinator, seqno))
            .is_some_and(|ps| ps.iter().all(|p| prepared.contains(&(*p, seqno))));
        if decided {
            order.entry(stream).or_default().push(seqno);
            let s = survivors.entry(seqno).or_insert_with(|| Survivor {
                seqno,
                commit_ts,
                streams: Vec::new(),
                batches: Vec::new(),
            });
            s.streams.push(stream);
            s.batches.push(bytes);
        }
    }
    for list in order.values_mut() {
        list.sort_unstable();
    }
    Ok(Survivors {
        commits: survivors,
        order,
        ambiguous,
    })
}

// ---------------------------------------------------------------------------------------
// The world
// ---------------------------------------------------------------------------------------

/// A conditional-write predicate on one column, as the model evaluates it.
#[derive(Debug, Clone, PartialEq)]
enum CasPred {
    Exists,
    Absent,
    Equals(Vec<u8>),
}

/// How the client submitted a commit.
#[derive(Debug, Clone)]
enum Kind {
    Plain,
    /// `check_and_mutate` on `(table, row)` with a predicate on `(family, qualifier)`.
    Cas {
        table: String,
        row: Vec<u8>,
        family: String,
        qualifier: Vec<u8>,
        pred: CasPred,
        /// What the model said when the predicate was evaluated (nothing in flight).
        expected: bool,
    },
    /// A transaction: reads at the engine snapshot, then the batch.
    Txn {
        snapshot: Seqno,
        reads: Vec<(String, Vec<u8>, String)>,
    },
}

/// One acknowledged (or in-flight) commit.
#[derive(Debug, Clone)]
struct Committed {
    seqno: Option<Seqno>,
    ops: Vec<ModelOp>,
    commit_ts: Option<Timestamp>,
    durability: Durability,
    /// The shards its rows route to.
    shards: Vec<u16>,
    acked: bool,
    kind: Kind,
}

impl Committed {
    /// A `(table, row, family)` the commit writes.
    fn touches(&self, table: &str, row: &[u8], family: &str) -> bool {
        self.ops.iter().any(|op| {
            op_table(op) == table
                && op_row(op) == row
                && match op {
                    ModelOp::DeleteRow { .. } => true,
                    ModelOp::Put { family: f, .. }
                    | ModelOp::Incr { family: f, .. }
                    | ModelOp::DeleteCell { family: f, .. }
                    | ModelOp::DeleteColumn { family: f, .. }
                    | ModelOp::DeleteFamily { family: f, .. } => f == family,
                }
        })
    }
}

/// The result of a blocking engine call run on a helper thread.
enum Outcome {
    Commit(Result<pigeonhole_engine::CommitInfo, Error>),
    Cas(Result<(bool, Option<pigeonhole_engine::CommitInfo>), Error>),
}

enum Pending {
    Poll(PendingCommit),
    Thread(mpsc::Receiver<Outcome>),
}

struct InFlight {
    pending: Pending,
    commit: Committed,
    armed: bool,
    /// Whether the commit was submitted alone (its timestamp is then the clock); a batched
    /// commit is logged (never `Durability::None`), so its timestamp is read from the WAL.
    alone: bool,
}

/// A mutation as both the engine and the model see it, for matching unacknowledged commits
/// against WAL records.
type MutationKey = (
    String,
    String,
    u8,
    Vec<u8>,
    Vec<u8>,
    Option<Timestamp>,
    Vec<u8>,
);

struct World {
    seed: u64,
    cfg: Config,
    vfs: Arc<SimVfs>,
    probe: FileRef,
    store: Option<Store>,
    model: Model,
    /// Engine seqnos the model holds, in order; `model seqno = position + 1`.
    engine_seqnos: Vec<Seqno>,
    /// Every commit since the last recovery, by engine seqno.
    history: BTreeMap<Seqno, Committed>,
    /// Commits that returned an error (or were in flight at a crash): maybe landed.
    unacked: Vec<Committed>,
    in_flight: Vec<InFlight>,
    /// A shard reported a poisoned stream: reopen once nothing is in flight.
    need_reopen: bool,
    /// Microseconds to move the clock once the current batch has completed, past the
    /// default timestamps its group assigned.
    pending_advance: u64,
    base: u64,
    snaps: Vec<Snapshot>,
    trace: Vec<String>,
    op_index: usize,
    stats: Stats,
    failure: Option<Failure>,
    reopens: usize,
    done: bool,
    workload: Option<std::iter::Take<Workload>>,
    /// Ops taken from the workload but not yet run (lookahead for batching).
    queued: std::collections::VecDeque<Op>,
}

impl World {
    fn now(&self) -> u64 {
        self.vfs.now_micros()
    }

    fn alive(&self) -> bool {
        self.probe.len().is_ok()
    }

    fn model_seqno(&self, engine_seqno: Seqno) -> Seqno {
        self.engine_seqnos.partition_point(|s| *s <= engine_seqno) as Seqno
    }

    fn store(&self) -> &Store {
        self.store.as_ref().expect("store open")
    }

    fn shards_of(&self, ops: &[ModelOp]) -> Vec<u16> {
        let store = self.store();
        let view = store.engine.snapshot().expect("snapshot").view().clone();
        let mut out = Vec::new();
        for op in ops {
            let table = store.tables[op_table(op)].id;
            if let Some((_, shard)) = view.tablets().route(table, op_row(op))
                && !out.contains(&shard.0)
            {
                out.push(shard.0);
            }
        }
        out
    }

    fn trace_on(&self) -> bool {
        std::env::var("PIGEONHOLE_TRACE").is_ok()
    }

    fn compare_dump(&self, snap: &Snapshot, class: FailureClass) -> Result<(), Fail> {
        let now = self.now();
        let engine = match self.store().dump(snap) {
            Ok(d) => d,
            Err(e) => return fail(FailureClass::Protocol, format!("dump failed: {e}")),
        };
        let model = match model_dump(&self.model, self.model_seqno(snap.seqno()), now) {
            Ok(d) => d,
            Err(e) => return fail(FailureClass::Protocol, format!("model dump failed: {e}")),
        };
        match first_diff(&engine, &model) {
            None => Ok(()),
            Some(d) => fail(
                class,
                format!(
                    "state at engine seqno {} (model {}) differs: {d}",
                    snap.seqno(),
                    self.model_seqno(snap.seqno())
                ),
            ),
        }
    }

    fn next_shards(&mut self) -> usize {
        if self.cfg.reopen_shards.is_empty() {
            self.cfg.shards
        } else {
            self.cfg.reopen_shards[self.reopens % self.cfg.reopen_shards.len()]
        }
    }

    /// The mutations of `ops` as the engine logs them (row deletes expanded per family).
    fn mutation_keys(ops: &[ModelOp]) -> BTreeSet<MutationKey> {
        let mut out = BTreeSet::new();
        for op in ops {
            let t = op_table(op).to_owned();
            match op {
                ModelOp::Put {
                    row,
                    family,
                    qualifier,
                    ts,
                    value,
                    ..
                } => {
                    out.insert((
                        t,
                        family.clone(),
                        1,
                        row.clone(),
                        qualifier.clone(),
                        *ts,
                        value.clone(),
                    ));
                }
                ModelOp::Incr {
                    row,
                    family,
                    qualifier,
                    delta,
                    ..
                } => {
                    out.insert((
                        t,
                        family.clone(),
                        2,
                        row.clone(),
                        qualifier.clone(),
                        None,
                        delta.to_le_bytes().to_vec(),
                    ));
                }
                ModelOp::DeleteCell {
                    row,
                    family,
                    qualifier,
                    ts,
                    ..
                } => {
                    out.insert((
                        t,
                        family.clone(),
                        3,
                        row.clone(),
                        qualifier.clone(),
                        Some(*ts),
                        Vec::new(),
                    ));
                }
                ModelOp::DeleteColumn {
                    row,
                    family,
                    qualifier,
                    ..
                } => {
                    out.insert((
                        t,
                        family.clone(),
                        4,
                        row.clone(),
                        qualifier.clone(),
                        None,
                        Vec::new(),
                    ));
                }
                ModelOp::DeleteFamily { row, family, .. } => {
                    out.insert((
                        t,
                        family.clone(),
                        5,
                        row.clone(),
                        Vec::new(),
                        None,
                        Vec::new(),
                    ));
                }
                ModelOp::DeleteRow { row, .. } => {
                    for f in families() {
                        out.insert((
                            t.clone(),
                            f.name,
                            5,
                            row.clone(),
                            Vec::new(),
                            None,
                            Vec::new(),
                        ));
                    }
                }
            }
        }
        out
    }

    /// The mutations of a surviving record set, decoded.
    fn survivor_keys(&self, s: &Survivor) -> BTreeSet<MutationKey> {
        let store = self.store();
        let table_names: HashMap<pigeonhole_engine::TableId, String> = store
            .tables
            .iter()
            .map(|(n, t)| (t.id, n.clone()))
            .collect();
        let mut out = BTreeSet::new();
        for bytes in &s.batches {
            let Ok(batch) = pigeonhole_format::wal::BatchRef::new(bytes) else {
                continue;
            };
            for m in batch.iter().flatten() {
                let payload = match pigeonhole_format::value::decode_value(m.value) {
                    Ok(v) => value_bytes(v),
                    Err(_) => m.value.to_vec(),
                };
                out.insert((
                    table_names.get(&m.table).cloned().unwrap_or_default(),
                    store
                        .family_names
                        .get(&m.family)
                        .cloned()
                        .unwrap_or_default(),
                    m.kind as u8,
                    m.row.to_vec(),
                    m.qualifier.to_vec(),
                    m.ts,
                    payload,
                ));
            }
        }
        out
    }

    /// Crashes (or notes the fault plan already did), checks the WAL against the promises,
    /// rebuilds the model from the survivors and reopens.
    fn crash_and_recover(
        &mut self,
        kind: CrashKind,
        already: bool,
        rng: &mut Rng,
    ) -> Result<(), Fail> {
        self.stats.crashes += 1;
        if self.trace_on() {
            eprintln!("crash {kind:?} already={already}");
        }
        if !already {
            self.vfs.crash(kind);
        }
        self.trace.push(format!(
            "CRASH {kind:?}{}",
            if already { " mid-commit" } else { "" }
        ));
        // Whatever was in flight is unacknowledged: it may or may not have landed.
        for inf in self.in_flight.drain(..) {
            let mut c = inf.commit;
            c.acked = false;
            self.unacked.push(c);
        }
        self.need_reopen = false;
        self.pending_advance = 0;
        self.store = None;
        self.snaps.clear();
        self.vfs.set_faults(FaultPlan::none());

        let survivors = match wal_survivors(&self.vfs, Path::new(DB)) {
            Ok(s) => s,
            Err(e) => return fail(FailureClass::Protocol, format!("reading the WAL: {e}")),
        };
        let floor = match kind {
            CrashKind::Process => Durability::Buffered,
            CrashKind::Power => Durability::GroupSync,
        };

        // The reopened engine is needed to decode survivors (ids to names).
        let shards = self.next_shards();
        self.reopens += 1;
        let budget = self.cfg.memtable_budget;
        let store = match Store::open(&self.vfs, shards, budget, &families()) {
            Ok(s) => s,
            Err(e) => return fail(FailureClass::Protocol, format!("recovery failed: {e}")),
        };
        self.store = Some(store);
        self.vfs.set_faults(self.cfg.faults.clone());

        // Unknown seqnos must match unacknowledged commits, by their mutations.
        let mut unacked = std::mem::take(&mut self.unacked);
        let mut matched: BTreeMap<Seqno, Committed> = BTreeMap::new();
        for (seqno, s) in &survivors.commits {
            if self.history.contains_key(seqno) {
                continue;
            }
            let keys = self.survivor_keys(s);
            match unacked
                .iter()
                .position(|c| Self::mutation_keys(&c.ops) == keys)
            {
                Some(i) => {
                    let mut c = unacked.remove(i);
                    c.seqno = Some(*seqno);
                    c.commit_ts = Some(s.commit_ts);
                    matched.insert(*seqno, c);
                }
                None => {
                    return fail(
                        FailureClass::RecoveredFromTheFuture,
                        format!("the WAL holds commit {seqno}, which the client never made"),
                    );
                }
            }
        }
        // The promise: every commit acknowledged at the floor level or stronger survives.
        for c in self.history.values() {
            let Some(seqno) = c.seqno else { continue };
            if c.acked && c.durability >= floor && !survivors.commits.contains_key(&seqno) {
                return fail(
                    FailureClass::LostAckedCommit,
                    format!(
                        "commit {seqno} ({:?}, shards {:?}) was acknowledged but did not survive",
                        c.durability, c.shards
                    ),
                );
            }
        }
        // Per stream, a single-shard commit's record precedes every later single-shard
        // commit of that stream in the log, so everything below the stream's last synced
        // commit must be there (a sync covers every earlier append).
        let mut by_shard: BTreeMap<u16, Vec<&Committed>> = BTreeMap::new();
        for c in self.history.values() {
            if c.acked && c.shards.len() == 1 && c.durability != Durability::None {
                by_shard.entry(c.shards[0]).or_default().push(c);
            }
        }
        for (shard, commits) in &by_shard {
            let last_strong = commits
                .iter()
                .filter(|c| c.durability >= floor)
                .filter_map(|c| c.seqno)
                .max();
            for c in commits {
                let Some(seqno) = c.seqno else { continue };
                if last_strong.is_some_and(|l| seqno < l) && !survivors.commits.contains_key(&seqno)
                {
                    return fail(
                        FailureClass::LostAckedCommit,
                        format!(
                            "stream {shard} lost commit {seqno} ({:?}) although its later commit {} was synced",
                            c.durability,
                            last_strong.unwrap()
                        ),
                    );
                }
            }
        }
        // All or nothing, checked against the WAL directly: a decided commit with a missing
        // PREPARE was never acknowledged at the floor level or stronger (those prepares were
        // durable before the COMMIT), and the engine applies none of it (checked below
        // through the model, which excludes it).
        for a in &survivors.ambiguous {
            if let Some(c) = self.history.get(a)
                && c.acked
                && c.durability >= floor
            {
                return fail(
                    FailureClass::LostAckedCommit,
                    format!(
                        "commit {a} ({:?}) was acknowledged but a participant's PREPARE is gone",
                        c.durability
                    ),
                );
            }
        }
        // Rebuild the model from the survivors, in seqno order.
        let mut kept: Vec<Committed> = Vec::new();
        for (seqno, c) in &self.history {
            if survivors.commits.contains_key(seqno) {
                kept.push(c.clone());
            }
        }
        kept.extend(matched.into_values());
        kept.sort_by_key(|c| c.seqno);
        let mut model = new_model();
        self.history.clear();
        self.engine_seqnos.clear();
        for mut c in kept {
            let seqno = c.seqno.expect("kept commits have a seqno");
            let ts = c.commit_ts.expect("kept commits have a timestamp");
            if let Err(e) = model.try_commit(&c.ops, ts, Durability::Sync) {
                return fail(FailureClass::Protocol, format!("model rebuild: {e}"));
            }
            self.engine_seqnos.push(seqno);
            c.acked = true;
            c.shards = Vec::new();
            self.history.insert(seqno, c);
        }
        self.model = model;
        // Routing under the new shard count, for the next crash's per-stream check.
        let keys: Vec<Seqno> = self.history.keys().copied().collect();
        for k in keys {
            let ops = self.history[&k].ops.clone();
            let shards = self.shards_of(&ops);
            self.history.get_mut(&k).unwrap().shards = shards;
        }

        let snap = match self.store().engine.snapshot() {
            Ok(s) => s,
            Err(e) => {
                return fail(
                    FailureClass::Protocol,
                    format!("snapshot after recovery: {e}"),
                );
            }
        };
        let recovered = snap.seqno();
        let last_kept = self.engine_seqnos.last().copied().unwrap_or(0);
        self.trace.push(format!(
            "RECOVERED with {shards} shards: engine seqno {recovered}, {} commits kept (last {last_kept}), {} ambiguous",
            self.engine_seqnos.len(),
            survivors.ambiguous.len()
        ));
        if recovered < last_kept {
            return fail(
                FailureClass::RecoveredStateMismatch,
                format!("visible seqno {recovered} is below the last surviving commit {last_kept}"),
            );
        }
        self.compare_dump(&snap, FailureClass::RecoveredStateMismatch)?;
        if recovered > 0 {
            let s = 1 + rng.below(recovered);
            self.compare_dump(&snap.at_seqno(s), FailureClass::RecoveredStateMismatch)?;
        }
        Ok(())
    }

    fn maybe_crash(&mut self, rng: &mut Rng) -> Result<(), Fail> {
        if rng.chance(self.cfg.crash_ppm) {
            let kind = if rng.chance(500_000) {
                CrashKind::Power
            } else {
                CrashKind::Process
            };
            self.crash_and_recover(kind, false, rng)?;
        }
        Ok(())
    }

    /// The commit timestamps the engine assigned, read from the WAL records (batched
    /// commits are always logged).
    fn logged_timestamps(&self) -> Result<BTreeMap<Seqno, Timestamp>, Fail> {
        self.vfs.set_faults(FaultPlan::none());
        let survivors = wal_survivors(&self.vfs, Path::new(DB));
        self.vfs.set_faults(self.cfg.faults.clone());
        match survivors {
            Ok(s) => Ok(s.commits.iter().map(|(k, v)| (*k, v.commit_ts)).collect()),
            Err(e) => fail(
                FailureClass::Protocol,
                format!("reading the WAL for timestamps: {e}"),
            ),
        }
    }

    /// Settles every in-flight commit that resolved. Returns whether the client may go on
    /// (nothing left in flight).
    fn poll_in_flight(&mut self, rng: &mut Rng) -> Result<bool, Fail> {
        if self.in_flight.is_empty() {
            if self.need_reopen {
                self.crash_and_recover(CrashKind::Process, false, rng)?;
            }
            return Ok(true);
        }
        if !self.alive() {
            if self.in_flight.iter().any(|i| i.armed) {
                self.stats.mid_commit_crashes += 1;
            }
            self.crash_and_recover(CrashKind::Power, true, rng)?;
            return Ok(true);
        }
        let mut resolved: Vec<(InFlight, Outcome)> = Vec::new();
        let mut still = Vec::new();
        for mut inf in self.in_flight.drain(..) {
            let outcome = match &mut inf.pending {
                Pending::Poll(pc) => match poll_commit(pc) {
                    Poll::Ready(r) => Some(Outcome::Commit(r)),
                    Poll::Pending => None,
                },
                Pending::Thread(rx) => rx.recv_timeout(Duration::from_millis(1)).ok(),
            };
            match outcome {
                Some(o) => resolved.push((inf, o)),
                None => still.push(inf),
            }
        }
        self.in_flight = still;
        if resolved.is_empty() {
            return Ok(false);
        }
        // Successful commits are applied to the model in seqno order.
        let mut successes: Vec<(Seqno, InFlight, Option<pigeonhole_engine::CommitInfo>)> =
            Vec::new();
        for (inf, outcome) in resolved {
            let armed = inf.armed;
            match outcome {
                Outcome::Commit(Ok(info)) => successes.push((info.seqno, inf, Some(info))),
                Outcome::Cas(Ok((applied, info))) => {
                    let Kind::Cas { expected, .. } = &inf.commit.kind else {
                        unreachable!()
                    };
                    if applied != *expected {
                        return fail(
                            FailureClass::LiveReadMismatch,
                            format!(
                                "check_and_mutate applied={applied} but the model's predicate said {expected}"
                            ),
                        );
                    }
                    match info {
                        Some(info) => successes.push((info.seqno, inf, Some(info))),
                        None => {
                            self.stats.cas_refused += 1;
                            self.trace.push("  -> predicate false, not applied".into());
                        }
                    }
                }
                Outcome::Commit(Err(Error::Busy)) | Outcome::Cas(Err(Error::Busy)) => {
                    self.stats.busy += 1;
                    self.trace.push("  -> busy (arena full)".into());
                }
                Outcome::Commit(Err(Error::Conflict)) => {
                    let Kind::Txn { snapshot, reads } = &inf.commit.kind else {
                        return fail(FailureClass::Protocol, "conflict on a plain commit".into());
                    };
                    // A real conflict: some commit above the snapshot touched a read key.
                    let real = self.history.values().any(|c| {
                        c.seqno.is_some_and(|s| s > *snapshot)
                            && reads.iter().any(|(t, r, f)| c.touches(t, r, f))
                    });
                    if !real {
                        return fail(
                            FailureClass::Protocol,
                            format!(
                                "spurious conflict: nothing above snapshot {snapshot} touched {reads:?}"
                            ),
                        );
                    }
                    self.stats.conflicts += 1;
                    self.trace.push("  -> conflict".into());
                }
                Outcome::Commit(Err(e)) | Outcome::Cas(Err(e))
                    if !self.alive() || is_crashed(&e) =>
                {
                    let mut c = inf.commit;
                    c.acked = false;
                    self.unacked.push(c);
                    if armed {
                        self.stats.mid_commit_crashes += 1;
                    }
                    for other in self.in_flight.drain(..) {
                        let mut c = other.commit;
                        c.acked = false;
                        self.unacked.push(c);
                    }
                    self.crash_and_recover(CrashKind::Power, true, rng)?;
                    return Ok(true);
                }
                Outcome::Commit(Err(Error::Io(e))) | Outcome::Cas(Err(Error::Io(e))) => {
                    // A failed write or sync poisoned a stream: the commit may have landed
                    // (a sync failure after apply), so it is unacknowledged, and the engine
                    // must be reopened once everything in flight has settled.
                    self.stats.io_errors += 1;
                    self.trace
                        .push(format!("  -> I/O error ({e}); reopen pending"));
                    let mut c = inf.commit;
                    c.acked = false;
                    self.unacked.push(c);
                    self.need_reopen = true;
                }
                Outcome::Commit(Err(e)) | Outcome::Cas(Err(e)) => {
                    return fail(
                        FailureClass::Protocol,
                        format!("unexpected commit error: {e}"),
                    );
                }
            }
            if armed {
                self.vfs.set_faults(self.cfg.faults.clone());
            }
        }
        successes.sort_by_key(|(s, ..)| *s);
        let mut logged: Option<BTreeMap<Seqno, Timestamp>> = None;
        for (seqno, inf, info) in successes {
            let info = info.expect("successes carry info");
            let mut c = inf.commit;
            if self.engine_seqnos.last().is_some_and(|l| *l >= seqno) {
                return fail(
                    FailureClass::Protocol,
                    format!(
                        "engine seqno {seqno} is not above the previous {:?}",
                        self.engine_seqnos.last()
                    ),
                );
            }
            if info.durability != c.durability {
                return fail(
                    FailureClass::Protocol,
                    format!("durability {:?} != {:?}", info.durability, c.durability),
                );
            }
            // The commit timestamp: the clock when the commit ran alone, else the record's.
            let ts = if c.durability == Durability::None {
                // Never logged: it ran alone with the clock past every timestamp floor.
                c.commit_ts.expect("alone commits know their timestamp")
            } else {
                if logged.is_none() {
                    logged = Some(self.logged_timestamps()?);
                }
                match logged.as_ref().and_then(|m| m.get(&seqno)) {
                    Some(ts) => *ts,
                    None => {
                        return fail(
                            FailureClass::Protocol,
                            format!(
                                "acknowledged commit {seqno} ({:?}) has no WAL record",
                                c.durability
                            ),
                        );
                    }
                }
            };
            if let Kind::Txn { snapshot, reads } = &c.kind {
                // Validation must have seen every commit between the snapshot and this one.
                if let Some(bad) = self.history.values().find(|h| {
                    h.seqno.is_some_and(|s| s > *snapshot && s < seqno)
                        && reads.iter().any(|(t, r, f)| h.touches(t, r, f))
                }) {
                    return fail(
                        FailureClass::Protocol,
                        format!(
                            "transaction {seqno} committed although commit {:?} touched its reads after snapshot {snapshot}",
                            bad.seqno
                        ),
                    );
                }
            }
            let m = match self.model.try_commit(&c.ops, ts, c.durability) {
                Ok(m) => m,
                Err(e) => return fail(FailureClass::Protocol, format!("model: {e}")),
            };
            if m != self.engine_seqnos.len() as Seqno + 1 {
                return fail(
                    FailureClass::Protocol,
                    format!("model seqno {m} out of step"),
                );
            }
            self.engine_seqnos.push(seqno);
            c.seqno = Some(seqno);
            c.commit_ts = Some(ts);
            c.acked = true;
            self.stats.commits += 1;
            if c.shards.len() > 1 {
                self.stats.cross_shard += 1;
            }
            self.history.insert(seqno, c);
        }
        // Read-your-writes: everything acknowledged is visible now.
        let snap = self.store().engine.snapshot().map_err(|e| Fail {
            class: FailureClass::Protocol,
            message: e.to_string(),
        })?;
        if let Some(last) = self.engine_seqnos.last()
            && snap.seqno() < *last
        {
            return fail(
                FailureClass::Protocol,
                format!("snapshot {} after commit {last}", snap.seqno()),
            );
        }
        if self.in_flight.is_empty() {
            // Every commit (and every aborted cross-shard one) may have consumed a default
            // timestamp: move the clock past them all, so the next `None` commit, which
            // runs alone, gets exactly the clock (decision D11's `max(now, floor + 1)`).
            if self.pending_advance > 0 {
                self.vfs.advance(1_000 * self.pending_advance);
                self.pending_advance = 0;
            }
            if self.need_reopen {
                self.crash_and_recover(CrashKind::Process, false, rng)?;
            }
            return Ok(true);
        }
        Ok(false)
    }

    /// Takes the next op from the workload (or the lookahead queue).
    fn next_op(&mut self) -> Option<Op> {
        if let Some(op) = self.queued.pop_front() {
            return Some(op);
        }
        self.workload.as_mut().and_then(|w| w.next())
    }

    fn arm(&mut self, rng: &mut Rng) -> bool {
        if rng.chance(self.cfg.mid_commit_crash_ppm) {
            let mut plan = self.cfg.faults.clone();
            plan.crash_after_ops = Some(self.vfs.mutating_ops() + 1 + rng.below(6));
            self.vfs.set_faults(plan);
            true
        } else {
            false
        }
    }

    fn prepare_ops(&self, ops: &mut [ModelOp]) {
        for o in ops.iter_mut() {
            place(o);
            match o {
                ModelOp::Put { ts: Some(t), .. } | ModelOp::DeleteCell { ts: t, .. } => {
                    *t += self.base;
                }
                _ => {}
            }
        }
    }

    /// Submits one plain commit. Returns false when the engine crashed instead.
    fn submit_plain(
        &mut self,
        ops: Vec<ModelOp>,
        durability: Durability,
        alone: bool,
        rng: &mut Rng,
    ) -> Result<bool, Fail> {
        let now = self.now();
        let armed = self.arm(rng);
        let shards = self.shards_of(&ops);
        self.stats.submitted += 1;
        self.trace.push(format!(
            "commit {durability:?} ts={now} shards={shards:?}{} [{}]{}",
            if alone { "" } else { " (batched)" },
            ops.iter().map(show).collect::<Vec<_>>().join("; "),
            if armed { " (crash armed)" } else { "" }
        ));
        let batch = match self.store().batch(&ops) {
            Ok(b) => b,
            Err(e) => return fail(FailureClass::Protocol, format!("batch: {e}")),
        };
        let commit = Committed {
            seqno: None,
            ops,
            commit_ts: alone.then_some(now),
            durability,
            shards,
            acked: false,
            kind: Kind::Plain,
        };
        match self.store().engine.submit(batch, Some(durability)) {
            Ok(pc) => {
                self.in_flight.push(InFlight {
                    pending: Pending::Poll(pc),
                    commit,
                    armed,
                    alone,
                });
                Ok(true)
            }
            Err(e) if is_crashed(&e) => {
                let mut c = commit;
                c.acked = false;
                self.unacked.push(c);
                self.crash_and_recover(CrashKind::Power, true, rng)?;
                Ok(false)
            }
            Err(e) => fail(FailureClass::Protocol, format!("submit: {e}")),
        }
    }

    /// Submits a `check_and_mutate` (alone) on a helper thread.
    fn submit_cas(
        &mut self,
        ops: Vec<ModelOp>,
        durability: Durability,
        rng: &mut Rng,
    ) -> Result<(), Fail> {
        let now = self.now();
        let (table, row) = (op_table(&ops[0]).to_owned(), op_row(&ops[0]).to_vec());
        let ops: Vec<ModelOp> = ops
            .into_iter()
            .filter(|o| op_table(o) == table && op_row(o) == row)
            .collect();
        let fams = families();
        let family = fams[rng.below(fams.len() as u64) as usize].name.clone();
        let qualifier = format!("q{}", rng.below(self.cfg.spec.qualifiers)).into_bytes();
        let current = self
            .model
            .try_get(
                &table,
                &row,
                &family,
                &qualifier,
                self.model.snapshot(),
                now,
            )
            .ok()
            .flatten();
        let pred = match rng.below(4) {
            0 => CasPred::Exists,
            1 => CasPred::Absent,
            2 => CasPred::Equals(
                current
                    .as_ref()
                    .map_or_else(|| b"none".to_vec(), |c| c.value.clone()),
            ),
            _ => CasPred::Equals(b"never".to_vec()),
        };
        let expected = match &pred {
            CasPred::Exists => current.is_some(),
            CasPred::Absent => current.is_none(),
            CasPred::Equals(v) => current.as_ref().is_some_and(|c| &c.value == v),
        };
        let armed = self.arm(rng);
        let shards = self.shards_of(&ops);
        self.stats.submitted += 1;
        let fam_id = self.store().family_ids[&(table.clone(), family.clone())];
        let engine = Arc::clone(&self.store().engine);
        let table_id = self.store().tables[&table].id;
        let engine_pred = match &pred {
            CasPred::Exists => Predicate::Exists {
                family: fam_id,
                qualifier: qualifier.clone(),
            },
            CasPred::Absent => Predicate::Absent {
                family: fam_id,
                qualifier: qualifier.clone(),
            },
            CasPred::Equals(v) => Predicate::Value {
                family: fam_id,
                qualifier: qualifier.clone(),
                predicate: pigeonhole_engine::ValuePredicate::Equals(v.clone()),
            },
        };
        self.trace.push(format!(
            "cas {durability:?} ts={now} {}/{family}:{} {pred:?} (model says {expected}) [{}]{}",
            text(&row),
            text(&qualifier),
            ops.iter().map(show).collect::<Vec<_>>().join("; "),
            if armed { " (crash armed)" } else { "" }
        ));
        let batch = match self.store().batch(&ops) {
            Ok(b) => b,
            Err(e) => return fail(FailureClass::Protocol, format!("batch: {e}")),
        };
        let (tx, rx) = mpsc::channel();
        let row2 = row.clone();
        std::thread::spawn(move || {
            let r = engine.check_and_mutate(table_id, &row2, &engine_pred, batch, Some(durability));
            let _ = tx.send(Outcome::Cas(r));
        });
        self.in_flight.push(InFlight {
            pending: Pending::Thread(rx),
            commit: Committed {
                seqno: None,
                ops,
                commit_ts: Some(now),
                durability,
                shards,
                acked: false,
                kind: Kind::Cas {
                    table,
                    row,
                    family,
                    qualifier,
                    pred,
                    expected,
                },
            },
            armed,
            alone: true,
        });
        self.pending_advance = 2;
        Ok(())
    }

    /// Begins a transaction, reads a few columns (checked against the model), and commits
    /// it on a helper thread.
    fn submit_txn(
        &mut self,
        ops: Vec<ModelOp>,
        durability: Durability,
        rng: &mut Rng,
    ) -> Result<(), Fail> {
        let now = self.now();
        let armed = self.arm(rng);
        let shards = self.shards_of(&ops);
        self.stats.submitted += 1;
        let store = self.store.as_ref().expect("store open");
        let mut txn = match store.engine.begin() {
            Ok(t) => t,
            Err(e) => return fail(FailureClass::Protocol, format!("begin: {e}")),
        };
        let snapshot = txn.snapshot().seqno();
        let fams = families();
        let mut reads = Vec::new();
        for i in 0..1 + rng.below(2) {
            // Read a row the batch writes (likely conflicts) or a random one.
            let (table, row) = if i == 0 {
                (op_table(&ops[0]).to_owned(), op_row(&ops[0]).to_vec())
            } else {
                let row = format!("row{:06}", rng.below(self.cfg.spec.rows)).into_bytes();
                (table_of(&row).to_owned(), row)
            };
            let family = fams[rng.below(fams.len() as u64) as usize].name.clone();
            let qualifier = format!("q{}", rng.below(self.cfg.spec.qualifiers)).into_bytes();
            let fam_id = store.family_ids[&(table.clone(), family.clone())];
            let got = txn
                .get(store.tables[&table].id, fam_id, &row, &qualifier)
                .map(|c| c.map(|c| (c.timestamp(), value_bytes(c.value()))));
            let want = self
                .model
                .try_get(
                    &table,
                    &row,
                    &family,
                    &qualifier,
                    self.model_seqno(snapshot),
                    now,
                )
                .map(|c| c.map(|c| (c.ts, c.value)));
            match (got, want) {
                (Ok(g), Ok(w)) if g == w => {}
                (Err(Error::Merge(_)), Err(_)) => {}
                (g, w) => {
                    return fail(
                        FailureClass::LiveReadMismatch,
                        format!(
                            "txn read {}/{family}:{}: engine {g:?} vs model {w:?}",
                            text(&row),
                            text(&qualifier)
                        ),
                    );
                }
            }
            reads.push((table, row, family));
        }
        let batch = match store.batch(&ops) {
            Ok(b) => b,
            Err(e) => return fail(FailureClass::Protocol, format!("batch: {e}")),
        };
        *txn.batch() = batch;
        // Half the time another writer touches a read key between the reads and the
        // commit, so validation must abort the transaction.
        let interfere = rng.chance(500_000);
        if interfere {
            let (t, r, f) = reads[0].clone();
            let value = if f.starts_with("counter") {
                77i64.to_le_bytes().to_vec()
            } else {
                b"interferer".to_vec()
            };
            let op = ModelOp::Put {
                table: t,
                row: r,
                family: f,
                qualifier: b"q0".to_vec(),
                ts: None,
                value,
            };
            self.vfs.advance(1_000);
            if !self.run_alone_now(vec![op], Durability::Buffered, rng)? {
                // The engine crashed under the interferer; the transaction is moot.
                return Ok(());
            }
        }
        self.trace.push(format!(
            "txn {durability:?} ts={now} snapshot={snapshot} reads={}{} [{}]{}",
            reads
                .iter()
                .map(|(t, r, f)| format!("{t}/{}/{f}", text(r)))
                .collect::<Vec<_>>()
                .join(","),
            if interfere { " (interfered)" } else { "" },
            ops.iter().map(show).collect::<Vec<_>>().join("; "),
            if armed { " (crash armed)" } else { "" }
        ));
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(Outcome::Commit(txn.commit(Some(durability))));
        });
        self.in_flight.push(InFlight {
            pending: Pending::Thread(rx),
            commit: Committed {
                seqno: None,
                ops,
                commit_ts: Some(now),
                durability,
                shards,
                acked: false,
                kind: Kind::Txn { snapshot, reads },
            },
            armed,
            alone: true,
        });
        self.pending_advance = if interfere { 3 } else { 2 };
        Ok(())
    }

    /// Commits `ops` right now, driving the shards until it resolves (used for interference
    /// inside a client step), and applies it to the model. Returns false when the engine
    /// crashed (or a stream failed) instead, after recovering.
    fn run_alone_now(
        &mut self,
        ops: Vec<ModelOp>,
        durability: Durability,
        rng: &mut Rng,
    ) -> Result<bool, Fail> {
        let now = self.now();
        let shards = self.shards_of(&ops);
        let batch = match self.store().batch(&ops) {
            Ok(b) => b,
            Err(e) => return fail(FailureClass::Protocol, format!("batch: {e}")),
        };
        let unacked = Committed {
            seqno: None,
            ops: ops.clone(),
            commit_ts: Some(now),
            durability,
            shards: shards.clone(),
            acked: false,
            kind: Kind::Plain,
        };
        let mut pc = match self.store().engine.submit(batch, Some(durability)) {
            Ok(pc) => pc,
            Err(e) if is_crashed(&e) => {
                self.unacked.push(unacked);
                self.crash_and_recover(CrashKind::Power, true, rng)?;
                return Ok(false);
            }
            Err(e) => return fail(FailureClass::Protocol, format!("submit: {e}")),
        };
        let info = loop {
            match poll_commit(&mut pc) {
                Poll::Ready(Ok(info)) => break info,
                Poll::Ready(Err(Error::Busy)) => {
                    self.stats.busy += 1;
                    return Ok(true);
                }
                Poll::Ready(Err(e)) => {
                    // Crashed, or a poisoned stream: unacknowledged; recover.
                    drop(pc);
                    self.unacked.push(unacked);
                    let alive = self.alive();
                    if alive {
                        self.stats.io_errors += 1;
                        self.trace
                            .push(format!("  interferer failed ({e}); reopening"));
                        self.crash_and_recover(CrashKind::Process, false, rng)?;
                    } else {
                        self.crash_and_recover(CrashKind::Power, true, rng)?;
                    }
                    return Ok(false);
                }
                Poll::Pending => {
                    let now = self.vfs.monotonic_nanos();
                    self.store.as_mut().expect("store open").step_shards(now);
                }
            }
        };
        if !self.alive() {
            // The armed crash fired right after the record reached the kernel: the commit
            // resolved, the engine is dead, and the record may be gone (a power loss).
            drop(pc);
            self.unacked.push(unacked);
            self.stats.mid_commit_crashes += 1;
            self.crash_and_recover(CrashKind::Power, true, rng)?;
            return Ok(false);
        }
        let logged = self.logged_timestamps()?;
        let ts = match logged.get(&info.seqno) {
            Some(ts) => *ts,
            None => {
                return fail(
                    FailureClass::Protocol,
                    format!(
                        "interferer {} has no WAL record (records seen: {:?})",
                        info.seqno,
                        logged.keys().collect::<Vec<_>>()
                    ),
                );
            }
        };
        self.trace.push(format!(
            "interferer {durability:?} ts={ts} seqno={} [{}]",
            info.seqno,
            ops.iter().map(show).collect::<Vec<_>>().join("; ")
        ));
        let m = match self.model.try_commit(&ops, ts, durability) {
            Ok(m) => m,
            Err(e) => return fail(FailureClass::Protocol, format!("model: {e}")),
        };
        if m != self.engine_seqnos.len() as Seqno + 1 {
            return fail(
                FailureClass::Protocol,
                format!("model seqno {m} out of step"),
            );
        }
        self.engine_seqnos.push(info.seqno);
        self.stats.commits += 1;
        self.history.insert(
            info.seqno,
            Committed {
                seqno: Some(info.seqno),
                ops,
                commit_ts: Some(ts),
                durability,
                shards,
                acked: true,
                kind: Kind::Plain,
            },
        );
        Ok(true)
    }

    fn step(&mut self, op: Op, rng: &mut Rng) -> Result<(), Fail> {
        if self.trace_on() {
            eprintln!("op #{} {op:?}", self.op_index);
        }
        self.vfs.advance(1_000);
        let now = self.now();
        match op {
            Op::Commit(mut ops, durability) => {
                let durability = self.cfg.durability.unwrap_or(durability);
                self.prepare_ops(&mut ops);
                if rng.chance(self.cfg.cas_ppm) {
                    return self.submit_cas(ops, durability, rng);
                }
                if rng.chance(self.cfg.txn_ppm) {
                    return self.submit_txn(ops, durability, rng);
                }
                // A batch of concurrent commits: this one plus following logged commits of
                // the workload (a `None` commit leaves no record to read its timestamp from).
                let want = if durability != Durability::None {
                    1 + rng.below(self.cfg.tasks.max(1) as u64) as usize
                } else {
                    1
                };
                let mut batch: Vec<(Vec<ModelOp>, Durability)> = vec![(ops, durability)];
                while batch.len() < want {
                    match self.next_op() {
                        Some(Op::Commit(mut more, d)) => {
                            // Raw ops go back to the queue; only accepted ones are prepared
                            // (the base offset must be added exactly once).
                            let d = self.cfg.durability.unwrap_or(d);
                            if d == Durability::None {
                                self.queued.push_front(Op::Commit(more, d));
                                break;
                            }
                            self.prepare_ops(&mut more);
                            batch.push((more, d));
                        }
                        Some(other) => {
                            self.queued.push_front(other);
                            break;
                        }
                        None => break,
                    }
                }
                if batch.len() > 1 {
                    let n = batch.len();
                    self.stats.batched += n;
                    for (ops, d) in batch {
                        if !self.submit_plain(ops, d, false, rng)? {
                            break;
                        }
                    }
                    self.pending_advance = n as u64 + 1;
                } else {
                    let (ops, d) = batch.pop().expect("one");
                    self.submit_plain(ops, d, true, rng)?;
                    self.pending_advance = 1;
                }
            }
            Op::Get {
                row,
                family,
                qualifier,
            } => {
                let snap = self.pick_snapshot(rng)?;
                let ms = self.model_seqno(snap.seqno());
                self.trace.push(format!(
                    "get {}/{family}:{} @engine {} (model {ms})",
                    text(&row),
                    text(&qualifier),
                    snap.seqno()
                ));
                let got = self.store().get(&snap, &row, &family, &qualifier);
                let want = self
                    .model
                    .try_get(table_of(&row), &row, &family, &qualifier, ms, now);
                match (got, want) {
                    (Ok(g), Ok(w)) if g == w => {}
                    (Err(Error::Merge(_)), Err(pigeonhole_sim::ModelError::MergeFailed(_))) => {}
                    (g, w) => {
                        return fail(
                            FailureClass::LiveReadMismatch,
                            format!(
                                "get {}/{family}:{} at engine seqno {}: engine {:?} vs model {:?}",
                                text(&row),
                                text(&qualifier),
                                snap.seqno(),
                                g.map(|c| c.as_ref().map(show_cell)),
                                w.map(|c| c.as_ref().map(show_cell))
                            ),
                        );
                    }
                }
            }
            Op::Scan { start, end } => {
                let snap = self.pick_snapshot(rng)?;
                let ms = self.model_seqno(snap.seqno());
                self.trace.push(format!(
                    "scan [{}, {}) @engine {} (model {ms})",
                    text(&start),
                    text(&end),
                    snap.seqno()
                ));
                for t in TABLES {
                    let got = self.store().scan(
                        &snap,
                        t,
                        Bound::Included(&start),
                        Bound::Excluded(&end),
                        1,
                    );
                    let want = self.model.try_scan(
                        t,
                        Bound::Included(&start),
                        Bound::Excluded(&end),
                        &[],
                        ms,
                        now,
                    );
                    match (got, want) {
                        (Ok(g), Ok(w)) => {
                            if let Some(d) = first_diff(&g, &w) {
                                return fail(
                                    FailureClass::LiveReadMismatch,
                                    format!("scan of {t} at engine seqno {}: {d}", snap.seqno()),
                                );
                            }
                        }
                        (Err(Error::Merge(_)), Err(pigeonhole_sim::ModelError::MergeFailed(_))) => {
                        }
                        (g, w) => {
                            return fail(
                                FailureClass::LiveReadMismatch,
                                format!(
                                    "scan of {t}: engine {:?} vs model {:?}",
                                    g.map(|_| ()),
                                    w.map(|_| ())
                                ),
                            );
                        }
                    }
                }
                if rng.chance(self.cfg.dump_ppm) {
                    self.trace
                        .push(format!("full dump @engine {}", snap.seqno()));
                    self.compare_dump(&snap, FailureClass::LiveReadMismatch)?;
                }
            }
            Op::Snapshot => {
                let snap = match self.store().engine.snapshot() {
                    Ok(s) => s,
                    Err(e) => return fail(FailureClass::Protocol, format!("snapshot: {e}")),
                };
                self.trace.push(format!("snapshot engine {}", snap.seqno()));
                self.snaps.push(snap);
                if self.snaps.len() > 8 {
                    self.snaps.remove(0);
                }
            }
        }
        if self.in_flight.is_empty() {
            self.maybe_crash(rng)?;
        }
        Ok(())
    }

    fn pick_snapshot(&self, rng: &mut Rng) -> Result<Snapshot, Fail> {
        if !self.snaps.is_empty() && rng.chance(300_000) {
            Ok(self.snaps[rng.below(self.snaps.len() as u64) as usize].clone())
        } else {
            self.store().engine.snapshot().map_err(|e| Fail {
                class: FailureClass::Protocol,
                message: format!("snapshot: {e}"),
            })
        }
    }
}

fn is_crashed(e: &Error) -> bool {
    match e {
        Error::Io(io) => io.kind == ErrorKind::Crashed,
        Error::Closed => true,
        _ => false,
    }
}

/// Runs one seeded model check.
pub fn run(seed: u64, cfg: &Config) -> Result<Stats, Failure> {
    let sim = Sim::with_faults(seed, cfg.faults.clone());
    let vfs = sim.vfs();
    if let Some(n) = cfg.crash_at {
        let mut plan = cfg.faults.clone();
        plan.crash_after_ops = Some(n);
        vfs.set_faults(plan);
    }
    let probe = vfs
        .open(Path::new("/db/probe"), OpenOptions::read_write_create())
        .expect("probe");
    let model = new_model();
    let store = match Store::open(&vfs, cfg.shards, cfg.memtable_budget, &families()) {
        Ok(s) => s,
        Err(e) => {
            // A crash-at-every-point sweep can hit the create itself.
            if cfg.crash_at.is_some() {
                let mut plan = cfg.faults.clone();
                plan.crash_after_ops = None;
                vfs.set_faults(plan);
                let probe = vfs
                    .open(Path::new("/db/probe"), OpenOptions::read_write_create())
                    .expect("probe");
                let store = Store::open(&vfs, cfg.shards, cfg.memtable_budget, &families())
                    .unwrap_or_else(|e| panic!("reopen after a crash during create: {e}"));
                return run_with(seed, cfg, sim, vfs, probe, model, store, 1);
            }
            panic!("open on a fresh filesystem: {e}");
        }
    };
    run_with(seed, cfg, sim, vfs, probe, model, store, 0)
}

#[allow(clippy::too_many_arguments)]
fn run_with(
    seed: u64,
    cfg: &Config,
    mut sim: Sim,
    vfs: Arc<SimVfs>,
    probe: FileRef,
    model: Model,
    store: Store,
    reopens: usize,
) -> Result<Stats, Failure> {
    let base = vfs.now_micros();
    let workload = Workload::new(seed ^ 0x5eed, TABLE, cfg.spec.clone()).take(cfg.ops);
    let world = Rc::new(RefCell::new(World {
        seed,
        cfg: cfg.clone(),
        vfs: Arc::clone(&vfs),
        probe,
        store: Some(store),
        model,
        engine_seqnos: Vec::new(),
        history: BTreeMap::new(),
        unacked: Vec::new(),
        in_flight: Vec::new(),
        need_reopen: false,
        pending_advance: 0,
        base,
        snaps: Vec::new(),
        trace: Vec::new(),
        op_index: 0,
        stats: Stats::default(),
        failure: None,
        reopens,
        done: false,
        workload: Some(workload),
        queued: std::collections::VecDeque::new(),
    }));

    // Shard drivers as scheduler tasks: whichever shard the scheduler picks runs one slice.
    let max_shards = cfg
        .shards
        .max(cfg.reopen_shards.iter().copied().max().unwrap_or(0));
    for i in 0..max_shards {
        let world = world.clone();
        let vfs = Arc::clone(&vfs);
        sim.spawn(
            "shard",
            Box::new(move |_rng| {
                let mut w = world.borrow_mut();
                if w.done {
                    return Step::Done;
                }
                let now = vfs.monotonic_nanos();
                if let Some(store) = w.store.as_mut()
                    && i < store.shards.len()
                {
                    store.shards[i].run_once(now + 1_000);
                }
                Step::Ready
            }),
        );
    }
    {
        let world = world.clone();
        sim.spawn(
            "client",
            Box::new(move |rng| {
                let mut w = world.borrow_mut();
                if w.failure.is_some() || w.done {
                    w.done = true;
                    return Step::Done;
                }
                let outcome = (|| -> Result<bool, Fail> {
                    if !w.poll_in_flight(rng)? {
                        return Ok(true);
                    }
                    let Some(op) = w.next_op() else {
                        return Ok(false);
                    };
                    w.op_index += 1;
                    w.stats.ops += 1;
                    w.step(op, rng)?;
                    Ok(true)
                })();
                match outcome {
                    Ok(true) => Step::Ready,
                    Ok(false) => {
                        w.done = true;
                        Step::Done
                    }
                    Err(f) => {
                        let failure = Failure {
                            seed: w.seed,
                            class: f.class,
                            op_index: w.op_index.saturating_sub(1),
                            message: f.message,
                            trace: std::mem::take(&mut w.trace),
                        };
                        w.failure = Some(failure);
                        w.done = true;
                        Step::Done
                    }
                }
            }),
        );
    }
    let budget = (cfg.ops as u64 + 10) * 4_000;
    let finished = sim.run_until(budget, &mut || world.borrow().done);
    let mut w = world.borrow_mut();
    if !finished && w.failure.is_none() {
        w.failure = Some(Failure {
            seed,
            class: FailureClass::Protocol,
            op_index: w.op_index,
            message: format!(
                "the run did not finish within {budget} scheduler steps (a hung commit?)"
            ),
            trace: std::mem::take(&mut w.trace),
        });
    }
    if w.failure.is_none() {
        // Finish with one more crash so the tail of every run is checked.
        let mut rng = sim.rng().fork();
        let kind = if rng.chance(500_000) {
            CrashKind::Power
        } else {
            CrashKind::Process
        };
        w.op_index = cfg.ops;
        if let Err(f) = w.crash_and_recover(kind, false, &mut rng) {
            w.failure = Some(Failure {
                seed,
                class: f.class,
                op_index: cfg.ops,
                message: f.message,
                trace: std::mem::take(&mut w.trace),
            });
        }
    }
    let result = match w.failure.take() {
        Some(f) => Err(f),
        None => Ok(w.stats),
    };
    // Tear down: close, then drive the shards so they finish the close.
    w.in_flight.clear();
    if let Some(mut store) = w.store.take() {
        let _ = store.engine.close();
        for _ in 0..4 {
            store.step_shards(vfs.monotonic_nanos());
        }
    }
    result
}

/// The final state of a run as a dump plus every read's result (for cross-shard-count
/// equivalence tests), with optional process crashes and reopens at fixed points.
pub fn final_dump(seed: u64, cfg: &Config) -> Rows {
    let sim = Sim::with_faults(seed, cfg.faults.clone());
    let vfs = sim.vfs();
    let mut store = Store::open(&vfs, cfg.shards, cfg.memtable_budget, &families()).expect("open");
    let base = vfs.now_micros();
    let mut results: Vec<String> = Vec::new();
    for (i, op) in Workload::new(seed ^ 0x5eed, TABLE, cfg.spec.clone())
        .take(cfg.ops)
        .enumerate()
    {
        vfs.advance(1_000);
        if let Some(every) = cfg.crash_every
            && i > 0
            && i % every == 0
        {
            vfs.crash(CrashKind::Process);
            drop(store);
            store =
                Store::open(&vfs, cfg.shards, cfg.memtable_budget, &families()).expect("reopen");
            results.push(format!(
                "reopen at {i}: seqno {}",
                store.engine.snapshot().unwrap().seqno()
            ));
        }
        match op {
            Op::Commit(mut ops, durability) => {
                for o in &mut ops {
                    place(o);
                    match o {
                        ModelOp::Put { ts: Some(t), .. } | ModelOp::DeleteCell { ts: t, .. } => {
                            *t += base
                        }
                        _ => {}
                    }
                }
                let batch = store.batch(&ops).expect("batch");
                let mut pc = store
                    .engine
                    .submit(batch, Some(durability))
                    .expect("submit");
                let info = loop {
                    match poll_commit(&mut pc) {
                        Poll::Ready(r) => break r.expect("commit"),
                        Poll::Pending => store.step_shards(vfs.monotonic_nanos()),
                    }
                };
                results.push(format!("commit {}", info.seqno));
            }
            Op::Get {
                row,
                family,
                qualifier,
            } => {
                let snap = store.engine.snapshot().unwrap();
                let got = store.get(&snap, &row, &family, &qualifier).expect("get");
                results.push(format!("get {:?}", got.map(|c| show_cell(&c))));
            }
            Op::Scan { start, end } => {
                let snap = store.engine.snapshot().unwrap();
                for t in TABLES {
                    let rows = store
                        .scan(&snap, t, Bound::Included(&start), Bound::Excluded(&end), 1)
                        .expect("scan");
                    results.push(format!("scan {t} {rows:?}"));
                }
            }
            Op::Snapshot => {}
        }
    }
    let snap = store.engine.snapshot().unwrap();
    let mut dump = store.dump(&snap).expect("dump");
    dump.push((
        b"__results__".to_vec(),
        results
            .into_iter()
            .map(|r| ModelCell {
                family: r,
                qualifier: Vec::new(),
                ts: 0,
                value: Vec::new(),
            })
            .collect(),
    ));
    let _ = store.engine.close();
    for _ in 0..4 {
        store.step_shards(vfs.monotonic_nanos());
    }
    drop(sim);
    dump
}

pub fn db_path() -> PathBuf {
    PathBuf::from(DB)
}
