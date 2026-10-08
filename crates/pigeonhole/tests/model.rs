//! The full-stack model check (decision D1): the sim `Workload` runs through the **public**
//! API (`RowMutation`, `WriteBatch`, `get`/`get_at`, row reads, scans with both the owned
//! iterator and the lending cursor, snapshots) on `SimVfs`, and every read is compared with
//! `pigeonhole_sim::Model`.
//!
//! **Commits.** One at a time, blocking, in engine-owned mode (shard threads). The simulated
//! clock only moves when the harness advances it (one microsecond per operation), so a
//! default commit timestamp is the clock at the commit (decision D11) and the model gets the
//! same value. A commit that touches one row uses `RowMutation` (every mutation kind); a
//! multi-row commit uses `WriteBatch` (every mutation kind too). A counter base at an
//! explicit timestamp is dropped on both sides: there is no typed `put_at`.
//!
//! **Crashes.** Process crashes and power losses between operations, and power losses in
//! the middle of a commit (`FaultPlan::crash_after_ops`). Crash runs use one shard, so every
//! commit is a single-shard record in one WAL stream (D84). The public API does not show
//! how many records survived, so after every crash each possible surviving prefix of the
//! stream, longest first, goes through `pigeonhole_sim`'s oracle: `recovered_from_records`
//! names the surviving commits, `check_acknowledged_survive` rejects a prefix that drops a
//! commit the durability levels promised (D42), and `Model::from_commits` rebuilds the
//! model to compare with the reopened store. Recovery must match one allowed prefix.
//! Multi-shard runs reopen cleanly instead.
//!
//! An armed power loss can fire on a shard's background I/O (a flush or a compaction)
//! rather than on a commit. A later step that fails while one is armed and the liveness
//! probe shows the crash fired is that power loss and recovers from it (issues #56, #62).
//! Crash runs also `flush` between operations (2%), so a power loss can land in a flush or
//! a compaction while a commit waits in a write stall (issue #70).
//!
//! Seeds: `PIGEONHOLE_SEED` (first seed, default 1) and `PIGEONHOLE_SEEDS` (count, default
//! 1). A failure prints its seed and the operation trace. Tablet changes are on, as by
//! default, with a fast balancer so tablets split, move and merge within a run;
//! `PIGEONHOLE_TABLET_CHANGES=0` runs every test with them off.
//! `PIGEONHOLE_DEFERRED_IO=1` defers submitted I/O (`SimVfs::set_deferred_io`): a device
//! thread completes it in an order the seed picks, so WAL group syncs and root commits stay
//! in flight while the shard threads go on. The OS schedules those threads, so such a run
//! does not replay exactly.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::ops::Bound;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError, mpsc};
use std::time::{Duration, Instant};

use pigeonhole::{Compaction, Durability, ErrorCode, Family, Options, Pigeonhole, Snapshot, Table};
use pigeonhole_io::sim::{CrashKind, FaultPlan, SimVfs};
use pigeonhole_io::{FileRef, OpenOptions, Vfs};
use pigeonhole_sim::{
    CommitStreams, Model, ModelCell, ModelFamily, ModelOp, Op, Rng, StreamCommit, StreamRecord,
    Workload, WorkloadSpec, check_acknowledged_survive, recovered_from_records,
};

const DB: &str = "/db/model.phdb";
/// A file whose handle dies with every other one at a crash: the liveness probe.
const PROBE: &str = "/db/probe";

/// Rows are spread over several tables (deterministically by row) so batches span shards.
const TABLES: [&str; 4] = ["t0", "t1", "t2", "t3"];

type Rows = Vec<(Vec<u8>, Vec<ModelCell>)>;

fn table_index(row: &[u8]) -> usize {
    let h = row.iter().fold(0usize, |h, b| {
        h.wrapping_mul(31).wrapping_add(usize::from(*b))
    });
    h % TABLES.len()
}

fn families() -> Vec<ModelFamily> {
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

/// `g` compacts tiered and `ttl` FIFO by time (#31, #32), and `f` uses zstd (#44); the
/// model depends on neither.
fn public_family(f: &ModelFamily) -> Family {
    let compaction = match f.name.as_str() {
        "g" => Compaction::Tiered,
        "ttl" => Compaction::FifoByTime,
        _ => Compaction::Leveled,
    };
    let family = Family::default()
        .max_versions(f.max_versions)
        .ttl(Duration::from_micros(f.ttl_micros))
        .compaction(compaction);
    // `f` stores its blocks with zstd (#44).
    let family = if f.name == "f" {
        family.zstd(3)
    } else {
        family
    };
    if f.i64_add {
        family
    } else {
        family.merge_operator("")
    }
}

fn create_tables(m: &mut Model) {
    for t in TABLES {
        m.create_table(t, families());
    }
}

fn new_model() -> Model {
    let mut m = Model::new();
    create_tables(&mut m);
    m
}

#[derive(Clone)]
struct Config {
    ops: usize,
    shards: usize,
    spec: WorkloadSpec,
    faults: FaultPlan,
    /// Per-op probability (ppm) of a crash between operations.
    crash_ppm: u32,
    /// Per-commit probability (ppm) of a power loss in the middle of the commit.
    mid_commit_crash_ppm: u32,
    /// Per-op probability (ppm) of a clean close and reopen.
    reopen_ppm: u32,
    /// Every commit uses this level instead of the workload's.
    durability: Option<Durability>,
    /// Memtable arena per shard (a memtable freezes and flushes at a quarter of it).
    memtable_budget: u64,
    /// Block cache capacity in bytes.
    block_cache: usize,
    /// Per-op probability (ppm) of an explicit `flush`.
    flush_ppm: u32,
    /// Per-op probability (ppm) of an explicit `compact`.
    compact_ppm: u32,
    /// `Options::tablet_changes`, with a balancer tuned so tablets split, move and merge
    /// within a run. On unless `PIGEONHOLE_TABLET_CHANGES=0`.
    tablet_changes: bool,
    /// With `tablet_changes`, tune the balancer so tablets change within a short run (else
    /// the engine's defaults, under which nothing changes in these runs).
    fast_balancer: bool,
    /// Defer submitted I/O to a device thread. Defaults to `PIGEONHOLE_DEFERRED_IO`.
    deferred_io: bool,
}

impl Config {
    fn quiet(ops: usize, shards: usize) -> Self {
        let mut spec = WorkloadSpec::default();
        spec.rows = 24;
        spec.qualifiers = 3;
        spec.families = families().into_iter().map(|f| f.name).collect();
        spec.max_value_len = 160;
        spec.max_batch = 5;
        Self {
            ops,
            shards,
            spec,
            faults: FaultPlan::none(),
            crash_ppm: 0,
            mid_commit_crash_ppm: 0,
            reopen_ppm: 10_000,
            durability: None,
            memtable_budget: 4 << 20,
            block_cache: 1 << 20,
            flush_ppm: 20_000,
            compact_ppm: 20_000,
            tablet_changes: std::env::var("PIGEONHOLE_TABLET_CHANGES").as_deref() != Ok("0"),
            fast_balancer: true,
            deferred_io: std::env::var("PIGEONHOLE_DEFERRED_IO").is_ok_and(|v| v == "1"),
        }
    }

    fn crashing(ops: usize) -> Self {
        let mut c = Self::quiet(ops, 1);
        c.faults.torn_writes = true;
        c.faults.reorder_unsynced = true;
        c.crash_ppm = 15_000;
        c.mid_commit_crash_ppm = 25_000;
        c
    }
}

/// A commit in commit order: acknowledged, or in flight at a crash.
#[derive(Clone)]
struct Logged {
    ops: Vec<ModelOp>,
    ts: u64,
    /// The level the commit asked for.
    durability: Durability,
    acked: bool,
}

#[derive(Debug, Default)]
struct Stats {
    commits: usize,
    crashes: usize,
    mid_commit_crashes: usize,
    reopens: usize,
    reads: usize,
    flushes: usize,
    compactions: usize,
    busy: usize,
    /// The database file's length at the end of the run.
    file_len: u64,
}

struct Run {
    cfg: Config,
    vfs: Arc<SimVfs>,
    db: Option<(Pigeonhole, Vec<Table>)>,
    /// Reopened with the store; its handle dies with the store's at a crash.
    probe: Option<FileRef>,
    model: Model,
    log: Vec<Logged>,
    /// Saved snapshots with the model seqno each one corresponds to.
    snaps: Vec<(Snapshot, u64)>,
    base: u64,
    /// A power loss is scheduled (`FaultPlan::crash_after_ops`) and may have fired already:
    /// the next I/O error is that crash, and the next crash or reopen is one.
    armed: bool,
    trace: Vec<String>,
    stats: Stats,
    deletes: Deletes,
}

/// Every delete generated so far (lost ones too: dropping more is harmless), so `prepare`
/// can leave out the explicit-timestamp writes whose result depends on whether a bottommost
/// compaction has purged a delete or old versions yet (decision D74). The engine compacts in
/// the background and the public API reports no purges to replay on the model (issue #45),
/// so such a write would make a read depend on timing (issues #67, #68).
#[derive(Default)]
struct Deletes {
    /// Newest `delete_column` timestamp per column.
    columns: BTreeMap<ColumnId, u64>,
    /// Newest family or row delete timestamp per `(table, row, family)`.
    families: BTreeMap<(String, Vec<u8>, String), u64>,
    /// `delete_cell` targets: column and timestamp.
    cells: BTreeSet<(ColumnId, u64)>,
}

/// `(table, row, family, qualifier)`.
type ColumnId = (String, Vec<u8>, String, Vec<u8>);

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// A borrowed public row as model cells.
fn ref_cells(row: &pigeonhole::RowRef<'_>) -> Vec<ModelCell> {
    row.iter()
        .map(|e| ModelCell {
            family: e.family.to_owned(),
            qualifier: e.qualifier.to_vec(),
            ts: e.cell.timestamp(),
            value: e.cell.value().to_vec(),
        })
        .collect()
}

/// An owned public row as model cells (through `Row::entry`).
fn owned_cells(row: &pigeonhole::Row) -> Vec<ModelCell> {
    (0..row.len())
        .map(|i| {
            let (family, qualifier, cell) = row.entry(i).expect("in range");
            ModelCell {
                family: family.to_owned(),
                qualifier: qualifier.to_vec(),
                ts: cell.timestamp(),
                value: cell.value().to_vec(),
            }
        })
        .collect()
}

/// One mutation, compactly, for traces.
fn show(op: &ModelOp) -> String {
    match op {
        ModelOp::Put {
            table,
            row,
            family,
            qualifier,
            ts,
            value,
        } => format!(
            "put {table}/{} {family}:{} ts={ts:?} len={}",
            text(row),
            text(qualifier),
            value.len()
        ),
        ModelOp::Incr {
            table,
            row,
            family,
            qualifier,
            delta,
        } => {
            format!(
                "incr {table}/{} {family}:{} {delta}",
                text(row),
                text(qualifier)
            )
        }
        ModelOp::DeleteCell {
            table,
            row,
            family,
            qualifier,
            ts,
        } => {
            format!(
                "del_cell {table}/{} {family}:{} ts={ts}",
                text(row),
                text(qualifier)
            )
        }
        ModelOp::DeleteColumn {
            table,
            row,
            family,
            qualifier,
        } => {
            format!("del_col {table}/{} {family}:{}", text(row), text(qualifier))
        }
        ModelOp::DeleteFamily { table, row, family } => {
            format!("del_fam {table}/{} {family}", text(row))
        }
        ModelOp::DeleteRow { table, row } => format!("del_row {table}/{}", text(row)),
    }
}

fn show_rows(rows: Option<&(Vec<u8>, Vec<ModelCell>)>) -> String {
    let Some((key, cells)) = rows else {
        return "-".into();
    };
    let cells: Vec<String> = cells
        .iter()
        .map(|c| {
            format!(
                "{}:{}@{} len={}",
                c.family,
                text(&c.qualifier),
                c.ts,
                c.value.len()
            )
        })
        .collect();
    format!("{} [{}]", text(key), cells.join(", "))
}

fn first_diff(got: &Rows, want: &Rows) -> Option<String> {
    for i in 0..got.len().max(want.len()) {
        if got.get(i) != want.get(i) {
            return Some(format!(
                "row {i}: store {} vs model {}",
                show_rows(got.get(i)),
                show_rows(want.get(i))
            ));
        }
    }
    None
}

impl Run {
    fn new(seed: u64, cfg: Config) -> Result<Self, String> {
        let vfs = SimVfs::new(seed);
        if cfg.deferred_io {
            vfs.set_deferred_io(true);
            // Ends with the `SimVfs`.
            drop(vfs.complete_io_in_background());
        }
        let base = vfs.now_micros();
        let mut run = Run {
            cfg,
            vfs,
            db: None,
            probe: None,
            model: new_model(),
            log: Vec::new(),
            snaps: Vec::new(),
            base,
            armed: false,
            trace: Vec::new(),
            stats: Stats::default(),
            deletes: Deletes::default(),
        };
        run.open()?;
        Ok(run)
    }

    fn open(&mut self) -> Result<(), String> {
        let mut options = Options::default()
            .vfs(Arc::clone(&self.vfs) as _)
            .shards(self.cfg.shards)
            .memtable_budget(self.cfg.memtable_budget)
            .wal_segment_size(256 << 10)
            .block_cache(self.cfg.block_cache)
            .tablet_changes(self.cfg.tablet_changes);
        if self.cfg.fast_balancer {
            // The clock moves 1 µs per operation: a balancer pass every ~15 operations.
            options = options.tablet_balance(Duration::from_micros(15), 3, 8 << 10);
        }
        let db = Pigeonhole::open(DB, options).map_err(|e| format!("open: {e}"))?;
        let mut tables = Vec::new();
        for name in TABLES {
            let mut builder = db.table(name).map_err(|e| e.to_string())?;
            for f in families() {
                builder = builder.family(&f.name, public_family(&f));
            }
            tables.push(
                builder
                    .create_if_missing()
                    .map_err(|e| format!("table {name}: {e}"))?,
            );
        }
        self.db = Some((db, tables));
        self.probe = Some(
            self.vfs
                .open(Path::new(PROBE), OpenOptions::read_write_create())
                .map_err(|e| format!("probe: {e}"))?,
        );
        Ok(())
    }

    /// Whether a crash killed the handles since the last open: with a power loss armed,
    /// whether it has fired (on a commit or on a shard's background I/O). Only a dead probe
    /// is evidence; with no probe open there is none.
    fn fired(&self) -> bool {
        self.probe.as_ref().is_some_and(|p| p.len().is_err())
    }

    /// A step failed with `e` while a power loss was armed and has fired: the failure is
    /// that crash (it may have hit a background flush or compaction, so a read finds the
    /// store dead), and the run recovers from it.
    fn recover_fired(&mut self, e: &pigeonhole::Error) -> Result<(), String> {
        self.trace.push(format!(
            "  -> the armed power loss fired ({e}, {:?})",
            e.code()
        ));
        self.crash_and_recover(CrashKind::Power, true)
    }

    fn db(&self) -> &Pigeonhole {
        &self.db.as_ref().expect("open").0
    }

    fn table(&self, i: usize) -> &Table {
        &self.db.as_ref().expect("open").1[i]
    }

    fn now(&self) -> u64 {
        self.vfs.now_micros()
    }

    /// Applies `ops` through the public API. Returns the commit result.
    fn commit(&self, ops: &[ModelOp], durability: Durability) -> pigeonhole::Result<()> {
        let key = |op: &ModelOp| -> (usize, Vec<u8>) {
            let row = match op {
                ModelOp::Put { row, .. }
                | ModelOp::Incr { row, .. }
                | ModelOp::DeleteCell { row, .. }
                | ModelOp::DeleteColumn { row, .. }
                | ModelOp::DeleteFamily { row, .. }
                | ModelOp::DeleteRow { row, .. } => row,
            };
            (table_index(row), row.clone())
        };
        let (ti, row) = key(&ops[0]);
        if ops.iter().all(|op| key(op) == (ti, row.clone())) {
            let mut m = self.table(ti).mutate(&row).durability(durability);
            for op in ops {
                m = match op {
                    ModelOp::Put {
                        family,
                        qualifier,
                        ts,
                        value,
                        ..
                    } => match (family.starts_with("counter"), ts) {
                        (true, _) => {
                            let v = <[u8; 8]>::try_from(&value[..]).expect("8-byte counter base");
                            m.put_i64(family, qualifier, i64::from_le_bytes(v))
                        }
                        (false, None) => m.put(family, qualifier, value),
                        (false, Some(ts)) => m.put_at(family, qualifier, *ts, value),
                    },
                    ModelOp::Incr {
                        family,
                        qualifier,
                        delta,
                        ..
                    } => m.incr(family, qualifier, *delta),
                    ModelOp::DeleteCell {
                        family,
                        qualifier,
                        ts,
                        ..
                    } => m.delete_cell(family, qualifier, *ts),
                    ModelOp::DeleteColumn {
                        family, qualifier, ..
                    } => m.delete_column(family, qualifier),
                    ModelOp::DeleteFamily { family, .. } => m.delete_family(family),
                    ModelOp::DeleteRow { .. } => m.delete_row(),
                };
            }
            m.commit().map(|_| ())
        } else {
            let mut wb = self.db().write_batch();
            for op in ops {
                let (ti, row) = key(op);
                let t = self.table(ti);
                match op {
                    ModelOp::Put {
                        family,
                        qualifier,
                        ts: None,
                        value,
                        ..
                    } if family.starts_with("counter") => {
                        let v = <[u8; 8]>::try_from(&value[..]).expect("8-byte counter base");
                        wb.put_i64(t, &row, family, qualifier, i64::from_le_bytes(v))
                    }
                    ModelOp::Put {
                        family,
                        qualifier,
                        ts: None,
                        value,
                        ..
                    } => wb.put(t, &row, family, qualifier, value),
                    ModelOp::Put {
                        family,
                        qualifier,
                        ts: Some(ts),
                        value,
                        ..
                    } => wb.put_at(t, &row, family, qualifier, *ts, value),
                    ModelOp::Incr {
                        family,
                        qualifier,
                        delta,
                        ..
                    } => wb.incr(t, &row, family, qualifier, *delta),
                    ModelOp::DeleteColumn {
                        family, qualifier, ..
                    } => wb.delete_column(t, &row, family, qualifier),
                    ModelOp::DeleteCell {
                        family,
                        qualifier,
                        ts,
                        ..
                    } => wb.delete_cell(t, &row, family, qualifier, *ts),
                    ModelOp::DeleteFamily { family, .. } => wb.delete_family(t, &row, family),
                    ModelOp::DeleteRow { .. } => wb.delete_row(t, &row),
                };
            }
            assert_eq!(wb.len(), ops.len());
            wb.commit_with(durability).map(|_| ())
        }
    }

    /// Prepares a generated commit: tables by row, explicit timestamps after the base, no
    /// counter base at an explicit timestamp (there is no typed `put_at`), and none of the
    /// writes a purge changes (see [`Deletes`]): a put at a timestamp an earlier delete
    /// covers (a later write with an explicit older timestamp), or a `delete_cell` in a
    /// family with `max_versions` (it may uncover an older version, or not once purged).
    fn prepare(&mut self, ops: Vec<ModelOp>) -> Vec<ModelOp> {
        let mut ops: Vec<ModelOp> = ops
            .into_iter()
            .map(|mut op| {
                let (table, row) = match &mut op {
                    ModelOp::Put { table, row, .. }
                    | ModelOp::Incr { table, row, .. }
                    | ModelOp::DeleteCell { table, row, .. }
                    | ModelOp::DeleteColumn { table, row, .. }
                    | ModelOp::DeleteFamily { table, row, .. }
                    | ModelOp::DeleteRow { table, row, .. } => (table, row),
                };
                *table = TABLES[table_index(row)].to_owned();
                if let ModelOp::Put { ts: Some(t), .. } | ModelOp::DeleteCell { ts: t, .. } =
                    &mut op
                {
                    *t += self.base;
                }
                op
            })
            .collect();
        // There is no typed put at an explicit timestamp: a counter's base is `put_i64`.
        ops.retain(|op| {
            !matches!(op, ModelOp::Put { family, ts: Some(_), .. } if family.starts_with("counter"))
        });
        // This commit's timestamp (the clock now, decision D11).
        let now = self.now();
        let d = &self.deletes;
        ops.retain(|op| match op {
            ModelOp::Put {
                table,
                row,
                family,
                qualifier,
                ts,
                ..
            } => {
                let ts = &ts.unwrap_or(now);
                let col = (
                    table.clone(),
                    row.clone(),
                    family.clone(),
                    qualifier.clone(),
                );
                let fam = (table.clone(), row.clone(), family.clone());
                !(d.columns.get(&col).is_some_and(|c| ts <= c)
                    || d.families.get(&fam).is_some_and(|f| ts <= f)
                    || d.cells.contains(&(col, *ts)))
            }
            ModelOp::DeleteCell { family, .. } => families()
                .iter()
                .any(|f| f.name == *family && f.max_versions == 0),
            _ => true,
        });
        let d = &mut self.deletes;
        for op in &ops {
            match op {
                ModelOp::DeleteCell {
                    table,
                    row,
                    family,
                    qualifier,
                    ts,
                } => {
                    let col = (
                        table.clone(),
                        row.clone(),
                        family.clone(),
                        qualifier.clone(),
                    );
                    d.cells.insert((col, *ts));
                }
                ModelOp::DeleteColumn {
                    table,
                    row,
                    family,
                    qualifier,
                } => {
                    let key = (
                        table.clone(),
                        row.clone(),
                        family.clone(),
                        qualifier.clone(),
                    );
                    let e = d.columns.entry(key).or_insert(now);
                    *e = (*e).max(now);
                }
                ModelOp::DeleteFamily { table, row, family } => {
                    let e = d
                        .families
                        .entry((table.clone(), row.clone(), family.clone()))
                        .or_insert(now);
                    *e = (*e).max(now);
                }
                ModelOp::DeleteRow { table, row } => {
                    for f in families() {
                        let e = d
                            .families
                            .entry((table.clone(), row.clone(), f.name))
                            .or_insert(now);
                        *e = (*e).max(now);
                    }
                }
                _ => {}
            }
        }
        ops
    }

    fn pick_snapshot(&self, rng: &mut Rng) -> Option<(Snapshot, u64)> {
        if !self.snaps.is_empty() && rng.chance(300_000) {
            Some(self.snaps[rng.below(self.snaps.len() as u64) as usize].clone())
        } else {
            None
        }
    }

    /// Every row of every table with every version, through the public API, as of now.
    fn dump(&self) -> pigeonhole::Result<Rows> {
        let mut out = Vec::new();
        for (i, t) in TABLES.iter().enumerate() {
            let mut it = self
                .table(i)
                .scan_bounds(Bound::Unbounded, Bound::Unbounded)
                .versions(0)
                .iter()?;
            while let Some(row) = it.next_ref()? {
                out.push(([t.as_bytes(), b"/", row.key()].concat(), ref_cells(&row)));
            }
        }
        Ok(out)
    }

    fn model_dump(model: &Model, now: u64) -> Rows {
        let mut out = Vec::new();
        for t in TABLES {
            for (row, _) in model.scan(
                t,
                Bound::Unbounded,
                Bound::Unbounded,
                &[],
                model.snapshot(),
                now,
            ) {
                let cells = model.read_row(t, &row, &[], 0, model.snapshot(), now);
                out.push(([t.as_bytes(), b"/", &row].concat(), cells));
            }
        }
        out
    }

    /// The logged commits as the sim oracle's commits: one shard, so each is a single record
    /// in stream 0, in commit order. Every commit has a WAL record: a `Durability::None`
    /// commit's record sits in the stream's buffer until the next stronger commit's write, a
    /// flush or a clean close carries it (decision #50). An unacknowledged commit counts as
    /// `Durability::None` (D42).
    fn stream_commits(&self) -> Vec<StreamCommit> {
        self.log
            .iter()
            .map(|c| StreamCommit {
                ops: c.ops.clone(),
                commit_ts: c.ts,
                durability: if c.acked {
                    c.durability
                } else {
                    Durability::None
                },
                streams: CommitStreams::Single(0),
            })
            .collect()
    }

    /// Reopens and finds which commits survived: after a crash, the longest prefix of the
    /// stream whose recovered commits (`recovered_from_records`) keep every commit the
    /// crash's floor promised (`check_acknowledged_survive`) and whose model
    /// (`Model::from_commits`) matches the store. Without a crash, every commit.
    fn recover(&mut self, kind: Option<CrashKind>) -> Result<(), String> {
        self.snaps.clear();
        self.open()?;
        let commits = self.stream_commits();
        let hi = commits.len();
        let stream = [(0..hi).map(StreamRecord::Single).collect::<Vec<_>>()];
        let now = self.now();
        let got = self.dump().map_err(|e| format!("dump after reopen: {e}"))?;
        let mut shortest = None;
        for n in (0..=hi).rev() {
            let survivors = recovered_from_records(&stream, &[n]);
            let allowed = match kind {
                None => n == hi,
                Some(kind) => check_acknowledged_survive(&commits, &survivors, kind).is_ok(),
            };
            if !allowed {
                // Shorter prefixes lose that commit too.
                break;
            }
            let m = Model::from_commits(create_tables, survivors.iter().map(|&i| &commits[i]));
            let want = Self::model_dump(&m, now);
            if want == got {
                self.trace.push(format!("RECOVERED {n} of {hi} commits"));
                self.log = survivors.iter().map(|&i| self.log[i].clone()).collect();
                for c in &mut self.log {
                    c.acked = true;
                }
                self.model = m;
                return Ok(());
            }
            shortest = Some((n, want));
        }
        let (n, want) = shortest.unwrap_or_default();
        Err(format!(
            "recovered state matches no allowed prefix (from {hi} commits down to {n}); vs \
             the shortest: {}",
            first_diff(&got, &want).unwrap_or_default()
        ))
    }

    /// Crashes (unless a fault plan already did), reopens, and checks what survived.
    ///
    /// While a power loss is armed, any crash is that power loss: the scheduled crash fires
    /// on the n-th mutating operation, which may be the WAL write of a commit that is then
    /// acknowledged, or the engine's background spare-segment preparation, so it can have
    /// fired with no error seen. Checking against a process crash's floor would demand
    /// `Buffered` commits the power loss was allowed to drop (issue #56).
    fn crash_and_recover(&mut self, kind: CrashKind, already: bool) -> Result<(), String> {
        let kind = if self.armed { CrashKind::Power } else { kind };
        self.stats.crashes += 1;
        self.trace.push(format!(
            "CRASH {kind:?}{}",
            if already { " mid-commit" } else { "" }
        ));
        if !already {
            self.vfs.crash(kind);
        }
        self.armed = false;
        // Dropping the handles closes the engine against the crashed files; errors ignored.
        self.db = None;
        self.vfs.set_faults(self.cfg.faults.clone());
        self.recover(Some(kind))
    }

    /// A `flush` or `compact` refused with `Busy` while this run's snapshots pin the
    /// memtable arena (issue #116): no chunk is left for the fresh memtables the flush needs,
    /// and on this frozen clock the engine refuses rather than wait for a timeout. Drops the
    /// snapshots so the caller can try again.
    fn busy_with_snapshots(&mut self, result: &pigeonhole::Result<()>) -> bool {
        if !matches!(result, Err(e) if e.code() == ErrorCode::Busy) || self.snaps.is_empty() {
            return false;
        }
        self.stats.busy += 1;
        self.trace.push("BUSY: snapshots dropped".into());
        self.snaps.clear();
        true
    }

    fn reopen(&mut self) -> Result<(), String> {
        self.stats.reopens += 1;
        self.trace.push("REOPEN".into());
        self.snaps.clear();
        let (db, tables) = self.db.take().expect("open");
        drop(tables);
        db.close().map_err(|e| format!("close: {e}"))?;
        self.recover(None)
    }

    fn compare_dump(&self, when: &str) -> Result<(), String> {
        let got = self.dump().map_err(|e| format!("dump {when}: {e}"))?;
        let want = Self::model_dump(&self.model, self.now());
        match first_diff(&got, &want) {
            None => Ok(()),
            Some(d) => Err(format!("state {when}: {d}")),
        }
    }

    fn step(&mut self, op: Op, rng: &mut Rng) -> Result<(), String> {
        self.vfs.advance(1_000);
        let now = self.now();
        match op {
            Op::Commit(ops, durability) => {
                let ops = self.prepare(ops);
                if ops.is_empty() {
                    return Ok(());
                }
                let durability = self.cfg.durability.unwrap_or(durability);
                if !self.armed && rng.chance(self.cfg.mid_commit_crash_ppm) {
                    self.armed = true;
                    let mut plan = self.cfg.faults.clone();
                    plan.crash_after_ops = Some(self.vfs.mutating_ops() + 1 + rng.below(6));
                    self.vfs.set_faults(plan);
                }
                let armed = self.armed;
                self.trace.push(format!(
                    "commit {durability:?} ts={now} [{}]{}",
                    ops.iter().map(show).collect::<Vec<_>>().join("; "),
                    if armed { " (crash armed)" } else { "" }
                ));
                let mut result = self.commit(&ops, durability);
                if let Err(e) = &result
                    && e.code() == ErrorCode::Busy
                    && armed
                    && self.fired()
                {
                    // The armed power loss fired (on a flush the wait for room started, say)
                    // and the commit was refused with `Busy` before it was logged: that
                    // refusal is the power loss's (D127). Refused, it never landed.
                    let e = e.clone();
                    return self.recover_fired(&e);
                }
                if matches!(&result, Err(e) if e.code() == ErrorCode::Busy)
                    && !self.snaps.is_empty()
                {
                    // The memtable arena is full of memtables this run's snapshots hold and
                    // nothing is left to flush: on this frozen clock the engine refuses the
                    // commit rather than wait for a timeout that never comes (issue #70). A
                    // refused commit must not be applied: the store still matches the model
                    // without it. Then drop the snapshots and try again.
                    self.stats.busy += 1;
                    self.trace.push("BUSY: snapshots dropped".into());
                    self.compare_dump("after a commit refused with Busy")?;
                    self.snaps.clear();
                    result = self.commit(&ops, durability);
                }
                match result {
                    Ok(()) => {
                        self.stats.commits += 1;
                        self.model
                            .try_commit(&ops, now, durability)
                            .map_err(|e| format!("model refused an acknowledged commit: {e}"))?;
                        self.log.push(Logged {
                            ops,
                            ts: now,
                            durability,
                            acked: true,
                        });
                    }
                    Err(e) if armed && self.fired() => {
                        // The power loss hit the commit: unacknowledged, it may have landed.
                        self.stats.mid_commit_crashes += 1;
                        self.log.push(Logged {
                            ops,
                            ts: now,
                            durability,
                            acked: false,
                        });
                        return self.recover_fired(&e);
                    }
                    Err(e) => return Err(format!("commit failed: {e} ({:?})", e.code())),
                }
            }
            Op::Get {
                row,
                family,
                qualifier,
            } => {
                self.stats.reads += 1;
                let ti = table_index(&row);
                let snap = self.pick_snapshot(rng);
                let t = self.table(ti);
                let got = match &snap {
                    Some((s, _)) => t.get_at(s, &row, &family, &qualifier),
                    None => t.get(&row, &family, &qualifier),
                };
                let ms = snap.as_ref().map_or(self.model.snapshot(), |s| s.1);
                let want = self
                    .model
                    .try_get(TABLES[ti], &row, &family, &qualifier, ms, now);
                let got = got.map(|c| {
                    c.map(|c| ModelCell {
                        family: family.clone(),
                        qualifier: qualifier.clone(),
                        ts: c.timestamp(),
                        value: c.value().to_vec(),
                    })
                });
                match (got, want) {
                    (Ok(g), Ok(w)) if g == w => {}
                    (Err(e), Err(pigeonhole_sim::ModelError::MergeFailed(_)))
                        if e.code() == ErrorCode::MergeFailed => {}
                    (Err(e), _) if self.armed && self.fired() => {
                        return self.recover_fired(&e);
                    }
                    (g, w) => {
                        return Err(format!(
                            "get {}/{family}:{} (model seqno {ms}): store {g:?} vs model {w:?}",
                            text(&row),
                            text(&qualifier)
                        ));
                    }
                }
                // A row read of the same row, all versions, through the owned path.
                let row_read = {
                    let mut r = t.row(&row).versions(0);
                    if let Some((s, _)) = &snap {
                        r = r.snapshot(s);
                    }
                    r.read()
                        .map(|r| r.map(|r| owned_cells(&r.to_owned())).unwrap_or_default())
                };
                let want = self.model.try_read_row(TABLES[ti], &row, &[], 0, ms, now);
                match (row_read, want) {
                    (Ok(g), Ok(w)) if g == w => {}
                    (Err(e), Err(pigeonhole_sim::ModelError::MergeFailed(_)))
                        if e.code() == ErrorCode::MergeFailed => {}
                    (Err(e), _) if self.armed && self.fired() => {
                        return self.recover_fired(&e);
                    }
                    (g, w) => {
                        return Err(format!(
                            "row read {} (model seqno {ms}): store {g:?} vs model {w:?}",
                            text(&row)
                        ));
                    }
                }
            }
            Op::Scan { start, end } => {
                self.stats.reads += 1;
                let snap = self.pick_snapshot(rng);
                let ms = snap.as_ref().map_or(self.model.snapshot(), |s| s.1);
                let lending = rng.chance(500_000);
                for (i, name) in TABLES.iter().enumerate() {
                    let mut scan = self
                        .table(i)
                        .scan_bounds(Bound::Included(&start), Bound::Excluded(&end));
                    if let Some((s, _)) = &snap {
                        scan = scan.snapshot(s);
                    }
                    let got: pigeonhole::Result<Rows> = scan.iter().and_then(|mut it| {
                        let mut rows = Vec::new();
                        if lending {
                            while let Some(r) = it.next_ref()? {
                                rows.push((r.key().to_vec(), ref_cells(&r)));
                            }
                        } else {
                            for r in it {
                                let r = r?;
                                rows.push((r.key().to_vec(), owned_cells(&r)));
                            }
                        }
                        Ok(rows)
                    });
                    let want = self.model.try_scan(
                        name,
                        Bound::Included(&start),
                        Bound::Excluded(&end),
                        &[],
                        ms,
                        now,
                    );
                    match (got, want) {
                        (Ok(g), Ok(w)) => {
                            if let Some(d) = first_diff(&g, &w) {
                                return Err(format!("scan of {name} (model seqno {ms}): {d}"));
                            }
                        }
                        (Err(e), _) if self.armed && self.fired() => {
                            return self.recover_fired(&e);
                        }
                        (Err(e), Err(pigeonhole_sim::ModelError::MergeFailed(_)))
                            if e.code() == ErrorCode::MergeFailed => {}
                        (g, w) => {
                            return Err(format!(
                                "scan of {name}: store {:?} vs model {:?}",
                                g.map(|_| ()),
                                w.map(|_| ())
                            ));
                        }
                    }
                }
            }
            Op::Snapshot => {
                let s = match self.db().snapshot() {
                    Ok(s) => s,
                    Err(e) if self.armed && self.fired() => return self.recover_fired(&e),
                    Err(e) => return Err(format!("snapshot: {e}")),
                };
                self.snaps.push((s, self.model.snapshot()));
                if self.snaps.len() > 6 {
                    self.snaps.remove(0);
                }
            }
        }
        if self.cfg.flush_ppm > 0 && rng.chance(self.cfg.flush_ppm) {
            self.stats.flushes += 1;
            self.trace.push("FLUSH".into());
            let mut result = self.db().flush();
            if self.busy_with_snapshots(&result) {
                result = self.db().flush();
            }
            match result {
                Ok(()) => {}
                Err(e) if self.armed && self.fired() => return self.recover_fired(&e),
                Err(e) => return Err(format!("flush failed: {e} ({:?})", e.code())),
            }
        }
        if self.cfg.compact_ppm > 0 && rng.chance(self.cfg.compact_ppm) {
            self.stats.compactions += 1;
            self.trace.push("COMPACT".into());
            let mut result = self.db().compact();
            if self.busy_with_snapshots(&result) {
                result = self.db().compact();
            }
            match result {
                Ok(()) => {}
                Err(e) if self.armed && self.fired() => return self.recover_fired(&e),
                Err(e) => return Err(format!("compact failed: {e} ({:?})", e.code())),
            }
        }
        if self.armed && rng.chance(self.cfg.reopen_ppm.max(self.cfg.crash_ppm)) {
            // The scheduled power loss, whether or not it has fired yet.
            self.crash_and_recover(CrashKind::Power, false)?;
        } else if rng.chance(self.cfg.crash_ppm) {
            // A scheduled power loss may already have hit a background flush or compaction
            // write: while one is armed, a crash counts as power loss.
            let kind = if self.armed || rng.chance(500_000) {
                CrashKind::Power
            } else {
                CrashKind::Process
            };
            self.crash_and_recover(kind, false)?;
        } else if !self.armed && rng.chance(self.cfg.reopen_ppm) {
            self.reopen()?;
        }
        Ok(())
    }
}

/// Runs `seed` under `cfg`, keeping `progress` at the step under way (for the watchdog in
/// [`check`]).
fn run(seed: u64, cfg: &Config, progress: &Mutex<String>) -> Result<Stats, String> {
    let at = |what: String| *progress.lock().unwrap_or_else(PoisonError::into_inner) = what;
    at("open".into());
    let mut run = Run::new(seed, cfg.clone())?;
    let mut rng = Rng::new(seed ^ 0xa11ce);
    let ops: Vec<Op> = Workload::new(seed, "t", cfg.spec.clone())
        .take(cfg.ops)
        .collect();
    let mut result = Ok(());
    for (i, op) in ops.into_iter().enumerate() {
        at(format!("op {i}: {op:?}"));
        if let Err(e) = run.step(op, &mut rng) {
            result = Err(format!("op {i}: {e}"));
            break;
        }
    }
    if result.is_ok() && run.armed {
        at("final crash and recovery".into());
        result = run.crash_and_recover(CrashKind::Power, false);
    }
    if result.is_ok() {
        at("final dump".into());
        result = run.compare_dump("at the end");
    }
    at("final close".into());
    match result {
        Ok(()) => {
            if let Ok(f) = run
                .vfs
                .open(Path::new(DB), OpenOptions::read_write_create())
            {
                run.stats.file_len = f.len().unwrap_or(0);
            }
            if let Some((db, tables)) = run.db.take() {
                drop(tables);
                db.close().map_err(|e| format!("final close: {e}"))?;
            }
            Ok(run.stats)
        }
        Err(e) => {
            let tail: Vec<&String> = run.trace.iter().rev().take(40).rev().collect();
            let trace = tail
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join("\n  ");
            Err(format!("seed {seed}: {e}\nlast operations:\n  {trace}"))
        }
    }
}

fn seeds() -> Vec<u64> {
    let var = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    };
    let base = var("PIGEONHOLE_SEED", 1);
    (base..base + var("PIGEONHOLE_SEEDS", 1)).collect()
}

/// How long one seed may run before [`check`] declares it hung: `PIGEONHOLE_SEED_TIMEOUT`
/// seconds, 120 by default (a seed normally takes well under a second).
fn seed_timeout() -> Duration {
    let secs = std::env::var("PIGEONHOLE_SEED_TIMEOUT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    Duration::from_secs(secs)
}

/// Runs every seed of `cfg`. Each runs on its own thread under a watchdog: a seed that
/// outlives [`seed_timeout`] is reported with its test, configuration and the step it was
/// on, and the process aborts, so a hang fails fast and names itself instead of eating the
/// job's timeout (#244, #209).
fn check(cfg: &Config) {
    let test = std::thread::current().name().unwrap_or("?").to_owned();
    let limit = seed_timeout();
    for seed in seeds() {
        let started = Instant::now();
        let progress = Arc::new(Mutex::new(String::new()));
        let (tx, rx) = mpsc::channel();
        let worker = {
            let (cfg, progress) = (cfg.clone(), Arc::clone(&progress));
            std::thread::Builder::new()
                .name(format!("{test} seed {seed}"))
                .spawn(move || {
                    let _ = tx.send(run(seed, &cfg, &progress));
                })
                .expect("spawn a seed's thread")
        };
        match rx.recv_timeout(limit) {
            Ok(Ok(stats)) => eprintln!("seed {seed}: {stats:?} in {:?}", started.elapsed()),
            Ok(Err(e)) => panic!("{e}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let at = progress
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .clone();
                // Straight to stderr: the test harness's capture would lose an `eprintln!`.
                let _ = writeln!(
                    std::io::stderr(),
                    "HUNG: {test}: seed {seed} ({} shards, tablet changes {}, deferred I/O {}) \
                     did not finish in {limit:?}; it was at {at}",
                    cfg.shards,
                    cfg.tablet_changes,
                    cfg.deferred_io
                );
                // What the shards do from here on, if anything (#244).
                pigeonhole_engine::set_tracing(true);
                std::thread::sleep(Duration::from_secs(5));
                let _ = writeln!(std::io::stderr(), "HUNG: end of the trace after the hang");
                std::process::abort();
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                if let Err(panic) = worker.join() {
                    std::panic::resume_unwind(panic);
                }
                unreachable!("a seed's thread ended without a result");
            }
        }
        let _ = worker.join();
    }
}

#[test]
fn quiet_runs_match_the_model() {
    for shards in [1, 2, 4, 8] {
        check(&Config::quiet(1000, shards));
    }
}

#[test]
fn crashes_and_reopens_match_a_durable_prefix() {
    check(&Config::crashing(1000));
}

#[test]
fn quiet_runs_with_tablet_changes_off_match_the_model() {
    for shards in [1, 2, 4, 8] {
        let mut cfg = Config::quiet(1000, shards);
        cfg.tablet_changes = false;
        check(&cfg);
    }
}

#[test]
fn crashes_with_tablet_changes_off_match_a_durable_prefix() {
    let mut cfg = Config::crashing(1000);
    cfg.tablet_changes = false;
    check(&cfg);
}

#[test]
fn every_durability_level_under_crashes() {
    for level in [
        Durability::None,
        Durability::Buffered,
        Durability::GroupSync,
        Durability::Sync,
    ] {
        let mut cfg = Config::crashing(500);
        cfg.durability = Some(level);
        cfg.crash_ppm = 40_000;
        cfg.mid_commit_crash_ppm = 60_000;
        check(&cfg);
    }
}

#[test]
fn a_crash_while_a_power_loss_is_armed_is_that_power_loss() {
    // Issue #56: the scheduled power loss fired after an acknowledged `Buffered` commit's
    // WAL write (no error seen), then a random process crash was checked at the process
    // floor and demanded the dropped commit. Fire the power loss directly so the test does
    // not depend on which mutating operation the scheduled one would land on.
    let seed = seeds()[0];
    let mut cfg = Config::crashing(0);
    cfg.faults = FaultPlan::none();
    (cfg.crash_ppm, cfg.mid_commit_crash_ppm, cfg.reopen_ppm) = (0, 0, 0);
    let mut run = Run::new(seed, cfg).expect("open");
    let mut rng = Rng::new(seed);
    for op in Workload::new(seed, "t", run.cfg.spec.clone()).take(40) {
        let Op::Commit(ops, _) = op else { continue };
        run.step(Op::Commit(ops, Durability::Buffered), &mut rng)
            .expect("commit");
    }
    assert!(
        run.log.iter().any(|c| c.acked),
        "seed {seed}: nothing committed"
    );
    run.armed = true;
    run.vfs.crash(CrashKind::Power);
    let result = run.crash_and_recover(CrashKind::Process, false);
    assert!(
        run.trace.iter().any(|t| t == "CRASH Power"),
        "seed {seed}: the armed power loss was checked as a process crash"
    );
    result.unwrap_or_else(|e| panic!("seed {seed}: {e}"));
}

#[test]
fn identical_results_across_shard_counts() {
    // The same workload with no crashes ends in the same state whatever the shard count.
    let seed = seeds()[0];
    let mut dumps: BTreeMap<usize, Rows> = BTreeMap::new();
    for shards in [1, 3] {
        let mut cfg = Config::quiet(600, shards);
        cfg.reopen_ppm = 0;
        let mut run = Run::new(seed, cfg.clone()).expect("open");
        let mut rng = Rng::new(seed);
        for op in Workload::new(seed, "t", cfg.spec.clone()).take(cfg.ops) {
            run.step(op, &mut rng).expect("step");
        }
        dumps.insert(shards, run.dump().expect("dump"));
    }
    assert_eq!(dumps[&1], dumps[&3], "seed {seed}");
}

/// A one-shard run whose commits a clean close and reopen persisted to SSTs, with the block
/// cache off so reads reach the files and the shards idle (no background I/O pending).
fn run_with_flushed_commits(seed: u64) -> (Run, Rng) {
    let mut cfg = Config::crashing(0);
    cfg.faults = FaultPlan::none();
    (cfg.crash_ppm, cfg.mid_commit_crash_ppm, cfg.reopen_ppm) = (0, 0, 0);
    cfg.memtable_budget = 256 << 10;
    cfg.block_cache = 0;
    // The background I/O these runs arm a power loss for must be a flush alone: with the
    // fast balancer a size split started beside it, and its manifest commit could take the
    // crash before the commit under test was acknowledged (seed 51 on CI, PR #168).
    cfg.fast_balancer = false;
    let mut run = Run::new(seed, cfg).expect("open");
    let mut rng = Rng::new(seed);
    for op in Workload::new(seed, "t", run.cfg.spec.clone()).take(60) {
        let Op::Commit(ops, _) = op else { continue };
        run.step(Op::Commit(ops, Durability::Buffered), &mut rng)
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
    run.reopen().unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    (run, rng)
}

fn scan_all() -> Op {
    Op::Scan {
        start: Vec::new(),
        end: vec![0xff],
    }
}

#[test]
fn a_read_after_a_background_fired_crash_is_that_crash() {
    // Issue #62: a power loss is armed on the next mutating operation and a
    // `Durability::None` commit (which writes nothing itself) fills the memtable, so a
    // shard's background flush fires the crash with no client call in progress. The next
    // read finds the SSTs' handles dead: the error is the armed power loss, and the run
    // recovers from it.
    let seed = seeds()[0];
    let (mut run, mut rng) = run_with_flushed_commits(seed);
    // `step` rolls a client `flush()` (and `compact()`) after a commit; at some seeds that
    // call, not the background flush, takes the armed crash on the commit's own step
    // (issue #101). Leave the flushing to the shard.
    (run.cfg.flush_ppm, run.cfg.compact_ppm) = (0, 0);
    let mut plan = run.cfg.faults.clone();
    plan.crash_after_ops = Some(run.vfs.mutating_ops() + 1);
    run.vfs.set_faults(plan);
    run.armed = true;
    let big = ModelOp::Put {
        table: "t".into(),
        row: b"big".to_vec(),
        family: "f".into(),
        qualifier: b"q".to_vec(),
        ts: None,
        value: vec![7; 100 << 10],
    };
    run.step(Op::Commit(vec![big], Durability::None), &mut rng)
        .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    assert!(
        run.armed && run.log.last().is_some_and(|c| c.acked),
        "seed {seed}: the crash fired on the commit itself: {:?}",
        run.trace.iter().rev().take(4).collect::<Vec<_>>()
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !run.fired() {
        assert!(
            std::time::Instant::now() < deadline,
            "seed {seed}: the background flush never fired the armed crash"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    run.step(scan_all(), &mut rng)
        .unwrap_or_else(|e| panic!("seed {seed}: {e}\n{}", run.trace.join("\n")));
    assert!(
        !run.armed && run.trace.iter().any(|t| t == "CRASH Power mid-commit"),
        "seed {seed}: the read did not recover from the armed crash: {:?}",
        run.trace.iter().rev().take(5).collect::<Vec<_>>()
    );
}

#[test]
fn an_io_error_while_an_unfired_crash_is_armed_is_not_that_crash() {
    // Issue #62's other half: armed alone is never sufficient. A power loss is armed far in
    // the future (it does not fire) and every read fails with an injected I/O error: the
    // scan's error is a real failure, not the crash, and the run must report it rather
    // than recover as if the power had gone.
    let seed = seeds()[0];
    let (mut run, mut rng) = run_with_flushed_commits(seed);
    let mut plan = run.cfg.faults.clone();
    plan.crash_after_ops = Some(run.vfs.mutating_ops() + 1_000_000);
    plan.io_error_ppm = 1_000_000;
    run.vfs.set_faults(plan);
    run.armed = true;
    let crashes = run.stats.crashes;
    let result = run.step(scan_all(), &mut rng);
    run.vfs.set_faults(FaultPlan::none());
    assert!(!run.fired(), "seed {seed}: the armed crash fired");
    assert_eq!(
        run.stats.crashes, crashes,
        "seed {seed}: an I/O error with the crash unfired was taken for the crash"
    );
    let e = result.expect_err("the injected read error was not reported");
    assert!(e.contains("scan of"), "seed {seed}: {e}");
}
