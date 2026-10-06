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
//! the middle of a commit (`FaultPlan::crash_after_ops`). Crash runs use one shard, so the
//! survivors are a prefix of the commit log: recovery must reproduce exactly the model of
//! some prefix that keeps every commit the durability levels promised
//! (`Model::crash_window`). Multi-shard runs reopen cleanly instead.
//!
//! Seeds: `PIGEONHOLE_SEED` (first seed, default 1) and `PIGEONHOLE_SEEDS` (count, default
//! 3). A failure prints its seed and the operation trace.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use pigeonhole::{Durability, ErrorCode, Family, Options, Pigeonhole, Snapshot, Table};
use pigeonhole_io::Vfs;
use pigeonhole_io::sim::{CrashKind, FaultPlan, SimVfs};
use pigeonhole_sim::{Model, ModelCell, ModelFamily, ModelOp, Op, Rng, Workload, WorkloadSpec};

const DB: &str = "/db/model.phdb";

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

fn public_family(f: &ModelFamily) -> Family {
    let family = Family::default()
        .max_versions(f.max_versions)
        .ttl(Duration::from_micros(f.ttl_micros));
    if f.i64_add {
        family
    } else {
        family.merge_operator("")
    }
}

fn new_model() -> Model {
    let mut m = Model::new();
    for t in TABLES {
        m.create_table(t, families());
    }
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
}

struct Run {
    cfg: Config,
    vfs: Arc<SimVfs>,
    db: Option<(Pigeonhole, Vec<Table>)>,
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
}

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
        let base = vfs.now_micros();
        let mut run = Run {
            cfg,
            vfs,
            db: None,
            model: new_model(),
            log: Vec::new(),
            snaps: Vec::new(),
            base,
            armed: false,
            trace: Vec::new(),
            stats: Stats::default(),
        };
        run.open()?;
        Ok(run)
    }

    fn open(&mut self) -> Result<(), String> {
        let options = Options::default()
            .vfs(Arc::clone(&self.vfs) as _)
            .shards(self.cfg.shards)
            .memtable_budget(4 << 20)
            .wal_segment_size(256 << 10)
            .block_cache(1 << 20);
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
        Ok(())
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

    /// Prepares a generated commit: tables by row, explicit timestamps after the base, and
    /// no counter base at an explicit timestamp (there is no typed `put_at`).
    fn prepare(&self, ops: Vec<ModelOp>) -> Vec<ModelOp> {
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

    /// Rebuilds the model from the first `n` logged commits. Every commit has a WAL record:
    /// a `Durability::None` commit's record sits in the stream's buffer until the next
    /// stronger commit's write, a flush or a clean close carries it (decision #50), so it
    /// survives exactly when the stream's prefix through it does.
    fn rebuild(&self, n: usize) -> Result<Model, String> {
        let mut m = new_model();
        for c in self.log[..n].iter() {
            m.try_commit(&c.ops, c.ts, Durability::Sync)
                .map_err(|e| format!("model rebuild: {e}"))?;
        }
        Ok(m)
    }

    /// Reopens and finds which commits survived: a prefix of the log (one shard, one stream)
    /// that keeps every acknowledged commit at the crash's floor level or stronger. Without
    /// a crash, every commit.
    fn recover(&mut self, kind: Option<CrashKind>) -> Result<(), String> {
        self.snaps.clear();
        self.open()?;
        let floor = match kind {
            Some(CrashKind::Process) => Durability::Buffered,
            Some(CrashKind::Power) => Durability::GroupSync,
            None => Durability::None,
        };
        let hi = self.log.len();
        let lo = match kind {
            None => hi,
            Some(_) => self
                .log
                .iter()
                .rposition(|c| c.acked && c.durability >= floor)
                .map_or(0, |i| i + 1),
        };
        let now = self.now();
        let got = self.dump().map_err(|e| format!("dump after reopen: {e}"))?;
        for n in (lo..=hi).rev() {
            let m = self.rebuild(n)?;
            if Self::model_dump(&m, now) == got {
                self.trace
                    .push(format!("RECOVERED {n} of {hi} commits (must keep {lo})"));
                self.log.truncate(n);
                for c in &mut self.log {
                    c.acked = true;
                }
                self.model = m;
                return Ok(());
            }
        }
        let want = Self::model_dump(&self.rebuild(lo)?, now);
        Err(format!(
            "recovered state matches no allowed prefix in [{lo}, {hi}] of the commit log; vs \
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
                match self.commit(&ops, durability) {
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
                    Err(e) if armed && e.code() == ErrorCode::Io => {
                        // The power loss hit the commit: unacknowledged, it may have landed.
                        self.stats.mid_commit_crashes += 1;
                        self.log.push(Logged {
                            ops,
                            ts: now,
                            durability,
                            acked: false,
                        });
                        return self.crash_and_recover(CrashKind::Power, true);
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
                    (Err(e), _) if self.armed && e.code() == ErrorCode::Io => {
                        // The scheduled power loss hit a background write (a flush or a
                        // compaction) rather than a commit: the read finds the store dead.
                        return self.crash_and_recover(CrashKind::Power, true);
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
                    (Err(e), _) if self.armed && e.code() == ErrorCode::Io => {
                        return self.crash_and_recover(CrashKind::Power, true);
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
                        (Err(e), _) if self.armed && e.code() == ErrorCode::Io => {
                            return self.crash_and_recover(CrashKind::Power, true);
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
                let s = self.db().snapshot().map_err(|e| format!("snapshot: {e}"))?;
                self.snaps.push((s, self.model.snapshot()));
                if self.snaps.len() > 6 {
                    self.snaps.remove(0);
                }
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

fn run(seed: u64, cfg: &Config) -> Result<Stats, String> {
    let mut run = Run::new(seed, cfg.clone())?;
    let mut rng = Rng::new(seed ^ 0xa11ce);
    let ops: Vec<Op> = Workload::new(seed, "t", cfg.spec.clone())
        .take(cfg.ops)
        .collect();
    let mut result = Ok(());
    for (i, op) in ops.into_iter().enumerate() {
        if let Err(e) = run.step(op, &mut rng) {
            result = Err(format!("op {i}: {e}"));
            break;
        }
    }
    if result.is_ok() && run.armed {
        result = run.crash_and_recover(CrashKind::Power, false);
    }
    if result.is_ok() {
        result = run.compare_dump("at the end");
    }
    match result {
        Ok(()) => {
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
    (base..base + var("PIGEONHOLE_SEEDS", 3)).collect()
}

fn check(cfg: &Config) {
    for seed in seeds() {
        match run(seed, cfg) {
            Ok(stats) => eprintln!("seed {seed}: {stats:?}"),
            Err(e) => panic!("{e}"),
        }
    }
}

#[test]
#[ignore = "engine Milestone B compacts: bottommost compactions purge per decision D74, which \
            this model does not replay (the public API exposes no compaction records); \
            issue #45"]
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
