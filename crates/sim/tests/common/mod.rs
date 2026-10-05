//! The crash/replay checker: drives a [`toy::Toy`] store and the reference [`Model`] with the
//! same seeded workload under `SimVfs` faults, crashes the machine at random points (and in
//! the middle of commits), and compares every observation against the model.
#![allow(dead_code)]

pub mod toy;

use std::fmt;

use pigeonhole_format::{Durability, Seqno};
use pigeonhole_io::ErrorKind;
use pigeonhole_io::Vfs;
use pigeonhole_io::sim::{CrashKind, FaultPlan};
use pigeonhole_sim::{Model, ModelCell, ModelFamily, ModelOp, Op, Sim, Workload, WorkloadSpec};

use toy::{TABLE, Toy, Variant};

#[derive(Clone)]
pub struct Config {
    pub ops: usize,
    pub faults: FaultPlan,
    /// Per-op probability (ppm) of crashing between operations.
    pub crash_ppm: u32,
    /// Per-commit probability (ppm) of crashing in the middle of the commit.
    pub mid_commit_crash_ppm: u32,
    pub spec: WorkloadSpec,
}

impl Config {
    pub fn standard(ops: usize) -> Self {
        let mut faults = FaultPlan::none();
        faults.torn_writes = true;
        faults.reorder_unsynced = true;
        let mut spec = WorkloadSpec::default();
        spec.rows = 20;
        spec.qualifiers = 3;
        spec.families = vec!["f".into(), "g".into(), "ttl".into(), "counter".into()];
        spec.max_value_len = 120;
        Config {
            ops,
            faults,
            crash_ppm: 20_000,
            mid_commit_crash_ppm: 30_000,
            spec,
        }
    }
}

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

#[derive(Debug, Default, Clone, Copy)]
pub struct Stats {
    pub ops: usize,
    pub commits: usize,
    pub crashes: usize,
    pub mid_commit_crashes: usize,
}

/// A checker failure with everything needed to replay it.
pub struct Failure {
    pub seed: u64,
    pub variant: Variant,
    /// Index of the operation at which the divergence was observed. The run is
    /// deterministic, so the ops up to and including it are the shortest prefix that fails.
    pub op_index: usize,
    pub message: String,
    pub trace: Vec<String>,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "checker failure: seed={} variant={:?} at op #{}: {}",
            self.seed, self.variant, self.op_index, self.message
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

fn show(op: &ModelOp) -> String {
    let s = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
    match op {
        ModelOp::Put {
            row,
            family,
            qualifier,
            ts,
            value,
            ..
        } => {
            format!(
                "put {}/{family}:{} ts={ts:?} {}B",
                s(row),
                s(qualifier),
                value.len()
            )
        }
        ModelOp::Incr {
            row,
            family,
            qualifier,
            delta,
            ..
        } => {
            format!("incr {}/{family}:{} {delta:+}", s(row), s(qualifier))
        }
        ModelOp::DeleteCell {
            row,
            family,
            qualifier,
            ts,
            ..
        } => {
            format!("del-cell {}/{family}:{} ts={ts}", s(row), s(qualifier))
        }
        ModelOp::DeleteColumn {
            row,
            family,
            qualifier,
            ..
        } => {
            format!("del-col {}/{family}:{}", s(row), s(qualifier))
        }
        ModelOp::DeleteFamily { row, family, .. } => format!("del-fam {}/{family}", s(row)),
        ModelOp::DeleteRow { row, .. } => format!("del-row {}", s(row)),
    }
}

fn first_diff(a: &[(Vec<u8>, Vec<ModelCell>)], b: &[(Vec<u8>, Vec<ModelCell>)]) -> Option<String> {
    for i in 0..a.len().max(b.len()) {
        if a.get(i) != b.get(i) {
            return Some(format!(
                "first difference at row index {i}: store {:?} vs model {:?}",
                a.get(i),
                b.get(i)
            ));
        }
    }
    None
}

struct Run<'a> {
    seed: u64,
    variant: Variant,
    cfg: &'a Config,
    sim: Sim,
    store: Toy,
    model: Model,
    base: u64,
    snaps: Vec<Seqno>,
    trace: Vec<String>,
    op_index: usize,
    stats: Stats,
}

impl Run<'_> {
    fn fail(&mut self, message: String) -> Failure {
        Failure {
            seed: self.seed,
            variant: self.variant,
            op_index: self.op_index,
            message,
            trace: std::mem::take(&mut self.trace),
        }
    }

    fn now(&self) -> u64 {
        self.sim.vfs().now_micros()
    }

    /// Compares the store's full state at `snapshot` with the model's.
    fn compare_dump(&self, snapshot: Seqno) -> Result<(), String> {
        let now = self.now();
        let store = self.store.dump(snapshot, now);
        let model: Vec<_> = self
            .model
            .scan(
                TABLE,
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
                &[],
                snapshot,
                now,
            )
            .into_iter()
            .map(|(row, _)| {
                let cells = self.model.read_row(TABLE, &row, &[], 0, snapshot, now);
                (row, cells)
            })
            .collect();
        match first_diff(&store, &model) {
            None => Ok(()),
            Some(d) => Err(format!("state at snapshot {snapshot} differs: {d}")),
        }
    }

    /// Crashes (or notes that the plan already did), recovers, and checks the recovered
    /// state against what the model allows.
    fn crash_and_recover(&mut self, kind: CrashKind, already_crashed: bool) -> Result<(), String> {
        self.stats.crashes += 1;
        let window = self.model.crash_window(kind);
        if !already_crashed {
            self.sim.crash(kind);
        }
        self.trace.push(format!(
            "CRASH {kind:?}{} (window must={} may={})",
            if already_crashed { " mid-commit" } else { "" },
            window.must_survive,
            window.may_survive
        ));
        self.sim.vfs().set_faults(self.cfg.faults.clone());
        self.store = Toy::open(&self.sim.vfs(), self.variant, &families())
            .map_err(|e| format!("recovery failed: {e}"))?;
        let k = self.store.last_seqno();
        self.trace.push(format!("RECOVERED at seqno {k}"));
        if k < window.must_survive {
            return Err(format!(
                "lost an acknowledged commit: recovered seqno {k} < must-survive {}",
                window.must_survive
            ));
        }
        if k > window.may_survive {
            return Err(format!(
                "recovered seqno {k} beyond anything committed ({})",
                window.may_survive
            ));
        }
        self.compare_dump(k)?;
        self.model.recover(kind, k);
        self.snaps.retain(|s| *s <= k);
        // Older snapshots of the recovered store must agree too.
        if k > 0 {
            let s = 1 + self.sim.rng().below(k);
            self.compare_dump(s)?;
        }
        Ok(())
    }

    fn step(&mut self, op: Op) -> Result<(), String> {
        let vfs = self.sim.vfs();
        vfs.advance(1_000);
        let now = self.now();
        match op {
            Op::Commit(mut ops, durability) => {
                for o in &mut ops {
                    match o {
                        ModelOp::Put { ts: Some(t), .. } | ModelOp::DeleteCell { ts: t, .. } => {
                            *t += self.base;
                        }
                        _ => {}
                    }
                }
                self.stats.commits += 1;
                let armed = self.sim.rng().chance(self.cfg.mid_commit_crash_ppm);
                if armed {
                    let mut plan = self.cfg.faults.clone();
                    plan.crash_after_ops = Some(vfs.mutating_ops() + 1 + self.sim.rng().below(4));
                    vfs.set_faults(plan);
                }
                self.trace.push(format!(
                    "commit {durability:?} ts={now} [{}]{}",
                    ops.iter().map(show).collect::<Vec<_>>().join("; "),
                    if armed { " (crash armed)" } else { "" }
                ));
                let result = self.store.commit(&ops, now, durability);
                let dead = armed && !self.store.alive();
                match result {
                    Ok(seq) => {
                        let m = self.model.commit(&ops, now, durability);
                        if seq != m {
                            return Err(format!("store seqno {seq} != model seqno {m}"));
                        }
                    }
                    Err(e) if e.kind == ErrorKind::Crashed => {
                        // In flight: unacknowledged, but it may have survived.
                        self.model.commit(&ops, now, Durability::None);
                    }
                    Err(e) => return Err(format!("unexpected I/O error: {e}")),
                }
                if dead {
                    self.stats.mid_commit_crashes += 1;
                    return self.crash_and_recover(CrashKind::Power, true);
                }
                if armed {
                    vfs.set_faults(self.cfg.faults.clone());
                }
            }
            Op::Get {
                row,
                family,
                qualifier,
            } => {
                let snap = self.pick_snapshot();
                let got = self.store.get(&row, &family, &qualifier, snap, now);
                let want = self.model.get(TABLE, &row, &family, &qualifier, snap, now);
                if got != want {
                    return Err(format!(
                        "get {:?}/{family}:{:?} at snapshot {snap}: store {got:?} vs model {want:?}",
                        String::from_utf8_lossy(&row),
                        String::from_utf8_lossy(&qualifier)
                    ));
                }
            }
            Op::Scan { start, end } => {
                let snap = self.pick_snapshot();
                let got = self.store.scan(&start, &end, snap, now);
                let want = self.model.scan(
                    TABLE,
                    std::ops::Bound::Included(&start[..]),
                    std::ops::Bound::Excluded(&end[..]),
                    &[],
                    snap,
                    now,
                );
                if let Some(d) = first_diff(&got, &want) {
                    return Err(format!("scan at snapshot {snap}: {d}"));
                }
                if self.sim.rng().chance(100_000) {
                    self.compare_dump(snap)?;
                }
            }
            Op::Snapshot => {
                self.snaps.push(self.model.snapshot());
                if self.snaps.len() > 8 {
                    self.snaps.remove(0);
                }
            }
        }
        if self.sim.rng().chance(self.cfg.crash_ppm) {
            let kind = if self.sim.rng().chance(500_000) {
                CrashKind::Power
            } else {
                CrashKind::Process
            };
            self.crash_and_recover(kind, false)?;
        }
        Ok(())
    }

    fn pick_snapshot(&mut self) -> Seqno {
        if !self.snaps.is_empty() && self.sim.rng().chance(300_000) {
            let i = self.sim.rng().below(self.snaps.len() as u64) as usize;
            self.snaps[i]
        } else {
            self.model.snapshot()
        }
    }
}

/// Runs one seeded crash/replay check of `variant`.
pub fn run(seed: u64, variant: Variant, cfg: &Config) -> Result<Stats, Failure> {
    let sim = Sim::with_faults(seed, cfg.faults.clone());
    let vfs = sim.vfs();
    let mut model = Model::new();
    model.create_table(TABLE, families());
    let store = Toy::open(&vfs, variant, &families()).expect("open on a fresh filesystem");
    let base = vfs.now_micros();
    let mut run = Run {
        seed,
        variant,
        cfg,
        sim,
        store,
        model,
        base,
        snaps: Vec::new(),
        trace: Vec::new(),
        op_index: 0,
        stats: Stats::default(),
    };
    let workload = Workload::new(seed ^ 0x5eed, TABLE, cfg.spec.clone());
    for (i, op) in workload.take(cfg.ops).enumerate() {
        run.op_index = i;
        run.stats.ops += 1;
        if let Err(message) = run.step(op) {
            return Err(run.fail(message));
        }
    }
    // Finish with one more crash so the tail of every run is checked.
    run.op_index = cfg.ops;
    let kind = if run.sim.rng().chance(500_000) {
        CrashKind::Power
    } else {
        CrashKind::Process
    };
    if let Err(message) = run.crash_and_recover(kind, false) {
        return Err(run.fail(message));
    }
    Ok(run.stats)
}
