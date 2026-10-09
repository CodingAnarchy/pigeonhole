//! The crash/replay checker: drives a [`toy::Toy`] store and the reference [`Model`] with
//! seeded workloads under `SimVfs` faults, crashes the machine at random points (and in the
//! middle of commits), and compares every observation against the model.
//!
//! **Scheduling.** Client tasks run on the [`Sim`] scheduler: each task owns a workload and
//! executes one operation per step against a shared world, so with several tasks the
//! scheduler interleaves their operations (and their sleeps drive the simulated clock). The
//! store itself is single-threaded, like a shard, so a step is atomic; what the scheduler
//! varies is the order in which the clients' operations reach it. A crash replaces the store
//! with the recovered one and the clients carry on, as they would after an application
//! restart.
//!
//! **Crashes in the middle of a commit** use `FaultPlan::crash_after_ops`, which the io
//! simulator always treats as a power loss ([`CrashKind::Power`]); process crashes are
//! injected between operations only.

pub mod toy;

use std::cell::RefCell;
use std::fmt;
use std::ops::Bound;
use std::rc::Rc;

use pigeonhole_format::{Durability, Seqno};
use pigeonhole_io::ErrorKind;
use pigeonhole_io::Vfs;
use pigeonhole_io::sim::{CrashKind, FaultPlan, SimVfs};
use pigeonhole_sim::{
    Model, ModelCell, ModelFamily, ModelOp, Op, Rng, Sim, Step, Workload, WorkloadSpec,
};

use std::sync::Arc;
use toy::{TABLE, Toy, Variant};

#[derive(Clone)]
pub struct Config {
    /// Total operations across all client tasks.
    pub ops: usize,
    /// Client tasks interleaved by the scheduler.
    pub tasks: usize,
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
            tasks: 1,
            faults,
            crash_ppm: 20_000,
            mid_commit_crash_ppm: 30_000,
            spec,
        }
    }
}

pub fn families() -> Vec<ModelFamily> {
    let f = |name: &str, max_versions, ttl_micros, i64_add| {
        ModelFamily::new(name)
            .max_versions(max_versions)
            .ttl_micros(ttl_micros)
            .i64_add(i64_add)
            .counter(false)
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

/// What kind of divergence the checker saw, so tests can pin the expected class per bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// Recovery returned fewer commits than a crash must preserve.
    LostAckedCommit,
    /// Recovery returned commits that were never made.
    RecoveredFromTheFuture,
    /// The recovered state is not the model's state at the recovered seqno.
    RecoveredStateMismatch,
    /// A live read or scan disagreed with the model.
    LiveReadMismatch,
    /// The store and model disagree about a commit's seqno, or an unexpected error occurred.
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
    pub variant: Variant,
    pub class: FailureClass,
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
            "checker failure ({:?}): seed={} variant={:?} at op #{}: {}",
            self.class, self.seed, self.variant, self.op_index, self.message
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
        } => {
            format!(
                "put {}/{family}:{} ts={ts:?} {}B",
                text(row),
                text(qualifier),
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
            format!("incr {}/{family}:{} {delta:+}", text(row), text(qualifier))
        }
        ModelOp::DeleteCell {
            row,
            family,
            qualifier,
            ts,
            ..
        } => {
            format!(
                "del-cell {}/{family}:{} ts={ts}",
                text(row),
                text(qualifier)
            )
        }
        ModelOp::DeleteColumn {
            row,
            family,
            qualifier,
            ..
        } => {
            format!("del-col {}/{family}:{}", text(row), text(qualifier))
        }
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

fn show_opt(c: &Option<ModelCell>) -> String {
    c.as_ref().map_or("none".into(), show_cell)
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

fn first_diff(
    store: &[(Vec<u8>, Vec<ModelCell>)],
    model: &[(Vec<u8>, Vec<ModelCell>)],
) -> Option<String> {
    (0..store.len().max(model.len())).find_map(|i| {
        (store.get(i) != model.get(i)).then(|| {
            format!(
                "first difference at row #{i}: store {} vs model {}",
                show_row(store.get(i)),
                show_row(model.get(i))
            )
        })
    })
}

/// Everything the clients share.
struct World {
    seed: u64,
    variant: Variant,
    cfg: Config,
    vfs: Arc<SimVfs>,
    store: Toy,
    model: Model,
    base: u64,
    snaps: Vec<Seqno>,
    trace: Vec<String>,
    op_index: usize,
    stats: Stats,
    failure: Option<Failure>,
}

impl World {
    fn now(&self) -> u64 {
        self.vfs.now_micros()
    }

    /// Compares the store's full state at `snapshot` with the model's.
    fn compare_dump(&self, snapshot: Seqno, class: FailureClass) -> Result<(), Fail> {
        let now = self.now();
        let store = self.store.dump(snapshot, now);
        let model: Vec<_> = self
            .model
            .scan(
                TABLE,
                Bound::Unbounded,
                Bound::Unbounded,
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
            Some(d) => fail(class, format!("state at snapshot {snapshot} differs: {d}")),
        }
    }

    /// Crashes (or notes that the fault plan already did), recovers, and checks the recovered
    /// state against what the model allows.
    fn crash_and_recover(
        &mut self,
        kind: CrashKind,
        already_crashed: bool,
        rng: &mut Rng,
    ) -> Result<(), Fail> {
        self.stats.crashes += 1;
        let window = self.model.crash_window(kind);
        if !already_crashed {
            self.vfs.crash(kind);
        }
        self.trace.push(format!(
            "CRASH {kind:?}{} (window must={} may={})",
            if already_crashed { " mid-commit" } else { "" },
            window.must_survive,
            window.may_survive
        ));
        self.vfs.set_faults(self.cfg.faults.clone());
        self.store = match Toy::open(&self.vfs, self.variant, &families()) {
            Ok(store) => store,
            Err(e) => return fail(FailureClass::Protocol, format!("recovery failed: {e}")),
        };
        let k = self.store.last_seqno();
        self.trace.push(format!("RECOVERED at seqno {k}"));
        if k < window.must_survive {
            return fail(
                FailureClass::LostAckedCommit,
                format!(
                    "recovered seqno {k} < must-survive {}: an acknowledged commit was lost",
                    window.must_survive
                ),
            );
        }
        if k > window.may_survive {
            return fail(
                FailureClass::RecoveredFromTheFuture,
                format!(
                    "recovered seqno {k} beyond anything committed ({})",
                    window.may_survive
                ),
            );
        }
        self.compare_dump(k, FailureClass::RecoveredStateMismatch)?;
        self.model.recover(kind, k);
        self.snaps.retain(|s| *s <= k);
        // An older snapshot of the recovered store must agree too.
        if k > 0 {
            let s = 1 + rng.below(k);
            self.compare_dump(s, FailureClass::RecoveredStateMismatch)?;
        }
        Ok(())
    }

    fn pick_snapshot(&self, rng: &mut Rng) -> Seqno {
        if !self.snaps.is_empty() && rng.chance(300_000) {
            self.snaps[rng.below(self.snaps.len() as u64) as usize]
        } else {
            self.model.snapshot()
        }
    }

    fn step(&mut self, op: Op, rng: &mut Rng) -> Result<(), Fail> {
        self.vfs.advance(1_000);
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
                let armed = rng.chance(self.cfg.mid_commit_crash_ppm);
                if armed {
                    let mut plan = self.cfg.faults.clone();
                    plan.crash_after_ops = Some(self.vfs.mutating_ops() + 1 + rng.below(4));
                    self.vfs.set_faults(plan);
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
                        let m = match self.model.try_commit(&ops, now, durability) {
                            Ok(m) => m,
                            Err(e) => return fail(FailureClass::Protocol, format!("model: {e}")),
                        };
                        if seq != m {
                            return fail(
                                FailureClass::Protocol,
                                format!("store seqno {seq} != model seqno {m}"),
                            );
                        }
                    }
                    Err(e) if e.kind == ErrorKind::Crashed => {
                        // In flight: unacknowledged, but it may have survived.
                        let _ = self.model.try_commit(&ops, now, Durability::None);
                    }
                    Err(e) => {
                        return fail(FailureClass::Protocol, format!("unexpected I/O error: {e}"));
                    }
                }
                if dead {
                    self.stats.mid_commit_crashes += 1;
                    return self.crash_and_recover(CrashKind::Power, true, rng);
                }
                if armed {
                    self.vfs.set_faults(self.cfg.faults.clone());
                }
            }
            Op::Get {
                row,
                family,
                qualifier,
            } => {
                let snap = self.pick_snapshot(rng);
                self.trace.push(format!(
                    "get {}/{family}:{} @snapshot {snap}",
                    text(&row),
                    text(&qualifier)
                ));
                let got = self.store.get(&row, &family, &qualifier, snap, now);
                let want = self.model.get(TABLE, &row, &family, &qualifier, snap, now);
                if got != want {
                    return fail(
                        FailureClass::LiveReadMismatch,
                        format!(
                            "get {}/{family}:{} at snapshot {snap}: store {} vs model {}",
                            text(&row),
                            text(&qualifier),
                            show_opt(&got),
                            show_opt(&want)
                        ),
                    );
                }
            }
            Op::Scan { start, end } => {
                let snap = self.pick_snapshot(rng);
                self.trace.push(format!(
                    "scan [{}, {}) @snapshot {snap}",
                    text(&start),
                    text(&end)
                ));
                let got = self.store.scan(&start, &end, snap, now);
                let want = self.model.scan(
                    TABLE,
                    Bound::Included(&start[..]),
                    Bound::Excluded(&end[..]),
                    &[],
                    snap,
                    now,
                );
                if let Some(d) = first_diff(&got, &want) {
                    return fail(
                        FailureClass::LiveReadMismatch,
                        format!("scan at snapshot {snap}: {d}"),
                    );
                }
                if rng.chance(100_000) {
                    self.trace.push(format!("full dump @snapshot {snap}"));
                    self.compare_dump(snap, FailureClass::LiveReadMismatch)?;
                }
            }
            Op::Snapshot => {
                self.trace
                    .push(format!("snapshot {}", self.model.snapshot()));
                self.snaps.push(self.model.snapshot());
                if self.snaps.len() > 8 {
                    self.snaps.remove(0);
                }
            }
        }
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
}

/// Runs one seeded crash/replay check of `variant`.
pub fn run(seed: u64, variant: Variant, cfg: &Config) -> Result<Stats, Failure> {
    let mut sim = Sim::with_faults(seed, cfg.faults.clone());
    let vfs = sim.vfs();
    let mut model = Model::new();
    model.create_table(TABLE, families());
    let store = Toy::open(&vfs, variant, &families()).expect("open on a fresh filesystem");
    let base = vfs.now_micros();
    let world = Rc::new(RefCell::new(World {
        seed,
        variant,
        cfg: cfg.clone(),
        vfs: vfs.clone(),
        store,
        model,
        base,
        snaps: Vec::new(),
        trace: Vec::new(),
        op_index: 0,
        stats: Stats::default(),
        failure: None,
    }));

    let tasks = cfg.tasks.max(1);
    for t in 0..tasks {
        let world = world.clone();
        let vfs = vfs.clone();
        let mut workload =
            Workload::new(seed ^ 0x5eed ^ ((t as u64) << 40), TABLE, cfg.spec.clone())
                .take(cfg.ops.div_ceil(tasks));
        sim.spawn(
            "client",
            Box::new(move |rng| {
                let mut w = world.borrow_mut();
                if w.failure.is_some() {
                    return Step::Done;
                }
                let Some(op) = workload.next() else {
                    return Step::Done;
                };
                w.op_index += 1;
                w.stats.ops += 1;
                if let Err(f) = w.step(op, rng) {
                    let failure = Failure {
                        seed: w.seed,
                        variant: w.variant,
                        class: f.class,
                        op_index: w.op_index - 1,
                        message: f.message,
                        trace: std::mem::take(&mut w.trace),
                    };
                    w.failure = Some(failure);
                    return Step::Done;
                }
                // Sometimes think for a while: sleeping advances simulated time.
                if rng.chance(300_000) {
                    Step::SleepUntil(vfs.monotonic_nanos() + rng.below(5_000))
                } else {
                    Step::Ready
                }
            }),
        );
    }
    sim.run_until_idle();

    let mut w = world.borrow_mut();
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
                variant,
                class: f.class,
                op_index: cfg.ops,
                message: f.message,
                trace: std::mem::take(&mut w.trace),
            });
        }
    }
    match w.failure.take() {
        Some(f) => Err(f),
        None => Ok(w.stats),
    }
}
