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
//! middle of a commit (`FaultPlan::crash_after_ops`). Recovery is checked against what the
//! files hold: the manifest's per-stream checkpoints and per-slot flushed seqnos (read before
//! the reopen), and every stream's surviving records after its checkpoint. Each stream keeps
//! a prefix of its records (decision D84): a record survives if the WAL still holds it or
//! its commit is in SSTs below the checkpoint. The harness tracks the order records were
//! appended to each stream (`Engine::take_appended`), and on every crash hands those records
//! and the surviving prefixes to `pigeonhole_sim::recovered_from_records` (D114): a
//! single-shard commit survives iff its record does, a cross-shard commit iff every PREPARE
//! and its COMMIT do (D83), however other commits' records interleave. Commits only SSTs
//! still hold are recognized by their raw entries (`Engine::raw_entries`). The model is
//! rebuilt with `Model::from_commits`, the durability promise checked with
//! `check_acknowledged_survive`, and the purges of every durable bottommost compaction
//! re-applied (decision D74). An armed power loss can fire on a shard's background I/O
//! (a flush or compaction) with nothing in flight: the next step that sees an error while
//! the liveness probe is dead recovers from that crash instead of failing (issue #62).
//!
//! **Flush and compaction.** Memtables are tiny, so flushes and compactions run throughout;
//! the workload also calls `flush` and `compact` explicitly. Every compaction the engine
//! commits is read back (`Engine::take_compactions`): bottommost ones are applied to the model
//! as `Model::purge`, and the dumps of every held snapshot are re-checked, so a compaction
//! never changes a read at a live snapshot.
// Shared by several test binaries, each using a subset of it.
#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::future::Future;
use std::ops::Bound;
use std::path::Path;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use pigeonhole_engine::{
    AppendedKind, CompactionRecord, Compression, Engine, EngineOptions, EngineShard, Error,
    FamilyId, FamilyOptions, PendingCommit, PendingMaintenance, PickerOptions, Predicate, ScanSpec,
    Snapshot, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::key::decode_key;
use pigeonhole_format::manifest::{CompactionStyle, FamilyKind};
use pigeonhole_format::wal::WalRecord;
use pigeonhole_format::{Durability, Lsn, ManifestVersion, Seqno, StreamId, TableId, Timestamp};
use pigeonhole_io::sim::{CrashKind, FaultPlan, SimOp, SimVfs};
use pigeonhole_io::{ErrorKind, FileRef, OpenOptions, Vfs};
use pigeonhole_sim::{
    COUNTER_TS, CommitStreams, Model, ModelCell, ModelFamily, ModelOp, ModelPurge, Op, Rng, Sim,
    Step, StreamCommit, StreamRecord, Workload, WorkloadSpec, check_acknowledged_survive,
    combine_counter_writes, recovered_from_records,
};

pub const TABLE: &str = "t";
pub const DB: &str = "/db/data.phdb";

/// A table is one tablet on one shard until splits exist, so the checker spreads rows over
/// several tables (deterministically by row) to get cross-shard commits.
pub const TABLES: [&str; 4] = ["t0", "t1", "t2", "t3"];

/// Splits, merges and moves `engine` completed since open, summed over shards.
pub fn tablet_changes(engine: &Engine) -> (u64, u64, u64) {
    engine.shard_stats().iter().fold((0, 0, 0), |(s, m, v), x| {
        (s + x.splits, m + x.merges, v + x.moves)
    })
}

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

/// The families every model-check run uses. `counter` is a 0.1.0-style family with the
/// `i64` add operator (operands at the commit timestamp, D41). The `counters` test target
/// swaps `g` and `ttl` for the counter families `sum` and `sum_ttl` (D179; the workload
/// writes buckets only in `sum_ttl`), keeping four families per table; the `slots` target
/// has all six (#283).
pub fn families() -> Vec<ModelFamily> {
    let f = |name: &str, max_versions, ttl_micros, i64_add| ModelFamily {
        name: name.into(),
        max_versions,
        ttl_micros,
        i64_add,
        counter: is_sum(name),
    };
    if env!("CARGO_CRATE_NAME") == "slots" {
        // Six families: 24 slots on a shard holding every table (#283).
        vec![
            f("f", 0, 0, false),
            f("g", 2, 0, false),
            f("ttl", 0, 40, false),
            f("counter", 3, 0, true),
            f("sum", 2, 0, false),
            f("sum_ttl", 0, 40, false),
        ]
    } else if env!("CARGO_CRATE_NAME") == "counters" {
        vec![
            f("f", 0, 0, false),
            f("counter", 3, 0, true),
            f("sum", 2, 0, false),
            f("sum_ttl", 0, 40, false),
        ]
    } else {
        vec![
            f("f", 0, 0, false),
            f("g", 2, 0, false),
            f("ttl", 0, 40, false),
            f("counter", 3, 0, true),
        ]
    }
}

/// A counter family of decision D179.
fn is_sum(family: &str) -> bool {
    family.starts_with("sum")
}

/// A family holding only `i64`s (what `put_i64` writes).
fn is_i64(family: &str) -> bool {
    family.starts_with("counter") || is_sum(family)
}

/// Where a put or operand without a timestamp lands: the counter's timestamp in a counter
/// family, else the commit's.
fn default_ts(family: &str, commit_ts: Timestamp) -> Timestamp {
    if is_sum(family) {
        COUNTER_TS
    } else {
        commit_ts
    }
}

/// Moves the workload's logical explicit timestamps up by `base`, except a counter family's
/// fixed timestamp (a cell delete of the counter itself).
fn shift_ts(op: &mut ModelOp, base: Timestamp) {
    if let ModelOp::Put {
        ts: Some(t),
        family,
        ..
    }
    | ModelOp::Incr {
        ts: Some(t),
        family,
        ..
    }
    | ModelOp::DeleteCell { ts: t, family, .. } = op
        && !(is_sum(family) && *t == COUNTER_TS)
    {
        *t += base;
    }
}

/// Whether `op` writes at the commit timestamp.
fn takes_commit_ts(op: &ModelOp) -> bool {
    match op {
        ModelOp::Put { ts, family, .. } | ModelOp::Incr { ts, family, .. } => {
            ts.is_none() && !is_sum(family)
        }
        ModelOp::DeleteCell { .. } => false,
        _ => true,
    }
}

/// The engine options of a model family. `g` compacts tiered (issue #31) and `ttl` FIFO by
/// time (issue #32), so every suite runs every picker, and `f` uses zstd (issue #44); the
/// reference model depends on neither.
fn family_options(f: &ModelFamily) -> FamilyOptions {
    let compaction = match f.name.as_str() {
        "g" => CompactionStyle::Tiered,
        "ttl" => CompactionStyle::FifoByTime,
        _ => CompactionStyle::Leveled,
    };
    FamilyOptions {
        compaction,
        // `f` stores its blocks with zstd (#44), the others with LZ4.
        compression: if f.name == "f" {
            Compression::Zstd
        } else {
            Compression::Lz4
        },
        // Small blob thresholds (values are up to 160 bytes), so flushes and compactions
        // separate many values and blob GC runs (issue #33).
        blob_threshold: match f.name.as_str() {
            "f" => 40,
            "g" => 100,
            "ttl" => 60,
            _ => FamilyOptions::default().blob_threshold,
        },
        max_versions: f.max_versions,
        ttl_micros: f.ttl_micros,
        merge_operator: if f.i64_add || f.counter {
            "pigeonhole.i64_add".to_owned()
        } else {
            String::new()
        },
        kind: if f.counter {
            FamilyKind::Counter
        } else {
            FamilyKind::Standard
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
    /// `final_dump` only: a flush and full compaction every this many ops (with background
    /// compaction off, so purges happen at the same points for every shard count).
    pub compact_every: Option<usize>,
    /// A memtable freezes at this many bytes (tiny, so flushes run throughout).
    pub memtable_freeze_bytes: u64,
    /// Compaction tuning (small levels, so compactions run throughout).
    pub compaction: PickerOptions,
    /// Per-op probability (ppm) of an explicit `flush` / `compact`.
    pub flush_ppm: u32,
    pub compact_ppm: u32,
    /// Turns on `EngineOptions::tablet_changes` (splits, merges, moves and the balancer) and
    /// the harness's allowances for them; the knobs below need it. Off, the engine and the
    /// harness behave as without tablet changes.
    pub tablet_changes: bool,
    /// Per-op probability (ppm) of requesting a random tablet split, merge or move; it runs
    /// while the workload goes on (interleaved with commits, two-phase commits and crashes).
    pub tablet_ops_ppm: u32,
    /// Run the balancer every few operations with tiny thresholds, so splits, moves and
    /// merges also happen on their own.
    pub balance_fast: bool,
    /// `final_dump` only: a deterministic split, move or merge every this many ops.
    pub tablet_every: Option<usize>,
    /// `SimVfs::set_deferred_io`: submitted I/O (WAL group syncs, root commits) stays in
    /// flight until the scheduler completes it, in an order the seed picks, so it spans
    /// shard slices and client steps. Defaults to `PIGEONHOLE_DEFERRED_IO` (`1` on).
    pub deferred_io: bool,
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
        let mut compaction = PickerOptions::default();
        compaction.l0_trigger = 2;
        compaction.level_base_bytes = 48 << 10;
        compaction.level_multiplier = 2;
        compaction.max_levels = 4;
        compaction.target_sst_bytes = 64 << 10;
        // Tablet changes are on, as in `EngineOptions`. `PIGEONHOLE_TABLET_CHANGES=1` adds the
        // fast balancer to every suite built on this config, so tablets change within a run;
        // `=0` turns tablet changes off.
        let tablets_env = std::env::var("PIGEONHOLE_TABLET_CHANGES").ok();
        Config {
            ops,
            faults,
            crash_ppm: 15_000,
            mid_commit_crash_ppm: 25_000,
            spec,
            shards: 2,
            reopen_shards: Vec::new(),
            memtable_budget: 1 << 20,
            durability: None,
            crash_at: None,
            dump_ppm: 100_000,
            tasks: 4,
            cas_ppm: 80_000,
            txn_ppm: 80_000,
            crash_every: None,
            compact_every: None,
            memtable_freeze_bytes: 16 << 10,
            compaction,
            flush_ppm: 30_000,
            compact_ppm: 15_000,
            tablet_changes: tablets_env.as_deref() != Some("0"),
            tablet_ops_ppm: 0,
            balance_fast: tablets_env.as_deref() == Some("1"),
            tablet_every: None,
            deferred_io: std::env::var("PIGEONHOLE_DEFERRED_IO").is_ok_and(|v| v == "1"),
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
    /// Armed power losses that fired on background I/O and surfaced as a later step's error.
    pub background_crashes: usize,
    pub busy: usize,
    pub flushes: u64,
    pub compactions: u64,
    pub purges: usize,
    /// Records naming a seqno no commit of the client holds (refused attempts, lost
    /// commits), left out of the recovery oracle.
    pub unowned_records: usize,
    /// Earlier copies of a `(seqno, kind)` whose seqno recovery reused after a crash.
    pub reused_records: usize,
    /// Surviving seqnos with only PREPAREs and no COMMIT on any stream (an attempt a shard
    /// changing a tablet refused, retried under a new seqno; D83 never recovers them), kept
    /// out of the match with unacknowledged commits. Tablet changes only.
    pub refused_attempts: usize,
    /// Commits recovered only from SSTs (their WAL records were checkpointed away).
    pub sst_only: usize,
    /// PREPAREs of an unacknowledged cross-shard commit with no COMMIT on any stream that a
    /// checkpoint passed: an aborted or undecided attempt's PREPARE passes at once (D116),
    /// and such a commit is never recovered (D83, D114).
    pub undecided_prepares_passed: usize,
    /// Mutating VFS operations the run made before its final crash: the crash points a
    /// sweep of the same seed and config must cover.
    pub mutating_ops: u64,
    /// Tablet changes requested by the workload that completed, and refused ones.
    pub tablet_changes: usize,
    pub tablet_refused: usize,
    /// Splits, merges and moves the engines performed (requested or the balancer's).
    pub engine_tablet_changes: (u64, u64, u64),
    /// Client steps that found submitted I/O still in flight (`Config::deferred_io`).
    pub io_in_flight_steps: usize,
    /// Reads at a held snapshot checked against the model rebuilt for its view, because a
    /// compaction published after it purged something (`World::model_at`).
    pub reads_at_older_views: usize,
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
    /// A flush or compaction changed a read at a snapshot that was live when it ran.
    SnapshotChanged,
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
        let shown: usize = std::env::var("PIGEONHOLE_TRACE_LINES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(40);
        let skip = self.trace.len().saturating_sub(shown);
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
    format!("{}:{}", op_table(op), show_in_table(op))
}

fn show_in_table(op: &ModelOp) -> String {
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
            ts,
            delta,
            ..
        } => format!(
            "incr {}/{family}:{} ts={ts:?} {delta:+}",
            text(row),
            text(qualifier)
        ),
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
    /// Set by any shard's wakeup callback: work arrived for a shard that reported idle.
    pub woke: Arc<AtomicBool>,
    /// Completes deferred I/O between shard passes (see `Config::deferred_io`).
    pub vfs: Arc<SimVfs>,
}

impl Drop for Store {
    /// A dropped shard's final sync waits for its stream's older syncs (#190). A device
    /// completes those while it waits; with deferred I/O this thread is the device, so it
    /// completes everything in flight first (after a crash, the operations fail).
    fn drop(&mut self) {
        self.vfs.complete_all_io();
    }
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

/// Options with the checker's flush and compaction knobs.
pub fn options_for(vfs: Arc<SimVfs>, shards: usize, cfg: &Config) -> EngineOptions {
    let mut o = options(vfs, shards, cfg.memtable_budget);
    o.memtable_freeze_bytes = cfg.memtable_freeze_bytes;
    o.compaction = cfg.compaction.clone();
    o.tablet_changes = cfg.tablet_changes;
    if cfg.balance_fast {
        // The clock moves 1 µs per operation: a balancer pass every ~15 operations.
        o.balance_interval_nanos = 15_000;
        o.balance_min_writes = 3;
        o.tablet_split_bytes = 24 << 10;
    }
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
        Self::open_with(vfs, options(Arc::clone(vfs), shards, budget), fams)
    }

    /// Opens with the checker's options.
    pub fn open_cfg(vfs: &Arc<SimVfs>, shards: usize, cfg: &Config) -> Result<Self, Error> {
        let store = Self::open_with(vfs, options_for(Arc::clone(vfs), shards, cfg), &families())?;
        // Values above 120 bytes (of up to 160) are separated at commit time (#230). Not
        // with deferred I/O: the commit waits for a manifest commit on the thread that must
        // also complete the in-flight I/O a background manifest commit holds.
        if !cfg.deferred_io {
            store.engine.set_inline_value_limit(LARGE_VALUE);
        }
        Ok(store)
    }

    fn open_with(
        vfs: &Arc<SimVfs>,
        options: EngineOptions,
        fams: &[ModelFamily],
    ) -> Result<Self, Error> {
        let (engine, mut shards) = Engine::open_application_owned(Path::new(DB), options)?;
        // The harness reads the append order and the compactions back (`take_appended`,
        // `take_compactions`).
        engine.record_history(true);
        let woke = Arc::new(AtomicBool::new(false));
        for s in &mut shards {
            let w = Arc::clone(&woke);
            s.set_wakeup(Box::new(move || w.store(true, Ordering::Release)));
        }
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
            woke,
            vfs: Arc::clone(vfs),
        })
    }

    /// Runs every shard until none has work left, no shard was woken during the last pass
    /// (a shard may wake another one that already reported idle in the same pass) and no
    /// deferred I/O is in flight (one operation completes per pass).
    pub fn run_until_idle(&mut self) {
        loop {
            self.woke.store(false, Ordering::Release);
            let mut more = false;
            for s in &mut self.shards {
                more |= s.run_once(u64::MAX);
            }
            more |= self.vfs.complete_io();
            if !more && !self.woke.load(Ordering::Acquire) {
                return;
            }
        }
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
                    let value = match (is_i64(family), <[u8; 8]>::try_from(value.as_slice())) {
                        (true, Ok(b)) => ValueRef::I64(i64::from_le_bytes(b)),
                        _ => ValueRef::Bytes(value),
                    };
                    wb.put(t, fam(family), row, qualifier, *ts, value)?
                }
                ModelOp::Incr {
                    row,
                    family,
                    qualifier,
                    ts: None,
                    delta,
                    ..
                } => wb.merge(t, fam(family), row, qualifier, ValueRef::I64(*delta))?,
                ModelOp::Incr {
                    row,
                    family,
                    qualifier,
                    ts: Some(ts),
                    delta,
                    ..
                } => wb.merge_at(t, fam(family), row, qualifier, *ts, ValueRef::I64(*delta))?,
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

    /// Runs every shard once, then completes one deferred I/O operation if any is in flight.
    pub fn step_shards(&mut self, now: u64) {
        for s in &mut self.shards {
            s.run_once(now + 1_000);
        }
        self.vfs.complete_io();
    }

    /// Drives the shards until `m` resolves (or the engine dies).
    pub fn drive(
        &mut self,
        vfs: &Arc<SimVfs>,
        mut m: PendingMaintenance,
        alive: impl Fn() -> bool,
    ) -> Result<(), Error> {
        let mut cx = Context::from_waker(Waker::noop());
        for step in 0..200_000 {
            match Pin::new(&mut m).poll(&mut cx) {
                Poll::Ready(r) => return r,
                Poll::Pending => self.step_shards(vfs.monotonic_nanos()),
            }
            if step % 64 == 63 && !alive() {
                return Err(Error::Io(pigeonhole_io::Error::new(
                    ErrorKind::Crashed,
                    "crashed during maintenance",
                )));
            }
        }
        panic!("maintenance did not finish in 200k steps")
    }

    /// The table and family ids of a model op's `(table, family)`.
    pub fn ids(&self, table: &str, family: &str) -> (TableId, FamilyId) {
        (
            self.tables[table].id,
            self.family_ids[&(table.to_owned(), family.to_owned())],
        )
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

/// The payload length above which `Store::open_cfg` separates values at commit time.
const LARGE_VALUE: usize = 120;

/// A put's value as the recovery oracle compares it: itself, or for a value above
/// [`LARGE_VALUE`] (logged as a blob pointer when separated at commit time) its length.
fn record_value(value: &[u8]) -> Vec<u8> {
    if value.len() > LARGE_VALUE {
        large_value_key(value.len())
    } else {
        value.to_vec()
    }
}

fn large_value_key(len: usize) -> Vec<u8> {
    format!("<large {len}>").into_bytes()
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
// WAL records
// ---------------------------------------------------------------------------------------

/// A record kind, as it sits in a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RecKind {
    Batch,
    Prepare,
    Commit,
}

/// What the WAL holds (after every stream's checkpoint).
#[derive(Debug, Default)]
pub struct WalRead {
    /// Per stream, its records in log order.
    pub streams: BTreeMap<u32, Vec<(Seqno, RecKind)>>,
    /// The mutations and timestamp of every seqno with a Batch or Prepare record.
    pub batches: BTreeMap<Seqno, Survivor>,
    /// COMMIT decisions: `(coordinator stream, seqno) -> participant streams`.
    pub commits: BTreeMap<(u32, Seqno), Vec<u32>>,
}

/// A commit's records as the WAL holds them.
#[derive(Debug, Clone)]
pub struct Survivor {
    pub seqno: Seqno,
    pub commit_ts: Timestamp,
    /// The encoded mutations, per record.
    pub batches: Vec<Vec<u8>>,
}

/// Reads every stream of `db` from its manifest checkpoint.
pub fn read_wal(
    vfs: &Arc<SimVfs>,
    db: &Path,
    checkpoints: &BTreeMap<StreamId, Lsn>,
) -> Result<WalRead, String> {
    let vfs_ref: pigeonhole_io::VfsRef = Arc::clone(vfs) as pigeonhole_io::VfsRef;
    let opened =
        pigeonhole_pager::Pager::open(&vfs_ref, db, false).map_err(|e| format!("pager: {e}"))?;
    let db_id = opened.db_id();
    drop(opened);
    let streams =
        pigeonhole_wal::discover_streams(&vfs_ref, db).map_err(|e| format!("discover: {e}"))?;
    let mut out = WalRead::default();
    for stream in streams {
        let checkpoint = checkpoints.get(&stream).copied().unwrap_or_default();
        let mut rec = pigeonhole_wal::Recovery::open(&vfs_ref, db, stream, db_id, checkpoint)
            .map_err(|e| format!("recovery open {}: {e}", stream.0))?;
        let list = out.streams.entry(stream.0).or_default();
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
                    list.push((seqno, RecKind::Batch));
                    out.batches
                        .entry(seqno)
                        .or_insert_with(|| Survivor {
                            seqno,
                            commit_ts,
                            batches: Vec::new(),
                        })
                        .batches
                        .push(batch.as_bytes().to_vec());
                }
                WalRecord::Prepare {
                    seqno,
                    commit_ts,
                    batch,
                    ..
                } => {
                    list.push((seqno, RecKind::Prepare));
                    out.batches
                        .entry(seqno)
                        .or_insert_with(|| Survivor {
                            seqno,
                            commit_ts,
                            batches: Vec::new(),
                        })
                        .batches
                        .push(batch.as_bytes().to_vec());
                }
                WalRecord::Commit {
                    seqno,
                    participants,
                } => {
                    list.push((seqno, RecKind::Commit));
                    out.commits.insert(
                        (stream.0, seqno),
                        participants.iter().map(|p| p.0).collect(),
                    );
                }
            }
        }
    }
    Ok(out)
}

/// The commit timestamps of every logged seqno.
pub fn logged_timestamps_of(wal: &WalRead) -> BTreeMap<Seqno, Timestamp> {
    wal.batches.iter().map(|(k, v)| (*k, v.commit_ts)).collect()
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
    /// The streams its records go to (as submitted; fixed for the commit's life).
    streams: CommitStreams,
    acked: bool,
    kind: Kind,
}

/// Where a commit's records go: one stream, or every written and read shard with the shard
/// of the first row as coordinator (the engine's routing).
fn streams_of(shards: &[u16], reads: &[u16]) -> CommitStreams {
    let mut all: Vec<usize> = shards.iter().map(|s| *s as usize).collect();
    for r in reads {
        if !all.contains(&(*r as usize)) {
            all.push(*r as usize);
        }
    }
    match all.as_slice() {
        [] => CommitStreams::Single(0),
        [s] => CommitStreams::Single(*s),
        _ => CommitStreams::Cross {
            coordinator: all[0],
            participants: all,
        },
    }
}

/// The records of `streams`, as `(stream, kind)` in the order the engine appends them per
/// stream (a participant's PREPARE before the coordinator's COMMIT).
fn records_of(streams: &CommitStreams) -> Vec<(u32, RecKind)> {
    match streams {
        CommitStreams::Single(s) => vec![(*s as u32, RecKind::Batch)],
        CommitStreams::Cross {
            participants,
            coordinator,
        } => participants
            .iter()
            .map(|p| (*p as u32, RecKind::Prepare))
            .chain(std::iter::once((*coordinator as u32, RecKind::Commit)))
            .collect(),
    }
}

/// A compaction (or a flush that purged versions, #287) the engine committed, to replay on
/// the model after a recovery.
#[derive(Debug, Clone)]
struct PurgeEvent {
    manifest_version: ManifestVersion,
    /// Only a bottommost compaction purges; the others still bound historical reads.
    bottommost: bool,
    /// A flush's guarded version purge over exactly these engine seqnos (#287), and the
    /// visible seqno when it installed.
    flush_inputs: Option<Vec<Seqno>>,
    flush_installed: Seqno,
    table: String,
    family: String,
    /// Engine seqnos (mapped to model seqnos when applied).
    snapshots: Vec<Seqno>,
    now: Timestamp,
    min_ts_above: Timestamp,
    max_seqno: Seqno,
    rows: (Bound<Vec<u8>>, Bound<Vec<u8>>),
}

impl PurgeEvent {
    /// Whether the event purges anything (a bottommost compaction or a flush's version
    /// purge); the others only bound historical reads.
    fn purges(&self) -> bool {
        self.bottommost || self.flush_inputs.is_some()
    }
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
    /// Attempts the engine refused (a false predicate, a conflict, a full arena): a
    /// cross-shard one may have left PREPARE records under a seqno of its own.
    aborted: Vec<Committed>,
    /// Seqnos of WAL survivors a recovery already settled as never committed: a refused
    /// attempt's PREPAREs, or a commit the crash lost. Their records stay valid until a
    /// checkpoint passes them, so a later recovery reads them again, and they must not be
    /// matched to a commit submitted after the recovery that settled them (issue #162).
    /// Only seqnos whose records the WAL holds at that recovery are settled, and the engine
    /// never hands one out again: it starts above every seqno the WAL replays.
    settled_seqnos: BTreeSet<Seqno>,
    in_flight: Vec<InFlight>,
    /// A shard reported a poisoned stream: reopen once nothing is in flight.
    need_reopen: bool,
    /// Microseconds to move the clock once the current batch has completed, past the
    /// default timestamps its group assigned.
    pending_advance: u64,
    base: u64,
    snaps: Vec<Snapshot>,
    /// `Stats::reads_at_older_views`, counted by `&self` reads.
    reads_at_older_views: std::cell::Cell<usize>,
    trace: Vec<String>,
    op_index: usize,
    stats: Stats,
    failure: Option<Failure>,
    reopens: usize,
    done: bool,
    workload: Option<std::iter::Take<Workload>>,
    /// Ops taken from the workload but not yet run (lookahead for batching).
    queued: std::collections::VecDeque<Op>,
    /// Per stream, the records appended to it in order, as observed in the WAL (plus
    /// `None` commits, placed when they resolve: their records wait in the buffer).
    stream_records: BTreeMap<u32, Vec<(Seqno, RecKind)>>,
    /// Per stream, parallel to `stream_records`: how many crashes had happened when each
    /// record was appended (a seqno may be reused only across a crash).
    stream_epochs: BTreeMap<u32, Vec<usize>>,
    /// Commit timestamps of the batch records the engine appended, by seqno, with the crash
    /// count when appended (seqnos are reused only across a crash).
    appended_ts: HashMap<Seqno, (usize, Timestamp)>,
    /// Bottommost compactions applied to the model, by manifest version.
    purges: Vec<PurgeEvent>,
    /// Compactions reported by the engine whose manifest version is not published yet.
    pending_purges: Vec<CompactionRecord>,
    /// Reads at seqnos at or below this (engine seqnos) may differ from the model: a
    /// compaction dropped versions no live snapshot needed.
    compaction_floor: Seqno,
    /// Whether the configured fault plan is active (off while recovery checks the files).
    faults_active: bool,
    /// A tablet change requested by the workload, still running.
    tablet_pending: Option<(PendingMaintenance, String)>,
}

impl World {
    #[allow(clippy::too_many_arguments)]
    fn new(
        seed: u64,
        cfg: &Config,
        vfs: &Arc<SimVfs>,
        probe: FileRef,
        model: Model,
        store: Store,
        reopens: usize,
        workload: Option<std::iter::Take<Workload>>,
    ) -> Self {
        World {
            seed,
            cfg: cfg.clone(),
            vfs: Arc::clone(vfs),
            probe,
            store: Some(store),
            model,
            engine_seqnos: Vec::new(),
            history: BTreeMap::new(),
            unacked: Vec::new(),
            aborted: Vec::new(),
            settled_seqnos: BTreeSet::new(),
            in_flight: Vec::new(),
            need_reopen: false,
            pending_advance: 0,
            base: vfs.now_micros(),
            snaps: Vec::new(),
            reads_at_older_views: std::cell::Cell::new(0),
            trace: Vec::new(),
            op_index: 0,
            stats: Stats::default(),
            failure: None,
            reopens,
            done: false,
            workload,
            queued: std::collections::VecDeque::new(),
            stream_records: BTreeMap::new(),
            stream_epochs: BTreeMap::new(),
            appended_ts: HashMap::new(),
            purges: Vec::new(),
            pending_purges: Vec::new(),
            compaction_floor: 0,
            faults_active: true,
            tablet_pending: None,
        }
    }

    fn now(&self) -> u64 {
        self.vfs.now_micros()
    }

    fn alive(&self) -> bool {
        self.probe.len().is_ok()
    }

    /// A step saw an error with the liveness probe dead: an armed power loss fired on a
    /// shard's background I/O (a flush or compaction) while nothing was in flight, and the
    /// error is that crash, not a divergence (issue #62). Recovers from it.
    fn background_crash(&mut self, e: impl fmt::Display, rng: &mut Rng) -> Result<(), Fail> {
        self.stats.background_crashes += 1;
        self.trace.push(format!(
            "  -> the armed power loss fired in the background ({e})"
        ));
        self.crash_and_recover(CrashKind::Power, true, rng)
    }

    /// The outcome of a check that reads the store (a dump, a held snapshot's re-check):
    /// a failure with the liveness probe dead is an armed power loss that fired in the
    /// background, so the run recovers from it instead. Returns whether it did.
    fn recover_if_fired(&mut self, r: Result<(), Fail>, rng: &mut Rng) -> Result<bool, Fail> {
        match r {
            Ok(()) => Ok(false),
            Err(f) if !self.alive() => {
                self.background_crash(f.message, rng)?;
                Ok(true)
            }
            Err(f) => Err(f),
        }
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
        // In the engine's routing order: `Engine::route` moves row deletes after every other
        // mutation, and the first shard coordinates a cross-shard commit.
        let (rest, row_deletes): (Vec<&ModelOp>, Vec<&ModelOp>) = ops
            .iter()
            .partition(|op| !matches!(op, ModelOp::DeleteRow { .. }));
        for op in rest.into_iter().chain(row_deletes) {
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

    /// Appends the records the engine logged since the last call to the per-stream append
    /// order (the engine reports every record it appends, in order, test hook).
    fn drain_appended(&mut self) {
        let Some(store) = self.store.as_ref() else {
            return;
        };
        for r in store.engine.take_appended() {
            let kind = match r.kind {
                AppendedKind::Batch => RecKind::Batch,
                AppendedKind::Prepare => RecKind::Prepare,
                AppendedKind::Commit => RecKind::Commit,
            };
            if kind == RecKind::Batch {
                self.appended_ts
                    .insert(r.seqno, (self.stats.crashes, r.commit_ts));
            }
            self.stream_records
                .entry(u32::from(r.stream))
                .or_default()
                .push((r.seqno, kind));
            self.stream_epochs
                .entry(u32::from(r.stream))
                .or_default()
                .push(self.stats.crashes);
        }
    }

    /// The streams holding records of `seqno`, from the engine's append order: `None` when
    /// it appended none. A cross-shard commit whose COMMIT was never appended keeps
    /// `guess`'s coordinator.
    fn streams_from_records(&self, seqno: Seqno, guess: &CommitStreams) -> Option<CommitStreams> {
        let mut single = None;
        let mut participants = Vec::new();
        let mut coordinator = None;
        for (stream, list) in &self.stream_records {
            for (q, kind) in list {
                if *q != seqno {
                    continue;
                }
                match kind {
                    RecKind::Batch => single = Some(*stream as usize),
                    RecKind::Prepare => participants.push(*stream as usize),
                    RecKind::Commit => coordinator = Some(*stream as usize),
                }
            }
        }
        if let Some(s) = single {
            return Some(CommitStreams::Single(s));
        }
        if participants.is_empty() && coordinator.is_none() {
            return None;
        }
        let coordinator = coordinator.unwrap_or(match guess {
            CommitStreams::Cross { coordinator, .. } => *coordinator,
            CommitStreams::Single(s) => *s,
        });
        Some(CommitStreams::Cross {
            participants,
            coordinator,
        })
    }

    /// Checks the records the WAL holds now against the engine's append order: they are a
    /// contiguous window of each stream (everything before it was checkpointed, everything
    /// after it is still buffered or was lost).
    fn observe_streams(&mut self, wal: &WalRead) -> Result<(), Fail> {
        self.drain_appended();
        for (stream, observed) in &wal.streams {
            let known = self.stream_records.entry(*stream).or_default();
            let Some(first) = observed.first() else {
                continue;
            };
            let Some(k) = known.iter().position(|r| r == first) else {
                return fail(
                    FailureClass::Protocol,
                    format!(
                        "stream {stream}: the WAL holds {first:?}, which the engine never appended (appended: {known:?}, WAL: {observed:?})"
                    ),
                );
            };
            for (i, r) in observed.iter().enumerate() {
                match known.get(k + i) {
                    Some(have) if have == r => {}
                    Some(have) => {
                        return fail(
                            FailureClass::Protocol,
                            format!(
                                "stream {stream}: record {i} after the checkpoint is {r:?} but the engine appended {have:?} there"
                            ),
                        );
                    }
                    None => {
                        return fail(
                            FailureClass::Protocol,
                            format!(
                                "stream {stream}: record {i} after the checkpoint is {r:?}, beyond everything the engine appended (appended: {known:?}, WAL: {observed:?})"
                            ),
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// Reads the WAL (faults off) and merges its record order.
    fn observe_wal(&mut self) -> Result<WalRead, Fail> {
        self.vfs.set_faults(FaultPlan::none());
        self.faults_active = false;
        let manifest = Engine::inspect_manifest(
            &(Arc::clone(&self.vfs) as pigeonhole_io::VfsRef),
            Path::new(DB),
        );
        let wal = manifest.and_then(|m| {
            read_wal(&self.vfs, Path::new(DB), &m.checkpoints).map_err(Error::Corruption)
        });
        self.vfs.set_faults(self.cfg.faults.clone());
        self.faults_active = true;
        let wal = match wal {
            Ok(w) => w,
            Err(e) => return fail(FailureClass::Protocol, format!("reading the WAL: {e}")),
        };
        if self.trace_on() {
            eprintln!(
                "wal: streams {:?} batches {:?}",
                wal.streams,
                wal.batches.keys().collect::<Vec<_>>()
            );
        }
        self.observe_streams(&wal)?;
        Ok(wal)
    }

    /// Takes the compactions the engine committed; applies the purge of every durable
    /// bottommost one to the model and re-checks the held snapshots.
    fn drain_compactions(&mut self) -> Result<(), Fail> {
        // A commit still in flight may hold a seqno a compaction already took as input: its
        // purge waits until that commit is in the model (issue #98).
        let upto = if self.in_flight.is_empty() {
            Seqno::MAX
        } else {
            self.engine_seqnos.last().copied().unwrap_or(0)
        };
        self.drain_compactions_upto(upto)
    }

    /// Applies the purges of committed compactions whose inputs hold no seqno above `upto`
    /// (`max_seqno <= upto`); the others stay pending. The model then takes every purge
    /// between the commits it orders by seqno, as the engine did.
    fn drain_compactions_upto(&mut self, upto: Seqno) -> Result<(), Fail> {
        let store = self.store.as_ref().expect("store open");
        let mut records = store.engine.take_gc_records();
        records.append(&mut self.pending_purges);
        if records.is_empty() {
            return Ok(());
        }
        let published = store
            .engine
            .snapshot()
            .map_or(0, |s| s.view().manifest_version());
        let table_names: HashMap<TableId, String> = store
            .tables
            .iter()
            .map(|(n, t)| (t.id, n.clone()))
            .collect();
        let family_names = store.family_names.clone();
        let mut applied = false;
        for r in records {
            // A flush's purge also waits for every commit before its install (#287).
            if r.manifest_version > published || r.max_seqno.max(r.install_seqno.get()) > upto {
                self.pending_purges.push(r);
                continue;
            }
            if !r.flush {
                self.stats.compactions += 1;
            }
            self.compaction_floor = self.compaction_floor.max(r.max_seqno);
            applied = true;
            let (Some(table), Some(family)) =
                (table_names.get(&r.table), family_names.get(&r.family))
            else {
                continue;
            };
            let event = PurgeEvent {
                manifest_version: r.manifest_version,
                bottommost: r.bottommost,
                flush_inputs: r.input_seqnos.clone().filter(|_| r.versions_purge),
                flush_installed: r.install_seqno.get(),
                table: table.clone(),
                family: family.clone(),
                snapshots: r.snapshots.clone(),
                now: r.now,
                min_ts_above: r.min_ts_above,
                max_seqno: r.max_seqno,
                rows: (
                    r.rows.0.clone().map_or(Bound::Unbounded, Bound::Included),
                    r.rows.1.clone().map_or(Bound::Unbounded, Bound::Excluded),
                ),
            };
            if event.purges() {
                self.trace.push(format!(
                    "purge{} {}/{} at manifest {} (snapshots {:?}, min_ts_above {}, max_seqno {})",
                    if event.flush_inputs.is_some() {
                        " (flush)"
                    } else {
                        ""
                    },
                    event.table,
                    event.family,
                    event.manifest_version,
                    event.snapshots,
                    event.min_ts_above,
                    event.max_seqno
                ));
                self.apply_purge(&event);
                self.stats.purges += 1;
            }
            self.purges.push(event);
        }
        if applied {
            self.check_held_snapshots()?;
        }
        Ok(())
    }

    /// Records every compaction the engine committed but the harness has not applied yet
    /// (a maintenance call that failed, a crash): the recovery replays the durable ones.
    fn stash_compactions(&mut self) {
        let Some(store) = self.store.as_ref() else {
            self.pending_purges.clear();
            return;
        };
        let mut records = store.engine.take_gc_records();
        records.append(&mut self.pending_purges);
        let table_names: HashMap<TableId, String> = store
            .tables
            .iter()
            .map(|(n, t)| (t.id, n.clone()))
            .collect();
        for r in records {
            let (Some(table), Some(family)) =
                (table_names.get(&r.table), store.family_names.get(&r.family))
            else {
                continue;
            };
            self.purges.push(PurgeEvent {
                manifest_version: r.manifest_version,
                bottommost: r.bottommost,
                flush_inputs: r.input_seqnos.clone().filter(|_| r.versions_purge),
                flush_installed: r.install_seqno.get(),
                table: table.clone(),
                family: family.clone(),
                snapshots: r.snapshots.clone(),
                now: r.now,
                min_ts_above: r.min_ts_above,
                max_seqno: r.max_seqno,
                rows: (
                    r.rows.0.clone().map_or(Bound::Unbounded, Bound::Included),
                    r.rows.1.clone().map_or(Bound::Unbounded, Bound::Excluded),
                ),
            });
        }
    }

    fn faults_on(&self) -> bool {
        self.faults_active
    }

    fn model_purge(&self, e: &PurgeEvent) -> ModelPurge {
        ModelPurge {
            table: e.table.clone(),
            family: e.family.clone(),
            rows: e.rows.clone(),
            snapshots: e.snapshots.iter().map(|s| self.model_seqno(*s)).collect(),
            now: e.now,
            min_ts_above: e.min_ts_above,
            max_seqno: self.model_seqno(e.max_seqno),
        }
    }

    fn apply_purge(&mut self, e: &PurgeEvent) {
        let mut model = std::mem::take(&mut self.model);
        self.purge_on(&mut model, e);
        self.model = model;
    }

    /// Applies `e`'s purge to `model`: a bottommost compaction's, or a flush's guarded
    /// version purge over its input seqnos (mapped to model seqnos).
    fn purge_on(&self, model: &mut Model, e: &PurgeEvent) {
        let p = self.model_purge(e);
        match &e.flush_inputs {
            Some(inputs) => {
                let inputs: Vec<Seqno> = inputs.iter().map(|s| self.model_seqno(*s)).collect();
                model.purge_versions(&p, &inputs, self.model_seqno(e.flush_installed));
            }
            None if e.bottommost => model.purge(&p),
            None => {}
        }
    }

    /// The model a read at `snap` must match, when it is not `self.model`. A snapshot reads
    /// the view it was taken in, which holds only the compactions published by then; the
    /// model takes a purge at its `max_seqno`. Usually the same thing, but a commit applied
    /// between a bottommost compaction's GC decision and its publication counts as a later
    /// write (D74, D147), above `max_seqno`, while a snapshot taken in that window still
    /// sees the deletes the compaction purged (issue #204). For such a snapshot this is the
    /// model rebuilt from the commits and only the purges its view holds, as recovery
    /// rebuilds it.
    fn model_at(&self, snap: &Snapshot) -> Option<Model> {
        let view = snap.view().manifest_version();
        let newer = |p: &PurgeEvent| p.purges() && p.manifest_version > view;
        if !self.purges.iter().any(newer) {
            return None;
        }
        self.reads_at_older_views
            .set(self.reads_at_older_views.get() + 1);
        let commits: Vec<StreamCommit> = self
            .history
            .values()
            .map(|c| StreamCommit {
                ops: c.ops.clone(),
                commit_ts: c.commit_ts.expect("acknowledged commits have a timestamp"),
                durability: Durability::Sync,
                streams: c.streams.clone(),
            })
            .collect();
        let mut model = Model::from_commits(
            |m| {
                for t in TABLES {
                    m.create_table(t, families());
                }
            },
            &commits,
        );
        for p in self.purges.iter().filter(|p| p.purges() && !newer(p)) {
            self.purge_on(&mut model, p);
        }
        Some(model)
    }

    /// Every held snapshot still reads as the model says (at the current clock: TTL moves
    /// on), so a flush or compaction never changed a read at a live snapshot.
    fn check_held_snapshots(&self) -> Result<(), Fail> {
        for snap in &self.snaps {
            self.compare_dump(snap, FailureClass::SnapshotChanged)?;
        }
        Ok(())
    }

    /// Runs an explicit flush or full compaction, driving the shards until it finishes.
    fn maintenance(&mut self, compact: bool, rng: &mut Rng) -> Result<(), Fail> {
        self.trace
            .push(if compact { "compact" } else { "flush" }.to_owned());
        let armed = self.arm(rng);
        let vfs = Arc::clone(&self.vfs);
        let store = self.store.as_mut().expect("store open");
        let pending = if compact {
            store.engine.compact_pending(None)
        } else {
            store.engine.flush_pending()
        };
        let probe = self.probe.clone();
        let result = match pending {
            Ok(m) => store.drive(&vfs, m, || probe.len().is_ok()),
            Err(e) => Err(e),
        };
        if armed {
            self.vfs.set_faults(self.cfg.faults.clone());
            self.faults_active = true;
            self.faults_active = true;
        }
        match result {
            Ok(()) => {
                if !compact {
                    self.stats.flushes += 1;
                }
            }
            Err(e) if !self.alive() || is_crashed(&e) => {
                self.stats.mid_commit_crashes += usize::from(armed);
                self.crash_and_recover(CrashKind::Power, true, rng)?;
                return Ok(());
            }
            Err(Error::Io(_)) => {
                self.stats.io_errors += 1;
                self.trace
                    .push("  -> maintenance I/O error; reopen pending".into());
                self.need_reopen = true;
                return Ok(());
            }
            Err(e) => return fail(FailureClass::Protocol, format!("maintenance: {e}")),
        }
        let checked = self
            .drain_compactions()
            .and_then(|()| self.check_held_snapshots());
        self.recover_if_fired(checked, rng).map(|_| ())
    }

    fn compare_dump(&self, snap: &Snapshot, class: FailureClass) -> Result<(), Fail> {
        let now = self.now();
        let engine = match self.store().dump(snap) {
            Ok(d) => d,
            Err(Error::Io(_)) if self.cfg.faults.io_error_ppm > 0 && self.faults_on() => {
                // An injected read failure: the dump reports it, nothing else.
                return Ok(());
            }
            Err(e) => return fail(FailureClass::Protocol, format!("dump failed: {e}")),
        };
        let rebuilt = self.model_at(snap);
        let model = rebuilt.as_ref().unwrap_or(&self.model);
        let model = match model_dump(model, self.model_seqno(snap.seqno()), now) {
            Ok(d) => d,
            Err(e) => return fail(FailureClass::Protocol, format!("model dump failed: {e}")),
        };
        if let Some(d) = first_diff(&engine, &model) {
            return fail(
                class,
                format!(
                    "state at engine seqno {} (model {}) differs: {d}",
                    snap.seqno(),
                    self.model_seqno(snap.seqno())
                ),
            );
        }
        // Blob live counts match the pointers the SSTs hold (reads may fail when faults
        // are injected; the check then proves nothing).
        if !(self.cfg.faults.io_error_ppm > 0 && self.faults_on())
            && let Err(e) = self.store().engine.check_blob_accounting()
        {
            return fail(
                FailureClass::LiveReadMismatch,
                format!("blob accounting: {e}"),
            );
        }
        Ok(())
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
        // Counter writes of one cell combine before they are logged (#295).
        for op in &combine_counter_writes(ops, |_, f| is_sum(f)) {
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
                    // The engine logs a counter family's default timestamp as explicit.
                    let ts = ts.or(is_sum(family).then_some(COUNTER_TS));
                    out.insert((
                        t,
                        family.clone(),
                        1,
                        row.clone(),
                        qualifier.clone(),
                        ts,
                        record_value(value),
                    ));
                }
                ModelOp::Incr {
                    row,
                    family,
                    qualifier,
                    ts,
                    delta,
                    ..
                } => {
                    let ts = ts.or(is_sum(family).then_some(COUNTER_TS));
                    out.insert((
                        t,
                        family.clone(),
                        2,
                        row.clone(),
                        qualifier.clone(),
                        ts,
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
                // A value separated at commit time (#230) is logged as its blob pointer,
                // which may name a file blob GC has dropped since: large values compare
                // by length (`record_value`).
                let payload = match pigeonhole_format::value::decode_value(m.value) {
                    Ok(ValueRef::Blob(p)) => large_value_key(p.len as usize - 1),
                    Ok(v) => record_value(&value_bytes(v)),
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

    /// Crashes (or notes the fault plan already did), works out from the manifest, the WAL
    /// and the reopened engine's raw entries which commits recovery must have applied,
    /// checks the durability promises, rebuilds the model from those commits and compares
    /// the reopened engine with it.
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
        // Whatever was in flight may have consumed default timestamps the recovered floor
        // keeps: the clock moves past them, as it would have after their acknowledgement.
        if self.pending_advance > 0 {
            self.vfs.advance(1_000 * self.pending_advance);
            self.pending_advance = 0;
        }
        self.stash_compactions();
        self.drain_appended();
        self.tablet_pending = None;
        self.count_tablet_changes();
        self.store = None;
        self.snaps.clear();
        self.vfs.set_faults(FaultPlan::none());
        self.faults_active = false;

        // What the files hold, before the reopen changes them (a changed shard count flushes
        // and checkpoints everything at open).
        let vfs_ref: pigeonhole_io::VfsRef = Arc::clone(&self.vfs) as pigeonhole_io::VfsRef;
        let manifest = match Engine::inspect_manifest(&vfs_ref, Path::new(DB)) {
            Ok(m) => m,
            Err(e) => return fail(FailureClass::Protocol, format!("reading the manifest: {e}")),
        };
        let wal = match read_wal(&self.vfs, Path::new(DB), &manifest.checkpoints) {
            Ok(w) => w,
            Err(e) => return fail(FailureClass::Protocol, format!("reading the WAL: {e}")),
        };
        self.observe_streams(&wal)?;
        if self.trace_on() {
            eprintln!(
                "crash: checkpoints {:?} wal {:?} known {:?}",
                manifest.checkpoints, wal.streams, self.stream_records
            );
        }

        // The reopened engine is needed to decode survivors (ids to names) and for the raw
        // entries of commits only SSTs still hold.
        let shards = self.next_shards();
        self.reopens += 1;
        let cfg = self.cfg.clone();
        let store = match Store::open_cfg(&self.vfs, shards, &cfg) {
            Ok(s) => s,
            Err(e) => return fail(FailureClass::Protocol, format!("recovery failed: {e}")),
        };
        self.store = Some(store);
        // The crash killed every handle, the liveness probe's included.
        self.probe = match self
            .vfs
            .open(Path::new("/db/probe"), OpenOptions::read_write_create())
        {
            Ok(p) => p,
            Err(e) => return fail(FailureClass::Protocol, format!("reopening the probe: {e}")),
        };
        // Faults stay off until the recovered state has been checked: the checks read the
        // files directly and must not fail on an injected error.

        // Unknown seqnos in the WAL must match unacknowledged commits, by their mutations. A
        // cross-shard commit may have lost some shares, so a seqno's surviving records may
        // hold a subset of its commit's mutations, and a share can fit several commits (a row
        // delete covers a family delete of the same row). Each commit owns at most one seqno,
        // so seqnos and commits are matched as a whole (a maximum matching, exact fits tried
        // first), never greedily in seqno order.
        let mut unacked = std::mem::take(&mut self.unacked);
        let unacked_keys: Vec<BTreeSet<MutationKey>> = unacked
            .iter()
            .map(|c| Self::mutation_keys(&c.ops))
            .collect();
        let mut unknown: Vec<(Seqno, Timestamp, BTreeSet<MutationKey>, Vec<usize>)> = Vec::new();
        for (seqno, sv) in &wal.batches {
            if self.history.contains_key(seqno) || self.settled_seqnos.contains(seqno) {
                continue;
            }
            let keys = self.survivor_keys(sv);
            if keys.is_empty() {
                // A read-only share (a transaction's reads on this shard): nothing to
                // recover from it, and its commit is known through its other records.
                continue;
            }
            // PREPAREs of a cross-shard attempt whose coordinator never appended a COMMIT
            // record to any stream (it aborted: a participant refused, or it never got that
            // far) can never be recovered (D83). A commit refused by a shard moving a tablet
            // is retried under a new seqno, so such an attempt must not be taken for an
            // unacknowledged commit's seqno. Only tablet changes refuse and retry this way.
            let prepares_only = wal
                .streams
                .values()
                .flatten()
                .all(|(s, kind)| *s != *seqno || *kind == RecKind::Prepare);
            let committed_anywhere = self
                .stream_records
                .values()
                .flatten()
                .any(|(s, kind)| *s == *seqno && *kind == RecKind::Commit);
            if self.cfg.tablet_changes && prepares_only && !committed_anywhere {
                self.settled_seqnos.insert(*seqno);
                self.stats.refused_attempts += 1;
                self.trace
                    .push(format!("  seqno {seqno}: PREPAREs of a refused attempt"));
                continue;
            }
            let exact = (0..unacked.len()).filter(|&i| unacked_keys[i] == keys);
            let within = (0..unacked.len())
                .filter(|&i| unacked_keys[i] != keys && keys.is_subset(&unacked_keys[i]));
            let fits = exact.chain(within).collect();
            unknown.push((*seqno, sv.commit_ts, keys, fits));
        }
        // A seqno an aborted attempt also explains (its PREPARE) is matched last: a refused
        // attempt's records can share keys with an unacknowledged commit (a row delete's
        // family markers carry no timestamp), and taking that commit for them would leave
        // the commit's own records unexplained (#308). Augmenting paths never unmatch a
        // seqno, so the others keep their commits.
        let explained = |keys: &BTreeSet<MutationKey>| {
            self.aborted
                .iter()
                .any(|c| keys.is_subset(&Self::mutation_keys(&c.ops)))
        };
        let order: Vec<usize> = {
            let (optional, required): (Vec<usize>, Vec<usize>) =
                (0..unknown.len()).partition(|&i| explained(&unknown[i].2));
            required.into_iter().chain(optional).collect()
        };
        let fits: Vec<&[usize]> = order.iter().map(|&i| unknown[i].3.as_slice()).collect();
        let mut owners = vec![None; unknown.len()];
        for (&i, owner) in order.iter().zip(match_seqnos(&fits, unacked.len())) {
            owners[i] = owner;
        }
        let mut matched: BTreeMap<Seqno, Committed> = BTreeMap::new();
        let mut taken = vec![false; unacked.len()];
        for ((seqno, commit_ts, keys, _), owner) in unknown.iter().zip(owners) {
            if let Some(i) = owner {
                let mut c = unacked[i].clone();
                c.seqno = Some(*seqno);
                c.commit_ts = Some(*commit_ts);
                matched.insert(*seqno, c);
                taken[i] = true;
                continue;
            }
            // The PREPARE records of a refused attempt (never committed).
            if self
                .aborted
                .iter()
                .any(|c| keys.is_subset(&Self::mutation_keys(&c.ops)))
            {
                self.settled_seqnos.insert(*seqno);
                continue;
            }
            return fail(
                FailureClass::RecoveredFromTheFuture,
                format!(
                    "the WAL holds commit {seqno}, which the client never made: {:?} (unacked: {:?}; history has {:?})",
                    keys.iter()
                        .map(|k| (
                            &k.0,
                            &k.1,
                            k.2,
                            String::from_utf8_lossy(&k.3).into_owned(),
                            k.5
                        ))
                        .collect::<Vec<_>>(),
                    unacked.iter().map(|c| &c.ops).collect::<Vec<_>>(),
                    self.history.keys().collect::<Vec<_>>()
                ),
            );
        }
        let mut taken = taken.into_iter();
        unacked.retain(|_| !taken.next().expect("one flag per commit"));
        // Commits whose records were checkpointed away survive in SSTs: the reopened
        // engine's raw entries name their seqnos.
        let raw = {
            let store = self.store();
            let snap = store.engine.snapshot().map_err(|e| Fail {
                class: FailureClass::Protocol,
                message: format!("snapshot after recovery: {e}"),
            })?;
            store.engine.raw_entries(&snap).map_err(|e| Fail {
                class: FailureClass::Protocol,
                message: format!("raw entries: {e}"),
            })?
        };
        let mut raw_by_seqno: BTreeMap<Seqno, BTreeSet<MutationKey>> = BTreeMap::new();
        {
            let store = self.store();
            let table_names: HashMap<TableId, String> = store
                .tables
                .iter()
                .map(|(n, t)| (t.id, n.clone()))
                .collect();
            for e in &raw {
                let Ok(parts) = decode_key(&e.key) else {
                    continue;
                };
                let mut row = Vec::new();
                parts.row.unescape_into(&mut row);
                let mut qualifier = Vec::new();
                if let Some(q) = parts.qualifier {
                    q.unescape_into(&mut qualifier);
                }
                let payload = match pigeonhole_format::value::decode_value(&e.value) {
                    Ok(v) => value_bytes(v),
                    Err(_) => e.value.to_vec(),
                };
                raw_by_seqno.entry(parts.seqno).or_default().insert((
                    table_names.get(&e.table).cloned().unwrap_or_default(),
                    store
                        .family_names
                        .get(&e.family)
                        .cloned()
                        .unwrap_or_default(),
                    parts.kind as u8,
                    row,
                    qualifier,
                    Some(parts.ts),
                    payload,
                ));
            }
        }
        let mut sst_only: BTreeSet<Seqno> = BTreeSet::new();
        for (seqno, entries) in &raw_by_seqno {
            if self.history.contains_key(seqno) || matched.contains_key(seqno) {
                continue;
            }
            // Every raw entry must come from the unacknowledged commit (compaction may
            // have dropped some of its entries, never added any).
            let candidates: BTreeSet<Timestamp> = entries.iter().filter_map(|k| k.5).collect();
            let found = unacked.iter().position(|c| {
                candidates.iter().any(|t| {
                    let expected = Self::applied_keys(&c.ops, *t);
                    entries.iter().all(|k| expected.contains(k))
                })
            });
            match found {
                Some(i) => {
                    let mut c = unacked.remove(i);
                    let ts = candidates
                        .iter()
                        .find(|t| {
                            let expected = Self::applied_keys(&c.ops, **t);
                            entries.iter().all(|k| expected.contains(k))
                        })
                        .copied()
                        .expect("matched above");
                    c.seqno = Some(*seqno);
                    c.commit_ts = Some(ts);
                    matched.insert(*seqno, c);
                    sst_only.insert(*seqno);
                }
                None => {
                    return fail(
                        FailureClass::RecoveredFromTheFuture,
                        format!(
                            "the SSTs hold entries of seqno {seqno}, which no commit of the client explains: {entries:?}"
                        ),
                    );
                }
            }
        }
        self.stats.sst_only += sst_only.len();

        // Every commit the client knows about, in seqno order, with its streams.
        let mut all: Vec<Committed> = self.history.values().cloned().collect();
        all.extend(matched.into_values());
        all.sort_by_key(|c| c.seqno);
        // The streams a commit's records went to, from the engine's append order (the
        // routing guess stands for a commit that never reached a stream).
        for c in &mut all {
            if let Some(seqno) = c.seqno
                && let Some(streams) = self.streams_from_records(seqno, &c.streams)
            {
                c.streams = streams;
            }
        }
        let flushed_table_ids: HashMap<String, TableId> = self
            .store()
            .tables
            .iter()
            .map(|(n, t)| (n.clone(), t.id))
            .collect();
        // The tablet holding a row, by the manifest's row ranges (tablets split and merge).
        let tablet_of = |table: TableId, row: &[u8]| -> Option<pigeonhole_format::TabletId> {
            manifest
                .tablet_ranges
                .iter()
                .find(|(_, t, start, end)| {
                    *t == table
                        && start.as_slice() <= row
                        && end.as_ref().is_none_or(|e| row < e.as_slice())
                })
                .map(|(id, ..)| *id)
        };
        let fully_flushed = |c: &Committed| -> bool {
            let Some(seqno) = c.seqno else { return false };
            c.ops.iter().all(|op| {
                let table = op_table(op);
                let fams: Vec<String> = match op {
                    ModelOp::DeleteRow { .. } => families().into_iter().map(|f| f.name).collect(),
                    ModelOp::Put { family, .. }
                    | ModelOp::Incr { family, .. }
                    | ModelOp::DeleteCell { family, .. }
                    | ModelOp::DeleteColumn { family, .. }
                    | ModelOp::DeleteFamily { family, .. } => vec![family.clone()],
                };
                fams.iter().all(|f| {
                    let Some(tid) = flushed_table_ids.get(table) else {
                        return false;
                    };
                    let Some(tablet) = tablet_of(*tid, op_row(op)) else {
                        return false;
                    };
                    let fid = self.store().family_ids[&(table.to_owned(), f.clone())];
                    manifest.flushed.get(&(tablet, fid)).copied().unwrap_or(0) >= seqno
                })
            })
        };
        let flushed: Vec<bool> = all.iter().map(fully_flushed).collect();
        let position = |stream: u32, seqno: Seqno, kind: RecKind| -> Option<usize> {
            self.stream_records
                .get(&stream)
                .and_then(|l| l.iter().position(|r| *r == (seqno, kind)))
        };
        // Per stream, the surviving prefix: everything the WAL still holds, and everything
        // below it or beyond it that SSTs hold.
        let mut survivors: BTreeMap<u32, usize> = BTreeMap::new();
        for (stream, list) in &self.stream_records {
            let observed = wal
                .streams
                .get(stream)
                .and_then(|o| o.last())
                .map_or(0, |last| {
                    list.iter().position(|r| r == last).map_or(0, |i| i + 1)
                });
            let mut cut = observed;
            for (c, is_flushed) in all.iter().zip(&flushed) {
                if !*is_flushed {
                    continue;
                }
                for (s, kind) in records_of(&c.streams) {
                    if s == *stream
                        && let Some(i) =
                            position(s, c.seqno.expect("flushed commits have a seqno"), kind)
                    {
                        cut = cut.max(i + 1);
                    }
                }
            }
            survivors.insert(*stream, cut);
        }
        // Every record below a stream's cut is recoverable (WAL or SSTs): a hole would
        // mean the engine lost a record in the middle of a stream.
        let decided = |seqno: Seqno| {
            self.stream_records
                .values()
                .flatten()
                .any(|r| *r == (seqno, RecKind::Commit))
        };
        let mut undecided_passed = 0;
        for (stream, cut) in &survivors {
            let list = &self.stream_records[stream];
            let observed_from = wal
                .streams
                .get(stream)
                .and_then(|o| o.first())
                .and_then(|f| list.iter().position(|r| r == f))
                .unwrap_or(list.len());
            for (i, (seqno, _)) in list.iter().enumerate().take(*cut) {
                if i >= observed_from {
                    continue;
                }
                let ok = all
                    .iter()
                    .zip(&flushed)
                    .any(|(c, f)| *f && c.seqno == Some(*seqno));
                if !ok {
                    let commit = all.iter().find(|c| c.seqno == Some(*seqno));
                    let Some(c) = commit else {
                        // A refused attempt's PREPARE (no commit to recover) or an
                        // unacknowledged commit that left nothing behind.
                        continue;
                    };
                    // A cross-shard commit no coordinator appended a COMMIT for aborted or
                    // was never decided: its PREPAREs pass the checkpoint at once (D116),
                    // and it is lost as a whole (D83, D114; the record-level rule below
                    // checks that). Its shares on other streams may survive and make it
                    // known here (issue #181). An acknowledged one always has a COMMIT.
                    if matches!(c.streams, CommitStreams::Cross { .. }) && !decided(*seqno) {
                        if c.acked {
                            return fail(
                                FailureClass::Protocol,
                                format!(
                                    "cross-shard commit {seqno} was acknowledged, but no stream holds its COMMIT"
                                ),
                            );
                        }
                        undecided_passed += 1;
                        continue;
                    }
                    return fail(
                        FailureClass::LostAckedCommit,
                        format!(
                            "stream {stream} record {i} (seqno {seqno}) is below the checkpoint but not in SSTs, while record {} survived (commit: {:?}; flushed: {:?}; tablets: {:?}; wal: {:?})",
                            cut - 1,
                            commit.map(|c| (&c.ops, &c.streams, c.acked, c.durability)),
                            manifest.flushed,
                            manifest.tablets,
                            wal.streams.get(stream)
                        ),
                    );
                }
            }
        }
        self.stats.undecided_prepares_passed += undecided_passed;
        // The sim's record-level rule (D83, D84, D114) over every stream's records in append
        // order: a single-shard commit survives iff its record is in its stream's surviving
        // prefix, a cross-shard commit iff its COMMIT is and every participant the COMMIT
        // names still holds its PREPARE. Commits whose records were checkpointed away survive
        // through SSTs.
        let (records, counts) = self.sim_records(&all, &survivors)?;
        let mut recovered: BTreeSet<usize> = recovered_from_records(&records, &counts)
            .into_iter()
            .collect();
        recovered.extend((0..all.len()).filter(|i| flushed[*i]));
        let recovered: Vec<usize> = recovered.into_iter().collect();
        for &i in &recovered {
            if let Some(seqno) = all[i].seqno
                && !raw_by_seqno.contains_key(&seqno)
                && !wal.batches.contains_key(&seqno)
                && !all[i].ops.is_empty()
                && !flushed[i]
            {
                // Recovered by the rule but nowhere in the files: only possible for a commit
                // whose entries a compaction dropped entirely, which cannot happen to one the
                // WAL still had to replay.
                return fail(
                    FailureClass::RecoveredStateMismatch,
                    format!(
                        "commit {seqno} should have been recovered but neither the WAL nor the SSTs hold it"
                    ),
                );
            }
        }
        // The durability promise (D42, D84), through the sim's checker.
        let stream_commits: Vec<StreamCommit> = all
            .iter()
            .map(|c| StreamCommit {
                ops: c.ops.clone(),
                commit_ts: c.commit_ts.unwrap_or(0),
                durability: if c.acked {
                    c.durability
                } else {
                    Durability::None
                },
                streams: c.streams.clone(),
            })
            .collect();
        if let Err(i) = check_acknowledged_survive(&stream_commits, &recovered, kind) {
            let c = &all[i];
            return fail(
                FailureClass::LostAckedCommit,
                format!(
                    "commit {:?} ({:?}, streams {:?}) was acknowledged but did not survive",
                    c.seqno, c.durability, c.streams
                ),
            );
        }
        // A commit lost here stays lost, but its appended records (a PREPARE without its
        // COMMIT) remain valid WAL records until a checkpoint passes them.
        for (i, c) in all.iter().enumerate() {
            if !recovered.contains(&i)
                && let Some(seqno) = c.seqno
            {
                // Only records the WAL still holds come back; a lost commit with none left
                // frees its seqno (the streams go on from the last kept record).
                if wal.batches.contains_key(&seqno) {
                    self.settled_seqnos.insert(seqno);
                }
                self.aborted.push(c.clone());
            }
        }
        // Rebuild the model from the survivors, in seqno order, then re-apply every durable
        // purge.
        let kept: Vec<Committed> = recovered.iter().map(|i| all[*i].clone()).collect();
        let model = Model::from_commits(
            |m| {
                for t in TABLES {
                    m.create_table(t, families());
                }
            },
            &kept
                .iter()
                .map(|c| StreamCommit {
                    ops: c.ops.clone(),
                    commit_ts: c.commit_ts.expect("kept commits have a timestamp"),
                    durability: Durability::Sync,
                    streams: c.streams.clone(),
                })
                .collect::<Vec<_>>(),
        );
        self.history.clear();
        self.engine_seqnos.clear();
        for mut c in kept {
            let seqno = c.seqno.expect("kept commits have a seqno");
            self.engine_seqnos.push(seqno);
            c.acked = true;
            self.history.insert(seqno, c);
        }
        self.model = model;
        // Each stream goes on from the last record the WAL kept: what came after it is
        // gone (the next crash's records continue from there, possibly reusing seqnos).
        // Streams beyond the new shard count were flushed and removed at open (D20).
        for (stream, list) in &mut self.stream_records {
            let keep = if *stream as usize >= shards {
                0
            } else {
                wal.streams
                    .get(stream)
                    .and_then(|o| o.last())
                    .and_then(|last| list.iter().position(|r| r == last))
                    .map_or(0, |i| i + 1)
            };
            list.truncate(keep);
            if let Some(epochs) = self.stream_epochs.get_mut(stream) {
                epochs.truncate(keep);
            }
        }
        let purges: Vec<PurgeEvent> = self
            .purges
            .iter()
            .filter(|p| p.manifest_version <= manifest.version)
            .cloned()
            .collect();
        for p in &purges {
            self.compaction_floor = self.compaction_floor.max(p.max_seqno);
            if p.purges() {
                self.apply_purge(p);
            }
        }
        self.purges = purges;
        // Routing under the new shard count (for the shards of later commits).
        let keys: Vec<Seqno> = self.history.keys().copied().collect();
        for k in keys {
            let ops = self.history[&k].ops.clone();
            let shards = self.shards_of(&ops);
            self.history.get_mut(&k).unwrap().shards = shards;
        }
        self.drain_compactions()?;

        let snap = match self.store().engine.snapshot() {
            Ok(s) => s,
            Err(e) => {
                return fail(
                    FailureClass::Protocol,
                    format!("snapshot after recovery: {e}"),
                );
            }
        };
        let recovered_seqno = snap.seqno();
        let last_kept = self.engine_seqnos.last().copied().unwrap_or(0);
        self.trace.push(format!(
            "RECOVERED with {shards} shards: engine seqno {recovered_seqno}, {} commits kept (last {last_kept}), manifest {} ({} from SSTs only)",
            self.engine_seqnos.len(),
            manifest.version,
            sst_only.len()
        ));
        if recovered_seqno < last_kept {
            return fail(
                FailureClass::RecoveredStateMismatch,
                format!(
                    "visible seqno {recovered_seqno} is below the last surviving commit {last_kept}"
                ),
            );
        }
        self.compare_dump(&snap, FailureClass::RecoveredStateMismatch)?;
        if recovered_seqno > self.compaction_floor {
            let s = self.compaction_floor + 1 + rng.below(recovered_seqno - self.compaction_floor);
            self.compare_dump(&snap.at_seqno(s), FailureClass::RecoveredStateMismatch)?;
        }
        self.vfs.set_faults(self.cfg.faults.clone());
        self.faults_active = true;
        Ok(())
    }

    /// Every stream's records in append order as the sim's `StreamRecord`s, naming commits by
    /// their index in `all`, and how many of them lie in the stream's surviving prefix.
    ///
    /// Two kinds of record name no commit of `all` and are left out, each counted in
    /// `Stats`. A record whose seqno no commit holds (`unowned_records`) is a refused
    /// attempt's PREPARE, a lost cross-shard commit's PREPARE or COMMIT, or an
    /// unacknowledged commit's record lost past the cut; a single-shard record inside the
    /// surviving prefix would be a commit the client never made, and fails. A repeat of a
    /// `(seqno, kind)` on one stream (`reused_records`) is legal only across a crash, when
    /// recovery reused the seqno of a commit it lost: the latest copy belongs to the live
    /// commit and the earlier ones to the lost one. Two copies appended between the same
    /// two crashes fail.
    fn sim_records(
        &mut self,
        all: &[Committed],
        survivors: &BTreeMap<u32, usize>,
    ) -> Result<(Vec<Vec<StreamRecord>>, Vec<usize>), Fail> {
        let index: HashMap<Seqno, usize> = all
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.seqno.map(|s| (s, i)))
            .collect();
        let streams = self
            .stream_records
            .keys()
            .next_back()
            .map_or(0, |s| *s as usize + 1);
        let mut records = vec![Vec::new(); streams];
        let mut counts = vec![0; streams];
        let (mut unowned, mut reused) = (0, 0);
        for (stream, list) in &self.stream_records {
            let s = *stream as usize;
            // Records only come from `drain_appended`, which keeps the epochs alongside.
            let epochs = self
                .stream_epochs
                .get(stream)
                .map_or(&[][..], Vec::as_slice);
            let cut = survivors.get(stream).copied().unwrap_or(0);
            // The latest copy of each (seqno, kind), and the epochs of all its copies.
            let mut latest: BTreeMap<(Seqno, RecKind), usize> = BTreeMap::new();
            for (pos, r) in list.iter().enumerate() {
                if let Some(prev) = latest.insert(*r, pos)
                    && epochs[prev] == epochs[pos]
                {
                    return fail(
                        FailureClass::Protocol,
                        format!(
                            "stream {stream}: {r:?} appended twice with no crash between (records {prev} and {pos}): {list:?}"
                        ),
                    );
                }
            }
            for (pos, (seqno, kind)) in list.iter().enumerate() {
                let Some(&i) = index.get(seqno) else {
                    if pos < cut && *kind == RecKind::Batch {
                        return fail(
                            FailureClass::RecoveredFromTheFuture,
                            format!(
                                "stream {stream} record {pos} (seqno {seqno}) survived but no commit of the client has that seqno"
                            ),
                        );
                    }
                    unowned += 1;
                    continue;
                };
                if latest[&(*seqno, *kind)] != pos {
                    reused += 1;
                    continue;
                }
                records[s].push(match kind {
                    RecKind::Batch => StreamRecord::Single(i),
                    RecKind::Prepare => StreamRecord::Prepare(i),
                    RecKind::Commit => StreamRecord::Commit {
                        commit: i,
                        participants: match &all[i].streams {
                            CommitStreams::Cross { participants, .. } => participants.clone(),
                            CommitStreams::Single(_) => Vec::new(),
                        },
                    },
                });
                if pos < cut {
                    counts[s] += 1;
                }
            }
        }
        self.stats.unowned_records += unowned;
        self.stats.reused_records += reused;
        Ok((records, counts))
    }

    /// The entries `ops` leave in the store when committed at `commit_ts` (same-commit
    /// collapse, decision D34; a row delete is one marker per family).
    fn applied_keys(ops: &[ModelOp], commit_ts: Timestamp) -> BTreeSet<MutationKey> {
        #[allow(clippy::type_complexity)]
        let mut last: BTreeMap<(String, String, Vec<u8>, Vec<u8>, Timestamp), MutationKey> =
            BTreeMap::new();
        for op in &combine_counter_writes(ops, |_, f| is_sum(f)) {
            let t = op_table(op).to_owned();
            let mut push = |family: String,
                            kind: u8,
                            row: &[u8],
                            qualifier: &[u8],
                            ts: Timestamp,
                            value: Vec<u8>| {
                last.insert(
                    (
                        t.clone(),
                        family.clone(),
                        row.to_vec(),
                        qualifier.to_vec(),
                        ts,
                    ),
                    (
                        t.clone(),
                        family,
                        kind,
                        row.to_vec(),
                        qualifier.to_vec(),
                        Some(ts),
                        value,
                    ),
                );
            };
            match op {
                ModelOp::Put {
                    row,
                    family,
                    qualifier,
                    ts,
                    value,
                    ..
                } => push(
                    family.clone(),
                    1,
                    row,
                    qualifier,
                    ts.unwrap_or(default_ts(family, commit_ts)),
                    value.clone(),
                ),
                ModelOp::Incr {
                    row,
                    family,
                    qualifier,
                    ts,
                    delta,
                    ..
                } => push(
                    family.clone(),
                    2,
                    row,
                    qualifier,
                    ts.unwrap_or(default_ts(family, commit_ts)),
                    delta.to_le_bytes().to_vec(),
                ),
                ModelOp::DeleteCell {
                    row,
                    family,
                    qualifier,
                    ts,
                    ..
                } => push(family.clone(), 3, row, qualifier, *ts, Vec::new()),
                ModelOp::DeleteColumn {
                    row,
                    family,
                    qualifier,
                    ..
                } => push(family.clone(), 4, row, qualifier, commit_ts, Vec::new()),
                ModelOp::DeleteFamily { row, family, .. } => {
                    push(family.clone(), 5, row, &[], commit_ts, Vec::new())
                }
                ModelOp::DeleteRow { row, .. } => {
                    for f in families() {
                        push(f.name, 5, row, &[], commit_ts, Vec::new());
                    }
                }
            }
        }
        last.into_values().collect()
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
    /// commits are always logged). Also records the order of every stream's records.
    fn logged_timestamps(&mut self) -> Result<BTreeMap<Seqno, Timestamp>, Fail> {
        let wal = self.observe_wal()?;
        Ok(logged_timestamps_of(&wal))
    }

    /// The timestamp of logged commit `seqno` whose record a flush already checkpointed away:
    /// from the engine's append record (test hook) when it was appended since the last
    /// crash, else from the entries it left. A compaction may have dropped every entry
    /// (a family delete purged with what it covers, expired TTL cells), so the append
    /// record comes first.
    fn checkpointed_commit_ts(&mut self, seqno: Seqno, ops: &[ModelOp]) -> Result<Timestamp, Fail> {
        self.drain_appended();
        match self.appended_ts.get(&seqno) {
            Some(&(epoch, ts)) if epoch == self.stats.crashes => Ok(ts),
            _ => self.raw_commit_ts(seqno, ops),
        }
    }

    /// The timestamp of commit `seqno` from the entries it left in the store, for a record
    /// a flush already checkpointed away: the timestamp of any entry an op with a default
    /// timestamp produced, and no op with an explicit one could have (0 when every op
    /// carries its own).
    fn raw_commit_ts(&self, seqno: Seqno, ops: &[ModelOp]) -> Result<Timestamp, Fail> {
        if !ops.iter().any(takes_commit_ts) {
            return Ok(0);
        }
        let store = self.store();
        let snap = store.engine.snapshot().map_err(|e| Fail {
            class: FailureClass::Protocol,
            message: format!("snapshot: {e}"),
        })?;
        let raw = store.engine.raw_entries(&snap).map_err(|e| Fail {
            class: FailureClass::Protocol,
            message: format!("raw entries: {e}"),
        })?;
        let table_names: HashMap<TableId, String> = store
            .tables
            .iter()
            .map(|(n, t)| (t.id, n.clone()))
            .collect();
        for e in &raw {
            let Ok(parts) = decode_key(&e.key) else {
                continue;
            };
            if parts.seqno != seqno {
                continue;
            }
            let mut row = Vec::new();
            parts.row.unescape_into(&mut row);
            let mut qualifier = Vec::new();
            if let Some(q) = parts.qualifier {
                q.unescape_into(&mut qualifier);
            }
            let table = table_names.get(&e.table).cloned().unwrap_or_default();
            let family = store
                .family_names
                .get(&e.family)
                .cloned()
                .unwrap_or_default();
            let default_ts = ops.iter().any(|op| {
                op_table(op) == table
                    && op_row(op) == row
                    && match op {
                        ModelOp::Put {
                            family: f,
                            qualifier: q,
                            ..
                        }
                        | ModelOp::Incr {
                            family: f,
                            qualifier: q,
                            ..
                        } => f == &family && q == &qualifier && takes_commit_ts(op),
                        ModelOp::DeleteColumn {
                            family: f,
                            qualifier: q,
                            ..
                        } => f == &family && q == &qualifier,
                        ModelOp::DeleteFamily { family: f, .. } => {
                            f == &family && qualifier.is_empty()
                        }
                        ModelOp::DeleteRow { .. } => qualifier.is_empty(),
                        ModelOp::DeleteCell { .. } => false,
                    }
            });
            // An op on the same column with an explicit timestamp equal to this entry's may
            // have written it instead (#163: a default and an explicit put on one column).
            // Such an entry says nothing about the commit timestamp: look for another one.
            let explicit_here = ops.iter().any(|op| {
                op_table(op) == table
                    && op_row(op) == row
                    && match op {
                        ModelOp::Put {
                            family: f,
                            qualifier: q,
                            ts: Some(ts),
                            ..
                        }
                        | ModelOp::Incr {
                            family: f,
                            qualifier: q,
                            ts: Some(ts),
                            ..
                        }
                        | ModelOp::DeleteCell {
                            family: f,
                            qualifier: q,
                            ts,
                            ..
                        } => f == &family && q == &qualifier && *ts == parts.ts,
                        ModelOp::Put {
                            family: f,
                            qualifier: q,
                            ts: None,
                            ..
                        }
                        | ModelOp::Incr {
                            family: f,
                            qualifier: q,
                            ts: None,
                            ..
                        } => f == &family && q == &qualifier && is_sum(f) && parts.ts == COUNTER_TS,
                        _ => false,
                    }
            });
            if default_ts && !explicit_here {
                return Ok(parts.ts);
            }
        }
        fail(
            FailureClass::Protocol,
            format!(
                "commit {seqno} is checkpointed and none of its entries is left to read its timestamp from"
            ),
        )
    }

    /// Moves the clock past every shard's default-timestamp floor.
    fn clock_past_floors(&mut self) {
        let Some(store) = &self.store else {
            return;
        };
        if !self.cfg.tablet_changes {
            return;
        }
        let floor = store.engine.max_ts_floor();
        let now = self.now();
        if floor >= now {
            self.vfs.advance(1_000 * (floor - now + 1));
        }
    }

    /// Adds the open engine's split, merge and move counts to the stats.
    fn count_tablet_changes(&mut self) {
        if let Some(store) = &self.store {
            let (s, m, v) = tablet_changes(&store.engine);
            self.stats.engine_tablet_changes.0 += s;
            self.stats.engine_tablet_changes.1 += m;
            self.stats.engine_tablet_changes.2 += v;
        }
    }

    /// Requests a random split, merge or move (it runs while the workload goes on).
    fn request_tablet_change(&mut self, rng: &mut Rng) -> Result<(), Fail> {
        let table = TABLES[rng.below(TABLES.len() as u64) as usize];
        let row = format!("row{:06}", rng.below(self.cfg.spec.rows.max(1))).into_bytes();
        let shards = self.store().shards.len().max(1) as u64;
        let kind = rng.below(3);
        let to = rng.below(shards) as u16;
        let store = self.store();
        let tid = store.tables[table].id;
        let (what, pending) = match kind {
            0 => ("split", store.engine.split_tablet_pending(tid, &row)),
            1 => ("merge", store.engine.merge_tablets_pending(tid, &row)),
            _ => ("move", store.engine.move_tablet_pending(tid, &row, to)),
        };
        let label = format!("tablet {what} {table} at {} (to {to})", text(&row));
        self.trace.push(label.clone());
        match pending {
            Ok(p) => self.tablet_pending = Some((p, label)),
            Err(e) => {
                if !self.tablet_change_refused(&e) {
                    return fail(FailureClass::Protocol, format!("{label}: {e}"));
                }
            }
        }
        Ok(())
    }

    /// Whether a tablet change's error is an acceptable refusal (a stale or invalid request,
    /// a crash or an injected I/O error, which the commits around it notice).
    fn tablet_change_refused(&mut self, e: &Error) -> bool {
        let ok = matches!(
            e,
            Error::InvalidArgument(_)
                | Error::TableNotFound(_)
                | Error::Unsupported(_)
                | Error::Closed
                | Error::Io(_)
        );
        if ok {
            self.stats.tablet_refused += 1;
            self.trace.push(format!("  -> tablet change refused: {e}"));
        }
        ok
    }

    /// Polls the running tablet change, if any.
    fn poll_tablet_change(&mut self) -> Result<(), Fail> {
        let Some((p, _)) = self.tablet_pending.as_mut() else {
            return Ok(());
        };
        let mut cx = Context::from_waker(Waker::noop());
        let Poll::Ready(r) = Pin::new(p).poll(&mut cx) else {
            return Ok(());
        };
        let (_, label) = self.tablet_pending.take().expect("checked");
        if !self.alive() {
            // Crashed meanwhile: the next commit notices and recovers.
            return Ok(());
        }
        match r {
            Ok(()) => {
                self.stats.tablet_changes += 1;
                self.trace.push(format!("  {label}: done"));
            }
            Err(e) => {
                if !self.tablet_change_refused(&e) {
                    return fail(FailureClass::Protocol, format!("{label}: {e}"));
                }
            }
        }
        let checked = self
            .drain_compactions()
            .and_then(|()| self.check_held_snapshots());
        match checked {
            // An armed power loss fired meanwhile on I/O no client waits for (a commit-time
            // separation's manifest commit runs on the committing thread, #230): the next
            // commit notices and recovers.
            Err(_) if !self.alive() => Ok(()),
            r => r,
        }
    }

    /// Settles every in-flight commit that resolved. Returns whether the client may go on
    /// (nothing left in flight).
    fn poll_in_flight(&mut self, rng: &mut Rng) -> Result<bool, Fail> {
        self.poll_tablet_change()?;
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
                Pending::Thread(rx) => recv_within(rx, Duration::from_millis(1)),
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
                            self.aborted.push(inf.commit);
                        }
                    }
                }
                Outcome::Commit(Err(Error::Busy)) | Outcome::Cas(Err(Error::Busy)) => {
                    self.stats.busy += 1;
                    self.trace.push("  -> busy (arena full)".into());
                    self.aborted.push(inf.commit);
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
                    self.aborted.push(inf.commit);
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
                self.faults_active = true;
                self.faults_active = true;
                self.faults_active = true;
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
                let now = c.commit_ts.expect("alone commits know their timestamp");
                if self.cfg.tablet_changes {
                    // A commit a moving tablet refused (`Moved`) or parked runs again later,
                    // above every floor its participants reached meanwhile (a retry keeps
                    // its first timestamp only when strictly above them): read it from the
                    // entries, which must never be older than the clock it ran at. Entries a
                    // compaction already dropped leave the clock, as without tablet changes.
                    match self.raw_commit_ts(seqno, &c.ops).unwrap_or(0) {
                        0 => now,
                        ts if ts >= now => ts,
                        ts => {
                            return fail(
                                FailureClass::Protocol,
                                format!("commit {seqno} at ts {ts}, below the clock {now}"),
                            );
                        }
                    }
                } else {
                    now
                }
            } else {
                if logged.is_none() {
                    logged = Some(self.logged_timestamps()?);
                }
                match logged.as_ref().and_then(|m| m.get(&seqno)) {
                    Some(ts) => *ts,
                    // Flushed and checkpointed before the client looked: the engine's
                    // append record says, else the entries.
                    None => self.checkpointed_commit_ts(seqno, &c.ops)?,
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
            // Compactions whose inputs ended below this commit come before it.
            self.drain_compactions_upto(seqno - 1)?;
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
            self.drain_appended();
            self.check_fresh_seqno(seqno)?;
            self.history.insert(seqno, c);
        }
        let drained = self.drain_compactions();
        if self.recover_if_fired(drained, rng)? {
            return Ok(true);
        }
        // Read-your-writes: everything acknowledged is visible now.
        let snap = match self.store().engine.snapshot() {
            Ok(s) => s,
            Err(e) if !self.alive() => {
                self.background_crash(e, rng)?;
                return Ok(true);
            }
            Err(e) => return fail(FailureClass::Protocol, e.to_string()),
        };
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
            // A commit refused by a shard moving a tablet is retried, sometimes with a new
            // default timestamp: past every floor as well.
            self.clock_past_floors();
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
            self.faults_active = true;
            true
        } else {
            false
        }
    }

    fn prepare_ops(&self, ops: &mut [ModelOp]) {
        for o in ops.iter_mut() {
            place(o);
            shift_ts(o, self.base);
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
            streams: streams_of(&shards, &[]),
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
            // A crash (possibly fired earlier on a shard's background I/O, which then fails
            // the submit with whatever error the dead store reports first, issue #62).
            Err(e) if is_crashed(&e) || !self.alive() => {
                let mut c = commit;
                c.acked = false;
                self.unacked.push(c);
                self.crash_and_recover(CrashKind::Power, true, rng)?;
                Ok(false)
            }
            Err(Error::Io(e)) if self.cfg.faults.io_error_ppm > 0 => {
                // Refused outright (a poisoned pager, D58): nothing was logged.
                self.stats.io_errors += 1;
                self.trace
                    .push(format!("  -> refused ({e}); reopen pending"));
                self.need_reopen = true;
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
                streams: streams_of(&shards, &[]),
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
                (Err(e), _) if !self.alive() => {
                    // An armed power loss fired on background I/O (D127).
                    drop(txn);
                    return self.background_crash(e, rng);
                }
                (Err(Error::Io(e)), _) if self.cfg.faults.io_error_ppm > 0 && self.faults_on() => {
                    // An injected read failure (issue #99): nothing was submitted, so the
                    // transaction is abandoned; a poisoned store is reopened.
                    drop(txn);
                    self.stats.io_errors += 1;
                    self.trace.push(format!("  -> txn read I/O error ({e})"));
                    self.need_reopen = true;
                    return Ok(());
                }
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
            let value = if is_i64(&f) {
                77i64.to_le_bytes().to_vec()
            } else {
                b"interferer".to_vec()
            };
            // A counter family with a TTL refuses its fixed timestamp: a bucket instead.
            let ts = (is_sum(&f) && f.contains("ttl")).then_some(now);
            let op = ModelOp::Put {
                table: t,
                row: r,
                family: f,
                qualifier: b"q0".to_vec(),
                ts,
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
        // Every shard owning a read row is a participant with an empty PREPARE (D91).
        let read_shards: Vec<u16> = {
            let store = self.store();
            let view = store.engine.snapshot().expect("snapshot").view().clone();
            reads
                .iter()
                .filter_map(|(t, r, _)| {
                    view.tablets()
                        .route(store.tables[t].id, r)
                        .map(|(_, s)| s.0)
                })
                .collect()
        };
        self.in_flight.push(InFlight {
            pending: Pending::Thread(rx),
            commit: Committed {
                seqno: None,
                ops,
                commit_ts: Some(now),
                durability,
                streams: streams_of(&shards, &read_shards),
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
            streams: streams_of(&shards, &[]),
            shards: shards.clone(),
            acked: false,
            kind: Kind::Plain,
        };
        let mut pc = match self.store().engine.submit(batch, Some(durability)) {
            Ok(pc) => pc,
            Err(e) if is_crashed(&e) || !self.alive() => {
                self.unacked.push(unacked);
                self.crash_and_recover(CrashKind::Power, true, rng)?;
                return Ok(false);
            }
            Err(Error::Io(e)) if self.cfg.faults.io_error_ppm > 0 => {
                self.stats.io_errors += 1;
                self.trace
                    .push(format!("  -> refused ({e}); reopen pending"));
                self.need_reopen = true;
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
            None => self.checkpointed_commit_ts(info.seqno, &ops)?,
        };
        self.trace.push(format!(
            "interferer {durability:?} ts={ts} seqno={} [{}]",
            info.seqno,
            ops.iter().map(show).collect::<Vec<_>>().join("; ")
        ));
        self.drain_compactions_upto(info.seqno - 1)?;
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
        let c = Committed {
            seqno: Some(info.seqno),
            ops,
            commit_ts: Some(ts),
            durability,
            streams: streams_of(&shards, &[]),
            shards,
            acked: true,
            kind: Kind::Plain,
        };
        self.drain_appended();
        self.check_fresh_seqno(info.seqno)?;
        self.history.insert(info.seqno, c);
        Ok(true)
    }

    /// An acknowledged commit's seqno must be new: never one a recovery settled as not
    /// committed (its records may still be in the WAL, so a reuse would make them
    /// ambiguous).
    fn check_fresh_seqno(&self, seqno: Seqno) -> Result<(), Fail> {
        if self.settled_seqnos.contains(&seqno) {
            return fail(
                FailureClass::Protocol,
                format!(
                    "commit acknowledged under seqno {seqno}, which a recovery settled as never committed"
                ),
            );
        }
        Ok(())
    }

    fn step(&mut self, op: Op, rng: &mut Rng) -> Result<(), Fail> {
        if self.trace_on() {
            eprintln!("op #{} {op:?}", self.op_index);
        }
        self.vfs.advance(1_000);
        if self.cfg.tablet_changes && !self.alive() {
            // An armed crash fired in the background (a flush, a compaction, or a tablet
            // change running while the workload goes on): recover before going on, whatever
            // this step would have done (a commit would see a poisoned pager instead).
            self.background_crash("found before the step", rng)?;
            if self.store.is_none() {
                return Ok(());
            }
        }
        // No draw at all without tablet changes, so every other seed runs as before.
        if self.cfg.tablet_changes
            && self.tablet_pending.is_none()
            && rng.chance(self.cfg.tablet_ops_ppm)
        {
            self.request_tablet_change(rng)?;
        }
        if rng.chance(self.cfg.flush_ppm) {
            self.maintenance(false, rng)?;
            if self.store.is_none() {
                return Ok(());
            }
        } else if rng.chance(self.cfg.compact_ppm) {
            self.maintenance(true, rng)?;
            if self.store.is_none() {
                return Ok(());
            }
        }
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
                let snap = match self.pick_snapshot(rng) {
                    Ok(s) => s,
                    Err(f) if !self.alive() => return self.background_crash(f.message, rng),
                    Err(f) => return Err(f),
                };
                let ms = self.model_seqno(snap.seqno());
                self.trace.push(format!(
                    "get {}/{family}:{} @engine {} (model {ms})",
                    text(&row),
                    text(&qualifier),
                    snap.seqno()
                ));
                let got = self.store().get(&snap, &row, &family, &qualifier);
                let rebuilt = self.model_at(&snap);
                let want = rebuilt.as_ref().unwrap_or(&self.model).try_get(
                    table_of(&row),
                    &row,
                    &family,
                    &qualifier,
                    ms,
                    now,
                );
                match (got, want) {
                    (Ok(g), Ok(w)) if g == w => {}
                    (Err(Error::Merge(_)), Err(pigeonhole_sim::ModelError::MergeFailed(_))) => {}
                    (Err(e), _) if !self.alive() => return self.background_crash(e, rng),
                    (Err(Error::Io(e)), _) if self.cfg.faults.io_error_ppm > 0 => {
                        // An injected read failure: the read reports it, nothing else.
                        self.stats.io_errors += 1;
                        self.trace.push(format!("  -> read I/O error ({e})"));
                    }
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
                let snap = match self.pick_snapshot(rng) {
                    Ok(s) => s,
                    Err(f) if !self.alive() => return self.background_crash(f.message, rng),
                    Err(f) => return Err(f),
                };
                let ms = self.model_seqno(snap.seqno());
                self.trace.push(format!(
                    "scan [{}, {}) @engine {} (model {ms})",
                    text(&start),
                    text(&end),
                    snap.seqno()
                ));
                let rebuilt = self.model_at(&snap);
                let model = rebuilt.as_ref().unwrap_or(&self.model);
                for t in TABLES {
                    let got = self.store().scan(
                        &snap,
                        t,
                        Bound::Included(&start),
                        Bound::Excluded(&end),
                        1,
                    );
                    let want = model.try_scan(
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
                        (Err(e), _) if !self.alive() => return self.background_crash(e, rng),
                        (Err(Error::Io(e)), _) if self.cfg.faults.io_error_ppm > 0 => {
                            self.stats.io_errors += 1;
                            self.trace.push(format!("  -> read I/O error ({e})"));
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
                    let dumped = self.compare_dump(&snap, FailureClass::LiveReadMismatch);
                    if self.recover_if_fired(dumped, rng)? {
                        return Ok(());
                    }
                }
            }
            Op::Snapshot => {
                let snap = match self.store().engine.snapshot() {
                    Ok(s) => s,
                    Err(e) if !self.alive() => return self.background_crash(e, rng),
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

/// The helper thread's result if it arrives within `limit` of real time. Unlike
/// `recv_timeout`, which sleeps on the OS timer (about 15.6 ms per wait on Windows, longer
/// than asked on macOS), it yields until `limit` passes. The harness polls this between
/// simulation steps, so each poll costs at most `limit` on every OS (#91).
fn recv_within<T>(rx: &mpsc::Receiver<T>, limit: Duration) -> Option<T> {
    let end = std::time::Instant::now() + limit;
    loop {
        match rx.try_recv() {
            Ok(v) => return Some(v),
            Err(mpsc::TryRecvError::Disconnected) => return None,
            Err(mpsc::TryRecvError::Empty) if std::time::Instant::now() >= end => return None,
            Err(mpsc::TryRecvError::Empty) => std::thread::yield_now(),
        }
    }
}

/// A maximum matching of WAL seqnos to unacknowledged commits (Kuhn's augmenting paths):
/// `fits[s]` lists, in order of preference, the commits seqno `s` may belong to. Returns
/// each seqno's commit; no commit gets two seqnos.
fn match_seqnos(fits: &[&[usize]], commits: usize) -> Vec<Option<usize>> {
    fn augment(
        s: usize,
        fits: &[&[usize]],
        owner: &mut [Option<usize>],
        seen: &mut [bool],
    ) -> bool {
        for &c in fits[s] {
            if seen[c] {
                continue;
            }
            seen[c] = true;
            if owner[c].is_none_or(|t| augment(t, fits, owner, seen)) {
                owner[c] = Some(s);
                return true;
            }
        }
        false
    }
    let mut owner: Vec<Option<usize>> = vec![None; commits];
    for s in 0..fits.len() {
        augment(s, fits, &mut owner, &mut vec![false; commits]);
    }
    let mut out = vec![None; fits.len()];
    for (c, s) in owner.iter().enumerate() {
        if let Some(s) = s {
            out[*s] = Some(c);
        }
    }
    out
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
    run_recording(seed, cfg, false).0
}

/// Runs one seeded model check, also returning every mutating SimVfs operation it did (issue
/// #61: a seed replays the same I/O, background work included).
pub fn run_traced(seed: u64, cfg: &Config) -> (Result<Stats, Failure>, Vec<SimOp>) {
    run_recording(seed, cfg, true)
}

fn run_recording(seed: u64, cfg: &Config, record: bool) -> (Result<Stats, Failure>, Vec<SimOp>) {
    let sim = Sim::with_faults(seed, cfg.faults.clone());
    let vfs = sim.vfs();
    vfs.set_deferred_io(cfg.deferred_io);
    if record {
        vfs.record_ops();
    }
    let result = run_on(seed, cfg, sim, Arc::clone(&vfs));
    (result, vfs.recorded_ops())
}

fn run_on(seed: u64, cfg: &Config, sim: Sim, vfs: Arc<SimVfs>) -> Result<Stats, Failure> {
    if let Some(n) = cfg.crash_at {
        let mut plan = cfg.faults.clone();
        plan.crash_after_ops = Some(n);
        vfs.set_faults(plan);
    }
    let probe = vfs
        .open(Path::new("/db/probe"), OpenOptions::read_write_create())
        .expect("probe");
    let model = new_model();
    // The first open and table creation run without random I/O errors (a crash point can
    // still hit them); the plan applies from the first operation on.
    if cfg.faults.io_error_ppm > 0 {
        let mut plan = cfg.faults.clone();
        plan.io_error_ppm = 0;
        plan.crash_after_ops = cfg.crash_at;
        vfs.set_faults(plan);
    }
    let opened = Store::open_cfg(&vfs, cfg.shards, cfg);
    if cfg.faults.io_error_ppm > 0 && cfg.crash_at.is_none() {
        vfs.set_faults(cfg.faults.clone());
    }
    let store = match opened {
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
                let store = Store::open_cfg(&vfs, cfg.shards, cfg)
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
    let workload = Workload::new(seed ^ 0x5eed, TABLE, cfg.spec.clone()).take(cfg.ops);
    let world = Rc::new(RefCell::new(World::new(
        seed,
        cfg,
        &vfs,
        probe,
        model,
        store,
        reopens,
        Some(workload),
    )));

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
                    if w.vfs.io_in_flight() > 0 {
                        w.stats.io_in_flight_steps += 1;
                    }
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
    w.stats.mutating_ops = vfs.mutating_ops();
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
    w.stats.reads_at_older_views = w.reads_at_older_views.get();
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

/// Issue #62's scenario, deterministically: `cfg.ops` commits persisted to SSTs by a flush,
/// as many again left in the memtables, then a power loss armed on the next mutating
/// operation and fired by a shard's background flush with nothing in flight. Then `reads`
/// run as client steps until one of them meets the dead store and recovers from the armed
/// crash (`Stats::background_crashes`), which is checked like any other recovery. Use a
/// config without random flushes, compactions, crashes or helper-thread commits.
pub fn read_after_background_crash(
    seed: u64,
    cfg: &Config,
    reads: &[Op],
) -> Result<Stats, Failure> {
    let sim = Sim::with_faults(seed, cfg.faults.clone());
    let vfs = sim.vfs();
    vfs.set_deferred_io(cfg.deferred_io);
    let probe = vfs
        .open(Path::new("/db/probe"), OpenOptions::read_write_create())
        .expect("probe");
    let store = Store::open_cfg(&vfs, cfg.shards, cfg).expect("open");
    let mut w = World::new(seed, cfg, &vfs, probe, new_model(), store, 0, None);
    let mut rng = Rng::new(seed);
    let mut commits = Workload::new(seed ^ 0x5eed, TABLE, cfg.spec.clone())
        .filter(|op| matches!(op, Op::Commit(..)))
        .take(2 * cfg.ops);
    let outcome = (|| -> Result<(), Fail> {
        let mut commit_some = |w: &mut World, rng: &mut Rng, n: usize| -> Result<(), Fail> {
            for op in commits.by_ref().take(n) {
                w.step(op, rng)?;
                while !w.poll_in_flight(rng)? {
                    let now = w.vfs.monotonic_nanos();
                    w.store.as_mut().expect("store open").step_shards(now);
                }
            }
            Ok(())
        };
        commit_some(&mut w, &mut rng, cfg.ops)?;
        w.maintenance(false, &mut rng)?;
        commit_some(&mut w, &mut rng, cfg.ops)?;
        // Arm the power loss and let a shard's background flush fire it.
        let mut plan = cfg.faults.clone();
        plan.crash_after_ops = Some(vfs.mutating_ops() + 1);
        vfs.set_faults(plan);
        w.trace.push("background flush (crash armed)".into());
        let flush = w.store().engine.flush_pending();
        for _ in 0..10_000 {
            if !w.alive() {
                break;
            }
            let now = vfs.monotonic_nanos();
            w.store.as_mut().expect("store open").step_shards(now);
        }
        drop(flush);
        if w.alive() {
            return fail(
                FailureClass::Protocol,
                "the armed crash did not fire on the background flush".into(),
            );
        }
        for op in reads {
            w.step(op.clone(), &mut rng)?;
            if w.stats.background_crashes > 0 {
                break;
            }
        }
        Ok(())
    })();
    let result = match outcome {
        Ok(()) => Ok(w.stats),
        Err(f) => Err(Failure {
            seed,
            class: f.class,
            op_index: w.op_index,
            message: f.message,
            trace: std::mem::take(&mut w.trace),
        }),
    };
    if let Some(mut store) = w.store.take() {
        let _ = store.engine.close();
        for _ in 0..4 {
            store.step_shards(vfs.monotonic_nanos());
        }
    }
    result
}

/// Issue #181's scenario, deterministically: a cross-shard commit `X` whose coordinator
/// logged its PREPARE, then lost its stream to a failed write (another commit's), so it
/// never appended a COMMIT and `X` aborted after the participant's PREPARE landed. The
/// participant's checkpoint passes `X`'s PREPARE at once (D116) and a later commit there
/// survives, while an earlier commit on the coordinator's stream keeps its PREPARE; a
/// process crash makes the checker meet that hole (`Stats::undecided_prepares_passed`). With `acked`, the client is told `X` was
/// acknowledged, which no engine does without a COMMIT: the checker must fail `Protocol`.
/// `cfg` must have the coordinator and participant on different shards, no random faults,
/// crashes, maintenance or helper-thread commits, and a nonzero `io_error_ppm` (it marks
/// the injected error as expected; the error itself is injected here).
pub fn undecided_prepare_passes(seed: u64, cfg: &Config, acked: bool) -> Result<Stats, Failure> {
    let sim = Sim::with_faults(seed, FaultPlan::none());
    let vfs = sim.vfs();
    let probe = vfs
        .open(Path::new("/db/probe"), OpenOptions::read_write_create())
        .expect("probe");
    let store = Store::open_cfg(&vfs, cfg.shards, cfg).expect("open");
    let mut w = World::new(seed, cfg, &vfs, probe, new_model(), store, 0, None);
    let mut rng = Rng::new(seed);
    let outcome = (|| -> Result<(), Fail> {
        // Workload commits, prepared, with the shards they route to.
        let mut commits =
            Workload::new(seed ^ 0x5eed, TABLE, cfg.spec.clone()).filter_map(|op| match op {
                Op::Commit(ops, _) => Some(ops),
                _ => None,
            });
        let mut next = |w: &World, want: &dyn Fn(&[u16]) -> bool| -> Result<_, Fail> {
            for mut ops in commits.by_ref().take(10_000) {
                w.prepare_ops(&mut ops);
                let shards = w.shards_of(&ops);
                if want(&shards) {
                    return Ok((ops, shards));
                }
            }
            fail(FailureClass::Protocol, "no workload commit fits".into())
        };
        // X on two shards; the coordinator is the shard of its first row.
        let (x, shards) = next(&w, &|s| s.len() == 2)?;
        let (coordinator, participant) = (usize::from(shards[0]), usize::from(shards[1]));
        let (pin, _) = next(&w, &|s| s == [coordinator as u16])?;
        let (y, _) = next(&w, &|s| s == [coordinator as u16])?;
        let (z, _) = next(&w, &|s| s == [participant as u16])?;
        let has = |w: &World, stream: usize, kind: RecKind| {
            w.stream_records
                .get(&(stream as u32))
                .is_some_and(|l| l.iter().any(|r| r.1 == kind))
        };
        let step = |w: &mut World, shard: usize| {
            let now = w.vfs.monotonic_nanos();
            w.store.as_mut().expect("store open").shards[shard].run_once(now + 1_000);
            w.drain_appended();
        };
        // A commit on the coordinator's stream that no flush persists: its checkpoint stays
        // below it, so X's PREPARE there survives the crash and makes X known to the checker.
        w.submit_plain(pin, Durability::Sync, true, &mut rng)?;
        while !w.poll_in_flight(&mut rng)? {
            let now = w.vfs.monotonic_nanos();
            w.store.as_mut().expect("store open").step_shards(now);
        }
        w.drain_appended();
        // X's PREPARE on the coordinator, before the participant hears of X.
        w.submit_plain(x, Durability::Sync, true, &mut rng)?;
        for _ in 0..1_000 {
            if has(&w, coordinator, RecKind::Prepare) {
                break;
            }
            step(&mut w, coordinator);
        }
        if !has(&w, coordinator, RecKind::Prepare) || has(&w, participant, RecKind::Prepare) {
            return fail(
                FailureClass::Protocol,
                "the coordinator did not log its PREPARE first".into(),
            );
        }
        // Y's write fails and poisons the coordinator's stream: X can no longer be decided.
        w.submit_plain(y, Durability::Sync, true, &mut rng)?;
        let mut every_error = FaultPlan::none();
        every_error.io_error_ppm = 1_000_000;
        w.vfs.set_faults(every_error);
        w.trace.push("every read and write fails".into());
        let logged = |w: &World| {
            w.stream_records
                .get(&(coordinator as u32))
                .map_or(0, Vec::len)
        };
        let before = logged(&w);
        for _ in 0..1_000 {
            if logged(&w) > before {
                break;
            }
            step(&mut w, coordinator);
        }
        w.vfs.set_faults(FaultPlan::none());
        w.trace.push("faults off".into());
        // The participant logs X's PREPARE; the coordinator cannot log a COMMIT, and X and
        // Y fail. They are settled here, not by `poll_in_flight`, which would reopen the
        // store as soon as nothing is in flight.
        for _ in 0..10_000 {
            if w.in_flight.is_empty() && has(&w, participant, RecKind::Prepare) {
                break;
            }
            let now = w.vfs.monotonic_nanos();
            w.store.as_mut().expect("store open").step_shards(now);
            w.drain_appended();
            let mut i = 0;
            while i < w.in_flight.len() {
                let Pending::Poll(pc) = &mut w.in_flight[i].pending else {
                    unreachable!("plain commits only")
                };
                match poll_commit(pc) {
                    Poll::Pending => i += 1,
                    Poll::Ready(Err(Error::Io(e))) => {
                        w.trace.push(format!("  -> I/O error ({e})"));
                        w.stats.io_errors += 1;
                        let c = w.in_flight.remove(i).commit;
                        w.unacked.push(c);
                    }
                    Poll::Ready(r) => {
                        return fail(
                            FailureClass::Protocol,
                            format!("X or Y did not fail with an I/O error: {r:?}"),
                        );
                    }
                }
            }
        }
        if !w.in_flight.is_empty()
            || !has(&w, participant, RecKind::Prepare)
            || has(&w, coordinator, RecKind::Commit)
        {
            return fail(
                FailureClass::Protocol,
                "X did not abort with a PREPARE on each participant".into(),
            );
        }
        if acked {
            for c in &mut w.unacked {
                c.acked |= matches!(c.streams, CommitStreams::Cross { .. });
            }
        }
        // Z on the participant, past X's PREPARE, which its checkpoint passes at once.
        w.submit_plain(z, Durability::Sync, true, &mut rng)?;
        for _ in 0..10_000 {
            if w.poll_in_flight(&mut rng)? {
                break;
            }
            let now = w.vfs.monotonic_nanos();
            w.store.as_mut().expect("store open").step_shards(now);
        }
        if !w.in_flight.is_empty() {
            return fail(FailureClass::Protocol, "Z never resolved".into());
        }
        w.crash_and_recover(CrashKind::Process, false, &mut rng)
    })();
    let result = match outcome {
        Ok(()) => Ok(w.stats),
        Err(f) => Err(Failure {
            seed,
            class: f.class,
            op_index: w.op_index,
            message: f.message,
            trace: std::mem::take(&mut w.trace),
        }),
    };
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
    vfs.set_deferred_io(cfg.deferred_io);
    let mut store = Store::open_cfg(&vfs, cfg.shards, cfg).expect("open");
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
            store = Store::open_cfg(&vfs, cfg.shards, cfg).expect("reopen");
            // With tablet changes, seqnos are not compared: a commit refused by a shard
            // moving a tablet is retried under a new seqno, so numbering depends on the
            // shard count.
            if cfg.tablet_changes {
                results.push(format!("reopen at {i}"));
            } else {
                results.push(format!(
                    "reopen at {i}: seqno {}",
                    store.engine.snapshot().unwrap().seqno()
                ));
            }
        }
        if let Some(every) = cfg.tablet_every
            && i > 0
            && i % every == 0
        {
            // Every table: split at a row that moves with `i`, move the tablet holding it,
            // or merge it with its right neighbour (refusals are fine and the same for every
            // shard count only in effect, never in the results).
            let k = i / every;
            let row = format!("row{:06}", (k * 7) % cfg.spec.rows.max(1) as usize).into_bytes();
            for t in TABLES {
                let tid = store.tables[t].id;
                let shards = store.shards.len();
                let p = match k % 3 {
                    0 => store.engine.split_tablet_pending(tid, &row),
                    1 => store
                        .engine
                        .move_tablet_pending(tid, &row, (k % shards) as u16),
                    _ => store.engine.merge_tablets_pending(tid, &row),
                };
                if let Ok(p) = p {
                    let _ = store.drive(&vfs, p, || true);
                }
            }
        }
        if let Some(every) = cfg.compact_every
            && i > 0
            && i % every == 0
        {
            let m = store.engine.compact_pending(None).expect("compact");
            store.drive(&vfs, m, || true).expect("compact");
            results.push(format!("compact at {i}"));
        }
        match op {
            Op::Commit(mut ops, durability) => {
                for o in &mut ops {
                    place(o);
                    shift_ts(o, base);
                }
                let durability = cfg.durability.unwrap_or(durability);
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
                if cfg.tablet_changes {
                    results.push(format!("commit {i}"));
                } else {
                    results.push(format!("commit {}", info.seqno));
                }
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
