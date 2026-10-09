//! Benchmark workloads and comparison runners for Pigeonhole.
//!
//! A [`Workload`] generates operations; a [`Runner`] executes them against one store; a
//! [`Report`] summarizes latency percentiles and throughput. Runners for RocksDB, SQLite
//! (EAV) and fjall sit behind the cargo features `rocksdb`, `sqlite` and `fjall`, so their
//! dependencies never enter the default build.
//!
//! Every run records its seed and hardware description; results are labeled non-reference
//! until reference hardware exists (decision D5). The `phdb-bench` binary drives the
//! suite, writes JSON and markdown, and compares two runs; see `docs/bench.md`.
//!
//! ```
//! use pigeonhole_bench::{PigeonholeRunner, WorkloadConfig, WorkloadKind, run};
//!
//! let dir = std::env::temp_dir().join(format!("phdb-bench-doc-{}", std::process::id()));
//! std::fs::create_dir_all(&dir).unwrap();
//! let config = WorkloadConfig::smoke(WorkloadKind::YcsbB);
//! let mut runner = PigeonholeRunner::default().shards(1).memtable_budget(16 << 20);
//! let report = run(&mut runner, &config, &dir).unwrap();
//! assert_eq!(report.store, "pigeonhole");
//! assert!(report.throughput > 0.0 && report.p50 <= report.p99);
//! # std::fs::remove_dir_all(&dir).ok();
//! ```
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]

use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

mod histogram;
mod report;
mod rng;
mod runners;
mod workload;

pub use histogram::Histogram;
pub use report::{
    Comparison, Delta, Environment, LatencyStats, OpTypeStats, ReadSplit, RunDetail, RunRecord,
    SUITE_FORMAT, Scaling, ShardShare, Suite, Tolerance, compare,
};
#[cfg(feature = "fjall")]
pub use runners::fjall::FjallRunner;
pub use runners::pigeonhole::DEFAULT_MEMTABLE_BUDGET;
#[cfg(feature = "rocksdb")]
pub use runners::rocksdb::RocksDbRunner;
#[cfg(feature = "sqlite")]
pub use runners::sqlite::SqliteRunner;
pub use runners::{BLOOM_BITS, MemoryBudget};
pub use workload::{
    EDGE_FAMILY, FAMILIES, GROUP_COMMIT_CELLS, GROUP_COMMIT_ROWS, GROUP_COMMIT_THREADS,
    METRIC_FAMILY, SPARSE_FAMILY, TIME_SERIES_TTL, YCSB_FAMILY,
};

/// The benchmark suite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkloadKind {
    /// YCSB A: 50% reads, 50% updates, Zipfian.
    YcsbA,
    /// YCSB B: 95% reads, 5% updates.
    YcsbB,
    /// YCSB C: read only.
    YcsbC,
    /// YCSB D: read latest, 5% inserts.
    YcsbD,
    /// YCSB E: short scans, 5% inserts.
    YcsbE,
    /// YCSB F: read-modify-write.
    YcsbF,
    /// 1M rows x 0..10K qualifiers, Zipfian.
    SparseWide,
    /// Time series keyed by entity, with TTL.
    TimeSeriesTtl,
    /// Graph adjacency: `edge:<dst>` qualifiers, scan-heavy.
    Adjacency,
    /// Writes skewed across shards (scaling gate).
    SkewedMultiShard,
    /// Durable commits from concurrent clients: each operation commits
    /// [`GROUP_COMMIT_CELLS`] cells of one row, under `GroupSync` on Pigeonhole and
    /// fsync-per-write on the other stores (the Phase 3 commit-latency gate, #53).
    /// `phdb-bench` sets that durability for this workload; a library caller sets it on
    /// the runner ([`PigeonholeRunner::group_sync`]).
    GroupCommit,
}

impl WorkloadKind {
    /// Every workload, in suite order.
    pub const ALL: [WorkloadKind; 11] = [
        WorkloadKind::YcsbA,
        WorkloadKind::YcsbB,
        WorkloadKind::YcsbC,
        WorkloadKind::YcsbD,
        WorkloadKind::YcsbE,
        WorkloadKind::YcsbF,
        WorkloadKind::SparseWide,
        WorkloadKind::TimeSeriesTtl,
        WorkloadKind::Adjacency,
        WorkloadKind::SkewedMultiShard,
        WorkloadKind::GroupCommit,
    ];

    /// The CLI and report name (`ycsb-a`, `sparse-wide`, ...).
    ///
    /// ```
    /// use pigeonhole_bench::WorkloadKind;
    ///
    /// for kind in WorkloadKind::ALL {
    ///     assert_eq!(kind.name().parse::<WorkloadKind>(), Ok(kind));
    /// }
    /// ```
    pub fn name(self) -> &'static str {
        match self {
            WorkloadKind::YcsbA => "ycsb-a",
            WorkloadKind::YcsbB => "ycsb-b",
            WorkloadKind::YcsbC => "ycsb-c",
            WorkloadKind::YcsbD => "ycsb-d",
            WorkloadKind::YcsbE => "ycsb-e",
            WorkloadKind::YcsbF => "ycsb-f",
            WorkloadKind::SparseWide => "sparse-wide",
            WorkloadKind::TimeSeriesTtl => "time-series-ttl",
            WorkloadKind::Adjacency => "adjacency",
            WorkloadKind::SkewedMultiShard => "skewed-multi-shard",
            WorkloadKind::GroupCommit => "group-commit",
        }
    }
}

impl FromStr for WorkloadKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        WorkloadKind::ALL
            .into_iter()
            .find(|k| k.name() == s)
            .ok_or_else(|| format!("unknown workload {s:?}"))
    }
}

/// Parameters of one run.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkloadConfig {
    /// Which workload.
    pub kind: WorkloadKind,
    /// RNG seed (printed in the report).
    pub seed: u64,
    /// Rows loaded before measuring.
    pub records: u64,
    /// Operations measured.
    pub operations: u64,
    /// Value size in bytes.
    pub value_len: usize,
    /// Client threads.
    pub threads: usize,
    /// The workload's "now", in microseconds since the Unix epoch: the time-series
    /// workload places event timestamps relative to it, so that part of the loaded points
    /// is already older than [`TIME_SERIES_TTL`]. `0` (the default of every preset) means
    /// the wall clock when the [`Workload`] is created; set it to make the generated
    /// operations identical across runs.
    pub epoch_micros: u64,
}

impl WorkloadConfig {
    /// A size that runs in well under a second per workload, for tests and CI smoke
    /// runs: 1,000 records, 2,000 operations, 100-byte values, one client thread (two
    /// for [`WorkloadKind::SkewedMultiShard`]).
    pub fn smoke(kind: WorkloadKind) -> Self {
        Self {
            kind,
            seed: 0x5EED,
            records: 1_000,
            operations: 2_000,
            value_len: 100,
            threads: match kind {
                WorkloadKind::SkewedMultiShard | WorkloadKind::GroupCommit => 2,
                _ => 1,
            },
            epoch_micros: 0,
        }
    }

    /// The CLI default: 50,000 records, 200,000 operations, 100-byte values, one client
    /// thread (four for [`WorkloadKind::SkewedMultiShard`]). Small enough to finish in
    /// seconds per workload, so it is the quick check; [`WorkloadConfig::full`] is the
    /// spec's scale.
    pub fn small(kind: WorkloadKind) -> Self {
        Self {
            kind,
            seed: 0x5EED,
            records: if kind == WorkloadKind::GroupCommit {
                GROUP_COMMIT_ROWS
            } else {
                50_000
            },
            operations: if kind == WorkloadKind::GroupCommit {
                20_000
            } else {
                200_000
            },
            value_len: 100,
            threads: match kind {
                WorkloadKind::SkewedMultiShard | WorkloadKind::GroupCommit => 4,
                _ => 1,
            },
            epoch_micros: 0,
        }
    }

    /// The spec's scale (build plan: "1M rows" for sparse-wide), now that memtables flush
    /// to SSTs: 1,000,000 records and 1,000,000 operations with 100-byte values; the
    /// skewed workload does 2,000,000 writes from four client threads. The adjacency
    /// workload loads 2,000,000 edges (50,000 vertices of mean out-degree 40). With the
    /// default 64 MiB write buffer the data set is several times the memtable budget, so
    /// flushes and compactions run during the measurement. It is not larger than RAM on a
    /// typical workstation; raise `--records` for that.
    ///
    /// ```
    /// use pigeonhole_bench::{WorkloadConfig, WorkloadKind};
    ///
    /// let c = WorkloadConfig::full(WorkloadKind::SparseWide);
    /// assert_eq!((c.records, c.operations), (1_000_000, 1_000_000));
    /// assert_eq!(WorkloadConfig::smoke(WorkloadKind::SparseWide).records, 1_000);
    /// ```
    pub fn full(kind: WorkloadKind) -> Self {
        let mut c = Self::small(kind);
        c.records = match kind {
            WorkloadKind::Adjacency => 2_000_000,
            WorkloadKind::GroupCommit => GROUP_COMMIT_ROWS,
            _ => 1_000_000,
        };
        c.operations = match kind {
            WorkloadKind::SkewedMultiShard => 2_000_000,
            WorkloadKind::GroupCommit => 200_000,
            _ => 1_000_000,
        };
        c
    }

    /// The larger-than-memory preset (`--scale larger-than-ram`): the [`full`](Self::full)
    /// sizes, meant to run with [`MemoryBudget::larger_than_ram`] so the data set is tens
    /// of times the engine's write buffer plus block cache. Gets are split into cold
    /// (first touch of a row) and hot (repeat) in the report ([`ReadSplit`]).
    ///
    /// The data set is made larger than the *memory budget*, not larger than the
    /// machine's RAM: a bench that must run on a laptop and in CI bounds the engine's
    /// memory instead of needing a machine-sized file. Cold reads therefore miss the
    /// engine's cache but may still hit the OS page cache; see `docs/bench.md`.
    ///
    /// ```
    /// use pigeonhole_bench::{MemoryBudget, WorkloadConfig, WorkloadKind};
    ///
    /// let c = WorkloadConfig::larger_than_ram(WorkloadKind::YcsbC);
    /// assert_eq!(c, WorkloadConfig::full(WorkloadKind::YcsbC));
    /// // 1M rows x 10 fields x 100 bytes against 24 MiB of memory.
    /// let m = MemoryBudget::larger_than_ram();
    /// assert!(c.records * 10 * c.value_len as u64 > 30 * (m.write_buffer + m.cache));
    /// ```
    pub fn larger_than_ram(kind: WorkloadKind) -> Self {
        Self::full(kind)
    }
}

/// One generated operation, store-neutral.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BenchOp {
    /// Read one cell.
    Get {
        /// Row.
        row: Vec<u8>,
        /// Family.
        family: &'static str,
        /// Qualifier.
        qualifier: Vec<u8>,
    },
    /// Read every cell of one row's family (YCSB `readallfields`, a sparse row read).
    GetRow {
        /// Row.
        row: Vec<u8>,
        /// Family.
        family: &'static str,
    },
    /// Write cells of one row at the commit time.
    Put {
        /// Row.
        row: Vec<u8>,
        /// Family.
        family: &'static str,
        /// Qualifier/value pairs.
        cells: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// Write cells of one row at an explicit event time. A cell older than its family's
    /// TTL ([`TIME_SERIES_TTL`] for [`METRIC_FAMILY`]) is expired the moment it is written
    /// and no read returns it.
    PutAt {
        /// Row.
        row: Vec<u8>,
        /// Family.
        family: &'static str,
        /// Event time, microseconds since the Unix epoch.
        ts: u64,
        /// Qualifier/value pairs.
        cells: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// Scan `len` rows from `start`.
    Scan {
        /// Start row.
        start: Vec<u8>,
        /// Rows to read.
        len: u32,
    },
    /// Read then write one cell.
    ReadModifyWrite {
        /// Row.
        row: Vec<u8>,
        /// Qualifier.
        qualifier: Vec<u8>,
    },
}

impl BenchOp {
    /// The operation's type, as the report's per-type latency table names it.
    ///
    /// ```
    /// use pigeonhole_bench::BenchOp;
    ///
    /// let op = BenchOp::Scan { start: b"r".to_vec(), len: 10 };
    /// assert_eq!(op.type_name(), "scan");
    /// ```
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Get { .. } => "get",
            Self::GetRow { .. } => "row read",
            Self::Put { .. } => "put",
            Self::PutAt { .. } => "put at",
            Self::Scan { .. } => "scan",
            Self::ReadModifyWrite { .. } => "read-modify-write",
        }
    }
}

/// Generates the operations of a workload, deterministically from the seed.
///
/// The data model of each workload (rows, families, qualifiers, mixes) is documented
/// in `docs/bench.md`.
///
/// ```
/// use pigeonhole_bench::{BenchOp, Workload, WorkloadConfig, WorkloadKind};
///
/// let config = WorkloadConfig::smoke(WorkloadKind::YcsbC);
/// let mut w = Workload::new(config.clone());
/// assert_eq!(w.load_ops().count() as u64, config.records);
/// let ops: Vec<BenchOp> = w.run_ops().collect();
/// assert_eq!(ops.len() as u64, config.operations);
/// assert!(ops.iter().all(|op| matches!(op, BenchOp::GetRow { .. })));
/// // Same seed, same operations.
/// assert_eq!(Workload::new(config).run_ops().collect::<Vec<_>>(), ops);
/// ```
#[derive(Debug)]
pub struct Workload {
    generator: workload::Gen,
}

impl Workload {
    /// A generator for `config`.
    pub fn new(config: WorkloadConfig) -> Self {
        Self {
            generator: workload::Gen::new(config),
        }
    }

    /// Operations that load the initial data set.
    pub fn load_ops(&mut self) -> impl Iterator<Item = BenchOp> + '_ {
        self.generator.load()
    }

    /// Fails once the run has outlasted the time-series live margin; see
    /// `Gen::check_epoch_age`.
    pub(crate) fn check_epoch_age(&self) -> Result<(), String> {
        self.generator.check_epoch_age()
    }

    /// The measured operations. Each call yields the same sequence.
    pub fn run_ops(&mut self) -> impl Iterator<Item = BenchOp> + '_ {
        self.generator.run()
    }
}

/// Executes operations from one extra client thread against a store a [`Runner`] has
/// open.
pub trait Client: Send {
    /// Executes one operation.
    fn execute(&mut self, op: &BenchOp) -> Result<(), String>;
}

/// A store under test.
pub trait Runner {
    /// Store name for reports (`pigeonhole`, `rocksdb`, `sqlite-eav`, `fjall`).
    fn name(&self) -> &'static str;

    /// Opens a fresh store in `dir`.
    fn open(&mut self, dir: &Path) -> Result<(), String>;

    /// Executes one operation.
    fn execute(&mut self, op: &BenchOp) -> Result<(), String>;

    /// Flushes and closes.
    fn close(&mut self) -> Result<(), String>;

    /// Writes retried after the store answered `Busy` (a write stall that outlasted its
    /// timeout). Reported, not hidden; zero for stores that never refuse a write.
    fn busy_retries(&self) -> u64 {
        0
    }

    /// A client for another thread, sharing the open store. `None` (the default) means
    /// the runner serves one client, and [`run`] uses one thread whatever
    /// [`WorkloadConfig::threads`] asks for.
    fn client(&self) -> Option<Box<dyn Client>> {
        None
    }

    /// Store settings that affect results, for reports (shards, durability, ...).
    fn describe(&self) -> String {
        String::new()
    }

    /// Cumulative per-shard counters since open, indexed by shard, with `tablets_start`
    /// and `tablets_end` both the current tablet count; empty (the default) for stores
    /// without shards. [`run_detailed`] reads it around the measured phase.
    fn shard_shares(&self) -> Vec<ShardShare> {
        Vec::new()
    }
}

/// Runs Pigeonhole through its public API.
///
/// One table, `bench`, holds every workload's family ([`FAMILIES`]), each keeping one
/// version; [`METRIC_FAMILY`] has a TTL. Commits are `Buffered` unless
/// [`sync`](PigeonholeRunner::sync) is set.
#[derive(Debug, Default)]
pub struct PigeonholeRunner {
    settings: runners::pigeonhole::Settings,
    open: Option<runners::pigeonhole::Open>,
}

/// Latency and throughput of one run.
#[derive(Debug, Clone, PartialEq)]
pub struct Report {
    /// Store name.
    pub store: &'static str,
    /// Workload.
    pub workload: WorkloadKind,
    /// Seed.
    pub seed: u64,
    /// Median latency.
    pub p50: Duration,
    /// 99th percentile.
    pub p99: Duration,
    /// 99.9th percentile.
    pub p999: Duration,
    /// Operations per second.
    pub throughput: f64,
    /// Hardware description; reference runs only gate phases.
    pub hardware: String,
}

/// How [`run_detailed`] measures, beyond the workload itself.
///
/// ```
/// let opts = pigeonhole_bench::RunOptions::default();
/// assert_eq!(opts.warmup, 0.05);
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunOptions {
    /// Unrecorded warmup operations before the measured ones, as a fraction of
    /// [`WorkloadConfig::operations`] (default 0.05). They are the first operations of
    /// the same seeded stream, executed on one thread, so caches, allocators and lazily
    /// built structures are warm when the clock starts.
    pub warmup: f64,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self { warmup: 0.05 }
    }
}

impl RunOptions {
    fn warmup_ops(&self, config: &WorkloadConfig) -> u64 {
        (config.operations as f64 * self.warmup.max(0.0)).round() as u64
    }
}

/// Runs `config` against `runner` in `dir` and reports, with the default
/// [`RunOptions`].
///
/// Opens a fresh store in `dir`, loads the data set (not measured), runs the warmup,
/// generates every measured operation up front, then times each operation on
/// [`WorkloadConfig::threads`] client threads (one when the runner has no
/// [`Runner::client`]). Throughput is operations over the wall time of the measured
/// phase.
pub fn run(runner: &mut dyn Runner, config: &WorkloadConfig, dir: &Path) -> Result<Report, String> {
    let env = Environment::detect(dir);
    let record = run_detailed(runner, config, dir, &RunOptions::default())?;
    Ok(Report {
        store: runner.name(),
        workload: config.kind,
        seed: config.seed,
        p50: Duration::from_nanos(record.p50_ns),
        p99: Duration::from_nanos(record.p99_ns),
        p999: Duration::from_nanos(record.p999_ns),
        throughput: record.throughput,
        hardware: env.summary(),
    })
}

/// [`run`] with explicit [`RunOptions`], returning the full record written to JSON
/// reports.
///
/// Fails up front for [`WorkloadKind::YcsbF`] with more than one thread: its
/// read-modify-write is a get and then a put, not atomic, so concurrent clients would
/// lose each other's updates and the stores would no longer do the same work.
pub fn run_detailed(
    runner: &mut dyn Runner,
    config: &WorkloadConfig,
    dir: &Path,
    options: &RunOptions,
) -> Result<RunRecord, String> {
    if config.threads > 1 && config.kind == WorkloadKind::YcsbF {
        return Err(
            "ycsb-f runs on one thread: read-modify-write is not atomic in the runners, \
             so concurrent clients would race"
                .to_owned(),
        );
    }
    let warmup = options.warmup_ops(config);
    let mut workload = Workload::new(WorkloadConfig {
        operations: config.operations + warmup,
        ..config.clone()
    });
    runner.open(dir)?;
    let result = measure(runner, config, &mut workload, warmup)
        .and_then(|measured| workload.check_epoch_age().map(|()| measured));
    let busy_retries = runner.busy_retries();
    let closed = runner.close();
    let (load, elapsed, threads, hist, split, shards) = result?;
    closed?;
    let ops = hist.count();
    let (reads, by_type) = split.into_split();
    Ok(RunRecord {
        store: runner.name().to_owned(),
        store_config: runner.describe(),
        workload: config.kind.name().to_owned(),
        seed: config.seed,
        records: config.records,
        operations: ops,
        value_len: config.value_len,
        threads,
        load_secs: load.as_secs_f64(),
        run_secs: elapsed.as_secs_f64(),
        throughput: ops as f64 / elapsed.as_secs_f64().max(1e-9),
        p50_ns: hist.percentile(0.50).as_nanos() as u64,
        p99_ns: hist.percentile(0.99).as_nanos() as u64,
        p999_ns: hist.percentile(0.999).as_nanos() as u64,
        mean_ns: hist.mean().as_nanos() as u64,
        max_ns: hist.max().as_nanos() as u64,
        warmup_ops: warmup,
        detail: RunDetail {
            store_bytes: dir_bytes(dir),
            busy_retries,
            reads,
            by_type,
            shards,
        },
    })
}

/// Bytes of every file under `dir`: the store's size on disk after the run.
fn dir_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            match e.metadata() {
                Ok(m) if m.is_dir() => stack.push(e.path()),
                Ok(m) => total += m.len(),
                Err(_) => {}
            }
        }
    }
    total
}

/// Whether a measured operation is a read of a row the run has not read before.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Other,
    Cold,
    Hot,
}

/// Latency of gets, cold (first touch of a row since the store opened) and hot (the row
/// was read before), and of each operation type.
#[derive(Debug, Default)]
struct Split {
    cold: Histogram,
    hot: Histogram,
    by_type: std::collections::BTreeMap<&'static str, Histogram>,
}

impl Split {
    fn record(&mut self, op: &BenchOp, class: Class, latency: Duration) {
        match class {
            Class::Cold => self.cold.record(latency),
            Class::Hot => self.hot.record(latency),
            Class::Other => {}
        }
        self.by_type
            .entry(op.type_name())
            .or_default()
            .record(latency);
    }

    fn merge(&mut self, other: &Split) {
        self.cold.merge(&other.cold);
        self.hot.merge(&other.hot);
        for (name, h) in &other.by_type {
            self.by_type.entry(name).or_default().merge(h);
        }
    }

    fn into_split(self) -> (Option<ReadSplit>, Vec<OpTypeStats>) {
        let reads = (self.cold.count() + self.hot.count() > 0).then(|| ReadSplit {
            cold: LatencyStats::of(&self.cold),
            hot: LatencyStats::of(&self.hot),
        });
        let by_type = self
            .by_type
            .iter()
            .map(|(name, h)| OpTypeStats {
                op: (*name).to_owned(),
                stats: LatencyStats::of(h),
                mean_ns: h.mean().as_nanos() as u64,
            })
            .collect();
        (reads, by_type)
    }
}

/// Classifies each measured op; `seen` starts with the rows the warmup read.
fn classify(ops: &[BenchOp], seen: &mut std::collections::HashSet<u64>) -> Vec<Class> {
    ops.iter()
        .map(|op| match op {
            BenchOp::Get { row, .. } | BenchOp::GetRow { row, .. } => {
                if seen.insert(row_hash(row)) {
                    Class::Cold
                } else {
                    Class::Hot
                }
            }
            _ => Class::Other,
        })
        .collect()
}

fn row_hash(row: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    row.hash(&mut h);
    h.finish()
}

type Measured = (Duration, Duration, usize, Histogram, Split, Vec<ShardShare>);

/// Per-shard shares between two [`Runner::shard_shares`] readings.
fn shares_between(before: &[ShardShare], after: &[ShardShare]) -> Vec<ShardShare> {
    before
        .iter()
        .zip(after)
        .map(|(b, a)| ShardShare::between(b, a))
        .collect()
}

fn measure(
    runner: &mut dyn Runner,
    config: &WorkloadConfig,
    workload: &mut Workload,
    warmup: u64,
) -> Result<Measured, String> {
    let load_start = Instant::now();
    for op in workload.load_ops() {
        runner.execute(&op).map_err(|e| format!("load: {e}"))?;
    }
    let load = load_start.elapsed();

    let mut ops = workload.run_ops();
    let mut seen = std::collections::HashSet::new();
    for op in ops.by_ref().take(warmup as usize) {
        if let BenchOp::Get { row, .. } | BenchOp::GetRow { row, .. } = &op {
            seen.insert(row_hash(row));
        }
        runner.execute(&op).map_err(|e| format!("warmup: {e}"))?;
    }
    let ops: Vec<BenchOp> = ops.collect();
    let classes = classify(&ops, &mut seen);
    let threads = config.threads.max(1);
    let clients: Vec<Box<dyn Client>> = if threads > 1 {
        (0..threads).map_while(|_| runner.client()).collect()
    } else {
        Vec::new()
    };
    if clients.len() < 2 {
        let mut hist = Histogram::new();
        let mut split = Split::default();
        let before = runner.shard_shares();
        let start = Instant::now();
        for (op, class) in ops.iter().zip(&classes) {
            let t = Instant::now();
            runner.execute(op)?;
            let latency = t.elapsed();
            hist.record(latency);
            split.record(op, *class, latency);
        }
        let elapsed = start.elapsed();
        let shards = shares_between(&before, &runner.shard_shares());
        return Ok((load, elapsed, 1, hist, split, shards));
    }

    // Deal operations round-robin so each thread sees the workload's mix in order.
    let n = clients.len();
    let mut shares: Vec<Vec<(BenchOp, Class)>> = (0..n)
        .map(|_| Vec::with_capacity(ops.len() / n + 1))
        .collect();
    for (i, (op, class)) in ops.into_iter().zip(classes).enumerate() {
        shares[i % n].push((op, class));
    }
    let barrier = Arc::new(Barrier::new(n + 1));
    std::thread::scope(|scope| {
        let handles: Vec<_> = clients
            .into_iter()
            .zip(shares)
            .map(|(mut client, share)| {
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || -> Result<(Histogram, Split), String> {
                    let mut hist = Histogram::new();
                    let mut split = Split::default();
                    barrier.wait();
                    for (op, class) in &share {
                        let t = Instant::now();
                        client.execute(op)?;
                        let latency = t.elapsed();
                        hist.record(latency);
                        split.record(op, *class, latency);
                    }
                    Ok((hist, split))
                })
            })
            .collect();
        let before = runner.shard_shares();
        barrier.wait();
        let start = Instant::now();
        let mut hist = Histogram::new();
        let mut split = Split::default();
        let mut first_err = None;
        for h in handles {
            match h.join() {
                Ok(Ok((h, s))) => {
                    hist.merge(&h);
                    split.merge(&s);
                }
                Ok(Err(e)) => {
                    first_err.get_or_insert(e);
                }
                Err(_) => {
                    first_err.get_or_insert_with(|| "client thread panicked".to_owned());
                }
            }
        }
        let elapsed = start.elapsed();
        let shards = shares_between(&before, &runner.shard_shares());
        match first_err {
            Some(e) => Err(e),
            None => Ok((load, elapsed, n, hist, split, shards)),
        }
    })
}
