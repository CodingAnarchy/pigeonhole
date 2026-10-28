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
    Comparison, Delta, EngineCommit, Environment, GroupCommit, JUDGED_MIN_COMMITS, JUDGED_MIN_SECS,
    LatencyStats, OpTypeStats, Placement, ReadSplit, ReaderProcesses, Riders, Rounds, RunDetail,
    RunRecord, SUITE_FORMAT, Scaling, Settle, ShardShare, Stalls, Suite, Tolerance, compare,
};
#[cfg(feature = "fjall")]
pub use runners::fjall::FjallRunner;
pub use runners::inline::INLINE_IN_FLIGHT;
pub use runners::pigeonhole::DEFAULT_MEMTABLE_BUDGET;
pub use runners::placement::{PLACEMENT_RATE_PER_SHARD, PLACEMENT_SETTLE_BOUND, PlacementOptions};
pub use runners::reader_process::{EXE_ENV as READER_EXE_ENV, child as reader_child};
#[cfg(feature = "rocksdb")]
pub use runners::rocksdb::RocksDbRunner;
#[cfg(feature = "rocksdb")]
pub use runners::rocksdb_txn::{RocksDbTxnRunner, TxnMode};
#[cfg(feature = "sqlite")]
pub use runners::sqlite::SqliteRunner;
pub use runners::{BLOOM_BITS, CacheModel, MemoryBudget};
pub use workload::{
    COUNTER_THREADS, CROSS_SHARD_COMMIT, EDGE_FAMILY, FAMILIES, GROUP_COMMIT_CELLS,
    GROUP_COMMIT_ROWS, GROUP_COMMIT_THREADS, METRIC_FAMILY, SPARSE_FAMILY, TIME_SERIES_TTL,
    XSHARD_CLIENTS, XSHARD_CROSS_FRACTION, XSHARD_PARTITIONS, YCSB_FAMILY,
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
    /// Contended cross-shard commits (D246): [`XSHARD_CLIENTS`] clients over
    /// [`XSHARD_PARTITIONS`] partitions, half the commits one row in one partition and half
    /// one row in each of 2-4 partitions, committed together
    /// ([`BenchOp::Commit`]). Pigeonhole gives each partition a table of its own, so with
    /// tablet changes off they sit one per shard and a multi-row commit crosses shards;
    /// the other stores commit the rows in one batch or transaction.
    CrossShardContention,
    /// Counters (D247): from [`COUNTER_THREADS`] clients, 90% increments of a Zipfian row's
    /// counter ([`BenchOp::Incr`]) and 10% reads of one ([`BenchOp::ReadCounter`]). Pigeonhole
    /// increments a counter family without reading it; RocksDB uses a merge operator (an
    /// `i64` add), SQLite an upsert, fjall a locked read-modify-write. Not in `all`: the gate's
    /// `contended` step runs it.
    Counters,
    /// Transfers (D247, reported): from [`COUNTER_THREADS`] clients, each operation moves one
    /// unit between two Zipfian accounts in one transaction that reads both
    /// ([`BenchOp::Transfer`]). Pigeonhole runs an optimistic `Transaction`, retried on a
    /// conflict (counted, [`RunDetail::txn_conflicts`]); RocksDB runs an
    /// `OptimisticTransactionDB` transaction (or `TransactionDB`'s, as `rocksdb-txn`) and fjall
    /// an optimistic write transaction, each retried on a conflict (D274); SQLite uses an
    /// immediate transaction. Not in `all`.
    Transfer,
    /// Sparse-wide with deletes (D247, reported): [`WorkloadKind::SparseWide`]'s mix with 5% of
    /// the operations deleting, half a column ([`BenchOp::DeleteColumn`]) and half a whole row
    /// ([`BenchOp::DeleteRow`]), taken from its puts. Not in `all`.
    SparseWideDeletes,
    /// Adjacency with filtered scans (D247, reported): [`WorkloadKind::Adjacency`]'s mix with
    /// its one-vertex scans reading only the edges whose qualifier starts with a prefix that
    /// selects about a tenth of the vertices ([`BenchOp::ScanFiltered`]). Not in `all`.
    AdjacencyPrefix,
    /// Time series with time-range scans (D247, reported): [`WorkloadKind::TimeSeriesTtl`]'s mix
    /// with its newest-ten scans reading only the points from 12 to 6 hours old
    /// ([`BenchOp::ScanFiltered`]). Not in `all`.
    TimeSeriesRange,
}

impl WorkloadKind {
    /// The `all` suite, in order.
    pub const ALL: [WorkloadKind; 12] = [
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
        WorkloadKind::CrossShardContention,
    ];

    /// Every workload: [`ALL`](Self::ALL) and the ones only named runs take
    /// ([`WorkloadKind::Counters`], [`WorkloadKind::Transfer`],
    /// [`WorkloadKind::SparseWideDeletes`], [`WorkloadKind::AdjacencyPrefix`],
    /// [`WorkloadKind::TimeSeriesRange`]).
    pub const EVERY: [WorkloadKind; 17] = [
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
        WorkloadKind::CrossShardContention,
        WorkloadKind::Counters,
        WorkloadKind::Transfer,
        WorkloadKind::SparseWideDeletes,
        WorkloadKind::AdjacencyPrefix,
        WorkloadKind::TimeSeriesRange,
    ];

    /// The CLI and report name (`ycsb-a`, `sparse-wide`, ...).
    ///
    /// ```
    /// use pigeonhole_bench::WorkloadKind;
    ///
    /// for kind in WorkloadKind::EVERY {
    ///     assert_eq!(kind.name().parse::<WorkloadKind>(), Ok(kind));
    /// }
    /// assert!(WorkloadKind::ALL.iter().all(|k| WorkloadKind::EVERY.contains(k)));
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
            WorkloadKind::CrossShardContention => "cross-shard-contention",
            WorkloadKind::Counters => "counters",
            WorkloadKind::Transfer => "transfer",
            WorkloadKind::SparseWideDeletes => "sparse-wide-deletes",
            WorkloadKind::AdjacencyPrefix => "adjacency-prefix",
            WorkloadKind::TimeSeriesRange => "time-series-range",
        }
    }
}

impl FromStr for WorkloadKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        WorkloadKind::EVERY
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
    /// for [`WorkloadKind::SkewedMultiShard`], [`WorkloadKind::GroupCommit`] and
    /// [`WorkloadKind::CrossShardContention`]).
    pub fn smoke(kind: WorkloadKind) -> Self {
        Self {
            kind,
            seed: 0x5EED,
            records: 1_000,
            operations: 2_000,
            value_len: 100,
            threads: match kind {
                WorkloadKind::SkewedMultiShard
                | WorkloadKind::GroupCommit
                | WorkloadKind::CrossShardContention
                | WorkloadKind::Counters
                | WorkloadKind::Transfer => 2,
                _ => 1,
            },
            epoch_micros: 0,
        }
    }

    /// The CLI default: 50,000 records, 200,000 operations, 100-byte values, one client
    /// thread (four for [`WorkloadKind::SkewedMultiShard`] and [`WorkloadKind::GroupCommit`],
    /// [`XSHARD_CLIENTS`] for [`WorkloadKind::CrossShardContention`]). Small enough to finish in
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
                WorkloadKind::CrossShardContention => XSHARD_CLIENTS,
                WorkloadKind::Counters | WorkloadKind::Transfer => COUNTER_THREADS,
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
    /// Write cells of one or more rows in one commit, each row in a partition
    /// ([`WorkloadKind::CrossShardContention`], D246). Pigeonhole puts each partition in a
    /// table of its own; the other stores keep every partition's rows in one key space (the
    /// row keys name the partition) and commit the rows in one batch or transaction.
    Commit {
        /// Family.
        family: &'static str,
        /// The rows, each in a different partition.
        rows: Vec<PartitionRow>,
    },
    /// Add `delta` to a row's counter ([`WorkloadKind::Counters`], D247): a counter family's
    /// increment on Pigeonhole, a merge on RocksDB, an upsert on SQLite.
    Incr {
        /// Row.
        row: Vec<u8>,
        /// The amount added.
        delta: i64,
    },
    /// Read a row's counter ([`WorkloadKind::Counters`]).
    ReadCounter {
        /// Row.
        row: Vec<u8>,
    },
    /// Move `amount` from one account to another in one transaction that reads both
    /// balances ([`WorkloadKind::Transfer`]): `i64` little-endian, in [`YCSB_FAMILY`]'s
    /// `bal` column.
    Transfer {
        /// The account debited.
        from: Vec<u8>,
        /// The account credited (never `from`).
        to: Vec<u8>,
        /// The amount moved.
        amount: i64,
    },
    /// Delete one column of a row ([`WorkloadKind::SparseWideDeletes`]).
    DeleteColumn {
        /// Row.
        row: Vec<u8>,
        /// Family.
        family: &'static str,
        /// Qualifier.
        qualifier: Vec<u8>,
    },
    /// Delete a whole row, every family ([`WorkloadKind::SparseWideDeletes`]).
    DeleteRow {
        /// Row.
        row: Vec<u8>,
    },
    /// Scan `len` rows from `start` in one family, reading only the cells that pass the
    /// filters; a row with none of them is not a row (D247's filtered scans).
    ScanFiltered {
        /// Start row.
        start: Vec<u8>,
        /// Rows to read.
        len: u32,
        /// Family.
        family: &'static str,
        /// Only qualifiers starting with this.
        qualifier_prefix: Option<Vec<u8>>,
        /// Only cells with a timestamp in `start..end` (µs).
        time_range: Option<(u64, u64)>,
    },
}

/// One row of a [`BenchOp::Commit`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRow {
    /// Partition, below [`XSHARD_PARTITIONS`].
    pub partition: usize,
    /// Row (unique across partitions).
    pub row: Vec<u8>,
    /// Qualifier/value pairs.
    pub cells: Vec<(Vec<u8>, Vec<u8>)>,
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
    ///
    /// A [`BenchOp::Commit`] of one row is a `put`; of several, a
    /// [`CROSS_SHARD_COMMIT`].
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Commit { rows, .. } if rows.len() > 1 => CROSS_SHARD_COMMIT,
            Self::Commit { .. } => "put",
            Self::Incr { .. } => "incr",
            Self::ReadCounter { .. } => "counter read",
            Self::Transfer { .. } => "transfer",
            Self::DeleteColumn { .. } => "column delete",
            Self::DeleteRow { .. } => "row delete",
            Self::ScanFiltered { .. } => "filtered scan",
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

    /// Executes `ops` with several in flight at once (D247's async rows), returning each
    /// operation's latency from submission to resolution, in `ops`' order (`None` for one not
    /// started before `deadline`); `None` (the default) when the client runs one operation at
    /// a time.
    fn pipelined(
        &mut self,
        ops: &[&BenchOp],
        deadline: Option<Instant>,
    ) -> Option<Result<Vec<Option<Duration>>, String>> {
        let _ = (ops, deadline);
        None
    }
}

/// [`Runner::op_label`] for a transfer whose two accounts are on one shard.
pub const TRANSFER_ONE_SHARD: &str = "transfer (one shard)";
/// [`Runner::op_label`] for a transfer whose two accounts are on different shards.
pub const TRANSFER_CROSS_SHARD: &str = "transfer (cross-shard)";

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

    /// Called with the number of client threads before [`client`](Self::client) is asked for
    /// them, for a store that sizes per-connection memory from it (SQLite under
    /// [`CacheModel::Total`]). The default does nothing.
    fn plan_clients(&mut self, _clients: usize) {}

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

    /// A finer latency class for `op` than its [`BenchOp::type_name`], read before the
    /// measured pass (outside each operation's timing): `None` (the default) keeps the type
    /// name. The Pigeonhole runner splits transfers by whether their two accounts are on one
    /// shard ([`TRANSFER_ONE_SHARD`], [`TRANSFER_CROSS_SHARD`]), as placed at that moment.
    fn op_label(&self, op: &BenchOp) -> Option<&'static str> {
        let _ = op;
        None
    }

    /// Cumulative per-shard counters since open, indexed by shard, with `tablets_start`
    /// and `tablets_end` both the current tablet count; empty (the default) for stores
    /// without shards. [`run_detailed`] reads it around the measured phase.
    fn shard_shares(&self) -> Vec<ShardShare> {
        Vec::new()
    }

    /// The store's active shard count and the count its balancer is heading for, `None`
    /// (the default) for stores without one. The warm-up does not end while the two differ
    /// (D242's amendment, #983): a growth whose passes are still accruing, or the store's
    /// first pack from all its shards, has not moved a tablet yet, so the placement reads
    /// quiet right before it changes. Nor while fewer shards hold a tablet than the first
    /// (D242's second amendment): the growth's moves come one per dwell and can leave a
    /// quiet window between them.
    fn pack_target(&self) -> Option<(usize, usize)> {
        None
    }

    /// Marks the start of a run phase (`load`, `settle`, `warmup`, `measured`, then `end`)
    /// in the store's driver trace, if one was asked for (#1015); the default does nothing.
    fn mark_phase(&mut self, _phase: &str) {}

    /// The driver threads the store's shards run on and whether every shard is on its
    /// driver (D289 PR 2), `None` (the default) for stores without a load controller. The
    /// warm-up does not end while the controller resizes or still moves shards.
    fn driver_target(&self) -> Option<(usize, bool)> {
        None
    }

    /// Cumulative stall counters since open ([`Stalls`]); `None` (the default) for stores
    /// that do not report them. [`run_detailed`] reads it around the measured phase.
    fn stalls(&self) -> Option<Stalls> {
        None
    }

    /// Writes out the store's write buffer: the first part of the settle step after the
    /// load (D233). The default does nothing.
    fn flush(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// The store's background work so far, for the settle step after the load (D233) and
    /// the measured phase's report; `None` (the default) for stores without background work.
    fn background(&self) -> Option<Background> {
        None
    }

    /// Called after the load and the settle step, before the warmup: a runner set to load at
    /// buffered durability commits at its own from here on (D247). Returns whether the
    /// durability changed, so the warmup waits for the placement to react to it (D242); the
    /// default does nothing and returns `false`.
    fn measured_durability(&mut self) -> Result<bool, String> {
        Ok(false)
    }

    /// Starts background work riding on the measured phase, for runners asked to (D247's
    /// riders); the default does nothing. [`run_detailed`] calls it as the measured phase
    /// starts and [`stop_riders`](Self::stop_riders) as it ends.
    fn start_riders(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Stops the riders [`start_riders`](Self::start_riders) started and reports them; `None`
    /// (the default) when there were none.
    fn stop_riders(&mut self) -> Result<Option<Riders>, String> {
        Ok(None)
    }

    /// Starts reader processes beside the measured phase, for runners asked to (#683, D247
    /// item K): separate processes reading `config`'s loaded rows through
    /// `Pigeonhole::open_reader`. The default does nothing. [`run_detailed`] calls it as the
    /// measured phase starts and [`stop_reader_processes`](Self::stop_reader_processes) as it
    /// ends.
    fn start_reader_processes(&mut self, config: &WorkloadConfig) -> Result<(), String> {
        let _ = config;
        Ok(())
    }

    /// Stops the reader processes and reports them; `None` (the default) when there were
    /// none.
    fn stop_reader_processes(&mut self) -> Result<Option<ReaderProcesses>, String> {
        Ok(None)
    }

    /// Transactions retried after a conflict since open, for stores whose transactions can
    /// conflict; `None` (the default) elsewhere. [`run_detailed`] reads it around the measured
    /// phase.
    fn txn_conflicts(&self) -> Option<u64> {
        None
    }

    /// The store's last sequence number, for stores that number their commits; `None` (the
    /// default) elsewhere. [`run_detailed`] reads it around the measured phase: the
    /// sequence numbers used beyond one per commit are commit rounds that did not commit
    /// (D246).
    fn seqno(&self) -> Option<u64> {
        None
    }
}

/// A store's background work so far ([`Runner::background`]), sampled while settling after
/// the load and around the measured phase (D233).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Background {
    /// Flushes and compactions completed since open, for stores that count them.
    pub work: Option<u64>,
    /// Anything that changes while background work runs (the work count, a file count): the
    /// store has settled once it stays the same.
    pub marker: u64,
    /// Whether the store reports work running or queued now, for stores that tell.
    pub running: Option<bool>,
    /// Nanoseconds the store's own threads spent working since open, for stores that count
    /// it: a rise while no operation runs is background work.
    pub busy_nanos: Option<u64>,
}

/// How long a store's background work must stay quiet before the settle step ends (D233).
pub const SETTLE_QUIET: Duration = Duration::from_secs(1);
/// The longest the settle step waits (D233): past it the run goes on with a warning, and
/// the record says it did not settle.
pub const SETTLE_BOUND: Duration = Duration::from_secs(60);
/// How often the settle step samples the store's background work.
const SETTLE_POLL: Duration = Duration::from_millis(20);

/// The settle step between the load and the warmup (D233): flushes the store's write buffer,
/// then waits until its background work stays quiet for [`SETTLE_QUIET`]: no work running,
/// the marker unchanged, and its threads' work time rising by less than a quarter of the
/// poll interval per poll. Gives up after [`SETTLE_BOUND`] with a warning. Every store
/// takes the same step; a store without background work settles at once after its flush.
pub fn settle(runner: &mut dyn Runner) -> Result<Settle, String> {
    settle_within(runner, SETTLE_QUIET, SETTLE_BOUND)
}

/// [`settle`] with its quiet time and bound given.
fn settle_within(
    runner: &mut dyn Runner,
    quiet: Duration,
    bound: Duration,
) -> Result<Settle, String> {
    let start = Instant::now();
    let first = runner.background();
    runner.flush().map_err(|e| format!("settle: {e}"))?;
    let Some(mut last) = runner.background() else {
        return Ok(Settle {
            millis: start.elapsed().as_millis() as u64,
            settled: true,
            work: None,
        });
    };
    let mut quiet_since = Instant::now();
    let settled = loop {
        if quiet_since.elapsed() >= quiet {
            break true;
        }
        if start.elapsed() >= bound {
            eprintln!(
                "warning: {} still had background work {bound:?} after the load; the \
                 measured phase may include it",
                runner.name()
            );
            break false;
        }
        std::thread::sleep(SETTLE_POLL);
        let now = runner.background().unwrap_or_default();
        let worked = match (now.busy_nanos, last.busy_nanos) {
            (Some(n), Some(l)) => n.saturating_sub(l) > SETTLE_POLL.as_nanos() as u64 / 4,
            _ => false,
        };
        if now.running == Some(true) || now.marker != last.marker || worked {
            quiet_since = Instant::now();
        }
        last = now;
    };
    Ok(Settle {
        millis: start.elapsed().as_millis() as u64,
        settled,
        work: first
            .and_then(|f| f.work)
            .zip(last.work)
            .map(|(f, l)| l.saturating_sub(f)),
    })
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
/// assert_eq!(opts.min_measured, std::time::Duration::ZERO);
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunOptions {
    /// Unrecorded warmup operations before the measured ones, as a fraction of
    /// [`WorkloadConfig::operations`] (default 0.05). They are the first operations of
    /// the same seeded stream, executed on the measured phase's client threads and repeated
    /// until the tablet placement settles (D242), so caches, allocators, lazily built
    /// structures and the balancer are warm when the clock starts.
    pub warmup: f64,
    /// The shortest measured phase. An inline run (the scaling gate and D213's group
    /// commit, [`PigeonholeRunner::run_inline`]) repeats its writes until this much time has
    /// been measured (D219); a client run ([`run_detailed`]) goes on with the stream's next
    /// operations (D243). Zero (the default) measures the configured operations once.
    pub min_measured: std::time::Duration,
    /// Commits each shard's thread keeps in flight in an inline run (at least 1; default
    /// [`INLINE_IN_FLIGHT`], the shape D213 and D226 judge). Fewer trade throughput for
    /// latency: the gate reports 4 and 8 beside the judged 16 (D226).
    pub in_flight: usize,
    /// Inline runs: pin each shard's thread to a CPU of its own, one per physical core first
    /// (D220, D235; [`pigeonhole_io::sys::pin_plan`]). The gate judges pinned runs and
    /// reports unpinned ones beside them. Default off.
    pub pin: bool,
    /// The longest measured phase (D247): past it no client starts another operation, the
    /// run reports what it measured and the record says it was capped
    /// ([`RunDetail::capped`]). The warmup is bounded by its own cap. `None` (the default):
    /// unbounded, the configured operations (and `min_measured`) decide.
    pub max_measured: Option<std::time::Duration>,
    /// Client runs: the measured operations are offered at about this many per second in
    /// all, each client starting its next operation on a schedule of `rate / clients` (a
    /// late client catches up without sleeping). Latency is still each operation's own
    /// time: the store's service time at that offered load, as opposed to at its maximum
    /// (#759 judges read p99 both ways). The warmup is not paced; a client with a
    /// pipelined path runs operation by operation instead. `None` (the default): every
    /// client runs as fast as it can.
    pub offered_rate: Option<f64>,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            warmup: 0.05,
            min_measured: std::time::Duration::ZERO,
            in_flight: INLINE_IN_FLIGHT,
            pin: false,
            max_measured: None,
            offered_rate: None,
        }
    }
}

impl RunOptions {
    pub(crate) fn warmup_ops(&self, config: &WorkloadConfig) -> u64 {
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
/// [`WorkloadKind::YcsbF`] runs on any number of threads: every runner's read-modify-write
/// is atomic across clients (Pigeonhole's `commit_if`, retried on a lost race; D247).
pub fn run_detailed(
    runner: &mut dyn Runner,
    config: &WorkloadConfig,
    dir: &Path,
    options: &RunOptions,
) -> Result<RunRecord, String> {
    let warmup = options.warmup_ops(config);
    // A run lengthened to `min_measured` (D243) takes operations past the configured count
    // from the same stream.
    let operations = if options.min_measured.is_zero() {
        config.operations + warmup
    } else {
        u64::MAX
    };
    let mut workload = Workload::new(WorkloadConfig {
        operations,
        ..config.clone()
    });
    runner.open(dir)?;
    let result = measure(
        runner,
        config,
        &mut workload,
        warmup,
        options.min_measured,
        options.max_measured,
        options.offered_rate,
    )
    .and_then(|measured| workload.check_epoch_age().map(|()| measured));
    let busy_retries = runner.busy_retries();
    let closing = Instant::now();
    let closed = runner.close();
    let close_ms = closing.elapsed().as_secs_f64() * 1e3;
    let (load, elapsed, threads, hist, split, detail) = result?;
    let (
        shards,
        stalls,
        settle,
        background_work,
        warmed,
        seqnos_used,
        txn_conflicts,
        (riders, reader_processes),
        capped,
    ) = detail;
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
            stalls,
            engine_commit: None,
            settle: Some(settle),
            background_work,
            warmup_secs: Some(warmed.secs),
            warmup_settle_secs: Some(warmed.settle_secs),
            warmup_settled: Some(warmed.settled),
            drivers: warmed.drivers,
            seqnos_used,
            txn_conflicts,
            riders,
            reader_processes,
            close_ms: Some(close_ms),
            capped: options.max_measured.map(|_| capped),
            offered_rate: options.offered_rate,
        },
    })
}

/// Bytes of every file under `dir`: the store's size on disk after the run.
pub(crate) fn dir_bytes(dir: &Path) -> u64 {
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
    fn record(
        &mut self,
        op: &BenchOp,
        class: Class,
        label: Option<&'static str>,
        latency: Duration,
    ) {
        match class {
            Class::Cold => self.cold.record(latency),
            Class::Hot => self.hot.record(latency),
            Class::Other => {}
        }
        self.by_type
            .entry(label.unwrap_or_else(|| op.type_name()))
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

type Measured = (
    Duration,
    Duration,
    usize,
    Histogram,
    Split,
    (
        Vec<ShardShare>,
        Option<Stalls>,
        Settle,
        Option<u64>,
        Warmed,
        Option<u64>,
        Option<u64>,
        (Option<Riders>, Option<ReaderProcesses>),
        bool,
    ),
);

/// Background work completed between two [`Runner::background`] readings (D233).
fn work_between(before: Option<Background>, after: Option<Background>) -> Option<u64> {
    Some(after?.work?.saturating_sub(before?.work?))
}

/// What the measured phase stalled on, between two [`Runner::stalls`] readings.
fn stalls_between(before: Option<Stalls>, after: Option<Stalls>) -> Option<Stalls> {
    Some(Stalls::between(&before?, &after?))
}

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
    min_measured: Duration,
    max_measured: Option<Duration>,
    offered_rate: Option<f64>,
) -> Result<Measured, String> {
    let load_start = Instant::now();
    runner.mark_phase("load");
    for op in workload.load_ops() {
        runner.execute(&op).map_err(|e| format!("load: {e}"))?;
    }
    let load = load_start.elapsed();
    runner.mark_phase("settle");
    let settled = settle(runner)?;
    let switched = runner.measured_durability()?;

    let threads = config.threads.max(1);
    runner.plan_clients(if threads > 1 { threads } else { 0 });
    let mut clients: Vec<Box<dyn Client>> = if threads > 1 {
        (0..threads).map_while(|_| runner.client()).collect()
    } else {
        Vec::new()
    };
    if clients.len() < 2 {
        clients.clear();
    }
    let lanes = clients.len().max(1);

    let mut ops = workload.run_ops();
    let mut seen = std::collections::HashSet::new();
    let warm: Vec<BenchOp> = ops.by_ref().take(warmup as usize).collect();
    for op in &warm {
        if let BenchOp::Get { row, .. } | BenchOp::GetRow { row, .. } = op {
            seen.insert(row_hash(row));
        }
    }
    let warm = deal(warm.into_iter().map(|op| (op, Class::Other)), lanes);
    // D242, D247: after a durability switch packing re-places the tablets over 30-40 s, one
    // move per dwell with gaps longer than the usual quiet window, so the warmup waits longer.
    let (quiet, cap) = if switched {
        (WARMUP_SETTLED_AFTER_SWITCH, WARMUP_CAP_AFTER_SWITCH)
    } else {
        (WARMUP_SETTLED, WARMUP_CAP)
    };
    runner.mark_phase("warmup");
    let warmed = warm_up(runner, &mut clients, &warm, quiet, cap)?;

    // The measured operations: the configured count, then, while less than `min_measured`
    // has been measured, the stream's next operations, about enough for the rest at the rate
    // so far (D243). Each pass's operations are generated before its clock starts.
    let before = (
        runner.shard_shares(),
        runner.stalls(),
        runner.background(),
        runner.seqno(),
        runner.txn_conflicts(),
    );
    runner.start_riders()?;
    runner.start_reader_processes(config)?;
    let (mut elapsed, mut hist, mut split) = (Duration::ZERO, Histogram::new(), Split::default());
    let mut next = config.operations;
    // D247: past `max_measured` no client starts another operation.
    let deadline = max_measured.map(|m| Instant::now() + m);
    let mut capped = false;
    runner.mark_phase("measured");
    loop {
        let chunk: Vec<BenchOp> = ops.by_ref().take(next as usize).collect();
        if chunk.is_empty() {
            break;
        }
        let classes = classify(&chunk, &mut seen);
        let share = deal(chunk.into_iter().zip(classes), lanes);
        let labels: Vec<Vec<Option<&'static str>>> = share
            .iter()
            .map(|s| s.iter().map(|(op, _)| runner.op_label(op)).collect())
            .collect();
        let (e, h, s) = pass(
            runner,
            &mut clients,
            &share,
            Some(&labels),
            true,
            deadline,
            offered_rate,
        )?;
        elapsed += e;
        hist.merge(&h);
        split.merge(&s);
        if deadline.is_some_and(|d| Instant::now() >= d) {
            capped = true;
            break;
        }
        if elapsed >= min_measured {
            break;
        }
        let rate = hist.count() as f64 / elapsed.as_secs_f64().max(1e-9);
        let rest = (min_measured - elapsed).as_secs_f64() * rate * 1.1;
        next = (rest.ceil() as u64).max(MIN_EXTRA_OPS);
    }
    runner.mark_phase("end");
    let shards = (
        shares_between(&before.0, &runner.shard_shares()),
        stalls_between(before.1, runner.stalls()),
        settled,
        work_between(before.2, runner.background()),
        warmed,
        before
            .3
            .zip(runner.seqno())
            .map(|(b, a)| a.saturating_sub(b)),
        before
            .4
            .zip(runner.txn_conflicts())
            .map(|(b, a)| a.saturating_sub(b)),
        (runner.stop_riders()?, runner.stop_reader_processes()?),
        capped,
    );
    Ok((load, elapsed, lanes, hist, split, shards))
}

/// The fewest operations a pass that lengthens the measured phase runs (D243).
const MIN_EXTRA_OPS: u64 = 1_000;

/// How long the tablet placement must go without a split, merge or move before the warmup
/// ends (D242): longer than the balancer's dwell (10 passes of 100 ms, D146), so a quiet
/// stretch means the balancer settled, not that it is waiting out a dwell.
pub const WARMUP_SETTLED: Duration = Duration::from_secs(2);

/// The warmup's quiet window after the run's durability switched from the load's (D242,
/// D247): longer than packing's move cadence while it re-places the tablets for the new
/// level. On the AX102 (ctr274's D248 A/B, ycsb-a at 16 threads, default durability) that
/// re-placement took 30-40 s, one move per dwell with gaps longer than [`WARMUP_SETTLED`], so a
/// 2 s window, or a 5 s minimum before it, passed mid-spread.
pub const WARMUP_SETTLED_AFTER_SWITCH: Duration = Duration::from_secs(5);

/// The longest the warmup waits after a durability switch (D242, D247): room for the
/// 30-40 s re-placement and a quiet window after it.
pub const WARMUP_CAP_AFTER_SWITCH: Duration = Duration::from_secs(60);

/// The longest the warmup waits for the tablet placement to settle (D242): past it the run
/// measures anyway, with a warning, and the record says it did not settle. 60 s since D242's
/// third amendment (owner, 2026-10-27): the first pack from the unpacked start waits for
/// D255's 30 s window, and at Buffered skewed-multi-shard settled 0.4 s inside the former
/// 30 s cap.
pub const WARMUP_CAP: Duration = Duration::from_secs(60);

/// The warmup (D242): the warmup operations, repeated on the measured phase's own client
/// threads until the store's tablet placement has gone [`WARMUP_SETTLED`] without a split,
/// merge or move, at most [`WARMUP_CAP`]. Every store takes the same step; a store without
/// shards never changes placement, so it warms for [`WARMUP_SETTLED`]. After a durability
/// switch `quiet` and `cap` are [`WARMUP_SETTLED_AFTER_SWITCH`] and
/// [`WARMUP_CAP_AFTER_SWITCH`]. Returns how the warmup went; no warmup operations, no
/// warmup. Tests pass shorter windows.
fn warm_up(
    runner: &mut dyn Runner,
    clients: &mut [Box<dyn Client>],
    warm: &[Vec<(BenchOp, Class)>],
    quiet: Duration,
    cap: Duration,
) -> Result<Warmed, String> {
    let origin = Instant::now();
    warm_up_on(runner, clients, warm, quiet, cap, &|| origin.elapsed())
}

/// [`warm_up`] on `clock` (the time since any fixed origin), which times the quiet window,
/// the cap and the seconds returned. Tests pass a fake store's own clock, so the windows
/// are judged on the store's time, not on how fast the runner runs it (#936).
fn warm_up_on(
    runner: &mut dyn Runner,
    clients: &mut [Box<dyn Client>],
    warm: &[Vec<(BenchOp, Class)>],
    quiet: Duration,
    cap: Duration,
    clock: &dyn Fn() -> Duration,
) -> Result<Warmed, String> {
    if warm.iter().all(Vec::is_empty) {
        return Ok(Warmed {
            secs: 0.0,
            settle_secs: 0.0,
            settled: true,
            drivers: runner.driver_target().map(|(k, _)| k),
        });
    }
    // A pass stops at the cap on the wall clock too.
    let deadline = Instant::now() + cap;
    let started = clock();
    let mut last_change = started;
    let mut drivers = runner.driver_target();
    let settled = loop {
        let before = runner.shard_shares();
        pass(runner, clients, warm, None, false, Some(deadline), None)
            .map_err(|e| format!("warmup: {e}"))?;
        let after = runner.shard_shares();
        // The load controller (D289 PR 2) resizing, or shards still on their way to their
        // drivers, counts as a placement change too.
        let now_drivers = runner.driver_target();
        if now_drivers != drivers || now_drivers.is_some_and(|(_, settled)| !settled) {
            last_change = clock();
        }
        drivers = now_drivers;
        // A pending growth or first pack counts as a change: its moves have not started
        // yet (#983: a warm-up that ended before the store's first pack measured its 16-19
        // moves). So does a growth whose moves have not spread the tablets yet: fewer shards hold one than
        // the active count calls for (as many as there are tablets at most), and the next
        // move may be more than a quiet window away (one per dwell, behind a compaction).
        if unwinding(runner.pack_target(), &after) || placement_changed(&before, &after) {
            last_change = clock();
        }
        if clock() - last_change >= quiet {
            break true;
        }
        if clock() - started >= cap {
            eprintln!(
                "warning: {}'s tablet placement still changed {cap:?} into the warmup; the \
                 measured phase may include tablet changes",
                runner.name()
            );
            break false;
        }
    };
    Ok(Warmed {
        secs: (clock() - started).as_secs_f64(),
        settle_secs: (last_change - started).as_secs_f64(),
        settled,
        drivers: drivers.map(|(k, _)| k),
    })
}

/// How a warmup went ([`warm_up`]).
#[derive(Debug, Clone, Copy, PartialEq)]
struct Warmed {
    /// The warmup's seconds.
    secs: f64,
    /// Seconds into the warmup of the last placement change or pending growth or pack: the
    /// placement settled there, and the quiet window followed (#983).
    settle_secs: f64,
    /// Whether the placement settled within the cap.
    settled: bool,
    /// The driver threads the shards ran on at the end (D289 PR 2), if the store reports them.
    drivers: Option<usize>,
}

/// Whether the balancer is still heading somewhere (D242's amendments, #983): a growth or
/// the store's first pack from all its shards is pending (`target != k`; the store
/// publishes no other shrink), or fewer shards hold a tablet than the active count `k`,
/// bounded by the tablets there are. Later shrinks are not waited for: their last steps are
/// slow and the quiet rule covers their moves.
fn unwinding(target: Option<(usize, usize)>, shares: &[ShardShare]) -> bool {
    target.is_some_and(|(k, target)| {
        let holders = shares.iter().filter(|s| s.tablets_end > 0).count() as u64;
        let tablets: u64 = shares.iter().map(|s| s.tablets_end).sum();
        target != k || holders < (k as u64).min(tablets)
    })
}

/// Whether any shard split, merged or moved a tablet between two [`Runner::shard_shares`]
/// readings.
fn placement_changed(before: &[ShardShare], after: &[ShardShare]) -> bool {
    before.len() != after.len()
        || before
            .iter()
            .zip(after)
            .any(|(b, a)| (a.splits, a.merges, a.moves) != (b.splits, b.merges, b.moves))
}

/// Deals operations round-robin into `lanes` shares, so each client thread sees the
/// workload's mix in order.
fn deal(
    ops: impl ExactSizeIterator<Item = (BenchOp, Class)>,
    lanes: usize,
) -> Vec<Vec<(BenchOp, Class)>> {
    let mut shares: Vec<Vec<_>> = (0..lanes)
        .map(|_| Vec::with_capacity(ops.len() / lanes + 1))
        .collect();
    for (i, op) in ops.enumerate() {
        shares[i % lanes].push(op);
    }
    shares
}

/// Runs one share per client thread (on the runner itself when there are no clients), from
/// a common start, and returns the wall time and, when `record` is set, each operation's
/// latency. With `offered_rate`, each share starts its operations on a schedule of
/// `offered_rate / shares` per second ([`RunOptions::offered_rate`]).
fn pass(
    runner: &mut dyn Runner,
    clients: &mut [Box<dyn Client>],
    shares: &[Vec<(BenchOp, Class)>],
    labels: Option<&[Vec<Option<&'static str>>]>,
    record: bool,
    deadline: Option<Instant>,
    offered_rate: Option<f64>,
) -> Result<(Duration, Histogram, Split), String> {
    // A share's labels ([`Runner::op_label`]), `None` each when there are none.
    let label = |i: usize, j: usize| labels.and_then(|l| l.get(i)?.get(j).copied().flatten());
    // One share's gap between scheduled starts.
    let gap = offered_rate
        .filter(|r| *r > 0.0)
        .map(|r| Duration::from_secs_f64(shares.len().max(1) as f64 / r));
    let run = |exec: &mut dyn FnMut(&BenchOp) -> Result<(), String>,
               share: &[(BenchOp, Class)],
               i: usize|
     -> Result<(Histogram, Split), String> {
        let mut hist = Histogram::new();
        let mut split = Split::default();
        let paced_from = Instant::now();
        for (j, (op, class)) in share.iter().enumerate() {
            if let Some(gap) = gap {
                let due = paced_from + gap.mul_f64(j as f64);
                let now = Instant::now();
                if due > now {
                    std::thread::sleep(due - now);
                }
            }
            let t = Instant::now();
            if deadline.is_some_and(|d| t >= d) {
                break;
            }
            exec(op)?;
            if record {
                let latency = t.elapsed();
                hist.record(latency);
                split.record(op, *class, label(i, j), latency);
            }
        }
        Ok((hist, split))
    };
    if clients.is_empty() {
        let start = Instant::now();
        let (hist, split) = run(&mut |op| runner.execute(op), &shares[0], 0)?;
        return Ok((start.elapsed(), hist, split));
    }
    let barrier = Arc::new(Barrier::new(clients.len() + 1));
    std::thread::scope(|scope| {
        let handles: Vec<_> = clients
            .iter_mut()
            .zip(shares)
            .enumerate()
            .map(|(i, (client, share))| {
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    let ops: Vec<&BenchOp> = share.iter().map(|(op, _)| op).collect();
                    // A paced share runs operation by operation, on its schedule.
                    let pipelined = if gap.is_some() {
                        None
                    } else {
                        client.pipelined(&ops, deadline)
                    };
                    match pipelined {
                        Some(latencies) => {
                            let latencies = latencies?;
                            let (mut hist, mut split) = (Histogram::new(), Split::default());
                            if record {
                                for (j, ((op, class), latency)) in
                                    share.iter().zip(latencies).enumerate()
                                {
                                    let Some(latency) = latency else { continue };
                                    hist.record(latency);
                                    split.record(op, *class, label(i, j), latency);
                                }
                            }
                            Ok((hist, split))
                        }
                        None => run(&mut |op| client.execute(op), share, i),
                    }
                })
            })
            .collect();
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
        match first_err {
            Some(e) => Err(e),
            None => Ok((elapsed, hist, split)),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store whose background work runs for its first `busy_polls` samples after the
    /// flush (forever with `u32::MAX`).
    struct Fake {
        flushed: bool,
        polls: std::cell::Cell<u32>,
        busy_polls: u32,
        /// Tablet moves the store reports: one more per reading until `moving` readings.
        readings: std::cell::Cell<u64>,
        moving: u64,
        /// A timed spread on the fake's own clock (`now`): one more move every interval,
        /// this many in all. The fake's operations then advance `now` by `op_time` instead
        /// of sleeping, so the spread and the warmup's windows share one clock (#936).
        spread: Option<(Duration, u64)>,
        /// The fake's clock: the time its operations took.
        now: std::rc::Rc<std::cell::Cell<Duration>>,
        /// Operations executed, and how long each takes.
        executed: u64,
        op_time: Duration,
        /// Readings of the pack target that report a growth pending (4 of 16 shards), then
        /// none.
        targets: std::cell::Cell<u64>,
        growing: u64,
        /// The pending target reads as a first pack (16 shards heading for 5, #983) rather
        /// than a growth.
        packing: bool,
        /// Readings of the shares that report the tablets still on one of 4 active shards
        /// (3 there, 1 arriving on the next), then one per shard.
        unwinding: u64,
    }

    impl Runner for Fake {
        fn name(&self) -> &'static str {
            "fake"
        }
        fn open(&mut self, _: &Path) -> Result<(), String> {
            Ok(())
        }
        fn execute(&mut self, _: &BenchOp) -> Result<(), String> {
            self.executed += 1;
            self.now.set(self.now.get() + self.op_time);
            if self.spread.is_none() {
                std::thread::sleep(self.op_time);
            }
            Ok(())
        }
        fn close(&mut self) -> Result<(), String> {
            Ok(())
        }
        fn flush(&mut self) -> Result<(), String> {
            self.flushed = true;
            Ok(())
        }
        fn pack_target(&self) -> Option<(usize, usize)> {
            let n = self.targets.get();
            self.targets.set(n + 1);
            Some(match (n < self.growing, self.packing) {
                (true, false) => (4, 16),
                (true, true) => (16, 5),
                (false, _) => (16, 16),
            })
        }
        fn shard_shares(&self) -> Vec<ShardShare> {
            if let Some((every, moves)) = self.spread {
                let n = (self.now.get().as_nanos() / every.as_nanos()) as u64;
                return vec![ShardShare {
                    moves: n.min(moves),
                    ..ShardShare::default()
                }];
            }
            let n = self.readings.get();
            self.readings.set(n + 1);
            if self.unwinding > 0 {
                let spread = n >= self.unwinding;
                let tablets = |i: u64| if spread { 1 } else { [3, 1, 0, 0][i as usize] };
                return (0..4)
                    .map(|i| ShardShare {
                        tablets_end: tablets(i),
                        moves: u64::from(spread),
                        ..ShardShare::default()
                    })
                    .collect();
            }
            vec![ShardShare {
                moves: n.min(self.moving),
                ..ShardShare::default()
            }]
        }
        fn background(&self) -> Option<Background> {
            let n = self.polls.get();
            self.polls.set(n.saturating_add(1));
            let work = u64::from(n.min(self.busy_polls));
            Some(Background {
                work: Some(work),
                marker: work,
                running: Some(n < self.busy_polls),
                busy_nanos: None,
            })
        }
    }

    fn fake(busy_polls: u32) -> Fake {
        Fake {
            flushed: false,
            polls: std::cell::Cell::new(0),
            busy_polls,
            readings: std::cell::Cell::new(0),
            moving: 0,
            spread: None,
            now: std::rc::Rc::default(),
            executed: 0,
            op_time: Duration::ZERO,
            targets: std::cell::Cell::new(0),
            growing: 0,
            packing: false,
            unwinding: 0,
        }
    }

    fn warm_ops(n: usize) -> Vec<Vec<(BenchOp, Class)>> {
        let ops = Workload::new(WorkloadConfig::smoke(WorkloadKind::YcsbA))
            .run_ops()
            .take(n)
            .map(|op| (op, Class::Other))
            .collect::<Vec<_>>();
        deal(ops.into_iter(), 1)
    }

    #[test]
    fn the_warmup_waits_for_a_pending_growth_before_its_quiet_window() {
        // D242's amendment: a store whose balancer reports a growth pending (4 of 16 shards)
        // for its first 6 readings, with no tablet change yet. The quiet window starts only
        // once the target is reached, so the warm-up runs the 6 passes and then the window;
        // on the placement alone it would have ended after the window.
        const GROWING: u64 = 6;
        let mut r = fake(0);
        r.growing = GROWING;
        r.op_time = Duration::from_millis(5);
        let warm = warm_ops(4);
        let quiet = Duration::from_millis(100);
        let Warmed { secs, settled, .. } =
            warm_up(&mut r, &mut [], &warm, quiet, Duration::from_secs(10)).unwrap();
        assert!(settled);
        let passes = r.executed / 4;
        assert!(
            passes > GROWING,
            "ended with the growth pending: {passes} passes, {secs} s"
        );
        assert!(secs >= quiet.as_secs_f64());
        // A growth that never completes ends at the cap, unsettled.
        let mut r = fake(0);
        r.growing = u64::MAX;
        let cap = Duration::from_millis(200);
        let Warmed { secs, settled, .. } = warm_up(&mut r, &mut [], &warm, quiet, cap).unwrap();
        assert!(!settled);
        assert!(secs >= cap.as_secs_f64());
    }

    #[test]
    fn the_warmup_waits_for_a_pending_first_pack_and_reports_when_it_settled() {
        // #983: a store on all 16 shards whose balancer reports its first pack pending (to
        // 5) for its first 6 readings, with no tablet change yet. Before, the warm-up ended
        // on the quiet placement and the pack's moves landed in the measured phase.
        const PACKING: u64 = 6;
        let mut r = fake(0);
        r.growing = PACKING;
        r.packing = true;
        r.op_time = Duration::from_millis(5);
        let warm = warm_ops(4);
        let quiet = Duration::from_millis(100);
        let w = warm_up(&mut r, &mut [], &warm, quiet, Duration::from_secs(10)).unwrap();
        assert!(w.settled);
        let passes = r.executed / 4;
        assert!(
            passes > PACKING,
            "ended with the first pack pending: {passes} passes, {w:?}"
        );
        // It settled at the last pending reading, and the quiet window followed.
        assert!(w.settle_secs > 0.0, "{w:?}");
        assert!(w.secs >= w.settle_secs + quiet.as_secs_f64(), "{w:?}");
        assert!(unwinding(Some((16, 5)), &[]));
        assert!(!unwinding(Some((5, 5)), &[]));
    }

    #[test]
    fn the_warmup_waits_while_fewer_shards_hold_tablets_than_the_active_count() {
        // D242's second amendment: the balancer reports 4 active shards reached (no growth
        // pending), but for the first 6 readings only 2 of them hold tablets and no move
        // lands within a quiet window. The warm-up waits until every active shard holds
        // one; before, two quiet readings ended it inside the unwind (the gate's
        // cross-shard row at 228–236K, 2 of 6 reps).
        const UNWINDING: u64 = 6;
        let mut r = fake(0);
        r.growing = 0; // the target reads (16, 16): no growth pending
        r.unwinding = UNWINDING;
        r.op_time = Duration::from_millis(5);
        let warm = warm_ops(4);
        let quiet = Duration::from_millis(100);
        let Warmed { secs, settled, .. } =
            warm_up(&mut r, &mut [], &warm, quiet, Duration::from_secs(10)).unwrap();
        assert!(settled);
        let readings = r.readings.get();
        assert!(
            readings > UNWINDING,
            "ended with 2 of 4 active shards holding tablets: {readings} readings, {secs} s"
        );
        // Fewer tablets than active shards is not an unwind: one tablet on one of four
        // shards is as spread as it gets.
        let one = [ShardShare {
            tablets_end: 1,
            ..ShardShare::default()
        }];
        assert!(!unwinding(Some((4, 4)), &one));
        assert!(!unwinding(
            Some((4, 4)),
            &[one[0], one[0], ShardShare::default()]
        ));
        let three = ShardShare {
            tablets_end: 3,
            ..ShardShare::default()
        };
        assert!(unwinding(
            Some((4, 4)),
            &[three, one[0], ShardShare::default()]
        ));
        assert!(!unwinding(
            Some((2, 2)),
            &[three, one[0], ShardShare::default()]
        ));
        assert!(!unwinding(None, &[ShardShare::default()]));
    }

    #[test]
    fn the_warmup_repeats_until_the_placement_stops_changing() {
        // D242: a store whose tablets move for its first 6 readings (3 warmup passes).
        let mut r = fake(0);
        r.moving = 6;
        r.op_time = Duration::from_millis(5);
        let warm = warm_ops(4);
        let quiet = Duration::from_millis(100);
        let Warmed { secs, settled, .. } =
            warm_up(&mut r, &mut [], &warm, quiet, Duration::from_secs(10)).unwrap();
        assert!(settled);
        assert!(
            r.executed >= 4 * 4,
            "the passes that moved, then quiet: {}",
            r.executed
        );
        assert!(secs >= quiet.as_secs_f64());
        // A placement that never settles ends at the cap.
        let mut r = fake(0);
        r.moving = u64::MAX;
        let cap = Duration::from_millis(200);
        let Warmed { secs, settled, .. } = warm_up(&mut r, &mut [], &warm, quiet, cap).unwrap();
        assert!(!settled);
        assert!(secs >= cap.as_secs_f64());
        // No warmup operations, no warmup.
        let Warmed { secs, settled, .. } =
            warm_up(&mut r, &mut [], &warm_ops(0), quiet, cap).unwrap();
        assert_eq!((secs, settled), (0.0, true));
    }

    #[test]
    fn a_quiet_window_longer_than_the_move_cadence_outlasts_a_slow_spread() {
        // D242/D247: a store whose placement moves once every 300 ms for 1.5 s, as packing
        // re-places tablets one per dwell after a durability switch. A 200 ms window ends
        // between two moves, with moves still to come; a 500 ms one outlasts the spread.
        // On the fake's own clock (#936): its spread once ran on wall time, and on a loaded
        // macOS runner the warmup's passes stretched past the move cadence, so every window
        // saw a move (#741's CI, then #935's). A pass is 4 operations of 5 ms: 20 ms.
        const MOVES: u64 = 5;
        let spreading = |every: u64, moves: u64| {
            let mut r = fake(0);
            r.op_time = Duration::from_millis(5);
            r.spread = Some((Duration::from_millis(every), moves));
            r
        };
        let warm = warm_ops(4);
        let cap = Duration::from_secs(10);
        let run = |r: &mut Fake, quiet: u64, cap: Duration| {
            let now = r.now.clone();
            warm_up_on(
                r,
                &mut [],
                &warm,
                Duration::from_millis(quiet),
                cap,
                &|| now.get(),
            )
            .unwrap()
        };
        // 200 ms quiet before the first move at 300 ms: settled at 200 ms, all five to come.
        let mut early = spreading(300, MOVES);
        let Warmed {
            secs,
            settle_secs,
            settled,
            ..
        } = run(&mut early, 200, cap);
        assert_eq!(settle_secs, 0.0, "no change seen");
        let moved = early.shard_shares()[0].moves;
        assert!(
            settled && moved == 0 && secs == 0.2,
            "after move {moved} of {MOVES}, {secs} s"
        );
        // The last move at 1.5 s, then 500 ms quiet: settled at 1.5 s.
        let Warmed {
            secs,
            settle_secs,
            settled,
            ..
        } = run(&mut spreading(300, MOVES), 500, cap);
        assert!(settled && secs == 1.5 + 0.5, "outlasted the spread: {secs}");
        assert_eq!(settle_secs, 1.5);
        // A spread longer than the cap ends at the cap, not settled.
        let Warmed { secs, settled, .. } =
            run(&mut spreading(100, u64::MAX), 500, Duration::from_secs(1));
        assert!(!settled && secs == 1.0, "capped at {secs} s");
    }

    #[test]
    fn an_offered_rate_paces_the_measured_operations() {
        // #759: 40 instant operations offered at 200/s take about 195 ms (the first at once,
        // then every 5 ms), and the record names the rate; unpaced they take no time.
        let mut config = WorkloadConfig::smoke(WorkloadKind::YcsbA);
        config.records = 10;
        config.operations = 40;
        let paced = RunOptions {
            warmup: 0.0,
            offered_rate: Some(200.0),
            ..RunOptions::default()
        };
        let dir = std::env::temp_dir().join(format!("phdb-bench-rate-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let rec = run_detailed(&mut fake(0), &config, &dir, &paced).unwrap();
        assert_eq!((rec.operations, rec.detail.offered_rate), (40, Some(200.0)));
        assert!((0.18..2.0).contains(&rec.run_secs), "{}", rec.run_secs);
        let free = RunOptions {
            warmup: 0.0,
            ..RunOptions::default()
        };
        let rec = run_detailed(&mut fake(0), &config, &dir, &free).unwrap();
        assert_eq!(rec.detail.offered_rate, None);
        assert!(rec.run_secs < 0.18, "{}", rec.run_secs);
        std::fs::remove_dir(&dir).ok();
    }

    #[test]
    fn a_slow_run_stops_at_its_bound_and_says_it_was_capped() {
        // D247: 1,000 operations of 10 ms would measure 10 s; a 300 ms bound stops it.
        let mut r = fake(0);
        r.op_time = Duration::from_millis(10);
        let mut config = WorkloadConfig::smoke(WorkloadKind::YcsbA);
        config.records = 10;
        config.operations = 1_000;
        let options = RunOptions {
            warmup: 0.0,
            max_measured: Some(Duration::from_millis(300)),
            ..RunOptions::default()
        };
        let dir = std::env::temp_dir().join(format!("phdb-bench-max-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let rec = run_detailed(&mut r, &config, &dir, &options).unwrap();
        assert_eq!(rec.detail.capped, Some(true));
        assert!(
            rec.operations < 1_000 && rec.operations > 0,
            "{}",
            rec.operations
        );
        assert!(rec.run_secs < 1.0, "{}", rec.run_secs);
        // A bound the run never reaches: not capped.
        config.operations = 5;
        let rec = run_detailed(&mut fake(0), &config, &dir, &options).unwrap();
        assert_eq!((rec.detail.capped, rec.operations), (Some(false), 5));
        // No bound, no flag.
        let rec = run_detailed(&mut fake(0), &config, &dir, &RunOptions::default()).unwrap();
        assert_eq!(rec.detail.capped, None);
        std::fs::remove_dir(&dir).ok();
    }

    #[test]
    fn a_short_run_goes_on_until_its_minimum_is_measured() {
        // D243: 2 operations of 1 ms measure about 2 ms (tens on a loaded CI runner); a
        // 300 ms minimum takes more of the stream.
        let mut r = fake(0);
        r.op_time = Duration::from_millis(1);
        let mut config = WorkloadConfig::smoke(WorkloadKind::YcsbA);
        config.operations = 2;
        let options = RunOptions {
            warmup: 0.0,
            min_measured: Duration::from_millis(300),
            ..RunOptions::default()
        };
        let dir = std::env::temp_dir().join(format!("phdb-bench-min-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let rec = run_detailed(&mut r, &config, &dir, &options).unwrap();
        assert!(rec.run_secs >= 0.3, "{}", rec.run_secs);
        assert!(rec.operations > 2, "{}", rec.operations);
        assert_eq!(rec.detail.warmup_secs, Some(0.0));
        let once = RunOptions {
            warmup: 0.0,
            ..RunOptions::default()
        };
        let rec = run_detailed(&mut fake(0), &config, &dir, &once).unwrap();
        assert_eq!(
            rec.operations, 2,
            "no minimum: the configured operations once"
        );
        std::fs::remove_dir(&dir).unwrap();
    }

    #[test]
    fn settling_flushes_then_waits_for_the_background_work_to_stop() {
        let mut r = fake(5);
        let s = settle_within(&mut r, Duration::from_millis(100), Duration::from_secs(10)).unwrap();
        assert!(r.flushed);
        assert!(s.settled);
        assert_eq!(s.work, Some(5), "the work done while settling");
        assert!(s.millis >= 100, "quiet for the whole quiet time: {s:?}");
    }

    #[test]
    fn settling_gives_up_at_its_bound() {
        let mut r = fake(u32::MAX);
        let s = settle_within(
            &mut r,
            Duration::from_millis(100),
            Duration::from_millis(300),
        )
        .unwrap();
        assert!(!s.settled);
        assert!(s.millis >= 300);
    }
}
