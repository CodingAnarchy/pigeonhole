//! Benchmark workloads and comparison runners for Pigeonhole.
//!
//! A [`Workload`] generates operations; a [`Runner`] executes them against one store; a
//! [`Report`] summarizes latency percentiles and throughput. Runners for RocksDB, SQLite
//! (EAV) and fjall arrive with the bench task behind cargo features, so their dependencies
//! never enter the default build.
//!
//! Every run records its seed and hardware description; results are labeled non-reference
//! until reference hardware exists (decision D5).
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]
// Interface freeze: bodies are `todo!()`. Remove this allow when implementing.
#![allow(unused_variables, clippy::ptr_arg)]

use std::path::Path;
use std::time::Duration;

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
    /// Write cells of one row.
    Put {
        /// Row.
        row: Vec<u8>,
        /// Family.
        family: &'static str,
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

/// Generates the operations of a workload.
#[derive(Debug)]
pub struct Workload {
    _priv: (),
}

impl Workload {
    /// A generator for `config`.
    pub fn new(config: WorkloadConfig) -> Self {
        todo!()
    }

    /// Operations that load the initial data set.
    pub fn load_ops(&mut self) -> impl Iterator<Item = BenchOp> + '_ {
        std::iter::empty()
    }

    /// The measured operations.
    pub fn run_ops(&mut self) -> impl Iterator<Item = BenchOp> + '_ {
        std::iter::empty()
    }
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
}

/// Runs Pigeonhole through its public API.
#[derive(Debug, Default)]
pub struct PigeonholeRunner {
    _priv: (),
}

impl Runner for PigeonholeRunner {
    fn name(&self) -> &'static str {
        "pigeonhole"
    }

    fn open(&mut self, dir: &Path) -> Result<(), String> {
        todo!()
    }

    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        todo!()
    }

    fn close(&mut self) -> Result<(), String> {
        todo!()
    }
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

/// Runs `config` against `runner` in `dir` and reports.
pub fn run(runner: &mut dyn Runner, config: &WorkloadConfig, dir: &Path) -> Result<Report, String> {
    todo!()
}
