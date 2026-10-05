use pigeonhole_format::Durability;

use crate::ModelOp;

/// Shape of a generated workload.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkloadSpec {
    /// Distinct rows.
    pub rows: u64,
    /// Qualifiers per row (upper bound; drawn per write).
    pub qualifiers: u64,
    /// Families, by name; every table in the run has all of them.
    pub families: Vec<String>,
    /// Zipfian skew (0.0 = uniform).
    pub zipf_theta: f64,
    /// Fraction of operations that are reads.
    pub read_fraction: f64,
    /// Fraction of writes that are deletes of some kind.
    pub delete_fraction: f64,
    /// Mutations per commit (upper bound).
    pub max_batch: u32,
    /// Largest value in bytes.
    pub max_value_len: u32,
}

/// One generated operation.
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    /// Commit these mutations at this durability.
    Commit(Vec<ModelOp>, Durability),
    /// Point read of the latest version.
    Get {
        /// Row.
        row: Vec<u8>,
        /// Family.
        family: String,
        /// Qualifier.
        qualifier: Vec<u8>,
    },
    /// Ordered scan of `[start, end)`.
    Scan {
        /// Start row.
        start: Vec<u8>,
        /// End row.
        end: Vec<u8>,
    },
    /// Take a snapshot to read from later.
    Snapshot,
}

/// A seeded generator of [`Op`]s for one table.
#[derive(Debug)]
pub struct Workload {
    _priv: (),
}

impl Workload {
    /// A generator for `table` following `spec`.
    pub fn new(seed: u64, table: &str, spec: WorkloadSpec) -> Self {
        todo!()
    }
}

impl Iterator for Workload {
    type Item = Op;

    fn next(&mut self) -> Option<Op> {
        todo!()
    }
}
