use std::ops::Bound;

use pigeonhole_format::{Durability, Seqno, Timestamp};

/// Policy of a model family (mirrors the persisted family options that change semantics).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ModelFamily {
    /// Family name.
    pub name: String,
    /// Versions kept per column; 0 keeps all.
    pub max_versions: u32,
    /// TTL in microseconds; 0 disables it.
    pub ttl_micros: u64,
    /// Whether merge operands use the built-in `i64` add operator.
    pub i64_add: bool,
}

/// One mutation in a model commit. Rows, families and qualifiers are plain values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelOp {
    /// Put a value.
    Put {
        /// Table name.
        table: String,
        /// Row.
        row: Vec<u8>,
        /// Family name.
        family: String,
        /// Qualifier.
        qualifier: Vec<u8>,
        /// Timestamp; `None` uses the commit timestamp.
        ts: Option<Timestamp>,
        /// Value.
        value: Vec<u8>,
    },
    /// Add `delta` with the `i64` merge operator.
    Incr {
        /// Table name.
        table: String,
        /// Row.
        row: Vec<u8>,
        /// Family name.
        family: String,
        /// Qualifier.
        qualifier: Vec<u8>,
        /// Amount.
        delta: i64,
    },
    /// Delete one version.
    DeleteCell {
        /// Table name.
        table: String,
        /// Row.
        row: Vec<u8>,
        /// Family name.
        family: String,
        /// Qualifier.
        qualifier: Vec<u8>,
        /// Version timestamp.
        ts: Timestamp,
    },
    /// Delete all versions of a column at or below the commit timestamp.
    DeleteColumn {
        /// Table name.
        table: String,
        /// Row.
        row: Vec<u8>,
        /// Family name.
        family: String,
        /// Qualifier.
        qualifier: Vec<u8>,
    },
    /// Delete a family within a row.
    DeleteFamily {
        /// Table name.
        table: String,
        /// Row.
        row: Vec<u8>,
        /// Family name.
        family: String,
    },
    /// Delete a whole row (every family).
    DeleteRow {
        /// Table name.
        table: String,
        /// Row.
        row: Vec<u8>,
    },
}

/// A visible cell as the model returns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelCell {
    /// Family name.
    pub family: String,
    /// Qualifier.
    pub qualifier: Vec<u8>,
    /// Timestamp.
    pub ts: Timestamp,
    /// Resolved value.
    pub value: Vec<u8>,
}

/// What may survive a crash: every commit with seqno `<= must_survive` is present after
/// recovery, nothing above `may_survive` is, and the survivors form a prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrashWindow {
    /// Highest seqno that must survive.
    pub must_survive: Seqno,
    /// Highest seqno that may survive.
    pub may_survive: Seqno,
}

/// The reference model: an in-memory `BTreeMap` implementation of Pigeonhole's semantics.
#[derive(Debug, Default)]
pub struct Model {
    _priv: (),
}

impl Model {
    /// An empty model.
    pub fn new() -> Self {
        todo!()
    }

    /// Creates a table with families.
    pub fn create_table(&mut self, name: &str, families: Vec<ModelFamily>) {
        todo!()
    }

    /// Applies a commit atomically at the next seqno with timestamp `commit_ts`, remembering
    /// the durability level it was acknowledged with. Returns the seqno.
    pub fn commit(
        &mut self,
        ops: &[ModelOp],
        commit_ts: Timestamp,
        durability: Durability,
    ) -> Seqno {
        todo!()
    }

    /// The latest seqno (a snapshot of "everything so far").
    pub fn snapshot(&self) -> Seqno {
        todo!()
    }

    /// The newest visible version of a cell at `snapshot`, with TTL evaluated at `now`.
    pub fn get(
        &self,
        table: &str,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        snapshot: Seqno,
        now: Timestamp,
    ) -> Option<ModelCell> {
        todo!()
    }

    /// Up to `versions` visible versions of every cell of `row` in `families` (all if empty).
    pub fn read_row(
        &self,
        table: &str,
        row: &[u8],
        families: &[&str],
        versions: u32,
        snapshot: Seqno,
        now: Timestamp,
    ) -> Vec<ModelCell> {
        todo!()
    }

    /// Rows in `[start, end)` with their latest visible cells, in order.
    pub fn scan(
        &self,
        table: &str,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        families: &[&str],
        snapshot: Seqno,
        now: Timestamp,
    ) -> Vec<(Vec<u8>, Vec<ModelCell>)> {
        todo!()
    }

    /// What a crash of `kind` may lose, given each commit's acknowledged durability.
    pub fn crash_window(&self, kind: pigeonhole_io::sim::CrashKind) -> CrashWindow {
        todo!()
    }

    /// Discards every commit above `seqno` (after recovery revealed which prefix survived).
    pub fn truncate(&mut self, seqno: Seqno) {
        todo!()
    }
}
