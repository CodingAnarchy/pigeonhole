use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pigeonhole_compaction::ValuePredicate;
use pigeonhole_format::value::ValueRef;
use pigeonhole_format::{Durability, FamilyId, TableId, Timestamp};

use crate::{CellData, CommitInfo};

/// A multi-row write with one durability point. Mutations are encoded on insert in the WAL
/// batch encoding (`pigeonhole_format::wal::BatchBuilder`), so commit never re-encodes.
/// Commit is atomic per row always; across rows it is atomic too (two-phase commit across
/// shards when rows span shards).
#[derive(Debug, Default, Clone)]
pub struct WriteBatch {
    _priv: (),
}

impl WriteBatch {
    /// An empty batch.
    pub fn new() -> Self {
        todo!()
    }

    /// Puts a value. `ts = None` uses the commit timestamp.
    pub fn put(
        &mut self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
        ts: Option<Timestamp>,
        value: ValueRef<'_>,
    ) -> crate::Result<()> {
        todo!()
    }

    /// Writes a merge operand.
    pub fn merge(
        &mut self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
        operand: ValueRef<'_>,
    ) -> crate::Result<()> {
        todo!()
    }

    /// Deletes one version.
    pub fn delete_cell(
        &mut self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
        ts: Timestamp,
    ) -> crate::Result<()> {
        todo!()
    }

    /// Deletes all versions of a column at or below `ts` (`None`: the commit timestamp).
    pub fn delete_column(
        &mut self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
        ts: Option<Timestamp>,
    ) -> crate::Result<()> {
        todo!()
    }

    /// Deletes a family within a row at or below `ts`.
    pub fn delete_family(
        &mut self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        ts: Option<Timestamp>,
    ) -> crate::Result<()> {
        todo!()
    }

    /// Deletes a whole row: one family marker per family of the table (decision D10).
    pub fn delete_row(
        &mut self,
        table: TableId,
        row: &[u8],
        ts: Option<Timestamp>,
    ) -> crate::Result<()> {
        todo!()
    }

    /// Mutations so far.
    pub fn len(&self) -> usize {
        todo!()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        todo!()
    }

    /// Clears for reuse, keeping allocations.
    pub fn clear(&mut self) {
        todo!()
    }
}

/// A condition for `Engine::check_and_mutate`, evaluated on the owning shard at the latest
/// seqno.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// The column has a visible version.
    Exists {
        /// Family.
        family: FamilyId,
        /// Qualifier.
        qualifier: Vec<u8>,
    },
    /// The column has no visible version.
    Absent {
        /// Family.
        family: FamilyId,
        /// Qualifier.
        qualifier: Vec<u8>,
    },
    /// The column's latest value matches.
    Value {
        /// Family.
        family: FamilyId,
        /// Qualifier.
        qualifier: Vec<u8>,
        /// Condition.
        predicate: ValuePredicate,
    },
}

/// A commit submitted to its shard(s) and not yet resolved. The basis of sync commits
/// ([`PendingCommit::wait`]), async commits (`.await`) and ticket-style commits. Dropping it
/// does not roll back: the commit lands or fails atomically either way.
#[derive(Debug)]
#[must_use = "dropping a pending commit does not cancel it, but its result is lost"]
pub struct PendingCommit {
    _priv: (),
}

impl PendingCommit {
    /// Blocks until the commit meets its durability level.
    pub fn wait(self) -> crate::Result<CommitInfo> {
        todo!()
    }
}

impl Future for PendingCommit {
    type Output = crate::Result<CommitInfo>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}

/// An optimistic multi-row transaction (Phase 4). Reads record the `(row, family)` ranges
/// they touch; at commit, each participant shard checks its tablets for conflicting commits
/// since the snapshot during PREPARE, and any conflict aborts with `Error::Conflict`.
#[derive(Debug)]
pub struct Txn {
    _priv: (),
}

impl Txn {
    /// The transaction's snapshot seqno.
    pub fn snapshot(&self) -> &crate::Snapshot {
        todo!()
    }

    /// Reads a cell and records the read.
    pub fn get(
        &mut self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
    ) -> crate::Result<Option<CellData>> {
        todo!()
    }

    /// The buffered writes.
    pub fn batch(&mut self) -> &mut WriteBatch {
        todo!()
    }

    /// Validates and commits.
    pub fn commit(self, durability: Option<Durability>) -> crate::Result<CommitInfo> {
        todo!()
    }
}
