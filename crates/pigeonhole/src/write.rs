use std::marker::PhantomData;

use pigeonhole_format::Durability;

use crate::{CellRef, Condition, Result, Table};

/// The outcome of a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitInfo {
    /// The commit's sequence number.
    pub seqno: u64,
    /// The durability level actually applied.
    pub durability: Durability,
}

/// A single-row atomic mutation under construction. Every change commits all-or-nothing,
/// across families. Errors (unknown family, oversized key) surface at commit.
#[derive(Debug)]
#[must_use = "a mutation does nothing until .commit()"]
pub struct RowMutation<'t> {
    _priv: PhantomData<&'t Table>,
}

impl RowMutation<'_> {
    /// Puts bytes at the commit timestamp.
    pub fn put(self, family: &str, qualifier: &[u8], value: &[u8]) -> Self {
        todo!()
    }

    /// Puts bytes at an explicit timestamp (event time).
    pub fn put_at(self, family: &str, qualifier: &[u8], ts: u64, value: &[u8]) -> Self {
        todo!()
    }

    /// Puts a typed `i64`.
    pub fn put_i64(self, family: &str, qualifier: &[u8], value: i64) -> Self {
        todo!()
    }

    /// Puts a typed `f64`.
    pub fn put_f64(self, family: &str, qualifier: &[u8], value: f64) -> Self {
        todo!()
    }

    /// Atomically adds `delta` to an `i64` counter without reading it (a merge operand).
    pub fn incr(self, family: &str, qualifier: &[u8], delta: i64) -> Self {
        todo!()
    }

    /// Writes an operand for the family's merge operator.
    pub fn merge(self, family: &str, qualifier: &[u8], operand: &[u8]) -> Self {
        todo!()
    }

    /// Deletes one version.
    pub fn delete_cell(self, family: &str, qualifier: &[u8], ts: u64) -> Self {
        todo!()
    }

    /// Deletes every version of a column.
    pub fn delete_column(self, family: &str, qualifier: &[u8]) -> Self {
        todo!()
    }

    /// Deletes every column of a family in this row.
    pub fn delete_family(self, family: &str) -> Self {
        todo!()
    }

    /// Deletes the whole row.
    pub fn delete_row(self) -> Self {
        todo!()
    }

    /// Overrides durability for this commit.
    pub fn durability(self, durability: Durability) -> Self {
        todo!()
    }

    /// Commits.
    pub fn commit(self) -> Result<CommitInfo> {
        todo!()
    }

    /// Commits only if `condition` holds on this row, atomically (BigTable's
    /// `check_and_mutate`; Phase 2). Returns `None` if the condition failed.
    pub fn commit_if(self, condition: &Condition) -> Result<Option<CommitInfo>> {
        todo!()
    }
}

/// A multi-row write with one durability point. Atomic across rows (two-phase commit when the
/// rows live on different shards). Errors surface at commit.
#[derive(Debug)]
pub struct WriteBatch {
    _priv: (),
}

impl WriteBatch {
    /// Puts bytes at the commit timestamp.
    pub fn put(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[u8],
    ) -> &mut Self {
        todo!()
    }

    /// Puts bytes at an explicit timestamp.
    pub fn put_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: &[u8],
    ) -> &mut Self {
        todo!()
    }

    /// Adds to an `i64` counter.
    pub fn incr(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        delta: i64,
    ) -> &mut Self {
        todo!()
    }

    /// Deletes every version of a column.
    pub fn delete_column(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> &mut Self {
        todo!()
    }

    /// Deletes a whole row.
    pub fn delete_row(&mut self, table: &Table, row: &[u8]) -> &mut Self {
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

    /// Commits with the writer default durability.
    pub fn commit(self) -> Result<CommitInfo> {
        todo!()
    }

    /// Commits with `durability` for this commit only.
    pub fn commit_with(self, durability: Durability) -> Result<CommitInfo> {
        todo!()
    }
}

/// An optimistic multi-row transaction (Phase 4): serializable for the ranges it reads.
#[derive(Debug)]
pub struct Transaction {
    _priv: (),
}

impl Transaction {
    /// Reads a cell at the transaction's snapshot and records the read.
    pub fn get(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> Result<Option<CellRef<'_>>> {
        todo!()
    }

    /// Buffers a put.
    pub fn put(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[u8],
    ) -> &mut Self {
        todo!()
    }

    /// Buffers a column delete.
    pub fn delete_column(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> &mut Self {
        todo!()
    }

    /// Commits with the writer default; fails with
    /// [`ErrorCode::Conflict`](crate::ErrorCode::Conflict) on a conflicting commit.
    pub fn commit(self) -> Result<CommitInfo> {
        todo!()
    }

    /// Commits with `durability`.
    pub fn commit_with(self, durability: Durability) -> Result<CommitInfo> {
        todo!()
    }
}
