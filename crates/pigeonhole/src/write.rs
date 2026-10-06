use std::sync::Arc;

use pigeonhole_engine::{Engine, Predicate, Txn, ValueRef};
use pigeonhole_format::Durability;

use crate::table::TableCore;
use crate::{CellRef, Condition, Error, ErrorCode, Result, Table};

/// The outcome of a commit.
///
/// ```
/// use pigeonhole::{Durability, Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let path = pigeonhole::doc_support::temp_db("commitinfo.phdb");
/// let db = Pigeonhole::open(&path, Options::default().shards(2))?;
/// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
/// let a = t.mutate(b"row").put("f", b"q", b"1").commit()?;
/// let b = t.mutate(b"row").put("f", b"q", b"2").durability(Durability::Buffered).commit()?;
/// assert!(b.seqno > a.seqno);
/// assert_eq!(a.durability, Durability::GroupSync);
/// assert_eq!(b.durability, Durability::Buffered);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitInfo {
    /// The commit's sequence number.
    pub seqno: u64,
    /// The durability level actually applied.
    pub durability: Durability,
}

impl From<pigeonhole_engine::CommitInfo> for CommitInfo {
    fn from(c: pigeonhole_engine::CommitInfo) -> Self {
        Self {
            seqno: c.seqno,
            durability: c.durability,
        }
    }
}

/// An engine batch plus the first error met while building it (builders never fail; the
/// error surfaces at commit).
#[derive(Debug, Default)]
struct Builder {
    batch: pigeonhole_engine::WriteBatch,
    error: Option<Error>,
}

impl Builder {
    /// Runs `f` against the batch with `family` resolved, unless an earlier call failed.
    fn with(
        &mut self,
        table: &TableCore,
        family: &str,
        f: impl FnOnce(
            &mut pigeonhole_engine::WriteBatch,
            pigeonhole_engine::FamilyId,
        ) -> pigeonhole_engine::Result<()>,
    ) {
        if self.error.is_some() {
            return;
        }
        let r = table
            .family_id(family)
            .and_then(|id| f(&mut self.batch, id).map_err(Error::from));
        if let Err(e) = r {
            self.error = Some(e);
        }
    }

    fn row_delete(&mut self, table: &TableCore, row: &[u8]) {
        if self.error.is_none()
            && let Err(e) = self.batch.delete_row(table.info.id, row, None)
        {
            self.error = Some(e.into());
        }
    }

    /// Refuses a table handle from another database.
    fn check_engine(&mut self, engine: &Arc<Engine>, table: &Table) -> bool {
        if self.error.is_none() && !Arc::ptr_eq(engine, &table.core.engine) {
            self.error = Some(Error::new(
                ErrorCode::InvalidArgument,
                format!("table {:?} belongs to another database", table.name()),
            ));
        }
        self.error.is_none()
    }

    fn finish(self) -> Result<pigeonhole_engine::WriteBatch> {
        match self.error {
            Some(e) => Err(e),
            None => Ok(self.batch),
        }
    }
}

/// A single-row atomic mutation under construction. Every change commits all-or-nothing,
/// across families. Errors (unknown family, oversized key) surface at commit.
///
/// ```
/// use pigeonhole::{Condition, Durability, Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let path = pigeonhole::doc_support::temp_db("rowmutation.phdb");
/// let db = Pigeonhole::open(&path, Options::default().shards(2))?;
/// let pages = db.table("pages")?
///     .family("meta", Family::default())
///     .family("links", Family::default())
///     .create_if_missing()?;
/// pages
///     .mutate(b"com.example/a")
///     .put("meta", b"status", b"200")
///     .put("links", b"com.example/b", b"")
///     .incr("meta", b"hits", 1)
///     .durability(Durability::Buffered)
///     .commit()?;
///
/// // Compare-and-set: only the first claim wins.
/// let unclaimed = Condition::Absent { family: "meta".into(), qualifier: b"owner".to_vec() };
/// let first = pages.mutate(b"com.example/a").put("meta", b"owner", b"w1").commit_if(&unclaimed)?;
/// let second = pages.mutate(b"com.example/a").put("meta", b"owner", b"w2").commit_if(&unclaimed)?;
/// assert!(first.is_some() && second.is_none());
/// assert_eq!(pages.get(b"com.example/a", "meta", b"owner")?.unwrap().value(), b"w1");
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
#[must_use = "a mutation does nothing until .commit()"]
pub struct RowMutation<'t> {
    table: &'t TableCore,
    row: Vec<u8>,
    builder: Builder,
    durability: Option<Durability>,
}

impl<'t> RowMutation<'t> {
    pub(crate) fn new(table: &'t Table, row: &[u8]) -> Self {
        Self {
            table: &table.core,
            row: row.to_vec(),
            builder: Builder::default(),
            durability: None,
        }
    }

    /// Adds one mutation of this row in `family`.
    fn op(
        mut self,
        family: &str,
        f: impl FnOnce(
            &mut pigeonhole_engine::WriteBatch,
            pigeonhole_engine::TableId,
            pigeonhole_engine::FamilyId,
            &[u8],
        ) -> pigeonhole_engine::Result<()>,
    ) -> Self {
        let (table, row) = (self.table.info.id, &self.row);
        self.builder
            .with(self.table, family, |b, fam| f(b, table, fam, row));
        self
    }
}

impl RowMutation<'_> {
    /// Puts bytes at the commit timestamp.
    pub fn put(self, family: &str, qualifier: &[u8], value: &[u8]) -> Self {
        self.op(family, |b, t, f, row| {
            b.put(t, f, row, qualifier, None, ValueRef::Bytes(value))
        })
    }

    /// Puts bytes at an explicit timestamp (event time).
    pub fn put_at(self, family: &str, qualifier: &[u8], ts: u64, value: &[u8]) -> Self {
        self.op(family, |b, t, f, row| {
            b.put(t, f, row, qualifier, Some(ts), ValueRef::Bytes(value))
        })
    }

    /// Puts a typed `i64`.
    pub fn put_i64(self, family: &str, qualifier: &[u8], value: i64) -> Self {
        self.op(family, |b, t, f, row| {
            b.put(t, f, row, qualifier, None, ValueRef::I64(value))
        })
    }

    /// Puts a typed `f64`.
    pub fn put_f64(self, family: &str, qualifier: &[u8], value: f64) -> Self {
        self.op(family, |b, t, f, row| {
            b.put(t, f, row, qualifier, None, ValueRef::F64(value))
        })
    }

    /// Atomically adds `delta` to an `i64` counter without reading it (a merge operand).
    pub fn incr(self, family: &str, qualifier: &[u8], delta: i64) -> Self {
        self.op(family, |b, t, f, row| {
            b.merge(t, f, row, qualifier, ValueRef::I64(delta))
        })
    }

    /// Writes an operand for the family's merge operator.
    pub fn merge(self, family: &str, qualifier: &[u8], operand: &[u8]) -> Self {
        self.op(family, |b, t, f, row| {
            b.merge(t, f, row, qualifier, ValueRef::Bytes(operand))
        })
    }

    /// Deletes one version.
    pub fn delete_cell(self, family: &str, qualifier: &[u8], ts: u64) -> Self {
        self.op(family, |b, t, f, row| {
            b.delete_cell(t, f, row, qualifier, ts)
        })
    }

    /// Deletes every version of a column.
    pub fn delete_column(self, family: &str, qualifier: &[u8]) -> Self {
        self.op(family, |b, t, f, row| {
            b.delete_column(t, f, row, qualifier, None)
        })
    }

    /// Deletes every column of a family in this row.
    pub fn delete_family(self, family: &str) -> Self {
        self.op(family, |b, t, f, row| b.delete_family(t, f, row, None))
    }

    /// Deletes the whole row.
    pub fn delete_row(mut self) -> Self {
        self.builder.row_delete(self.table, &self.row);
        self
    }

    /// Overrides durability for this commit.
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = Some(durability);
        self
    }

    /// Commits.
    pub fn commit(self) -> Result<CommitInfo> {
        let batch = self.builder.finish()?;
        Ok(self.table.engine.commit(batch, self.durability)?.into())
    }

    /// Commits only if `condition` holds on this row, atomically (BigTable's
    /// `check_and_mutate`; Phase 2). Returns `None` if the condition failed.
    pub fn commit_if(self, condition: &Condition) -> Result<Option<CommitInfo>> {
        let batch = self.builder.finish()?;
        let predicate = match condition {
            Condition::Exists { family, qualifier } => Predicate::Exists {
                family: self.table.family_id(family)?,
                qualifier: qualifier.clone(),
            },
            Condition::Absent { family, qualifier } => Predicate::Absent {
                family: self.table.family_id(family)?,
                qualifier: qualifier.clone(),
            },
            Condition::Value {
                family,
                qualifier,
                filter,
            } => Predicate::Value {
                family: self.table.family_id(family)?,
                qualifier: qualifier.clone(),
                predicate: filter.to_engine(),
            },
        };
        let (applied, info) = self.table.engine.check_and_mutate(
            self.table.info.id,
            &self.row,
            &predicate,
            batch,
            self.durability,
        )?;
        Ok(if applied {
            info.map(CommitInfo::from)
        } else {
            None
        })
    }
}

/// A multi-row write with one durability point. Atomic across rows (two-phase commit when the
/// rows live on different shards). Errors surface at commit.
///
/// ```
/// use pigeonhole::{Durability, Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let path = pigeonhole::doc_support::temp_db("writebatch.phdb");
/// let db = Pigeonhole::open(&path, Options::default().shards(2))?;
/// let g = db.table("g")?
///     .family("out", Family::default())
///     .family("in", Family::default())
///     .create_if_missing()?;
/// // Add the edge a -> b in both directions, atomically.
/// let mut wb = db.write_batch();
/// wb.put(&g, b"node:a", "out", b"node:b", b"")
///     .put(&g, b"node:b", "in", b"node:a", b"")
///     .incr(&g, b"node:a", "out", b"degree", 1);
/// assert_eq!(wb.len(), 3);
/// let info = wb.commit_with(Durability::Sync)?;
/// assert_eq!(info.durability, Durability::Sync);
/// assert!(g.get(b"node:b", "in", b"node:a")?.is_some());
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct WriteBatch {
    engine: Arc<Engine>,
    builder: Builder,
}

impl WriteBatch {
    pub(crate) fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            builder: Builder::default(),
        }
    }

    /// Adds one mutation in `table`'s `family`.
    fn op(
        &mut self,
        table: &Table,
        family: &str,
        f: impl FnOnce(
            &mut pigeonhole_engine::WriteBatch,
            pigeonhole_engine::TableId,
            pigeonhole_engine::FamilyId,
        ) -> pigeonhole_engine::Result<()>,
    ) -> &mut Self {
        if self.builder.check_engine(&self.engine, table) {
            let id = table.core.info.id;
            self.builder
                .with(&table.core, family, |b, fam| f(b, id, fam));
        }
        self
    }
    /// Puts bytes at the commit timestamp.
    pub fn put(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[u8],
    ) -> &mut Self {
        self.op(table, family, |b, t, f| {
            b.put(t, f, row, qualifier, None, ValueRef::Bytes(value))
        })
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
        self.op(table, family, |b, t, f| {
            b.put(t, f, row, qualifier, Some(ts), ValueRef::Bytes(value))
        })
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
        self.op(table, family, |b, t, f| {
            b.merge(t, f, row, qualifier, ValueRef::I64(delta))
        })
    }

    /// Deletes every version of a column.
    pub fn delete_column(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> &mut Self {
        self.op(table, family, |b, t, f| {
            b.delete_column(t, f, row, qualifier, None)
        })
    }

    /// Deletes a whole row.
    pub fn delete_row(&mut self, table: &Table, row: &[u8]) -> &mut Self {
        if self.builder.check_engine(&self.engine, table) {
            self.builder.row_delete(&table.core, row);
        }
        self
    }

    /// Mutations so far.
    pub fn len(&self) -> usize {
        self.builder.batch.len()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.builder.batch.is_empty()
    }

    /// Commits with the writer default durability.
    pub fn commit(self) -> Result<CommitInfo> {
        let batch = self.builder.finish()?;
        Ok(self.engine.commit(batch, None)?.into())
    }

    /// Commits with `durability` for this commit only.
    pub fn commit_with(self, durability: Durability) -> Result<CommitInfo> {
        let batch = self.builder.finish()?;
        Ok(self.engine.commit(batch, Some(durability))?.into())
    }
}

/// An optimistic multi-row transaction (Phase 4): serializable for the ranges it reads.
///
/// ```
/// use pigeonhole::{ErrorCode, Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let path = pigeonhole::doc_support::temp_db("transaction.phdb");
/// let db = Pigeonhole::open(&path, Options::default().shards(2))?;
/// let bank = db.table("bank")?.family("acct", Family::default()).create_if_missing()?;
/// bank.mutate(b"alice").put("acct", b"balance", b"10").commit()?;
///
/// let mut txn = db.transaction()?;
/// let balance = txn.get(&bank, b"alice", "acct", b"balance")?.map(|c| c.to_owned());
/// assert_eq!(balance.unwrap().value(), b"10");
/// txn.put(&bank, b"alice", "acct", b"balance", b"5")
///     .put(&bank, b"bob", "acct", b"balance", b"5");
///
/// // A concurrent write to what the transaction read makes it conflict.
/// bank.mutate(b"alice").put("acct", b"balance", b"11").commit()?;
/// assert_eq!(txn.commit().unwrap_err().code(), ErrorCode::Conflict);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Transaction {
    engine: Arc<Engine>,
    txn: Txn,
    error: Option<Error>,
}

impl Transaction {
    pub(crate) fn new(engine: Arc<Engine>, txn: Txn) -> Self {
        Self {
            engine,
            txn,
            error: None,
        }
    }

    /// Buffers one mutation in `table`'s `family`.
    fn op(
        &mut self,
        table: &Table,
        family: &str,
        f: impl FnOnce(
            &mut pigeonhole_engine::WriteBatch,
            pigeonhole_engine::TableId,
            pigeonhole_engine::FamilyId,
        ) -> pigeonhole_engine::Result<()>,
    ) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        let r = check_table(&self.engine, table).and_then(|()| {
            let fam = table.core.family_id(family)?;
            Ok(f(self.txn.batch(), table.core.info.id, fam)?)
        });
        if let Err(e) = r {
            self.error = Some(e);
        }
        self
    }

    fn finish(self, durability: Option<Durability>) -> Result<CommitInfo> {
        if let Some(e) = self.error {
            return Err(e);
        }
        Ok(self.txn.commit(durability)?.into())
    }
    /// Reads a cell at the transaction's snapshot and records the read.
    pub fn get(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> Result<Option<CellRef<'_>>> {
        check_table(&self.engine, table)?;
        let family = table.core.family_id(family)?;
        Ok(self
            .txn
            .get(table.core.info.id, family, row, qualifier)?
            .map(CellRef::owned))
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
        self.op(table, family, |b, t, f| {
            b.put(t, f, row, qualifier, None, ValueRef::Bytes(value))
        })
    }

    /// Buffers a column delete.
    pub fn delete_column(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> &mut Self {
        self.op(table, family, |b, t, f| {
            b.delete_column(t, f, row, qualifier, None)
        })
    }

    /// Commits with the writer default; fails with
    /// [`ErrorCode::Conflict`](crate::ErrorCode::Conflict) on a conflicting commit.
    pub fn commit(self) -> Result<CommitInfo> {
        self.finish(None)
    }

    /// Commits with `durability`.
    pub fn commit_with(self, durability: Durability) -> Result<CommitInfo> {
        self.finish(Some(durability))
    }
}

/// Refuses a table handle from another database.
fn check_table(engine: &Arc<Engine>, table: &Table) -> Result<()> {
    if Arc::ptr_eq(engine, &table.core.engine) {
        Ok(())
    } else {
        Err(Error::new(
            ErrorCode::InvalidArgument,
            format!("table {:?} belongs to another database", table.name()),
        ))
    }
}
