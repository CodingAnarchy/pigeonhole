use std::sync::Arc;

use pigeonhole_engine::{FamilyId, Predicate, TableId, Txn, ValueRef};
use pigeonhole_format::Durability;
use pigeonhole_format::key::MAX_KEY_PART;

use crate::db::Db;
use crate::table::TableCore;
use crate::{CellRef, Condition, Error, ErrorCode, Result, Table};

type EngineBatch = pigeonhole_engine::WriteBatch;

/// The outcome of a commit.
///
/// ```
/// use pigeonhole::{Durability, Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(2))?;
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

/// The lengths of one mutation's parts, for size errors.
#[derive(Debug, Clone, Copy)]
struct Sizes {
    row: usize,
    qualifier: usize,
    value: usize,
}

impl Sizes {
    fn new(row: &[u8], qualifier: &[u8], value: usize) -> Self {
        Self {
            row: row.len(),
            qualifier: qualifier.len(),
            value,
        }
    }
}

/// An engine error with the sizes and limits filled in for `KeyTooLarge` and
/// `ValueTooLarge` (the engine's variants carry neither).
fn size_error(e: pigeonhole_engine::Error, sizes: Sizes, max_value: usize) -> Error {
    use pigeonhole_engine::Error as E;
    match e {
        E::KeyTooLarge => {
            let (what, len) = if sizes.row > MAX_KEY_PART {
                ("row key", sizes.row)
            } else {
                ("qualifier", sizes.qualifier)
            };
            Error::new(
                ErrorCode::KeyTooLarge,
                format!("{what} of {len} bytes exceeds the limit of {MAX_KEY_PART} bytes"),
            )
        }
        E::ValueTooLarge => value_too_large(sizes.value, max_value),
        e => e.into(),
    }
}

fn value_too_large(len: usize, max_value: usize) -> Error {
    Error::new(
        ErrorCode::ValueTooLarge,
        format!(
            "value of {len} bytes exceeds the limit of {max_value} bytes (the smallest of the \
             WAL segment payload, 64 MiB and half a shard's memtable arena; decision D16)"
        ),
    )
}

/// An engine batch plus the first error met while building it (builders never fail; the
/// error surfaces at commit).
#[derive(Debug, Default)]
struct Builder {
    batch: EngineBatch,
    error: Option<Error>,
    /// The largest value added, for a `ValueTooLarge` message at commit.
    largest_value: usize,
}

impl Builder {
    /// Runs `f` against the batch with `family` resolved, unless an earlier call failed.
    fn with(
        &mut self,
        table: &TableCore,
        family: &str,
        sizes: Sizes,
        f: impl FnOnce(&mut EngineBatch, FamilyId) -> pigeonhole_engine::Result<()>,
    ) {
        if self.error.is_some() {
            return;
        }
        self.largest_value = self.largest_value.max(sizes.value);
        let max_value = table.db.max_value;
        let r = table
            .family_id(family)
            .and_then(|id| f(&mut self.batch, id).map_err(|e| size_error(e, sizes, max_value)));
        if let Err(e) = r {
            self.error = Some(e);
        }
    }

    fn row_delete(&mut self, table: &TableCore, row: &[u8]) {
        if self.error.is_none()
            && let Err(e) = self.batch.delete_row(table.info.id, row, None)
        {
            self.error = Some(size_error(e, Sizes::new(row, &[], 0), 0));
        }
    }

    /// Refuses a table handle from another database.
    fn check_table(&mut self, db: &Arc<Db>, table: &Table) -> bool {
        if self.error.is_none()
            && let Err(e) = check_table(db, table)
        {
            self.error = Some(e);
        }
        self.error.is_none()
    }

    /// Commits through `db`, reporting the first builder error instead if there was one.
    fn commit(self, db: &Db, durability: Option<Durability>) -> Result<CommitInfo> {
        if let Some(e) = self.error {
            return Err(e);
        }
        let largest = self.largest_value;
        db.engine
            .commit(self.batch, durability)
            .map(CommitInfo::from)
            .map_err(|e| commit_error(e, largest, db.max_value))
    }
}

/// A commit's error: `ValueTooLarge` names the largest value of the commit.
fn commit_error(e: pigeonhole_engine::Error, largest_value: usize, max_value: usize) -> Error {
    match e {
        pigeonhole_engine::Error::ValueTooLarge => value_too_large(largest_value, max_value),
        e => e.into(),
    }
}

/// Refuses a table handle from another database.
fn check_table(db: &Arc<Db>, table: &Table) -> Result<()> {
    if Arc::ptr_eq(db, &table.core.db) {
        Ok(())
    } else {
        Err(Error::new(
            ErrorCode::InvalidArgument,
            format!("table {:?} belongs to another database", table.name()),
        ))
    }
}

/// A single-row atomic mutation under construction. Every change commits all-or-nothing,
/// across families. Errors (unknown family, oversized key) surface at commit.
///
/// ```
/// use pigeonhole::{Condition, Durability, Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(2))?;
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

    /// Adds one mutation of this row in `family` (`qualifier` and `value` only size errors).
    fn op(
        mut self,
        family: &str,
        qualifier: &[u8],
        value: usize,
        f: impl FnOnce(&mut EngineBatch, TableId, FamilyId, &[u8]) -> pigeonhole_engine::Result<()>,
    ) -> Self {
        let (table, row) = (self.table.info.id, &self.row);
        let sizes = Sizes::new(row, qualifier, value);
        self.builder
            .with(self.table, family, sizes, |b, fam| f(b, table, fam, row));
        self
    }
}

impl RowMutation<'_> {
    /// Puts bytes at the commit timestamp.
    pub fn put(self, family: &str, qualifier: &[u8], value: &[u8]) -> Self {
        self.op(family, qualifier, value.len(), |b, t, f, row| {
            b.put(t, f, row, qualifier, None, ValueRef::Bytes(value))
        })
    }

    /// Puts bytes at an explicit timestamp (event time).
    pub fn put_at(self, family: &str, qualifier: &[u8], ts: u64, value: &[u8]) -> Self {
        self.op(family, qualifier, value.len(), |b, t, f, row| {
            b.put(t, f, row, qualifier, Some(ts), ValueRef::Bytes(value))
        })
    }

    /// Puts a typed `i64`.
    pub fn put_i64(self, family: &str, qualifier: &[u8], value: i64) -> Self {
        self.op(family, qualifier, 8, |b, t, f, row| {
            b.put(t, f, row, qualifier, None, ValueRef::I64(value))
        })
    }

    /// Puts a typed `f64`.
    pub fn put_f64(self, family: &str, qualifier: &[u8], value: f64) -> Self {
        self.op(family, qualifier, 8, |b, t, f, row| {
            b.put(t, f, row, qualifier, None, ValueRef::F64(value))
        })
    }

    /// Atomically adds `delta` to an `i64` counter without reading it (a merge operand).
    pub fn incr(self, family: &str, qualifier: &[u8], delta: i64) -> Self {
        self.op(family, qualifier, 8, |b, t, f, row| {
            b.merge(t, f, row, qualifier, ValueRef::I64(delta))
        })
    }

    /// Writes an untyped operand for the family's merge operator (custom operators, Phase 2).
    /// The built-in `i64` add takes typed operands only: use `incr`, or the read fails with
    /// [`ErrorCode::MergeFailed`](crate::ErrorCode::MergeFailed).
    pub fn merge(self, family: &str, qualifier: &[u8], operand: &[u8]) -> Self {
        self.op(family, qualifier, operand.len(), |b, t, f, row| {
            b.merge(t, f, row, qualifier, ValueRef::Bytes(operand))
        })
    }

    /// Deletes one version.
    pub fn delete_cell(self, family: &str, qualifier: &[u8], ts: u64) -> Self {
        self.op(family, qualifier, 0, |b, t, f, row| {
            b.delete_cell(t, f, row, qualifier, ts)
        })
    }

    /// Deletes every version of a column.
    pub fn delete_column(self, family: &str, qualifier: &[u8]) -> Self {
        self.op(family, qualifier, 0, |b, t, f, row| {
            b.delete_column(t, f, row, qualifier, None)
        })
    }

    /// Deletes every column of a family in this row.
    pub fn delete_family(self, family: &str) -> Self {
        self.op(family, &[], 0, |b, t, f, row| {
            b.delete_family(t, f, row, None)
        })
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
        self.builder.commit(&self.table.db, self.durability)
    }

    /// Commits only if `condition` holds on this row, atomically (BigTable's
    /// `check_and_mutate`; Phase 2). Returns `None` if the condition failed.
    pub fn commit_if(self, condition: &Condition) -> Result<Option<CommitInfo>> {
        if let Some(e) = self.builder.error {
            return Err(e);
        }
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
        let db = &self.table.db;
        let largest = self.builder.largest_value;
        let (applied, info) = db
            .engine
            .check_and_mutate(
                self.table.info.id,
                &self.row,
                &predicate,
                self.builder.batch,
                self.durability,
            )
            .map_err(|e| commit_error(e, largest, db.max_value))?;
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
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(2))?;
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
    db: Arc<Db>,
    builder: Builder,
}

impl WriteBatch {
    pub(crate) fn new(db: Arc<Db>) -> Self {
        Self {
            db,
            builder: Builder::default(),
        }
    }

    /// Adds one mutation of `row` in `table`'s `family` (`qualifier` and `value` only size
    /// errors).
    fn op(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: usize,
        f: impl FnOnce(&mut EngineBatch, TableId, FamilyId) -> pigeonhole_engine::Result<()>,
    ) -> &mut Self {
        if self.builder.check_table(&self.db, table) {
            let id = table.core.info.id;
            let sizes = Sizes::new(row, qualifier, value);
            self.builder
                .with(&table.core, family, sizes, |b, fam| f(b, id, fam));
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
        self.op(table, row, family, qualifier, value.len(), |b, t, f| {
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
        self.op(table, row, family, qualifier, value.len(), |b, t, f| {
            b.put(t, f, row, qualifier, Some(ts), ValueRef::Bytes(value))
        })
    }

    /// Puts a typed `i64`.
    pub fn put_i64(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: i64,
    ) -> &mut Self {
        self.op(table, row, family, qualifier, 8, |b, t, f| {
            b.put(t, f, row, qualifier, None, ValueRef::I64(value))
        })
    }

    /// Puts a typed `f64`.
    pub fn put_f64(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: f64,
    ) -> &mut Self {
        self.op(table, row, family, qualifier, 8, |b, t, f| {
            b.put(t, f, row, qualifier, None, ValueRef::F64(value))
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
        self.op(table, row, family, qualifier, 8, |b, t, f| {
            b.merge(t, f, row, qualifier, ValueRef::I64(delta))
        })
    }

    /// Writes an untyped operand for the family's merge operator (custom operators, Phase 2).
    /// The built-in `i64` add takes typed operands only: use `incr`, or the read fails with
    /// [`ErrorCode::MergeFailed`](crate::ErrorCode::MergeFailed).
    pub fn merge(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        operand: &[u8],
    ) -> &mut Self {
        self.op(table, row, family, qualifier, operand.len(), |b, t, f| {
            b.merge(t, f, row, qualifier, ValueRef::Bytes(operand))
        })
    }

    /// Deletes one version.
    pub fn delete_cell(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
    ) -> &mut Self {
        self.op(table, row, family, qualifier, 0, |b, t, f| {
            b.delete_cell(t, f, row, qualifier, ts)
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
        self.op(table, row, family, qualifier, 0, |b, t, f| {
            b.delete_column(t, f, row, qualifier, None)
        })
    }

    /// Deletes every column of a family in a row.
    pub fn delete_family(&mut self, table: &Table, row: &[u8], family: &str) -> &mut Self {
        self.op(table, row, family, &[], 0, |b, t, f| {
            b.delete_family(t, f, row, None)
        })
    }

    /// Deletes a whole row.
    pub fn delete_row(&mut self, table: &Table, row: &[u8]) -> &mut Self {
        if self.builder.check_table(&self.db, table) {
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
        self.builder.commit(&self.db, None)
    }

    /// Commits with `durability` for this commit only.
    pub fn commit_with(self, durability: Durability) -> Result<CommitInfo> {
        self.builder.commit(&self.db, Some(durability))
    }
}

/// An optimistic multi-row transaction (Phase 4): serializable for the ranges it reads.
///
/// ```
/// use pigeonhole::{ErrorCode, Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(2))?;
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
    db: Arc<Db>,
    txn: Txn,
    error: Option<Error>,
    largest_value: usize,
}

impl Transaction {
    pub(crate) fn new(db: Arc<Db>, txn: Txn) -> Self {
        Self {
            db,
            txn,
            error: None,
            largest_value: 0,
        }
    }

    /// Buffers one mutation of `row` in `table`'s `family`.
    fn op(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: usize,
        f: impl FnOnce(&mut EngineBatch, TableId, FamilyId) -> pigeonhole_engine::Result<()>,
    ) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        self.largest_value = self.largest_value.max(value);
        let sizes = Sizes::new(row, qualifier, value);
        let max_value = self.db.max_value;
        let r = check_table(&self.db, table).and_then(|()| {
            let fam = table.core.family_id(family)?;
            f(self.txn.batch(), table.core.info.id, fam)
                .map_err(|e| size_error(e, sizes, max_value))
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
        self.db.check_open()?;
        let (largest, max) = (self.largest_value, self.db.max_value);
        self.txn
            .commit(durability)
            .map(CommitInfo::from)
            .map_err(|e| commit_error(e, largest, max))
    }

    /// Reads a cell at the transaction's snapshot and records the read.
    pub fn get(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> Result<Option<CellRef<'_>>> {
        check_table(&self.db, table)?;
        self.db.check_open()?;
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
        self.op(table, row, family, qualifier, value.len(), |b, t, f| {
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
        self.op(table, row, family, qualifier, 0, |b, t, f| {
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
