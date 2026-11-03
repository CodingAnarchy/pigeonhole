//! Databases (the writer and reader processes), batches and transactions.
//!
//! The block forms (`open`, `snapshot`, `write_batch`, `transaction`, `transact`) are Ruby
//! (`lib/pigeonhole/api.rb`, simp724's R2): a `break`, `throw` or `return` out of a block
//! runs Ruby's `ensure`, never unwinding through these frames.

use std::path::PathBuf;
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use magnus::{Error, Ruby, Value};
use pigeonhole::ErrorCode;

use crate::classes::{
    BatchState, Database, Reader, Snapshot, Table, TableBuilder, Transaction, WriteBatch,
};
use crate::convert::{Bytes, CellValue, Name, Secs, Val, lock, names};
use crate::error::{OrRb, engine, spent, with_code};
use crate::fork::ForkSafe;
use crate::gvl::without_gvl;
use crate::table::{Durability, Handle, Input, Op, aggregate_input, delete_op};

/// Applies one recorded write to a `WriteBatch` or `Transaction` (the same method names and
/// arguments: the table and row first).
macro_rules! apply {
    ($b:expr, $t:expr, $row:expr, $op:expr) => {{
        let (b, t, row) = ($b, $t, $row);
        match $op {
            Op::Put(f, q, None, Val::Bytes(v)) => b.put(t, row, f, q, v),
            Op::Put(f, q, Some(ts), Val::Bytes(v)) => b.put_at(t, row, f, q, *ts, v),
            Op::Put(f, q, None, Val::I64(n)) => b.put_i64(t, row, f, q, *n),
            Op::Put(f, q, Some(ts), Val::I64(n)) => b.put_i64_at(t, row, f, q, *ts, *n),
            Op::Put(f, q, None, Val::F64(x)) => b.put_f64(t, row, f, q, *x),
            Op::Put(f, q, Some(ts), Val::F64(x)) => b.put_f64_at(t, row, f, q, *ts, *x),
            Op::PutU64(f, q, None, n) => b.put_u64(t, row, f, q, *n),
            Op::PutU64(f, q, Some(ts), n) => b.put_u64_at(t, row, f, q, *ts, *n),
            Op::Incr(f, q, None, d) => b.incr(t, row, f, q, *d),
            Op::Incr(f, q, Some(ts), d) => b.incr_at(t, row, f, q, *ts, *d),
            Op::Aggregate(f, q, None, Input::I64(n)) => b.aggregate(t, row, f, q, *n),
            Op::Aggregate(f, q, Some(ts), Input::I64(n)) => b.aggregate_at(t, row, f, q, *ts, *n),
            Op::Aggregate(f, q, None, Input::U64(n)) => b.aggregate_u64(t, row, f, q, *n),
            Op::Aggregate(f, q, Some(ts), Input::U64(n)) => {
                b.aggregate_u64_at(t, row, f, q, *ts, *n)
            }
            Op::Aggregate(f, q, None, Input::F64(x)) => b.aggregate_f64(t, row, f, q, *x),
            Op::Aggregate(f, q, Some(ts), Input::F64(x)) => {
                b.aggregate_f64_at(t, row, f, q, *ts, *x)
            }
            Op::Aggregate(f, q, None, Input::Bytes(v)) => b.aggregate_bytes(t, row, f, q, v),
            Op::Aggregate(f, q, Some(ts), Input::Bytes(v)) => {
                b.aggregate_bytes_at(t, row, f, q, *ts, v)
            }
            Op::DeleteRow => b.delete_row(t, row),
            Op::DeleteFamily(f) => b.delete_family(t, row, f),
            Op::DeleteColumn(f, q) => b.delete_column(t, row, f, q),
            Op::DeleteCell(f, q, ts) => b.delete_cell(t, row, f, q, *ts),
        };
    }};
}

fn read<T>(l: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write<T>(l: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(std::sync::PoisonError::into_inner)
}

// ---- Opening ------------------------------------------------------------------------------

/// `Pigeonhole.native_open(path, create, shards, durability, block_cache, row_cache,
/// memtable_budget, shm_dir, read_only_fallback)`: opens (with `create`, creates) the
/// database at `path` as the writer process.
#[allow(clippy::too_many_arguments)]
pub(crate) fn open(
    ruby: &Ruby,
    path: PathBuf,
    create: bool,
    shards: Option<usize>,
    durability: Option<Durability>,
    block_cache: Option<usize>,
    row_cache: Option<usize>,
    memtable_budget: Option<u64>,
    shm_dir: Option<PathBuf>,
    read_only_fallback: Option<bool>,
) -> Result<Database, Error> {
    let mut o = pigeonhole::Options::default().create_if_missing(create);
    if let Some(n) = shards {
        o = o.shards(n);
    }
    if let Some(d) = durability {
        o = o.durability(d.into());
    }
    if let Some(n) = block_cache {
        o = o.block_cache(n);
    }
    if let Some(n) = row_cache {
        o = o.row_cache(n);
    }
    if let Some(n) = memtable_budget {
        o = o.memtable_budget(n);
    }
    if let Some(d) = shm_dir {
        o = o.shm_dir(d);
    }
    if let Some(b) = read_only_fallback {
        o = o.read_only_fallback(b);
    }
    let db = engine(ruby, move || pigeonhole::Pigeonhole::open(path, o))?;
    Ok(Database(ForkSafe::new(RwLock::new(Some(db)))))
}

/// `Pigeonhole.native_open_reader(path, shm_dir, block_cache, row_cache)`: opens the
/// database at `path` as a reader process of the writer that has it open (`NoWriter`
/// without one).
pub(crate) fn open_reader(
    ruby: &Ruby,
    path: PathBuf,
    shm_dir: Option<PathBuf>,
    block_cache: Option<usize>,
    row_cache: Option<usize>,
) -> Result<Reader, Error> {
    let mut o = pigeonhole::ReaderOptions::default();
    if let Some(d) = shm_dir {
        o = o.shm_dir(d);
    }
    if let Some(n) = block_cache {
        o = o.block_cache(n);
    }
    if let Some(n) = row_cache {
        o = o.row_cache(n);
    }
    let r = engine(ruby, move || pigeonhole::Pigeonhole::open_reader(path, o))?;
    Ok(Reader(ForkSafe::new(RwLock::new(Some(r)))))
}

/// The library's `RetryPolicy` with `transact`'s options.
fn policy(max_attempts: Option<u32>, deadline: Option<Secs>) -> pigeonhole::RetryPolicy {
    let mut p = pigeonhole::RetryPolicy::default();
    if let Some(n) = max_attempts {
        p = p.max_attempts(n);
    }
    if let Some(d) = deadline {
        p = p.deadline(d.0);
    }
    p
}

/// `Pigeonhole.native_retry_check(max_attempts, deadline)`: checks `transact`'s options.
pub(crate) fn retry_check(max_attempts: Option<u32>, deadline: Option<Secs>) {
    let _ = policy(max_attempts, deadline);
}

/// `Pigeonhole.native_retry_pause(max_attempts, deadline, code_num, attempt, elapsed)`: how
/// long `transact` sleeps before its next attempt after attempt `attempt` failed with the
/// code numbered `code_num` (`Conflict` or `Busy`), `elapsed` seconds after it started; `nil`
/// to give up and re-raise. The library's `RetryPolicy`, as Python's `transact` uses it.
pub(crate) fn retry_pause(
    max_attempts: Option<u32>,
    deadline: Option<Secs>,
    code_num: u32,
    attempt: u32,
    elapsed: f64,
) -> Option<f64> {
    let code = if code_num == ErrorCode::Conflict as u32 {
        ErrorCode::Conflict
    } else if code_num == ErrorCode::Busy as u32 {
        ErrorCode::Busy
    } else {
        return None;
    };
    let p = policy(max_attempts, deadline);
    if !p.retries_code(code) {
        return None;
    }
    let elapsed = Duration::try_from_secs_f64(elapsed).unwrap_or(Duration::ZERO);
    p.next_pause(code, attempt, elapsed)
        .map(|d| d.as_secs_f64())
}

// ---- Database -----------------------------------------------------------------------------

impl Database {
    fn db(&self, ruby: &Ruby) -> Result<pigeonhole::Pigeonhole, Error> {
        read(self.0.get(ruby)?)
            .clone()
            .ok_or_else(|| with_code(ruby, ErrorCode::Closed, "the database is closed"))
    }

    /// `table(name)`: a `TableBuilder`.
    pub(crate) fn table(ruby: &Ruby, rb_self: &Self, name: Name) -> Result<TableBuilder, Error> {
        Ok(TableBuilder::new(rb_self.db(ruby)?, name.0))
    }

    /// The table names.
    pub(crate) fn tables(ruby: &Ruby, rb_self: &Self) -> Result<Vec<String>, Error> {
        let db = rb_self.db(ruby)?;
        without_gvl(ruby, move || db.tables())
    }

    /// Drops a table and its data.
    pub(crate) fn drop_table(ruby: &Ruby, rb_self: &Self, name: Name) -> Result<(), Error> {
        let db = rb_self.db(ruby)?;
        engine(ruby, move || db.drop_table(&name.0))
    }

    /// `native_snapshot(durable)`.
    pub(crate) fn snapshot(ruby: &Ruby, rb_self: &Self, durable: bool) -> Result<Snapshot, Error> {
        let db = rb_self.db(ruby)?;
        engine(ruby, move || {
            if durable {
                db.durable_snapshot()
            } else {
                db.snapshot()
            }
        })
        .map(Snapshot::new)
    }

    /// `native_write_batch`.
    pub(crate) fn write_batch(ruby: &Ruby, rb_self: &Self) -> Result<WriteBatch, Error> {
        Ok(WriteBatch(ForkSafe::new(BatchState {
            db: rb_self.db(ruby)?,
            ops: Mutex::new(Some(Vec::new())),
        })))
    }

    /// `native_transaction`.
    pub(crate) fn transaction(ruby: &Ruby, rb_self: &Self) -> Result<Transaction, Error> {
        let db = rb_self.db(ruby)?;
        engine(ruby, move || db.transaction())
            .map(|t| Transaction(ForkSafe::new(Mutex::new(Some(t)))))
    }

    /// Flushes every memtable to the file.
    pub(crate) fn flush(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        let db = rb_self.db(ruby)?;
        engine(ruby, move || db.flush())
    }

    /// Compacts every table fully.
    pub(crate) fn compact(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        let db = rb_self.db(ruby)?;
        engine(ruby, move || db.compact())
    }

    /// Closes the database: flushes, checkpoints and releases the writer lock. Using it, or
    /// its tables, afterwards raises `Closed`. Closing twice does nothing. In a forked child
    /// it closes nothing: the handle is the parent's, and is forgotten.
    pub(crate) fn close(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        if rb_self.0.forked() {
            // `ForkSafe::drop` forgets the parent's handle; nothing to do now.
            return Ok(());
        }
        let db = write(rb_self.0.get(ruby)?).take();
        match db {
            Some(db) => engine(ruby, move || db.close()),
            None => Ok(()),
        }
    }

    /// Whether `close` was called.
    pub(crate) fn is_closed(ruby: &Ruby, rb_self: &Self) -> Result<bool, Error> {
        Ok(read(rb_self.0.get(ruby)?).is_none())
    }
}

// ---- Reader -------------------------------------------------------------------------------

impl Reader {
    fn db(&self, ruby: &Ruby) -> Result<pigeonhole::PigeonholeReader, Error> {
        read(self.0.get(ruby)?)
            .clone()
            .ok_or_else(|| with_code(ruby, ErrorCode::Closed, "the reader is closed"))
    }

    /// `table(name)`: a table, for reads.
    pub(crate) fn table(ruby: &Ruby, rb_self: &Self, name: Name) -> Result<Table, Error> {
        let db = rb_self.db(ruby)?;
        engine(ruby, move || db.table(&name.0)).map(|t| Table(ForkSafe::new(Handle::Reader(t))))
    }

    /// The table names.
    pub(crate) fn tables(ruby: &Ruby, rb_self: &Self) -> Result<Vec<String>, Error> {
        let db = rb_self.db(ruby)?;
        without_gvl(ruby, move || db.tables())
    }

    /// `native_snapshot(durable)`.
    pub(crate) fn snapshot(ruby: &Ruby, rb_self: &Self, durable: bool) -> Result<Snapshot, Error> {
        let db = rb_self.db(ruby)?;
        engine(ruby, move || {
            if durable {
                db.durable_snapshot()
            } else {
                db.snapshot()
            }
        })
        .map(Snapshot::new)
    }

    /// Closes the reader. Closing twice does nothing; in a forked child it closes nothing.
    pub(crate) fn close(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        if rb_self.0.forked() {
            return Ok(());
        }
        let r = write(rb_self.0.get(ruby)?).take();
        without_gvl(ruby, move || drop(r))
    }

    /// Whether `close` was called.
    pub(crate) fn is_closed(ruby: &Ruby, rb_self: &Self) -> Result<bool, Error> {
        Ok(read(rb_self.0.get(ruby)?).is_none())
    }
}

// ---- Batch and transaction writes ---------------------------------------------------------

/// A handle that records writes against a table and row: `WriteBatch` or `Transaction`.
pub(crate) trait Record {
    /// Records `op` on `table`'s `row`.
    fn record(
        ruby: &Ruby,
        rb_self: &Self,
        table: &Table,
        row: Vec<u8>,
        op: Op,
    ) -> Result<(), Error>;
}

/// `native_put(table, row, family, qualifier, value, ts)`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn w_put<T: Record>(
    ruby: &Ruby,
    rb_self: &T,
    table: &Table,
    row: Bytes,
    family: Name,
    qualifier: Bytes,
    value: CellValue,
    ts: Option<u64>,
) -> Result<(), Error> {
    T::record(
        ruby,
        rb_self,
        table,
        row.0,
        Op::Put(family.0, qualifier.0, ts, value.0),
    )
}

/// `native_put_u64(table, row, family, qualifier, value, ts)`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn w_put_u64<T: Record>(
    ruby: &Ruby,
    rb_self: &T,
    table: &Table,
    row: Bytes,
    family: Name,
    qualifier: Bytes,
    value: u64,
    ts: Option<u64>,
) -> Result<(), Error> {
    T::record(
        ruby,
        rb_self,
        table,
        row.0,
        Op::PutU64(family.0, qualifier.0, ts, value),
    )
}

/// `native_incr(table, row, family, qualifier, delta, ts)`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn w_incr<T: Record>(
    ruby: &Ruby,
    rb_self: &T,
    table: &Table,
    row: Bytes,
    family: Name,
    qualifier: Bytes,
    delta: i64,
    ts: Option<u64>,
) -> Result<(), Error> {
    T::record(
        ruby,
        rb_self,
        table,
        row.0,
        Op::Incr(family.0, qualifier.0, ts, delta),
    )
}

/// `native_aggregate(table, row, family, qualifier, value, ts)`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn w_aggregate<T: Record>(
    ruby: &Ruby,
    rb_self: &T,
    table: &Table,
    row: Bytes,
    family: Name,
    qualifier: Bytes,
    value: Value,
    ts: Option<u64>,
) -> Result<(), Error> {
    let input = aggregate_input(ruby, table.writer(ruby)?, &family.0, value)?;
    T::record(
        ruby,
        rb_self,
        table,
        row.0,
        Op::Aggregate(family.0, qualifier.0, ts, input),
    )
}

/// `native_delete(table, row, family, qualifier, ts)`.
pub(crate) fn w_delete<T: Record>(
    ruby: &Ruby,
    rb_self: &T,
    table: &Table,
    row: Bytes,
    family: Option<Name>,
    qualifier: Option<Bytes>,
    ts: Option<u64>,
) -> Result<(), Error> {
    let op = delete_op(ruby, family, qualifier, ts)?;
    T::record(ruby, rb_self, table, row.0, op)
}

// ---- WriteBatch ---------------------------------------------------------------------------

const BATCH_SPENT: &str = "the batch was already committed or discarded";

impl Record for WriteBatch {
    fn record(
        ruby: &Ruby,
        rb_self: &Self,
        table: &Table,
        row: Vec<u8>,
        op: Op,
    ) -> Result<(), Error> {
        let t = table.writer(ruby)?.clone();
        match lock(&rb_self.0.get(ruby)?.ops).as_mut() {
            Some(ops) => {
                ops.push((t, row, op));
                Ok(())
            }
            None => Err(spent(ruby, BATCH_SPENT)),
        }
    }
}

impl WriteBatch {
    /// Writes recorded so far (0 once committed or discarded).
    pub(crate) fn size(ruby: &Ruby, rb_self: &Self) -> Result<usize, Error> {
        Ok(lock(&rb_self.0.get(ruby)?.ops).as_ref().map_or(0, Vec::len))
    }

    /// `native_commit(durability)`: commits every write atomically; returns the commit's
    /// sequence number.
    pub(crate) fn commit(
        ruby: &Ruby,
        rb_self: &Self,
        durability: Option<Durability>,
    ) -> Result<u64, Error> {
        let state = rb_self.0.get(ruby)?;
        // The writes are taken inside the call: if an interrupt was pending and the call
        // didn't run, the batch keeps them.
        let r = without_gvl(ruby, move || {
            let ops = lock(&state.ops).take()?;
            let mut b = state.db.write_batch();
            for (t, row, op) in &ops {
                apply!(&mut b, t, row, op);
            }
            Some(match durability {
                Some(d) => b.commit_with(d.into()),
                None => b.commit(),
            })
        })?;
        match r {
            Some(r) => Ok(r.or_rb(ruby)?.seqno),
            None => Err(spent(ruby, BATCH_SPENT)),
        }
    }

    /// `native_discard`: drops the recorded writes (nothing if already spent).
    pub(crate) fn discard(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        lock(&rb_self.0.get(ruby)?.ops).take();
        Ok(())
    }

    /// `native_open?`: whether it can still record and commit.
    pub(crate) fn is_open(ruby: &Ruby, rb_self: &Self) -> Result<bool, Error> {
        Ok(lock(&rb_self.0.get(ruby)?.ops).is_some())
    }
}

// ---- Transaction --------------------------------------------------------------------------

const TXN_SPENT: &str = "the transaction was already committed or rolled back";

// The transaction's mutex is taken only inside `without_gvl` closures, and no guard outlives
// the closure that took it (simp724 on #1399). So a thread never waits on the mutex while
// holding the GVL, and a thread holding the mutex never needs the GVL: the two locks can't
// be held in opposite orders.

impl Record for Transaction {
    fn record(
        ruby: &Ruby,
        rb_self: &Self,
        table: &Table,
        row: Vec<u8>,
        op: Op,
    ) -> Result<(), Error> {
        let t = table.writer(ruby)?.clone();
        let m = rb_self.0.get(ruby)?;
        let recorded = without_gvl(ruby, move || match lock(m).as_mut() {
            Some(txn) => {
                apply!(txn, &t, &row, &op);
                true
            }
            None => false,
        })?;
        if recorded {
            Ok(())
        } else {
            Err(spent(ruby, TXN_SPENT))
        }
    }
}

impl Transaction {
    /// `get(table, row, family, qualifier)`: the newest version of one cell, as of the
    /// transaction's snapshot (and its own writes). The cell is part of what commit checks.
    pub(crate) fn get(
        ruby: &Ruby,
        rb_self: &Self,
        table: &Table,
        row: Bytes,
        family: Name,
        qualifier: Bytes,
    ) -> Result<Option<crate::classes::Cell>, Error> {
        let t = table.writer(ruby)?.clone();
        let m = rb_self.0.get(ruby)?;
        let (row, family, q) = (row.0, family.0, qualifier.0);
        let r = without_gvl(ruby, move || {
            lock(m).as_mut().map(|txn| {
                txn.get(&t, &row, &family, &q)
                    .map(|c| c.map(|c| c.to_owned()))
            })
        })?;
        match r {
            Some(r) => Ok(r.or_rb(ruby)?.map(crate::classes::Cell)),
            None => Err(spent(ruby, TXN_SPENT)),
        }
    }

    /// `get_row(table, row, families)`: a row's cells in `families`, as of the
    /// transaction's snapshot. The families read are part of what commit checks.
    pub(crate) fn get_row(
        ruby: &Ruby,
        rb_self: &Self,
        table: &Table,
        row: Bytes,
        families: magnus::RArray,
    ) -> Result<Option<crate::classes::Row>, Error> {
        let t = table.writer(ruby)?.clone();
        let families = names(Some(families))?.unwrap_or_default();
        let m = rb_self.0.get(ruby)?;
        let row = row.0;
        let r = without_gvl(ruby, move || {
            lock(m).as_mut().map(|txn| {
                let names: Vec<&str> = families.iter().map(String::as_str).collect();
                txn.get_row(&t, &row, &names)
                    .map(|r| r.map(|r| r.to_owned()))
            })
        })?;
        match r {
            Some(r) => Ok(r.or_rb(ruby)?.map(crate::classes::Row)),
            None => Err(spent(ruby, TXN_SPENT)),
        }
    }

    /// `native_commit(durability)`: commits; `Conflict` if a cell or family it read changed
    /// since its snapshot.
    pub(crate) fn commit(
        ruby: &Ruby,
        rb_self: &Self,
        durability: Option<Durability>,
    ) -> Result<u64, Error> {
        let m = rb_self.0.get(ruby)?;
        let r = without_gvl(ruby, move || {
            let txn = lock(m).take();
            txn.map(|txn| match durability {
                Some(d) => txn.commit_with(d.into()),
                None => txn.commit(),
            })
        })?;
        match r {
            Some(r) => Ok(r.or_rb(ruby)?.seqno),
            None => Err(spent(ruby, TXN_SPENT)),
        }
    }

    /// Drops the transaction's writes; raises if it was already committed or rolled back.
    pub(crate) fn rollback(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        let m = rb_self.0.get(ruby)?;
        let had = without_gvl(ruby, move || lock(m).take().is_some())?;
        if had {
            Ok(())
        } else {
            Err(spent(ruby, TXN_SPENT))
        }
    }

    /// `native_discard`: drops the transaction's writes, if it is still open.
    pub(crate) fn discard(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        let m = rb_self.0.get(ruby)?;
        without_gvl(ruby, move || drop(lock(m).take()))
    }

    /// `native_open?`: whether it can still be used.
    pub(crate) fn is_open(ruby: &Ruby, rb_self: &Self) -> Result<bool, Error> {
        let m = rb_self.0.get(ruby)?;
        without_gvl(ruby, move || lock(m).is_some())
    }
}
