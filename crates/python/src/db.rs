//! Databases (the writer and reader processes), batches and transactions.

use crate::Maybe;
use std::path::PathBuf;
use std::sync::{Mutex, RwLock};
use std::time::Instant;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::convert::{self, Val};
use crate::error::{self, OrPy, with_code};
use crate::read::{Cell, Row, Snapshot};
use crate::table::{
    Durability, Handle, Input, Op, Table, TableBuilder, aggregate_input, delete_op,
};

/// Applies one recorded write to a `WriteBatch` or `Transaction` (same method names and
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
            Op::PutVector(f, q, ts, v) => {
                use crate::vector::Vector;
                match (v, ts) {
                    (Vector::F32(x), None) => b.put_vector_f32(t, row, f, q, x),
                    (Vector::F32(x), Some(ts)) => b.put_vector_f32_at(t, row, f, q, *ts, x),
                    (Vector::I8(x), None) => b.put_vector_i8(t, row, f, q, x),
                    (Vector::I8(x), Some(ts)) => b.put_vector_i8_at(t, row, f, q, *ts, x),
                    (Vector::Bits(x), None) => b.put_vector_bits(t, row, f, q, x),
                    (Vector::Bits(x), Some(ts)) => b.put_vector_bits_at(t, row, f, q, *ts, x),
                }
            }
            Op::DeleteRow => b.delete_row(t, row),
            Op::DeleteFamily(f) => b.delete_family(t, row, f),
            Op::DeleteColumn(f, q) => b.delete_column(t, row, f, q),
            Op::DeleteCell(f, q, ts) => b.delete_cell(t, row, f, q, *ts),
        };
    }};
}

/// The writes `WriteBatch` and `Transaction` share, recorded against a table and row.
macro_rules! write_methods {
    ($ty:ident { $($extra:tt)* }) => {
        #[pymethods]
        impl $ty {
            $($extra)*

            /// Puts a cell in `table`'s `row`: bytes or str, int (`i64`) or float (`f64`).
            #[pyo3(signature = (table, row, family, qualifier, value, *, ts=None))]
            #[allow(clippy::too_many_arguments)]
            fn put<'py>(
                slf: &Bound<'py, Self>,
                table: &Table,
                row: &Bound<'_, PyAny>,
                family: String,
                qualifier: &Bound<'_, PyAny>,
                value: &Bound<'_, PyAny>,
                ts: Option<u64>,
            ) -> PyResult<Bound<'py, Self>> {
                let op = Op::Put(family, convert::bytes(qualifier)?, ts, convert::value(value)?);
                slf.get().record(table, convert::bytes(row)?, op)?;
                Ok(slf.clone())
            }

            /// Puts a cell holding an unsigned 64-bit int.
            #[pyo3(signature = (table, row, family, qualifier, value, *, ts=None))]
            #[allow(clippy::too_many_arguments)]
            fn put_u64<'py>(
                slf: &Bound<'py, Self>,
                table: &Table,
                row: &Bound<'_, PyAny>,
                family: String,
                qualifier: &Bound<'_, PyAny>,
                value: u64,
                ts: Option<u64>,
            ) -> PyResult<Bound<'py, Self>> {
                let op = Op::PutU64(family, convert::bytes(qualifier)?, ts, value);
                slf.get().record(table, convert::bytes(row)?, op)?;
                Ok(slf.clone())
            }

            /// Adds `delta` to a counter cell.
            #[pyo3(signature = (table, row, family, qualifier, delta, *, ts=None))]
            #[allow(clippy::too_many_arguments)]
            fn incr<'py>(
                slf: &Bound<'py, Self>,
                table: &Table,
                row: &Bound<'_, PyAny>,
                family: String,
                qualifier: &Bound<'_, PyAny>,
                delta: i64,
                ts: Option<u64>,
            ) -> PyResult<Bound<'py, Self>> {
                let op = Op::Incr(family, convert::bytes(qualifier)?, ts, delta);
                slf.get().record(table, convert::bytes(row)?, op)?;
                Ok(slf.clone())
            }

            /// Adds an input to an aggregate family's cell, read by the family's kind.
            #[pyo3(signature = (table, row, family, qualifier, value, *, ts=None))]
            #[allow(clippy::too_many_arguments)]
            fn aggregate<'py>(
                slf: &Bound<'py, Self>,
                table: &Table,
                row: &Bound<'_, PyAny>,
                family: String,
                qualifier: &Bound<'_, PyAny>,
                value: &Bound<'_, PyAny>,
                ts: Option<u64>,
            ) -> PyResult<Bound<'py, Self>> {
                let input = aggregate_input(table.writer()?, &family, value)?;
                let op = Op::Aggregate(family, convert::bytes(qualifier)?, ts, input);
                slf.get().record(table, convert::bytes(row)?, op)?;
                Ok(slf.clone())
            }

            /// Puts a vector in a vector family: a buffer (a NumPy array, `array.array`) or a
            /// sequence, of the family's element type (bits: packed, bytes-like).
            #[pyo3(signature = (table, row, family, qualifier, vector, *, ts=None))]
            #[allow(clippy::too_many_arguments)]
            fn put_vector<'py>(
                slf: &Bound<'py, Self>,
                table: &Table,
                row: &Bound<'_, PyAny>,
                family: String,
                qualifier: &Bound<'_, PyAny>,
                vector: &Bound<'_, PyAny>,
                ts: Option<u64>,
            ) -> PyResult<Bound<'py, Self>> {
                let v = crate::table::vector_input(table.writer()?, &family, vector)?;
                let op = Op::PutVector(family, convert::bytes(qualifier)?, ts, v);
                slf.get().record(table, convert::bytes(row)?, op)?;
                Ok(slf.clone())
            }

            /// Deletes `row`; with `family`, that family; with `qualifier` too, the column;
            /// with `ts` too, that one version.
            #[pyo3(signature = (table, row, family=None, qualifier=None, *, ts=None))]
            fn delete<'py>(
                slf: &Bound<'py, Self>,
                table: &Table,
                row: &Bound<'_, PyAny>,
                family: Option<String>,
                qualifier: Option<&Bound<'_, PyAny>>,
                ts: Option<u64>,
            ) -> PyResult<Bound<'py, Self>> {
                let op = delete_op(family, qualifier.map(convert::bytes).transpose()?, ts)?;
                slf.get().record(table, convert::bytes(row)?, op)?;
                Ok(slf.clone())
            }
        }
    };
}

/// The open database, as the writer process. Close it with `close()`, or use it as a
/// context manager.
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Database(
    RwLock<Option<pigeonhole::Pigeonhole>>,
    /// The memory stores opened on it (D311 T6): `close` closes them first, so their
    /// forgetter threads stop (simp724's P1 on #1340).
    Mutex<Vec<std::sync::Weak<crate::memory::StoreSlot>>>,
);

impl Database {
    /// Registers a memory store opened on this database, for `close` to close first. Under
    /// the stores lock, which `close` holds while it takes the list and the handle, so a
    /// store opened during a close is either closed by it or refused here (simp724's L1 on
    /// #1395): `Closed` once the database is gone.
    pub(crate) fn adopt_store(
        &self,
        slot: &std::sync::Arc<crate::memory::StoreSlot>,
    ) -> PyResult<()> {
        let mut stores = self.1.lock().expect("not poisoned");
        if self.0.read().expect("not poisoned").is_none() {
            return Err(with_code(
                pigeonhole::ErrorCode::Closed,
                "the database is closed",
            ));
        }
        stores.retain(|w| w.strong_count() > 0);
        stores.push(std::sync::Arc::downgrade(slot));
        Ok(())
    }

    pub(crate) fn db(&self) -> PyResult<pigeonhole::Pigeonhole> {
        self.0
            .read()
            .expect("not poisoned")
            .clone()
            .ok_or_else(|| with_code(pigeonhole::ErrorCode::Closed, "the database is closed"))
    }
}

/// Opens (with `create=True`, creates) the database at `path` as the writer process.
///
/// `change_feed=n` turns the change feed on (`Table.feed`), keeping up to `n` bytes of
/// history, for at most `feed_age` (seconds or a `timedelta`) if given; `feed_tables` limits
/// it to those tables. With neither `change_feed` nor `change_feed_off=True`, an open
/// inherits the last open's feed, so a feed's tokens stay valid across an open that doesn't
/// mention it; `change_feed_off=True` turns it off, which ends its history.
#[pyfunction]
#[pyo3(signature = (path, *, create=false, shards=None, durability=None, block_cache=None, row_cache=None, memtable_budget=None, shm_dir=None, read_only_fallback=None, change_feed=None, change_feed_off=false, feed_age=None, feed_tables=None))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn open(
    py: Python<'_>,
    path: PathBuf,
    create: bool,
    shards: Option<usize>,
    durability: Option<Durability>,
    block_cache: Option<usize>,
    row_cache: Option<usize>,
    memtable_budget: Option<u64>,
    shm_dir: Option<PathBuf>,
    read_only_fallback: Option<bool>,
    change_feed: Option<u64>,
    change_feed_off: bool,
    feed_age: Option<&Bound<'_, PyAny>>,
    feed_tables: Option<Vec<String>>,
) -> PyResult<Database> {
    let mut o = pigeonhole::Options::default().create_if_missing(create);
    o = o.maybe(shards, |o, n| o.shards(n));
    o = o.maybe(durability, |o, d| o.durability(d.into()));
    o = o.maybe(block_cache, |o, n| o.block_cache(n));
    o = o.maybe(row_cache, |o, n| o.row_cache(n));
    o = o.maybe(memtable_budget, |o, n| o.memtable_budget(n));
    o = o.maybe(shm_dir, |o, d| o.shm_dir(d));
    o = o.maybe(read_only_fallback, |o, b| o.read_only_fallback(b));
    if change_feed_off {
        if change_feed.is_some() || feed_age.is_some() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "change_feed_off or change_feed, not both",
            ));
        }
        o = o.change_feed_off();
    }
    match (change_feed, feed_age) {
        (Some(n), age) => {
            let mut r = pigeonhole::FeedRetention::bytes(n);
            if let Some(a) = age {
                r = r.age(convert::duration(a)?);
            }
            o = o.change_feed(r);
        }
        (None, Some(_)) => {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "feed_age needs change_feed (the history's size in bytes)",
            ));
        }
        (None, None) => {}
    }
    o = o.maybe(feed_tables, |o, t| o.feed_tables(t));
    let db = py
        .detach(move || pigeonhole::Pigeonhole::open(path, o))
        .or_py()?;
    Ok(Database(RwLock::new(Some(db)), Mutex::new(Vec::new())))
}

#[pymethods]
impl Database {
    /// A table builder: add families with `.family(...)`, then `.create()`,
    /// `.create_if_missing()` or `.open()`.
    fn table(&self, name: String) -> PyResult<TableBuilder> {
        Ok(TableBuilder {
            db: self.db()?,
            name,
            families: Vec::new(),
        })
    }

    /// The table names.
    fn tables(&self) -> PyResult<Vec<String>> {
        Ok(self.db()?.tables())
    }

    /// Drops a table and its data.
    fn drop_table(&self, py: Python<'_>, name: String) -> PyResult<()> {
        let db = self.db()?;
        py.detach(move || db.drop_table(&name)).or_py()
    }

    /// A snapshot of everything committed, or with `durable=True` of what is durable at its
    /// level.
    #[pyo3(signature = (*, durable=false))]
    fn snapshot(&self, py: Python<'_>, durable: bool) -> PyResult<Snapshot> {
        let db = self.db()?;
        py.detach(move || {
            if durable {
                db.durable_snapshot()
            } else {
                db.snapshot()
            }
        })
        .or_py()
        .map(Snapshot::new)
    }

    /// A batch of writes to any rows and tables, committed atomically by `commit()`, or on
    /// leaving a `with` block without an exception.
    fn write_batch(&self) -> PyResult<WriteBatch> {
        Ok(WriteBatch {
            db: self.db()?,
            ops: Mutex::new(Some(Vec::new())),
        })
    }

    /// An optimistic transaction: reads see one snapshot, and `commit()` fails with
    /// `Conflict` if a cell it read changed since. As a context manager it commits on a
    /// clean exit. `transact()` retries for you.
    fn transaction(&self, py: Python<'_>) -> PyResult<Transaction> {
        let db = self.db()?;
        let t = py.detach(move || db.transaction()).or_py()?;
        Ok(Transaction(Mutex::new(Some(t))))
    }

    /// Runs `fn(txn)` in a transaction and commits it, retrying on `Conflict` (and `Busy`)
    /// as the library's `RetryPolicy` does: with backoff, up to `max_attempts` (default 8)
    /// or `deadline` seconds. Returns what `fn` returned. `fn` may run more than once, so it
    /// should have no effects outside the transaction.
    #[pyo3(signature = (f, *, max_attempts=None, deadline=None, durability=None))]
    fn transact(
        &self,
        py: Python<'_>,
        f: &Bound<'_, PyAny>,
        max_attempts: Option<u32>,
        deadline: Option<&Bound<'_, PyAny>>,
        durability: Option<Durability>,
    ) -> PyResult<Py<PyAny>> {
        let db = self.db()?;
        let mut policy = pigeonhole::RetryPolicy::default();
        policy = policy.maybe(max_attempts, |policy, n| policy.max_attempts(n));
        if let Some(d) = deadline {
            policy = policy.deadline(convert::duration(d)?);
        }
        let started = Instant::now();
        let mut attempt = 1;
        loop {
            let db2 = db.clone();
            let txn = py.detach(move || db2.transaction()).or_py()?;
            let txn = Bound::new(py, Transaction(Mutex::new(Some(txn))))?;
            // A conflict surfaces at commit, or from a read inside `f`.
            let outcome = f.call1((&txn,)).and_then(|out| {
                txn.get().commit_with(py, durability)?;
                Ok(out.unbind())
            });
            let err = match outcome {
                Ok(out) => return Ok(out),
                Err(e) => e,
            };
            let code = if err.is_instance_of::<error::Conflict>(py) {
                pigeonhole::ErrorCode::Conflict
            } else if err.is_instance_of::<error::Busy>(py) {
                pigeonhole::ErrorCode::Busy
            } else {
                return Err(err);
            };
            if !policy.retries_code(code) {
                return Err(err);
            }
            match policy.next_pause(code, attempt, started.elapsed()) {
                Some(pause) => py.detach(|| std::thread::sleep(pause)),
                None => return Err(err),
            }
            attempt += 1;
        }
    }

    /// Flushes every memtable to the file.
    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let db = self.db()?;
        py.detach(move || db.flush()).or_py()
    }

    /// Compacts every table fully.
    fn compact(&self, py: Python<'_>) -> PyResult<()> {
        let db = self.db()?;
        py.detach(move || db.compact()).or_py()
    }

    /// Closes the database: flushes, checkpoints and releases the writer lock. Using it, or
    /// its tables, afterwards raises `Closed`. Closing twice does nothing.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        // The memory stores first: dropping their handles stops their forgetters (a slice in
        // flight fails with `Closed`, its deletion dropped whole). The list and the handle are
        // taken under the stores lock, so a store opened meanwhile is in the list or refused.
        let (stores, db) = {
            let mut stores = self.1.lock().expect("not poisoned");
            let db = self.0.write().expect("not poisoned").take();
            (std::mem::take(&mut *stores), db)
        };
        for s in stores.iter().filter_map(std::sync::Weak::upgrade) {
            crate::memory::close_slot(&s);
        }
        match db {
            Some(db) => py.detach(move || db.close()).or_py(),
            None => Ok(()),
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        _ty: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _tb: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }
}

/// A database opened as a reader process: it sees what the running writer process commits,
/// and can't write.
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Reader(
    RwLock<Option<pigeonhole::PigeonholeReader>>,
    /// The memory stores opened on it: `close` closes them first.
    Mutex<Vec<std::sync::Weak<crate::memory::ReadSlot>>>,
);

impl Reader {
    /// Registers a memory store opened on this reader, for `close` to close first; `Closed`
    /// once the reader is gone (as `Database::adopt_store`).
    pub(crate) fn adopt_store(
        &self,
        slot: &std::sync::Arc<crate::memory::ReadSlot>,
    ) -> PyResult<()> {
        let mut stores = self.1.lock().expect("not poisoned");
        if self.0.read().expect("not poisoned").is_none() {
            return Err(with_code(
                pigeonhole::ErrorCode::Closed,
                "the reader is closed",
            ));
        }
        stores.retain(|w| w.strong_count() > 0);
        stores.push(std::sync::Arc::downgrade(slot));
        Ok(())
    }

    pub(crate) fn db(&self) -> PyResult<pigeonhole::PigeonholeReader> {
        self.0
            .read()
            .expect("not poisoned")
            .clone()
            .ok_or_else(|| with_code(pigeonhole::ErrorCode::Closed, "the reader is closed"))
    }
}

/// Opens the database at `path` as a reader process of the writer that has it open
/// (`NoWriter` without one).
#[pyfunction]
#[pyo3(signature = (path, *, shm_dir=None, block_cache=None, row_cache=None))]
pub(crate) fn open_reader(
    py: Python<'_>,
    path: PathBuf,
    shm_dir: Option<PathBuf>,
    block_cache: Option<usize>,
    row_cache: Option<usize>,
) -> PyResult<Reader> {
    let mut o = pigeonhole::ReaderOptions::default();
    o = o.maybe(shm_dir, |o, d| o.shm_dir(d));
    o = o.maybe(block_cache, |o, n| o.block_cache(n));
    o = o.maybe(row_cache, |o, n| o.row_cache(n));
    let r = py
        .detach(move || pigeonhole::Pigeonhole::open_reader(path, o))
        .or_py()?;
    Ok(Reader(RwLock::new(Some(r)), Mutex::new(Vec::new())))
}

#[pymethods]
impl Reader {
    /// A table, for reads.
    fn table(&self, py: Python<'_>, name: String) -> PyResult<Table> {
        let db = self.db()?;
        py.detach(move || db.table(&name))
            .or_py()
            .map(|t| Table(Handle::Reader(t)))
    }

    /// The table names.
    fn tables(&self) -> PyResult<Vec<String>> {
        Ok(self.db()?.tables())
    }

    /// A snapshot of everything the writer committed, or with `durable=True` of what is
    /// durable at its level.
    #[pyo3(signature = (*, durable=false))]
    fn snapshot(&self, py: Python<'_>, durable: bool) -> PyResult<Snapshot> {
        let db = self.db()?;
        py.detach(move || {
            if durable {
                db.durable_snapshot()
            } else {
                db.snapshot()
            }
        })
        .or_py()
        .map(Snapshot::new)
    }

    /// Closes the reader. Closing twice does nothing.
    fn close(&self) {
        let stores = {
            let mut stores = self.1.lock().expect("not poisoned");
            self.0.write().expect("not poisoned").take();
            std::mem::take(&mut *stores)
        };
        for s in stores.iter().filter_map(std::sync::Weak::upgrade) {
            s.write().expect("not poisoned").take();
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &self,
        _ty: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _tb: &Bound<'_, PyAny>,
    ) -> bool {
        self.close();
        false
    }
}

/// One recorded batch write: its table, its row and the write.
type BatchOp = (pigeonhole::Table, Vec<u8>, Op);

/// Writes to any rows and tables, committed atomically by `commit()` (or on leaving a
/// `with` block without an exception; with one, nothing is written).
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct WriteBatch {
    db: pigeonhole::Pigeonhole,
    /// `None` once committed or discarded.
    ops: Mutex<Option<Vec<BatchOp>>>,
}

impl WriteBatch {
    fn record(&self, table: &Table, row: Vec<u8>, op: Op) -> PyResult<()> {
        let t = table.writer()?.clone();
        match self.ops.lock().expect("not poisoned").as_mut() {
            Some(ops) => {
                ops.push((t, row, op));
                Ok(())
            }
            None => Err(PyValueError::new_err(
                "the batch was already committed or discarded",
            )),
        }
    }
}

write_methods!(WriteBatch {
    /// Writes recorded so far.
    fn __len__(&self) -> usize {
        self.ops.lock().expect("not poisoned").as_ref().map_or(0, Vec::len)
    }

    /// Commits every write atomically; returns the commit's sequence number.
    #[pyo3(signature = (*, durability=None))]
    fn commit(&self, py: Python<'_>, durability: Option<Durability>) -> PyResult<u64> {
        let ops = self
            .ops
            .lock()
            .expect("not poisoned")
            .take()
            .ok_or_else(|| PyValueError::new_err("the batch was already committed or discarded"))?;
        let db = self.db.clone();
        py.detach(move || {
            let mut b = db.write_batch();
            for (t, row, op) in &ops {
                apply!(&mut b, t, row, op);
            }
            match durability {
                Some(d) => b.commit_with(d.into()),
                None => b.commit(),
            }
        })
        .or_py()
        .map(|i| i.seqno)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        ty: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _tb: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        if ty.is_none() {
            if self.ops.lock().expect("not poisoned").is_some() {
                self.commit(py, None)?;
            }
        } else {
            self.ops.lock().expect("not poisoned").take();
        }
        Ok(false)
    }
});

/// An optimistic transaction (see `Database.transaction`).
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Transaction(Mutex<Option<pigeonhole::Transaction>>);

impl Transaction {
    fn record(&self, table: &Table, row: Vec<u8>, op: Op) -> PyResult<()> {
        let t = table.writer()?;
        let mut guard = self.0.lock().expect("not poisoned");
        let txn = guard.as_mut().ok_or_else(done)?;
        apply!(txn, t, &row, &op);
        Ok(())
    }

    fn commit_with(&self, py: Python<'_>, durability: Option<Durability>) -> PyResult<u64> {
        let txn = self
            .0
            .lock()
            .expect("not poisoned")
            .take()
            .ok_or_else(done)?;
        py.detach(move || match durability {
            Some(d) => txn.commit_with(d.into()),
            None => txn.commit(),
        })
        .or_py()
        .map(|i| i.seqno)
    }
}

impl Transaction {
    /// Runs `f` on the open transaction (the memory store's `*_in` calls), with the GIL
    /// released; `ValueError` once it's committed or rolled back.
    pub(crate) fn with_txn<R: Send>(
        &self,
        py: Python<'_>,
        f: impl FnOnce(&mut pigeonhole::Transaction) -> R + Send,
    ) -> PyResult<R> {
        let mut guard = self.0.lock().expect("not poisoned");
        let txn = guard.as_mut().ok_or_else(done)?;
        Ok(py.detach(move || f(txn)))
    }
}

fn done() -> PyErr {
    PyValueError::new_err("the transaction was already committed or rolled back")
}

write_methods!(Transaction {
    /// The newest version of one cell, as of the transaction's snapshot (and its own
    /// writes). The cell is part of what commit checks.
    fn get(
        &self,
        py: Python<'_>,
        table: &Table,
        row: &Bound<'_, PyAny>,
        family: String,
        qualifier: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Cell>> {
        let t = table.writer()?.clone();
        let (row, q) = (convert::bytes(row)?, convert::bytes(qualifier)?);
        let mut guard = self.0.lock().expect("not poisoned");
        let txn = guard.as_mut().ok_or_else(done)?;
        py.detach(|| txn.get(&t, &row, &family, &q).map(|c| c.map(|c| c.to_owned())))
            .or_py()
            .map(|c| c.map(Cell))
    }

    /// A row's cells in `families`, as of the transaction's snapshot. The families read
    /// are part of what commit checks.
    fn get_row(
        &self,
        py: Python<'_>,
        table: &Table,
        row: &Bound<'_, PyAny>,
        families: Vec<String>,
    ) -> PyResult<Option<Row>> {
        let t = table.writer()?.clone();
        let row = convert::bytes(row)?;
        let mut guard = self.0.lock().expect("not poisoned");
        let txn = guard.as_mut().ok_or_else(done)?;
        py.detach(|| {
            let names: Vec<&str> = families.iter().map(String::as_str).collect();
            txn.get_row(&t, &row, &names).map(|r| r.map(|r| r.to_owned()))
        })
        .or_py()
        .map(|r| r.map(Row))
    }

    /// Commits; `Conflict` if a cell or family it read changed since its snapshot.
    #[pyo3(signature = (*, durability=None))]
    fn commit(&self, py: Python<'_>, durability: Option<Durability>) -> PyResult<u64> {
        self.commit_with(py, durability)
    }

    /// Drops the transaction's writes.
    fn rollback(&self) {
        self.0.lock().expect("not poisoned").take();
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        ty: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _tb: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let open = self.0.lock().expect("not poisoned").is_some();
        if ty.is_none() && open {
            self.commit_with(py, None)?;
        } else {
            self.rollback();
        }
        Ok(false)
    }
});
