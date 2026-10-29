//! RocksDB's own transactions for the transactional workloads (D274, amending D247):
//! [`WorkloadKind::Transfer`](crate::WorkloadKind::Transfer) and
//! [`WorkloadKind::YcsbF`](crate::WorkloadKind::YcsbF). The same key encoding, values,
//! options and memory budget as [`super::rocksdb`], whose plain `DB` runs every other
//! workload.
//!
//! - **Optimistic** (`rocksdb`, the judged baseline): `OptimisticTransactionDB`. A transaction
//!   reads with `get_pinned_for_update` at a snapshot taken at its start (validated at
//!   commit) and writes; a commit that
//!   finds a conflicting write fails with `Busy` and runs again, as Pigeonhole's optimistic
//!   transactions do.
//! - **Pessimistic** (`rocksdb-txn`, reported): `TransactionDB`. `get_for_update` takes the
//!   row's lock until the commit (the default lock timeout); the two accounts of a transfer
//!   are locked in key order, as an application avoiding deadlock would. A lock timeout
//!   (`TimedOut`) or `Busy` runs the transaction again.
//!
//! Every retry counts as a conflict ([`Runner::txn_conflicts`]).

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rocksdb::{
    BlockBasedOptions, Cache, Direction, ErrorKind, IteratorMode, OptimisticTransactionDB,
    OptimisticTransactionOptions, Options, TransactionDB, TransactionDBOptions, TransactionOptions,
    WriteBatchWithTransaction, WriteOptions,
};

use super::{
    BLOOM_BITS, Counted, MemoryBudget, Touched, durability, encode_value, family_id, keys,
    live_value, modified, now_micros, read_family, transfer_balances,
};
use crate::workload::YCSB_FAMILY;
use crate::{BenchOp, Client, Runner};

/// Which RocksDB transactions run the transactional workloads (D274).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TxnMode {
    /// `OptimisticTransactionDB`, retried on a conflict: the judged baseline.
    #[default]
    Optimistic,
    /// `TransactionDB` with `get_for_update` locks: reported beside it.
    Pessimistic,
}

enum Db {
    Optimistic(OptimisticTransactionDB),
    Pessimistic(TransactionDB),
}

/// Runs RocksDB's transactions (feature `rocksdb`, D274): the transfer and ycsb-f
/// workloads only; any other operation is an error.
///
/// ```
/// use pigeonhole_bench::{RocksDbTxnRunner, Runner, TxnMode};
///
/// assert_eq!(RocksDbTxnRunner::new(TxnMode::Optimistic).name(), "rocksdb");
/// assert_eq!(RocksDbTxnRunner::new(TxnMode::Pessimistic).name(), "rocksdb-txn");
/// ```
pub struct RocksDbTxnRunner {
    mode: TxnMode,
    sync: bool,
    load_buffered: bool,
    loading: bool,
    memory: MemoryBudget,
    db: Option<Arc<Db>>,
    conflicts: Arc<AtomicU64>,
    /// The retries by cause: `Busy`, `TryAgain`, `TimedOut` (perf287's F2: `TryAgain` means
    /// the memtable history was too short to validate, not a conflict).
    causes: Arc<[AtomicU64; 3]>,
}

impl std::fmt::Debug for RocksDbTxnRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksDbTxnRunner")
            .field("mode", &self.mode)
            .field("sync", &self.sync)
            .field("memory", &self.memory)
            .field("open", &self.db.is_some())
            .finish()
    }
}

impl RocksDbTxnRunner {
    /// A runner in `mode`, buffered, on [`MemoryBudget::default`].
    pub fn new(mode: TxnMode) -> Self {
        Self {
            mode,
            sync: false,
            load_buffered: false,
            loading: false,
            memory: MemoryBudget::default(),
            db: None,
            conflicts: Arc::default(),
            causes: Arc::default(),
        }
    }

    /// `true`: fsync the WAL on every commit; `false` (default): write it to the OS
    /// unsynced, like Pigeonhole's `Buffered` (as [`super::rocksdb::RocksDbRunner::sync`]).
    pub fn sync(mut self, yes: bool) -> Self {
        self.sync = yes;
        self
    }

    /// Write buffer size and block cache (default [`MemoryBudget::default`]).
    pub fn memory(mut self, memory: MemoryBudget) -> Self {
        self.memory = memory;
        self
    }

    /// `true`: the load writes the WAL unsynced, and the warmup and the measured phase as
    /// [`sync`](Self::sync) says (D247).
    pub fn load_buffered(mut self, yes: bool) -> Self {
        self.load_buffered = yes;
        self
    }

    /// The live value of `row`'s `qualifier` in the ycsb family (tests).
    #[cfg(test)]
    pub(crate) fn value(&self, row: &[u8], qualifier: &[u8]) -> Option<Vec<u8>> {
        let f = family_id(YCSB_FAMILY).ok()?;
        let key = keys::cell(row, f, qualifier);
        let stored = match self.db.as_deref()? {
            Db::Optimistic(db) => db.get_pinned(&key).ok()?.map(|v| v.to_vec()),
            Db::Pessimistic(db) => db.get_pinned(&key).ok()?.map(|v| v.to_vec()),
        }?;
        live_value(f, &stored, now_micros())
            .ok()?
            .map(<[u8]>::to_vec)
    }

    fn handle(&self, db: &Arc<Db>, sync: bool) -> Handle {
        let mut write = WriteOptions::default();
        write.set_sync(sync);
        Handle {
            db: Arc::clone(db),
            write,
            conflicts: Arc::clone(&self.conflicts),
            causes: Arc::clone(&self.causes),
        }
    }
}

struct Handle {
    db: Arc<Db>,
    write: WriteOptions,
    conflicts: Arc<AtomicU64>,
    causes: Arc<[AtomicU64; 3]>,
}

impl Runner for RocksDbTxnRunner {
    fn name(&self) -> &'static str {
        match self.mode {
            TxnMode::Optimistic => "rocksdb",
            TxnMode::Pessimistic => "rocksdb-txn",
        }
    }

    fn open(&mut self, dir: &Path) -> Result<(), String> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_write_buffer_size(usize::try_from(self.memory.write_buffer).unwrap_or(usize::MAX));
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&Cache::new_lru_cache(
            usize::try_from(self.memory.cache).unwrap_or(usize::MAX),
        ));
        table.set_bloom_filter(f64::from(BLOOM_BITS), false);
        opts.set_block_based_table_factory(&table);
        let path = dir.join("rocksdb");
        let e = |e: rocksdb::Error| e.to_string();
        let db = match self.mode {
            TxnMode::Optimistic => {
                Db::Optimistic(OptimisticTransactionDB::open(&opts, path).map_err(e)?)
            }
            TxnMode::Pessimistic => Db::Pessimistic(
                TransactionDB::open(&opts, &TransactionDBOptions::default(), path).map_err(e)?,
            ),
        };
        self.db = Some(Arc::new(db));
        self.loading = self.load_buffered;
        Ok(())
    }

    fn measured_durability(&mut self) -> Result<bool, String> {
        let switched = self.loading && self.sync;
        self.loading = false;
        Ok(switched)
    }

    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        self.execute_counted(op).map(drop)
    }

    fn close(&mut self) -> Result<(), String> {
        let c = |i: usize| self.causes[i].load(Ordering::Relaxed);
        if c(0) + c(1) + c(2) > 0 {
            eprintln!(
                "{} transaction retries: busy={} try_again={} timed_out={}",
                self.name(),
                c(0),
                c(1),
                c(2)
            );
        }
        match self.db.take().as_deref() {
            Some(Db::Optimistic(db)) => db.flush_wal(true).map_err(|e| e.to_string()),
            Some(Db::Pessimistic(db)) => db.flush_wal(true).map_err(|e| e.to_string()),
            None => Ok(()),
        }
    }

    fn flush(&mut self) -> Result<(), String> {
        match self.db.as_deref() {
            Some(Db::Optimistic(db)) => db.flush().map_err(|e| e.to_string()),
            Some(Db::Pessimistic(db)) => db.flush().map_err(|e| e.to_string()),
            None => Ok(()),
        }
    }

    /// The optimistic baseline's background work (D233), as [`super::rocksdb`] reads it.
    /// The pessimistic store reports none: this RocksDB crate exposes properties on a
    /// `TransactionDB` only in its multi-threaded mode.
    fn background(&self) -> Option<crate::Background> {
        match self.db.as_deref()? {
            Db::Optimistic(db) => Some(super::rocksdb::background(
                |name| db.property_int_value(name).ok().flatten(),
                |name| db.property_value(name).ok().flatten(),
            )),
            Db::Pessimistic(_) => None,
        }
    }

    fn client(&self) -> Option<Box<dyn Client>> {
        Some(Box::new(self.handle(self.db.as_ref()?, self.sync)))
    }

    fn describe(&self) -> String {
        format!(
            "{} bloom={BLOOM_BITS} {}{} txn={}",
            self.memory,
            durability(self.sync),
            if self.load_buffered {
                " load=buffered"
            } else {
                ""
            },
            match self.mode {
                TxnMode::Optimistic => "optimistic",
                TxnMode::Pessimistic => "pessimistic",
            }
        )
    }

    fn txn_conflicts(&self) -> Option<u64> {
        Some(self.conflicts.load(Ordering::Relaxed))
    }
}

impl Counted for RocksDbTxnRunner {
    fn execute_counted(&mut self, op: &BenchOp) -> Result<Touched, String> {
        let db = self
            .db
            .as_ref()
            .ok_or("rocksdb transaction runner is not open")?;
        self.handle(db, self.sync && !self.loading).execute(op)
    }
}

impl Client for Handle {
    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        Handle::execute(self, op).map(drop)
    }
}

/// The live value of a stored cell `v` of family `f`, if any (`None` when absent or
/// expired).
fn live(f: u8, v: Option<&[u8]>, now: u64) -> Result<Option<&[u8]>, String> {
    v.map(|v| live_value(f, v, now))
        .transpose()
        .map(Option::flatten)
}

/// A transaction attempt's failure that runs it again: a conflict found at commit
/// (`Busy`, `TryAgain`) or a lock wait that timed out (`TimedOut`).
fn retries(e: &rocksdb::Error) -> bool {
    matches!(
        e.kind(),
        ErrorKind::Busy | ErrorKind::TryAgain | ErrorKind::TimedOut
    )
}

/// Runs `$body` in a transaction of `$handle`'s database until it commits, `$txn` naming
/// the transaction (either kind; both have `get_pinned_for_update`, `put` and `commit`). `$body`
/// evaluates to `Result<Result<Touched, String>, rocksdb::Error>`: the outer error is the
/// database's, retried when [`retries`] says so.
macro_rules! in_txn {
    ($handle:expr, |$txn:ident| $body:expr) => {{
        let h = $handle;
        loop {
            let attempt: Result<Result<Touched, String>, rocksdb::Error> = match &*h.db {
                Db::Optimistic(db) => {
                    // A snapshot at the transaction's start, as Pigeonhole's transactions read
                    // at theirs (perf287's note on #912).
                    let mut o = OptimisticTransactionOptions::default();
                    o.set_snapshot(true);
                    let $txn = db.transaction_opt(&h.write, &o);
                    match $body {
                        Ok(Ok(t)) => $txn.commit().map(|()| Ok(t)),
                        other => other,
                    }
                }
                Db::Pessimistic(db) => {
                    let $txn = db.transaction_opt(&h.write, &TransactionOptions::default());
                    match $body {
                        Ok(Ok(t)) => $txn.commit().map(|()| Ok(t)),
                        other => other,
                    }
                }
            };
            match attempt {
                Ok(r) => break r,
                Err(e) if retries(&e) => {
                    h.conflicts.fetch_add(1, Ordering::Relaxed);
                    let cause = match e.kind() {
                        ErrorKind::Busy => 0,
                        ErrorKind::TryAgain => 1,
                        _ => 2,
                    };
                    h.causes[cause].fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => break Err(e.to_string()),
            }
        }
    }};
}

impl Handle {
    /// One atomic batch of a row's cells, stamped now (the load, and ycsb's puts).
    fn put(&self, row: &[u8], family: &str, cells: &[(Vec<u8>, Vec<u8>)]) -> Result<(), String> {
        let f = family_id(family)?;
        let ts = now_micros();
        let mut batch = WriteBatchWithTransaction::<true>::default();
        for (q, v) in cells {
            batch.put(keys::cell(row, f, q), encode_value(ts, v));
        }
        match &*self.db {
            Db::Optimistic(db) => db.write_opt(batch, &self.write),
            Db::Pessimistic(db) => db.write_opt(batch, &self.write),
        }
        .map_err(|e| e.to_string())
    }

    fn execute(&self, op: &BenchOp) -> Result<Touched, String> {
        match op {
            BenchOp::Put { row, family, cells } => {
                self.put(row, family, cells)?;
                Ok(Touched::default())
            }
            BenchOp::GetRow { row, family } => {
                let mut prefix = keys::row_prefix(row);
                prefix.push(family_id(family)?);
                let mode = IteratorMode::From(&prefix, Direction::Forward);
                match &*self.db {
                    Db::Optimistic(db) => read_family(db.iterator(mode), &prefix, now_micros()),
                    Db::Pessimistic(db) => read_family(db.iterator(mode), &prefix, now_micros()),
                }
            }
            BenchOp::ReadModifyWrite { row, qualifier } => {
                let f = family_id(YCSB_FAMILY)?;
                let key = keys::cell(row, f, qualifier);
                in_txn!(self, |txn| (|| {
                    let stored = txn.get_pinned_for_update(&key, true)?;
                    let mut t = Touched::default();
                    let old = match stored.as_deref().map(|v| live_value(f, v, now_micros())) {
                        Some(Err(e)) => return Ok(Err(e)),
                        Some(Ok(old)) => old,
                        None => None,
                    };
                    if let Some(v) = old {
                        t.cell(v);
                    }
                    txn.put(&key, encode_value(now_micros(), &modified(old)))?;
                    Ok(Ok(t))
                })())
            }
            BenchOp::Transfer { from, to, amount } => {
                let f = family_id(YCSB_FAMILY)?;
                let (ka, kb) = (keys::cell(from, f, b"bal"), keys::cell(to, f, b"bal"));
                in_txn!(self, |txn| (|| {
                    // Key order, so two pessimistic transfers over the same accounts never
                    // wait on each other's second lock.
                    let (first, second) = if ka <= kb { (&ka, &kb) } else { (&kb, &ka) };
                    let p1 = txn.get_pinned_for_update(first, true)?;
                    let p2 = txn.get_pinned_for_update(second, true)?;
                    let (a, b) = if ka <= kb { (p1, p2) } else { (p2, p1) };
                    let now = now_micros();
                    let (va, vb) = match (live(f, a.as_deref(), now), live(f, b.as_deref(), now)) {
                        (Ok(va), Ok(vb)) => (va, vb),
                        (Err(e), _) | (_, Err(e)) => return Ok(Err(e)),
                    };
                    let mut t = Touched::default();
                    for v in [va, vb].into_iter().flatten() {
                        t.cell(v);
                    }
                    let (na, nb) = transfer_balances(va, vb, *amount);
                    txn.put(&ka, encode_value(now, &na.to_le_bytes()))?;
                    txn.put(&kb, encode_value(now, &nb.to_le_bytes()))?;
                    Ok(Ok(t))
                })())
            }
            other => Err(format!(
                "RocksDB's transaction runner runs transfer and ycsb-f only (D274): {other:?}"
            )),
        }
    }
}
