//! fjall with the same hand-written wide-column key encoding as the RocksDB runner
//! ([`super::keys`]): one keyspace, one key per cell, latest value only, each value prefixed with the cell's timestamp; the TTL filter runs on read. Default fjall
//! options (which include bloom filters and no global write-buffer cap) except the
//! [`MemoryBudget`]: keyspace memtable size and block cache.

use std::path::Path;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fjall::{
    Database, Keyspace, KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace,
    OptimisticWriteTx, PersistMode, Readable,
};

use super::{
    Counted, MemoryBudget, Touched, durability, encode_value, family_id, keys, live_value,
    modified, now_micros, read_family, scan_rows, scan_rows_filtered, transfer_balances,
};
use crate::workload::YCSB_FAMILY;
use crate::{BenchOp, Client, Runner};

/// Runs fjall (feature `fjall`).
///
/// ```
/// use pigeonhole_bench::{FjallRunner, Runner};
///
/// assert_eq!(FjallRunner::default().name(), "fjall");
/// ```
#[derive(Default)]
pub struct FjallRunner {
    sync: bool,
    /// The load persists to the OS buffer, the measured phase at `sync` (D247).
    load_buffered: bool,
    memory: MemoryBudget,
    open: Option<Handle>,
}

impl std::fmt::Debug for FjallRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallRunner")
            .field("sync", &self.sync)
            .field("memory", &self.memory)
            .field("open", &self.open.is_some())
            .finish()
    }
}

impl FjallRunner {
    /// `true`: `fsync` the journal on every write batch; `false` (default): flush it to
    /// the OS without fsync, like Pigeonhole's `Buffered`.
    pub fn sync(mut self, yes: bool) -> Self {
        self.sync = yes;
        self
    }

    /// Memtable size and block cache (default [`MemoryBudget::default`]).
    pub fn memory(mut self, memory: MemoryBudget) -> Self {
        self.memory = memory;
        self
    }

    /// `true`: the load persists to the OS buffer, and the warmup and the measured phase as
    /// [`sync`](Self::sync) says (D247).
    pub fn load_buffered(mut self, yes: bool) -> Self {
        self.load_buffered = yes;
        self
    }

    fn mode(&self, sync: bool) -> PersistMode {
        if sync {
            PersistMode::SyncAll
        } else {
            PersistMode::Buffer
        }
    }
}

#[derive(Clone)]
struct Handle {
    /// The transactional database's plain handle, for every non-transactional operation.
    db: Database,
    cells: Keyspace,
    mode: PersistMode,
    /// The counters workload's keyspace (D247): one `i64` (little-endian) per row,
    /// incremented by a read-modify-write in a transaction (fjall has no merge operator).
    counters: Keyspace,
    /// fjall's optimistic transactions (D274): transfer, read-modify-write, counter
    /// increments and row deletes run in one, retried on a conflict.
    tx: OptimisticTxDatabase,
    tx_cells: OptimisticTxKeyspace,
    tx_counters: OptimisticTxKeyspace,
    /// Transactions retried on a conflict.
    conflicts: Arc<AtomicU64>,
}

impl Runner for FjallRunner {
    fn name(&self) -> &'static str {
        "fjall"
    }

    fn open(&mut self, dir: &Path) -> Result<(), String> {
        let e = |e: fjall::Error| e.to_string();
        let m = self.memory;
        let tx = OptimisticTxDatabase::builder(dir.join("fjall"))
            .cache_size(m.cache)
            .open()
            .map_err(e)?;
        let tx_cells = tx
            .keyspace("cells", || {
                KeyspaceCreateOptions::default().max_memtable_size(m.write_buffer)
            })
            .map_err(e)?;
        let tx_counters = tx
            .keyspace("counters", KeyspaceCreateOptions::default)
            .map_err(e)?;
        self.open = Some(Handle {
            db: tx.inner().clone(),
            cells: tx_cells.inner().clone(),
            counters: tx_counters.inner().clone(),
            mode: self.mode(self.sync && !self.load_buffered),
            tx,
            tx_cells,
            tx_counters,
            conflicts: Arc::default(),
        });
        Ok(())
    }

    fn measured_durability(&mut self) -> Result<bool, String> {
        let mode = self.mode(self.sync);
        if let Some(h) = self.open.as_mut() {
            h.mode = mode;
        }
        Ok(self.load_buffered && self.sync)
    }

    fn flush(&mut self) -> Result<(), String> {
        match &self.open {
            Some(h) => h
                .cells
                .rotate_memtable_and_wait()
                .map_err(|e| e.to_string()),
            None => Ok(()),
        }
    }

    fn background(&self) -> Option<crate::Background> {
        let h = self.open.as_ref()?;
        Some(crate::Background {
            work: None,
            marker: h.cells.table_count() as u64,
            running: Some(h.db.outstanding_flushes() > 0),
            busy_nanos: None,
        })
    }

    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        self.execute_counted(op).map(drop)
    }

    fn close(&mut self) -> Result<(), String> {
        if let Some(h) = self.open.take() {
            h.db.persist(h.mode).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn client(&self) -> Option<Box<dyn Client>> {
        Some(Box::new(self.open.clone()?))
    }

    fn txn_conflicts(&self) -> Option<u64> {
        Some(self.open.as_ref()?.conflicts.load(Ordering::Relaxed))
    }

    fn describe(&self) -> String {
        format!(
            "{} bloom=default {}{}",
            self.memory,
            durability(self.sync),
            if self.load_buffered {
                " load=buffered"
            } else {
                ""
            }
        )
    }
}

impl Counted for FjallRunner {
    fn execute_counted(&mut self, op: &BenchOp) -> Result<Touched, String> {
        self.open
            .as_ref()
            .ok_or("fjall runner is not open")?
            .execute(op)
    }
}

impl Client for Handle {
    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        Handle::execute(self, op).map(drop)
    }
}

impl Handle {
    /// Runs `body` in one of fjall's optimistic write transactions until it commits: a
    /// conflict at commit counts and runs it again (D274).
    fn in_tx(
        &self,
        mut body: impl FnMut(&mut OptimisticWriteTx) -> Result<Touched, String>,
    ) -> Result<Touched, String> {
        let e = |e: fjall::Error| e.to_string();
        loop {
            let mut tx = self.tx.write_tx().map_err(e)?.durability(Some(self.mode));
            let t = body(&mut tx)?;
            match tx.commit().map_err(e)? {
                Ok(()) => return Ok(t),
                Err(_conflict) => {
                    self.conflicts.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// One atomic batch of cells of one row, all stamped `ts`.
    fn put(
        &self,
        row: &[u8],
        family: &str,
        ts: u64,
        cells: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), String> {
        self.put_rows(family, ts, std::iter::once((row, cells)))
    }

    /// One atomic batch of cells of any rows, all stamped `ts` (D246's cross-shard commit).
    fn put_rows<'a>(
        &self,
        family: &str,
        ts: u64,
        rows: impl Iterator<Item = (&'a [u8], &'a [(Vec<u8>, Vec<u8>)])>,
    ) -> Result<(), String> {
        let f = family_id(family)?;
        let mut batch = self.db.batch().durability(Some(self.mode));
        for (row, cells) in rows {
            for (q, v) in cells {
                batch.insert(&self.cells, keys::cell(row, f, q), encode_value(ts, v));
            }
        }
        batch.commit().map_err(|e| e.to_string())
    }

    fn execute(&self, op: &BenchOp) -> Result<Touched, String> {
        let e = |e: fjall::Error| e.to_string();
        let mut t = Touched::default();
        match op {
            BenchOp::Get {
                row,
                family,
                qualifier,
            } => {
                let f = family_id(family)?;
                let key = keys::cell(row, f, qualifier);
                if let Some(v) = self.cells.get(key).map_err(e)?
                    && let Some(v) = live_value(f, &v, now_micros())?
                {
                    t.cell(v);
                }
            }
            BenchOp::GetRow { row, family } => {
                let mut prefix = keys::row_prefix(row);
                prefix.push(family_id(family)?);
                let iter = self.cells.range(prefix.clone()..).map(|g| g.into_inner());
                t = read_family(iter, &prefix, now_micros())?;
            }
            BenchOp::Put { row, family, cells } => {
                self.put(row, family, now_micros(), cells)?;
            }
            BenchOp::PutAt {
                row,
                family,
                ts,
                cells,
            } => self.put(row, family, *ts, cells)?,
            BenchOp::DeleteColumn {
                row,
                family,
                qualifier,
            } => {
                let mut batch = self.db.batch().durability(Some(self.mode));
                batch.remove(&self.cells, keys::cell(row, family_id(family)?, qualifier));
                batch.commit().map_err(e)?;
            }
            BenchOp::DeleteRow { row } => {
                // No range delete: remove each of the row's keys in one transaction (D274).
                let prefix = keys::row_prefix(row);
                self.in_tx(|tx| {
                    let keys: Vec<_> = tx
                        .prefix(&self.tx_cells, &prefix)
                        .map(|kv| kv.key())
                        .collect::<Result<_, _>>()
                        .map_err(e)?;
                    for key in keys {
                        tx.remove(&self.tx_cells, key);
                    }
                    Ok(Touched::default())
                })?;
            }
            BenchOp::Transfer { from, to, amount } => {
                // An optimistic transaction reading both balances, retried on a conflict
                // (D274), as Pigeonhole's.
                let f = family_id(YCSB_FAMILY)?;
                let (ka, kb) = (keys::cell(from, f, b"bal"), keys::cell(to, f, b"bal"));
                t = self.in_tx(|tx| {
                    let mut t = Touched::default();
                    let now = now_micros();
                    let a = tx.get(&self.tx_cells, &ka).map_err(e)?;
                    let b = tx.get(&self.tx_cells, &kb).map_err(e)?;
                    let va = a
                        .as_deref()
                        .map(|v| live_value(f, v, now))
                        .transpose()?
                        .flatten();
                    let vb = b
                        .as_deref()
                        .map(|v| live_value(f, v, now))
                        .transpose()?
                        .flatten();
                    for v in [va, vb].into_iter().flatten() {
                        t.cell(v);
                    }
                    let (na, nb) = transfer_balances(va, vb, *amount);
                    tx.insert(
                        &self.tx_cells,
                        ka.clone(),
                        encode_value(now, &na.to_le_bytes()),
                    );
                    tx.insert(
                        &self.tx_cells,
                        kb.clone(),
                        encode_value(now, &nb.to_le_bytes()),
                    );
                    Ok(t)
                })?;
            }
            BenchOp::Incr { row, delta } => {
                // fjall has no merge operator: a read and a write in a transaction (D274).
                self.in_tx(|tx| {
                    let n = tx
                        .get(&self.tx_counters, row)
                        .map_err(e)?
                        .and_then(|v| v.as_ref().try_into().ok().map(i64::from_le_bytes))
                        .unwrap_or(0);
                    tx.insert(
                        &self.tx_counters,
                        row.as_slice(),
                        n.wrapping_add(*delta).to_le_bytes(),
                    );
                    Ok(Touched::default())
                })?;
            }
            BenchOp::ReadCounter { row } => {
                if let Some(v) = self.counters.get(row).map_err(e)? {
                    t.cell(&v);
                }
            }
            BenchOp::Commit { family, rows } => self.put_rows(
                family,
                now_micros(),
                rows.iter().map(|r| (r.row.as_slice(), r.cells.as_slice())),
            )?,
            BenchOp::Scan { start, len } => {
                let from = keys::row_prefix(start);
                let iter = self.cells.range(from..).map(|g| g.into_inner());
                t = scan_rows(iter, *len, now_micros())?;
            }
            BenchOp::ScanFiltered {
                start,
                len,
                family,
                qualifier_prefix,
                time_range,
            } => {
                let from = keys::row_prefix(start);
                let iter = self.cells.range(from..).map(|g| g.into_inner());
                t = scan_rows_filtered(
                    iter,
                    *len,
                    now_micros(),
                    family_id(family)?,
                    qualifier_prefix.as_deref(),
                    *time_range,
                )?;
            }
            BenchOp::ReadModifyWrite { row, qualifier } => {
                let key = keys::cell(row, family_id(YCSB_FAMILY)?, qualifier);
                let f = family_id(YCSB_FAMILY)?;
                // Atomic across clients: the read and the write in one optimistic transaction,
                // retried on a conflict (D274).
                t = self.in_tx(|tx| {
                    let mut t = Touched::default();
                    let stored = tx.get(&self.tx_cells, &key).map_err(e)?;
                    let old = match &stored {
                        Some(v) => live_value(f, v, now_micros())?,
                        None => None,
                    };
                    if let Some(v) = old {
                        t.cell(v);
                    }
                    let new = encode_value(now_micros(), &modified(old));
                    tx.insert(&self.tx_cells, key.clone(), new);
                    Ok(t)
                })?;
            }
        }
        Ok(t)
    }
}
