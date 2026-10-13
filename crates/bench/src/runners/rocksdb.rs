//! RocksDB with a hand-written wide-column key encoding ([`super::keys`]): one key per cell,
//! latest value only, each value prefixed with the cell's timestamp; the TTL filter runs on
//! read. Default RocksDB options except the [`MemoryBudget`] (write buffer, LRU block cache)
//! and a 10-bit bloom filter; no compression codecs are compiled in.
//!
//! The `metric` family lives in its own column family with FIFO compaction and the family's
//! TTL, the counterpart of Pigeonhole's `FifoByTime` (D163, #236); every other family shares
//! the default one. A scan merges both column families once a `metric` cell was written, so
//! runs that never write one (the sparse-wide gate) scan one iterator as before.

use std::path::Path;
use std::sync::Arc;

use std::sync::atomic::{AtomicBool, Ordering};

use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompactionStyle, Direction,
    FifoCompactOptions, IteratorMode, Options, WriteBatch, WriteOptions,
};

use super::{
    BLOOM_BITS, Counted, MemoryBudget, RowLocks, Touched, durability, encode_value, family_id,
    keys, live_value, modified, now_micros, read_family, scan_rows, transfer_balances,
};
use crate::workload::{METRIC_FAMILY, TIME_SERIES_TTL, YCSB_FAMILY};

/// The column family of [`METRIC_FAMILY`].
const METRIC_CF: &str = "metric";
/// The counters workload's column family (D247): one `i64` (little-endian) per row,
/// incremented with an associative add merge operator.
const COUNTER_CF: &str = "counter";

/// The counter column family's merge: the stored value plus every operand, wrapping.
fn add_i64(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &rocksdb::MergeOperands,
) -> Option<Vec<u8>> {
    let read = |v: &[u8]| v.try_into().map_or(0, i64::from_le_bytes);
    let sum = operands
        .iter()
        .fold(existing.map_or(0, read), |n, op| n.wrapping_add(read(op)));
    Some(sum.to_le_bytes().to_vec())
}
use crate::{BenchOp, Client, Runner};

/// Runs RocksDB (feature `rocksdb`).
///
/// ```
/// use pigeonhole_bench::{RocksDbRunner, Runner};
///
/// assert_eq!(RocksDbRunner::default().name(), "rocksdb");
/// ```
#[derive(Default)]
pub struct RocksDbRunner {
    sync: bool,
    /// The load writes unsynced, the measured phase at `sync` (D247).
    load_buffered: bool,
    /// The load is running (unsynced when `load_buffered`).
    loading: bool,
    memory: MemoryBudget,
    db: Option<Arc<DB>>,
    /// Whether a `metric` cell was written: scans then merge both column families.
    metric_written: Arc<AtomicBool>,
    /// Row locks for an atomic read-modify-write across clients.
    locks: Arc<RowLocks>,
}

impl std::fmt::Debug for RocksDbRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksDbRunner")
            .field("sync", &self.sync)
            .field("memory", &self.memory)
            .field("open", &self.db.is_some())
            .finish()
    }
}

impl RocksDbRunner {
    /// `true`: fsync the WAL on every write; `false` (default): write the WAL to the OS
    /// without fsync, like Pigeonhole's `Buffered`.
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
}

struct Handle {
    db: Arc<DB>,
    write: WriteOptions,
    metric_written: Arc<AtomicBool>,
    locks: Arc<RowLocks>,
}

impl RocksDbRunner {
    fn handle(&self, db: &Arc<DB>, sync: bool) -> Handle {
        let mut write = WriteOptions::default();
        write.set_sync(sync);
        Handle {
            db: Arc::clone(db),
            write,
            metric_written: Arc::clone(&self.metric_written),
            locks: Arc::clone(&self.locks),
        }
    }
}

impl Runner for RocksDbRunner {
    fn name(&self) -> &'static str {
        "rocksdb"
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
        opts.create_missing_column_families(true);
        // `metric`: FIFO compaction with the family's TTL (judged on file age, not on the
        // cells' event times), and no size cap, as Pigeonhole's `FifoByTime` has none.
        let mut metric = opts.clone();
        metric.set_compaction_style(DBCompactionStyle::Fifo);
        let mut fifo = FifoCompactOptions::default();
        fifo.set_max_table_files_size(u64::MAX);
        metric.set_fifo_compaction_options(&fifo);
        metric.set_ttl(TIME_SERIES_TTL.as_secs());
        let mut counter = opts.clone();
        counter.set_merge_operator_associative("i64_add", add_i64);
        let db = DB::open_cf_descriptors(
            &opts,
            dir.join("rocksdb"),
            [
                ColumnFamilyDescriptor::new(METRIC_CF, metric),
                ColumnFamilyDescriptor::new(COUNTER_CF, counter),
            ],
        )
        .map_err(|e| e.to_string())?;
        self.db = Some(Arc::new(db));
        self.loading = self.load_buffered;
        Ok(())
    }

    fn measured_durability(&mut self) -> Result<(), String> {
        self.loading = false;
        Ok(())
    }

    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        self.execute_counted(op).map(drop)
    }

    fn close(&mut self) -> Result<(), String> {
        if let Some(db) = self.db.take() {
            db.flush_wal(true).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), String> {
        let Some(db) = &self.db else { return Ok(()) };
        db.flush().map_err(|e| e.to_string())?;
        if let Some(cf) = db.cf_handle(METRIC_CF) {
            db.flush_cf(cf).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn background(&self) -> Option<crate::Background> {
        let db = self.db.as_ref()?;
        let prop = |name: &str| db.property_int_value(name).ok().flatten().unwrap_or(0);
        let running = prop("rocksdb.num-running-compactions")
            + prop("rocksdb.num-running-flushes")
            + prop("rocksdb.compaction-pending")
            + prop("rocksdb.mem-table-flush-pending");
        Some(crate::Background {
            work: None,
            marker: prop("rocksdb.total-sst-files-size"),
            running: Some(running > 0),
            busy_nanos: None,
        })
    }

    fn client(&self) -> Option<Box<dyn Client>> {
        Some(Box::new(self.handle(self.db.as_ref()?, self.sync)))
    }

    fn describe(&self) -> String {
        format!(
            "{} bloom={BLOOM_BITS} {}{}",
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

impl Counted for RocksDbRunner {
    fn execute_counted(&mut self, op: &BenchOp) -> Result<Touched, String> {
        let db = self.db.as_ref().ok_or("rocksdb runner is not open")?;
        self.handle(db, self.sync && !self.loading).execute(op)
    }
}

impl Client for Handle {
    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        Handle::execute(self, op).map(drop)
    }
}

impl Handle {
    /// The `metric` column family for `family`, or `None` for the default one.
    fn cf(&self, family: &str) -> Result<Option<&rocksdb::ColumnFamily>, String> {
        if family != METRIC_FAMILY {
            return Ok(None);
        }
        self.db
            .cf_handle(METRIC_CF)
            .map(Some)
            .ok_or_else(|| "no metric column family".to_owned())
    }

    fn counter_cf(&self) -> Result<&rocksdb::ColumnFamily, String> {
        self.db
            .cf_handle(COUNTER_CF)
            .ok_or_else(|| "no counter column family".to_owned())
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

    /// One atomic batch of cells of any rows, all stamped `ts`: a cross-shard commit's
    /// counterpart is one multi-key `WriteBatch` (D246).
    fn put_rows<'a>(
        &self,
        family: &str,
        ts: u64,
        rows: impl Iterator<Item = (&'a [u8], &'a [(Vec<u8>, Vec<u8>)])>,
    ) -> Result<(), String> {
        let f = family_id(family)?;
        let cf = self.cf(family)?;
        let mut batch = WriteBatch::default();
        for (row, cells) in rows {
            for (q, v) in cells {
                let (key, value) = (keys::cell(row, f, q), encode_value(ts, v));
                match cf {
                    Some(cf) => batch.put_cf(cf, key, value),
                    None => batch.put(key, value),
                }
            }
        }
        if cf.is_some() {
            self.metric_written.store(true, Ordering::Relaxed);
        }
        self.db
            .write_opt(batch, &self.write)
            .map_err(|e| e.to_string())
    }

    fn execute(&self, op: &BenchOp) -> Result<Touched, String> {
        let e = |e: rocksdb::Error| e.to_string();
        let mut t = Touched::default();
        match op {
            BenchOp::Get {
                row,
                family,
                qualifier,
            } => {
                let f = family_id(family)?;
                let key = keys::cell(row, f, qualifier);
                let stored = match self.cf(family)? {
                    Some(cf) => self.db.get_pinned_cf(cf, key),
                    None => self.db.get_pinned(key),
                }
                .map_err(e)?;
                if let Some(v) = stored
                    && let Some(v) = live_value(f, &v, now_micros())?
                {
                    t.cell(v);
                }
            }
            BenchOp::GetRow { row, family } => {
                let mut prefix = keys::row_prefix(row);
                prefix.push(family_id(family)?);
                let mode = IteratorMode::From(&prefix, Direction::Forward);
                t = match self.cf(family)? {
                    Some(cf) => read_family(self.db.iterator_cf(cf, mode), &prefix, now_micros())?,
                    None => read_family(self.db.iterator(mode), &prefix, now_micros())?,
                };
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
            BenchOp::Transfer { from, to, amount } => {
                // Both rows locked from the reads to the batch (D247).
                let f = family_id(YCSB_FAMILY)?;
                let (ka, kb) = (keys::cell(from, f, b"bal"), keys::cell(to, f, b"bal"));
                let _held = self.locks.lock_two(&ka, &kb);
                let now = now_micros();
                let a = self.db.get_pinned(&ka).map_err(e)?;
                let b = self.db.get_pinned(&kb).map_err(e)?;
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
                drop((a, b));
                let mut batch = WriteBatch::default();
                batch.put(ka, encode_value(now, &na.to_le_bytes()));
                batch.put(kb, encode_value(now, &nb.to_le_bytes()));
                self.db.write_opt(batch, &self.write).map_err(e)?;
            }
            BenchOp::Incr { row, delta } => {
                let cf = self.counter_cf()?;
                self.db
                    .merge_cf_opt(cf, row, delta.to_le_bytes(), &self.write)
                    .map_err(e)?;
            }
            BenchOp::ReadCounter { row } => {
                if let Some(v) = self.db.get_pinned_cf(self.counter_cf()?, row).map_err(e)? {
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
                let mode = IteratorMode::From(&from, Direction::Forward);
                let main = self.db.iterator(mode);
                t = if self.metric_written.load(Ordering::Relaxed) {
                    let cf = self.cf(METRIC_FAMILY)?.expect("the metric family");
                    let metric = self.db.iterator_cf(cf, mode);
                    scan_rows(Merged::new(main, metric), *len, now_micros())?
                } else {
                    scan_rows(main, *len, now_micros())?
                };
            }
            BenchOp::ReadModifyWrite { row, qualifier } => {
                let key = keys::cell(row, family_id(YCSB_FAMILY)?, qualifier);
                let f = family_id(YCSB_FAMILY)?;
                // Atomic across clients: the read and the write under the row's lock.
                let _held = self.locks.lock(&key);
                let stored = self.db.get_pinned(&key).map_err(e)?;
                let old = match &stored {
                    Some(v) => live_value(f, v, now_micros())?,
                    None => None,
                };
                if let Some(v) = old {
                    t.cell(v);
                }
                let new = encode_value(now_micros(), &modified(old));
                drop(stored);
                self.db.put_opt(key, new, &self.write).map_err(e)?;
            }
        }
        Ok(t)
    }
}

/// Two ordered key-value iterators merged in key order (their keys never collide: the
/// column families hold different families, and the family byte is part of the key).
struct Merged<A: Iterator, B: Iterator> {
    a: std::iter::Peekable<A>,
    b: std::iter::Peekable<B>,
}

type Kv = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>;

impl<A: Iterator<Item = Kv>, B: Iterator<Item = Kv>> Merged<A, B> {
    fn new(a: A, b: B) -> Self {
        Self {
            a: a.peekable(),
            b: b.peekable(),
        }
    }
}

impl<A: Iterator<Item = Kv>, B: Iterator<Item = Kv>> Iterator for Merged<A, B> {
    type Item = Kv;

    fn next(&mut self) -> Option<Kv> {
        let take_a = match (self.a.peek(), self.b.peek()) {
            (None, None) => return None,
            (Some(_), None) | (Some(Err(_)), _) => true,
            (None, Some(_)) | (_, Some(Err(_))) => false,
            (Some(Ok((ka, _))), Some(Ok((kb, _)))) => ka <= kb,
        };
        if take_a { self.a.next() } else { self.b.next() }
    }
}
