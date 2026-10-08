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
    BLOOM_BITS, Counted, MemoryBudget, Touched, durability, encode_value, family_id, keys,
    live_value, modified, now_micros, read_family, scan_rows,
};
use crate::workload::{METRIC_FAMILY, TIME_SERIES_TTL, YCSB_FAMILY};

/// The column family of [`METRIC_FAMILY`].
const METRIC_CF: &str = "metric";
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
    memory: MemoryBudget,
    db: Option<Arc<DB>>,
    /// Whether a `metric` cell was written: scans then merge both column families.
    metric_written: Arc<AtomicBool>,
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
}

struct Handle {
    db: Arc<DB>,
    write: WriteOptions,
    metric_written: Arc<AtomicBool>,
}

fn handle(db: &Arc<DB>, sync: bool, metric_written: &Arc<AtomicBool>) -> Handle {
    let mut write = WriteOptions::default();
    write.set_sync(sync);
    Handle {
        db: Arc::clone(db),
        write,
        metric_written: Arc::clone(metric_written),
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
        let db = DB::open_cf_descriptors(
            &opts,
            dir.join("rocksdb"),
            [ColumnFamilyDescriptor::new(METRIC_CF, metric)],
        )
        .map_err(|e| e.to_string())?;
        self.db = Some(Arc::new(db));
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

    fn client(&self) -> Option<Box<dyn Client>> {
        Some(Box::new(handle(
            self.db.as_ref()?,
            self.sync,
            &self.metric_written,
        )))
    }

    fn describe(&self) -> String {
        format!(
            "{} bloom={BLOOM_BITS} {}",
            self.memory,
            durability(self.sync)
        )
    }
}

impl Counted for RocksDbRunner {
    fn execute_counted(&mut self, op: &BenchOp) -> Result<Touched, String> {
        let db = self.db.as_ref().ok_or("rocksdb runner is not open")?;
        handle(db, self.sync, &self.metric_written).execute(op)
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

    /// One atomic batch of cells of one row, all stamped `ts`.
    fn put(
        &self,
        row: &[u8],
        family: &str,
        ts: u64,
        cells: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), String> {
        let f = family_id(family)?;
        let cf = self.cf(family)?;
        let mut batch = WriteBatch::default();
        for (q, v) in cells {
            let (key, value) = (keys::cell(row, f, q), encode_value(ts, v));
            match cf {
                Some(cf) => batch.put_cf(cf, key, value),
                None => batch.put(key, value),
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
