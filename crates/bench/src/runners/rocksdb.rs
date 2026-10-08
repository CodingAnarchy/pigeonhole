//! RocksDB with a hand-written wide-column key encoding ([`super::keys`]): one column
//! family, one key per cell, latest value only, each value prefixed with the cell's timestamp; the
//! TTL filter runs on read. Default RocksDB options except the
//! [`MemoryBudget`] (write buffer, LRU block cache) and a 10-bit bloom filter; no
//! compression codecs are compiled in.

use std::path::Path;
use std::sync::Arc;

use rocksdb::{
    BlockBasedOptions, Cache, DB, Direction, IteratorMode, Options, WriteBatch, WriteOptions,
};

use super::{
    BLOOM_BITS, Counted, MemoryBudget, Touched, durability, encode_value, family_id, keys,
    live_value, modified, now_micros, read_family, scan_rows,
};
use crate::workload::YCSB_FAMILY;
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
}

fn handle(db: &Arc<DB>, sync: bool) -> Handle {
    let mut write = WriteOptions::default();
    write.set_sync(sync);
    Handle {
        db: Arc::clone(db),
        write,
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
        let db = DB::open(&opts, dir.join("rocksdb")).map_err(|e| e.to_string())?;
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
        Some(Box::new(handle(self.db.as_ref()?, self.sync)))
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
        handle(db, self.sync).execute(op)
    }
}

impl Client for Handle {
    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        Handle::execute(self, op).map(drop)
    }
}

impl Handle {
    /// One atomic batch of cells of one row, all stamped `ts`.
    fn put(
        &self,
        row: &[u8],
        family: &str,
        ts: u64,
        cells: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), String> {
        let f = family_id(family)?;
        let mut batch = WriteBatch::default();
        for (q, v) in cells {
            batch.put(keys::cell(row, f, q), encode_value(ts, v));
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
                if let Some(v) = self.db.get_pinned(key).map_err(e)?
                    && let Some(v) = live_value(f, &v, now_micros())?
                {
                    t.cell(v);
                }
            }
            BenchOp::GetRow { row, family } => {
                let mut prefix = keys::row_prefix(row);
                prefix.push(family_id(family)?);
                let iter = self
                    .db
                    .iterator(IteratorMode::From(&prefix, Direction::Forward));
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
            BenchOp::Scan { start, len } => {
                let from = keys::row_prefix(start);
                let iter = self
                    .db
                    .iterator(IteratorMode::From(&from, Direction::Forward));
                t = scan_rows(iter, *len, now_micros())?;
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
