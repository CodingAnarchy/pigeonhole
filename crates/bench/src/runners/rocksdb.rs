//! RocksDB with a hand-written wide-column key encoding ([`super::keys`]): one column
//! family, one key per cell, latest value only. Default RocksDB options; no compression
//! codecs are compiled in.

use std::path::Path;
use std::sync::Arc;

use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch, WriteOptions};

use super::{Counted, Touched, family_id, keys, modified, scan_rows};
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
    db: Option<Arc<DB>>,
}

impl std::fmt::Debug for RocksDbRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksDbRunner")
            .field("sync", &self.sync)
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
        if self.sync { "sync" } else { "buffered" }.to_owned()
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
    fn execute(&self, op: &BenchOp) -> Result<Touched, String> {
        let e = |e: rocksdb::Error| e.to_string();
        let mut t = Touched::default();
        match op {
            BenchOp::Get {
                row,
                family,
                qualifier,
            } => {
                let key = keys::cell(row, family_id(family)?, qualifier);
                if let Some(v) = self.db.get_pinned(key).map_err(e)? {
                    t.cell(&v);
                }
            }
            BenchOp::Put { row, family, cells } => {
                let f = family_id(family)?;
                let mut batch = WriteBatch::default();
                for (q, v) in cells {
                    batch.put(keys::cell(row, f, q), v);
                }
                self.db.write_opt(batch, &self.write).map_err(e)?;
            }
            BenchOp::Scan { start, len } => {
                let from = keys::row_prefix(start);
                let iter = self
                    .db
                    .iterator(IteratorMode::From(&from, Direction::Forward));
                t = scan_rows(iter, *len)?;
            }
            BenchOp::ReadModifyWrite { row, qualifier } => {
                let key = keys::cell(row, family_id(YCSB_FAMILY)?, qualifier);
                let old = self.db.get_pinned(&key).map_err(e)?;
                if let Some(v) = &old {
                    t.cell(v);
                }
                let new = modified(old.as_deref());
                drop(old);
                self.db.put_opt(key, new, &self.write).map_err(e)?;
            }
        }
        Ok(t)
    }
}
