//! fjall with the same hand-written wide-column key encoding as the RocksDB runner
//! ([`super::keys`]): one keyspace, one key per cell, latest value only, each value prefixed with the cell's timestamp; the TTL filter runs on read. Default fjall
//! options (which include bloom filters and no global write-buffer cap) except the
//! [`MemoryBudget`]: keyspace memtable size and block cache.

use std::path::Path;

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};

use super::{
    Counted, MemoryBudget, Touched, durability, encode_value, family_id, keys, live_value,
    modified, now_micros, read_family, scan_rows,
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
}

#[derive(Clone)]
struct Handle {
    db: Database,
    cells: Keyspace,
    mode: PersistMode,
}

impl Runner for FjallRunner {
    fn name(&self) -> &'static str {
        "fjall"
    }

    fn open(&mut self, dir: &Path) -> Result<(), String> {
        let e = |e: fjall::Error| e.to_string();
        let m = self.memory;
        let db = Database::builder(dir.join("fjall"))
            .cache_size(m.cache)
            .open()
            .map_err(e)?;
        let cells = db
            .keyspace("cells", || {
                KeyspaceCreateOptions::default().max_memtable_size(m.write_buffer)
            })
            .map_err(e)?;
        self.open = Some(Handle {
            db,
            cells,
            mode: if self.sync {
                PersistMode::SyncAll
            } else {
                PersistMode::Buffer
            },
        });
        Ok(())
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

    fn describe(&self) -> String {
        format!("{} bloom=default {}", self.memory, durability(self.sync))
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
    /// One atomic batch of cells of one row, all stamped `ts`.
    fn put(
        &self,
        row: &[u8],
        family: &str,
        ts: u64,
        cells: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), String> {
        let f = family_id(family)?;
        let mut batch = self.db.batch().durability(Some(self.mode));
        for (q, v) in cells {
            batch.insert(&self.cells, keys::cell(row, f, q), encode_value(ts, v));
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
            BenchOp::Scan { start, len } => {
                let from = keys::row_prefix(start);
                let iter = self.cells.range(from..).map(|g| g.into_inner());
                t = scan_rows(iter, *len, now_micros())?;
            }
            BenchOp::ReadModifyWrite { row, qualifier } => {
                let key = keys::cell(row, family_id(YCSB_FAMILY)?, qualifier);
                let f = family_id(YCSB_FAMILY)?;
                let stored = self.cells.get(&key).map_err(e)?;
                let old = match &stored {
                    Some(v) => live_value(f, v, now_micros())?,
                    None => None,
                };
                if let Some(v) = old {
                    t.cell(v);
                }
                let new = encode_value(now_micros(), &modified(old));
                let mut batch = self.db.batch().durability(Some(self.mode));
                batch.insert(&self.cells, key, new);
                batch.commit().map_err(e)?;
            }
        }
        Ok(t)
    }
}
