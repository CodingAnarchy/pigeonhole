//! fjall with the same hand-written wide-column key encoding as the RocksDB runner
//! ([`super::keys`]): one keyspace, one key per cell, latest value only. Default fjall
//! options.

use std::path::Path;

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};

use super::{Counted, Touched, family_id, keys, modified, scan_rows};
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
    open: Option<Handle>,
}

impl std::fmt::Debug for FjallRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallRunner")
            .field("sync", &self.sync)
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
        let db = Database::builder(dir.join("fjall")).open().map_err(e)?;
        let cells = db
            .keyspace("cells", KeyspaceCreateOptions::default)
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
        if self.sync { "sync" } else { "buffered" }.to_owned()
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
    fn execute(&self, op: &BenchOp) -> Result<Touched, String> {
        let e = |e: fjall::Error| e.to_string();
        let mut t = Touched::default();
        match op {
            BenchOp::Get {
                row,
                family,
                qualifier,
            } => {
                let key = keys::cell(row, family_id(family)?, qualifier);
                if let Some(v) = self.cells.get(key).map_err(e)? {
                    t.cell(&v);
                }
            }
            BenchOp::Put { row, family, cells } => {
                let f = family_id(family)?;
                let mut batch = self.db.batch().durability(Some(self.mode));
                for (q, v) in cells {
                    batch.insert(&self.cells, keys::cell(row, f, q), v.as_slice());
                }
                batch.commit().map_err(e)?;
            }
            BenchOp::Scan { start, len } => {
                let from = keys::row_prefix(start);
                let iter = self.cells.range(from..).map(|g| g.into_inner());
                t = scan_rows(iter, *len)?;
            }
            BenchOp::ReadModifyWrite { row, qualifier } => {
                let key = keys::cell(row, family_id(YCSB_FAMILY)?, qualifier);
                let old = self.cells.get(&key).map_err(e)?;
                if let Some(v) = &old {
                    t.cell(v);
                }
                let new = modified(old.as_deref());
                let mut batch = self.db.batch().durability(Some(self.mode));
                batch.insert(&self.cells, key, new);
                batch.commit().map_err(e)?;
            }
        }
        Ok(t)
    }
}
