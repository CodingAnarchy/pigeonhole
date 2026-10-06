//! The Pigeonhole runner, through the public `pigeonhole` API only.

use std::ops::Bound;
use std::path::Path;

use pigeonhole::{Durability, Family, Options, Pigeonhole, Table};

use super::{BLOOM_BITS, Counted, MemoryBudget, Touched, durability, modified};
use crate::workload::{FAMILIES, METRIC_FAMILY, TIME_SERIES_TTL, YCSB_FAMILY};
use crate::{BenchOp, Client, PigeonholeRunner, Runner};

/// Default per-shard memtable budget of the runner ([`MemoryBudget`]'s write buffer).
/// Until the engine flushes to SSTs (#37) all data stays in memtables and one table
/// lives on one shard, so this bounds the data set of a run.
pub const DEFAULT_MEMTABLE_BUDGET: u64 = 256 << 20;

#[derive(Debug, Clone, Default)]
pub(crate) struct Settings {
    pub(crate) shards: Option<usize>,
    pub(crate) memory: MemoryBudget,
    pub(crate) sync: bool,
}

#[derive(Debug)]
pub(crate) struct Open {
    db: Pigeonhole,
    table: Table,
}

fn err(e: pigeonhole::Error) -> String {
    format!("{:?}: {}", e.code(), e.message())
}

impl PigeonholeRunner {
    /// Shard threads (default: the engine default, one per available CPU).
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// let r = PigeonholeRunner::default().shards(2).memtable_budget(64 << 20);
    /// assert_eq!(r.name(), "pigeonhole");
    /// assert!(r.describe().contains("shards=2"));
    /// ```
    pub fn shards(mut self, n: usize) -> Self {
        self.settings.shards = Some(n);
        self
    }

    /// Memtable bytes per shard (default [`DEFAULT_MEMTABLE_BUDGET`]); the write buffer
    /// of the [`MemoryBudget`].
    pub fn memtable_budget(mut self, bytes: u64) -> Self {
        self.settings.memory.write_buffer = bytes;
        self
    }

    /// Memtable budget per shard and block cache.
    pub fn memory(mut self, memory: MemoryBudget) -> Self {
        self.settings.memory = memory;
        self
    }

    /// `true`: every commit is `Durability::Sync`-durable (fsync); `false` (default):
    /// `Buffered`, written to the OS before the commit returns but not fsynced.
    pub fn sync(mut self, yes: bool) -> Self {
        self.settings.sync = yes;
        self
    }

    fn table(&self) -> Result<&Table, String> {
        self.open
            .as_ref()
            .map(|o| &o.table)
            .ok_or_else(|| "pigeonhole runner is not open".to_owned())
    }
}

impl Runner for PigeonholeRunner {
    fn name(&self) -> &'static str {
        "pigeonhole"
    }

    fn open(&mut self, dir: &Path) -> Result<(), String> {
        let s = &self.settings;
        let mut options = Options::default()
            .durability(if s.sync {
                Durability::Sync
            } else {
                Durability::Buffered
            })
            .memtable_budget(s.memory.write_buffer)
            .block_cache(usize::try_from(s.memory.cache).unwrap_or(usize::MAX));
        if let Some(n) = s.shards {
            options = options.shards(n);
        }
        let db = Pigeonhole::open(dir.join("bench.phdb"), options).map_err(err)?;
        let mut builder = db.table("bench").map_err(err)?;
        for family in FAMILIES {
            let mut f = Family::default().max_versions(1).bloom_bits(BLOOM_BITS);
            if family == METRIC_FAMILY {
                f = f.ttl(TIME_SERIES_TTL);
            }
            builder = builder.family(family, f);
        }
        let table = builder.create_if_missing().map_err(err)?;
        self.open = Some(Open { db, table });
        Ok(())
    }

    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        self.execute_counted(op).map(drop)
    }

    fn close(&mut self) -> Result<(), String> {
        match self.open.take() {
            Some(Open { db, table }) => {
                drop(table);
                db.close().map_err(err)
            }
            None => Ok(()),
        }
    }

    fn client(&self) -> Option<Box<dyn Client>> {
        let table = self.open.as_ref()?.table.clone();
        Some(Box::new(PigeonholeClient { table }))
    }

    fn describe(&self) -> String {
        let s = &self.settings;
        let shards = s.shards.map_or_else(
            || {
                format!(
                    "default({})",
                    std::thread::available_parallelism().map_or(1, |n| n.get())
                )
            },
            |n| n.to_string(),
        );
        format!(
            "shards={shards} memtable={}MiB cache={}MiB bloom={BLOOM_BITS} {}",
            s.memory.write_buffer >> 20,
            s.memory.cache >> 20,
            durability(s.sync)
        )
    }
}

impl Counted for PigeonholeRunner {
    fn execute_counted(&mut self, op: &BenchOp) -> Result<Touched, String> {
        execute(self.table()?, op)
    }
}

struct PigeonholeClient {
    table: Table,
}

impl Client for PigeonholeClient {
    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        execute(&self.table, op).map(drop)
    }
}

fn execute(table: &Table, op: &BenchOp) -> Result<Touched, String> {
    let mut t = Touched::default();
    match op {
        BenchOp::Get {
            row,
            family,
            qualifier,
        } => {
            if let Some(cell) = table.get(row, family, qualifier).map_err(err)? {
                t.cell(cell.value());
            }
        }
        BenchOp::Put { row, family, cells } => {
            let mut m = table.mutate(row);
            for (q, v) in cells {
                m = m.put(family, q, v);
            }
            m.commit().map_err(err)?;
        }
        BenchOp::Scan { start, len } => {
            let mut it = table
                .scan_bounds(Bound::Included(start.as_slice()), Bound::Unbounded)
                .limit(u64::from(*len))
                .iter()
                .map_err(err)?;
            while let Some(row) = it.next_ref().map_err(err)? {
                for entry in row.iter() {
                    t.cell(entry.cell.value());
                }
            }
        }
        BenchOp::ReadModifyWrite { row, qualifier } => {
            let old = table.get(row, YCSB_FAMILY, qualifier).map_err(err)?;
            let new = modified(old.as_ref().map(|c| c.value()));
            if let Some(cell) = &old {
                t.cell(cell.value());
            }
            drop(old);
            table
                .mutate(row)
                .put(YCSB_FAMILY, qualifier, &new)
                .commit()
                .map_err(err)?;
        }
    }
    Ok(t)
}
