//! The Pigeonhole runner, through the public `pigeonhole` API only.

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pigeonhole::{Compaction, Durability, ErrorCode, Family, Options, Pigeonhole, Table};

use super::{BLOOM_BITS, Counted, MemoryBudget, Touched, durability, modified};
use crate::workload::{FAMILIES, METRIC_FAMILY, TIME_SERIES_TTL, YCSB_FAMILY};
use crate::{BenchOp, Client, PigeonholeRunner, Runner, ShardShare};

/// Default per-shard memtable budget of the runner ([`MemoryBudget`]'s write buffer): the
/// engine's own default.
pub const DEFAULT_MEMTABLE_BUDGET: u64 = 64 << 20;

#[derive(Debug, Clone, Default)]
pub(crate) struct Settings {
    pub(crate) shards: Option<usize>,
    pub(crate) memory: MemoryBudget,
    pub(crate) sync: bool,
    /// Commits are `GroupSync` (overrides `sync`).
    pub(crate) group_sync: bool,
    /// `None`: the library default (on).
    pub(crate) tablet_changes: Option<bool>,
    /// Row cache bytes (`Options::row_cache`); 0, the library default, is off.
    pub(crate) row_cache: u64,
}

#[derive(Debug)]
pub(crate) struct Open {
    db: Pigeonhole,
    table: Table,
    /// Writes retried after `Busy`, shared with every client.
    busy: Arc<AtomicU64>,
}

/// How many times a write refused with `Busy` is retried (each after the engine's own
/// stall timeout) before the run fails. The engine already stalls writers while a flush
/// frees room; `Busy` means that wait outlasted its timeout, which a bench on a slow
/// disk can hit. Retrying keeps the run going and the count is reported.
const BUSY_RETRIES: u32 = 20;

fn retry_busy<T>(
    busy: &AtomicU64,
    mut f: impl FnMut() -> Result<T, pigeonhole::Error>,
) -> Result<T, String> {
    let mut tries = 0;
    loop {
        match f() {
            Err(e) if e.code() == ErrorCode::Busy && tries < BUSY_RETRIES => {
                tries += 1;
                busy.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(50));
            }
            r => return r.map_err(err),
        }
    }
}

pub(crate) fn err(e: pigeonhole::Error) -> String {
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

    /// `true`: every commit is `Durability::GroupSync`-durable: fsynced before it returns,
    /// with concurrent commits sharing one fsync. Overrides [`sync`](Self::sync). The
    /// durability of [`WorkloadKind::GroupCommit`](crate::WorkloadKind::GroupCommit).
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// assert!(PigeonholeRunner::default().group_sync(true).describe().contains(" group-sync"));
    /// ```
    pub fn group_sync(mut self, yes: bool) -> Self {
        self.settings.group_sync = yes;
        self
    }

    /// `true` (the library default): tablets split, merge and move between shards, so the
    /// bench table's writes spread over every shard ([`Options::tablet_changes`]); `false`:
    /// the table is one tablet on one shard.
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// assert!(PigeonholeRunner::default().describe().contains("tablets=on"));
    /// let r = PigeonholeRunner::default().shards(4).tablet_changes(false);
    /// assert!(!r.describe().contains("tablets=on"));
    /// ```
    pub fn tablet_changes(mut self, yes: bool) -> Self {
        self.settings.tablet_changes = Some(yes);
        self
    }

    /// Row cache bytes (`Options::row_cache`, D201); 0 (the default) leaves it off. The
    /// gate window measures reads with it on and off to decide its default (#405).
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// assert!(!PigeonholeRunner::default().describe().contains("rowcache"));
    /// let r = PigeonholeRunner::default().row_cache(64 << 20);
    /// assert!(r.describe().contains(" rowcache=64MiB"));
    /// ```
    pub fn row_cache(mut self, bytes: u64) -> Self {
        self.settings.row_cache = bytes;
        self
    }
}

/// The store's options for `s` (shared by the runner and the inline scaling driver).
pub(crate) fn options(s: &Settings) -> Options {
    let mut options = Options::default()
        .durability(if s.group_sync {
            Durability::GroupSync
        } else if s.sync {
            Durability::Sync
        } else {
            Durability::Buffered
        })
        .memtable_budget(s.memory.write_buffer)
        .block_cache(usize::try_from(s.memory.cache).unwrap_or(usize::MAX));
    if let Some(yes) = s.tablet_changes {
        options = options.tablet_changes(yes);
    }
    if let Some(n) = s.shards {
        options = options.shards(n);
    }
    if s.row_cache > 0 {
        options = options.row_cache(usize::try_from(s.row_cache).unwrap_or(usize::MAX));
    }
    options
}

/// The bench table with every workload's families.
pub(crate) fn create_table(db: &Pigeonhole) -> Result<Table, String> {
    let mut builder = db.table("bench").map_err(err)?;
    for family in FAMILIES {
        let mut f = Family::default().max_versions(1).bloom_bits(BLOOM_BITS);
        if family == METRIC_FAMILY {
            // Whole SSTs drop once their newest point expires (D163, #236).
            f = f.ttl(TIME_SERIES_TTL).compaction(Compaction::FifoByTime);
        }
        builder = builder.family(family, f);
    }
    builder.create_if_missing().map_err(err)
}

impl Runner for PigeonholeRunner {
    fn name(&self) -> &'static str {
        "pigeonhole"
    }

    fn open(&mut self, dir: &Path) -> Result<(), String> {
        let db = Pigeonhole::open(dir.join("bench.phdb"), options(&self.settings)).map_err(err)?;
        let table = create_table(&db)?;
        self.open = Some(Open {
            db,
            table,
            busy: Arc::default(),
        });
        Ok(())
    }

    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        self.execute_counted(op).map(drop)
    }

    fn close(&mut self) -> Result<(), String> {
        match self.open.take() {
            Some(Open { db, table, .. }) => {
                drop(table);
                db.close().map_err(err)
            }
            None => Ok(()),
        }
    }

    fn busy_retries(&self) -> u64 {
        self.open
            .as_ref()
            .map_or(0, |o| o.busy.load(Ordering::Relaxed))
    }

    fn shard_shares(&self) -> Vec<ShardShare> {
        self.open.as_ref().map_or_else(Vec::new, |o| {
            o.db.shard_stats()
                .into_iter()
                .map(|s| ShardShare {
                    commits: s.commits,
                    tablets_start: s.tablets,
                    tablets_end: s.tablets,
                    splits: s.splits,
                    merges: s.merges,
                    moves: s.moves,
                })
                .collect()
        })
    }

    fn stalls(&self) -> Option<crate::Stalls> {
        let m = self.open.as_ref()?.db.engine_metrics();
        Some(crate::Stalls {
            write_stalls: m.stalls.0,
            write_stall_nanos: m.stalls.1,
            flushes: m.flushes,
            compactions: m.compactions,
            unpin_passes: m.unpin.0,
            unpin_flushes: m.unpin.1,
            wal_inline_syncs: m.wal_inline_syncs,
            file_growths: m.file_growths.0,
            file_growth_nanos: m.file_growths.1,
        })
    }

    fn client(&self) -> Option<Box<dyn Client>> {
        let open = self.open.as_ref()?;
        Some(Box::new(PigeonholeClient {
            table: open.table.clone(),
            busy: Arc::clone(&open.busy),
        }))
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
            "shards={shards} memtable={}MiB cache={}MiB bloom={BLOOM_BITS} {}{}{}",
            s.memory.write_buffer >> 20,
            s.memory.cache >> 20,
            if s.group_sync {
                "group-sync"
            } else {
                durability(s.sync)
            },
            if s.tablet_changes == Some(false) {
                ""
            } else {
                " tablets=on"
            },
            if s.row_cache > 0 {
                format!(" rowcache={}MiB", s.row_cache >> 20)
            } else {
                String::new()
            }
        )
    }
}

impl Counted for PigeonholeRunner {
    fn execute_counted(&mut self, op: &BenchOp) -> Result<Touched, String> {
        let o = self.open.as_ref().ok_or("pigeonhole runner is not open")?;
        execute(&o.table, &o.busy, op)
    }
}

struct PigeonholeClient {
    table: Table,
    busy: Arc<AtomicU64>,
}

impl Client for PigeonholeClient {
    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        execute(&self.table, &self.busy, op).map(drop)
    }
}

fn execute(table: &Table, busy: &AtomicU64, op: &BenchOp) -> Result<Touched, String> {
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
        BenchOp::GetRow { row, family } => {
            if let Some(row) = table.row(row).family(family).read().map_err(err)? {
                for entry in row.iter() {
                    t.cell(entry.cell.value());
                }
            }
        }
        BenchOp::Put { row, family, cells } => {
            retry_busy(busy, || {
                let mut m = table.mutate(row);
                for (q, v) in cells {
                    m = m.put(family, q, v);
                }
                m.commit()
            })?;
        }
        BenchOp::PutAt {
            row,
            family,
            ts,
            cells,
        } => {
            retry_busy(busy, || {
                let mut m = table.mutate(row);
                for (q, v) in cells {
                    m = m.put_at(family, q, *ts, v);
                }
                m.commit()
            })?;
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
            retry_busy(busy, || {
                table.mutate(row).put(YCSB_FAMILY, qualifier, &new).commit()
            })?;
        }
    }
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Workload, WorkloadConfig, WorkloadKind};

    fn opened(tag: &str) -> (PigeonholeRunner, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("phdb-bench-ph-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut r = PigeonholeRunner::default()
            .shards(1)
            .memtable_budget(16 << 20);
        r.open(&dir).unwrap();
        (r, dir)
    }

    #[test]
    fn cells_older_than_the_ttl_are_not_read() {
        let (mut r, dir) = opened("ttl");
        let mut w = Workload::new(WorkloadConfig::smoke(WorkloadKind::TimeSeriesTtl));
        let (mut live, mut dead) = (0, 0);
        for op in w.load_ops() {
            r.execute(&op).unwrap();
            let BenchOp::PutAt {
                row, family, ts, ..
            } = &op
            else {
                panic!("the time-series load is timestamped puts")
            };
            let get = BenchOp::Get {
                row: row.clone(),
                family,
                qualifier: b"v".to_vec(),
            };
            let by_row = BenchOp::GetRow {
                row: row.clone(),
                family,
            };
            let got = r.execute_counted(&get).unwrap().cells;
            assert_eq!(got, r.execute_counted(&by_row).unwrap().cells);
            let now = crate::runners::now_micros();
            assert_eq!(got == 0, ts + TIME_SERIES_TTL.as_micros() as u64 <= now);
            if got == 0 { dead += 1 } else { live += 1 }
        }
        assert_eq!(dead * 3, live, "a quarter of the points are expired");
        r.close().unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_row_read_returns_every_field() {
        let (mut r, dir) = opened("row");
        let mut w = Workload::new(WorkloadConfig::smoke(WorkloadKind::YcsbC));
        let load: Vec<BenchOp> = w.load_ops().collect();
        for op in &load {
            r.execute(op).unwrap();
        }
        let BenchOp::Put { row, family, .. } = &load[0] else {
            panic!("the YCSB load is puts")
        };
        let read = BenchOp::GetRow {
            row: row.clone(),
            family,
        };
        assert_eq!(r.execute_counted(&read).unwrap().cells, 10);
        r.close().unwrap();
        std::fs::remove_dir_all(dir).ok();
    }
}
