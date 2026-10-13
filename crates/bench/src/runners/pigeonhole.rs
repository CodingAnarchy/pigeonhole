//! The Pigeonhole runner, through the public `pigeonhole` API only.

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pigeonhole::{
    Compaction, Durability, ErrorCode, Family, Options, Pigeonhole, RowMutation, Table,
};

use super::{BLOOM_BITS, Counted, MemoryBudget, Touched, durability, modified};
use crate::workload::{FAMILIES, METRIC_FAMILY, TIME_SERIES_TTL, XSHARD_PARTITIONS, YCSB_FAMILY};
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
    /// Commits keep the library's default durability, `Options::default()`'s (overrides
    /// `sync` and `group_sync`; D247).
    pub(crate) default_durability: bool,
    /// The load commits `Buffered`, the measured phase at the run's durability (D247).
    pub(crate) load_buffered: bool,
    /// `None`: the library default (on).
    pub(crate) tablet_changes: Option<bool>,
    /// Packing (`Options::tablet_packing`, D217); on, the library default.
    pub(crate) no_packing: bool,
    /// Row cache bytes (`Options::row_cache`); `None` keeps the library default (D216), 0
    /// turns it off.
    pub(crate) row_cache: Option<u64>,
    /// A shard's idle spin before it parks (`Options::shard_spin`, D198), in microseconds;
    /// `None`: the library default.
    pub(crate) shard_spin_micros: Option<u64>,
    /// The inline driver splits the table over every shard before loading it (D213).
    pub(crate) presplit: bool,
    /// The pre-split cuts at the measured writes' quantiles rather than the loaded rows', so
    /// each piece takes about the same share of the writes (D231's engine scaling).
    pub(crate) presplit_writes: bool,
}

#[derive(Debug)]
pub(crate) struct Open {
    target: Target,
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
    /// Shard threads (default: the engine default, one per physical core, D220).
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

    /// `true`: commits keep the library's default durability (`Options::default()`, which is
    /// `Durability::GroupSync`), whatever [`sync`](Self::sync) and
    /// [`group_sync`](Self::group_sync) say: the durability D247's `contended` step measures.
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// let r = PigeonholeRunner::default().default_durability(true);
    /// assert!(r.describe().contains(" group-sync(default)"));
    /// ```
    pub fn default_durability(mut self, yes: bool) -> Self {
        self.settings.default_durability = yes;
        self
    }

    /// `true`: the load commits `Buffered` (D108's parity level), and the warmup and the
    /// measured phase at the run's durability, so a durable level's run is not mostly its
    /// load (D247). The settle step flushes the load either way.
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// let r = PigeonholeRunner::default().default_durability(true).load_buffered(true);
    /// assert!(r.describe().contains(" group-sync(default) load=buffered"));
    /// ```
    pub fn load_buffered(mut self, yes: bool) -> Self {
        self.settings.load_buffered = yes;
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

    /// `false` turns off packing a lightly written store's tablets onto few shards
    /// ([`Options::tablet_packing`], D217; on by default).
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// let r = PigeonholeRunner::default().packing(false);
    /// assert!(r.describe().contains("packing=off"));
    /// ```
    pub fn packing(mut self, yes: bool) -> Self {
        self.settings.no_packing = !yes;
        self
    }

    /// Row cache bytes (`Options::row_cache`, D201); 0 turns it off. Without a call the
    /// library default applies (on, D216), and the description names no row cache, so runs
    /// stay comparable with results from before D216 under the same key.
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// assert!(!PigeonholeRunner::default().describe().contains("rowcache"));
    /// let r = PigeonholeRunner::default().row_cache(64 << 20);
    /// assert!(r.describe().contains(" rowcache=64MiB"));
    /// assert!(PigeonholeRunner::default().row_cache(0).describe().contains(" rowcache=off"));
    /// ```
    pub fn row_cache(mut self, bytes: u64) -> Self {
        self.settings.row_cache = Some(bytes);
        self
    }

    /// A shard's idle spin before it parks, in microseconds (`Options::shard_spin`, D198);
    /// the library default when not set.
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// assert!(!PigeonholeRunner::default().describe().contains("shard_spin"));
    /// let r = PigeonholeRunner::default().shard_spin_micros(200);
    /// assert!(r.describe().contains(" shard_spin=200us"));
    /// ```
    pub fn shard_spin_micros(mut self, micros: u64) -> Self {
        self.settings.shard_spin_micros = Some(micros);
        self
    }

    /// `true`: [`run_inline`](Self::run_inline) splits the table at the loaded rows'
    /// quantiles and hands one piece to each shard before loading it, so every shard owns
    /// rows from the start instead of waiting for the balancer to spread them (D213's
    /// group-commit shape). The balancer then never runs, so the placement holds: the run
    /// fails if the split did not leave one tablet per shard, or if it changed before the
    /// measured phase.
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// assert!(PigeonholeRunner::default().presplit(true).describe().contains(" presplit"));
    /// ```
    pub fn presplit(mut self, yes: bool) -> Self {
        self.settings.presplit = yes;
        self
    }

    /// `true`: as [`presplit`](Self::presplit), cutting by the measured writes (each row
    /// weighing its writes) so the heaviest piece is as light as it can be (D231: engine
    /// scaling, judged on per-shard throughput with the placement fixed). A row hotter than a
    /// piece's share sits alone.
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// let r = PigeonholeRunner::default().presplit_writes(true);
    /// assert!(r.describe().contains(" presplit=writes"));
    /// ```
    pub fn presplit_writes(mut self, yes: bool) -> Self {
        self.settings.presplit = yes;
        self.settings.presplit_writes = yes;
        self
    }
}

/// How [`describe`](Runner::describe) names the library's default durability, `GroupSync`
/// (`Options::default()`; a test checks a default commit's level).
fn default_durability() -> &'static str {
    "group-sync(default)"
}

/// The store's options for `s` (shared by the runner and the inline scaling driver).
pub(crate) fn options(s: &Settings) -> Options {
    let mut options = Options::default();
    if !s.default_durability {
        options = options.durability(if s.group_sync {
            Durability::GroupSync
        } else if s.sync {
            Durability::Sync
        } else {
            Durability::Buffered
        });
    }
    let mut options = options
        .memtable_budget(s.memory.write_buffer)
        .block_cache(usize::try_from(s.memory.cache).unwrap_or(usize::MAX));
    if let Some(yes) = s.tablet_changes {
        options = options.tablet_changes(yes);
    }
    if s.no_packing {
        options = options.tablet_packing(false);
    }
    if let Some(n) = s.shards {
        options = options.shards(n);
    }
    if let Some(bytes) = s.row_cache {
        options = options.row_cache(usize::try_from(bytes).unwrap_or(usize::MAX));
    }
    if let Some(us) = s.shard_spin_micros {
        options = options.shard_spin(Duration::from_micros(us));
    }
    if s.presplit {
        // The pre-split placement stays fixed (D213): no balancer pass ever falls due, and no
        // load or size threshold is reached, while tablet changes stay on for the split
        // itself and commits still track their tablets' load as by default.
        options = options.tablet_balance(Duration::MAX, u64::MAX, u64::MAX);
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

/// The cross-shard contention workload's partition tables (D246), `xs00` to `xs15`, created
/// one after another as perf2's measurement did, so with tablet changes off the engine
/// places them one per shard.
fn create_partitions(db: &Pigeonhole) -> Result<Vec<Table>, String> {
    (0..XSHARD_PARTITIONS)
        .map(|p| {
            let f = Family::default().max_versions(1).bloom_bits(BLOOM_BITS);
            db.table(&format!("xs{p:02}"))
                .map_err(err)?
                .family(YCSB_FAMILY, f)
                .create_if_missing()
                .map_err(err)
        })
        .collect()
}

impl Runner for PigeonholeRunner {
    fn name(&self) -> &'static str {
        "pigeonhole"
    }

    fn open(&mut self, dir: &Path) -> Result<(), String> {
        let db = Pigeonhole::open(dir.join("bench.phdb"), options(&self.settings)).map_err(err)?;
        let table = create_table(&db)?;
        self.open = Some(Open {
            target: Target {
                db,
                table,
                parts: Arc::default(),
                load_buffered: self.settings.load_buffered,
            },
            busy: Arc::default(),
        });
        Ok(())
    }

    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        self.execute_counted(op).map(drop)
    }

    fn close(&mut self) -> Result<(), String> {
        match self.open.take() {
            Some(Open {
                target: Target {
                    db, table, parts, ..
                },
                ..
            }) => {
                drop((table, parts));
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
            o.target
                .db
                .shard_stats()
                .into_iter()
                .map(|s| ShardShare {
                    commits: s.commits,
                    tablets_start: s.tablets,
                    tablets_end: s.tablets,
                    splits: s.splits,
                    merges: s.merges,
                    moves: s.moves,
                    busy_nanos: s.busy_nanos,
                })
                .collect()
        })
    }

    fn stalls(&self) -> Option<crate::Stalls> {
        Some(stalls(&self.open.as_ref()?.target.db))
    }

    fn flush(&mut self) -> Result<(), String> {
        match &self.open {
            Some(o) => o.target.db.flush().map_err(err),
            None => Ok(()),
        }
    }

    fn background(&self) -> Option<crate::Background> {
        let db = &self.open.as_ref()?.target.db;
        let m = db.engine_metrics();
        let work = m.flushes + m.compactions;
        Some(crate::Background {
            work: Some(work),
            marker: work,
            running: None,
            busy_nanos: Some(db.shard_stats().iter().map(|s| s.busy_nanos).sum()),
        })
    }

    fn measured_durability(&mut self) -> Result<(), String> {
        if let Some(o) = self.open.as_mut() {
            o.target.load_buffered = false;
        }
        Ok(())
    }

    fn seqno(&self) -> Option<u64> {
        self.open
            .as_ref()?
            .target
            .db
            .snapshot()
            .ok()
            .map(|s| s.seqno())
    }

    fn client(&self) -> Option<Box<dyn Client>> {
        let open = self.open.as_ref()?;
        Some(Box::new(PigeonholeClient {
            target: open.target.clone(),
            busy: Arc::clone(&open.busy),
        }))
    }

    fn describe(&self) -> String {
        let s = &self.settings;
        let shards = s.shards.map_or_else(
            || format!("default({})", pigeonhole_io::sys::default_shards()),
            |n| n.to_string(),
        );
        format!(
            "shards={shards} memtable={}MiB cache={}MiB bloom={BLOOM_BITS} {}{}{}{}{}{}",
            s.memory.write_buffer >> 20,
            s.memory.cache >> 20,
            if s.default_durability {
                default_durability()
            } else if s.group_sync {
                "group-sync"
            } else {
                durability(s.sync)
            },
            if s.load_buffered {
                " load=buffered"
            } else {
                ""
            },
            match (s.tablet_changes == Some(false), s.no_packing) {
                (true, _) => "",
                (false, false) => " tablets=on",
                (false, true) => " tablets=on packing=off",
            },
            match s.row_cache {
                None => String::new(),
                Some(0) => " rowcache=off".to_owned(),
                Some(bytes) => format!(" rowcache={}MiB", bytes >> 20),
            },
            s.shard_spin_micros
                .map_or_else(String::new, |us| format!(" shard_spin={us}us")),
            match (s.presplit, s.presplit_writes) {
                (_, true) => " presplit=writes",
                (true, false) => " presplit",
                _ => "",
            }
        )
    }
}

impl Counted for PigeonholeRunner {
    fn execute_counted(&mut self, op: &BenchOp) -> Result<Touched, String> {
        let o = self.open.as_mut().ok_or("pigeonhole runner is not open")?;
        if matches!(op, BenchOp::Commit { .. }) && o.target.parts.is_empty() {
            o.target.parts = Arc::new(create_partitions(&o.target.db)?);
        }
        execute(&o.target, &o.busy, op)
    }
}

/// What an operation runs against: the bench table, and the partition tables with the
/// database for multi-table commits.
#[derive(Debug, Clone)]
pub(crate) struct Target {
    db: Pigeonhole,
    table: Table,
    parts: Arc<Vec<Table>>,
    /// Commits are `Buffered` (the load of a run set to [`PigeonholeRunner::load_buffered`]).
    load_buffered: bool,
}

struct PigeonholeClient {
    target: Target,
    busy: Arc<AtomicU64>,
}

impl Client for PigeonholeClient {
    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        execute(&self.target, &self.busy, op).map(drop)
    }
}

fn execute(target: &Target, busy: &AtomicU64, op: &BenchOp) -> Result<Touched, String> {
    let table = &target.table;
    // A load at buffered durability (D247).
    let buffered = target.load_buffered;
    fn level(buffered: bool, m: RowMutation<'_>) -> RowMutation<'_> {
        if buffered {
            m.durability(Durability::Buffered)
        } else {
            m
        }
    }
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
                let mut m = level(buffered, table.mutate(row));
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
                let mut m = level(buffered, table.mutate(row));
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
                level(buffered, table.mutate(row))
                    .put(YCSB_FAMILY, qualifier, &new)
                    .commit()
            })?;
        }
        BenchOp::Commit { family, rows } => {
            let part = |p: usize| {
                target
                    .parts
                    .get(p)
                    .ok_or_else(|| format!("no partition table {p}: the load creates them"))
            };
            if let [r] = rows.as_slice() {
                let table = part(r.partition)?;
                retry_busy(busy, || {
                    let mut m = level(buffered, table.mutate(&r.row));
                    for (q, v) in &r.cells {
                        m = m.put(family, q, v);
                    }
                    m.commit()
                })?;
            } else {
                let tables = rows
                    .iter()
                    .map(|r| part(r.partition))
                    .collect::<Result<Vec<_>, _>>()?;
                retry_busy(busy, || {
                    let mut wb = target.db.write_batch();
                    for (r, table) in rows.iter().zip(&tables) {
                        for (q, v) in &r.cells {
                            wb.put(table, &r.row, family, q, v);
                        }
                    }
                    if target.load_buffered {
                        wb.commit_with(Durability::Buffered)
                    } else {
                        wb.commit()
                    }
                })?;
            }
        }
    }
    Ok(t)
}

/// The engine's cumulative stall and visibility counters ([`crate::Stalls`], ICR 0015,
/// ICR 0023).
pub(crate) fn stalls(db: &Pigeonhole) -> crate::Stalls {
    let m = db.engine_metrics();
    let rc = db.row_cache_stats();
    crate::Stalls {
        write_stalls: m.write_stalls,
        write_stall_nanos: m.write_stall_nanos,
        flushes: m.flushes,
        compactions: m.compactions,
        unpin_passes: m.wal_unpin_passes,
        unpin_flushes: m.wal_unpin_flushes,
        wal_inline_syncs: m.wal_inline_syncs,
        file_growths: m.file_growths,
        file_growth_nanos: m.file_growth_nanos,
        file_growths_ahead: m.file_growths_ahead,
        shard_parks: m.shard_parks,
        shard_wakes: m.shard_wakes,
        commit_parks: m.commit_parks,
        visibility_waits: m.visibility_waits,
        visibility_scanned: m.visibility_waiters_ahead,
        visibility_wake_passes: m.visibility_wake_passes,
        visibility_woken: m.visibility_woken,
        visibility_contended: m.visibility_contended,
        wal_rollover_blocks: m.wal_rollover_blocks,
        slot_refusals: db.tablet_refusals().0,
        row_cache_hits: rc.hits,
        row_cache_misses: rc.misses,
        row_cache_fills: rc.fills,
    }
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
    fn a_cross_shard_commit_writes_each_row_in_its_partition_table() {
        // D246: tablet changes off, so the partition tables sit one per shard.
        let dir = std::env::temp_dir().join(format!("phdb-bench-ph-xs-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut r = PigeonholeRunner::default()
            .shards(4)
            .tablet_changes(false)
            .memtable_budget(16 << 20);
        r.open(&dir).unwrap();
        let mut w = Workload::new(WorkloadConfig::smoke(WorkloadKind::CrossShardContention));
        for op in w.load_ops() {
            r.execute(&op).unwrap();
        }
        let op = w
            .run_ops()
            .find(|op| op.type_name() == crate::CROSS_SHARD_COMMIT)
            .expect("the run has cross-shard commits");
        let before = r.seqno().unwrap();
        r.execute(&op).unwrap();
        assert!(r.seqno().unwrap() > before);
        let BenchOp::Commit { family, rows } = &op else {
            unreachable!()
        };
        let parts = &r.open.as_ref().unwrap().target.parts;
        for row in rows {
            let (q, v) = &row.cells[0];
            let got = parts[row.partition].get(&row.row, family, q).unwrap();
            assert_eq!(got.as_ref().map(|c| c.value()), Some(v.as_slice()));
            // Not in any other partition.
            for (p, t) in parts.iter().enumerate() {
                if p != row.partition {
                    assert!(t.get(&row.row, family, q).unwrap().is_none());
                }
            }
        }
        r.close().unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_contention_run_counts_its_cross_shard_commits_and_seqnos() {
        let dir = std::env::temp_dir().join(format!("phdb-bench-ph-xsr-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut r = PigeonholeRunner::default()
            .shards(4)
            .memtable_budget(16 << 20);
        let config = WorkloadConfig::smoke(WorkloadKind::CrossShardContention);
        let options = crate::RunOptions {
            warmup: 0.0,
            ..crate::RunOptions::default()
        };
        let rec = crate::run_detailed(&mut r, &config, &dir, &options).unwrap();
        let count = |name: &str| {
            rec.detail
                .by_type
                .iter()
                .find(|t| t.op == name)
                .map_or(0, |t| t.stats.count)
        };
        let cross = count(crate::CROSS_SHARD_COMMIT);
        assert!(cross > 0);
        assert_eq!(cross + count("put"), config.operations);
        // Every commit uses a sequence number; refused rounds use more.
        assert!(rec.detail.seqnos_used.unwrap() >= config.operations);
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn the_default_durability_is_group_sync() {
        // D247: `default_durability` keeps `Options::default()`'s level, which `describe`
        // names group-sync.
        let dir = std::env::temp_dir().join(format!("phdb-bench-ph-dur-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut r = PigeonholeRunner::default()
            .shards(1)
            .sync(true)
            .default_durability(true);
        r.open(&dir).unwrap();
        let info = r
            .open
            .as_ref()
            .unwrap()
            .target
            .table
            .mutate(b"r")
            .put(YCSB_FAMILY, b"q", b"v");
        assert_eq!(info.commit().unwrap().durability, Durability::GroupSync);
        assert!(r.describe().contains(default_durability()));
        r.close().unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_buffered_load_switches_to_the_runs_durability_after_the_settle() {
        // D247: the load commits Buffered; from `measured_durability` on, the default level.
        let dir = std::env::temp_dir().join(format!("phdb-bench-ph-lb-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut r = PigeonholeRunner::default()
            .shards(1)
            .default_durability(true)
            .load_buffered(true);
        r.open(&dir).unwrap();
        let levels = |r: &PigeonholeRunner| -> Vec<u64> {
            let db = &r.open.as_ref().unwrap().target.db;
            db.commit_latency_buckets()
                .iter()
                .map(|b| b.iter().sum())
                .collect()
        };
        let put = |r: &mut PigeonholeRunner, i: u32| {
            r.execute(&BenchOp::Put {
                row: format!("r{i}").into_bytes(),
                family: YCSB_FAMILY,
                cells: vec![(b"q".to_vec(), b"v".to_vec())],
            })
            .unwrap();
        };
        let start = levels(&r);
        for i in 0..10 {
            put(&mut r, i);
        }
        let loaded = levels(&r);
        r.measured_durability().unwrap();
        for i in 10..20 {
            put(&mut r, i);
        }
        let measured = levels(&r);
        let grew = |a: &[u64], b: &[u64]| -> Vec<usize> {
            (0..a.len()).filter(|&l| b[l] > a[l]).collect()
        };
        let (load_level, run_level) = (grew(&start, &loaded), grew(&loaded, &measured));
        assert_eq!(load_level.len(), 1, "{start:?} -> {loaded:?}");
        assert_eq!(run_level.len(), 1, "{loaded:?} -> {measured:?}");
        assert_ne!(
            load_level, run_level,
            "the load and the run commit at different levels"
        );
        // The client a run creates after the switch commits at the run's level too.
        let mut c = r.client().unwrap();
        let before = levels(&r);
        c.execute(&BenchOp::Put {
            row: b"c".to_vec(),
            family: YCSB_FAMILY,
            cells: vec![(b"q".to_vec(), b"v".to_vec())],
        })
        .unwrap();
        drop(c);
        assert_eq!(grew(&before, &levels(&r)), run_level);
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
