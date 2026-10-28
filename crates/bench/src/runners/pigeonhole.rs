//! The Pigeonhole runner, through the public `pigeonhole` API only.

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pigeonhole::{
    Compaction, Condition, Durability, ErrorCode, Family, Options, Pigeonhole, RetryPolicy,
    RowMutation, Table, Transaction, ValueFilter,
};

use super::{BLOOM_BITS, Counted, MemoryBudget, Touched, durability, modified, transfer_balances};
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
    /// Inline page-cache reads (`Options::nowait_reads`, D292); on, the library default.
    pub(crate) no_nowait_reads: bool,
    /// Row cache bytes (`Options::row_cache`); `None` keeps the library default (D216), 0
    /// turns it off.
    pub(crate) row_cache: Option<u64>,
    /// A shard's idle spin before it parks (`Options::shard_spin`, D198), in microseconds;
    /// `None`: the library default.
    pub(crate) shard_spin_micros: Option<u64>,
    /// The threads the shards run on (`Options::shard_threads`, D289); the library default
    /// when not set.
    pub(crate) shard_threads: Option<usize>,
    /// Application-owned opens only: background workers for flush and compaction
    /// (`Options::background_workers`, D284), each driven on a thread of its own; 0 runs
    /// them on the shards.
    pub(crate) background_workers: usize,
    /// Flush and compaction threads (`Options::compaction_cores`, D252); the library
    /// default when not set.
    pub(crate) compaction_cores: Option<usize>,
    /// `Options::cpu_profile(CpuProfile::Server)` (D289) when set; the embedded default
    /// otherwise.
    pub(crate) server_profile: bool,
    /// `Options::adaptive_shard_threads(true)` (D289's controller, experimental) when set.
    pub(crate) adaptive_shard_threads: bool,
    /// A blocking `None`/`Buffered` commit's spin before it parks (`Options::commit_spin`,
    /// D253), in microseconds; `None`: the library default.
    pub(crate) commit_spin_micros: Option<u64>,
    /// The inline driver splits the table over every shard before loading it (D213).
    pub(crate) presplit: bool,
    /// Riders on the measured phase (D247): snapshot and durable-snapshot readers, then a
    /// durable backup and a compaction.
    pub(crate) riders: bool,
    /// Reader processes beside the measured phase (#683): this many `phdb-bench reader`
    /// children doing ycsb-c reads through `open_reader`; 0 runs none.
    pub(crate) reader_processes: usize,
    /// Client threads keep this many operations in flight through the async API (D247's
    /// async rows); 0 runs one at a time.
    pub(crate) async_in_flight: usize,
    /// The inline driver makes about a fifth of its commits batches across shards (D247's
    /// app-owned row).
    pub(crate) cross_batches: bool,
    /// With `cross_batches`, every other batch is a read-modify-write transaction instead.
    pub(crate) cross_readers: bool,
    /// The inline driver opens the store engine-owned: its threads issue the same writes
    /// but drive no shard (D280's comparator for the application-owned row).
    pub(crate) inline_engine_owned: bool,
    /// The pre-split cuts at the measured writes' quantiles rather than the loaded rows', so
    /// each piece takes about the same share of the writes (D231's engine scaling).
    pub(crate) presplit_writes: bool,
    /// ycsb-f's read-modify-write runs in a `Transaction` instead of `commit_if` (D274: the
    /// judged row, transaction against transaction; the runner is named `pigeonhole-txn`).
    pub(crate) rmw_txn: bool,
    /// Transactions retry with `RetryPolicy`'s backoff from this base, in microseconds, to its
    /// default cap: a reported variant (D274). `None`: the library's default policy, as the
    /// judged rows run.
    pub(crate) txn_backoff_micros: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct Open {
    target: Target,
    /// Writes retried after `Busy`, shared with every client.
    busy: Arc<AtomicU64>,
    /// The store's directory (the riders' backup goes under it).
    dir: std::path::PathBuf,
    /// The riders' threads while the measured phase runs.
    riders: Option<riders::Running>,
    /// The reader processes while the measured phase runs.
    readers: Option<super::reader_process::Running>,
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

    /// `true`: ycsb-f's read-modify-write runs in an optimistic `Transaction` (read, put,
    /// commit, retried on a conflict) instead of `commit_if` (D274). The runner is then
    /// named `pigeonhole-txn`, the row judged against RocksDB's `OptimisticTransactionDB`;
    /// `commit_if` stays the idiomatic fast path, reported beside it.
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// let r = PigeonholeRunner::default().rmw_in_transaction(true);
    /// assert_eq!(r.name(), "pigeonhole-txn");
    /// assert!(r.describe().contains(" rmw=txn"));
    /// ```
    pub fn rmw_in_transaction(mut self, yes: bool) -> Self {
        self.settings.rmw_txn = yes;
        self
    }

    /// Transactions (transfer, ycsb-f's `pigeonhole-txn`) retry a conflict after
    /// `RetryPolicy`'s full-jitter backoff from `micros` (0: at once) instead of the library
    /// default's. A reported variant: the judged rows run `transact` with the default policy,
    /// each store's out-of-the-box retry (D274).
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// assert!(!PigeonholeRunner::default().describe().contains("txn_backoff"));
    /// let r = PigeonholeRunner::default().txn_backoff_micros(25);
    /// assert!(r.describe().contains(" txn_backoff=25us"));
    /// ```
    pub fn txn_backoff_micros(mut self, micros: u64) -> Self {
        self.settings.txn_backoff_micros = Some(micros);
        self
    }

    /// `true`: background work rides on the measured phase (D247, reported): one thread scans
    /// 10 rows at a time under a snapshot re-taken every 100 ms, another the same under a
    /// durable snapshot, and a third takes a durable backup 2 s in, then a full compaction.
    /// The record's [`Riders`](crate::Riders) has their latencies and durations.
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// assert!(PigeonholeRunner::default().riders(true).describe().contains(" riders"));
    /// ```
    pub fn riders(mut self, yes: bool) -> Self {
        self.settings.riders = yes;
        self
    }

    /// Runs `n` reader processes beside the measured phase (#683, D247 item K, reported):
    /// `phdb-bench reader` children attached with `Pigeonhole::open_reader`, each reading
    /// whole `ycsb` rows, Zipfian over the loaded records, for as long as the phase runs. The
    /// record's [`ReaderProcesses`](crate::ReaderProcesses) has their reads, latencies and
    /// errors. The children are this executable (or [`READER_EXE_ENV`](crate::READER_EXE_ENV)).
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// let r = PigeonholeRunner::default().reader_processes(2);
    /// assert!(r.describe().contains(" readers=2"));
    /// ```
    pub fn reader_processes(mut self, n: usize) -> Self {
        self.settings.reader_processes = n;
        self
    }

    /// Each client thread keeps `n` operations in flight through the async API (D247, perf287's
    /// E/E′): puts as `commit_async`, row reads as `read_async`, gets as `get_async`, polled by
    /// the thread itself; any other operation runs on its own in between. 0 (the default) runs
    /// one operation at a time. Latency is each operation's submission to resolution.
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// assert!(PigeonholeRunner::default().async_in_flight(16).describe().contains(" async=16"));
    /// ```
    pub fn async_in_flight(mut self, n: usize) -> Self {
        self.settings.async_in_flight = n;
        self
    }

    /// `true`: [`run_inline`](Self::run_inline) makes every fifth commit of each shard thread a
    /// `WriteBatch` of its write and 1-3 rows picked across the whole stream, most owned by
    /// other shards: about 20% cross-shard batches (D247, perf287's L).
    pub fn cross_batches(mut self, yes: bool) -> Self {
        self.settings.cross_batches = yes;
        self
    }

    /// `true`, with [`cross_batches`](Self::cross_batches): every other such batch is a
    /// transaction that reads a cold companion row of each of its rows and writes the rows,
    /// so commits that read and blind writes meet on the same hot rows (#816, D267). A
    /// transaction that conflicts is retried and counted.
    pub fn cross_readers(mut self, yes: bool) -> Self {
        self.settings.cross_readers = yes;
        self
    }

    /// `true`: [`run_inline`](Self::run_inline) opens the store engine-owned. Each of its
    /// threads issues the same share of the same stream (with the same batches, and the same
    /// commits in flight) through `commit_async`, but drives no shard: the engine's own shard
    /// threads run the commits. That's the application-owned row's comparator on the same
    /// workload (D280, #816, #710).
    pub fn inline_engine_owned(mut self, yes: bool) -> Self {
        self.settings.inline_engine_owned = yes;
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

    /// Inline page-cache reads (`Options::nowait_reads`, D292, Linux); on without a call.
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// let r = PigeonholeRunner::default().nowait_reads(false);
    /// assert!(r.describe().contains("nowait=off"));
    /// ```
    pub fn nowait_reads(mut self, yes: bool) -> Self {
        self.settings.no_nowait_reads = !yes;
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

    /// The threads the shards run on (`Options::shard_threads`, D289); the library default
    /// when not set.
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// assert!(!PigeonholeRunner::default().describe().contains("shard_threads"));
    /// let r = PigeonholeRunner::default().shard_threads(4);
    /// assert!(r.describe().contains(" shard_threads=4"));
    /// ```
    pub fn shard_threads(mut self, k: usize) -> Self {
        self.settings.shard_threads = Some(k);
        self
    }

    /// Flush and compaction threads (`Options::compaction_cores`, D252); the library
    /// default when not set.
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// assert!(!PigeonholeRunner::default().describe().contains("compaction_cores"));
    /// let r = PigeonholeRunner::default().compaction_cores(8);
    /// assert!(r.describe().contains(" compaction_cores=8"));
    /// ```
    pub fn compaction_cores(mut self, c: usize) -> Self {
        self.settings.compaction_cores = Some(c);
        self
    }

    /// The server CPU profile (`Options::cpu_profile(CpuProfile::Server)`, D289, ICR 0081):
    /// flush and compaction take half the physical cores rather than a quarter.
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// assert!(!PigeonholeRunner::default().describe().contains("profile=server"));
    /// assert!(PigeonholeRunner::default().server_profile().describe().contains(" profile=server"));
    /// ```
    pub fn server_profile(mut self) -> Self {
        self.settings.server_profile = true;
        self
    }

    /// D289's load controller sizes the shard threads
    /// (`Options::adaptive_shard_threads(true)`, experimental).
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// assert!(!PigeonholeRunner::default().describe().contains("adaptive"));
    /// let r = PigeonholeRunner::default().adaptive_shard_threads();
    /// assert!(r.describe().contains(" shard_threads=adaptive"));
    /// ```
    pub fn adaptive_shard_threads(mut self) -> Self {
        self.settings.adaptive_shard_threads = true;
        self
    }

    /// Application-owned runs ([`run_inline`](Self::run_inline) without
    /// [`inline_engine_owned`](Self::inline_engine_owned)): `n` background workers for flush
    /// and compaction (`Options::background_workers`, D284), each driven on a thread of its
    /// own (`AppOwned::spawn_background_threads`); 0 (the default) runs them on the shards.
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// assert!(!PigeonholeRunner::default().describe().contains("workers"));
    /// let r = PigeonholeRunner::default().background_workers(4);
    /// assert!(r.describe().contains(" workers=4"));
    /// ```
    pub fn background_workers(mut self, n: usize) -> Self {
        self.settings.background_workers = n;
        self
    }

    /// A blocking `None`/`Buffered` commit's spin before it parks, in microseconds
    /// (`Options::commit_spin`, D253); the library default when not set.
    ///
    /// ```
    /// use pigeonhole_bench::{PigeonholeRunner, Runner};
    ///
    /// assert!(!PigeonholeRunner::default().describe().contains("commit_spin"));
    /// let r = PigeonholeRunner::default().commit_spin_micros(0);
    /// assert!(r.describe().contains(" commit_spin=0us"));
    /// ```
    pub fn commit_spin_micros(mut self, micros: u64) -> Self {
        self.settings.commit_spin_micros = Some(micros);
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

/// D247's riders: background readers, a backup and a compaction beside a measured phase.
mod riders {
    use std::ops::Bound;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::JoinHandle;
    use std::time::{Duration, Instant};

    use pigeonhole::{Pigeonhole, Snapshot, Table};

    use super::err;
    use crate::{Histogram, LatencyStats, Riders};

    /// How often a reader takes a new snapshot.
    const RESNAPSHOT: Duration = Duration::from_millis(100);
    /// How far into the measured phase the backup starts.
    const BACKUP_AFTER: Duration = Duration::from_secs(2);

    type Reader = JoinHandle<Result<(Histogram, u64), String>>;
    /// The backup and compaction thread: their milliseconds, each `None` if not reached.
    type Work = JoinHandle<Result<(Option<f64>, Option<f64>), String>>;

    pub(super) struct Running {
        stop: Arc<AtomicBool>,
        plain: Reader,
        durable: Reader,
        work: Work,
        backup: std::path::PathBuf,
    }

    impl std::fmt::Debug for Running {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Running").finish_non_exhaustive()
        }
    }

    pub(super) fn start(db: &Pigeonhole, table: &Table, backup: std::path::PathBuf) -> Running {
        let stop = Arc::new(AtomicBool::new(false));
        let reader = |durable: bool| {
            let (db, table, stop) = (db.clone(), table.clone(), Arc::clone(&stop));
            std::thread::spawn(move || read(&db, &table, &stop, durable))
        };
        let (plain, durable) = (reader(false), reader(true));
        let work = {
            let (db, stop, path) = (db.clone(), Arc::clone(&stop), backup.clone());
            std::thread::spawn(move || {
                let begun = Instant::now();
                while begun.elapsed() < BACKUP_AFTER {
                    if stop.load(Ordering::Relaxed) {
                        return Ok((None, None));
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                let t = Instant::now();
                db.backup_durable(&path).map_err(err)?;
                let backup_ms = t.elapsed().as_secs_f64() * 1e3;
                // Once started, the backup and the compaction both run to the end, whenever
                // the measured phase ends: `stop` joins this thread, so the report always has
                // both durations, not a race between the compaction and the phase's end.
                let t = Instant::now();
                db.compact().map_err(err)?;
                Ok((Some(backup_ms), Some(t.elapsed().as_secs_f64() * 1e3)))
            })
        };
        Running {
            stop,
            plain,
            durable,
            work,
            backup,
        }
    }

    /// Scans 10 rows from a random start under a snapshot taken anew every
    /// [`RESNAPSHOT`], until stopped.
    fn read(
        db: &Pigeonhole,
        table: &Table,
        stop: &AtomicBool,
        durable: bool,
    ) -> Result<(Histogram, u64), String> {
        let take = || -> Result<Snapshot, String> {
            if durable {
                db.durable_snapshot()
            } else {
                db.snapshot()
            }
            .map_err(err)
        };
        let (mut hist, mut taken) = (Histogram::new(), 1u64);
        let mut snap = take()?;
        let mut since = Instant::now();
        let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ u64::from(durable);
        while !stop.load(Ordering::Relaxed) {
            if since.elapsed() >= RESNAPSHOT {
                snap = take()?;
                taken += 1;
                since = Instant::now();
            }
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            // YCSB rows are `user` and 20 digits; a random 20-digit start lands among them.
            let start = format!("user{:020}", x % 10u64.pow(19));
            let t = Instant::now();
            let mut it = table
                .scan_bounds(Bound::Included(start.as_bytes()), Bound::Unbounded)
                .snapshot(&snap)
                .limit(10)
                .iter()
                .map_err(err)?;
            while it.next_ref().map_err(err)?.is_some() {}
            hist.record(t.elapsed());
        }
        Ok((hist, taken))
    }

    impl Running {
        pub(super) fn stop(self) -> Result<Riders, String> {
            self.stop.store(true, Ordering::Relaxed);
            let join = |h: Reader| h.join().map_err(|_| "a rider panicked".to_owned())?;
            let (plain, snapshots) = join(self.plain)?;
            let (durable, durable_snapshots) = join(self.durable)?;
            let (backup_ms, compact_ms) = self
                .work
                .join()
                .map_err(|_| "the backup rider panicked".to_owned())??;
            // Its size, for a comparison at the same size (D280, #715): a file or a directory.
            let backup_bytes = backup_ms.map(|_| match std::fs::metadata(&self.backup) {
                Ok(m) if m.is_dir() => crate::dir_bytes(&self.backup),
                Ok(m) => m.len(),
                Err(_) => 0,
            });
            // Not counted in the store's size.
            let _ = std::fs::remove_dir_all(&self.backup);
            let _ = std::fs::remove_file(&self.backup);
            Ok(Riders {
                snapshot_scans: LatencyStats::of(&plain),
                durable_snapshot_scans: LatencyStats::of(&durable),
                snapshots,
                durable_snapshots,
                backup_ms,
                compact_ms,
                backup_bytes,
                ..Riders::default()
            })
        }
    }
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
    if s.no_nowait_reads {
        options = options.nowait_reads(false);
    }
    if let Some(n) = s.shards {
        options = options.shards(n);
    }
    if let Some(bytes) = s.row_cache {
        options = options.row_cache(usize::try_from(bytes).unwrap_or(usize::MAX));
    }
    if let Some(k) = s.shard_threads {
        options = options.shard_threads(k);
    }
    options = options.background_workers(s.background_workers);
    if let Some(c) = s.compaction_cores {
        options = options.compaction_cores(c);
    }
    if s.server_profile {
        options = options.cpu_profile(pigeonhole::CpuProfile::Server);
    }
    if s.adaptive_shard_threads {
        options = options.adaptive_shard_threads(true);
    }
    if let Some(us) = s.shard_spin_micros {
        options = options.shard_spin(Duration::from_micros(us));
    }
    if let Some(us) = s.commit_spin_micros {
        options = options.commit_spin(Duration::from_micros(us));
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

/// The counters workload's table (D247): `ctr`, one counter family.
fn create_counters(db: &Pigeonhole) -> Result<Table, String> {
    db.table("ctr")
        .map_err(err)?
        .family(COUNTER_FAMILY, Family::counter().bloom_bits(BLOOM_BITS))
        .create_if_missing()
        .map_err(err)
}

/// The transfer workload's balance column, in [`YCSB_FAMILY`].
const BALANCE: &[u8] = b"bal";

/// The family and qualifier of the counters workload's counter.
const COUNTER_FAMILY: &str = "n";
const COUNTER: &[u8] = b"n";

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
        if self.settings.rmw_txn {
            "pigeonhole-txn"
        } else {
            "pigeonhole"
        }
    }

    fn open(&mut self, dir: &Path) -> Result<(), String> {
        let db = Pigeonhole::open(dir.join("bench.phdb"), options(&self.settings)).map_err(err)?;
        let table = create_table(&db)?;
        self.open = Some(Open {
            target: Target {
                db,
                table,
                parts: Arc::default(),
                counters: None,
                conflicts: Arc::default(),
                load_buffered: self.settings.load_buffered,
                rmw_txn: self.settings.rmw_txn,
                retry: retry_policy(&self.settings),
            },
            busy: Arc::default(),
            dir: dir.to_path_buf(),
            riders: None,
            readers: None,
        });
        Ok(())
    }

    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        self.execute_counted(op).map(drop)
    }

    fn close(&mut self) -> Result<(), String> {
        match self.open.take() {
            Some(Open {
                target:
                    Target {
                        db,
                        table,
                        parts,
                        counters,
                        ..
                    },
                ..
            }) => {
                drop((table, parts, counters));
                report_nowait_reads(&db);
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

    fn pack_target(&self) -> Option<(usize, usize)> {
        self.open.as_ref().and_then(|o| o.target.db.pack_target())
    }

    fn op_label(&self, op: &BenchOp) -> Option<&'static str> {
        let BenchOp::Transfer { from, to, .. } = op else {
            return None;
        };
        let table = &self.open.as_ref()?.target.table;
        Some(if table.shard_of(from)? == table.shard_of(to)? {
            crate::TRANSFER_ONE_SHARD
        } else {
            crate::TRANSFER_CROSS_SHARD
        })
    }

    fn driver_target(&self) -> Option<(usize, bool)> {
        self.open.as_ref().and_then(|o| o.target.db.driver_target())
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
                    commit_inflight_nanos: s.commit_inflight_nanos,
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

    fn measured_durability(&mut self) -> Result<bool, String> {
        Ok(self
            .open
            .as_mut()
            .is_some_and(|o| std::mem::take(&mut o.target.load_buffered)))
    }

    fn start_riders(&mut self) -> Result<(), String> {
        if !self.settings.riders {
            return Ok(());
        }
        let o = self.open.as_mut().ok_or("pigeonhole runner is not open")?;
        o.riders = Some(riders::start(
            &o.target.db,
            &o.target.table,
            o.dir.join("rider-backup"),
        ));
        Ok(())
    }

    fn stop_riders(&mut self) -> Result<Option<crate::Riders>, String> {
        match self.open.as_mut().and_then(|o| o.riders.take()) {
            Some(r) => r.stop().map(Some),
            None => Ok(None),
        }
    }

    fn start_reader_processes(&mut self, config: &crate::WorkloadConfig) -> Result<(), String> {
        let n = self.settings.reader_processes;
        if n == 0 {
            return Ok(());
        }
        let o = self.open.as_mut().ok_or("pigeonhole runner is not open")?;
        o.readers = Some(super::reader_process::spawn(
            &o.target.db,
            &o.dir.join("bench.phdb"),
            n,
            config,
        )?);
        Ok(())
    }

    fn stop_reader_processes(&mut self) -> Result<Option<crate::ReaderProcesses>, String> {
        match self.open.as_mut().and_then(|o| o.readers.take()) {
            Some(r) => r.stop().map(Some),
            None => Ok(None),
        }
    }

    fn txn_conflicts(&self) -> Option<u64> {
        Some(self.open.as_ref()?.target.conflicts.load(Ordering::Relaxed))
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
            in_flight: self.settings.async_in_flight,
        }))
    }

    fn describe(&self) -> String {
        let s = &self.settings;
        let shards = s.shards.map_or_else(
            || format!("default({})", pigeonhole_io::sys::default_shards()),
            |n| n.to_string(),
        );
        format!(
            "shards={shards} memtable={}MiB cache={}MiB bloom={BLOOM_BITS} {}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
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
            if s.no_nowait_reads { " nowait=off" } else { "" },
            match s.row_cache {
                None => String::new(),
                Some(0) => " rowcache=off".to_owned(),
                Some(bytes) => format!(" rowcache={}MiB", bytes >> 20),
            },
            s.shard_spin_micros
                .map_or_else(String::new, |us| format!(" shard_spin={us}us")),
            s.shard_threads
                .map_or_else(String::new, |k| format!(" shard_threads={k}")),
            if s.background_workers > 0 {
                format!(" workers={}", s.background_workers)
            } else {
                String::new()
            },
            s.compaction_cores
                .map_or_else(String::new, |c| format!(" compaction_cores={c}")),
            if s.server_profile {
                " profile=server"
            } else {
                ""
            },
            if s.adaptive_shard_threads {
                " shard_threads=adaptive"
            } else {
                ""
            },
            s.commit_spin_micros
                .map_or_else(String::new, |us| format!(" commit_spin={us}us")),
            match (s.presplit, s.presplit_writes) {
                (_, true) => " presplit=writes",
                (true, false) => " presplit",
                _ => "",
            },
            if s.riders { " riders" } else { "" },
            if s.reader_processes > 0 {
                format!(" readers={}", s.reader_processes)
            } else {
                String::new()
            },
            if s.async_in_flight > 0 {
                format!(" async={}", s.async_in_flight)
            } else {
                String::new()
            },
            if s.rmw_txn { " rmw=txn" } else { "" },
            s.txn_backoff_micros
                .map_or_else(String::new, |us| format!(" txn_backoff={us}us")),
        )
    }
}

#[cfg(test)]
impl PigeonholeRunner {
    /// The first byte of a bench-table cell's value, for tests.
    pub(crate) fn first_byte(&self, row: &[u8], qualifier: &[u8]) -> Option<u8> {
        let table = &self.open.as_ref()?.target.table;
        let cell = table.get(row, YCSB_FAMILY, qualifier).ok()??;
        cell.value().first().copied()
    }

    /// A transfer workload balance, for tests.
    pub(crate) fn balance(&self, row: &[u8]) -> Option<i64> {
        let table = &self.open.as_ref()?.target.table;
        let cell = table.get(row, YCSB_FAMILY, BALANCE).ok()??;
        Some(i64::from_le_bytes(cell.value().try_into().ok()?))
    }

    /// A counter of the counters workload, for tests.
    pub(crate) fn counter(&self, row: &[u8]) -> Option<i64> {
        let table = self.open.as_ref()?.target.counters.as_ref()?;
        table.get(row, COUNTER_FAMILY, COUNTER).ok()??.as_i64()
    }
}

impl Counted for PigeonholeRunner {
    fn execute_counted(&mut self, op: &BenchOp) -> Result<Touched, String> {
        let o = self.open.as_mut().ok_or("pigeonhole runner is not open")?;
        if matches!(op, BenchOp::Commit { .. }) && o.target.parts.is_empty() {
            o.target.parts = Arc::new(create_partitions(&o.target.db)?);
        }
        if matches!(op, BenchOp::Incr { .. }) && o.target.counters.is_none() {
            o.target.counters = Some(create_counters(&o.target.db)?);
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
    /// The counters workload's table (D247), created by its load's first increment.
    counters: Option<Table>,
    /// Transactions retried after a conflict, shared with every client (D247).
    conflicts: Arc<AtomicU64>,
    /// Commits are `Buffered` (the load of a run set to [`PigeonholeRunner::load_buffered`]).
    load_buffered: bool,
    /// [`PigeonholeRunner::rmw_in_transaction`].
    rmw_txn: bool,
    /// How transactions retry (D274): the library default, or a variant's base.
    retry: RetryPolicy,
}

/// How the runners' transactions retry (D274): the library's default `RetryPolicy`, or a
/// variant's backoff base (`PigeonholeRunner::txn_backoff_micros`) to the default cap.
pub(crate) fn retry_policy(s: &Settings) -> RetryPolicy {
    s.txn_backoff_micros
        .map_or_else(RetryPolicy::default, |us| {
            RetryPolicy::default().backoff(Duration::from_micros(us), Duration::from_millis(100))
        })
}

/// Runs `body` in a transaction through [`Pigeonhole::transact`] with the target's
/// `RetryPolicy` (D274: each store's out-of-the-box retry; Pigeonhole's is `transact` with the
/// default policy). Each attempt after the first follows a conflict (or an expired snapshot)
/// and is counted; `Busy` ends `transact` and is retried here as for every other write.
fn transact<T>(
    target: &Target,
    busy: &AtomicU64,
    buffered: bool,
    mut body: impl FnMut(&mut Transaction) -> Result<T, pigeonhole::Error>,
) -> Result<T, String> {
    let policy = if buffered {
        target.retry.clone().durability(Durability::Buffered)
    } else {
        target.retry.clone()
    };
    retry_busy(busy, || {
        let mut first = true;
        target
            .db
            .transact(&policy, |txn| {
                if !std::mem::take(&mut first) {
                    target.conflicts.fetch_add(1, Ordering::Relaxed);
                }
                body(txn)
            })
            .map(|(v, _)| v)
    })
}

struct PigeonholeClient {
    target: Target,
    busy: Arc<AtomicU64>,
    /// Operations kept in flight through the async API; 0 runs one at a time.
    in_flight: usize,
}

impl Client for PigeonholeClient {
    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        execute(&self.target, &self.busy, op).map(drop)
    }

    fn pipelined(
        &mut self,
        ops: &[&BenchOp],
        deadline: Option<Instant>,
    ) -> Option<Result<Vec<Option<Duration>>, String>> {
        (self.in_flight > 0).then(|| pipeline::run(self, ops, deadline))
    }
}

/// D247's async rows: a client thread keeping operations in flight through the async API,
/// polled by the thread itself, woken by the futures' wakers (no executor).
mod pipeline {
    use std::collections::VecDeque;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::{Duration, Instant};

    use super::{PigeonholeClient, err, execute};
    use crate::BenchOp;

    /// Unparks the client thread.
    struct Unpark(std::thread::Thread);

    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    type Pending = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

    /// The operation as a future, or `None` for one the async API does not cover.
    fn submit(c: &PigeonholeClient, op: &BenchOp) -> Option<Pending> {
        let table = &c.target.table;
        Some(match op {
            BenchOp::Put { row, family, cells } => {
                let mut m = table.mutate(row);
                for (q, v) in cells {
                    m = m.put(family, q, v);
                }
                let f = m.commit_async();
                Box::pin(async move { f.await.map(drop).map_err(err) })
            }
            BenchOp::Get {
                row,
                family,
                qualifier,
            } => {
                let f = table.get_async(row, family, qualifier);
                Box::pin(async move { f.await.map(drop).map_err(err) })
            }
            BenchOp::GetRow { row, family } => {
                let f = table.row(row).family(family).read_async();
                Box::pin(async move { f.await.map(drop).map_err(err) })
            }
            _ => return None,
        })
    }

    pub(super) fn run(
        c: &mut PigeonholeClient,
        ops: &[&BenchOp],
        deadline: Option<Instant>,
    ) -> Result<Vec<Option<Duration>>, String> {
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut cx = Context::from_waker(&waker);
        let mut latencies = vec![None; ops.len()];
        let mut flight: VecDeque<(usize, Instant, Pending)> = VecDeque::with_capacity(c.in_flight);
        let mut next = 0;
        // Past the deadline no operation starts; those in flight finish.
        let mut ends = ops.len();
        while next < ends || !flight.is_empty() {
            if deadline.is_some_and(|d| Instant::now() >= d) {
                ends = next;
            }
            while flight.len() < c.in_flight && next < ends {
                let t = Instant::now();
                match submit(c, ops[next]) {
                    Some(f) => flight.push_back((next, t, f)),
                    None => {
                        execute(&c.target, &c.busy, ops[next])?;
                        latencies[next] = Some(t.elapsed());
                    }
                }
                next += 1;
            }
            let before = flight.len();
            let mut i = 0;
            while i < flight.len() {
                if let Poll::Ready(r) = flight[i].2.as_mut().poll(&mut cx) {
                    let (n, t, _) = flight.remove(i).expect("in range");
                    r?;
                    latencies[n] = Some(t.elapsed());
                } else {
                    i += 1;
                }
            }
            if flight.len() == before && !flight.is_empty() {
                std::thread::park_timeout(Duration::from_millis(1));
            }
        }
        Ok(latencies)
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
        BenchOp::ScanFiltered {
            start,
            len,
            family,
            qualifier_prefix,
            time_range,
        } => {
            let mut scan = table
                .scan_bounds(Bound::Included(start.as_slice()), Bound::Unbounded)
                .family(family)
                .limit(u64::from(*len));
            if let Some(q) = qualifier_prefix {
                scan = scan.qualifier_prefix(q);
            }
            if let Some((a, b)) = time_range {
                scan = scan.time_range(*a..*b);
            }
            let mut it = scan.iter().map_err(err)?;
            while let Some(row) = it.next_ref().map_err(err)? {
                for entry in row.iter() {
                    t.cell(entry.cell.value());
                }
            }
        }
        BenchOp::ReadModifyWrite { row, qualifier } if target.rmw_txn => {
            // D274: transaction against transaction. Read, put and commit in an optimistic
            // transaction through `transact` (its default retry), as transfer.
            transact(target, busy, buffered, |txn| {
                let old = txn
                    .get(table, row, YCSB_FAMILY, qualifier)?
                    .map(|c| c.value().to_vec());
                t = Touched::default();
                if let Some(v) = &old {
                    t.cell(v);
                }
                txn.put(
                    table,
                    row,
                    YCSB_FAMILY,
                    qualifier,
                    &modified(old.as_deref()),
                );
                Ok(())
            })?;
        }
        BenchOp::ReadModifyWrite { row, qualifier } => {
            // Atomic across clients (D247): the write commits only if the cell still holds
            // what was read, else the read and the write go again; a lost race counts as a
            // conflict (D274).
            loop {
                let old = table.get(row, YCSB_FAMILY, qualifier).map_err(err)?;
                let new = modified(old.as_ref().map(|c| c.value()));
                let condition = match &old {
                    Some(cell) => Condition::Value {
                        family: YCSB_FAMILY.to_owned(),
                        qualifier: qualifier.clone(),
                        filter: ValueFilter::Equals(cell.value().to_vec()),
                    },
                    None => Condition::Absent {
                        family: YCSB_FAMILY.to_owned(),
                        qualifier: qualifier.clone(),
                    },
                };
                t = Touched::default();
                if let Some(cell) = &old {
                    t.cell(cell.value());
                }
                drop(old);
                let applied = retry_busy(busy, || {
                    level(buffered, table.mutate(row))
                        .put(YCSB_FAMILY, qualifier, &new)
                        .commit_if(&condition)
                })?;
                if applied.is_some() {
                    break;
                }
                target.conflicts.fetch_add(1, Ordering::Relaxed);
            }
        }
        BenchOp::DeleteColumn {
            row,
            family,
            qualifier,
        } => {
            retry_busy(busy, || {
                level(buffered, table.mutate(row))
                    .delete_column(family, qualifier)
                    .commit()
            })?;
        }
        BenchOp::DeleteRow { row } => {
            retry_busy(busy, || {
                level(buffered, table.mutate(row)).delete_row().commit()
            })?;
        }
        BenchOp::Transfer { from, to, amount } => {
            // An optimistic transaction reading both balances, through `transact` (D274).
            transact(target, busy, buffered, |txn| {
                let a = txn
                    .get(table, from, YCSB_FAMILY, BALANCE)?
                    .map(|c| c.value().to_vec());
                let b = txn
                    .get(table, to, YCSB_FAMILY, BALANCE)?
                    .map(|c| c.value().to_vec());
                t = Touched::default();
                for v in [&a, &b].into_iter().flatten() {
                    t.cell(v);
                }
                let (na, nb) = transfer_balances(a.as_deref(), b.as_deref(), *amount);
                txn.put(table, from, YCSB_FAMILY, BALANCE, &na.to_le_bytes());
                txn.put(table, to, YCSB_FAMILY, BALANCE, &nb.to_le_bytes());
                Ok(())
            })?;
        }
        BenchOp::Incr { row, delta } => {
            let table = target
                .counters
                .as_ref()
                .ok_or("no counters table: the load creates it")?;
            retry_busy(busy, || {
                level(buffered, table.mutate(row))
                    .incr(COUNTER_FAMILY, COUNTER, *delta)
                    .commit()
            })?;
        }
        BenchOp::ReadCounter { row } => {
            let table = target
                .counters
                .as_ref()
                .ok_or("no counters table: the load creates it")?;
            if let Some(cell) = table.get(row, COUNTER_FAMILY, COUNTER).map_err(err)? {
                let n = cell.as_i64().ok_or("a counter that is not an i64")?;
                t.cell(&n.to_le_bytes());
            }
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

/// D292's inline page-cache reads over the database's life, on stderr: hits, misses, the
/// misses' mean and bucketed p50/p99 (perf287's S3), refusals and breaker trips.
fn report_nowait_reads(db: &Pigeonhole) {
    let Some(n) = db.nowait_reads() else {
        return;
    };
    // The upper bound of the bucket holding the `q`th quantile of misses, in µs.
    let quantile = |q: f64| {
        let want = (n.misses as f64 * q).ceil() as u64;
        let mut seen = 0;
        for (i, &c) in n.miss_buckets.iter().enumerate() {
            seen += c;
            if seen >= want.max(1) {
                return if i + 1 == n.miss_buckets.len() {
                    format!(">={}", 1u64 << (i - 1))
                } else {
                    format!("<{}", 1u64 << i)
                };
            }
        }
        "-".to_owned()
    };
    eprintln!(
        "nowait reads: on={} hits {} misses {} (mean {:.1} us, p50 {} us, p99 {} us) \
         refused {} breaks {} buckets {:?}",
        n.on,
        n.hits,
        n.misses,
        n.miss_nanos as f64 / n.misses.max(1) as f64 / 1000.0,
        quantile(0.5),
        quantile(0.99),
        n.refused,
        n.breaks,
        n.miss_buckets,
    );
}

/// The engine's cumulative stall and visibility counters ([`crate::Stalls`], ICR 0015,
/// ICR 0023).
pub(crate) fn stalls(db: &Pigeonhole) -> crate::Stalls {
    let m = db.engine_metrics();
    let rc = db.row_cache_stats();
    let rings = db.io_rings().unwrap_or_default();
    crate::Stalls {
        ring_attach_failures: rings.attach_failures,
        ring_inline: rings.inline,
        write_stalls: m.write_stalls,
        write_stall_nanos: m.write_stall_nanos,
        room_stalls: m.room_stalls,
        room_stall_nanos: m.room_stall_nanos,
        pacing_stalls: m.pacing_stalls,
        pacing_stall_nanos: m.pacing_stall_nanos,
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
        row_cache_write_throughs: rc.write_throughs,
        xshard_rounds: m.cross_shard.rounds,
        xshard_refused_rounds: m.cross_shard.refused_rounds,
        // Always 0 since D263 (the counters are deprecated); the report keeps its columns.
        xshard_refused_below_floor: 0,
        xshard_refused_move_raise: 0,
        xshard_refused_moved: m.cross_shard.refused_moved,
        xshard_refused_room: m.cross_shard.refused_room,
        xshard_refused_pending_overlap: m.cross_shard.refused_pending_overlap,
        xshard_share_waits: m.cross_shard.share_waits,
        xshard_share_wait_nanos: m.cross_shard.share_wait_nanos,
        xshard_chained: m.cross_shard.chained,
        xshard_wounds: m.cross_shard.wounds,
        xshard_max_attempts_blind: m.cross_shard.max_attempts_blind,
        xshard_max_attempts_reading: m.cross_shard.max_attempts_reading,
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
        // The engine counts the refused rounds itself (ICR 0043); every one of them used a
        // sequence number too.
        let rounds = rec.rounds().unwrap();
        let refused = rounds.refused.expect("Metrics::cross_shard");
        assert!(refused <= rounds.inferred().unwrap(), "{rounds:?}");
        assert!(rec.detail.stalls.unwrap().xshard_rounds >= cross);
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
        assert!(
            r.measured_durability().unwrap(),
            "the load was buffered, the run is not"
        );
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
    fn riders_read_under_snapshots_then_back_up_and_compact() {
        // D247: the riders run beside a 3 s measured phase and report.
        let dir = std::env::temp_dir().join(format!("phdb-bench-ph-riders-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut r = PigeonholeRunner::default()
            .shards(2)
            .memtable_budget(16 << 20)
            .riders(true);
        let config = WorkloadConfig::smoke(WorkloadKind::YcsbA);
        let options = crate::RunOptions {
            warmup: 0.0,
            min_measured: Duration::from_secs(3),
            ..crate::RunOptions::default()
        };
        let rec = crate::run_detailed(&mut r, &config, &dir, &options).unwrap();
        let riders = rec.detail.riders.expect("riders");
        assert!(riders.snapshot_scans.count > 0 && riders.durable_snapshot_scans.count > 0);
        assert!(
            riders.snapshots >= 10 && riders.durable_snapshots >= 10,
            "{riders:?}"
        );
        assert!(
            riders.backup_ms.is_some() && riders.compact_ms.is_some(),
            "{riders:?}"
        );
        assert!(rec.detail.close_ms.is_some());
        assert!(!dir.join("rider-backup").exists(), "the backup is removed");
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn filtered_scans_select() {
        // D247: the edge prefix reads some of a vertex's edges, not all of them.
        let kind = WorkloadKind::AdjacencyPrefix;
        let (mut r, dir) = opened(kind.name());
        let mut w = Workload::new(WorkloadConfig::smoke(kind));
        for op in w.load_ops() {
            r.execute(&op).unwrap();
        }
        let (mut filtered, mut plain) = (0, 0);
        for op in w.run_ops() {
            let BenchOp::ScanFiltered { start, len, .. } = &op else {
                continue;
            };
            filtered += r.execute_counted(&op).unwrap().cells;
            let all = BenchOp::Scan {
                start: start.clone(),
                len: *len,
            };
            plain += r.execute_counted(&all).unwrap().cells;
        }
        assert!(0 < filtered && filtered < plain, "{filtered} of {plain}");
        r.close().unwrap();
        std::fs::remove_dir_all(dir).ok();
        // The time window reads points from 12 to 6 hours old, not the newest ones.
        let kind = WorkloadKind::TimeSeriesRange;
        let (mut r, dir) = opened(kind.name());
        let mut w = Workload::new(WorkloadConfig::smoke(kind));
        for op in w.load_ops() {
            r.execute(&op).unwrap();
        }
        let op = w
            .run_ops()
            .find(|op| matches!(op, BenchOp::ScanFiltered { .. }))
            .unwrap();
        let BenchOp::ScanFiltered {
            start,
            len,
            family,
            time_range: Some((a, b)),
            ..
        } = &op
        else {
            unreachable!()
        };
        assert_eq!(r.execute_counted(&op).unwrap().cells, u64::from(*len));
        let table = &r.open.as_ref().unwrap().target.table;
        let mut it = table
            .scan_bounds(Bound::Included(start.as_slice()), Bound::Unbounded)
            .family(family)
            .time_range(*a..*b)
            .limit(u64::from(*len))
            .iter()
            .unwrap();
        while let Some(row) = it.next_ref().unwrap() {
            for e in row.iter() {
                assert!((*a..*b).contains(&e.cell.timestamp()));
            }
        }
        drop(it);
        r.close().unwrap();
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn async_clients_keep_operations_in_flight_and_measure_each() {
        // D247: ycsb-a through the async API, 8 in flight per client thread.
        let dir = std::env::temp_dir().join(format!("phdb-bench-ph-async-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut r = PigeonholeRunner::default()
            .shards(2)
            .memtable_budget(16 << 20)
            .async_in_flight(8);
        let mut config = WorkloadConfig::smoke(WorkloadKind::YcsbA);
        config.threads = 2;
        let options = crate::RunOptions {
            warmup: 0.0,
            ..crate::RunOptions::default()
        };
        let rec = crate::run_detailed(&mut r, &config, &dir, &options).unwrap();
        assert_eq!(rec.operations, config.operations);
        assert_eq!(rec.threads, 2);
        let count = |name: &str| {
            rec.detail
                .by_type
                .iter()
                .find(|t| t.op == name)
                .map_or(0, |t| t.stats.count)
        };
        assert_eq!(count("put") + count("row read"), config.operations);
        assert!(rec.p50_ns > 0);
        assert!(rec.store_config.contains(" async=8"));
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
