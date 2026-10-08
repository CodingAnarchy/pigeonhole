use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pigeonhole_engine::{
    CachePriority, CompactionStyle, Compression, EngineOptions, FamilyOptions,
};
use pigeonhole_format::Durability;
use pigeonhole_io::VfsRef;

use crate::MergeOperator;

/// Name of the built-in `i64` add operator, the default operator of every family.
pub(crate) const I64_ADD: &str = "pigeonhole.i64_add";

/// `n` days, for TTLs: `Family::default().ttl(days(30))`. Saturates instead of overflowing.
pub fn days(n: u64) -> Duration {
    Duration::from_secs(n.saturating_mul(86_400))
}

/// Database options. Process-local: nothing here is stored in the file, so reopening with
/// different options changes them. Zero config is valid.
///
/// ```
/// use pigeonhole::{Durability, Options};
///
/// let options = Options::default()
///     .durability(Durability::Buffered)
///     .shards(2)
///     .memtable_budget(8 << 20)
///     .block_cache(64 << 20);
/// # let _ = options;
/// ```
#[derive(Debug, Clone)]
pub struct Options {
    durability: Durability,
    shards: usize,
    compaction_cores: usize,
    pin_threads: bool,
    memtable_budget: u64,
    block_cache: Option<usize>,
    row_cache: usize,
    shm_dir: Option<PathBuf>,
    create_if_missing: bool,
    merge_operators: Vec<Arc<dyn MergeOperator>>,
    allow_unregistered_merge_operators: bool,
    vfs: Option<VfsRef>,
    wal_segment_size: Option<u64>,
    tablet_changes: bool,
    tablet_balance: Option<(Duration, u64, u64)>,
    write_stall_timeout: Option<Duration>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            durability: Durability::GroupSync,
            shards: 0,
            compaction_cores: 0,
            pin_threads: false,
            memtable_budget: 64 << 20,
            block_cache: None,
            row_cache: 0,
            shm_dir: None,
            create_if_missing: true,
            merge_operators: Vec::new(),
            allow_unregistered_merge_operators: false,
            vfs: None,
            wal_segment_size: None,
            tablet_changes: true,
            tablet_balance: None,
            write_stall_timeout: None,
        }
    }
}

impl Options {
    /// Writer default durability (default [`Durability::GroupSync`]).
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Number of shard threads (default: CPUs available to the process). `1` is a valid
    /// single-threaded configuration.
    pub fn shards(mut self, n: usize) -> Self {
        self.shards = n;
        self
    }

    /// Dedicate `k` extra threads to flush and compaction (pinned when
    /// [`pin_threads`](Options::pin_threads) is on).
    ///
    /// Engine-owned mode only. [`Pigeonhole::open_application_owned`](crate::Pigeonhole::open_application_owned)
    /// starts no shard or compaction threads, so it fails with
    /// [`ErrorCode::InvalidArgument`](crate::ErrorCode::InvalidArgument) when `k > 0`
    /// (decision D40); flush and compaction then run on the shards you drive.
    pub fn compaction_cores(mut self, k: usize) -> Self {
        self.compaction_cores = k;
        self
    }

    /// Pin each shard thread, and each [`compaction_cores`](Options::compaction_cores)
    /// thread, to its own CPU (default off). Engine-owned mode only:
    /// [`Pigeonhole::open_application_owned`](crate::Pigeonhole::open_application_owned)
    /// runs shards on your threads and ignores it.
    ///
    /// Shard `i` goes to the `i`-th CPU (wrapping) of the set the opening thread may run on.
    /// Turn it on only when this database owns those CPUs, typically one database per
    /// process with `shards` equal to the CPUs it was given. Otherwise pinning stacks
    /// threads on the same cores: two pinned databases in one process both put shard 0 on
    /// the first CPU, containers limited by a CPU quota (not a cpuset) all pin to the host's
    /// first CPUs, and an opener already pinned to one CPU puts every shard on it.
    ///
    /// ```
    /// use pigeonhole::Options;
    ///
    /// // A dedicated host: one shard per CPU, each pinned.
    /// let options = Options::default().shards(8).pin_threads(true);
    /// # let _ = options;
    /// ```
    pub fn pin_threads(mut self, yes: bool) -> Self {
        self.pin_threads = yes;
        self
    }

    /// Memtable arena per shard, in bytes (default 64 MiB): the in-memory write buffer, also
    /// the size of the shared-memory arena. Data beyond it is flushed into the file, so it
    /// does not bound the database size; it bounds the largest value and batch. A batch
    /// whose cells need more than about half of it fails with
    /// [`ErrorCode::BatchTooLarge`](crate::ErrorCode::BatchTooLarge).
    ///
    /// The shared-memory region holds every shard's arena (each rounded up to 2 MiB) plus
    /// about 10 MiB of views and reader slots, 266 MiB for the default budget on 4 CPUs: on
    /// Linux, memory in `/dev/shm` (or a file
    /// in [`shm_dir`](Options::shm_dir)), used only as the memtables fill. Opening checks
    /// that the filesystem has that much free and fails with
    /// [`ErrorCode::ShmUnavailable`](crate::ErrorCode::ShmUnavailable) if it does not.
    pub fn memtable_budget(mut self, bytes: u64) -> Self {
        self.memtable_budget = bytes;
        self
    }

    /// How long a write, `flush` or `compact` may wait out a write stall (L0 compaction
    /// falling behind, or the memtable arena full while snapshots or a slow flush hold it)
    /// before it fails with [`ErrorCode::Busy`](crate::ErrorCode::Busy) (default 30 s).
    /// Latency-sensitive applications can fail fast and retry later; batch loaders can wait
    /// longer. `Duration::ZERO` refuses as soon as a wait would begin. On a clock that does
    /// not move (a simulator's), a wait nothing can end is refused at once whatever this
    /// says.
    ///
    /// ```
    /// use std::time::Duration;
    /// use pigeonhole::Options;
    ///
    /// let options = Options::default().write_stall_timeout(Duration::from_millis(500));
    /// # let _ = options;
    /// ```
    pub fn write_stall_timeout(mut self, timeout: Duration) -> Self {
        self.write_stall_timeout = Some(timeout);
        self
    }

    /// Block cache capacity in bytes.
    pub fn block_cache(mut self, bytes: usize) -> Self {
        self.block_cache = Some(bytes);
        self
    }

    /// Row cache capacity in bytes (default 0: disabled).
    pub fn row_cache(mut self, bytes: usize) -> Self {
        self.row_cache = bytes;
        self
    }

    /// Put the shared-memory file in `dir` (for example a tmpfs mount) instead of the
    /// memory-backed default.
    pub fn shm_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.shm_dir = Some(dir.into());
        self
    }

    /// Create the file if missing (default true).
    pub fn create_if_missing(mut self, yes: bool) -> Self {
        self.create_if_missing = yes;
        self
    }

    /// Registers a merge operator, referenced by families through its name
    /// ([`Family::merge_operator`]). A family naming an operator that is not registered is
    /// refused at creation, and a database whose families name one opens only with
    /// [`allow_unregistered_merge_operators`](Self::allow_unregistered_merge_operators), with
    /// [`ErrorCode::UnknownMergeOperator`](crate::ErrorCode::UnknownMergeOperator) otherwise.
    /// Registering an operator under the built-in name `pigeonhole.i64_add` replaces it.
    pub fn merge_operator(mut self, op: Arc<dyn MergeOperator>) -> Self {
        self.merge_operators.push(op);
        self
    }

    /// Open even if a family names an unregistered merge operator: the handle is read-only,
    /// compaction is off, and reads of affected cells fail with
    /// [`ErrorCode::UnknownMergeOperator`](crate::ErrorCode::UnknownMergeOperator).
    pub fn allow_unregistered_merge_operators(mut self, yes: bool) -> Self {
        self.allow_unregistered_merge_operators = yes;
        self
    }

    /// Let a table's tablets split, merge and move between shards (default on), so the
    /// writes of one table spread over every shard. Off, each table is one tablet on one
    /// shard: writes to a single table use one shard thread whatever [`shards`](Self::shards)
    /// says, and only commits touching several tables run on several shards.
    ///
    /// On, each shard's balancer splits a tablet at 256 MiB of live data or under sustained
    /// write skew, moves tablets to colder shards and merges small cold neighbours. Reads are
    /// never blocked by a change. Known limits: tablet owners are not stored in the file, so
    /// a reopen places the tablets again; and two commits on one row that are in flight
    /// together may be applied in either order while that row's tablet moves (a commit
    /// submitted after an earlier one returned is always applied after it).
    ///
    /// ```
    /// use pigeonhole::Options;
    ///
    /// // Keep every table on one shard.
    /// let options = Options::default().shards(4).tablet_changes(false);
    /// # let _ = options;
    /// ```
    pub fn tablet_changes(mut self, yes: bool) -> Self {
        self.tablet_changes = yes;
        self
    }

    /// Run on a custom filesystem implementation. Used by the deterministic simulation
    /// suites; applications never need it.
    #[doc(hidden)]
    pub fn vfs(mut self, vfs: pigeonhole_io::VfsRef) -> Self {
        self.vfs = Some(vfs);
        self
    }

    /// WAL segment size in bytes (default 64 MiB; a multiple of 32 KiB, decision D43). A test
    /// hook (ICR 0005): small segments keep simulated opens fast. It also lowers the largest
    /// value a commit may carry (decision D16). Applications never need it.
    #[doc(hidden)]
    pub fn wal_segment_size(mut self, bytes: u64) -> Self {
        self.wal_segment_size = Some(bytes);
        self
    }

    /// Balancer tuning, a test hook (ICR 0009): a pass every `interval`, write skew acted on
    /// once a shard writes `min_writes` rows in one, and size splits at `split_bytes`. The
    /// simulation suites use tiny values so tablets change during short runs. Only read with
    /// [`tablet_changes`](Self::tablet_changes) on. Applications never need it.
    #[doc(hidden)]
    pub fn tablet_balance(mut self, interval: Duration, min_writes: u64, split_bytes: u64) -> Self {
        self.tablet_balance = Some((interval, min_writes, split_bytes));
        self
    }
}

/// Options for a read-only handle in another process.
///
/// ```
/// use pigeonhole::ReaderOptions;
///
/// let options = ReaderOptions::default().block_cache(32 << 20);
/// # let _ = options;
/// ```
#[derive(Debug, Clone, Default)]
pub struct ReaderOptions {
    block_cache: Option<usize>,
    shm_dir: Option<PathBuf>,
    merge_operators: Vec<Arc<dyn MergeOperator>>,
    vfs: Option<VfsRef>,
}

impl ReaderOptions {
    /// Block cache capacity in bytes (each reader process has its own).
    pub fn block_cache(mut self, bytes: usize) -> Self {
        self.block_cache = Some(bytes);
        self
    }

    /// Where the writer put the shared-memory file, if not the default.
    pub fn shm_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.shm_dir = Some(dir.into());
        self
    }

    /// Registers a merge operator.
    pub fn merge_operator(mut self, op: Arc<dyn MergeOperator>) -> Self {
        self.merge_operators.push(op);
        self
    }

    /// Custom filesystem (simulation).
    #[doc(hidden)]
    pub fn vfs(mut self, vfs: pigeonhole_io::VfsRef) -> Self {
        self.vfs = Some(vfs);
        self
    }
}

/// Block-cache priority of a family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Priority {
    /// Evicted first.
    Low,
    /// The default.
    #[default]
    Normal,
    /// Evicted last.
    High,
}

/// Compaction strategy of a family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Compaction {
    /// Lowest read and space amplification (the default).
    #[default]
    Leveled,
    /// Lowest write amplification: sorted runs merge when their sizes are similar or space
    /// amplification passes 200%.
    Tiered,
    /// Drop whole files, with no rewrite, once their newest timestamp expires; for TTL'd,
    /// time-ordered data. Without a TTL nothing expires (small files still merge).
    FifoByTime,
}

/// A column family's policy. Stored in the file with the family.
///
/// The defaults: every version kept, no TTL, 10 bloom bits per key, LZ4 blocks of 16 KiB,
/// values over 4 KiB separated (Phase 2), the built-in `pigeonhole.i64_add` merge operator
/// (so `incr` works on any family), normal cache priority, leveled compaction.
///
/// Because every family carries the `i64` add operator, a column holds either plain values or
/// a counter: an `incr` on top of a base that is not an 8-byte `i64` fails at read with
/// [`ErrorCode::MergeFailed`](crate::ErrorCode::MergeFailed) (decision D41). Use `merge_operator("")` for a family without
/// one.
///
/// ```
/// use pigeonhole::{days, Family, Priority};
///
/// let hot = Family::default().max_versions(1).cache_priority(Priority::High);
/// let expiring = Family::default().ttl(days(30)).uncompressed();
/// assert_ne!(hot, expiring);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Family {
    options: FamilyOptions,
}

impl Default for Family {
    fn default() -> Self {
        Self {
            options: FamilyOptions {
                merge_operator: I64_ADD.to_owned(),
                ..FamilyOptions::default()
            },
        }
    }
}

impl Family {
    /// Keep at most `n` versions per column (0 keeps all).
    pub fn max_versions(mut self, n: u32) -> Self {
        self.options.max_versions = n;
        self
    }

    /// Cells older than `ttl` (by timestamp) expire.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.options.ttl_micros = u64::try_from(ttl.as_micros()).unwrap_or(u64::MAX);
        self
    }

    /// Bloom filter bits per key (0 disables filters; default 10).
    pub fn bloom_bits(mut self, bits: u8) -> Self {
        self.options.bloom_bits = bits;
        self
    }

    /// Store values longer than `bytes` in blob extents (default 4096; Phase 2). Stored with
    /// the family now; values stay inline until blob separation lands.
    pub fn blob_threshold(mut self, bytes: u32) -> Self {
        self.options.blob_threshold = bytes;
        self
    }

    /// LZ4 block compression (the default).
    pub fn lz4(mut self) -> Self {
        self.options.compression = Compression::Lz4;
        self
    }

    /// zstd block compression at `level` (libzstd's levels: 1 to 22, higher is smaller and
    /// slower to write; negative levels are faster; 0 and the default are 3). Smaller files
    /// than LZ4 for most data, at a higher CPU cost to write and read.
    pub fn zstd(mut self, level: i8) -> Self {
        self.options.compression = Compression::Zstd;
        self.options.compression_level = level;
        self
    }

    /// No block compression (hot, small families).
    pub fn uncompressed(mut self) -> Self {
        self.options.compression = Compression::None;
        self
    }

    /// Target uncompressed data-block size in bytes (default 16 KiB).
    pub fn block_size(mut self, bytes: u32) -> Self {
        self.options.block_size = bytes;
        self
    }

    /// Merge operator for this family, by registered name. `incr` needs none: it uses the
    /// built-in `pigeonhole.i64_add`, which is the default operator. An empty name leaves
    /// the family without one, so merge operands (and `incr`) are refused at commit.
    pub fn merge_operator(mut self, name: &str) -> Self {
        name.clone_into(&mut self.options.merge_operator);
        self
    }

    /// Block-cache priority.
    pub fn cache_priority(mut self, priority: Priority) -> Self {
        self.options.cache_priority = match priority {
            Priority::Low => CachePriority::Low,
            Priority::Normal => CachePriority::Normal,
            Priority::High => CachePriority::High,
        };
        self
    }

    /// Compaction strategy (default `Leveled`).
    pub fn compaction(mut self, strategy: Compaction) -> Self {
        self.options.compaction = match strategy {
            Compaction::Leveled => CompactionStyle::Leveled,
            Compaction::Tiered => CompactionStyle::Tiered,
            Compaction::FifoByTime => CompactionStyle::FifoByTime,
        };
        self
    }
}

impl Options {
    /// The engine configuration these options describe.
    pub(crate) fn to_engine(&self) -> EngineOptions {
        let vfs = self.vfs.clone().unwrap_or_else(default_vfs);
        let mut o = EngineOptions::new(vfs);
        o.create_if_missing = self.create_if_missing;
        o.shards = self.shards;
        o.compaction_threads = self.compaction_cores;
        o.pin_threads = self.pin_threads;
        o.durability = self.durability;
        o.memtable_budget = self.memtable_budget;
        o.memtable_freeze_bytes = self.memtable_budget / 4;
        if let Some(bytes) = self.block_cache {
            o.block_cache_bytes = bytes;
        }
        o.row_cache_bytes = self.row_cache;
        o.shm_dir.clone_from(&self.shm_dir);
        o.allow_unregistered_merge = self.allow_unregistered_merge_operators;
        if let Some(bytes) = self.wal_segment_size {
            o.wal.segment_size = bytes;
        }
        o.tablet_changes = self.tablet_changes;
        if let Some(timeout) = self.write_stall_timeout {
            o.write_stall_timeout_nanos = u64::try_from(timeout.as_nanos()).unwrap_or(u64::MAX);
        }
        if let Some((interval, min_writes, split_bytes)) = self.tablet_balance {
            o.balance_interval_nanos = u64::try_from(interval.as_nanos()).unwrap_or(u64::MAX);
            o.balance_min_writes = min_writes;
            o.tablet_split_bytes = split_bytes;
        }
        for op in &self.merge_operators {
            o.merge_operators.register(Arc::clone(op));
        }
        o
    }
}

impl ReaderOptions {
    /// The engine configuration these options describe.
    pub(crate) fn to_engine(&self) -> EngineOptions {
        let vfs = self.vfs.clone().unwrap_or_else(default_vfs);
        let mut o = EngineOptions::new(vfs);
        if let Some(bytes) = self.block_cache {
            o.block_cache_bytes = bytes;
        }
        o.shm_dir.clone_from(&self.shm_dir);
        for op in &self.merge_operators {
            o.merge_operators.register(Arc::clone(op));
        }
        o
    }
}

impl Family {
    /// The persisted options.
    pub(crate) fn to_engine(&self) -> FamilyOptions {
        self.options.clone()
    }
}

/// The platform's default filesystem backend.
fn default_vfs() -> VfsRef {
    pigeonhole_io::pread::PreadVfs::new(0)
}
