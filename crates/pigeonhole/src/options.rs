use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pigeonhole_engine::{
    CachePriority, CompactionStyle, Compression, EngineOptions, FamilyKind, FamilyOptions,
};
use pigeonhole_format::Durability;
use pigeonhole_io::VfsRef;

use crate::MergeOperator;

/// Name of the built-in `i64` add operator, which counter families sum with.
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
    allow_fuse: bool,
    io_backend: Option<IoBackend>,
    direct_io: Option<bool>,
    vfs: Option<VfsRef>,
    wal_segment_size: Option<u64>,
    tablet_changes: bool,
    tablet_balance: Option<(Duration, u64, u64)>,
    write_stall_timeout: Option<Duration>,
    commit_spin: Option<Duration>,
    shard_spin: Option<Duration>,
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
            allow_fuse: false,
            io_backend: None,
            direct_io: None,
            vfs: None,
            wal_segment_size: None,
            tablet_changes: true,
            tablet_balance: None,
            write_stall_timeout: None,
            commit_spin: None,
            shard_spin: None,
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

    /// How long a thread waiting for its buffered (or non-durable) commit polls for the
    /// result before it sleeps (default zero for now; D198's 15 µs default lands with its
    /// follow-up). A shard usually answers within a few
    /// microseconds, and putting the thread to sleep and waking it costs that again or more
    /// (D198). A durable commit waits for a disk sync and sleeps at once. A wait that keeps
    /// finding nothing polls less often. `Duration::ZERO` always sleeps at once: for
    /// battery-powered or CPU-constrained hosts.
    ///
    /// ```
    /// use std::time::Duration;
    /// use pigeonhole::Options;
    ///
    /// let options = Options::default().commit_spin(Duration::ZERO).shard_spin(Duration::ZERO);
    /// # let _ = options;
    /// ```
    pub fn commit_spin(mut self, window: Duration) -> Self {
        self.commit_spin = Some(window);
        self
    }

    /// How long a shard thread that just handled a commit keeps polling for the next one
    /// before it sleeps (default zero for now; D198's 50 µs default lands with its
    /// follow-up), so a steady stream of commits is taken
    /// without waking the thread each time. An idle database never polls, and polls that
    /// keep finding nothing poll less often. `Duration::ZERO` always sleeps at once. Ignored
    /// by application-owned shards, whose loop the application runs.
    pub fn shard_spin(mut self, window: Duration) -> Self {
        self.shard_spin = Some(window);
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

    /// Accept a database on a FUSE filesystem (default off; decision D173). Use it only for a
    /// **local** FUSE mount you trust, such as ntfs-3g or an encrypted home directory
    /// (gocryptfs). FUSE byte-range locks may be local to one host, so two hosts could
    /// both open the file as writers and corrupt it, and a sync may not reach stable
    /// storage, so a commit acknowledged as durable could be lost in a power failure. Never
    /// set it for sshfs, s3fs, gcsfuse or other network-backed FUSE mounts. Network and
    /// cluster filesystems (NFS, SMB, GPFS, ...) are refused with
    /// [`ErrorCode::NetworkFilesystem`](crate::ErrorCode::NetworkFilesystem) either way.
    pub fn allow_fuse(mut self, yes: bool) -> Self {
        self.allow_fuse = yes;
        self
    }

    /// The I/O backend (default [`IoBackend::Pread`]). With [`IoBackend::Uring`] the open
    /// fails with [`ErrorCode::Unsupported`](crate::ErrorCode::Unsupported) where io_uring
    /// is unavailable (not Linux, an old kernel, a container that blocks it);
    /// [`IoBackend::Auto`] uses `pread` there instead.
    ///
    /// ```
    /// use pigeonhole::{IoBackend, Options};
    ///
    /// let options = Options::default().io_backend(IoBackend::Auto);
    /// # let _ = options;
    /// ```
    pub fn io_backend(mut self, backend: IoBackend) -> Self {
        self.io_backend = Some(backend);
        self
    }

    /// Read and write SST and blob extents with direct I/O (#403): `O_DIRECT` on Linux,
    /// `F_NOCACHE` on macOS, `FILE_FLAG_NO_BUFFERING` on Windows, through a second handle on
    /// the file, so the block cache is their only cache. Superblocks, the manifest and the
    /// WAL stay buffered. A file system that refuses direct I/O keeps buffered I/O. Default
    /// off; with it on, size [`Options::block_cache`] for the working set, since the OS no
    /// longer caches SST blocks.
    pub fn direct_io(mut self, yes: bool) -> Self {
        self.direct_io = Some(yes);
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
    allow_fuse: bool,
    io_backend: Option<IoBackend>,
    direct_io: Option<bool>,
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

    /// Accept a database on a FUSE filesystem, as [`Options::allow_fuse`] (the same risks
    /// apply; default off).
    pub fn allow_fuse(mut self, yes: bool) -> Self {
        self.allow_fuse = yes;
        self
    }

    /// The I/O backend, as [`Options::io_backend`] (default [`IoBackend::Pread`]).
    pub fn io_backend(mut self, backend: IoBackend) -> Self {
        self.io_backend = Some(backend);
        self
    }

    /// Direct I/O for SST and blob extents, as [`Options::direct_io`] (default off).
    pub fn direct_io(mut self, yes: bool) -> Self {
        self.direct_io = Some(yes);
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

/// A column family's policy. Stored in the file with the family; a family's kind and
/// options are fixed when it is created.
///
/// The defaults: every version kept, no TTL, 10 bloom bits per key, LZ4 blocks of 16 KiB,
/// values over 4 KiB separated, no merge operator, normal cache priority, leveled
/// compaction.
///
/// Counters live in a **counter family**, declared with [`Family::counter`] (decision D179,
/// after Bigtable's aggregate families). `incr` on any other family fails with
/// [`ErrorCode::InvalidArgument`](crate::ErrorCode::InvalidArgument).
///
/// ```
/// use pigeonhole::{days, Family, Priority};
///
/// let hot = Family::default().max_versions(1).cache_priority(Priority::High);
/// let expiring = Family::default().ttl(days(30)).uncompressed();
/// assert_ne!(hot, expiring);
/// let hits = Family::counter();
/// let daily = Family::counter().ttl(days(90));
/// assert_ne!(hits, daily);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Family {
    options: FamilyOptions,
}

impl Family {
    /// A counter family: every cell is an `i64` sum (decision D179).
    ///
    /// - [`incr`](crate::RowMutation::incr) adds to the column's counter, which lives at
    ///   one fixed timestamp (0), so a counter is one cell however often it is incremented:
    ///   its increments combine when read and when compacted.
    /// - [`incr_at`](crate::RowMutation::incr_at) adds to a *bucket*: the version at a
    ///   timestamp you choose (an hour or a day, say). Each bucket is its own version. The
    ///   TTL expires each bucket; `max_versions` limits what reads return, but compaction
    ///   keeps older buckets (a later delete of a newer one shows them again), so bound
    ///   storage with a TTL.
    /// - [`put_i64`](crate::RowMutation::put_i64) sets the counter (`put_i64_at` a bucket);
    ///   later increments add to it. Other puts are refused with `InvalidArgument`.
    /// - A delete removes what earlier commits wrote: an `incr` after `delete_column`
    ///   starts the counter again from 0. A write in the same commit as the delete is not
    ///   hidden (`incr` then `delete_column` in one mutation leaves the increment). A
    ///   delete without a timestamp takes the commit timestamp, so a bucket at a later
    ///   timestamp survives it.
    ///
    /// With a TTL the fixed timestamp would expire at once, so such a family takes only
    /// buckets (`incr_at`, `put_i64_at`); `incr` and `put_i64` fail with `InvalidArgument`.
    ///
    /// ```
    /// use pigeonhole::{Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default())?;
    /// let t = db.table("pages")?.family("hits", Family::counter()).create_if_missing()?;
    /// t.mutate(b"home").incr("hits", b"total", 2).commit()?;
    /// t.mutate(b"home").incr("hits", b"total", 3).commit()?;
    /// assert_eq!(t.get(b"home", "hits", b"total")?.unwrap().as_i64(), Some(5));
    ///
    /// // Hourly buckets: one version per hour.
    /// let hour = 3_600_000_000; // microseconds
    /// t.mutate(b"home").incr_at("hits", b"hourly", 7 * hour, 1).commit()?;
    /// t.mutate(b"home").incr_at("hits", b"hourly", 8 * hour, 4).commit()?;
    /// t.mutate(b"home").incr_at("hits", b"hourly", 8 * hour, 1).commit()?;
    /// let row = t.row(b"home").qualifier_prefix(b"hourly").versions(0).read()?.unwrap();
    /// let hourly: Vec<_> = row
    ///     .iter()
    ///     .map(|e| (e.cell.timestamp(), e.cell.as_i64().unwrap()))
    ///     .collect();
    /// assert_eq!(hourly, [(8 * hour, 5), (7 * hour, 1)]);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn counter() -> Self {
        Self {
            options: FamilyOptions::default()
                .merge_operator(I64_ADD.to_owned())
                .kind(FamilyKind::Counter),
        }
    }

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

    /// Store values longer than `bytes` in blob files (default 4096; `u32::MAX` never). A
    /// flush or compaction moves such a value out of the family's tree, which keeps a 16-byte
    /// pointer, so compactions and scans of other columns do not copy it. Blob GC rewrites
    /// blob files once they are half garbage.
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

    /// Merge operator for this family, by registered name (see
    /// [`Options::merge_operator`]): [`RowMutation::merge`](crate::RowMutation::merge)
    /// writes its operands. Counters need none: declare a [`Family::counter`]. A counter
    /// family sums with the built-in `pigeonhole.i64_add` and refuses any other name at
    /// creation.
    ///
    /// A family that names `pigeonhole.i64_add` without being a counter family is how
    /// families created by 0.1.0 read: `incr` there writes at the commit timestamp and
    /// runs of increments fold across timestamps (decision D41). New counters belong in a
    /// counter family; see the migration note in the changelog.
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
    pub(crate) fn to_engine(&self) -> Result<EngineOptions, pigeonhole_engine::Error> {
        let vfs = match &self.vfs {
            Some(v) => Arc::clone(v),
            None => backend_vfs(self.io_backend)?,
        };
        let direct_io = self.direct_io.unwrap_or_else(env_direct_io);
        let mut o = EngineOptions::new(vfs);
        o.direct_io = direct_io;
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
        o.allow_fuse = self.allow_fuse;
        if let Some(bytes) = self.wal_segment_size {
            o.wal.segment_size = bytes;
        }
        o.tablet_changes = self.tablet_changes;
        if let Some(window) = self.commit_spin {
            o.commit_spin_nanos = u64::try_from(window.as_nanos()).unwrap_or(u64::MAX);
        }
        if let Some(window) = self.shard_spin {
            o.shard_spin_nanos = u64::try_from(window.as_nanos()).unwrap_or(u64::MAX);
        }
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
        Ok(o)
    }
}

impl ReaderOptions {
    /// The engine configuration these options describe.
    pub(crate) fn to_engine(&self) -> Result<EngineOptions, pigeonhole_engine::Error> {
        let vfs = match &self.vfs {
            Some(v) => Arc::clone(v),
            None => backend_vfs(self.io_backend)?,
        };
        let direct_io = self.direct_io.unwrap_or_else(env_direct_io);
        let mut o = EngineOptions::new(vfs);
        o.direct_io = direct_io;
        if let Some(bytes) = self.block_cache {
            o.block_cache_bytes = bytes;
        }
        o.shm_dir.clone_from(&self.shm_dir);
        o.allow_fuse = self.allow_fuse;
        for op in &self.merge_operators {
            o.merge_operators.register(Arc::clone(op));
        }
        Ok(o)
    }
}

impl Family {
    /// The persisted options.
    pub(crate) fn to_engine(&self) -> FamilyOptions {
        self.options.clone()
    }
}

/// Which I/O backend a database runs its file I/O on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum IoBackend {
    /// A pool of threads serving submitted I/O with `pread` and `pwrite`, on every platform
    /// (the default).
    #[default]
    Pread,
    /// io_uring (Linux): submitted I/O goes through a ring instead of a thread pool. Opening
    /// fails where it is unavailable.
    Uring,
    /// io_uring where it is available, `pread` otherwise.
    Auto,
}

/// A database's io_uring rings now ([`Pigeonhole::io_rings`](crate::Pigeonhole::io_rings)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub struct IoRings {
    /// Rings: the shared one, plus one per shard thread that got its own.
    pub rings: usize,
    /// Rings with a registered buffer pool; the rest use plain buffers.
    pub pooled: usize,
}

/// The backend `backend` names; unset, the one `PIGEONHOLE_IO` names (a test variable:
/// `pread`, `uring` or `auto`), else [`IoBackend::Pread`].
fn backend_vfs(backend: Option<IoBackend>) -> Result<VfsRef, pigeonhole_engine::Error> {
    let backend = backend.unwrap_or_else(|| match std::env::var("PIGEONHOLE_IO").as_deref() {
        Ok("uring") => IoBackend::Uring,
        Ok("auto") => IoBackend::Auto,
        _ => IoBackend::Pread,
    });
    let pread = || -> VfsRef { pigeonhole_io::pread::PreadVfs::new(0) };
    match backend {
        IoBackend::Pread => Ok(pread()),
        IoBackend::Uring => uring_vfs().ok_or(pigeonhole_engine::Error::Unsupported(
            "io_uring is unavailable on this system",
        )),
        IoBackend::Auto => Ok(uring_vfs().unwrap_or_else(pread)),
    }
}

/// `PIGEONHOLE_DIRECT=1` (a test variable) turns direct I/O on where the options leave it
/// unset.
fn env_direct_io() -> bool {
    std::env::var("PIGEONHOLE_DIRECT").as_deref() == Ok("1")
}

/// An io_uring backend, if the kernel offers one.
#[cfg(target_os = "linux")]
fn uring_vfs() -> Option<VfsRef> {
    pigeonhole_io::uring::UringVfs::new()
        .ok()
        .map(|v| v as VfsRef)
}

#[cfg(not(target_os = "linux"))]
fn uring_vfs() -> Option<VfsRef> {
    None
}
