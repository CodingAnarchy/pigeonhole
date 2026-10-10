use std::path::PathBuf;

use pigeonhole_compaction::{MergeRegistry, PickerOptions};
use pigeonhole_format::Durability;
use pigeonhole_io::VfsRef;
use pigeonhole_wal::WalOptions;

/// Engine configuration. Process-local; nothing here is stored in the file except through
/// table and family creation.
///
/// ```
/// use pigeonhole_engine::EngineOptions;
/// use pigeonhole_io::sim::SimVfs;
///
/// let mut options = EngineOptions::new(SimVfs::new(1));
/// options.shards = 2;
/// options.create_if_missing = true;
/// assert_eq!(options.memtable_budget, 64 << 20);
/// assert_eq!(options.memtable_freeze_bytes, 16 << 20);
/// assert!(!options.pin_threads);
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EngineOptions {
    /// Filesystem and clocks (`PreadVfs` normally, `SimVfs` under test).
    pub vfs: VfsRef,
    /// Create the file if missing.
    pub create_if_missing: bool,
    /// Shard count; 0 means available CPUs.
    pub shards: usize,
    /// Pin each shard thread (and each compaction thread) to one CPU (default off).
    /// Engine-owned mode only; application-owned shards run on the caller's threads.
    ///
    /// Shard `i` is pinned to the `i`-th CPU (wrapping) of the set the opening thread may
    /// run on, so turn it on only when this database owns those CPUs: two pinned databases
    /// in one process, several containers sharing a host's CPUs, or an opener already pinned
    /// to a few CPUs would stack every shard on the same cores.
    pub pin_threads: bool,
    /// Extra threads dedicated to flush and compaction; 0 runs them on the shards.
    /// Engine-owned mode only: [`Engine::open_application_owned`](crate::Engine::open_application_owned)
    /// refuses a nonzero value with `InvalidArgument` (decision D40).
    pub compaction_threads: usize,
    /// Writer default durability.
    pub durability: Durability,
    /// Memtable arena per shard, in bytes (default 64 MiB).
    pub memtable_budget: u64,
    /// A memtable freezes at this many bytes (default 1/4 of the budget; never below two
    /// arena chunks, `memtable_budget / 32`).
    pub memtable_freeze_bytes: u64,
    /// Block cache capacity in bytes.
    pub block_cache_bytes: usize,
    /// Row cache capacity in bytes (0 disables it).
    pub row_cache_bytes: usize,
    /// Directory for the shared-memory file instead of the memory-backed default.
    pub shm_dir: Option<PathBuf>,
    /// Reader slots in the shared-memory region.
    pub reader_slots: u32,
    /// WAL segment configuration.
    pub wal: WalOptions,
    /// Bytes a shard's WAL stream may hold past its oldest record that is not yet in SSTs
    /// before the shard flushes the memtables that record (and the ones after it) wrote,
    /// however small they are (#137). Without it one write to a slot that is never written
    /// again would keep the stream's checkpoint, and so the WAL and the next open's replay,
    /// growing for the life of the process. 0 (the default) means twice `memtable_budget`.
    pub wal_pin_bytes: u64,
    /// Tablet split threshold in bytes of live data (default 256 MiB). A tablet whose SSTs
    /// hold at least this much splits in two; two adjacent cold tablets on one shard holding
    /// less than a quarter of it together merge. Only read when `tablet_changes` is on.
    pub tablet_split_bytes: u64,
    /// Lets tablets split, merge and move (default on). Off, every table stays one tablet
    /// on shard `tablet % shards` and the balancer never runs (the engine's own tests can
    /// still request changes through `test-hooks`; they are refused with `Unsupported`).
    ///
    /// Known limits when on:
    /// - each shard holds at most a quarter of its arena's chunks in `(tablet, family)`
    ///   slots; arenas are cut into at least 256 chunks when this is on, so every shard
    ///   serves at least 64 slots, and splits and moves past that are refused (the balancer
    ///   skips them);
    /// - tablet owners are not persisted: a reopen places every tablet again (on shard
    ///   `tablet % shards` when that keeps the shard within its slots, else on the shard
    ///   holding the fewest), losing earlier moves, and cuts the arenas into smaller chunks
    ///   when the tablets need more slots than that;
    /// - commits in flight together on one row may be applied in either order during a
    ///   move, even from one thread (see [`Engine::submit`](crate::Engine::submit)).
    pub tablet_changes: bool,
    /// How often each shard's balancer looks at its tablets (nanoseconds, default 100 ms):
    /// size splits, write-skew splits, moves to colder shards and merges of cold tablets.
    /// An idle shard backs off: a pass that finds nothing to do, with no writes and no tablet
    /// change since the last one, doubles the interval up to 10 s (never below this one);
    /// the next write or tablet change returns it to this interval. 0 turns the balancer off (tablets then change only through explicit requests). Only
    /// read when `tablet_changes` is on.
    pub balance_interval_nanos: u64,
    /// Rows a shard must write in one balancer interval before write skew moves or splits
    /// its tablets (default 2,000), so an idle database never reshuffles. Only read when
    /// `tablet_changes` is on.
    pub balance_min_writes: u64,
    /// A shard whose write load (a moving average over intervals) exceeds this multiple of
    /// the mean over all shards is skewed (default 1.25). Only read when `tablet_changes` is
    /// on.
    pub balance_skew: f64,
    /// Merge operators available to this process.
    pub merge_operators: MergeRegistry,
    /// Open even if a family names an unregistered merge operator: read-only, compaction
    /// off, and reads of affected cells fail with `UnknownMergeOperator`.
    pub allow_unregistered_merge: bool,
    /// Accept a database on a FUSE filesystem (D173, #299): only for a local FUSE mount
    /// the user trusts (ntfs-3g, an encrypted home directory). FUSE byte-range locks may be
    /// local to one host, so two hosts could both open it as writers, and a sync may not
    /// reach stable storage. Network and cluster filesystems stay refused. Default off.
    pub allow_fuse: bool,
    /// Read and write SST and blob extents through a second handle opened for direct I/O
    /// (#403): `O_DIRECT` on Linux, `F_NOCACHE` on macOS, `FILE_FLAG_NO_BUFFERING` on
    /// Windows, so the block cache is their only cache. A file system that refuses it keeps
    /// the buffered handle. Superblocks, the manifest and the WAL stay buffered. Default
    /// off.
    pub direct_io: bool,
    /// Compaction tuning (L0 trigger, level sizes, output SST size). The L0 trigger also
    /// drives write stalls.
    pub compaction: PickerOptions,
    /// How long a commit waits for a flush to free memtable arena room before it is refused
    /// with `Busy` (nanoseconds). A full arena stalls writers rather than refusing them; the
    /// wait is counted in `Metrics::stalls`.
    pub write_stall_timeout_nanos: u64,
    /// Overrides the memtable arena's chunk size (bytes; a multiple of 64, at least 1 KiB),
    /// which is otherwise sized for the slots a shard holds (#283). For tests that need a
    /// particular arena layout, such as one a flush can starve.
    #[doc(hidden)]
    pub arena_chunk_bytes: Option<usize>,
    /// How long a slot whose compaction failed waits before it is retried (nanoseconds,
    /// default 1 s), doubling with each failure in a row up to 60 times this. A refused WAL
    /// checkpoint retries on the same schedule. Mostly for tests, which shorten it.
    pub compaction_backoff_nanos: u64,
    /// How long the shard waits before retrying a failed flush (nanoseconds, default
    /// 10 ms), doubling with each failure in a row up to 100 times this. Mostly for tests.
    pub flush_backoff_nanos: u64,
    /// How soon a wait for memtable arena room on a moving clock first looks again for room
    /// that nothing announced (a snapshot dropped on another thread, a reader process's
    /// unpin): nanoseconds, default 1 ms, doubling up to 100 times this. Mostly for tests.
    pub room_recheck_nanos: u64,
    /// How long a thread waiting for its buffered (or non-durable) commit polls for the
    /// result before it parks: nanoseconds, default 15 µs (D198). A durable commit waits for
    /// a sync and parks at once. Zero always parks (battery-powered or CPU-constrained
    /// hosts). A poll that finds nothing backs off on that thread.
    pub commit_spin_nanos: u64,
    /// How long an engine-owned shard thread that just handled a message keeps polling its
    /// queue before it parks: nanoseconds, default 50 µs (D198). An idle shard never polls.
    /// Zero always parks. Application-owned shards are driven by the application and ignore
    /// it.
    pub shard_spin_nanos: u64,
}

impl EngineOptions {
    /// Defaults over the given filesystem.
    pub fn new(vfs: VfsRef) -> Self {
        let memtable_budget = 64 << 20;
        Self {
            vfs,
            create_if_missing: false,
            shards: 0,
            pin_threads: false,
            compaction_threads: 0,
            durability: Durability::GroupSync,
            memtable_budget,
            memtable_freeze_bytes: memtable_budget / 4,
            block_cache_bytes: 256 << 20,
            row_cache_bytes: 0,
            shm_dir: None,
            reader_slots: 126,
            wal: WalOptions::default(),
            wal_pin_bytes: 0,
            tablet_split_bytes: 256 << 20,
            tablet_changes: true,
            balance_interval_nanos: 100_000_000,
            balance_min_writes: 2_000,
            balance_skew: 1.25,
            merge_operators: MergeRegistry::default(),
            allow_unregistered_merge: false,
            allow_fuse: false,
            direct_io: false,
            compaction: PickerOptions::default(),
            write_stall_timeout_nanos: 30_000_000_000,
            arena_chunk_bytes: None,
            compaction_backoff_nanos: 1_000_000_000,
            flush_backoff_nanos: 10_000_000,
            room_recheck_nanos: 1_000_000,
            commit_spin_nanos: 15_000,
            shard_spin_nanos: 50_000,
        }
    }
}
