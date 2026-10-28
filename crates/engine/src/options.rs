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
/// assert!(options.pin_threads);
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EngineOptions {
    /// Filesystem and clocks (`PreadVfs` normally, `SimVfs` under test).
    pub vfs: VfsRef,
    /// Create the file if missing.
    pub create_if_missing: bool,
    /// Shard count; 0 means one per physical core (D220).
    pub shards: usize,
    /// Pin each shard thread (and each compaction thread) to a CPU of its own (default on,
    /// D235). Engine-owned mode only; application-owned shards run on the caller's threads.
    ///
    /// The CPUs come from the opening thread's affinity set, one per physical core first and
    /// then their SMT siblings (D220), skipping CPUs another pinned engine in this process
    /// holds. Threads stay unpinned, and the open still succeeds, where the platform has no
    /// hard affinity (macOS), a CPU quota is narrower than the affinity set, there are fewer
    /// free CPUs than threads, or the OS refuses a pin; `Engine::metrics` reports it
    /// (`pinned_threads`, `unpinned`). Engines in *other processes* on the host don't know
    /// about each other and would pin to the same CPUs: give each process its own CPUs (an
    /// affinity mask, a cpuset) or turn this off.
    pub pin_threads: bool,
    /// Extra threads dedicated to flush and compaction; 0 (the default here) runs them on the
    /// shards. `pigeonhole::Options` defaults engine-owned opens to a quarter of the physical
    /// cores (half with `CpuProfile::Server`), 1 to one per shard (D289); this crate keeps 0 so its deterministic tests run flush on the shard.
    /// Engine-owned mode only: [`Engine::open_application_owned`](crate::Engine::open_application_owned)
    /// refuses a nonzero value with `InvalidArgument` (decision D40).
    pub compaction_threads: usize,
    /// Engine-owned mode only: the driver threads the shards run on (D289, ICR 0079). 0
    /// (the default) gives each shard a thread of its own; `k` runs shard `i` on thread
    /// `i % k`.
    pub shard_threads: usize,
    /// Engine-owned mode only: size the driver threads to the load (D289 PR 2), up to
    /// `shard_threads` (one per shard if 0). Off by default here, so this crate's tests run
    /// on fixed threads; `pigeonhole::Options::adaptive_shard_threads` turns it on (opt-in,
    /// #1015).
    pub adaptive_drivers: bool,
    /// Application-owned mode only: background workers for flush and compaction that the
    /// application drives from threads it owns (D284), handed out by
    /// [`Engine::open_application_owned_with`](crate::Engine::open_application_owned_with);
    /// 0 (the default) runs flush and compaction on the shards. Engine-owned opens use
    /// `compaction_threads` instead and ignore this.
    pub background_workers: usize,
    /// Writer default durability.
    pub durability: Durability,
    /// Memtable arena per shard, in bytes (default 64 MiB).
    pub memtable_budget: u64,
    /// A memtable freezes at this many bytes (default 1/4 of the budget; never below two
    /// arena chunks, `memtable_budget / 32`).
    pub memtable_freeze_bytes: u64,
    /// Block cache capacity in bytes.
    pub block_cache_bytes: usize,
    /// Row cache capacity in bytes (0 disables it; writer process only, D201).
    pub row_cache_bytes: usize,
    /// A counter get that folds at least this many entries leaves its result in the active
    /// memtable as a base, so the counter's next reads stop there (D291; default 128, 0
    /// turns it off). Writer process only. For tests and benchmarks.
    #[doc(hidden)]
    pub counter_checkpoint_steps: u64,
    /// Largest encoded family row the row cache stores (default 4 KiB).
    pub row_cache_max_row: usize,
    /// `(table, family)` names the row cache serves; empty means every family.
    pub row_cache_families: Vec<(String, String)>,
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
    /// growing for the life of the process. 0 (the default) means twice `memtable_budget`,
    /// which the 2026-10-12 gate measured against four times: as fast, with a smaller WAL and
    /// a shorter replay after a crash (decision D229).
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
    /// the mean over all shards is skewed (default 1.25). It acts on the skew after 10
    /// skewed passes in a row, and keeps acting until its load falls below
    /// `1 + 0.4 × (skew − 1)` times the mean (D214). Skew while the mean is falling (other
    /// shards going idle) doesn't count; skew on the shards' slow averages (about 10 s) is
    /// acted on too, one change per 10 s (D224). Only read when `tablet_changes` is on.
    pub balance_skew: f64,
    /// Packing (D217, default on): while writes are light, the balancer keeps tablets with
    /// write load on as few shards as stay about half busy (the summed busy fraction of all
    /// shards over 0.5, rounded up), so a commit seldom finds its shard asleep; it spreads
    /// them again within about a second once those shards pass 75% busy. Application-owned
    /// shards report no busy time, so it never acts on them. Off, tablets spread over every
    /// shard as before. Only read when `tablet_changes` is on.
    pub balance_pack: bool,
    /// Merge operators available to this process.
    pub merge_operators: MergeRegistry,
    /// Open read-only instead of failing when this build cannot write the file (D269, ICR
    /// 0059): the file sets a read-only-compatible feature bit this build does not know
    /// (otherwise `UnsupportedFeature`), or a family names a merge operator this process has
    /// not registered (otherwise `UnknownMergeOperator`; reads of affected cells fail with
    /// it). Such an open writes nothing to the database's files: no format upgrade, no new
    /// WAL segment, no flush or compaction, no clean-close commit. It replays the WAL into
    /// memory, and fails with `ReadOnlyOpen` if that does not fit `memtable_budget`.
    pub read_only_fallback: bool,
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
    /// How long a commit, `flush` or `compact` waits before it is refused with `Busy`
    /// (nanoseconds). A full arena stalls writers rather than refusing them: each waiting
    /// call is refused once it has itself waited this long (D240), and the shard's wait is
    /// counted in `Metrics::stalls`. A commit that waits on a tablet change, or a cross-shard
    /// commit retried after a participant refused it for one, or below its timestamp floor
    /// (D244, with tablet changes on or off), is refused this long after it was submitted
    /// (D241). The L0 stall paces commits and never refuses them.
    pub write_stall_timeout_nanos: u64,
    /// Overrides the memtable arena's chunk size (bytes; a multiple of 64, at least 1 KiB),
    /// which is otherwise sized for the slots a shard holds (#283). For tests that need a
    /// particular arena layout, such as one a flush can starve.
    #[doc(hidden)]
    pub arena_chunk_bytes: Option<usize>,
    /// Overrides the page file's growth ahead of need, `(block, from)` in bytes: once no
    /// free block of `block` bytes is left and the file is at least `from` long, it grows by
    /// such a block outside the allocator lock (64 MiB from 128 MiB by default). For tests on
    /// the simulator, which keeps whole files in memory, so they reach it.
    #[doc(hidden)]
    pub grow_ahead: Option<(u64, u64)>,
    /// Overrides how many balancer passes (or as many base intervals of clock) a tablet must
    /// go unwritten, and have been on its shard, before it counts as cold for merges and
    /// consolidation (100 by default, D224). For tests of merges, which shorten it.
    #[doc(hidden)]
    pub balance_cold_passes: Option<u64>,
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
    /// result before it parks: nanoseconds, default 50 µs (D198, D253). A durable commit waits for
    /// a sync and parks at once. Zero always parks (battery-powered or CPU-constrained
    /// hosts). A poll that finds nothing backs off on that thread.
    pub commit_spin_nanos: u64,
    /// How long an engine-owned shard thread that just handled a message keeps polling its
    /// queue before it parks: nanoseconds, default 200 µs (D198, amended 2026-10-19 from
    /// 50 µs). An idle shard never polls.
    /// Zero always parks. Application-owned shards are driven by the application and ignore
    /// it.
    pub shard_spin_nanos: u64,
    /// The writer keeps a process-local stale-tail index beside each memtable, and row reads
    /// and scans pass a finished column's superseded memtable versions in one jump (D194,
    /// on by default per D199). Same results either way. It costs a write that overwrites a
    /// column about 200 instructions (D199). Reader processes have no index and step.
    pub memtable_tail_index: bool,
    /// Bytes of async commits a shard holds submitted and not yet resolved before the next
    /// one waits to submit (D210). 0 (the default) means half `memtable_budget`.
    pub async_commit_bound: u64,
}

impl EngineOptions {
    /// Defaults over the given filesystem.
    pub fn new(vfs: VfsRef) -> Self {
        let memtable_budget = 64 << 20;
        Self {
            vfs,
            create_if_missing: false,
            shards: 0,
            pin_threads: true,
            compaction_threads: 0,
            shard_threads: 0,
            adaptive_drivers: false,
            background_workers: 0,
            durability: Durability::GroupSync,
            memtable_budget,
            memtable_freeze_bytes: memtable_budget / 4,
            block_cache_bytes: 256 << 20,
            row_cache_bytes: 0,
            counter_checkpoint_steps: crate::read::CHECKPOINT_STEPS,
            row_cache_max_row: 4 << 10,
            row_cache_families: Vec::new(),
            shm_dir: None,
            reader_slots: 128,
            wal: WalOptions::default(),
            wal_pin_bytes: 0,
            tablet_split_bytes: 256 << 20,
            tablet_changes: true,
            balance_interval_nanos: 100_000_000,
            balance_min_writes: 2_000,
            balance_skew: 1.25,
            balance_pack: true,
            merge_operators: MergeRegistry::default(),
            read_only_fallback: false,
            allow_fuse: false,
            direct_io: false,
            compaction: PickerOptions::default(),
            write_stall_timeout_nanos: 30_000_000_000,
            arena_chunk_bytes: None,
            grow_ahead: None,
            balance_cold_passes: None,
            compaction_backoff_nanos: 1_000_000_000,
            flush_backoff_nanos: 10_000_000,
            room_recheck_nanos: 1_000_000,
            commit_spin_nanos: 50_000,
            shard_spin_nanos: 200_000,
            memtable_tail_index: true,
            async_commit_bound: 0,
        }
    }
}
