use std::path::PathBuf;

use pigeonhole_compaction::MergeRegistry;
use pigeonhole_format::Durability;
use pigeonhole_io::VfsRef;
use pigeonhole_wal::WalOptions;

/// Engine configuration. Process-local; nothing here is stored in the file except through
/// table and family creation.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EngineOptions {
    /// Filesystem and clocks (`PreadVfs` normally, `SimVfs` under test).
    pub vfs: VfsRef,
    /// Create the file if missing.
    pub create_if_missing: bool,
    /// Shard count; 0 means available CPUs.
    pub shards: usize,
    /// Pin shard threads (engine-owned mode; see `Engine::open_application_owned` for the
    /// other mode).
    pub pin_threads: bool,
    /// Extra threads dedicated to flush and compaction; 0 runs them on the shards.
    /// Engine-owned mode only: [`Engine::open_application_owned`](crate::Engine::open_application_owned)
    /// refuses a nonzero value with `InvalidArgument` (decision D40).
    pub compaction_threads: usize,
    /// Writer default durability.
    pub durability: Durability,
    /// Memtable arena per shard, in bytes (default 64 MiB).
    pub memtable_budget: u64,
    /// A memtable freezes at this many bytes (default 1/4 of the budget).
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
    /// Tablet split threshold in bytes of live data (default 256 MiB).
    pub tablet_split_bytes: u64,
    /// Merge operators available to this process.
    pub merge_operators: MergeRegistry,
    /// Open even if a family names an unregistered merge operator: read-only, compaction
    /// off, and reads of affected cells fail with `UnknownMergeOperator`.
    pub allow_unregistered_merge: bool,
}

impl EngineOptions {
    /// Defaults over the given filesystem.
    pub fn new(vfs: VfsRef) -> Self {
        todo!()
    }
}
