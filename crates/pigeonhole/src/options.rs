use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pigeonhole_format::Durability;

use crate::MergeOperator;

/// `n` days, for TTLs: `Family::default().ttl(days(30))`.
pub fn days(n: u64) -> Duration {
    Duration::from_secs(n * 86_400)
}

/// Database options. Process-local: nothing here is stored in the file, so reopening with
/// different options changes them. Zero config is valid.
#[derive(Debug, Clone)]
pub struct Options {
    _priv: (),
}

impl Default for Options {
    fn default() -> Self {
        todo!()
    }
}

impl Options {
    /// Writer default durability (default [`Durability::GroupSync`]).
    pub fn durability(self, durability: Durability) -> Self {
        todo!()
    }

    /// Number of shard threads (default: CPUs available to the process). `1` is a valid
    /// single-threaded configuration.
    pub fn shards(self, n: usize) -> Self {
        todo!()
    }

    /// Dedicate `k` extra pinned threads to flush and compaction.
    pub fn compaction_cores(self, k: usize) -> Self {
        todo!()
    }

    /// Memtable arena per shard, in bytes (default 64 MiB).
    pub fn memtable_budget(self, bytes: u64) -> Self {
        todo!()
    }

    /// Block cache capacity in bytes.
    pub fn block_cache(self, bytes: usize) -> Self {
        todo!()
    }

    /// Row cache capacity in bytes (default 0: disabled).
    pub fn row_cache(self, bytes: usize) -> Self {
        todo!()
    }

    /// Put the shared-memory file in `dir` (for example a tmpfs mount) instead of the
    /// memory-backed default.
    pub fn shm_dir(self, dir: impl Into<PathBuf>) -> Self {
        todo!()
    }

    /// Create the file if missing (default true).
    pub fn create_if_missing(self, yes: bool) -> Self {
        todo!()
    }

    /// Registers a merge operator, referenced by families through its name.
    pub fn merge_operator(self, op: Arc<dyn MergeOperator>) -> Self {
        todo!()
    }

    /// Open even if a family names an unregistered merge operator: the handle is read-only,
    /// compaction is off, and reads of affected cells fail with
    /// [`ErrorCode::UnknownMergeOperator`](crate::ErrorCode::UnknownMergeOperator).
    pub fn allow_unregistered_merge_operators(self, yes: bool) -> Self {
        todo!()
    }

    /// Run on a custom filesystem implementation. Used by the deterministic simulation
    /// suites; applications never need it.
    pub fn vfs(self, vfs: pigeonhole_io::VfsRef) -> Self {
        todo!()
    }
}

/// Options for a read-only handle in another process.
#[derive(Debug, Clone)]
pub struct ReaderOptions {
    _priv: (),
}

impl Default for ReaderOptions {
    fn default() -> Self {
        todo!()
    }
}

impl ReaderOptions {
    /// Block cache capacity in bytes (each reader process has its own).
    pub fn block_cache(self, bytes: usize) -> Self {
        todo!()
    }

    /// Where the writer put the shared-memory file, if not the default.
    pub fn shm_dir(self, dir: impl Into<PathBuf>) -> Self {
        todo!()
    }

    /// Registers a merge operator.
    pub fn merge_operator(self, op: Arc<dyn MergeOperator>) -> Self {
        todo!()
    }

    /// Custom filesystem (simulation).
    pub fn vfs(self, vfs: pigeonhole_io::VfsRef) -> Self {
        todo!()
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
    /// Lowest write amplification (Phase 2).
    Tiered,
    /// Drop whole files when their newest timestamp expires (Phase 2; needs a TTL).
    FifoByTime,
}

/// A column family's policy. Stored in the file with the family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Family {
    _priv: (),
}

impl Default for Family {
    fn default() -> Self {
        todo!()
    }
}

impl Family {
    /// Keep at most `n` versions per column (0 keeps all).
    pub fn max_versions(self, n: u32) -> Self {
        todo!()
    }

    /// Cells older than `ttl` (by timestamp) expire.
    pub fn ttl(self, ttl: Duration) -> Self {
        todo!()
    }

    /// Bloom filter bits per key (0 disables filters; default 10).
    pub fn bloom_bits(self, bits: u8) -> Self {
        todo!()
    }

    /// Store values longer than `bytes` in blob extents (default 4096; Phase 2).
    pub fn blob_threshold(self, bytes: u32) -> Self {
        todo!()
    }

    /// LZ4 block compression (the default).
    pub fn lz4(self) -> Self {
        todo!()
    }

    /// zstd block compression at `level` (Phase 2).
    pub fn zstd(self, level: i8) -> Self {
        todo!()
    }

    /// No block compression (hot, small families).
    pub fn uncompressed(self) -> Self {
        todo!()
    }

    /// Target uncompressed data-block size in bytes (default 16 KiB).
    pub fn block_size(self, bytes: u32) -> Self {
        todo!()
    }

    /// Merge operator for this family, by registered name. `incr` needs none: it uses the
    /// built-in `pigeonhole.i64_add`, which is the default operator.
    pub fn merge_operator(self, name: &str) -> Self {
        todo!()
    }

    /// Block-cache priority.
    pub fn cache_priority(self, priority: Priority) -> Self {
        todo!()
    }

    /// Compaction strategy.
    pub fn compaction(self, strategy: Compaction) -> Self {
        todo!()
    }
}
