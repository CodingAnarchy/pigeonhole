use std::path::Path;
use std::sync::Arc;

use pigeonhole_format::manifest::FamilyOptions;
use pigeonhole_format::{Durability, FamilyId, Seqno, TableId};

use crate::{
    CellData, EngineOptions, PendingCommit, Predicate, ReadSpec, Result, RowData, ScanCursor,
    ScanSpec, Snapshot, Txn, WriteBatch,
};

/// Whether this handle may write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The one writer process.
    Writer,
    /// A read-only process: no shard threads, no WAL, reads the writer's memtables through
    /// shared memory.
    Reader,
}

/// A family as the catalog knows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FamilyInfo {
    /// Id.
    pub id: FamilyId,
    /// Name.
    pub name: String,
    /// Persisted policy.
    pub options: FamilyOptions,
}

/// A table as the catalog knows it. Immutable; adding a family publishes a new `TableInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    /// Id.
    pub id: TableId,
    /// Name.
    pub name: String,
    /// Families in creation order.
    pub families: Vec<FamilyInfo>,
}

impl TableInfo {
    /// Looks up a family by name.
    pub fn family(&self, name: &str) -> Option<&FamilyInfo> {
        todo!()
    }
}

/// The outcome of a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitInfo {
    /// The commit's seqno.
    pub seqno: Seqno,
    /// The durability level actually applied.
    pub durability: Durability,
}

/// Counters and latencies.
#[derive(Debug, Clone, Default)]
pub struct Metrics {
    /// Commits per durability level, indexed by `Durability as usize`.
    pub commits: [u64; 4],
    /// Commit latency in nanoseconds (p50, p99, p99.9) per durability level.
    pub commit_latency_nanos: [[u64; 3]; 4],
    /// Flushes completed.
    pub flushes: u64,
    /// Compactions completed.
    pub compactions: u64,
    /// Write stalls (token-bucket waits) and total stalled nanoseconds.
    pub stalls: (u64, u64),
    /// Block cache hits and misses.
    pub block_cache: (u64, u64),
}

/// An open database. `Send + Sync`; share it with `Arc`.
#[derive(Debug)]
pub struct Engine {
    _priv: (),
}

impl Engine {
    /// Opens (or creates) the database as the writer: takes the writer lock, reads the
    /// superblock and manifest, sets up shared memory, replays every WAL stream, resolves
    /// prepared cross-shard commits, and starts the shards (in engine-owned mode).
    pub fn open(path: &Path, options: EngineOptions) -> Result<Arc<Engine>> {
        todo!()
    }

    /// Opens in application-owned mode: as [`Engine::open`], but returns one [`EngineShard`]
    /// per shard for the application to drive instead of starting threads. Starts no
    /// threads at all, so a nonzero `options.compaction_threads` fails with
    /// `InvalidArgument` before anything is opened (decision D40).
    pub fn open_application_owned(
        path: &Path,
        options: EngineOptions,
    ) -> Result<(Arc<Engine>, Vec<EngineShard>)> {
        todo!()
    }

    /// Opens a read-only handle in another process (Phase 4): attaches to the live region,
    /// claims a reader slot, loads the manifest named in the region header.
    ///
    /// The handle is read-only, but the process opens the database file **read-write**
    /// (decision D36): the shm-init and last-one-out presence locks are exclusive byte-range
    /// locks, which POSIX grants only on a writable descriptor. A reader writes nothing to
    /// the file. As with SQLite in WAL mode, a reader therefore needs write permission on
    /// the file and cannot open it on read-only media.
    pub fn open_reader(path: &Path, options: EngineOptions) -> Result<Arc<Engine>> {
        todo!()
    }

    /// Writer or reader.
    pub fn role(&self) -> Role {
        todo!()
    }

    // ---- catalog ----

    /// Creates a table with its families (a manifest commit).
    pub fn create_table(
        &self,
        name: &str,
        families: &[(String, FamilyOptions)],
    ) -> Result<Arc<TableInfo>> {
        todo!()
    }

    /// Adds a family to a table.
    pub fn add_family(
        &self,
        table: TableId,
        name: &str,
        options: FamilyOptions,
    ) -> Result<Arc<TableInfo>> {
        todo!()
    }

    /// Looks up a table by name.
    pub fn table(&self, name: &str) -> Option<Arc<TableInfo>> {
        todo!()
    }

    /// Every table.
    pub fn tables(&self) -> Vec<Arc<TableInfo>> {
        todo!()
    }

    /// Drops a table and all its data.
    pub fn drop_table(&self, table: TableId) -> Result<()> {
        todo!()
    }

    // ---- writes ----

    /// The writer default durability.
    pub fn default_durability(&self) -> Durability {
        todo!()
    }

    /// Changes the writer default; applies to commits that start afterwards.
    pub fn set_default_durability(&self, durability: Durability) {
        todo!()
    }

    /// Submits a batch: routes each row to its shard (inline if called on the owning shard),
    /// single-shard fast path or two-phase commit. `None` uses the writer default.
    pub fn submit(
        &self,
        batch: WriteBatch,
        durability: Option<Durability>,
    ) -> Result<PendingCommit> {
        todo!()
    }

    /// Submits and waits. Returns once the commit meets its durability level **and** is
    /// visible (`visible_seqno >= seqno`), so the caller reads its own write (D19). A
    /// cross-shard commit becomes visible only when every participant has applied, so its
    /// latency includes the slowest participant's group.
    pub fn commit(&self, batch: WriteBatch, durability: Option<Durability>) -> Result<CommitInfo> {
        todo!()
    }

    /// Applies `batch` (which must touch only `row`) if `predicate` holds, atomically on the
    /// owning shard. Returns whether it applied (Phase 2).
    pub fn check_and_mutate(
        &self,
        table: TableId,
        row: &[u8],
        predicate: &Predicate,
        batch: WriteBatch,
        durability: Option<Durability>,
    ) -> Result<(bool, Option<CommitInfo>)> {
        todo!()
    }

    /// Starts an optimistic transaction (Phase 4).
    pub fn begin(&self) -> Result<Txn> {
        todo!()
    }

    // ---- reads ----

    /// A snapshot of everything committed and applied so far. In a reader process this may
    /// do I/O: re-attach after a writer restart (`ShmRegion::is_stale`) and reload the
    /// manifest when its version changed (`Pager::reload_root`).
    pub fn snapshot(&self) -> Result<Snapshot> {
        todo!()
    }

    /// The newest visible version of one cell.
    pub fn get(
        &self,
        snapshot: &Snapshot,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
    ) -> Result<Option<CellData>> {
        todo!()
    }

    /// The newest visible version of one cell as of now, without creating a snapshot: loads
    /// the view through an `arc-swap` guard (no reference-count traffic) and pins only what
    /// the returned value needs.
    pub fn get_latest(
        &self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
    ) -> Result<Option<CellData>> {
        todo!()
    }

    /// Reads one row, projected by `spec`. `None` if the row has no visible cell.
    pub fn read_row(
        &self,
        snapshot: &Snapshot,
        table: TableId,
        row: &[u8],
        spec: &ReadSpec,
    ) -> Result<Option<RowData>> {
        todo!()
    }

    /// Starts an ordered scan.
    pub fn scan(&self, snapshot: &Snapshot, table: TableId, spec: ScanSpec) -> Result<ScanCursor> {
        todo!()
    }

    // ---- maintenance ----

    /// Freezes and flushes every memtable; returns when the SSTs are in the manifest.
    pub fn flush(&self) -> Result<()> {
        todo!()
    }

    /// Compacts every family of `table` (or all tables) fully.
    pub fn compact(&self, table: Option<TableId>) -> Result<()> {
        todo!()
    }

    /// Writes a consistent single-file copy to `dest` while writers run.
    pub fn backup(&self, dest: &Path) -> Result<()> {
        todo!()
    }

    /// Relocates tail extents and truncates the file.
    pub fn shrink(&self) -> Result<u64> {
        todo!()
    }

    /// Current metrics.
    pub fn metrics(&self) -> Metrics {
        todo!()
    }

    /// Stops the shards, flushes nothing extra, and if this is the last process checkpoints
    /// and removes the WAL files and the shared-memory region.
    pub fn close(&self) -> Result<()> {
        todo!()
    }
}

/// One shard in application-owned mode. Move it to the thread that should run it.
#[derive(Debug)]
pub struct EngineShard {
    _priv: (),
}

impl EngineShard {
    /// Shard index.
    pub fn index(&self) -> u16 {
        todo!()
    }

    /// Runs queued writes, the group commit, and background work until `deadline_nanos`.
    /// Returns whether work remains.
    pub fn run_once(&mut self, deadline_nanos: u64) -> bool {
        todo!()
    }

    /// Commits `batch` inline if every row belongs to this shard; otherwise submits it.
    pub fn commit_local(
        &mut self,
        batch: WriteBatch,
        durability: Option<Durability>,
    ) -> Result<PendingCommit> {
        todo!()
    }

    /// Registers a callback the engine calls when work arrives for this shard.
    pub fn set_wakeup(&mut self, wake: Box<dyn Fn() + Send + Sync>) {
        todo!()
    }
}
