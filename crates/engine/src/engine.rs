#[cfg(feature = "test-hooks")]
pub(crate) mod hooks;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use arc_swap::ArcSwap;
use pigeonhole_cache::BlockCache;
use pigeonhole_compaction::MergeRegistry;
use pigeonhole_format::key::compare;
use pigeonhole_format::manifest::{Edit, FamilyKind, FamilyOptions};
use pigeonhole_format::shm::ViewRecord;
use pigeonhole_format::value::{ValueRef, ValueTag, encode_value};
use pigeonhole_format::wal::{BatchBuilder, Mutation, WalRecord};
use pigeonhole_format::{
    Durability, FamilyId, Kind, Lsn, ManifestVersion, Seqno, StreamId, TableId, TabletId, Timestamp,
};
use pigeonhole_io::{ErrorKind, FileRef, Locality, OpenOptions};
use pigeonhole_memtable::{ArenaRegion, MemtableReader, ShardArena};
use pigeonhole_pager::Pager;
use pigeonhole_runtime::{
    Runtime, RuntimeConfig, ShardDriver, ShardId, Submitter, Waiter, completion,
};
use pigeonhole_shm::{Presence, ReaderSlot, Role as ShmRole, ShmConfig, ShmRegion, WriterLock};
use pigeonhole_sst::SstWriterOptions;
use pigeonhole_wal::{Recovery, Wal, WalStream, discover_streams, stream_path};
use smallvec::SmallVec;

use crate::catalog::{Catalog, FamilyMeta, MergeKind};
use crate::flush::{SstSink, write_memtable};
use crate::manifest::{self, ManifestWriter, ReqKind};
use crate::read::{self, get_in};
use crate::shard::{
    BalanceConfig, CloseState, CommitReq, CoordinateReq, FreezeWaiters, LoadSlot, Locks,
    MaintenanceGuard, Padded, ReplayedKind, Reply, ShardMetrics, ShardMsg, ShardState, Shared,
    VisibilityWaiters, bucket_floor, split_by_shard,
};
use crate::snapshot::{
    LiveSeqnos, LiveSnapshot, LiveViews, MemSet, SeqnoPin, ShardMems, SstSet, TabletEntry,
    TabletMap, View, ViewPin,
};
use crate::write::{COUNTER_TS, ReadKey};

/// The shards a commit writes to: inline for up to four (no allocation per commit, #320).
pub(crate) type Shards = smallvec::SmallVec<[ShardId; 4]>;
use crate::{
    CellData, EngineOptions, Error, PendingCheck, PendingCommit, Predicate, ReadSpec, Result,
    RowCacheStats, RowData, ScanCursor, ScanSpec, Snapshot, Txn, WriteBatch,
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
#[non_exhaustive]
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
#[non_exhaustive]
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
        self.families.iter().find(|f| f.name == name)
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
#[non_exhaustive]
pub struct Metrics {
    /// Commits per durability level, indexed by `Durability as usize`.
    pub commits: [u64; 4],
    /// Commit latency in nanoseconds (p50, p99, p99.9) per durability level.
    pub commit_latency_nanos: [[u64; 3]; 4],
    /// Flushes completed.
    pub flushes: u64,
    /// Compactions completed.
    pub compactions: u64,
    /// Flushes that failed (retried on a backoff; the WAL keeps their data).
    pub flush_failures: u64,
    /// Compactions that failed or could not start (their slot backs off, then retries).
    pub compaction_failures: u64,
    /// Write stalls (token-bucket waits and refused commits) and total stalled nanoseconds.
    pub stalls: (u64, u64),
    /// Block cache hits and misses (the cache does not count them yet: always zero).
    pub block_cache: (u64, u64),
    /// Passes that flushed slots pinning a WAL checkpoint (a shard's own, past
    /// `EngineOptions::wal_pin_bytes`, or at another shard's request), and the memtables
    /// they froze below the size threshold (#137). Flushes per pass is the cost of the
    /// bound: many small L0 SSTs per pass mean cold slots spread over many shards.
    pub unpin: (u64, u64),
    /// WAL rollovers that synced the full segment on a shard thread because no spare slot
    /// was ready (decision D30's exception, #19).
    pub wal_inline_syncs: u64,
    /// Times a shard held a group back because its WAL stream's segment was nearly full
    /// while that segment's header still waited for the previous segment's sync (#19).
    /// Expected to stay near 0.
    pub wal_rollover_blocks: u64,
    /// File growths and the nanoseconds they held the page allocator across a `fallocate`
    /// and a `sync_all` (#28, #182).
    pub file_growths: (u64, u64),
    /// File reads made synchronously inside async reads: a separated value too large to
    /// cache (D196, #398), a block the cache could not keep, or a read that missed too
    /// often. Zero when async reads never block their executor thread.
    pub async_sync_reads: u64,
}

/// One shard's share of the work, for benchmarks that check writes spread over shards
/// (issue #51). Counters are cumulative since open; take two and subtract for a phase.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ShardStats {
    /// Commits the shard applied, summed over durability levels.
    pub commits: u64,
    /// Tablets the shard owns in the current view, over every table.
    pub tablets: u64,
    /// Splits, merges and moves the shard completed.
    pub splits: u64,
    /// See [`ShardStats::splits`].
    pub merges: u64,
    /// See [`ShardStats::splits`].
    pub moves: u64,
}

/// The lock-page byte range of the superblock-less file check (decision D59).
const INTERRUPTED_CREATE_MAX: u64 = 64 * 1024;

/// A reader process's attachment to the writer's shared memory.
struct ReaderState {
    file: FileRef,
    presence: Mutex<Option<Presence>>,
    shm: Mutex<Attachment>,
    /// `(manifest version, catalog, the file the manifest was read from)` as last loaded.
    catalog: Mutex<(ManifestVersion, Arc<Catalog>, FileRef)>,
    /// The last view built from the region, by view version.
    view: Mutex<Option<Arc<View>>>,
    shards: usize,
    registry: Arc<MergeRegistry>,
    #[cfg(feature = "test-hooks")]
    hooks: hooks::ReaderHooks,
}

/// A reader process's attachment to one region generation.
struct Attachment {
    shm: ShmRegion,
    slot: ReaderSlot,
    /// Whether the slot holds a pin.
    pinned: bool,
    /// Snapshots taken in this generation still alive; the pin moves forward when it drops
    /// to zero.
    live: Arc<AtomicUsize>,
}

/// Everything behind an [`Engine`] (and the handle a [`Txn`] keeps).
pub(crate) struct Inner {
    pub(crate) shared: Arc<Shared>,
    role: Role,
    /// Refuses writes and catalog changes with `ReadOnly`: a reader process, or a writer
    /// opened with `allow_unregistered_merge` over a family whose operator is unknown.
    read_only: bool,
    options: EngineOptions,
    path: PathBuf,
    runtime: Mutex<Option<Runtime<ShardState>>>,
    application_owned: bool,
    closing: AtomicBool,
    reader: Option<ReaderState>,
    /// Longest stored value a WAL record and a memtable entry carry (decision D16): a
    /// longer put is separated into a blob file when it is routed (#230). Atomic so a test
    /// can shrink it (`Engine::set_inline_value_limit`).
    max_value: AtomicUsize,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("role", &self.role)
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

/// Unpinned tries of `Engine::get_latest` before it takes a pinned snapshot.
const GET_LATEST_TRIES: u32 = 4;

/// An open database. `Send + Sync`; share it with `Arc`.
///
/// ```
/// use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ReadSpec, ValueRef, WriteBatch};
/// use pigeonhole_io::sim::SimVfs;
///
/// # fn main() -> pigeonhole_engine::Result<()> {
/// let mut options = EngineOptions::new(SimVfs::new(1));
/// options.create_if_missing = true;
/// options.shards = 2;
/// options.memtable_budget = 4 << 20;
/// let db = Engine::open("/db/data.phdb".as_ref(), options)?;
///
/// let pages = db.create_table("pages", &[("meta".into(), FamilyOptions::default())])?;
/// let meta = pages.family("meta").unwrap().id;
/// let mut wb = WriteBatch::new();
/// wb.put(pages.id, meta, b"com.example/a", b"status", None, ValueRef::Bytes(b"200"))?;
/// let info = db.commit(wb, None)?;
///
/// let snap = db.snapshot()?;
/// assert!(snap.seqno() >= info.seqno);
/// let cell = db.get(&snap, pages.id, meta, b"com.example/a", b"status")?.unwrap();
/// assert_eq!(cell.value(), ValueRef::Bytes(b"200"));
/// let row = db.read_row(&snap, pages.id, b"com.example/a", &ReadSpec::default())?.unwrap();
/// assert_eq!(row.cells.len(), 1);
/// // Flush to an SST; the same reads now come from the page file.
/// db.flush()?;
/// let snap = db.snapshot()?;
/// let cell = db.get(&snap, pages.id, meta, b"com.example/a", b"status")?.unwrap();
/// assert_eq!(cell.value(), ValueRef::Bytes(b"200"));
/// db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Engine {
    inner: Arc<Inner>,
}

/// Which embedding mode an open uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    EngineOwned,
    ApplicationOwned,
}

/// The seqno ceiling the shared-memory counter starts from after recovery.
fn first_seqno(ceiling: Seqno, max_replayed: Seqno) -> Seqno {
    ceiling.max(max_replayed + 1).max(1)
}

/// Most `(tablet, family)` slots an arena of `arena_len` bytes in `chunk` chunks serves: a
/// quarter of its chunks (every slot written to takes a chunk, usually two before it
/// freezes, and frozen memtables keep theirs until flushed).
pub(crate) fn max_slots(arena_len: usize, chunk: usize) -> usize {
    arena_len / chunk.max(1) / 4
}

/// The arena chunk size (D136, #104, #283): at least 256 chunks per arena, so every shard
/// serves 64 slots whatever the budget, and smaller still when the tablets placed at open
/// need more (a reopen with fewer shards after splits, or many tables and families with
/// tablet changes off). Never below 1 KiB.
fn arena_chunk_size(arena_len: usize, placed_slots: usize) -> usize {
    let mut chunk = (arena_len / 256).clamp(1024, ShardArena::DEFAULT_CHUNK);
    if max_slots(arena_len, chunk) < placed_slots {
        chunk = arena_len / (4 * placed_slots);
    }
    chunk.max(1024) & !63
}

fn block_cache(bytes: usize, shards: usize) -> Arc<BlockCache> {
    Arc::new(if bytes == 0 {
        BlockCache::disabled()
    } else {
        BlockCache::new(bytes, shards.clamp(1, 64))
    })
}

/// The `(tablet, family)` slots one record writes on one shard.
type Slots = Vec<(TabletId, FamilyId)>;

/// A replayed record of one stream, kept until every stream is read and the cross-shard
/// decisions are resolved.
struct ReplayedRecord {
    end: Lsn,
    seqno: Seqno,
    kind: ReplayedRecordKind,
}

enum ReplayedRecordKind {
    Single { slots: Vec<(TabletId, FamilyId)> },
    Prepare { coordinator: StreamId },
    Commit { participants: Vec<StreamId> },
}

/// The rounds of a full compaction with tablet changes on (issue #94). A tablet that moves
/// during a round can leave a shard before its round reached it and reach one whose round
/// is over, so a round during which a tablet change finished is followed by another; the
/// balancer starts no change while one runs (`Shared::full_compactions`), so the rounds end
/// once the changes in flight at the start have finished.
#[derive(Debug)]
struct CompactRounds {
    shared: Arc<Shared>,
    table: Option<TableId>,
    /// `Shared::tablet_epoch` when the current round was sent.
    epoch: u64,
}

impl CompactRounds {
    fn new(shared: &Arc<Shared>, table: Option<TableId>) -> Option<Self> {
        if !shared.balance.enabled {
            return None;
        }
        shared.full_compactions.fetch_add(1, Ordering::AcqRel);
        Some(Self {
            shared: Arc::clone(shared),
            table,
            epoch: shared.tablet_epoch.load(Ordering::Acquire),
        })
    }

    /// The next round's replies, if a tablet change finished during the last one.
    fn again(&mut self) -> Option<Result<Vec<Waiter<Result<()>>>>> {
        let epoch = self.shared.tablet_epoch.load(Ordering::Acquire);
        if epoch == self.epoch {
            return None;
        }
        self.epoch = epoch;
        Some(compact_round(&self.shared, self.table))
    }
}

impl Drop for CompactRounds {
    fn drop(&mut self) {
        self.shared.full_compactions.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Asks every shard to compact its slots (of `table`) into the last level.
fn compact_round(shared: &Shared, table: Option<TableId>) -> Result<Vec<Waiter<Result<()>>>> {
    let mut waiters = Vec::with_capacity(shared.shards);
    for i in 0..shared.shards {
        let (tx, rx) = completion();
        shared
            .submitter(ShardId(i as u16))
            .submit(ShardMsg::CompactAll { table, reply: tx })?;
        waiters.push(rx);
    }
    Ok(waiters)
}

impl Engine {
    /// Opens (or creates) the database as the writer: takes the writer lock, reads the
    /// superblock and manifest, sets up shared memory, replays every WAL stream, resolves
    /// prepared cross-shard commits, and starts the shards (in engine-owned mode).
    pub fn open(path: &Path, options: EngineOptions) -> Result<Arc<Engine>> {
        let (engine, _) = Self::open_writer(path, options, Mode::EngineOwned)?;
        Ok(engine)
    }

    /// Opens in application-owned mode: as [`Engine::open`], but returns one [`EngineShard`]
    /// per shard for the application to drive instead of starting threads. Starts no
    /// shard or compaction threads (the Vfs may still run its own I/O threads; the default
    /// `PreadVfs` runs 2-16), so a nonzero `options.compaction_threads` fails with
    /// `InvalidArgument` before anything is opened (decision D40).
    ///
    /// After [`Engine::close`], keep driving each shard until [`EngineShard::closed`]
    /// returns `Some`, then drop it: the shards finish their in-flight work, flush,
    /// checkpoint and sync their streams as part of the close, and `run_once` returns
    /// `false` while that I/O is in flight (the wakeup fires when it completes).
    ///
    /// A thread that drives a shard (it last ran [`EngineShard::run_once`]) must not block
    /// on shard work, and the engine refuses rather than deadlock (decision D88): `commit`,
    /// `check_and_mutate`, `Txn::commit`, `flush` and `compact` fail there with
    /// [`Error::InvalidArgument`] before submitting anything, and [`PendingCommit::wait`]
    /// on an already-submitted commit fails with [`Error::WouldDeadlock`] (the commit will
    /// apply; await its future from the event loop). A thread holding a shard it has never
    /// run is not detected: run the shard before committing from that thread. Catalog
    /// changes (`create_table`, `add_family`, `drop_table`) and `shrink` only wait for the
    /// manifest writer and may be called from any thread.
    pub fn open_application_owned(
        path: &Path,
        options: EngineOptions,
    ) -> Result<(Arc<Engine>, Vec<EngineShard>)> {
        if options.compaction_threads != 0 {
            return Err(Error::InvalidArgument(
                "compaction_cores must be 0 in application-owned mode (decision D40)".to_owned(),
            ));
        }
        let (engine, shards) = Self::open_writer(path, options, Mode::ApplicationOwned)?;
        Ok((engine, shards.unwrap_or_default()))
    }

    fn open_writer(
        path: &Path,
        options: EngineOptions,
        mode: Mode,
    ) -> Result<(Arc<Engine>, Option<Vec<EngineShard>>)> {
        let shards = if options.shards == 0 {
            pigeonhole_io::sys::available_cpus().max(1)
        } else {
            options.shards
        };
        if shards > 1 << 16 {
            return Err(Error::InvalidArgument("at most 65536 shards".to_owned()));
        }
        if options.memtable_budget < 2 * ShardArena::DEFAULT_CHUNK as u64 / 4 {
            return Err(Error::InvalidArgument(
                "memtable_budget must be at least 128 KiB".to_owned(),
            ));
        }
        let vfs = Arc::clone(&options.vfs);
        let registry = Arc::new(options.merge_operators.clone());

        // Create first, then open through the normal path (the writer lock comes first).
        if !vfs.exists(path)? {
            if !options.create_if_missing {
                return Err(Error::Io(pigeonhole_io::Error::new(
                    ErrorKind::NotFound,
                    "open database",
                )));
            }
            match Pager::create(&vfs, path) {
                Ok(pager) => drop(pager),
                Err(pigeonhole_pager::Error::Io(e)) if e.kind == ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e.into()),
            }
        }

        // 1. The lock handle: the local-filesystem check, then the writer byte. The check
        //    comes first (#147): it only reads (`fstatfs`), and on a network filesystem
        //    without lock support taking the lock fails with a bare I/O error instead.
        let mut open_opts = OpenOptions::read();
        open_opts.write = true;
        let file = vfs.open(path, open_opts)?;
        check_local(&file, options.allow_fuse)?;
        let writer_lock = WriterLock::acquire(&file)?;
        let identity = file.identity()?;

        // 2. The page file and the manifest.
        let opened = match Pager::open(&vfs, path, true) {
            Ok(o) => o,
            Err(pigeonhole_pager::Error::Format(e)) => {
                return Err(interrupted_create(path, &file, e)?);
            }
            Err(e) => return Err(e.into()),
        };
        let db_id = opened.db_id();
        // The clean flag is informational only: WAL replay is never skipped (decision D57).
        let _clean = opened.clean_shutdown();
        let (mut catalog, manifest_extents) =
            manifest::load(&opened, shards, Arc::clone(&registry))?;
        // With tablet changes on, owners are placed within the arenas' slot budgets below.
        catalog.reassign(shards);
        let mut live = manifest_extents;
        live.extend(catalog.data_extents());
        let pager = Arc::new(opened.finish(live)?);
        open_data_file(&vfs, path, &pager, options.direct_io, true)?;
        if catalog.has_unknown_merge && !options.allow_unregistered_merge {
            let name = catalog
                .tables()
                .flat_map(|t| t.families.iter())
                .map(|f| f.options.merge_operator.clone())
                .find(|n| !n.is_empty() && registry.get(n).is_none())
                .unwrap_or_default();
            return Err(Error::UnknownMergeOperator(name));
        }
        // Opened only because unregistered operators are allowed: read-only (#43).
        let read_only = catalog.has_unknown_merge;

        // 3. Shared memory under a new generation, then presence (D37).
        let mut shm_config = ShmConfig::new(shards as u32);
        shm_config.arena_bytes = options.memtable_budget;
        shm_config.reader_slots = options.reader_slots.max(1);
        shm_config.dir = options.shm_dir.clone();
        shm_config.first_seqno = first_seqno(catalog.counters.seqno_ceiling, 0);
        let shm = ShmRegion::open(&vfs, &file, identity, db_id, ShmRole::Writer, &shm_config)?;
        let presence = Presence::acquire(&file)?;

        // 4. Shard states over the arenas. With tablet changes on, every tablet is placed so
        //    no shard holds more slots than its arena serves where the tablets allow, and the
        //    chunk size shrinks when they do not (#104). Off, tablets stay where `shard_for`
        //    puts them, and the chunks are sized the same way for the slots that gives (#283).
        let arena_len = shm.arena(0).2;
        let chunk = if options.tablet_changes {
            let base = arena_chunk_size(arena_len, 0);
            let placed = catalog.place(shards, max_slots(arena_len, base));
            arena_chunk_size(arena_len, placed)
        } else {
            arena_chunk_size(arena_len, catalog.max_slots_per_shard(shards))
        };
        let chunk = options.arena_chunk_bytes.unwrap_or(chunk);
        let tablets = Arc::new(TabletMap::build(1, &catalog.tablets()));
        let manifest_version = pager.root().manifest_version;
        let cache = block_cache(options.block_cache_bytes, shards);
        let live_views = Arc::new(LiveViews::default());
        // A memtable's allocation grows a chunk at a time: a threshold at or below one chunk
        // would freeze it at its first insert.
        let freeze_bytes = options.memtable_freeze_bytes.max(2 * chunk as u64);
        let empty_view = Arc::new(View {
            version: 0,
            manifest_version,
            tablets: Arc::new(TabletMap::default()),
            catalog: Arc::new(Catalog::with_registry(Arc::clone(&registry))),
            mems: (0..shards)
                .map(|_| Arc::new(ShardMems::default()))
                .collect(),
            ssts: Arc::new(SstSet::empty(pager.data_file().clone(), Arc::clone(&cache))),
            _pin: None,
        });
        let shared = Arc::new(Shared {
            vfs: Arc::clone(&vfs),
            shm: shm.clone(),
            shards,
            view: ArcSwap::new(empty_view),
            async_sync_reads: AtomicU64::new(0),
            view_lock: Mutex::new(0),
            manifest: Mutex::new(ManifestWriter::new(Arc::clone(&pager))),
            manifest_queue: Default::default(),
            manifest_busy: AtomicBool::new(false),
            pager: Arc::clone(&pager),
            cache: Arc::clone(&cache),
            row_cache: crate::row_cache::RowCaches::new(
                options.row_cache_bytes,
                options.row_cache_max_row,
                options.row_cache_families.clone(),
            ),
            sst_ids: Arc::new(AtomicU64::new(catalog.counters.next_sst.max(1))),
            blob_ids: Arc::new(AtomicU32::new(catalog.counters.next_blob_file.max(1))),
            live_views: Arc::clone(&live_views),
            live_seqnos: Arc::new(LiveSeqnos::default()),
            flushed_roots: Mutex::new(HashSet::new()),
            busy_ssts: Mutex::new(HashSet::new()),
            large_pending: Mutex::new(HashSet::new()),
            large_open: AtomicUsize::new(0),
            large_logged: Mutex::new(HashSet::new()),
            large_dropped: Mutex::new(HashSet::new()),
            view_versions: Mutex::new(BTreeMap::new()),
            #[cfg(feature = "test-hooks")]
            hooks: Default::default(),
            picker: options.compaction.clone(),
            write_stall_timeout_nanos: options.write_stall_timeout_nanos,
            compaction_backoff_nanos: options.compaction_backoff_nanos.max(1),
            flush_backoff_nanos: options.flush_backoff_nanos.max(1),
            room_recheck_nanos: options.room_recheck_nanos.max(1),
            commit_spin_nanos: options.commit_spin_nanos,
            tail_index: options.memtable_tail_index,
            locks: Mutex::new(Some(Locks {
                _writer: writer_lock,
                presence,
            })),
            default_durability: AtomicU8::new(options.durability as u8),
            closed: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            maintenance: AtomicUsize::new(0),
            pager_poisoned: AtomicBool::new(false),
            close: CloseState {
                remaining: AtomicUsize::new(shards),
                done: Mutex::new(None),
                failed: AtomicBool::new(false),
                final_pending: AtomicBool::new(false),
                outcome: Mutex::new(None),
            },
            metrics: (0..shards).map(|_| ShardMetrics::default()).collect(),
            ts_floors: (0..shards)
                .map(|_| Padded(AtomicU64::new(catalog.counters.ts_floor)))
                .collect(),
            ts_raises: (0..shards).map(|_| Padded(AtomicU64::new(0))).collect(),
            ts_raisers: (0..shards).map(|_| Mutex::new((0, 0))).collect(),
            loads: (0..shards).map(|_| LoadSlot::default()).collect(),
            balance: BalanceConfig {
                enabled: options.tablet_changes,
                interval_nanos: options.balance_interval_nanos,
                min_writes: options.balance_min_writes,
                skew: options.balance_skew,
                split_bytes: options.tablet_split_bytes.max(1),
            },
            view_capacity: shm_config.view_buffer_bytes as usize,
            tablet_epoch: AtomicU64::new(0),
            full_compactions: AtomicUsize::new(0),
            waiters: VisibilityWaiters::default(),
            shard_died: AtomicBool::new(false),
            manifest_flight: Default::default(),
            drivers: if mode == Mode::ApplicationOwned {
                crate::waker::Drivers::new(shards)
            } else {
                Default::default()
            },
            freeze_waiters: FreezeWaiters::default(),
            memtable_freeze_bytes: freeze_bytes,
            wal_pin_bytes: match options.wal_pin_bytes {
                0 => options.memtable_budget.saturating_mul(2),
                n => n,
            },
            submitters: std::sync::OnceLock::new(),
            shm_dir: options.shm_dir.clone(),
            identity,
            path: path.to_path_buf(),
        });
        // Extents the oldest live in-process view alone kept retired are reclaimed as soon
        // as it goes, not at the next manifest commit (a shard's momentary view would
        // otherwise leave them retired on an idle database). Never blocking: it runs on the
        // thread dropping the view. A reader process's unpin still waits for the next
        // commit. Weak: the registry lives in `Shared`.
        let weak = Arc::downgrade(&shared);
        live_views.on_oldest_released(Box::new(move || {
            if let Some(shared) = weak.upgrade() {
                shared.try_reclaim();
            }
        }));
        let mut states: Vec<ShardState> = Vec::with_capacity(shards);
        let flushed: HashMap<(TabletId, FamilyId), Seqno> =
            catalog.flushed.iter().map(|(k, v)| (*k, *v)).collect();
        for i in 0..shards {
            let (region, offset, len) = shm.arena(i as u32);
            let arena = ArenaRegion::new(region, offset, len)?;
            let mut state = ShardState::new(
                ShardId(i as u16),
                Arc::clone(&shared),
                arena,
                chunk,
                Arc::clone(&tablets),
                catalog.counters.ts_floor,
            );
            let stream = StreamId(i as u32);
            state.set_recovery_state(
                flushed.clone(),
                catalog
                    .checkpoints
                    .get(&stream)
                    .copied()
                    .unwrap_or_default(),
                None,
            );
            states.push(state);
        }

        // 5. Replay every WAL stream (whatever the shard count was), then resolve PREPAREs.
        let mut max_seqno = 0;
        let mut recoveries: Vec<(StreamId, Recovery)> = Vec::new();
        let mut stashed: Vec<(StreamId, Seqno, u64, StreamId, Vec<u8>)> = Vec::new();
        // `(coordinator stream, seqno) -> participant streams` of every COMMIT decision.
        let mut commits: HashMap<(StreamId, Seqno), Vec<StreamId>> = HashMap::new();
        let mut replayed: Vec<(StreamId, Vec<ReplayedRecord>)> = Vec::new();
        // Recovered memtables written to SSTs mid-replay when an arena ran short (#143).
        let mut spill = Spilled::new(Arc::clone(&pager));
        let streams = discover_streams(&vfs, path)?;
        for &stream in &streams {
            let checkpoint = catalog
                .checkpoints
                .get(&stream)
                .copied()
                .unwrap_or_default();
            let mut rec = Recovery::open(&vfs, path, stream, db_id, checkpoint)?;
            let mut records = Vec::new();
            while let Some((end, record)) = rec.next_record()? {
                match record {
                    WalRecord::Batch {
                        seqno,
                        commit_ts,
                        batch,
                    } => {
                        make_room(&shared, &catalog, &mut states, batch.as_bytes(), &mut spill)?;
                        let mut slots = Vec::new();
                        for s in &mut states {
                            slots.extend(
                                s.replay(batch.as_bytes(), seqno, commit_ts)
                                    .map_err(replay_error)?,
                            );
                        }
                        records.push(ReplayedRecord {
                            end,
                            seqno,
                            kind: ReplayedRecordKind::Single { slots },
                        });
                    }
                    WalRecord::Prepare {
                        seqno,
                        commit_ts,
                        coordinator,
                        batch,
                    } => {
                        stashed.push((
                            stream,
                            seqno,
                            commit_ts,
                            coordinator,
                            batch.as_bytes().to_vec(),
                        ));
                        records.push(ReplayedRecord {
                            end,
                            seqno,
                            kind: ReplayedRecordKind::Prepare { coordinator },
                        });
                    }
                    WalRecord::Commit {
                        seqno,
                        participants,
                    } => {
                        let list: Vec<StreamId> = participants.iter().collect();
                        commits.insert((stream, seqno), list.clone());
                        records.push(ReplayedRecord {
                            end,
                            seqno,
                            kind: ReplayedRecordKind::Commit { participants: list },
                        });
                    }
                }
            }
            max_seqno = max_seqno.max(rec.max_seqno());
            recoveries.push((stream, rec));
            replayed.push((stream, records));
        }
        // A decided commit is applied only if every participant its COMMIT names still
        // holds its PREPARE: all or nothing. A `GroupSync`/`Sync` commit's prepares were
        // durable before the COMMIT was written, so this never discards one; a `Buffered`
        // commit whose prepare a power loss took is dropped whole rather than in part.
        let prepared: HashSet<(StreamId, Seqno)> = stashed
            .iter()
            .map(|(stream, seqno, ..)| (*stream, *seqno))
            .collect();
        let complete = |coordinator: StreamId, seqno: Seqno| -> bool {
            commits
                .get(&(coordinator, seqno))
                .is_some_and(|ps| ps.iter().all(|p| prepared.contains(&(*p, seqno))))
        };
        // `(participant stream, seqno) -> slots written on each shard` of applied prepares.
        let mut applied_slots: HashMap<(StreamId, Seqno), Vec<Slots>> = HashMap::new();
        for (stream, seqno, commit_ts, coordinator, bytes) in &stashed {
            crate::shard::trace!(
                "replay prepare stream {} seqno {seqno} coordinator {} complete={} decision={:?}",
                stream.0,
                coordinator.0,
                complete(*coordinator, *seqno),
                commits.get(&(*coordinator, *seqno))
            );
            if !complete(*coordinator, *seqno) {
                continue;
            }
            make_room(&shared, &catalog, &mut states, bytes, &mut spill)?;
            let mut per_shard = Vec::with_capacity(states.len());
            for s in &mut states {
                per_shard.push(s.replay(bytes, *seqno, *commit_ts).map_err(replay_error)?);
            }
            applied_slots.insert((*stream, *seqno), per_shard);
        }
        let next = first_seqno(catalog.counters.seqno_ceiling, max_seqno);
        let current = shm.next_seqno();
        if next > current {
            shm.reserve_seqnos(next - current);
        }

        // 6. Streams. With the same layout as before, every stream's unflushed records are
        // logged on its shard for checkpointing. Otherwise (D20), or when replay already
        // spilled recovered memtables to SSTs, everything recovered is flushed now, every
        // stream checkpointed to its end, and the extra streams removed.
        let same_layout = !spill.spilled
            && streams.len() == shards
            && streams.iter().enumerate().all(|(i, s)| s.0 as usize == i);
        let mut have_wal = vec![false; shards];
        if same_layout {
            for ((stream, records), (_, rec)) in replayed.into_iter().zip(&recoveries) {
                let i = stream.0 as usize;
                let state = &mut states[i];
                let end = rec.end();
                state.set_recovery_state(
                    flushed.clone(),
                    catalog
                        .checkpoints
                        .get(&stream)
                        .copied()
                        .unwrap_or_default(),
                    Some(end),
                );
                for r in records {
                    let kind = match r.kind {
                        ReplayedRecordKind::Single { slots } => ReplayedKind::Single { slots },
                        ReplayedRecordKind::Prepare { coordinator } => {
                            // With tablet changes on, every shard's slots: tablets owned
                            // elsewhere since a move are flushed by their owner, and the
                            // checkpoint waits for that. Off, only this shard's.
                            let applied = applied_slots.get(&(stream, r.seqno));
                            let slots = if options.tablet_changes {
                                applied.map(|per| per.concat()).unwrap_or_default()
                            } else {
                                applied
                                    .map(|per| per.get(i).cloned().unwrap_or_default())
                                    .unwrap_or_default()
                            };
                            ReplayedKind::Prepare {
                                slots,
                                coordinator: ShardId(coordinator.0 as u16),
                                applied: applied.is_some(),
                            }
                        }
                        ReplayedRecordKind::Commit { participants } => ReplayedKind::Commit {
                            participants: participants
                                .iter()
                                .map(|p| ShardId(p.0 as u16))
                                .collect(),
                            complete: complete(stream, r.seqno),
                        },
                    };
                    state.log_replayed(r.end, r.seqno, kind);
                }
            }
            for (stream, rec) in recoveries {
                let i = stream.0 as usize;
                let wal = rec.into_stream(options.wal)?;
                let _ = shared.metrics[i].wal.set(wal.counters());
                states[i].set_wal(Box::new(wal));
                have_wal[i] = true;
            }
        } else {
            flush_recovered(
                &shared,
                &mut catalog,
                &mut states,
                &recoveries,
                shards,
                spill,
            )?;
            let extra: Vec<StreamId> = streams
                .iter()
                .copied()
                .filter(|s| s.0 as usize >= shards)
                .collect();
            for (stream, rec) in recoveries {
                let i = stream.0 as usize;
                if i < shards {
                    let end = rec.end();
                    let mut wal = rec.into_stream(options.wal)?;
                    wal.checkpoint(end)?;
                    let _ = shared.metrics[i].wal.set(wal.counters());
                    states[i].set_wal(Box::new(wal));
                    states[i].set_recovery_state(
                        catalog.flushed.iter().map(|(k, v)| (*k, *v)).collect(),
                        end,
                        Some(end),
                    );
                    have_wal[i] = true;
                }
            }
            for stream in extra {
                vfs.remove(&stream_path(path, stream))?;
            }
            if !streams.is_empty() {
                let dir = match path.parent() {
                    Some(d) if !d.as_os_str().is_empty() => d,
                    _ => Path::new("."),
                };
                vfs.sync_dir(dir)?;
            }
        }
        let missing: Vec<StreamId> = (0..shards)
            .filter(|&i| !have_wal[i])
            .map(|i| StreamId(i as u32))
            .collect();
        for (stream, wal) in missing.iter().zip(WalStream::create_all(
            &vfs,
            path,
            &missing,
            db_id,
            options.wal,
        )?) {
            let _ = shared.metrics[stream.0 as usize].wal.set(wal.counters());
            states[stream.0 as usize].set_wal(Box::new(wal));
        }
        for s in &mut states {
            s.finish_replay();
        }
        sweep_unreferenced_blobs(&shared, &mut catalog, &states, shards)?;

        // The timestamp floor starts above every replayed commit (D11).
        for (i, s) in states.iter().enumerate() {
            shared.ts_floors[i].0.store(s.ts_floor(), Ordering::Release);
        }

        // 7. The first view: tablets, recovered memtables, the manifest version and its SSTs.
        let mems: Vec<Arc<ShardMems>> = states
            .iter()
            .map(|s| {
                Arc::new(ShardMems {
                    map: s.mem_sets().into_iter().collect(),
                })
            })
            .collect();
        let manifest_version = pager.root().manifest_version;
        let catalog = Arc::new(catalog);
        let mut no_readers = HashMap::new();
        let ssts = Arc::new(SstSet::build(
            &catalog,
            None,
            &mut no_readers,
            pager.data_file().clone(),
            Arc::clone(&cache),
        ));
        let first_view = Arc::new(View {
            version: 1,
            manifest_version,
            tablets: Arc::clone(&tablets),
            catalog: Arc::clone(&catalog),
            mems,
            ssts,
            _pin: Some(ViewPin::new(&live_views, manifest_version)),
        });
        shm.publish_view(&first_view.to_record())?;
        shm.set_manifest_version(manifest_version);
        shared
            .view_versions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(1, manifest_version);
        shared.view.store(Arc::clone(&first_view));
        *shared
            .view_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = 1;

        // 8. The runtime.
        let max_value = (options.wal.segment_size as usize)
            .saturating_sub(64 * 1024)
            .min(64 << 20)
            .min((options.memtable_budget / 2) as usize);
        let mut config = RuntimeConfig::new(Arc::clone(&vfs));
        config.shards = shards;
        config.pin_threads = options.pin_threads;
        config.compaction_threads = options.compaction_threads;
        config.idle_spin = std::time::Duration::from_nanos(options.shard_spin_nanos);
        let inner = Arc::new(Inner {
            shared: Arc::clone(&shared),
            role: Role::Writer,
            read_only,
            options,
            path: path.to_path_buf(),
            runtime: Mutex::new(None),
            application_owned: mode == Mode::ApplicationOwned,
            closing: AtomicBool::new(false),
            reader: None,
            max_value: AtomicUsize::new(max_value),
        });
        let engine = Arc::new(Engine {
            inner: Arc::clone(&inner),
        });
        let drivers = match mode {
            Mode::EngineOwned => {
                let rt = Runtime::start(config, states)?;
                let submitters: Vec<Submitter<ShardMsg>> = (0..shards)
                    .map(|i| rt.submitter(ShardId(i as u16)))
                    .collect();
                let _ = shared.submitters.set(submitters);
                *inner.runtime.lock().unwrap_or_else(PoisonError::into_inner) = Some(rt);
                None
            }
            Mode::ApplicationOwned => {
                let drivers = Runtime::application_owned(config, states)?;
                let submitters: Vec<Submitter<ShardMsg>> = (0..shards)
                    .map(|i| drivers[0].submitter(ShardId(i as u16)))
                    .collect();
                let _ = shared.submitters.set(submitters);
                Some(
                    drivers
                        .into_iter()
                        .map(|driver| EngineShard {
                            driver: Some(driver),
                            engine: Arc::clone(&inner),
                        })
                        .collect(),
                )
            }
        };
        for i in 0..shards {
            let _ = shared.submitter(ShardId(i as u16)).submit(ShardMsg::Start);
        }
        Ok((engine, drivers))
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
        let vfs = Arc::clone(&options.vfs);
        let registry = Arc::new(options.merge_operators.clone());
        let mut open_opts = OpenOptions::read();
        open_opts.write = true;
        let file = vfs.open(path, open_opts)?;
        check_local(&file, options.allow_fuse)?;
        let presence = Presence::acquire(&file)?;
        let identity = file.identity()?;
        let opened = match Pager::open(&vfs, path, false) {
            Ok(o) => o,
            Err(pigeonhole_pager::Error::Format(e)) => {
                return Err(interrupted_create(path, &file, e)?);
            }
            Err(e) => return Err(e.into()),
        };
        let db_id = opened.db_id();
        let mut shm_config = ShmConfig::new(1);
        shm_config.dir = options.shm_dir.clone();
        let shm = ShmRegion::open(&vfs, &file, identity, db_id, ShmRole::Reader, &shm_config)?;
        let shards = shm.shard_count() as usize;
        let slot = shm.claim_reader_slot(vfs.current_process())?;
        let (catalog, _) = manifest::load(&opened, shards, Arc::clone(&registry))?;
        let manifest_version = opened.root().manifest_version;
        let manifest_file = opened.file().clone();
        let pager = Arc::new(opened.finish([])?);
        open_data_file(&vfs, path, &pager, options.direct_io, false)?;
        let cache = block_cache(options.block_cache_bytes, shards);
        // The read-only pager is kept by the manifest writer (which never commits here).
        let empty_view = Arc::new(View {
            version: 0,
            manifest_version,
            tablets: Arc::new(TabletMap::default()),
            catalog: Arc::new(Catalog::with_registry(Arc::clone(&registry))),
            mems: (0..shards)
                .map(|_| Arc::new(ShardMems::default()))
                .collect(),
            ssts: Arc::new(SstSet::empty(pager.data_file().clone(), Arc::clone(&cache))),
            _pin: None,
        });
        let shared = Arc::new(Shared {
            vfs: Arc::clone(&vfs),
            shm: shm.clone(),
            shards,
            view: ArcSwap::new(empty_view),
            async_sync_reads: AtomicU64::new(0),
            view_lock: Mutex::new(0),
            manifest: Mutex::new(ManifestWriter::new(Arc::clone(&pager))),
            manifest_queue: Default::default(),
            manifest_busy: AtomicBool::new(false),
            pager: Arc::clone(&pager),
            cache: Arc::clone(&cache),
            // Reader processes have no write watermarks, so no row cache (D201).
            row_cache: None,
            sst_ids: Arc::new(AtomicU64::new(0)),
            blob_ids: Arc::new(AtomicU32::new(0)),
            live_views: Arc::new(LiveViews::default()),
            live_seqnos: Arc::new(LiveSeqnos::default()),
            flushed_roots: Mutex::new(HashSet::new()),
            busy_ssts: Mutex::new(HashSet::new()),
            large_pending: Mutex::new(HashSet::new()),
            large_open: AtomicUsize::new(0),
            large_logged: Mutex::new(HashSet::new()),
            large_dropped: Mutex::new(HashSet::new()),
            view_versions: Mutex::new(BTreeMap::new()),
            #[cfg(feature = "test-hooks")]
            hooks: Default::default(),
            picker: options.compaction.clone(),
            write_stall_timeout_nanos: options.write_stall_timeout_nanos,
            compaction_backoff_nanos: options.compaction_backoff_nanos.max(1),
            flush_backoff_nanos: options.flush_backoff_nanos.max(1),
            room_recheck_nanos: options.room_recheck_nanos.max(1),
            commit_spin_nanos: options.commit_spin_nanos,
            tail_index: false,
            locks: Mutex::new(None),
            default_durability: AtomicU8::new(options.durability as u8),
            closed: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            maintenance: AtomicUsize::new(0),
            pager_poisoned: AtomicBool::new(false),
            close: CloseState::default(),
            metrics: Vec::new(),
            ts_floors: Vec::new(),
            ts_raises: Vec::new(),
            ts_raisers: Vec::new(),
            loads: Vec::new(),
            balance: BalanceConfig::default(),
            view_capacity: shm_config.view_buffer_bytes as usize,
            tablet_epoch: AtomicU64::new(0),
            full_compactions: AtomicUsize::new(0),
            waiters: VisibilityWaiters::default(),
            shard_died: AtomicBool::new(false),
            manifest_flight: Default::default(),
            drivers: Default::default(),
            freeze_waiters: FreezeWaiters::default(),
            memtable_freeze_bytes: options.memtable_freeze_bytes.max(1),
            wal_pin_bytes: 0,
            submitters: std::sync::OnceLock::new(),
            shm_dir: options.shm_dir.clone(),
            identity,
            path: path.to_path_buf(),
        });
        let reader = ReaderState {
            file,
            presence: Mutex::new(Some(presence)),
            shm: Mutex::new(Attachment {
                shm,
                slot,
                pinned: false,
                live: Arc::new(AtomicUsize::new(0)),
            }),
            catalog: Mutex::new((manifest_version, Arc::new(catalog), manifest_file)),
            view: Mutex::new(None),
            shards,
            registry,
            #[cfg(feature = "test-hooks")]
            hooks: Default::default(),
        };
        let inner = Arc::new(Inner {
            shared,
            role: Role::Reader,
            read_only: true,
            options,
            path: path.to_path_buf(),
            runtime: Mutex::new(None),
            application_owned: false,
            closing: AtomicBool::new(false),
            reader: Some(reader),
            max_value: AtomicUsize::new(0),
        });
        Ok(Arc::new(Engine { inner }))
    }

    /// Writer or reader.
    pub fn role(&self) -> Role {
        self.inner.role
    }

    // ---- catalog ----

    /// Creates a table with its families (a manifest commit).
    pub fn create_table(
        &self,
        name: &str,
        families: &[(String, FamilyOptions)],
    ) -> Result<Arc<TableInfo>> {
        self.inner.create_table(name, families)
    }

    /// Adds a family to a table.
    pub fn add_family(
        &self,
        table: TableId,
        name: &str,
        options: FamilyOptions,
    ) -> Result<Arc<TableInfo>> {
        self.inner.add_family(table, name, options)
    }

    /// Looks up a table by name.
    pub fn table(&self, name: &str) -> Option<Arc<TableInfo>> {
        self.inner.catalog().table_by_name(name).cloned()
    }

    /// Every table.
    pub fn tables(&self) -> Vec<Arc<TableInfo>> {
        self.inner.catalog().tables().cloned().collect()
    }

    /// Drops a table and all its data.
    pub fn drop_table(&self, table: TableId) -> Result<()> {
        self.inner.drop_table(table)
    }

    // ---- writes ----

    /// The writer default durability.
    pub fn default_durability(&self) -> Durability {
        self.inner.shared.default_durability()
    }

    /// Changes the writer default; applies to commits that start afterwards.
    pub fn set_default_durability(&self, durability: Durability) {
        self.inner
            .shared
            .default_durability
            .store(durability as u8, Ordering::Relaxed);
    }

    /// Submits a batch: routes each row to its shard (inline if called on the owning shard),
    /// single-shard fast path or two-phase commit. `None` uses the writer default.
    ///
    /// **Order.** A commit submitted after an earlier one was acknowledged is applied after
    /// it. Commits still in flight together have no order between them, even from one
    /// thread: with `EngineOptions::tablet_changes` on, a commit that reached a tablet's old
    /// owner during a move is applied after a later one that reached the new owner directly
    /// (D132). To order two writes to one row, wait for the first before submitting the
    /// second.
    pub fn submit(
        &self,
        batch: WriteBatch,
        durability: Option<Durability>,
    ) -> Result<PendingCommit> {
        self.inner.submit(batch, durability, None, None)
    }

    /// Submits and waits. Returns once the commit meets its durability level **and** is
    /// visible (`visible_seqno >= seqno`), so the caller reads its own write (D19). A
    /// cross-shard commit becomes visible only when every participant has applied, so its
    /// latency includes the slowest participant's group.
    ///
    /// A `Durability::None` commit writes no log record: it is visible at once and lost by
    /// any crash unless a flush persisted it first. The same holds for a commit whose WAL
    /// sync fails after its records were written and applied: the caller gets an `Io`
    /// error, the shard refuses further writes until the database is reopened, and the
    /// data stays visible until then (it may or may not survive the reopen).
    ///
    /// A commit waits (inside the engine) while the memtable arena is full until a flush
    /// frees room, and while L0 is deep for the token bucket's next slot; it fails with
    /// [`Error::Busy`] only when no flush could ever make it fit.
    ///
    /// In application-owned mode, on a thread that drives a shard this fails with
    /// [`Error::InvalidArgument`] before submitting anything, since waiting there could
    /// deadlock (D88): use [`submit`](Engine::submit) and await the future from the event
    /// loop.
    pub fn commit(&self, batch: WriteBatch, durability: Option<Durability>) -> Result<CommitInfo> {
        self.inner.shared.refuse_blocking_on_driver("commit")?;
        self.submit(batch, durability)?.wait()
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
        self.inner
            .shared
            .refuse_blocking_on_driver("check_and_mutate")?;
        self.inner
            .submit_check_and_mutate(table, row, predicate, batch, durability)?
            .wait()
    }

    /// Submits [`check_and_mutate`](Engine::check_and_mutate) without waiting: the
    /// [`PendingCheck`] resolves as `check_and_mutate` returns (an async caller polls it).
    /// It never blocks, so a thread that drives a shard may call it (D88).
    pub fn submit_check_and_mutate(
        &self,
        table: TableId,
        row: &[u8],
        predicate: &Predicate,
        batch: WriteBatch,
        durability: Option<Durability>,
    ) -> Result<PendingCheck> {
        self.inner
            .submit_check_and_mutate(table, row, predicate, batch, durability)
    }

    /// Starts an optimistic transaction (Phase 4).
    pub fn begin(&self) -> Result<Txn> {
        if self.inner.read_only {
            return Err(Error::ReadOnly);
        }
        let snapshot = self.snapshot()?;
        Ok(Txn {
            engine: Arc::clone(&self.inner),
            snapshot,
            reads: Vec::new(),
            batch: WriteBatch::new(),
        })
    }

    // ---- reads ----

    /// A snapshot of everything committed and applied so far. In a reader process this may
    /// do I/O: re-attach after a writer restart (`ShmRegion::is_stale`) and reload the
    /// manifest when its version changed (`Pager::reload_root`).
    pub fn snapshot(&self) -> Result<Snapshot> {
        self.inner.snapshot()
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
        self.inner.get(snapshot, table, family, row, qualifier)
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
        let inner = &self.inner;
        if inner.role == Role::Reader {
            // The caller holds no snapshot, so one that a writer restart expired between
            // taking it and reading is retried with a fresh one (re-attached).
            let mut tries = 0;
            loop {
                let snapshot = inner.snapshot()?;
                match inner.get(&snapshot, table, family, row, qualifier) {
                    Err(Error::SnapshotExpired) if tries < READER_EXPIRED_RETRIES => tries += 1,
                    other => return other,
                }
            }
        }
        // The seqno first, then the view: a seqno is only visible once the view holding its
        // memtables is published. Unpinned, the seqno must still be current after the view
        // loads: then no flush in between had inputs above it, so its GC kept what a read at
        // it sees (#315 review). Otherwise read again; under a steady stream of commits, fall
        // back to a pinned snapshot. (The loop of `Inner::latest_view`, written out: through
        // the helper, a point get measured a few instructions more, #287.)
        for _ in 0..GET_LATEST_TRIES {
            let seqno = inner.shared.shm.visible_seqno();
            #[cfg(feature = "test-hooks")]
            inner.shared.hooks.before_latest_view_load.run();
            let view = inner.shared.view.load();
            if inner.shared.shm.visible_seqno() != seqno {
                continue;
            }
            let now = inner.shared.vfs.now_micros();
            if let Some(rc) = &inner.shared.row_cache
                && let Some(hit) = crate::row_cache::get_cached(
                    rc, &view, seqno, now, table, family, row, qualifier,
                )
            {
                return Ok(hit);
            }
            return get_in::<false>(&view, seqno, now, table, family, row, qualifier, || {
                Arc::clone(&view)
            });
        }
        let snapshot = inner.snapshot()?;
        inner.get(&snapshot, table, family, row, qualifier)
    }

    /// Reads one row, projected by `spec`. `None` if the row has no visible cell.
    pub fn read_row(
        &self,
        snapshot: &Snapshot,
        table: TableId,
        row: &[u8],
        spec: &ReadSpec,
    ) -> Result<Option<RowData>> {
        let families = families_in_order(&snapshot.view, table, &spec.families)?;
        let now = self.inner.shared.vfs.now_micros();
        snapshot.checked(read::read_row(snapshot, table, row, &families, spec, now))
    }

    /// Reads one row, projected by `spec`, into `sink`: the cells go straight into the
    /// caller's buffer, with no intermediate [`RowData`]. Returns whether the row has a
    /// visible cell.
    pub fn read_row_into(
        &self,
        snapshot: &Snapshot,
        table: TableId,
        row: &[u8],
        spec: &ReadSpec,
        sink: &mut impl read::RowSink,
    ) -> Result<bool> {
        self.read_row_into_families(snapshot, table, row, &spec.families, spec, sink)
    }

    /// As [`Engine::read_row_into`], reading `families` (empty: every family of the table)
    /// instead of `spec.families`, so a caller with the ids at hand need not build a `Vec`
    /// for them.
    pub fn read_row_into_families(
        &self,
        snapshot: &Snapshot,
        table: TableId,
        row: &[u8],
        families: &[FamilyId],
        spec: &ReadSpec,
        sink: &mut impl read::RowSink,
    ) -> Result<bool> {
        let families = families_in_order(&snapshot.view, table, families)?;
        let now = self.inner.shared.vfs.now_micros();
        snapshot.checked(read::read_row_into(
            &snapshot.view,
            snapshot.seqno,
            table,
            row,
            &families,
            spec,
            now,
            false,
            sink,
        ))
    }

    /// [`Engine::get_latest`] as a future that never blocks on a block the cache does not
    /// hold: it fetches what it misses through the VFS's asynchronous reads and reads again
    /// (D196, ICR 0014). Memtable and cache hits resolve on the first poll.
    pub fn get_latest_async(
        &self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
    ) -> crate::GetFuture {
        crate::GetFuture::new(Arc::clone(&self.inner), None, table, family, row, qualifier)
    }

    /// [`Engine::get`] (at `snapshot`) as a future, as [`Engine::get_latest_async`].
    pub fn get_async(
        &self,
        snapshot: &Snapshot,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
    ) -> crate::GetFuture {
        crate::GetFuture::new(
            Arc::clone(&self.inner),
            Some(snapshot.clone()),
            table,
            family,
            row,
            qualifier,
        )
    }

    /// [`Engine::read_row_latest_into`] as a future, as [`Engine::get_latest_async`]: it
    /// resolves to `sink` holding the row, or `None` if the row has no visible cell.
    pub fn read_row_latest_async<S: read::RowSink + Clone + Unpin>(
        &self,
        table: TableId,
        row: &[u8],
        families: &[FamilyId],
        spec: &ReadSpec,
        sink: S,
    ) -> crate::RowFuture<S> {
        crate::RowFuture::new(
            Arc::clone(&self.inner),
            None,
            table,
            row,
            families,
            spec,
            sink,
        )
    }

    /// [`Engine::read_row_into_families`] (at `snapshot`) as a future, as
    /// [`Engine::read_row_latest_async`].
    pub fn read_row_async<S: read::RowSink + Clone + Unpin>(
        &self,
        snapshot: &Snapshot,
        table: TableId,
        row: &[u8],
        families: &[FamilyId],
        spec: &ReadSpec,
        sink: S,
    ) -> crate::RowFuture<S> {
        crate::RowFuture::new(
            Arc::clone(&self.inner),
            Some(snapshot.clone()),
            table,
            row,
            families,
            spec,
            sink,
        )
    }

    /// As [`Engine::read_row_into_families`], as of now and without creating a snapshot:
    /// the view is read as [`Engine::get_latest`] reads it (the visible seqno, the view
    /// through an `arc-swap` guard, the seqno again), and a value the sink pins rather than
    /// copies pins only that view.
    pub fn read_row_latest_into(
        &self,
        table: TableId,
        row: &[u8],
        families: &[FamilyId],
        spec: &ReadSpec,
        sink: &mut impl read::RowSink,
    ) -> Result<bool> {
        let inner = &self.inner;
        if inner.role == Role::Reader {
            // As in `get_latest`: a snapshot a writer restart expired is retried.
            let mut tries = 0;
            loop {
                let snapshot = inner.snapshot()?;
                match self.read_row_into_families(&snapshot, table, row, families, spec, sink) {
                    Err(Error::SnapshotExpired) if tries < READER_EXPIRED_RETRIES => tries += 1,
                    other => return other,
                }
            }
        }
        if let Some((view, seqno)) = inner.latest_view() {
            let families = families_in_order(&view, table, families)?;
            let now = inner.shared.vfs.now_micros();
            if let Some(rc) = &inner.shared.row_cache
                && crate::row_cache::serves_spec(spec)
            {
                return crate::row_cache::read_row_cached(
                    rc, &view, seqno, table, row, &families, spec, now, false, sink,
                );
            }
            return read::read_row_into(
                &view, seqno, table, row, &families, spec, now, false, sink,
            );
        }
        let snapshot = inner.snapshot()?;
        self.read_row_into_families(&snapshot, table, row, families, spec, sink)
    }

    /// Starts an ordered scan.
    pub fn scan(&self, snapshot: &Snapshot, table: TableId, spec: ScanSpec) -> Result<ScanCursor> {
        let families = families_in_order(&snapshot.view, table, &spec.read.families)?;
        let now = self.inner.shared.vfs.now_micros();
        Ok(ScanCursor::new(
            snapshot.clone(),
            table,
            spec,
            families.into_vec(),
            now,
        ))
    }

    /// [`Engine::scan`] for an async scan: its rows are polled with
    /// [`ScanCursor::poll_next_row`], which never blocks on a block it could predict (D196).
    pub fn scan_async(
        &self,
        snapshot: &Snapshot,
        table: TableId,
        spec: ScanSpec,
    ) -> Result<ScanCursor> {
        Ok(self
            .scan(snapshot, table, spec)?
            .into_async(Arc::clone(&self.inner.shared)))
    }

    // ---- maintenance ----

    /// Freezes and flushes every memtable; returns when the SSTs are in the manifest. A
    /// memtable the arena has no chunk to replace (snapshots pin the retired ones) waits as
    /// a write stall does, and the flush fails with [`Error::Busy`] when no chunk frees up
    /// in time (D124, D126; issue #116).
    pub fn flush(&self) -> Result<()> {
        self.inner.shared.refuse_blocking_on_driver("flush")?;
        self.inner.flush_pending()?.wait()
    }

    /// Compacts every family of `table` (or all tables) fully: flushes, then merges every
    /// level into the last one.
    pub fn compact(&self, table: Option<TableId>) -> Result<()> {
        self.inner.shared.refuse_blocking_on_driver("compact")?;
        self.inner.compact_pending(table)?.wait()
    }

    /// Submits [`flush`](Engine::flush) without waiting: the [`PendingMaintenance`] resolves
    /// as `flush` returns (an async caller polls it). It never blocks, so a thread that
    /// drives a shard may call it (D88).
    pub fn submit_flush(&self) -> Result<PendingMaintenance> {
        self.inner.flush_pending()
    }

    /// Submits [`compact`](Engine::compact) without waiting, as
    /// [`submit_flush`](Engine::submit_flush). Each further round of a full compaction is
    /// submitted when the future sees the previous one done.
    pub fn submit_compact(&self, table: Option<TableId>) -> Result<PendingMaintenance> {
        self.inner.compact_pending(table)
    }

    /// Writes a consistent single-file copy to `dest` while writers run: everything visible
    /// at a snapshot taken now, as a clean database that opens without WAL replay.
    pub fn backup(&self, dest: &Path) -> Result<()> {
        let inner = &self.inner;
        if inner.role != Role::Writer {
            return Err(Error::ReadOnly);
        }
        let _guard = inner.enter_maintenance()?;
        let snapshot = inner.snapshot()?;
        crate::maintenance::backup(&inner.shared, snapshot, dest)
    }

    /// Truncates the free tail, relocates tail extents the manifest names and truncates the
    /// file again (decision D60). Returns the bytes the file shrank by. An extent with no
    /// free extent of its size below it stays; `NoSpace` means the disk filled while the
    /// manifest was being moved.
    pub fn shrink(&self) -> Result<u64> {
        let inner = &self.inner;
        if inner.role != Role::Writer {
            return Err(Error::ReadOnly);
        }
        let _guard = inner.enter_maintenance()?;
        crate::maintenance::shrink(&inner.shared)
    }

    /// What the row cache did since open (D201); all zero when it is off or in a reader
    /// process. A method rather than [`Metrics`] fields, which would break struct literals.
    pub fn row_cache_stats(&self) -> RowCacheStats {
        self.inner
            .shared
            .row_cache
            .as_ref()
            .map(|rc| rc.stats())
            .unwrap_or_default()
    }

    /// Tablet changes refused for lack of room, summed over shards since open, as
    /// `(slot budget, view size)`: splits and moves a shard's memtable slots could not take
    /// (a size split the balancer found due but skipped counts once per balancer pass), and
    /// splits refused because the tablet map would not fit the shared-memory view buffer
    /// (D28). A rising first count means tablets that should split cannot: raise
    /// `memtable_budget` (#122). A method rather than a [`Metrics`] field, which would
    /// break struct literals.
    pub fn tablet_refusals(&self) -> (u64, u64) {
        self.inner.shared.metrics.iter().fold((0, 0), |(s, v), m| {
            (
                s + m.refused_slots.load(Ordering::Relaxed),
                v + m.refused_view.load(Ordering::Relaxed),
            )
        })
    }

    /// Per-shard commits, tablets and tablet changes, indexed by shard (a bench hook,
    /// issue #51).
    #[doc(hidden)]
    pub fn shard_stats(&self) -> Vec<ShardStats> {
        let shared = &self.inner.shared;
        let mut out: Vec<ShardStats> = shared
            .metrics
            .iter()
            .map(|m| ShardStats {
                commits: m.commits.iter().map(|c| c.load(Ordering::Relaxed)).sum(),
                tablets: 0,
                splits: m.splits.load(Ordering::Relaxed),
                merges: m.merges.load(Ordering::Relaxed),
                moves: m.moves.load(Ordering::Relaxed),
            })
            .collect();
        for t in shared.view.load().tablets().iter() {
            if let Some(s) = out.get_mut(usize::from(t.shard.0)) {
                s.tablets += 1;
            }
        }
        out
    }

    /// Current metrics.
    pub fn metrics(&self) -> Metrics {
        let shared = &self.inner.shared;
        let mut m = Metrics::default();
        for d in 0..4 {
            let mut hist = [0u64; 64];
            for s in &shared.metrics {
                m.commits[d] += s.commits[d].load(Ordering::Relaxed);
                for (b, h) in hist.iter_mut().enumerate() {
                    *h += s.latency[d][b].load(Ordering::Relaxed);
                }
            }
            let total: u64 = hist.iter().sum();
            if total > 0 {
                for (i, q) in [0.5, 0.99, 0.999].into_iter().enumerate() {
                    let target = ((total as f64 * q).ceil() as u64).max(1);
                    let mut seen = 0;
                    for (b, h) in hist.iter().enumerate() {
                        seen += h;
                        if seen >= target {
                            m.commit_latency_nanos[d][i] = bucket_floor(b);
                            break;
                        }
                    }
                }
            }
        }
        for s in &shared.metrics {
            m.flushes += s.flushes.load(Ordering::Relaxed);
            m.compactions += s.compactions.load(Ordering::Relaxed);
            m.flush_failures += s.flush_failures.load(Ordering::Relaxed);
            m.compaction_failures += s.compaction_failures.load(Ordering::Relaxed);
            m.stalls.0 += s.stalls.load(Ordering::Relaxed);
            m.stalls.1 += s.stall_nanos.load(Ordering::Relaxed);
            m.unpin.0 += s.unpin_passes.load(Ordering::Relaxed);
            m.unpin.1 += s.unpin_flushes.load(Ordering::Relaxed);
            m.wal_inline_syncs += s.wal.get().map_or(0, |c| c.inline_rollover_syncs());
            m.wal_rollover_blocks += s.wal.get().map_or(0, |c| c.rollover_blocks());
        }
        let pager = shared.pager.stats();
        m.file_growths = (pager.growths, pager.growth_nanos);
        m.async_sync_reads = shared.async_sync_reads.load(Ordering::Relaxed);
        m
    }

    /// Stops the shards: every shard finishes its in-flight groups and cross-shard commits,
    /// flushes its memtables, checkpoints and syncs its stream; the last one records the
    /// clean close and, if no reader is attached, removes the WAL files and the
    /// shared-memory region, leaving one file at rest.
    ///
    /// In engine-owned mode this waits for the shards and returns the final result (a shard
    /// thread that panicked is reported as an error).
    ///
    /// In application-owned mode the application keeps driving every shard as usual until
    /// [`EngineShard::closed`] returns `Some`, then drops it. Called on a thread that drives
    /// no shard, once every shard has been run, `close` waits for that and returns the final
    /// result.
    ///
    /// Called on a thread that drives a shard, or before every shard has been run at least
    /// once, `close` cannot wait: it returns `Ok(())` as soon as it has told the shards to
    /// close. That `Ok` says nothing about the outcome: a failed or unclean close is then
    /// reported **only** by [`EngineShard::closed`].
    pub fn close(&self) -> Result<()> {
        self.inner.close(true)
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if self.inner.role == Role::Writer {
            let _ = self.inner.close(!self.inner.application_owned);
        } else {
            let _ = self.inner.close_reader();
        }
    }
}

/// Flushes every recovered memtable synchronously at open (the shard count changed, so the
/// streams' records no longer map to shards; decision D20): one SST per non-empty memtable,
/// `SetFlushed` per slot, every stream checkpointed to its end (extra streams to zero, since
/// they are removed afterwards).
/// Recovered memtables already written to SSTs at open, committed with the rest by
/// `flush_recovered` (#143). Until that commit succeeds the SSTs' extents are pending
/// output: dropping this (the open failed anywhere after a spill) gives them back.
struct Spilled {
    pager: Arc<Pager>,
    /// Replay ran short of arena room at least once.
    spilled: bool,
    /// The edits to commit: the `AddSst` of every SST written so far, then (in
    /// `flush_recovered`) the rest of the open-time flush.
    edits: Vec<Edit>,
    /// The largest seqno written to SSTs per slot.
    flushed: BTreeMap<(TabletId, FamilyId), Seqno>,
}

impl Spilled {
    fn new(pager: Arc<Pager>) -> Self {
        Self {
            pager,
            spilled: false,
            edits: Vec::new(),
            flushed: BTreeMap::new(),
        }
    }
}

impl Drop for Spilled {
    fn drop(&mut self) {
        for e in self.edits.drain(..) {
            if let Edit::AddSst { meta, .. } = e {
                self.pager.abandon(meta.extent);
            }
        }
    }
}

/// Before replaying `bytes`: if some shard's arena may not hold its share, writes every
/// recovered memtable to SSTs first. Replay cannot flush the way a running shard does (it
/// may not move a checkpoint past records whose prepares are still being resolved), so it
/// spills everything and finishes as a layout change does (D121): the edits commit with
/// the flush of what is left, which checkpoints every stream to its end. Without this, a
/// database reopened with fewer shards or a smaller budget than it crashed with could not
/// open at all (#143).
fn make_room(
    shared: &Shared,
    catalog: &Catalog,
    states: &mut [ShardState],
    bytes: &[u8],
    spill: &mut Spilled,
) -> Result<()> {
    if states.iter().all(|s| s.replay_fits(bytes)) || !states.iter().any(|s| s.has_recovered()) {
        return Ok(());
    }
    crate::shard::trace!("replay: an arena is short, spilling recovered memtables");
    spill.spilled = true;
    spill_recovered(shared, catalog, states, spill)
}

/// Writes every recovered memtable of `states` to L0 SSTs, adding to `spill`.
fn spill_recovered(
    shared: &Shared,
    catalog: &Catalog,
    states: &mut [ShardState],
    spill: &mut Spilled,
) -> Result<()> {
    let created = shared.vfs.now_micros();
    for state in states.iter_mut() {
        for ((tablet, family), reader, bytes, max_seqno) in state.take_memtables() {
            let Some(meta) = catalog.family(family) else {
                continue;
            };
            let mut opts = SstWriterOptions::for_family(&meta.options, meta.table, family, tablet);
            opts.created_micros = created;
            let mut sink = SstSink::new(
                Arc::clone(&shared.pager),
                Arc::clone(&shared.sst_ids),
                opts,
                bytes,
            );
            if let Err(e) = write_memtable(&mut sink, &reader) {
                sink.abandon();
                return Err(e);
            }
            spill.edits.extend(sink.take_edits(tablet, family, 0));
            let f = spill.flushed.entry((tablet, family)).or_insert(max_seqno);
            *f = (*f).max(max_seqno);
        }
        state.release_taken();
    }
    Ok(())
}

/// After replay (#230): drops every blob file that nothing points into, neither an SST's
/// recorded references (#240), nor an SST of its family without a record, nor a recovered
/// memtable entry. Flushes, compactions, blob GC and backups commit a blob file in the same
/// manifest commit as the SSTs that point into it, so only a value separated at commit time
/// can leave one: its commit never became durable, or was refused and its release did not
/// run before the process ended.
fn sweep_unreferenced_blobs(
    shared: &Shared,
    catalog: &mut Catalog,
    states: &[ShardState],
    shards: usize,
) -> Result<()> {
    if catalog.blob_files.is_empty() {
        return Ok(());
    }
    use pigeonhole_format::superblock::ExtentRef;
    use pigeonhole_format::{BlobFileId, Cursor};

    let mut keep: HashSet<BlobFileId> = HashSet::new();
    let mut unrecorded: HashSet<FamilyId> = HashSet::new();
    for ((_, family), list) in &catalog.ssts {
        for (_, meta) in list {
            match catalog.blob_refs.get(&meta.id) {
                Some(refs) => keep.extend(refs.iter().map(|(id, _)| *id)),
                None => {
                    unrecorded.insert(*family);
                }
            }
        }
    }
    for state in states {
        for (_, set) in state.mem_sets() {
            for reader in &set.readers {
                let mut it = reader.iter();
                it.seek_to_first()?;
                while it.valid() {
                    if let Some(p) = pigeonhole_compaction::blob_pointer(it.value()) {
                        keep.insert(p.blob_file);
                    }
                    it.next()?;
                }
            }
        }
    }
    let dropped: Vec<(BlobFileId, Vec<ExtentRef>)> = catalog
        .blob_files
        .iter()
        .filter(|(id, b)| !keep.contains(id) && !unrecorded.contains(&b.family))
        .map(|(id, b)| (*id, b.extents.clone()))
        .collect();
    if dropped.is_empty() {
        return Ok(());
    }
    crate::shard::trace!(
        "open: dropping blob files nothing points into: {:?}",
        dropped.iter().map(|(id, _)| id.0).collect::<Vec<_>>()
    );
    let edits: Vec<Edit> = dropped
        .iter()
        .map(|(id, _)| Edit::DropBlobFile { blob_file: *id })
        .collect();
    for e in &edits {
        catalog.apply(e, shards)?;
    }
    let version = shared
        .manifest
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .commit(catalog, &edits)?;
    for (_, extents) in dropped {
        for e in extents {
            shared.pager.retire(e, version);
        }
    }
    Ok(())
}

fn flush_recovered(
    shared: &Shared,
    catalog: &mut Catalog,
    states: &mut [ShardState],
    recoveries: &[(StreamId, Recovery)],
    shards: usize,
    mut spill: Spilled,
) -> Result<()> {
    // On any error below, dropping `spill` gives back every SST's extent.
    spill_recovered(shared, catalog, states, &mut spill)?;
    let flushed = std::mem::take(&mut spill.flushed);
    let edits = &mut spill.edits;
    for ((tablet, family), seqno) in flushed {
        edits.push(Edit::SetFlushed {
            tablet,
            family,
            seqno,
        });
    }
    for (stream, rec) in recoveries {
        let lsn = if (stream.0 as usize) < shards {
            rec.end()
        } else {
            Lsn::default()
        };
        edits.push(Edit::WalCheckpoint {
            stream: *stream,
            lsn,
        });
    }
    catalog.counters.next_sst = shared.sst_ids.load(Ordering::Relaxed);
    catalog.counters.seqno_ceiling = shared.shm.next_seqno();
    edits.push(catalog.counters_edit());
    for e in edits.iter() {
        catalog.apply(e, shards)?;
    }
    let mut writer = shared
        .manifest
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    writer.commit(catalog, edits)?;
    // Published: nothing to give back.
    edits.clear();
    Ok(())
}

/// Decision D59: a file without a valid superblock that is at most 64 KiB long looks like an
/// interrupted create. It is never removed or changed.
fn interrupted_create(path: &Path, file: &FileRef, e: pigeonhole_format::Error) -> Result<Error> {
    let len = file.len()?;
    Ok(if len <= INTERRUPTED_CREATE_MAX {
        Error::Corruption(format!(
            "{} has no valid superblock and is only {len} bytes long: it appears to be an \
             interrupted create and holds no data; delete it and open again (decision D59)",
            path.display()
        ))
    } else {
        Error::Corruption(format!("{} has no valid superblock ({e})", path.display()))
    })
}

fn replay_error(e: Error) -> Error {
    match e {
        Error::Busy => Error::InvalidArgument(
            "a WAL record is larger than a shard's memtable arena; reopen with a larger \
             memtable_budget"
                .to_owned(),
        ),
        e => e,
    }
}

/// The families of `table` to read: in creation order, or as listed (duplicates and unknown
/// ids dropped), decision D39.
/// Inline for up to four families, so a row read allocates nothing for them (#287).
pub(crate) fn families_in_order(
    view: &View,
    table: TableId,
    listed: &[FamilyId],
) -> Result<SmallVec<[FamilyId; 4]>> {
    let info = view
        .catalog
        .table(table)
        .ok_or_else(|| Error::TableNotFound(format!("table {}", table.0)))?;
    if listed.is_empty() {
        return Ok(info.families.iter().map(|f| f.id).collect());
    }
    let mut out = SmallVec::with_capacity(listed.len());
    for &f in listed {
        if !out.contains(&f) && info.families.iter().any(|x| x.id == f) {
            out.push(f);
        }
    }
    Ok(out)
}

/// A maintenance operation in flight ([`Engine::submit_flush`], [`Engine::submit_compact`]):
/// the replies of every shard. `flush` and `compact` block on it; an async caller polls it
/// as a `Future`, woken by the shards' replies. It resolves only once every shard replied
/// (to every round of a full compaction), with the last failure if any. Dropping it does
/// not stop the operation.
#[derive(Debug)]
#[must_use = "dropping a pending flush or compaction does not stop it, but its result is lost"]
pub struct PendingMaintenance {
    waiters: Vec<Waiter<Result<()>>>,
    rounds: Option<CompactRounds>,
    /// A failed reply of the current round, reported once every shard has replied (the
    /// future; the blocking `wait` keeps its own).
    pub(crate) failed: Option<Error>,
}

impl PendingMaintenance {
    /// Blocks until every shard replied (to every round of a full compaction).
    pub(crate) fn wait(mut self) -> Result<()> {
        loop {
            let mut result = Ok(());
            for w in std::mem::take(&mut self.waiters) {
                match w.wait().unwrap_or(Err(Error::Closed)) {
                    Ok(()) => {}
                    Err(e) => result = Err(e),
                }
            }
            result?;
            match self.rounds.as_mut().and_then(CompactRounds::again) {
                Some(round) => self.waiters = round?,
                None => return Ok(()),
            }
        }
    }
}

impl std::future::Future for PendingMaintenance {
    type Output = Result<()>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        loop {
            match self.poll_round(cx) {
                std::task::Poll::Ready(Ok(())) => {}
                other => return other,
            }
            let this = &mut *self;
            match this.rounds.as_mut().map(CompactRounds::again) {
                Some(Some(round)) => this.waiters = round?,
                _ => return std::task::Poll::Ready(Ok(())),
            }
        }
    }
}

impl PendingMaintenance {
    /// Polls the current round's replies. Like the blocking `wait` (#148, review 1-2 F10),
    /// it resolves only once every shard has replied, with the last failure if any, so the
    /// caller never sees a result while other shards still work.
    fn poll_round(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<()>> {
        let mut i = 0;
        while i < self.waiters.len() {
            match std::pin::Pin::new(&mut self.waiters[i]).poll(cx) {
                std::task::Poll::Ready(Some(Ok(()))) => {
                    self.waiters.swap_remove(i);
                }
                std::task::Poll::Ready(Some(Err(e))) => {
                    self.waiters.swap_remove(i);
                    self.failed = Some(e);
                }
                std::task::Poll::Ready(None) => {
                    self.waiters.swap_remove(i);
                    self.failed = Some(Error::Closed);
                }
                std::task::Poll::Pending => i += 1,
            }
        }
        if !self.waiters.is_empty() {
            return std::task::Poll::Pending;
        }
        match self.failed.take() {
            Some(e) => std::task::Poll::Ready(Err(e)),
            None => std::task::Poll::Ready(Ok(())),
        }
    }
}

impl Inner {
    /// Enters application-thread maintenance: refused once the engine is closing, and the
    /// final close waits until the guard is dropped (7 F7-4).
    fn enter_maintenance(&self) -> Result<MaintenanceGuard<'_>> {
        MaintenanceGuard::enter(&self.shared, || self.check_open())
    }

    fn check_open(&self) -> Result<()> {
        if self.closing.load(Ordering::Acquire) || self.shared.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if self.shared.pager_poisoned.load(Ordering::Acquire) {
            return Err(ManifestWriter::poisoned_error());
        }
        Ok(())
    }

    /// The current catalog (the one the current view carries).
    fn catalog(&self) -> Arc<Catalog> {
        if let Some(r) = &self.reader {
            // Table metadata only (no view is built from it): the newest version the
            // region names is good enough, and the cached catalog when the root moved on.
            let version = r
                .shm
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .shm
                .manifest_version();
            let _ = self.reader_catalog(version);
            return Arc::clone(&r.catalog.lock().unwrap_or_else(PoisonError::into_inner).1);
        }
        Arc::clone(&self.shared.view.load().catalog)
    }

    /// Runs a catalog change: `f` edits a copy of the catalog (under the manifest writer's
    /// exclusion) and returns the edits, which are committed to the manifest and published
    /// in a new view.
    fn catalog_change(
        &self,
        f: impl FnOnce(&mut Catalog) -> Result<Vec<Edit>> + Send + 'static,
    ) -> Result<Arc<View>> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        let _guard = self.enter_maintenance()?;
        let version = manifest::commit_from_thread(&self.shared, ReqKind::Catalog(Box::new(f)))?;
        let view = self.shared.view.load_full();
        debug_assert!(view.manifest_version >= version);
        Ok(view)
    }

    fn create_table(
        &self,
        name: &str,
        families: &[(String, FamilyOptions)],
    ) -> Result<Arc<TableInfo>> {
        if name.is_empty() {
            return Err(Error::InvalidArgument("empty table name".to_owned()));
        }
        let mut seen = HashSet::new();
        for (fname, options) in families {
            if fname.is_empty() {
                return Err(Error::InvalidArgument("empty family name".to_owned()));
            }
            if !seen.insert(fname.as_str()) {
                return Err(Error::FamilyExists(fname.clone()));
            }
            check_merge_operator(
                options,
                &self.options.merge_operators,
                self.options.allow_unregistered_merge,
            )?;
        }
        let name_owned = name.to_owned();
        let families = families.to_vec();
        let view = self.catalog_change(move |catalog| {
            if catalog.table_by_name(&name_owned).is_some() {
                return Err(Error::TableExists(name_owned.clone()));
            }
            let table = catalog.alloc_table();
            let mut edits = vec![Edit::CreateTable {
                table,
                name: name_owned.clone(),
            }];
            for (fname, options) in &families {
                let family = catalog.alloc_family();
                edits.push(Edit::PutFamily {
                    table,
                    family,
                    name: fname.clone(),
                    options: options.clone(),
                });
            }
            let tablet = catalog.alloc_tablet();
            edits.push(Edit::PutTablet {
                tablet,
                table,
                start: Vec::new(),
                end: None,
            });
            Ok(edits)
        })?;
        view.catalog
            .table_by_name(name)
            .cloned()
            .ok_or_else(|| Error::TableNotFound(name.to_owned()))
    }

    fn add_family(
        &self,
        table: TableId,
        name: &str,
        options: FamilyOptions,
    ) -> Result<Arc<TableInfo>> {
        if name.is_empty() {
            return Err(Error::InvalidArgument("empty family name".to_owned()));
        }
        check_merge_operator(
            &options,
            &self.options.merge_operators,
            self.options.allow_unregistered_merge,
        )?;
        let name_owned = name.to_owned();
        let view = self.catalog_change(move |catalog| {
            let info = catalog
                .table(table)
                .ok_or_else(|| Error::TableNotFound(format!("table {}", table.0)))?;
            if info.family(&name_owned).is_some() {
                return Err(Error::FamilyExists(name_owned.clone()));
            }
            let family = catalog.alloc_family();
            Ok(vec![Edit::PutFamily {
                table,
                family,
                name: name_owned.clone(),
                options,
            }])
        })?;
        view.catalog
            .table(table)
            .cloned()
            .ok_or_else(|| Error::TableNotFound(format!("table {}", table.0)))
    }

    fn drop_table(&self, table: TableId) -> Result<()> {
        let dropped = self.catalog().tablet_ids_of(table);
        self.catalog_change(move |catalog| {
            let Some(info) = catalog.table(table) else {
                return Err(Error::TableNotFound(format!("table {}", table.0)));
            };
            // The table's blob files go with it (their extents are retired at this version).
            let families: Vec<FamilyId> = info.families.iter().map(|f| f.id).collect();
            let mut edits = vec![Edit::DropTable { table }];
            edits.extend(
                catalog
                    .blob_files
                    .iter()
                    .filter(|(_, b)| families.contains(&b.family))
                    .map(|(id, _)| Edit::DropBlobFile { blob_file: *id }),
            );
            Ok(edits)
        })?;
        self.shared.broadcast(|| ShardMsg::DropTablets {
            tablets: dropped.clone(),
        });
        Ok(())
    }

    /// The longest stored value a batch carries inline (D16; longer puts are separated).
    pub(crate) fn inline_limit(&self) -> usize {
        self.max_value.load(Ordering::Relaxed) + 1
    }

    /// Separates the puts of `builder` above the inline limit into blob files (#230). Waits
    /// for a manifest commit when there is one; the waiting thread drives that commit
    /// itself (`manifest::commit_from_thread`, as `shrink` does), so it may drive a shard.
    fn separate_large(
        &self,
        builder: BatchBuilder,
    ) -> Result<(BatchBuilder, Option<crate::large::LargeValues>)> {
        crate::large::separate(&self.shared, builder, self.inline_limit())
    }

    /// Validates and routes a batch: per-shard parts in first-appearance order.
    fn route(&self, mut batch: WriteBatch, view: &View) -> Result<(BatchBuilder, Shards)> {
        let catalog = &view.catalog;
        for rd in std::mem::take(&mut batch.row_deletes) {
            let info = catalog
                .table(rd.table)
                .ok_or_else(|| Error::TableNotFound(format!("table {}", rd.table.0)))?;
            for f in &info.families {
                batch
                    .builder
                    .push(rd.table, f.id, Kind::FamilyDelete, &rd.row, &[], rd.ts, &[])?;
            }
        }
        let mut shards = Shards::new();
        // Whether a counter-family put or operand takes the fixed timestamp (D179), and how
        // many counter puts and operands there are (two may combine, #295).
        let mut fixed_ts = false;
        let mut counter_writes = 0usize;
        // The shard of the last row routed: a batch's writes to one row route once.
        let mut last: Option<(TableId, &[u8], ShardId)> = None;
        for m in batch.batch().iter() {
            let m = m?;
            let Some(meta) = catalog.family(m.family) else {
                return Err(Error::FamilyNotFound(format!("family {}", m.family.0)));
            };
            if meta.table != m.table {
                return Err(Error::FamilyNotFound(format!(
                    "family {} is not in table {}",
                    m.family.0, m.table.0
                )));
            }
            if meta.options.kind == FamilyKind::Counter {
                fixed_ts |= check_counter_write(&m, meta, catalog)?;
                counter_writes += usize::from(matches!(m.kind, Kind::Put | Kind::Merge));
            } else if m.kind == Kind::Merge && m.ts.is_some() {
                return Err(Error::InvalidArgument(format!(
                    "{} is not a counter family: only a counter family takes increments at a \
                     chosen timestamp",
                    family_name(catalog, m.table, m.family)
                )));
            }
            if m.kind == Kind::Merge {
                match meta.merge {
                    MergeKind::None => {
                        return Err(Error::InvalidArgument(format!(
                            "{} has no merge operator: increments need a counter family",
                            family_name(catalog, m.table, m.family)
                        )));
                    }
                    MergeKind::Unknown => {
                        return Err(Error::UnknownMergeOperator(
                            meta.options.merge_operator.clone(),
                        ));
                    }
                    MergeKind::I64Add | MergeKind::Registered => {}
                }
            }
            let shard = match last {
                Some((table, row, shard)) if table == m.table && compare(row, m.row).is_eq() => {
                    shard
                }
                _ => {
                    let Some((_, shard)) = view.tablets().route(m.table, m.row) else {
                        return Err(Error::TableNotFound(format!("table {}", m.table.0)));
                    };
                    last = Some((m.table, m.row, shard));
                    shard
                }
            };
            if !shards.contains(&shard) {
                shards.push(shard);
            }
        }
        if fixed_ts || counter_writes > 1 {
            return Ok((prepare_counter_writes(&batch.builder, catalog)?, shards));
        }
        Ok((batch.builder, shards))
    }

    pub(crate) fn submit(
        &self,
        batch: WriteBatch,
        durability: Option<Durability>,
        validate: Option<(Seqno, Vec<ReadKey>)>,
        predicate: Option<(TableId, Vec<u8>, Predicate)>,
    ) -> Result<PendingCommit> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        self.check_open()?;
        let durability = durability.unwrap_or_else(|| self.shared.default_durability());
        // One tablet map for the whole routing: its version goes with a cross-shard commit,
        // which a participant moving a tablet refuses and the coordinator retries.
        let view = self.shared.view.load();
        let (builder, mut shards) = self.route(batch, &view)?;
        let (builder, large) = self.separate_large(builder)?;
        // Every shard that owns a row the transaction read validates it at PREPARE, so two
        // transactions cannot each read what the other writes (write skew).
        if let Some((_, reads)) = &validate {
            for r in reads {
                if let Some((_, shard)) = view.tablets().route(r.table, &r.row)
                    && !shards.contains(&shard)
                {
                    shards.push(shard);
                }
            }
        }
        let submitted_at = self.shared.vfs.monotonic_nanos();
        let (tx, waiter) = completion();
        if let Some(large) = large {
            large.settle_on(&tx, crate::large::commit_succeeded);
        }
        if shards.len() <= 1 {
            let shard = shards.first().copied().unwrap_or(ShardId(0));
            self.shared
                .submitter(shard)
                .submit(ShardMsg::Commit(CommitReq {
                    bytes: builder,
                    durability,
                    reply: Reply::Commit(tx),
                    submitted_at,
                    validate,
                    predicate: predicate.map(Box::new),
                    commit_ts: None,
                    map_version: view.tablets().version(),
                    attempts: 0,
                }))?;
        } else {
            if predicate.is_some() {
                return Err(Error::InvalidArgument(
                    "check_and_mutate touches one row".to_owned(),
                ));
            }
            let parts = split_by_shard(&view, &builder, &shards)?;
            let coordinator = shards[0];
            self.shared
                .submitter(coordinator)
                .submit(ShardMsg::Coordinate(Box::new(CoordinateReq {
                    parts,
                    durability,
                    reply: tx,
                    submitted_at,
                    validate,
                    map_version: view.tablets.version(),
                    commit_ts: None,
                    epoch: 0,
                    attempts: 0,
                })))?;
        }
        Ok(PendingCommit {
            waiter,
            shared: Arc::clone(&self.shared),
            resolved: None,
            // A durable commit waits for a sync: polling for it would only burn the CPU.
            spins: matches!(durability, Durability::None | Durability::Buffered),
        })
    }

    fn submit_check_and_mutate(
        &self,
        table: TableId,
        row: &[u8],
        predicate: &Predicate,
        batch: WriteBatch,
        durability: Option<Durability>,
    ) -> Result<PendingCheck> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        self.check_open()?;
        for rd in &batch.row_deletes {
            if rd.table != table || rd.row != row {
                return Err(Error::InvalidArgument(
                    "check_and_mutate batch must touch only the checked row".to_owned(),
                ));
            }
        }
        for m in batch.batch().iter() {
            let m = m?;
            if m.table != table || m.row != row {
                return Err(Error::InvalidArgument(
                    "check_and_mutate batch must touch only the checked row".to_owned(),
                ));
            }
        }
        let durability = durability.unwrap_or_else(|| self.shared.default_durability());
        let view = self.shared.view.load();
        let (builder, shards) = self.route(batch, &view)?;
        let (builder, large) = self.separate_large(builder)?;
        let shard = match shards.as_slice() {
            [] => view
                .tablets()
                .route(table, row)
                .map(|(_, s)| s)
                .ok_or_else(|| Error::TableNotFound(format!("table {}", table.0)))?,
            [s] => *s,
            _ => unreachable!("one row routes to one shard"),
        };
        let submitted_at = self.shared.vfs.monotonic_nanos();
        let (tx, rx) = completion();
        if let Some(large) = large {
            large.settle_on(&tx, crate::large::check_succeeded);
        }
        self.shared
            .submitter(shard)
            .submit(ShardMsg::Commit(CommitReq {
                bytes: builder,
                durability,
                reply: Reply::Check(tx),
                submitted_at,
                validate: None,
                predicate: Some(Box::new((table, row.to_vec(), predicate.clone()))),
                commit_ts: None,
                map_version: view.tablets().version(),
                attempts: 0,
            }))?;
        Ok(PendingCheck {
            waiter: rx,
            shared: Arc::clone(&self.shared),
            resolved: None,
        })
    }

    /// The view and a seqno it covers, for an unpinned read as of now (writer process). The
    /// seqno first, then the view: a seqno is only visible once the view holding its
    /// memtables is published (and with them any blob pointers they hold, D188). Unpinned,
    /// the seqno must still be current after the view loads: then no flush in between had
    /// inputs above it, so its GC kept what a read at it sees (#315 review). Otherwise read
    /// again; under a steady stream of commits, `None` after [`GET_LATEST_TRIES`], and the
    /// caller falls back to a pinned snapshot.
    /// `Engine::get_latest` writes the same loop out (see there).
    // Inlined: called out of line, a row read paid for the call and the returned guard.
    #[inline(always)]
    pub(crate) fn is_reader(&self) -> bool {
        self.role == Role::Reader
    }

    pub(crate) fn latest_view(&self) -> Option<(arc_swap::Guard<Arc<View>>, Seqno)> {
        for _ in 0..GET_LATEST_TRIES {
            let seqno = self.shared.shm.visible_seqno();
            #[cfg(feature = "test-hooks")]
            self.shared.hooks.before_latest_view_load.run();
            let view = self.shared.view.load();
            if self.shared.shm.visible_seqno() == seqno {
                return Some((view, seqno));
            }
        }
        None
    }

    pub(crate) fn snapshot(&self) -> Result<Snapshot> {
        if self.reader.is_some() {
            return self.reader_snapshot();
        }
        // The seqno is pinned before the view loads (and read under the pin's lock), so a
        // flush GC that publishes in between keeps what it reads; still seqno before view: a
        // seqno is only visible once the view holding its memtables is published.
        let pin =
            SeqnoPin::pin_visible(&self.shared.live_seqnos, || self.shared.shm.visible_seqno());
        let seqno = pin.seqno();
        #[cfg(feature = "test-hooks")]
        self.shared.hooks.before_snapshot_view_load.run();
        let view = self.shared.view.load_full();
        Ok(Snapshot {
            seqno,
            view,
            _live: None,
            _pin: Some(Arc::new(pin)),
        })
    }

    pub(crate) fn get(
        &self,
        snapshot: &Snapshot,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
    ) -> Result<Option<CellData>> {
        let now = self.shared.vfs.now_micros();
        snapshot.checked(get_in::<false>(
            &snapshot.view,
            snapshot.seqno,
            now,
            table,
            family,
            row,
            qualifier,
            || Arc::clone(&snapshot.view),
        ))
    }

    fn flush_pending(&self) -> Result<PendingMaintenance> {
        if self.role != Role::Writer {
            return Err(Error::ReadOnly);
        }
        self.check_open()?;
        let mut waiters = Vec::with_capacity(self.shared.shards);
        for i in 0..self.shared.shards {
            let (tx, rx) = completion();
            self.shared
                .submitter(ShardId(i as u16))
                .submit(ShardMsg::FlushAll { reply: tx })?;
            waiters.push(rx);
        }
        Ok(PendingMaintenance {
            waiters,
            rounds: None,
            failed: None,
        })
    }

    fn compact_pending(&self, table: Option<TableId>) -> Result<PendingMaintenance> {
        if self.role != Role::Writer {
            return Err(Error::ReadOnly);
        }
        self.check_open()?;
        let rounds = CompactRounds::new(&self.shared, table);
        let waiters = compact_round(&self.shared, table)?;
        Ok(PendingMaintenance {
            waiters,
            rounds,
            failed: None,
        })
    }

    // ---- close ----

    fn close(&self, wait: bool) -> Result<()> {
        if self.role != Role::Writer {
            return self.close_reader();
        }
        if self.closing.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let (tx, rx) = completion();
        *self
            .shared
            .close
            .done
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(tx);
        // Every shard may have reported already (dropped, or its thread panicked) and the
        // final close run before `done` was set: answer it here. The final close records
        // its outcome before it takes `done`, so one of the two sides sees the other.
        if let Some(outcome) = self
            .shared
            .close
            .outcome
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            && let Some(tx) = self
                .shared
                .close
                .done
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
        {
            tx.notify(outcome.as_ref().copied().map_err(Error::duplicate));
        }
        if let Some(submitters) = self.shared.submitters.get() {
            for s in submitters {
                let _ = s.submit(ShardMsg::Close);
            }
        }
        // Application-owned shards are driven by the application's threads, possibly the
        // caller's own: the close waits only when every shard is run by another thread (or
        // already dropped); otherwise the shards finish as they are run and report the
        // outcome through `EngineShard::closed`.
        let result = if wait && (!self.application_owned || self.shared.drivers.driven_elsewhere())
        {
            rx.wait().unwrap_or(Err(Error::Closed))
        } else {
            drop(rx);
            Ok(())
        };
        if !self.application_owned {
            let rt = self
                .runtime
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(rt) = rt {
                // A shard thread that panicked resumes its panic here: report it instead
                // (this also runs from `Drop for Engine`).
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| rt.shutdown())) {
                    Ok(r) => drop(r?),
                    Err(panic) => return Err(shard_panicked(&*panic)),
                }
            }
        }
        result
    }

    fn close_reader(&self) -> Result<()> {
        let Some(r) = &self.reader else {
            return Ok(());
        };
        if self.closing.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        {
            let shm = r.shm.lock().unwrap_or_else(PoisonError::into_inner);
            shm.slot.unpin();
        }
        let presence = r
            .presence
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(presence) = presence
            && presence.try_become_last()?
        {
            // Last one out (issue #20): only if no writer holds or is taking the writer byte,
            // which a writer keeps from before it is present until it is.
            if let Ok(lock) = WriterLock::acquire(&r.file) {
                // The writer closed cleanly (its streams are checkpointed to their ends): the
                // WAL files are not needed. After a writer crash they are, so they stay.
                if Pager::open(&self.shared.vfs, &self.path, false)
                    .map(|o| o.clean_shutdown())
                    .unwrap_or(false)
                {
                    crate::shard::remove_wal_files(&self.shared.vfs, &self.path)?;
                }
                ShmRegion::remove(
                    &self.shared.vfs,
                    self.shared.identity,
                    self.shared.shm_dir.as_deref(),
                )?;
                drop(lock);
            }
        }
        self.shared.closed.store(true, Ordering::Release);
        Ok(())
    }

    // ---- reader process ----

    /// The catalog of manifest `version` and the file it was read from: the cached one, or
    /// the durable root's when that is `version`. `None` when the durable root is another
    /// version: the writer committed a newer root and has not published its view yet (or
    /// published one since the record was read). A view must pair a record with the catalog
    /// of the record's own manifest version (issue #140): a memtable the record lists is in
    /// a newer catalog as an SST too, and a newer record no longer lists a memtable whose
    /// SST an older catalog lacks.
    fn reader_catalog(&self, version: ManifestVersion) -> Result<Option<(Arc<Catalog>, FileRef)>> {
        let r = self.reader.as_ref().expect("reader");
        let mut cached = r.catalog.lock().unwrap_or_else(PoisonError::into_inner);
        if cached.0 != version {
            // The reader's own read-only pager: re-reads the superblocks, nothing more.
            let pager = &self.shared.pager;
            pager.reload_root()?;
            let root = pager.root();
            if root.manifest_version != version {
                return Ok(None);
            }
            #[cfg(feature = "test-hooks")]
            r.hooks.before_manifest_load.run();
            let (catalog, _) =
                manifest::load_root(pager.file(), &root, r.shards, Arc::clone(&r.registry))?;
            *cached = (version, Arc::new(catalog), pager.data_file().clone());
        }
        Ok(Some((Arc::clone(&cached.1), cached.2.clone())))
    }

    /// The view of the record currently published in `shm`, or `None` while the durable
    /// root names another manifest version (see [`Inner::reader_catalog`]).
    fn reader_view(&self, shm: &ShmRegion) -> Result<Option<Arc<View>>> {
        let r = self.reader.as_ref().expect("reader");
        let record = shm.read_view()?;
        #[cfg(feature = "test-hooks")]
        r.hooks.after_record.run();
        let Some((catalog, file)) = self.reader_catalog(record.manifest_version)? else {
            return Ok(None);
        };
        let mut cached = r.view.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(v) = &*cached
            && v.version == record.view_version
            && Arc::ptr_eq(&v.catalog, &catalog)
        {
            return Ok(Some(Arc::clone(v)));
        }
        let prev = cached.as_ref().map(|v| Arc::clone(&v.ssts));
        let view = Arc::new(view_from_record(
            shm,
            &record,
            catalog,
            prev.as_deref(),
            file,
            Arc::clone(&self.shared.cache),
        )?);
        *cached = Some(Arc::clone(&view));
        Ok(Some(view))
    }

    fn reader_snapshot(&self) -> Result<Snapshot> {
        let r = self.reader.as_ref().expect("reader");
        let deadline = std::time::Instant::now() + READER_VIEW_WAIT;
        let mut waits = 0u32;
        loop {
            let mut guard = r.shm.lock().unwrap_or_else(PoisonError::into_inner);
            let a = &mut *guard;
            if a.shm.is_stale() {
                let shm = a.shm.reattach(&self.shared.vfs, &r.file)?;
                let slot = shm.claim_reader_slot(self.shared.vfs.current_process())?;
                // Snapshots of the old generation keep counting there: they can no longer be
                // read, so they must not hold this generation's pin back.
                *a = Attachment {
                    shm,
                    slot,
                    pinned: false,
                    live: Arc::new(AtomicUsize::new(0)),
                };
                *r.view.lock().unwrap_or_else(PoisonError::into_inner) = None;
                // Start the new generation from the new writer's root.
                r.catalog.lock().unwrap_or_else(PoisonError::into_inner).0 = NO_MANIFEST_VERSION;
            }
            // The pin protects the oldest live snapshot and every newer view. While snapshots
            // of this generation are alive it stays put; once none is left it moves forward to
            // this one, so a long-lived reader never blocks reclamation for ever.
            let seqno = a.shm.visible_seqno();
            let seqno = if a.pinned && a.live.load(Ordering::Acquire) > 0 {
                seqno
            } else {
                let (s, _) = a.slot.pin(seqno, a.shm.view_version());
                a.pinned = true;
                s
            };
            a.live.fetch_add(1, Ordering::AcqRel);
            let live = Arc::new(LiveSnapshot {
                count: Arc::clone(&a.live),
                shm: a.shm.clone(),
            });
            let shm = a.shm.clone();
            drop(guard);
            // The record is read after the pin, so the pin covers it.
            match self.reader_view(&shm) {
                // A writer restarted while the view was built (whatever the build read, a
                // failure included): re-attach and start over.
                Ok(Some(_)) | Err(_) if live.check_current().is_err() => continue,
                Ok(Some(view)) => {
                    return Ok(Snapshot {
                        seqno,
                        view,
                        _live: Some(live),
                        _pin: None,
                    });
                }
                Err(e) => return Err(e),
                Ok(None) => {
                    // Dropping `live` releases this attempt's count; the pin stays valid for
                    // the next attempt, which reads a newer record.
                    drop(live);
                    if std::time::Instant::now() >= deadline {
                        // The durable root stayed ahead of the published view: the writer
                        // died between its root commit and its publish, or is stalled there.
                        // A new writer republishes.
                        return Err(Error::Busy);
                    }
                    if waits < 16 {
                        std::thread::yield_now();
                    } else {
                        // 50 µs doubling to 1.6 ms.
                        let micros = 50u64 << (waits - 16).min(5);
                        std::thread::sleep(std::time::Duration::from_micros(micros));
                    }
                    waits += 1;
                }
            }
        }
    }
}

/// How long a reader waits for the writer to publish the view of a root it already
/// committed before `snapshot()` gives up with [`Error::Busy`].
const READER_VIEW_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

/// How often `get_latest` in a reader process retries a read a writer restart expired.
const READER_EXPIRED_RETRIES: u32 = 8;

/// Names no manifest version: a reader's catalog cache after a re-attach.
const NO_MANIFEST_VERSION: ManifestVersion = ManifestVersion::MAX;

/// Builds a reader's view from the record published in shared memory.
fn view_from_record(
    shm: &ShmRegion,
    record: &ViewRecord,
    catalog: Arc<Catalog>,
    prev: Option<&SstSet>,
    file: FileRef,
    cache: Arc<BlockCache>,
) -> Result<View> {
    let tablets: Vec<TabletEntry> = record
        .tablets
        .iter()
        .map(|t| TabletEntry {
            id: t.tablet,
            table: t.table,
            start: t.start.clone(),
            end: t.end.clone(),
            shard: ShardId(t.shard),
        })
        .collect();
    let mut arenas: HashMap<u16, ArenaRegion> = HashMap::new();
    type Listed = (ShardId, Vec<(u8, MemtableReader, u32)>);
    let mut sets: HashMap<(TabletId, FamilyId), Listed> = HashMap::new();
    for m in &record.memtables {
        let arena = match arenas.get(&m.shard) {
            Some(a) => a.clone(),
            None => {
                let (region, offset, len) = shm.arena(u32::from(m.shard));
                let a = ArenaRegion::new(region, offset, len)?;
                arenas.insert(m.shard, a.clone());
                a
            }
        };
        let reader = MemtableReader::open(arena, m.root)?;
        sets.entry((m.tablet, m.family))
            .or_insert_with(|| (ShardId(m.shard), Vec::new()))
            .1
            .push((m.age, reader, m.root));
    }
    let mut pieces: Vec<pigeonhole_format::hash::FastMap<(TabletId, FamilyId), Arc<MemSet>>> =
        (0..shm.shard_count()).map(|_| Default::default()).collect();
    for (k, (shard, mut list)) in sets {
        list.sort_by_key(|(age, ..)| *age);
        let roots = list.iter().map(|(_, _, r)| *r).collect();
        let readers = list.into_iter().map(|(_, r, _)| r).collect();
        if let Some(piece) = pieces.get_mut(usize::from(shard.0)) {
            piece.insert(
                k,
                Arc::new(MemSet {
                    shard,
                    readers,
                    roots,
                }),
            );
        }
    }
    let mut no_readers = HashMap::new();
    let ssts = Arc::new(SstSet::build(&catalog, prev, &mut no_readers, file, cache));
    Ok(View {
        version: record.view_version,
        manifest_version: record.manifest_version,
        tablets: Arc::new(TabletMap::build(record.view_version, &tablets)),
        catalog,
        mems: pieces
            .into_iter()
            .map(|map| Arc::new(ShardMems { map }))
            .collect(),
        ssts,
        _pin: None,
    })
}

/// Checks a put or operand into a counter family (D179): its value must be a stored `i64`,
/// and one without a timestamp takes the fixed counter timestamp, which a family with a TTL
/// refuses (it would expire at once). Returns whether the mutation needs that timestamp.
fn check_counter_write(m: &Mutation<'_>, meta: &FamilyMeta, catalog: &Catalog) -> Result<bool> {
    if !matches!(m.kind, Kind::Put | Kind::Merge) {
        return Ok(false);
    }
    if m.value.len() != 9 || m.value[0] != ValueTag::I64 as u8 {
        return Err(Error::InvalidArgument(format!(
            "{} is a counter family: it holds only i64 values (put_i64 / incr)",
            family_name(catalog, m.table, m.family)
        )));
    }
    if m.ts.is_some() {
        return Ok(false);
    }
    if meta.options.ttl_micros != 0 {
        return Err(Error::InvalidArgument(format!(
            "counter {} has a TTL, so its fixed-timestamp counter would expire at once: \
             write a bucket with an explicit timestamp (incr_at / put_i64_at)",
            family_name(catalog, m.table, m.family)
        )));
    }
    Ok(true)
}

/// `family "name" of table "name"` for messages (ids if the catalog does not know them).
fn family_name(catalog: &Catalog, table: TableId, family: FamilyId) -> String {
    match catalog.table(table) {
        Some(t) => match t.families.iter().find(|f| f.id == family) {
            Some(f) => format!("family {:?} of table {:?}", f.name, t.name),
            None => format!("family {} of table {:?}", family.0, t.name),
        },
        None => format!("family {} of table {}", family.0, table.0),
    }
}

/// `(table, family, row, qualifier, timestamp)` of a cell in a batch.
type CellKey<'a> = (TableId, FamilyId, &'a [u8], &'a [u8], Timestamp);

/// `batch` with its counter-family writes prepared (D179, #295): a put or operand without a
/// timestamp moves to the fixed counter timestamp, [`COUNTER_TS`], and an operand of a cell
/// (column and timestamp) that an earlier put or operand of this batch wrote combines into
/// that write: the put's value or the operand grows by it, in write order. Deletes do not
/// stop a combination; D34 then collapses what is left of one cell to the last write.
fn prepare_counter_writes(batch: &BatchBuilder, catalog: &Catalog) -> Result<BatchBuilder> {
    struct Write<'a> {
        m: Mutation<'a>,
        /// The value of a combined write, replacing `m.value`.
        value: Option<Vec<u8>>,
    }
    let mut writes: Vec<Write<'_>> = Vec::new();
    // The latest counter put or operand of each cell.
    let mut latest: HashMap<CellKey<'_>, usize> = HashMap::new();
    for m in batch.batch().iter() {
        let mut m = m?;
        let counter = catalog
            .family(m.family)
            .is_some_and(|f| f.options.kind == FamilyKind::Counter);
        if !(counter && matches!(m.kind, Kind::Put | Kind::Merge)) {
            writes.push(Write { m, value: None });
            continue;
        }
        let ts = *m.ts.get_or_insert(COUNTER_TS);
        let key = (m.table, m.family, m.row, m.qualifier, ts);
        if m.kind == Kind::Merge
            && let Some(&i) = latest.get(&key)
        {
            let w = &mut writes[i];
            let sum = stored_i64(w.value.as_deref().unwrap_or(w.m.value))
                .wrapping_add(stored_i64(m.value));
            let mut v = Vec::with_capacity(9);
            encode_value(&mut v, ValueRef::I64(sum));
            w.value = Some(v);
            continue;
        }
        latest.insert(key, writes.len());
        writes.push(Write { m, value: None });
    }
    let mut out = BatchBuilder::new();
    for w in &writes {
        let m = &w.m;
        let value = w.value.as_deref().unwrap_or(m.value);
        out.push(m.table, m.family, m.kind, m.row, m.qualifier, m.ts, value)?;
    }
    Ok(out)
}

/// Refuses a file on a network filesystem, and on FUSE unless `allow_fuse` (D173, #299).
fn check_local(file: &FileRef, allow_fuse: bool) -> Result<()> {
    match file.locality()? {
        Locality::Local => Ok(()),
        Locality::Fuse if allow_fuse => Ok(()),
        _ => Err(Error::NetworkFilesystem),
    }
}

/// The `i64` in a stored counter value (`check_counter_write` admitted only those).
fn stored_i64(v: &[u8]) -> i64 {
    v.get(1..9)
        .and_then(|b| <[u8; 8]>::try_from(b).ok())
        .map_or(0, i64::from_le_bytes)
}

fn check_merge_operator(
    options: &FamilyOptions,
    registry: &MergeRegistry,
    allow_unregistered: bool,
) -> Result<()> {
    let name = options.merge_operator.as_str();
    if options.kind == FamilyKind::Counter && name != crate::catalog::I64_ADD {
        return Err(Error::InvalidArgument(format!(
            "a counter family sums i64s with {}, not {name:?}",
            crate::catalog::I64_ADD
        )));
    }
    if name.is_empty() || registry.get(name).is_some() || allow_unregistered {
        Ok(())
    } else {
        Err(Error::UnknownMergeOperator(name.to_owned()))
    }
}

/// One shard in application-owned mode. Move it to the thread that should run it.
///
/// ```no_run
/// use pigeonhole_engine::{Engine, EngineOptions};
/// use pigeonhole_io::sim::SimVfs;
///
/// # fn main() -> pigeonhole_engine::Result<()> {
/// let mut options = EngineOptions::new(SimVfs::new(1));
/// options.create_if_missing = true;
/// options.shards = 1;
/// let (db, mut shards) = Engine::open_application_owned("/db/data.phdb".as_ref(), options)?;
/// // The application's event loop drives each shard from its own core thread.
/// while shards[0].run_once(u64::MAX) {}
/// db.close()?;
/// // This thread drives the shard, so `close` cannot wait: drive it until it has closed.
/// while shards[0].closed().is_none() {
///     shards[0].run_once(u64::MAX);
/// }
/// assert!(shards[0].closed().unwrap().is_ok());
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct EngineShard {
    driver: Option<ShardDriver<ShardState>>,
    engine: Arc<Inner>,
}

impl EngineShard {
    /// Shard index.
    pub fn index(&self) -> u16 {
        self.driver.as_ref().map_or(0, |d| d.shard().0)
    }

    /// Runs queued writes, the group commit, and background work until `deadline_nanos`.
    /// Returns whether work remains. Work blocked on I/O in flight does not count: the
    /// wakeup fires when it completes.
    ///
    /// The calling thread counts as this shard's driver from now on (until another thread
    /// runs it or it is dropped), so its blocking calls fail rather than deadlock (see
    /// [`Engine::open_application_owned`]). Until a thread first runs the shard, nobody
    /// counts as its driver: a thread holding a shard it has not run yet is not protected,
    /// and must not block on a commit (run the shard first).
    pub fn run_once(&mut self, deadline_nanos: u64) -> bool {
        let Some(d) = self.driver.as_mut() else {
            return false;
        };
        self.engine.shared.drivers.enter(usize::from(d.shard().0));
        d.run_once(deadline_nanos)
    }

    /// The outcome of the database's close once it has finished (every shard closed and
    /// the last one recorded the clean close), or `None` before. After [`Engine::close`],
    /// keep driving the shard until this is `Some`, then drop it. Every shard reports the
    /// same outcome; an `Err` means the close was not clean or failed (the next open
    /// replays the WAL).
    pub fn closed(&self) -> Option<Result<()>> {
        self.engine
            .shared
            .close
            .outcome
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|r| r.as_ref().copied().map_err(Error::duplicate))
    }

    /// The earliest deadline (VFS `monotonic_nanos`) of background work sleeping on this
    /// shard (a write stall's refill, a wait for arena room's timeout, a failed compaction's
    /// backoff), or `None`. After [`run_once`](EngineShard::run_once) returns `false`, call
    /// it again when the wakeup fires or this deadline passes, whichever comes first.
    pub fn next_deadline(&self) -> Option<u64> {
        self.driver.as_ref().and_then(ShardDriver::next_deadline)
    }

    /// Commits `batch` inline if every row belongs to this shard; otherwise submits it.
    pub fn commit_local(
        &mut self,
        batch: WriteBatch,
        durability: Option<Durability>,
    ) -> Result<PendingCommit> {
        let engine = Arc::clone(&self.engine);
        engine.check_open()?;
        let durability = durability.unwrap_or_else(|| engine.shared.default_durability());
        let view = engine.shared.view.load();
        let (builder, shards) = engine.route(batch, &view)?;
        let me = ShardId(self.index());
        let Some(driver) = self.driver.as_mut() else {
            return Err(Error::Closed);
        };
        if shards.len() > 1 || shards.first().is_some_and(|s| *s != me) {
            // Not ours: the regular path (a queue hop, or two-phase commit).
            let mut wb = WriteBatch::new();
            wb.builder = builder;
            return engine.submit(wb, Some(durability), None, None);
        }
        let (builder, large) = engine.separate_large(builder)?;
        let submitted_at = engine.shared.vfs.monotonic_nanos();
        let (tx, waiter) = completion();
        if let Some(large) = large {
            large.settle_on(&tx, crate::large::commit_succeeded);
        }
        driver.with_handler(|h, ctx| {
            h.commit_inline(
                CommitReq {
                    bytes: builder,
                    durability,
                    reply: Reply::Commit(tx),
                    submitted_at,
                    validate: None,
                    predicate: None,
                    commit_ts: None,
                    map_version: view.tablets().version(),
                    attempts: 0,
                },
                ctx,
            )
        });
        Ok(PendingCommit {
            waiter,
            shared: Arc::clone(&engine.shared),
            resolved: None,
            spins: false,
        })
    }

    /// Registers a callback the engine calls when work arrives for this shard.
    pub fn set_wakeup(&mut self, wake: Box<dyn Fn() + Send + Sync>) {
        if let Some(d) = self.driver.as_mut() {
            d.set_wakeup(wake);
        }
    }
}

impl Drop for EngineShard {
    fn drop(&mut self) {
        if let Some(driver) = self.driver.take() {
            self.engine
                .shared
                .drivers
                .dropped(usize::from(driver.shard().0));
            let mut state = driver.shutdown();
            state.abandon(&self.engine.shared);
        }
    }
}

/// The error for a shard thread that panicked (issue #135).
fn shard_panicked(panic: &(dyn std::any::Any + Send)) -> Error {
    let msg = panic
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_owned());
    crate::error::io_other("shard thread panicked", msg)
}

/// With `direct`, opens `path` again for direct I/O (#403) as the pager's handle for SST and
/// blob extents. A file system that refuses direct I/O keeps the buffered handle.
fn open_data_file(
    vfs: &pigeonhole_io::VfsRef,
    path: &Path,
    pager: &Pager,
    direct: bool,
    write: bool,
) -> Result<()> {
    if !direct {
        return Ok(());
    }
    let mut opts = OpenOptions::read();
    opts.write = write;
    opts.direct = true;
    match vfs.open(path, opts) {
        Ok(f) => {
            pager.set_data_file(f);
            Ok(())
        }
        Err(e) if e.kind == ErrorKind::Unsupported => {
            crate::shard::trace!("direct I/O unavailable here ({e}); SSTs stay buffered");
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
