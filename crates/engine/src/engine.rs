use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use arc_swap::ArcSwap;
use pigeonhole_cache::BlockCache;
use pigeonhole_compaction::MergeRegistry;
use pigeonhole_format::manifest::{Edit, FamilyOptions};
use pigeonhole_format::shm::ViewRecord;
use pigeonhole_format::wal::{BatchBuilder, WalRecord};
use pigeonhole_format::{
    Durability, FamilyId, Kind, Lsn, ManifestVersion, Seqno, StreamId, TableId, TabletId,
};
use pigeonhole_io::{ErrorKind, FileRef, OpenOptions};
use pigeonhole_memtable::{ArenaRegion, MemtableReader, ShardArena};
use pigeonhole_pager::Pager;
use pigeonhole_runtime::{
    Runtime, RuntimeConfig, ShardDriver, ShardId, Submitter, Waiter, completion,
};
use pigeonhole_shm::{Presence, ReaderSlot, Role as ShmRole, ShmConfig, ShmRegion, WriterLock};
use pigeonhole_sst::SstWriterOptions;
use pigeonhole_wal::{Recovery, Wal, WalStream, discover_streams, stream_path};

use crate::catalog::{Catalog, MergeKind};
use crate::flush::{SstSink, write_memtable};
use crate::manifest::{self, ManifestWriter, ReqKind};
use crate::read::{self, get_in};
use crate::shard::{
    BalanceConfig, CloseState, CommitReq, CoordinateReq, FreezeWaiters, LoadSlot, Locks, Padded,
    ReplayedKind, Reply, ShardMetrics, ShardMsg, ShardState, Shared, VisibilityWaiters,
    bucket_floor, split_by_shard,
};
use crate::snapshot::{
    LiveSeqnos, LiveSnapshot, LiveViews, MemSet, SeqnoPin, ShardMems, SstSet, TabletEntry,
    TabletMap, View, ViewPin,
};
use crate::write::ReadKey;
use crate::{
    CellData, EngineOptions, Error, PendingCommit, Predicate, ReadSpec, Result, RowData,
    ScanCursor, ScanSpec, Snapshot, Txn, WriteBatch,
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
pub struct Metrics {
    /// Commits per durability level, indexed by `Durability as usize`.
    pub commits: [u64; 4],
    /// Commit latency in nanoseconds (p50, p99, p99.9) per durability level.
    pub commit_latency_nanos: [[u64; 3]; 4],
    /// Flushes completed.
    pub flushes: u64,
    /// Compactions completed.
    pub compactions: u64,
    /// Write stalls (token-bucket waits and refused commits) and total stalled nanoseconds.
    pub stalls: (u64, u64),
    /// Block cache hits and misses (the cache does not count them yet: always zero).
    pub block_cache: (u64, u64),
}

/// The lock-page byte range of the superblock-less file check (decision D59).
const INTERRUPTED_CREATE_MAX: u64 = 64 * 1024;

/// A reader process's attachment to the writer's shared memory.
struct ReaderState {
    file: FileRef,
    presence: Mutex<Option<Presence>>,
    shm: Mutex<(ShmRegion, ReaderSlot, bool)>,
    /// `(manifest version, catalog, the file the manifest was read from)` as last loaded.
    catalog: Mutex<(ManifestVersion, Arc<Catalog>, FileRef)>,
    /// The last view built from the region, by view version.
    view: Mutex<Option<Arc<View>>>,
    shards: usize,
    /// Snapshots of this process still alive; the pin moves forward when it drops to zero.
    live: Arc<AtomicUsize>,
    registry: Arc<MergeRegistry>,
}

/// Everything behind an [`Engine`] (and the handle a [`Txn`] keeps).
pub(crate) struct Inner {
    pub(crate) shared: Arc<Shared>,
    role: Role,
    options: EngineOptions,
    path: PathBuf,
    runtime: Mutex<Option<Runtime<ShardState>>>,
    application_owned: bool,
    closing: AtomicBool,
    reader: Option<ReaderState>,
    /// Largest value accepted at write time (decision D16).
    max_value: usize,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("role", &self.role)
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

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

fn chunk_size(budget: u64) -> usize {
    let chunk = (budget / 64).clamp(1024, ShardArena::DEFAULT_CHUNK as u64) as usize;
    chunk & !63
}

/// Most `(tablet, family)` slots an arena of `arena_len` bytes in `chunk` chunks serves: a
/// quarter of its chunks (every slot written to takes a chunk, usually two before it
/// freezes, and frozen memtables keep theirs until flushed).
pub(crate) fn max_slots(arena_len: usize, chunk: usize) -> usize {
    arena_len / chunk.max(1) / 4
}

/// The arena chunk size with tablet changes on (D136, #104): at least 256 chunks per arena,
/// so every shard serves 64 slots whatever the budget, and smaller still when the tablets
/// placed at open need more (a reopen with fewer shards after splits). Never below 1 KiB.
fn tablet_chunk_size(arena_len: usize, placed_slots: usize) -> usize {
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

/// A future for a maintenance operation (`flush`, `compact`) a test harness drives without
/// blocking (the `test-hooks` feature).
#[cfg(feature = "test-hooks")]
#[derive(Debug)]
#[doc(hidden)]
pub struct PendingMaintenance {
    waiters: Vec<Waiter<Result<()>>>,
    rounds: Option<CompactRounds>,
}

#[cfg(feature = "test-hooks")]
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

#[cfg(feature = "test-hooks")]
impl PendingMaintenance {
    fn poll_round(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<()>> {
        let mut failed = None;
        let mut i = 0;
        while i < self.waiters.len() {
            match std::pin::Pin::new(&mut self.waiters[i]).poll(cx) {
                std::task::Poll::Ready(Some(Ok(()))) => {
                    self.waiters.swap_remove(i);
                }
                std::task::Poll::Ready(Some(Err(e))) => {
                    self.waiters.swap_remove(i);
                    failed = Some(e);
                }
                std::task::Poll::Ready(None) => {
                    self.waiters.swap_remove(i);
                    failed = Some(Error::Closed);
                }
                std::task::Poll::Pending => i += 1,
            }
        }
        if let Some(e) = failed {
            return std::task::Poll::Ready(Err(e));
        }
        if self.waiters.is_empty() {
            std::task::Poll::Ready(Ok(()))
        } else {
            std::task::Poll::Pending
        }
    }
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

/// One raw entry of a table (a test hook).
#[cfg(feature = "test-hooks")]
#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct RawEntry {
    /// Table.
    pub table: TableId,
    /// Family.
    pub family: FamilyId,
    /// Internal key.
    pub key: Vec<u8>,
    /// Stored value.
    pub value: Vec<u8>,
}

/// A tablet's id, table and row range `[start, end)` (a test hook).
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub type TabletRange = (TabletId, TableId, Vec<u8>, Option<Vec<u8>>);

/// What the manifest of a closed database records (a test hook).
#[cfg(feature = "test-hooks")]
#[derive(Debug, Clone, Default)]
#[doc(hidden)]
pub struct ManifestInfo {
    /// Manifest version.
    pub version: ManifestVersion,
    /// Per-stream checkpoints.
    pub checkpoints: BTreeMap<StreamId, Lsn>,
    /// Flushed-through seqno per `(tablet, family)`.
    pub flushed: BTreeMap<(TabletId, FamilyId), Seqno>,
    /// Tablets and their tables.
    pub tablets: Vec<(TabletId, TableId)>,
    /// Tablets with their tables and row ranges `[start, end)` (empty start and `None` end
    /// are unbounded).
    pub tablet_ranges: Vec<TabletRange>,
    /// Whether the last close was clean.
    pub clean: bool,
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
    /// threads at all, so a nonzero `options.compaction_threads` fails with
    /// `InvalidArgument` before anything is opened (decision D40).
    ///
    /// After [`Engine::close`], keep driving each shard with [`EngineShard::run_once`] until
    /// it returns `false`, then drop it: the shards finish their in-flight work, flush,
    /// checkpoint and sync their streams as part of the close.
    ///
    /// Catalog changes (`create_table`, `add_family`, `drop_table`) and `flush`, `compact`
    /// and `shrink` wait for a manifest commit; call them from a thread that does not drive
    /// the shards, or they wait for ever.
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

        // 1. The lock handle: writer byte, then the local-filesystem check (D37 order).
        let mut open_opts = OpenOptions::read();
        open_opts.write = true;
        let file = vfs.open(path, open_opts)?;
        let writer_lock = WriterLock::acquire(&file)?;
        if !file.is_local()? {
            return Err(Error::NetworkFilesystem);
        }
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
        if catalog.has_unknown_merge && !options.allow_unregistered_merge {
            let name = catalog
                .tables()
                .flat_map(|t| t.families.iter())
                .map(|f| f.options.merge_operator.clone())
                .find(|n| !n.is_empty() && registry.get(n).is_none())
                .unwrap_or_default();
            return Err(Error::UnknownMergeOperator(name));
        }

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
        //    chunk size shrinks when they do not (#104).
        let chunk = if options.tablet_changes {
            let arena_len = shm.arena(0).2;
            let base = tablet_chunk_size(arena_len, 0);
            let placed = catalog.place(shards, max_slots(arena_len, base));
            tablet_chunk_size(arena_len, placed)
        } else {
            chunk_size(options.memtable_budget)
        };
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
            ssts: Arc::new(SstSet::empty(pager.file().clone(), Arc::clone(&cache))),
            _pin: None,
        });
        let shared = Arc::new(Shared {
            vfs: Arc::clone(&vfs),
            shm: shm.clone(),
            shards,
            view: ArcSwap::new(empty_view),
            view_lock: Mutex::new(0),
            manifest: Mutex::new(ManifestWriter::new(Arc::clone(&pager))),
            manifest_queue: Default::default(),
            manifest_busy: AtomicBool::new(false),
            pager: Arc::clone(&pager),
            cache: Arc::clone(&cache),
            sst_ids: Arc::new(AtomicU64::new(catalog.counters.next_sst.max(1))),
            blob_ids: Arc::new(AtomicU32::new(catalog.counters.next_blob_file.max(1))),
            live_views: Arc::clone(&live_views),
            live_seqnos: Arc::new(LiveSeqnos::default()),
            flushed_roots: Mutex::new(HashSet::new()),
            busy_ssts: Mutex::new(HashSet::new()),
            view_versions: Mutex::new(BTreeMap::new()),
            compactions: Mutex::new(Vec::new()),
            #[cfg(feature = "test-hooks")]
            appended: Mutex::new(Vec::new()),
            #[cfg(feature = "test-hooks")]
            manifest_race: AtomicBool::new(false),
            #[cfg(feature = "test-hooks")]
            manifest_race_waiter: Mutex::new(None),
            #[cfg(feature = "test-hooks")]
            manifest_park: AtomicBool::new(false),
            #[cfg(feature = "test-hooks")]
            manifest_parked: Mutex::new(None),
            picker: options.compaction.clone(),
            write_stall_timeout_nanos: options.write_stall_timeout_nanos,
            locks: Mutex::new(Some(Locks {
                _writer: writer_lock,
                presence,
            })),
            default_durability: AtomicU8::new(options.durability as u8),
            closed: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            pager_poisoned: AtomicBool::new(false),
            close: CloseState {
                remaining: AtomicUsize::new(shards),
                done: Mutex::new(None),
                failed: AtomicBool::new(false),
                final_pending: AtomicBool::new(false),
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
            freeze_waiters: FreezeWaiters::default(),
            memtable_freeze_bytes: freeze_bytes,
            submitters: std::sync::OnceLock::new(),
            shm_dir: options.shm_dir.clone(),
            identity,
            path: path.to_path_buf(),
        });
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
        // logged on its shard for checkpointing. Otherwise (D20) everything recovered is
        // flushed now, every stream checkpointed to its end, and the extra streams removed.
        let same_layout =
            streams.len() == shards && streams.iter().enumerate().all(|(i, s)| s.0 as usize == i);
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
                states[i].set_wal(Box::new(rec.into_stream(options.wal)?));
                have_wal[i] = true;
            }
        } else {
            flush_recovered(
                &shared,
                &mut catalog,
                &mut states,
                &recoveries,
                shards,
                &options,
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
        for (i, have) in have_wal.iter().enumerate() {
            if !have {
                let wal = WalStream::create(&vfs, path, StreamId(i as u32), db_id, options.wal)?;
                states[i].set_wal(Box::new(wal));
            }
        }
        for s in &mut states {
            s.finish_replay();
        }

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
            pager.file().clone(),
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
        let inner = Arc::new(Inner {
            shared: Arc::clone(&shared),
            role: Role::Writer,
            options,
            path: path.to_path_buf(),
            runtime: Mutex::new(None),
            application_owned: mode == Mode::ApplicationOwned,
            closing: AtomicBool::new(false),
            reader: None,
            max_value,
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
        if !file.is_local()? {
            return Err(Error::NetworkFilesystem);
        }
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
            ssts: Arc::new(SstSet::empty(pager.file().clone(), Arc::clone(&cache))),
            _pin: None,
        });
        let shared = Arc::new(Shared {
            vfs: Arc::clone(&vfs),
            shm: shm.clone(),
            shards,
            view: ArcSwap::new(empty_view),
            view_lock: Mutex::new(0),
            manifest: Mutex::new(ManifestWriter::new(Arc::clone(&pager))),
            manifest_queue: Default::default(),
            manifest_busy: AtomicBool::new(false),
            pager: Arc::clone(&pager),
            cache: Arc::clone(&cache),
            sst_ids: Arc::new(AtomicU64::new(0)),
            blob_ids: Arc::new(AtomicU32::new(0)),
            live_views: Arc::new(LiveViews::default()),
            live_seqnos: Arc::new(LiveSeqnos::default()),
            flushed_roots: Mutex::new(HashSet::new()),
            busy_ssts: Mutex::new(HashSet::new()),
            view_versions: Mutex::new(BTreeMap::new()),
            compactions: Mutex::new(Vec::new()),
            #[cfg(feature = "test-hooks")]
            appended: Mutex::new(Vec::new()),
            #[cfg(feature = "test-hooks")]
            manifest_race: AtomicBool::new(false),
            #[cfg(feature = "test-hooks")]
            manifest_race_waiter: Mutex::new(None),
            #[cfg(feature = "test-hooks")]
            manifest_park: AtomicBool::new(false),
            #[cfg(feature = "test-hooks")]
            manifest_parked: Mutex::new(None),
            picker: options.compaction.clone(),
            write_stall_timeout_nanos: options.write_stall_timeout_nanos,
            locks: Mutex::new(None),
            default_durability: AtomicU8::new(options.durability as u8),
            closed: AtomicBool::new(false),
            closing: AtomicBool::new(false),
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
            freeze_waiters: FreezeWaiters::default(),
            memtable_freeze_bytes: options.memtable_freeze_bytes.max(1),
            submitters: std::sync::OnceLock::new(),
            shm_dir: options.shm_dir.clone(),
            identity,
            path: path.to_path_buf(),
        });
        let reader = ReaderState {
            file,
            presence: Mutex::new(Some(presence)),
            shm: Mutex::new((shm, slot, false)),
            catalog: Mutex::new((manifest_version, Arc::new(catalog), manifest_file)),
            view: Mutex::new(None),
            shards,
            live: Arc::new(AtomicUsize::new(0)),
            registry,
        };
        let inner = Arc::new(Inner {
            shared,
            role: Role::Reader,
            options,
            path: path.to_path_buf(),
            runtime: Mutex::new(None),
            application_owned: false,
            closing: AtomicBool::new(false),
            reader: Some(reader),
            max_value: 0,
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
    pub fn commit(&self, batch: WriteBatch, durability: Option<Durability>) -> Result<CommitInfo> {
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
            .check_and_mutate(table, row, predicate, batch, durability)
    }

    /// Starts an optimistic transaction (Phase 4).
    pub fn begin(&self) -> Result<Txn> {
        if self.inner.role != Role::Writer {
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
            let snapshot = inner.snapshot()?;
            return inner.get(&snapshot, table, family, row, qualifier);
        }
        // The seqno first, then the view: a seqno is only visible once the view holding its
        // memtables is published.
        let seqno = inner.shared.shm.visible_seqno();
        let view = inner.shared.view.load();
        let now = inner.shared.vfs.now_micros();
        get_in(&view, seqno, now, table, family, row, qualifier, || {
            Arc::clone(&view)
        })
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
        read::read_row(snapshot, table, row, &families, spec, now)
    }

    /// Starts an ordered scan.
    pub fn scan(&self, snapshot: &Snapshot, table: TableId, spec: ScanSpec) -> Result<ScanCursor> {
        let families = families_in_order(&snapshot.view, table, &spec.read.families)?;
        let now = self.inner.shared.vfs.now_micros();
        Ok(ScanCursor::new(
            snapshot.clone(),
            table,
            spec,
            families,
            now,
        ))
    }

    // ---- maintenance ----

    /// Freezes and flushes every memtable; returns when the SSTs are in the manifest. A
    /// memtable the arena has no chunk to replace (snapshots pin the retired ones) waits as
    /// a write stall does, and the flush fails with [`Error::Busy`] when no chunk frees up
    /// in time (D124, D126; issue #116).
    pub fn flush(&self) -> Result<()> {
        self.inner.flush_pending()?.wait()
    }

    /// Compacts every family of `table` (or all tables) fully: flushes, then merges every
    /// level into the last one.
    pub fn compact(&self, table: Option<TableId>) -> Result<()> {
        self.inner.compact_pending(table)?.wait()
    }

    /// Writes a consistent single-file copy to `dest` while writers run: everything visible
    /// at a snapshot taken now, as a clean database that opens without WAL replay.
    pub fn backup(&self, dest: &Path) -> Result<()> {
        let inner = &self.inner;
        if inner.role != Role::Writer {
            return Err(Error::ReadOnly);
        }
        inner.check_open()?;
        let snapshot = inner.snapshot()?;
        crate::maintenance::backup(&inner.shared, &snapshot, dest)
    }

    /// Relocates tail extents the manifest names and truncates the file (decision D60).
    /// Returns the bytes released.
    pub fn shrink(&self) -> Result<u64> {
        let inner = &self.inner;
        if inner.role != Role::Writer {
            return Err(Error::ReadOnly);
        }
        inner.check_open()?;
        crate::maintenance::shrink(&inner.shared)
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
            m.stalls.0 += s.stalls.load(Ordering::Relaxed);
            m.stalls.1 += s.stall_nanos.load(Ordering::Relaxed);
        }
        m
    }

    /// Stops the shards: every shard finishes its in-flight groups and cross-shard commits,
    /// flushes its memtables, checkpoints and syncs its stream; the last one records the
    /// clean close and, if no reader is attached, removes the WAL files and the
    /// shared-memory region, leaving one file at rest.
    ///
    /// In engine-owned mode this waits for the shards and returns the final result. In
    /// application-owned mode it returns at once after telling every shard to close: the
    /// application keeps driving each [`EngineShard::run_once`] until it returns `false`
    /// (the last shard to finish records the clean close), then drops the shards.
    pub fn close(&self) -> Result<()> {
        self.inner.close(true)
    }

    // ---- test hooks ----

    /// `flush` as a future (test harnesses that drive the shards themselves).
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn flush_pending(&self) -> Result<PendingMaintenance> {
        self.inner.flush_pending()
    }

    /// `compact` as a future.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn compact_pending(&self, table: Option<TableId>) -> Result<PendingMaintenance> {
        self.inner.compact_pending(table)
    }

    /// Every entry of every table in `snapshot`'s view, raw (no resolution), in key order
    /// per `(table, family)`.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn raw_entries(&self, snapshot: &Snapshot) -> Result<Vec<RawEntry>> {
        use pigeonhole_compaction::MergingCursor;
        use pigeonhole_format::Cursor;
        let view = &snapshot.view;
        let mut out = Vec::new();
        let all = pigeonhole_format::scan::ScanFilter::all();
        for t in view.catalog.tablets() {
            // Children of a split share SSTs holding their siblings' rows too.
            let (start, end) = crate::read::clamp_to_tablet(&t, None, None)?;
            for family in view.catalog.family_ids_of(t.table) {
                let sources = view.scan_sources(t.shard, t.id, family, &all, None, None)?;
                let mut merged = MergingCursor::new(sources);
                match &start {
                    Some(s) => merged.seek(s)?,
                    None => merged.seek_to_first()?,
                }
                while merged.valid() && end.as_deref().is_none_or(|e| merged.key() < e) {
                    out.push(RawEntry {
                        table: t.table,
                        family,
                        key: merged.key().to_vec(),
                        value: merged.value().to_vec(),
                    });
                    merged.next()?;
                }
            }
        }
        Ok(out)
    }

    /// Bytes the pager holds (allocated or retired) that neither the catalog nor the
    /// manifest root references: output in flight, extents awaiting reclamation, or leaked.
    /// Zero once idle and reclaimed (test hook).
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn unreferenced_bytes(&self) -> u64 {
        let shared = &self.inner.shared;
        let root = shared
            .manifest
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .root();
        let mut live: Vec<_> = shared.view.load().catalog.data_extents();
        live.extend(root.snapshot);
        live.extend(root.log);
        live.sort();
        live.dedup();
        let referenced: u64 = live.iter().map(|e| e.len()).sum();
        let stats = shared.pager.stats();
        (stats.allocated_bytes + stats.retired_bytes).saturating_sub(referenced)
    }

    /// Splits the tablet of `table` holding row `at` at `at`; both halves stay on its shard.
    /// Fails with `InvalidArgument` when `at` is the tablet's first row.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn split_tablet_pending(&self, table: TableId, at: &[u8]) -> Result<PendingMaintenance> {
        let (tablet, shard) = self.inner.tablet_at(table, at)?;
        self.inner.tablet_op(
            shard,
            crate::shard::TabletOpKind::Split {
                tablet,
                keys: vec![at.to_vec()],
                owners: vec![shard, shard],
            },
        )
    }

    /// Moves the tablet of `table` holding `row` to shard `to`.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn move_tablet_pending(
        &self,
        table: TableId,
        row: &[u8],
        to: u16,
    ) -> Result<PendingMaintenance> {
        let (tablet, shard) = self.inner.tablet_at(table, row)?;
        self.inner.tablet_op(
            shard,
            crate::shard::TabletOpKind::Move {
                tablet,
                to: ShardId(to),
            },
        )
    }

    /// Merges the tablet of `table` holding `row` with its right neighbour (both must be on
    /// one shard).
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn merge_tablets_pending(&self, table: TableId, row: &[u8]) -> Result<PendingMaintenance> {
        let view = self.inner.shared.view.load_full();
        let list = view.tablets.tablets_of(table);
        let i = list
            .iter()
            .position(|t| {
                t.start.as_slice() <= row && t.end.as_ref().is_none_or(|e| row < e.as_slice())
            })
            .ok_or_else(|| Error::TableNotFound(format!("table {}", table.0)))?;
        let (Some(l), Some(r)) = (list.get(i), list.get(i + 1)) else {
            return Err(Error::InvalidArgument("no right neighbour".to_owned()));
        };
        self.inner.tablet_op(
            l.shard,
            crate::shard::TabletOpKind::Merge {
                left: l.id,
                right: r.id,
            },
        )
    }

    /// Runs every shard's balancer now (whatever interval is configured); resolves once the
    /// splits, moves or merges it started are done.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn balance_pending(&self) -> Result<PendingMaintenance> {
        self.inner.check_open()?;
        let mut waiters = Vec::with_capacity(self.inner.shared.shards);
        for i in 0..self.inner.shared.shards {
            let (tx, rx) = completion();
            self.inner
                .shared
                .submitter(ShardId(i as u16))
                .submit(ShardMsg::Balance { reply: Some(tx) })?;
            waiters.push(rx);
        }
        Ok(PendingMaintenance {
            waiters,
            rounds: None,
        })
    }

    /// The largest default-timestamp floor of any shard (including floors raised by shards
    /// handing over tablets): the next default timestamp anywhere is above it.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn max_ts_floor(&self) -> pigeonhole_format::Timestamp {
        let shared = &self.inner.shared;
        shared
            .ts_floors
            .iter()
            .chain(&shared.ts_raises)
            .map(|f| f.0.load(Ordering::Acquire))
            .max()
            .unwrap_or(0)
    }

    /// Splits, merges and moves completed since open, summed over shards.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn tablet_changes(&self) -> (u64, u64, u64) {
        let mut out = (0, 0, 0);
        for m in &self.inner.shared.metrics {
            out.0 += m.splits.load(Ordering::Relaxed);
            out.1 += m.merges.load(Ordering::Relaxed);
            out.2 += m.moves.load(Ordering::Relaxed);
        }
        out
    }

    /// The compactions committed since the last call (or since open).
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn take_compactions(&self) -> Vec<crate::compact::CompactionRecord> {
        std::mem::take(
            &mut *self
                .inner
                .shared
                .compactions
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    /// The WAL records appended since the last call (or since open), in append order.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn take_appended(&self) -> Vec<crate::shard::AppendedRecord> {
        std::mem::take(
            &mut *self
                .inner
                .shared
                .appended
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    /// While `park` is set, a background manifest commit whose root commit completed waits
    /// before it publishes and answers; clearing it wakes the parked commit (test hook).
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn park_manifest_commits(&self, park: bool) {
        let shared = &self.inner.shared;
        shared.manifest_park.store(park, Ordering::Release);
        if !park {
            let parked = shared
                .manifest_parked
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(w) = parked {
                w.wake();
            }
        }
    }

    /// Whether a background manifest commit is parked (test hook).
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn manifest_commit_parked(&self) -> bool {
        self.inner
            .shared
            .manifest_parked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    /// Whether every shard has closed and the final close waits for the manifest writer
    /// (test hook).
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn final_close_pending(&self) -> bool {
        self.inner
            .shared
            .close
            .final_pending
            .load(Ordering::Acquire)
    }

    /// Commits an empty manifest delta from this thread while a second request lands in
    /// the window between the drain's last `begin` and the release of the writer's
    /// exclusion (as a shard's submit would, whose pump then leaves). Returns whether that
    /// second request was committed too, which the release-then-re-check rule guarantees.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn probe_manifest_release_window(&self) -> Result<bool> {
        use std::task::{Context, Poll, Waker};
        let shared = &self.inner.shared;
        shared.manifest_race.store(true, Ordering::Release);
        manifest::commit_from_thread(shared, manifest::ReqKind::Edits(Vec::new()))?;
        let waiter = shared
            .manifest_race_waiter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let Some(mut waiter) = waiter else {
            return Err(Error::Corruption("the race window was not entered".into()));
        };
        let mut cx = Context::from_waker(Waker::noop());
        Ok(match std::pin::Pin::new(&mut waiter).poll(&mut cx) {
            Poll::Ready(Some(Ok(_))) => true,
            Poll::Ready(Some(Err(e))) => return Err(e),
            Poll::Ready(None) | Poll::Pending => false,
        })
    }

    /// Reads the manifest of a database no writer has open.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn inspect_manifest(vfs: &pigeonhole_io::VfsRef, path: &Path) -> Result<ManifestInfo> {
        let opened = Pager::open(vfs, path, false)?;
        let clean = opened.clean_shutdown();
        let (catalog, _) = manifest::load(&opened, 1, Arc::new(MergeRegistry::default()))?;
        Ok(ManifestInfo {
            version: opened.root().manifest_version,
            checkpoints: catalog.checkpoints.clone(),
            flushed: catalog.flushed.clone(),
            tablets: catalog.tablets().iter().map(|t| (t.id, t.table)).collect(),
            tablet_ranges: catalog
                .tablets()
                .into_iter()
                .map(|t| (t.id, t.table, t.start, t.end))
                .collect(),
            clean,
        })
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
fn flush_recovered(
    shared: &Shared,
    catalog: &mut Catalog,
    states: &mut [ShardState],
    recoveries: &[(StreamId, Recovery)],
    shards: usize,
    options: &EngineOptions,
) -> Result<()> {
    let mut edits = Vec::new();
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
            for meta in sink.outputs.drain(..) {
                edits.push(Edit::AddSst {
                    tablet,
                    family,
                    level: 0,
                    meta,
                });
            }
            edits.push(Edit::SetFlushed {
                tablet,
                family,
                seqno: max_seqno,
            });
        }
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
    let _ = options;
    edits.push(catalog.counters_edit());
    for e in &edits {
        catalog.apply(e, shards)?;
    }
    let mut writer = shared
        .manifest
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    writer.commit(catalog, &edits).inspect_err(|_| {
        for e in &edits {
            if let Edit::AddSst { meta, .. } = e {
                shared.pager.abandon(meta.extent);
            }
        }
    })?;
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
            "the memtable budget is too small to hold the WAL's unflushed data; reopen with a \
             larger memtable_budget"
                .to_owned(),
        ),
        e => e,
    }
}

/// The families of `table` to read: in creation order, or as listed (duplicates and unknown
/// ids dropped), decision D39.
fn families_in_order(view: &View, table: TableId, listed: &[FamilyId]) -> Result<Vec<FamilyId>> {
    let info = view
        .catalog
        .table(table)
        .ok_or_else(|| Error::TableNotFound(format!("table {}", table.0)))?;
    if listed.is_empty() {
        return Ok(info.families.iter().map(|f| f.id).collect());
    }
    let mut out = Vec::with_capacity(listed.len());
    for &f in listed {
        if !out.contains(&f) && info.families.iter().any(|x| x.id == f) {
            out.push(f);
        }
    }
    Ok(out)
}

/// A maintenance future: the replies of every shard.
#[cfg(not(feature = "test-hooks"))]
#[derive(Debug)]
pub(crate) struct PendingMaintenance {
    waiters: Vec<Waiter<Result<()>>>,
    rounds: Option<CompactRounds>,
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

#[cfg(feature = "test-hooks")]
impl Inner {
    /// The tablet of `table` holding `row` and its owner.
    fn tablet_at(&self, table: TableId, row: &[u8]) -> Result<(TabletId, ShardId)> {
        self.check_open()?;
        self.shared
            .view
            .load()
            .tablets()
            .route(table, row)
            .ok_or_else(|| Error::TableNotFound(format!("table {}", table.0)))
    }

    /// Sends a tablet change to `shard`, the tablets' owner.
    fn tablet_op(
        &self,
        shard: ShardId,
        op: crate::shard::TabletOpKind,
    ) -> Result<PendingMaintenance> {
        if self.role != Role::Writer {
            return Err(Error::ReadOnly);
        }
        self.check_open()?;
        let (tx, rx) = completion();
        self.shared.submitter(shard).submit(ShardMsg::TabletOp {
            op,
            reply: Some(tx),
        })?;
        Ok(PendingMaintenance {
            waiters: vec![rx],
            rounds: None,
        })
    }
}

impl Inner {
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
            let _ = self.reader_refresh();
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
        if self.role != Role::Writer {
            return Err(Error::ReadOnly);
        }
        self.check_open()?;
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
            if catalog.table(table).is_none() {
                return Err(Error::TableNotFound(format!("table {}", table.0)));
            }
            Ok(vec![Edit::DropTable { table }])
        })?;
        self.shared.broadcast(|| ShardMsg::DropTablets {
            tablets: dropped.clone(),
        });
        Ok(())
    }

    /// Validates and routes a batch: per-shard parts in first-appearance order.
    fn route(&self, mut batch: WriteBatch, view: &View) -> Result<(BatchBuilder, Vec<ShardId>)> {
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
        let mut shards: Vec<ShardId> = Vec::new();
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
            if m.kind == Kind::Merge {
                match meta.merge {
                    MergeKind::None => {
                        return Err(Error::InvalidArgument(format!(
                            "family {} has no merge operator",
                            m.family.0
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
            if m.value.len() > self.max_value + 1 {
                return Err(Error::ValueTooLarge);
            }
            let Some((_, shard)) = view.tablets().route(m.table, m.row) else {
                return Err(Error::TableNotFound(format!("table {}", m.table.0)));
            };
            if !shards.contains(&shard) {
                shards.push(shard);
            }
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
        if self.role != Role::Writer {
            return Err(Error::ReadOnly);
        }
        self.check_open()?;
        let durability = durability.unwrap_or_else(|| self.shared.default_durability());
        // One tablet map for the whole routing: its version goes with a cross-shard commit,
        // which a participant moving a tablet refuses and the coordinator retries.
        let view = self.shared.view.load();
        let (builder, mut shards) = self.route(batch, &view)?;
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
        let shm = self.shared.shm.clone();
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
                    predicate,
                    commit_ts: None,
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
                .submit(ShardMsg::Coordinate(CoordinateReq {
                    parts,
                    durability,
                    reply: tx,
                    submitted_at,
                    validate,
                    map_version: view.tablets.version(),
                    commit_ts: None,
                    epoch: 0,
                }))?;
        }
        Ok(PendingCommit {
            waiter,
            shm,
            shared: Arc::clone(&self.shared),
            resolved: None,
        })
    }

    fn check_and_mutate(
        &self,
        table: TableId,
        row: &[u8],
        predicate: &Predicate,
        batch: WriteBatch,
        durability: Option<Durability>,
    ) -> Result<(bool, Option<CommitInfo>)> {
        if self.role != Role::Writer {
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
        self.shared
            .submitter(shard)
            .submit(ShardMsg::Commit(CommitReq {
                bytes: builder,
                durability,
                reply: Reply::Check(tx),
                submitted_at,
                validate: None,
                predicate: Some((table, row.to_vec(), predicate.clone())),
                commit_ts: None,
            }))?;
        let (applied, info) = rx.wait().unwrap_or(Err(Error::Closed))?;
        if let Some(info) = info {
            while self.shared.shm.visible_seqno() < info.seqno {
                std::thread::yield_now();
            }
        }
        Ok((applied, info))
    }

    pub(crate) fn snapshot(&self) -> Result<Snapshot> {
        if self.reader.is_some() {
            return self.reader_snapshot();
        }
        let seqno = self.shared.shm.visible_seqno();
        let view = self.shared.view.load_full();
        Ok(Snapshot {
            seqno,
            view,
            _live: None,
            _pin: Some(Arc::new(SeqnoPin::new(&self.shared.live_seqnos, seqno))),
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
        get_in(
            &snapshot.view,
            snapshot.seqno,
            now,
            table,
            family,
            row,
            qualifier,
            || Arc::clone(&snapshot.view),
        )
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
        })
    }

    fn compact_pending(&self, table: Option<TableId>) -> Result<PendingMaintenance> {
        if self.role != Role::Writer {
            return Err(Error::ReadOnly);
        }
        self.check_open()?;
        let rounds = CompactRounds::new(&self.shared, table);
        let waiters = compact_round(&self.shared, table)?;
        Ok(PendingMaintenance { waiters, rounds })
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
        if let Some(submitters) = self.shared.submitters.get() {
            for s in submitters {
                let _ = s.submit(ShardMsg::Close);
            }
        }
        // Application-owned shards are driven by the application's threads, possibly the
        // caller's own, so the close never blocks there: the shards finish as they are run.
        let result = if wait && !self.application_owned {
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
                rt.shutdown()?;
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
            shm.1.unpin();
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

    /// Reloads the manifest if the region names a newer version.
    fn reader_refresh(&self) -> Result<()> {
        let r = self.reader.as_ref().expect("reader");
        let version = r
            .shm
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .0
            .manifest_version();
        let mut cached = r.catalog.lock().unwrap_or_else(PoisonError::into_inner);
        if cached.0 == version {
            return Ok(());
        }
        let opened = Pager::open(&self.shared.vfs, &self.path, false)?;
        let (catalog, _) = manifest::load(&opened, r.shards, Arc::clone(&r.registry))?;
        *cached = (
            opened.root().manifest_version,
            Arc::new(catalog),
            opened.file().clone(),
        );
        Ok(())
    }

    fn reader_snapshot(&self) -> Result<Snapshot> {
        let r = self.reader.as_ref().expect("reader");
        let mut guard = r.shm.lock().unwrap_or_else(PoisonError::into_inner);
        if guard.0.is_stale() {
            let shm = guard.0.reattach(&self.shared.vfs, &r.file)?;
            let slot = shm.claim_reader_slot(self.shared.vfs.current_process())?;
            *guard = (shm, slot, false);
            *r.view.lock().unwrap_or_else(PoisonError::into_inner) = None;
        }
        let (shm, slot, pinned) = &mut *guard;
        // The pin protects the oldest live snapshot and every newer view. While snapshots
        // of this process are alive it stays put; once none is left it moves forward to this
        // one, so a long-lived reader never blocks reclamation for ever.
        let seqno = shm.visible_seqno();
        let seqno = if *pinned && r.live.load(Ordering::Acquire) > 0 {
            seqno
        } else {
            let (s, _) = slot.pin(seqno, shm.view_version());
            *pinned = true;
            s
        };
        r.live.fetch_add(1, Ordering::AcqRel);
        let live = Some(Arc::new(LiveSnapshot {
            count: Arc::clone(&r.live),
        }));
        let record = shm.read_view()?;
        let shm = shm.clone();
        drop(guard);
        self.reader_refresh()?;
        let (catalog, file) = {
            let c = r.catalog.lock().unwrap_or_else(PoisonError::into_inner);
            (Arc::clone(&c.1), c.2.clone())
        };
        let mut cached = r.view.lock().unwrap_or_else(PoisonError::into_inner);
        let view = match &*cached {
            Some(v) if v.version == record.view_version && Arc::ptr_eq(&v.catalog, &catalog) => {
                Arc::clone(v)
            }
            _ => {
                let prev = cached.as_ref().map(|v| Arc::clone(&v.ssts));
                let view = Arc::new(view_from_record(
                    &shm,
                    &record,
                    catalog,
                    prev.as_deref(),
                    file,
                    Arc::clone(&self.shared.cache),
                )?);
                *cached = Some(Arc::clone(&view));
                view
            }
        };
        Ok(Snapshot {
            seqno,
            view,
            _live: live,
            _pin: None,
        })
    }
}

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
    let mut pieces: Vec<HashMap<(TabletId, FamilyId), Arc<MemSet>>> =
        (0..shm.shard_count()).map(|_| HashMap::new()).collect();
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

fn check_merge_operator(
    options: &FamilyOptions,
    registry: &MergeRegistry,
    allow_unregistered: bool,
) -> Result<()> {
    let name = options.merge_operator.as_str();
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
/// while shards[0].run_once(u64::MAX) {}
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
    /// Returns whether work remains.
    pub fn run_once(&mut self, deadline_nanos: u64) -> bool {
        self.driver
            .as_mut()
            .is_some_and(|d| d.run_once(deadline_nanos))
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
        let submitted_at = engine.shared.vfs.monotonic_nanos();
        let (tx, waiter) = completion();
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
                },
                ctx,
            )
        });
        Ok(PendingCommit {
            waiter,
            shm: engine.shared.shm.clone(),
            shared: Arc::clone(&engine.shared),
            resolved: None,
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
            let mut state = driver.shutdown();
            state.abandon(&self.engine.shared);
        }
    }
}
