use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use arc_swap::ArcSwap;
use pigeonhole_format::manifest::{Edit, FamilyOptions};
use pigeonhole_format::shm::ViewRecord;
use pigeonhole_format::wal::{BatchBuilder, WalRecord};
use pigeonhole_format::{
    Durability, FamilyId, Kind, ManifestVersion, Seqno, StreamId, TableId, TabletId,
};
use pigeonhole_io::{ErrorKind, FileRef, OpenOptions};
use pigeonhole_memtable::{ArenaRegion, MemtableReader, ShardArena};
use pigeonhole_pager::Pager;
use pigeonhole_runtime::{Runtime, RuntimeConfig, ShardDriver, ShardId, Submitter, completion};
use pigeonhole_shm::{Presence, ReaderSlot, Role as ShmRole, ShmConfig, ShmRegion, WriterLock};
use pigeonhole_wal::{Recovery, WalStream, discover_streams};

use crate::catalog::{Catalog, MergeKind};
use crate::flush::FlushBackend;
use crate::manifest::{self, ManifestWriter};
use crate::read::{self, sources_for};
use crate::resolve::{Merge, Resolver};
use crate::shard::{
    CloseState, CommitReq, CoordinateReq, Locks, Padded, Reply, ShardMetrics, ShardMsg, ShardState,
    Shared, VisibilityWaiters, bucket_floor,
};
use crate::snapshot::{LiveSnapshot, MemSet, ShardMems, TabletEntry, TabletMap, View};
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
    /// Write stalls (token-bucket waits) and total stalled nanoseconds.
    pub stalls: (u64, u64),
    /// Block cache hits and misses.
    pub block_cache: (u64, u64),
}

/// The lock-page byte range of the superblock-less file check (decision D59).
const INTERRUPTED_CREATE_MAX: u64 = 64 * 1024;

/// A reader process's attachment to the writer's shared memory.
struct ReaderState {
    file: FileRef,
    presence: Mutex<Option<Presence>>,
    shm: Mutex<(ShmRegion, ReaderSlot, bool)>,
    /// `(manifest version, catalog)` as last loaded.
    catalog: Mutex<(ManifestVersion, Arc<Catalog>)>,
    /// The last view built from the region, by view version.
    view: Mutex<Option<Arc<View>>>,
    shards: usize,
    /// Snapshots of this process still alive; the pin moves forward when it drops to zero.
    live: Arc<AtomicUsize>,
}

/// Everything behind an [`Engine`] (and the handle a [`Txn`] keeps).
pub(crate) struct Inner {
    pub(crate) shared: Arc<Shared>,
    role: Role,
    options: EngineOptions,
    path: PathBuf,
    runtime: Mutex<Option<Runtime<ShardState>>>,
    application_owned: bool,
    /// Serializes catalog changes (each is a manifest commit).
    catalog_lock: Mutex<()>,
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
    /// it returns `false`, then drop it: the shards finish their in-flight work and sync
    /// their streams as part of the close.
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
        let (mut catalog, manifest_extents) = manifest::load(&opened, shards)?;
        catalog.reassign(shards);
        let mut live = manifest_extents;
        live.extend(catalog.data_extents());
        let pager = Arc::new(opened.finish(live)?);
        if catalog.has_unknown_merge && !options.allow_unregistered_merge {
            let name = catalog
                .tables()
                .flat_map(|t| t.families.iter())
                .map(|f| f.options.merge_operator.clone())
                .find(|n| !n.is_empty() && n != crate::catalog::I64_ADD)
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

        // 4. Shard states over the arenas.
        let tablets = Arc::new(TabletMap::build(1, &catalog.tablets()));
        let manifest_version = pager.root().manifest_version;
        let empty_view = Arc::new(View {
            version: 0,
            manifest_version,
            tablets: Arc::new(TabletMap::default()),
            catalog: Arc::new(Catalog::default()),
            mems: (0..shards)
                .map(|_| Arc::new(ShardMems::default()))
                .collect(),
        });
        let shared = Arc::new(Shared {
            vfs: Arc::clone(&vfs),
            shm: shm.clone(),
            shards,
            view: ArcSwap::new(empty_view),
            view_lock: Mutex::new(0),
            manifest: Mutex::new(ManifestWriter::new(Arc::clone(&pager))),
            locks: Mutex::new(Some(Locks {
                _writer: writer_lock,
                presence,
            })),
            default_durability: AtomicU8::new(options.durability as u8),
            closed: AtomicBool::new(false),
            pager_poisoned: AtomicBool::new(false),
            close: CloseState {
                remaining: AtomicUsize::new(shards),
                done: Mutex::new(None),
                failed: AtomicBool::new(false),
            },
            metrics: (0..shards).map(|_| ShardMetrics::default()).collect(),
            ts_floors: (0..shards)
                .map(|_| Padded(AtomicU64::new(catalog.counters.ts_floor)))
                .collect(),
            waiters: VisibilityWaiters::default(),
            memtable_freeze_bytes: options.memtable_freeze_bytes.max(1),
            flush: FlushBackend::default(),
            submitters: std::sync::OnceLock::new(),
            shm_dir: options.shm_dir.clone(),
            identity,
        });
        let chunk = chunk_size(options.memtable_budget);
        let mut states: Vec<ShardState> = Vec::with_capacity(shards);
        for i in 0..shards {
            let (region, offset, len) = shm.arena(i as u32);
            let arena = ArenaRegion::new(region, offset, len)?;
            states.push(ShardState::new(
                ShardId(i as u16),
                Arc::clone(&shared),
                arena,
                chunk,
                Arc::clone(&tablets),
                catalog.counters.ts_floor,
            ));
        }

        // 5. Replay every WAL stream (whatever the shard count was), then resolve PREPAREs.
        let mut max_seqno = 0;
        let mut recoveries: Vec<(StreamId, Recovery)> = Vec::new();
        let mut stashed: Vec<(StreamId, Seqno, u64, StreamId, Vec<u8>)> = Vec::new();
        // `(coordinator stream, seqno) -> participant streams` of every COMMIT decision.
        let mut commits: HashMap<(StreamId, Seqno), Vec<StreamId>> = HashMap::new();
        for stream in discover_streams(&vfs, path)? {
            let checkpoint = catalog
                .checkpoints
                .get(&stream)
                .copied()
                .unwrap_or_default();
            let mut rec = Recovery::open(&vfs, path, stream, db_id, checkpoint)?;
            while let Some((_, record)) = rec.next_record()? {
                match record {
                    WalRecord::Batch {
                        seqno,
                        commit_ts,
                        batch,
                    } => {
                        for s in &mut states {
                            s.replay(batch.as_bytes(), seqno, commit_ts)
                                .map_err(replay_error)?;
                        }
                    }
                    WalRecord::Prepare {
                        seqno,
                        commit_ts,
                        coordinator,
                        batch,
                    } => stashed.push((
                        stream,
                        seqno,
                        commit_ts,
                        coordinator,
                        batch.as_bytes().to_vec(),
                    )),
                    WalRecord::Commit {
                        seqno,
                        participants,
                    } => {
                        commits.insert((stream, seqno), participants.iter().collect());
                    }
                }
            }
            max_seqno = max_seqno.max(rec.max_seqno());
            recoveries.push((stream, rec));
        }
        // A decided commit is applied only if every participant its COMMIT names still
        // holds its PREPARE: all or nothing. A `GroupSync`/`Sync` commit's prepares were
        // durable before the COMMIT was written, so this never discards one; a `Buffered`
        // commit whose prepare a power loss took is dropped whole rather than in part.
        let prepared: HashSet<(StreamId, Seqno)> = stashed
            .iter()
            .map(|(stream, seqno, ..)| (*stream, *seqno))
            .collect();
        for (_, seqno, commit_ts, coordinator, bytes) in &stashed {
            let Some(participants) = commits.get(&(*coordinator, *seqno)) else {
                continue;
            };
            if !participants
                .iter()
                .all(|p| prepared.contains(&(*p, *seqno)))
            {
                continue;
            }
            for s in &mut states {
                s.replay(bytes, *seqno, *commit_ts).map_err(replay_error)?;
            }
        }
        let next = first_seqno(catalog.counters.seqno_ceiling, max_seqno);
        let current = shm.next_seqno();
        if next > current {
            shm.reserve_seqnos(next - current);
        }
        let mut have_wal = vec![false; shards];
        for (stream, rec) in recoveries {
            let i = stream.0 as usize;
            if i < shards {
                states[i].set_wal(Box::new(rec.into_stream(options.wal)?));
                have_wal[i] = true;
            }
            // Streams beyond the shard count are replayed every open and kept until a flush
            // can persist them (decision D20; Milestone B).
        }
        for (i, have) in have_wal.iter().enumerate() {
            if !have {
                let wal = WalStream::create(&vfs, path, StreamId(i as u32), db_id, options.wal)?;
                states[i].set_wal(Box::new(wal));
            }
        }

        // The timestamp floor starts above every replayed commit (D11).
        for (i, s) in states.iter().enumerate() {
            shared.ts_floors[i].0.store(s.ts_floor(), Ordering::Release);
        }

        // 6. The first view: tablets, recovered memtables, the manifest version.
        let mems: Vec<Arc<ShardMems>> = states
            .iter()
            .map(|s| {
                Arc::new(ShardMems {
                    map: s.mem_sets().into_iter().collect(),
                })
            })
            .collect();
        let catalog = Arc::new(catalog);
        let first_view = Arc::new(View {
            version: 1,
            manifest_version,
            tablets: Arc::clone(&tablets),
            catalog: Arc::clone(&catalog),
            mems,
        });
        shm.publish_view(&first_view.to_record())?;
        shm.set_manifest_version(manifest_version);
        shared.view.store(Arc::clone(&first_view));
        *shared
            .view_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = 1;

        // 7. The runtime.
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
            catalog_lock: Mutex::new(()),
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
        let (catalog, _) = manifest::load(&opened, shards)?;
        let manifest_version = opened.root().manifest_version;
        let pager = Arc::new(opened.finish([])?);
        // The read-only pager is kept by the manifest writer (which never commits here).
        let empty_view = Arc::new(View {
            version: 0,
            manifest_version,
            tablets: Arc::new(TabletMap::default()),
            catalog: Arc::new(Catalog::default()),
            mems: (0..shards)
                .map(|_| Arc::new(ShardMems::default()))
                .collect(),
        });
        let shared = Arc::new(Shared {
            vfs: Arc::clone(&vfs),
            shm: shm.clone(),
            shards,
            view: ArcSwap::new(empty_view),
            view_lock: Mutex::new(0),
            manifest: Mutex::new(ManifestWriter::new(Arc::clone(&pager))),
            locks: Mutex::new(None),
            default_durability: AtomicU8::new(options.durability as u8),
            closed: AtomicBool::new(false),
            pager_poisoned: AtomicBool::new(false),
            close: CloseState::default(),
            metrics: Vec::new(),
            ts_floors: Vec::new(),
            waiters: VisibilityWaiters::default(),
            memtable_freeze_bytes: options.memtable_freeze_bytes.max(1),
            flush: FlushBackend::default(),
            submitters: std::sync::OnceLock::new(),
            shm_dir: options.shm_dir.clone(),
            identity,
        });
        let reader = ReaderState {
            file,
            presence: Mutex::new(Some(presence)),
            shm: Mutex::new((shm, slot, false)),
            catalog: Mutex::new((manifest_version, Arc::new(catalog))),
            view: Mutex::new(None),
            shards,
            live: Arc::new(AtomicUsize::new(0)),
        };
        let inner = Arc::new(Inner {
            shared,
            role: Role::Reader,
            options,
            path: path.to_path_buf(),
            runtime: Mutex::new(None),
            application_owned: false,
            catalog_lock: Mutex::new(()),
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
    /// any crash. The same holds for a commit whose WAL sync fails after its records were
    /// written and applied: the caller gets an `Io` error, the shard refuses further writes
    /// until the database is reopened, and the data stays visible until then (it may or
    /// may not survive the reopen).
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

    /// Freezes and flushes every memtable; returns when the SSTs are in the manifest.
    ///
    /// Until SST flushes land (Milestone B) this freezes every non-empty active memtable and
    /// retains the frozen ones in every view; nothing is written to the page file.
    pub fn flush(&self) -> Result<()> {
        let inner = &self.inner;
        if inner.role != Role::Writer {
            return Err(Error::ReadOnly);
        }
        inner.check_open()?;
        let mut waiters = Vec::with_capacity(inner.shared.shards);
        for i in 0..inner.shared.shards {
            let (tx, rx) = completion();
            inner
                .shared
                .submitter(ShardId(i as u16))
                .submit(ShardMsg::Freeze { reply: tx })?;
            waiters.push(rx);
        }
        for rx in waiters {
            rx.wait().unwrap_or(Err(Error::Closed))?;
        }
        Ok(())
    }

    /// Compacts every family of `table` (or all tables) fully.
    pub fn compact(&self, table: Option<TableId>) -> Result<()> {
        let _ = table;
        Err(Error::Unsupported(
            "compaction lands with pigeonhole-compaction (Milestone B)",
        ))
    }

    /// Writes a consistent single-file copy to `dest` while writers run.
    pub fn backup(&self, dest: &Path) -> Result<()> {
        let _ = dest;
        Err(Error::Unsupported("online backup lands with Milestone B"))
    }

    /// Relocates tail extents and truncates the file.
    pub fn shrink(&self) -> Result<u64> {
        Err(Error::Unsupported("online shrink lands with Milestone B"))
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
            m.stalls.0 += s.stalls.load(Ordering::Relaxed);
            m.stalls.1 += s.stall_nanos.load(Ordering::Relaxed);
        }
        m
    }

    /// Stops the shards, flushes nothing extra, and if this is the last process checkpoints
    /// and removes the WAL files and the shared-memory region.
    ///
    /// Every shard finishes its in-flight groups and cross-shard commits, syncs its stream,
    /// and the last one records the clean close and removes the shared-memory region when no
    /// reader is attached. WAL files stay until SST flushes exist (Milestone B): the data
    /// in them has nowhere else to go yet.
    ///
    /// In engine-owned mode this waits for the shards and returns the final result. In
    /// application-owned mode it returns at once after telling every shard to close: the
    /// application keeps driving each [`EngineShard::run_once`] until it returns `false`
    /// (the last shard to finish records the clean close), then drops the shards.
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

/// A point get through `view` at `seqno`.
#[allow(clippy::too_many_arguments)]
fn get_in(
    view: &View,
    seqno: Seqno,
    now: u64,
    table: TableId,
    family: FamilyId,
    row: &[u8],
    qualifier: &[u8],
    pin: impl FnOnce() -> Arc<View>,
) -> Result<Option<CellData>> {
    let Some((tablet, shard)) = view.tablets().route(table, row) else {
        return Err(Error::TableNotFound(format!("table {}", table.0)));
    };
    let Some(meta) = view.catalog.family(family) else {
        return Err(Error::FamilyNotFound(format!("family {}", family.0)));
    };
    if meta.table != table {
        return Err(Error::FamilyNotFound(format!("family {}", family.0)));
    }
    let sources = sources_for(view, shard, tablet, family);
    if sources.is_empty() {
        return Ok(None);
    }
    let opts = ReadSpec {
        versions: 1,
        ..ReadSpec::default()
    }
    .resolve_opts(meta, seqno, now);
    let mut resolver = Resolver::new(Merge::new(sources), opts);
    resolver.seek_column(row, qualifier)?;
    match resolver.next_cell()? {
        Some(cell) => Ok(Some(CellData::from_resolved(&cell, pin))),
        None => Ok(None),
    }
}

impl Inner {
    fn check_open(&self) -> Result<()> {
        if self.closing.load(Ordering::Acquire) || self.shared.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if self.shared.pager_poisoned.load(Ordering::Acquire) {
            return Err(Error::Io(pigeonhole_io::Error::new(
                ErrorKind::Other,
                "a manifest commit failed earlier; reopen the database (decision D58)",
            )));
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

    /// Runs a catalog change: `f` edits a copy of the catalog and returns the edits, which
    /// are committed to the manifest and published in a new view.
    fn catalog_change(
        &self,
        f: impl FnOnce(&mut Catalog) -> Result<Vec<Edit>>,
    ) -> Result<Arc<View>> {
        if self.role != Role::Writer {
            return Err(Error::ReadOnly);
        }
        self.check_open()?;
        let _guard = self
            .catalog_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let current = self.shared.view.load_full();
        let mut catalog = (*current.catalog).clone();
        let mut edits = f(&mut catalog)?;
        // Counters go with every commit: ids, the seqno ceiling and the timestamp floor.
        catalog.counters.seqno_ceiling = self.shared.shm.next_seqno();
        catalog.counters.ts_floor = self
            .shared
            .ts_floors
            .iter()
            .map(|f| f.0.load(Ordering::Acquire))
            .max()
            .unwrap_or(0)
            .max(catalog.counters.ts_floor);
        edits.push(catalog.counters_edit());
        for e in &edits {
            catalog.apply(e, self.shared.shards)?;
        }
        let version = self
            .shared
            .manifest
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .commit(&catalog, &edits)
            .inspect_err(|_| self.shared.pager_poisoned.store(true, Ordering::Release))?;
        let catalog = Arc::new(catalog);
        let tablets = catalog.tablets();
        let live: HashSet<TabletId> = tablets.iter().map(|t| t.id).collect();
        self.shared.publish_view(|cur, view_version| {
            // Pieces of shards that held a dropped tablet are rebuilt without it (rare).
            let mems = cur
                .mems
                .iter()
                .map(|piece| {
                    if piece.map.keys().all(|k| live.contains(&k.0)) {
                        Arc::clone(piece)
                    } else {
                        Arc::new(ShardMems {
                            map: piece
                                .map
                                .iter()
                                .filter(|(k, _)| live.contains(&k.0))
                                .map(|(k, v)| (*k, Arc::clone(v)))
                                .collect(),
                        })
                    }
                })
                .collect();
            View {
                version: view_version,
                manifest_version: version,
                tablets: Arc::new(TabletMap::build(cur.tablets.version() + 1, &tablets)),
                catalog: Arc::clone(&catalog),
                mems,
            }
        })
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
            check_merge_operator(options, self.options.allow_unregistered_merge)?;
        }
        let view = self.catalog_change(|catalog| {
            if catalog.table_by_name(name).is_some() {
                return Err(Error::TableExists(name.to_owned()));
            }
            let table = catalog.alloc_table();
            let mut edits = vec![Edit::CreateTable {
                table,
                name: name.to_owned(),
            }];
            for (fname, options) in families {
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
        check_merge_operator(&options, self.options.allow_unregistered_merge)?;
        let view = self.catalog_change(|catalog| {
            let info = catalog
                .table(table)
                .ok_or_else(|| Error::TableNotFound(format!("table {}", table.0)))?;
            if info.family(name).is_some() {
                return Err(Error::FamilyExists(name.to_owned()));
            }
            let family = catalog.alloc_family();
            Ok(vec![Edit::PutFamily {
                table,
                family,
                name: name.to_owned(),
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
        self.catalog_change(|catalog| {
            if catalog.table(table).is_none() {
                return Err(Error::TableNotFound(format!("table {}", table.0)));
            }
            Ok(vec![Edit::DropTable { table }])
        })?;
        for i in 0..self.shared.shards {
            let _ = self
                .shared
                .submitter(ShardId(i as u16))
                .submit(ShardMsg::DropTablets {
                    tablets: dropped.clone(),
                });
        }
        Ok(())
    }

    /// Validates and routes a batch: per-shard parts in first-appearance order.
    fn route(&self, mut batch: WriteBatch) -> Result<(BatchBuilder, Vec<ShardId>)> {
        let view = self.shared.view.load();
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
                    MergeKind::I64Add => {}
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
        let (builder, mut shards) = self.route(batch)?;
        // Every shard that owns a row the transaction read validates it at PREPARE, so two
        // transactions cannot each read what the other writes (write skew).
        if let Some((_, reads)) = &validate {
            let view = self.shared.view.load();
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
                }))?;
        } else {
            if predicate.is_some() {
                return Err(Error::InvalidArgument(
                    "check_and_mutate touches one row".to_owned(),
                ));
            }
            let parts = split_by_shard(&self.shared.view.load(), &builder, &shards)?;
            let coordinator = shards[0];
            self.shared
                .submitter(coordinator)
                .submit(ShardMsg::Coordinate(CoordinateReq {
                    parts,
                    durability,
                    reply: tx,
                    submitted_at,
                    validate,
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
        let (builder, shards) = self.route(batch)?;
        let shard = match shards.as_slice() {
            [] => self
                .shared
                .view
                .load()
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
        let (catalog, _) = manifest::load(&opened, r.shards)?;
        *cached = (opened.root().manifest_version, Arc::new(catalog));
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
        let catalog = Arc::clone(&r.catalog.lock().unwrap_or_else(PoisonError::into_inner).1);
        let mut cached = r.view.lock().unwrap_or_else(PoisonError::into_inner);
        let view = match &*cached {
            Some(v) if v.version == record.view_version && Arc::ptr_eq(&v.catalog, &catalog) => {
                Arc::clone(v)
            }
            _ => {
                let view = Arc::new(view_from_record(&shm, &record, catalog)?);
                *cached = Some(Arc::clone(&view));
                view
            }
        };
        Ok(Snapshot {
            seqno,
            view,
            _live: live,
        })
    }
}

/// Builds a reader's view from the record published in shared memory.
fn view_from_record(shm: &ShmRegion, record: &ViewRecord, catalog: Arc<Catalog>) -> Result<View> {
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
    Ok(View {
        version: record.view_version,
        manifest_version: record.manifest_version,
        tablets: Arc::new(TabletMap::build(record.view_version, &tablets)),
        catalog,
        mems: pieces
            .into_iter()
            .map(|map| Arc::new(ShardMems { map }))
            .collect(),
    })
}

fn check_merge_operator(options: &FamilyOptions, allow_unregistered: bool) -> Result<()> {
    let name = options.merge_operator.as_str();
    if name.is_empty() || name == crate::catalog::I64_ADD || allow_unregistered {
        Ok(())
    } else {
        Err(Error::UnknownMergeOperator(name.to_owned()))
    }
}

/// Splits a batch into one builder per shard, in `shards` order.
fn split_by_shard(
    view: &View,
    builder: &BatchBuilder,
    shards: &[ShardId],
) -> Result<Vec<(ShardId, Arc<BatchBuilder>)>> {
    let mut parts: Vec<(ShardId, BatchBuilder)> =
        shards.iter().map(|s| (*s, BatchBuilder::new())).collect();
    for m in builder.batch().iter() {
        let m = m?;
        let Some((_, shard)) = view.tablets().route(m.table, m.row) else {
            return Err(Error::TableNotFound(format!("table {}", m.table.0)));
        };
        let part = parts
            .iter_mut()
            .find(|(s, _)| *s == shard)
            .expect("routed while listing shards");
        part.1
            .push(m.table, m.family, m.kind, m.row, m.qualifier, m.ts, m.value)?;
    }
    Ok(parts.into_iter().map(|(s, b)| (s, Arc::new(b))).collect())
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

    /// Commits `batch` inline if every row belongs to this shard; otherwise submits it.
    pub fn commit_local(
        &mut self,
        batch: WriteBatch,
        durability: Option<Durability>,
    ) -> Result<PendingCommit> {
        let engine = Arc::clone(&self.engine);
        engine.check_open()?;
        let durability = durability.unwrap_or_else(|| engine.shared.default_durability());
        let (builder, shards) = engine.route(batch)?;
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
