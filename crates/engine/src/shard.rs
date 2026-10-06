//! The per-shard state and the shard loop body: the group commit on the shard's WAL stream,
//! memtable application with same-commit collapse (D34), the two-phase commit protocol for
//! cross-shard batches, freezing, and the close handshake.
//!
//! One `ShardState` per shard, owned by its thread (or its `EngineShard` driver). Shards
//! share nothing mutable except the documented queues, the shared-memory watermarks and the
//! engine-wide [`Shared`] state (lock-free view publication behind a publish mutex).

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::hash::Hasher;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use arc_swap::ArcSwap;
use pigeonhole_format::key::{encode_key, encode_marker_key, encode_row_prefix, split_suffix};
use pigeonhole_format::wal::{BatchBuilder, BatchRef, Mutation, StreamList, WalRecord};
use pigeonhole_format::{
    Cursor, Durability, FamilyId, Kind, Lsn, Seqno, StreamId, TableId, TabletId, Timestamp,
};
use pigeonhole_io::{ErrorKind, VfsRef};
use pigeonhole_memtable::{ArenaRegion, Memtable, MemtableReader, Retired, ShardArena};
use pigeonhole_runtime::{
    Notifier, ShardContext, ShardHandler, ShardId, Submitter, Task, TaskPoll, TaskWaker,
};
use pigeonhole_shm::{Presence, ShmRegion, WriterLock};
use pigeonhole_wal::{CommitTicket, SpareSegments, Wal};

use crate::manifest::ManifestWriter;
use crate::resolve::{Merge, ResolveOpts, Resolver, SourceCursor};
use crate::snapshot::{MemSet, TabletMap, View};
use crate::write::ReadKey;
use crate::{CommitInfo, Error, Predicate, Result};

/// Worst-case memtable overhead per entry: node header and a full tower, plus alignment.
const ENTRY_OVERHEAD: usize = 12 + 4 * 16 + 8;
/// Fixed bytes of an internal key beyond the escaped row and qualifier.
const KEY_FIXED: usize = 2 + 2 + 17;

// ---------------------------------------------------------------------------------------
// Engine-wide state shared with the shards
// ---------------------------------------------------------------------------------------

/// Counters and a log-scale latency histogram for one shard, updated only by that shard.
#[derive(Debug)]
pub(crate) struct ShardMetrics {
    pub commits: [AtomicU64; 4],
    /// Bucket `b` holds latencies from `bucket_floor(b)` nanoseconds up to the next floor.
    pub latency: [[AtomicU64; 64]; 4],
    pub stalls: AtomicU64,
    pub stall_nanos: AtomicU64,
    pub flushes: AtomicU64,
}

impl Default for ShardMetrics {
    fn default() -> Self {
        Self {
            commits: Default::default(),
            latency: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            stalls: AtomicU64::new(0),
            stall_nanos: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
        }
    }
}

impl ShardMetrics {
    fn record(&self, durability: Durability, nanos: u64) {
        let d = durability as usize;
        self.commits[d].fetch_add(1, Ordering::Relaxed);
        self.latency[d][bucket(nanos)].fetch_add(1, Ordering::Relaxed);
    }
}

/// Half-power-of-two buckets: 64 buckets cover up to 2^32 ns.
pub(crate) fn bucket(nanos: u64) -> usize {
    if nanos < 2 {
        return 0;
    }
    let log2 = 63 - nanos.leading_zeros() as usize;
    let half = ((nanos >> (log2 - 1)) & 1) as usize;
    (log2 * 2 + half).min(63)
}

/// The lower edge of a bucket in nanoseconds.
pub(crate) fn bucket_floor(b: usize) -> u64 {
    let log2 = b / 2;
    let base = 1u64 << log2;
    if b % 2 == 1 { base + base / 2 } else { base }
}

/// The locks a writer holds for its lifetime, released at the final close.
#[derive(Debug)]
pub(crate) struct Locks {
    pub _writer: WriterLock,
    pub presence: Presence,
}

/// The close handshake: each shard reports when its WAL is synced and its queues drained;
/// the last one runs the final steps and resolves `done`.
#[derive(Debug, Default)]
pub(crate) struct CloseState {
    pub remaining: AtomicUsize,
    pub done: Mutex<Option<Notifier<Result<()>>>>,
}

/// Engine-wide state every shard and every caller shares.
pub(crate) struct Shared {
    pub vfs: VfsRef,
    pub shm: ShmRegion,
    pub shards: usize,
    pub view: ArcSwap<View>,
    /// Serializes view publishers (shards creating memtables, catalog commits) and holds the
    /// last published version.
    pub view_lock: Mutex<u64>,
    pub manifest: Mutex<ManifestWriter>,
    pub locks: Mutex<Option<Locks>>,
    pub default_durability: AtomicU8,
    pub closed: AtomicBool,
    /// A manifest (root) commit failed: the pager is poisoned (decision D58) and every later
    /// write fails until the database is reopened.
    pub pager_poisoned: AtomicBool,
    pub close: CloseState,
    pub metrics: Vec<ShardMetrics>,
    /// Per-shard largest default timestamp assigned (decision D11), read at manifest commits.
    pub ts_floors: Vec<AtomicU64>,
    pub memtable_freeze_bytes: u64,
    /// Where frozen memtables go.
    pub flush: crate::flush::FlushBackend,
    /// Submitters for every shard, set once the runtime is built.
    pub submitters: std::sync::OnceLock<Vec<Submitter<ShardMsg>>>,
    pub shm_dir: Option<std::path::PathBuf>,
    pub identity: pigeonhole_io::FileIdentity,
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shared")
            .field("shards", &self.shards)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Shared {
    pub(crate) fn submitter(&self, shard: ShardId) -> &Submitter<ShardMsg> {
        &self.submitters.get().expect("runtime started")[usize::from(shard.0)]
    }

    /// Publishes a new view built by `f` from the current one, in shared memory too. The
    /// view version is bumped under the publish lock.
    pub(crate) fn publish_view(&self, f: impl FnOnce(&View, u64) -> View) -> Result<Arc<View>> {
        let mut last = self
            .view_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let current = self.view.load_full();
        let version = *last + 1;
        let view = Arc::new(f(&current, version));
        debug_assert_eq!(view.version, version);
        self.shm.publish_view(&view.to_record())?;
        self.shm.set_manifest_version(view.manifest_version);
        self.view.store(Arc::clone(&view));
        *last = version;
        Ok(view)
    }

    pub(crate) fn default_durability(&self) -> Durability {
        match self.default_durability.load(Ordering::Relaxed) {
            0 => Durability::None,
            1 => Durability::Buffered,
            3 => Durability::Sync,
            _ => Durability::GroupSync,
        }
    }

    /// The final close, run by the last shard to finish: records a clean close and, if this
    /// is the last process, removes the shared-memory region. WAL files stay until flushes
    /// exist (Milestone B), since the memtables they back have nowhere else to go.
    pub(crate) fn final_close(&self) -> Result<()> {
        let clean = {
            let mut manifest = self.manifest.lock().unwrap_or_else(PoisonError::into_inner);
            manifest.mark_clean()
        };
        let locks = self
            .locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(locks) = locks {
            // Our writer byte is held (so no opening writer can be mid-open); the presence
            // upgrade tells whether any reader is still attached.
            if locks.presence.try_become_last()? {
                ShmRegion::remove(&self.vfs, self.identity, self.shm_dir.as_deref())?;
            }
            drop(locks);
        }
        clean
    }

    fn report_closed(&self) {
        if self.close.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            let result = self.final_close();
            self.closed.store(true, Ordering::Release);
            if let Some(n) = self
                .close
                .done
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
            {
                n.notify(result);
            }
        }
    }
}

// ---------------------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------------------

/// What a committer wants back.
#[derive(Debug)]
pub(crate) enum Reply {
    Commit(Notifier<Result<CommitInfo>>),
    Check(Notifier<Result<(bool, Option<CommitInfo>)>>),
    /// Internal (2PC records): nothing to notify directly.
    None,
}

impl Reply {
    fn resolve(self, outcome: Result<CommitInfo>) {
        match self {
            Reply::Commit(n) => n.notify(outcome),
            Reply::Check(n) => n.notify(outcome.map(|i| (true, Some(i)))),
            Reply::None => {}
        }
    }
}

/// A single-shard commit request.
#[derive(Debug)]
pub(crate) struct CommitReq {
    /// The encoded batch, moved out of the `WriteBatch` (never re-encoded).
    pub bytes: BatchBuilder,
    pub durability: Durability,
    pub reply: Reply,
    pub submitted_at: u64,
    /// Optimistic validation: the snapshot seqno and the reads to check.
    pub validate: Option<(Seqno, Vec<ReadKey>)>,
    /// `check_and_mutate`: the row and predicate to test first.
    pub predicate: Option<(TableId, Vec<u8>, Predicate)>,
}

/// A cross-shard commit request, sent to the coordinator (the shard owning the first row).
#[derive(Debug)]
pub(crate) struct CoordinateReq {
    pub parts: Vec<(ShardId, Arc<BatchBuilder>)>,
    pub durability: Durability,
    pub reply: Notifier<Result<CommitInfo>>,
    pub submitted_at: u64,
    pub validate: Option<(Seqno, Vec<ReadKey>)>,
}

/// A participant's share of a cross-shard commit.
#[derive(Debug)]
pub(crate) struct PrepareReq {
    pub seqno: Seqno,
    pub commit_ts: Timestamp,
    pub coordinator: ShardId,
    pub bytes: Arc<BatchBuilder>,
    pub durability: Durability,
    pub validate: Option<(Seqno, Vec<ReadKey>)>,
}

/// Why a participant could not prepare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrepareError {
    Conflict,
    Busy,
    Closed,
    Io,
}

impl From<PrepareError> for Error {
    fn from(e: PrepareError) -> Self {
        match e {
            PrepareError::Conflict => Error::Conflict,
            PrepareError::Busy => Error::Busy,
            PrepareError::Closed => Error::Closed,
            PrepareError::Io => poisoned_error(),
        }
    }
}

fn poisoned_error() -> Error {
    Error::Io(pigeonhole_io::Error::new(
        ErrorKind::Other,
        "the shard's WAL stream failed earlier; reopen the database",
    ))
}

fn prepare_error_of(e: &Error) -> PrepareError {
    match e {
        Error::Conflict => PrepareError::Conflict,
        Error::Busy => PrepareError::Busy,
        Error::Closed => PrepareError::Closed,
        _ => PrepareError::Io,
    }
}

/// Messages a shard handles. An engine enum, so the queue hot path has no boxing.
#[derive(Debug)]
pub(crate) enum ShardMsg {
    Commit(CommitReq),
    Coordinate(CoordinateReq),
    Prepare(PrepareReq),
    Prepared {
        seqno: Seqno,
        from: ShardId,
        error: Option<PrepareError>,
    },
    Decide {
        seqno: Seqno,
        commit: bool,
    },
    Applied {
        seqno: Seqno,
        from: ShardId,
    },
    /// A WAL sync covering groups up to `group` finished.
    SyncDone {
        group: u64,
        result: std::result::Result<Lsn, pigeonhole_io::Error>,
    },
    /// Freeze every non-empty active memtable (`Engine::flush`).
    Freeze {
        reply: Notifier<Result<()>>,
    },
    /// A table was dropped: forget its tablets' memtables.
    DropTablets {
        tablets: Vec<TabletId>,
    },
    /// Start-up work (spare WAL segments).
    Start,
    /// Nothing: forces a drain so deferred members form a group.
    Kick,
    /// Stop accepting writes, finish in-flight work, sync the stream, report.
    Close,
}

// ---------------------------------------------------------------------------------------
// Groups and two-phase commit state
// ---------------------------------------------------------------------------------------

#[derive(Debug)]
enum MemberKind {
    Single,
    Prepare { coordinator: ShardId },
    CommitRecord { participants: Vec<ShardId> },
}

/// One record of a group: a commit, a participant's PREPARE, or a coordinator's COMMIT.
#[derive(Debug)]
struct Member {
    kind: MemberKind,
    durability: Durability,
    seqno: Seqno,
    commit_ts: Timestamp,
    bytes: Bytes,
    reply: Reply,
    submitted_at: u64,
    validate: Option<(Seqno, Vec<ReadKey>)>,
    predicate: Option<(TableId, Vec<u8>, Predicate)>,
    ticket: Option<CommitTicket>,
    /// Failed before being logged (not applied, resolved with this error).
    failed: Option<Error>,
}

#[derive(Debug)]
enum Bytes {
    Own(BatchBuilder),
    Shared(Arc<BatchBuilder>),
    /// A COMMIT record's encoded participant list.
    Streams(Vec<u8>),
}

impl Bytes {
    fn as_slice(&self) -> &[u8] {
        match self {
            Bytes::Own(b) => b.batch().as_bytes(),
            Bytes::Shared(b) => b.batch().as_bytes(),
            Bytes::Streams(v) => v,
        }
    }
}

impl Member {
    fn single(req: CommitReq) -> Self {
        Member {
            kind: MemberKind::Single,
            durability: req.durability,
            seqno: 0,
            commit_ts: 0,
            bytes: Bytes::Own(req.bytes),
            reply: req.reply,
            submitted_at: req.submitted_at,
            validate: req.validate,
            predicate: req.predicate,
            ticket: None,
            failed: None,
        }
    }

    fn record(&self) -> Result<WalRecord<'_>> {
        Ok(match &self.kind {
            MemberKind::Single => WalRecord::Batch {
                seqno: self.seqno,
                commit_ts: self.commit_ts,
                batch: BatchRef::new(self.bytes.as_slice())?,
            },
            MemberKind::Prepare { coordinator } => WalRecord::Prepare {
                seqno: self.seqno,
                commit_ts: self.commit_ts,
                coordinator: StreamId(u32::from(coordinator.0)),
                batch: BatchRef::new(self.bytes.as_slice())?,
            },
            MemberKind::CommitRecord { .. } => WalRecord::Commit {
                seqno: self.seqno,
                participants: StreamList::new(self.bytes.as_slice())?,
            },
        })
    }

    /// Whether the member needs a conditional check against the applied state.
    fn conditional(&self) -> bool {
        self.predicate.is_some() || self.validate.is_some()
    }
}

/// A group whose WAL sync is in flight.
#[derive(Debug)]
struct Group {
    id: u64,
    /// Lowest seqno the group reserved (its watermark), if any.
    first: Option<Seqno>,
    members: Vec<Member>,
}

/// A cross-shard commit this shard coordinates.
#[derive(Debug)]
struct Coord {
    participants: usize,
    durability: Durability,
    reply: Option<Notifier<Result<CommitInfo>>>,
    submitted_at: u64,
    prepared: usize,
    applied: usize,
    failed: Option<Error>,
    decided: bool,
    shards: Vec<ShardId>,
}

/// A share this shard prepared and holds until the decision.
#[derive(Debug)]
struct PreparedShare {
    bytes: Arc<BatchBuilder>,
    commit_ts: Timestamp,
    coordinator: ShardId,
}

/// A memtable with the smallest user timestamp written to it: a compaction's
/// `GcPolicy::min_ts_above` is the minimum over the live memtables above its inputs
/// (data above an input can hold newer entries with older explicit timestamps).
#[derive(Debug)]
struct MemEntry {
    table: Memtable,
    min_ts: Timestamp,
}

impl MemEntry {
    fn new(table: Memtable) -> Self {
        Self {
            table,
            min_ts: u64::MAX,
        }
    }
}

/// The memtables of one `(tablet, family)` on this shard.
#[derive(Debug)]
struct MemSlot {
    active: MemEntry,
    /// Newest first.
    frozen: Vec<MemEntry>,
}

impl MemSlot {
    fn set(&self, shard: ShardId) -> Arc<MemSet> {
        let mut readers = Vec::with_capacity(1 + self.frozen.len());
        let mut roots = Vec::with_capacity(1 + self.frozen.len());
        readers.push(self.active.table.reader());
        roots.push(self.active.table.root());
        for m in &self.frozen {
            readers.push(m.table.reader());
            roots.push(m.table.root());
        }
        Arc::new(MemSet {
            shard,
            readers,
            roots,
        })
    }

    fn readers(&self) -> Vec<MemtableReader> {
        std::iter::once(self.active.table.reader())
            .chain(self.frozen.iter().map(|m| m.table.reader()))
            .collect()
    }

    /// The smallest user timestamp in any of these memtables.
    #[allow(dead_code)]
    fn min_ts(&self) -> Timestamp {
        self.frozen
            .iter()
            .map(|m| m.min_ts)
            .fold(self.active.min_ts, Timestamp::min)
    }
}

/// Finds or creates the slot of `(tablet, family)`; a split borrow so the arena stays usable.
fn slot_of<'a>(
    memtables: &'a mut HashMap<(TabletId, FamilyId), MemSlot>,
    arena: &mut ShardArena,
    view_dirty: &mut bool,
    key: (TabletId, FamilyId),
) -> Result<&'a mut MemSlot> {
    match memtables.entry(key) {
        std::collections::hash_map::Entry::Occupied(e) => Ok(e.into_mut()),
        std::collections::hash_map::Entry::Vacant(e) => {
            let active = MemEntry::new(Memtable::create(arena)?);
            *view_dirty = true;
            Ok(e.insert(MemSlot {
                active,
                frozen: Vec::new(),
            }))
        }
    }
}

/// Same-commit collapse scratch (decision D34): the last mutation per
/// `(table, family, row, qualifier, timestamp)` wins. Keyed by two independent 64-bit
/// hashes, so a false collision needs a 128-bit coincidence.
#[derive(Debug, Default)]
struct Dedup {
    hashes: Vec<(u64, u64)>,
    last: HashMap<(u64, u64), u32>,
}

impl Dedup {
    /// Fills the tables for `batch`. Returns whether any duplicate exists.
    fn scan(&mut self, batch: BatchRef<'_>, commit_ts: Timestamp) -> bool {
        self.hashes.clear();
        self.last.clear();
        let mut dup = false;
        for (i, m) in batch.iter().enumerate() {
            let Ok(m) = m else {
                continue;
            };
            let h = hash_mutation(&m, commit_ts);
            self.hashes.push(h);
            if self.last.insert(h, i as u32).is_some() {
                dup = true;
            }
        }
        dup
    }

    fn wins(&self, i: usize) -> bool {
        self.hashes
            .get(i)
            .is_none_or(|h| self.last.get(h).copied() == Some(i as u32))
    }
}

fn hash_mutation(m: &Mutation<'_>, commit_ts: Timestamp) -> (u64, u64) {
    let ts = m.ts.unwrap_or(commit_ts);
    // Markers live in their own key space and never collapse with column entries.
    let marker = u8::from(m.kind == Kind::FamilyDelete);
    let mut a = DefaultHasher::new();
    a.write_u32(m.table.0);
    a.write_u32(m.family.0);
    a.write_u8(marker);
    a.write(m.row);
    a.write_u8(0xff);
    a.write(m.qualifier);
    a.write_u64(ts);
    let mut b = Vec::with_capacity(m.row.len() + m.qualifier.len() + 24);
    b.extend_from_slice(&m.table.0.to_le_bytes());
    b.extend_from_slice(&m.family.0.to_le_bytes());
    b.push(marker);
    b.extend_from_slice(&(m.row.len() as u32).to_le_bytes());
    b.extend_from_slice(m.row);
    b.extend_from_slice(m.qualifier);
    b.extend_from_slice(&ts.to_le_bytes());
    (a.finish(), pigeonhole_format::checksum::xxh3_64(&b))
}

fn hash_row(table: TableId, row: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    h.write_u32(table.0);
    h.write(row);
    h.finish()
}

/// Prepares spare WAL segments off the foreground loop (decision D35).
struct SpareTask {
    spares: SpareSegments,
    running: Arc<AtomicBool>,
}

impl Task for SpareTask {
    fn run(&mut self, _deadline_nanos: u64, _waker: &TaskWaker) -> TaskPoll {
        let _ = self.spares.prepare(self.spares.target());
        self.running.store(false, Ordering::Release);
        TaskPoll::Done
    }

    fn name(&self) -> &'static str {
        "wal-spares"
    }
}

// ---------------------------------------------------------------------------------------
// The shard
// ---------------------------------------------------------------------------------------

/// Everything one shard owns.
pub(crate) struct ShardState {
    pub(crate) id: ShardId,
    shared: Arc<Shared>,
    pub(crate) wal: Option<Box<dyn Wal>>,
    /// A write or sync failed: the stream is poisoned until reopen.
    poisoned: bool,
    arena: ShardArena,
    chunk_size: usize,
    memtables: HashMap<(TabletId, FamilyId), MemSlot>,
    /// Retired memtables (dropped tables) waiting for reader processes to release their
    /// views: `(view version that dropped them, token)`.
    retired: Vec<(u64, Retired)>,
    /// Routing, refreshed when the view's tablet map changes.
    tablets: Arc<TabletMap>,
    /// Members drained since the last group.
    pending: Vec<Member>,
    /// Groups whose sync is in flight, oldest first.
    unresolved: VecDeque<Group>,
    next_group: u64,
    /// Cross-shard seqnos this shard coordinates, not yet applied everywhere.
    held: BTreeSet<Seqno>,
    coord: HashMap<Seqno, Coord>,
    prepared: HashMap<Seqno, PreparedShare>,
    /// Largest default timestamp assigned on this shard (D11).
    ts_floor: Timestamp,
    key_buf: Vec<u8>,
    dedup: Dedup,
    touched: HashSet<u64>,
    closing: bool,
    close_reported: bool,
    spares: Option<SpareSegments>,
    spares_running: Arc<AtomicBool>,
    /// A memtable was created or frozen since the last view publish.
    view_dirty: bool,
}

impl std::fmt::Debug for ShardState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardState")
            .field("id", &self.id)
            .field("memtables", &self.memtables.len())
            .field("pending", &self.pending.len())
            .field("unresolved", &self.unresolved.len())
            .field("held", &self.held)
            .finish_non_exhaustive()
    }
}

impl ShardState {
    pub(crate) fn new(
        id: ShardId,
        shared: Arc<Shared>,
        region: ArenaRegion,
        chunk_size: usize,
        tablets: Arc<TabletMap>,
        ts_floor: Timestamp,
    ) -> Self {
        Self {
            id,
            shared,
            wal: None,
            poisoned: false,
            arena: ShardArena::new(region, chunk_size),
            chunk_size,
            memtables: HashMap::new(),
            retired: Vec::new(),
            tablets,
            pending: Vec::new(),
            unresolved: VecDeque::new(),
            next_group: 1,
            held: BTreeSet::new(),
            coord: HashMap::new(),
            prepared: HashMap::new(),
            ts_floor,
            key_buf: Vec::new(),
            dedup: Dedup::default(),
            touched: HashSet::new(),
            closing: false,
            close_reported: false,
            spares: None,
            spares_running: Arc::new(AtomicBool::new(false)),
            view_dirty: false,
        }
    }

    pub(crate) fn set_wal(&mut self, wal: Box<dyn Wal>) {
        self.spares = wal.spares();
        self.wal = Some(wal);
    }

    pub(crate) fn raise_ts_floor(&mut self, ts: Timestamp) {
        self.ts_floor = self.ts_floor.max(ts);
    }

    /// The memtable sets of this shard, for a view.
    pub(crate) fn mem_sets(&self) -> Vec<((TabletId, FamilyId), Arc<MemSet>)> {
        self.memtables
            .iter()
            .map(|(k, slot)| (*k, slot.set(self.id)))
            .collect()
    }

    /// Applies a replayed record at open (before the shards run).
    pub(crate) fn replay(
        &mut self,
        bytes: &[u8],
        seqno: Seqno,
        commit_ts: Timestamp,
    ) -> Result<()> {
        self.raise_ts_floor(commit_ts);
        self.apply(bytes, seqno, commit_ts)
    }

    // ---- memtables and views ----

    /// Publishes this shard's memtable sets into a new view.
    fn publish_memtables(&mut self) -> Result<()> {
        self.view_dirty = false;
        let id = self.id;
        let sets = self.mem_sets();
        let mine: Vec<(TabletId, FamilyId)> = self.memtables.keys().copied().collect();
        self.shared.publish_view(|current, version| {
            let mut memtables = current.memtables.clone();
            memtables.retain(|k, set| set.shard != id || mine.contains(k));
            for (k, set) in sets {
                memtables.insert(k, set);
            }
            View {
                version,
                manifest_version: current.manifest_version,
                tablets: Arc::clone(&current.tablets),
                catalog: Arc::clone(&current.catalog),
                memtables,
            }
        })?;
        Ok(())
    }

    /// Worst-case arena bytes `batch` needs, so a commit is refused (`Busy`) before its
    /// record is logged rather than half-applied.
    fn arena_needed(batch: BatchRef<'_>, chunk: usize) -> usize {
        let mut total = 0usize;
        for m in batch.iter().flatten() {
            let key = 2 * (m.row.len() + m.qualifier.len()) + KEY_FIXED;
            total += ENTRY_OVERHEAD + key + m.value.len();
        }
        // Each allocation may waste the tail of the previous run (less than the entry), and a
        // new memtable needs a chunk of its own.
        2 * total + 2 * chunk
    }

    fn has_room(&self, bytes: &[u8]) -> bool {
        match BatchRef::new(bytes) {
            Ok(batch) => self.arena.free_bytes() >= Self::arena_needed(batch, self.chunk_size),
            Err(_) => true,
        }
    }

    /// Freezes active memtables over the threshold (or every non-empty one when `all`).
    fn freeze(&mut self, all: bool) -> Result<()> {
        let threshold = self.shared.memtable_freeze_bytes as usize;
        let keys: Vec<(TabletId, FamilyId)> = self.memtables.keys().copied().collect();
        for key in keys {
            let slot = self.memtables.get_mut(&key).expect("key listed");
            let big = slot.active.table.allocated_bytes() >= threshold;
            if slot.active.table.is_empty() || !(all || big) {
                continue;
            }
            let Ok(fresh) = Memtable::create(&mut self.arena) else {
                // No chunk for a new active memtable: keep writing into this one; the
                // arena-room check turns later commits into `Busy`.
                continue;
            };
            let mut old = std::mem::replace(&mut slot.active, MemEntry::new(fresh));
            old.table.freeze();
            // Without a persisting backend the frozen memtable stays in every view.
            if !self.shared.flush.persists() {
                slot.frozen.insert(0, old);
            }
            self.view_dirty = true;
        }
        if self.view_dirty {
            self.publish_memtables()?;
        }
        Ok(())
    }

    /// Reclaims retired memtables no reader slot pins any more.
    fn reclaim_retired(&mut self) {
        if self.retired.is_empty() {
            return;
        }
        let oldest_pinned_view = self.shared.shm.oldest_reader_pin().map(|(_, v)| v);
        let mut keep = Vec::new();
        for (dropped_at, retired) in self.retired.drain(..) {
            match oldest_pinned_view {
                Some(v) if v != 0 && v < dropped_at => keep.push((dropped_at, retired)),
                _ => self.arena.reclaim(retired),
            }
        }
        self.retired = keep;
    }

    fn drop_tablets(&mut self, tablets: &[TabletId]) -> Result<()> {
        let keys: Vec<(TabletId, FamilyId)> = self
            .memtables
            .keys()
            .filter(|k| tablets.contains(&k.0))
            .copied()
            .collect();
        if keys.is_empty() {
            return Ok(());
        }
        let mut retired = Vec::new();
        for key in keys {
            let slot = self.memtables.remove(&key).expect("listed");
            retired.push(slot.active.table.retire());
            retired.extend(slot.frozen.into_iter().map(|m| m.table.retire()));
        }
        self.view_dirty = true;
        self.publish_memtables()?;
        let version = self.shared.view.load().version;
        self.retired
            .extend(retired.into_iter().map(|r| (version, r)));
        self.reclaim_retired();
        Ok(())
    }

    fn refresh_tablets(&mut self) {
        let view = self.shared.view.load();
        if view.tablets.version() != self.tablets.version() {
            self.tablets = Arc::clone(&view.tablets);
        }
    }

    // ---- watermarks and timestamps ----

    fn watermark(&self) -> Seqno {
        let held = self.held.first().copied().unwrap_or(u64::MAX);
        let groups = self
            .unresolved
            .iter()
            .filter_map(|g| g.first)
            .min()
            .unwrap_or(u64::MAX);
        held.min(groups)
    }

    fn publish_watermark(&self) {
        self.shared
            .shm
            .publish_pending(u32::from(self.id.0), self.watermark());
    }

    /// Reserves `n` seqnos per FORMAT §11.3: publish the lower bound, reserve, publish the
    /// reservation.
    fn reserve(&mut self, n: u64) -> Seqno {
        let shm = &self.shared.shm;
        let shard = u32::from(self.id.0);
        shm.publish_pending(shard, self.watermark().min(shm.next_seqno()));
        let first = shm.reserve_seqnos(n);
        shm.publish_pending(shard, self.watermark().min(first));
        first
    }

    fn default_ts(&mut self) -> Timestamp {
        let now = self.shared.vfs.now_micros();
        let ts = now.max(self.ts_floor + 1);
        self.ts_floor = ts;
        self.shared.ts_floors[usize::from(self.id.0)].store(ts, Ordering::Release);
        ts
    }

    // ---- reads on the shard (predicates, validation) ----

    /// Resolves the newest version of one column over this shard's memtables at the latest
    /// state (everything applied).
    fn read_latest(
        &self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let view = self.shared.view.load();
        let Some((tablet, _)) = view.tablets().route(table, row) else {
            return Ok(None);
        };
        let Some(meta) = view.catalog.family(family) else {
            return Ok(None);
        };
        let Some(slot) = self.memtables.get(&(tablet, family)) else {
            return Ok(None);
        };
        let sources: Vec<SourceCursor> = slot
            .readers()
            .iter()
            .map(|r| SourceCursor::Mem(r.iter()))
            .collect();
        let mut opts = ResolveOpts::new(u64::MAX, self.shared.vfs.now_micros());
        opts.ttl_micros = meta.options.ttl_micros;
        opts.max_versions = meta.options.max_versions;
        opts.merge = meta.merge;
        let mut resolver = Resolver::new(Merge::new(sources), opts);
        resolver.seek_column(row, qualifier)?;
        Ok(resolver.next_cell()?.map(|c| c.value.to_vec()))
    }

    fn evaluate(&self, table: TableId, row: &[u8], predicate: &Predicate) -> Result<bool> {
        Ok(match predicate {
            Predicate::Exists { family, qualifier } => {
                self.read_latest(table, *family, row, qualifier)?.is_some()
            }
            Predicate::Absent { family, qualifier } => {
                self.read_latest(table, *family, row, qualifier)?.is_none()
            }
            Predicate::Value {
                family,
                qualifier,
                predicate,
            } => self
                .read_latest(table, *family, row, qualifier)?
                .is_some_and(|v| crate::resolve::predicate_matches(predicate, &v)),
        })
    }

    /// Whether any entry of `(table, row, family)` on this shard has a seqno above
    /// `snapshot` (optimistic validation). Reads of rows other shards own are theirs to check.
    fn conflicts(&mut self, snapshot: Seqno, read: &ReadKey) -> Result<bool> {
        let Some((tablet, shard)) = self.tablets.route(read.table, &read.row) else {
            return Ok(false);
        };
        if shard != self.id {
            return Ok(false);
        }
        let Some(slot) = self.memtables.get(&(tablet, read.family)) else {
            return Ok(false);
        };
        self.key_buf.clear();
        encode_row_prefix(&mut self.key_buf, &read.row)?;
        let prefix = &self.key_buf;
        for reader in slot.readers() {
            let mut it = reader.iter();
            it.seek(prefix)?;
            while it.valid() && it.key().starts_with(prefix) {
                let (_, _, seqno, _) = split_suffix(it.key())?;
                if seqno > snapshot {
                    return Ok(true);
                }
                it.next()?;
            }
        }
        Ok(false)
    }

    // ---- apply ----

    /// Applies `bytes` at `seqno`/`commit_ts` to this shard's memtables, last write winning
    /// per `(column, timestamp)` (decision D34).
    fn apply(&mut self, bytes: &[u8], seqno: Seqno, commit_ts: Timestamp) -> Result<()> {
        let batch = BatchRef::new(bytes)?;
        let dups = self.dedup.scan(batch, commit_ts);
        let tablets = Arc::clone(&self.tablets);
        let mut last_route: Option<(TableId, &[u8], TabletId)> = None;
        let mut key_buf = std::mem::take(&mut self.key_buf);
        let mut result = Ok(());
        for (i, m) in batch.iter().enumerate() {
            let m = match m {
                Ok(m) => m,
                Err(e) => {
                    result = Err(e.into());
                    break;
                }
            };
            if dups && !self.dedup.wins(i) {
                continue;
            }
            let tablet = match last_route {
                Some((t, r, id)) if t == m.table && r == m.row => id,
                _ => {
                    // A row of a dropped table, or one another shard owns (replay of a
                    // stream written under a different shard count), is not ours.
                    let Some((id, owner)) = tablets.route(m.table, m.row) else {
                        continue;
                    };
                    if owner != self.id {
                        continue;
                    }
                    last_route = Some((m.table, m.row, id));
                    id
                }
            };
            let ts = m.ts.unwrap_or(commit_ts);
            key_buf.clear();
            let encoded = if m.kind == Kind::FamilyDelete {
                encode_marker_key(&mut key_buf, m.row, ts, seqno)
            } else {
                encode_key(&mut key_buf, m.row, m.qualifier, ts, seqno, m.kind)
            };
            if let Err(e) = encoded {
                result = Err(e.into());
                break;
            }
            let slot = match slot_of(
                &mut self.memtables,
                &mut self.arena,
                &mut self.view_dirty,
                (tablet, m.family),
            ) {
                Ok(s) => s,
                Err(e) => {
                    result = Err(e);
                    break;
                }
            };
            if let Err(e) = slot.active.table.insert(&mut self.arena, &key_buf, m.value) {
                result = Err(e.into());
                break;
            }
            slot.active.min_ts = slot.active.min_ts.min(ts);
        }
        self.key_buf = key_buf;
        result
    }

    // ---- the group commit ----

    /// Fails every member of a group that could not be logged.
    fn fail_all(&mut self, members: Vec<Member>, ctx: &mut ShardContext<'_, ShardMsg>) {
        for mut m in members {
            if m.failed.is_none() {
                m.failed = Some(poisoned_error());
            }
            self.settle(m, Ok(()), ctx);
        }
        self.publish_watermark();
    }

    /// Runs the group commit over everything drained since the last one.
    fn run_group(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        if self.pending.is_empty() {
            return;
        }
        self.refresh_tablets();
        let members = std::mem::take(&mut self.pending);

        // Admission, in order. A conditional member whose row an earlier member of this
        // group already touched runs in the next group instead, with everything after it,
        // so conditions see the applied state and submission order holds per row.
        let mut admitted: Vec<Member> = Vec::with_capacity(members.len());
        self.touched.clear();
        let mut iter = members.into_iter();
        while let Some(mut m) = iter.next() {
            if self.closing && matches!(m.kind, MemberKind::Single) {
                m.failed = Some(Error::Closed);
                self.settle(m, Ok(()), ctx);
                continue;
            }
            if self.poisoned {
                m.failed = Some(poisoned_error());
                self.settle(m, Ok(()), ctx);
                continue;
            }
            let rows: Vec<u64> = match (&m.kind, BatchRef::new(m.bytes.as_slice())) {
                (MemberKind::CommitRecord { .. }, _) | (_, Err(_)) => Vec::new(),
                (_, Ok(batch)) => batch
                    .iter()
                    .flatten()
                    .map(|mu| hash_row(mu.table, mu.row))
                    .collect(),
            };
            if m.conditional() && rows.iter().any(|h| self.touched.contains(h)) {
                self.pending.push(m);
                self.pending.extend(iter);
                break;
            }
            if let Some((table, row, predicate)) = &m.predicate {
                match self.evaluate(*table, row, predicate) {
                    Ok(true) => {}
                    Ok(false) => {
                        if let Reply::Check(n) = m.reply {
                            n.notify(Ok((false, None)));
                        }
                        continue;
                    }
                    Err(e) => {
                        m.failed = Some(e);
                        self.settle(m, Ok(()), ctx);
                        continue;
                    }
                }
            }
            if let Some((snapshot, reads)) = &m.validate {
                let mut conflict = false;
                for r in reads {
                    match self.conflicts(*snapshot, r) {
                        Ok(true) => {
                            conflict = true;
                            break;
                        }
                        Ok(false) => {}
                        Err(e) => {
                            m.failed = Some(e);
                            break;
                        }
                    }
                }
                if conflict {
                    m.failed = Some(Error::Conflict);
                }
                if m.failed.is_some() {
                    self.settle(m, Ok(()), ctx);
                    continue;
                }
            }
            if !matches!(m.kind, MemberKind::CommitRecord { .. })
                && !self.has_room(m.bytes.as_slice())
            {
                let metrics = &self.shared.metrics[usize::from(self.id.0)];
                metrics.stalls.fetch_add(1, Ordering::Relaxed);
                m.failed = Some(Error::Busy);
                self.settle(m, Ok(()), ctx);
                continue;
            }
            self.touched.extend(rows);
            admitted.push(m);
        }
        if !self.pending.is_empty() {
            let _ = ctx.submitter(self.id).submit(ShardMsg::Kick);
        }
        if admitted.is_empty() {
            self.publish_watermark();
            return;
        }

        // Seqnos (one reservation per group) and timestamps.
        let singles = admitted
            .iter()
            .filter(|m| matches!(m.kind, MemberKind::Single))
            .count() as u64;
        let first = (singles > 0).then(|| self.reserve(singles));
        let mut next = first.unwrap_or(0);
        for m in &mut admitted {
            match m.kind {
                MemberKind::Single => {
                    m.seqno = next;
                    next += 1;
                    m.commit_ts = self.default_ts();
                }
                MemberKind::Prepare { .. } | MemberKind::CommitRecord { .. } => {
                    self.raise_ts_floor(m.commit_ts);
                }
            }
        }

        // The log: append every record, one write, one submitted sync.
        let mut group = Group {
            id: self.next_group,
            first,
            members: admitted,
        };
        self.next_group += 1;
        let mut need_group_sync = false;
        let mut appended = false;
        let mut unsynced = false;
        let mut last_sync: Option<pigeonhole_io::Completion<Lsn>> = None;
        for m in &mut group.members {
            if m.durability == Durability::None || m.failed.is_some() {
                continue;
            }
            let Some(wal) = self.wal.as_mut() else {
                m.failed = Some(Error::Unsupported("no WAL stream"));
                continue;
            };
            let record = match m.record() {
                Ok(r) => r,
                Err(e) => {
                    m.failed = Some(e);
                    continue;
                }
            };
            match wal.append(&record, m.durability) {
                Ok(t) => {
                    m.ticket = Some(t);
                    appended = true;
                    unsynced = true;
                }
                Err(pigeonhole_wal::Error::RecordTooLarge) => {
                    m.failed = Some(Error::RecordTooLarge);
                    continue;
                }
                Err(pigeonhole_wal::Error::InvalidArgument { what }) => {
                    m.failed = Some(Error::InvalidArgument(what.to_owned()));
                    continue;
                }
                Err(_) => {
                    self.poisoned = true;
                    self.fail_all(group.members, ctx);
                    return;
                }
            }
            match m.durability {
                Durability::Sync => {
                    let synced = wal.write().and_then(|_| wal.submit_sync());
                    match synced {
                        Ok(c) => {
                            last_sync = Some(c);
                            unsynced = false;
                        }
                        Err(_) => {
                            self.poisoned = true;
                            self.fail_all(group.members, ctx);
                            return;
                        }
                    }
                }
                Durability::GroupSync => need_group_sync = true,
                Durability::Buffered | Durability::None => {}
            }
        }
        if appended {
            let wal = self.wal.as_mut().expect("appended through it");
            if wal.write().is_err() {
                self.poisoned = true;
                self.fail_all(group.members, ctx);
                return;
            }
            if need_group_sync && unsynced {
                match wal.submit_sync() {
                    Ok(c) => last_sync = Some(c),
                    Err(_) => {
                        self.poisoned = true;
                        self.fail_all(group.members, ctx);
                        return;
                    }
                }
            }
        }

        // Apply. Every member is logged (or failed): a mid-batch failure here is a bug or an
        // arena miscount; the shard poisons itself so nothing half-applied is extended.
        for m in &mut group.members {
            if m.failed.is_some() {
                continue;
            }
            if let MemberKind::Single = m.kind
                && let Err(e) = self.apply(m.bytes.as_slice(), m.seqno, m.commit_ts)
            {
                self.poisoned = true;
                m.failed = Some(e);
            }
        }
        if self.view_dirty
            && let Err(e) = self.publish_memtables()
        {
            self.poisoned = true;
            for m in &mut group.members {
                if m.failed.is_none() {
                    m.failed = Some(Error::Corruption(format!("view publish failed: {e}")));
                }
            }
        }
        if self.freeze(false).is_err() {
            self.poisoned = true;
        }

        // Resolution: now, or when the sync completes.
        match last_sync {
            None => {
                self.publish_watermark();
                self.resolve_group(group, Ok(()), ctx);
            }
            Some(completion) => {
                let id = group.id;
                self.unresolved.push_back(group);
                self.publish_watermark();
                let submitter = ctx.submitter(self.id).clone();
                // Runs on the resolving thread (the I/O backend, or inline under the
                // simulator); the dropped completion keeps the closure installed.
                drop(completion.map(move |r| {
                    let _ = submitter.submit(ShardMsg::SyncDone {
                        group: id,
                        result: r,
                    });
                    Ok(())
                }));
            }
        }
        self.reclaim_retired();
        self.maybe_prepare_spares(ctx);
    }

    fn maybe_prepare_spares(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        let Some(spares) = &self.spares else {
            return;
        };
        if spares.ready() >= spares.target() || self.spares_running.swap(true, Ordering::AcqRel) {
            return;
        }
        ctx.spawn(Box::new(SpareTask {
            spares: spares.clone(),
            running: Arc::clone(&self.spares_running),
        }));
    }

    /// Resolves every unresolved group up to `through` (syncs cover everything before them).
    fn resolve_through(
        &mut self,
        through: u64,
        result: std::result::Result<Lsn, pigeonhole_io::Error>,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        let failed = result.is_err();
        if failed {
            self.poisoned = true;
        }
        while self.unresolved.front().is_some_and(|g| g.id <= through) {
            let group = self.unresolved.pop_front().expect("checked");
            self.publish_watermark();
            let outcome = if failed {
                Err(Error::Io(pigeonhole_io::Error::new(
                    ErrorKind::Other,
                    "WAL sync failed; the commit may not be durable",
                )))
            } else {
                Ok(())
            };
            self.resolve_group(group, outcome, ctx);
        }
        self.try_finish_close(ctx);
    }

    /// Settles every member of a resolved group.
    fn resolve_group(
        &mut self,
        group: Group,
        outcome: Result<()>,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        let failed = outcome.is_err();
        for m in group.members {
            let outcome = if failed {
                Err(Error::Io(pigeonhole_io::Error::new(
                    ErrorKind::Other,
                    "WAL sync failed; the commit may not be durable",
                )))
            } else {
                Ok(())
            };
            self.settle(m, outcome, ctx);
        }
        self.try_finish_close(ctx);
    }

    /// Delivers one member's outcome: a reply, a PREPARED, or the COMMIT decision.
    fn settle(&mut self, mut m: Member, outcome: Result<()>, ctx: &mut ShardContext<'_, ShardMsg>) {
        let result: Result<CommitInfo> = match (m.failed.take(), outcome) {
            (Some(e), _) => Err(e),
            (None, Err(e)) => Err(e),
            (None, Ok(())) => Ok(CommitInfo {
                seqno: m.seqno,
                durability: m.durability,
            }),
        };
        match m.kind {
            MemberKind::Single => {
                if result.is_ok() {
                    let now = ctx.now_nanos();
                    self.shared.metrics[usize::from(self.id.0)]
                        .record(m.durability, now.saturating_sub(m.submitted_at));
                }
                m.reply.resolve(result);
            }
            MemberKind::Prepare { coordinator } => {
                let error = result.as_ref().err().map(prepare_error_of);
                if error.is_some() {
                    self.prepared.remove(&m.seqno);
                }
                self.send(
                    coordinator,
                    ShardMsg::Prepared {
                        seqno: m.seqno,
                        from: self.id,
                        error,
                    },
                    ctx,
                );
            }
            MemberKind::CommitRecord { participants } => {
                // The decision stands once the record is written; a failed sync only means
                // the caller cannot be promised durability (the stream is poisoned).
                if let (Err(e), Some(c)) = (result, self.coord.get_mut(&m.seqno)) {
                    c.failed = Some(e);
                }
                for p in participants {
                    self.send(
                        p,
                        ShardMsg::Decide {
                            seqno: m.seqno,
                            commit: true,
                        },
                        ctx,
                    );
                }
            }
        }
    }

    /// Sends a message to `shard`, handling our own inline.
    fn send(&mut self, shard: ShardId, msg: ShardMsg, ctx: &mut ShardContext<'_, ShardMsg>) {
        if shard == self.id {
            self.handle_msg(msg, ctx);
        } else if let Err(pigeonhole_runtime::Error::Closed) = ctx.submitter(shard).submit(msg) {
            // The peer is gone (shutdown): nothing more can happen for that commit.
        }
    }

    // ---- two-phase commit ----

    fn start_coordination(&mut self, req: CoordinateReq, ctx: &mut ShardContext<'_, ShardMsg>) {
        if self.closing || self.poisoned {
            req.reply.notify(Err(if self.closing {
                Error::Closed
            } else {
                poisoned_error()
            }));
            return;
        }
        let seqno = self.reserve(1);
        self.held.insert(seqno);
        self.publish_watermark();
        let commit_ts = self.default_ts();
        let shards: Vec<ShardId> = req.parts.iter().map(|(s, _)| *s).collect();
        self.coord.insert(
            seqno,
            Coord {
                participants: req.parts.len(),
                durability: req.durability,
                reply: Some(req.reply),
                submitted_at: req.submitted_at,
                prepared: 0,
                applied: 0,
                failed: None,
                decided: false,
                shards: shards.clone(),
            },
        );
        for (shard, bytes) in req.parts {
            let msg = ShardMsg::Prepare(PrepareReq {
                seqno,
                commit_ts,
                coordinator: self.id,
                bytes,
                durability: req.durability,
                validate: req.validate.clone(),
            });
            if shard == self.id {
                self.handle_msg(msg, ctx);
            } else if ctx.submitter(shard).submit(msg).is_err() {
                self.on_prepared(seqno, shard, Some(PrepareError::Closed), ctx);
            }
        }
    }

    fn on_prepare(&mut self, req: PrepareReq, ctx: &mut ShardContext<'_, ShardMsg>) {
        if self.closing || self.poisoned {
            let error = Some(if self.closing {
                PrepareError::Closed
            } else {
                PrepareError::Io
            });
            self.send(
                req.coordinator,
                ShardMsg::Prepared {
                    seqno: req.seqno,
                    from: self.id,
                    error,
                },
                ctx,
            );
            return;
        }
        self.prepared.insert(
            req.seqno,
            PreparedShare {
                bytes: Arc::clone(&req.bytes),
                commit_ts: req.commit_ts,
                coordinator: req.coordinator,
            },
        );
        self.pending.push(Member {
            kind: MemberKind::Prepare {
                coordinator: req.coordinator,
            },
            durability: req.durability,
            seqno: req.seqno,
            commit_ts: req.commit_ts,
            bytes: Bytes::Shared(req.bytes),
            reply: Reply::None,
            submitted_at: 0,
            validate: req.validate,
            predicate: None,
            ticket: None,
            failed: None,
        });
    }

    fn on_prepared(
        &mut self,
        seqno: Seqno,
        _from: ShardId,
        error: Option<PrepareError>,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        let Some(c) = self.coord.get_mut(&seqno) else {
            return;
        };
        if let Some(e) = error
            && c.failed.is_none()
        {
            c.failed = Some(e.into());
        }
        c.prepared += 1;
        if c.prepared < c.participants || c.decided {
            return;
        }
        c.decided = true;
        if c.failed.is_some() {
            let shards = c.shards.clone();
            for p in shards {
                self.send(
                    p,
                    ShardMsg::Decide {
                        seqno,
                        commit: false,
                    },
                    ctx,
                );
            }
            return;
        }
        let streams: Vec<StreamId> = c.shards.iter().map(|s| StreamId(u32::from(s.0))).collect();
        let (durability, shards) = (c.durability, c.shards.clone());
        let mut encoded = Vec::new();
        if let Err(e) = StreamList::encode(&streams, &mut encoded) {
            c.failed = Some(e.into());
            for p in shards {
                self.send(
                    p,
                    ShardMsg::Decide {
                        seqno,
                        commit: false,
                    },
                    ctx,
                );
            }
            return;
        }
        self.pending.push(Member {
            kind: MemberKind::CommitRecord {
                participants: shards,
            },
            durability,
            seqno,
            commit_ts: 0,
            bytes: Bytes::Streams(encoded),
            reply: Reply::None,
            submitted_at: 0,
            validate: None,
            predicate: None,
            ticket: None,
            failed: None,
        });
        let _ = ctx.submitter(self.id).submit(ShardMsg::Kick);
    }

    fn on_decide(&mut self, seqno: Seqno, commit: bool, ctx: &mut ShardContext<'_, ShardMsg>) {
        let Some(share) = self.prepared.remove(&seqno) else {
            return;
        };
        if commit {
            self.refresh_tablets();
            if self
                .apply(share.bytes.batch().as_bytes(), seqno, share.commit_ts)
                .is_err()
            {
                self.poisoned = true;
            }
            if self.view_dirty && self.publish_memtables().is_err() {
                self.poisoned = true;
            }
            if self.freeze(false).is_err() {
                self.poisoned = true;
            }
        }
        self.send(
            share.coordinator,
            ShardMsg::Applied {
                seqno,
                from: self.id,
            },
            ctx,
        );
    }

    fn on_applied(&mut self, seqno: Seqno, _from: ShardId, ctx: &mut ShardContext<'_, ShardMsg>) {
        let Some(c) = self.coord.get_mut(&seqno) else {
            return;
        };
        c.applied += 1;
        if c.applied < c.participants {
            return;
        }
        let mut c = self.coord.remove(&seqno).expect("present");
        self.held.remove(&seqno);
        self.publish_watermark();
        let outcome = match c.failed.take() {
            Some(e) => Err(e),
            None => {
                let now = ctx.now_nanos();
                self.shared.metrics[usize::from(self.id.0)]
                    .record(c.durability, now.saturating_sub(c.submitted_at));
                Ok(CommitInfo {
                    seqno,
                    durability: c.durability,
                })
            }
        };
        if let Some(reply) = c.reply.take() {
            reply.notify(outcome);
        }
        self.try_finish_close(ctx);
    }

    // ---- close ----

    fn try_finish_close(&mut self, _ctx: &mut ShardContext<'_, ShardMsg>) {
        if !self.closing
            || self.close_reported
            || !self.unresolved.is_empty()
            || !self.coord.is_empty()
            || !self.prepared.is_empty()
            || !self.pending.is_empty()
        {
            return;
        }
        self.close_reported = true;
        if let Some(wal) = self.wal.as_mut()
            && !self.poisoned
        {
            let _ = wal.sync();
        }
        self.shared.report_closed();
    }

    /// Finishes a shard whose driver is dropped before the close handshake completed: the
    /// stream is synced and the close reported, so `Engine::close` never waits forever.
    pub(crate) fn abandon(&mut self, shared: &Shared) {
        if self.close_reported {
            return;
        }
        self.closing = true;
        self.close_reported = true;
        if let Some(wal) = self.wal.as_mut()
            && !self.poisoned
        {
            let _ = wal.sync();
        }
        if shared.close.remaining.load(Ordering::Acquire) > 0 {
            shared.report_closed();
        }
    }

    /// Commits inline on the shard's own thread (application-owned mode): the member joins
    /// whatever is pending and the group runs now.
    pub(crate) fn commit_inline(&mut self, req: CommitReq, ctx: &mut ShardContext<'_, ShardMsg>) {
        self.pending.push(Member::single(req));
        self.run_group(ctx);
    }

    fn handle_msg(&mut self, msg: ShardMsg, ctx: &mut ShardContext<'_, ShardMsg>) {
        match msg {
            ShardMsg::Commit(req) => {
                if self.closing {
                    req.reply.resolve(Err(Error::Closed));
                } else {
                    self.pending.push(Member::single(req));
                }
            }
            ShardMsg::Coordinate(req) => self.start_coordination(req, ctx),
            ShardMsg::Prepare(req) => self.on_prepare(req, ctx),
            ShardMsg::Prepared { seqno, from, error } => self.on_prepared(seqno, from, error, ctx),
            ShardMsg::Decide { seqno, commit } => self.on_decide(seqno, commit, ctx),
            ShardMsg::Applied { seqno, from } => self.on_applied(seqno, from, ctx),
            ShardMsg::SyncDone { group, result } => self.resolve_through(group, result, ctx),
            ShardMsg::Freeze { reply } => {
                let r = self.freeze(true);
                self.shared.metrics[usize::from(self.id.0)]
                    .flushes
                    .fetch_add(1, Ordering::Relaxed);
                reply.notify(r);
            }
            ShardMsg::DropTablets { tablets } => {
                if self.drop_tablets(&tablets).is_err() {
                    self.poisoned = true;
                }
            }
            ShardMsg::Start => {
                self.maybe_prepare_spares(ctx);
            }
            ShardMsg::Kick => {}
            ShardMsg::Close => {
                self.closing = true;
                self.try_finish_close(ctx);
            }
        }
    }
}

impl ShardHandler for ShardState {
    type Msg = ShardMsg;

    fn handle(&mut self, ctx: &mut ShardContext<'_, Self::Msg>, msg: Self::Msg) {
        self.handle_msg(msg, ctx);
    }

    fn end_batch(&mut self, ctx: &mut ShardContext<'_, Self::Msg>) {
        self.run_group(ctx);
        self.try_finish_close(ctx);
    }
}
