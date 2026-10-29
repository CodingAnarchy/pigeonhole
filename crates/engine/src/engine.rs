#[cfg(feature = "test-hooks")]
pub(crate) mod hooks;
mod info;

pub use info::{DbInfo, FamilyStorage, LevelStorage, TableStorage};

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
    PoolDriver, Runtime, RuntimeConfig, ShardDriver, ShardId, Submitter, Waiter, WorkerPool,
    completion,
};
use pigeonhole_shm::{Presence, Role as ShmRole, ShmConfig, ShmRegion, WriterLock};
use pigeonhole_sst::SstWriterOptions;
use pigeonhole_wal::{Recovery, Wal, WalStream, discover_streams, stream_path};
use smallvec::SmallVec;

use crate::catalog::{Catalog, FamilyMeta, MergeKind};
use crate::flush::{SstSink, write_memtable};
use crate::manifest::{self, ManifestWriter, ReqKind};
use crate::read::{self, get_in};
use crate::shard::{
    BalanceConfig, COLD_PASSES, CloseState, CommitReq, CoordinateReq, CrossRefusal, LoadSlot,
    Locks, MaintenanceGuard, Padded, ReplayedKind, Reply, ShardMetrics, ShardMsg, ShardState,
    Shared, VisibilityCounts, WatermarkWaiters, bucket_floor, split_by_shard,
};
use crate::snapshot::{
    LiveSeqnos, LiveSnapshot, LiveViews, MemSet, SeqnoPin, ShardMems, SstSet, TabletEntry,
    TabletMap, View, ViewPin,
};
use crate::write::{COUNTER_TS, Submission, Validate};

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
    /// Compactions an application's background worker held when the shard took them back
    /// (the close, or a dropped worker; D284), dropped before their commit: not failures,
    /// their slots compact again.
    pub compactions_reclaimed: u64,
    /// Application-owned background workers (D284), summed over shards: tasks a shard took
    /// back from the workers' queue because no worker made a pass for 10 ms (workers
    /// configured but not driven), and flushes a wait for memtable room took back from a
    /// worker that held them without running them. Both stay 0 while the workers are driven.
    pub background_take_backs: (u64, u64),
    /// Write stalls and total stalled nanoseconds: the sum of `room_stalls` and
    /// `pacing_stalls`, kept as it was before the split (ICR 0054).
    pub stalls: (u64, u64),
    /// Waits for memtable arena room (D124): commits held until a flush frees room. A count
    /// of waits begun and the nanoseconds spent in them, summed over shards.
    pub room_stalls: (u64, u64),
    /// L0 pacing (D119): commits held by the token bucket while L0 is too deep, until
    /// compaction catches up. A count of pacing spans begun and the nanoseconds spent in
    /// them, summed over shards.
    pub pacing_stalls: (u64, u64),
    /// Block cache lookups that hit and that missed since open (#57). A miss is a block read
    /// from the file; with the cache disabled (`block_cache_bytes` 0) nothing is counted.
    pub block_cache: (u64, u64),
    /// Bytes of SST top indexes and filters pinned in the block cache by open SST readers
    /// (D215). They count against the capacity and are never evicted: past about half of it,
    /// index partitions and data blocks no longer stay cached and a cold get costs two reads
    /// rather than one. Size `block_cache_bytes` to at least twice this.
    pub block_cache_pinned: u64,
    /// The block cache's capacity in bytes (`EngineOptions::block_cache_bytes`).
    pub block_cache_capacity: u64,
    /// Times `block_cache_pinned` rose past half of `block_cache_capacity` since open: a
    /// warning that the cache is too small for the open SSTs' metadata (D215). Counted at
    /// most once until the pinned bytes fall below 40% of the capacity again.
    pub block_cache_pinned_warnings: u64,
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
    /// File growths ahead of need (64 MiB each, outside the allocator lock, the sync
    /// submitted to the I/O backend), and the nanoseconds their `fallocate`s took on the
    /// allocating thread.
    pub file_growths_ahead: (u64, u64),
    /// File reads made synchronously inside async reads: a separated value too large to
    /// cache (D196, #398), a block the cache could not keep, or a read that missed too
    /// often. Zero when async reads never block their executor thread.
    pub async_sync_reads: u64,
    /// Idle parks of the shards' engine-owned threads (after their idle spin, D198), and
    /// submits or notifications that found a shard asleep and woke it, summed over shards
    /// (ICR 0027). Per commit, they show how often a commit pays a shard wakeup. An
    /// application-owned shard sleeps in the application's loop: only its wakes count.
    pub shard_idle: (u64, u64),
    /// Nanoseconds the shards' engine-owned threads spent in work turns (handling messages,
    /// running their tasks; not spinning or parked), summed over shards (D217, ICR 0033).
    /// Its rise over an interval, divided by the interval, is how many shards' worth of
    /// work the store did. Application-owned shards count nothing.
    pub shard_busy_nanos: u64,
    /// Nanoseconds the compaction pool's threads spent in slices of the shards' flushes and
    /// compactions, summed over shards (D256). Its rise over an interval, divided by the
    /// interval, is how many threads' worth of pool work the store did; zero with the pool
    /// off (that work is inside `shard_busy_nanos` then).
    pub shard_pool_busy_nanos: u64,
    /// Nanoseconds the shards' engine-owned threads spent in their spin before parking
    /// (D198), summed over shards (D256); not in `shard_busy_nanos`. Its rise over an
    /// interval, divided by the interval, is how many threads' worth of polling the store
    /// did: awake time its clients caused without work behind it.
    pub shard_spin_time_nanos: u64,
    /// Nanoseconds client commits spent in flight, from submit to reply, summed over the
    /// commits every shard completed (D257), plus each cross-shard commit's time from
    /// submit to the decision at every participant (D262). Its rise over an interval,
    /// divided by the interval, is the mean number of commit rounds in flight (Little's
    /// law): the concurrency packing sizes the active shards by, at most the clients with
    /// a commit outstanding times the shards a commit occupies.
    pub shard_commit_inflight_nanos: u64,
    /// Blocking waits for a commit's reply (`PendingCommit::wait`, `Engine::commit`) that
    /// parked their thread after the D198 spin (ICR 0027). Async waits never park.
    pub commit_parks: u64,
    /// Reader slots in use and the slot count, `(active, total)` (#693 D1). Both 0 in a
    /// reader process.
    pub reader_slots: (u64, u64),
    /// Reader slots the writer freed because their process was gone (#833).
    pub reader_slots_reclaimed: u64,
    /// How far behind the current view and visible seqno the oldest reader pin is, `(views,
    /// seqnos)`: `(0, 0)` with no pin. A pin that keeps falling behind holds the writer's
    /// memtables and extents, and eventually stalls commits into `Busy` (#693 D1).
    pub reader_pin_lag: (u64, u64),
    /// How many of the threads an engine-owned open started are pinned to a CPU of their
    /// own (D235), and how many it started (shards and compaction threads): `(pinned,
    /// started)`. Both 0 for an application-owned or closed engine.
    pub pinned_threads: (u64, u64),
    /// Why threads are unpinned although `pin_threads` asked for them: the platform has no
    /// hard thread affinity (macOS), a CPU quota is narrower than the affinity set, too few
    /// free CPUs in the affinity set (other pinned databases in this process hold the rest;
    /// with room for the shards but not the compaction threads, only the compaction threads
    /// are unpinned, D252), or the OS refused (some threads unpinned). `None` when every
    /// thread is pinned or pinning is off (D235).
    pub unpinned: Option<&'static str>,
    /// Commit waits (blocking and async) that found their seqno not yet visible and
    /// registered as waiters on the global watermark (D19, D227, ICR 0023), and the waiters already listed in the
    /// registry part each registration joined (a queue depth: registration no longer scans
    /// for a duplicate). Waits per commit near 0 mean the watermark is not what commits
    /// wait on (#154).
    pub visibility_waits: (u64, u64),
    /// Wake passes over that list (a watermark publish that reached a registered seqno), and
    /// the async commits they woke.
    pub visibility_wakes: (u64, u64),
    /// Registrations and wake passes that found the list's lock held by another thread.
    pub visibility_contended: u64,
    /// Cross-shard commits' two-phase rounds and the PREPAREs participants refused, by
    /// reason (ICR 0043).
    pub cross_shard: CrossShardMetrics,
    /// The drift gauge (D271, ICR 0064): how far the largest default timestamp handed out is
    /// ahead of the clock now, and the largest such lead at any reservation since open, in
    /// µs. Both 0 while the clock leads.
    pub default_ts_lead_micros: (u64, u64),
}

/// Cross-shard commits' two-phase rounds and refusals since open, summed over shards
/// (ICR 0043; [`Metrics::cross_shard`]).
///
/// A participant that refuses a PREPARE makes the round abort everywhere, and the commit
/// runs again in a new round (or fails with [`Error::Busy`] at the write-stall timeout,
/// D241). `refused_rounds` over the cross-shard commits in an interval is the wasted rounds
/// per commit; `rounds - refused_rounds` counts the rounds that committed or failed for
/// another reason (a conflict, an I/O error). One refused round can hold several refusals,
/// one per refusing participant.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CrossShardMetrics {
    /// Two-phase rounds decided, by every coordinator.
    pub rounds: u64,
    /// Rounds that a participant refused.
    pub refused_rounds: u64,
    /// Always 0 since D263: a PREPARE carries no timestamp, so none is refused for its
    /// order with the participant's writes (D11, D244, D245 before it). Removed at the 1.0
    /// API freeze.
    #[deprecated(note = "always 0 since D263; removed at the 1.0 API freeze")]
    pub refused_below_floor: u64,
    /// Always 0 since D263: a PREPARE carries no timestamp, so none is refused below a
    /// tablet move's raise (D11 before it). Removed at the 1.0 API freeze.
    #[deprecated(note = "always 0 since D263; removed at the 1.0 API freeze")]
    pub refused_move_raise: u64,
    /// PREPAREs refused because a row's tablet was being split, merged or moved, or had
    /// moved to another shard (D145).
    pub refused_moved: u64,
    /// PREPAREs refused because the participant had no arena room for the share (ICR 0042).
    pub refused_room: u64,
    /// PREPAREs refused because they overlapped a row of an undecided share another commit
    /// prepared there (D249, ICR 0045). Retried once it is decided; not a conflict.
    pub refused_pending_overlap: u64,
    /// Commits so refused that read a row the overlapping share then committed a write to,
    /// after their snapshot: they fail with [`Error::Conflict`] at once instead of retrying
    /// into a validation failure (D276, ICR 0066). Once per commit, by its coordinator,
    /// however many participants found it doomed.
    pub failed_fast: u64,
    /// Singles held back behind an undecided share (D263), once per group
    /// that held them.
    pub share_waits: u64,
    /// The time shards had at least one single held back behind an
    /// undecided share (D263), summed over shards.
    pub share_wait_nanos: u64,
    /// Always 0 since D271: a blind write over an undecided share's write is admitted, so no
    /// PREPARE is chained (D267's chaining went). Kept for the API (ICR 0056).
    pub chained: u64,
    /// Always 0 since D271: nothing chains, so nothing is wounded (D267). Kept for the API.
    pub wounds: u64,
    /// The most attempts (two-phase rounds) one committed cross-shard commit that only
    /// writes took, over every coordinator since open (ICR 0056). Only commits that
    /// committed count: one that gave up with `Busy` at the write-stall timeout never shows
    /// here, so read it together with the `Busy` failures.
    pub max_attempts_blind: u64,
    /// The same for commits that read (validated reads): with no hard bound on their
    /// refusals (D249, D263, D267), the worst case seen. Committed commits only, as above.
    pub max_attempts_reading: u64,
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
    /// Nanoseconds the shard's engine-owned thread spent in work turns, not spinning or
    /// parked (D217, ICR 0033); zero for an application-owned shard. Its rise over a phase,
    /// divided by the phase's length, is the fraction of the time the shard was busy.
    pub busy_nanos: u64,
    /// Nanoseconds of group-sync in-flight time: the integral over time of the shard's group
    /// syncs in flight (D248), as of the last sync submitted or completed. Its rise over an
    /// interval, divided by the interval (capped at 1), is the sync utilization packing
    /// counts as busy. Zero for `Buffered` and `None` commits.
    pub sync_busy_nanos: u64,
    /// Nanoseconds the compaction pool's threads spent in slices of this shard's flushes
    /// and compactions (D256); zero with the pool off (they run inside the shard's turns
    /// and count in `busy_nanos`). Its rise over an interval, divided by the interval, is
    /// the pool term packing adds to the shard's turn busy.
    pub pool_busy_nanos: u64,
    /// Nanoseconds the shard's engine-owned thread spent in its spin before parking (D198:
    /// polling for the next message after handling one), not in `busy_nanos` (D256; the
    /// spin window is `EngineOptions::shard_spin_nanos`). Its rise over an interval, divided
    /// by the interval, is how much of the time the shard was awake without work: since
    /// D257 reported, not a packing term. Zero for an application-owned shard.
    pub spin_time_nanos: u64,
    /// Nanoseconds the client commits this shard completed spent in flight, from submit to
    /// reply (D257), plus the cross-shard shares it decided, from submit to the decision
    /// (D262). Its rise over an interval, divided by the interval, is the mean number of
    /// commit rounds in flight at the shard: summed over shards, the concurrency packing
    /// sizes the active shards by.
    pub commit_inflight_nanos: u64,
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
}

/// What a reader used in a process other than the one that opened it fails with
/// (`Unsupported`, perf287's review of #898, F1). A forked child inherits the reader's file
/// handle, which on Linux shares the parent's open file description: a slot it claimed
/// through it would share locks with the parent's, so each could release the other's and
/// the child's slot would outlive it.
pub(crate) const FORKED_READER: &str =
    "this reader was opened in another process (fork); open a new reader here";

/// What a writer engine used in a process other than the one that opened it fails with
/// (`Unsupported`, #901): a forked child has none of its shard, compaction or I/O threads,
/// shares its descriptors (on Linux its open file descriptions) and its shared-memory
/// arenas, which the parent keeps writing and reclaiming. Any operation there would hang
/// or read memory the parent reuses.
pub(crate) const FORKED_ENGINE: &str =
    "this database was opened in another process (fork); open it again here";

/// The error a writer-process handle (an engine, a snapshot) fails with in a forked child.
#[cold]
pub(crate) fn forked() -> Error {
    Error::Unsupported(FORKED_ENGINE)
}

/// Claims a reader slot through `file`, its liveness lock taken through that handle (#693
/// PR 3). Refused in a process other than `opened_in` (a forked child, [`FORKED_READER`]).
/// The reader abandons its previous slot before every re-claim, so `file` holds no slot's
/// liveness byte here: the claim would skip such a byte, costing a slot (perf287's review
/// of #898, S2).
fn claim_slot(
    shm: &ShmRegion,
    process: pigeonhole_io::ProcessId,
    file: &FileRef,
    opened_in: u32,
) -> Result<pigeonhole_shm::ReaderSlot> {
    if pigeonhole_io::sys::process_id() != opened_in {
        return Err(Error::Unsupported(FORKED_READER));
    }
    debug_assert!(
        (0..u64::from(shm.reader_slot_usage().1)).all(|i| {
            file.holds(pigeonhole_format::superblock::READER_LOCK_BASE + i)
                .is_none()
        }),
        "a reader claims a slot through a handle that holds another slot's liveness byte"
    );
    Ok(shm.claim_reader_slot_locked(process, file)?)
}

/// A reader process's attachment to one region generation.
struct Attachment {
    shm: ShmRegion,
    /// This generation's slot and the pins its live snapshots hold there (#693 PR 2).
    pins: Arc<crate::snapshot::ReaderPins>,
    /// This generation's row cache (D281): built at each attach, so a re-attach (a writer
    /// restart, a recovery, a handover) starts empty and never compares against another
    /// generation's watermarks. `None` when the reader's row cache is off.
    row_cache: Option<Arc<crate::row_cache::RowCaches>>,
}

/// A reader process's row cache over `shm`'s watermarks (D281), `None` when `options` turn
/// it off.
fn reader_row_cache(
    options: &EngineOptions,
    shm: &ShmRegion,
) -> Option<Arc<crate::row_cache::RowCaches>> {
    let epochs = crate::row_cache::RowEpochs::new(shm.row_epochs());
    // A writer that does not raise the slots (its row cache off): no cache here, never one
    // keyed on slots that stay 0.
    if !epochs.maintained() {
        return None;
    }
    crate::row_cache::RowCaches::new(
        options.row_cache_bytes,
        options.row_cache_max_row,
        options.row_cache_families.clone(),
        Arc::new(epochs),
    )
    .map(Arc::new)
}

/// Everything behind an [`Engine`] (and the handle a [`Txn`] keeps).
pub(crate) struct Inner {
    pub(crate) shared: Arc<Shared>,
    /// One handle on `shared` per shard, for pending commits (D236).
    shard_refs: Box<[Arc<ShardRef>]>,
    role: Role,
    /// Refuses writes, catalog changes and maintenance with `ReadOnly`: a reader process, or
    /// a writer opened read-only by `read_only_fallback` (ICR 0059), which writes nothing.
    read_only: bool,
    options: EngineOptions,
    path: PathBuf,
    runtime: Mutex<Option<Runtime<ShardState>>>,
    application_owned: bool,
    /// Application-owned mode's background workers (D284): closed first by the close, which
    /// takes what they hold back to the shards; and, until the open hands them out, the
    /// workers themselves.
    workers: std::sync::OnceLock<WorkerPool>,
    unclaimed_workers: Mutex<Vec<PoolDriver>>,
    closing: AtomicBool,
    reader: Option<ReaderState>,
    /// Longest stored value a WAL record and a memtable entry carry (decision D16): a
    /// longer put is separated into a blob file when it is routed (#230). Atomic so a test
    /// can shrink it (`Engine::set_inline_value_limit`).
    max_value: AtomicUsize,
    /// Async commits' backpressure (D210); writer only.
    admission: Option<Arc<crate::admission::Admission>>,
    /// The process that opened it ([`pigeonhole_io::sys::process_id`]). In any other process
    /// (a forked child's copy) every entry point fails with `Unsupported` and close and drop
    /// do nothing (#901, and #898 for a reader).
    opened_in: u32,
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

/// [`Inner::fast_admit`]'s outcome: a charge (none when unbounded) to submit with, or the
/// admission to wait on.
/// The engine's shared state as one shard's commits reach it (D236): one handle per shard,
/// each in its own cache lines, so the reference count a pending commit takes is its shard's
/// alone rather than one every committing thread writes.
#[derive(Debug)]
#[repr(align(128))]
pub(crate) struct ShardRef(Arc<Shared>);

impl std::ops::Deref for ShardRef {
    type Target = Shared;
    fn deref(&self) -> &Shared {
        &self.0
    }
}

/// What [`Inner::route_and_send`] did with a commit's request.
pub(crate) enum Routed<C> {
    /// Sent to `target`; the commit's waiter, whether a wait may spin, and the admission
    /// charge it took (`()` for a commit that takes none).
    Sent {
        target: ShardId,
        waiter: Waiter<Result<crate::write::Settled>>,
        spins: bool,
        charge: C,
    },
    /// Built and held back, to send once admitted.
    Held(Prepared),
}

/// How [`Inner::route_and_send`] admits a request once it knows its shard (D236): a
/// blocking commit takes no admission ([`NoAdmission`]); an async one charges its shard's
/// count (`(&Admission, bytes)`). Generic, so the blocking path compiles to no admission
/// code at all.
pub(crate) trait Admitter {
    type Charge;
    /// The charge on `target`, or `None` to hold the request back.
    fn admit(self, target: ShardId) -> Option<Self::Charge>;
}

/// No admission: every request is sent.
pub(crate) struct NoAdmission;

impl Admitter for NoAdmission {
    type Charge = ();
    #[inline(always)]
    fn admit(self, _: ShardId) -> Option<()> {
        Some(())
    }
}

impl Admitter for (&crate::admission::Admission, u64) {
    type Charge = crate::admission::Charge;
    #[inline(always)]
    fn admit(self, target: ShardId) -> Option<crate::admission::Charge> {
        self.0.try_fast(usize::from(target.0), self.1)
    }
}

/// A routed commit request not yet sent ([`Inner::route_and_send`]).
pub(crate) struct Prepared {
    /// The shard it goes to: its only shard, or a cross-shard commit's coordinator.
    pub(crate) target: ShardId,
    msg: ShardMsg,
    waiter: Waiter<Result<crate::write::Settled>>,
    spins: bool,
}

pub(crate) enum FastAdmit {
    Charged(crate::admission::Charge),
    Wait(crate::Admit),
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

fn block_cache(bytes: usize, _shards: usize) -> Arc<BlockCache> {
    Arc::new(if bytes == 0 {
        BlockCache::disabled()
    } else {
        // The cache's own default (up to 64 shards of at least 256 KiB), not one per engine
        // shard: every reader thread reads through it, so one engine shard meant one lock
        // for all of them (#18).
        BlockCache::new(bytes, 0)
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
    Commit,
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
        let (engine, _) =
            Self::open_writer(path, options, Mode::EngineOwned, None).map_err(|(e, _)| e)?;
        Ok(engine)
    }

    /// Opens the database an engine closed with [`Engine::close_for_reopen`] left, on the
    /// writer lock and presence that close kept (D265): as [`Engine::open`], without taking
    /// them, so no other writer could open the file in between and no reader removed its
    /// shared memory. The WAL is replayed as after a crashed writer. `path` must be the
    /// handover's file (`InvalidArgument` otherwise). On an error the handover comes back
    /// with it, the locks still held, so the caller can try again without opening the window
    /// (D265's budget: an `Io` at reopen retries). For `pigeonhole`'s automatic recovery
    /// (#620); not a stable API.
    #[doc(hidden)]
    pub fn open_with(
        path: &Path,
        options: EngineOptions,
        handover: Handover,
    ) -> std::result::Result<Arc<Engine>, (Error, Handover)> {
        match Self::open_writer(path, options, Mode::EngineOwned, Some(handover)) {
            Ok((engine, _)) => Ok(engine),
            // A failed open always gives the handover back (the locks move into the engine
            // as its last step); the `None` arm is unreachable by construction.
            Err((e, handover)) => Err((
                e,
                handover.unwrap_or_else(|| Handover {
                    locks: None,
                    path: path.to_path_buf(),
                }),
            )),
        }
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
        if options.background_workers != 0 {
            return Err(Error::InvalidArgument(
                "background_workers needs Engine::open_application_owned_with, which hands \
                 the workers out (D284)"
                    .to_owned(),
            ));
        }
        let owned = Self::open_application_owned_with(path, options)?;
        Ok((owned.engine, owned.shards))
    }

    /// As [`Engine::open_application_owned`], also handing out `options.background_workers`
    /// background workers (D284) for the application to drive from threads it owns, as it
    /// drives the shards: flush and compaction then run on them, not in slices on the
    /// shards. Still starts no threads (decision D40).
    ///
    /// The engine's close never waits for the application to drive a worker: it takes
    /// what the workers hold back to the shards (flushes finish there; compactions are
    /// dropped and run again after the next open), waiting only for a slice a worker is
    /// running at that moment. A worker that is never driven delays its tasks until the
    /// shards take them back (10 ms after a pass by any worker, or 2 ms into a wait for
    /// memtable room), which `Submitter`-level counters record.
    pub fn open_application_owned_with(path: &Path, options: EngineOptions) -> Result<AppOwned> {
        if options.compaction_threads != 0 {
            return Err(Error::InvalidArgument(
                "compaction_cores must be 0 in application-owned mode (decision D40)".to_owned(),
            ));
        }
        let (engine, shards) =
            Self::open_writer(path, options, Mode::ApplicationOwned, None).map_err(|(e, _)| e)?;
        let workers = std::mem::take(
            &mut *engine
                .inner
                .unclaimed_workers
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
        .into_iter()
        .map(|driver| BackgroundWorker {
            driver,
            engine: Arc::clone(&engine.inner),
        })
        .collect();
        Ok(AppOwned {
            engine,
            shards: shards.unwrap_or_default(),
            workers,
        })
    }

    /// `open_writer_inner`, keeping the locks out of the engine until it is fully open: on an
    /// error they come back (with a handover, the one that came in), so a reopen can retry
    /// without releasing them (D265).
    #[allow(clippy::type_complexity)]
    fn open_writer(
        path: &Path,
        options: EngineOptions,
        mode: Mode,
        handover: Option<Handover>,
    ) -> std::result::Result<(Arc<Engine>, Option<Vec<EngineShard>>), (Error, Option<Handover>)>
    {
        let handed_over = handover.is_some();
        let mut held = match handover {
            Some(Handover { locks, path: from }) => {
                if from != path {
                    return Err((
                        Error::InvalidArgument(format!(
                            "open_with: {} is not the handover's file {}",
                            path.display(),
                            from.display()
                        )),
                        Some(Handover { locks, path: from }),
                    ));
                }
                locks.map(|l| *l)
            }
            None => None,
        };
        match Self::open_writer_inner(path, options, mode, &mut held) {
            Ok(opened) => Ok(opened),
            Err(e) => Err((
                e,
                handed_over.then(|| Handover {
                    locks: held.map(Box::new),
                    path: path.to_path_buf(),
                }),
            )),
        }
    }

    /// `held`: the locks of a handover (D265), or `None` to take them; filled at step 3 and
    /// moved into the engine as the last step, so an error leaves them in `held`.
    fn open_writer_inner(
        path: &Path,
        options: EngineOptions,
        mode: Mode,
        held: &mut Option<Locks>,
    ) -> Result<(Arc<Engine>, Option<Vec<EngineShard>>)> {
        let shards = if options.shards == 0 {
            pigeonhole_io::sys::default_shards()
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
        //    A handover (D265) brings the handle with both locks held: nothing to take.
        let (file, writer_lock) = match held.as_ref() {
            Some(locks) => (locks.file.clone(), None),
            None => {
                let mut open_opts = OpenOptions::read();
                open_opts.write = true;
                let file = vfs.open(path, open_opts)?;
                check_local(&file, options.allow_fuse)?;
                let writer_lock = WriterLock::acquire(&file)?;
                (file, Some(writer_lock))
            }
        };
        let identity = file.identity()?;

        // 2. The page file and the manifest. A file this build may read but not write (a
        //    read-only-compatible feature bit it does not know, D269) opens read-only under
        //    `read_only_fallback` (ICR 0059): `no_writes`, and nothing below writes to it.
        let mut no_writes = false;
        let opened = match Pager::open(&vfs, path, true) {
            Ok(o) => o,
            Err(pigeonhole_pager::Error::Format(
                pigeonhole_format::Error::UnsupportedFeature { .. },
            )) if options.read_only_fallback => {
                no_writes = true;
                Pager::open(&vfs, path, false)?
            }
            // A newer build's file is intact: say so, not "no valid superblock".
            Err(pigeonhole_pager::Error::Format(
                e @ (pigeonhole_format::Error::UnsupportedFeature { .. }
                | pigeonhole_format::Error::UnsupportedVersion { .. }),
            )) => return Err(e.into()),
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
        // Refuse what will be refused before `finish`, which may upgrade the file's
        // superblocks: a refused open must leave an older file as it was (#496). An
        // unregistered merge operator opens read-only under `read_only_fallback`.
        if catalog.has_unknown_merge && !options.read_only_fallback {
            let name = catalog
                .tables()
                .flat_map(|t| t.families.iter())
                .map(|f| f.options.merge_operator.clone())
                .find(|n| !n.is_empty() && registry.get(n).is_none())
                .unwrap_or_default();
            return Err(Error::UnknownMergeOperator(name));
        }
        no_writes |= catalog.has_unknown_merge;
        let opened = if no_writes {
            opened.into_read_only()
        } else {
            opened
        };
        let mut live = manifest_extents;
        live.extend(catalog.data_extents());
        // `finish` upgrades an older file's superblocks before anything below writes
        // (current-version bytes behind older superblocks are what an older build skips as
        // torn, #774). A read-only pager neither syncs nor upgrades: a `no_writes` open
        // leaves the file exactly as it was.
        let pager = Arc::new(opened.finish(live)?);
        open_data_file(&vfs, path, &pager, options.direct_io, !no_writes)?;
        if let Some((block, from)) = options.grow_ahead
            && !no_writes
        {
            pager.set_grow_ahead(block, from)?;
        }
        let read_only = no_writes;

        // 3. Shared memory under a new generation, then presence (D37). On a handover the
        //    presence is already held on this handle: the region's `present(file)` is a
        //    no-op re-take (shared to shared, same owner), and D37's "not already present"
        //    constraint is not hit, since a handover's old region has the current layout
        //    version (`alone()` is reached only for another version).
        let mut shm_config = ShmConfig::new(shards as u32);
        shm_config.arena_bytes = options.memtable_budget;
        shm_config.reader_slots = options.reader_slots.max(1);
        shm_config.dir = options.shm_dir.clone();
        shm_config.first_seqno = first_seqno(catalog.counters.seqno_ceiling, 0);
        shm_config.row_epoch_slots = crate::row_cache::table_slots(options.row_cache_bytes);
        // The slots are raised only with the writer's own row cache on, and the header says
        // so: readers cache only then (D281; a raise on every commit cost the cache-off
        // commit shapes 1.5%).
        shm_config.row_epochs_maintained = options.row_cache_bytes > 0;
        let shm = ShmRegion::open(&vfs, &file, identity, db_id, ShmRole::Writer, &shm_config)?;
        // A new generation, so new zero slots: reader processes re-attach, which clears their
        // caches (D281; every writer open, a recovery's and a handover's too, builds one).
        let row_epochs = Arc::new(crate::row_cache::RowEpochs::new(shm.row_epochs()));
        if held.is_none() {
            let presence = Presence::acquire(&file)?;
            *held = Some(Locks {
                _writer: writer_lock.expect("taken at step 1 when there is no handover"),
                presence,
                file: file.clone(),
            });
        }

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
            released: false,
        });
        let shared = Arc::new(Shared {
            vfs: Arc::clone(&vfs),
            shm: shm.clone(),
            reader_reclaim_at: AtomicU64::new(0),
            reader_slots_reclaimed: AtomicU64::new(0),
            shards,
            view: ArcSwap::new(empty_view),
            async_sync_reads: AtomicU64::new(0),
            commit_parks: AtomicU64::new(0),
            view_lock: Mutex::new(0),
            view_record: Mutex::default(),
            manifest: Mutex::new(ManifestWriter::new(Arc::clone(&pager))),
            manifest_queue: Default::default(),
            manifest_busy: AtomicBool::new(false),
            pager: Arc::clone(&pager),
            cache: Arc::clone(&cache),
            row_cache: crate::row_cache::RowCaches::new(
                options.row_cache_bytes,
                options.row_cache_max_row,
                options.row_cache_families.clone(),
                Arc::clone(&row_epochs),
            ),
            row_epochs: row_epochs.maintained().then_some(row_epochs),
            sst_ids: Arc::new(AtomicU64::new(catalog.counters.next_sst.max(1))),
            blob_ids: Arc::new(AtomicU32::new(catalog.counters.next_blob_file.max(1))),
            live_views: Arc::clone(&live_views),
            live_seqnos: Arc::new(LiveSeqnos::default()),
            flushed_roots: Default::default(),
            busy_ssts: Mutex::new(HashSet::new()),
            large: Default::default(),
            large_open: AtomicUsize::new(0),
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
            // Filled as the last step of the open (`held`).
            locks: Mutex::new(None),
            keep_locks: AtomicBool::new(false),
            arena_discard: AtomicU8::new(crate::shard::ARENAS_PENDING),
            close_done: AtomicBool::new(false),
            application_owned: mode == Mode::ApplicationOwned,
            no_writes,
            lock_file: Some(file.clone()),
            opened_in: pigeonhole_io::sys::process_id(),
            default_durability: AtomicU8::new(options.durability as u8),
            closed: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            pager_poisoned: AtomicBool::new(false),
            failure: Default::default(),
            close: CloseState::new(shards),
            metrics: (0..shards).map(|_| ShardMetrics::default()).collect(),
            flushed_through: (0..shards).map(|_| Padded(AtomicU64::new(0))).collect(),
            clock: Default::default(),
            loads: (0..shards).map(|_| LoadSlot::default()).collect(),
            balance: BalanceConfig {
                enabled: options.tablet_changes,
                interval_nanos: options.balance_interval_nanos,
                min_writes: options.balance_min_writes,
                skew: options.balance_skew,
                split_bytes: options.tablet_split_bytes.max(1),
                // Application-owned shards report no busy time, so packing never acts on
                // them (D217): D213's pinned placement stays.
                pack: options.balance_pack,
                cold_passes: options.balance_cold_passes.unwrap_or(COLD_PASSES),
            },
            view_capacity: shm_config.view_buffer_bytes as usize,
            tablet_epoch: AtomicU64::new(0),
            full_compactions: AtomicUsize::new(0),
            pack_k: AtomicUsize::new(0),
            pack_target: AtomicUsize::new(0),
            waiters: WatermarkWaiters::new(shards),
            durable_waiters: WatermarkWaiters::new(shards),
            visibility: VisibilityCounts::default(),
            shard_died: AtomicBool::new(false),
            manifest_flight: Default::default(),
            drivers: if mode == Mode::ApplicationOwned {
                crate::waker::Drivers::new(shards)
            } else {
                Default::default()
            },
            memtable_freeze_bytes: freeze_bytes,
            wal_pin_bytes: match options.wal_pin_bytes {
                0 => options.memtable_budget.saturating_mul(2),
                n => n,
            },
            submitters: std::sync::OnceLock::new(),
            wal_io: (0..shards).map(|_| std::sync::OnceLock::new()).collect(),
            shm_dir: options.shm_dir.clone(),
            identity,
            path: path.to_path_buf(),
        });
        // D290: the write-through overlays only puts newer than every delete the file holds.
        if let Some(rc) = &shared.row_cache {
            rc.note_delete(
                catalog
                    .ssts
                    .values()
                    .flat_map(|list| list.iter())
                    .filter(|(_, m)| m.deletes > 0)
                    .map(|(_, m)| m.ts_range.1)
                    .max()
                    .unwrap_or(0),
            );
        }
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
        // A closed instance whose last view outlived its close releases its memory once the
        // view goes (D265): the arena pages are discarded only under the registry's lock, when
        // no view can read them.
        let weak = Arc::downgrade(&shared);
        live_views.on_all_released(Box::new(move || {
            if let Some(shared) = weak.upgrade() {
                shared.release_memory();
            }
        }));
        // Counter checkpoints (D291): a get through one of this writer's views hands its
        // result to the row's shard. Best effort: before the runtime starts, or once the
        // engine is gone or its inbox closed, the checkpoint is dropped.
        if options.counter_checkpoint_steps > 0 {
            pigeonhole_compaction::COUNTER_FOLD_MIN.fetch_min(
                options.counter_checkpoint_steps,
                std::sync::atomic::Ordering::Relaxed,
            );
            let weak = Arc::downgrade(&shared);
            live_views.on_counter_checkpoint(
                options.counter_checkpoint_steps,
                Box::new(move |c: crate::read::CounterCheckpoint| {
                    if let Some(shared) = weak.upgrade()
                        && let Some(submitters) = shared.submitters.get()
                    {
                        let shard = usize::from(c.shard.0);
                        let _ = submitters[shard].submit(ShardMsg::CounterCheckpoint(Box::new(c)));
                    }
                }),
            );
        }
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
        // With the seqno and timestamp a DECISION applies the commit at (D263);
        // `None` for a 0.3.0 COMMIT, whose PREPAREs apply at their own.
        #[allow(clippy::type_complexity)]
        let mut commits: HashMap<(StreamId, Seqno), (Vec<StreamId>, Option<(Seqno, u64)>)> =
            HashMap::new();
        let mut replayed: Vec<(StreamId, Vec<ReplayedRecord>)> = Vec::new();
        // Recovered memtables written to SSTs mid-replay when an arena ran short (#143).
        let mut spill = Spilled::new(Arc::clone(&pager));
        spill.refuse = no_writes;
        let streams = discover_streams(&vfs, path)?;
        for &stream in &streams {
            let checkpoint = catalog
                .checkpoints
                .get(&stream)
                .copied()
                .unwrap_or_default();
            let mut rec = if no_writes {
                Recovery::open_read_only(&vfs, path, stream, db_id, checkpoint)?
            } else {
                Recovery::open(&vfs, path, stream, db_id, checkpoint)?
            };
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
                        commits.insert((stream, seqno), (list, None));
                        records.push(ReplayedRecord {
                            end,
                            seqno,
                            kind: ReplayedRecordKind::Commit,
                        });
                    }
                    WalRecord::Decision {
                        seqno,
                        apply_seqno,
                        commit_ts,
                        participants,
                    } => {
                        let list: Vec<StreamId> = participants.iter().collect();
                        commits.insert((stream, seqno), (list, Some((apply_seqno, commit_ts))));
                        records.push(ReplayedRecord {
                            end,
                            seqno,
                            kind: ReplayedRecordKind::Commit,
                        });
                    }
                    // `WalRecord::decode` returns no other type in this build.
                    _ => return Err(Error::Corruption("wal record type".into())),
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
                .is_some_and(|(ps, _)| ps.iter().all(|p| prepared.contains(&(*p, seqno))))
        };
        // `(participant stream, seqno) -> slots written on each shard` of applied prepares.
        let mut applied_slots: HashMap<(StreamId, Seqno), (Seqno, Vec<Slots>)> = HashMap::new();
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
            let (apply_seqno, apply_ts) = commits
                .get(&(*coordinator, *seqno))
                .and_then(|(_, at)| *at)
                .unwrap_or((*seqno, *commit_ts));
            let mut per_shard = Vec::with_capacity(states.len());
            for s in &mut states {
                per_shard.push(
                    s.replay(bytes, apply_seqno, apply_ts)
                        .map_err(replay_error)?,
                );
            }
            applied_slots.insert((*stream, *seqno), (apply_seqno, per_shard));
        }
        let next = first_seqno(catalog.counters.seqno_ceiling, max_seqno);
        // The allocator starts above every recovered seqno, and shm mirrors it (D271); its
        // timestamp is seeded once replay has raised the floors (below).
        shm.advance_next_seqno(next);
        shared.clock.seed_seqno(shm.next_seqno());

        // 6. Streams. With the same layout as before, every stream's unflushed records are
        // logged on its shard for checkpointing. Otherwise (D20), or when replay already
        // spilled recovered memtables to SSTs, everything recovered is flushed now, every
        // stream checkpointed to its end, and the extra streams removed.
        let same_layout = !spill.spilled
            && streams.len() == shards
            && streams.iter().enumerate().all(|(i, s)| s.0 as usize == i);
        let mut have_wal = vec![false; shards];
        // The shards the replay applied a commit's shares to: its records' holders (D266),
        // whichever shards held them before (a reopen places tablets again).
        let holders_of = |coordinator: StreamId, seqno: Seqno| -> crate::shard::Holders {
            let mut on = std::collections::BTreeSet::new();
            if let Some((ps, _)) = commits.get(&(coordinator, seqno)) {
                for p in ps.iter() {
                    if let Some((_, per)) = applied_slots.get(&(*p, seqno)) {
                        on.extend(
                            per.iter()
                                .enumerate()
                                .filter(|(_, slots)| !slots.is_empty())
                                .map(|(i, _)| ShardId(i as u16)),
                        );
                    }
                }
            }
            on.into_iter().collect()
        };
        if no_writes {
            // A read-only open (ICR 0059) keeps what it replayed in its memtables, whatever
            // the layout (replay routes each record to the shard owning its rows), and
            // writes no stream: no new segment, no flush, no checkpoint. The WAL files stay
            // as they are for the next writable open to replay.
            drop(replayed);
            drop(recoveries);
        } else if same_layout {
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
                                applied.map(|(_, per)| per.concat()).unwrap_or_default()
                            } else {
                                applied
                                    .map(|(_, per)| per.get(i).cloned().unwrap_or_default())
                                    .unwrap_or_default()
                            };
                            ReplayedKind::Prepare {
                                slots,
                                applied: applied
                                    .map(|(s, _)| (*s, holders_of(coordinator, r.seqno))),
                            }
                        }
                        ReplayedRecordKind::Commit => ReplayedKind::Commit {
                            holders: holders_of(stream, r.seqno),
                            apply: complete(stream, r.seqno).then(|| {
                                commits
                                    .get(&(stream, r.seqno))
                                    .and_then(|(_, at)| *at)
                                    .map_or(r.seqno, |(s, _)| s)
                            }),
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
            .filter(|&i| !have_wal[i] && !no_writes)
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
        if !no_writes {
            sweep_unreferenced_blobs(&shared, &mut catalog, &states, shards)?;
        }

        // D271: the allocator's `H` is the largest default timestamp handed out: the
        // manifest's floor and every replayed timestamp (§5), each shard's last stamp once
        // replay raised it. Not the clock: stamps take `max(now, ..)` themselves, and a
        // decision must only be above `H`.
        let h = states
            .iter()
            .map(|s| s.last_stamp())
            .max()
            .unwrap_or(0)
            .max(catalog.counters.ts_floor);
        shared.clock.seed_ts(h);

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
            released: false,
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
        config.shard_threads = options.shard_threads;
        config.adaptive_drivers = options.adaptive_drivers;
        config.idle_spin = std::time::Duration::from_nanos(options.shard_spin_nanos);
        let bound = match options.async_commit_bound {
            0 => options.memtable_budget / 2,
            b => b,
        };
        let admission = Some(Arc::new(crate::admission::Admission::new(shards, bound)));
        let inner = Arc::new(Inner {
            shared: Arc::clone(&shared),
            shard_refs: (0..shards.max(1))
                .map(|_| Arc::new(ShardRef(Arc::clone(&shared))))
                .collect(),
            role: Role::Writer,
            read_only,
            options,
            path: path.to_path_buf(),
            runtime: Mutex::new(None),
            workers: std::sync::OnceLock::new(),
            unclaimed_workers: Mutex::new(Vec::new()),
            application_owned: mode == Mode::ApplicationOwned,
            closing: AtomicBool::new(false),
            reader: None,
            max_value: AtomicUsize::new(max_value),
            admission,
            opened_in: pigeonhole_io::sys::process_id(),
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
                config.compaction_threads = inner.options.background_workers;
                let owned = Runtime::application_owned_with_workers(config, states)?;
                if let Some(pool) = owned.pool {
                    let _ = inner.workers.set(pool);
                }
                *inner
                    .unclaimed_workers
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = owned.workers;
                let drivers = owned.shards;
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
        // The locks move in last: an error above leaves them with the caller (a handover
        // retries on them, D265).
        *shared.locks.lock().unwrap_or_else(PoisonError::into_inner) = held.take();
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
        let opened_in = pigeonhole_io::sys::process_id();
        let slot = claim_slot(&shm, vfs.current_process(), &file, opened_in)?;
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
            released: false,
        });
        let shared = Arc::new(Shared {
            vfs: Arc::clone(&vfs),
            shm: shm.clone(),
            reader_reclaim_at: AtomicU64::new(0),
            reader_slots_reclaimed: AtomicU64::new(0),
            shards,
            view: ArcSwap::new(empty_view),
            async_sync_reads: AtomicU64::new(0),
            commit_parks: AtomicU64::new(0),
            view_lock: Mutex::new(0),
            view_record: Mutex::default(),
            manifest: Mutex::new(ManifestWriter::new(Arc::clone(&pager))),
            manifest_queue: Default::default(),
            manifest_busy: AtomicBool::new(false),
            pager: Arc::clone(&pager),
            cache: Arc::clone(&cache),
            // Reader processes have no write watermarks, so no row cache (D201).
            row_cache: None,
            row_epochs: None,
            sst_ids: Arc::new(AtomicU64::new(0)),
            blob_ids: Arc::new(AtomicU32::new(0)),
            live_views: Arc::new(LiveViews::default()),
            live_seqnos: Arc::new(LiveSeqnos::default()),
            flushed_roots: Default::default(),
            busy_ssts: Mutex::new(HashSet::new()),
            large: Default::default(),
            large_open: AtomicUsize::new(0),
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
            keep_locks: AtomicBool::new(false),
            arena_discard: AtomicU8::new(crate::shard::ARENAS_PENDING),
            close_done: AtomicBool::new(false),
            application_owned: false,
            no_writes: true,
            lock_file: None,
            opened_in: pigeonhole_io::sys::process_id(),
            default_durability: AtomicU8::new(options.durability as u8),
            closed: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            pager_poisoned: AtomicBool::new(false),
            failure: Default::default(),
            close: CloseState::new(0),
            metrics: Vec::new(),
            clock: Default::default(),
            flushed_through: Vec::new(),
            loads: Vec::new(),
            balance: BalanceConfig::default(),
            view_capacity: shm_config.view_buffer_bytes as usize,
            tablet_epoch: AtomicU64::new(0),
            full_compactions: AtomicUsize::new(0),
            pack_k: AtomicUsize::new(0),
            pack_target: AtomicUsize::new(0),
            waiters: WatermarkWaiters::new(shards),
            durable_waiters: WatermarkWaiters::new(shards),
            visibility: VisibilityCounts::default(),
            shard_died: AtomicBool::new(false),
            manifest_flight: Default::default(),
            drivers: Default::default(),
            memtable_freeze_bytes: options.memtable_freeze_bytes.max(1),
            wal_pin_bytes: 0,
            submitters: std::sync::OnceLock::new(),
            wal_io: (0..shards).map(|_| std::sync::OnceLock::new()).collect(),
            shm_dir: options.shm_dir.clone(),
            identity,
            path: path.to_path_buf(),
        });
        let reader = ReaderState {
            file,
            presence: Mutex::new(Some(presence)),
            shm: Mutex::new(Attachment {
                row_cache: reader_row_cache(&options, &shm),
                shm,
                pins: crate::snapshot::ReaderPins::new(slot),
            }),
            catalog: Mutex::new((manifest_version, Arc::new(catalog), manifest_file)),
            view: Mutex::new(None),
            shards,
            registry,
        };
        let inner = Arc::new(Inner {
            // A reader commits nothing; one handle keeps `shard_ref` total.
            shard_refs: Box::new([Arc::new(ShardRef(Arc::clone(&shared)))]),
            shared,
            role: Role::Reader,
            read_only: true,
            options,
            path: path.to_path_buf(),
            runtime: Mutex::new(None),
            workers: std::sync::OnceLock::new(),
            unclaimed_workers: Mutex::new(Vec::new()),
            application_owned: false,
            closing: AtomicBool::new(false),
            reader: Some(reader),
            max_value: AtomicUsize::new(0),
            admission: None,
            opened_in,
        });
        Ok(Arc::new(Engine { inner }))
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
        if self.inner.forked() {
            return None;
        }
        // In a writer, looked up under the view's guard: a row read checks its table's entry
        // on every call, and cloning the catalog's `Arc` was a reference count every reader
        // thread wrote (#709).
        if !self.inner.is_reader() {
            return self
                .inner
                .shared
                .view
                .load()
                .catalog
                .table_by_name(name)
                .cloned();
        }
        self.inner.catalog().table_by_name(name).cloned()
    }

    /// Every table.
    pub fn tables(&self) -> Vec<Arc<TableInfo>> {
        if self.inner.forked() {
            return Vec::new();
        }
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
        if self.inner.forked() {
            return;
        }
        self.inner
            .shared
            .default_durability
            .store(durability as u8, Ordering::Relaxed);
    }

    /// An async commit's admission for `batch` (D210): charged to the shard of its first row
    /// (a cross-shard batch's coordinator) at once when that shard has room and no earlier
    /// async commit waits, otherwise queued. Submit once [`Admit::poll_admit`](crate::Admit::poll_admit) is ready,
    /// then [`Admit::attach`](crate::Admit::attach) it to the submitted commit, which returns the charge when the
    /// commit resolves. Blocking commits take no admission.
    pub fn admit(&self, batch: &WriteBatch) -> crate::Admit {
        if self.inner.forked() {
            return crate::Admit::new(None, 0, 0);
        }
        self.inner.admit(batch)
    }

    /// Bytes of async commits charged to `shard` and not yet resolved (D210; tests and
    /// benches).
    #[doc(hidden)]
    pub fn admission_inflight(&self, shard: usize) -> u64 {
        if self.inner.forked() {
            return 0;
        }
        self.inner
            .admission
            .as_ref()
            .map_or(0, |a| a.inflight(shard))
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
        self.inner
            .submit(batch, durability, None, None)
            .map(PendingCommit::from)
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
        self.inner.commit_blocking(batch, durability)
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

    /// [`submit`](Self::submit) for an async commit (D210): submitted at once while the
    /// shards' async commits in flight leave room, with its charge returned when it
    /// resolves; otherwise the pending commit holds the batch and its place in its shard's
    /// queue, and submits it when polled once admitted. Dropped before then, it writes
    /// nothing.
    pub fn submit_async(
        &self,
        batch: WriteBatch,
        durability: Option<Durability>,
    ) -> Result<PendingCommit> {
        let inner = &self.inner;
        let Some(a) = &inner.admission else {
            return inner
                .submit(batch, durability, None, None)
                .map(PendingCommit::from);
        };
        // D236: routed once; the charge goes to the shard the request is sent to (a
        // cross-shard commit's coordinator), on that shard's own count.
        let bytes = crate::admission::batch_bytes(&batch);
        match inner.route_and_send(batch, durability, None, None, (&**a, bytes))? {
            Routed::Held(prepared) => {
                let shard = usize::from(prepared.target.0);
                let admit = crate::Admit::new(Some(a), shard, bytes);
                let engine = Arc::clone(inner);
                // Sent once admitted; the `Mutex` only makes the closure `Sync`.
                let prepared = Mutex::new(Some(prepared));
                Ok(PendingCommit::waiting(
                    inner.shard_ref(ShardId(shard as u16)),
                    admit,
                    Box::new(move || {
                        let prepared = prepared
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .take()
                            .ok_or(Error::Closed)?;
                        engine.send(prepared).map(PendingCommit::from)
                    }),
                ))
            }
            Routed::Sent {
                target,
                waiter,
                spins,
                charge,
            } => {
                let pending = PendingCommit::from(Submission {
                    waiter,
                    shared: inner.shard_ref(target),
                    spins,
                });
                let mut charge = charge;
                charge.attach(&pending.sub.waiter);
                Ok(pending)
            }
        }
    }

    /// [`submit_check_and_mutate`](Self::submit_check_and_mutate) for an async commit, as
    /// [`submit_async`](Self::submit_async).
    pub fn submit_check_and_mutate_async(
        &self,
        table: TableId,
        row: &[u8],
        predicate: &Predicate,
        batch: WriteBatch,
        durability: Option<Durability>,
    ) -> Result<PendingCheck> {
        let mut charge = match self.inner.fast_admit(Some((table, row)), &batch) {
            FastAdmit::Wait(admit) => {
                let inner = Arc::clone(&self.inner);
                let (row, predicate) = (row.to_vec(), predicate.clone());
                return Ok(PendingCheck::waiting(
                    self.inner.shard_ref(ShardId(admit.shard() as u16)),
                    admit,
                    Box::new(move || {
                        inner.submit_check_and_mutate(table, &row, &predicate, batch, durability)
                    }),
                ));
            }
            FastAdmit::Charged(charge) => charge,
        };
        // A failed submission drops the charge, which returns it.
        let pending = self
            .inner
            .submit_check_and_mutate(table, row, predicate, batch, durability)?;
        charge.attach(&pending.waiter);
        Ok(pending)
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

    /// A snapshot that sees a commit only once it is durable at its level on its shard (D227,
    /// ICR 0037): what [`Engine::snapshot`] saw before D227. Every commit it reads is durable
    /// at its level: `GroupSync` and `Sync` commits survive a power loss, `Buffered` ones a
    /// process crash, `None` ones nothing. It may lag the visible state by the syncs in
    /// flight. In a reader process it may do I/O, as `snapshot` does.
    pub fn durable_snapshot(&self) -> Result<Snapshot> {
        self.inner.snapshot_of(true)
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
                // The generation's row cache first (D281); a hit is owned data.
                if let Some(rc) = snapshot.reader_row_cache()
                    && let Some(hit) = crate::row_cache::get_cached(
                        rc,
                        &snapshot.view,
                        snapshot.seqno,
                        inner.shared.vfs.now_micros(),
                        table,
                        family,
                        row,
                        qualifier,
                    )
                {
                    return Ok(hit);
                }
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
        inner.check_process()?;
        for _ in 0..GET_LATEST_TRIES {
            let seqno = inner.shared.shm.visible_seqno();
            #[cfg(feature = "test-hooks")]
            inner
                .shared
                .hooks
                .sites
                .run(hooks::HookSite::LatestViewLoad);
            let view = inner.shared.view.load();
            if inner.shared.shm.visible_seqno() != seqno {
                continue;
            }
            if view.released {
                return Err(Error::Closed);
            }
            let clock = || inner.shared.vfs.now_micros();
            if let Some(rc) = &inner.shared.row_cache
                && let Some(got) = crate::row_cache::get_through(
                    rc,
                    &view,
                    seqno,
                    clock(),
                    table,
                    family,
                    row,
                    qualifier,
                )
            {
                return got;
            }
            return get_in::<false>(&view, seqno, clock, table, family, row, qualifier, || {
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
        snapshot.check_process()?;
        let families = families_in_order(&snapshot.view, table, &spec.families)?;
        let now = self.inner.shared.vfs.now_micros();
        snapshot.checked(read::read_row(snapshot, table, row, &families, spec, now))
    }

    /// Reads one row, projected by `spec`, into `sink`: the cells go straight into the
    /// caller's buffer, with no intermediate [`RowData`]. Reads `families` (empty: every
    /// family of the table) instead of `spec.families`, so a caller with the ids at hand need
    /// not build a `Vec` for them. Returns whether the row has a visible cell.
    pub fn read_row_into_families(
        &self,
        snapshot: &Snapshot,
        table: TableId,
        row: &[u8],
        families: &[FamilyId],
        spec: &ReadSpec,
        sink: &mut impl read::RowSink,
    ) -> Result<bool> {
        snapshot.check_process()?;
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

    /// A reader process's latest row read through its generation's row cache (D281): served
    /// or read at `snapshot`, a fill stored only while the snapshot still protects what it
    /// read, and the result checked as any snapshot read.
    #[allow(clippy::too_many_arguments)]
    fn read_row_cached_at(
        &self,
        rc: &crate::row_cache::RowCaches,
        snapshot: &Snapshot,
        table: TableId,
        row: &[u8],
        families: &[FamilyId],
        spec: &ReadSpec,
        sink: &mut impl read::RowSink,
    ) -> Result<bool> {
        snapshot.check_process()?;
        let families = families_in_order(&snapshot.view, table, families)?;
        let now = self.inner.shared.vfs.now_micros();
        snapshot.checked(crate::row_cache::read_row_cached(
            rc,
            &snapshot.view,
            snapshot.seqno,
            table,
            row,
            &families,
            spec,
            now,
            false,
            sink,
            || snapshot.check_current().is_ok(),
        ))
    }

    /// [`Engine::get_latest`] as a future that never blocks on a block the cache does not
    /// hold: it fetches what it misses through the VFS's asynchronous reads and reads again
    /// (D196, ICR 0014). In a writer process the get runs at the call first, as the sync one
    /// does: a memtable or cache hit resolves there, as of the call, and the future holds
    /// only its result (#709). Otherwise the read point is taken on the first poll.
    pub fn get_latest_async(
        &self,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
    ) -> crate::GetFuture {
        crate::GetFuture::new(&self.inner, None, table, family, row, qualifier)
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
            &self.inner,
            Some(snapshot.clone()),
            table,
            family,
            row,
            qualifier,
        )
    }

    /// [`Engine::read_row_latest_into`] that never blocks: the read runs now in cache-only
    /// mode, through the row cache, and returns `None` when it would need a block the cache
    /// does not hold (and in a reader process). `sink` may then hold some of the row's cells;
    /// start [`Engine::read_row_latest_async`] with an empty one. A memtable or cache hit is
    /// the async read's fast path: no future, no reference counted (#709).
    pub fn try_read_row_latest_into(
        &self,
        table: TableId,
        row: &[u8],
        families: &[FamilyId],
        spec: &ReadSpec,
        sink: &mut impl read::RowSink,
    ) -> Option<Result<bool>> {
        crate::nonblocking::try_latest_now(&self.inner, |view, seqno, now, rc| {
            let families = families_in_order(view, table, families)?;
            if let Some(rc) = rc
                && crate::row_cache::serves_spec(spec)
            {
                return crate::row_cache::read_row_cached(
                    rc,
                    view,
                    seqno,
                    table,
                    row,
                    &families,
                    spec,
                    now,
                    true,
                    sink,
                    || true,
                );
            }
            read::read_row_into(view, seqno, table, row, &families, spec, now, true, sink)
        })
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
                let read = match snapshot.reader_row_cache() {
                    Some(rc) if crate::row_cache::serves_spec(spec) => {
                        self.read_row_cached_at(rc, &snapshot, table, row, families, spec, sink)
                    }
                    _ => self.read_row_into_families(&snapshot, table, row, families, spec, sink),
                };
                match read {
                    Err(Error::SnapshotExpired) if tries < READER_EXPIRED_RETRIES => tries += 1,
                    other => return other,
                }
            }
        }
        if let Some((view, seqno)) = inner.latest_view()? {
            let families = families_in_order(&view, table, families)?;
            let now = inner.shared.vfs.now_micros();
            if let Some(rc) = &inner.shared.row_cache
                && crate::row_cache::serves_spec(spec)
            {
                return crate::row_cache::read_row_cached(
                    rc,
                    &view,
                    seqno,
                    table,
                    row,
                    &families,
                    spec,
                    now,
                    false,
                    sink,
                    || true,
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
        snapshot.check_process()?;
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
    /// at a snapshot taken now, as a clean database that opens without WAL replay. Visible is
    /// not durable (D227): the copy can hold commits whose sync had not finished, which a
    /// power loss then removes from this database.
    ///
    /// A reader process can back up too (ICR 0049). Its snapshot's pin in shared memory pins
    /// the whole view, memtables included, so the writer frees none of that view's memtables
    /// until the copy is done (the writer itself releases them after copying them, #262): a
    /// long backup from a reader of a write-heavy database can fill the writer's memtable
    /// arenas and make its commits wait. A writer restart while the copy runs fails it with
    /// `SnapshotExpired`, removing the partial copy, even one during the copy's own manifest
    /// commit, after every read (the data was complete; the check is conservative).
    pub fn backup(&self, dest: &Path) -> Result<()> {
        self.backup_of(dest, false)
    }

    /// As [`Engine::backup`], at a durable snapshot ([`Engine::durable_snapshot`], D227, ICR
    /// 0037): the copy holds only commits durable here at their level (a `GroupSync` or
    /// `Sync` one survives a power loss here; a `Buffered` or `None` one may not).
    pub fn backup_durable(&self, dest: &Path) -> Result<()> {
        self.backup_of(dest, true)
    }

    fn backup_of(&self, dest: &Path, durable: bool) -> Result<()> {
        let inner = &self.inner;
        let _guard = inner.enter_maintenance()?;
        let snapshot = inner.snapshot_of(durable)?;
        crate::maintenance::backup(&inner.shared, snapshot, dest)
    }

    /// Truncates the free tail, relocates tail extents the manifest names and truncates the
    /// file again (decision D60). Returns the bytes the file shrank by. An extent with no
    /// free extent of its size below it stays; `NoSpace` means the disk filled while the
    /// manifest was being moved.
    pub fn shrink(&self) -> Result<u64> {
        let inner = &self.inner;
        if inner.read_only {
            return Err(Error::ReadOnly);
        }
        let _guard = inner.enter_maintenance()?;
        crate::maintenance::shrink(&inner.shared)
    }

    /// Counter checkpoints (D291) the shards kept, and those they dropped, since open. For
    /// tests and benchmarks.
    #[doc(hidden)]
    pub fn counter_checkpoints(&self) -> (u64, u64) {
        self.inner.shared.metrics.iter().fold((0, 0), |(k, d), m| {
            (
                k + m.checkpoints.load(Ordering::Relaxed),
                d + m.checkpoints_dropped.load(Ordering::Relaxed),
            )
        })
    }

    /// What the row cache did since open (D201); all zero when it is off. In a reader process,
    /// its cache since the last attach to a writer generation (D281). A method rather than [`Metrics`] fields, which would break struct literals.
    pub fn row_cache_stats(&self) -> RowCacheStats {
        if self.inner.forked() {
            return RowCacheStats::default();
        }
        if let Some(r) = &self.inner.reader {
            // A reader process: its current generation's cache (D281), from 0 at each
            // re-attach.
            let a = r.shm.lock().unwrap_or_else(PoisonError::into_inner);
            return a
                .row_cache
                .as_ref()
                .map(|rc| rc.stats())
                .unwrap_or_default();
        }
        self.inner
            .shared
            .row_cache
            .as_ref()
            .map(|rc| rc.stats())
            .unwrap_or_default()
    }

    /// Cached family rows a commit moved forward since open (D290's write-through, ICR
    /// 0082): 0 with the row cache off and in a reader process. A method of its own: a
    /// [`RowCacheStats`] field would break struct literals.
    pub fn row_cache_write_throughs(&self) -> u64 {
        if self.inner.forked() {
            return 0;
        }
        self.inner
            .shared
            .row_cache
            .as_ref()
            .map_or(0, |rc| rc.patches())
    }

    /// The shard that owns `row` of `table` in the current tablet map, or `None` if the
    /// table does not exist (ICR 0022). A routing hint for application-owned mode: it may
    /// change after a split, merge or move, and a commit from any thread is correct whatever
    /// shard owns its rows.
    pub fn shard_of(&self, table: TableId, row: &[u8]) -> Option<usize> {
        if self.inner.forked() {
            return None;
        }
        let view = self.inner.shared.view.load();
        view.tablets()
            .route(table, row)
            .map(|(_, shard)| usize::from(shard.0))
    }

    /// Splits the tablet of `table` holding `keys[0]` at `keys` (sorted, all inside that
    /// tablet), handing child `i` to shard `owners[i]`, and waits until the change is done.
    /// A bench hook (D213): a gate workload's table spread over every shard before it is
    /// measured, instead of waiting for the balancer to split and move it. Fails as a
    /// tablet change does (`InvalidArgument` for keys or owners that do not fit,
    /// `Unsupported` past a shard's slot budget).
    #[doc(hidden)]
    pub fn split_table(&self, table: TableId, keys: Vec<Vec<u8>>, owners: &[u16]) -> Result<()> {
        self.inner.shared.refuse_blocking_on_driver("split_table")?;
        let first = keys
            .first()
            .ok_or_else(|| Error::InvalidArgument("no split keys".to_owned()))?;
        let (tablet, shard) = self.inner.tablet_at(table, first)?;
        let owners = owners.iter().map(|&s| ShardId(s)).collect();
        self.inner
            .tablet_op(
                shard,
                crate::shard::TabletOpKind::Split {
                    tablet,
                    keys,
                    owners,
                },
            )?
            .wait()
    }

    /// Commit latency histograms per durability level (indexed by `Durability as usize`),
    /// summed over shards since open: half-power-of-two buckets of the time from a commit's
    /// submission to its resolution on its shard, bucket `b` starting at
    /// [`commit_latency_floor(b)`](Self::commit_latency_floor). A bench hook (D213): two
    /// readings, subtracted, give a phase's engine-side commit latency, which
    /// [`Metrics::commit_latency_nanos`] (since open) cannot.
    #[doc(hidden)]
    pub fn commit_latency_buckets(&self) -> [[u64; 64]; 4] {
        if self.inner.forked() {
            return [[0; 64]; 4];
        }
        let mut out = [[0u64; 64]; 4];
        for s in &self.inner.shared.metrics {
            for (d, hist) in out.iter_mut().enumerate() {
                for (b, h) in hist.iter_mut().enumerate() {
                    *h += s.latency[d][b].load(Ordering::Relaxed);
                }
            }
        }
        out
    }

    /// The lower bound in nanoseconds of [`commit_latency_buckets`](Self::commit_latency_buckets)'
    /// bucket `b`.
    #[doc(hidden)]
    pub fn commit_latency_floor(b: usize) -> u64 {
        bucket_floor(b)
    }

    /// Tablet changes refused for lack of room, summed over shards since open, as
    /// `(slot budget, view size)`: splits and moves a shard's memtable slots could not take
    /// (a size split the balancer found due but skipped counts once per balancer pass), and
    /// splits refused because the tablet map would not fit the shared-memory view buffer
    /// (D28). A rising first count means tablets that should split cannot: raise
    /// `memtable_budget` (#122). A method rather than a [`Metrics`] field, which would
    /// break struct literals.
    pub fn tablet_refusals(&self) -> (u64, u64) {
        if self.inner.forked() {
            return (0, 0);
        }
        self.inner.shared.metrics.iter().fold((0, 0), |(s, v), m| {
            (
                s + m.refused_slots.load(Ordering::Relaxed),
                v + m.refused_view.load(Ordering::Relaxed),
            )
        })
    }

    /// The active shard count shard 0's balancer published at its last pass and the count it
    /// is heading for: the same unless a growth is accruing its passes, then the count that
    /// growth reaches (D217's `k`, D242's amendment of 2026-10-19), or the store reads light
    /// enough for its first pack from all its shards, then the count that pack reaches
    /// (#983). `None` before its first decision (packing off, one shard, or no pass yet). A
    /// bench hook: the warm-up does not end while the two differ, so a measured phase never
    /// starts inside a regrow after a packed load or before the store's first pack.
    #[doc(hidden)]
    pub fn pack_target(&self) -> Option<(usize, usize)> {
        if self.inner.forked() {
            return None;
        }
        let target = self.inner.shared.pack_target.load(Ordering::Acquire);
        (target > 0).then(|| (self.inner.shared.pack_k.load(Ordering::Acquire), target))
    }

    /// The driver threads the shards run on now and whether every shard is on its driver
    /// (D289 PR 2), `None` without engine-owned threads. A bench hook: the warm-up does not end
    /// while the controller is still resizing or moving shards.
    #[doc(hidden)]
    pub fn driver_target(&self) -> Option<(usize, bool)> {
        let rt = self
            .inner
            .runtime
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        rt.as_ref()
            .map(|rt| (rt.driver_target(), rt.drivers_settled()))
    }

    /// Fixes the driver target at `k`, or with `None` gives it back to the controller (D289
    /// PR 2). For tests that move shards between drivers under load.
    #[doc(hidden)]
    pub fn force_driver_target(&self, k: Option<usize>) {
        let rt = self
            .inner
            .runtime
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(rt) = rt.as_ref() {
            rt.force_driver_target(k);
        }
    }

    /// The load controller's windows since the last call, one JSON object each (D289,
    /// #1015); empty in application-owned mode. The first call starts the log, and without
    /// the controller an observe-only one that never resizes. A bench hook.
    #[doc(hidden)]
    pub fn driver_trace(&self) -> Vec<String> {
        let rt = self
            .inner
            .runtime
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        rt.as_ref().map(|rt| rt.driver_trace()).unwrap_or_default()
    }

    /// Per-shard commits, tablets and tablet changes, indexed by shard (a bench hook,
    /// issue #51).
    #[doc(hidden)]
    pub fn shard_stats(&self) -> Vec<ShardStats> {
        if self.inner.forked() {
            return Vec::new();
        }
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
                busy_nanos: 0,
                sync_busy_nanos: m.sync_busy_nanos.load(Ordering::Relaxed),
                pool_busy_nanos: 0,
                spin_time_nanos: 0,
                commit_inflight_nanos: m.commit_inflight_nanos.load(Ordering::Relaxed),
            })
            .collect();
        if let Some(subs) = shared.submitters.get() {
            for (s, sub) in out.iter_mut().zip(subs.iter()) {
                s.busy_nanos = sub.busy_nanos();
                s.pool_busy_nanos = sub.pool_busy_nanos();
                s.spin_time_nanos = sub.spin_nanos();
            }
        }
        for t in shared.view.load().tablets().iter() {
            if let Some(s) = out.get_mut(usize::from(t.shard.0)) {
                s.tablets += 1;
            }
        }
        out
    }

    /// Whether this instance has failed (ICR 0040): a shard died (its thread panicked, or
    /// its application-owned driver was dropped before the close finished) or poisoned
    /// itself (a WAL write or sync failed, or its state could not be updated), or a manifest
    /// commit to the main file failed. A failed instance refuses some or all commits until
    /// it is closed and reopened; [`Engine::failure`] says which shard and why. Cheap: one
    /// atomic load.
    pub fn is_failed(&self) -> bool {
        if self.inner.forked() {
            return false;
        }
        self.inner.shared.failure.get().is_some()
    }

    /// The instance's first failure, if any (see [`Engine::is_failed`]). Later failures do
    /// not replace it.
    pub fn failure(&self) -> Option<crate::Failure> {
        if self.inner.forked() {
            return None;
        }
        self.inner.shared.failure.get()
    }

    /// Current metrics.
    pub fn metrics(&self) -> Metrics {
        if self.inner.forked() {
            return Metrics::default();
        }
        let shared = &self.inner.shared;
        let mut m = Metrics::default();
        if let Some(rt) = self
            .inner
            .runtime
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
        {
            let p = rt.pinning();
            m.pinned_threads = (p.pinned as u64, p.threads as u64);
            m.unpinned = match (p.unpinned, p.failed) {
                (Some(why), _) => Some(why.as_str()),
                (None, 0) => None,
                (None, _) => Some("the OS refused some pins"),
            };
        }
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
            m.compactions_reclaimed += s.compactions_reclaimed.load(Ordering::Relaxed);
            m.room_stalls.0 += s.room_stalls.load(Ordering::Relaxed);
            m.room_stalls.1 += s.room_stall_nanos.load(Ordering::Relaxed);
            m.pacing_stalls.0 += s.pacing_stalls.load(Ordering::Relaxed);
            m.pacing_stalls.1 += s.pacing_stall_nanos.load(Ordering::Relaxed);
            m.unpin.0 += s.unpin_passes.load(Ordering::Relaxed);
            m.unpin.1 += s.unpin_flushes.load(Ordering::Relaxed);
            m.wal_inline_syncs += s.wal.get().map_or(0, |c| c.inline_rollover_syncs());
            m.wal_rollover_blocks += s.wal.get().map_or(0, |c| c.rollover_blocks());
            let x = &mut m.cross_shard;
            x.rounds += s.cross.rounds.load(Ordering::Relaxed);
            x.refused_rounds += s.cross.refused_rounds.load(Ordering::Relaxed);
            let refused = |r: CrossRefusal| s.cross.refused[r as usize].load(Ordering::Relaxed);
            x.refused_moved += refused(CrossRefusal::Moved);
            x.refused_room += refused(CrossRefusal::Room);
            x.refused_pending_overlap += refused(CrossRefusal::PendingOverlap);
            x.failed_fast += s.cross.failed_fast.load(Ordering::Relaxed);
            x.share_waits += s.cross.share_waits.load(Ordering::Relaxed);
            x.share_wait_nanos += s.cross.share_wait_nanos.load(Ordering::Relaxed);
            let most = |i: usize| s.cross.max_attempts[i].load(Ordering::Relaxed);
            x.max_attempts_blind = x.max_attempts_blind.max(most(0));
            x.max_attempts_reading = x.max_attempts_reading.max(most(1));
        }
        let pager = shared.pager.stats();
        m.stalls = (
            m.room_stalls.0 + m.pacing_stalls.0,
            m.room_stalls.1 + m.pacing_stalls.1,
        );
        m.file_growths = (pager.growths, pager.growth_nanos);
        m.file_growths_ahead = (pager.growths_ahead, pager.ahead_nanos);
        m.async_sync_reads = shared.async_sync_reads.load(Ordering::Relaxed);
        m.default_ts_lead_micros = shared.clock.lead(shared.vfs.now_micros());
        m.block_cache = shared.cache.hits_and_misses();
        m.block_cache_pinned = shared.cache.pinned() as u64;
        m.block_cache_capacity = shared.cache.capacity() as u64;
        m.block_cache_pinned_warnings = shared.cache.pinned_warnings();
        m.commit_parks = shared.commit_parks.load(Ordering::Relaxed);
        if self.inner.reader.is_none() {
            let (active, total) = shared.shm.reader_slot_usage();
            m.reader_slots = (u64::from(active), u64::from(total));
            m.reader_slots_reclaimed = shared.reader_slots_reclaimed.load(Ordering::Relaxed);
            if let Some((seqno, view)) = shared.shm.oldest_reader_pin() {
                let views = if view == 0 {
                    0
                } else {
                    shared.shm.view_version().saturating_sub(view)
                };
                let seqnos = if seqno == 0 {
                    0
                } else {
                    shared.shm.visible_seqno().saturating_sub(seqno)
                };
                m.reader_pin_lag = (views, seqnos);
            }
        }
        m.shard_commit_inflight_nanos = shared
            .metrics
            .iter()
            .map(|m| m.commit_inflight_nanos.load(Ordering::Relaxed))
            .sum();
        if let Some(subs) = shared.submitters.get() {
            for sub in subs.iter() {
                let (parks, wakes) = sub.idle_counts();
                m.shard_idle.0 += parks;
                m.shard_idle.1 += wakes;
                m.shard_busy_nanos += sub.busy_nanos();
                m.shard_pool_busy_nanos += sub.pool_busy_nanos();
                m.shard_spin_time_nanos += sub.spin_nanos();
                let (fallbacks, room) = sub.background_take_backs();
                m.background_take_backs.0 += fallbacks;
                m.background_take_backs.1 += room;
            }
        }
        let w = &shared.visibility;
        m.visibility_waits = (
            w.waits.load(Ordering::Relaxed),
            w.scanned.load(Ordering::Relaxed),
        );
        m.visibility_wakes = (
            w.passes.load(Ordering::Relaxed),
            w.woken.load(Ordering::Relaxed),
        );
        m.visibility_contended = shared.waiters.contended();
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
    ///
    /// The close returns, and releases the writer lock, only once every WAL write the
    /// instance submitted to the I/O backend has landed (#824), so no opener can reuse a
    /// segment slot under a write that lands later. On a device that never answers it
    /// blocks, as a blocking sync would; so does dropping the last `Engine` handle, on
    /// whatever thread does it, and a second `close` made meanwhile. In application-owned
    /// mode that wait runs inside the final close, on the application thread driving the
    /// last shard, before [`EngineShard::closed`] reports.
    pub fn close(&self) -> Result<()> {
        // A forked child's copy does nothing (#901, #898): no flush, WAL, root commit,
        // cleanup or lock release.
        if pigeonhole_io::sys::process_id() != self.inner.opened_in {
            return Ok(());
        }
        self.inner.close(true)
    }

    /// Closes as [`Engine::close`] does, keeping the writer lock and presence for an
    /// [`Engine::open_with`] of the same path (D265): the close's result, and the handover,
    /// or `None` when this call did not start the close (a `close`, or another
    /// `close_for_reopen`, was already under way: this call waits for it, and that close
    /// releases the locks, or hands them over if it is a `close_for_reopen` itself) or when
    /// the final close had already released them (every shard
    /// was dead and the close ran before this call); the caller then falls back to
    /// [`Engine::open`]. Dropping the handover releases the locks, but runs no last-one-out
    /// cleanup: the WAL files and the shm region stay, as after a crashed writer, and the
    /// next open replays and rebuilds. Engine-owned writers only: an application-owned
    /// engine's close cannot wait, and a reader holds no writer lock. For `pigeonhole`'s
    /// automatic recovery (#620); not a stable API.
    #[doc(hidden)]
    pub fn close_for_reopen(&self) -> (Result<()>, Option<Handover>) {
        if let Err(e) = self.inner.check_process() {
            return (Err(e), None);
        }
        if self.inner.role != Role::Writer || self.inner.application_owned {
            return (
                Err(Error::InvalidArgument(
                    "close_for_reopen needs an engine-owned writer".to_owned(),
                )),
                None,
            );
        }
        let (result, started_here) = self.inner.close_with(true, true);
        // The instance is closed either way: release what it holds (the view, the caches;
        // the arenas once the reopen supersedes the region and no view lives).
        self.inner.shared.release_memory();
        if !started_here {
            return (result, None);
        }
        let locks = self
            .inner
            .shared
            .locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        (
            result,
            locks.map(|locks| Handover {
                locks: Some(Box::new(locks)),
                path: self.inner.path.clone(),
            }),
        )
    }

    /// Releases what a closed writer still holds that is large, so a handle that keeps it
    /// alive keeps only a light shell (D265): the last view (its frozen memtables, open SSTs
    /// and the blocks they pin), the block and row caches, and the memtable arenas' pages.
    /// [`Engine::close_for_reopen`] runs it; call it again once the reopen ([`Engine::open_with`])
    /// succeeded, since the arenas are freed only when no other process can take the region
    /// for current (the reopen's new generation supersedes it) and no in-process view lives
    /// (a snapshot from before the close holds one: then the release runs when the last
    /// goes). Linux frees the arena pages; macOS and Windows cannot (they stay until the
    /// last handle and snapshot drop). A no-op on an open instance, on a reader and on an
    /// application-owned engine (the application drives its close); a plain [`Engine::close`]
    /// releases nothing, unless a `close_for_reopen` raced it (that call releases after the
    /// close it waited for). For `pigeonhole`'s
    /// automatic recovery (#620); not a stable API.
    #[doc(hidden)]
    pub fn release_memory(&self) {
        if self.inner.forked() {
            return;
        }
        if self.inner.role == Role::Writer && !self.inner.application_owned {
            self.inner.shared.release_memory();
        }
    }

    /// Waits until this instance records a failure ([`Engine::is_failed`]; `true`, at once if
    /// it already has), or until `deadline` passes or the instance's close finishes
    /// (`false`). Blocks on a condition variable: no polling. For `pigeonhole`'s automatic
    /// recovery (#620, D265); not a stable API.
    #[doc(hidden)]
    pub fn wait_failed(&self, deadline: std::time::Instant) -> bool {
        if self.inner.forked() {
            return false;
        }
        let shared = &self.inner.shared;
        let failed = shared
            .failure
            .wait(deadline, || shared.closed.load(Ordering::Acquire));
        #[cfg(feature = "test-hooks")]
        if failed {
            shared.hooks.sites.run(hooks::HookSite::FailureWake);
        }
        failed
    }
}

/// What [`Engine::close_for_reopen`] keeps for [`Engine::open_with`]: the writer lock, the
/// presence and the handle they live on, and the path they belong to (D265). Dropping it
/// releases the locks but runs no last-one-out cleanup: the WAL files and the shm region
/// stay, as after a crashed writer.
#[doc(hidden)]
#[derive(Debug)]
pub struct Handover {
    /// `None` only in the handover `open_with` returns when it lost the locks (never
    /// expected: they move into the engine as the open's last step). Boxed: it rides in
    /// `open_with`'s error, which clippy keeps small.
    locks: Option<Box<Locks>>,
    path: PathBuf,
}

/// Closes as [`Engine::close`] does (an application-owned engine's drop cannot wait). It
/// can block on a device that never answers, as `close` does (#824).
impl Drop for Engine {
    fn drop(&mut self) {
        // In a forked child nothing of the engine may be torn down: its threads run only in
        // the parent (joining them would wait for ever) and its descriptors and arenas are
        // the parent's. Leak the instance (#901).
        if pigeonhole_io::sys::process_id() != self.inner.opened_in {
            std::mem::forget(Arc::clone(&self.inner));
            return;
        }
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
    /// A read-only open (ICR 0059): running short fails with `ReadOnlyOpen` instead.
    refuse: bool,
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
            refuse: false,
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
    if spill.refuse {
        return Err(Error::ReadOnlyOpen(
            "the WAL to replay does not fit the memtable budget, and replay would have to \
             flush it to SSTs; raise memtable_budget to open read-only"
                .to_owned(),
        ));
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
    for ((_, family), list) in catalog.ssts.iter() {
        for (_, meta) in list.iter() {
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
        .map(|(id, b)| (*id, b.extents.to_vec()))
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
    let before = catalog.counters;
    let nothing_recovered = flushed.is_empty() && spill.edits.is_empty() && recoveries.is_empty();
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
    catalog.counters.seqno_ceiling = shared.clock.next_seqno();
    // The streams are checkpointed past every replayed record, so their timestamps now live
    // only in SSTs: the floor must carry them, or a crash before the next manifest commit
    // reopens with a lower `H` and nothing to replay, and a clock behind them stamps below
    // existing versions (#653, D11). Replay raised each shard's last stamp (perf287, #899).
    catalog.counters.ts_floor = catalog
        .counters
        .ts_floor
        .max(states.iter().map(ShardState::last_stamp).max().unwrap_or(0));
    if nothing_recovered && catalog.counters == before {
        // A clean reopen (#158, D203): no stream to replay, nothing spilled, and the
        // counters are what the manifest holds. The commit would publish nothing; skipping
        // it saves the open two flushes. The clean flag stays set until the next commit
        // (informational only, D57).
        return Ok(());
    }
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
    /// The process that submitted it: polled or waited on in a forked child, it fails
    /// `Unsupported` at once (#901, perf287's M2).
    owner: u32,
}

impl PendingMaintenance {
    /// Blocks until every shard replied (to every round of a full compaction).
    pub(crate) fn wait(mut self) -> Result<()> {
        if self.owner != pigeonhole_io::sys::process_id() {
            return Err(forked());
        }
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
        if self.owner != pigeonhole_io::sys::process_id() {
            return std::task::Poll::Ready(Err(forked()));
        }
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
        // A reader, or a writer opened read-only (ICR 0059): nothing may write.
        if self.read_only {
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
            failed: None,
            owner: pigeonhole_io::sys::process_id(),
        })
    }

    pub(crate) fn admit(&self, batch: &WriteBatch) -> crate::Admit {
        self.admit_routed(None, batch)
    }

    /// The async fast path (D210, D236): a charge on its shard's count when it fits and no
    /// one waits there, else the admission to wait on.
    #[inline]
    pub(crate) fn fast_admit(
        &self,
        row: Option<(TableId, &[u8])>,
        batch: &WriteBatch,
    ) -> FastAdmit {
        let Some(a) = &self.admission else {
            return FastAdmit::Charged(crate::admission::Charge::none());
        };
        let shard = self.admission_shard(a, row, batch);
        let bytes = crate::admission::batch_bytes(batch);
        match a.try_fast(shard, bytes) {
            Some(charge) => FastAdmit::Charged(charge),
            None => FastAdmit::Wait(crate::Admit::new(Some(a), shard, bytes)),
        }
    }

    /// Admits `batch` on the shard of `row` when given, else of the batch's first row.
    pub(crate) fn admit_routed(
        &self,
        row: Option<(TableId, &[u8])>,
        batch: &WriteBatch,
    ) -> crate::Admit {
        let shard = self
            .admission
            .as_ref()
            .map_or(0, |a| self.admission_shard(a, row, batch));
        crate::Admit::new(
            self.admission.as_ref(),
            shard,
            crate::admission::batch_bytes(batch),
        )
    }

    /// The shard an async commit of `batch` is charged to: that of `row` when given, else
    /// of the batch's first row (a cross-shard batch's coordinator). With one shard, no
    /// routing.
    /// The handle on the shared state for commits to `shard` (D236).
    pub(crate) fn shard_ref(&self, shard: ShardId) -> Arc<ShardRef> {
        // Routing only names shards that exist; a reader commits nothing.
        Arc::clone(&self.shard_refs[usize::from(shard.0)])
    }

    fn admission_shard(
        &self,
        a: &crate::admission::Admission,
        row: Option<(TableId, &[u8])>,
        batch: &WriteBatch,
    ) -> usize {
        if !a.sharded() {
            return 0;
        }
        let view = self.shared.view.load();
        let route = |t: TableId, r: &[u8]| view.tablets().route(t, r);
        let routed = match row {
            Some((t, r)) => route(t, r),
            None => match batch.builder.batch().iter().next().and_then(|m| m.ok()) {
                Some(m) => route(m.table, m.row),
                None => batch
                    .row_deletes
                    .first()
                    .and_then(|d| route(d.table, &d.row)),
            },
        };
        routed.map_or(0, |(_, s)| usize::from(s.0))
    }

    /// Enters application-thread maintenance: refused once the engine is closing, and the
    /// final close waits until the guard is dropped (7 F7-4).
    fn enter_maintenance(&self) -> Result<MaintenanceGuard<'_>> {
        MaintenanceGuard::enter(&self.shared, || self.check_open())
    }

    /// `check_open` for a new commit: also refused once a shard died (a panic, a dropped
    /// application-owned shard, a failed WAL sync or a failed cross-shard apply). Its writes
    /// could never become durable or visible here (the durable watermark is stuck, or the
    /// dead shard's watermark is), so it fails fast with `Closed`: nothing was written, so
    /// not committed (D227, ICR 0038).
    fn check_writable(&self) -> Result<()> {
        self.check_open()?;
        if self.shared.shard_died.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        Ok(())
    }

    /// In a forked child of the process that opened the engine (#901): the rule there is
    /// that a handle may only be dropped or closed; every other entry point fails
    /// `Unsupported` ([`Inner::check_process`]) or is inert (an accessor returns its empty or
    /// default value without taking any lock, which a parent thread may have held at the
    /// fork). ICR 0067.
    #[inline]
    pub(crate) fn forked(&self) -> bool {
        pigeonhole_io::sys::process_id() != self.opened_in
    }

    /// `Unsupported` in a process other than the one that opened the engine (a forked
    /// child, #901): before anything touches a shard, a queue, a ring or the shared arenas.
    /// One compare with the cached process id.
    #[inline]
    pub(crate) fn check_process(&self) -> Result<()> {
        if pigeonhole_io::sys::process_id() != self.opened_in {
            return Err(if self.reader.is_some() {
                Error::Unsupported(FORKED_READER)
            } else {
                forked()
            });
        }
        Ok(())
    }

    pub(crate) fn check_open(&self) -> Result<()> {
        self.check_process()?;
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
            check_merge_operator(options, &self.options.merge_operators)?;
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
        check_merge_operator(&options, &self.options.merge_operators)?;
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
        validate: Option<Arc<Validate>>,
        predicate: Option<(TableId, Vec<u8>, Predicate)>,
    ) -> Result<Submission> {
        let routed = self.route_and_send(batch, durability, validate, predicate, NoAdmission)?;
        self.submission(routed)
    }

    /// Routes `batch` and sends its request to its shard (a cross-shard commit's
    /// coordinator). With `admit` (an async commit's admission and bytes), it first charges
    /// that shard (D236), so the commit is routed once; without room it builds the request
    /// and hands it back to send later ([`send`](Self::send)). A request sent later than it was routed is still
    /// correct: a shard that handed a tablet away re-routes commits routed with an older
    /// tablet map. The request is built inside the `submit` call that sends it, never moved
    /// through a struct or a closure on the way: that cost the commit shapes 60–90
    /// instructions each.
    #[inline(always)]
    pub(crate) fn route_and_send<A: Admitter>(
        &self,
        batch: WriteBatch,
        durability: Option<Durability>,
        validate: Option<Arc<Validate>>,
        predicate: Option<(TableId, Vec<u8>, Predicate)>,
        admit: A,
    ) -> Result<Routed<A::Charge>> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        self.check_writable()?;
        let durability = durability.unwrap_or_else(|| self.shared.default_durability());
        // One tablet map for the whole routing: its version goes with a cross-shard commit,
        // which a participant moving a tablet refuses and the coordinator retries.
        let view = self.shared.view.load();
        let (builder, mut shards) = self.route(batch, &view)?;
        let (builder, large) = self.separate_large(builder)?;
        // Every shard that owns a row the transaction read validates it at PREPARE, so two
        // transactions cannot each read what the other writes (write skew).
        if let Some(reads) = validate.as_ref().map(|v| &v.reads) {
            for r in reads {
                if let Some((_, shard)) = view.tablets().route(r.table, &r.row)
                    && !shards.contains(&shard)
                {
                    shards.push(shard);
                }
            }
        }
        let submitted_at = self.shared.vfs.monotonic_nanos();
        let (tx, waiter) = crate::write::commit_completion();
        if let Some(large) = large {
            large.settle_on(&tx, crate::large::commit_succeeded);
        }
        // A durable commit waits for a sync: polling for it would only burn the CPU.
        let spins = matches!(durability, Durability::None | Durability::Buffered);
        if shards.len() <= 1 {
            let target = shards.first().copied().unwrap_or(ShardId(0));
            macro_rules! request {
                () => {
                    ShardMsg::Commit(CommitReq {
                        bytes: builder,
                        durability,
                        reply: Reply::Commit(tx),
                        submitted_at,
                        validate,
                        predicate: predicate.map(Box::new),
                        retry: None,
                        map_version: view.tablets().version(),
                    })
                };
            }
            let Some(charge) = admit.admit(target) else {
                return Ok(Routed::Held(Prepared {
                    target,
                    msg: request!(),
                    waiter,
                    spins,
                }));
            };
            // A failed submit drops the charge, which returns it.
            self.shared.submitter(target).submit(request!())?;
            return Ok(Routed::Sent {
                target,
                waiter,
                spins,
                charge,
            });
        }
        if predicate.is_some() {
            return Err(Error::InvalidArgument(
                "check_and_mutate touches one row".to_owned(),
            ));
        }
        let parts = split_by_shard(&view, &builder, &shards)?;
        let target = shards[0];
        let msg = ShardMsg::Coordinate(Box::new(CoordinateReq {
            parts,
            durability,
            reply: tx,
            submitted_at,
            validate,
            map_version: view.tablets.version(),
            epoch: 0,
            first_seqno: 0,
            prev_shards: Vec::new(),
            attempts: 0,
            wait: None,
        }));
        let Some(charge) = admit.admit(target) else {
            return Ok(Routed::Held(Prepared {
                target,
                msg,
                waiter,
                spins,
            }));
        };
        // A failed submit drops the charge, which returns it.
        self.shared.submitter(target).submit(msg)?;
        Ok(Routed::Sent {
            target,
            waiter,
            spins,
            charge,
        })
    }

    /// Submits and waits on the engine's borrowed state (D236): a blocking commit takes no
    /// reference on the engine.
    fn commit_blocking(
        &self,
        batch: WriteBatch,
        durability: Option<Durability>,
    ) -> Result<CommitInfo> {
        match self.route_and_send(batch, durability, None, None, NoAdmission)? {
            Routed::Sent {
                mut waiter, spins, ..
            } => crate::write::wait_blocking(&self.shared, &mut waiter, spins),
            Routed::Held(p) => self.send(p)?.wait(),
        }
    }

    /// The pending commit for a request [`route_and_send`](Self::route_and_send) sent.
    #[inline(always)]
    pub(crate) fn submission(&self, routed: Routed<()>) -> Result<Submission> {
        match routed {
            Routed::Sent {
                target,
                waiter,
                spins,
                ..
            } => Ok(Submission {
                waiter,
                shared: self.shard_ref(target),
                spins,
            }),
            Routed::Held(p) => self.send(p),
        }
    }

    /// Sends a request [`route_and_send`](Self::route_and_send) held back (an async commit
    /// that waited for room).
    pub(crate) fn send(&self, prepared: Prepared) -> Result<Submission> {
        let Prepared {
            target,
            msg,
            waiter,
            spins,
        } = prepared;
        self.shared.submitter(target).submit(msg)?;
        Ok(Submission {
            waiter,
            shared: self.shard_ref(target),
            spins,
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
        self.check_writable()?;
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
                retry: None,
                map_version: view.tablets().version(),
            }))?;
        Ok(PendingCheck {
            waiter: rx,
            shared: self.shard_ref(shard),
            resolved: None,
            deferred: None,
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

    pub(crate) fn latest_view(&self) -> Result<Option<(arc_swap::Guard<Arc<View>>, Seqno)>> {
        self.check_process()?;
        for _ in 0..GET_LATEST_TRIES {
            let seqno = self.shared.shm.visible_seqno();
            #[cfg(feature = "test-hooks")]
            self.shared.hooks.sites.run(hooks::HookSite::LatestViewLoad);
            let view = self.shared.view.load();
            if self.shared.shm.visible_seqno() == seqno {
                if view.released {
                    return Err(Error::Closed);
                }
                return Ok(Some((view, seqno)));
            }
        }
        Ok(None)
    }

    pub(crate) fn snapshot(&self) -> Result<Snapshot> {
        self.snapshot_of(false)
    }

    /// A snapshot at the visible seqno, or (`durable`) at the durable one (D227, ICR 0037).
    pub(crate) fn snapshot_of(&self, durable: bool) -> Result<Snapshot> {
        if self.reader.is_some() {
            return self.reader_snapshot(durable);
        }
        self.check_process()?;
        // The seqno is pinned before the view loads (and read under the pin's lock), so a
        // flush GC that publishes in between keeps what it reads; still seqno before view: a
        // seqno is only visible once the view holding its memtables is published.
        let pin = SeqnoPin::pin_visible(&self.shared.live_seqnos, || {
            if durable {
                self.shared.shm.durable_seqno()
            } else {
                self.shared.shm.visible_seqno()
            }
        });
        let seqno = pin.seqno();
        #[cfg(feature = "test-hooks")]
        self.shared
            .hooks
            .sites
            .run(hooks::HookSite::SnapshotViewLoad);
        let view = self.shared.view.load_full();
        if view.released {
            return Err(Error::Closed);
        }
        Ok(Snapshot {
            seqno,
            view,
            _live: None,
            _pin: Some(Arc::new(pin)),
            taken_in: self.opened_in,
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
        snapshot.check_process()?;
        // D290: a snapshot's (and so a transaction's) gets go through the writer's row cache
        // at the snapshot's seqno, as latest gets do at theirs.
        if let Some(rc) = &self.shared.row_cache
            && let Some(got) = crate::row_cache::get_through(
                rc,
                &snapshot.view,
                snapshot.seqno,
                self.shared.vfs.now_micros(),
                table,
                family,
                row,
                qualifier,
            )
        {
            return snapshot.checked(got);
        }
        snapshot.checked(get_in::<false>(
            &snapshot.view,
            snapshot.seqno,
            || self.shared.vfs.now_micros(),
            table,
            family,
            row,
            qualifier,
            || Arc::clone(&snapshot.view),
        ))
    }

    fn flush_pending(&self) -> Result<PendingMaintenance> {
        // A reader, or a writer opened read-only (ICR 0059): nothing may write.
        if self.read_only {
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
            owner: pigeonhole_io::sys::process_id(),
        })
    }

    fn compact_pending(&self, table: Option<TableId>) -> Result<PendingMaintenance> {
        // A reader, or a writer opened read-only (ICR 0059): nothing may write.
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        self.check_open()?;
        let rounds = CompactRounds::new(&self.shared, table);
        let waiters = compact_round(&self.shared, table)?;
        Ok(PendingMaintenance {
            waiters,
            rounds,
            failed: None,
            owner: pigeonhole_io::sys::process_id(),
        })
    }

    // ---- close ----

    fn close(&self, wait: bool) -> Result<()> {
        self.close_with(wait, false).0
    }

    /// `close`, and whether this call is the one that started the close. With `keep_locks`
    /// (a `close_for_reopen`, D265) the final close leaves the locks in place, but only when
    /// this call started it: a call that finds the close under way sets nothing (the close
    /// that started first releases the locks when it ends, or hands them over if it is a
    /// `close_for_reopen`) and, since its caller wants the outcome, waits for that close to
    /// finish and returns its result.
    fn close_with(&self, wait: bool, keep_locks: bool) -> (Result<()>, bool) {
        if self.role != Role::Writer {
            return (self.close_reader(), false);
        }
        if self.closing.swap(true, Ordering::AcqRel) {
            // The close under way ends with the streams' submitted I/O landed and the locks
            // released (or handed over): wait for that whole close, not just for `closed`,
            // so an open this caller makes next cannot race a write of this instance (#824).
            // Not on a thread that may have to drive it (application-owned, not driven
            // elsewhere): the winner does not wait there either.
            let shared = &self.shared;
            if wait && (!self.application_owned || shared.drivers.driven_elsewhere()) {
                shared
                    .failure
                    .wait_for(|| shared.close_done.load(Ordering::Acquire));
            }
            if keep_locks {
                let outcome = shared
                    .close
                    .outcome(|o| o.as_ref().copied().map_err(Error::duplicate));
                return (outcome.unwrap_or(Ok(())), false);
            }
            return (Ok(()), false);
        }
        if keep_locks {
            self.shared.keep_locks.store(true, Ordering::Release);
        }
        if let Some(a) = &self.admission {
            a.close();
        }
        // Application-owned background workers (D284): everything they hold or have queued
        // goes back to the shards, which finish the flushes as part of this close; it waits
        // only for a slice running right now, never for the application to drive a worker.
        // Nothing is held here that a slice could take.
        if let Some(workers) = self.workers.get() {
            workers.close();
        }
        let (tx, rx) = completion();
        // Every shard may have reported already (dropped, or its thread panicked) and the
        // final close run before this: then it is answered at once (`close_gate`).
        if let Some((tx, outcome)) = self
            .shared
            .close
            .register(tx, |o| o.as_ref().copied().map_err(Error::duplicate))
        {
            tx.notify(outcome);
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
                let shutdown = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    rt.shutdown()
                })) {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(e)) => Err(e.into()),
                    Err(panic) => Err(shard_panicked(&*panic)),
                };
                // Every thread is joined, so nothing submits more: wait for what the streams
                // still have in flight on the I/O backend's own threads (a poisoned shard's
                // final sync waited for nothing; a spare preparation's zero-fill writes and a
                // held header's chain are never waited for), whatever the shutdown's outcome
                // and the shards' state, and only then release the writer lock (#824): any
                // opener, in this process (the handover, D265, or the recovery guide's close
                // and reopen) or another, could otherwise reuse a WAL slot under a write
                // that lands later.
                self.shared.release_after_io();
                if let Err(e) = shutdown {
                    return (Err(e), true);
                }
            } else {
                self.shared.release_after_io();
            }
        }
        (result, true)
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
            shm.pins.close();
        }
        let presence = r
            .presence
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        // In a forked child the presence handle is the parent's (`Presence` releases nothing
        // there, and `try_become_last` is false): no last-one-out cleanup.
        if let Some(presence) = presence
            && pigeonhole_io::sys::process_id() == self.opened_in
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
        // A `wait_failed` on a reader handle ends at its close, as on a writer (D265).
        self.shared.failure.notify();
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
            self.shared
                .hooks
                .sites
                .run(hooks::HookSite::ReaderManifestLoad);
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
        self.shared
            .hooks
            .sites
            .run(hooks::HookSite::ReaderViewRecord);
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

    fn reader_snapshot(&self, durable: bool) -> Result<Snapshot> {
        let r = self.reader.as_ref().expect("reader");
        // A forked child's copy of the reader takes no snapshot (and so claims no slot).
        if pigeonhole_io::sys::process_id() != self.opened_in {
            return Err(Error::Unsupported(FORKED_READER));
        }
        let deadline = std::time::Instant::now() + READER_VIEW_WAIT;
        let mut waits = 0u32;
        loop {
            let mut guard = r.shm.lock().unwrap_or_else(PoisonError::into_inner);
            let a = &mut *guard;
            if a.shm.is_stale() {
                let shm = a.shm.reattach(&self.shared.vfs, &r.file)?;
                // The old slot's liveness byte is on this same file handle: give it up before
                // claiming in the new generation (`ReaderSlot::abandon`).
                a.pins.abandon();
                let slot = claim_slot(
                    &shm,
                    self.shared.vfs.current_process(),
                    &r.file,
                    self.opened_in,
                )?;
                // Snapshots of the old generation leave the abandoned slot alone: they can no
                // longer be read, so they must not hold this generation's pin back.
                *a = Attachment {
                    row_cache: reader_row_cache(&self.options, &shm),
                    shm,
                    pins: crate::snapshot::ReaderPins::new(slot),
                };
                *r.view.lock().unwrap_or_else(PoisonError::into_inner) = None;
                // Start the new generation from the new writer's root.
                r.catalog.lock().unwrap_or_else(PoisonError::into_inner).0 = NO_MANIFEST_VERSION;
            } else if !a.pins.slot().is_owned() {
                // The slot was freed under this process (#834 review). Defensive since #693
                // PR 3: the writer frees a slot only once it holds the slot's liveness lock,
                // which this process holds until it drops the slot. If it happened anyway,
                // the snapshots fail as expired and pinning there could overwrite another
                // reader's pins: abandon it and claim a fresh slot.
                a.pins.abandon();
                a.pins = crate::snapshot::ReaderPins::new(claim_slot(
                    &a.shm,
                    self.shared.vfs.current_process(),
                    &r.file,
                    self.opened_in,
                )?);
            }
            // The slot pins exactly what this reader's live snapshots need: this one's seqno
            // and view join them here, before the view is read (#693 PR 2).
            let (seqno, key) = a.pins.take(&a.shm, durable);
            let live = Arc::new(LiveSnapshot {
                pins: Arc::clone(&a.pins),
                key,
                shm: a.shm.clone(),
                slot: a.pins.slot().ownership(),
                row_cache: a.row_cache.clone(),
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
                        taken_in: self.opened_in,
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
        released: false,
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
pub(crate) fn check_local(file: &FileRef, allow_fuse: bool) -> Result<()> {
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

fn check_merge_operator(options: &FamilyOptions, registry: &MergeRegistry) -> Result<()> {
    let name = options.merge_operator.as_str();
    if options.kind == FamilyKind::Counter && name != crate::catalog::I64_ADD {
        return Err(Error::InvalidArgument(format!(
            "a counter family sums i64s with {}, not {name:?}",
            crate::catalog::I64_ADD
        )));
    }
    if name.is_empty() || registry.get(name).is_some() {
        Ok(())
    } else {
        Err(Error::UnknownMergeOperator(name.to_owned()))
    }
}

/// What [`Engine::open_application_owned_with`] hands the application (D284).
#[derive(Debug)]
#[non_exhaustive]
pub struct AppOwned {
    /// The engine.
    pub engine: Arc<Engine>,
    /// One per shard, in shard order: drive each from a thread you own.
    pub shards: Vec<EngineShard>,
    /// The background workers (`EngineOptions::background_workers` of them): drive each
    /// from a thread you own too, or flush and compaction wait for the shards to take them
    /// back.
    pub workers: Vec<BackgroundWorker>,
}

/// One application-owned background worker (D284): runs flushes and compactions the shards
/// hand off. Drive it like an [`EngineShard`]: call [`run_once`](Self::run_once) until it
/// returns `false`, then [`wait`](Self::wait) (or wait for the
/// [`set_wakeup`](Self::set_wakeup) callback or [`next_deadline`](Self::next_deadline)).
/// Each slice runs its I/O inline on the calling thread, so the thread may stop driving it at
/// any time: the engine's close and a shard waiting for memtable room take its work back.
/// After the close, [`closed`](Self::closed) is `true` and it can be dropped; dropping it
/// earlier hands what it holds back to the shards.
#[derive(Debug)]
pub struct BackgroundWorker {
    driver: PoolDriver,
    engine: Arc<Inner>,
}

impl BackgroundWorker {
    fn forked(&self) -> bool {
        pigeonhole_io::sys::process_id() != self.engine.opened_in
    }

    /// Runs the worker's tasks until `deadline_nanos` (VFS monotonic clock) or until none is
    /// runnable. Returns whether work remains. Does nothing once the engine closed, and in a
    /// forked child (#901).
    pub fn run_once(&mut self, deadline_nanos: u64) -> bool {
        if self.forked() {
            return false;
        }
        self.driver.run_once(deadline_nanos)
    }

    /// Waits until the worker has work again, the next deadline, or `timeout`. It may return
    /// early: loop on [`run_once`](Self::run_once).
    pub fn wait(&mut self, timeout: std::time::Duration) {
        if !self.forked() {
            self.driver.wait(timeout);
        }
    }

    /// A waker that ends this worker's [`wait`](Self::wait) from any thread.
    pub fn waker(&self) -> std::task::Waker {
        self.driver.waker()
    }

    /// Registers a callback for when the worker has work again (called once right away).
    pub fn set_wakeup(&mut self, wake: Box<dyn Fn() + Send + Sync>) {
        self.driver.set_wakeup(wake);
    }

    /// The earliest deadline (VFS monotonic clock) of a task sleeping on this worker.
    pub fn next_deadline(&self) -> Option<u64> {
        if self.forked() {
            return None;
        }
        self.driver.next_deadline()
    }

    /// Whether the engine closed the workers: nothing more to run, drop it.
    pub fn closed(&self) -> bool {
        self.driver.closed()
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
    /// In a forked child (#901): the shard is the parent's, and every driving method does
    /// nothing (`run_once` returns `false`, `commit_local` fails `Unsupported`).
    fn forked(&self) -> bool {
        pigeonhole_io::sys::process_id() != self.engine.opened_in
    }

    /// Shard index.
    pub fn index(&self) -> u16 {
        self.driver.as_ref().map_or(0, |d| d.shard().0)
    }

    /// Runs queued writes, the group commit, and background work until `deadline_nanos`.
    /// Returns whether work remains. Work blocked on I/O in flight does not count: the
    /// wakeup fires when it completes.
    ///
    /// A turn that resolved a commit (or another future) the application polls with this
    /// shard's [`waker`](Self::waker) returns once its queued work is done, before the
    /// background work, so the caller sees the result sooner; the background work runs on the
    /// next call, and at the latest every ninth (D237).
    ///
    /// The calling thread counts as this shard's driver from now on (until another thread
    /// runs it or it is dropped), so its blocking calls fail rather than deadlock (see
    /// [`Engine::open_application_owned`]). Until a thread first runs the shard, nobody
    /// counts as its driver: a thread holding a shard it has not run yet is not protected,
    /// and must not block on a commit (run the shard first).
    pub fn run_once(&mut self, deadline_nanos: u64) -> bool {
        // A forked child's copy runs nothing: its state machine would append to the
        // parent's log and apply into the parent's arenas (#901, perf287's M1).
        if self.forked() {
            return false;
        }
        let Some(d) = self.driver.as_mut() else {
            return false;
        };
        self.engine.shared.drivers.enter(usize::from(d.shard().0));
        d.run_once(deadline_nanos)
    }

    /// How many messages this shard has handled and background slices it has run, in all
    /// (`ShardDriver::work_count`): for tests that drive shards by hand and run them until
    /// nothing is queued and nothing remains (a pass in which no `run_once` returned `true`
    /// and no count moved). 0 in a forked child.
    #[doc(hidden)]
    pub fn work_count(&self) -> u64 {
        self.driver.as_ref().map_or(0, |d| d.work_count())
    }

    /// Before handing this shard to another thread: drives it until the calling thread has
    /// none of its I/O in flight (#492, ICR 0029). With io_uring in application-owned mode
    /// each driving thread has its own ring, and only that thread can complete what was
    /// submitted to it: a shard moved with I/O still on the old thread's ring waits on it
    /// for ever once that thread stops running turns. Call it on the old thread, then move
    /// the shard. A no-op on the pread and synchronous backends (no per-thread rings), and
    /// in engine-owned mode (shards never move). Debug builds panic when a shard turns up on
    /// another thread with I/O left on the old one unreleased.
    pub fn release(&mut self) {
        if self.forked() {
            return;
        }
        if let Some(d) = self.driver.as_mut() {
            d.release();
        }
    }

    /// The outcome of the database's close once it has finished (every shard closed and
    /// the last one recorded the clean close), or `None` before. After [`Engine::close`],
    /// keep driving the shard until this is `Some`, then drop it. Every shard reports the
    /// same outcome; an `Err` means the close was not clean or failed (the next open
    /// replays the WAL).
    pub fn closed(&self) -> Option<Result<()>> {
        if self.forked() {
            return None;
        }
        self.engine
            .shared
            .close
            .outcome(|r| r.as_ref().copied().map_err(Error::duplicate))
    }

    /// Waits until this shard has work again (#553, ICR 0035): a commit or message arrives,
    /// a [`waker`](EngineShard::waker) fires, the calling thread's own I/O completes, the
    /// next background deadline passes, or `timeout` passes. Call it after
    /// [`run_once`](EngineShard::run_once) returns `false`, on the thread that runs the
    /// shard. It may return early: loop on `run_once`. It needs no completion fd: with
    /// io_uring it waits in the thread's ring while its own I/O is in flight.
    pub fn wait(&mut self, timeout: std::time::Duration) {
        if self.forked() {
            return;
        }
        if let Some(d) = self.driver.as_mut() {
            d.wait(timeout);
        }
    }

    /// A waker that ends this shard's [`wait`](EngineShard::wait) from any thread, for
    /// futures the driving thread polls itself (a commit's future, resolved on another
    /// shard's thread).
    pub fn waker(&self) -> std::task::Waker {
        if self.forked() {
            return std::task::Waker::noop().clone();
        }
        self.driver
            .as_ref()
            .map_or_else(|| std::task::Waker::noop().clone(), |d| d.waker())
    }

    /// The earliest deadline (VFS `monotonic_nanos`) of background work sleeping on this
    /// shard (a write stall's refill, a wait for arena room's timeout, a failed compaction's
    /// backoff), or `None`. After [`run_once`](EngineShard::run_once) returns `false`, call
    /// it again when the wakeup fires or this deadline passes, whichever comes first.
    pub fn next_deadline(&self) -> Option<u64> {
        if self.forked() {
            return None;
        }
        self.driver.as_ref().and_then(ShardDriver::next_deadline)
    }

    /// Commits `batch` inline if every row belongs to this shard; otherwise submits it.
    pub fn commit_local(
        &mut self,
        batch: WriteBatch,
        durability: Option<Durability>,
    ) -> Result<PendingCommit> {
        let engine = Arc::clone(&self.engine);
        engine.check_writable()?;
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
            return engine
                .submit(wb, Some(durability), None, None)
                .map(PendingCommit::from);
        }
        let (builder, large) = engine.separate_large(builder)?;
        let submitted_at = engine.shared.vfs.monotonic_nanos();
        let (tx, waiter) = crate::write::commit_completion();
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
                    retry: None,
                    map_version: view.tablets().version(),
                },
                ctx,
            )
        });
        Ok(PendingCommit::from(Submission {
            waiter,
            shared: engine.shard_ref(me),
            spins: false,
        }))
    }

    /// Registers a callback the engine calls when work arrives for this shard.
    pub fn set_wakeup(&mut self, wake: Box<dyn Fn() + Send + Sync>) {
        if self.forked() {
            return;
        }
        if let Some(d) = self.driver.as_mut() {
            d.set_wakeup(wake);
        }
    }
}

impl Drop for EngineShard {
    fn drop(&mut self) {
        // As `Engine`'s drop: a forked child's copy shuts nothing down (#901).
        if pigeonhole_io::sys::process_id() != self.engine.opened_in {
            std::mem::forget(self.driver.take());
            return;
        }
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
