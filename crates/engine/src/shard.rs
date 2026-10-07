//! The per-shard state and the shard loop body: the group commit on the shard's WAL stream,
//! memtable application with same-commit collapse (D34), the two-phase commit protocol for
//! cross-shard batches, freezing and flushing, WAL checkpoints (D24), compaction scheduling,
//! write stalls, and the close handshake.
//!
//! One `ShardState` per shard, owned by its thread (or its `EngineShard` driver). Shards
//! share nothing mutable except the documented queues, the shared-memory watermarks and the
//! engine-wide [`Shared`] state (lock-free view publication behind a publish mutex, the
//! manifest queue, and the registries flush and compaction consult).

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::hash::Hasher;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Waker;

use arc_swap::ArcSwap;
use pigeonhole_cache::BlockCache;
use pigeonhole_compaction::{CompactionPicker, MergingCursor, PickerOptions, ResolveOptions};
use pigeonhole_format::key::{encode_key, encode_marker_key, encode_row_prefix, split_suffix};
use pigeonhole_format::manifest::{CompactionStyle, Edit};
use pigeonhole_format::scan::ScanFilter;
use pigeonhole_format::wal::{BatchBuilder, BatchRef, StreamList, WalRecord};
use pigeonhole_format::{
    Cursor, Durability, FamilyId, Kind, Lsn, ManifestVersion, Seqno, SstId, StreamId, TableId,
    TabletId, Timestamp,
};
use pigeonhole_io::{ErrorKind, VfsRef};
use pigeonhole_memtable::{ArenaRegion, Memtable, MemtableReader, Retired, ShardArena};
use pigeonhole_pager::Pager;
use pigeonhole_runtime::{
    Notifier, ShardContext, ShardHandler, ShardId, Submitter, Task, TaskPoll, TaskWaker,
};
use pigeonhole_shm::{Presence, ShmRegion, WriterLock};
use pigeonhole_wal::{CommitTicket, SpareSegments, Wal};

use crate::catalog::MergeKind;
use crate::compact::{self, CompactionRecord, CompactionWork};
use crate::flush::{FlushItem, FlushTask, FlushedItem};
use crate::manifest::{self, ManifestPump, ManifestQueue, ManifestReq, ManifestWriter};
use crate::snapshot::{LiveSeqnos, LiveViews, MemSet, ShardMems, TabletMap, View};
use crate::source::{Probe, Resolver, Source, mem_sources_from, sst_sources_point};
use crate::write::ReadKey;
use crate::{CommitInfo, Error, Predicate, Result};

/// Worst-case memtable overhead per entry: node header and a full tower, plus alignment.
const ENTRY_OVERHEAD: usize = 12 + 4 * 16 + 8;
/// Fixed bytes of an internal key beyond the escaped row and qualifier.
const KEY_FIXED: usize = 2 + 2 + 17;
/// Token-bucket capacity (groups) and refill rate at an L0 score of 1 (groups per second).
const STALL_CAPACITY: f64 = 8.0;
const STALL_RATE: f64 = 4000.0;
/// Polls in a row that see the clock unchanged before a stall timer gives up: the clock is
/// frozen (the simulator) or coarse, and the stall ends on a background event instead.
const STALL_TIMER_FROZEN_POLLS: u32 = 1024;
/// Failed flushes in a row after which a wait for arena room ends with `Busy`.
const ROOM_FLUSH_ATTEMPTS: u32 = 4;

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
    pub flush_nanos: AtomicU64,
    pub compactions: AtomicU64,
    pub compaction_nanos: AtomicU64,
}

impl Default for ShardMetrics {
    fn default() -> Self {
        Self {
            commits: Default::default(),
            latency: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            stalls: AtomicU64::new(0),
            stall_nanos: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
            flush_nanos: AtomicU64::new(0),
            compactions: AtomicU64::new(0),
            compaction_nanos: AtomicU64::new(0),
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

/// The close handshake: each shard reports when its WAL is checkpointed, synced and
/// dropped; the last one runs the final steps and resolves `done`.
#[derive(Debug, Default)]
pub(crate) struct CloseState {
    pub remaining: AtomicUsize,
    pub done: Mutex<Option<Notifier<Result<()>>>>,
    /// A shard's final WAL sync (or flush) failed: the close is not clean.
    pub failed: AtomicBool,
    /// Every shard has reported: the final close runs once it holds the manifest writer's
    /// exclusion (whoever holds it then runs it when it releases).
    pub final_pending: AtomicBool,
}

/// One cache line per shard, so shards never share a line through these counters.
#[derive(Debug, Default)]
#[repr(align(64))]
pub(crate) struct Padded(pub AtomicU64);

/// Commits waiting for the global watermark to reach their seqno (async `PendingCommit`).
#[derive(Debug, Default)]
pub(crate) struct VisibilityWaiters {
    pub count: AtomicUsize,
    pub list: Mutex<Vec<(Seqno, Waker)>>,
}

/// Engine-wide state every shard and every caller shares.
pub(crate) struct Shared {
    pub vfs: VfsRef,
    pub shm: ShmRegion,
    pub shards: usize,
    pub view: ArcSwap<View>,
    /// Serializes view publishers (shards creating memtables, manifest commits) and holds
    /// the last published version.
    pub view_lock: Mutex<u64>,
    pub manifest: Mutex<ManifestWriter>,
    pub manifest_queue: ManifestQueue,
    /// Held by whoever is committing a manifest batch.
    pub manifest_busy: AtomicBool,
    pub pager: Arc<Pager>,
    pub cache: Arc<BlockCache>,
    pub sst_ids: Arc<AtomicU64>,
    pub blob_ids: Arc<AtomicU32>,
    pub live_views: Arc<LiveViews>,
    pub live_seqnos: Arc<LiveSeqnos>,
    /// Memtables `(shard, root)` whose SSTs are in the manifest, excluded from every view
    /// until their shard retires them.
    pub flushed_roots: Mutex<HashSet<(u16, u32)>>,
    /// SSTs a running compaction or relocation reads or replaces.
    pub busy_ssts: Mutex<HashSet<SstId>>,
    /// Published view version -> manifest version, to map reader-slot pins to extents.
    pub view_versions: Mutex<BTreeMap<u64, ManifestVersion>>,
    /// Every committed compaction (test hook).
    pub compactions: Mutex<Vec<CompactionRecord>>,
    /// Every WAL record appended, in append order (test hook).
    #[cfg(feature = "test-hooks")]
    pub appended: Mutex<Vec<AppendedRecord>>,
    /// Test hook: arm the manifest queue's release window (see `manifest::race_window`).
    #[cfg(feature = "test-hooks")]
    pub manifest_race: AtomicBool,
    #[cfg(feature = "test-hooks")]
    pub manifest_race_waiter:
        Mutex<Option<pigeonhole_runtime::Waiter<Result<pigeonhole_format::ManifestVersion>>>>,
    /// Test hook: park background manifest commits before `end` (see `manifest::parked`).
    #[cfg(feature = "test-hooks")]
    pub manifest_park: AtomicBool,
    #[cfg(feature = "test-hooks")]
    pub manifest_parked: Mutex<Option<pigeonhole_runtime::TaskWaker>>,
    pub picker: PickerOptions,
    /// How long a commit waits for arena room before `Busy`.
    pub write_stall_timeout_nanos: u64,
    pub locks: Mutex<Option<Locks>>,
    pub default_durability: AtomicU8,
    pub closed: AtomicBool,
    /// `Engine::close` has started: background compaction stops.
    pub closing: AtomicBool,
    /// A manifest (root) commit failed: the pager is poisoned (decision D58) and every later
    /// write fails until the database is reopened.
    pub pager_poisoned: AtomicBool,
    pub close: CloseState,
    pub metrics: Vec<ShardMetrics>,
    /// Per-shard largest default timestamp assigned (decision D11), read at manifest commits.
    pub ts_floors: Vec<Padded>,
    pub waiters: VisibilityWaiters,
    /// Shards with a freeze deferred until the watermark passes their memtable (kicked by
    /// whoever publishes a watermark).
    pub freeze_waiting: AtomicUsize,
    pub freeze_waiters: Mutex<Vec<u16>>,
    pub memtable_freeze_bytes: u64,
    /// Submitters for every shard, set once the runtime is built.
    pub submitters: std::sync::OnceLock<Vec<Submitter<ShardMsg>>>,
    pub shm_dir: Option<std::path::PathBuf>,
    pub identity: pigeonhole_io::FileIdentity,
    pub path: std::path::PathBuf,
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

    /// Sends `f()` to every shard (nothing before the runtime exists).
    pub(crate) fn broadcast(&self, f: impl Fn() -> ShardMsg) {
        if let Some(subs) = self.submitters.get() {
            for s in subs {
                let _ = s.submit(f());
            }
        }
    }

    /// Registers an async waiter for `seqno` to become visible; returns true if it already is
    /// (the caller then proceeds without waiting).
    pub(crate) fn wait_visible(&self, seqno: Seqno, waker: &Waker) -> bool {
        if self.shm.visible_seqno() >= seqno {
            return true;
        }
        {
            let mut list = self
                .waiters
                .list
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            list.push((seqno, waker.clone()));
            self.waiters.count.store(list.len(), Ordering::Release);
        }
        // The shards check the count after each watermark publish; a publish between the
        // first check and the registration is caught by this second look.
        self.shm.visible_seqno() >= seqno
    }

    /// Wakes every registered waiter whose seqno is visible now. Called by shards after a
    /// watermark publish, and only when someone is registered (one relaxed load otherwise).
    pub(crate) fn wake_visible(&self) {
        if self.freeze_waiting.load(Ordering::Acquire) != 0 {
            let shards: Vec<u16> = std::mem::take(
                &mut *self
                    .freeze_waiters
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner),
            );
            self.freeze_waiting.store(0, Ordering::Release);
            for s in shards {
                let _ = self.submitter(ShardId(s)).submit(ShardMsg::Kick);
            }
        }
        if self.waiters.count.load(Ordering::Acquire) == 0 {
            return;
        }
        let visible = self.shm.visible_seqno();
        let mut list = self
            .waiters
            .list
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut i = 0;
        while i < list.len() {
            if list[i].0 <= visible {
                let (_, w) = list.swap_remove(i);
                w.wake();
            } else {
                i += 1;
            }
        }
        self.waiters.count.store(list.len(), Ordering::Release);
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
        {
            let mut vv = self
                .view_versions
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            vv.insert(version, view.manifest_version);
            // Keep what reader pins can still name: the oldest pinned view and newer.
            let floor = self.shm.oldest_reader_pin().map_or(version, |(_, v)| {
                if v == 0 { version } else { v.min(version) }
            });
            let keep: BTreeMap<u64, ManifestVersion> = vv.split_off(&floor);
            let last_below = vv.iter().next_back().map(|(k, v)| (*k, *v));
            *vv = keep;
            if let Some((k, v)) = last_below {
                vv.insert(k, v);
            }
        }
        self.view.store(Arc::clone(&view));
        *last = version;
        Ok(view)
    }

    /// The oldest manifest version any view uses: live in-process views and the view a
    /// reader slot pins (a pin protects its version and newer).
    pub(crate) fn oldest_live_manifest(&self) -> ManifestVersion {
        let current = self.view.load().manifest_version;
        let mut oldest = self.live_views.oldest().unwrap_or(current).min(current);
        if let Some((_, view_version)) = self.shm.oldest_reader_pin()
            && view_version != 0
        {
            let vv = self
                .view_versions
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let pinned = vv.range(..=view_version).next_back().map_or(0, |(_, m)| *m);
            oldest = oldest.min(pinned);
        }
        oldest
    }

    /// Frees extents no view can reach any more (decision D61 clamps to the durable root).
    pub(crate) fn reclaim(&self) {
        self.pager.reclaim(self.oldest_live_manifest());
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
    /// is the last process, removes the WAL files (every stream is checkpointed to its end
    /// by now) and the shared-memory region, so the database is one file at rest.
    pub(crate) fn final_close(&self) -> Result<()> {
        // A failed final sync means the close is not clean: the flag stays clear so the next
        // open replays, and the caller learns about it.
        let mut clean = if self.close.failed.load(Ordering::Acquire) {
            Err(Error::Io(pigeonhole_io::Error::new(
                ErrorKind::Other,
                "a shard's final flush, checkpoint or WAL sync failed; the close is not clean",
            )))
        } else {
            Ok(())
        };
        let locks = self
            .locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        // Our writer byte is held (so no opening writer can be mid-open); the presence
        // upgrade tells whether any reader is still attached.
        let last = match &locks {
            Some(locks) => locks.presence.try_become_last()?,
            None => false,
        };
        if clean.is_ok() && last {
            // The WAL files go away with this close: the streams the next open creates
            // start over at epoch 1, so their checkpoints must not point into the old
            // files (recovery would skip everything below them).
            clean = self.forget_checkpoints();
        }
        if clean.is_ok() {
            // Under the exclusion (see `try_final_close`): no commit is in flight, and the
            // queue is drained first, so nothing commits after the clean mark and clears it.
            manifest::drain_sync(self);
            let mut manifest = self.manifest.lock().unwrap_or_else(PoisonError::into_inner);
            clean = manifest.mark_clean();
        }
        if let Some(locks) = locks {
            if last {
                if clean.is_ok() {
                    remove_wal_files(&self.vfs, &self.path)?;
                }
                ShmRegion::remove(&self.vfs, self.identity, self.shm_dir.as_deref())?;
            }
            drop(locks);
        }
        clean
    }

    /// Commits a manifest delta resetting every stream's checkpoint to the start. Called
    /// with the manifest writer's exclusion held: the delta goes through the queue, after
    /// every commit still queued, and is computed against the catalog they leave.
    fn forget_checkpoints(&self) -> Result<()> {
        // What is queued may move checkpoints: commit it first, then look.
        manifest::drain_sync(self);
        if self.view.load().catalog.checkpoints.is_empty() {
            return Ok(());
        }
        let change = |catalog: &mut crate::catalog::Catalog| {
            Ok(catalog
                .checkpoints
                .keys()
                .map(|stream| Edit::WalCheckpoint {
                    stream: *stream,
                    lsn: Lsn::default(),
                })
                .collect())
        };
        manifest::commit_held(self, manifest::ReqKind::Catalog(Box::new(change))).map(|_| ())
    }

    fn report_closed(&self) {
        if self.close.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.close.final_pending.store(true, Ordering::SeqCst);
            self.try_final_close();
        }
    }

    /// Runs the final close if it is pending and the manifest writer's exclusion is free.
    /// A background commit (a compaction's, say) may hold it with its root commit in
    /// flight; the final close's own commits must not interleave with it, or both would be
    /// prepared from the same writer state (issue #78). Its holder calls this again from
    /// `manifest::release`. Never blocks, so it is safe on the shard thread that runs the
    /// holder's pump.
    pub(crate) fn try_final_close(&self) {
        // Pairs with the fence in `manifest::release`.
        std::sync::atomic::fence(Ordering::SeqCst);
        if !self.close.final_pending.load(Ordering::SeqCst) || !manifest::claim(self) {
            return;
        }
        if !self.close.final_pending.swap(false, Ordering::SeqCst) {
            manifest::release(self);
            return;
        }
        let result = self.final_close();
        manifest::release(self);
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

/// Removes every WAL stream file of `path` (after a clean close checkpointed them all).
pub(crate) fn remove_wal_files(vfs: &VfsRef, path: &std::path::Path) -> Result<()> {
    for stream in pigeonhole_wal::discover_streams(vfs, path)? {
        vfs.remove(&pigeonhole_wal::stream_path(path, stream))?;
    }
    let dir = match path.parent() {
        Some(d) if !d.as_os_str().is_empty() => d,
        _ => std::path::Path::new("."),
    };
    vfs.sync_dir(dir)?;
    Ok(())
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
    TooLarge,
    Io,
}

impl From<PrepareError> for Error {
    fn from(e: PrepareError) -> Self {
        match e {
            PrepareError::Conflict => Error::Conflict,
            PrepareError::Busy => Error::Busy,
            PrepareError::Closed => Error::Closed,
            PrepareError::TooLarge => Error::RecordTooLarge,
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
        Error::RecordTooLarge => PrepareError::TooLarge,
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
        coordinator: ShardId,
    },
    Applied {
        seqno: Seqno,
        from: ShardId,
        /// The participant could not apply its share (the data may be half applied there;
        /// the shard poisons itself) or had dropped it: the coordinator never acks `Ok`.
        error: Option<PrepareError>,
    },
    /// A WAL sync covering groups up to `group` finished.
    SyncDone {
        group: u64,
        result: std::result::Result<Lsn, pigeonhole_io::Error>,
    },
    /// Freeze every non-empty active memtable and reply once everything frozen so far is
    /// in the manifest (`Engine::flush`).
    FlushAll {
        reply: Notifier<Result<()>>,
    },
    /// A flush task finished (its manifest commit succeeded, or it failed).
    Flushed {
        items: Vec<FlushedItem>,
        result: Result<ManifestVersion>,
        nanos: u64,
    },
    /// Sync the stream (a flush's durability barrier) and reply when done.
    SyncBarrier {
        reply: Notifier<Result<()>>,
    },
    /// A participant's share of cross-shard commit `seqno` is in SSTs.
    ShareFlushed {
        seqno: Seqno,
    },
    /// The coordinator's checkpoint passed the COMMIT record of `seqno`: the participant's
    /// PREPARE is no longer needed.
    CommitCheckpointed {
        seqno: Seqno,
    },
    /// A `WalCheckpoint` edit for this stream is durable (or failed).
    Checkpointed {
        lsn: Lsn,
        commits: Vec<(Seqno, Vec<ShardId>)>,
        result: Result<ManifestVersion>,
    },
    /// The manifest changed: score the shard's slots for compaction, refresh stalls.
    Maintain,
    /// A compaction task finished.
    CompactionDone {
        inputs: Vec<SstId>,
        result: Result<ManifestVersion>,
        nanos: u64,
    },
    /// Compact every slot of `table` (or all tables) into the last level (`Engine::compact`).
    CompactAll {
        table: Option<TableId>,
        reply: Notifier<Result<()>>,
    },
    /// Run a manifest pump (a request was queued).
    PumpManifest,
    /// A table was dropped: forget its tablets' memtables.
    DropTablets {
        tablets: Vec<TabletId>,
    },
    /// Start-up work (spare WAL segments).
    Start,
    /// Nothing: forces a drain so deferred members form a group.
    Kick,
    /// Stop accepting writes, finish in-flight work, flush, checkpoint, sync, report.
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

/// A WAL record the engine appended to a stream (test hook): the per-stream append order,
/// which decides what a crash keeps (a stream survives as a prefix).
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendedRecord {
    pub stream: u16,
    pub seqno: Seqno,
    pub kind: AppendedKind,
    pub durability: Durability,
}

/// The kind of an [`AppendedRecord`].
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendedKind {
    Batch,
    Prepare,
    Commit,
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
    /// Arena bytes reserved for it until it is applied or dropped.
    reserved: usize,
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
            reserved: 0,
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
    /// Arena bytes reserved until the decision.
    reserved: usize,
    /// Its rows are counted in `pending_rows` (from admission until the decision).
    tracked: bool,
}

/// A memtable with what flush and compaction need to know about it: the smallest user
/// timestamp written to it (a compaction's `GcPolicy::min_ts_above`, decision D70) and
/// whether it holds applied shares of cross-shard commits (a flush then syncs every stream
/// before it persists them).
#[derive(Debug)]
struct MemEntry {
    table: Memtable,
    min_ts: Timestamp,
    has_shares: bool,
}

impl MemEntry {
    fn new(table: Memtable) -> Self {
        Self {
            table,
            min_ts: u64::MAX,
            has_shares: false,
        }
    }

    fn max_seqno(&self) -> Seqno {
        self.table.seqno_range().map_or(0, |(_, max)| max)
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
    fn set(&self, shard: ShardId, flushed: &HashSet<(u16, u32)>) -> Arc<MemSet> {
        let mut readers = Vec::with_capacity(1 + self.frozen.len());
        let mut roots = Vec::with_capacity(1 + self.frozen.len());
        readers.push(self.active.table.reader());
        roots.push(self.active.table.root());
        for m in &self.frozen {
            if flushed.contains(&(shard.0, m.table.root())) {
                continue;
            }
            readers.push(m.table.reader());
            roots.push(m.table.root());
        }
        Arc::new(MemSet {
            shard,
            readers,
            roots,
        })
    }

    /// The readers of the memtables not yet in SSTs, active first.
    fn readers(&self, shard: ShardId, flushed: &HashSet<(u16, u32)>) -> Vec<MemtableReader> {
        std::iter::once(self.active.table.reader())
            .chain(
                self.frozen
                    .iter()
                    .filter(|m| !flushed.contains(&(shard.0, m.table.root())))
                    .map(|m| m.table.reader()),
            )
            .collect()
    }

    /// The smallest seqno in any of these memtables, `None` when all are empty.
    fn min_seqno(&self) -> Option<Seqno> {
        std::iter::once(&self.active)
            .chain(&self.frozen)
            .filter_map(|m| m.table.seqno_range().map(|(min, _)| min))
            .min()
    }

    /// The smallest user timestamp in any of these memtables.
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
/// `(table, family, row, qualifier, timestamp)` wins. Mutations are compared exactly (by the
/// bytes of their key parts, located by offset within the batch), with reusable buffers and
/// no per-cell allocation.
#[derive(Debug, Default)]
struct Dedup {
    keys: Vec<MutKey>,
    order: Vec<u32>,
    loser: Vec<bool>,
}

/// Where a mutation's key parts lie within the batch bytes.
#[derive(Debug, Clone, Copy)]
struct MutKey {
    table: u32,
    family: u32,
    marker: bool,
    row: (u32, u32),
    qualifier: (u32, u32),
    ts: Timestamp,
}

impl Dedup {
    /// Fills the tables for `batch` (whose bytes are `bytes`). Returns whether any
    /// duplicate exists.
    fn scan(&mut self, batch: BatchRef<'_>, bytes: &[u8], commit_ts: Timestamp) -> bool {
        self.keys.clear();
        let base = bytes.as_ptr() as usize;
        let span = |part: &[u8]| -> (u32, u32) {
            let off = (part.as_ptr() as usize).wrapping_sub(base);
            (off as u32, part.len() as u32)
        };
        for m in batch.iter().flatten() {
            self.keys.push(MutKey {
                table: m.table.0,
                family: m.family.0,
                marker: m.kind == Kind::FamilyDelete,
                row: span(m.row),
                qualifier: span(m.qualifier),
                ts: m.ts.unwrap_or(commit_ts),
            });
        }
        let n = self.keys.len();
        self.loser.clear();
        self.loser.resize(n, false);
        if n < 2 {
            return false;
        }
        self.order.clear();
        self.order.extend(0..n as u32);
        let keys = &self.keys;
        let part = |(off, len): (u32, u32)| &bytes[off as usize..(off + len) as usize];
        let cmp = |a: &MutKey, b: &MutKey| {
            (a.table, a.family, a.marker)
                .cmp(&(b.table, b.family, b.marker))
                .then_with(|| part(a.row).cmp(part(b.row)))
                .then_with(|| part(a.qualifier).cmp(part(b.qualifier)))
                .then_with(|| a.ts.cmp(&b.ts))
        };
        self.order
            .sort_unstable_by(|a, b| cmp(&keys[*a as usize], &keys[*b as usize]).then(a.cmp(b)));
        let mut dup = false;
        let order = &self.order;
        for w in order.windows(2) {
            let (a, b) = (w[0] as usize, w[1] as usize);
            if cmp(&keys[a], &keys[b]).is_eq() {
                // Equal keys sort by index, so the earlier one loses to the later one.
                self.loser[a] = true;
                dup = true;
            }
        }
        dup
    }

    fn wins(&self, i: usize) -> bool {
        !self.loser.get(i).copied().unwrap_or(false)
    }
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

/// Kicks the shard once a write stall's wait has passed (or the stall was cancelled).
///
/// The timer is only a shortcut for a running clock: when the clock stops moving it gives
/// up rather than spin (the shard loop never reaches its slice deadline on a frozen clock),
/// and the stall ends on the background event it waits for (issue #70).
struct StallTimer {
    vfs: VfsRef,
    release_at: u64,
    /// Set by the shard to cancel the timer, and by the timer when it finishes.
    cancel: Arc<AtomicBool>,
    submitter: Submitter<ShardMsg>,
    last_now: u64,
    frozen_polls: u32,
}

impl StallTimer {
    fn new(
        vfs: &VfsRef,
        release_at: u64,
        cancel: Arc<AtomicBool>,
        submitter: Submitter<ShardMsg>,
    ) -> Self {
        Self {
            vfs: Arc::clone(vfs),
            release_at,
            cancel,
            submitter,
            last_now: 0,
            frozen_polls: 0,
        }
    }
}

impl Task for StallTimer {
    fn run(&mut self, _deadline_nanos: u64, _waker: &TaskWaker) -> TaskPoll {
        if self.cancel.load(Ordering::Acquire) {
            return TaskPoll::Done;
        }
        let now = self.vfs.monotonic_nanos();
        if now < self.release_at {
            if now != self.last_now {
                self.last_now = now;
                self.frozen_polls = 0;
                return TaskPoll::Pending;
            }
            self.frozen_polls += 1;
            if self.frozen_polls < STALL_TIMER_FROZEN_POLLS {
                return TaskPoll::Pending;
            }
            // The shard arms a new timer at the next event that finds it still stalled.
            self.cancel.store(true, Ordering::Release);
            return TaskPoll::Done;
        }
        self.cancel.store(true, Ordering::Release);
        let _ = self.submitter.submit(ShardMsg::Kick);
        TaskPoll::Done
    }

    fn name(&self) -> &'static str {
        "stall"
    }
}

/// A group waiting for a flush to free memtable arena room (a write stall, counted in the
/// metrics), refused with `Busy` once `write_stall_timeout_nanos` have passed or
/// `ROOM_FLUSH_ATTEMPTS` flushes in a row failed.
#[derive(Debug)]
struct RoomWait {
    since: u64,
    /// The timeout timer's cancel flag (set once it finished).
    timer: Arc<AtomicBool>,
    failed_flushes: u32,
}

/// A record of this stream the checkpoint cannot pass yet.
#[derive(Debug)]
struct Logged {
    /// Position just past the record.
    end: Lsn,
    seqno: Seqno,
    kind: LoggedKind,
}

#[derive(Debug)]
enum LoggedKind {
    /// A single-shard commit writing to these slots.
    Single { slots: Vec<(TabletId, FamilyId)> },
    /// A participant's PREPARE writing to these slots.
    Prepare { slots: Vec<(TabletId, FamilyId)> },
    /// A coordinator's COMMIT decision.
    Commit { participants: Vec<ShardId> },
}

/// A share this shard applied whose coordinator has not yet been told it is in SSTs.
#[derive(Debug)]
struct UnreportedShare {
    seqno: Seqno,
    coordinator: ShardId,
    slots: Vec<(TabletId, FamilyId)>,
}

/// The per-shard token bucket on L0 depth (spec: Write path).
#[derive(Debug)]
struct Stall {
    /// Highest L0 score over the shard's slots (`CompactionPicker::score`).
    score: f64,
    tokens: f64,
    last_refill: u64,
    /// The timer's cancel flag (set once it finished).
    timer: Option<Arc<AtomicBool>>,
    /// When the current stall started (0 = none).
    since: u64,
}

impl Default for Stall {
    fn default() -> Self {
        Self {
            score: 0.0,
            tokens: STALL_CAPACITY,
            last_refill: 0,
            timer: None,
            since: 0,
        }
    }
}

impl Stall {
    fn cancel_timer(&mut self) {
        if let Some(t) = self.timer.take() {
            t.store(true, Ordering::Release);
        }
    }
}

/// Where a shard is in its close sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloseStage {
    Open,
    /// Writes refused; waiting for in-flight groups and cross-shard commits.
    Draining,
    /// Everything frozen; waiting for the flushes and the checkpoint exchange.
    Flushing,
    /// The final checkpoint edit is in flight.
    Checkpointing,
    Reported,
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
    /// Retired memtables waiting for reader processes to release their views:
    /// `(view version that dropped them, token)`.
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
    /// Rows (hashed with their table) of prepared, undecided shares, with a count per row.
    pending_rows: HashMap<u64, u32>,
    /// Arena bytes reserved by admitted-but-unapplied members and undecided shares.
    reserved: usize,
    /// `(tablet, family)` slots whose active memtable crossed the freeze threshold.
    to_freeze: Vec<(TabletId, FamilyId)>,
    /// Largest default timestamp assigned on this shard (D11).
    ts_floor: Timestamp,
    key_buf: Vec<u8>,
    dedup: Dedup,
    touched: HashSet<u64>,
    /// Slots the last `apply` wrote (deduplicated).
    touched_slots: Vec<(TabletId, FamilyId)>,
    closing: bool,
    close_stage: CloseStage,
    spares: Option<SpareSegments>,
    spares_running: Arc<AtomicBool>,
    /// A memtable was created or frozen since the last view publish.
    view_dirty: bool,
    /// Replaying the WAL at open: mutations at or below a slot's flushed seqno are skipped.
    replaying: bool,

    // ---- flush ----
    /// Every write to `(tablet, family)` with a seqno at or below this is in SSTs.
    flushed: HashMap<(TabletId, FamilyId), Seqno>,
    /// Frozen memtables not yet handed to a flush task.
    flush_queue: Vec<FlushItem>,
    /// Roots handed to the running flush task.
    flushing: Vec<u32>,
    flush_running: bool,
    /// `Engine::flush` callers waiting for every frozen memtable to reach the manifest.
    flush_waiters: Vec<Notifier<Result<()>>>,
    /// A group is deferred until a flush frees arena room.
    wait_room: bool,
    /// The wait for arena room in progress: when it began and its timeout timer.
    room_wait: Option<RoomWait>,
    /// A freeze waits for the watermark to pass the memtable (registered in `Shared`).
    freeze_deferred: bool,
    /// A freeze of every memtable (`flush`, `compact`, close) is still owed: some memtable
    /// held a seqno the watermark had not reached yet.
    freeze_all_pending: bool,
    /// A flush failed while closing: the close gives up on flushing (the WAL keeps the data)
    /// and is not clean.
    flush_failed: bool,
    /// Tablets dropped since open: their records need no flush before a checkpoint.
    dropped: HashSet<TabletId>,

    // ---- checkpoints ----
    /// Records the checkpoint cannot pass, in log order.
    log: VecDeque<Logged>,
    /// End of the newest record ever appended to the stream.
    last_end: Option<Lsn>,
    /// The checkpoint the manifest holds.
    checkpoint: Lsn,
    /// The checkpoint the next edit should record (end of the longest unneeded prefix).
    checkpoint_candidate: Lsn,
    checkpoint_inflight: bool,
    checkpoint_dirty: bool,
    /// COMMIT records the candidate passed: participants are told once the edit is durable.
    passed_commits: Vec<(Seqno, Vec<ShardId>)>,
    /// Prepares and commits of aborted or incomplete cross-shard commits (never needed).
    aborted: HashSet<Seqno>,
    /// Commits whose coordinator checkpointed the COMMIT record.
    commit_ckpt: HashSet<Seqno>,
    /// Coordinator: `(participants that reported their share flushed, participants)` per
    /// commit whose COMMIT record is in the log.
    share_reports: HashMap<Seqno, (usize, usize)>,
    /// Participant: applied shares whose flush has not been reported to the coordinator.
    unreported: Vec<UnreportedShare>,

    // ---- compaction and stalls ----
    picker: CompactionPicker,
    /// The slot a compaction task is running for.
    compaction: Option<(TabletId, FamilyId)>,
    /// `Engine::compact` callers: `(table filter, reply)`, served in order.
    compact_all: VecDeque<(Option<TableId>, Notifier<Result<()>>)>,
    /// The last compaction error (reported to a `compact` caller).
    compaction_error: Option<Error>,
    /// A background compaction failed: none starts until a flush or new writes happen.
    compaction_backoff: bool,
    stall: Stall,
}

impl std::fmt::Debug for ShardState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardState")
            .field("id", &self.id)
            .field("memtables", &self.memtables.len())
            .field("pending", &self.pending.len())
            .field("unresolved", &self.unresolved.len())
            .field("held", &self.held)
            .field("log", &self.log.len())
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
        let picker = CompactionPicker::new(CompactionStyle::Leveled, shared.picker.clone());
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
            pending_rows: HashMap::new(),
            reserved: 0,
            to_freeze: Vec::new(),
            ts_floor,
            key_buf: Vec::new(),
            dedup: Dedup::default(),
            touched: HashSet::new(),
            touched_slots: Vec::new(),
            closing: false,
            close_stage: CloseStage::Open,
            spares: None,
            spares_running: Arc::new(AtomicBool::new(false)),
            view_dirty: false,
            replaying: true,
            flushed: HashMap::new(),
            flush_queue: Vec::new(),
            flushing: Vec::new(),
            flush_running: false,
            flush_waiters: Vec::new(),
            wait_room: false,
            room_wait: None,
            freeze_deferred: false,
            freeze_all_pending: false,
            flush_failed: false,
            dropped: HashSet::new(),
            log: VecDeque::new(),
            last_end: None,
            checkpoint: Lsn::default(),
            checkpoint_candidate: Lsn::default(),
            checkpoint_inflight: false,
            checkpoint_dirty: false,
            passed_commits: Vec::new(),
            aborted: HashSet::new(),
            commit_ckpt: HashSet::new(),
            share_reports: HashMap::new(),
            unreported: Vec::new(),
            picker,
            compaction: None,
            compact_all: VecDeque::new(),
            compaction_error: None,
            compaction_backoff: false,
            stall: Stall::default(),
        }
    }

    pub(crate) fn set_wal(&mut self, wal: Box<dyn Wal>) {
        self.spares = wal.spares();
        self.wal = Some(wal);
    }

    /// The per-slot flushed seqnos and this stream's checkpoint, from the manifest at open.
    pub(crate) fn set_recovery_state(
        &mut self,
        flushed: HashMap<(TabletId, FamilyId), Seqno>,
        checkpoint: Lsn,
        end: Option<Lsn>,
    ) {
        self.flushed = flushed;
        self.checkpoint = checkpoint;
        self.checkpoint_candidate = checkpoint;
        self.last_end = end;
    }

    /// Replay is over: later applies are live.
    pub(crate) fn finish_replay(&mut self) {
        self.replaying = false;
    }

    pub(crate) fn raise_ts_floor(&mut self, ts: Timestamp) {
        trace!("shard {} raise floor {} -> {ts}", self.id.0, self.ts_floor);
        self.ts_floor = self.ts_floor.max(ts);
    }

    /// The largest default timestamp known to this shard (seeded from replay, D11).
    pub(crate) fn ts_floor(&self) -> Timestamp {
        self.ts_floor
    }

    /// The memtable sets of this shard, for a view.
    pub(crate) fn mem_sets(&self) -> Vec<((TabletId, FamilyId), Arc<MemSet>)> {
        let flushed = self
            .shared
            .flushed_roots
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.memtables
            .iter()
            .map(|(k, slot)| (*k, slot.set(self.id, &flushed)))
            .collect()
    }

    /// Applies a replayed record at open (before the shards run).
    pub(crate) fn replay(
        &mut self,
        bytes: &[u8],
        seqno: Seqno,
        commit_ts: Timestamp,
    ) -> Result<Vec<(TabletId, FamilyId)>> {
        self.raise_ts_floor(commit_ts);
        self.apply(bytes, seqno, commit_ts)?;
        Ok(self.touched_slots.clone())
    }

    /// Records a replayed record of this stream for checkpointing: a single commit, an
    /// applied prepare (`coordinator`), an aborted or incomplete one (`needed == false`), or
    /// a COMMIT decision.
    pub(crate) fn log_replayed(&mut self, end: Lsn, seqno: Seqno, kind: ReplayedKind) {
        self.last_end = Some(self.last_end.map_or(end, |l| l.max(end)));
        match kind {
            ReplayedKind::Single { slots } => self.log.push_back(Logged {
                end,
                seqno,
                kind: LoggedKind::Single { slots },
            }),
            ReplayedKind::Prepare {
                slots,
                coordinator,
                applied,
            } => {
                if applied {
                    self.unreported.push(UnreportedShare {
                        seqno,
                        coordinator,
                        slots: slots.clone(),
                    });
                } else {
                    self.aborted.insert(seqno);
                }
                self.log.push_back(Logged {
                    end,
                    seqno,
                    kind: LoggedKind::Prepare { slots },
                });
            }
            ReplayedKind::Commit {
                participants,
                complete,
            } => {
                if !complete {
                    self.aborted.insert(seqno);
                }
                self.share_reports.entry(seqno).or_insert((0, 0)).1 = participants.len();
                self.log.push_back(Logged {
                    end,
                    seqno,
                    kind: LoggedKind::Commit { participants },
                });
            }
        }
    }

    /// Takes every memtable (open-time flush when the shard count changed, decision D20).
    pub(crate) fn take_memtables(
        &mut self,
    ) -> Vec<((TabletId, FamilyId), MemtableReader, u64, Seqno)> {
        let mut out = Vec::new();
        let keys: Vec<_> = self.memtables.keys().copied().collect();
        for key in keys {
            let slot = self.memtables.remove(&key).expect("listed");
            for m in std::iter::once(slot.active).chain(slot.frozen) {
                if !m.table.is_empty() {
                    out.push((
                        key,
                        m.table.reader(),
                        m.table.allocated_bytes() as u64,
                        m.max_seqno(),
                    ));
                }
                let retired = m.table.retire();
                self.arena.reclaim(retired);
            }
        }
        self.log.clear();
        self.unreported.clear();
        self.share_reports.clear();
        self.aborted.clear();
        out
    }

    /// The slots `bytes` writes on this shard (routing only, no apply).
    fn slots_of(&mut self, bytes: &[u8]) -> Vec<(TabletId, FamilyId)> {
        let mut out: Vec<(TabletId, FamilyId)> = Vec::new();
        let Ok(batch) = BatchRef::new(bytes) else {
            return out;
        };
        let tablets = Arc::clone(&self.tablets);
        for m in batch.iter().flatten() {
            let Some((tablet, owner)) = tablets.route(m.table, m.row) else {
                continue;
            };
            if owner != self.id {
                continue;
            }
            let key = (tablet, m.family);
            if !out.contains(&key) {
                out.push(key);
            }
        }
        out
    }

    // ---- memtables and views ----

    /// Publishes this shard's memtable sets into a new view: only this shard's piece is
    /// rebuilt; the other shards' pieces are shared by reference.
    fn publish_memtables(&mut self) -> Result<()> {
        self.view_dirty = false;
        let i = usize::from(self.id.0);
        let sets = self.mem_sets();
        let piece = Arc::new(ShardMems {
            map: sets.into_iter().collect(),
        });
        self.shared.publish_view(|current, version| {
            let mut mems = current.mems.clone();
            if i < mems.len() {
                mems[i] = piece;
            }
            View {
                version,
                manifest_version: current.manifest_version,
                tablets: Arc::clone(&current.tablets),
                catalog: Arc::clone(&current.catalog),
                mems,
                ssts: Arc::clone(&current.ssts),
                _pin: Some(crate::snapshot::ViewPin::new(
                    &self.shared.live_views,
                    current.manifest_version,
                )),
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

    /// Forces the arena to account for retired memtables whose last in-process handle has
    /// dropped since (the arena releases them on its next allocation).
    fn refresh_free(&mut self) {
        if let Ok(m) = Memtable::create(&mut self.arena) {
            self.arena.reclaim(m.retire());
        }
    }

    /// Reserves arena room for `bytes` (on top of everything already reserved by members of
    /// this group and undecided shares) or returns `None` when it would not fit.
    fn reserve_room(&mut self, bytes: &[u8]) -> std::result::Result<usize, Room> {
        let needed = match BatchRef::new(bytes) {
            Ok(batch) => Self::arena_needed(batch, self.chunk_size),
            Err(_) => 0,
        };
        if self.arena.free_bytes() < self.reserved.saturating_add(needed) {
            self.refresh_free();
        }
        trace!(
            "shard {} reserve: needed={needed} free={} reserved={} total={}",
            self.id.0,
            self.arena.free_bytes(),
            self.reserved,
            self.arena.region().len()
        );
        if self.arena.free_bytes() < self.reserved.saturating_add(needed) {
            let total = self.arena.region().len();
            return Err(if needed + 2 * self.chunk_size > total {
                Room::Never
            } else {
                Room::Wait
            });
        }
        self.reserved += needed;
        Ok(needed)
    }

    /// The wait for arena room is over: account the stall and cancel its timer.
    fn end_room_wait(&mut self, now: u64) {
        if let Some(w) = self.room_wait.take() {
            w.timer.store(true, Ordering::Release);
            self.shared.metrics[usize::from(self.id.0)]
                .stall_nanos
                .fetch_add(now.saturating_sub(w.since), Ordering::Relaxed);
        }
    }

    fn release_room(&mut self, bytes: usize) {
        self.reserved = self.reserved.saturating_sub(bytes);
    }

    /// Freezes the active memtables that crossed the threshold during `apply` (or every
    /// non-empty one when `all`), and queues them for flushing. A memtable freezes only
    /// once every seqno it holds is visible: then its largest seqno is exact as the slot's
    /// flushed-through seqno (no lower seqno can land in a newer memtable), so replay skips
    /// exactly what the SST holds.
    fn freeze(&mut self, all: bool) -> Result<()> {
        let all = all || self.freeze_all_pending;
        let threshold = self.shared.memtable_freeze_bytes as usize;
        let keys: Vec<(TabletId, FamilyId)> = if all {
            self.to_freeze.clear();
            self.memtables.keys().copied().collect()
        } else {
            std::mem::take(&mut self.to_freeze)
        };
        let visible = self.shared.shm.visible_seqno();
        let view = self.shared.view.load();
        let mut deferred = false;
        for key in keys {
            let Some(slot) = self.memtables.get_mut(&key) else {
                continue;
            };
            let big = slot.active.table.allocated_bytes() >= threshold;
            trace!(
                "shard {} freeze {:?}: all={all} big={big} empty={} max_seqno={} visible={visible}",
                self.id.0,
                key,
                slot.active.table.is_empty(),
                slot.active.max_seqno()
            );
            if slot.active.table.is_empty() || !(all || big) {
                continue;
            }
            if slot.active.max_seqno() > visible {
                deferred = true;
                if !self.to_freeze.contains(&key) {
                    self.to_freeze.push(key);
                }
                continue;
            }
            let Some(meta) = view.catalog.family(key.1) else {
                continue;
            };
            let Ok(fresh) = Memtable::create(&mut self.arena) else {
                // No chunk for a new active memtable: keep writing into this one; the
                // arena-room check defers later commits until a flush frees space.
                trace!(
                    "shard {} freeze {:?}: no chunk for a fresh memtable",
                    self.id.0, key
                );
                continue;
            };
            let mut old = std::mem::replace(&mut slot.active, MemEntry::new(fresh));
            old.table.freeze();
            self.flush_queue.push(FlushItem {
                table: meta.table,
                tablet: key.0,
                family: key.1,
                root: old.table.root(),
                reader: old.table.reader(),
                bytes: old.table.allocated_bytes() as u64,
                max_seqno: old.max_seqno(),
                has_shares: old.has_shares,
                options: meta.options.clone(),
            });
            slot.frozen.insert(0, old);
            self.view_dirty = true;
        }
        self.freeze_all_pending = all && deferred;
        if deferred && !self.freeze_deferred {
            self.freeze_deferred = true;
            self.shared
                .freeze_waiters
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(self.id.0);
            self.shared.freeze_waiting.fetch_add(1, Ordering::AcqRel);
        } else if !deferred {
            self.freeze_deferred = false;
        }
        if self.view_dirty {
            self.publish_memtables()?;
        }
        Ok(())
    }

    /// Starts a flush task over everything queued, if none is running.
    fn spawn_flush(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        if self.flush_running || self.flush_queue.is_empty() {
            return;
        }
        if self.shared.pager_poisoned.load(Ordering::Acquire) {
            for w in self.flush_waiters.drain(..) {
                w.notify(Err(ManifestWriter::poisoned_error()));
            }
            return;
        }
        let items = std::mem::take(&mut self.flush_queue);
        self.flushing = items.iter().map(|i| i.root).collect();
        self.flush_running = true;
        ctx.spawn(Box::new(FlushTask::new(
            Arc::clone(&self.shared),
            self.id,
            items,
        )));
    }

    /// Whether every frozen memtable has reached the manifest.
    fn flush_idle(&self) -> bool {
        !self.flush_running
            && self.flush_queue.is_empty()
            && self.memtables.values().all(|s| s.frozen.is_empty())
    }

    fn check_flush_waiters(&mut self) {
        if self.freeze_all_pending || !self.to_freeze.is_empty() {
            return;
        }
        if self.flush_idle() {
            for w in self.flush_waiters.drain(..) {
                w.notify(Ok(()));
            }
        }
    }

    /// A flush task finished.
    fn on_flushed(
        &mut self,
        items: Vec<FlushedItem>,
        result: Result<ManifestVersion>,
        nanos: u64,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        self.flush_running = false;
        self.flushing.clear();
        match result {
            Ok(_) => {
                let metrics = &self.shared.metrics[usize::from(self.id.0)];
                metrics.flushes.fetch_add(1, Ordering::Relaxed);
                metrics.flush_nanos.fetch_add(nanos, Ordering::Relaxed);
                let version = self.shared.view.load().version;
                {
                    let mut roots = self
                        .shared
                        .flushed_roots
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    for item in &items {
                        roots.remove(&(self.id.0, item.root));
                    }
                }
                for item in items {
                    let key = (item.tablet, item.family);
                    let e = self.flushed.entry(key).or_insert(0);
                    *e = (*e).max(item.max_seqno);
                    if let Some(slot) = self.memtables.get_mut(&key)
                        && let Some(pos) =
                            slot.frozen.iter().position(|m| m.table.root() == item.root)
                    {
                        let m = slot.frozen.remove(pos);
                        self.retired.push((version, m.table.retire()));
                    }
                }
                self.reclaim_retired();
                self.report_shares_flushed(ctx);
                self.advance_checkpoint(ctx);
            }
            Err(e) => {
                // The frozen memtables stay (the WAL keeps their data); they are queued again
                // at the next flush trigger, never in a tight loop. A poisoned pager stops
                // flushing until reopen; a failure while closing makes the close unclean.
                trace!("shard {} flush failed: {e}", self.id.0);
                self.requeue_frozen();
                // Whoever asked for this flush hears about the failure now rather than
                // waiting for a retry that may never come (a dead device).
                let msg = e.to_string();
                for w in self.flush_waiters.drain(..) {
                    w.notify(Err(crate::error::io_other("flush", msg.clone())));
                }
                // A full compaction starts with a flush: it fails with it.
                for (_, w) in self.compact_all.drain(..) {
                    w.notify(Err(crate::error::io_other("flush", msg.clone())));
                }
                if self.closing {
                    self.flush_failed = true;
                    self.shared.close.failed.store(true, Ordering::Release);
                }
                if self.wait_room && !self.closing {
                    // The waiting members try the flush again (until their stall timeout or
                    // `ROOM_FLUSH_ATTEMPTS` failures).
                    if let Some(w) = &mut self.room_wait {
                        w.failed_flushes += 1;
                    }
                    self.wait_room = false;
                    let _ = ctx.submitter(self.id).submit(ShardMsg::Kick);
                }
                self.try_finish_close(ctx);
                return;
            }
        }
        if self.wait_room {
            self.wait_room = false;
            let _ = ctx.submitter(self.id).submit(ShardMsg::Kick);
        }
        self.compaction_backoff = false;
        self.check_flush_waiters();
        self.spawn_flush(ctx);
        self.maintain(ctx);
        self.try_finish_close(ctx);
    }

    /// Queues every frozen memtable that is not in the manifest (after a failed flush).
    fn requeue_frozen(&mut self) {
        let view = self.shared.view.load();
        let mut items = Vec::new();
        for (key, slot) in &self.memtables {
            let Some(meta) = view.catalog.family(key.1) else {
                continue;
            };
            for m in &slot.frozen {
                if self.flush_queue.iter().any(|i| i.root == m.table.root()) {
                    continue;
                }
                items.push(FlushItem {
                    table: meta.table,
                    tablet: key.0,
                    family: key.1,
                    root: m.table.root(),
                    reader: m.table.reader(),
                    bytes: m.table.allocated_bytes() as u64,
                    max_seqno: m.max_seqno(),
                    has_shares: m.has_shares,
                    options: meta.options.clone(),
                });
            }
        }
        self.flush_queue.extend(items);
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
        self.flushed.retain(|k, _| !tablets.contains(&k.0));
        self.flush_queue.retain(|i| !tablets.contains(&i.tablet));
        self.dropped.extend(tablets.iter().copied());
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
        self.shared.wake_visible();
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
        trace!(
            "shard {} default_ts now={now} floor={} -> {ts}",
            self.id.0, self.ts_floor
        );
        self.ts_floor = ts;
        self.shared.ts_floors[usize::from(self.id.0)]
            .0
            .store(ts, Ordering::Release);
        ts
    }

    // ---- reads on the shard (predicates, validation) ----

    /// The sources of `(tablet, family)` for a point read at the applied state: this
    /// shard's memtables (not yet in SSTs) and the view's SSTs.
    fn point_sources(
        &self,
        view: &View,
        tablet: TabletId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
    ) -> Result<Vec<Source>> {
        let mut out = Vec::new();
        if let Some(slot) = self.memtables.get(&(tablet, family)) {
            let flushed = self
                .shared
                .flushed_roots
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            mem_sources_from(
                &slot.readers(self.id, &flushed),
                &ScanFilter::all(),
                &mut out,
            );
        }
        let l = view.locate(self.id, tablet, family);
        if let Some(fam) = l.ssts
            && !fam.is_empty()
        {
            let probe = Probe::new(row, qualifier)?;
            sst_sources_point(fam, &view.ssts, &probe, l.priority, &mut out)?;
        }
        Ok(out)
    }

    /// Resolves the newest version of one column at the latest state (everything applied).
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
        let sources = self.point_sources(&view, tablet, family, row, qualifier)?;
        if sources.is_empty() {
            return Ok(None);
        }
        let mut opts = ResolveOptions::new(u64::MAX, self.shared.vfs.now_micros());
        opts.ttl_micros = meta.options.ttl_micros;
        opts.versions = 1;
        opts.merge = meta.merge_op.clone();
        let mut resolver = Resolver::new(MergingCursor::new(sources), opts);
        resolver.seek_column(row, qualifier)?;
        Ok(resolver
            .next_cell()
            .map_err(|e| crate::read::read_error(e, meta))?
            .map(|c| c.value.to_vec()))
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
                .is_some_and(|v| crate::read::predicate_matches(predicate, &v)),
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
        self.key_buf.clear();
        encode_row_prefix(&mut self.key_buf, &read.row)?;
        let prefix = std::mem::take(&mut self.key_buf);
        let mut found = false;
        if let Some(slot) = self.memtables.get(&(tablet, read.family)) {
            let flushed = self
                .shared
                .flushed_roots
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            'outer: for reader in slot.readers(self.id, &flushed) {
                let mut it = reader.iter();
                it.seek(&prefix)?;
                while it.valid() && it.key().starts_with(&prefix) {
                    let (_, _, seqno, _) = split_suffix(it.key())?;
                    if seqno > snapshot {
                        found = true;
                        break 'outer;
                    }
                    it.next()?;
                }
            }
        }
        if !found {
            let view = self.shared.view.load();
            let l = view.locate(self.id, tablet, read.family);
            if let Some(fam) = l.ssts {
                'ssts: for sst in fam.iter() {
                    if sst.meta.seqno_range.1 <= snapshot {
                        continue;
                    }
                    let mut past = Vec::new();
                    crate::read::past_row(&prefix, &mut past);
                    if sst.first_row() >= past.as_slice() || sst.last_row() < prefix.as_slice() {
                        continue;
                    }
                    let reader = sst.reader(&view.ssts, l.priority)?;
                    let mut it =
                        reader.iter(ScanFilter::all(), pigeonhole_sst::ReadOptions::default());
                    it.seek(&prefix)?;
                    while it.valid() && it.key().starts_with(&prefix) {
                        let (_, _, seqno, _) = split_suffix(it.key())?;
                        if seqno > snapshot {
                            found = true;
                            break 'ssts;
                        }
                        it.next()?;
                    }
                }
            }
        }
        self.key_buf = prefix;
        Ok(found)
    }

    // ---- apply ----

    /// Applies `bytes` at `seqno`/`commit_ts` to this shard's memtables, last write winning
    /// per `(column, timestamp)` (decision D34). Records the slots written in
    /// `touched_slots`.
    fn apply(&mut self, bytes: &[u8], seqno: Seqno, commit_ts: Timestamp) -> Result<()> {
        let batch = BatchRef::new(bytes)?;
        let dups = self.dedup.scan(batch, bytes, commit_ts);
        let threshold = self.shared.memtable_freeze_bytes as usize;
        let tablets = Arc::clone(&self.tablets);
        let mut last_route: Option<(TableId, &[u8], TabletId)> = None;
        let mut key_buf = std::mem::take(&mut self.key_buf);
        let mut result = Ok(());
        self.touched_slots.clear();
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
            if self.replaying
                && self
                    .flushed
                    .get(&(tablet, m.family))
                    .is_some_and(|f| seqno <= *f)
            {
                // Already in an SST (decision: replay applies only seqnos above SetFlushed).
                continue;
            }
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
            if !self.touched_slots.contains(&(tablet, m.family)) {
                self.touched_slots.push((tablet, m.family));
            }
            if slot.active.table.allocated_bytes() >= threshold
                && !self.to_freeze.contains(&(tablet, m.family))
            {
                self.to_freeze.push((tablet, m.family));
            }
        }
        self.key_buf = key_buf;
        result
    }

    // ---- stalls ----

    /// Whether the group must wait for the token bucket (L0 too deep). Spawns the timer
    /// that re-kicks the shard once a token is due.
    fn stalled(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) -> bool {
        let now = ctx.now_nanos();
        // A stall lets compaction catch up; when none can run (the last one failed, the
        // pager or this shard is poisoned) holding writers would hold them for ever.
        let mut hopeless = self.compaction_backoff
            || self.poisoned
            || self.shared.pager_poisoned.load(Ordering::Acquire);
        // A stall always waits on a running compaction, whose completion kicks the shard:
        // that ends it even when the clock does not move (issue #70).
        if !hopeless && self.stall.score >= 1.0 && self.compaction.is_none() {
            self.maintain(ctx);
            hopeless = self.compaction.is_none();
        }
        if self.stall.score < 1.0 || hopeless {
            self.stall.cancel_timer();
            self.stall.tokens = STALL_CAPACITY;
            self.stall.last_refill = now;
            if self.stall.since != 0 {
                self.shared.metrics[usize::from(self.id.0)]
                    .stall_nanos
                    .fetch_add(now.saturating_sub(self.stall.since), Ordering::Relaxed);
                self.stall.since = 0;
            }
            return false;
        }
        let rate = STALL_RATE / self.stall.score;
        let elapsed = now.saturating_sub(self.stall.last_refill) as f64 / 1e9;
        self.stall.last_refill = now;
        self.stall.tokens = (self.stall.tokens + elapsed * rate).min(STALL_CAPACITY);
        if self.stall.tokens >= 1.0 {
            self.stall.tokens -= 1.0;
            self.stall.cancel_timer();
            if self.stall.since != 0 {
                self.shared.metrics[usize::from(self.id.0)]
                    .stall_nanos
                    .fetch_add(now.saturating_sub(self.stall.since), Ordering::Relaxed);
                self.stall.since = 0;
            }
            return false;
        }
        if self.stall.since == 0 {
            self.stall.since = now;
            self.shared.metrics[usize::from(self.id.0)]
                .stalls
                .fetch_add(1, Ordering::Relaxed);
        }
        if self
            .stall
            .timer
            .as_ref()
            .is_none_or(|t| t.load(Ordering::Acquire))
        {
            let cancel = Arc::new(AtomicBool::new(false));
            self.stall.timer = Some(Arc::clone(&cancel));
            let wait = ((1.0 - self.stall.tokens) / rate * 1e9) as u64;
            ctx.spawn(Box::new(StallTimer::new(
                &self.shared.vfs,
                now + wait.max(1_000),
                cancel,
                ctx.submitter(self.id).clone(),
            )));
        }
        true
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
        if self.stalled(ctx) {
            return;
        }
        self.refresh_tablets();
        self.compaction_backoff = false;
        let members = std::mem::take(&mut self.pending);

        // Admission, in order. A conditional member whose row an earlier member of this
        // group already touched runs in the next group instead, with everything after it,
        // so conditions see the applied state and submission order holds per row.
        let mut admitted: Vec<Member> = Vec::with_capacity(members.len());
        self.touched.clear();
        // Once a conditional member is held back, every later plain commit waits with it
        // (per-row submission order); two-phase-commit records never wait, since the share
        // the member waits for may need them to be decided.
        let mut cut = false;
        let mut need_room = false;
        for mut m in members {
            if cut && matches!(m.kind, MemberKind::Single) {
                self.pending.push(m);
                continue;
            }
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
            // A conditional member reads the applied state: its written rows, its read
            // keys and its predicate row must not be touched by an earlier member of this
            // group (it then waits for the next group, with everything after it, so per-row
            // submission order holds) nor by a prepared, undecided share (a single member
            // waits for the decision; a PREPARE aborts instead, since waiting on another
            // commit's decision could deadlock two coordinators).
            if m.conditional() {
                let (same_group, pending_share) = self.conflicts_with_group(&m);
                if same_group || (pending_share && matches!(m.kind, MemberKind::Single)) {
                    if matches!(m.kind, MemberKind::Single) {
                        cut = true;
                        self.pending.push(m);
                    } else {
                        // A PREPARE that must wait aborts instead (deadlock avoidance).
                        m.failed = Some(Error::Conflict);
                        self.settle(m, Ok(()), ctx);
                    }
                    continue;
                }
                if pending_share {
                    m.failed = Some(Error::Conflict);
                    self.settle(m, Ok(()), ctx);
                    continue;
                }
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
            if !matches!(m.kind, MemberKind::CommitRecord { .. }) {
                match self.reserve_room(m.bytes.as_slice()) {
                    Ok(bytes) => m.reserved = bytes,
                    Err(Room::Never) => {
                        let metrics = &self.shared.metrics[usize::from(self.id.0)];
                        metrics.stalls.fetch_add(1, Ordering::Relaxed);
                        m.failed = Some(Error::Busy);
                        self.settle(m, Ok(()), ctx);
                        continue;
                    }
                    Err(Room::Wait) => {
                        if self.shared.pager_poisoned.load(Ordering::Acquire) {
                            // No flush can ever free anything: refuse now.
                            m.failed = Some(poisoned_error());
                            self.settle(m, Ok(()), ctx);
                            continue;
                        }
                        // Wait for a flush to free room (a write stall): this member and
                        // everything after it run in a later group.
                        cut = true;
                        need_room = true;
                        self.pending.push(m);
                        continue;
                    }
                }
            }
            self.touch_rows(&m);
            if let MemberKind::Prepare { .. } = m.kind
                && let Some(share) = self.prepared.get_mut(&m.seqno)
                && !share.tracked
            {
                share.tracked = true;
                let bytes = Arc::clone(&share.bytes);
                self.track_share_rows(bytes.batch().as_bytes(), true);
            }
            admitted.push(m);
        }
        if need_room {
            self.wait_room = true;
            let now = ctx.now_nanos();
            let timeout = self.shared.write_stall_timeout_nanos;
            match &self.room_wait {
                None => {
                    // The stall begins: count it, and arm the timeout.
                    let metrics = &self.shared.metrics[usize::from(self.id.0)];
                    metrics.stalls.fetch_add(1, Ordering::Relaxed);
                    let cancel = Arc::new(AtomicBool::new(false));
                    ctx.spawn(Box::new(StallTimer::new(
                        &self.shared.vfs,
                        now.saturating_add(timeout),
                        Arc::clone(&cancel),
                        ctx.submitter(self.id).clone(),
                    )));
                    self.room_wait = Some(RoomWait {
                        since: now,
                        timer: cancel,
                        failed_flushes: 0,
                    });
                }
                Some(w)
                    if now.saturating_sub(w.since) >= timeout
                        || w.failed_flushes >= ROOM_FLUSH_ATTEMPTS =>
                {
                    // Nothing freed room in time (or the flushes that would keep failing,
                    // which a frozen clock never times out): refuse the waiting members.
                    self.end_room_wait(now);
                    self.wait_room = false;
                    let waiting = std::mem::take(&mut self.pending);
                    for mut m in waiting {
                        if matches!(m.kind, MemberKind::CommitRecord { .. }) {
                            self.pending.push(m);
                            continue;
                        }
                        m.failed = Some(Error::Busy);
                        self.settle(m, Ok(()), ctx);
                    }
                    return;
                }
                Some(w) => {
                    // A timer that gave up on a stopped clock is armed again.
                    if w.timer.load(Ordering::Acquire) {
                        let cancel = Arc::new(AtomicBool::new(false));
                        ctx.spawn(Box::new(StallTimer::new(
                            &self.shared.vfs,
                            w.since.saturating_add(timeout),
                            Arc::clone(&cancel),
                            ctx.submitter(self.id).clone(),
                        )));
                        if let Some(w) = &mut self.room_wait {
                            w.timer = cancel;
                        }
                    }
                }
            }
            // A flush frees room (one that failed is tried again).
            let _ = self.freeze(true);
            self.spawn_flush(ctx);
        } else {
            if let Some(w) = &self.room_wait
                && !self.wait_room
            {
                let _ = w;
                self.end_room_wait(ctx.now_nanos());
            }
            if !self.pending.is_empty() {
                let _ = ctx.submitter(self.id).submit(ShardMsg::Kick);
            }
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
        // A `None` member's record only enters the buffer (decision #50): no write() or sync
        // of its own; the next stronger member's carries it.
        let mut appended = false;
        let mut unsynced = false;
        let mut last_sync: Option<pigeonhole_io::Completion<Lsn>> = None;
        for m in &mut group.members {
            if m.failed.is_some() {
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
            let result = wal.append(&record, m.durability);
            // Test hook: the append order. A failed write may still have landed (a crash
            // or an I/O error mid-write), so every attempt the stream accepted counts.
            #[cfg(feature = "test-hooks")]
            if !matches!(
                result,
                Err(pigeonhole_wal::Error::RecordTooLarge
                    | pigeonhole_wal::Error::InvalidArgument { .. })
            ) {
                self.shared
                    .appended
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(AppendedRecord {
                        stream: self.id.0,
                        seqno: m.seqno,
                        kind: match &m.kind {
                            MemberKind::Single => AppendedKind::Batch,
                            MemberKind::Prepare { .. } => AppendedKind::Prepare,
                            MemberKind::CommitRecord { .. } => AppendedKind::Commit,
                        },
                        durability: m.durability,
                    });
            }
            match result {
                Ok(t) => {
                    m.ticket = Some(t);
                    if m.durability != Durability::None {
                        appended = true;
                        unsynced = true;
                    }
                }
                Err(pigeonhole_wal::Error::RecordTooLarge) => {
                    m.failed = Some(Error::RecordTooLarge);
                    continue;
                }
                Err(pigeonhole_wal::Error::InvalidArgument { what }) => {
                    m.failed = Some(Error::InvalidArgument(what.to_owned()));
                    continue;
                }
                Err(e) => {
                    trace!("shard {} append failed: {e}", self.id.0);
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
            match &m.kind {
                MemberKind::Single => {
                    if let Err(e) = self.apply(m.bytes.as_slice(), m.seqno, m.commit_ts) {
                        trace!("shard {} apply of {} failed: {e}", self.id.0, m.seqno);
                        self.poisoned = true;
                        m.failed = Some(e);
                    }
                    self.release_room(m.reserved);
                    m.reserved = 0;
                    if let Some(t) = m.ticket {
                        self.last_end = Some(t.end);
                        self.log.push_back(Logged {
                            end: t.end,
                            seqno: m.seqno,
                            kind: LoggedKind::Single {
                                slots: self.touched_slots.clone(),
                            },
                        });
                    }
                }
                MemberKind::Prepare { .. } => {
                    // The share keeps its reservation until the decision.
                    if let Some(share) = self.prepared.get_mut(&m.seqno) {
                        share.reserved = m.reserved;
                        m.reserved = 0;
                    }
                    if let Some(t) = m.ticket {
                        let slots = self.slots_of(m.bytes.as_slice());
                        self.last_end = Some(t.end);
                        self.log.push_back(Logged {
                            end: t.end,
                            seqno: m.seqno,
                            kind: LoggedKind::Prepare { slots },
                        });
                    }
                }
                MemberKind::CommitRecord { participants } => {
                    if let Some(t) = m.ticket {
                        self.last_end = Some(t.end);
                        self.share_reports.entry(m.seqno).or_insert((0, 0)).1 = participants.len();
                        self.log.push_back(Logged {
                            end: t.end,
                            seqno: m.seqno,
                            kind: LoggedKind::Commit {
                                participants: participants.clone(),
                            },
                        });
                    }
                }
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
        self.spawn_flush(ctx);

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

    /// Whether `m`'s rows (written, read, or its predicate row) overlap rows touched by an
    /// earlier member of this group, and rows of prepared undecided shares.
    fn conflicts_with_group(&self, m: &Member) -> (bool, bool) {
        let mut same = false;
        let mut pending = false;
        let mut check = |h: u64| {
            same |= self.touched.contains(&h);
            pending |= self.pending_rows.contains_key(&h);
        };
        if let Ok(batch) = BatchRef::new(m.bytes.as_slice()) {
            for mu in batch.iter().flatten() {
                check(hash_row(mu.table, mu.row));
            }
        }
        if let Some((_, reads)) = &m.validate {
            for r in reads {
                check(hash_row(r.table, &r.row));
            }
        }
        if let Some((table, row, _)) = &m.predicate {
            check(hash_row(*table, row));
        }
        (same, pending)
    }

    /// Records `m`'s written rows as touched by this group.
    fn touch_rows(&mut self, m: &Member) {
        if matches!(m.kind, MemberKind::CommitRecord { .. }) {
            return;
        }
        if let Ok(batch) = BatchRef::new(m.bytes.as_slice()) {
            for mu in batch.iter().flatten() {
                self.touched.insert(hash_row(mu.table, mu.row));
            }
        }
    }

    /// Counts or releases the rows of a prepared share.
    fn track_share_rows(&mut self, bytes: &[u8], add: bool) {
        let Ok(batch) = BatchRef::new(bytes) else {
            return;
        };
        for mu in batch.iter().flatten() {
            let h = hash_row(mu.table, mu.row);
            if add {
                *self.pending_rows.entry(h).or_insert(0) += 1;
            } else if let Some(n) = self.pending_rows.get_mut(&h) {
                *n -= 1;
                if *n == 0 {
                    self.pending_rows.remove(&h);
                }
            }
        }
    }

    /// Delivers one member's outcome: a reply, a PREPARED, or the COMMIT decision.
    fn settle(&mut self, mut m: Member, outcome: Result<()>, ctx: &mut ShardContext<'_, ShardMsg>) {
        let never_logged = m.ticket.is_none() && m.durability != Durability::None;
        if m.failed.is_some() {
            // Not applied: its reservation is free again.
            self.release_room(m.reserved);
            m.reserved = 0;
        }
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
                if let Err(e) = &result {
                    trace!("shard {} prepare {} failed: {e}", self.id.0, m.seqno);
                }
                let error = result.as_ref().err().map(prepare_error_of);
                if error.is_some()
                    && let Some(share) = self.prepared.remove(&m.seqno)
                {
                    if share.tracked {
                        self.track_share_rows(share.bytes.batch().as_bytes(), false);
                    }
                    self.release_room(share.reserved);
                    // A failed prepare's record (if any) is never needed.
                    self.aborted.insert(m.seqno);
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
                // The decision stands once the record is written (a failed sync only means
                // the caller cannot be promised durability); a record that was never logged
                // decides abort, since recovery could never find it.
                let commit = match (&result, never_logged) {
                    (Ok(_), _) => true,
                    (Err(_), true) => false,
                    (Err(_), false) => true,
                };
                if let (Err(e), Some(c)) = (result, self.coord.get_mut(&m.seqno)) {
                    c.failed = Some(e);
                }
                if !commit {
                    self.aborted.insert(m.seqno);
                }
                for p in participants {
                    self.send(
                        p,
                        ShardMsg::Decide {
                            seqno: m.seqno,
                            commit,
                            coordinator: self.id,
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
                reserved: 0,
                tracked: false,
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
            reserved: 0,
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
            self.aborted.insert(seqno);
            for p in shards {
                self.send(
                    p,
                    ShardMsg::Decide {
                        seqno,
                        commit: false,
                        coordinator: self.id,
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
            self.aborted.insert(seqno);
            for p in shards {
                self.send(
                    p,
                    ShardMsg::Decide {
                        seqno,
                        commit: false,
                        coordinator: self.id,
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
            reserved: 0,
        });
        let _ = ctx.submitter(self.id).submit(ShardMsg::Kick);
    }

    fn on_decide(
        &mut self,
        seqno: Seqno,
        commit: bool,
        coordinator: ShardId,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        // Every decision is answered, even for a share this shard never held or already
        // dropped (a refused or failed PREPARE): the coordinator waits for every participant.
        let mut error = None;
        if let Some(share) = self.prepared.remove(&seqno) {
            if share.tracked {
                self.track_share_rows(share.bytes.batch().as_bytes(), false);
            }
            self.release_room(share.reserved);
            if commit {
                self.refresh_tablets();
                match self.apply(share.bytes.batch().as_bytes(), seqno, share.commit_ts) {
                    Ok(()) => {
                        let slots = self.touched_slots.clone();
                        for key in &slots {
                            if let Some(slot) = self.memtables.get_mut(key) {
                                slot.active.has_shares = true;
                            }
                        }
                        if slots.is_empty() {
                            // An empty share (validation only): nothing to flush.
                            self.send(coordinator, ShardMsg::ShareFlushed { seqno }, ctx);
                        } else {
                            self.unreported.push(UnreportedShare {
                                seqno,
                                coordinator,
                                slots,
                            });
                        }
                    }
                    Err(e) => {
                        // Possibly half applied: this shard extends nothing further.
                        self.poisoned = true;
                        error = Some(prepare_error_of(&e));
                    }
                }
                if self.view_dirty && self.publish_memtables().is_err() {
                    self.poisoned = true;
                    error = error.or(Some(PrepareError::Io));
                }
                if self.freeze(false).is_err() {
                    self.poisoned = true;
                }
                self.spawn_flush(ctx);
            } else {
                self.aborted.insert(seqno);
                self.advance_checkpoint(ctx);
            }
            // A conditional member deferred behind this share may run now.
            if !self.pending.is_empty() {
                let _ = ctx.submitter(self.id).submit(ShardMsg::Kick);
            }
        } else if commit {
            // Decided commit, share gone: it was never prepared here (refused) or failed.
            error = Some(PrepareError::Io);
        }
        self.send(
            coordinator,
            ShardMsg::Applied {
                seqno,
                from: self.id,
                error,
            },
            ctx,
        );
    }

    fn on_applied(
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

    // ---- checkpoints ----

    /// Whether the checkpoint may not pass `l` yet.
    fn needed(&self, l: &Logged) -> bool {
        let unflushed = |slots: &[(TabletId, FamilyId)]| {
            slots
                .iter()
                .filter(|s| !self.dropped.contains(&s.0))
                .any(|s| self.flushed.get(s).copied().unwrap_or(0) < l.seqno)
        };
        // A COMMIT this shard coordinates is unneeded once every participant's share is in
        // SSTs (decision D24); its own PREPARE (logged before it) goes with it.
        let commit_done = |seqno: Seqno| {
            self.share_reports
                .get(&seqno)
                .is_some_and(|(reported, total)| reported >= total)
        };
        match &l.kind {
            LoggedKind::Single { slots } => unflushed(slots),
            LoggedKind::Prepare { slots } => {
                !self.aborted.contains(&l.seqno)
                    && (unflushed(slots)
                        || !(self.commit_ckpt.contains(&l.seqno) || commit_done(l.seqno)))
            }
            LoggedKind::Commit { .. } => !self.aborted.contains(&l.seqno) && !commit_done(l.seqno),
        }
    }

    /// Tells coordinators about applied shares whose slots are all in SSTs now.
    fn report_shares_flushed(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        trace!(
            "shard {} report shares: flushed={:?} unreported={:?}",
            self.id.0, self.flushed, self.unreported
        );
        let mut i = 0;
        while i < self.unreported.len() {
            let u = &self.unreported[i];
            let done = u.slots.iter().all(|s| {
                self.dropped.contains(&s.0) || self.flushed.get(s).copied().unwrap_or(0) >= u.seqno
            });
            if done {
                let u = self.unreported.swap_remove(i);
                self.send(
                    u.coordinator,
                    ShardMsg::ShareFlushed { seqno: u.seqno },
                    ctx,
                );
            } else {
                i += 1;
            }
        }
    }

    /// Pops every unneeded record from the front of the log and, if the checkpoint moved,
    /// submits a `WalCheckpoint` edit (one in flight at a time).
    fn advance_checkpoint(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        while let Some(front) = self.log.front() {
            if self.needed(front) {
                break;
            }
            let l = self.log.pop_front().expect("checked");
            self.checkpoint_candidate = self.checkpoint_candidate.max(l.end);
            self.aborted.remove(&l.seqno);
            match l.kind {
                LoggedKind::Commit { participants } => {
                    self.share_reports.remove(&l.seqno);
                    self.passed_commits.push((l.seqno, participants));
                }
                LoggedKind::Prepare { .. } => {
                    self.commit_ckpt.remove(&l.seqno);
                }
                LoggedKind::Single { .. } => {}
            }
        }
        if self.log.is_empty()
            && let Some(end) = self.last_end
        {
            self.checkpoint_candidate = self.checkpoint_candidate.max(end);
        }
        // Never name bytes the kernel has not seen (a `None` record still in the buffer).
        if let Some(wal) = self.wal.as_ref() {
            self.checkpoint_candidate = self
                .checkpoint_candidate
                .min(wal.written())
                .max(self.checkpoint);
        }
        if self.checkpoint_candidate <= self.checkpoint && self.passed_commits.is_empty() {
            return;
        }
        if self.checkpoint_inflight {
            self.checkpoint_dirty = true;
            return;
        }
        if self.shared.pager_poisoned.load(Ordering::Acquire) {
            return;
        }
        self.checkpoint_inflight = true;
        self.checkpoint_dirty = false;
        let lsn = self.checkpoint_candidate;
        let commits = std::mem::take(&mut self.passed_commits);
        let submitter = ctx.submitter(self.id).clone();
        let stream = StreamId(u32::from(self.id.0));
        let req = ManifestReq::edits(vec![Edit::WalCheckpoint { stream, lsn }], move |result| {
            let _ = submitter.submit(ShardMsg::Checkpointed {
                lsn,
                commits,
                result,
            });
        });
        manifest::submit(&self.shared, self.id, req);
    }

    fn on_checkpointed(
        &mut self,
        lsn: Lsn,
        commits: Vec<(Seqno, Vec<ShardId>)>,
        result: Result<ManifestVersion>,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        self.checkpoint_inflight = false;
        match result {
            Ok(_) => {
                self.checkpoint = self.checkpoint.max(lsn);
                if let Some(wal) = self.wal.as_mut()
                    && !self.poisoned
                    && wal.checkpoint(lsn).is_err()
                {
                    self.poisoned = true;
                }
                for (seqno, participants) in commits {
                    for p in participants {
                        self.send(p, ShardMsg::CommitCheckpointed { seqno }, ctx);
                    }
                }
            }
            Err(e) => {
                // Retry at the next checkpoint event; the commits passed stay queued.
                trace!("shard {} checkpoint failed: {e}", self.id.0);
                self.passed_commits.extend(commits);
                self.checkpoint_dirty = true;
                if self.closing {
                    self.shared.close.failed.store(true, Ordering::Release);
                }
            }
        }
        if self.checkpoint_dirty && !self.shared.pager_poisoned.load(Ordering::Acquire) {
            self.checkpoint_dirty = false;
            self.advance_checkpoint(ctx);
        }
        self.try_finish_close(ctx);
    }

    // ---- compaction ----

    /// The families of the tablets this shard owns, with their SSTs in `view`.
    fn owned_slots(&self, view: &View) -> Vec<(TabletId, FamilyId)> {
        let mut out = Vec::new();
        for t in view.tablets.iter() {
            if t.shard != self.id {
                continue;
            }
            for f in view.catalog.family_ids_of(t.table) {
                out.push((t.id, f));
            }
        }
        out
    }

    /// Scores the shard's slots, refreshes the stall score and starts the most urgent
    /// compaction (or the next step of a full compaction) if none is running.
    fn maintain(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        if self.replaying {
            return;
        }
        let view = self.shared.view.load_full();
        let mut score = 0.0f64;
        let mut best: Option<(f64, (TabletId, FamilyId))> = None;
        for key in self.owned_slots(&view) {
            let Some(fam) = view.ssts.family(key.0, key.1) else {
                continue;
            };
            let Some(meta) = view.catalog.family(key.1) else {
                continue;
            };
            if meta.merge == MergeKind::Unknown {
                continue;
            }
            let s = self.picker.score(&fam.levels_meta());
            score = score.max(s);
            if s >= 1.0 && best.is_none_or(|(b, _)| s > b) {
                best = Some((s, key));
            }
        }
        trace!(
            "shard {} maintain: score={score:.2} best={best:?} compaction={:?} full_waiters={}",
            self.id.0,
            self.compaction,
            self.compact_all.len()
        );
        let was_stalled = self.stall.score >= 1.0;
        self.stall.score = score;
        if was_stalled && score < 1.0 {
            self.stall.cancel_timer();
            if !self.pending.is_empty() {
                let _ = ctx.submitter(self.id).submit(ShardMsg::Kick);
            }
        }
        if self.compaction.is_some() || self.closing || self.shared.closing.load(Ordering::Acquire)
        {
            return;
        }
        if self.shared.pager_poisoned.load(Ordering::Acquire) {
            for (_, w) in self.compact_all.drain(..) {
                w.notify(Err(poisoned_error()));
            }
            return;
        }
        // Full compactions first (a caller waits), one slot at a time.
        while let Some((filter, _)) = self.compact_all.front() {
            if !self.flush_idle() {
                self.spawn_flush(ctx);
                return;
            }
            let filter = *filter;
            let last = self.picker.options().max_levels.max(2) - 1;
            let busy: Vec<SstId> = self
                .shared
                .busy_ssts
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .copied()
                .collect();
            let mut task = None;
            for key in self.owned_slots(&view) {
                let Some(fam) = view.ssts.family(key.0, key.1) else {
                    continue;
                };
                let Some(meta) = view.catalog.family(key.1) else {
                    continue;
                };
                if filter.is_some_and(|t| t != meta.table) || meta.merge == MergeKind::Unknown {
                    continue;
                }
                if let Some(t) = compact::plan_full(key.0, key.1, &fam.levels_meta(), last, &busy) {
                    task = Some((key, t));
                    break;
                }
            }
            match task {
                Some((key, task)) => match self.start_compaction(&view, key, task, ctx) {
                    Ok(()) => return,
                    Err(e) => {
                        let (_, reply) = self.compact_all.pop_front().expect("front");
                        reply.notify(Err(e));
                    }
                },
                None => {
                    let (_, reply) = self.compact_all.pop_front().expect("front");
                    reply.notify(match self.compaction_error.take() {
                        Some(e) => Err(e),
                        None => Ok(()),
                    });
                }
            }
        }
        let Some((_, key)) = best else {
            return;
        };
        // After a failure, nothing retries until a flush or new writes clear the backoff
        // (a stall then retries it), so a dead device does not loop.
        if self.compaction_backoff {
            return;
        }
        let Some(fam) = view.ssts.family(key.0, key.1) else {
            return;
        };
        let Some(meta) = view.catalog.family(key.1) else {
            return;
        };
        let busy: Vec<SstId> = self
            .shared
            .busy_ssts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .copied()
            .collect();
        let now = self.shared.vfs.now_micros();
        let Some(task) = self.picker.pick(
            key.0,
            key.1,
            &fam.levels_meta(),
            &busy,
            now,
            meta.options.ttl_micros,
        ) else {
            return;
        };
        let _ = self.start_compaction(&view, key, task, ctx);
    }

    fn start_compaction(
        &mut self,
        view: &Arc<View>,
        key: (TabletId, FamilyId),
        mut task: pigeonhole_compaction::CompactionTask,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) -> Result<()> {
        let tablet = view
            .tablets
            .entry(key.0)
            .cloned()
            .ok_or_else(|| Error::TableNotFound(format!("tablet {}", key.0.0)))?;
        let fam = view
            .ssts
            .family(key.0, key.1)
            .ok_or_else(|| Error::Corruption("no SSTs".to_owned()))?;
        let meta = view
            .catalog
            .family(key.1)
            .cloned()
            .ok_or_else(|| Error::FamilyNotFound(format!("family {}", key.1.0)))?;
        compact::narrow(&mut task, &tablet, &view.catalog)?;
        trace!(
            "shard {} compaction start {:?}: inputs {:?} -> level {} ({:?})",
            self.id.0, key, task.inputs, task.output_level, task.kind
        );
        let mem_min_ts = self.memtables.get(&key).map_or(u64::MAX, MemSlot::min_ts);
        let now = self.shared.vfs.now_micros();
        let gc = compact::gc_policy(&self.shared, fam, &task, mem_min_ts, now);
        let record =
            (task.kind == pigeonhole_compaction::TaskKind::Rewrite).then(|| CompactionRecord {
                manifest_version: 0,
                table: meta.table,
                tablet: key.0,
                family: key.1,
                bottommost: gc.bottommost,
                snapshots: gc.snapshots.clone(),
                now: gc.now,
                min_ts_above: gc.min_ts_above,
                max_seqno: compact::max_input_seqno(
                    fam,
                    &task,
                    self.memtables.get(&key).and_then(MemSlot::min_seqno),
                    self.shared.shm.visible_seqno(),
                ),
                rows: (
                    (!tablet.start.is_empty()).then(|| tablet.start.clone()),
                    tablet.end.clone(),
                ),
            });
        // Claim the inputs under one lock: a shrink may have claimed one since the plan
        // was made against the busy set (then this round is skipped; `maintain` retries).
        let ids: Vec<SstId> = task
            .inputs
            .iter()
            .flat_map(|(_, ids)| ids.iter().copied())
            .collect();
        {
            let mut busy = self
                .shared
                .busy_ssts
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if ids.iter().any(|id| busy.contains(id)) {
                return Ok(());
            }
            busy.extend(ids.iter().copied());
        }
        let work = match CompactionWork::new(
            Arc::clone(&self.shared),
            self.id,
            Arc::clone(view),
            fam,
            meta,
            task,
            gc,
            record,
        ) {
            Ok(w) => w,
            Err(e) => {
                let mut busy = self
                    .shared
                    .busy_ssts
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                for id in &ids {
                    busy.remove(id);
                }
                return Err(e);
            }
        };
        self.compaction = Some(key);
        ctx.spawn(Box::new(work));
        Ok(())
    }

    fn on_compaction_done(
        &mut self,
        inputs: Vec<SstId>,
        result: Result<ManifestVersion>,
        nanos: u64,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        {
            let mut busy = self
                .shared
                .busy_ssts
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            for id in &inputs {
                busy.remove(id);
            }
        }
        self.compaction = None;
        trace!(
            "shard {} compaction done: {:?} ({nanos} ns)",
            self.id.0,
            result.as_ref().map(|_| ())
        );
        match result {
            Ok(_) => {
                let metrics = &self.shared.metrics[usize::from(self.id.0)];
                metrics.compactions.fetch_add(1, Ordering::Relaxed);
                metrics.compaction_nanos.fetch_add(nanos, Ordering::Relaxed);
                // Compaction progress refills the bucket as time does: a stall paces writers
                // even on a clock that does not move.
                self.stall.tokens = (self.stall.tokens + 1.0).min(STALL_CAPACITY);
            }
            // The table was dropped while this compaction ran: its output was abandoned and
            // its inputs are retired with the table. Nothing failed; a full compaction goes
            // on with the remaining tables.
            Err(Error::TableNotFound(_)) => {}
            Err(e) => {
                // A full compaction reports the failure to its caller; a background one
                // waits for the next trigger (a flush or new writes) rather than retrying
                // in a loop against a device that keeps failing.
                match self.compact_all.pop_front() {
                    Some((_, reply)) => reply.notify(Err(e)),
                    None => self.compaction_error = Some(e),
                }
                self.compaction_backoff = true;
            }
        }
        // A stalled group waits on this compaction: it runs again whatever the outcome.
        if !self.pending.is_empty() {
            let _ = ctx.submitter(self.id).submit(ShardMsg::Kick);
        }
        self.maintain(ctx);
    }

    // ---- close ----

    fn try_finish_close(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        if !self.closing || self.close_stage == CloseStage::Reported {
            return;
        }
        trace!(
            "shard {} close: stage {:?} unresolved={} coord={} prepared={} pending={} flush_idle={} \
             flush_running={} queue={} frozen={} to_freeze={:?} deferred={} log={} ckpt_inflight={} \
             dirty={} failed={} unreported={} share_reports={:?} commit_ckpt={:?} aborted={:?}",
            self.id.0,
            self.close_stage,
            self.unresolved.len(),
            self.coord.len(),
            self.prepared.len(),
            self.pending.len(),
            self.flush_idle(),
            self.flush_running,
            self.flush_queue.len(),
            self.memtables
                .values()
                .map(|s| s.frozen.len())
                .sum::<usize>(),
            self.to_freeze,
            self.freeze_deferred,
            self.log.len(),
            self.checkpoint_inflight,
            self.checkpoint_dirty,
            self.flush_failed,
            self.unreported.len(),
            self.share_reports,
            self.commit_ckpt,
            self.aborted,
        );
        if tracing() {
            for l in &self.log {
                eprintln!(
                    "  shard {} log: seqno {} {:?} needed={}",
                    self.id.0,
                    l.seqno,
                    l.kind,
                    self.needed(l)
                );
            }
        }
        if !self.unresolved.is_empty()
            || !self.coord.is_empty()
            || !self.prepared.is_empty()
            || !self.pending.is_empty()
        {
            return;
        }
        if self.close_stage == CloseStage::Draining {
            self.close_stage = CloseStage::Flushing;
            // Abandon the stall: nothing is admitted any more.
            self.stall.score = 0.0;
            self.stall.cancel_timer();
            if self.freeze(true).is_err() {
                self.shared.close.failed.store(true, Ordering::Release);
            }
            self.spawn_flush(ctx);
        }
        if self.close_stage == CloseStage::Flushing
            && !self.flush_running
            && (self.flush_failed || self.shared.pager_poisoned.load(Ordering::Acquire))
        {
            self.flush_failed = true;
            self.shared.close.failed.store(true, Ordering::Release);
            // Give up on flushing: the WAL keeps everything, the next open replays it, and
            // the close is reported unclean.
            self.close_stage = CloseStage::Checkpointing;
            self.checkpoint_inflight = false;
        }
        if self.close_stage == CloseStage::Flushing {
            if self.freeze_all_pending || self.freeze_deferred {
                // A freeze waited for visibility; try again (the watermark moves once the
                // other shards finish their groups, and they kick us).
                if self.freeze(true).is_err() {
                    self.shared.close.failed.store(true, Ordering::Release);
                }
                self.spawn_flush(ctx);
            }
            if !self.flush_idle() || self.freeze_all_pending || !self.to_freeze.is_empty() {
                return;
            }
            // Buffered `None` records reach the file now, so the final checkpoint can name
            // the end of the stream.
            if let Some(wal) = self.wal.as_mut()
                && !self.poisoned
                && wal.write().is_err()
            {
                self.poisoned = true;
            }
            self.report_shares_flushed(ctx);
            self.advance_checkpoint(ctx);
            if !self.log.is_empty() || self.checkpoint_inflight || self.checkpoint_dirty {
                // Waiting for the other shards' flushes and checkpoints (ShareFlushed and
                // CommitCheckpointed arrive as messages), or for our own edit.
                return;
            }
            self.close_stage = CloseStage::Checkpointing;
        }
        if self.close_stage == CloseStage::Checkpointing {
            if self.checkpoint_inflight {
                return;
            }
            self.close_stage = CloseStage::Reported;
            self.final_sync();
            // The stream is checkpointed to its end: the last process removes the files.
            self.wal = None;
            self.shared.report_closed();
        }
    }

    /// The final sync of the stream at close; a failure (or an earlier poisoning) makes the
    /// close unclean.
    fn final_sync(&mut self) {
        let failed = match self.wal.as_mut() {
            Some(wal) if !self.poisoned => wal.sync().is_err(),
            Some(_) => true,
            None => false,
        };
        trace!(
            "shard {} final sync failed={failed} poisoned={}",
            self.id.0, self.poisoned
        );
        if failed {
            self.shared.close.failed.store(true, Ordering::Release);
        }
    }

    /// Finishes a shard whose driver is dropped before the close handshake completed: the
    /// stream is synced and the close reported, so `Engine::close` never waits forever. The
    /// WAL files stay (the close is not clean).
    pub(crate) fn abandon(&mut self, shared: &Shared) {
        if self.close_stage == CloseStage::Reported {
            return;
        }
        self.closing = true;
        self.close_stage = CloseStage::Reported;
        self.final_sync();
        shared.close.failed.store(true, Ordering::Release);
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

    fn on_sync_barrier(
        &mut self,
        reply: Notifier<Result<()>>,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        let _ = ctx;
        trace!(
            "shard {} barrier: poisoned={} wal={}",
            self.id.0,
            self.poisoned,
            self.wal.is_some()
        );
        if self.poisoned {
            reply.notify(Err(poisoned_error()));
            return;
        }
        match self.wal.as_mut() {
            Some(wal) => match wal.submit_sync() {
                Ok(c) => {
                    drop(c.map(move |r| {
                        reply.notify(r.map(|_| ()).map_err(Error::from));
                        Ok(())
                    }));
                }
                Err(e) => {
                    self.poisoned = true;
                    reply.notify(Err(e.into()));
                }
            },
            // Closed cleanly: the stream was synced before it was dropped.
            None if self.close_stage == CloseStage::Reported
                && !self.shared.close.failed.load(Ordering::Acquire) =>
            {
                reply.notify(Ok(()))
            }
            None => reply.notify(Err(Error::Unsupported("no WAL stream"))),
        }
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
            ShardMsg::Decide {
                seqno,
                commit,
                coordinator,
            } => self.on_decide(seqno, commit, coordinator, ctx),
            ShardMsg::Applied { seqno, from, error } => self.on_applied(seqno, from, error, ctx),
            ShardMsg::SyncDone { group, result } => self.resolve_through(group, result, ctx),
            ShardMsg::FlushAll { reply } => {
                if let Err(e) = self.freeze(true) {
                    reply.notify(Err(e));
                    return;
                }
                self.flush_waiters.push(reply);
                self.spawn_flush(ctx);
                self.check_flush_waiters();
            }
            ShardMsg::Flushed {
                items,
                result,
                nanos,
            } => self.on_flushed(items, result, nanos, ctx),
            ShardMsg::SyncBarrier { reply } => self.on_sync_barrier(reply, ctx),
            ShardMsg::ShareFlushed { seqno } => {
                // The COMMIT record may not be logged yet (a participant flushed before the
                // coordinator's group ran): the count waits for it.
                self.share_reports.entry(seqno).or_insert((0, usize::MAX)).0 += 1;
                self.advance_checkpoint(ctx);
                self.try_finish_close(ctx);
            }
            ShardMsg::CommitCheckpointed { seqno } => {
                self.commit_ckpt.insert(seqno);
                self.advance_checkpoint(ctx);
                self.try_finish_close(ctx);
            }
            ShardMsg::Checkpointed {
                lsn,
                commits,
                result,
            } => self.on_checkpointed(lsn, commits, result, ctx),
            ShardMsg::Maintain => {
                self.maintain(ctx);
                if self.freeze_deferred || !self.to_freeze.is_empty() {
                    let _ = self.freeze(false);
                    self.spawn_flush(ctx);
                }
                if !self.flush_queue.is_empty() {
                    self.spawn_flush(ctx);
                }
                self.reclaim_retired();
            }
            ShardMsg::CompactionDone {
                inputs,
                result,
                nanos,
            } => self.on_compaction_done(inputs, result, nanos, ctx),
            ShardMsg::CompactAll { table, reply } => {
                if self.closing {
                    reply.notify(Err(Error::Closed));
                    return;
                }
                if let Err(e) = self.freeze(true) {
                    reply.notify(Err(e));
                    return;
                }
                self.compact_all.push_back((table, reply));
                self.spawn_flush(ctx);
                self.maintain(ctx);
            }
            ShardMsg::PumpManifest => {
                ctx.spawn(Box::new(ManifestPump::new(Arc::clone(&self.shared))));
            }
            ShardMsg::DropTablets { tablets } => {
                if self.drop_tablets(&tablets).is_err() {
                    self.poisoned = true;
                }
            }
            ShardMsg::Start => {
                self.replaying = false;
                self.maybe_prepare_spares(ctx);
                // Replayed shares with nothing left to flush report at once.
                self.report_shares_flushed(ctx);
                self.advance_checkpoint(ctx);
                self.spawn_flush(ctx);
                self.maintain(ctx);
            }
            ShardMsg::Kick => {}
            ShardMsg::Close => {
                if !self.closing {
                    self.closing = true;
                    self.close_stage = CloseStage::Draining;
                    self.shared.closing.store(true, Ordering::Release);
                }
                self.try_finish_close(ctx);
            }
        }
    }
}

/// Whether `PIGEONHOLE_TRACE` is set: the close and checkpoint protocol logs its steps.
pub(crate) fn tracing() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("PIGEONHOLE_TRACE").is_some())
}

macro_rules! trace {
    ($($arg:tt)*) => {
        if $crate::shard::tracing() {
            eprintln!($($arg)*);
        }
    };
}
pub(crate) use trace;

/// Why a member could not reserve arena room.
enum Room {
    /// It can never fit, even in an empty arena.
    Never,
    /// A flush may free enough.
    Wait,
}

/// A replayed record, for the checkpoint log.
#[derive(Debug)]
pub(crate) enum ReplayedKind {
    Single {
        slots: Vec<(TabletId, FamilyId)>,
    },
    Prepare {
        slots: Vec<(TabletId, FamilyId)>,
        coordinator: ShardId,
        applied: bool,
    },
    Commit {
        participants: Vec<ShardId>,
        complete: bool,
    },
}

impl ShardHandler for ShardState {
    type Msg = ShardMsg;

    fn handle(&mut self, ctx: &mut ShardContext<'_, Self::Msg>, msg: Self::Msg) {
        self.handle_msg(msg, ctx);
    }

    fn end_batch(&mut self, ctx: &mut ShardContext<'_, Self::Msg>) {
        self.run_group(ctx);
        if (self.freeze_all_pending
            || (!self.to_freeze.is_empty() && self.freeze_deferred)
            || self.wait_room)
            && self.freeze(false).is_ok()
        {
            self.spawn_flush(ctx);
            self.check_flush_waiters();
        }
        self.try_finish_close(ctx);
    }
}
