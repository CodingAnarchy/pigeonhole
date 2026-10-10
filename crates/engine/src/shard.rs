//! The per-shard state and the shard loop body: the group commit on the shard's WAL stream,
//! memtable application with same-commit collapse (D34), the two-phase commit protocol for
//! cross-shard batches, freezing and flushing, WAL checkpoints (D24), compaction scheduling,
//! write stalls, and the close handshake.
//!
//! One `ShardState` per shard, owned by its thread (or its `EngineShard` driver). Shards
//! share nothing mutable except the documented queues, the shared-memory watermarks and the
//! engine-wide [`Shared`] state (lock-free view publication behind a publish mutex, the
//! manifest queue, and the registries flush and compaction consult).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Waker;

use arc_swap::ArcSwap;
use pigeonhole_cache::BlockCache;
use pigeonhole_compaction::{CompactionPicker, MergingCursor, PickerOptions, ResolveOptions};
use pigeonhole_format::hash::{FastBuildHasher, FastMap, FastSet};
use pigeonhole_format::key::{
    compare, encode_key_after_row, encode_marker_after_row, encode_row_prefix, split_suffix,
};
use pigeonhole_format::manifest::{CompactionStyle, Edit, FamilyKind};
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

mod tablets;

use tablets::{ActiveOp, TabletLoad};
pub(crate) use tablets::{BalanceConfig, LoadSlot, TabletOpKind, split_by_shard};

use crate::catalog::{Catalog, MergeKind};
use crate::compact::{self, CompactionRecord, CompactionWork};
use crate::flush::{FlushItem, FlushTask, FlushedItem};
use crate::manifest::{self, ManifestPump, ManifestQueue, ManifestReq, ManifestWriter};
use crate::snapshot::{LiveSeqnos, LiveViews, MemSet, ShardMems, TabletMap, View};
use crate::source::{ColumnKey, Probe, Resolver, Source, mem_sources_from, sst_sources_point};
use crate::write::ReadKey;
use crate::{CommitInfo, Error, Predicate, Result};

/// Worst-case memtable overhead per entry: node header and a full tower, plus alignment.
const ENTRY_OVERHEAD: usize = 12 + 4 * 16 + 8;
/// Fixed bytes of an internal key beyond the escaped row and qualifier.
const KEY_FIXED: usize = 2 + 2 + 17;
/// Token-bucket capacity (groups) and refill rate at an L0 score of 1 (groups per second).
const STALL_CAPACITY: f64 = 8.0;
/// Bytes a logged record counts for on top of its payload (framing and record header),
/// for the WAL pin limit (#137).
const LOGGED_OVERHEAD: u64 = 64;
const STALL_RATE: f64 = 4000.0;
/// Polls in a row that see the clock unchanged before a stall timer gives up: the clock is
/// frozen (the simulator) or coarse, and the stall ends on a background event instead.
const STALL_TIMER_FROZEN_POLLS: u32 = 1024;
/// Failed flushes in a row after which a wait for arena room ends with `Busy` on a frozen
/// clock (a moving one waits for `write_stall_timeout_nanos`).
const ROOM_FLUSH_ATTEMPTS: u32 = 4;
/// A backoff that starts at `base` and doubles with each failure in a row (`failures`, at
/// least one), up to `cap` times `base`. The bases are `EngineOptions` fields: a failed
/// compaction's (1 s, capped at 60 s), a failed flush's (10 ms, capped at 1 s) and a room
/// wait's re-check (1 ms, capped at 100 ms).
fn backoff_nanos(base: u64, cap: u64, failures: u32) -> u64 {
    // 2^32 times the base is far past any cap; saturate rather than overflow.
    let doublings = failures.saturating_sub(1).min(32);
    base.saturating_mul(1 << doublings)
        .min(base.saturating_mul(cap))
        .max(base)
}

/// Caps of the backoffs, as multiples of their bases.
const COMPACTION_BACKOFF_CAP: u64 = 60;
const FLUSH_BACKOFF_CAP: u64 = 100;
const ROOM_RECHECK_CAP: u64 = 100;

/// The wait before retrying after `failures` failed compactions in a row (at least one).
/// Attempts of a cross-shard commit refused with `Moved` before it fails with `Busy`
/// (#102): a commit that keeps meeting tablet changes gives up rather than retrying for ever.
pub(crate) const MOVED_RETRIES: u32 = 16;

/// Whether a cross-shard commit refused with `Moved` for the `attempts`-th time, `waited`
/// nanoseconds after it was submitted, fails with `Busy` instead of retrying.
fn moved_gives_up(attempts: u32, waited: u64, timeout: u64) -> bool {
    attempts >= MOVED_RETRIES || waited >= timeout
}

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
    /// Flushes and background compactions that failed (issue #141: they are retried on a
    /// backoff, and nothing else reports them).
    pub flush_failures: AtomicU64,
    pub compaction_failures: AtomicU64,
    pub splits: AtomicU64,
    pub merges: AtomicU64,
    pub moves: AtomicU64,
    /// WAL unpin passes (#137) and the memtables they froze below the size threshold.
    pub unpin_passes: AtomicU64,
    pub unpin_flushes: AtomicU64,
    /// Tablet changes refused (or a due size split skipped) for lack of memtable slots, and
    /// splits refused because the tablet map would outgrow the shared-memory view (D28).
    pub refused_slots: AtomicU64,
    pub refused_view: AtomicU64,
    /// The shard's WAL stream stall counters (ICR 0015), once the stream is open.
    pub wal: std::sync::OnceLock<Arc<pigeonhole_wal::WalCounters>>,
    /// Test-hook counters (`engine::hooks::ShardCounters`).
    #[cfg(feature = "test-hooks")]
    pub hooks: crate::engine::hooks::ShardCounters,
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
            flush_failures: AtomicU64::new(0),
            compaction_failures: AtomicU64::new(0),
            splits: AtomicU64::new(0),
            merges: AtomicU64::new(0),
            moves: AtomicU64::new(0),
            unpin_passes: AtomicU64::new(0),
            unpin_flushes: AtomicU64::new(0),
            refused_slots: AtomicU64::new(0),
            refused_view: AtomicU64::new(0),
            wal: std::sync::OnceLock::new(),
            #[cfg(feature = "test-hooks")]
            hooks: Default::default(),
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
    /// The final close's outcome once it ran (`EngineShard::closed`).
    pub outcome: Mutex<Option<Result<()>>>,
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

/// Shards waiting for the watermark to pass a deferred freeze, each with the seqno its
/// freeze needs visible. A publisher wakes (and forgets) only the shards whose seqno is
/// visible, so a watermark that did not move far enough wakes nobody, and a shard that
/// defers again registers again.
#[derive(Debug)]
pub(crate) struct FreezeWaiters {
    /// The smallest seqno any registered shard waits for (`u64::MAX` when none): a publish
    /// below it skips the lock.
    min_needed: AtomicU64,
    list: Mutex<Vec<(u16, Seqno)>>,
}

impl Default for FreezeWaiters {
    fn default() -> Self {
        Self {
            min_needed: AtomicU64::new(u64::MAX),
            list: Mutex::new(Vec::new()),
        }
    }
}

impl FreezeWaiters {
    /// Registers `shard` as waiting until `needed` is visible (replacing an earlier
    /// registration). The caller re-reads the visible seqno afterwards: a watermark
    /// published before the registration landed may have missed it.
    pub(crate) fn register(&self, shard: u16, needed: Seqno) {
        let mut list = self.list.lock().unwrap_or_else(PoisonError::into_inner);
        match list.iter_mut().find(|(s, _)| *s == shard) {
            Some(entry) => entry.1 = needed,
            None => list.push((shard, needed)),
        }
        let min = list.iter().map(|(_, n)| *n).min().unwrap_or(u64::MAX);
        self.min_needed.store(min, Ordering::SeqCst);
        drop(list);
        // Pairs with the fence in `take_ready`: either the publisher sees this registration
        // or the caller's re-read sees the publisher's watermark.
        std::sync::atomic::fence(Ordering::SeqCst);
    }

    /// Takes the shards whose needed seqno is at or below what `visible` reads (one load and
    /// no lock when none is ready).
    pub(crate) fn take_ready(&self, visible: impl Fn() -> Seqno) -> Vec<u16> {
        std::sync::atomic::fence(Ordering::SeqCst);
        let min = self.min_needed.load(Ordering::SeqCst);
        if min == u64::MAX {
            return Vec::new();
        }
        let visible = visible();
        if visible < min {
            return Vec::new();
        }
        let mut list = self.list.lock().unwrap_or_else(PoisonError::into_inner);
        let mut ready = Vec::new();
        list.retain(|&(s, needed)| {
            let keep = needed > visible;
            if !keep {
                ready.push(s);
            }
            keep
        });
        let min = list.iter().map(|(_, n)| *n).min().unwrap_or(u64::MAX);
        self.min_needed.store(min, Ordering::SeqCst);
        ready
    }
}

/// Engine-wide state every shard and every caller shares.
pub(crate) struct Shared {
    pub vfs: VfsRef,
    pub shm: ShmRegion,
    pub shards: usize,
    pub view: ArcSwap<View>,
    /// Reads inside async calls that went to the file synchronously (D196; `Metrics`).
    pub async_sync_reads: AtomicU64,
    /// Serializes view publishers (shards creating memtables, manifest commits) and holds
    /// the last published version.
    pub view_lock: Mutex<u64>,
    pub manifest: Mutex<ManifestWriter>,
    pub manifest_queue: ManifestQueue,
    /// Held by whoever is committing a manifest batch.
    pub manifest_busy: AtomicBool,
    pub pager: Arc<Pager>,
    pub cache: Arc<BlockCache>,
    /// The row cache (D201): writer process only, `None` when off.
    pub row_cache: Option<crate::row_cache::RowCaches>,
    pub sst_ids: Arc<AtomicU64>,
    pub blob_ids: Arc<AtomicU32>,
    pub live_views: Arc<LiveViews>,
    pub live_seqnos: Arc<LiveSeqnos>,
    /// Memtables `(shard, root)` whose SSTs are in the manifest, excluded from every view
    /// until their shard retires them. Extended only together with storing the view that
    /// adds their SSTs (`publish_view`): a shard building its memtable piece (under
    /// `view_lock`) or reading its memtables beside the view's SSTs (loading the view while
    /// holding this lock) sees both or neither.
    pub flushed_roots: Mutex<HashSet<(u16, u32)>>,
    /// SSTs a running compaction or relocation reads or replaces.
    pub busy_ssts: Mutex<HashSet<SstId>>,
    /// Blob files of values separated at commit time whose commit has not settled yet, or
    /// whose release (`DropBlobFile`) has not committed yet (#230): published, but nothing
    /// may point into them yet.
    pub large_pending: Mutex<HashSet<pigeonhole_format::BlobFileId>>,
    /// How many `LargeValues` guards are alive: while it is zero, shards skip looking for
    /// pointers in the batches they log.
    pub large_open: AtomicUsize,
    /// Blob files of values separated at commit time whose batch reached its commit point:
    /// its record was handed to the WAL (single-shard batch, or a cross-shard PREPARE). From
    /// there the batch may be applied, or replayed, so its files are never released (#230).
    pub large_logged: Mutex<HashSet<pigeonhole_format::BlobFileId>>,
    /// Blob files of values separated at commit time that the same-commit collapse (D34)
    /// dropped at apply, for the commit's settle hook to release (#230).
    pub large_dropped: Mutex<HashSet<pigeonhole_format::BlobFileId>>,
    /// Published view version -> manifest version, to map reader-slot pins to extents.
    pub view_versions: Mutex<BTreeMap<u64, ManifestVersion>>,
    /// Test hooks (`engine::hooks`); none of them is set or read unless a test asks.
    #[cfg(feature = "test-hooks")]
    pub hooks: crate::engine::hooks::Hooks,
    pub picker: PickerOptions,
    /// How long a commit waits for arena room before `Busy`.
    pub write_stall_timeout_nanos: u64,
    /// Backoff bases (`EngineOptions`; issue #141).
    pub compaction_backoff_nanos: u64,
    pub flush_backoff_nanos: u64,
    pub room_recheck_nanos: u64,
    /// `EngineOptions::commit_spin_nanos` (D198).
    pub commit_spin_nanos: u64,
    pub locks: Mutex<Option<Locks>>,
    pub default_durability: AtomicU8,
    pub closed: AtomicBool,
    /// `Engine::close` has started: background compaction stops.
    pub closing: AtomicBool,
    /// Application threads inside `shrink`, `backup` or a catalog change. The final close
    /// waits for them (the last one out runs it): it marks the file clean and releases the
    /// writer lock, after which nothing may commit or truncate (7 F7-4).
    pub maintenance: AtomicUsize,
    /// A manifest (root) commit failed: the pager is poisoned (decision D58) and every later
    /// write fails until the database is reopened.
    pub pager_poisoned: AtomicBool,
    pub close: CloseState,
    pub metrics: Vec<ShardMetrics>,
    /// Per-shard largest default timestamp assigned (decision D11), read at manifest commits
    /// and by coordinators choosing a cross-shard commit timestamp.
    pub ts_floors: Vec<Padded>,
    /// Per-shard floor raises from shards handing it tablets (D11: the floor travels with
    /// the tablet); the shard's next default timestamp is above it.
    pub ts_raises: Vec<Padded>,
    /// Per shard, the shards whose floor set its current raise (a bit per shard below 64),
    /// with that raise: a retried commit may tie a raise only its own shards made.
    pub ts_raisers: Vec<Mutex<(Timestamp, u64)>>,
    /// Per-shard load as of its last balancer interval.
    pub loads: Vec<LoadSlot>,
    pub balance: BalanceConfig,
    /// Bytes of a shared-memory view buffer (a split that would outgrow it is refused, D28).
    pub view_capacity: usize,
    /// Bumped whenever a tablet change finishes or is given up (see `CoordinateReq::epoch`).
    pub tablet_epoch: AtomicU64,
    /// Full compactions (`Engine::compact`) in progress: the balancer starts no change
    /// meanwhile, so a tablet does not move past the shards' rounds (issue #94).
    pub full_compactions: AtomicUsize,
    pub waiters: VisibilityWaiters,
    /// A shard died (`Shared::shard_died`).
    pub shard_died: AtomicBool,
    /// The manifest pump's root commit in flight, which a blocked thread may finish.
    pub manifest_flight: manifest::Flight,
    /// Which thread drives each application-owned shard (empty in engine-owned mode).
    pub drivers: crate::waker::Drivers,
    /// Shards with a freeze deferred until the watermark passes their memtable (kicked by
    /// whoever publishes a watermark).
    pub freeze_waiters: FreezeWaiters,
    pub memtable_freeze_bytes: u64,
    /// Bytes a stream may hold past its oldest needed record before the slots pinning it
    /// are flushed (`EngineOptions::wal_pin_bytes`, resolved; #137).
    pub wal_pin_bytes: u64,
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

    /// Marks the close unclean. The first time, every shard is kicked: one waiting at close
    /// for its peers' reports (`ShareFlushed`, `CommitCheckpointed`) stops waiting, since a
    /// shard that gave up will never send them (5-6 5.1).
    pub(crate) fn fail_close(&self) {
        if !self.close.failed.swap(true, Ordering::AcqRel) {
            self.broadcast(|| ShardMsg::Kick);
        }
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
    /// (the caller then proceeds without waiting). A waker registered again for the same
    /// seqno (a spurious wake) is not listed twice.
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
            if !list.iter().any(|(s, w)| *s == seqno && w.will_wake(waker)) {
                list.push((seqno, waker.clone()));
            }
            self.waiters.count.store(list.len(), Ordering::Release);
        }
        // The shards check the count after each watermark publish (behind a SeqCst fence,
        // `FreezeWaiters::take_ready`); this fence orders our count store before the second
        // look, so either the publisher sees us registered or we see its watermark.
        std::sync::atomic::fence(Ordering::SeqCst);
        self.shm.visible_seqno() >= seqno
    }

    /// Whether visibility can no longer advance: the close finished, or a shard died (its
    /// thread panicked, or its `EngineShard` was dropped, which also fails the close). A
    /// visibility wait then ends with `Closed`.
    pub(crate) fn visibility_ended(&self) -> bool {
        self.closed.load(Ordering::SeqCst) || self.close.failed.load(Ordering::SeqCst)
    }

    /// Wakes every visibility waiter whatever its seqno, after `visibility_ended` became
    /// true: each re-checks and ends.
    pub(crate) fn wake_all_visible(&self) {
        let woken = {
            let mut list = self
                .waiters
                .list
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            self.waiters.count.store(0, Ordering::Release);
            std::mem::take(&mut *list)
        };
        for (_, w) in woken {
            w.wake();
        }
    }

    /// A shard died (its thread panicked, or its `EngineShard` was dropped before it
    /// closed): its pending watermark never moves again. The close becomes unclean, waits
    /// for visibility end, and the other shards are kicked so a close in progress stops
    /// waiting on it (they give up flushing).
    pub(crate) fn shard_died(&self) {
        self.shard_died.store(true, Ordering::SeqCst);
        self.fail_close();
        self.wake_all_visible();
        // `fail_close` kicks only the first time the close fails: kick again, the death is news.
        self.broadcast(|| ShardMsg::Kick);
    }

    /// In application-owned mode, refuses a call that submits work and blocks for it when
    /// the calling thread drives a shard (it could deadlock): nothing is submitted (D88).
    pub(crate) fn refuse_blocking_on_driver(&self, what: &str) -> Result<()> {
        if self.drivers.current_drives() {
            return Err(Error::InvalidArgument(format!(
                "{what} blocks, and this thread drives a shard (application-owned mode), so \
                 it could deadlock; nothing was submitted: submit and await the future from \
                 the event loop, or call it from another thread"
            )));
        }
        Ok(())
    }

    /// Wakes every registered waiter whose seqno is visible now. Called by shards after a
    /// watermark publish, and only when someone is registered (one relaxed load otherwise).
    pub(crate) fn wake_visible(&self) {
        for s in self.freeze_waiters.take_ready(|| self.shm.visible_seqno()) {
            let _ = self.submitter(ShardId(s)).submit(ShardMsg::Kick);
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
    ///
    /// `flushed` names the memtables `(shard, root)` whose SSTs the new view adds: they join
    /// `flushed_roots` under that lock and in the same step as the view is stored, so no
    /// view, and no shard reading its own memtables beside the view's SSTs, ever sees a
    /// memtable together with its SST (issue #131: a merge operand counted twice).
    pub(crate) fn publish_view(
        &self,
        flushed: &[(u16, u32)],
        f: impl FnOnce(&View, u64) -> View,
    ) -> Result<Arc<View>> {
        #[cfg(feature = "test-hooks")]
        self.hooks.before_view_publish.run();
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
        {
            let mut roots = self
                .flushed_roots
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            roots.extend(flushed.iter().copied());
            self.view.store(Arc::clone(&view));
        }
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

    /// As [`Shared::reclaim`], skipped if the pager is busy (it may be mid-`fsync`): for the
    /// reclaim when an old view goes, on whatever thread drops it (a user's snapshot, a
    /// scan's end, a shard). The busy holder's commit or shrink reclaims anyway.
    pub(crate) fn try_reclaim(&self) {
        let _ = self.pager.try_reclaim(self.oldest_live_manifest());
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

    /// Runs the final close if it is pending, no application thread is inside maintenance
    /// (`MaintenanceGuard`), and the manifest writer's exclusion is free.
    /// A background commit (a compaction's, say) may hold it with its root commit in
    /// flight; the final close's own commits must not interleave with it, or both would be
    /// prepared from the same writer state (issue #78). Its holder calls this again from
    /// `manifest::release`. Never blocks, so it is safe on the shard thread that runs the
    /// holder's pump.
    pub(crate) fn try_final_close(&self) {
        // Pairs with the fence in `manifest::release`.
        std::sync::atomic::fence(Ordering::SeqCst);
        if !self.close.final_pending.load(Ordering::SeqCst)
            || self.maintenance.load(Ordering::SeqCst) > 0
            || !manifest::claim(self)
        {
            return;
        }
        if !self.close.final_pending.swap(false, Ordering::SeqCst) {
            manifest::release(self);
            return;
        }
        let result = self.final_close();
        *self
            .close
            .outcome
            .lock()
            .unwrap_or_else(PoisonError::into_inner) =
            Some(result.as_ref().copied().map_err(Error::duplicate));
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
        // Application-owned shards that finished their part are idle, and their drivers
        // sleep until the wakeup: wake them to see `EngineShard::closed`. Visibility waits
        // still pending now never complete: end them.
        self.broadcast(|| ShardMsg::Kick);
        self.wake_all_visible();
    }
}

impl Drop for ShardState {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.report_panicked();
        }
    }
}

/// An application thread inside `shrink`, `backup` or a catalog change (`Shared::maintenance`).
/// Dropping it runs the final close if that waited for it.
pub(crate) struct MaintenanceGuard<'a>(&'a Shared);

impl<'a> MaintenanceGuard<'a> {
    /// Enters maintenance; `check_open` runs after the count is raised, so either it sees
    /// the close or the final close sees the count.
    pub(crate) fn enter(
        shared: &'a Shared,
        check_open: impl FnOnce() -> Result<()>,
    ) -> Result<Self> {
        shared.maintenance.fetch_add(1, Ordering::SeqCst);
        let guard = Self(shared);
        std::sync::atomic::fence(Ordering::SeqCst);
        check_open()?;
        Ok(guard)
    }
}

impl Drop for MaintenanceGuard<'_> {
    fn drop(&mut self) {
        // Pairs with the fence in `Shared::try_final_close`: either it sees the count, or
        // this sees its pending flag.
        self.0.maintenance.fetch_sub(1, Ordering::SeqCst);
        self.0.try_final_close();
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
/// A `check_and_mutate`'s table, row and predicate.
pub(crate) type RowPredicate = Box<(TableId, Vec<u8>, Predicate)>;

#[derive(Debug)]
pub(crate) struct CommitReq {
    /// The encoded batch, moved out of the `WriteBatch` (never re-encoded).
    pub bytes: BatchBuilder,
    pub durability: Durability,
    pub reply: Reply,
    pub submitted_at: u64,
    /// Optimistic validation: the snapshot seqno and the reads to check.
    pub validate: Option<(Seqno, Vec<ReadKey>)>,
    /// `check_and_mutate`: the row and predicate to test first. Boxed: rare, and the
    /// largest part of the request, which every queued message is sized for.
    pub predicate: Option<RowPredicate>,
    /// A retried cross-shard commit's first timestamp, kept when still above the floor.
    pub commit_ts: Option<Preset>,
    /// Version of the tablet map the commit was routed with (0: unknown): a shard that
    /// handed a tablet away checks the routing of commits routed with an older map.
    pub map_version: u64,
    /// Times a participant refused this (cross-shard) commit with `Moved` so far: past
    /// `MOVED_RETRIES` it fails with `Busy` (#102).
    pub attempts: u32,
}

/// A refused commit's first timestamp, carried into its retry.
#[derive(Debug, Clone)]
pub(crate) struct Preset {
    pub ts: Timestamp,
    /// Shards whose floor may equal `ts` because of this commit itself (the coordinator that
    /// assigned it and the participants that prepared it). Any other shard at `ts` made a
    /// write of its own there, and the retry takes a new timestamp rather than tie with it.
    pub own: Vec<ShardId>,
}

/// Whether a retried commit may keep timestamp `ts` on `shard`, whose floor is `floor` and
/// whose raise is `raise`, set by the shards in the bit mask `raisers`: above both, or equal
/// only where the shards that reached `ts` are the commit's own (`Preset::own`).
fn preset_fits(
    ts: Timestamp,
    own: &[ShardId],
    shard: ShardId,
    floor: Timestamp,
    (raise, raisers): (Timestamp, u64),
) -> bool {
    let own_mask = own
        .iter()
        .filter(|s| s.0 < 64)
        .fold(0u64, |m, s| m | (1 << s.0));
    let floor_ok = ts > floor || (ts == floor && own.contains(&shard));
    let raise_ok = ts > raise || (ts == raise && raisers & !own_mask == 0);
    floor_ok && raise_ok
}

/// A cross-shard commit request, sent to the coordinator (the shard owning the first row).
#[derive(Debug)]
pub(crate) struct CoordinateReq {
    pub parts: Vec<(ShardId, Arc<BatchBuilder>)>,
    pub durability: Durability,
    pub reply: Notifier<Result<CommitInfo>>,
    pub submitted_at: u64,
    pub validate: Option<(Seqno, Vec<ReadKey>)>,
    /// Version of the tablet map the parts were routed with: a commit a participant refused
    /// because it is moving a tablet is retried once the map is newer.
    pub map_version: u64,
    /// A retry's first commit timestamp, kept when it is still above every participant's
    /// floor (the refused attempt then leaves no trace).
    pub commit_ts: Option<Preset>,
    /// `Shared::tablet_epoch` when the commit was coordinated: a refused commit is also
    /// retried once a tablet change has finished since (it may have failed, leaving the map
    /// as it was).
    pub epoch: u64,
    /// `CommitReq::attempts`.
    pub attempts: u32,
}

/// A participant's share of a cross-shard commit.
#[derive(Debug)]
pub(crate) struct PrepareReq {
    /// Version of the tablet map the share was routed with.
    pub map_version: u64,
    pub seqno: Seqno,
    pub commit_ts: Timestamp,
    pub coordinator: ShardId,
    pub bytes: Arc<BatchBuilder>,
    pub durability: Durability,
    pub validate: Option<(Seqno, Vec<ReadKey>)>,
    /// Every shard the commit was routed to: a share routed with an older tablet map runs
    /// again only when one of its rows now lives outside them (#102).
    pub participants: Vec<ShardId>,
}

/// Why a participant could not prepare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrepareError {
    Conflict,
    /// A row's tablet is being split, merged or moved, or now lives on another shard: the
    /// coordinator retries through the new tablet map.
    Moved,
    /// The commit timestamp is below a default timestamp this shard already assigned (D11,
    /// issue #105): the coordinator retries at once with a fresh timestamp.
    BelowFloor,
    Busy,
    Closed,
    TooLarge,
    /// The share can never fit the participant's arena (issue #141).
    NeverFits,
    Io,
}

impl From<PrepareError> for Error {
    fn from(e: PrepareError) -> Self {
        match e {
            PrepareError::Conflict => Error::Conflict,
            PrepareError::Moved | PrepareError::BelowFloor | PrepareError::Busy => Error::Busy,
            PrepareError::Closed => Error::Closed,
            PrepareError::TooLarge => Error::RecordTooLarge,
            PrepareError::NeverFits => Error::BatchTooLarge,
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
        Error::BatchTooLarge => PrepareError::NeverFits,
        _ => PrepareError::Io,
    }
}

/// Messages a shard handles. An engine enum, so the queue hot path has no boxing.
#[derive(Debug)]
pub(crate) enum ShardMsg {
    Commit(CommitReq),
    /// Boxed, so a queued message is sized for a single-shard commit.
    Coordinate(Box<CoordinateReq>),
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
    /// Another shard's checkpoint is pinned by cross-shard commits up to `through`: flush
    /// whatever this shard holds of them (#137).
    Unpin {
        through: Seqno,
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
    /// A failed background compaction's backoff passed (on a moving clock): retry.
    RetryCompaction,
    /// A refused checkpoint's backoff passed (on a moving clock): retry.
    RetryCheckpoint,
    /// A failed flush's backoff passed: retry (issue #141).
    RetryFlush,
    /// Split, merge or move tablets this shard owns (replies when done, if asked). Sent by
    /// the test hooks only; the balancer starts its own changes.
    #[cfg_attr(not(feature = "test-hooks"), allow(dead_code))]
    TabletOp {
        op: TabletOpKind,
        reply: Option<Notifier<Result<()>>>,
    },
    /// A tablet change's manifest commit finished.
    TabletOpDone {
        result: Result<ManifestVersion>,
    },
    /// Run the balancer now (a test hook); replies once whatever it started is done.
    #[cfg_attr(not(feature = "test-hooks"), allow(dead_code))]
    Balance {
        reply: Option<Notifier<Result<()>>>,
    },
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
    predicate: Option<RowPredicate>,
    ticket: Option<CommitTicket>,
    /// Failed before being logged (not applied, resolved with this error).
    failed: Option<Error>,
    /// Arena bytes reserved for it until it is applied or dropped.
    reserved: usize,
    /// A PREPARE: the tablet map version its share was routed with (0 otherwise).
    map_version: u64,
    /// A retried commit: the shards that may already hold its timestamp (`Preset::own`).
    preset_own: Vec<ShardId>,
    /// `CommitReq::attempts`.
    attempts: u32,
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
            commit_ts: req.commit_ts.as_ref().map_or(0, |p| p.ts),
            attempts: req.attempts,
            preset_own: req.commit_ts.map(|p| p.own).unwrap_or_default(),
            bytes: Bytes::Own(req.bytes),
            reply: req.reply,
            submitted_at: req.submitted_at,
            validate: req.validate,
            predicate: req.predicate,
            ticket: None,
            failed: None,
            reserved: 0,
            map_version: req.map_version,
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
    /// The request, kept for a retry when a participant refused with `Moved`.
    parts: Vec<(ShardId, Arc<BatchBuilder>)>,
    validate: Option<(Seqno, Vec<ReadKey>)>,
    map_version: u64,
    moved: bool,
    /// A participant refused `commit_ts` as below its floor: the retry takes a fresh one.
    below_floor: bool,
    commit_ts: Timestamp,
    /// Shards that may hold `commit_ts` because of this commit (see `Preset::own`).
    own: Vec<ShardId>,
    epoch: u64,
    /// `CommitReq::attempts`.
    attempts: u32,
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
    /// `PrepareReq::participants`.
    participants: Vec<ShardId>,
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
    /// Holds a delete of any kind (the flush GC guard, #287).
    deletes: bool,
}

impl MemEntry {
    fn new(table: Memtable) -> Self {
        Self {
            table,
            min_ts: u64::MAX,
            has_shares: false,
            deletes: false,
        }
    }

    fn max_seqno(&self) -> Seqno {
        self.table.seqno_range().map_or(0, |(_, max)| max)
    }
}

/// The flush GC guard (#287, see `docs/design/questions/engine.md`): whether no source of
/// slot `key` other than its memtable with root `root` can hold a delete, judged now on the
/// shard thread as the memtable is queued. The slot's other memtables (the active one
/// included: an applied cross-shard share can carry a seqno below the flushed memtable's),
/// every prepared share writing `key`'s family (it lands later, possibly under a lower
/// seqno; a share that does not decode counts), and every SST of the slot in `view`. Whatever
/// commits here afterwards has a seqno above the visible watermark, so above every entry of
/// the flushed memtable (it froze only once all of them were visible).
fn flush_guard(
    slot: &MemSlot,
    root: u32,
    prepared: &HashMap<Seqno, PreparedShare>,
    view: &View,
    key: (TabletId, FamilyId),
) -> bool {
    let mems = std::iter::once(&slot.active)
        .chain(&slot.frozen)
        .filter(|m| m.table.root() != root)
        .all(|m| !m.deletes);
    let shares = !prepared.values().any(|share| {
        share
            .bytes
            .batch()
            .iter()
            .any(|m| m.is_err() || m.is_ok_and(|m| m.family == key.1))
    });
    let ssts = view
        .ssts
        .family(key.0, key.1)
        .is_none_or(|fam| fam.iter().all(|s| s.meta.deletes == 0));
    mems && shares && ssts
}

/// Guard states of a queued flush (#287): it may install its purge (`IN_FLIGHT`), a delete in
/// its family arrived first (`VOIDED`: the commit is refused and the memtable flushed again
/// without the guard), or its commit took the purge (`INSTALLING`: deletes in the family wait
/// until the shard learns the outcome, so they become visible only after the install).
pub(crate) const GUARD_IN_FLIGHT: u8 = 0;
pub(crate) const GUARD_VOIDED: u8 = 1;
pub(crate) const GUARD_INSTALLING: u8 = 2;

/// A fresh guard state for the flush of memtable `root` of `family`, tracked in `guarded`.
fn guard_state(
    guarded: &mut Vec<(u32, FamilyId, Arc<AtomicU8>)>,
    root: u32,
    family: FamilyId,
) -> Arc<AtomicU8> {
    let state = Arc::new(AtomicU8::new(GUARD_IN_FLIGHT));
    guarded.retain(|g| g.0 != root);
    guarded.push((root, family, Arc::clone(&state)));
    state
}

/// Consecutive voided purges after which a slot compacts without one (#316). The purge
/// is left to a later compaction.
const PURGE_VOID_LIMIT: u32 = 3;

/// A bottommost compaction's purge guard (#316). Its purge assumed that nothing outside
/// its inputs has a timestamp below `min_ts_above`; a write in `family` with a timestamp
/// below that bound and at most `max_ts` (the newest input timestamp: a write above every
/// input changes nothing the purge drops) breaks that assumption.
#[derive(Debug)]
struct CompactionGuard {
    family: FamilyId,
    min_ts_above: Timestamp,
    max_ts: Timestamp,
    state: Arc<AtomicU8>,
}

/// The memtables of one `(tablet, family)` on this shard.
#[derive(Debug)]
struct MemSlot {
    active: MemEntry,
    /// Newest first.
    frozen: Vec<MemEntry>,
    seal: Seal,
}

/// A closing shard flushes an active memtable in place when the arena has no chunk for its
/// replacement (snapshots may pin every retired memtable until the close returns, issue
/// #111): nothing writes to it any more, and the slot goes once its SST is in the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seal {
    /// The active memtable takes writes (or is empty).
    Open,
    /// The active memtable is frozen and queued for flushing, or being flushed.
    Flushing,
    /// Its SST is in the manifest: no view lists it again, and the slot is dropped once
    /// its older frozen memtables are flushed too.
    Flushed,
}

impl MemSlot {
    fn set(&self, shard: ShardId, flushed: &HashSet<(u16, u32)>) -> Arc<MemSet> {
        let mut readers = Vec::with_capacity(1 + self.frozen.len());
        let mut roots = Vec::with_capacity(1 + self.frozen.len());
        for m in self.unflushed(shard, flushed) {
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
        self.unflushed(shard, flushed)
            .map(|m| m.table.reader())
            .collect()
    }

    /// The memtables not yet in SSTs, active first.
    fn unflushed<'a>(
        &'a self,
        shard: ShardId,
        flushed: &'a HashSet<(u16, u32)>,
    ) -> impl Iterator<Item = &'a MemEntry> {
        let active = (self.seal != Seal::Flushed).then_some(&self.active);
        active
            .into_iter()
            .chain(&self.frozen)
            .filter(move |m| !flushed.contains(&(shard.0, m.table.root())))
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
    memtables: &'a mut BTreeMap<(TabletId, FamilyId), MemSlot>,
    arena: &mut ShardArena,
    view_dirty: &mut bool,
    key: (TabletId, FamilyId),
) -> Result<&'a mut MemSlot> {
    match memtables.entry(key) {
        std::collections::btree_map::Entry::Occupied(e) => Ok(e.into_mut()),
        std::collections::btree_map::Entry::Vacant(e) => {
            let active = MemEntry::new(Memtable::create(arena)?);
            *view_dirty = true;
            Ok(e.insert(MemSlot {
                active,
                frozen: Vec::new(),
                seal: Seal::Open,
            }))
        }
    }
}

/// Same-commit collapse scratch (decision D34): the last mutation per
/// `(table, family, row, qualifier, timestamp)` wins. Mutations are compared exactly (by the
/// bytes of their key parts, located by offset within the batch), with reusable buffers and
/// no per-cell allocation. The scan is also the batch's only decode on apply: `apply` reads
/// each mutation back from `keys`.
#[derive(Debug, Default)]
struct Dedup {
    keys: Vec<MutKey>,
    order: Vec<u32>,
    loser: Vec<bool>,
    /// The batch failed to decode after `keys.len()` mutations.
    error: Option<pigeonhole_format::Error>,
}

/// A mutation of the batch, its parts located within the batch bytes.
#[derive(Debug, Clone, Copy)]
struct MutKey {
    table: u32,
    family: u32,
    kind: Kind,
    /// The timestamp it applies at (its own, or the commit's).
    ts: Timestamp,
    row: (u32, u32),
    qualifier: (u32, u32),
    value: (u32, u32),
}

impl MutKey {
    fn marker(&self) -> bool {
        self.kind == Kind::FamilyDelete
    }
}

/// `part` of the batch `bytes` (a `MutKey` span).
fn span(bytes: &[u8], (off, len): (u32, u32)) -> &[u8] {
    &bytes[off as usize..(off + len) as usize]
}

impl Dedup {
    /// Fills the tables for `batch` (whose bytes are `bytes`). Returns whether any
    /// duplicate exists.
    fn scan(&mut self, batch: BatchRef<'_>, bytes: &[u8], commit_ts: Timestamp) -> bool {
        self.keys.clear();
        self.error = None;
        let base = bytes.as_ptr() as usize;
        let at = |part: &[u8]| -> (u32, u32) {
            let off = (part.as_ptr() as usize).wrapping_sub(base);
            (off as u32, part.len() as u32)
        };
        for m in batch.iter() {
            let m = match m {
                Ok(m) => m,
                Err(e) => {
                    self.error = Some(e);
                    break;
                }
            };
            self.keys.push(MutKey {
                table: m.table.0,
                family: m.family.0,
                kind: m.kind,
                ts: m.ts.unwrap_or(commit_ts),
                row: at(m.row),
                qualifier: at(m.qualifier),
                value: at(m.value),
            });
        }
        let n = self.keys.len();
        self.loser.clear();
        self.loser.resize(n, false);
        if n < 2 {
            return false;
        }
        let keys = &self.keys;
        let part = |at: (u32, u32)| span(bytes, at);
        let cmp = |a: &MutKey, b: &MutKey| {
            (a.table, a.family, a.marker())
                .cmp(&(b.table, b.family, b.marker()))
                .then_with(|| compare(part(a.row), part(b.row)))
                .then_with(|| compare(part(a.qualifier), part(b.qualifier)))
                .then_with(|| a.ts.cmp(&b.ts))
        };
        // Already strictly increasing (a row's cells in qualifier order, a sorted batch): no
        // two are equal, so nothing collapses and the sort is skipped.
        if keys.windows(2).all(|w| cmp(&w[0], &w[1]).is_lt()) {
            return false;
        }
        self.order.clear();
        self.order.extend(0..n as u32);
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

/// A row's hash for the admission sets (`touched`, `pending_rows`, `parked_rows`): one seeded
/// fast hasher for the process, so every shard hashes a row alike, and user row bytes cannot
/// be chosen to collide.
fn hash_row(table: TableId, row: &[u8]) -> u64 {
    static ROWS: std::sync::LazyLock<FastBuildHasher> =
        std::sync::LazyLock::new(FastBuildHasher::default);
    ROWS.hash_one((table.0, row))
}

/// Prepares spare WAL segments off the foreground loop (decision D35).
struct SpareTask {
    spares: SpareSegments,
    running: Arc<AtomicBool>,
}

impl Task for SpareTask {
    fn run(&mut self, _deadline_nanos: u64, _waker: &TaskWaker) -> TaskPoll {
        // A failed sync poisons the stream, and its next append or sync fails; a failed
        // allocation or write leaves the slots blank for a later attempt.
        let _ = self.spares.prepare(self.spares.target());
        self.running.store(false, Ordering::Release);
        TaskPoll::Done
    }

    fn name(&self) -> &'static str {
        "wal-spares"
    }
}

/// A clock timer's state, shared between the shard and the timer task.
#[derive(Debug)]
struct TimerState {
    /// Set by the shard to cancel the timer.
    cancel: AtomicBool,
    /// Set by the timer when it fired or gave up.
    done: AtomicBool,
    /// The clock reading at which the timer gave up because the clock stopped moving
    /// (`u64::MAX`: it did not give up).
    frozen_at: AtomicU64,
    /// The timer task's waker while it sleeps, so a cancel ends the sleep at once.
    waker: Mutex<Option<TaskWaker>>,
}

impl TimerState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            cancel: AtomicBool::new(false),
            done: AtomicBool::new(false),
            frozen_at: AtomicU64::new(u64::MAX),
            waker: Mutex::new(None),
        })
    }

    fn cancel(&self) {
        self.cancel.store(true, Ordering::Release);
        let waker = self
            .waker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(w) = waker {
            w.wake();
        }
    }

    /// The timer is no longer running.
    fn finished(&self) -> bool {
        self.done.load(Ordering::Acquire) || self.cancel.load(Ordering::Acquire)
    }

    /// The timer fired (its deadline passed).
    fn fired(&self) -> bool {
        self.done.load(Ordering::Acquire) && self.frozen_at.load(Ordering::Acquire) == u64::MAX
    }

    /// The timer gave up on a stopped clock and the clock still reads `now`: only events
    /// can end the wait (the frozen-clock fallbacks of issue #70 apply).
    fn frozen(&self, now: u64) -> bool {
        self.frozen_at.load(Ordering::Acquire) == now
    }
}

/// Sends its message to the shard once a deadline on the VFS clock passes (unless
/// cancelled).
///
/// On a real clock (`Vfs::clock_is_simulated` false) the timer sleeps until the deadline
/// at once and never gives up (#263). On a simulated clock it first polls the clock. Once
/// it sees the clock move it sleeps until the deadline
/// (`TaskPoll::SleepUntil`: the shard parks rather than spins, issue #89). The runtime runs a
/// sleeping task early when the clock has not moved since the shard went idle, and the timer
/// then polls again. After `STALL_TIMER_FROZEN_POLLS` polls in a row that see the clock
/// unchanged it gives up, records the reading and kicks the shard, which then applies its
/// frozen-clock fallback while the clock still reads the same (issue #70, D126): on a
/// frozen clock a polling timer would spin for ever, since the shard loop never reaches its
/// slice deadline either.
struct ClockTimer {
    vfs: VfsRef,
    release_at: u64,
    state: Arc<TimerState>,
    submitter: Submitter<ShardMsg>,
    /// Sent when the deadline passes.
    fire: Option<ShardMsg>,
    last_now: u64,
    frozen_polls: u32,
}

impl ClockTimer {
    fn new(
        vfs: &VfsRef,
        release_at: u64,
        state: Arc<TimerState>,
        submitter: Submitter<ShardMsg>,
        fire: ShardMsg,
    ) -> Self {
        Self {
            vfs: Arc::clone(vfs),
            release_at,
            state,
            submitter,
            fire: Some(fire),
            last_now: u64::MAX,
            frozen_polls: 0,
        }
    }
}

impl Task for ClockTimer {
    fn run(&mut self, _deadline_nanos: u64, waker: &TaskWaker) -> TaskPoll {
        if self.state.cancel.load(Ordering::Acquire) {
            return TaskPoll::Done;
        }
        let now = self.vfs.monotonic_nanos();
        if now < self.release_at && !self.vfs.clock_is_simulated() {
            // A real clock always moves on, however coarse its ticks: sleep until the
            // deadline, never taking it as stopped (#263).
            *self
                .state
                .waker
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(waker.clone());
            if self.state.cancel.load(Ordering::Acquire) {
                return TaskPoll::Done;
            }
            return TaskPoll::SleepUntil(self.release_at);
        }
        if now < self.release_at {
            if now != self.last_now {
                let moved = self.last_now != u64::MAX;
                self.last_now = now;
                self.frozen_polls = 0;
                if !moved {
                    return TaskPoll::Pending;
                }
                // The clock moves on its own: sleep until the deadline.
                *self
                    .state
                    .waker
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = Some(waker.clone());
                if self.state.cancel.load(Ordering::Acquire) {
                    return TaskPoll::Done;
                }
                return TaskPoll::SleepUntil(self.release_at);
            }
            self.frozen_polls += 1;
            if self.frozen_polls < STALL_TIMER_FROZEN_POLLS {
                return TaskPoll::Pending;
            }
            self.state.frozen_at.store(now, Ordering::Release);
            self.state.done.store(true, Ordering::Release);
            let _ = self.submitter.submit(ShardMsg::Kick);
            return TaskPoll::Done;
        }
        self.state.done.store(true, Ordering::Release);
        if let Some(msg) = self.fire.take() {
            let _ = self.submitter.submit(msg);
        }
        TaskPoll::Done
    }

    fn name(&self) -> &'static str {
        "timer"
    }
}

/// Arena room a batch needs (`ShardState::arena_needed`).
#[derive(Debug, Clone, Copy)]
struct ArenaNeed {
    /// Worst-case bytes of free chunks it uses.
    bytes: usize,
    /// Chunks its largest entry takes (contiguous).
    run_chunks: usize,
    /// Chunks its smallest entry larger than a chunk takes (0 without one).
    min_large: usize,
    /// Chunks its allocations can take in all.
    alloc_chunks: usize,
}

/// A group waiting for a flush to free memtable arena room (a write stall, counted in the
/// metrics), refused with `Busy` once `write_stall_timeout_nanos` have passed (D124). On a
/// frozen clock it is also refused after `ROOM_FLUSH_ATTEMPTS` failed flushes in a row, or
/// at once when nothing can free room (issue #70).
#[derive(Debug)]
struct RoomWait {
    since: u64,
    /// The timeout timer.
    timer: Arc<TimerState>,
    failed_flushes: u32,
    /// On a moving clock, kicks the shard to look for room again: room freed by a snapshot
    /// dropped on another thread or a reader process's unpin is not announced (issue #141).
    recheck: Option<Arc<TimerState>>,
    /// The re-check timer's last interval (the base doubling up to 100 times it).
    recheck_nanos: u64,
}

impl RoomWait {
    fn new(since: u64, timer: Arc<TimerState>) -> Self {
        Self {
            since,
            timer,
            failed_flushes: 0,
            recheck: None,
            recheck_nanos: 0,
        }
    }

    /// Arms the re-check timer unless one is pending: it kicks the shard after `base` (1 ms
    /// by default), then twice as long each time up to 100 times `base`, so freed room is
    /// seen within about 100 ms.
    fn arm_recheck(
        &mut self,
        vfs: &VfsRef,
        base: u64,
        ctx: &mut ShardContext<'_, ShardMsg>,
        me: ShardId,
    ) {
        if self.recheck.as_ref().is_some_and(|t| !t.finished()) {
            return;
        }
        self.recheck_nanos = if self.recheck_nanos == 0 {
            base
        } else {
            self.recheck_nanos
                .saturating_mul(2)
                .min(base.saturating_mul(ROOM_RECHECK_CAP))
        };
        let state = TimerState::new();
        ctx.spawn(Box::new(ClockTimer::new(
            vfs,
            ctx.now_nanos().saturating_add(self.recheck_nanos),
            Arc::clone(&state),
            ctx.submitter(me).clone(),
            ShardMsg::Kick,
        )));
        self.recheck = Some(state);
    }

    /// Whether the clock stopped at `now`: the timeout or the re-check timer gave up on this
    /// reading.
    fn stopped(&self, now: u64) -> bool {
        self.timer.frozen(now) || self.recheck.as_ref().is_some_and(|t| t.frozen(now))
    }

    /// The wait is over: stops both timers.
    fn cancel(&self) {
        self.timer.cancel();
        if let Some(t) = &self.recheck {
            t.cancel();
        }
    }
}

/// A record of this stream the checkpoint cannot pass yet.
#[derive(Debug)]
struct Logged {
    /// Position just past the record.
    end: Lsn,
    /// Bytes logged on this shard before the record (`ShardState::log_bytes` when it was
    /// appended; records replayed at open count as none).
    pos: u64,
    seqno: Seqno,
    kind: LoggedKind,
}

/// The slots a logged single-shard commit wrote: inline for up to two (most commits write
/// one tablet's family), so logging a commit allocates nothing (#320).
type LoggedSlots = smallvec::SmallVec<[(TabletId, FamilyId); 2]>;

#[derive(Debug)]
enum LoggedKind {
    /// A single-shard commit writing to these slots.
    Single { slots: LoggedSlots },
    /// A participant's PREPARE writing to these slots.
    Prepare {
        slots: Vec<(TabletId, FamilyId)>,
        coordinator: ShardId,
    },
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
    /// The timer that kicks the shard once a token is due.
    timer: Option<Arc<TimerState>>,
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
            t.cancel();
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
    /// End of the newest PREPARE or COMMIT record given a ticket since open. Other shards' flushes
    /// of cross-shard shares need the stream durable only through here, so a poisoned
    /// stream's barrier still holds for them once it is (5-6 5.1).
    cross_end: Lsn,
    /// After the close: whether the stream was durable through `cross_end` when dropped.
    cross_durable_at_close: bool,
    arena: ShardArena,
    chunk_size: usize,
    /// Ordered, so freezes and flushes visit slots in the same order on every run (#61).
    memtables: BTreeMap<(TabletId, FamilyId), MemSlot>,
    /// Retired memtables waiting for reader processes to release their views:
    /// `(view version that dropped them, token)`.
    retired: Vec<(u64, Retired)>,
    /// Routing, refreshed when the view's tablet map changes.
    tablets: Arc<TabletMap>,
    /// Members drained since the last group.
    pending: Vec<Member>,
    /// Emptied member vectors kept for the next group and `pending` (#320): a commit then
    /// allocates none for the group's bookkeeping.
    member_bufs: Vec<Vec<Member>>,
    /// Groups whose sync is in flight, oldest first.
    unresolved: VecDeque<Group>,
    next_group: u64,
    /// Cross-shard seqnos this shard coordinates, not yet applied everywhere.
    held: BTreeSet<Seqno>,
    coord: HashMap<Seqno, Coord>,
    prepared: HashMap<Seqno, PreparedShare>,
    /// Queued or running flushes that may purge versions under the guard (#287), as
    /// `(memtable root, family, state)`: a delete in the family voids one before it installs,
    /// and waits while one installs.
    guarded: Vec<(u32, FamilyId, Arc<AtomicU8>)>,
    /// Roots whose guarded flush a delete voided: flushed again without the guard.
    voided_roots: HashSet<u32>,
    /// The running compaction's purge guard (#316), when it is bottommost and purges
    /// below a `min_ts_above` it sampled at its plan: a write in the family that could
    /// land below that bound, among the inputs' timestamps, voids it before it installs and
    /// waits while it installs.
    compaction_guard: Option<CompactionGuard>,
    /// Consecutive voided compactions per slot: at `PURGE_VOID_LIMIT` the slot compacts
    /// without the purge below `min_ts_above` (and so without a guard), so a backfill
    /// written newest to oldest cannot keep it from installing.
    purge_voids: HashMap<(TabletId, FamilyId), u32>,
    /// Rows (hashed with their table) of prepared, undecided shares, with a count per row.
    pending_rows: FastMap<u64, u32>,
    /// Arena bytes reserved by admitted-but-unapplied members and undecided shares.
    reserved: usize,
    /// `(tablet, family)` slots whose active memtable crossed the freeze threshold.
    to_freeze: Vec<(TabletId, FamilyId)>,
    /// Largest default timestamp assigned on this shard (D11).
    ts_floor: Timestamp,
    key_buf: Vec<u8>,
    dedup: Dedup,
    /// The row hashes of the member `reserve_room` sized last, for `touch_rows`.
    admit_rows: Vec<u64>,
    touched: FastSet<u64>,
    /// Slots the last `apply` wrote (deduplicated).
    touched_slots: Vec<(TabletId, FamilyId)>,
    closing: bool,
    close_stage: CloseStage,
    spares: Option<SpareSegments>,
    spares_running: Arc<AtomicBool>,
    /// The epoch of the segment the stream started at open: spares are prepared only once
    /// the stream is past half of it (#143).
    open_epoch: u32,
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
    /// The last freeze of every memtable left a non-empty one active: the arena had no
    /// chunk for its replacement (snapshots may pin the retired ones). `flush` and
    /// `compact` callers wait for it, and the freeze is tried again (issue #116).
    starved_all: bool,
    /// The wait of `flush` and `compact` callers for a chunk (`starved_all`): when it
    /// began and its timeout timer. It ends with `Busy` as a write stall does (D124, D126).
    starve_wait: Option<RoomWait>,
    /// A flush failed while closing: the close gives up on flushing (the WAL keeps the data)
    /// and is not clean.
    flush_failed: bool,
    /// Flushes failed in a row (reset by one that succeeds).
    flush_failures: u32,
    /// After a failed flush, none starts until this timer fires (`RetryFlush`), so a device
    /// that keeps failing is not rewritten back to back (issue #141). On a frozen clock the
    /// timer gives up and the next flush trigger retries: retrying at once when it gives up
    /// would loop (it gives up within microseconds when the shard is driven in a loop).
    flush_retry: Option<Arc<TimerState>>,
    /// Tablets dropped since open: their records need no flush before a checkpoint.
    dropped: HashSet<TabletId>,

    // ---- checkpoints ----
    /// Records the checkpoint cannot pass, in log order.
    log: VecDeque<Logged>,
    /// Bytes logged since open (record payloads plus a fixed overhead each): positions for
    /// the WAL pin limit (#137).
    log_bytes: u64,
    /// Slots flushed because their records pinned the checkpoint, whatever their size,
    /// until their active memtable freezes.
    unpin: BTreeSet<(TabletId, FamilyId)>,
    /// The last unpin pass forced every record below this position, at `log_bytes` equal
    /// to `unpin_at`.
    unpin_upto: u64,
    unpin_at: u64,
    /// The shards already sent `Unpin` for a needed record, by its seqno (pruned when the
    /// record leaves `log`).
    asked: HashMap<Seqno, Vec<ShardId>>,
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
    /// Pickers by `CompactionStyle`: each family compacts by its own style.
    leveled: CompactionPicker,
    tiered: CompactionPicker,
    fifo: CompactionPicker,
    /// The slot a compaction task is running for.
    compaction: Option<(TabletId, FamilyId)>,
    /// `Engine::compact` callers: `(table filter, reply)`, served in order.
    compact_all: VecDeque<(Option<TableId>, Notifier<Result<()>>)>,
    /// The running compaction was started for the `compact_all` caller at the front: its
    /// failure is that caller's. A background compaction's failure is nobody's: it backs
    /// off (issue #141).
    compaction_full: bool,
    /// Blob GC planning (issues #33, #240).
    blob_gc: compact::BlobGc,
    /// Every slot due for a compaction is backing off after a failure, so no compaction can
    /// relieve an L0 stall now (set by `maintain`; D119, issue #141).
    due_backing_off: bool,
    /// Fires (`RetryCompaction`) at its deadline, the earliest end of a slot's backoff.
    backoff_timer: Option<(Arc<TimerState>, u64)>,
    /// Slots whose compaction failed: failures in a row and the time (VFS monotonic
    /// nanoseconds) before which no background compaction picks them again, so one slot
    /// that keeps failing does not stop compaction for the rest of the shard (issue #141).
    slot_backoff: HashMap<(TabletId, FamilyId), (u32, u64)>,
    /// Checkpoints refused in a row (a full disk), and the timer that retries the last one
    /// on a moving clock (an idle shard has no other event to retry it on).
    checkpoint_failures: u32,
    checkpoint_timer: Option<Arc<TimerState>>,
    stall: Stall,

    // ---- tablet changes and the balancer ----
    /// Tablets the running change splits, merges or moves.
    moving: HashSet<TabletId>,
    op: Option<ActiveOp>,
    op_queue: VecDeque<(TabletOpKind, Option<Notifier<Result<()>>>)>,
    /// Single-shard commits touching `moving` tablets, held (unlogged) until the change is
    /// done and then routed again.
    parked: Vec<Member>,
    parked_rows: FastSet<u64>,
    /// Fires when the oldest parked commit or retry reaches `write_stall_timeout_nanos`
    /// (its deadline, its state); both then fail with `Busy` (#102).
    tablet_timer: Option<(u64, Arc<TimerState>)>,
    /// The tablet map version as of the last change that handed a tablet to another shard
    /// (0: none). Commits routed with an older map may arrive for rows this shard no longer
    /// owns, so admission checks their routing; newer ones skip the check (#102).
    lost_version: u64,
    /// Cross-shard commits to retry once the tablet map changes.
    retries: Vec<CoordinateReq>,
    /// Slots whose SSTs, inherited from a split's parent, still hold a sibling's rows and so
    /// block a merge the balancer wants: rewritten when no other compaction is due (#95).
    cleanups: Vec<(TabletId, FamilyId)>,
    /// The next background compaction goes to a queued cleanup (they alternate with due
    /// compactions, so a busy shard never starves them).
    cleanup_turn: bool,
    /// Writes per tablet in the current balancer interval.
    loads: HashMap<TabletId, TabletLoad>,
    window_writes: u64,
    next_balance: u64,
    rng: u64,
    /// Moving average of `window_writes` over balancer intervals (#103).
    load_ewma: f64,
    /// Balancer passes run so far.
    balance_pass: u64,
    /// The pass at which each owned tablet arrived on this shard (0: held at the first pass).
    arrived: HashMap<TabletId, u64>,
    /// The pass at which the balancer last started a move or a split over shards.
    skew_pass: Option<u64>,
    /// Wakes the shard for its next balancer pass (#103).
    balance_timer: Option<Arc<TimerState>>,
    /// The current balancer interval: the base `balance_interval_nanos`, doubled after each
    /// idle pass up to 10 s (0 before the first pass).
    balance_interval: u64,
    /// `Shared::tablet_epoch` at the last balancer pass: a change since then ends the
    /// idle backoff.
    balance_epoch: u64,
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
        let leveled = CompactionPicker::new(CompactionStyle::Leveled, shared.picker.clone());
        let tiered = CompactionPicker::new(CompactionStyle::Tiered, shared.picker.clone());
        let fifo = CompactionPicker::new(CompactionStyle::FifoByTime, shared.picker.clone());
        let shard = Self {
            id,
            shared,
            wal: None,
            poisoned: false,
            cross_end: Lsn::default(),
            cross_durable_at_close: false,
            arena: ShardArena::new(region, chunk_size),
            chunk_size,
            memtables: BTreeMap::new(),
            retired: Vec::new(),
            tablets,
            pending: Vec::new(),
            member_bufs: Vec::new(),
            unresolved: VecDeque::new(),
            next_group: 1,
            held: BTreeSet::new(),
            coord: HashMap::new(),
            prepared: HashMap::new(),
            guarded: Vec::new(),
            voided_roots: HashSet::new(),
            compaction_guard: None,
            purge_voids: HashMap::new(),
            pending_rows: FastMap::default(),
            reserved: 0,
            to_freeze: Vec::new(),
            ts_floor,
            key_buf: Vec::new(),
            dedup: Dedup::default(),
            admit_rows: Vec::new(),
            touched: FastSet::default(),
            touched_slots: Vec::new(),
            closing: false,
            close_stage: CloseStage::Open,
            spares: None,
            spares_running: Arc::new(AtomicBool::new(false)),
            open_epoch: 0,
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
            starved_all: false,
            starve_wait: None,
            flush_failed: false,
            flush_failures: 0,
            flush_retry: None,
            dropped: HashSet::new(),
            log: VecDeque::new(),
            log_bytes: 0,
            unpin: BTreeSet::new(),
            unpin_upto: 0,
            unpin_at: 0,
            asked: HashMap::new(),
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
            leveled,
            tiered,
            fifo,
            compaction: None,
            compact_all: VecDeque::new(),
            compaction_full: false,
            blob_gc: compact::BlobGc,
            due_backing_off: false,
            backoff_timer: None,
            slot_backoff: HashMap::new(),
            checkpoint_failures: 0,
            checkpoint_timer: None,
            stall: Stall::default(),
            moving: HashSet::new(),
            op: None,
            op_queue: VecDeque::new(),
            parked: Vec::new(),
            parked_rows: FastSet::default(),
            tablet_timer: None,
            lost_version: 0,
            retries: Vec::new(),
            cleanups: Vec::new(),
            cleanup_turn: false,
            loads: HashMap::new(),
            window_writes: 0,
            next_balance: 0,
            rng: 0x9E37_79B9_7F4A_7C15 ^ (u64::from(id.0) + 1),
            load_ewma: 0.0,
            balance_pass: 0,
            arrived: HashMap::new(),
            skew_pass: None,
            balance_timer: None,
            balance_interval: 0,
            balance_epoch: 0,
        };
        shard.publish_arena();
        shard
    }

    /// Test hook: the arena's free bytes, largest free run and size, as one snapshot
    /// (`Engine::arena_free`), from the shard's construction on. Nothing without test hooks.
    fn publish_arena(&self) {
        #[cfg(feature = "test-hooks")]
        {
            *self.shared.metrics[usize::from(self.id.0)]
                .hooks
                .arena
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = (
                self.arena.free_bytes() as u64,
                self.arena.largest_free_run() as u64,
                self.arena.region().len() as u64,
            );
        }
    }

    pub(crate) fn set_wal(&mut self, wal: Box<dyn Wal>) {
        self.spares = wal.spares();
        self.open_epoch = wal.written().epoch();
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

    /// Whether tablets may split, merge and move (`EngineOptions::tablet_changes`). Off, a
    /// table is one tablet on shard `tablet % shards` for ever.
    pub(crate) fn tablets_on(&self) -> bool {
        self.shared.balance.enabled
    }

    pub(crate) fn raise_ts_floor(&mut self, ts: Timestamp) {
        trace!("shard {} raise floor {} -> {ts}", self.id.0, self.ts_floor);
        if ts > self.ts_floor {
            self.ts_floor = ts;
            // Coordinators read it to keep a moved tablet's timestamps above its old floor.
            if self.tablets_on()
                && let Some(f) = self.shared.ts_floors.get(usize::from(self.id.0))
            {
                f.0.store(ts, Ordering::Release);
            }
        }
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

    /// Whether the arena has room to replay `bytes` without a flush: the rows this shard
    /// owns, as `arena_needed` counts them, plus a chunk for every slot the record creates.
    /// Replay cannot flush on its own; the caller spills every recovered memtable to SSTs
    /// first when this is false (#143).
    pub(crate) fn replay_fits(&self, bytes: &[u8]) -> bool {
        let Ok(batch) = BatchRef::new(bytes) else {
            return true;
        };
        let mut total = 0usize;
        let mut new_slots: Vec<(TabletId, FamilyId)> = Vec::new();
        for m in batch.iter().flatten() {
            let Some((tablet, owner)) = self.tablets.route(m.table, m.row) else {
                continue;
            };
            if owner != self.id {
                continue;
            }
            let key = 2 * (m.row.len() + m.qualifier.len()) + KEY_FIXED;
            total += ENTRY_OVERHEAD + key + m.value.len();
            if !self.memtables.contains_key(&(tablet, m.family))
                && !new_slots.contains(&(tablet, m.family))
            {
                new_slots.push((tablet, m.family));
            }
        }
        total == 0 || self.arena.free_bytes() >= 2 * total + (new_slots.len() + 2) * self.chunk_size
    }

    /// Whether any memtable holds recovered data.
    pub(crate) fn has_recovered(&self) -> bool {
        self.memtables
            .values()
            .any(|s| !s.active.table.is_empty() || !s.frozen.is_empty())
    }

    /// The memtables `take_memtables` returned are written out and their readers dropped:
    /// give their chunks back to the arena.
    pub(crate) fn release_taken(&mut self) {
        self.refresh_free();
    }

    /// Records a replayed record of this stream for checkpointing: a single commit, an
    /// applied prepare (`coordinator`), an aborted or incomplete one (`needed == false`), or
    /// a COMMIT decision.
    pub(crate) fn log_replayed(&mut self, end: Lsn, seqno: Seqno, kind: ReplayedKind) {
        self.last_end = Some(self.last_end.map_or(end, |l| l.max(end)));
        match kind {
            ReplayedKind::Single { slots } => self.log.push_back(Logged {
                end,
                pos: self.log_bytes,
                seqno,
                kind: LoggedKind::Single {
                    slots: slots.into_iter().collect(),
                },
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
                    pos: self.log_bytes,
                    seqno,
                    kind: LoggedKind::Prepare { slots, coordinator },
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
                    pos: self.log_bytes,
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
        self.asked.clear();
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
        let this = &*self;
        this.shared.publish_view(&[], |current, version| {
            // Built under the publish lock: built before it, the piece could miss a manifest
            // commit that published the SST of one of these memtables in between, and put
            // the memtable back beside its SST (issue #131).
            let piece = Arc::new(ShardMems {
                map: this.mem_sets().into_iter().collect(),
            });
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
                    &this.shared.live_views,
                    current.manifest_version,
                )),
            }
        })?;
        Ok(())
    }

    /// Worst-case arena bytes `batch` needs, so a commit waits for room (or is refused with
    /// `BatchTooLarge` when it could never fit) before its record is logged rather than
    /// half-applied.
    ///
    /// A memtable bump-allocates entries in runs of whole chunks. An entry that does not fit
    /// the rest of the current run takes a new run, rounded up to whole chunks, and the
    /// rest of the old run is lost. That lost tail is shorter than both the entry that did
    /// not fit and one chunk. So every entry costs at most its size plus `min(size,
    /// chunk)`, and a single large value costs about its size, not twice it (issue #141).
    /// Each memtable the batch writes to may also leave its last run's tail unused: one
    /// chunk more per slot touched. The bound is checked by the memtable crate's property
    /// test `a_batch_stays_within_the_engine_bound`.
    ///
    /// An entry larger than a chunk needs contiguous free chunks, which free bytes scattered
    /// across the arena do not provide. So this also returns the largest and smallest such
    /// entry's size in chunks, and how many chunks the batch's allocations can take in all
    /// (each entry at most its own, each slot it creates one).
    /// Also leaves in `rows` the hash of each row the batch writes (once per run of writes
    /// to one row), which `touch_rows` records if the member is admitted.
    fn arena_needed(&self, batch: BatchRef<'_>, rows: &mut Vec<u64>) -> ArenaNeed {
        rows.clear();
        let mut total = 0usize;
        let mut waste = 0usize;
        let mut largest = 0usize;
        let (mut alloc_chunks, mut min_large) = (0usize, usize::MAX);
        // `(tablet, family)` slots the batch writes to, and those it would create (each a
        // memtable with a chunk of its own; a table split into many tablets has many slots).
        // Inline for a few: admission computes this for every commit (#320).
        let mut touched: smallvec::SmallVec<[(TabletId, FamilyId); 4]> = smallvec::SmallVec::new();
        let mut unrouted = 0usize;
        let mut new_slots = 0usize;
        // The row of the previous write and its tablet: a run of writes to one row routes
        // and hashes it once.
        let mut last: Option<(TableId, &[u8], Option<TabletId>)> = None;
        for m in batch.iter().flatten() {
            let key = 2 * (m.row.len() + m.qualifier.len()) + KEY_FIXED;
            let entry = ENTRY_OVERHEAD + key + m.value.len();
            total += entry;
            waste += entry.min(self.chunk_size);
            largest = largest.max(entry);
            // The arena's first chunk loses a 64-byte prefix.
            let chunks = (entry + 64).div_ceil(self.chunk_size);
            alloc_chunks += chunks;
            if chunks > 1 {
                min_large = min_large.min(chunks);
            }
            let tablet = match last {
                Some((t, r, tablet)) if t == m.table && compare(r, m.row).is_eq() => tablet,
                _ => {
                    let tablet = self.tablets.route(m.table, m.row).map(|(t, _)| t);
                    rows.push(hash_row(m.table, m.row));
                    last = Some((m.table, m.row, tablet));
                    tablet
                }
            };
            match tablet {
                Some(tablet) if !touched.contains(&(tablet, m.family)) => {
                    touched.push((tablet, m.family));
                    if self.tablets_on() && !self.memtables.contains_key(&(tablet, m.family)) {
                        new_slots += 1;
                    }
                }
                Some(_) => {}
                None => unrouted += 1,
            }
        }
        // A new memtable needs a chunk of its own, and a freeze needs one more.
        let chunks = touched.len() + unrouted + new_slots + 2;
        ArenaNeed {
            bytes: total + waste + chunks * self.chunk_size,
            run_chunks: (largest + 64).div_ceil(self.chunk_size).max(1),
            min_large: if min_large == usize::MAX {
                0
            } else {
                min_large
            },
            alloc_chunks: alloc_chunks + new_slots,
        }
    }

    /// Retires the memtables of slots that hold nothing (an empty active memtable, nothing
    /// frozen): each pins a whole arena chunk, and a shard with many tablets would otherwise
    /// fill its arena with empty memtables. A later write creates the slot again. Called only
    /// when a commit waits for arena room (#104).
    fn retire_idle_slots(&mut self) -> Result<()> {
        if !self.tablets_on() {
            return Ok(());
        }
        let idle: Vec<(TabletId, FamilyId)> = self
            .memtables
            .iter()
            .filter(|(k, slot)| {
                slot.active.table.is_empty()
                    && slot.frozen.is_empty()
                    && !self.to_freeze.contains(k)
            })
            .map(|(k, _)| *k)
            .collect();
        if idle.is_empty() {
            return Ok(());
        }
        let mut retired = Vec::with_capacity(idle.len());
        for key in idle {
            if let Some(slot) = self.memtables.remove(&key) {
                retired.push(slot.active.table.retire());
            }
        }
        self.publish_retired(retired)
    }

    /// Publishes a view without memtables just removed from the slots, then hands their
    /// chunks to `retired` until no reader can still see them. They are kept even when the
    /// publish fails (the shard is then poisoned, and the still-current view names them
    /// until reopen): dropping the tokens would leak the chunks for the rest of the run
    /// (review 5-6 5.8, #148).
    fn publish_retired(&mut self, retired: Vec<Retired>) -> Result<()> {
        self.view_dirty = true;
        let published = self.publish_memtables();
        let current = self.shared.view.load().version;
        // A failed publish leaves the old view current: the chunks free once readers move
        // past it, to the version that would have dropped them.
        let version = if published.is_ok() {
            current
        } else {
            current + 1
        };
        self.retired
            .extend(retired.into_iter().map(|r| (version, r)));
        self.reclaim_retired();
        published
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
        let mut rows = std::mem::take(&mut self.admit_rows);
        rows.clear();
        let need = match BatchRef::new(bytes) {
            Ok(batch) => self.arena_needed(batch, &mut rows),
            Err(_) => ArenaNeed {
                bytes: 0,
                run_chunks: 1,
                min_large: 0,
                alloc_chunks: 0,
            },
        };
        self.admit_rows = rows;
        let needed = need.bytes;
        // Entries within a chunk fit any free chunk. A larger entry must also find a long
        // enough run (issue #141: a run too short for it, with enough free chunks elsewhere,
        // used to poison the shard at apply). Before any large entry allocates, at most the
        // batch's other allocations and those reserved for this group's earlier members have
        // taken chunks: if a block of the largest entry's size is sure to be left after
        // that many (`ShardArena::blocks_left`), every large entry finds a run.
        let chunk = self.chunk_size;
        let reserved = self.reserved;
        let bytes_fit = |arena: &ShardArena| arena.free_bytes() >= reserved.saturating_add(needed);
        let run_fits = |arena: &ShardArena| {
            need.run_chunks <= 1
                || arena.blocks_left(
                    need.run_chunks,
                    (need.alloc_chunks - need.min_large) + reserved.div_ceil(chunk),
                ) >= 1
        };
        let fits = |arena: &ShardArena| bytes_fit(arena) && run_fits(arena);
        if !fits(&self.arena) {
            // Memtables whose readers left since the last reclaim count too.
            self.reclaim_retired();
            self.refresh_free();
        }
        trace!(
            "shard {} reserve: needed={needed} run={} of {} chunks free={} largest={} reserved={} total={}",
            self.id.0,
            need.run_chunks,
            need.alloc_chunks,
            self.arena.free_bytes(),
            self.arena.largest_free_run(),
            self.reserved,
            self.arena.region().len()
        );
        if !fits(&self.arena) {
            #[cfg(feature = "test-hooks")]
            if bytes_fit(&self.arena) {
                self.shared.metrics[usize::from(self.id.0)]
                    .hooks
                    .run_waits
                    .fetch_add(1, Ordering::Relaxed);
            }
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
            w.cancel();
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
            let mut keys = std::mem::take(&mut self.to_freeze);
            // A tablet being split, merged or moved freezes whatever it holds, and so does a
            // slot pinning the WAL checkpoint (#137).
            for key in self.memtables.keys() {
                if (self.moving.contains(&key.0) || self.unpin.contains(key)) && !keys.contains(key)
                {
                    keys.push(*key);
                }
            }
            keys
        };
        let tablets_on = self.tablets_on();
        let visible = self.shared.shm.visible_seqno();
        let view = self.shared.view.load();
        let mut deferred = false;
        let mut starved = false;
        // The seqno that must be visible before every deferred memtable can freeze.
        let mut needed: Seqno = 0;
        for key in keys {
            let Some(slot) = self.memtables.get_mut(&key) else {
                continue;
            };
            if slot.seal != Seal::Open {
                continue;
            }
            let big = slot.active.table.allocated_bytes() >= threshold;
            trace!(
                "shard {} freeze {:?}: all={all} big={big} empty={} max_seqno={} visible={visible}",
                self.id.0,
                key,
                slot.active.table.is_empty(),
                slot.active.max_seqno()
            );
            let forced = self.moving.contains(&key.0) || self.unpin.contains(&key);
            if slot.active.table.is_empty() || !(all || big || forced) {
                continue;
            }
            if slot.active.max_seqno() > visible {
                deferred = true;
                needed = needed.max(slot.active.max_seqno());
                if !self.to_freeze.contains(&key) {
                    self.to_freeze.push(key);
                }
                continue;
            }
            let Some(meta) = view.catalog.family(key.1) else {
                continue;
            };
            let item = |m: &MemEntry, guard: Option<Arc<AtomicU8>>| FlushItem {
                table: meta.table,
                tablet: key.0,
                family: key.1,
                root: m.table.root(),
                reader: m.table.reader(),
                bytes: m.table.allocated_bytes() as u64,
                max_seqno: m.max_seqno(),
                has_shares: m.has_shares,
                guard,
                options: meta.options.clone(),
            };
            // The fresh active memtable takes a chunk: never one admitted commits reserved.
            if tablets_on
                && self.arena.free_bytes() < self.reserved.saturating_add(self.chunk_size)
                && let Ok(m) = Memtable::create(&mut self.arena)
            {
                // As `refresh_free`: the arena accounts for memtables released since.
                self.arena.reclaim(m.retire());
            }
            let reserved = tablets_on
                && self.arena.free_bytes() < self.reserved.saturating_add(self.chunk_size);
            let fresh = if reserved {
                trace!(
                    "shard {} freeze {:?}: the arena's free chunks are reserved",
                    self.id.0, key
                );
                None
            } else {
                Memtable::create(&mut self.arena).ok()
            };
            let Some(fresh) = fresh else {
                trace!(
                    "shard {} freeze {:?}: no chunk for a fresh memtable",
                    self.id.0, key
                );
                if self.close_stage == CloseStage::Flushing {
                    // Nothing commits on a closing shard any more: flush this one in place
                    // rather than wait for a chunk that live snapshots may hold until the
                    // close returns (issue #111).
                    slot.active.table.freeze();
                    let root = slot.active.table.root();
                    let guard = (!self.replaying
                        && !self.voided_roots.contains(&root)
                        && flush_guard(slot, root, &self.prepared, &view, key))
                    .then(|| guard_state(&mut self.guarded, root, key.1));
                    self.flush_queue.push(item(&slot.active, guard));
                    slot.seal = Seal::Flushing;
                }
                // Otherwise keep writing into this one; the arena-room check defers later
                // commits until a flush frees space, and a `flush` or `compact` caller
                // waits until this one freezes too.
                starved = true;
                continue;
            };
            if !(all || big) && self.unpin.contains(&key) {
                self.shared.metrics[usize::from(self.id.0)]
                    .unpin_flushes
                    .fetch_add(1, Ordering::Relaxed);
            }
            let mut old = std::mem::replace(&mut slot.active, MemEntry::new(fresh));
            old.table.freeze();
            let root = old.table.root();
            let guard = (!self.replaying
                && !self.voided_roots.contains(&root)
                && flush_guard(slot, root, &self.prepared, &view, key))
            .then(|| guard_state(&mut self.guarded, root, key.1));
            self.flush_queue.push(item(&old, guard));
            slot.frozen.insert(0, old);
            self.view_dirty = true;
        }
        // A forced slot is done once its active memtable froze (or is empty, or gone).
        let memtables = &self.memtables;
        self.unpin.retain(|k| {
            memtables
                .get(k)
                .is_some_and(|s| s.seal == Seal::Open && !s.active.table.is_empty())
        });
        self.freeze_all_pending = all && deferred;
        if all {
            self.starved_all = starved;
        }
        if deferred {
            // Register for the watermark kick every time the freeze defers, with the seqno it
            // waits for: a publish wakes this shard only once that seqno is visible (main's
            // flag-guarded registration was lost after the first wake, and a tablet change
            // waits on the freeze with every write to its tablet parked, so nothing else
            // would wake it). A watermark published since `visible` was read may have missed
            // the registration: look again.
            self.freeze_deferred = true;
            self.shared.freeze_waiters.register(self.id.0, needed);
            if self.shared.shm.visible_seqno() >= needed
                && let Some(subs) = self.shared.submitters.get()
            {
                let _ = subs[usize::from(self.id.0)].submit(ShardMsg::Kick);
            }
        } else {
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
        // A failed flush waits out its backoff (the timer's `RetryFlush` comes back here),
        // unless a `flush` or `compact` caller or a tablet change waits for it: a failure
        // answers those callers and gives the change up at once, so each retries at most
        // once and never loops.
        if self.flush_waiters.is_empty()
            && self.compact_all.is_empty()
            && self.op.is_none()
            && self.flush_retry.as_ref().is_some_and(|t| !t.finished())
        {
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
        if self.freeze_all_pending || self.starved_all || !self.to_freeze.is_empty() {
            return;
        }
        if self.flush_idle() {
            for w in self.flush_waiters.drain(..) {
                w.notify(Ok(()));
            }
        }
    }

    /// While a freeze of every memtable is starved of chunks and `flush` or `compact`
    /// callers wait for it, tries it again (a flush or a reclaim may have freed one), and
    /// refuses them with `Busy` once nothing frees one in time, as a write stall is (D124,
    /// D126): after `write_stall_timeout_nanos` on a moving clock, and at once on a frozen
    /// clock with nothing in flight that could free a chunk (in-process snapshots, which
    /// only the waiting callers can drop, hold the rest; issue #116).
    fn retry_starved_freeze(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        let waiting = !self.flush_waiters.is_empty() || !self.compact_all.is_empty();
        if !self.starved_all || !waiting || self.closing {
            if let Some(w) = self.starve_wait.take() {
                w.cancel();
            }
            if !waiting {
                self.starved_all = false;
            }
            return;
        }
        self.reclaim_retired();
        if self.freeze(true).is_ok() {
            self.spawn_flush(ctx);
        }
        if !self.starved_all {
            if let Some(w) = self.starve_wait.take() {
                w.cancel();
            }
            self.check_flush_waiters();
            return;
        }
        let now = ctx.now_nanos();
        let timeout = self.shared.write_stall_timeout_nanos;
        let arm = |since: u64, ctx: &mut ShardContext<'_, ShardMsg>| {
            let state = TimerState::new();
            ctx.spawn(Box::new(ClockTimer::new(
                &self.shared.vfs,
                since.saturating_add(timeout),
                Arc::clone(&state),
                ctx.submitter(self.id).clone(),
                ShardMsg::Kick,
            )));
            state
        };
        let (since, frozen) = match &self.starve_wait {
            None => {
                let timer = arm(now, ctx);
                self.starve_wait = Some(RoomWait::new(now, timer));
                (now, false)
            }
            // Either timer giving up on this reading means the clock stopped (#244).
            Some(w) => (w.since, w.stopped(now)),
        };
        if let Some(w) = &self.starve_wait
            && w.timer.finished()
            && !frozen
            && now.saturating_sub(since) < timeout
        {
            // A timer that gave up on a clock that has moved since is armed again.
            let timer = arm(since, ctx);
            if let Some(w) = &mut self.starve_wait {
                w.timer = timer;
            }
        }
        let idle = frozen
            && !self.flush_running
            && self.flush_queue.is_empty()
            && !self.freeze_deferred
            && self.retired.is_empty();
        if !frozen && let Some(w) = &mut self.starve_wait {
            // As for a room wait: freed chunks are announced to nobody (issue #141).
            w.arm_recheck(
                &self.shared.vfs,
                self.shared.room_recheck_nanos,
                ctx,
                self.id,
            );
        }
        if now.saturating_sub(since) >= timeout || idle {
            trace!(
                "shard {} starved freeze: refusing {} flush and {} compact callers \
                 (waited {} ns, frozen clock {frozen}, idle {idle})",
                self.id.0,
                self.flush_waiters.len(),
                self.compact_all.len(),
                now.saturating_sub(since)
            );
            if let Some(w) = self.starve_wait.take() {
                w.cancel();
            }
            self.starved_all = false;
            for w in self.flush_waiters.drain(..) {
                w.notify(Err(Error::Busy));
            }
            for (_, w) in self.compact_all.drain(..) {
                w.notify(Err(Error::Busy));
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
        // The guarded flushes this one carried are settled either way (#287). One a delete
        // voided was refused at its commit: flush those memtables again without the guard,
        // at once (not a failure). Members that waited on an install run again at the end of
        // this batch (`end_batch`).
        let mut voided = false;
        self.guarded.retain(|(root, _, state)| {
            if !items.iter().any(|i| i.root == *root) {
                return true;
            }
            if state.load(Ordering::Acquire) == GUARD_VOIDED {
                voided = true;
                self.voided_roots.insert(*root);
            }
            false
        });
        let result = match result {
            Err(_) if voided => {
                trace!(
                    "shard {} guarded flush voided by a delete; flushing again",
                    self.id.0
                );
                self.requeue_frozen();
                self.spawn_flush(ctx);
                return;
            }
            r => r,
        };
        match result {
            Ok(_) => {
                self.flush_failures = 0;
                for item in &items {
                    self.voided_roots.remove(&item.root);
                }
                if let Some(t) = self.flush_retry.take() {
                    t.cancel();
                }
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
                    let Some(slot) = self.memtables.get_mut(&key) else {
                        continue;
                    };
                    if let Some(pos) = slot.frozen.iter().position(|m| m.table.root() == item.root)
                    {
                        let m = slot.frozen.remove(pos);
                        self.retired.push((version, m.table.retire()));
                    } else if slot.seal == Seal::Flushing && slot.active.table.root() == item.root {
                        slot.seal = Seal::Flushed;
                    }
                    if slot.seal == Seal::Flushed && slot.frozen.is_empty() {
                        let slot = self.memtables.remove(&key).expect("present");
                        self.retired.push((version, slot.active.table.retire()));
                    }
                }
                // Idle slots keep their memtables here: retiring them after every flush
                // would churn a fresh chunk per slot each cycle, and a reader process's pin
                // keeps every retired chunk (#104). They retire under arena pressure only.
                self.reclaim_retired();
                self.report_shares_flushed(ctx);
                self.advance_checkpoint(ctx);
            }
            Err(e) => {
                // The frozen memtables stay (the WAL keeps their data) and are queued again;
                // no flush starts until a backoff (10 ms doubling to 1 s) passes, so a
                // device that keeps failing is not rewritten back to back, even while a
                // writer waits for room (issue #141). A poisoned pager stops flushing until
                // reopen; a failure while closing makes the close unclean.
                trace!("shard {} flush failed: {e}", self.id.0);
                self.shared.metrics[usize::from(self.id.0)]
                    .flush_failures
                    .fetch_add(1, Ordering::Relaxed);
                self.requeue_frozen();
                if !self.closing {
                    self.flush_failures = self.flush_failures.saturating_add(1);
                    if let Some(t) = self.flush_retry.take() {
                        t.cancel();
                    }
                    let state = TimerState::new();
                    self.flush_retry = Some(Arc::clone(&state));
                    ctx.spawn(Box::new(ClockTimer::new(
                        &self.shared.vfs,
                        ctx.now_nanos().saturating_add(backoff_nanos(
                            self.shared.flush_backoff_nanos,
                            FLUSH_BACKOFF_CAP,
                            self.flush_failures,
                        )),
                        state,
                        ctx.submitter(self.id).clone(),
                        ShardMsg::RetryFlush,
                    )));
                }
                // A tablet change waiting for this flush gives up (its tablets stay put).
                self.abort_op(crate::error::relay("flush", &e), ctx);
                // Whoever asked for this flush hears about the failure now rather than
                // waiting for a retry that may never come (a dead device).
                for w in self.flush_waiters.drain(..) {
                    w.notify(Err(crate::error::relay("flush", &e)));
                }
                // A full compaction starts with a flush: it fails with it.
                for (_, w) in self.compact_all.drain(..) {
                    w.notify(Err(crate::error::relay("flush", &e)));
                }
                if self.closing {
                    self.flush_failed = true;
                    self.shared.fail_close();
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
        if self.pending.is_empty() {
            // No group waits any more: the wait (and its timeout timer) ends here rather
            // than at the next group, which may never come (issue #88).
            self.end_room_wait(ctx.now_nanos());
        }
        self.end_backoff_on_event(ctx.now_nanos());
        self.check_flush_waiters();
        self.spawn_flush(ctx);
        self.maintain(ctx);
        self.progress_op(ctx);
        self.try_finish_close(ctx);
    }

    /// Queues every frozen memtable that is not in the manifest (after a failed flush).
    fn requeue_frozen(&mut self) {
        let view = self.shared.view.load();
        let mut items = Vec::new();
        for (&key, slot) in &self.memtables {
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
                    guard: (!self.replaying
                        && !self.voided_roots.contains(&m.table.root())
                        && flush_guard(slot, m.table.root(), &self.prepared, &view, key))
                    .then(|| guard_state(&mut self.guarded, m.table.root(), key.1)),
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
        // The balancer's samples and arrival passes of the dropped tablets go too (review
        // 5-6 6.4, #148): nothing else removes them.
        for t in tablets {
            self.loads.remove(t);
            self.arrived.remove(t);
        }
        if keys.is_empty() {
            return Ok(());
        }
        let mut retired = Vec::new();
        for key in keys {
            let slot = self.memtables.remove(&key).expect("listed");
            retired.push(slot.active.table.retire());
            retired.extend(slot.frozen.into_iter().map(|m| m.table.retire()));
        }
        self.publish_retired(retired)
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

    /// `preset` (a retried commit's first timestamp) when it still fits this shard's floor
    /// (`preset_fits`), else a fresh default timestamp.
    fn default_ts_or(&mut self, preset: Timestamp, own: &[ShardId]) -> Timestamp {
        if preset == 0 {
            return self.default_ts();
        }
        let raise = *self.shared.ts_raisers[usize::from(self.id.0)]
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if preset_fits(preset, own, self.id, self.ts_floor, raise) {
            self.raise_ts_floor(preset);
            return preset;
        }
        self.default_ts()
    }

    fn default_ts(&mut self) -> Timestamp {
        let now = self.shared.vfs.now_micros();
        // A shard that handed us a tablet raised our floor to its own (D11).
        let raised = self.shared.ts_raises[usize::from(self.id.0)]
            .0
            .load(Ordering::Acquire);
        self.ts_floor = self.ts_floor.max(raised);
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

    /// The sources of `(tablet, family)` for a point read at the applied state: `mems`
    /// (this shard's memtables not yet in SSTs) and the view's SSTs.
    fn point_sources(
        &self,
        view: &View,
        mems: &[MemtableReader],
        tablet: TabletId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
    ) -> Result<Vec<Source>> {
        let mut out = Vec::new();
        mem_sources_from(mems, &ScanFilter::all(), &mut out);
        let l = view.locate(self.id, tablet, family);
        if let Some(fam) = l.ssts
            && !fam.is_empty()
        {
            let mut key = ColumnKey::new(row, qualifier);
            let probe = Probe::new(&mut key)?;
            sst_sources_point::<false>(fam, &view.ssts, &probe, l.priority, &mut out)?;
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
        // The view is loaded and the memtables picked while `flushed_roots` is held: a
        // manifest commit adds a flushed memtable's root and stores the view with its SST
        // under that lock (issue #131), so the two never both hold its data, nor both miss it.
        let (view, mems) = {
            let flushed = self
                .shared
                .flushed_roots
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let view = self.shared.view.load_full();
            let mems = view
                .tablets()
                .route(table, row)
                .and_then(|(tablet, _)| self.memtables.get(&(tablet, family)))
                .map(|slot| slot.readers(self.id, &flushed))
                .unwrap_or_default();
            (view, mems)
        };
        let Some((tablet, _)) = view.tablets().route(table, row) else {
            return Ok(None);
        };
        let Some(meta) = view.catalog.family(family) else {
            return Ok(None);
        };
        let sources = self.point_sources(&view, &mems, tablet, family, row, qualifier)?;
        if sources.is_empty() {
            return Ok(None);
        }
        let mut opts = ResolveOptions::new(u64::MAX, self.shared.vfs.now_micros());
        opts.ttl_micros = meta.options.ttl_micros;
        opts.versions = 1;
        opts.merge = meta.merge_op.clone();
        opts.counter = meta.options.kind == FamilyKind::Counter;
        let resolver_blobs = crate::read::ResolverBlobs::attach(&mut opts, &view.ssts);
        let mut resolver = Resolver::new(MergingCursor::new(sources), opts);
        resolver.seek_column(row, qualifier)?;
        let next = resolver.next_cell();
        crate::read::ResolverBlobs::check(resolver_blobs.as_ref())?;
        let Some(cell) = next.map_err(|e| crate::read::read_error(e, meta))? else {
            return Ok(None);
        };
        // A separated value is compared as the value, not its pointer.
        Ok(Some(match view.ssts.read_blob(cell.value)? {
            Some(v) => v.to_vec(),
            None => cell.value.to_vec(),
        }))
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
        // The row whose escaped prefix `key_buf` starts with, and its length.
        let mut row_prefix: Option<(&[u8], usize)> = None;
        let mut key_buf = std::mem::take(&mut self.key_buf);
        let mut result = Ok(());
        self.touched_slots.clear();
        let track = !self.replaying
            && self.shared.balance.enabled
            && self.shared.balance.interval_nanos > 0;
        // The mutations as the scan decoded them (the batch is decoded once).
        let keys = std::mem::take(&mut self.dedup.keys);
        for (i, k) in keys.iter().enumerate() {
            let m = pigeonhole_format::wal::Mutation {
                table: TableId(k.table),
                family: FamilyId(k.family),
                kind: k.kind,
                row: span(bytes, k.row),
                qualifier: span(bytes, k.qualifier),
                ts: Some(k.ts),
                value: span(bytes, k.value),
            };
            if dups && !self.dedup.wins(i) {
                // A value separated at commit time that loses the collapse: its blob file
                // (its own, `large::separate`) holds nothing live; the commit's settle hook
                // releases it (#230). At replay the open sweep does.
                if !self.replaying
                    && m.kind == Kind::Put
                    && let Some(p) = pigeonhole_compaction::blob_pointer(m.value)
                {
                    self.shared
                        .large_dropped
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(p.blob_file);
                }
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
                    if track {
                        self.note_write(id, m.row);
                    }
                    // Before the write is inserted, so before it can be visible (D201).
                    if let Some(rc) = &self.shared.row_cache
                        && !self.replaying
                    {
                        rc.note_write(m.table, m.row, seqno);
                    }
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
            // The escaped row stays in `key_buf` across a run of writes to one row: only the
            // qualifier and suffix are encoded for each.
            let row_len = match row_prefix {
                Some((row, len)) if compare(row, m.row).is_eq() => len,
                _ => {
                    key_buf.clear();
                    if let Err(e) = encode_row_prefix(&mut key_buf, m.row) {
                        result = Err(e.into());
                        break;
                    }
                    row_prefix = Some((m.row, key_buf.len()));
                    key_buf.len()
                }
            };
            key_buf.truncate(row_len);
            if m.kind == Kind::FamilyDelete {
                encode_marker_after_row(&mut key_buf, ts, seqno);
            } else if let Err(e) =
                encode_key_after_row(&mut key_buf, m.qualifier, ts, seqno, m.kind)
            {
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
            if !matches!(m.kind, Kind::Put | Kind::Merge) {
                slot.active.deletes = true;
            }
            if !self.touched_slots.contains(&(tablet, m.family)) {
                self.touched_slots.push((tablet, m.family));
            }
            if slot.active.table.allocated_bytes() >= threshold
                && !self.to_freeze.contains(&(tablet, m.family))
            {
                self.to_freeze.push((tablet, m.family));
            }
        }
        self.dedup.keys = keys;
        if result.is_ok()
            && let Some(e) = self.dedup.error.take()
        {
            // Mutations before the undecodable one were applied, as they always were.
            result = Err(e.into());
        }
        self.key_buf = key_buf;
        result
    }

    // ---- stalls ----

    /// Whether the group must wait for the token bucket (L0 too deep). Spawns the timer
    /// that re-kicks the shard once a token is due.
    fn stalled(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) -> bool {
        let now = ctx.now_nanos();
        let frozen = self.stall.timer.as_ref().is_some_and(|t| t.frozen(now));
        // A stall lets compaction catch up; when none can run (the last one failed, the
        // pager or this shard is poisoned) holding writers would hold them for ever.
        let mut hopeless = self.due_backing_off
            || self.poisoned
            || self.shared.pager_poisoned.load(Ordering::Acquire);
        if !hopeless && self.stall.score >= 1.0 && self.compaction.is_none() {
            self.maintain(ctx);
            // On a frozen clock the bucket never refills: only a compaction's completion
            // can end the stall, so with none running writers are admitted (issue #70). A
            // moving clock keeps pacing them (D119).
            hopeless = self.due_backing_off || (frozen && self.compaction.is_none());
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
        // A timer that gave up on a clock that has not moved since is not armed again: the
        // running compaction's completion kicks the shard.
        if self
            .stall
            .timer
            .as_ref()
            .is_none_or(|t| t.finished() && !t.frozen(now))
        {
            let state = TimerState::new();
            self.stall.timer = Some(Arc::clone(&state));
            let wait = ((1.0 - self.stall.tokens) / rate * 1e9) as u64;
            ctx.spawn(Box::new(ClockTimer::new(
                &self.shared.vfs,
                now + wait.max(1_000),
                state,
                ctx.submitter(self.id).clone(),
                ShardMsg::Kick,
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
            // Nothing waits for room any more (the waiting commits were parked or
            // forwarded by a tablet change): the stall is over and its timer goes.
            if self.tablets_on() && self.room_wait.is_some() && !self.wait_room {
                self.end_room_wait(ctx.now_nanos());
            }
            return;
        }
        trace!(
            "shard {} run_group pending={} stall={}",
            self.id.0,
            self.pending.len(),
            self.stall.score
        );
        if self.stalled(ctx) {
            return;
        }
        self.refresh_tablets();
        self.end_backoff_on_event(ctx.now_nanos());
        let spare = self.member_buf();
        let mut members = std::mem::replace(&mut self.pending, spare);

        // Admission, in order. A conditional member whose row an earlier member of this
        // group already touched runs in the next group instead, with everything after it,
        // so conditions see the applied state and submission order holds per row.
        let mut admitted = self.member_buf();
        admitted.reserve(members.len());
        self.touched.clear();
        // Once a conditional member is held back, every later plain commit waits with it
        // (per-row submission order); two-phase-commit records never wait, since the share
        // the member waits for may need them to be decided.
        let mut cut = false;
        let mut need_room = false;
        // A member waits for a guarded flush's install: the shard wakes on its outcome.
        let mut guard_wait = false;
        for mut m in members.drain(..) {
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
            // Rows in a tablet being split, merged or moved wait for the change (a PREPARE is
            // refused and retried); rows another shard owns now go there.
            let Some(mut m) = self.route_member(m, ctx) else {
                continue;
            };
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
            if let Some((table, row, predicate)) = m.predicate.as_deref() {
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
            // A delete in a family whose memtable is being flushed under the guard (#287),
            // or a write below a running bottommost compaction's bound (#316), voids that
            // purge, or waits while its commit installs it.
            if (!self.guarded.is_empty() || self.compaction_guard.is_some())
                && !matches!(m.kind, MemberKind::CommitRecord { .. })
                && !self.admit_against_guards(&m)
            {
                // It waits for the install's outcome (`on_flushed`), a PREPARE too: the
                // install never waits on any commit's decision, so this cannot deadlock two
                // coordinators the way waiting on another share could (and a refusal would
                // park the coordinator until the tablet map changes).
                if matches!(m.kind, MemberKind::Single) {
                    cut = true;
                }
                guard_wait = true;
                self.pending.push(m);
                continue;
            }
            #[cfg(feature = "test-hooks")]
            if !admitted.is_empty()
                && matches!(m.kind, MemberKind::Single)
                && self
                    .shared
                    .hooks
                    .room_wait_once
                    .swap(false, Ordering::AcqRel)
            {
                cut = true;
                need_room = true;
                self.pending.push(m);
                continue;
            }
            if !matches!(m.kind, MemberKind::CommitRecord { .. }) {
                match self.reserve_room(m.bytes.as_slice()) {
                    Ok(bytes) => m.reserved = bytes,
                    Err(Room::Never) => {
                        // Not a stall: no flush could ever make room. The caller must split
                        // the batch or raise `memtable_budget` (issue #141).
                        m.failed = Some(Error::BatchTooLarge);
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
        self.return_member_buf(members);
        if need_room {
            self.wait_room = true;
            let now = ctx.now_nanos();
            let timeout = self.shared.write_stall_timeout_nanos;
            // The clock has stopped if either of the wait's timers gave up on this reading.
            // The re-check timer counts too (#244): the timeout timer may have seen the clock
            // move and be asleep towards a deadline the clock never reaches, and a shard
            // kicked by each re-check never goes idle for it to be polled and notice.
            let frozen = self.room_wait.as_ref().is_some_and(|w| w.stopped(now));
            let mut refuse = false;
            match &self.room_wait {
                None => {
                    // The stall begins: count it, and arm the timeout.
                    let metrics = &self.shared.metrics[usize::from(self.id.0)];
                    metrics.stalls.fetch_add(1, Ordering::Relaxed);
                    let state = TimerState::new();
                    ctx.spawn(Box::new(ClockTimer::new(
                        &self.shared.vfs,
                        now.saturating_add(timeout),
                        Arc::clone(&state),
                        ctx.submitter(self.id).clone(),
                        ShardMsg::Kick,
                    )));
                    self.room_wait = Some(RoomWait::new(now, state));
                }
                Some(w)
                    if now.saturating_sub(w.since) >= timeout
                        || (frozen && w.failed_flushes >= ROOM_FLUSH_ATTEMPTS) =>
                {
                    // Nothing freed room in time (D124), or on a frozen clock, which never
                    // reaches the timeout, the flushes keep failing (issue #70).
                    refuse = true;
                }
                Some(w) => {
                    // A timer that gave up on a clock that has moved since is armed again.
                    if w.timer.finished() && !frozen {
                        let state = TimerState::new();
                        ctx.spawn(Box::new(ClockTimer::new(
                            &self.shared.vfs,
                            w.since.saturating_add(timeout),
                            Arc::clone(&state),
                            ctx.submitter(self.id).clone(),
                            ShardMsg::Kick,
                        )));
                        if let Some(w) = &mut self.room_wait {
                            w.timer = state;
                        }
                    }
                }
            }
            // A flush frees room (one that failed is tried again), and so do empty slots.
            if self.retire_idle_slots().is_err() {
                self.poisoned = true;
            }
            let _ = self.freeze(true);
            // That freeze may have queued guarded flushes after this group's members were
            // admitted but before they are applied (below, into the fresh active memtables):
            // their deletes void those guards now. Nothing claims a fresh guard before the
            // flush task runs, after this call.
            for m in &admitted {
                if !matches!(m.kind, MemberKind::CommitRecord { .. }) {
                    let admitted_again = self.admit_against_guards(m);
                    debug_assert!(admitted_again, "a fresh guard is never installing");
                }
            }
            self.spawn_flush(ctx);
            // On a frozen clock, with no flush, deferred freeze, undecided share or unsynced
            // group in flight and no retired memtable a reader process still pins, nothing
            // the shard will hear of can free room (in-process snapshots hold what is left)
            // and the timeout never comes: refuse now (issue #70). A moving clock waits for
            // the timeout (D124), since a reader may release room meanwhile.
            let idle = frozen
                && !self.flush_running
                && self.flush_queue.is_empty()
                && !self.freeze_deferred
                && self.prepared.is_empty()
                && self.unresolved.is_empty()
                && self.retired.is_empty();
            trace!(
                "shard {} room wait: frozen={frozen} idle={idle} refuse={refuse}",
                self.id.0
            );
            if !refuse && !idle && !frozen {
                // Room freed by a snapshot dropped on another thread or a reader process's
                // unpin is announced to nobody: look again soon (issue #141).
                if let Some(w) = &mut self.room_wait {
                    w.arm_recheck(
                        &self.shared.vfs,
                        self.shared.room_recheck_nanos,
                        ctx,
                        self.id,
                    );
                }
            }
            if refuse || idle {
                // Refuse the waiting members; this group's admitted ones still apply.
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
            }
        } else {
            if let Some(w) = &self.room_wait
                && !self.wait_room
            {
                let _ = w;
                self.end_room_wait(ctx.now_nanos());
            }
            // A member waiting for a guarded flush's install is woken by its outcome
            // (`on_flushed`, then `end_batch`), not by kicking the shard round the loop for
            // the whole commit.
            if !self.pending.is_empty() && !guard_wait {
                let _ = ctx.submitter(self.id).submit(ShardMsg::Kick);
            }
        }
        if admitted.is_empty() {
            self.return_member_buf(admitted);
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
                    m.commit_ts = self.default_ts_or(m.commit_ts, &m.preset_own);
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
            // The commit point of a value separated at commit time: a record the stream
            // accepted (even one whose write then fails: it may have landed) may be applied
            // or replayed, so its blob files must stay (#230).
            if self.shared.large_open.load(Ordering::Acquire) > 0
                && !matches!(
                    result,
                    Err(pigeonhole_wal::Error::RecordTooLarge
                        | pigeonhole_wal::Error::InvalidArgument { .. })
                )
                && let WalRecord::Batch { batch, .. } | WalRecord::Prepare { batch, .. } = &record
            {
                crate::large::note_logged(&self.shared, *batch);
            }
            // Test hook: the append order. A failed write may still have landed (a crash
            // or an I/O error mid-write), so every attempt the stream accepted counts.
            #[cfg(feature = "test-hooks")]
            if self.shared.hooks.record.load(Ordering::Acquire)
                && !matches!(
                    result,
                    Err(pigeonhole_wal::Error::RecordTooLarge
                        | pigeonhole_wal::Error::InvalidArgument { .. })
                )
            {
                use crate::engine::hooks::{AppendedKind, AppendedRecord};
                self.shared
                    .hooks
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
                        commit_ts: m.commit_ts,
                    });
            }
            match result {
                Ok(t) => {
                    m.ticket = Some(t);
                    // Every ticketed PREPARE or COMMIT counts, even one whose group fails
                    // below (`fail_all`): a COMMIT with a ticket still decides commit, so
                    // peers' barriers must wait for it to be durable.
                    if !matches!(m.kind, MemberKind::Single) {
                        self.cross_end = self.cross_end.max(t.end);
                    }
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
                    #[cfg(feature = "test-hooks")]
                    let applied = if self
                        .shared
                        .hooks
                        .fail_next_apply
                        .swap(false, Ordering::AcqRel)
                    {
                        Err(Error::Busy)
                    } else {
                        self.apply(m.bytes.as_slice(), m.seqno, m.commit_ts)
                    };
                    #[cfg(not(feature = "test-hooks"))]
                    let applied = self.apply(m.bytes.as_slice(), m.seqno, m.commit_ts);
                    if let Err(e) = applied {
                        trace!("shard {} apply of {} failed: {e}", self.id.0, m.seqno);
                        self.poisoned = true;
                        m.failed = Some(e);
                    }
                    self.release_room(m.reserved);
                    m.reserved = 0;
                    if let Some(t) = m.ticket {
                        let kind = LoggedKind::Single {
                            slots: self.touched_slots.iter().copied().collect(),
                        };
                        self.log_record(t.end, m.seqno, m.bytes.as_slice().len(), kind);
                    }
                }
                MemberKind::Prepare { coordinator } => {
                    // The share keeps its reservation until the decision.
                    if let Some(share) = self.prepared.get_mut(&m.seqno) {
                        share.reserved = m.reserved;
                        m.reserved = 0;
                    }
                    if let Some(t) = m.ticket {
                        let slots = self.slots_of(m.bytes.as_slice());
                        let kind = LoggedKind::Prepare {
                            slots,
                            coordinator: *coordinator,
                        };
                        self.log_record(t.end, m.seqno, m.bytes.as_slice().len(), kind);
                    }
                }
                MemberKind::CommitRecord { participants } => {
                    if let Some(t) = m.ticket {
                        self.share_reports.entry(m.seqno).or_insert((0, 0)).1 = participants.len();
                        let kind = LoggedKind::Commit {
                            participants: participants.clone(),
                        };
                        self.log_record(t.end, m.seqno, 0, kind);
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
        self.limit_wal_pin(ctx);
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

    /// Appends a logged record of `len` payload bytes to `log`.
    fn log_record(&mut self, end: Lsn, seqno: Seqno, len: usize, kind: LoggedKind) {
        self.last_end = Some(end);
        let pos = self.log_bytes;
        self.log_bytes += len as u64 + LOGGED_OVERHEAD;
        self.log.push_back(Logged {
            end,
            pos,
            seqno,
            kind,
        });
    }

    /// Bounds the bytes the checkpoint cannot pass (#137). A slot written once and never
    /// again neither reaches the freeze threshold nor, while other slots flush and free the
    /// arena, gets frozen by a full arena, so its records would keep the checkpoint (and the
    /// WAL, `log` and the next open's replay) growing for the life of the process. Once the
    /// log holds more than `wal_pin_bytes` past its oldest needed record, every slot the
    /// records in the oldest half of that span wrote is flushed whatever its size, and the
    /// other shards holding shares of its cross-shard commits are asked to do the same. One
    /// pass per limit's worth of new bytes, unless the checkpoint moves past the last pass.
    fn limit_wal_pin(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        let limit = self.shared.wal_pin_bytes;
        let Some(front) = self.log.front() else {
            return;
        };
        if limit == 0 || self.closing || self.log_bytes - front.pos <= limit {
            return;
        }
        if front.pos < self.unpin_upto && self.log_bytes < self.unpin_at.saturating_add(limit) {
            // The last pass is still flushing.
            return;
        }
        let upto = self.log_bytes - limit / 2;
        trace!(
            "shard {} unpin: front at {} of {} bytes, forcing below {upto}",
            self.id.0, front.pos, self.log_bytes
        );
        self.unpin_upto = upto;
        self.unpin_at = self.log_bytes;
        self.shared.metrics[usize::from(self.id.0)]
            .unpin_passes
            .fetch_add(1, Ordering::Relaxed);
        self.force_pinning(|l| l.pos < upto, true, ctx);
    }

    /// Marks for flushing every unflushed slot of the needed records `which` selects, and
    /// sends `Unpin` to the shards their cross-shard commits wait for: always from this
    /// shard's own pass (`origin`), and only to shards not yet asked about that record when
    /// answering another shard's `Unpin`, so the requests between a coordinator and its
    /// participants stop after one round.
    fn force_pinning(
        &mut self,
        which: impl Fn(&Logged) -> bool,
        origin: bool,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        let Ok(view) = self.checkpoint_view() else {
            return;
        };
        let catalog = view.as_ref().map(|v| &*v.catalog);
        let mut wanted: Vec<(ShardId, Seqno)> = Vec::new();
        let mut slots = Vec::new();
        for l in self.log.iter().filter(|l| which(l)) {
            if !self.needed(catalog, l) {
                continue;
            }
            match &l.kind {
                LoggedKind::Single { slots: s } => slots.extend(
                    s.iter()
                        .filter(|s| !self.slot_flushed(catalog, s, l.seqno))
                        .copied(),
                ),
                LoggedKind::Prepare {
                    slots: s,
                    coordinator,
                } => {
                    slots.extend(
                        s.iter()
                            .filter(|s| !self.slot_flushed(catalog, s, l.seqno))
                            .copied(),
                    );
                    // The coordinator's checkpoint must pass the COMMIT too.
                    wanted.push((*coordinator, l.seqno));
                }
                LoggedKind::Commit { participants } => {
                    wanted.extend(participants.iter().map(|&p| (p, l.seqno)));
                }
            }
        }
        self.unpin.extend(slots);
        let mut asks: BTreeMap<ShardId, Seqno> = BTreeMap::new();
        for (shard, seqno) in wanted {
            if shard == self.id {
                continue;
            }
            let sent = self.asked.entry(seqno).or_default();
            if sent.contains(&shard) {
                if !origin {
                    continue;
                }
            } else {
                sent.push(shard);
            }
            let through = asks.entry(shard).or_insert(seqno);
            *through = (*through).max(seqno);
        }
        for (shard, through) in asks {
            self.send(shard, ShardMsg::Unpin { through }, ctx);
        }
    }

    /// Another shard's checkpoint waits for cross-shard commits up to `through`. Every
    /// request is served (requests carry no order: a commit decided later may have a lower
    /// seqno), so the only dedupe is `force_pinning`'s per record and shard.
    fn on_unpin(&mut self, through: Seqno, ctx: &mut ShardContext<'_, ShardMsg>) {
        if self.closing {
            return;
        }
        self.shared.metrics[usize::from(self.id.0)]
            .unpin_passes
            .fetch_add(1, Ordering::Relaxed);
        self.force_pinning(|l| l.seqno <= through, false, ctx);
        let shares: Vec<(TabletId, FamilyId)> = self
            .unreported
            .iter()
            .filter(|u| u.seqno <= through)
            .flat_map(|u| u.slots.iter().copied())
            .collect();
        self.unpin.extend(shares);
        if self.freeze(false).is_err() {
            self.poisoned = true;
        }
        self.spawn_flush(ctx);
    }

    fn maybe_prepare_spares(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        let Some(spares) = &self.spares else {
            return;
        };
        // Not before the stream is half way through the segment it opened in: a small
        // database never pays for spares, and a busy one has half a segment of writes to
        // prepare them in before the first rollover needs one (#143).
        let due = self.wal.as_ref().is_some_and(|w| {
            let at = w.written();
            at.epoch() != self.open_epoch || 2 * u64::from(at.offset()) >= spares.segment_size()
        });
        if !due
            || spares.ready() >= spares.target()
            || self.spares_running.swap(true, Ordering::AcqRel)
        {
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
        let mut members = group.members;
        for m in members.drain(..) {
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
        self.return_member_buf(members);
        self.try_finish_close(ctx);
    }

    /// An empty member vector, from the spares if one is kept.
    fn member_buf(&mut self) -> Vec<Member> {
        self.member_bufs.pop().unwrap_or_default()
    }

    /// Keeps an emptied member vector for reuse (a few, none oversized).
    fn return_member_buf(&mut self, buf: Vec<Member>) {
        debug_assert!(buf.is_empty());
        if self.member_bufs.len() < 4 && buf.capacity() <= 256 {
            self.member_bufs.push(buf);
        }
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
        if let Some((table, row, _)) = m.predicate.as_deref() {
            check(hash_row(*table, row));
        }
        (same, pending)
    }

    /// Records `m`'s written rows as touched by this group.
    /// Uses the row hashes `reserve_room` left for `m`, which admission sizes just before.
    fn touch_rows(&mut self, m: &Member) {
        if matches!(m.kind, MemberKind::CommitRecord { .. }) {
            return;
        }
        self.touched.extend(self.admit_rows.iter().copied());
    }

    /// Counts or releases the rows of a prepared share.
    /// Checks a member against the guarded flushes (#287) and the running compaction's
    /// guard (#316): voids every guard it touches that has not started installing, and
    /// returns false (wait) if one has. A flush's guard is touched by a delete in its family;
    /// the compaction's by any write in its family whose timestamp may land below the
    /// guard's bound and at most its newest input timestamp. The member's seqno is above every
    /// entry the purge read, and it becomes visible only after admission, so it either voids
    /// the purge or lands after the install.
    fn admit_against_guards(&self, m: &Member) -> bool {
        #[cfg(feature = "test-hooks")]
        if self.shared.hooks.flush_gc_mutation.load(Ordering::Acquire) == 3 {
            return true;
        }
        let Ok(batch) = BatchRef::new(m.bytes.as_slice()) else {
            // An undecodable batch fails its apply; it writes nothing.
            return true;
        };
        // The least timestamp a mutation without one gets: for a single commit its preset if
        // that still fits the floor, else a fresh default above the floor (`default_ts_or`);
        // a share's commit timestamp, which may be below this shard's floor (D11 refuses
        // that only with tablet changes on).
        let default_ts = match m.kind {
            MemberKind::Single if m.commit_ts == 0 => self.ts_floor + 1,
            MemberKind::Single => m.commit_ts.min(self.ts_floor + 1),
            _ => m.commit_ts,
        };
        let mut families: Vec<FamilyId> = Vec::new();
        let mut below = false;
        for mu in batch.iter().flatten() {
            if !matches!(mu.kind, Kind::Put | Kind::Merge) && !families.contains(&mu.family) {
                families.push(mu.family);
            }
            if let Some(g) = &self.compaction_guard
                && mu.family == g.family
            {
                // The purge drops only input entries below `min_ts_above` (a column's versions
                // only when its newest input is below it), so a write at or above that bound
                // is one it allowed for. A write above every input is a newer version, a
                // delete hiding no input, or one hiding them all, the same with or without
                // the purge.
                let ts = mu.ts.unwrap_or(default_ts);
                below |= ts < g.min_ts_above && ts <= g.max_ts;
            }
        }
        let flushes = self
            .guarded
            .iter()
            .filter(|(_, family, _)| families.contains(family))
            .map(|(_, _, state)| state);
        let compaction = self
            .compaction_guard
            .as_ref()
            .filter(|_| below)
            .map(|g| &g.state);
        for state in flushes.chain(compaction) {
            match state.compare_exchange(
                GUARD_IN_FLIGHT,
                GUARD_VOIDED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) | Err(GUARD_VOIDED) => {}
                Err(_) => return false,
            }
        }
        true
    }

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
                    self.mark_aborted(m.seqno);
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
                    self.mark_aborted(m.seqno);
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
        let shards: Vec<ShardId> = req.parts.iter().map(|(s, _)| *s).collect();
        trace!(
            "shard {} coordinates {seqno} over {shards:?} routed with map v{}",
            self.id.0, req.map_version
        );
        // Above every participant's floor too, including the floor a shard handing it a
        // tablet raised it to (`ts_raises`): a tablet's default timestamps never go
        // backwards whichever shard coordinates a commit touching it (D11). A retry keeps its
        // first timestamp only when it is above every participant's floor, or equal to the
        // floor of a shard that reached it through this very commit (`Preset::own`): any
        // other tie would be a write that participant already made at that timestamp.
        let mut floor = 0;
        let mut fits = req.commit_ts.is_some();
        if self.tablets_on() {
            for s in &shards {
                let i = usize::from(s.0);
                let own_floor = self.shared.ts_floors[i].0.load(Ordering::Acquire);
                let raise = *self.shared.ts_raisers[i]
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                floor = floor.max(own_floor).max(raise.0);
                fits &= req
                    .commit_ts
                    .as_ref()
                    .is_some_and(|p| preset_fits(p.ts, &p.own, *s, own_floor, raise));
            }
        }
        let attempts = req.attempts;
        let (commit_ts, own) = match req.commit_ts {
            Some(p) if fits => {
                self.raise_ts_floor(p.ts);
                let mut own = p.own;
                if !own.contains(&self.id) {
                    own.push(self.id);
                }
                (p.ts, own)
            }
            _ => {
                self.ts_floor = self.ts_floor.max(floor);
                (self.default_ts(), vec![self.id])
            }
        };
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
                // Kept for a retry only when a participant may refuse with `Moved`.
                parts: if self.tablets_on() {
                    req.parts.clone()
                } else {
                    Vec::new()
                },
                validate: if self.tablets_on() {
                    req.validate.clone()
                } else {
                    None
                },
                map_version: req.map_version,
                moved: false,
                below_floor: false,
                commit_ts,
                own,
                epoch: self.shared.tablet_epoch.load(Ordering::Acquire),
                attempts,
            },
        );
        for (shard, bytes) in req.parts {
            let msg = ShardMsg::Prepare(PrepareReq {
                map_version: req.map_version,
                seqno,
                commit_ts,
                coordinator: self.id,
                bytes,
                durability: req.durability,
                validate: req.validate.clone(),
                participants: shards.clone(),
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
                participants: req.participants,
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
            map_version: req.map_version,
            preset_own: Vec::new(),
            attempts: 0,
        });
    }

    fn on_prepared(
        &mut self,
        seqno: Seqno,
        from: ShardId,
        error: Option<PrepareError>,
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        trace!(
            "shard {} on_prepared {seqno} from {} {error:?}",
            self.id.0, from.0
        );
        let Some(c) = self.coord.get_mut(&seqno) else {
            return;
        };
        match error {
            Some(PrepareError::Moved) => c.moved = true,
            Some(PrepareError::BelowFloor) => {
                c.moved = true;
                c.below_floor = true;
            }
            Some(e) if c.failed.is_none() => c.failed = Some(e.into()),
            Some(_) => {}
            // It raised its floor to the commit timestamp.
            None => {
                if !c.own.contains(&from) {
                    c.own.push(from);
                }
            }
        }
        c.prepared += 1;
        if c.prepared < c.participants || c.decided {
            return;
        }
        c.decided = true;
        if c.failed.is_some() || c.moved {
            let shards = c.shards.clone();
            self.mark_aborted(seqno);
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
            self.mark_aborted(seqno);
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
            map_version: 0,
            preset_own: Vec::new(),
            attempts: 0,
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
                self.mark_aborted(seqno);
                self.advance_checkpoint(ctx);
            }
            // A conditional member deferred behind this share may run now.
            if !self.pending.is_empty() {
                let _ = ctx.submitter(self.id).submit(ShardMsg::Kick);
            }
            self.progress_op(ctx);
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
        trace!(
            "shard {} on_applied {seqno}: {}/{} moved={}",
            self.id.0, c.applied, c.participants, c.moved
        );
        if c.applied < c.participants {
            return;
        }
        let mut c = self.coord.remove(&seqno).expect("present");
        self.held.remove(&seqno);
        self.publish_watermark();
        if c.moved
            && c.failed.is_none()
            && let Some(reply) = c.reply.take()
        {
            // A participant was splitting, merging or moving a tablet: the commit was
            // aborted everywhere and runs again once the tablet map has changed. One that
            // refused the timestamp as below its floor runs again at once, with a fresh one.
            // Either gives up once it was refused too often or waited too long (#102).
            let attempts = c.attempts.saturating_add(1);
            if moved_gives_up(
                attempts,
                ctx.now_nanos().saturating_sub(c.submitted_at),
                self.shared.write_stall_timeout_nanos,
            ) {
                trace!(
                    "shard {} gives up on {seqno} after {attempts} attempts",
                    self.id.0
                );
                reply.notify(Err(Error::Busy));
                self.try_finish_close(ctx);
                return;
            }
            let req = CoordinateReq {
                parts: std::mem::take(&mut c.parts),
                durability: c.durability,
                reply,
                submitted_at: c.submitted_at,
                validate: c.validate.take(),
                map_version: c.map_version,
                commit_ts: (!c.below_floor).then(|| Preset {
                    ts: c.commit_ts,
                    own: std::mem::take(&mut c.own),
                }),
                epoch: c.epoch,
                attempts,
            };
            if c.below_floor {
                self.retry(req, ctx);
            } else {
                self.retries.push(req);
                self.run_retries(ctx);
            }
            self.try_finish_close(ctx);
            return;
        }
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

    /// Whether the checkpoint may not pass `l` yet. Slots are checked against the catalog's
    /// flushed seqnos, not this shard's: after a move or a reopen a slot's records may sit
    /// in this stream while another shard owns and flushes it. A slot whose tablet is gone
    /// (its table dropped, or retired by a split or merge once everything in it was flushed)
    /// needs nothing.
    fn needed(&self, catalog: Option<&Catalog>, l: &Logged) -> bool {
        let unflushed = |slots: &[(TabletId, FamilyId)]| {
            slots
                .iter()
                .any(|s| !self.slot_flushed(catalog, s, l.seqno))
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
            LoggedKind::Prepare { slots, .. } => {
                !self.aborted.contains(&l.seqno)
                    && (unflushed(slots)
                        || !(self.commit_ckpt.contains(&l.seqno) || commit_done(l.seqno)))
            }
            LoggedKind::Commit { .. } => !self.aborted.contains(&l.seqno) && !commit_done(l.seqno),
        }
    }

    /// Whether every write to `slot` up to `seqno` is in SSTs, or the slot is gone. With
    /// tablet changes on, `catalog` is the published catalog: after a move or a reopen a
    /// slot's records may sit in this stream while another shard owns and flushes it, and a
    /// tablet retired by a split or merge (or dropped) needs nothing. Otherwise this shard's
    /// own flushed seqnos decide.
    fn slot_flushed(
        &self,
        catalog: Option<&Catalog>,
        s: &(TabletId, FamilyId),
        seqno: Seqno,
    ) -> bool {
        match catalog {
            Some(c) => c.flushed_through(s.0, s.1).is_none_or(|f| f >= seqno),
            None => {
                self.dropped.contains(&s.0) || self.flushed.get(s).copied().unwrap_or(0) >= seqno
            }
        }
    }

    /// The published view when tablet changes are on (see `slot_flushed`), `Err` while still
    /// opening (the catalog is not published yet).
    fn checkpoint_view(&self) -> std::result::Result<Option<Arc<View>>, ()> {
        if !self.tablets_on() {
            return Ok(None);
        }
        let view = self.shared.view.load_full();
        if view.version == 0 {
            Err(())
        } else {
            Ok(Some(view))
        }
    }

    /// Tells coordinators about applied shares whose slots are all in SSTs now.
    fn report_shares_flushed(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        let Ok(view) = self.checkpoint_view() else {
            return;
        };
        let catalog = view.as_ref().map(|v| &*v.catalog);
        trace!(
            "shard {} report shares: flushed={:?} unreported={:?}",
            self.id.0, self.flushed, self.unreported
        );
        let mut i = 0;
        while i < self.unreported.len() {
            let u = &self.unreported[i];
            let done = u
                .slots
                .iter()
                .all(|s| self.slot_flushed(catalog, s, u.seqno));
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
        let Ok(view) = self.checkpoint_view() else {
            return;
        };
        let catalog = view.as_ref().map(|v| &*v.catalog);
        while let Some(front) = self.log.front() {
            if self.needed(catalog, front) {
                break;
            }
            let l = self.log.pop_front().expect("checked");
            self.checkpoint_candidate = self.checkpoint_candidate.max(l.end);
            self.aborted.remove(&l.seqno);
            self.asked.remove(&l.seqno);
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
        if self.log.is_empty() {
            // No record left names a dropped tablet (a table created again gets new ones):
            // `dropped` has nothing more to answer (review 5-6 6.4, #148).
            self.dropped.clear();
            if let Some(end) = self.last_end {
                self.checkpoint_candidate = self.checkpoint_candidate.max(end);
            }
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
        let failed = result.is_err();
        match result {
            Ok(_) => {
                self.checkpoint_failures = 0;
                if let Some(t) = self.checkpoint_timer.take() {
                    t.cancel();
                }
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
                // Retry at the next checkpoint event, or once a backoff passes; the commits
                // passed stay queued.
                trace!("shard {} checkpoint failed: {e}", self.id.0);
                self.passed_commits.extend(commits);
                self.checkpoint_dirty = true;
                if self.closing {
                    self.shared.fail_close();
                } else if !self.shared.pager_poisoned.load(Ordering::Acquire) {
                    self.back_off_checkpoint(ctx);
                }
            }
        }
        // A refused checkpoint (a full disk) is not retried at once, which would spin while
        // the disk stays full; the next checkpoint event or the backoff timer retries it.
        if self.checkpoint_dirty && !failed && !self.shared.pager_poisoned.load(Ordering::Acquire) {
            self.checkpoint_dirty = false;
            self.advance_checkpoint(ctx);
        }
        self.try_finish_close(ctx);
    }

    /// A checkpoint was refused (the manifest found no space): retry after the compaction
    /// backoff for as many refusals in a row (1 s doubling to 60 s), so an idle shard
    /// recycles its WAL segments once space is freed, without spinning while it is not.
    fn back_off_checkpoint(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        self.checkpoint_failures = self.checkpoint_failures.saturating_add(1);
        if let Some(t) = self.checkpoint_timer.take() {
            t.cancel();
        }
        let state = TimerState::new();
        self.checkpoint_timer = Some(Arc::clone(&state));
        ctx.spawn(Box::new(ClockTimer::new(
            &self.shared.vfs,
            ctx.now_nanos().saturating_add(backoff_nanos(
                self.shared.compaction_backoff_nanos,
                COMPACTION_BACKOFF_CAP,
                self.checkpoint_failures,
            )),
            state,
            ctx.submitter(self.id).clone(),
            ShardMsg::RetryCheckpoint,
        )));
    }

    // ---- compaction ----

    /// The families of the tablets this shard owns, with their SSTs in `view`.
    fn owned_slots(&self, view: &View) -> Vec<(TabletId, FamilyId)> {
        let mut out = Vec::new();
        for t in view.tablets.iter() {
            // A tablet being split, merged or moved starts no compaction.
            if t.shard != self.id || self.moving.contains(&t.id) {
                continue;
            }
            for f in view.catalog.family_ids_of(t.table) {
                out.push((t.id, f));
            }
        }
        out
    }

    /// Whether the test hooks record history (`Engine::record_history`; never without the
    /// `test-hooks` feature).
    fn recording(&self) -> bool {
        #[cfg(feature = "test-hooks")]
        {
            self.shared.hooks.record.load(Ordering::Acquire)
        }
        #[cfg(not(feature = "test-hooks"))]
        {
            false
        }
    }

    /// The picker for a family compacting by `style`.
    fn picker(&self, style: CompactionStyle) -> &CompactionPicker {
        match style {
            CompactionStyle::Leveled => &self.leveled,
            CompactionStyle::Tiered => &self.tiered,
            CompactionStyle::FifoByTime => &self.fifo,
        }
    }

    /// Scores the shard's slots, refreshes the stall score (L0 depth only, D119) and starts
    /// the most urgent compaction (or the next step of a full compaction) if none is running.
    fn maintain(&mut self, ctx: &mut ShardContext<'_, ShardMsg>) {
        if self.replaying {
            return;
        }
        let view = self.shared.view.load_full();
        let now = self.shared.vfs.now_micros();
        // The write stall's score: the highest L0 depth over its trigger among the slots.
        let mut score = 0.0f64;
        // Slots that need a compaction, most urgent first.
        let mut due: Vec<(f64, (TabletId, FamilyId))> = Vec::new();
        // The earliest future expiry of a FIFO-by-time slot's SST (#232).
        let mut next_expiry: Option<Timestamp> = None;
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
            let picker = self.picker(meta.options.compaction);
            let levels = fam.levels_meta();
            let s = picker.score_at(&levels, now, meta.options.ttl_micros);
            score = score.max(picker.stall_score(&levels));
            if let Some(at) = picker.next_expiry(&levels, meta.options.ttl_micros)
                && at > now
            {
                next_expiry = Some(next_expiry.map_or(at, |e: Timestamp| e.min(at)));
            }
            if s >= 1.0 {
                due.push((s, key));
            }
        }
        due.sort_by(|a, b| b.0.total_cmp(&a.0));
        let now_nanos = ctx.now_nanos();
        // Backoffs of slots this shard no longer has (a dropped table, a tablet merged away
        // or moved off) go (issue #141).
        if !self.slot_backoff.is_empty() {
            let id = self.id;
            self.slot_backoff.retain(|k, _| {
                view.tablets.entry(k.0).is_some_and(|t| t.shard == id)
                    && view.catalog.family(k.1).is_some()
            });
        }
        // A stall waits for compaction only while some due slot may compact (D119).
        self.due_backing_off = !due.is_empty()
            && due.iter().all(|(_, k)| {
                self.slot_backoff
                    .get(k)
                    .is_some_and(|&(_, until)| until > now_nanos)
            });
        trace!(
            "shard {} maintain: score={score:.2} best={:?} compaction={:?} full_waiters={}",
            self.id.0,
            due.first(),
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
        // An idle FIFO-by-time family drops an SST when it expires, not at its next flush
        // (#232): the compaction retry timer fires then. On a stopped clock that timer gave
        // up and is not armed again until the clock moves (D126, D161).
        if let Some(at) = next_expiry
            && !self
                .backoff_timer
                .as_ref()
                .is_some_and(|(t, _)| t.frozen(now_nanos))
        {
            let wait = at.saturating_sub(now).saturating_mul(1000);
            self.arm_compaction_retry(now_nanos.saturating_add(wait), ctx);
        }
        // Full compactions first (a caller waits), one slot at a time.
        while let Some((filter, _)) = self.compact_all.front() {
            if !self.flush_idle() || self.freeze_all_pending || self.starved_all {
                self.spawn_flush(ctx);
                return;
            }
            let filter = *filter;
            let last = self.shared.picker.max_levels.max(2) - 1;
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
                let Some(tablet) = view.tablets.entry(key.0) else {
                    continue;
                };
                let rewrite = self.tablets_on();
                if let Some(mut t) = compact::plan_full(
                    tablet,
                    key.1,
                    &fam.levels_meta(),
                    last,
                    &busy,
                    rewrite,
                    self.shared.picker.target_sst_bytes,
                ) {
                    self.blob_gc.full(&view.catalog, &mut t);
                    task = Some((key, t));
                    break;
                }
            }
            match task {
                Some((key, task)) => match self.start_compaction(&view, key, task, ctx) {
                    Ok(()) => {
                        self.compaction_full = true;
                        return;
                    }
                    Err(e) => {
                        let (_, reply) = self.compact_all.pop_front().expect("front");
                        reply.notify(Err(e));
                    }
                },
                None => {
                    // Every slot is fully compacted. A background failure meanwhile is not
                    // this caller's (issue #141): its own failures were reported at once.
                    let (_, reply) = self.compact_all.pop_front().expect("front");
                    reply.notify(Ok(()));
                }
            }
        }
        // A slot whose compaction failed is not picked again until its backoff ends (or,
        // on a frozen clock, a flush completes or a group is admitted), so a dead device
        // does not loop, and one bad slot does not stop the others (issues #70, #79, #141).
        // A poisoned shard starts none: it is dead until reopen.
        if self.poisoned {
            return;
        }
        if due.is_empty() && self.cleanups.is_empty() {
            self.start_blob_gc(&view, ctx);
            return;
        }
        let busy: Vec<SstId> = self
            .shared
            .busy_ssts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .copied()
            .collect();
        // A queued cleanup takes every other compaction, so writes elsewhere on the shard
        // never starve it (and the merge waiting on it); not while writers stall on L0.
        if self.cleanup_turn && self.stall.since == 0 && !self.cleanups.is_empty() {
            self.start_cleanup(&view, &busy, ctx);
            if self.compaction.is_some() {
                self.cleanup_turn = false;
                return;
            }
        }
        // The earliest end of a quarantine that kept a due slot from being picked.
        let mut quarantined: Option<u64> = None;
        // The most urgent slot the picker finds work in (another one's inputs may be busy,
        // or its compactions keep failing: it waits out its own backoff, issue #141).
        for (_, key) in due {
            if let Some(&(_, until)) = self.slot_backoff.get(&key)
                && until > now_nanos
            {
                quarantined = Some(quarantined.map_or(until, |q: u64| q.min(until)));
                continue;
            }
            let Some(fam) = view.ssts.family(key.0, key.1) else {
                continue;
            };
            let Some(meta) = view.catalog.family(key.1) else {
                continue;
            };
            let Some(task) = self.picker(meta.options.compaction).pick(
                key.0,
                key.1,
                &fam.levels_meta(),
                &busy,
                now,
                meta.options.ttl_micros,
            ) else {
                continue;
            };
            if let Err(e) = self.start_compaction(&view, key, task, ctx) {
                trace!("shard {} compaction start failed: {e}", self.id.0);
                self.back_off_compaction(key, ctx);
            }
            self.cleanup_turn = true;
            return;
        }
        // Nothing urgent: rewrite an inherited SST that blocks a merge (#95), or empty blob
        // files that are mostly garbage.
        self.start_cleanup(&view, &busy, ctx);
        if self.compaction.is_none() {
            self.start_blob_gc(&view, ctx);
        }
        // A slot passed over for its backoff is retried when the backoff ends, even if
        // nothing else happens on the shard by then.
        if let Some(until) = quarantined {
            self.arm_compaction_retry(until, ctx);
        }
    }

    /// Starts a blob GC of a slot that may point into a blob file that is mostly garbage
    /// (issue #33), if there is one and no compaction runs.
    fn start_blob_gc(&mut self, view: &Arc<View>, ctx: &mut ShardContext<'_, ShardMsg>) {
        if self.compaction.is_some() {
            return;
        }
        let now_nanos = ctx.now_nanos();
        let slots: Vec<(TabletId, FamilyId)> = self
            .owned_slots(view)
            .into_iter()
            .filter(|k| {
                view.catalog
                    .family(k.1)
                    .is_some_and(|m| m.merge != MergeKind::Unknown)
                    && !self
                        .slot_backoff
                        .get(k)
                        .is_some_and(|&(_, until)| until > now_nanos)
            })
            .collect();
        let busy: Vec<SstId> = self
            .shared
            .busy_ssts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .copied()
            .collect();
        let last = self.shared.picker.max_levels.max(2) - 1;
        let min_garbage = self.shared.picker.target_sst_bytes / 16;
        let Some((key, task)) = self.blob_gc.plan(view, &slots, last, &busy, min_garbage) else {
            return;
        };
        if let Err(e) = self.start_compaction(view, key, task, ctx) {
            trace!("shard {} blob GC start failed: {e}", self.id.0);
            self.back_off_compaction(key, ctx);
        }
    }

    /// Makes sure a `RetryCompaction` fires by `deadline`.
    fn arm_compaction_retry(&mut self, deadline: u64, ctx: &mut ShardContext<'_, ShardMsg>) {
        if let Some((t, at)) = &self.backoff_timer
            && !t.finished()
            && *at <= deadline
        {
            return;
        }
        if let Some((t, _)) = self.backoff_timer.take() {
            t.cancel();
        }
        let state = TimerState::new();
        self.backoff_timer = Some((Arc::clone(&state), deadline));
        ctx.spawn(Box::new(ClockTimer::new(
            &self.shared.vfs,
            deadline,
            state,
            ctx.submitter(self.id).clone(),
            ShardMsg::RetryCompaction,
        )));
    }

    /// A flush completed or a group was admitted. On a frozen clock the backoff timer never
    /// fires, so these events end a compaction backoff (and every slot's quarantine) there
    /// (D126). On a moving clock only the timer does: otherwise under steady writes every
    /// group would restart a compaction that keeps failing (issue #141).
    fn end_backoff_on_event(&mut self, now: u64) {
        let frozen = self
            .backoff_timer
            .as_ref()
            .is_some_and(|(t, _)| t.frozen(now));
        if frozen {
            self.slot_backoff.clear();
            self.due_backing_off = false;
        }
    }

    /// `key`'s compaction failed (or could not start): no background compaction picks that
    /// slot again until `compaction_backoff_nanos` of its failures in a row pass, or, on a
    /// frozen clock only, a flush completes or a group is admitted (D119, D126; issues #70,
    /// #79, #141). Other slots go on compacting.
    fn back_off_compaction(
        &mut self,
        key: (TabletId, FamilyId),
        ctx: &mut ShardContext<'_, ShardMsg>,
    ) {
        self.shared.metrics[usize::from(self.id.0)]
            .compaction_failures
            .fetch_add(1, Ordering::Relaxed);
        let e = self.slot_backoff.entry(key).or_insert((0, 0));
        e.0 = e.0.saturating_add(1);
        e.1 = ctx.now_nanos().saturating_add(backoff_nanos(
            self.shared.compaction_backoff_nanos,
            COMPACTION_BACKOFF_CAP,
            e.0,
        ));
        let until = e.1;
        self.arm_compaction_retry(until, ctx);
    }

    /// The smallest user timestamp `family` gets from the shares this shard prepared and
    /// holds until the decision: a committed share lands in the memtables under its own,
    /// possibly lower, seqno, so a compaction counts it as above its inputs (D70; issue
    /// #132). Every tablet of the family counts, since routing may change before the
    /// decision; `u64::MAX` when there is none.
    fn prepared_min_ts(&self, family: FamilyId) -> Timestamp {
        let mut min = u64::MAX;
        for share in self.prepared.values() {
            for m in share.bytes.batch().iter() {
                // A share that does not decode fails its apply; it adds nothing.
                if let Ok(m) = m
                    && m.family == family
                {
                    min = min.min(m.ts.unwrap_or(share.commit_ts));
                }
            }
        }
        min
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
        let mem_min_ts = self
            .memtables
            .get(&key)
            .map_or(u64::MAX, MemSlot::min_ts)
            .min(self.prepared_min_ts(key.1));
        let now = self.shared.vfs.now_micros();
        let mut gc = compact::gc_policy(
            &self.shared,
            fam,
            &task,
            (
                mem_min_ts,
                self.memtables.get(&key).and_then(MemSlot::min_seqno),
            ),
            self.prepared.keys().copied(),
            meta.options.kind == FamilyKind::Counter,
            now,
        );
        // A slot whose purge writes kept voiding compacts once without it (#316).
        if self
            .purge_voids
            .get(&key)
            .is_some_and(|n| *n >= PURGE_VOID_LIMIT)
        {
            gc.min_ts_above = 0;
        }
        // A test hook's record: production builds keep none (5-6 6.2), and test builds only
        // while a test records (`Engine::record_history`).
        let record = (self.recording()
            && matches!(
                task.kind,
                pigeonhole_compaction::TaskKind::Rewrite
                    | pigeonhole_compaction::TaskKind::BlobGc { .. }
            ))
        .then(|| CompactionRecord {
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
            flush: false,
            versions_purge: false,
            input_seqnos: None,
            install_seqno: Default::default(),
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
        // A bottommost purge below `min_ts_above` runs under a guard (#316): a write that
        // could land among its inputs below that bound before it installs voids it.
        let guard = (gc.bottommost
            && gc.min_ts_above > 0
            && meta.options.kind != FamilyKind::Counter
            && matches!(
                task.kind,
                pigeonhole_compaction::TaskKind::Rewrite
                    | pigeonhole_compaction::TaskKind::BlobGc { .. }
            ))
        .then(|| CompactionGuard {
            family: key.1,
            min_ts_above: gc.min_ts_above,
            max_ts: fam
                .iter()
                .filter(|s| ids.contains(&s.meta.id))
                .map(|s| s.meta.ts_range.1)
                .max()
                .unwrap_or(u64::MAX),
            state: Arc::new(AtomicU8::new(GUARD_IN_FLIGHT)),
        });
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
            Ok(mut w) => {
                w.guard = guard.as_ref().map(|g| Arc::clone(&g.state));
                w
            }
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
        self.compaction_guard = guard;
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
        let key = self.compaction.take();
        let full = std::mem::take(&mut self.compaction_full);
        // A write voided the purge before its commit (#316): the commit was refused, and
        // the slot compacts again at once with a fresh bound (not a failure).
        let voided = self
            .compaction_guard
            .take()
            .is_some_and(|g| g.state.load(Ordering::Acquire) == GUARD_VOIDED);
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
                if let Some(key) = key {
                    self.slot_backoff.remove(&key);
                    self.purge_voids.remove(&key);
                }
                // On a frozen clock the bucket never refills: compaction progress paces a
                // stall instead (issue #70).
                let now = ctx.now_nanos();
                if self.stall.timer.as_ref().is_some_and(|t| t.frozen(now)) {
                    self.stall.tokens = (self.stall.tokens + 1.0).min(STALL_CAPACITY);
                }
            }
            // The table was dropped while this compaction ran: its output was abandoned and
            // its inputs are retired with the table. Nothing failed; a full compaction goes
            // on with the remaining tables.
            Err(Error::TableNotFound(_)) => {}
            Err(Error::Busy) if voided => {
                if let Some(key) = key {
                    *self.purge_voids.entry(key).or_insert(0) += 1;
                }
                #[cfg(feature = "test-hooks")]
                self.shared.metrics[usize::from(self.id.0)]
                    .hooks
                    .purge_voids
                    .fetch_add(1, Ordering::Relaxed);
                trace!(
                    "shard {} compaction purge voided by a write; compacting again",
                    self.id.0
                );
            }
            Err(e) => {
                // A full compaction reports the failure to its caller. A background one is
                // nobody's (a later `compact` must not inherit it, issue #141): it backs off,
                // and its slot waits out a backoff of its own so the other slots still
                // compact.
                if full && let Some((_, reply)) = self.compact_all.pop_front() {
                    reply.notify(Err(e));
                }
                if let Some(key) = key {
                    self.back_off_compaction(key, ctx);
                }
            }
        }
        // A stalled group waits on this compaction: it runs again whatever the outcome.
        if !self.pending.is_empty() {
            let _ = ctx.submitter(self.id).submit(ShardMsg::Kick);
        }
        self.maintain(ctx);
        self.progress_op(ctx);
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
            let view = self.checkpoint_view().ok().flatten();
            for l in &self.log {
                trace!(
                    "  shard {} log: seqno {} {:?} needed={}",
                    self.id.0,
                    l.seqno,
                    l.kind,
                    self.needed(view.as_ref().map(|v| &*v.catalog), l)
                );
            }
        }
        if !self.unresolved.is_empty()
            || !self.coord.is_empty()
            || !self.prepared.is_empty()
            || !self.pending.is_empty()
            || self.op.is_some()
            || !self.parked.is_empty()
            || !self.retries.is_empty()
        {
            return;
        }
        if self.close_stage == CloseStage::Draining {
            self.close_stage = CloseStage::Flushing;
            // Abandon the stalls: nothing is admitted any more (issue #88).
            self.stall.score = 0.0;
            self.stall.cancel_timer();
            self.end_room_wait(ctx.now_nanos());
            self.wait_room = false;
            // No compaction starts while closing: its backoff timer has nothing to retry.
            if let Some((t, _)) = self.backoff_timer.take() {
                t.cancel();
            }
            // Close's own flush advances the checkpoint; a refused one makes it unclean.
            if let Some(t) = self.checkpoint_timer.take() {
                t.cancel();
            }
            // The close's flush goes at once; one that fails gives up flushing.
            if let Some(t) = self.flush_retry.take() {
                t.cancel();
            }
            if self.freeze(true).is_err() {
                self.shared.fail_close();
            }
            self.spawn_flush(ctx);
        }
        if self.close_stage == CloseStage::Flushing
            && !self.flush_running
            && (self.flush_failed
                || self.shared.pager_poisoned.load(Ordering::Acquire)
                || self.shared.shard_died.load(Ordering::Acquire))
        {
            self.flush_failed = true;
            self.shared.fail_close();
            // Give up on flushing: the WAL keeps everything, the next open replays it, and
            // the close is reported unclean. A dead shard (issue #135) pins the global
            // watermark, so a deferred freeze would wait for it for ever.
            self.close_stage = CloseStage::Checkpointing;
            self.checkpoint_inflight = false;
        }
        if self.close_stage == CloseStage::Flushing {
            if self.freeze_all_pending || self.freeze_deferred {
                // A freeze waited for visibility; try again (the watermark moves once the
                // other shards finish their groups, and they kick us).
                if self.freeze(true).is_err() {
                    self.shared.fail_close();
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
            if self.checkpoint_inflight {
                // Waiting for our own edit.
                return;
            }
            if (!self.log.is_empty() || self.checkpoint_dirty)
                && !self.shared.close.failed.load(Ordering::Acquire)
            {
                // Waiting for the other shards' flushes and checkpoints (ShareFlushed and
                // CommitCheckpointed arrive as messages). Once the close is unclean, a shard
                // that gave up flushing never sends its reports: stop waiting (the WAL keeps
                // whatever the checkpoint did not pass, and the next open replays it).
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
        self.cross_durable_at_close = if self.wal.is_some() {
            self.cross_records_durable()
        } else {
            !self.poisoned
        };
        trace!(
            "shard {} final sync failed={failed} poisoned={}",
            self.id.0, self.poisoned
        );
        if failed {
            self.shared.fail_close();
        }
    }

    /// Records that cross-shard commit `seqno` aborted, so its PREPARE or COMMIT record in
    /// `log` is never needed. A seqno with no record there (refused before it was logged,
    /// or already passed by the checkpoint) is not kept: only the removal of its record
    /// would ever drop it from the set (5-6 6.3). A PREPARE is logged before its shard
    /// replies `Prepared`, and a decision follows every reply, so the record is there by
    /// the time a decision names it.
    fn mark_aborted(&mut self, seqno: Seqno) {
        // Newest first: a decision usually follows its own record closely.
        if self.log.iter().rev().any(|l| l.seqno == seqno) {
            self.aborted.insert(seqno);
        }
    }

    /// Whether every PREPARE and COMMIT record appended since open is durable.
    fn cross_records_durable(&self) -> bool {
        self.wal
            .as_ref()
            .is_some_and(|w| w.durable() >= self.cross_end)
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
        shared.shard_died();
        if shared.close.remaining.load(Ordering::Acquire) > 0 {
            shared.report_closed();
        }
    }

    /// The shard's thread is unwinding from a panic: report the shard closed (and the close
    /// unclean), so a close waiting for every shard ends instead of hanging (issue #135). No
    /// final sync: the shard's state is not trusted.
    fn report_panicked(&mut self) {
        if self.close_stage == CloseStage::Reported {
            return;
        }
        self.closing = true;
        self.close_stage = CloseStage::Reported;
        self.shared.shard_died();
        if self.shared.close.remaining.load(Ordering::Acquire) > 0 {
            self.shared.report_closed();
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
            // The barrier covers records other shards' shares depend on: this stream's
            // PREPAREs and COMMITs. Durable ones keep holding after the failure, so one
            // shard's WAL error does not fail every peer's flush (5-6 5.1).
            if self.cross_records_durable() {
                reply.notify(Ok(()));
            } else {
                reply.notify(Err(poisoned_error()));
            }
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
            // Closed: the stream was synced before it was dropped, or (after a failure) its
            // cross-shard records were durable by then.
            None if self.close_stage == CloseStage::Reported && self.cross_durable_at_close => {
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
            ShardMsg::Coordinate(req) => self.start_coordination(*req, ctx),
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
            ShardMsg::Unpin { through } => self.on_unpin(through, ctx),
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
                if !self.replaying && self.tablets_on() {
                    // Another shard's flush may cover slots logged in this stream (moved
                    // tablets, a reopen), and the tablet map may have changed.
                    self.report_shares_flushed(ctx);
                    self.advance_checkpoint(ctx);
                    self.progress_op(ctx);
                    self.run_retries(ctx);
                    self.try_finish_close(ctx);
                }
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
            ShardMsg::RetryFlush => {
                // Only the current backoff timer's firing ends the backoff.
                if self.flush_retry.as_ref().is_some_and(|t| t.fired()) {
                    self.flush_retry = None;
                    self.spawn_flush(ctx);
                }
            }
            ShardMsg::RetryCompaction => {
                // Only the current backoff timer's firing ends the backoff.
                if self.backoff_timer.as_ref().is_some_and(|(t, _)| t.fired()) {
                    self.backoff_timer = None;
                    self.maintain(ctx);
                }
            }
            ShardMsg::RetryCheckpoint => {
                // Only the current timer's firing retries.
                if self.checkpoint_timer.as_ref().is_some_and(|t| t.fired()) {
                    self.checkpoint_timer = None;
                    self.advance_checkpoint(ctx);
                }
            }
            ShardMsg::TabletOp { op, reply } => self.request_tablet_op(op, reply, ctx),
            ShardMsg::TabletOpDone { result } => self.on_tablet_op_done(result, ctx),
            ShardMsg::Balance { reply } => self.maybe_balance(true, reply, ctx),
            ShardMsg::Close => {
                if !self.closing {
                    self.closing = true;
                    self.close_stage = CloseStage::Draining;
                    self.shared.closing.store(true, Ordering::Release);
                    // Queued tablet changes never start; the running one finishes if it is
                    // committing and is given up otherwise, so close never waits on the
                    // drain of a change (or the commits parked behind it, #102).
                    for (_, reply) in self.op_queue.drain(..) {
                        if let Some(r) = reply {
                            r.notify(Err(Error::Closed));
                        }
                    }
                    self.abort_op(Error::Closed, ctx);
                    self.cancel_balance_timer();
                    self.run_retries(ctx);
                }
                self.try_finish_close(ctx);
            }
        }
    }
}

/// Whether `PIGEONHOLE_TRACE` is set: the close and checkpoint protocol logs its steps.
pub(crate) fn tracing() -> bool {
    match TRACING.load(Ordering::Relaxed) {
        TRACE_UNSET => {
            let on = std::env::var_os("PIGEONHOLE_TRACE").is_some();
            let _ = TRACING.compare_exchange(
                TRACE_UNSET,
                if on { TRACE_ON } else { TRACE_OFF },
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            TRACING.load(Ordering::Relaxed) == TRACE_ON
        }
        state => state == TRACE_ON,
    }
}

/// `tracing`'s state: not read from the environment yet, off, or on.
static TRACING: AtomicU8 = AtomicU8::new(TRACE_UNSET);
const TRACE_UNSET: u8 = 0;
const TRACE_OFF: u8 = 1;
const TRACE_ON: u8 = 2;

/// Turns the engine's `PIGEONHOLE_TRACE` logging on or off for the whole process (test
/// hook: a model suite's watchdog turns it on once a seed hangs, to see what the shards
/// are doing then without tracing the whole run, #244).
#[doc(hidden)]
pub fn set_tracing(on: bool) {
    TRACING.store(if on { TRACE_ON } else { TRACE_OFF }, Ordering::Relaxed);
}

/// Logs to stderr when `PIGEONHOLE_TRACE` is set (CONTRIBUTING.md, "Test environment variables"):
/// the only output the engine library writes. It writes to the stderr handle itself, not
/// through `eprintln!`, so a test harness's output capture (which shard threads spawned by
/// a test inherit) cannot swallow it when a hung test aborts the process.
macro_rules! trace {
    ($($arg:tt)*) => {
        if $crate::shard::tracing() {
            use std::io::Write as _;
            let _ = writeln!(std::io::stderr(), $($arg)*);
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
        if self.op.is_some() {
            self.progress_op(ctx);
        }
        self.run_retries(ctx);
        self.expire_tablet_waits(ctx);
        self.maybe_balance(false, None, ctx);
        if (self.freeze_all_pending
            || (!self.to_freeze.is_empty() && self.freeze_deferred)
            || self.wait_room)
            && self.freeze(false).is_ok()
        {
            self.spawn_flush(ctx);
            self.check_flush_waiters();
        }
        self.retry_starved_freeze(ctx);
        self.try_finish_close(ctx);
        #[cfg(feature = "test-hooks")]
        {
            let m = &self.shared.metrics[usize::from(self.id.0)].hooks;
            m.aborted
                .store(self.aborted.len() as u64, Ordering::Relaxed);
        }
        self.publish_arena();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The D34 collapse finds the same losers whether a batch arrives strictly increasing
    /// (the sort is skipped), out of order, or with repeats inside an increasing run.
    #[test]
    fn dedup_finds_the_losers_in_any_order() {
        let (t, f) = (TableId(1), FamilyId(1));
        let scan = |cells: &[(&[u8], &[u8], Option<u64>)]| {
            let mut b = BatchBuilder::new();
            for (row, q, ts) in cells {
                b.push(t, f, Kind::Put, row, q, *ts, b"v").unwrap();
            }
            let batch = b.batch();
            let mut d = Dedup::default();
            let dup = d.scan(batch, batch.as_bytes(), 9);
            (dup, (0..cells.len()).map(|i| d.wins(i)).collect::<Vec<_>>())
        };
        // A row's cells in order: nothing collapses.
        assert_eq!(
            scan(&[(b"r", b"a", None), (b"r", b"b", None), (b"s", b"a", None)]),
            (false, vec![true, true, true])
        );
        // Out of order with one repeat: the earlier copy loses.
        assert_eq!(
            scan(&[(b"r", b"b", None), (b"r", b"a", None), (b"r", b"b", None)]),
            (true, vec![false, true, true])
        );
        // Increasing but for a repeat next to its first copy (a default timestamp equals the
        // commit's, 9): the sort runs and the first copy loses.
        assert_eq!(
            scan(&[
                (b"r", b"a", None),
                (b"r", b"b", Some(9)),
                (b"r", b"b", None)
            ]),
            (true, vec![true, false, true])
        );
        // Same column at two timestamps is two cells.
        assert_eq!(
            scan(&[(b"r", b"a", Some(3)), (b"r", b"a", Some(4))]),
            (false, vec![true, true])
        );
    }

    /// Issue #244: a room wait whose timeout timer saw the clock move sleeps towards a
    /// deadline a stopped clock never reaches, and a shard kicked by each re-check never goes
    /// idle for that timer to be polled again. A re-check timer that gave up on this reading
    /// must be enough to call the clock stopped, or the wait never takes the frozen-clock
    /// path (refused with `Busy`, issue #70) and the shard spins (seen in the public model
    /// suite, seed 53).
    #[test]
    fn a_room_wait_sees_a_stopped_clock_through_its_recheck_timer() {
        let mut wait = RoomWait::new(10, TimerState::new());
        assert!(!wait.stopped(50));
        // The timeout timer sleeps; the re-check timer gave up with the clock at 50.
        let recheck = TimerState::new();
        recheck.frozen_at.store(50, Ordering::Release);
        recheck.done.store(true, Ordering::Release);
        wait.recheck = Some(recheck);
        assert!(wait.stopped(50));
        // Once the clock moves on, it is not stopped any more.
        assert!(!wait.stopped(51));
        // The timeout timer giving up counts as before.
        let wait = RoomWait::new(10, TimerState::new());
        wait.timer.frozen_at.store(60, Ordering::Release);
        assert!(wait.stopped(60));
    }

    #[test]
    fn a_retry_keeps_its_timestamp_only_where_no_other_write_reached_it() {
        let (a, b, c) = (ShardId(0), ShardId(1), ShardId(2));
        let own = [a, b];
        // Strictly above the floor and the raise: kept anywhere.
        assert!(preset_fits(10, &own, c, 9, (9, 0b100)));
        // Below either: never.
        assert!(!preset_fits(10, &own, a, 11, (0, 0)));
        assert!(!preset_fits(10, &own, a, 9, (11, 0b1)));
        // A tie with the floor only on a shard that reached it through this commit.
        assert!(preset_fits(10, &own, b, 10, (0, 0)));
        assert!(!preset_fits(10, &own, c, 10, (0, 0)));
        // A tie with a raise only when every shard that raised it is the commit's own.
        assert!(preset_fits(10, &own, c, 9, (10, 0b011)));
        assert!(!preset_fits(10, &own, c, 9, (10, 0b101)));
    }

    #[test]
    fn a_freeze_waiter_wakes_only_once_its_seqno_is_visible() {
        let w = FreezeWaiters::default();
        w.register(3, 10);
        w.register(1, 5);
        // A watermark that did not reach either seqno wakes nobody.
        assert!(w.take_ready(|| 4).is_empty());
        assert_eq!(w.take_ready(|| 7), [1]);
        assert!(w.take_ready(|| 9).is_empty());
        // Registering again replaces the seqno.
        w.register(3, 12);
        assert!(w.take_ready(|| 10).is_empty());
        assert_eq!(w.take_ready(|| 12), [3]);
        assert!(w.take_ready(|| u64::MAX - 1).is_empty());
        // A shard that defers again after its wake is woken again.
        w.register(3, 20);
        assert_eq!(w.take_ready(|| 20), [3]);
    }

    #[test]
    fn a_freeze_waiter_registering_during_a_take_is_never_lost() {
        // Registrants race a taker while a checker looks: under the lock the published
        // minimum always equals the list's smallest needed seqno, so a registration racing a
        // take is never hidden from later publishers.
        const SHARDS: u16 = 4;
        let w = FreezeWaiters::default();
        let stop = AtomicUsize::new(0);
        std::thread::scope(|s| {
            for id in 0..SHARDS {
                let (w, stop) = (&w, &stop);
                s.spawn(move || {
                    let mut n = 0u64;
                    while stop.load(Ordering::Acquire) == 0 {
                        n += 1;
                        w.register(id, n);
                    }
                });
            }
            s.spawn(|| {
                let mut v = 0u64;
                while stop.load(Ordering::Acquire) == 0 {
                    v += 1;
                    let _ = w.take_ready(|| v);
                }
            });
            for _ in 0..200_000 {
                let list = w.list.lock().unwrap_or_else(PoisonError::into_inner);
                let min = w.min_needed.load(Ordering::SeqCst);
                let want = list.iter().map(|(_, n)| *n).min().unwrap_or(u64::MAX);
                if min != want {
                    stop.store(1, Ordering::Release);
                    panic!("published minimum {min} with {:?} registered", *list);
                }
            }
            stop.store(1, Ordering::Release);
        });
    }

    #[test]
    fn a_commit_refused_with_moved_gives_up_after_the_cap_or_the_timeout() {
        let timeout = 30_000_000_000;
        assert!(!moved_gives_up(1, 0, timeout));
        assert!(!moved_gives_up(MOVED_RETRIES - 1, timeout - 1, timeout));
        assert!(moved_gives_up(MOVED_RETRIES, 0, timeout));
        assert!(moved_gives_up(1, timeout, timeout));
    }

    #[test]
    fn compaction_backoff_doubles_from_one_second_up_to_a_minute() {
        let base = 1_000_000_000;
        let backoff = |f| backoff_nanos(base, COMPACTION_BACKOFF_CAP, f);
        let secs: Vec<u64> = (1..=9).map(|f| backoff(f) / base).collect();
        assert_eq!(secs, [1, 2, 4, 8, 16, 32, 60, 60, 60]);
        // Never shorter than the first wait, never longer than the cap, at any count.
        assert_eq!(backoff(0), base);
        for f in [64, 65, 1_000, u32::MAX] {
            assert_eq!(backoff(f), 60 * base);
        }
        // A flush's: 10 ms doubling up to 1 s.
        let ms: Vec<u64> = (1..=9)
            .map(|f| backoff_nanos(10_000_000, FLUSH_BACKOFF_CAP, f) / 1_000_000)
            .collect();
        assert_eq!(ms, [10, 20, 40, 80, 160, 320, 640, 1000, 1000]);
    }
}

#[cfg(test)]
mod message_size {
    /// Every queued message takes the size of the largest `ShardMsg` variant, and the
    /// channel zeroes and copies whole slots (#64: 264 bytes cost about 270 instructions of
    /// `memset` per commit). Keep a single-shard commit the largest variant, and small:
    /// box rare, large fields instead of growing it.
    #[test]
    fn a_shard_message_stays_small() {
        assert!(
            std::mem::size_of::<super::ShardMsg>() <= 144,
            "ShardMsg grew to {} bytes",
            std::mem::size_of::<super::ShardMsg>()
        );
    }
}
