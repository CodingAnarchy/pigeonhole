//! Test hooks: the `test-hooks` feature, which only the engine's own tests and benches
//! enable (its dev-dependency on itself). Everything a test reaches into the engine with
//! is here: the `#[doc(hidden)]` `Engine` methods, the types they return, and the state
//! they keep in `Shared::hooks` and `ShardMetrics::hooks`.
//!
//! The rules:
//! - Every hook is used by a committed test; delete one when its last test goes.
//! - A hook does nothing until a test sets it: callbacks run once, flags start clear.
//!   `take_appended` and `take_compactions` record only after a test turns recording on
//!   with `record_history` (D164's follow-up, #148): with workspace feature unification
//!   every `test-hooks` build would otherwise grow those vectors for a whole run nobody
//!   reads. The [`ShardCounters`] are plain counters and always count. None changes what
//!   the engine does.
//! - Prefer a public or application-owned seam (`EngineShard` over `SimVfs`,
//!   `Engine::shard_stats`) to a new hook.
//!
//! The `#[cfg(feature = "test-hooks")]` sites elsewhere in the crate only run, check or
//! record into this state; `manifest.rs` keeps the three helpers that need its private
//! commit state (`race_window`, `parked`, `parks`).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use pigeonhole_compaction::MergeRegistry;
use pigeonhole_format::{
    Durability, FamilyId, Lsn, ManifestVersion, Seqno, StreamId, TableId, TabletId, Timestamp,
};
use pigeonhole_pager::Pager;
use pigeonhole_runtime::{ShardId, TaskWaker, Waiter, completion};

pub use super::PendingMaintenance;
pub use crate::compact::CompactionRecord;

use super::Engine;
use crate::manifest;
use crate::shard::ShardMsg;
use crate::snapshot::{Snapshot, TabletMap};
use crate::{Error, Result};

/// Where a one-shot test callback runs (`Engine::on_next`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookSite {
    /// At the start of the next view publish (a shard publishing its memtables or a
    /// manifest commit), before that publish takes the publish lock: where another thread's
    /// publish can land in between.
    ViewPublish,
    /// On the calling thread of the next writer `snapshot()`, after it pinned its seqno and
    /// before it loads the view: where a test publishes a flush in between (#315 review).
    SnapshotViewLoad,
    /// On the calling thread of the next unpinned read as of now (`get_latest`,
    /// `read_row_latest_into`), after its first visible-seqno read and before it loads the
    /// view. The callback may set this site again, to run in the read's next try too.
    LatestViewLoad,
    /// In the next `shrink` round, after it has read the catalog and before it relocates
    /// anything: where a compaction or `drop_table` can commit under it.
    ShrinkRelocates,
    /// On the thread that `Engine::wait_failed` woke with a failure, before it returns:
    /// where the recovery supervisor has not marked the failure yet, and a `close()` can
    /// reach the failed engine first (D265).
    FailureWake,
    /// On the calling thread of the next `backup`, right after it released its snapshot's
    /// memtables and before it merges the SSTs: where a test writes past the arena while
    /// the backup still runs (#262).
    BackupReleasedMemtables,
    /// In the next `shrink` round that commits, after it has written its copies and before
    /// it commits them: where a `drop_table` makes a copy one to abandon.
    ShrinkCommits,
    /// Reader processes: in the next snapshot, between reading the view record from shared
    /// memory and loading the catalog of its manifest version.
    ReaderViewRecord,
    /// Reader processes: when a snapshot found the durable root at the view record's
    /// manifest version and is about to read that manifest.
    ReaderManifestLoad,
}

/// A one-shot test callback.
type Callback = Box<dyn FnOnce() + Send>;

/// The one-shot callbacks tests set, at most one per site (`Hooks::sites`).
#[derive(Default)]
pub(crate) struct Sites(Mutex<Vec<(HookSite, Callback)>>);

impl Sites {
    /// Sets the callback for `site`, replacing one that has not run.
    pub(crate) fn set(&self, site: HookSite, f: Callback) {
        let mut v = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        v.retain(|(s, _)| *s != site);
        v.push((site, f));
    }

    /// Runs `site`'s callback if one is set (outside the lock).
    pub(crate) fn run(&self, site: HookSite) {
        let f = {
            let mut v = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            let i = v.iter().position(|(s, _)| *s == site);
            i.map(|i| v.swap_remove(i).1)
        };
        if let Some(f) = f {
            f();
        }
    }
}

/// A fault or forced path a test switches on (`Engine::inject`). The `Next` ones are
/// one-shot: the site that takes one clears it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// While on, the manifest writer refuses every batch made only of WAL checkpoints with
    /// `NoSpace`, through the path a snapshot rewrite that finds no space takes (nothing
    /// written, the writer stays usable).
    RefuseCheckpoints = 0,
    /// While on, manifest commits drop every `SstBlobRefs` edit and the catalog keeps no
    /// blob references, so the file looks as one written before tag 13 (#240).
    OmitBlobRefs = 1,
    /// Fails the next apply after its WAL append, of a single-shard batch or of a
    /// cross-shard share: it is not applied, and the shard poisons itself, as an arena
    /// miscount would (#230). The commit returns `OutcomeUnknown` either way: a single-shard
    /// one's record replays at the next open, and a cross-shard one decided to commit is
    /// settled there too (D227, ICR 0038, ICR 0040).
    NextApply = 2,
    /// Fails the next freeze a tablet change (a split, merge or move) makes of its tablets'
    /// memtables: the shard poisons itself, as an arena miscount would, and the instance
    /// reports the failure (ICR 0040).
    NextTabletFreeze = 3,
    /// The next commit group's second member waits for room as if the arena were full: the
    /// stall path freezes memtables after the group's first member was admitted (#315
    /// review).
    NextRoomWait = 4,
    /// The next freeze finds no chunk for a fresh memtable, as a starved arena would give
    /// it: a closing shard then flushes the active memtable in place (#111, #838).
    NextFreezeStarved = 5,
    /// As `NextRoomWait`, for the group's third member: two members are admitted first, so a
    /// group can wait for room with two singles on one row admitted, ranked 0 and 1 (#900).
    ThirdRoomWait = 6,
    /// The next commit group's second append fails as an I/O error would, before anything of
    /// that member reaches the stream's buffer; the first member is appended (and written and
    /// synced if it is a `Sync` one). The shard poisons itself (#921, #925).
    SecondAppend = 7,
    /// The next commit group's view publish (`publish_memtables`, after its write and apply)
    /// fails: every member of the group fails with `Corruption` and the shard poisons itself.
    /// A Buffered PREPARE in it has voted already (D279), so its commit ends `OutcomeUnknown`.
    NextGroupPublish = 8,
    /// While on, a coordinator holds every PREPARE it would send another shard (in
    /// `Hooks::held_prepares`) instead of sending it, until `Engine::release_held_prepares`
    /// sends them. Hand-driven tests use it to build an arrival order explicitly, rather
    /// than from the order messages were queued in: a participant's turn between the
    /// coordinator reserving a commit's seqno and the PREPARE reaching it.
    HoldPrepares = 9,
    /// While on, a coordinator sets its DECISION members aside instead of admitting them
    /// (D286's tests: an early `Decide` is applied while its DECISION is unwritten). Switch
    /// it off with `Engine::release_held_decisions`, which kicks the shards: the held
    /// DECISIONs rejoin the end of their shard's queue at its next group. That reorders them
    /// after anything queued meanwhile, so don't use it to reason about group order.
    HoldDecisionRecords = 10,
    /// As `NextApply`, but only a single-shard batch's apply takes it: a cross-shard share
    /// applies as usual. For tests that poison a shard with a share still to apply somewhere,
    /// which would otherwise take the fault in its place (#998).
    NextSingleApply = 11,
}

/// The faults on, a bit per `Fault` (`Hooks::faults`).
#[derive(Default)]
pub(crate) struct Faults(AtomicU16);

impl Faults {
    fn bit(f: Fault) -> u16 {
        1 << (f as u16)
    }

    /// Switches `f` on or off.
    fn set(&self, f: Fault, on: bool) {
        if on {
            self.0.fetch_or(Self::bit(f), Ordering::AcqRel);
        } else {
            self.0.fetch_and(!Self::bit(f), Ordering::AcqRel);
        }
    }

    /// Whether `f` is on.
    pub(crate) fn on(&self, f: Fault) -> bool {
        self.0.load(Ordering::Acquire) & Self::bit(f) != 0
    }

    /// Whether `f` was on, switching it off (a one-shot fault's site).
    pub(crate) fn take(&self, f: Fault) -> bool {
        self.0.fetch_and(!Self::bit(f), Ordering::AcqRel) & Self::bit(f) != 0
    }
}

/// The engine-wide hook state (`Shared::hooks`).
#[derive(Default)]
pub(crate) struct Hooks {
    /// The one-shot callbacks tests set at named sites (`Engine::on_next`).
    pub sites: Sites,
    /// The error of a refused arena discard (`Shared::try_discard_arenas`, D265).
    pub arena_discard_error: Mutex<Option<String>>,
    /// The faults and forced paths tests switch on (`Engine::inject`).
    pub faults: Faults,
    /// PREPAREs held by `Fault::HoldPrepares`, with their participant, in send order
    /// (`Engine::release_held_prepares`).
    pub held_prepares: Mutex<Vec<(pigeonhole_runtime::ShardId, crate::shard::ShardMsg)>>,
    /// Whether `compactions` and `appended` record (`Engine::record_history`); off until a
    /// test turns it on.
    pub record: AtomicBool,
    /// Every committed compaction (`Engine::take_compactions`; the records are never built
    /// without the feature, 5-6 6.2).
    pub compactions: Mutex<Vec<CompactionRecord>>,
    /// Every WAL record appended, in append order (`Engine::take_appended`).
    pub appended: Mutex<Vec<AppendedRecord>>,
    /// The seqnos of cross-shard attempts a participant refused (`Moved`, `Room` or
    /// `PendingOverlap`),
    /// whose commit was retried under a new seqno or gave up (`Engine::take_refused_attempts`).
    pub refused: Mutex<Vec<Seqno>>,
    /// PREPAREs refused for arena room (D245, `Engine::room_refusals`).
    pub room_refusals: AtomicU64,
    /// Arms the manifest queue's release window (`manifest::race_window`,
    /// `Engine::probe_manifest_release_window`), and the waiter of the request it pushed.
    pub manifest_race: AtomicBool,
    pub manifest_race_waiter: Mutex<Option<Waiter<Result<ManifestVersion>>>>,
    /// Threads that parked in `manifest::commit_req_from_thread`, counted after their last
    /// look and just before `park` (`Engine::manifest_thread_parks`).
    pub manifest_thread_parks: AtomicUsize,
    /// Parks background manifest commits before `end` (`manifest::parked`,
    /// `Engine::park_manifest_commits`), and the parked pump's waker.
    pub manifest_park: AtomicBool,
    pub manifest_parked: Mutex<Option<TaskWaker>>,
    /// Holds each compaction job before its first slice (`Engine::hold_compactions`), and
    /// the held jobs' ids and wakers, oldest first (`Engine::release_held_compaction` takes
    /// one to let it run; clearing the hold takes them all).
    pub compaction_hold: AtomicBool,
    pub compaction_held: Mutex<Vec<(u64, TaskWaker)>>,
    pub compaction_hold_ids: AtomicU64,
    /// Compaction jobs that finished and submitted their commit (`Engine::
    /// compaction_commits_submitted`, D284's tests).
    pub compaction_commits: AtomicU64,
    /// Compaction jobs run a few units per slice (`Engine::compaction_tiny_slices`).
    pub compaction_tiny_slices: AtomicBool,
    /// A deliberate fault in the flush GC (`Engine::mutate_flush_gc`, #287): 1 treats the
    /// guard as always holding, 2 drops the snapshot floor (no live read point is kept), 3
    /// lets writes pass guarded flushes and compactions without voiding them.
    pub flush_gc_mutation: std::sync::atomic::AtomicU8,
}

/// A deliberate fault in the flush GC, for tests that check the oracle catches it
/// (`Engine::mutate_flush_gc`, #287).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum FlushGcMutation {
    /// The real GC.
    None,
    /// Purge versions beyond `max_versions` even when another source holds a delete.
    DropGuard,
    /// Keep no live snapshot's versions (only the latest read point).
    DropSnapshotFloor,
    /// A delete arriving while a guarded flush is in flight, or a write below a running
    /// bottommost compaction's bound (#316), does not void it or wait for its install.
    IgnoreVoids,
    /// A counter family's flush is told of another source of the slot whose seqnos span its
    /// memtable's (D287): it must then keep every operand rather than combine them.
    CounterSourceInside,
}

/// A balancer load term a test chooses for a shard (`Engine::set_shard_load`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadTerm {
    /// The shard thread's busy fraction (0 to 1; D217).
    Busy = 0,
    /// Its writers' stall term (D255): 0 or 1 as measured. A fraction in between reads as
    /// stalled only past `PACK_HOT` (0.9), like a turn busy fraction.
    Stalled = 1,
    /// The pool's work for it (a fraction of the time; it may exceed 1 when several pool
    /// threads serve the shard at once; D256).
    Pool = 2,
    /// Client commits in flight at it on average (0 up; a closed-loop client's worth is at
    /// most 1; D257).
    Inflight = 3,
}

impl LoadTerm {
    const COUNT: usize = 4;

    /// The largest value a test may choose.
    fn max(self) -> f64 {
        match self {
            LoadTerm::Busy | LoadTerm::Stalled => 1.0,
            LoadTerm::Pool => 16.0,
            LoadTerm::Inflight => 1e6,
        }
    }
}

/// A shard's test-hook counters (`ShardMetrics::hooks`), stored after each batch.
#[derive(Debug, Default)]
pub(crate) struct ShardCounters {
    /// Size of the shard's aborted-seqno set (`Engine::aborted_seqnos`).
    pub aborted: AtomicU64,
    /// The arena's free bytes, largest free run and size (`Engine::arena_free`: a test
    /// checks it built the fragmented arena it means to, issue #141).
    /// One snapshot, so a reader never pairs values from different batches.
    pub arena: Mutex<(u64, u64, u64)>,
    /// Reservations that found enough free bytes but no run long enough for their largest
    /// entry (`Engine::arena_run_waits`: a test checks it reached that case, issue #141).
    pub run_waits: AtomicU64,
    /// Compactions whose purge a write voided before they installed
    /// (`Engine::compaction_purge_voids`, #316).
    pub purge_voids: AtomicU64,
    /// The load terms the shard's balancer measures, by `LoadTerm` (millionths plus one; 0
    /// measures the real one), `Engine::set_shard_load` (D217, D255, D256, D257's packing
    /// tests).
    pub load_overrides: [AtomicU64; LoadTerm::COUNT],
    /// The flushed-through seqno the shard last published from `on_unpin` (an unpin that
    /// found nothing unflushed at or below its seqno, #724), `Engine::unpin_published`.
    pub unpin_published: AtomicU64,
    /// Flushes the manifest refused because a delete voided one of their purge guards,
    /// flushed again without it (#287), `Engine::flush_voids` (#838's test).
    pub voided_flushes: AtomicU64,
    /// Decisions that reached the shard before their share's PREPARE settled and waited for
    /// it (D279), `Engine::parked_decisions`.
    pub parked_decisions: AtomicU64,
    /// `Decide { commit: false }` messages the shard sent as a coordinator,
    /// `Engine::abort_decides_sent` (D286's tests: no abort follows an early commit
    /// `Decide`).
    pub abort_decides: AtomicU64,
    /// Entries read validation visited in the shard's memtables (`Engine::validate_entries`,
    /// D273's tests: a hot row validates in a few entries, not one per version).
    pub validate_entries: AtomicU64,
    /// The active shard count (`k`, D217) the shard's balancer used at its last pass, plus
    /// one (0 before its first pass), `Engine::pack_ks`.
    pub pack_k: AtomicU64,
    /// PREPAREs the shard refuses as overlapping an undecided share before routing them as
    /// usual (`Engine::refuse_prepares`).
    pub refuse_prepares: AtomicU64,
    /// `AwaitFlushed` and `FlushedThrough` messages the shard handled
    /// (`Engine::flushed_msgs`, D266).
    pub flushed_msgs: AtomicU64,
    /// Records in the shard's log (those its checkpoint has not passed) after its last
    /// batch (`Engine::logged_records`, D254).
    pub logged: AtomicU64,
    /// Spans the shard added to `commit_inflight_nanos` (`ShardMetrics::count_inflight`):
    /// one per commit it replied to and one per share it decided for another coordinator
    /// (D257, D262), `Engine::inflight_counts`.
    pub inflight_counts: AtomicU64,
    /// The most log records the shard's checkpoint popped in one batch (#763's budget),
    /// `Engine::max_checkpoint_pops`.
    pub checkpoint_pops_max: AtomicU64,
}

impl ShardCounters {
    /// The value a test chose for `term`, or `None` to measure the real one.
    pub(crate) fn load_override(&self, term: LoadTerm) -> Option<f64> {
        match self.load_overrides[term as usize].load(Ordering::Relaxed) {
            0 => None,
            v => Some((v - 1) as f64 / 1e6),
        }
    }
}

/// What a closed instance still holds (`Engine::memory_release`, D265).
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRelease {
    /// Memtable sets the current view lists.
    pub view_memtables: usize,
    /// SSTs the current view's set holds open.
    pub view_ssts: usize,
    /// Bytes in the block cache.
    pub block_cache_bytes: usize,
    /// Bytes in the row cache (`None` when it is off).
    pub row_cache_bytes: Option<usize>,
    /// The arenas' state.
    pub arenas: ArenaDiscard,
    /// The error of a refused discard.
    pub arena_error: Option<String>,
}

/// What became of a closed instance's arena pages (`Engine::memory_release`, D265).
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArenaDiscard {
    /// Not freed yet: the instance is open, a view lives, or the region is still current.
    Pending,
    /// Freed.
    Discarded,
    /// The OS refused (`Unsupported` on macOS and Windows, `EOPNOTSUPP` on a `shm_dir` file
    /// system without hole punching, …): they stay until the last mapping goes. The error is
    /// in `MemoryRelease::arena_error`.
    Refused,
}

/// A WAL record the engine appended to a stream (test hook): the per-stream append order,
/// which decides what a crash keeps (a stream survives as a prefix).
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendedRecord {
    pub stream: u16,
    pub seqno: Seqno,
    pub kind: AppendedKind,
    pub durability: Durability,
    /// The record's commit timestamp (0 for a COMMIT record): a harness reads a commit's
    /// timestamp here when its record was checkpointed and compaction dropped every entry
    /// it wrote.
    pub commit_ts: Timestamp,
    /// A COMMIT's (DECISION's) applied seqno, the commit's `seqno` being its id (D263); 0
    /// for other records.
    pub apply_seqno: Seqno,
}

/// The kind of an [`AppendedRecord`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendedKind {
    Batch,
    Prepare,
    Commit,
}

/// One raw entry of a table (a test hook).
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
#[doc(hidden)]
pub type TabletRange = (TabletId, TableId, Vec<u8>, Option<Vec<u8>>);

/// What the manifest of a closed database records (a test hook).
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
    /// Runs `f` once, the next time the engine reaches `site` (test hook): see
    /// [`HookSite`] for where each runs. Replaces a callback set there that has not run.
    #[doc(hidden)]
    pub fn on_next(&self, site: HookSite, f: Box<dyn FnOnce() + Send>) {
        self.inner.shared.hooks.sites.set(site, f);
    }

    /// Switches fault `fault` on or off (test hook): see [`Fault`] for each. A one-shot fault
    /// (`Next…`) switches itself off where it fires.
    #[doc(hidden)]
    pub fn inject(&self, fault: Fault, on: bool) {
        self.inner.shared.hooks.faults.set(fault, on);
    }

    /// Sends the PREPAREs `Fault::HoldPrepares` held, in the order they were held; returns
    /// how many (test hook). Switch `HoldPrepares` off first, so a hold can't leak into a
    /// later step. A participant already closed refuses the send (`Closed`) and the PREPARE
    /// is dropped: the test then sees its commit unanswered.
    ///
    /// # Panics
    /// If `HoldPrepares` is still on.
    #[doc(hidden)]
    pub fn release_held_prepares(&self) -> usize {
        let hooks = &self.inner.shared.hooks;
        assert!(
            !hooks.faults.on(Fault::HoldPrepares),
            "release_held_prepares with Fault::HoldPrepares still on"
        );
        let held = std::mem::take(&mut *hooks.held_prepares.lock().unwrap());
        let n = held.len();
        let subs = self
            .inner
            .shared
            .submitters
            .get()
            .expect("the shards started");
        for (to, msg) in held {
            let _ = subs[usize::from(to.0)].submit(msg);
        }
        n
    }

    /// The flushed-through seqno shard `shard` last published from an unpin that found
    /// nothing unflushed at or below its seqno, 0 if none (test hook, #724).
    #[doc(hidden)]
    pub fn unpin_published(&self, shard: usize) -> Seqno {
        self.inner.shared.metrics[shard]
            .hooks
            .unpin_published
            .load(Ordering::Relaxed)
    }

    /// Asks shard `shard` to unpin through `through`, as a peer whose checkpoint a commit's
    /// records pin does (`ShardMsg::Unpin`, test hook).
    #[doc(hidden)]
    pub fn request_unpin(&self, shard: usize, through: Seqno) -> Result<()> {
        self.inner
            .shared
            .submitter(ShardId(shard as u16))
            .submit(ShardMsg::Unpin { through })?;
        Ok(())
    }

    /// Shard `shard`'s published flushed-through seqno (`Shared::flushed_through`, #724):
    /// every write it will hold at or below it is in a durable SST. 0 before its first
    /// publish (test hook).
    #[doc(hidden)]
    pub fn flushed_through(&self, shard: usize) -> Seqno {
        self.inner.shared.flushed_through[shard]
            .0
            .load(Ordering::Acquire)
    }

    /// Makes shard `shard`'s balancer measure `term` as `value` from now on (clamped to the
    /// term's range, `LoadTerm`), or the real value with `None` (test hook: the packing
    /// tests, decided on chosen loads under any clock).
    #[doc(hidden)]
    pub fn set_shard_load(&self, shard: usize, term: LoadTerm, value: Option<f64>) {
        let v = value.map_or(0, |b| (b.clamp(0.0, term.max()) * 1e6).round() as u64 + 1);
        self.inner.shared.metrics[shard].hooks.load_overrides[term as usize]
            .store(v, Ordering::Relaxed);
    }

    /// The seqnos of this process's live snapshots, the ones compaction's GC keeps
    /// (`gc_snapshots`): a transaction's commit holds its snapshot here until it resolves
    /// (#672).
    #[doc(hidden)]
    pub fn live_snapshot_seqnos(&self) -> Vec<Seqno> {
        self.inner.shared.live_seqnos.list()
    }

    /// The next seqno to reserve (D263: a harness learns which seqnos a crash
    /// freed for reuse).
    #[doc(hidden)]
    pub fn next_seqno(&self) -> Seqno {
        self.inner.shared.shm.next_seqno()
    }

    /// `flush` as a future (test harnesses that drive the shards themselves).
    #[doc(hidden)]
    pub fn flush_pending(&self) -> Result<PendingMaintenance> {
        self.inner.flush_pending()
    }

    /// `compact` as a future.
    #[doc(hidden)]
    pub fn compact_pending(&self, table: Option<TableId>) -> Result<PendingMaintenance> {
        self.inner.compact_pending(table)
    }

    /// Every entry of every table in `snapshot`'s view, raw (no resolution; separated
    /// values read from their blob files), in key order per `(table, family)`.
    #[doc(hidden)]
    pub fn raw_entries(&self, snapshot: &Snapshot) -> Result<Vec<RawEntry>> {
        let read = || -> Result<Vec<RawEntry>> {
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
                        // A separated value is listed as the value, not its pointer.
                        let value = match view.ssts.read_blob(merged.value())? {
                            Some(v) => v.to_vec(),
                            None => merged.value().to_vec(),
                        };
                        out.push(RawEntry {
                            table: t.table,
                            family,
                            key: merged.key().to_vec(),
                            value,
                        });
                        merged.next()?;
                    }
                }
            }
            Ok(out)
        };
        snapshot.checked(read())
    }

    /// Bytes the pager holds (allocated or retired) that neither the catalog nor the
    /// manifest root references: output in flight, extents awaiting reclamation, or leaked.
    /// Zero once idle and reclaimed (test hook).
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

    /// [`unreferenced_bytes`](Self::unreferenced_bytes) split: `(allocated, retired)`, the
    /// unreferenced bytes still allocated (output in flight or leaked: a compaction or
    /// flush that never committed and was never abandoned) and those retired, which a
    /// later reclaim frees once no view reaches them (test hook, D284).
    #[doc(hidden)]
    pub fn unreferenced_split(&self) -> (u64, u64) {
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
        (
            stats.allocated_bytes.saturating_sub(referenced),
            stats.retired_bytes,
        )
    }

    /// Holds the pager's allocator until the guard drops (test hook, D284): a reclaim a
    /// dropped view only tries (`Shared::try_reclaim`) skips meanwhile, as when another
    /// thread grows the file or commits.
    #[doc(hidden)]
    pub fn hold_pager_alloc(&self) -> impl Sized + '_ {
        self.inner.shared.pager.hold_alloc_for_test()
    }

    /// Every SST the current catalog names, as `(level, length, size class)` (test hook:
    /// the footprint test reads the live bytes and the SST count per run, #185).
    #[doc(hidden)]
    pub fn sst_lens(&self) -> Vec<(u8, u64, u8)> {
        let catalog = Arc::clone(&self.inner.shared.view.load().catalog);
        catalog
            .ssts
            .values()
            .flat_map(|l| l.iter())
            .map(|(level, meta)| (*level, meta.len, meta.extent.size_class))
            .collect()
    }

    /// Every SST the current catalog names, as `(table, first page, size class)` of its
    /// extent (test hook: where `shrink` put it, #314).
    #[doc(hidden)]
    pub fn sst_extents(&self) -> Vec<(TableId, u64, u8)> {
        let catalog = Arc::clone(&self.inner.shared.view.load().catalog);
        let mut out = Vec::new();
        for ((tablet, _), list) in catalog.ssts.iter() {
            let Some(entry) = catalog.tablet(*tablet) else {
                continue;
            };
            out.extend(
                list.iter()
                    .map(|(_, meta)| (entry.table, meta.extent.page, meta.extent.size_class)),
            );
        }
        out
    }

    /// Every SST the current catalog names, as `(table, level, sst id)` (test hook).
    #[doc(hidden)]
    pub fn sst_levels(&self) -> Vec<(TableId, u8, u64)> {
        let catalog = Arc::clone(&self.inner.shared.view.load().catalog);
        let mut out = Vec::new();
        for ((tablet, _), list) in catalog.ssts.iter() {
            let Some(entry) = catalog.tablet(*tablet) else {
                continue;
            };
            out.extend(
                list.iter()
                    .map(|(level, meta)| (entry.table, *level, meta.id.0)),
            );
        }
        out
    }

    /// Sets the inline value limit (D16) to `payload_bytes`: a put whose value payload is
    /// longer is separated into a blob file at commit time (#230). Lets tests reach that
    /// path with small values (the real limit is tens of MiB). Test hook.
    #[doc(hidden)]
    pub fn set_inline_value_limit(&self, payload_bytes: usize) {
        self.inner.max_value.store(payload_bytes, Ordering::Relaxed);
    }

    /// Every blob file the current catalog names, as `(family, blob file id, total bytes,
    /// live bytes)` (test hook).
    #[doc(hidden)]
    pub fn blob_files(&self) -> Vec<(FamilyId, u32, u64, u64)> {
        let catalog = Arc::clone(&self.inner.shared.view.load().catalog);
        catalog
            .blob_files
            .iter()
            .map(|(id, b)| (b.family, id.0, b.total_bytes, b.live_bytes))
            .collect()
    }

    /// Checks blob accounting in the current view (test hook): every blob pointer an SST
    /// holds within its tablet's rows names a blob file of the catalog, of the same family,
    /// and each file's live bytes are exactly the bytes those pointers reference (16 plus
    /// the value's length each; an SST shared by several tablets counts once per tablet,
    /// for its rows in that tablet; memtable pointers count too), and every SST's recorded
    /// blob references (#240) are
    /// exactly the pointers it holds. Describes the first mismatch.
    #[doc(hidden)]
    pub fn check_blob_accounting(&self) -> std::result::Result<(), String> {
        use pigeonhole_compaction::{blob_pointer, record_bytes};
        use pigeonhole_format::Cursor;
        use pigeonhole_format::key::{Kind, split_suffix};
        use pigeonhole_sst::{ReadOptions, ScanFilter};

        // A file is settling if `large_pending` holds it before or after the view is loaded:
        // a release (`large.rs`) drops it from `large_pending` only after the view without
        // it is published (so the set read before covers a view that still has it), and a
        // new file joins `large_pending` before the view with it is published (so the set
        // read after covers that).
        let settling = || self.inner.shared.large.pending();
        let mut pending = settling();
        let view = self.inner.shared.view.load_full();
        pending.extend(settling());
        let catalog = &view.catalog;
        let mut refs: BTreeMap<u32, u64> = BTreeMap::new();
        let err = |e: &dyn std::fmt::Display| e.to_string();
        for tablet in catalog.tablets() {
            let range = crate::compact::tablet_range(&tablet).map_err(|e| err(&e))?;
            for family in catalog.family_ids_of(tablet.table) {
                let Some(fam) = view.ssts.family(tablet.id, family) else {
                    continue;
                };
                for sst in fam.iter() {
                    let reader = sst
                        .reader(&view.ssts, pigeonhole_cache::Priority::Low)
                        .map_err(|e| err(&e))?;
                    let mut it = reader.iter(ScanFilter::all(), ReadOptions::default());
                    match &range.start {
                        Some(s) => it.seek(s),
                        None => it.seek_to_first(),
                    }
                    .map_err(|e| err(&e))?;
                    while it.valid() {
                        if range.end.as_deref().is_some_and(|e| it.key() >= e) {
                            break;
                        }
                        let (_, _, _, kind) = split_suffix(it.key()).map_err(|e| err(&e))?;
                        if kind == Kind::Put
                            && let Some(p) = blob_pointer(it.value())
                        {
                            match catalog.blob_files.get(&p.blob_file) {
                                Some(b) if b.family == family => {}
                                Some(b) => {
                                    return Err(format!(
                                        "SST {} of family {} points into blob file {} of family {}",
                                        sst.meta.id.0, family.0, p.blob_file.0, b.family.0
                                    ));
                                }
                                None => {
                                    return Err(format!(
                                        "SST {} points into blob file {}, which the catalog \
                                         does not name",
                                        sst.meta.id.0, p.blob_file.0
                                    ));
                                }
                            }
                            *refs.entry(p.blob_file.0).or_default() += record_bytes(p.len);
                        }
                        it.next().map_err(|e| err(&e))?;
                    }
                }
            }
        }
        // Memtables hold the pointers of values separated at commit time (#230) until their
        // flush; a frozen memtable never appears together with its SST.
        for (_, set) in view.all_memtables() {
            for reader in &set.readers {
                let mut it = reader.iter();
                it.seek_to_first().map_err(|e| err(&e))?;
                while it.valid() {
                    let (_, _, _, kind) = split_suffix(it.key()).map_err(|e| err(&e))?;
                    if kind == Kind::Put
                        && let Some(p) = blob_pointer(it.value())
                    {
                        if !catalog.blob_files.contains_key(&p.blob_file) {
                            return Err(format!(
                                "a memtable points into blob file {}, which the catalog \
                                 does not name",
                                p.blob_file.0
                            ));
                        }
                        *refs.entry(p.blob_file.0).or_default() += record_bytes(p.len);
                    }
                    it.next().map_err(|e| err(&e))?;
                }
            }
        }
        // Each SST's recorded references (#240) are exactly the pointers it holds.
        let mut seen = std::collections::HashSet::new();
        for fam in view.ssts.map.values() {
            for sst in fam.iter() {
                if !seen.insert(sst.meta.id) {
                    continue;
                }
                let Some(recorded) = catalog.blob_refs.get(&sst.meta.id) else {
                    continue;
                };
                let reader = sst
                    .reader(&view.ssts, pigeonhole_cache::Priority::Low)
                    .map_err(|e| err(&e))?;
                let mut it = reader.iter(ScanFilter::all(), ReadOptions::default());
                it.seek_to_first().map_err(|e| err(&e))?;
                let mut actual = Vec::new();
                while it.valid() {
                    pigeonhole_compaction::note_blob_ref(&mut actual, it.key(), it.value());
                    it.next().map_err(|e| err(&e))?;
                }
                if &actual != recorded {
                    return Err(format!(
                        "SST {} records blob references {recorded:?} but holds {actual:?}",
                        sst.meta.id.0
                    ));
                }
            }
        }
        // A large value's file whose commit has not settled (or whose release has not
        // committed) may have nothing pointing into it yet (#230).
        for (id, b) in catalog.blob_files.iter() {
            if pending.contains(id) {
                continue;
            }
            let referenced = refs.get(&id.0).copied().unwrap_or(0);
            if referenced != b.live_bytes {
                return Err(format!(
                    "blob file {}: {} live bytes recorded, {referenced} referenced \
                     ({} written)",
                    id.0, b.live_bytes, b.total_bytes
                ));
            }
        }
        Ok(())
    }

    /// Makes shard `shard` refuse the next `n` PREPAREs as overlapping an undecided share
    /// (`PendingOverlap`), whatever their rows (test hook: the cost of refused rounds). Each
    /// refused round aborts with no record on that shard, and its coordinator retries once
    /// the abort's decision there wakes it.
    #[doc(hidden)]
    pub fn refuse_prepares(&self, shard: usize, n: u64) {
        self.inner.shared.metrics[shard]
            .hooks
            .refuse_prepares
            .store(n, Ordering::Release);
    }

    /// The mean commits in flight shard `shard`'s balancer published at its last pass
    /// (D257's moving average of the rise of `commit_inflight_nanos` over each interval),
    /// `None` before its first pass. Test hook: one blocking client reads at most about 1.
    #[doc(hidden)]
    pub fn shard_inflight(&self, shard: usize) -> Option<f64> {
        self.inner.shared.loads[shard]
            .read_inflight_fresh(u64::MAX, u64::MAX)
            .map(|v| v as f64 / 1e6)
    }

    /// The stall fraction shard `shard`'s balancer published at its last pass (D255: the
    /// moving average of the time its commits waited for arena room or were paced on L0
    /// depth, 0 to 1), `None` before its first pass. Test hook.
    #[doc(hidden)]
    pub fn shard_stall_busy(&self, shard: usize) -> Option<f64> {
        self.inner.shared.loads[shard]
            .read_stall_busy_fresh(u64::MAX, u64::MAX)
            .map(|v| v as f64 / 1e6)
    }

    /// The busy fraction and the turn-busy fraction shard `shard`'s balancer published at
    /// its last pass (D217's moving average of the largest of turn + pool, sync and stall,
    /// D256, D257; and D251's turn-and-stall average), `None` before its first pass. Test
    /// hook: with the pool term above zero the first exceeds the second.
    #[doc(hidden)]
    pub fn shard_busy_terms(&self, shard: usize) -> Option<(f64, f64)> {
        let slot = &self.inner.shared.loads[shard];
        let busy = slot.read_busy_fresh(u64::MAX, u64::MAX)?;
        let turn = slot.read_turn_busy_fresh(u64::MAX, u64::MAX)?;
        Some((busy as f64 / 1e6, turn as f64 / 1e6))
    }

    /// Claims shard `shard` as a skew change handing it load would (D224), for `passes` of
    /// its balancer passes. False while another claim holds it. Test hook: D270's packing
    /// moves leave a claimed shard alone.
    #[doc(hidden)]
    pub fn claim_shard(&self, shard: usize, passes: u64) -> bool {
        let now = self.inner.shared.vfs.monotonic_nanos();
        self.inner.shared.loads[shard].claim(passes, u64::MAX, now)
    }

    /// Whether a skew change's claim holds shard `shard` now (D224). Test hook: D270's
    /// change that waits on a compaction claims nothing.
    #[doc(hidden)]
    pub fn shard_claimed(&self, shard: usize) -> bool {
        let now = self.inner.shared.vfs.monotonic_nanos();
        !self.inner.shared.loads[shard].claimable(now)
    }

    /// The active shard count (`k`, D217's packing) each shard's balancer used at its last
    /// pass, by shard (`None` before its first pass). Test hook: every shard must use the same.
    #[doc(hidden)]
    pub fn pack_ks(&self) -> Vec<Option<usize>> {
        self.inner
            .shared
            .metrics
            .iter()
            .map(|m| match m.hooks.pack_k.load(Ordering::Relaxed) {
                0 => None,
                v => Some(v as usize - 1),
            })
            .collect()
    }

    /// Bytes in the block cache (test hook: `shrink`'s tests check that an abandoned copy
    /// leaves nothing cached).
    #[doc(hidden)]
    pub fn block_cache_usage(&self) -> usize {
        self.inner.shared.cache.usage()
    }

    /// Runs `f` once, in the calling thread's next row read, after its first family and
    /// before its second: where a test flushes, compacts and reclaims while the read holds
    /// its view (#392 review; test hook).
    #[doc(hidden)]
    pub fn between_row_read_families(&self, f: Box<dyn FnOnce()>) {
        crate::read::BETWEEN_ROW_FAMILIES.with(|h| *h.borrow_mut() = Some(f));
    }

    /// Splits the tablet of `table` holding row `at` at `at`; both halves stay on its shard.
    /// Fails with `InvalidArgument` when `at` is the tablet's first row.
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
            failed: None,
            owner: pigeonhole_io::sys::process_id(),
        })
    }

    /// The largest default timestamp handed out (the allocator's `H`, D271): every default
    /// timestamp so far is at or below it.
    #[doc(hidden)]
    pub fn last_default_ts(&self) -> pigeonhole_format::Timestamp {
        self.inner.shared.clock.last_ts()
    }

    /// Memtable jumps reads have taken in this process (D194). Test hook.
    #[doc(hidden)]
    pub fn tail_skips() -> u64 {
        crate::shard::TAIL_SKIPS.load(Ordering::Relaxed)
    }

    /// SST column skips reads have taken in this process (ICR 0041). Test hook.
    #[doc(hidden)]
    pub fn sst_skips() -> u64 {
        crate::shard::SST_SKIPS.load(Ordering::Relaxed)
    }

    /// Breaks the flush GC on purpose (#287), so a test can check the oracle catches it;
    /// [`FlushGcMutation::None`] restores it.
    #[doc(hidden)]
    pub fn mutate_flush_gc(&self, m: FlushGcMutation) {
        let v = match m {
            FlushGcMutation::None => 0,
            FlushGcMutation::DropGuard => 1,
            FlushGcMutation::DropSnapshotFloor => 2,
            FlushGcMutation::IgnoreVoids => 3,
            FlushGcMutation::CounterSourceInside => 4,
        };
        self.inner
            .shared
            .hooks
            .flush_gc_mutation
            .store(v, Ordering::Release);
    }

    /// Turns recording for [`take_compactions`](Self::take_compactions) (flushes too, #287)
    /// and [`take_appended`](Self::take_appended) on or off (off at open). A test that reads
    /// them turns it on first.
    #[doc(hidden)]
    pub fn record_history(&self, on: bool) {
        self.inner.shared.hooks.record.store(on, Ordering::Release);
    }

    /// The compactions committed since the last call (or since recording started), when
    /// recording is on ([`record_history`](Self::record_history)).
    #[doc(hidden)]
    pub fn take_compactions(&self) -> Vec<crate::compact::CompactionRecord> {
        let mut all = self
            .inner
            .shared
            .hooks
            .compactions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let (flushes, compactions) = std::mem::take(&mut *all).into_iter().partition(|r| r.flush);
        *all = flushes;
        compactions
    }

    /// Every compaction and flush record committed since the last call, in commit order
    /// (#287: a flush's GC drops history and may purge versions, so the model replays both).
    #[doc(hidden)]
    pub fn take_gc_records(&self) -> Vec<crate::compact::CompactionRecord> {
        std::mem::take(
            &mut *self
                .inner
                .shared
                .hooks
                .compactions
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    /// What a closed instance still holds of what `Engine::release_memory` releases (D265):
    /// how many memtable sets and SSTs the current view lists, the bytes in the block cache
    /// and in the row cache (`None` when it is off), the arenas' state and the error of a
    /// refused discard.
    #[doc(hidden)]
    pub fn memory_release(&self) -> MemoryRelease {
        let shared = &self.inner.shared;
        let view = shared.view.load();
        MemoryRelease {
            view_memtables: view.mems.iter().map(|m| m.map.len()).sum(),
            view_ssts: view.ssts.map.values().map(|f| f.iter().count()).sum(),
            block_cache_bytes: shared.cache.usage(),
            row_cache_bytes: shared.row_cache.as_ref().map(|r| r.usage()),
            arenas: match shared.arena_discard.load(Ordering::Acquire) {
                crate::shard::ARENAS_DISCARDED => ArenaDiscard::Discarded,
                crate::shard::ARENAS_REFUSED => ArenaDiscard::Refused,
                _ => ArenaDiscard::Pending,
            },
            arena_error: shared
                .hooks
                .arena_discard_error
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
        }
    }

    /// How many bytes of the shared-memory arenas are resident (`mincore`; Linux mappings
    /// only, `Unsupported` elsewhere and on the simulated VFS).
    #[doc(hidden)]
    pub fn arena_resident_bytes(&self) -> Result<usize> {
        Ok(self.inner.shared.shm.arena_resident_bytes()?)
    }

    /// Seqnos the shards hold as aborted cross-shard commits, as of each shard's last
    /// batch (test hook).
    #[doc(hidden)]
    pub fn aborted_seqnos(&self) -> u64 {
        self.inner
            .shared
            .metrics
            .iter()
            .map(|m| m.hooks.aborted.load(Ordering::Relaxed))
            .sum()
    }

    /// How many times shard `shard` found enough free arena bytes for a batch but no run
    /// long enough for its largest entry, and made it wait (issue #141).
    #[doc(hidden)]
    pub fn arena_run_waits(&self, shard: usize) -> u64 {
        self.inner.shared.metrics[shard]
            .hooks
            .run_waits
            .load(Ordering::Relaxed)
    }

    /// How many flushes on shard `shard` the manifest refused for a purge guard a delete had
    /// voided, so that they flushed again without it (#287, test hook).
    #[doc(hidden)]
    pub fn flush_voids(&self, shard: usize) -> u64 {
        self.inner.shared.metrics[shard]
            .hooks
            .voided_flushes
            .load(Ordering::Relaxed)
    }

    /// How many decisions on shard `shard` arrived before their share's PREPARE settled and
    /// waited for it (D279, test hook).
    #[doc(hidden)]
    pub fn parked_decisions(&self, shard: usize) -> u64 {
        self.inner.shared.metrics[shard]
            .hooks
            .parked_decisions
            .load(Ordering::Relaxed)
    }

    /// Switches `Fault::HoldDecisionRecords` off and kicks every shard, so the DECISIONs it
    /// held join their next group (test hook).
    #[doc(hidden)]
    pub fn release_held_decisions(&self) {
        self.inner
            .shared
            .hooks
            .faults
            .set(Fault::HoldDecisionRecords, false);
        if let Some(subs) = self.inner.shared.submitters.get() {
            for s in subs {
                let _ = s.submit(crate::shard::ShardMsg::Kick);
            }
        }
    }

    /// How many `Decide { commit: false }` shard `shard` sent as a coordinator (D286, test
    /// hook).
    #[doc(hidden)]
    pub fn abort_decides_sent(&self, shard: usize) -> u64 {
        self.inner.shared.metrics[shard]
            .hooks
            .abort_decides
            .load(Ordering::Relaxed)
    }

    /// How many memtable entries read validation on shard `shard` has visited (D273, test
    /// hook).
    #[doc(hidden)]
    pub fn validate_entries(&self, shard: usize) -> u64 {
        self.inner.shared.metrics[shard]
            .hooks
            .validate_entries
            .load(Ordering::Relaxed)
    }

    /// [`Engine::backup`] at `snapshot`: with [`Snapshot::at_seqno`], below commits its SSTs
    /// already hold, so the backup rewrites those SSTs instead of copying them (#715).
    #[doc(hidden)]
    pub fn backup_at(&self, snapshot: Snapshot, dest: &Path) -> Result<()> {
        let _guard = self.inner.enter_maintenance()?;
        crate::maintenance::backup(&self.inner.shared, snapshot, dest)
    }

    /// How many compactions on shard `shard` a write voided before they installed their
    /// purge, so that they compacted again (#316).
    #[doc(hidden)]
    pub fn compaction_purge_voids(&self, shard: usize) -> u64 {
        self.inner.shared.metrics[shard]
            .hooks
            .purge_voids
            .load(Ordering::Relaxed)
    }

    /// How many `AwaitFlushed` and `FlushedThrough` messages shard `shard` handled: one pair
    /// per peer a blocked checkpoint waits for, not one per share (D266). Test hook.
    #[doc(hidden)]
    pub fn flushed_msgs(&self, shard: usize) -> u64 {
        self.inner.shared.metrics[shard]
            .hooks
            .flushed_msgs
            .load(Ordering::Relaxed)
    }

    /// How many spans shard `shard` added to its commits' in-flight time: one per commit it
    /// replied to (D257) and one per share it decided for another shard's coordination
    /// (D262); its own share counts nothing at the decision. Test hook.
    #[doc(hidden)]
    pub fn inflight_counts(&self, shard: usize) -> u64 {
        self.inner.shared.metrics[shard]
            .hooks
            .inflight_counts
            .load(Ordering::Relaxed)
    }

    /// The records shard `shard`'s checkpoint has not passed yet, as of its last batch: a
    /// test checks a participant's PREPAREs go once their commits settle (D254).
    #[doc(hidden)]
    pub fn logged_records(&self, shard: usize) -> u64 {
        self.inner.shared.metrics[shard]
            .hooks
            .logged
            .load(Ordering::Relaxed)
    }

    /// The most records shard `shard`'s checkpoint popped from its log in one batch so far
    /// (#763: at most the pop budget while open). Test hook.
    #[doc(hidden)]
    pub fn max_checkpoint_pops(&self, shard: usize) -> u64 {
        self.inner.shared.metrics[shard]
            .hooks
            .checkpoint_pops_max
            .load(Ordering::Relaxed)
    }

    /// Shard `shard`'s arena after its last batch: free bytes, the usable bytes of its
    /// largest run of free chunks, and its size (issue #141).
    #[doc(hidden)]
    pub fn arena_free(&self, shard: usize) -> (u64, u64, u64) {
        *self.inner.shared.metrics[shard]
            .hooks
            .arena
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The WAL records appended since the last call (or since recording started), in append
    /// order, when recording is on ([`record_history`](Self::record_history)).
    #[doc(hidden)]
    pub fn take_appended(&self) -> Vec<AppendedRecord> {
        std::mem::take(
            &mut *self
                .inner
                .shared
                .hooks
                .appended
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    /// PREPAREs refused for arena room since open (D245). Counted only in builds with this
    /// feature (test hook).
    #[doc(hidden)]
    pub fn room_refusals(&self) -> u64 {
        self.inner
            .shared
            .hooks
            .room_refusals
            .load(Ordering::Relaxed)
    }

    /// The seqnos of cross-shard attempts refused by a participant (`Moved`, `Room` or
    /// `PendingOverlap`) since the last call, when recording is on
    /// ([`record_history`](Self::record_history)): their PREPAREs never get a COMMIT, and the
    /// commit runs again under a new seqno or gives up with `Busy`.
    #[doc(hidden)]
    pub fn take_refused_attempts(&self) -> Vec<Seqno> {
        std::mem::take(
            &mut *self
                .inner
                .shared
                .hooks
                .refused
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    /// Takes the manifest writer's exclusion on this thread, as a commit in progress holds
    /// it: shards' manifest requests queue up (their pumps find it held and leave) until
    /// [`release_manifest`](Self::release_manifest). Returns whether it was free (test hook).
    #[doc(hidden)]
    pub fn hold_manifest(&self) -> bool {
        manifest::claim(&self.inner.shared)
    }

    /// How many manifest requests wait in the queue, behind a held exclusion
    /// ([`hold_manifest`](Self::hold_manifest)) or a commit in progress (test hook).
    #[doc(hidden)]
    pub fn manifest_queue_len(&self) -> usize {
        self.inner.shared.manifest_queue.len()
    }

    /// Commits every queued manifest request on this thread, which holds the exclusion
    /// ([`hold_manifest`](Self::hold_manifest)) (test hook).
    #[doc(hidden)]
    pub fn drain_manifest(&self) {
        manifest::drain_sync(&self.inner.shared);
    }

    /// Commits what is queued and releases the exclusion
    /// [`hold_manifest`](Self::hold_manifest) took (test hook).
    #[doc(hidden)]
    pub fn release_manifest(&self) {
        let shared = &self.inner.shared;
        loop {
            manifest::drain_sync(shared);
            manifest::release(shared);
            if shared.manifest_queue.is_empty() || !manifest::claim(shared) {
                break;
            }
        }
    }

    /// Whether a shard died (a panic, a dropped application-owned shard, a failed WAL sync or
    /// a failed cross-shard apply): new commits are then refused with `Closed` (D227). Lets
    /// the model checker tell that refusal from a wrong one. Test hook.
    #[doc(hidden)]
    pub fn shard_died(&self) -> bool {
        self.inner.shared.shard_died.load(Ordering::Acquire)
    }

    /// While `park` is set, a background manifest commit whose root commit completed waits
    /// before it publishes and answers; clearing it wakes the parked commit (test hook).
    #[doc(hidden)]
    pub fn park_manifest_commits(&self, park: bool) {
        let shared = &self.inner.shared;
        shared.hooks.manifest_park.store(park, Ordering::Release);
        if !park {
            let parked = shared
                .hooks
                .manifest_parked
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(w) = parked {
                w.wake();
            }
        }
    }

    /// The oldest `(seqno, view version)` any reader slot of the current region generation
    /// pins, and the view version the writer published last (test hook).
    #[doc(hidden)]
    pub fn reader_pin_and_view(&self) -> (Option<(Seqno, u64)>, u64) {
        let shared = &self.inner.shared;
        (shared.shm.oldest_reader_pin(), shared.view.load().version)
    }

    /// While `hold` is set, every compaction job waits before its first slice until
    /// [`release_held_compaction`](Self::release_held_compaction) lets it run; clearing it
    /// lets them all run (test hook, #316).
    #[doc(hidden)]
    pub fn hold_compactions(&self, hold: bool) {
        let hooks = &self.inner.shared.hooks;
        hooks.compaction_hold.store(hold, Ordering::Release);
        if !hold {
            let held = std::mem::take(
                &mut *hooks
                    .compaction_held
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner),
            );
            for (_, w) in held {
                w.wake();
            }
        }
    }

    /// Lets the oldest held compaction job run (test hook).
    #[doc(hidden)]
    pub fn release_held_compaction(&self) {
        let mut held = self
            .inner
            .shared
            .hooks
            .compaction_held
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let first = (!held.is_empty()).then(|| held.remove(0));
        drop(held);
        if let Some((_, w)) = first {
            w.wake();
        }
    }

    /// Compactions that finished their job and submitted their commit (test hook, D284):
    /// one stopped before this is still writing its output.
    #[doc(hidden)]
    pub fn compaction_commits_submitted(&self) -> u64 {
        self.inner
            .shared
            .hooks
            .compaction_commits
            .load(Ordering::Acquire)
    }

    /// While set, every compaction job's slice does a few units of work (one clock check's
    /// worth) and yields (test hook, D284): a test can stop it part-way.
    #[doc(hidden)]
    pub fn compaction_tiny_slices(&self, on: bool) {
        self.inner
            .shared
            .hooks
            .compaction_tiny_slices
            .store(on, Ordering::Release);
    }

    /// Whether a compaction job is held before its first slice (test hook).
    #[doc(hidden)]
    pub fn compaction_held(&self) -> bool {
        !self
            .inner
            .shared
            .hooks
            .compaction_held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    }

    /// Threads that parked waiting for the manifest writer (`commit_req_from_thread`):
    /// counted after their last look at the exclusion and the pump's commit, so from then
    /// on only a wake (a release, a root commit's completion, their own request's answer)
    /// unparks them (test hook).
    #[doc(hidden)]
    pub fn manifest_thread_parks(&self) -> usize {
        self.inner
            .shared
            .hooks
            .manifest_thread_parks
            .load(Ordering::Acquire)
    }

    /// Whether a manifest pump's root commit is in flight, not yet completed (test hook).
    #[doc(hidden)]
    pub fn manifest_commit_in_flight(&self) -> bool {
        self.inner.shared.manifest_flight.in_flight()
    }

    /// Whether a background manifest commit is parked (test hook).
    #[doc(hidden)]
    pub fn manifest_commit_parked(&self) -> bool {
        self.inner
            .shared
            .hooks
            .manifest_parked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    /// Whether every shard has closed and the final close waits for the manifest writer
    /// (test hook).
    #[doc(hidden)]
    pub fn final_close_pending(&self) -> bool {
        self.inner.shared.close.pending()
    }

    /// Whether the final close has run (test hook: application-owned shards finish the close
    /// as they are driven).
    #[doc(hidden)]
    pub fn close_finished(&self) -> bool {
        self.inner.shared.closed.load(Ordering::Acquire)
    }

    /// Commits an empty manifest delta from this thread while a second request lands in
    /// the window between the drain's last `begin` and the release of the writer's
    /// exclusion (as a shard's submit would, whose pump then leaves). Returns whether that
    /// second request was committed too, which the release-then-re-check rule guarantees.
    #[doc(hidden)]
    pub fn probe_manifest_release_window(&self) -> Result<bool> {
        use std::task::{Context, Poll, Waker};
        let shared = &self.inner.shared;
        shared.hooks.manifest_race.store(true, Ordering::Release);
        manifest::commit_from_thread(shared, manifest::ReqKind::Edits(Vec::new()))?;
        let waiter = shared
            .hooks
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
    #[doc(hidden)]
    pub fn inspect_manifest(vfs: &pigeonhole_io::VfsRef, path: &Path) -> Result<ManifestInfo> {
        let opened = Pager::open(vfs, path, false)?;
        let clean = opened.clean_shutdown();
        let (catalog, _) = manifest::load(&opened, 1, Arc::new(MergeRegistry::default()))?;
        Ok(ManifestInfo {
            version: opened.root().manifest_version,
            checkpoints: (*catalog.checkpoints).clone(),
            flushed: (*catalog.flushed).clone(),
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

/// A tablet, its owning shard and its row range `[start, end)` (a test hook).
#[doc(hidden)]
pub type TabletOwner = (TabletId, u16, Vec<u8>, Option<Vec<u8>>);

impl TabletMap {
    /// The tablets of `table` in row order, as `(tablet, shard, start, end)` (a test hook).
    #[doc(hidden)]
    pub fn ranges(&self, table: TableId) -> Vec<TabletOwner> {
        self.tablets_of(table)
            .iter()
            .map(|t| (t.id, t.shard.0, t.start.clone(), t.end.clone()))
            .collect()
    }
}
