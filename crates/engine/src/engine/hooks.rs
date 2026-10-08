//! Test hooks: the `test-hooks` feature, which only the engine's own tests and benches
//! enable (its dev-dependency on itself). Everything a test reaches into the engine with
//! is here: the `#[doc(hidden)]` `Engine` methods, the types they return, and the state
//! they keep in `Shared::hooks`, `ShardMetrics::hooks` and `ReaderState::hooks`.
//!
//! The rules:
//! - Every hook is used by a committed test; delete one when its last test goes.
//! - A hook does nothing until a test sets it: callbacks run once, flags start clear.
//!   The exceptions record what a test reads back later (`take_appended`,
//!   `take_compactions` and the [`ShardCounters`]): they record in every `test-hooks`
//!   build, but nothing reads them except a test. None changes what the engine does.
//! - Prefer a public or application-owned seam (`EngineShard` over `SimVfs`,
//!   `Engine::shard_stats`) to a new hook.
//!
//! The `#[cfg(feature = "test-hooks")]` sites elsewhere in the crate only run, check or
//! record into this state; `manifest.rs` keeps the three helpers that need its private
//! commit state (`race_window`, `parked`, `parks`).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use pigeonhole_compaction::MergeRegistry;
use pigeonhole_format::{
    Durability, FamilyId, Lsn, ManifestVersion, Seqno, StreamId, TableId, TabletId, Timestamp,
};
use pigeonhole_pager::Pager;
use pigeonhole_runtime::{ShardId, TaskWaker, Waiter, completion};

pub use super::PendingMaintenance;
pub use crate::compact::CompactionRecord;

use super::{CompactRounds, Engine, Inner, Role};
use crate::manifest;
use crate::shard::ShardMsg;
use crate::snapshot::{Snapshot, TabletMap};
use crate::{Error, Result};

/// A callback a test sets to run once, the next time the engine reaches its site.
#[derive(Default)]
pub(crate) struct Once(Mutex<Option<Box<dyn FnOnce() + Send>>>);

impl Once {
    /// Sets the callback, replacing one that has not run.
    pub(crate) fn set(&self, f: Box<dyn FnOnce() + Send>) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(f);
    }

    /// Runs the callback if one is set (outside the lock).
    pub(crate) fn run(&self) {
        let f = self.0.lock().unwrap_or_else(PoisonError::into_inner).take();
        if let Some(f) = f {
            f();
        }
    }
}

/// The engine-wide hook state (`Shared::hooks`).
#[derive(Default)]
pub(crate) struct Hooks {
    /// Every committed compaction (`Engine::take_compactions`; the records are never built
    /// without the feature, 5-6 6.2).
    pub compactions: Mutex<Vec<CompactionRecord>>,
    /// Every WAL record appended, in append order (`Engine::take_appended`).
    pub appended: Mutex<Vec<AppendedRecord>>,
    /// Arms the manifest queue's release window (`manifest::race_window`,
    /// `Engine::probe_manifest_release_window`), and the waiter of the request it pushed.
    pub manifest_race: AtomicBool,
    pub manifest_race_waiter: Mutex<Option<Waiter<Result<ManifestVersion>>>>,
    /// Runs at the start of the next `publish_view`, before the publish lock
    /// (`Engine::before_next_view_publish`).
    pub before_view_publish: Once,
    /// Runs in the next shrink round, between its catalog read and its relocations
    /// (`Engine::before_shrink_relocates`).
    pub before_shrink_relocates: Once,
    /// Runs in the next shrink round that commits, after its copies are written and before
    /// the commit (`Engine::before_shrink_commits`).
    pub before_shrink_commits: Once,
    /// Parks background manifest commits before `end` (`manifest::parked`,
    /// `Engine::park_manifest_commits`), and the parked pump's waker.
    pub manifest_park: AtomicBool,
    pub manifest_parked: Mutex<Option<TaskWaker>>,
    /// Refuses batches of only `WalCheckpoint` edits as `NoSpace`, as a snapshot rewrite
    /// that finds no space does (`Engine::refuse_checkpoints`).
    pub refuse_checkpoints: AtomicBool,
}

/// A shard's test-hook counters (`ShardMetrics::hooks`), stored after each batch.
#[derive(Debug, Default)]
pub(crate) struct ShardCounters {
    /// Size of the shard's aborted-seqno set (`Engine::aborted_seqnos`).
    pub aborted: AtomicU64,
    /// The arena's free bytes, largest free run and size (`Engine::arena_free`: a test
    /// checks it built the fragmented arena it means to, issue #141).
    pub arena_free: AtomicU64,
    pub arena_run: AtomicU64,
    pub arena_len: AtomicU64,
    /// Reservations that found enough free bytes but no run long enough for their largest
    /// entry (`Engine::arena_run_waits`: a test checks it reached that case, issue #141).
    pub run_waits: AtomicU64,
}

/// A reader process's snapshot hooks (`ReaderState::hooks`).
#[derive(Default)]
pub(crate) struct ReaderHooks {
    /// Between reading the view record and loading the catalog of its manifest version.
    pub after_record: Once,
    /// After the durable root matched the record's manifest version, before the manifest
    /// is read.
    pub before_manifest_load: Once,
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

impl Engine {
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

    /// Every entry of every table in `snapshot`'s view, raw (no resolution), in key order
    /// per `(table, family)`.
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

    /// Every SST the current catalog names, as `(table, level, sst id)` (test hook).
    #[doc(hidden)]
    pub fn sst_levels(&self) -> Vec<(TableId, u8, u64)> {
        let catalog = Arc::clone(&self.inner.shared.view.load().catalog);
        let mut out = Vec::new();
        for ((tablet, _), list) in &catalog.ssts {
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

    /// Bytes in the block cache (test hook: `shrink`'s tests check that an abandoned copy
    /// leaves nothing cached).
    #[doc(hidden)]
    pub fn block_cache_usage(&self) -> usize {
        self.inner.shared.cache.usage()
    }

    /// Runs `f` once, in the next `shrink` round, after it has read the catalog and before it
    /// relocates anything: where a compaction or `drop_table` can commit under it (test
    /// hook).
    #[doc(hidden)]
    pub fn before_shrink_relocates(&self, f: Box<dyn FnOnce() + Send>) {
        self.inner.shared.hooks.before_shrink_relocates.set(f);
    }

    /// Runs `f` once, in the next `shrink` round that commits, after it has written its
    /// copies and before it commits them: where a `drop_table` makes a copy one to abandon
    /// (test hook).
    #[doc(hidden)]
    pub fn before_shrink_commits(&self, f: Box<dyn FnOnce() + Send>) {
        self.inner.shared.hooks.before_shrink_commits.set(f);
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
        })
    }

    /// The largest default-timestamp floor of any shard (including floors raised by shards
    /// handing over tablets): the next default timestamp anywhere is above it.
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

    /// Publishes `ts` as `shard`'s default-timestamp floor, whatever the shard assigned: what
    /// a coordinator reads when it loads the floor just before the shard publishes a higher
    /// one (a race between threads, issue #105). The shard's own floor is unchanged.
    #[doc(hidden)]
    pub fn publish_stale_ts_floor(&self, shard: u16, ts: pigeonhole_format::Timestamp) {
        self.inner.shared.ts_floors[usize::from(shard)]
            .0
            .store(ts, Ordering::Release);
    }

    /// The compactions committed since the last call (or since open).
    #[doc(hidden)]
    pub fn take_compactions(&self) -> Vec<crate::compact::CompactionRecord> {
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

    /// Shard `shard`'s arena after its last batch: free bytes, the usable bytes of its
    /// largest run of free chunks, and its size (issue #141).
    #[doc(hidden)]
    pub fn arena_free(&self, shard: usize) -> (u64, u64, u64) {
        let m = &self.inner.shared.metrics[shard].hooks;
        (
            m.arena_free.load(Ordering::Relaxed),
            m.arena_run.load(Ordering::Relaxed),
            m.arena_len.load(Ordering::Relaxed),
        )
    }

    /// The WAL records appended since the last call (or since open), in append order.
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

    /// Takes the manifest writer's exclusion on this thread, as a commit in progress holds
    /// it: shards' manifest requests queue up (their pumps find it held and leave) until
    /// [`release_manifest`](Self::release_manifest). Returns whether it was free (test hook).
    #[doc(hidden)]
    pub fn hold_manifest(&self) -> bool {
        manifest::claim(&self.inner.shared)
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

    /// Runs `f` once, at the start of the next view publish (a shard publishing its
    /// memtables or a manifest commit), before that publish takes the publish lock: where
    /// another thread's publish can land in between (test hook).
    #[doc(hidden)]
    pub fn before_next_view_publish(&self, f: Box<dyn FnOnce() + Send>) {
        self.inner.shared.hooks.before_view_publish.set(f);
    }

    /// While `refuse` is set, the manifest writer refuses every batch made only of WAL
    /// checkpoints with `NoSpace`, through the path a snapshot rewrite that finds no space
    /// takes (nothing written, the writer stays usable). Test hook.
    #[doc(hidden)]
    pub fn refuse_checkpoints(&self, refuse: bool) {
        self.inner
            .shared
            .hooks
            .refuse_checkpoints
            .store(refuse, Ordering::Release);
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

    /// Reader processes: runs `hook` once, in the next snapshot, between reading the view
    /// record from shared memory and loading the catalog of its manifest version (test hook).
    #[doc(hidden)]
    pub fn on_reader_view_record(&self, hook: Box<dyn FnOnce() + Send>) {
        if let Some(r) = &self.inner.reader {
            r.hooks.after_record.set(hook);
        }
    }

    /// Reader processes: runs `hook` once, when a snapshot found the durable root at the
    /// view record's manifest version and is about to read that manifest (test hook).
    #[doc(hidden)]
    pub fn on_reader_manifest_load(&self, hook: Box<dyn FnOnce() + Send>) {
        if let Some(r) = &self.inner.reader {
            r.hooks.before_manifest_load.set(hook);
        }
    }

    /// The oldest `(seqno, view version)` any reader slot of the current region generation
    /// pins, and the view version the writer published last (test hook).
    #[doc(hidden)]
    pub fn reader_pin_and_view(&self) -> (Option<(Seqno, u64)>, u64) {
        let shared = &self.inner.shared;
        (shared.shm.oldest_reader_pin(), shared.view.load().version)
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
        self.inner
            .shared
            .close
            .final_pending
            .load(Ordering::Acquire)
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
