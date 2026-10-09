//! Test hooks: the `test-hooks` feature, which only the engine's own tests and benches
//! enable (its dev-dependency on itself). Everything a test reaches into the engine with
//! is here: the `#[doc(hidden)]` `Engine` methods, the types they return, and the state
//! they keep in `Shared::hooks`, `ShardMetrics::hooks` and `ReaderState::hooks`.
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
    /// Whether `compactions` and `appended` record (`Engine::record_history`); off until a
    /// test turns it on.
    pub record: AtomicBool,
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
    /// Makes the next group's second member wait for arena room, as a full arena would
    /// (`Engine::force_room_wait_once`, #315 review).
    pub room_wait_once: AtomicBool,
    /// Runs in the next writer `snapshot()`, between pinning its seqno and loading the view
    /// (`Engine::before_snapshot_view_load`, #315 review).
    pub before_snapshot_view_load: Once,
    /// Runs in the next shrink round, between its catalog read and its relocations
    /// (`Engine::before_shrink_relocates`).
    pub before_shrink_relocates: Once,
    /// Runs once `backup` has released its snapshot's memtables, before its long merge
    /// (`Engine::after_backup_releases_memtables`).
    pub after_backup_releases_memtables: Once,
    /// Runs in the next shrink round that commits, after its copies are written and before
    /// the commit (`Engine::before_shrink_commits`).
    pub before_shrink_commits: Once,
    /// Parks background manifest commits before `end` (`manifest::parked`,
    /// `Engine::park_manifest_commits`), and the parked pump's waker.
    pub manifest_park: AtomicBool,
    pub manifest_parked: Mutex<Option<TaskWaker>>,
    /// Holds each compaction job before its first slice (`Engine::hold_compactions`), and
    /// the held job's waker (`Engine::release_held_compaction` takes it to let it run).
    pub compaction_hold: AtomicBool,
    pub compaction_held: Mutex<Option<TaskWaker>>,
    /// Refuses batches of only `WalCheckpoint` edits as `NoSpace`, as a snapshot rewrite
    /// that finds no space does (`Engine::refuse_checkpoints`).
    pub refuse_checkpoints: AtomicBool,
    /// Commits no `SstBlobRefs` edit and keeps no blob references, as a build from before
    /// tag 13 (#240) wrote (`Engine::omit_blob_refs`).
    pub omit_blob_refs: AtomicBool,
    /// Fails the next single-shard batch's apply with `Busy` after its WAL append, without
    /// applying it, as an arena miscount would (`Engine::fail_next_apply`).
    pub fail_next_apply: AtomicBool,
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
    /// Polls the current round's replies. Like the blocking `wait` (#148, review 1-2 F10),
    /// it resolves only once every shard has replied, with the last failure if any, so the
    /// harness sees what production does: never a result while other shards still work.
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
            failed: None,
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

    /// Every SST the current catalog names, as `(level, length, size class)` (test hook:
    /// the footprint test reads the live bytes and the SST count per run, #185).
    #[doc(hidden)]
    pub fn sst_lens(&self) -> Vec<(u8, u64, u8)> {
        let catalog = Arc::clone(&self.inner.shared.view.load().catalog);
        catalog
            .ssts
            .values()
            .flatten()
            .map(|(level, meta)| (*level, meta.len, meta.extent.size_class))
            .collect()
    }

    /// Every SST the current catalog names, as `(table, first page, size class)` of its
    /// extent (test hook: where `shrink` put it, #314).
    #[doc(hidden)]
    pub fn sst_extents(&self) -> Vec<(TableId, u64, u8)> {
        let catalog = Arc::clone(&self.inner.shared.view.load().catalog);
        let mut out = Vec::new();
        for ((tablet, _), list) in &catalog.ssts {
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

        let view = self.inner.shared.view.load_full();
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
        let pending = self
            .inner
            .shared
            .large_pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        for (id, b) in &catalog.blob_files {
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

    /// The next commit group's second member waits for room as if the arena were full: the
    /// stall path freezes memtables after the group's first member was admitted (test hook,
    /// #315 review).
    #[doc(hidden)]
    pub fn force_room_wait_once(&self) {
        self.inner
            .shared
            .hooks
            .room_wait_once
            .store(true, Ordering::Release);
    }

    /// Runs `f` once, on the calling thread of the next writer `snapshot()`, after it pinned
    /// its seqno and before it loads the view: where a test publishes a flush in between
    /// (#315 review; test hook).
    #[doc(hidden)]
    pub fn before_snapshot_view_load(&self, f: Box<dyn FnOnce() + Send>) {
        self.inner.shared.hooks.before_snapshot_view_load.set(f);
    }

    /// Runs `f` once, on the calling thread of the next `backup`, right after it released its
    /// snapshot's memtables and before it merges the SSTs: where a test writes past the
    /// arena while the backup still runs (#262; test hook).
    #[doc(hidden)]
    pub fn after_backup_releases_memtables(&self, f: Box<dyn FnOnce() + Send>) {
        self.inner
            .shared
            .hooks
            .after_backup_releases_memtables
            .set(f);
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
            failed: None,
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

    /// Breaks the flush GC on purpose (#287), so a test can check the oracle catches it;
    /// [`FlushGcMutation::None`] restores it.
    #[doc(hidden)]
    pub fn mutate_flush_gc(&self, m: FlushGcMutation) {
        let v = match m {
            FlushGcMutation::None => 0,
            FlushGcMutation::DropGuard => 1,
            FlushGcMutation::DropSnapshotFloor => 2,
            FlushGcMutation::IgnoreVoids => 3,
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

    /// How many compactions on shard `shard` a write voided before they installed their
    /// purge, so that they compacted again (#316).
    #[doc(hidden)]
    pub fn compaction_purge_voids(&self, shard: usize) -> u64 {
        self.inner.shared.metrics[shard]
            .hooks
            .purge_voids
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

    /// While `omit` is set, manifest commits drop every `SstBlobRefs` edit and the catalog
    /// keeps no blob references, so the file looks as one written before tag 13 (#240).
    /// Test hook.
    #[doc(hidden)]
    pub fn omit_blob_refs(&self, omit: bool) {
        self.inner
            .shared
            .hooks
            .omit_blob_refs
            .store(omit, Ordering::Release);
    }

    /// Fails the next single-shard batch after its WAL append: it is not applied, the shard
    /// poisons itself and the commit returns `Busy`, as an arena miscount would (#230).
    /// Test hook.
    #[doc(hidden)]
    pub fn fail_next_apply(&self) {
        self.inner
            .shared
            .hooks
            .fail_next_apply
            .store(true, Ordering::Release);
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

    /// While `hold` is set, every compaction job waits before its first slice until
    /// [`release_held_compaction`](Self::release_held_compaction) lets it run; clearing it
    /// lets them all run (test hook, #316).
    #[doc(hidden)]
    pub fn hold_compactions(&self, hold: bool) {
        let hooks = &self.inner.shared.hooks;
        hooks.compaction_hold.store(hold, Ordering::Release);
        if !hold {
            self.release_held_compaction();
        }
    }

    /// Lets the held compaction job run (test hook).
    #[doc(hidden)]
    pub fn release_held_compaction(&self) {
        let held = self
            .inner
            .shared
            .hooks
            .compaction_held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(w) = held {
            w.wake();
        }
    }

    /// Whether a compaction job is held before its first slice (test hook).
    #[doc(hidden)]
    pub fn compaction_held(&self) -> bool {
        self.inner
            .shared
            .hooks
            .compaction_held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
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
