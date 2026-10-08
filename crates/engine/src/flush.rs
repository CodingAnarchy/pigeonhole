//! Flushing frozen memtables to SSTs: a cooperative task per shard writes each frozen
//! memtable through `SstWriter` into pager extents (values above the family's blob
//! threshold into blob files, issue #33), syncs the WAL streams whose records the
//! data came from (so a flush never persists a share of a cross-shard commit before every
//! PREPARE and the COMMIT are durable), commits `AddSst` + `SetFlushed` edits through the
//! manifest writer, and reports back to the shard, which retires the memtables.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Poll;

use pigeonhole_cache::BlockCache;
use pigeonhole_compaction::{BlobSink, NewBlobFile, encode_blob_stored, note_blob_ref, separates};
use pigeonhole_format::key::split_suffix;
use pigeonhole_format::manifest::{Edit, FamilyOptions, SstMeta};
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::{
    BlobFileId, Cursor, FamilyId, ManifestVersion, Seqno, SstId, TableId, TabletId,
};
use pigeonhole_io::FileRef;
use pigeonhole_memtable::{MemIter, MemtableReader};
use pigeonhole_pager::Pager;
use pigeonhole_runtime::{ShardId, Task, TaskPoll, TaskWaker, Waiter, completion};
use pigeonhole_sst::{SstReader, SstWriter, SstWriterOptions};

use crate::catalog::Catalog;
use crate::manifest::{self, ManifestReq};
use crate::shard::{ShardMsg, Shared};
use crate::snapshot::SstSet;
use crate::waker::StdWaker;
use crate::{Error, Result};

/// Largest extent the pager hands out.
const MAX_EXTENT: u64 = 64 << 20;
/// Entries between clock checks while writing.
const CLOCK_EVERY: u32 = 256;

/// A frozen memtable to flush.
#[derive(Debug)]
pub(crate) struct FlushItem {
    pub table: TableId,
    pub tablet: TabletId,
    pub family: FamilyId,
    pub root: u32,
    pub reader: MemtableReader,
    /// Arena bytes the memtable holds (an upper bound on its SST size).
    pub bytes: u64,
    /// Every seqno at or below this of `(tablet, family)` is in the memtable or older ones.
    pub max_seqno: Seqno,
    /// Holds applied shares of cross-shard commits: every stream is synced before the
    /// manifest commit.
    pub has_shares: bool,
    /// The flush GC guard held when the memtable was queued (#287): no other source of the
    /// slot could hold a delete, so versions beyond `max_versions` may be purged. Its state
    /// (`shard::GUARD_*`) is shared with the shard: the commit installs the purge only if no
    /// delete in the family voided it meanwhile.
    pub guard: Option<Arc<std::sync::atomic::AtomicU8>>,
    pub options: FamilyOptions,
}

/// What the shard learns when a flush is done.
#[derive(Debug, Clone)]
pub(crate) struct FlushedItem {
    pub tablet: TabletId,
    pub family: FamilyId,
    pub root: u32,
    pub max_seqno: Seqno,
}

/// Separated values of a sink: the blob files and the family's threshold.
struct Separation {
    sink: BlobSink,
    threshold: u32,
}

/// Writes entries into SSTs cut at the extent size, allocating extents from the pager.
/// With [`SstSink::separating`], values above the blob threshold go to blob files and the
/// SSTs hold their pointers.
pub(crate) struct SstSink {
    pager: Arc<Pager>,
    file: FileRef,
    options: SstWriterOptions,
    /// Extent size to ask for (rounded up by the pager).
    estimate: u64,
    open: Option<(SstWriter, ExtentRef)>,
    pub outputs: Vec<SstMeta>,
    /// Each output's blob references (parallel to `outputs`; `Edit::SstBlobRefs`, #240).
    pub refs: Vec<Vec<(BlobFileId, u64)>>,
    /// The open SST's blob references.
    open_refs: Vec<(BlobFileId, u64)>,
    sst_ids: Arc<std::sync::atomic::AtomicU64>,
    separation: Option<Separation>,
    /// Blob files finished by [`SstSink::finish_blobs`].
    pub blob_files: Vec<NewBlobFile>,
}

impl SstSink {
    pub(crate) fn new(
        pager: Arc<Pager>,
        sst_ids: Arc<std::sync::atomic::AtomicU64>,
        options: SstWriterOptions,
        estimate: u64,
    ) -> Self {
        let file = pager.file().clone();
        Self {
            pager,
            file,
            options,
            estimate: estimate.clamp(64 << 10, MAX_EXTENT),
            open: None,
            outputs: Vec::new(),
            refs: Vec::new(),
            open_refs: Vec::new(),
            sst_ids,
            separation: None,
            blob_files: Vec::new(),
        }
    }

    /// Separates values above `threshold` (the family's `blob_threshold`) into blob files
    /// with ids from `blob_ids`. Call [`SstSink::finish_blobs`] after the last entry.
    pub(crate) fn separating(
        mut self,
        blob_ids: Arc<std::sync::atomic::AtomicU32>,
        threshold: u32,
        target_sst_bytes: u64,
    ) -> Self {
        if threshold != u32::MAX {
            self.separation = Some(Separation {
                sink: BlobSink::new(
                    Arc::clone(&self.pager),
                    blob_ids,
                    self.estimate / 2,
                    target_sst_bytes.saturating_mul(4),
                ),
                threshold,
            });
        }
        self
    }

    /// Adds an entry, separating its value first if it is a large put.
    pub(crate) fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if let Some(sep) = &mut self.separation
            && value.len() > sep.threshold as usize + 1
            && split_suffix(key).is_ok_and(|(_, _, _, kind)| separates(kind, value, sep.threshold))
        {
            let ptr = sep.sink.append(value)?;
            return self.add_raw(key, &encode_blob_stored(&ptr));
        }
        self.add_raw(key, value)
    }

    /// Finishes the blob files written so far into [`SstSink::blob_files`].
    pub(crate) fn finish_blobs(&mut self) -> Result<()> {
        if let Some(sep) = self.separation.take() {
            self.blob_files = sep.sink.finish()?;
        }
        Ok(())
    }

    fn add_raw(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if let Some((w, _)) = &self.open
            && !w.fits(key.len(), value.len())
        {
            self.cut()?;
        }
        if self.open.is_none() {
            let need = ((key.len() + value.len()) as u64 * 2 + (64 << 10)).max(self.estimate);
            let extent = self.pager.allocate(need.min(MAX_EXTENT))?;
            let id = SstId(self.sst_ids.fetch_add(1, Ordering::Relaxed));
            self.open = Some((
                SstWriter::new(self.file.clone(), extent, id, self.options.clone()),
                extent,
            ));
        }
        let (w, _) = self.open.as_mut().expect("opened above");
        w.add(key, value)?;
        note_blob_ref(&mut self.open_refs, key, value);
        Ok(())
    }

    /// Finishes the open SST, if any, trimming its extent to its length.
    pub(crate) fn cut(&mut self) -> Result<()> {
        let Some((w, extent)) = self.open.take() else {
            return Ok(());
        };
        let refs = std::mem::take(&mut self.open_refs);
        if w.entries() == 0 {
            self.pager.abandon(w.abandon());
            return Ok(());
        }
        match w.finish() {
            Ok(mut meta) => {
                // The estimate is the memtable's size; give back what the SST did not use.
                meta.extent = self.pager.trim(meta.extent, meta.len);
                self.outputs.push(meta);
                self.refs.push(refs);
                Ok(())
            }
            Err(e) => {
                self.pager.abandon(extent);
                Err(e.into())
            }
        }
    }

    /// Returns every extent written so far to the pager.
    pub(crate) fn abandon(&mut self) {
        if let Some(mut sep) = self.separation.take() {
            sep.sink.abandon();
        }
        for f in self.blob_files.drain(..) {
            for e in f.extents {
                self.pager.abandon(e);
            }
        }
        if let Some((w, _)) = self.open.take() {
            self.pager.abandon(w.abandon());
        }
        for meta in self.outputs.drain(..) {
            self.pager.abandon(meta.extent);
        }
        self.refs.clear();
        self.open_refs.clear();
    }

    /// The `AddSst` and `SstBlobRefs` edits of the outputs (drained) for `(tablet, family)`
    /// at `level`.
    pub(crate) fn take_edits(
        &mut self,
        tablet: TabletId,
        family: FamilyId,
        level: u8,
    ) -> Vec<Edit> {
        let mut edits = Vec::new();
        for (meta, refs) in self.outputs.drain(..).zip(self.refs.drain(..)) {
            let sst = meta.id;
            edits.push(Edit::AddSst {
                tablet,
                family,
                level,
                meta,
            });
            edits.push(Edit::SstBlobRefs { sst, refs });
        }
        edits
    }

    /// Opens readers for the outputs (so the manifest commit publishes them without I/O on
    /// the read path).
    pub(crate) fn open_readers(
        &self,
        cache: &Arc<BlockCache>,
        priority: pigeonhole_cache::Priority,
    ) -> Result<Vec<(SstId, Arc<SstReader>)>> {
        self.outputs
            .iter()
            .map(|m| {
                Ok((
                    m.id,
                    Arc::new(SstReader::open(
                        self.file.clone(),
                        m,
                        Arc::clone(cache),
                        priority,
                    )?),
                ))
            })
            .collect()
    }
}

/// Writes one memtable through a sink, all at once (open-time flushes).
pub(crate) fn write_memtable(sink: &mut SstSink, reader: &MemtableReader) -> Result<()> {
    let mut it = reader.iter();
    it.seek_to_first()?;
    while it.valid() {
        sink.add(it.key(), it.value())?;
        it.next()?;
    }
    sink.cut()
}

/// The GC a flush runs its memtable through (#287): compaction's rules for a
/// non-bottommost job over the slot, with the same read points (live snapshots and the
/// oldest reader pin), plus the guarded purge of versions beyond `max_versions` when the
/// shard found no delete outside the memtable as it queued it.
fn flush_gc(
    shared: &Shared,
    item: &FlushItem,
) -> (
    pigeonhole_compaction::StreamGc,
    pigeonhole_compaction::GcPolicy,
) {
    let view = shared.view.load();
    let now = shared.vfs.now_micros();
    let mut policy =
        pigeonhole_compaction::GcPolicy::new(crate::compact::gc_snapshots(shared), now, false);
    policy.no_outside_deletes = item.guard.is_some();
    // The record (test hook) describes the real GC, so a deliberate fault below shows up.
    let real = policy.clone();
    #[cfg(feature = "test-hooks")]
    match shared
        .hooks
        .flush_gc_mutation
        .load(std::sync::atomic::Ordering::Acquire)
    {
        1 => policy.no_outside_deletes = true,
        2 => policy.snapshots.clear(),
        _ => {}
    }
    let merge = view
        .catalog
        .family(item.family)
        .and_then(|m| m.merge_op.clone());
    let gc = pigeonhole_compaction::StreamGc::new(&policy, &item.options, merge);
    (gc, real)
}

/// The record of a flush (test hook, #287): its GC's read points and clock. Every flush
/// records, since its GC drops history (entries hidden at every live read point) as a
/// non-bottommost compaction does; one that ran the guarded version purge also records the
/// exact seqnos its memtable held. `None` for a tablet gone from the view.
#[cfg(feature = "test-hooks")]
fn flush_record(
    shared: &Shared,
    item: &FlushItem,
    policy: &pigeonhole_compaction::GcPolicy,
) -> Result<Option<crate::compact::CompactionRecord>> {
    use pigeonhole_format::manifest::FamilyKind;
    let view = shared.view.load();
    let Some(tablet) = view.tablets.entry(item.tablet) else {
        return Ok(None);
    };
    let versions_purge = policy.no_outside_deletes
        && item.options.max_versions != 0
        && item.options.kind != FamilyKind::Counter;
    let mut seqnos = std::collections::BTreeSet::new();
    if versions_purge {
        let mut it = item.reader.iter();
        it.seek_to_first()?;
        while it.valid() {
            if let Ok((_, _, seqno, _)) = pigeonhole_format::key::split_suffix(it.key()) {
                seqnos.insert(seqno);
            }
            it.next()?;
        }
    }
    Ok(Some(crate::compact::CompactionRecord {
        manifest_version: 0,
        table: item.table,
        tablet: item.tablet,
        family: item.family,
        bottommost: false,
        snapshots: policy.snapshots.clone(),
        now: policy.now,
        min_ts_above: 0,
        max_seqno: item.max_seqno,
        rows: (
            (!tablet.start.is_empty()).then(|| tablet.start.clone()),
            tablet.end.clone(),
        ),
        flush: true,
        versions_purge,
        input_seqnos: versions_purge.then(|| seqnos.into_iter().collect()),
        install_seqno: Default::default(),
    }))
}

enum Stage {
    Write,
    Barrier(Vec<Waiter<Result<()>>>),
    Commit(Waiter<Result<ManifestVersion>>),
    Done,
}

/// The flush task of one shard: every item in turn, then the barrier, then the commit.
pub(crate) struct FlushTask {
    shared: Arc<Shared>,
    shard: ShardId,
    items: Vec<FlushItem>,
    idx: usize,
    iter: Option<MemIter>,
    sink: Option<SstSink>,
    /// Compaction's GC over the memtable being written (#287).
    gc: Option<pigeonhole_compaction::StreamGc>,
    /// Per written item, the live-byte change of blob files whose pointers the GC dropped.
    blob_deltas: Vec<crate::compact::BlobChanges>,
    /// Records of the flushed items (test hook), and the install seqnos the commit fills in.
    #[cfg(feature = "test-hooks")]
    records: Vec<crate::compact::CompactionRecord>,
    #[cfg(feature = "test-hooks")]
    install_seqnos: Vec<Arc<std::sync::atomic::AtomicU64>>,
    /// `(item index, sink outputs)` per flushed item.
    written: Vec<(usize, SstSink)>,
    stage: Stage,
    waker: StdWaker,
    started: u64,
}

impl FlushTask {
    pub(crate) fn new(shared: Arc<Shared>, shard: ShardId, items: Vec<FlushItem>) -> Self {
        let started = shared.vfs.monotonic_nanos();
        Self {
            shared,
            shard,
            items,
            idx: 0,
            iter: None,
            sink: None,
            gc: None,
            blob_deltas: Vec::new(),
            #[cfg(feature = "test-hooks")]
            records: Vec::new(),
            #[cfg(feature = "test-hooks")]
            install_seqnos: Vec::new(),
            written: Vec::new(),
            stage: Stage::Write,
            waker: StdWaker::default(),
            started,
        }
    }

    fn report(&mut self, result: Result<ManifestVersion>) {
        let items = self
            .items
            .iter()
            .map(|i| FlushedItem {
                tablet: i.tablet,
                family: i.family,
                root: i.root,
                max_seqno: i.max_seqno,
            })
            .collect();
        let nanos = self
            .shared
            .vfs
            .monotonic_nanos()
            .saturating_sub(self.started);
        let _ = self.shared.submitter(self.shard).submit(ShardMsg::Flushed {
            items,
            result,
            nanos,
        });
        self.stage = Stage::Done;
    }

    fn fail(&mut self, e: Error) {
        crate::shard::trace!(
            "flush task shard {} failed in stage {}: {e}",
            self.shard.0,
            match self.stage {
                Stage::Write => "write",
                Stage::Barrier(_) => "barrier",
                Stage::Commit(_) => "commit",
                Stage::Done => "done",
            }
        );
        // Once submitted, the outputs belong to the manifest: a refused request's extents
        // are abandoned by `manifest::begin`, a committed one's are named by the catalog
        // (even when the reply is an error from a failed view publish). Abandoning them
        // here again would free an extent another writer may hold by now.
        let submitted = matches!(self.stage, Stage::Commit(_));
        if let Some(mut s) = self.sink.take()
            && !submitted
        {
            s.abandon();
        }
        for (_, mut s) in self.written.drain(..) {
            if !submitted {
                s.abandon();
            }
        }
        self.report(Err(e));
    }

    /// Writes until the deadline. Returns whether every item is written.
    fn write(&mut self, deadline: u64) -> Result<bool> {
        let mut n = 0u32;
        while self.idx < self.items.len() {
            let item = &self.items[self.idx];
            if self.sink.is_none() {
                let mut options = SstWriterOptions::for_family(
                    &item.options,
                    item.table,
                    item.family,
                    item.tablet,
                );
                options.created_micros = self.shared.vfs.now_micros();
                self.sink = Some(
                    SstSink::new(
                        Arc::clone(&self.shared.pager),
                        Arc::clone(&self.shared.sst_ids),
                        options,
                        item.bytes,
                    )
                    .separating(
                        Arc::clone(&self.shared.blob_ids),
                        item.options.blob_threshold,
                        self.shared.picker.target_sst_bytes,
                    ),
                );
                let mut it = item.reader.iter();
                it.seek_to_first()?;
                self.iter = Some(it);
                let (gc, _policy) = flush_gc(&self.shared, item);
                #[cfg(feature = "test-hooks")]
                if self
                    .shared
                    .hooks
                    .record
                    .load(std::sync::atomic::Ordering::Acquire)
                    && let Some(r) = flush_record(&self.shared, item, &_policy)?
                {
                    if r.versions_purge {
                        self.install_seqnos.push(Arc::clone(&r.install_seqno.0));
                    }
                    self.records.push(r);
                }
                self.gc = Some(gc);
            }
            let (Some(it), Some(sink), Some(gc)) =
                (self.iter.as_mut(), self.sink.as_mut(), self.gc.as_mut())
            else {
                unreachable!("set above");
            };
            while gc.step(it)? {
                gc.drain(|k, v| sink.add(k, v))?;
                n += 1;
                if n.is_multiple_of(CLOCK_EVERY)
                    && deadline != u64::MAX
                    && self.shared.vfs.monotonic_nanos() >= deadline
                {
                    return Ok(false);
                }
            }
            sink.cut()?;
            sink.finish_blobs()?;
            let sink = self.sink.take().expect("open");
            self.iter = None;
            if let Some(gc) = self.gc.take()
                && !gc.blob_delta().is_empty()
            {
                // One change set per family, each file's deltas summed: `blob_edits` writes a
                // file's new live count from the committing catalog, so two change sets naming
                // one file (tablets of a family flushed together, pointing into one commit-time
                // blob file) would each overwrite the other's decrement.
                let family = self.items[self.idx].family;
                let pos = match self.blob_deltas.iter().position(|c| c.family == family) {
                    Some(pos) => pos,
                    None => {
                        self.blob_deltas.push(crate::compact::BlobChanges {
                            family,
                            new: Vec::new(),
                            delta: Vec::new(),
                        });
                        self.blob_deltas.len() - 1
                    }
                };
                let delta = &mut self.blob_deltas[pos].delta;
                for &(file, d) in gc.blob_delta() {
                    match delta.iter_mut().find(|x| x.0 == file) {
                        Some(x) => x.1 += d,
                        None => delta.push((file, d)),
                    }
                }
            }
            self.written.push((self.idx, sink));
            self.idx += 1;
        }
        Ok(true)
    }

    /// Syncs the WAL streams the flushed data came from: the shard's own always (so a
    /// crash never loses an earlier commit of the same stream that a flushed later one
    /// survives), every stream when the memtables hold shares of cross-shard commits.
    fn barrier(&mut self) -> Vec<Waiter<Result<()>>> {
        let all = self.items.iter().any(|i| i.has_shares);
        let shards: Vec<ShardId> = if all {
            (0..self.shared.shards).map(|i| ShardId(i as u16)).collect()
        } else {
            vec![self.shard]
        };
        let mut waiters = Vec::with_capacity(shards.len());
        for s in shards {
            let (tx, rx) = completion();
            match self
                .shared
                .submitter(s)
                .submit(ShardMsg::SyncBarrier { reply: tx })
            {
                Ok(()) => waiters.push(rx),
                Err(_) => {
                    // The shard is gone (shutdown): its stream can no longer be synced.
                    let (tx, rx) = completion();
                    tx.notify(Err(Error::Closed));
                    waiters.push(rx);
                }
            }
        }
        waiters
    }

    fn submit_commit(&mut self) -> Result<Waiter<Result<ManifestVersion>>> {
        let mut edits = Vec::new();
        let mut readers = Vec::new();
        for (idx, sink) in &self.written {
            let item = &self.items[*idx];
            let priority = SstSet::priority(item.options.cache_priority);
            readers.extend(sink.open_readers(&self.shared.cache, priority)?);
            for f in &sink.blob_files {
                edits.push(Edit::PutBlobFile {
                    blob_file: f.id,
                    family: item.family,
                    extents: f.extents.clone(),
                    total_bytes: f.total_bytes,
                    live_bytes: f.total_bytes,
                });
            }
            for (meta, refs) in sink.outputs.iter().zip(&sink.refs) {
                edits.push(Edit::AddSst {
                    tablet: item.tablet,
                    family: item.family,
                    level: 0,
                    meta: meta.clone(),
                });
                edits.push(Edit::SstBlobRefs {
                    sst: meta.id,
                    refs: refs.clone(),
                });
            }
            edits.push(Edit::SetFlushed {
                tablet: item.tablet,
                family: item.family,
                seqno: item.max_seqno,
            });
        }
        // Blob pointers the GC dropped lower their files' live counts, computed against the
        // catalog the commit applies to (as a compaction's are). A purge under the guard
        // installs only if no delete in its family voided it since the memtable was queued;
        // from here on such deletes wait for this commit's outcome (#287).
        let deltas = std::mem::take(&mut self.blob_deltas);
        let guards: Vec<Arc<std::sync::atomic::AtomicU8>> =
            self.items.iter().filter_map(|i| i.guard.clone()).collect();
        #[cfg(feature = "test-hooks")]
        let install_seqnos = std::mem::take(&mut self.install_seqnos);
        let mut on_refusal = Vec::new();
        let kind = if deltas.is_empty() && guards.is_empty() {
            manifest::ReqKind::Edits(edits)
        } else {
            // Freed by the writer if the closure refuses (a voided guard).
            on_refusal = edits.clone();
            // Weak: a request still queued when the engine goes (a crash, deferred I/O) must
            // not keep it alive through its own manifest queue.
            #[cfg(feature = "test-hooks")]
            let shared = Arc::downgrade(&self.shared);
            manifest::ReqKind::Catalog(Box::new(move |catalog: &mut Catalog| {
                use crate::shard::{GUARD_IN_FLIGHT, GUARD_INSTALLING};
                use std::sync::atomic::Ordering;
                for g in &guards {
                    match g.compare_exchange(
                        GUARD_IN_FLIGHT,
                        GUARD_INSTALLING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        // A re-run of this closure finds its own claim.
                        Ok(_) | Err(GUARD_INSTALLING) => {}
                        Err(_) => {
                            return Err(Error::Busy);
                        }
                    }
                }
                #[cfg(feature = "test-hooks")]
                for s in &install_seqnos {
                    if let Some(shared) = shared.upgrade() {
                        s.store(shared.shm.visible_seqno(), Ordering::Release);
                    }
                }
                let mut edits = edits;
                for changes in deltas {
                    edits.extend(crate::compact::blob_edits(catalog, changes)?);
                }
                Ok(edits)
            }))
        };
        let (tx, rx) = completion();
        let req = ManifestReq {
            kind,
            readers,
            flushed_roots: self.items.iter().map(|i| (self.shard.0, i.root)).collect(),
            #[cfg(feature = "test-hooks")]
            compactions: std::mem::take(&mut self.records),
            #[cfg(not(feature = "test-hooks"))]
            compactions: Vec::new(),
            rewrite_snapshot: false,
            dropped_ok: self.items.iter().map(|i| (i.tablet, i.table)).collect(),
            on_refusal,
            reply: Box::new(manifest::notify(tx)),
        };
        manifest::submit(&self.shared, self.shard, req);
        Ok(rx)
    }
}

impl Task for FlushTask {
    fn run(&mut self, deadline_nanos: u64, waker: &TaskWaker) -> TaskPoll {
        crate::shard::trace!(
            "flush task shard {}: run in stage {}",
            self.shard.0,
            match self.stage {
                Stage::Write => "write",
                Stage::Barrier(_) => "barrier",
                Stage::Commit(_) => "commit",
                Stage::Done => "done",
            }
        );
        loop {
            match &mut self.stage {
                Stage::Write => match self.write(deadline_nanos) {
                    Ok(true) => {
                        let waiters = self.barrier();
                        crate::shard::trace!(
                            "flush task shard {}: written {} items, barrier over {} shards",
                            self.shard.0,
                            self.items.len(),
                            waiters.len()
                        );
                        self.stage = Stage::Barrier(waiters);
                    }
                    Ok(false) => return TaskPoll::Pending,
                    Err(e) => {
                        self.fail(e);
                        return TaskPoll::Done;
                    }
                },
                Stage::Barrier(waiters) => {
                    // A resolved waiter is dropped (its value is taken by the first poll).
                    let mut failed = None;
                    let mut i = 0;
                    while i < waiters.len() {
                        match self.waker.poll(waker, &mut waiters[i]) {
                            Poll::Ready(Some(Ok(()))) => {
                                waiters.swap_remove(i);
                            }
                            Poll::Ready(Some(Err(e))) => {
                                waiters.swap_remove(i);
                                failed = Some(e);
                            }
                            Poll::Ready(None) => {
                                waiters.swap_remove(i);
                                failed = Some(Error::Closed);
                            }
                            Poll::Pending => i += 1,
                        }
                    }
                    if let Some(e) = failed {
                        self.fail(e);
                        return TaskPoll::Done;
                    }
                    if !waiters.is_empty() {
                        crate::shard::trace!(
                            "flush task shard {}: {} barrier replies pending",
                            self.shard.0,
                            waiters.len()
                        );
                        return TaskPoll::Blocked;
                    }
                    crate::shard::trace!("flush task shard {}: submitting", self.shard.0);
                    match self.submit_commit() {
                        Ok(rx) => self.stage = Stage::Commit(rx),
                        Err(e) => {
                            self.fail(e);
                            return TaskPoll::Done;
                        }
                    }
                }
                Stage::Commit(rx) => match self.waker.poll(waker, rx) {
                    Poll::Ready(Some(Ok(v))) => {
                        self.report(Ok(v));
                        return TaskPoll::Done;
                    }
                    Poll::Ready(Some(Err(e))) => {
                        // The manifest writer refused or failed: the outputs are not
                        // referenced (abandoned by the writer on refusal; leaked until reopen
                        // on a poisoned pager).
                        crate::shard::trace!(
                            "flush task shard {} commit refused: {e}",
                            self.shard.0
                        );
                        self.written.clear();
                        self.report(Err(e));
                        return TaskPoll::Done;
                    }
                    Poll::Ready(None) => {
                        crate::shard::trace!(
                            "flush task shard {} commit reply dropped",
                            self.shard.0
                        );
                        self.written.clear();
                        self.report(Err(Error::Closed));
                        return TaskPoll::Done;
                    }
                    Poll::Pending => return TaskPoll::Blocked,
                },
                Stage::Done => return TaskPoll::Done,
            }
        }
    }

    fn name(&self) -> &'static str {
        "flush"
    }
}
