//! Compaction scheduling: scoring a shard's `(tablet, family)` levels after each manifest
//! commit, narrowing picker tasks to the tablet (decision D79), the GC policy (decision
//! D70), and the cooperative task that runs a `CompactionJob` and commits its output.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::task::Poll;

use pigeonhole_compaction::{
    BlobFileStat, CompactionJob, CompactionOutput, CompactionTask, GcPolicy, JobContext, JobPoll,
    KeyRange, Levels, NewBlobFile, TaskKind, pick_blob_gc,
};
use pigeonhole_format::key::encode_row_prefix;
use pigeonhole_format::manifest::{Edit, SstMeta};
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::{
    BlobFileId, FamilyId, ManifestVersion, Seqno, SstId, TableId, TabletId, Timestamp,
};
use pigeonhole_runtime::{ShardId, Task, TaskPoll, TaskWaker, Waiter, completion};
use pigeonhole_sst::SstReader;

use crate::catalog::{Catalog, FamilyMeta};
use crate::manifest::{self, ManifestReq};
use crate::shard::{ShardMsg, Shared};
use crate::snapshot::{FamilySsts, SstSet, TabletEntry, View};
use crate::waker::StdWaker;
use crate::{Error, Result};

/// What a compaction purged, for checking the engine against the reference model's
/// `Model::purge` (decision D74). Every committed compaction is recorded; `bottommost` says
/// whether it could purge. A test hook (`Engine::take_compactions`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct CompactionRecord {
    /// The manifest version that published the output.
    pub manifest_version: ManifestVersion,
    /// Table.
    pub table: TableId,
    /// Tablet.
    pub tablet: TabletId,
    /// Family.
    pub family: FamilyId,
    /// Whether the output was the bottommost data for the range (purges allowed).
    pub bottommost: bool,
    /// The live snapshot seqnos the GC kept.
    pub snapshots: Vec<Seqno>,
    /// The GC's clock.
    pub now: Timestamp,
    /// `GcPolicy::min_ts_above`.
    pub min_ts_above: Timestamp,
    /// The newest seqno among the inputs.
    pub max_seqno: Seqno,
    /// The tablet's row range (unescaped; `None` is unbounded).
    pub rows: (Option<Vec<u8>>, Option<Vec<u8>>),
}

/// The tablet's row range as a key range of row prefixes.
pub(crate) fn tablet_range(tablet: &TabletEntry) -> Result<KeyRange> {
    let start = if tablet.start.is_empty() {
        None
    } else {
        let mut k = Vec::new();
        encode_row_prefix(&mut k, &tablet.start)?;
        Some(k)
    };
    let end = match &tablet.end {
        Some(e) => {
            let mut k = Vec::new();
            encode_row_prefix(&mut k, e)?;
            Some(k)
        }
        None => None,
    };
    Ok(KeyRange { start, end })
}

/// Whether `meta` holds keys outside `range`: an SST a split's child inherited (D13), which
/// still holds its sibling's rows even once the sibling no longer references it.
pub(crate) fn sticks_out(meta: &SstMeta, range: &KeyRange) -> bool {
    range
        .start
        .as_ref()
        .is_some_and(|s| meta.smallest_key.as_slice() < s.as_slice())
        || range
            .end
            .as_ref()
            .is_some_and(|e| meta.largest_key.as_slice() >= e.as_slice())
}

/// Narrows a picker task to the tablet's rows and turns a `TrivialMove` of an SST shared
/// with a sibling tablet, or holding rows outside the tablet, into a `Rewrite` (decision
/// D79): the rewrite drops the rows outside, so the SST stops blocking a merge.
pub(crate) fn narrow(
    task: &mut CompactionTask,
    tablet: &TabletEntry,
    catalog: &Catalog,
) -> Result<()> {
    let range = tablet_range(tablet)?;
    task.subranges = task.subranges.iter().map(|s| s.intersect(&range)).collect();
    if task.subranges.is_empty() {
        task.subranges = vec![range.clone()];
    }
    task.range = range;
    if task.kind == TaskKind::TrivialMove
        && task
            .inputs
            .iter()
            .flat_map(|(_, ids)| ids.iter())
            .any(|id| {
                catalog.sst_shared(*id)
                    || catalog
                        .sst(tablet.id, task.family, *id)
                        .is_some_and(|(_, m)| sticks_out(m, &task.range))
            })
    {
        task.kind = TaskKind::Rewrite;
    }
    Ok(())
}

/// A task compacting every level of a slot into the last level (`Engine::compact`).
///
/// With `rewrite` (tablet changes on), a lone SST above the last level is rewritten rather
/// than moved, so the compaction purges what a bottommost compaction may (D74) whatever
/// the slot's layout: splits and moves flush and share SSTs at points that depend on the
/// shard count, and a move would keep deletes a rewrite of the same rows drops (issue #94).
pub(crate) fn plan_full(
    tablet: &TabletEntry,
    family: FamilyId,
    levels: &Levels,
    last_level: u8,
    busy: &[SstId],
    rewrite: bool,
) -> Option<CompactionTask> {
    let inputs: Vec<(u8, Vec<SstId>)> = levels
        .levels
        .iter()
        .enumerate()
        .filter(|(_, l)| !l.is_empty())
        .map(|(n, l)| (n as u8, l.iter().map(|s| s.id).collect()))
        .collect();
    let total: usize = inputs.iter().map(|(_, ids)| ids.len()).sum();
    if total == 0 {
        return None;
    }
    if inputs
        .iter()
        .flat_map(|(_, ids)| ids)
        .any(|id| busy.contains(id))
    {
        return None;
    }
    // Already one run at the bottom: nothing to do, unless it holds rows outside the tablet
    // (inherited from a split's parent), which a rewrite drops.
    if inputs.len() == 1 && inputs[0].0 == last_level {
        let range = tablet_range(tablet).ok()?;
        if !levels
            .levels
            .iter()
            .flatten()
            .any(|m| sticks_out(m, &range))
        {
            return None;
        }
    }
    let kind = if total == 1 && !rewrite {
        TaskKind::TrivialMove
    } else {
        TaskKind::Rewrite
    };
    Some(CompactionTask {
        tablet: tablet.id,
        family,
        range: KeyRange::all(),
        subranges: vec![KeyRange::all()],
        inputs,
        output_level: last_level,
        kind,
    })
}

/// A slot: one `(tablet, family)` tree.
type Slot = (TabletId, FamilyId);

/// One shard's blob GC planning (issue #33).
///
/// A blob file is emptied by rewriting every slot whose SSTs may point into it: a
/// `BlobGc` task over all of the slot's SSTs copies the values still live in the file into
/// new ones, and the file is dropped once its live count reaches zero (`blob_edits`). The
/// manifest does not record which SSTs point into which file, so each candidate file is
/// rewritten out of every slot of its family once. After a slot's blob GC commits it holds
/// no pointer into the file (its rows' values were copied, and nothing it compacts later
/// can bring one back), so the slot is not picked for that file again. The record is
/// in memory only: after a reopen, or for a tablet that a merge created or a move brought
/// here, a slot may be rewritten once more for nothing.
#[derive(Debug, Default)]
pub(crate) struct BlobGc {
    done: HashMap<BlobFileId, HashSet<Slot>>,
    running: Option<(Slot, Vec<BlobFileId>)>,
}

impl BlobGc {
    /// The next blob GC task among `slots` (oldest candidate file first), or `None`.
    /// Candidates are files at least half garbage with at least `min_garbage` garbage
    /// bytes (`pick_blob_gc`). Slots with no SSTs need no rewrite and are marked done.
    pub(crate) fn plan(
        &mut self,
        view: &View,
        slots: &[Slot],
        last_level: u8,
        busy: &[SstId],
        min_garbage: u64,
    ) -> Option<(Slot, CompactionTask)> {
        let catalog = &view.catalog;
        self.done
            .retain(|id, _| catalog.blob_files.contains_key(id));
        let mut candidates: HashMap<FamilyId, Vec<BlobFileId>> = HashMap::new();
        for (id, b) in &catalog.blob_files {
            let stat = BlobFileStat {
                id: *id,
                total_bytes: b.total_bytes,
                live_bytes: b.live_bytes,
            };
            if !pick_blob_gc(&[stat], min_garbage).is_empty() {
                candidates.entry(b.family).or_default().push(*id);
            }
        }
        for &slot in slots {
            let Some(files) = candidates.get(&slot.1) else {
                continue;
            };
            let pending: Vec<BlobFileId> = files
                .iter()
                .copied()
                .filter(|id| !self.done.get(id).is_some_and(|d| d.contains(&slot)))
                .collect();
            if pending.is_empty() {
                continue;
            }
            let inputs: Vec<(u8, Vec<SstId>)> = view
                .ssts
                .family(slot.0, slot.1)
                .map(|fam| {
                    fam.levels
                        .iter()
                        .enumerate()
                        .filter(|(_, l)| !l.is_empty())
                        .map(|(n, l)| (n as u8, l.iter().map(|s| s.meta.id).collect()))
                        .collect()
                })
                .unwrap_or_default();
            if inputs.is_empty() {
                self.mark_done(slot, &pending);
                continue;
            }
            if inputs
                .iter()
                .flat_map(|(_, ids)| ids)
                .any(|id| busy.contains(id))
            {
                continue;
            }
            let task = CompactionTask {
                tablet: slot.0,
                family: slot.1,
                range: KeyRange::all(),
                subranges: vec![KeyRange::all()],
                inputs,
                output_level: last_level,
                kind: TaskKind::BlobGc {
                    blob_files: pending,
                },
            };
            return Some((slot, task));
        }
        None
    }

    /// Turns a full compaction's rewrite of `slot` into a blob GC of every file of the family
    /// with any garbage (the inputs are rewritten anyway; `Engine::compact` reclaims all it
    /// can).
    pub(crate) fn full(&self, catalog: &Catalog, task: &mut CompactionTask) {
        if task.kind != TaskKind::Rewrite {
            return;
        }
        let files: Vec<BlobFileId> = catalog
            .blob_files
            .iter()
            .filter(|(_, b)| b.family == task.family && b.live_bytes < b.total_bytes)
            .map(|(id, _)| *id)
            .collect();
        if !files.is_empty() {
            task.kind = TaskKind::BlobGc { blob_files: files };
        }
    }

    /// A blob GC task started for `slot`.
    pub(crate) fn started(&mut self, slot: Slot, task: &CompactionTask) {
        if let TaskKind::BlobGc { blob_files } = &task.kind {
            self.running = Some((slot, blob_files.clone()));
        }
    }

    /// The running compaction finished; a blob GC that committed marks its slot done.
    pub(crate) fn finished(&mut self, committed: bool) {
        if let Some((slot, files)) = self.running.take()
            && committed
        {
            self.mark_done(slot, &files);
        }
    }

    fn mark_done(&mut self, slot: Slot, files: &[BlobFileId]) {
        for id in files {
            self.done.entry(*id).or_default().insert(slot);
        }
    }
}

/// The GC policy of `task` over `fam` (decision D70): every live snapshot of this process
/// plus the oldest reader pin's seqno (a reader's snapshots pin its own view, so the
/// oldest pin bounds everything a reader can still read), whether the output is bottommost,
/// and the smallest timestamp above the inputs (other SSTs of the slot and its memtables).
pub(crate) fn gc_policy(
    shared: &Shared,
    fam: &FamilySsts,
    task: &CompactionTask,
    mem_min_ts: Timestamp,
    now: Timestamp,
) -> GcPolicy {
    let mut snapshots = shared.live_seqnos.list();
    if let Some((seqno, _)) = shared.shm.oldest_reader_pin()
        && seqno != 0
    {
        snapshots.push(seqno);
    }
    snapshots.sort_unstable();
    snapshots.dedup();
    let input_ids: Vec<SstId> = task
        .inputs
        .iter()
        .flat_map(|(_, ids)| ids.iter().copied())
        .collect();
    // An output in L0 (a FIFO merge of some L0 files) is bottommost only if it takes every
    // SST of the slot: the L0 files left out may be older.
    let bottommost = fam
        .levels
        .iter()
        .enumerate()
        .skip(usize::from(task.output_level) + 1)
        .all(|(_, l)| l.is_empty())
        && (task.output_level != 0 || fam.iter().all(|s| input_ids.contains(&s.meta.id)));
    let mut gc = GcPolicy::new(snapshots, now, bottommost);
    gc.min_ts_above = fam
        .iter()
        .filter(|s| !input_ids.contains(&s.meta.id))
        .map(|s| s.meta.ts_range.0)
        .fold(mem_min_ts, Timestamp::min);
    gc
}

/// `CompactionRecord::max_seqno`: a seqno at or below which every entry of the slot is an
/// input or was already dropped. The newest seqno among the inputs, raised to just below the
/// oldest entry above them (the slot's other SSTs and memtables; `visible` when there is
/// none): an output SST's seqno range covers only the entries it kept, so an entry an
/// earlier compaction dropped (hidden by a delete at every read point) can be newer than
/// every input, and the model must still count it as one (issue #66). Never above `visible`:
/// a seqno past it may belong to a cross-shard commit not applied here yet (prepared and
/// undecided, or its PREPARE still on the way), which lands in the memtables below their
/// oldest entry (issue #132). Inputs never pass `visible` (a memtable freezes once it is).
pub(crate) fn max_input_seqno(
    fam: &FamilySsts,
    task: &CompactionTask,
    mem_min_seqno: Option<Seqno>,
    visible: Seqno,
) -> Seqno {
    let is_input = |id: SstId| task.inputs.iter().any(|(_, ids)| ids.contains(&id));
    let inputs = fam
        .iter()
        .filter(|s| is_input(s.meta.id))
        .map(|s| s.meta.seqno_range.1)
        .max()
        .unwrap_or(0);
    let above = fam
        .iter()
        .filter(|s| !is_input(s.meta.id))
        .map(|s| s.meta.seqno_range.0)
        .chain(mem_min_seqno)
        .min()
        .map_or(visible, |m| m.saturating_sub(1))
        .min(visible);
    inputs.max(above)
}

/// Manifest edits plus the readers of the SSTs they add.
type Outputs = (Vec<Edit>, Vec<(SstId, Arc<SstReader>)>);

enum Stage {
    Run,
    Commit(Waiter<Result<ManifestVersion>>),
    Done,
}

/// A running compaction of one slot.
pub(crate) struct CompactionWork {
    shared: Arc<Shared>,
    shard: ShardId,
    pub task: CompactionTask,
    job: Option<CompactionJob>,
    /// The view the inputs were taken from: pins their extents while the job reads them.
    _view: Arc<View>,
    meta: FamilyMeta,
    record: Option<CompactionRecord>,
    stage: Stage,
    waker: StdWaker,
    started: u64,
}

impl CompactionWork {
    /// Prepares the work: opens the input readers and builds the job (a `TrivialMove` or
    /// `Drop` needs none).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        shared: Arc<Shared>,
        shard: ShardId,
        view: Arc<View>,
        fam: &FamilySsts,
        meta: FamilyMeta,
        task: CompactionTask,
        gc: GcPolicy,
        record: Option<CompactionRecord>,
    ) -> Result<Self> {
        let started = shared.vfs.monotonic_nanos();
        let job = if matches!(task.kind, TaskKind::Rewrite | TaskKind::BlobGc { .. }) {
            let priority = SstSet::priority(meta.options.cache_priority);
            let mut inputs: Vec<Arc<SstReader>> = Vec::new();
            for (_, ids) in &task.inputs {
                for id in ids {
                    let (_, sst) = fam.find(*id).ok_or_else(|| {
                        Error::Corruption(format!("compaction input {} is gone", id.0))
                    })?;
                    inputs.push(sst.reader(&view.ssts, priority)?);
                }
            }
            let mut ctx = JobContext::new(
                meta.table,
                meta.options.clone(),
                Arc::clone(&shared.pager),
                Arc::clone(&shared.cache),
                Arc::clone(&shared.sst_ids),
                Arc::clone(&shared.blob_ids),
                gc,
            );
            ctx.merge = meta.merge_op.clone();
            if let TaskKind::BlobGc { blob_files } = &task.kind {
                for id in blob_files {
                    let reader = view.ssts.blob_reader(*id).ok_or_else(|| {
                        Error::Corruption(format!("blob GC input {} is gone", id.0))
                    })?;
                    ctx.blob_files.push((*id, reader));
                }
            }
            ctx.target_sst_bytes = shared.picker.target_sst_bytes;
            ctx.clock = Some(Arc::clone(&shared.vfs));
            Some(CompactionJob::new(task.clone(), inputs, ctx))
        } else {
            None
        };
        Ok(Self {
            shared,
            shard,
            task,
            job,
            _view: view,
            meta,
            record,
            stage: Stage::Run,
            waker: StdWaker::default(),
            started,
        })
    }

    /// The input SST ids (busy while the work runs).
    pub(crate) fn inputs(&self) -> Vec<SstId> {
        self.task
            .inputs
            .iter()
            .flat_map(|(_, ids)| ids.iter().copied())
            .collect()
    }

    fn report(&mut self, result: Result<ManifestVersion>) {
        let nanos = self
            .shared
            .vfs
            .monotonic_nanos()
            .saturating_sub(self.started);
        let _ = self
            .shared
            .submitter(self.shard)
            .submit(ShardMsg::CompactionDone {
                inputs: self.inputs(),
                result,
                nanos,
            });
        self.stage = Stage::Done;
    }

    /// The manifest edits of an output (a job's, or a move or drop computed here), and its
    /// blob file changes, which [`blob_edits`] turns into edits against the catalog at
    /// commit time.
    fn edits(
        &self,
        fam_catalog: &Catalog,
        output: Option<CompactionOutput>,
    ) -> Result<(Outputs, BlobChanges)> {
        let (tablet, family) = (self.task.tablet, self.task.family);
        let mut edits = Vec::new();
        let mut readers = Vec::new();
        let mut blobs = BlobChanges::default();
        match (self.task.kind.clone(), output) {
            (TaskKind::Rewrite | TaskKind::BlobGc { .. }, Some(out)) => {
                let priority = SstSet::priority(self.meta.options.cache_priority);
                for (level, meta) in out.added {
                    readers.push((
                        meta.id,
                        Arc::new(SstReader::open(
                            self.shared.pager.file().clone(),
                            &meta,
                            Arc::clone(&self.shared.cache),
                            priority,
                        )?),
                    ));
                    edits.push(Edit::AddSst {
                        tablet,
                        family,
                        level,
                        meta,
                    });
                }
                for sst in out.removed {
                    edits.push(Edit::RemoveSst {
                        tablet,
                        family,
                        sst,
                    });
                }
                blobs = BlobChanges {
                    family,
                    new: out.new_blob_files,
                    delta: out.blob_live_delta,
                };
            }
            (TaskKind::TrivialMove, _) => {
                for (_, ids) in &self.task.inputs {
                    for id in ids {
                        let (_, meta) = fam_catalog.sst(tablet, family, *id).ok_or_else(|| {
                            Error::Corruption(format!("moved SST {} is gone", id.0))
                        })?;
                        edits.push(Edit::RemoveSst {
                            tablet,
                            family,
                            sst: *id,
                        });
                        edits.push(Edit::AddSst {
                            tablet,
                            family,
                            level: self.task.output_level,
                            meta: (**meta).clone(),
                        });
                    }
                }
            }
            (TaskKind::Drop, _) => {
                for (_, ids) in &self.task.inputs {
                    for id in ids {
                        edits.push(Edit::RemoveSst {
                            tablet,
                            family,
                            sst: *id,
                        });
                    }
                }
            }
            (TaskKind::Rewrite | TaskKind::BlobGc { .. }, None) => {}
        }
        Ok(((edits, readers), blobs))
    }

    fn submit(
        &mut self,
        output: Option<CompactionOutput>,
    ) -> Result<Waiter<Result<ManifestVersion>>> {
        let catalog = Arc::clone(&self.shared.view.load().catalog);
        if catalog.tablet(self.task.tablet).is_none() {
            // The table was dropped while this ran (its SSTs went with it, retired at the
            // drop): nothing to publish. Free only what this job wrote.
            for x in output.iter().flat_map(written_extents) {
                self.shared.pager.abandon(x);
            }
            return Err(Error::TableNotFound("the table was dropped".to_owned()));
        }
        // What this job wrote, freed if the edits cannot be built (an output SST that fails
        // to open): nothing else ever names it (5-6 5.4).
        let written: Vec<ExtentRef> = output.iter().flat_map(written_extents).collect();
        let ((edits, readers), blobs) = match self.edits(&catalog, output) {
            Ok(e) => e,
            Err(e) => {
                for x in written {
                    self.shared.pager.abandon(x);
                }
                return Err(e);
            }
        };
        let (tx, rx) = completion();
        // Blob live counts change against the catalog at commit time: other compactions of
        // the family (other tablets after a split share its blob files) commit meanwhile.
        let kind = if blobs.is_empty() {
            manifest::ReqKind::Edits(edits)
        } else {
            manifest::ReqKind::Catalog(Box::new(move |catalog: &mut Catalog| {
                let mut edits = edits;
                edits.extend(blob_edits(catalog, blobs)?);
                Ok(edits)
            }))
        };
        let req = ManifestReq {
            kind,
            readers,
            flushed_roots: Vec::new(),
            compaction: self.record.take(),
            rewrite_snapshot: false,
            dropped_ok: Vec::new(),
            reply: Box::new(manifest::notify(tx)),
        };
        manifest::submit(&self.shared, self.shard, req);
        Ok(rx)
    }
}

/// A compaction's blob file changes: the files it wrote and the live-byte change of older
/// ones.
#[derive(Debug, Default)]
pub(crate) struct BlobChanges {
    pub family: FamilyId,
    pub new: Vec<NewBlobFile>,
    pub delta: Vec<(BlobFileId, i64)>,
}

impl BlobChanges {
    fn is_empty(&self) -> bool {
        self.new.is_empty() && self.delta.is_empty()
    }
}

/// The edits of `changes` against `catalog` (the one the commit applies to): a
/// `PutBlobFile` for each new file (all live), and for each older file the catalog still
/// names its new live count, or a `DropBlobFile` once nothing references it. A file the
/// catalog no longer names (its table was dropped) is left alone.
///
/// A delta larger than a file's live count means the accounting undercounted: dropping the
/// file would lose values some SST still points to. That is refused with `Corruption`
/// (the request commits nothing and its outputs are freed), never clamped to zero.
pub(crate) fn blob_edits(catalog: &Catalog, changes: BlobChanges) -> Result<Vec<Edit>> {
    let mut edits = Vec::new();
    for f in changes.new {
        edits.push(Edit::PutBlobFile {
            blob_file: f.id,
            family: changes.family,
            extents: f.extents,
            total_bytes: f.total_bytes,
            live_bytes: f.total_bytes,
        });
    }
    for (blob_file, delta) in changes.delta {
        let Some(b) = catalog.blob_files.get(&blob_file) else {
            continue;
        };
        let Some(live_bytes) = b.live_bytes.checked_add_signed(delta) else {
            return Err(Error::Corruption(format!(
                "blob file {} would go below zero live bytes ({} {delta})",
                blob_file.0, b.live_bytes
            )));
        };
        if live_bytes == 0 {
            edits.push(Edit::DropBlobFile { blob_file });
        } else {
            edits.push(Edit::PutBlobFile {
                blob_file,
                family: b.family,
                extents: b.extents.clone(),
                total_bytes: b.total_bytes,
                live_bytes,
            });
        }
    }
    Ok(edits)
}

/// Every extent a compaction's output occupies: its SSTs and its new blob files.
fn written_extents(out: &CompactionOutput) -> Vec<ExtentRef> {
    out.added
        .iter()
        .map(|(_, meta)| meta.extent)
        .chain(
            out.new_blob_files
                .iter()
                .flat_map(|b| b.extents.iter().copied()),
        )
        .collect()
}

impl Task for CompactionWork {
    fn run(&mut self, deadline_nanos: u64, waker: &TaskWaker) -> TaskPoll {
        loop {
            match &mut self.stage {
                Stage::Run => {
                    let output = match self.job.take() {
                        Some(mut job) => match job.run(deadline_nanos) {
                            Ok(JobPoll::Pending) => {
                                self.job = Some(job);
                                return TaskPoll::Pending;
                            }
                            Ok(JobPoll::Done) => match job.finish() {
                                Ok(out) => Some(out),
                                Err(e) => {
                                    self.report(Err(e.into()));
                                    return TaskPoll::Done;
                                }
                            },
                            Err(e) => {
                                job.abort();
                                self.report(Err(e.into()));
                                return TaskPoll::Done;
                            }
                        },
                        None => None,
                    };
                    match self.submit(output) {
                        Ok(rx) => self.stage = Stage::Commit(rx),
                        Err(e) => {
                            self.report(Err(e));
                            return TaskPoll::Done;
                        }
                    }
                }
                Stage::Commit(rx) => match self.waker.poll(waker, rx) {
                    Poll::Ready(Some(r)) => {
                        self.report(r);
                        return TaskPoll::Done;
                    }
                    Poll::Ready(None) => {
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
        "compaction"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_format::superblock::ExtentRef;

    fn catalog_with_file(live_bytes: u64) -> Catalog {
        let mut c = Catalog::default();
        c.apply(
            &Edit::PutBlobFile {
                blob_file: BlobFileId(7),
                family: FamilyId(1),
                extents: vec![ExtentRef {
                    page: 16,
                    size_class: 0,
                }],
                total_bytes: 1000,
                live_bytes,
            },
            1,
        )
        .unwrap();
        c
    }

    fn changes(delta: i64) -> BlobChanges {
        BlobChanges {
            family: FamilyId(1),
            new: Vec::new(),
            delta: vec![(BlobFileId(7), delta)],
        }
    }

    #[test]
    fn a_file_is_dropped_exactly_at_zero_and_an_undercount_is_refused() {
        let c = catalog_with_file(300);
        assert!(matches!(
            blob_edits(&c, changes(-100)).unwrap()[..],
            [Edit::PutBlobFile {
                live_bytes: 200,
                ..
            }]
        ));
        assert!(matches!(
            blob_edits(&c, changes(-300)).unwrap()[..],
            [Edit::DropBlobFile {
                blob_file: BlobFileId(7)
            }]
        ));
        // More dropped than recorded live: refused, never clamped into a drop.
        assert!(matches!(
            blob_edits(&c, changes(-301)),
            Err(Error::Corruption(_))
        ));
    }
}
