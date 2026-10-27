//! Compaction scheduling: scoring a shard's `(tablet, family)` levels after each manifest
//! commit, narrowing picker tasks to the tablet (decision D79), the GC policy (decision
//! D70), and the cooperative task that runs a `CompactionJob` and commits its output.

use std::collections::HashMap;
use std::sync::Arc;
use std::task::Poll;

use pigeonhole_compaction::{
    BlobFileStat, BlobRefs, CompactionJob, CompactionOutput, CompactionTask, GcPolicy, JobContext,
    JobPoll, KeyRange, Levels, NewBlobFile, OtherSource, TaskKind, pick_blob_gc,
};
use pigeonhole_format::key::encode_row_prefix;
use pigeonhole_format::manifest::{Edit, FamilyKind, FamilyOptions, SstMeta};
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
#[non_exhaustive]
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
    /// A flush's record, not a compaction's (#287: a flush's GC drops history too).
    pub flush: bool,
    /// A flush that ran the guarded purge of versions beyond `max_versions` (#287): no
    /// delete purge, and versions counted within the flushed memtable only.
    pub versions_purge: bool,
    /// A flush's input: the seqnos of the commits its memtable held (the slot's other
    /// sources can hold seqnos in the same range). `None` for a compaction, whose input is
    /// every entry of the range at or below `max_seqno`.
    pub input_seqnos: Option<Vec<Seqno>>,
    /// A flush with `versions_purge`: the visible seqno when its commit installed the purge.
    /// Every delete of the family at or below it was there before the install.
    pub install_seqno: SeqnoCell,
}

/// A seqno filled in when a flush installs its purge (test hook, #287): the visible seqno at
/// that moment. Compares by value.
#[derive(Debug, Clone, Default)]
#[doc(hidden)]
pub struct SeqnoCell(pub Arc<std::sync::atomic::AtomicU64>);

impl SeqnoCell {
    /// The seqno (0 until set).
    pub fn get(&self) -> Seqno {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }
}

impl PartialEq for SeqnoCell {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}

impl Eq for SeqnoCell {}

/// The row prefix of an internal key (the whole key if it has none).
fn row_of(key: &[u8]) -> &[u8] {
    &key[..pigeonhole_format::key::row_prefix_len(key).unwrap_or(key.len())]
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

/// What a full compaction reads of an SST's properties to judge it (D268): its distinct
/// rows, and `Properties::fully_purged`.
pub(crate) struct PurgeProps {
    pub rows: u64,
    pub fully_purged: Option<u32>,
}

/// Whether an SST a full compaction would otherwise leave as it is may hold entries a
/// bottommost rewrite would purge (#805, #820, D268), from its metadata and `props` (read
/// only when the manifest's metadata cannot settle it; `None` if they could not be read):
/// a cell old enough to have expired under the family's TTL; or, unless a bottommost
/// compaction that nothing held back wrote it under a policy that still holds
/// (`Properties::fully_purged`: a counter family, or the family keeps every version now or
/// at least as many as then), delete markers, or, in a family that keeps `max_versions`
/// (not a counter, whose versions are never purged), more entries than rows (possibly more
/// versions of a column than the family keeps; a row of several columns counts too, so
/// such an SST without the property is rewritten once per `compact()` call, the shard's
/// `full_purged`). Not seen without the property: a keep-all family's put shadowed by a
/// newer put at the same timestamp, kept because a snapshot needed it when the SST was
/// written.
pub(crate) fn may_hold_purgeable(
    sst: &SstMeta,
    family: &FamilyOptions,
    now: Timestamp,
    props: impl FnOnce() -> Option<PurgeProps>,
) -> bool {
    if family.ttl_micros > 0 && sst.ts_range.0.saturating_add(family.ttl_micros) <= now {
        return true;
    }
    let counter = family.kind == FamilyKind::Counter;
    let versions = !counter && family.max_versions > 0;
    if sst.deletes == 0 && !versions {
        return false;
    }
    let Some(props) = props() else {
        return true;
    };
    let purged = props.fully_purged.is_some_and(|then| {
        counter || family.max_versions == 0 || (then != 0 && family.max_versions >= then)
    });
    !purged && (sst.deletes > 0 || (versions && sst.entries > props.rows))
}

/// A task compacting every level of a slot into the last level (`Engine::compact`).
///
/// A lone SST, or a run already at the last level, is rewritten rather than moved or left
/// alone when `purgeable` says one of its SSTs may hold entries a bottommost rewrite purges
/// ([`may_hold_purgeable`]): a full compaction leaves nothing purgeable (#805, #820, D268).
/// The caller answers no for a slot the same full compaction already rewrote, so a rewrite
/// whose output still matches does not repeat.
///
/// A lone SST is also rewritten, not moved, when it is larger than one output piece
/// (`output_piece_bytes` at `target`): the rewrite cuts it into pieces that pack below one
/// another, so a compacted and shrunk file is about as large as its data (#185).
///
/// With `rewrite` (tablet changes on), a lone SST above the last level is rewritten rather
/// than moved, so the compaction purges what a bottommost compaction may (D74) whatever
/// the slot's layout: splits and moves flush and share SSTs at points that depend on the
/// shard count, and a move would keep deletes a rewrite of the same rows drops (issue #94).
#[allow(clippy::too_many_arguments)]
pub(crate) fn plan_full(
    tablet: &TabletEntry,
    family: FamilyId,
    levels: &Levels,
    last_level: u8,
    busy: &[SstId],
    rewrite: bool,
    target: u64,
    mut purgeable: impl FnMut(&SstMeta) -> bool,
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
    // Already one run at the bottom: nothing to do, unless it may hold purgeable entries
    // (#820) or rows outside the tablet (inherited from a split's parent), which a rewrite
    // drops.
    // A lone SST larger than one output piece is cut into pieces (#185), unless it holds a
    // single row: outputs are cut only between rows, so its rewrite would be the same SST,
    // and a full compaction, which runs until nothing is left to do, would never end.
    let lone = (total == 1)
        .then(|| levels.levels.iter().flatten().next())
        .flatten();
    let one_piece = lone.is_none_or(|s| {
        pigeonhole_compaction::output_piece_bytes(s.len, target) >= s.len
            || row_of(&s.smallest_key) == row_of(&s.largest_key)
    });
    // Asked only where the answer changes the plan: a run already at the bottom, or a lone
    // SST that would be moved.
    let settled = inputs.len() == 1 && inputs[0].0 == last_level && one_piece;
    let movable = total == 1 && !rewrite && one_piece;
    let purge = (settled || movable) && levels.levels.iter().flatten().any(|s| purgeable(s));
    if settled && !purge {
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
    let kind = if movable && !purge {
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

/// Blob GC planning (issues #33, #240).
///
/// A blob file is emptied by rewriting every slot whose SSTs point into it: a `BlobGc`
/// task over all of the slot's SSTs copies the values still live in the file into new
/// ones, and the file is dropped once its live count reaches zero (`blob_edits`). The
/// manifest records which blob files each SST points into (`SstBlobRefs`), so only those
/// slots are rewritten, and a slot whose blob GC committed is not picked again for the
/// file (its new SSTs hold no pointer into it), across reopens too. An SST without a
/// record (written by an older build) counts as pointing into every file of its family.
#[derive(Debug, Default)]
pub(crate) struct BlobGc;

impl BlobGc {
    /// The next blob GC task among `slots` (oldest candidate file first), or `None`.
    /// Candidates are files at least half garbage with at least `min_garbage` garbage
    /// bytes (`pick_blob_gc`); a slot is picked for the candidates its SSTs point into.
    pub(crate) fn plan(
        &self,
        view: &View,
        slots: &[Slot],
        last_level: u8,
        busy: &[SstId],
        min_garbage: u64,
    ) -> Option<(Slot, CompactionTask)> {
        let catalog = &view.catalog;
        let mut candidates: HashMap<FamilyId, Vec<BlobFileId>> = HashMap::new();
        for (id, b) in catalog.blob_files.iter() {
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
            let Some(fam) = view.ssts.family(slot.0, slot.1) else {
                continue;
            };
            let pending: Vec<BlobFileId> = files
                .iter()
                .copied()
                .filter(|f| fam.iter().any(|s| catalog.may_reference(s.meta.id, *f)))
                .collect();
            if pending.is_empty() {
                continue;
            }
            let inputs: Vec<(u8, Vec<SstId>)> = fam
                .levels
                .iter()
                .enumerate()
                .filter(|(_, l)| !l.is_empty())
                .map(|(n, l)| (n as u8, l.iter().map(|s| s.meta.id).collect()))
                .collect();
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
}

/// The read points GC must preserve (decision D70): every live snapshot of this process plus
/// the oldest reader pin's seqno (a reader's snapshots pin its own view, so the oldest pin
/// bounds everything a reader can still read), ascending.
pub(crate) fn gc_snapshots(shared: &Shared) -> Vec<Seqno> {
    let mut snapshots = shared.live_seqnos.list();
    if let Some((seqno, _)) = shared.shm.oldest_reader_pin()
        && seqno != 0
    {
        snapshots.push(seqno);
    }
    snapshots.sort_unstable();
    snapshots.dedup();
    snapshots
}

/// The GC policy of `task` over `fam` (decision D70): every live snapshot of this process
/// plus the oldest reader pin's seqno (a reader's snapshots pin its own view, so the
/// oldest pin bounds everything a reader can still read), whether the output is bottommost,
/// and the smallest timestamp above the inputs (other SSTs of the slot and its memtables).
/// For a counter family it also lists the other sources that may hold a seqno at or below
/// the newest input seqno (D179): the slot's other SSTs, its memtables (from
/// `mem_min_seqno`) and the prepared shares (`prepared_seqnos`).
pub(crate) fn gc_policy(
    shared: &Shared,
    fam: &FamilySsts,
    task: &CompactionTask,
    (mem_min_ts, mem_min_seqno): (Timestamp, Option<Seqno>),
    prepared_seqnos: impl Iterator<Item = Seqno>,
    counter: bool,
    now: Timestamp,
) -> GcPolicy {
    let snapshots = gc_snapshots(shared);
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
    // Without an input found in `fam` the bound is unknown: `None` guards everything.
    let inputs_max = fam
        .iter()
        .filter(|s| input_ids.contains(&s.meta.id))
        .map(|s| s.meta.seqno_range.1)
        .max();
    if counter && let Some(inputs_max) = inputs_max {
        let ssts = fam
            .iter()
            .filter(|s| !input_ids.contains(&s.meta.id))
            .map(|s| OtherSource {
                keys: Some((s.meta.smallest_key.clone(), s.meta.largest_key.clone())),
                seqnos: s.meta.seqno_range,
            });
        let unbounded = mem_min_seqno
            .into_iter()
            .chain(prepared_seqnos)
            .map(|lo| OtherSource {
                keys: None,
                seqnos: (lo, Seqno::MAX),
            });
        gc.other_sources = Some(
            ssts.chain(unbounded)
                .filter(|o| o.seqnos.0 <= inputs_max)
                .collect(),
        );
    }
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
    view: Arc<View>,
    /// A `Drop`'s blob live-byte changes: the pointers its SSTs held (read before submit).
    drop_delta: Vec<(BlobFileId, i64)>,
    /// The finished job's per-SST blob references (#240).
    blob_refs: BlobRefs,
    meta: FamilyMeta,
    record: Option<CompactionRecord>,
    /// A bottommost purge's guard (#316): its commit installs only if no write below its
    /// `min_ts_above` voided it since the plan (see `Shard::admit_against_guards`).
    pub guard: Option<Arc<crate::purge_guard::PurgeGuard>>,
    /// Test hook (`Engine::hold_compactions`): held before the first slice, then released.
    #[cfg(feature = "test-hooks")]
    hold: (Option<u64>, bool),
    stage: Stage,
    waker: StdWaker,
    started: u64,
    /// Taken back from a background worker and dropped before its commit (D284): its report
    /// says so (not a failure; the slot compacts again).
    reclaimed: bool,
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
            view,
            drop_delta: Vec::new(),
            blob_refs: Vec::new(),
            meta,
            record,
            guard: None,
            #[cfg(feature = "test-hooks")]
            hold: (None, false),
            stage: Stage::Run,
            waker: StdWaker::default(),
            started,
            reclaimed: false,
        })
    }

    /// For a `Drop`, which removes whole SSTs without a job: the live bytes the blob files
    /// lose, `16 + len` for every blob pointer a dropped SST holds within the tablet's rows
    /// (the same accounting a job does for the puts it drops). Uses the SST's recorded
    /// references when they cover only this tablet's rows, and reads the SST otherwise;
    /// nothing when the family has no blob files.
    fn count_dropped_blobs(&mut self) -> Result<()> {
        use pigeonhole_compaction::{blob_pointer, record_bytes};
        use pigeonhole_format::Cursor;
        use pigeonhole_format::key::{Kind, split_suffix};
        use pigeonhole_sst::{ReadOptions, ScanFilter};

        let family = self.task.family;
        if !self
            .view
            .catalog
            .blob_files
            .values()
            .any(|b| b.family == family)
        {
            return Ok(());
        }
        let fam = self
            .view
            .ssts
            .family(self.task.tablet, family)
            .ok_or_else(|| Error::Corruption("dropped SSTs are gone".to_owned()))?;
        let priority = SstSet::priority(self.meta.options.cache_priority);
        let mut read = ReadOptions::default();
        read.fill_cache = false;
        read.readahead_blocks = 4;
        let range = &self.task.range;
        let mut delta: Vec<(BlobFileId, i64)> = Vec::new();
        for id in self.task.inputs.iter().flat_map(|(_, ids)| ids.iter()) {
            let (_, sst) = fam
                .find(*id)
                .ok_or_else(|| Error::Corruption(format!("dropped SST {} is gone", id.0)))?;
            // Its recorded references (#240) count every row: usable unless the SST also
            // holds rows outside this tablet (shared with a sibling, or inherited from a
            // split's parent).
            let catalog = &self.view.catalog;
            if let Some(refs) = catalog.blob_refs.get(id)
                && !catalog.sst_shared(*id)
                && !sticks_out(&sst.meta, range)
            {
                for (blob_file, bytes) in refs {
                    let bytes = *bytes as i64;
                    match delta.iter_mut().find(|d| d.0 == *blob_file) {
                        Some(d) => d.1 -= bytes,
                        None => delta.push((*blob_file, -bytes)),
                    }
                }
                continue;
            }
            let mut it = sst
                .reader(&self.view.ssts, priority)?
                .iter(ScanFilter::all(), read);
            match &range.start {
                Some(s) => it.seek(s)?,
                None => it.seek_to_first()?,
            }
            while it.valid() && range.end.as_deref().is_none_or(|e| it.key() < e) {
                if split_suffix(it.key()).is_ok_and(|(_, _, _, k)| k == Kind::Put)
                    && let Some(p) = blob_pointer(it.value())
                {
                    let bytes = record_bytes(p.len) as i64;
                    match delta.iter_mut().find(|d| d.0 == p.blob_file) {
                        Some(d) => d.1 -= bytes,
                        None => delta.push((p.blob_file, -bytes)),
                    }
                }
                it.next()?;
            }
        }
        self.drop_delta = delta;
        Ok(())
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
                reclaimed: self.reclaimed,
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
                let mut blob_refs = self.blob_refs.clone();
                for (level, meta) in out.added {
                    let refs = blob_refs
                        .iter()
                        .position(|(id, _)| *id == meta.id)
                        .map(|i| blob_refs.swap_remove(i).1);
                    // The job reports every output's references; in release builds a
                    // missing one only leaves the SST unrecorded (blob GC then treats it
                    // as pointing anywhere).
                    debug_assert!(refs.is_some(), "no blob references for SST {}", meta.id.0);
                    readers.push((
                        meta.id,
                        Arc::new(SstReader::open(
                            self.shared.pager.data_file().clone(),
                            &meta,
                            Arc::clone(&self.shared.cache),
                            priority,
                        )?),
                    ));
                    let sst = meta.id;
                    edits.push(Edit::AddSst {
                        tablet,
                        family,
                        level,
                        meta,
                    });
                    if let Some(refs) = refs {
                        edits.push(Edit::SstBlobRefs { sst, refs });
                    }
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
                blobs = BlobChanges {
                    family,
                    new: Vec::new(),
                    delta: self.drop_delta.clone(),
                };
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
        let mut on_refusal = Vec::new();
        let guard = self.guard.clone();
        let kind = if blobs.is_empty() && guard.is_none() {
            manifest::ReqKind::Edits(edits)
        } else {
            // Freed by the writer if the closure refuses (an undercounted blob file, or a
            // voided guard).
            on_refusal = edits.clone();
            on_refusal.extend(blobs.new.iter().map(|f| Edit::PutBlobFile {
                blob_file: f.id,
                family: blobs.family,
                extents: f.extents.clone(),
                total_bytes: f.total_bytes,
                live_bytes: f.total_bytes,
            }));
            manifest::ReqKind::Catalog(Box::new(move |catalog: &mut Catalog| {
                // A bottommost purge installs only if no write below its bound arrived
                // since the plan; from here on such writes wait for this commit (#316).
                // A re-run of this closure finds its own claim (`PurgeGuard::claim`).
                if guard.as_ref().is_some_and(|g| !g.claim()) {
                    return Err(Error::Busy);
                }
                let mut edits = edits;
                if !blobs.is_empty() {
                    edits.extend(blob_edits(catalog, blobs)?);
                }
                Ok(edits)
            }))
        };
        let req = ManifestReq {
            kind,
            readers,
            flushed_roots: Vec::new(),
            compactions: self.record.take().into_iter().collect(),
            rewrite_snapshot: false,
            dropped_ok: Vec::new(),
            on_refusal,
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
                extents: b.extents.to_vec(),
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
                    // The engine is closing (#991): the job stops here rather than finish
                    // the rewrite, since there may never be another open to reclaim its
                    // output. As in `reclaim`, nothing it submitted is in flight (its output
                    // writes complete inside its slices, `write_at`, and its reads own their
                    // buffers), so its extents go back to the pager now, before the close
                    // ends: the shard waits for this report.
                    if self
                        .shared
                        .closing
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        if let Some(job) = self.job.take() {
                            job.abort();
                        }
                        self.report(Err(Error::Closed));
                        return TaskPoll::Done;
                    }
                    #[cfg(feature = "test-hooks")]
                    if !self.hold.1
                        && self
                            .shared
                            .hooks
                            .compaction_hold
                            .load(std::sync::atomic::Ordering::Acquire)
                    {
                        let mut held = self
                            .shared
                            .hooks
                            .compaction_held
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        match self.hold.0 {
                            // Released: its waker was taken.
                            Some(id) if !held.iter().any(|(h, _)| *h == id) => {
                                self.hold.1 = true;
                            }
                            Some(id) => {
                                if let Some(e) = held.iter_mut().find(|(h, _)| *h == id) {
                                    e.1 = waker.clone();
                                }
                                return TaskPoll::Blocked;
                            }
                            None => {
                                let id = self
                                    .shared
                                    .hooks
                                    .compaction_hold_ids
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                held.push((id, waker.clone()));
                                self.hold.0 = Some(id);
                                return TaskPoll::Blocked;
                            }
                        }
                    }
                    // A write voided the purge (#316): the commit would be refused, so stop
                    // now rather than finish the rewrite.
                    if self.guard.as_ref().is_some_and(|g| g.voided()) {
                        if let Some(job) = self.job.take() {
                            job.abort();
                        }
                        self.report(Err(Error::Busy));
                        return TaskPoll::Done;
                    }
                    // Test hook (D284's tests): slices of a few units each, so a test can
                    // stop the job part-way at a point of its choosing.
                    #[cfg(feature = "test-hooks")]
                    let deadline_nanos = if self
                        .shared
                        .hooks
                        .compaction_tiny_slices
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        0
                    } else {
                        deadline_nanos
                    };
                    let output = match self.job.take() {
                        Some(mut job) => match job.run(deadline_nanos) {
                            Ok(JobPoll::Pending) => {
                                self.job = Some(job);
                                return TaskPoll::Pending;
                            }
                            Ok(JobPoll::Done) => match job.finish_with_blob_refs() {
                                Ok((out, refs)) => {
                                    self.blob_refs = refs;
                                    Some(out)
                                }
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
                        None => {
                            if self.task.kind == TaskKind::Drop
                                && let Err(e) = self.count_dropped_blobs()
                            {
                                self.report(Err(e));
                                return TaskPoll::Done;
                            }
                            None
                        }
                    };
                    #[cfg(feature = "test-hooks")]
                    self.shared
                        .hooks
                        .compaction_commits
                        .fetch_add(1, std::sync::atomic::Ordering::Release);
                    match self.submit(output) {
                        Ok(rx) => {
                            self.stage = Stage::Commit(rx);
                            // A close no longer waits for it (#991).
                            let _ = self
                                .shared
                                .submitter(self.shard)
                                .submit(ShardMsg::CompactionCommitting);
                        }
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

    /// Taken back from a background worker (D284), on its shard. Before its commit is
    /// submitted it is dropped here: its output was never installed, and nothing it
    /// submitted is in flight (its output writes complete inside its slices, `write_at`, and
    /// a worker's slices run their reads inline, `pigeonhole_io::inline_io`), so the job's
    /// extents go back to the pager now and the shard sees its inputs free to compact again.
    /// After the submit it only waits for the manifest's reply, and keeps waiting, failed
    /// or not: in `Stage::Commit` its state is only the reply's receiver (the output belongs
    /// to the manifest request), so a slice that panicked there left nothing half-updated.
    fn reclaim(&mut self, _failed: bool) -> bool {
        if !matches!(self.stage, Stage::Run) {
            return true;
        }
        if let Some(job) = self.job.take() {
            job.abort();
        }
        self.reclaimed = true;
        self.report(Err(Error::Closed));
        false
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

    fn sst(id: u64, row: &[u8]) -> SstMeta {
        let mut key = Vec::new();
        encode_row_prefix(&mut key, row).unwrap();
        SstMeta {
            id: SstId(id),
            extent: ExtentRef {
                page: 16 * id,
                size_class: 0,
            },
            len: 4096,
            smallest_key: key.clone(),
            largest_key: key,
            seqno_range: (1, 1),
            ts_range: (1, 1),
            entries: 1,
            deletes: 0,
        }
    }

    /// #240: blob GC rewrites only the slots whose SSTs point into a candidate file (or
    /// whose SSTs have no record, written by an older build).
    #[test]
    fn blob_gc_picks_only_slots_pointing_into_a_candidate() {
        let (table, family, f1) = (TableId(1), FamilyId(2), BlobFileId(9));
        let mut c = Catalog::default();
        let mut edits = vec![
            Edit::CreateTable {
                table,
                name: "t".into(),
            },
            Edit::PutFamily {
                table,
                family,
                name: "f".into(),
                options: pigeonhole_format::manifest::FamilyOptions::default(),
            },
            Edit::PutTablet {
                tablet: TabletId(1),
                table,
                start: Vec::new(),
                end: Some(b"m".to_vec()),
            },
            Edit::PutTablet {
                tablet: TabletId(2),
                table,
                start: b"m".to_vec(),
                end: None,
            },
            // Mostly garbage: a candidate.
            Edit::PutBlobFile {
                blob_file: f1,
                family,
                extents: vec![ExtentRef {
                    page: 4096,
                    size_class: 0,
                }],
                total_bytes: 1 << 20,
                live_bytes: 1000,
            },
        ];
        for (tablet, id, row) in [(1, 10, &b"a"[..]), (2, 20, b"x")] {
            edits.push(Edit::AddSst {
                tablet: TabletId(tablet),
                family,
                level: 1,
                meta: sst(id, row),
            });
        }
        // Tablet 1's SST points into the file; tablet 2's points into none.
        edits.push(Edit::SstBlobRefs {
            sst: SstId(10),
            refs: vec![(f1, 1000)],
        });
        edits.push(Edit::SstBlobRefs {
            sst: SstId(20),
            refs: Vec::new(),
        });
        for e in &edits {
            c.apply(e, 1).unwrap();
        }
        let view = |c: &Catalog| {
            let vfs = pigeonhole_io::sim::SimVfs::new(1);
            let file = pigeonhole_io::Vfs::open(
                &*vfs,
                "/f".as_ref(),
                pigeonhole_io::OpenOptions::read_write_create(),
            )
            .unwrap();
            let cache = Arc::new(pigeonhole_cache::BlockCache::new(1 << 20, 1));
            let ssts = SstSet::build(c, None, &mut HashMap::new(), file, cache);
            let tablets: Vec<TabletEntry> = c.tablets();
            View {
                version: 1,
                manifest_version: 1,
                tablets: Arc::new(crate::snapshot::TabletMap::build(1, &tablets)),
                catalog: Arc::new(c.clone()),
                mems: Vec::new(),
                ssts: Arc::new(ssts),
                _pin: None,
                released: false,
            }
        };
        let slots = [(TabletId(1), family), (TabletId(2), family)];
        let plan = |c: &Catalog| {
            BlobGc
                .plan(&view(c), &slots, 6, &[], 1)
                .map(|(slot, task)| (slot, task.kind))
        };
        assert_eq!(
            plan(&c),
            Some((
                (TabletId(1), family),
                TaskKind::BlobGc {
                    blob_files: vec![f1]
                }
            ))
        );
        // Once tablet 1's SSTs no longer point into it, nothing is left to rewrite.
        c.apply(
            &Edit::SstBlobRefs {
                sst: SstId(10),
                refs: Vec::new(),
            },
            1,
        )
        .unwrap();
        assert_eq!(plan(&c), None);
        // An SST without a record may point anywhere.
        Arc::make_mut(&mut c.blob_refs).remove(&SstId(20));
        assert_eq!(plan(&c).map(|p| p.0), Some((TabletId(2), family)));
        // Records of SSTs no tablet references go at the end of a batch.
        c.apply(
            &Edit::RemoveSst {
                tablet: TabletId(1),
                family,
                sst: SstId(10),
            },
            1,
        )
        .unwrap();
        c.prune_blob_refs();
        assert!(!c.blob_refs.contains_key(&SstId(10)));
    }

    /// D268: what makes an SST a full compaction would otherwise leave alone a candidate for
    /// a rewrite, with and without the fully-purged property.
    #[test]
    fn may_hold_purgeable_honours_the_fully_purged_property_except_for_expiry() {
        let sst = |entries, deletes, ts| SstMeta {
            id: SstId(1),
            extent: ExtentRef {
                page: 16,
                size_class: 0,
            },
            len: 4096,
            smallest_key: Vec::new(),
            largest_key: Vec::new(),
            seqno_range: (1, 10),
            ts_range: (ts, ts),
            entries,
            deletes,
        };
        let family = |max_versions, ttl_micros| {
            let mut f = FamilyOptions::default();
            f.max_versions = max_versions;
            f.ttl_micros = ttl_micros;
            f
        };
        let props = |rows, fully_purged| move || Some(PurgeProps { rows, fully_purged });
        let check = |s: &SstMeta, f: &FamilyOptions, p: Option<u32>| {
            may_hold_purgeable(s, f, 1_000, props(50, p))
        };
        // Keep-all, no deletes: nothing to purge, properties not read.
        assert!(!may_hold_purgeable(
            &sst(100, 0, 500),
            &family(0, 0),
            1_000,
            || -> Option<PurgeProps> { panic!("read") }
        ));
        // Deletes, or more entries than rows under max_versions: a candidate without the
        // property, not with it.
        assert!(check(&sst(100, 1, 500), &family(0, 0), None));
        assert!(!check(&sst(100, 1, 500), &family(0, 0), Some(0)));
        assert!(check(&sst(100, 0, 500), &family(1, 0), None));
        assert!(!check(&sst(100, 0, 500), &family(1, 0), Some(1)));
        // Purged under a policy that no longer holds: fewer versions kept now, or every
        // version kept then and a limit now.
        assert!(!check(&sst(100, 0, 500), &family(2, 0), Some(1)));
        assert!(check(&sst(100, 0, 500), &family(1, 0), Some(2)));
        assert!(check(&sst(100, 0, 500), &family(1, 0), Some(0)));
        // Unreadable properties: a candidate.
        assert!(may_hold_purgeable(
            &sst(100, 0, 500),
            &family(1, 0),
            1_000,
            || None
        ));
        // Expiry whatever the property says: the property is about what nothing held back,
        // not about what has expired since.
        assert!(check(&sst(50, 0, 500), &family(1, 400), Some(1)));
        assert!(!check(&sst(50, 0, 700), &family(1, 400), Some(1)));
    }
}
