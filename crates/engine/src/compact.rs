//! Compaction scheduling: scoring a shard's `(tablet, family)` levels after each manifest
//! commit, narrowing picker tasks to the tablet (decision D79), the GC policy (decision
//! D70), and the cooperative task that runs a `CompactionJob` and commits its output.

use std::sync::Arc;
use std::task::Poll;

use pigeonhole_compaction::{
    CompactionJob, CompactionOutput, CompactionTask, GcPolicy, JobContext, JobPoll, KeyRange,
    Levels, TaskKind,
};
use pigeonhole_format::key::encode_row_prefix;
use pigeonhole_format::manifest::Edit;
use pigeonhole_format::{FamilyId, ManifestVersion, Seqno, SstId, TableId, TabletId, Timestamp};
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

/// Narrows a picker task to the tablet's rows and turns a `TrivialMove` of an SST shared
/// with a sibling tablet into a `Rewrite` (decision D79).
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
            .any(|id| catalog.sst_shared(*id))
    {
        task.kind = TaskKind::Rewrite;
    }
    Ok(())
}

/// A task compacting every level of a slot into the last level (`Engine::compact`).
pub(crate) fn plan_full(
    tablet: TabletId,
    family: FamilyId,
    levels: &Levels,
    last_level: u8,
    busy: &[SstId],
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
    // Already one run at the bottom: nothing to do.
    if inputs.len() == 1 && inputs[0].0 == last_level {
        return None;
    }
    let kind = if total == 1 {
        TaskKind::TrivialMove
    } else {
        TaskKind::Rewrite
    };
    Some(CompactionTask {
        tablet,
        family,
        range: KeyRange::all(),
        subranges: vec![KeyRange::all()],
        inputs,
        output_level: last_level,
        kind,
    })
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
    let bottommost = fam
        .levels
        .iter()
        .enumerate()
        .skip(usize::from(task.output_level) + 1)
        .all(|(_, l)| l.is_empty());
    let mut gc = GcPolicy::new(snapshots, now, bottommost);
    gc.min_ts_above = fam
        .iter()
        .filter(|s| !input_ids.contains(&s.meta.id))
        .map(|s| s.meta.ts_range.0)
        .fold(mem_min_ts, Timestamp::min);
    gc
}

/// The newest seqno among the task's inputs.
pub(crate) fn max_input_seqno(fam: &FamilySsts, task: &CompactionTask) -> Seqno {
    task.inputs
        .iter()
        .flat_map(|(_, ids)| ids.iter())
        .filter_map(|id| fam.find(*id).map(|(_, s)| s.meta.seqno_range.1))
        .max()
        .unwrap_or(0)
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
        let job = if task.kind == TaskKind::Rewrite {
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

    /// The manifest edits of an output (a job's, or a move or drop computed here).
    fn edits(&self, fam_catalog: &Catalog, output: Option<CompactionOutput>) -> Result<Outputs> {
        let (tablet, family) = (self.task.tablet, self.task.family);
        let mut edits = Vec::new();
        let mut readers = Vec::new();
        match (self.task.kind.clone(), output) {
            (TaskKind::Rewrite, Some(out)) => {
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
                for (blob_file, delta) in out.blob_live_delta {
                    if let Some(b) = fam_catalog.blob_files.get(&blob_file) {
                        edits.push(Edit::PutBlobFile {
                            blob_file,
                            family: b.family,
                            extents: b.extents.clone(),
                            total_bytes: b.total_bytes,
                            live_bytes: b.live_bytes.saturating_add_signed(delta),
                        });
                    }
                }
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
            (TaskKind::Rewrite, None) | (TaskKind::BlobGc { .. }, _) => {}
        }
        Ok((edits, readers))
    }

    fn submit(
        &mut self,
        output: Option<CompactionOutput>,
    ) -> Result<Waiter<Result<ManifestVersion>>> {
        let catalog = Arc::clone(&self.shared.view.load().catalog);
        let (edits, readers) = self.edits(&catalog, output)?;
        let (tx, rx) = completion();
        let req = ManifestReq {
            kind: manifest::ReqKind::Edits(edits),
            readers,
            flushed_roots: Vec::new(),
            compaction: self.record.take(),
            rewrite_snapshot: false,
            reply: Box::new(manifest::notify(tx)),
        };
        manifest::submit(&self.shared, self.shard, req);
        Ok(rx)
    }
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
