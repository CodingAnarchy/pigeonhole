//! Running a compaction: merge the inputs, apply GC, write the outputs.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use pigeonhole_cache::BlockCache;
use pigeonhole_format::key::row_prefix_len;
use pigeonhole_format::manifest::{FamilyOptions, SstMeta};
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::{BlobFileId, Cursor, Seqno, SstId, TableId, Timestamp};
use pigeonhole_io::VfsRef;
use pigeonhole_pager::Pager;
use pigeonhole_sst::{
    BlobReader, ReadOptions, ScanFilter, SstIter, SstReader, SstWriter, SstWriterOptions,
};

use crate::gc::{Gc, GcConfig, OutBuf};
use crate::{CompactionTask, KeyRange, MergeOperator, MergingCursor, Result, TaskKind};

/// Largest extent the pager hands out (size class 10).
const MAX_EXTENT: u64 = 64 << 20;

/// Units of work (one marker or one `(column, timestamp)` group) between clock checks.
const CLOCK_EVERY: u32 = 64;

/// Units of work per [`CompactionJob::run`] call when the context has no clock.
const SLICE_WITHOUT_CLOCK: u32 = 4096;

/// Which versions compaction must keep.
///
/// Dropping a delete or a version at the bottommost level is safe for the inputs, but data
/// *above* them (newer levels and memtables) can hold entries with older user timestamps
/// (written later with an explicit timestamp) that a delete hides, or cell deletes that hide
/// the newer versions a `max_versions` purge counted on. So bottommost purges only touch
/// timestamps below [`GcPolicy::min_ts_above`], which the engine sets to the smallest
/// timestamp of anything above the inputs in the task's range. Expired data and entries
/// hidden or shadowed within the inputs are dropped at any level.
///
/// ```
/// use pigeonhole_compaction::GcPolicy;
///
/// // Snapshots at seqnos 10 and 42 are open; the output is the last level, and nothing
/// // above it is older than 1_000.
/// let mut gc = GcPolicy::new(vec![10, 42], 1_700_000_000_000_000, true);
/// gc.min_ts_above = 1_000;
/// assert!(gc.bottommost);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct GcPolicy {
    /// Every live snapshot seqno (in-process and reader slots), ascending. A version visible
    /// at any of them, or newer than all of them, is kept.
    pub snapshots: Vec<Seqno>,
    /// Current time in microseconds.
    pub now: Timestamp,
    /// Whether the output is the bottommost data for its key range (tombstones can go).
    pub bottommost: bool,
    /// Smallest timestamp of any entry above the inputs in the task's range (L0 files and
    /// levels not in the task, memtables), or `u64::MAX` if there is none. Bottommost purges
    /// of deletes and of versions beyond `max_versions` apply only below it. Default 0: purge
    /// nothing at the bottom until the engine supplies the bound.
    pub min_ts_above: Timestamp,
}

impl GcPolicy {
    /// A policy keeping everything visible at `snapshots`.
    pub fn new(snapshots: Vec<Seqno>, now: Timestamp, bottommost: bool) -> Self {
        Self {
            snapshots,
            now,
            bottommost,
            min_ts_above: 0,
        }
    }
}

/// Everything a job needs from the engine.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct JobContext {
    /// Table.
    pub table: TableId,
    /// Family options (codec, block size, filters, versions, TTL, blob threshold).
    pub family: FamilyOptions,
    /// Space for outputs.
    pub pager: Arc<Pager>,
    /// Cache for reading inputs (without filling it).
    pub cache: Arc<BlockCache>,
    /// The family's merge operator, if any.
    pub merge: Option<Arc<dyn MergeOperator>>,
    /// Allocator for new SST ids (shared with the engine, persisted via manifest counters).
    pub sst_ids: Arc<AtomicU64>,
    /// Allocator for new blob file ids (likewise).
    pub blob_ids: Arc<AtomicU32>,
    /// Open readers for the family's blob files (to copy live values during blob GC).
    pub blob_files: Vec<(BlobFileId, Arc<BlobReader>)>,
    /// GC rules.
    pub gc: GcPolicy,
    /// Target output SST size (the picker's `target_sst_bytes`; default 64 MiB). Outputs are
    /// cut at row boundaries near it.
    pub target_sst_bytes: u64,
    /// Clock for [`CompactionJob::run`] deadlines (`Vfs::monotonic_nanos`). Without one,
    /// each `run` call does a fixed slice of work, or all of it for a `u64::MAX` deadline.
    pub clock: Option<VfsRef>,
}

impl JobContext {
    /// A context with no merge operator and no blob files.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table: TableId,
        family: FamilyOptions,
        pager: Arc<Pager>,
        cache: Arc<BlockCache>,
        sst_ids: Arc<AtomicU64>,
        blob_ids: Arc<AtomicU32>,
        gc: GcPolicy,
    ) -> Self {
        Self {
            table,
            family,
            pager,
            cache,
            merge: None,
            sst_ids,
            blob_ids,
            blob_files: Vec::new(),
            gc,
            target_sst_bytes: 64 << 20,
            clock: None,
        }
    }
}

/// Progress of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobPoll {
    /// More to do; call [`CompactionJob::run`] again.
    Pending,
    /// Finished; take [`CompactionJob::finish`].
    Done,
}

/// A blob file a job created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewBlobFile {
    /// Id (from `JobContext::blob_ids`).
    pub id: BlobFileId,
    /// Its extents in logical order.
    pub extents: Vec<ExtentRef>,
    /// Bytes written (all live at creation).
    pub total_bytes: u64,
}

/// The result of a job, for the engine to turn into manifest edits.
#[derive(Debug, Clone, Default)]
pub struct CompactionOutput {
    /// New SSTs and their levels.
    pub added: Vec<(u8, SstMeta)>,
    /// Removed SSTs (their extents are retired after the manifest commit).
    pub removed: Vec<SstId>,
    /// Blob files created (value separation at the output level, or blob GC copies).
    pub new_blob_files: Vec<NewBlobFile>,
    /// Change in live bytes per existing blob file (negative: values dropped).
    pub blob_live_delta: Vec<(BlobFileId, i64)>,
    /// Blob files with no live bytes left, to drop (`DropBlobFile`) and retire.
    pub dropped_blob_files: Vec<BlobFileId>,
}

/// The SST being written.
#[derive(Debug)]
struct Open {
    writer: SstWriter,
    extent: ExtentRef,
}

/// Where kept entries go: output SSTs cut near the target size, at row boundaries.
#[derive(Debug)]
struct Sink {
    pager: Arc<Pager>,
    sst_ids: Arc<AtomicU64>,
    options: SstWriterOptions,
    target: u64,
    open: Option<Open>,
    outputs: Vec<SstMeta>,
    last_row: Vec<u8>,
}

impl Sink {
    fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let row_start = self.last_row.is_empty() || !key.starts_with(&self.last_row);
        if let Some(o) = &self.open {
            // Prefer to cut between rows once less than an eighth of the extent is left.
            let margin = (o.extent.len() / 8) as usize;
            if (row_start && !o.writer.fits(0, margin)) || !o.writer.fits(key.len(), value.len()) {
                self.cut()?;
            }
        }
        if self.open.is_none() {
            let need = (key.len() + value.len()) as u64 * 2 + (64 << 10);
            let extent = self.pager.allocate(self.target.max(need).min(MAX_EXTENT))?;
            let id = SstId(self.sst_ids.fetch_add(1, Ordering::Relaxed));
            let file = self.pager.file().clone();
            self.open = Some(Open {
                writer: SstWriter::new(file, extent, id, self.options.clone()),
                extent,
            });
        }
        let Some(o) = &mut self.open else {
            unreachable!("opened above");
        };
        o.writer.add(key, value)?;
        if row_start {
            self.last_row.clear();
            let n = row_prefix_len(key).unwrap_or(key.len());
            self.last_row.extend_from_slice(&key[..n]);
        }
        Ok(())
    }

    /// Finishes the open SST, if any.
    fn cut(&mut self) -> Result<()> {
        let Some(o) = self.open.take() else {
            return Ok(());
        };
        if o.writer.entries() == 0 {
            self.pager.abandon(o.writer.abandon());
            return Ok(());
        }
        match o.writer.finish() {
            Ok(meta) => {
                self.outputs.push(meta);
                Ok(())
            }
            Err(e) => {
                self.pager.abandon(o.extent);
                Err(e.into())
            }
        }
    }

    fn abandon(&mut self) {
        if let Some(o) = self.open.take() {
            self.pager.abandon(o.writer.abandon());
        }
        for meta in self.outputs.drain(..) {
            self.pager.abandon(meta.extent);
        }
    }
}

/// A running compaction. Cooperative: each [`CompactionJob::run`] call does a bounded slice
/// of work.
///
/// A `Rewrite` merges the inputs within the task's (sub)ranges, applies GC (see
/// [`GcPolicy`]) and writes outputs into fresh extents, which are referenced only by the
/// returned [`CompactionOutput`]: until the engine commits it, the inputs are untouched and
/// an aborted or crashed job leaves nothing behind but space the next open reclaims (D8).
/// `TrivialMove` and `Drop` need no I/O: the engine applies them from the task and its
/// `SstMeta`s, and a job given one finishes at once with an empty output. Blob GC is Phase 2;
/// such a job also finishes empty.
///
/// After `run` fails, call [`CompactionJob::abort`].
///
/// ```
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicU32, AtomicU64};
/// use pigeonhole_cache::{BlockCache, Priority};
/// use pigeonhole_compaction::{
///     CompactionJob, CompactionTask, GcPolicy, JobContext, JobPoll, KeyRange, TaskKind,
/// };
/// use pigeonhole_format::key::{Kind, encode_key};
/// use pigeonhole_format::manifest::FamilyOptions;
/// use pigeonhole_format::{FamilyId, SstId, TableId, TabletId};
/// use pigeonhole_io::VfsRef;
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_pager::Pager;
/// use pigeonhole_sst::{SstReader, SstWriter, SstWriterOptions};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let vfs: VfsRef = SimVfs::new(1);
/// let pager = Arc::new(Pager::create(&vfs, "/db".as_ref())?);
/// let cache = Arc::new(BlockCache::new(1 << 20, 1));
/// let family = FamilyOptions::default();
/// let opts = SstWriterOptions::for_family(&family, TableId(1), FamilyId(1), TabletId(1));
///
/// // Two versions of one cell in two SSTs; the newer one deletes the older (ts <= 20).
/// let mut inputs = Vec::new();
/// for (id, kind, ts, seqno) in [(1, Kind::Put, 10, 1), (2, Kind::ColumnDelete, 20, 2)] {
///     let extent = pager.allocate(1)?;
///     let mut w = SstWriter::new(pager.file().clone(), extent, SstId(id), opts.clone());
///     let mut k = Vec::new();
///     encode_key(&mut k, b"row", b"q", ts, seqno, kind)?;
///     w.add(&k, if kind == Kind::Put { b"\x00v" } else { b"" })?;
///     let meta = w.finish()?;
///     inputs.push(Arc::new(SstReader::open(pager.file().clone(), &meta, cache.clone(), Priority::Low)?));
/// }
///
/// let task = CompactionTask {
///     tablet: TabletId(1),
///     family: FamilyId(1),
///     range: KeyRange::all(),
///     subranges: vec![KeyRange::all()],
///     inputs: vec![(1, vec![SstId(2)]), (2, vec![SstId(1)])],
///     output_level: 2,
///     kind: TaskKind::Rewrite,
/// };
/// let mut gc = GcPolicy::new(vec![], 0, true); // no snapshots, last level,
/// gc.min_ts_above = u64::MAX; // nothing above it
/// let ctx = JobContext::new(TableId(1), family, pager, cache,
///     Arc::new(AtomicU64::new(100)), Arc::new(AtomicU32::new(1)), gc);
/// let mut job = CompactionJob::new(task, inputs, ctx);
/// while job.run(u64::MAX)? == JobPoll::Pending {}
/// let out = job.finish()?;
/// assert!(out.added.is_empty()); // the put and its tombstone are both gone
/// assert_eq!(out.removed, [SstId(2), SstId(1)]);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct CompactionJob {
    task: CompactionTask,
    clock: Option<VfsRef>,
    cursor: MergingCursor<SstIter>,
    gc: Gc,
    out: OutBuf,
    sink: Sink,
    subranges: Vec<KeyRange>,
    sub: usize,
    sub_end: Option<Vec<u8>>,
    positioned: bool,
    done: bool,
}

impl CompactionJob {
    /// Prepares `task` over the open readers of its inputs.
    pub fn new(task: CompactionTask, inputs: Vec<Arc<SstReader>>, context: JobContext) -> Self {
        let mut read = ReadOptions::default();
        read.fill_cache = false;
        read.readahead_blocks = 4;
        let cursor = MergingCursor::new(
            inputs
                .iter()
                .map(|r| r.iter(ScanFilter::all(), read))
                .collect(),
        );
        let gc = Gc::new(GcConfig {
            snapshots: context.gc.snapshots.clone(),
            now: context.gc.now,
            bottommost: context.gc.bottommost,
            min_ts_above: context.gc.min_ts_above,
            ttl_micros: context.family.ttl_micros,
            max_versions: context.family.max_versions,
            merge: context.merge.clone(),
        });
        let mut options =
            SstWriterOptions::for_family(&context.family, context.table, task.family, task.tablet);
        options.created_micros = context.gc.now;
        let subranges = if task.subranges.is_empty() {
            vec![task.range.clone()]
        } else {
            task.subranges
                .iter()
                .map(|s| s.intersect(&task.range))
                .collect()
        };
        let done = task.kind != TaskKind::Rewrite;
        Self {
            sink: Sink {
                pager: context.pager,
                sst_ids: context.sst_ids,
                options,
                target: context.target_sst_bytes.max(1),
                open: None,
                outputs: Vec::new(),
                last_row: Vec::new(),
            },
            clock: context.clock,
            task,
            cursor,
            gc,
            out: OutBuf::default(),
            subranges,
            sub: 0,
            sub_end: None,
            positioned: false,
            done,
        }
    }

    /// Entries read from the inputs so far.
    pub fn entries_read(&self) -> u64 {
        self.gc.read
    }

    /// Entries written to the outputs so far (combined operands count once).
    pub fn entries_written(&self) -> u64 {
        self.gc.kept
    }

    /// Works until `deadline_nanos` (monotonic, from the Vfs clock) or completion.
    pub fn run(&mut self, deadline_nanos: u64) -> Result<JobPoll> {
        let mut units = 0u32;
        while !self.done {
            if !self.positioned {
                let Some(range) = self.subranges.get(self.sub) else {
                    self.sink.cut()?;
                    self.done = true;
                    break;
                };
                match &range.start {
                    Some(s) => self.cursor.seek(s)?,
                    None => self.cursor.seek_to_first()?,
                }
                self.sub_end = range.end.clone();
                self.gc.reset();
                self.positioned = true;
            }
            let more = self
                .gc
                .step(&mut self.cursor, self.sub_end.as_deref(), &mut self.out)?;
            for i in 0..self.out.len() {
                let (k, v) = self.out.get(i);
                self.sink.add(k, v)?;
            }
            self.out.clear();
            if !more {
                self.sub += 1;
                self.positioned = false;
                continue;
            }
            units += 1;
            if deadline_nanos != u64::MAX {
                match &self.clock {
                    Some(c) if units.is_multiple_of(CLOCK_EVERY) => {
                        if c.monotonic_nanos() >= deadline_nanos {
                            return Ok(JobPoll::Pending);
                        }
                    }
                    None if units >= SLICE_WITHOUT_CLOCK => return Ok(JobPoll::Pending),
                    _ => {}
                }
            }
        }
        Ok(JobPoll::Done)
    }

    /// The result. Call once after `Done` (if called earlier, it first runs the job to
    /// completion).
    pub fn finish(mut self) -> Result<CompactionOutput> {
        if !self.done {
            self.run(u64::MAX)?;
        }
        if self.task.kind != TaskKind::Rewrite {
            return Ok(CompactionOutput::default());
        }
        let level = self.task.output_level;
        Ok(CompactionOutput {
            added: self.sink.outputs.drain(..).map(|m| (level, m)).collect(),
            removed: self
                .task
                .inputs
                .iter()
                .flat_map(|(_, ids)| ids.iter().copied())
                .collect(),
            new_blob_files: Vec::new(),
            blob_live_delta: std::mem::take(&mut self.gc.blob_delta),
            dropped_blob_files: Vec::new(),
        })
    }

    /// Abandons the job, returning any output extents to the pager.
    pub fn abort(mut self) {
        self.sink.abandon();
    }
}
