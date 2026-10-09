//! Running a compaction: merge the inputs, apply GC, write the outputs.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use pigeonhole_cache::BlockCache;
use pigeonhole_format::key::{row_prefix_len, split_suffix};
use pigeonhole_format::manifest::{FamilyKind, FamilyOptions, SstMeta};
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::{BlobFileId, Cursor, Seqno, SstId, TableId, Timestamp};
use pigeonhole_io::VfsRef;
use pigeonhole_pager::Pager;
use pigeonhole_sst::{
    BlobReader, ReadOptions, ScanFilter, SstIter, SstReader, SstWriter, SstWriterOptions,
};

use crate::blob::{
    BLOB_STORED_LEN, BlobSink, blob_pointer, encode_blob_stored, note_blob_ref, record_bytes,
    separates,
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
    /// Counter families only (decision D179): the other sources of the slot (SSTs not in
    /// the task, memtables, prepared cross-shard shares) that may hold an entry with a seqno
    /// at or below the newest input seqno; every such source must be listed. A delete there
    /// can hide one of two input operands but not the other, so operands are not combined
    /// across its seqno range; and a bottommost delete is purged without the `min_ts_above`
    /// rule only when no listed source overlapping its row starts at or below its seqno
    /// (#290). `None` (the default) means unknown: no counter operand is combined and only
    /// the `min_ts_above` rule purges. (Counter families never get a `max_versions` purge,
    /// whatever this holds.)
    pub other_sources: Option<Vec<OtherSource>>,
    /// No source of the slot outside the job's input can hold a delete (every other SST
    /// has none, nor do the other memtables or prepared shares). Then a version beyond
    /// `max_versions` among the input's own versions of a column stays hidden whatever lies
    /// outside, so it is purged at any level, not only the bottommost (#287; see
    /// `docs/design/questions/engine.md`). Ignored for counter families (D186). Default false.
    pub no_outside_deletes: bool,
}

/// A source of a compaction's slot outside its inputs, for [`GcPolicy::other_sources`]: an
/// SST not in the task, a memtable, or a prepared, undecided cross-shard share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtherSource {
    /// Smallest and largest internal key, or `None` for a source that may hold any key (a
    /// memtable or a prepared share).
    pub keys: Option<(Vec<u8>, Vec<u8>)>,
    /// Smallest and largest seqno.
    pub seqnos: (Seqno, Seqno),
}

impl GcPolicy {
    /// A policy keeping everything visible at `snapshots`.
    pub fn new(snapshots: Vec<Seqno>, now: Timestamp, bottommost: bool) -> Self {
        Self {
            snapshots,
            now,
            bottommost,
            min_ts_above: 0,
            other_sources: None,
            no_outside_deletes: false,
        }
    }
}

/// Compaction's GC over any ordered stream of internal entries, with exactly the rules a
/// job with the same [`GcPolicy`] applies: a flush runs its memtable through one as a
/// non-bottommost job (#287).
#[derive(Debug)]
pub struct StreamGc {
    gc: Gc,
    out: OutBuf,
}

impl StreamGc {
    /// GC under `policy` for a family with `family`'s options and merge operator.
    pub fn new(
        policy: &GcPolicy,
        family: &FamilyOptions,
        merge: Option<Arc<dyn MergeOperator>>,
    ) -> Self {
        Self {
            gc: Gc::new(GcConfig {
                snapshots: policy.snapshots.clone(),
                now: policy.now,
                bottommost: policy.bottommost,
                min_ts_above: policy.min_ts_above,
                ttl_micros: family.ttl_micros,
                max_versions: family.max_versions,
                merge,
                counter: family.kind == FamilyKind::Counter,
                other_sources: policy.other_sources.clone(),
                no_outside_deletes: policy.no_outside_deletes,
            }),
            out: OutBuf::default(),
        }
    }

    /// Reads the next unit at `cursor` (a family marker or a whole `(column, timestamp)`
    /// group) and buffers what it keeps. Returns false at the end of the cursor.
    pub fn step<C: Cursor>(&mut self, cursor: &mut C) -> std::result::Result<bool, C::Error> {
        // Entries a step left in the GC's group buffer (not drained yet) are copied out
        // before this step reuses it.
        self.out.own(self.gc.group_data());
        self.gc.step(cursor, None, &mut self.out)
    }

    /// Hands every buffered entry to `f`, in order, and empties the buffer.
    pub fn drain<E>(
        &mut self,
        mut f: impl FnMut(&[u8], &[u8]) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        for i in 0..self.out.len() {
            let (k, v) = self.out.get(i, self.gc.group_data());
            f(k, v)?;
        }
        self.out.clear();
        Ok(())
    }

    /// Live-byte change per blob file from dropped blob pointers.
    pub fn blob_delta(&self) -> &[(BlobFileId, i64)] {
        &self.gc.blob_delta
    }

    /// Entries read and kept so far.
    pub fn counts(&self) -> (u64, u64) {
        (self.gc.read, self.gc.kept)
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
    /// Open readers for the family's blob files (to copy live values during blob GC). A
    /// [`TaskKind::BlobGc`] job needs one for every blob file it empties.
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

/// Per SST, the blob files its puts point into and the bytes they reference
/// ([`CompactionJob::finish_with_blob_refs`]).
pub type BlobRefs = Vec<(SstId, Vec<(BlobFileId, u64)>)>;

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
#[non_exhaustive]
pub struct CompactionOutput {
    /// New SSTs and their levels.
    pub added: Vec<(u8, SstMeta)>,
    /// Removed SSTs (their extents are retired after the manifest commit).
    pub removed: Vec<SstId>,
    /// Blob files created (value separation at the output level, or blob GC copies).
    pub new_blob_files: Vec<NewBlobFile>,
    /// Change in live bytes per existing blob file (negative: values dropped).
    pub blob_live_delta: Vec<(BlobFileId, i64)>,
    /// Blob files with no live bytes left, to drop (`DropBlobFile`) and retire. Always empty:
    /// a job does not know a file's live bytes (other tablets may reference it after a
    /// split), so the engine drops a file once its live count, updated from
    /// `blob_live_delta`, reaches zero.
    pub dropped_blob_files: Vec<BlobFileId>,
}

/// The SST being written.
#[derive(Debug)]
struct Open {
    writer: SstWriter,
    extent: ExtentRef,
}

/// The size of the next output SST of a level (≥ 1) output stream with about `remaining`
/// bytes left to write, the next one included (#185): a power of two that the SST fills,
/// the target's class while more than the target remains, then at most half of what
/// remains, so the pieces of a stream pack into a file about as large as the data (an
/// extent is aligned to its size and unit 0 is the header, so a file is at least twice its
/// largest extent). The smallest piece is 1 MiB, or 64 KiB for a stream under 2 MiB; the
/// last piece of a stream is what is left, rounded up to a power of two.
///
/// ```
/// use pigeonhole_compaction::output_piece_bytes;
///
/// let mib = 1 << 20;
/// let target = 64 * mib;
/// assert_eq!(output_piece_bytes(200 * mib, target), 64 * mib); // full target-size SSTs
/// assert_eq!(output_piece_bytes(51 * mib, target), 16 * mib); // then at most half the rest
/// assert_eq!(output_piece_bytes(3 * mib, target), mib);
/// assert_eq!(output_piece_bytes(1536 << 10, target), 512 << 10); // a small stream
/// assert_eq!(output_piece_bytes(48 << 10, target), 64 << 10); // the last piece
/// ```
pub fn output_piece_bytes(remaining: u64, target: u64) -> u64 {
    const UNIT: u64 = 64 << 10;
    let floor_pow2 = |n: u64| {
        if n == 0 {
            0
        } else {
            1u64 << (63 - n.leading_zeros())
        }
    };
    let top = floor_pow2(target.clamp(UNIT, MAX_EXTENT));
    if remaining > top {
        return top;
    }
    let min = if remaining < 2 << 20 { UNIT } else { 1 << 20 };
    if remaining <= min {
        return remaining.max(1).next_power_of_two().max(UNIT);
    }
    floor_pow2(remaining / 2).max(min)
}

/// Where kept entries go: output SSTs cut at row boundaries, sized by [`output_piece_bytes`]
/// for outputs below L0 (#185).
///
/// The remaining output is projected from the inputs: the bytes not yet read, scaled by the
/// output-to-input ratio so far (GC only shrinks data), so a compaction that drops most of
/// its input still cuts small pieces. Each output's extent is that piece's size (or the
/// entry at hand, if larger). An output is cut between rows once less than a thirty-second
/// of its extent is left, or early once it holds the smaller piece a later projection calls
/// for; its extent is trimmed to its length at finish. An L0 output (a FIFO merge of L0 files) is one SST, sized to the input
/// left, as every L0 file stays one SST (#106).
#[derive(Debug)]
struct Sink {
    pager: Arc<Pager>,
    sst_ids: Arc<AtomicU64>,
    options: SstWriterOptions,
    target: u64,
    /// Whether outputs are cut into power-of-two pieces (outputs below L0).
    pieces: bool,
    /// Total length and entries of the inputs, and the entries read so far.
    input_bytes: u64,
    input_entries: u64,
    read: u64,
    /// Total length of the outputs finished so far.
    written: u64,
    open: Option<Open>,
    outputs: Vec<SstMeta>,
    /// The open SST's blob references, then each output's (parallel to `outputs`).
    open_refs: Vec<(BlobFileId, u64)>,
    output_refs: Vec<Vec<(BlobFileId, u64)>>,
    last_row: Vec<u8>,
    /// Separated values (created on first use).
    blobs: Option<BlobSink>,
    blob_ids: Arc<AtomicU32>,
    /// Puts whose payload is longer than this are separated (`u32::MAX`: never).
    blob_threshold: u32,
    /// Blob files a blob GC job empties: values in them are copied to new files.
    gc_files: Vec<(BlobFileId, Arc<BlobReader>)>,
    /// Live bytes moved out of `gc_files`, per file (negative).
    copied: Vec<(BlobFileId, i64)>,
}

impl Sink {
    /// Separates or copies `value` into a blob file when the entry needs it (a put above the
    /// threshold, or a pointer into a file being emptied); returns the stored value to
    /// write.
    fn place(&mut self, key: &[u8], value: &[u8]) -> Result<Option<[u8; BLOB_STORED_LEN]>> {
        let Ok((_, _, _, kind)) = split_suffix(key) else {
            return Ok(None);
        };
        if let Some(ptr) = blob_pointer(value) {
            if kind != pigeonhole_format::key::Kind::Put {
                return Ok(None);
            }
            let Some((_, reader)) = self.gc_files.iter().find(|(id, _)| *id == ptr.blob_file)
            else {
                return Ok(None);
            };
            let cell = Arc::clone(reader).read(&ptr)?;
            let bytes = record_bytes(ptr.len) as i64;
            match self.copied.iter_mut().find(|d| d.0 == ptr.blob_file) {
                Some(d) => d.1 -= bytes,
                None => self.copied.push((ptr.blob_file, -bytes)),
            }
            let new = self.blob_sink().append(&cell)?;
            return Ok(Some(encode_blob_stored(&new)));
        }
        if separates(kind, value, self.blob_threshold) {
            let new = self.blob_sink().append(value)?;
            return Ok(Some(encode_blob_stored(&new)));
        }
        Ok(None)
    }

    fn blob_sink(&mut self) -> &mut BlobSink {
        let (input, target) = (self.input_bytes, self.target);
        self.blobs.get_or_insert_with(|| {
            BlobSink::new(
                Arc::clone(&self.pager),
                Arc::clone(&self.blob_ids),
                input / 2,
                target.saturating_mul(4),
            )
        })
    }

    fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let row_start =
            self.last_row.is_empty() || !pigeonhole_format::key::starts_with(key, &self.last_row);
        if let Some(o) = &self.open {
            // Prefer to cut between rows once less than an eighth of the extent is left (a
            // thirty-second for pieces, which should fill their class: the index, filters
            // and footer take a few KiB), or (pieces) once the output holds the piece the
            // projection now calls for, if that is smaller than its extent.
            let margin = (o.extent.len() / if self.pieces { 32 } else { 8 }) as usize;
            let piece_full = self.pieces && row_start && {
                let len = o.writer.data_len();
                let piece = output_piece_bytes(len + self.projected_rest(), self.target);
                piece < o.extent.len() && len >= piece - piece / 32
            };
            if (row_start && !o.writer.fits(0, margin))
                || piece_full
                || !o.writer.fits(key.len(), value.len())
            {
                self.cut()?;
            }
        }
        if self.open.is_none() {
            let need = (key.len() + value.len()) as u64 * 2 + (64 << 10);
            let extent = self.pager.allocate(self.extent_bytes(need))?;
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
        note_blob_ref(&mut self.open_refs, key, value);
        if row_start {
            self.last_row.clear();
            let n = row_prefix_len(key).unwrap_or(key.len());
            self.last_row.extend_from_slice(&key[..n]);
        }
        Ok(())
    }

    /// The output still to come after the open SST (#185): the input not yet read, scaled
    /// by the output-to-input ratio so far once there is enough of it to go by.
    fn projected_rest(&self) -> u64 {
        let total = self.input_entries.max(1);
        let read = self.read.min(total);
        let unread =
            (u128::from(self.input_bytes) * u128::from(total - read) / u128::from(total)) as u64;
        let open = self.open.as_ref().map_or(0, |o| o.writer.data_len());
        let out = self.written + open;
        // Below a sixty-fourth of the input read, or before any output, the ratio says
        // little: assume everything survives.
        if read * 64 < total || out == 0 {
            return unread;
        }
        let consumed = (u128::from(self.input_bytes) * u128::from(read) / u128::from(total)).max(1);
        let ratio_out = u128::from(unread) * u128::from(out) / consumed;
        ratio_out.min(u128::from(unread)) as u64
    }

    /// Bytes to allocate for the next output, at least `need` (room for the entry at hand).
    fn extent_bytes(&self, need: u64) -> u64 {
        if self.pieces {
            // Exactly the piece: the output fills it, and is trimmed at finish if it ends
            // smaller.
            let piece = output_piece_bytes(self.projected_rest(), self.target);
            return piece.max(need).min(MAX_EXTENT);
        }
        let rest = self.input_bytes.saturating_sub(self.written);
        // An eighth more covers the cut margin in `add`; past the estimate, take the target.
        let estimate = if rest == 0 {
            self.target
        } else {
            (rest + rest / 8 + (64 << 10)).min(self.target)
        };
        estimate.max(need).min(MAX_EXTENT)
    }

    /// Finishes the open SST, if any, trimming its extent to its length.
    fn cut(&mut self) -> Result<()> {
        let Some(o) = self.open.take() else {
            return Ok(());
        };
        let refs = std::mem::take(&mut self.open_refs);
        if o.writer.entries() == 0 {
            self.pager.abandon(o.writer.abandon());
            return Ok(());
        }
        match o.writer.finish() {
            Ok(mut meta) => {
                meta.extent = self.pager.trim(meta.extent, meta.len);
                self.written += meta.len;
                self.outputs.push(meta);
                self.output_refs.push(refs);
                Ok(())
            }
            Err(e) => {
                self.pager.abandon(o.extent);
                Err(e.into())
            }
        }
    }

    fn abandon(&mut self) {
        if let Some(mut b) = self.blobs.take() {
            b.abandon();
        }
        if let Some(o) = self.open.take() {
            self.pager.abandon(o.writer.abandon());
        }
        for meta in self.outputs.drain(..) {
            self.pager.abandon(meta.extent);
        }
        self.output_refs.clear();
        self.open_refs.clear();
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
/// `SstMeta`s, and a job given one finishes at once with an empty output.
///
/// Every kept put whose payload is longer than the family's `blob_threshold` is separated
/// into a new blob file (FORMAT §7) and the output stores its pointer; values that are
/// already pointers pass through. A `BlobGc` job is a `Rewrite` that also copies the values
/// still referenced in its blob files into new ones. The output lists the new files and the
/// live-byte change of every older file it stopped referencing (`blob_live_delta`).
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
            counter: context.family.kind == FamilyKind::Counter,
            other_sources: context.gc.other_sources.clone(),
            no_outside_deletes: context.gc.no_outside_deletes,
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
        debug_assert!(
            subranges.iter().chain([&task.range]).all(|r| {
                [&r.start, &r.end]
                    .into_iter()
                    .flatten()
                    .all(|b| row_prefix_len(b).is_ok_and(|n| n == b.len()))
            }),
            "task range and subrange bounds must be encoded row prefixes"
        );
        let gc_files = match &task.kind {
            TaskKind::BlobGc { blob_files } => context
                .blob_files
                .iter()
                .filter(|(id, _)| blob_files.contains(id))
                .cloned()
                .collect(),
            _ => Vec::new(),
        };
        let done = !rewrites(&task.kind);
        Self {
            sink: Sink {
                pager: context.pager,
                sst_ids: context.sst_ids,
                options,
                target: context.target_sst_bytes.max(1),
                pieces: task.output_level > 0,
                input_bytes: inputs.iter().map(|r| r.len_bytes()).sum(),
                input_entries: inputs.iter().map(|r| r.properties().entries).sum(),
                read: 0,
                written: 0,
                open: None,
                outputs: Vec::new(),
                open_refs: Vec::new(),
                output_refs: Vec::new(),
                last_row: Vec::new(),
                blobs: None,
                blob_ids: context.blob_ids,
                blob_threshold: context.family.blob_threshold,
                gc_files,
                copied: Vec::new(),
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
            self.sink.read = self.gc.read;
            for i in 0..self.out.len() {
                let (k, v) = self.out.get(i, self.gc.group_data());
                match self.sink.place(k, v)? {
                    Some(stored) => self.sink.add(k, &stored)?,
                    None => self.sink.add(k, v)?,
                }
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
    ///
    /// If the remaining work fails, the outputs written so far are returned to the pager.
    pub fn finish(self) -> Result<CompactionOutput> {
        self.finish_with_blob_refs().map(|(out, _)| out)
    }

    /// [`finish`](Self::finish), plus each added SST's blob references
    /// (`Edit::SstBlobRefs`, #240): the blob files its puts point into and the bytes they
    /// reference, sorted by blob file, an empty list for an SST with no pointer. Every SST
    /// of `added` has an entry, in the same order.
    pub fn finish_with_blob_refs(mut self) -> Result<(CompactionOutput, BlobRefs)> {
        if !self.done
            && let Err(e) = self.run(u64::MAX)
        {
            self.sink.abandon();
            return Err(e);
        }
        if !rewrites(&self.task.kind) {
            return Ok((CompactionOutput::default(), Vec::new()));
        }
        let new_blob_files = match self.sink.blobs.take().map(BlobSink::finish) {
            Some(Ok(files)) => files,
            Some(Err(e)) => {
                self.sink.abandon();
                return Err(e);
            }
            None => Vec::new(),
        };
        let mut blob_live_delta = std::mem::take(&mut self.gc.blob_delta);
        for (id, d) in self.sink.copied.drain(..) {
            match blob_live_delta.iter_mut().find(|x| x.0 == id) {
                Some(x) => x.1 += d,
                None => blob_live_delta.push((id, d)),
            }
        }
        let level = self.task.output_level;
        let blob_refs = self
            .sink
            .outputs
            .iter()
            .map(|m| m.id)
            .zip(self.sink.output_refs.drain(..))
            .collect();
        let out = CompactionOutput {
            added: self.sink.outputs.drain(..).map(|m| (level, m)).collect(),
            removed: self
                .task
                .inputs
                .iter()
                .flat_map(|(_, ids)| ids.iter().copied())
                .collect(),
            new_blob_files,
            blob_live_delta,
            dropped_blob_files: Vec::new(),
        };
        Ok((out, blob_refs))
    }

    /// Abandons the job, returning any output extents to the pager.
    pub fn abort(mut self) {
        self.sink.abandon();
    }
}

/// Whether a task kind rewrites its inputs (and so runs as a job).
fn rewrites(kind: &TaskKind) -> bool {
    matches!(kind, TaskKind::Rewrite | TaskKind::BlobGc { .. })
}
