//! Compaction pickers, jobs, merge operators and GC for Pigeonhole.
//!
//! - [`CompactionPicker`] chooses work for one `(tablet, family)` from its [`Levels`].
//! - [`CompactionJob`] runs it as a cooperative, time-sliced job: merges inputs, applies
//!   version/TTL/tombstone GC that preserves every live snapshot, resolves merge operands,
//!   separates large values into blob files, and returns a [`CompactionOutput`] the engine
//!   turns into manifest edits.
//! - [`MergingCursor`] and [`CellResolver`] are the shared read machinery: the engine's read
//!   path runs the same resolver over memtables and SSTs, so reads and compaction can never
//!   disagree about visibility.
//! - [`MergeOperator`]s are identified by name in the file.
//!
//! Scheduling is not here: the engine decides when, the runtime decides where.
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]
// Interface freeze: bodies are `todo!()`. Remove this allow when implementing.
#![allow(unused_variables, clippy::ptr_arg)]

use std::fmt;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use pigeonhole_cache::BlockCache;
use pigeonhole_format::manifest::{CompactionStyle, FamilyOptions, SstMeta};
use pigeonhole_format::{BlobFileId, Cursor, FamilyId, Seqno, SstId, TableId, TabletId, Timestamp};
use pigeonhole_pager::Pager;
use pigeonhole_sst::SstReader;

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Compaction errors.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Reading or writing an SST failed.
    Sst(pigeonhole_sst::Error),
    /// Allocating an extent failed.
    Pager(pigeonhole_pager::Error),
    /// A merge operator failed.
    Merge(MergeError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl std::error::Error for Error {}

impl From<pigeonhole_sst::Error> for Error {
    fn from(e: pigeonhole_sst::Error) -> Self {
        Self::Sst(e)
    }
}

impl From<pigeonhole_pager::Error> for Error {
    fn from(e: pigeonhole_pager::Error) -> Self {
        Self::Pager(e)
    }
}

impl From<MergeError> for Error {
    fn from(e: MergeError) -> Self {
        Self::Merge(e)
    }
}

// ---------------------------------------------------------------------------------------
// Merge operators
// ---------------------------------------------------------------------------------------

/// A merge operator failure (bad operand encoding, overflow policy).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeError {
    /// Operator name.
    pub operator: String,
    /// What went wrong.
    pub message: String,
}

impl fmt::Display for MergeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl std::error::Error for MergeError {}

/// A user-defined merge, applied at read and compaction time to operands written blind.
///
/// Identified by [`MergeOperator::name`], which is stored in the family's options in the
/// file, so a binary that registers a different operator under that name is the only way to
/// misinterpret the data. Values and operands are stored values (tag byte included).
pub trait MergeOperator: Send + Sync + fmt::Debug {
    /// Stable name, stored in the file. Built-ins use the `pigeonhole.` prefix.
    fn name(&self) -> &str;

    /// Combines `base` (the newest put below the operands, or none) with `operands` (oldest
    /// first) into a stored value appended to `out`.
    fn full_merge(
        &self,
        base: Option<&[u8]>,
        operands: &[&[u8]],
        out: &mut Vec<u8>,
    ) -> Result<(), MergeError>;

    /// Combines adjacent operands (oldest first) into one operand appended to `out`, if the
    /// operator is associative. Returns `false` if it cannot (compaction then keeps them).
    fn partial_merge(&self, operands: &[&[u8]], out: &mut Vec<u8>) -> Result<bool, MergeError> {
        Ok(false)
    }
}

/// Built-in `i64` add (the `incr` operator): operands and values are `ValueTag::I64`; a
/// missing base counts as 0; overflow wraps. Name: `pigeonhole.i64_add`.
#[derive(Debug, Clone, Copy, Default)]
pub struct I64Add;

impl MergeOperator for I64Add {
    fn name(&self) -> &str {
        "pigeonhole.i64_add"
    }

    fn full_merge(
        &self,
        base: Option<&[u8]>,
        operands: &[&[u8]],
        out: &mut Vec<u8>,
    ) -> Result<(), MergeError> {
        todo!()
    }

    fn partial_merge(&self, operands: &[&[u8]], out: &mut Vec<u8>) -> Result<bool, MergeError> {
        todo!()
    }
}

/// Operators available to this process, by name. Built-ins are always present.
#[derive(Debug, Clone, Default)]
pub struct MergeRegistry {
    _priv: (),
}

impl MergeRegistry {
    /// A registry with the built-ins.
    pub fn new() -> Self {
        todo!()
    }

    /// Registers `op` under its name, replacing any previous one.
    pub fn register(&mut self, op: Arc<dyn MergeOperator>) {
        todo!()
    }

    /// Looks up an operator.
    pub fn get(&self, name: &str) -> Option<Arc<dyn MergeOperator>> {
        todo!()
    }
}

// ---------------------------------------------------------------------------------------
// Shared read machinery
// ---------------------------------------------------------------------------------------

/// A k-way merge of cursors into one ordered cursor. Sources are ordered newest first; since
/// internal keys are unique (they contain the seqno), ties cannot occur.
#[derive(Debug)]
pub struct MergingCursor<C> {
    _sources: Vec<C>,
}

impl<C: Cursor> MergingCursor<C> {
    /// Merges `sources`.
    pub fn new(sources: Vec<C>) -> Self {
        todo!()
    }
}

impl<C: Cursor> Cursor for MergingCursor<C> {
    type Error = C::Error;

    fn valid(&self) -> bool {
        todo!()
    }

    fn key(&self) -> &[u8] {
        todo!()
    }

    fn value(&self) -> &[u8] {
        todo!()
    }

    fn seek_to_first(&mut self) -> Result<(), C::Error> {
        todo!()
    }

    fn seek(&mut self, target: &[u8]) -> Result<(), C::Error> {
        todo!()
    }

    fn next(&mut self) -> Result<(), C::Error> {
        todo!()
    }

    fn skip_row(&mut self) -> Result<(), C::Error> {
        todo!()
    }
}

/// A predicate on a resolved value.
#[derive(Debug, Clone, PartialEq)]
pub enum ValuePredicate {
    /// Value bytes equal.
    Equals(Vec<u8>),
    /// Value bytes start with.
    Prefix(Vec<u8>),
    /// Value bytes within the range.
    Range(Bound<Vec<u8>>, Bound<Vec<u8>>),
    /// The value is an `i64` and compares to the operand.
    I64(std::cmp::Ordering, i64),
}

/// What the read path asks of the resolver.
#[derive(Debug, Clone)]
pub struct ResolveOptions {
    /// Ignore entries with a newer seqno.
    pub snapshot: Seqno,
    /// Current time in microseconds, for TTL.
    pub now: Timestamp,
    /// Family TTL in microseconds (0 = none).
    pub ttl_micros: u64,
    /// Versions to return per column (1 = latest only; 0 = all retained).
    pub versions: u32,
    /// Columns to return per row (0 = unlimited); the rest of the row is skipped.
    pub columns_per_row: u32,
    /// Keep only cells whose resolved value matches.
    pub value: Option<ValuePredicate>,
    /// The family's merge operator, if any.
    pub merge: Option<Arc<dyn MergeOperator>>,
}

/// A cell as resolved: visible at the snapshot, not deleted, not expired, merges applied.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResolvedCell<'a> {
    /// The cell's internal key (row, qualifier, version); decode with
    /// `pigeonhole_format::decode_key`.
    pub key: &'a [u8],
    /// Timestamp.
    pub ts: Timestamp,
    /// Stored value (tag byte included): borrowed from the source, or from the resolver's
    /// merge buffer.
    pub value: &'a [u8],
    /// Whether `value` borrows the current source entry (so the caller can pin it instead of
    /// copying) rather than the merge buffer.
    pub from_source: bool,
}

/// Applies MVCC visibility to an ordered cursor: snapshot seqno, cell/column/family deletes,
/// TTL, version limits, columns per row, value predicates and merge resolution. A lending
/// iterator; no allocation per cell except merge results.
#[derive(Debug)]
pub struct CellResolver<C> {
    _cursor: C,
}

impl<C: Cursor> CellResolver<C>
where
    C::Error: From<MergeError>,
{
    /// A resolver over `cursor`.
    pub fn new(cursor: C, options: ResolveOptions) -> Self {
        todo!()
    }

    /// Positions at the first entry `>= key` (an encoded seek or row prefix).
    pub fn seek(&mut self, key: &[u8]) -> Result<(), C::Error> {
        todo!()
    }

    /// The next visible cell, or `None` at the end of the cursor.
    pub fn next_cell(&mut self) -> Result<Option<ResolvedCell<'_>>, C::Error> {
        todo!()
    }

    /// Skips the rest of the current row.
    pub fn skip_row(&mut self) -> Result<(), C::Error> {
        todo!()
    }

    /// The underlying cursor (to pin the current block for a zero-copy value).
    pub fn cursor(&self) -> &C {
        todo!()
    }
}

// ---------------------------------------------------------------------------------------
// Picking and running compactions
// ---------------------------------------------------------------------------------------

/// The SSTs of one `(tablet, family)` by level. Level 0 is ordered newest first and may
/// overlap; deeper levels are sorted by key and disjoint.
#[derive(Debug, Clone, Default)]
pub struct Levels {
    /// `levels[n]` is level `n`.
    pub levels: Vec<Vec<Arc<SstMeta>>>,
}

/// Tuning for the pickers.
#[derive(Debug, Clone, PartialEq)]
pub struct PickerOptions {
    /// L0 file count that triggers an L0 compaction.
    pub l0_trigger: u32,
    /// Target size of level 1 in bytes.
    pub level_base_bytes: u64,
    /// Size ratio between adjacent levels.
    pub level_multiplier: u32,
    /// Number of levels.
    pub max_levels: u8,
    /// Target output SST size.
    pub target_sst_bytes: u64,
}

impl Default for PickerOptions {
    fn default() -> Self {
        todo!()
    }
}

/// One unit of compaction work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionTask {
    /// Tablet.
    pub tablet: TabletId,
    /// Family.
    pub family: FamilyId,
    /// Input SSTs by level.
    pub inputs: Vec<(u8, Vec<SstId>)>,
    /// Level the outputs go to.
    pub output_level: u8,
    /// How to carry it out.
    pub kind: TaskKind,
}

/// How a task changes the tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    /// Merge inputs into new SSTs.
    Rewrite,
    /// Move one SST down a level without rewriting it.
    TrivialMove,
    /// Drop whole SSTs (FIFO-by-time expiry): no I/O.
    Drop,
}

/// Picks compaction work for one `(tablet, family)`.
#[derive(Debug, Clone)]
pub struct CompactionPicker {
    _priv: (),
}

impl CompactionPicker {
    /// A picker for `style`. Phase 1 implements `Leveled`; the others return no work until
    /// Phase 2.
    pub fn new(style: CompactionStyle, options: PickerOptions) -> Self {
        todo!()
    }

    /// Urgency: `>= 1.0` means compaction is due. The engine services the highest score
    /// first and throttles writes on L0 depth.
    pub fn score(&self, levels: &Levels) -> f64 {
        todo!()
    }

    /// The next task, or `None`. `busy` lists SSTs already in a running job. `now` drives
    /// FIFO expiry.
    pub fn pick(
        &self,
        tablet: TabletId,
        family: FamilyId,
        levels: &Levels,
        busy: &[SstId],
        now: Timestamp,
        ttl_micros: u64,
    ) -> Option<CompactionTask> {
        todo!()
    }
}

/// Which versions compaction must keep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcPolicy {
    /// Every live snapshot seqno (in-process and reader slots), ascending. A version visible
    /// at any of them, or newer than all of them, is kept.
    pub snapshots: Vec<Seqno>,
    /// Current time in microseconds.
    pub now: Timestamp,
    /// Whether the output is the bottommost data for its key range (tombstones can go).
    pub bottommost: bool,
}

/// Everything a job needs from the engine.
#[derive(Debug, Clone)]
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
    /// GC rules.
    pub gc: GcPolicy,
}

/// Progress of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobPoll {
    /// More to do; call [`CompactionJob::run`] again.
    Pending,
    /// Finished; take [`CompactionJob::finish`].
    Done,
}

/// The result of a job, for the engine to turn into manifest edits.
#[derive(Debug, Clone, Default)]
pub struct CompactionOutput {
    /// New SSTs and their levels.
    pub added: Vec<(u8, SstMeta)>,
    /// Removed SSTs (their extents are retired after the manifest commit).
    pub removed: Vec<SstId>,
    /// Change in live bytes per blob file (negative: values dropped).
    pub blob_live_delta: Vec<(BlobFileId, i64)>,
}

/// A running compaction. Cooperative: each [`CompactionJob::run`] call does a bounded slice
/// of work.
#[derive(Debug)]
pub struct CompactionJob {
    _priv: (),
}

impl CompactionJob {
    /// Prepares `task` over the open readers of its inputs.
    pub fn new(task: CompactionTask, inputs: Vec<Arc<SstReader>>, context: JobContext) -> Self {
        todo!()
    }

    /// Works until `deadline_nanos` (monotonic, from the Vfs clock) or completion.
    pub fn run(&mut self, deadline_nanos: u64) -> Result<JobPoll> {
        todo!()
    }

    /// The result. Call once after `Done`.
    pub fn finish(self) -> Result<CompactionOutput> {
        todo!()
    }

    /// Abandons the job, returning any output extents to the pager.
    pub fn abort(self) {
        todo!()
    }
}
