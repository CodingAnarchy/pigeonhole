//! Bounded-result read pushdown (D296, #490 A): a visitor runs on each resolved cell of the
//! input, where the scan would have handed it out, and a reducer keeps a bounded result (the
//! k best, the first n, one per key). Only the reducer's result leaves the read.
//!
//! The scan's own code ([`super::ScanCursor`]) is untouched: this module's loop is a sibling
//! that drives the same per-family [`Lane`]s (resolver over the tablet's sources), so the
//! visitor sees exactly what a scan returns (snapshot, deletes, TTL, versions, merges and
//! value predicates applied) and reads each value where the lane holds it, without assembling
//! rows. #1167 measured that any change to the scan's per-cell loop costs its shapes.
//!
//! Work is cut at tablet boundaries into groups. Each group has its own visitor and reducer,
//! and stops only on its own account (a visitor's [`Visit::Stop`], or its reducer's `Break`):
//! no group stops because of another, so a group's result is a function of its key range at
//! the snapshot, and merging the groups' results gives exactly the sequential result at any
//! fan-out (D296 rev 1.2). The one shared flag ends every group on an error.

use std::ops::{Bound, ControlFlow};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use pigeonhole_format::key::{Escaped, TERMINATOR, encode_row_prefix};
use pigeonhole_format::{FamilyId, TableId, Timestamp};

use super::{
    FORK_CHECK_ROWS, Lane, MergeKind, ReadSpec, clamp_into, give_back, past_row, row_of,
    skips_columns, take_scan_resolver,
};
use crate::snapshot::{Snapshot, TabletEntry, View};
use crate::{Error, Result};

/// A row range on unescaped row keys, `(start, end)`.
pub type RowRange = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// What a bounded read reads.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ReadInput {
    /// Row ranges, sorted and disjoint, each `(start, end)` on unescaped row keys. One range
    /// is a scan's input; several cover, for example, an IVF index's probed partitions in one
    /// read under one snapshot.
    Ranges(Vec<RowRange>),
}

/// What a [`CellVisitor`] tells the read to do after one cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Visit<I> {
    /// Nothing from this cell; go on to the next.
    Skip,
    /// Push `I` to the reducer and go on.
    Emit(I),
    /// Push `I`, then skip the rest of this row (a seek past it, not a walk).
    EmitAndNextRow(I),
    /// Skip the rest of this row.
    NextRow,
    /// End this group's read.
    Stop,
}

/// Runs on each resolved cell of a bounded read.
pub trait CellVisitor {
    /// What it hands the reducer.
    type Item;

    /// One cell, borrowed for the call: `row` and `qualifier` unescaped, `value` the stored
    /// value with its tag byte (in place where the source holds it).
    fn visit(
        &mut self,
        row: &[u8],
        family: FamilyId,
        qualifier: &[u8],
        ts: Timestamp,
        value: &[u8],
    ) -> Visit<Self::Item>;
}

/// The cell an item came from, borrowed for a [`Reducer::push`]. Cells order as a scan hands
/// them out: by row, then family in the read's family order, then qualifier, then newest
/// version first. That is a total order over a read's cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CellRef<'a> {
    /// Unescaped row key.
    pub row: &'a [u8],
    /// The family's position in the read's family order (the scan's).
    pub family_rank: u32,
    /// The family.
    pub family: FamilyId,
    /// Unescaped qualifier.
    pub qualifier: &'a [u8],
    /// The version's timestamp.
    pub ts: Timestamp,
}

impl CellRef<'_> {
    fn order(&self) -> (&[u8], u32, &[u8], std::cmp::Reverse<Timestamp>) {
        (
            self.row,
            self.family_rank,
            self.qualifier,
            std::cmp::Reverse(self.ts),
        )
    }

    /// An owned copy.
    pub fn to_key(&self) -> CellKey {
        CellKey {
            row: self.row.to_vec(),
            family_rank: self.family_rank,
            family: self.family,
            qualifier: self.qualifier.to_vec(),
            ts: self.ts,
        }
    }
}

impl PartialOrd for CellRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CellRef<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order().cmp(&other.order())
    }
}

/// An owned [`CellRef`]: a result's cell, ordered the same way.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CellKey {
    /// Unescaped row key.
    pub row: Vec<u8>,
    /// The family's position in the read's family order.
    pub family_rank: u32,
    /// The family.
    pub family: FamilyId,
    /// Unescaped qualifier.
    pub qualifier: Vec<u8>,
    /// The version's timestamp.
    pub ts: Timestamp,
}

impl CellKey {
    /// This key, borrowed.
    pub fn as_ref(&self) -> CellRef<'_> {
        CellRef {
            row: &self.row,
            family_rank: self.family_rank,
            family: self.family,
            qualifier: &self.qualifier,
            ts: self.ts,
        }
    }
}

impl PartialOrd for CellKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CellKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(&other.as_ref())
    }
}

/// Keeps a bounded result. `merge` must be associative and commutative, and the result a
/// function of the multiset of pushes, so fan-out equals the sequential read.
pub trait Reducer: Send {
    /// What it keeps.
    type Item: Send;

    /// One item from `cell`. A group pushes in cell order. `Break` ends this group's read
    /// (the reducer is full, or has all it can take).
    fn push(&mut self, cell: CellRef<'_>, item: Self::Item) -> ControlFlow<()>;

    /// Folds another group's result in.
    fn merge(&mut self, other: Self);

    /// The result, in the reducer's order.
    fn finish(self) -> Vec<(CellKey, Self::Item)>;
}

/// A bounded read's input, per-row selection and fan-out.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ReduceSpec {
    /// The rows read.
    pub input: ReadInput,
    /// Families, qualifier selection, time range, value predicates and versions, as for a scan.
    pub read: ReadSpec,
    /// Threads the call may use (1 = the caller's only). More run tablet groups on scoped
    /// threads owned by the call, never the engine's: only when the input is large enough to
    /// pay for them (`parallel_floor` estimated entries per thread), and never on a thread
    /// that drives a shard (D88).
    pub parallelism: usize,
    /// Estimated entries per thread below which a read does not fan out
    /// ([`PARALLEL_FLOOR_ENTRIES`] by default; 0 fans out whenever there are groups).
    pub parallel_floor: u64,
}

impl ReduceSpec {
    /// `input` with the default selection, on the caller's thread.
    pub fn new(input: ReadInput) -> Self {
        Self {
            input,
            read: ReadSpec::default(),
            parallelism: 1,
            parallel_floor: PARALLEL_FLOOR_ENTRIES,
        }
    }
}

/// A bounded read's result and counts.
#[derive(Debug)]
#[non_exhaustive]
pub struct Reduced<I> {
    /// `(cell, item)`, in the reducer's order.
    pub items: Vec<(CellKey, I)>,
    /// Counts over every group.
    pub stats: ReduceStats,
}

/// Counts of a bounded read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReduceStats {
    /// Tablet groups read.
    pub groups: u64,
    /// Rows the visitor saw a cell of.
    pub rows: u64,
    /// Cells the visitor saw.
    pub cells: u64,
    /// Threads used (1 when the read ran on the caller's thread).
    pub threads: u64,
}

impl ReduceStats {
    fn add(&mut self, o: &ReduceStats) {
        self.groups += o.groups;
        self.rows += o.rows;
        self.cells += o.cells;
    }
}

/// Estimated entries a tablet group must hold, on average, before a read fans out: about
/// 20,000 rows at today's per-row cost is some 2 ms, against 10-20 µs to start a thread
/// (D296 rev 1, perf287's work floor).
pub const PARALLEL_FLOOR_ENTRIES: u64 = 20_000;

/// One tablet's share of one input range.
struct Group<'a> {
    tablet: &'a TabletEntry,
    /// The range, unescaped (for the excluded start and the end check).
    start: &'a Bound<Vec<u8>>,
    end: &'a Bound<Vec<u8>>,
    /// The range clamped to the tablet, as row prefixes (`None` = unbounded).
    lo: Option<Vec<u8>>,
    hi: Option<Vec<u8>>,
}

/// Cuts `input` into tablet groups, in key order.
fn groups<'a>(view: &'a View, table: TableId, input: &'a ReadInput) -> Result<Vec<Group<'a>>> {
    let ReadInput::Ranges(ranges) = input;
    // Sorted and disjoint, or a cell would be visited (and counted by a reducer) twice.
    for pair in ranges.windows(2) {
        if !before(&pair[0].1, &pair[1].0) {
            return Err(Error::InvalidArgument(
                "bounded read: the ranges must be sorted and disjoint".to_owned(),
            ));
        }
    }
    let mut out = Vec::new();
    let (mut lo, mut hi) = (Vec::new(), Vec::new());
    for (start, end) in ranges {
        let s = match start {
            Bound::Included(s) | Bound::Excluded(s) => {
                let mut k = Vec::new();
                encode_row_prefix(&mut k, s)?;
                Some(k)
            }
            Bound::Unbounded => None,
        };
        let e = match end {
            Bound::Excluded(e) => {
                let mut k = Vec::new();
                encode_row_prefix(&mut k, e)?;
                Some(k)
            }
            Bound::Included(e) => {
                let mut k = Vec::new();
                encode_row_prefix(&mut k, e)?;
                let mut past = Vec::new();
                past_row(&k, &mut past);
                Some(past)
            }
            Bound::Unbounded => None,
        };
        for tablet in view.tablets().tablets_of(table) {
            let (has_lo, has_hi) =
                clamp_into(tablet, s.as_deref(), e.as_deref(), &mut lo, &mut hi)?;
            if has_lo && has_hi && lo >= hi {
                continue;
            }
            out.push(Group {
                tablet,
                start,
                end,
                lo: has_lo.then(|| lo.clone()),
                hi: has_hi.then(|| hi.clone()),
            });
        }
    }
    Ok(out)
}

/// Whether every row a range ending at `end` holds comes before every row a range starting
/// at `start` holds.
fn before(end: &Bound<Vec<u8>>, start: &Bound<Vec<u8>>) -> bool {
    match (end, start) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(e), Bound::Included(s)) => e < s,
        (Bound::Included(e) | Bound::Excluded(e), Bound::Excluded(s))
        | (Bound::Excluded(e), Bound::Included(s)) => e <= s,
    }
}

/// Entries the group's sources hold (SSTs it may read, memtables): an over-estimate after a
/// split, enough to decide whether fan-out pays.
fn estimate(view: &View, families: &[FamilyId], g: &Group<'_>) -> u64 {
    let mut n = 0;
    for &family in families {
        let located = view.locate(g.tablet.shard, g.tablet.id, family);
        if let Some(ssts) = located.ssts {
            n += ssts
                .levels
                .iter()
                .flatten()
                .map(|s| s.meta.entries)
                .sum::<u64>();
        }
        if let Some(mems) = located.mems {
            n += mems.readers.iter().map(|r| r.len() as u64).sum::<u64>();
        }
    }
    n
}

/// What a group's read needs from the call.
struct Ctx<'a> {
    snapshot: &'a Snapshot,
    families: &'a [FamilyId],
    read: &'a ReadSpec,
    now: Timestamp,
    abort: &'a AtomicBool,
}

/// Opens the group's lanes, one per family with data in the tablet's range, each seeked to
/// the group's start: [`super::ScanCursor`]'s tablet opening, for one tablet.
fn open_lanes(ctx: &Ctx<'_>, g: &Group<'_>, lanes: &mut Vec<Lane>) -> Result<()> {
    let view = &ctx.snapshot.view;
    for &family in ctx.families {
        let located = view.locate(g.tablet.shard, g.tablet.id, family);
        if !located.has_data() {
            continue;
        }
        let Some(meta) = view.catalog.family(family) else {
            continue;
        };
        let (mut opts, filter) = ctx.read.resolve_opts(meta, ctx.snapshot.seqno, ctx.now);
        let resolver_blobs = super::ResolverBlobs::attach(&mut opts, &view.ssts);
        let mut resolver = take_scan_resolver();
        let filled = view.scan_sources_located::<false>(
            &located,
            &filter,
            g.lo.as_deref(),
            g.hi.as_deref(),
            false,
            resolver.cursor_mut().sources_mut(),
        );
        if let Err(e) = filled {
            give_back(resolver);
            return Err(e);
        }
        if resolver.cursor_mut().sources().is_empty() {
            give_back(resolver);
            continue;
        }
        opts.skip_columns = skips_columns(resolver.cursor_mut().sources());
        resolver.cursor_mut().reset();
        resolver.reset(opts);
        resolver.set_upper_bound(g.hi.as_deref());
        lanes.push(Lane {
            family,
            unknown_merge: (meta.merge == MergeKind::Unknown)
                .then(|| meta.options.merge_operator.clone()),
            resolver,
            grew: false,
            ssts: Arc::clone(&view.ssts),
            resolver_blobs,
            ts: 0,
            pinned: None,
            pending: false,
            done: false,
        });
        let lane = lanes.last_mut().expect("pushed above");
        lane.resolver.seek(g.lo.as_deref().unwrap_or(&[]))?;
    }
    Ok(())
}

/// The family's position in the read's family order.
fn rank_of(families: &[FamilyId], family: FamilyId) -> u32 {
    families
        .iter()
        .position(|f| *f == family)
        .unwrap_or(usize::MAX) as u32
}

/// Whether the lane's pending cell is in the row whose escaped key is `row_esc`.
fn in_row(lane: &Lane, row_esc: &[u8]) -> bool {
    if !lane.pending {
        return false;
    }
    let col = lane.col();
    let row_len = row_esc.len() + TERMINATOR.len();
    col.len() >= row_len && col.starts_with(row_esc) && col[row_esc.len()..row_len] == TERMINATOR
}

/// Drops the rest of the row in every lane (a seek past it).
fn skip_row(lanes: &mut [Lane], row_esc: &[u8]) -> Result<()> {
    for lane in lanes {
        if in_row(lane, row_esc) {
            lane.pending = false;
            lane.resolver.skip_row()?;
        }
    }
    Ok(())
}

/// Reads one group into `reducer`.
fn run_group<V, R>(
    ctx: &Ctx<'_>,
    g: &Group<'_>,
    visitor: &mut V,
    reducer: &mut R,
    stats: &mut ReduceStats,
) -> Result<()>
where
    V: CellVisitor<Item = R::Item>,
    R: Reducer,
{
    ctx.snapshot.check_process()?;
    let mut lanes = Vec::new();
    let opened = open_lanes(ctx, g, &mut lanes);
    // Each lane's family position in the read's family order, once per group (perf287's
    // nit on #1209: not a search per cell).
    let ranks: Vec<u32> = lanes
        .iter()
        .map(|l| rank_of(ctx.families, l.family))
        .collect();
    let read =
        opened.and_then(|()| visit_lanes(ctx, g, &mut lanes, &ranks, visitor, reducer, stats));
    for lane in lanes {
        if !lane.grew {
            give_back(lane.resolver);
        }
    }
    stats.groups += 1;
    ctx.snapshot.checked(read)
}

fn visit_lanes<V, R>(
    ctx: &Ctx<'_>,
    g: &Group<'_>,
    lanes: &mut [Lane],
    ranks: &[u32],
    visitor: &mut V,
    reducer: &mut R,
    stats: &mut ReduceStats,
) -> Result<()>
where
    V: CellVisitor<Item = R::Item>,
    R: Reducer,
{
    let (mut row_esc, mut row, mut qualifier) = (Vec::new(), Vec::new(), Vec::new());
    loop {
        if ctx.abort.load(Ordering::Relaxed) {
            return Ok(());
        }
        for lane in lanes.iter_mut() {
            if !lane.pending && !lane.done {
                lane.fetch()?;
            }
        }
        // The smallest pending row across lanes.
        let Some(best) = lanes
            .iter()
            .filter(|l| l.pending)
            .map(|l| row_of(l.col()))
            .min()
        else {
            return Ok(());
        };
        row_esc.clear();
        row_esc.extend_from_slice(best);
        row.clear();
        Escaped::new(&row_esc).unescape_long_into(&mut row);
        if matches!(g.start, Bound::Excluded(s) if *s == row) {
            skip_row(lanes, &row_esc)?;
            continue;
        }
        let past = match g.end {
            Bound::Included(e) => row > *e,
            Bound::Excluded(e) => row >= *e,
            Bound::Unbounded => false,
        };
        if past {
            return Ok(());
        }
        stats.rows += 1;
        if stats.rows.is_multiple_of(FORK_CHECK_ROWS) {
            ctx.snapshot.check_process()?;
        }
        let row_len = row_esc.len() + TERMINATOR.len();
        let mut next_row = false;
        'lanes: for (lane, &family_rank) in lanes.iter_mut().zip(ranks) {
            while in_row(lane, &row_esc) {
                let col = lane.col();
                let end = col.len().saturating_sub(2).max(row_len);
                qualifier.clear();
                Escaped::new(&col[row_len..end]).unescape_into(&mut qualifier);
                stats.cells += 1;
                let visit = visitor.visit(&row, lane.family, &qualifier, lane.ts, lane.value());
                let cell = CellRef {
                    row: &row,
                    family_rank,
                    family: lane.family,
                    qualifier: &qualifier,
                    ts: lane.ts,
                };
                let (item, after) = match visit {
                    Visit::Skip => (None, None),
                    Visit::Emit(item) => (Some(item), None),
                    Visit::EmitAndNextRow(item) => (Some(item), Some(false)),
                    Visit::NextRow => (None, Some(false)),
                    Visit::Stop => return Ok(()),
                };
                if let Some(item) = item
                    && reducer.push(cell, item).is_break()
                {
                    return Ok(());
                }
                if after.is_some() {
                    next_row = true;
                    break 'lanes;
                }
                lane.fetch()?;
            }
        }
        if next_row {
            skip_row(lanes, &row_esc)?;
        }
    }
}

/// [`Engine::reduce`](crate::Engine::reduce): the groups, sequential or fanned out.
pub(crate) fn reduce<V, R>(
    snapshot: &Snapshot,
    table: TableId,
    families: &[FamilyId],
    spec: &ReduceSpec,
    (now, may_fan_out): (Timestamp, bool),
    visitor: &(impl Fn() -> V + Sync),
    reducer: &(impl Fn() -> R + Sync),
) -> Result<Reduced<R::Item>>
where
    V: CellVisitor<Item = R::Item>,
    R: Reducer,
{
    let view = Arc::clone(&snapshot.view);
    let groups = groups(&view, table, &spec.input)?;
    let abort = AtomicBool::new(false);
    let ctx = Ctx {
        snapshot,
        families,
        read: &spec.read,
        now,
        abort: &abort,
    };
    let threads = if may_fan_out && spec.parallelism > 1 && groups.len() > 1 {
        let by_work = match spec.parallel_floor {
            0 => usize::MAX,
            floor => {
                let total: u64 = groups.iter().map(|g| estimate(&view, families, g)).sum();
                usize::try_from(total / floor).unwrap_or(usize::MAX).max(1)
            }
        };
        spec.parallelism.min(groups.len()).min(by_work)
    } else {
        1
    };
    // One visitor and reducer per group: a group stops only on its own account.
    let run = |g: &Group<'_>, stats: &mut ReduceStats| -> Result<R> {
        let mut v = visitor();
        let mut r = reducer();
        run_group(&ctx, g, &mut v, &mut r, stats)?;
        Ok(r)
    };
    let mut stats = ReduceStats::default();
    let mut acc = reducer();
    if threads <= 1 {
        for g in &groups {
            acc.merge(run(g, &mut stats)?);
        }
        stats.threads = 1;
    } else {
        let next = AtomicUsize::new(0);
        let results: Vec<Result<(R, ReduceStats)>> = std::thread::scope(|s| {
            let workers: Vec<_> = (0..threads)
                .map(|_| {
                    s.spawn(|| -> Result<(R, ReduceStats)> {
                        let mut mine = reducer();
                        let mut st = ReduceStats::default();
                        loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            let Some(g) = groups.get(i) else {
                                return Ok((mine, st));
                            };
                            match run(g, &mut st) {
                                Ok(r) => mine.merge(r),
                                Err(e) => {
                                    abort.store(true, Ordering::Relaxed);
                                    return Err(e);
                                }
                            }
                        }
                    })
                })
                .collect();
            workers
                .into_iter()
                // A worker's panic is the caller's, as it would be on the caller's thread.
                .map(|w| w.join().unwrap_or_else(|p| std::panic::resume_unwind(p)))
                .collect()
        });
        for r in results {
            let (r, st) = r?;
            acc.merge(r);
            stats.add(&st);
        }
        stats.threads = threads as u64;
    }
    Ok(Reduced {
        items: acc.finish(),
        stats,
    })
}

/// The `k` items with the smallest scores, ties broken by cell order: a total order, so the
/// result is the same at any fan-out and in any push order (D296 R1).
#[derive(Debug, Clone)]
pub struct TopK<S> {
    k: usize,
    /// Sorted ascending by (score, cell); at most `k`.
    kept: Vec<(S, CellKey)>,
}

impl<S: Ord> TopK<S> {
    /// Keeps the `k` smallest scores.
    pub fn new(k: usize) -> Self {
        Self {
            k,
            kept: Vec::with_capacity(k.min(1024) + 1),
        }
    }

    /// Whether `(score, cell)` would enter.
    fn enters(&self, score: &S, cell: CellRef<'_>) -> bool {
        if self.k == 0 {
            return false;
        }
        match self.kept.last() {
            Some((ws, wc)) if self.kept.len() == self.k => (score, cell) < (ws, wc.as_ref()),
            _ => true,
        }
    }

    fn insert(&mut self, score: S, key: CellKey) {
        if !self.enters(&score, key.as_ref()) {
            return;
        }
        let at = self.kept.partition_point(|(s, k)| (s, k) < (&score, &key));
        self.kept.insert(at, (score, key));
        self.kept.truncate(self.k);
    }
}

impl<S: Ord + Send> Reducer for TopK<S> {
    type Item = S;

    fn push(&mut self, cell: CellRef<'_>, item: S) -> ControlFlow<()> {
        // Only a candidate that enters is copied.
        if self.enters(&item, cell) {
            self.insert(item, cell.to_key());
        }
        ControlFlow::Continue(())
    }

    fn merge(&mut self, other: Self) {
        for (s, key) in other.kept {
            self.insert(s, key);
        }
    }

    fn finish(self) -> Vec<(CellKey, S)> {
        self.kept.into_iter().map(|(s, k)| (k, s)).collect()
    }
}

/// The first `n` cells in scan order. A group pushes in that order and stops at its own n-th;
/// merging keeps the `n` smallest cells, which is the sequential first `n` (D296 rev 1.2).
#[derive(Debug, Clone)]
pub struct Limit<I> {
    n: usize,
    /// In cell order; at most `n`.
    kept: Vec<(CellKey, I)>,
}

impl<I> Limit<I> {
    /// Keeps the first `n`.
    pub fn new(n: usize) -> Self {
        Self {
            n,
            kept: Vec::new(),
        }
    }
}

impl<I: Send> Reducer for Limit<I> {
    type Item = I;

    fn push(&mut self, cell: CellRef<'_>, item: I) -> ControlFlow<()> {
        if self.kept.len() >= self.n {
            return ControlFlow::Break(());
        }
        // A group pushes in cell order, so this appends.
        debug_assert!(self.kept.last().is_none_or(|(k, _)| k.as_ref() < cell));
        self.kept.push((cell.to_key(), item));
        if self.kept.len() >= self.n {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    fn merge(&mut self, other: Self) {
        let mut all = std::mem::take(&mut self.kept);
        all.extend(other.kept);
        all.sort_by(|a, b| a.0.cmp(&b.0));
        all.truncate(self.n);
        self.kept = all;
    }

    fn finish(self) -> Vec<(CellKey, I)> {
        self.kept
    }
}

/// One item per key the item gives (for example a traversal's destination node), the one
/// from the first cell in scan order when a key repeats; keys in `exclude` are dropped
/// (#489's visited set). Order-independent, so the same at any fan-out (D296 S2).
pub struct Dedupe<I, K, F> {
    key_of: F,
    exclude: Option<Arc<std::collections::HashSet<K>>>,
    kept: std::collections::HashMap<K, (CellKey, I)>,
}

impl<I, K, F> std::fmt::Debug for Dedupe<I, K, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dedupe")
            .field("kept", &self.kept.len())
            .finish_non_exhaustive()
    }
}

impl<I, K: std::hash::Hash + Eq, F: Fn(&I) -> K> Dedupe<I, K, F> {
    /// One item per `key_of(item)`, none whose key is in `exclude`.
    pub fn new(key_of: F, exclude: Option<Arc<std::collections::HashSet<K>>>) -> Self {
        Self {
            key_of,
            exclude,
            kept: std::collections::HashMap::new(),
        }
    }

    fn offer(&mut self, cell: CellRef<'_>, item: I) {
        let key = (self.key_of)(&item);
        if self.exclude.as_ref().is_some_and(|x| x.contains(&key)) {
            return;
        }
        match self.kept.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                if cell < o.get().0.as_ref() {
                    o.insert((cell.to_key(), item));
                }
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert((cell.to_key(), item));
            }
        }
    }
}

impl<I, K, F> Reducer for Dedupe<I, K, F>
where
    I: Send,
    K: std::hash::Hash + Eq + Send + Sync,
    F: Fn(&I) -> K + Send,
{
    type Item = I;

    fn push(&mut self, cell: CellRef<'_>, item: I) -> ControlFlow<()> {
        self.offer(cell, item);
        ControlFlow::Continue(())
    }

    fn merge(&mut self, other: Self) {
        for (_, (cell, item)) in other.kept {
            self.offer(cell.as_ref(), item);
        }
    }

    fn finish(self) -> Vec<(CellKey, I)> {
        let mut out: Vec<_> = self.kept.into_values().collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(row: &'static str, rank: u32, q: &'static str, ts: Timestamp) -> CellRef<'static> {
        CellRef {
            row: row.as_bytes(),
            family_rank: rank,
            family: FamilyId(rank),
            qualifier: q.as_bytes(),
            ts,
        }
    }

    fn rows<I>(items: &[(CellKey, I)]) -> Vec<(&[u8], u32, &[u8], Timestamp)> {
        items
            .iter()
            .map(|(k, _)| {
                (
                    k.row.as_slice(),
                    k.family_rank,
                    k.qualifier.as_slice(),
                    k.ts,
                )
            })
            .collect()
    }

    #[test]
    fn cells_order_as_a_scan_does_family_before_qualifier_newest_version_first() {
        assert!(
            cell("r", 0, "z", 1) < cell("r", 1, "a", 1),
            "family rank before qualifier"
        );
        assert!(
            cell("r", 0, "q", 9) < cell("r", 0, "q", 3),
            "newest version first"
        );
        assert!(cell("a", 9, "z", 0) < cell("b", 0, "a", 9), "row first");
    }

    #[test]
    fn top_k_breaks_equal_scores_by_cell_order_in_any_merge_order() {
        // Equal scores across "groups": the first cells in scan order among the ties win,
        // including two versions of one column and the same qualifier in two families.
        let cells = [
            (cell("r3", 0, "a", 1), 1u32),
            (cell("r1", 1, "a", 1), 1),
            (cell("r1", 0, "b", 1), 1),
            (cell("r1", 0, "b", 5), 1),
            (cell("r2", 0, "a", 1), 1),
            (cell("r0", 0, "z", 1), 2),
        ];
        let mut seq = TopK::new(3);
        for (c, s) in cells {
            let _ = seq.push(c, s);
        }
        let want = seq.finish();
        assert_eq!(
            rows(&want),
            vec![
                (&b"r1"[..], 0, &b"b"[..], 5),
                (b"r1", 0, b"b", 1),
                (b"r1", 1, b"a", 1)
            ]
        );
        for split in 0..=cells.len() {
            let (a, b) = cells.split_at(split);
            let (mut x, mut y) = (TopK::new(3), TopK::new(3));
            for (c, s) in a {
                let _ = x.push(*c, *s);
            }
            for (c, s) in b.iter().rev() {
                let _ = y.push(*c, *s);
            }
            y.merge(x);
            assert_eq!(y.finish(), want, "split at {split}");
        }
    }

    #[test]
    fn limit_keeps_the_first_n_cells_in_scan_order_whichever_group_finishes_first() {
        // Within a row, family 0's "z" comes before family 1's "a" (ctr274's case).
        let mut early = Limit::new(2);
        assert!(early.push(cell("a", 0, "z", 1), 1).is_continue());
        assert!(early.push(cell("a", 1, "a", 1), 2).is_break());
        let mut late = Limit::new(2);
        let _ = late.push(cell("m", 0, "a", 1), 3);
        let _ = late.push(cell("n", 0, "a", 1), 4);
        late.merge(early);
        let got: Vec<i32> = late.finish().into_iter().map(|(_, i)| i).collect();
        assert_eq!(got, vec![1, 2]);
    }

    #[test]
    fn dedupe_keeps_the_first_cell_per_key_and_drops_excluded_keys() {
        fn key(i: &u32) -> u32 {
            *i
        }
        let exclude = Arc::new(std::collections::HashSet::from([9u32]));
        let mut a = Dedupe::new(key as fn(&u32) -> u32, Some(Arc::clone(&exclude)));
        let _ = a.push(cell("r2", 0, "q", 1), 7);
        let _ = a.push(cell("r1", 0, "q", 1), 9);
        let mut b = Dedupe::new(key as fn(&u32) -> u32, Some(exclude));
        let _ = b.push(cell("r1", 0, "q", 1), 7);
        a.merge(b);
        let got = a.finish();
        assert_eq!(got.len(), 1);
        assert_eq!((got[0].0.row.as_slice(), got[0].1), (&b"r1"[..], 7));
    }
}
