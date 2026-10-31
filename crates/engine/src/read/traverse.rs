//! Traversal pushdown (D304, #489 item 3): a breadth-first expansion over adjacency rows, one
//! bounded read per hop. A node is a row, its edges are the columns of one family, and an
//! edge's qualifier is the destination's row key.
//!
//! Each hop is [`reduce_into`] over a [`ReadInput::Keys`] input (the frontier, D296 PR 2 on
//! D299's multi-get) with two pieces:
//!
//! - [`EdgeVisitor`] emits each edge's source as its index in the frontier, and after
//!   `per_node_limit` edges of a row skips the rest of it (a seek, D296's `EmitAndNextRow`);
//! - [`DedupeLimit`] keeps each destination not yet visited once, with the smallest source
//!   index that reached it, and at most `hop_limit` of them: the first distinct destinations
//!   in scan order, which local stops keep exact under fan-out (D304 §2.3).
//!
//! The visited set is held in memory, about (key length + 48) bytes per node, and bounded by
//! `max_nodes`.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

use pigeonhole_format::{FamilyId, TableId, Timestamp};

use super::keys::{KeyCursor, KeySet};
use super::reduce::{CellKey, CellRef, CellVisitor, ReadInput, ReduceSpec, Reducer, Visit};
use super::{ReadSpec, ValuePredicate, reduce::reduce_into};
use crate::snapshot::Snapshot;
use crate::{Error, Result};

/// Most hops a traversal takes.
pub const MAX_HOPS: u32 = 64;

/// Nodes a traversal visits at most unless told otherwise (D304: about 0.6 GB of visited
/// set at 16-byte keys).
pub const DEFAULT_MAX_NODES: u64 = 10_000_000;

/// Keys per thread below which a hop does not fan out (D304: about 2 ms of rows at #489's
/// sparse cost, against 10-20 µs to start a thread).
pub const TRAVERSE_PARALLEL_FLOOR: u64 = 1_000;

/// What a traversal reads and how far.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TraverseSpec {
    /// The edge family.
    pub family: FamilyId,
    /// Hops, 1 to [`MAX_HOPS`].
    pub hops: u32,
    /// Edges read at most per node and hop (0: all).
    pub per_node_limit: u32,
    /// New nodes kept at most per hop (0: all).
    pub hop_limit: u64,
    /// Nodes visited at most in all, the seeds included.
    pub max_nodes: u64,
    /// The edges' selection: qualifiers, time range and value predicate (`families`,
    /// `versions` and `columns_per_row` are the traversal's own).
    pub read: ReadSpec,
    /// Threads a hop may use ([`ReduceSpec::parallelism`]).
    pub parallelism: usize,
    /// Keys per thread below which a hop does not fan out.
    pub parallel_floor: u64,
}

impl TraverseSpec {
    /// One hop over `family`, no limits but [`DEFAULT_MAX_NODES`], on the caller's thread.
    pub fn new(family: FamilyId) -> Self {
        Self {
            family,
            hops: 1,
            per_node_limit: 0,
            hop_limit: 0,
            max_nodes: DEFAULT_MAX_NODES,
            read: ReadSpec::default(),
            parallelism: 1,
            parallel_floor: TRAVERSE_PARALLEL_FLOOR,
        }
    }
}

/// One hop's new nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Hop {
    /// The nodes, sorted.
    pub nodes: KeySet,
    /// For each node, the index in the previous hop's nodes (the seeds for the first hop) of
    /// the source of the first edge that reached it.
    pub parents: Vec<u32>,
}

/// Counts of a traversal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TraverseStats {
    /// Frontier rows with an edge the selection keeps.
    pub rows: u64,
    /// Edges read.
    pub edges: u64,
    /// Hops that `hop_limit` or `max_nodes` cut short.
    pub hops_cut: u32,
    /// Whether `max_nodes` was reached with hops left to take: a hop cut short by it, or the
    /// remaining hops skipped (they may have found nothing new).
    pub cut_by_max_nodes: bool,
    /// The most threads a hop used (1 when every hop ran on the caller's thread).
    pub threads: u64,
}

/// A traversal's hops, in order; fewer than asked when a frontier ran empty or `max_nodes`
/// was reached.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Traversal {
    /// Hop 1 first.
    pub hops: Vec<Hop>,
    /// Counts.
    pub stats: TraverseStats,
}

/// Emits each edge's source (its index in the frontier), and ends a row after
/// `per_node_limit` edges.
struct EdgeVisitor {
    frontier: Arc<KeySet>,
    per_node_limit: u32,
    /// The current source's index, and edges read from it.
    src: Option<usize>,
    count: u32,
}

impl CellVisitor for EdgeVisitor {
    type Item = u32;

    fn visit(
        &mut self,
        row: &[u8],
        _family: FamilyId,
        _qualifier: &[u8],
        _ts: Timestamp,
        _value: &[u8],
    ) -> Visit<u32> {
        let src = match self.src {
            Some(i) if self.frontier.get(i) == row => i,
            _ => {
                // A new row: one search per row, never per edge.
                let i = index_of(&self.frontier, row);
                self.src = Some(i);
                self.count = 0;
                i
            }
        };
        self.count += 1;
        let item = src as u32;
        if self.per_node_limit != 0 && self.count >= self.per_node_limit {
            Visit::EmitAndNextRow(item)
        } else {
            Visit::Emit(item)
        }
    }
}

/// The index of `key` in `keys` (the reads come from the key set, so it is there).
fn index_of(keys: &KeySet, key: &[u8]) -> usize {
    let (mut lo, mut hi) = (0, keys.len());
    while lo < hi {
        let mid = (lo + hi) / 2;
        if keys.get(mid) < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    debug_assert!(
        lo < keys.len() && keys.get(lo) == key,
        "a read row is a frontier key"
    );
    lo
}

/// One entry per destination (the cell's qualifier) not in `exclude`, with the smallest
/// source index that reached it; at most `n`: the destinations whose first edge comes first
/// in scan order. A group stops at its `n + 1`-th distinct destination, which D304 §2.3
/// shows drops nothing the sequential result keeps; `merge` is the union with the smaller
/// source per destination, then the `n` smallest (source, destination), so it is
/// commutative and associative.
pub(crate) struct DedupeLimit {
    n: usize,
    exclude: Arc<HashSet<Vec<u8>>>,
    frontier: Arc<KeySet>,
    family: FamilyId,
    kept: HashMap<Vec<u8>, u32>,
    /// Some destination was left out for `n`.
    full: bool,
}

impl DedupeLimit {
    pub(crate) fn new(
        n: usize,
        exclude: Arc<HashSet<Vec<u8>>>,
        frontier: Arc<KeySet>,
        family: FamilyId,
    ) -> Self {
        Self {
            n,
            exclude,
            frontier,
            family,
            kept: HashMap::new(),
            full: false,
        }
    }

    /// Keeps the `n` smallest (source, destination) entries.
    fn truncate(&mut self) {
        if self.kept.len() <= self.n {
            return;
        }
        let mut all: Vec<(u32, Vec<u8>)> = self.kept.drain().map(|(k, s)| (s, k)).collect();
        all.sort_unstable();
        all.truncate(self.n);
        self.kept = all.into_iter().map(|(s, k)| (k, s)).collect();
        self.full = true;
    }

    /// The destinations, sorted, with their sources; and whether any was left out.
    pub(crate) fn into_nodes(mut self) -> (KeySet, Vec<u32>, bool) {
        self.truncate();
        let mut kept: Vec<(Vec<u8>, u32)> = self.kept.into_iter().collect();
        kept.sort_unstable();
        let nodes =
            KeySet::from_sorted(kept.iter().map(|(k, _)| k)).expect("distinct keys, sorted");
        (nodes, kept.into_iter().map(|(_, s)| s).collect(), self.full)
    }
}

impl Reducer for DedupeLimit {
    type Item = u32;

    fn push(&mut self, cell: CellRef<'_>, src: u32) -> ControlFlow<()> {
        let dest = cell.qualifier;
        if self.exclude.contains(dest) {
            return ControlFlow::Continue(());
        }
        if let Some(s) = self.kept.get_mut(dest) {
            // A group pushes in cell order, so this is never smaller; kept for any caller.
            *s = (*s).min(src);
            return ControlFlow::Continue(());
        }
        if self.kept.len() >= self.n {
            self.full = true;
            return ControlFlow::Break(());
        }
        self.kept.insert(dest.to_vec(), src);
        ControlFlow::Continue(())
    }

    fn merge(&mut self, other: Self) {
        self.full |= other.full;
        for (k, s) in other.kept {
            self.kept
                .entry(k)
                .and_modify(|x| *x = (*x).min(s))
                .or_insert(s);
        }
        self.truncate();
    }

    /// The kept destinations as cells: the source row and the destination qualifier. The
    /// version's timestamp is not kept (0). Traversal itself uses `into_nodes`.
    fn finish(mut self) -> Vec<(CellKey, u32)> {
        self.truncate();
        let mut out: Vec<(CellKey, u32)> = self
            .kept
            .into_iter()
            .map(|(q, s)| {
                (
                    CellKey {
                        row: self.frontier.get(s as usize).to_vec(),
                        family_rank: 0,
                        family: self.family,
                        qualifier: q,
                        ts: 0,
                    },
                    s,
                )
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}

/// Checks a traversal's arguments: its families (the edge family alone) and the edges' read.
fn checked(
    snapshot: &Snapshot,
    table: TableId,
    spec: &TraverseSpec,
) -> Result<(Vec<FamilyId>, ReadSpec)> {
    if !(1..=MAX_HOPS).contains(&spec.hops) {
        return Err(Error::InvalidArgument(format!(
            "a traversal takes 1 to {MAX_HOPS} hops, not {}",
            spec.hops
        )));
    }
    let families = crate::engine::families_in_order(&snapshot.view, table, &[spec.family])?;
    if families.as_slice() != [spec.family] {
        return Err(Error::InvalidArgument(format!(
            "family {} is not in table {}",
            spec.family.0, table.0
        )));
    }
    let mut read = spec.read.clone();
    read.families = vec![spec.family];
    read.versions = 1;
    read.columns_per_row = 0;
    Ok((families.into_vec(), read))
}

/// [`Engine::traverse`](crate::Engine::traverse).
pub(crate) fn traverse(
    snapshot: &Snapshot,
    table: TableId,
    seeds: Arc<KeySet>,
    spec: &TraverseSpec,
    ctx: (Timestamp, bool, &Arc<crate::shard::Shared>),
) -> Result<Traversal> {
    let (families, read) = checked(snapshot, table, spec)?;
    let mut out = Traversal::default();
    if seeds.is_empty() {
        return Ok(out);
    }
    let mut visited: Arc<HashSet<Vec<u8>>> = Arc::new(seeds.iter().map(<[u8]>::to_vec).collect());
    let mut frontier = seeds;
    for _ in 0..spec.hops {
        // Between hops, as a multi-get between rows (D304 §2.5): nothing is read after a
        // fork (#901) or once the snapshot is no longer current (a close or a recovery onto
        // a newer engine, D265, #826). The key cursors check it per key as well.
        snapshot.check_process()?;
        snapshot.check_current()?;
        let room = spec.max_nodes.saturating_sub(visited.len() as u64);
        if room == 0 {
            out.stats.cut_by_max_nodes = true;
            break;
        }
        let by_hop = if spec.hop_limit == 0 {
            u64::MAX
        } else {
            spec.hop_limit
        };
        let limit = usize::try_from(by_hop.min(room)).unwrap_or(usize::MAX);
        let mut rspec = ReduceSpec::new(ReadInput::Keys(Arc::clone(&frontier)));
        rspec.read = read.clone();
        rspec.parallelism = spec.parallelism.max(1);
        rspec.parallel_floor = spec.parallel_floor;
        let (acc, stats) = reduce_into(
            snapshot,
            table,
            &families,
            &rspec,
            ctx,
            &|| EdgeVisitor {
                frontier: Arc::clone(&frontier),
                per_node_limit: spec.per_node_limit,
                src: None,
                count: 0,
            },
            &|| {
                DedupeLimit::new(
                    limit,
                    Arc::clone(&visited),
                    Arc::clone(&frontier),
                    spec.family,
                )
            },
        )?;
        out.stats.rows += stats.rows;
        out.stats.edges += stats.cells;
        out.stats.threads = out.stats.threads.max(stats.threads);
        let (nodes, parents, full) = acc.into_nodes();
        if full {
            out.stats.hops_cut += 1;
            if room < by_hop {
                out.stats.cut_by_max_nodes = true;
            }
        }
        if nodes.is_empty() {
            break;
        }
        // The hop's readers (scoped threads, or this thread) have dropped their clones, so
        // the set grows in place; a clone kept alive would copy it every hop.
        debug_assert_eq!(Arc::strong_count(&visited), 1, "the visited set is shared");
        Arc::make_mut(&mut visited).extend(nodes.iter().map(<[u8]>::to_vec));
        frontier = Arc::new(nodes.clone());
        out.hops.push(Hop { nodes, parents });
    }
    Ok(out)
}

/// Cells an async traversal reads in one poll before it yields (D304 rev 1, A2): no poll
/// runs long, whatever a hub's degree. It counts every edge the read returns, the value
/// predicate's misses included (#1237), and the multi-get cursor yields after
/// [`POLL_ABSENT_KEYS`](crate::POLL_ABSENT_KEYS) keys without a row. Versions the read
/// resolves away (deleted, expired, outside the time range) are still stepped over inside
/// one cell's read, uncounted: a hub of mostly deleted edges (until compaction drops them)
/// can read more in one poll.
pub const TRAVERSE_POLL_CELLS: u32 = 4_096;

/// An async traversal ([`Engine::traverse_async`](crate::Engine::traverse_async), D304 PR
/// 2): the hops of [`Engine::traverse`](crate::Engine::traverse), each read on the caller's
/// task through one multi-get cursor over the frontier ([`KeyCursor::poll_next_key`]: keys
/// positioned cache-only, the next keys' blocks fetched ahead), with the same visitor and
/// reducer run on its borrowed cells. It never fans out, and D296's determinism makes its
/// result the sync one. A key's cells past the first are read synchronously where a block
/// misses (counted in `async_sync_reads`), and a poll yields after
/// [`TRAVERSE_POLL_CELLS`] cells, the value predicate's misses included (it is applied here,
/// not in the read), or after [`POLL_ABSENT_KEYS`](crate::POLL_ABSENT_KEYS) keys without a
/// row. Its state (the hop's reducer, the visitor's row and count,
/// the cursor) survives each `Pending`; dropping it releases the cursor's pins and the
/// snapshot.
#[must_use = "a future does nothing unless polled"]
pub struct PendingTraversal {
    snapshot: Snapshot,
    table: TableId,
    families: Vec<FamilyId>,
    read: ReadSpec,
    spec: TraverseSpec,
    now: Timestamp,
    shared: Arc<crate::shard::Shared>,
    visited: Arc<HashSet<Vec<u8>>>,
    frontier: Arc<KeySet>,
    hops_done: u32,
    out: Traversal,
    hop: Option<HopRead>,
    /// The edges' value predicate, applied to each cell before the visitor rather than in
    /// the read, so the edges it drops count toward a poll's budget (#1237), with the SSTs to
    /// read a separated value from.
    filter: Option<(ValuePredicate, Arc<crate::snapshot::SstSet>)>,
    /// The checks failed, or the result was handed out.
    failed: Option<Error>,
    done: bool,
}

impl std::fmt::Debug for PendingTraversal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingTraversal")
            .field("hops_done", &self.hops_done)
            .field("reading", &self.hop.is_some())
            .finish_non_exhaustive()
    }
}

/// One hop being read.
struct HopRead {
    cursor: KeyCursor,
    visitor: EdgeVisitor,
    reducer: DedupeLimit,
    /// The key whose cells are being read.
    key: Option<usize>,
    /// The hop's limit came from `max_nodes`.
    by_room: bool,
}

/// Where reading a key's cells stopped.
enum KeyEnd {
    /// The key's cells are done.
    Done,
    /// The poll's budget ran out mid-key.
    Budget,
    /// The reducer is full: the hop is done.
    Full,
}

impl PendingTraversal {
    pub(crate) fn new(
        snapshot: Snapshot,
        table: TableId,
        seeds: Arc<KeySet>,
        spec: TraverseSpec,
        now: Timestamp,
        shared: Arc<crate::shard::Shared>,
    ) -> Self {
        let (families, mut read, failed) = match checked(&snapshot, table, &spec) {
            Ok((f, r)) => (f, r, None),
            Err(e) => (Vec::new(), ReadSpec::default(), Some(e)),
        };
        // With one version per column, the read's predicate tests the version the visitor
        // sees (D22), so testing it here keeps the result.
        let filter = read
            .value
            .take()
            .map(|p| (p, Arc::clone(&snapshot.view.ssts)));
        let visited = Arc::new(seeds.iter().map(<[u8]>::to_vec).collect());
        Self {
            snapshot,
            table,
            families,
            read,
            hops_done: if seeds.is_empty() { spec.hops } else { 0 },
            spec,
            now,
            shared,
            visited,
            frontier: seeds,
            out: Traversal::default(),
            hop: None,
            filter,
            failed,
            done: false,
        }
    }

    /// Starts the next hop: `false` when the traversal is over.
    fn start_hop(&mut self) -> Result<bool> {
        if self.hops_done >= self.spec.hops {
            return Ok(false);
        }
        self.snapshot.check_process()?;
        self.snapshot.check_current()?;
        let room = self
            .spec
            .max_nodes
            .saturating_sub(self.visited.len() as u64);
        if room == 0 {
            self.out.stats.cut_by_max_nodes = true;
            return Ok(false);
        }
        let by_hop = if self.spec.hop_limit == 0 {
            u64::MAX
        } else {
            self.spec.hop_limit
        };
        let limit = usize::try_from(by_hop.min(room)).unwrap_or(usize::MAX);
        let cursor = KeyCursor::over(
            self.snapshot.clone(),
            self.table,
            self.families.clone(),
            self.read.clone(),
            self.now,
            Arc::clone(&self.frontier),
            Arc::clone(&self.shared),
        )?;
        // Every hop runs on the polling task.
        self.out.stats.threads = 1;
        self.hop = Some(HopRead {
            cursor,
            visitor: EdgeVisitor {
                frontier: Arc::clone(&self.frontier),
                per_node_limit: self.spec.per_node_limit,
                src: None,
                count: 0,
            },
            reducer: DedupeLimit::new(
                limit,
                Arc::clone(&self.visited),
                Arc::clone(&self.frontier),
                self.spec.family,
            ),
            key: None,
            by_room: room < by_hop,
        });
        Ok(true)
    }

    /// Ends the hop being read: its nodes join the visited set and become the frontier.
    /// `false` when it found none (the traversal is over).
    fn finish_hop(&mut self) -> bool {
        let hop = self.hop.take().expect("a hop is being read");
        let HopRead {
            cursor,
            visitor,
            reducer,
            by_room,
            ..
        } = hop;
        drop((cursor, visitor));
        let (nodes, parents, full) = reducer.into_nodes();
        self.hops_done += 1;
        if full {
            self.out.stats.hops_cut += 1;
            if by_room {
                self.out.stats.cut_by_max_nodes = true;
            }
        }
        if nodes.is_empty() {
            return false;
        }
        // The hop's reducer, the one other holder, was consumed above.
        debug_assert_eq!(
            Arc::strong_count(&self.visited),
            1,
            "the visited set is shared"
        );
        Arc::make_mut(&mut self.visited).extend(nodes.iter().map(<[u8]>::to_vec));
        self.frontier = Arc::new(nodes.clone());
        self.out.hops.push(Hop { nodes, parents });
        true
    }

    fn step(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<Traversal>> {
        use std::task::Poll;
        if let Some(e) = self.failed.take() {
            return Poll::Ready(Err(e));
        }
        let mut budget = TRAVERSE_POLL_CELLS;
        loop {
            if self.hop.is_none() {
                match self.start_hop() {
                    Ok(true) => {}
                    Ok(false) => return Poll::Ready(Ok(std::mem::take(&mut self.out))),
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }
            let frontier = Arc::clone(&self.frontier);
            let hop = self.hop.as_mut().expect("started above");
            let ended = match hop.key {
                Some(i) => {
                    let HopRead {
                        cursor,
                        visitor,
                        reducer,
                        ..
                    } = hop;
                    let row = frontier.get(i);
                    let before = budget;
                    let filter = self.filter.as_ref();
                    let ended =
                        cursor.counted(|c| read_key(c, row, visitor, reducer, filter, &mut budget));
                    self.out.stats.edges += u64::from(before - budget);
                    match ended {
                        Ok(KeyEnd::Done) => {
                            hop.key = None;
                            false
                        }
                        Ok(KeyEnd::Budget) => {
                            // Yield: the key's state is kept, and this task polled again.
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                        Ok(KeyEnd::Full) => true,
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                }
                None => match hop.cursor.poll_next_key(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(None)) => true,
                    Poll::Ready(Ok(Some(i))) => {
                        hop.key = Some(i);
                        self.out.stats.rows += 1;
                        false
                    }
                },
            };
            if ended && !self.finish_hop() {
                self.hops_done = self.spec.hops;
            }
        }
    }
}

/// Runs the visitor and reducer over the current key's cells, within `budget` cells: those
/// `filter` drops count too, and never reach the visitor (nor its per-node count).
fn read_key(
    c: &mut KeyCursor,
    row: &[u8],
    visitor: &mut EdgeVisitor,
    reducer: &mut DedupeLimit,
    filter: Option<&(ValuePredicate, Arc<crate::snapshot::SstSet>)>,
    budget: &mut u32,
) -> Result<KeyEnd> {
    loop {
        if *budget == 0 {
            return Ok(KeyEnd::Budget);
        }
        let Some(cell) = c.next_cell()? else {
            return Ok(KeyEnd::Done);
        };
        *budget -= 1;
        if let Some((p, ssts)) = filter {
            let kept = match pigeonhole_compaction::blob_pointer(cell.stored) {
                Some(ptr) => p.matches(&ssts.read_pointer(&ptr)?),
                None => p.matches(cell.stored),
            };
            if !kept {
                continue;
            }
        }
        let cref = CellRef {
            row,
            family_rank: 0,
            family: cell.family,
            qualifier: cell.qualifier,
            ts: cell.ts,
        };
        let (item, next_row) =
            match visitor.visit(row, cell.family, cell.qualifier, cell.ts, cell.stored) {
                Visit::Emit(src) => (Some(src), false),
                Visit::EmitAndNextRow(src) => (Some(src), true),
                Visit::NextRow => (None, true),
                Visit::Skip => (None, false),
                Visit::Stop => return Ok(KeyEnd::Full),
            };
        if let Some(src) = item
            && reducer.push(cref, src).is_break()
        {
            return Ok(KeyEnd::Full);
        }
        if next_row {
            c.skip_rest_of_key();
            return Ok(KeyEnd::Done);
        }
    }
}

impl std::future::Future for PendingTraversal {
    type Output = Result<Traversal>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let _poll = pigeonhole_io::async_poll_scope();
        let this = self.get_mut();
        if this.done {
            return std::task::Poll::Ready(Err(Error::InvalidArgument(
                "a traversal polled after it resolved".to_owned(),
            )));
        }
        let r = this.step(cx);
        if r.is_ready() {
            this.done = true;
            this.hop = None;
        }
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deterministic xorshift.
    struct Rng(u64);

    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    fn cell<'a>(row: &'a [u8], q: &'a [u8]) -> CellRef<'a> {
        CellRef {
            row,
            family_rank: 0,
            family: FamilyId(1),
            qualifier: q,
            ts: 1,
        }
    }

    /// The sequential result: the first `n` distinct non-excluded destinations in
    /// (source, destination) order, with their first source.
    fn reference(
        edges: &[(u32, Vec<u8>)],
        n: usize,
        exclude: &HashSet<Vec<u8>>,
    ) -> Vec<(Vec<u8>, u32)> {
        let mut seen: Vec<(Vec<u8>, u32)> = Vec::new();
        for (s, d) in edges {
            if exclude.contains(d) || seen.iter().any(|(k, _)| k == d) {
                continue;
            }
            if seen.len() == n {
                break;
            }
            seen.push((d.clone(), *s));
        }
        seen.sort();
        seen
    }

    /// D304 §2.3 and rev 2: any cut of the edges into groups (in order), each pushed with
    /// its local stop and merged in any order, equals the sequential result.
    #[test]
    fn dedupe_limit_merges_to_the_sequential_result_in_any_order() {
        let seed = 0x5EED_D304;
        let mut rng = Rng(seed);
        for case in 0..300 {
            let sources = 1 + rng.below(12) as u32;
            let frontier = Arc::new(KeySet::from_unsorted(
                (0..sources).map(|s| vec![b's', s as u8]),
            ));
            // Edges in scan order: by source, then destination (distinct per source).
            let mut edges = Vec::new();
            for s in 0..sources {
                let mut dests: Vec<Vec<u8>> = (0..rng.below(6))
                    .map(|_| vec![b'd', rng.below(10) as u8])
                    .collect();
                dests.sort();
                dests.dedup();
                edges.extend(dests.into_iter().map(|d| (s, d)));
            }
            let exclude: HashSet<Vec<u8>> = (0..rng.below(3))
                .map(|_| vec![b'd', rng.below(10) as u8])
                .collect();
            let exclude = Arc::new(exclude);
            let n = 1 + rng.below(8) as usize;
            let want = reference(&edges, n, &exclude);
            // Cut at source boundaries (groups are tablets: whole rows).
            let mut cuts: Vec<u32> = (1..sources).filter(|_| rng.below(2) == 0).collect();
            cuts.insert(0, 0);
            cuts.push(sources);
            let mut parts: Vec<DedupeLimit> = cuts
                .windows(2)
                .map(|w| {
                    let mut r = DedupeLimit::new(
                        n,
                        Arc::clone(&exclude),
                        Arc::clone(&frontier),
                        FamilyId(1),
                    );
                    for (s, d) in edges.iter().filter(|(s, _)| (w[0]..w[1]).contains(s)) {
                        let row = frontier.get(*s as usize).to_vec();
                        if r.push(cell(&row, d), *s).is_break() {
                            break;
                        }
                    }
                    r
                })
                .collect();
            // Merge in a random order.
            let mut acc = parts.swap_remove(rng.below(parts.len() as u64) as usize);
            while !parts.is_empty() {
                let next = parts.swap_remove(rng.below(parts.len() as u64) as usize);
                if rng.below(2) == 0 {
                    acc.merge(next);
                } else {
                    let mut other = next;
                    other.merge(acc);
                    acc = other;
                }
            }
            let (nodes, parents, _) = acc.into_nodes();
            let got: Vec<(Vec<u8>, u32)> = nodes.iter().map(<[u8]>::to_vec).zip(parents).collect();
            assert_eq!(got, want, "seed {seed:#x}, case {case}");
        }
    }

    #[test]
    fn the_edge_visitor_caps_each_row() {
        let frontier = Arc::new(KeySet::from_unsorted([&b"a"[..], b"b"]));
        let mut v = EdgeVisitor {
            frontier,
            per_node_limit: 2,
            src: None,
            count: 0,
        };
        let f = FamilyId(1);
        assert_eq!(v.visit(b"a", f, b"x", 1, b""), Visit::Emit(0));
        assert_eq!(v.visit(b"a", f, b"y", 1, b""), Visit::EmitAndNextRow(0));
        assert_eq!(v.visit(b"b", f, b"x", 1, b""), Visit::Emit(1));
    }
}
