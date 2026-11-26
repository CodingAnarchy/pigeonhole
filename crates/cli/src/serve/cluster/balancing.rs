//! D332 PR 3: the balancer's loop on the map's writer, around the pure policy
//! ([`super::balance`]).
//!
//! One of the writer's duties ([`Node::maybe_balance`]): a pass every [`PASS`], only while
//! the map's balancer is ON or PAUSED and this node is the writer holding its lease, at any
//! term (simp724's Amendment 2: every map change the balancer makes checks the lease again).
//! A pass:
//! 1. **Finds an earlier writer's balancer move** (a `bal-` NODE record) with no task of
//!    this writer's running it. Before its commit point it's aborted: the decision was an
//!    older reign's, on older samples, and that writer may still be copying (simp724's M1).
//!    The abort removes the record, sends the target's copy aside, unfences the partition
//!    on its node and records an ABORT event. From its commit point the move is finished.
//!    A `bal-` range move or split is the same through D331 PR 5's driver: aborted before
//!    HANDED_OVER (a split's new partition removed once empty), an ABORTING one's abort
//!    carried on, and finished from HANDED_OVER (`range_move::resume`).
//! 2. **Waits** while any move is in flight (one action at a time), or while PAUSED.
//! 3. **Surveys** every node's `Status` with `open_only` (perf287), and runs the policy.
//! 4. **Acts:** a whole move runs on a task of its own through `phdb cluster move`'s steps
//!    (`moving::run_as`, its record `bal-<ms>`), with the writer's cluster token (#2094). A
//!    pin or an unpin is a map change. A split runs the same way through D331 PR 5's split
//!    driver (`range_move::split_as`, a `bal-` record), which names the new partition
//!    `<P>-<v>`. A merge runs through D331 PR 6's merge (`range_merge::run_as`, each range
//!    a `bal-` record), with no merge record of its own: one interrupted part-way leaves a
//!    valid layout, and a later pass decides afresh.
//! 5. **Removes an emptied source:** a partition the balancer merged away (its last history
//!    entry MERGE) that holds nothing but is still listed, through the merge's removal, each
//!    pass until nothing waits for it (`drop_waits`). The removal reads those before it
//!    fences, so a pass while S waits costs Status calls only. A paused balancer does it
//!    too. While S waits, or its removal stopped (named in `state`), the pass goes on
//!    without it.
//!
//! A pass that does nothing changes nothing in the map: the current skip is in this
//! node's `NodeStatus.balancer` only (simp724's M2, perf287's should-fix 1). The counters
//! are in memory and start again with each writer term (S3).

use std::collections::HashMap;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use super::balance::{self, Action, Bounds, Counters, Decision, Kind, NodeView, PartView};
use super::proto::{
    self, BalancerEvent, BalancerEventKind, BalancerMode, ClusterMap, LocalPartitionState,
    MoveHistory, NodeStatus, PartitionRequest, PartitionRole, SetMapRequest,
};
use super::{Node, map, schema};

/// How often the balancer's pass runs.
pub(crate) const PASS: Duration = Duration::from_secs(30);

/// A test's shorter pass, by the node's cluster directory.
#[cfg(test)]
pub(crate) static PASS_FOR: std::sync::Mutex<Vec<(std::path::PathBuf, Duration)>> =
    std::sync::Mutex::new(Vec::new());

/// A fault seam (tests): the next unfence the balancer on the node in the listed cluster
/// directory sends after an abort fails, once per entry, as a node that doesn't answer
/// would fail it (#2151).
#[cfg(test)]
pub(crate) static FAIL_UNFENCE: std::sync::Mutex<Vec<std::path::PathBuf>> =
    std::sync::Mutex::new(Vec::new());

/// A partition an aborted balancer move left fenced and closed on its node, whose unfence
/// failed (#2151): each pass tries again, and `state` names it.
///
/// It lifts only the fence that move set (ai1249's S1 on #2170), so it's kept only while
/// that is certainly the fence in place: the partition on the node and at the epoch it had
/// at the abort, no move naming it ([`Unfence::stands`]), and this node the writer since,
/// in the abort's term or the one right after it ([`Unfence::carries`]).
#[derive(Debug, Clone)]
struct Unfence {
    partition: String,
    move_id: String,
    error: String,
    /// The partition's node and epoch at the abort.
    node: String,
    epoch: u64,
    /// The writer term of the abort.
    term: u64,
}

impl Unfence {
    /// Whether `map` still has the partition as the abort left it: on the same node at the
    /// same epoch, with no move naming it. A relocation, a failover, a promote or a forced
    /// map changes one of the two without a move record, and whoever made that change, or a
    /// move that names the partition now, owns its fence.
    fn stands(&self, map: &ClusterMap) -> bool {
        map.partitions
            .iter()
            .any(|p| p.id == self.partition && p.node == self.node && p.epoch == self.epoch)
            && !map
                .moves
                .iter()
                .any(|m| m.from == self.partition || (!map::node_move(m) && m.to == self.partition))
    }

    /// Whether it carries into this node's new writer term `term`: only the term right
    /// after the abort's. Terms are one writer's each, so no other node was the writer in
    /// between; past that, another writer (or an operator, under it) may have set the fence.
    fn carries(&self, term: u64) -> bool {
        self.term.checked_add(1) == Some(term)
    }
}

/// The writer's balancer state: in memory, new with each writer term (S3).
#[derive(Debug, Default)]
pub(crate) struct Run {
    term: Option<u64>,
    counters: Counters,
    /// When the last pass ran, on the lease clock.
    last_pass: Option<u64>,
    /// `NodeStatus.balancer`.
    state: String,
    /// The move this writer runs.
    moving: Option<tokio::task::JoinHandle<()>>,
    /// Actions taken, by kind (`pigeonhole_cluster_balancer_actions_total`).
    actions: HashMap<&'static str, u64>,
    /// Each node's load per unit of weight over the mean, at the last pass.
    ratios: Vec<(String, f64)>,
    /// A pass is running ([`Node::spawn_balance`]).
    passing: bool,
    /// Split candidates set aside, until when (ms), and why: a survey found no cut, the
    /// owner didn't answer it, or the split couldn't start (perf2's and ai1249's S1 on
    /// #2137). `state` names them.
    no_cut: HashMap<String, (u64, String)>,
    /// Partitions still to unfence after an abort (#2151). Carried only into this node's
    /// own next term ([`Unfence::carries`]).
    unfence: Vec<Unfence>,
}

fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn kind_of(k: BalancerEventKind) -> Option<Kind> {
    match k {
        BalancerEventKind::Move => Some(Kind::Move),
        BalancerEventKind::Split => Some(Kind::Split),
        BalancerEventKind::Merge => Some(Kind::Merge),
        BalancerEventKind::Pin => Some(Kind::Pin),
        _ => None,
    }
}

/// The map's bounds, each 0 taking its default.
pub(crate) fn bounds_of(map: &ClusterMap) -> Bounds {
    let d = Bounds::default();
    let Some(b) = &map.balance else { return d };
    let pick_f = |v: f64, d: f64| if v == 0.0 { d } else { v };
    let pick_u = |v: u64, d: u64| if v == 0 { d } else { v };
    let max = pick_u(b.partition_max_bytes, d.partition_max_bytes);
    Bounds {
        size_ratio: pick_f(b.size_ratio, d.size_ratio),
        load_ratio: pick_f(b.load_ratio, d.load_ratio),
        partition_max_bytes: max,
        partition_max_load: pick_f(b.partition_max_load, d.partition_max_load),
        partition_min_bytes: pick_u(b.partition_min_bytes, max / 8),
        passes: if b.passes == 0 { d.passes } else { b.passes },
        dwell_ms: pick_u(b.dwell_ms, d.dwell_ms),
        max_moves: if b.max_moves == 0 {
            d.max_moves
        } else {
            b.max_moves
        },
        bounce_window_ms: pick_u(b.bounce_window_ms, d.bounce_window_ms),
    }
}

/// The policy's view of the cluster, from the map and the pass's `statuses` (by node id;
/// a node missing didn't answer). `in_doubt` says whether a partition has in-doubt shares.
pub(crate) fn views(
    map: &ClusterMap,
    statuses: &HashMap<String, NodeStatus>,
    in_doubt: impl Fn(&str) -> bool,
) -> (Vec<NodeView>, Vec<PartView>) {
    let mut nodes: Vec<NodeView> = map
        .nodes
        .iter()
        .map(|n| NodeView {
            id: n.id.clone(),
            weight: if n.weight == 0 {
                1.0
            } else {
                f64::from(n.weight)
            },
            fresh: statuses.contains_key(&n.id),
            incomplete: None,
        })
        .collect();
    let parts = map
        .partitions
        .iter()
        .map(|p| {
            let lp = statuses.get(&p.node).and_then(|st| {
                st.partitions
                    .iter()
                    .find(|lp| lp.id == p.id && lp.role() != PartitionRole::Replica)
            });
            // Status measures only a partition open there, serving or fenced (#2158): any other
            // leaves its node's numbers incomplete, so the node isn't fresh this pass.
            if statuses.contains_key(&p.node) {
                let why = match lp.map(|lp| lp.state()) {
                    Some(LocalPartitionState::Serving | LocalPartitionState::Fenced) => None,
                    Some(LocalPartitionState::Closed) => Some("is closed"),
                    Some(LocalPartitionState::Opening) => Some("is opening"),
                    _ => Some("isn't open"),
                };
                if let Some(why) = why
                    && let Some(n) = nodes.iter_mut().find(|n| n.id == p.node && n.fresh)
                {
                    n.fresh = false;
                    n.incomplete = Some(format!(
                        "node {}'s numbers are incomplete: partition {} {why} there",
                        n.id, p.id
                    ));
                }
            }
            let tables: Vec<&proto::TablePlacement> = map
                .tables
                .iter()
                .filter(|t| t.partitions.contains(&p.id))
                .collect();
            let whole_only = tables
                .iter()
                .any(|t| !t.group.is_empty() || !map::followed_by(map, &t.name).is_empty());
            let mut neighbours = Vec::new();
            for t in &tables {
                for (i, q) in t.partitions.iter().enumerate() {
                    if *q != p.id {
                        continue;
                    }
                    for j in [i.checked_sub(1), Some(i + 1)].into_iter().flatten() {
                        if let Some(n) = t.partitions.get(j)
                            && *n != p.id
                            && !neighbours.contains(n)
                        {
                            neighbours.push(n.clone());
                        }
                    }
                }
            }
            // Its own ranges, and the pins that overlap them (ai1249's M1, M2 on #2103).
            let mut ranges = Vec::new();
            let mut pinned_ranges = Vec::new();
            for t in &tables {
                let own: Vec<balance::KeySpan> = map::table_ranges(t)
                    .into_iter()
                    .filter(|(_, _, q)| *q == p.id)
                    .map(|(start, end, _)| balance::KeySpan {
                        table: t.name.clone(),
                        start,
                        end,
                    })
                    .collect();
                for pin in &t.pinned_ranges {
                    let key = |k: &Option<proto::SplitKey>| {
                        k.as_ref()
                            .map(|k| map::split_key_bytes(k).unwrap_or_default())
                    };
                    let (start, end) = (key(&pin.start).unwrap_or_default(), key(&pin.end));
                    let overlaps = own.iter().any(|r| {
                        end.as_ref().is_none_or(|e| r.start < *e)
                            && r.end.as_ref().is_none_or(|e| start < *e)
                    });
                    if overlaps {
                        pinned_ranges.push(balance::KeySpan {
                            table: t.name.clone(),
                            start,
                            end,
                        });
                    }
                }
                ranges.extend(own);
            }
            PartView {
                id: p.id.clone(),
                node: p.node.clone(),
                bytes: lp.map_or(0, |lp| lp.file_bytes),
                load: lp.map_or(0.0, |lp| lp.load),
                replicas: p.replicas.clone(),
                pinned: p.pinned,
                history: p
                    .moves_history
                    .iter()
                    .filter_map(|h| kind_of(h.kind()).map(|k| (k, h.at_ms)))
                    .collect(),
                busy: map.moves.iter().any(|m| m.from == p.id || m.to == p.id) || in_doubt(&p.id),
                whole_only,
                tablets: Vec::new(),
                neighbours,
                ranges,
                pinned_ranges,
            }
        })
        .collect();
    (nodes, parts)
}

/// D332: partition `partition`'s tablets, answered by its owner for the map's writer
/// (`Forward`'s `tablets` arm, cluster scope): the engine's `tablets()` and the partition's
/// sampled read keys counted per tablet, with its epoch, so the writer drops an answer from
/// an owner since replaced.
pub(crate) async fn serve_tablets(
    cluster: &crate::serve::router::Cluster,
    partition: &str,
) -> Result<proto::TabletsReply, crate::serve::errors::ApiError> {
    use crate::serve::errors::ApiError;
    let map = cluster.node.get_map()?;
    let Some(p) = map.partitions.iter().find(|p| p.id == partition) else {
        return Err(ApiError::wrong_partition(
            map.map_version,
            format!("no partition {partition:?} in the map"),
        ));
    };
    let Some(service) = cluster
        .here(partition)
        .filter(|_| p.node == cluster.node.id())
    else {
        return Err(ApiError::wrong_partition(
            map.map_version,
            format!("partition {partition:?} isn't served here"),
        ));
    };
    let part = service.part()?;
    let (db, load, epoch) = (part.db.clone(), Arc::clone(&part.load), p.epoch);
    // In memory: a walk of every tablet's SST metadata, with no file reads (perf2's N4 on
    // #2137); off the async workers all the same (perf287).
    let (tablets, (keys, each)) =
        tokio::task::spawn_blocking(move || (db.tablets(), load.read_keys()))
            .await
            .map_err(|e| ApiError::internal(format!("a partition's tablets: {e}")))?;
    // Each sampled key to its tablet, by a binary search over the tablets in (table, start)
    // order (perf2's N3 on #2137).
    let mut order: Vec<usize> = (0..tablets.len()).collect();
    order.sort_by(|&x, &y| {
        (tablets[x].table.as_str(), &tablets[x].start[..])
            .cmp(&(tablets[y].table.as_str(), &tablets[y].start[..]))
    });
    let mut reads = vec![0u64; tablets.len()];
    for k in &keys {
        let at = order.partition_point(|&j| {
            (tablets[j].table.as_str(), &tablets[j].start[..]) <= (k.table.as_str(), &k.key[..])
        });
        let Some(&j) = at.checked_sub(1).and_then(|i| order.get(i)) else {
            continue;
        };
        let t = &tablets[j];
        if t.table == k.table && t.end.as_deref().is_none_or(|e| k.key[..] < *e) {
            reads[j] += 1;
        }
    }
    Ok(proto::TabletsReply {
        tablets: tablets
            .into_iter()
            .zip(reads)
            .map(|(t, reads)| proto::TabletInfo {
                table: t.table,
                start: t.start.into(),
                end: t.end.unwrap_or_default().into(),
                bytes: t.bytes,
                write_load: t.write_load,
                reads,
            })
            .collect(),
        read_each: each,
        epoch,
    })
}

impl Node {
    /// The policy's tablets for split candidate `p` (D332 PR 5's design §1): asked of its
    /// owner, bounded by the push timeout. `None` when the owner can't answer, or answers
    /// at another epoch than the map's (a stale owner).
    async fn tablets_of(&self, map: &ClusterMap, p: &PartView) -> Option<Vec<balance::Tablet>> {
        let shared = self.server.get().and_then(|o| o.shared.upgrade())?;
        let cluster = shared.router.cluster.as_ref()?;
        let epoch = map.partitions.iter().find(|q| q.id == p.id)?.epoch;
        let reply = if p.node == self.id {
            serve_tablets(cluster, &p.id).await.ok()?
        } else {
            let address = &map.nodes.iter().find(|n| n.id == p.node)?.address;
            let fwd = proto::ForwardRequest {
                map_version: map.map_version,
                caller: Some(
                    super::forward::forwarded_caller(&crate::serve::auth::Caller::anonymous())
                        .ok()?,
                ),
                partition: p.id.clone(),
                op: Some(proto::forward_request::Op::Tablets(proto::TabletsRequest {
                    partition: p.id.clone(),
                })),
                ..Default::default()
            };
            match cluster
                .forwarder
                .send(address, fwd, self.peers.push_timeout, false)
                .await
                .ok()?
                .reply
            {
                Some(proto::forward_response::Reply::Tablets(r)) => r,
                _ => return None,
            }
        };
        if reply.epoch != epoch {
            return None;
        }
        let report: Vec<balance::TabletReport> = reply
            .tablets
            .into_iter()
            .map(|t| balance::TabletReport {
                table: t.table,
                start: t.start.to_vec(),
                bytes: t.bytes,
                write_load: t.write_load,
                reads: t.reads,
            })
            .collect();
        Some(balance::tablet_loads(&report, p.load))
    }
}

/// `m` with `e` appended to its events, the oldest dropped past [`map::BALANCER_EVENTS`].
fn push_event(m: &mut ClusterMap, e: BalancerEvent) {
    m.balancer_events.push(e);
    let over = m.balancer_events.len().saturating_sub(map::BALANCER_EVENTS);
    m.balancer_events.drain(..over);
}

/// The partitions the balancer merged away that the map still lists while they hold
/// nothing: each one's last history entry is MERGE (only a balancer merge's source gets
/// one), and nothing places it. An operator's merge, or a partition never given a range,
/// isn't the balancer's to remove.
fn emptied(m: &ClusterMap) -> Vec<String> {
    m.partitions
        .iter()
        .filter(|p| {
            !map::placed(m, &p.id)
                && p.moves_history
                    .last()
                    .is_some_and(|h| h.kind() == BalancerEventKind::Merge)
        })
        .map(|p| p.id.clone())
        .collect()
}

/// What `state` says of a partition an aborted move left fenced (#2151).
/// `st`, then each of `more` that says something, after a `; `.
fn with_waiting(mut st: String, more: &[String]) -> String {
    for m in more.iter().filter(|m| !m.is_empty()) {
        st.push_str("; ");
        st.push_str(m);
    }
    st
}

/// #2240: the node of each partition an abort left fenced, whose unfence hasn't succeeded
/// yet ([`Node::retry_unfences`]), sits out the pass. Not fresh, it's neither a source nor
/// a target, and its numbers stay out of the means: they leave out or misstate the fenced
/// partition. A fenced partition answering in its node's Status counts as measured in
/// [`views`], so this doesn't rest on that. The rest of the cluster is balanced meanwhile
/// (#2170 decided nothing at all).
fn hold_back(nodes: &mut [NodeView], held: &[Unfence]) {
    for u in held {
        if let Some(n) = nodes.iter_mut().find(|n| n.id == u.node) {
            n.fresh = false;
            n.incomplete = Some(format!(
                "node {} sits out the balancer's passes while partition {} waits for its \
                 unfence after aborted move {}",
                n.id, u.partition, u.move_id
            ));
        }
    }
}

fn still_fenced(partition: &str, move_id: &str, error: &str) -> String {
    format!(
        "{partition} is still fenced and closed after aborted move {move_id} ({error}): the \
         balancer tries again each pass, or run phdb cluster unfence {partition} on its node"
    )
}

/// What an abort's state line adds when its ABORT event's map change failed (audit120's
/// nit on #2137): the abort happened, the event wasn't recorded.
fn unrecorded(recorded: Result<(), String>) -> String {
    recorded
        .err()
        .map(|e| format!(" (its ABORT event wasn't recorded: {e})"))
        .unwrap_or_default()
}

/// `p`'s history in `m` with `kind` at `at` appended, the oldest dropped past
/// [`map::MOVES_HISTORY`].
fn push_history(m: &mut ClusterMap, p: &str, kind: BalancerEventKind, at: u64) {
    for q in m.partitions.iter_mut().filter(|q| q.id == p) {
        let mut h = MoveHistory {
            at_ms: at,
            ..MoveHistory::default()
        };
        h.set_kind(kind);
        q.moves_history.push(h);
        let over = q.moves_history.len().saturating_sub(map::MOVES_HISTORY);
        q.moves_history.drain(..over);
    }
}

fn event(kind: BalancerEventKind, partition: &str, reason: &str, at: u64) -> BalancerEvent {
    let mut e = BalancerEvent {
        partition: partition.to_owned(),
        reason: reason.to_owned(),
        at_ms: at,
        ..BalancerEvent::default()
    };
    e.set_kind(kind);
    e
}

impl Node {
    fn balancing(&self) -> std::sync::MutexGuard<'_, Run> {
        self.balancing
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// `NodeStatus.balancer`.
    pub(crate) fn balancer_state(&self) -> String {
        self.balancing().state.clone()
    }

    fn set_balancer_state(&self, s: String) {
        self.balancing().state = s;
    }

    /// The pass's length here (a test's own, or [`PASS`]).
    fn pass(&self) -> Duration {
        #[cfg(test)]
        if let Some((_, d)) = PASS_FOR
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|(dir, _)| *dir == self.dir)
        {
            return *d;
        }
        PASS
    }

    /// The balancer's pass, if due (the module doc).
    /// Runs a balancer pass on a task of its own, unless one is running: its calls to other
    /// nodes (an abort, a pin, a survey) can wait on a node that's down, and the writer's
    /// other duties, failover among them, mustn't wait on them (perf287's question on
    /// #2103).
    pub(crate) fn spawn_balance(self: &Arc<Self>) {
        {
            let mut r = self.balancing();
            if r.passing {
                return;
            }
            r.passing = true;
        }
        /// Clears `passing` however the pass ends, a panic included.
        struct Done(std::sync::Weak<Node>);
        impl Drop for Done {
            fn drop(&mut self) {
                if let Some(node) = self.0.upgrade() {
                    node.balancing().passing = false;
                }
            }
        }
        let done = Done(Arc::downgrade(self));
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return;
        };
        rt.spawn(async move {
            if let Some(node) = done.0.upgrade() {
                node.maybe_balance().await;
            }
            drop(done);
        });
    }

    pub(crate) async fn maybe_balance(self: &Arc<Self>) {
        let Ok(map) = self.get_map() else { return };
        let mode = map.balancer();
        if schema::writer(&map) != self.id
            || matches!(mode, BalancerMode::Unspecified | BalancerMode::Off)
        {
            self.set_balancer_state(String::new());
            return;
        }
        if self.writer_lease_remaining(&map).is_zero() {
            // A balancer move left mid-way (a hand-over's range refuses writes) is named
            // (audit120's nit on #2137).
            let left = map
                .moves
                .iter()
                .find(|m| m.id.starts_with("bal-"))
                .map(|m| {
                    format!(
                        "; balancer move {} of {} ({:?}) stays recorded until a writer holds it",
                        m.id,
                        m.from,
                        map::phase(m)
                    )
                })
                .unwrap_or_default();
            self.set_balancer_state(format!("waiting: this writer holds no writer lease{left}"));
            return;
        }
        let now = self.grants.clock().now();
        let pass = u64::try_from(self.pass().as_nanos()).unwrap_or(u64::MAX);
        {
            let mut r = self.balancing();
            if r.term != Some(map.writer_term) {
                // A new term's counters; the move in flight, the running pass and the
                // metric's totals carry over.
                let (unfence, dropped): (Vec<Unfence>, Vec<Unfence>) =
                    std::mem::take(&mut r.unfence)
                        .into_iter()
                        .partition(|u| u.carries(map.writer_term));
                for u in dropped {
                    eprintln!(
                        "phdb serve: the balancer no longer retries unfencing {} after \
                         aborted move {} (the writer term went from {} to {}): if it's still \
                         closed on node {}, run phdb cluster unfence {} there",
                        u.partition, u.move_id, u.term, map.writer_term, u.node, u.partition
                    );
                }
                *r = Run {
                    term: Some(map.writer_term),
                    moving: r.moving.take(),
                    actions: std::mem::take(&mut r.actions),
                    passing: r.passing,
                    unfence,
                    ..Run::default()
                };
            }
            if r.moving.as_ref().is_some_and(|h| !h.is_finished()) {
                return;
            }
            r.moving = None;
            if r.last_pass.is_some_and(|t| now < t.saturating_add(pass)) {
                return;
            }
            r.last_pass = Some(now);
        }
        // A partition an abort left fenced (#2151): each pass tries its unfence again. Until
        // it serves, its node sits out the pass ([`hold_back`]), and the rest of the cluster
        // is still balanced (#2240). `state` names it whatever the pass decides to wait on.
        let held = self.retry_unfences(&map).await;
        let mut waiting: Vec<String> = held
            .iter()
            .map(|u| still_fenced(&u.partition, &u.move_id, &u.error))
            .collect();
        // An earlier writer's balancer move, which no task of this writer's runs (M1).
        if let Some(rec) = map
            .moves
            .iter()
            .find(|m| map::node_move(m) && m.id.starts_with("bal-"))
            .cloned()
        {
            if !map::committed(map::phase(&rec)) {
                self.abort_balancer_move(&map, &rec).await;
            } else {
                self.set_balancer_state(format!(
                    "finishing move {} of {} to node {} (past its commit point)",
                    rec.id, rec.from, rec.to
                ));
                self.spawn_move(&map, &rec.from, &rec.to, None);
            }
            return;
        }
        // A balancer range move or split no task of this writer's runs (D332 PR 5's design
        // §5): an earlier writer's, or this writer's own that stopped. Aborted before its
        // hand-over, ABORTING carried on, finished from HANDED_OVER.
        if let Some(rec) = map
            .moves
            .iter()
            .find(|m| !map::node_move(m) && m.id.starts_with("bal-"))
            .cloned()
        {
            if map::committed(map::phase(&rec)) {
                self.set_balancer_state(format!(
                    "finishing range move {} of {} into {} (past its commit point)",
                    rec.id, rec.from, rec.to
                ));
                self.spawn_resume(&map, &rec.id);
            } else {
                self.abort_balancer_range_move(&map, &rec).await;
            }
            return;
        }
        if let Some(m) = map.moves.first() {
            self.set_balancer_state(with_waiting(
                format!("waiting: move {} is in flight", m.id),
                &waiting,
            ));
            return;
        }
        // A source the balancer emptied by a merge (D332 PR 5's design §3): its removal.
        let emptied = emptied(&map);
        for s in &emptied {
            let Some(args) = self.own_args(&map) else {
                return;
            };
            match super::range_merge::remove(&args, s).await {
                Ok(super::range_merge::Ended::Waiting(why)) => {
                    waiting.push(format!("{s}, merged away, waits for {why}"));
                }
                Ok(super::range_merge::Ended::Removed { dropped, left }) => {
                    let mut st = format!("removed {s}, merged away earlier");
                    if !dropped.is_empty() {
                        st.push_str(&format!(" (moved aside: {})", dropped.join(", ")));
                    }
                    if !left.is_empty() {
                        st.push_str(&format!("; not done: {}", left.join("; ")));
                    }
                    self.set_balancer_state(st);
                    return;
                }
                // A stop is named in `state` like a wait, and the pass goes on: a fence it left
                // (an operator's to lift) mustn't halt every other action (audit120's S2 on
                // #2137). Each later pass tries the removal again.
                Err(f) => {
                    waiting.push(format!(
                        "the removal of {s}, merged away, stopped: {}",
                        f.message()
                    ));
                }
            }
        }
        // A paused balancer names the removals that wait or stopped, as a skip does (audit120's
        // nit on #2137): pausing doesn't hide a fence an operator has to lift.
        if mode == BalancerMode::Paused {
            self.set_balancer_state(with_waiting("paused".into(), &waiting));
            return;
        }
        let mut statuses: HashMap<String, NodeStatus> = self
            .survey_with(&map, true)
            .await
            .into_iter()
            .filter_map(|(id, st)| st.map(|st| (id, st)))
            .collect();
        let node = Arc::clone(self);
        if let Ok(mine) = tokio::task::spawn_blocking(move || node.status_with(true)).await {
            statuses.insert(self.id.clone(), mine);
        }
        let in_doubt = |p: &str| {
            self.grants
                .last_grant(p)
                .is_some_and(|g| g.in_doubt_count > 0)
        };
        let (mut nodes, mut parts) = views(&map, &statuses, in_doubt);
        hold_back(&mut nodes, &held);
        // An emptied source is neither moved whole nor merged into while it waits.
        parts.retain(|p| !emptied.contains(&p.id));
        let bounds = bounds_of(&map);
        let at = wall_ms();
        // The tablets of one split candidate a pass, through the split loop's gates (D332 PR
        // 5's design §1; S1 on #2137): before `decide`, which counts each pass's conditions
        // once. One with no cut, or whose owner didn't answer, is set aside for a dwell, so
        // the next pass surveys the next one over.
        let candidate = {
            let r = self.balancing();
            let until: HashMap<String, u64> =
                r.no_cut.iter().map(|(p, (t, _))| (p.clone(), *t)).collect();
            balance::split_candidate(&nodes, &parts, &bounds, &r.counters, at, &until)
        };
        if let Some(i) = candidate {
            let p = parts[i].clone();
            let why = match self.tablets_of(&map, &p).await {
                Some(t) => {
                    parts[i].tablets = t;
                    balance::unsplittable(&parts[i])
                }
                None => Some(format!("{}'s owner didn't answer the tablets survey", p.id)),
            };
            if let Some(why) = why {
                self.set_aside(&p.id, at, &bounds, why);
            }
        }
        // Still the writer, with its lease, at the same map, after the survey's wait: or a
        // later pass decides (S4; audit120's S1 on #2137).
        let Ok(current) = self.get_map() else { return };
        if (current.writer_term, current.map_version) != (map.writer_term, map.map_version)
            || self.writer_lease_remaining(&current).is_zero()
        {
            return;
        }
        // The copy-time check's rate: this node's `--move-rate-bytes`, as the nodes' figure.
        // A split's range copy is held to its source's; a whole move's copy isn't held to
        // one, so for it this is a planning figure (perf287 on the D332 PR 5 design).
        let rate = self
            .server
            .get()
            .and_then(|o| o.shared.upgrade())
            .and_then(|sh| sh.router.cluster.as_ref().map(|c| c.range_copies.rate()))
            .unwrap_or(64 << 20);
        let decision = {
            let mut r = self.balancing();
            let d = balance::decide(&nodes, &parts, &bounds, &mut r.counters, at, rate);
            r.ratios = node_ratios(&nodes, &parts);
            d
        };
        match decision {
            Decision::Skip(why) => {
                waiting.push(self.set_aside_now(at));
                self.set_balancer_state(with_waiting(format!("skip: {why}"), &waiting));
            }
            Decision::Act(a @ Action::Split { .. }, why) => {
                self.start_split(&map, &statuses, a, &why, at);
            }
            Decision::Act(a @ Action::Merge { .. }, why) => {
                self.start_merge(&map, a, &why, at);
            }
            Decision::Act(a @ Action::Move { .. }, why) => self.start_move(&map, a, &why, at).await,
            Decision::Act(a @ (Action::Pin { .. } | Action::Unpin { .. }), why) => {
                self.pin(&map, &a, &why, at).await;
            }
        }
    }

    /// Sets `m` (built from `map`, its version raised) under the writer lease, at any term:
    /// compare-and-set against `map`'s version.
    async fn balancer_change(
        self: &Arc<Self>,
        map: &ClusterMap,
        m: ClusterMap,
    ) -> Result<(), String> {
        if self.writer_lease_remaining(map).is_zero() {
            return Err("this writer holds no writer lease".into());
        }
        self.set_map(SetMapRequest {
            map: Some(m),
            expected_map_version: map.map_version,
            copies: Vec::new(),
            force: false,
        })
        .await
        .map(drop)
        .map_err(|e| e.message)
    }

    /// Starts the move `a` on a task of its own (see [`Self::spawn_move`]). Its MOVE event
    /// and history go in the map change that records the move, and the dwell starts once
    /// that has landed: a move that can't start isn't recorded or counted (audit120's S1,
    /// simp724's S1 on #2103).
    async fn start_move(self: &Arc<Self>, map: &ClusterMap, a: Action, why: &str, at: u64) {
        let Action::Move { partition, to, .. } = &a else {
            return;
        };
        self.set_balancer_state(format!("starting the move of {partition} to {to}: {why}"));
        self.spawn_move(map, partition, to, Some((a.clone(), why.to_owned(), at)));
    }

    /// Runs `phdb cluster move`'s steps for `partition` to `to` on a task of its own, with
    /// this node's cluster token (#2094), its map changes set here in process (audit120's
    /// M1 on #2103), its record a `bal-` one. `start`, for a new move: the action, why and
    /// when, recorded with the record. `None` finishes an earlier writer's move.
    fn spawn_move(
        self: &Arc<Self>,
        map: &ClusterMap,
        partition: &str,
        to: &str,
        start: Option<(Action, String, u64)>,
    ) {
        let Some(args) = self.own_args(map) else {
            return;
        };
        let (p, to, node) = (partition.to_owned(), to.to_owned(), Arc::downgrade(self));
        let bounds = bounds_of(map);
        let task = tokio::spawn(async move {
            let with = |m: &mut ClusterMap| -> Result<(), crate::exit::Failure> {
                let Some((
                    Action::Move {
                        partition,
                        from,
                        to,
                    },
                    why,
                    at,
                )) = &start
                else {
                    return Ok(());
                };
                // The decision's inputs (S4): no other move began meanwhile.
                if m.moves.iter().any(|r| r.from != *partition) {
                    return Err(crate::exit::Failure::usage(
                        "another move started since the balancer decided",
                    ));
                }
                let mut e = event(BalancerEventKind::Move, partition, why, *at);
                e.from = from.clone();
                e.to = to.clone();
                push_event(m, e);
                push_history(m, partition, BalancerEventKind::Move, *at);
                Ok(())
            };
            let started = |id: &str| {
                let Some(node) = node.upgrade() else { return };
                if let Some((
                    a @ Action::Move {
                        partition,
                        from,
                        to,
                    },
                    why,
                    at,
                )) = &start
                {
                    {
                        let mut r = node.balancing();
                        r.counters.took(a, *at, &bounds);
                        *r.actions.entry("move").or_insert(0) += 1;
                    }
                    node.set_balancer_state(format!(
                        "moving {partition} from {from} to {to} ({id}): {why}"
                    ));
                }
            };
            let how = super::moving::Start {
                prefix: "bal",
                with: Some(&with),
                on_started: Some(&started),
            };
            let out = super::moving::run_as(&args, &p, &to, false, None, &how).await;
            if let Err(f) = &out {
                eprintln!(
                    "phdb serve: the balancer's move of {p} to {to} stopped: {}",
                    f.message()
                );
                if let Some(node) = node.upgrade() {
                    node.set_balancer_state(format!(
                        "the move of {p} to {to} stopped: {}",
                        f.message()
                    ));
                }
            }
        });
        self.balancing().moving = Some(task);
    }

    /// Starts the split `a` on a task of its own, through D331 PR 5's split driver with a
    /// `bal-` record: its SPLIT event and P's history go in the change that records the move
    /// and adds the new partition, and the dwell starts once that has landed (as a move's).
    /// The driver names the new partition `<P>-<v>` at the version its start lands at, so
    /// the name is read from the record, never computed here (ai1249); the check below only
    /// skips a split whose name couldn't be valid.
    fn start_split(
        self: &Arc<Self>,
        map: &ClusterMap,
        statuses: &HashMap<String, NodeStatus>,
        a: Action,
        why: &str,
        at: u64,
    ) {
        let Action::Split {
            partition,
            table,
            key,
            node: to_node,
            bytes,
            ..
        } = &a
        else {
            return;
        };
        // D331's own limits on the source (its followers, its move rate and retention cap),
        // reckoned on the moving tablets' bytes (the coordinator's amendment), then the
        // dwell, which is the balancer's own (simp724, perf287's should-fix 2).
        let bounds = bounds_of(map);
        // A split that can't start: said in `state`, and its partition set aside (S1 on
        // #2137), so the next pass surveys the next candidate over.
        let cant = |reason: String| {
            self.set_balancer_state(format!("skip: {why}, but {reason}"));
            self.set_aside(partition, at, &bounds, reason);
        };
        let plan = match super::range_move::plan(map, statuses, partition, Some(*bytes)) {
            Ok(plan) => plan,
            Err(
                super::range_move::Skip::Busy(s)
                | super::range_move::Skip::Fit(s)
                | super::range_move::Skip::Unknown(s),
            ) => {
                cant(format!("its split can't start: {s}"));
                return;
            }
        };
        if !balance::copy_fits_dwell(plan.copy, &bounds) {
            cant(format!(
                "its split's copy of {} bytes would take {} s at the source's move rate, \
                 longer than the dwell",
                plan.bytes,
                plan.copy.as_secs()
            ));
            return;
        }
        let next = map.map_version.saturating_add(1);
        let Some(new) = balance::split_id(partition, next) else {
            cant(format!(
                "its split's new partition {partition}-{next} would be over 64 characters"
            ));
            return;
        };
        if map.partitions.iter().any(|q| q.id == new) {
            cant(format!("its split's new partition {new} is taken"));
            return;
        }
        let Some(args) = self.own_args(map) else {
            return;
        };
        self.set_balancer_state(format!(
            "starting the split of {partition}'s {table} onto node {to_node}: {why}"
        ));
        let spec = super::range_move::SplitSpec {
            table: table.clone(),
            from: partition.clone(),
            key: key.clone(),
            node: to_node.clone(),
            id: None,
        };
        let (why, node) = (why.to_owned(), Arc::downgrade(self));
        let task = tokio::spawn(async move {
            let Action::Split {
                partition,
                table,
                key,
                node: to_node,
                ..
            } = &a
            else {
                return;
            };
            let with = |m: &mut ClusterMap| -> Result<(), crate::exit::Failure> {
                // The decision's inputs (S4): no other move began meanwhile.
                if m.moves.iter().any(|r| r.from != *partition) {
                    return Err(crate::exit::Failure::usage(
                        "another move started since the balancer decided",
                    ));
                }
                let mut e = event(BalancerEventKind::Split, partition, &why, at);
                e.to = to_node.clone();
                e.table = table.clone();
                e.split_key = Some(map::canonical_key(key));
                push_event(m, e);
                // P keeps its history; the new partition starts with none (simp724's M2).
                push_history(m, partition, BalancerEventKind::Split, at);
                Ok(())
            };
            let started = |id: &str| {
                let Some(node) = node.upgrade() else { return };
                {
                    let mut r = node.balancing();
                    r.counters.took(&a, at, &bounds);
                    *r.actions.entry("split").or_insert(0) += 1;
                }
                let new = node
                    .get_map()
                    .ok()
                    .and_then(|m| m.moves.iter().find(|r| r.id == id).map(|r| r.to.clone()))
                    .unwrap_or_default();
                node.set_balancer_state(format!(
                    "splitting {partition}'s {table} into {new} on node {to_node} ({id}): {why}"
                ));
            };
            let how = super::moving::Start {
                prefix: "bal",
                with: Some(&with),
                on_started: Some(&started),
            };
            let out = super::range_move::split_as(&args, &spec, None, &how).await;
            if let Err(f) = &out {
                eprintln!(
                    "phdb serve: the balancer's split of {partition} stopped: {}",
                    f.message()
                );
                if let Some(node) = node.upgrade() {
                    node.set_balancer_state(format!(
                        "the split of {partition} stopped: {}",
                        f.message()
                    ));
                }
            }
        });
        self.balancing().moving = Some(task);
    }

    /// Sets split candidate `p` aside for a dwell from `at`: the next pass surveys the next
    /// one over (S1 on #2137).
    fn set_aside(&self, p: &str, at: u64, bounds: &Bounds, why: String) {
        self.balancing()
            .no_cut
            .insert(p.to_owned(), (at.saturating_add(bounds.dwell_ms), why));
    }

    /// The split candidates set aside at `at`, for `state` (S1 on #2137): what a skip that
    /// names an unsurveyed partition hides.
    fn set_aside_now(&self, at: u64) -> String {
        let mut r = self.balancing();
        r.no_cut.retain(|_, (until, _)| at < *until);
        let mut aside: Vec<String> = r
            .no_cut
            .iter()
            .map(|(p, (until, why))| {
                format!(
                    "{p} set aside for {} s more as a split candidate: {why}",
                    until.saturating_sub(at) / 1000
                )
            })
            .collect();
        aside.sort();
        aside.join("; ")
    }

    /// Starts the merge `a` of S into T on a task of its own: D331 PR 6's merge, each of S's
    /// ranges a `bal-` range move into T, then S's removal (D332 PR 5's design §3). The MERGE
    /// event and S's history go in the change that records the first range, and the dwell
    /// starts once that has landed (as a split's). A merge that stops part-way leaves S
    /// holding the rest, a valid layout; a later pass judges S again, and an emptied S's
    /// removal is the pass's own ([`emptied`]).
    fn start_merge(self: &Arc<Self>, map: &ClusterMap, a: Action, why: &str, at: u64) {
        let Action::Merge { from, into } = &a else {
            return;
        };
        let Some(args) = self.own_args(map) else {
            return;
        };
        self.set_balancer_state(format!("starting the merge of {from} into {into}: {why}"));
        let bounds = bounds_of(map);
        let (why, node) = (why.to_owned(), Arc::downgrade(self));
        let task = tokio::spawn(async move {
            let Action::Merge { from, into } = &a else {
                return;
            };
            // Whether the first range's record has landed, with the event and history.
            let recorded = std::sync::atomic::AtomicBool::new(false);
            let with = |m: &mut ClusterMap| -> Result<(), crate::exit::Failure> {
                // The decision's inputs (S4): no other move began meanwhile.
                if m.moves.iter().any(|r| r.from != *from) {
                    return Err(crate::exit::Failure::usage(
                        "another move started since the balancer decided",
                    ));
                }
                if recorded.load(std::sync::atomic::Ordering::Acquire) {
                    return Ok(());
                }
                let mut e = event(BalancerEventKind::Merge, from, &why, at);
                e.to = into.clone();
                push_event(m, e);
                // S's, which marks it as the balancer's to remove once emptied.
                push_history(m, from, BalancerEventKind::Merge, at);
                Ok(())
            };
            let started = |id: &str| {
                let Some(node) = node.upgrade() else { return };
                if !recorded.swap(true, std::sync::atomic::Ordering::AcqRel) {
                    let mut r = node.balancing();
                    r.counters.took(&a, at, &bounds);
                    *r.actions.entry("merge").or_insert(0) += 1;
                }
                node.set_balancer_state(format!(
                    "merging {from} into {into}, range move {id}: {why}"
                ));
            };
            let how = super::moving::Start {
                prefix: "bal",
                with: Some(&with),
                on_started: Some(&started),
            };
            let out = super::range_merge::run_as(&args, from, into, None, true, &how).await;
            let Some(node) = node.upgrade() else { return };
            match out {
                Ok(r) => {
                    let field = |k: &str| {
                        r.document
                            .get(k)
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_owned()
                    };
                    let (source, detail) = (field("source"), field("detail"));
                    node.set_balancer_state(if source == "removed" {
                        format!("merged {from} into {into}, and removed {from}: {why}")
                    } else {
                        format!(
                            "merged {from}'s ranges into {into}; {from}'s removal waits for \
                             {detail}"
                        )
                    });
                }
                Err(f) => {
                    eprintln!(
                        "phdb serve: the balancer's merge of {from} into {into} stopped: {}",
                        f.message()
                    );
                    node.set_balancer_state(format!(
                        "the merge of {from} into {into} stopped: {}",
                        f.message()
                    ));
                }
            }
        });
        self.balancing().moving = Some(task);
    }

    /// Finishes an earlier writer's balancer range move or split `id`, past its commit point,
    /// on a task of its own (D331 PR 5's `resume`: from the record, by its phase).
    fn spawn_resume(self: &Arc<Self>, map: &ClusterMap, id: &str) {
        let Some(args) = self.own_args(map) else {
            return;
        };
        let (id, node) = (id.to_owned(), Arc::downgrade(self));
        let task = tokio::spawn(async move {
            if let Err(f) = super::range_move::resume(&args, &id, None).await {
                eprintln!(
                    "phdb serve: the balancer finishing move {id} stopped: {}",
                    f.message()
                );
                if let Some(node) = node.upgrade() {
                    node.set_balancer_state(format!(
                        "finishing move {id} stopped: {}",
                        f.message()
                    ));
                }
            }
        });
        self.balancing().moving = Some(task);
    }

    /// Ends a stopped balancer range move or split `rec` (an earlier writer's, or this
    /// writer's own whose task stopped) before its commit point,
    /// or carries on its abort (ABORTING): D331 PR 5's abort, which for a split also removes
    /// its new partition once empty (simp724's M1 on the D332 PR 5 design). Then an ABORT
    /// event. A target that can't drop its copy within the bound leaves the record
    /// ABORTING; a later pass tries again.
    async fn abort_balancer_range_move(self: &Arc<Self>, map: &ClusterMap, rec: &proto::RangeMove) {
        let Some(args) = self.own_args(map) else {
            return;
        };
        self.set_balancer_state(format!(
            "aborting the stopped balancer range move {} of {} into {}",
            rec.id, rec.from, rec.to
        ));
        if let Err(f) =
            super::range_move::abort(&args, &rec.id, Some(Duration::from_secs(60))).await
        {
            self.set_balancer_state(format!(
                "aborting the stopped balancer range move {} of {}: {}",
                rec.id,
                rec.from,
                f.message()
            ));
            return;
        }
        let Ok(now) = self.get_map() else { return };
        let mut m = now.clone();
        m.map_version += 1;
        let at = wall_ms();
        let mut e = event(
            BalancerEventKind::Abort,
            &rec.from,
            "a stopped balancer range move (an earlier writer's, or this one's own), before its \
             hand-over: this writer decides afresh",
            at,
        );
        e.move_id = rec.id.clone();
        e.to = rec.to.clone();
        e.table = rec.table.clone();
        push_event(&mut m, e);
        let recorded = self.balancer_change(&now, m).await;
        *self.balancing().actions.entry("abort").or_insert(0) += 1;
        self.set_balancer_state(format!(
            "aborted the stopped balancer range move {} of {} into {}{}",
            rec.id,
            rec.from,
            rec.to,
            unrecorded(recorded)
        ));
    }

    /// A map change of this writer's own balancer move, set here in process (audit120's M1
    /// on #2103: `SetMap` takes the admin scope, and the writer holds only the cluster
    /// token), compare-and-set against the map `m` was built from. Under the writer lease at
    /// any term (simp724's M1 on #2103, the plan's Amendment 2): a writer cut off at term 0
    /// can't go on moving.
    pub(crate) async fn set_own_map(
        self: &Arc<Self>,
        m: ClusterMap,
        copies: Vec<String>,
    ) -> Result<proto::SetMapResponse, crate::serve::errors::ApiError> {
        let current = self.get_map()?;
        if schema::writer(&current) == self.id && self.writer_lease_remaining(&current).is_zero() {
            return Err(crate::serve::errors::ApiError::partition_unavailable(
                "this writer holds no writer lease: the balancer's move changes no map without \
                 it (D332)",
            ));
        }
        let expected = m.map_version.saturating_sub(1);
        self.set_map(SetMapRequest {
            map: Some(m),
            expected_map_version: expected,
            copies,
            force: false,
        })
        .await
    }

    /// This node as `phdb cluster` reaches it, with its own cluster token and CA.
    /// `None`, said in the balancer's state, when the map doesn't list this node (audit120's
    /// nit on #2103).
    pub(super) fn own_args(self: &Arc<Self>, map: &ClusterMap) -> Option<super::cli::NodeArgs> {
        let Some(n) = map.nodes.iter().find(|n| n.id == self.id) else {
            self.set_balancer_state(format!(
                "waiting: the map doesn't list this node ({}), so it can't run a move",
                self.id
            ));
            return None;
        };
        Some(super::cli::NodeArgs {
            node: format!("{}://{}", self.peers.scheme, n.address),
            token_file: None,
            ca: None,
            own: Some(Box::new(super::cli::Own {
                token: self.peers.token.clone(),
                ca: self.peers.ca.clone(),
                node: Arc::downgrade(self),
            })),
        })
    }

    /// Ends an earlier writer's balancer move `rec`, before its commit point (M1): its
    /// record goes and the target's copy aside (`moving::abort`), the partition is unfenced
    /// on its node, and an ABORT event is recorded.
    async fn abort_balancer_move(self: &Arc<Self>, map: &ClusterMap, rec: &proto::RangeMove) {
        let Some(args) = self.own_args(map) else {
            return;
        };
        if let Err(f) = super::moving::abort(&args, &rec.from).await {
            self.set_balancer_state(format!(
                "aborting an earlier writer's move {} of {}: {}",
                rec.id,
                rec.from,
                f.message()
            ));
            return;
        }
        // The move fenced and closed P on its node: serve it there again. The record is gone
        // (first, so a rerun of the move loses its compare-and-set before P takes writes), so
        // an unfence that fails is kept and tried again each pass (#2151).
        let still_fenced = match self.unfence_after_abort(map, &rec.from).await {
            Ok(()) => String::new(),
            Err(e) => {
                eprintln!(
                    "phdb serve: the balancer unfencing {} after aborting move {}: {e}; it \
                     tries again each pass",
                    rec.from, rec.id
                );
                let note = format!("; {}", still_fenced(&rec.from, &rec.id, &e));
                // Kept with where the partition is now: the retry lifts this move's fence
                // only (ai1249's S1 on #2170).
                if let Some(p) = map.partitions.iter().find(|p| p.id == rec.from) {
                    self.balancing().unfence.push(Unfence {
                        partition: rec.from.clone(),
                        move_id: rec.id.clone(),
                        error: e,
                        node: p.node.clone(),
                        epoch: p.epoch,
                        term: map.writer_term,
                    });
                }
                note
            }
        };
        let Ok(now) = self.get_map() else { return };
        let mut m = now.clone();
        m.map_version += 1;
        let at = wall_ms();
        let mut e = event(
            BalancerEventKind::Abort,
            &rec.from,
            "an earlier writer's move, before its commit point: this writer decides afresh",
            at,
        );
        e.move_id = rec.id.clone();
        e.to = rec.to.clone();
        push_event(&mut m, e);
        let recorded = self.balancer_change(&now, m).await;
        *self.balancing().actions.entry("abort").or_insert(0) += 1;
        self.set_balancer_state(format!(
            "aborted an earlier writer's move {} of {} to node {}{}{still_fenced}",
            rec.id,
            rec.from,
            rec.to,
            unrecorded(recorded)
        ));
    }

    /// Unfences `partition` on the node `map` gives it to, after a balancer move's abort.
    async fn unfence_after_abort(&self, map: &ClusterMap, partition: &str) -> Result<(), String> {
        #[cfg(test)]
        {
            let mut fail = FAIL_UNFENCE.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(i) = fail.iter().position(|d| d == self.dir()) {
                fail.swap_remove(i);
                return Err("the unfence failed (a test's fault)".to_owned());
            }
        }
        let owner = map
            .partitions
            .iter()
            .find(|p| p.id == partition)
            .map(|p| p.node.as_str())
            .ok_or_else(|| format!("the map has no partition {partition}"))?;
        let n = map
            .nodes
            .iter()
            .find(|n| n.id == owner)
            .ok_or_else(|| format!("the map has no node {owner}"))?;
        let req = PartitionRequest {
            partition: partition.to_owned(),
            ..PartitionRequest::default()
        };
        let mut c = self.client(&n.address).await?;
        c.unfence(self.request(req)?)
            .await
            .map(drop)
            .map_err(|s| format!("node {owner}: {}", s.message()))
    }

    /// Tries again each unfence an abort left ([`Run::unfence`], #2151). One is dropped once
    /// it succeeds, or once the map no longer has its partition as the abort left it
    /// ([`Unfence::stands`]). The ones still waiting, whose nodes sit out the pass
    /// ([`hold_back`], #2240).
    async fn retry_unfences(&self, map: &ClusterMap) -> Vec<Unfence> {
        let pending = self.balancing().unfence.clone();
        if pending.is_empty() {
            return Vec::new();
        }
        let mut left = Vec::new();
        for mut u in pending {
            if !u.stands(map) {
                continue;
            }
            match self.unfence_after_abort(map, &u.partition).await {
                Ok(()) => eprintln!(
                    "phdb serve: the balancer unfenced {} (left fenced by aborted move {})",
                    u.partition, u.move_id
                ),
                Err(e) => {
                    u.error = e;
                    left.push(u);
                }
            }
        }
        self.balancing().unfence.clone_from(&left);
        left
    }

    /// An auto-pin, or its expiry.
    async fn pin(self: &Arc<Self>, map: &ClusterMap, a: &Action, why: &str, at: u64) {
        let (p, on) = match a {
            Action::Pin { partition } => (partition, true),
            Action::Unpin { partition } => (partition, false),
            _ => return,
        };
        let mut m = map.clone();
        m.map_version += 1;
        for q in m.partitions.iter_mut().filter(|q| q.id == *p) {
            q.pinned = on;
            if !on {
                q.moves_history
                    .retain(|h| h.kind() != BalancerEventKind::Pin);
            }
        }
        if on {
            push_history(&mut m, p, BalancerEventKind::Pin, at);
        }
        push_event(&mut m, event(BalancerEventKind::Pin, p, why, at));
        match self.balancer_change(map, m).await {
            Ok(()) => {
                *self.balancing().actions.entry("pin").or_insert(0) += 1;
                self.set_balancer_state(format!(
                    "{} {p}: {why}",
                    if on { "pinned" } else { "unpinned" }
                ));
            }
            Err(e) => self.set_balancer_state(format!("skip: pinning {p}: {e}")),
        }
    }

    /// `pigeonhole_cluster_balancer_actions_total{kind}` and
    /// `pigeonhole_cluster_node_load_ratio{node}`, from this node's balancer (the writer's).
    pub(crate) fn render_balancer_metrics(&self, text: &mut String) {
        use std::fmt::Write as _;
        let r = self.balancing();
        let _ = writeln!(
            text,
            "# HELP pigeonhole_cluster_balancer_actions_total The balancer's actions while this \
             node is the map's writer (D332).\n# TYPE pigeonhole_cluster_balancer_actions_total \
             counter"
        );
        let mut kinds: Vec<_> = r.actions.iter().collect();
        kinds.sort();
        for (k, n) in kinds {
            let _ = writeln!(
                text,
                "pigeonhole_cluster_balancer_actions_total{{kind=\"{k}\"}} {n}"
            );
        }
        let _ = writeln!(
            text,
            "# HELP pigeonhole_cluster_node_load_ratio Each node's load per unit of weight over \
             the mean, at the balancer's last pass (D332).\n# TYPE \
             pigeonhole_cluster_node_load_ratio gauge"
        );
        for (n, ratio) in &r.ratios {
            let _ = writeln!(
                text,
                "pigeonhole_cluster_node_load_ratio{{node=\"{n}\"}} {ratio}"
            );
        }
    }
}

/// Each fresh node's load per unit of weight over the mean.
fn node_ratios(nodes: &[NodeView], parts: &[PartView]) -> Vec<(String, f64)> {
    let loads: Vec<(String, f64)> = nodes
        .iter()
        .filter(|n| n.fresh)
        .map(|n| {
            let l: f64 = parts
                .iter()
                .filter(|p| p.node == n.id)
                .map(|p| p.load)
                .sum();
            (
                n.id.clone(),
                l / if n.weight > 0.0 { n.weight } else { 1.0 },
            )
        })
        .collect();
    let mean = loads.iter().map(|(_, l)| l).sum::<f64>() / loads.len().max(1) as f64;
    loads
        .into_iter()
        .map(|(n, l)| (n, if mean > 0.0 { l / mean } else { 1.0 }))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{ClusterMap, Unfence, proto};

    fn kept() -> Unfence {
        Unfence {
            partition: "p1".into(),
            move_id: "bal-1".into(),
            error: String::new(),
            node: "a".into(),
            epoch: 4,
            term: 7,
        }
    }

    fn map_with(node: &str, epoch: u64) -> ClusterMap {
        ClusterMap {
            partitions: vec![proto::Partition {
                id: "p1".into(),
                node: node.into(),
                epoch,
                ..proto::Partition::default()
            }],
            ..ClusterMap::default()
        }
    }

    /// ai1249's S1 on #2170: a kept unfence lifts only the fence its aborted move set. It
    /// goes once the partition isn't where the abort left it (a relocation, a failover or a
    /// forced map changes its node or epoch with no move record), once a move names it, or
    /// once the map has no such partition.
    #[test]
    fn a_kept_unfence_stands_only_while_the_partition_is_as_the_abort_left_it() {
        let u = kept();
        assert!(u.stands(&map_with("a", 4)));
        assert!(!u.stands(&map_with("b", 4)), "another node");
        assert!(!u.stands(&map_with("a", 5)), "a new epoch on the same node");
        assert!(!u.stands(&ClusterMap::default()), "no such partition");
        // A whole move of p1, and a range move into it.
        let mut moving = map_with("a", 4);
        moving.moves.push(proto::RangeMove {
            id: "mv".into(),
            from: "p1".into(),
            to: "b".into(),
            ..proto::RangeMove::default()
        });
        assert!(!u.stands(&moving), "a move of it");
        let mut into = map_with("a", 4);
        into.moves.push(proto::RangeMove {
            id: "range-1".into(),
            from: "p2".into(),
            to: "p1".into(),
            table: "users".into(),
            ..proto::RangeMove::default()
        });
        assert!(!u.stands(&into), "a range move into it");
    }

    /// It carries into this node's own next writer term only: no other node held a term in
    /// between. Not into a later one, nor (never seen) an earlier one.
    #[test]
    fn a_kept_unfence_carries_into_the_next_term_only() {
        let u = kept();
        assert!(u.carries(8));
        assert!(!u.carries(9));
        assert!(!u.carries(7));
        assert!(!u.carries(6));
        let mut last = kept();
        last.term = u64::MAX;
        assert!(!last.carries(0));
    }

    /// #2240: the node of a partition whose unfence is still waiting sits out the pass, even
    /// when it answered with every partition measured (a fenced one counts as measured);
    /// the other nodes are left as they were.
    #[test]
    fn a_waiting_unfence_holds_back_only_its_node() {
        use super::balance::NodeView;
        let view = |id: &str| NodeView {
            id: id.into(),
            weight: 1.0,
            fresh: true,
            incomplete: None,
        };
        let mut nodes = vec![view("a"), view("b"), view("c")];
        super::hold_back(&mut nodes, &[]);
        assert!(nodes.iter().all(|n| n.fresh && n.incomplete.is_none()));
        let mut gone = kept();
        gone.node = "z".into();
        super::hold_back(&mut nodes, &[kept(), gone]);
        assert!(!nodes[0].fresh);
        assert_eq!(
            nodes[0].incomplete.as_deref(),
            Some(
                "node a sits out the balancer's passes while partition p1 waits for its \
                 unfence after aborted move bal-1"
            )
        );
        assert!(nodes[1..].iter().all(|n| n.fresh && n.incomplete.is_none()));
    }

    /// #2158 item 1: a node that answered, but without numbers for a partition the map
    /// places on it (closed, opening, or missing from its Status), isn't fresh, and says
    /// why. A fenced partition is still measured.
    #[test]
    fn a_node_is_fresh_only_with_numbers_for_every_partition_placed_on_it() {
        use super::proto::{LocalPartition, LocalPartitionState as S, NodeStatus};
        use std::collections::HashMap;
        let placed = [
            ("p1", "a"),
            ("p2", "b"),
            ("p3", "c"),
            ("p4", "d"),
            ("p5", "e"),
        ];
        let map = ClusterMap {
            nodes: ["a", "b", "c", "d", "e"]
                .iter()
                .map(|id| proto::Node {
                    id: (*id).into(),
                    ..proto::Node::default()
                })
                .collect(),
            partitions: placed
                .iter()
                .map(|(id, node)| proto::Partition {
                    id: (*id).into(),
                    node: (*node).into(),
                    ..proto::Partition::default()
                })
                .collect(),
            ..ClusterMap::default()
        };
        let status = |p: &str, state: Option<S>| {
            let mut st = NodeStatus::default();
            if let Some(state) = state {
                let mut lp = LocalPartition {
                    id: p.into(),
                    ..LocalPartition::default()
                };
                lp.set_state(state);
                st.partitions.push(lp);
            }
            st
        };
        // e doesn't answer at all.
        let statuses: HashMap<String, NodeStatus> = [
            ("a", status("p1", Some(S::Serving))),
            ("b", status("p2", Some(S::Fenced))),
            ("c", status("p3", Some(S::Closed))),
            ("d", status("p4", None)),
        ]
        .into_iter()
        .map(|(n, st)| (n.to_owned(), st))
        .collect();
        let (nodes, _) = super::views(&map, &statuses, |_| false);
        let of = |id: &str| nodes.iter().find(|n| n.id == id).expect("a node");
        assert!(of("a").fresh && of("a").incomplete.is_none());
        assert!(
            of("b").fresh && of("b").incomplete.is_none(),
            "fenced is measured"
        );
        assert!(!of("c").fresh);
        assert_eq!(
            of("c").incomplete.as_deref(),
            Some("node c's numbers are incomplete: partition p3 is closed there")
        );
        assert!(!of("d").fresh);
        assert_eq!(
            of("d").incomplete.as_deref(),
            Some("node d's numbers are incomplete: partition p4 isn't open there")
        );
        // Not answering is the old staleness, with nothing to name.
        assert!(!of("e").fresh && of("e").incomplete.is_none());
    }
}
