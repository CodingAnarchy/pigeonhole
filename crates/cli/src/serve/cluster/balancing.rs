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
//! 2. **Waits** while any move is in flight (one action at a time), or while PAUSED.
//! 3. **Surveys** every node's `Status` with `open_only` (perf287), and runs the policy.
//! 4. **Acts:** a whole move runs on a task of its own through `phdb cluster move`'s steps
//!    (`moving::run_as`, its record `bal-<ms>`), with the writer's cluster token (#2094). A
//!    pin or an unpin is a map change. A split or a merge waits for D331 PR 5 and PR 6.
//!
//! A pass that does nothing changes nothing in the map: the current skip is in this
//! node's `NodeStatus.balancer` only (simp724's M2, perf287's should-fix 1). The counters
//! are in memory and start again with each writer term (S3).

use std::collections::HashMap;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use super::balance::{self, Action, Bounds, Counters, Decision, Kind, NodeView, PartView};
use super::proto::{
    self, BalancerEvent, BalancerEventKind, BalancerMode, ClusterMap, MoveHistory, NodeStatus,
    PartitionRequest, PartitionRole, SetMapRequest,
};
use super::{Node, map, schema};

/// How often the balancer's pass runs.
pub(crate) const PASS: Duration = Duration::from_secs(30);

/// A test's shorter pass, by the node's cluster directory.
#[cfg(test)]
pub(crate) static PASS_FOR: std::sync::Mutex<Vec<(std::path::PathBuf, Duration)>> =
    std::sync::Mutex::new(Vec::new());

/// `--move-rate-bytes`' default, per second, for the copy-time check until D331 PR 4b adds
/// the flag.
const MOVE_RATE: u64 = 64 << 20;

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
    let nodes = map
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

/// `m` with `e` appended to its events, the oldest dropped past [`map::BALANCER_EVENTS`].
fn push_event(m: &mut ClusterMap, e: BalancerEvent) {
    m.balancer_events.push(e);
    let over = m.balancer_events.len().saturating_sub(map::BALANCER_EVENTS);
    m.balancer_events.drain(..over);
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
            self.set_balancer_state("waiting: this writer holds no writer lease".into());
            return;
        }
        let now = self.grants.clock().now();
        let pass = u64::try_from(self.pass().as_nanos()).unwrap_or(u64::MAX);
        {
            let mut r = self.balancing();
            if r.term != Some(map.writer_term) {
                // A new term's counters; the move in flight, the running pass and the
                // metric's totals carry over.
                *r = Run {
                    term: Some(map.writer_term),
                    moving: r.moving.take(),
                    actions: std::mem::take(&mut r.actions),
                    passing: r.passing,
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
        if let Some(m) = map.moves.first() {
            self.set_balancer_state(format!("waiting: move {} is in flight", m.id));
            return;
        }
        if mode == BalancerMode::Paused {
            self.set_balancer_state("paused".into());
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
        // Still the writer, with its lease, at the same map: or a later pass decides (S4).
        let Ok(current) = self.get_map() else { return };
        if (current.writer_term, current.map_version) != (map.writer_term, map.map_version)
            || self.writer_lease_remaining(&current).is_zero()
        {
            return;
        }
        let in_doubt = |p: &str| {
            self.grants
                .last_grant(p)
                .is_some_and(|g| g.in_doubt_count > 0)
        };
        let (nodes, parts) = views(&map, &statuses, in_doubt);
        let bounds = bounds_of(&map);
        let at = wall_ms();
        let decision = {
            let mut r = self.balancing();
            let d = balance::decide(&nodes, &parts, &bounds, &mut r.counters, at, MOVE_RATE);
            r.ratios = node_ratios(&nodes, &parts);
            d
        };
        match decision {
            Decision::Skip(why) => self.set_balancer_state(format!("skip: {why}")),
            Decision::Act(Action::Split { partition, .. }, why) => {
                self.set_balancer_state(format!(
                    "skip: a split of {partition} ({why}) waits for D331 PR 5's driver"
                ));
            }
            Decision::Act(Action::Merge { from, into }, why) => {
                self.set_balancer_state(format!(
                    "skip: a merge of {from} into {into} ({why}) waits for D331 PR 6"
                ));
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
    fn own_args(self: &Arc<Self>, map: &ClusterMap) -> Option<super::cli::NodeArgs> {
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
        // The move fenced and closed P on its node: serve it there again.
        let owner = map
            .partitions
            .iter()
            .find(|p| p.id == rec.from)
            .map(|p| p.node.clone())
            .unwrap_or_default();
        if let Some(n) = map.nodes.iter().find(|n| n.id == owner) {
            let req = PartitionRequest {
                partition: rec.from.clone(),
                ..PartitionRequest::default()
            };
            let lifted = async {
                let mut c = self.client(&n.address).await?;
                c.unfence(self.request(req)?)
                    .await
                    .map(drop)
                    .map_err(|s| s.message().to_owned())
            }
            .await;
            if let Err(e) = lifted {
                eprintln!(
                    "phdb serve: the balancer unfencing {} on node {owner} after aborting move \
                     {}: {e}",
                    rec.from, rec.id
                );
            }
        }
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
        let _ = self.balancer_change(&now, m).await;
        *self.balancing().actions.entry("abort").or_insert(0) += 1;
        self.set_balancer_state(format!(
            "aborted an earlier writer's move {} of {} to node {}",
            rec.id, rec.from, rec.to
        ));
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
