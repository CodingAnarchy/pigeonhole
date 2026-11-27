//! `phdb cluster promote P --to ID` (D328 PR 3): make the node holding one of P's replica
//! copies P's owner. The copy is closed (a map without ID in P's replicas), promoted to a
//! primary at the last commit it applied and stamped at the next epoch on ID, then the map
//! gives ID the partition at that epoch, with a new history. The old owner must be fenced
//! or closed, or unreachable (`--old-unreachable`), and D326's leases keep it from serving
//! once the new owner does. Run again, it finishes a promote that stopped.
//!
//! The steps after the old owner's checks ([`run`]) are shared with the map's writer, which
//! fails a down partition over the same way (D330 PR 2, `failover.rs`). With writer terms,
//! the map records each promote pending at its epoch before the stamp, and the next epoch
//! is above every one a promote of P recorded ([`target_epoch`]), not always the map's
//! + 1: a copy stamped by a promote that never finished is never taken for a new history.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::{Value, json};

use super::cli::{NodeArgs, connect, report, request, set_map, url_of};
use super::moving::{At, serving};
use crate::exit::Failure;
use crate::output::Report;
use crate::serve::proto::{self, GetMapRequest, StatusRequest};

/// A test's fault seam: the next promote through each of these `--node` URLs stops after
/// its stamp, before its map, as a crash there would leave it. Keyed by the URL, so tests
/// running at once don't take each other's.
#[cfg(test)]
pub(crate) static STOP_AFTER_STAMP: std::sync::Mutex<Vec<String>> =
    std::sync::Mutex::new(Vec::new());

/// How long the CLI's survey of a node's `Status` waits (#2152).
const SURVEY_WAIT: Duration = Duration::from_secs(3);

/// Past the longest grant's time left, before the promote looks again (#2152): the grantors'
/// clocks and the survey's own delay.
const GRANT_SLACK: Duration = Duration::from_millis(500);

/// How long the promote waits for ID to close its copy after the map dropped it.
const CLOSE_WAIT: Duration = Duration::from_secs(60);

/// How long a planned promote waits for the copy to hear its fenced owner (#2162 item 4):
/// the owner sends at least once a second while idle.
const SETTLE_WAIT: Duration = Duration::from_secs(3);

/// Past the copy's own reading, before a message counts as heard after the fence: the two
/// clocks run at the same rate, and this covers the copy's apply of it. Not the call's
/// latency: [`settle`] measures from when it sent the call, before the copy's reading
/// (feed1248's N2 on #2184).
const SETTLE_SLACK: Duration = Duration::from_millis(200);

/// Fences `partition` again on its owner at `url`, within [`SETTLE_WAIT`]: the answer
/// comes once its feed's durable frontier has reached its last commit (feed1248's S1 on
/// #2184), so what it sends from then on is final. Only for an operator's fence, which this
/// repeats and adds nothing to. Whether it answered so.
async fn fence_again(args: &NodeArgs, url: &str, partition: &str) -> bool {
    let call = async {
        let (mut c, token) = connect(args, url).await.ok()?;
        let req = request(
            proto::PartitionRequest {
                partition: partition.to_owned(),
                force: false,
            },
            token.as_deref(),
        )
        .ok()?;
        c.fence(req).await.ok()
    };
    matches!(tokio::time::timeout(SETTLE_WAIT, call).await, Ok(Some(_)))
}

/// Waits up to [`SETTLE_WAIT`] for node `to`'s copy of `partition` (at `url`) to have heard
/// its owner since this call and applied through the durable seqno it heard (#2162 item 4).
/// The owner's frontier is final ([`fence_again`]), so that is the last one. Whether it did.
async fn settle(args: &NodeArgs, url: &str, partition: &str) -> bool {
    let since = tokio::time::Instant::now();
    loop {
        // The copy reads its lag after this: heard at least that long before `sent`.
        let sent = tokio::time::Instant::now();
        if let Ok(Some(lp)) = row_on(args, url, partition).await
            && lp.role() == proto::PartitionRole::Replica
            && Duration::from_millis(lp.replica_lag_ms) + SETTLE_SLACK < sent - since
            && lp.replica_applied_seqno >= lp.replica_primary_seqno
        {
            return true;
        }
        if since.elapsed() > SETTLE_WAIT {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// ID's row for `partition` in its `Status`.
async fn row_on(
    args: &NodeArgs,
    url: &str,
    partition: &str,
) -> Result<Option<proto::LocalPartition>, Failure> {
    let (mut c, token) = connect(args, url).await?;
    c.status(request(StatusRequest::default(), token.as_deref())?)
        .await
        .map(|r| {
            r.into_inner()
                .partitions
                .into_iter()
                .find(|lp| lp.id == partition)
        })
        .map_err(|s| Failure::usage(&format!("{url}: {}", s.message())))
}

/// Takes node `to` out of `partition`'s replicas in `m`, through the writer, and waits for
/// it to close its replica copy (D328): what a promotion does first, and what a move,
/// relocate or restore to a node holding P's replica does before its stamp (S4). Nothing if
/// `to` isn't one of P's replicas. Whether it was.
pub(crate) async fn leave_replicas(
    args: &NodeArgs,
    m: &proto::ClusterMap,
    partition: &str,
    to: &str,
) -> Result<bool, Failure> {
    let Some(p) = m.partitions.iter().find(|p| p.id == partition) else {
        return Ok(false);
    };
    if !p.replicas.iter().any(|r| r == to) {
        return Ok(false);
    }
    let Some(url) = m
        .nodes
        .iter()
        .find(|n| n.id == to)
        .map(|n| url_of(args, &n.address))
    else {
        return Err(Failure::usage(&format!("the map has no node {to:?}")));
    };
    let mut m1 = m.clone();
    m1.map_version += 1;
    for q in m1.partitions.iter_mut().filter(|q| q.id == partition) {
        q.replicas.retain(|r| r != to);
    }
    set_map(args, m1, false, Vec::new()).await?;
    wait_closed(args, &url, partition, to).await?;
    Ok(true)
}

/// Waits for node `to` (at `url`) to close its replica copy of `partition`, which a map
/// without it in P's replicas closes.
async fn wait_closed(args: &NodeArgs, url: &str, partition: &str, to: &str) -> Result<(), Failure> {
    let deadline = tokio::time::Instant::now() + CLOSE_WAIT;
    loop {
        let now = row_on(args, url, partition).await?;
        if now.is_none_or(|lp| lp.role() != proto::PartitionRole::Replica) {
            return Ok(());
        }
        if tokio::time::Instant::now() > deadline {
            return Err(Failure::usage(&format!(
                "node {to:?} hasn't closed its replica copy of {partition} within {}s of the map \
                 that took it out of {partition}'s replicas: run this again",
                CLOSE_WAIT.as_secs()
            )));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The map as `--node` has it.
async fn map_of(args: &NodeArgs) -> Result<proto::ClusterMap, Failure> {
    let (mut c, token) = connect(args, &args.node).await?;
    Ok(c.get_map(request(GetMapRequest {}, token.as_deref())?)
        .await
        .map_err(|s| Failure::usage(&format!("{}: {}", args.node, s.message())))?
        .into_inner())
}

/// `phdb cluster promote`.
pub(crate) async fn promote(
    args: &NodeArgs,
    partition: &str,
    to: &str,
    old_unreachable: bool,
    timeout: Option<Duration>,
) -> Result<Report, Failure> {
    let m = map_of(args).await?;
    let Some(p) = m.partitions.iter().find(|p| p.id == partition).cloned() else {
        return Err(Failure::usage(&format!(
            "the map (version {}) has no partition {partition:?}",
            m.map_version
        )));
    };
    let url = |id: &str| {
        m.nodes
            .iter()
            .find(|n| n.id == id)
            .map(|n| url_of(args, &n.address))
    };
    if url(to).is_none() {
        return Err(Failure::usage(&format!("the map has no node {to:?}")));
    }
    let new_address = m
        .nodes
        .iter()
        .find(|n| n.id == to)
        .map(|n| n.address.clone())
        .unwrap_or_default();
    if p.node == to {
        // Done by an earlier run: still waited for, so the report means it serves (#2035).
        let new = At::new(args, to, &new_address).await?;
        serving(&new, partition, p.epoch, timeout).await?;
        let mut r = report(
            "cluster promote",
            vec![
                ("partition", json!(partition)),
                ("node", json!(to)),
                ("epoch", json!(p.epoch)),
                ("done", json!("the map already gives it this node")),
            ],
            Value::Null,
        );
        r.status = crate::ExitStatus::SUCCESS;
        return Ok(r);
    }
    let old_url = url(&p.node).unwrap_or_default();
    // The owner: fenced or closed, or unreachable (relocate's check, D317's S7 and S3).
    use proto::LocalPartitionState as S;
    let owner = if old_unreachable {
        if let Ok(Ok(Some(lp))) =
            tokio::time::timeout(Duration::from_secs(3), row_on(args, &old_url, partition)).await
            && lp.state() == S::Serving
        {
            return Err(Failure::usage(&format!(
                "--old-unreachable, but node {:?} answers and serves partition {partition}: \
                 fence it there first (phdb cluster fence), or stop its process",
                p.node
            )));
        }
        None
    } else {
        let lp = row_on(args, &old_url, partition).await?;
        if !lp
            .as_ref()
            .is_some_and(|lp| matches!(lp.state(), S::Fenced | S::Closed))
        {
            return Err(Failure::usage(&format!(
                "partition {partition} isn't fenced or closed on its owner, node {:?}: fence it \
                 there first (phdb cluster fence), or pass --old-unreachable if that node's \
                 process is stopped",
                p.node
            )));
        }
        lp
    };
    // A fenced owner takes no commit and still streams its feed (Follow is lease-gated,
    // not fence-gated, D328's M1). The operator's fence, once more, answers when the owner's
    // durable frontier is final (feed1248's S1 on #2184: a fed `Buffered` commit passes it
    // only at the owner's background sync); then wait, bounded, for the copy to hear it and
    // apply through it, so the loss counted below is exact (#2162 item 4: a promote right
    // after the fence counted from a frontier up to a heartbeat old). A closed owner doesn't
    // stream, and another fence (a lapsed lease, a start) isn't the operator's to set again:
    // their counts are lower bounds.
    let operator_fenced = owner.as_ref().is_some_and(|lp| {
        lp.state() == S::Fenced && lp.fence_reason() == proto::FenceReason::Operator
    });
    // Whether the fence again answered, and then whether the copy settled: each failure
    // says which step fell short (feed1248's N4 on #2184).
    let refenced =
        operator_fenced && url(to).is_some() && fence_again(args, &old_url, partition).await;
    let settled = match url(to) {
        Some(to_url) if refenced => settle(args, &to_url, partition).await,
        _ => false,
    };
    let ops = CliOps {
        args,
        map: &m,
        timeout,
    };
    let done = run(&ops, &m, partition, to, &How::Manual { old_unreachable }).await?;
    for w in &done.warnings {
        eprintln!("phdb cluster promote: warning: {w}");
    }
    let (applied, primary, replica) = (done.applied, done.primary, done.replica);
    let lost = if old_unreachable {
        json!(
            "unknown: the old owner couldn't be asked, so commits it acknowledged after the \
             copy's last one may be lost, however many"
        )
    } else if replica {
        json!(primary.saturating_sub(applied))
    } else {
        Value::Null
    };
    let mut r = report(
        "cluster promote",
        vec![
            ("partition", json!(partition)),
            ("from", json!(p.node)),
            ("node", json!(to)),
            ("epoch", json!(done.epoch)),
            ("applied_seqno", json!(applied)),
            ("owner_durable_seqno", json!(primary)),
            ("lost_commits", lost),
            (
                "lost_commits_counted",
                json!(if old_unreachable {
                    "not counted: the old owner couldn't be asked"
                } else if settled {
                    "exact: the owner's fence found its durable seqno final, and the copy \
                     heard it after that and applied through it"
                } else if owner.as_ref().is_some_and(|lp| lp.state() == S::Closed) {
                    "a lower bound: the owner was closed, so the copy last heard its durable \
                     seqno before that, and commits it made durable since aren't counted"
                } else if !operator_fenced {
                    "a lower bound: the owner's fence isn't the operator's (its lease lapsed, \
                     or it awaits its peers' map), so commits it made durable after the \
                     copy's last message aren't counted"
                } else if !refenced {
                    "a lower bound: fencing the owner again didn't answer within 3 s, so its \
                     durable seqno may not be final, and commits it made durable after the \
                     copy's last message aren't counted"
                } else {
                    "a lower bound: the fenced owner's final durable seqno didn't reach the \
                     copy within 3 s, so commits it made durable after the copy's last \
                     message aren't counted"
                }),
            ),
            ("map_version", json!(done.map_version)),
        ],
        Value::Null,
    );
    if !done.warnings.is_empty() {
        r.document.insert("warnings".into(), json!(done.warnings));
    }
    if owner.is_some_and(|lp| lp.state() == S::Fenced) {
        r.document.insert(
            "old_owner".into(),
            json!(
                "still holds its files, fenced: drop them there once checked (phdb cluster \
                 drop-partition)"
            ),
        );
    }
    Ok(r)
}

/// How a promote was asked for.
pub(crate) enum How {
    /// `phdb cluster promote`: the operator's checks of the old owner were made. With
    /// `old_unreachable` the owner couldn't be asked, so it may come back while the promote
    /// runs (#2152).
    Manual { old_unreachable: bool },
    /// The map's writer failing a down partition over (D330 PR 2): the in-doubt shares the
    /// old owner last reported, for the event, and the partition's failover count rises.
    Failover {
        in_doubt_xids: Vec<String>,
        in_doubt_count: u64,
    },
}

/// What a promote did.
#[derive(Debug)]
pub(crate) struct Promoted {
    /// The partition's epoch on its new node.
    pub(crate) epoch: u64,
    /// The copy's applied seqno and the owner's durable seqno as it last heard it.
    pub(crate) applied: u64,
    pub(crate) primary: u64,
    /// Whether the copy was still a replica when this run found it (not stamped by an
    /// earlier run).
    pub(crate) replica: bool,
    pub(crate) map_version: u64,
    /// What the promote couldn't check, for the caller to report (#2152: no node reported
    /// its lease, so the time between the surveys of the old owner's grants went unbounded).
    pub(crate) warnings: Vec<String>,
}

/// A planned map change: its version, and how many of its nodes hold it, the writer
/// included.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Set {
    pub(crate) version: u64,
    pub(crate) held: usize,
}

/// What a promote does to the cluster (D330 PR 2's F1), so `phdb cluster promote` and the
/// map's writer failing a partition over run one path: over the CLI's clients
/// ([`CliOps`]), or over the writer's peers (`failover.rs`).
pub(crate) trait Promoter {
    /// Node `id`'s `Status` row for `partition`.
    async fn row(
        &self,
        id: &str,
        partition: &str,
    ) -> Result<Option<proto::LocalPartition>, Failure>;
    /// The map as the writer has it.
    async fn map(&self) -> Result<proto::ClusterMap, Failure>;
    /// Sets `m` (its version already raised) as a planned change.
    async fn set_map(&self, m: proto::ClusterMap) -> Result<Set, Failure>;
    /// Now, in nanoseconds, on a clock that counts a suspend (the lease clock's).
    fn now(&self) -> u64;
    /// Every node of the map's `Status`, by id: `None` for one that didn't answer (#2152),
    /// each waited for `wait` at most (the writer's own survey waits less still).
    async fn survey(
        &self,
        wait: Duration,
    ) -> Result<HashMap<String, Option<proto::NodeStatus>>, Failure>;
    /// Waits `d` (#2152: for an unreachable owner's grants to run out).
    async fn sleep(&self, d: Duration) {
        tokio::time::sleep(d).await;
    }
    /// Stamps node `id`'s copy: its fork token, for a promote (D336; empty when unknown).
    async fn stamp(&self, id: &str, req: proto::StampRequest) -> Result<Vec<u8>, Failure>;
    /// Waits for node `id` to close its replica copy of `partition`.
    async fn wait_closed(&self, id: &str, partition: &str) -> Result<(), Failure>;
    /// Waits for node `id` to serve `partition` at `epoch`.
    async fn wait_serving(&self, id: &str, partition: &str, epoch: u64) -> Result<(), Failure>;
    /// A test's fault seam after the stamp, before the map.
    fn after_stamp(&self) -> Result<(), Failure> {
        Ok(())
    }
}

/// The epoch a promote of `p` to node `to` stamps its copy at (simp724's S2 on the D330
/// PR 2 plan), and whether it resumes a pending promote: the latest pending promote's
/// `to_epoch` when it's to `to`; otherwise one above P's epoch and every epoch a promote of
/// P has recorded, so a copy an unfinished promote stamped never matches a new history.
pub(crate) fn target_epoch(m: &proto::ClusterMap, p: &proto::Partition, to: &str) -> (u64, bool) {
    use proto::{FailoverKind as K, FailoverState as St};
    let of_p = || {
        m.failovers
            .iter()
            .filter(|e| e.kind() == K::Partition && e.partition == p.id && e.to_epoch > p.epoch)
    };
    if let Some(latest) = of_p()
        .filter(|e| e.state() == St::Pending)
        .max_by_key(|e| e.to_epoch)
        && latest.to == to
    {
        return (latest.to_epoch, true);
    }
    let floor = of_p().map(|e| e.to_epoch).max().unwrap_or(0).max(p.epoch);
    (floor + 1, false)
}

/// The highest epoch any promote of `partition` recorded in `m` (0: none): a restore, or any
/// other move of it, stamps above it, so a copy an unfinished promote stamped never matches
/// the new history (simp724's S2; found reviewing D331 PR 3a).
pub(crate) fn promote_floor(m: &proto::ClusterMap, partition: &str) -> u64 {
    m.failovers
        .iter()
        .filter(|e| e.kind() == proto::FailoverKind::Partition && e.partition == partition)
        .map(|e| e.to_epoch)
        .max()
        .unwrap_or(0)
}

/// P's next epoch for any change that raises it (a relocation, a move, a hand-over): one
/// above both its epoch and every epoch a promote of it recorded, so it never lands on a
/// copy an unfinished or abandoned promote stamped (#2070, simp724).
pub(crate) fn next_epoch(m: &proto::ClusterMap, p: &proto::Partition) -> u64 {
    p.epoch.max(promote_floor(m, &p.id)) + 1
}

/// Whether `e` is kept in `m` whatever the event count: pending, or abandoned while its
/// partition's epoch is at or below its `to_epoch`, where it's the floor (#2070,
/// simp724), or while a hand-over of the partition is in flight. Past that, P's epoch is
/// the floor itself. A done promote whose old owner's tail is still PENDING too (D336): the
/// event is the tail's record.
pub(crate) fn pinned(m: &proto::ClusterMap, e: &proto::FailoverEvent) -> bool {
    use proto::{FailoverKind as K, FailoverState as St};
    if e.tail_state() == proto::TailState::Pending {
        return true;
    }
    match e.state() {
        St::Pending => true,
        // While a hand-over of its partition is in flight, too: a source that crashed before
        // its stamp checks its file against the floor when it opens (ai1249's review).
        St::Abandoned => {
            e.kind() == K::Partition
                && (m
                    .partitions
                    .iter()
                    .any(|p| p.id == e.partition && p.epoch <= e.to_epoch)
                    || super::moves::handing_over(m, &e.partition).is_some())
        }
        _ => false,
    }
}

/// Appends `e` to `m`'s failover events, then drops the oldest finished ones past
/// [`super::map::FAILOVER_EVENTS`]; [`pinned`] ones are never dropped.
pub(crate) fn push_event(m: &mut proto::ClusterMap, e: proto::FailoverEvent) {
    m.failovers.push(e);
    let keep: Vec<bool> = m.failovers.iter().map(|e| pinned(m, e)).collect();
    let mut finished = keep.iter().filter(|k| !**k).count();
    let mut i = 0;
    m.failovers.retain(|_| {
        let pin = keep[i];
        i += 1;
        if finished > super::map::FAILOVER_EVENTS && !pin {
            finished -= 1;
            false
        } else {
            true
        }
    });
}

/// The wall clock in ms since the Unix epoch, for an event's `at_ms`.
fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Why `partition` can't fail over or be promoted in `m` at all now, if a range move (D331)
/// naming it, as its source or its target, is at HANDING_OVER: its fence and hand-over
/// run on the epochs it recorded, and #2160 bounds the phase. Past the commit point it no
/// longer holds the source (the coordinator's ruling on #2154): a promoted copy of S takes
/// the record with it, serves none of the handed-over range by the map, and drops its copy
/// at CLEANING. The target, past it, may move only to a copy that holds the copied range
/// ([`target_copy_holds`]).
pub(crate) fn range_move_holds(m: &proto::ClusterMap, partition: &str) -> Option<String> {
    let r = m.moves.iter().find(|r| {
        !super::map::node_move(r)
            && (r.from == partition || r.to == partition)
            && super::map::phase(r) == proto::MovePhase::HandingOver
    })?;
    Some(format!(
        "range move {} of table {:?} ({} to {}) is at its hand-over (HANDING_OVER): let it \
         finish or abort (phdb cluster move-range, or --abort {}), then promote",
        r.id, r.table, r.from, r.to, r.id
    ))
}

/// Whether replica copy `row` of `partition` may be promoted, or taken by a failover, under
/// `m`'s range moves into it past their commit point (#2154, the coordinator's ruling):
/// the range is the target's from HANDED_OVER, and the copy must provably hold all of it.
/// It does when its file shows the move's copy done and its catch-up past the source's
/// `final_seqno` (`Status` reads both from the copy's own progress rows, which the
/// target's commits carry with the copied data, so a replica that applied them holds the
/// data too). Otherwise the target stays where it is until the move ends.
pub(crate) fn target_copy_holds(
    m: &proto::ClusterMap,
    partition: &str,
    row: &proto::LocalPartition,
) -> Result<(), String> {
    for r in m.moves.iter().filter(|r| {
        !super::map::node_move(r)
            && r.to == partition
            && super::map::committed(super::map::phase(r))
    }) {
        let shown = row.moves.iter().find(|s| s.id == r.id);
        if !shown.is_some_and(|s| s.copy_done && s.source_applied_seqno >= r.final_seqno) {
            return Err(format!(
                "range move {} gave {partition} a range of table {:?} (past its hand-over), \
                 and this copy can't be shown to hold all of it (copy done: {}, caught up \
                 through {} of {}): {partition} stays on its owner until the move ends",
                r.id,
                r.table,
                shown.is_some_and(|s| s.copy_done),
                shown.map_or(0, |s| s.source_applied_seqno),
                r.final_seqno
            ));
        }
    }
    Ok(())
}

/// The pending promote of `p` in `m` that keeps `p`'s lease from its owner (#2152): the
/// latest PENDING partition event of it above its epoch, from its owner. Grantors give P's
/// owner no lease while it's there (`lease.rs`), so an owner that comes back mid-promote
/// can't take writes the promoted copy would lose.
pub(crate) fn pending_promote<'a>(
    m: &'a proto::ClusterMap,
    p: &proto::Partition,
) -> Option<&'a proto::FailoverEvent> {
    use proto::{FailoverKind as K, FailoverState as St};
    m.failovers
        .iter()
        .filter(|e| {
            e.kind() == K::Partition
                && e.partition == p.id
                && e.from == p.node
                && e.state() == St::Pending
                && e.to_epoch > p.epoch
        })
        .max_by_key(|e| e.to_epoch)
}

/// Why a promote of `p` must stop for its owner, by a survey of every node of `m` (#2152):
/// the owner serves P, or another node still grants it P's lease, or too few of the other
/// nodes can be asked to tell. An owner's lease needs a majority, at most one grant of it its
/// own, so a survey that hears all but `majority − 2` of the others (none restarted, since a
/// restart forgets its grants: D326's start grace) sees at least one grant of any lease it
/// holds. `None`: it holds none.
pub(crate) fn owner_back(
    m: &proto::ClusterMap,
    p: &proto::Partition,
    statuses: &HashMap<String, Option<proto::NodeStatus>>,
) -> Option<String> {
    let owner = statuses.get(&p.node).and_then(Option::as_ref);
    if super::failover::serves(p, owner) {
        return Some(format!(
            "node {:?}, {}'s owner, answers and serves it",
            p.node, p.id
        ));
    }
    // An owner that answers and holds no lease of P says so itself: a lease it took between
    // two surveys within a lease of each other would still run at the second, so the other
    // nodes needn't all be asked (only grants they do report count, below).
    let unleased = owner.is_some_and(|st| {
        !st.partitions.iter().any(|lp| {
            lp.id == p.id
                && lp.role() != proto::PartitionRole::Replica
                && lp.epoch == p.epoch
                && lp.lease_remaining_ms > 0
        })
    });
    let mut unknown = Vec::new();
    for n in m.nodes.iter().filter(|n| n.id != p.node) {
        let Some(Some(st)) = statuses.get(&n.id) else {
            unknown.push(format!("{} didn't answer", n.id));
            continue;
        };
        if let Some(g) = st
            .grants
            .iter()
            .find(|g| g.partition == p.id && g.holder == p.node && g.remaining_ms > 0)
        {
            return Some(format!(
                "node {} grants {}'s lease to its owner, node {:?} ({} ms left): the owner \
                 holds it or renewed it",
                n.id, p.id, p.node, g.remaining_ms
            ));
        }
        if st.grace_remaining_ms > 0 {
            unknown.push(format!("{} restarted", n.id));
        }
    }
    let majority = m.nodes.len() / 2 + 1;
    (!unleased && unknown.len() + 2 > majority).then(|| {
        format!(
            "can't tell whether {}'s owner, node {:?}, took its lease back: {} ({} of the \
             other nodes can't be asked, and a lease needs only {} of them)",
            p.id,
            p.node,
            unknown.join(", "),
            unknown.len(),
            majority - 1
        )
    })
}

/// The longest time left on another node's grant of `p`'s lease to its owner, in
/// `statuses` (#2152): `None` when none is live.
fn owner_grant_left(
    p: &proto::Partition,
    statuses: &HashMap<String, Option<proto::NodeStatus>>,
) -> Option<Duration> {
    statuses
        .iter()
        .filter(|(id, _)| **id != p.node)
        .filter_map(|(_, st)| st.as_ref())
        .flat_map(|st| &st.grants)
        .filter(|g| g.partition == p.id && g.holder == p.node && g.remaining_ms > 0)
        .map(|g| Duration::from_millis(g.remaining_ms))
        .max()
}

/// The cluster's lease as `statuses` report it (D326: every node runs with one), the
/// smallest reported; `None` when no node reports it (an older build). With the node ids
/// that didn't.
pub(crate) fn surveyed_lease(
    statuses: &HashMap<String, Option<proto::NodeStatus>>,
) -> (Option<Duration>, Vec<String>) {
    let mut lease = None;
    let mut silent = Vec::new();
    for (id, st) in statuses {
        match st.as_ref().map(|st| st.lease_ms) {
            Some(0) => silent.push(id.clone()),
            Some(ms) => {
                let d = Duration::from_millis(ms);
                lease = Some(lease.map_or(d, |l: Duration| l.min(d)));
            }
            None => {}
        }
    }
    silent.sort();
    (lease, silent)
}

/// Abandons the pending promote of `partition` to `to` at `target` before its stamp
/// (#2152): the event ABANDONED, and `to` back in P's replicas, since its copy is still
/// unstamped. Waits for that copy to close first, so the node opens it again from the new
/// map rather than finishing a close begun on the old one. Adding a replica needs every
/// node to answer (D328's map-feature check), so with one down the promote is abandoned
/// without it (#2171). The map version, and whether `to` is back among the replicas.
async fn abandon_unstamped(
    ops: &impl Promoter,
    partition: &str,
    to: &str,
    target: u64,
) -> Result<(u64, bool), Failure> {
    use proto::{FailoverKind as K, FailoverState as St};
    let _ = ops.wait_closed(to, partition).await;
    let base = ops.map().await?;
    let mut m = base.clone();
    m.map_version += 1;
    let mut abandoned = false;
    for e in m.failovers.iter_mut().filter(|e| {
        e.kind() == K::Partition
            && e.partition == partition
            && e.to == to
            && e.to_epoch == target
            && e.state() == St::Pending
    }) {
        e.set_state(St::Abandoned);
        abandoned = true;
    }
    let mut back = m.clone();
    let mut readd = false;
    for q in back.partitions.iter_mut().filter(|q| q.id == partition) {
        if q.node != to && !q.replicas.iter().any(|r| r == to) {
            q.replicas.push(to.to_owned());
            readd = true;
        }
    }
    if readd {
        if let Ok(set) = ops.set_map(back).await {
            return Ok((set.version, true));
        }
    } else if !abandoned {
        return Ok((base.map_version, true));
    }
    if !abandoned {
        return Ok((base.map_version, false));
    }
    Ok((ops.set_map(m).await?.version, false))
}

/// How long each survey after the first waits for a node (#2171): a sixth of the lease the
/// first one learned, so two surveys and the first map between them (each push within
/// `--cluster-push-timeout-ms`, at most a third of it) fit inside the bound with a node
/// down. A connect to a stopped node took about 2 s on Windows, past a short lease.
fn survey_wait(statuses: &HashMap<String, Option<proto::NodeStatus>>) -> Duration {
    surveyed_lease(statuses)
        .0
        .map_or(SURVEY_WAIT, |l| (l / 6).min(SURVEY_WAIT))
}

/// Promotes node `to`'s copy of `partition` in `m` (D328 PR 3, with D330 PR 2's pending
/// events): the steps after the caller's checks of the old owner.
/// 1. `to` leaves P's replicas, so it closes its copy; with writer terms the same map
///    records the promote pending at its epoch ([`target_epoch`]), before any stamp. When
///    the old owner couldn't be asked (a failover, or `--old-unreachable`), that map must
///    reach a majority, and surveys of P's grants before and after it must find none to the
///    owner, within a lease of each other (#2152); else the promote stops, abandoned.
/// 2. The copy is promoted and stamped at that epoch.
/// 3. The map gives P to `to` there, with a new history, and marks the promote done.
/// 4. It waits for `to` to serve P.
///
/// Run again, it finishes one that stopped.
pub(crate) async fn run(
    ops: &impl Promoter,
    m: &proto::ClusterMap,
    partition: &str,
    to: &str,
    how: &How,
) -> Result<Promoted, Failure> {
    use proto::{FailoverKind as K, FailoverState as St};
    let Some(p) = m.partitions.iter().find(|p| p.id == partition).cloned() else {
        return Err(Failure::usage(&format!(
            "the map (version {}) has no partition {partition:?}",
            m.map_version
        )));
    };
    if let Some(why) = range_move_holds(m, partition) {
        return Err(Failure::usage(&why));
    }
    // ID's copy: open and following (its row a replica's), or closed by an earlier run (its
    // file's own row), or stamped by one (at the target epoch).
    let row = ops.row(to, partition).await?.ok_or_else(|| {
        Failure::usage(&format!(
            "node {to:?} has no copy of partition {partition}: add it to {partition}'s \
             replicas (phdb cluster set-map) and let it follow first"
        ))
    })?;
    // Every node keeps the failover fields once the map has a writer term or failover on
    // (each is set only then): the promote is recorded pending, at an epoch above every
    // pending one's. Otherwise, as before D330, at the map's epoch + 1.
    let terms = m.writer_term > 0 || m.failover;
    let (target, resumes) = if terms {
        target_epoch(m, &p, to)
    } else {
        (p.epoch + 1, false)
    };
    let replica = row.role() == proto::PartitionRole::Replica;
    let stamped = !replica && row.epoch == target;
    // #2154: a copy taking over a range move's target must hold the copied range. A copy
    // past this check (closed or stamped by an earlier run) was checked then, and no move
    // can commit its hand-over while this promote is pending (map.rs's move steps, cli692's
    // Q1 on #2209), so no range moved into P since.
    if replica && let Err(why) = target_copy_holds(m, partition, &row) {
        return Err(Failure::usage(&why));
    }
    // The copy's identity epoch, the owner's at its bootstrap (`e_b`, D328's amendment): its
    // history began at or before it, and the map's epoch is at or past it.
    let e_b = row.epoch;
    if !stamped && !(p.history_epoch <= e_b && e_b <= p.epoch) {
        return Err(Failure::usage(&format!(
            "node {to:?}'s copy of {partition} is at epoch {e_b}, outside its history \
             ({} to the map's {}): it's from before a relocate without copy or a restore, so it \
             bootstraps again first; promote it once it follows",
            p.history_epoch, p.epoch
        )));
    }
    let (applied, primary) = (row.replica_applied_seqno, row.replica_primary_seqno);
    // #2152: an owner that couldn't be asked may come back while this runs, renew its lease
    // and take writes the copy would never have. Until the copy is stamped, P's grants are
    // surveyed before step 1's map and again once that map, whose pending promote stops
    // every grant of P to its owner, is on a majority.
    let guard = !stamped
        && match how {
            How::Manual { old_unreachable } => *old_unreachable,
            How::Failover { .. } => true,
        };
    let mut warnings = Vec::new();
    let first = if guard {
        let mut t0 = ops.now();
        let mut statuses = ops.survey(SURVEY_WAIT).await?;
        // A first survey that waited longer on a node that's down than the later ones
        // will is taken again, at their wait, to start the clock (#2171).
        let wait = survey_wait(&statuses);
        if Duration::from_nanos(ops.now().saturating_sub(t0)) > wait {
            t0 = ops.now();
            statuses = ops.survey(wait).await?;
        }
        // `--old-unreachable` right after the owner stopped: the others' grants to it end
        // within their time left, so the promote waits them out, two leases at most (a
        // stopping owner may renew once more on its way down). Grants still live after that
        // are an owner that renews: alive, so the promote doesn't start.
        if matches!(how, How::Manual { .. }) {
            let began = t0;
            while let Some(left) = owner_grant_left(&p, &statuses) {
                let lease = surveyed_lease(&statuses).0.unwrap_or(left);
                let waited = Duration::from_nanos(ops.now().saturating_sub(began));
                if waited >= lease * 2 {
                    break;
                }
                ops.sleep(left + GRANT_SLACK).await;
                t0 = ops.now();
                statuses = ops.survey(wait).await?;
            }
        }
        if let Some(why) = owner_back(m, &p, &statuses) {
            return Err(Failure::usage(&format!(
                "{why}: the promote of {partition} to node {to:?} didn't start"
            )));
        }
        Some((t0, surveyed_lease(&statuses).0, wait))
    } else {
        None
    };
    // 1. ID leaves P's replicas, so it closes its copy (D328 PR 1's sync); with writer terms
    // the same map records the promote pending at `target`, before the stamp (S2). A copy
    // still open while `to` is out of P's replicas is an earlier run's, closing (simp724's
    // S1 on #2007): it's waited for, not left again.
    // Only a copy still a replica leaves the replicas here: a stamped one whose node is back
    // in them was put there since (a set-map), which step 3 refuses rather than undoes
    // (simp724's M1 on #2007).
    let leave = replica && p.replicas.iter().any(|r| r == to);
    let record = terms && !resumes && !stamped;
    // With the guard, a map is set even with nothing to change (a rerun's), so the pending
    // promote is known to be on a majority before the stamp (#2159's item 2).
    if leave || record || guard {
        let mut m1 = m.clone();
        m1.map_version += 1;
        if leave {
            for q in m1.partitions.iter_mut().filter(|q| q.id == partition) {
                q.replicas.retain(|r| r != to);
            }
        }
        if record {
            for e in m1.failovers.iter_mut().filter(|e| {
                e.kind() == K::Partition && e.partition == partition && e.state() == St::Pending
            }) {
                e.set_state(St::Abandoned);
            }
            let (in_doubt_xids, in_doubt_count) = match how {
                How::Manual { .. } => (Vec::new(), 0),
                How::Failover {
                    in_doubt_xids,
                    in_doubt_count,
                } => (in_doubt_xids.clone(), *in_doubt_count),
            };
            let mut e = proto::FailoverEvent {
                partition: partition.to_owned(),
                from: p.node.clone(),
                to: to.to_owned(),
                at_ms: wall_ms(),
                writer: super::schema::writer(m).to_owned(),
                writer_term: m.writer_term,
                applied_seqno: applied,
                primary_seqno: primary,
                in_doubt_xids,
                in_doubt_count,
                to_epoch: target,
                ..proto::FailoverEvent::default()
            };
            e.set_kind(K::Partition);
            e.set_state(St::Pending);
            push_event(&mut m1, e);
        }
        let set = ops.set_map(m1).await?;
        let majority = m.nodes.len() / 2 + 1;
        if guard && set.held < majority {
            return Err(Failure::usage(&format!(
                "map version {} reached {} of the map's {} nodes, not a majority: the promote \
                 of {partition} to node {to:?} stays pending, unstamped, for a later run",
                set.version,
                set.held,
                m.nodes.len()
            )));
        }
    }
    if let Some((t0, lease, wait)) = first {
        let statuses = ops.survey(wait).await?;
        let now = ops.now();
        let (reported, silent) = surveyed_lease(&statuses);
        let why = owner_back(m, &p, &statuses).or_else(|| match lease.or(reported) {
            // A grant the owner took after the first survey runs a lease on its grantor's
            // clock, so the second survey still sees it if it ends within L·(1−ρ)/(1+ρ) on
            // this one (D326's holder bound).
            Some(l) if now > super::lease::holder_end(t0, l) => Some(format!(
                "the two surveys of {partition}'s grants took {} ms, more than a lease of {} \
                 ms allows: a grant its owner took between them may have run out unseen",
                (now - t0) / 1_000_000,
                l.as_millis()
            )),
            Some(_) => None,
            None => {
                warnings.push(format!(
                    "no node reported its lease (an older build: {}), so the time between \
                     the surveys of {partition}'s grants to its owner wasn't bounded",
                    silent.join(", ")
                ));
                None
            }
        });
        if let Some(why) = why {
            let (v, back) = abandon_unstamped(ops, partition, to, target).await?;
            let replicas = if back {
                format!("node {to:?} is one of its replicas again")
            } else {
                format!(
                    "add node {to:?} back to {partition}'s replicas (phdb cluster set-map) once \
                     every node answers"
                )
            };
            return Err(Failure::usage(&format!(
                "{why}. The promote of {partition} to node {to:?} is abandoned at map version \
                 {v}, before its stamp: {replicas}"
            )));
        }
    }
    if replica {
        ops.wait_closed(to, partition).await?;
    }
    // 2. The copy, promoted and stamped at the target epoch. A copy an earlier run stamped
    // is stamped again, which does nothing but answer its fork token (D336, Amendment 4: an
    // earlier run's reply may have been lost). That's reached only while the event is still
    // PENDING, so before the map gives `to` the partition: a stamp is refused while P is open
    // on `to` (cli692's Q1 on #2239).
    let fork = {
        ops.stamp(
            to,
            proto::StampRequest {
                cluster_id: m.cluster_id.clone(),
                partition: partition.to_owned(),
                from_epoch: p.epoch,
                to_epoch: target,
                restore: false,
                restore_from_cluster_id: String::new(),
                promote: true,
                replica_epoch: e_b,
                create: false,
                create_move: String::new(),
                create_version: 0,
            },
        )
        .await?
    };
    ops.after_stamp()?;
    // 3. The map: P on ID at the target epoch, with a new history (no `copies`), from the
    // map as it is now (step 1 changed it). The map as this promote read it: P still on its
    // owner at its epoch, and `to` not back in its replicas. Anything else (a set-map,
    // relocate or promote since) is not overwritten (simp724's M1 on #2007).
    let mut m2 = ops.map().await?;
    let Some(now) = m2.partitions.iter().find(|q| q.id == partition) else {
        return Err(Failure::usage(&format!(
            "the map (version {}) no longer has partition {partition}",
            m2.map_version
        )));
    };
    // The writer abandoned this promote since step 1: P's owner came back and serves
    // (#2070, simp724). The stamped copy is an orphan to drop.
    if m2.failovers.iter().any(|e| {
        e.kind() == K::Partition
            && e.partition == partition
            && e.to == to
            && e.to_epoch == target
            && e.state() == St::Abandoned
    }) {
        return Err(Failure::usage(&format!(
            "the promote of {partition} to node {to:?} at epoch {target} was abandoned: its \
             owner, node {:?}, came back and serves it. Node {to:?}'s copy stamped at epoch \
             {target} must be dropped there (phdb cluster drop-partition {partition} on node \
             {to:?})",
            p.node
        )));
    }
    // A range move that reached its hand-over since step 1 keeps P's epoch: the promote
    // stays pending, for a run after the move.
    if let Some(why) = range_move_holds(&m2, partition) {
        return Err(Failure::usage(&why));
    }
    if now.node != p.node || now.epoch != p.epoch || now.replicas.iter().any(|r| r == to) {
        return Err(Failure::usage(&format!(
            "the map changed during the promote: {partition} is on node {:?} at epoch {} in \
             map version {} (this promote read node {:?} at epoch {}), and node {to:?}'s copy is \
             stamped at epoch {target}. Check the map, then run the promote again or move that \
             file aside (phdb cluster drop-partition {partition} on node {to:?})",
            now.node, now.epoch, m2.map_version, p.node, p.epoch
        )));
    }
    m2.map_version += 1;
    // A whole-partition move of P not yet at its commit point stops here (D332 PR 2,
    // simp724's M2): this map gives P another node and epoch, so its copy on the target is
    // stale. Its record goes in the same map.
    m2.moves.retain(|r| {
        !(super::map::node_move(r)
            && r.from == partition
            && !super::map::committed(super::map::phase(r)))
    });
    for q in m2.partitions.iter_mut().filter(|q| q.id == partition) {
        q.node = to.to_owned();
        q.epoch = target;
        q.replicas.retain(|r| r != to);
        if matches!(how, How::Failover { .. }) {
            q.failover_count += 1;
        }
    }
    for e in m2.failovers.iter_mut().filter(|e| {
        e.kind() == K::Partition
            && e.partition == partition
            && e.to == to
            && e.to_epoch == target
            && e.state() == St::Pending
    }) {
        e.set_state(St::Done);
    }
    // D336: the old owner's tail, from the fork on, in the event this map finishes. Without
    // a fork token the event keeps none of its fields (UNSPECIFIED, which reads as NONE).
    // A map with them needs every node to keep them (`tail_replay`): when one doesn't, the
    // map is set without them, since the promote must go on either way.
    let plain = m2.clone();
    if fork.is_empty() {
        warnings.push(format!(
            "node {to:?}'s copy of {partition} reported no fork token (its promote ran on an \
             older build, or its token was lost): the old owner's tail can't be replayed"
        ));
    } else {
        for e in m2.failovers.iter_mut().filter(|e| {
            e.kind() == K::Partition
                && e.partition == partition
                && e.to == to
                && e.to_epoch == target
                && e.state() == St::Done
        }) {
            let tail = tail_state(how, e);
            e.set_tail_state(tail);
            if tail == proto::TailState::Pending {
                e.fork_token = fork.clone().into();
                e.from_history_epoch = p.history_epoch;
                e.from_epoch = p.epoch;
            }
        }
        discard_past_cap(&mut m2, partition);
    }
    let map_version = match ops.set_map(m2).await {
        // The install's `tail_replay` gate, by its whole refusal (cli692's Q2 on #2239): a
        // map set's error reaches here as text alone (both paths make it a usage failure).
        Err(f) if !fork.is_empty() && f.message().contains(&super::tail_replay_refusal()) => {
            warnings.push(format!(
                "the cluster hasn't adopted map feature {:?} (ClusterMap.adopted_features), \
                 and not every node answered with it now: {partition}'s old owner's tail isn't \
                 recorded. Once every node runs a build with it, set any map while they all \
                 answer (phdb cluster set-map), so a later promote records its tail",
                super::TAIL_REPLAY_FEATURE
            ));
            ops.set_map(plain).await?.version
        }
        r => r?.version,
    };
    // 4. Serving on ID, as a move waits for its new node (#2035): done means P takes reads
    // and writes there.
    ops.wait_serving(to, partition, target).await?;
    Ok(Promoted {
        epoch: target,
        applied,
        primary,
        replica,
        map_version,
        warnings,
    })
}

/// The most old owners' tails one partition keeps PENDING (D336, the owner's Q5).
pub(crate) const PENDING_TAILS: usize = 4;

/// D336: past [`PENDING_TAILS`] PENDING tails of `partition` in `m`, the oldest are
/// DISCARDED, their fork tokens cleared: the cap bounds the map, and a partition that failed
/// over that often without a replay has bigger problems. Status shows each discarded one.
pub(crate) fn discard_past_cap(m: &mut proto::ClusterMap, partition: &str) {
    let pending = m
        .failovers
        .iter()
        .filter(|e| e.partition == partition && e.tail_state() == proto::TailState::Pending)
        .count();
    let mut over = pending.saturating_sub(PENDING_TAILS);
    // The map's events are in the order they were recorded: the oldest come first.
    for e in m.failovers.iter_mut() {
        if over == 0 {
            break;
        }
        if e.partition == partition && e.tail_state() == proto::TailState::Pending {
            e.set_tail_state(proto::TailState::Discarded);
            e.fork_token = Default::default();
            over -= 1;
        }
    }
}

/// D336 (#2148): whether a promote leaves an old owner's tail to replay. Kept (PENDING)
/// when the old owner may hold commits the copy lacks: a writer failover, a promote with the
/// old owner unreachable, or a reachable one whose durable seqno, as the copy last heard it,
/// is past what the copy applied or unknown. Called only with a fork token: without one
/// there's nothing a replay could start from, and the event records no tail.
fn tail_state(how: &How, e: &proto::FailoverEvent) -> proto::TailState {
    let may_hold = match how {
        How::Failover { .. }
        | How::Manual {
            old_unreachable: true,
        } => true,
        How::Manual {
            old_unreachable: false,
        } => e.primary_seqno == 0 || e.primary_seqno > e.applied_seqno,
    };
    if may_hold {
        proto::TailState::Pending
    } else {
        proto::TailState::None
    }
}

/// [`Promoter`] over the CLI's clients (`phdb cluster promote`).
struct CliOps<'a> {
    args: &'a NodeArgs,
    map: &'a proto::ClusterMap,
    timeout: Option<Duration>,
}

impl CliOps<'_> {
    fn address(&self, id: &str) -> Result<String, Failure> {
        self.map
            .nodes
            .iter()
            .find(|n| n.id == id)
            .map(|n| n.address.clone())
            .ok_or_else(|| Failure::usage(&format!("the map has no node {id:?}")))
    }
}

impl Promoter for CliOps<'_> {
    async fn row(
        &self,
        id: &str,
        partition: &str,
    ) -> Result<Option<proto::LocalPartition>, Failure> {
        row_on(self.args, &url_of(self.args, &self.address(id)?), partition).await
    }

    async fn map(&self) -> Result<proto::ClusterMap, Failure> {
        map_of(self.args).await
    }

    async fn set_map(&self, m: proto::ClusterMap) -> Result<Set, Failure> {
        let set = set_map(self.args, m, false, Vec::new()).await?;
        let version = set
            .document
            .get("map_version")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        // The writer's pushes to the others; it holds the map itself.
        let pushed = set
            .document
            .get("pushed")
            .and_then(Value::as_array)
            .map_or(0, |a| {
                a.iter()
                    .filter(|p| p.get("ok").and_then(Value::as_bool) == Some(true))
                    .count()
            });
        Ok(Set {
            version,
            held: pushed + 1,
        })
    }

    fn now(&self) -> u64 {
        pigeonhole_io::sys::boot_clock_ns()
    }

    async fn survey(
        &self,
        wait: Duration,
    ) -> Result<HashMap<String, Option<proto::NodeStatus>>, Failure> {
        // In parallel, so a node that doesn't answer costs one wait, not one each: the
        // time between the two surveys is bounded.
        let wait = wait.min(SURVEY_WAIT);
        let mut asks = tokio::task::JoinSet::new();
        for n in &self.map.nodes {
            let (args, id) = (self.args.clone(), n.id.clone());
            let url = url_of(self.args, &n.address);
            asks.spawn(async move {
                let st = tokio::time::timeout(wait, async {
                    let (mut c, token) = connect(&args, &url).await.ok()?;
                    c.status(request(StatusRequest::default(), token.as_deref()).ok()?)
                        .await
                        .ok()
                        .map(tonic::Response::into_inner)
                })
                .await
                .ok()
                .flatten();
                (id, st)
            });
        }
        Ok(asks.join_all().await.into_iter().collect())
    }

    async fn stamp(&self, id: &str, req: proto::StampRequest) -> Result<Vec<u8>, Failure> {
        let url = url_of(self.args, &self.address(id)?);
        let (mut c, token) = connect(self.args, &url).await?;
        let lp = c
            .stamp(request(req, token.as_deref())?)
            .await
            .map_err(|s| Failure::usage(&format!("{url}: stamp: {}", s.message())))?
            .into_inner();
        Ok(lp.fork_token.to_vec())
    }

    async fn wait_closed(&self, id: &str, partition: &str) -> Result<(), Failure> {
        wait_closed(
            self.args,
            &url_of(self.args, &self.address(id)?),
            partition,
            id,
        )
        .await
    }

    async fn wait_serving(&self, id: &str, partition: &str, epoch: u64) -> Result<(), Failure> {
        let new = At::new(self.args, id, &self.address(id)?).await?;
        serving(&new, partition, epoch, self.timeout).await
    }

    fn after_stamp(&self) -> Result<(), Failure> {
        #[cfg(test)]
        {
            let mut armed = STOP_AFTER_STAMP
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(i) = armed.iter().position(|n| *n == self.args.node) {
                armed.swap_remove(i);
                return Err(Failure::usage("the test's fault after the promote's stamp"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::{ClusterMap, FailoverEvent, FailoverKind, FailoverState as S, Partition};

    fn ev(p: &str, to: &str, to_epoch: u64, state: S) -> FailoverEvent {
        let mut e = FailoverEvent {
            partition: p.into(),
            to: to.into(),
            to_epoch,
            ..Default::default()
        };
        e.set_kind(FailoverKind::Partition);
        e.set_state(state);
        e
    }

    fn with_p3(epoch: u64) -> ClusterMap {
        ClusterMap {
            partitions: vec![Partition {
                id: "p3".into(),
                node: "c".into(),
                epoch,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn a_promote_stamps_above_every_epoch_a_promote_of_its_partition_recorded() {
        // simp724's S2: a copy an unfinished promote stamped is never at the new epoch.
        let mut m = with_p3(5);
        let at = |m: &ClusterMap, to: &str| target_epoch(m, &m.partitions[0].clone(), to);
        assert_eq!(at(&m, "b"), (6, false));
        // Pending to b: b resumes it; another node goes above it.
        m.failovers.push(ev("p3", "b", 6, S::Pending));
        assert_eq!(at(&m, "b"), (6, true));
        assert_eq!(at(&m, "a"), (7, false));
        // A later promote to a took over: b's is abandoned, its epoch still a floor.
        m.failovers[0].set_state(S::Abandoned);
        m.failovers.push(ev("p3", "a", 7, S::Pending));
        assert_eq!(at(&m, "a"), (7, true));
        assert_eq!(at(&m, "b"), (8, false));
        // Another partition's events, and those at or below p3's epoch, don't count.
        let mut m = with_p3(5);
        m.failovers.push(ev("p9", "b", 9, S::Pending));
        m.failovers.push(ev("p3", "b", 5, S::Done));
        assert_eq!(at(&m, "b"), (6, false));
    }

    #[test]
    fn a_restore_stamps_above_every_epoch_a_promote_of_its_partition_recorded() {
        // Found reviewing D331 PR 3a: a restore's new epoch takes the same floor.
        let mut m = with_p3(5);
        assert_eq!(promote_floor(&m, "p3"), 0);
        m.failovers.push(ev("p3", "b", 6, S::Abandoned));
        m.failovers.push(ev("p3", "a", 8, S::Pending));
        m.failovers.push(ev("p9", "a", 20, S::Pending));
        assert_eq!(promote_floor(&m, "p3"), 8);
    }

    // ---- #2152: an owner back between a promote's first map and its last ----

    use proto::{LeaseWithheld, LocalPartition, NodeStatus};
    use std::cell::{Cell, RefCell};

    type Survey = HashMap<String, Option<NodeStatus>>;

    /// A scripted [`Promoter`]: each survey answers the next of `surveys`, each `set_map`
    /// reaches `held` nodes, and each clock read moves `step` ns on. Nothing waits.
    struct Fake {
        map: RefCell<ClusterMap>,
        surveys: RefCell<Vec<Survey>>,
        held: usize,
        clock: Cell<u64>,
        step: u64,
        stamped: Cell<bool>,
        /// The next run stops after its stamp, before its map (as `STOP_AFTER_STAMP`).
        stop_after_stamp: Cell<bool>,
    }

    impl Promoter for Fake {
        async fn row(&self, _id: &str, partition: &str) -> Result<Option<LocalPartition>, Failure> {
            // b's copy: a replica from its bootstrap at epoch 3, until it's stamped at 4.
            let mut lp = LocalPartition {
                id: partition.into(),
                epoch: 3,
                replica_applied_seqno: 7,
                ..Default::default()
            };
            if self.stamped.get() {
                lp.epoch = 4;
                lp.set_role(proto::PartitionRole::Owner);
            } else {
                lp.set_role(proto::PartitionRole::Replica);
            }
            Ok(Some(lp))
        }

        async fn map(&self) -> Result<ClusterMap, Failure> {
            Ok(self.map.borrow().clone())
        }

        async fn set_map(&self, m: ClusterMap) -> Result<Set, Failure> {
            let version = m.map_version;
            *self.map.borrow_mut() = m;
            Ok(Set {
                version,
                held: self.held,
            })
        }

        fn now(&self) -> u64 {
            let t = self.clock.get() + self.step;
            self.clock.set(t);
            t
        }

        async fn sleep(&self, d: Duration) {
            self.clock
                .set(self.clock.get() + u64::try_from(d.as_nanos()).unwrap());
        }

        async fn survey(&self, _wait: Duration) -> Result<Survey, Failure> {
            let mut s = self.surveys.borrow_mut();
            assert!(!s.is_empty(), "a survey the test didn't expect");
            Ok(s.remove(0))
        }

        async fn stamp(&self, _id: &str, _req: proto::StampRequest) -> Result<Vec<u8>, Failure> {
            self.stamped.set(true);
            Ok(b"fork".to_vec())
        }

        async fn wait_closed(&self, _id: &str, _partition: &str) -> Result<(), Failure> {
            Ok(())
        }

        async fn wait_serving(&self, _id: &str, _p: &str, _epoch: u64) -> Result<(), Failure> {
            Ok(())
        }

        fn after_stamp(&self) -> Result<(), Failure> {
            if self.stop_after_stamp.replace(false) {
                return Err(Failure::usage("the test's stop after the stamp"));
            }
            Ok(())
        }
    }

    /// Runs `f` to its end: the fake never waits, so it's ready at once.
    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        let mut f = std::pin::pin!(f);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        match f.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(v) => v,
            std::task::Poll::Pending => panic!("the fake promote waited"),
        }
    }

    /// Three nodes with writer terms; p3 on c at epoch 3, b its replica.
    fn three() -> ClusterMap {
        ClusterMap {
            writer_term: 1,
            map_version: 10,
            nodes: ["a", "b", "c"]
                .map(|id| proto::Node {
                    id: id.into(),
                    ..Default::default()
                })
                .to_vec(),
            partitions: vec![Partition {
                id: "p3".into(),
                node: "c".into(),
                epoch: 3,
                history_epoch: 1,
                replicas: vec!["b".into()],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// a and b answer with no grant and `lease_ms`; c, the owner, doesn't answer.
    fn quiet(lease_ms: u64) -> Survey {
        let mut s: Survey = ["a", "b"]
            .map(|id| {
                let st = NodeStatus {
                    node_id: id.into(),
                    lease_ms,
                    ..Default::default()
                };
                (id.to_owned(), Some(st))
            })
            .into_iter()
            .collect();
        s.insert("c".into(), None);
        s
    }

    fn fake(surveys: Vec<Survey>, held: usize, step: Duration) -> Fake {
        Fake {
            map: RefCell::new(three()),
            surveys: RefCell::new(surveys),
            held,
            clock: Cell::new(0),
            step: u64::try_from(step.as_nanos()).unwrap(),
            stamped: Cell::new(false),
            stop_after_stamp: Cell::new(false),
        }
    }

    /// D336 (cli692's Q1 on #2239): a promote stopped after its stamp, before its map, is run
    /// again. The rerun finds the copy stamped, stamps it again (which only answers its fork
    /// token), and records the token and a PENDING tail in the event its map finishes.
    #[test]
    fn a_promote_rerun_after_its_stamp_records_the_fork_token() {
        let f = fake(
            vec![quiet(10_000), quiet(10_000)],
            2,
            Duration::from_secs(1),
        );
        f.stop_after_stamp.set(true);
        block_on(run(&f, &three(), "p3", "b", &failover())).unwrap_err();
        assert!(f.stamped.get());
        assert_eq!(f.map.borrow().failovers[0].state(), S::Pending);
        let m = f.map.borrow().clone();
        block_on(run(&f, &m, "p3", "b", &failover())).unwrap();
        let m = f.map.borrow();
        let e = &m.failovers[0];
        assert_eq!(e.state(), S::Done);
        assert_eq!(e.tail_state(), proto::TailState::Pending);
        assert_eq!(&e.fork_token[..], b"fork");
        assert_eq!((e.from_history_epoch, e.from_epoch), (1, 3));
    }

    fn failover() -> How {
        How::Failover {
            in_doubt_xids: Vec::new(),
            in_doubt_count: 0,
        }
    }

    #[test]
    fn a_failover_finishes_when_both_surveys_find_no_grant_to_the_owner() {
        let f = fake(
            vec![quiet(10_000), quiet(10_000)],
            2,
            Duration::from_secs(1),
        );
        let done = block_on(run(&f, &three(), "p3", "b", &failover())).unwrap();
        assert!(done.warnings.is_empty(), "{:?}", done.warnings);
        assert!(f.stamped.get());
        let m = f.map.borrow();
        assert_eq!(
            (m.partitions[0].node.as_str(), m.partitions[0].epoch),
            ("b", 4)
        );
        assert_eq!(m.failovers[0].state(), S::Done);
    }

    #[test]
    fn a_failover_whose_owner_took_a_grant_before_its_first_map_landed_is_abandoned_unstamped() {
        // The issue's race: the owner renewed at the old map before the pending promote
        // reached the grantors. The second survey sees the grant, and the promote stops
        // before its stamp, with b back among p3's replicas.
        let mut back = quiet(10_000);
        back.get_mut("a")
            .unwrap()
            .as_mut()
            .unwrap()
            .grants
            .push(LeaseWithheld {
                partition: "p3".into(),
                holder: "c".into(),
                remaining_ms: 4_000,
            });
        let f = fake(vec![quiet(10_000), back], 2, Duration::from_secs(1));
        let e = block_on(run(&f, &three(), "p3", "b", &failover())).unwrap_err();
        assert!(
            e.message().contains("grants p3's lease to its owner"),
            "{}",
            e.message()
        );
        assert!(!f.stamped.get());
        let m = f.map.borrow();
        assert_eq!(m.failovers[0].state(), S::Abandoned);
        assert_eq!(
            (m.partitions[0].node.as_str(), m.partitions[0].epoch),
            ("c", 3)
        );
        assert_eq!(m.partitions[0].replicas, ["b"]);
    }

    #[test]
    fn a_failover_whose_first_map_misses_a_majority_stays_pending_unstamped() {
        let f = fake(vec![quiet(10_000)], 1, Duration::from_secs(1));
        let e = block_on(run(&f, &three(), "p3", "b", &failover())).unwrap_err();
        assert!(e.message().contains("not a majority"), "{}", e.message());
        assert!(!f.stamped.get());
        assert_eq!(f.map.borrow().failovers[0].state(), S::Pending);
    }

    #[test]
    fn surveys_more_than_a_lease_apart_abandon_the_promote() {
        // 11 s between the clock reads, with 10 s leases: a grant taken after the first
        // survey could have run out before the second (L·0.99/1.01 is 9.8 s).
        // The first survey, slower than a sixth of the lease, is taken again (#2171).
        let f = fake(
            vec![quiet(10_000), quiet(10_000), quiet(10_000)],
            2,
            Duration::from_secs(11),
        );
        let e = block_on(run(&f, &three(), "p3", "b", &failover())).unwrap_err();
        assert!(e.message().contains("more than a lease"), "{}", e.message());
        assert!(!f.stamped.get());
        assert_eq!(f.map.borrow().failovers[0].state(), S::Abandoned);
    }

    #[test]
    fn with_no_lease_reported_the_promote_goes_on_with_a_warning_naming_the_nodes() {
        // The coordinator's ruling: older nodes (lease_ms 0) don't stop it.
        let f = fake(
            vec![quiet(0), quiet(0), quiet(0)],
            2,
            Duration::from_secs(30),
        );
        let done = block_on(run(&f, &three(), "p3", "b", &failover())).unwrap();
        assert_eq!(done.warnings.len(), 1, "{:?}", done.warnings);
        assert!(done.warnings[0].contains("a, b"), "{}", done.warnings[0]);
        assert!(f.stamped.get());
    }

    /// The owner just stopped: `--old-unreachable` waits its grants out, then goes on;
    /// grants still live after two leases mean the owner renews, so it doesn't start.
    #[test]
    fn an_unreachable_owners_grants_are_waited_out() {
        let granted = |ms| {
            let mut s = quiet(10_000);
            s.get_mut("a")
                .unwrap()
                .as_mut()
                .unwrap()
                .grants
                .push(LeaseWithheld {
                    partition: "p3".into(),
                    holder: "c".into(),
                    remaining_ms: ms,
                });
            s
        };
        let how = How::Manual {
            old_unreachable: true,
        };
        let f = fake(
            vec![granted(4_000), quiet(10_000), quiet(10_000)],
            2,
            Duration::from_millis(10),
        );
        block_on(run(&f, &three(), "p3", "b", &how)).unwrap();
        assert!(f.stamped.get());
        // A renewal on its way down is waited out too.
        let f = fake(
            vec![granted(4_000), granted(9_000), quiet(10_000), quiet(10_000)],
            2,
            Duration::from_millis(10),
        );
        block_on(run(&f, &three(), "p3", "b", &how)).unwrap();
        assert!(f.stamped.get());
        // 4.5 + 9.5 + 9.5 s of grants: past two 10 s leases, the owner renews.
        let f = fake(
            vec![
                granted(4_000),
                granted(9_000),
                granted(9_000),
                granted(9_000),
            ],
            2,
            Duration::from_millis(10),
        );
        let e = block_on(run(&f, &three(), "p3", "b", &how)).unwrap_err();
        assert!(e.message().contains("didn't start"), "{}", e.message());
        assert!(!f.stamped.get());
        assert!(f.map.borrow().failovers.is_empty());
    }

    #[test]
    fn a_promote_of_a_fenced_reachable_owner_doesnt_survey() {
        // The operator fenced or closed it, so it can't take writes: no survey (the fake
        // panics on one), and its first map needn't reach a majority (held 1 of 3).
        let f = fake(Vec::new(), 1, Duration::from_secs(1));
        let how = How::Manual {
            old_unreachable: false,
        };
        block_on(run(&f, &three(), "p3", "b", &how)).unwrap();
        assert!(f.stamped.get());
    }

    /// #2171: with a node down, putting the copy back among the replicas is refused (every
    /// node must answer to add one), so the abandon goes ahead without it and says so.
    #[test]
    fn an_abandon_whose_re_add_is_refused_abandons_without_it() {
        struct Refusing(Fake);
        impl Promoter for Refusing {
            async fn row(&self, id: &str, p: &str) -> Result<Option<LocalPartition>, Failure> {
                self.0.row(id, p).await
            }
            async fn map(&self) -> Result<ClusterMap, Failure> {
                self.0.map().await
            }
            async fn set_map(&self, m: ClusterMap) -> Result<Set, Failure> {
                let readds = m.partitions[0].replicas.iter().any(|r| r == "b")
                    && m.failovers.iter().any(|e| e.state() == S::Abandoned);
                if readds {
                    return Err(Failure::usage("the map can't add a read replica"));
                }
                self.0.set_map(m).await
            }
            fn now(&self) -> u64 {
                self.0.now()
            }
            async fn survey(&self, wait: Duration) -> Result<Survey, Failure> {
                self.0.survey(wait).await
            }
            async fn sleep(&self, d: Duration) {
                self.0.sleep(d).await;
            }
            async fn stamp(&self, id: &str, req: proto::StampRequest) -> Result<Vec<u8>, Failure> {
                self.0.stamp(id, req).await
            }
            async fn wait_closed(&self, id: &str, p: &str) -> Result<(), Failure> {
                self.0.wait_closed(id, p).await
            }
            async fn wait_serving(&self, id: &str, p: &str, e: u64) -> Result<(), Failure> {
                self.0.wait_serving(id, p, e).await
            }
        }
        let mut back = quiet(10_000);
        back.get_mut("a")
            .unwrap()
            .as_mut()
            .unwrap()
            .grants
            .push(LeaseWithheld {
                partition: "p3".into(),
                holder: "c".into(),
                remaining_ms: 4_000,
            });
        let f = Refusing(fake(vec![quiet(10_000), back], 2, Duration::from_secs(1)));
        let e = block_on(run(&f, &three(), "p3", "b", &failover())).unwrap_err();
        assert!(
            e.message().contains("once every node answers"),
            "{}",
            e.message()
        );
        let m = f.0.map.borrow();
        assert_eq!(m.failovers[0].state(), S::Abandoned);
        assert!(m.partitions[0].replicas.is_empty());
    }

    #[test]
    fn the_owner_back_test_needs_all_but_majority_minus_two_of_the_others() {
        let m = three();
        let p = &m.partitions[0];
        assert_eq!(owner_back(&m, p, &quiet(10_000)), None);
        // Three nodes: a lease needs one other grant, so both others must be asked.
        let mut s = quiet(10_000);
        s.insert("a".into(), None);
        assert!(owner_back(&m, p, &s).unwrap().contains("a didn't answer"));
        let mut s = quiet(10_000);
        s.get_mut("b").unwrap().as_mut().unwrap().grace_remaining_ms = 500;
        assert!(owner_back(&m, p, &s).unwrap().contains("b restarted"));
        // The owner answers with no lease of p3: the others' silence doesn't matter.
        let mut s = quiet(10_000);
        s.insert("a".into(), None);
        let mut lp = LocalPartition {
            id: "p3".into(),
            epoch: 3,
            ..Default::default()
        };
        lp.set_role(proto::PartitionRole::Owner);
        lp.set_state(proto::LocalPartitionState::Fenced);
        let owner = NodeStatus {
            node_id: "c".into(),
            partitions: vec![lp.clone()],
            ..Default::default()
        };
        s.insert("c".into(), Some(owner.clone()));
        assert_eq!(owner_back(&m, p, &s), None);
        // With a lease running there, it does.
        lp.lease_remaining_ms = 700;
        s.insert(
            "c".into(),
            Some(NodeStatus {
                partitions: vec![lp],
                ..owner
            }),
        );
        assert!(owner_back(&m, p, &s).unwrap().contains("a didn't answer"));
        // Five nodes: a lease needs two other grants, so one of four may be missing.
        let mut m5 = three();
        for id in ["d", "e"] {
            m5.nodes.push(proto::Node {
                id: id.into(),
                ..Default::default()
            });
        }
        let mut s = quiet(10_000);
        s.insert(
            "d".into(),
            Some(NodeStatus {
                node_id: "d".into(),
                ..Default::default()
            }),
        );
        s.insert("e".into(), None);
        assert_eq!(owner_back(&m5, &m5.partitions[0], &s), None);
        s.insert("d".into(), None);
        assert!(owner_back(&m5, &m5.partitions[0], &s).is_some());
    }

    /// D331 (the coordinator's ruling): from HANDING_OVER through CLEANING a range move
    /// holds both its partitions against a promote or a failover, the one site both take
    /// ([`run`] checks it first and again before its map). Before it (and ABORTING) the move
    /// aborts on an epoch change instead; a NODE move and another partition's don't count.
    #[test]
    fn a_range_move_holds_both_its_partitions_only_at_its_hand_over() {
        // #2154 (the coordinator's ruling): HANDING_OVER holds both. Past the commit point the
        // source may go, and the target only to a copy holding the range
        // (`target_copy_holds`). Before it, the move aborts on an epoch change instead. A
        // NODE move and another partition's don't count.
        use proto::{MoveKind, MovePhase as P, MoveStatus, RangeMove};
        let with = |phase: P, kind: MoveKind| {
            let mut m = with_p3(5);
            let mut r = RangeMove {
                id: "mv1".into(),
                kind: kind as i32,
                table: "t".into(),
                from: "p3".into(),
                to: "p4".into(),
                final_seqno: 40,
                ..Default::default()
            };
            r.set_phase(phase);
            m.moves.push(r);
            m
        };
        let m = with(P::HandingOver, MoveKind::Range);
        for p in ["p3", "p4"] {
            let why = range_move_holds(&m, p).unwrap_or_else(|| panic!("{p}"));
            assert!(why.contains("range move mv1"), "{why}");
        }
        assert_eq!(range_move_holds(&m, "p9"), None);
        assert_eq!(
            range_move_holds(&with(P::HandingOver, MoveKind::Node), "p3"),
            None
        );
        for phase in [
            P::Copying,
            P::CatchingUp,
            P::Aborting,
            P::HandedOver,
            P::Cleaning,
        ] {
            let m = with(phase, MoveKind::Range);
            assert_eq!(range_move_holds(&m, "p3"), None, "{phase:?}");
            assert_eq!(range_move_holds(&m, "p4"), None, "{phase:?}");
        }
        // The target past the commit point: only a copy whose own rows show the copy done
        // and caught up through `final_seqno`.
        let row = |done: bool, applied: u64| proto::LocalPartition {
            id: "p4".into(),
            moves: vec![MoveStatus {
                id: "mv1".into(),
                copy_done: done,
                source_applied_seqno: applied,
                ..Default::default()
            }],
            ..Default::default()
        };
        for phase in [P::HandedOver, P::Cleaning] {
            let m = with(phase, MoveKind::Range);
            assert!(
                target_copy_holds(&m, "p4", &row(true, 40)).is_ok(),
                "{phase:?}"
            );
            assert!(target_copy_holds(&m, "p4", &row(true, 39)).is_err());
            assert!(target_copy_holds(&m, "p4", &row(false, 40)).is_err());
            assert!(target_copy_holds(&m, "p4", &proto::LocalPartition::default()).is_err());
            // The source's copies, and another partition's, need nothing.
            assert!(target_copy_holds(&m, "p3", &proto::LocalPartition::default()).is_ok());
        }
        for phase in [P::Copying, P::CatchingUp, P::HandingOver, P::Aborting] {
            let m = with(phase, MoveKind::Range);
            assert!(
                target_copy_holds(&m, "p4", &proto::LocalPartition::default()).is_ok(),
                "{phase:?}: before the commit point the target's copy isn't the range's"
            );
        }
    }

    #[test]
    fn the_window_keeps_sixteen_finished_events_and_every_pending_one() {
        let mut m = with_p3(1);
        push_event(&mut m, ev("p3", "b", 100, S::Pending));
        for i in 0..20 {
            push_event(&mut m, ev("p1", "a", i, S::Done));
        }
        assert_eq!(m.failovers.len(), super::super::map::FAILOVER_EVENTS + 1);
        assert_eq!(m.failovers[0].to_epoch, 100, "the pending one stays");
        assert_eq!(m.failovers[1].to_epoch, 4, "the oldest finished ones went");
        assert_eq!(m.failovers.last().unwrap().to_epoch, 19);
    }
}

#[cfg(test)]
mod tail_tests {
    use super::*;

    fn done_tail(p: &str, epoch: u64, tail: proto::TailState) -> proto::FailoverEvent {
        let mut e = proto::FailoverEvent {
            partition: p.into(),
            from: "a".into(),
            to: "b".into(),
            to_epoch: epoch,
            fork_token: format!("fork{epoch}").into_bytes().into(),
            ..Default::default()
        };
        e.set_kind(proto::FailoverKind::Partition);
        e.set_state(proto::FailoverState::Done);
        e.set_tail_state(tail);
        e
    }

    /// D336, the owner's Q5: past 4 PENDING tails of a partition, the oldest are DISCARDED
    /// and their tokens cleared; another partition's are untouched.
    #[test]
    fn past_four_pending_tails_the_oldest_are_discarded() {
        let mut m = proto::ClusterMap::default();
        for epoch in 1..=6 {
            m.failovers
                .push(done_tail("p1", epoch, proto::TailState::Pending));
        }
        m.failovers
            .push(done_tail("p2", 1, proto::TailState::Pending));
        discard_past_cap(&mut m, "p1");
        let states: Vec<_> = m.failovers.iter().map(|e| e.tail_state()).collect();
        use proto::TailState::{Discarded, Pending};
        assert_eq!(
            states,
            [
                Discarded, Discarded, Pending, Pending, Pending, Pending, Pending
            ]
        );
        assert!(m.failovers[0].fork_token.is_empty());
        assert!(!m.failovers[2].fork_token.is_empty());
    }

    /// D336: a finished event whose tail is PENDING is kept past the ring of finished events.
    #[test]
    fn a_pending_tail_is_kept_past_the_event_ring() {
        let mut m = proto::ClusterMap::default();
        m.failovers
            .push(done_tail("p1", 1, proto::TailState::Pending));
        for epoch in 2..=(super::super::map::FAILOVER_EVENTS as u64 + 3) {
            push_event(&mut m, done_tail("p2", epoch, proto::TailState::None));
        }
        assert_eq!(m.failovers[0].partition, "p1");
        assert_eq!(m.failovers[0].tail_state(), proto::TailState::Pending);
    }

    /// D336: which promotes leave a tail to replay.
    #[test]
    fn a_tail_is_pending_when_the_old_owner_may_hold_commits() {
        let mut e = proto::FailoverEvent::default();
        let failover = How::Failover {
            in_doubt_xids: Vec::new(),
            in_doubt_count: 0,
        };
        let unreachable = How::Manual {
            old_unreachable: true,
        };
        let reachable = How::Manual {
            old_unreachable: false,
        };
        assert_eq!(tail_state(&failover, &e), proto::TailState::Pending);
        assert_eq!(tail_state(&unreachable, &e), proto::TailState::Pending);
        // The owner's durable seqno unknown: it may.
        assert_eq!(tail_state(&reachable, &e), proto::TailState::Pending);
        (e.applied_seqno, e.primary_seqno) = (10, 10);
        assert_eq!(tail_state(&reachable, &e), proto::TailState::None);
        e.primary_seqno = 11;
        assert_eq!(tail_state(&reachable, &e), proto::TailState::Pending);
    }
}
