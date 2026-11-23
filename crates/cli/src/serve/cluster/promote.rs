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

/// How long the promote waits for ID to close its copy after the map dropped it.
const CLOSE_WAIT: Duration = Duration::from_secs(60);

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
    let ops = CliOps {
        args,
        map: &m,
        timeout,
    };
    let done = run(&ops, &m, partition, to, &How::Manual).await?;
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
                } else {
                    "from the owner's durable seqno as the copy last heard it, with the owner \
                     fenced or closed, so it took no commit since"
                }),
            ),
            ("map_version", json!(done.map_version)),
        ],
        Value::Null,
    );
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
    /// `phdb cluster promote`: the operator's checks of the old owner were made.
    Manual,
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
    /// Sets `m` (its version already raised) as a planned change; its version.
    async fn set_map(&self, m: proto::ClusterMap) -> Result<u64, Failure>;
    /// Stamps node `id`'s copy.
    async fn stamp(&self, id: &str, req: proto::StampRequest) -> Result<(), Failure>;
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
/// the floor itself.
pub(crate) fn pinned(m: &proto::ClusterMap, e: &proto::FailoverEvent) -> bool {
    use proto::{FailoverKind as K, FailoverState as St};
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

/// Promotes node `to`'s copy of `partition` in `m` (D328 PR 3, with D330 PR 2's pending
/// events): the steps after the caller's checks of the old owner.
/// 1. `to` leaves P's replicas, so it closes its copy; with writer terms the same map
///    records the promote pending at its epoch ([`target_epoch`]), before any stamp.
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
    // 1. ID leaves P's replicas, so it closes its copy (D328 PR 1's sync); with writer terms
    // the same map records the promote pending at `target`, before the stamp (S2). A copy
    // still open while `to` is out of P's replicas is an earlier run's, closing (simp724's
    // S1 on #2007): it's waited for, not left again.
    // Only a copy still a replica leaves the replicas here: a stamped one whose node is back
    // in them was put there since (a set-map), which step 3 refuses rather than undoes
    // (simp724's M1 on #2007).
    let leave = replica && p.replicas.iter().any(|r| r == to);
    let record = terms && !resumes && !stamped;
    if leave || record {
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
                How::Manual => (Vec::new(), 0),
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
        ops.set_map(m1).await?;
    }
    if replica {
        ops.wait_closed(to, partition).await?;
    }
    // 2. The copy, promoted and stamped at the target epoch.
    if !stamped {
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
        .await?;
    }
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
    let map_version = ops.set_map(m2).await?;
    // 4. Serving on ID, as a move waits for its new node (#2035): done means P takes reads
    // and writes there.
    ops.wait_serving(to, partition, target).await?;
    Ok(Promoted {
        epoch: target,
        applied,
        primary,
        replica,
        map_version,
    })
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

    async fn set_map(&self, m: proto::ClusterMap) -> Result<u64, Failure> {
        let set = set_map(self.args, m, false, Vec::new()).await?;
        Ok(set
            .document
            .get("map_version")
            .and_then(Value::as_u64)
            .unwrap_or(0))
    }

    async fn stamp(&self, id: &str, req: proto::StampRequest) -> Result<(), Failure> {
        let url = url_of(self.args, &self.address(id)?);
        let (mut c, token) = connect(self.args, &url).await?;
        c.stamp(request(req, token.as_deref())?)
            .await
            .map_err(|s| Failure::usage(&format!("{url}: stamp: {}", s.message())))?;
        Ok(())
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
