//! Partition failover by the map's writer (D330 PR 2, design §4–6, ICR 0117's D330 section).
//!
//! While the map has `failover` on, the writer checks after each lease round
//! ([`Node::maybe_fail_over`], one of its duties, off the round's task):
//! - **Down:** its own grant of P to P's owner ran out unrenewed at least the failover margin
//!   A (`--cluster-failover-after-ms`, 2·L by default) ago. A writer that took office after
//!   the grant counts from its first renewal seen, or from L after it took office (simp724's
//!   S3 on the plan): its own missing grants aren't an owner's silence.
//! - **Not failed over, and why** (`NodeStatus.failover_blocked`): the partition opts out
//!   (`Partition.failover` OFF), it has no replicas, its owner answers and serves it or the
//!   operator fenced or closed it there, or no replica qualifies.
//! - **The replica:** of P's replicas, those following, at the writer's own map and term
//!   (simp724's S2 on the design), whose copy is inside P's history and fresh as of the
//!   owner's last renewal plus one heartbeat (and within `--cluster-failover-max-lag-ms` if
//!   set); the one with the highest applied seqno, then map order. A copy stamped by a
//!   pending promote of P is taken first, to finish it (finish only: the plan's F4).
//! - **The promote** is `phdb cluster promote`'s ([`promote::run`]) over the writer's peers:
//!   the map records it pending before the stamp and done with the move, with the in-doubt
//!   shares the old owner sent with its last renewal (F7), and P's `failover_count` rises.
//!   One at a time: the next waits for a later round.
//!
//! The new owner serves P only once a majority grants its lease, which they do only after
//! their grants to the old one ran out (D326): the survey here only saves a promote that
//! couldn't serve.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use super::promote::{self, How, Promoter};
use super::proto::{
    self, ClusterMap, FailoverKind, FailoverMode, FailoverState, LocalPartitionState, NodeStatus,
    PartitionRole, ReplicaState, SetMapRequest, StampRequest, StatusRequest,
};
use super::{Node, schema};
use crate::exit::Failure;

/// How often an owner's change feed tells its replicas its durable frontier when idle: a
/// copy's freshness is as of its last message, at most this before the owner's last renewal.
const HEARTBEAT: Duration = Duration::from_secs(1);

/// How long the writer waits for a promoted copy to close, then to serve.
const STEP_WAIT: Duration = Duration::from_secs(60);

/// A test's fault seam: the next failover by a node in each listed cluster directory stops
/// after its stamp, before its map, as the writer dying there would leave it.
#[cfg(test)]
pub(crate) static STOP_AFTER_STAMP: Mutex<Vec<std::path::PathBuf>> = Mutex::new(Vec::new());

/// The writer's failover state.
#[derive(Debug, Default)]
pub(crate) struct Failovers {
    /// The writer term this node last found itself writer at, and when, on its lease clock:
    /// a partition's silence counts from there at the earliest (S3).
    office: Mutex<(u64, u64)>,
    /// Each down partition not failed over, and why, as the last check found them.
    blocked: Mutex<BTreeMap<String, String>>,
    /// When each blocked partition that went through the last survey is due another, on the
    /// lease clock, and the map version it was surveyed at (perf287's S1 on #2069): after
    /// the failover margin, or when the grant it waits on ends (simp724's nit), or on a map
    /// change, not every lease round.
    surveyed: Mutex<HashMap<String, (u64, u64)>>,
    /// When the blocked partitions with a pending promote are next due a look at their
    /// owners, on the lease clock (#2152): once per failover margin, with failover off too.
    waiting_due: Mutex<u64>,
}

/// A test's count: the failover surveys each node (by cluster directory) has run (perf287's
/// S1 on #2069).
#[cfg(test)]
pub(crate) static SURVEYS: Mutex<Vec<(std::path::PathBuf, u64)>> = Mutex::new(Vec::new());

impl Failovers {
    /// When this node took office at `term`, recording `now` if it's a new term.
    pub(super) fn office(&self, term: u64, now: u64) -> u64 {
        let mut o = self.office.lock().unwrap_or_else(PoisonError::into_inner);
        if o.0 != term || o.1 == 0 {
            *o = (term, now);
        }
        o.1
    }

    /// `NodeStatus.failover_blocked`.
    pub(crate) fn blocked(&self) -> HashMap<String, String> {
        self.blocked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn set_blocked(&self, blocked: BTreeMap<String, String>) {
        *self.blocked.lock().unwrap_or_else(PoisonError::into_inner) = blocked;
    }

    /// Partition `id`'s last blocked reason, if any.
    fn blocked_reason(&self, id: &str) -> Option<String> {
        self.blocked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .cloned()
    }

    /// Whether blocked partition `id` is due a survey at `now` under map `version`: never
    /// surveyed, or its due time passed, or the map changed since.
    fn survey_due(&self, id: &str, now: u64, version: u64) -> bool {
        self.surveyed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .is_none_or(|(due, v)| *v != version || now >= *due)
    }

    /// The partitions a survey left blocked, each with when it's due again. Only those that
    /// went through it (perf2's S2 on #2069): one that wasn't keeps no stale stamp.
    fn note_surveyed(&self, due: Vec<(String, u64)>, version: u64) {
        let mut s = self.surveyed.lock().unwrap_or_else(PoisonError::into_inner);
        s.clear();
        s.extend(due.into_iter().map(|(id, at)| (id, (at, version))));
    }
}

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

fn millis(ns: u64) -> u64 {
    ns / 1_000_000
}

/// Another node that still grants down partition `p`'s lease to its owner, and that grant,
/// from the survey's `statuses`. The owner's own grant to itself isn't a majority's: it
/// renews that one even cut off from every grantor, so only the others' count.
fn still_granted<'a>(
    p: &proto::Partition,
    statuses: &'a HashMap<String, NodeStatus>,
) -> Option<(&'a String, &'a proto::LeaseWithheld)> {
    statuses
        .iter()
        .filter(|(id, _)| **id != p.node)
        .find_map(|(id, st)| {
            st.grants
                .iter()
                .find(|g| g.partition == p.id && g.holder == p.node && g.remaining_ms > 0)
                .map(|g| (id, g))
        })
}

/// Partition `p` as the writer sees it before any survey (the module doc's Down): the pure
/// part of [`Node::maybe_fail_over`], which the seeded model calls too (simp724 on #2070).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Check {
    /// Not down: its owner's grant here ran out less than the margin ago, or still runs.
    Up,
    /// Down, and not failed over, for this reason.
    Blocked(String),
    /// Down, for the survey and [`qualify`]: when its owner last renewed it here, on the
    /// writer's lease clock (`None`: not since this node started).
    Survey(Option<u64>),
    /// Not down, with a promote of it left pending: its owner renewed it here since this
    /// node took office, so it came back. Once the survey finds it serving P at its epoch,
    /// the promote is abandoned ([`abandoned`]): it can't safely finish while the old owner
    /// holds a lease, and left pending it would never clear (#2070, simp724).
    Abandon,
}

/// What an abandoned promote of `p` left, while it pins the floor: the copy it stamped,
/// for the operator to drop.
fn orphaned(map: &ClusterMap, p: &proto::Partition) -> Option<String> {
    map.failovers
        .iter()
        .filter(|e| {
            e.kind() == FailoverKind::Partition
                && e.partition == p.id
                && e.state() == FailoverState::Abandoned
                && e.to_epoch > p.epoch
        })
        .max_by_key(|e| e.to_epoch)
        .map(|e| {
            format!(
                "an unfinished promote stamped node {} at epoch {}: drop that copy (phdb cluster \
                 drop-partition {} on node {})",
                e.to, e.to_epoch, p.id, e.to
            )
        })
}

/// Whether `st`, partition `p`'s owner's status, has it serving `p` at its epoch.
pub(crate) fn serves(p: &proto::Partition, st: Option<&NodeStatus>) -> bool {
    st.is_some_and(|st| {
        st.partitions.iter().any(|lp| {
            lp.id == p.id
                && lp.role() != PartitionRole::Replica
                && lp.epoch == p.epoch
                && lp.state() == LocalPartitionState::Serving
        })
    })
}

/// `map` with the live pending promotes of `ids` abandoned, its version raised: the
/// writer's change once each one's owner serves it again (#2070).
pub(crate) fn abandoned(map: &ClusterMap, ids: &[String]) -> ClusterMap {
    let mut m = map.clone();
    m.map_version += 1;
    let epochs: HashMap<&str, u64> = map
        .partitions
        .iter()
        .map(|p| (p.id.as_str(), p.epoch))
        .collect();
    for e in m.failovers.iter_mut().filter(|e| {
        e.kind() == FailoverKind::Partition
            && e.state() == FailoverState::Pending
            && ids.contains(&e.partition)
            && epochs
                .get(e.partition.as_str())
                .is_some_and(|ep| *ep < e.to_epoch)
    }) {
        e.set_state(FailoverState::Abandoned);
    }
    m
}

/// Whether partition `p`'s pending promote is to be abandoned for its owner (#2152). That
/// promote's first map keeps P's lease from the owner ([`promote::pending_promote`]), so an
/// owner that came back can't renew it, and [`check`]'s renewal test never sees it. Its
/// place: `asked`, the owner asked this node for P's lease within the last lease (withheld
/// here, `Grants::asked_withheld_since`), so it runs its rounds and can send; and it answers
/// with P open at its epoch, fenced only for want of its lease (or serving). As with a
/// renewal (#2070), this wins over finishing a stamped copy: the promote never got its final
/// map, which marks it done in the same change, so its copy took no write and the owner holds
/// everything (simp724's Q2 on the plan).
pub(crate) fn owner_ready(
    map: &ClusterMap,
    p: &proto::Partition,
    statuses: &HashMap<String, NodeStatus>,
    asked: bool,
) -> bool {
    if !asked || promote::pending_promote(map, p).is_none() {
        return false;
    }
    statuses.get(&p.node).is_some_and(|st| {
        st.partitions.iter().any(|lp| {
            lp.id == p.id
                && lp.role() != PartitionRole::Replica
                && lp.epoch == p.epoch
                && match lp.state() {
                    LocalPartitionState::Serving => true,
                    LocalPartitionState::Fenced => {
                        lp.fence_reason() != proto::FenceReason::Operator
                    }
                    _ => false,
                }
        })
    })
}

/// `map` with the pending promotes of `ready` (each [`owner_ready`]) abandoned, its version
/// raised, and each promote's copy back in its partition's replicas when its node answers
/// with it below the promote's epoch, unstamped (#2152). A copy that doesn't answer may be
/// stamped: an orphan for the operator to drop, as the guide says.
pub(crate) fn abandoned_for_owner(
    map: &ClusterMap,
    ready: &[proto::Partition],
    statuses: &HashMap<String, NodeStatus>,
) -> ClusterMap {
    let ids: Vec<String> = ready.iter().map(|p| p.id.clone()).collect();
    let mut m = abandoned(map, &ids);
    for p in ready {
        let Some(e) = promote::pending_promote(map, p) else {
            continue;
        };
        let unstamped = statuses.get(&e.to).is_some_and(|st| {
            st.partitions
                .iter()
                .any(|lp| lp.id == p.id && lp.epoch < e.to_epoch)
        });
        if !unstamped {
            continue;
        }
        for q in m.partitions.iter_mut().filter(|q| q.id == p.id) {
            if q.node != e.to && !q.replicas.contains(&e.to) {
                q.replicas.push(e.to.clone());
            }
        }
    }
    m
}

/// [`Check`] for `p` in `map` at `now`: `until` is the end of the owner's last grant of `p`
/// here at its epoch, `office` when this node took office at the map's term, `lease` and
/// `margin` (A) in ns.
pub(crate) fn check(
    map: &ClusterMap,
    p: &proto::Partition,
    until: Option<u64>,
    office: u64,
    now: u64,
    lease: u64,
    margin: u64,
) -> Check {
    let pending = map.failovers.iter().any(|e| {
        e.kind() == FailoverKind::Partition
            && e.partition == p.id
            && e.state() == FailoverState::Pending
            && e.to_epoch > p.epoch
    });
    // Its owner renewed it here in office, and that grant runs: it came back (#2070).
    if pending && until.is_some_and(|u| u > office && now < u) {
        return Check::Abandon;
    }
    // From the owner's last grant here if this node saw it renewed in office, else from a
    // lease after taking office (S3: when P counts as down, only).
    let lapsed_at = match until {
        Some(u) if u > office => u,
        _ => office.saturating_add(lease),
    };
    if now < lapsed_at.saturating_add(margin) {
        return Check::Up;
    }
    // With the map's failover off, a down partition is only reported (§6).
    if !map.failover {
        return Check::Blocked("failover off".to_owned());
    }
    if p.failover() == FailoverMode::Off {
        return Check::Blocked("failover off for this partition".to_owned());
    }
    // A promote left pending has already taken its copy out of the replicas: it's finished
    // from its event (F4), so an empty list doesn't stop it.
    if p.replicas.is_empty() && !pending {
        return Check::Blocked("no replicas".to_owned());
    }
    Check::Survey(until.map(|u| u.saturating_sub(lease)))
}

/// The down partitions a pass surveys (perf2's S2 on #2069): every one once any is due, since
/// the survey is the cost, so none keeps a stale reason through another's survey; or, if
/// none is due, none, each keeping its last reason (`Err`).
fn survey_or_keep(down: Vec<Down>) -> Result<Vec<Down>, Vec<(String, String)>> {
    if !down.is_empty() && down.iter().all(|d| d.kept.is_some()) {
        return Err(down
            .into_iter()
            .map(|d| (d.p.id, d.kept.unwrap_or_default()))
            .collect());
    }
    Ok(down)
}

/// The replica down partition `p` fails over to, or why none: the module doc's rules, the
/// pure part of [`Node::maybe_fail_over`], which the seeded model calls too (simp724 on
/// #2070). `renewed` is [`Check::Survey`]'s, `statuses` the survey's (the writer's own
/// included), `now` the writer's lease clock, and `max_lag` `--cluster-failover-max-lag-ms`.
pub(crate) fn qualify(
    map: &ClusterMap,
    p: &proto::Partition,
    renewed: Option<u64>,
    statuses: &HashMap<String, NodeStatus>,
    now: u64,
    max_lag: Option<Duration>,
) -> Result<String, String> {
    if let Some(st) = statuses.get(&p.node)
        && let Some(lp) = st.partitions.iter().find(|lp| lp.id == p.id)
        && lp.role() != PartitionRole::Replica
    {
        match lp.state() {
            LocalPartitionState::Serving => {
                return Err(format!("waiting: node {} answers and serves it", p.node));
            }
            // An operator's fence or close only: an owner that answers but has lost its
            // lease reports Fenced too (awaiting its peers), and is down for this.
            LocalPartitionState::Closed => {
                return Err(format!("closed by the operator on node {}", p.node));
            }
            LocalPartitionState::Fenced if lp.fence_reason() == proto::FenceReason::Operator => {
                return Err(format!("fenced by the operator on node {}", p.node));
            }
            _ => {}
        }
    }
    // A whole-partition move of P past its commit point (D332 PR 2): P is on the move's
    // target, and only finishing the move serves it.
    if let Some(m) = map.moves.iter().find(|m| {
        super::map::node_move(m) && m.from == p.id && super::map::committed(super::map::phase(m))
    }) {
        return Err(format!(
            "move {} (relocating it to node {}) is past its commit point: let it finish, or \
             use a forced map",
            m.id, m.to
        ));
    }
    // A range move naming P at its hand-over or past it (D331): it only goes on.
    if let Some(why) = super::promote::range_move_holds(map, &p.id) {
        return Err(why);
    }
    // A promote of P left pending (its writer stopped after the stamp): finished on the
    // copy it stamped, if that node answers with it (finish only, F4).
    if let Some(e) = map
        .failovers
        .iter()
        .filter(|e| {
            e.kind() == FailoverKind::Partition
                && e.partition == p.id
                && e.state() == FailoverState::Pending
                && e.to_epoch > p.epoch
        })
        .max_by_key(|e| e.to_epoch)
    {
        let stamped = statuses.get(&e.to).is_some_and(|st| {
            st.partitions.iter().any(|lp| {
                lp.id == p.id && lp.role() != PartitionRole::Replica && lp.epoch == e.to_epoch
            })
        });
        if stamped {
            return Ok(e.to.clone());
        }
    }
    // Freshness as of the owner's last renewal: its grant here began a lease before it
    // ended. `replica_lag_ms` is the replica's own monotonic clock since its last fresh
    // message, and `now - renewed` the writer's lease clock since that renewal: two clocks,
    // compared only to within the heartbeat and drift (simp724's N1).
    // From the owner's real last renewal here (simp724's B1): this node is one of P's
    // grantors, so it has it whenever it ran while the owner renewed. Without one there's
    // no baseline to bound what a failover loses, so the writer doesn't guess.
    let Some(renewed) = renewed else {
        return Err(format!(
            "no renewal of {}'s owner (node {}) seen here since this node started: promote \
             by hand (phdb cluster promote {} --to <replica> --old-unreachable)",
            p.id, p.node, p.id
        ));
    };
    let since_renewal = millis(now.saturating_sub(renewed));
    // Any other node still granting P's lease to its owner (design §4, simp724's B2 on
    // the plan): the owner is alive and leased there, cut off from this node alone (§8
    // test 7). The grant round would keep a promoted copy from serving until those grants
    // end anyway; this saves moving P off an owner that serves. A node that didn't
    // answer the survey is unknown, not granting (simp724): the writer goes on, since
    // waiting on silence would never fail over a partition whose grantors are down too,
    // and the grant round still keeps two owners from serving at once.
    if let Some((id, g)) = still_granted(p, statuses) {
        return Err(format!(
            "waiting: node {id} still grants its lease to node {} ({} ms left)",
            p.node, g.remaining_ms
        ));
    }
    let mut best: Option<(u64, String)> = None;
    let mut why = Vec::new();
    for r in &p.replicas {
        let Some(st) = statuses.get(r) else {
            why.push(format!("{r}: didn't answer"));
            continue;
        };
        let Some(lp) = st
            .partitions
            .iter()
            .find(|lp| lp.id == p.id && lp.role() == PartitionRole::Replica)
        else {
            // A copy an unfinished promote stamped (Amendment 1's S2): it serves no
            // history now, and only the operator should drop it (simp724's N1 on #2069).
            let orphan = st.partitions.iter().find(|lp| {
                lp.id == p.id
                    && lp.role() != PartitionRole::Replica
                    && lp.epoch != p.epoch
                    && map.failovers.iter().any(|e| {
                        e.kind() == FailoverKind::Partition
                            && e.partition == p.id
                            && e.to == *r
                            && e.to_epoch == lp.epoch
                    })
            });
            why.push(match orphan {
                Some(lp) => format!(
                    "{r}: its copy was stamped at epoch {} by an unfinished promote: drop it \
                     there (phdb cluster drop-partition {} on node {r})",
                    lp.epoch, p.id
                ),
                None => format!("{r}: no replica copy open"),
            });
            continue;
        };
        if !matches!(
            lp.replica_state(),
            ReplicaState::Following | ReplicaState::Behind
        ) {
            why.push(format!("{r}: {:?}", lp.replica_state()));
            continue;
        }
        if (st.writer_term, st.map_version) != (map.writer_term, map.map_version) {
            why.push(format!(
                "{r}: holds map version {} at term {}, not the writer's",
                st.map_version, st.writer_term
            ));
            continue;
        }
        if !(p.history_epoch <= lp.epoch && lp.epoch <= p.epoch) {
            why.push(format!(
                "{r}: its copy is at epoch {}, outside P's history",
                lp.epoch
            ));
            continue;
        }
        let stale_by = lp.replica_lag_ms.saturating_sub(since_renewal);
        if stale_by > millis(nanos(HEARTBEAT)) {
            why.push(format!(
                "{r}: {stale_by} ms staler than the owner's last renewal"
            ));
            continue;
        }
        if let Some(max) = max_lag
            && Duration::from_millis(stale_by) > max
        {
            why.push(format!(
                "{r}: {stale_by} ms behind, over --cluster-failover-max-lag-ms"
            ));
            continue;
        }
        // Highest applied, then map order (the first such).
        if best
            .as_ref()
            .is_none_or(|(applied, _)| lp.replica_applied_seqno > *applied)
        {
            best = Some((lp.replica_applied_seqno, r.clone()));
        }
    }
    best.map(|(_, r)| r)
        .ok_or_else(|| format!("no fresh replica ({})", why.join("; ")))
}

/// A partition the writer finds down: its map entry, and when its owner's grant here ran
/// out (on the writer's lease clock), with the in-doubt shares the owner last sent.
struct Down {
    p: proto::Partition,
    /// When the owner last renewed P with this node, on its lease clock: its last grant's
    /// end less a lease, whatever this node's office time (simp724's B1 on #2069). `None`
    /// when this node has seen no renewal (it started since).
    renewed: Option<u64>,
    in_doubt_xids: Vec<String>,
    in_doubt_count: u64,
    /// Its last blocked reason while it isn't due a survey: kept unless another down
    /// partition's survey runs anyway.
    kept: Option<String>,
}

impl Node {
    /// Abandons the pending promotes of `back`, partitions whose owner came back
    /// ([`Check::Abandon`]), once the survey finds each serving at its epoch (#2070); and of
    /// `waiting`, blocked partitions with a pending promote, once their owner answers
    /// waiting for the lease that promote withholds ([`owner_ready`], #2152).
    async fn abandon_pending(
        self: &Arc<Self>,
        map: &ClusterMap,
        back: &[proto::Partition],
        waiting: &[proto::Partition],
    ) {
        let mut statuses: HashMap<String, NodeStatus> = self
            .survey(map)
            .await
            .into_iter()
            .filter_map(|(id, st)| st.map(|st| (id, st)))
            .collect();
        // This node's own, for a promote's copy here.
        let node = Arc::clone(self);
        if let Ok(mine) = tokio::task::spawn_blocking(move || node.status()).await {
            statuses.insert(self.id.clone(), mine);
        }
        let ps: Vec<proto::Partition> = back
            .iter()
            .filter(|p| serves(p, statuses.get(&p.node)))
            .chain(waiting.iter().filter(|p| {
                let now = self.grants.clock().now();
                let since = now.saturating_sub(nanos(self.grants.lease()));
                let asked = self
                    .grants
                    .asked_withheld_since(&p.id, &p.node, p.epoch, since);
                owner_ready(map, p, &statuses, asked)
            }))
            .cloned()
            .collect();
        if ps.is_empty() {
            return;
        }
        let ids: Vec<String> = ps.iter().map(|p| p.id.clone()).collect();
        // Still the writer, with its lease, at the same map, or a later round decides.
        let Ok(current) = self.get_map() else { return };
        if (current.writer_term, current.map_version) != (map.writer_term, map.map_version)
            || self.writer_may_change(&current).is_err()
        {
            return;
        }
        let ops = NodeOps {
            node: Arc::downgrade(self),
        };
        match ops
            .set_map(abandoned_for_owner(map, &ps, &statuses))
            .await
            .map(|s| s.version)
        {
            Ok(v) => eprintln!(
                "phdb serve: abandoned the pending promote of {} at map version {v}: the owner \
                 came back and serves it (#2070). A copy that promote stamped must be dropped",
                ids.join(", ")
            ),
            Err(f) => eprintln!(
                "phdb serve: abandoning the pending promote of {}: {}",
                ids.join(", "),
                f.message()
            ),
        }
    }

    /// The failover check (the module doc). Runs on the writer's duty task.
    pub(crate) async fn maybe_fail_over(self: &Arc<Self>) {
        let Ok(map) = self.get_map() else { return };
        if schema::writer(&map) != self.id || self.writer_may_change(&map).is_err() {
            self.failovers.set_blocked(BTreeMap::new());
            return;
        }
        let now = self.grants.clock().now();
        let lease = nanos(self.grants.lease());
        let margin = nanos(self.peers.failover_after);
        let office = self.failovers.office(map.writer_term, now);
        let mut blocked = BTreeMap::new();
        let mut down = Vec::new();
        let mut back = Vec::new();
        let mut waiting = Vec::new();
        for p in map.partitions.iter().filter(|p| p.node != self.id) {
            let until = self.grants.last_grant_until(&p.id, &p.node, p.epoch);
            let renewed = match check(&map, p, until, office, now, lease, margin) {
                Check::Up => {
                    // An abandoned promote's copy is an orphan until the operator drops it
                    // (#2070, simp724's point 4).
                    if let Some(why) = orphaned(&map, p) {
                        blocked.insert(p.id.clone(), why);
                    }
                    continue;
                }
                Check::Abandon => {
                    back.push(p.clone());
                    continue;
                }
                Check::Blocked(why) => {
                    // A pending promote's owner may be back and waiting for the lease that
                    // promote withholds, with failover off too (#2152): abandoned below once
                    // it answers so.
                    if promote::pending_promote(&map, p).is_some() {
                        waiting.push(p.clone());
                    }
                    blocked.insert(p.id.clone(), why);
                    continue;
                }
                Check::Survey(renewed) => renewed,
            };
            // Not due a survey (perf287's S1): its last reason stands, unless another down
            // partition's survey runs anyway (perf2's S2 on #2069).
            let kept = if self.failovers.survey_due(&p.id, now, map.map_version) {
                None
            } else {
                self.failovers.blocked_reason(&p.id)
            };
            let (in_doubt_xids, in_doubt_count) = self
                .grants
                .last_grant(&p.id)
                .filter(|g| g.holder == p.node && g.epoch == p.epoch)
                .map(|g| (g.in_doubt_xids, g.in_doubt_count))
                .unwrap_or_default();
            down.push(Down {
                p: p.clone(),
                renewed,
                in_doubt_xids,
                in_doubt_count,
                kept,
            });
        }
        if !waiting.is_empty() {
            let mut due = self
                .failovers
                .waiting_due
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if now < *due {
                waiting.clear();
            } else {
                *due = now.saturating_add(margin);
            }
        }
        if !back.is_empty() || !waiting.is_empty() {
            self.abandon_pending(&map, &back, &waiting).await;
        }
        let down = match survey_or_keep(down) {
            Ok(down) => down,
            Err(kept) => {
                blocked.extend(kept);
                Vec::new()
            }
        };
        if down.is_empty() {
            self.failovers.set_blocked(blocked);
            return;
        }
        #[cfg(test)]
        {
            let mut s = SURVEYS.lock().unwrap_or_else(PoisonError::into_inner);
            match s.iter_mut().find(|(d, _)| *d == self.dir) {
                Some((_, n)) => *n += 1,
                None => s.push((self.dir.clone(), 1)),
            }
        }
        let mut statuses: HashMap<String, NodeStatus> = self
            .survey(&map)
            .await
            .into_iter()
            .filter_map(|(id, st)| st.map(|st| (id, st)))
            .collect();
        let node = Arc::clone(self);
        if let Ok(mine) = tokio::task::spawn_blocking(move || node.status()).await {
            statuses.insert(self.id.clone(), mine);
        }
        // The survey took time: still the writer, with its lease, at the same map (simp724's
        // (a) on the stall fix), or a later round decides.
        let Ok(current) = self.get_map() else { return };
        if (current.writer_term, current.map_version) != (map.writer_term, map.map_version)
            || self.writer_may_change(&current).is_err()
        {
            return;
        }
        // #2152: a pending promote whose owner came back, and waits for the lease that
        // promote withholds, is abandoned so the owner serves again. The map changed, so a
        // later round decides the rest.
        let since = now.saturating_sub(lease);
        let asked = |p: &proto::Partition| {
            self.grants
                .asked_withheld_since(&p.id, &p.node, p.epoch, since)
        };
        let (ready, down): (Vec<Down>, Vec<Down>) = down
            .into_iter()
            .partition(|d| owner_ready(&map, &d.p, &statuses, asked(&d.p)));
        if !ready.is_empty() {
            let ps: Vec<proto::Partition> = ready.iter().map(|d| d.p.clone()).collect();
            let ids: Vec<&str> = ps.iter().map(|p| p.id.as_str()).collect();
            let ops = NodeOps {
                node: Arc::downgrade(self),
            };
            match ops.set_map(abandoned_for_owner(&map, &ps, &statuses)).await {
                Ok(s) => eprintln!(
                    "phdb serve: abandoned the pending promote of {} at map version {}: the \
                     owner came back and waits for its lease (#2152)",
                    ids.join(", "),
                    s.version
                ),
                Err(f) => eprintln!(
                    "phdb serve: abandoning the pending promote of {}: {}",
                    ids.join(", "),
                    f.message()
                ),
            }
            self.failovers.set_blocked(blocked);
            return;
        }
        let now = self.grants.clock().now();
        let mut chosen = None;
        let mut due = Vec::new();
        for d in down {
            // Due again once the grant it may wait on ends (simp724's nit on #2069), else
            // after the margin.
            let again = still_granted(&d.p, &statuses).map_or(margin, |(_, g)| {
                nanos(Duration::from_millis(g.remaining_ms))
            });
            due.push((d.p.id.clone(), now.saturating_add(again)));
            match qualify(
                &map,
                &d.p,
                d.renewed,
                &statuses,
                now,
                self.peers.failover_max_lag,
            ) {
                Ok(to) if chosen.is_none() => chosen = Some((d, to)),
                // One promote at a time: the others wait for a later round.
                Ok(to) => {
                    blocked.insert(
                        d.p.id.clone(),
                        format!("waiting: another failover runs first (replica on node {to})"),
                    );
                }
                Err(why) => {
                    blocked.insert(d.p.id.clone(), why);
                }
            }
        }
        // The ones the survey left blocked are due again as above, or on a map change.
        due.retain(|(id, _)| blocked.contains_key(id));
        self.failovers.note_surveyed(due, map.map_version);
        let Some((d, to)) = chosen else {
            self.failovers.set_blocked(blocked);
            return;
        };
        self.failovers.set_blocked(blocked.clone());
        let ops = NodeOps {
            node: Arc::downgrade(self),
        };
        let how = How::Failover {
            in_doubt_xids: d.in_doubt_xids,
            in_doubt_count: d.in_doubt_count,
        };
        match promote::run(&ops, &map, &d.p.id, &to, &how).await {
            Ok(done) => {
                for w in &done.warnings {
                    eprintln!("phdb serve: failing partition {} over: {w}", d.p.id);
                }
                eprintln!(
                    "phdb serve: failed partition {} over from node {} to node {to} at epoch {} \
                     (applied seqno {}, the owner's durable seqno {} as the copy last heard it; \
                     map version {}, writer term {})",
                    d.p.id,
                    d.p.node,
                    done.epoch,
                    done.applied,
                    done.primary,
                    done.map_version,
                    map.writer_term
                );
            }
            Err(f) => {
                eprintln!(
                    "phdb serve: failing partition {} over to node {to}: {}",
                    d.p.id,
                    f.message()
                );
                blocked.insert(
                    d.p.id.clone(),
                    format!("the promote to node {to} stopped: {}", f.message()),
                );
                self.failovers.set_blocked(blocked);
            }
        }
    }

    /// `pigeonhole_cluster_failovers_total` and `pigeonhole_cluster_writer_term`, from the
    /// map this node holds: every node shows the same counts (simp724's N2).
    pub(crate) fn render_failover_metrics(&self, text: &mut String) {
        use std::fmt::Write as _;
        let Ok(map) = self.get_map() else { return };
        let _ = writeln!(
            text,
            "# HELP pigeonhole_cluster_failovers_total Failovers of each partition to a \
             replica, by the map's writer (D330).\n# TYPE pigeonhole_cluster_failovers_total \
             counter"
        );
        for p in &map.partitions {
            let _ = writeln!(
                text,
                "pigeonhole_cluster_failovers_total{{partition=\"{}\"}} {}",
                p.id, p.failover_count
            );
        }
        let _ = writeln!(
            text,
            "# HELP pigeonhole_cluster_writer_term The map's writer term (D330).\n# TYPE \
             pigeonhole_cluster_writer_term gauge\npigeonhole_cluster_writer_term {}",
            map.writer_term
        );
    }

    /// Node `id`'s address in this node's map.
    fn address_of(&self, id: &str) -> Result<String, Failure> {
        let map = self.get_map().map_err(|e| Failure::usage(&e.message))?;
        map.nodes
            .iter()
            .find(|n| n.id == id)
            .map(|n| n.address.clone())
            .ok_or_else(|| Failure::usage(&format!("the map has no node {id:?}")))
    }
}

/// [`Promoter`] over the writer's peers. It holds the node weakly (simp724's N2 on #2069): a
/// node closed during a promote ends it at its next step, rather than the close waiting out
/// the promote's waits.
struct NodeOps {
    node: std::sync::Weak<Node>,
}

impl NodeOps {
    /// The node, while it's open and still the writer with its lease (perf287's N1 on
    /// #2069): a promote stops at its next step otherwise.
    fn node(&self) -> Result<Arc<Node>, Failure> {
        let node = self
            .node
            .upgrade()
            .ok_or_else(|| Failure::usage("this node closed during the failover"))?;
        let map = node.get_map().map_err(|e| Failure::usage(&e.message))?;
        if schema::writer(&map) != node.id {
            return Err(Failure::usage(
                "this node is no longer the map's writer: the failover stopped",
            ));
        }
        node.writer_may_change(&map)
            .map_err(|e| Failure::usage(&format!("the failover stopped: {}", e.message)))?;
        Ok(node)
    }

    /// Polls node `id`'s row for `partition` until `done`, for [`STEP_WAIT`].
    async fn wait_for(
        &self,
        id: &str,
        partition: &str,
        what: &str,
        done: impl Fn(Option<&proto::LocalPartition>) -> bool,
    ) -> Result<(), Failure> {
        let deadline = tokio::time::Instant::now() + STEP_WAIT;
        loop {
            // Stops at once if this node closed or lost its writer lease meanwhile.
            self.node()?;
            if let Ok(row) = self.row(id, partition).await
                && done(row.as_ref())
            {
                return Ok(());
            }
            if tokio::time::Instant::now() > deadline {
                return Err(Failure::usage(&format!(
                    "node {id:?} didn't {what} {partition} within {}s",
                    STEP_WAIT.as_secs()
                )));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

impl Promoter for NodeOps {
    async fn row(
        &self,
        id: &str,
        partition: &str,
    ) -> Result<Option<proto::LocalPartition>, Failure> {
        let node = self.node()?;
        let st = if id == node.id {
            let node = Arc::clone(&node);
            tokio::task::spawn_blocking(move || node.status())
                .await
                .map_err(|e| Failure::usage(&e.to_string()))?
        } else {
            let address = node.address_of(id)?;
            let timeout = node.peers.push_timeout;
            tokio::time::timeout(timeout, async {
                let mut c = node.client(&address).await?;
                c.status(node.request(StatusRequest::default())?)
                    .await
                    .map(tonic::Response::into_inner)
                    .map_err(|s| s.message().to_owned())
            })
            .await
            .map_err(|_| Failure::usage(&format!("node {id:?}: timed out")))?
            .map_err(|e| Failure::usage(&format!("node {id:?}: {e}")))?
        };
        Ok(st.partitions.into_iter().find(|lp| lp.id == partition))
    }

    async fn map(&self) -> Result<ClusterMap, Failure> {
        self.node()?
            .get_map()
            .map_err(|e| Failure::usage(&e.message))
    }

    async fn set_map(&self, m: ClusterMap) -> Result<promote::Set, Failure> {
        let expected = m.map_version.saturating_sub(1);
        self.node()?
            .set_map(SetMapRequest {
                copies: Vec::new(),
                expected_map_version: expected,
                map: Some(m),
                force: false,
            })
            .await
            // This node, the writer, holds it, and the pushes say who else does.
            .map(|r| promote::Set {
                version: r.map_version,
                held: 1 + r.pushed.iter().filter(|p| p.ok).count(),
            })
            .map_err(|e| Failure::usage(&e.message))
    }

    fn now(&self) -> u64 {
        self.node
            .upgrade()
            .map_or_else(pigeonhole_io::sys::boot_clock_ns, |n| {
                n.grants.clock().now()
            })
    }

    async fn survey(
        &self,
        _wait: Duration,
    ) -> Result<HashMap<String, Option<NodeStatus>>, Failure> {
        // The writer's survey waits `--cluster-push-timeout-ms`, at most a sixth of the
        // lease, for each node already.
        let node = self.node()?;
        let map = node.get_map().map_err(|e| Failure::usage(&e.message))?;
        let mut statuses: HashMap<String, Option<NodeStatus>> =
            node.survey(&map).await.into_iter().collect();
        let own = Arc::clone(&node);
        let mine = tokio::task::spawn_blocking(move || own.status())
            .await
            .map_err(|e| Failure::usage(&e.to_string()))?;
        statuses.insert(node.id.clone(), Some(mine));
        Ok(statuses)
    }

    async fn stamp(&self, id: &str, req: StampRequest) -> Result<(), Failure> {
        let node = self.node()?;
        if id == node.id {
            return node
                .stamp(req)
                .await
                .map(|_| ())
                .map_err(|e| Failure::usage(&format!("stamp: {}", e.message)));
        }
        let address = node.address_of(id)?;
        let mut c = node
            .client(&address)
            .await
            .map_err(|e| Failure::usage(&e))?;
        c.stamp(node.request(req).map_err(|e| Failure::usage(&e))?)
            .await
            .map_err(|s| Failure::usage(&format!("node {id:?}: stamp: {}", s.message())))?;
        Ok(())
    }

    async fn wait_closed(&self, id: &str, partition: &str) -> Result<(), Failure> {
        self.wait_for(id, partition, "close its replica copy of", |row| {
            row.is_none_or(|lp| lp.role() != PartitionRole::Replica)
        })
        .await
    }

    async fn wait_serving(&self, id: &str, partition: &str, epoch: u64) -> Result<(), Failure> {
        self.wait_for(id, partition, "serve", |row| {
            row.is_some_and(|lp| lp.state() == LocalPartitionState::Serving && lp.epoch == epoch)
        })
        .await
    }

    fn after_stamp(&self) -> Result<(), Failure> {
        #[cfg(test)]
        {
            let mut armed = STOP_AFTER_STAMP
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let dir = self.node.upgrade().map(|n| n.dir.clone());
            if let Some(i) = armed.iter().position(|d| Some(d) == dir.as_ref()) {
                armed.swap_remove(i);
                return Err(Failure::usage(
                    "the test's fault after the failover's stamp",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn down(id: &str, kept: Option<&str>) -> Down {
        Down {
            p: proto::Partition {
                id: id.into(),
                node: "c".into(),
                ..Default::default()
            },
            renewed: None,
            in_doubt_xids: Vec::new(),
            in_doubt_count: 0,
            kept: kept.map(str::to_owned),
        }
    }

    /// perf2's S2 on #2069: P1, blocked and not due, goes through the survey that P2, newly
    /// down, brings, so its reason is refreshed; with neither due, both keep theirs.
    #[test]
    fn a_survey_any_partition_is_due_takes_every_down_partition() {
        let both = survey_or_keep(vec![down("p1", Some("waiting")), down("p2", None)])
            .map(|d| d.into_iter().map(|d| d.p.id).collect::<Vec<_>>());
        assert_eq!(both, Ok(vec!["p1".to_owned(), "p2".to_owned()]));
        let kept =
            survey_or_keep(vec![down("p1", Some("a")), down("p2", Some("b"))]).map(|d| d.len());
        assert_eq!(
            kept,
            Err(vec![
                ("p1".to_owned(), "a".to_owned()),
                ("p2".to_owned(), "b".to_owned())
            ])
        );
        assert_eq!(survey_or_keep(Vec::new()).map(|d| d.len()), Ok(0));
    }

    /// A partition is due again at its own time (simp724's nit: a grant's end, else the
    /// margin), or on a map change; one the last survey didn't take is due at once.
    #[test]
    fn a_blocked_partition_is_due_at_its_own_time_or_on_a_map_change() {
        let f = Failovers::default();
        f.note_surveyed(vec![("p1".into(), 500), ("p2".into(), 2_000)], 7);
        assert!(!f.survey_due("p1", 499, 7));
        assert!(f.survey_due("p1", 500, 7));
        assert!(!f.survey_due("p2", 500, 7));
        assert!(f.survey_due("p2", 500, 8), "a map change");
        assert!(f.survey_due("p3", 0, 7), "not in the last survey");
        // The next survey's stamps replace them: one it didn't take keeps none.
        f.note_surveyed(vec![("p2".into(), 3_000)], 7);
        assert!(f.survey_due("p1", 0, 7));
    }

    /// D331: a partition a range move holds from its hand-over on is blocked from failover,
    /// with the move named; before the hand-over it isn't held by the move.
    #[test]
    fn a_range_move_from_its_hand_over_blocks_failover_of_its_source_and_target() {
        use proto::{MoveKind, MovePhase, RangeMove};
        let d = down("p1", None);
        let at = |phase: MovePhase, from: &str, to: &str| {
            let mut r = RangeMove {
                id: "mv1".into(),
                kind: MoveKind::Range as i32,
                table: "t".into(),
                from: from.into(),
                to: to.into(),
                ..Default::default()
            };
            r.set_phase(phase);
            ClusterMap {
                partitions: vec![d.p.clone()],
                moves: vec![r],
                ..Default::default()
            }
        };
        let none = HashMap::new();
        for (from, to) in [("p1", "p2"), ("p2", "p1")] {
            for phase in [
                MovePhase::HandingOver,
                MovePhase::HandedOver,
                MovePhase::Cleaning,
            ] {
                let why = qualify(&at(phase, from, to), &d.p, None, &none, 0, None).unwrap_err();
                assert!(why.contains("range move mv1"), "{phase:?}: {why}");
            }
            let why = qualify(
                &at(MovePhase::Copying, from, to),
                &d.p,
                None,
                &none,
                0,
                None,
            )
            .err()
            .unwrap_or_default();
            assert!(!why.contains("range move"), "{why}");
        }
    }

    /// #2152: a pending promote withholds P's lease from its owner, so an owner that came
    /// back waits fenced; the writer abandons the promote once the owner asked it for the
    /// lease and answers waiting, and puts an unstamped copy back among P's replicas.
    #[test]
    fn a_pending_promote_is_abandoned_once_its_owner_waits_for_the_lease() {
        let mut map = ClusterMap {
            map_version: 7,
            partitions: vec![proto::Partition {
                id: "p1".into(),
                node: "c".into(),
                epoch: 3,
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut e = proto::FailoverEvent {
            partition: "p1".into(),
            from: "c".into(),
            to: "b".into(),
            to_epoch: 4,
            ..Default::default()
        };
        e.set_kind(FailoverKind::Partition);
        e.set_state(FailoverState::Pending);
        map.failovers.push(e);
        let row = |epoch, state: LocalPartitionState, reason: proto::FenceReason| {
            let mut lp = proto::LocalPartition {
                id: "p1".into(),
                epoch,
                ..Default::default()
            };
            lp.set_role(PartitionRole::Owner);
            lp.set_state(state);
            lp.set_fence_reason(reason);
            NodeStatus {
                partitions: vec![lp],
                ..Default::default()
            }
        };
        let waits = row(
            3,
            LocalPartitionState::Fenced,
            proto::FenceReason::AwaitingPeerMap,
        );
        let unstamped = row(
            3,
            LocalPartitionState::Closed,
            proto::FenceReason::Unspecified,
        );
        let statuses: HashMap<String, NodeStatus> =
            [("c".into(), waits.clone()), ("b".into(), unstamped)].into();
        let p = map.partitions[0].clone();
        assert!(owner_ready(&map, &p, &statuses, true));
        let m = abandoned_for_owner(&map, std::slice::from_ref(&p), &statuses);
        assert_eq!(m.map_version, 8);
        assert_eq!(m.failovers[0].state(), FailoverState::Abandoned);
        assert_eq!(m.partitions[0].replicas, ["b"]);
        // An owner that answers but hasn't asked for the lease (it can't send: a one-way
        // cut) may not be able to renew: the promote goes on.
        assert!(!owner_ready(&map, &p, &statuses, false));
        // A stamped copy that answers is abandoned too, as on a renewal (#2070): it never
        // served. It stays out of the replicas, an orphan for the operator.
        let stamped = row(
            4,
            LocalPartitionState::Closed,
            proto::FenceReason::Unspecified,
        );
        let s: HashMap<String, NodeStatus> =
            [("c".into(), waits.clone()), ("b".into(), stamped)].into();
        assert!(owner_ready(&map, &p, &s, true));
        assert!(
            abandoned_for_owner(&map, std::slice::from_ref(&p), &s).partitions[0]
                .replicas
                .is_empty()
        );
        // An owner the operator fenced isn't waiting for the lease.
        let fenced = row(3, LocalPartitionState::Fenced, proto::FenceReason::Operator);
        let s: HashMap<String, NodeStatus> = [("c".into(), fenced)].into();
        assert!(!owner_ready(&map, &p, &s, true));
        // A copy that doesn't answer may be stamped: abandoned, but not put back.
        let s: HashMap<String, NodeStatus> = [("c".into(), waits)].into();
        assert!(owner_ready(&map, &p, &s, true));
        let m = abandoned_for_owner(&map, &[p], &s);
        assert_eq!(m.failovers[0].state(), FailoverState::Abandoned);
        assert!(m.partitions[0].replicas.is_empty());
    }

    /// The still-granting grant comes from another node only: the owner's own grant to
    /// itself isn't a majority's.
    #[test]
    fn only_another_nodes_grant_to_the_owner_holds_a_partition() {
        let grant = |ms| proto::LeaseWithheld {
            partition: "p1".into(),
            holder: "c".into(),
            remaining_ms: ms,
        };
        let st = |grants| NodeStatus {
            grants,
            ..Default::default()
        };
        let d = down("p1", None);
        let own: HashMap<String, NodeStatus> = [("c".to_owned(), st(vec![grant(900)]))].into();
        assert!(still_granted(&d.p, &own).is_none());
        let other: HashMap<String, NodeStatus> = [
            ("b".to_owned(), st(vec![grant(400)])),
            ("c".to_owned(), st(vec![grant(900)])),
        ]
        .into();
        let (id, g) = still_granted(&d.p, &other).unwrap();
        assert_eq!((id.as_str(), g.remaining_ms), ("b", 400));
    }
}
