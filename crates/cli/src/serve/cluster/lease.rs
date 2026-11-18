//! Partition leases (D326): a partition serves on its node only while a majority of the
//! map's nodes grant it, each for `--cluster-lease-ms` (L) on its own clock.
//!
//! **A grantor** ([`Grants::grant`]) answers a node's lease round (`Cluster.Lease`, and a
//! node's own vote through the same table): at its own map's version only, for the
//! partitions that map gives the asking node at the epochs asked, until L after it received
//! the request. While its grant of a partition to one node is live, it withholds its vote
//! for that partition from any other: majorities intersect, so no second node can gather one
//! while the first's lease runs. Grants live in memory, so a node that starts grants nothing
//! for `L·(1+ρ)` (the start grace): it may have granted one just before it stopped.
//!
//! **A holder** ([`holder_end`]) takes `t0`, its clock before it sends a round's first
//! request. Once a majority grants, its lease runs until `t0 + L·(1−ρ)/(1+ρ)`: each grantor
//! received the request at or after `t0` and counts L on a clock no faster than `1+ρ`; the
//! holder counts on one no slower than `1−ρ`, so its lease ends, in real time, before the
//! earliest grant it relied on (audit120's Q5). ρ is each clock's drift against real time,
//! fixed at 1%.
//!
//! **The writer lease (D330)** is a pseudo-partition, [`WRITER_LEASE`], in the same rounds
//! and the same table, so its withheld votes and expiry are a partition's. A grantor renews
//! it only to the node its map names as writer, at the map's term. An **election vote**
//! ([`Grants::vote`]) is a request for it at a term above every one the grantor knows: the
//! grantor records that term durably before it answers, and from then refuses any lower
//! term's writer, lease or map (the term fence, D330 amendment 1's M1).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use super::proto::{
    ClusterMap, LeasePart, LeaseRequest, LeaseResponse, LeaseWithheld, ReleaseRequest,
};
use crate::serve::errors::ApiError;
use crate::serve::gate::LeaseClock;

/// The writer lease's name in a round (D330): a reserved partition id.
pub(crate) const WRITER_LEASE: &str = "__writer";

/// ρ, each clock's rate drift against real time, in parts per million: 1%.
const RHO_PPM: u64 = 10_000;

/// A holder's lease from a round that started at `t0`, with leases of `lease`: until
/// `t0 + L·(1−ρ)/(1+ρ)` on its clock.
pub(crate) fn holder_end(t0: u64, lease: Duration) -> u64 {
    let l = nanos(lease);
    t0.saturating_add(scale(l, 1_000_000 - RHO_PPM, 1_000_000 + RHO_PPM))
}

/// The start grace: `L·(1+ρ)`. A grant made just before a restart was to a holder whose
/// round started before it, so that holder stops within `L/(1+ρ)` of real time after it;
/// `L·(1+ρ)` on this node's clock is at least L of real time (audit120's Q5).
pub(crate) fn grace(lease: Duration) -> Duration {
    Duration::from_nanos(scale(nanos(lease), 1_000_000 + RHO_PPM, 1_000_000))
}

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// `v·num/den`, without overflow for any lease a flag can set.
fn scale(v: u64, num: u64, den: u64) -> u64 {
    u64::try_from(u128::from(v) * u128::from(num) / u128::from(den)).unwrap_or(u64::MAX)
}

/// A random id for this start of the node, never 0 (a request without one).
fn start_id() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    h.write_u32(std::process::id());
    h.finish().max(1)
}

fn millis(ns: u64) -> u64 {
    ns / 1_000_000
}

/// One node's grant here.
#[derive(Debug)]
struct Grant {
    /// The holder's start id (`LeaseRequest.start`): a request with another is a new start.
    start: u64,
    /// The holder's round it was made for.
    round: u64,
    /// The partitions granted, with their epochs.
    parts: BTreeMap<String, u64>,
    /// When it ends, on this node's clock.
    until: u64,
}

/// What a node last granted of one partition (D330 PR 2), kept past the grant's end: the
/// writer counts a partition's downtime from `until`, and names the in-doubt shares its
/// owner last reported when it fails the partition over.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LastGrant {
    /// The node it was granted to, and at what epoch.
    pub(crate) holder: String,
    pub(crate) epoch: u64,
    /// When the grant ends, on this node's lease clock.
    pub(crate) until: u64,
    /// The in-doubt xids and their count the holder sent with the request (F7).
    pub(crate) in_doubt_xids: Vec<String>,
    pub(crate) in_doubt_count: u64,
}

/// A node's grants: the leases it promised, by holder (itself included).
#[derive(Debug)]
pub(crate) struct Grants {
    clock: Arc<LeaseClock>,
    /// L.
    lease: Duration,
    /// When this node started, on its clock: it grants nothing until the start grace ends.
    started: u64,
    /// This start's id (`LeaseRequest.start`): random, so a grantor tells a restarted node
    /// from a delayed request of its last start.
    start: u64,
    by_holder: Mutex<BTreeMap<String, Grant>>,
    /// The highest writer term this node voted for (D330), as its `cluster-writer-term` file
    /// holds it: a vote is written there before it's answered.
    voted: Mutex<u64>,
    /// When the last writer lease granted here ends, on this node's clock: the start
    /// grace's end before any. How long the writer has gone without one is counted from it.
    writer_until: Mutex<u64>,
    /// Each partition's last grant here (D330 PR 2), by partition id: never dropped when it
    /// ends, only replaced by the next. Locked after `by_holder`, never before.
    last: Mutex<BTreeMap<String, LastGrant>>,
}

impl Grants {
    /// A starting node's grants: none, and the start grace from now.
    pub(crate) fn new(clock: Arc<LeaseClock>, lease: Duration) -> Self {
        let started = clock.now();
        Self {
            clock,
            lease,
            started,
            start: start_id(),
            by_holder: Mutex::new(BTreeMap::new()),
            voted: Mutex::new(0),
            writer_until: Mutex::new(started.saturating_add(nanos(grace(lease)))),
            last: Mutex::new(BTreeMap::new()),
        }
    }

    /// Grants that start with `term` voted for, from the node's `cluster-writer-term` file.
    pub(crate) fn with_voted(self, term: u64) -> Self {
        *self.voted.lock().unwrap_or_else(PoisonError::into_inner) = term;
        self
    }

    /// Every partition lease granted here that still runs, to whom, and for how long
    /// (`NodeStatus.grants`, D330 PR 2): the writer asks before it fails a partition over.
    /// Not the writer lease.
    pub(crate) fn granted(&self) -> Vec<LeaseWithheld> {
        let now = self.clock.now();
        let by = self
            .by_holder
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut out = Vec::new();
        for (holder, g) in by.iter().filter(|(_, g)| g.until > now) {
            for partition in g.parts.keys().filter(|p| *p != WRITER_LEASE) {
                out.push(LeaseWithheld {
                    partition: partition.clone(),
                    holder: holder.clone(),
                    remaining_ms: millis(g.until - now).max(1),
                });
            }
        }
        out
    }

    /// When partition `id`'s last grant here ends (or ended), if it was to `holder` at
    /// `epoch`: the failover check's per-round question, without cloning the grant.
    pub(crate) fn last_grant_until(&self, id: &str, holder: &str, epoch: u64) -> Option<u64> {
        self.last
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .filter(|g| g.holder == holder && g.epoch == epoch)
            .map(|g| g.until)
    }

    /// Partition `id`'s last grant here, ended or not (D330 PR 2).
    pub(crate) fn last_grant(&self, id: &str) -> Option<LastGrant> {
        self.last
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .cloned()
    }

    /// The highest writer term this node voted for (0: none).
    pub(crate) fn voted(&self) -> u64 {
        *self.voted.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The highest writer term this node knows: its map's, or one it voted for. A map or a
    /// writer lease below it is refused (the term fence).
    pub(crate) fn term_floor(&self, mine: &ClusterMap) -> u64 {
        self.voted().max(mine.writer_term)
    }

    /// How long ago the last writer lease granted here ended: zero while one runs.
    pub(crate) fn writer_lapsed(&self) -> Duration {
        let until = *self
            .writer_until
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Duration::from_nanos(self.clock.now().saturating_sub(until))
    }

    /// The time left on this node's grant of the writer lease to `holder`: zero if none runs.
    pub(crate) fn writer_remaining(&self, holder: &str) -> Duration {
        let now = self.clock.now();
        let by = self
            .by_holder
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        by.get(holder)
            .filter(|g| g.until > now && g.parts.contains_key(WRITER_LEASE))
            .map_or(Duration::ZERO, |g| Duration::from_nanos(g.until - now))
    }

    /// Notes a writer lease granted until `until`.
    fn writer_granted(&self, until: u64) {
        let mut w = self
            .writer_until
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *w = (*w).max(until);
    }

    /// This start's id.
    pub(crate) fn start(&self) -> u64 {
        self.start
    }

    /// L.
    pub(crate) fn lease(&self) -> Duration {
        self.lease
    }

    /// The node's lease clock.
    pub(crate) fn clock(&self) -> &Arc<LeaseClock> {
        &self.clock
    }

    /// The time left on the start grace: zero once it's over.
    pub(crate) fn grace_remaining(&self) -> Duration {
        let end = self.started.saturating_add(nanos(grace(self.lease)));
        Duration::from_nanos(end.saturating_sub(self.clock.now()))
    }

    /// L in whole milliseconds, as `LeaseRequest.lease_ms` carries it.
    pub(crate) fn lease_ms(&self) -> u64 {
        u64::try_from(self.lease.as_millis()).unwrap_or(u64::MAX)
    }

    /// Answers `req`, from node `req.node_id`, against this node's map `mine` (D326's
    /// grantor rules, in the module's doc). The cluster id was checked by the caller.
    /// `FailedPrecondition` when the asking node's lease isn't this node's (audit120's M1):
    /// the holder's bound holds only for a lease no longer than each grantor's.
    ///
    /// No grace exemption, even for a map that names this node alone (audit120's S4): a
    /// forced map that dropped the other nodes clears `previous_nodes`, so nothing tells a
    /// node that was always alone from one whose grant to a peer may still run.
    pub(crate) fn grant(
        &self,
        mine: &ClusterMap,
        req: &LeaseRequest,
    ) -> Result<LeaseResponse, ApiError> {
        if req.lease_ms != self.lease_ms() {
            return Err(ApiError::failed_precondition(format!(
                "node {} runs with --cluster-lease-ms {} and this node with {}: every node of a \
                 cluster must take the same lease, so this node grants it nothing",
                req.node_id,
                req.lease_ms,
                self.lease_ms()
            )));
        }
        let mut out = LeaseResponse::default();
        // Maps sort by `(writer_term, map_version)` (D330): an old writer's minority map is
        // behind an elected one whatever its version.
        let (theirs, ours) = (
            (req.writer_term, req.map_version),
            (mine.writer_term, mine.map_version),
        );
        if ours > theirs {
            out.newer_map_version = mine.map_version;
            return Ok(out);
        }
        // Behind the asking node: it pushes its map in its round before it asks, so this is
        // a race with that push. Nothing is granted; its next round asks again.
        if ours < theirs {
            return Ok(out);
        }
        let grace = self.grace_remaining();
        if !grace.is_zero() {
            out.grace_remaining_ms = u64::try_from(grace.as_millis()).unwrap_or(u64::MAX).max(1);
            return Ok(out);
        }
        let now = self.clock.now();
        let mut by = self
            .by_holder
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        by.retain(|_, g| g.until > now);
        // An older round of the same start of the holder, arriving after a newer one:
        // ignored, so it can't put back what the newer one no longer asked for. Another
        // start's request replaces the grant whatever its round (a node that starts holds no
        // lease, and its rounds count from the start again: audit120's S-b).
        if let Some(g) = by.get(&req.node_id)
            && g.start == req.start
            && g.round > req.round
        {
            eprintln!(
                "phdb serve: node {}'s lease round {} arrived after its round {}: ignored",
                req.node_id, req.round, g.round
            );
            return Ok(out);
        }
        let mut parts = BTreeMap::new();
        let voted = self.voted();
        let until = now.saturating_add(nanos(self.lease));
        let mut last = Vec::new();
        for LeasePart {
            partition,
            epoch,
            term,
            in_doubt_xids,
            in_doubt_count,
        } in &req.parts
        {
            let given = if partition == WRITER_LEASE {
                // The writer lease (D330): to the map's writer at the map's term, unless this
                // node voted for a higher one; or renewed to the node it voted for, at that
                // term, while its vote's grant runs (a candidate before its map lands).
                let writer = super::schema::writer(mine) == req.node_id
                    && *term == mine.writer_term
                    && *term >= voted;
                let candidate = *term == voted
                    && voted > mine.writer_term
                    && by.get(&req.node_id).is_some_and(|g| {
                        g.start == req.start && g.parts.get(WRITER_LEASE) == Some(term)
                    });
                writer || candidate
            } else {
                mine.partitions
                    .iter()
                    .any(|p| p.id == *partition && p.node == req.node_id && p.epoch == *epoch)
            };
            if !given {
                continue;
            }
            // A partition's grant is keyed by its epoch, the writer lease's by its term.
            let key = if partition == WRITER_LEASE {
                *term
            } else {
                *epoch
            };
            let other = by
                .iter()
                .filter(|(holder, _)| **holder != req.node_id)
                .find(|(_, g)| g.parts.contains_key(partition));
            if let Some((holder, g)) = other {
                out.withheld.push(LeaseWithheld {
                    partition: partition.clone(),
                    holder: holder.clone(),
                    remaining_ms: millis(g.until - now).max(1),
                });
                continue;
            }
            parts.insert(partition.clone(), key);
            out.granted.push(LeasePart {
                partition: partition.clone(),
                epoch: *epoch,
                term: *term,
                ..LeasePart::default()
            });
            if partition != WRITER_LEASE {
                last.push((
                    partition.clone(),
                    LastGrant {
                        holder: req.node_id.clone(),
                        epoch: *epoch,
                        until,
                        in_doubt_xids: in_doubt_xids.clone(),
                        in_doubt_count: *in_doubt_count,
                    },
                ));
            }
        }
        if !last.is_empty() {
            self.last
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend(last);
        }
        if parts.contains_key(WRITER_LEASE) {
            self.writer_granted(until);
        }
        // The new grant replaces the holder's last one: what it no longer asks for, or that
        // the map no longer gives it, is no longer granted.
        by.insert(
            req.node_id.clone(),
            Grant {
                start: req.start,
                round: req.round,
                parts,
                until,
            },
        );
        Ok(out)
    }

    /// An election vote (D330): `req` asks for the writer lease alone, at a term above the
    /// map's `mine`. Granted, like a lease and whatever the request's map version (the
    /// voters may hold different ones), when this node's start grace is over, no other
    /// node's writer lease runs here (the withheld vote), and the term is above every one this
    /// node knows; `persist` then records the term durably before the answer leaves (the
    /// term fence must survive a restart). Otherwise the answer says why: the withheld
    /// holder, the grace left, or this node's highest term in `writer_term`.
    ///
    /// The vote joins the candidate's grant here rather than replacing it: dropping its
    /// partitions would free them to another node while the candidate still serves them.
    pub(crate) fn vote(
        &self,
        mine: &ClusterMap,
        req: &LeaseRequest,
        persist: impl FnOnce(u64) -> std::io::Result<()>,
    ) -> Result<LeaseResponse, ApiError> {
        self.ballot(mine, req, None, persist)
    }

    /// An election's pre-vote (D330 PR 2, `LeaseRequest.pre_vote`): [`Grants::vote`]'s answer
    /// without its effects. Nothing is recorded, granted or fenced. It's also refused while
    /// the writer lease ran out here less than `margin` ago (simp724), so a node whose writer
    /// is healthy doesn't help a candidate that merely can't reach it.
    pub(crate) fn pre_vote(
        &self,
        mine: &ClusterMap,
        req: &LeaseRequest,
        margin: Duration,
    ) -> Result<LeaseResponse, ApiError> {
        self.ballot(mine, req, Some(margin), |_| Ok(()))
    }

    /// [`Grants::vote`], or with `pre_vote` (the failover margin) [`Grants::pre_vote`].
    fn ballot(
        &self,
        mine: &ClusterMap,
        req: &LeaseRequest,
        pre_vote: Option<Duration>,
        persist: impl FnOnce(u64) -> std::io::Result<()>,
    ) -> Result<LeaseResponse, ApiError> {
        let [part] = req.parts.as_slice() else {
            return Err(ApiError::invalid(
                "an election vote asks for the writer lease alone",
            ));
        };
        if part.partition != WRITER_LEASE {
            return Err(ApiError::invalid(
                "an election vote asks for the writer lease alone",
            ));
        }
        if req.lease_ms != self.lease_ms() {
            return Err(ApiError::failed_precondition(format!(
                "node {} runs with --cluster-lease-ms {} and this node with {}: every node of a \
                 cluster must take the same lease, so this node grants it nothing",
                req.node_id,
                req.lease_ms,
                self.lease_ms()
            )));
        }
        let mut out = LeaseResponse::default();
        let grace = self.grace_remaining();
        if !grace.is_zero() {
            out.grace_remaining_ms = u64::try_from(grace.as_millis()).unwrap_or(u64::MAX).max(1);
            return Ok(out);
        }
        let now = self.clock.now();
        let mut by = self
            .by_holder
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        by.retain(|_, g| g.until > now);
        let mut voted = self.voted.lock().unwrap_or_else(PoisonError::into_inner);
        let floor = (*voted).max(mine.writer_term);
        if part.term <= floor {
            out.writer_term = floor;
            return Ok(out);
        }
        let other = by
            .iter()
            .filter(|(holder, _)| **holder != req.node_id)
            .find(|(_, g)| g.parts.contains_key(WRITER_LEASE));
        if let Some((holder, g)) = other {
            out.withheld.push(LeaseWithheld {
                partition: WRITER_LEASE.to_owned(),
                holder: holder.clone(),
                remaining_ms: millis(g.until - now).max(1),
            });
            out.writer_term = floor;
            return Ok(out);
        }
        if let Some(margin) = pre_vote {
            let lapsed = self.writer_lapsed();
            if lapsed < margin {
                out.withheld.push(LeaseWithheld {
                    partition: WRITER_LEASE.to_owned(),
                    holder: super::schema::writer(mine).to_owned(),
                    remaining_ms: millis(nanos(margin - lapsed)).max(1),
                });
                out.writer_term = floor;
                return Ok(out);
            }
            out.granted.push(part.clone());
            out.writer_term = part.term;
            return Ok(out);
        }
        persist(part.term)
            .map_err(|e| ApiError::internal(format!("record the writer term voted for: {e}")))?;
        *voted = part.term;
        let until = now.saturating_add(nanos(self.lease));
        self.writer_granted(until);
        match by.get_mut(&req.node_id) {
            Some(g) if g.start == req.start => {
                g.parts.insert(WRITER_LEASE.to_owned(), part.term);
                g.round = g.round.max(req.round);
                g.until = g.until.max(until);
            }
            _ => {
                by.insert(
                    req.node_id.clone(),
                    Grant {
                        start: req.start,
                        round: req.round,
                        parts: BTreeMap::from([(WRITER_LEASE.to_owned(), part.term)]),
                        until,
                    },
                );
            }
        }
        out.granted.push(part.clone());
        out.writer_term = part.term;
        Ok(out)
    }

    /// `Cluster.Release` (D326 PR 2): node `req.node_id` closed the partitions it names, so
    /// its grant here drops them now. Only a grant of the same start, at a round no older
    /// than the grant's: a delayed release from an earlier start, or one that a newer lease
    /// round already passed, would end a grant that's live, and the holder may be serving
    /// under it. Whether anything was dropped.
    pub(crate) fn release(&self, req: &ReleaseRequest) -> bool {
        let now = self.clock.now();
        let mut by = self
            .by_holder
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        by.retain(|_, g| g.until > now);
        let Some(g) = by.get_mut(&req.node_id) else {
            return false;
        };
        if g.start != req.start || g.round > req.round {
            eprintln!(
                "phdb serve: node {}'s release (start {}, round {}) is older than its grant \
                 here (start {}, round {}): ignored",
                req.node_id, req.start, req.round, g.start, g.round
            );
            return false;
        }
        // From here, a lease request of an older round is ignored too, so it can't put back
        // what this release dropped.
        g.round = req.round;
        let before = g.parts.len();
        for LeasePart {
            partition,
            epoch,
            term,
            ..
        } in &req.parts
        {
            // The writer lease is keyed by its term (D330).
            let key = if partition == WRITER_LEASE {
                term
            } else {
                epoch
            };
            if g.parts.get(partition) == Some(key) {
                g.parts.remove(partition);
            }
        }
        g.parts.len() < before
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::proto::{Node as MapNode, Partition};

    fn map(version: u64, owners: &[(&str, &str, u64)]) -> ClusterMap {
        ClusterMap {
            cluster_id: "c1".into(),
            map_version: version,
            nodes: ["a", "b", "c"]
                .iter()
                .map(|n| MapNode {
                    id: (*n).into(),
                    address: format!("127.0.0.1:{}", 1000 + u16::from(n.as_bytes()[0])),
                })
                .collect(),
            partitions: owners
                .iter()
                .map(|(id, node, epoch)| Partition {
                    id: (*id).into(),
                    node: (*node).into(),
                    epoch: *epoch,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// A request from node `node`'s first start, with the tests' 10 s lease.
    fn ask(node: &str, version: u64, round: u64, parts: &[(&str, u64)]) -> LeaseRequest {
        LeaseRequest {
            cluster_id: "c1".into(),
            node_id: node.into(),
            map_version: version,
            round,
            lease_ms: 10_000,
            start: 1,
            writer_term: 0,
            pre_vote: false,
            parts: parts
                .iter()
                .map(|(p, e)| LeasePart {
                    partition: (*p).into(),
                    epoch: *e,
                    term: 0,
                    ..LeasePart::default()
                })
                .collect(),
        }
    }

    /// `g`'s answer, which the tests' requests never make an error.
    fn grant(g: &Grants, mine: &ClusterMap, req: &LeaseRequest) -> LeaseResponse {
        g.grant(mine, req).unwrap()
    }

    fn granted(r: &LeaseResponse) -> Vec<&str> {
        r.granted.iter().map(|p| p.partition.as_str()).collect()
    }

    /// Grants on a clock past their start grace.
    fn grants(lease: Duration) -> (Grants, Arc<LeaseClock>) {
        let clock = Arc::new(LeaseClock::default());
        let g = Grants::new(Arc::clone(&clock), lease);
        clock.advance(grace(lease));
        (g, clock)
    }

    #[test]
    fn the_holders_bound_and_the_grace_keep_rho_on_both_sides() {
        let l = Duration::from_secs(10);
        // 10 s · 0.99 / 1.01 and 10 s · 1.01.
        assert_eq!(holder_end(5, l), 5 + 9_801_980_198);
        assert_eq!(grace(l), Duration::from_millis(10_100));
    }

    #[test]
    fn a_grantor_withholds_a_partition_from_another_node_while_its_grant_runs() {
        // D326's one rule for the count: while b's grant of p1 to a is live, b withholds
        // its vote for p1 from any other node, and says how long is left.
        let (g, clock) = grants(Duration::from_secs(10));
        let v1 = map(1, &[("p1", "a", 1), ("p2", "b", 1)]);
        let r = grant(&g, &v1, &ask("a", 1, 1, &[("p1", 1)]));
        assert_eq!(granted(&r), ["p1"]);
        // v2 relocates p1 to c at epoch 2 without a (cut off): c asks while a's grant runs.
        let v2 = map(2, &[("p1", "c", 2), ("p2", "b", 1)]);
        clock.advance(Duration::from_secs(3));
        let r = grant(&g, &v2, &ask("c", 2, 1, &[("p1", 2)]));
        assert!(r.granted.is_empty(), "{r:?}");
        assert_eq!(r.withheld.len(), 1);
        assert_eq!(r.withheld[0].holder, "a");
        assert!(
            (6_900..=7_000).contains(&r.withheld[0].remaining_ms),
            "{r:?}"
        );
        // Once it ran out, c's lease is granted.
        clock.advance(Duration::from_secs(7));
        assert_eq!(
            granted(&grant(&g, &v2, &ask("c", 2, 2, &[("p1", 2)]))),
            ["p1"]
        );
        // And a, asking at v1 again, is told of v2 and granted nothing.
        let r = grant(&g, &v2, &ask("a", 1, 2, &[("p1", 1)]));
        assert_eq!(r.newer_map_version, 2);
        assert!(r.granted.is_empty());
    }

    #[test]
    fn a_grant_is_only_what_the_map_gives_the_asker_at_the_versions_asked() {
        let (g, _clock) = grants(Duration::from_secs(10));
        let v3 = map(3, &[("p1", "a", 2), ("p2", "b", 1)]);
        // Another node's partition, or another epoch: neither granted nor withheld.
        let r = grant(&g, &v3, &ask("a", 3, 1, &[("p1", 1), ("p2", 1)]));
        assert!(r.granted.is_empty() && r.withheld.is_empty(), "{r:?}");
        assert_eq!(
            granted(&grant(&g, &v3, &ask("a", 3, 2, &[("p1", 2)]))),
            ["p1"]
        );
        // A grantor behind the asker grants nothing (the asker's push hasn't landed).
        let r = grant(&g, &v3, &ask("a", 4, 3, &[("p1", 2)]));
        assert!(r.granted.is_empty() && r.newer_map_version == 0, "{r:?}");
    }

    #[test]
    fn a_renewal_replaces_the_last_grant_and_an_older_round_is_ignored() {
        // A grant covers what the holder asked in its last round (audit120's S2): a part it
        // no longer asks for is free for another node at once. A delayed request of an
        // older round can't put it back.
        let (g, _clock) = grants(Duration::from_secs(10));
        let v1 = map(1, &[("p1", "a", 1), ("p2", "a", 1)]);
        assert_eq!(
            granted(&grant(&g, &v1, &ask("a", 1, 5, &[("p1", 1), ("p2", 1)]))).len(),
            2
        );
        assert_eq!(
            granted(&grant(&g, &v1, &ask("a", 1, 6, &[("p1", 1)]))),
            ["p1"]
        );
        assert!(granted(&grant(&g, &v1, &ask("a", 1, 5, &[("p2", 1)]))).is_empty());
        let v2 = map(2, &[("p1", "a", 1), ("p2", "b", 2)]);
        assert_eq!(
            granted(&grant(&g, &v2, &ask("b", 2, 1, &[("p2", 2)]))),
            ["p2"]
        );
    }

    #[test]
    fn a_starting_node_grants_nothing_until_its_grace_ends() {
        // Grants live in memory: a restarted node may have granted a lease just before it
        // stopped, so it grants none, its own included, for L·(1+ρ). A map that names it
        // alone is no exception (audit120's S4).
        let lease = Duration::from_secs(10);
        let clock = Arc::new(LeaseClock::default());
        let g = Grants::new(Arc::clone(&clock), lease);
        let alone = ClusterMap {
            nodes: vec![MapNode {
                id: "a".into(),
                address: "127.0.0.1:1".into(),
            }],
            ..map(1, &[("p1", "a", 1)])
        };
        let r = grant(&g, &alone, &ask("a", 1, 1, &[("p1", 1)]));
        assert!(r.granted.is_empty());
        assert!((10_000..=10_100).contains(&r.grace_remaining_ms), "{r:?}");
        clock.advance(Duration::from_millis(10_100));
        assert!(g.grace_remaining().is_zero());
        assert_eq!(
            granted(&grant(&g, &alone, &ask("a", 1, 2, &[("p1", 1)]))),
            ["p1"]
        );
    }

    #[test]
    fn a_grantor_refuses_a_holder_with_another_lease_and_a_new_start_replaces_its_grant() {
        // audit120's M1: a holder whose lease is longer than its grantors' would serve past
        // their grants, so a different length is refused, naming both.
        let (g, _clock) = grants(Duration::from_secs(10));
        let v1 = map(1, &[("p1", "a", 1), ("p2", "a", 1)]);
        let e = g
            .grant(
                &v1,
                &LeaseRequest {
                    lease_ms: 30_000,
                    ..ask("a", 1, 1, &[("p1", 1)])
                },
            )
            .unwrap_err();
        assert_eq!(e.code, "FailedPrecondition", "{}", e.message);
        assert!(
            e.message.contains("30000") && e.message.contains("10000"),
            "{}",
            e.message
        );
        // audit120's S-b: a restarted node's rounds start again at 1. Its old start's
        // round 9 doesn't make them stale: a new start replaces the grant.
        assert_eq!(
            grant(&g, &v1, &ask("a", 1, 9, &[("p1", 1), ("p2", 1)]))
                .granted
                .len(),
            2
        );
        let restarted = LeaseRequest {
            start: 2,
            ..ask("a", 1, 1, &[("p1", 1)])
        };
        assert_eq!(granted(&grant(&g, &v1, &restarted)), ["p1"]);
        // And the old start's delayed round 9 is now another start's, so it replaces again:
        // still a grant to a, never one that keeps p2 from a new owner past its lease.
        let v2 = map(2, &[("p1", "a", 1), ("p2", "b", 2)]);
        assert_eq!(
            granted(&grant(&g, &v2, &ask("b", 2, 1, &[("p2", 2)]))),
            ["p2"]
        );
    }

    fn release(node: &str, start: u64, round: u64, parts: &[(&str, u64)]) -> ReleaseRequest {
        ReleaseRequest {
            cluster_id: "c1".into(),
            node_id: node.into(),
            round,
            parts: parts
                .iter()
                .map(|(p, e)| LeasePart {
                    partition: (*p).into(),
                    epoch: *e,
                    term: 0,
                    ..LeasePart::default()
                })
                .collect(),
            start,
        }
    }

    #[test]
    fn a_release_frees_a_partition_at_once_and_a_lease_round_older_than_it_is_ignored() {
        // D326 PR 2: a planned move's old node closes P and releases it, so a grantor
        // stops withholding P at once rather than when the grant runs out.
        let (g, _clock) = grants(Duration::from_secs(10));
        let v1 = map(1, &[("p1", "a", 1), ("p2", "a", 1)]);
        assert_eq!(
            grant(&g, &v1, &ask("a", 1, 5, &[("p1", 1), ("p2", 1)]))
                .granted
                .len(),
            2
        );
        assert!(g.release(&release("a", 1, 6, &[("p1", 1)])));
        let v2 = map(2, &[("p1", "b", 2), ("p2", "a", 1)]);
        assert_eq!(
            granted(&grant(&g, &v2, &ask("b", 2, 1, &[("p1", 2)]))),
            ["p1"]
        );
        // a's lease round 5, delayed past its release, can't take p1 back: it's older.
        let r = grant(&g, &v1, &ask("a", 1, 5, &[("p1", 1), ("p2", 1)]));
        assert!(r.granted.is_empty() && r.newer_map_version == 0, "{r:?}");
    }

    #[test]
    fn a_delayed_release_from_a_previous_start_never_ends_a_live_grant() {
        // The coordinator's case on ReleaseRequest.start: node a releases p1 and restarts;
        // its new start is granted p1 again and serves under it. The old start's release,
        // delayed past that, must not drop p1 from the live grant, or another node could
        // gather p1's lease while a serves.
        let (g, _clock) = grants(Duration::from_secs(10));
        let v1 = map(1, &[("p1", "a", 1)]);
        assert_eq!(
            granted(&grant(&g, &v1, &ask("a", 1, 9, &[("p1", 1)]))),
            ["p1"]
        );
        let stale = release("a", 1, 10, &[("p1", 1)]);
        let restarted = LeaseRequest {
            start: 2,
            ..ask("a", 1, 1, &[("p1", 1)])
        };
        assert_eq!(granted(&grant(&g, &v1, &restarted)), ["p1"]);
        assert!(!g.release(&stale), "a release of the earlier start");
        // p1 is still a's here: b is refused it.
        let v2 = map(2, &[("p1", "b", 2)]);
        let r = grant(&g, &v2, &ask("b", 2, 1, &[("p1", 2)]));
        assert!(r.granted.is_empty(), "{r:?}");
        assert_eq!(r.withheld[0].holder, "a", "{r:?}");
        // The same start's release at an older round is ignored as well.
        let (g, _clock) = grants(Duration::from_secs(10));
        assert_eq!(
            granted(&grant(&g, &v1, &ask("a", 1, 9, &[("p1", 1)]))),
            ["p1"]
        );
        assert!(!g.release(&release("a", 1, 8, &[("p1", 1)])));
    }

    /// A request for the writer lease alone, at `term`, from node `node` whose map is at
    /// `(writer_term, version)`.
    fn ask_writer(
        node: &str,
        writer_term: u64,
        version: u64,
        round: u64,
        term: u64,
    ) -> LeaseRequest {
        LeaseRequest {
            writer_term,
            parts: vec![LeasePart {
                partition: WRITER_LEASE.into(),
                epoch: 0,
                term,
                ..LeasePart::default()
            }],
            ..ask(node, version, round, &[])
        }
    }

    /// A map at writer term `term`, naming `writer` (term 0: none).
    fn termed(version: u64, writer: &str, term: u64) -> ClusterMap {
        ClusterMap {
            writer: if term == 0 {
                String::new()
            } else {
                writer.into()
            },
            writer_term: term,
            ..map(version, &[("p1", "b", 1)])
        }
    }

    #[test]
    fn the_writer_lease_goes_only_to_the_maps_writer_at_its_term() {
        let (g, _clock) = grants(Duration::from_secs(10));
        // Term 0: the smallest id, a.
        let v1 = termed(1, "", 0);
        assert_eq!(
            granted(&grant(&g, &v1, &ask_writer("a", 0, 1, 1, 0))),
            [WRITER_LEASE]
        );
        assert!(granted(&grant(&g, &v1, &ask_writer("b", 0, 1, 1, 0))).is_empty());
        // Term 3, naming c: c at 3 only.
        let (g, _clock) = grants(Duration::from_secs(10));
        let v2 = termed(2, "c", 3);
        assert!(granted(&grant(&g, &v2, &ask_writer("a", 3, 2, 1, 3))).is_empty());
        assert!(granted(&grant(&g, &v2, &ask_writer("c", 3, 2, 1, 2))).is_empty());
        assert_eq!(
            granted(&grant(&g, &v2, &ask_writer("c", 3, 2, 2, 3))),
            [WRITER_LEASE]
        );
        assert!(g.writer_remaining("c") > Duration::ZERO);
        assert!(g.writer_lapsed().is_zero());
    }

    #[test]
    fn maps_sort_by_term_then_version_in_a_grant() {
        // simp724's S1 on the PR 1 plan: an old writer's map at a higher version but a lower
        // term is behind, so the grantor answers with its newer map rather than a mismatch.
        let (g, _clock) = grants(Duration::from_secs(10));
        let elected = termed(5, "b", 2);
        let r = grant(&g, &elected, &ask_writer("a", 1, 7, 1, 1));
        assert_eq!(r.newer_map_version, 5, "{r:?}");
        assert!(r.granted.is_empty());
        // And a grantor behind the asker by term grants nothing, whatever the versions.
        let r = grant(&g, &termed(9, "a", 1), &ask_writer("b", 2, 5, 1, 2));
        assert!(r.granted.is_empty() && r.newer_map_version == 0, "{r:?}");
    }

    #[test]
    fn a_pre_vote_records_nothing_and_waits_for_the_writer_lease_to_be_out_for_the_margin() {
        // D330 PR 2: a pre-vote answers as the vote would, and changes nothing.
        let (g, clock) = grants(Duration::from_secs(10));
        let v1 = termed(4, "a", 1);
        let margin = Duration::from_secs(5);
        let pre = |req: LeaseRequest| LeaseRequest {
            pre_vote: true,
            ..req
        };
        // a's writer lease runs here: withheld.
        assert_eq!(
            granted(&grant(&g, &v1, &ask_writer("a", 1, 4, 1, 1))),
            [WRITER_LEASE]
        );
        let r = g
            .pre_vote(&v1, &pre(ask_writer("b", 1, 4, 1, 2)), margin)
            .unwrap();
        assert!(r.granted.is_empty() && r.withheld[0].holder == "a", "{r:?}");
        // Out, but for less than the margin: still withheld, the margin's rest as its time.
        clock.advance(Duration::from_secs(12));
        let r = g
            .pre_vote(&v1, &pre(ask_writer("b", 1, 4, 2, 2)), margin)
            .unwrap();
        assert!(r.granted.is_empty(), "{r:?}");
        assert!(r.withheld[0].remaining_ms > 0 && r.withheld[0].remaining_ms <= 5_000);
        // Past the margin: it would vote, and nothing is recorded or granted.
        clock.advance(margin);
        let r = g
            .pre_vote(&v1, &pre(ask_writer("b", 1, 4, 3, 2)), margin)
            .unwrap();
        assert_eq!((granted(&r), r.writer_term), (vec![WRITER_LEASE], 2));
        assert_eq!(g.voted(), 0);
        assert!(g.writer_remaining("b").is_zero());
        assert!(g.writer_lapsed() >= margin);
        // c's pre-vote at the same term is answered alike: none was recorded for b.
        let r = g
            .pre_vote(&v1, &pre(ask_writer("c", 1, 4, 1, 2)), margin)
            .unwrap();
        assert_eq!(granted(&r), [WRITER_LEASE]);
        // A pre-vote at a term this node knows is refused with it.
        let r = g
            .pre_vote(&v1, &pre(ask_writer("c", 1, 4, 2, 1)), margin)
            .unwrap();
        assert!(r.granted.is_empty() && r.writer_term == 1, "{r:?}");
    }

    #[test]
    fn a_vote_is_recorded_before_it_is_answered_and_fences_lower_terms() {
        let (g, clock) = grants(Duration::from_secs(10));
        let v1 = termed(4, "a", 1);
        let recorded = std::cell::Cell::new(0);
        let vote = |g: &Grants, req: &LeaseRequest| {
            g.vote(&v1, req, |t| {
                recorded.set(t);
                Ok(())
            })
            .unwrap()
        };
        // Not a lone writer-lease request: refused.
        assert!(
            g.vote(&v1, &ask("b", 4, 1, &[("p1", 1)]), |_| Ok(()))
                .is_err()
        );
        // a's writer lease runs here: b's vote is withheld, and nothing recorded.
        assert_eq!(
            granted(&grant(&g, &v1, &ask_writer("a", 1, 4, 1, 1))),
            [WRITER_LEASE]
        );
        let r = vote(&g, &ask_writer("b", 1, 3, 1, 2));
        assert!(r.granted.is_empty() && r.withheld[0].holder == "a", "{r:?}");
        assert_eq!((recorded.get(), g.voted()), (0, 0));
        // Once it has run out, b is voted term 2, whatever its map version, and the term
        // is recorded first.
        clock.advance(Duration::from_secs(11));
        assert!(g.writer_lapsed() >= Duration::from_secs(1));
        let r = vote(&g, &ask_writer("b", 1, 3, 2, 2));
        assert_eq!(granted(&r), [WRITER_LEASE]);
        assert_eq!((recorded.get(), g.voted(), r.writer_term), (2, 2, 2));
        // c's vote at the same term is refused with the term to beat; a's renewal at term 1
        // too, as is any term at or below 2.
        let r = vote(&g, &ask_writer("c", 1, 4, 1, 2));
        assert!(r.granted.is_empty() && r.writer_term == 2, "{r:?}");
        assert!(granted(&grant(&g, &v1, &ask_writer("a", 1, 4, 2, 1))).is_empty());
        assert_eq!(g.term_floor(&v1), 2);
        // b renews its vote, as a candidate, before its map lands.
        let r = grant(&g, &v1, &ask_writer("b", 1, 4, 3, 2));
        assert_eq!(granted(&r), [WRITER_LEASE]);
        // A vote whose record fails isn't granted.
        clock.advance(Duration::from_secs(11));
        assert!(
            g.vote(&v1, &ask_writer("c", 1, 4, 2, 3), |_| Err(
                std::io::Error::other("disk")
            ))
            .is_err()
        );
        assert_eq!(g.voted(), 2);
    }

    #[test]
    fn a_vote_keeps_the_candidates_partition_grants() {
        // Dropping them would free p1 to another node while the candidate still serves it.
        let (g, _clock) = grants(Duration::from_secs(10));
        let v1 = map(3, &[("p1", "b", 1)]);
        assert_eq!(
            granted(&grant(&g, &v1, &ask("b", 3, 1, &[("p1", 1)]))),
            ["p1"]
        );
        let r = g
            .vote(&v1, &ask_writer("b", 0, 3, 2, 1), |_| Ok(()))
            .unwrap();
        assert_eq!(granted(&r), [WRITER_LEASE]);
        let other = grant(
            &g,
            &map(3, &[("p1", "c", 1)]),
            &ask("c", 3, 1, &[("p1", 1)]),
        );
        assert!(
            other.granted.is_empty() && other.withheld[0].holder == "b",
            "{other:?}"
        );
    }

    #[test]
    fn a_release_of_the_writer_lease_frees_it_at_once() {
        // simp724's N2: a writer the map replaced releases its lease, so the new writer
        // needn't wait out its grants.
        let (g, _clock) = grants(Duration::from_secs(10));
        let v1 = termed(1, "a", 1);
        assert_eq!(
            granted(&grant(&g, &v1, &ask_writer("a", 1, 1, 1, 1))),
            [WRITER_LEASE]
        );
        let rel = ReleaseRequest {
            parts: vec![LeasePart {
                partition: WRITER_LEASE.into(),
                epoch: 0,
                term: 1,
                ..LeasePart::default()
            }],
            ..release("a", 1, 2, &[])
        };
        assert!(g.release(&rel));
        assert!(g.writer_remaining("a").is_zero());
        let r = g
            .vote(&v1, &ask_writer("b", 1, 1, 1, 2), |_| Ok(()))
            .unwrap();
        assert_eq!(granted(&r), [WRITER_LEASE]);
    }
}
