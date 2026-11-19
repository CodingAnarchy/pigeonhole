//! The map's writer under a lease, with terms (D330 PR 1, ICR 0117's D330 section).
//!
//! **The writer lease** ([`super::lease::WRITER_LEASE`]) rides the writer's D326 lease
//! rounds: a majority grants it to the node their map names as writer, at the map's term,
//! and withholds it from any other while it runs. From term 1 on, the writer changes the map
//! only while it holds it ([`Node::writer_may_change`]), checked before the install and again
//! before the push. At term 0 (a map from before D330, or a cluster that never elected) the
//! writer is the smallest id and needs no lease, so a cluster whose older nodes don't grant
//! it can still change its map; it asks for one all the same, so its lapse shows.
//!
//! **An election** ([`Node::maybe_elect`]) runs only while the map has `failover` on (the
//! owner's Q1, the coordinator's rule on the PR 1 plan's C2): once this node's grant of the
//! writer lease has been out for the failover margin, every node it reaches says the same,
//! and each smaller live id has had a margin to go first,
//! 1. it asks every node for the writer lease at a term above every one it has seen (a vote,
//!    [`super::lease::Grants::vote`]), and stops unless a majority grants it;
//! 2. it reads the newest map, by `(writer_term, map_version)`, that the majority holds;
//! 3. it writes that map back at its term, naming itself, at the next version, and pushes
//!    it until a majority holds it.
//!
//! A change the old writer pushed to a majority is in the map step 2 reads; one that reached
//! only a minority wasn't confirmed (D317 rule 3), so nothing served under it, until the
//! writer pushes it again at its term (simp724's N3 on #2056); an elected map replaces it. A node that
//! voted for a term refuses every map and writer lease below it (the term fence), so the old
//! writer's late pushes can't reach a majority once the vote has.

use std::sync::{Arc, PoisonError};
use std::time::Duration;

use super::proto::{ClusterMap, LeasePart, LeaseRequest, LeaseResponse, StatusRequest};
use super::{JOINT_PUSHES, MapSource, Node, WRITER_TERM_FILE, map, schema, write_synced};
use crate::serve::errors::ApiError;

/// The vote's part: the writer lease at `term`.
fn writer_part(term: u64) -> LeasePart {
    LeasePart {
        partition: super::lease::WRITER_LEASE.to_owned(),
        epoch: 0,
        term,
        ..LeasePart::default()
    }
}

impl Node {
    /// The writer lease this node's rounds ask for, at what term: the map's writer at the
    /// map's term; a candidate at the term it was voted, until its map lands.
    pub(crate) fn writer_term_to_ask(&self, map: &ClusterMap) -> Option<u64> {
        let candidate = self.candidate.load(std::sync::atomic::Ordering::Acquire);
        if candidate > map.writer_term {
            return Some(candidate);
        }
        (schema::writer(map) == self.id).then_some(map.writer_term)
    }

    /// Takes the writer lease a round gave this node (D330): a majority of the round's map's
    /// nodes granted it at the term asked, with the map held by a majority.
    pub(crate) fn note_writer_lease(&self, round: &super::fence::Round) {
        let Some(term) = self.writer_term_to_ask(&round.map) else {
            return;
        };
        if !round.majority()
            || round
                .granted
                .get(super::lease::WRITER_LEASE)
                .copied()
                .unwrap_or(0)
                * 2
                <= round.map.nodes.len()
        {
            return;
        }
        let end = super::lease::holder_end(round.t0, self.grants.lease());
        let mut held = self
            .writer_lease
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if (term, end) > *held {
            *held = (term, end);
        }
    }

    /// The time left on this node's writer lease at `map`'s term: zero if it holds none.
    pub(crate) fn writer_lease_remaining(&self, map: &ClusterMap) -> Duration {
        let (term, until) = *self
            .writer_lease
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if term != map.writer_term || schema::writer(map) != self.id {
            return Duration::ZERO;
        }
        Duration::from_nanos(until.saturating_sub(self.grants.clock().now()))
    }

    /// `NodeStatus.writer_lease_remaining_ms`: the writer's own lease, or this node's grant
    /// of it to the writer.
    pub(crate) fn writer_lease_status(&self, map: &ClusterMap) -> Duration {
        let w = schema::writer(map);
        if w == self.id {
            self.writer_lease_remaining(map)
        } else {
            self.grants.writer_remaining(w)
        }
    }

    /// Whether the writer may change `current` now (D330): at term 0 always; from term 1, only
    /// while it holds the writer lease. Checked before an install and again before its push.
    pub(crate) fn writer_may_change(&self, current: &ClusterMap) -> Result<(), ApiError> {
        if current.writer_term == 0 || !self.writer_lease_remaining(current).is_zero() {
            return Ok(());
        }
        Err(ApiError::partition_unavailable(format!(
            "this node is the map's writer at term {}, but holds no writer lease: a majority \
             of the map's nodes must grant it first. Try again; if this node is cut off, \
             another may be elected in its place (D330)",
            current.writer_term
        )))
    }

    /// [`Node::writer_may_change`], waiting up to a lease and a push timeout for the writer
    /// lease, with lease rounds meanwhile: a writer just named by a forced map or an election
    /// gets it once the grants to the old writer run out.
    pub(crate) async fn await_writer_lease(
        self: &Arc<Self>,
        current: &ClusterMap,
    ) -> Result<(), ApiError> {
        let until = tokio::time::Instant::now()
            + super::lease::grace(self.grants.lease())
            + self.peers.push_timeout;
        // Backing off as the renewals do while a lease is missing (simp724's N2 on #2056).
        let mut pause = super::fence::RETRY_FIRST;
        loop {
            match self.writer_may_change(current) {
                Ok(()) => return Ok(()),
                Err(e) if tokio::time::Instant::now() >= until => return Err(e),
                Err(_) => {}
            }
            self.confirm_round().await;
            let left = until.saturating_duration_since(tokio::time::Instant::now());
            tokio::time::sleep(pause.min(left)).await;
            pause = (pause * 2).min(super::fence::RETRY_MAX);
        }
    }

    /// `Cluster.Lease` for an election vote: a request for the writer lease alone, above
    /// every term this node knows. Its term is synced to [`WRITER_TERM_FILE`] before the
    /// answer (the term fence).
    ///
    /// It also takes an elected writer's lone renewal at a grantor still behind its map
    /// (simp724's N1 on #2056): that term is above the grantor's, so the grantor votes for it,
    /// which is what the election asked of a majority already. Harmless, and the grantor's
    /// next round takes the elected map.
    pub(crate) fn is_vote(&self, mine: &ClusterMap, req: &LeaseRequest) -> bool {
        matches!(req.parts.as_slice(), [p] if p.partition == super::lease::WRITER_LEASE
            && p.term > self.grants.term_floor(mine))
    }

    /// Answers vote `req` against `mine` on a blocking thread (the fsync).
    pub(crate) async fn vote_async(
        self: &Arc<Self>,
        mine: ClusterMap,
        req: LeaseRequest,
    ) -> Result<LeaseResponse, ApiError> {
        let node = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            node.grants.vote(&mine, &req, |term| {
                write_synced(&node.dir, WRITER_TERM_FILE, format!("{term}\n").as_bytes())
            })
        })
        .await?
    }

    /// Gives up the writer lease once `map` names another writer (simp724's N2 on the PR 1
    /// plan): a release of it to every node of the map, as for a closed partition, so the
    /// new writer needn't wait out this one's grants. A release that's lost costs that wait,
    /// never safety.
    pub(crate) fn release_writer_if_replaced(&self, map: &ClusterMap) {
        let held = {
            let mut held = self
                .writer_lease
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if held.0 == 0 && held.1 == 0 || schema::writer(map) == self.id {
                return;
            }
            std::mem::take(&mut *held)
        };
        self.release_part_soon(writer_part(held.0));
    }

    /// The election (module doc), after a lease round: only while `failover` is on, this node
    /// isn't the writer, and the writer lease has been out here for the failover margin.
    pub(crate) async fn maybe_elect(self: &Arc<Self>) {
        #[cfg(test)]
        if super::seam(&super::NO_ELECTION, &self.dir) {
            return;
        }
        let Ok(map) = self.get_map() else { return };
        let margin = self.peers.failover_after;
        let ready = map.failover
            && schema::writer(&map) != self.id
            && map.nodes.iter().any(|n| n.id == self.id)
            && self.synced.load(std::sync::atomic::Ordering::Acquire)
            && self.grants.writer_lapsed() >= margin;
        if !ready {
            *self.elect_since() = None;
            return;
        }
        // The survey (simp724's S3 on the design): only saves a vote that can't win. Every
        // node it reaches must report no writer lease, and none a newer map; "live" is one
        // that answered.
        let mut live_smaller = 0u32;
        let mut top_term = self.grants.term_floor(&map);
        for (id, st) in self.survey(&map).await {
            let Some(st) = st else { continue };
            if st.writer_lease_remaining_ms > 0 {
                *self.elect_since() = None;
                return;
            }
            top_term = top_term.max(st.writer_term);
            if id < self.id && id != schema::writer(&map) {
                live_smaller += 1;
            }
        }
        // The survey took time: the election is still due by this node's own state now
        // (simp724's (a)), or it waits for the next tick.
        let still = self.get_map().is_ok_and(|m| !map::newer(&m, &map))
            && self.grants.writer_lapsed() >= margin;
        if !still {
            *self.elect_since() = None;
            return;
        }
        // Each smaller live id gets a margin to go first.
        let now = tokio::time::Instant::now();
        let since = *self.elect_since().get_or_insert(now);
        if now.saturating_duration_since(since) < margin * live_smaller {
            return;
        }
        if let Err(e) = self.elect(&map, top_term + 1).await {
            eprintln!(
                "phdb serve: an election of the map's writer at term {}: {e}",
                top_term + 1
            );
        }
    }

    /// Every other node's `Status`, asked at once under one deadline (simp724's S1 on the D330
    /// PR 2 plan): the push timeout, but at most a sixth of the lease, so one black-holed
    /// node can't hold a survey, or the duties after it, for long. `None` for a node that
    /// didn't answer in time.
    pub(crate) async fn survey(
        self: &Arc<Self>,
        map: &ClusterMap,
    ) -> Vec<(String, Option<super::proto::NodeStatus>)> {
        let deadline = self.peers.push_timeout.min(self.grants.lease() / 6);
        let mut asks = tokio::task::JoinSet::new();
        for n in map.nodes.iter().filter(|n| n.id != self.id) {
            let (node, id, address) = (Arc::clone(self), n.id.clone(), n.address.clone());
            asks.spawn(async move {
                let st = tokio::time::timeout(deadline, async {
                    let mut c = node.client(&address).await?;
                    c.status(node.request(StatusRequest {})?)
                        .await
                        .map(tonic::Response::into_inner)
                        .map_err(|s| s.message().to_owned())
                })
                .await;
                (id, st.ok().and_then(Result::ok))
            });
        }
        asks.join_all().await
    }

    /// Runs the writer's duties after a lease round (an election if one is due, then the
    /// failovers, D330), off the round's own task: a slow peer delays them, never this node's
    /// renewals (simp724's S1 on the PR 2 plan). One run at a time; a tick that finds one
    /// running leaves it.
    pub(crate) fn spawn_duties(self: &Arc<Self>) {
        if self.duties.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return;
        }
        /// Clears `duties` however the task ends, a panic included (simp724's (b)): a flag
        /// left set would stop the duties for good.
        struct Claimed(std::sync::Weak<Node>);
        impl Drop for Claimed {
            fn drop(&mut self) {
                if let Some(node) = self.0.upgrade() {
                    node.duties
                        .store(false, std::sync::atomic::Ordering::Release);
                }
            }
        }
        let claimed = Claimed(Arc::downgrade(self));
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return;
        };
        // The node only while a step runs, as the lease rounds hold it: one dropped (closed)
        // meanwhile ends the duties.
        rt.spawn(async move {
            if let Some(node) = claimed.0.upgrade() {
                node.maybe_adopt_term().await;
            }
            if let Some(node) = claimed.0.upgrade() {
                node.maybe_elect().await;
            }
            if let Some(node) = claimed.0.upgrade() {
                node.maybe_fail_over().await;
            }
            drop(claimed);
        });
    }

    /// #2073 (simp724's design): a node that voted in an election that lost takes no map
    /// below that term (the term fence), so it stays on its old map with its partitions
    /// fenced. The live writer adopts the term: an election at the one above, which its
    /// majority grants (the holder's exception in the pre-vote). It never loosens a vote's
    /// promise, and a term that really won isn't disturbed: the writer goes ahead only while
    /// it holds a writer lease from a round that began after it saw the node behind (were
    /// that term the winner, this lease would have lapsed). One attempt per refused term per
    /// lease, while the node stays behind.
    pub(crate) async fn maybe_adopt_term(self: &Arc<Self>) {
        #[cfg(test)]
        if super::seam(&super::NO_ADOPT, &self.dir) {
            return;
        }
        let Ok(map) = self.get_map() else { return };
        if schema::writer(&map) != self.id || self.writer_may_change(&map).is_err() {
            return;
        }
        let now = self.grants.clock().now();
        let lease = self.grants.lease();
        let due = self
            .stranded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .due(&map, now, lease);
        if due.is_empty() {
            return;
        }
        // Each one's highest known term, from its `Status`: one above the map's is a vote
        // the fence holds it to.
        let mut refused: Option<(u64, u64, String)> = None;
        for (id, st) in self.survey(&map).await {
            let (Some(st), Some(&(_, seen))) = (st, due.iter().find(|(d, _)| *d == id)) else {
                continue;
            };
            if st.writer_term > map.writer_term
                && refused.as_ref().is_none_or(|(n, ..)| st.writer_term > *n)
            {
                refused = Some((st.writer_term, seen, id));
            }
        }
        let Some((n, seen, on)) = refused else { return };
        let (term, end) = *self
            .writer_lease
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if term != map.writer_term || end <= super::lease::holder_end(seen, lease) {
            return;
        }
        let now = self.grants.clock().now();
        if !self
            .stranded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .try_term(n, now, lease)
        {
            return;
        }
        // The survey took time: still this map's writer, with its lease (simp724's (a)).
        let still = self
            .get_map()
            .is_ok_and(|m| (m.writer_term, m.map_version) == (map.writer_term, map.map_version))
            && self.writer_may_change(&map).is_ok();
        if !still {
            return;
        }
        match self.elect(&map, n + 1).await {
            Ok(()) => eprintln!(
                "phdb serve: took writer term {} above term {n}, which node {on} voted for in \
                 an election that lost, so that it takes the map again (#2073)",
                n + 1
            ),
            Err(e) => eprintln!(
                "phdb serve: taking writer term {} above node {on}'s vote for term {n}: {e}",
                n + 1
            ),
        }
    }

    fn elect_since(&self) -> std::sync::MutexGuard<'_, Option<tokio::time::Instant>> {
        self.elect_since
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Steps 1 to 3 at `term` (the module doc). An error says why it stopped.
    async fn elect(self: &Arc<Self>, map: &ClusterMap, term: u64) -> Result<(), String> {
        let t0 = self.grants.clock().now();
        let req = LeaseRequest {
            cluster_id: map.cluster_id.clone(),
            node_id: self.id.clone(),
            map_version: map.map_version,
            round: self.next_round(),
            parts: vec![writer_part(term)],
            lease_ms: self.grants.lease_ms(),
            start: self.grants.start(),
            writer_term: map.writer_term,
            pre_vote: false,
        };
        // 0. The pre-vote (Raft's, D330 PR 2): real votes are asked, and this node's own
        // recorded, only once a majority would vote. A recorded vote fences maps below its
        // term, so a node that can't win, cut off from the rest, would otherwise refuse the
        // live writer's maps for good once it rejoins.
        let pre = LeaseRequest {
            pre_vote: true,
            ..req.clone()
        };
        let own = self.grants.pre_vote(map, &pre, self.peers.failover_after);
        let (would, why) = self.ballots(map, own, &pre).await;
        if would.len() * 2 <= map.nodes.len() {
            return Err(format!(
                "its pre-vote: {} of {} nodes would vote for it ({})",
                would.len(),
                map.nodes.len(),
                why.join("; ")
            ));
        }
        // 1. The vote, this node's own included.
        let own = self.vote_async(map.clone(), req.clone()).await;
        let (granted, why) = self.ballots(map, own, &req).await;
        if granted.len() * 2 <= map.nodes.len() {
            return Err(format!(
                "{} of {} nodes voted for it ({})",
                granted.len(),
                map.nodes.len(),
                why.join("; ")
            ));
        }
        self.candidate
            .store(term, std::sync::atomic::Ordering::Release);
        let end = super::lease::holder_end(t0, self.grants.lease());
        let result = self.take_office(term, end, &granted).await;
        self.candidate
            .store(0, std::sync::atomic::Ordering::Release);
        result
    }

    /// Ballot `req` (a vote or a pre-vote) at every node of `map`: this node's answer `own`,
    /// then each other node's within the push timeout. The nodes that granted it (each id,
    /// and its address but for this node), and why each other one didn't. A build without
    /// leases answers `Unimplemented`, so it neither votes nor pre-votes.
    async fn ballots(
        self: &Arc<Self>,
        map: &ClusterMap,
        own: Result<LeaseResponse, ApiError>,
        req: &LeaseRequest,
    ) -> (Vec<(String, Option<String>)>, Vec<String>) {
        let mut granted = Vec::new();
        let mut why = Vec::new();
        match own {
            Ok(r) if !r.granted.is_empty() => granted.push((self.id.clone(), None)),
            Ok(r) => why.push(format!("{}: {}", self.id, refusal(&r))),
            Err(e) => why.push(format!("{}: {}", self.id, e.message)),
        }
        #[cfg(test)]
        if !req.pre_vote && super::seam(&super::DROP_VOTES, &self.dir) {
            why.push("its vote requests were dropped (a test's seam)".to_owned());
            return (granted, why);
        }
        let mut asks = tokio::task::JoinSet::new();
        for n in map.nodes.iter().filter(|n| n.id != self.id) {
            let (node, id, address, req) = (
                Arc::clone(self),
                n.id.clone(),
                n.address.clone(),
                req.clone(),
            );
            asks.spawn(async move {
                let r =
                    tokio::time::timeout(node.peers.push_timeout, node.ask_lease(&address, req))
                        .await
                        .map_or_else(
                            |_| Err("timed out".to_owned()),
                            |r| r.map_err(|s| s.message().to_owned()),
                        );
                (id, address, r)
            });
        }
        for (id, address, r) in asks.join_all().await {
            match r {
                Ok(r) if !r.granted.is_empty() => granted.push((id, Some(address))),
                Ok(r) => why.push(format!("{id}: {}", refusal(&r))),
                Err(e) => why.push(format!("{id}: {e}")),
            }
        }
        (granted, why)
    }

    /// Steps 2 and 3, as the writer voted at `term`, whose vote holds until `end` on the lease
    /// clock: the majority `granted` voted for it (each id, and its address but for this node).
    async fn take_office(
        self: &Arc<Self>,
        term: u64,
        end: u64,
        granted: &[(String, Option<String>)],
    ) -> Result<(), String> {
        // 2. The newest map the majority holds.
        let mut newest = self.get_map().map_err(|e| e.message)?;
        for (id, address) in granted {
            let Some(address) = address else { continue };
            match tokio::time::timeout(self.peers.push_timeout, self.fetch_map(address)).await {
                Ok(Ok(m)) if map::newer(&m, &newest) => newest = m,
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(format!("node {id}'s map: {e}")),
                Err(_) => return Err(format!("node {id}'s map: timed out")),
            }
        }
        if !newest.nodes.iter().any(|n| n.id == self.id) {
            return Err(format!(
                "the newest map, version {}, no longer lists this node",
                newest.map_version
            ));
        }
        // 3. That map at this term, naming this node, with the election recorded (D330 §6).
        let mut elected = newest.clone();
        elected.writer = self.id.clone();
        elected.writer_term = term;
        elected.map_version = newest.map_version + 1;
        let mut event = super::proto::FailoverEvent {
            from: schema::writer(&newest).to_owned(),
            to: self.id.clone(),
            at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            writer: self.id.clone(),
            writer_term: term,
            ..Default::default()
        };
        event.set_kind(super::proto::FailoverKind::Writer);
        event.set_state(super::proto::FailoverState::Done);
        super::promote::push_event(&mut elected, event);
        if self.grants.clock().now() >= end {
            return Err("the vote's lease ran out before the map was written".to_owned());
        }
        let node = Arc::clone(self);
        let installed = elected.clone();
        tokio::task::spawn_blocking(move || {
            node.install(&installed, MapSource::Peer, |st| match &st.map {
                Some((old, _)) if !map::newer(&installed, old) => {
                    Err(ApiError::failed_precondition(format!(
                        "this node took map version {} at term {} meanwhile",
                        old.map_version, old.writer_term
                    )))
                }
                _ => Ok(None),
            })
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.message)?;
        {
            let mut held = self
                .writer_lease
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            *held = (term, end);
        }
        eprintln!(
            "phdb serve: elected the map's writer at term {term} by {} of {} nodes; map \
             version {} (from version {} at term {})",
            granted.len(),
            elected.nodes.len(),
            elected.map_version,
            newest.map_version,
            newest.writer_term
        );
        // Pushed until a majority holds it, while the lease runs.
        let until = tokio::time::Instant::now() + self.peers.push_timeout * JOINT_PUSHES;
        let mut pause = Duration::from_millis(100);
        loop {
            if self.grants.clock().now() >= end && self.writer_lease_remaining(&elected).is_zero() {
                return Err("the writer lease ran out while the elected map was pushed".to_owned());
            }
            let pushed = self.push_each(&elected, &elected.nodes).await;
            let holders = 1 + pushed.iter().filter(|p| p.ok).count();
            if holders * 2 > elected.nodes.len() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= until {
                return Err(format!(
                    "{holders} of {} nodes took the elected map: the lease rounds go on \
                     pushing it",
                    elected.nodes.len()
                ));
            }
            tokio::time::sleep(pause).await;
            pause = (pause * 2).min(Duration::from_secs(1));
        }
    }
}

/// Why a vote wasn't granted, from its answer.
/// The writer's watch for nodes a lost election stranded (#2073).
#[derive(Debug, Default)]
pub(crate) struct Stranded {
    /// Each suspect, and since when on the lease clock: a node the writer kept answering
    /// with its newer map, or whose push the term fence refused.
    suspects: std::collections::HashMap<String, u64>,
    /// The refused term last tried, and when: one attempt per term per lease (simp724).
    tried: Option<(u64, u64)>,
}

impl Stranded {
    /// The writer answered `node`'s lease request with its newer map at `now`.
    pub(crate) fn behind(&mut self, node: &str, now: u64) {
        self.suspects.entry(node.to_owned()).or_insert(now);
    }

    /// `node` asked at a map no older than the writer's.
    pub(crate) fn caught_up(&mut self, node: &str) {
        self.suspects.remove(node);
    }

    /// The term fence refused the writer's push to `node` at `now`: it's looked at on the
    /// next duty, not after a lease more.
    pub(crate) fn refused(&mut self, node: &str, now: u64, lease: Duration) {
        let since = now.saturating_sub(nanos(lease));
        let at = self.suspects.entry(node.to_owned()).or_insert(since);
        *at = (*at).min(since);
    }

    /// The suspects of `map` behind for a lease or more at `now`, each with since when.
    /// One the map no longer lists is dropped.
    fn due(&mut self, map: &ClusterMap, now: u64, lease: Duration) -> Vec<(String, u64)> {
        self.suspects
            .retain(|id, _| map.nodes.iter().any(|n| n.id == *id));
        self.suspects
            .iter()
            .filter(|(_, since)| now.saturating_sub(**since) >= nanos(lease))
            .map(|(id, since)| (id.clone(), *since))
            .collect()
    }

    /// Whether to try refused term `n` at `now`: not tried within a lease.
    fn try_term(&mut self, n: u64, now: u64, lease: Duration) -> bool {
        if self
            .tried
            .is_some_and(|(t, at)| t == n && now.saturating_sub(at) < nanos(lease))
        {
            return false;
        }
        self.tried = Some((n, now));
        true
    }
}

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

fn refusal(r: &LeaseResponse) -> String {
    if let Some(w) = r.withheld.first() {
        return format!(
            "node {}'s writer lease has {} ms left there",
            w.holder, w.remaining_ms
        );
    }
    if r.grace_remaining_ms > 0 {
        return format!("in its start grace for {} ms more", r.grace_remaining_ms);
    }
    format!("it knows writer term {}", r.writer_term)
}

/// D330 PR 1's seeded model, the writer half (design §8 test 5, with simp724's addition):
/// five nodes with drifting clocks, their real [`super::lease::Grants`], and the map rules
/// above (renewal, change under the lease, election, the term fence), under random cuts. It
/// asserts that at most one node holds the writer lease at any instant, that a
/// `(writer_term, map_version)` never names two maps, and that no writer pushes past its
/// lease's end on its own clock.
#[cfg(test)]
mod model {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use super::super::lease::{Grants, WRITER_LEASE, holder_end};
    use super::super::proto::{ClusterMap, LeasePart, LeaseRequest, Node as MapNode, Partition};
    use super::super::{map, schema};
    use crate::serve::gate::LeaseClock;

    const NODES: usize = 5;
    const LEASE: Duration = Duration::from_secs(3);
    const STEP: Duration = Duration::from_millis(100);

    /// xorshift64*: the model's only randomness.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    struct Sim {
        clock: Arc<LeaseClock>,
        /// Per mille of drift: the clock runs at `1 + drift/1000` (|drift| < 10, under ρ).
        drift: i64,
        grants: Grants,
        map: ClusterMap,
        /// The writer lease as this node holds it: term, end on its own clock.
        held: Option<(u64, u64)>,
        up: bool,
        round: u64,
    }

    fn id(i: usize) -> String {
        format!("n{i}")
    }

    fn first_map() -> ClusterMap {
        ClusterMap {
            cluster_id: "c1".into(),
            map_version: 1,
            failover: true,
            nodes: (0..NODES)
                .map(|i| MapNode {
                    id: id(i),
                    address: format!("127.0.0.1:{}", 1000 + i),
                })
                .collect(),
            partitions: vec![Partition {
                id: "p".into(),
                node: id(0),
                epoch: 1,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn part(term: u64) -> LeasePart {
        LeasePart {
            partition: WRITER_LEASE.into(),
            epoch: 0,
            term,
            ..LeasePart::default()
        }
    }

    struct World {
        nodes: Vec<Sim>,
        rng: Rng,
        /// Every map written, by `(term, version)`: its contents' tag.
        written: HashMap<(u64, u64), u64>,
        tag: u64,
        /// Whether an election can lose its vote after its pre-vote passed (#2073): off in the
        /// settle phase.
        lose_votes: bool,
    }

    impl World {
        fn reach(&self, i: usize, j: usize) -> bool {
            i == j || (self.nodes[i].up && self.nodes[j].up)
        }

        /// Node `j` takes `m` as a push or a pull does: newer by `(term, version)`, and not
        /// below a term it voted for.
        fn offer(&mut self, j: usize, m: &ClusterMap) -> bool {
            let n = &mut self.nodes[j];
            if m.writer_term < n.grants.voted() {
                return false;
            }
            if map::newer(m, &n.map) {
                n.map = m.clone();
                // A writer the map replaced holds nothing (it releases, N2).
                if schema::writer(&n.map) != id(j) {
                    n.held = None;
                }
            }
            n.map.writer_term == m.writer_term && n.map.map_version == m.map_version
        }

        fn record(&mut self, m: &ClusterMap, seed: u64) {
            let tag = m.partitions[0].memory_bytes;
            let had = *self
                .written
                .entry((m.writer_term, m.map_version))
                .or_insert(tag);
            assert_eq!(
                had, tag,
                "seed {seed}: two maps at term {} version {}",
                m.writer_term, m.map_version
            );
        }

        fn holds(&self, i: usize) -> bool {
            let n = &self.nodes[i];
            matches!(n.held, Some((term, end)) if term == n.map.writer_term
                && schema::writer(&n.map) == id(i)
                && n.clock.now() < end)
        }

        /// A writer's lease round: it pushes its map to whoever is behind, then asks.
        fn renew(&mut self, i: usize) {
            let m = self.nodes[i].map.clone();
            if schema::writer(&m) != id(i) {
                return;
            }
            let t0 = self.nodes[i].clock.now();
            self.nodes[i].round += 1;
            let req = LeaseRequest {
                cluster_id: "c1".into(),
                node_id: id(i),
                map_version: m.map_version,
                round: self.nodes[i].round,
                parts: vec![part(m.writer_term)],
                lease_ms: u64::try_from(LEASE.as_millis()).unwrap(),
                start: self.nodes[i].grants.start(),
                writer_term: m.writer_term,
                pre_vote: false,
            };
            let mut granted = 0;
            for j in 0..NODES {
                if !self.reach(i, j) {
                    continue;
                }
                self.offer(j, &m);
                let r = self.nodes[j]
                    .grants
                    .grant(&self.nodes[j].map, &req)
                    .unwrap();
                granted += r.granted.len();
            }
            if granted * 2 > NODES {
                let end = holder_end(t0, LEASE);
                self.nodes[i].held = Some((m.writer_term, end));
            }
        }

        /// A map change by the writer, under its lease from term 1 (checked again before
        /// each push).
        fn change(&mut self, i: usize, seed: u64) {
            let mut m = self.nodes[i].map.clone();
            if schema::writer(&m) != id(i) || (m.writer_term > 0 && !self.holds(i)) {
                return;
            }
            self.tag += 1;
            m.map_version += 1;
            m.partitions[0].memory_bytes = self.tag;
            self.record(&m, seed);
            self.nodes[i].map = m.clone();
            for j in 0..NODES {
                if j == i || !self.reach(i, j) {
                    continue;
                }
                if m.writer_term > 0 {
                    assert!(
                        self.holds(i),
                        "seed {seed}: n{i} pushed past its writer lease"
                    );
                }
                self.offer(j, &m);
            }
        }

        /// An election by `i`, if due (the module doc's steps).
        /// [`Self::ballot`], then the pre-vote's invariant (D330 PR 2, simp724): a node that
        /// can't reach a majority records no vote, its own or another's, so isolation never
        /// raises a term.
        /// And (#2073, simp724): no vote is recorded while another node holds a live writer
        /// lease. With `adopt`, `i` is the writer adopting a term a lost election left.
        fn elect(&mut self, i: usize, margin: Duration, seed: u64, adopt: bool) {
            let reachable = (0..NODES).filter(|&j| self.reach(i, j)).count();
            let other = (0..NODES).find(|&j| j != i && self.holds(j));
            let before: Vec<u64> = self.nodes.iter().map(|n| n.grants.voted()).collect();
            self.ballot(i, margin, seed, adopt);
            let after: Vec<u64> = self.nodes.iter().map(|n| n.grants.voted()).collect();
            if reachable * 2 <= NODES {
                assert_eq!(
                    before, after,
                    "seed {seed}: n{i}, reaching {reachable} of {NODES}, recorded a vote"
                );
            }
            if let Some(j) = other {
                assert_eq!(
                    before, after,
                    "seed {seed}: n{i}'s election recorded a vote while n{j} holds the writer lease"
                );
            }
        }

        /// The election as `elect` runs it: the pre-vote, then the vote, through the grantors'
        /// own [`Grants::pre_vote`] and [`Grants::vote`].
        fn ballot(&mut self, i: usize, margin: Duration, seed: u64, adopt: bool) {
            let mine = self.nodes[i].map.clone();
            if adopt {
                if schema::writer(&mine) != id(i) || !self.holds(i) {
                    return;
                }
            } else if schema::writer(&mine) == id(i)
                || self.nodes[i].grants.writer_lapsed() < margin
            {
                return;
            }
            let mut term = self.nodes[i].grants.term_floor(&mine);
            for j in 0..NODES {
                if self.reach(i, j) {
                    term = term.max(self.nodes[j].grants.term_floor(&self.nodes[j].map));
                }
            }
            // The writer adopts only a term above its map's that a node it reaches holds.
            if adopt && term <= mine.writer_term {
                return;
            }
            term += 1;
            let t0 = self.nodes[i].clock.now();
            self.nodes[i].round += 1;
            let req = LeaseRequest {
                cluster_id: "c1".into(),
                node_id: id(i),
                map_version: mine.map_version,
                round: self.nodes[i].round,
                parts: vec![part(term)],
                lease_ms: u64::try_from(LEASE.as_millis()).unwrap(),
                start: self.nodes[i].grants.start(),
                writer_term: mine.writer_term,
                pre_vote: false,
            };
            let pre = LeaseRequest {
                pre_vote: true,
                ..req.clone()
            };
            let would = (0..NODES)
                .filter(|&j| self.reach(i, j))
                .filter(|&j| {
                    let n = &self.nodes[j];
                    !n.grants
                        .pre_vote(&n.map, &pre, margin)
                        .unwrap()
                        .granted
                        .is_empty()
                })
                .count();
            if would * 2 <= NODES {
                return;
            }
            // A vote lost between the rounds (#2073): the candidate's own is recorded, and
            // no other node's.
            if !adopt && self.lose_votes && self.rng.below(6) == 0 {
                let n = &self.nodes[i];
                n.grants.vote(&n.map, &req, |_| Ok(())).unwrap();
                return;
            }
            let mut voters = Vec::new();
            for j in 0..NODES {
                if !self.reach(i, j) {
                    continue;
                }
                let n = &self.nodes[j];
                let r = n.grants.vote(&n.map, &req, |_| Ok(())).unwrap();
                if !r.granted.is_empty() {
                    voters.push(j);
                }
            }
            if voters.len() * 2 <= NODES {
                return;
            }
            let mut newest = mine;
            for &j in &voters {
                if map::newer(&self.nodes[j].map, &newest) {
                    newest = self.nodes[j].map.clone();
                }
            }
            let mut elected = newest;
            elected.writer = id(i);
            elected.writer_term = term;
            elected.map_version += 1;
            self.record(&elected, seed);
            if !self.offer(i, &elected) {
                return;
            }
            self.nodes[i].held = Some((term, holder_end(t0, LEASE)));
            for j in 0..NODES {
                if j != i && self.reach(i, j) {
                    assert!(
                        self.holds(i),
                        "seed {seed}: n{i} pushed its elected map late"
                    );
                    self.offer(j, &elected);
                }
            }
        }
    }

    fn run(seed: u64) {
        let mut rng = Rng(seed | 1);
        let nodes = (0..NODES)
            .map(|_| {
                let clock = Arc::new(LeaseClock::default());
                let drift = i64::try_from(rng.below(19)).unwrap() - 9;
                Sim {
                    grants: Grants::new(Arc::clone(&clock), LEASE),
                    clock,
                    drift,
                    map: first_map(),
                    held: None,
                    up: true,
                    round: 0,
                }
            })
            .collect();
        let mut w = World {
            nodes,
            rng,
            written: HashMap::new(),
            tag: 0,
            lose_votes: true,
        };
        let margin = LEASE * 2;
        for _ in 0..3_000 {
            for n in &w.nodes {
                let ns = i64::try_from(STEP.as_nanos()).unwrap();
                n.clock.advance(Duration::from_nanos(
                    u64::try_from(ns + ns * n.drift / 1000).unwrap(),
                ));
            }
            // A node is cut off or joins again now and then.
            if w.rng.below(40) == 0 {
                let i = usize::try_from(w.rng.below(NODES as u64)).unwrap();
                w.nodes[i].up = !w.nodes[i].up;
            }
            for _ in 0..3 {
                let i = usize::try_from(w.rng.below(NODES as u64)).unwrap();
                match w.rng.below(5) {
                    0 | 1 => w.renew(i),
                    2 => w.change(i, seed),
                    3 => w.elect(i, margin, seed, false),
                    _ => w.elect(i, margin, seed, true),
                }
            }
            let holders: Vec<usize> = (0..NODES).filter(|&i| w.holds(i)).collect();
            assert!(
                holders.len() <= 1,
                "seed {seed}: writer leases held at once by {holders:?}"
            );
        }
        // Elections happened at all (the model isn't vacuous).
        let top = w.nodes.iter().map(|n| n.map.writer_term).max().unwrap();
        assert!(top > 0, "seed {seed}: no election in the run");
        // Settle (#2073): every node up, no vote lost. Every node then holds the writer's map,
        // and none is left with a vote above it: a lost election strands no one.
        w.lose_votes = false;
        for n in &mut w.nodes {
            n.up = true;
        }
        for _ in 0..1_500 {
            for n in &w.nodes {
                let ns = i64::try_from(STEP.as_nanos()).unwrap();
                n.clock.advance(Duration::from_nanos(
                    u64::try_from(ns + ns * n.drift / 1000).unwrap(),
                ));
            }
            for _ in 0..3 {
                let i = usize::try_from(w.rng.below(NODES as u64)).unwrap();
                match w.rng.below(4) {
                    0 | 1 => w.renew(i),
                    2 => w.elect(i, margin, seed, false),
                    _ => w.elect(i, margin, seed, true),
                }
            }
            let holders: Vec<usize> = (0..NODES).filter(|&i| w.holds(i)).collect();
            assert!(
                holders.len() <= 1,
                "seed {seed}: writer leases held at once by {holders:?} (settling)"
            );
        }
        let maps: Vec<(u64, u64)> = w
            .nodes
            .iter()
            .map(|n| (n.map.writer_term, n.map.map_version))
            .collect();
        assert!(
            maps.iter().all(|m| *m == maps[0]),
            "seed {seed}: settled nodes hold different maps: {maps:?}"
        );
        for (i, n) in w.nodes.iter().enumerate() {
            assert!(
                n.grants.voted() <= n.map.writer_term,
                "seed {seed}: n{i} is left with a vote for term {} above its map's {}",
                n.grants.voted(),
                n.map.writer_term
            );
        }
    }

    #[test]
    fn writer_leases_never_overlap_and_a_term_and_version_name_one_map() {
        for seed in [1, 2, 3, 0x5eed, 0xd330, 0xfeed_beef, 42, 7_777] {
            run(seed);
        }
    }
}
