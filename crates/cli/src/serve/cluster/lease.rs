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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use super::proto::{ClusterMap, LeasePart, LeaseRequest, LeaseResponse, LeaseWithheld};
use crate::serve::gate::LeaseClock;

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

fn millis(ns: u64) -> u64 {
    ns / 1_000_000
}

/// One node's grant here.
#[derive(Debug)]
struct Grant {
    /// The holder's round it was made for.
    round: u64,
    /// The partitions granted, with their epochs.
    parts: BTreeMap<String, u64>,
    /// When it ends, on this node's clock.
    until: u64,
}

/// A node's grants: the leases it promised, by holder (itself included).
#[derive(Debug)]
pub(crate) struct Grants {
    clock: Arc<LeaseClock>,
    /// L.
    lease: Duration,
    /// When this node started, on its clock: it grants nothing until the start grace ends.
    started: u64,
    by_holder: Mutex<BTreeMap<String, Grant>>,
}

impl Grants {
    /// A starting node's grants: none, and the start grace from now.
    pub(crate) fn new(clock: Arc<LeaseClock>, lease: Duration) -> Self {
        let started = clock.now();
        Self {
            clock,
            lease,
            started,
            by_holder: Mutex::new(BTreeMap::new()),
        }
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

    /// Answers `req`, from node `req.node_id`, against this node's map `mine` (D326's
    /// grantor rules, in the module's doc). The cluster id was checked by the caller.
    ///
    /// No grace exemption, even for a map that names this node alone (audit120's S4): a
    /// forced map that dropped the other nodes clears `previous_nodes`, so nothing tells a
    /// node that was always alone from one whose grant to a peer may still run.
    pub(crate) fn grant(&self, mine: &ClusterMap, req: &LeaseRequest) -> LeaseResponse {
        let mut out = LeaseResponse::default();
        if mine.map_version > req.map_version {
            out.newer_map_version = mine.map_version;
            return out;
        }
        // Behind the asking node: it pushes its map in its round before it asks, so this is
        // a race with that push. Nothing is granted; its next round asks again.
        if mine.map_version < req.map_version {
            return out;
        }
        let grace = self.grace_remaining();
        if !grace.is_zero() {
            out.grace_remaining_ms = u64::try_from(grace.as_millis()).unwrap_or(u64::MAX).max(1);
            return out;
        }
        let now = self.clock.now();
        let mut by = self
            .by_holder
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        by.retain(|_, g| g.until > now);
        // An older round of the same holder, arriving after a newer one: ignored, so it
        // can't put back what the newer one no longer asked for.
        if by.get(&req.node_id).is_some_and(|g| g.round > req.round) {
            return out;
        }
        let mut parts = BTreeMap::new();
        for LeasePart { partition, epoch } in &req.parts {
            let given = mine
                .partitions
                .iter()
                .any(|p| p.id == *partition && p.node == req.node_id && p.epoch == *epoch);
            if !given {
                continue;
            }
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
            parts.insert(partition.clone(), *epoch);
            out.granted.push(LeasePart {
                partition: partition.clone(),
                epoch: *epoch,
            });
        }
        // The new grant replaces the holder's last one: what it no longer asks for, or that
        // the map no longer gives it, is no longer granted.
        by.insert(
            req.node_id.clone(),
            Grant {
                round: req.round,
                parts,
                until: now.saturating_add(nanos(self.lease)),
            },
        );
        out
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

    fn ask(node: &str, version: u64, round: u64, parts: &[(&str, u64)]) -> LeaseRequest {
        LeaseRequest {
            cluster_id: "c1".into(),
            node_id: node.into(),
            map_version: version,
            round,
            parts: parts
                .iter()
                .map(|(p, e)| LeasePart {
                    partition: (*p).into(),
                    epoch: *e,
                })
                .collect(),
        }
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
        let r = g.grant(&v1, &ask("a", 1, 1, &[("p1", 1)]));
        assert_eq!(granted(&r), ["p1"]);
        // v2 relocates p1 to c at epoch 2 without a (cut off): c asks while a's grant runs.
        let v2 = map(2, &[("p1", "c", 2), ("p2", "b", 1)]);
        clock.advance(Duration::from_secs(3));
        let r = g.grant(&v2, &ask("c", 2, 1, &[("p1", 2)]));
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
            granted(&g.grant(&v2, &ask("c", 2, 2, &[("p1", 2)]))),
            ["p1"]
        );
        // And a, asking at v1 again, is told of v2 and granted nothing.
        let r = g.grant(&v2, &ask("a", 1, 2, &[("p1", 1)]));
        assert_eq!(r.newer_map_version, 2);
        assert!(r.granted.is_empty());
    }

    #[test]
    fn a_grant_is_only_what_the_map_gives_the_asker_at_the_versions_asked() {
        let (g, _clock) = grants(Duration::from_secs(10));
        let v3 = map(3, &[("p1", "a", 2), ("p2", "b", 1)]);
        // Another node's partition, or another epoch: neither granted nor withheld.
        let r = g.grant(&v3, &ask("a", 3, 1, &[("p1", 1), ("p2", 1)]));
        assert!(r.granted.is_empty() && r.withheld.is_empty(), "{r:?}");
        assert_eq!(
            granted(&g.grant(&v3, &ask("a", 3, 2, &[("p1", 2)]))),
            ["p1"]
        );
        // A grantor behind the asker grants nothing (the asker's push hasn't landed).
        let r = g.grant(&v3, &ask("a", 4, 3, &[("p1", 2)]));
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
            granted(&g.grant(&v1, &ask("a", 1, 5, &[("p1", 1), ("p2", 1)]))).len(),
            2
        );
        assert_eq!(
            granted(&g.grant(&v1, &ask("a", 1, 6, &[("p1", 1)]))),
            ["p1"]
        );
        assert!(granted(&g.grant(&v1, &ask("a", 1, 5, &[("p2", 1)]))).is_empty());
        let v2 = map(2, &[("p1", "a", 1), ("p2", "b", 2)]);
        assert_eq!(
            granted(&g.grant(&v2, &ask("b", 2, 1, &[("p2", 2)]))),
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
        let r = g.grant(&alone, &ask("a", 1, 1, &[("p1", 1)]));
        assert!(r.granted.is_empty());
        assert!((10_000..=10_100).contains(&r.grace_remaining_ms), "{r:?}");
        clock.advance(Duration::from_millis(10_100));
        assert!(g.grace_remaining().is_zero());
        assert_eq!(
            granted(&g.grant(&alone, &ask("a", 1, 2, &[("p1", 1)]))),
            ["p1"]
        );
    }
}
