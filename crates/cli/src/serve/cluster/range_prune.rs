//! D331 PR 5: pruning a range move's rows once nothing needs them, each node its own
//! partitions', on a slow timer (design "Pruning", feed1248's P1–P3, simp724's watermark).
//!
//! - **A source's moved-away row** goes once the map no longer has its move; every node of
//!   the map answers with a map at or past the hand-over's version (none routes the range
//!   here by an older one); the transaction lifetime plus the request timeout have passed
//!   since the partition recorded it (M3's conflicts need it no longer); and the
//!   partition's feed retention has passed it (P3: the resume rule's own test, a resume
//!   from the token taken at the hand-over expires).
//! - **A target's serving row** goes once its node's map no longer has the move (P1: the
//!   gate serves the range from CLEANING only while the row exists); no older moved-away row
//!   here overlaps it (it clears one, a move back); and the partition's feed retention has
//!   passed its `t_from` (a resume from its token expires).
//! - **Each prune is one logged write** that deletes the row and raises its table's
//!   watermark ([`ranges::prune_row`]), under the partition's write gate, so only an owner
//!   holding its lease prunes, and replicas carry both. A cluster component starting below
//!   the watermark with no live row is `FeedExpired` on every copy (P2), whatever that
//!   copy's own retention.
//! - **The table on the source:** once the last moved-away row of a table moved off whole
//!   goes, the schema reconcile drops the table at the next map install (it keeps a table
//!   only while the map places it here, a move names it, or a moved-away row does).

use std::sync::Arc;
use std::time::Duration;

use pigeonhole::{ErrorCode, FeedToken, Pigeonhole};

use super::Node;
use crate::serve::proto::ClusterMap;
use crate::serve::ranges::{self, Moved, Pruned};

/// How often a node prunes, by default (`Peers::prune_interval`; shorter in tests).
pub(crate) const INTERVAL: Duration = Duration::from_secs(60);

/// What the prune of one partition's rows depends on, gathered by the pass.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Bounds {
    /// Now, in milliseconds since the Unix epoch on this node's clock.
    pub now_ms: u64,
    /// The transaction lifetime plus the request timeout.
    pub open_for: Duration,
    /// The lowest map version every node of the map answered with (`None`: one didn't
    /// answer, so no moved-away row goes).
    pub every_node_at: Option<u64>,
}

/// The rows of a partition to prune now, by the module's rules: pure, but for
/// `passed(row)`, whether the partition's feed retention has passed the row.
pub(crate) fn prunable(
    map: &ClusterMap,
    moved: &[Moved],
    serving: &[Moved],
    bounds: Bounds,
    mut passed: impl FnMut(&Moved) -> bool,
) -> Vec<(Pruned, Moved)> {
    let live = |m: &Moved| map.moves.iter().any(|r| r.id == m.id);
    let mut out = Vec::new();
    for m in moved {
        let settled = bounds.every_node_at.is_some_and(|v| v >= m.version);
        let old = bounds.now_ms
            >= m.at_ms
                .saturating_add(u64::try_from(bounds.open_for.as_millis()).unwrap_or(u64::MAX));
        if !live(m) && settled && old && passed(m) {
            out.push((Pruned::Moved, m.clone()));
        }
    }
    for m in serving {
        let clears = moved
            .iter()
            .any(|o| o.table == m.table && o.version < m.version && overlaps(o, m));
        if !live(m) && !clears && passed(m) {
            out.push((Pruned::Serving, m.clone()));
        }
    }
    out
}

/// Whether two rows' ranges of one table overlap.
fn overlaps(a: &Moved, b: &Moved) -> bool {
    let before = |end: &Option<Vec<u8>>, start: &[u8]| end.as_deref().is_some_and(|e| e <= start);
    !before(&a.end, &b.start) && !before(&b.end, &a.start)
}

/// Whether `db`'s feed retention has passed `m`'s token: a resume from it expires (P3). No
/// token (the feed was off at the hand-over) or no table: nothing is retained for it.
fn retention_passed(db: &Pigeonhole, m: &Moved) -> bool {
    if m.token.is_empty() {
        return true;
    }
    let t = match db.table(&m.table).and_then(|t| t.open()) {
        Ok(t) => t,
        Err(e) => return e.code() == ErrorCode::TableNotFound,
    };
    match t
        .feed()
        .resume(&FeedToken::from_bytes(m.token.clone()))
        .open()
    {
        Ok(_) => false,
        Err(e) => matches!(
            e.code(),
            ErrorCode::FeedExpired | ErrorCode::SnapshotExpired
        ),
    }
}

impl Node {
    /// The node's pruning timer, for as long as it runs: a pass every
    /// `Peers::prune_interval`, one at a time. One loop per node; it holds the node only
    /// while a pass runs.
    pub(crate) fn watch_range_prunes(self: &Arc<Self>) {
        if self.pruning.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return;
        }
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            self.pruning
                .store(false, std::sync::atomic::Ordering::Release);
            return;
        };
        let me = Arc::downgrade(self);
        let every = self.peers.prune_interval;
        rt.spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(node) = me.upgrade() else { return };
                node.prune_ranges().await;
            }
        });
    }

    /// One pass: each partition served here prunes the rows the rules allow. Errors are
    /// logged; the next pass tries again.
    pub(crate) async fn prune_ranges(self: &Arc<Self>) {
        let Ok(map) = self.get_map() else { return };
        let shared = self.server.get().and_then(|s| s.shared.upgrade());
        let Some(parts) = shared
            .as_ref()
            .and_then(|sh| sh.router.cluster.as_ref().map(|c| c.parts.load_full()))
        else {
            return;
        };
        let mut every_node_at: Option<Option<u64>> = None;
        for (id, service) in parts.iter() {
            let (Some(gate), Ok(part)) = (service.gate(), service.part()) else {
                continue;
            };
            let db = part.db.clone();
            let (moved, serving) = match (ranges::read_moved(&db), ranges::read_serving(&db)) {
                (Ok(m), Ok(s)) => (m, s),
                (Err(e), _) | (_, Err(e)) => {
                    eprintln!("phdb serve: partition {id}: the range rows' prune: {e}");
                    continue;
                }
            };
            if moved.is_empty() && serving.is_empty() {
                continue;
            }
            // Asked once a pass, only when a moved-away row could go.
            if !moved.is_empty() && every_node_at.is_none() {
                every_node_at = Some(self.every_node_at(&map).await);
            }
            let bounds = Bounds {
                now_ms: now_ms(),
                open_for: service.sessions.txn_lifetime() + service.request_timeout(),
                every_node_at: every_node_at.flatten(),
            };
            let map2 = map.clone();
            let check = db.clone();
            let doomed = tokio::task::spawn_blocking(move || {
                prunable(&map2, &moved, &serving, bounds, |m| {
                    retention_passed(&check, m)
                })
            })
            .await
            .unwrap_or_default();
            if doomed.is_empty() {
                continue;
            }
            // Under the gate: an owner holding its lease, not fenced (feed1248's "only under
            // the gate's lease").
            let Ok(held) = gate.enter() else { continue };
            let write = db.clone();
            let pruned = tokio::task::spawn_blocking(move || {
                let mut done = Vec::new();
                for (kind, m) in &doomed {
                    match ranges::prune_row(&write, *kind, m) {
                        Ok(()) => done.push(m.id.clone()),
                        Err(e) => eprintln!("phdb serve: {e}"),
                    }
                }
                done
            })
            .await
            .unwrap_or_default();
            let leased = held.committed();
            drop(held);
            if pruned.is_empty() {
                continue;
            }
            if let Err(e) = leased {
                eprintln!(
                    "phdb serve: partition {id}: the range rows' prune: {}",
                    e.message
                );
            }
            eprintln!(
                "phdb serve: partition {id}: pruned the range rows of move{} {}",
                if pruned.len() > 1 { "s" } else { "" },
                pruned.join(", ")
            );
            // The gate takes the file's rows again, then its state from them.
            match (ranges::read_moved(&db), ranges::read_serving(&db)) {
                (Ok(m), Ok(s)) => {
                    let visible = db.visible_seqno();
                    gate.ranges().load_rows_at(m, s, visible);
                    gate.ranges().update(&map, id, visible);
                }
                (Err(e), _) | (_, Err(e)) => {
                    eprintln!("phdb serve: partition {id}: reload the range rows: {e}");
                }
            }
        }
    }

    /// The lowest map version this node and every other of `map` hold (`None` if one
    /// doesn't answer).
    async fn every_node_at(self: &Arc<Self>, map: &ClusterMap) -> Option<u64> {
        let mut lowest = map.map_version;
        for (_, st) in self.survey_with(map, true).await {
            lowest = lowest.min(st?.map_version);
        }
        Some(lowest)
    }
}

/// Now, in milliseconds since the Unix epoch, on this node's clock.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::proto::RangeMove;

    fn row(id: &str, start: &str, end: Option<&str>, version: u64, at_ms: u64) -> Moved {
        Moved {
            id: id.into(),
            table: "users".into(),
            start: start.as_bytes().to_vec(),
            end: end.map(|e| e.as_bytes().to_vec()),
            version,
            at_ms,
            final_seqno: 50,
            seqno: 40,
            ..Default::default()
        }
    }

    /// Each rule on its own holds a row back; with all met it goes (feed1248's P1, P3; the
    /// design's bounds).
    #[test]
    fn a_row_is_pruned_only_once_every_rule_allows_it() {
        let open_for = Duration::from_secs(10);
        let ok = Bounds {
            now_ms: 100_000,
            open_for,
            every_node_at: Some(9),
        };
        let m = row("mv1", "d", Some("m"), 7, 50_000);
        let none = ClusterMap::default();
        let ids =
            |v: Vec<(Pruned, Moved)>| v.into_iter().map(|(k, m)| (k, m.id)).collect::<Vec<_>>();
        let all = |_: &Moved| true;
        assert_eq!(
            ids(prunable(&none, std::slice::from_ref(&m), &[], ok, all)),
            [(Pruned::Moved, "mv1".to_owned())]
        );
        // The move still in the map.
        let live = ClusterMap {
            moves: vec![RangeMove {
                id: "mv1".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(prunable(&live, std::slice::from_ref(&m), &[], ok, all).is_empty());
        // A node below the hand-over's version, or one that didn't answer.
        for every in [Some(6), None] {
            let b = Bounds {
                every_node_at: every,
                ..ok
            };
            assert!(prunable(&none, std::slice::from_ref(&m), &[], b, all).is_empty());
        }
        // Within the transaction lifetime plus the request timeout.
        let early = Bounds {
            now_ms: 59_999,
            ..ok
        };
        assert!(prunable(&none, std::slice::from_ref(&m), &[], early, all).is_empty());
        // Retention not past it.
        assert!(prunable(&none, std::slice::from_ref(&m), &[], ok, |_| false).is_empty());

        // A serving row: P1 (its move in the map), retention, and an older moved-away row
        // it overlaps, which it clears.
        let s = row("mv2", "d", Some("m"), 9, 0);
        assert_eq!(
            ids(prunable(&none, &[], std::slice::from_ref(&s), ok, all)),
            [(Pruned::Serving, "mv2".to_owned())]
        );
        let live2 = ClusterMap {
            moves: vec![RangeMove {
                id: "mv2".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(prunable(&live2, &[], std::slice::from_ref(&s), ok, all).is_empty());
        assert!(prunable(&none, &[], std::slice::from_ref(&s), ok, |_| false).is_empty());
        let older = row("mv0", "k", None, 3, 99_999);
        assert!(
            ids(prunable(
                &none,
                std::slice::from_ref(&older),
                std::slice::from_ref(&s),
                ok,
                all
            ))
            .iter()
            .all(|(k, _)| *k != Pruned::Serving),
            "an older overlapping moved-away row keeps it"
        );
        let apart = row("mv0", "m", None, 3, 99_999);
        assert!(
            ids(prunable(&none, &[apart], &[s], ok, all))
                .contains(&(Pruned::Serving, "mv2".to_owned())),
            "[m, end) doesn't overlap [d, m)"
        );
    }
}
