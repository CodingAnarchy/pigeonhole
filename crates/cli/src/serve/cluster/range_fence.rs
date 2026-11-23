//! D331 PR 5: a range move's fence on its source, and the seqno it reports
//! (`MoveStatus.fence_seqno`), which the move's driver records as its `final_seqno`.
//!
//! Once its gate fences the range (HANDING_OVER), the source reports its visible seqno only
//! after (feed1248's F1, simp724's M1):
//! - every write admitted before the fence has finished ([`WriteGate::drain_range_fence`]:
//!   only those, so a busy partition drains), and
//! - no D329 share still in doubt here may touch the range ([`RangeGate::in_doubt_on`]:
//!   simp724's rule, conservative for any share it can't place).
//!
//! Every write to the range the source took is then at or below it. Keyed by move id (F3),
//! kept while the move is in the map, and reported again unchanged (F2's rerun keeps the
//! driver's). In memory: a restart fences again and reports again, and the driver keeps the
//! first it recorded.
//!
//! [`RangeGate::in_doubt_on`]: crate::serve::ranges::RangeGate::in_doubt_on

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use pigeonhole::Pigeonhole;

use super::Node;
use super::map;
use crate::serve::gate::WriteGate;
use crate::serve::proto::{ClusterMap, MovePhase};

/// How often a fence waiting on a share in doubt looks again.
const IN_DOUBT_POLL: Duration = Duration::from_millis(50);

/// The fences this node's partitions hold as a move's source, and the seqnos they report.
#[derive(Debug, Default)]
pub(crate) struct Fences {
    /// Move id → its fence's seqno, once reported.
    reported: Mutex<HashMap<String, u64>>,
    /// Moves whose fence's task runs.
    running: Mutex<HashSet<String>>,
}

impl Fences {
    /// Move `id`'s fence seqno here, 0 until reported.
    pub(crate) fn seqno(&self, id: &str) -> u64 {
        self.reported().get(id).copied().unwrap_or(0)
    }

    fn reported(&self) -> std::sync::MutexGuard<'_, HashMap<String, u64>> {
        self.reported.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn running(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
        self.running.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// After `map` settled on partition `partition` here (its gate `gate`, file `db`):
    /// starts the fence's task of each move from it at HANDING_OVER that its gate fences
    /// and that has neither reported nor a task running; forgets the moves the map no longer
    /// has at HANDING_OVER from a partition here. `request_timeout` bounds one drain's wait.
    pub(crate) fn sync(
        self: &Arc<Self>,
        node: &Weak<Node>,
        map: &ClusterMap,
        partition: &str,
        gate: &Arc<WriteGate>,
        db: &Pigeonhole,
        request_timeout: Duration,
    ) {
        let handing: Vec<String> = map
            .moves
            .iter()
            .filter(|m| {
                !map::node_move(m) && m.from == partition && map::phase(m) == MovePhase::HandingOver
            })
            .map(|m| m.id.clone())
            .collect();
        for id in handing {
            let fenced = gate.ranges().load().as_ref().is_some_and(|s| s.fences(&id));
            if !fenced || self.reported().contains_key(&id) || !self.running().insert(id.clone()) {
                continue;
            }
            let Ok(rt) = tokio::runtime::Handle::try_current() else {
                self.running().remove(&id);
                continue;
            };
            let task = Task {
                fences: Arc::clone(self),
                node: node.clone(),
                id,
                partition: partition.to_owned(),
                gate: Arc::clone(gate),
                db: db.clone(),
                request_timeout,
            };
            rt.spawn(task.run());
        }
    }

    /// Forgets every move `map` no longer has at HANDING_OVER or past it: an abort, or a
    /// move done.
    pub(crate) fn forget_gone(&self, map: &ClusterMap) {
        let live = |id: &str| {
            map.moves.iter().any(|m| {
                m.id == id && {
                    let p = map::phase(m);
                    p == MovePhase::HandingOver || map::committed(p)
                }
            })
        };
        self.reported().retain(|id, _| live(id));
    }
}

/// One move's fence task.
struct Task {
    fences: Arc<Fences>,
    node: Weak<Node>,
    id: String,
    partition: String,
    gate: Arc<WriteGate>,
    db: Pigeonhole,
    request_timeout: Duration,
}

impl Task {
    /// Whether the move is still at HANDING_OVER from this partition in the node's map, and
    /// the partition still open here behind the same gate, fencing it.
    fn live(&self) -> bool {
        let Some(node) = self.node.upgrade() else {
            return false;
        };
        let Ok(map) = node.get_map() else {
            return false;
        };
        let handing = map.moves.iter().any(|m| {
            m.id == self.id && m.from == self.partition && map::phase(m) == MovePhase::HandingOver
        });
        handing
            && node
                .fences
                .gate(&self.partition)
                .is_some_and(|(g, _)| Arc::ptr_eq(&g, &self.gate))
            && self
                .gate
                .ranges()
                .load()
                .as_ref()
                .is_some_and(|s| s.fences(&self.id))
    }

    async fn run(self) {
        let seqno = self.fence().await;
        if let Some(seqno) = seqno {
            self.fences.reported().insert(self.id.clone(), seqno);
        }
        self.fences.running().remove(&self.id);
    }

    /// The fence's seqno, or `None` once the move or the partition is gone from here.
    async fn fence(&self) -> Option<u64> {
        // 1. The writes admitted before the fence (F1). Waited for again past a timeout: the
        //    driver bounds the whole fence, and aborts past its bound (simp724).
        loop {
            if !self.live() {
                return None;
            }
            if self
                .gate
                .drain_range_fence(self.request_timeout)
                .await
                .is_ok()
            {
                break;
            }
        }
        // 2. No share in doubt here that may touch the range (M1, simp724's rule).
        loop {
            if !self.live() {
                return None;
            }
            let in_doubt: Vec<Vec<u8>> = self.db.in_doubt().into_iter().map(|(x, _)| x).collect();
            if self.gate.ranges().in_doubt_on(&self.id, &in_doubt) == 0 {
                break;
            }
            tokio::time::sleep(IN_DOUBT_POLL).await;
        }
        // 3. Every write to the range taken here is at or below it.
        Some(self.db.visible_seqno())
    }
}
