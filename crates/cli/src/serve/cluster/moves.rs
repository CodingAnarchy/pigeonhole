//! A node's side of D331's range moves (PR 2: the gates, no data moving yet): arming each
//! partition's range gate as it opens, and, on each map install, deriving every open
//! partition's gate again and taking the source's hand-over.
//!
//! **The source's hand-over (S1, as simp724 and cli692 agreed it).** The map that takes a
//! move to HANDED_OVER raises its source S's epoch by one, on the same node, keeping S's
//! history. The node holding S, taking that map:
//! 1. its install has already ended S's lease (`lapse_unnamed`: the map no longer names this
//!    node for S at its file's epoch), so S serves nothing from the install on, and every
//!    D329 read hold taken under the old lease's term is lost (simp724's S2). The lapse is
//!    repeated here, explicitly, so it doesn't rest on that rule;
//! 2. records the moved-away range in S's file ([`super::super::ranges::Moved`]), so a
//!    restart rebuilds the gate from it before serving;
//! 3. writes the new epoch into S's identity row, a local write in its own partition (until
//!    D331 only `Stamp` raised an epoch, on a closed file);
//! 4. serves S at the new epoch, and asks for its lease at once (`confirm_soon`) rather than
//!    at the next renewal: S's whole partition pauses for that one round, which the guide
//!    states beside the range's pause.

use std::sync::Arc;

use pigeonhole::Pigeonhole;

use super::Node;
use super::identity;
use super::map;
use crate::serve::gate::WriteGate;
use crate::serve::proto::{ClusterMap, MovePhase};
use crate::serve::ranges::{self, Moved};

impl Node {
    /// Arms partition `id`'s range gate as it opens here, before it serves: its file's
    /// moved-away rows, then the node's map. **Blocking** (a read of the file).
    pub(crate) fn arm_ranges(
        &self,
        gate: &WriteGate,
        id: &str,
        db: &Pigeonhole,
    ) -> Result<(), String> {
        gate.ranges().load_moved(ranges::read_moved(db)?);
        if let Ok(map) = self.get_map() {
            gate.ranges().update(&map, id, db.visible_seqno());
        }
        Ok(())
    }

    /// After an install publishes `map`: each partition served here derives its range gate
    /// again, and a source whose move the map takes to HANDED_OVER takes its hand-over (the
    /// module's doc). **Blocking** (writes to the source's file), on the install's thread.
    pub(super) fn apply_moves(&self, map: &ClusterMap) {
        let Some(shared) = self.server.get().and_then(|o| o.shared.upgrade()) else {
            return;
        };
        let Some(cluster) = shared.router.cluster.as_ref() else {
            return;
        };
        let mut raised = false;
        for (id, service) in cluster.parts.load().iter() {
            let (Some((gate, epoch)), Ok(part)) = (self.fences.gate(id), service.part()) else {
                continue;
            };
            let handing = map.moves.iter().find(|m| {
                m.from == *id
                    && map::phase(m) >= MovePhase::HandedOver
                    && !gate.ranges().has_moved(&m.id)
            });
            if let Some(m) = handing {
                match self.hand_over(&gate, id, epoch, &part.db, map, m) {
                    Ok(up) => raised |= up,
                    Err(e) => eprintln!(
                        "phdb serve: partition {id}'s hand-over of move {:?} didn't record: \
                         {e}; it gates the range from the map meanwhile, and records it at \
                         the next map or restart",
                        m.id
                    ),
                }
            }
            gate.ranges().update(map, id, part.db.visible_seqno());
        }
        if raised {
            self.confirm_soon();
        }
    }

    /// Takes move `m`'s hand-over on its source `id`, served here at `epoch` behind `gate`
    /// (steps 1–4 of the module's doc). Whether it raised the epoch.
    fn hand_over(
        &self,
        gate: &Arc<WriteGate>,
        id: &str,
        epoch: u64,
        db: &Pigeonhole,
        map: &ClusterMap,
        m: &crate::serve::proto::RangeMove,
    ) -> Result<bool, String> {
        let (start, end) = map::move_range(m)?;
        let to = map
            .partitions
            .iter()
            .find(|p| p.id == *id && p.node == self.id)
            .map(|p| p.epoch);
        // 1. No lease at the old epoch's term from here on.
        gate.lapse();
        // 2. The moved-away row, at S's seqno now: every transaction open now is open across
        // the hand-over (M3).
        let row = Moved {
            id: m.id.clone(),
            table: m.table.clone(),
            start,
            end,
            term: m.handed_over_term,
            version: m.handed_over_version,
            seqno: db.visible_seqno(),
        };
        ranges::write_moved(db, &row)?;
        gate.ranges().add_moved(row);
        // 3–4. The new epoch, when the map gives it on this node (a later map may have moved
        // S since, and its own open stamps it).
        let Some(to) = to.filter(|e| *e > epoch) else {
            return Ok(false);
        };
        let mut ident = identity::read(db)?
            .ok_or_else(|| format!("partition {id}'s file has no identity row"))?;
        ident.epoch = to;
        identity::write(db, &ident)?;
        self.fences.raise_epoch(id, to);
        eprintln!(
            "phdb serve: partition {id} handed table {:?}'s range over (move {:?}): its \
             epoch is {to} now, and it asks for its lease at it",
            m.table, m.id
        );
        Ok(true)
    }
}
