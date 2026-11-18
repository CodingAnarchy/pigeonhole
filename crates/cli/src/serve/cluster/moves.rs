//! A node's side of D331's range moves (PR 2: the gates, no data moving yet): bringing each
//! partition it serves up to the map at every install and at open ([`Node::settle`]),
//! which derives its range gate and finishes its hand-over as a source.
//!
//! **The source's hand-over (S1, as simp724 and cli692 agreed it).** The map that takes a
//! move to HANDED_OVER raises its source S's epoch by one, on the same node, keeping S's
//! history. The node holding S, taking that map:
//! 1. ends S's lease (its install's `lapse_unnamed` already has: the map no longer names
//!    this node for S at its file's epoch), so S serves nothing until the new lease, and
//!    every D329 read hold under the old lease's term is lost (simp724's S2). The lapse is
//!    repeated here, explicitly, so it doesn't rest on that rule;
//! 2. records the moved-away range in S's file ([`Moved`]), so an open gates it from there;
//! 3. writes the new epoch into S's identity row, a local write in its own partition (until
//!    D331 only `Stamp` raised an epoch, on a closed file);
//! 4. derives S's range gate from the map, then serves S at the new epoch and asks for its
//!    lease at once (`confirm_soon`), in that order, so a renewal can't lease S before its
//!    gate says the range is gone. S's whole partition pauses for that one round, which
//!    the guide states beside the range's pause.
//!
//! **Idempotent, keyed on epochs (simp724's B3).** Each step checks what's already done: the
//! row by its move id, the identity row and the served epoch against the map's. So a step
//! that failed is finished by the next install, and a crash between the map's persist and
//! the stamp is finished by the open ([`finish_hand_over`], which `open_partition` runs on a
//! file one epoch below the map's).

use pigeonhole::Pigeonhole;

use super::Node;
use super::identity;
use super::map;
use crate::serve::gate::WriteGate;
use crate::serve::proto::{ClusterMap, MovePhase, RangeMove};
use crate::serve::ranges::{self, Moved};

/// The move `map` hands `partition` over in as its source, from HANDED_OVER on, if any.
pub(super) fn handing_over<'m>(map: &'m ClusterMap, partition: &str) -> Option<&'m RangeMove> {
    map.moves
        .iter()
        .find(|m| m.from == partition && map::phase(m) >= MovePhase::HandedOver)
}

/// Now, in milliseconds since the Unix epoch, on this node's clock (a row's time).
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Fault seam (tests): the node directory whose next hand-over stamp fails once.
#[cfg(test)]
pub(super) static FAIL_STAMP: std::sync::Mutex<Option<std::path::PathBuf>> =
    std::sync::Mutex::new(None);

/// Steps 2 and 3 on partition `partition`'s file `db`, for move `m` of `map`, as far as
/// they aren't done: the row when the file lacks it, and the map's epoch on this node,
/// `epoch`, when the file is below it. The row it wrote, if any. Run by a node holding the
/// partition open, and by `open_partition` on a file the map is one epoch past. `dir` is
/// the node's directory (the fault seam's key).
pub(super) fn finish_hand_over(
    db: &Pigeonhole,
    m: &RangeMove,
    epoch: u64,
    dir: &std::path::Path,
) -> Result<Option<Moved>, String> {
    #[cfg(not(test))]
    let _ = dir;
    let mut wrote = None;
    if !ranges::read_moved(db)?.iter().any(|r| r.id == m.id) {
        let (start, end) = map::move_range(m)?;
        let row = Moved {
            id: m.id.clone(),
            table: m.table.clone(),
            start,
            end,
            term: m.handed_over_term,
            version: m.handed_over_version,
            seqno: db.visible_seqno(),
            at_ms: now_ms(),
        };
        ranges::write_moved(db, &row)?;
        wrote = Some(row);
    }
    let mut ident = identity::read(db)?.ok_or("the file has no identity row")?;
    if ident.epoch < epoch {
        #[cfg(test)]
        if FAIL_STAMP
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take_if(|d| d == dir)
            .is_some()
        {
            return Err("the fault seam failed the stamp".into());
        }
        ident.epoch = epoch;
        identity::write(db, &ident)?;
    }
    Ok(wrote)
}

impl Node {
    /// Arms partition `id`'s range gate as it opens here (its file's moved-away rows), then
    /// settles it with the node's map, under the install lock, so an install either ran
    /// before (and this sees its map) or runs after `join` (and sees the partition).
    /// `join` publishes the partition (its service into the node's parts). **Blocking.**
    pub(crate) fn arm_ranges(
        &self,
        gate: &WriteGate,
        id: &str,
        db: &Pigeonhole,
        join: impl FnOnce(),
    ) -> Result<(), String> {
        let _one = self
            .installing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        gate.ranges()
            .load_rows(ranges::read_moved(db)?, ranges::read_serving(db)?);
        let raised = match self.get_map() {
            Ok(map) => self.settle(gate, id, db, &map)?,
            Err(_) => false,
        };
        join();
        drop(_one);
        if raised {
            self.ask_after_raise();
        }
        Ok(())
    }

    /// After an install publishes `map` (under the install lock): every partition served
    /// here settles to it. **Blocking**, on the install's thread.
    pub(super) fn apply_moves(&self, map: &ClusterMap) {
        let Some(shared) = self.server.get().and_then(|o| o.shared.upgrade()) else {
            return;
        };
        let Some(cluster) = shared.router.cluster.as_ref() else {
            return;
        };
        let mut raised = false;
        for (id, service) in cluster.parts.load().iter() {
            let (Some((gate, _)), Ok(part)) = (self.fences.gate(id), service.part()) else {
                continue;
            };
            match self.settle(&gate, id, &part.db, map) {
                Ok(up) => raised |= up,
                Err(e) => eprintln!(
                    "phdb serve: partition {id} couldn't take map version {}'s range moves: \
                     {e}; it gates them from the map meanwhile, and the next map or its next \
                     open finishes them",
                    map.map_version
                ),
            }
        }
        // D331 PR 3: each replica copy here derives its gate again too, from its file's
        // rows (its follower brings them in) and the new map.
        for (id, copy) in cluster.replicas.load().iter() {
            let Ok(part) = copy.service.part() else {
                continue;
            };
            if let Err(e) = copy.follower.arm_ranges(&part.db) {
                eprintln!(
                    "phdb serve: the replica copy of {id} couldn't take map version {}'s \
                     range moves: {e}; it keeps its last gate",
                    map.map_version
                );
            }
        }
        if raised {
            self.ask_after_raise();
        }
    }

    /// A partition's epoch rose here (a hand-over): its renewals run, if they don't yet, and
    /// it asks for its lease now, as a partition that opens does (`confirm_opened`), not at
    /// the next renewal.
    fn ask_after_raise(&self) {
        if let Some(node) = self.me.upgrade() {
            node.renew_leases();
        }
        self.confirm_soon();
    }

    /// Brings partition `id`, served here behind `gate`, up to `map`: puts up a new fence's
    /// marker, finishes a hand-over of a move from it (the module's doc), and derives its
    /// range gate. Whether its served epoch rose. Each step is idempotent.
    fn settle(
        &self,
        gate: &WriteGate,
        id: &str,
        db: &Pigeonhole,
        map: &ClusterMap,
    ) -> Result<bool, String> {
        let served = self.fences.gate(id).map_or(0, |(_, e)| e);
        let mine = map
            .partitions
            .iter()
            .find(|p| p.id == id && p.node == self.id)
            .map_or(0, |p| p.epoch);
        // A new fence: its marker moves the visible seqno past the fence's (B2).
        let fencing = map.moves.iter().find(|m| {
            m.from == id
                && map::phase(m) == MovePhase::HandingOver
                && !gate
                    .ranges()
                    .load()
                    .as_ref()
                    .is_some_and(|s| s.fences(&m.id))
        });
        let mut visible = db.visible_seqno();
        if let Some(m) = fencing {
            visible = ranges::mark_fence(db, &m.id)?;
        }
        // A target taking a move's CLEANING map records that it serves the range, with its
        // visible seqno just before (`t_from`), before its gate opens it (simp724's C2): so
        // nothing of the range lands here at or below `t_from` but the move's feed-silent
        // copy, and a later map that gives a range back to its old owner is told apart
        // from one that returns it with no copy (cli692's point on the PR 2 review).
        for m in map.moves.iter().filter(|m| {
            m.to == id && map::phase(m) >= MovePhase::Cleaning && !gate.ranges().has_serving(&m.id)
        }) {
            let (start, end) = map::move_range(m)?;
            let row = Moved {
                id: m.id.clone(),
                table: m.table.clone(),
                start,
                end,
                term: m.handed_over_term,
                version: m.handed_over_version,
                seqno: db.visible_seqno(),
                at_ms: now_ms(),
            };
            ranges::write_serving(db, &row)?;
            gate.ranges().add_serving(row);
        }
        let Some(m) = handing_over(map, id).filter(|m| {
            // The map's epoch for it here is past the one it's served at, or its file lacks
            // the row: not yet (fully) taken.
            mine > served || !gate.ranges().has_moved(&m.id)
        }) else {
            gate.ranges().update(map, id, visible);
            return Ok(false);
        };
        let raise = mine > served;
        // 1.
        if raise {
            gate.lapse();
        }
        // 2–3. The gate follows the map whether or not they finish: a failed step leaves
        // the range gated as the map says, the epoch unraised, and the next map or open to
        // finish them.
        let finished = finish_hand_over(db, m, mine, &self.dir).and_then(|row| match row {
            Some(row) => {
                gate.ranges().add_moved(row);
                Ok(())
            }
            None if !gate.ranges().has_moved(&m.id) => {
                gate.ranges()
                    .load_rows(ranges::read_moved(db)?, ranges::read_serving(db)?);
                Ok(())
            }
            None => Ok(()),
        });
        // 4.
        gate.ranges().update(map, id, db.visible_seqno());
        finished?;
        if raise {
            self.fences.raise_epoch(id, mine);
            eprintln!(
                "phdb serve: partition {id} handed table {:?}'s range over (move {:?}): its \
                 epoch is {mine} now, and it asks for its lease at it",
                m.table, m.id
            );
        }
        Ok(raise)
    }
}
