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
    map.moves.iter().find(|m| {
        !map::node_move(m) && m.from == partition && map::phase(m) >= MovePhase::HandedOver
    })
}

/// `db`'s D310 feed token now for a feed of `table` (D331 PR 3b's `t_from`), `None` when
/// its change feed is off.
fn hand_over_token(db: &Pigeonhole, table: &str) -> Result<Option<Vec<u8>>, String> {
    let t = db
        .table(table)
        .and_then(|b| b.open())
        .map_err(|e| format!("open table {table:?}: {e}"))?;
    match t.feed().open() {
        Ok(feed) => Ok(Some(feed.token().as_bytes().to_vec())),
        Err(e) if e.code() == pigeonhole::ErrorCode::InvalidArgument => Ok(None),
        Err(e) => Err(format!("a feed of table {table:?}: {e}")),
    }
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
            // PR 3b: a cluster component of a table moved away whole reads up to here.
            final_seqno: m.final_seqno,
            ..Default::default()
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

    /// The range copies this node serves as a source (D331 PR 4b), once it serves.
    pub(super) fn range_copies(&self) -> Option<std::sync::Arc<super::range_copy::Sources>> {
        let shared = self.server.get().and_then(|o| o.shared.upgrade())?;
        shared
            .router
            .cluster
            .as_ref()
            .map(|c| std::sync::Arc::clone(&c.range_copies))
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
        // D331 PR 4b: copies this node no longer serves as their source let go.
        cluster.range_copies.release_gone(map, &self.id);
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
            !map::node_move(m)
                && m.from == id
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
            !map::node_move(m)
                && m.to == id
                && map::phase(m) >= MovePhase::Cleaning
                && !gate.ranges().has_serving(&m.id)
        }) {
            let (start, end) = map::move_range(m)?;
            // `t_from` as a D310 token (PR 3b): a feed of the table opened now, before the
            // gate opens the range, so every event of it here past that token is new.
            // Without the change feed, none: no cluster subscription reads this partition.
            let token = hand_over_token(db, &m.table)?;
            let seqno = token
                .as_deref()
                .and_then(super::token::token_seqno)
                .unwrap_or_else(|| db.visible_seqno());
            let row = Moved {
                id: m.id.clone(),
                table: m.table.clone(),
                start,
                end,
                term: m.handed_over_term,
                version: m.handed_over_version,
                seqno,
                at_ms: now_ms(),
                source: m.from.clone(),
                final_seqno: m.final_seqno,
                token: token.unwrap_or_default(),
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

/// What a restored file's clip did ([`clip_restored`]): rows deleted, D329 shares still in
/// doubt (the clip runs again once they're decided), and the newest timestamp in the file.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Clipped {
    pub deleted: u64,
    pub in_doubt: usize,
    pub newest_ts: u64,
}

/// Rows a clip deletes per batch.
const CLIP_BATCH: usize = 256;

/// Clips restored partition `partition`'s file `db` to `map` (D331 PR 3, design §6): a
/// cluster backup has no common point, so a partition's copy may hold rows of a range a
/// move gave another (the restore rolls each in-flight move back or forward by its phase,
/// and its map's placements say who holds what; a backup never spans a hand-over, since
/// `backup::moved` refuses one). For each table the map places:
/// - first, the timestamp floor rises to the newest timestamp of **any** row (simp724's S1:
///   a move's copied rows carry the source's timestamps, M1, and a delete written below a
///   cell's would leave that cell visible);
/// - then rows outside this partition's ranges, and outside any range an in-flight move
///   names it the target of (simp724's N3: the move's copy), are deleted feed-silently
///   (they're neither new history here nor anyone's to deliver);
/// - moved-away rows covering a range the map gives it back are cut to the rest (the
///   restore asserts the map), and every one left drops its hand-over pair to (0, 0): a
///   backup's pairs are another cluster's numbering, so any later move back must count as
///   newer (cli692).
///
/// The marker goes only once no D329 share is in doubt here (simp724's S2): a blind delete
/// isn't refused by one, and a share decided commit later would bring its rows back, so the
/// clip runs again at the next open after it's decided. Run before the partition serves;
/// rerun from the start if it stops (cli692's F5): each step finds done what it did.
/// **Blocking.**
pub(super) fn clip_restored(
    db: &Pigeonhole,
    map: &ClusterMap,
    partition: &str,
) -> Result<Clipped, String> {
    use std::ops::Bound;
    let mut out = Clipped::default();
    let mut held: std::collections::HashMap<&str, Vec<ranges::KeyRange>> =
        std::collections::HashMap::new();
    let mut tables = Vec::new();
    for t in &map.tables {
        let mut mine: Vec<ranges::KeyRange> = map::table_ranges(t)
            .into_iter()
            .filter(|(_, _, p)| *p == partition)
            .map(|(a, b, _)| (a, b))
            .collect();
        held.insert(t.name.as_str(), mine.clone());
        // An in-flight move's copy into this partition stays (simp724's N3).
        for m in map
            .moves
            .iter()
            .filter(|m| !map::node_move(m) && m.to == partition && m.table == t.name)
        {
            mine.push(map::move_range(m)?);
        }
        match db.table(&t.name).and_then(|b| b.open()) {
            Ok(table) => tables.push((t.name.as_str(), table, mine)),
            Err(e) if e.code() == pigeonhole::ErrorCode::TableNotFound => {}
            Err(e) => return Err(format!("open table {:?}: {e}", t.name)),
        }
    }
    // 1. The floor, over every row (S1).
    for (name, table, _) in &tables {
        for row in table
            .scan_bounds(Bound::Unbounded, Bound::Unbounded)
            .iter()
            .map_err(|e| format!("scan table {name:?}: {e}"))?
        {
            let row = row.map_err(|e| format!("scan table {name:?}: {e}"))?;
            for c in row.view().iter() {
                out.newest_ts = out.newest_ts.max(c.cell.timestamp());
            }
        }
    }
    if out.newest_ts > 0 {
        db.raise_ts_floor(out.newest_ts)
            .map_err(|e| format!("raise the timestamp floor: {e}"))?;
    }
    // 2. The deletes.
    for (name, table, mine) in &tables {
        let inside = |k: &[u8]| {
            mine.iter()
                .any(|(a, b)| a.as_slice() <= k && b.as_deref().is_none_or(|e| k < e))
        };
        let mut doomed: Vec<Vec<u8>> = Vec::with_capacity(CLIP_BATCH);
        // The scan reads at its own snapshot, so the deletes behind it don't disturb it.
        for row in table
            .scan_bounds(Bound::Unbounded, Bound::Unbounded)
            .iter()
            .map_err(|e| format!("scan table {name:?}: {e}"))?
        {
            let row = row.map_err(|e| format!("scan table {name:?}: {e}"))?;
            if !inside(row.key()) {
                doomed.push(row.key().to_vec());
                if doomed.len() == CLIP_BATCH {
                    out.deleted += delete_silently(db, table, &doomed)
                        .map_err(|e| format!("clip table {name:?}: {e}"))?;
                    doomed.clear();
                }
            }
        }
        out.deleted +=
            delete_silently(db, table, &doomed).map_err(|e| format!("clip table {name:?}: {e}"))?;
    }
    for r in ranges::read_moved(db)? {
        let Some(mine) = held.get(r.table.as_str()) else {
            continue;
        };
        let mut left = vec![(r.start.clone(), r.end.clone())];
        for h in mine {
            left = left
                .into_iter()
                .flat_map(|piece| ranges::subtract(&piece, h))
                .collect();
        }
        // What's left keeps no hand-over pair (cli692): a backup's pairs are another
        // cluster's numbering, or older than anything after the restore, so every serving row
        // the restored cluster writes (a move back) must count as newer.
        let whole = left.len() == 1 && left[0] == (r.start.clone(), r.end.clone());
        if whole && (r.term, r.version) == (0, 0) {
            continue;
        }
        if whole {
            ranges::write_moved(
                db,
                &Moved {
                    term: 0,
                    version: 0,
                    ..r.clone()
                },
            )?;
            continue;
        }
        let pieces: Vec<Moved> = left
            .into_iter()
            .enumerate()
            .map(|(i, (start, end))| Moved {
                id: format!("{}~{i}", r.id),
                start,
                end,
                term: 0,
                version: 0,
                ..r.clone()
            })
            .collect();
        ranges::rewrite_moved(db, &r, &pieces)?;
    }
    // A share in doubt keeps the marker: decided commit, its writes would come back.
    out.in_doubt = db.in_doubt().len();
    if out.in_doubt == 0 {
        ranges::clear_clip(db)?;
    }
    Ok(out)
}

/// Deletes rows `keys` of `table` feed-silently, in one batch: how many. A `Busy` (a write
/// stall) is retried a few times, then fails the clip, which runs again at the next open.
fn delete_silently(
    db: &Pigeonhole,
    table: &pigeonhole::Table,
    keys: &[Vec<u8>],
) -> Result<u64, pigeonhole::Error> {
    if keys.is_empty() {
        return Ok(0);
    }
    let mut tries = 0;
    loop {
        let mut batch = db.write_batch();
        batch.mark_feed_silent();
        for k in keys {
            batch.delete_row(table, k);
        }
        match batch.commit() {
            Ok(_) => return Ok(keys.len() as u64),
            Err(e) if e.code() == pigeonhole::ErrorCode::Busy && tries < 5 => {
                tries += 1;
                std::thread::sleep(std::time::Duration::from_millis(50 << tries));
            }
            Err(e) => return Err(e),
        }
    }
}
