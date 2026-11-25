//! D331 PR 5: a range move's drops, each from a drop-pending row `drop/<id>` in the moved
//! family that a restart runs again until it's done (4a's design, cli692's condition 5).
//!
//! - **The source's, at CLEANING:** its copy of the moved range goes once the target serves
//!   it. Done, its `MoveStatus.dropped` says so, and the driver removes the record.
//! - **The target's reconcile (simp724's M2):** a copy here that no live move owns (its
//!   record ABORTING, or gone from the map while the range isn't this partition's) is
//!   dropped: its follower stopped and joined first, so its last commit is in, then the
//!   range, then its progress rows. Run on every map and at open, so a target that was down
//!   when the abort landed cleans up when it comes back. A copy whose move finished (the map
//!   gives the range here) only loses its progress rows.
//!
//! The rows are written in the settle (under the install lock, before a partition that
//! opens serves); the drops run on the runtime. A new move's follower waits while a drop
//! row overlapping its range is here ([`pending_overlap`], cli692's Must 4).

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use pigeonhole::{Family, Pigeonhole};

use super::identity;
use super::map;
use super::range_follow::Followers;
use crate::serve::proto::{ClusterMap, MovePhase, RangeMove};

/// The moved family, where the drop rows sit beside the moved-away, serving and progress
/// rows, each kind under its own prefix.
const FAMILY: &str = "moved";
const DROP: &[u8] = b"drop/";

/// How long a drop the engine refuses (`Busy`: a share in doubt here) waits to try again.
const RETRY: Duration = Duration::from_millis(500);

/// One drop-pending row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Pending {
    pub id: String,
    pub table: String,
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
    /// The target's (an abort's): its follower is joined, and its progress rows go too.
    pub target: bool,
}

/// The drops this node runs, and those done, by move id.
#[derive(Debug, Default)]
pub(crate) struct Drops {
    done: Mutex<HashSet<String>>,
    running: Mutex<HashSet<String>>,
}

impl Drops {
    /// Whether move `id`'s drop here is done (`MoveStatus.dropped`).
    pub(crate) fn dropped(&self, id: &str) -> bool {
        self.done().contains(id)
    }

    fn done(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
        self.done.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn running(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
        self.running.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Partition `partition`'s file `db` under `map`: writes the drop rows `map` calls for
    /// (the source's at CLEANING, the target's reconcile) and prunes the progress rows of
    /// moves that finished here. Before the node's followers take the map, so a new move's
    /// follower sees an overlapping drop's row; [`start`](Self::start) after, so a stopped
    /// follower is joined. **Blocking.**
    pub(crate) fn write_rows(
        &self,
        map: &ClusterMap,
        partition: &str,
        db: &Pigeonhole,
    ) -> Result<(), String> {
        // The source's, at CLEANING.
        for m in map.moves.iter().filter(|m| {
            !map::node_move(m)
                && m.from == partition
                && map::target_serves(map::phase(m))
                && !self.dropped(&m.id)
        }) {
            let (start, end) = map::move_range(m)?;
            write_pending(
                db,
                &Pending {
                    id: m.id.clone(),
                    table: m.table.clone(),
                    start,
                    end,
                    target: false,
                },
            )?;
        }
        // The target's, at ABORTING, whatever it copied so far: none yet is a drop with
        // nothing to drop, which still says `dropped` (a split's record waits for it). The
        // drop joins the follower first, so a first chunk it commits after this goes too.
        for m in map.moves.iter().filter(|m| {
            !map::node_move(m)
                && m.to == partition
                && map::phase(m) == MovePhase::Aborting
                && !self.dropped(&m.id)
        }) {
            let (start, end) = map::move_range(m)?;
            write_pending(
                db,
                &Pending {
                    id: m.id.clone(),
                    table: m.table.clone(),
                    start,
                    end,
                    target: true,
                },
            )?;
        }
        // The target's reconcile.
        for (id, range) in super::range_follow::copies(db).map_err(|e| e.message)? {
            let record = map.moves.iter().find(|m| m.id == id);
            let owned = record.is_some_and(|m| {
                m.to == partition && {
                    let p = map::phase(m);
                    // A phase this build doesn't know (a newer writer's) counts as owned:
                    // never drop a range the target may own (cli692's nit on #2130).
                    p == MovePhase::Unspecified || map::follows(p) || map::target_serves(p)
                }
            });
            if owned {
                continue;
            }
            let range = match (record, range) {
                (Some(m), _) => {
                    let (start, end) = map::move_range(m)?;
                    Some((m.table.clone(), start, end))
                }
                (None, r) => r,
            };
            let Some((table, start, end)) = range else {
                eprintln!(
                    "phdb serve: partition {partition} holds range move {id:?}'s copy, which \
                     the map no longer has, and its progress doesn't name its range (it began \
                     before D331 PR 5): drop the range by hand"
                );
                continue;
            };
            // A finished move: the map gives the range here, and only its rows go.
            let finished = record.is_none()
                && map
                    .tables
                    .iter()
                    .any(|t| t.name == table && map::inside(t, &start, end.as_deref(), partition));
            if finished {
                super::range_follow::prune(db, &id).map_err(|e| e.message)?;
                continue;
            }
            write_pending(
                db,
                &Pending {
                    id,
                    table,
                    start,
                    end,
                    target: true,
                },
            )?;
        }
        // Forget the drops of moves the map dropped.
        self.done()
            .retain(|id| map.moves.iter().any(|m| &m.id == id));
        Ok(())
    }

    /// Starts a drop of every pending row in `db` not yet running, on the runtime if there
    /// is one; `follows`, the node's followers, which a target's drop joins first.
    /// **Blocking** (its read).
    pub(crate) fn start(
        self: &Arc<Self>,
        db: &Pigeonhole,
        follows: Option<&Arc<Followers>>,
    ) -> Result<(), String> {
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return Ok(());
        };
        for p in read_pending(db)? {
            if !self.running().insert(p.id.clone()) {
                continue;
            }
            let (drops, db, follows) = (Arc::clone(self), db.clone(), follows.cloned());
            rt.spawn(async move {
                let id = p.id.clone();
                #[cfg(test)]
                hold(&id).await;
                match drops.run(&db, p, follows).await {
                    Ok(()) => {
                        drops.done().insert(id.clone());
                    }
                    Err(e) => eprintln!(
                        "phdb serve: range move {id:?}'s drop stopped: {e}; the next map or \
                         open runs it again"
                    ),
                }
                drops.running().remove(&id);
            });
        }
        Ok(())
    }

    /// Runs drop `p` to its end: the target's follower joined first, then the range, then
    /// (the target's) its progress rows, then the drop row.
    async fn run(
        &self,
        db: &Pigeonhole,
        p: Pending,
        follows: Option<Arc<Followers>>,
    ) -> Result<(), String> {
        if p.target
            && let Some(f) = &follows
        {
            f.join(&p.id).await;
        }
        let table = match db.table(&p.table).and_then(|b| b.open()) {
            Ok(t) => Some(t),
            // The copy never created it: nothing to drop.
            Err(e) if e.code() == pigeonhole::ErrorCode::TableNotFound => None,
            Err(e) => return Err(format!("open table {:?}: {e}", p.table)),
        };
        while let Some(table) = &table {
            let (db2, table2, p2) = (db.clone(), table.clone(), p.clone());
            let dropped = tokio::task::spawn_blocking(move || {
                db2.drop_range(&table2, &p2.start, p2.end.as_deref())
            })
            .await
            .map_err(|e| format!("the drop's task: {e}"))?;
            match dropped {
                Ok(()) => break,
                Err(e) if e.code() == pigeonhole::ErrorCode::Busy => {
                    tokio::time::sleep(RETRY).await;
                }
                Err(e) => return Err(format!("drop the range: {e}")),
            }
        }
        let (db2, id) = (db.clone(), p.id.clone());
        let target = p.target;
        tokio::task::spawn_blocking(move || -> Result<(), String> {
            if target {
                super::range_follow::prune(&db2, &id).map_err(|e| e.message)?;
            }
            clear_pending(&db2, &id)
        })
        .await
        .map_err(|e| format!("the drop's task: {e}"))?
    }
}

/// `__phdb_cluster` with the moved family.
fn moved_table(db: &Pigeonhole) -> Result<pigeonhole::Table, String> {
    db.table(identity::TABLE)
        .and_then(|b| b.family(FAMILY, Family::default()).create_if_missing())
        .map_err(|e| format!("open the moved-away ranges: {e}"))
}

fn pending_row(id: &str) -> Vec<u8> {
    [DROP, id.as_bytes()].concat()
}

/// Writes drop row `p`, unless it's there.
fn write_pending(db: &Pigeonhole, p: &Pending) -> Result<(), String> {
    let t = moved_table(db)?;
    let row = pending_row(&p.id);
    if t.get(&row, FAMILY, b"table")
        .map_err(|e| format!("read a drop row: {e}"))?
        .is_some()
    {
        return Ok(());
    }
    let mut w = t
        .mutate(&row)
        .put(FAMILY, b"table", p.table.as_bytes())
        .put(FAMILY, b"start", &p.start)
        .put(FAMILY, b"target", if p.target { b"1" } else { b"0" });
    if let Some(end) = &p.end {
        w = w.put(FAMILY, b"end", end);
    }
    w.commit()
        .map(drop)
        .map_err(|e| format!("write range move {:?}'s drop row: {e}", p.id))
}

fn clear_pending(db: &Pigeonhole, id: &str) -> Result<(), String> {
    moved_table(db)?
        .mutate(&pending_row(id))
        .delete_row()
        .commit()
        .map(drop)
        .map_err(|e| format!("clear range move {id:?}'s drop row: {e}"))
}

/// The drop rows in `db`.
pub(crate) fn read_pending(db: &Pigeonhole) -> Result<Vec<Pending>, String> {
    let t = db
        .table(identity::TABLE)
        .and_then(|b| b.open())
        .map_err(|e| format!("open the moved-away ranges: {e}"))?;
    if !t.families().contains(&FAMILY) {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for row in t
        .scan_prefix(DROP)
        .family(FAMILY)
        .iter()
        .map_err(|e| format!("read the drop rows: {e}"))?
    {
        let row = row.map_err(|e| format!("read the drop rows: {e}"))?;
        let cell = |q: &[u8]| row.get(FAMILY, q).map(|c| c.value().to_vec());
        let (Some(table), Some(start)) = (cell(b"table"), cell(b"start")) else {
            continue;
        };
        out.push(Pending {
            id: String::from_utf8_lossy(&row.key()[DROP.len()..]).into_owned(),
            table: String::from_utf8_lossy(&table).into_owned(),
            start,
            end: cell(b"end"),
            target: cell(b"target").as_deref() == Some(b"1"),
        });
    }
    Ok(out)
}

/// Whether a drop row of move `id` itself is here: the drop of an aborted record's copy
/// under the same id, which a map brought back (#2141). Real records never reuse an id
/// (`<prefix>-<ms>`, taken at their start), so only a hand-built map does. The follower waits
/// for it before it reads its progress rows, which that drop deletes with the range.
pub(crate) fn pending_own(db: &Pigeonhole, id: &str) -> Result<bool, String> {
    Ok(read_pending(db)?.iter().any(|p| p.id == id))
}

/// A drop row here overlapping move `m`'s range, other than its own: its follower waits
/// (cli692's Must 4). The id of the move it's from.
pub(crate) fn pending_overlap(db: &Pigeonhole, m: &RangeMove) -> Result<Option<String>, String> {
    let (start, end) = map::move_range(m)?;
    let overlaps = |p: &Pending| {
        p.table == m.table
            && p.id != m.id
            && end.as_deref().is_none_or(|e| p.start.as_slice() < e)
            && p.end.as_deref().is_none_or(|e| start.as_slice() < e)
    };
    Ok(read_pending(db)?.into_iter().find(overlaps).map(|p| p.id))
}

/// A hold seam (tests): the drop of the move named first, once, waits before it starts
/// until the test releases it, after telling the test it got there, so a test can close
/// the partition under a drop that hasn't run (main's Windows red on 0285e076f).
#[cfg(test)]
pub(crate) static HOLD: Mutex<Vec<Held>> = Mutex::new(Vec::new());

/// One [`HOLD`]: the move id, then the reached and go signals.
#[cfg(test)]
pub(crate) type Held = (
    String,
    Arc<tokio::sync::Notify>,
    tokio::sync::oneshot::Receiver<()>,
);

#[cfg(test)]
async fn hold(id: &str) {
    let held = {
        let mut holds = HOLD.lock().unwrap_or_else(PoisonError::into_inner);
        holds
            .iter()
            .position(|(m, _, _)| m == id)
            .map(|i| holds.swap_remove(i))
    };
    if let Some((_, reached, go)) = held {
        reached.notify_one();
        let _ = go.await;
    }
}
