//! A lease grantor's durable record of each `replica_acks` partition's fallback (D335 PR
//! 2b, ICR 0146, §7's ASYNC and simp724's M1).
//!
//! An owner falling back to answering writes without their replica acknowledgements first
//! asks its lease round to record where it began (`LeasePart.degraded_from`). Each grantor
//! records it in [`DEGRADED_FILE`] per partition and epoch, synced before its answer echoes
//! it, and keeps it past the grant's expiry and across restarts, until a later round at the
//! same epoch clears it (`degraded_clear`) or a newer epoch or history replaces it. The
//! writer's survey reads it (`NodeStatus.degraded`) when it fails the partition over (PR 3).
//!
//! A grantor records CLEAR when it first grants a `replica_acks` partition at an epoch, so a
//! partition absent here is only ever unknown, never clear by default (simp724's Q2).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use serde::{Deserialize, Serialize};

use super::proto::{ClusterMap, DegradedRecord, DegradedState, LeasePart, Position};
use crate::serve::acks::Pos;

/// The records, in the node's directory: JSON, written whole and synced.
pub(crate) const DEGRADED_FILE: &str = "cluster-degraded.json";

/// What a grantor knows of one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Record {
    /// The epoch the record is of.
    epoch: u64,
    /// The partition's `history_epoch` then: a record of another history is another
    /// partition's, by the same id.
    history: u64,
    /// FLAGGED from this position (seqno, n), or CLEAR.
    from: Option<(u64, u32)>,
}

impl Record {
    fn from_pos(&self) -> Option<Pos> {
        self.from.map(|(seqno, n)| Pos { seqno, n })
    }
}

/// What a lease round asked of a partition's record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ask {
    /// Nothing: a renewal. A first grant at an epoch still records CLEAR.
    Nothing,
    Flag(Pos),
    Clear,
}

impl Ask {
    fn of(part: &LeasePart) -> Self {
        if part.degraded_clear {
            Self::Clear
        } else if let Some(p) = &part.degraded_from {
            Self::Flag(Pos::from(p))
        } else {
            Self::Nothing
        }
    }
}

/// A node's records, as a grantor.
#[derive(Debug)]
pub(crate) struct Records {
    dir: PathBuf,
    table: Mutex<BTreeMap<String, Record>>,
}

impl Records {
    /// None yet, for `dir`: [`Records::load`] reads the file.
    pub(crate) fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
            table: Mutex::new(BTreeMap::new()),
        }
    }

    /// Reads [`DEGRADED_FILE`] (none: no record). A file that doesn't parse fails the
    /// node's start, as a bad `cluster-writer-term` does: dropping it would lose a flag.
    pub(crate) fn load(&self) -> Result<(), String> {
        let path = self.dir.join(DEGRADED_FILE);
        let table = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| format!("{DEGRADED_FILE} isn't this build's records: {e}"))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => return Err(format!("read {DEGRADED_FILE}: {e}")),
        };
        *self.lock() = table;
        Ok(())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Record>> {
        self.table.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn persist(&self, table: &BTreeMap<String, Record>) -> std::io::Result<()> {
        #[cfg(test)]
        if super::seams::fail_degraded_writes(&self.dir) {
            return Err(std::io::Error::other("a test failed the write"));
        }
        let bytes = serde_json::to_vec(table).map_err(std::io::Error::other)?;
        super::write_synced(&self.dir, DEGRADED_FILE, &bytes)
    }

    /// `NodeStatus.degraded`: every record, in partition order.
    pub(crate) fn list(&self) -> Vec<DegradedRecord> {
        self.lock()
            .iter()
            .map(|(partition, r)| DegradedRecord {
                partition: partition.clone(),
                epoch: r.epoch,
                state: if r.from.is_some() {
                    DegradedState::Flagged
                } else {
                    DegradedState::Clear
                } as i32,
                degraded_from: r.from_pos().map(Position::from),
            })
            .collect()
    }

    /// Where partition `id`'s fallback began, if this node recorded it FLAGGED at `epoch`
    /// of `history`: an owner that reopens `id` resumes it (simp724's P1).
    pub(crate) fn flagged(&self, id: &str, epoch: u64, history: u64) -> Option<Pos> {
        self.lock()
            .get(id)
            .filter(|r| r.epoch == epoch && r.history == history)
            .and_then(Record::from_pos)
    }

    /// A lease grant's records (`mine` is the request's map: a grant is given only at the
    /// grantor's own version): for each of `granted` on a partition whose map sets
    /// `replica_acks`, what `asked` asked of it, written and synced if it changed anything.
    /// Then, and only then, each grant echoes its record: `degraded_from` where FLAGGED
    /// (the record's start, which may be below the one asked), unless the round asked to
    /// clear; `degraded_clear` where CLEAR and the round asked to clear. A failed write echoes
    /// nothing and returns the error: the grants stand, but the owner counts no record.
    pub(crate) fn record(
        &self,
        mine: &ClusterMap,
        asked: &[LeasePart],
        granted: &mut [LeasePart],
    ) -> std::io::Result<()> {
        let mut table = self.lock();
        let mut next = table.clone();
        let mut asks = Vec::new();
        for g in granted.iter() {
            let Some(p) = mine
                .partitions
                .iter()
                .find(|p| p.id == g.partition && p.replica_acks > 0)
            else {
                continue;
            };
            let ask = asked
                .iter()
                .find(|a| a.partition == g.partition)
                .map_or(Ask::Nothing, Ask::of);
            if let Some(r) = apply(next.get(&p.id).copied(), g.epoch, p.history_epoch, ask) {
                next.insert(p.id.clone(), r);
            }
            asks.push((g.partition.clone(), ask));
        }
        if next != *table {
            self.persist(&next)?;
            *table = next;
        }
        // The echoes, from the synced table alone (simp724: never from the request, so no
        // copy of it can echo by construction).
        for g in granted.iter_mut() {
            let ask = asks
                .iter()
                .find(|(p, _)| *p == g.partition)
                .map(|(_, a)| *a);
            let Some(r) = table.get(&g.partition).filter(|r| r.epoch == g.epoch) else {
                continue;
            };
            match (ask, r.from_pos()) {
                // A FLAGGED record is echoed whatever was asked: an owner that no longer
                // knows of it (restarted without its own record, or one that left after a
                // majority's clear this grantor missed) resumes its fallback and clears it,
                // so no grantor keeps a stale flag.
                (Some(Ask::Flag(_) | Ask::Nothing), Some(from)) => {
                    g.degraded_from = Some(from.into());
                }
                (Some(Ask::Clear), None) => g.degraded_clear = true,
                _ => {}
            }
            debug_assert!(
                g.degraded_from.is_none() || r.from.is_some(),
                "an echo not from a synced record"
            );
        }
        Ok(())
    }

    /// At an install of `map` on this node: the records of partitions it no longer has go,
    /// and so do those of an older history than its (a new history: P re-created, or
    /// relocated without its file). A record of a newer history than this map's is kept:
    /// this node's map may lag the owner's, and the next one catches up (simp724).
    pub(crate) fn retain(&self, map: &ClusterMap) -> std::io::Result<()> {
        let mut table = self.lock();
        let mut next = table.clone();
        next.retain(|id, r| {
            map.partitions
                .iter()
                .find(|p| p.id == *id)
                .is_some_and(|p| p.history_epoch <= r.history)
        });
        if next != *table {
            self.persist(&next)?;
            *table = next;
        }
        Ok(())
    }
}

/// A record after a grant at `epoch` of `history` that asked `ask`: none when it stays.
fn apply(r: Option<Record>, epoch: u64, history: u64, ask: Ask) -> Option<Record> {
    let fresh = Record {
        epoch,
        history,
        from: match ask {
            Ask::Flag(p) => Some((p.seqno, p.n)),
            Ask::Clear | Ask::Nothing => None,
        },
    };
    let Some(r) = r else {
        return Some(fresh);
    };
    // Another history: at grant time the request's map is this node's own, so a record of
    // a newer history than it can only be a dead one (P re-created, with lower epochs), and
    // a record of an older one is a past history's. Either way it's replaced (simp724).
    if r.history != history {
        return Some(fresh);
    }
    // By epoch first, then state: a lower epoch never changes a record, so an old owner's
    // late round can't lower what a promoted owner's rounds recorded (simp724).
    if epoch < r.epoch {
        return None;
    }
    if epoch > r.epoch {
        return Some(fresh);
    }
    let from = match (ask, r.from_pos()) {
        (Ask::Nothing, _) => return None,
        (Ask::Clear, _) => None,
        // A start already recorded stays unless lower: a flag is never raised without a
        // clear between, as writes past the lower one may have been answered uncovered.
        (Ask::Flag(p), Some(old)) => Some(p.min(old)),
        (Ask::Flag(p), None) => Some(p),
    };
    let next = Record {
        from: from.map(|p| (p.seqno, p.n)),
        ..r
    };
    (next != r).then_some(next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::proto::{ClusterMap, Partition};

    fn pos(seqno: u64) -> Pos {
        Pos { seqno, n: 0 }
    }

    #[test]
    fn a_first_grant_records_clear_and_an_epoch_never_goes_back() {
        let r = apply(None, 3, 1, Ask::Nothing).unwrap();
        assert_eq!(r.from, None);
        assert_eq!(r.epoch, 3);
        // Flagged, then a lower epoch's clear changes nothing.
        let f = apply(Some(r), 3, 1, Ask::Flag(pos(10))).unwrap();
        assert_eq!(f.from_pos(), Some(pos(10)));
        assert_eq!(apply(Some(f), 2, 1, Ask::Clear), None);
        // A renewal changes nothing; a higher flag keeps the lower start.
        assert_eq!(apply(Some(f), 3, 1, Ask::Nothing), None);
        assert_eq!(apply(Some(f), 3, 1, Ask::Flag(pos(20))), None);
        let lower = apply(Some(f), 3, 1, Ask::Flag(pos(5))).unwrap();
        assert_eq!(lower.from_pos(), Some(pos(5)));
        // The clear at the epoch, then a newer epoch starts clear.
        let c = apply(Some(f), 3, 1, Ask::Clear).unwrap();
        assert_eq!(c.from, None);
        let n = apply(Some(f), 4, 1, Ask::Nothing).unwrap();
        assert_eq!((n.epoch, n.from), (4, None));
    }

    /// simp724: P dropped and created again with the same id, at a lower epoch and history.
    /// At grant time the map is the grantor's own, so the old history's FLAGGED record is a
    /// dead one, and the new P's grant replaces it.
    #[test]
    fn a_recreated_partitions_grant_replaces_the_dead_historys_record() {
        let dead = Record {
            epoch: 7,
            history: 5,
            from: Some((40, 0)),
        };
        let r = apply(Some(dead), 1, 1, Ask::Nothing).unwrap();
        assert_eq!(
            r,
            Record {
                epoch: 1,
                history: 1,
                from: None
            }
        );
        let r = apply(Some(dead), 1, 1, Ask::Flag(pos(3))).unwrap();
        assert_eq!(r.from_pos(), Some(pos(3)));
    }

    fn map(parts: &[(&str, u64, u64, u32)]) -> ClusterMap {
        ClusterMap {
            partitions: parts
                .iter()
                .map(|&(id, epoch, history, k)| Partition {
                    id: id.into(),
                    epoch,
                    history_epoch: history,
                    replica_acks: k,
                    ..Partition::default()
                })
                .collect(),
            ..ClusterMap::default()
        }
    }

    fn part(id: &str, epoch: u64, ask: Ask) -> LeasePart {
        LeasePart {
            partition: id.into(),
            epoch,
            degraded_from: match ask {
                Ask::Flag(p) => Some(p.into()),
                _ => None,
            },
            degraded_clear: ask == Ask::Clear,
            ..LeasePart::default()
        }
    }

    fn granted(asked: &[LeasePart]) -> Vec<LeasePart> {
        asked
            .iter()
            .map(|a| LeasePart {
                partition: a.partition.clone(),
                epoch: a.epoch,
                ..LeasePart::default()
            })
            .collect()
    }

    #[test]
    fn a_grant_echoes_only_what_it_synced_and_a_restart_keeps_it() {
        let dir = tempfile::tempdir().unwrap();
        let m = map(&[("p1", 2, 1, 1), ("p0", 2, 1, 0)]);
        let recs = Records::new(dir.path());
        recs.load().unwrap();
        let asked = [
            part("p1", 2, Ask::Flag(pos(9))),
            part("p0", 2, Ask::Flag(pos(9))),
        ];
        let mut g = granted(&asked);
        recs.record(&m, &asked, &mut g).unwrap();
        // p1 (k = 1) echoes; p0 (k = 0) keeps no record and echoes nothing.
        assert_eq!(g[0].degraded_from, Some(pos(9).into()));
        assert_eq!(g[1].degraded_from, None);
        // Restarted: the flag is still there, and Status lists it.
        let again = Records::new(dir.path());
        again.load().unwrap();
        assert_eq!(again.flagged("p1", 2, 1), Some(pos(9)));
        let listed = again.list();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].state, DegradedState::Flagged as i32);
        // A renewal that asks nothing still echoes the flag, for an owner that lost it.
        let asked = [part("p1", 2, Ask::Nothing)];
        let mut g = granted(&asked);
        again.record(&m, &asked, &mut g).unwrap();
        assert_eq!(g[0].degraded_from, Some(pos(9).into()));
        // A clear echoes as a clear.
        let asked = [part("p1", 2, Ask::Clear)];
        let mut g = granted(&asked);
        again.record(&m, &asked, &mut g).unwrap();
        assert!(g[0].degraded_clear);
        assert_eq!(again.flagged("p1", 2, 1), None);
        assert_eq!(again.list()[0].state, DegradedState::Clear as i32);
    }

    #[test]
    fn a_failed_write_echoes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let m = map(&[("p1", 2, 1, 1)]);
        let recs = Records::new(dir.path());
        super::super::seams::FAIL_DEGRADED_WRITES
            .lock()
            .unwrap()
            .push(dir.path().to_path_buf());
        let asked = [part("p1", 2, Ask::Flag(pos(9)))];
        let mut g = granted(&asked);
        assert!(recs.record(&m, &asked, &mut g).is_err());
        assert_eq!(g[0].degraded_from, None);
        assert!(recs.list().is_empty());
    }

    #[test]
    fn an_install_drops_a_gone_or_older_history_and_keeps_a_newer_one() {
        let dir = tempfile::tempdir().unwrap();
        let recs = Records::new(dir.path());
        let m = map(&[("p1", 2, 1, 1), ("p2", 3, 3, 1), ("p3", 1, 1, 1)]);
        let asked = [
            part("p1", 2, Ask::Flag(pos(1))),
            part("p2", 3, Ask::Flag(pos(1))),
            part("p3", 1, Ask::Flag(pos(1))),
        ];
        let mut g = granted(&asked);
        recs.record(&m, &asked, &mut g).unwrap();
        // p1 gone; p2's map lags at an older history (kept); p3 at a newer one (dropped).
        recs.retain(&map(&[("p2", 2, 2, 1), ("p3", 4, 4, 1)]))
            .unwrap();
        let ids: Vec<_> = recs.list().into_iter().map(|r| r.partition).collect();
        assert_eq!(ids, ["p2"]);
    }
}
