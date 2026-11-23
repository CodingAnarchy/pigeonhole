//! D331 PR 6: what keeps a partition a merge emptied in the cluster map
//! (`LocalPartition.drop_waits`, `.coord/cli692-d331-pr6-design.md` §2).

use super::*;

impl Service {
    /// The first thing that still needs this partition listed, though the map places no
    /// range on it (a merge emptied it), or `None` once it may be removed. Blocking: it reads
    /// the decision rows. In the design's order:
    /// 1. a moved-away row: the feed tail of a range moved off it, readable until its feed
    ///    retention passes that move's last seqno and the row is pruned;
    /// 2. a decision row a resolver or `Outcome` may still need (D329 2b: not collectable);
    /// 3. a spanning transaction homed here still open;
    /// 4. a share of its own held in doubt.
    ///
    /// A share elsewhere whose transaction is homed here is the remover's to check, on every
    /// node (simp724's M1 on the design).
    pub(crate) fn drop_waits(&self, now_ms: u64) -> Option<String> {
        use super::super::cluster::spanning::{
            DECISION_FAMILY, DECISION_QUALIFIER, DECISIONS, collectable,
        };
        if let Some(m) = self.gate.as_ref().and_then(|g| {
            g.ranges()
                .moved_rows()
                .into_iter()
                .max_by_key(|m| m.final_seqno)
        }) {
            return Some(format!(
                "its change feed: move {} took {:?} at seqno {}; its moved-away row is pruned \
                 once this partition's feed retention passes that",
                m.id, m.table, m.final_seqno
            ));
        }
        let db = &self.part_opt()?.db;
        if let Ok(table) = db.table(DECISIONS).and_then(|t| t.open()) {
            let retention = u64::try_from(self.outcome_retention().as_millis()).unwrap_or(u64::MAX);
            let rows = match table.scan_prefix(b"").iter() {
                Ok(rows) => rows,
                Err(e) => return Some(format!("its decision rows couldn't be read: {e}")),
            };
            let mut needed = 0usize;
            for row in rows {
                match row {
                    Ok(row) => {
                        if row
                            .get(DECISION_FAMILY, DECISION_QUALIFIER)
                            .is_some_and(|c| !collectable(c.value(), now_ms, retention))
                        {
                            needed += 1;
                        }
                    }
                    Err(e) => return Some(format!("its decision rows couldn't be read: {e}")),
                }
            }
            if needed > 0 {
                return Some(format!(
                    "{needed} transaction outcome(s) homed here, kept until every branch has \
                     acknowledged and --txn-outcome-retention has passed"
                ));
            }
        }
        let homes = self.sessions.live_homes();
        if homes > 0 {
            return Some(format!(
                "{homes} spanning transaction(s) homed here still open (--txn-lifetime-ms \
                 bounds them)"
            ));
        }
        let shares = db.in_doubt_shares().len();
        if shares > 0 {
            return Some(format!("{shares} share(s) of its own held in doubt"));
        }
        None
    }
}
