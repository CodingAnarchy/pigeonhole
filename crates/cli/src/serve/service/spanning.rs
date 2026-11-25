use super::*;

/// The home's part of a spanning commit that wrote, as [`Service::spanning_write`] takes
/// it: its transaction, the durability asked for, and the rows the `Commit` carried for
/// the home (already applied to the transaction; kept for the range rule, #2155).
struct HomeCommit {
    txn: pigeonhole::Transaction,
    durability: Option<pigeonhole::Durability>,
    carried: Vec<proto::RowWrites>,
}

impl Service {
    /// The home partition's `Begin` of a spanning transaction (D329): its id carries the
    /// home's history epoch (`epoch`), and is its xid. The decision table is made first, if
    /// it's missing, so it's older than the transaction's snapshot: its commit point reads
    /// and writes it, which a table created after the snapshot would refuse.
    pub(crate) async fn begin_spanning(
        &self,
        r: proto::BeginRequest,
        caller: &Caller,
        epoch: u64,
    ) -> Result<proto::BeginResponse, ApiError> {
        if let Some(gate) = &self.gate {
            gate.serving()?;
        }
        drop(self.decisions_table().await?);
        let txn = self.part()?.db.transaction()?;
        let snapshot_seqno = txn.snapshot_seqno();
        let txn_id = self.sessions.begin_spanning(
            caller.principal,
            txn,
            convert::durability(r.durability),
            epoch,
        )?;
        Ok(proto::BeginResponse {
            txn_id,
            snapshot_seqno,
        })
    }

    /// Buffers decoded `rows` ([`txn_rows`](Self::txn_rows)) in the transaction `g` holds:
    /// it can't fail (D329's coordinator buffers a spanning call's home rows on the session
    /// it already holds).
    pub(crate) fn buffer_rows(
        g: &mut super::super::sessions::TxnGuard,
        rows: &[(Table, &[u8], Vec<convert::TxnOp<'_>>)],
    ) {
        for (table, row, ops) in rows {
            convert::txn_apply(g.txn(), table, row, ops);
        }
        if !rows.is_empty() {
            g.wrote_now();
        }
    }

    /// Whether the partition takes writes now (its gate admits one): what a spanning call
    /// checks before its first row goes to a branch.
    pub(crate) fn writable(&self) -> Result<(), ApiError> {
        match &self.gate {
            Some(gate) => gate.enter().map(drop),
            None => Ok(()),
        }
    }

    /// D331's range rule for a spanning transaction's home (#2155), as a plain commit's
    /// ([`Service::commit`]): `rows`, the home's own, are refused as writes to a range a
    /// move fences or moved away, and with `snapshot` a transaction that writes is refused
    /// when it's open across the range's fence or hand-over (M3). A `TxnWrite` passes no
    /// snapshot: its rows only. `Ok` off a cluster and while no move names the partition.
    pub(crate) fn home_ranges(
        &self,
        rows: &[proto::RowWrites],
        snapshot: Option<u64>,
    ) -> Result<(), ApiError> {
        match &self.gate {
            Some(gate) => Self::ranges_commit(gate, rows, snapshot),
            None => Ok(()),
        }
    }

    /// [`home_ranges`](Self::home_ranges) on `gate`, with one load of its range state.
    fn ranges_commit(
        gate: &super::super::gate::WriteGate,
        rows: &[proto::RowWrites],
        snapshot: Option<u64>,
    ) -> Result<(), ApiError> {
        match &*gate.ranges().load() {
            Some(ranges) => ranges.commit(rows, snapshot.unwrap_or(0), snapshot.is_some()),
            None => Ok(()),
        }
    }

    /// Ends spanning transaction `g` after a call that sent rows to a branch failed (D329,
    /// audit120's M3b on #2062): its branches are aborted (in the background, on their own
    /// budget), and `err` becomes the session's recorded outcome, so a retry finds the
    /// transaction ended rather than applying those rows again. Returns `err`.
    pub(crate) fn end_spanning(
        mut g: super::super::sessions::TxnGuard,
        err: ApiError,
        driver: &super::super::cluster::spanning::Driver,
    ) -> ApiError {
        use super::super::cluster::spanning::SpanView;
        if let Some(span) = g.span() {
            let mut branches = span.branches(&driver.home);
            branches.extend(span.idle(&driver.home));
            let d = driver.for_round();
            tokio::spawn(async move { d.abort(&branches).await });
        }
        let (txn, _, ticket) = g.commit();
        drop(txn);
        ticket.finish(Err(err.clone()));
        err
    }

    /// How long after its begin an absent decision record of a spanning transaction is
    /// presumed aborted (D329 2b): `--txn-outcome-retention`. A committed record can be
    /// collected from its commit time plus retention, which may be just after the begin, so
    /// from begin plus retention on an absent record may be a collected commit: not found,
    /// never presumed aborted (audit120's M1 on #2076). The startup floor (retention at least
    /// the lifetime, the request timeout and a minute) puts a transaction's whole life
    /// inside this window, with the minute for clock skew.
    pub(crate) fn outcome_horizon(&self) -> std::time::Duration {
        self.limits.txn_outcome_retention
    }

    /// `--txn-outcome-retention`.
    pub(crate) fn outcome_retention(&self) -> std::time::Duration {
        self.limits.txn_outcome_retention
    }

    /// Records that branches `acked` applied spanning transaction `xid`'s commit (D329 2b), in
    /// its decision row, for its GC. A row that's gone, or isn't a commit, is left as it is.
    pub(crate) async fn ack_decision(&self, xid: &str, acked: &[String]) -> Result<(), ApiError> {
        use super::super::cluster::spanning::{DECISION_FAMILY, DECISION_QUALIFIER, with_acks};
        if acked.is_empty() {
            return Ok(());
        }
        let table = self.decisions_table().await?;
        let row = xid.as_bytes().to_vec();
        for _ in 0..3 {
            let mut txn = self.part()?.db.transaction()?;
            let Some(c) = txn
                .get_async(&table, &row, DECISION_FAMILY, DECISION_QUALIFIER)
                .await?
            else {
                return Ok(());
            };
            let Some(value) = with_acks(c.value(), acked) else {
                return Ok(());
            };
            txn.put(&table, &row, DECISION_FAMILY, DECISION_QUALIFIER, &value);
            let held = match &self.gate {
                Some(gate) => Some(gate.enter()?),
                None => None,
            };
            let done = txn.commit_async().await.map_err(ApiError::from);
            drop(held);
            match done {
                Ok(_) => return Ok(()),
                Err(e) if e.code == "Conflict" => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Deletes this home's decision rows past `--txn-outcome-retention` at `now_ms` (D329
    /// 2b): commits every branch acknowledged, and abort markers. Returns how many went.
    pub(crate) async fn sweep_decisions(&self, now_ms: u64) -> Result<usize, ApiError> {
        use super::super::cluster::spanning::{DECISION_FAMILY, DECISION_QUALIFIER, collectable};
        // A home that never decided anything has no table: nothing to sweep, and nothing
        // written to create it (the balancer judges an idle partition's device bytes).
        let Some(table) = self.existing_decisions_table()? else {
            return Ok(0);
        };
        let retention = u64::try_from(self.outcome_retention().as_millis()).unwrap_or(u64::MAX);
        let gone: Vec<Vec<u8>> = {
            let table = table.clone();
            self.blocking(move || {
                let mut gone = Vec::new();
                for row in table.scan_prefix(b"").iter()? {
                    let row = row?;
                    let Some(cell) = row.get(DECISION_FAMILY, DECISION_QUALIFIER) else {
                        continue;
                    };
                    if collectable(cell.value(), now_ms, retention) {
                        gone.push(row.key().to_vec());
                    }
                }
                Ok(gone)
            })
            .await?
        };
        let held = match &self.gate {
            Some(gate) => Some(gate.enter()?),
            None => None,
        };
        let mut n = 0;
        for key in &gone {
            table.mutate(key).delete_row().commit()?;
            n += 1;
        }
        drop(held);
        Ok(n)
    }

    /// This home's committed decision rows past `--txn-outcome-retention` at `now_ms` that
    /// some branch never acked, at most `limit` of them, after xid `after` if given (the
    /// sweep's cursor): each xid with the branches it still waits on (#2098).
    pub(crate) async fn unacked_decisions(
        &self,
        now_ms: u64,
        limit: usize,
        after: Option<&str>,
    ) -> Result<Vec<(String, Vec<String>)>, ApiError> {
        use super::super::cluster::spanning::{DECISION_FAMILY, DECISION_QUALIFIER, unacked};
        let Some(table) = self.existing_decisions_table()? else {
            return Ok(Vec::new());
        };
        let retention = u64::try_from(self.outcome_retention().as_millis()).unwrap_or(u64::MAX);
        let after = after.map(|a| a.as_bytes().to_vec());
        self.blocking(move || {
            let mut out = Vec::new();
            for row in table.scan_prefix(b"").iter()? {
                if out.len() >= limit {
                    break;
                }
                let row = row?;
                if after.as_deref().is_some_and(|a| row.key() <= a) {
                    continue;
                }
                let Some(cell) = row.get(DECISION_FAMILY, DECISION_QUALIFIER) else {
                    continue;
                };
                let (Ok(xid), Some(waiting)) = (
                    std::str::from_utf8(row.key()),
                    unacked(cell.value(), now_ms, retention),
                ) else {
                    continue;
                };
                out.push((xid.to_owned(), waiting));
            }
            Ok(out)
        })
        .await
    }

    /// What transaction `id` is now, without taking it (D329's `Outcome` and resolver), as
    /// `principal` sees it; `None` is the node's own call ([`super::super::sessions::Sessions::peek`]).
    pub(crate) fn peek_txn(
        &self,
        id: &str,
        principal: Option<super::super::auth::Principal>,
    ) -> super::super::sessions::Peek {
        self.sessions.peek(id, principal)
    }

    /// `--txn-lifetime-ms`.
    pub(crate) fn txn_lifetime(&self) -> std::time::Duration {
        self.sessions.txn_lifetime()
    }

    /// Spanning transaction `xid`'s decision row on this home partition (D329), read now:
    /// `None` when it has none.
    pub(crate) async fn decision(&self, xid: &str) -> Result<Option<Vec<u8>>, ApiError> {
        use super::super::cluster::spanning::{DECISION_FAMILY, DECISION_QUALIFIER};
        let table = self.decisions_table().await?;
        let row = xid.as_bytes().to_vec();
        self.blocking(move || {
            Ok(table
                .get(&row, DECISION_FAMILY, DECISION_QUALIFIER)?
                .map(|c| c.value().to_vec()))
        })
        .await
    }

    /// Makes spanning transaction `xid`'s outcome final as aborted, if nothing decided it
    /// (D329: the resolver's and `Outcome`'s absent row, with the home's history unchanged):
    /// an OCC transaction that reads the row and, absent, writes the abort marker through
    /// the gate at `GroupSync`, so the commit point (which reads the row absent and writes
    /// `COMMITTED`) and this can't both win. Returns the row as it is now: the marker, or
    /// what a commit point that won the race wrote.
    pub(crate) async fn decide_absent(&self, xid: &str, epoch: u64) -> Result<Vec<u8>, ApiError> {
        use super::super::cluster::spanning::{DECISION_FAMILY, DECISION_QUALIFIER, aborted_value};
        let table = self.decisions_table().await?;
        let row = xid.as_bytes().to_vec();
        for _ in 0..3 {
            let mut txn = self.part()?.db.transaction()?;
            if let Some(c) = txn
                .get_async(&table, &row, DECISION_FAMILY, DECISION_QUALIFIER)
                .await?
            {
                return Ok(c.value().to_vec());
            }
            let marker = aborted_value(epoch);
            txn.put(&table, &row, DECISION_FAMILY, DECISION_QUALIFIER, &marker);
            let held = match &self.gate {
                Some(gate) => Some(gate.enter()?),
                None => None,
            };
            let committed = txn
                .commit_with_async(pigeonhole::Durability::GroupSync)
                .await
                .map_err(ApiError::from)
                .and_then(|c| match &held {
                    Some(h) => h.committed().map(|()| c),
                    None => Ok(c),
                });
            drop(held);
            match committed {
                Ok(_) => return Ok(marker),
                // The commit point (or another resolver) wrote it meanwhile: read again.
                Err(e) if e.code == "Conflict" => continue,
                Err(e) => return Err(e),
            }
        }
        Err(ApiError::unavailable(
            "the transaction's decision row kept changing: ask again",
        ))
    }

    /// Takes transaction `id` for `caller`'s call (D329's coordinator: a spanning
    /// transaction's home session, with its span).
    pub(crate) fn take_txn(&self, id: &str, caller: &Caller) -> Result<Taken, ApiError> {
        self.sessions.take(id, caller.principal)
    }

    /// Opens spanning transaction `xid`'s branch on this partition (D329), at its snapshot
    /// now, for the client the home forwarded; with `renew`, renews the open one.
    pub(crate) fn begin_branch(
        &self,
        xid: &str,
        caller: &Caller,
        renew: bool,
    ) -> Result<(), ApiError> {
        if let Some(gate) = &self.gate {
            gate.serving()?;
        }
        self.sessions.begin_branch(
            caller.principal,
            || Ok(self.part()?.db.transaction()?),
            xid,
            renew,
        )
    }

    /// Prepares branch `xid` (D329) for its home's commit. A writing transaction's branch
    /// prepares durably, at `GroupSync`, through the gate (a write: its reads and writes
    /// held until its decide). A read-only one holds its reads in memory, under the lease
    /// term it was taken in, kept in `spanning` for the release (for twice the request
    /// timeout at most: its release round is one call).
    pub(crate) async fn prepare_branch(
        &self,
        xid: &str,
        read_only: bool,
        spanning: &Arc<super::super::cluster::spanning::Spanning>,
        partition: &str,
    ) -> Result<proto::BranchPrepared, ApiError> {
        let gate = self
            .gate
            .as_ref()
            .ok_or_else(|| ApiError::internal("a branch off a cluster partition"))?;
        let mut g = match self.sessions.take_branch(xid)? {
            Taken::Open(g) => g,
            Taken::Done(outcome) => return Err(ApiError::transaction_finished(&outcome)),
        };
        // A prepared branch's tombstone: its outcome is its home's to give.
        let prepared_mark = || ApiError::invalid("the branch is prepared: its home decides it");
        if read_only {
            let term = gate.serving_term()?;
            let (txn, _, ticket) = g.commit();
            let held = self
                .blocking(move || txn.hold_reads().map_err(ApiError::from))
                .await;
            // Taken, and the lease ran on unbroken: the hold counts from now.
            let held = held.and_then(|h| match gate.serving_term() {
                Ok(t) if t == term => Ok(h),
                _ => Err(lost_hold(partition)),
            });
            let ttl = self.request_timeout() * 2;
            let prepared = held.map(|h| proto::BranchPrepared {
                hold: spanning.keep_hold(h, partition, term, ttl),
                term,
            });
            ticket.finish(Err(prepared
                .as_ref()
                .err()
                .cloned()
                .unwrap_or_else(prepared_mark)));
            return prepared;
        }
        let held = gate.enter()?;
        // D331 PR 5 (cli692's M1 on #2130): a local commit's range rule, under the held
        // count. A branch whose transaction began before a range fence or hand-over here may
        // hold rows of that range written before it: refused (retry-safe), as a local
        // commit is, so its share can't land above the fence's seqno. One that passes is
        // either waited for by the fence's drain (admitted before it) or saw no fence.
        if let Some(ranges) = &*gate.ranges().load() {
            ranges.commit(&[], g.txn().snapshot_seqno(), true)?;
        }
        // D331 PR 5: what its writes touched, for a range move's in-doubt watch. Recorded
        // before the share can be in doubt.
        gate.ranges().prepared(xid.as_bytes(), g.watched().clone());
        let (txn, _, ticket) = g.commit();
        let xid_bytes = xid.as_bytes().to_vec();
        let prepared = self
            .blocking(move || {
                txn.prepare(&xid_bytes, pigeonhole::Durability::GroupSync)
                    .map_err(ApiError::from)
            })
            .await
            .and_then(|c| held.committed().map(|()| c));
        drop(held);
        ticket.finish(Err(prepared
            .as_ref()
            .err()
            .cloned()
            .unwrap_or_else(prepared_mark)));
        prepared.map(|_| proto::BranchPrepared { hold: 0, term: 0 })
    }

    /// Decides branch `xid` (D329). With `hold`, a read-only branch's release: whether its
    /// hold lasted, under lease term `term` (`Conflict` if not). Otherwise its decide through
    /// the gate, synced (the prepare's `GroupSync`) before it answers: the commit's info, or
    /// `None` for an abort or a share no longer in doubt. An abort also ends a branch that
    /// never prepared.
    pub(crate) async fn decide_branch(
        &self,
        xid: &str,
        commit: bool,
        hold: u64,
        term: u64,
        spanning: &super::super::cluster::spanning::Spanning,
        partition: &str,
    ) -> Result<Option<pigeonhole::CommitInfo>, ApiError> {
        let gate = self
            .gate
            .as_ref()
            .ok_or_else(|| ApiError::internal("a branch off a cluster partition"))?;
        if hold != 0 {
            let Some((h, taken)) = spanning.take_hold(hold, partition) else {
                return Err(lost_hold(partition));
            };
            let lasted = self
                .blocking(move || h.release().map_err(ApiError::from))
                .await?;
            return match gate.serving_term() {
                Ok(t) if lasted && t == term && taken == term => Ok(None),
                _ => Err(lost_hold(partition)),
            };
        }
        if !commit && let Ok(Taken::Open(g)) = self.sessions.take_branch(xid) {
            g.abort();
        }
        let held = gate.enter()?;
        let db = self.part()?.db.clone();
        let xid_bytes = xid.as_bytes().to_vec();
        let decided = self
            .blocking(move || db.decide(&xid_bytes, commit).map_err(ApiError::from))
            .await;
        let decided = decided.and_then(|c| held.committed().map(|()| c));
        drop(held);
        decided
    }

    /// The home's commit of spanning transaction `r.txn_id` (D329), its session `g` already
    /// taken by the coordinator (its tombstone answered a repeated `Commit` there), with
    /// `driver` reaching its branches: prepares, the commit point (the decision row in the
    /// home's own commit) and the decides when anything was written anywhere; holds and
    /// their release round when nothing was.
    ///
    /// Until the session turns to `Committing`, a refusal puts it back as it was, its span
    /// and branches untouched, so a retried `Commit` commits the whole transaction (audit120's
    /// M1 on #2062). From then the round runs detached, in its own task, as
    /// [`commit`](Self::commit)'s does: a client that goes away never leaves branches
    /// prepared past the commit point undecided by this node.
    pub(crate) async fn commit_spanning(
        self: &Arc<Self>,
        mut g: super::super::sessions::TxnGuard,
        r: proto::TxnCommitRequest,
        caller: &Caller,
        budget: OwnedSemaphorePermit,
        driver: super::super::cluster::spanning::Driver,
        forwarded: bool,
    ) -> Result<proto::TxnCommitResponse, ApiError> {
        use super::super::cluster::spanning::SpanView;
        let gate = Arc::clone(
            self.gate
                .as_ref()
                .ok_or_else(|| ApiError::internal("a spanning commit off a cluster"))?,
        );
        let home = driver.home.to_string();
        let span = g
            .span()
            .cloned()
            .ok_or_else(|| ApiError::internal("a spanning id on a plain session"))?;
        let writes = span.wrote() || !r.rows.is_empty() || g.wrote();
        // The coordinator checked these before the commit's rows went to branches; if one
        // fails now anyway (a table dropped in between), a commit that sent rows ends the
        // transaction rather than leave them applied for a retry (audit120's M3b).
        let refuse = |g, e: ApiError| {
            if forwarded {
                Err(Self::end_spanning(g, e, &driver))
            } else {
                Err(e)
            }
        };
        if writes && !caller.scope.allows(Scope::Write) {
            return refuse(g, ApiError::permission_denied("commit writes"));
        }
        let rows = match self.txn_rows(&r.rows).await {
            Ok(rows) => rows,
            Err(e) => return refuse(g, e),
        };
        let decisions = if writes {
            match self.decisions_table().await {
                Ok(t) => Some(t),
                Err(e) => return refuse(g, e),
            }
        } else {
            None
        };
        for (table, row, ops) in &rows {
            convert::txn_apply(g.txn(), table, row, ops);
        }
        drop(rows);
        let branches = span.branches(&home);
        let idle = span.idle(&home);
        let home_read = span.read(&home);
        let carried = r.rows;
        let (txn, durability, ticket) = g.commit();
        let me = Arc::clone(self);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let metrics = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            // Branches opened that did nothing aren't prepared (they'd refuse): their
            // sessions end (audit120's S3), beside the round, not before it (a silent node
            // there mustn't spend the client's deadline).
            if !idle.is_empty() {
                let d = driver.for_round();
                tokio::spawn(async move { d.abort(&idle).await });
            }
            let outcome = match decisions {
                Some(decisions) => {
                    let home = HomeCommit {
                        txn,
                        durability,
                        carried,
                    };
                    me.spanning_write(home, &decisions, &gate, &driver, &branches)
                        .await
                }
                None => {
                    me.spanning_read(txn, home_read, &gate, &driver, &branches)
                        .await
                }
            };
            ticket.finish(outcome.clone());
            drop(budget);
            // The branches its decide round applied, recorded for the decision row's GC,
            // after the reply (D329 2b).
            let applied: Vec<String> = match &outcome {
                Ok(r) => r
                    .parts
                    .iter()
                    .skip(1)
                    .filter(|p| p.applied)
                    .map(|p| p.partition.clone())
                    .collect(),
                Err(_) => Vec::new(),
            };
            if tx.send(outcome).is_err() {
                metrics.replies_lost.fetch_add(1, Ordering::Relaxed);
            }
            if !applied.is_empty()
                && let Err(e) = me.ack_decision(&driver.xid, &applied).await
            {
                eprintln!(
                    "phdb serve: recording a transaction's branch acks: {}",
                    e.message
                );
            }
        });
        rx.await.unwrap_or_else(|_| {
            Err(ApiError::outcome_unknown(
                "the commit's task ended without an outcome",
            ))
        })
    }

    /// The home's decision table (D329). Created through the gate if it's missing (a write
    /// to the file: never past a fence's drain), the first time a spanning transaction
    /// begins on the partition.
    /// This home's decision table, if it has one: the decision sweeps read it, and never
    /// create it, so an idle partition's sweep writes nothing (#2164: a catalog change on
    /// every partition of an idle cluster showed in its device load).
    pub(crate) fn existing_decisions_table(&self) -> Result<Option<Table>, ApiError> {
        use super::super::cluster::spanning::DECISIONS;
        Ok(self.part()?.db.table(DECISIONS).and_then(|t| t.open()).ok())
    }

    pub(crate) async fn decisions_table(&self) -> Result<Table, ApiError> {
        use super::super::cluster::spanning::{DECISION_FAMILY, DECISIONS};
        let db = self.part()?.db.clone();
        if let Ok(t) = db.table(DECISIONS).and_then(|t| t.open()) {
            return Ok(t);
        }
        let held = match &self.gate {
            Some(gate) => Some(gate.enter()?),
            None => None,
        };
        let table = self
            .blocking(move || {
                Ok(db
                    .table(DECISIONS)?
                    .family(DECISION_FAMILY, pigeonhole::Family::default())
                    .create_if_missing()?)
            })
            .await;
        drop(held);
        table
    }

    /// A spanning commit that wrote: every branch prepares, then the commit point, then the
    /// decides. Aborts and decides run on a budget of their own ([`Driver::for_round`]),
    /// since the prepare round may have used the client's (audit120's S1); the answer waits
    /// for the decides only until the client's deadline.
    ///
    /// [`Driver::for_round`]: super::super::cluster::spanning::Driver::for_round
    async fn spanning_write(
        &self,
        home: HomeCommit,
        decisions: &Table,
        gate: &Arc<super::super::gate::WriteGate>,
        driver: &super::super::cluster::spanning::Driver,
        branches: &[String],
    ) -> super::super::sessions::Outcome {
        let HomeCommit {
            mut txn,
            durability,
            carried,
        } = home;
        use super::super::cluster::spanning::{
            DECISION_FAMILY, DECISION_QUALIFIER, committed_value,
        };
        let conflict = |why: String| ApiError::from_code(pigeonhole::ErrorCode::Conflict, &why);
        let abort = || async { driver.for_round().abort(branches).await };
        if let Err(e) = driver.check_feature().await {
            abort().await;
            return Err(e);
        }
        let (_, refused) = driver.prepare(branches, false).await;
        if let Some(e) = refused {
            abort().await;
            // Never committed: the commit point wasn't reached, so any refusal, a timeout
            // too, is safe to retry.
            return Err(conflict(format!(
                "a branch refused its prepare ({}): {}",
                e.code, e.message
            )));
        }
        #[cfg(test)]
        {
            use super::super::cluster::spanning::seams;
            if seams::at(&driver.xid, seams::Step::CommitPoint).await {
                return Err(ApiError::outcome_unknown(
                    "abandoned before the commit point",
                ));
            }
        }
        // D331 PR 6 (Amendment 4's MUST): no commit point on a home the map places nothing
        // on (a merge emptied it), so no decision row is born there after. One already
        // racing this is a live home session, which keeps the home listed (`drop_waits`)
        // until it ends.
        if driver
            .node
            .get_map()
            .is_ok_and(|m| !super::super::cluster::map::placed(&m, &driver.home))
        {
            abort().await;
            return Err(conflict(format!(
                "partition {:?} holds no range now (a merge emptied it), so it is no \
                 transaction's home: begin it again",
                driver.home
            )));
        }
        // The commit point: the decision row read (absent: no abort marker) and written in
        // the home's own commit, through its gate, at GroupSync or stronger.
        let epoch = super::super::sessions::spanning_parts(&driver.xid).map_or(0, |(_, e, _)| e);
        let row = driver.xid.as_bytes().to_vec();
        let marker = match txn
            .get_async(decisions, &row, DECISION_FAMILY, DECISION_QUALIFIER)
            .await
        {
            Ok(m) => m,
            Err(e) => {
                abort().await;
                return Err(e.into());
            }
        };
        if marker.is_some() {
            abort().await;
            return Err(conflict(
                "the transaction was decided without its home (an abort marker)".into(),
            ));
        }
        txn.put(
            decisions,
            &row,
            DECISION_FAMILY,
            DECISION_QUALIFIER,
            &committed_value(epoch, branches),
        );
        // Its records and feed events carry the xid, as every branch's share does (D329 2b):
        // a subscriber groups the commit's shares by it. A home-only commit too (F3).
        txn.tag_xid(driver.xid.as_bytes());
        let held = match gate.enter() {
            Ok(h) => h,
            Err(e) => {
                abort().await;
                return Err(e);
            }
        };
        // D331's range rule under the held count (#2155), as a plain commit's and a branch's
        // prepare: the rows this commit carried, as writes, and M3, so a home row buffered
        // before a range's fence or hand-over can't land above the fence's seqno. One that
        // passes is either waited for by the fence's drain (admitted before it) or saw no
        // fence. Before the commit point: the branches abort, and the refusal is retry-safe.
        if let Err(e) = Self::ranges_commit(gate, &carried, Some(txn.snapshot_seqno())) {
            drop(held);
            abort().await;
            return Err(e);
        }
        let d = durability
            .unwrap_or(pigeonhole::Durability::GroupSync)
            .max(pigeonhole::Durability::GroupSync);
        let committed = match tokio::spawn(txn.commit_with_async(d)).await {
            Ok(r) => r.map_err(ApiError::from),
            Err(e) => Err(ApiError::outcome_unknown(format!(
                "the commit's task failed: {e}"
            ))),
        }
        .and_then(|c| held.committed().map(|()| c));
        drop(held);
        let info = match committed {
            Ok(c) => c,
            // Unknown: the branches stay in doubt, and the resolver settles them from the
            // decision row (D329 PR 2b).
            Err(e) if e.code == "OutcomeUnknown" => return Err(e),
            Err(e) => {
                abort().await;
                return Err(e);
            }
        };
        #[cfg(test)]
        {
            use super::super::cluster::spanning::seams;
            if seams::at(&driver.xid, seams::Step::Decide).await {
                return Err(ApiError::outcome_unknown("abandoned before the decides"));
            }
        }
        let home = proto::TxnPart {
            partition: driver.home.to_string(),
            seqno: info.seqno,
            applied: true,
        };
        // The decides on their own budget, in a task of their own: they go on to every
        // branch whatever the client's deadline; the answer waits for them until then.
        let decides = {
            let (d, b) = (driver.for_round(), branches.to_vec());
            tokio::spawn(async move { d.decide(&b, true).await })
        };
        let decided = tokio::time::timeout_at(driver.deadline, decides).await;
        let mut parts = vec![home];
        match decided {
            Ok(Ok(decided)) => parts.extend(decided),
            // Not all answered in time (or the round's task failed): each is committed, and
            // its resolver applies it if this round doesn't.
            _ => parts.extend(branches.iter().map(|p| proto::TxnPart {
                partition: p.clone(),
                seqno: 0,
                applied: false,
            })),
        }
        Ok(proto::TxnCommitResponse {
            commit: Some(convert::commit_info(info)),
            parts,
        })
    }

    /// A spanning commit that wrote nothing: every partition that read holds its reads, the
    /// home too, then a release round confirms that every hold lasted (`Conflict` if not).
    async fn spanning_read(
        &self,
        txn: pigeonhole::Transaction,
        home_read: bool,
        gate: &Arc<super::super::gate::WriteGate>,
        driver: &super::super::cluster::spanning::Driver,
        branches: &[String],
    ) -> super::super::sessions::Outcome {
        let seqno = txn.snapshot_seqno();
        let lost = || {
            ApiError::from_code(
                pigeonhole::ErrorCode::Conflict,
                "a read of the transaction changed or lost its hold before the commit (a \
                 partition's lease lapsed, it moved or it reopened): retry it",
            )
        };
        // The home's own hold first: a branch's taken after it overlaps it. A home whose
        // read call failed holds nothing (`InvalidArgument`: no reads).
        let home = if home_read {
            let term = gate.serving_term()?;
            match self
                .blocking(move || txn.hold_reads().map_err(ApiError::from))
                .await
            {
                Ok(hold) => Some((hold, term)),
                Err(e) if e.code == "InvalidArgument" => None,
                Err(e) => return Err(e),
            }
        } else {
            drop(txn);
            None
        };
        let (held, refused) = driver.prepare(branches, true).await;
        #[cfg(test)]
        {
            use super::super::cluster::spanning::seams;
            if seams::at(&driver.xid, seams::Step::Release).await {
                // Abandoned: the branches' holds are never released (they expire).
                return Err(lost());
            }
        }
        let round = driver.for_round();
        let lasted = refused.is_none() && round.release(&held).await;
        let home_lasted = match home {
            Some((hold, term)) => {
                let lasted = self
                    .blocking(move || hold.release().map_err(ApiError::from))
                    .await
                    .unwrap_or(false);
                lasted && gate.serving_term().is_ok_and(|t| t == term)
            }
            None => true,
        };
        if refused.is_some() {
            // Release whatever was held; the answer is the refusal's.
            let _ = round.release(&held).await;
        }
        if lasted && home_lasted {
            Ok(proto::TxnCommitResponse {
                commit: Some(proto::CommitInfo {
                    seqno,
                    durability: 0,
                }),
                parts: Vec::new(),
            })
        } else {
            Err(lost())
        }
    }
}
