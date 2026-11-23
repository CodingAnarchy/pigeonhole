use super::*;

/// A read-only spanning transaction's hold on `partition` lost (D329): released, the
/// partition's lease lapsed or changed term, it moved, or this node restarted.
pub(super) fn lost_hold(partition: &str) -> ApiError {
    ApiError::from_code(
        pigeonhole::ErrorCode::Conflict,
        &format!(
            "the transaction's hold of its reads on partition {partition:?} didn't last to \
             its commit: retry it"
        ),
    )
}

impl Service {
    /// `Transactions.Begin`: a transaction at a snapshot taken now, held for the caller.
    pub(super) fn begin(
        &self,
        r: proto::BeginRequest,
        caller: &Caller,
    ) -> Result<proto::BeginResponse, ApiError> {
        let txn = self.part()?.db.transaction()?;
        let snapshot_seqno = txn.snapshot_seqno();
        let txn_id =
            self.sessions
                .begin(caller.principal, txn, convert::durability(r.durability))?;
        Ok(proto::BeginResponse {
            txn_id,
            snapshot_seqno,
        })
    }

    /// Takes transaction `id` for a call that only an open transaction answers.
    fn open_txn(
        &self,
        id: &str,
        caller: &Caller,
    ) -> Result<super::super::sessions::TxnGuard, ApiError> {
        match self.sessions.take(id, caller.principal)? {
            Taken::Open(g) => Ok(g),
            Taken::Done(outcome) => Err(ApiError::transaction_finished(&outcome)),
        }
    }

    /// A request's rows, ready to buffer: each row's table opened and every write checked
    /// and decoded (`convert::txn_ops`), all before the transaction is taken out. So a missing
    /// table or a malformed write fails the call with nothing buffered (simp724's T1 on
    /// #1511), and buffering the rest can't fail on the server's side.
    pub(crate) async fn txn_rows<'r>(
        &self,
        rows: &'r [proto::RowWrites],
    ) -> Result<Vec<(Table, &'r [u8], Vec<convert::TxnOp<'r>>)>, ApiError> {
        let mut out = Vec::with_capacity(rows.len());
        for rw in rows {
            let key = rw
                .row
                .as_ref()
                .ok_or_else(|| ApiError::invalid("each row's writes name its table and row"))?;
            let ops = convert::txn_ops(&rw.writes)?;
            out.push((self.table(&key.table).await?, &key.row[..], ops));
        }
        Ok(out)
    }

    pub(super) async fn txn_get(
        &self,
        r: proto::TxnGetRequest,
        caller: &Caller,
    ) -> Result<proto::GetResponse, ApiError> {
        let table = self.table(&r.table).await?;
        let mut g = self.open_txn(&r.txn_id, caller)?;
        let cell = g
            .txn()
            .get_async(&table, &r.row, &r.family, &r.qualifier)
            .await?;
        drop(g);
        Ok(proto::GetResponse {
            cell: cell.map(|c| {
                let kind = convert::View::of(&table, &r.family);
                convert::cell(&r.family, &r.qualifier, &c, kind, None)
            }),
        })
    }

    /// On the blocking pool, as `get_row` is sync-only (D196): the transaction is lent to the
    /// blocking call and back, and a panic there expires the session (uncommitted).
    pub(super) async fn txn_get_row(
        &self,
        r: proto::TxnGetRowRequest,
        caller: &Caller,
    ) -> Result<proto::ReadRowResponse, ApiError> {
        let table = self.table(&r.table).await?;
        let mut g = self.open_txn(&r.txn_id, caller)?;
        let mut txn = g.lend();
        let (txn, row) = self
            .blocking(move || {
                let families: Vec<&str> = r.families.iter().map(String::as_str).collect();
                let kinds = convert::Kinds::new(&table);
                let row = txn
                    .get_row(&table, &r.row, &families)
                    .map(|row| row.map(|row| convert::row(row, &kinds, None)));
                Ok((txn, row))
            })
            .await?;
        g.restore(txn);
        drop(g);
        let row = row?;
        let bytes = row.as_ref().map_or(0, row_bytes);
        Ok(proto::ReadRowResponse {
            row,
            next: Default::default(),
            bytes,
        })
    }

    /// `Transactions.Write`: buffers the writes, all or none (`txn_rows`). What only the
    /// engine can check (a family, a size) fails the `Commit`, as in-process.
    pub(super) async fn txn_write(
        &self,
        r: proto::TxnWriteRequest,
        caller: &Caller,
    ) -> Result<proto::Empty, ApiError> {
        let rows = self.txn_rows(&r.rows).await?;
        let mut g = self.open_txn(&r.txn_id, caller)?;
        // D331 PR 5's in-doubt watch: the sequence before the state (simp724's rule).
        let watch = self.gate.as_ref().map(|gate| {
            let seq = gate.ranges().watch_seq();
            let moves = gate
                .ranges()
                .load()
                .as_ref()
                .map_or_else(Vec::new, |s| s.watched(&r.rows));
            (seq, moves)
        });
        for (table, row, ops) in &rows {
            convert::txn_apply(g.txn(), table, row, ops);
        }
        if !rows.is_empty() {
            g.wrote_now();
            if let Some((seq, moves)) = watch {
                g.note_write(seq, moves);
            }
        }
        Ok(proto::Empty {})
    }

    /// `Transactions.Commit`, detached from the request (D313 B2): the commit runs in its own
    /// task, which records the outcome as the session's tombstone (B1), so a client that goes
    /// away loses only the reply. A repeated `Commit` answers the tombstone.
    pub(super) async fn commit(
        &self,
        r: proto::TxnCommitRequest,
        caller: &Caller,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::TxnCommitResponse, ApiError> {
        if !r.rows.is_empty() && !caller.scope.allows(Scope::Write) {
            return Err(ApiError::permission_denied("commit writes"));
        }
        let rows = self.txn_rows(&r.rows).await?;
        let mut g = match self.sessions.take(&r.txn_id, caller.principal)? {
            Taken::Open(g) => g,
            Taken::Done(outcome) => return outcome,
        };
        // A commit that writes is a write to a fenced partition (D317 ticket 6); a
        // read-only one is a consistent read. Refused, the transaction stays open.
        let held = match &self.gate {
            Some(gate) if g.wrote() || !rows.is_empty() => Some(gate.enter()?),
            _ => None,
        };
        // D331: the rows it carries, as writes, and M3, a transaction open across a range's
        // fence or hand-over, as simp724 agreed it ([`super::super::ranges`]); one load.
        if let Some(gate) = &self.gate
            && let Some(ranges) = &*gate.ranges().load()
        {
            ranges.commit(
                &r.rows,
                g.txn().snapshot_seqno(),
                g.wrote() || !rows.is_empty(),
            )?;
        }
        for (table, row, ops) in &rows {
            convert::txn_apply(g.txn(), table, row, ops);
        }
        drop(rows);
        let (txn, durability, ticket) = g.commit();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let metrics = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            let commit = match durability {
                Some(d) => txn.commit_with_async(d),
                None => txn.commit_async(),
            };
            // Its own task, so a panic still records an outcome (unknown), never nothing.
            let outcome = match tokio::spawn(commit).await {
                Ok(r) => r.map_err(ApiError::from),
                Err(e) => Err(ApiError::outcome_unknown(format!(
                    "the commit's task failed: {e}"
                ))),
            };
            // A commit that returned past the partition's lease is unknown (D326), and a
            // retry of it finds the same.
            let outcome = match (outcome, &held) {
                (Ok(c), Some(h)) => h.committed().map(|()| c),
                (o, _) => o,
            };
            ticket.finish(outcome.clone().map(|c| proto::TxnCommitResponse {
                commit: Some(convert::commit_info(c)),
                parts: Vec::new(),
            }));
            drop(budget);
            drop(held);
            if tx.send(outcome).is_err() {
                metrics.replies_lost.fetch_add(1, Ordering::Relaxed);
            }
        });
        let outcome = rx.await.unwrap_or_else(|_| {
            Err(ApiError::outcome_unknown(
                "the commit's task ended without an outcome",
            ))
        })?;
        Ok(proto::TxnCommitResponse {
            commit: Some(convert::commit_info(outcome)),
            parts: Vec::new(),
        })
    }

    pub(super) fn abort(
        &self,
        r: &proto::TxnAbortRequest,
        caller: &Caller,
    ) -> Result<proto::Empty, ApiError> {
        self.open_txn(&r.txn_id, caller)?.abort();
        Ok(proto::Empty {})
    }

    pub(super) fn open_snapshot(
        &self,
        r: &proto::OpenSnapshotRequest,
        caller: &Caller,
    ) -> Result<proto::OpenSnapshotResponse, ApiError> {
        let snapshot = if r.durable {
            self.part()?.db.durable_snapshot()?
        } else {
            self.part()?.db.snapshot()?
        };
        let seqno = snapshot.seqno();
        let snapshot_id = self.sessions.open_snapshot(caller.principal, snapshot)?;
        Ok(proto::OpenSnapshotResponse { snapshot_id, seqno })
    }
}
