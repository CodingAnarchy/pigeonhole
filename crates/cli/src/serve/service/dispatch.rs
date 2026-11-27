use super::*;

impl Service {
    /// Answers `op` for `caller`: the scope check, the in-flight bytes budget (unless the
    /// front-end already reserved it, `prepaid`), then the call. A replica refuses writes
    /// with the primary's URL in the error.
    pub(crate) async fn call(
        &self,
        mut op: ServiceOp,
        caller: &Caller,
        prepaid: Option<OwnedSemaphorePermit>,
    ) -> Result<ServiceReply, ApiError> {
        // A mask a partition's open or close published (D317 ticket 6), taken by a busy
        // worker that hasn't parked since.
        super::super::placement::refresh();
        let budget = self.admit(&op.admit(), caller, prepaid).await?;
        // A cluster partition's gate (D317 ticket 6, D326), once admitted, so a fence's
        // drain never waits behind a write's wait for the budget. A write is held until the
        // call answers (a write that runs on past its request extends it); a read needs the
        // partition's lease. Each is checked again once it's done: a write whose commit
        // returned past the lease's end is `OutcomeUnknown`, and a read whose snapshot may
        // have been taken past it is refused (audit120's S3).
        let (held, read) = match &self.gate {
            Some(gate) if op.writes() => (Some(gate.enter()?), None),
            Some(gate) if op.reads_partition() => {
                gate.serving()?;
                (None, Some(gate))
            }
            _ => (None, None),
        };
        // D335: a write on a partition whose map sets `replica_acks` waits for that many
        // replicas, unless it opted down (the request's `replication`, or the header). It's
        // refused at admission while too few are healthy (REFUSE), and its durability is
        // raised to `GroupSync` (the feed ships only what's durable on the owner). The
        // map's k decides, never whether a stream is attached now (simp724 on #2236). A
        // transaction's `Commit` waits where it's answered (`commit`), before its outcome
        // is recorded.
        let waits = match (&held, &self.gate, &self.part) {
            (Some(_), Some(gate), Some(part))
                if part.acks.k() > 0 && !caller.replication_async && !op.replication_async() =>
            {
                part.acks.admit(gate.partition())?;
                op.raise_durability();
                true
            }
            _ => false,
        };
        // D331: a range a move fences, moved away or doesn't serve here yet. One load while
        // no move names the partition. A transaction's `Commit` is checked where it's
        // answered, its rows as writes, with its snapshot (`commit`).
        let mut touched = None;
        if let Some(gate) = &self.gate
            && !matches!(op, ServiceOp::Commit(_))
            && let Some(ranges) = &*gate.ranges().load()
        {
            ranges.check(&op, op.writes())?;
            // #2160: a read's keys, for the check again past its snapshot. Only while the
            // partition has range state (a move names it, or a range moved off it and its
            // rows aren't pruned yet), so other reads copy nothing. A read admitted before
            // the state reached this node isn't checked again; that needs a stall through a
            // whole move (audit120's point 3 on #2218).
            if read.is_some() {
                touched = Some(crate::serve::ranges::Touched::of(&op));
            }
        }
        self.copy_ranges(&op)?;
        // D332: a cluster partition's sampled loads. A read's first key, one read in 64 per
        // thread; a call's CPU per poll, one in 16 per thread. An unsampled call touches
        // nothing shared (perf287's must).
        let load = match (&self.gate, &self.part) {
            (Some(_), Some(part)) => Some(&part.load),
            _ => None,
        };
        if let Some(load) = load
            && op.reads_partition()
            && super::super::cluster::load::sample_read()
        {
            let _ = op.touches(&mut |table, touch| {
                // A range read's first key; one from the table's start has none to sample.
                let key = match touch {
                    Touch::Row(k) | Touch::Prefix(k) => k,
                    Touch::Range {
                        start: Bound::Included(k) | Bound::Excluded(k),
                        ..
                    } => k,
                    Touch::Range {
                        start: Bound::Unbounded,
                        ..
                    } => return ControlFlow::Continue(()),
                };
                load.add_read(table, key);
                ControlFlow::Break(())
            });
        }
        let mut reply = match load {
            Some(load) if super::super::cluster::load::sample_cpu() => {
                super::super::cluster::load::sampled_call(
                    Arc::clone(load),
                    self.answer(op, caller, budget),
                )
                .await
            }
            _ => self.answer(op, caller, budget).await,
        };
        // D335: the wait, after the commit and before D326's check. Boxed, and the reply
        // moved into it and back, so nothing of this call lives across its await: a reply
        // held there would take a slot of its own in every call's future (+304 bytes, the
        // serve callgrind shapes, D193). Only a write that waits pays for it.
        if waits {
            reply = Box::pin(self.wait_for_acks(reply)).await;
        }
        if reply.is_ok() {
            if let Some(h) = &held
                && let Err(e) = h.committed()
            {
                reply = Err(e);
            } else if held.is_some()
                && let (Some(part), Ok(r)) = (&self.part, &reply)
            {
                // D335: timed until the frontier covers it (one load with no stream).
                part.acks.committed(|| match committed_seqno(r) {
                    Committed::At(s) => s,
                    _ => part.db.visible_seqno(),
                });
            } else if let Some(gate) = read
                && let Err(e) = gate.serving()
            {
                reply = Err(e);
            } else if let (Some(gate), Some(touched)) = (read, &touched)
                && let Some(ranges) = &*gate.ranges().load()
                && let Err(e) = ranges.check_touched(touched)
            {
                // #2160: the range check again, past the snapshot. A move only takes a
                // range away from here, so one still served now was served when the read
                // took its snapshot; one handed over since may have taken it after the
                // target began to serve, so it's refused (retry-safe, at the new owner).
                reply = Err(e);
            }
        }
        drop(held);
        match (reply, self.replica.as_ref().filter(|r| r.refuses_writes())) {
            // A transaction's Commit with writes: the engine's gate (R6) refused it.
            (Err(e), Some(replica)) if e.code == "ReadOnly" => {
                Err(ApiError::replica_read_only(&replica.primary()))
            }
            // A cluster partition closed under the call (D317 ticket 6's close, S5): the
            // partition is unavailable here, and the client routes again.
            (Err(e), _)
                if e.code == "Closed"
                    && let Some(gate) = &self.gate =>
            {
                Err(ApiError::partition_unavailable(format!(
                    "partition {:?} closed on this node while the call ran: {}",
                    gate.partition(),
                    e.message
                )))
            }
            (reply, _) => reply,
        }
    }

    /// The partition's write gate, on a cluster partition's service.
    pub(crate) fn gate(&self) -> Option<&Arc<super::super::gate::WriteGate>> {
        self.gate.as_ref()
    }

    /// Every call's rules, before it runs, in one place (D313 §7 as amended by ticket 8c):
    /// gRPC and HTTP calls come through [`call`](Self::call), and `/mcp`'s tools call it
    /// directly. In order:
    /// - **a token limited to namespaces:** `PermissionDenied` for anything but a memory call
    ///   (audit120's F5);
    /// - **a replica's write refusal:** a write a caller may make is refused with `ReadOnly`
    ///   naming the primary, rather than a permission error;
    /// - **the caller's scope:** `PermissionDenied` below it;
    /// - **the request's size:** `--max-request-bytes`;
    /// - **the in-flight budget** (R3): `size` bytes, held by the returned permit until the
    ///   reply is built. Waiting for it counts against the request's deadline; nothing has
    ///   been submitted while it waits. Both front-ends reserve it before reading the
    ///   message (perf287's F2 on #1501): HTTP from `Content-Length`, gRPC from the message
    ///   header (`grpc::prepay`). It's taken here only for a call that comes without one.
    ///   Reads hold it until their reply is built; a write hands it to its detached task,
    ///   so a client that goes away doesn't free bytes the write still holds (simp724's S1
    ///   on #1501).
    pub(crate) async fn admit(
        &self,
        call: &Admit,
        caller: &Caller,
        prepaid: Option<OwnedSemaphorePermit>,
    ) -> Result<OwnedSemaphorePermit, ApiError> {
        self.check(call, caller)?;
        match prepaid {
            Some(p) => Ok(p),
            None => self.reserve(call.size).await,
        }
    }

    /// [`admit`](Self::admit)'s checks without its reservation: a `/mcp` tool call, whose
    /// request's bytes are its POST's, already reserved (ai1249's S1 on #1666).
    pub(crate) fn check(&self, call: &Admit, caller: &Caller) -> Result<(), ApiError> {
        if caller.memory_only() && !call.memory {
            return Err(namespaced_denied(&format!("call {}", call.what)));
        }
        if let Some(replica) = self.replica.as_ref().filter(|r| r.refuses_writes())
            && call.writes
            && caller.scope.allows(call.scope)
        {
            return Err(ApiError::replica_read_only(&replica.primary()));
        }
        let peer = call.peer && caller.scope == Scope::Cluster;
        if !caller.scope.allows(call.scope) && !peer {
            return Err(ApiError::permission_denied(&format!("call {}", call.what)));
        }
        if call.size > self.limits.max_request_bytes {
            return Err(ApiError::request_too_large(self.limits.max_request_bytes));
        }
        Ok(())
    }

    async fn answer(
        &self,
        op: ServiceOp,
        caller: &Caller,
        budget: OwnedSemaphorePermit,
    ) -> Result<ServiceReply, ApiError> {
        match op {
            // A read that fails on `Io` changed nothing, so it may be sent
            // again (`retry_from` REQUEST, ICR 0112).
            // The unary reads charge their replies to the budget (#1567, revised after g46):
            // see `read_charged`. The request's share goes first, so a read never holds it
            // while it waits for its reply's.
            ServiceOp::Get(r) => {
                drop(budget);
                // One cell, with no cap to reserve against: charged after the read, waiting
                // for its size if the budget is full (never read twice).
                let reply = self
                    .get(r, caller)
                    .await
                    .map(ServiceReply::Get)
                    .map_err(ApiError::for_read)?;
                self.charge_built(caller, reply).await
            }
            ServiceOp::MultiGet(r) => {
                drop(budget);
                let cap = cap(r.page_bytes);
                self.read_charged(caller, cap, r, |r| async move {
                    self.multi_get(r, caller)
                        .await
                        .map(|(reply, r)| (ServiceReply::MultiGet(reply), r))
                })
                .await
            }
            ServiceOp::ReadRow(r) => {
                drop(budget);
                let cap = cap(r.page_bytes);
                self.read_charged(caller, cap, r, |r| async move {
                    self.read_row(r, caller)
                        .await
                        .map(|(reply, r)| (ServiceReply::ReadRow(reply), r))
                })
                .await
            }
            ServiceOp::Scan(r) => self
                .scan(r, caller, budget)
                .await
                .map(ServiceReply::Scan)
                .map_err(ApiError::for_read),
            ServiceOp::Mutate(r) => self.mutate(r, budget).await.map(ServiceReply::Mutate),
            ServiceOp::CheckAndMutate(r) => self
                .check_and_mutate(r, budget)
                .await
                .map(ServiceReply::CheckAndMutate),
            ServiceOp::Batch(r) => self.batch(r, budget).await.map(ServiceReply::Mutate),
            ServiceOp::MutateReturning(r) => self
                .mutate_returning(r, budget)
                .await
                .map(ServiceReply::MutateReturning),
            ServiceOp::Begin(r) => {
                drop(budget);
                self.begin(r, caller).map(ServiceReply::Begin)
            }
            ServiceOp::TxnGet(r) => {
                let reply = self.txn_get(r, caller).await.map(ServiceReply::Get);
                drop(budget);
                reply.map_err(ApiError::for_read)
            }
            ServiceOp::TxnGetRow(r) => {
                let reply = self.txn_get_row(r, caller).await.map(ServiceReply::ReadRow);
                drop(budget);
                reply.map_err(ApiError::for_read)
            }
            ServiceOp::TxnWrite(r) => {
                let reply = self.txn_write(r, caller).await.map(ServiceReply::Empty);
                drop(budget);
                reply
            }
            ServiceOp::Commit(r) => self
                .commit(r, caller, budget)
                .await
                .map(ServiceReply::Commit),
            ServiceOp::Abort(r) => {
                drop(budget);
                self.abort(&r, caller).map(ServiceReply::Empty)
            }
            // A cluster's home answers it through `spanning::settle`, before here; this is a
            // single node's: a session's recorded outcome, while it lasts (D329 2b).
            ServiceOp::Outcome(r) => {
                drop(budget);
                use super::super::sessions::Peek;
                use proto::TxnOutcomeState as S;
                let answer = |state: S, reason: &str| proto::TxnOutcomeResponse {
                    state: state as i32,
                    reason: reason.to_owned(),
                    ..Default::default()
                };
                match self.peek_txn(&r.txn_id, Some(caller.principal)) {
                    Peek::Live => Ok(answer(
                        S::InProgress,
                        "the transaction is open or committing",
                    )),
                    Peek::Done(Ok(c)) => Ok(proto::TxnOutcomeResponse {
                        state: S::Committed as i32,
                        commit: c.commit,
                        parts: c.parts,
                        reason: String::new(),
                    }),
                    Peek::Done(Err(e)) => Ok(answer(
                        if e.code == "OutcomeUnknown" {
                            S::Unknown
                        } else {
                            S::Aborted
                        },
                        &e.message,
                    )),
                    Peek::Gone | Peek::Foreign => Err(ApiError::not_found(
                        "no such transaction (its outcome is kept only while its session's \
                         record is)",
                    )),
                }
                .map(ServiceReply::Outcome)
            }
            ServiceOp::OpenSnapshot(r) => {
                drop(budget);
                self.open_snapshot(&r, caller)
                    .map(ServiceReply::OpenSnapshot)
            }
            ServiceOp::ListTables(_) => {
                drop(budget);
                let tables = self.user_tables();
                Ok(ServiceReply::Tables(proto::ListTablesResponse { tables }))
            }
            ServiceOp::CreateTable(r) => {
                let reply = self.create_table(r).await.map(ServiceReply::Table);
                drop(budget);
                reply
            }
            ServiceOp::DropTable(r) => {
                drop(budget);
                self.drop_table(r.name)
                    .await
                    .map(|()| ServiceReply::Empty(proto::Empty {}))
            }
            ServiceOp::Info(_) => {
                drop(budget);
                let db = self.part()?.db.clone();
                let info = self.blocking(move || Ok(db.info()?)).await?;
                let tables = self.user_tables();
                Ok(ServiceReply::Info(proto::InfoResponse {
                    path: self.part()?.db.path().display().to_string(),
                    file_bytes: info.file_bytes,
                    free_bytes: info.free_bytes.unwrap_or(0),
                    tables,
                    version: env!("CARGO_PKG_VERSION").to_owned(),
                    replica: self.replica_info(),
                    partitions: Vec::new(),
                    other_partitions: Vec::new(),
                }))
            }
            // On a cluster node the router serves these across its partitions (#1852): one
            // that reaches here is a single node's, or a partition's own.
            ServiceOp::Flush(r) => {
                drop(budget);
                if !r.partition.is_empty() {
                    return Err(single_node_maintenance());
                }
                self.part()?.db.flush_async().await?;
                Ok(ServiceReply::Empty(proto::Empty {}))
            }
            ServiceOp::Compact(r) => {
                drop(budget);
                if !r.partition.is_empty() {
                    return Err(single_node_maintenance());
                }
                self.part()?.db.compact_async().await?;
                Ok(ServiceReply::Empty(proto::Empty {}))
            }
            ServiceOp::Backup(r) => {
                drop(budget);
                if !r.partition.is_empty() {
                    return Err(single_node_partition());
                }
                self.backup(self.part()?, r).await.map(ServiceReply::Backup)
            }
            ServiceOp::BackupDownload(r) => {
                drop(budget);
                if !r.partition.is_empty() {
                    return Err(single_node_partition());
                }
                if r.fenced_file.is_some() {
                    return Err(ApiError::invalid(
                        "fenced_file is a cluster node's (D317 ticket 6): a single node's \
                         download is a backup",
                    ));
                }
                self.backup_download(self.part()?, r, None)
                    .await
                    .map(ServiceReply::Download)
            }
            ServiceOp::Promote(_) => {
                drop(budget);
                let Some(replica) = &self.replica else {
                    return Err(ApiError::failed_precondition(
                        "this server isn't a replica (--replica-of)",
                    ));
                };
                let primary_token = replica.promote(&self.part()?.db).await?;
                // A primary now: background indexes rebalance again, from handles opened
                // afresh (cli692's F3 on #1583).
                self.part()?.indexes.forget();
                self.part()?.indexes.pause_rebalancing(false);
                // And a text index rebuild stopped on the primary, then copied, resumes here.
                self.resume_text_rebuilds();
                // The memory store opens as the writer now, at once, created if missing
                // (cli692's D3 on #1618). Its failure doesn't fail the promote (the database
                // is what's promoted): it's the replica's last error, and memory calls answer
                // it until the next clear.
                if let Some(memory) = self.part()?.memory.get() {
                    memory.promote();
                    let slot = Arc::clone(memory);
                    let opened = self.blocking(move || slot.get()).await;
                    let failed = match opened {
                        Ok(super::super::memslot::Entry::Failed(e)) | Err(e) => Some(e.message),
                        Ok(_) => None,
                    };
                    if let Some(e) = failed {
                        eprintln!(
                            "phdb serve: after the promote, memory store {}: {e}",
                            memory.name()
                        );
                        replica.note_error(format!("memory store {}: {e}", memory.name()));
                    }
                }
                Ok(ServiceReply::Promote(proto::PromoteResponse {
                    primary_token: primary_token.into(),
                }))
            }
            ServiceOp::Follow(r) => {
                drop(budget);
                self.follow(r).map(ServiceReply::Follow)
            }
            ServiceOp::Metrics(_) => {
                drop(budget);
                // A single node: a cluster node answers it in the router (D317 ticket 8b).
                Ok(ServiceReply::Metrics(proto::MetricsResponse {
                    text: self.metrics_text(&self.single_parts(), &[&self.sessions]),
                }))
            }
            ServiceOp::DescribeTable(r) => {
                drop(budget);
                let table = self.table(&r.name).await?;
                Ok(ServiceReply::Table(describe(&table)))
            }
            ServiceOp::Traverse(r) => {
                drop(budget);
                let cap = cap(r.page_bytes);
                self.read_charged(caller, cap, r, |r| async move {
                    let reply = self.traverse(&r, caller).await?;
                    Ok((ServiceReply::Traverse(reply), r))
                })
                .await
            }
            ServiceOp::Nearest(r) => {
                drop(budget);
                let cap = cap(r.page_bytes);
                self.read_charged(caller, cap, r, |r| async move {
                    let reply = self.nearest(&r, caller).await?;
                    Ok((ServiceReply::Nearest(reply), r))
                })
                .await
            }
            ServiceOp::CreateIndex(r) => {
                drop(budget);
                self.create_index(r).await.map(ServiceReply::Empty)
            }
            ServiceOp::VectorInsert(r) => {
                self.vector_insert(r, budget).await.map(ServiceReply::Empty)
            }
            ServiceOp::VectorDelete(r) => {
                self.vector_delete(r, budget).await.map(ServiceReply::Empty)
            }
            ServiceOp::VectorSearch(r) => {
                drop(budget);
                self.vector_search(r)
                    .await
                    .map(ServiceReply::VectorSearch)
                    .map_err(ApiError::for_read)
            }
            ServiceOp::VectorVerify(r) => {
                drop(budget);
                self.vector_verify(r)
                    .await
                    .map(ServiceReply::VectorVerify)
                    .map_err(ApiError::for_read)
            }
            ServiceOp::TextCreateIndex(r) => {
                drop(budget);
                self.text_create_index(r).await.map(ServiceReply::Empty)
            }
            ServiceOp::TextPut(r) => self.text_put(r, budget).await.map(ServiceReply::Empty),
            ServiceOp::TextDelete(r) => self.text_delete(r, budget).await.map(ServiceReply::Empty),
            ServiceOp::TextSearch(r) => {
                drop(budget);
                self.text_search(r)
                    .await
                    .map(ServiceReply::TextSearch)
                    .map_err(ApiError::for_read)
            }
            // Boxed: its slot wait and the rebuild's state would otherwise widen the future
            // every call moves (serve-mutate's ceiling on #1975).
            ServiceOp::TextRebuild(r) => {
                drop(budget);
                Box::pin(self.text_rebuild(r))
                    .await
                    .map(ServiceReply::TextRebuild)
            }
            ServiceOp::TextVerify(r) => {
                drop(budget);
                self.text_verify(r)
                    .await
                    .map(ServiceReply::TextVerify)
                    .map_err(ApiError::for_read)
            }
            ServiceOp::Remember(r) => self
                .remember(r, caller, budget)
                .await
                .map(ServiceReply::Remember),
            ServiceOp::Recall(r) => {
                drop(budget);
                let (reply, charge) = self.recall(r, caller).await.map_err(ApiError::for_read)?;
                let reply = ServiceReply::Recall(reply);
                match charge {
                    Some(charge) => Ok(self.charged(caller, reply, charge)),
                    // A budget within the default page: charged once built (never run twice).
                    None => self.charge_built(caller, reply).await,
                }
            }
            ServiceOp::MarkUsed(r) => {
                let ids = super::super::memory::ids(&r.ids)?;
                self.memory_write("Memory.MarkUsed", caller, budget, move |core, scope| {
                    let m = core.mark_used(scope, &ids)?;
                    Ok(proto::MarkUsedResponse {
                        marked: m.marked as u64,
                        missing: m.missing.iter().map(|id| id.to_bytes().into()).collect(),
                    })
                })
                .await
                .map(ServiceReply::MarkUsed)
            }
            ServiceOp::Supersede(r) => {
                let (old, new) = (
                    super::super::memory::id(&r.old)?,
                    super::super::memory::id(&r.new)?,
                );
                self.memory_write("Memory.Supersede", caller, budget, move |core, scope| {
                    core.supersede(scope, &old, &new)?;
                    Ok(proto::Empty {})
                })
                .await
                .map(ServiceReply::Empty)
            }
            ServiceOp::Link(r) => {
                let (from, to) = (
                    super::super::memory::id(&r.from)?,
                    super::super::memory::id(&r.to)?,
                );
                let ty = super::super::memory::link_type(r.link_type.as_ref())?;
                let weight = r.weight;
                self.memory_write("Memory.Link", caller, budget, move |core, scope| {
                    core.link(scope, &from, &to, ty, weight)?;
                    Ok(proto::Empty {})
                })
                .await
                .map(ServiceReply::Empty)
            }
            ServiceOp::Forget(r) => {
                let ids = super::super::memory::ids(&r.ids)?;
                self.memory_write("Memory.Forget", caller, budget, move |core, scope| {
                    // Serve answers the failure; the deletes before it stand.
                    let done = core.forget(scope, &ids)?;
                    if let Some((e, _left)) = done.failed {
                        return Err(e.into());
                    }
                    Ok(proto::ForgetResponse {
                        forgotten: done.deleted,
                        missing: done.missing.iter().map(|id| id.to_bytes().into()).collect(),
                    })
                })
                .await
                .map(ServiceReply::Forget)
            }
            // Each step's request moved out of `op`, never `op` itself: an arm that uses `op`
            // whole keeps it alive across every await of this match, which grew every call's
            // future by its size (serve-get's and serve-mutate's counts on #1939).
            ServiceOp::RecallPool(r) => {
                drop(budget);
                self.recall_pool(r, caller)
                    .await
                    .map_err(ApiError::for_read)
            }
            ServiceOp::RecallBodies(r) => {
                drop(budget);
                self.recall_bodies(r, caller)
                    .await
                    .map_err(ApiError::for_read)
            }
            ServiceOp::TextStats(r) => {
                drop(budget);
                self.text_stats(r)
                    .await
                    .map(ServiceReply::TextStats)
                    .map_err(ApiError::for_read)
            }
            ServiceOp::TextSearchPart(r) => {
                drop(budget);
                self.text_search_part(r)
                    .await
                    .map(ServiceReply::TextSearch)
                    .map_err(ApiError::for_read)
            }
            ServiceOp::MemoryWatch(r) => {
                drop(budget);
                self.memory_watch(r, caller)
                    .await
                    .map(ServiceReply::MemoryWatch)
                    .map_err(ApiError::for_read)
            }
            ServiceOp::VectorBulkLoad(op) => {
                drop(budget);
                let index = self.index(&op.index).await?;
                let budget = super::super::bulk::Budget {
                    pool: &self.bulk_budget,
                    limit: self.limits.bulk_load_bytes as u64,
                };
                self.part()?
                    .bulk
                    .run(index, op, self.gate.as_ref(), budget)
                    .await
                    .map(|()| ServiceReply::Empty(proto::Empty {}))
            }
            ServiceOp::Subscribe(r) => self
                .subscribe(r, caller, budget)
                .await
                .map(ServiceReply::Feed)
                .map_err(ApiError::for_read),
            ServiceOp::ReleaseSnapshot(r) => {
                drop(budget);
                self.sessions
                    .release(&r.snapshot_id, caller.principal)
                    .map(|()| ServiceReply::Empty(proto::Empty {}))
            }
        }
    }
}

impl Service {
    /// D335: waits until k replicas acknowledged `reply`'s commit (its own position), the
    /// timeout, or the lease's end, and answers it with its `CommitInfo.replica_acks` set;
    /// or `OutcomeUnknown`, when the wait ends without them.
    async fn wait_for_acks(
        &self,
        mut reply: Result<ServiceReply, ApiError>,
    ) -> Result<ServiceReply, ApiError> {
        let (Some(gate), Some(part)) = (&self.gate, &self.part) else {
            return reply;
        };
        let seqno = match reply.as_ref().map(committed_seqno) {
            Ok(Committed::At(s)) => s,
            // A write whose reply names no seqno (a text, vector or memory write, a schema
            // change): the node's visible seqno after it returned, at or past its own, so
            // it may wait longer, never less.
            Ok(Committed::Unknown) => part.db.visible_seqno(),
            Ok(Committed::Nothing) | Err(_) => return reply,
        };
        match part
            .acks
            .wait(crate::serve::acks::Pos::commit(seqno), gate)
            .await
        {
            Ok(n) => {
                if let Ok(r) = &mut reply {
                    set_replica_acks(r, n);
                }
                reply
            }
            Err(e) => Err(e),
        }
    }
}

/// What a write's reply says it committed (D335): the seqno its `CommitInfo` names, none
/// to name (a conditional write that didn't apply), or a write whose reply carries none.
enum Committed {
    At(u64),
    Nothing,
    Unknown,
}

fn committed_seqno(reply: &ServiceReply) -> Committed {
    let commit = match reply {
        ServiceReply::Mutate(r) => r.commit.as_ref(),
        ServiceReply::MutateReturning(r) => r.commit.as_ref(),
        ServiceReply::CheckAndMutate(r) if !r.applied => return Committed::Nothing,
        ServiceReply::CheckAndMutate(r) => r.commit.as_ref(),
        _ => return Committed::Unknown,
    };
    commit.map_or(Committed::Unknown, |c| Committed::At(c.seqno))
}

/// Sets the reply's `CommitInfo.replica_acks` to `n`, the replicas that acknowledged it.
fn set_replica_acks(reply: &mut ServiceReply, n: u32) {
    let commit = match reply {
        ServiceReply::Mutate(r) => r.commit.as_mut(),
        ServiceReply::MutateReturning(r) => r.commit.as_mut(),
        ServiceReply::CheckAndMutate(r) => r.commit.as_mut(),
        _ => None,
    };
    if let Some(c) = commit {
        c.replica_acks = n;
    }
}
