use super::*;

/// A recall's step's recall, decoded (#1915).
fn step_recall(r: Option<proto::RecallRequest>) -> Result<crate::memcore::RecallArgs, ApiError> {
    r.ok_or_else(|| ApiError::invalid("a recall's step carries no recall"))
        .and_then(super::memory::recall)
}

impl Service {
    /// The memory store, opened on the blocking pool if the slot holds none (#1618), or
    /// `Unsupported` without `--memory`. On a replica without the store: `ReplicaBehind`
    /// while its copy is still arriving, else `Unavailable` (the primary hasn't created it,
    /// or the replica hasn't applied that yet; ICR 0112).
    async fn memory_core(&self) -> Result<Arc<crate::memcore::MemCore>, ApiError> {
        let slot = self
            .part()?
            .memory
            .get()
            .cloned()
            .ok_or_else(|| self.no_memory())?;
        let entry = match slot.cached() {
            Some(entry) => entry,
            None => {
                // Under the memory sub-limit (M6): a call waiting for another's open waits on
                // the semaphore, not in a blocking-pool thread (cli692's R2 on #1639).
                let slot = Arc::clone(&slot);
                self.memory_blocking(move || slot.get()).await?
            }
        };
        self.entry_core(&slot, entry)
    }

    /// The core a slot's entry holds, or why there's none.
    fn entry_core(
        &self,
        slot: &super::memslot::MemSlot,
        entry: super::memslot::Entry,
    ) -> Result<Arc<crate::memcore::MemCore>, ApiError> {
        use super::memslot::Entry;
        match entry {
            Entry::Core(core) => Ok(core),
            Entry::Failed(e) => Err(e),
            Entry::Missing => {
                let name = slot.name();
                match self.replica.as_ref().and_then(|r| r.settling()) {
                    Some(state) => Err(ApiError::replica_behind(format!(
                        "this replica is {state}: memory store {name} isn't on it yet"
                    ))),
                    None => Err(ApiError::unavailable(format!(
                        "memory store {name} isn't on this replica yet: the primary hasn't \
                         created it, or the replica hasn't applied that yet"
                    ))),
                }
            }
        }
    }

    /// `f` on the blocking pool under the memory sub-limit (M6). The wait for a memory slot
    /// counts against the request timeout.
    async fn memory_blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
    ) -> Result<T, ApiError> {
        let slot = self.memory_place().await?;
        let r = self.blocking(f).await;
        drop(slot);
        r
    }

    /// A place under the memory sub-limit (M6), waited for within the request timeout. Taken
    /// before a blocking-pool place, always, so the two are never waited for in the other
    /// order. A `/mcp` memory tool's op takes it as the RPC does (#1915 PR 3b): the message
    /// holds only its `/mcp` place.
    pub(crate) async fn memory_place(&self) -> Result<OwnedSemaphorePermit, ApiError> {
        budget_within(&self.memory_calls, 1, self.limits.request_timeout)
            .await
            .map_err(|_| ApiError::overloaded("the server's memory calls are all busy"))
    }

    /// A memory write: detached (its real outcome even if the client goes away), on the
    /// blocking pool under the memory sub-limit, with the caller's namespaces.
    async fn memory_write<T: Send + std::fmt::Debug + 'static>(
        &self,
        rpc: &'static str,
        caller: &Caller,
        budget: OwnedSemaphorePermit,
        f: impl FnOnce(&crate::memcore::MemCore, crate::memcore::Scope<'_>) -> Result<T, ApiError>
        + Send
        + 'static,
    ) -> Result<T, ApiError> {
        let core = self.memory_core().await?;
        let namespaces = caller.namespaces.clone();
        let (calls, pool, wait) = (
            Arc::clone(&self.memory_calls),
            Arc::clone(&self.blocking),
            self.limits.request_timeout,
        );
        self.detached(rpc, budget, async move {
            let _slot = budget_within(&calls, 1, wait)
                .await
                .map_err(|_| ApiError::overloaded("the server's memory calls are all busy"))?;
            blocking(&pool, move || {
                f(&core, crate::memcore::Scope(namespaces.as_deref()))
            })
            .await
        })
        .await
    }

    /// `Memory.Remember`: a memory in its namespace, embedded by the server if it has an
    /// embedder. Detached, as every write.
    async fn remember(
        &self,
        r: proto::RememberRequest,
        caller: &Caller,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::RememberResponse, ApiError> {
        let (m, supersedes) = super::memory::remember(r)?;
        self.memory_write("Memory.Remember", caller, budget, move |core, scope| {
            let (done, supersede_error) = core.remember(scope, m, supersedes)?;
            Ok(super::memory::remembered(&done, supersede_error))
        })
        .await
    }

    /// `Memory.Recall`: the best memories for a query, within a byte budget (the server's
    /// default when the request sets none, so the reply is bounded).
    async fn recall(
        &self,
        r: proto::RecallRequest,
        caller: &Caller,
    ) -> Result<(proto::RecallResponse, Option<super::charge::Charge>), ApiError> {
        let core = self.memory_core().await?;
        // A budget above the default page reserves its share before a memory-call slot
        // (cli692's points on #1567): a recall waiting for budget doesn't hold one. A smaller
        // one is charged once built.
        let cap = usize::try_from(core.reply_cap(r.byte_budget)).unwrap_or(usize::MAX);
        let charge = if cap > FAST_CAP {
            Some(self.reserve_reply(caller, cap).await?)
        } else {
            None
        };
        let a = super::memory::recall(r)?;
        let namespaces = caller.namespaces.clone();
        let reply = self
            .memory_blocking(move || {
                let (hits, stats) = core.recall(crate::memcore::Scope(namespaces.as_deref()), a)?;
                Ok(proto::RecallResponse {
                    hits: hits.into_iter().map(super::memory::hit).collect(),
                    truncated: stats.truncated,
                })
            })
            .await?;
        Ok((reply, charge))
    }

    /// A step of a recall whose namespaces are on several partitions, on this one (#1915,
    /// ICR 0139): its pool, or a hop's part, as versioned bytes. The caller's namespaces are
    /// checked here again. A hop in a version this node doesn't read is
    /// `FailedPrecondition`: the nodes' builds differ.
    async fn recall_pool(
        &self,
        r: proto::RecallPoolRequest,
        caller: &Caller,
    ) -> Result<ServiceReply, ApiError> {
        let core = self.memory_core().await?;
        let namespaces = caller.namespaces.clone();
        if r.now_us == 0 {
            return Err(ApiError::invalid("a recall's step carries no clock"));
        }
        let hop = if r.hop.is_empty() {
            None
        } else {
            Some(
                pigeonhole_memory::RecallHop::from_bytes(&r.hop).map_err(|e| {
                    ApiError::failed_precondition(format!(
                        "a recall's hop from another node that this node can't read: {e}"
                    ))
                })?,
            )
        };
        let a = step_recall(r.request)?;
        let now = r.now_us;
        let pool = self
            .memory_blocking(move || {
                let scope = crate::memcore::Scope(namespaces.as_deref());
                Ok(core.recall_pool(scope, a, now, hop.as_ref())?)
            })
            .await?;
        Ok(ServiceReply::RecallPool(proto::RecallPoolResponse {
            pool: pool.into(),
        }))
    }

    /// A step of a recall whose namespaces are on several partitions, on this one (#1915,
    /// ICR 0139): its hits' bodies, as versioned bytes. The caller's namespaces are checked
    /// here again.
    async fn recall_bodies(
        &self,
        r: proto::RecallBodiesRequest,
        caller: &Caller,
    ) -> Result<ServiceReply, ApiError> {
        let core = self.memory_core().await?;
        let namespaces = caller.namespaces.clone();
        let ids = super::memory::ids(&r.ids)?;
        let a = step_recall(r.request)?;
        let bodies = self
            .memory_blocking(move || {
                let scope = crate::memcore::Scope(namespaces.as_deref());
                Ok(core.recall_bodies(scope, a, &ids)?)
            })
            .await?;
        Ok(ServiceReply::RecallBodies(proto::RecallBodiesResponse {
            bodies: bodies.into(),
        }))
    }

    /// `Memory.Watch`: new memories in some namespaces, under a `--max-streams` slot. On a
    /// cluster partition, a cluster watch's component ([`super::cluster::memwatch`]).
    pub(crate) async fn memory_watch(
        &self,
        r: proto::MemoryWatchRequest,
        caller: &Caller,
    ) -> Result<super::memory::MemoryEvents, ApiError> {
        // Before the store: a swap from here on ends the watch, and on a cluster partition
        // so does a close or a move (#1915 PR 2b).
        let part = self.part()?;
        let swaps = part.memory.get().map(|m| m.swaps());
        let cluster = self.gate.as_ref().map(|gate| super::memory::WatchPart {
            gate: Arc::clone(gate),
            ended: part.feeds.parts_ended(),
        });
        let core = self.memory_core().await?;
        let Some(swaps) = swaps else {
            return Err(self.no_memory());
        };
        let slot = self.stream_slot()?;
        super::memory::watch(
            core,
            crate::memcore::Scope(caller.namespaces.as_deref()),
            r,
            slot,
            Arc::clone(&self.blocking),
            self.limits.stream_idle,
            swaps,
            cluster,
        )
    }
}
