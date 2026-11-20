use super::*;

/// Runs a sync-only call on the blocking pool, holding one of `pool`'s permits.
pub(crate) async fn blocking<T: Send + 'static>(
    pool: &Arc<Semaphore>,
    f: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
) -> Result<T, ApiError> {
    held(pool_place(pool).await?, f).await
}

/// One of `pool`'s permits: a place on the blocking pool.
async fn pool_place(pool: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit, ApiError> {
    Arc::clone(pool)
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal("the blocking pool closed"))
}

/// Runs a sync-only call on the blocking pool, in the place `_place` holds.
async fn held<T: Send + 'static>(
    _place: OwnedSemaphorePermit,
    f: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
) -> Result<T, ApiError> {
    // A sampled call's work is timed where it runs (D332, simp724's M1 on #2063).
    let sampled = super::cluster::load::sampled();
    tokio::task::spawn_blocking(move || {
        // A reused pool thread takes a mask published since it started (D317 ticket 6).
        super::placement::refresh();
        super::cluster::load::run_sampled(sampled.as_ref(), f)
    })
    .await
    .map_err(|e| ApiError::internal(format!("a blocking call failed: {e}")))?
}

impl Service {
    /// Runs a `/mcp` message on a blocking thread under a `--mcp-blocking` place, never a
    /// `--blocking-threads` permit: its tools' calls are ops through the router, and each
    /// takes a permit of its own (ICR 0139's PR 3 section). The conditions this rests on
    /// (simp724's review of #1915 PR 3):
    /// - (a) the place is awaited here, on the async side, before the thread;
    /// - (b) only `/mcp` threads wait on blocking work: an op's own blocking work never
    ///   waits on another's;
    /// - (c) every other wait on the dispatch path is a try or bounded (`--max-concurrent`
    ///   sheds, a forward to a full node answers `Overloaded`, the memory place waits
    ///   within the request timeout, the stream and scan places are tries; a charged reply
    ///   a tool still holds keeps its in-flight bytes while the tool makes its next call,
    ///   whose `budget_within` is bounded);
    /// - (d) tokio's blocking-thread cap leaves room for the ops beside the messages
    ///   (`--blocking-threads` + 16 + `--mcp-blocking`, `serve_shared`).
    ///
    /// Waits for a place until `until`, then `Overloaded`.
    pub(crate) async fn mcp_blocking<T: Send + 'static>(
        &self,
        until: tokio::time::Instant,
        f: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
    ) -> Result<T, ApiError> {
        let place = tokio::time::timeout_at(until, Arc::clone(&self.mcp_places).acquire_owned())
            .await
            .map_err(|_| {
                ApiError::overloaded("the server is at its `/mcp` message limit (--mcp-blocking)")
            })?
            .map_err(|_| ApiError::internal("the `/mcp` places closed"))?;
        held(place, f).await
    }

    /// Runs a sync-only call on the blocking pool, at most `--blocking-threads` at once.
    pub(crate) async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
    ) -> Result<T, ApiError> {
        blocking(&self.blocking, f).await
    }

    /// Waits up to `timeout` for the light pool's running calls (shutdown, before the
    /// databases close; simp724's S2 on #1972); whether they finished.
    pub(crate) fn drain_lite(&self, timeout: Duration) -> bool {
        self.lite.drain(timeout)
    }

    /// A `--max-streams` place, now or `Overloaded`: a feed or watch stream's, or a `/mcp`
    /// `feed_poll`'s for its wait.
    pub(crate) fn stream_slot(&self) -> Result<OwnedSemaphorePermit, ApiError> {
        Arc::clone(&self.streams).try_acquire_owned().map_err(|_| {
            ApiError::overloaded("the server is at its concurrent stream limit (--max-streams)")
        })
    }

    /// Runs a write as its own task and awaits its outcome (B2). If the request goes away
    /// first, the task carries on, and its outcome is logged and counted.
    async fn detached<T: Send + std::fmt::Debug + 'static>(
        &self,
        rpc: &'static str,
        budget: OwnedSemaphorePermit,
        write: impl Future<Output = Result<T, ApiError>> + Send + 'static,
    ) -> Result<T, ApiError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let metrics = Arc::clone(&self.metrics);
        // The fence's drain waits for the write, not just its request (`call` admitted it).
        let held = self.gate.as_ref().map(super::gate::WriteGate::extend);
        tokio::spawn(async move {
            let outcome = write.await;
            // The write is done: its bytes leave the budget now, not when its request did.
            drop(budget);
            drop(held);
            if let Err(lost) = tx.send(outcome) {
                metrics.replies_lost.fetch_add(1, Ordering::Relaxed);
                match lost {
                    Ok(v) => eprintln!(
                        "phdb serve: {rpc}'s client went away; the write committed: {v:?}"
                    ),
                    Err(e) => eprintln!(
                        "phdb serve: {rpc}'s client went away; the write failed: {} ({})",
                        e.code, e.message
                    ),
                }
            }
        });
        rx.await.unwrap_or_else(|_| {
            Err(ApiError::internal(
                "a write's task ended without an outcome",
            ))
        })
    }
}
