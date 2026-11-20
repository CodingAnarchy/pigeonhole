use super::*;

impl Service {
    /// A unary read, charged (#1567, revised after g46).
    ///
    /// - **A cap within the default page ([`FAST_CAP`]):** the reply is built first, then
    ///   [`try_charge`](Self::try_charge)d. If its share isn't free now, the strict path: the
    ///   built reply goes, the cap is reserved (waiting up to the request timeout, else
    ///   `Overloaded`), and the read runs again under it. What's built unaccounted is at
    ///   most `--max-concurrent` × the default page (cli692's H1).
    /// - **A larger cap:** reserved before the read, as before g46, since its builds run on
    ///   the Tokio workers and would otherwise be bounded only by `--max-concurrent` × 8 MiB.
    ///
    /// The read takes the request and gives it back with its reply, so the fast path never
    /// copies it; only the strict path reads it a second time (perf287's g54 profile).
    async fn read_charged<R, Fut>(
        &self,
        caller: &Caller,
        cap: usize,
        mut r: R,
        read: impl Fn(R) -> Fut,
    ) -> Result<ServiceReply, ApiError>
    where
        Fut: Future<Output = Result<(ServiceReply, R), ApiError>>,
    {
        if cap <= FAST_CAP {
            let (reply, back) = read(r).await.map_err(ApiError::for_read)?;
            let (reply, settled) = self.try_charge(caller, reply);
            if settled {
                return Ok(reply);
            }
            drop(reply);
            r = back;
            self.replies.rereads.fetch_add(1, Ordering::Relaxed);
        }
        let charge = self.reserve_reply(caller, cap).await?;
        let (reply, _) = read(r).await.map_err(ApiError::for_read)?;
        Ok(self.charged(caller, reply, charge))
    }

    /// A built reply with its share: taken now if it's free, else waited for (its size,
    /// within the request timeout, else `Overloaded`). For a reply too costly to build twice
    /// (a recall) or with no cap to reserve against (`Get`).
    pub(crate) async fn charge_built(
        &self,
        caller: &Caller,
        reply: ServiceReply,
    ) -> Result<ServiceReply, ApiError> {
        let (reply, settled) = self.try_charge(caller, reply);
        if settled {
            return Ok(reply);
        }
        let charge = self
            .reserve_reply(caller, reply.encoded_len() + super::charge::GRPC_FRAME)
            .await?;
        Ok(self.charged(caller, reply, charge))
    }

    /// `reply` with its share, taken now if it's free: `(reply, true)` when it's settled
    /// (charged, or needing no charge), `(reply, false)` when its share isn't free.
    ///
    /// - **A reply within one slice takes none:** it goes to the transport whole, so its
    ///   share would be given back at the response anyway. So the hot small-read path does
    ///   no budget work (g46's readrow-small).
    /// - **HTTP/JSON's share** is twice the protobuf's, fitted to the rendered body later.
    /// - **A waiting reservation keeps its place:** Tokio's semaphore gives a queued
    ///   `acquire_many` every free permit and each release to the queue first, so this
    ///   `try_acquire` can't overtake it.
    fn try_charge(&self, caller: &Caller, reply: ServiceReply) -> (ServiceReply, bool) {
        let size = reply.encoded_len() + super::charge::GRPC_FRAME;
        let want = if caller.json {
            size.saturating_mul(2)
        } else {
            size
        };
        if want <= super::charge::PIECE {
            return (reply, true);
        }
        match super::charge::Charge::try_reserve(
            &self.inflight,
            self.limits.inflight_bytes,
            want,
            &self.replies,
        ) {
            Some(charge) => (ServiceReply::Charged(Box::new(reply), charge), true),
            None => (reply, false),
        }
    }

    /// A unary read's reply share on the strict path (#1567): `cap` bytes, twice that for
    /// an HTTP/JSON reply (cli692's D2), within the request timeout, else `Overloaded`.
    async fn reserve_reply(
        &self,
        caller: &Caller,
        cap: usize,
    ) -> Result<super::charge::Charge, ApiError> {
        let want = if caller.json {
            cap.saturating_mul(2)
        } else {
            cap
        };
        super::charge::Charge::reserve(
            &self.inflight,
            self.limits.inflight_bytes,
            want,
            self.limits.request_timeout,
            &self.replies,
        )
        .await
    }

    /// A fanned-out read's part (D317, #1715): the same `MultiGet` or `Nearest` read a call
    /// makes (its scope check, blocking-pool slot and per-call limits included), for a
    /// request whose entry node holds the budget for its page ([`Self::fanout_hold`]): no
    /// budget here, neither admission nor a reply charge. The fan-out calls it for its local
    /// part, and the router for a part a peer forwarded (`fanout_part`).
    pub(crate) async fn fanout_part(
        &self,
        op: ServiceOp,
        caller: &Caller,
    ) -> Result<ServiceReply, ApiError> {
        // A rebuild's part (#1722 PR 2c) is a write: the partition's own `Text.Rebuild`,
        // admitted, gated and fenced as one.
        if matches!(op, ServiceOp::TextRebuild(_)) {
            return Box::pin(self.call(op, caller, None)).await;
        }
        // The caller's scope, as admission checks it; the budget is the fanning-out node's.
        self.check(&op.admit(), caller)?;
        // Its partition's lease, before the read and again after it took its snapshot
        // (D326, audit120's S3); and its ranges a move gates (D331).
        if let Some(gate) = &self.gate {
            gate.serving()?;
            if let Some(ranges) = &*gate.ranges().load() {
                ranges.check(&op, false)?;
            }
        }
        self.copy_ranges(&op)?;
        let reply = self.fanout_read(op, caller).await?;
        if let Some(gate) = &self.gate {
            gate.serving()?;
        }
        Ok(reply)
    }

    /// [`fanout_part`](Self::fanout_part)'s read.
    async fn fanout_read(&self, op: ServiceOp, caller: &Caller) -> Result<ServiceReply, ApiError> {
        match op {
            ServiceOp::MultiGet(r) => self
                .multi_get(r, caller)
                .await
                .map(|(reply, _)| ServiceReply::MultiGet(reply))
                .map_err(ApiError::for_read),
            ServiceOp::Nearest(r) => self
                .nearest(&r, caller)
                .await
                .map(ServiceReply::Nearest)
                .map_err(ApiError::for_read),
            // An index's part (#1722 PR 1b): the partition's own search or check.
            ServiceOp::VectorSearch(r) => self
                .vector_search(r)
                .await
                .map(ServiceReply::VectorSearch)
                .map_err(ApiError::for_read),
            ServiceOp::VectorVerify(r) => self
                .vector_verify(r)
                .await
                .map(ServiceReply::VectorVerify)
                .map_err(ApiError::for_read),
            ServiceOp::TextVerify(r) => self
                .text_verify(r)
                .await
                .map(ServiceReply::TextVerify)
                .map_err(ApiError::for_read),
            ServiceOp::RecallPool(r) => self
                .recall_pool(r, caller)
                .await
                .map_err(ApiError::for_read),
            ServiceOp::RecallBodies(r) => self
                .recall_bodies(r, caller)
                .await
                .map_err(ApiError::for_read),
            ServiceOp::TextStats(r) => self
                .text_stats(r)
                .await
                .map(ServiceReply::TextStats)
                .map_err(ApiError::for_read),
            ServiceOp::TextSearchPart(r) => self
                .text_search_part(r)
                .await
                .map(ServiceReply::TextSearch)
                .map_err(ApiError::for_read),
            _ => Err(ApiError::internal(
                "a fan-out's local part is a multi-get, a nearest search, an index's search or \
                 check, a text search's round, or a recall's step",
            )),
        }
    }

    /// A fanned-out read's holding (#1715): `bytes` of the in-flight budget (at most the
    /// whole of it, so a lone request runs), waiting at most `wait`, else `Overloaded` as a
    /// reply's share does.
    pub(crate) async fn fanout_hold(
        &self,
        bytes: usize,
        wait: Duration,
    ) -> Result<super::charge::Charge, ApiError> {
        super::charge::Charge::reserve(
            &self.inflight,
            self.limits.inflight_bytes,
            bytes,
            wait,
            &self.replies,
        )
        .await
    }

    /// [`Self::fanout_hold`] only if the bytes are free now: a read ahead, or a further
    /// concurrent part, on spare budget.
    pub(crate) fn fanout_try_hold(&self, bytes: usize) -> Option<super::charge::Charge> {
        super::charge::Charge::try_reserve(
            &self.inflight,
            self.limits.inflight_bytes,
            bytes,
            &self.replies,
        )
    }

    /// A fanned-out read's reply, charged with its holding (#1715): fitted to the reply's
    /// share as [`Self::reserve_reply`] sizes it (twice for HTTP/JSON), so the reply's
    /// charge takes over the pages' with no gap and no second reservation. Only an
    /// HTTP/JSON share larger than the holding waits, at most `wait`.
    pub(crate) async fn fanout_reply(
        &self,
        caller: &Caller,
        reply: ServiceReply,
        mut holding: super::charge::Charge,
        wait: Duration,
    ) -> Result<ServiceReply, ApiError> {
        let size = reply.encoded_len() + super::charge::GRPC_FRAME;
        let want = if caller.json {
            size.saturating_mul(2)
        } else {
            size
        };
        holding.fit(want, self.limits.inflight_bytes, wait).await?;
        Ok(self.charged(caller, reply, holding))
    }

    /// `reply` with its charge: a gRPC reply's trimmed to its encoded message now; an
    /// HTTP/JSON one's fitted to its rendered body by the front-end.
    fn charged(
        &self,
        caller: &Caller,
        reply: ServiceReply,
        mut charge: super::charge::Charge,
    ) -> ServiceReply {
        if !caller.json {
            charge.trim(reply.encoded_len() + super::charge::GRPC_FRAME);
        }
        ServiceReply::Charged(Box::new(reply), charge)
    }
}
