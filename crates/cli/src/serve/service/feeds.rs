use super::*;

impl Service {
    /// `Feed.Subscribe` (D313 R2): the client's tables resolved once (so its hub and any
    /// catch-up stream follow the same table ids), its start checked, and its stream handed to
    /// the fan-out. It holds a `--max-streams` slot until it ends.
    async fn subscribe(
        &self,
        r: proto::SubscribeRequest,
        caller: &Caller,
        budget: OwnedSemaphorePermit,
    ) -> Result<super::feed::FeedEvents, ApiError> {
        if r.allow_partial || !r.skip_through.is_empty() || r.checkpoint_every != 0 {
            return Err(ApiError::invalid(
                "allow_partial, skip_through and checkpoint_every are for a cluster \
                 subscription (D317), and this server isn't a cluster node",
            ));
        }
        drop(budget);
        self.subscribe_part(r, caller, PartStart::default()).await
    }

    /// D331 PR 3b: the last seqno of a range move that took table `table` from this
    /// partition, while its moved-away row is here: a cluster component of it reads up to
    /// there (the latest, for several).
    pub(crate) fn moved_away(&self, table: &str) -> Option<u64> {
        self.gate
            .as_ref()?
            .ranges()
            .moved_rows()
            .into_iter()
            .filter(|m| m.table == table)
            .map(|m| m.final_seqno)
            .max()
    }

    /// A subscription of this server's database: a single node's `Feed.Subscribe`, or one
    /// partition's part of a cluster subscription (D317 ticket 7), whose `skip` passes over
    /// commits the client already handled ([`super::feed::Spec::skip`]) and which
    /// `announce`s its start ([`super::feed::Spec::announce`]). Holds a `--max-streams`
    /// place either way.
    ///
    /// D331 PR 3b, on a cluster partition a range move gave a range of these tables: a
    /// cluster component (`announce`) that starts at or before a move's hand-over carries
    /// its barrier ([`super::feed::Barrier`]), and `from_hand_over` starts it at the earliest
    /// hand-over of these tables here, not at `r`'s start (a resume that gained this
    /// partition through a move; `FeedExpired` if no move did).
    pub(crate) async fn subscribe_part(
        &self,
        mut r: proto::SubscribeRequest,
        caller: &Caller,
        part: PartStart,
    ) -> Result<super::feed::FeedEvents, ApiError> {
        let PartStart {
            skip,
            announce,
            internal,
            from_hand_over,
            ends_at,
        } = part;
        use super::feed::Start;
        use proto::subscribe_request::Start as S;
        let slot = self.stream_slot()?;
        if r.tables.is_empty() {
            return Err(ApiError::invalid("a subscription names at least one table"));
        }
        let mut names = r.tables.clone();
        names.sort();
        names.dedup();
        let mut tables = Vec::with_capacity(names.len());
        for name in &names {
            tables.push(self.table(name).await?);
        }
        let barriers = if announce {
            self.hand_overs(&names, &mut r, from_hand_over)?
        } else {
            Vec::new()
        };
        let start = match r.start {
            None | Some(S::FromOldest(false)) => Start::Now,
            Some(S::FromOldest(true)) => Start::Oldest,
            Some(S::FromSnapshot(id)) => Start::Snapshot(
                self.sessions
                    .snapshot(&id, caller.principal)?
                    .snapshot()
                    .clone(),
            ),
            Some(S::Resume(token)) => {
                Start::Token(pigeonhole::FeedToken::from_bytes(token.to_vec()))
            }
        };
        let metrics = Arc::clone(&self.metrics);
        let started = Instant::now();
        self.part()?
            .feeds
            .subscribe(
                super::feed::Spec {
                    tables,
                    prefix: r.prefix.to_vec(),
                    visible: r.visible,
                    start,
                    skip,
                    announce,
                    internal,
                },
                slot,
                move |e| {
                    // A client's own subscription that hit the idle limit; a cluster
                    // subscription's part (`announce`) is counted with its subscription.
                    if !announce && e.is_some_and(|e| e.code == "DeadlineExceeded") {
                        metrics.feed_idle_drops.fetch_add(1, Ordering::Relaxed);
                    }
                    metrics.count(FEED, e.map(|e| e.code.as_str()), started.elapsed());
                },
            )
            .and_then(|events| match &self.gate {
                // A cluster partition's: delivered only under its lease (D326 PR 2). A
                // subscribe isn't a read for the gate (feed1248's F1): it waits for a lease
                // it hasn't had yet, and is refused only where it lapsed or the node can't
                // reach a majority (F2).
                Some(gate) => events.leased(Arc::clone(gate)),
                None => Ok(events),
            })
            .map(|events| {
                // A cluster component of a cluster partition watches for a hand-over it
                // takes while running (D331 PR 3b).
                let watch = self
                    .gate
                    .as_ref()
                    .filter(|_| announce)
                    .map(|g| (Arc::clone(g), names.clone()));
                events.with_barriers(barriers, ends_at, watch)
            })
    }

    /// The range moves' hand-overs a cluster component of `tables` here carries (D331 PR
    /// 3b): one barrier for each serving row of these tables, whatever the start.
    /// `from_hand_over` first sets `r`'s start to the earliest of their tokens. None on a
    /// partition no move gave a range.
    fn hand_overs(
        &self,
        tables: &[String],
        r: &mut proto::SubscribeRequest,
        from_hand_over: bool,
    ) -> Result<Vec<super::feed::Barrier>, ApiError> {
        use proto::subscribe_request::Start as S;
        let rows: Vec<super::ranges::Moved> = self
            .gate
            .as_ref()
            .map(|g| g.ranges().serving_rows())
            .unwrap_or_default()
            .into_iter()
            .filter(|m| !m.token.is_empty() && tables.contains(&m.table))
            .collect();
        if from_hand_over {
            // A move into this partition that hasn't reached CLEANING (or whose serving row
            // isn't here yet) is no gap: it serves none of the range yet, so the resume asks
            // again shortly (cli692's M1 on PR 3b). No move at all is one.
            let handing = self
                .gate
                .as_ref()
                .and_then(|g| g.ranges().load().as_ref().cloned())
                .is_some_and(|s| tables.iter().any(|t| s.awaits_hand_over(t)));
            if rows.is_empty() && handing {
                return Err(ApiError::partition_unavailable(
                    "this partition is taking a range of these tables over from another, \
                     and doesn't serve it yet: resume again shortly",
                ));
            }
            let first = rows.iter().min_by_key(|m| m.seqno).ok_or_else(|| {
                ApiError::from_code(
                    pigeonhole::ErrorCode::FeedExpired,
                    "no range move gave this partition a range of these tables, so a \
                     resume has no hand-over to start it at: subscribe again",
                )
            })?;
            r.start = Some(S::Resume(first.token.clone().into()));
        }
        Ok(barriers_for(rows, r.start.as_ref()))
    }

    /// The feed fan-out's limits: a cluster subscription's buffer and idle limit are a
    /// single client's (D317 ticket 7), on a node that only routes too.
    /// The node's feed state every partition shares: its catch-up slots and the rings
    /// that split `--feed-ring-bytes` (#1798's cluster feed hubs use both).
    pub(crate) fn feed_shared(&self) -> &super::feed::FeedShared {
        &self.feed_shared
    }

    pub(crate) fn feed_limits(&self) -> super::feed::FeedLimits {
        feed_limits(&self.limits)
    }
}
