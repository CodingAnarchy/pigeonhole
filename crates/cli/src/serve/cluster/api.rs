use super::*;
use tower::Layer as _;

/// The `Cluster` service over gRPC.
#[derive(Debug, Clone)]
/// The `Cluster` service: the node, and on a node that serves data (not a bare node in a
/// test) the server, which a `Forward` is served through.
struct ClusterGrpc(Arc<Node>, Option<Arc<super::super::Shared>>);

impl ClusterGrpc {
    fn caller<T>(&self, req: &Request<T>, scopes: &[Scope], what: &str) -> Result<Caller, Status> {
        let caller = self.0.caller(super::super::grpc::auth_header(req))?;
        self.0.allowed(&caller, scopes, what)?;
        self.0.maybe_pull(&caller, req.metadata());
        Ok(caller)
    }
}

pub(super) const READERS: &[Scope] = &[Scope::Read, Scope::Cluster];

/// `Cluster.ReadTail` and `Cluster.ReplayTail`'s paths (D336 PR 3a): each on a route of its
/// own, outside the `Cluster` calls' small limit, as a memory watch's is.
pub(super) const READ_TAIL: &str = "/pigeonhole.v1.Cluster/ReadTail";
const REPLAY_TAIL: &str = "/pigeonhole.v1.Cluster/ReplayTail";

impl ClusterGrpc {
    /// Partition `id`'s line of `Status`.
    async fn local(&self, id: &str) -> Result<LocalPartition, ApiError> {
        let node = Arc::clone(&self.0);
        let status = tokio::task::spawn_blocking(move || node.status()).await?;
        status
            .partitions
            .into_iter()
            .find(|p| p.id == id)
            .ok_or_else(|| ApiError::not_found(format!("no partition {id:?} on this node")))
    }
}

#[tonic::async_trait]
impl proto::cluster_server::Cluster for ClusterGrpc {
    /// `phdb cluster fence P --node N` (D317 ticket 6): admin scope, or a node's cluster token
    /// (a move the map's writer runs, D332).
    async fn fence(
        &self,
        req: Request<proto::PartitionRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        // An operator's, or the map's writer's in a move it runs (D332, D331 PR 5).
        self.caller(&req, &[Scope::Admin, Scope::Cluster], "fence a partition")?;
        let id = req.into_inner().partition;
        self.0.fence(&id).await?;
        Ok(tonic::Response::new(self.local(&id).await?))
    }

    /// `phdb cluster unfence P --node N` (D317 ticket 6): admin scope, or a node's cluster
    /// token (a move the map's writer runs lifts its own fence on abort, D332).
    async fn unfence(
        &self,
        req: Request<proto::PartitionRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        // A move the writer runs lifts its own fence on abort (the coordinator, #2094).
        let caller = self.caller(&req, &[Scope::Admin, Scope::Cluster], "unfence a partition")?;
        let req = req.into_inner();
        // `force` skips the check that the peers' newest map still names this node: never
        // a move's abort, so an admin's only (simp724's should on #2094).
        if req.force && caller.scope == Scope::Cluster {
            return Err(ApiError::permission_denied(
                "unfence with --force: a forced unfence is an admin's",
            )
            .into());
        }
        let id = req.partition;
        self.0.unfence(&id, req.force).await?;
        Ok(tonic::Response::new(self.local(&id).await?))
    }

    /// `phdb cluster txn decide` (D329 2b): decides a share held in doubt on this node's
    /// partition, for a transaction whose home will never answer. Admin or cluster scope.
    /// It can break the transaction's atomicity, so without `force` it's refused while the
    /// home answers `ResolveTxn` (its resolver settles the share then).
    async fn decide_txn(
        &self,
        req: Request<proto::DecideTxnRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        self.caller(
            &req,
            &[Scope::Admin, Scope::Cluster],
            "decide a transaction",
        )?;
        let r = req.into_inner();
        let Some(shared) = &self.1 else {
            return Err(ApiError::unsupported("this node doesn't serve data").into());
        };
        let Some(cluster) = shared.router.cluster.as_ref() else {
            return Err(ApiError::unsupported("this node isn't a cluster node").into());
        };
        let service = {
            let routes = cluster.node.routes();
            let Some(routes) = routes.as_ref() else {
                return Err(
                    ApiError::wrong_partition(0, "this node has no cluster map yet").into(),
                );
            };
            cluster.owned_here(routes, &r.partition)?
        };
        if !r.force {
            let asked = spanning::ask_home(
                &cluster.node,
                &cluster.parts,
                &cluster.forwarder,
                &cluster.spanning,
                &r.xid,
                None,
                shared.limits.request_timeout,
                false,
            )
            .await;
            // A home that answers UNKNOWN (its history changed) can't settle the share: the
            // operator's decide is what's left (audit120's S1 on #2076).
            if let Ok(home) = asked
                && home.state != proto::TxnOutcomeState::Unknown as i32
            {
                return Err(ApiError::failed_precondition(format!(
                    "the transaction's home answers ({:?}: {}), so its resolver settles this \
                     share; --force only for a home that will never answer, and it can break \
                     the transaction's atomicity",
                    proto::TxnOutcomeState::try_from(home.state)
                        .unwrap_or(proto::TxnOutcomeState::Unspecified),
                    home.reason
                ))
                .into());
            }
        }
        service
            .decide_branch(&r.xid, r.commit, 0, 0, &cluster.spanning, &r.partition)
            .await?;
        // A commit applied by hand acks its home too, so its record can be collected: owed,
        // and sent by the resolver's next pass, not waited for here (#2078, audit120's Q3).
        if r.commit {
            cluster.spanning.queue_ack(&r.xid, &r.partition);
        }
        Ok(tonic::Response::new(self.local(&r.partition).await?))
    }

    /// `phdb cluster close P --node N` (D317 amendment 2): admin scope, or a node's cluster
    /// token (a move the map's writer runs, D332).
    async fn close(
        &self,
        req: Request<proto::PartitionRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        self.caller(&req, &[Scope::Admin, Scope::Cluster], "close a partition")?;
        let id = req.into_inner().partition;
        self.0.close(&id).await?;
        Ok(tonic::Response::new(self.local(&id).await?))
    }

    /// `phdb cluster drop-partition P --node N` (D317 ticket 6): admin scope, or a node's
    /// cluster token (a move the map's writer runs, D332).
    async fn drop_partition(
        &self,
        req: Request<proto::PartitionRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        self.caller(
            &req,
            &[Scope::Admin, Scope::Cluster],
            "drop a partition's files",
        )?;
        let id = req.into_inner().partition;
        Ok(tonic::Response::new(self.0.drop_partition(&id).await?))
    }

    /// `Cluster.Stamp` (D317 ticket 6): relocate's and restore's first step on the node the
    /// partition goes to. Cluster scope, as a peer's calls; `phdb cluster relocate` sends it
    /// with the cluster token. The reply's `files` is the set it installed, if any.
    async fn stamp(
        &self,
        req: Request<proto::StampRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        self.caller(&req, &[Scope::Cluster, Scope::Admin], "stamp a partition")?;
        let req = req.into_inner();
        let id = req.partition.clone();
        let (installed, fork) = self.0.stamp(req).await?;
        let mut lp = self.local(&id).await?;
        lp.files = installed;
        lp.fork_token = fork.into();
        Ok(tonic::Response::new(lp))
    }

    /// Node to node (D326): a lease round's request for this node's grant. Cluster scope.
    async fn lease(
        &self,
        req: Request<proto::LeaseRequest>,
    ) -> Result<tonic::Response<proto::LeaseResponse>, Status> {
        self.caller(&req, &[Scope::Cluster], "ask for a partition lease")?;
        #[cfg(test)]
        if fence::pre_lease(&self.0.dir) || fence::lease_unrouted(&self.0.dir) {
            return Err(Status::unimplemented("a build before D326"));
        }
        let req = req.into_inner();
        let mine = self.0.get_map()?;
        if req.cluster_id != mine.cluster_id {
            return Err(ApiError::invalid(format!(
                "a lease request for cluster {:?} reached a node of cluster {:?}",
                req.cluster_id, mine.cluster_id
            ))
            .into());
        }
        // A pre-vote is answered as a ballot whatever its term: never as a lease request,
        // which could grant the writer lease it names.
        if req.pre_vote {
            return Ok(tonic::Response::new(self.0.grants.pre_vote(
                &mine,
                &req,
                self.0.peers.failover_after,
            )?));
        }
        if self.0.is_vote(&mine, &req) {
            return Ok(tonic::Response::new(self.0.vote_async(mine, req).await?));
        }
        let r = self.0.grants.grant(&mine, &req)?;
        // D335 PR 2b: the grant's fallback records, synced before the answer echoes them.
        let r = self.0.record_degraded(&mine, &req, r).await;
        // The writer notes an asker it answers with its newer map (#2073): one that stays
        // behind may be stranded by the term fence.
        if schema::writer(&mine) == self.0.id {
            let now = self.0.grants.clock().now();
            let mut s = self
                .0
                .stranded
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if r.newer_map_version != 0 {
                s.behind(&req.node_id, now);
            } else {
                s.caught_up(&req.node_id);
            }
        }
        Ok(tonic::Response::new(r))
    }

    /// Node to node (D326 PR 2): a node gave up partitions it closed. Cluster scope.
    async fn release(
        &self,
        req: Request<proto::ReleaseRequest>,
    ) -> Result<tonic::Response<proto::ReleaseResponse>, Status> {
        self.caller(&req, &[Scope::Cluster], "release a partition lease")?;
        #[cfg(test)]
        if fence::pre_lease(&self.0.dir) || fence::lease_unrouted(&self.0.dir) {
            return Err(Status::unimplemented("a build before D326"));
        }
        let req = req.into_inner();
        let mine = self.0.get_map()?;
        if req.cluster_id != mine.cluster_id {
            return Err(ApiError::invalid(format!(
                "a lease release for cluster {:?} reached a node of cluster {:?}",
                req.cluster_id, mine.cluster_id
            ))
            .into());
        }
        self.0.grants.release(&req);
        Ok(tonic::Response::new(proto::ReleaseResponse {}))
    }

    async fn get_map(
        &self,
        req: Request<GetMapRequest>,
    ) -> Result<tonic::Response<ClusterMap>, Status> {
        self.caller(&req, READERS, "read the cluster map")?;
        Ok(tonic::Response::new(self.0.get_map()?))
    }

    async fn set_map(
        &self,
        req: Request<SetMapRequest>,
    ) -> Result<tonic::Response<SetMapResponse>, Status> {
        self.caller(&req, &[Scope::Admin], "set the cluster map")?;
        Ok(tonic::Response::new(
            self.0.set_map(req.into_inner()).await?,
        ))
    }

    async fn push_map(
        &self,
        req: Request<PushMapRequest>,
    ) -> Result<tonic::Response<PushMapResponse>, Status> {
        // Only a node pushes (an admin sets the map with `SetMap`). Without tokens (a
        // loopback test cluster) everyone may.
        self.caller(&req, &[Scope::Cluster], "push a cluster map")?;
        let from = req
            .metadata()
            .get(NODE_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let map = req
            .into_inner()
            .map
            .ok_or_else(|| ApiError::invalid("PushMap needs a map"))?;
        // A push the cut overtook isn't taken (#2259): its dial passed before the cut.
        #[cfg(test)]
        if let Some(from) = &from
            && self.0.cut_from(from)
        {
            return Err(ApiError::partition_unavailable(format!(
                "node {from}'s push: cut off (a test's seam)"
            ))
            .into());
        }
        Ok(tonic::Response::new(
            self.0.push_map_async(map, from).await?,
        ))
    }

    async fn status(
        &self,
        req: Request<StatusRequest>,
    ) -> Result<tonic::Response<NodeStatus>, Status> {
        self.caller(&req, READERS, "read the cluster status")?;
        #[cfg(test)]
        while HOLD_STATUS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&self.0.dir)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let open_only = req.get_ref().open_only;
        let node = Arc::clone(&self.0);
        let status = tokio::task::spawn_blocking(move || node.status_with(open_only))
            .await
            .map_err(ApiError::from)?;
        Ok(tonic::Response::new(status))
    }

    /// Node to node: a call another node routed here (D317 ticket 2). Cluster scope; the
    /// call is then checked for the caller it names, and served only if this node's map
    /// gives it the key.
    async fn forward(
        &self,
        req: Request<proto::ForwardRequest>,
    ) -> Result<tonic::Response<proto::ForwardResponse>, Status> {
        let peer = self.caller(&req, &[Scope::Cluster], "forward a call")?;
        #[cfg(test)]
        seams::hold_forward(&self.0.dir).await;
        let Some(shared) = &self.1 else {
            return Err(ApiError::unsupported("this node doesn't serve data").into());
        };
        self.0.catch_up(&peer, req.metadata()).await?;
        // The sender's deadline for the call (it sends what's left of its client's), for a
        // replica copy's read-your-writes wait (D328).
        let deadline = crate::serve::replica::ReadDeadline::new(
            std::time::Instant::now(),
            shared.limits.request_timeout,
            crate::serve::grpc::grpc_timeout(req.metadata().as_ref()),
        );
        let msg = req.into_inner();
        // A spanning transaction's branch call (D329): served on the partition it names.
        if spanning::is_branch(&msg) {
            // The nodes' own arms carry no client to check (#2165): on a cluster with tokens,
            // the resolver's and the sweep's anonymous caller would be refused. The data arms
            // bind their sessions to the client's token, as on a single node.
            let caller = if spanning::is_node_arm(&msg) {
                crate::serve::auth::Caller::anonymous()
            } else {
                forward::caller_of(msg.caller, &self.0.tokens)?
            };
            let Some(cluster) = shared.router.cluster.as_ref() else {
                return Err(ApiError::unsupported("this node isn't a cluster node").into());
            };
            let service = {
                let routes = cluster.node.routes();
                let Some(routes) = routes.as_ref() else {
                    return Err(
                        ApiError::wrong_partition(0, "this node has no cluster map yet").into(),
                    );
                };
                cluster.owned_here(routes, &msg.partition)?
            };
            let op = msg
                .op
                .ok_or_else(|| ApiError::invalid("a forward carries no call"))?;
            let reply = spanning::serve_branch(
                &cluster.node,
                &cluster.spanning,
                service,
                &msg.partition,
                op,
                &caller,
            )
            .await?;
            return Ok(tonic::Response::new(reply));
        }
        // D331 PR 4b: a range move's copy chunk, answered by the source's copy server.
        if let Some(proto::forward_request::Op::RangeCopy(r)) = msg.op {
            let Some(cluster) = shared.router.cluster.as_ref() else {
                return Err(ApiError::unsupported("this node isn't a cluster node").into());
            };
            let chunk = cluster
                .range_copies
                .serve(cluster, &msg.partition, r)
                .await?;
            return Ok(tonic::Response::new(proto::ForwardResponse {
                reply: Some(proto::forward_response::Reply::RangeCopy(chunk)),
                ..Default::default()
            }));
        }
        // D332: a partition's tablets, for the map's writer's balancer.
        if let Some(proto::forward_request::Op::Tablets(r)) = &msg.op {
            let Some(cluster) = shared.router.cluster.as_ref() else {
                return Err(ApiError::unsupported("this node isn't a cluster node").into());
            };
            let reply = balancing::serve_tablets(cluster, &r.partition).await?;
            return Ok(tonic::Response::new(proto::ForwardResponse {
                reply: Some(proto::forward_response::Reply::Tablets(reply)),
                ..Default::default()
            }));
        }
        let op = forward::op_of(msg.op)?;
        // A schema change at the map's writer takes its caller's scope uncapped, from this
        // node's own token entry (D317 amendment 1).
        let caller = if crate::serve::router::map_schema_change(&op) {
            forward::schema_caller_of(msg.caller, &self.0.tokens)?
        } else {
            forward::caller_of(msg.caller, &self.0.tokens)?
        };
        // A read the entry node chose this node's replica copy for (D328).
        let replica = msg.to_replica.then_some(crate::serve::router::ReplicaRead {
            max_lag_ms: msg.replica_max_lag_ms,
            min_primary_seqno: (msg.min_primary_seqno > 0).then_some(msg.min_primary_seqno),
        });
        let (reply, served) = shared
            .router
            .serve_forwarded(
                op,
                &caller,
                msg.fanout_part,
                &msg.partition,
                replica.map(|r| (r, deadline)),
            )
            .await?;
        let (reply, charge) = forward::reply_to_forward(reply, served)?;
        let mut resp = tonic::Response::new(reply);
        // A read's charge lasts until the reply's body is written: `routes` holds it there.
        if let Some(c) = charge {
            resp.extensions_mut()
                .insert(super::super::charge::Charged::new(c));
        }
        Ok(resp)
    }

    type ForwardSubscribeStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<proto::FeedEvent, Status>> + Send>,
    >;

    type ReadTailStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<proto::ReadTailEvent, Status>> + Send>,
    >;

    /// Node to node (D336 PR 3a): this node's old file of a partition, as the source of its
    /// tail's replay ([`tail_source`](super::tail_source)). Cluster scope; under a
    /// `--max-streams` place, on a route of its own outside the `Cluster` calls' small limit,
    /// since a tail may take a while to read.
    async fn read_tail(
        &self,
        req: Request<proto::ReadTailRequest>,
    ) -> Result<tonic::Response<Self::ReadTailStream>, Status> {
        self.caller(&req, &[Scope::Cluster], "read an old owner's tail")?;
        let Some(shared) = &self.1 else {
            return Err(ApiError::unsupported("this node doesn't serve data").into());
        };
        let slot = shared.router.service.stream_slot()?;
        let tail = self.0.read_tail(req.into_inner(), slot).await?;
        Ok(tonic::Response::new(Box::pin(
            crate::serve::grpc::GrpcStream(tail),
        )))
    }

    /// `phdb cluster replay-tail P` (D336 PR 3a), on P's owner: replays an old owner's tail
    /// into P ([`tail_replay`](super::tail_replay)), and answers P's line of `Status` with its
    /// progress. Admin scope, or a node's cluster token (the map's writer's automatic
    /// replay). On a route of its own, outside the `Cluster` calls' small limit: it waits up
    /// to its timeout.
    async fn replay_tail(
        &self,
        req: Request<proto::ReplayTailRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        self.caller(
            &req,
            &[Scope::Admin, Scope::Cluster],
            "replay an old owner's tail",
        )?;
        let r = req.into_inner();
        let id = r.partition.clone();
        self.0.replay_tail(r).await?;
        Ok(tonic::Response::new(self.local(&id).await?))
    }

    /// Served by a route of its own ([`feed::ForwardSubscribeGrpc`]), ahead of this one.
    async fn forward_subscribe(
        &self,
        _req: Request<proto::ForwardSubscribeRequest>,
    ) -> Result<tonic::Response<Self::ForwardSubscribeStream>, Status> {
        Err(Status::unimplemented(
            "this node doesn't serve data, so it has no partition to subscribe to",
        ))
    }

    type ForwardMemoryWatchStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<proto::MemoryEvent, Status>> + Send>,
    >;

    /// Node to node: one partition's component of a memory watch another node merges
    /// (#1915 PR 2b). Cluster scope; served for the caller it names, if this node's map
    /// gives it the partition. Mounted on a route of its own, outside the `Cluster` calls'
    /// small limit, since a watch lasts: it holds a `--max-streams` place on the partition.
    async fn forward_memory_watch(
        &self,
        req: Request<proto::ForwardMemoryWatchRequest>,
    ) -> Result<tonic::Response<Self::ForwardMemoryWatchStream>, Status> {
        let peer = self.caller(&req, &[Scope::Cluster], "forward a memory watch")?;
        let Some(shared) = &self.1 else {
            return Err(ApiError::unsupported("this node doesn't serve data").into());
        };
        self.0.maybe_pull(&peer, req.metadata());
        self.0.catch_up(&peer, req.metadata()).await?;
        let events = shared
            .router
            .serve_forwarded_memory_watch(req.into_inner())
            .await?;
        Ok(tonic::Response::new(Box::pin(
            crate::serve::grpc::GrpcStream(events),
        )))
    }

    /// Node to node: one partition's part of a `Vectors.BulkLoad` another node splits by id
    /// (#1722 PR 3). Cluster scope; served for the caller it names, as this node's own load
    /// of the partition, if this node's map gives it the partition. Mounted on a route of its
    /// own behind `prepay`, outside the `Cluster` calls' small limit (a load runs for
    /// minutes), so each message reserves its share of the in-flight budget as it arrives.
    async fn forward_bulk_load(
        &self,
        req: Request<tonic::Streaming<proto::ForwardBulkLoadRequest>>,
    ) -> Result<tonic::Response<proto::Empty>, Status> {
        let peer = self.caller(&req, &[Scope::Cluster], "forward a bulk load")?;
        let Some(shared) = &self.1 else {
            return Err(ApiError::unsupported("this node doesn't serve data").into());
        };
        self.0.catch_up(&peer, req.metadata()).await?;
        let (budget, meter) = crate::serve::grpc::stream_budget(&req);
        let limits = shared.limits;
        let wait = crate::serve::grpc::grpc_timeout(req.metadata().as_ref())
            .map_or(limits.request_timeout, |d| d.min(limits.request_timeout));
        let mut stream = req.into_inner();
        let Some(first) = stream.message().await? else {
            return Err(ApiError::invalid(
                "a forwarded BulkLoad's first message names its partition and index",
            )
            .into());
        };
        let caller = forward::caller_of(first.caller, &self.0.tokens)?;
        let load = first
            .load
            .ok_or_else(|| ApiError::invalid("a forwarded BulkLoad carries no load"))?;
        shared
            .router
            .serve_forwarded_bulk_load(
                &first.partition,
                load,
                budget,
                crate::serve::grpc::ForwardSource { stream, meter },
                &caller,
                wait,
            )
            .await?;
        Ok(tonic::Response::new(proto::Empty {}))
    }

    type ForwardScanStream =
        std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<proto::ScanPage, Status>> + Send>>;

    /// Node to node: one partition's part of a scan another node fans out (D317 ticket 4a).
    /// Cluster scope; served for the caller it names, as this node's own scan of the
    /// partition, if this node's map gives it the partition.
    async fn forward_scan(
        &self,
        req: Request<proto::ForwardScanRequest>,
    ) -> Result<tonic::Response<Self::ForwardScanStream>, Status> {
        let peer = self.caller(&req, &[Scope::Cluster], "forward a scan")?;
        let Some(shared) = &self.1 else {
            return Err(ApiError::unsupported("this node doesn't serve data").into());
        };
        self.0.catch_up(&peer, req.metadata()).await?;
        let msg = req.into_inner();
        let caller = forward::caller_of(msg.caller, &self.0.tokens)?;
        let scan = msg
            .scan
            .ok_or_else(|| ApiError::invalid("a forwarded scan carries no scan"))?;
        let pages = shared
            .router
            .serve_forwarded_scan(
                &msg.partition,
                scan,
                &caller,
                msg.to_replica.then_some(msg.replica_max_lag_ms),
            )
            .await?;
        Ok(tonic::Response::new(Box::pin(
            super::super::grpc::GrpcStream(pages),
        )))
    }
}

fn http_caller(
    node: &Node,
    headers: &HeaderMap,
    scopes: &[Scope],
    what: &str,
) -> Result<(), ApiError> {
    let caller = node.caller(crate::serve::http::auth_header(headers))?;
    node.allowed(&caller, scopes, what)
}

async fn http_get_map(State(node): State<Arc<Node>>, headers: HeaderMap) -> Response {
    match http_caller(&node, &headers, READERS, "read the cluster map")
        .and_then(|()| node.get_map())
    {
        Ok(m) => axum::Json(m).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn http_set_map(
    State(node): State<Arc<Node>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let run = async {
        http_caller(&node, &headers, &[Scope::Admin], "set the cluster map")?;
        let req: SetMapRequest = serde_json::from_slice(&body)
            .map_err(|e| ApiError::invalid(format!("not a SetMapRequest: {e}")))?;
        node.set_map(req).await
    };
    match run.await {
        Ok(r) => axum::Json(r).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn http_status(State(node): State<Arc<Node>>, headers: HeaderMap) -> Response {
    if let Err(e) = http_caller(&node, &headers, READERS, "read the cluster status") {
        return e.into_response();
    }
    match tokio::task::spawn_blocking(move || node.status()).await {
        Ok(s) => axum::Json(s).into_response(),
        Err(e) => ApiError::internal(e.to_string()).into_response(),
    }
}

/// The `Cluster` service's routes, gRPC and HTTP, which the data listener mounts beside the
/// data routes (D317 ticket 2). Outside the server's load shedding, in lanes of their own
/// (#2258): a node busy with data must still take a map, or a re-forward's pull fails
/// exactly when it matters (cli692's 3), and must still answer its peers' lease rounds, or
/// every owner that asks it loses its lease while its runtime sits idle.
///
/// - `Lease` and `Release` (and so the votes, which `Lease` carries): a lane of their own,
///   since an answer that's late fences an owner.
/// - `Forward` and `ForwardScan`, the data calls (a spanning transaction's branch calls
///   among them): a lane of their own. An owner that sheds one answers `Overloaded`, which
///   the forwarding node passes back.
/// - Every other call (map reads, pushes and installs, status, fences): the control lane.
///   So a forward's own map pull (`catch_up`, from the node that sent it) never waits
///   behind forwards: two nodes forwarding to each other can't hold each other's pulls.
///
/// Each lane's limit is shared by its routes. Before #2258 one limit took every call, and
/// a few dozen forwards in flight left a lease round's calls queued until they timed out.
/// The `Cluster` paths routed apart from the control lane (#2258).
const LEASE: &str = "/pigeonhole.v1.Cluster/Lease";
const RELEASE: &str = "/pigeonhole.v1.Cluster/Release";
const FORWARD: &str = "/pigeonhole.v1.Cluster/Forward";
const FORWARD_SCAN: &str = "/pigeonhole.v1.Cluster/ForwardScan";

pub(crate) fn routes(
    node: Arc<Node>,
    shared: Option<Arc<super::super::Shared>>,
    max_request_bytes: usize,
) -> axum::Router {
    let idle = shared.as_ref().map(|s| s.limits.stream_idle);
    // A cluster subscription's parts last as long as it does: outside the limit below, each
    // holding a `--max-streams` place instead (D317 ticket 7).
    // So do a memory watch's (#1915 PR 2b): the `Cluster` service again, on that one path.
    let streams = shared.as_ref().map(|shared| {
        let feeds = crate::serve::grpc::EventsGrpc(feed::ForwardSubscribeGrpc {
            node: Arc::clone(&node),
            shared: Arc::clone(shared),
            max_request_bytes,
        });
        let watches = proto::cluster_server::ClusterServer::new(ClusterGrpc(
            Arc::clone(&node),
            Some(Arc::clone(shared)),
        ))
        .max_decoding_message_size(max_request_bytes.saturating_add(forward::FORWARD_ENVELOPE));
        // A BulkLoad's part (#1722 PR 3): a client stream, metered by `prepay` as a client's
        // `Vectors.BulkLoad` is, and outside the limit below, since a load runs for minutes.
        let loads = tower::ServiceBuilder::new()
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(shared),
                crate::serve::grpc::prepay,
            ))
            .service(
                proto::cluster_server::ClusterServer::new(ClusterGrpc(
                    Arc::clone(&node),
                    Some(Arc::clone(shared)),
                ))
                .max_decoding_message_size(
                    max_request_bytes.saturating_add(forward::FORWARD_ENVELOPE),
                ),
            );
        (feeds, watches, loads)
    });
    // A forward carries a call the entry node took within its `--max-request-bytes`, inside
    // an envelope (audit120's S-C2).
    let grpc = proto::cluster_server::ClusterServer::new(ClusterGrpc(Arc::clone(&node), shared))
        .max_decoding_message_size(max_request_bytes.saturating_add(forward::FORWARD_ENVELOPE));
    let control = tower::limit::GlobalConcurrencyLimitLayer::new(CLUSTER_CALLS);
    let leases = tower::limit::GlobalConcurrencyLimitLayer::new(LEASE_CALLS);
    let data = tower::limit::GlobalConcurrencyLimitLayer::new(CLUSTER_CALLS);
    let mut app = axum::Router::new()
        .route("/v1/cluster/map", get(http_get_map).post(http_set_map))
        .route("/v1/cluster/status", get(http_status))
        .with_state(node)
        .layer(control.clone())
        .route_service(
            "/pigeonhole.v1.Cluster/{*method}",
            control.layer(grpc.clone()),
        )
        .route_service(LEASE, leases.layer(grpc.clone()))
        .route_service(RELEASE, leases.layer(grpc.clone()))
        .route_service(FORWARD, data.layer(grpc.clone()))
        .route_service(FORWARD_SCAN, data.layer(grpc));
    // A forwarded read's charge (#1567) moves into its reply's body, as the data routes'
    // `prepay` does, so it's held until the reply is written (cli692's F1 on ticket 2).
    if let Some(idle) = idle {
        app =
            app.layer(axum::middleware::map_response(
                move |resp: axum::response::Response| async move {
                    super::super::charge::hold(resp, idle)
                },
            ));
    }
    match streams {
        Some((feeds, watches, loads)) => app
            .route_service(feed::FORWARD_SUBSCRIBE, feeds)
            .route_service(memwatch::FORWARD_MEMORY_WATCH, watches.clone())
            .route_service(READ_TAIL, watches.clone())
            .route_service(REPLAY_TAIL, watches)
            .route_service(crate::serve::grpc::FORWARD_BULK_LOAD, loads),
        None => app,
    }
}
