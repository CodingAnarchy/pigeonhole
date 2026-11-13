//! D317's Router seam: every protocol front-end calls `Router::dispatch`. A single node
//! passes straight to its one partition. A cluster node (D317 ticket 2) routes by the
//! partition map: a single-key call to its partition, served here or forwarded to the node
//! that owns it, without touching any handler.
//!
//! perf287's conditions on the seam: the operation is moved, never cloned or boxed per call,
//! and a single node does no map load at all (`cluster` is `None`, one branch).

use std::sync::Arc;

use super::auth::Caller;
use super::cluster::forward::{self, Forwarder};
use super::cluster::route::Routes;
use super::errors::ApiError;
use super::proto::{ForwardRequest, RoutingKey, RowWrites, ScanRequest, Selection, routing_key};
use super::replica::ReadDeadline;
use super::service::{Partition, Service, ServiceOp, ServiceReply};

/// A cluster node's routing: the node (its map, routes and peers) and the partitions it
/// serves, each with its own service (D317 ticket 8a), by id.
#[derive(Debug)]
pub(crate) struct Cluster {
    pub(crate) node: Arc<super::cluster::Node>,
    pub(crate) parts: Parts,
    pub(crate) forwarder: Arc<Forwarder>,
    /// Partition files streamed in for a restore or a relocation (D317), staged in the
    /// node's cluster directory.
    pub(crate) uploads: Arc<super::cluster::upload::Uploads>,
}

/// A cluster node's partitions open here, each served by its own [`Service`] sharing the
/// node's (D317 ticket 8a), by id. Shared with a cluster subscription's task, which opens a
/// part served here (ticket 7). Swapped whole as a partition opens or closes on a running
/// node (ticket 6); every call loads it once.
pub(crate) type Parts = Arc<arc_swap::ArcSwap<PartMap>>;

/// [`Parts`]' map: each open partition's service, by id.
pub(crate) type PartMap = std::collections::BTreeMap<String, Arc<Service>>;

impl Cluster {
    /// Partition `id`'s service, if it's open here.
    pub(crate) fn here(&self, id: &str) -> Option<Arc<Service>> {
        self.parts.load().get(id).cloned()
    }
}

/// A partition's service that serves one ([`Service::part_opt`] is `Some`), read as its
/// [`Partition`]: what [`Router::parts`] and [`Router::partition`] give, owned, so a
/// partition closed meanwhile stays whole until it's dropped.
#[derive(Debug, Clone)]
pub(crate) struct PartRef(Arc<Service>);

impl PartRef {
    fn of(service: &Arc<Service>) -> Option<Self> {
        service.part_opt().map(|_| Self(Arc::clone(service)))
    }
}

impl std::ops::Deref for PartRef {
    type Target = Partition;

    fn deref(&self) -> &Partition {
        // `of` made it only from a service with a partition, which never loses it.
        self.0
            .part_opt()
            .expect("a PartRef's service serves a partition")
    }
}

/// Where a call goes.
#[derive(Debug)]
pub(crate) struct Router {
    /// Shared with a cluster subscription's task, which reopens a part served here
    /// (D317 ticket 7).
    pub(crate) service: Arc<Service>,
    pub(crate) cluster: Option<Cluster>,
}

impl Router {
    pub(crate) fn single(service: Service) -> Self {
        Self {
            service: Arc::new(service),
            cluster: None,
        }
    }

    /// A cluster node's router: `service` is the node's (its budgets, pools, sessions caps
    /// and metrics; it serves no partition itself), and `parts` the partitions the map gives
    /// the node, each with its service made from the node's ([`Service::for_partition`]).
    /// None: the node only routes.
    pub(crate) fn cluster(
        service: Service,
        node: Arc<super::cluster::Node>,
        parts: Vec<(String, Service)>,
    ) -> Self {
        let forwarder = Arc::new(Forwarder::new(Arc::clone(&node)));
        let parts = parts.into_iter().map(|(id, s)| (id, Arc::new(s))).collect();
        let uploads = Arc::new(super::cluster::upload::Uploads::new(
            node.dir(),
            1,
            super::cluster::upload::max_chunk(service.limits().max_request_bytes),
        ));
        Self {
            service: Arc::new(service),
            cluster: Some(Cluster {
                node,
                parts: Arc::new(arc_swap::ArcSwap::from_pointee(parts)),
                forwarder,
                uploads,
            }),
        }
    }

    /// `--max-uploads`: a cluster node's partition uploads at once.
    pub(crate) fn max_uploads(mut self, max: usize) -> Self {
        if let Some(c) = self.cluster.as_mut() {
            Arc::get_mut(&mut c.uploads)
                .expect("--max-uploads is set before the router is shared")
                .set_max(max);
        }
        self
    }

    /// `Admin.PartitionUpload` (D317): `stream`'s file of a partition this node doesn't
    /// have open, staged for the stamp that installs it. Admin scope; a cluster node only.
    pub(crate) async fn partition_upload(
        &self,
        caller: &Caller,
        mut stream: tonic::Streaming<super::proto::PartitionUploadChunk>,
    ) -> Result<super::proto::PartitionUploadReply, ApiError> {
        let uploads = self.uploads_for(caller)?;
        // Each message within `--stream-idle-ms` (cli692's F1 on #1728): a stalled stream
        // ends, so its upload goes back to resumable and its `--max-uploads` place is free.
        let idle = self.service.limits().stream_idle;
        let mut next = async || match tokio::time::timeout(idle, stream.message()).await {
            Ok(m) => {
                m.map_err(|s| ApiError::invalid(format!("the upload's stream: {}", s.message())))
            }
            Err(_) => Err(ApiError::deadline(
                "no upload message for --stream-idle-ms: resume from PartitionUploadStatus",
            )),
        };
        let Some(first) = next().await? else {
            return Err(ApiError::invalid("an upload sends its header first"));
        };
        let Some(header) = first.header.clone() else {
            return Err(ApiError::invalid(
                "an upload's first message carries its header",
            ));
        };
        let partitions = self.partitions();
        let open = |p: &str| partitions.iter().any(|(id, _)| id == p);
        let mut writing = uploads.begin(header, open)?;
        let mut chunk = Some(first);
        while let Some(c) = chunk.take() {
            if !c.data.is_empty() {
                // One chunk's write (at most 1 MiB), off the async threads.
                writing = tokio::task::spawn_blocking(move || writing.write(&c).map(|()| writing))
                    .await
                    .map_err(|e| ApiError::internal(format!("an upload's write: {e}")))??;
            }
            chunk = next().await?;
        }
        // The SHA-256 of the whole file and its sync, likewise.
        tokio::task::spawn_blocking(move || writing.finish())
            .await
            .map_err(|e| ApiError::internal(format!("an upload's finish: {e}")))?
    }

    /// `Admin.PartitionUploadStatus` (D317).
    pub(crate) fn partition_upload_status(
        &self,
        caller: &Caller,
        upload_id: &str,
    ) -> Result<super::proto::PartitionUploadStatusReply, ApiError> {
        self.uploads_for(caller)?.status(upload_id)
    }

    fn uploads_for(
        &self,
        caller: &Caller,
    ) -> Result<&Arc<super::cluster::upload::Uploads>, ApiError> {
        if caller.memory_only() {
            return Err(super::service::namespaced_denied("call PartitionUpload"));
        }
        if !caller.scope.allows(super::auth::Scope::Admin) {
            return Err(ApiError::permission_denied("call PartitionUpload"));
        }
        match &self.cluster {
            Some(c) => Ok(&c.uploads),
            None => Err(ApiError::unsupported(
                "a partition upload is for a cluster node (phdb serve --cluster)",
            )),
        }
    }

    /// The cluster node, on a cluster node.
    pub(crate) fn cluster_node(&self) -> Option<&Arc<super::cluster::Node>> {
        self.cluster.as_ref().map(|c| &c.node)
    }

    /// A cluster node's partitions open now, by id, in id order (D317 ticket 8a). Nothing on
    /// a single node, whose one database is the service's. Callers use it, never
    /// `Cluster::parts` or `Service::part` (simp724's A1 on 8b).
    pub(crate) fn partitions(&self) -> Vec<(String, PartRef)> {
        self.cluster
            .iter()
            .flat_map(|c| {
                c.parts
                    .load()
                    .iter()
                    .filter_map(|(id, s)| PartRef::of(s).map(|p| (id.clone(), p)))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Every database this server serves: a single node's one, or a cluster node's
    /// partitions open now (D317 ticket 8a). For what's done to each alike: feeds started,
    /// the dump, bulk loads stopped, indexes closed, health.
    pub(crate) fn parts(&self) -> Vec<PartRef> {
        match &self.cluster {
            None => PartRef::of(&self.service).into_iter().collect(),
            Some(_) => self.partitions().into_iter().map(|(_, p)| p).collect(),
        }
    }

    /// Partition `id` on a cluster node: `WrongPartition` when the map gives it to another
    /// node, ticket 2's `not_open` when the map gives it here but it isn't open, `NotFound`
    /// when the map has no such partition.
    pub(crate) fn partition(&self, id: &str) -> Result<PartRef, ApiError> {
        let Some(cluster) = &self.cluster else {
            return Err(ApiError::invalid("this server isn't a cluster node"));
        };
        let routes = cluster.node.routes();
        let Some(routes) = routes.as_ref() else {
            return Err(ApiError::partition_unavailable(
                "this cluster node has no map yet",
            ));
        };
        let owner = routes.owner(id).ok_or_else(|| {
            ApiError::not_found(format!("partition {id:?} isn't in the cluster map"))
        })?;
        if owner.node != cluster.node.id() {
            return Err(ApiError::wrong_partition(
                routes.map_version(),
                format!("partition {id:?} is node {:?}'s", owner.node),
            ));
        }
        cluster
            .here(id)
            .as_ref()
            .and_then(PartRef::of)
            .ok_or_else(|| not_open(id))
    }

    /// The metrics as Prometheus text (`/metrics`, `Admin.Metrics`): a cluster node's
    /// partitions' each labelled by id (D317 ticket 8b), a single node's one unlabelled.
    pub(crate) fn metrics_text(&self) -> String {
        let Some(cluster) = &self.cluster else {
            return self
                .service
                .metrics_text(&self.service.single_parts(), &[&self.service.sessions]);
        };
        let partitions = self.partitions();
        let parts: Vec<_> = partitions
            .iter()
            .map(|(id, p)| (Some(id.as_str()), &**p))
            .collect();
        // The node's table and every partition's open now (cli692's F1 on #1718).
        let open = cluster.parts.load();
        let sessions: Vec<&super::sessions::Sessions> = std::iter::once(&self.service.sessions)
            .chain(open.values().map(|s| &s.sessions))
            .map(|s| &**s)
            .collect();
        self.service.metrics_text(&parts, &sessions)
    }

    /// A cluster node's `Admin.Backup` or `BackupDownload` (D317 ticket 8b): of a partition
    /// this node holds, named by the request, and never forwarded. A backup's bytes don't
    /// take a hop: `phdb cluster backup` asks each partition's owner from the map.
    async fn backup(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<ServiceReply, ApiError> {
        drop(self.service.admit(&op.admit(), caller, prepaid).await?);
        let named = |partition: &str| {
            if partition.is_empty() {
                // Even on a node with one partition, so it stays the same under 8a.
                return Err(ApiError::invalid(
                    "a cluster node's backup names its partition (`partition`)",
                ));
            }
            self.partition(partition)
        };
        let map_version = cluster
            .node
            .routes()
            .as_ref()
            .map_or(0, |r| r.map_version());
        match op {
            ServiceOp::Backup(r) => {
                let part = named(&r.partition)?;
                self.service
                    .backup(&part, r)
                    .await
                    .map(ServiceReply::Backup)
            }
            // A closed partition's file as it lies, for a relocation by copy (D317 ticket 6).
            ServiceOp::BackupDownload(r) if r.fenced_file.is_some() => {
                if r.partition.is_empty() {
                    return Err(ApiError::invalid(
                        "a fenced file names its partition (`partition`)",
                    ));
                }
                let name = r.fenced_file.as_deref().unwrap_or_default();
                let (path, file) = cluster.node.closed_file(&r.partition, name)?;
                self.service
                    .fenced_download(path, file, r)
                    .await
                    .map(ServiceReply::Download)
            }
            ServiceOp::BackupDownload(r) => {
                let part = named(&r.partition)?;
                self.service
                    .backup_download(&part, r, Some(map_version))
                    .await
                    .map(ServiceReply::Download)
            }
            _ => Err(ApiError::internal("not a backup")),
        }
    }

    /// Answers `op` for `caller`, wherever its partition lives.
    pub(crate) async fn dispatch(
        &self,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<ServiceReply, ApiError> {
        self.dispatch_by(op, caller, prepaid, None).await
    }

    /// [`dispatch`](Self::dispatch) with the request's deadline, from its arrival: a forward
    /// takes what's left of it (cli692's F2 on ticket 2); without one, the request timeout
    /// from now.
    async fn dispatch_by(
        &self,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<tokio::sync::OwnedSemaphorePermit>,
        deadline: Option<std::time::Instant>,
    ) -> Result<ServiceReply, ApiError> {
        if let Some(cluster) = &self.cluster {
            // The schema is the map's (ticket 5).
            if matches!(
                op,
                ServiceOp::ListTables(_)
                    | ServiceOp::DescribeTable(_)
                    | ServiceOp::CreateTable(_)
                    | ServiceOp::DropTable(_)
            ) {
                let deadline = deadline.map_or_else(
                    || tokio::time::Instant::now() + self.service.request_timeout(),
                    tokio::time::Instant::from_std,
                );
                return self
                    .cluster_schema(cluster, op, caller, prepaid, deadline)
                    .await;
            }
            // A read across a table's partitions fans out (D317 tickets 4a and 4b); a
            // subscription follows every partition it covers (ticket 7). One at a snapshot
            // reads its partition only (ticket 3): a multi-get is routed and forwarded as a
            // single-key call is, a scan streams that partition alone, and a nearest search
            // or traversal is refused by `route()`.
            let fans = match &op {
                ServiceOp::MultiGet(r) => snapshot_of(r.selection.as_ref()).is_none(),
                ServiceOp::Nearest(r) => r.snapshot_id.is_empty(),
                ServiceOp::Traverse(r) => r.snapshot_id.is_empty(),
                _ => false,
            };
            match &op {
                _ if fans => {
                    let deadline = deadline.map_or_else(
                        || tokio::time::Instant::now() + self.service.request_timeout(),
                        tokio::time::Instant::from_std,
                    );
                    return match op {
                        ServiceOp::Nearest(_) => {
                            self.fan_nearest(cluster, op, caller, prepaid, deadline)
                                .await
                        }
                        ServiceOp::Traverse(_) => {
                            self.fan_traverse(cluster, op, caller, prepaid, deadline)
                                .await
                        }
                        _ => {
                            self.fan_multi_get(cluster, op, caller, prepaid, deadline)
                                .await
                        }
                    };
                }
                ServiceOp::Scan(_) => return self.fan_scan(cluster, op, caller, prepaid).await,
                ServiceOp::Subscribe(_) => {
                    return self.subscribe(cluster, op, caller, prepaid).await;
                }
                // Served where the partition is, never forwarded (D317 ticket 8b).
                ServiceOp::Backup(_) | ServiceOp::BackupDownload(_) => {
                    return self.backup(cluster, op, caller, prepaid).await;
                }
                // This node's own, every partition it holds labelled (D317 ticket 8b).
                ServiceOp::Metrics(_) => {
                    drop(self.service.admit(&op.admit(), caller, prepaid).await?);
                    return Ok(ServiceReply::Metrics(super::proto::MetricsResponse {
                        text: self.metrics_text(),
                    }));
                }
                _ => {}
            }
            return match cluster.target(&op)? {
                Place::Here(service) => service.call(op, caller, prepaid).await,
                Place::There(owner) => {
                    let deadline = deadline.map_or_else(
                        || tokio::time::Instant::now() + self.service.request_timeout(),
                        tokio::time::Instant::from_std,
                    );
                    self.forward(cluster, op, caller, prepaid, owner, deadline)
                        .await
                }
            };
        }
        self.service.call(op, caller, prepaid).await
    }

    /// Forwards `op` to `owner`, the node serving its partition. The entry node's own checks
    /// run first (scope, size, the in-flight budget, held until the owner answers). A
    /// `WrongPartition` answer means the owner's map is newer: this node pulls it (awaited,
    /// single-flight) and routes once more; a second one goes to the client. The forwarded
    /// node never forwards on.
    async fn forward(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<tokio::sync::OwnedSemaphorePermit>,
        owner: Target,
        deadline: tokio::time::Instant,
    ) -> Result<ServiceReply, ApiError> {
        // One deadline for the whole forward, from the request's arrival: the budget's wait,
        // a send, and after a WrongPartition the pull and the second send (cli692's F2).
        let left = || {
            deadline
                .checked_duration_since(tokio::time::Instant::now())
                .filter(|d| !d.is_zero())
                .ok_or_else(ApiError::deadline_exceeded)
        };
        let budget = self.service.admit(&op.admit(), caller, prepaid).await?;
        let writes = forward_writes(&op);
        let req = ForwardRequest {
            fanout_part: false,
            map_version: owner.map_version,
            caller: Some(forward::forwarded_caller(caller)?),
            op: Some(forward::op_to_forward(op)?),
        };
        // For the rare resend. Cheap: the generated messages hold their bytes as `Bytes`
        // (protogen's `config.bytes`), so this copies a request's structure, not its values.
        let again = req.clone();
        let reply = cluster
            .forwarder
            .send(&owner.address, req, left()?, writes)
            .await;
        let reply = match reply {
            Err(e) if e.code == "WrongPartition" => {
                tokio::time::timeout(
                    left()?,
                    cluster
                        .node
                        .pull_now(&owner.node, &owner.address, owner.map_version),
                )
                .await
                .map_err(|_| ApiError::deadline_exceeded())?
                .map_err(|why| {
                    ApiError::partition_unavailable(format!(
                        "node {} has a newer cluster map, which this node couldn't take: \
                             {why}",
                        owner.node
                    ))
                })?;
                let op = forward::op_of(again.op.clone())?;
                match cluster.target(&op)? {
                    // The newer map gives the key to this node.
                    Place::Here(service) => return service.call(op, caller, Some(budget)).await,
                    Place::There(next) => {
                        let mut again = again;
                        again.map_version = next.map_version;
                        cluster
                            .forwarder
                            .send(&next.address, again, left()?, writes)
                            .await
                    }
                }
            }
            reply => reply,
        };
        drop(budget);
        let reply = forward::reply_of(reply?)?;
        // A read's reply is charged here too, for this node's own client: its front-end
        // holds the charge until the reply is written (#1567, cli692's F1 on ticket 2).
        match reply {
            ServiceReply::Get(_) | ServiceReply::ReadRow(_) | ServiceReply::MultiGet(_) => {
                self.service.charge_built(caller, reply).await
            }
            reply => Ok(reply),
        }
    }

    /// `Feed.Subscribe` on a cluster node (D317 ticket 7): one stream over every partition
    /// the subscription covers, after this node's own checks (scope and size; the in-flight
    /// bytes are the request's, freed once it's read, as on a single node).
    async fn subscribe(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<ServiceReply, ApiError> {
        let budget = self.service.admit(&op.admit(), caller, prepaid).await?;
        drop(budget);
        let ServiceOp::Subscribe(r) = op else {
            return Err(ApiError::internal("not a subscription"));
        };
        super::cluster::feed::subscribe(
            Arc::clone(&cluster.node),
            Arc::clone(&cluster.forwarder),
            Arc::clone(&cluster.parts),
            Arc::clone(&self.service),
            r,
            caller,
        )
        .await
        .map(ServiceReply::Feed)
    }

    /// `Cluster.ForwardSubscribe` here (D317 ticket 7): one partition's part of a cluster
    /// subscription that another node entered.
    pub(crate) async fn serve_forwarded_subscribe(
        &self,
        req: super::proto::ForwardSubscribeRequest,
    ) -> Result<super::feed::FeedEvents, ApiError> {
        let Some(cluster) = &self.cluster else {
            return Err(ApiError::unsupported("this node isn't a cluster node"));
        };
        super::cluster::feed::serve(&cluster.node, &cluster.parts, req).await
    }

    /// A call another node forwarded here (D317 ticket 2): served only if this node's map
    /// gives it the key, otherwise `WrongPartition` with this node's map version. Never
    /// forwarded on, so a split view of the map can't bounce a call between nodes.
    /// `fanout_part`: one partition's part of a read the sending node fans out (#1715). That
    /// node holds the budget for its page, so a multi-get's or nearest search's part is
    /// served here with none ([`Service::fanout_part`]): an owner never waits on budget
    /// another node's holdings could fill, so fan-outs through several nodes can't wait on
    /// one another (perf287's g78). Only peers call this (`Cluster.Forward` takes cluster
    /// scope), and the `Cluster` calls' concurrency limit bounds how many run at once.
    pub(crate) async fn serve_forwarded(
        &self,
        op: ServiceOp,
        caller: &Caller,
        fanout_part: bool,
    ) -> Result<ServiceReply, ApiError> {
        let Some(cluster) = &self.cluster else {
            return Err(ApiError::unsupported("this node isn't a cluster node"));
        };
        if matches!(op, ServiceOp::CreateTable(_) | ServiceOp::DropTable(_)) {
            return self.serve_forwarded_schema(cluster, op, caller).await;
        }
        let service = {
            let routes = cluster.node.routes();
            let Some(routes) = routes.as_ref() else {
                return Err(ApiError::wrong_partition(
                    0,
                    "this node has no cluster map yet",
                ));
            };
            let p = route(routes, &op)?;
            let mine = routes
                .owner(&p)
                .is_some_and(|o| o.node == cluster.node.id());
            if !mine {
                // The map moved the partition away, or never gave it here: the sender's map
                // is older or newer than this node's; it pulls and routes again.
                return Err(ApiError::wrong_partition(
                    routes.map_version(),
                    format!("this node doesn't serve partition {p:?}"),
                ));
            }
            cluster.here(&p).ok_or_else(|| not_open(&p))?
        };
        match op {
            ServiceOp::MultiGet(_) | ServiceOp::Nearest(_) if fanout_part => {
                service.fanout_part(op, caller).await
            }
            op => service.call(op, caller, None).await,
        }
    }

    /// [`dispatch`](Self::dispatch) once a replica has applied the client's
    /// `x-pigeonhole-min-primary-seqno` (#1577), `min_primary_seqno` being the header as
    /// parsed. Every front-end's calls that may carry it come here, so they can't drift
    /// apart (cli692's N1 on #1594).
    pub(crate) async fn dispatch_after_primary(
        &self,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<tokio::sync::OwnedSemaphorePermit>,
        min_primary_seqno: Result<Option<u64>, ApiError>,
        deadline: ReadDeadline,
    ) -> Result<ServiceReply, ApiError> {
        let at = deadline.at;
        self.service
            .await_primary(&op, min_primary_seqno?, deadline)
            .await?;
        self.dispatch_by(op, caller, prepaid, Some(at)).await
    }
}

/// Where a routed call goes when it isn't this node's.
pub(crate) struct Target {
    node: String,
    address: String,
    map_version: u64,
}

/// Where a routed call goes: a partition's service here, or the node serving it.
pub(crate) enum Place {
    Here(Arc<Service>),
    There(Target),
}

impl Cluster {
    /// Where `op`'s partition is served: here, or the node serving it.
    fn target(&self, op: &ServiceOp) -> Result<Place, ApiError> {
        let routes = self.node.routes();
        let Some(routes) = routes.as_ref() else {
            return Err(ApiError::partition_unavailable(
                "this cluster node has no map yet",
            ));
        };
        let partition = route(routes, op)?;
        let owner = routes.owner(&partition).ok_or_else(|| {
            ApiError::internal(format!("the map lists no node for partition {partition:?}"))
        })?;
        // Served here only when the map gives this node the partition *and* this node has
        // it open: a partition the map moved away is the new owner's, even if its file is
        // still open here.
        if owner.node == self.node.id() {
            return self
                .here(&partition)
                .map(Place::Here)
                .ok_or_else(|| not_open(&partition));
        }
        Ok(Place::There(Target {
            node: owner.node.clone(),
            address: owner.address.clone(),
            map_version: routes.map_version(),
        }))
    }
}

/// The partition `op` goes to on a cluster node: a single-key call's, a batch's when all
/// its rows are in one, and a transaction's or snapshot's (D317 ticket 3): its `route` when
/// it begins, then the partition its id names, with every row it reads or writes checked to
/// be in it (`CrossPartition`). The owner routes a forwarded call again, so it checks too.
/// Everything else is refused until its D317 ticket.
pub(crate) fn route(routes: &Routes, op: &ServiceOp) -> Result<String, ApiError> {
    let one = |table: &str, row: &[u8]| {
        routes
            .partition(table, row)
            .map(str::to_owned)
            .ok_or_else(|| ApiError::not_found(format!("table {table:?} isn't in the cluster map")))
    };
    let writes = |w: &Option<RowWrites>| {
        let key = w
            .as_ref()
            .and_then(|w| w.row.as_ref())
            .ok_or_else(|| ApiError::invalid("the writes name no row"))?;
        one(&key.table, &key.row)
    };
    // A session's partition, from its id; a row of `table` must be in it.
    let session = |id: &str, what: &str| session_partition(routes, id, what);
    let within = |p: String, what: &str, table: &str, row: &[u8]| {
        let q = one(table, row)?;
        if q == p {
            Ok(p)
        } else {
            Err(outside(what, &p, &q, table, row))
        }
    };
    let rows_within = |p: String, what: &str, rows: &[RowWrites]| {
        for w in rows {
            let key = w
                .row
                .as_ref()
                .ok_or_else(|| ApiError::invalid("the writes name no row"))?;
            let q = one(&key.table, &key.row)?;
            if q != p {
                return Err(outside(what, &p, &q, &key.table, &key.row));
            }
        }
        Ok(p)
    };
    match op {
        ServiceOp::Get(r) if !r.snapshot_id.is_empty() => {
            let p = session(&r.snapshot_id, "snapshot")?;
            within(p, "the snapshot", &r.table, &r.row)
        }
        ServiceOp::Get(r) => one(&r.table, &r.row),
        ServiceOp::ReadRow(r) => match snapshot_of(r.selection.as_ref()) {
            Some(id) => within(session(id, "snapshot")?, "the snapshot", &r.table, &r.row),
            None => one(&r.table, &r.row),
        },
        ServiceOp::Mutate(r) | ServiceOp::MutateReturning(r) => writes(&r.writes),
        ServiceOp::CheckAndMutate(r) => writes(&r.writes),
        ServiceOp::Batch(r) => {
            // Every row's partition, and the first row in each, before anything is sent.
            let mut parts: Vec<(String, String)> = Vec::new();
            for w in &r.rows {
                let key = w
                    .row
                    .as_ref()
                    .ok_or_else(|| ApiError::invalid("a batch's writes name no row"))?;
                let p = one(&key.table, &key.row)?;
                if !parts.iter().any(|(q, _)| *q == p) {
                    // Escaped, so a binary key reads unambiguously (cli692's nit).
                    let first = format!("{}/{}", key.table, key.row.escape_ascii());
                    parts.push((p, first));
                }
            }
            match parts.len() {
                0 => Err(ApiError::invalid("an empty batch")),
                1 => Ok(parts.remove(0).0),
                _ => Err(ApiError::cross_partition(format!(
                    "the batch's rows are in {} partitions, and a batch commits in one: {}",
                    parts.len(),
                    parts
                        .iter()
                        .map(|(p, row)| format!("{p} (from {row})"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))),
            }
        }
        ServiceOp::Begin(r) => begins(routes, r.route.as_ref(), "a transaction"),
        ServiceOp::OpenSnapshot(r) => begins(routes, r.route.as_ref(), "a snapshot"),
        ServiceOp::TxnGet(r) => {
            let p = session(&r.txn_id, "transaction")?;
            within(p, "the transaction", &r.table, &r.row)
        }
        ServiceOp::TxnGetRow(r) => {
            let p = session(&r.txn_id, "transaction")?;
            within(p, "the transaction", &r.table, &r.row)
        }
        ServiceOp::TxnWrite(r) => rows_within(
            session(&r.txn_id, "transaction")?,
            "the transaction",
            &r.rows,
        ),
        ServiceOp::Commit(r) => rows_within(
            session(&r.txn_id, "transaction")?,
            "the transaction",
            &r.rows,
        ),
        ServiceOp::Abort(r) => session(&r.txn_id, "transaction"),
        ServiceOp::ReleaseSnapshot(r) => session(&r.snapshot_id, "snapshot"),
        // A read at a snapshot stays in the snapshot's partition (cli692's S3 on ticket 3).
        // Without one, a client's fans out before it gets here (ticket 4a), and this is one
        // partition's keys of a multi-get another node fans out: every key in it.
        ServiceOp::MultiGet(r) => match snapshot_of(r.selection.as_ref()) {
            Some(id) => {
                let p = session(id, "snapshot")?;
                for k in &r.keys {
                    let q = one(&r.table, k)?;
                    if q != p {
                        return Err(outside("the snapshot", &p, &q, &r.table, k));
                    }
                }
                Ok(p)
            }
            None => {
                let mut parts = r.keys.iter().map(|k| one(&r.table, k));
                match parts.next() {
                    None => Err(ApiError::invalid("a forwarded multi-get with no keys")),
                    Some(first) => {
                        let first = first?;
                        for p in parts {
                            if p? != first {
                                return Err(ApiError::invalid(
                                    "a forwarded multi-get's keys are in more than one partition",
                                ));
                            }
                        }
                        Ok(first)
                    }
                }
            }
        },
        // A scan fans out before it gets here (ticket 4a); one at a snapshot reads only the
        // snapshot's partition, which this checks.
        ServiceOp::Scan(r) => match snapshot_of(r.selection.as_ref()) {
            Some(id) => {
                let p = session(id, "snapshot")?;
                let span = scan_span(routes, r)?;
                match span.iter().find(|q| **q != p) {
                    None => Ok(p),
                    Some(q) => Err(ApiError::cross_partition(format!(
                        "the scan of table {:?} reaches partition {q}, and the snapshot is \
                         partition {p}'s: a snapshot reads one partition",
                        r.table
                    ))),
                }
            }
            None => Err(later("a read across partitions", 4)),
        },
        ServiceOp::Nearest(r) if !r.snapshot_id.is_empty() => Err(ApiError::unsupported(
            "a nearest-neighbour search at a snapshot isn't supported on a cluster",
        )),
        ServiceOp::Traverse(r) if !r.snapshot_id.is_empty() => Err(ApiError::unsupported(
            "a traversal at a snapshot isn't supported on a cluster",
        )),
        // One partition's part of a `Nearest` another node fans out (D317 ticket 4b): its
        // keys, or a range clipped to the partition. A client's fans out before it gets here.
        ServiceOp::Nearest(r) => nearest_partition(routes, r),
        ServiceOp::Traverse(_) => Err(ApiError::internal(
            "a fanned-out read isn't routed to one partition",
        )),
        // An index lives in one partition's file, and no D317 ticket routes a search to it
        // (#1722, 1.3.0).
        ServiceOp::VectorSearch(_) | ServiceOp::TextSearch(_) => Err(ApiError::unsupported(
            "indexed vector and text search isn't supported on a cluster in this release: use \
             Vectors.Nearest, which fans out across the partitions",
        )),
        ServiceOp::ListTables(_)
        | ServiceOp::CreateTable(_)
        | ServiceOp::DropTable(_)
        | ServiceOp::DescribeTable(_) => Err(ApiError::internal(
            "a schema call is answered from the cluster map, not routed to a partition",
        )),
        // A cluster subscription is routed before this ([`Router::subscribe`]).
        ServiceOp::Subscribe(_) => Err(ApiError::internal("a subscription isn't routed by key")),
        // A backup is served before this ([`Router::backup`]).
        ServiceOp::Backup(_) | ServiceOp::BackupDownload(_) => {
            Err(ApiError::internal("a backup isn't routed by key"))
        }
        _ => Err(ApiError::unsupported(
            "this call isn't served by a cluster node (D317)",
        )),
    }
}

/// The partition a forwarded `Nearest` names: all its keys', or its clipped range's, which
/// must lie within one partition (the fanning-out node clipped it).
fn nearest_partition(
    routes: &Routes,
    r: &super::proto::NearestRequest,
) -> Result<String, ApiError> {
    use super::proto::nearest_request::Source;
    let one = |row: &[u8]| {
        routes
            .partition(&r.table, row)
            .map(str::to_owned)
            .ok_or_else(|| {
                ApiError::not_found(format!("table {:?} isn't in the cluster map", r.table))
            })
    };
    let outside_one =
        || ApiError::invalid("a forwarded nearest search reaches outside one partition");
    match &r.source {
        Some(Source::Keys(list)) => {
            let mut parts = list.keys.iter().map(|k| one(k));
            let first = parts
                .next()
                .ok_or_else(|| ApiError::invalid("a forwarded nearest search with no keys"))??;
            for p in parts {
                if p? != first {
                    return Err(outside_one());
                }
            }
            Ok(first)
        }
        Some(Source::Range(range)) => {
            let Some(super::proto::key_range::Range::Bounds(b)) = &range.range else {
                return Err(ApiError::invalid(
                    "a forwarded nearest search's range is bounds",
                ));
            };
            let lo = b.start.as_ref().map_or(&[][..], |s| &s.key[..]);
            let hi = b.end.as_ref().map(|e| (&e.key[..], e.inclusive));
            let span = routes.span(&r.table, lo, hi).ok_or_else(|| {
                ApiError::not_found(format!("table {:?} isn't in the cluster map", r.table))
            })?;
            match span.as_slice() {
                [p] => Ok((*p).to_owned()),
                _ => Err(outside_one()),
            }
        }
        None => Err(ApiError::invalid(
            "a forwarded nearest search names no source",
        )),
    }
}

/// A read's snapshot, if it names one.
pub(crate) fn snapshot_of(s: Option<&Selection>) -> Option<&str> {
    s.map(|s| s.snapshot_id.as_str())
        .filter(|id| !id.is_empty())
}

/// Where a transaction or snapshot begins on a cluster: its `route`, which every cluster
/// requires, one partition or many (cli692's S2 on ticket 3), so a client doesn't break
/// when the map gains one.
fn begins(routes: &Routes, key: Option<&RoutingKey>, what: &str) -> Result<String, ApiError> {
    match key.and_then(|k| k.key.as_ref()) {
        Some(routing_key::Key::Row(k)) => routes
            .partition(&k.table, &k.row)
            .map(str::to_owned)
            .ok_or_else(|| {
                ApiError::not_found(format!("table {:?} isn't in the cluster map", k.table))
            }),
        Some(routing_key::Key::Partition(p)) => {
            if routes.owner(p).is_some() {
                Ok(p.clone())
            } else {
                Err(ApiError::not_found(format!(
                    "the cluster map has no partition {p:?}"
                )))
            }
        }
        None => Err(ApiError::invalid(format!(
            "{what} on a cluster names where it runs: `route`, a row or a partition"
        ))),
    }
}

/// The partition a session id names (`<partition>:<32 hex>`). Any other form, or a
/// partition the map doesn't list, is `NotFound`, as an unknown id is on a single node.
fn session_partition(routes: &Routes, id: &str, what: &str) -> Result<String, ApiError> {
    super::sessions::partition_of(id)
        .map(|(p, _)| p)
        .filter(|p| routes.owner(p).is_some())
        .map(str::to_owned)
        .ok_or_else(|| ApiError::not_found(format!("no such {what}")))
}

/// A row outside a session's partition (D317 ticket 3).
fn outside(what: &str, session: &str, row_partition: &str, table: &str, row: &[u8]) -> ApiError {
    ApiError::cross_partition(format!(
        "row {table}/{} is in partition {row_partition}, and {what} is partition {session}'s: \
         a transaction or snapshot stays in one partition",
        row.escape_ascii()
    ))
}

/// The partitions a scan's range reaches.
fn scan_span<'a>(routes: &'a Routes, r: &ScanRequest) -> Result<Vec<&'a str>, ApiError> {
    use super::proto::key_range::Range;
    let (lo, hi): (&[u8], Option<(Vec<u8>, bool)>) =
        match r.range.as_ref().and_then(|k| k.range.as_ref()) {
            None => (&[], None),
            Some(Range::Prefix(p)) => (p, prefix_end(p).map(|e| (e, false))),
            Some(Range::Bounds(b)) => (
                b.start.as_ref().map_or(&[][..], |s| &s.key[..]),
                b.end.as_ref().map(|e| (e.key.to_vec(), e.inclusive)),
            ),
        };
    routes
        .span(
            &r.table,
            lo,
            hi.as_ref().map(|(k, inc)| (k.as_slice(), *inc)),
        )
        .ok_or_else(|| ApiError::not_found(format!("table {:?} isn't in the cluster map", r.table)))
}

/// The first key after every key starting with `prefix`; `None` when there is none (all
/// `0xff`, or empty).
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// Whether a forward of `op` that fails after it was sent leaves an outcome unknown (B3):
/// a write, and a transaction's buffered writes, commit and abort (ticket 3).
fn forward_writes(op: &ServiceOp) -> bool {
    // `TxnWrite` needs write scope, so `writes()` has it; a `Commit` without rows commits
    // the buffered ones.
    op.writes() || matches!(op, ServiceOp::Commit(_) | ServiceOp::Abort(_))
}

/// A partition the map gives this node that it doesn't have open: it opens partitions at
/// start, and moving one in is D317's ticket 6.
pub(crate) fn not_open(partition: &str) -> ApiError {
    ApiError::partition_unavailable(format!(
        "the map gives this node partition {partition:?}, but it isn't open here: a node \
         opens its partitions at start, and relocation comes with D317's ticket 6"
    ))
}

/// A call a later D317 ticket brings to a cluster.
pub(crate) fn later(what: &str, ticket: u32) -> ApiError {
    ApiError::unsupported(format!(
        "{what} in a cluster comes with D317's ticket {ticket}"
    ))
}
