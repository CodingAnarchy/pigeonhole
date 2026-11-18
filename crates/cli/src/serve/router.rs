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
use super::cluster::route::{Routes, prefix_end};
use super::cluster::spanning;
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
    /// `--memory` on a cluster node (#1915): what each partition the map places the memory
    /// store on opens its store with, once the map does.
    pub(crate) memory: Option<Arc<super::memslot::Recipe>>,
    /// This node's replica copies of partitions other nodes own (D328), by partition id:
    /// apart from `parts`, so nothing that serves or maintains an owner's partition sees
    /// one.
    pub(crate) replicas: Replicas,
    /// Replicas a routed read found behind or unreachable (D328), by (node, partition), and
    /// when: skipped for [`REPLICA_BACKOFF`] after, so reads don't each pay for the miss.
    pub(crate) replica_backoff:
        Arc<std::sync::Mutex<std::collections::HashMap<(String, String), std::time::Instant>>>,
    /// The spanning transactions this node coordinates, and the read-only holds its
    /// partitions keep for others' (D329).
    pub(crate) spanning: Arc<spanning::Spanning>,
}

impl Cluster {
    /// A spanning transaction's coordinator for `op` at its home, `home` (D329): the call's
    /// transaction, client and deadline, with handles to reach its branches.
    pub(crate) fn driver(
        &self,
        home_service: &Service,
        op: &ServiceOp,
        caller: &Caller,
        deadline: tokio::time::Instant,
    ) -> Result<spanning::Driver, ApiError> {
        let home = home_service
            .gate()
            .map(|g| g.partition().to_owned())
            .ok_or_else(|| ApiError::internal("a spanning transaction off a cluster"))?;
        let xid = match op {
            ServiceOp::TxnGet(r) => r.txn_id.as_str(),
            ServiceOp::TxnGetRow(r) => r.txn_id.as_str(),
            ServiceOp::TxnWrite(r) => r.txn_id.as_str(),
            ServiceOp::Commit(r) => r.txn_id.as_str(),
            ServiceOp::Abort(r) => r.txn_id.as_str(),
            _ => "",
        };
        Ok(spanning::Driver {
            node: Arc::clone(&self.node),
            parts: Arc::clone(&self.parts),
            forwarder: Arc::clone(&self.forwarder),
            spanning: Arc::clone(&self.spanning),
            xid: xid.into(),
            home: home.into(),
            caller: caller.clone(),
            deadline,
            renew_after: home_service.txn_idle() / 2,
            max_branches: home_service.txn_max_branches(),
        })
    }
}

/// The most of what's left of a read's time one replica's attempt takes (simp724's S2 on
/// #2001): half, so a replica that is slow, rather than one that refuses, leaves the next
/// one or the owner the other half.
pub(crate) fn replica_attempt(left: std::time::Duration) -> std::time::Duration {
    left / 2
}

/// How long a routed read skips a replica it found behind or unreachable (D328).
const REPLICA_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);

/// A cluster node's replica copy of a partition (D328): a read-only service over its
/// engine, and the follower that applies the owner's feed to it.
#[derive(Debug, Clone)]
pub(crate) struct ReplicaCopy {
    pub(crate) service: Arc<Service>,
    pub(crate) follower: Arc<super::replica::Replica>,
}

/// [`Cluster::replicas`]: swapped whole as a copy opens or closes.
pub(crate) type Replicas = Arc<arc_swap::ArcSwap<std::collections::BTreeMap<String, ReplicaCopy>>>;

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

    /// The service of `partition`, which a peer sent this node a call for, by `routes`: the
    /// owner's side of a forward (a call's, a scan's). `WrongPartition` with this node's map
    /// version when the map doesn't give it here (it moved away, or was never here: the
    /// sender's map is older or newer than this node's, and it pulls and routes again), and
    /// [`not_open`] when the map gives it here but it isn't open.
    pub(crate) fn owned_here(
        &self,
        routes: &Routes,
        partition: &str,
    ) -> Result<Arc<Service>, ApiError> {
        let mine = routes
            .owner(partition)
            .is_some_and(|o| o.node == self.node.id());
        if !mine {
            return Err(ApiError::wrong_partition(
                routes.map_version(),
                format!("this node doesn't serve partition {partition:?}"),
            ));
        }
        self.here(partition).ok_or_else(|| not_open(partition))
    }
}

/// Where `partition` is served, by `routes`, as an entry node sees it: here, or the node the
/// map gives it. Here only when the map gives this node the partition *and* this node has it
/// open: a partition the map moved away is the new owner's, even if its file is still open
/// here. One copy for a routed call ([`Cluster::target`]) and a fanned-out read's part
/// (`cluster::fanout`), which holds the node and its parts without the [`Cluster`].
///
/// A partition the map doesn't list is `WrongPartition` with the map's version (#1896): a
/// fanned-out part named it from an older map, and a newer map dropped it since. The
/// caller re-reads the map and routes again.
pub(crate) fn place(
    node: &super::cluster::Node,
    parts: &Parts,
    routes: &Routes,
    partition: &str,
) -> Result<Place, ApiError> {
    let owner = routes.owner(partition).ok_or_else(|| {
        ApiError::wrong_partition(
            routes.map_version(),
            format!("the map no longer lists partition {partition:?}: a newer map dropped it"),
        )
    })?;
    if owner.node == node.id() {
        return parts
            .load()
            .get(partition)
            .map(|s| Place::Here(Arc::clone(s)))
            .ok_or_else(|| not_open(partition));
    }
    Ok(Place::There(Target {
        node: owner.node.clone(),
        address: owner.address.clone(),
        map_version: routes.map_version(),
    }))
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
                memory: None,
                replicas: Arc::default(),
                replica_backoff: Arc::default(),
                spanning: spanning::Spanning::new(),
            }),
        }
    }

    /// `--memory` on a cluster node (#1915): the recipe its partitions' stores open with.
    pub(crate) fn memory_recipe(mut self, recipe: Option<super::memslot::Recipe>) -> Self {
        if let Some(c) = self.cluster.as_mut() {
            c.memory = recipe.map(Arc::new);
        }
        self
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
        partition: &str,
    ) -> Result<super::proto::PartitionUploadStatusReply, ApiError> {
        self.uploads_for(caller)?.status(upload_id, partition)
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

    /// The memory store's slot of this node's first partition, if the node serves one
    /// (`--memory`): its name, model and limits are the node's, the same on every partition.
    pub(crate) fn memory_slot(&self) -> Option<Arc<super::memslot::MemSlot>> {
        self.parts().first()?.memory.get().cloned()
    }

    /// A cluster node's replica copies open now (D328); none on a single node.
    pub(crate) fn replica_copies(&self) -> Vec<ReplicaCopy> {
        self.cluster
            .as_ref()
            .map(|c| c.replicas.load().values().cloned().collect())
            .unwrap_or_default()
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
        let mut text = self.service.metrics_text(&parts, &sessions);
        // Each replica copy's follower, labelled by its partition (D328).
        let copies = cluster.replicas.load();
        let followers: Vec<(&str, &super::replica::Replica)> = copies
            .iter()
            .map(|(id, c)| (id.as_str(), &*c.follower))
            .collect();
        super::replica::Replica::render_copies(&followers, &mut text);
        text
    }

    /// A cluster node's `Admin.Info`, `Flush` or `Compact` (ICR 0117, #1852), answered here
    /// for the partitions this node holds and never forwarded, as a backup is. `Info` lists
    /// each partition the map gives this node, with its state as `Cluster.Status` reports it,
    /// and the map's other partitions. `Flush` and `Compact` act on every partition the map
    /// gives this node that's open here (serving or fenced: a fenced partition's files still
    /// change), one after another, trying each, or on the one the request names.
    async fn cluster_admin(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<ServiceReply, ApiError> {
        drop(self.service.admit(&op.admit(), caller, prepaid).await?);
        let (r, compact) = match op {
            ServiceOp::Info(_) => return self.cluster_info(cluster).await.map(ServiceReply::Info),
            ServiceOp::Flush(r) => (r, false),
            ServiceOp::Compact(r) => (r, true),
            _ => return Err(ApiError::internal("not a maintenance call")),
        };
        let parts = if r.partition.is_empty() {
            // The partitions the map gives this node, as `Info` lists them: not a copy the map
            // moved away that's still open here, fenced until it's closed (simp724's N1).
            let routes = cluster.node.routes();
            let mine: std::collections::BTreeSet<&str> = routes
                .as_ref()
                .map(|r| r.partitions_of(cluster.node.id()).collect())
                .unwrap_or_default();
            let mut parts = self.partitions();
            parts.retain(|(id, _)| mine.contains(id.as_str()));
            parts.sort_by(|a, b| a.0.cmp(&b.0));
            parts
        } else {
            vec![(r.partition.clone(), self.partition(&r.partition)?)]
        };
        // Every one is tried, so one that fails doesn't leave the rest undone; the answer is
        // the first failure's code, naming every partition that failed (simp724's N2).
        let mut failed: Vec<ApiError> = Vec::new();
        for (id, part) in parts {
            let done = if compact {
                part.db.compact_async().await
            } else {
                part.db.flush_async().await
            };
            if let Err(e) = done {
                let mut e = ApiError::from(e);
                e.message = format!("partition {id}: {}", e.message);
                failed.push(e);
            }
        }
        match failed.len() {
            0 => Ok(ServiceReply::Empty(super::proto::Empty {})),
            1 => Err(failed.remove(0)),
            _ => {
                let all: Vec<String> = failed.iter().map(|e| e.message.clone()).collect();
                let mut first = failed.remove(0);
                first.message = all.join("; ");
                Err(first)
            }
        }
    }

    /// `Admin.Info` on a cluster node (#1852): see [`cluster_admin`](Self::cluster_admin).
    async fn cluster_info(
        &self,
        cluster: &Cluster,
    ) -> Result<super::proto::InfoResponse, ApiError> {
        let routes = cluster.node.routes();
        let Some(routes) = routes.as_ref() else {
            return Err(ApiError::partition_unavailable(
                "this cluster node has no map yet",
            ));
        };
        let me = cluster.node.id();
        let mut ids: Vec<&str> = routes.partitions_of(me).collect();
        ids.sort_unstable();
        let open: std::collections::BTreeMap<String, PartRef> =
            self.partitions().into_iter().collect();
        let (mut partitions, mut file_bytes, mut free_bytes) = (Vec::new(), 0u64, None);
        for id in ids {
            let (state, reason) = cluster.node.local_state(id);
            let mut p = super::proto::PartitionInfo {
                id: id.to_owned(),
                path: cluster
                    .node
                    .dir()
                    .join(format!("{id}.phdb"))
                    .display()
                    .to_string(),
                ..Default::default()
            };
            p.set_state(state);
            p.set_fence_reason(reason);
            p.awaiting_peers = cluster.node.awaiting_peers(id);
            p.lease_remaining_ms =
                u64::try_from(cluster.node.lease_remaining(id).as_millis()).unwrap_or(u64::MAX);
            if let Some(part) = open.get(id) {
                let db = part.db.clone();
                let info = self.service.blocking(move || Ok(db.info()?)).await?;
                p.path = part.db.path().display().to_string();
                p.file_bytes = info.file_bytes;
                p.tables = part.0.user_tables();
                file_bytes += info.file_bytes;
                free_bytes = free_bytes.or(info.free_bytes);
            }
            partitions.push(p);
        }
        // Each replica copy here (D328), with its follower's state; its `state` is unset, as
        // it serves no routed call in this release.
        let copies = cluster.replicas.load_full();
        for (id, copy) in copies.iter() {
            let mut p = super::proto::PartitionInfo {
                id: id.clone(),
                replica: copy.follower.info(),
                ..Default::default()
            };
            if let Some(part) = PartRef::of(&copy.service) {
                let db = part.db.clone();
                let info = self.service.blocking(move || Ok(db.info()?)).await?;
                p.path = part.db.path().display().to_string();
                p.file_bytes = info.file_bytes;
                p.tables = part.0.user_tables();
                file_bytes += info.file_bytes;
                free_bytes = free_bytes.or(info.free_bytes);
            }
            partitions.push(p);
        }
        Ok(super::proto::InfoResponse {
            path: cluster.node.dir().display().to_string(),
            file_bytes,
            free_bytes: free_bytes.unwrap_or(0),
            tables: routes.table_names(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            replica: None,
            partitions,
            other_partitions: routes.partitions_not_of(me),
        })
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

    /// `Replication.Follow` on a cluster node (D328): a replica copy's follower, at the node
    /// its map says owns the partition. The token is opaque, so it's tried on each partition
    /// this node owns and has open, and the one whose database it names serves it (another
    /// database's token is refused by the engine, `InvalidArgument`). None: `WrongPartition`
    /// with this node's map version, so the follower re-reads the map (Q5: never forwarded,
    /// as a long stream through two hops would be).
    async fn follow_owned(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
    ) -> Result<ServiceReply, ApiError> {
        let ServiceOp::Follow(r) = op else {
            return Err(ApiError::internal("not a Follow"));
        };
        let routes = cluster.node.routes();
        let Some(routes) = routes.as_ref() else {
            return Err(ApiError::partition_unavailable(
                "this node has no cluster map yet",
            ));
        };
        let me = cluster.node.id();
        let parts = cluster.parts.load_full();
        for (id, service) in parts.iter() {
            if routes.owner(id).is_none_or(|o| o.node != me) {
                continue;
            }
            match service
                .call(ServiceOp::Follow(r.clone()), caller, None)
                .await
            {
                Err(e) if e.code == "InvalidArgument" && e.message.contains("another database") => {
                }
                answer => return answer,
            }
        }
        Err(ApiError::wrong_partition(
            routes.map_version(),
            "no partition this node owns is the database this token names: follow the \
             partition's owner in a newer map",
        ))
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
    ///
    /// **The large cluster-only futures are boxed** (the map's schema calls, the fan-outs and
    /// the forward, #1902): this future is moved once or more per call, and a future is as
    /// large as the largest it awaits in place. Awaited in place, those made every call's
    /// future, a single node's included, over twice the size of [`Service::call`]'s, and a
    /// change to a fan-out changed what a single node's `Get` cost. A boxed one costs its
    /// path one allocation, beside calls to several nodes.
    ///
    /// **The rest stay in place:** [`Service::call`], which every single-node call takes,
    /// and the cluster's scan fan-out, subscription, backup and admin calls, whose futures
    /// are each smaller than `Service::call`'s, so they don't size this one. A test below
    /// keeps this future within a margin of `Service::call`'s; one of them that outgrows it
    /// is boxed as the others are. The serve callgrind shapes (`serveshapes.rs`) count what
    /// a growth of `Service::call` itself costs.
    pub(crate) async fn dispatch_by(
        &self,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<tokio::sync::OwnedSemaphorePermit>,
        deadline: Option<std::time::Instant>,
    ) -> Result<ServiceReply, ApiError> {
        // A cluster token's replica calls (`Admit::peer`, D328) are one cluster node's of
        // another. A single node has no peers: there, a cluster token would read the whole
        // database through them, so it keeps its scope's reach, which is none (simp724's S1
        // on #1974). Only a cluster-scope caller pays for the check.
        if self.cluster.is_none() && caller.scope == super::auth::Scope::Cluster {
            let admit = op.admit();
            if admit.peer {
                return Err(ApiError::permission_denied(&format!(
                    "call {} on a single node: a cluster token makes it only of a cluster \
                     node, for a replica copy",
                    admit.what
                )));
            }
        }
        if let Some(cluster) = &self.cluster {
            // The schema is the map's (ticket 5).
            if map_schema_change(&op)
                || matches!(op, ServiceOp::ListTables(_) | ServiceOp::DescribeTable(_))
            {
                let deadline = deadline.map_or_else(
                    || tokio::time::Instant::now() + self.service.request_timeout(),
                    tokio::time::Instant::from_std,
                );
                return Box::pin(self.cluster_schema(cluster, op, caller, prepaid, deadline)).await;
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
                            Box::pin(self.fan_nearest(cluster, op, caller, prepaid, deadline, None))
                                .await
                        }
                        ServiceOp::Traverse(_) => {
                            Box::pin(
                                self.fan_traverse(cluster, op, caller, prepaid, deadline, None),
                            )
                            .await
                        }
                        _ => {
                            Box::pin(
                                self.fan_multi_get(cluster, op, caller, prepaid, deadline, None),
                            )
                            .await
                        }
                    };
                }
                ServiceOp::Scan(_) => {
                    return self.fan_scan(cluster, op, caller, prepaid, None).await;
                }
                // A load split by id across the index's partitions (#1722 PR 3).
                ServiceOp::VectorBulkLoad(_) => {
                    return Box::pin(self.fan_bulk_load(cluster, op, caller, prepaid)).await;
                }
                // A search or check across an index's partitions (#1722 PR 1b), and a
                // text index's rebuild (PR 2c).
                ServiceOp::VectorSearch(_)
                | ServiceOp::VectorVerify(_)
                | ServiceOp::TextVerify(_)
                | ServiceOp::TextSearch(_)
                | ServiceOp::TextRebuild(_) => {
                    let deadline = deadline.map_or_else(
                        || tokio::time::Instant::now() + self.service.request_timeout(),
                        tokio::time::Instant::from_std,
                    );
                    return Box::pin(self.fan_index(cluster, op, caller, prepaid, deadline)).await;
                }
                // A memory call across partitions (#1915): a recall in steps, a `MarkUsed`
                // or `Forget` split.
                ServiceOp::Recall(_) | ServiceOp::MarkUsed(_) | ServiceOp::Forget(_)
                    if Self::memory_span(cluster, &op)?.is_some() =>
                {
                    let deadline = deadline.map_or_else(
                        || tokio::time::Instant::now() + self.service.request_timeout(),
                        tokio::time::Instant::from_std,
                    );
                    return Box::pin(self.fan_memory(cluster, op, caller, prepaid, deadline)).await;
                }
                ServiceOp::Subscribe(_) => {
                    return self.subscribe(cluster, op, caller, prepaid).await;
                }
                // Boxed, as the fan-outs are: a cluster-only future awaited in place here
                // would widen every call's future (#1902).
                ServiceOp::MemoryWatch(_) => {
                    return Box::pin(self.memory_watch(cluster, op, caller, prepaid)).await;
                }
                // A replica copy's follower, here at P's owner (D328): never forwarded. Boxed,
                // as it awaits a partition's whole `Service::call` (#1902).
                ServiceOp::Follow(_) => {
                    return Box::pin(self.follow_owned(cluster, op, caller)).await;
                }
                // A replica copy is made its partition's owner through the map (D328 PR 3),
                // never in place: it would be a writer the map doesn't name.
                ServiceOp::Promote(_) => {
                    return Err(ApiError::failed_precondition(
                        "on a cluster node, a partition's replica is promoted with phdb cluster \
                         promote PARTITION --to NODE, which stamps it and moves the map",
                    ));
                }
                // Served where the partition is, never forwarded (D317 ticket 8b).
                ServiceOp::Backup(_) | ServiceOp::BackupDownload(_) => {
                    return self.backup(cluster, op, caller, prepaid).await;
                }
                // Across the partitions this node holds, never forwarded (ICR 0117, #1852).
                ServiceOp::Info(_) | ServiceOp::Flush(_) | ServiceOp::Compact(_) => {
                    return self.cluster_admin(cluster, op, caller, prepaid).await;
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
                // A spanning transaction's call at its home's owner (D329).
                Place::Here(service) if spanning::spanning_call(&op) => {
                    let deadline = deadline.map_or_else(
                        || tokio::time::Instant::now() + self.service.request_timeout(),
                        tokio::time::Instant::from_std,
                    );
                    let driver = cluster.driver(&service, &op, caller, deadline)?;
                    Box::pin(spanning::serve_home(driver, service, op, caller, prepaid)).await
                }
                Place::Here(service) => service.call(op, caller, prepaid).await,
                Place::There(owner) => {
                    let deadline = deadline.map_or_else(
                        || tokio::time::Instant::now() + self.service.request_timeout(),
                        tokio::time::Instant::from_std,
                    );
                    Box::pin(self.forward(cluster, op, caller, prepaid, owner, deadline)).await
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
    pub(crate) async fn forward(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<tokio::sync::OwnedSemaphorePermit>,
        owner: Target,
        deadline: tokio::time::Instant,
    ) -> Result<ServiceReply, ApiError> {
        self.forward_to(cluster, op, caller, prepaid, Fwd::Owner(owner), deadline)
            .await
            .map(|(reply, _)| reply)
    }

    /// [`forward`](Self::forward) to `to`: P's owner, or a node holding P's replica copy
    /// (D328), with the seqno the serving node read at (`ForwardResponse.served_seqno`).
    /// A replica's `WrongPartition` isn't followed: the caller tries the next replica.
    async fn forward_to(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<tokio::sync::OwnedSemaphorePermit>,
        to: Fwd,
        deadline: tokio::time::Instant,
    ) -> Result<(ServiceReply, u64), ApiError> {
        let (owner, replica) = match to {
            Fwd::Owner(t) => (t, None),
            Fwd::Replica(t, r) => (t, Some(r)),
        };
        // One deadline for the whole forward, from the request's arrival: the budget's wait,
        // a send, and after a WrongPartition the pull and the second send (cli692's F2).
        let left = || {
            deadline
                .checked_duration_since(tokio::time::Instant::now())
                .filter(|d| !d.is_zero())
                .ok_or_else(ApiError::deadline_exceeded)
        };
        // `supersedes` changes what's written: the owner applies it, or the call is refused
        // (#1915 PR 3b). Here, in the forward (boxed), not in `dispatch_by`, whose future
        // every call holds; an owner that is this node has the feature.
        if let ServiceOp::Remember(r) = &op
            && !r.supersedes.is_empty()
        {
            self.memory_fields(&op, deadline).await?;
        }
        let budget = self.service.admit(&op.admit(), caller, prepaid).await?;
        let writes = forward_writes(&op);
        let req = ForwardRequest {
            fanout_part: false,
            map_version: owner.map_version,
            caller: Some(forward::forwarded_caller(caller)?),
            to_replica: replica.is_some(),
            replica_max_lag_ms: replica.map_or(0, |r| r.max_lag_ms),
            min_primary_seqno: replica.and_then(|r| r.min_primary_seqno).unwrap_or(0),
            partition: String::new(),
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
            // A replica that doesn't hold the map's copy any more: the caller tries the next.
            Err(e) if e.code == "WrongPartition" && replica.is_none() => {
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
                    Place::Here(service) => {
                        let reply = service.call(op, caller, Some(budget)).await?;
                        return Ok((reply, visible_seqno(&service)));
                    }
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
        let reply = reply?;
        let served = reply.served_seqno;
        let reply = forward::reply_of(reply)?;
        // A read's reply is charged here too, for this node's own client: its front-end
        // holds the charge until the reply is written (#1567, cli692's F1 on ticket 2).
        let reply = match reply {
            ServiceReply::Get(_) | ServiceReply::ReadRow(_) | ServiceReply::MultiGet(_) => {
                self.service.charge_built(caller, reply).await?
            }
            reply => reply,
        };
        Ok((reply, served))
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

    /// `Memory.Watch` on a cluster node (#1915 PR 2b): one stream over every partition the
    /// watch covers, after this node's own checks, as [`Self::subscribe`].
    async fn memory_watch(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<ServiceReply, ApiError> {
        let budget = self.service.admit(&op.admit(), caller, prepaid).await?;
        drop(budget);
        let ServiceOp::MemoryWatch(r) = op else {
            return Err(ApiError::internal("not a memory watch"));
        };
        super::cluster::memwatch::watch(
            Arc::clone(&cluster.node),
            Arc::clone(&cluster.forwarder),
            Arc::clone(&cluster.parts),
            Arc::clone(&self.service),
            r,
            caller,
        )
        .await
        .map(ServiceReply::MemoryWatch)
    }

    /// `Cluster.ForwardMemoryWatch` here (#1915 PR 2b): one partition's component of a
    /// memory watch that another node merges.
    pub(crate) async fn serve_forwarded_memory_watch(
        &self,
        req: super::proto::ForwardMemoryWatchRequest,
    ) -> Result<super::memory::MemoryEvents, ApiError> {
        let Some(cluster) = &self.cluster else {
            return Err(ApiError::unsupported("this node isn't a cluster node"));
        };
        super::cluster::memwatch::serve(&cluster.node, &cluster.parts, req).await
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
    ///
    /// `partition`: the partition a fanned-out part of an index search or check is for
    /// (#1722 PR 1b), whose request names only the index; empty otherwise.
    ///
    /// `replica`: a read the sending node chose this node's replica copy of P for (D328),
    /// with the client's bound and the call's deadline: served from the copy, never as P's
    /// owner. Returns the seqno it was served at, for a single-partition read.
    pub(crate) async fn serve_forwarded(
        &self,
        op: ServiceOp,
        caller: &Caller,
        fanout_part: bool,
        partition: &str,
        replica: Option<(ReplicaRead, ReadDeadline)>,
    ) -> Result<(ServiceReply, u64), ApiError> {
        let Some(cluster) = &self.cluster else {
            return Err(ApiError::unsupported("this node isn't a cluster node"));
        };
        if map_schema_change(&op) {
            return Ok((self.serve_forwarded_schema(cluster, op, caller).await?, 0));
        }
        if let Some((read, deadline)) = replica {
            let partition = {
                let routes = cluster.node.routes();
                let Some(routes) = routes.as_ref() else {
                    return Err(ApiError::wrong_partition(
                        0,
                        "this node has no cluster map yet",
                    ));
                };
                route(routes, &op)?
            };
            if fanout_part && matches!(op, ServiceOp::MultiGet(_) | ServiceOp::Nearest(_)) {
                return Self::serve_copy_part(cluster, &partition, op, caller, read).await;
            }
            return Self::serve_copy(cluster, &partition, op, caller, None, read, deadline).await;
        }
        let service = {
            let routes = cluster.node.routes();
            let Some(routes) = routes.as_ref() else {
                return Err(ApiError::wrong_partition(
                    0,
                    "this node has no cluster map yet",
                ));
            };
            let p = if partition.is_empty() {
                route(routes, &op)?
            } else {
                index_part_route(routes, &op, partition)?
            };
            cluster.owned_here(routes, &p)?
        };
        let single = matches!(op, ServiceOp::Get(_) | ServiceOp::ReadRow(_));
        let reply = match op {
            ServiceOp::MultiGet(_)
            | ServiceOp::Nearest(_)
            | ServiceOp::VectorSearch(_)
            | ServiceOp::VectorVerify(_)
            | ServiceOp::TextVerify(_)
            | ServiceOp::TextStats(_)
            | ServiceOp::TextSearchPart(_)
                if fanout_part =>
            {
                service.fanout_part(op, caller).await?
            }
            // A spanning transaction's call at its home's owner (D329), forwarded by the
            // node the client called.
            op if spanning::spanning_call(&op) => {
                let deadline = tokio::time::Instant::now() + self.service.request_timeout();
                let driver = cluster.driver(&service, &op, caller, deadline)?;
                Box::pin(spanning::serve_home(
                    driver,
                    Arc::clone(&service),
                    op,
                    caller,
                    None,
                ))
                .await?
            }
            op => service.call(op, caller, None).await?,
        };
        Ok((reply, if single { visible_seqno(&service) } else { 0 }))
    }

    /// A read from this node's replica copy of `partition` (D328): once the copy has
    /// applied the client's read-your-writes seqno (within the deadline), and if it's
    /// fresh within the client's bound, else `ReplicaBehind`. Returns the copy's applied
    /// seqno after the read: the next read sent with it as its seqno is at least as new.
    async fn serve_copy(
        cluster: &Cluster,
        partition: &str,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<tokio::sync::OwnedSemaphorePermit>,
        read: ReplicaRead,
        deadline: ReadDeadline,
    ) -> Result<(ServiceReply, u64), ApiError> {
        let copy = cluster
            .replicas
            .load()
            .get(partition)
            .cloned()
            .ok_or_else(|| {
                ApiError::partition_unavailable(format!(
                    "this node holds no replica copy of partition {partition}"
                ))
            })?;
        copy.service
            .await_primary(&op, read.min_primary_seqno, deadline)
            .await?;
        copy.follower.within(read.max_lag_ms)?;
        let reply = copy.service.call(op, caller, prepaid).await?;
        Ok((reply, copy.follower.applied_seqno()))
    }

    /// One partition's part of a read another node fans out, from this node's replica copy
    /// (D328 PR 2b): no budget of its own, as an owner's part has none, and the copy's
    /// freshness within the bound before and after.
    async fn serve_copy_part(
        cluster: &Cluster,
        partition: &str,
        op: ServiceOp,
        caller: &Caller,
        read: ReplicaRead,
    ) -> Result<(ServiceReply, u64), ApiError> {
        let copy = cluster
            .replicas
            .load()
            .get(partition)
            .cloned()
            .ok_or_else(|| {
                ApiError::partition_unavailable(format!(
                    "this node holds no replica copy of partition {partition}"
                ))
            })?;
        copy.follower.within(read.max_lag_ms)?;
        let reply = copy.service.fanout_part(op, caller).await?;
        copy.follower.within(read.max_lag_ms)?;
        Ok((reply, 0))
    }

    /// A single-partition read a replica may serve (D328): P's replicas in the map's order,
    /// skipping one found behind or unreachable in the last [`REPLICA_BACKOFF`], each
    /// checking the bound itself; then P's owner. One deadline for all of them, from the
    /// request's arrival. A refusal any of them would give (a missing table, the scope)
    /// ends it there.
    async fn read_replicas(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        mut prepaid: Option<tokio::sync::OwnedSemaphorePermit>,
        read: ReplicaRead,
        deadline: ReadDeadline,
    ) -> Result<(ServiceReply, super::replica::Served), ApiError> {
        use super::replica::Served;
        let (partition, owner) = {
            let routes = cluster.node.routes();
            let Some(routes) = routes.as_ref() else {
                return Err(ApiError::partition_unavailable(
                    "this cluster node has no map yet",
                ));
            };
            let p = route(routes, &op)?;
            let owner = routes.owner(&p).cloned();
            (p, owner)
        };
        let me = cluster.node.id().to_owned();
        let replicas = owner
            .as_ref()
            .map(|o| o.replicas.clone())
            .unwrap_or_default();
        let map_version = cluster
            .node
            .routes()
            .as_ref()
            .map_or(0, |r| r.map_version());
        for (node, address) in replicas {
            let key = (node.clone(), partition.clone());
            if cluster
                .replica_backoff
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&key)
                .is_some_and(|at| at.elapsed() < REPLICA_BACKOFF)
            {
                continue;
            }
            let Some(again) = read_again(&op) else {
                break;
            };
            // At most half of what's left (simp724's S2 on #2001).
            let left = deadline
                .at
                .saturating_duration_since(std::time::Instant::now());
            let attempt = std::time::Instant::now() + replica_attempt(left);
            let tried = if node == me {
                Self::serve_copy(
                    cluster,
                    &partition,
                    again,
                    caller,
                    prepaid.take(),
                    read,
                    ReadDeadline {
                        at: attempt,
                        ..deadline
                    },
                )
                .await
            } else {
                let target = Target {
                    node: node.clone(),
                    address,
                    map_version,
                };
                Box::pin(self.forward_to(
                    cluster,
                    again,
                    caller,
                    prepaid.take(),
                    Fwd::Replica(target, read),
                    tokio::time::Instant::from_std(attempt),
                ))
                .await
            };
            match tried {
                Ok((reply, seqno)) => {
                    return Ok((
                        reply,
                        Served {
                            replica: true,
                            seqno,
                        },
                    ));
                }
                Err(e) if falls_back(&e) => {
                    cluster
                        .replica_backoff
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(key, std::time::Instant::now());
                }
                Err(e) => return Err(e),
            }
        }
        let (reply, seqno) = match cluster.target(&op)? {
            Place::Here(service) => {
                let reply = service.call(op, caller, prepaid).await?;
                (reply, visible_seqno(&service))
            }
            Place::There(owner) => {
                Box::pin(self.forward_to(
                    cluster,
                    op,
                    caller,
                    prepaid,
                    Fwd::Owner(owner),
                    tokio::time::Instant::from_std(deadline.at),
                ))
                .await?
            }
        };
        Ok((
            reply,
            Served {
                replica: false,
                seqno,
            },
        ))
    }

    /// Whether this is a cluster node's router.
    pub(crate) fn is_cluster(&self) -> bool {
        self.cluster.is_some()
    }

    /// [`dispatch_after_primary`](Self::dispatch_after_primary), and on a cluster node a read
    /// sent with `x-pigeonhole-read-replica-max-lag-ms` (`max_lag`, as parsed) that a
    /// replica may serve (D328): with what served it, for the response's headers. Every
    /// front-end's reads come here.
    pub(crate) async fn dispatch_read(
        &self,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<tokio::sync::OwnedSemaphorePermit>,
        min_primary_seqno: Result<Option<u64>, ApiError>,
        max_lag: Result<Option<u64>, ApiError>,
        deadline: ReadDeadline,
    ) -> Result<(ServiceReply, Option<super::replica::Served>), ApiError> {
        if let Some(cluster) = &self.cluster
            && let Some(max_lag_ms) = max_lag?
        {
            let min_primary_seqno = min_primary_seqno?;
            // A fanned-out read's parts, from each partition's replicas first (D328 PR 2b). At
            // a snapshot it's the owner's, as `dispatch_by` routes it.
            let fan_read = ReplicaRead {
                max_lag_ms,
                min_primary_seqno: None,
            };
            if min_primary_seqno.is_none() {
                let at = tokio::time::Instant::from_std(deadline.at);
                match &op {
                    ServiceOp::MultiGet(r) if snapshot_of(r.selection.as_ref()).is_none() => {
                        let reply = Box::pin(self.fan_multi_get(
                            cluster,
                            op,
                            caller,
                            prepaid,
                            at,
                            Some(fan_read),
                        ))
                        .await?;
                        return Ok((reply, None));
                    }
                    ServiceOp::Nearest(r) if r.snapshot_id.is_empty() => {
                        let reply = Box::pin(self.fan_nearest(
                            cluster,
                            op,
                            caller,
                            prepaid,
                            at,
                            Some(fan_read),
                        ))
                        .await?;
                        return Ok((reply, None));
                    }
                    ServiceOp::Traverse(r) if r.snapshot_id.is_empty() => {
                        let reply = Box::pin(self.fan_traverse(
                            cluster,
                            op,
                            caller,
                            prepaid,
                            at,
                            Some(fan_read),
                        ))
                        .await?;
                        return Ok((reply, None));
                    }
                    _ => {}
                }
            }
            // A scan's pages, from each partition's replicas first (D328 PR 2b, S2).
            if matches!(op, ServiceOp::Scan(_)) && min_primary_seqno.is_none() {
                let read = ReplicaRead {
                    max_lag_ms,
                    min_primary_seqno: None,
                };
                let reply =
                    Box::pin(self.fan_scan(cluster, op, caller, prepaid, Some(read))).await?;
                return Ok((reply, None));
            }
            if replica_readable(&op) {
                let read = ReplicaRead {
                    max_lag_ms,
                    min_primary_seqno,
                };
                let (reply, served) =
                    Box::pin(self.read_replicas(cluster, op, caller, prepaid, read, deadline))
                        .await?;
                return Ok((reply, Some(served)));
            }
            // A read across partitions with one partition's seqno can't be honoured: seqnos
            // are per partition (D328 §3).
            if min_primary_seqno.is_some() && fans_out(&op) {
                return Err(ApiError::invalid(format!(
                    "{} names one partition's seqno, so it can't go with a read across \
                     partitions",
                    super::replica::MIN_PRIMARY_SEQNO
                )));
            }
            let reply = self
                .dispatch_after_primary(op, caller, prepaid, Ok(min_primary_seqno), deadline)
                .await?;
            return Ok((reply, None));
        }
        let reply = self
            .dispatch_after_primary(op, caller, prepaid, min_primary_seqno, deadline)
            .await?;
        Ok((reply, None))
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
    pub(crate) node: String,
    pub(crate) address: String,
    pub(crate) map_version: u64,
}

/// Where [`Router::forward_to`] sends a call: P's owner, or a node holding P's replica copy
/// with the client's bound (D328).
pub(crate) enum Fwd {
    Owner(Target),
    Replica(Target, ReplicaRead),
}

/// A read a replica may serve (D328): the client's bound and read-your-writes seqno.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReplicaRead {
    pub(crate) max_lag_ms: u64,
    pub(crate) min_primary_seqno: Option<u64>,
}

/// A node's routed reads' record of replicas found behind or unreachable (D328).
pub(crate) type Backoff =
    Arc<std::sync::Mutex<std::collections::HashMap<(String, String), std::time::Instant>>>;

/// What a fanned-out read needs to try P's replicas (D328 PR 2b): the client's bound, this
/// node's own copies and the node's back-off record.
#[derive(Clone, Debug)]
pub(crate) struct ReplicaRoute {
    pub(crate) read: ReplicaRead,
    pub(crate) copies: Replicas,
    pub(crate) backoff: Backoff,
}

impl ReplicaRoute {
    /// The route of a read with `read`'s bound on `cluster`.
    pub(crate) fn of(cluster: &Cluster, read: ReplicaRead) -> Self {
        Self {
            read,
            copies: Arc::clone(&cluster.replicas),
            backoff: Arc::clone(&cluster.replica_backoff),
        }
    }

    /// Whether `node`'s replica of `partition` was found behind or unreachable lately.
    pub(crate) fn backed_off(&self, node: &str, partition: &str) -> bool {
        self.backoff
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(node.to_owned(), partition.to_owned()))
            .is_some_and(|at| at.elapsed() < REPLICA_BACKOFF)
    }

    /// `partition`'s replicas a read may try now, in the map's order: not backed off, and
    /// not in `tried` (the ones this read tried already). Each node's id and address
    /// (simp724's N1 on #2001: one walk for every fanned-out read).
    pub(crate) fn candidates(
        &self,
        routes: &super::cluster::route::Routes,
        partition: &str,
        tried: &[String],
    ) -> Vec<(String, String)> {
        routes
            .owner(partition)
            .map(|o| o.replicas.clone())
            .unwrap_or_default()
            .into_iter()
            .filter(|(node, _)| !tried.contains(node) && !self.backed_off(node, partition))
            .collect()
    }

    /// Records `node`'s replica of `partition` as behind or unreachable now.
    pub(crate) fn back_off(&self, node: &str, partition: &str) {
        self.backoff
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                (node.to_owned(), partition.to_owned()),
                std::time::Instant::now(),
            );
    }
}

/// The seqno a read on `service`'s partition was served at, read after it: the owner's
/// visible seqno (`x-pigeonhole-served-seqno`, D328). 0 without a partition.
fn visible_seqno(service: &Service) -> u64 {
    service.part().map_or(0, |p| p.db.visible_seqno())
}

/// Where a routed call goes: a partition's service here, or the node serving it.
pub(crate) enum Place {
    Here(Arc<Service>),
    There(Target),
}

impl Cluster {
    /// Where `op`'s partition is served: here, or the node serving it.
    pub(crate) fn target(&self, op: &ServiceOp) -> Result<Place, ApiError> {
        let routes = self.node.routes();
        let Some(routes) = routes.as_ref() else {
            return Err(ApiError::partition_unavailable(
                "this cluster node has no map yet",
            ));
        };
        let partition = route(routes, op)?;
        place(&self.node, &self.parts, routes, &partition)
    }
}

/// The partition `op` goes to on a cluster node: a single-key call's, a batch's when all
/// its rows are in one, and a transaction's or snapshot's (D317 ticket 3): its `route` when
/// it begins, then the partition its id names, with every row it reads or writes checked to
/// be in it (`CrossPartition`). The owner routes a forwarded call again, so it checks too.
/// Everything else is refused.
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
    let spanning = |id: &str| super::sessions::spanning_parts(id).is_some();
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
        // A spanning transaction's call goes to its home, whatever rows it names: the home's
        // owner sends each row of another partition on to that partition's branch (D329).
        ServiceOp::TxnGet(r) if spanning(&r.txn_id) => session(&r.txn_id, "transaction"),
        ServiceOp::TxnGetRow(r) if spanning(&r.txn_id) => session(&r.txn_id, "transaction"),
        ServiceOp::TxnWrite(r) if spanning(&r.txn_id) => session(&r.txn_id, "transaction"),
        ServiceOp::Commit(r) if spanning(&r.txn_id) => session(&r.txn_id, "transaction"),
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
            None => Err(unrouted()),
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
        // Agent memory (#1915, ICR 0139): the partition holding the namespaces the call
        // names. An id carries its namespace; a link and a supersede stay within one, which
        // the store checks.
        ServiceOp::Remember(r) => memory_route(routes, [r.namespace.as_str()], "Memory.Remember"),
        ServiceOp::Link(r) => {
            let from = super::memory::id(&r.from)?;
            memory_route(routes, [from.namespace()], "Memory.Link")
        }
        ServiceOp::Supersede(r) => {
            let old = super::memory::id(&r.old)?;
            memory_route(routes, [old.namespace()], "Memory.Supersede")
        }
        ServiceOp::MarkUsed(r) => {
            let ids = super::memory::ids(&r.ids)?;
            memory_route(
                routes,
                ids.iter().map(|id| id.namespace()),
                "Memory.MarkUsed",
            )
        }
        ServiceOp::Forget(r) => {
            let ids = super::memory::ids(&r.ids)?;
            memory_route(routes, ids.iter().map(|id| id.namespace()), "Memory.Forget")
        }
        ServiceOp::Recall(r) => memory_route(
            routes,
            r.namespaces.iter().map(String::as_str),
            "Memory.Recall",
        ),
        // A spanning recall's steps (#1915), each on one partition's namespaces.
        ServiceOp::RecallPool(r) => memory_route(
            routes,
            r.request
                .iter()
                .flat_map(|q| q.namespaces.iter().map(String::as_str)),
            "Memory.RecallPool",
        ),
        ServiceOp::RecallBodies(r) => memory_route(
            routes,
            r.request
                .iter()
                .flat_map(|q| q.namespaces.iter().map(String::as_str)),
            "Memory.RecallBodies",
        ),
        // Merged across its partitions by `dispatch`, never routed to one (#1915 PR 2b).
        ServiceOp::MemoryWatch(_) => Err(ApiError::internal(
            "Memory.Watch is merged across partitions, not routed",
        )),
        // An index write (#1722): to the partition holding its id, by the table the index
        // follows or by its own ranges. A text document's id is its source row's key, so its
        // postings land beside it.
        ServiceOp::VectorInsert(r) => index_route(routes, &r.index, &r.id),
        ServiceOp::VectorDelete(r) => index_route(routes, &r.index, &r.id),
        ServiceOp::TextPut(r) => index_route(routes, &r.index, &r.id),
        ServiceOp::TextDelete(r) => index_route(routes, &r.index, &r.id),
        // Answered before this, by [`Router::dispatch`]'s own arms, and never forwarded: a
        // schema change (the map writer's), a schema read, a read that fans out, a
        // subscription and a backup.
        op if map_schema_change(op) => Err(unrouted()),
        // Fanned out across the index's partitions by `dispatch`, each part naming its
        // partition (#1722 PR 1b): [`index_part_route`].
        // A text search's two rounds (#1722 PR 2b) too, and a rebuild (PR 2c). A bulk load
        // is split by id (PR 3).
        ServiceOp::VectorBulkLoad(_)
        | ServiceOp::VectorSearch(_)
        | ServiceOp::VectorVerify(_)
        | ServiceOp::TextVerify(_)
        | ServiceOp::TextRebuild(_)
        | ServiceOp::TextSearch(_)
        | ServiceOp::TextStats(_)
        | ServiceOp::TextSearchPart(_) => Err(unrouted()),
        ServiceOp::Traverse(_)
        | ServiceOp::ListTables(_)
        | ServiceOp::DescribeTable(_)
        | ServiceOp::Subscribe(_)
        | ServiceOp::Backup(_)
        | ServiceOp::BackupDownload(_) => Err(unrouted()),
        _ => Err(ApiError::unsupported(
            "this call isn't served by a cluster node (D317)",
        )),
    }
}

/// Whether `op` is a schema change the map's writer makes (D317 ticket 5): a table's create or
/// drop, or an index's create (#1722; its drop is a `DropTable`).
pub(crate) fn map_schema_change(op: &ServiceOp) -> bool {
    matches!(
        op,
        ServiceOp::CreateTable(_)
            | ServiceOp::DropTable(_)
            | ServiceOp::CreateIndex(_)
            | ServiceOp::TextCreateIndex(_)
    )
}

/// The partition a fanned-out part of an index search or check names (#1722 PR 1b), if
/// the map places the op's index on it; else `WrongPartition`, since the sending node's
/// map placed it there.
fn index_part_route(routes: &Routes, op: &ServiceOp, partition: &str) -> Result<String, ApiError> {
    let index: &str = match op {
        ServiceOp::VectorSearch(r) => &r.index,
        ServiceOp::VectorVerify(r) => &r.index,
        ServiceOp::TextVerify(r) | ServiceOp::TextRebuild(r) => &r.index,
        // A text search's rounds carry the client's search: one without it is malformed,
        // not a part for another map (simp724's R1 on #2029).
        ServiceOp::TextStats(super::proto::TextStatsRequest { search, .. })
        | ServiceOp::TextSearchPart(super::proto::TextSearchPartRequest { search, .. }) => {
            match search {
                Some(s) => &s.index,
                None => {
                    return Err(ApiError::invalid(format!(
                        "a forwarded {} carries no search",
                        op.name()
                    )));
                }
            }
        }
        _ => {
            return Err(ApiError::invalid(format!(
                "a forwarded {} doesn't name a partition",
                op.name()
            )));
        }
    };
    match routes.index_partitions(index, b"") {
        Some(ps) if ps.contains(&partition) => Ok(partition.to_owned()),
        _ => Err(ApiError::wrong_partition(
            routes.map_version(),
            format!("index {index:?} isn't on partition {partition} in this node's map"),
        )),
    }
}

/// The partition holding id `id` of index `index` (#1722).
fn index_route(routes: &Routes, index: &str, id: &[u8]) -> Result<String, ApiError> {
    routes
        .index_partition(index, id)
        .map(str::to_owned)
        .ok_or_else(|| {
            ApiError::from_code(
                pigeonhole::ErrorCode::TableNotFound,
                &format!("index {index:?} isn't in the cluster map"),
            )
        })
}

/// The partition holding every namespace a memory call names (#1915): `what`'s. A call
/// whose namespaces are on several partitions never comes here: the entry node fans a
/// recall out and splits a `MarkUsed` or `Forget` ([`Router::fan_memory`]), and a call
/// that can't span (a link's or supersede's ends are in one namespace) routes by one id.
fn memory_route<'a>(
    routes: &Routes,
    namespaces: impl IntoIterator<Item = &'a str>,
    what: &str,
) -> Result<String, ApiError> {
    let parts = memory_partitions(routes, namespaces)?;
    let mut it = parts.into_iter();
    match (it.next(), it.next()) {
        (Some((p, _)), None) => Ok(p),
        (Some((p, a)), Some((q, b))) => Err(ApiError::cross_partition(format!(
            "{what}'s namespaces are on more than one partition ({:?} on {p}, {:?} on {q}): \
             only the node the client called splits it",
            a[0], b[0]
        ))),
        (None, _) => Err(ApiError::invalid(format!("{what} names no namespace"))),
    }
}

/// Each partition holding some of the namespaces a memory call names, with its namespaces
/// in order, each once (#1915). The namespaces are checked as the store checks them
/// (non-empty, no `\0`), so a bad one is refused here rather than routed. A cluster whose
/// map places no store refuses every memory call.
pub(crate) fn memory_partitions<'a>(
    routes: &Routes,
    namespaces: impl IntoIterator<Item = &'a str>,
) -> Result<std::collections::BTreeMap<String, Vec<String>>, ApiError> {
    if routes.memory_store().is_none() {
        return Err(ApiError::unsupported(
            "this cluster has no memory store: its map places none (ClusterMap.memory)",
        ));
    }
    let mut out: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for ns in namespaces {
        if ns.is_empty() {
            return Err(ApiError::invalid("a namespace can't be empty"));
        }
        if ns.as_bytes().contains(&0) {
            return Err(ApiError::invalid(format!(
                "a namespace can't contain \\0: {ns:?}"
            )));
        }
        let p = routes
            .memory_partition(ns)
            .ok_or_else(|| ApiError::internal("the memory placement has no partitions"))?;
        let list = out.entry(p.to_owned()).or_default();
        if !list.iter().any(|n| n == ns) {
            list.push(ns.to_owned());
        }
    }
    Ok(out)
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

/// Whether a forward of `op` that fails after it was sent leaves an outcome unknown (B3):
/// a write, and a transaction's buffered writes, commit and abort (ticket 3).
/// Whether a replica may serve `op` (D328 PR 2a): a single-partition read with no
/// snapshot (a transaction's reads are `TxnGet` and `TxnGetRow`, never routed).
fn replica_readable(op: &ServiceOp) -> bool {
    match op {
        ServiceOp::Get(r) => r.snapshot_id.is_empty(),
        ServiceOp::ReadRow(_) => true,
        _ => false,
    }
}

/// Whether `op` is a read a cluster fans out over partitions.
fn fans_out(op: &ServiceOp) -> bool {
    matches!(
        op,
        ServiceOp::MultiGet(_)
            | ServiceOp::Scan(_)
            | ServiceOp::Nearest(_)
            | ServiceOp::Traverse(_)
    )
}

/// Another copy of a read [`replica_readable`] accepts, for the next replica or the owner:
/// `ServiceOp` holds its request by value, and only these are tried more than once.
fn read_again(op: &ServiceOp) -> Option<ServiceOp> {
    match op {
        ServiceOp::Get(r) => Some(ServiceOp::Get(r.clone())),
        ServiceOp::ReadRow(r) => Some(ServiceOp::ReadRow(r.clone())),
        _ => None,
    }
}

/// Whether a replica's refusal sends the read on to the next replica or the owner (D328):
/// it's behind, it doesn't hold the copy (or the copy closed under the read, as a map that
/// drops it closes it), or it couldn't be reached. Anything else (a missing table, the
/// caller's scope) the owner would answer the same.
pub(crate) fn falls_back(e: &ApiError) -> bool {
    matches!(
        e.code.as_str(),
        "ReplicaBehind" | "PartitionUnavailable" | "WrongPartition" | "Unavailable" | "Closed"
    )
}

fn forward_writes(op: &ServiceOp) -> bool {
    // `TxnWrite` needs write scope, so `writes()` has it; a `Commit` without rows commits
    // the buffered ones.
    op.writes() || matches!(op, ServiceOp::Commit(_) | ServiceOp::Abort(_))
}

/// A partition the map gives this node that it doesn't have open: it is still opening (a
/// map just gave it here), was closed by the operator, or failed to open.
pub(crate) fn not_open(partition: &str) -> ApiError {
    ApiError::partition_unavailable(format!(
        "the map gives this node partition {partition:?}, but it isn't open here: it is \
         still opening, was closed (phdb cluster close), or failed to open; phdb cluster \
         status shows which"
    ))
}

/// The answer of [`route`] to a call no caller brings it: one answered before it's routed.
fn unrouted() -> ApiError {
    ApiError::internal("this call isn't routed to one partition")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A part of an index's search or check is served on the partition it names, only if
    /// the map places the index there (#1722 PR 1b); a part of another call never names one.
    #[test]
    fn an_index_part_routes_to_the_partition_it_names_if_the_index_is_there() {
        use super::super::proto::{ClusterMap, IndexPlacement, Partition, TablePlacement};
        let part = |id: &str| Partition {
            id: id.into(),
            node: "a".into(),
            ..Default::default()
        };
        let map = ClusterMap {
            map_version: 3,
            partitions: vec![part("p1"), part("p2"), part("p3")],
            tables: vec![TablePlacement {
                name: "users".into(),
                splits: vec![super::super::proto::SplitKey {
                    key: Some(super::super::proto::split_key::Key::Text("m".into())),
                }],
                partitions: vec!["p1".into(), "p2".into()],
                ..Default::default()
            }],
            indexes: vec![IndexPlacement {
                name: "uv".into(),
                follows: "users".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let routes = Routes::new(&map);
        let search = ServiceOp::VectorSearch(super::super::proto::VectorSearchRequest {
            index: "uv".into(),
            ..Default::default()
        });
        let verify =
            ServiceOp::TextVerify(super::super::proto::TextIndexRequest { index: "uv".into() });
        let rebuild =
            ServiceOp::TextRebuild(super::super::proto::TextIndexRequest { index: "uv".into() });
        // A text search's two rounds (#1722 PR 2b) name the index inside their search.
        let text = || super::super::proto::TextSearchRequest {
            index: "uv".into(),
            ..Default::default()
        };
        let stats = ServiceOp::TextStats(super::super::proto::TextStatsRequest {
            search: Some(text()),
        });
        let part = ServiceOp::TextSearchPart(super::super::proto::TextSearchPartRequest {
            search: Some(text()),
            stats: Default::default(),
        });
        for op in [&search, &verify, &rebuild, &stats, &part] {
            assert_eq!(index_part_route(&routes, op, "p2").unwrap(), "p2");
            let e = index_part_route(&routes, op, "p3").unwrap_err();
            assert_eq!(e.code, "WrongPartition", "{}", e.message);
        }
        // Without its search, a round is malformed, not a part for another map.
        let bare = [
            ServiceOp::TextStats(Default::default()),
            ServiceOp::TextSearchPart(Default::default()),
        ];
        for op in &bare {
            let e = index_part_route(&routes, op, "p2").unwrap_err();
            assert_eq!(e.code, "InvalidArgument", "{}", e.message);
        }
        let lost = ServiceOp::VectorSearch(super::super::proto::VectorSearchRequest {
            index: "nope".into(),
            ..Default::default()
        });
        assert_eq!(
            index_part_route(&routes, &lost, "p1").unwrap_err().code,
            "WrongPartition"
        );
        let get = ServiceOp::Get(Default::default());
        assert_eq!(
            index_part_route(&routes, &get, "p1").unwrap_err().code,
            "InvalidArgument"
        );
        // Unnamed, an index's search isn't routed by its content.
        assert!(route(&routes, &search).is_err());
    }

    /// The size of the future `f` returns, without calling it.
    fn future_size<A, F: Future>(_f: impl FnOnce(A) -> F) -> usize {
        std::mem::size_of::<F>()
    }

    #[test]
    fn dispatch_by_awaits_nothing_in_place_larger_than_a_service_call() {
        // #1902: a future is as large as the largest it awaits in place, and every call's
        // future holds `dispatch_by`'s. A cluster-only future awaited in place there, larger
        // than `Service::call`'s, makes a single node's `Get` pay for it. The margin is what
        // `dispatch_by` keeps beside its awaitee: the op twice, and its other arguments.
        type Args<'a> = (
            &'a Router,
            ServiceOp,
            &'a Caller,
            Option<tokio::sync::OwnedSemaphorePermit>,
        );
        let dispatch = future_size(|(r, op, caller, prepaid): Args<'_>| {
            r.dispatch_by(op, caller, prepaid, None)
        });
        let call =
            future_size(|(r, op, caller, prepaid): Args<'_>| r.service.call(op, caller, prepaid));
        let margin = 2 * std::mem::size_of::<ServiceOp>() + 512;
        assert!(
            dispatch <= call + margin,
            "dispatch_by's future is {dispatch} bytes, Service::call's {call}, and the margin \
             {margin}: box the future dispatch_by awaits in place that outgrew Service::call's \
             (Box::pin, as the fan-outs are), or every call pays for its size (#1902)"
        );
    }
}
