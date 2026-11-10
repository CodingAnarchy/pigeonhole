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
use super::service::{Service, ServiceOp, ServiceReply};

/// A cluster node's routing: the node (its map, routes and peers) and the partition it
/// serves, if the map gives it one.
#[derive(Debug)]
pub(crate) struct Cluster {
    pub(crate) node: Arc<super::cluster::Node>,
    pub(crate) local: Option<String>,
    pub(crate) forwarder: Arc<Forwarder>,
}

/// Where a call goes.
#[derive(Debug)]
pub(crate) struct Router {
    pub(crate) service: Service,
    pub(crate) cluster: Option<Cluster>,
}

impl Router {
    pub(crate) fn single(service: Service) -> Self {
        Self {
            service,
            cluster: None,
        }
    }

    /// A cluster node's router: `service` serves `local`, the partition the map gives the
    /// node (`None`: the node only routes).
    pub(crate) fn cluster(
        service: Service,
        node: Arc<super::cluster::Node>,
        local: Option<String>,
    ) -> Self {
        let forwarder = Arc::new(Forwarder::new(Arc::clone(&node)));
        // Its sessions' ids name its partition, so any node routes a call on one (ticket 3).
        let service = match &local {
            Some(p) => service.sessions_on(p),
            None => service,
        };
        Self {
            service,
            cluster: Some(Cluster {
                node,
                local,
                forwarder,
            }),
        }
    }

    /// The cluster node, on a cluster node.
    pub(crate) fn cluster_node(&self) -> Option<&Arc<super::cluster::Node>> {
        self.cluster.as_ref().map(|c| &c.node)
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
            // A read across a table's partitions fans out (D317 ticket 4a).
            // One at a snapshot reads its partition only (ticket 3): a multi-get is routed and
            // forwarded as a single-key call is, a scan streams that partition alone.
            match &op {
                ServiceOp::MultiGet(r) if snapshot_of(r.selection.as_ref()).is_none() => {
                    let deadline = deadline.map_or_else(
                        || tokio::time::Instant::now() + self.service.request_timeout(),
                        tokio::time::Instant::from_std,
                    );
                    return self
                        .fan_multi_get(cluster, op, caller, prepaid, deadline)
                        .await;
                }
                ServiceOp::Scan(_) => return self.fan_scan(cluster, op, caller, prepaid).await,
                _ => {}
            }
            let target = cluster.target(&op)?;
            if let Some(owner) = target {
                let deadline = deadline.map_or_else(
                    || tokio::time::Instant::now() + self.service.request_timeout(),
                    tokio::time::Instant::from_std,
                );
                return self
                    .forward(cluster, op, caller, prepaid, owner, deadline)
                    .await;
            }
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
                    None => return self.service.call(op, caller, Some(budget)).await,
                    Some(next) => {
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

    /// A call another node forwarded here (D317 ticket 2): served only if this node's map
    /// gives it the key, otherwise `WrongPartition` with this node's map version. Never
    /// forwarded on, so a split view of the map can't bounce a call between nodes.
    pub(crate) async fn serve_forwarded(
        &self,
        op: ServiceOp,
        caller: &Caller,
    ) -> Result<ServiceReply, ApiError> {
        let Some(cluster) = &self.cluster else {
            return Err(ApiError::unsupported("this node isn't a cluster node"));
        };
        {
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
            if cluster.local.as_deref() != Some(p.as_str()) {
                return Err(not_open(&p));
            }
        }
        self.service.call(op, caller, None).await
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

impl Cluster {
    /// `None`: `op`'s partition is served here; otherwise the node serving it.
    fn target(&self, op: &ServiceOp) -> Result<Option<Target>, ApiError> {
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
            return if self.local.as_deref() == Some(partition.as_str()) {
                Ok(None)
            } else {
                Err(not_open(&partition))
            };
        }
        Ok(Some(Target {
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
        ServiceOp::Nearest(_)
        | ServiceOp::Traverse(_)
        | ServiceOp::VectorSearch(_)
        | ServiceOp::TextSearch(_) => Err(later("a read across partitions", 4)),
        ServiceOp::ListTables(_)
        | ServiceOp::CreateTable(_)
        | ServiceOp::DropTable(_)
        | ServiceOp::DescribeTable(_) => Err(later("the schema", 5)),
        ServiceOp::Subscribe(_) => Err(later("a change feed", 7)),
        ServiceOp::Backup(_) | ServiceOp::BackupDownload(_) => Err(later("a backup", 8)),
        _ => Err(ApiError::unsupported(
            "this call isn't served by a cluster node (D317)",
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
