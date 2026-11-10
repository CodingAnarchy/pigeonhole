//! D317's Router seam: every protocol front-end calls `Router::dispatch`. A single node
//! passes straight to its one partition. A cluster node (D317 ticket 2) routes by the
//! partition map: a single-key call to its partition, served here or forwarded to the node
//! that owns it, without touching any handler.
//!
//! perf287's conditions on the seam: the operation is moved, never cloned or boxed per call,
//! and a single node does no map load at all (`cluster` is `None`, one branch).

use std::sync::Arc;

use super::auth::Caller;
use super::cluster::route::Routes;
use super::errors::ApiError;
use super::proto::RowWrites;
use super::replica::ReadDeadline;
use super::service::{Service, ServiceOp, ServiceReply};

/// A cluster node's routing: the node (its map, routes and peers) and the partition it
/// serves, if the map gives it one.
#[derive(Debug)]
pub(crate) struct Cluster {
    node: Arc<super::cluster::Node>,
    local: Option<String>,
}

/// Where a call goes.
#[derive(Debug)]
pub(crate) struct Router {
    pub(crate) service: Service,
    cluster: Option<Cluster>,
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
        Self {
            service,
            cluster: Some(Cluster { node, local }),
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
        if let Some(cluster) = &self.cluster {
            let partition = {
                let routes = cluster.node.routes();
                let Some(routes) = routes.as_ref() else {
                    return Err(ApiError::partition_unavailable(
                        "this cluster node has no map yet",
                    ));
                };
                route(routes, &op)?
            };
            if cluster.local.as_deref() != Some(partition.as_str()) {
                // Forwarding to the owning node is this ticket's next commit.
                return Err(ApiError::partition_unavailable(format!(
                    "partition {partition:?} is served by another node"
                )));
            }
        }
        self.service.call(op, caller, prepaid).await
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
        self.service
            .await_primary(&op, min_primary_seqno?, deadline)
            .await?;
        self.dispatch(op, caller, prepaid).await
    }
}

/// The partition `op` goes to on a cluster node: a single-key call's, or a batch's when all
/// its rows are in one. Everything else is refused until its D317 ticket.
fn route(routes: &Routes, op: &ServiceOp) -> Result<String, ApiError> {
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
    match op {
        ServiceOp::Get(r) if !r.snapshot_id.is_empty() => Err(later("a snapshot", 3)),
        ServiceOp::Get(r) => one(&r.table, &r.row),
        ServiceOp::ReadRow(r) => one(&r.table, &r.row),
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
                    let first = format!("{}/{}", key.table, String::from_utf8_lossy(&key.row));
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
        ServiceOp::Begin(_)
        | ServiceOp::TxnGet(_)
        | ServiceOp::TxnGetRow(_)
        | ServiceOp::TxnWrite(_)
        | ServiceOp::Commit(_)
        | ServiceOp::Abort(_) => Err(later("a transaction", 3)),
        ServiceOp::OpenSnapshot(_) | ServiceOp::ReleaseSnapshot(_) => Err(later("a snapshot", 3)),
        ServiceOp::MultiGet(_)
        | ServiceOp::Scan(_)
        | ServiceOp::Nearest(_)
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

/// A call a later D317 ticket brings to a cluster.
fn later(what: &str, ticket: u32) -> ApiError {
    ApiError::unsupported(format!(
        "{what} in a cluster comes with D317's ticket {ticket}"
    ))
}
