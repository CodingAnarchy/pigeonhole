//! D317's Router seam: every protocol front-end calls `Router::dispatch`, which today passes
//! straight to the one local partition. D317's ticket 2 fills `Cluster` in (the partition
//! map, forwarding to the owning node) without touching any handler.
//!
//! perf287's conditions on the seam: the operation is moved, never cloned or boxed per call,
//! and a single node does no map load at all. `Cluster` is uninhabited, so `cluster` is
//! always `None` here and the branch compiles away.

use super::auth::Caller;
use super::errors::ApiError;
use super::service::{Service, ServiceOp, ServiceReply};

/// The partition map and the peers, once D317 lands.
#[derive(Debug)]
pub(crate) enum Cluster {}

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

    /// Answers `op` for `caller`, wherever its partition lives (today: here).
    pub(crate) async fn dispatch(
        &self,
        op: ServiceOp,
        caller: &Caller,
    ) -> Result<ServiceReply, ApiError> {
        if let Some(cluster) = &self.cluster {
            match *cluster {}
        }
        self.service.call(op, caller).await
    }
}
