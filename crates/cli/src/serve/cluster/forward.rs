//! Forwarding a routed call to the node that owns its partition (D317 §3, ticket 2).
//!
//! - **Typed, not a byte proxy:** the entry node sends the decoded call as a
//!   `Cluster.Forward` with the caller it's for, and the owner checks that caller's scope and
//!   namespaces again before it serves the call.
//! - **A few connections per peer:** each peer gets [`CHANNELS`] HTTP/2 connections, used
//!   round-robin, so one lost packet or one connection's flow-control window doesn't stall
//!   every forwarded call. Each connects on first use.
//! - **B3 across the hop:** a forward that fails before it's sent (no connection), or that
//!   the owner's HTTP/2 layer refused unprocessed (`REFUSED_STREAM`, or a `GOAWAY` below the
//!   stream), answers `PartitionUnavailable`, which is retry-safe. Any other transport
//!   failure after the send answers `OutcomeUnknown` for a write, since it may have been
//!   applied, and `PartitionUnavailable` for a read. An answer the owner gave (an error with
//!   its detail) passes back unchanged.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tonic::transport::{Channel, Endpoint};

use super::Node;
use crate::serve::auth::{Caller, Scope};
use crate::serve::errors::ApiError;
use crate::serve::proto::{
    ForwardRequest, ForwardResponse, ForwardedCaller, cluster_client::ClusterClient,
    forward_request, forward_response,
};
use crate::serve::service::{ServiceOp, ServiceReply};

/// HTTP/2 connections per peer (D317 §3; perf287's serve bench may tune it).
pub(crate) const CHANNELS: usize = 4;

/// One peer's connections.
struct Peer {
    endpoint: Endpoint,
    slots: [tokio::sync::OnceCell<Channel>; CHANNELS],
    next: AtomicUsize,
}

/// A node's forwarding: its peers' connections, made on first use.
pub(crate) struct Forwarder {
    node: Arc<Node>,
    peers: Mutex<HashMap<String, Arc<Peer>>>,
}

impl std::fmt::Debug for Forwarder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Forwarder").finish_non_exhaustive()
    }
}

impl Forwarder {
    pub(crate) fn new(node: Arc<Node>) -> Self {
        Self {
            node,
            peers: Mutex::new(HashMap::new()),
        }
    }

    fn peer(&self, address: &str) -> Result<Arc<Peer>, ApiError> {
        let mut peers = self.peers.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(p) = peers.get(address) {
            return Ok(Arc::clone(p));
        }
        // Peers a newer map no longer lists go as a new one is added (cli692's nit).
        if let Some(routes) = self.node.routes().as_ref() {
            peers.retain(|a, _| routes.has_address(a));
        }
        let endpoint = self
            .node
            .endpoint(address)
            .map_err(|e| ApiError::partition_unavailable(format!("node at {address}: {e}")))?;
        let peer = Arc::new(Peer {
            endpoint,
            slots: std::array::from_fn(|_| tokio::sync::OnceCell::new()),
            next: AtomicUsize::new(0),
        });
        peers.insert(address.to_owned(), Arc::clone(&peer));
        Ok(peer)
    }

    /// Sends `req` to the node at `address`, within `timeout`. `writes`: the call writes, so a
    /// failure after the send leaves its outcome unknown.
    pub(crate) async fn send(
        &self,
        address: &str,
        req: ForwardRequest,
        timeout: Duration,
        writes: bool,
    ) -> Result<ForwardResponse, ApiError> {
        let peer = self.peer(address)?;
        let slot = &peer.slots[peer.next.fetch_add(1, Ordering::Relaxed) % CHANNELS];
        // Before the send: a connection that can't be made sent nothing.
        let channel = slot
            .get_or_try_init(|| peer.endpoint.connect())
            .await
            .map_err(|e| {
                ApiError::partition_unavailable(format!(
                    "the partition's node at {address} can't be reached: {e}"
                ))
            })?
            .clone();
        let mut request = self.node.request(req).map_err(ApiError::internal)?;
        request.set_timeout(timeout);
        match ClusterClient::new(channel).forward(request).await {
            Ok(reply) => Ok(reply.into_inner()),
            Err(status) => Err(classify(&status, address, writes)),
        }
    }
}

/// What a failed forward answers the client (B3).
fn classify(status: &tonic::Status, address: &str, writes: bool) -> ApiError {
    // The owner answered: its error, as it gave it.
    if let Some(e) = ApiError::from_status(status) {
        return e;
    }
    let what = format!("the forward to the partition's node at {address} failed: {status}");
    if !writes || refused_unprocessed(status) {
        ApiError::partition_unavailable(what)
    } else {
        ApiError::outcome_unknown(format!("{what}; the write may have been applied"))
    }
}

/// Whether the forward never reached the owner, or reached it and was refused unprocessed:
/// a connection refused (a channel reconnecting to a node that's down), a `REFUSED_STREAM`
/// reset, or a `GOAWAY` (the server names the last stream it processed, so the client fails
/// only streams it didn't). Either way nothing ran (cli692's point 5).
fn refused_unprocessed(status: &tonic::Status) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(status);
    while let Some(e) = source {
        if let Some(h2) = e.downcast_ref::<h2::Error>() {
            return h2.reason() == Some(h2::Reason::REFUSED_STREAM) || h2.is_go_away();
        }
        // A channel reconnecting to a node that's down: nothing was sent.
        if let Some(io) = e.downcast_ref::<std::io::Error>()
            && io.kind() == std::io::ErrorKind::ConnectionRefused
        {
            return true;
        }
        source = e.source();
    }
    false
}

/// `caller` as a forward carries it: only data calls are forwarded, so a read or write scope
/// (an admin may make any data call, so it goes as `write`; a cluster token makes none).
pub(crate) fn forwarded_caller(caller: &Caller) -> Result<ForwardedCaller, ApiError> {
    let scope = match caller.scope {
        Scope::Read => "read",
        Scope::Write | Scope::Admin => "write",
        Scope::Cluster => {
            return Err(ApiError::permission_denied(
                "make a data call with a cluster token",
            ));
        }
    };
    Ok(ForwardedCaller {
        scope: scope.to_owned(),
        all_namespaces: caller.namespaces.is_none(),
        namespaces: caller.namespaces.clone().unwrap_or_default(),
    })
}

/// The caller a forward is for, on the owner: never more than `write` (D317 ticket 2).
pub(crate) fn caller_of(f: Option<ForwardedCaller>) -> Result<Caller, ApiError> {
    let f = f.ok_or_else(|| ApiError::invalid("a forward names no caller"))?;
    let scope = match f.scope.as_str() {
        "read" => Scope::Read,
        "write" => Scope::Write,
        other => {
            return Err(ApiError::invalid(format!(
                "a forward's caller scope is read or write, not {other:?}"
            )));
        }
    };
    Ok(Caller {
        // Not a token of this node's: forwarded calls hold no sessions.
        token: u32::MAX,
        scope,
        namespaces: (!f.all_namespaces).then_some(f.namespaces),
    })
}

/// A routed call as a forward's op. Only what [`crate::serve::router`] routes reaches here.
pub(crate) fn op_to_forward(op: ServiceOp) -> Result<forward_request::Op, ApiError> {
    use forward_request::Op;
    Ok(match op {
        ServiceOp::Get(r) => Op::Get(r),
        ServiceOp::ReadRow(r) => Op::ReadRow(r),
        ServiceOp::Mutate(r) => Op::Mutate(r),
        ServiceOp::CheckAndMutate(r) => Op::CheckAndMutate(r),
        ServiceOp::MutateReturning(r) => Op::MutateReturning(r),
        ServiceOp::Batch(r) => Op::Batch(r),
        other => {
            return Err(ApiError::internal(format!(
                "{} isn't a call that's forwarded",
                other.name()
            )));
        }
    })
}

/// A forward's op as the call it carries, on the owner.
pub(crate) fn op_of(op: Option<forward_request::Op>) -> Result<ServiceOp, ApiError> {
    use forward_request::Op;
    Ok(
        match op.ok_or_else(|| ApiError::invalid("a forward carries no call"))? {
            Op::Get(r) => ServiceOp::Get(r),
            Op::ReadRow(r) => ServiceOp::ReadRow(r),
            Op::Mutate(r) => ServiceOp::Mutate(r),
            Op::CheckAndMutate(r) => ServiceOp::CheckAndMutate(r),
            Op::MutateReturning(r) => ServiceOp::MutateReturning(r),
            Op::Batch(r) => ServiceOp::Batch(r),
        },
    )
}

/// The owner's reply, as a forward carries it.
pub(crate) fn reply_to_forward(reply: ServiceReply) -> Result<ForwardResponse, ApiError> {
    use forward_response::Reply;
    let reply = match reply {
        ServiceReply::Get(r) => Reply::Get(r),
        ServiceReply::ReadRow(r) => Reply::ReadRow(r),
        ServiceReply::Mutate(r) => Reply::Mutate(r),
        ServiceReply::CheckAndMutate(r) => Reply::CheckAndMutate(r),
        ServiceReply::MutateReturning(r) => Reply::MutateReturning(r),
        _ => {
            return Err(ApiError::internal(
                "a forwarded call answered an unexpected reply",
            ));
        }
    };
    Ok(ForwardResponse { reply: Some(reply) })
}

/// A forward's reply as the call's own, on the entry node.
pub(crate) fn reply_of(reply: ForwardResponse) -> Result<ServiceReply, ApiError> {
    use forward_response::Reply;
    Ok(
        match reply
            .reply
            .ok_or_else(|| ApiError::internal("a forward's reply is empty"))?
        {
            Reply::Get(r) => ServiceReply::Get(r),
            Reply::ReadRow(r) => ServiceReply::ReadRow(r),
            Reply::Mutate(r) => ServiceReply::Mutate(r),
            Reply::CheckAndMutate(r) => ServiceReply::CheckAndMutate(r),
            Reply::MutateReturning(r) => ServiceReply::MutateReturning(r),
        },
    )
}
