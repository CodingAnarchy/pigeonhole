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
use crate::serve::auth::{ANONYMOUS, Caller, Scope, TokenId, Tokens};
use crate::serve::errors::ApiError;
use crate::serve::proto::{
    ForwardRequest, ForwardResponse, ForwardScanRequest, ForwardedCaller, ScanPage,
    cluster_client::ClusterClient, forward_request, forward_response,
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
    /// Connections for forwarded streams (`ForwardScan`): one per stream while it runs,
    /// reused once it has ended (see [`StreamLease`]).
    stream_peers: Mutex<HashMap<String, Arc<StreamPeer>>>,
}

/// The most idle stream connections kept per peer for reuse.
const IDLE_STREAM_CHANNELS: usize = 4;

/// One peer's stream connections: none shared by two streams at once.
struct StreamPeer {
    endpoint: Endpoint,
    idle: Mutex<Vec<Channel>>,
}

/// A forwarded stream's connection, its own while the stream runs (simp724's S1 on #1698).
/// HTTP/2 counts a stream's unread data against its connection's window too, so a client that
/// stops reading one stream would, on a shared connection, stall every other stream there. A
/// stream that ended cleanly gives its connection back for the next one; a connection whose
/// stream failed or was dropped partway is closed.
pub(crate) struct StreamLease {
    channel: Option<Channel>,
    peer: Arc<StreamPeer>,
    clean: bool,
}

impl StreamLease {
    /// The connection, for a client of the stream.
    pub(crate) fn channel(&self) -> Channel {
        self.channel.clone().expect("held until the lease drops")
    }

    /// The stream ended cleanly: its connection may serve another.
    pub(crate) fn ended(&mut self) {
        self.clean = true;
    }
}

impl Drop for StreamLease {
    fn drop(&mut self) {
        if !self.clean {
            return;
        }
        if let Some(channel) = self.channel.take() {
            let mut idle = self
                .peer
                .idle
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if idle.len() < IDLE_STREAM_CHANNELS {
                idle.push(channel);
            }
        }
    }
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
            stream_peers: Mutex::new(HashMap::new()),
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
        // One deadline for the connect and the call (cli692's F1 on #1730): a node that
        // takes connections and never answers can't hold a call, or a fanned-out read's
        // pages' budget, past it (#1715).
        let deadline = tokio::time::Instant::now() + timeout;
        // Before the send: a connection that can't be made sent nothing.
        let channel =
            tokio::time::timeout_at(deadline, slot.get_or_try_init(|| peer.endpoint.connect()))
                .await
                .map_err(|_| {
                    ApiError::partition_unavailable(format!(
                        "the partition's node at {address} didn't take a connection in time"
                    ))
                })?
                .map_err(|e| {
                    ApiError::partition_unavailable(format!(
                        "the partition's node at {address} can't be reached: {e}"
                    ))
                })?
                .clone();
        let mut request = self.node.request(req).map_err(ApiError::internal)?;
        // `grpc-timeout` tells the owner what's left; the wait here ends at the same
        // deadline. That's after the send: a write's outcome is unknown (B3), a read's node
        // unavailable.
        request.set_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()));
        match tokio::time::timeout_at(deadline, peer_client(channel).forward(request)).await {
            Ok(Ok(reply)) => Ok(reply.into_inner()),
            Ok(Err(status)) => Err(classify(&status, address, writes)),
            Err(_) => Err(classify(
                &tonic::Status::deadline_exceeded("no answer within the call's deadline"),
                address,
                writes,
            )),
        }
    }

    /// Opens one partition's part of a fanned-out scan on the node at `address` (D317
    /// ticket 4), on the stream connections. The stream has no deadline of its own: its
    /// pages are paced by the client, and each side's `--stream-idle-ms` ends a stalled one.
    /// A failure is the owner's answer, or `PartitionUnavailable` (a read).
    pub(crate) async fn scan(
        &self,
        address: &str,
        req: ForwardScanRequest,
        open_within: Duration,
    ) -> Result<(tonic::Streaming<ScanPage>, StreamLease), ApiError> {
        let lease = self.stream_channel(address, open_within).await?;
        let request = self.node.request(req).map_err(ApiError::internal)?;
        let mut client = peer_client(lease.channel());
        match tokio::time::timeout(open_within, client.forward_scan(request)).await {
            Ok(Ok(reply)) => Ok((reply.into_inner(), lease)),
            Ok(Err(status)) => Err(classify(&status, address, false)),
            Err(_) => Err(ApiError::deadline_exceeded()),
        }
    }

    /// A connection to the node at `address` for one forwarded stream, its own while the
    /// stream runs: an idle one a finished stream gave back, or a new one (within
    /// `open_within`). Every node-to-node stream opens through here (`ForwardScan`, and
    /// ticket 7's feeds), never on the unary forwards' connections. A stream that ends
    /// cleanly calls [`StreamLease::ended`] to give the connection back; one that never does
    /// (a feed, which lives for hours) keeps a connection to itself, closed when it ends.
    pub(crate) async fn stream_channel(
        &self,
        address: &str,
        open_within: Duration,
    ) -> Result<StreamLease, ApiError> {
        let peer = self.stream_peer(address)?;
        let idle = peer
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop();
        let channel = match idle {
            Some(c) => c,
            None => tokio::time::timeout(open_within, peer.endpoint.connect())
                .await
                .map_err(|_| ApiError::deadline_exceeded())?
                .map_err(|e| {
                    ApiError::partition_unavailable(format!(
                        "the partition's node at {address} can't be reached: {e}"
                    ))
                })?,
        };
        Ok(StreamLease {
            channel: Some(channel),
            peer,
            clean: false,
        })
    }

    fn stream_peer(&self, address: &str) -> Result<Arc<StreamPeer>, ApiError> {
        let mut peers = self
            .stream_peers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(p) = peers.get(address) {
            return Ok(Arc::clone(p));
        }
        if let Some(routes) = self.node.routes().as_ref() {
            peers.retain(|a, _| routes.has_address(a));
        }
        let endpoint = self
            .node
            .endpoint(address)
            .map_err(|e| ApiError::partition_unavailable(format!("node at {address}: {e}")))?;
        let peer = Arc::new(StreamPeer {
            endpoint,
            idle: Mutex::new(Vec::new()),
        });
        peers.insert(address.to_owned(), Arc::clone(&peer));
        Ok(peer)
    }
}

/// A forwarded scan's failure partway (a page that didn't arrive) as the client's error.
pub(crate) fn stream_error(status: &tonic::Status, address: &str) -> ApiError {
    classify(status, address, false)
}

/// What a failed forward answers the client (B3).
/// A forward's envelope over the call it carries (the caller, the map version, the op's
/// framing): the owner's decode limit is the node's `--max-request-bytes` plus this, so a
/// call the entry node took isn't refused by the owner for its envelope (audit120's S-C2).
pub(crate) const FORWARD_ENVELOPE: usize = 64 << 10;

/// The `Cluster` client a node calls a peer with. Its replies aren't capped here (tonic's
/// default is 4 MiB): a forwarded reply or page is as large as the owner's own limits let
/// it be, a cell or a page up to 8 MiB among them, and a peer is a node of this cluster,
/// as a replica trusts its primary (audit120's S-C2).
pub(crate) fn peer_client(
    channel: tonic::transport::Channel,
) -> ClusterClient<tonic::transport::Channel> {
    ClusterClient::new(channel).max_decoding_message_size(usize::MAX)
}

pub(crate) fn classify(status: &tonic::Status, address: &str, writes: bool) -> ApiError {
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
/// any failure to connect (tonic's `ConnectError`: refused, timed out, unreachable), a
/// `REFUSED_STREAM` reset, or a `GOAWAY` (the server names the last stream it processed, so
/// the client fails only streams it didn't). Either way nothing ran (cli692's point 5).
fn refused_unprocessed(status: &tonic::Status) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(status);
    while let Some(e) = source {
        if let Some(h2) = e.downcast_ref::<h2::Error>() {
            return h2.reason() == Some(h2::Reason::REFUSED_STREAM) || h2.is_go_away();
        }
        // A channel (re)connecting to the owner that couldn't: refused, timed out (Windows
        // reports a dead loopback port as a connect timeout, not a refusal), or unreachable.
        // A connection that was never made sent nothing, whatever the OS says inside it.
        if e.downcast_ref::<tonic::ConnectError>().is_some() {
            return true;
        }
        if let Some(io) = e.downcast_ref::<std::io::Error>()
            && io.kind() == std::io::ErrorKind::ConnectionRefused
        {
            return true;
        }
        source = e.source();
    }
    false
}

/// `caller` as a forward carries it: its scope (a cluster token makes no data or schema
/// call) and its namespaces.
pub(crate) fn forwarded_caller(caller: &Caller) -> Result<ForwardedCaller, ApiError> {
    let scope = match caller.scope {
        Scope::Read => "read",
        // An admin may make any data call a write token may. A schema change at the map's
        // writer takes the caller's scope from the writer's own token entry (D317 amendment
        // 1), never from the message.
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
        // Its sessions' binding (D317 ticket 3); none on a cluster without tokens.
        token_digest: if caller.principal == ANONYMOUS {
            prost::bytes::Bytes::new()
        } else {
            prost::bytes::Bytes::copy_from_slice(caller.principal.digest())
        },
    })
}

/// The caller a forward is for, on the owner (D317 ticket 3): this node's own entry for
/// the token's digest, so its scope (capped at `write`, D317 ticket 2) and namespaces are
/// what this node lists, and its sessions are bound to the token as on a single node
/// (cli692's R1 on #1702). A digest this node doesn't list, or any digest on a node without
/// tokens, is `PermissionDenied` (R2): a cluster's nodes list the same tokens. Without
/// tokens anywhere, the forward's scope and namespaces stand, and the caller is anyone.
pub(crate) fn caller_of(f: Option<ForwardedCaller>, tokens: &Tokens) -> Result<Caller, ApiError> {
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
    if !tokens.any() {
        if !f.token_digest.is_empty() {
            return Err(ApiError::unlisted_token());
        }
        return Ok(Caller {
            principal: ANONYMOUS,
            token: TokenId::ANONYMOUS,
            scope,
            namespaces: (!f.all_namespaces).then_some(f.namespaces),
            // The reply goes back to the forwarding node as protobuf; that node charges and
            // renders it for its own client (#1567).
            json: false,
        });
    }
    // Never the digest in the message (cli692's S1 on ticket 3).
    let digest: [u8; 32] = f
        .token_digest
        .as_ref()
        .try_into()
        .map_err(|_| ApiError::unlisted_token())?;
    let mut caller = tokens
        .caller_by_digest(&digest)
        .ok_or_else(ApiError::unlisted_token)?;
    caller.scope = match caller.scope {
        Scope::Read => Scope::Read,
        Scope::Write | Scope::Admin => Scope::Write,
        Scope::Cluster => {
            return Err(ApiError::permission_denied(
                "make a data call with a cluster token",
            ));
        }
    };
    Ok(caller)
}

/// The caller of a schema change forwarded to the map's writer (D317 amendment 1): this
/// node's own entry for the token's digest, uncapped, so the writer checks a drop's `admin`
/// itself, as a single node does. [`ForwardedCaller`]'s scope stays `read` or `write`: no
/// forward carries `admin`. Without tokens anywhere, the caller is anyone, with every scope,
/// as on a single node without tokens. A digest this node doesn't list, a digest sent to a
/// node without tokens, or a `cluster` token, is refused.
pub(crate) fn schema_caller_of(
    f: Option<ForwardedCaller>,
    tokens: &Tokens,
) -> Result<Caller, ApiError> {
    let f = f.ok_or_else(|| ApiError::invalid("a forward names no caller"))?;
    if !tokens.any() {
        if !f.token_digest.is_empty() {
            return Err(ApiError::unlisted_token());
        }
        return Ok(Caller::anonymous());
    }
    let digest: [u8; 32] = f
        .token_digest
        .as_ref()
        .try_into()
        .map_err(|_| ApiError::unlisted_token())?;
    let caller = tokens
        .caller_by_digest(&digest)
        .ok_or_else(ApiError::unlisted_token)?;
    if caller.scope == Scope::Cluster {
        return Err(ApiError::permission_denied(
            "make a schema call with a cluster token",
        ));
    }
    Ok(caller)
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
        ServiceOp::MultiGet(r) => Op::MultiGet(r),
        ServiceOp::Begin(r) => Op::Begin(r),
        ServiceOp::TxnGet(r) => Op::TxnGet(r),
        ServiceOp::TxnGetRow(r) => Op::TxnGetRow(r),
        ServiceOp::TxnWrite(r) => Op::TxnWrite(r),
        ServiceOp::Commit(r) => Op::Commit(r),
        ServiceOp::Abort(r) => Op::Abort(r),
        ServiceOp::OpenSnapshot(r) => Op::OpenSnapshot(r),
        ServiceOp::ReleaseSnapshot(r) => Op::ReleaseSnapshot(r),
        ServiceOp::Nearest(r) => Op::Nearest(r),
        ServiceOp::CreateTable(r) => Op::CreateTable(r),
        ServiceOp::DropTable(r) => Op::DropTable(r),
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
            Op::MultiGet(r) => ServiceOp::MultiGet(r),
            Op::Begin(r) => ServiceOp::Begin(r),
            Op::TxnGet(r) => ServiceOp::TxnGet(r),
            Op::TxnGetRow(r) => ServiceOp::TxnGetRow(r),
            Op::TxnWrite(r) => ServiceOp::TxnWrite(r),
            Op::Commit(r) => ServiceOp::Commit(r),
            Op::Abort(r) => ServiceOp::Abort(r),
            Op::OpenSnapshot(r) => ServiceOp::OpenSnapshot(r),
            Op::ReleaseSnapshot(r) => ServiceOp::ReleaseSnapshot(r),
            Op::Nearest(r) => ServiceOp::Nearest(r),
            Op::CreateTable(r) => ServiceOp::CreateTable(r),
            Op::DropTable(r) => ServiceOp::DropTable(r),
        },
    )
}

/// The owner's reply, as a forward carries it, and a read's charge (#1567), which the
/// response holds until its body has been written (cli692's F1 on ticket 2).
pub(crate) fn reply_to_forward(
    reply: ServiceReply,
) -> Result<(ForwardResponse, Option<crate::serve::charge::Charge>), ApiError> {
    use forward_response::Reply;
    let (reply, charge) = reply.uncharge();
    let reply = match reply {
        ServiceReply::Get(r) => Reply::Get(r),
        ServiceReply::ReadRow(r) => Reply::ReadRow(r),
        ServiceReply::Mutate(r) => Reply::Mutate(r),
        ServiceReply::CheckAndMutate(r) => Reply::CheckAndMutate(r),
        ServiceReply::MutateReturning(r) => Reply::MutateReturning(r),
        ServiceReply::MultiGet(r) => Reply::MultiGet(r),
        ServiceReply::Begin(r) => Reply::Begin(r),
        ServiceReply::Commit(r) => Reply::Commit(r),
        ServiceReply::OpenSnapshot(r) => Reply::OpenSnapshot(r),
        ServiceReply::Table(r) => Reply::Table(r),
        ServiceReply::Empty(r) => Reply::Empty(r),
        ServiceReply::Nearest(r) => Reply::Nearest(r),
        _ => {
            return Err(ApiError::internal(
                "a forwarded call answered an unexpected reply",
            ));
        }
    };
    Ok((ForwardResponse { reply: Some(reply) }, charge))
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
            Reply::MultiGet(r) => ServiceReply::MultiGet(r),
            Reply::Begin(r) => ServiceReply::Begin(r),
            Reply::Commit(r) => ServiceReply::Commit(r),
            Reply::OpenSnapshot(r) => ServiceReply::OpenSnapshot(r),
            Reply::Table(r) => ServiceReply::Table(r),
            Reply::Empty(r) => ServiceReply::Empty(r),
            Reply::Nearest(r) => ServiceReply::Nearest(r),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::{caller_of, classify};
    use crate::serve::auth::{Scope, Tokens};
    use crate::serve::proto::ForwardedCaller;

    fn forwarded(scope: &str, token: &[u8]) -> Option<ForwardedCaller> {
        let digest: [u8; 32] = <sha2::Sha256 as sha2::Digest>::digest(token).into();
        Some(ForwardedCaller {
            scope: scope.into(),
            namespaces: Vec::new(),
            all_namespaces: true,
            token_digest: if token.is_empty() {
                Default::default()
            } else {
                digest.to_vec().into()
            },
        })
    }

    /// The owner's own entry for the token decides the forwarded caller's scope and
    /// namespaces, whatever the entry node sent (cli692's R1 on #1702); a digest it doesn't
    /// list, or any digest on a node without tokens, is refused (R2).
    #[test]
    fn a_forwarded_caller_is_what_this_node_lists_for_its_token() {
        let tokens =
            Tokens::parse("read ns=team-a t0ken\nadmin b0ss\ncluster n0de\n", "t").unwrap();
        let c = caller_of(forwarded("write", b"t0ken"), &tokens).unwrap();
        assert_eq!(c.scope, Scope::Read);
        assert_eq!(c.namespaces, Some(vec!["team-a".to_owned()]));
        let boss = caller_of(forwarded("read", b"b0ss"), &tokens).unwrap();
        assert_eq!((boss.scope, boss.namespaces), (Scope::Write, None));
        for (f, why) in [
            (forwarded("write", b"other"), "unlisted"),
            (forwarded("write", b""), "no digest"),
            (forwarded("write", b"n0de"), "a cluster token"),
        ] {
            let e = caller_of(f, &tokens).unwrap_err();
            assert_eq!(e.code, "PermissionDenied", "{why}: {}", e.message);
        }
        let none = Tokens::parse("", "t").unwrap();
        let e = caller_of(forwarded("write", b"t0ken"), &none).unwrap_err();
        assert_eq!(e.code, "PermissionDenied", "{}", e.message);
        let anyone = caller_of(forwarded("write", b""), &none).unwrap();
        assert_eq!(anyone.scope, Scope::Write);
    }

    /// tonic's status for a failed connect, as the channel builds it: a `ConnectError` around
    /// the OS's error.
    fn connect_failed(kind: std::io::ErrorKind) -> tonic::Status {
        let io = std::io::Error::new(kind, "connect");
        let err = tonic::ConnectError(Box::new(io));
        tonic::Status::from_error(Box::new(err))
    }

    #[test]
    fn a_forward_that_never_connected_is_retry_safe_however_the_os_says_it() {
        // Windows reports a closed loopback port as a connect timeout (#1686's CI): that
        // write was never sent, so it's PartitionUnavailable, not OutcomeUnknown.
        for kind in [
            std::io::ErrorKind::ConnectionRefused,
            std::io::ErrorKind::TimedOut,
            std::io::ErrorKind::HostUnreachable,
        ] {
            let e = classify(&connect_failed(kind), "127.0.0.1:1", true);
            assert_eq!(e.code, "PartitionUnavailable", "{kind:?}: {}", e.message);
        }
        // A failure after the send, with no connect error, leaves a write's outcome unknown.
        let e = classify(&tonic::Status::unavailable("connection reset"), "x", true);
        assert_eq!(e.code, "OutcomeUnknown");
        let e = classify(&tonic::Status::unavailable("connection reset"), "x", false);
        assert_eq!(e.code, "PartitionUnavailable");
    }
}
