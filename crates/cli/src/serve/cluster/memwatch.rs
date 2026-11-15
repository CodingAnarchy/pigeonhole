//! `Memory.Watch` on a cluster node (#1915 PR 2b, ICR 0139): one component per partition
//! holding the watched namespaces (every partition of the placement for a watch of every
//! namespace), merged here as their commits arrive.
//!
//! - **Components:** a partition served here is watched here; another node's over
//!   `Cluster.ForwardMemoryWatch`, on the watch's own connection to it. Either way the
//!   component is the partition's own watch ([`crate::serve::memory::watch`] with a
//!   [`WatchPart`](crate::serve::memory::WatchPart)): delivered under the partition's lease,
//!   ended by its close (`PartitionUnavailable`) or a move (`WrongPartition`).
//! - **Whole commits:** every event of a commit carries the same token, which points after
//!   the whole commit. A component's stream marks commits with `Progress` events (one first,
//!   at its start, and one after each commit with memories), and the merge moves each commit
//!   as one, so no event of another partition lands inside it and resuming from any event's
//!   token never skips the rest of a commit. The marks never reach the client.
//! - **The token** is a [`ClusterToken`] on every event: each partition at its last whole
//!   commit delivered, the event's own at the end of its commit.
//!   [`memory_watch_hash`] ties it to the store and namespaces.
//! - **Nothing is delivered before every component's start mark**, so the first event's
//!   token covers every partition.
//! - A component's error ends the watch (no `allow_partial`); the client resumes from its
//!   last token. Events keep each partition's order; there's no order across partitions.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_core::Stream;
use prost::bytes::Bytes;
use tokio::sync::mpsc;

use super::Node;
use super::token::{At, ClusterToken, TokenCache, memory_watch_hash};
use crate::serve::auth::Caller;
use crate::serve::errors::ApiError;
use crate::serve::feed::TakeClock;
use crate::serve::memory::MemoryEvents;
use crate::serve::proto;
use crate::serve::router::Parts;
use crate::serve::service::Service;

/// `Cluster.ForwardMemoryWatch`'s path.
pub(crate) const FORWARD_MEMORY_WATCH: &str = "/pigeonhole.v1.Cluster/ForwardMemoryWatch";

/// The events a merged watch buffers for its client, as a single node's watch does.
const EVENTS: usize = 64;

type Component = Pin<Box<dyn Stream<Item = Result<proto::MemoryEvent, ApiError>> + Send>>;

/// `Memory.Watch` on a cluster node: checks it, opens a component on every partition it
/// covers, then merges them on a task of its own. Holds one `--max-streams` place here for
/// the whole watch; each component holds one on its partition's node.
pub(crate) async fn watch(
    node: Arc<Node>,
    forwarder: Arc<super::forward::Forwarder>,
    parts: Parts,
    service: Arc<Service>,
    r: proto::MemoryWatchRequest,
    caller: &Caller,
) -> Result<MemoryEvents, ApiError> {
    // The single node's scope rule, here and again on each owner: a namespace-scoped token
    // names its namespaces, all of them its own.
    crate::memcore::Scope(caller.namespaces.as_deref())
        .check_all(&r.namespaces)
        .map_err(ApiError::from)?;
    let mut namespaces = r.namespaces.clone();
    namespaces.sort();
    namespaces.dedup();
    let guard = node.routes();
    let Some(routes) = guard.as_ref() else {
        return Err(ApiError::partition_unavailable(
            "this cluster node has no map yet",
        ));
    };
    let Some(store) = routes.memory_store() else {
        return Err(ApiError::unsupported(
            "this cluster has no memory store: its map places none (ClusterMap.memory)",
        ));
    };
    // Each partition the watch covers, with its namespaces there (none: every namespace).
    let placed: BTreeMap<String, Vec<String>> = if namespaces.is_empty() {
        routes
            .memory_partitions()
            .iter()
            .map(|p| (p.clone(), Vec::new()))
            .collect()
    } else {
        crate::serve::router::memory_partitions(routes, namespaces.iter().map(String::as_str))?
    };
    let hash = memory_watch_hash(store, &namespaces);
    let cluster_id = routes.cluster_id().to_owned();
    let map_version = routes.map_version();
    let mut starts: BTreeMap<String, (u64, Bytes)> = BTreeMap::new();
    if r.resume.is_empty() {
        for p in placed.keys() {
            let epoch = routes.owner(p).map_or(0, |o| o.epoch);
            starts.insert(p.clone(), (epoch, Bytes::new()));
        }
    } else {
        let t = ClusterToken::decode(&r.resume).map_err(|why| {
            ApiError::invalid(format!(
                "the resume token isn't a cluster memory watch's token ({why})"
            ))
        })?;
        starts = resume_starts(&t, &cluster_id, hash, &placed, |p| {
            routes.owner(p).map_or(0, |o| o.history_epoch)
        })?;
    }
    drop(guard);
    let slot = service.stream_slot()?;
    let within = service.request_timeout();
    let mut open = Open {
        within,
        node,
        forwarder,
        parts,
        caller: caller.clone(),
        map_version,
        channels: Vec::new(),
    };
    // Every component open, and its start mark taken, before anything is delivered: all
    // within the request timeout, since a component delivers nothing, its start mark
    // included, before its partition's first lease (simp724's S5).
    let deadline = tokio::time::Instant::now() + within;
    let mut components = Vec::with_capacity(placed.len());
    for (p, ns) in placed {
        let (epoch, resume) = starts.remove(&p).expect("a start for every partition");
        let request = proto::MemoryWatchRequest {
            namespaces: ns,
            resume,
        };
        let started = tokio::time::timeout_at(deadline, async {
            let mut events = open.open(&p, request).await?;
            match next(&mut events).await {
                Some(Ok(proto::MemoryEvent {
                    token,
                    event: Some(proto::memory_event::Event::Progress(_)),
                })) => Ok((events, token)),
                Some(Err(e)) => Err(e),
                _ => Err(ApiError::internal(format!(
                    "partition {p:?}'s watch didn't start with its position"
                ))),
            }
        })
        .await;
        let (events, start) = match started {
            Ok(started) => started?,
            Err(_) => {
                return Err(ApiError::partition_unavailable(format!(
                    "partition {p:?}'s watch didn't start within {} ms (its node may hold no \
                     lease for it yet): watch again",
                    within.as_millis()
                )));
            }
        };
        components.push(Part {
            partition: p,
            epoch,
            at: At::Token(start),
            events,
            pending: Vec::new(),
        });
    }
    let taken = Arc::new(TakeClock::new());
    let (tx, rx) = mpsc::channel(EVENTS);
    let idle = service.limits().stream_idle;
    let merge = Merge {
        cluster_id,
        map_version,
        hash,
        cache: TokenCache::default(),
        parts: components,
        next: 0,
    };
    let took = Arc::clone(&taken);
    // The connections to other nodes last as long as the merge.
    let channels = open.channels;
    tokio::spawn(async move {
        let _slot = slot;
        let _channels = channels;
        merge.run(tx, took, idle).await;
    });
    Ok(MemoryEvents::from_channel(rx, taken))
}

/// Each component's start from a resume token, after checking that it's this cluster's and
/// this watch's, and that no partition's history changed under it (D317 ticket 6, as a
/// feed's token).
fn resume_starts(
    t: &ClusterToken,
    cluster_id: &str,
    hash: u64,
    placed: &BTreeMap<String, Vec<String>>,
    history_epoch: impl Fn(&str) -> u64,
) -> Result<BTreeMap<String, (u64, Bytes)>, ApiError> {
    if t.cluster_id != cluster_id {
        return Err(ApiError::invalid(format!(
            "the token is cluster {:?}'s, not this one's",
            t.cluster_id
        )));
    }
    if t.subscription != hash {
        return Err(ApiError::invalid(
            "the token is another watch's (its store or namespaces differ), or a feed's",
        ));
    }
    let theirs: Vec<&str> = t.components.iter().map(|c| c.partition.as_str()).collect();
    let ours: Vec<&str> = placed.keys().map(String::as_str).collect();
    if theirs != ours {
        return Err(ApiError::invalid(format!(
            "the token covers partitions {theirs:?}, and this watch covers {ours:?}"
        )));
    }
    let mut starts = BTreeMap::new();
    for c in &t.components {
        let began = history_epoch(&c.partition);
        if began > c.epoch {
            return Err(ApiError::from_code(
                pigeonhole::ErrorCode::FeedExpired,
                &format!(
                    "partition {:?} has another history since the token (taken at epoch {}, \
                     its history began at {began}: a promotion or a restore): watch again",
                    c.partition, c.epoch
                ),
            ));
        }
        let At::Token(at) = &c.at else {
            return Err(ApiError::invalid(
                "a memory watch's token has a position for every partition",
            ));
        };
        starts.insert(c.partition.clone(), (c.epoch, at.clone()));
    }
    Ok(starts)
}

/// What opening a component needs.
struct Open {
    /// How long a connection to another node may take to open.
    within: std::time::Duration,
    node: Arc<Node>,
    forwarder: Arc<super::forward::Forwarder>,
    parts: Parts,
    caller: Caller,
    map_version: u64,
    /// The watch's connections to other nodes, one per node (simp724's S1 on the feed).
    channels: Vec<(String, super::forward::StreamLease)>,
}

impl Open {
    async fn open(
        &mut self,
        partition: &str,
        request: proto::MemoryWatchRequest,
    ) -> Result<Component, ApiError> {
        let owner = {
            let routes = self.node.routes();
            let routes = routes
                .as_ref()
                .ok_or_else(|| ApiError::partition_unavailable("this node has no map"))?;
            routes.owner(partition).cloned().ok_or_else(|| {
                ApiError::internal(format!("partition {partition:?} isn't in the map"))
            })?
        };
        if owner.node == self.node.id() {
            let Some(here) = self.parts.load().get(partition).cloned() else {
                return Err(ApiError::partition_unavailable(format!(
                    "the map gives this node partition {partition:?}, but it isn't open here"
                )));
            };
            let events = here
                .memory_watch(request, &self.caller)
                .await
                .map_err(|e| super::feed::closed_since(&self.parts, partition, &here, e))?;
            super::feed::still_owner(&self.node, partition)?;
            super::feed::still_served(&self.parts, partition, &here)?;
            return Ok(Box::pin(events));
        }
        let channel = match self.channels.iter().find(|(a, _)| *a == owner.address) {
            Some((_, lease)) => lease.channel(),
            None => {
                let within = self.within;
                let lease = self
                    .forwarder
                    .stream_channel(&owner.address, within)
                    .await
                    .map_err(|e| match e.code.as_str() {
                        "DeadlineExceeded" => ApiError::partition_unavailable(format!(
                            "the partition's node at {} didn't accept a connection within {} \
                             ms",
                            owner.address,
                            within.as_millis()
                        )),
                        _ => e,
                    })?;
                let channel = lease.channel();
                self.channels.push((owner.address.clone(), lease));
                channel
            }
        };
        let req = proto::ForwardMemoryWatchRequest {
            map_version: self.map_version,
            caller: Some(super::forward::forwarded_caller(&self.caller)?),
            partition: partition.to_owned(),
            request: Some(request),
        };
        let request = self.node.request(req).map_err(ApiError::internal)?;
        // A memory's event can be larger than tonic's default 4 MiB cap.
        let mut grpc = tonic::client::Grpc::new(channel).max_decoding_message_size(usize::MAX);
        let address = owner.address.clone();
        let failed = |status: tonic::Status| match status.code() {
            // A node built before #1915's PR 2b.
            tonic::Code::Unimplemented => ApiError::unsupported(format!(
                "the node at {address} holding partition {partition:?} doesn't serve a \
                 memory watch across nodes (an older build): {}",
                status.message()
            )),
            _ => super::forward::classify(&status, &address, false),
        };
        grpc.ready()
            .await
            .map_err(|e| failed(tonic::Status::unavailable(e.to_string())))?;
        let path = tonic::codegen::http::uri::PathAndQuery::from_static(FORWARD_MEMORY_WATCH);
        let stream = grpc
            .server_streaming(request, path, tonic_prost::ProstCodec::default())
            .await
            .map_err(failed)?
            .into_inner();
        let address = owner.address;
        Ok(Box::pin(Remote { stream, address }))
    }
}

/// Another node's component.
struct Remote {
    stream: tonic::Streaming<proto::MemoryEvent>,
    address: String,
}

impl Stream for Remote {
    type Item = Result<proto::MemoryEvent, ApiError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        Pin::new(&mut this.stream).poll_next(cx).map(|item| {
            item.map(|r| r.map_err(|s| super::forward::classify(&s, &this.address, false)))
        })
    }
}

async fn next(events: &mut Component) -> Option<Result<proto::MemoryEvent, ApiError>> {
    std::future::poll_fn(|cx| events.as_mut().poll_next(cx)).await
}

/// A component in the merge.
struct Part {
    partition: String,
    epoch: u64,
    /// After its last whole commit delivered.
    at: At,
    events: Component,
    /// The memories of the commit it's in the middle of, until the commit's end mark.
    pending: Vec<proto::WatchedMemory>,
}

/// A component's whole commit: its memories (none: a progress commit) and its end.
struct Commit {
    memories: Vec<proto::WatchedMemory>,
    token: Bytes,
}

struct Merge {
    cluster_id: String,
    map_version: u64,
    hash: u64,
    cache: TokenCache,
    parts: Vec<Part>,
    /// The component polled first next time, for fairness.
    next: usize,
}

impl Merge {
    /// The next whole commit of any component, or the error that ends the watch.
    fn poll_commit(&mut self, cx: &mut Context<'_>) -> Poll<Result<(usize, Commit), ApiError>> {
        let n = self.parts.len();
        for k in 0..n {
            let i = (self.next + k) % n;
            let part = &mut self.parts[i];
            loop {
                let event = match part.events.as_mut().poll_next(cx) {
                    Poll::Pending => break,
                    Poll::Ready(None) => {
                        return Poll::Ready(Err(ApiError::partition_unavailable(format!(
                            "partition {:?}'s watch ended",
                            part.partition
                        ))));
                    }
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                    Poll::Ready(Some(Ok(e))) => e,
                };
                match event.event {
                    Some(proto::memory_event::Event::Memory(m)) => part.pending.push(m),
                    // A commit's end mark, or a commit with no memories watched.
                    Some(proto::memory_event::Event::Progress(_)) => {
                        self.next = (i + 1) % n;
                        let memories = std::mem::take(&mut part.pending);
                        return Poll::Ready(Ok((
                            i,
                            Commit {
                                memories,
                                token: event.token,
                            },
                        )));
                    }
                    None => {}
                }
            }
        }
        Poll::Pending
    }

    /// The token after component `i`'s commit ending at `token`, the others where they are.
    fn token(&mut self, i: usize, token: Bytes) -> Bytes {
        self.parts[i].at = At::Token(token);
        let mut out = Vec::new();
        self.cache.encode(
            &self.cluster_id,
            self.map_version,
            self.hash,
            self.parts
                .iter()
                .map(|p| (p.partition.as_str(), p.epoch, &p.at)),
            &mut out,
        );
        out.into()
    }

    async fn run(
        mut self,
        tx: mpsc::Sender<Result<proto::MemoryEvent, ApiError>>,
        taken: Arc<TakeClock>,
        idle: std::time::Duration,
    ) {
        // Room in the client's channel within `--stream-idle-ms` of its last take.
        let send = |e| async {
            let mut e = Some(e);
            loop {
                match tokio::time::timeout(idle, tx.reserve()).await {
                    Ok(Ok(room)) => {
                        room.send(e.take().expect("sent once"));
                        return true;
                    }
                    Ok(Err(_)) => return false,
                    Err(_) if taken.last_or_start().elapsed() >= idle => return false,
                    Err(_) => {}
                }
            }
        };
        loop {
            let next = tokio::select! {
                biased;
                // A client that went away ends the merge, and with it the components.
                () = tx.closed() => return,
                next = std::future::poll_fn(|cx| self.poll_commit(cx)) => next,
            };
            let (i, commit) = match next {
                Ok(c) => c,
                Err(e) => {
                    let _ = send(Err(e)).await;
                    return;
                }
            };
            let token = self.token(i, commit.token);
            if commit.memories.is_empty() {
                let progress = proto::MemoryEvent {
                    token,
                    event: Some(proto::memory_event::Event::Progress(proto::Progress {})),
                };
                if !send(Ok(progress)).await {
                    return;
                }
                continue;
            }
            for m in commit.memories {
                let e = proto::MemoryEvent {
                    token: token.clone(),
                    event: Some(proto::memory_event::Event::Memory(m)),
                };
                if !send(Ok(e)).await {
                    return;
                }
            }
        }
    }
}

/// The owner's side of `Cluster.ForwardMemoryWatch`: served only if this node's map gives it
/// the partition, the memory store is placed on it, and every namespace named is in it;
/// otherwise `WrongPartition` with this node's map version, never forwarded on.
pub(crate) async fn serve(
    node: &Node,
    parts: &Parts,
    req: proto::ForwardMemoryWatchRequest,
) -> Result<MemoryEvents, ApiError> {
    let caller = super::forward::caller_of(req.caller, &node.tokens)?;
    let r = req
        .request
        .ok_or_else(|| ApiError::invalid("a ForwardMemoryWatch carries no request"))?;
    let service = {
        let routes = node.routes();
        let Some(routes) = routes.as_ref() else {
            return Err(ApiError::wrong_partition(
                0,
                "this node has no cluster map yet",
            ));
        };
        let mine = routes
            .owner(&req.partition)
            .is_some_and(|o| o.node == node.id());
        let placed = routes.memory_partitions().contains(&req.partition)
            && r.namespaces
                .iter()
                .all(|ns| routes.memory_partition(ns) == Some(req.partition.as_str()));
        if !mine || !placed {
            return Err(ApiError::wrong_partition(
                routes.map_version(),
                format!(
                    "this node doesn't serve partition {:?}'s memory with those namespaces",
                    req.partition
                ),
            ));
        }
        let Some(service) = parts.load().get(&req.partition).cloned() else {
            return Err(ApiError::partition_unavailable(format!(
                "the map gives this node partition {:?}, but it isn't open here",
                req.partition
            )));
        };
        service
    };
    let events = service
        .memory_watch(r, &caller)
        .await
        .map_err(|e| super::feed::closed_since(parts, &req.partition, &service, e))?;
    super::feed::still_owner(node, &req.partition)?;
    super::feed::still_served(parts, &req.partition, &service)?;
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::cluster::token::{Component as TokenPart, subscription_hash};

    /// A component fed by the test.
    struct Fed(mpsc::UnboundedReceiver<Result<proto::MemoryEvent, ApiError>>);

    impl Stream for Fed {
        type Item = Result<proto::MemoryEvent, ApiError>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.0.poll_recv(cx)
        }
    }

    fn memory(text: &str, token: &'static [u8]) -> Result<proto::MemoryEvent, ApiError> {
        Ok(proto::MemoryEvent {
            token: Bytes::from_static(token),
            event: Some(proto::memory_event::Event::Memory(proto::WatchedMemory {
                text: text.into(),
                ..Default::default()
            })),
        })
    }

    fn mark(token: &'static [u8]) -> Result<proto::MemoryEvent, ApiError> {
        Ok(proto::MemoryEvent {
            token: Bytes::from_static(token),
            event: Some(proto::memory_event::Event::Progress(proto::Progress {})),
        })
    }

    /// Each partition's position in a merged event's token.
    fn positions(token: &[u8]) -> Vec<(String, Bytes)> {
        let t = ClusterToken::decode(token).unwrap();
        assert_eq!(
            (t.cluster_id.as_str(), t.map_version, t.subscription),
            ("c1", 3, 7)
        );
        t.components
            .into_iter()
            .map(|c| match c.at {
                At::Token(b) => (c.partition, b),
                other => panic!("a position: {other:?}"),
            })
            .collect()
    }

    /// #1915 PR 2b: the merge moves a component's commit whole. Partition a's first memory
    /// arrives, then partition b's whole commit, then the rest of a's commit: b's commit is
    /// delivered first, then a's two memories together. Every event's token holds each
    /// partition at its last whole commit delivered, its own at its commit's end, so resuming
    /// from b's event doesn't skip a's commit.
    #[tokio::test]
    async fn a_merged_watch_delivers_each_partitions_commit_whole() {
        let (a_tx, a_rx) = mpsc::unbounded_channel();
        let (b_tx, b_rx) = mpsc::unbounded_channel();
        let part = |p: &str, events: Fed, start: &'static [u8]| Part {
            partition: p.into(),
            epoch: 1,
            at: At::Token(Bytes::from_static(start)),
            events: Box::pin(events),
            pending: Vec::new(),
        };
        let merge = Merge {
            cluster_id: "c1".into(),
            map_version: 3,
            hash: 7,
            cache: TokenCache::default(),
            parts: vec![part("a", Fed(a_rx), b"a0"), part("b", Fed(b_rx), b"b0")],
            next: 0,
        };
        let (tx, mut rx) = mpsc::channel(16);
        let taken = Arc::new(TakeClock::new());
        tokio::spawn(merge.run(tx, taken, std::time::Duration::from_secs(5)));
        a_tx.send(memory("a1", b"a1")).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        b_tx.send(memory("b1", b"b1")).unwrap();
        b_tx.send(mark(b"b1")).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        a_tx.send(memory("a2", b"a1")).unwrap();
        a_tx.send(mark(b"a1")).unwrap();
        // A commit with no memories watched: progress.
        b_tx.send(mark(b"b2")).unwrap();
        let mut got = Vec::new();
        for _ in 0..4 {
            let e = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let what = match e.event {
                Some(proto::memory_event::Event::Memory(m)) => m.text,
                _ => "progress".to_owned(),
            };
            got.push((what, positions(&e.token)));
        }
        let at = |a: &'static [u8], b: &'static [u8]| {
            vec![
                ("a".to_owned(), Bytes::from_static(a)),
                ("b".to_owned(), Bytes::from_static(b)),
            ]
        };
        assert_eq!(
            got,
            vec![
                ("b1".to_owned(), at(b"a0", b"b1")),
                ("a1".to_owned(), at(b"a1", b"b1")),
                ("a2".to_owned(), at(b"a1", b"b1")),
                ("progress".to_owned(), at(b"a1", b"b2")),
            ]
        );
        // A component that ends without an error ends the watch with `PartitionUnavailable`.
        drop(a_tx);
        let e = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(e.code, "PartitionUnavailable", "{}", e.message);
        assert!(rx.recv().await.is_none());
    }

    /// A watch's token: this cluster's, this watch's, every partition's position, and no
    /// partition's history changed under it. A watch of every namespace, another namespace
    /// set, another store and a feed subscription each hash apart.
    #[test]
    fn a_watch_resumes_only_from_its_own_token() {
        let all = memory_watch_hash("notes", &[]);
        let alice = memory_watch_hash("notes", &["alice".to_owned()]);
        let both = memory_watch_hash("notes", &["alice".to_owned(), "bob".to_owned()]);
        let other = memory_watch_hash("other", &["alice".to_owned()]);
        let feed = subscription_hash(&["alice".to_owned()], b"", false);
        let hashes = [all, alice, both, other, feed];
        for (i, h) in hashes.iter().enumerate() {
            for g in &hashes[i + 1..] {
                assert_ne!(h, g, "{hashes:?}");
            }
        }
        let token = |hash, parts: &[&str], at: At| ClusterToken {
            cluster_id: "c1".into(),
            map_version: 3,
            subscription: hash,
            components: parts
                .iter()
                .map(|p| TokenPart {
                    partition: (*p).into(),
                    epoch: 2,
                    at: at.clone(),
                })
                .collect(),
        };
        let placed: BTreeMap<String, Vec<String>> =
            ["p1", "p2"].map(|p| (p.to_owned(), Vec::new())).into();
        let at = At::Token(Bytes::from_static(b"t"));
        let starts =
            |t: &ClusterToken, history: u64| resume_starts(t, "c1", all, &placed, |_| history);
        let s = starts(&token(all, &["p1", "p2"], at.clone()), 2).unwrap();
        assert_eq!(s.len(), 2);
        let refused = |t: ClusterToken, code: &str, says: &str| {
            let e = starts(&t, 2).unwrap_err();
            assert_eq!(e.code, code, "{}", e.message);
            assert!(e.message.contains(says), "{}", e.message);
        };
        refused(
            token(alice, &["p1", "p2"], at.clone()),
            "InvalidArgument",
            "another watch's",
        );
        refused(
            token(feed, &["p1", "p2"], at.clone()),
            "InvalidArgument",
            "another watch's",
        );
        refused(
            token(all, &["p1"], at.clone()),
            "InvalidArgument",
            "covers partitions",
        );
        refused(
            token(all, &["p1", "p2"], At::Now),
            "InvalidArgument",
            "a position for every",
        );
        let mut elsewhere = token(all, &["p1", "p2"], at.clone());
        elsewhere.cluster_id = "c2".into();
        refused(elsewhere, "InvalidArgument", "cluster \"c2\"'s");
        let e = starts(&token(all, &["p1", "p2"], at), 3).unwrap_err();
        assert_eq!(e.code, "FeedExpired", "{}", e.message);
    }
}
