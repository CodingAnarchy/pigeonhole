//! `Vectors.BulkLoad` into an index on several partitions (#1722 PR 3, ICR 0138).
//!
//! - **The entry node splits the client's stream by id.** It reads the client's messages
//!   as a single node does (each holding its share of the in-flight budget, each within
//!   `--stream-idle-ms`), and sends each message's vectors to the partition each id's range
//!   is in, as one message a partition, each with its piece of the message's budget. A
//!   partition's part opens with its first vector: a load of its own on this node, or
//!   `Cluster.ForwardBulkLoad` to the node serving it, which serves it as that partition's
//!   own `BulkLoad`, metered and gated as a client's.
//! - **`expected` is the client's on every partition** (the coordinator's call, E1): a
//!   partition's share of the load isn't known until it has been read, and each partition's
//!   load ends at about twice its `expected`, so only the whole load's bound can't fail one.
//!   Each partition's lists are sized for the whole load, and each reserves the whole load's
//!   worst case from its node's `--bulk-load-bytes`. So a node that can't hold that for
//!   every partition of the index it serves refuses the load at once ([`bulk::fits`]):
//!   this node before it reads anything, another node as its first part opens.
//! - **Backpressure:** a message's parts go out before the next message is read, so a slow
//!   partition slows the client, as a slow loader does on one node. A partition left waiting
//!   meanwhile gets an empty message every third of `--stream-idle-ms`, so its node's idle
//!   bound doesn't end its part while another partition is the slow one.
//! - **Strict:** a partition whose part fails (it can't be reached, its index isn't empty,
//!   a map change moved it, it ran past its `expected`) fails the load, naming it. The other
//!   parts are ended (a local load's input breaks off, a forwarded stream ends), each keeping
//!   the vectors it took, and the error comes once every part has answered, as one node's
//!   comes once its loader has stopped. If any part may have taken a vector, it says the
//!   index may be loaded in part (`retry_safe: false`).
//! - **A client that goes away** drops the call: every part's input ends as above, and the
//!   parts finish on their own, as one node's load does.
//! - **Before it reads anything,** every node serving a part of the index must run a build
//!   that serves `ForwardBulkLoad` ([`INDEX_BULK_LOAD_FEATURE`](super::INDEX_BULK_LOAD_FEATURE)):
//!   an older one is refused with `Unsupported`, naming it, rather than part way.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, mpsc};
use tokio::task::JoinHandle;

use super::forward;
use super::route::Routes;
use crate::serve::auth::Caller;
use crate::serve::bulk::{self, Batch, BulkLoadOp, Item, Msg};
use crate::serve::errors::ApiError;
use crate::serve::proto;
use crate::serve::router::{Cluster, Place, Router, Target};
use crate::serve::service::{ServiceOp, ServiceReply};

/// `e`, its message naming `partition`.
fn on(partition: &str, mut e: ApiError) -> ApiError {
    e.message = format!("partition {partition}: {}", e.message);
    e
}

/// Whether a part that answered `r` may have taken a vector: it loaded, or its failure says
/// it did or may have (`applied_partly`), or the forward's outcome is unknown. A part refused
/// before it took anything (its index isn't empty, its node can't hold it) took none.
fn took(r: &Result<(), ApiError>) -> bool {
    match r {
        Ok(()) => true,
        // `applied_partly`'s phrases and the loader's, not "loaded" alone, which "overloaded"
        // has (simp724's n1 on #2045).
        Err(e) => {
            e.message.contains("were loaded")
                || e.message.contains("may have been loaded")
                || e.code == "OutcomeUnknown"
        }
    }
}

/// How many of the partitions in `placed` the node serving `partition` serves.
fn siblings(routes: &Routes, placed: &[String], partition: &str) -> u64 {
    let Some(node) = routes.owner(partition).map(|o| o.node.as_str()) else {
        return 1;
    };
    placed
        .iter()
        .filter(|p| routes.owner(p).is_some_and(|o| o.node == node))
        .count() as u64
}

/// Where a partition's part of the load goes.
#[derive(Clone)]
enum Feed {
    /// A load on this node: the loader's own channel.
    Local(mpsc::Sender<Msg>),
    /// `ForwardBulkLoad` to the node serving it: the stream's next `load`.
    Remote(mpsc::Sender<proto::VectorBulkLoadRequest>),
}

impl Feed {
    /// Sends `items` (`bytes` of the message, holding `budget`) to the part; `false` when
    /// the part has already ended, so it took nothing more. A local part's loader holds the
    /// budget until it takes the last of them, as one node's does; a forwarded part's goes
    /// once the stream has them, since the serving node reserves them again as they arrive.
    async fn deliver(
        self,
        items: Vec<Item>,
        bytes: u64,
        budget: Option<OwnedSemaphorePermit>,
    ) -> bool {
        match self {
            Feed::Local(tx) => tx
                .send(Msg::Batch(Batch::of(items, bytes, budget)))
                .await
                .is_ok(),
            Feed::Remote(tx) => {
                let items = items
                    .iter()
                    .map(|(id, e)| proto::VectorItem {
                        id: id.clone(),
                        vector: Some(crate::serve::convert::vector_of(e)),
                    })
                    .collect();
                let sent = tx
                    .send(proto::VectorBulkLoadRequest {
                        items,
                        ..Default::default()
                    })
                    .await
                    .is_ok();
                drop(budget);
                sent
            }
        }
    }
}

/// One partition's part: its feed, and the task that ends with its answer.
struct Part {
    partition: String,
    feed: Option<Feed>,
    run: JoinHandle<Result<(), ApiError>>,
    /// The task's answer, once it has given it (a `JoinHandle` is polled to its end once).
    done: Option<Result<(), ApiError>>,
}

impl Part {
    /// The part's answer, waiting for it.
    async fn answer(&mut self) -> Result<(), ApiError> {
        if self.done.is_none() {
            self.done = Some(
                (&mut self.run)
                    .await
                    .unwrap_or_else(|e| Err(ApiError::internal(format!("a load's part: {e}")))),
            );
        }
        self.done.clone().unwrap_or(Ok(()))
    }
}

/// The first part (by index) whose task has ended, with its answer; pending while none has.
fn ended(parts: &mut [Part], cx: &mut Context<'_>) -> Poll<(usize, Result<(), ApiError>)> {
    for (i, p) in parts.iter_mut().enumerate() {
        if p.done.is_some() {
            continue;
        }
        if let Poll::Ready(r) = Pin::new(&mut p.run).poll(cx) {
            let r = r.unwrap_or_else(|e| Err(ApiError::internal(format!("a load's part: {e}"))));
            p.done = Some(r.clone());
            return Poll::Ready((i, r));
        }
    }
    Poll::Pending
}

/// A part that ended before the client's stream did: its failure, naming it.
fn early(parts: &[Part], i: usize, r: Result<(), ApiError>) -> ApiError {
    on(
        &parts[i].partition,
        r.err()
            .unwrap_or_else(|| ApiError::internal("its load ended before the client's stream")),
    )
}

/// A forwarded part's messages: the first (with the header), then each `load` the entry
/// node sends, and an empty one whenever it has sent nothing for `every`, so the serving
/// node's `--stream-idle-ms` doesn't end a part held up by another partition.
struct Outgoing {
    first: Option<proto::ForwardBulkLoadRequest>,
    rx: mpsc::Receiver<proto::VectorBulkLoadRequest>,
    keep: Pin<Box<tokio::time::Sleep>>,
    every: Duration,
}

impl Outgoing {
    fn rearm(&mut self) {
        self.keep
            .as_mut()
            .reset(tokio::time::Instant::now() + self.every);
    }
}

impl futures_core::Stream for Outgoing {
    type Item = proto::ForwardBulkLoadRequest;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if let Some(first) = this.first.take() {
            this.rearm();
            return Poll::Ready(Some(first));
        }
        let load = match this.rx.poll_recv(cx) {
            Poll::Ready(Some(load)) => load,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => match this.keep.as_mut().poll(cx) {
                Poll::Ready(()) => proto::VectorBulkLoadRequest::default(),
                Poll::Pending => return Poll::Pending,
            },
        };
        this.rearm();
        Poll::Ready(Some(proto::ForwardBulkLoadRequest {
            load: Some(load),
            ..Default::default()
        }))
    }
}

/// A forwarded part: `ForwardBulkLoad` to `to`, on a connection of its own, until the
/// serving node answers.
async fn remote(
    node: Arc<super::Node>,
    forwarder: Arc<forward::Forwarder>,
    to: Target,
    out: Outgoing,
    open_within: Duration,
) -> Result<(), ApiError> {
    let mut lease = forwarder.stream_channel(&to.address, open_within).await?;
    let request = node.request(out).map_err(ApiError::internal)?;
    match forward::peer_client(lease.channel())
        .forward_bulk_load(request)
        .await
    {
        Ok(_) => {
            lease.ended();
            Ok(())
        }
        // A build before #1722 PR 3, which the feature check missed (its answer was cached).
        Err(s) if s.code() == tonic::Code::Unimplemented => Err(ApiError::unsupported(format!(
            "node {} doesn't serve a BulkLoad across an index's partitions (an older build)",
            to.node
        ))),
        // A load is a write: past a failure, what it loaded is unknown.
        Err(s) => Err(forward::classify(&s, &to.address, true)),
    }
}

impl Router {
    /// `Vectors.BulkLoad` into an index on several partitions (#1722 PR 3): the client's
    /// stream split by id into a part per partition (the module's doc).
    pub(crate) async fn fan_bulk_load(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<OwnedSemaphorePermit>,
    ) -> Result<ServiceReply, ApiError> {
        let request = self.service.admit(&op.admit(), caller, prepaid).await?;
        drop(request);
        let ServiceOp::VectorBulkLoad(BulkLoadOp {
            index,
            expected,
            mut input,
            wait,
            ..
        }) = op
        else {
            return Err(ApiError::internal("fan_bulk_load takes a BulkLoad"));
        };
        let routes = cluster
            .node
            .routes()
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| ApiError::partition_unavailable("this node has no cluster map yet"))?;
        let placed: Vec<String> = routes
            .index_partitions(&index, b"")
            .ok_or_else(|| {
                ApiError::from_code(
                    pigeonhole::ErrorCode::TableNotFound,
                    &format!("index {index:?} isn't in the cluster map"),
                )
            })?
            .into_iter()
            .map(str::to_owned)
            .collect();
        // Every node with a part, before anything is read (the coordinator's call).
        let owners: Vec<(String, String)> = placed
            .iter()
            .filter_map(|p| routes.owner(p).map(|o| (o.node.clone(), o.address.clone())))
            .collect();
        let down = tokio::time::timeout(
            wait,
            cluster.node.owners_keep(
                owners.into_iter(),
                super::INDEX_BULK_LOAD_FEATURE,
                "a Vectors.BulkLoad across an index's partitions",
            ),
        )
        .await
        .map_err(|_| ApiError::deadline_exceeded())?
        .map_err(|e| match e.code.as_str() {
            "FailedPrecondition" => ApiError::unsupported(e.message),
            _ => e,
        })?;
        if let Some(node) = down.first() {
            let p = placed
                .iter()
                .find(|p| routes.owner(p).is_some_and(|o| o.node == *node))
                .map_or("", String::as_str);
            return Err(ApiError::partition_unavailable(format!(
                "partition {p} isn't reachable: its node {node} didn't answer"
            )));
        }
        // This node's own `--bulk-load-bytes`, for a part on each of its partitions of the
        // index, before anything is read (audit120's S1 and simp724's M1 on #2045). Another
        // node checks its own as its first part opens.
        if let Some(p) = placed
            .iter()
            .find(|p| routes.owner(p).is_some_and(|o| o.node == cluster.node.id()))
            && let Place::Here(service) =
                crate::serve::router::place(&cluster.node, &cluster.parts, &routes, p)
                    .map_err(|e| on(p, e))?
        {
            service
                .bulk_fits(&index, expected, siblings(&routes, &placed, p))
                .await
                .map_err(|e| on(p, e))?;
        }
        let mut parts: Vec<Part> = Vec::new();
        let read = async {
            loop {
                let msg = tokio::select! {
                    m = input.recv() => m,
                    (i, r) = std::future::poll_fn(|cx| ended(&mut parts, cx)) => {
                        return Err(early(&parts, i, r));
                    }
                };
                let batch = match msg {
                    Some(Msg::Batch(b)) => b,
                    Some(Msg::End) => return Ok(()),
                    Some(Msg::Failed(e)) => return Err(e),
                    None => return Err(ApiError::internal("the load's input ended early")),
                };
                let (items, bytes, mut budget) = batch.into_parts();
                let total = items.len().max(1) as u64;
                // The message's vectors by partition, in the order each first appears.
                let mut groups: Vec<(String, Vec<Item>)> = Vec::new();
                for item in items {
                    let p = routes.index_partition(&index, &item.0).ok_or_else(|| {
                        ApiError::internal(format!("index {index:?} places no partition for an id"))
                    })?;
                    match groups.iter_mut().find(|(g, _)| g == p) {
                        Some((_, v)) => v.push(item),
                        None => groups.push((p.to_owned(), vec![item])),
                    }
                }
                let mut left = total as usize;
                for (p, items) in groups {
                    let i = match parts.iter().position(|x| x.partition == p) {
                        Some(i) => i,
                        None => {
                            parts.push(self.open_part(
                                cluster, &routes, &placed, &p, &index, expected, wait, caller,
                            )?);
                            parts.len() - 1
                        }
                    };
                    // This group's piece of the message's budget, by its share of the vectors
                    // (audit120's S3, simp724's S1 on #2045): the last group takes the rest.
                    let piece = if items.len() >= left {
                        budget.take()
                    } else {
                        budget.as_mut().and_then(|b| {
                            let n = b.num_permits() * items.len() / left.max(1);
                            b.split(n)
                        })
                    };
                    left = left.saturating_sub(items.len());
                    let share = bytes * items.len() as u64 / total;
                    let Some(feed) = parts[i].feed.clone() else {
                        return Err(on(&p, ApiError::internal("its part has no input")));
                    };
                    // Another part's end is seen while this one waits for room (audit120's
                    // N2 on #2045).
                    let delivered = tokio::select! {
                        ok = feed.deliver(items, share, piece) => ok,
                        (j, r) = std::future::poll_fn(|cx| ended(&mut parts, cx)) => {
                            return Err(early(&parts, j, r));
                        }
                    };
                    if !delivered {
                        let r = parts[i].answer().await;
                        return Err(early(&parts, i, r));
                    }
                }
            }
        };
        let outcome: Result<(), ApiError> = read.await;
        // The client's end, or the load's failure, to every part: a local load is told the
        // end (or its input breaks off), a forwarded stream ends. Each keeps what it took.
        let clean = outcome.is_ok();
        for p in &mut parts {
            if let (true, Some(Feed::Local(tx))) = (clean, p.feed.take()) {
                let _ = tx.send(Msg::End).await;
            }
            p.feed = None;
        }
        // Then every part's answer, as one node answers once its loader has stopped
        // (audit120's S4 on #2045): a client told to load again finds the slots free.
        let mut first = outcome.err();
        let mut partly = false;
        for p in &mut parts {
            let r = p.answer().await;
            partly |= took(&r);
            if let Err(e) = r {
                first.get_or_insert_with(|| on(&p.partition, e));
            }
        }
        match first {
            None => Ok(ServiceReply::Empty(proto::Empty {})),
            Some(e) if partly => Err(e.applied_partly(false)),
            Some(e) => Err(e),
        }
    }

    /// Opens `partition`'s part of a load into `index`: a load of its own here, or a
    /// forwarded stream to the node serving it.
    #[allow(clippy::too_many_arguments)]
    fn open_part(
        &self,
        cluster: &Cluster,
        routes: &Routes,
        placed: &[String],
        partition: &str,
        index: &str,
        expected: u64,
        wait: Duration,
        caller: &Caller,
    ) -> Result<Part, ApiError> {
        let place = crate::serve::router::place(&cluster.node, &cluster.parts, routes, partition)
            .map_err(|e| on(partition, e))?;
        let (feed, run) = match place {
            Place::Here(service) => {
                let (tx, input) = mpsc::channel(1);
                let op = ServiceOp::VectorBulkLoad(BulkLoadOp {
                    index: index.to_owned(),
                    expected,
                    input,
                    wait,
                    siblings: siblings(routes, placed, partition),
                });
                let caller = caller.clone();
                let run =
                    tokio::spawn(async move { service.call(op, &caller, None).await.map(drop) });
                (Feed::Local(tx), run)
            }
            Place::There(to) => {
                let (tx, rx) = mpsc::channel(1);
                let every = (self.service.limits().stream_idle / 3).max(Duration::from_millis(1));
                let out = Outgoing {
                    first: Some(proto::ForwardBulkLoadRequest {
                        map_version: to.map_version,
                        caller: Some(forward::forwarded_caller(caller)?),
                        partition: partition.to_owned(),
                        load: Some(proto::VectorBulkLoadRequest {
                            index: index.to_owned(),
                            expected,
                            items: Vec::new(),
                        }),
                    }),
                    rx,
                    keep: Box::pin(tokio::time::sleep(every)),
                    every,
                };
                let run = tokio::spawn(remote(
                    Arc::clone(&cluster.node),
                    Arc::clone(&cluster.forwarder),
                    to,
                    out,
                    wait,
                ));
                (Feed::Remote(tx), run)
            }
        };
        Ok(Part {
            partition: partition.to_owned(),
            feed: Some(feed),
            run,
            done: None,
        })
    }

    /// `Cluster.ForwardBulkLoad` here (#1722 PR 3): one partition's part of a load another
    /// node splits, served as this partition's own `BulkLoad`, if this node's map places the
    /// index on it and gives it the partition (`WrongPartition` otherwise). Refused at once
    /// unless this node's `--bulk-load-bytes` holds a part on every partition of the index
    /// it serves ([`bulk::fits`]).
    pub(crate) async fn serve_forwarded_bulk_load(
        &self,
        partition: &str,
        load: proto::VectorBulkLoadRequest,
        budget: Option<OwnedSemaphorePermit>,
        source: impl bulk::Source,
        caller: &Caller,
        wait: Duration,
    ) -> Result<(), ApiError> {
        let Some(cluster) = &self.cluster else {
            return Err(ApiError::unsupported("this node isn't a cluster node"));
        };
        let (service, siblings) = {
            let routes = cluster.node.routes();
            let Some(routes) = routes.as_ref() else {
                return Err(ApiError::wrong_partition(
                    0,
                    "this node has no cluster map yet",
                ));
            };
            let placed: Vec<String> = routes
                .index_partitions(&load.index, b"")
                .unwrap_or_default()
                .into_iter()
                .map(str::to_owned)
                .collect();
            if !placed.iter().any(|p| p == partition) {
                return Err(ApiError::wrong_partition(
                    routes.map_version(),
                    format!(
                        "this node's map doesn't place index {:?} on partition {partition:?}",
                        load.index
                    ),
                ));
            }
            (
                cluster.owned_here(routes, partition)?,
                siblings(routes, &placed, partition),
            )
        };
        let (tx, input) = mpsc::channel(1);
        let op = ServiceOp::VectorBulkLoad(BulkLoadOp {
            index: load.index.clone(),
            expected: load.expected,
            input,
            wait,
            siblings,
        });
        let pump = bulk::pump(tx, (load, budget), source);
        let call = service.call(op, caller, None);
        tokio::pin!(pump, call);
        // The loader ends the call; a pump that ends first (the stream did) leaves it to
        // finish.
        let reply = tokio::select! {
            r = &mut call => r,
            () = &mut pump => call.await,
        };
        reply.map(drop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn next(out: &mut Outgoing) -> Option<proto::ForwardBulkLoadRequest> {
        std::future::poll_fn(|cx| futures_core::Stream::poll_next(Pin::new(&mut *out), cx)).await
    }

    /// #1722 PR 3: a forwarded part sends its header first, each load as it comes, and an
    /// empty load whenever nothing has come for `every`, so its node's `--stream-idle-ms`
    /// doesn't end it while another partition holds the load up; and ends when the entry
    /// node drops its sender.
    #[tokio::test(start_paused = true)]
    async fn a_forwarded_part_keeps_its_stream_alive_while_it_waits() {
        let every = Duration::from_secs(10);
        let (tx, rx) = mpsc::channel(1);
        let mut out = Outgoing {
            first: Some(proto::ForwardBulkLoadRequest {
                partition: "p2".into(),
                ..Default::default()
            }),
            rx,
            keep: Box::pin(tokio::time::sleep(every)),
            every,
        };
        assert_eq!(next(&mut out).await.unwrap().partition, "p2");
        // Nothing for `every`: an empty load, no sooner.
        let start = tokio::time::Instant::now();
        let kept = next(&mut out).await.unwrap();
        assert_eq!(start.elapsed(), every);
        assert!(
            kept.load
                .is_some_and(|l| l.items.is_empty() && l.index.is_empty())
        );
        // A load as it comes, which restarts the wait.
        tokio::time::advance(every / 2).await;
        tx.send(proto::VectorBulkLoadRequest {
            items: vec![proto::VectorItem::default()],
            ..Default::default()
        })
        .await
        .unwrap();
        assert_eq!(next(&mut out).await.unwrap().load.unwrap().items.len(), 1);
        let start = tokio::time::Instant::now();
        assert!(next(&mut out).await.unwrap().load.unwrap().items.is_empty());
        assert_eq!(start.elapsed(), every);
        drop(tx);
        assert!(next(&mut out).await.is_none());
    }
}
