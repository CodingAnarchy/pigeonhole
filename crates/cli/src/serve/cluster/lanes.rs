//! A node's limits on the forwarded calls it serves, by who sent them (#2258).
//!
//! `Forward` and `ForwardScan` are routed apart from the `Cluster` control calls
//! ([`routes`](super::routes)); here each forwarded call takes a place in one of three lanes
//! before it's served:
//!
//! - **Clients:** a call another node forwards for its client. As many at once as
//!   `--max-concurrent`, and one past that is refused at once (`Overloaded`, which the entry
//!   node passes back), as a client's own call is: never queued into its deadline.
//! - **Nodes:** a node's own arm, which no client waits on directly: a spanning
//!   transaction's prepare and decide and the resolver's and the sweep's asks (D329), a range
//!   move's copy chunk (D331), the balancer's tablets (D332). [`NODE_CALLS`] at once, and
//!   one past that waits for a place, up to its deadline, since a refusal would only be
//!   retried later (and an in-doubt share holds its rows meanwhile).
//! - **Parts:** one partition's part of a read another node fans out (#1715), served with no
//!   budget of its own. [`READ_PARTS`] at once, and one past that is refused at once: this
//!   bound is what keeps a node's memory for other nodes' parts at [`READ_PARTS`] pages.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::serve::errors::ApiError;
use crate::serve::proto;

/// A node's own forwarded arms served at once, in a queue of their own.
pub(crate) const NODE_CALLS: usize = 16;

/// Parts of other nodes' fanned-out reads served at once: their pages are this node's
/// memory with no budget of their own (`serve.md`, "Reads across partitions").
pub(crate) const READ_PARTS: usize = 16;

/// Which lane a forwarded call takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lane {
    Clients,
    Nodes,
    Parts,
}

impl Lane {
    /// The lane of forwarded call `msg`.
    pub(crate) fn of(msg: &proto::ForwardRequest) -> Self {
        use proto::forward_request::Op;
        if super::spanning::is_node_arm(msg)
            || matches!(msg.op, Some(Op::RangeCopy(_) | Op::Tablets(_)))
        {
            Self::Nodes
        } else if msg.fanout_part {
            Self::Parts
        } else {
            Self::Clients
        }
    }
}

/// The three lanes' places. The clients' lane is sized on first use, from the server's
/// `--max-concurrent`.
#[derive(Debug)]
pub(crate) struct Lanes {
    clients: OnceLock<Arc<Semaphore>>,
    nodes: Arc<Semaphore>,
    parts: Arc<Semaphore>,
}

impl Default for Lanes {
    fn default() -> Self {
        Self {
            clients: OnceLock::new(),
            nodes: Arc::new(Semaphore::new(NODE_CALLS)),
            parts: Arc::new(Semaphore::new(READ_PARTS)),
        }
    }
}

impl Lanes {
    /// A place in `lane`, held until the call is served. `max_concurrent`: the clients' lane's
    /// size. `wait`: how long a node's arm waits for a place.
    pub(crate) async fn enter(
        &self,
        lane: Lane,
        max_concurrent: usize,
        wait: Duration,
    ) -> Result<OwnedSemaphorePermit, ApiError> {
        let refused = |what: &str, n: usize| {
            ApiError::overloaded(format!(
                "this node is serving {n} forwarded {what} at once, its limit"
            ))
        };
        match lane {
            Lane::Clients => {
                let clients = self
                    .clients
                    .get_or_init(|| Arc::new(Semaphore::new(max_concurrent.max(1))));
                Arc::clone(clients)
                    .try_acquire_owned()
                    .map_err(|_| refused("calls", max_concurrent.max(1)))
            }
            Lane::Parts => Arc::clone(&self.parts)
                .try_acquire_owned()
                .map_err(|_| refused("read parts", READ_PARTS)),
            Lane::Nodes => {
                match tokio::time::timeout(wait, Arc::clone(&self.nodes).acquire_owned()).await {
                    Ok(Ok(p)) => Ok(p),
                    Ok(Err(_)) => Err(ApiError::internal("the nodes' lane is closed")),
                    Err(_) => Err(ApiError::overloaded(format!(
                        "this node's {NODE_CALLS} places for other nodes' own calls stayed \
                         taken for {wait:?}"
                    ))),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::forward_request::Op;

    fn msg(op: Option<Op>, fanout_part: bool) -> proto::ForwardRequest {
        proto::ForwardRequest {
            op,
            fanout_part,
            ..Default::default()
        }
    }

    #[test]
    fn a_forwarded_call_takes_its_senders_lane() {
        let decide = Some(Op::BranchDecide(proto::BranchDecide::default()));
        let resolve = Some(Op::ResolveTxn(proto::ResolveTxn::default()));
        let copy = Some(Op::RangeCopy(proto::RangeCopyRequest::default()));
        let tablets = Some(Op::Tablets(proto::TabletsRequest::default()));
        for op in [decide, resolve, copy, tablets] {
            assert_eq!(Lane::of(&msg(op.clone(), false)), Lane::Nodes, "{op:?}");
        }
        // A client's own arms of a spanning transaction are the client's.
        let write = Some(Op::TxnWrite(proto::TxnWriteRequest::default()));
        let mut w = msg(write, false);
        w.partition = "p2".into();
        assert_eq!(Lane::of(&w), Lane::Clients);
        assert_eq!(Lane::of(&msg(None, false)), Lane::Clients);
        assert_eq!(Lane::of(&msg(None, true)), Lane::Parts);
    }

    #[test]
    fn a_clients_call_or_a_read_part_past_its_lane_is_refused_at_once_and_a_nodes_arm_waits() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let lanes = Lanes::default();
            let long = Duration::from_secs(30);
            let mut held = Vec::new();
            for _ in 0..3 {
                held.push(lanes.enter(Lane::Clients, 3, long).await.unwrap());
            }
            let t = std::time::Instant::now();
            let e = lanes.enter(Lane::Clients, 3, long).await.unwrap_err();
            assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
            assert_eq!(e.to_status().code(), tonic::Code::Unavailable, "{e:?}");
            for _ in 0..READ_PARTS {
                held.push(lanes.enter(Lane::Parts, 3, long).await.unwrap());
            }
            assert!(lanes.enter(Lane::Parts, 3, long).await.is_err());
            // Neither lane takes the others' places.
            for _ in 0..NODE_CALLS {
                held.push(lanes.enter(Lane::Nodes, 3, long).await.unwrap());
            }
            // A node's arm past its lane waits: refused only once its wait runs out...
            let t = std::time::Instant::now();
            let e = lanes
                .enter(Lane::Nodes, 3, Duration::from_millis(200))
                .await
                .unwrap_err();
            assert!(
                t.elapsed() >= Duration::from_millis(200),
                "{:?}",
                t.elapsed()
            );
            assert_eq!(e.to_status().code(), tonic::Code::Unavailable, "{e:?}");
            // ... and takes a place given back meanwhile.
            let waiting = lanes.enter(Lane::Nodes, 3, long);
            let give_back = async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                held.pop();
            };
            let (place, ()) = tokio::join!(waiting, give_back);
            drop(place.unwrap());
        });
    }
}
