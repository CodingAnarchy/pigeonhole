//! D331 PR 4b: a range move's catch-up stream, the source's side (`ForwardSubscribe` with
//! `raw`).
//!
//! The move's target reads the source's history of its range from the copy's first token,
//! and applies it as the source logged it ([`super::range_follow`]). The stream is the
//! target's alone: it opens its own feed of the move's table, never a hub's ring, and never
//! rejoins one.
//!
//! - **Who's served** (feed1248's G1, cli692's M3): only this node as the source's owner in
//!   its map, at the move's `from_epoch`, for a live range move (never D332's NODE) naming
//!   it as `from`, from COPYING through HANDED_OVER, and only for exactly the move's bounds
//!   (an absent bound is open, as in `RangeMove`). HANDED_OVER is included: the target is
//!   still catching up to `final_seqno` then, and a reconnect must find the stream.
//! - **What it sends:** each commit's changes inside the range as a `RawCommit` (kinds,
//!   timestamps and stored values as logged, feed-silent commits included, durable only); a
//!   commit with none in the range, a catalog change, or a range drop outside the range as
//!   `Progress`; a quiet table a `Progress` every `--feed-quiet` interval. A commit
//!   straddling a bound sends only its in-range part (cli692).
//! - **How it ends:** a range drop that removes any of the range ends it with an error, never
//!   `Progress` (feed1248's G2: the target would keep rows the source dropped). Once the
//!   move is handed over and the stream has passed its `final_seqno`, it ends with
//!   `MovedAway`, as a cluster component of a moved table does. A map that no longer gives
//!   the move, or the source, here ends it with `WrongPartition`.

use std::sync::Arc;
use std::time::Duration;

use prost::bytes::Bytes;

use super::Node;
use crate::serve::errors::ApiError;
use crate::serve::feed::{FeedEvents, Out, Sink};
use crate::serve::proto::{self, MovePhase, subscribe_request::Start as S};
use crate::serve::router::Cluster;

/// How often the stream looks at the map for its move's end when the feed is quiet.
const MAP_TICK: Duration = Duration::from_millis(200);

/// A range move's catch-up stream from partition `req.partition`, served here.
pub(crate) async fn serve(
    cluster: &Cluster,
    req: proto::ForwardSubscribeRequest,
) -> Result<FeedEvents, ApiError> {
    let (node, parts) = (&cluster.node, &cluster.parts);
    if !req.raw {
        return Err(ApiError::invalid(
            "a range bound is for a range move's catch-up stream only (raw)",
        ));
    }
    let r = req
        .request
        .as_ref()
        .ok_or_else(|| ApiError::invalid("a ForwardSubscribe carries no request"))?;
    let [table] = &r.tables[..] else {
        return Err(ApiError::invalid(
            "a range move's catch-up stream reads exactly its move's table",
        ));
    };
    let Some(S::Resume(token)) = &r.start else {
        return Err(ApiError::invalid(
            "a range move's catch-up stream starts at its copy's token",
        ));
    };
    if r.allow_partial
        || !r.prefix.is_empty()
        || r.visible
        || r.checkpoint_every != 0
        || !r.skip_through.is_empty()
        || req.skip_through != 0
    {
        return Err(ApiError::invalid(
            "a range move's catch-up stream takes no client options",
        ));
    }
    let m = live_move(node, &req, table)?;
    let (start, end) = super::map::move_range(&m).map_err(ApiError::internal)?;
    let Some(service) = parts.load().get(&req.partition).cloned() else {
        return Err(ApiError::partition_unavailable(format!(
            "the map gives this node partition {:?}, but it isn't open here",
            req.partition
        )));
    };
    let slot = service.stream_slot()?;
    let db = service.part()?.db.clone();
    let t = db.table(table)?.open()?;
    let mut feed = t
        .feed()
        .resume(&pigeonhole::FeedToken::from_bytes(token.to_vec()))
        .open()?;
    // Feed-silent commits too (the copy's own and other moves' are the range's history),
    // durable only: the target applies nothing the source could still lose.
    feed.include_silent();
    let limits = service.feed_limits();
    let (sink, events, end_tx) = Sink::new(&limits);
    let stream = Stream {
        node: Arc::clone(node),
        sources: Arc::clone(&cluster.range_copies),
        partition: req.partition,
        move_id: m.id.clone(),
        table: t,
        start,
        end,
        quiet: limits.quiet,
        sink,
    };
    tokio::spawn(async move {
        let outcome = stream.run(feed).await;
        drop(slot);
        if let Err(e) = outcome
            && !stream.sink.is_closed()
        {
            let _ = end_tx.send(e);
        }
    });
    Ok(events)
}

/// The live move the request names: G1's and M3's checks.
fn live_move(
    node: &Node,
    req: &proto::ForwardSubscribeRequest,
    table: &str,
) -> Result<proto::RangeMove, ApiError> {
    let map = node.get_map()?;
    let m = map
        .moves
        .iter()
        .find(|m| {
            !super::map::node_move(m)
                && m.from == req.partition
                && m.table == table
                && m.start == req.range_start
                && m.end == req.range_end
        })
        .ok_or_else(|| {
            ApiError::wrong_partition(
                map.map_version,
                format!(
                    "no range move of table {table:?} from partition {:?} with those bounds here",
                    req.partition
                ),
            )
        })?;
    if !(MovePhase::Copying..=MovePhase::HandedOver).contains(&super::map::phase(m)) {
        return Err(ApiError::failed_precondition(format!(
            "range move {:?} is at {:?}: it has no catch-up stream",
            m.id,
            super::map::phase(m)
        )));
    }
    source_here(node.id(), &map, m)?;
    Ok(m.clone())
}

/// M3: this node owns `m`'s source in `map`, at the epoch it had when the copy began
/// (`from_epoch`, 0 when the record predates it).
fn source_here(me: &str, map: &proto::ClusterMap, m: &proto::RangeMove) -> Result<(), ApiError> {
    let ok = map
        .partitions
        .iter()
        .any(|p| p.id == m.from && p.node == me && (m.from_epoch == 0 || p.epoch == m.from_epoch));
    if ok {
        Ok(())
    } else {
        Err(ApiError::wrong_partition(
            map.map_version,
            format!(
                "this node no longer serves range move {:?}'s source {:?} at its epoch",
                m.id, m.from
            ),
        ))
    }
}

/// One catch-up stream's state.
struct Stream {
    node: Arc<Node>,
    /// The copies served here: the stream keeps its move's hold from going idle.
    sources: Arc<super::range_copy::Sources>,
    partition: String,
    move_id: String,
    table: pigeonhole::Table,
    start: Vec<u8>,
    end: Option<Vec<u8>>,
    quiet: Duration,
    sink: Sink,
}

/// What the stream read next, owned (the feed lends its events).
enum Read {
    /// A commit's changes inside the range (none: the commit is only a position).
    Commit(Option<proto::RawCommit>),
    /// A catalog change, or a range drop outside the range: a position.
    Passed,
    /// A range drop that removes some of the range.
    Dropped(u64),
}

impl Stream {
    /// Sends the range's history until the move ends it, the target goes away, or it fails.
    async fn run(&self, mut feed: pigeonhole::Feed) -> Result<(), ApiError> {
        let mut sent = Bytes::new();
        let mut quiet = tokio::time::Instant::now() + self.quiet;
        let mut looked = tokio::time::Instant::now();
        loop {
            let read = tokio::select! {
                biased;
                () = self.sink.closed() => return Ok(()),
                r = tokio::time::timeout(MAP_TICK, next(&mut feed, self)) => r,
            };
            let token = Bytes::copy_from_slice(feed.token().as_bytes());
            let event = match read {
                Ok(read) => match read? {
                    Read::Commit(Some(c)) => Some(proto::feed_event::Event::RawCommit(c)),
                    Read::Commit(None) | Read::Passed => None,
                    Read::Dropped(seqno) => {
                        return Err(ApiError::failed_precondition(format!(
                            "a range drop at seqno {seqno} on partition {:?} removed rows of \
                             range move {:?}'s range: the move must be aborted",
                            self.partition, self.move_id
                        )));
                    }
                },
                // Quiet: the end may have come (the map's, not the feed's).
                Err(_) => None,
            };
            let now = tokio::time::Instant::now();
            if let Some(event) = event {
                self.send(token.clone(), event).await?;
                sent = token.clone();
                quiet = now + self.quiet;
            } else if now >= quiet && token != sent {
                self.send(
                    token.clone(),
                    proto::feed_event::Event::Progress(proto::Progress {}),
                )
                .await?;
                sent = token.clone();
                quiet = now + self.quiet;
            }
            // A map read copies it: at most once a tick.
            if looked.elapsed() < MAP_TICK {
                continue;
            }
            looked = tokio::time::Instant::now();
            self.sources.touch(&self.move_id);
            if self.ended(&token).await? {
                return Err(ApiError::moved_away(format!(
                    "range move {:?} was handed over, and its catch-up stream passed its last \
                     seqno",
                    self.move_id
                )));
            }
        }
    }

    /// Whether the move was handed over and the stream, at `token`, passed its `final_seqno`
    /// (its position then sent, so the target has it). `WrongPartition` when the map no
    /// longer gives the move, or its source, here.
    async fn ended(&self, token: &Bytes) -> Result<bool, ApiError> {
        let map = self.node.get_map()?;
        let Some(m) = map.moves.iter().find(|m| m.id == self.move_id) else {
            return Err(ApiError::wrong_partition(
                map.map_version,
                format!("the map no longer has range move {:?}", self.move_id),
            ));
        };
        source_here(self.node.id(), &map, m)?;
        let done = super::map::phase(m) >= MovePhase::HandedOver
            && m.final_seqno != 0
            && super::token::token_seqno(token).is_some_and(|s| s >= m.final_seqno);
        if done {
            self.send(
                token.clone(),
                proto::feed_event::Event::Progress(proto::Progress {}),
            )
            .await?;
        }
        Ok(done)
    }

    async fn send(&self, token: Bytes, event: proto::feed_event::Event) -> Result<(), ApiError> {
        self.sink
            .put(Out::event(proto::FeedEvent {
                token,
                event: Some(event),
                partition: String::new(),
                xid: Bytes::new(),
            }))
            .await
    }

    /// `row` is in the range.
    fn holds(&self, row: &[u8]) -> bool {
        row >= self.start.as_slice() && self.end.as_deref().is_none_or(|e| row < e)
    }

    /// `ev` as the stream sends it.
    fn read(&self, ev: &pigeonhole::FeedEvent) -> Read {
        match ev {
            pigeonhole::FeedEvent::Commit(c) => {
                let changes: Vec<proto::RawChange> = c
                    .changes()
                    .iter()
                    .filter(|ch| self.holds(ch.row()))
                    .map(|ch| proto::RawChange {
                        family: ch.family().to_owned(),
                        kind: super::range_copy::logged_kind(ch.kind()),
                        row: Bytes::copy_from_slice(ch.row()),
                        qualifier: Bytes::copy_from_slice(ch.qualifier()),
                        timestamp: ch.timestamp(),
                        stored: Bytes::copy_from_slice(ch.stored()),
                    })
                    .collect();
                Read::Commit((!changes.is_empty()).then(|| proto::RawCommit {
                    seqno: c.seqno(),
                    timestamp: c.timestamp(),
                    changes,
                }))
            }
            pigeonhole::FeedEvent::RangeDrop(d)
                if d.overlaps(&self.table, &self.start, self.end.as_deref()) =>
            {
                Read::Dropped(d.seqno())
            }
            _ => Read::Passed,
        }
    }
}

/// The feed's next event, as the stream reads it.
async fn next(feed: &mut pigeonhole::Feed, s: &Stream) -> Result<Read, ApiError> {
    std::future::poll_fn(|cx| match feed.poll_next_event(cx) {
        std::task::Poll::Ready(Ok(ev)) => std::task::Poll::Ready(Ok(s.read(ev))),
        std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(ApiError::from(e))),
        std::task::Poll::Pending => std::task::Poll::Pending,
    })
    .await
}
