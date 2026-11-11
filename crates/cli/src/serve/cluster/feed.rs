//! Cluster feeds (D317 ticket 7, §8, ICR 0117): a `Feed.Subscribe` on a cluster node follows
//! every partition its tables are placed on, and merges them into one stream.
//!
//! - **A component per partition.** Each partition the subscription covers (its tables'
//!   placements, narrowed by its prefix) has its own single-node subscription on the node
//!   serving it: a partition served here through the local service, which shares its
//!   hubs and rings with every other subscriber; another node's through
//!   `Cluster.ForwardSubscribe`. A component announces its start position at once, so it
//!   knows where it is before any commit.
//! - **Merged as they arrive.** A partition's commits keep its order, so each row's do; there
//!   is no order across partitions. Each event names its partition.
//! - **Checkpoint tokens** (option (C), confirmed by the coordinator): the cluster token
//!   ([`super::token`]) holds every component's position, so it rides only on every Nth
//!   event, `N = ceil(len / 150 B)` of the last checkpoint, at most [`MAX_EVERY`], and 1 for
//!   one partition; also on a progress event once a quiet interval passes with the position
//!   moved, and before the stream ends with an error. Events between checkpoints carry no
//!   token: a resume from the last one may deliver them again, and the client recognises
//!   them by `(partition, seqno)`, or names them in `skip_through` so the server passes over
//!   them.
//! - **Connections of their own** (simp724's S1): a subscription's components on a peer
//!   share one HTTP/2 connection, opened for the subscription and never used by forwarded
//!   calls, so a subscriber that stops reading holds no other caller's flow-control window.
//! - **A partition that goes away** ends the stream with `PartitionUnavailable`, unless the
//!   request has `allow_partial`. Then it's reported in a `PartitionState` event and opened
//!   again from its own position, so nothing is skipped. A `WrongPartition` answer (the map
//!   moved it) makes the node pull the newer map and open it where the map now puts it.

use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use prost::bytes::{Buf, BufMut, Bytes};
use tonic::transport::Channel;

use super::Node;
use super::token::{At, ClusterToken, Component, subscription_hash};
use crate::serve::auth::Caller;
use crate::serve::errors::ApiError;
use crate::serve::feed::{FeedEvents, Out, SKIP_LIMIT, Sink};
use crate::serve::proto::{self, subscribe_request::Start as S};
use crate::serve::service::Service;

/// Checkpoints are at most this many events apart: a resume repeats at most this many, and
/// a `skip_through` passes over at most this many in a partition ([`SKIP_LIMIT`]).
pub(crate) const MAX_EVERY: u64 = SKIP_LIMIT;

/// Token bytes per event the checkpoint cadence aims for, on average.
const TOKEN_BYTES_PER_EVENT: usize = 150;

/// How long an unavailable partition waits between attempts to open it again.
const RETRY: Duration = Duration::from_secs(1);

/// Remote components opened at once, per subscription (D317 §7's `--fanout-forwards`).
const OPENS: usize = 8;

/// `FeedEvent`'s fields, as the merge reads and writes them.
const TOKEN_FIELD: u32 = 1;
const COMMIT_FIELD: u32 = 2;
const PROGRESS_FIELD: u32 = 4;
const PARTITION_FIELD: u32 = 5;

/// What a subscription needs to open (and open again) its components: shared by its task.
struct Ctx {
    node: Arc<Node>,
    /// The partition served here and its service, if any.
    local: Option<String>,
    service: Arc<Service>,
    caller: Caller,
    prefix: Bytes,
    visible: bool,
    /// Opens the node-to-node streams' connections, apart from forwarded unary calls'.
    forwarder: Arc<super::forward::Forwarder>,
    /// The subscription's connection to each peer (S1): a stream lease of its own, taken on
    /// first use and held until the subscription ends, never given back (`ended`).
    channels: Mutex<HashMap<String, Arc<tokio::sync::OnceCell<super::forward::StreamLease>>>>,
}

/// A component's stream of encoded events.
type Events = Pin<Box<dyn Stream<Item = Result<Bytes, ApiError>> + Send>>;

/// One partition's part of the subscription.
struct Part {
    partition: String,
    /// The subscription's tables placed on it, sorted.
    tables: Vec<String>,
    epoch: u64,
    at: At,
    /// Commits through this seqno after `at` are passed over (`skip_through`); 0: none.
    skip: u64,
    events: Option<Events>,
    /// While unavailable (only with `allow_partial`): when to try again.
    retry: Option<tokio::time::Instant>,
    /// An open of it is running in the background.
    opening: bool,
    /// Reported unavailable, and not yet available again.
    unavailable: bool,
}

/// `Feed.Subscribe` on a cluster node: opens every component, then merges them on a task of
/// its own. A component that can't be opened fails the call, unless `allow_partial`.
pub(crate) async fn subscribe(
    node: Arc<Node>,
    forwarder: Arc<super::forward::Forwarder>,
    local: Option<String>,
    service: Arc<Service>,
    r: proto::SubscribeRequest,
    caller: &Caller,
) -> Result<FeedEvents, ApiError> {
    if r.tables.is_empty() {
        return Err(ApiError::invalid("a subscription names at least one table"));
    }
    if r.checkpoint_every > MAX_EVERY as u32 {
        return Err(ApiError::invalid(format!(
            "checkpoint_every is at most {MAX_EVERY}"
        )));
    }
    let mut tables = r.tables.clone();
    tables.sort();
    tables.dedup();
    let guard = node.routes();
    let Some(routes) = guard.as_ref() else {
        return Err(ApiError::partition_unavailable(
            "this cluster node has no map yet",
        ));
    };
    // Each partition the subscription covers, with its tables on it.
    let mut placed: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for t in &tables {
        let parts = routes
            .partitions_of_table(t, &r.prefix)
            .ok_or_else(|| ApiError::not_found(format!("table {t:?} isn't in the cluster map")))?;
        for p in parts {
            placed.entry(p.to_owned()).or_default().push(t.clone());
        }
    }
    let epoch = |p: &str| routes.owner(p).map_or(0, |o| o.epoch);
    let subscription = subscription_hash(&tables, &r.prefix, r.visible);
    let mut skips: HashMap<String, u64> = HashMap::new();
    for s in &r.skip_through {
        if !placed.contains_key(&s.partition) {
            return Err(ApiError::invalid(format!(
                "skip_through names partition {:?}, which this subscription doesn't cover",
                s.partition
            )));
        }
        skips.insert(s.partition.clone(), s.seqno);
    }
    let resumed = matches!(r.start, Some(S::Resume(_)));
    let starts: BTreeMap<String, At> = match r.start {
        None | Some(S::FromOldest(false)) => placed.keys().map(|p| (p.clone(), At::Now)).collect(),
        Some(S::FromOldest(true)) => placed.keys().map(|p| (p.clone(), At::Oldest)).collect(),
        Some(S::FromSnapshot(_)) => {
            return Err(ApiError::unsupported(
                "a snapshot start isn't supported on a cluster subscription",
            ));
        }
        Some(S::Resume(bytes)) => {
            let t = ClusterToken::decode(&bytes).map_err(|why| {
                ApiError::invalid(format!(
                    "the resume token isn't a cluster feed token ({why})"
                ))
            })?;
            resume_starts(&t, routes.cluster_id(), subscription, &placed, epoch)?
        }
    };
    if !skips.is_empty() && !resumed {
        return Err(ApiError::invalid("skip_through needs a resume token"));
    }
    let map_version = routes.map_version();
    let parts: Vec<Part> = placed
        .into_iter()
        .map(|(partition, tables)| Part {
            epoch: epoch(&partition),
            at: starts[&partition].clone(),
            skip: skips.get(&partition).copied().unwrap_or(0),
            partition,
            tables,
            events: None,
            retry: None,
            opening: false,
            unavailable: false,
        })
        .collect();
    let cluster_id = routes.cluster_id().to_owned();
    drop(guard);
    // One `--max-streams` place here for the whole subscription (simp724's C); each
    // component holds one on its partition's node, as any client there does.
    let slot = service.stream_slot()?;
    let ctx = Arc::new(Ctx {
        node,
        forwarder,
        local,
        service: Arc::clone(&service),
        caller: caller.clone(),
        prefix: r.prefix.clone(),
        visible: r.visible,
        channels: Mutex::new(HashMap::new()),
    });
    let mut merge = Merge {
        ctx,
        parts,
        allow_partial: r.allow_partial,
        every: u64::from(r.checkpoint_every),
        map_version,
        subscription,
        cluster_id,
        since_checkpoint: 0,
        next_checkpoint: 1,
        moved: false,
        states: Vec::new(),
        opens: tokio::task::JoinSet::new(),
    };
    merge.open_all().await?;
    let limits = service.feed_limits();
    let (sink, events, end) = Sink::new(&limits);
    tokio::spawn(async move {
        let outcome = merge.run(&sink, limits.quiet).await;
        drop(slot);
        if let Err(e) = outcome
            && !sink.is_closed()
        {
            // The connection may have gone meanwhile: nobody to tell.
            let _ = end.send(e);
        }
    });
    Ok(events)
}

/// Each component's start from a resume token, after checking that it's this cluster's,
/// this subscription's, and that the map hasn't moved anything under it.
fn resume_starts(
    t: &ClusterToken,
    cluster_id: &str,
    subscription: u64,
    placed: &BTreeMap<String, Vec<String>>,
    epoch: impl Fn(&str) -> u64,
) -> Result<BTreeMap<String, At>, ApiError> {
    if t.cluster_id != cluster_id {
        return Err(ApiError::invalid(format!(
            "the token is cluster {:?}'s, not this one's",
            t.cluster_id
        )));
    }
    if t.subscription != subscription {
        return Err(ApiError::invalid(
            "the token is another subscription's: its tables, prefix or visibility differ",
        ));
    }
    let theirs: Vec<&str> = t.components.iter().map(|c| c.partition.as_str()).collect();
    let ours: Vec<&str> = placed.keys().map(String::as_str).collect();
    if theirs != ours {
        // Splits are fixed and placements keep their partitions, so only a table that's new
        // to the subscription can do this (simp724's N1).
        return Err(expired(format!(
            "the cluster map changed this subscription's partitions since the token \
             ({theirs:?} then, {ours:?} now): subscribe again"
        )));
    }
    let mut starts = BTreeMap::new();
    for c in &t.components {
        let now = epoch(&c.partition);
        if c.epoch != now {
            // A relocation by promotion gives the partition another history, so its token
            // can't resume there. D317's ticket 6 records which relocations copied the file
            // and kept it; until then no epoch moves.
            return Err(expired(format!(
                "partition {:?} was relocated since the token (epoch {} then, {now} now): \
                 subscribe again",
                c.partition, c.epoch
            )));
        }
        starts.insert(c.partition.clone(), c.at.clone());
    }
    Ok(starts)
}

fn expired(message: String) -> ApiError {
    ApiError::from_code(pigeonhole::ErrorCode::FeedExpired, &message)
}

/// The subscription's task.
struct Merge {
    ctx: Arc<Ctx>,
    parts: Vec<Part>,
    allow_partial: bool,
    /// `checkpoint_every`; 0: from the token's size.
    every: u64,
    map_version: u64,
    subscription: u64,
    cluster_id: String,
    /// Commits sent since the last checkpoint.
    since_checkpoint: u64,
    next_checkpoint: u64,
    /// A position moved since the last checkpoint.
    moved: bool,
    /// `PartitionState` events to send.
    states: Vec<proto::FeedEvent>,
    /// Parts opening again in the background, each bounded by the request timeout, so
    /// unreachable partitions never stop the merge from reading the others (simp724's S1 on
    /// #1700).
    opens: tokio::task::JoinSet<(usize, Result<Events, ApiError>)>,
}

/// What a component's event was.
enum Kind {
    Commit,
    Progress,
    Other,
}

impl Merge {
    /// Opens every component, at most [`OPENS`] at once. A failure fails the call, unless
    /// `allow_partial`: then the part starts unavailable, reported in a first event.
    async fn open_all(&mut self) -> Result<(), ApiError> {
        let mut set = tokio::task::JoinSet::new();
        let mut opened: Vec<Option<Result<Events, ApiError>>> =
            (0..self.parts.len()).map(|_| None).collect();
        let mut next = 0;
        loop {
            while set.len() < OPENS && next < self.parts.len() {
                let (ctx, request) = (Arc::clone(&self.ctx), self.request(next));
                set.spawn(async move { (next, ctx.open(request).await) });
                next += 1;
            }
            let Some(done) = set.join_next().await else {
                break;
            };
            let (i, result) = done.map_err(|e| ApiError::internal(e.to_string()))?;
            opened[i] = Some(result);
        }
        for (i, result) in opened.into_iter().enumerate() {
            self.opened(i, result.expect("every part was opened"))?;
        }
        Ok(())
    }

    /// What opening part `i` asks for, from where it is.
    fn request(&self, i: usize) -> Open {
        let p = &self.parts[i];
        Open {
            partition: p.partition.clone(),
            request: proto::SubscribeRequest {
                tables: p.tables.clone(),
                prefix: self.ctx.prefix.clone(),
                visible: self.ctx.visible,
                start: match &p.at {
                    At::Now => None,
                    At::Oldest => Some(S::FromOldest(true)),
                    At::Token(t) => Some(S::Resume(t.clone())),
                },
                ..Default::default()
            },
            skip: p.skip,
            map_version: self.map_version,
        }
    }

    /// Part `i` is unavailable: reported once, and tried again after [`RETRY`].
    fn unavailable(&mut self, i: usize, e: &ApiError) {
        let p = &mut self.parts[i];
        p.events = None;
        p.retry = Some(tokio::time::Instant::now() + RETRY);
        if !p.unavailable {
            p.unavailable = true;
            self.states.push(state_event(&p.partition, Some(e)));
        }
    }

    /// An open of part `i` finished: its stream from now on, and reported available again
    /// if it wasn't; or unavailable until the next try (`allow_partial`), or the stream's
    /// end.
    fn opened(&mut self, i: usize, result: Result<Events, ApiError>) -> Result<(), ApiError> {
        self.parts[i].opening = false;
        match result {
            Ok(events) => {
                // A reopen may have taken a newer map: later checkpoints carry it (simp724's
                // nit on round 2).
                if let Some(r) = self.ctx.node.routes().as_ref() {
                    self.map_version = self.map_version.max(r.map_version());
                }
                let p = &mut self.parts[i];
                p.events = Some(events);
                p.retry = None;
                if std::mem::take(&mut p.unavailable) {
                    self.states.push(state_event(&p.partition, None));
                }
                Ok(())
            }
            Err(e) if self.allow_partial && retryable(&e) => {
                self.unavailable(i, &e);
                Ok(())
            }
            Err(e) => Err(in_partition(&self.parts[i].partition, e)),
        }
    }

    /// Starts opening part `i` again in the background, from its position, where the map
    /// puts it; after pulling the map from its current owner first (`pull`: that owner
    /// answered `WrongPartition`, so its map is newer).
    fn reopen(&mut self, i: usize, pull: bool) {
        let owner = self
            .ctx
            .node
            .routes()
            .as_ref()
            .and_then(|r| r.owner(&self.parts[i].partition).cloned());
        let p = &mut self.parts[i];
        p.retry = None;
        p.opening = true;
        let (ctx, open, epoch) = (Arc::clone(&self.ctx), self.request(i), self.parts[i].epoch);
        let pull = pull.then_some(owner).flatten();
        self.opens
            .spawn(async move { (i, ctx.reopen(open, epoch, pull).await) });
    }

    async fn run(&mut self, sink: &Sink, quiet: Duration) -> Result<(), ApiError> {
        let mut last_sent = tokio::time::Instant::now();
        let mut turn = 0;
        loop {
            for event in std::mem::take(&mut self.states) {
                sink.put(Out::Event(event)).await?;
                last_sent = tokio::time::Instant::now();
            }
            let retry_at = self.parts.iter().filter_map(|p| p.retry).min();
            let quiet_at = last_sent + quiet;
            let next = std::future::poll_fn(|cx| poll_parts(&mut self.parts, &mut turn, cx));
            tokio::select! {
                () = sink.closed() => return Ok(()),
                (i, item) = next => {
                    let ended = match item {
                        Some(Ok(bytes)) => match self.cover(i, bytes) {
                            Ok(Some(out)) => {
                                sink.put(out).await?;
                                last_sent = tokio::time::Instant::now();
                                None
                            }
                            Ok(None) => None,
                            Err(e) => Some(e),
                        },
                        Some(Err(e)) => Some(e),
                        None => Some(ApiError::partition_unavailable("its stream ended")),
                    };
                    if let Some(e) = ended
                        && let Err(e) = self.failed(i, e)
                    {
                        self.last_checkpoint(sink).await;
                        return Err(e);
                    }
                }
                Some(done) = self.opens.join_next(), if !self.opens.is_empty() => {
                    let (i, result) = done.map_err(|e| ApiError::internal(e.to_string()))?;
                    if let Err(e) = self.opened(i, result) {
                        self.last_checkpoint(sink).await;
                        return Err(e);
                    }
                }
                () = sleep_until(retry_at) => {
                    let now = tokio::time::Instant::now();
                    for i in 0..self.parts.len() {
                        if self.parts[i].retry.is_some_and(|at| at <= now) {
                            self.reopen(i, false);
                        }
                    }
                }
                () = tokio::time::sleep_until(quiet_at) => {
                    if self.moved {
                        sink.put(Out::Event(self.progress())).await?;
                    }
                    last_sent = tokio::time::Instant::now();
                }
            }
        }
    }

    /// Takes part `i`'s event: its position, and what to send for it, if anything.
    fn cover(&mut self, i: usize, bytes: Bytes) -> Result<Option<Out>, ApiError> {
        let (token, body, kind) = split(bytes).map_err(|why| {
            ApiError::internal(format!(
                "partition {:?}'s feed event can't be read: {why}",
                self.parts[i].partition
            ))
        })?;
        if !token.is_empty() {
            self.parts[i].at = At::Token(token);
            self.moved = true;
        }
        match kind {
            // The position is all a progress event says, and the merged stream says it at
            // its own checkpoints.
            Kind::Progress | Kind::Other => Ok(None),
            Kind::Commit => {
                // A part's commits arrive in seqno order and those through `skip` are passed
                // over by its node, so any commit is past it (simp724's N2).
                self.parts[i].skip = 0;
                self.since_checkpoint += 1;
                let mut tail = Vec::with_capacity(16 + self.parts[i].partition.len());
                put_field(
                    &mut tail,
                    PARTITION_FIELD,
                    self.parts[i].partition.as_bytes(),
                );
                if self.since_checkpoint >= self.next_checkpoint {
                    let token = self.checkpoint();
                    put_field(&mut tail, TOKEN_FIELD, &token);
                }
                Ok(Some(Out::Spliced {
                    body,
                    tail: Bytes::from(tail),
                }))
            }
        }
    }

    /// The cluster token as of now, and the next checkpoint's distance from it.
    fn checkpoint(&mut self) -> Vec<u8> {
        let token = self.token().encode();
        self.since_checkpoint = 0;
        self.moved = false;
        self.next_checkpoint = if self.parts.len() == 1 {
            1
        } else if self.every > 0 {
            self.every
        } else {
            (token.len().div_ceil(TOKEN_BYTES_PER_EVENT) as u64).clamp(1, MAX_EVERY)
        };
        token
    }

    fn token(&self) -> ClusterToken {
        ClusterToken {
            cluster_id: self.cluster_id.clone(),
            map_version: self.map_version,
            subscription: self.subscription,
            components: self
                .parts
                .iter()
                .map(|p| Component {
                    partition: p.partition.clone(),
                    epoch: p.epoch,
                    at: p.at.clone(),
                })
                .collect(),
        }
    }

    /// A progress event with a checkpoint.
    fn progress(&mut self) -> proto::FeedEvent {
        proto::FeedEvent {
            token: Bytes::from(self.checkpoint()),
            event: Some(proto::feed_event::Event::Progress(proto::Progress {})),
            partition: String::new(),
        }
    }

    /// Before the stream ends with an error: a checkpoint, if the position moved since the
    /// last, so the client resumes from as late as it can. Best effort.
    async fn last_checkpoint(&mut self, sink: &Sink) {
        if self.moved {
            let _ = sink.put(Out::Event(self.progress())).await;
        }
    }

    /// Part `i`'s stream failed with `e`: opened again where the map now puts it after a
    /// `WrongPartition`; otherwise unavailable (`allow_partial`), or the stream's end.
    fn failed(&mut self, i: usize, e: ApiError) -> Result<(), ApiError> {
        self.parts[i].events = None;
        if e.code == "WrongPartition" {
            // The owner's map is newer: take it, then open the part where it says.
            self.reopen(i, true);
            return Ok(());
        }
        if self.allow_partial && retryable(&e) {
            self.unavailable(i, &e);
            return Ok(());
        }
        Err(in_partition(&self.parts[i].partition, e))
    }
}

/// Polls every available part, from `turn` on so none starves, for its next event.
fn poll_parts(
    parts: &mut [Part],
    turn: &mut usize,
    cx: &mut Context<'_>,
) -> Poll<(usize, Option<Result<Bytes, ApiError>>)> {
    let n = parts.len();
    for k in 0..n {
        let i = (*turn + k) % n;
        if let Some(events) = parts[i].events.as_mut()
            && let Poll::Ready(item) = events.as_mut().poll_next(cx)
        {
            *turn = (i + 1) % n;
            return Poll::Ready((i, item));
        }
    }
    Poll::Pending
}

async fn sleep_until(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Whether a part that failed with `e` may come back: its node was unreachable, overloaded
/// or not serving it yet. Anything else (an expired or bad position, a refusal) ends the
/// stream even with `allow_partial`.
fn retryable(e: &ApiError) -> bool {
    matches!(
        e.code.as_str(),
        "PartitionUnavailable" | "WrongPartition" | "Unavailable" | "Overloaded" | "Busy"
    )
}

/// `e`, naming the partition it came from.
fn in_partition(partition: &str, mut e: ApiError) -> ApiError {
    e.message = format!("partition {partition:?}: {}", e.message);
    e
}

/// A `PartitionState` event: unavailable for `e`, or available again.
fn state_event(partition: &str, e: Option<&ApiError>) -> proto::FeedEvent {
    proto::FeedEvent {
        token: Bytes::new(),
        event: Some(proto::feed_event::Event::PartitionState(
            proto::PartitionState {
                partition: partition.to_owned(),
                available: e.is_none(),
                reason: e.map(|e| e.code.clone()).unwrap_or_default(),
                message: e.map(|e| e.message.clone()).unwrap_or_default(),
            },
        )),
        partition: partition.to_owned(),
    }
}

/// An encoded `FeedEvent` as its token, the rest of its fields as they are, and its kind.
/// The token is the first field as prost encodes it, so the rest is usually a slice.
fn split(bytes: Bytes) -> Result<(Bytes, Bytes, Kind), String> {
    let mut token = Bytes::new();
    let mut kind = Kind::Other;
    let mut kept: Vec<(usize, usize)> = Vec::new();
    let mut buf = bytes.clone();
    while buf.has_remaining() {
        let start = bytes.len() - buf.remaining();
        let key = prost::encoding::decode_varint(&mut buf).map_err(|e| e.to_string())?;
        let (field, wire) = ((key >> 3) as u32, key & 7);
        match wire {
            0 => {
                prost::encoding::decode_varint(&mut buf).map_err(|e| e.to_string())?;
            }
            2 => {
                let len = prost::encoding::decode_varint(&mut buf).map_err(|e| e.to_string())?;
                let len = usize::try_from(len).map_err(|e| e.to_string())?;
                if buf.remaining() < len {
                    return Err("truncated".to_owned());
                }
                let at = bytes.len() - buf.remaining();
                if field == TOKEN_FIELD {
                    token = bytes.slice(at..at + len);
                }
                buf.advance(len);
            }
            w => return Err(format!("wire type {w}")),
        }
        match field {
            TOKEN_FIELD => continue,
            COMMIT_FIELD => kind = Kind::Commit,
            PROGRESS_FIELD => kind = Kind::Progress,
            _ => {}
        }
        let end = bytes.len() - buf.remaining();
        match kept.last_mut() {
            Some(last) if last.1 == start => last.1 = end,
            _ => kept.push((start, end)),
        }
    }
    let body = match kept.as_slice() {
        [] => Bytes::new(),
        [(a, b)] => bytes.slice(*a..*b),
        ranges => {
            let mut v = Vec::with_capacity(bytes.len());
            for (a, b) in ranges {
                v.extend_from_slice(&bytes[*a..*b]);
            }
            Bytes::from(v)
        }
    };
    Ok((token, body, kind))
}

/// Appends a length-delimited field.
fn put_field(out: &mut Vec<u8>, field: u32, value: &[u8]) {
    prost::encoding::encode_key(field, prost::encoding::WireType::LengthDelimited, out);
    prost::encoding::encode_varint(value.len() as u64, out);
    out.put_slice(value);
}

/// What opening one component sends.
struct Open {
    partition: String,
    request: proto::SubscribeRequest,
    skip: u64,
    map_version: u64,
}

impl Ctx {
    /// Opens a component where the map puts its partition: here, or on its node. Bounded by
    /// the request timeout, the connection and the first response included, so a node that
    /// accepts connections but never answers can't hold an open forever.
    async fn open(self: Arc<Self>, open: Open) -> Result<Events, ApiError> {
        let within = self.service.request_timeout();
        let partition = open.partition.clone();
        tokio::time::timeout(within, Arc::clone(&self).open_now(open))
            .await
            .unwrap_or_else(|_| {
                Err(ApiError::partition_unavailable(format!(
                    "partition {partition:?}'s node didn't answer within {} ms",
                    within.as_millis()
                )))
            })
    }

    /// A background reopen ([`Merge::reopen`]): after `pull`'s map, if given (bounded by the
    /// request timeout too), and only at the epoch the part's position is from.
    async fn reopen(
        self: Arc<Self>,
        mut open: Open,
        epoch: u64,
        pull: Option<super::route::Owner>,
    ) -> Result<Events, ApiError> {
        if let Some(o) = pull {
            let within = self.service.request_timeout();
            let _ = tokio::time::timeout(
                within,
                self.node.pull_now(&o.node, &o.address, open.map_version),
            )
            .await;
        }
        let now = {
            let routes = self.node.routes();
            let routes = routes
                .as_ref()
                .ok_or_else(|| ApiError::partition_unavailable("this node has no map"))?;
            open.map_version = open.map_version.max(routes.map_version());
            routes.owner(&open.partition).map_or(0, |o| o.epoch)
        };
        if now != epoch {
            // A relocation by promotion gives the partition another history (ticket 6 lets a
            // relocation by file copy keep it).
            return Err(expired(format!(
                "partition {:?} was relocated (epoch {epoch} to {now}): subscribe again",
                open.partition
            )));
        }
        self.open(open).await
    }

    async fn open_now(self: Arc<Self>, open: Open) -> Result<Events, ApiError> {
        let owner = {
            let routes = self.node.routes();
            let routes = routes
                .as_ref()
                .ok_or_else(|| ApiError::partition_unavailable("this node has no map"))?;
            routes.owner(&open.partition).cloned().ok_or_else(|| {
                ApiError::internal(format!("the map lists no node for {:?}", open.partition))
            })?
        };
        if owner.node == self.node.id() {
            if self.local.as_deref() != Some(open.partition.as_str()) {
                return Err(ApiError::partition_unavailable(format!(
                    "the map gives this node partition {:?}, but it isn't open here",
                    open.partition
                )));
            }
            let events = self
                .service
                .subscribe_part(open.request, &self.caller, open.skip, true)
                .await?;
            return Ok(Box::pin(LocalEvents(events)));
        }
        let channel = self.channel(&owner.address).await?;
        let req = proto::ForwardSubscribeRequest {
            map_version: open.map_version,
            caller: Some(super::forward::forwarded_caller(&self.caller)?),
            partition: open.partition,
            request: Some(open.request),
            skip_through: open.skip,
        };
        let request = self.node.request(req).map_err(ApiError::internal)?;
        let mut grpc = tonic::client::Grpc::new(channel);
        let address = owner.address.clone();
        let failed = |status: tonic::Status| super::forward::classify(&status, &address, false);
        grpc.ready()
            .await
            .map_err(|e| failed(tonic::Status::unavailable(e.to_string())))?;
        let path = tonic::codegen::http::uri::PathAndQuery::from_static(FORWARD_SUBSCRIBE);
        let stream = grpc
            .server_streaming(request, path, RawCodec)
            .await
            .map_err(failed)?
            .into_inner();
        Ok(Box::pin(RemoteEvents {
            stream,
            address: owner.address,
        }))
    }

    /// The subscription's connection to the peer at `address` (S1): a stream lease of its
    /// own from [`super::forward::Forwarder::stream_channel`], taken once on first use
    /// (concurrent opens wait for it, simp724's N1) and shared by the subscription's
    /// components there, never by forwarded calls or other streams. Never `ended`: it closes
    /// with the subscription.
    async fn channel(&self, address: &str) -> Result<Channel, ApiError> {
        let cell = Arc::clone(
            self.channels
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(address.to_owned())
                .or_default(),
        );
        let within = self.service.request_timeout();
        let lease = cell
            .get_or_try_init(|| async {
                self.forwarder
                    .stream_channel(address, within)
                    .await
                    .map_err(|e| match e.code.as_str() {
                        // A connect that timed out is the node being unreachable, which
                        // `allow_partial` retries, not the client's deadline.
                        "DeadlineExceeded" => ApiError::partition_unavailable(format!(
                            "the partition's node at {address} didn't accept a connection \
                             within {} ms",
                            within.as_millis()
                        )),
                        _ => e,
                    })
            })
            .await?;
        Ok(lease.channel())
    }
}

/// `Cluster.ForwardSubscribe`'s path.
pub(crate) const FORWARD_SUBSCRIBE: &str = "/pigeonhole.v1.Cluster/ForwardSubscribe";

/// A local component: its events' encodings.
struct LocalEvents(FeedEvents);

impl Stream for LocalEvents {
    type Item = Result<Bytes, ApiError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0)
            .poll_out(cx)
            .map(|item| item.map(|r| r.map(|out| out.to_bytes())))
    }
}

/// A remote component: its events as the bytes they came in.
struct RemoteEvents {
    stream: tonic::Streaming<Bytes>,
    address: String,
}

impl Stream for RemoteEvents {
    type Item = Result<Bytes, ApiError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        Pin::new(&mut this.stream).poll_next(cx).map(|item| {
            item.map(|r| r.map_err(|s| super::forward::classify(&s, &this.address, false)))
        })
    }
}

/// `ForwardSubscribe`'s client codec: the request as prost encodes it, the events as their
/// bytes, which the merge splices without decoding.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RawCodec;

impl tonic::codec::Codec for RawCodec {
    type Encode = proto::ForwardSubscribeRequest;
    type Decode = Bytes;
    type Encoder = tonic_prost::ProstEncoder<proto::ForwardSubscribeRequest>;
    type Decoder = RawDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        tonic_prost::ProstEncoder::default()
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawDecoder
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RawDecoder;

impl tonic::codec::Decoder for RawDecoder {
    type Item = Bytes;
    type Error = tonic::Status;

    fn decode(
        &mut self,
        src: &mut tonic::codec::DecodeBuf<'_>,
    ) -> Result<Option<Bytes>, tonic::Status> {
        let n = src.remaining();
        Ok(Some(src.copy_to_bytes(n)))
    }
}

/// The owner's side of `Cluster.ForwardSubscribe`: served only if this node's map gives it
/// the partition, every table of the request is placed on it, and it's open here; otherwise
/// `WrongPartition` with this node's map version, never forwarded on.
pub(crate) async fn serve(
    node: &Node,
    local: Option<&str>,
    service: &Service,
    req: proto::ForwardSubscribeRequest,
) -> Result<FeedEvents, ApiError> {
    let caller = super::forward::caller_of(req.caller, &node.tokens)?;
    let r = req
        .request
        .ok_or_else(|| ApiError::invalid("a ForwardSubscribe carries no request"))?;
    if r.allow_partial || !r.skip_through.is_empty() || r.checkpoint_every != 0 {
        return Err(ApiError::invalid(
            "a forwarded subscription is one partition's: no cluster-only fields",
        ));
    }
    {
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
        let placed = r.tables.iter().all(|t| {
            routes
                .partitions_of_table(t, b"")
                .is_some_and(|ps| ps.contains(&req.partition.as_str()))
        });
        if !mine || !placed {
            return Err(ApiError::wrong_partition(
                routes.map_version(),
                format!(
                    "this node doesn't serve partition {:?} with those tables",
                    req.partition
                ),
            ));
        }
        if local != Some(req.partition.as_str()) {
            return Err(ApiError::partition_unavailable(format!(
                "the map gives this node partition {:?}, but it isn't open here",
                req.partition
            )));
        }
    }
    service
        .subscribe_part(r, &caller, req.skip_through, true)
        .await
}

/// `Cluster.ForwardSubscribe` over gRPC, routed by hand as `Feed.Subscribe` is: a ring entry
/// goes to the forwarding node as the bytes it was encoded to once. Mounted outside the
/// `Cluster` calls' small limit, since a feed stream lasts; it holds a `--max-streams` place.
#[derive(Clone)]
pub(crate) struct ForwardSubscribeGrpc {
    pub(crate) node: Arc<Node>,
    pub(crate) shared: Arc<crate::serve::Shared>,
    pub(crate) max_request_bytes: usize,
}

/// Its codec: the request decoded as the generated codec does, the events written by
/// [`Out::encode`].
struct ForwardSubscribeCodec;

impl tonic::codec::Codec for ForwardSubscribeCodec {
    type Encode = Out;
    type Decode = proto::ForwardSubscribeRequest;
    type Encoder = crate::serve::grpc::OutEncoder;
    type Decoder = tonic_prost::ProstDecoder<proto::ForwardSubscribeRequest>;

    fn encoder(&mut self) -> Self::Encoder {
        crate::serve::grpc::OutEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        tonic_prost::ProstDecoder::default()
    }
}

struct ForwardSubscribeSvc(ForwardSubscribeGrpc);

impl tonic::server::ServerStreamingService<proto::ForwardSubscribeRequest> for ForwardSubscribeSvc {
    type Response = Out;
    type ResponseStream = crate::serve::grpc::GrpcEvents;
    type Future = tonic::codegen::BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;

    fn call(&mut self, req: tonic::Request<proto::ForwardSubscribeRequest>) -> Self::Future {
        let this = self.0.clone();
        Box::pin(async move {
            let header = req
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok());
            let caller = this.node.caller(header)?;
            this.node.allowed(
                &caller,
                &[crate::serve::auth::Scope::Cluster],
                "forward a subscription",
            )?;
            this.node.maybe_pull(&caller, req.metadata());
            let events = this
                .shared
                .router
                .serve_forwarded_subscribe(req.into_inner())
                .await?;
            Ok(tonic::Response::new(crate::serve::grpc::GrpcEvents(events)))
        })
    }
}

impl<B> tower::Service<tonic::codegen::http::Request<B>> for ForwardSubscribeGrpc
where
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: tonic::codegen::http::Request<B>) -> Self::Future {
        let svc = ForwardSubscribeSvc(self.clone());
        let max = self.max_request_bytes;
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(ForwardSubscribeCodec)
                .apply_max_message_size_config(Some(max), Some(usize::MAX));
            Ok(grpc.server_streaming(svc, req).await)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(token: &[u8], commit_seqno: Option<u64>) -> Bytes {
        let e = proto::FeedEvent {
            token: Bytes::copy_from_slice(token),
            event: Some(match commit_seqno {
                Some(seqno) => proto::feed_event::Event::Commit(proto::FeedCommit {
                    seqno,
                    timestamp: 7,
                    changes: Vec::new(),
                }),
                None => proto::feed_event::Event::Progress(proto::Progress {}),
            }),
            partition: String::new(),
        };
        Bytes::from(prost::Message::encode_to_vec(&e))
    }

    #[test]
    fn an_event_splits_into_its_token_and_the_rest_and_splices_back() {
        let bytes = event(b"position", Some(42));
        let (token, body, kind) = split(bytes).unwrap();
        assert_eq!(&token[..], b"position");
        assert!(matches!(kind, Kind::Commit));
        // The body plus the merge's fields is the event the client gets.
        let mut tail = Vec::new();
        put_field(&mut tail, PARTITION_FIELD, b"p7");
        put_field(&mut tail, TOKEN_FIELD, b"checkpoint");
        let out = Out::Spliced {
            body,
            tail: Bytes::from(tail),
        }
        .into_proto();
        assert_eq!(out.partition, "p7");
        assert_eq!(&out.token[..], b"checkpoint");
        match out.event {
            Some(proto::feed_event::Event::Commit(c)) => assert_eq!(c.seqno, 42),
            other => panic!("{other:?}"),
        }
        let (token, body, kind) = split(event(b"later", None)).unwrap();
        assert_eq!(&token[..], b"later");
        assert!(matches!(kind, Kind::Progress));
        assert!(!body.is_empty());
        assert!(
            split(Bytes::from_static(&[0x0a, 0x05, 1])).is_err(),
            "truncated"
        );
    }
}
