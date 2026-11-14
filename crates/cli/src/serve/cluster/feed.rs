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

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use prost::bytes::{Buf, BufMut, Bytes};
use tonic::transport::Channel;

use super::Node;
use super::token::{At, ClusterToken, subscription_hash};
use crate::serve::auth::Caller;
use crate::serve::errors::ApiError;
use crate::serve::feed::{FeedEvents, Out, SKIP_LIMIT, TakeClock};
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
pub(super) const TOKEN_FIELD: u32 = 1;
const COMMIT_FIELD: u32 = 2;
const PROGRESS_FIELD: u32 = 4;
const PARTITION_FIELD: u32 = 5;

/// What a subscription needs to open (and open again) its components: shared by its task.
pub(super) struct Ctx {
    pub(super) node: Arc<Node>,
    /// The partitions served here, each with its service (D317 ticket 8a).
    pub(super) parts: crate::serve::router::Parts,
    /// The node's service: its stream slots, limits and timeouts.
    pub(super) service: Arc<Service>,
    caller: Caller,
    prefix: Bytes,
    pub(super) visible: bool,
    /// Opens the node-to-node streams' connections, apart from forwarded unary calls'.
    forwarder: Arc<super::forward::Forwarder>,
    /// The subscription's connection to each peer (S1): a stream lease of its own, taken on
    /// first use and held until the subscription ends, never given back (`ended`).
    channels: Mutex<HashMap<String, Arc<tokio::sync::OnceCell<super::forward::StreamLease>>>>,
}

/// A component's stream of encoded events.
type Events = Pin<Box<dyn Stream<Item = Result<Bytes, ApiError>> + Send>>;

/// One partition's part of the subscription.
pub(super) struct Part {
    partition: String,
    /// The subscription's tables placed on it, sorted.
    tables: Vec<String>,
    epoch: u64,
    pub(super) at: At,
    /// Commits through this seqno after `at` are passed over (`skip_through`); 0: none.
    skip: u64,
    events: Option<Events>,
    /// While unavailable (only with `allow_partial`): when to try again.
    retry: Option<tokio::time::Instant>,
    /// An open of it is running in the background.
    opening: bool,
    /// Reported unavailable, and not yet available again.
    pub(super) unavailable: bool,
}

/// `Feed.Subscribe` on a cluster node: opens every component, then merges them on a task of
/// its own. A component that can't be opened fails the call, unless `allow_partial`.
pub(crate) async fn subscribe(
    node: Arc<Node>,
    forwarder: Arc<super::forward::Forwarder>,
    served: crate::serve::router::Parts,
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
    let history = |p: &str| routes.owner(p).map_or(0, |o| o.history_epoch);
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
    let oldest = matches!(r.start, Some(S::FromOldest(true)));
    // A member's first position on a merge of its own: the token it resumes from.
    let token = match &r.start {
        Some(S::Resume(bytes)) => bytes.clone(),
        _ => Bytes::new(),
    };
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
            resume_starts(&t, routes.cluster_id(), subscription, &placed, history)?
        }
    };
    if !skips.is_empty() && !resumed {
        return Err(ApiError::invalid("skip_through needs a resume token"));
    }
    let map_version = routes.map_version();
    let cluster_id = routes.cluster_id().to_owned();
    drop(guard);
    // One `--max-streams` place here for the whole subscription (simp724's C); each
    // component holds one on its partition's node, as any client there does.
    let slot = service.stream_slot()?;
    let ctx = Arc::new(Ctx {
        node,
        forwarder,
        parts: served,
        service: Arc::clone(&service),
        caller: caller.clone(),
        prefix: r.prefix.clone(),
        visible: r.visible,
        channels: Mutex::new(HashMap::new()),
    });
    let every = u64::from(r.checkpoint_every);
    let plan = Plan {
        ctx,
        placed,
        allow_partial: r.allow_partial,
        every,
        map_version,
        subscription,
        cluster_id,
    };
    // A `Now` start shares its key's merge (#1798's B): one per key, not one per subscriber.
    let key = super::hub::Key::new(
        &tables,
        &r.prefix,
        r.visible,
        r.allow_partial,
        every,
        caller,
    );
    super::hub::subscribe(super::hub::Subscribe {
        plan,
        key,
        starts,
        shareable: !resumed && !oldest && skips.is_empty(),
        skips,
        slot,
        token,
    })
    .await
}

/// What a subscription's merges are made from: its hub's, and each member's own (#1798).
#[derive(Clone)]
pub(super) struct Plan {
    pub(super) ctx: Arc<Ctx>,
    /// Each partition the subscription covers, with its tables on it.
    placed: BTreeMap<String, Vec<String>>,
    allow_partial: bool,
    every: u64,
    map_version: u64,
    subscription: u64,
    cluster_id: String,
}

impl Plan {
    /// The partitions, in the merge's parts' order.
    pub(super) fn partitions(&self) -> Vec<String> {
        self.placed.keys().cloned().collect()
    }

    /// A merge from `starts`, its parts not opened yet; `shared`: a hub's
    /// ([`crate::serve::feed::Spec::internal`] for its local components).
    pub(super) fn merge(
        &self,
        starts: &BTreeMap<String, At>,
        skips: &HashMap<String, u64>,
        shared: bool,
    ) -> Merge {
        let epochs: Vec<u64> = {
            let routes = self.ctx.node.routes();
            self.placed
                .keys()
                .map(|p| {
                    routes
                        .as_ref()
                        .and_then(|r| r.owner(p))
                        .map_or(0, |o| o.epoch)
                })
                .collect()
        };
        let parts = self
            .placed
            .iter()
            .zip(epochs)
            .map(|((partition, tables), epoch)| Part {
                epoch,
                at: starts.get(partition).cloned().unwrap_or(At::Now),
                skip: skips.get(partition).copied().unwrap_or(0),
                partition: partition.clone(),
                tables: tables.clone(),
                events: None,
                retry: None,
                opening: false,
                unavailable: false,
            })
            .collect();
        Merge {
            ctx: Arc::clone(&self.ctx),
            parts,
            allow_partial: self.allow_partial,
            every: self.every,
            map_version: self.map_version,
            subscription: self.subscription,
            cluster_id: self.cluster_id.clone(),
            since_checkpoint: 0,
            next_checkpoint: 1,
            moved: false,
            states: Vec::new(),
            token: Vec::new(),
            cache: super::token::TokenCache::default(),
            tail: tail_buffer(),
            opens: tokio::task::JoinSet::new(),
            shared,
            last: None,
        }
    }

    /// Each part's start from one of this subscription's checkpoints, as a resume checks
    /// it: a member that fell off its hub's ring goes on from its last one (#1798).
    pub(super) fn starts_from(&self, token: &[u8]) -> Result<BTreeMap<String, At>, ApiError> {
        let t = ClusterToken::decode(token).map_err(|why| {
            ApiError::internal(format!("a member's checkpoint can't be read: {why}"))
        })?;
        let routes = self.ctx.node.routes();
        let history = |p: &str| {
            routes
                .as_ref()
                .and_then(|r| r.owner(p))
                .map_or(0, |o| o.history_epoch)
        };
        resume_starts(
            &t,
            &self.cluster_id,
            self.subscription,
            &self.placed,
            history,
        )
    }
}

/// Since when the client has left events untaken: its last take, or when its buffer last
/// went from empty to holding events (`waiting_since`), whichever is later.
pub(super) fn untaken_since(
    taken: &TakeClock,
    waiting_since: tokio::time::Instant,
) -> tokio::time::Instant {
    taken.last_or_start().max(waiting_since)
}

/// A subscription ended at its idle limit: counted, said, and its error left for a client
/// that never reads it.
pub(super) fn drop_idle(
    metrics: &crate::serve::service::Metrics,
    partitions: &[String],
    started: std::time::Instant,
    idle: Duration,
) {
    metrics
        .feed_idle_drops
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    metrics.count(
        crate::serve::service::FEED,
        Some("DeadlineExceeded"),
        started.elapsed(),
    );
    eprintln!(
        "phdb serve: a cluster feed subscription over {partitions:?} left events untaken for \
         --stream-idle-ms ({} ms): ended",
        idle.as_millis()
    );
}

/// A merge with what its stream holds: its timers, what goes out before anything else, and
/// a test's `--max-streams` place (a cluster subscription's member holds its own, #1798).
pub(super) struct Merging {
    pub(super) merge: Merge,
    _slot: Option<tokio::sync::OwnedSemaphorePermit>,
    retry: Timer,
    quiet: Quiet,
    turn: usize,
    /// Out first: parts' states, and the last checkpoint then the error that ends the stream
    /// (simp724's R2).
    queued: VecDeque<Result<Out, ApiError>>,
    ended: bool,
}

impl Merging {
    pub(super) fn new(
        merge: Merge,
        slot: Option<tokio::sync::OwnedSemaphorePermit>,
        quiet: Duration,
    ) -> Self {
        Self {
            merge,
            _slot: slot,
            retry: Timer::new(),
            quiet: Quiet::new(quiet),
            turn: 0,
            queued: VecDeque::new(),
            ended: false,
        }
    }

    /// The next item, registering every wake source when there's none: an open finishing,
    /// the retry timer, a part's event, the quiet timer (simp724's R3).
    pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Out, ApiError>>> {
        loop {
            if let Some(item) = self.queued.pop_front() {
                if item.is_ok() {
                    self.quiet.mark();
                }
                return Poll::Ready(Some(item));
            }
            if self.ended {
                return Poll::Ready(None);
            }
            // A part's state goes out next, right after the event that failed it.
            if !self.merge.states.is_empty() {
                for e in std::mem::take(&mut self.merge.states) {
                    self.queued.push_back(Ok(Out::event(e)));
                }
                continue;
            }
            if !self.merge.opens.is_empty()
                && let Poll::Ready(Some(done)) = self.merge.opens.poll_join_next(cx)
            {
                let opened = done
                    .map_err(|e| ApiError::internal(e.to_string()))
                    .and_then(|(i, result)| self.merge.opened(i, result));
                if let Err(e) = opened {
                    self.end(e);
                }
                continue;
            }
            self.retry
                .set(self.merge.parts.iter().filter_map(|p| p.retry).min());
            if self.retry.poll(cx).is_ready() {
                // Every part due is reopened, so the next `set` moves or disarms it.
                let now = tokio::time::Instant::now();
                for i in 0..self.merge.parts.len() {
                    if self.merge.parts[i].retry.is_some_and(|at| at <= now) {
                        self.merge.reopen(i, false);
                    }
                }
                continue;
            }
            if let Poll::Ready((i, item)) = poll_parts(&mut self.merge.parts, &mut self.turn, cx) {
                match self.merge.item(i, item) {
                    Ok(Some(out)) => {
                        self.quiet.mark();
                        return Poll::Ready(Some(Ok(out)));
                    }
                    // A progress passed over, or a part's failure handled: on to the next,
                    // never a stall with nothing registered.
                    Ok(None) => continue,
                    Err(e) => {
                        self.end(e);
                        continue;
                    }
                }
            }
            // Nothing ready: the batch is over, and the quiet interval counts from its last
            // send, timed once here (#1758's g88).
            self.quiet.settle();
            if self.quiet.poll(cx).is_ready() {
                if self.merge.moved {
                    self.queued.push_back(Ok(Out::event(self.merge.progress())));
                } else {
                    self.quiet.sent();
                }
                continue;
            }
            return Poll::Pending;
        }
    }

    /// The stream ends with `e`: after a checkpoint, if the position moved since the last,
    /// so the client resumes from as late as it can.
    fn end(&mut self, e: ApiError) {
        if self.merge.moved {
            self.queued.push_back(Ok(Out::event(self.merge.progress())));
        }
        self.queued.push_back(Err(e));
        self.ended = true;
    }
}

/// Each component's start from a resume token, after checking that it's this cluster's,
/// this subscription's, and that no partition's history changed under it: a component taken
/// at epoch `e` resumes while its partition's `history_epoch` is at most `e` (D317 ticket 6:
/// a relocation by copy keeps the history; a promotion or a restore starts another).
fn resume_starts(
    t: &ClusterToken,
    cluster_id: &str,
    subscription: u64,
    placed: &BTreeMap<String, Vec<String>>,
    history_epoch: impl Fn(&str) -> u64,
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
        let began = history_epoch(&c.partition);
        if began > c.epoch {
            return Err(expired(format!(
                "partition {:?} has another history since the token (taken at epoch {}, its \
                 history began at {began}: a promotion or a restore): subscribe again",
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

/// A subscription's merge: its hub's (#1798), or a member's own.
pub(super) struct Merge {
    ctx: Arc<Ctx>,
    pub(super) parts: Vec<Part>,
    pub(super) allow_partial: bool,
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
    /// The last checkpoint's token ([`Merge::checkpoint`]), kept so a checkpoint per event
    /// allocates nothing (#1758), and what it was built from: only a part that moved is
    /// encoded again (2a).
    pub(super) token: Vec<u8>,
    cache: super::token::TokenCache,
    /// Each commit's tail is written here and split off ([`Merge::cover`]).
    tail: prost::bytes::BytesMut,
    /// Parts opening again in the background, each bounded by the request timeout, so
    /// unreachable partitions never stop the merge from reading the others (simp724's S1 on
    /// #1700).
    opens: tokio::task::JoinSet<(usize, Opened)>,
    /// A cluster feed hub's merge (#1798): its local components don't give their hubs'
    /// rings a full share ([`crate::serve::feed::Spec::internal`]).
    shared: bool,
    /// The last commit [`Merge::cover`] sent: its part, and whether it carried a checkpoint.
    pub(super) last: Option<(usize, bool)>,
}

/// What a component's event was.
enum Kind {
    Commit,
    Progress,
    Other,
}

impl Merge {
    /// Whether every part has said where it is (its first event), or is unavailable
    /// (`allow_partial`): a hub's first checkpoint then names every part (#1798).
    pub(super) fn placed(&self) -> bool {
        self.parts
            .iter()
            .all(|p| p.unavailable || matches!(p.at, At::Token(_)))
    }

    /// A checkpoint now, as its token's bytes.
    pub(super) fn checkpoint_now(&mut self) -> Vec<u8> {
        self.checkpoint();
        self.token.clone()
    }

    /// Opens every component, at most [`OPENS`] at once. A failure fails the call, unless
    /// `allow_partial`: then the part starts unavailable, reported in a first event.
    pub(super) async fn open_all(&mut self) -> Result<(), ApiError> {
        let mut set = tokio::task::JoinSet::new();
        let mut opened: Vec<Option<Opened>> = (0..self.parts.len()).map(|_| None).collect();
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
            epoch: p.epoch,
            internal: self.shared,
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
    fn opened(&mut self, i: usize, result: Opened) -> Result<(), ApiError> {
        self.parts[i].opening = false;
        match result {
            Ok((events, epoch)) => {
                // A reopen may have taken a newer map: later checkpoints carry it (simp724's
                // nit on round 2).
                if let Some(r) = self.ctx.node.routes().as_ref() {
                    self.map_version = self.map_version.max(r.map_version());
                }
                let p = &mut self.parts[i];
                p.events = Some(events);
                // Where it opened: later checkpoints carry it, so a token taken after a
                // relocation by copy is the new epoch's (D317 ticket 6).
                p.epoch = epoch;
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
        let (ctx, open) = (Arc::clone(&self.ctx), self.request(i));
        let pull = pull.then_some(owner).flatten();
        self.opens
            .spawn(async move { (i, ctx.reopen(open, pull).await) });
    }

    /// Part `i`'s next item: an event to send; nothing (a progress, or the part's failure
    /// handled: reopened, or unavailable with `allow_partial`); or the error that ends the
    /// stream.
    fn item(
        &mut self,
        i: usize,
        item: Option<Result<Bytes, ApiError>>,
    ) -> Result<Option<Out>, ApiError> {
        let ended = match item {
            Some(Ok(bytes)) => match self.cover(i, bytes) {
                Ok(out) => return Ok(out),
                Err(e) => e,
            },
            Some(Err(e)) => e,
            None => ApiError::partition_unavailable("its stream ended"),
        };
        self.failed(i, ended).map(|()| None)
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
                // The tail goes into the merge's own buffer, split off for the event: no
                // allocation per event once it has grown (#1758).
                put_field(
                    &mut self.tail,
                    PARTITION_FIELD,
                    self.parts[i].partition.as_bytes(),
                );
                let checkpoint = self.since_checkpoint >= self.next_checkpoint;
                if checkpoint {
                    self.checkpoint();
                    put_field(&mut self.tail, TOKEN_FIELD, &self.token);
                }
                self.last = Some((i, checkpoint));
                Ok(Some(Out::Spliced {
                    body,
                    tail: self.tail.split().freeze(),
                }))
            }
        }
    }

    /// The cluster token as of now, into [`Merge::token`], and the next checkpoint's distance
    /// from it. One per event on a one-partition subscription, so it builds the bytes from
    /// the parts in reused buffers (#1758).
    fn checkpoint(&mut self) {
        self.token.clear();
        self.cache.encode(
            &self.cluster_id,
            self.map_version,
            self.subscription,
            self.parts
                .iter()
                .map(|p| (p.partition.as_str(), p.epoch, &p.at)),
            &mut self.token,
        );
        let token = &self.token;
        self.since_checkpoint = 0;
        self.moved = false;
        self.next_checkpoint = if self.parts.len() == 1 {
            1
        } else if self.every > 0 {
            self.every
        } else {
            (token.len().div_ceil(TOKEN_BYTES_PER_EVENT) as u64).clamp(1, MAX_EVERY)
        };
    }

    /// A progress event with a checkpoint.
    fn progress(&mut self) -> proto::FeedEvent {
        proto::FeedEvent {
            token: {
                self.checkpoint();
                Bytes::copy_from_slice(&self.token)
            },
            event: Some(proto::feed_event::Event::Progress(proto::Progress {})),
            partition: String::new(),
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

/// What a merge's tail buffer starts with ([`Merge::tail`]): each event's tail is split off
/// it, and a buffer that started empty reallocates exactly to size after each split, then
/// grows: 4 allocations an event against main's 2 for a client that's behind (cli692's S1
/// on #1766, measured). Starting with room, it allocates about once per this many bytes of
/// tails (0.02 an event at 8 KiB). Not `BytesMut::new()`: a test holds it.
const TAIL_CAPACITY: usize = 8 << 10;

fn tail_buffer() -> prost::bytes::BytesMut {
    prost::bytes::BytesMut::with_capacity(TAIL_CAPACITY)
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

/// The quiet interval: done once `quiet` passes with nothing sent. Its timer lags the last
/// send rather than following it: when it fires early (something was sent since it was set)
/// it moves to the real deadline, so it moves at most once per quiet period, not once per
/// send (#1758).
struct Quiet {
    quiet: Duration,
    last_sent: tokio::time::Instant,
    /// Sends since `last_sent` was taken ([`Quiet::mark`]), not yet timed.
    marked: bool,
    timer: Timer,
    /// How many times the clock was read for a send, for the tests.
    #[cfg(test)]
    reads: u32,
}

impl Quiet {
    fn new(quiet: Duration) -> Self {
        let last_sent = tokio::time::Instant::now();
        let mut timer = Timer::new();
        timer.set(Some(last_sent + quiet));
        Self {
            quiet,
            last_sent,
            marked: false,
            timer,
            #[cfg(test)]
            reads: 0,
        }
    }

    /// Something was sent now.
    fn sent(&mut self) {
        self.mark();
        self.settle();
    }

    /// Something was sent: timed at the next [`Quiet::settle`], once for a batch of sends
    /// (#1758's g88: a clock read per event).
    fn mark(&mut self) {
        self.marked = true;
    }

    /// The last send, if any since the last settle, was now: called right after a batch's
    /// last send, so the quiet bound counts from it exactly.
    fn settle(&mut self) {
        if std::mem::take(&mut self.marked) {
            self.last_sent = tokio::time::Instant::now();
            #[cfg(test)]
            {
                self.reads += 1;
            }
        }
    }

    /// Ready once `quiet` has passed since the last send; otherwise registered to wake then.
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        // Every batch settles before the merge waits again, or the quiet bound would count
        // from an earlier send: checked in every test.
        debug_assert!(!self.marked, "a send marked but never settled");
        loop {
            std::task::ready!(self.timer.poll(cx));
            let due = self.last_sent + self.quiet;
            if tokio::time::Instant::now() >= due {
                return Poll::Ready(());
            }
            self.timer.set(Some(due));
        }
    }
}

/// A timer kept for a whole stream: set to a deadline, or to none, and moved only when the
/// deadline changes (#1758). Waiting on it doesn't register anything new, and dropping a
/// wait (another `select!` branch won) leaves it armed.
struct Timer {
    sleep: Pin<Box<tokio::time::Sleep>>,
    at: Option<tokio::time::Instant>,
    /// How many times it was moved, for the tests.
    #[cfg(test)]
    moves: u32,
}

impl Timer {
    fn new() -> Self {
        Self {
            // Not polled until it's set, so not registered.
            sleep: Box::pin(tokio::time::sleep_until(tokio::time::Instant::now())),
            at: None,
            #[cfg(test)]
            moves: 0,
        }
    }

    /// Arms it for `at`, or disarms it (`None`). Unchanged costs nothing.
    fn set(&mut self, at: Option<tokio::time::Instant>) {
        if at == self.at {
            return;
        }
        self.at = at;
        if let Some(at) = at {
            self.sleep.as_mut().reset(at);
            #[cfg(test)]
            {
                self.moves += 1;
            }
        }
    }

    /// Ready once its deadline passes; never while disarmed (and then registers nothing).
    /// Stays ready until it's set again.
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        match self.at {
            Some(_) => self.sleep.as_mut().poll(cx),
            None => Poll::Pending,
        }
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
    // The body's ranges: one in the usual case (the token is its first field), held without
    // an allocation (#1758); more only if the token sits between other fields.
    let mut first: Option<(usize, usize)> = None;
    let mut more: Vec<(usize, usize)> = Vec::new();
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
        match (more.last_mut(), &mut first) {
            (Some(last), _) if last.1 == start => last.1 = end,
            (Some(_), _) => more.push((start, end)),
            (None, Some(f)) if f.1 == start => f.1 = end,
            (None, Some(_)) => more.push((start, end)),
            (None, None) => first = Some((start, end)),
        }
    }
    let body = match (first, more.is_empty()) {
        (None, _) => Bytes::new(),
        (Some((a, b)), true) => bytes.slice(a..b),
        (Some(f), false) => {
            let mut v = Vec::with_capacity(bytes.len());
            for (a, b) in std::iter::once(f).chain(more) {
                v.extend_from_slice(&bytes[a..b]);
            }
            Bytes::from(v)
        }
    };
    Ok((token, body, kind))
}

/// Appends a length-delimited field.
pub(super) fn put_field(out: &mut impl BufMut, field: u32, value: &[u8]) {
    prost::encoding::encode_key(field, prost::encoding::WireType::LengthDelimited, out);
    prost::encoding::encode_varint(value.len() as u64, out);
    out.put_slice(value);
}

/// A part opened: its events, and the epoch it opened at (the map's for its partition then).
type Opened = Result<(Events, u64), ApiError>;

/// What opening one component sends.
#[derive(Clone)]
struct Open {
    partition: String,
    request: proto::SubscribeRequest,
    skip: u64,
    map_version: u64,
    /// The epoch the part's position is from: the open refuses a partition whose history
    /// began after it (D317 ticket 6).
    epoch: u64,
    /// A cluster feed hub's component (#1798, [`crate::serve::feed::Spec::internal`]).
    internal: bool,
}

impl Ctx {
    /// Opens a component where the map puts its partition: here, or on its node. Bounded by
    /// the request timeout, the connection and the first response included, so a node that
    /// accepts connections but never answers can't hold an open forever.
    async fn open(self: Arc<Self>, open: Open) -> Opened {
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
    /// request timeout too). The open checks the partition's history against the epoch the
    /// part's position is from.
    async fn reopen(self: Arc<Self>, mut open: Open, pull: Option<super::route::Owner>) -> Opened {
        let moved = pull.is_some();
        if let Some(o) = pull {
            let within = self.service.request_timeout();
            let _ = tokio::time::timeout(
                within,
                self.node.pull_now(&o.node, &o.address, open.map_version),
            )
            .await;
        }
        if let Some(r) = self.node.routes().as_ref() {
            open.map_version = open.map_version.max(r.map_version());
        }
        if !moved {
            return self.open(open).await;
        }
        // The partition moved (its old node answered `WrongPartition`): its new node may
        // still be opening it, so `PartitionUnavailable` is tried again before it counts,
        // every try within what's left of one request timeout (audit120's S-C5). A client
        // that goes away needs no check here: dropping its merge drops `opens`, which
        // aborts this task (simp724's N2 on #1805).
        let within = self.service.request_timeout();
        let until = tokio::time::Instant::now() + within;
        let mut pause = Duration::from_millis(50);
        loop {
            let left = until.saturating_duration_since(tokio::time::Instant::now());
            let tried = tokio::time::timeout(left, Arc::clone(&self).open_now(open.clone()))
                .await
                .unwrap_or_else(|_| {
                    Err(ApiError::partition_unavailable(format!(
                        "partition {:?} moved, and its new node didn't serve it within {} ms",
                        open.partition,
                        within.as_millis()
                    )))
                });
            match tried {
                Err(e)
                    if e.code == "PartitionUnavailable"
                        && tokio::time::Instant::now() + pause < until =>
                {
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(RETRY);
                }
                r => return r,
            }
        }
    }

    /// Opens part `open` where the map puts it now, returning its events and the epoch it
    /// opened at. A partition whose history began after the part's position (a promotion
    /// or a restore since) is `FeedExpired`; one relocated by copying its files keeps its
    /// history, and the position continues there (D317 ticket 6).
    async fn open_now(self: Arc<Self>, open: Open) -> Opened {
        let owner = {
            let routes = self.node.routes();
            let routes = routes
                .as_ref()
                .ok_or_else(|| ApiError::partition_unavailable("this node has no map"))?;
            routes.owner(&open.partition).cloned().ok_or_else(|| {
                ApiError::internal(format!("the map lists no node for {:?}", open.partition))
            })?
        };
        if owner.history_epoch > open.epoch {
            return Err(expired(format!(
                "partition {:?} has another history since this position (from epoch {}, its \
                 history began at {}: a promotion or a restore): subscribe again",
                open.partition, open.epoch, owner.history_epoch
            )));
        }
        let epoch = owner.epoch;
        if owner.node == self.node.id() {
            let Some(here) = self.parts.load().get(&open.partition).cloned() else {
                return Err(ApiError::partition_unavailable(format!(
                    "the map gives this node partition {:?}, but it isn't open here",
                    open.partition
                )));
            };
            let events = here
                .subscribe_part(open.request, &self.caller, open.skip, true, open.internal)
                .await?;
            still_owner(&self.node, &open.partition)?;
            return Ok((Box::pin(LocalEvents(events)), epoch));
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
        // A commit's event can be larger than tonic's default 4 MiB cap (audit120's S-C2).
        let mut grpc = tonic::client::Grpc::new(channel).max_decoding_message_size(usize::MAX);
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
        Ok((
            Box::pin(RemoteEvents {
                stream,
                address: owner.address,
            }),
            epoch,
        ))
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
    parts: &crate::serve::router::Parts,
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
        let Some(service) = parts.load().get(&req.partition).cloned() else {
            return Err(ApiError::partition_unavailable(format!(
                "the map gives this node partition {:?}, but it isn't open here",
                req.partition
            )));
        };
        service
    };
    let events = service
        .subscribe_part(r, &caller, req.skip_through, true, false)
        .await?;
    still_owner(node, &req.partition)?;
    Ok(events)
}

/// After a component subscribed on this node: `WrongPartition` if a map installed since
/// the check before it gives the partition to another node. That install's
/// [`Feeds::end_parts`](crate::serve::feed::Feeds::end_parts) may have run before the
/// component was there to end, and its routes were stored before it ran, so this sees them
/// (audit120's S-C5).
fn still_owner(node: &Node, partition: &str) -> Result<(), ApiError> {
    let routes = node.routes();
    let routes = routes.as_ref();
    if routes.is_some_and(|r| r.owner(partition).is_some_and(|o| o.node == node.id())) {
        return Ok(());
    }
    Err(ApiError::wrong_partition(
        routes.map_or(0, |r| r.map_version()),
        format!("the map moved partition {partition:?} off this node"),
    ))
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
            this.node.catch_up(&caller, req.metadata()).await?;
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
                .apply_max_message_size_config(
                    Some(max.saturating_add(super::forward::FORWARD_ENVELOPE)),
                    Some(usize::MAX),
                );
            Ok(grpc.server_streaming(svc, req).await)
        })
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::serve::cluster::token::Component;

    #[test]
    fn a_token_resumes_across_a_copy_and_expires_across_another_history() {
        // D317 ticket 6: a component taken at epoch `e` resumes while its partition's
        // history began at or before `e`.
        let token = ClusterToken {
            cluster_id: "c1".into(),
            map_version: 3,
            subscription: 7,
            components: ["p1", "p2"]
                .map(|p| Component {
                    partition: p.into(),
                    epoch: 2,
                    at: At::Token(Bytes::from_static(b"t")),
                })
                .to_vec(),
        };
        let placed: BTreeMap<String, Vec<String>> = ["p1", "p2"]
            .map(|p| (p.to_owned(), vec!["users".to_owned()]))
            .into();
        let starts = |history: [u64; 2]| {
            resume_starts(&token, "c1", 7, &placed, |p: &str| {
                if p == "p1" { history[0] } else { history[1] }
            })
        };
        // Maps from before ticket 6 (0), a history from before the token, one that began
        // with it, and one a copy since kept (still 2 at a later epoch): all resume.
        for h in [[0, 0], [1, 2], [2, 2]] {
            let s = starts(h).unwrap();
            assert_eq!(s.len(), 2, "{h:?}");
        }
        // p2 promoted or restored at epoch 3: its history began after the token.
        let e = starts([2, 3]).unwrap_err();
        assert_eq!(e.code, "FeedExpired", "{e:?}");
        assert!(e.message.contains("\"p2\" has another history"), "{e:?}");
    }

    /// A runtime on a paused clock: virtual time, advanced only when every task waits.
    pub(in crate::serve::cluster) fn paused() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap()
    }

    /// A timer as a future: ready once it fires.
    async fn wait(t: &mut Timer) {
        std::future::poll_fn(|cx| t.poll(cx)).await;
    }

    /// The quiet interval as a future: ready once it has passed since the last send.
    async fn quiet_passed(q: &mut Quiet) {
        std::future::poll_fn(|cx| q.poll(cx)).await;
    }

    /// Whether `f` is done within `d` of virtual time.
    async fn done_within(d: Duration, f: impl std::future::Future<Output = ()>) -> bool {
        tokio::time::timeout(d, f).await.is_ok()
    }

    #[test]
    fn a_timer_moves_only_when_its_deadline_changes() {
        // #1758: the merge sets the retry timer on every event; only a change may move it.
        paused().block_on(async {
            let start = tokio::time::Instant::now();
            let mut t = Timer::new();
            assert!(
                !done_within(Duration::from_secs(3600), wait(&mut t)).await,
                "unset"
            );
            let at = tokio::time::Instant::now() + Duration::from_secs(1);
            for _ in 0..1000 {
                t.set(Some(at));
            }
            assert_eq!(t.moves, 1, "set to the same deadline 1000 times");
            wait(&mut t).await;
            assert_eq!(tokio::time::Instant::now(), at, "done at its deadline");
            assert!(start < at);
            assert!(
                done_within(Duration::ZERO, wait(&mut t)).await,
                "stays done until set again"
            );
            t.set(None);
            assert!(
                !done_within(Duration::from_secs(3600), wait(&mut t)).await,
                "disarmed"
            );
            let later = tokio::time::Instant::now() + Duration::from_secs(5);
            t.set(Some(later));
            wait(&mut t).await;
            assert_eq!(tokio::time::Instant::now(), later);
            assert_eq!(t.moves, 2);
        });
    }

    #[test]
    fn a_batch_of_sends_reads_the_clock_once_and_quiet_counts_from_its_end() {
        // #1758's g88 (2c-3): the merge marks each send and settles once per drained batch.
        // One clock read for the batch, and the quiet bound counts exactly from the settle,
        // right after the batch's last send.
        paused().block_on(async {
            let quiet = Duration::from_millis(100);
            let mut q = Quiet::new(quiet);
            tokio::time::advance(Duration::from_millis(30)).await;
            for _ in 0..500 {
                q.mark();
            }
            q.settle();
            assert_eq!(q.reads, 1, "one read for 500 sends");
            let last = tokio::time::Instant::now();
            // Settling with nothing marked reads nothing and moves nothing.
            tokio::time::advance(Duration::from_millis(10)).await;
            q.settle();
            assert_eq!(q.reads, 1);
            quiet_passed(&mut q).await;
            assert_eq!(tokio::time::Instant::now(), last + quiet);
        });
    }

    #[test]
    fn the_quiet_timer_moves_once_per_quiet_period_not_per_send() {
        // #1758: a send per event moved a new quiet timer per event. Sends now only note the
        // time; the timer moves when it fires early, and the wait still ends exactly `quiet`
        // after the last send.
        paused().block_on(async {
            let quiet = Duration::from_millis(100);
            let mut q = Quiet::new(quiet);
            assert_eq!(q.timer.moves, 1, "armed once");
            // 1000 sends 1 ms apart: 10 quiet periods' worth, never quiet.
            for _ in 0..1000 {
                tokio::time::advance(Duration::from_millis(1)).await;
                q.sent();
                assert!(
                    !done_within(Duration::ZERO, quiet_passed(&mut q)).await,
                    "not quiet yet"
                );
            }
            assert!(
                q.timer.moves <= 11,
                "moved {} times for 1000 sends over 10 quiet periods",
                q.timer.moves
            );
            let last = tokio::time::Instant::now();
            quiet_passed(&mut q).await;
            assert_eq!(
                tokio::time::Instant::now(),
                last + quiet,
                "quiet after the last send"
            );
            // Quiet again a period after it's noted as sent.
            q.sent();
            quiet_passed(&mut q).await;
            assert_eq!(tokio::time::Instant::now(), last + quiet * 2);
        });
    }

    pub(in crate::serve::cluster) fn event(token: &[u8], commit_seqno: Option<u64>) -> Bytes {
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

    /// A D310 version-2 token of 4 streams at `n`: what a one-partition subscription's part
    /// carries on every event.
    pub(in crate::serve::cluster) fn d310_token(n: u64) -> Vec<u8> {
        let mut t = vec![2u8];
        t.extend_from_slice(&[7u8; 16]);
        t.extend_from_slice(&4u32.to_le_bytes());
        t.extend_from_slice(&(1_000_000 + n).to_le_bytes());
        t.extend_from_slice(&0x1234_5678_9abc_def0u64.to_le_bytes());
        for s in 0..4u64 {
            let hint = (3u64 << 40) + 4096 * (n + s);
            t.extend_from_slice(&hint.to_le_bytes());
            t.extend_from_slice(&(hint + 512).to_le_bytes());
        }
        t.extend_from_slice(&5u64.to_le_bytes());
        t.extend_from_slice(&9u64.to_le_bytes());
        t
    }

    /// Times a one-partition merge's per-event work (#1798 (b)): `cover` of a commit, with
    /// its checkpoint token. Run by hand: `cargo test --release ... -- --ignored
    /// one_partition_cover_cost --nocapture`.
    #[test]
    #[ignore = "a timing, run by hand"]
    fn one_partition_cover_cost() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _guard = rt.enter();
        let (_feed, events) = fed();
        let mut m = merging(vec![Some(events)], QUIET);
        let batch: Vec<Bytes> = (0..10_000u64)
            .map(|n| event(&d310_token(n), Some(n + 1)))
            .collect();
        let mut sink = 0usize;
        for e in &batch {
            sink += m.merge.cover(0, e.clone()).unwrap().is_some() as usize;
        }
        // More rounds for a profiler (`COVER_ROUNDS`).
        let rounds: u32 = std::env::var("COVER_ROUNDS")
            .ok()
            .and_then(|r| r.parse().ok())
            .unwrap_or(50);
        let started = std::time::Instant::now();
        for _ in 0..rounds {
            for e in &batch {
                if let Some(Out::Spliced { tail, .. }) = m.merge.cover(0, e.clone()).unwrap() {
                    sink += tail.len();
                }
            }
        }
        let per = started.elapsed() / (rounds * batch.len() as u32);
        eprintln!("one-partition cover: {per:?} an event ({sink})");
    }

    #[test]
    fn a_merge_s_tail_buffer_starts_with_room() {
        // cli692's S1 on #1766: an empty start reallocates per event once tails are split
        // off it. Tails split off a buffer with room share its allocation.
        let mut tail = tail_buffer();
        assert!(tail.capacity() >= 4 << 10, "{}", tail.capacity());
        let before = tail.as_ptr();
        put_field(&mut tail, PARTITION_FIELD, b"p1");
        let first = tail.split().freeze();
        put_field(&mut tail, PARTITION_FIELD, b"p2");
        let second = tail.split().freeze();
        assert_eq!(
            first.as_ptr(),
            before,
            "the first tail is in the buffer's allocation"
        );
        assert_eq!(
            second.as_ptr(),
            before.wrapping_add(first.len()),
            "and the next one right after it"
        );
    }

    /// A part's events as a test feeds them.
    struct Fed(tokio::sync::mpsc::UnboundedReceiver<Result<Bytes, ApiError>>);

    impl Stream for Fed {
        type Item = Result<Bytes, ApiError>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.0.poll_recv(cx)
        }
    }

    pub(in crate::serve::cluster) type Feed =
        tokio::sync::mpsc::UnboundedSender<Result<Bytes, ApiError>>;

    pub(in crate::serve::cluster) fn fed() -> (Feed, Events) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (tx, Box::pin(Fed(rx)))
    }

    /// A merge of `n` parts (`p0`, `p1`, ...), each fed by the test or not open yet
    /// (`None`), with `allow_partial`, as its client's stream would hold it. Its node has no
    /// map, so a reopen fails as unavailable.
    pub(in crate::serve::cluster) fn merging(
        events: Vec<Option<Events>>,
        quiet: Duration,
    ) -> Merging {
        let dir = super::super::tests::test_dir();
        let node = super::super::tests::node(&dir, "a");
        let db = pigeonhole::Pigeonhole::open(
            dir.join("svc.phdb"),
            pigeonhole::Options::default().create_if_missing(true),
        )
        .unwrap();
        let service = Arc::new(Service::new(
            db,
            crate::serve::server_tests::limits(),
            2,
            Arc::new(crate::serve::service::Metrics::default()),
        ));
        let ctx = Arc::new(Ctx {
            node: Arc::clone(&node),
            parts: Arc::new(arc_swap::ArcSwap::from_pointee(BTreeMap::new())),
            service,
            caller: Caller::anonymous(),
            prefix: Bytes::new(),
            visible: false,
            forwarder: Arc::new(super::super::forward::Forwarder::new(node)),
            channels: Mutex::new(HashMap::new()),
        });
        let parts = events
            .into_iter()
            .enumerate()
            .map(|(i, events)| Part {
                partition: format!("p{i}"),
                tables: vec!["users".into()],
                epoch: 1,
                at: At::Now,
                skip: 0,
                opening: events.is_none(),
                events,
                retry: None,
                unavailable: false,
            })
            .collect();
        let merge = Merge {
            ctx,
            parts,
            allow_partial: true,
            every: 0,
            map_version: 1,
            subscription: 9,
            cluster_id: "c1".into(),
            since_checkpoint: 0,
            next_checkpoint: 1,
            moved: false,
            states: Vec::new(),
            token: Vec::new(),
            cache: super::super::token::TokenCache::default(),
            tail: tail_buffer(),
            opens: tokio::task::JoinSet::new(),
            shared: false,
            last: None,
        };
        let slot = Arc::new(tokio::sync::Semaphore::new(1))
            .try_acquire_owned()
            .unwrap();
        Merging::new(merge, Some(slot), quiet)
    }

    /// The next item, within `d` of virtual time: `None` if nothing woke the stream with one.
    async fn next_within(m: &mut Merging, d: Duration) -> Option<Option<Result<Out, ApiError>>> {
        tokio::time::timeout(d, std::future::poll_fn(|cx| m.poll(cx)))
            .await
            .ok()
    }

    pub(in crate::serve::cluster) fn commit_of(
        item: Option<Option<Result<Out, ApiError>>>,
    ) -> (String, u64) {
        let e = item
            .expect("woken")
            .expect("not ended")
            .unwrap()
            .into_proto();
        match e.event {
            Some(proto::feed_event::Event::Commit(c)) => (e.partition, c.seqno),
            other => panic!("{other:?}"),
        }
    }

    pub(in crate::serve::cluster) const QUIET: Duration = Duration::from_secs(10);

    /// `m`'s subscription as a [`Plan`] (#1798's member tests).
    pub(in crate::serve::cluster) fn plan_of(m: &Merging) -> Plan {
        let merge = &m.merge;
        Plan {
            ctx: Arc::clone(&merge.ctx),
            placed: merge
                .parts
                .iter()
                .map(|p| (p.partition.clone(), p.tables.clone()))
                .collect(),
            allow_partial: merge.allow_partial,
            every: merge.every,
            map_version: merge.map_version,
            subscription: merge.subscription,
            cluster_id: merge.cluster_id.clone(),
        }
    }

    #[test]
    fn the_quiet_tick_alone_wakes_the_stream_with_a_checkpoint() {
        // simp724's R3: with the parts quiet, the quiet timer is what wakes the stream.
        paused().block_on(async {
            let (p0, events) = fed();
            let mut m = merging(vec![Some(events)], QUIET);
            p0.send(Ok(event(b"t1", Some(1)))).unwrap();
            // The part's own progress moves the position past the last checkpoint (one
            // part: every commit is one), and sends nothing.
            p0.send(Ok(event(b"t2", None))).unwrap();
            let start = tokio::time::Instant::now();
            assert_eq!(
                commit_of(next_within(&mut m, Duration::ZERO).await),
                ("p0".into(), 1)
            );
            // Nothing more from the part: the next item is the quiet checkpoint, at the
            // quiet interval after the last send exactly: only the quiet timer could wake
            // it.
            let e = next_within(&mut m, QUIET * 2)
                .await
                .expect("woken by the quiet timer")
                .unwrap()
                .unwrap()
                .into_proto();
            assert!(
                matches!(e.event, Some(proto::feed_event::Event::Progress(_))),
                "{e:?}"
            );
            assert!(!e.token.is_empty(), "a checkpoint");
            assert_eq!(tokio::time::Instant::now(), start + QUIET);
        });
    }

    #[test]
    fn an_open_finishing_alone_wakes_the_stream() {
        paused().block_on(async {
            let mut m = merging(vec![None], QUIET);
            let (p0, events) = fed();
            m.merge.opens.spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                (0, Ok((events, 1)))
            });
            p0.send(Ok(event(b"t1", Some(5)))).unwrap();
            assert_eq!(
                commit_of(next_within(&mut m, Duration::from_millis(200)).await),
                ("p0".into(), 5),
                "the open's finish woke it, and its part's event came"
            );
        });
    }

    #[test]
    fn the_retry_timer_alone_wakes_the_stream_to_reopen_a_part() {
        paused().block_on(async {
            let mut m = merging(vec![None], QUIET);
            m.merge.parts[0].opening = false;
            let start = tokio::time::Instant::now();
            m.merge.parts[0].retry = Some(start + RETRY);
            m.merge.parts[0].unavailable = true;
            // Nothing to send: it waits, and the retry timer wakes it at RETRY to reopen p0
            // (which fails, its node having no map, and is due again a RETRY later).
            assert!(
                next_within(&mut m, RETRY + Duration::from_millis(50))
                    .await
                    .is_none()
            );
            let again = m.merge.parts[0].retry;
            assert!(
                m.merge.parts[0].opening || again.is_some_and(|at| at > start + RETRY),
                "reopened at the retry: {again:?}"
            );
        });
    }

    #[test]
    fn a_skipped_event_never_stalls_the_stream() {
        // A progress event is passed over (`cover` gives nothing): the stream goes on to the
        // next, rather than returning Pending with nothing to wake it.
        paused().block_on(async {
            let (p0, events) = fed();
            let mut m = merging(vec![Some(events)], QUIET);
            p0.send(Ok(event(b"t1", None))).unwrap();
            p0.send(Ok(event(b"t2", None))).unwrap();
            p0.send(Ok(event(b"t3", Some(3)))).unwrap();
            assert_eq!(
                commit_of(next_within(&mut m, Duration::ZERO).await),
                ("p0".into(), 3)
            );
        });
    }

    #[test]
    fn a_part_gone_unavailable_is_reported_right_after_its_failure() {
        // simp724's N1 on #1772, end to end: p0's commits keep coming, p1 fails (with
        // allow_partial); p1's state is the next item after its failure, not behind p0's.
        paused().block_on(async {
            let (p0, e0) = fed();
            let (p1, e1) = fed();
            let mut m = merging(vec![Some(e0), Some(e1)], QUIET);
            for s in 1..=10 {
                p0.send(Ok(event(format!("t{s}").as_bytes(), Some(s))))
                    .unwrap();
            }
            p1.send(Err(ApiError::partition_unavailable("gone")))
                .unwrap();
            // Round-robin from p0: p0's first, then p1's failure, whose state comes next.
            assert_eq!(
                commit_of(next_within(&mut m, Duration::ZERO).await),
                ("p0".into(), 1)
            );
            let e = next_within(&mut m, Duration::ZERO)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .into_proto();
            match e.event {
                Some(proto::feed_event::Event::PartitionState(st)) => {
                    assert_eq!((st.partition.as_str(), st.available), ("p1", false));
                }
                other => panic!("p1's state next, got {other:?}"),
            }
            assert_eq!(
                commit_of(next_within(&mut m, Duration::ZERO).await),
                ("p0".into(), 2)
            );
        });
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
        // A token between other fields (not prost's order, but valid): the body is the
        // fields around it, joined.
        let mut odd = Vec::new();
        put_field(&mut odd, PARTITION_FIELD, b"p1");
        put_field(&mut odd, TOKEN_FIELD, b"tok");
        put_field(&mut odd, PROGRESS_FIELD, b"");
        let (token, body, kind) = split(Bytes::from(odd)).unwrap();
        assert_eq!(&token[..], b"tok");
        assert!(matches!(kind, Kind::Progress));
        let mut want = Vec::new();
        put_field(&mut want, PARTITION_FIELD, b"p1");
        put_field(&mut want, PROGRESS_FIELD, b"");
        assert_eq!(&body[..], &want[..]);
    }
}
