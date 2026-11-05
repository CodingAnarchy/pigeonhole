//! `Feed.Subscribe` (D313 §4, R2): one engine subscription per table set in use, fanned out
//! in the server.
//!
//! - **A hub per table set and visibility.** It owns one engine `FeedStream` for exactly that
//!   spec (the `Table` handles the service resolved, so the same table ids as a client's own
//!   stream would use) and appends each commit to a bounded ring with the token taken right
//!   after it and the seqno before it. A shard thread wakes one reader per hub, whatever the
//!   number of clients, and the WAL is read once per hub. Hubs are keyed by the exact set,
//!   never a union that grows: a wider feed resumed from a narrower token can miss the new
//!   tables' commits its cursor had already passed (the reader filters as it reads).
//! - **A client** is a task with a one-event channel to its connection. On the ring it takes
//!   the entries after its position (each an `Arc`, so a send is a refcount), applies its
//!   prefix, and encodes on its own task. A commit with no row under the prefix is passed
//!   over, as the engine's prefix feed does, and later covered by a progress event.
//! - **Off the ring:** a client that starts from a token, a snapshot or the oldest commit,
//!   that falls off the ring's tail, or whose hub fails, reads its own `FeedStream` (same
//!   tables, no prefix, so it sees the hub's exact commit sequence) from its own last token.
//!   It rejoins the ring at the entry whose predecessor is the last commit it delivered, so
//!   nothing is missed or repeated (feed1248's (2)). At most `--max-catch-up` own streams run
//!   at once; the rest wait.
//! - **Tokens** are always the one taken right after the last commit a client covered,
//!   delivered or passed over, never a live one (feed1248's (1)), so a resume never skips.
//!   The server compares tokens only for equality (has the position moved), never orders
//!   them.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_core::Stream;
use pigeonhole::nonblocking::FeedStream;
use pigeonhole::{Commit, FeedToken, Snapshot, Table};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};

use super::convert;
use super::errors::ApiError;
use super::proto;

/// The feed fan-out's limits.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FeedLimits {
    /// Bytes of commits each hub's ring holds (`--feed-ring-bytes`), tokens included: memory
    /// outside R3's in-flight budget.
    pub ring_bytes: usize,
    /// Hubs at once (`--max-feed-hubs`): distinct table sets subscribed.
    pub max_hubs: usize,
    /// Clients reading their own catch-up stream at once (`--max-catch-up`).
    pub max_catch_up: usize,
    /// After this long without an event, a stream whose position moved sends a progress
    /// event (perf287's S2, ICR 0112: about 10 s).
    pub quiet: Duration,
    /// How long a client may leave an event untaken (`--stream-idle-ms`, perf287's G2).
    pub stream_idle: Duration,
}

/// Where a client starts.
pub(crate) enum Start {
    Now,
    Snapshot(Snapshot),
    Oldest,
    Token(FeedToken),
}

/// What a client subscribed to, resolved by the service.
pub(crate) struct Spec {
    /// Sorted by name, without repeats: the hub's key.
    pub tables: Vec<Table>,
    pub prefix: Vec<u8>,
    pub visible: bool,
    pub start: Start,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct HubKey {
    tables: Vec<String>,
    visible: bool,
}

/// One commit in a hub's ring.
struct Entry {
    commit: Commit,
    /// The hub feed's token right after this commit.
    token: Vec<u8>,
    /// Where the hub's feed was just before it: the seqno of the commit before it, or for
    /// the hub's first entry the snapshot the feed started after.
    prev: u64,
    bytes: usize,
}

#[derive(Default)]
struct Ring {
    entries: VecDeque<Arc<Entry>>,
    /// The index of `entries[0]` in the hub's whole sequence.
    base: u64,
    bytes: usize,
    /// The token the hub's feed started at, before any entry.
    start_token: Vec<u8>,
    /// The seqno of the snapshot the hub's feed started after: it holds every commit of its
    /// spec above it.
    start_seqno: u64,
    /// The shared feed ended (an error): clients move to their own streams.
    failed: bool,
}

impl Ring {
    fn head(&self) -> u64 {
        self.base + self.entries.len() as u64
    }

    /// Where a client at `seqno` rejoins: the index of the first entry after it. The client
    /// has every commit of the spec up to `seqno`, and the ring every one after the position
    /// before its first entry. So it rejoins only if that position is at most `seqno` (nothing
    /// between them was evicted) and the hub's feed has passed `seqno` (nothing it will add
    /// is one the client has): feed1248's (2) on ticket 5.
    fn rejoin_at(&self, seqno: u64) -> Option<u64> {
        let covered_from = self.entries.front().map_or(self.start_seqno, |e| e.prev);
        let passed = self
            .entries
            .back()
            .map_or(self.start_seqno, |e| e.commit.seqno());
        if covered_from > seqno || passed < seqno {
            return None;
        }
        let i = self
            .entries
            .iter()
            .position(|e| e.commit.seqno() > seqno)
            .unwrap_or(self.entries.len());
        Some(self.base + i as u64)
    }
}

struct Hub {
    ring: Mutex<Ring>,
    /// The ring's head index, bumped by each commit and by a failure.
    head: watch::Sender<u64>,
    task: tokio::task::AbortHandle,
    stats: Arc<FeedStats>,
}

impl Hub {
    fn ring(&self) -> MutexGuard<'_, Ring> {
        self.ring.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn push(&self, commit: Commit, token: Vec<u8>, limit: usize) {
        let bytes = entry_bytes(&commit, &token);
        let mut r = self.ring();
        let prev = r.entries.back().map_or(r.start_seqno, |e| e.commit.seqno());
        r.entries.push_back(Arc::new(Entry {
            commit,
            token,
            prev,
            bytes,
        }));
        r.bytes += bytes;
        // Always keep the newest: a client at the head needs its boundary.
        while r.bytes > limit && r.entries.len() > 1 {
            if let Some(e) = r.entries.pop_front() {
                r.bytes -= e.bytes;
                r.base += 1;
            }
        }
        let head = r.head();
        drop(r);
        self.head.send_replace(head);
    }

    fn fail(&self) {
        self.ring().failed = true;
        self.stats.hub_failures.fetch_add(1, Ordering::Relaxed);
        // Wake every client: each moves to its own stream once it has taken the ring's rest.
        self.head.send_modify(|_| {});
    }
}

impl Drop for Hub {
    fn drop(&mut self) {
        // The last client left: the engine subscription (and its shard wakers) goes.
        self.task.abort();
    }
}

/// A commit's size in the ring: its changes, its token (about 45 + 16 × shards bytes,
/// feed1248's (1)), and a fixed overhead.
fn entry_bytes(c: &Commit, token: &[u8]) -> usize {
    let changes: usize = c
        .changes()
        .iter()
        .map(|ch| {
            48 + ch.table().len()
                + ch.family().len()
                + ch.row().len()
                + ch.qualifier().len()
                + ch.value().map_or(0, |v| value_len(&v))
        })
        .sum();
    96 + changes + token.len()
}

fn value_len(v: &pigeonhole::Value<'_>) -> usize {
    match v {
        pigeonhole::Value::Bytes(b) => b.len(),
        pigeonhole::Value::Vector(v) => v.as_bytes().len(),
        _ => 8,
    }
}

/// The fan-out's counters, for `/metrics`.
#[derive(Debug, Default)]
pub(crate) struct FeedStats {
    pub hub_failures: AtomicU64,
    /// Clients that fell off a ring's tail onto their own stream.
    pub fell_behind: AtomicU64,
    /// Clients that rejoined a ring from their own stream.
    pub rejoined: AtomicU64,
    /// Own streams that waited for a `--max-catch-up` slot.
    pub catch_up_waits: AtomicU64,
}

/// The hubs, by table set.
pub(crate) struct Feeds {
    db: pigeonhole::Pigeonhole,
    hubs: Mutex<HashMap<HubKey, Weak<Hub>>>,
    catch_up: Arc<Semaphore>,
    limits: FeedLimits,
    pub(crate) stats: Arc<FeedStats>,
}

impl std::fmt::Debug for Feeds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Feeds")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

/// A gauge snapshot for `/metrics`.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct FeedGauges {
    pub hubs: usize,
    pub ring_bytes: usize,
    pub catching_up: usize,
}

impl Feeds {
    pub(crate) fn new(db: pigeonhole::Pigeonhole, limits: FeedLimits) -> Arc<Self> {
        Arc::new(Self {
            db,
            hubs: Mutex::new(HashMap::new()),
            catch_up: Arc::new(Semaphore::new(limits.max_catch_up.max(1))),
            limits,
            stats: Arc::default(),
        })
    }

    pub(crate) fn gauges(&self) -> FeedGauges {
        let hubs = self.hubs.lock().unwrap_or_else(PoisonError::into_inner);
        let live: Vec<Arc<Hub>> = hubs.values().filter_map(Weak::upgrade).collect();
        drop(hubs);
        FeedGauges {
            hubs: live.len(),
            ring_bytes: live.iter().map(|h| h.ring().bytes).sum(),
            catching_up: self.limits.max_catch_up.max(1) - self.catch_up.available_permits(),
        }
    }

    /// The hub for `spec`'s tables, opened if none is live.
    fn hub(&self, spec: &Spec) -> Result<Arc<Hub>, ApiError> {
        let key = HubKey {
            tables: spec.tables.iter().map(|t| t.name().to_owned()).collect(),
            visible: spec.visible,
        };
        let mut hubs = self.hubs.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(hub) = hubs.get(&key).and_then(Weak::upgrade) {
            return Ok(hub);
        }
        hubs.retain(|_, w| w.strong_count() > 0);
        if hubs.len() >= self.limits.max_hubs.max(1) {
            return Err(ApiError::overloaded(
                "the server is at its feed hub limit (--max-feed-hubs): too many distinct table \
                 sets are subscribed",
            ));
        }
        // From a snapshot taken now, at the visibility it delivers at, so the ring knows the
        // position it starts from exactly: a client catching up can then rejoin it before
        // the hub has a commit of its own.
        let snapshot = if spec.visible {
            self.db.snapshot()?
        } else {
            self.db.durable_snapshot()?
        };
        let start_seqno = snapshot.seqno();
        let mut stream = FeedStream::new(open(
            &spec.tables,
            spec.visible,
            &Start::Snapshot(snapshot),
        )?);
        let start_token = stream.token().as_bytes().to_vec();
        let (head, _) = watch::channel(0);
        let limit = self.limits.ring_bytes;
        let stats = Arc::clone(&self.stats);
        let hub = Arc::new_cyclic(|weak: &Weak<Hub>| {
            let weak = weak.clone();
            let task = tokio::spawn(async move {
                loop {
                    let item = next(&mut stream).await;
                    let Some(hub) = weak.upgrade() else { return };
                    match item {
                        Some(Ok(commit)) => {
                            hub.push(commit, stream.token().as_bytes().to_vec(), limit);
                        }
                        // An expiry, `Closed`, damage: each client goes on from its own
                        // token on its own stream, and the engine decides each (feed1248's
                        // (5)).
                        _ => {
                            hub.fail();
                            return;
                        }
                    }
                }
            })
            .abort_handle();
            Hub {
                ring: Mutex::new(Ring {
                    start_token,
                    start_seqno,
                    ..Ring::default()
                }),
                head,
                task,
                stats,
            }
        });
        hubs.insert(key, Arc::downgrade(&hub));
        Ok(hub)
    }

    /// Starts a client's stream of events into `tx`, and its `end`. The caller holds the
    /// client's stream slot in `slot`, released when the stream ends.
    pub(crate) fn subscribe(
        self: &Arc<Self>,
        spec: Spec,
        slot: OwnedSemaphorePermit,
        on_end: impl FnOnce(Option<&ApiError>) + Send + 'static,
    ) -> Result<FeedEvents, ApiError> {
        let hub = self.hub(&spec)?;
        let (tx, rx) = mpsc::channel(1);
        let (end_tx, end_rx) = oneshot::channel();
        let client = Client {
            feeds: Arc::clone(self),
            hub,
            tables: spec.tables,
            prefix: spec.prefix,
            visible: spec.visible,
            tx,
        };
        // The first own stream opens here, so a bad token or snapshot fails the call rather
        // than the stream.
        let first = match spec.start {
            Start::Now => None,
            start => Some((open(&client.tables, client.visible, &start)?, start)),
        };
        tokio::spawn(async move {
            let outcome = match client.run(first).await {
                // The client went away: not a failure.
                Err(_) if client.tx.is_closed() => Ok(()),
                r => r,
            };
            on_end(outcome.as_ref().err());
            drop(slot);
            if let Err(e) = outcome {
                let _ = end_tx.send(e);
            }
        });
        Ok(FeedEvents {
            events: rx,
            end: Some(end_rx),
        })
    }
}

/// A feed of exactly `tables` (no prefix: the client filters), from `start`.
fn open(tables: &[Table], visible: bool, start: &Start) -> Result<pigeonhole::Feed, ApiError> {
    let (first, rest) = tables
        .split_first()
        .ok_or_else(|| ApiError::invalid("a subscription names at least one table"))?;
    let mut b = first.feed();
    for t in rest {
        b = b.table(t);
    }
    if visible {
        b = b.visible();
    }
    b = match start {
        Start::Now => b,
        Start::Snapshot(s) => b.from_snapshot(s),
        Start::Oldest => b.from_oldest(),
        Start::Token(t) => b.resume(t),
    };
    Ok(b.open()?)
}

/// A feed stream's next item, as a future.
async fn next(stream: &mut FeedStream) -> Option<pigeonhole::Result<Commit>> {
    std::future::poll_fn(|cx| Pin::new(&mut *stream).poll_next(cx)).await
}

/// Where a client reads.
enum Source {
    /// The hub's ring, from this index.
    Ring(u64),
    /// Its own stream, holding a `--max-catch-up` slot. Boxed: a client changes source
    /// rarely.
    Own(Box<FeedStream>, OwnedSemaphorePermit),
}

struct Client {
    feeds: Arc<Feeds>,
    hub: Arc<Hub>,
    tables: Vec<Table>,
    prefix: Vec<u8>,
    visible: bool,
    tx: mpsc::Sender<proto::FeedEvent>,
}

/// A client's position: the last commit it covered and the token right after it.
struct Position {
    seqno: Option<u64>,
    token: Vec<u8>,
    /// The token of the last event sent, to tell whether a progress event would say more.
    sent: Vec<u8>,
    last_event: Instant,
}

impl Client {
    async fn run(&self, first: Option<(pigeonhole::Feed, Start)>) -> Result<(), ApiError> {
        let mut pos;
        let mut source = match first {
            Some((feed, start)) => {
                // A snapshot start is a known position; a token's or the oldest isn't, until
                // the stream delivers a commit.
                let known = match &start {
                    Start::Snapshot(s) => Some(s.seqno()),
                    _ => None,
                };
                // Opened at subscribe so a bad start fails the call. Waiting for a catch-up
                // slot, it would hold an engine reader (and its shard wakers): drop it, and
                // open it again once there's a slot.
                let (feed, slot) = match Arc::clone(&self.feeds.catch_up).try_acquire_owned() {
                    Ok(slot) => (feed, slot),
                    Err(_) => {
                        drop(feed);
                        let slot = self.catch_up_slot().await?;
                        (open(&self.tables, self.visible, &start)?, slot)
                    }
                };
                let stream = FeedStream::new(feed);
                let token = stream.token().as_bytes().to_vec();
                pos = Position {
                    seqno: known,
                    sent: token.clone(),
                    token,
                    last_event: Instant::now(),
                };
                Source::Own(Box::new(stream), slot)
            }
            None => {
                let r = self.hub.ring();
                let last = r.entries.back();
                pos = Position {
                    seqno: Some(last.map_or(r.start_seqno, |e| e.commit.seqno())),
                    token: last.map_or_else(|| r.start_token.clone(), |e| e.token.clone()),
                    sent: Vec::new(),
                    last_event: Instant::now(),
                };
                pos.sent = pos.token.clone();
                Source::Ring(r.head())
            }
        };
        let mut head = self.hub.head.subscribe();
        loop {
            source = match source {
                Source::Ring(at) => self.read_ring(at, &mut pos, &mut head).await?,
                Source::Own(stream, slot) => self.read_own(stream, slot, &mut pos).await?,
            };
        }
    }

    async fn catch_up_slot(&self) -> Result<OwnedSemaphorePermit, ApiError> {
        let sem = Arc::clone(&self.feeds.catch_up);
        if let Ok(p) = Arc::clone(&sem).try_acquire_owned() {
            return Ok(p);
        }
        self.feeds
            .stats
            .catch_up_waits
            .fetch_add(1, Ordering::Relaxed);
        sem.acquire_owned()
            .await
            .map_err(|_| ApiError::internal("the catch-up slots closed"))
    }

    /// Reads the ring from `at` until the client must leave it: fallen off the tail, or the
    /// hub failed. Then its own stream, from its own last token.
    async fn read_ring(
        &self,
        mut at: u64,
        pos: &mut Position,
        head: &mut watch::Receiver<u64>,
    ) -> Result<Source, ApiError> {
        loop {
            let (batch, behind, failed) = {
                let r = self.hub.ring();
                if at < r.base {
                    (Vec::new(), true, r.failed)
                } else {
                    let from = (at - r.base) as usize;
                    let batch: Vec<Arc<Entry>> =
                        r.entries.iter().skip(from).take(64).cloned().collect();
                    (batch, false, r.failed)
                }
            };
            if behind || (failed && batch.is_empty()) {
                if behind {
                    self.feeds.stats.fell_behind.fetch_add(1, Ordering::Relaxed);
                }
                let feed = open(
                    &self.tables,
                    self.visible,
                    &Start::Token(FeedToken::from_bytes(pos.token.clone())),
                )?;
                return Ok(Source::Own(
                    Box::new(FeedStream::new(feed)),
                    self.catch_up_slot().await?,
                ));
            }
            if batch.is_empty() {
                let quiet =
                    tokio::time::sleep_until((pos.last_event + self.feeds.limits.quiet).into());
                tokio::select! {
                    changed = head.changed() => {
                        if changed.is_err() {
                            return Err(ApiError::internal("the feed's hub ended"));
                        }
                    }
                    () = quiet => self.progress(pos).await?,
                }
                continue;
            }
            for e in batch {
                at += 1;
                self.cover(&e.commit, &e.token, pos).await?;
            }
        }
    }

    /// Reads its own stream until it can rejoin the ring.
    async fn read_own(
        &self,
        mut stream: Box<FeedStream>,
        slot: OwnedSemaphorePermit,
        pos: &mut Position,
    ) -> Result<Source, ApiError> {
        loop {
            if let Some(seqno) = pos.seqno {
                let r = self.hub.ring();
                if !r.failed
                    && let Some(at) = r.rejoin_at(seqno)
                {
                    drop(r);
                    drop(slot);
                    self.feeds.stats.rejoined.fetch_add(1, Ordering::Relaxed);
                    return Ok(Source::Ring(at));
                }
            }
            let quiet = tokio::time::sleep_until((pos.last_event + self.feeds.limits.quiet).into());
            tokio::select! {
                item = next(&mut stream) => match item {
                    Some(Ok(commit)) => {
                        let token = stream.token().as_bytes().to_vec();
                        self.cover(&commit, &token, pos).await?;
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => return Err(ApiError::internal("the feed ended without an error")),
                },
                () = quiet => {
                    // The stream may have read past commits of other tables: its position
                    // moved without a commit.
                    pos.token = stream.token().as_bytes().to_vec();
                    self.progress(pos).await?;
                }
            }
        }
    }

    /// Covers one commit of the hub's spec: sends it if a row matches the prefix, else passes
    /// over it (a later progress event carries the position).
    async fn cover(
        &self,
        commit: &Commit,
        token: &[u8],
        pos: &mut Position,
    ) -> Result<(), ApiError> {
        pos.seqno = Some(commit.seqno());
        pos.token = token.to_vec();
        if let Some(c) = filtered(commit, &self.prefix) {
            self.send(
                proto::FeedEvent {
                    token: token.to_vec().into(),
                    event: Some(proto::feed_event::Event::Commit(c)),
                },
                pos,
            )
            .await?;
        }
        Ok(())
    }

    /// A progress event, if the position moved since the last event and the stream was quiet.
    async fn progress(&self, pos: &mut Position) -> Result<(), ApiError> {
        if pos.token == pos.sent {
            // Nothing to say: look again after another quiet interval.
            pos.last_event = Instant::now();
            return Ok(());
        }
        self.send(
            proto::FeedEvent {
                token: pos.token.clone().into(),
                event: Some(proto::feed_event::Event::Progress(proto::Progress {})),
            },
            pos,
        )
        .await
    }

    async fn send(&self, event: proto::FeedEvent, pos: &mut Position) -> Result<(), ApiError> {
        let token = event.token.to_vec();
        match tokio::time::timeout(self.feeds.limits.stream_idle, self.tx.send(event)).await {
            Ok(Ok(())) => {
                pos.sent = token;
                pos.last_event = Instant::now();
                Ok(())
            }
            // The client went away: end quietly.
            Ok(Err(_)) => Err(ApiError::internal("the client went away")),
            Err(_) => Err(ApiError::deadline(format!(
                "the client took no feed event for --stream-idle-ms ({} ms); resume from the \
                 last event's token",
                self.feeds.limits.stream_idle.as_millis()
            ))),
        }
    }
}

/// `commit` as the client sees it: its changes to rows under `prefix`, or `None` when it has
/// none (the engine's prefix feed passes such a commit over).
fn filtered(commit: &Commit, prefix: &[u8]) -> Option<proto::FeedCommit> {
    let changes: Vec<proto::Change> = commit
        .changes()
        .iter()
        .filter(|ch| ch.row().starts_with(prefix))
        .map(|ch| proto::Change {
            table: ch.table().to_owned(),
            family: ch.family().to_owned(),
            kind: change_kind(ch.kind()) as i32,
            row: ch.row().to_vec().into(),
            qualifier: ch.qualifier().to_vec().into(),
            timestamp: ch.timestamp(),
            value: ch.value().map(convert::value_out),
        })
        .collect();
    (!changes.is_empty()).then(|| proto::FeedCommit {
        seqno: commit.seqno(),
        timestamp: commit.timestamp(),
        changes,
    })
}

fn change_kind(k: pigeonhole::ChangeKind) -> proto::ChangeKind {
    use pigeonhole::ChangeKind as K;
    match k {
        K::Put => proto::ChangeKind::Put,
        K::Merge => proto::ChangeKind::Merge,
        K::DeleteCell => proto::ChangeKind::DeleteCell,
        K::DeleteColumn => proto::ChangeKind::DeleteColumn,
        K::DeleteFamily => proto::ChangeKind::DeleteFamily,
        _ => proto::ChangeKind::Unspecified,
    }
}

/// A client's events, then its error if it failed.
#[derive(Debug)]
pub(crate) struct FeedEvents {
    events: mpsc::Receiver<proto::FeedEvent>,
    end: Option<oneshot::Receiver<ApiError>>,
}

impl Stream for FeedEvents {
    type Item = Result<proto::FeedEvent, ApiError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.events.poll_recv(cx) {
            Poll::Ready(Some(e)) => return Poll::Ready(Some(Ok(e))),
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => {}
        }
        let Some(end) = self.end.as_mut() else {
            return Poll::Ready(None);
        };
        let out = match Pin::new(end).poll(cx) {
            Poll::Ready(Ok(e)) => Some(Err(e)),
            Poll::Ready(Err(_)) => None,
            Poll::Pending => return Poll::Pending,
        };
        self.end = None;
        Poll::Ready(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole::{Family, FeedRetention, Options, Pigeonhole};

    /// Real commits of seqnos the test can't choose, read off a feed.
    fn commits(n: usize) -> (Vec<Commit>, Pigeonhole, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("phdb-feed-ring-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = Pigeonhole::open(
            dir.join("r.phdb"),
            Options::default()
                .create_if_missing(true)
                .shards(1)
                .change_feed(FeedRetention::bytes(1 << 20)),
        )
        .unwrap();
        let t = db
            .table("t")
            .unwrap()
            .family("f", Family::default())
            .create()
            .unwrap();
        let mut feed = t.feed().open().unwrap();
        let mut out = Vec::new();
        for i in 0..n {
            t.mutate(format!("r{i}").as_bytes())
                .put("f", b"q", b"v")
                .commit()
                .unwrap();
            out.push(feed.next(Duration::from_secs(5)).unwrap().unwrap().clone());
        }
        drop((feed, t));
        (out, db, dir)
    }

    fn ring(start: u64, commits: &[Commit], evicted: usize) -> Ring {
        let mut r = Ring {
            start_seqno: start,
            ..Ring::default()
        };
        let mut prev = start;
        for c in commits {
            r.entries.push_back(Arc::new(Entry {
                commit: c.clone(),
                token: Vec::new(),
                prev,
                bytes: 0,
            }));
            prev = c.seqno();
        }
        for _ in 0..evicted {
            r.entries.pop_front();
            r.base += 1;
        }
        r
    }

    #[test]
    fn a_client_rejoins_only_where_the_ring_covers_it_and_has_passed_it() {
        let (c, db, dir) = commits(3);
        let (a, b, z) = (c[0].seqno(), c[1].seqno(), c[2].seqno());
        let start = a - 1;
        let r = ring(start, &c[..2], 0);
        assert_eq!(
            r.rejoin_at(start),
            Some(0),
            "at the start: from the first entry"
        );
        assert_eq!(r.rejoin_at(a), Some(1));
        assert_eq!(r.rejoin_at(b), Some(2), "at the last entry: the head");
        // A client whose own stream is ahead of the hub (it has z, which the hub hasn't
        // added yet) must not rejoin: z would come again.
        assert_eq!(r.rejoin_at(z), None);
        // Nor one behind what the ring still covers: the evicted commits would be missed.
        let r = ring(start, &c[..3], 2);
        assert_eq!(r.rejoin_at(a), None);
        assert_eq!(r.rejoin_at(b), Some(2));
        assert_eq!(r.rejoin_at(z), Some(3));
        // A hub with no commit yet covers from its start.
        let r = ring(z, &[], 0);
        assert_eq!(r.rejoin_at(z), Some(0));
        assert_eq!(r.rejoin_at(b), None);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
