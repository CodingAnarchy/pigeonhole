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
    /// Bytes of commits the rings hold in all (`--feed-ring-bytes`), tokens included (about
    /// 45 + 16 × shards bytes each): memory outside R3's in-flight budget. Each hub's ring is
    /// capped at this over the live hubs (perf287's K2 on #1518).
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
    /// The tables' feed ids, so a table dropped and created again under its name is another
    /// key (feed1248's R2, simp724's F4 on #1518).
    tables: Vec<u64>,
    visible: bool,
}

/// One commit in a hub's ring.
struct Entry {
    commit: Commit,
    /// The hub feed's token right after this commit.
    token: Vec<u8>,
    /// The hub feed's position just before it (`Feed::position`): every commit of the spec
    /// at or below it came before this one.
    prev: u64,
    bytes: usize,
}

#[derive(Default)]
struct Ring {
    entries: VecDeque<Arc<Entry>>,
    /// The index of `entries[0]` in the hub's whole sequence.
    base: u64,
    bytes: usize,
    /// The hub feed's position when it opened: with no entry yet, the ring holds every
    /// commit of the spec above it.
    start_seqno: u64,
    /// The hub feed's latest position, after its last commit or its last poll that found
    /// none (so it follows other tables' writes too), and the token taken with it: one pair,
    /// from one poll (simp724's F1 on #1518).
    passed: u64,
    passed_token: Vec<u8>,
    /// The shared feed ended (an error): clients move to their own streams.
    failed: bool,
}

impl Ring {
    fn head(&self) -> u64 {
        self.base + self.entries.len() as u64
    }

    /// Where a client at position `seqno` rejoins: the index of the first entry after it.
    /// The client has covered every commit of the spec up to `seqno` (its own stream's
    /// `position`), and the ring holds every one after the position before its first entry.
    /// So it rejoins only if that position is at most `seqno` (nothing between them was
    /// evicted) and the hub's feed has passed `seqno` (nothing it will add is one the client
    /// has): feed1248's (2) on ticket 5.
    fn rejoin_at(&self, seqno: u64) -> Option<u64> {
        let covered_from = self.entries.front().map_or(self.start_seqno, |e| e.prev);
        if covered_from > seqno || self.passed < seqno {
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
    /// Live hubs, shared: each ring's cap is the total over it.
    live: Arc<AtomicU64>,
}

impl Hub {
    fn ring(&self) -> MutexGuard<'_, Ring> {
        self.ring.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn push(&self, commit: Commit, token: Vec<u8>, position: u64, total: usize) {
        let limit = total / self.live.load(Ordering::Relaxed).max(1) as usize;
        let bytes = entry_bytes(&commit, &token);
        let mut r = self.ring();
        let prev = r.passed;
        r.passed = position.max(commit.seqno());
        r.passed_token = token.clone();
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

    /// The hub's feed found nothing at `position`: the ring holds everything up to it.
    fn note_passed(&self, position: u64, token: impl FnOnce() -> Vec<u8>) {
        let mut r = self.ring();
        if position > r.passed {
            r.passed = position;
            r.passed_token = token();
        }
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
        self.live.fetch_sub(1, Ordering::Relaxed);
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
    live: Arc<AtomicU64>,
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
            live: Arc::default(),
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

    /// The hub for `tables`, opened if none is live. A failed hub is replaced (feed1248's
    /// R1, simp724's F2 on #1518): its clients are on their own streams, and rejoin the new
    /// one; a new subscriber must not start from the failed one's last position.
    fn hub(&self, tables: &[Table], visible: bool) -> Result<Arc<Hub>, ApiError> {
        let key = HubKey {
            tables: tables.iter().map(Table::feed_id).collect(),
            visible,
        };
        let mut hubs = self.hubs.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(hub) = hubs.get(&key).and_then(Weak::upgrade)
            && !hub.ring().failed
        {
            return Ok(hub);
        }
        hubs.retain(|k, w| w.strong_count() > 0 && *k != key);
        if hubs.len() >= self.limits.max_hubs.max(1) {
            return Err(ApiError::overloaded(
                "the server is at its feed hub limit (--max-feed-hubs): too many distinct table \
                 sets are subscribed",
            ));
        }
        // From a snapshot taken now, at the visibility it delivers at, so the ring knows the
        // position it starts from exactly: a client catching up can then rejoin it before
        // the hub has a commit of its own.
        let snapshot = if visible {
            self.db.snapshot()?
        } else {
            self.db.durable_snapshot()?
        };
        let mut stream = FeedStream::new(open(tables, visible, &Start::Snapshot(snapshot))?);
        let start_seqno = stream.position().seqno();
        let start_token = stream.token().as_bytes().to_vec();
        let (head, _) = watch::channel(0);
        let limit = self.limits.ring_bytes;
        let stats = Arc::clone(&self.stats);
        let live = Arc::clone(&self.live);
        live.fetch_add(1, Ordering::Relaxed);
        let hub = Arc::new_cyclic(|weak: &Weak<Hub>| {
            let weak = weak.clone();
            let task = tokio::spawn(async move {
                loop {
                    // A poll that finds nothing records how far the feed got, so the ring's
                    // `passed` follows other tables' writes (the engine wakes the hub on each
                    // frontier move).
                    let item = std::future::poll_fn(|cx| {
                        let polled = Pin::new(&mut stream).poll_next(cx);
                        if polled.is_pending()
                            && let Some(hub) = weak.upgrade()
                        {
                            let at = stream.position().seqno();
                            hub.note_passed(at, || stream.token().as_bytes().to_vec());
                        }
                        polled
                    })
                    .await;
                    let Some(hub) = weak.upgrade() else { return };
                    match item {
                        Some(Ok(commit)) => {
                            let at = stream.position().seqno();
                            hub.push(commit, stream.token().as_bytes().to_vec(), at, limit);
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
                    start_seqno,
                    passed: start_seqno,
                    passed_token: start_token,
                    ..Ring::default()
                }),
                head,
                task,
                stats,
                live,
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
        let hub = self.hub(&spec.tables, spec.visible)?;
        // A Now start's place on the ring, then the database's seqno (simp724's F5 on
        // #1518), in that order: everything at or past that head is newer than its `passed`,
        // and whatever of it is at or below `after` predates the subscribe and is passed over.
        // Taken here, not when the client's task first runs, so a commit pushed meanwhile
        // (the caller's own write, say) is still the client's.
        let ring_start = matches!(spec.start, Start::Now).then(|| {
            let r = hub.ring();
            (r.head(), r.passed, r.passed_token.clone())
        });
        let after = match spec.start {
            Start::Now if spec.visible => self.db.snapshot()?.seqno(),
            Start::Now => self.db.durable_snapshot()?.seqno(),
            _ => 0,
        };
        let (tx, rx) = mpsc::channel(1);
        let (end_tx, end_rx) = oneshot::channel();
        let client = Client {
            feeds: Arc::clone(self),
            hub: Mutex::new(hub),
            after,
            tables: spec.tables,
            prefix: spec.prefix,
            visible: spec.visible,
            tx,
        };
        // The first own stream opens here, so a bad token or snapshot fails the call rather
        // than the stream. Its start is kept as the stream's token, never the `Snapshot`: a
        // client waiting for a catch-up slot holds bytes, not a view that pins memtables
        // (perf287's K1 on #1518).
        let begin = match (spec.start, ring_start) {
            (Start::Now, Some((at, seqno, token))) => Begin::Ring { at, seqno, token },
            (start, _) => {
                let feed = open(&client.tables, client.visible, &start)?;
                drop(start);
                let token = feed.token();
                Begin::Own(Box::new(feed), token)
            }
        };
        tokio::spawn(async move {
            let outcome = match client.run(begin).await {
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

/// Where a client starts, as `subscribe` found it.
enum Begin {
    /// On the ring at `at`, its position `seqno` and the token with it.
    Ring { at: u64, seqno: u64, token: Vec<u8> },
    /// On its own feed (opened to check the start), and that start as a token. Boxed: the
    /// feed is large, and a client begins once.
    Own(Box<pigeonhole::Feed>, FeedToken),
}

/// How often a client on its own stream looks at its hub's ring for a place to rejoin.
const REJOIN_TICK: Duration = Duration::from_millis(250);

/// How often a client whose hub failed tries the key's current hub.
const HUB_RETRY: Duration = Duration::from_secs(1);

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
    /// Its hub: replaced by the key's current one when it fails (F2).
    hub: Mutex<Arc<Hub>>,
    /// For a `Now` start, the database's seqno when it subscribed: commits at or below it
    /// are passed over, as an in-process feed opened then would never deliver them, even
    /// when the hub hadn't read them yet.
    after: u64,
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
    fn hub(&self) -> Arc<Hub> {
        Arc::clone(&self.hub.lock().unwrap_or_else(PoisonError::into_inner))
    }

    async fn run(&self, begin: Begin) -> Result<(), ApiError> {
        let mut pos;
        let mut source = match begin {
            Begin::Own(feed, start) => {
                // Opened at subscribe so a bad start fails the call. Waiting for a catch-up
                // slot, it would hold an engine reader (and its shard wakers): drop it, and
                // open it again from its start token once there's a slot.
                let (feed, slot) = match Arc::clone(&self.feeds.catch_up).try_acquire_owned() {
                    Ok(slot) => (*feed, slot),
                    Err(_) => {
                        drop(feed);
                        let slot = self.catch_up_slot().await?;
                        (
                            open(&self.tables, self.visible, &Start::Token(start))?,
                            slot,
                        )
                    }
                };
                let stream = FeedStream::new(feed);
                let token = stream.token().as_bytes().to_vec();
                // A position from the start: a token or the oldest start can rejoin as soon
                // as the ring covers it, without waiting for a commit (simp724's F1).
                pos = Position {
                    seqno: Some(stream.position().seqno()),
                    sent: token.clone(),
                    token,
                    last_event: Instant::now(),
                };
                Source::Own(Box::new(stream), slot)
            }
            Begin::Ring { at, seqno, token } => {
                pos = Position {
                    seqno: Some(seqno),
                    sent: token.clone(),
                    token,
                    last_event: Instant::now(),
                };
                Source::Ring(at)
            }
        };
        loop {
            source = match source {
                Source::Ring(at) => self.read_ring(at, &mut pos).await?,
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
        tokio::select! {
            // A client that leaves while queued gives up its place.
            () = self.tx.closed() => Err(ApiError::internal("the client went away")),
            slot = sem.acquire_owned() => {
                slot.map_err(|_| ApiError::internal("the catch-up slots closed"))
            }
        }
    }

    /// Reads the ring from `at` until the client must leave it: fallen off the tail, or the
    /// hub failed. Then its own stream, from its own last token.
    async fn read_ring(&self, mut at: u64, pos: &mut Position) -> Result<Source, ApiError> {
        let hub = self.hub();
        // Subscribed before the first look at the ring, so no commit after it is missed.
        let mut head = hub.head.subscribe();
        loop {
            let (batch, behind, failed) = {
                let r = hub.ring();
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
                // The slot first, then the feed: a waiting client holds no engine reader
                // (simp724's F3, feed1248's S2 on #1518). The token is its whole position.
                let slot = self.catch_up_slot().await?;
                let feed = open(
                    &self.tables,
                    self.visible,
                    &Start::Token(FeedToken::from_bytes(pos.token.clone())),
                )?;
                return Ok(Source::Own(Box::new(FeedStream::new(feed)), slot));
            }
            if batch.is_empty() {
                let quiet =
                    tokio::time::sleep_until((pos.last_event + self.feeds.limits.quiet).into());
                tokio::select! {
                    // A client that went away ends here, even one whose prefix never
                    // matches (it would otherwise hold its stream slot until a send failed).
                    () = self.tx.closed() => return Err(ApiError::internal("the client went away")),
                    changed = head.changed() => {
                        if changed.is_err() {
                            return Err(ApiError::internal("the feed's hub ended"));
                        }
                    }
                    () = quiet => {
                        // At the head: the hub's feed may have got further than its last
                        // commit (other tables' writes), and its token with it.
                        {
                            let r = hub.ring();
                            if at == r.head() && pos.seqno.is_none_or(|s| r.passed > s) {
                                pos.seqno = Some(r.passed);
                                pos.token = r.passed_token.clone();
                            }
                        }
                        self.progress(pos).await?;
                    }
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
        let mut hub_retry: Option<Instant> = None;
        loop {
            // A failed hub: take the key's current one, opening a new one if none (F2), at
            // most once a second so a hub that keeps failing isn't reopened in a loop.
            let mut hub = self.hub();
            if hub.ring().failed && hub_retry.is_none_or(|t| t.elapsed() >= HUB_RETRY) {
                hub_retry = Some(Instant::now());
                if let Ok(new) = self.feeds.hub(&self.tables, self.visible) {
                    *self.hub.lock().unwrap_or_else(PoisonError::into_inner) = Arc::clone(&new);
                    hub = new;
                }
            }
            if let Some(seqno) = pos.seqno {
                let r = hub.ring();
                if !r.failed
                    && let Some(at) = r.rejoin_at(seqno)
                {
                    drop(r);
                    drop(slot);
                    self.feeds.stats.rejoined.fetch_add(1, Ordering::Relaxed);
                    return Ok(Source::Ring(at));
                }
            }
            // Woken by a commit, or every `REJOIN_TICK` to look at the ring again: a caught-up
            // client gives its catch-up slot back as soon as it can rejoin.
            let tick = tokio::time::sleep(REJOIN_TICK.min(self.feeds.limits.quiet));
            tokio::select! {
                () = self.tx.closed() => return Err(ApiError::internal("the client went away")),
                item = next(&mut stream) => match item {
                    Some(Ok(commit)) => {
                        let token = stream.token().as_bytes().to_vec();
                        self.cover(&commit, &token, pos).await?;
                        pos.seqno = Some(stream.position().seqno());
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => return Err(ApiError::internal("the feed ended without an error")),
                },
                () = tick => {
                    // The stream may have read past commits of other tables: its position
                    // moved without a commit, and its token with it.
                    let at = stream.position().seqno();
                    if pos.seqno.is_none_or(|s| at > s) {
                        pos.seqno = Some(at);
                        pos.token = stream.token().as_bytes().to_vec();
                    }
                    if pos.last_event.elapsed() >= self.feeds.limits.quiet {
                        self.progress(pos).await?;
                    }
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
        if commit.seqno() <= self.after {
            return Ok(());
        }
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
    fn commits(name: &str, n: usize) -> (Vec<Commit>, Pigeonhole, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("phdb-feed-{name}-{}", std::process::id()));
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
            passed: commits.last().map_or(start, Commit::seqno),
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

    /// Commit seqnos from `events` until none comes for `quiet` (durable feeds: a commit
    /// arrives once synced, which a slow disk or VM can take a while).
    async fn seqnos(events: &mut FeedEvents, quiet: Duration) -> Vec<u64> {
        let mut out = Vec::new();
        while let Ok(Some(e)) = tokio::time::timeout(
            quiet,
            std::future::poll_fn(|cx| Pin::new(&mut *events).poll_next(cx)),
        )
        .await
        {
            if let Some(proto::feed_event::Event::Commit(c)) = e.unwrap().event {
                out.push(c.seqno);
            }
        }
        out
    }

    #[test]
    fn a_failed_hub_is_replaced_and_its_clients_rejoin_the_new_one() {
        // feed1248's R1 and simp724's F2 on #1518.
        let (_, db, dir) = commits("fail", 0);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let t = db.table("t").unwrap().open().unwrap();
        let feeds = Feeds::new(
            db.clone(),
            FeedLimits {
                ring_bytes: 1 << 20,
                max_hubs: 4,
                max_catch_up: 1,
                quiet: Duration::from_secs(10),
                stream_idle: Duration::from_secs(5),
            },
        );
        let slots = Arc::new(Semaphore::new(8));
        let spec = || Spec {
            tables: vec![t.clone()],
            prefix: Vec::new(),
            visible: false,
            start: Start::Now,
        };
        rt.block_on(async {
            let slot = || Arc::clone(&slots).try_acquire_owned().unwrap();
            let mut a = feeds.subscribe(spec(), slot(), |_| {}).unwrap();
            let first = feeds.hub(std::slice::from_ref(&t), false).unwrap();
            let w = |row: &str| {
                t.mutate(row.as_bytes())
                    .put("f", b"q", b"v")
                    .commit()
                    .unwrap()
                    .seqno
            };
            let mut want = vec![w("a1")];
            assert_eq!(seqnos(&mut a, Duration::from_secs(1)).await, want);
            // The shared feed fails (an expiry, say): its engine task ends.
            first.task.abort();
            first.fail();
            want.push(w("a2"));
            // A new subscriber gets a fresh hub, and starts now, not from the failed one.
            let mut b = feeds.subscribe(spec(), slot(), |_| {}).unwrap();
            let second = feeds.hub(std::slice::from_ref(&t), false).unwrap();
            assert!(!Arc::ptr_eq(&first, &second));
            let after = vec![w("b1"), w("b2")];
            want.extend(&after);
            assert_eq!(seqnos(&mut b, Duration::from_secs(1)).await, after);
            // The old client went on from its own token, once each, and rejoined the new hub,
            // giving its catch-up slot back (one slot: a second laggard would wait for it).
            let got = seqnos(&mut a, Duration::from_secs(2)).await;
            assert_eq!(got, want[1..]);
            assert_eq!(feeds.stats.hub_failures.load(Ordering::Relaxed), 1);
            assert!(feeds.stats.rejoined.load(Ordering::Relaxed) >= 1);
            assert_eq!(feeds.gauges().catching_up, 0);
            drop((a, b, first, second));
        });
        drop(t);
        drop(rt);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_now_start_keeps_a_commit_pushed_before_its_task_first_runs() {
        // simp724's F5 on #1518, made deterministic: on a current-thread runtime the client's
        // task can't run until the test awaits, so the ring gets the commit first. The start
        // taken at subscribe still covers it; one read when the task first ran would not.
        let (_, db, dir) = commits("now", 0);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let t = db.table("t").unwrap().open().unwrap();
        let feeds = Feeds::new(
            db.clone(),
            FeedLimits {
                ring_bytes: 1 << 20,
                max_hubs: 4,
                max_catch_up: 1,
                quiet: Duration::from_secs(10),
                stream_idle: Duration::from_secs(5),
            },
        );
        let slots = Arc::new(Semaphore::new(1));
        rt.block_on(async {
            let hub = feeds.hub(std::slice::from_ref(&t), false).unwrap();
            // The hub's own task stops; the test pushes for it.
            hub.task.abort();
            let mut direct = t.feed().open().unwrap();
            let mut c = feeds
                .subscribe(
                    Spec {
                        tables: vec![t.clone()],
                        prefix: Vec::new(),
                        visible: false,
                        start: Start::Now,
                    },
                    Arc::clone(&slots).try_acquire_owned().unwrap(),
                    |_| {},
                )
                .unwrap();
            let n = t.mutate(b"x").put("f", b"q", b"v").commit().unwrap().seqno;
            let commit = direct
                .next(Duration::from_secs(5))
                .unwrap()
                .unwrap()
                .clone();
            let token = direct.token().as_bytes().to_vec();
            hub.push(commit, token, direct.position().seqno(), 1 << 20);
            assert_eq!(seqnos(&mut c, Duration::from_secs(1)).await, [n]);
            drop((c, hub));
        });
        drop(t);
        drop(rt);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_client_rejoins_only_where_the_ring_covers_it_and_has_passed_it() {
        let (c, db, dir) = commits("ring", 3);
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
