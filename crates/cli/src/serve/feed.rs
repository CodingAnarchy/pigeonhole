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
//! - **A client** is a task with a buffered channel to its connection ([`CLIENT_EVENTS`],
//!   [`CLIENT_BYTES`]). On the ring it takes the entries after its position (each an `Arc`,
//!   so a send is a refcount); with a prefix, it filters and encodes on its own task. A commit with no row under the prefix is passed
//!   over, as the engine's prefix feed does, and later covered by a progress event.
//! - **Encoded once:** each entry is encoded by the hub for the clients with no prefix, and
//!   those clients queue the entry itself. gRPC writes its bytes as they are (`Out::encode`);
//!   the HTTP feed still renders JSON per subscriber (`Out::into_proto`).
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
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_core::Stream;
use pigeonhole::nonblocking::FeedStream;
use pigeonhole::{Commit, FeedToken, Snapshot, Table};
use prost::bytes::Bytes;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};

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
    /// Events a client's connection may hold untaken ([`CLIENT_EVENTS`]), and their encoded
    /// bytes ([`CLIENT_BYTES`]): the per-client buffer.
    pub client_events: usize,
    pub client_bytes: usize,
}

/// Events buffered per client. More than one lets the gRPC encoder pack every event that is
/// ready into one HTTP/2 frame: with one, each event cost every subscriber its own stream
/// poll, its own trips through the connection's shared lock and its own frame (#1397's
/// acceptance, ticket 5).
pub(crate) const CLIENT_EVENTS: usize = 64;

/// Encoded bytes buffered per client, at most: the bound on a client's buffer however large
/// its commits. An event larger than this is still sent, alone.
pub(crate) const CLIENT_BYTES: usize = 256 << 10;

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
pub(crate) struct Entry {
    commit: Commit,
    /// The hub feed's token right after this commit.
    token: Bytes,
    /// The hub feed's position just before it (`Feed::position`): every commit of the spec
    /// at or below it came before this one.
    prev: u64,
    /// Its share of `--feed-ring-bytes`: the commit, its token and `encoded`.
    bytes: usize,
    /// The commit as a client with no prefix gets it, encoded once for every such client on
    /// the ring: `None` if it has no change to send. Encoded by the hub, before the entry is
    /// pushed, so no client waits on another's encode (perf287's P2 on #1541).
    encoded: Option<Bytes>,
}

impl Entry {
    /// An entry for `commit`, encoded for clients with no prefix and sized with that
    /// encoding (simp724's R1, perf287's P1 on #1541); `prev` is set when it's pushed.
    fn new(commit: Commit, token: Bytes) -> Self {
        let encoded = filtered(&commit, b"")
            .map(|c| prost::Message::encode_to_vec(&commit_event(token.clone(), c)).into());
        let bytes = entry_bytes(&commit, &token) + encoded.as_ref().map_or(0, Bytes::len);
        Entry {
            commit,
            token,
            prev: 0,
            bytes,
            encoded,
        }
    }

    /// The `FeedEvent` a client with no prefix sends for this commit, encoded.
    fn encoded(&self) -> Option<&Bytes> {
        self.encoded.as_ref()
    }
}

/// A commit event: `c` with the token right after it.
fn commit_event(token: Bytes, c: proto::FeedCommit) -> proto::FeedEvent {
    proto::FeedEvent {
        token,
        event: Some(proto::feed_event::Event::Commit(c)),
    }
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
    passed_token: Bytes,
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

/// A client's state for `phdb serve`'s SIGUSR1 dump (perf287's wedged streams on #1397):
/// where it is in its hub's ring, what it has sent into its buffer and what the transport has
/// taken from it, and when. Relaxed atomics: a snapshot, not a protocol.
#[derive(Debug, Default)]
pub(crate) struct Probe {
    id: u64,
    tables: String,
    /// 0 starting, 1 waiting at the ring's head, 2 sending from the ring, 3 on its own feed,
    /// 4 waiting for a catch-up slot, 5 waiting for room in its buffer.
    state: AtomicU8,
    ring_at: AtomicU64,
    sent: AtomicU64,
    taken: AtomicU64,
    last_send_us: AtomicU64,
    last_take_us: AtomicU64,
    hub: Mutex<Weak<Hub>>,
}

impl Probe {
    fn set(&self, state: u8) {
        self.state.store(state, Ordering::Relaxed);
    }
}

/// Microseconds since the process's first feed call, for the dump's "how long ago".
fn now_us() -> u64 {
    static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_micros() as u64
}

struct Hub {
    /// When the hub last pushed a commit (`now_us`), for the dump.
    last_push_us: AtomicU64,
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

    fn push(&self, commit: Commit, token: Bytes, position: u64, total: usize) {
        let limit = total / self.live.load(Ordering::Relaxed).max(1) as usize;
        // Encoded here, on the hub's task and outside the ring's lock.
        let seqno = commit.seqno();
        let mut entry = Entry::new(commit, token.clone());
        let bytes = entry.bytes;
        let mut r = self.ring();
        entry.prev = r.passed;
        r.passed = position.max(seqno);
        r.passed_token = token;
        r.entries.push_back(Arc::new(entry));
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
        self.last_push_us.store(now_us(), Ordering::Relaxed);
        self.head.send_replace(head);
    }

    /// The hub's feed found nothing at `position`: the ring holds everything up to it.
    fn note_passed(&self, position: u64, token: impl FnOnce() -> Bytes) {
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
    /// Where hubs and clients run: the server's feed runtime, once `run_on` set it.
    runtime: std::sync::OnceLock<tokio::runtime::Handle>,
    db: pigeonhole::Pigeonhole,
    hubs: Mutex<HashMap<HubKey, Weak<Hub>>>,
    catch_up: Arc<Semaphore>,
    limits: FeedLimits,
    pub(crate) stats: Arc<FeedStats>,
    live: Arc<AtomicU64>,
    /// Every client's probe, for the dump; dead ones are pruned as new ones join.
    probes: Mutex<Vec<Weak<Probe>>>,
    next_probe: AtomicU64,
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
            runtime: std::sync::OnceLock::new(),
            db,
            hubs: Mutex::new(HashMap::new()),
            catch_up: Arc::new(Semaphore::new(limits.max_catch_up.max(1))),
            limits,
            stats: Arc::default(),
            live: Arc::default(),
            probes: Mutex::new(Vec::new()),
            next_probe: AtomicU64::new(1),
        })
    }

    /// Runs the hubs and the clients' delivery on `runtime` from now on, instead of the
    /// caller's: the server's feed runtime (ticket 5's (A), with perf287's D1: the hubs too),
    /// so delivery never holds a worker that serves requests. Set once, before serving.
    pub(crate) fn run_on(&self, runtime: tokio::runtime::Handle) {
        let _ = self.runtime.set(runtime);
    }

    /// Spawns `f` on the feed runtime, or on the caller's without one (tests).
    fn spawn<F>(&self, f: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match self.runtime.get() {
            Some(rt) => rt.spawn(f),
            None => tokio::spawn(f),
        }
    }

    /// Every hub and client, one per line, for `phdb serve`'s SIGUSR1 dump: a stream that
    /// stops delivering shows whether the transport stopped taking (`depth` grows, `take`
    /// ages), the client stopped following its hub (`at` behind `head`, state 1), or the hub
    /// stopped receiving (`push` ages).
    pub(crate) fn dump(&self) -> String {
        use std::fmt::Write as _;
        let now = now_us();
        let ago = |t: u64| now.saturating_sub(t) / 1000;
        let mut out = String::new();
        let hubs: Vec<Arc<Hub>> = self
            .hubs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter_map(Weak::upgrade)
            .collect();
        let _ = writeln!(out, "phdb serve: feed dump: {} hubs", hubs.len());
        for h in &hubs {
            let r = h.ring();
            let _ = writeln!(
                out,
                "  hub {:p}: head {} base {} entries {} bytes {} passed {} failed {} push {} ms ago",
                Arc::as_ptr(h),
                r.head(),
                r.base,
                r.entries.len(),
                r.bytes,
                r.passed,
                r.failed,
                ago(h.last_push_us.load(Ordering::Relaxed)),
            );
        }
        let probes: Vec<Arc<Probe>> = self
            .probes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        let _ = writeln!(out, "phdb serve: feed dump: {} clients", probes.len());
        for p in &probes {
            let hub = p
                .hub
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .upgrade();
            let head = hub.as_ref().map(|h| h.ring().head());
            let (sent, taken) = (
                p.sent.load(Ordering::Relaxed),
                p.taken.load(Ordering::Relaxed),
            );
            let _ = writeln!(
                out,
                "  client {} [{}]: state {} at {} head {} sent {} taken {} depth {} send {} ms ago take {} ms ago hub {:p}",
                p.id,
                p.tables,
                p.state.load(Ordering::Relaxed),
                p.ring_at.load(Ordering::Relaxed),
                head.map_or_else(|| "-".to_owned(), |h| h.to_string()),
                sent,
                taken,
                sent.saturating_sub(taken),
                ago(p.last_send_us.load(Ordering::Relaxed)),
                ago(p.last_take_us.load(Ordering::Relaxed)),
                hub.as_ref().map_or(std::ptr::null(), Arc::as_ptr),
            );
        }
        out
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

    /// Fails every hub: the database under them was replaced (#1436's re-bootstrap). Their
    /// clients go on from their own tokens on their own streams, and the engine decides
    /// each (a token of the replaced copy is another database's); new subscribers open new
    /// hubs.
    pub(crate) fn fail_all(&self) {
        let mut hubs = self.hubs.lock().unwrap_or_else(PoisonError::into_inner);
        let live: Vec<Arc<Hub>> = hubs.drain().filter_map(|(_, w)| w.upgrade()).collect();
        drop(hubs);
        for hub in live {
            hub.task.abort();
            hub.fail();
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
        let start_token = token_of(&stream);
        let (head, _) = watch::channel(0);
        let limit = self.limits.ring_bytes;
        let stats = Arc::clone(&self.stats);
        let live = Arc::clone(&self.live);
        live.fetch_add(1, Ordering::Relaxed);
        let hub = Arc::new_cyclic(|weak: &Weak<Hub>| {
            let weak = weak.clone();
            let task = self
                .spawn(async move {
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
                                hub.note_passed(at, || token_of(&stream));
                            }
                            polled
                        })
                        .await;
                        let Some(hub) = weak.upgrade() else { return };
                        match item {
                            Some(Ok(commit)) => {
                                let at = stream.position().seqno();
                                hub.push(commit, token_of(&stream), at, limit);
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
                last_push_us: AtomicU64::new(now_us()),
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
        let (tx, rx) = mpsc::channel(self.limits.client_events.max(1));
        let budget = Arc::new(Semaphore::new(
            self.limits.client_bytes.clamp(1, Semaphore::MAX_PERMITS),
        ));
        let taken = Arc::new(Notify::new());
        let (end_tx, end_rx) = oneshot::channel();
        let probe = Arc::new(Probe {
            id: self.next_probe.fetch_add(1, Ordering::Relaxed),
            tables: spec
                .tables
                .iter()
                .map(|t| t.name().to_owned())
                .collect::<Vec<_>>()
                .join(","),
            hub: Mutex::new(Arc::downgrade(&hub)),
            ..Probe::default()
        });
        {
            let mut probes = self.probes.lock().unwrap_or_else(PoisonError::into_inner);
            probes.retain(|p| p.strong_count() > 0);
            probes.push(Arc::downgrade(&probe));
        }
        let client = Client {
            feeds: Arc::clone(self),
            hub: Mutex::new(hub),
            after,
            tables: spec.tables,
            prefix: spec.prefix,
            visible: spec.visible,
            tx,
            budget,
            taken: Arc::clone(&taken),
            probe: Arc::clone(&probe),
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
        self.spawn(async move {
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
            taken,
            end: Some(end_rx),
            probe,
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

/// A stream's token right after what it has read, as the bytes events carry.
fn token_of(stream: &FeedStream) -> Bytes {
    static CALLS: AtomicU64 = AtomicU64::new(0);
    static SLOW: AtomicU64 = AtomicU64::new(0);
    static SLOW_US: AtomicU64 = AtomicU64::new(0);
    let t0 = std::time::Instant::now();
    let b = Bytes::copy_from_slice(stream.token().as_bytes());
    let us = t0.elapsed().as_micros() as u64;
    let n = CALLS.fetch_add(1, Ordering::Relaxed) + 1;
    if us >= 1000 {
        let k = SLOW.fetch_add(1, Ordering::Relaxed) + 1;
        let tot = SLOW_US.fetch_add(us, Ordering::Relaxed) + us;
        if k.is_power_of_two() || k % 256 == 0 {
            eprintln!(
                "DBG {} token_of slow #{k} {us}us (slow total {tot}us, calls {n})",
                now_us()
            );
        }
    } else if n.is_power_of_two() && n >= 1024 {
        eprintln!("DBG {} token_of calls {n} last {us}us", now_us());
    }
    b
}

/// A feed stream's next item, as a future.
async fn next(stream: &mut FeedStream) -> Option<pigeonhole::Result<Commit>> {
    std::future::poll_fn(|cx| Pin::new(&mut *stream).poll_next(cx)).await
}

/// Where a client starts, as `subscribe` found it.
enum Begin {
    /// On the ring at `at`, its position `seqno` and the token with it.
    Ring { at: u64, seqno: u64, token: Bytes },
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
    tx: mpsc::Sender<(Out, OwnedSemaphorePermit)>,
    /// The client's buffer's free bytes: each event in its channel holds its encoded size.
    budget: Arc<Semaphore>,
    /// Poked each time the client takes an event: a send waiting for room starts its
    /// `--stream-idle-ms` again (simp724's I1 on #1536).
    taken: Arc<Notify>,
    probe: Arc<Probe>,
}

/// A client's position: the last commit it covered and the token right after it.
struct Position {
    seqno: Option<u64>,
    token: Bytes,
    /// The token of the last event sent, to tell whether a progress event would say more.
    sent: Bytes,
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
                let token = token_of(&stream);
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
        self.probe.set(4);
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
        // Reused across wakes: one allocation per client, not one per commit.
        let mut batch: Vec<Arc<Entry>> = Vec::with_capacity(64);
        loop {
            let (behind, failed) = {
                let r = hub.ring();
                if at < r.base {
                    (true, r.failed)
                } else {
                    let from = ((at - r.base) as usize).min(r.entries.len());
                    batch.extend(r.entries.range(from..).take(64).cloned());
                    (false, r.failed)
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
                    &Start::Token(FeedToken::from_bytes(pos.token.to_vec())),
                )?;
                return Ok(Source::Own(Box::new(FeedStream::new(feed)), slot));
            }
            if batch.is_empty() {
                self.probe.set(1);
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
            self.probe.set(2);
            for e in batch.drain(..) {
                at += 1;
                self.cover_entry(&e, pos).await?;
                self.probe.ring_at.store(at, Ordering::Relaxed);
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
        self.probe.set(3);
        loop {
            // A failed hub: take the key's current one, opening a new one if none (F2), at
            // most once a second so a hub that keeps failing isn't reopened in a loop.
            let mut hub = self.hub();
            if hub.ring().failed && hub_retry.is_none_or(|t| t.elapsed() >= HUB_RETRY) {
                hub_retry = Some(Instant::now());
                if let Ok(new) = self.feeds.hub(&self.tables, self.visible) {
                    *self.hub.lock().unwrap_or_else(PoisonError::into_inner) = Arc::clone(&new);
                    *self
                        .probe
                        .hub
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner) = Arc::downgrade(&new);
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
                        let token = token_of(&stream);
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
                        pos.token = token_of(&stream);
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
        token: &Bytes,
        pos: &mut Position,
    ) -> Result<(), ApiError> {
        pos.seqno = Some(commit.seqno());
        pos.token = token.clone();
        if commit.seqno() <= self.after {
            return Ok(());
        }
        if let Some(c) = filtered(commit, &self.prefix) {
            self.send(Out::Event(commit_event(token.clone(), c)), pos)
                .await?;
        }
        Ok(())
    }

    /// Covers one entry of the ring. A client with no prefix sends the entry itself: its
    /// event is encoded once, for every such client (perf287's profile on #1536: building
    /// and encoding the same event per subscriber was two thirds of the feed's CPU).
    async fn cover_entry(&self, e: &Arc<Entry>, pos: &mut Position) -> Result<(), ApiError> {
        if !self.prefix.is_empty() {
            return self.cover(&e.commit, &e.token, pos).await;
        }
        pos.seqno = Some(e.commit.seqno());
        pos.token = e.token.clone();
        if e.commit.seqno() <= self.after || e.encoded().is_none() {
            return Ok(());
        }
        self.send(Out::Shared(Arc::clone(e)), pos).await
    }

    /// A progress event, if the position moved since the last event and the stream was quiet.
    async fn progress(&self, pos: &mut Position) -> Result<(), ApiError> {
        if pos.token == pos.sent {
            // Nothing to say: look again after another quiet interval.
            pos.last_event = Instant::now();
            return Ok(());
        }
        self.send(
            Out::Event(proto::FeedEvent {
                token: pos.token.clone(),
                event: Some(proto::feed_event::Event::Progress(proto::Progress {})),
            }),
            pos,
        )
        .await
    }

    /// Counts a send into the client's buffer, for the dump.
    fn sent(&self) {
        self.probe.sent.fetch_add(1, Ordering::Relaxed);
        self.probe.last_send_us.store(now_us(), Ordering::Relaxed);
    }

    async fn send(&self, event: Out, pos: &mut Position) -> Result<(), ApiError> {
        let token = event.token().clone();
        // Room in the client's buffer, then a place in its channel, both inside the idle
        // limit: a client that takes nothing fills its buffer and then times out.
        let bytes = event.encoded_len().clamp(
            1,
            self.feeds
                .limits
                .client_bytes
                .clamp(1, Semaphore::MAX_PERMITS),
        );
        let n = u32::try_from(bytes).unwrap_or(u32::MAX);
        let gone = || ApiError::internal("the client went away");
        // The fast path: room in the buffer and a place in the channel at once, with no
        // timer or waiter registered. Arming them on every send was 64% of the samples in
        // futex calls at 256 subscribers (perf287's profile on #1397).
        let (event, held) = match Arc::clone(&self.budget).try_acquire_many_owned(n) {
            Ok(room) => match self.tx.try_send((event, room)) {
                Ok(()) => {
                    pos.sent = token;
                    pos.last_event = Instant::now();
                    self.sent();
                    return Ok(());
                }
                Err(mpsc::error::TrySendError::Full((event, room))) => (event, Some(room)),
                Err(mpsc::error::TrySendError::Closed(_)) => return Err(gone()),
            },
            Err(tokio::sync::TryAcquireError::NoPermits) => (event, None),
            Err(tokio::sync::TryAcquireError::Closed) => return Err(gone()),
        };
        // The slow path: wait for whichever was missing.
        let was = self.probe.state.swap(5, Ordering::Relaxed);
        let budget = Arc::clone(&self.budget);
        let queued = async {
            let room = match held {
                Some(room) => room,
                None => budget.acquire_many_owned(n).await.map_err(|_| ())?,
            };
            self.tx.send((event, room)).await.map_err(|_| ())
        };
        tokio::pin!(queued);
        // The idle limit counts from the client's last take, not from the start of the wait:
        // a large event may wait for most of the buffer to drain, and a client taking events
        // all along is slow, not idle (simp724's I1 on #1536).
        loop {
            let taken = self.taken.notified();
            tokio::select! {
                // The send first: room that is there wins over a timer that fired meanwhile.
                biased;
                sent = &mut queued => {
                    return match sent {
                        Ok(()) => {
                            pos.sent = token;
                            pos.last_event = Instant::now();
                            self.sent();
                            self.probe.set(was);
                            Ok(())
                        }
                        // The client went away: end quietly.
                        Err(()) => Err(gone()),
                    };
                }
                () = taken => {}
                () = self.tx.closed() => return Err(gone()),
                () = tokio::time::sleep(self.feeds.limits.stream_idle) => {
                    return Err(ApiError::deadline(format!(
                        "the client took no feed event for --stream-idle-ms ({} ms); resume \
                         from the last event's token",
                        self.feeds.limits.stream_idle.as_millis()
                    )));
                }
            }
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
    events: mpsc::Receiver<(Out, OwnedSemaphorePermit)>,
    /// Poked on each take, for a send waiting for room.
    taken: Arc<Notify>,
    end: Option<oneshot::Receiver<ApiError>>,
    probe: Arc<Probe>,
}

/// An event as it leaves a client: its own, or a ring entry encoded once for every client
/// with no prefix.
pub(crate) enum Out {
    Event(proto::FeedEvent),
    /// An entry whose `encoded()` is `Some`.
    Shared(Arc<Entry>),
}

impl Out {
    fn token(&self) -> &Bytes {
        match self {
            Out::Event(e) => &e.token,
            Out::Shared(e) => &e.token,
        }
    }

    /// Its size on the wire, for the client's buffer.
    fn encoded_len(&self) -> usize {
        match self {
            Out::Event(e) => prost::Message::encoded_len(e),
            Out::Shared(e) => e.encoded().map_or(0, Bytes::len),
        }
    }

    /// The event as a message (HTTP, which renders JSON, and tests).
    pub(crate) fn into_proto(self) -> proto::FeedEvent {
        match self {
            Out::Event(e) => e,
            Out::Shared(e) => {
                let c = filtered(&e.commit, b"").unwrap_or_default();
                commit_event(e.token.clone(), c)
            }
        }
    }

    /// Writes the event's encoding to `buf` (gRPC): a shared entry's bytes as they are.
    pub(crate) fn encode(
        &self,
        buf: &mut impl prost::bytes::BufMut,
    ) -> Result<(), prost::EncodeError> {
        match self {
            Out::Event(e) => prost::Message::encode(e, buf),
            Out::Shared(e) => {
                if let Some(b) = e.encoded() {
                    buf.put_slice(b);
                }
                Ok(())
            }
        }
    }
}

impl FeedEvents {
    /// The next event as it left the client, then the client's error if it failed.
    pub(crate) fn poll_out(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Out, ApiError>>> {
        match self.events.poll_recv(cx) {
            // Taken: its bytes go back to the client's buffer.
            Poll::Ready(Some((e, room))) => {
                drop(room);
                self.probe.taken.fetch_add(1, Ordering::Relaxed);
                self.probe.last_take_us.store(now_us(), Ordering::Relaxed);
                self.taken.notify_one();
                return Poll::Ready(Some(Ok(e)));
            }
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

impl Stream for FeedEvents {
    type Item = Result<proto::FeedEvent, ApiError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_out(cx)
            .map(|item| item.map(|r| r.map(Out::into_proto)))
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
                prev,
                ..Entry::new(c.clone(), Bytes::new())
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
                client_events: CLIENT_EVENTS,
                client_bytes: CLIENT_BYTES,
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
    fn hubs_and_clients_run_on_the_feed_runtime() {
        // Ticket 5's (A): with a feed runtime, a subscription's hub and client tasks run
        // there, not on the request's runtime, and its events still reach the stream the
        // request side reads.
        let (_, db, dir) = commits("feedrt", 0);
        let requests = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let delivery = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("phdb-serve-feed")
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
                client_events: CLIENT_EVENTS,
                client_bytes: CLIENT_BYTES,
            },
        );
        feeds.run_on(delivery.handle().clone());
        let slots = Arc::new(Semaphore::new(8));
        let before = delivery.metrics().num_alive_tasks();
        requests.block_on(async {
            let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
            let spec = Spec {
                tables: vec![t.clone()],
                prefix: Vec::new(),
                visible: false,
                start: Start::Now,
            };
            let mut events = feeds.subscribe(spec, slot, |_| {}).unwrap();
            assert_eq!(
                delivery.metrics().num_alive_tasks(),
                before + 2,
                "the hub and the client"
            );
            assert_eq!(
                tokio::runtime::Handle::current()
                    .metrics()
                    .num_alive_tasks(),
                0,
                "nothing of the feed on the request side"
            );
            let seqno = t.mutate(b"r").put("f", b"q", b"v").commit().unwrap().seqno;
            assert_eq!(seqnos(&mut events, Duration::from_secs(1)).await, [seqno]);
            // The SIGUSR1 dump shows the hub and the client: one commit sent, one taken.
            let dump = feeds.dump();
            assert!(
                dump.contains("1 hubs") && dump.contains("1 clients"),
                "{dump}"
            );
            assert!(
                dump.contains("[t]: state 1") && dump.contains("sent 1 taken 1 depth 0"),
                "{dump}"
            );
        });
        drop(t);
        drop(requests);
        drop(delivery);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_slow_client_that_keeps_taking_outlasts_a_large_events_wait() {
        // simp724's I1 on #1536: a large event waits for the whole buffer to drain. A client
        // that takes an event every 200 ms, under the 250 ms idle limit, drains it in far
        // more than 250 ms, and must not time out: the limit counts from its last take. On a
        // paused clock (#1531), so the pacing and the limit are virtual time, whatever the
        // runner's speed. The hub's task is stopped and the test pushes for it.
        let (_, db, dir) = commits("slow", 0);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        let t = db.table("t").unwrap().open().unwrap();
        let feeds = Feeds::new(
            db.clone(),
            FeedLimits {
                ring_bytes: 16 << 20,
                max_hubs: 4,
                max_catch_up: 1,
                quiet: Duration::from_secs(3600),
                stream_idle: Duration::from_millis(250),
                client_events: CLIENT_EVENTS,
                client_bytes: 4096,
            },
        );
        let slots = Arc::new(Semaphore::new(1));
        let mut direct = t.feed().open().unwrap();
        let mut want = Vec::new();
        rt.block_on(async {
            let hub = feeds.hub(std::slice::from_ref(&t), false).unwrap();
            hub.task.abort();
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
            // 60 small commits, then one larger than the whole buffer, then a small one.
            let rows = (0..60)
                .map(|i| (format!("r{i:04}").into_bytes(), vec![b'v']))
                .chain([
                    (b"zbig".to_vec(), vec![7u8; 8192]),
                    (b"zz".to_vec(), vec![b'v']),
                ]);
            for (row, value) in rows {
                want.push(
                    t.mutate(&row)
                        .put("f", b"q", &value)
                        .commit()
                        .unwrap()
                        .seqno,
                );
                let commit = direct
                    .next(Duration::from_secs(5))
                    .unwrap()
                    .unwrap()
                    .clone();
                let token = Bytes::copy_from_slice(direct.token().as_bytes());
                hub.push(commit, token, direct.position().seqno(), 16 << 20);
            }
            let mut got = Vec::new();
            while got.len() < want.len() {
                use futures_core::Stream as _;
                let e = std::future::poll_fn(|cx| Pin::new(&mut c).poll_next(cx))
                    .await
                    .expect("the stream goes on")
                    .unwrap_or_else(|e| panic!("after {} events: {e:?}", got.len()));
                if let Some(proto::feed_event::Event::Commit(commit)) = e.event {
                    got.push(commit.seqno);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            assert_eq!(got, want);
            drop((c, hub));
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
                client_events: CLIENT_EVENTS,
                client_bytes: CLIENT_BYTES,
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
            let token = Bytes::copy_from_slice(direct.token().as_bytes());
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
    fn a_shared_entry_is_the_exact_encoding_of_the_event_a_client_with_no_prefix_covers() {
        // simp724's T1 on #1541: byte for byte, built here on its own, not through the
        // entry's helpers.
        let (commits, db, dir) = commits("encoded", 3);
        for c in &commits {
            let token = Bytes::from_static(b"the token right after it");
            let e = Arc::new(Entry::new(c.clone(), token.clone()));
            let want = prost::Message::encode_to_vec(&proto::FeedEvent {
                token: token.clone(),
                event: Some(proto::feed_event::Event::Commit(proto::FeedCommit {
                    seqno: c.seqno(),
                    timestamp: c.timestamp(),
                    changes: c
                        .changes()
                        .iter()
                        .map(|ch| proto::Change {
                            table: ch.table().to_owned(),
                            family: ch.family().to_owned(),
                            kind: change_kind(ch.kind()) as i32,
                            row: ch.row().to_vec().into(),
                            qualifier: ch.qualifier().to_vec().into(),
                            timestamp: ch.timestamp(),
                            value: ch.value().map(convert::value_out),
                        })
                        .collect(),
                })),
            });
            assert_eq!(e.encoded().map(|b| &b[..]), Some(&want[..]));
            let mut wire = Vec::new();
            Out::Shared(Arc::clone(&e)).encode(&mut wire).unwrap();
            assert_eq!(wire, want, "the gRPC codec writes those bytes");
            assert_eq!(Out::Shared(Arc::clone(&e)).encoded_len(), want.len());
            // Counted in the ring's budget (R1, P1).
            assert_eq!(e.bytes, entry_bytes(c, &token) + want.len());
            let decoded = <proto::FeedEvent as prost::Message>::decode(&want[..]).unwrap();
            assert_eq!(
                Out::Shared(e).into_proto(),
                decoded,
                "and HTTP sees that event"
            );
        }
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
