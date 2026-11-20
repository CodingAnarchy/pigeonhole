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

mod events;
use events::*;
mod client;
use client::*;
mod hub;
use hub::*;
mod entry;
use entry::*;
mod tracker;
use tracker::*;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_core::Stream;
use pigeonhole::nonblocking::FeedStream;
use pigeonhole::{Commit, FeedToken, Snapshot, Table};
use prost::bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};

use super::lease::DeliveryLease;

use super::convert;
use super::errors::ApiError;
use super::proto;

/// The feed fan-out's limits.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FeedLimits {
    /// Bytes of commits the rings hold in all (`--feed-ring-bytes`), as their encoded events,
    /// tokens included (about 45 + 16 × shards bytes each): memory outside R3's in-flight
    /// budget. Each hub's ring is
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
    /// Places in a client's channel, each an event or a batch ([`CLIENT_EVENTS`]), and the
    /// encoded bytes its connection may hold untaken ([`CLIENT_BYTES`], each event charged at
    /// least [`EVENT_FLOOR`]): the per-client buffer.
    pub client_events: usize,
    pub client_bytes: usize,
    /// How long a hub on the hub runtime must have had at most one client before it moves
    /// back to the feed runtime ([`SOLO_AFTER`]; #1689).
    pub solo_after: Duration,
    /// How long a caught-up hub, and a caught-up client, wait after a small batch before
    /// looking again (#1901): `--feed-hub-coalesce-ticks`, `--feed-client-coalesce-ticks`.
    pub coalesce: Coalesce,
}

/// After a small batch (at most [`SMALL_BATCH`] commits), how long a hub waits before it polls
/// its feed again, and a client before it looks at its ring's head again, so that the commits
/// arriving meanwhile are read together (#1901: at s20p1 two hub threads each ran about a
/// core, waking once per commit). The wait comes after work, never before it: a commit after
/// a quiet gap longer than the wait is read at once. Zero never waits; `phdb serve` waits one
/// tick on the hub side by default on Linux, and none elsewhere, where a tick lasts the OS
/// timer's: about 14–16 ms on Windows, 3–10 ms (up to 30 ms) on macOS
/// (`--feed-hub-coalesce-ticks`). Tests keep zero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Coalesce {
    pub hub: Duration,
    pub client: Duration,
}

/// A batch of at most this many commits is a small one: the hub or client is caught up, and
/// may wait ([`Coalesce`]). A bigger batch means it's behind, and it looks again at once.
const SMALL_BATCH: u32 = 4;

/// A hub tells its clients about its pushes at most this many pushes apart, so a hub that's
/// behind (its feed never empty) still reaches them at a steady cadence (perf2 on #1901's
/// design note); otherwise once its feed has nothing more, after the batch.
const NOTIFY_EVERY: u32 = 64;

/// How long a hub must have had at most one client before its task moves back to the feed
/// runtime (#1689): a client that joins and leaves doesn't bounce it on every join.
pub(crate) const SOLO_AFTER: Duration = Duration::from_secs(1);

/// Places in a client's channel: each holds an event or a batch of them ([`Sink::put_all`]).
/// More than one lets the gRPC encoder pack every event that is ready into one HTTP/2 frame:
/// with one, each event cost every subscriber its own stream poll, its own trips through the
/// connection's shared lock and its own frame (#1397's acceptance, ticket 5). The buffer's
/// bound is [`CLIENT_BYTES`], with each event charged at least [`EVENT_FLOOR`].
pub(crate) const CLIENT_EVENTS: usize = 64;

/// Encoded bytes buffered per client, at most: the bound on a client's buffer however large
/// its commits, and however many events it holds (each charged at least [`EVENT_FLOOR`]). An
/// event larger than this is still sent, alone.
pub(crate) const CLIENT_BYTES: usize = 256 << 10;

/// The least an event is charged against [`CLIENT_BYTES`]: its `Out` and its share of a
/// batch, so a buffer of tiny events is bounded by the same bytes (simp724's (1) on #1737's
/// F3-0). Without it, 64 batches of 64 progress-sized events would sit outside the bound.
const EVENT_FLOOR: usize = 64;

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
    /// A cluster component resumed with `skip_through` (D317 ticket 7): commits after the
    /// start up to this seqno are passed over, as already handled, at most [`SKIP_LIMIT`] of
    /// them that would have been sent; 0: none.
    pub skip: u64,
    /// Send the start's position as a first progress event, before any commit: a cluster
    /// subscription's part (D317 ticket 7) then knows where it is from the start, so it can
    /// resume after a break that came before any event, with nothing skipped.
    pub announce: bool,
    /// A cluster feed hub's own component (#1798): a hub whose members are all these runs
    /// on the hub runtime, never beside its one member ([`HubOf::external`]).
    pub internal: bool,
}

/// At most this many commits a client would have been sent are passed over for `skip`: a
/// cluster subscription's checkpoints are at most this many events apart, so a client can't
/// have handled more after one (perf287's cap on ticket 7). More is `InvalidArgument`, never
/// a silent gap.
pub(crate) const SKIP_LIMIT: u64 = 64;

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
    /// Hub tasks that moved between the feed and hub runtimes (#1689).
    pub hub_moves: AtomicU64,
}

/// The hubs, by table set.
pub(crate) struct Feeds {
    /// Where hubs and clients run: the server's feed runtime, once `run_on` set it.
    runtime: std::sync::OnceLock<tokio::runtime::Handle>,
    /// Where the hubs run: the server's hub runtime, once `run_on` set it (#1397: on the
    /// feed runtime they queued behind the clients' tasks and fell behind their feeds).
    hub_runtime: std::sync::OnceLock<tokio::runtime::Handle>,
    db: pigeonhole::Pigeonhole,
    hubs: Mutex<HashMap<HubKey, Weak<Hub>>>,
    /// The node's catch-up slots (`--max-catch-up`), shared by its partitions' feeds.
    catch_up: Arc<Semaphore>,
    limits: FeedLimits,
    pub(crate) stats: Arc<FeedStats>,
    /// The node's live hubs, every partition's: `--max-feed-hubs` counts them (D317 ticket
    /// 8a).
    live: Arc<AtomicU64>,
    /// The node's rings that split `--feed-ring-bytes` ([`FeedShared::sharers`]).
    sharers: Arc<AtomicU64>,
    /// Ends every cluster component stream (`Spec::announce`) open on this partition, with
    /// the error sent: the node's map no longer gives it the partition (audit120's S-C5).
    parts_ended: watch::Sender<Option<ApiError>>,
    /// A hold seam (#1926): run in [`hub`](Self::hub) right after a new hub's task is
    /// spawned, with what the task sets once it has handled its first item.
    #[cfg(test)]
    #[allow(clippy::type_complexity)]
    pub(crate) after_hub_spawn: Mutex<Option<Box<dyn Fn(&Arc<Started>) + Send>>>,
}

/// A hub's place in the node's live count (`--max-feed-hubs`, D317 ticket 8a), taken before
/// the hub opens and given back if it doesn't: the hub, once made, gives it back when it's
/// dropped.
struct HubPlace<'a> {
    live: &'a AtomicU64,
    kept: bool,
}

impl<'a> HubPlace<'a> {
    /// A place under `max` (`replacing`: a failed hub of the same key that's being replaced
    /// still counts until it's dropped, and not against its replacement), or `None`.
    fn take(live: &'a AtomicU64, replacing: u64, max: u64) -> Option<Self> {
        let mut n = live.load(Ordering::Relaxed);
        loop {
            if n.saturating_sub(replacing) >= max {
                return None;
            }
            match live.compare_exchange_weak(n, n + 1, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return Some(Self { live, kept: false }),
                Err(now) => n = now,
            }
        }
    }

    /// The hub opened: it holds the place now.
    fn keep(mut self) {
        self.kept = true;
    }
}

impl Drop for HubPlace<'_> {
    fn drop(&mut self) {
        if !self.kept {
            self.live.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// What a node's feeds share across its partitions (D317 ticket 8a, the coordinator's call
/// on cli692's S1 on #1718): the catch-up slots and the live hub count, so
/// `--feed-ring-bytes`, `--max-feed-hubs` and `--max-catch-up` are the node's, "in all", as
/// they are on a single node.
#[derive(Debug, Clone)]
pub(crate) struct FeedShared {
    pub(crate) catch_up: Arc<Semaphore>,
    live: Arc<AtomicU64>,
    /// The rings with a member, which split `--feed-ring-bytes`: single-node hubs' and
    /// cluster feed hubs' alike ([`HubOf::ring_limit`], #1798).
    pub(crate) sharers: Arc<AtomicU64>,
}

impl FeedShared {
    pub(crate) fn new(max_catch_up: usize) -> Self {
        Self {
            catch_up: Arc::new(Semaphore::new(max_catch_up.max(1))),
            live: Arc::default(),
            sharers: Arc::default(),
        }
    }
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
    /// Hubs whose task runs on the feed runtime beside their one client (#1689).
    pub solo_hubs: usize,
}

impl Feeds {
    pub(crate) fn new(
        db: pigeonhole::Pigeonhole,
        limits: FeedLimits,
        shared: &FeedShared,
    ) -> Arc<Self> {
        Arc::new(Self {
            runtime: std::sync::OnceLock::new(),
            hub_runtime: std::sync::OnceLock::new(),
            db,
            hubs: Mutex::new(HashMap::new()),
            catch_up: Arc::clone(&shared.catch_up),
            limits,
            stats: Arc::default(),
            live: Arc::clone(&shared.live),
            sharers: Arc::clone(&shared.sharers),
            parts_ended: watch::Sender::new(None),
            #[cfg(test)]
            after_hub_spawn: Mutex::new(None),
        })
    }

    /// The database's seqno now, at the visibility a feed delivers at: what a `Now` start
    /// passes over up to (simp724's F5 on #1518; a cluster hub's joiner, #1798).
    pub(crate) fn seqno_now(&self, visible: bool) -> Result<u64, ApiError> {
        Ok(if visible {
            self.db.snapshot()?.seqno()
        } else {
            self.db.durable_snapshot()?.seqno()
        })
    }

    /// Ends every cluster component stream open on this partition now with `why`
    /// (`WrongPartition`: the map moved the partition while it stayed open here, fenced), so
    /// the subscriptions they belong to open it again where the map puts it (audit120's
    /// S-C5). A client's own subscription, on a single node, is never ended this way. A
    /// component opened after this checks the map itself.
    pub(crate) fn end_parts(&self, why: ApiError) {
        self.parts_ended.send_replace(Some(why));
    }

    /// The end [`end_parts`](Self::end_parts) sends from now on: a cluster memory watch's
    /// component ends with it too (#1915 PR 2b).
    pub(crate) fn parts_ended(&self) -> watch::Receiver<Option<ApiError>> {
        self.parts_ended.subscribe()
    }

    /// Runs the clients' delivery on `runtime` and the hubs on `hubs` from now on, instead of
    /// the caller's: the server's feed runtime (ticket 5's (A)), so delivery never holds a
    /// worker that serves requests, and the hub runtime, so a hub reading its engine feed
    /// never queues behind its clients' tasks (#1397's 16 hubs × 16). Set once, before
    /// serving.
    pub(crate) fn run_on(&self, runtime: tokio::runtime::Handle, hubs: tokio::runtime::Handle) {
        let _ = self.runtime.set(runtime);
        let _ = self.hub_runtime.set(hubs);
    }

    /// The hub runtime's threads (#1719), once `run_on` set it.
    pub(crate) fn hub_threads(&self) -> Option<usize> {
        self.hub_runtime.get().map(|h| h.metrics().num_workers())
    }

    /// Spawns a hub's task on the hub runtime, or as [`spawn`](Self::spawn) does without one.
    fn spawn_hub<F>(&self, f: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match self.hub_runtime.get() {
            Some(rt) => rt.spawn(f),
            None => self.spawn(f),
        }
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

    pub(crate) fn gauges(&self) -> FeedGauges {
        let hubs = self.hubs.lock().unwrap_or_else(PoisonError::into_inner);
        let live: Vec<Arc<Hub>> = hubs.values().filter_map(Weak::upgrade).collect();
        drop(hubs);
        FeedGauges {
            hubs: live.len(),
            ring_bytes: live.iter().map(|h| h.ring().bytes).sum(),
            catching_up: self.limits.max_catch_up.max(1) - self.catch_up.available_permits(),
            solo_hubs: live
                .iter()
                .filter(|h| h.solo.load(Ordering::Relaxed))
                .count(),
        }
    }

    /// A report of every live hub and where its clients are, for `phdb serve` on SIGUSR1
    /// (#1397's 16 hubs × 16): how far each hub's engine feed is behind the database, its
    /// ring against its share of `--feed-ring-bytes`, and its clients on the ring (entries
    /// behind its head), on their own streams (seqnos behind the hub) and waiting for a
    /// `--max-catch-up` slot. Blocking: it takes a snapshot for the database's seqno.
    /// Only a Unix server has the signal, hence the allowance elsewhere (Windows CI on #1643).
    #[cfg_attr(not(unix), allow(dead_code))]
    pub(crate) fn dump(&self) -> String {
        use std::fmt::Write as _;
        fn spread(mut v: Vec<u64>) -> String {
            if v.is_empty() {
                return "-".into();
            }
            v.sort_unstable();
            format!("{}/{}/{}", v[0], v[v.len() / 2], v[v.len() - 1])
        }
        let db_seqno = self.db.snapshot().map(|s| s.seqno()).ok();
        let hubs: Vec<(HubKey, Arc<Hub>)> = {
            let hubs = self.hubs.lock().unwrap_or_else(PoisonError::into_inner);
            hubs.iter()
                .filter_map(|(k, w)| w.upgrade().map(|h| (k.clone(), h)))
                .collect()
        };
        let st = &self.stats;
        let max_catch_up = self.limits.max_catch_up.max(1);
        let mut out = String::new();
        let _ = writeln!(
            out,
            "phdb serve: feed dump: {} hubs on {} hub threads, database seqno {}; catch-up slots \
             {}/{max_catch_up} in use; fell behind {}, rejoined {}, catch-up waits {}, hub failures {}",
            hubs.len(),
            self.hub_threads()
                .map_or_else(|| "?".into(), |n| n.to_string()),
            db_seqno.map_or_else(|| "?".into(), |s| s.to_string()),
            max_catch_up - self.catch_up.available_permits(),
            st.fell_behind.load(Ordering::Relaxed),
            st.rejoined.load(Ordering::Relaxed),
            st.catch_up_waits.load(Ordering::Relaxed),
            st.hub_failures.load(Ordering::Relaxed),
        );
        for (i, (key, hub)) in hubs.iter().enumerate() {
            let (base, head, entries, bytes, passed, failed) = {
                let r = hub.ring();
                (
                    r.base,
                    r.head(),
                    r.entries.len(),
                    r.bytes,
                    r.passed,
                    r.failed,
                )
            };
            let limit = hub.ring_limit(self.limits.ring_bytes);
            let trackers = hub.trackers();
            let (mut on_ring, mut own, mut waiting) = (Vec::new(), Vec::new(), 0usize);
            for t in &trackers {
                let at = t.at.load(Ordering::Relaxed);
                match t.state.load(Ordering::Relaxed) {
                    ON_RING => on_ring.push(head.saturating_sub(at)),
                    ON_OWN => own.push(passed.saturating_sub(at)),
                    _ => waiting += 1,
                }
            }
            let _ = writeln!(
                out,
                "  hub {i} (feeds {:?}, {}{}, {}): engine feed {} seqnos behind (at {passed}); ring {entries} \
                 entries [{base}, {head}), {bytes}/{limit} bytes; clients: {} on the ring (entries \
                 behind its head, min/p50/max {}), {} on their own streams (seqnos behind the hub {}), \
                 {waiting} waiting for a catch-up slot",
                key.tables,
                if key.visible { "visible" } else { "durable" },
                if failed { ", FAILED" } else { "" },
                if hub.solo.load(Ordering::Relaxed) {
                    "solo on the feed threads"
                } else {
                    "on the hub threads"
                },
                db_seqno.map_or_else(|| "?".into(), |s| s.saturating_sub(passed).to_string()),
                on_ring.len(),
                spread(on_ring.clone()),
                own.len(),
                spread(own.clone()),
            );
        }
        out
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
            // Under the task's lock, so a task moving runtimes (#1689) either is aborted
            // here or sees the hub failed and stops.
            let task = hub.task();
            if let Some(task) = &*task {
                task.abort();
            }
            hub.fail();
            drop(task);
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
        // A failed hub under this key is being replaced: it's still live until its clients
        // let go, and mustn't count against its own replacement.
        let replacing = u64::from(hubs.get(&key).and_then(Weak::upgrade).is_some());
        hubs.retain(|k, w| w.strong_count() > 0 && *k != key);
        // The node's live hubs, every partition's (D317 ticket 8a): `--max-feed-hubs` is
        // the node's. The place is taken here, atomically: two partitions' hubs opening at
        // once each hold only their own map's lock, so a load then an add would let both
        // past the limit. It's given back if the hub doesn't open.
        let place = HubPlace::take(&self.live, replacing, self.limits.max_hubs.max(1) as u64)
            .ok_or_else(|| {
                ApiError::overloaded(
                    "the server is at its feed hub limit (--max-feed-hubs): too many distinct \
                     table sets are subscribed",
                )
            })?;
        // From a snapshot taken now, at the visibility it delivers at, so the ring knows the
        // position it starts from exactly: a client catching up can then rejoin it before
        // the hub has a commit of its own.
        let snapshot = if visible {
            self.db.snapshot()?
        } else {
            self.db.durable_snapshot()?
        };
        let stream = FeedStream::new(open(tables, visible, &Start::Snapshot(snapshot))?);
        let start_seqno = stream.position().seqno();
        let start_token = token_of(&stream);
        let stats = Arc::clone(&self.stats);
        let live = Arc::clone(&self.live);
        let sharers = Arc::clone(&self.sharers);
        // From here the hub holds the place, and gives it back when it's dropped.
        place.keep();
        // A new hub has one client coming: it starts beside it on the feed runtime (#1689),
        // when the server has both runtimes.
        let runtimes = self
            .runtime
            .get()
            .cloned()
            .zip(self.hub_runtime.get().cloned());
        // The hub first, its task after, holding it: a task spawned while the hub was still
        // being made (`Arc::new_cyclic`) could finish its first poll before the hub existed,
        // find nothing to upgrade, and stop, leaving the hub's clients nothing ever again
        // (#1926). Its handle is set under the map's lock, which `fail_all` takes, before the
        // hub is in the map: nothing fails or replaces it in between.
        let hub = Arc::new(Hub::new(
            Ring::new(SingleRing {
                start_seqno,
                passed: start_seqno,
                passed_token: start_token,
                failed: false,
            }),
            runtimes.is_some(),
            stats,
            live,
            sharers,
        ));
        #[cfg(test)]
        let started = Arc::new(Started::default());
        let driver = Driver {
            stream,
            hub: Arc::downgrade(&hub),
            ring_bytes: self.limits.ring_bytes,
            runtimes: runtimes.clone(),
            solo_after: self.limits.solo_after,
            place: Place::Feed,
            alone: None,
            coalesce: self.limits.coalesce.hub,
            unnotified: 0,
            batch: 0,
            quiet_until: None,
            #[cfg(test)]
            started: Some(Arc::clone(&started)),
        };
        let task = match &runtimes {
            Some((feed, _)) => feed.spawn(drive(driver)),
            None => self.spawn_hub(drive(driver)),
        };
        #[cfg(test)]
        if let Some(seam) = &*self
            .after_hub_spawn
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
        {
            seam(&started);
        }
        // An overwrite with no check, and safe: the slot is still empty. The only other writer
        // is the task's own runtime move (`drive`), which needs two members or a cluster merge,
        // and nobody can join a hub before it's in the map, below (cli692's N1 on #1927). No
        // placeholder task is spawned for the meantime: one on the caller's runtime was work
        // of the feed's on the request side.
        hub.set_task(task.abort_handle());
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
        let spec_announce = spec.announce;
        let (sink, events, end_tx) = Sink::new(&self.limits);
        let tracker = Arc::new(Tracker::default());
        let joined = hub.join(&tracker, spec.internal);
        drop(hub);
        let client = Client {
            feeds: Arc::clone(self),
            hub: Mutex::new(joined),
            after,
            skip: spec.skip,
            skipped: AtomicU64::new(0),
            announce: spec.announce,
            internal: spec.internal,
            tables: spec.tables,
            prefix: spec.prefix,
            visible: spec.visible,
            sink,
            tracker,
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
        let ended = spec_announce.then(|| self.parts_ended.subscribe());
        self.spawn(async move {
            let run = client.run(begin);
            let outcome = match ended {
                None => run.await,
                // The end first: a close sends it just before the database closes, and the
                // run, woken by both, would otherwise end with the engine's `Closed`.
                Some(mut ended) => tokio::select! {
                    biased;
                    why = async {
                        // The partition's feeds dropped: nothing ends it this way any more.
                        if ended.changed().await.is_err() {
                            std::future::pending::<()>().await;
                        }
                        ended.borrow().clone()
                    } => Err(why.unwrap_or_else(|| {
                        ApiError::internal("the partition's feeds ended this stream")
                    })),
                    r = run => r,
                },
            };
            let outcome = match outcome {
                // The client went away: not a failure.
                Err(_) if client.sink.is_closed() => Ok(()),
                r => r,
            };
            on_end(outcome.as_ref().err());
            drop(slot);
            if let Err(e) = outcome {
                let _ = end_tx.send(e);
            }
        });
        Ok(events)
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
    Bytes::copy_from_slice(stream.token().as_bytes())
}

/// A feed stream's next item, as a future.
async fn next(stream: &mut FeedStream) -> Option<pigeonhole::Result<Commit>> {
    std::future::poll_fn(|cx| Pin::new(&mut *stream).poll_next(cx)).await
}

#[cfg(test)]
mod tests;
