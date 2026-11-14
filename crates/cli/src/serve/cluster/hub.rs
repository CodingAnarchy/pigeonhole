//! Cluster feed hubs (#1798, `feed1248-design-1798-hub.md` §2, §7, §8): one merge per
//! subscription key, shared by every subscriber of that key, as a single node's hub shares
//! one engine feed.
//!
//! - **The key** is everything the merged stream depends on: the tables, the prefix, the
//!   visibility, `allow_partial`, `checkpoint_every`, and the caller's token, scope and
//!   namespaces (a hub's remote components are authorized as the caller who opened them,
//!   simp724's Q1). The partitions the map gives the tables must match too.
//! - **The hub** runs the subscription's [`Merging`] as one task on the feed runtime and
//!   appends what it produces to a ring ([`RingOf<Cluster>`]): each commit's event, encoded
//!   once, with each component's position after it. Members read the ring at their own pace,
//!   as single-node clients do, and take the same buffer and idle limits ([`Sink`]).
//! - **A member** joins a `Now` subscription at the ring's head. Each part's commits at or
//!   below its own position are passed over: for a local partition that's the partition's
//!   seqno when it subscribed, so a local `Now` is exact (simp724's F5 on #1518); for a
//!   remote one it's the hub's position.
//! - **Off the ring:** a start from a token, the oldest commit or with `skip_through`, a key
//!   with no hub (at `--max-feed-hubs`), and a member the ring's tail passed each run a merge
//!   of their own. A fallen-behind member takes a `--max-catch-up` slot for it and waits for
//!   one, as a single-node client does. A member on its own merge rejoins the ring where the
//!   ring covers every part from its own position (§2.4).
//! - **A hub's end:** its merge's error, or its last member gone. An ending hub leaves the
//!   registry before its end is visible on its ring, so a joiner racing it opens a new one
//!   (simp724's S3).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use prost::bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::feed::{Merge, Merging, Plan, drop_idle, untaken_since};
use super::token::At;
use crate::serve::auth::{Caller, Scope, TokenId};
use crate::serve::errors::ApiError;
use crate::serve::feed::{
    FeedEvents, FeedStats, HubOf, JoinedOf, ON_OWN, ON_RING, Out, RingKind, RingOf, Sink, Tracker,
    WAITING,
};
use crate::serve::proto;
use crate::serve::service::Metrics;

/// A position not known yet: a part whose component hasn't said where it is.
pub(crate) const UNKNOWN: u64 = u64::MAX;

/// How long a joiner waits for a new hub's components to say where they are.
const HUB_START: Duration = Duration::from_secs(10);

/// How often a member on its own merge looks at its hub's ring for a place to rejoin.
const REJOIN_TICK: Duration = Duration::from_millis(250);

/// How often a member on its own merge with no running hub looks for its key's hub (as a
/// single node's client does for a failed hub's replacement, cli692's S1 on #1884).
const HUB_RETRY: Duration = Duration::from_secs(1);

/// An entry's size beside its bytes and positions, as a single node's (`ENTRY_OVERHEAD`).
const ENTRY_OVERHEAD: usize = 112;

/// A cluster feed hub's ring ([`RingKind`]).
pub(crate) struct Cluster;

/// One event of a hub's merged stream, encoded once for every member.
pub(crate) struct ClusterEntry {
    /// A commit's event without its token, as its component sent it; or a whole
    /// `PartitionState` event.
    pub(crate) body: Bytes,
    /// What the merge adds to a commit: its partition, and on a checkpoint the cluster token.
    pub(crate) tail: Bytes,
    /// The checkpoint's token (a slice of `tail`), or empty.
    pub(crate) token: Bytes,
    /// A commit's part and seqno; `None` for a state event.
    commit: Option<(u32, u64)>,
    bytes: usize,
}

/// Where a hub's merge is, beside its ring.
pub(crate) struct ClusterRing {
    /// The hub's partitions, in its parts' order: a member's positions are in this order.
    partitions: Vec<String>,
    /// Each part's position before the front entry ([`UNKNOWN`] until known).
    front: Vec<u64>,
    /// Each entry's parts' positions after it, flat: entry `i`'s are `[i·n, i·n + n)`. One
    /// allocation for the ring, never one per entry (perf287's S1).
    after: VecDeque<u64>,
    /// The hub's positions now: after its last entry, or a quiet checkpoint since.
    passed: Vec<u64>,
    /// The hub's last checkpoint: an entry's or a quiet one's. Never ahead of `passed`.
    token: Bytes,
    /// Every component has said where it is, and `token` is a checkpoint of all of them.
    ready: bool,
    /// The hub's merge ended with this; nothing more comes.
    ended: Option<ApiError>,
    /// Each part's last `PartitionState` event while it's unavailable (`allow_partial`): a
    /// joiner gets these first, as a subscription's first events say which parts are out.
    out: Vec<Option<Bytes>>,
}

impl RingKind for Cluster {
    type Entry = ClusterEntry;
    type State = ClusterRing;

    fn bytes(e: &ClusterEntry) -> usize {
        e.bytes
    }

    fn evicted(state: &mut ClusterRing, _e: &ClusterEntry) {
        for p in &mut state.front {
            *p = state.after.pop_front().unwrap_or(UNKNOWN);
        }
    }
}

impl ClusterRing {
    fn new(partitions: Vec<String>) -> Self {
        let n = partitions.len();
        Self {
            partitions,
            front: vec![UNKNOWN; n],
            after: VecDeque::new(),
            passed: vec![UNKNOWN; n],
            token: Bytes::new(),
            ready: false,
            ended: None,
            out: vec![None; n],
        }
    }
}

/// A ring with its rejoin rule.
type Ring = RingOf<Cluster>;

/// Where a member at positions `own` rejoins the ring (§2.4): the latest index `i` where every
/// part's position before entry `i` is at most the member's. The ring holds every commit of
/// a part after that position, so passing over those at or below `own` delivers each once.
/// `None` while some part's position before the front is unknown or past the member's.
fn rejoin_index(r: &Ring, own: &[u64]) -> Option<u64> {
    let n = r.partitions.len();
    if own.len() != n
        || r.front
            .iter()
            .zip(own)
            .any(|(&f, &o)| f == UNKNOWN || f > o)
    {
        return None;
    }
    let len = r.entries.len();
    let mut at = len;
    for (p, &o) in own.iter().enumerate() {
        // The entries' positions of `p` only rise once known (the front is known, so all
        // of them are), so the first after `o` is found by bisection.
        let (mut lo, mut hi) = (0usize, len);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if r.after[mid * n + p] <= o {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        // Entries `[0, lo)` end at or below `o`: the member may join after any of them.
        at = at.min(lo);
    }
    Some(r.base + at as u64)
}

/// A cluster feed hub.
pub(crate) type ClusterHub = HubOf<Cluster>;

/// What a hub is shared by (simp724's Q1): never the token's digest, which the dump would
/// show; [`TokenId`] names the token.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Key {
    tables: Vec<String>,
    prefix: Bytes,
    visible: bool,
    allow_partial: bool,
    every: u64,
    token: TokenId,
    scope: Scope,
    namespaces: Option<Vec<String>>,
}

impl Key {
    pub(crate) fn new(
        tables: &[String],
        prefix: &Bytes,
        visible: bool,
        allow_partial: bool,
        every: u64,
        caller: &Caller,
    ) -> Self {
        Self {
            tables: tables.to_vec(),
            prefix: prefix.clone(),
            visible,
            allow_partial,
            every,
            token: caller.token,
            scope: caller.scope,
            namespaces: caller.namespaces.clone(),
        }
    }
}

/// Why a member runs a merge of its own (§8, Q5: the dump counts each).
#[derive(Clone, Copy)]
enum Why {
    /// A start from a token, the oldest commit or with `skip_through`.
    Start,
    /// Fell off its hub's ring.
    Behind,
    /// No hub for its key: at `--max-feed-hubs`, or the key's hub covers other partitions.
    NoHub,
}

/// A node's cluster feed hubs, by key.
#[derive(Debug, Default)]
pub(crate) struct Hubs {
    hubs: Mutex<HashMap<Key, Weak<ClusterHub>>>,
    /// Live hubs: at most `--max-feed-hubs` (simp724's S1).
    live: Arc<AtomicU64>,
    stats: Arc<FeedStats>,
    /// Members' own merges running now, by [`Why`].
    own: [AtomicU64; 3],
}

impl Hubs {
    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<Key, Weak<ClusterHub>>> {
        self.hubs.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The live hub for `key` over `partitions`, if one is running.
    fn get(&self, key: &Key, partitions: &[String]) -> Option<Arc<ClusterHub>> {
        let hub = self.map().get(key).and_then(Weak::upgrade)?;
        let live = {
            let r = hub.ring();
            r.ended.is_none() && r.partitions == partitions
        };
        live.then_some(hub)
    }

    /// A place for a new hub under `max`, or `None` at the limit.
    fn place(&self, max: u64) -> bool {
        let mut n = self.live.load(Ordering::Relaxed);
        loop {
            if n >= max {
                return false;
            }
            match self
                .live
                .compare_exchange_weak(n, n + 1, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(now) => n = now,
            }
        }
    }

    /// The hub for `key` now: `hub`, or one that opened meanwhile (then `hub` is dropped).
    fn insert(&self, key: Key, hub: Arc<ClusterHub>) -> Arc<ClusterHub> {
        let mut map = self.map();
        if let Some(other) = map.get(&key).and_then(Weak::upgrade) {
            let partitions = hub.ring().partitions.clone();
            let live = {
                let r = other.ring();
                r.ended.is_none() && r.partitions == partitions
            };
            if live {
                return other;
            }
        }
        map.retain(|_, w| w.strong_count() > 0);
        map.insert(key, Arc::downgrade(&hub));
        hub
    }

    /// Takes `hub` out of the registry, if it's still `key`'s: before its end is visible, so
    /// a joiner racing the end opens a new hub (simp724's S3).
    fn remove(&self, key: &Key, hub: &ClusterHub) {
        let mut map = self.map();
        if map
            .get(key)
            .is_some_and(|w| std::ptr::eq(w.as_ptr(), std::ptr::from_ref(hub)))
        {
            map.remove(key);
        }
    }

    /// The live hubs, for the gauge and the dump.
    fn live_hubs(&self) -> Vec<(Key, Arc<ClusterHub>)> {
        self.map()
            .iter()
            .filter_map(|(k, w)| w.upgrade().map(|h| (k.clone(), h)))
            .collect()
    }

    /// The hubs and the members' own merges, for `phdb serve` on SIGUSR1 (§8, Q5). Keys show
    /// the token as `token #i` or `anonymous`, never its digest.
    #[cfg_attr(not(unix), allow(dead_code))]
    pub(crate) fn dump(&self, ring_bytes: usize) -> String {
        use std::fmt::Write as _;
        let hubs = self.live_hubs();
        let own = |w: Why| self.own[w as usize].load(Ordering::Relaxed);
        let st = &self.stats;
        let mut out = String::new();
        let _ = writeln!(
            out,
            "phdb serve: cluster feed dump: {} hubs; own merges: {} from their start, {} fallen \
             behind, {} with no hub; fell behind {}, rejoined {}, catch-up waits {}",
            hubs.len(),
            own(Why::Start),
            own(Why::Behind),
            own(Why::NoHub),
            st.fell_behind.load(Ordering::Relaxed),
            st.rejoined.load(Ordering::Relaxed),
            st.catch_up_waits.load(Ordering::Relaxed),
        );
        for (i, (key, hub)) in hubs.iter().enumerate() {
            let (entries, bytes, head, ended, partitions) = {
                let r = hub.ring();
                (
                    r.entries.len(),
                    r.bytes,
                    r.head(),
                    r.ended.is_some(),
                    r.partitions.clone(),
                )
            };
            let trackers = hub.trackers();
            let mut on_ring = 0usize;
            let mut own_merge = 0usize;
            let mut waiting = 0usize;
            for t in &trackers {
                match t.state.load(Ordering::Relaxed) {
                    ON_RING => on_ring += 1,
                    ON_OWN => own_merge += 1,
                    _ => waiting += 1,
                }
            }
            let _ = writeln!(
                out,
                "  cluster hub {i} (tables {:?}, prefix {} bytes, {}, {:?}, partitions \
                 {partitions:?}){}: ring {entries} entries to {head}, {bytes}/{} bytes; {} \
                 members: {on_ring} on the ring, {own_merge} on their own merge, {waiting} \
                 waiting for a catch-up slot",
                key.tables,
                key.prefix.len(),
                if key.visible { "visible" } else { "durable" },
                key.token,
                if ended { ", ENDED" } else { "" },
                hub.ring_limit(ring_bytes),
                hub.members(),
            );
        }
        out
    }
}

/// Counts something in a gauge while held.
struct Count(Arc<Metrics>, fn(&Metrics) -> &AtomicU64);

impl Count {
    fn new(metrics: &Arc<Metrics>, gauge: fn(&Metrics) -> &AtomicU64) -> Self {
        gauge(metrics).fetch_add(1, Ordering::Relaxed);
        Count(Arc::clone(metrics), gauge)
    }
}

impl Drop for Count {
    fn drop(&mut self) {
        (self.1)(&self.0).fetch_sub(1, Ordering::Relaxed);
    }
}

fn merges(m: &Metrics) -> &AtomicU64 {
    &m.cluster_merges
}

fn hub_members(m: &Metrics) -> &AtomicU64 {
    &m.cluster_hub_members
}

/// A member's own merge counted under its reason, while it runs.
struct OwnCount(Weak<super::Node>, usize);

impl OwnCount {
    fn new(node: &Arc<super::Node>, why: Why) -> Self {
        node.feed_hubs.own[why as usize].fetch_add(1, Ordering::Relaxed);
        OwnCount(Arc::downgrade(node), why as usize)
    }
}

impl Drop for OwnCount {
    fn drop(&mut self) {
        if let Some(node) = self.0.upgrade() {
            node.feed_hubs.own[self.1].fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// A member's place on its hub, counted in `cluster_hub_members` while held.
struct Joined {
    hub: JoinedOf<Cluster>,
    _count: Count,
}

impl Joined {
    fn new(hub: &Arc<ClusterHub>, tracker: &Arc<Tracker>, metrics: &Arc<Metrics>) -> Self {
        Joined {
            hub: hub.join(tracker, false),
            _count: Count::new(metrics, hub_members),
        }
    }
}

/// Each part's position in `merge` now ([`UNKNOWN`] for a part with none yet).
fn positions(merge: &Merge, out: &mut Vec<u64>) {
    out.clear();
    out.extend(merge.parts.iter().map(|p| position(&p.at)));
}

/// A component position's seqno: the D310 token's last seqno delivered (or passed over),
/// read from its fixed header ([`super::token`] reads the same layout).
fn position(at: &At) -> u64 {
    match at {
        At::Token(t) if t.len() >= 29 && matches!(t[0], 1..=3) => {
            u64::from_le_bytes(t[21..29].try_into().expect("8 bytes"))
        }
        _ => UNKNOWN,
    }
}

/// Opens `key`'s hub from `merge` (its components opened), its task spawned.
fn open_hub(
    node: &Arc<super::Node>,
    key: Key,
    merge: Merge,
    partitions: Vec<String>,
    service: &crate::serve::service::Service,
) -> Arc<ClusterHub> {
    let limits = service.feed_limits();
    let (quiet, ring_bytes) = (limits.quiet, limits.ring_bytes);
    let sharers = Arc::clone(&service.feed_shared().sharers);
    let metrics = &service.metrics;
    let hubs = &node.feed_hubs;
    let hub = Arc::new(ClusterHub::new(
        RingOf::new(ClusterRing::new(partitions)),
        tokio::runtime::Handle::current()
            .spawn(async {})
            .abort_handle(),
        false,
        Arc::clone(&hubs.stats),
        Arc::clone(&hubs.live),
        sharers,
    ));
    let merging = Merging::new(merge, None, quiet);
    let run = drive(
        Arc::downgrade(&hub),
        merging,
        key.clone(),
        Arc::downgrade(node),
        ring_bytes,
        Count::new(metrics, merges),
    );
    let task = match node.feed_runtime() {
        Some(rt) => rt.spawn(run),
        None => tokio::spawn(run),
    };
    hub.set_task(task.abort_handle());
    hubs.insert(key, hub)
}

/// A hub's task: its merge, read into its ring until the merge ends or the hub goes.
async fn drive(
    hub: Weak<ClusterHub>,
    mut merging: Merging,
    key: Key,
    node: Weak<super::Node>,
    ring_bytes: usize,
    _count: Count,
) {
    let mut at: Vec<u64> = Vec::new();
    let mut ready = false;
    loop {
        crate::serve::placement::refresh();
        let item = std::future::poll_fn(|cx| {
            let polled = merging.poll(cx);
            // Every component has said where it is (its announce): the hub's first checkpoint,
            // and joiners may take it (§2.3).
            if !ready && merging.merge.placed() {
                ready = true;
                if let Some(hub) = hub.upgrade() {
                    let token = Bytes::from(merging.merge.checkpoint_now());
                    positions(&merging.merge, &mut at);
                    let mut r = hub.ring();
                    r.token = token;
                    r.passed.clone_from(&at);
                    r.ready = true;
                    drop(r);
                    hub.head.send_modify(|_| {});
                }
            }
            polled
        })
        .await;
        let Some(hub) = hub.upgrade() else { return };
        match item {
            Some(Ok(out)) => push(&hub, out, &mut merging.merge, &mut at, ring_bytes),
            end => {
                let e = match end {
                    Some(Err(e)) => e,
                    _ => ApiError::internal("the subscription's merge ended"),
                };
                // Out of the registry first, then visible (simp724's S3).
                if let Some(node) = node.upgrade() {
                    node.feed_hubs.remove(&key, &hub);
                }
                hub.ring().ended = Some(e);
                hub.head.send_modify(|_| {});
                return;
            }
        }
        drop(hub);
        // A merge with a backlog is always ready: let the runtime's other tasks run.
        tokio::task::coop::consume_budget().await;
    }
}

/// Appends what the hub's merge produced: a commit or a state event as an entry; a quiet
/// checkpoint as the hub's position and token only.
fn push(hub: &ClusterHub, out: Out, merge: &mut Merge, at: &mut Vec<u64>, ring_bytes: usize) {
    positions(merge, at);
    let n = at.len();
    let entry = match out {
        Out::Spliced { body, tail } => {
            let Some((i, checkpoint)) = merge.last.take() else {
                return;
            };
            let token = if checkpoint {
                tail.slice(tail.len() - merge.token.len()..)
            } else {
                Bytes::new()
            };
            let seqno = at[i];
            ClusterEntry {
                bytes: ENTRY_OVERHEAD + body.len() + tail.len() + 8 * n,
                body,
                tail,
                token,
                commit: Some((i as u32, seqno)),
            }
        }
        Out::Event(e) if matches!(e.event, Some(proto::feed_event::Event::Progress(_))) => {
            let mut r = hub.ring();
            if !e.token.is_empty() {
                r.token = e.token;
            }
            r.passed.clone_from(at);
            return;
        }
        other => {
            let body = other.to_bytes();
            // A part's state: kept while it's out, for joiners.
            if let Out::Event(e) = &other
                && let Some(proto::feed_event::Event::PartitionState(st)) = &e.event
            {
                let mut r = hub.ring();
                if let Some(i) = r.partitions.iter().position(|p| *p == st.partition) {
                    r.out[i] = (!st.available).then(|| body.clone());
                }
            }
            ClusterEntry {
                bytes: ENTRY_OVERHEAD + body.len() + 8 * n,
                body,
                tail: Bytes::new(),
                token: Bytes::new(),
                commit: None,
            }
        }
    };
    let limit = hub.ring_limit(ring_bytes);
    let mut r = hub.ring();
    if !entry.token.is_empty() {
        r.token = entry.token.clone();
    }
    r.passed.clone_from(at);
    r.after.extend(at.iter().copied());
    r.push(Arc::new(entry), limit);
    let head = r.head();
    drop(r);
    hub.head.send_replace(head);
}

/// A cluster subscription's start (§2.3): on its key's hub, or on a merge of its own.
pub(super) struct Subscribe {
    pub(super) plan: Plan,
    pub(super) key: Key,
    pub(super) starts: BTreeMap<String, At>,
    pub(super) skips: HashMap<String, u64>,
    /// A `Now` start with no `skip_through`: it may share a hub.
    pub(super) shareable: bool,
    /// The subscription's `--max-streams` place.
    pub(super) slot: OwnedSemaphorePermit,
    /// The resume token, for a member's own merge's first position.
    pub(super) token: Bytes,
}

/// Starts a cluster subscription: its member task on the feed runtime, and its events.
pub(super) async fn subscribe(s: Subscribe) -> Result<FeedEvents, ApiError> {
    let Subscribe {
        plan,
        key,
        starts,
        skips,
        shareable,
        slot,
        token,
    } = s;
    let ctx = Arc::clone(&plan.ctx);
    let node = Arc::clone(&ctx.node);
    let service = Arc::clone(&ctx.service);
    let limits = service.feed_limits();
    let shared = service.feed_shared();
    let partitions = plan.partitions();
    let metrics = Arc::clone(&service.metrics);
    let tracker = Arc::new(Tracker::default());
    let n = partitions.len();
    // A `Now` start: its key's hub, opened if none and there's room (simp724's S1).
    // A `Now` start: its key's hub, opened if none and there's room (simp724's S1). A hub
    // found just as its merge ended is no use to a joiner with no head yet: it looks once
    // more, and opens a new one if none runs (simp724's S3 on #1884).
    let mut joined_head = None;
    for attempt in 0..2 {
        if !shareable {
            break;
        }
        let mut hub = node.feed_hubs.get(&key, &partitions);
        if hub.is_none() && node.feed_hubs.place(limits.max_hubs.max(1) as u64) {
            let mut merge = plan.merge(&starts, &skips, true);
            if let Err(e) = merge.open_all().await {
                node.feed_hubs.live.fetch_sub(1, Ordering::Relaxed);
                return Err(e);
            }
            hub = Some(open_hub(
                &node,
                key.clone(),
                merge,
                partitions.clone(),
                &service,
            ));
        }
        let Some(hub) = hub else { break };
        #[cfg(test)]
        hold(&HOLD_READY, node.dir()).await;
        let joined = Joined::new(&hub, &tracker, &metrics);
        match ready_head(&hub).await? {
            Ready::Head(head) => {
                joined_head = Some((joined, head));
                break;
            }
            Ready::Ended(_) if attempt == 0 => {}
            Ready::Ended(e) => return Err(e),
        }
    }
    let begin = match joined_head {
        Some((joined, head)) => {
            let Head {
                at,
                token,
                passed,
                out,
            } = head;
            #[cfg(test)]
            hold(&HOLD_JOIN, node.dir()).await;
            // Then each local partition's seqno (F5's order: head first): commits at or
            // below it predate the subscribe and are passed over.
            let mut own = Vec::with_capacity(n);
            for (p, partition) in partitions.iter().enumerate() {
                own.push(match local_seqno(&plan, partition)? {
                    Some(s) => s,
                    None if passed[p] != UNKNOWN => passed[p],
                    None => 0,
                });
            }
            Begin::Ring {
                joined,
                at,
                own,
                token,
                out,
            }
        }
        None => {
            let why = if shareable { Why::NoHub } else { Why::Start };
            let mut merge = plan.merge(&starts, &skips, false);
            merge.open_all().await?;
            // A start of its own rejoins its key's hub, if one runs.
            let joined = node
                .feed_hubs
                .get(&key, &partitions)
                .map(|h| Joined::new(&h, &tracker, &metrics));
            Begin::Own {
                joined,
                merging: Box::new(Merging::new(merge, None, limits.quiet)),
                count: (Count::new(&metrics, merges), OwnCount::new(&node, why)),
                own: vec![0; n],
                token,
            }
        }
    };
    let (sink, events, end) = Sink::new(&limits);
    let now = tokio::time::Instant::now();
    let member = Member {
        sink,
        plan,
        key,
        node: Arc::clone(&node),
        hub: None,
        own: Vec::new(),
        token: Bytes::new(),
        sent: Bytes::new(),
        last_event: now,
        waiting_since: now,
        quiet: limits.quiet,
        idle: limits.stream_idle,
        catch_up: Arc::clone(&shared.catch_up),
        metrics: Arc::clone(&metrics),
        tracker,
        partitions,
        started: std::time::Instant::now(),
        _slot: slot,
    };
    drop(start(member, begin, end, node.feed_runtime()));
    Ok(events)
}

/// Runs `member` from `begin` as a task on `runtime` (the feed runtime), or the caller's:
/// its error, if it fails, goes to `end`, after its last event.
fn start(
    member: Member,
    begin: Begin,
    end: tokio::sync::oneshot::Sender<ApiError>,
    runtime: Option<tokio::runtime::Handle>,
) -> tokio::task::JoinHandle<()> {
    let run = async move {
        match member.run(begin).await {
            Stop::Failed(e) => {
                let _ = end.send(e);
            }
            Stop::Idle | Stop::Gone => {}
        }
    };
    match runtime {
        Some(rt) => rt.spawn(run),
        None => tokio::spawn(run),
    }
}

/// A joiner's place: the ring's head, the hub's checkpoint and positions there, and the
/// state events of its parts that are out.
struct Head {
    at: u64,
    token: Bytes,
    passed: Vec<u64>,
    out: Vec<Bytes>,
}

/// What a joiner found on its hub: its place, or the hub's end.
enum Ready {
    Head(Head),
    Ended(ApiError),
}

/// The head of `hub`'s ring once its components have said where they are, with its
/// checkpoint and positions there, taken together (simp724's point on §2.3).
async fn ready_head(hub: &ClusterHub) -> Result<Ready, ApiError> {
    let mut head = hub.head.subscribe();
    let wait = async {
        loop {
            {
                let r = hub.ring();
                if let Some(e) = &r.ended {
                    return Ok(Ready::Ended(e.clone()));
                }
                if r.ready {
                    return Ok(Ready::Head(Head {
                        at: r.head(),
                        token: r.token.clone(),
                        passed: r.passed.clone(),
                        out: r.out.iter().flatten().cloned().collect(),
                    }));
                }
            }
            if head.changed().await.is_err() {
                return Err(ApiError::internal("the subscription's hub ended"));
            }
        }
    };
    tokio::time::timeout(HUB_START, wait).await.map_err(|_| {
        ApiError::partition_unavailable(
            "the subscription's partitions didn't say where they are in time",
        )
    })?
}

/// A hold seam (tests): the next `Now` joiner on each listed cluster directory waits after it
/// took the ring's head, before it reads its local partitions' seqnos, until the sender
/// sends or is dropped. One entry per directory, so tests running at once don't take each
/// other's.
#[cfg(test)]
pub(crate) static HOLD_JOIN: std::sync::Mutex<
    Vec<(std::path::PathBuf, tokio::sync::oneshot::Receiver<()>)>,
> = std::sync::Mutex::new(Vec::new());

/// A hold seam (tests): the next `Now` joiner on each listed cluster directory waits after it
/// found or opened its key's hub, before it joins it, as [`HOLD_JOIN`] does.
#[cfg(test)]
pub(crate) static HOLD_READY: std::sync::Mutex<
    Vec<(std::path::PathBuf, tokio::sync::oneshot::Receiver<()>)>,
> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
async fn hold(
    seam: &std::sync::Mutex<Vec<(std::path::PathBuf, tokio::sync::oneshot::Receiver<()>)>>,
    dir: &std::path::Path,
) {
    let wait = {
        let mut armed = seam.lock().unwrap_or_else(PoisonError::into_inner);
        armed
            .iter()
            .position(|(d, _)| d == dir)
            .map(|i| armed.swap_remove(i).1)
    };
    if let Some(wait) = wait {
        let _ = wait.await;
    }
}

/// `partition`'s seqno now, if it's served here: the map gives it to this node and it's open.
fn local_seqno(plan: &Plan, partition: &str) -> Result<Option<u64>, ApiError> {
    let ctx = &plan.ctx;
    let here = ctx
        .node
        .routes()
        .as_ref()
        .is_some_and(|r| r.owner(partition).is_some_and(|o| o.node == ctx.node.id()));
    if !here {
        return Ok(None);
    }
    let Some(service) = ctx.parts.load().get(partition).cloned() else {
        return Ok(None);
    };
    Ok(Some(service.part()?.feeds.seqno_now(ctx.visible)?))
}

/// Where a member starts.
enum Begin {
    /// On its hub's ring at `at`, with its own positions and the hub's checkpoint there.
    Ring {
        joined: Joined,
        at: u64,
        own: Vec<u64>,
        token: Bytes,
        /// Its parts' state events, while they're out: sent first.
        out: Vec<Bytes>,
    },
    /// On a merge of its own (opened, so a bad start failed the call), a member of its
    /// key's hub if one runs.
    Own {
        joined: Option<Joined>,
        merging: Box<Merging>,
        count: (Count, OwnCount),
        own: Vec<u64>,
        token: Bytes,
    },
}

/// Where a member reads.
enum Source {
    Ring(u64),
    /// Its own merge; with the `--max-catch-up` slot it holds when it fell behind.
    Own(
        Box<Merging>,
        Option<OwnedSemaphorePermit>,
        (Count, OwnCount),
    ),
}

/// Why a member's stream ended.
enum Stop {
    /// The client went away.
    Gone,
    /// The client left events untaken for its idle limit (counted and said already).
    Idle,
    Failed(ApiError),
}

/// A cluster subscription's member: its client's stream, fed from its hub's ring or its own
/// merge.
struct Member {
    sink: Sink,
    plan: Plan,
    /// Its key, to find its hub again when it has none running.
    key: Key,
    node: Arc<super::Node>,
    hub: Option<Joined>,
    /// Each part's position the member has covered: its commits at or below it are passed
    /// over (§2.4; for a `Now` joiner, a local partition's seqno at subscribe, F5).
    own: Vec<u64>,
    /// Its last checkpoint: what a resume from now starts from.
    token: Bytes,
    /// The token of the last event sent.
    sent: Bytes,
    last_event: tokio::time::Instant,
    /// When the client's buffer last went from empty to holding events (#1798 (a)).
    waiting_since: tokio::time::Instant,
    quiet: Duration,
    idle: Duration,
    catch_up: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    tracker: Arc<Tracker>,
    partitions: Vec<String>,
    started: std::time::Instant,
    /// The subscription's `--max-streams` place.
    _slot: OwnedSemaphorePermit,
}

/// What a member's wait ended with.
enum Woke<T> {
    Gone,
    Idle,
    Item(T),
    Tick,
}

impl Member {
    async fn run(mut self, begin: Begin) -> Stop {
        let mut source = match begin {
            Begin::Ring {
                joined,
                at,
                own,
                token,
                out,
            } => {
                self.hub = Some(joined);
                self.own = own;
                self.token = token;
                for state in out {
                    let event = Out::Spliced {
                        body: state,
                        tail: Bytes::new(),
                    };
                    if let Err(stop) = self.send(event, false).await {
                        return stop;
                    }
                }
                Source::Ring(at)
            }
            Begin::Own {
                joined,
                merging,
                count,
                own,
                token,
            } => {
                self.hub = joined;
                self.own = own;
                self.token = token.clone();
                self.sent = token;
                Source::Own(merging, None, count)
            }
        };
        loop {
            let next = match source {
                Source::Ring(at) => self.read_ring(at).await,
                Source::Own(merging, slot, count) => self.read_own(merging, slot, count).await,
            };
            source = match next {
                Ok(s) => s,
                Err(Stop::Idle) => {
                    drop_idle(&self.metrics, &self.partitions, self.started, self.idle);
                    return Stop::Idle;
                }
                Err(stop) => return stop,
            };
        }
    }

    fn hub(&self) -> Option<Arc<ClusterHub>> {
        self.hub.as_ref().map(|j| Arc::clone(&j.hub.0))
    }

    /// Waits for `item`, the client leaving, `tick`, or the idle limit while events wait in
    /// the client's buffer (#1798 (a)'s S1: on a quiet feed no send would notice).
    async fn wait<T>(
        &self,
        item: impl std::future::Future<Output = T>,
        tick: Option<tokio::time::Instant>,
    ) -> Woke<T> {
        let pending = self.sink.pending();
        let deadline = untaken_since(self.sink.taken(), self.waiting_since) + self.idle;
        tokio::select! {
            biased;
            () = self.sink.closed() => Woke::Gone,
            () = tokio::time::sleep_until(deadline), if pending => {
                if self.stalled() { Woke::Idle } else { Woke::Tick }
            }
            t = item => Woke::Item(t),
            () = sleep_until_opt(tick) => Woke::Tick,
        }
    }

    /// Whether the client has stopped reading: events wait in its buffer, and it took none
    /// for the idle limit.
    fn stalled(&self) -> bool {
        self.sink.pending()
            && untaken_since(self.sink.taken(), self.waiting_since) + self.idle
                <= tokio::time::Instant::now()
    }

    async fn send(&mut self, out: Out, checkpoint: bool) -> Result<(), Stop> {
        if !self.sink.pending() {
            self.waiting_since = tokio::time::Instant::now();
        }
        match self.sink.put(out).await {
            Ok(()) => {}
            Err(e) if e.code == "DeadlineExceeded" => return Err(Stop::Idle),
            Err(_) => return Err(Stop::Gone),
        }
        if checkpoint {
            self.sent = self.token.clone();
        }
        self.last_event = tokio::time::Instant::now();
        Ok(())
    }

    /// A progress event with its last checkpoint, if that's more than the client has.
    async fn progress(&mut self) -> Result<(), Stop> {
        if self.token.is_empty() || self.token == self.sent {
            self.last_event = tokio::time::Instant::now();
            return Ok(());
        }
        let event = proto::FeedEvent {
            token: self.token.clone(),
            event: Some(proto::feed_event::Event::Progress(proto::Progress {})),
            partition: String::new(),
        };
        self.send(Out::event(event), true).await
    }

    /// Raises its positions to `at`'s known ones: it has covered everything up to them.
    fn cover_to(&mut self, at: &[u64]) {
        for (o, &a) in self.own.iter_mut().zip(at) {
            if a != UNKNOWN && a > *o {
                *o = a;
            }
        }
    }

    /// Reads its hub's ring from `at` until it falls off the tail, or the hub ends.
    async fn read_ring(&mut self, mut at: u64) -> Result<Source, Stop> {
        let Some(hub) = self.hub() else {
            return Err(Stop::Failed(ApiError::internal(
                "a member on no hub's ring",
            )));
        };
        let n = self.own.len();
        let mut head = hub.head.subscribe();
        let mut batch: Vec<Arc<ClusterEntry>> = Vec::with_capacity(64);
        let mut after: Vec<u64> = Vec::with_capacity(64 * n);
        loop {
            crate::serve::placement::refresh();
            self.tracker.set(ON_RING, at);
            let (behind, ended, at_head) = {
                let r = hub.ring();
                if at < r.base {
                    (true, None, None)
                } else {
                    let from = ((at - r.base) as usize).min(r.entries.len());
                    let to = (from + 64).min(r.entries.len());
                    batch.extend(r.entries.range(from..to).cloned());
                    after.extend(r.after.range(from * n..to * n).copied());
                    let at_head =
                        (to == r.entries.len()).then(|| (r.token.clone(), r.passed.clone()));
                    (false, r.ended.clone(), at_head)
                }
            };
            if behind {
                return self.fall_behind().await;
            }
            if batch.is_empty() {
                if let Some(e) = ended {
                    // The hub's last checkpoint, then its error (simp724's R2 on the merge).
                    if let Some((token, passed)) = at_head {
                        self.cover_to(&passed);
                        if !token.is_empty() {
                            self.token = token;
                        }
                    }
                    self.progress().await?;
                    return Err(Stop::Failed(e));
                }
                let quiet = self.last_event + self.quiet;
                match self.wait(head.changed(), Some(quiet)).await {
                    Woke::Gone => return Err(Stop::Gone),
                    Woke::Idle => return Err(Stop::Idle),
                    Woke::Item(Ok(())) => {}
                    Woke::Item(Err(_)) => {
                        return Err(Stop::Failed(ApiError::internal(
                            "the subscription's hub ended",
                        )));
                    }
                    Woke::Tick => {
                        if tokio::time::Instant::now() >= quiet {
                            // At the head: the hub's position and checkpoint may have moved
                            // without an entry (a quiet checkpoint).
                            if let Some((token, passed)) = at_head {
                                self.cover_to(&passed);
                                if !token.is_empty() {
                                    self.token = token;
                                }
                            }
                            self.progress().await?;
                        }
                    }
                }
                continue;
            }
            for (k, e) in batch.drain(..).enumerate() {
                at += 1;
                let send = match e.commit {
                    Some((p, s)) => self.own.get(p as usize).is_none_or(|&o| s > o),
                    None => true,
                };
                let checkpoint = !e.token.is_empty();
                if checkpoint {
                    self.token = e.token.clone();
                }
                if send && !checkpoint && self.sent.is_empty() {
                    // Its first commit carries a checkpoint, as every subscription's does:
                    // its own, never ahead of this commit (so a resume repeats at most).
                    let mut tail = prost::bytes::BytesMut::with_capacity(e.tail.len() + 80);
                    tail.extend_from_slice(&e.tail);
                    super::feed::put_field(&mut tail, super::feed::TOKEN_FIELD, &self.token);
                    let out = Out::Spliced {
                        body: e.body.clone(),
                        tail: tail.freeze(),
                    };
                    self.send(out, true).await?;
                } else if send {
                    self.send(Out::Merged(Arc::clone(&e)), checkpoint).await?;
                }
                self.cover_to(&after[k * n..(k + 1) * n]);
            }
            after.clear();
        }
    }

    /// Off the ring's tail: a merge of its own from its last checkpoint, with a
    /// `--max-catch-up` slot, waiting for one if none is free (§8, S2).
    async fn fall_behind(&mut self) -> Result<Source, Stop> {
        self.node
            .feed_hubs
            .stats
            .fell_behind
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .cluster_fell_behind
            .fetch_add(1, Ordering::Relaxed);
        let slot = self.catch_up_slot().await?;
        let starts = self.plan.starts_from(&self.token).map_err(Stop::Failed)?;
        let mut merge = self.plan.merge(&starts, &HashMap::new(), false);
        merge.open_all().await.map_err(Stop::Failed)?;
        Ok(Source::Own(
            Box::new(Merging::new(merge, None, self.quiet)),
            Some(slot),
            (
                Count::new(&self.metrics, merges),
                OwnCount::new(&self.node, Why::Behind),
            ),
        ))
    }

    async fn catch_up_slot(&mut self) -> Result<OwnedSemaphorePermit, Stop> {
        if let Ok(p) = Arc::clone(&self.catch_up).try_acquire_owned() {
            return Ok(p);
        }
        self.node
            .feed_hubs
            .stats
            .catch_up_waits
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .cluster_catch_up_waits
            .fetch_add(1, Ordering::Relaxed);
        self.tracker.set(WAITING, 0);
        let sem = Arc::clone(&self.catch_up);
        tokio::select! {
            () = self.sink.closed() => Err(Stop::Gone),
            slot = sem.acquire_owned() => slot.map_err(|_| {
                Stop::Failed(ApiError::internal("the catch-up slots closed"))
            }),
        }
    }

    /// Reads its own merge until it can rejoin its hub's ring.
    async fn read_own(
        &mut self,
        mut merging: Box<Merging>,
        mut slot: Option<OwnedSemaphorePermit>,
        count: (Count, OwnCount),
    ) -> Result<Source, Stop> {
        let mut at: Vec<u64> = Vec::new();
        let mut hub_retry: Option<tokio::time::Instant> = None;
        loop {
            crate::serve::placement::refresh();
            self.tracker
                .set(ON_OWN, self.own.first().copied().unwrap_or(0));
            // No running hub: look for the key's at most once a second (cli692's S1 on
            // #1884). Until there's one to rejoin, the merge is an unshared one, like a key's
            // past the hub limit: it gives its --max-catch-up slot back.
            if !self.hub_runs() && hub_retry.is_none_or(|t| t.elapsed() >= HUB_RETRY) {
                hub_retry = Some(tokio::time::Instant::now());
                if let Some(h) = self.node.feed_hubs.get(&self.key, &self.partitions) {
                    self.hub = Some(Joined::new(&h, &self.tracker, &self.metrics));
                } else {
                    self.hub = None;
                    drop(slot.take());
                }
            }
            if let Some(i) = self.rejoin() {
                self.node
                    .feed_hubs
                    .stats
                    .rejoined
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .cluster_rejoined
                    .fetch_add(1, Ordering::Relaxed);
                drop((merging, slot, count));
                return Ok(Source::Ring(i));
            }
            let tick = tokio::time::Instant::now() + REJOIN_TICK;
            let item = match self
                .wait(std::future::poll_fn(|cx| merging.poll(cx)), Some(tick))
                .await
            {
                Woke::Gone => return Err(Stop::Gone),
                Woke::Idle => return Err(Stop::Idle),
                Woke::Tick => continue,
                Woke::Item(item) => item,
            };
            let out = match item {
                Some(Ok(out)) => out,
                Some(Err(e)) => return Err(Stop::Failed(e)),
                None => {
                    return Err(Stop::Failed(ApiError::internal(
                        "the subscription's merge ended",
                    )));
                }
            };
            let merge = &mut merging.merge;
            positions(merge, &mut at);
            match out {
                Out::Spliced { .. } => {
                    let Some((i, checkpoint)) = merge.last.take() else {
                        continue;
                    };
                    if checkpoint {
                        self.token = Bytes::copy_from_slice(&merge.token);
                    }
                    if self.own.get(i).is_none_or(|&o| at[i] > o) {
                        self.send(out, checkpoint).await?;
                    }
                }
                Out::Event(e) if matches!(e.event, Some(proto::feed_event::Event::Progress(_))) => {
                    self.token = e.token.clone();
                    if self.token != self.sent {
                        self.send(Out::Event(e), true).await?;
                    }
                }
                other => self.send(other, false).await?,
            }
            self.cover_to(&at);
        }
    }

    /// Whether its hub is there and hasn't ended.
    fn hub_runs(&self) -> bool {
        self.hub().is_some_and(|h| h.ring().ended.is_none())
    }

    /// Where it rejoins its hub's ring now, if it can: the hub runs, covers its partitions,
    /// and the member has a checkpoint to fall back to.
    fn rejoin(&self) -> Option<u64> {
        let hub = self.hub()?;
        if self.token.is_empty() {
            return None;
        }
        let r = hub.ring();
        if !r.ready || r.ended.is_some() || r.partitions != self.partitions {
            return None;
        }
        rejoin_index(&r, &self.own)
    }
}

/// Sleeps until `at`, or forever.
async fn sleep_until_opt(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use super::*;
    use crate::serve::cluster::feed::tests::{
        QUIET, commit_of, event, fed, merging, paused, plan_of,
    };

    /// A ring over partitions `p0`, `p1`, starting at `front`.
    fn ring(front: [u64; 2]) -> Ring {
        let mut r: Ring = RingOf::new(ClusterRing::new(vec!["p0".into(), "p1".into()]));
        r.front = front.to_vec();
        r
    }

    /// Appends a commit of part `p` at `seqno`, the positions after it `after`.
    fn push(r: &mut Ring, p: u32, seqno: u64, after: [u64; 2], limit: usize) {
        r.after.extend(after);
        r.push(
            Arc::new(ClusterEntry {
                body: Bytes::new(),
                tail: Bytes::new(),
                token: Bytes::new(),
                commit: Some((p, seqno)),
                bytes: 100,
            }),
            limit,
        );
    }

    /// What a member at `own` delivers from `r` if it rejoins: each entry from the rejoin
    /// index whose commit is past its position.
    fn delivered(r: &Ring, own: [u64; 2]) -> Option<Vec<(u32, u64)>> {
        let at = rejoin_index(r, &own)?;
        Some(
            r.entries
                .range((at - r.base) as usize..)
                .filter_map(|e| e.commit)
                .filter(|&(p, s)| s > own[p as usize])
                .collect(),
        )
    }

    #[test]
    fn a_member_rejoins_where_the_ring_delivers_every_commit_past_it_once() {
        // Two partitions, interleaved, from positions 10 and 20.
        let mut r = ring([10, 20]);
        let commits = [(0, 11), (1, 21), (0, 12), (1, 25), (1, 26), (0, 15)];
        let mut at = [10, 20];
        for &(p, s) in &commits {
            at[p as usize] = s;
            push(&mut r, p, s, at, usize::MAX);
        }
        // Every position the ring covers, against what it must deliver: each commit past
        // the member's own position, once (a commit at exactly it included, §6).
        for o0 in 10..=16 {
            for o1 in 20..=27 {
                let want: Vec<(u32, u64)> = commits
                    .iter()
                    .copied()
                    .filter(|&(p, s)| s > [o0, o1][p as usize])
                    .collect();
                assert_eq!(delivered(&r, [o0, o1]), Some(want), "own [{o0}, {o1}]");
            }
        }
        // Behind the front: the ring can't cover it.
        assert_eq!(rejoin_index(&r, &[9, 20]), None);
        assert_eq!(rejoin_index(&r, &[10, 19]), None);
        // The newest positions: at the head.
        assert_eq!(rejoin_index(&r, &[99, 99]), Some(r.head()));
        // Evicting moves the front to the positions after what went.
        // Two entries of 100 bytes fit under 200: the five before them go.
        push(&mut r, 1, 30, [15, 30], 200);
        assert_eq!(r.base, 5);
        assert_eq!(r.front, vec![12, 26]);
        assert_eq!(rejoin_index(&r, &[11, 26]), None, "p0's 12 was evicted");
        assert_eq!(delivered(&r, [12, 26]), Some(vec![(0, 15), (1, 30)]));
        // A part whose position isn't known yet: no rejoin.
        let mut r = ring([10, UNKNOWN]);
        push(&mut r, 0, 11, [11, UNKNOWN], usize::MAX);
        assert_eq!(rejoin_index(&r, &[10, 99]), None);
    }

    fn caller(token: TokenId, scope: Scope, namespaces: Option<Vec<String>>) -> Caller {
        let mut c = Caller::anonymous();
        c.token = token;
        c.scope = scope;
        c.namespaces = namespaces;
        c
    }

    #[test]
    fn a_hub_is_one_tokens_and_one_requests() {
        // simp724's Q1: a hub's components are authorized as the caller who opened them, so
        // a key never spans two tokens, scopes or namespace sets.
        let key = |c: &Caller, prefix: &[u8], every: u64| {
            Key::new(
                &["t".into()],
                &Bytes::copy_from_slice(prefix),
                false,
                false,
                every,
                c,
            )
        };
        let a = caller(TokenId::ANONYMOUS, Scope::Read, None);
        assert_eq!(key(&a, b"", 0), key(&a.clone(), b"", 0));
        let mut c = Caller::anonymous();
        c.scope = Scope::Read;
        for other in [
            caller(TokenId::ANONYMOUS, Scope::Write, None),
            caller(TokenId::ANONYMOUS, Scope::Read, Some(vec!["n".into()])),
        ] {
            assert_ne!(key(&a, b"", 0), key(&other, b"", 0));
        }
        assert_ne!(key(&a, b"", 0), key(&a, b"x", 0), "another prefix");
        assert_ne!(key(&a, b"", 0), key(&a, b"", 5), "another checkpoint_every");
        // Two configured tokens: two keys, and neither shows a digest.
        let tokens = crate::serve::auth::Tokens::parse("read one\nread two\n", "t").unwrap();
        let (one, two) = (tokens.caller("one").unwrap(), tokens.caller("two").unwrap());
        assert_ne!(key(&one, b"", 0), key(&two, b"", 0));
        assert_eq!(format!("{:?}", one.token), "token #0");
        assert!(!format!("{:?}", key(&two, b"", 0)).contains(&hex(two.principal.digest())));
    }

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    #[test]
    fn the_registry_never_hands_out_an_ended_removed_or_other_partitions_hub() {
        let rt = paused();
        rt.block_on(async {
            let hubs = Hubs::default();
            let parts = vec!["p0".to_string()];
            let hub = |parts: &[String]| {
                Arc::new(ClusterHub::new(
                    RingOf::new(ClusterRing::new(parts.to_vec())),
                    tokio::spawn(async {}).abort_handle(),
                    false,
                    Arc::default(),
                    Arc::clone(&hubs.live),
                    Arc::default(),
                ))
            };
            let key = Key::new(
                &["t".into()],
                &Bytes::new(),
                false,
                false,
                0,
                &Caller::anonymous(),
            );
            let h = hubs.insert(key.clone(), hub(&parts));
            assert!(hubs.get(&key, &parts).is_some());
            assert!(
                hubs.get(&key, &["p0".into(), "p1".into()]).is_none(),
                "a newer map's partitions"
            );
            // Out of the registry before its end is visible (simp724's S3): a joiner then
            // opens a new hub, though this one hasn't ended yet.
            hubs.remove(&key, &h);
            assert!(hubs.get(&key, &parts).is_none());
            let h2 = hubs.insert(key.clone(), hub(&parts));
            assert!(!Arc::ptr_eq(&h, &h2));
            // An ended hub still in the map isn't joined either.
            h2.ring().ended = Some(ApiError::internal("over"));
            assert!(hubs.get(&key, &parts).is_none());
            // Removing a hub that's no longer the key's leaves the key's alone.
            let h3 = hubs.insert(key.clone(), hub(&parts));
            hubs.remove(&key, &h);
            assert!(hubs.get(&key, &parts).is_some_and(|h| Arc::ptr_eq(&h, &h3)));
        });
    }

    /// A member on a merge of its own (`m`), as a start or a key with no hub runs one: the
    /// client's stream, its metrics, the task, and its `--max-streams` place's semaphore.
    struct Task {
        events: FeedEvents,
        metrics: Arc<Metrics>,
        handle: tokio::task::JoinHandle<()>,
        slots: Arc<Semaphore>,
    }

    fn task(m: Merging, idle: Duration) -> Task {
        let plan = plan_of(&m);
        let node = Arc::clone(&plan.ctx.node);
        let limits = crate::serve::feed::FeedLimits {
            ring_bytes: 1 << 20,
            max_hubs: 4,
            max_catch_up: 1,
            quiet: QUIET,
            stream_idle: idle,
            client_events: crate::serve::feed::CLIENT_EVENTS,
            client_bytes: crate::serve::feed::CLIENT_BYTES,
            solo_after: crate::serve::feed::SOLO_AFTER,
        };
        let (sink, events, end) = Sink::new(&limits);
        let metrics = Arc::new(Metrics::default());
        let slots = Arc::new(Semaphore::new(1));
        let now = tokio::time::Instant::now();
        let n = m.merge.parts.len();
        let member = Member {
            sink,
            plan,
            key: test_key(),
            node: Arc::clone(&node),
            hub: None,
            own: Vec::new(),
            token: Bytes::new(),
            sent: Bytes::new(),
            last_event: now,
            waiting_since: now,
            quiet: QUIET,
            idle,
            catch_up: Arc::new(Semaphore::new(1)),
            metrics: Arc::clone(&metrics),
            tracker: Arc::default(),
            partitions: vec!["p0".into()],
            started: std::time::Instant::now(),
            _slot: Arc::clone(&slots).try_acquire_owned().unwrap(),
        };
        let begin = Begin::Own {
            joined: None,
            merging: Box::new(m),
            count: (
                Count::new(&metrics, merges),
                OwnCount::new(&node, Why::Start),
            ),
            own: vec![0; n],
            token: Bytes::new(),
        };
        Task {
            events,
            metrics,
            handle: start(member, begin, end, None),
            slots,
        }
    }

    /// The key the member tests' merges run under.
    fn test_key() -> Key {
        Key::new(
            &["users".into()],
            &Bytes::new(),
            false,
            true,
            0,
            &Caller::anonymous(),
        )
    }

    #[test]
    fn a_member_on_its_own_merge_gives_its_slot_back_while_no_hub_runs_and_finds_a_new_one() {
        // cli692's S1 on #1884: a member that fell behind holds a --max-catch-up slot for
        // its own merge. If its hub then ends, it couldn't rejoin it, and held the slot
        // (shared with single-node streams) until its client left. It looks for the key's
        // hub once a second, and gives the slot back while none runs.
        paused().block_on(async {
            let (_p0, events) = fed();
            let m = merging(vec![Some(events)], QUIET);
            let plan = plan_of(&m);
            let node = Arc::clone(&plan.ctx.node);
            let parts = vec!["p0".to_string()];
            let hub = |hubs: &Hubs| {
                Arc::new(ClusterHub::new(
                    RingOf::new(ClusterRing::new(parts.clone())),
                    tokio::spawn(async {}).abort_handle(),
                    false,
                    Arc::default(),
                    Arc::clone(&hubs.live),
                    Arc::default(),
                ))
            };
            let ended = hub(&node.feed_hubs);
            ended.ring().ended = Some(ApiError::internal("over"));
            let catch_up = Arc::new(Semaphore::new(1));
            let limits = crate::serve::feed::FeedLimits {
                ring_bytes: 1 << 20,
                max_hubs: 4,
                max_catch_up: 1,
                quiet: QUIET,
                stream_idle: Duration::from_secs(30),
                client_events: crate::serve::feed::CLIENT_EVENTS,
                client_bytes: crate::serve::feed::CLIENT_BYTES,
                solo_after: crate::serve::feed::SOLO_AFTER,
            };
            let (sink, _events, _end) = Sink::new(&limits);
            let metrics = Arc::new(Metrics::default());
            let tracker: Arc<Tracker> = Arc::default();
            let now = tokio::time::Instant::now();
            let mut member = Member {
                sink,
                plan,
                key: test_key(),
                node: Arc::clone(&node),
                hub: Some(Joined::new(&ended, &tracker, &metrics)),
                own: vec![0],
                token: Bytes::new(),
                sent: Bytes::new(),
                last_event: now,
                waiting_since: now,
                quiet: QUIET,
                idle: Duration::from_secs(30),
                catch_up: Arc::clone(&catch_up),
                metrics: Arc::clone(&metrics),
                tracker,
                partitions: parts.clone(),
                started: std::time::Instant::now(),
                _slot: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            };
            let slot = Arc::clone(&catch_up).try_acquire_owned().unwrap();
            let count = (
                Count::new(&metrics, merges),
                OwnCount::new(&node, Why::Behind),
            );
            let run = tokio::spawn(async move {
                let _ = member.read_own(Box::new(m), Some(slot), count).await;
            });
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(catch_up.available_permits(), 1, "its slot is back");
            // A hub for its key opens: it joins it, to rejoin once it can.
            let live = node.feed_hubs.insert(test_key(), hub(&node.feed_hubs));
            tokio::time::sleep(HUB_RETRY * 2).await;
            assert_eq!(live.members(), 1, "it found the key's new hub");
            assert_eq!(ended.members(), 0, "and left the ended one");
            run.abort();
        });
    }

    /// The client's next item, within `d` of virtual time.
    async fn take(e: &mut FeedEvents, d: Duration) -> Option<Option<Result<Out, ApiError>>> {
        tokio::time::timeout(d, std::future::poll_fn(|cx| Pin::new(&mut *e).poll_out(cx)))
            .await
            .ok()
    }

    fn load(n: &AtomicU64) -> u64 {
        n.load(Ordering::Relaxed)
    }

    #[test]
    fn a_client_that_leaves_events_untaken_is_ended_at_the_limit_and_one_with_none_is_not() {
        // simp724's S1 on the #1798 (a) design, kept by B's members: on a quiet feed nothing
        // waits for room, so a send never notices a stalled client; the member's own timer
        // must. A client that never reads loses its subscription (its slot, its merge) at
        // the limit, counted once. One with nothing waiting for it is waiting for a commit,
        // and stays.
        const LIMIT: Duration = Duration::from_millis(400);
        paused().block_on(async {
            let (p0, events) = fed();
            let t = task(merging(vec![Some(events)], QUIET), LIMIT);
            p0.send(Ok(event(b"t1", Some(1)))).unwrap();
            // Never taken.
            tokio::time::sleep(LIMIT / 2).await;
            assert!(!t.handle.is_finished(), "not yet: half the limit");
            tokio::time::sleep(LIMIT).await;
            assert!(t.handle.is_finished(), "ended at the limit");
            assert_eq!(load(&t.metrics.feed_idle_drops), 1);
            assert_eq!(
                t.slots.available_permits(),
                1,
                "its --max-streams place is free"
            );
            assert_eq!(load(&t.metrics.cluster_merges), 0);
            drop(t.events);

            // Nothing sent: ten limits later it's still there, never counted.
            let (_p1, events) = fed();
            let t = task(merging(vec![Some(events)], QUIET), LIMIT);
            tokio::time::sleep(LIMIT * 10).await;
            assert!(
                !t.handle.is_finished(),
                "a client with nothing waiting stays"
            );
            assert_eq!(load(&t.metrics.feed_idle_drops), 0);
        });
    }

    #[test]
    fn the_first_event_after_a_quiet_spell_isnt_counted_as_untaken_for_the_spell() {
        // Found building #1798 (a): the limit counted from the last take alone, so the first
        // event after a quiet spell longer than the limit looked untaken for all of it, and
        // a client reading along was ended. It counts from that event's arrival.
        const LIMIT: Duration = Duration::from_millis(400);
        paused().block_on(async {
            let (p0, events) = fed();
            let mut t = task(merging(vec![Some(events)], QUIET), LIMIT);
            p0.send(Ok(event(b"t1", Some(1)))).unwrap();
            commit_of(take(&mut t.events, Duration::from_millis(10)).await);
            tokio::time::sleep(LIMIT * 5).await;
            p0.send(Ok(event(b"t2", Some(2)))).unwrap();
            assert_eq!(
                commit_of(take(&mut t.events, LIMIT / 4).await),
                ("p0".into(), 2)
            );
            assert!(!t.handle.is_finished());
            assert_eq!(load(&t.metrics.feed_idle_drops), 0);
        });
    }

    #[test]
    fn the_last_checkpoint_comes_before_the_error() {
        // simp724's S2 on (a): the error goes through the stream's end, read only once every
        // event is taken, so it never overtakes the checkpoint before it. A part failing
        // without `allow_partial` after more than a buffer's worth of commits.
        paused().block_on(async {
            let (p0, events) = fed();
            let mut m = merging(vec![Some(events)], QUIET);
            m.merge.allow_partial = false;
            let mut t = task(m, Duration::from_secs(30));
            let n = crate::serve::feed::CLIENT_EVENTS as u64 + 6;
            for i in 1..=n {
                p0.send(Ok(event(b"t", Some(i)))).unwrap();
            }
            // The position moves past the last commit, then the part fails.
            p0.send(Ok(event(b"later", None))).unwrap();
            p0.send(Err(ApiError::invalid("the part's own refusal")))
                .unwrap();
            let mut seen = Vec::new();
            while let Some(item) = take(&mut t.events, Duration::from_secs(1)).await {
                match item {
                    None => break,
                    Some(r) => seen.push(r),
                }
            }
            let commits = seen
                .iter()
                .filter(|r| matches!(r, Ok(Out::Spliced { .. })))
                .count();
            assert_eq!(commits as u64, n, "every commit");
            let k = seen.len();
            assert!(
                matches!(&seen[k - 1], Err(e) if e.message.contains("refusal")),
                "the error last"
            );
            let last = match &seen[k - 2] {
                Ok(o) => o.to_bytes(),
                Err(e) => panic!("two errors: {e:?}"),
            };
            let e = <proto::FeedEvent as prost::Message>::decode(last).unwrap();
            assert!(
                matches!(e.event, Some(proto::feed_event::Event::Progress(_)))
                    && !e.token.is_empty(),
                "a checkpoint right before it: {e:?}"
            );
        });
    }

    #[test]
    fn a_client_gone_ends_a_quiet_merge_at_once() {
        // simp724's S3 on (a): a quiet member waits on its merge, not in a send, so it must
        // notice the client going there too, not at its next send or tick.
        paused().block_on(async {
            let (_p0, events) = fed();
            let t = task(merging(vec![Some(events)], QUIET), Duration::from_secs(30));
            tokio::task::yield_now().await;
            assert_eq!(load(&t.metrics.cluster_merges), 1);
            drop(t.events);
            tokio::time::timeout(Duration::from_millis(1), t.handle)
                .await
                .expect("ended without waiting for any timer")
                .unwrap();
            assert_eq!(load(&t.metrics.cluster_merges), 0);
        });
    }

    #[test]
    fn everything_ready_arrives_in_order_with_none_stranded() {
        // (a)'s S4: commits all ready at once, more than the client's buffer holds, all
        // arrive in order, none left behind with no wake registered (no further event or
        // timer here would wake the member).
        paused().block_on(async {
            let (p0, events) = fed();
            let mut t = task(merging(vec![Some(events)], QUIET), Duration::from_secs(30));
            let n = 3 * crate::serve::feed::CLIENT_EVENTS as u64 + 5;
            for i in 1..=n {
                p0.send(Ok(event(b"t", Some(i)))).unwrap();
            }
            let mut got = Vec::new();
            while got.len() < n as usize {
                got.push(commit_of(take(&mut t.events, Duration::from_secs(1)).await).1);
            }
            assert_eq!(
                got,
                (1..=n).collect::<Vec<_>>(),
                "all, in order, none stranded"
            );
        });
    }
}
