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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_core::Stream;
use pigeonhole::nonblocking::FeedStream;
use pigeonhole::{Commit, FeedToken, Snapshot, Table};
use prost::bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};

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
    /// Events a client's connection may hold untaken ([`CLIENT_EVENTS`]), and their encoded
    /// bytes ([`CLIENT_BYTES`]): the per-client buffer.
    pub client_events: usize,
    pub client_bytes: usize,
    /// How long a hub on the hub runtime must have had at most one client before it moves
    /// back to the feed runtime ([`SOLO_AFTER`]; #1689).
    pub solo_after: Duration,
}

/// How long a hub must have had at most one client before its task moves back to the feed
/// runtime (#1689): a client that joins and leaves doesn't bounce it on every join.
pub(crate) const SOLO_AFTER: Duration = Duration::from_secs(1);

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
    /// A cluster component resumed with `skip_through` (D317 ticket 7): commits after the
    /// start up to this seqno are passed over, as already handled, at most [`SKIP_LIMIT`] of
    /// them that would have been sent; 0: none.
    pub skip: u64,
    /// Send the start's position as a first progress event, before any commit: a cluster
    /// subscription's part (D317 ticket 7) then knows where it is from the start, so it can
    /// resume after a break that came before any event, with nothing skipped.
    pub announce: bool,
}

/// At most this many commits a client would have been sent are passed over for `skip`: a
/// cluster subscription's checkpoints are at most this many events apart, so a client can't
/// have handled more after one (perf287's cap on ticket 7). More is `InvalidArgument`, never
/// a silent gap.
pub(crate) const SKIP_LIMIT: u64 = 64;

/// Where a client is, for the feed dump ([`Feeds::dump`], `phdb serve` on SIGUSR1): two
/// relaxed stores per batch, read only by the dump.
#[derive(Debug, Default)]
struct Tracker {
    /// [`ON_RING`], [`ON_OWN`] or [`WAITING`].
    state: AtomicU64,
    /// On the ring, its next index; on its own stream, the seqno it covered.
    at: AtomicU64,
    /// The hub it's counted under (its address): a client that moved to a failed hub's
    /// replacement is counted there only (perf287's N1 on #1643).
    hub: AtomicUsize,
}

const ON_RING: u64 = 0;
const ON_OWN: u64 = 1;
/// Waiting for a `--max-catch-up` slot, delivering nothing.
const WAITING: u64 = 2;

impl Tracker {
    fn set(&self, state: u64, at: u64) {
        self.state.store(state, Ordering::Relaxed);
        self.at.store(at, Ordering::Relaxed);
    }
}

/// When a client last took an event, for a send waiting for room: its `--stream-idle-ms`
/// counts from the last take (simp724's I1 on #1536). A take stores the time; it wakes no
/// one. A waiting send's one timer, when it fires, reads it and waits on if the client took
/// something meanwhile (perf287's profile on #1397: a wake and a new timer per take, per
/// subscriber, were `Client::send`'s 9.6% self at 16 hubs × 16).
#[derive(Debug)]
pub(crate) struct TakeClock {
    /// On Tokio's clock, so a paused test clock pauses it too.
    base: tokio::time::Instant,
    /// Nanoseconds from `base` to the last take; 0 before the first.
    last: AtomicU64,
}

impl TakeClock {
    pub(crate) fn new() -> Self {
        Self {
            base: tokio::time::Instant::now(),
            last: AtomicU64::new(0),
        }
    }

    /// A take, now.
    pub(crate) fn took(&self) {
        let nanos = u64::try_from(self.base.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.last.store(nanos.max(1), Ordering::Relaxed);
    }

    /// The last take, or the clock's start (the stream's) before the first: what an idle
    /// limit counts from (#1798 (a)'s merge task).
    pub(crate) fn last_or_start(&self) -> tokio::time::Instant {
        self.last().unwrap_or(self.base)
    }

    /// When the last take was, or `None` before the first.
    pub(crate) fn last(&self) -> Option<tokio::time::Instant> {
        match self.last.load(Ordering::Relaxed) {
            0 => None,
            n => Some(self.base + Duration::from_nanos(n)),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct HubKey {
    /// The tables' feed ids, so a table dropped and created again under its name is another
    /// key (feed1248's R2, simp724's F4 on #1518).
    tables: Vec<u64>,
    visible: bool,
}

/// One commit in a hub's ring: its encoded event and little else, so a ring's share of
/// `--feed-ring-bytes` holds as many commits as it can (#1397's 16 hubs × 16, where each
/// hub's share is a sixteenth). The decoded commit isn't kept: a client with a row prefix
/// filters the encoded event's changes instead ([`Entry::for_prefix`]).
pub(crate) struct Entry {
    /// The commit's seqno.
    seqno: u64,
    /// The hub feed's token right after this commit: a slice of `encoded` when there is
    /// one (its first field), so it's held once.
    token: Bytes,
    /// The hub feed's position just before it (`Feed::position`): every commit of the spec
    /// at or below it came before this one.
    prev: u64,
    /// Its share of `--feed-ring-bytes`: `encoded` (the token included) or the token, and a
    /// fixed overhead.
    bytes: usize,
    /// The commit as a client with no prefix gets it, encoded once for every such client on
    /// the ring: `None` if it has no change to send. Encoded by the hub, before the entry is
    /// pushed, so no client waits on another's encode (perf287's P2 on #1541).
    encoded: Option<Bytes>,
}

/// An entry's size beside its bytes: the struct (88), the `Arc`'s counts (16) and the ring's
/// slot (8) (perf287 on #1655).
const ENTRY_OVERHEAD: usize = 112;

impl Entry {
    /// An entry for `commit`, encoded for clients with no prefix and sized with that
    /// encoding (simp724's R1, perf287's P1 on #1541); `prev` is set when it's pushed.
    fn new(commit: &Commit, token: Bytes) -> Self {
        let encoded: Option<Bytes> = filtered(commit, b"")
            .map(|c| prost::Message::encode_to_vec(&commit_event(token.clone(), c)).into());
        // The token is the event's field 1, which prost writes first: its tag, its length,
        // then its bytes. Kept as that slice, it isn't held twice.
        let token = match &encoded {
            Some(e) => {
                let at = 1 + prost::encoding::encoded_len_varint(token.len() as u64);
                match e.get(at..at + token.len()) {
                    Some(b) if !token.is_empty() && b == &token[..] => {
                        e.slice(at..at + token.len())
                    }
                    _ => token,
                }
            }
            None => token,
        };
        let bytes = ENTRY_OVERHEAD + encoded.as_ref().map_or(token.len(), Bytes::len);
        Entry {
            seqno: commit.seqno(),
            token,
            prev: 0,
            bytes,
            encoded,
        }
    }

    /// The event a client with `prefix` sends for this entry: the commit's changes to rows
    /// starting with it, or `None` if there are none. Decoded from `encoded`, in the order a
    /// filter of the commit gives them.
    fn for_prefix(&self, prefix: &[u8]) -> Option<proto::FeedEvent> {
        let bytes = self.encoded.as_ref()?;
        // From the `Bytes` itself: protogen's `bytes(["."])` makes the row, qualifier, value
        // and token fields slices of it, not copies (perf287 on #1655).
        let mut event = <proto::FeedEvent as prost::Message>::decode(bytes.clone()).ok()?;
        let Some(proto::feed_event::Event::Commit(c)) = &mut event.event else {
            return None;
        };
        c.changes.retain(|ch| ch.row.starts_with(prefix));
        (!c.changes.is_empty()).then_some(event)
    }

    /// The `FeedEvent` a client with no prefix sends for this commit, encoded.
    fn encoded(&self) -> Option<&Bytes> {
        self.encoded.as_ref()
    }
}

/// How an entry's changes stand against a client's row prefix ([`rows`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rows {
    /// Every change is to a matching row: the client's event is the shared one.
    All,
    /// No change is: nothing to send.
    None,
    /// Some are, or the encoding isn't one the walk reads: decode and filter it.
    Mixed,
}

/// Where an encoded `FeedEvent`'s changes' rows stand against `prefix` (#1780), read off
/// the encoding with no allocation: the commit's changes' `row` fields only, every other
/// field stepped over by its wire type. A client whose prefix matches every row (perf287's
/// g90: `w0` over writers `w000`-`w015`) then sends the shared encoding, as a client with
/// no prefix does, instead of decoding and encoding each entry again for itself: 13 times
/// the shared path's cost per event (cli692's micro-benchmark), on every client of a hub.
/// Anything the walk doesn't expect (a truncated encoding, a group, a second commit field,
/// another event) is [`Rows::Mixed`], so the decode decides, never a skip.
fn rows(event: &[u8], prefix: &[u8]) -> Rows {
    rows_of(event, prefix).unwrap_or(Rows::Mixed)
}

/// `FeedEvent.commit`, `FeedCommit.changes` and `Change.row`.
const FEED_COMMIT: u64 = 2;
const COMMIT_CHANGES: u64 = 3;
const CHANGE_ROW: u64 = 4;

fn rows_of(mut event: &[u8], prefix: &[u8]) -> Option<Rows> {
    let mut commit = None;
    while !event.is_empty() {
        match field(&mut event)? {
            (FEED_COMMIT, Some(c)) if commit.is_none() => commit = Some(c),
            // Another commit field (prost would merge them), or another of the oneof's events
            // (the planned catalog event, progress, partition state).
            (FEED_COMMIT | 3 | 4 | 6, _) => return None,
            _ => {}
        }
    }
    let mut commit = commit?;
    let (mut all, mut any) = (true, false);
    while !commit.is_empty() {
        match field(&mut commit)? {
            (COMMIT_CHANGES, Some(change)) => {
                let matches = row_of(change)?.starts_with(prefix);
                all &= matches;
                any |= matches;
            }
            (COMMIT_CHANGES, None) => return None,
            _ => {}
        }
    }
    Some(match (any, all) {
        // No change at all is none matching, as the decode's filter leaves it.
        (false, _) => Rows::None,
        (true, true) => Rows::All,
        (true, false) => Rows::Mixed,
    })
}

/// An encoded `Change`'s row: its last `row` field, as prost decodes it, or empty without
/// one (proto3's default, which only an empty prefix matches).
fn row_of(mut change: &[u8]) -> Option<&[u8]> {
    let mut row: &[u8] = &[];
    while !change.is_empty() {
        match field(&mut change)? {
            (CHANGE_ROW, Some(r)) => row = r,
            (CHANGE_ROW, None) => return None,
            _ => {}
        }
    }
    Some(row)
}

/// The next field of an encoded message: its number, and its bytes if it's length-delimited
/// (`None` for a varint or fixed-width one, stepped over). `None` for anything else: a
/// group, field 0, a key past `u32::MAX`, or a truncated encoding.
fn field<'a>(buf: &mut &'a [u8]) -> Option<(u64, Option<&'a [u8]>)> {
    let key = prost::encoding::decode_varint(buf).ok()?;
    let number = key >> 3;
    // As prost's decode refuses them (feedback from feed1248, N1 on #1783).
    if number == 0 || key > u64::from(u32::MAX) {
        return None;
    }
    let value = match key & 7 {
        0 => {
            prost::encoding::decode_varint(buf).ok()?;
            None
        }
        1 => {
            *buf = buf.get(8..)?;
            None
        }
        5 => {
            *buf = buf.get(4..)?;
            None
        }
        2 => {
            let len = usize::try_from(prost::encoding::decode_varint(buf).ok()?).ok()?;
            let value = buf.get(..len)?;
            *buf = &buf[len..];
            Some(value)
        }
        _ => return None,
    };
    Some((number, value))
}

/// What a client with a (non-empty) `prefix` sends for entry `e`, if anything: the shared
/// encoding when every change matches, nothing when none does, and the matching changes
/// decoded off it otherwise (#1780). The same bytes the decode alone would send, in every
/// case (`a_prefix_client_sends_what_the_decode_would_whatever_the_rows`).
fn prefix_out(e: &Arc<Entry>, prefix: &[u8]) -> Option<Out> {
    match rows(e.encoded()?, prefix) {
        Rows::All => Some(Out::Shared(Arc::clone(e))),
        Rows::None => None,
        Rows::Mixed => e.for_prefix(prefix).map(Out::event),
    }
}

/// A commit event: `c` with the token right after it.
fn commit_event(token: Bytes, c: proto::FeedCommit) -> proto::FeedEvent {
    proto::FeedEvent {
        token,
        event: Some(proto::feed_event::Event::Commit(c)),
        partition: String::new(),
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
        // The entries are in seqno order (the hub pushes its feed's commits as they come),
        // so the first one after `seqno` is found by bisection. A scan from the front walked
        // nearly the whole ring, with its lock held, for a client that rejoins near the
        // head: 2.2 ms at a million entries (audit120's pass 2).
        let i = self.entries.partition_point(|e| e.seqno <= seqno);
        Some(self.base + i as u64)
    }
}

struct Hub {
    ring: Mutex<Ring>,
    /// The ring's head index, bumped by each commit and by a failure.
    head: watch::Sender<u64>,
    /// Its task: replaced each time the task moves between runtimes ([`drive`]).
    task: Mutex<tokio::task::AbortHandle>,
    /// Its clients, on its ring or on their own streams until they rejoin it: a [`Joined`]
    /// each. At most one, its task runs on the feed runtime (#1689).
    members: AtomicUsize,
    /// Bumped by each join, so the task sees a client that joined and left between two of
    /// its looks at `members`.
    joins: AtomicU64,
    /// Whether its task runs on the feed runtime beside its one client (#1689), for the
    /// gauge and the dump.
    solo: AtomicBool,
    stats: Arc<FeedStats>,
    /// Live hubs, shared: each ring's cap is the total over it.
    live: Arc<AtomicU64>,
    /// Its clients' trackers, for the dump; a gone client's is pruned there.
    clients: Mutex<Vec<Weak<Tracker>>>,
}

impl Hub {
    fn ring(&self) -> MutexGuard<'_, Ring> {
        self.ring.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn task(&self) -> MutexGuard<'_, tokio::task::AbortHandle> {
        self.task.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Makes `tracker`'s client one of this hub's members until the [`Joined`] drops.
    fn join(self: &Arc<Self>, tracker: &Arc<Tracker>) -> Joined {
        self.track(tracker);
        self.members.fetch_add(1, Ordering::Relaxed);
        self.joins.fetch_add(1, Ordering::Relaxed);
        Joined(Arc::clone(self))
    }

    /// Counts `tracker`'s client under this hub, for the dump. Gone clients and those that
    /// moved to another hub are pruned here whenever the list reaches a power of two, so it
    /// stays bounded on a server nobody dumps (perf287's R1 on #1643), and in the dump.
    fn track(&self, tracker: &Arc<Tracker>) {
        let me = std::ptr::from_ref(self) as usize;
        tracker.hub.store(me, Ordering::Relaxed);
        let mut clients = self.clients.lock().unwrap_or_else(PoisonError::into_inner);
        if clients.len() >= 16 && clients.len().is_power_of_two() {
            clients.retain(|w| {
                w.upgrade()
                    .is_some_and(|t| t.hub.load(Ordering::Relaxed) == me)
            });
        }
        clients.push(Arc::downgrade(tracker));
    }

    fn push(&self, commit: Commit, token: Bytes, position: u64, total: usize) {
        let limit = total / self.live.load(Ordering::Relaxed).max(1) as usize;
        // Encoded here, on the hub's task and outside the ring's lock.
        let seqno = commit.seqno();
        let mut entry = Entry::new(&commit, token.clone());
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
        self.head.send_replace(head);
    }

    /// The hub's feed found nothing at `position`: the ring holds everything up to it.
    ///
    /// The token is made outside the ring's lock: its cost grows with what the feed holds
    /// unreturned, and clients and subscribes take that lock on network threads (#1397's
    /// fan-out wedge). Only the hub's own task moves `passed`, so it can't move between the
    /// check and the store.
    fn note_passed(&self, position: u64, token: impl FnOnce() -> Bytes) {
        if position <= self.ring().passed {
            return;
        }
        let token = token();
        let mut r = self.ring();
        if position > r.passed {
            r.passed = position;
            r.passed_token = token;
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
        self.task
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .abort();
        self.live.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A client's membership of its hub, counted in the hub's `members` while held.
struct Joined(Arc<Hub>);

impl Drop for Joined {
    fn drop(&mut self) {
        self.0.members.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Where a hub's task runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Place {
    /// The feed runtime, beside its one client's task: a commit wakes the client without a
    /// cross-runtime handoff (#1689, g64's +6.4% CPU per put at one subscriber).
    Feed,
    /// The hub runtime, apart from its clients' delivery, so a hub never queues behind its
    /// clients' tasks (#1397's fan-out wedge).
    Hubs,
}

/// A hub's task: its engine stream, read into its ring.
struct Driver {
    stream: FeedStream,
    hub: Weak<Hub>,
    /// `--feed-ring-bytes`, shared by the live hubs.
    ring_bytes: usize,
    /// The server's feed and hub runtimes, when it has both: the task moves between them.
    runtimes: Option<(tokio::runtime::Handle, tokio::runtime::Handle)>,
    solo_after: Duration,
    place: Place,
    /// When the task first saw its hub alone on the hub runtime, and the joins then.
    alone: Option<(Instant, u64)>,
}

impl Driver {
    /// Where the task should run now, if not where it is: on the hub runtime at once with two
    /// or more clients, back on the feed runtime after `solo_after` with at most one.
    ///
    /// Asked only after a commit, so a hub left alone on the hub runtime by a quiet table
    /// stays there until its next commit. That's harmless: with no commits there's no
    /// handoff to save (perf287's N1 on #1716).
    fn moves(&mut self, hub: &Hub) -> Option<Place> {
        self.runtimes.as_ref()?;
        let members = hub.members.load(Ordering::Relaxed);
        match self.place {
            Place::Feed => (members >= 2).then_some(Place::Hubs),
            Place::Hubs if members >= 2 => {
                self.alone = None;
                None
            }
            Place::Hubs => {
                let joins = hub.joins.load(Ordering::Relaxed);
                match self.alone {
                    Some((since, j)) if j == joins => {
                        (since.elapsed() >= self.solo_after).then_some(Place::Feed)
                    }
                    _ => {
                        self.alone = Some((Instant::now(), joins));
                        None
                    }
                }
            }
        }
    }
}

/// Runs a hub's task: reads its engine stream into its ring until the hub goes or the stream
/// fails. When the hub's client count calls for the other runtime (#1689), it spawns itself
/// there between two commits, handing over the stream itself, so the ring sees one unbroken
/// sequence of pushes. Boxed: the task spawns its own kind.
fn drive(mut d: Driver) -> Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    Box::pin(async move {
        loop {
            // A mask a partition's open or close published (D317 ticket 6): a busy hub
            // may not park for the whole load (perf287's P1 on #1755).
            super::placement::refresh();
            // A poll that finds nothing records how far the feed got, so the ring's `passed`
            // follows other tables' writes (the engine wakes the hub on each frontier move).
            let item = std::future::poll_fn(|cx| {
                let polled = Pin::new(&mut d.stream).poll_next(cx);
                if polled.is_pending()
                    && let Some(hub) = d.hub.upgrade()
                {
                    let at = d.stream.position().seqno();
                    hub.note_passed(at, || token_of(&d.stream));
                }
                polled
            })
            .await;
            let Some(hub) = d.hub.upgrade() else { return };
            match item {
                Some(Ok(commit)) => {
                    let at = d.stream.position().seqno();
                    hub.push(commit, token_of(&d.stream), at, d.ring_bytes);
                    if let Some(to) = d.moves(&hub)
                        && let Some((feed, hubs)) = d.runtimes.clone()
                    {
                        d.place = to;
                        d.alone = None;
                        let rt = if to == Place::Feed { feed } else { hubs };
                        // Under the task's lock and holding `hub`: the hub can't drop (and
                        // abort the old handle) before the new task's handle is in place. A
                        // hub failed meanwhile (`fail_all` aborts the handle it finds, this
                        // one, which is past its last await) stops here instead, its place
                        // and the count as they were (perf287's N2 on #1716).
                        let mut task = hub.task();
                        if !hub.ring().failed {
                            hub.solo.store(to == Place::Feed, Ordering::Relaxed);
                            hub.stats.hub_moves.fetch_add(1, Ordering::Relaxed);
                            *task = rt.spawn(drive(d)).abort_handle();
                        }
                        return;
                    }
                    drop(hub);
                    // A feed with a backlog is always ready, and it isn't part of Tokio's
                    // budget: without this, a hub that fell behind keeps its worker, and the
                    // other hubs on its runtime starve (#1397's fan-out wedge; perf287's N1
                    // on #1696).
                    tokio::task::coop::consume_budget().await;
                }
                // An expiry, `Closed`, damage: each client goes on from its own token on its
                // own stream, and the engine decides each (feed1248's (5)).
                _ => {
                    hub.fail();
                    return;
                }
            }
        }
    })
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
    /// The node's live hubs, every partition's: the ring budget (`--feed-ring-bytes`) is
    /// split across them, and `--max-feed-hubs` counts them (D317 ticket 8a).
    live: Arc<AtomicU64>,
    /// Ends every cluster component stream (`Spec::announce`) open on this partition, with
    /// the error sent: the node's map no longer gives it the partition (audit120's S-C5).
    parts_ended: watch::Sender<Option<ApiError>>,
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
    catch_up: Arc<Semaphore>,
    live: Arc<AtomicU64>,
}

impl FeedShared {
    pub(crate) fn new(max_catch_up: usize) -> Self {
        Self {
            catch_up: Arc::new(Semaphore::new(max_catch_up.max(1))),
            live: Arc::default(),
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
            parts_ended: watch::Sender::new(None),
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
            let limit = self.limits.ring_bytes / hub.live.load(Ordering::Relaxed).max(1) as usize;
            let me = Arc::as_ptr(hub) as usize;
            let trackers: Vec<Arc<Tracker>> = {
                let mut c = hub.clients.lock().unwrap_or_else(PoisonError::into_inner);
                c.retain(|w| {
                    w.upgrade()
                        .is_some_and(|t| t.hub.load(Ordering::Relaxed) == me)
                });
                c.iter().filter_map(Weak::upgrade).collect()
            };
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
            task.abort();
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
        let (head, _) = watch::channel(0);
        let stats = Arc::clone(&self.stats);
        let live = Arc::clone(&self.live);
        // From here the hub holds the place, and gives it back when it's dropped.
        place.keep();
        // A new hub has one client coming: it starts beside it on the feed runtime (#1689),
        // when the server has both runtimes.
        let runtimes = self
            .runtime
            .get()
            .cloned()
            .zip(self.hub_runtime.get().cloned());
        let hub = Arc::new_cyclic(|weak: &Weak<Hub>| {
            let driver = Driver {
                stream,
                hub: weak.clone(),
                ring_bytes: self.limits.ring_bytes,
                runtimes: runtimes.clone(),
                solo_after: self.limits.solo_after,
                place: Place::Feed,
                alone: None,
            };
            let task = match &runtimes {
                Some((feed, _)) => feed.spawn(drive(driver)),
                None => self.spawn_hub(drive(driver)),
            };
            Hub {
                ring: Mutex::new(Ring {
                    start_seqno,
                    passed: start_seqno,
                    passed_token: start_token,
                    ..Ring::default()
                }),
                head,
                task: Mutex::new(task.abort_handle()),
                members: AtomicUsize::new(0),
                joins: AtomicU64::new(0),
                solo: AtomicBool::new(runtimes.is_some()),
                stats,
                live,
                clients: Mutex::default(),
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
        let spec_announce = spec.announce;
        let (sink, events, end_tx) = Sink::new(&self.limits);
        let tracker = Arc::new(Tracker::default());
        let joined = hub.join(&tracker);
        drop(hub);
        let client = Client {
            feeds: Arc::clone(self),
            hub: Mutex::new(joined),
            after,
            skip: spec.skip,
            skipped: AtomicU64::new(0),
            announce: spec.announce,
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
    hub: Mutex<Joined>,
    /// For a `Now` start, the database's seqno when it subscribed: commits at or below it
    /// are passed over, as an in-process feed opened then would never deliver them, even
    /// when the hub hadn't read them yet.
    after: u64,
    /// Commits up to this seqno are passed over as already handled ([`Spec::skip`]).
    skip: u64,
    /// How many of those would have been sent: at most [`SKIP_LIMIT`].
    skipped: AtomicU64,
    /// [`Spec::announce`].
    announce: bool,
    tables: Vec<Table>,
    prefix: Vec<u8>,
    visible: bool,
    sink: Sink,
    /// Where it is, for the dump.
    tracker: Arc<Tracker>,
}

/// Where a stream's events go: its connection's buffered channel, bounded by events and by
/// bytes, and ended by the idle limit when the connection stops taking. A single node's
/// client and a cluster subscription's merge (D317 ticket 7) both send through one.
pub(crate) struct Sink {
    tx: mpsc::Sender<(Out, OwnedSemaphorePermit)>,
    /// The buffer's free bytes: each event in the channel holds its encoded size.
    budget: Arc<Semaphore>,
    /// When the connection last took an event: a send waiting for room counts its
    /// `--stream-idle-ms` from it (simp724's I1 on #1536).
    taken: Arc<TakeClock>,
    client_bytes: usize,
    stream_idle: Duration,
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
        Arc::clone(&self.hub.lock().unwrap_or_else(PoisonError::into_inner).0)
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
        if self.announce {
            let event = proto::FeedEvent {
                token: pos.token.clone(),
                event: Some(proto::feed_event::Event::Progress(proto::Progress {})),
                partition: String::new(),
            };
            self.send(Out::event(event), &mut pos).await?;
        }
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
        self.tracker.set(WAITING, 0);
        tokio::select! {
            // A client that leaves while queued gives up its place.
            () = self.sink.closed() => Err(ApiError::internal("the client went away")),
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
            // Per delivery batch, as the hub does per read (perf287's P1 on #1755).
            super::placement::refresh();
            self.tracker.set(ON_RING, at);
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
                let quiet =
                    tokio::time::sleep_until((pos.last_event + self.feeds.limits.quiet).into());
                tokio::select! {
                    // A client that went away ends here, even one whose prefix never
                    // matches (it would otherwise hold its stream slot until a send failed).
                    () = self.sink.closed() => return Err(ApiError::internal("the client went away")),
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
            for e in batch.drain(..) {
                at += 1;
                self.cover_entry(&e, pos).await?;
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
        // Its stream is read once, without waiting, before the client may rejoin the ring: a
        // resume whose token the engine refused (another history, retention passed) reports
        // that on the first read, and a ring that happens to cover the token's seqno would
        // otherwise take the client on without it (#1805's Windows run: a stale copy's token
        // went on). Nothing ready yet: it may rejoin at once.
        let first =
            std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut *stream).poll_next(cx))).await;
        match first {
            Poll::Ready(Some(Ok(commit))) => {
                let token = token_of(&stream);
                self.cover(&commit, &token, pos).await?;
                pos.seqno = Some(stream.position().seqno());
            }
            Poll::Ready(Some(Err(e))) => return Err(e.into()),
            Poll::Ready(None) => {
                return Err(ApiError::internal("the feed ended without an error"));
            }
            Poll::Pending => {}
        }
        loop {
            super::placement::refresh();
            // A failed hub: take the key's current one, opening a new one if none (F2), at
            // most once a second so a hub that keeps failing isn't reopened in a loop.
            let mut hub = self.hub();
            if hub.ring().failed && hub_retry.is_none_or(|t| t.elapsed() >= HUB_RETRY) {
                hub_retry = Some(Instant::now());
                if let Ok(new) = self.feeds.hub(&self.tables, self.visible) {
                    let joined = new.join(&self.tracker);
                    *self.hub.lock().unwrap_or_else(PoisonError::into_inner) = joined;
                    hub = new;
                }
            }
            self.tracker.set(ON_OWN, pos.seqno.unwrap_or(0));
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
                () = self.sink.closed() => return Err(ApiError::internal("the client went away")),
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
                }
            }
            // After either arm: commits that keep arriving, none matching the prefix, would
            // otherwise win every round and the token would never advance (audit120).
            if pos.last_event.elapsed() >= self.feeds.limits.quiet {
                self.progress(pos).await?;
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
            if self.skipping(commit.seqno())? {
                return Ok(());
            }
            self.send(Out::event(commit_event(token.clone(), c)), pos)
                .await?;
        }
        Ok(())
    }

    /// Whether the commit at `seqno`, which would be sent, is passed over as one the client
    /// already handled ([`Spec::skip`]); `InvalidArgument` past [`SKIP_LIMIT`] of them.
    fn skipping(&self, seqno: u64) -> Result<bool, ApiError> {
        if seqno > self.skip {
            return Ok(false);
        }
        if self.skipped.fetch_add(1, Ordering::Relaxed) >= SKIP_LIMIT {
            return Err(ApiError::invalid(format!(
                "skip_through passes over more than {SKIP_LIMIT} commits after the token \
                 (through seqno {}): a client can't have handled more after a checkpoint",
                self.skip
            )));
        }
        Ok(true)
    }

    /// Covers one entry of the ring. A client with no prefix sends the entry itself: its
    /// event is encoded once, for every such client (perf287's profile on #1536: building
    /// and encoding the same event per subscriber was two thirds of the feed's CPU).
    async fn cover_entry(&self, e: &Arc<Entry>, pos: &mut Position) -> Result<(), ApiError> {
        pos.seqno = Some(e.seqno);
        pos.token = e.token.clone();
        if e.seqno <= self.after || e.encoded().is_none() {
            return Ok(());
        }
        if self.prefix.is_empty() {
            if self.skipping(e.seqno)? {
                return Ok(());
            }
            return self.send(Out::Shared(Arc::clone(e)), pos).await;
        }
        // A prefix: the entry's changes to matching rows, if any (a later progress event
        // carries the position past one with none), the shared encoding when they all
        // match (#1780).
        match prefix_out(e, &self.prefix) {
            Some(_) if self.skipping(e.seqno)? => Ok(()),
            Some(out) => self.send(out, pos).await,
            None => Ok(()),
        }
    }

    /// A progress event, if the position moved since the last event and the stream was quiet.
    async fn progress(&self, pos: &mut Position) -> Result<(), ApiError> {
        if pos.token == pos.sent {
            // Nothing to say: look again after another quiet interval.
            pos.last_event = Instant::now();
            return Ok(());
        }
        self.send(
            Out::event(proto::FeedEvent {
                token: pos.token.clone(),
                event: Some(proto::feed_event::Event::Progress(proto::Progress {})),
                partition: String::new(),
            }),
            pos,
        )
        .await
    }

    async fn send(&self, event: Out, pos: &mut Position) -> Result<(), ApiError> {
        let token = event.token().clone();
        self.sink.put(event).await?;
        pos.sent = token;
        pos.last_event = Instant::now();
        Ok(())
    }
}

impl Sink {
    /// A stream's sink, the events its connection takes, and where its task's error goes.
    pub(crate) fn new(limits: &FeedLimits) -> (Self, FeedEvents, oneshot::Sender<ApiError>) {
        let (tx, rx) = mpsc::channel(limits.client_events.max(1));
        let client_bytes = limits.client_bytes.clamp(1, Semaphore::MAX_PERMITS);
        let taken = Arc::new(TakeClock::new());
        let (end_tx, end_rx) = oneshot::channel();
        let sink = Sink {
            tx,
            budget: Arc::new(Semaphore::new(client_bytes)),
            taken: Arc::clone(&taken),
            client_bytes,
            stream_idle: limits.stream_idle,
        };
        let events = FeedEvents(EventSource::Sink {
            events: rx,
            taken,
            end: Some(end_rx),
        });
        (sink, events, end_tx)
    }

    /// Whether the connection went away.
    pub(crate) fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    /// Resolves once the connection has gone away.
    pub(crate) async fn closed(&self) {
        self.tx.closed().await;
    }

    /// Queues `event` for the connection: room in its buffer, then a place in its channel,
    /// both inside the idle limit, so a connection that takes nothing fills its buffer and
    /// then times out.
    pub(crate) async fn put(&self, event: Out) -> Result<(), ApiError> {
        let bytes = event.encoded_len().clamp(1, self.client_bytes);
        let n = u32::try_from(bytes).unwrap_or(u32::MAX);
        // The fast path: room in the buffer and a place in the channel at once, with no
        // timer or waiter registered. Arming them on every send was 64% of the samples in
        // futex calls at 256 subscribers (perf287's profile on #1397).
        let (event, held) = match Arc::clone(&self.budget).try_acquire_many_owned(n) {
            Ok(room) => match self.tx.try_send((event, room)) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full((event, room))) => (event, Some(room)),
                Err(mpsc::error::TrySendError::Closed(_)) => return Err(gone()),
            },
            Err(tokio::sync::TryAcquireError::NoPermits) => (event, None),
            Err(tokio::sync::TryAcquireError::Closed) => return Err(gone()),
        };
        // The slow path, boxed: its timer and waits made this future 1.2 KB, built (and
        // copied) for every event though the fast path needs none of it (#1758's g86c).
        Box::pin(self.wait_to_put(event, held, n)).await
    }

    /// [`Sink::put`]'s slow path: waits for room in the buffer (unless `held`) and a place in
    /// the channel, within the idle limit.
    async fn wait_to_put(
        &self,
        event: Out,
        held: Option<OwnedSemaphorePermit>,
        n: u32,
    ) -> Result<(), ApiError> {
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
        // all along is slow, not idle (simp724's I1 on #1536). One timer: when it fires, a
        // take since the wait began moves the deadline on, and it waits again.
        let idle = self.stream_idle;
        let began = tokio::time::Instant::now();
        let timer = tokio::time::sleep_until(began + idle);
        tokio::pin!(timer);
        loop {
            tokio::select! {
                // The send first: room that is there wins over a timer that fired meanwhile.
                biased;
                sent = &mut queued => {
                    // The client went away: end quietly.
                    return sent.map_err(|()| gone());
                }
                () = self.tx.closed() => return Err(gone()),
                () = &mut timer => {
                    let from = self.taken.last().map_or(began, |t| t.max(began));
                    if from + idle > tokio::time::Instant::now() {
                        timer.as_mut().reset(from + idle);
                        continue;
                    }
                    return Err(ApiError::deadline(format!(
                        "the client took no feed event for --stream-idle-ms ({} ms); resume \
                         from the last event's token",
                        idle.as_millis()
                    )));
                }
            }
        }
    }
}

/// The error a send to a client that went away ends with.
fn gone() -> ApiError {
    ApiError::internal("the client went away")
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

/// A client's events, then its error if it failed: a single node's, from its [`Sink`]; or
/// a cluster subscription's, merged as the client polls ([`crate::serve::cluster::feed`],
/// #1758's B).
pub(crate) struct FeedEvents(EventSource);

enum EventSource {
    Sink {
        events: mpsc::Receiver<(Out, OwnedSemaphorePermit)>,
        /// Each take's time, for a send waiting for room.
        taken: Arc<TakeClock>,
        end: Option<oneshot::Receiver<ApiError>>,
    },
    Merged(Box<crate::serve::cluster::feed::ClusterEvents>),
}

impl std::fmt::Debug for FeedEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.0 {
            EventSource::Sink { .. } => "FeedEvents(sink)",
            EventSource::Merged(_) => "FeedEvents(merged)",
        })
    }
}

/// An event as it leaves a client: its own, or a ring entry encoded once for every client
/// with no prefix.
pub(crate) enum Out {
    /// Boxed, so `Out` is two `Bytes` wide (`Spliced`), not a whole message: every event is
    /// moved through a `Sink`'s put and channel by value, once per stream it goes to (#1758's
    /// g88: those moves were most of the cluster path's memmove).
    Event(Box<proto::FeedEvent>),
    /// An entry whose `encoded()` is `Some`.
    Shared(Arc<Entry>),
    /// A cluster subscription's event (D317 ticket 7): a component's event without its
    /// token (`body`), then the fields the merge adds (`tail`: the partition, and on a
    /// checkpoint the cluster token). A protobuf message is the concatenation of its fields,
    /// so the component's encoding is sent as it is, not decoded and built again.
    Spliced { body: Bytes, tail: Bytes },
}

impl Out {
    pub(crate) fn event(e: proto::FeedEvent) -> Self {
        Out::Event(Box::new(e))
    }

    fn token(&self) -> &Bytes {
        static NONE: Bytes = Bytes::new();
        match self {
            Out::Event(e) => &e.token,
            Out::Shared(e) => &e.token,
            Out::Spliced { .. } => &NONE,
        }
    }

    /// Its size on the wire, for the client's buffer.
    pub(crate) fn encoded_len(&self) -> usize {
        match self {
            Out::Event(e) => prost::Message::encoded_len(e),
            Out::Shared(e) => e.encoded().map_or(0, Bytes::len),
            Out::Spliced { body, tail } => body.len() + tail.len(),
        }
    }

    /// The event's encoding, as one buffer (a cluster component's event, to splice).
    pub(crate) fn to_bytes(&self) -> Bytes {
        match self {
            Out::Event(e) => Bytes::from(prost::Message::encode_to_vec(e)),
            Out::Shared(e) => e.encoded().cloned().unwrap_or_default(),
            Out::Spliced { body, tail } => [body.as_ref(), tail.as_ref()].concat().into(),
        }
    }

    /// The event as a message (HTTP, which renders JSON, and tests).
    pub(crate) fn into_proto(self) -> proto::FeedEvent {
        match self {
            Out::Event(e) => *e,
            Out::Shared(e) => e
                .encoded()
                .and_then(|b| <proto::FeedEvent as prost::Message>::decode(b.clone()).ok())
                .unwrap_or_else(|| commit_event(e.token.clone(), proto::FeedCommit::default())),
            Out::Spliced { .. } => {
                <proto::FeedEvent as prost::Message>::decode(self.to_bytes()).unwrap_or_default()
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
            Out::Spliced { body, tail } => {
                buf.put_slice(body);
                buf.put_slice(tail);
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
        let (events, taken, end) = match &mut self.0 {
            EventSource::Merged(m) => return m.poll_out(cx),
            EventSource::Sink { events, taken, end } => (events, taken, end),
        };
        match events.poll_recv(cx) {
            // Taken: its bytes go back to the client's buffer.
            Poll::Ready(Some((e, room))) => {
                drop(room);
                taken.took();
                return Poll::Ready(Some(Ok(e)));
            }
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => {}
        }
        let Some(rx) = end.as_mut() else {
            return Poll::Ready(None);
        };
        let out = match Pin::new(rx).poll(cx) {
            Poll::Ready(Ok(e)) => Some(Err(e)),
            Poll::Ready(Err(_)) => None,
            Poll::Pending => return Poll::Pending,
        };
        *end = None;
        Poll::Ready(out)
    }

    /// A cluster subscription's events: merged as the client polls.
    pub(crate) fn merged(events: crate::serve::cluster::feed::ClusterEvents) -> Self {
        FeedEvents(EventSource::Merged(Box::new(events)))
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

    /// A hub's tracker list stays bounded without a dump (perf287's R1 on #1643): gone
    /// clients and those counted under another hub are pruned as it grows.
    #[test]
    fn a_hubs_tracker_list_stays_bounded_without_a_dump() {
        let (head, _) = watch::channel(0);
        let hub = |head: watch::Sender<u64>| Hub {
            ring: Mutex::default(),
            head,
            task: Mutex::new(
                tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap()
                    .spawn(async {})
                    .abort_handle(),
            ),
            members: AtomicUsize::new(0),
            joins: AtomicU64::new(0),
            solo: AtomicBool::new(false),
            stats: Arc::default(),
            live: Arc::new(AtomicU64::new(1)),
            clients: Mutex::default(),
        };
        let (a, b) = (hub(head.clone()), hub(head));
        // Ten thousand clients that came and went.
        for _ in 0..10_000 {
            a.track(&Arc::new(Tracker::default()));
        }
        assert!(a.clients.lock().unwrap().len() <= 32, "pruned as it grew");
        // Live clients stay; one that moved to `b` is no longer counted under `a`.
        let live: Vec<Arc<Tracker>> = (0..20).map(|_| Arc::new(Tracker::default())).collect();
        for t in &live {
            a.track(t);
        }
        b.track(&live[0]);
        for _ in 0..64 {
            a.track(&Arc::new(Tracker::default()));
        }
        let me = std::ptr::from_ref(&a) as usize;
        let counted = a
            .clients
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|t| t.hub.load(Ordering::Relaxed) == me)
            .count();
        assert_eq!(counted, 19, "the 19 live clients still on `a`");
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
                ..Entry::new(c, Bytes::new())
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
                solo_after: SOLO_AFTER,
            },
            &FeedShared::new(1),
        );
        let slots = Arc::new(Semaphore::new(8));
        let spec = || Spec {
            tables: vec![t.clone()],
            prefix: Vec::new(),
            visible: false,
            start: Start::Now,
            skip: 0,
            announce: false,
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
            first.task().abort();
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

    /// A server's three runtimes, as `phdb serve` names them: requests, feed delivery and
    /// hubs, one worker each but `delivery_threads` for delivery.
    fn server_runtimes(
        delivery_threads: usize,
    ) -> (
        tokio::runtime::Runtime,
        tokio::runtime::Runtime,
        tokio::runtime::Runtime,
    ) {
        let rt = |threads: usize, name: Option<&str>| {
            let mut b = tokio::runtime::Builder::new_multi_thread();
            b.worker_threads(threads).enable_all();
            if let Some(name) = name {
                b.thread_name(name);
            }
            b.build().unwrap()
        };
        (
            rt(1, None),
            rt(delivery_threads, Some("phdb-serve-feed")),
            rt(1, Some("phdb-serve-feed-hub")),
        )
    }

    #[test]
    fn an_out_is_two_bytes_wide_not_a_whole_message() {
        // #1758's g88: each event's `Out` is moved by value through every stream's put and
        // channel slot; with the message inline it was 136 bytes.
        let size = std::mem::size_of::<Out>();
        assert!(size <= 72, "Out is {size} bytes");
    }

    #[test]
    fn a_put_with_room_builds_a_small_future() {
        // #1758's g86c: a cluster subscription puts every event through a `Sink`, and with
        // the slow path's timer and waits inline the future was 1.2 KB, built and copied per
        // event though a put with room needs none of it. Boxed, the slow path costs only
        // when the client is behind.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (sink, _events, _end) = Sink::new(&limits_solo_after(SOLO_AFTER));
            let put = sink.put(Out::Spliced {
                body: prost::bytes::Bytes::from_static(b"x"),
                tail: prost::bytes::Bytes::new(),
            });
            let size = std::mem::size_of_val(&put);
            assert!(size <= 512, "Sink::put's future is {size} bytes");
            put.await.unwrap();
        });
    }

    fn limits_solo_after(solo_after: Duration) -> FeedLimits {
        FeedLimits {
            ring_bytes: 16 << 20,
            max_hubs: 4,
            max_catch_up: 1,
            quiet: Duration::from_secs(10),
            stream_idle: Duration::from_secs(5),
            client_events: CLIENT_EVENTS,
            client_bytes: CLIENT_BYTES,
            solo_after,
        }
    }

    fn now_spec(t: &Table) -> Spec {
        Spec {
            tables: vec![t.clone()],
            prefix: Vec::new(),
            visible: false,
            start: Start::Now,
            skip: 0,
            announce: false,
        }
    }

    /// Waits up to a second for `rt` to have `n` live tasks: a task that moved away is
    /// released once its runtime gets to it.
    async fn tasks_on(rt: &tokio::runtime::Runtime, n: usize) -> usize {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            let alive = rt.metrics().num_alive_tasks();
            if alive == n || tokio::time::Instant::now() >= deadline {
                return alive;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[test]
    fn hubs_and_clients_run_on_their_own_runtimes() {
        // Ticket 5's (A) and (C), with #1689's fast path: with a server's runtimes, a
        // subscription's client task runs on the feed runtime, and so does its hub while it
        // has that one client. A second client moves the hub to the hub runtime; alone again
        // for `solo_after`, it moves back. Nothing of the feed runs on the request's runtime,
        // and events still reach the stream the request side reads (perf287's T1 on #1696).
        let (_, db, dir) = commits("feedrt", 0);
        let (requests, delivery, hubs) = server_runtimes(1);
        let t = db.table("t").unwrap().open().unwrap();
        let feeds = Feeds::new(
            db.clone(),
            limits_solo_after(Duration::from_millis(50)),
            &FeedShared::new(1),
        );
        feeds.run_on(delivery.handle().clone(), hubs.handle().clone());
        let slots = Arc::new(Semaphore::new(8));
        let before = delivery.metrics().num_alive_tasks();
        let hubs_before = hubs.metrics().num_alive_tasks();
        requests.block_on(async {
            let put = |row: &[u8]| t.mutate(row).put("f", b"q", b"v").commit().unwrap().seqno;
            let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
            let mut a = feeds.subscribe(now_spec(&t), slot, |_| {}).unwrap();
            assert_eq!(
                delivery.metrics().num_alive_tasks(),
                before + 2,
                "the client and its hub, on the feed runtime"
            );
            assert_eq!(hubs.metrics().num_alive_tasks(), hubs_before);
            assert_eq!(feeds.gauges().solo_hubs, 1);
            assert_eq!(
                tokio::runtime::Handle::current()
                    .metrics()
                    .num_alive_tasks(),
                0,
                "nothing of the feed on the request side"
            );
            let s1 = put(b"r1");
            assert_eq!(seqnos(&mut a, Duration::from_millis(300)).await, [s1]);

            // A second client: the next commit moves the hub to the hub runtime.
            let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
            let mut b = feeds.subscribe(now_spec(&t), slot, |_| {}).unwrap();
            let s2 = put(b"r2");
            assert_eq!(seqnos(&mut a, Duration::from_millis(300)).await, [s2]);
            assert_eq!(seqnos(&mut b, Duration::from_millis(300)).await, [s2]);
            assert_eq!(
                tasks_on(&hubs, hubs_before + 1).await,
                hubs_before + 1,
                "the hub"
            );
            assert_eq!(
                tasks_on(&delivery, before + 2).await,
                before + 2,
                "two clients"
            );
            assert_eq!(feeds.gauges().solo_hubs, 0);

            // Alone again once B's task has ended: it stays on the hub runtime for
            // `solo_after`, then moves back.
            drop(b);
            assert_eq!(
                tasks_on(&delivery, before + 1).await,
                before + 1,
                "B's task ended"
            );
            let s3 = put(b"r3");
            assert_eq!(seqnos(&mut a, Duration::from_millis(100)).await, [s3]);
            tokio::time::sleep(Duration::from_millis(100)).await;
            let s4 = put(b"r4");
            assert_eq!(seqnos(&mut a, Duration::from_millis(300)).await, [s4]);
            assert_eq!(tasks_on(&hubs, hubs_before).await, hubs_before);
            assert_eq!(tasks_on(&delivery, before + 2).await, before + 2);
            assert_eq!(feeds.gauges().solo_hubs, 1);
            assert_eq!(feeds.stats.hub_moves.load(Ordering::Relaxed), 2);
            drop(a);
        });
        // The dump names the hub runtime's threads (#1719): one here.
        assert!(
            feeds.dump().contains(" hubs on 1 hub threads,"),
            "{}",
            feeds.dump()
        );
        drop(t);
        drop(requests);
        drop(delivery);
        drop(hubs);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_hub_moving_between_runtimes_mid_stream_delivers_every_event_once_in_order() {
        // #1689: a hub's task moves between the feed and hub runtimes as a second client
        // comes and goes, while commits keep coming. The stream moves with it between two
        // polls, so a client there throughout gets every commit once, in order, and each
        // passing client a gapless run of them. No hysteresis, so it moves as often as it can.
        const COMMITS: usize = 1000;
        let (_, db, dir) = commits("feedmove", 0);
        let (requests, delivery, hubs) = server_runtimes(2);
        let t = db.table("t").unwrap().open().unwrap();
        let feeds = Feeds::new(
            db.clone(),
            limits_solo_after(Duration::ZERO),
            &FeedShared::new(1),
        );
        feeds.run_on(delivery.handle().clone(), hubs.handle().clone());
        let slots = Arc::new(Semaphore::new(8));
        requests.block_on(async {
            let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
            let mut a = feeds.subscribe(now_spec(&t), slot, |_| {}).unwrap();
            let writer = {
                let t = t.clone();
                std::thread::spawn(move || {
                    (0..COMMITS)
                        .map(|i| {
                            if i % 5 == 0 {
                                std::thread::sleep(Duration::from_millis(1));
                            }
                            t.mutate(format!("r{i}").as_bytes())
                                .put("f", b"q", b"v")
                                .commit()
                                .unwrap()
                                .seqno
                        })
                        .collect::<Vec<u64>>()
                })
            };
            // A reads all along, on its own task, while B comes and goes.
            let reader = tokio::spawn(async move {
                let got = seqnos(&mut a, Duration::from_secs(2)).await;
                drop(a);
                got
            });
            let mut passing = Vec::new();
            while !writer.is_finished() {
                let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
                let mut b = feeds.subscribe(now_spec(&t), slot, |_| {}).unwrap();
                let mut run = Vec::new();
                while run.len() < 20
                    && let Ok(Some(e)) = tokio::time::timeout(
                        Duration::from_millis(5),
                        std::future::poll_fn(|cx| Pin::new(&mut b).poll_next(cx)),
                    )
                    .await
                {
                    if let Some(proto::feed_event::Event::Commit(c)) = e.unwrap().event {
                        run.push(c.seqno);
                    }
                }
                passing.push(run);
                drop(b);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let want = writer.join().unwrap();
            let got = reader.await.unwrap();
            assert_eq!(got.len(), want.len(), "every commit once");
            assert_eq!(got, want, "in order");
            for run in passing.iter().filter(|r| !r.is_empty()) {
                let from = want.iter().position(|s| *s == run[0]).unwrap();
                assert_eq!(run[..], want[from..from + run.len()], "a gapless run");
            }
            let moves = feeds.stats.hub_moves.load(Ordering::Relaxed);
            assert!(moves >= 6, "the hub moved back and forth ({moves} moves)");
        });
        drop(t);
        drop(requests);
        drop(delivery);
        drop(hubs);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_hub_with_a_backlog_yields_its_feed_worker() {
        // #1397's fan-out wedge: a hub that fell behind is always ready, and its feed isn't
        // part of Tokio's budget. It must still give its worker back now and then, or other
        // hubs and every client on the feed runtime starve until its backlog is gone.
        const BACKLOG: u64 = 3000;
        let (_, db, dir) = commits("hubyield", 0);
        let requests = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let delivery = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let t = db.table("t").unwrap().open().unwrap();
        let feeds = Feeds::new(
            db.clone(),
            FeedLimits {
                ring_bytes: 64 << 20,
                max_hubs: 4,
                max_catch_up: 1,
                quiet: Duration::from_secs(10),
                stream_idle: Duration::from_secs(30),
                client_events: CLIENT_EVENTS,
                client_bytes: CLIENT_BYTES,
                solo_after: SOLO_AFTER,
            },
            &FeedShared::new(1),
        );
        feeds.run_on(delivery.handle().clone(), delivery.handle().clone());
        let slots = Arc::new(Semaphore::new(8));
        let seen = requests.block_on(async {
            let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
            let spec = Spec {
                tables: vec![t.clone()],
                prefix: Vec::new(),
                visible: false,
                start: Start::Now,
                skip: 0,
                announce: false,
            };
            let _events = feeds.subscribe(spec, slot, |_| {}).unwrap();
            // Park the feed runtime's one worker while the backlog is written, so the hub
            // finds all of it ready at once.
            let (park, parked) = std::sync::mpsc::channel::<()>();
            let (release, wait) = std::sync::mpsc::channel::<()>();
            delivery.spawn(async move {
                park.send(()).unwrap();
                let _ = wait.recv();
            });
            parked.recv().unwrap();
            for i in 0..BACKLOG {
                t.mutate(format!("r{i}").as_bytes())
                    .put("f", b"q", b"v")
                    .commit()
                    .unwrap();
            }
            // Queued behind the hub's wake: it runs once the hub gives the worker back.
            let probe = {
                let feeds = Arc::clone(&feeds);
                delivery.spawn(async move {
                    let hubs = feeds.hubs.lock().unwrap();
                    let hub = hubs.values().next().unwrap().upgrade().unwrap();
                    hub.ring().head()
                })
            };
            release.send(()).unwrap();
            probe.await.unwrap()
        });
        // Not a tighter bar: the probe waits in the runtime's injection queue, which a worker
        // checks only every `global_queue_interval` ticks while the yielded hub sits on its
        // local queue, so how far the hub gets first is Tokio's tuning (feed1248's Q1 on
        // #1599). Without the yield it gets through all of the backlog.
        assert!(
            seen < BACKLOG,
            "the hub kept its worker through {seen} of {BACKLOG} backlogged commits"
        );
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
                solo_after: SOLO_AFTER,
            },
            &FeedShared::new(1),
        );
        let slots = Arc::new(Semaphore::new(1));
        let mut direct = t.feed().open().unwrap();
        let mut want = Vec::new();
        rt.block_on(async {
            let hub = feeds.hub(std::slice::from_ref(&t), false).unwrap();
            hub.task().abort();
            let mut c = feeds
                .subscribe(
                    Spec {
                        tables: vec![t.clone()],
                        prefix: Vec::new(),
                        visible: false,
                        start: Start::Now,
                        skip: 0,
                        announce: false,
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
    fn a_client_that_stops_taking_times_out_an_idle_limit_after_its_last_take() {
        // The other half of simp724's I1 on #1536, for the one-timer wait (perf287's profile
        // on #1397): while one large event waits for the client's buffer to drain, the client
        // takes events, then stops. It's ended exactly `--stream-idle-ms` after its last
        // take: not when the limit has passed since the wait began (that was during the
        // takes), and not later. On a paused clock, so the times are exact.
        let (_, db, dir) = commits("idle-from-take", 0);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        let t = db.table("t").unwrap().open().unwrap();
        let idle = Duration::from_millis(250);
        let feeds = Feeds::new(
            db.clone(),
            FeedLimits {
                ring_bytes: 16 << 20,
                max_hubs: 4,
                max_catch_up: 1,
                quiet: Duration::from_secs(3600),
                stream_idle: idle,
                client_events: CLIENT_EVENTS,
                client_bytes: 4096,
                solo_after: SOLO_AFTER,
            },
            &FeedShared::new(1),
        );
        let slots = Arc::new(Semaphore::new(1));
        let mut direct = t.feed().open().unwrap();
        let ended: Arc<Mutex<Option<(tokio::time::Instant, String)>>> = Arc::default();
        rt.block_on(async {
            let hub = feeds.hub(std::slice::from_ref(&t), false).unwrap();
            hub.task().abort();
            let end = Arc::clone(&ended);
            let mut c = feeds
                .subscribe(
                    Spec {
                        tables: vec![t.clone()],
                        prefix: Vec::new(),
                        visible: false,
                        start: Start::Now,
                        skip: 0,
                        announce: false,
                    },
                    Arc::clone(&slots).try_acquire_owned().unwrap(),
                    move |e| {
                        *end.lock().unwrap() = Some((
                            tokio::time::Instant::now(),
                            e.map_or_else(String::new, |e| e.code.clone()),
                        ));
                    },
                )
                .unwrap();
            // 60 events of over 200 bytes (the buffer holds under 20 of them), then one larger
            // than the whole buffer, which waits for all of it to drain.
            for i in 0..61 {
                let value = if i < 60 {
                    vec![b'v'; 100]
                } else {
                    vec![7u8; 8192]
                };
                t.mutate(format!("r{i:04}").as_bytes())
                    .put("f", b"q", &value)
                    .commit()
                    .unwrap();
                let commit = direct
                    .next(Duration::from_secs(5))
                    .unwrap()
                    .unwrap()
                    .clone();
                let token = Bytes::copy_from_slice(direct.token().as_bytes());
                hub.push(commit, token, direct.position().seqno(), 16 << 20);
            }
            // 50 takes, 100 ms apart (each under the limit), then none: the large event began
            // waiting after about 40, so its wait spans the last ten or so.
            let mut last = tokio::time::Instant::now();
            for _ in 0..50 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                use futures_core::Stream as _;
                std::future::poll_fn(|cx| Pin::new(&mut c).poll_next(cx))
                    .await
                    .expect("the stream goes on")
                    .expect("an event");
                last = tokio::time::Instant::now();
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
            let (at, code) = ended.lock().unwrap().clone().expect("the stream ended");
            assert_eq!(code, "DeadlineExceeded");
            assert_eq!(
                at - last,
                idle,
                "the idle limit counts from the last take, exactly"
            );
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
                solo_after: SOLO_AFTER,
            },
            &FeedShared::new(1),
        );
        let slots = Arc::new(Semaphore::new(1));
        rt.block_on(async {
            let hub = feeds.hub(std::slice::from_ref(&t), false).unwrap();
            // The hub's own task stops; the test pushes for it.
            hub.task().abort();
            let mut direct = t.feed().open().unwrap();
            let mut c = feeds
                .subscribe(
                    Spec {
                        tables: vec![t.clone()],
                        prefix: Vec::new(),
                        visible: false,
                        start: Start::Now,
                        skip: 0,
                        announce: false,
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
            let e = Arc::new(Entry::new(c, token.clone()));
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
                partition: String::new(),
            });
            assert_eq!(e.encoded().map(|b| &b[..]), Some(&want[..]));
            let mut wire = Vec::new();
            Out::Shared(Arc::clone(&e)).encode(&mut wire).unwrap();
            assert_eq!(wire, want, "the gRPC codec writes those bytes");
            assert_eq!(Out::Shared(Arc::clone(&e)).encoded_len(), want.len());
            // Counted in the ring's budget (R1, P1): the encoding, which holds the token,
            // and the fixed overhead. The token is a slice of the encoding, not a copy.
            assert_eq!(e.bytes, ENTRY_OVERHEAD + want.len());
            assert_eq!(e.token, token);
            let enc = e.encoded().unwrap();
            assert!(
                enc.as_ptr() <= e.token.as_ptr()
                    && e.token.as_ptr() as usize + e.token.len()
                        <= enc.as_ptr() as usize + enc.len(),
                "the token is held once, inside the encoding"
            );
            // A prefix client gets the changes to its rows, as a filter of the commit
            // would give them.
            for prefix in [&b""[..], b"r", b"zz"] {
                let got = e.for_prefix(prefix);
                let want = filtered(c, prefix).map(|f| commit_event(token.clone(), f));
                assert_eq!(got, want, "prefix {prefix:?}");
            }
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

    /// #1780: a prefix client reads which rows an entry's changes are to off its encoding.
    /// Whatever the rows, it sends the bytes the decode-and-filter path would send: the
    /// shared encoding itself when every change matches, nothing when none does. Random
    /// entries, seeded (`PHDB_ROWS_SEED`; the seed prints on failure), some with fields the
    /// walk doesn't know; and any truncated encoding goes to the decode, never skipped.
    #[test]
    fn a_prefix_client_sends_what_the_decode_would_whatever_the_rows() {
        use prost::encoding::{WireType, encode_key, encode_varint};
        let seed: u64 = std::env::var("PHDB_ROWS_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0x1780);
        let mut x = seed | 1;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        // Short rows and prefixes over two letters, so they match, half match and miss.
        fn word(next: &mut dyn FnMut() -> u64, min: u64) -> Vec<u8> {
            let n = min + next() % (4 - min);
            (0..n).map(|_| b"ab"[(next() % 2) as usize]).collect()
        }
        // The decode's verdict on an encoding: whether the prefix keeps all, none or some of
        // its commit's changes. `None` if it doesn't decode as a commit.
        fn judge(event: &[u8], prefix: &[u8]) -> Option<Rows> {
            let ev = <proto::FeedEvent as prost::Message>::decode(event).ok()?;
            let Some(proto::feed_event::Event::Commit(c)) = ev.event else {
                return None;
            };
            let kept = c
                .changes
                .iter()
                .filter(|ch| ch.row.starts_with(prefix))
                .count();
            Some(match kept {
                0 => Rows::None,
                k if k == c.changes.len() => Rows::All,
                _ => Rows::Mixed,
            })
        }
        let (mut seen_all, mut seen_none, mut seen_mixed) = (0, 0, 0);
        for step in 0..5000 {
            let unknown = next() % 4 == 0;
            // Now and then a shape the walk leaves to the decode, or steps over (feed1248's N2
            // on #1783): 0, another event after the commit; 1, an unknown field between
            // changes; 2, a change field with a varint's wire type; 3, a key past u32::MAX.
            let odd = match next() % 16 {
                k @ 0..=3 => Some(k),
                _ => None,
            };
            let token: Vec<u8> = (0..next() % 24).map(|_| next() as u8).collect();
            let mut commit = Vec::new();
            encode_varint(1 << 3, &mut commit);
            encode_varint(next() % 1_000_000, &mut commit);
            encode_varint(2 << 3, &mut commit);
            encode_varint(next(), &mut commit);
            for _ in 0..1 + next() % 4 {
                let change = proto::Change {
                    table: "busy".into(),
                    family: "f".into(),
                    kind: (1 + next() % 5) as i32,
                    row: word(&mut next, 0).into(),
                    qualifier: word(&mut next, 0).into(),
                    timestamp: next() % 4,
                    value: (next() % 2 == 0).then(|| proto::Value {
                        kind: Some(proto::value::Kind::Bytes(word(&mut next, 0).into())),
                    }),
                };
                let mut c = prost::Message::encode_to_vec(&change);
                if unknown {
                    // Fields 9-12, one of each wire type the walk steps over.
                    encode_key(9, WireType::Varint, &mut c);
                    encode_varint(next(), &mut c);
                    encode_key(10, WireType::LengthDelimited, &mut c);
                    encode_varint(2, &mut c);
                    c.extend_from_slice(b"zz");
                    encode_key(11, WireType::ThirtyTwoBit, &mut c);
                    c.extend_from_slice(&[1, 2, 3, 4]);
                    encode_key(12, WireType::SixtyFourBit, &mut c);
                    c.extend_from_slice(&[0; 8]);
                    // A second `row`: the decode takes the last one, so the walk must too.
                    if next() % 2 == 0 {
                        let row = word(&mut next, 0);
                        encode_key(4, WireType::LengthDelimited, &mut c);
                        encode_varint(row.len() as u64, &mut c);
                        c.extend_from_slice(&row);
                    }
                }
                encode_key(3, WireType::LengthDelimited, &mut commit);
                encode_varint(c.len() as u64, &mut commit);
                commit.extend_from_slice(&c);
                if odd == Some(1) {
                    encode_key(9, WireType::LengthDelimited, &mut commit);
                    encode_varint(1, &mut commit);
                    commit.push(b'x');
                }
            }
            if odd == Some(2) {
                encode_key(3, WireType::Varint, &mut commit);
                encode_varint(7, &mut commit);
            }
            let mut event = Vec::new();
            if odd == Some(3) {
                encode_varint(1 << 35, &mut event);
                encode_varint(1, &mut event);
            }
            if !token.is_empty() {
                encode_key(1, WireType::LengthDelimited, &mut event);
                encode_varint(token.len() as u64, &mut event);
                event.extend_from_slice(&token);
            }
            encode_key(2, WireType::LengthDelimited, &mut event);
            encode_varint(commit.len() as u64, &mut event);
            event.extend_from_slice(&commit);
            if unknown {
                encode_key(9, WireType::Varint, &mut event);
                encode_varint(next(), &mut event);
            }
            if odd == Some(0) {
                // A progress or partition state event: the oneof's last field wins.
                encode_key(
                    if next() % 2 == 0 { 4 } else { 6 },
                    WireType::LengthDelimited,
                    &mut event,
                );
                encode_varint(0, &mut event);
            }
            let event = Bytes::from(event);
            let e = Arc::new(Entry {
                seqno: step,
                token: Bytes::new(),
                prev: 0,
                bytes: 0,
                encoded: Some(event.clone()),
            });
            // Mostly one letter, so every case comes up often.
            let prefix = if next() % 4 == 0 {
                word(&mut next, 1)
            } else {
                vec![b"ab"[(next() % 2) as usize]]
            };
            let why = format!("seed {seed}, step {step}, prefix {prefix:?}");
            // The decode path: the matching changes, or none (an encoding that doesn't
            // decode, or isn't a commit, sends nothing).
            let all_changes = <proto::FeedEvent as prost::Message>::decode(event.clone())
                .ok()
                .and_then(|ev| match ev.event {
                    Some(proto::feed_event::Event::Commit(c)) => Some(c.changes.len()),
                    _ => None,
                });
            let decoded = e.for_prefix(&prefix);
            let want = match &decoded {
                None => Rows::None,
                Some(ev) => match &ev.event {
                    Some(proto::feed_event::Event::Commit(c))
                        if Some(c.changes.len()) == all_changes =>
                    {
                        Rows::All
                    }
                    _ => Rows::Mixed,
                },
            };
            // The walk's verdict is the decode's, except that a shape it leaves to the decode
            // is `Mixed`.
            let r = rows(&event, &prefix);
            if matches!(odd, Some(0 | 2 | 3)) {
                assert!(r == Rows::Mixed || r == want, "{why}: {r:?}, odd {odd:?}");
            } else {
                assert_eq!(r, want, "{why}");
            }
            let got = prefix_out(&e, &prefix);
            match r {
                Rows::All => {
                    seen_all += 1;
                    assert!(matches!(got, Some(Out::Shared(_))), "{why}");
                }
                Rows::None => {
                    seen_none += 1;
                    assert!(got.is_none(), "{why}");
                }
                Rows::Mixed => {
                    seen_mixed += 1;
                    assert!(got.is_none() || matches!(got, Some(Out::Event(_))), "{why}");
                }
            }
            // The same bytes as the decode path's event (fields the encoder doesn't know
            // aren't encoded again, so only without them).
            if !unknown && odd != Some(1) {
                assert_eq!(
                    got.map(|o| o.to_bytes()),
                    decoded.map(|ev| Bytes::from(prost::Message::encode_to_vec(&ev))),
                    "{why}"
                );
            }
            // Cut short anywhere: what the decode makes of it, or, if it can't, for it to judge.
            // Only a cut between whole fields decodes (one before a trailing unknown field).
            let cut = (next() % event.len() as u64) as usize;
            let got = rows(&event[..cut], &prefix);
            match judge(&event[..cut], &prefix) {
                Some(want) => assert_eq!(got, want, "{why}, cut at {cut}"),
                None => assert_eq!(got, Rows::Mixed, "{why}, cut at {cut}"),
            }
        }
        assert!(
            seen_all > 250 && seen_none > 250 && seen_mixed > 250,
            "seed {seed}: all {seen_all}, none {seen_none}, mixed {seen_mixed}"
        );
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
