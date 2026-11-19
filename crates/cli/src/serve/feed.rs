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

/// Where a client is, for the feed dump ([`Feeds::dump`], `phdb serve` on SIGUSR1): two
/// relaxed stores per batch, read only by the dump.
#[derive(Debug, Default)]
pub(crate) struct Tracker {
    /// [`ON_RING`], [`ON_OWN`] or [`WAITING`].
    pub(crate) state: AtomicU64,
    /// On the ring, its next index; on its own stream, the seqno it covered.
    at: AtomicU64,
    /// The hub it's counted under (its address): a client that moved to a failed hub's
    /// replacement is counted there only (perf287's N1 on #1643).
    hub: AtomicUsize,
}

pub(crate) const ON_RING: u64 = 0;
pub(crate) const ON_OWN: u64 = 1;
/// Waiting for a `--max-catch-up` slot, delivering nothing.
pub(crate) const WAITING: u64 = 2;

impl Tracker {
    pub(crate) fn set(&self, state: u64, at: u64) {
        self.state.store(state, Ordering::Relaxed);
        self.at.store(at, Ordering::Relaxed);
    }
}

/// When a client last took an event, for a send waiting for room: its `--stream-idle-ms`
/// counts from the last take (simp724's I1 on #1536). A take stores the time; it wakes no
/// one. A waiting send's one timer, when it fires, reads it and waits on if the client took
/// something meanwhile (perf287's profile on #1397: a wake and a new timer per take, per
/// subscriber, were `Client::send`'s 9.6% self at 16 hubs × 16).
///
/// A batch ([`Sink::put_all`]) is stamped when the connection takes it from the channel and
/// at its last event; its events in between are stamped only while a send waits
/// ([`TakeClock::watch`]). A clock read per event was 6% of the network workers' cycles at
/// saturation (g129, #1737), and without a waiting send no one reads the time between. So
/// a waiting send still sees every take, and a client draining a long batch slowly is slow,
/// not idle.
#[derive(Debug)]
pub(crate) struct TakeClock {
    /// On Tokio's clock, so a paused test clock pauses it too.
    base: tokio::time::Instant,
    /// Nanoseconds from `base` to the last take; 0 before the first.
    last: AtomicU64,
    /// Sends waiting for room now ([`TakeClock::watch`]).
    watchers: AtomicUsize,
}

impl TakeClock {
    pub(crate) fn new() -> Self {
        Self {
            base: tokio::time::Instant::now(),
            last: AtomicU64::new(0),
            watchers: AtomicUsize::new(0),
        }
    }

    /// A take, now.
    pub(crate) fn took(&self) {
        let nanos = u64::try_from(self.base.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.last.store(nanos.max(1), Ordering::Relaxed);
    }

    /// A take within a batch: stamped only while a send waits for room. Sequentially
    /// consistent with [`TakeClock::watch`]'s count, which a waiting send raises before it
    /// reads the time its wait began: a take that reads 0 was ordered before the raise, so
    /// at most one event at the wait's start goes unstamped, and the wait counts from its
    /// start anyway (simp724's N1 on #2012).
    fn took_within(&self) {
        if self.watchers.load(Ordering::SeqCst) != 0 {
            self.took();
        }
    }

    /// While the guard lives, every take is stamped, not just a batch's first and last: a
    /// send waiting for room counts its idle limit from the client's last take.
    fn watch(&self) -> Watch<'_> {
        self.watchers.fetch_add(1, Ordering::SeqCst);
        Watch(self)
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

/// A send waiting for room ([`TakeClock::watch`]).
struct Watch<'a>(&'a TakeClock);

impl Drop for Watch<'_> {
    fn drop(&mut self) {
        self.0.watchers.fetch_sub(1, Ordering::SeqCst);
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
        let encoded: Option<Bytes> = filtered(commit, b"").map(|c| {
            let xid = commit.xid().unwrap_or_default();
            prost::Message::encode_to_vec(&commit_event(token.clone(), c, xid)).into()
        });
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

/// A commit event: `c` with the token right after it, and its spanning transaction's id
/// (`xid`, D329 2b; empty for any other commit).
fn commit_event(token: Bytes, c: proto::FeedCommit, xid: &[u8]) -> proto::FeedEvent {
    proto::FeedEvent {
        token,
        event: Some(proto::feed_event::Event::Commit(c)),
        partition: String::new(),
        xid: Bytes::copy_from_slice(xid),
    }
}

/// What a hub's ring holds (#1798): a single node's commits ([`Single`]), or a cluster
/// feed hub's merged events (`cluster::hub`). A type parameter, never a trait object, so
/// the single node's ring compiles as it did before the cluster's shared it (simp724's Q3).
pub(crate) trait RingKind: Sized + Send + Sync + 'static {
    type Entry: Send + Sync + 'static;
    /// What the ring knows beside its entries: where its feed is.
    type State: Send + 'static;
    /// An entry's share of `--feed-ring-bytes`.
    fn bytes(e: &Self::Entry) -> usize;
    /// `e` left the ring's front.
    fn evicted(_state: &mut Self::State, _e: &Self::Entry) {}
}

/// A hub's bounded ring: its entries, the index of the first in the hub's whole sequence,
/// their bytes, and the kind's state (reached through `Deref`).
pub(crate) struct RingOf<K: RingKind> {
    pub(crate) entries: VecDeque<Arc<K::Entry>>,
    /// The index of `entries[0]` in the hub's whole sequence.
    pub(crate) base: u64,
    pub(crate) bytes: usize,
    pub(crate) state: K::State,
}

impl<K: RingKind> std::ops::Deref for RingOf<K> {
    type Target = K::State;

    fn deref(&self) -> &K::State {
        &self.state
    }
}

impl<K: RingKind> std::ops::DerefMut for RingOf<K> {
    fn deref_mut(&mut self) -> &mut K::State {
        &mut self.state
    }
}

impl<K: RingKind> RingOf<K> {
    pub(crate) fn new(state: K::State) -> Self {
        Self {
            entries: VecDeque::new(),
            base: 0,
            bytes: 0,
            state,
        }
    }

    pub(crate) fn head(&self) -> u64 {
        self.base + self.entries.len() as u64
    }

    /// Appends `e`, then drops entries from the front while the ring is over `limit`, always
    /// keeping the newest: a client at the head needs its boundary.
    pub(crate) fn push(&mut self, e: Arc<K::Entry>, limit: usize) {
        self.bytes += K::bytes(&e);
        self.entries.push_back(e);
        while self.bytes > limit && self.entries.len() > 1 {
            if let Some(e) = self.entries.pop_front() {
                self.bytes -= K::bytes(&e);
                self.base += 1;
                K::evicted(&mut self.state, &e);
            }
        }
    }
}

/// A single node's ring ([`RingKind`]): its commits as [`Entry`]s.
pub(crate) struct Single;

impl RingKind for Single {
    type Entry = Entry;
    type State = SingleRing;

    fn bytes(e: &Entry) -> usize {
        e.bytes
    }
}

/// Where a single node's hub feed is, beside its ring.
#[derive(Default)]
pub(crate) struct SingleRing {
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

type Ring = RingOf<Single>;

impl Ring {
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

/// A hub: its ring, its task, and its members.
pub(crate) struct HubOf<K: RingKind> {
    ring: Mutex<RingOf<K>>,
    /// The ring's head index, bumped by each push and by a failure.
    pub(crate) head: watch::Sender<u64>,
    /// Its task, once it's spawned after the hub is made ([`set_task`](Self::set_task),
    /// #1926): replaced each time the task moves between runtimes ([`drive`]).
    task: Mutex<Option<tokio::task::AbortHandle>>,
    /// Its clients, on its ring or on their own streams until they rejoin it: a [`JoinedOf`]
    /// each. At most one, a single node's hub task runs on the feed runtime (#1689).
    members: AtomicUsize,
    /// Its members that aren't a cluster hub's merge (#1798). While it has members and none
    /// of these, its task stays on the hub runtime: its readers are merges on the feed
    /// runtime, so running it beside them (#1689) only adds the engine's reads to the
    /// runtime that delivers every cluster subscriber's events (g100).
    external: AtomicUsize,
    /// Bumped by each join, so the task sees a client that joined and left between two of
    /// its looks at `members`.
    joins: AtomicU64,
    /// Whether its task runs on the feed runtime beside its one client (#1689), for the
    /// gauge and the dump.
    solo: AtomicBool,
    stats: Arc<FeedStats>,
    /// Live hubs of its kind: `--max-feed-hubs` counts them.
    live: Arc<AtomicU64>,
    /// The node's rings that split `--feed-ring-bytes`: every hub with a member.
    sharers: Arc<AtomicU64>,
    /// Its clients' trackers, for the dump; a gone client's is pruned there.
    clients: Mutex<Vec<Weak<Tracker>>>,
    /// Times it told its clients its head moved (an inspection count for tests, #1901).
    #[cfg(test)]
    notifies: AtomicU64,
}

pub(crate) type Hub = HubOf<Single>;

impl<K: RingKind> HubOf<K> {
    /// A hub with `ring`, holding a place in `live`. Its task is spawned after it, holding
    /// it, and set with [`set_task`](Self::set_task) (#1926).
    pub(crate) fn new(
        ring: RingOf<K>,
        solo: bool,
        stats: Arc<FeedStats>,
        live: Arc<AtomicU64>,
        sharers: Arc<AtomicU64>,
    ) -> Self {
        let (head, _) = watch::channel(ring.head());
        Self {
            ring: Mutex::new(ring),
            head,
            task: Mutex::new(None),
            members: AtomicUsize::new(0),
            external: AtomicUsize::new(0),
            joins: AtomicU64::new(0),
            solo: AtomicBool::new(solo),
            stats,
            live,
            sharers,
            clients: Mutex::default(),
            #[cfg(test)]
            notifies: AtomicU64::new(0),
        }
    }

    pub(crate) fn ring(&self) -> MutexGuard<'_, RingOf<K>> {
        self.ring.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn task(&self) -> MutexGuard<'_, Option<tokio::task::AbortHandle>> {
        self.task.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Sets its task's handle: the task starts after the hub is made, holding it (#1926).
    pub(crate) fn set_task(&self, task: tokio::task::AbortHandle) {
        *self.task() = Some(task);
    }

    /// Makes `tracker`'s client one of this hub's members until the [`JoinedOf`] drops;
    /// `internal`: a cluster hub's merge (#1798), which keeps the task on the hub runtime.
    pub(crate) fn join(self: &Arc<Self>, tracker: &Arc<Tracker>, internal: bool) -> JoinedOf<K> {
        self.track(tracker);
        // `external` before `members` (Release): whoever sees this membership in `members`
        // (Acquire, [`HubOf::internal`]) sees a client's `external` with it (cli692's N1).
        if !internal {
            self.external.fetch_add(1, Ordering::Relaxed);
        }
        if self.members.fetch_add(1, Ordering::Release) == 0 {
            self.sharers.fetch_add(1, Ordering::Relaxed);
        }
        self.joins.fetch_add(1, Ordering::Relaxed);
        JoinedOf(Arc::clone(self), internal)
    }

    /// Its members now.
    pub(crate) fn members(&self) -> usize {
        self.members.load(Ordering::Relaxed)
    }

    /// Its ring's cap: an equal share of `--feed-ring-bytes` (`total`) among the rings with
    /// a member, a cluster hub's and the partition hubs its merges read alike (#1798).
    pub(crate) fn ring_limit(&self, total: usize) -> usize {
        total / self.sharers.load(Ordering::Relaxed).max(1) as usize
    }

    /// Whether its members are all cluster hubs' merges, and there's at least one. `members`
    /// first: a client counted there is counted in `external` too ([`HubOf::join`]). A client
    /// leaving can still read as a merge for a moment, which costs at most one move to the
    /// hub runtime and back after `solo_after`.
    fn internal(&self) -> bool {
        self.members.load(Ordering::Acquire) > 0 && self.external.load(Ordering::Relaxed) == 0
    }

    /// Counts `tracker`'s client under this hub, for the dump. Gone clients and those that
    /// moved to another hub are pruned here whenever the list reaches a power of two, so it
    /// stays bounded on a server nobody dumps (perf287's R1 on #1643), and in the dump.
    pub(crate) fn track(&self, tracker: &Arc<Tracker>) {
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

    /// Its live clients' trackers, pruning gone ones and those counted under another hub.
    pub(crate) fn trackers(&self) -> Vec<Arc<Tracker>> {
        let me = std::ptr::from_ref(self) as usize;
        let mut c = self.clients.lock().unwrap_or_else(PoisonError::into_inner);
        c.retain(|w| {
            w.upgrade()
                .is_some_and(|t| t.hub.load(Ordering::Relaxed) == me)
        });
        c.iter().filter_map(Weak::upgrade).collect()
    }
}

impl Hub {
    fn push(&self, commit: Commit, token: Bytes, position: u64, total: usize) {
        let limit = self.ring_limit(total);
        // Encoded here, on the hub's task and outside the ring's lock.
        let seqno = commit.seqno();
        let mut entry = Entry::new(&commit, token.clone());
        let mut r = self.ring();
        entry.prev = r.passed;
        r.passed = position.max(seqno);
        r.passed_token = token;
        r.push(Arc::new(entry), limit);
    }

    /// Tells the clients the ring's head moved: once per batch of pushes ([`drive`], #1901).
    fn notify(&self) {
        let head = self.ring().head();
        self.head.send_replace(head);
        #[cfg(test)]
        self.notifies.fetch_add(1, Ordering::Relaxed);
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
        // The head's current value, so the watch says where the ring is (perf2's nit on #1938).
        self.notify();
    }
}

impl<K: RingKind> Drop for HubOf<K> {
    fn drop(&mut self) {
        // The last client left: the hub's feed (and its shard wakers) goes.
        if let Some(task) = self.task.get_mut().unwrap_or_else(PoisonError::into_inner) {
            task.abort();
        }
        self.live.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A client's membership of its hub, counted in the hub's `members` while held; `.1`:
/// a cluster hub's merge ([`HubOf::join`]).
pub(crate) struct JoinedOf<K: RingKind>(pub(crate) Arc<HubOf<K>>, bool);

type Joined = JoinedOf<Single>;

impl<K: RingKind> Drop for JoinedOf<K> {
    fn drop(&mut self) {
        let hub = &self.0;
        if hub.members.fetch_sub(1, Ordering::Relaxed) == 1 {
            hub.sharers.fetch_sub(1, Ordering::Relaxed);
        }
        if !self.1 {
            hub.external.fetch_sub(1, Ordering::Relaxed);
        }
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
    /// `Coalesce::hub`.
    coalesce: Duration,
    /// Pushes since the clients were last told ([`NOTIFY_EVERY`]).
    unnotified: u32,
    /// Commits read since the feed last had nothing.
    batch: u32,
    /// After a small batch, the feed isn't polled again before this ([`Coalesce`]).
    quiet_until: Option<Instant>,
    /// Set once the task has tried its hub with its first item (`Feeds::after_hub_spawn`'s
    /// hold seam, #1926).
    #[cfg(test)]
    started: Option<Arc<Started>>,
}

/// A drive task's first item handled: what [`Feeds::after_hub_spawn`] waits for (#1926).
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct Started(Mutex<bool>, std::sync::Condvar);

#[cfg(test)]
impl Started {
    fn set(&self) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.1.notify_all();
    }

    /// Waits up to `within` for the task's first item.
    pub(crate) fn wait(&self, within: Duration) -> bool {
        let g = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let (g, _) = self
            .1
            .wait_timeout_while(g, within, |set| !*set)
            .unwrap_or_else(PoisonError::into_inner);
        *g
    }
}

impl Driver {
    /// Where the task should run now, if not where it is: on the hub runtime at once with two
    /// or more clients, or with only cluster hubs' merges ([`HubOf::external`]); back on the
    /// feed runtime after `solo_after` with at most one other client.
    ///
    /// Asked only after a commit, so a hub left alone on the hub runtime by a quiet table
    /// stays there until its next commit. That's harmless: with no commits there's no
    /// handoff to save (perf287's N1 on #1716).
    fn moves(&mut self, hub: &Hub) -> Option<Place> {
        self.runtimes.as_ref()?;
        let shared = hub.members.load(Ordering::Relaxed) >= 2 || hub.internal();
        match self.place {
            Place::Feed => shared.then_some(Place::Hubs),
            Place::Hubs if shared => {
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
            // After a small batch (#1901): the commits arriving meanwhile are read together.
            if let Some(until) = d.quiet_until.take()
                && Instant::now() < until
            {
                tokio::time::sleep_until(until.into()).await;
            }
            // A poll that finds nothing records how far the feed got, so the ring's `passed`
            // follows other tables' writes (the engine wakes the hub on each frontier move).
            // It also ends a batch: the clients are told of its pushes, and after a small one
            // the next poll waits (`Coalesce::hub`).
            let item = std::future::poll_fn(|cx| {
                let polled = Pin::new(&mut d.stream).poll_next(cx);
                if polled.is_pending()
                    && let Some(hub) = d.hub.upgrade()
                {
                    let at = d.stream.position().seqno();
                    hub.note_passed(at, || token_of(&d.stream));
                    if d.unnotified > 0 {
                        hub.notify();
                        d.unnotified = 0;
                    }
                    if d.batch > 0 && d.batch <= SMALL_BATCH && !d.coalesce.is_zero() {
                        d.quiet_until = Some(Instant::now() + d.coalesce);
                    }
                    d.batch = 0;
                }
                polled
            })
            .await;
            // `None` only once every client has let the hub go: the task starts after its hub
            // is made (`Feeds::hub`, #1926).
            let hub = d.hub.upgrade();
            #[cfg(test)]
            if let Some(started) = d.started.take() {
                started.set();
            }
            let Some(hub) = hub else { return };
            match item {
                Some(Ok(commit)) => {
                    let at = d.stream.position().seqno();
                    hub.push(commit, token_of(&d.stream), at, d.ring_bytes);
                    d.batch = d.batch.saturating_add(1);
                    d.unnotified += 1;
                    if d.unnotified >= NOTIFY_EVERY {
                        hub.notify();
                        d.unnotified = 0;
                    }
                    if let Some(to) = d.moves(&hub)
                        && let Some((feed, hubs)) = d.runtimes.clone()
                    {
                        d.place = to;
                        d.alone = None;
                        // Its clients hear of every push before the task moves.
                        if d.unnotified > 0 {
                            hub.notify();
                            d.unnotified = 0;
                        }
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
                            *task = Some(rt.spawn(drive(d)).abort_handle());
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
    /// [`Spec::internal`].
    internal: bool,
    tables: Vec<Table>,
    prefix: Vec<u8>,
    visible: bool,
    sink: Sink,
    /// Where it is, for the dump.
    tracker: Arc<Tracker>,
}

/// Where a stream's events go: its connection's buffered channel, bounded by places and by
/// bytes, and ended by the idle limit when the connection stops taking. A single node's
/// client and a cluster subscription's merge (D317 ticket 7) both send through one.
pub(crate) struct Sink {
    tx: mpsc::Sender<(Queued, OwnedSemaphorePermit)>,
    /// The buffer's free bytes: each event or batch in the channel, or a batch the
    /// connection is still taking events from, holds its charge ([`EVENT_FLOOR`]).
    budget: Arc<Semaphore>,
    /// When the connection last took an event: a send waiting for room counts its
    /// `--stream-idle-ms` from it (simp724's I1 on #1536).
    taken: Arc<TakeClock>,
    client_bytes: usize,
    stream_idle: Duration,
    /// Where the stream starts, sent once it's known ([`Sink::started`], read by
    /// [`FeedEvents::start`]).
    start: Mutex<Option<oneshot::Sender<Bytes>>>,
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
        // Where it starts ([`FeedEvents::start`]): the ring's position it joins at, known
        // now. An own stream's, once its first read has taken the start (`read_own`): a
        // refused resume reports that read's error instead.
        if let Begin::Ring { token, .. } = &begin {
            self.sink.started(token);
        }
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
                xid: Default::default(),
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
            () = self.sink.closed() => Err(gone()),
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
        // After a small batch, the head isn't looked at again before this (`Coalesce`, #1901).
        let mut quiet_until: Option<Instant> = None;
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
            if batch.is_empty()
                && let Some(until) = quiet_until.take()
                && Instant::now() < until
            {
                tokio::select! {
                    () = self.sink.closed() => return Err(gone()),
                    () = tokio::time::sleep_until(until.into()) => {}
                }
                continue;
            }
            if batch.is_empty() {
                let quiet =
                    tokio::time::sleep_until((pos.last_event + self.feeds.limits.quiet).into());
                tokio::select! {
                    // A client that went away ends here, even one whose prefix never
                    // matches (it would otherwise hold its stream slot until a send failed).
                    () = self.sink.closed() => return Err(gone()),
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
            let n = batch.len() as u32;
            // The pass's events go to the connection as one batch (#1737's F3-0): one place in
            // its channel and one charge against its buffer, not one per event. Its entries
            // are held as `Arc`s, so the ring overrunning while the batch waits for room takes
            // none of them: the next read finds the client behind, and it goes on from
            // `pos.token`, the last commit covered here (simp724's (2)).
            let mut outs = Vec::with_capacity(batch.len());
            for e in batch.drain(..) {
                at += 1;
                if let Some(out) = self.cover_entry(&e, pos)? {
                    outs.push(out);
                }
            }
            self.send_all(outs, pos).await?;
            let client = self.feeds.limits.coalesce.client;
            if n <= SMALL_BATCH && !client.is_zero() {
                quiet_until = Some(Instant::now() + client);
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
        let before = pos.token.clone();
        let first =
            std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut *stream).poll_next(cx))).await;
        if !matches!(first, Poll::Ready(Some(Err(_)))) {
            // The start is taken: where it is ([`FeedEvents::start`]), before any commit.
            self.sink.started(&before);
        }
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
                    let joined = new.join(&self.tracker, self.internal);
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
                () = self.sink.closed() => return Err(gone()),
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
            let xid = commit.xid().unwrap_or_default();
            self.send(Out::event(commit_event(token.clone(), c, xid)), pos)
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
    /// and encoding the same event per subscriber was two thirds of the feed's CPU). The
    /// position moves past the entry here; the event, if any, is the caller's to send
    /// ([`Client::send_all`]).
    fn cover_entry(&self, e: &Arc<Entry>, pos: &mut Position) -> Result<Option<Out>, ApiError> {
        pos.seqno = Some(e.seqno);
        pos.token = e.token.clone();
        if e.seqno <= self.after || e.encoded().is_none() {
            return Ok(None);
        }
        if self.prefix.is_empty() {
            if self.skipping(e.seqno)? {
                return Ok(None);
            }
            return Ok(Some(Out::Shared(Arc::clone(e))));
        }
        // A prefix: the entry's changes to matching rows, if any (a later progress event
        // carries the position past one with none), the shared encoding when they all
        // match (#1780).
        match prefix_out(e, &self.prefix) {
            Some(_) if self.skipping(e.seqno)? => Ok(None),
            out => Ok(out),
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
                xid: Default::default(),
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

    /// Sends `events`, in order, as one batch where the client's buffer allows
    /// ([`Sink::put_all`]). Each event carries its own token, so a resume after any of them
    /// neither skips nor repeats.
    async fn send_all(&self, events: Vec<Out>, pos: &mut Position) -> Result<(), ApiError> {
        let Some(token) = events.last().map(|e| e.token().clone()) else {
            return Ok(());
        };
        self.sink.put_all(events).await?;
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
        let (start_tx, start_rx) = oneshot::channel();
        let sink = Sink {
            tx,
            budget: Arc::new(Semaphore::new(client_bytes)),
            taken: Arc::clone(&taken),
            client_bytes,
            stream_idle: limits.stream_idle,
            start: Mutex::new(Some(start_tx)),
        };
        let events = FeedEvents(
            EventSource::Sink {
                events: rx,
                draining: None,
                taken,
                end: Some(end_rx),
                start: Some(start_rx),
            },
            None,
            None,
        );
        (sink, events, end_tx)
    }

    /// The stream starts at `token`: for [`FeedEvents::start`], once (later calls do
    /// nothing).
    pub(crate) fn started(&self, token: &Bytes) {
        if let Some(tx) = self
            .start
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            let _ = tx.send(token.clone());
        }
    }

    /// Whether [`Sink::started`] is still to be told.
    pub(crate) fn starting(&self) -> bool {
        self.start
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    /// Whether events wait in the connection's buffer, untaken.
    pub(crate) fn pending(&self) -> bool {
        self.budget.available_permits() < self.client_bytes
    }

    /// When the connection last took an event ([`TakeClock`]: of a batch, its first and last
    /// event, and every event while a send waits).
    pub(crate) fn taken(&self) -> &TakeClock {
        &self.taken
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
        let bytes = charge(&event);
        self.queue(Queued::One(event), bytes).await
    }

    /// Queues `events`, in order, as few batches as the buffer allows: each batch is one
    /// place in the channel and one charge against the buffer, and the connection takes its
    /// events without going back to the channel (#1737's F3-0: the per-event channel pop,
    /// buffer release and clock read were about 500 cycles an event on the network workers
    /// at saturation, g129). A batch closes before an event that would take it past the
    /// buffer, so none is larger than the buffer but a single event that is.
    pub(crate) async fn put_all(&self, mut events: Vec<Out>) -> Result<(), ApiError> {
        while !events.is_empty() {
            let mut bytes = 0usize;
            let mut fit = 0;
            for e in &events {
                let c = charge(e);
                if fit > 0 && bytes.saturating_add(c) > self.client_bytes {
                    break;
                }
                bytes = bytes.saturating_add(c);
                fit += 1;
            }
            // The rest, if this batch closes early; usually none, and no allocation.
            let rest = if fit < events.len() {
                events.split_off(fit)
            } else {
                Vec::new()
            };
            let batch = if events.len() == 1 {
                Queued::One(events.pop().expect("one event"))
            } else {
                Queued::Many(events)
            };
            self.queue(batch, bytes).await?;
            events = rest;
        }
        Ok(())
    }

    /// Queues an event or a batch charged `bytes` (at most the whole buffer).
    async fn queue(&self, item: Queued, bytes: usize) -> Result<(), ApiError> {
        let n = u32::try_from(bytes.clamp(1, self.client_bytes)).unwrap_or(u32::MAX);
        // The fast path: room in the buffer and a place in the channel at once, with no
        // timer or waiter registered. Arming them on every send was 64% of the samples in
        // futex calls at 256 subscribers (perf287's profile on #1397).
        let (item, held) = match Arc::clone(&self.budget).try_acquire_many_owned(n) {
            Ok(room) => match self.tx.try_send((item, room)) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full((item, room))) => (item, Some(room)),
                Err(mpsc::error::TrySendError::Closed(_)) => return Err(gone()),
            },
            Err(tokio::sync::TryAcquireError::NoPermits) => (item, None),
            Err(tokio::sync::TryAcquireError::Closed) => return Err(gone()),
        };
        // The slow path, boxed: its timer and waits made this future 1.2 KB, built (and
        // copied) for every event though the fast path needs none of it (#1758's g86c).
        Box::pin(self.wait_to_put(item, held, n)).await
    }

    /// [`Sink::queue`]'s slow path: waits for room in the buffer (unless `held`) and a place
    /// in the channel, within the idle limit.
    async fn wait_to_put(
        &self,
        item: Queued,
        held: Option<OwnedSemaphorePermit>,
        n: u32,
    ) -> Result<(), ApiError> {
        let budget = Arc::clone(&self.budget);
        let queued = async {
            let room = match held {
                Some(room) => room,
                None => budget.acquire_many_owned(n).await.map_err(|_| ())?,
            };
            self.tx.send((item, room)).await.map_err(|_| ())
        };
        tokio::pin!(queued);
        // The idle limit counts from the client's last take, not from the start of the wait:
        // a large event may wait for most of the buffer to drain, and a client taking events
        // all along is slow, not idle (simp724's I1 on #1536). One timer: when it fires, a
        // take since the wait began moves the deadline on, and it waits again. Watched, so
        // takes within a batch are stamped too ([`TakeClock`]), from before the wait begins.
        let _watch = self.taken.watch();
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
                    return Err(idle_error(idle));
                }
            }
        }
    }
}

/// The error a feed stream whose client took no event for `idle` (`--stream-idle-ms`) ends
/// with, on a single node or a cluster (#2021): resumable from the last event's token.
pub(crate) fn idle_error(idle: Duration) -> ApiError {
    ApiError::deadline(format!(
        "the client took no feed event for --stream-idle-ms ({} ms); resume from the last \
         event's token",
        idle.as_millis()
    ))
}

/// The error a send to a client that went away ends with.
fn gone() -> ApiError {
    ApiError::internal("the client went away")
}

/// What an event costs a client's buffer: its encoded size, at least [`EVENT_FLOOR`].
fn charge(event: &Out) -> usize {
    event.encoded_len().max(EVENT_FLOOR)
}

/// A place in a client's channel: one event, or a batch of them, in order ([`Sink::put_all`]).
enum Queued {
    One(Out),
    /// At least two events.
    Many(Vec<Out>),
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
pub(crate) struct FeedEvents(
    EventSource,
    Option<Box<DeliveryLease>>,
    /// A range move's target's component: the barriers it carries (D331 PR 3b).
    Option<Box<Barriers>>,
);

/// A range move's hand-over in its target's cluster component (D331 PR 3b, cli692's (d)):
/// the component hands it out just before its first event past `t_from` (feed1248's M4: any
/// event that moves its position there, progress included), so the entry node's merge holds
/// the component until the source's has passed `final_seqno`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Barrier {
    /// The target's seqno at the hand-over: its feed token's, `token`.
    pub t_from: u64,
    /// The target's D310 feed token at `t_from`: the barrier event's token, so the merge's
    /// position for the component stays there while it holds it (feed1248's M6).
    pub token: Bytes,
    pub move_id: String,
    pub source: String,
    pub final_seqno: u64,
}

/// A component's barriers still to hand out, in `(t_from, move id)` order (feed1248's S4),
/// and an event held back behind one.
#[derive(Default)]
struct Barriers {
    pending: std::collections::VecDeque<Barrier>,
    held: Option<Out>,
    /// A cluster component of a cluster partition (PR 3b): its partition's gate and its
    /// tables, so a hand-over the partition takes while the component runs (its serving
    /// row written at CLEANING) adds that barrier too, not only those known at its open.
    watch: Option<(Arc<super::gate::WriteGate>, Vec<String>)>,
    /// The gate's serving rows' version last looked at, the moves already barriered, and the
    /// last seqno handed out (a barrier at or past it still comes in time).
    version: u64,
    known: Vec<String>,
    last: u64,

    /// A component of tables a range move took from its partition whole: it ends, cleanly
    /// for the merge (`MovedAway`), once it has handed out an event at or past this seqno.
    ends_at: Option<u64>,
    /// It has: `MovedAway` next, then nothing.
    done: Option<bool>,
}

/// A hold seam (tests): a one-shot closure a component of the named partition runs between
/// reading an event and loading its gate's serving-row version (D331 PR 3b, feed1248's
/// point 1), so a test can take a hand-over exactly there. A no-op when unset.
#[cfg(test)]
pub(crate) static AFTER_READ: std::sync::Mutex<Option<AfterRead>> = std::sync::Mutex::new(None);

/// [`AFTER_READ`]'s hook: the partition, and what runs there.
#[cfg(test)]
pub(crate) type AfterRead = (String, Box<dyn FnOnce() + Send>);

impl Barriers {
    /// Runs [`AFTER_READ`]'s closure, if it's set for this component's partition.
    #[cfg(test)]
    fn after_read(&self) {
        let Some((gate, _)) = &self.watch else {
            return;
        };
        let hook = {
            let mut slot = AFTER_READ
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match slot.take() {
                Some((p, f)) if p == gate.partition() => Some(f),
                other => {
                    *slot = other;
                    None
                }
            }
        };
        if let Some(f) = hook {
            f();
        }
    }

    /// The barriers a hand-over taken since the last look adds, and the end a move that took
    /// the component's last table here sets (one load while neither changed).
    fn refresh(&mut self) {
        let Some((gate, tables)) = &self.watch else {
            return;
        };
        let version = gate.ranges().rows_version();
        if version == self.version {
            return;
        }
        self.version = version;
        // Every table moved away whole, each with its row (feed1248's M3 on PR 3b): read up to
        // the moves' last seqno, then end, as a component opened after the move does
        // (`moved_end`). Without this, a subscription connected across the move kept the
        // partition in its tokens, and a reconnect after PR 5 pruned the row would expire.
        if self.ends_at.is_none()
            && let Some(state) = gate.ranges().load().as_ref()
            && tables.iter().all(|t| state.moved_whole(t))
        {
            let rows = gate.ranges().moved_rows();
            let ends: Option<Vec<u64>> = tables
                .iter()
                .map(|t| {
                    rows.iter()
                        .filter(|m| &m.table == t)
                        .map(|m| m.final_seqno)
                        .max()
                })
                .collect();
            if let Some(end) = ends.and_then(|e| e.into_iter().max()) {
                self.ends_at = Some(end);
                if self.last >= end {
                    self.done = Some(true);
                }
            }
        }
        let mut added = false;
        for m in gate.ranges().serving_rows() {
            if m.token.is_empty() || !tables.contains(&m.table) || self.known.contains(&m.id) {
                continue;
            }
            self.known.push(m.id.clone());
            // Already past its `t_from` (events of the partition's other ranges, read between
            // the hand-over's token and its row): the range takes writes only once the gate
            // opens, after the row, so the barrier still comes before any of them, now, and
            // without a token, so the merge's position for the component doesn't move
            // (feed1248's point 2: a token past `t_from` checkpointed while held would let a
            // resume start past the barrier).
            let (t_from, token) = if m.seqno < self.last {
                (self.last, Bytes::new())
            } else {
                (m.seqno, m.token.into())
            };
            self.pending.push_back(Barrier {
                t_from,
                token,
                move_id: m.id,
                source: m.source,
                final_seqno: m.final_seqno,
            });
            added = true;
        }
        if added {
            self.pending
                .make_contiguous()
                .sort_by(|a, b| (a.t_from, &a.move_id).cmp(&(b.t_from, &b.move_id)));
        }
    }

    /// `out` is going out: past `ends_at`, the component ends next; and its position.
    fn passed(&mut self, out: &Out) {
        if let Some(s) = crate::serve::cluster::token::token_seqno(out.token()) {
            self.last = self.last.max(s);
        }
        if let Some(end) = self.ends_at
            && crate::serve::cluster::token::token_seqno(out.token()).is_some_and(|s| s >= end)
        {
            self.done = Some(true);
        }
    }

    /// The barrier due before `out`, if one is: the next one, when `out`'s token is past its
    /// `t_from`.
    fn due(&mut self, out: &Out) -> Option<Barrier> {
        let next = self.pending.front()?;
        // A component's commits carry D310 tokens, and its other events one or none
        // (feed1248's S2 and its nit: a tokenless commit past `t_from` is exactly what would
        // slip by unheld). Failing closed (handing the barrier out) isn't safe: its token
        // would move the merge's position to `t_from` ahead of events still to come.
        debug_assert!(
            crate::serve::cluster::token::token_seqno(out.token()).is_some()
                || (out.token().is_empty() && !out.is_commit()),
            "a component's commit without a D310 token, or an event with another token"
        );
        let seqno = crate::serve::cluster::token::token_seqno(out.token())?;
        if seqno > next.t_from {
            self.pending.pop_front()
        } else {
            None
        }
    }
}

impl Barrier {
    fn out(self) -> Out {
        Out::event(proto::FeedEvent {
            token: self.token,
            event: Some(proto::feed_event::Event::MoveBarrier(proto::MoveBarrier {
                move_id: self.move_id,
                source: self.source,
                final_seqno: self.final_seqno,
            })),
            partition: String::new(),
            // A barrier isn't a commit: no transaction's id (D329 2b).
            xid: Default::default(),
        })
    }
}

impl EventSource {
    /// The take clock the sink's idle limit reads.
    fn taken(&self) -> &TakeClock {
        let EventSource::Sink { taken, .. } = self;
        taken
    }
}

enum EventSource {
    Sink {
        events: mpsc::Receiver<(Queued, OwnedSemaphorePermit)>,
        /// The rest of a batch taken from the channel, and its charge: held until its last
        /// event is taken, so the buffer's bound and [`Sink::pending`] count a batch the
        /// connection holds under back-pressure (simp724's (3) on #1737's F3-0).
        draining: Option<(std::vec::IntoIter<Out>, OwnedSemaphorePermit)>,
        /// Each take's time, for a send waiting for room.
        taken: Arc<TakeClock>,
        end: Option<oneshot::Receiver<ApiError>>,
        /// Where the stream starts, once its task knows ([`FeedEvents::start`]).
        start: Option<oneshot::Receiver<Bytes>>,
    },
}

impl std::fmt::Debug for FeedEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.0 {
            EventSource::Sink { .. } => "FeedEvents(sink)",
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
    /// A cluster feed hub's entry (#1798): encoded once for every member, as `Shared` is.
    Merged(Arc<crate::serve::cluster::hub::ClusterEntry>),
}

impl Out {
    pub(crate) fn event(e: proto::FeedEvent) -> Self {
        Out::Event(Box::new(e))
    }

    /// Whether it's a commit (a ring entry always is). Only for the barriers' debug check.
    fn is_commit(&self) -> bool {
        match self {
            Out::Event(e) => matches!(e.event, Some(proto::feed_event::Event::Commit(_))),
            Out::Shared(_) => true,
            Out::Spliced { .. } | Out::Merged(_) => false,
        }
    }

    fn token(&self) -> &Bytes {
        static NONE: Bytes = Bytes::new();
        match self {
            Out::Event(e) => &e.token,
            Out::Shared(e) => &e.token,
            Out::Spliced { .. } => &NONE,
            Out::Merged(e) => &e.token,
        }
    }

    /// Its size on the wire, for the client's buffer.
    pub(crate) fn encoded_len(&self) -> usize {
        match self {
            Out::Event(e) => prost::Message::encoded_len(e),
            Out::Shared(e) => e.encoded().map_or(0, Bytes::len),
            Out::Spliced { body, tail } => body.len() + tail.len(),
            Out::Merged(e) => e.body.len() + e.tail.len(),
        }
    }

    /// The event's encoding, as one buffer (a cluster component's event, to splice).
    pub(crate) fn to_bytes(&self) -> Bytes {
        match self {
            Out::Event(e) => Bytes::from(prost::Message::encode_to_vec(e)),
            Out::Shared(e) => e.encoded().cloned().unwrap_or_default(),
            Out::Spliced { body, tail } => [body.as_ref(), tail.as_ref()].concat().into(),
            Out::Merged(e) if e.tail.is_empty() => e.body.clone(),
            Out::Merged(e) => [e.body.as_ref(), e.tail.as_ref()].concat().into(),
        }
    }

    /// The event as a message (HTTP, which renders JSON, and tests).
    pub(crate) fn into_proto(self) -> proto::FeedEvent {
        match self {
            Out::Event(e) => *e,
            Out::Shared(e) => e
                .encoded()
                .and_then(|b| <proto::FeedEvent as prost::Message>::decode(b.clone()).ok())
                .unwrap_or_else(|| {
                    commit_event(e.token.clone(), proto::FeedCommit::default(), b"")
                }),
            Out::Spliced { .. } | Out::Merged(_) => {
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
            Out::Merged(e) => {
                buf.put_slice(&e.body);
                buf.put_slice(&e.tail);
                Ok(())
            }
        }
    }
}

impl FeedEvents {
    /// Where the stream starts: a token that resumes after everything it will deliver comes
    /// before (#1915 PR 3: `/mcp`'s `feed_poll` returns it when no commit arrives, as the
    /// library's `Feed::token()` at open does). A single node's is its start's; a cluster
    /// subscription's, once every partition's part has placed it. Waits until then; asked
    /// again, the same token. The stream's error if it ends first.
    pub(crate) async fn start(&mut self) -> Result<Vec<u8>, ApiError> {
        let EventSource::Sink { start, end, .. } = &mut self.0;
        let Some(rx) = start.as_mut() else {
            return Err(ApiError::internal("a stream's start asked for twice"));
        };
        match rx.await {
            Ok(token) => {
                *start = None;
                Ok(token.to_vec())
            }
            Err(_) => {
                // The stream's task ended first: its error, which it sends as it ends.
                *start = None;
                Err(match end.take() {
                    Some(e) => e.await.unwrap_or_else(|_| {
                        ApiError::unavailable("the subscription ended before it started")
                    }),
                    None => ApiError::unavailable("the subscription ended before it started"),
                })
            }
        }
    }

    /// These events, delivered only while cluster partition `gate`'s lease holds (D326 PR
    /// 2): they wait while it has had none since it opened and the node reaches a majority,
    /// and end with `PartitionUnavailable` once it lapses or the node doesn't (feed1248's
    /// F2). Refused at once in that last case, so a subscribe never starts there.
    pub(crate) fn leased(mut self, gate: Arc<super::gate::WriteGate>) -> Result<Self, ApiError> {
        self.1 = Some(Box::new(DeliveryLease::new(gate)?));
        Ok(self)
    }

    /// The next event as it left the client, then the client's error if it failed.
    ///
    /// On a cluster partition, the lease is checked on every poll ([`DeliveryLease`]): that
    /// replaces a check once the subscribe is in place (feed1248's F3, `still_leased`), since
    /// a lease that lapsed between the subscribe's checks and its first poll ends it here.
    /// Carries `barriers` (D331 PR 3b): each handed out just before the first event past its
    /// `t_from`. Sorted here. Ends past `ends_at`, if set, with `MovedAway`. With `watch`, a
    /// cluster partition's gate and the component's tables, it takes a hand-over's barrier
    /// while it runs too.
    pub(crate) fn with_barriers(
        mut self,
        mut barriers: Vec<Barrier>,
        ends_at: Option<u64>,
        watch: Option<(Arc<super::gate::WriteGate>, Vec<String>)>,
    ) -> Self {
        if barriers.is_empty() && ends_at.is_none() && watch.is_none() {
            return self;
        }
        barriers.sort_by(|a, b| (a.t_from, &a.move_id).cmp(&(b.t_from, &b.move_id)));
        let version = watch.as_ref().map_or(0, |(g, _)| g.ranges().rows_version());
        self.2 = Some(Box::new(Barriers {
            known: barriers.iter().map(|b| b.move_id.clone()).collect(),
            pending: barriers.into(),
            held: None,
            watch,
            version,
            last: 0,
            ends_at,
            done: None,
        }));
        self
    }

    /// The next event, a due barrier first (D331 PR 3b): one load while the component
    /// carries none.
    pub(crate) fn poll_out(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Out, ApiError>>> {
        if self.2.is_none() {
            return self.poll_out_inner(cx);
        }
        if let Some(barriers) = self.2.as_mut() {
            match barriers.done {
                Some(true) => {
                    barriers.done = Some(false);
                    return Poll::Ready(Some(Err(ApiError::moved_away(
                        "this component's tables moved to other partitions: it has read up \
                         to their range moves' last seqno",
                    ))));
                }
                Some(false) => return Poll::Ready(None),
                None => {}
            }
            if let Some(held) = barriers.held.take() {
                // Another barrier may be due before the same event.
                if let Some(b) = barriers.due(&held) {
                    barriers.held = Some(held);
                    return Poll::Ready(Some(Ok(b.out())));
                }
                barriers.passed(&held);
                return Poll::Ready(Some(Ok(held)));
            }
        }
        let out = self.as_mut().poll_out_inner(cx);
        if let Poll::Ready(Some(Ok(event))) = out {
            let barriers = self.2.as_mut().expect("checked above");
            // After the read, before the send (feed1248's point 1): the hand-over writes its
            // serving row (bumping the version, Release) before its gate opens the range, so
            // a commit of the range this read returned was published after it, and the
            // reader's Acquire of that commit happens before this load (Acquire), which sees
            // the row. A load before the read could miss it and send the commit unheld.
            #[cfg(test)]
            barriers.after_read();
            barriers.refresh();
            return Poll::Ready(Some(Ok(match barriers.due(&event) {
                Some(b) => {
                    barriers.held = Some(event);
                    b.out()
                }
                None => {
                    barriers.passed(&event);
                    event
                }
            })));
        }
        out
    }

    fn poll_out_inner(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Out, ApiError>>> {
        // Without a lease, the sink's own end comes first (audit120's S-C5), and each look
        // while waiting for a first lease counts as a take (feed1248's S1): see
        // [`DeliveryLease::before`].
        if let Some(mut lease) = self.1.take() {
            let gated = lease.before(cx, &mut *self);
            self.1 = Some(lease);
            if let Some(answer) = gated {
                return answer;
            }
        }
        let out = self.as_mut().poll_events(cx);
        if let Some(lease) = self.1.as_mut() {
            lease.after(out.is_pending(), cx);
        }
        out
    }

    /// The sink's error, if it has ended with one, without taking its events.
    fn poll_end(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<ApiError>> {
        let EventSource::Sink { end, .. } = &mut self.0;
        let Some(rx) = end.as_mut() else {
            return Poll::Pending;
        };
        match Pin::new(rx).poll(cx) {
            Poll::Ready(Ok(e)) => {
                *end = None;
                Poll::Ready(Some(e))
            }
            Poll::Ready(Err(_)) | Poll::Pending => Poll::Pending,
        }
    }

    /// The sink's next event, then its error if it failed. A batch's events come from the
    /// batch, not the channel: one channel pop, one release of its charge (after its last
    /// event) and two clock reads a batch, not one of each an event (#1737's F3-0).
    ///
    /// Each event still spends a unit of the task's Tokio budget, as a channel pop does: the
    /// connection's body task (hyper's `PipeToSendStream`) polls for frames until this goes
    /// `Pending`, so with the batch's events free it would run up to 128 batches on its
    /// network worker before yielding it, and the puts that worker serves would wait (#2027).
    fn poll_events(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Out, ApiError>>> {
        let (events, draining, taken, end) = match &mut self.0 {
            EventSource::Sink {
                events,
                draining,
                taken,
                end,
                ..
            } => (events, draining, taken, end),
        };
        if let Some((rest, _)) = draining.as_mut()
            && rest.len() > 0
        {
            // Out of budget: nothing taken, and the task is woken once it has yielded. The
            // batch's first event paid through the channel's pop.
            let coop = std::task::ready!(tokio::task::coop::poll_proceed(cx));
            let e = rest.next().expect("a batch event left");
            coop.made_progress();
            if rest.len() == 0 {
                // Its last event: the batch's bytes go back to the client's buffer.
                *draining = None;
                taken.took();
            } else {
                taken.took_within();
            }
            return Poll::Ready(Some(Ok(e)));
        }
        *draining = None;
        match events.poll_recv(cx) {
            // Taken: its bytes go back to the client's buffer.
            Poll::Ready(Some((Queued::One(e), room))) => {
                drop(room);
                taken.took();
                return Poll::Ready(Some(Ok(e)));
            }
            // A batch: its bytes stay charged until its last event is taken.
            Poll::Ready(Some((Queued::Many(batch), room))) => {
                taken.took();
                let mut rest = batch.into_iter();
                let e = rest.next().expect("a batch holds at least two events");
                *draining = Some((rest, room));
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
}

impl super::lease::LeaseHooks for FeedEvents {
    fn poll_own_end(&mut self, cx: &mut Context<'_>) -> Poll<Option<ApiError>> {
        Pin::new(self).poll_end(cx)
    }

    fn took(&self) {
        self.0.taken().took();
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
        let dir = crate::test_dir::fresh(&format!("phdb-feed-{name}-"));
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

    /// The next of `events`' items.
    async fn next_out(events: &mut FeedEvents) -> Option<Result<Out, ApiError>> {
        std::future::poll_fn(|cx| Pin::new(&mut *events).poll_out(cx)).await
    }

    /// D331 PR 3b: a range move's target's component hands out each barrier just before
    /// its first event past the barrier's `t_from` (feed1248's M4), several in `(t_from, id)`
    /// order (S4), none before an event at exactly `t_from` (M5's start there), and a
    /// component of tables moved away whole ends with `MovedAway` once it has handed out an
    /// event at or past its end, then nothing.
    #[tokio::test]
    async fn a_targets_component_hands_out_its_barriers_before_the_events_past_them() {
        let token = |seqno: u64| {
            let mut t = vec![1u8];
            t.extend_from_slice(&[7u8; 16]);
            t.extend_from_slice(&1u32.to_le_bytes());
            t.extend_from_slice(&seqno.to_le_bytes());
            Bytes::from(t)
        };
        let (sink, events, _end) = Sink::new(&limits_solo_after(SOLO_AFTER));
        for seqno in [5, 10, 15, 20] {
            sink.put(Out::event(proto::FeedEvent {
                token: token(seqno),
                ..Default::default()
            }))
            .await
            .unwrap();
        }
        let barrier = |t_from, id: &str| Barrier {
            t_from,
            token: token(t_from),
            move_id: id.into(),
            source: "p1".into(),
            final_seqno: 99,
        };
        let mut events = events.with_barriers(
            vec![
                barrier(12, "mv2"),
                barrier(8, "mv1"),
                barrier(12, "mv1"),
                barrier(5, "mv0"),
            ],
            Some(15),
            None,
        );
        let mut seen = Vec::new();
        loop {
            match next_out(&mut events).await {
                Some(Ok(out)) => {
                    let e = out.into_proto();
                    let seqno = crate::serve::cluster::token::token_seqno(&e.token).unwrap();
                    seen.push(match e.event {
                        Some(proto::feed_event::Event::MoveBarrier(b)) => {
                            format!("{}@{seqno}", b.move_id)
                        }
                        _ => format!("{seqno}"),
                    });
                }
                Some(Err(e)) => {
                    seen.push(e.code);
                    break;
                }
                None => break,
            }
        }
        assert_eq!(
            seen,
            [
                "5",
                "mv0@5",
                "mv1@8",
                "10",
                "mv1@12",
                "mv2@12",
                "15",
                "MovedAway"
            ]
        );
        assert!(next_out(&mut events).await.is_none(), "and nothing after");
    }

    /// D331 PR 3b (feed1248's point 1): a hand-over taken between a running component's
    /// read of an event and its look at the serving rows still puts the barrier before that
    /// event. Mutant: load the version before the read, and this fails (the event goes out
    /// unheld).
    #[tokio::test]
    async fn a_hand_over_taken_after_a_read_still_puts_its_barrier_first() {
        use crate::serve::gate::{LeaseClock, WriteGate};
        let token = |seqno: u64| {
            let mut t = vec![1u8];
            t.extend_from_slice(&[7u8; 16]);
            t.extend_from_slice(&1u32.to_le_bytes());
            t.extend_from_slice(&seqno.to_le_bytes());
            Bytes::from(t)
        };
        let gate = WriteGate::opened("seam-p", false, Arc::new(LeaseClock::default()));
        let (sink, events, _end) = Sink::new(&limits_solo_after(SOLO_AFTER));
        sink.put(Out::event(proto::FeedEvent {
            token: token(20),
            ..Default::default()
        }))
        .await
        .unwrap();
        let mut events = events.with_barriers(
            Vec::new(),
            None,
            Some((Arc::clone(&gate), vec!["users".into()])),
        );
        let hand_over = {
            let gate = Arc::clone(&gate);
            let token = token(10);
            move || {
                gate.ranges().add_serving(crate::serve::ranges::Moved {
                    id: "mv1".into(),
                    table: "users".into(),
                    start: b"c".to_vec(),
                    end: Some(b"f".to_vec()),
                    seqno: 10,
                    source: "p1".into(),
                    final_seqno: 99,
                    token: token.to_vec(),
                    ..Default::default()
                });
            }
        };
        *AFTER_READ.lock().unwrap() = Some(("seam-p".into(), Box::new(hand_over)));
        let first = next_out(&mut events).await.unwrap().unwrap().into_proto();
        assert!(
            matches!(first.event, Some(proto::feed_event::Event::MoveBarrier(_))),
            "the barrier first: {first:?}"
        );
        let second = next_out(&mut events).await.unwrap().unwrap().into_proto();
        assert_eq!(
            crate::serve::cluster::token::token_seqno(&second.token),
            Some(20)
        );
    }

    /// D326 PR 2 (feed1248's F2, F3 and F5): a cluster partition's component delivers only
    /// under its lease. Without one since it opened (and the node reaching a majority), it
    /// waits, delivering nothing, its catch-up included; once leased, it delivers; when the
    /// lease runs out, it ends with the lease's error even with nothing to deliver; and a
    /// node whose last round reached no majority refuses the subscribe.
    #[tokio::test]
    async fn a_cluster_components_events_wait_for_a_lease_and_end_when_it_runs_out() {
        use crate::serve::gate::{LeaseClock, WriteGate};
        let clock = Arc::new(LeaseClock::default());
        let gate = WriteGate::opened("p1", false, Arc::clone(&clock));
        let (sink, events, _end) = Sink::new(&limits_solo_after(SOLO_AFTER));
        sink.put(Out::event(proto::FeedEvent::default()))
            .await
            .unwrap();
        let mut events = events.leased(Arc::clone(&gate)).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(300), next_out(&mut events))
                .await
                .is_err(),
            "nothing before the lease"
        );
        gate.renew(clock.now() + 400_000_000);
        let first = tokio::time::timeout(Duration::from_secs(5), next_out(&mut events))
            .await
            .expect("delivered once leased");
        assert!(matches!(first, Some(Ok(_))));
        // Quiet: no event to carry it, and the lease runs out.
        let end = tokio::time::timeout(Duration::from_secs(5), next_out(&mut events))
            .await
            .expect("woken at the lease's end");
        let Some(Err(e)) = end else {
            panic!("the lease's error, not an event or the end");
        };
        assert_eq!(e.code, "PartitionUnavailable", "{}", e.message);
        assert!(e.message.contains("ran out"), "{}", e.message);
        assert!(next_out(&mut events).await.is_none(), "then the end");

        // A node whose last round reached no majority: refused at once.
        clock.set_majority(false);
        let gate = WriteGate::opened("p2", false, Arc::clone(&clock));
        let (_sink, events, _end) = Sink::new(&limits_solo_after(SOLO_AFTER));
        let e = events.leased(gate).unwrap_err();
        assert_eq!(e.code, "PartitionUnavailable", "{}", e.message);
    }

    /// feed1248's S1 on #1912: a component waiting for its partition's first lease takes
    /// nothing, and its sink's idle limit counts from the last take. Each look while it waits
    /// counts as one, so a backlog that fills the buffer meanwhile isn't dropped as idle; once
    /// leased, every event is delivered.
    #[tokio::test]
    async fn a_component_waiting_for_its_lease_isnt_dropped_as_idle() {
        use crate::serve::gate::{LeaseClock, WriteGate};
        let clock = Arc::new(LeaseClock::default());
        let gate = WriteGate::opened("p1", false, Arc::clone(&clock));
        let limits = FeedLimits {
            stream_idle: Duration::from_millis(300),
            client_events: 1,
            ..limits_solo_after(SOLO_AFTER)
        };
        let (sink, events, _end) = Sink::new(&limits);
        let mut events = events.leased(Arc::clone(&gate)).unwrap();
        let sink = Arc::new(sink);
        let putter = {
            let sink = Arc::clone(&sink);
            tokio::spawn(async move {
                for _ in 0..3 {
                    sink.put(Out::event(proto::FeedEvent::default())).await?;
                }
                Ok::<(), ApiError>(())
            })
        };
        // Four idle limits of waiting, the buffer full behind it.
        assert!(
            tokio::time::timeout(Duration::from_millis(1200), next_out(&mut events))
                .await
                .is_err()
        );
        assert!(!putter.is_finished(), "the putter still waits for room");
        gate.renew(clock.now() + 3_600_000_000_000);
        for i in 0..3 {
            let got = tokio::time::timeout(Duration::from_secs(5), next_out(&mut events))
                .await
                .unwrap_or_else(|_| panic!("event {i} delivered once leased"));
            assert!(matches!(got, Some(Ok(_))), "event {i}");
        }
        putter.await.unwrap().unwrap();
    }

    /// A hub's tracker list stays bounded without a dump (perf287's R1 on #1643): gone
    /// clients and those counted under another hub are pruned as it grows.
    #[test]
    fn a_hubs_tracker_list_stays_bounded_without_a_dump() {
        let hub = || {
            Hub::new(
                Ring::new(SingleRing::default()),
                false,
                Arc::default(),
                Arc::new(AtomicU64::new(1)),
                Arc::default(),
            )
        };
        let (a, b) = (hub(), hub());
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
        let mut r = Ring::new(SingleRing {
            start_seqno: start,
            passed: commits.last().map_or(start, Commit::seqno),
            ..SingleRing::default()
        });
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
                coalesce: Coalesce::default(),
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
            internal: false,
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
            first.task().as_ref().expect("spawned").abort();
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
            rt(1, Some("phdb-serve-hub")),
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

    /// A small event whose token is `i`, so the order it comes out in can be checked.
    fn ev(i: u8) -> Out {
        Out::event(proto::FeedEvent {
            token: Bytes::from(vec![i]),
            ..Default::default()
        })
    }

    /// The token of the next event `events` gives, which must be one.
    async fn next_token(events: &mut FeedEvents) -> u8 {
        match next_out(events).await {
            Some(Ok(e)) => e.token()[0],
            other => panic!("an event, not {:?}", other.map(|r| r.err())),
        }
    }

    /// #1737's F3-0: a batch's events come out in order, one channel place for all of them,
    /// and the batch's charge stays on the client's buffer until its last event is taken
    /// (simp724's (3)), so `pending` says so while the connection holds the rest.
    #[tokio::test]
    async fn a_batch_is_taken_in_order_and_holds_its_charge_to_its_last_event() {
        let (sink, mut events, _end) = Sink::new(&limits_solo_after(SOLO_AFTER));
        sink.put_all((0..5).map(ev).collect()).await.unwrap();
        assert_eq!(
            sink.budget.available_permits(),
            sink.client_bytes - 5 * EVENT_FLOOR,
            "five tiny events, each charged the floor"
        );
        assert_eq!(sink.tx.max_capacity() - sink.tx.capacity(), 1, "one place");
        for i in 0..5 {
            assert!(
                sink.pending(),
                "charged until the batch's last event is taken"
            );
            assert_eq!(next_token(&mut events).await, i);
        }
        assert!(!sink.pending(), "the batch's charge is back");
    }

    /// simp724's (1) on #1737's F3-0: tiny events are charged [`EVENT_FLOOR`] each, and a
    /// batch closes before an event that would take it past the buffer. With room for four,
    /// ten events go as batches of four, four and two; each waits for the one before to be
    /// taken to its last event, and all ten come out in order.
    #[tokio::test]
    async fn a_batch_closes_before_the_buffer_and_tiny_events_are_charged_a_floor() {
        let limits = FeedLimits {
            client_bytes: 4 * EVENT_FLOOR,
            ..limits_solo_after(SOLO_AFTER)
        };
        let (sink, mut events, _end) = Sink::new(&limits);
        let sink = Arc::new(sink);
        let putter = {
            let sink = Arc::clone(&sink);
            tokio::spawn(async move { sink.put_all((0..10).map(ev).collect()).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(sink.budget.available_permits(), 0, "four fill the buffer");
        for i in 0..3 {
            assert_eq!(next_token(&mut events).await, i);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !putter.is_finished(),
            "the next four wait for the last of the first"
        );
        assert_eq!(
            sink.tx.max_capacity() - sink.tx.capacity(),
            0,
            "nothing queued"
        );
        for i in 3..10 {
            assert_eq!(next_token(&mut events).await, i);
        }
        putter.await.unwrap().unwrap();
        assert!(!sink.pending());
    }

    /// simp724's I1 on #1536, with batches (#1737's F3-0): a client that takes a batch's
    /// events one every 200 ms, under the 250 ms idle limit, while a send waits for the room
    /// the batch holds, is slow, not idle. Takes within a batch are stamped while a send
    /// waits, so the wait counts from the last of them. On a paused clock, so exact.
    #[tokio::test(start_paused = true)]
    async fn a_client_taking_a_batch_slowly_is_slow_not_idle() {
        let limits = FeedLimits {
            client_bytes: 4 * EVENT_FLOOR,
            stream_idle: Duration::from_millis(250),
            ..limits_solo_after(SOLO_AFTER)
        };
        let (sink, mut events, _end) = Sink::new(&limits);
        let sink = Arc::new(sink);
        sink.put_all((0..4).map(ev).collect()).await.unwrap();
        let putter = {
            let sink = Arc::clone(&sink);
            tokio::spawn(async move { sink.put(ev(4)).await })
        };
        for i in 0..4 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert_eq!(next_token(&mut events).await, i);
        }
        putter
            .await
            .unwrap()
            .expect("not timed out: the client took all along");
        assert_eq!(next_token(&mut events).await, 4);
    }

    /// #2027: a batch's events spend the task's Tokio budget as channel pops do, so a
    /// connection taking a long batch gives its worker back every 128 events at most (the
    /// first event's pop included), and gets every event, in order, across the yields.
    /// `gate` leases the stream (simp724): a serving lease changes none of that.
    async fn a_long_batch_yields_every_budget(gate: Option<Arc<crate::serve::gate::WriteGate>>) {
        const N: u16 = 300;
        let (sink, events, _end) = Sink::new(&limits_solo_after(SOLO_AFTER));
        let mut events = match gate {
            Some(g) => events.leased(g).unwrap(),
            None => events,
        };
        let ev = |i: u16| {
            Out::event(proto::FeedEvent {
                token: Bytes::copy_from_slice(&i.to_le_bytes()),
                ..Default::default()
            })
        };
        sink.put_all((0..N).map(ev).collect()).await.unwrap();
        let mut got = Vec::new();
        let mut rounds = Vec::new();
        while got.len() < usize::from(N) {
            // One task poll: events until the stream says Pending.
            let n = std::future::poll_fn(|cx| {
                let mut n = 0;
                loop {
                    match Pin::new(&mut events).poll_out(cx) {
                        Poll::Ready(Some(Ok(e))) => {
                            got.push(u16::from_le_bytes([e.token()[0], e.token()[1]]));
                            n += 1;
                        }
                        Poll::Pending => return Poll::Ready(n),
                        Poll::Ready(other) => panic!("an event, not {:?}", other.map(|r| r.err())),
                    }
                }
            })
            .await;
            rounds.push(n);
            // Out of budget: the next poll of the task gets a new one.
            tokio::task::yield_now().await;
        }
        assert!(rounds.iter().all(|&n| n <= 128), "{rounds:?}");
        assert_eq!(rounds[0], 128, "{rounds:?}");
        assert_eq!(got, (0..N).collect::<Vec<_>>(), "every event, in order");
        assert!(!sink.pending(), "the batch's charge is back");
    }

    #[tokio::test]
    async fn a_long_batch_gives_its_worker_back_every_128_events() {
        a_long_batch_yields_every_budget(None).await;
    }

    #[tokio::test]
    async fn a_leased_long_batch_gives_its_worker_back_and_delivers_it_all() {
        use crate::serve::gate::{LeaseClock, WriteGate};
        let clock = Arc::new(LeaseClock::default());
        let gate = WriteGate::opened("p1", false, Arc::clone(&clock));
        gate.renew(clock.now() + 60_000_000_000);
        a_long_batch_yields_every_budget(Some(gate)).await;
    }

    /// simp724 on #1737's F3-0: a cluster component whose lease runs out while the connection
    /// holds the rest of a batch ends with the lease's error before the batch's next event.
    #[tokio::test]
    async fn a_lease_that_runs_out_mid_batch_ends_the_stream_before_its_next_event() {
        use crate::serve::gate::{LeaseClock, WriteGate};
        let clock = Arc::new(LeaseClock::default());
        let gate = WriteGate::opened("p1", false, Arc::clone(&clock));
        let (sink, events, _end) = Sink::new(&limits_solo_after(SOLO_AFTER));
        let mut events = events.leased(Arc::clone(&gate)).unwrap();
        gate.renew(clock.now() + 300_000_000);
        sink.put_all((0..3).map(ev).collect()).await.unwrap();
        assert_eq!(next_token(&mut events).await, 0);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let Some(Err(e)) = next_out(&mut events).await else {
            panic!("the lease's error, not the batch's next event");
        };
        assert_eq!(e.code, "PartitionUnavailable", "{}", e.message);
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
            coalesce: Coalesce::default(),
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
            internal: false,
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

    /// #1926: a new hub whose task handles its first item before [`Feeds::hub`] returns still
    /// delivers. The task used to be spawned while its hub was being made (`Arc::new_cyclic`):
    /// at a commit already waiting, it found no hub to upgrade and stopped, and the
    /// subscription got nothing ever again (1 run in 15-60 of the cluster pin test). The seam
    /// holds `hub` right after the spawn, commits, and waits for the task's first item, so
    /// that order happens every time.
    #[test]
    fn a_hub_whose_task_runs_before_the_hub_is_returned_still_delivers() {
        let (_, db, dir) = commits("hubstart", 0);
        let (requests, delivery, hubs) = server_runtimes(1);
        let t = db.table("t").unwrap().open().unwrap();
        let feeds = Feeds::new(
            db.clone(),
            limits_solo_after(Duration::from_secs(60)),
            &FeedShared::new(1),
        );
        feeds.run_on(delivery.handle().clone(), hubs.handle().clone());
        let seam_db = db.clone();
        *feeds.after_hub_spawn.lock().unwrap() = Some(Box::new(move |started: &Arc<Started>| {
            // A commit the hub's stream starts before, which its task reads while the hub is
            // still being opened.
            let t = seam_db.table("t").unwrap().open().unwrap();
            t.mutate(b"r0").put("f", b"q", b"v").commit().unwrap();
            assert!(
                started.wait(Duration::from_secs(10)),
                "the hub's task never handled its first item"
            );
        }));
        let slots = Arc::new(Semaphore::new(8));
        requests.block_on(async {
            let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
            let mut a = feeds.subscribe(now_spec(&t), slot, |_| {}).unwrap();
            *feeds.after_hub_spawn.lock().unwrap() = None;
            let s1 = t.mutate(b"r1").put("f", b"q", b"v").commit().unwrap().seqno;
            assert_eq!(
                seqnos(&mut a, Duration::from_millis(500)).await,
                [s1],
                "the hub delivers after its task handled a commit before it was returned"
            );
            drop(a);
        });
        drop(t);
        drop(requests);
        drop(delivery);
        drop(hubs);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// #1901 (A): a hub tells its clients once per batch it reads, not once per commit, and
    /// at least every [`NOTIFY_EVERY`] pushes, so a hub whose feed never runs dry still
    /// reaches them (perf2 on the design note). 200 commits are ready before its first poll.
    #[test]
    fn a_hub_tells_its_clients_once_per_batch_and_at_least_every_64_pushes() {
        let (_, db, dir) = commits("hubnotify", 0);
        let t = db.table("t").unwrap().open().unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let hub = Arc::new(Hub::new(
            Ring::new(SingleRing::default()),
            false,
            Arc::default(),
            Arc::new(AtomicU64::new(1)),
            Arc::default(),
        ));
        let snapshot = db.snapshot().unwrap();
        let stream = open(std::slice::from_ref(&t), true, &Start::Snapshot(snapshot)).unwrap();
        let put = |row: &str| {
            t.mutate(row.as_bytes())
                .put("f", b"q", b"v")
                .commit()
                .unwrap();
        };
        for i in 0..200 {
            put(&format!("r{i:03}"));
        }
        let d = Driver {
            stream: FeedStream::new(stream),
            hub: Arc::downgrade(&hub),
            ring_bytes: 16 << 20,
            runtimes: None,
            solo_after: Duration::ZERO,
            place: Place::Feed,
            alone: None,
            coalesce: Duration::ZERO,
            unnotified: 0,
            batch: 0,
            quiet_until: None,
            started: None,
        };
        let task = rt.spawn(drive(d));
        let head_at = |n: u64| {
            rt.block_on(async {
                for _ in 0..400 {
                    if hub.ring().head() >= n {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                panic!("the hub's head never reached {n}");
            });
        };
        head_at(200);
        rt.block_on(async { tokio::time::sleep(Duration::from_millis(50)).await });
        assert_eq!(
            hub.notifies.load(Ordering::Relaxed),
            4,
            "at 64, 128 and 192 pushes, then once the feed had nothing more"
        );
        put("r200");
        head_at(201);
        rt.block_on(async { tokio::time::sleep(Duration::from_millis(50)).await });
        assert_eq!(
            hub.notifies.load(Ordering::Relaxed),
            5,
            "one commit, one notify"
        );
        task.abort();
        drop((hub, t, rt));
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// #1901 (B): a caught-up hub waits `Coalesce::hub` after a small batch, never before one.
    /// After a quiet gap a lone commit is delivered at once; two commits made during the wait
    /// after it are read together when it ends, one batch and one notify. The wait is 2 s and
    /// the lone commit's bound half of it, so a hub that waited before it fails by a wide
    /// margin while a slow runner doesn't (#1970's Windows CI took 1.27 s with a disk sync
    /// inside the old 280 ms bound).
    #[test]
    fn a_caught_up_hub_waits_after_a_small_batch_but_never_before_one() {
        let (_, db, dir) = commits("hubcoalesce", 0);
        let (requests, delivery, hubs) = server_runtimes(1);
        let t = db.table("t").unwrap().open().unwrap();
        let mut limits = limits_solo_after(Duration::from_secs(60));
        const WAIT: Duration = Duration::from_secs(2);
        limits.coalesce.hub = WAIT;
        let feeds = Feeds::new(db.clone(), limits, &FeedShared::new(1));
        feeds.run_on(delivery.handle().clone(), hubs.handle().clone());
        let slots = Arc::new(Semaphore::new(8));
        requests.block_on(async {
            // The visible feed and Buffered commits: no disk sync inside the timed windows. On
            // the durable feed a commit's group sync counted toward the hub's wait, and a slow
            // Windows runner's took 1.27 s (#1970's CI).
            let put = |row: &[u8]| {
                t.mutate(row)
                    .put("f", b"q", b"v")
                    .durability(pigeonhole::Durability::Buffered)
                    .commit()
                    .unwrap()
                    .seqno
            };
            let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
            let spec = Spec {
                visible: true,
                ..now_spec(&t)
            };
            let mut a = feeds.subscribe(spec, slot, |_| {}).unwrap();
            let hub = feeds
                .hubs
                .lock()
                .unwrap()
                .values()
                .find_map(Weak::upgrade)
                .unwrap();
            // No batch yet, so no wait: the first commit is read at once.
            tokio::time::sleep(Duration::from_millis(400)).await;
            let s1 = put(b"r1");
            let t0 = Instant::now();
            assert_eq!(seqnos(&mut a, Duration::from_millis(100)).await, [s1]);
            let first = t0.elapsed();
            assert!(
                first < WAIT / 2,
                "a lone commit waited: {first:?} (its 100 ms quiet included)"
            );
            // The hub is in its wait after that batch of one: both are read after it. Apart by
            // a tenth of the wait, so a hub that didn't wait would read r2 alone.
            let notified = hub.notifies.load(Ordering::Relaxed);
            let s2 = put(b"r2");
            tokio::time::sleep(WAIT / 10).await;
            let s3 = put(b"r3");
            let mut got = Vec::new();
            while got.len() < 2 {
                let e = tokio::time::timeout(
                    WAIT * 2,
                    std::future::poll_fn(|cx| Pin::new(&mut a).poll_next(cx)),
                )
                .await
                .unwrap_or_else(|_| panic!("r2 and r3 not read within twice the wait: {got:?}"));
                if let Some(proto::feed_event::Event::Commit(c)) = e.unwrap().unwrap().event {
                    got.push(c.seqno);
                }
            }
            assert_eq!(got, [s2, s3]);
            assert_eq!(
                hub.notifies.load(Ordering::Relaxed) - notified,
                1,
                "two commits during the wait, one batch"
            );
            drop((a, hub));
        });
        drop(t);
        drop(requests);
        drop(delivery);
        drop(hubs);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_single_nodes_hub_placement_is_mains() {
        // #1798, g102: on a node with no cluster merges, `Driver::moves` decides as main's
        // did (4a47d2a47): the hub runtime at once with two or more clients, back to the feed
        // runtime once alone for `solo_after`. B's `internal()` must never read a client-only
        // hub as a merge's, at any member count, so a single node's placement is unchanged.
        // `main` is main's rule, written out; every step's answer is checked against it.
        fn main(place: Place, members: usize, alone_due: bool) -> Option<Place> {
            match place {
                Place::Feed => (members >= 2).then_some(Place::Hubs),
                Place::Hubs if members >= 2 => None,
                Place::Hubs => alone_due.then_some(Place::Feed),
            }
        }
        let (_, db, dir) = commits("feedpin", 0);
        let t = db.table("t").unwrap().open().unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let hub = Arc::new(Hub::new(
            Ring::new(SingleRing::default()),
            true,
            Arc::default(),
            Arc::new(AtomicU64::new(1)),
            Arc::default(),
        ));
        let snapshot = db.snapshot().unwrap();
        let stream = open(std::slice::from_ref(&t), true, &Start::Snapshot(snapshot)).unwrap();
        let mut d = Driver {
            stream: FeedStream::new(stream),
            hub: Arc::downgrade(&hub),
            ring_bytes: 16 << 20,
            runtimes: Some((rt.handle().clone(), rt.handle().clone())),
            solo_after: Duration::ZERO,
            place: Place::Feed,
            alone: None,
            coalesce: Duration::ZERO,
            unnotified: 0,
            batch: 0,
            quiet_until: None,
            started: None,
        };
        // One step, checked against main's rule; `alone_due`: main's answer alone on the hub
        // runtime (the step after the one that started the `solo_after` clock).
        let step = |d: &mut Driver, alone_due: bool| {
            let members = hub.members();
            assert!(!hub.internal(), "a client-only hub with {members} members");
            let got = d.moves(&hub);
            let want = main(d.place, members, alone_due);
            assert_eq!(got, want, "{members} members at {:?}", d.place);
            if let Some(p) = got {
                d.place = p;
            }
            got
        };
        let join = || hub.join(&Arc::new(Tracker::default()), false);
        let mut joined = Vec::new();
        assert_eq!(step(&mut d, false), None, "no members yet");
        joined.push(join());
        assert_eq!(step(&mut d, false), None, "one client: solo, beside it");
        joined.push(join());
        assert_eq!(step(&mut d, false), Some(Place::Hubs), "a second client");
        joined.push(join());
        assert_eq!(step(&mut d, false), None, "three: stays");
        joined.truncate(2);
        assert_eq!(step(&mut d, false), None, "two: stays");
        joined.truncate(1);
        assert_eq!(step(&mut d, false), None, "alone: the clock starts");
        assert_eq!(
            step(&mut d, true),
            Some(Place::Feed),
            "alone past solo_after"
        );
        joined.clear();
        assert_eq!(step(&mut d, false), None, "none left, on the feed runtime");
        drop((d, t));
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_hub_read_only_by_cluster_merges_runs_on_the_hub_runtime_with_a_full_share() {
        // #1798, g100: a partition hub whose one member is a cluster hub's merge isn't solo.
        // Its reader runs on the feed runtime, which also delivers every cluster subscriber's
        // events, so the hub's engine reads go to the hub runtime at its first commit and
        // stay there past `solo_after`. Its ring takes an equal share of `--feed-ring-bytes`
        // with every other ring that has a member, as a client's hub does.
        let (_, db, dir) = commits("feedint", 0);
        let (requests, delivery, hubs) = server_runtimes(1);
        let t = db.table("t").unwrap().open().unwrap();
        let u = db
            .table("u")
            .unwrap()
            .family("f", Family::default())
            .create()
            .unwrap();
        let shared = FeedShared::new(1);
        let feeds = Feeds::new(
            db.clone(),
            limits_solo_after(Duration::from_millis(50)),
            &shared,
        );
        feeds.run_on(delivery.handle().clone(), hubs.handle().clone());
        let slots = Arc::new(Semaphore::new(8));
        let hubs_before = hubs.metrics().num_alive_tasks();
        requests.block_on(async {
            let put =
                |t: &Table, row: &[u8]| t.mutate(row).put("f", b"q", b"v").commit().unwrap().seqno;
            let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
            let merge = Spec {
                internal: true,
                ..now_spec(&t)
            };
            let mut m = feeds.subscribe(merge, slot, |_| {}).unwrap();
            let s1 = put(&t, b"r1");
            assert_eq!(seqnos(&mut m, Duration::from_millis(300)).await, [s1]);
            assert_eq!(
                tasks_on(&hubs, hubs_before + 1).await,
                hubs_before + 1,
                "the merge's hub, on the hub runtime"
            );
            assert_eq!(feeds.gauges().solo_hubs, 0);
            // Past `solo_after`, with another commit: still there.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let s2 = put(&t, b"r2");
            assert_eq!(seqnos(&mut m, Duration::from_millis(300)).await, [s2]);
            let s3 = put(&t, b"r3");
            assert_eq!(seqnos(&mut m, Duration::from_millis(300)).await, [s3]);
            assert_eq!(tasks_on(&hubs, hubs_before + 1).await, hubs_before + 1);
            assert_eq!(feeds.gauges().solo_hubs, 0);
            assert_eq!(feeds.stats.hub_moves.load(Ordering::Relaxed), 1);

            // A client's hub beside it: two rings with members, so each has half.
            let slot = Arc::clone(&slots).try_acquire_owned().unwrap();
            let mut c = feeds.subscribe(now_spec(&u), slot, |_| {}).unwrap();
            assert_eq!(shared.sharers.load(Ordering::Relaxed), 2);
            let total = 16 << 20;
            for h in feeds
                .hubs
                .lock()
                .unwrap()
                .values()
                .filter_map(Weak::upgrade)
            {
                assert_eq!(h.ring_limit(total), total / 2);
            }
            let s4 = put(&u, b"r4");
            assert_eq!(seqnos(&mut c, Duration::from_millis(300)).await, [s4]);
            assert_eq!(feeds.gauges().solo_hubs, 1, "the client's hub is solo");
            drop(c);
            drop(m);
            // Every membership gone, once the clients' tasks end: no ring splits the budget.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            while shared.sharers.load(Ordering::Relaxed) != 0
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert_eq!(shared.sharers.load(Ordering::Relaxed), 0);
        });
        drop((t, u));
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
                coalesce: Coalesce::default(),
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
                internal: false,
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
            // All but the last Buffered: 3000 group-syncs took about a minute on Windows CI
            // (#1961). The subscription reads the durable feed, so the last commit's group
            // sync makes the whole backlog durable before the hub sees any of it.
            let mut last = 0;
            for i in 0..BACKLOG {
                let durability = if i + 1 == BACKLOG {
                    pigeonhole::Durability::GroupSync
                } else {
                    pigeonhole::Durability::Buffered
                };
                last = t
                    .mutate(format!("r{i}").as_bytes())
                    .put("f", b"q", b"v")
                    .durability(durability)
                    .commit()
                    .unwrap()
                    .seqno;
            }
            assert!(
                db.durable_snapshot().unwrap().seqno() >= last,
                "the backlog is durable before the hub is released"
            );
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
                coalesce: Coalesce::default(),
            },
            &FeedShared::new(1),
        );
        let slots = Arc::new(Semaphore::new(1));
        let mut direct = t.feed().open().unwrap();
        let mut want = Vec::new();
        rt.block_on(async {
            let hub = feeds.hub(std::slice::from_ref(&t), false).unwrap();
            hub.task().as_ref().expect("spawned").abort();
            let mut c = feeds
                .subscribe(
                    Spec {
                        tables: vec![t.clone()],
                        prefix: Vec::new(),
                        visible: false,
                        start: Start::Now,
                        skip: 0,
                        announce: false,
                        internal: false,
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
                coalesce: Coalesce::default(),
            },
            &FeedShared::new(1),
        );
        let slots = Arc::new(Semaphore::new(1));
        let mut direct = t.feed().open().unwrap();
        let ended: Arc<Mutex<Option<(tokio::time::Instant, String)>>> = Arc::default();
        rt.block_on(async {
            let hub = feeds.hub(std::slice::from_ref(&t), false).unwrap();
            hub.task().as_ref().expect("spawned").abort();
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
                        internal: false,
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
                coalesce: Coalesce::default(),
            },
            &FeedShared::new(1),
        );
        let slots = Arc::new(Semaphore::new(1));
        rt.block_on(async {
            let hub = feeds.hub(std::slice::from_ref(&t), false).unwrap();
            // The hub's own task stops; the test pushes for it.
            hub.task().as_ref().expect("spawned").abort();
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
                        internal: false,
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
                xid: Default::default(),
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
                let want = filtered(c, prefix).map(|f| commit_event(token.clone(), f, b""));
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
