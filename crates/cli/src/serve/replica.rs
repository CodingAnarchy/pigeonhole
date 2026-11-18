//! Replication between servers (#1436, ICR 0112's amendment).
//!
//! **The primary's side** is `Replication.Follow`: the primary's durable feed of every table
//! from a replica's token, as batches of encoded events (`pigeonhole`'s hidden
//! `ReplicatedEvent` encoding, opaque here). Each follower reads on a thread of its own, under
//! a `--max-streams` slot, so a replica catching up never holds a runtime worker.
//!
//! **The replica's side** is `phdb serve --replica-of`: a task that bootstraps the database
//! from the primary's `Admin.BackupDownload` (with `replica`), then follows it. Events are
//! applied in runs: a run is applied when a catalog change arrives (it applies on its own,
//! between runs), when it reaches `--replica-run` events or [`RUN_BYTES`], or when no further
//! message is ready, so a caught-up replica applies each message at once and a loaded one
//! amortizes one `GroupSync` over a run. Each apply stores the primary's token with what it
//! applied, so a reconnect, a restart or an apply error resumes exactly where the last apply
//! ended. A token the primary no longer keeps, or another database at the URL, bootstraps
//! again into a side file while the old copy keeps serving, then swaps it in.

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_core::Stream;
use pigeonhole::replication::ReplicatedEvent;
use pigeonhole::{FeedToken, Pigeonhole};
use sha2::Digest;
use tokio::sync::{OwnedSemaphorePermit, mpsc, watch};

use super::Shared;
use super::errors::ApiError;
use super::proto;
use super::service::{FOLLOW, Metrics, Service};

/// A `Follow` message's events, at most.
const FOLLOW_EVENTS: usize = 256;

/// A `Follow` message's bytes, about: a message is cut once its events pass this.
const FOLLOW_BYTES: usize = 1 << 20;

/// How long a quiet `Follow` waits before it sends a message with no events, so the replica
/// sees the primary's frontier move and knows it is caught up.
const QUIET: Duration = Duration::from_secs(1);

/// A replica's run, in bytes at most (with `--replica-run` events at most).
pub(crate) const RUN_BYTES: usize = 8 << 20;

/// The default `--replica-run`.
pub(crate) const RUN_EVENTS: usize = 256;

/// The request header a client sends with a read on a replica to see its own writes (#1577):
/// the seqno of its commit on the primary (`CommitInfo.seqno`, decimal). The replica waits,
/// up to the request's deadline, until it has applied the primary's commits through it. A
/// server that isn't a replica ignores it.
pub(crate) const MIN_PRIMARY_SEQNO: &str = "x-pigeonhole-min-primary-seqno";

/// When a read's wait for its primary seqno ends: one instant for the whole request, from
/// its arrival, so the budget wait and this one share a bound (cli692's R1 on #1594).
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReadDeadline {
    pub(crate) at: Instant,
    /// The client's own deadline set it (gRPC's `grpc-timeout`, shorter than the server's
    /// request timeout): its end answers `DeadlineExceeded`, as the budget wait does.
    pub(crate) client: bool,
}

impl ReadDeadline {
    /// From a request that arrived at `arrived`, with the server's `limit` and the client's
    /// own deadline, if it set one.
    pub(crate) fn new(arrived: Instant, limit: Duration, client: Option<Duration>) -> Self {
        match client {
            Some(d) if d <= limit => Self {
                at: arrived + d,
                client: true,
            },
            _ => Self {
                at: arrived + limit,
                client: false,
            },
        }
    }
}

/// [`MIN_PRIMARY_SEQNO`]'s value: none without the header, `InvalidArgument` unless it's a
/// decimal `u64`.
pub(crate) fn min_primary_seqno(value: Option<&[u8]>) -> Result<Option<u64>, ApiError> {
    let Some(value) = value else {
        return Ok(None);
    };
    std::str::from_utf8(value)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Some)
        .ok_or_else(|| {
            ApiError::invalid(format!(
                "{MIN_PRIMARY_SEQNO} is a primary commit's seqno, a decimal number"
            ))
        })
}

/// The request header a client sends with a read on a cluster node to let a replica of the
/// partition serve it (D328): the most staleness it accepts, in milliseconds (decimal). A
/// replica serves it only if it heard from the owner, and had applied everything the owner
/// had then made durable, at most that long ago on its own clock; otherwise the next
/// replica or the owner does. Without it, every read is the owner's.
pub(crate) const READ_REPLICA_MAX_LAG_MS: &str = "x-pigeonhole-read-replica-max-lag-ms";

/// The response header naming what served a read sent with [`READ_REPLICA_MAX_LAG_MS`]:
/// `owner` or `replica` (D328's Q7).
pub(crate) const SERVED_BY: &str = "x-pigeonhole-served-by";

/// The response header with the seqno such a read was served at, in the owner's numbering:
/// a replica's applied seqno after the read, or the owner's visible seqno after it. Sent
/// back as [`MIN_PRIMARY_SEQNO`], it makes the next read at least as new (monotonic reads,
/// D328's Q7).
pub(crate) const SERVED_SEQNO: &str = "x-pigeonhole-served-seqno";

/// [`READ_REPLICA_MAX_LAG_MS`]'s value: none without the header, `InvalidArgument` unless
/// it's a decimal `u64`.
pub(crate) fn read_replica_max_lag(value: Option<&[u8]>) -> Result<Option<u64>, ApiError> {
    let Some(value) = value else {
        return Ok(None);
    };
    std::str::from_utf8(value)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Some)
        .ok_or_else(|| {
            ApiError::invalid(format!(
                "{READ_REPLICA_MAX_LAG_MS} is the staleness a read accepts, in milliseconds"
            ))
        })
}

/// What served a read that a replica could have (D328): the response's [`SERVED_BY`] and
/// [`SERVED_SEQNO`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Served {
    pub(crate) replica: bool,
    pub(crate) seqno: u64,
}

impl Served {
    /// The two response headers' values.
    pub(crate) fn headers(self) -> [(&'static str, String); 2] {
        [
            (
                SERVED_BY,
                if self.replica { "replica" } else { "owner" }.to_owned(),
            ),
            (SERVED_SEQNO, self.seqno.to_string()),
        ]
    }
}

/// The longest pause between a replica's attempts after an error.
const MAX_BACKOFF: Duration = Duration::from_secs(10);

/// How long a follow must run before the pause after the next error starts short again.
const STEADY: Duration = Duration::from_secs(30);

/// The least a bootstrap waits for the primary's first answer, and between its heartbeats
/// while it writes its copy (#1611): three of the heartbeats it sends every second, however
/// short the stream idle limit (which bounds the waits for the copy's data).
const BEAT_WAIT: Duration = Duration::from_secs(3);

/// Whether an apply's refusal says the replica's tables aren't its primary's (#1853): a
/// commit to a table or family it doesn't have, or a catalog change to one it lacks or
/// already has under another id. Retrying from the stored token refuses again, for ever:
/// only a new bootstrap brings the two back together. The primary's feed delivers every
/// catalog change, so this follows a change its history never recorded: a 1.1.x session's,
/// after a rollback of the primary.
fn diverged(e: &pigeonhole::Error) -> bool {
    matches!(
        e.code(),
        pigeonhole::ErrorCode::TableNotFound
            | pigeonhole::ErrorCode::FamilyNotFound
            | pigeonhole::ErrorCode::TableExists
            | pigeonhole::ErrorCode::FamilyExists
    )
}

/// An apply's refusal as the loop's halt: [`diverged`] bootstraps again, anything else
/// retries.
fn applying(what: &str, e: &pigeonhole::Error) -> Halt {
    if diverged(e) {
        Halt::Expired(format!(
            "{what}: {e}; the replica's tables are no longer its primary's (a schema change \
             the primary's feed didn't record, such as one made while it ran 1.1.x), so it \
             bootstraps again"
        ))
    } else {
        Halt::Failed(format!("{what}: {e}"))
    }
}

// ---- The primary's side: `Replication.Follow` ----

/// A `Follow` stream: batches as the follower's thread reads them, then its error if it
/// failed.
pub(crate) struct FollowBatches {
    rx: mpsc::Receiver<Result<proto::ReplicationBatch, ApiError>>,
    /// A cluster partition's: delivered only while it holds its lease (D328's M1, D326), so
    /// a deposed owner, cut off from the cluster but not from its replica, stops feeding it.
    lease: Option<super::lease::DeliveryLease>,
}

impl std::fmt::Debug for FollowBatches {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FollowBatches")
            .field("leased", &self.lease.is_some())
            .finish_non_exhaustive()
    }
}

impl FollowBatches {
    /// These batches, delivered only while cluster partition `gate`'s lease holds (D328's
    /// M1): they wait while it has had none since it opened, and end with
    /// `PartitionUnavailable` once it lapses or the node can't reach a majority (refused at
    /// once then), as a feed component's events do. The replica takes that as a break: it
    /// reads the map again, and follows P's owner then.
    pub(crate) fn leased(mut self, gate: Arc<super::gate::WriteGate>) -> Result<Self, ApiError> {
        self.lease = Some(super::lease::DeliveryLease::new(gate)?);
        Ok(self)
    }
}

/// The follower's thread ends the stream itself (its error is its last item), so the lease
/// check has no separate end to wait for, and no take clock: the thread's own idle limit
/// counts from the replica's last take of a message.
struct NoHooks;

impl super::lease::LeaseHooks for NoHooks {
    fn poll_own_end(&mut self, _: &mut Context<'_>) -> Poll<Option<ApiError>> {
        Poll::Pending
    }

    fn took(&self) {}
}

impl Stream for FollowBatches {
    type Item = Result<proto::ReplicationBatch, ApiError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(lease) = self.lease.as_mut()
            && let Some(answer) = lease.before(cx, &mut NoHooks)
        {
            return answer;
        }
        let next = self.rx.poll_recv(cx);
        if let Some(lease) = self.lease.as_mut() {
            lease.after(next.is_pending(), cx);
        }
        next
    }
}

/// Opens the primary's durable feed of every table from `token` and starts its follower's
/// thread. The thread holds `slot` until the stream ends: the replica went away, took no
/// message for `idle`, or the feed failed (its error is the stream's last item).
///
/// Nothing joins the thread. At shutdown it may still hold its feed and a database handle
/// when the server closes the database: that's safe, since the feed then fails (`Closed`)
/// and the thread ends, its send failing or its error unsent (cli692's F1 on #1583).
pub(crate) fn follow(
    db: &Pigeonhole,
    token: Vec<u8>,
    slot: OwnedSemaphorePermit,
    idle: Duration,
    metrics: Arc<Metrics>,
    placement: Option<Arc<super::placement::Placement>>,
) -> Result<FollowBatches, ApiError> {
    if token.is_empty() {
        return Err(ApiError::invalid(
            "Follow needs the token of a replica's bootstrap or of its last apply",
        ));
    }
    let mut feed = db
        .feed()
        .resume(&FeedToken::from_bytes(token))
        .open()
        .map_err(|e| ApiError::from_engine(&e).for_read())?;
    // A replica follows every commit, a range move's feed-silent copy and cleanup too (D331,
    // ICR 0141): always, since only a replica's follower calls Follow. Each crosses as a
    // version 2 event, which a replica built before D331 refuses (its halt is the backstop
    // behind the writer's `range_moves` gate, never something an operator should meet).
    feed.include_silent();
    let (tx, rx) = mpsc::channel(2);
    let db = db.clone();
    std::thread::Builder::new()
        .name("phdb-serve-follow".into())
        .spawn(move || {
            // Off the shards' cores, as the runtime's threads: a replica catching up reads
            // and encodes at full speed (cli692's F1 on #1583).
            if let Some(p) = &placement {
                p.enter();
            }
            let started = Instant::now();
            let outcome = pump(&db, feed, &tx, idle);
            metrics.count(
                FOLLOW,
                outcome.as_ref().err().map(|e| e.code.as_str()),
                started.elapsed(),
            );
            if let Err(e) = outcome {
                let _ = tx.try_send(Err(e));
            }
            drop(slot);
        })
        .map_err(|e| ApiError::internal(format!("starting a follower's thread: {e}")))?;
    Ok(FollowBatches { rx, lease: None })
}

/// The follower's loop: a message per batch of events, or per [`QUIET`] with none.
fn pump(
    db: &Pigeonhole,
    mut feed: pigeonhole::Feed,
    tx: &mpsc::Sender<Result<proto::ReplicationBatch, ApiError>>,
    idle: Duration,
) -> Result<(), ApiError> {
    let engine = |e: pigeonhole::Error| ApiError::from_engine(&e).for_read();
    loop {
        let mut events = Vec::new();
        let mut bytes = 0;
        let mut caught_up = false;
        if feed.next_event(QUIET).map_err(engine)?.is_some() {
            loop {
                let event = feed
                    .replicated()
                    .ok_or_else(|| ApiError::internal("a delivered event has no replicated form"))?
                    .encode();
                bytes += event.len();
                events.push(event.into());
                if events.len() >= FOLLOW_EVENTS || bytes >= FOLLOW_BYTES {
                    break;
                }
                if feed.try_next_event().map_err(engine)?.is_none() {
                    caught_up = true;
                    break;
                }
            }
        } else {
            caught_up = true;
        }
        let batch = proto::ReplicationBatch {
            events,
            // The durable frontier, which this feed can deliver up to, not the visible
            // seqno, which runs ahead of it under `Buffered` (cli692's F5 on #1583).
            primary_seqno: db.replication_frontier(),
            caught_up,
            // The catalog at the feed's position, after these events (#1853 part C2).
            catalog_digest: feed.catalog_digest(),
        };
        if !send_within(tx, batch, idle)? {
            // The replica went away.
            return Ok(());
        }
    }
}

/// How long a follower waits between tries while its replica's buffer is full.
const SEND_RETRY: Duration = Duration::from_millis(5);

/// Sends `batch`, waiting at most `idle` for room: `false` if the replica went away,
/// `DeadlineExceeded` if it took nothing for `idle`. On this thread alone, with no runtime
/// call, so it holds while the runtime shuts down (cli692 on #1583: a tokio timer from
/// `Handle::block_on` panics then). It polls only while the replica is slower than the feed.
fn send_within(
    tx: &mpsc::Sender<Result<proto::ReplicationBatch, ApiError>>,
    batch: proto::ReplicationBatch,
    idle: Duration,
) -> Result<bool, ApiError> {
    let deadline = Instant::now() + idle;
    let mut item = Ok(batch);
    loop {
        match tx.try_send(item) {
            Ok(()) => return Ok(true),
            Err(mpsc::error::TrySendError::Closed(_)) => return Ok(false),
            Err(mpsc::error::TrySendError::Full(back)) => {
                if Instant::now() >= deadline {
                    return Err(ApiError::deadline(
                        "the replica took no message for --stream-idle-ms; it resumes from its \
                         token",
                    ));
                }
                item = back;
                std::thread::sleep(SEND_RETRY);
            }
        }
    }
}

// ---- The replica's side: `phdb serve --replica-of` ----

/// `--replica-of` and its settings.
#[derive(Clone)]
pub(crate) struct ReplicaConfig {
    /// The primary's URL, `http://` or `https://`.
    pub primary: String,
    /// The bearer token the replica sends the primary (feed and admin scope there).
    pub token: Option<String>,
    /// A run's events, at most (`--replica-run`).
    pub run_events: usize,
}

/// Without the token: only whether one is set, so no log or panic message can carry it.
impl std::fmt::Debug for ReplicaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaConfig")
            .field("primary", &self.primary)
            .field("token", &self.token.as_ref().map(|_| "<set>"))
            .field("run_events", &self.run_events)
            .finish()
    }
}

/// Where a replica follows from.
#[derive(Debug)]
enum Upstream {
    /// A single node's `--replica-of` URL, with its token ([`ReplicaConfig`]).
    Url,
    /// A cluster partition's owner, as the node's map names it (D328).
    Owner {
        node: std::sync::Weak<super::cluster::Node>,
        partition: String,
    },
}

/// Where a replica's loop is, as `ReplicaInfo.state` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Starting,
    Bootstrapping,
    Following,
    ReBootstrapping,
    /// After an error, which `last_error` gives.
    Retrying,
    /// A promote failed after it stopped the loop.
    Stopped,
    /// A primary now: `Admin.Info` reports no replica.
    Promoted,
}

impl State {
    fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Bootstrapping => "bootstrapping",
            Self::Following => "following",
            Self::ReBootstrapping => "re-bootstrapping",
            Self::Retrying => "retrying",
            Self::Stopped => "stopped",
            Self::Promoted => "promoted",
        }
    }

    /// Whether the replica's copy is still arriving or being swapped in.
    fn copying(self) -> bool {
        matches!(self, Self::Bootstrapping | Self::ReBootstrapping)
    }
}

/// What `Admin.Info` and `/metrics` report.
#[derive(Debug)]
struct Status {
    state: State,
    applied: u64,
    primary_seqno: u64,
    caught_up: bool,
    /// Since when the replica hasn't been caught up (when not).
    behind_since: Instant,
    last_error: String,
    /// While a bootstrap runs, its step and the copy's bytes received (#1595: a bootstrap
    /// that stops says where).
    bootstrap_phase: &'static str,
    bootstrap_bytes: u64,
    /// A cluster replica copy's freshness (D328), on its own clock: the arrival of the
    /// newest message whose `primary_seqno` (the owner's durable frontier at its send) the
    /// copy has applied through. So the copy then held every commit the owner had made
    /// durable when that message left, whether or not it said `caught_up`, and under
    /// sustained writes it keeps moving (simp724's note on #1974, D330's M2). The owner
    /// sends a message at least once a second while idle, so it ages from the moment the
    /// stream stops.
    fresh_at: Option<Instant>,
    /// Messages not yet applied through: each one's `primary_seqno` and arrival, oldest
    /// first, at most [`PENDING_FRESH`] (an older one dropped only makes `fresh_at` later).
    pending_fresh: std::collections::VecDeque<(u64, Instant)>,
}

/// How many messages' frontiers [`Status::pending_fresh`] keeps.
const PENDING_FRESH: usize = 1024;

impl Status {
    /// A message with the owner's durable frontier `primary_seqno` arrived at `at`.
    fn heard(&mut self, primary_seqno: u64, at: Instant) {
        match self.pending_fresh.back_mut() {
            // The same frontier again (an idle owner's heartbeat): once applied through, the
            // copy is fresh as of this later message (simp724's M1 on #1989).
            Some((seqno, arrived)) if *seqno == primary_seqno => *arrived = at,
            Some(&mut (seqno, _)) if primary_seqno < seqno => {}
            _ => {
                if self.pending_fresh.len() == PENDING_FRESH {
                    self.pending_fresh.pop_front();
                }
                self.pending_fresh.push_back((primary_seqno, at));
            }
        }
        self.settle_fresh();
    }

    /// Moves `fresh_at` to the newest message the copy has applied through.
    fn settle_fresh(&mut self) {
        while let Some(&(seqno, at)) = self.pending_fresh.front() {
            if self.applied < seqno {
                break;
            }
            self.fresh_at = Some(self.fresh_at.map_or(at, |f| f.max(at)));
            self.pending_fresh.pop_front();
        }
    }
}

/// A cluster node's replica copy's follower, as `Cluster.Status` reports it (D328).
#[derive(Debug, Clone)]
pub(crate) struct CopyStatus {
    pub(crate) state: proto::ReplicaState,
    /// The owner it follows, as last dialled ("" before the first).
    pub(crate) owner: String,
    pub(crate) applied: u64,
    pub(crate) primary_seqno: u64,
    /// Since `fresh_at`; since the follower started before its first.
    pub(crate) lag_ms: u64,
    pub(crate) last_error: String,
}

/// What a replica copy's last error says when its owner's feed is off (S1).
pub(crate) const OWNER_FEED_OFF: &str = "change feed is off";

/// A replica server's loop and its state.
#[derive(Debug)]
pub(crate) struct Replica {
    config: ReplicaConfig,
    /// Where it follows from.
    upstream: Upstream,
    /// A replica copy's owner as last dialled, its id and address (empty before the first).
    now: Mutex<(String, String)>,
    /// When it was made: a copy's lag counts from here until it's first fresh.
    started: Instant,
    status: Mutex<Status>,
    /// Set once `Admin.Promote` made the database a primary: writes are taken, and the
    /// replica reports nothing.
    promoted: AtomicBool,
    /// Set while an `Admin.Promote` runs: a second one is refused rather than racing it to
    /// `promote` (cli692's F4 on #1583).
    promoting: AtomicBool,
    stop: watch::Sender<bool>,
    /// Cuts the loop's pause after a failed follow short (D330 amendment 2): a map that
    /// changes the partition's owner, epoch or history wakes a cluster copy, so it finds the
    /// new owner at once rather than after up to [`MAX_BACKOFF`]. A wake while it follows
    /// skips the next pause.
    wake: tokio::sync::Notify,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// The primary's seqno applied through, as `Status::applied`, for reads waiting on it
    /// (#1577). It goes back to 0 only when a bootstrap swaps another copy in.
    applied: watch::Sender<u64>,
    /// Set when a bootstrap swaps a copy in, cleared by the first catalog digest that
    /// matches the primary's (#1853 part C2). A mismatch while set can't be a schema change
    /// the primary's feed missed (no 1.1.x session runs under a serving primary between a
    /// copy and its batches), so another copy wouldn't fix it: the loop retries instead of
    /// downloading the database again (cli692's S1 on #1878).
    unconfirmed: AtomicBool,
    /// A cluster replica copy's range gate (D331 PR 3): the ranges a move gates on the
    /// partition it copies, from the node's map and the rows its file holds. Never set on a
    /// single node's replica.
    ranges: super::ranges::RangeGate,
}

/// A copy's range rows and its file's visible seqno ([`Replica::arm_ranges`]).
type RangeRows = (Vec<super::ranges::Moved>, Vec<super::ranges::Moved>, u64);

/// The range rows in `db` (D331 PR 3). **Blocking.**
fn range_rows(db: &Pigeonhole) -> Result<RangeRows, String> {
    let rows = (
        super::ranges::read_moved(db)?,
        super::ranges::read_serving(db)?,
        db.visible_seqno(),
    );
    #[cfg(test)]
    ARMED.lock().unwrap_or_else(PoisonError::into_inner).push((
        db.path().to_path_buf(),
        rows.0.len(),
        rows.1.len(),
    ));
    Ok(rows)
}

/// The fault seam: in the tests, a promote of the database at this path fails after the
/// loop stopped, as a failed `Pigeonhole::promote` would.
#[cfg(test)]
pub(crate) static FAIL_PROMOTE: Mutex<Option<PathBuf>> = Mutex::new(None);

/// The tests' seam (simp724's S1 on #2001): a replica copy whose database is under one of
/// these directories applies nothing while it's listed, though it still hears its owner,
/// as a copy that fell behind would: still fresh as of its last apply, until the bound.
#[cfg(test)]
pub(crate) static HOLD_APPLIES: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// The tests' inspection seam: the served database's path for each bootstrap that swapped
/// its copy in (a failed attempt isn't one), so a test can tell a follower that resumed from
/// one that bootstrapped again (D328).
#[cfg(test)]
pub(crate) static BOOTSTRAPS: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// Each replica copy's range rows as its gate last read them (D331 PR 3): its file's path
/// and how many moved-away and serving rows it held, so a test can tell the rows reached a
/// copy through its follower.
#[cfg(test)]
pub(crate) static ARMED: Mutex<Vec<(PathBuf, usize, usize)>> = Mutex::new(Vec::new());

/// `Admin.Promote`'s answer before the first bootstrap has finished.
fn not_bootstrapped() -> ApiError {
    ApiError::failed_precondition(
        "the replica hasn't finished its bootstrap: there is nothing to promote",
    )
}

/// What a replica's loop serves through: the service whose database it follows into (a
/// single node's, or a cluster node's replica copy, D328), and the idle limit its streams
/// take (`--stream-idle-ms`).
struct On<'a> {
    service: &'a Service,
    idle: Duration,
}

/// Why following stopped.
enum Halt {
    /// `Admin.Promote` or shutdown.
    Stopped,
    /// The primary no longer keeps the replica's position, or is another database: bootstrap
    /// again.
    Expired(String),
    /// Anything else: retry from the stored token after a pause.
    Failed(String),
}

impl Replica {
    pub(crate) fn new(config: ReplicaConfig) -> Arc<Self> {
        Self::with_upstream(config, Upstream::Url)
    }

    /// A cluster node's replica copy of `partition` (D328): it follows the partition's owner
    /// as `node`'s map names it, with the node's cluster token.
    pub(crate) fn of_partition(
        node: &Arc<super::cluster::Node>,
        partition: &str,
        run_events: usize,
    ) -> Arc<Self> {
        let config = ReplicaConfig {
            primary: String::new(),
            token: None,
            run_events,
        };
        Self::with_upstream(
            config,
            Upstream::Owner {
                node: Arc::downgrade(node),
                partition: partition.to_owned(),
            },
        )
    }

    fn with_upstream(config: ReplicaConfig, upstream: Upstream) -> Arc<Self> {
        Arc::new(Self {
            config,
            upstream,
            now: Mutex::new((String::new(), String::new())),
            started: Instant::now(),
            status: Mutex::new(Status {
                state: State::Starting,
                applied: 0,
                primary_seqno: 0,
                caught_up: false,
                behind_since: Instant::now(),
                last_error: String::new(),
                bootstrap_phase: "",
                bootstrap_bytes: 0,
                fresh_at: None,
                pending_fresh: std::collections::VecDeque::new(),
            }),
            promoted: AtomicBool::new(false),
            promoting: AtomicBool::new(false),
            unconfirmed: AtomicBool::new(false),
            ranges: super::ranges::RangeGate::default(),
            stop: watch::channel(false).0,
            wake: tokio::sync::Notify::new(),
            task: Mutex::new(None),
            applied: watch::channel(0).0,
        })
    }

    /// The primary's URL.
    pub(crate) fn primary(&self) -> String {
        match &self.upstream {
            Upstream::Url => self.config.primary.clone(),
            Upstream::Owner { partition, .. } => {
                let now = self.now.lock().unwrap_or_else(PoisonError::into_inner);
                if now.0.is_empty() {
                    format!("partition {partition}'s owner")
                } else {
                    format!("node {} at {}", now.0, now.1)
                }
            }
        }
    }

    /// The replica's state while its copy is still arriving or being swapped
    /// (`starting`, `bootstrapping`, `re-bootstrapping`), else `None`: a memory store it
    /// lacks then is `ReplicaBehind`, not `Unavailable` (#1618).
    pub(crate) fn settling(&self) -> Option<&'static str> {
        let state = self.status().state;
        (state == State::Starting || state.copying()).then_some(state.as_str())
    }

    /// Records `error` as the replica's last error (`Admin.Info`), its state as it is: for a
    /// failure after a promote that doesn't fail it (#1618's memory store).
    pub(crate) fn note_error(&self, error: String) {
        self.status().last_error = error;
    }

    /// Whether the server refuses writes: a replica until it is promoted.
    pub(crate) fn refuses_writes(&self) -> bool {
        !self.promoted.load(Ordering::Acquire)
    }

    fn status(&self) -> std::sync::MutexGuard<'_, Status> {
        self.status.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn set_state(&self, state: State) {
        self.status().state = state;
    }

    /// A bootstrap's step, and the copy's bytes received so far ("" and 0 once it ends).
    fn set_bootstrap(&self, phase: &'static str, bytes: u64) {
        let mut s = self.status();
        s.bootstrap_phase = phase;
        s.bootstrap_bytes = bytes;
    }

    /// A bootstrap's step, its bytes as they are.
    fn set_bootstrap_phase(&self, phase: &'static str) {
        self.status().bootstrap_phase = phase;
    }

    fn fail(&self, state: State, error: String) {
        let mut s = self.status();
        s.state = state;
        s.last_error = error;
    }

    /// The primary seqno the served engine has applied through: a run's last event's, or a
    /// catalog change's. A read's `x-pigeonhole-min-primary-seqno` is a `CommitInfo.seqno`,
    /// and every one of those names a commit the primary's feed of every table delivers,
    /// a `None` commit's and a cross-shard transaction's included (cli692's Q1 on #1594;
    /// the server tests check both), so the value reaches it without a later commit.
    fn set_applied(&self, applied: u64) {
        let mut s = self.status();
        s.applied = applied;
        s.settle_fresh();
        drop(s);
        self.applied.send_replace(applied);
    }

    /// Waits until the replica has applied the primary's commits through `seqno`, at most
    /// until `deadline` (#1577: read-your-writes on a replica). At the deadline,
    /// `ReplicaBehind` (retry-safe), or `DeadlineExceeded` when the client's own deadline
    /// was the bound, as the budget wait answers (cli692's R1 on #1594); `ReplicaBehind` at
    /// once while a bootstrap runs: a copy being swapped in has nothing to wait on.
    /// Immediate once promoted, a wait already running included: the promotion sends
    /// `u64::MAX` (cli692's R2 and simp724's N2 on #1594).
    pub(crate) async fn wait_applied(
        &self,
        seqno: u64,
        deadline: ReadDeadline,
    ) -> Result<(), ApiError> {
        if !self.refuses_writes() {
            return Ok(());
        }
        let mut rx = self.applied.subscribe();
        if *rx.borrow() >= seqno {
            return Ok(());
        }
        let state = self.status().state;
        if state.copying() {
            return Err(ApiError::replica_behind(format!(
                "this replica is {}: it can't wait for the primary's seqno {seqno}",
                state.as_str()
            )));
        }
        let wait = tokio::time::timeout_at(deadline.at.into(), rx.wait_for(|&a| a >= seqno));
        match wait.await {
            Ok(Ok(_)) => Ok(()),
            _ if deadline.client => Err(ApiError::deadline_exceeded()),
            _ => Err(ApiError::replica_behind(format!(
                "this replica has applied the primary's commits through seqno {}, and was \
                 waiting for {seqno} when the request timed out: retry, or read from the \
                 primary",
                *self.applied.borrow()
            ))),
        }
    }

    /// `Admin.Info`'s section: `None` once promoted.
    pub(crate) fn info(&self) -> Option<proto::ReplicaInfo> {
        if !self.refuses_writes() {
            return None;
        }
        let s = self.status();
        Some(proto::ReplicaInfo {
            primary: self.primary(),
            state: s.state.as_str().to_owned(),
            applied_primary_seqno: s.applied,
            primary_seqno: s.primary_seqno,
            lag_commits: s.primary_seqno.saturating_sub(s.applied),
            lag_ms: self.lag_ms(&s),
            last_error: s.last_error.clone(),
            bootstrap_phase: s.bootstrap_phase.to_owned(),
            bootstrap_bytes: s.bootstrap_bytes,
        })
    }

    /// How far behind it is, in milliseconds. A single node's replica: since it was last
    /// caught up, 0 while it is. A cluster replica copy (D328): since its owner last said it
    /// was caught up (`fresh_at`, since the follower started before the first), so it ages
    /// from the moment its stream stops, as a read's bound needs.
    fn lag_ms(&self, s: &Status) -> u64 {
        let since = match self.upstream {
            Upstream::Owner { .. } => s.fresh_at.unwrap_or(self.started),
            Upstream::Url => {
                if s.caught_up {
                    return 0;
                }
                s.behind_since
            }
        };
        u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// A cluster node's replica copies' `/metrics` gauges (D328), as a single node's replica
    /// reports them, each labelled by its partition.
    pub(crate) fn render_copies(copies: &[(&str, &Self)], out: &mut String) {
        use std::fmt::Write;
        let infos: Vec<(&str, proto::ReplicaInfo)> = copies
            .iter()
            .filter_map(|(id, r)| r.info().map(|i| (*id, i)))
            .collect();
        if infos.is_empty() {
            return;
        }
        type Gauge = fn(&proto::ReplicaInfo) -> String;
        let gauges: [(&str, Gauge); 4] = [
            ("pigeonhole_replica_following", |i| {
                u8::from(i.state == "following").to_string()
            }),
            ("pigeonhole_replica_applied_primary_seqno", |i| {
                i.applied_primary_seqno.to_string()
            }),
            ("pigeonhole_replica_lag_commits", |i| {
                i.lag_commits.to_string()
            }),
            ("pigeonhole_replica_lag_seconds", |i| {
                (i.lag_ms as f64 / 1000.0).to_string()
            }),
        ];
        for (name, value) in gauges {
            let _ = writeln!(out, "# TYPE {name} gauge");
            for (id, info) in &infos {
                let _ = writeln!(
                    out,
                    "{name}{{pigeonhole_partition=\"{id}\"}} {}",
                    value(info)
                );
            }
        }
    }

    /// A cluster replica copy's check of a read's bound (D328): `ReplicaBehind` unless the
    /// copy is fresh within `max_lag_ms` on its own clock, and is following (a copy that
    /// stopped, or is bootstrapping a first copy, serves nothing).
    pub(crate) fn within(&self, max_lag_ms: u64) -> Result<(), ApiError> {
        let s = self.status();
        let lag = self.lag_ms(&s);
        // Not while it bootstraps again: after a new history (a relocate without copy, a
        // restore) the old copy may hold writes the new owner never had, so it isn't a
        // stale prefix of P's history (simp724's M2 on #1989).
        let serves = matches!(s.state, State::Following | State::Retrying) && s.fresh_at.is_some();
        if serves && lag <= max_lag_ms {
            return Ok(());
        }
        Err(ApiError::replica_behind(format!(
            "this replica of partition {} is {lag} ms behind its owner ({}), more than the \
             {max_lag_ms} ms the read allows: the next replica or the owner serves it",
            self.partition().unwrap_or_default(),
            s.state.as_str()
        )))
    }

    /// The copy's range gate (D331 PR 3).
    pub(crate) fn ranges(&self) -> &super::ranges::RangeGate {
        &self.ranges
    }

    /// Derives the copy's range gate again from the node's map and the rows in `db`, the
    /// copy's file: at its open, at each map install, and after a run it applied while the
    /// gate waited for a row. **Blocking.**
    pub(crate) fn arm_ranges(&self, db: &Pigeonhole) -> Result<(), String> {
        self.take_rows(range_rows(db)?);
        Ok(())
    }

    /// [`arm_ranges`](Self::arm_ranges) with the rows read: the moved-away and serving rows
    /// and the file's visible seqno.
    fn take_rows(&self, (moved, serving, visible): RangeRows) {
        let Upstream::Owner { node, partition } = &self.upstream else {
            return;
        };
        let Some(node) = node.upgrade() else {
            return;
        };
        self.ranges.load_rows(moved, serving);
        if let Ok(map) = node.get_map() {
            self.ranges.update(&map, partition, visible);
        }
    }

    /// The owner's seqno this copy has applied through, for [`SERVED_SEQNO`].
    pub(crate) fn applied_seqno(&self) -> u64 {
        *self.applied.borrow()
    }

    /// The partition a cluster replica copy follows, `None` for a single node's replica.
    fn partition(&self) -> Option<&str> {
        match &self.upstream {
            Upstream::Owner { partition, .. } => Some(partition),
            Upstream::Url => None,
        }
    }

    /// A cluster replica copy's follower for `Cluster.Status` (D328).
    pub(crate) fn copy_status(&self) -> CopyStatus {
        let s = self.status();
        let state = match s.state {
            State::Starting | State::Bootstrapping | State::ReBootstrapping => {
                proto::ReplicaState::Bootstrapping
            }
            State::Following => proto::ReplicaState::Following,
            State::Retrying if s.last_error.contains(OWNER_FEED_OFF) => {
                proto::ReplicaState::OwnerFeedOff
            }
            State::Retrying => proto::ReplicaState::Behind,
            State::Stopped | State::Promoted => proto::ReplicaState::Stopped,
        };
        CopyStatus {
            state,
            owner: self
                .now
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .0
                .clone(),
            applied: s.applied,
            primary_seqno: s.primary_seqno,
            lag_ms: self.lag_ms(&s),
            last_error: s.last_error.clone(),
        }
    }

    /// The `/metrics` gauges.
    pub(crate) fn render(&self, out: &mut String) {
        use std::fmt::Write;
        let Some(info) = self.info() else { return };
        let following = u8::from(info.state == "following");
        let _ = writeln!(out, "# TYPE pigeonhole_replica_following gauge");
        let _ = writeln!(out, "pigeonhole_replica_following {following}");
        let _ = writeln!(out, "# TYPE pigeonhole_replica_applied_primary_seqno gauge");
        let _ = writeln!(
            out,
            "pigeonhole_replica_applied_primary_seqno {}",
            info.applied_primary_seqno
        );
        let _ = writeln!(out, "# TYPE pigeonhole_replica_lag_commits gauge");
        let _ = writeln!(out, "pigeonhole_replica_lag_commits {}", info.lag_commits);
        let _ = writeln!(out, "# TYPE pigeonhole_replica_lag_seconds gauge");
        let _ = writeln!(
            out,
            "pigeonhole_replica_lag_seconds {}",
            info.lag_ms as f64 / 1000.0
        );
    }

    /// Starts the loop on the current runtime.
    pub(crate) fn spawn(self: &Arc<Self>, shared: Arc<Shared>) {
        let me = Arc::clone(self);
        let task = tokio::spawn(async move {
            let on = On {
                service: &shared.router.service,
                idle: shared.limits.stream_idle,
            };
            me.run(&on).await;
        });
        *self.task.lock().unwrap_or_else(PoisonError::into_inner) = Some(task);
    }

    /// Starts the loop on the current runtime for a cluster node's replica copy (D328):
    /// `service` is the copy's, and `idle` the node's `--stream-idle-ms`.
    pub(crate) fn spawn_on(self: &Arc<Self>, service: Arc<Service>, idle: Duration) {
        let me = Arc::clone(self);
        let task = tokio::spawn(async move {
            let on = On {
                service: &service,
                idle,
            };
            me.run(&on).await;
        });
        *self.task.lock().unwrap_or_else(PoisonError::into_inner) = Some(task);
    }

    /// Cuts a failed follow's pause short and resets its backoff (D330 amendment 2): the
    /// node's map changed the partition's owner, epoch or history.
    pub(crate) fn wake(&self) {
        self.wake.notify_one();
    }

    /// Stops the loop and waits for it: an apply in flight finishes, and what arrived before
    /// the stop is applied.
    pub(crate) async fn stop(&self) {
        self.stop.send_replace(true);
        let task = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    /// `Admin.Promote`: stops the loop (an apply in flight finishes first), then makes the
    /// database a primary at the last event applied. Returns the primary's token stored with
    /// it.
    pub(crate) async fn promote(&self, db: &Pigeonhole) -> Result<Vec<u8>, ApiError> {
        if !self.refuses_writes() {
            return Err(ApiError::failed_precondition(
                "this server was already promoted",
            ));
        }
        if self.promoting.swap(true, Ordering::AcqRel) {
            return Err(ApiError::failed_precondition(
                "this server is already being promoted",
            ));
        }
        let promoted = self.promote_once(db).await;
        if promoted.is_err() {
            // Another Promote may try again.
            self.promoting.store(false, Ordering::Release);
        }
        promoted
    }

    async fn promote_once(&self, db: &Pigeonhole) -> Result<Vec<u8>, ApiError> {
        // Refused before the loop stops: during the first bootstrap there is nothing to
        // promote, and a stopped loop would leave the replica bootstrapping until a restart
        // (audit120). Once a bootstrap has stored a position, every later copy has one too.
        let check = db.clone();
        let bootstrapped = tokio::task::spawn_blocking(move || check.replicated_seqno())
            .await
            .map_err(|e| ApiError::internal(format!("promote: {e}")))??
            .is_some();
        if !bootstrapped {
            return Err(not_bootstrapped());
        }
        self.stop().await;
        let db = db.clone();
        let token = tokio::task::spawn_blocking(move || -> Result<_, ApiError> {
            if db.replicated_seqno()?.is_none() {
                return Ok(None);
            }
            #[cfg(test)]
            if FAIL_PROMOTE
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_deref()
                == Some(db.path())
            {
                return Err(ApiError::internal("the test's fault in the promote"));
            }
            Ok(Some(db.promote()?))
        })
        .await
        .map_err(|e| ApiError::internal(format!("promote: {e}")))
        .flatten()
        .and_then(|t| t.ok_or_else(not_bootstrapped));
        let token = match token {
            Ok(token) => token,
            Err(e) => {
                // Past the stop, a failed promote leaves a replica that follows nothing:
                // Info and the `following` gauge say so (audit120's S1 on #1801).
                self.fail(State::Stopped, format!("promote failed: {}", e.message));
                return Err(e);
            }
        };
        self.promoted.store(true, Ordering::Release);
        // A primary now: every read waiting for a seqno passes (the loop is stopped, so
        // nothing sets the value again).
        self.applied.send_replace(u64::MAX);
        self.set_state(State::Promoted);
        Ok(token.map(|t| t.as_bytes().to_vec()).unwrap_or_default())
    }

    /// The loop: follow; after an error, pause (doubling, up to [`MAX_BACKOFF`]) and go on
    /// from the stored token; on an expiry, bootstrap again. The pause resets once a follow
    /// has run for [`STEADY`], so an expiry right after a bootstrap (a primary that can't
    /// serve the token it just gave) pauses too rather than downloading back to back.
    async fn run(&self, on: &On<'_>) {
        let mut stop = self.stop.subscribe();
        let mut backoff = Duration::from_millis(100);
        // A side copy a process left when it died mid-bootstrap goes now (#1752): a replica
        // that restarts with a stored token follows from it, so no bootstrap would clear it
        // until the next one. Its failure only leaves the files (every bootstrap clears
        // them first, #1748): Info says why.
        if let Ok(part) = on.service.part()
            && let Err(e) = clear_side(&side_path(part.db.path())).await
        {
            self.status().last_error = e;
        }
        loop {
            if *stop.borrow() {
                return;
            }
            let began = Instant::now();
            let halt = self.follow(on, &mut stop).await;
            if began.elapsed() >= STEADY {
                backoff = Duration::from_millis(100);
            }
            let failed = match halt {
                Halt::Stopped => return,
                Halt::Expired(why) => {
                    self.fail(State::ReBootstrapping, why);
                    match self.bootstrap(on, &mut stop).await {
                        Ok(()) => None,
                        Err(Halt::Stopped) => return,
                        Err(Halt::Expired(e) | Halt::Failed(e)) => Some(e),
                    }
                }
                Halt::Failed(e) => Some(e),
            };
            if let Some(e) = failed {
                self.fail(State::Retrying, e);
            }
            let woken = tokio::select! {
                () = tokio::time::sleep(backoff) => false,
                () = self.wake.notified() => true,
                _ = stop.changed() => return,
            };
            backoff = if woken {
                Duration::from_millis(100)
            } else {
                (backoff * 2).min(MAX_BACKOFF)
            };
        }
    }

    /// A channel to the primary. A cluster node's replica copy (D328) looks its partition's
    /// owner up in the node's map each time, so a break after a move or relocate finds the
    /// new owner, and dials it as the node dials its peers.
    async fn connect(&self) -> Result<tonic::transport::Channel, String> {
        if let Upstream::Owner { node, partition } = &self.upstream {
            let node = node
                .upgrade()
                .ok_or_else(|| "the node is stopping".to_owned())?;
            let (owner, address) = node.owner_of(partition)?;
            *self.now.lock().unwrap_or_else(PoisonError::into_inner) = (owner, address.clone());
            return node.peer_channel(&address).await;
        }
        let mut endpoint = tonic::transport::Endpoint::from_shared(self.config.primary.clone())
            .map_err(|e| format!("--replica-of {}: {e}", self.config.primary))?
            .connect_timeout(Duration::from_secs(10))
            .http2_keep_alive_interval(Duration::from_secs(30));
        if self.config.primary.starts_with("https://") {
            endpoint = endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new().with_native_roots())
                .map_err(|e| format!("TLS to {}: {e}", self.config.primary))?;
        }
        endpoint
            .connect()
            .await
            .map_err(|e| format!("connecting to {}: {e}", self.config.primary))
    }

    /// For a replica copy (D328): why its token can't resume, when the map says partition P's
    /// history began after the copy's bootstrap. The copy's identity row is the owner's at
    /// the bootstrap snapshot, at epoch `e_b`, and a feed token taken at `e_b` resumes only
    /// while P's `history_epoch <= e_b` (D317 ticket 6). A copy move keeps `history_epoch`;
    /// a relocate without copy, a restore or a promotion sets it to the new epoch. `None`
    /// for a single node's replica, or when either can't be read (the follow then tries).
    async fn history_moved(&self, db: &pigeonhole::Pigeonhole) -> Option<String> {
        let Upstream::Owner { node, partition } = &self.upstream else {
            return None;
        };
        let history = node.upgrade()?.history_epoch(partition)?;
        let db = db.clone();
        let copy = tokio::task::spawn_blocking(move || crate::serve::cluster::identity::read(&db))
            .await
            .ok()?
            .ok()??
            .epoch;
        (history > copy).then(|| {
            format!(
                "partition {partition}'s history began again at epoch {history}, after this \
                 copy's bootstrap at epoch {copy} (a relocate without copy, or a restore): it \
                 bootstraps again"
            )
        })
    }

    /// `req` with the replica's bearer token: a cluster node's own (its cluster token, id and
    /// map version) for a replica copy (D328).
    fn request<T>(&self, message: T) -> Result<tonic::Request<T>, String> {
        if let Upstream::Owner { node, .. } = &self.upstream {
            let node = node
                .upgrade()
                .ok_or_else(|| "the node is stopping".to_owned())?;
            return node.request(message);
        }
        let mut req = tonic::Request::new(message);
        if let Some(token) = &self.config.token {
            let value = format!("Bearer {token}")
                .parse()
                .map_err(|_| "the replica's token isn't a valid header value".to_owned())?;
            req.metadata_mut().insert("authorization", value);
        }
        Ok(req)
    }

    /// Follows the primary from the stored token (bootstrapping first if there is none)
    /// until it stops.
    async fn follow(&self, on: &On<'_>, stop: &mut watch::Receiver<bool>) -> Halt {
        // The replica's service (a single node's, or a cluster node's replica copy) always
        // serves its database.
        let db = match on.service.part() {
            Ok(p) => p.db.clone(),
            Err(e) => return Halt::Failed(e.message),
        };
        let token = loop {
            let stored = {
                let db = db.clone();
                tokio::task::spawn_blocking(move || {
                    Ok::<_, pigeonhole::Error>((db.replicated_seqno()?, db.replicated_token()?))
                })
                .await
            };
            match stored {
                Ok(Ok((Some(applied), Some(token)))) => {
                    self.set_applied(applied);
                    break token;
                }
                // Not a replica yet: the first bootstrap, then follow from its token. (A
                // replica with no token can't come from `--replica-of`: its bootstrap stores
                // one.)
                // `phdb serve` checked at start that the file was empty or a replica's.
                Ok(Ok(_)) => {
                    self.set_state(State::Bootstrapping);
                    if let Err(halt) = self.bootstrap(on, stop).await {
                        return halt;
                    }
                }
                Ok(Err(e)) => return Halt::Failed(format!("reading the replica's state: {e}")),
                Err(e) => return Halt::Failed(format!("reading the replica's state: {e}")),
            }
        };
        // A replica copy whose partition began another history since its bootstrap (a
        // relocate without copy, a restore): its token can't resume there, and the owner
        // can't say so, as it can't tell which partition another database's token was for.
        if let Some(why) = self.history_moved(&db).await {
            // Its freshness was of the old history's: none until the new copy is followed.
            let mut s = self.status();
            s.fresh_at = None;
            s.pending_fresh.clear();
            drop(s);
            return Halt::Expired(why);
        }
        let channel = match self.connect().await {
            Ok(c) => c,
            Err(e) => return Halt::Failed(e),
        };
        let mut client = proto::replication_client::ReplicationClient::new(channel)
            .max_decoding_message_size(usize::MAX);
        let req = match self.request(proto::FollowRequest {
            token: token.as_bytes().to_vec().into(),
        }) {
            Ok(r) => r,
            Err(e) => return Halt::Failed(e),
        };
        let opened = tokio::select! {
            r = client.follow(req) => r,
            _ = stop.changed() => return Halt::Stopped,
        };
        let mut stream = match opened {
            Ok(r) => r.into_inner(),
            Err(status) => return halt_of(&status),
        };
        self.set_state(State::Following);
        let mut run = Run::default();
        let mut peeked = None;
        loop {
            let next = match peeked.take() {
                Some(m) => m,
                None => tokio::select! {
                    m = stream.message() => m,
                    _ = stop.changed() => {
                        // Promote or shutdown: what arrived is applied first.
                        return match self.apply_run(&db, &mut run).await {
                            Ok(()) => Halt::Stopped,
                            Err(halt) => halt,
                        };
                    }
                },
            };
            let batch = match next {
                Ok(Some(b)) => b,
                Ok(None) => {
                    return Halt::Failed(format!("{} ended the stream", self.primary()));
                }
                Err(status) => return halt_of(&status),
            };
            {
                let mut s = self.status();
                s.primary_seqno = s.primary_seqno.max(batch.primary_seqno);
                s.heard(batch.primary_seqno, Instant::now());
            }
            let quiet = batch.events.is_empty();
            for bytes in &batch.events {
                let event = match ReplicatedEvent::decode(bytes) {
                    Ok(e) => e,
                    Err(e) => return Halt::Failed(format!("a replicated event: {e}")),
                };
                if event.is_catalog() {
                    // D315: a catalog change applies on its own, between runs.
                    if let Err(halt) = self.apply_run(&db, &mut run).await {
                        return halt;
                    }
                    if let Err(halt) = self.apply_catalog(on, event).await {
                        return halt;
                    }
                    continue;
                }
                run.bytes += bytes.len();
                run.events.push(event);
                if (run.events.len() >= self.config.run_events.max(1) || run.bytes >= RUN_BYTES)
                    && let Err(halt) = self.apply_run(&db, &mut run).await
                {
                    return halt;
                }
            }
            // The latency bound: no further message ready, so what arrived is applied now.
            if !run.events.is_empty() {
                peeked = ready(&mut stream);
                if peeked.is_none()
                    && let Err(halt) = self.apply_run(&db, &mut run).await
                {
                    return halt;
                }
            }
            // Every event up to the batch's end is applied (#1853 part C2): the replica's
            // catalog must now be its primary's at the same position. A different one is a
            // schema change the primary's feed never delivered (one made under 1.1.x after a
            // rollback: a drop, or a create nothing has written to, which no apply refuses),
            // so the replica bootstraps again. A primary that sends no digest (1.2.x) isn't
            // checked.
            if run.events.is_empty()
                && peeked.is_none()
                && let Some(want) = batch.catalog_digest
            {
                let have = db.replication_catalog_digest();
                if have == want {
                    self.unconfirmed.store(false, Ordering::Relaxed);
                } else if self.unconfirmed.load(Ordering::Relaxed) {
                    // A fresh copy that doesn't match: the two sides' digests disagree for
                    // another reason, which a copy won't change (cli692's S1 on #1878). The
                    // one exception, a primary rolled back between the copy and its first
                    // match, can't be told apart here: a restart starts confirmed, so the
                    // message names that way out.
                    return Halt::Failed(format!(
                        "the replica's catalog digest {have:016x} isn't its primary's \
                         {want:016x} right after a bootstrap: a copy won't fix it, so it \
                         retries (report this); if the primary was rolled back since this \
                         copy, restart the replica to take a new copy"
                    ));
                } else {
                    return Halt::Expired(
                        "the replica's tables are no longer its primary's at the same position \
                         (a schema change the primary's feed didn't record, such as one made \
                         while it ran 1.1.x), so it bootstraps again"
                            .to_owned(),
                    );
                }
            }
            let mut s = self.status();
            let caught_up = batch.caught_up && run.events.is_empty() && peeked.is_none();
            if caught_up {
                s.caught_up = true;
            } else if s.caught_up && !quiet {
                s.caught_up = false;
                s.behind_since = Instant::now();
            }
            if !s.last_error.is_empty() && s.state == State::Following {
                s.last_error.clear();
            }
        }
    }

    /// Applies `run` (if any) in one call, with its last event's token. A refusal that says
    /// the replica's tables aren't its primary's ([`diverged`]) bootstraps again.
    async fn apply_run(&self, db: &Pigeonhole, run: &mut Run) -> Result<(), Halt> {
        let Some(last) = run.events.last() else {
            return Ok(());
        };
        #[cfg(test)]
        while HOLD_APPLIES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|d| db.path().starts_with(d))
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let applied = last.primary_seqno();
        let events = std::mem::take(&mut run.events);
        run.bytes = 0;
        let db = db.clone();
        let rearm = self.ranges.pending().then(|| db.clone());
        tokio::task::spawn_blocking(move || {
            let token = events.last().map(|e| e.token().clone());
            match token {
                Some(t) => db.apply_replicated(&events, &t),
                None => Ok(()),
            }
        })
        .await
        .map_err(|e| Halt::Failed(format!("applying a run: {e}")))?
        .map_err(|e| applying("applying a run", &e))?;
        self.set_applied(applied);
        // D331 PR 3: a gate waiting for a row (a target's serving row, a source's moved-away
        // one) looks again once the run that may have brought it is in.
        if let Some(db) = rearm {
            let rows = tokio::task::spawn_blocking(move || range_rows(&db))
                .await
                .map_err(|e| Halt::Failed(format!("re-arming the range gate: {e}")))?;
            match rows {
                Ok(rows) => self.take_rows(rows),
                Err(e) => {
                    eprintln!("phdb serve: a replica copy's range gate: {e}; it keeps the last");
                }
            }
        }
        Ok(())
    }

    /// Applies a catalog change, then drops the service's cached table handles: a table
    /// dropped (or dropped and made again) on the primary must not serve from its old one.
    async fn apply_catalog(&self, on: &On<'_>, event: ReplicatedEvent) -> Result<(), Halt> {
        let applied = event.primary_seqno();
        let db = on
            .service
            .part()
            .map_err(|e| Halt::Failed(e.message))?
            .db
            .clone();
        let outcome = tokio::task::spawn_blocking(move || db.apply_catalog(&event, event.token()))
            .await
            .map_err(|e| Halt::Failed(format!("applying a catalog change: {e}")));
        on.service.tables_changed();
        outcome?.map_err(|e| applying("applying a catalog change", &e))?;
        let applied = applied.max(self.status().applied);
        self.set_applied(applied);
        Ok(())
    }

    /// Downloads a bootstrap copy into a side file, makes it a replica at the copy's token,
    /// and swaps it in for the served database. The old copy serves until the swap.
    async fn bootstrap(&self, on: &On<'_>, stop: &mut watch::Receiver<bool>) -> Result<(), Halt> {
        let service = on.service;
        let db = service
            .part()
            .map_err(|e| Halt::Failed(e.message))?
            .db
            .clone();
        let side = side_path(db.path());
        // Every file of an earlier side copy goes first: a bootstrap that failed after its
        // copy's clean close, or a process that died mid-bootstrap, leaves the copy's WAL and
        // feed sidecars (a feed's clean close keeps its WAL), and a fresh copy opened beside
        // them fails, every time, with "WAL segment belongs to another database" (#1748). A
        // stop mid-download leaves a partial side file, which this removes too.
        clear_side(&side).await.map_err(Halt::Failed)?;
        let token = tokio::select! {
            r = self.download(&side, on.idle) => r.map_err(Halt::Failed)?,
            _ = stop.changed() => return Err(Halt::Stopped),
        };
        // From here the served engine may be the new copy, whose applied seqno can be below
        // the old copy's (a primary that itself replaced its file from an older backup), so
        // the value never exceeds what the served engine applied (simp724's N1 on #1594).
        // The follow after the swap reads the copy's own from its stored state.
        self.set_applied(0);
        // Another copy, maybe of another history: the old frontiers name its seqnos.
        self.status().pending_fresh.clear();
        let swap_side = side.clone();
        self.set_bootstrap_phase("opening the copy");
        let opened = tokio::task::spawn_blocking(move || {
            // A small memtable budget: this open only records the token, and the copy has no
            // log to replay (a backup copy is one file). The shared-memory check is then
            // 8 MiB a shard, not the default 64 (#1752): a node short of `/dev/shm` still
            // bootstraps, as the served engine's own region is already held. The shard
            // count stays the copy's (another would end its feed's history), and the swap
            // opens it with the server's own options.
            let copy = Pigeonhole::open(
                &swap_side,
                pigeonhole::Options::default().memtable_budget(8 << 20),
            )?;
            copy.begin_replication(&FeedToken::from_bytes(token))?;
            copy.close()
        })
        .await
        .map_err(|e| e.to_string())
        .and_then(|r| r.map_err(|e| e.to_string()));
        if let Err(e) = opened {
            let _ = clear_side(&side).await;
            return Err(Halt::Failed(format!("bootstrap: {e}")));
        }
        self.set_bootstrap_phase("swapping");
        let swap_side = side.clone();
        // A panic in the swap is a failed swap (#1752): its side files go and the served
        // handle drops what it cached, as for an error, since the swap may have got as far as
        // replacing or closing the engine.
        let swapped = tokio::task::spawn_blocking(move || db.replace_with(&swap_side))
            .await
            .map_err(|e| e.to_string())
            .and_then(|r| r.map_err(|e| e.to_string()));
        // The side file is the database now, or the bootstrap failed: either way its files go
        // (after a failure, its WAL and feed sidecars too: #1748).
        let _ = clear_side(&side).await;
        // The served handle is on the new engine (or closed, if the swap failed late):
        // nothing cached of the old one may serve again.
        service.engine_replaced();
        swapped.map_err(|e| Halt::Failed(format!("bootstrap: {e}")))?;
        #[cfg(test)]
        BOOTSTRAPS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(
                service
                    .part()
                    .map_or_else(|_| PathBuf::new(), |p| p.db.path().to_path_buf()),
            );
        self.unconfirmed.store(true, Ordering::Relaxed);
        let mut s = self.status();
        s.caught_up = false;
        s.behind_since = Instant::now();
        s.last_error.clear();
        // Done: Info shows no bootstrap step. A failed one keeps its last, for the error.
        s.bootstrap_phase = "";
        s.bootstrap_bytes = 0;
        Ok(())
    }

    /// `Admin.BackupDownload` with `replica` into `side`, resumed after a broken stream
    /// ([`download`](super::download::download)), checked against its SHA-256. Returns the
    /// copy's feed token.
    async fn download(&self, side: &Path, idle: Duration) -> Result<Vec<u8>, String> {
        use super::download::{Break, Failed, Step, Waits, admin, download};
        let primary = &self.primary();
        self.set_bootstrap("connecting", 0);
        let first = admin(self.connect().await?);
        let mut sink = SideFile::create(side).await?;
        // The primary answers at once and sends a heartbeat (a chunk with no data) every
        // second while it writes its copy (#1611), so every wait is bounded however long
        // the copy takes: `BEAT_WAIT` (or the idle limit, if longer) for the answer and
        // between heartbeats, the stream idle limit for the copy's data (#1595: a stalled
        // stream used to hold the bootstrap for ever, with nothing in Info).
        let waits = Waits {
            answer: idle.max(BEAT_WAIT),
            data: idle,
        };
        let say = |b: Break<'_>| match b {
            Break::Connect(e) => e.to_owned(),
            Break::NoAnswer(wait) => format!(
                "{primary} sent no bootstrap copy within {} ms",
                wait.as_millis()
            ),
            Break::NoChunk(wait) => format!(
                "no chunk of the bootstrap copy from {primary} within {} ms",
                wait.as_millis()
            ),
            Break::Ended => "the stream ended before the last chunk".to_owned(),
            Break::Status(status) => status.message().to_owned(),
        };
        // Info says where the bootstrap is, and why it's resuming (#1595), while the state
        // stays `bootstrapping`.
        let step = |s: Step<'_>| match s {
            Step::Waiting => self.set_bootstrap("waiting for the primary's copy", 0),
            Step::Data(bytes) => self.set_bootstrap("downloading", bytes),
            Step::Resuming(broke) => {
                self.status().last_error = format!("downloading the bootstrap copy: {broke}");
            }
            Step::Fresh(why) => {
                self.status().last_error =
                    format!("downloading the bootstrap copy: {why}; starting a fresh copy");
            }
        };
        let copy = download(
            first,
            || async { self.connect().await.map(admin) },
            |message| self.request(message),
            proto::BackupDownloadRequest {
                fenced_file: None,
                durable: true,
                backup_id: String::new(),
                offset: 0,
                replica: true,
                // A replica copy's: the partition, at its owner (D328).
                partition: match &self.upstream {
                    Upstream::Url => String::new(),
                    Upstream::Owner { partition, .. } => partition.clone(),
                },
            },
            &mut sink,
            waits,
            say,
            step,
        )
        .await
        .map_err(|e| match e {
            Failed::Request(e) => e,
            Failed::Refused(status) => format!("bootstrap from {primary}: {}", status.message()),
            Failed::GaveUp(why) => format!("downloading the bootstrap copy: {why}"),
            Failed::Sink(e) => format!("writing {}: {e}", side.display()),
            Failed::Offset { got, expected } => {
                format!("the bootstrap copy's chunk is at {got}, expected {expected}")
            }
            Failed::TooLong(why) => format!(
                "{primary} sent a chunk of its bootstrap copy over this replica's limit: {why}"
            ),
        })?;
        if sink.hash.finalize().as_slice() != copy.last.sha256.as_ref() {
            return Err("the bootstrap copy's SHA-256 doesn't match".to_owned());
        }
        sink.file
            .sync_all()
            .await
            .map_err(|e| format!("syncing {}: {e}", side.display()))?;
        if copy.feed_token.is_empty() {
            return Err(format!(
                "{primary} sent no feed token with its bootstrap copy"
            ));
        }
        Ok(copy.feed_token.to_vec())
    }
}

/// A bootstrap copy's side file, hashed as it is written.
struct SideFile {
    path: PathBuf,
    file: tokio::fs::File,
    hash: sha2::Sha256,
}

impl SideFile {
    async fn create(path: &Path) -> Result<Self, String> {
        let file = tokio::fs::File::create(path)
            .await
            .map_err(|e| format!("creating {}: {e}", path.display()))?;
        Ok(Self {
            path: path.to_owned(),
            file,
            hash: sha2::Sha256::new(),
        })
    }
}

impl super::download::Sink for SideFile {
    async fn write(&mut self, data: &prost::bytes::Bytes) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        self.file.write_all(data).await?;
        self.hash.update(data);
        Ok(())
    }

    async fn restart(&mut self) -> std::io::Result<()> {
        self.file = tokio::fs::File::create(&self.path).await?;
        self.hash = sha2::Sha256::new();
        Ok(())
    }
}

/// A run being gathered.
#[derive(Default)]
struct Run {
    events: Vec<ReplicatedEvent>,
    bytes: usize,
}

/// The next message if one is ready now, without waiting.
fn ready(
    stream: &mut tonic::Streaming<proto::ReplicationBatch>,
) -> Option<Result<Option<proto::ReplicationBatch>, tonic::Status>> {
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);
    match Pin::new(stream).poll_next(&mut cx) {
        Poll::Ready(Some(Ok(b))) => Some(Ok(Some(b))),
        Poll::Ready(Some(Err(e))) => Some(Err(e)),
        Poll::Ready(None) => Some(Ok(None)),
        Poll::Pending => None,
    }
}

/// What a `Follow` error means for the replica.
fn halt_of(status: &tonic::Status) -> Halt {
    match status.code() {
        // FeedExpired; or the token is another database's (the primary at the URL changed).
        tonic::Code::OutOfRange | tonic::Code::InvalidArgument => {
            Halt::Expired(status.message().to_owned())
        }
        _ => Halt::Failed(status.message().to_owned()),
    }
}

/// Removes every file of the side copy at `side` (its main file, WAL streams and feed
/// sidecar, as `Pigeonhole::database_files` lists them, ICR 0135), on a blocking thread.
/// Only `<db>.bootstrap`'s set: never the served database's.
async fn clear_side(side: &Path) -> Result<(), String> {
    let side = side.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let files = Pigeonhole::database_files(&side)
            .map_err(|e| format!("listing {}'s files: {e}", side.display()))?;
        for f in files {
            match std::fs::remove_file(&f) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(format!("removing {}: {e}", f.display())),
            }
        }
        Ok(())
    })
    .await
    .map_err(|e| format!("clearing the bootstrap's side copy: {e}"))?
}

/// The bootstrap copy's side file, beside the database.
fn side_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".bootstrap");
    path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status() -> Status {
        Status {
            state: State::Following,
            applied: 0,
            primary_seqno: 0,
            caught_up: false,
            behind_since: Instant::now(),
            last_error: String::new(),
            bootstrap_phase: "",
            bootstrap_bytes: 0,
            fresh_at: None,
            pending_fresh: std::collections::VecDeque::new(),
        }
    }

    #[test]
    fn a_copy_under_sustained_writes_is_fresh_as_of_the_message_it_applied_through() {
        // D330's M2 (simp724's note on #1974): an owner under load rarely sends
        // `caught_up`, but each message names its durable frontier. Once the copy has
        // applied through a message's frontier, it's fresh as of that message's arrival.
        let t0 = Instant::now();
        let at = |ms| t0 + Duration::from_millis(ms);
        let mut s = status();
        s.heard(100, at(0));
        s.heard(250, at(10));
        assert_eq!(s.fresh_at, None, "nothing applied yet");
        s.applied = 120;
        s.settle_fresh();
        assert_eq!(
            s.fresh_at,
            Some(at(0)),
            "through the first message's frontier"
        );
        s.heard(400, at(20));
        s.applied = 260;
        s.settle_fresh();
        assert_eq!(s.fresh_at, Some(at(10)));
        // The same frontier again moves the pending message's arrival to the later one, and
        // an idle owner's message, whose frontier the copy already holds, makes it fresh as
        // of that message's arrival at once (simp724's M1 on #1989).
        s.heard(400, at(25));
        assert_eq!(s.pending_fresh.len(), 1);
        s.applied = 400;
        s.heard(400, at(30));
        assert_eq!(s.fresh_at, Some(at(30)), "the latest message naming 400");
        s.heard(400, at(35));
        assert_eq!(s.fresh_at, Some(at(35)), "an idle owner's heartbeat");
        s.heard(401, at(40));
        s.applied = 401;
        s.settle_fresh();
        assert_eq!(s.fresh_at, Some(at(40)));
        assert!(s.pending_fresh.is_empty());
    }

    #[test]
    fn a_replica_config_never_formats_its_token() {
        let c = super::ReplicaConfig {
            primary: "https://primary:7448".into(),
            token: Some("s3cret-token".into()),
            run_events: 256,
        };
        let text = format!("{c:?}");
        assert!(!text.contains("s3cret"), "{text}");
        assert!(
            text.contains("<set>") && text.contains("primary:7448"),
            "{text}"
        );
    }
}
