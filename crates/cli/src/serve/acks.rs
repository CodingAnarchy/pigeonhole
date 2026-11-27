//! A partition owner's table of its replicas' acknowledgements, and the wait on them
//! (D335, ICR 0146).
//!
//! A cluster partition's map replica that follows through `Replication.FollowAcked` sends an
//! `Ack` after each apply that stored its token: everything at or before the position is
//! applied and durable in its copy. The owner keeps the last position per replica node.
//!
//! **The wait (PR 2):** with the map's `Partition.replica_acks` (k) at 1 or more, a write
//! is answered only once k of the map's replicas acknowledged its commit's position
//! ([`ReplicaAcks::wait`]), or answers `OutcomeUnknown` at `--cluster-replica-ack-timeout-ms`
//! or when the partition's lease ends first. While fewer than k replicas are healthy (§7),
//! writes are refused at admission ([`ReplicaAcks::admit`], the map's REFUSE). Whether a
//! write waits follows the map's k ([`ReplicaAcks::configure`], at open and on every map
//! install), never whether a stream is attached at that instant (simp724 on #2236).
//!
//! **The fallback (PR 2b, §7's ASYNC):** with the map's `replica_ack_policy` at ASYNC, a
//! short partition takes writes instead, and its owner enters [`Degraded`] from the position
//! just past the frontier. It answers a write without its k acknowledgements only once a
//! majority of the lease's grantors durably recorded that position (`LeasePart.
//! degraded_from`, echoed once synced: [`ReplicaAcks::marked`]), and only while still short.
//! It leaves once k replicas are healthy and acknowledged every write answered uncovered,
//! through a recorded clear (`LeasePart.degraded_clear`); in between, writes wait for their
//! acknowledgements as usual.
//!
//! **Who counts:** only the map's replicas of the partition. An install that drops a node
//! from `replicas`, or changes the partition's history, uncounts its stream (feed1248's A1):
//! the stream's later acks are ignored until the node follows again.
//!
//! The table lives in the serve layer, never in the engine: a partition at k = 0 with no
//! `FollowAcked` stream pays two relaxed loads per write.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, oneshot};

use super::errors::ApiError;
use super::gate::WriteGate;
use super::proto;

/// A position in a partition's replicated feed (`pigeonhole.v1.Position`): the last commit
/// at or below `seqno`, then `n` more events that aren't commits. Ordered as `(seqno, n)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub(crate) struct Pos {
    pub(crate) seqno: u64,
    pub(crate) n: u32,
}

impl Pos {
    /// A commit's position.
    pub(crate) fn commit(seqno: u64) -> Self {
        Self { seqno, n: 0 }
    }
}

impl From<&proto::Position> for Pos {
    fn from(p: &proto::Position) -> Self {
        Self {
            seqno: p.seqno,
            n: p.n,
        }
    }
}

impl From<Pos> for proto::Position {
    fn from(p: Pos) -> Self {
        Self {
            seqno: p.seqno,
            n: p.n,
        }
    }
}

/// The header (HTTP, or gRPC metadata) a write opts down with: `async` answers without
/// waiting for the partition's replica acks, as `ReplicationMode::Async` in the request
/// does. `default` is the map's k, as no header is.
pub(crate) const REPLICATION_HEADER: &str = "x-pigeonhole-replication";

/// Whether [`REPLICATION_HEADER`]'s value `v` asks for no wait. `InvalidArgument` for a
/// value other than `async` or `default` (in any case), so a typo never silently waits.
pub(crate) fn replication_async(v: Option<&[u8]>) -> Result<bool, ApiError> {
    match v {
        None => Ok(false),
        Some(v) if v.eq_ignore_ascii_case(b"async") => Ok(true),
        Some(v) if v.eq_ignore_ascii_case(b"default") => Ok(false),
        Some(v) => Err(ApiError::invalid(format!(
            "{REPLICATION_HEADER} is async or default, not {:?}",
            String::from_utf8_lossy(v)
        ))),
    }
}

/// The wait histogram's bucket bounds, in microseconds: 100 µs to 10 s, as the request
/// durations'.
const BUCKETS_US: [u64; 16] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 5_000_000, 10_000_000,
];

/// At most this many commits are timed while unacknowledged; a commit past it isn't
/// timed (a replica far behind, or none acknowledging, would otherwise grow this without
/// bound). Counted in `untimed`.
const PENDING_CAP: usize = 1 << 16;

/// §7: a counted replica is unhealthy once a write has waited this long and the replica
/// has neither acknowledged it nor acknowledged anything for as long: twice a replica
/// follow's heartbeat (1 s). An idle partition's replicas stay healthy on their stream.
const STALL: Duration = Duration::from_secs(2);

/// What the map says of one partition's acknowledgements: [`ReplicaAcks::configure`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct AckConfig {
    /// `Partition.replica_acks`: how many replicas a write waits for (0: none).
    pub(crate) k: u32,
    /// `Partition.replicas`: the only nodes whose acknowledgements count.
    pub(crate) replicas: Vec<String>,
    /// `Partition.history_epoch`: a change ends every stream's count.
    pub(crate) history: u64,
    /// The map's version: an older one never replaces a newer.
    pub(crate) version: u64,
    /// `--cluster-replica-ack-timeout-ms`: how long a write waits.
    pub(crate) timeout: Duration,
    /// `Partition.replica_ack_policy` is ASYNC (PR 2b): a short partition falls back rather
    /// than refusing writes.
    pub(crate) fallback: bool,
    /// The map's node ids, sorted: the set a fallback's record is counted against
    /// (simp724's P2 on the 2b plan).
    pub(crate) nodes: Vec<String>,
}

impl AckConfig {
    /// Partition `id`'s, as `map` has it, with the node's `timeout`: k = 0 and no
    /// replicas when `map` doesn't name it.
    pub(crate) fn of(map: &proto::ClusterMap, id: &str, timeout: Duration) -> Self {
        let nodes = node_set(map);
        match map.partitions.iter().find(|p| p.id == id) {
            Some(p) => Self {
                k: p.replica_acks,
                replicas: p.replicas.clone(),
                history: p.history_epoch,
                version: map.map_version,
                timeout,
                fallback: p.replica_ack_policy == proto::ReplicaAckPolicy::Async as i32,
                nodes,
            },
            None => Self {
                version: map.map_version,
                timeout,
                nodes,
                ..Self::default()
            },
        }
    }
}

/// `map`'s node ids, sorted: what a lease round's majority is counted over.
pub(crate) fn node_set(map: &proto::ClusterMap) -> Vec<String> {
    let mut nodes: Vec<String> = map.nodes.iter().map(|n| n.id.clone()).collect();
    nodes.sort_unstable();
    nodes
}

/// What a lease round carries for a degraded partition (PR 2b): the position its fallback
/// starts at, or that it ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mark {
    From(Pos),
    Clear,
}

/// A partition's fallback on its owner (§7's ASYNC), from entering until a majority
/// recorded its end.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Degraded {
    /// The first position that may be answered without k acknowledgements: just past the
    /// frontier when it began, so every write not covered then is at or past it.
    from: Pos,
    /// The node set a majority of whose grantors echoed `from` (synced), if one has: writes
    /// are answered uncovered only while it's the map's (P2).
    recorded: Option<Vec<String>>,
    /// The highest position answered uncovered, or at a reopen every position up to the
    /// partition's visible seqno (simp724's P1): the frontier must pass it before the clear.
    last_uncovered: Option<Pos>,
    /// Leaving: the rounds carry the clear, and writes wait for their acknowledgements.
    clearing: bool,
}

/// One partition's acknowledgements, on its owner.
#[derive(Debug, Default)]
pub(crate) struct ReplicaAcks {
    /// `FollowAcked` streams counted now. Commits are timed only while one is.
    streams: AtomicUsize,
    /// The map's k, for a write's check without the lock.
    k: AtomicU32,
    inner: Mutex<Inner>,
    /// The node's lease rounds' kick (PR 2b): entering the fallback asks for a round now
    /// rather than at the next third of a lease. Set once at open.
    kick: std::sync::OnceLock<Arc<Notify>>,
}

#[derive(Debug, Default)]
struct Inner {
    /// What the map says now.
    config: AckConfig,
    /// Per counted replica node: its live stream's generation, its last acknowledgement
    /// (none before the stream's first), and when it last moved. At most one stream per
    /// node counts: a new one replaces the old one's entry.
    nodes: HashMap<String, Entry>,
    next_generation: u64,
    /// Commits returned and not yet acknowledged, by seqno, with when each returned: the
    /// wait histogram's.
    pending: VecDeque<(u64, Instant)>,
    /// The k-th highest position the counted replicas acknowledged (the highest at
    /// k = 0, for the metrics): every write at or below it is covered.
    frontier: Option<Pos>,
    /// Writes waiting for the frontier, by their position, each with when it began.
    waiters: BTreeMap<Pos, Vec<(oneshot::Sender<u32>, Instant)>>,
    /// Every write that waited and isn't covered yet, by position, with when it began to
    /// wait (the earliest at a position). One that timed out or was answered otherwise
    /// stays here until the frontier passes it, so a replica that stopped acknowledging
    /// reads as unhealthy (§7) though no write waits on it any more (simp724's N1 on
    /// #2238). At most [`PENDING_CAP`]: past it a write isn't added, which only keeps the
    /// lowest positions, the ones health reads.
    uncovered: BTreeMap<Pos, Instant>,
    /// The partition closed here: no write waits any more.
    closed: bool,
    /// The fallback (PR 2b), while in it.
    degraded: Option<Degraded>,
    /// Times the fallback began: `/metrics`' `_degraded_total`.
    degraded_total: u64,
    /// The wait histogram: per bucket (not cumulative; the last is past the last bound),
    /// the sum in microseconds, and the commits not timed for `PENDING_CAP`.
    waits: [u64; BUCKETS_US.len() + 1],
    wait_sum_us: u64,
    untimed: u64,
    /// The counters `/metrics` shows: writes answered `OutcomeUnknown` at the timeout or
    /// for the lease's end, and refused while short.
    timeouts: u64,
    lease_lost: u64,
    refused: u64,
    /// Time with fewer than k healthy replicas: the total of past spells, and when the
    /// current one began.
    short_total: Duration,
    short_since: Option<Instant>,
}

#[derive(Debug)]
struct Entry {
    generation: u64,
    acked: Option<Pos>,
    /// When it attached or last acknowledged something new.
    moved: Instant,
}

impl ReplicaAcks {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes what the map says of the partition, at open and on every install (each
    /// under the node's install lock, so the newest wins; an older version is ignored);
    /// waiters the new frontier covers wake.
    ///
    /// A node no longer among `replicas` stops counting (feed1248's A1) by the count's
    /// filter ([`Inner::counted`]), not by dropping its stream's entry: nothing makes that
    /// stream end at the install, and a node removed and added again by two quick installs
    /// may keep the same live stream, whose acknowledgements stay true of its copy. When
    /// the partition's history changed, every entry's acknowledgement is forgotten (its
    /// positions were the old history's) but the streams stay attached, so each counts
    /// again from its next acknowledgement, without a reconnect (feed1248's Q1 and Q2 on
    /// #2238). The waiters, the uncovered writes and a fallback are kept across that
    /// change: a swap of P's file reopens it here, with a new table, so a history change on
    /// the same table keeps the owner's feed numbering, and their positions stay true
    /// (simp724's note 2 on #2238).
    pub(crate) fn configure(&self, config: AckConfig) {
        let mut inner = self.lock();
        if config.version < inner.config.version {
            return;
        }
        if inner.config.version > 0 && inner.config.history != config.history {
            let now = Instant::now();
            for e in inner.nodes.values_mut() {
                e.acked = None;
                e.moved = now;
            }
        }
        // A record counted over another node set may share no node with a majority of
        // this one (3 nodes to 5): it's counted again (simp724's P2).
        if let Some(d) = inner.degraded.as_mut()
            && d.recorded.as_ref().is_some_and(|set| *set != config.nodes)
        {
            d.recorded = None;
        }
        self.k.store(config.k, Ordering::Relaxed);
        inner.config = config;
        inner.refresh(Instant::now());
    }

    /// The node's lease rounds' kick, at open (PR 2b).
    pub(crate) fn set_kick(&self, kick: Arc<Notify>) {
        let _ = self.kick.set(kick);
    }

    fn kick(&self) {
        // `notify_one` keeps a permit, so a kick while a round runs sends the next one at
        // once (simp724).
        if let Some(n) = self.kick.get() {
            n.notify_one();
        }
    }

    /// The map's k: whether a write here waits for replicas (0: it doesn't).
    #[inline]
    pub(crate) fn k(&self) -> u32 {
        self.k.load(Ordering::Relaxed)
    }

    /// A `FollowAcked` stream from replica `node` begins: it replaces the node's earlier
    /// stream, if any, whose acknowledgements stop counting. The entry goes when the
    /// returned [`Attached`] is dropped (the stream ended). Its acknowledgements count only
    /// while the map lists the node among the partition's replicas: the follow checked the
    /// node's map, and this table may take that map a moment later (its install configures
    /// it after storing it), so the entry is kept and filtered, not refused.
    pub(crate) fn attach(self: &Arc<Self>, node: &str) -> Attached {
        let mut inner = self.lock();
        inner.next_generation += 1;
        let generation = inner.next_generation;
        if inner.closed {
            return Attached {
                acks: Arc::clone(self),
                node: node.to_owned(),
                generation,
            };
        }
        let entry = Entry {
            generation,
            acked: None,
            moved: Instant::now(),
        };
        if inner.nodes.insert(node.to_owned(), entry).is_none() {
            self.streams.fetch_add(1, Ordering::Relaxed);
        }
        inner.refresh(Instant::now());
        Attached {
            acks: Arc::clone(self),
            node: node.to_owned(),
            generation,
        }
    }

    /// A write's admission (§7, REFUSE): `Unavailable` while fewer than k of the map's
    /// replicas are healthy, retry-safe. Nothing to check at k = 0.
    #[inline]
    pub(crate) fn admit(&self, partition: &str) -> Result<(), ApiError> {
        if self.k() == 0 {
            return Ok(());
        }
        let now = Instant::now();
        let mut inner = self.lock();
        inner.track_short(now);
        let healthy = inner.healthy(now);
        if healthy >= inner.config.k {
            return Ok(());
        }
        if inner.config.fallback {
            // §7's ASYNC: taken, and answered once the fallback is recorded.
            let kick = inner.enter();
            drop(inner);
            if kick {
                self.kick();
            }
            return Ok(());
        }
        inner.refused += 1;
        Err(ApiError::unavailable(format!(
            "partition {partition:?} has {healthy} healthy replicas, and its writes wait for \
             {} (replica_acks): the write is refused, and nothing was written. Try again \
             once its replicas catch up",
            inner.config.k
        )))
    }

    /// A write on this partition returned committed: `seqno` (or a later seqno, read after
    /// the commit) is timed until the frontier covers it. One relaxed load while no stream
    /// is counted; `seqno` is read only when one is.
    #[inline]
    pub(crate) fn committed(&self, seqno: impl FnOnce() -> u64) {
        if self.streams.load(Ordering::Relaxed) == 0 {
            return;
        }
        let seqno = seqno();
        let now = Instant::now();
        let mut inner = self.lock();
        if inner.frontier.is_some_and(|f| f >= Pos::commit(seqno)) {
            // Already acknowledged (the commit was visible, and shipped, before it returned).
            inner.observe(0);
            return;
        }
        if inner.pending.len() >= PENDING_CAP {
            inner.untimed += 1;
            return;
        }
        // Commits return out of seqno order: kept sorted, so an acknowledgement drains a
        // prefix. Most arrive in order, so this inserts at the back.
        let at = inner.pending.partition_point(|&(s, _)| s <= seqno);
        inner.pending.insert(at, (seqno, now));
    }

    /// Waits until k of the map's replicas acknowledged `pos`, a write's commit (its own
    /// position, never one past it: a later one would only wait longer). How many had
    /// when it woke; 0 at once at k = 0. `OutcomeUnknown` when the timeout passes first,
    /// when `gate`'s lease ends first, or when the partition closes: the commit is durable
    /// and visible here, but a failover may lose it until replicas hold it.
    pub(crate) async fn wait(&self, pos: Pos, gate: &WriteGate) -> Result<u32, ApiError> {
        let k = self.k();
        if k == 0 {
            return Ok(0);
        }
        let partition = gate.partition();
        let (mut rx, timeout) = {
            let mut inner = self.lock();
            let timeout = inner.config.timeout;
            if inner.closed {
                return Err(closed(partition));
            }
            if inner.frontier.is_some_and(|f| f >= pos) {
                return Ok(inner.acked_at(pos));
            }
            let now = Instant::now();
            if inner.fallback_now(pos, now) {
                return Ok(inner.answer_uncovered(pos, now));
            }
            let (tx, rx) = oneshot::channel();
            inner.waiters.entry(pos).or_default().push((tx, now));
            if inner.uncovered.len() < PENDING_CAP {
                inner.uncovered.entry(pos).or_insert(now);
            }
            (rx, timeout)
        };
        let deadline = tokio::time::Instant::now() + timeout;
        let lost = loop {
            let ended = gate.ended();
            tokio::pin!(ended);
            ended.as_mut().enable();
            if !gate.leased() {
                break Lost::Lease;
            }
            let left = gate.lease_remaining();
            tokio::select! {
                woke = &mut rx => {
                    return woke.map_err(|_| closed(partition));
                }
                () = tokio::time::sleep_until(deadline) => break Lost::Timeout,
                // The lease's end on the clock: checked again, as a renewal may have moved it.
                () = tokio::time::sleep(left) => {}
                () = &mut ended => {}
            }
        };
        drop(rx);
        let mut inner = self.lock();
        if let Some(ws) = inner.waiters.get_mut(&pos) {
            ws.retain(|(tx, _)| !tx.is_closed());
            if ws.is_empty() {
                inner.waiters.remove(&pos);
            }
        }
        Err(match lost {
            Lost::Timeout => {
                inner.timeouts += 1;
                ApiError::outcome_unknown(format!(
                    "partition {partition:?}: the write committed on its owner, but {k} of its \
                     replicas didn't acknowledge it within {timeout:?} \
                     (--cluster-replica-ack-timeout-ms): it may be lost to a failover until \
                     they do. Read it back before you retry"
                ))
            }
            Lost::Lease => {
                inner.lease_lost += 1;
                ApiError::outcome_unknown(format!(
                    "partition {partition:?}'s lease on this node ended while the write \
                     waited for {k} replicas to acknowledge it: it may or may not be kept \
                     after a failover. Read it back from the partition's node before you retry"
                ))
            }
        })
    }

    /// The partition closes here: every waiting write answers `OutcomeUnknown`, and no
    /// later one waits.
    pub(crate) fn close(&self) {
        let mut inner = self.lock();
        inner.closed = true;
        inner.waiters.clear();
    }

    /// `Cluster.Status`'s `acked_position` (the frontier) and `healthy_replicas` (§7),
    /// counting only the streams of `replicas` (the partition's map replicas now).
    pub(crate) fn status(&self, replicas: &[String]) -> (Option<Pos>, u32) {
        let now = Instant::now();
        let inner = self.lock();
        let healthy = inner
            .nodes
            .iter()
            .filter(|(node, e)| {
                replicas.iter().any(|r| r == *node)
                    && inner.config.replicas.iter().any(|r| r == *node)
                    && inner.is_healthy(e, now)
            })
            .count();
        (inner.frontier, u32::try_from(healthy).unwrap_or(u32::MAX))
    }

    /// `LocalPartition.degraded_from`: where the fallback began, while in it.
    pub(crate) fn degraded_from(&self) -> Option<Pos> {
        self.lock().degraded.as_ref().map(|d| d.from)
    }

    /// What this lease round carries for the partition (PR 2b), if it's in the fallback:
    /// its start, or once k replicas are healthy and the frontier passed every write
    /// answered uncovered, its end.
    pub(crate) fn lease_mark(&self) -> Option<Mark> {
        let now = Instant::now();
        let mut inner = self.lock();
        let healthy = inner.healthy(now) >= inner.config.k;
        let frontier = inner.frontier;
        let d = inner.degraded.as_mut()?;
        if !d.clearing
            && healthy
            && d.last_uncovered
                .is_none_or(|l| frontier.is_some_and(|f| f >= l))
        {
            d.clearing = true;
        }
        Some(if d.clearing {
            Mark::Clear
        } else {
            Mark::From(d.from)
        })
    }

    /// A lease round over `nodes` (the round's map's, sorted) carried `mark`, and a majority
    /// of them echoed it, synced. A start: writes at or past it are answered uncovered from
    /// now while short, those waiting first. An end: the fallback is over. Ignored when the
    /// partition has moved on since (another start, or no longer leaving), or when `nodes`
    /// isn't the map's node set now (P2).
    pub(crate) fn marked(&self, mark: Mark, nodes: &[String]) {
        let now = Instant::now();
        let mut inner = self.lock();
        if inner.config.nodes != nodes {
            return;
        }
        let Some(d) = inner.degraded.as_mut() else {
            return;
        };
        match mark {
            Mark::From(from) if from == d.from && !d.clearing => {
                d.recorded = Some(nodes.to_vec());
                inner.wake_uncovered(now);
            }
            Mark::Clear if d.clearing => inner.degraded = None,
            _ => {}
        }
    }

    /// A grantor echoed a recorded start below this owner's (one it recorded before: a
    /// restart here lost it): this owner starts there instead, which only covers more, and
    /// counts the record again.
    pub(crate) fn lower_from(&self, from: Pos) {
        let mut inner = self.lock();
        if let Some(d) = inner.degraded.as_mut()
            && !d.clearing
            && from < d.from
        {
            d.from = from;
            d.recorded = None;
        }
    }

    /// The partition reopened here while its own grantor record says FLAGGED from `from`
    /// (simp724's P1): the fallback resumes, with every position up to `visible` (its
    /// visible seqno at open) taken as possibly answered uncovered, so it ends only once
    /// the replicas hold them all. Recorded again by the next round, which is asked now.
    pub(crate) fn resume(&self, from: Pos, visible: u64) {
        let mut inner = self.lock();
        if inner.config.k == 0 || inner.degraded.is_some() || inner.closed {
            return;
        }
        inner.degraded = Some(Degraded {
            from,
            recorded: None,
            last_uncovered: Some(Pos::commit(visible)),
            clearing: false,
        });
        drop(inner);
        self.kick();
    }

    /// Partition `partition`'s lines of the metrics [`render_all`](Self::render_all) heads:
    /// `waits` the histogram, `acked` the per-replica gauge, and `counters` the four
    /// counters (only for a partition at k ≥ 1, or one that counted something).
    pub(crate) fn render(&self, partition: &str, out: &mut Rendered) {
        use std::fmt::Write as _;
        let now = Instant::now();
        let mut inner = self.lock();
        inner.track_short(now);
        let timed: u64 = inner.waits.iter().sum();
        if timed > 0 {
            let waits = &mut out.waits;
            let mut seen = 0;
            for (i, n) in inner.waits.iter().enumerate() {
                seen += n;
                let le = BUCKETS_US
                    .get(i)
                    .map_or_else(|| "+Inf".to_owned(), |&b| (b as f64 / 1e6).to_string());
                let _ = writeln!(
                    waits,
                    "pigeonhole_cluster_replica_ack_wait_seconds_bucket{{partition=\"{partition}\",le=\"{le}\"}} {seen}"
                );
            }
            let _ = writeln!(
                waits,
                "pigeonhole_cluster_replica_ack_wait_seconds_sum{{partition=\"{partition}\"}} {}",
                inner.wait_sum_us as f64 / 1e6
            );
            let _ = writeln!(
                waits,
                "pigeonhole_cluster_replica_ack_wait_seconds_count{{partition=\"{partition}\"}} {seen}"
            );
        }
        let mut nodes: Vec<_> = inner
            .nodes
            .iter()
            .filter(|(n, _)| inner.config.replicas.iter().any(|r| r == *n))
            .filter_map(|(n, e)| e.acked.map(|p| (n, p)))
            .collect();
        nodes.sort();
        for (node, p) in nodes {
            let _ = writeln!(
                out.acked,
                "pigeonhole_cluster_replica_acked_seqno{{partition=\"{partition}\",replica=\"{node}\"}} {}",
                p.seqno
            );
        }
        let short = inner.short_total.as_secs_f64();
        if inner.config.k > 0
            || inner.timeouts + inner.lease_lost + inner.refused + inner.degraded_total > 0
            || short > 0.0
        {
            let degraded = u8::from(inner.degraded.is_some());
            for (lines, value) in [
                (&mut out.timeouts, inner.timeouts.to_string()),
                (&mut out.lease_lost, inner.lease_lost.to_string()),
                (&mut out.short, short.to_string()),
                (&mut out.refused, inner.refused.to_string()),
                (&mut out.degraded, degraded.to_string()),
                (&mut out.degraded_total, inner.degraded_total.to_string()),
            ] {
                let _ = writeln!(lines, "{{partition=\"{partition}\"}} {value}");
            }
        }
    }

    /// The `HELP` and `TYPE` lines for [`render`](Self::render)'s metrics, followed by what
    /// each partition rendered. Nothing for a metric no partition rendered.
    pub(crate) fn render_all(text: &mut String, out: &Rendered) {
        if !out.waits.is_empty() {
            text.push_str(
                "# HELP pigeonhole_cluster_replica_ack_wait_seconds From a write's commit on \
                 the partition's owner until the map's replica_acks replicas acknowledged it \
                 (D335; the first replica's at replica_acks 0).\n# TYPE \
                 pigeonhole_cluster_replica_ack_wait_seconds histogram\n",
            );
            text.push_str(&out.waits);
        }
        if !out.acked.is_empty() {
            text.push_str(
                "# HELP pigeonhole_cluster_replica_acked_seqno The seqno of each replica's \
                 last acknowledgement to the partition's owner (D335).\n# TYPE \
                 pigeonhole_cluster_replica_acked_seqno gauge\n",
            );
            text.push_str(&out.acked);
        }
        for (lines, name, help) in [
            (
                &out.timeouts,
                "pigeonhole_cluster_replica_ack_timeouts_total",
                "Writes answered OutcomeUnknown because replica_acks replicas didn't \
                 acknowledge them within --cluster-replica-ack-timeout-ms (D335). Most are \
                 covered later.",
            ),
            (
                &out.lease_lost,
                "pigeonhole_cluster_replica_ack_lease_lost_total",
                "Writes answered OutcomeUnknown because the partition's lease ended while \
                 they waited for replica acknowledgements (D335).",
            ),
            (
                &out.short,
                "pigeonhole_cluster_replica_ack_short_seconds_total",
                "Time the partition had fewer healthy replicas than replica_acks (D335).",
            ),
            (
                &out.refused,
                "pigeonhole_cluster_replica_ack_refused_total",
                "Writes refused because the partition had fewer healthy replicas than \
                 replica_acks (D335).",
            ),
            (
                &out.degraded,
                "pigeonhole_cluster_replica_ack_degraded",
                "1 while the partition's owner is in its replica_ack_policy ASYNC fallback, \
                 and may answer writes without replica_acks acknowledgements (D335).",
            ),
            (
                &out.degraded_total,
                "pigeonhole_cluster_replica_ack_degraded_total",
                "Times the partition's owner entered its replica_ack_policy ASYNC fallback \
                 (D335).",
            ),
        ] {
            if lines.is_empty() {
                continue;
            }
            let kind = if name.ends_with("_degraded") {
                "gauge"
            } else {
                "counter"
            };
            text.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n"));
            for line in lines.lines() {
                text.push_str(name);
                text.push_str(line);
                text.push('\n');
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn untimed(&self) -> u64 {
        self.lock().untimed
    }

    #[cfg(test)]
    pub(crate) fn timed(&self) -> u64 {
        self.lock().waits.iter().sum()
    }
}

/// What [`ReplicaAcks::render`] collects across partitions, per metric.
#[derive(Debug, Default)]
pub(crate) struct Rendered {
    waits: String,
    acked: String,
    timeouts: String,
    lease_lost: String,
    short: String,
    refused: String,
    degraded: String,
    degraded_total: String,
}

/// Why a write's wait ended without its acknowledgements.
enum Lost {
    Timeout,
    Lease,
}

fn closed(partition: &str) -> ApiError {
    ApiError::outcome_unknown(format!(
        "partition {partition:?} closed on this node while the write waited for its replicas \
         to acknowledge it: it may or may not be kept after a failover. Read it back from \
         the partition's node before you retry"
    ))
}

impl Inner {
    /// Enters the fallback, or stays in it (short again while leaving: the clear is given
    /// up, the start and its record kept). Whether a lease round should go now.
    fn enter(&mut self) -> bool {
        match self.degraded.as_mut() {
            Some(d) => {
                d.clearing = false;
                d.recorded.is_none()
            }
            None => {
                let from = self.frontier.map_or(Pos::default(), |f| Pos {
                    seqno: f.seqno,
                    n: f.n.saturating_add(1),
                });
                self.degraded = Some(Degraded {
                    from,
                    recorded: None,
                    last_uncovered: None,
                    clearing: false,
                });
                self.degraded_total += 1;
                true
            }
        }
    }

    /// Whether a write at `pos` may be answered without its acknowledgements now: the
    /// policy is ASYNC, the partition is short, and a majority of this map's node set
    /// recorded a start at or below `pos`. Never while leaving, so the frontier can catch
    /// up with the last write answered uncovered.
    fn fallback_now(&self, pos: Pos, now: Instant) -> bool {
        self.config.fallback
            && self.degraded.as_ref().is_some_and(|d| {
                !d.clearing && pos >= d.from && d.recorded.as_ref() == Some(&self.config.nodes)
            })
            && self.healthy(now) < self.config.k
    }

    /// Answers a write at `pos` without its acknowledgements: the count it has. It stays
    /// uncovered for health (N1) until the frontier passes it.
    fn answer_uncovered(&mut self, pos: Pos, now: Instant) -> u32 {
        if let Some(d) = self.degraded.as_mut() {
            d.last_uncovered = Some(d.last_uncovered.map_or(pos, |l| l.max(pos)));
        }
        if self.uncovered.len() < PENDING_CAP {
            self.uncovered.entry(pos).or_insert(now);
        }
        self.acked_at(pos)
    }

    /// The start just recorded: every write waiting at or past it is answered uncovered,
    /// if the partition is still short.
    fn wake_uncovered(&mut self, now: Instant) {
        let Some(from) = self.degraded.as_ref().map(|d| d.from) else {
            return;
        };
        if !self.fallback_now(from, now) {
            return;
        }
        let waiting: Vec<Pos> = self.waiters.range(from..).map(|(p, _)| *p).collect();
        for pos in waiting {
            let n = self.answer_uncovered(pos, now);
            for (tx, _) in self.waiters.remove(&pos).unwrap_or_default() {
                let _ = tx.send(n);
            }
        }
    }

    /// The entries that count: those of the map's replicas now.
    fn counted(&self) -> impl Iterator<Item = &Entry> {
        self.nodes
            .iter()
            .filter(|(node, _)| self.config.replicas.iter().any(|r| r == *node))
            .map(|(_, e)| e)
    }

    /// The frontier again, after an acknowledgement or a change of the counted streams,
    /// and the waiters it covers woken. With fewer than k acknowledgements it's none: the
    /// frontier can then fall (a stream ended), but a write answered stays answered, as
    /// the replicas that acknowledged it still hold it.
    fn refresh(&mut self, now: Instant) {
        let need = self.config.k.max(1) as usize;
        let mut acked: Vec<Pos> = self.counted().filter_map(|e| e.acked).collect();
        self.frontier = if acked.len() >= need {
            acked.sort_unstable_by(|a, b| b.cmp(a));
            Some(acked[need - 1])
        } else {
            None
        };
        if let Some(f) = self.frontier
            && self.config.k > 0
        {
            while let Some(first) = self.waiters.first_entry()
                && *first.key() <= f
            {
                let (pos, ws) = first.remove_entry();
                let n = self.acked_at(pos);
                for (tx, _) in ws {
                    let _ = tx.send(n);
                }
            }
            while let Some(first) = self.uncovered.first_entry()
                && *first.key() <= f
            {
                first.remove();
            }
        }
        if self.config.k == 0 {
            self.uncovered.clear();
        }
        self.track_short(now);
    }

    /// How many counted replicas acknowledged `pos` or past it.
    fn acked_at(&self, pos: Pos) -> u32 {
        let n = self
            .counted()
            .filter(|e| e.acked.is_some_and(|a| a >= pos))
            .count();
        u32::try_from(n).unwrap_or(u32::MAX)
    }

    /// §7's healthy: a counted replica, unless the lowest uncovered write began waiting
    /// [`STALL`] ago for a position past its last acknowledgement, and it acknowledged
    /// nothing new in that time.
    fn is_healthy(&self, e: &Entry, now: Instant) -> bool {
        let Some((&pos, &since)) = self.uncovered.first_key_value() else {
            return true;
        };
        let stalled = now.saturating_duration_since(since) >= STALL
            && e.acked.is_none_or(|a| a < pos)
            && now.saturating_duration_since(e.moved) >= STALL;
        !stalled
    }

    fn healthy(&self, now: Instant) -> u32 {
        let n = self.counted().filter(|e| self.is_healthy(e, now)).count();
        u32::try_from(n).unwrap_or(u32::MAX)
    }

    /// Adds the short spell's time so far: k ≥ 1 and fewer than k healthy replicas.
    fn track_short(&mut self, now: Instant) {
        let short = self.config.k > 0 && !self.closed && self.healthy(now) < self.config.k;
        match (short, self.short_since) {
            (true, None) => self.short_since = Some(now),
            (true, Some(since)) => {
                self.short_total += now.saturating_duration_since(since);
                self.short_since = Some(now);
            }
            (false, Some(since)) => {
                self.short_total += now.saturating_duration_since(since);
                self.short_since = None;
            }
            (false, None) => {}
        }
    }

    fn observe(&mut self, us: u64) {
        let bucket = BUCKETS_US.partition_point(|&b| b < us);
        self.waits[bucket] += 1;
        self.wait_sum_us += us;
    }
}

/// A `FollowAcked` stream's place in its partition's [`ReplicaAcks`], while it lives.
#[derive(Debug)]
pub(crate) struct Attached {
    acks: Arc<ReplicaAcks>,
    node: String,
    generation: u64,
}

impl Attached {
    /// The stream's acknowledgement of `pos`. Its first replaces the node's entry, even
    /// with a lower position (a re-bootstrapped copy starts from its snapshot); after that
    /// a lower one is ignored, so the entry only rises while the stream lives. Ignored once
    /// a newer stream from the node replaced this one, or an install uncounted it.
    pub(crate) fn ack(&self, pos: Pos) {
        let now = Instant::now();
        let mut inner = self.acks.lock();
        let Some(e) = inner.nodes.get_mut(&self.node) else {
            return;
        };
        if e.generation != self.generation {
            return;
        }
        if e.acked.is_some_and(|a| a >= pos) {
            return;
        }
        e.acked = Some(pos);
        e.moved = now;
        inner.refresh(now);
        let Some(frontier) = inner.frontier else {
            return;
        };
        while let Some(&(seqno, at)) = inner.pending.front() {
            if Pos::commit(seqno) > frontier {
                break;
            }
            inner.pending.pop_front();
            let us =
                u64::try_from(now.saturating_duration_since(at).as_micros()).unwrap_or(u64::MAX);
            inner.observe(us);
        }
    }
}

impl Drop for Attached {
    fn drop(&mut self) {
        let mut inner = self.acks.lock();
        if inner
            .nodes
            .get(&self.node)
            .is_some_and(|e| e.generation == self.generation)
        {
            inner.nodes.remove(&self.node);
            self.acks.streams.fetch_sub(1, Ordering::Relaxed);
            inner.refresh(Instant::now());
            if inner.nodes.is_empty() {
                // Nothing will acknowledge them: not timed.
                let left = inner.pending.len() as u64;
                inner.untimed += left;
                inner.pending.clear();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configured(k: u32, of: &[&str]) -> Arc<ReplicaAcks> {
        let acks = Arc::new(ReplicaAcks::default());
        acks.configure(config(k, of, 1));
        acks
    }

    fn config(k: u32, of: &[&str], version: u64) -> AckConfig {
        AckConfig {
            fallback: false,
            nodes: Vec::new(),
            k,
            replicas: replicas(of),
            history: 1,
            version,
            timeout: Duration::from_secs(30),
        }
    }

    fn replicas(of: &[&str]) -> Vec<String> {
        of.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn a_nodes_first_ack_replaces_its_entry_and_later_ones_only_rise() {
        let acks = configured(0, &["a"]);
        let a = acks.attach("a");
        a.ack(Pos::commit(10));
        a.ack(Pos::commit(7));
        assert_eq!(acks.status(&replicas(&["a"])), (Some(Pos::commit(10)), 1));
        // A new stream from the same node: its first ack replaces the entry, even lower
        // (simp724's M2), and the old stream's acks no longer count.
        let a2 = acks.attach("a");
        a2.ack(Pos::commit(5));
        a.ack(Pos::commit(20));
        assert_eq!(acks.status(&replicas(&["a"])), (Some(Pos::commit(5)), 1));
        // The old stream's end leaves the new one's entry.
        drop(a);
        assert_eq!(acks.status(&replicas(&["a"])), (Some(Pos::commit(5)), 1));
        drop(a2);
        assert_eq!(acks.status(&replicas(&["a"])), (None, 0));
    }

    #[test]
    fn only_the_maps_replicas_count() {
        let acks = configured(0, &["a", "b"]);
        let a = acks.attach("a");
        let b = acks.attach("b");
        // Not a map replica: never counted.
        let c = acks.attach("c");
        a.ack(Pos::commit(3));
        b.ack(Pos { seqno: 3, n: 1 });
        c.ack(Pos::commit(9));
        assert_eq!(
            acks.status(&replicas(&["a", "b"])),
            (Some(Pos { seqno: 3, n: 1 }), 2)
        );
        // An install that drops b uncounts its stream (A1): its later acks are ignored.
        acks.configure(config(0, &["a"], 2));
        b.ack(Pos::commit(8));
        assert_eq!(
            acks.status(&replicas(&["a", "b"])),
            (Some(Pos::commit(3)), 1)
        );
        // An older map never replaces a newer.
        acks.configure(config(0, &["a", "b"], 1));
        assert_eq!(
            acks.status(&replicas(&["a", "b"])),
            (Some(Pos::commit(3)), 1)
        );
        // A new history forgets every acknowledgement, but the streams stay attached: the
        // next acknowledgement counts without a reconnect (feed1248's Q1 on #2238).
        acks.configure(AckConfig {
            history: 2,
            ..config(0, &["a"], 3)
        });
        assert_eq!(acks.status(&replicas(&["a"])), (None, 1));
        a.ack(Pos::commit(2));
        assert_eq!(acks.status(&replicas(&["a"])), (Some(Pos::commit(2)), 1));
        // b, added again while its stream lived: counted again (feed1248's Q2).
        acks.configure(AckConfig {
            history: 2,
            ..config(0, &["a", "b"], 4)
        });
        b.ack(Pos::commit(9));
        assert_eq!(acks.status(&replicas(&["a", "b"])).1, 2);
    }

    #[test]
    fn a_commit_is_timed_to_the_first_ack_at_or_past_it() {
        let acks = configured(0, &["a", "b"]);
        // No stream: nothing timed, and the seqno isn't even read.
        acks.committed(|| unreachable!("no stream is attached"));
        let a = acks.attach("a");
        let b = acks.attach("b");
        acks.committed(|| 5);
        acks.committed(|| 3);
        acks.committed(|| 8);
        a.ack(Pos::commit(4));
        assert_eq!(acks.timed(), 1, "3 only");
        b.ack(Pos::commit(5));
        assert_eq!(acks.timed(), 2, "5 too");
        // Already past the frontier when it returned: a zero wait.
        acks.committed(|| 2);
        assert_eq!(acks.timed(), 3);
        // The last stream's end: what's left can't be timed.
        drop(a);
        drop(b);
        assert_eq!((acks.timed(), acks.untimed()), (3, 1));
        let mut out = Rendered::default();
        acks.render("p1", &mut out);
        assert!(
            out.waits
                .contains("pigeonhole_cluster_replica_ack_wait_seconds_count{partition=\"p1\"} 3"),
            "{out:?}"
        );
        assert!(out.acked.is_empty(), "no stream now: {out:?}");
        assert!(out.timeouts.is_empty(), "k = 0 counts nothing: {out:?}");
    }

    #[test]
    fn the_frontier_is_the_kth_highest_ack() {
        let acks = configured(2, &["a", "b", "c"]);
        let a = acks.attach("a");
        let b = acks.attach("b");
        let c = acks.attach("c");
        a.ack(Pos::commit(9));
        assert_eq!(
            acks.status(&replicas(&["a", "b", "c"])).0,
            None,
            "one of two"
        );
        b.ack(Pos::commit(4));
        c.ack(Pos::commit(6));
        assert_eq!(
            acks.status(&replicas(&["a", "b", "c"])).0,
            Some(Pos::commit(6))
        );
    }

    #[test]
    fn the_gauge_names_each_acknowledging_replica() {
        let acks = configured(0, &["a", "b"]);
        let a = acks.attach("a");
        let _b = acks.attach("b");
        a.ack(Pos::commit(42));
        let mut out = Rendered::default();
        acks.render("p1", &mut out);
        assert_eq!(
            out.acked,
            "pigeonhole_cluster_replica_acked_seqno{partition=\"p1\",replica=\"a\"} 42\n"
        );
        let mut text = String::new();
        ReplicaAcks::render_all(&mut text, &out);
        assert!(text.contains("# TYPE pigeonhole_cluster_replica_acked_seqno gauge"));
        assert!(!text.contains("ack_wait_seconds"), "nothing timed: {text}");
    }

    fn leased_gate() -> Arc<WriteGate> {
        let clock = Arc::new(crate::serve::gate::LeaseClock::default());
        let gate = WriteGate::opened("p1", false, clock);
        gate.renew(u64::MAX);
        gate
    }

    #[tokio::test]
    async fn a_write_waits_for_k_acks_of_its_own_position() {
        let acks = configured(2, &["a", "b"]);
        let gate = leased_gate();
        let a = acks.attach("a");
        let b = acks.attach("b");
        let waiting = {
            let acks = Arc::clone(&acks);
            let gate = Arc::clone(&gate);
            tokio::spawn(async move { acks.wait(Pos::commit(5), &gate).await })
        };
        tokio::task::yield_now().await;
        a.ack(Pos::commit(7));
        // A prepare's position at the same seqno sorts after the commit's (n > 0); one
        // replica alone doesn't cover it at k = 2.
        b.ack(Pos::commit(4));
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        b.ack(Pos::commit(5));
        assert_eq!(waiting.await.unwrap().unwrap(), 2);
        // Already covered: answered at once.
        assert_eq!(acks.wait(Pos::commit(5), &gate).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn a_wait_ends_unknown_at_its_timeout_or_the_leases_end() {
        let acks = configured(1, &["a"]);
        let gate = leased_gate();
        let _a = acks.attach("a");
        acks.configure(AckConfig {
            timeout: Duration::from_millis(20),
            ..config(1, &["a"], 2)
        });
        let e = acks.wait(Pos::commit(1), &gate).await.unwrap_err();
        assert_eq!(e.code, "OutcomeUnknown", "{e:?}");
        assert!(e.message.contains("didn't acknowledge"), "{e:?}");
        acks.configure(config(1, &["a"], 3));
        let waiting = {
            let acks = Arc::clone(&acks);
            let gate = Arc::clone(&gate);
            tokio::spawn(async move { acks.wait(Pos::commit(2), &gate).await })
        };
        tokio::task::yield_now().await;
        gate.lapse();
        let e = waiting.await.unwrap().unwrap_err();
        assert!(e.message.contains("lease"), "{e:?}");
        assert!(acks.lock().waiters.is_empty(), "both pruned");
        let mut out = Rendered::default();
        acks.render("p1", &mut out);
        assert_eq!(out.timeouts, "{partition=\"p1\"} 1\n");
        assert_eq!(out.lease_lost, "{partition=\"p1\"} 1\n");
    }

    #[tokio::test]
    async fn a_close_answers_every_waiter() {
        let acks = configured(1, &["a"]);
        let gate = leased_gate();
        let _a = acks.attach("a");
        let waiting = {
            let acks = Arc::clone(&acks);
            let gate = Arc::clone(&gate);
            tokio::spawn(async move { acks.wait(Pos::commit(2), &gate).await })
        };
        tokio::task::yield_now().await;
        acks.close();
        let e = waiting.await.unwrap().unwrap_err();
        assert!(e.message.contains("closed"), "{e:?}");
    }

    #[test]
    fn too_few_healthy_replicas_refuse_a_write() {
        let acks = configured(2, &["a", "b"]);
        let _a = acks.attach("a");
        assert_eq!(acks.admit("p1").unwrap_err().code, "Unavailable");
        let _b = acks.attach("b");
        acks.admit("p1").unwrap();
        // At k = 0 nothing is checked.
        acks.configure(config(0, &["a", "b"], 2));
        acks.admit("p1").unwrap();
        let mut out = Rendered::default();
        acks.render("p1", &mut out);
        assert_eq!(out.refused, "{partition=\"p1\"} 1\n");
    }

    #[test]
    fn a_replica_that_stops_acknowledging_a_waiting_write_is_unhealthy() {
        let acks = configured(1, &["a"]);
        let a = acks.attach("a");
        a.ack(Pos::commit(1));
        let long_ago = Instant::now() - STALL - Duration::from_millis(1);
        let mut inner = acks.lock();
        inner.nodes.get_mut("a").unwrap().moved = long_ago;
        assert_eq!(
            inner.healthy(Instant::now()),
            1,
            "idle: healthy on its stream"
        );
        // A write that waited past STALL, and timed out: still uncovered.
        inner.uncovered.insert(Pos::commit(2), long_ago);
        assert_eq!(
            inner.healthy(Instant::now()),
            0,
            "a write waited past STALL"
        );
        // Its acknowledgement covers it: healthy again.
        drop(inner);
        a.ack(Pos::commit(2));
        assert_eq!(acks.lock().healthy(Instant::now()), 1);
        assert!(acks.lock().uncovered.is_empty());
    }

    /// simp724's N1 on #2238: two writes timed out, and one replica covers only the first.
    /// The second stays uncovered, so the replica that acknowledged neither stays unhealthy
    /// (with a single "oldest", the first's cover forgot it).
    #[test]
    fn every_timed_out_write_stays_uncovered_until_the_frontier_passes_it() {
        let acks = configured(1, &["a", "b"]);
        let a = acks.attach("a");
        let b = acks.attach("b");
        a.ack(Pos::commit(1));
        b.ack(Pos::commit(1));
        let long_ago = Instant::now() - STALL - Duration::from_millis(1);
        {
            let mut inner = acks.lock();
            for e in inner.nodes.values_mut() {
                e.moved = long_ago;
            }
            inner.uncovered.insert(Pos::commit(2), long_ago);
            inner.uncovered.insert(Pos::commit(3), long_ago);
            assert_eq!(inner.healthy(Instant::now()), 0);
        }
        // a covers 2 (the frontier at k = 1): a moved, b didn't, and 3 is still uncovered.
        a.ack(Pos::commit(2));
        let inner = acks.lock();
        assert_eq!(
            inner.uncovered.keys().copied().collect::<Vec<_>>(),
            [Pos::commit(3)]
        );
        assert_eq!(inner.healthy(Instant::now()), 1, "a alone: b still stalled");
        drop(inner);
        drop(b);
    }

    fn nodes(of: &[&str]) -> Vec<String> {
        replicas(of)
    }

    /// An ASYNC partition at k = 1 of replica `a`, in a map of nodes n1..n3.
    fn fallback(k: u32) -> Arc<ReplicaAcks> {
        let acks = Arc::new(ReplicaAcks::default());
        acks.configure(AckConfig {
            fallback: true,
            nodes: nodes(&["n1", "n2", "n3"]),
            ..config(k, &["a"], 1)
        });
        acks
    }

    async fn pending(acks: &Arc<ReplicaAcks>, gate: &Arc<WriteGate>, pos: Pos) -> bool {
        tokio::time::timeout(Duration::from_millis(50), acks.wait(pos, gate))
            .await
            .is_err()
    }

    /// PR 2b: a short ASYNC partition takes the write, and answers it without its
    /// acknowledgement only once a round's majority recorded the start, of this map's node
    /// set (simp724's P2); then it leaves through a recorded clear.
    #[tokio::test]
    async fn an_async_partition_answers_uncovered_only_once_its_start_is_recorded() {
        let acks = fallback(1);
        let gate = leased_gate();
        acks.admit("p1").unwrap();
        assert_eq!(acks.degraded_from(), Some(Pos::default()));
        assert_eq!(acks.lease_mark(), Some(Mark::From(Pos::default())));
        // Not recorded: the write waits.
        assert!(pending(&acks, &gate, Pos::commit(5)).await);
        // Recorded over another node set: still not.
        acks.marked(Mark::From(Pos::default()), &nodes(&["n1", "n2"]));
        assert!(pending(&acks, &gate, Pos::commit(5)).await);
        // A waiting write is answered the moment the record lands.
        let waiting = {
            let (acks, gate) = (Arc::clone(&acks), Arc::clone(&gate));
            tokio::spawn(async move { acks.wait(Pos::commit(5), &gate).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        acks.marked(Mark::From(Pos::default()), &nodes(&["n1", "n2", "n3"]));
        assert_eq!(waiting.await.unwrap().unwrap(), 0);
        // And a later one at once.
        assert_eq!(acks.wait(Pos::commit(6), &gate).await.unwrap(), 0);
        // A node-set change: counted again before anything else is answered uncovered.
        acks.configure(AckConfig {
            fallback: true,
            nodes: nodes(&["n1", "n2", "n3", "n4", "n5"]),
            ..config(1, &["a"], 2)
        });
        assert!(pending(&acks, &gate, Pos::commit(7)).await);
        // The replica is back, but hasn't acknowledged 6: no clear yet, and a write waits
        // for its acknowledgement as usual.
        let a = acks.attach("a");
        a.ack(Pos::commit(4));
        assert_eq!(acks.lease_mark(), Some(Mark::From(Pos::default())));
        a.ack(Pos::commit(6));
        assert_eq!(acks.lease_mark(), Some(Mark::Clear));
        assert!(pending(&acks, &gate, Pos::commit(8)).await);
        // The clear recorded: out of the fallback.
        acks.marked(Mark::Clear, &nodes(&["n1", "n2", "n3", "n4", "n5"]));
        assert_eq!(acks.degraded_from(), None);
        assert_eq!(acks.lease_mark(), None);
        let mut out = Rendered::default();
        acks.render("p1", &mut out);
        assert_eq!(out.degraded, "{partition=\"p1\"} 0\n");
        assert_eq!(out.degraded_total, "{partition=\"p1\"} 1\n");
    }

    /// Short again while leaving: the clear is given up, the start and its record kept.
    #[tokio::test]
    async fn short_again_while_leaving_keeps_the_start_and_its_record() {
        let acks = fallback(1);
        let gate = leased_gate();
        let a = acks.attach("a");
        a.ack(Pos::commit(3));
        // a stalls: a write waited past STALL for a position it never acknowledged.
        {
            let long_ago = Instant::now() - STALL - Duration::from_millis(1);
            let mut inner = acks.lock();
            inner.nodes.get_mut("a").unwrap().moved = long_ago;
            inner.uncovered.insert(Pos::commit(4), long_ago);
        }
        acks.admit("p1").unwrap();
        let from = Pos { seqno: 3, n: 1 };
        assert_eq!(acks.degraded_from(), Some(from), "just past the frontier");
        acks.marked(Mark::From(from), &nodes(&["n1", "n2", "n3"]));
        assert_eq!(acks.wait(Pos::commit(4), &gate).await.unwrap(), 0);
        a.ack(Pos::commit(4));
        assert_eq!(acks.lease_mark(), Some(Mark::Clear));
        drop(a);
        acks.admit("p1").unwrap();
        assert_eq!(acks.lease_mark(), Some(Mark::From(from)));
        assert_eq!(acks.wait(Pos::commit(5), &gate).await.unwrap(), 0);
        let mut out = Rendered::default();
        acks.render("p1", &mut out);
        assert_eq!(out.degraded_total, "{partition=\"p1\"} 1\n", "one entry");
    }

    /// A grantor's earlier start lowers the owner's (it lost its own); a reopen resumes
    /// the fallback until the replicas hold every write up to the visible seqno (P1).
    #[tokio::test]
    async fn a_lower_echo_and_a_reopen_cover_more() {
        let acks = fallback(1);
        acks.admit("p1").unwrap();
        let a = acks.attach("a");
        a.ack(Pos::commit(9));
        drop(a);
        // Entered at (0, 0) before the ack; a lower echo can't go below it.
        acks.lower_from(Pos::default());
        assert_eq!(acks.degraded_from(), Some(Pos::default()));

        let reopened = fallback(1);
        reopened.resume(Pos::commit(3), 10);
        assert_eq!(reopened.lease_mark(), Some(Mark::From(Pos::commit(3))));
        reopened.lower_from(Pos::commit(2));
        assert_eq!(reopened.degraded_from(), Some(Pos::commit(2)));
        let a = reopened.attach("a");
        a.ack(Pos::commit(9));
        assert_eq!(reopened.lease_mark(), Some(Mark::From(Pos::commit(2))));
        a.ack(Pos::commit(10));
        assert_eq!(reopened.lease_mark(), Some(Mark::Clear));
        // REFUSE (no fallback) never resumes at k = 0.
        let plain = configured(0, &["a"]);
        plain.resume(Pos::commit(3), 10);
        assert_eq!(plain.lease_mark(), None);
    }
}
