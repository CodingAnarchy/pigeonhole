//! The shared service layer (D313 §2): every protocol's calls end here, as one typed
//! operation (`ServiceOp`) answered by one typed reply. gRPC and HTTP/JSON only decode into
//! these and encode out of them, so neither wraps the other.
//!
//! **Writes run detached** (simp724's B2): each commit is spawned as its own task, and the
//! request awaits its outcome. A client that goes away loses only the reply; the commit lands
//! or fails as it would have, and a lost reply is logged and counted.

mod drop_waits;
mod spanning;
mod txn;
use txn::*;
mod feeds;
mod memory_calls;
mod scan;
pub(crate) use scan::*;
mod index_calls;
pub(crate) use index_calls::*;
mod data;
mod pool;
pub(crate) use pool::*;
mod copies;
pub(crate) use copies::*;
mod metrics;
pub(crate) use metrics::*;
mod schema;
use schema::*;
mod partition;
pub(crate) use partition::*;
mod dispatch;
mod fanout;
mod ops;
pub(crate) use ops::*;
use std::collections::HashMap;
use std::ops::{Bound, ControlFlow};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use pigeonhole::{KeySet, KeysPosition, Pigeonhole, RowPosition, ScanPosition, Table};
use pigeonhole_text::QueryStats;
use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};

use super::auth::{Caller, Scope};
use super::config::Limits;
use super::convert;
use super::errors::ApiError;
use super::proto;
use super::sessions::{SnapshotGuard, Taken};

/// A page's size when a request leaves it at 0 (D313 §4: 1 MiB), and the cap on a request's
/// own.
const DEFAULT_PAGE_BYTES: u64 = 1 << 20;
pub(crate) const MAX_PAGE_BYTES: u64 = 8 << 20;

/// `Vectors.Nearest`'s largest `k`: the engine keeps `k` candidates while it ranks, before
/// the reply's byte budget cuts it (simp724's L1 on #1565).
pub(crate) const MAX_NEAREST_K: u32 = 10_000;

/// One call's answer. Replies are moved once, from the service to the encoder, so the
/// Get variant's inline cell isn't boxed: boxing would cost an allocation on every Get to
/// shrink an enum nobody stores (perf287 on D317's seam).
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum ServiceReply {
    Get(proto::GetResponse),
    MultiGet(proto::MultiGetResponse),
    ReadRow(proto::ReadRowResponse),
    Mutate(proto::MutateResponse),
    CheckAndMutate(proto::CheckAndMutateResponse),
    MutateReturning(proto::MutateReturningResponse),
    Scan(ScanPages),
    Begin(proto::BeginResponse),
    Commit(proto::TxnCommitResponse),
    OpenSnapshot(proto::OpenSnapshotResponse),
    Empty(proto::Empty),
    Feed(super::feed::FeedEvents),
    Tables(proto::ListTablesResponse),
    Table(proto::TableResponse),
    Info(proto::InfoResponse),
    Backup(proto::BackupResponse),
    Download(super::backup::Download),
    Metrics(proto::MetricsResponse),
    Traverse(proto::TraverseResponse),
    Nearest(proto::NearestResponse),
    VectorSearch(proto::VectorSearchResponse),
    VectorVerify(proto::VectorVerifyResponse),
    Promote(proto::PromoteResponse),
    Follow(super::replica::FollowBatches),
    TextSearch(proto::TextSearchResponse),
    TextRebuild(proto::TextRebuildResponse),
    TextVerify(proto::TextVerifyResponse),
    Remember(proto::RememberResponse),
    Recall(proto::RecallResponse),
    MarkUsed(proto::MarkUsedResponse),
    Forget(proto::ForgetResponse),
    MemoryWatch(super::memory::MemoryEvents),
    RecallPool(proto::RecallPoolResponse),
    /// `Transactions.Outcome` (D329 2b).
    Outcome(proto::TxnOutcomeResponse),
    RecallBodies(proto::RecallBodiesResponse),
    TextStats(proto::TextStatsResponse),
    /// A unary read's reply with its share of `--inflight-bytes` (#1567): held until the
    /// reply's body has been written ([`super::charge`]).
    Charged(Box<ServiceReply>, super::charge::Charge),
}

impl ServiceReply {
    /// The reply, and its charge if it has one.
    pub(crate) fn uncharge(self) -> (ServiceReply, Option<super::charge::Charge>) {
        match self {
            ServiceReply::Charged(r, c) => (*r, Some(c)),
            r => (r, None),
        }
    }

    /// A charged reply's message size, encoded: what a gRPC reply is charged (#1567).
    fn encoded_len(&self) -> usize {
        use prost::Message;
        match self {
            ServiceReply::Get(r) => r.encoded_len(),
            ServiceReply::MultiGet(r) => r.encoded_len(),
            ServiceReply::ReadRow(r) => r.encoded_len(),
            ServiceReply::Traverse(r) => r.encoded_len(),
            ServiceReply::Nearest(r) => r.encoded_len(),
            ServiceReply::Recall(r) => r.encoded_len(),
            _ => 0,
        }
    }
}

/// How a subscription of this server's database starts ([`Service::subscribe_part`]): a
/// client's own on a single node (the default), or one partition's part of a cluster
/// subscription.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct PartStart {
    /// Commits through this seqno after the start are passed over (`skip_through`).
    pub skip: u64,
    /// A cluster component: it announces its start, and carries D331's barriers.
    pub announce: bool,
    /// A cluster feed hub's component (#1798).
    pub internal: bool,
    /// D331 PR 3b: start at the partition's hand-over for the tables.
    pub from_hand_over: bool,
    /// D331 PR 3b: end (cleanly, for the merge) past this seqno: a component of tables a
    /// range move took from this partition whole.
    pub ends_at: Option<u64>,
}

/// `Data.Scan`'s index in `RPCS`: its outcome is counted when its stream ends.
pub(crate) const SCAN: usize = 7;

/// The RPCs, by `ServiceOp::index`.
/// `Feed.Subscribe`'s index in `RPCS`: counted when its stream ends, as a scan's is.
pub(crate) const FEED: usize = 16;

/// What [`Service::admit`] checks a call against: its name (for the refusal's message), the
/// scope it needs, whether it writes, and its request's size.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Admit {
    pub what: &'static str,
    pub scope: Scope,
    pub writes: bool,
    pub size: usize,
    /// A memory call (`Memory`'s RPCs, `/mcp`'s memory tools): the only calls a token
    /// limited to namespaces may make (audit120's F5).
    pub memory: bool,
    /// A call one cluster node makes of another: a replica copy's `Follow` and bootstrap
    /// download (D328), and a move's download of a partition's closed files, which the map's
    /// writer runs (D332, D331 PR 5; the coordinator's scope ruling, #2094). On a cluster
    /// node, the node's cluster token may make it as well as an admin token; a single node's
    /// router refuses a cluster token's (simp724's S1 on #1974).
    pub peer: bool,
}

/// `PermissionDenied` for a call other than a memory call by a token limited to namespaces
/// (audit120's F5).
pub(crate) fn namespaced_denied(what: &str) -> ApiError {
    ApiError::permission_denied(&format!(
        "{what}: a token limited to namespaces (ns=) reaches only the memory calls"
    ))
}

/// `Admin.BackupDownload`'s index in `RPCS`: counted when its stream ends.
pub(crate) const DOWNLOAD: usize = 25;

/// The engine metrics' attribute naming a cluster node's partition (D317 ticket 8b).
pub(crate) const PARTITION_ATTRIBUTE: &str = "pigeonhole.partition";

/// `Replication.Follow`'s index in `RPCS`: counted when its stream ends.
pub(crate) const FOLLOW: usize = 36;

/// `Memory.Watch`'s index in `RPCS`.
pub(crate) const MEMORY_WATCH: usize = 49;

/// The service's open tables by name, and the generation
/// [`tables_changed`](Service::tables_changed) bumps (#1632).
#[derive(Clone, Default)]
struct TableCache {
    generation: u64,
    map: HashMap<String, Table>,
}

/// The feed fan-out's limits, from the server's.
fn feed_limits(limits: &Limits) -> super::feed::FeedLimits {
    super::feed::FeedLimits {
        ring_bytes: limits.feed_ring_bytes,
        max_hubs: limits.max_feed_hubs,
        max_catch_up: limits.max_catch_up,
        quiet: limits.feed_quiet,
        stream_idle: limits.stream_idle,
        client_events: limits.feed_client_events,
        client_bytes: limits.feed_client_bytes,
        solo_after: super::feed::SOLO_AFTER,
        coalesce: limits.feed_coalesce,
    }
}

/// The database, its open tables, and the limits every call passes through.
pub(crate) struct Service {
    /// The in-flight bytes budget (R3), in bytes.
    inflight: Arc<Semaphore>,
    /// Blocking-pool calls running at once.
    blocking: Arc<Semaphore>,
    /// The light pool (#1880): small, bounded sync calls on the network workers' CPUs.
    lite: Arc<super::lite::Lite>,
    /// `/mcp` messages running at once (`--mcp-blocking`), each on a blocking thread of
    /// its own, outside `blocking`, whose permits its tools' ops take (ICR 0139's PR 3
    /// section).
    mcp_places: Arc<Semaphore>,
    /// Scans streaming at once (`--max-scans`).
    scans: Arc<Semaphore>,
    /// `--bulk-load-bytes`, in KiB: what the node's bulk loads hold at most, each its worst
    /// case for its life (the audit's F7). A cluster node's partitions share it.
    bulk_budget: Arc<Semaphore>,
    /// Transactions and pinned snapshots between requests (D313 §3).
    pub(crate) sessions: Arc<super::sessions::Sessions>,
    /// The node's session counts, shared by its partitions' tables (D317 ticket 8a).
    session_caps: Arc<super::sessions::Caps>,
    /// The node's feed limits' shared state, every partition's feeds' (D317 ticket 8a).
    feed_shared: super::feed::FeedShared,
    /// Feed and watch streams at once (`--max-streams`): a stream outlives its request's
    /// `--max-concurrent` slot, as a scan does.
    streams: Arc<Semaphore>,
    /// `Text.Rebuild`s at once: one, on a thread of its own.
    text_rebuilds: Arc<super::texts::RebuildSlot>,
    /// Memory calls at once: a sub-limit of the blocking pool, so a slow embedding endpoint
    /// can't take every blocking thread (ai1249's M6 on D313 §7).
    memory_calls: Arc<Semaphore>,
    /// Scan page reads at once: half the blocking pool, at least 1, so scans can't take
    /// every blocking thread from the other calls (perf287's G3 on #1510).
    scan_reads: Arc<Semaphore>,
    limits: Limits,
    pub(crate) metrics: Arc<Metrics>,
    /// The open connections against `--max-connections` (#1604): the listener counts them,
    /// the metrics show them.
    pub(crate) connections: Arc<super::listener::Connections>,
    /// Set on a replica server (`--replica-of`, #1436). A whole-node mode today, refused with
    /// `--cluster`; a cluster's replicas are per partition, from D317 ticket 8.
    pub(crate) replica: Option<Arc<super::replica::Replica>>,
    /// Where the server's own threads run (none in tests).
    placement: Option<Arc<super::placement::Placement>>,
    /// The unary read replies' charges (#1567).
    pub(crate) replies: Arc<super::charge::ReplyStats>,
    /// A cluster partition's write gate (D317 ticket 6): its writes answer `Fenced` while
    /// it's fenced. The node's fences hold it too. `None` on a single node and on the
    /// node's routing service.
    gate: Option<Arc<super::gate::WriteGate>>,
    /// The partition served here: the database and what's opened on it (D317's S1, cli692's
    /// split of node and partition state). `None` on a cluster node the map gives no
    /// partition, which only routes (D317 ticket 2); always `Some` on a single node.
    part: Option<Partition>,
}

impl std::fmt::Debug for Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Service")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl Service {
    /// The partition served here. A cluster node the map gives no partition only routes:
    /// the router forwards or refuses every call that needs one before it reaches the
    /// service, so this answers only a call that slipped past it, a server bug. `Internal`,
    /// not a retryable code, so a client doesn't loop on it (cli692's note 1 on d0c6312a).
    pub(crate) fn part(&self) -> Result<&Partition, ApiError> {
        self.part_opt().ok_or_else(|| {
            ApiError::internal("this cluster node only routes and serves no partition")
        })
    }

    /// `--request-timeout-ms`: the bound a forwarded call gets (D317 ticket 2).
    /// The in-flight bytes budget (R3), which a cluster's fanned-out scan pages count
    /// against (D317 ticket 4).
    pub(crate) fn inflight(&self) -> &Arc<Semaphore> {
        &self.inflight
    }

    pub(crate) fn limits(&self) -> &Limits {
        &self.limits
    }

    /// The most branches a spanning transaction may open (`--txn-max-branches`, D329).
    pub(crate) fn txn_max_branches(&self) -> usize {
        self.limits.txn_max_branches
    }

    /// How long a transaction may go idle (`--txn-idle-ms`).
    pub(crate) fn txn_idle(&self) -> std::time::Duration {
        self.limits.leases.txn_idle
    }

    pub(crate) fn request_timeout(&self) -> std::time::Duration {
        self.limits.request_timeout
    }

    /// The partition served here, if any: for the node's own work (health, metrics,
    /// shutdown), which a node that only routes does without one.
    pub(crate) fn part_opt(&self) -> Option<&Partition> {
        self.part.as_ref()
    }

    /// A node serving `db`, opened with `config`.
    pub(crate) fn new(
        db: Pigeonhole,
        limits: Limits,
        blocking_threads: usize,
        metrics: Arc<Metrics>,
        config: PartitionConfig,
    ) -> Self {
        let feeds = super::feed::FeedShared::new(limits.max_catch_up);
        let placement = config.placement.clone();
        let part = Partition::new(db, None, &limits, &feeds, None, config);
        Self::node(
            Some(part),
            limits,
            blocking_threads,
            metrics,
            feeds,
            placement,
        )
    }

    /// Partition `id` on `db`, one of this cluster node's (D317 ticket 8a): its own database,
    /// tables, indexes, feeds and session table (ids `<id>:<hex>`), sharing the node's
    /// budgets, pools, connections, metrics, limits and session caps, and the node's one
    /// `backups` in `config` (simp724's N1 on #1711). Its writes pass `gate`. Calls are
    /// admitted once, by whichever service takes them (cli692's Q1).
    pub(crate) fn for_partition(
        &self,
        db: Pigeonhole,
        id: &str,
        gate: Arc<super::gate::WriteGate>,
        config: PartitionConfig,
    ) -> Self {
        let placement = config.placement.clone();
        let part = Partition::new(
            db,
            Some(id),
            &self.limits,
            &self.feed_shared,
            Some(&gate),
            config,
        );
        Self {
            inflight: Arc::clone(&self.inflight),
            blocking: Arc::clone(&self.blocking),
            lite: Arc::clone(&self.lite),
            mcp_places: Arc::clone(&self.mcp_places),
            scans: Arc::clone(&self.scans),
            bulk_budget: Arc::clone(&self.bulk_budget),
            sessions: super::sessions::Sessions::sharing(
                self.limits.leases,
                Some(id.to_owned()),
                Arc::clone(&self.session_caps),
            ),
            session_caps: Arc::clone(&self.session_caps),
            feed_shared: self.feed_shared.clone(),
            streams: Arc::clone(&self.streams),
            text_rebuilds: Arc::clone(&self.text_rebuilds),
            memory_calls: Arc::clone(&self.memory_calls),
            scan_reads: Arc::clone(&self.scan_reads),
            limits: self.limits,
            metrics: Arc::clone(&self.metrics),
            connections: Arc::clone(&self.connections),
            replica: None,
            placement,
            replies: Arc::clone(&self.replies),
            gate: Some(gate),
            part: Some(part),
        }
    }

    /// Resumes the partition's stopped text index rebuilds once it takes writes (#1756): at
    /// its open (an owner's: the callers that open one call it, after the service is whole,
    /// never `for_partition`, which a replica copy is built with too), and after a promote.
    /// Not on a replica, whose writes are refused.
    pub(crate) fn resume_text_rebuilds(&self) {
        if self.replica.as_ref().is_some_and(|r| r.refuses_writes()) {
            return;
        }
        if let Some(part) = &self.part {
            part.texts.resume_soon(&self.text_rebuilds);
            // With `--text-pack`, its indexes are packed in the background too (#1585).
            part.texts.pack_soon(&self.text_rebuilds);
        }
    }

    /// `--text-pack` (#1585): this node's partitions pack their text indexes in the
    /// background. Off unless called: a packed index can't be read by a 1.2 build.
    pub(crate) fn text_pack(self, on: bool) -> Self {
        if on {
            self.text_rebuilds.pack_on();
        }
        self
    }

    /// A cluster node the map gives no partition: it only routes (D317 ticket 2). Its
    /// threads run on `placement`'s CPUs, as [`PartitionConfig::placement`] says.
    pub(crate) fn routing_only(
        limits: Limits,
        blocking_threads: usize,
        metrics: Arc<Metrics>,
        placement: Option<Arc<super::placement::Placement>>,
    ) -> Self {
        let feeds = super::feed::FeedShared::new(limits.max_catch_up);
        Self::node(None, limits, blocking_threads, metrics, feeds, placement)
    }

    fn node(
        part: Option<Partition>,
        limits: Limits,
        blocking_threads: usize,
        metrics: Arc<Metrics>,
        feed_shared: super::feed::FeedShared,
        placement: Option<Arc<super::placement::Placement>>,
    ) -> Self {
        let session_caps = Arc::new(super::sessions::Caps::default());
        Self {
            feed_shared,
            connections: super::listener::Connections::new(limits.max_connections),
            replies: Arc::new(super::charge::ReplyStats::default()),
            inflight: Arc::new(Semaphore::new(
                limits.inflight_bytes.min(Semaphore::MAX_PERMITS),
            )),
            blocking: Arc::new(Semaphore::new(blocking_threads.max(1))),
            // Only under the mask, as many places as network workers (simp724's C1 on #1880,
            // S1 on #1972).
            lite: Arc::new(match &placement {
                Some(p) if p.masked() => super::lite::Lite::new(p.io_threads, Some(Arc::clone(p))),
                _ => super::lite::Lite::off(),
            }),
            mcp_places: Arc::new(Semaphore::new(limits.mcp_blocking.max(1))),
            scans: Arc::new(Semaphore::new(limits.max_scans.max(1))),
            bulk_budget: super::bulk::budget_pool(limits.bulk_load_bytes),
            sessions: super::sessions::Sessions::sharing(
                limits.leases,
                None,
                Arc::clone(&session_caps),
            ),
            session_caps,
            streams: Arc::new(Semaphore::new(limits.max_streams.max(1))),
            text_rebuilds: Arc::new(super::texts::RebuildSlot::new()),
            memory_calls: Arc::new(Semaphore::new((blocking_threads / 2).max(1))),
            scan_reads: Arc::new(Semaphore::new((blocking_threads / 2).max(1))),
            limits,
            metrics,
            replica: None,
            placement,
            gate: None,
            part,
        }
    }

    /// Makes this server a replica of `replica`'s primary: writes are refused until it is
    /// promoted. The loop is started by the caller once the server runs.
    pub(crate) fn replica(mut self, replica: Arc<super::replica::Replica>) -> Self {
        self.replica = Some(replica);
        // A cluster replica copy (D328) serves reads under its own freshness bound, which
        // the router checks per request against the client's, never under a lease: copies
        // stay out of the lease machinery (S3). Its writes are refused by the engine (R6).
        self.gate = None;
        // A replica's rebalancer could only fail (its writes are refused, R6): none runs
        // until a promote (cli692's F3 on #1583).
        if let Some(part) = &self.part {
            part.indexes.pause_rebalancing(true);
        }
        self
    }

    /// Read-your-writes on a replica (#1577): with the client's `min_primary_seqno` (the
    /// `x-pigeonhole-min-primary-seqno` header), a read waits, until `deadline` at most, for
    /// the replica to apply the primary's commits through it. Reads only: a write is refused
    /// on a replica anyway, and `Info` and `Metrics` answer at once (a monitor must see a
    /// lagging replica). A server that isn't a replica, or was promoted, ignores it. The wait
    /// holds the request's slot and budget, as any read in progress does.
    pub(crate) async fn await_primary(
        &self,
        op: &ServiceOp,
        min_primary_seqno: Option<u64>,
        deadline: super::replica::ReadDeadline,
    ) -> Result<(), ApiError> {
        let (Some(seqno), Some(replica)) = (min_primary_seqno, &self.replica) else {
            return Ok(());
        };
        if op.scope() != Scope::Read || matches!(op, ServiceOp::Info(_) | ServiceOp::Metrics(_)) {
            return Ok(());
        }
        replica.wait_applied(seqno, deadline).await
    }

    /// `Admin.Info`'s replica section.
    fn replica_info(&self) -> Option<proto::ReplicaInfo> {
        self.replica.as_ref().and_then(|r| r.info())
    }

    /// The catalog changed under the service (a replica applied a primary's change): the
    /// cached table, vector index and text index handles go, and the next call on each name
    /// opens it
    /// again.
    ///
    /// The memory store is opened again too (#1618). Its clear is synchronous: the replica
    /// publishes the seqno it applied only after this returns, so a read waiting for that
    /// seqno opens the store afresh (cli692's D2).
    pub(crate) fn tables_changed(&self) {
        self.handles_changed();
        if let Some(memory) = self.part.as_ref().and_then(|p| p.memory.get()) {
            memory.clear();
        }
    }

    /// The cached table, vector index and text index handles go: a replica's catalog change,
    /// or a `DropTable` here (#1588), whose name may have been an index's. The next call on
    /// each name opens it again. A forgotten background index's rebalancer sees its index
    /// close and ends. An open running across this keeps nothing (#1632).
    fn handles_changed(&self) {
        let Some(part) = &self.part else { return };
        part.catalog.fetch_add(1, Ordering::Release);
        part.tables.rcu(|c| TableCache {
            generation: c.generation + 1,
            map: HashMap::new(),
        });
        part.indexes.forget();
        part.texts.forget();
    }

    /// The served database was replaced under the handle (#1436's bootstrap): the table
    /// cache, the feed hubs, the memory store's watches and the sessions are of the old
    /// engine, and go.
    pub(crate) fn engine_replaced(&self) {
        self.tables_changed();
        if let Some(part) = &self.part {
            if let Some(memory) = part.memory.get() {
                memory.replaced();
            }
            part.feeds.fail_all();
        }
        self.sessions.drop_idle();
    }

    /// Serves the store `slot` holds, if any, as the `Memory` service, with at most `calls`
    /// memory calls on the blocking pool at once (0: half the pool, at least 1). A cluster
    /// node's service takes `calls` alone: its partitions share the limit and set their
    /// stores later (#1915).
    pub(crate) fn memory_opt(
        mut self,
        slot: Option<super::memslot::MemSlot>,
        calls: usize,
    ) -> Self {
        if calls > 0 {
            self.memory_calls = Arc::new(Semaphore::new(calls));
        }
        if let (Some(slot), Some(part)) = (slot, &self.part) {
            // A new partition's: nothing set it before.
            let _ = part.memory.set(Arc::new(slot));
        }
        self
    }

    /// The table cache's generation, for a test to run [`table`](Self::table)'s steps with a
    /// catalog change between them (#1632).
    #[cfg(test)]
    pub(crate) fn table_generation(&self) -> u64 {
        self.part.as_ref().map_or(0, |p| p.tables.load().generation)
    }

    /// Whether the table cache holds a handle for `name`.
    #[cfg(test)]
    pub(crate) fn table_cached(&self, name: &str) -> bool {
        self.part
            .as_ref()
            .is_some_and(|p| p.tables.load().map.contains_key(name))
    }

    /// Sets a cluster partition's memory store slot (#1915), unless one is set already.
    pub(crate) fn set_memory(&self, slot: super::memslot::MemSlot) {
        if let Some(part) = &self.part {
            let _ = part.memory.set(Arc::new(slot));
        }
    }

    /// The node's CPU placement, if it has one.
    pub(crate) fn placement_of(&self) -> Option<&Arc<super::placement::Placement>> {
        self.placement.as_ref()
    }

    /// The memory store's slot: `/mcp`'s memory tools (8c-3b), and a test's inspection of
    /// the store it holds (#1640).
    pub(crate) fn memory_slot(&self) -> Option<&Arc<super::memslot::MemSlot>> {
        self.part.as_ref()?.memory.get()
    }

    /// A memory call's answer where no store is served. On a cluster partition a memory
    /// call reaches here only because the map places the store on it, so its node lacks
    /// `--memory` (simp724's ask 2 on #1920): `FailedPrecondition`, naming the flag.
    fn no_memory(&self) -> ApiError {
        match &self.gate {
            Some(_) => ApiError::failed_precondition(
                "the cluster map places the memory store on this partition, but its node was \
                 started without --memory: restart it with --memory NAME and the memory flags \
                 the other nodes have",
            ),
            None => {
                ApiError::unsupported("this server has no memory store (phdb serve --memory NAME)")
            }
        }
    }

    /// The in-flight budget's free bytes, for tests to check nothing leaked.
    #[cfg(test)]
    pub(crate) fn inflight_available(&self) -> usize {
        self.inflight.available_permits()
    }

    /// `--bulk-load-bytes`' free KiB, for tests to check a load released its reservation.
    #[cfg(test)]
    pub(crate) fn bulk_budget_available(&self) -> usize {
        self.bulk_budget.available_permits()
    }

    /// Takes `bytes` of the in-flight budget, waiting at most the request timeout: a call's
    /// own, or for a front-end that knows a request's size before reading it (HTTP's
    /// `Content-Length`).
    pub(crate) async fn reserve(&self, bytes: usize) -> Result<OwnedSemaphorePermit, ApiError> {
        budget_within(&self.inflight, bytes, self.limits.request_timeout).await
    }

    /// `reserve`, waiting at most `wait`.
    pub(crate) async fn reserve_within(
        &self,
        bytes: usize,
        wait: Duration,
    ) -> Result<OwnedSemaphorePermit, ApiError> {
        budget_within(&self.inflight, bytes, wait).await
    }
}
