//! The shared service layer (D313 §2): every protocol's calls end here, as one typed
//! operation (`ServiceOp`) answered by one typed reply. gRPC and HTTP/JSON only decode into
//! these and encode out of them, so neither wraps the other.
//!
//! **Writes run detached** (simp724's B2): each commit is spawned as its own task, and the
//! request awaits its outcome. A client that goes away loses only the reply; the commit lands
//! or fails as it would have, and a lost reply is logged and counted.

mod memory_calls;
use memory_calls::*;
mod scan;
use scan::*;
mod index_calls;
use index_calls::*;
mod data;
use data::*;
mod pool;
use pool::*;
mod copies;
use copies::*;
mod metrics;
use metrics::*;
mod schema;
use schema::*;
mod partition;
use partition::*;
mod fanout;
use fanout::*;
mod dispatch;
use dispatch::*;
mod ops;
use ops::*;
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

    /// `Feed.Subscribe` (D313 R2): the client's tables resolved once (so its hub and any
    /// catch-up stream follow the same table ids), its start checked, and its stream handed to
    /// the fan-out. It holds a `--max-streams` slot until it ends.
    async fn subscribe(
        &self,
        r: proto::SubscribeRequest,
        caller: &Caller,
        budget: OwnedSemaphorePermit,
    ) -> Result<super::feed::FeedEvents, ApiError> {
        if r.allow_partial || !r.skip_through.is_empty() || r.checkpoint_every != 0 {
            return Err(ApiError::invalid(
                "allow_partial, skip_through and checkpoint_every are for a cluster \
                 subscription (D317), and this server isn't a cluster node",
            ));
        }
        drop(budget);
        self.subscribe_part(r, caller, PartStart::default()).await
    }

    /// D331 PR 3b: the last seqno of a range move that took table `table` from this
    /// partition, while its moved-away row is here: a cluster component of it reads up to
    /// there (the latest, for several).
    pub(crate) fn moved_away(&self, table: &str) -> Option<u64> {
        self.gate
            .as_ref()?
            .ranges()
            .moved_rows()
            .into_iter()
            .filter(|m| m.table == table)
            .map(|m| m.final_seqno)
            .max()
    }

    /// A subscription of this server's database: a single node's `Feed.Subscribe`, or one
    /// partition's part of a cluster subscription (D317 ticket 7), whose `skip` passes over
    /// commits the client already handled ([`super::feed::Spec::skip`]) and which
    /// `announce`s its start ([`super::feed::Spec::announce`]). Holds a `--max-streams`
    /// place either way.
    ///
    /// D331 PR 3b, on a cluster partition a range move gave a range of these tables: a
    /// cluster component (`announce`) that starts at or before a move's hand-over carries
    /// its barrier ([`super::feed::Barrier`]), and `from_hand_over` starts it at the earliest
    /// hand-over of these tables here, not at `r`'s start (a resume that gained this
    /// partition through a move; `FeedExpired` if no move did).
    pub(crate) async fn subscribe_part(
        &self,
        mut r: proto::SubscribeRequest,
        caller: &Caller,
        part: PartStart,
    ) -> Result<super::feed::FeedEvents, ApiError> {
        let PartStart {
            skip,
            announce,
            internal,
            from_hand_over,
            ends_at,
        } = part;
        use super::feed::Start;
        use proto::subscribe_request::Start as S;
        let slot = self.stream_slot()?;
        if r.tables.is_empty() {
            return Err(ApiError::invalid("a subscription names at least one table"));
        }
        let mut names = r.tables.clone();
        names.sort();
        names.dedup();
        let mut tables = Vec::with_capacity(names.len());
        for name in &names {
            tables.push(self.table(name).await?);
        }
        let barriers = if announce {
            self.hand_overs(&names, &mut r, from_hand_over)?
        } else {
            Vec::new()
        };
        let start = match r.start {
            None | Some(S::FromOldest(false)) => Start::Now,
            Some(S::FromOldest(true)) => Start::Oldest,
            Some(S::FromSnapshot(id)) => Start::Snapshot(
                self.sessions
                    .snapshot(&id, caller.principal)?
                    .snapshot()
                    .clone(),
            ),
            Some(S::Resume(token)) => {
                Start::Token(pigeonhole::FeedToken::from_bytes(token.to_vec()))
            }
        };
        let metrics = Arc::clone(&self.metrics);
        let started = Instant::now();
        self.part()?
            .feeds
            .subscribe(
                super::feed::Spec {
                    tables,
                    prefix: r.prefix.to_vec(),
                    visible: r.visible,
                    start,
                    skip,
                    announce,
                    internal,
                },
                slot,
                move |e| {
                    // A client's own subscription that hit the idle limit; a cluster
                    // subscription's part (`announce`) is counted with its subscription.
                    if !announce && e.is_some_and(|e| e.code == "DeadlineExceeded") {
                        metrics.feed_idle_drops.fetch_add(1, Ordering::Relaxed);
                    }
                    metrics.count(FEED, e.map(|e| e.code.as_str()), started.elapsed());
                },
            )
            .and_then(|events| match &self.gate {
                // A cluster partition's: delivered only under its lease (D326 PR 2). A
                // subscribe isn't a read for the gate (feed1248's F1): it waits for a lease
                // it hasn't had yet, and is refused only where it lapsed or the node can't
                // reach a majority (F2).
                Some(gate) => events.leased(Arc::clone(gate)),
                None => Ok(events),
            })
            .map(|events| {
                // A cluster component of a cluster partition watches for a hand-over it
                // takes while running (D331 PR 3b).
                let watch = self
                    .gate
                    .as_ref()
                    .filter(|_| announce)
                    .map(|g| (Arc::clone(g), names.clone()));
                events.with_barriers(barriers, ends_at, watch)
            })
    }

    /// The range moves' hand-overs a cluster component of `tables` here carries (D331 PR
    /// 3b): one barrier for each serving row of these tables, whatever the start.
    /// `from_hand_over` first sets `r`'s start to the earliest of their tokens. None on a
    /// partition no move gave a range.
    fn hand_overs(
        &self,
        tables: &[String],
        r: &mut proto::SubscribeRequest,
        from_hand_over: bool,
    ) -> Result<Vec<super::feed::Barrier>, ApiError> {
        use proto::subscribe_request::Start as S;
        let rows: Vec<super::ranges::Moved> = self
            .gate
            .as_ref()
            .map(|g| g.ranges().serving_rows())
            .unwrap_or_default()
            .into_iter()
            .filter(|m| !m.token.is_empty() && tables.contains(&m.table))
            .collect();
        if from_hand_over {
            // A move into this partition that hasn't reached CLEANING (or whose serving row
            // isn't here yet) is no gap: it serves none of the range yet, so the resume asks
            // again shortly (cli692's M1 on PR 3b). No move at all is one.
            let handing = self
                .gate
                .as_ref()
                .and_then(|g| g.ranges().load().as_ref().cloned())
                .is_some_and(|s| tables.iter().any(|t| s.awaits_hand_over(t)));
            if rows.is_empty() && handing {
                return Err(ApiError::partition_unavailable(
                    "this partition is taking a range of these tables over from another, \
                     and doesn't serve it yet: resume again shortly",
                ));
            }
            let first = rows.iter().min_by_key(|m| m.seqno).ok_or_else(|| {
                ApiError::from_code(
                    pigeonhole::ErrorCode::FeedExpired,
                    "no range move gave this partition a range of these tables, so a \
                     resume has no hand-over to start it at: subscribe again",
                )
            })?;
            r.start = Some(S::Resume(first.token.clone().into()));
        }
        Ok(barriers_for(rows, r.start.as_ref()))
    }

    /// The feed fan-out's limits: a cluster subscription's buffer and idle limit are a
    /// single client's (D317 ticket 7), on a node that only routes too.
    /// The node's feed state every partition shares: its catch-up slots and the rings
    /// that split `--feed-ring-bytes` (#1798's cluster feed hubs use both).
    pub(crate) fn feed_shared(&self) -> &super::feed::FeedShared {
        &self.feed_shared
    }

    pub(crate) fn feed_limits(&self) -> super::feed::FeedLimits {
        feed_limits(&self.limits)
    }

    /// `Transactions.Begin`: a transaction at a snapshot taken now, held for the caller.
    fn begin(
        &self,
        r: proto::BeginRequest,
        caller: &Caller,
    ) -> Result<proto::BeginResponse, ApiError> {
        let txn = self.part()?.db.transaction()?;
        let snapshot_seqno = txn.snapshot_seqno();
        let txn_id =
            self.sessions
                .begin(caller.principal, txn, convert::durability(r.durability))?;
        Ok(proto::BeginResponse {
            txn_id,
            snapshot_seqno,
        })
    }

    /// Takes transaction `id` for a call that only an open transaction answers.
    fn open_txn(&self, id: &str, caller: &Caller) -> Result<super::sessions::TxnGuard, ApiError> {
        match self.sessions.take(id, caller.principal)? {
            Taken::Open(g) => Ok(g),
            Taken::Done(outcome) => Err(ApiError::transaction_finished(&outcome)),
        }
    }

    /// A request's rows, ready to buffer: each row's table opened and every write checked
    /// and decoded (`convert::txn_ops`), all before the transaction is taken out. So a missing
    /// table or a malformed write fails the call with nothing buffered (simp724's T1 on
    /// #1511), and buffering the rest can't fail on the server's side.
    pub(crate) async fn txn_rows<'r>(
        &self,
        rows: &'r [proto::RowWrites],
    ) -> Result<Vec<(Table, &'r [u8], Vec<convert::TxnOp<'r>>)>, ApiError> {
        let mut out = Vec::with_capacity(rows.len());
        for rw in rows {
            let key = rw
                .row
                .as_ref()
                .ok_or_else(|| ApiError::invalid("each row's writes name its table and row"))?;
            let ops = convert::txn_ops(&rw.writes)?;
            out.push((self.table(&key.table).await?, &key.row[..], ops));
        }
        Ok(out)
    }

    async fn txn_get(
        &self,
        r: proto::TxnGetRequest,
        caller: &Caller,
    ) -> Result<proto::GetResponse, ApiError> {
        let table = self.table(&r.table).await?;
        let mut g = self.open_txn(&r.txn_id, caller)?;
        let cell = g
            .txn()
            .get_async(&table, &r.row, &r.family, &r.qualifier)
            .await?;
        drop(g);
        Ok(proto::GetResponse {
            cell: cell.map(|c| {
                let kind = convert::View::of(&table, &r.family);
                convert::cell(&r.family, &r.qualifier, &c, kind, None)
            }),
        })
    }

    /// On the blocking pool, as `get_row` is sync-only (D196): the transaction is lent to the
    /// blocking call and back, and a panic there expires the session (uncommitted).
    async fn txn_get_row(
        &self,
        r: proto::TxnGetRowRequest,
        caller: &Caller,
    ) -> Result<proto::ReadRowResponse, ApiError> {
        let table = self.table(&r.table).await?;
        let mut g = self.open_txn(&r.txn_id, caller)?;
        let mut txn = g.lend();
        let (txn, row) = self
            .blocking(move || {
                let families: Vec<&str> = r.families.iter().map(String::as_str).collect();
                let kinds = convert::Kinds::new(&table);
                let row = txn
                    .get_row(&table, &r.row, &families)
                    .map(|row| row.map(|row| convert::row(row, &kinds, None)));
                Ok((txn, row))
            })
            .await?;
        g.restore(txn);
        drop(g);
        let row = row?;
        let bytes = row.as_ref().map_or(0, row_bytes);
        Ok(proto::ReadRowResponse {
            row,
            next: Default::default(),
            bytes,
        })
    }

    /// `Transactions.Write`: buffers the writes, all or none (`txn_rows`). What only the
    /// engine can check (a family, a size) fails the `Commit`, as in-process.
    async fn txn_write(
        &self,
        r: proto::TxnWriteRequest,
        caller: &Caller,
    ) -> Result<proto::Empty, ApiError> {
        let rows = self.txn_rows(&r.rows).await?;
        let mut g = self.open_txn(&r.txn_id, caller)?;
        for (table, row, ops) in &rows {
            convert::txn_apply(g.txn(), table, row, ops);
        }
        if !rows.is_empty() {
            g.wrote_now();
        }
        Ok(proto::Empty {})
    }

    /// `Transactions.Commit`, detached from the request (D313 B2): the commit runs in its own
    /// task, which records the outcome as the session's tombstone (B1), so a client that goes
    /// away loses only the reply. A repeated `Commit` answers the tombstone.
    async fn commit(
        &self,
        r: proto::TxnCommitRequest,
        caller: &Caller,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::TxnCommitResponse, ApiError> {
        if !r.rows.is_empty() && !caller.scope.allows(Scope::Write) {
            return Err(ApiError::permission_denied("commit writes"));
        }
        let rows = self.txn_rows(&r.rows).await?;
        let mut g = match self.sessions.take(&r.txn_id, caller.principal)? {
            Taken::Open(g) => g,
            Taken::Done(outcome) => return outcome,
        };
        // A commit that writes is a write to a fenced partition (D317 ticket 6); a
        // read-only one is a consistent read. Refused, the transaction stays open.
        let held = match &self.gate {
            Some(gate) if g.wrote() || !rows.is_empty() => Some(gate.enter()?),
            _ => None,
        };
        // D331: the rows it carries, as writes, and M3, a transaction open across a range's
        // fence or hand-over, as simp724 agreed it ([`super::ranges`]); one load.
        if let Some(gate) = &self.gate
            && let Some(ranges) = &*gate.ranges().load()
        {
            ranges.commit(
                &r.rows,
                g.txn().snapshot_seqno(),
                g.wrote() || !rows.is_empty(),
            )?;
        }
        for (table, row, ops) in &rows {
            convert::txn_apply(g.txn(), table, row, ops);
        }
        drop(rows);
        let (txn, durability, ticket) = g.commit();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let metrics = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            let commit = match durability {
                Some(d) => txn.commit_with_async(d),
                None => txn.commit_async(),
            };
            // Its own task, so a panic still records an outcome (unknown), never nothing.
            let outcome = match tokio::spawn(commit).await {
                Ok(r) => r.map_err(ApiError::from),
                Err(e) => Err(ApiError::outcome_unknown(format!(
                    "the commit's task failed: {e}"
                ))),
            };
            // A commit that returned past the partition's lease is unknown (D326), and a
            // retry of it finds the same.
            let outcome = match (outcome, &held) {
                (Ok(c), Some(h)) => h.committed().map(|()| c),
                (o, _) => o,
            };
            ticket.finish(outcome.clone().map(|c| proto::TxnCommitResponse {
                commit: Some(convert::commit_info(c)),
                parts: Vec::new(),
            }));
            drop(budget);
            drop(held);
            if tx.send(outcome).is_err() {
                metrics.replies_lost.fetch_add(1, Ordering::Relaxed);
            }
        });
        let outcome = rx.await.unwrap_or_else(|_| {
            Err(ApiError::outcome_unknown(
                "the commit's task ended without an outcome",
            ))
        })?;
        Ok(proto::TxnCommitResponse {
            commit: Some(convert::commit_info(outcome)),
            parts: Vec::new(),
        })
    }

    fn abort(&self, r: &proto::TxnAbortRequest, caller: &Caller) -> Result<proto::Empty, ApiError> {
        self.open_txn(&r.txn_id, caller)?.abort();
        Ok(proto::Empty {})
    }

    /// The home partition's `Begin` of a spanning transaction (D329): its id carries the
    /// home's history epoch (`epoch`), and is its xid. The decision table is made first, if
    /// it's missing, so it's older than the transaction's snapshot: its commit point reads
    /// and writes it, which a table created after the snapshot would refuse.
    pub(crate) async fn begin_spanning(
        &self,
        r: proto::BeginRequest,
        caller: &Caller,
        epoch: u64,
    ) -> Result<proto::BeginResponse, ApiError> {
        if let Some(gate) = &self.gate {
            gate.serving()?;
        }
        drop(self.decisions_table().await?);
        let txn = self.part()?.db.transaction()?;
        let snapshot_seqno = txn.snapshot_seqno();
        let txn_id = self.sessions.begin_spanning(
            caller.principal,
            txn,
            convert::durability(r.durability),
            epoch,
        )?;
        Ok(proto::BeginResponse {
            txn_id,
            snapshot_seqno,
        })
    }

    /// Buffers decoded `rows` ([`txn_rows`](Self::txn_rows)) in the transaction `g` holds:
    /// it can't fail (D329's coordinator buffers a spanning call's home rows on the session
    /// it already holds).
    pub(crate) fn buffer_rows(
        g: &mut super::sessions::TxnGuard,
        rows: &[(Table, &[u8], Vec<convert::TxnOp<'_>>)],
    ) {
        for (table, row, ops) in rows {
            convert::txn_apply(g.txn(), table, row, ops);
        }
        if !rows.is_empty() {
            g.wrote_now();
        }
    }

    /// Whether the partition takes writes now (its gate admits one): what a spanning call
    /// checks before its first row goes to a branch.
    pub(crate) fn writable(&self) -> Result<(), ApiError> {
        match &self.gate {
            Some(gate) => gate.enter().map(drop),
            None => Ok(()),
        }
    }

    /// Ends spanning transaction `g` after a call that sent rows to a branch failed (D329,
    /// audit120's M3b on #2062): its branches are aborted (in the background, on their own
    /// budget), and `err` becomes the session's recorded outcome, so a retry finds the
    /// transaction ended rather than applying those rows again. Returns `err`.
    pub(crate) fn end_spanning(
        mut g: super::sessions::TxnGuard,
        err: ApiError,
        driver: &super::cluster::spanning::Driver,
    ) -> ApiError {
        use super::cluster::spanning::SpanView;
        if let Some(span) = g.span() {
            let mut branches = span.branches(&driver.home);
            branches.extend(span.idle(&driver.home));
            let d = driver.for_round();
            tokio::spawn(async move { d.abort(&branches).await });
        }
        let (txn, _, ticket) = g.commit();
        drop(txn);
        ticket.finish(Err(err.clone()));
        err
    }

    /// How long after its begin an absent decision record of a spanning transaction is
    /// presumed aborted (D329 2b): `--txn-outcome-retention`. A committed record can be
    /// collected from its commit time plus retention, which may be just after the begin, so
    /// from begin plus retention on an absent record may be a collected commit: not found,
    /// never presumed aborted (audit120's M1 on #2076). The startup floor (retention at least
    /// the lifetime, the request timeout and a minute) puts a transaction's whole life
    /// inside this window, with the minute for clock skew.
    pub(crate) fn outcome_horizon(&self) -> std::time::Duration {
        self.limits.txn_outcome_retention
    }

    /// `--txn-outcome-retention`.
    pub(crate) fn outcome_retention(&self) -> std::time::Duration {
        self.limits.txn_outcome_retention
    }

    /// Records that branches `acked` applied spanning transaction `xid`'s commit (D329 2b), in
    /// its decision row, for its GC. A row that's gone, or isn't a commit, is left as it is.
    pub(crate) async fn ack_decision(&self, xid: &str, acked: &[String]) -> Result<(), ApiError> {
        use super::cluster::spanning::{DECISION_FAMILY, DECISION_QUALIFIER, with_acks};
        if acked.is_empty() {
            return Ok(());
        }
        let table = self.decisions_table().await?;
        let row = xid.as_bytes().to_vec();
        for _ in 0..3 {
            let mut txn = self.part()?.db.transaction()?;
            let Some(c) = txn
                .get_async(&table, &row, DECISION_FAMILY, DECISION_QUALIFIER)
                .await?
            else {
                return Ok(());
            };
            let Some(value) = with_acks(c.value(), acked) else {
                return Ok(());
            };
            txn.put(&table, &row, DECISION_FAMILY, DECISION_QUALIFIER, &value);
            let held = match &self.gate {
                Some(gate) => Some(gate.enter()?),
                None => None,
            };
            let done = txn.commit_async().await.map_err(ApiError::from);
            drop(held);
            match done {
                Ok(_) => return Ok(()),
                Err(e) if e.code == "Conflict" => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Deletes this home's decision rows past `--txn-outcome-retention` at `now_ms` (D329
    /// 2b): commits every branch acknowledged, and abort markers. Returns how many went.
    pub(crate) async fn sweep_decisions(&self, now_ms: u64) -> Result<usize, ApiError> {
        use super::cluster::spanning::{DECISION_FAMILY, DECISION_QUALIFIER, collectable};
        let table = self.decisions_table().await?;
        let retention = u64::try_from(self.outcome_retention().as_millis()).unwrap_or(u64::MAX);
        let gone: Vec<Vec<u8>> = {
            let table = table.clone();
            self.blocking(move || {
                let mut gone = Vec::new();
                for row in table.scan_prefix(b"").iter()? {
                    let row = row?;
                    let Some(cell) = row.get(DECISION_FAMILY, DECISION_QUALIFIER) else {
                        continue;
                    };
                    if collectable(cell.value(), now_ms, retention) {
                        gone.push(row.key().to_vec());
                    }
                }
                Ok(gone)
            })
            .await?
        };
        let held = match &self.gate {
            Some(gate) => Some(gate.enter()?),
            None => None,
        };
        let mut n = 0;
        for key in &gone {
            table.mutate(key).delete_row().commit()?;
            n += 1;
        }
        drop(held);
        Ok(n)
    }

    /// What transaction `id` is now, without taking it (D329's `Outcome` and resolver), as
    /// `principal` sees it; `None` is the node's own call ([`super::sessions::Sessions::peek`]).
    pub(crate) fn peek_txn(
        &self,
        id: &str,
        principal: Option<super::auth::Principal>,
    ) -> super::sessions::Peek {
        self.sessions.peek(id, principal)
    }

    /// `--txn-lifetime-ms`.
    pub(crate) fn txn_lifetime(&self) -> std::time::Duration {
        self.sessions.txn_lifetime()
    }

    /// Spanning transaction `xid`'s decision row on this home partition (D329), read now:
    /// `None` when it has none.
    pub(crate) async fn decision(&self, xid: &str) -> Result<Option<Vec<u8>>, ApiError> {
        use super::cluster::spanning::{DECISION_FAMILY, DECISION_QUALIFIER};
        let table = self.decisions_table().await?;
        let row = xid.as_bytes().to_vec();
        self.blocking(move || {
            Ok(table
                .get(&row, DECISION_FAMILY, DECISION_QUALIFIER)?
                .map(|c| c.value().to_vec()))
        })
        .await
    }

    /// Makes spanning transaction `xid`'s outcome final as aborted, if nothing decided it
    /// (D329: the resolver's and `Outcome`'s absent row, with the home's history unchanged):
    /// an OCC transaction that reads the row and, absent, writes the abort marker through
    /// the gate at `GroupSync`, so the commit point (which reads the row absent and writes
    /// `COMMITTED`) and this can't both win. Returns the row as it is now: the marker, or
    /// what a commit point that won the race wrote.
    pub(crate) async fn decide_absent(&self, xid: &str, epoch: u64) -> Result<Vec<u8>, ApiError> {
        use super::cluster::spanning::{DECISION_FAMILY, DECISION_QUALIFIER, aborted_value};
        let table = self.decisions_table().await?;
        let row = xid.as_bytes().to_vec();
        for _ in 0..3 {
            let mut txn = self.part()?.db.transaction()?;
            if let Some(c) = txn
                .get_async(&table, &row, DECISION_FAMILY, DECISION_QUALIFIER)
                .await?
            {
                return Ok(c.value().to_vec());
            }
            let marker = aborted_value(epoch);
            txn.put(&table, &row, DECISION_FAMILY, DECISION_QUALIFIER, &marker);
            let held = match &self.gate {
                Some(gate) => Some(gate.enter()?),
                None => None,
            };
            let committed = txn
                .commit_with_async(pigeonhole::Durability::GroupSync)
                .await
                .map_err(ApiError::from)
                .and_then(|c| match &held {
                    Some(h) => h.committed().map(|()| c),
                    None => Ok(c),
                });
            drop(held);
            match committed {
                Ok(_) => return Ok(marker),
                // The commit point (or another resolver) wrote it meanwhile: read again.
                Err(e) if e.code == "Conflict" => continue,
                Err(e) => return Err(e),
            }
        }
        Err(ApiError::unavailable(
            "the transaction's decision row kept changing: ask again",
        ))
    }

    /// Takes transaction `id` for `caller`'s call (D329's coordinator: a spanning
    /// transaction's home session, with its span).
    pub(crate) fn take_txn(&self, id: &str, caller: &Caller) -> Result<Taken, ApiError> {
        self.sessions.take(id, caller.principal)
    }

    /// Opens spanning transaction `xid`'s branch on this partition (D329), at its snapshot
    /// now, for the client the home forwarded; with `renew`, renews the open one.
    pub(crate) fn begin_branch(
        &self,
        xid: &str,
        caller: &Caller,
        renew: bool,
    ) -> Result<(), ApiError> {
        if let Some(gate) = &self.gate {
            gate.serving()?;
        }
        self.sessions.begin_branch(
            caller.principal,
            || Ok(self.part()?.db.transaction()?),
            xid,
            renew,
        )
    }

    /// Prepares branch `xid` (D329) for its home's commit. A writing transaction's branch
    /// prepares durably, at `GroupSync`, through the gate (a write: its reads and writes
    /// held until its decide). A read-only one holds its reads in memory, under the lease
    /// term it was taken in, kept in `spanning` for the release (for twice the request
    /// timeout at most: its release round is one call).
    pub(crate) async fn prepare_branch(
        &self,
        xid: &str,
        read_only: bool,
        spanning: &Arc<super::cluster::spanning::Spanning>,
        partition: &str,
    ) -> Result<proto::BranchPrepared, ApiError> {
        let gate = self
            .gate
            .as_ref()
            .ok_or_else(|| ApiError::internal("a branch off a cluster partition"))?;
        let g = match self.sessions.take_branch(xid)? {
            Taken::Open(g) => g,
            Taken::Done(outcome) => return Err(ApiError::transaction_finished(&outcome)),
        };
        // A prepared branch's tombstone: its outcome is its home's to give.
        let prepared_mark = || ApiError::invalid("the branch is prepared: its home decides it");
        if read_only {
            let term = gate.serving_term()?;
            let (txn, _, ticket) = g.commit();
            let held = self
                .blocking(move || txn.hold_reads().map_err(ApiError::from))
                .await;
            // Taken, and the lease ran on unbroken: the hold counts from now.
            let held = held.and_then(|h| match gate.serving_term() {
                Ok(t) if t == term => Ok(h),
                _ => Err(lost_hold(partition)),
            });
            let ttl = self.request_timeout() * 2;
            let prepared = held.map(|h| proto::BranchPrepared {
                hold: spanning.keep_hold(h, partition, term, ttl),
                term,
            });
            ticket.finish(Err(prepared
                .as_ref()
                .err()
                .cloned()
                .unwrap_or_else(prepared_mark)));
            return prepared;
        }
        let held = gate.enter()?;
        let (txn, _, ticket) = g.commit();
        let xid_bytes = xid.as_bytes().to_vec();
        let prepared = self
            .blocking(move || {
                txn.prepare(&xid_bytes, pigeonhole::Durability::GroupSync)
                    .map_err(ApiError::from)
            })
            .await
            .and_then(|c| held.committed().map(|()| c));
        drop(held);
        ticket.finish(Err(prepared
            .as_ref()
            .err()
            .cloned()
            .unwrap_or_else(prepared_mark)));
        prepared.map(|_| proto::BranchPrepared { hold: 0, term: 0 })
    }

    /// Decides branch `xid` (D329). With `hold`, a read-only branch's release: whether its
    /// hold lasted, under lease term `term` (`Conflict` if not). Otherwise its decide through
    /// the gate, synced (the prepare's `GroupSync`) before it answers: the commit's info, or
    /// `None` for an abort or a share no longer in doubt. An abort also ends a branch that
    /// never prepared.
    pub(crate) async fn decide_branch(
        &self,
        xid: &str,
        commit: bool,
        hold: u64,
        term: u64,
        spanning: &super::cluster::spanning::Spanning,
        partition: &str,
    ) -> Result<Option<pigeonhole::CommitInfo>, ApiError> {
        let gate = self
            .gate
            .as_ref()
            .ok_or_else(|| ApiError::internal("a branch off a cluster partition"))?;
        if hold != 0 {
            let Some((h, taken)) = spanning.take_hold(hold, partition) else {
                return Err(lost_hold(partition));
            };
            let lasted = self
                .blocking(move || h.release().map_err(ApiError::from))
                .await?;
            return match gate.serving_term() {
                Ok(t) if lasted && t == term && taken == term => Ok(None),
                _ => Err(lost_hold(partition)),
            };
        }
        if !commit && let Ok(Taken::Open(g)) = self.sessions.take_branch(xid) {
            g.abort();
        }
        let held = gate.enter()?;
        let db = self.part()?.db.clone();
        let xid_bytes = xid.as_bytes().to_vec();
        let decided = self
            .blocking(move || db.decide(&xid_bytes, commit).map_err(ApiError::from))
            .await;
        let decided = decided.and_then(|c| held.committed().map(|()| c));
        drop(held);
        decided
    }

    /// The home's commit of spanning transaction `r.txn_id` (D329), its session `g` already
    /// taken by the coordinator (its tombstone answered a repeated `Commit` there), with
    /// `driver` reaching its branches: prepares, the commit point (the decision row in the
    /// home's own commit) and the decides when anything was written anywhere; holds and
    /// their release round when nothing was.
    ///
    /// Until the session turns to `Committing`, a refusal puts it back as it was, its span
    /// and branches untouched, so a retried `Commit` commits the whole transaction (audit120's
    /// M1 on #2062). From then the round runs detached, in its own task, as
    /// [`commit`](Self::commit)'s does: a client that goes away never leaves branches
    /// prepared past the commit point undecided by this node.
    pub(crate) async fn commit_spanning(
        self: &Arc<Self>,
        mut g: super::sessions::TxnGuard,
        r: proto::TxnCommitRequest,
        caller: &Caller,
        budget: OwnedSemaphorePermit,
        driver: super::cluster::spanning::Driver,
        forwarded: bool,
    ) -> Result<proto::TxnCommitResponse, ApiError> {
        use super::cluster::spanning::SpanView;
        let gate = Arc::clone(
            self.gate
                .as_ref()
                .ok_or_else(|| ApiError::internal("a spanning commit off a cluster"))?,
        );
        let home = driver.home.to_string();
        let span = g
            .span()
            .cloned()
            .ok_or_else(|| ApiError::internal("a spanning id on a plain session"))?;
        let writes = span.wrote() || !r.rows.is_empty() || g.wrote();
        // The coordinator checked these before the commit's rows went to branches; if one
        // fails now anyway (a table dropped in between), a commit that sent rows ends the
        // transaction rather than leave them applied for a retry (audit120's M3b).
        let refuse = |g, e: ApiError| {
            if forwarded {
                Err(Self::end_spanning(g, e, &driver))
            } else {
                Err(e)
            }
        };
        if writes && !caller.scope.allows(Scope::Write) {
            return refuse(g, ApiError::permission_denied("commit writes"));
        }
        let rows = match self.txn_rows(&r.rows).await {
            Ok(rows) => rows,
            Err(e) => return refuse(g, e),
        };
        let decisions = if writes {
            match self.decisions_table().await {
                Ok(t) => Some(t),
                Err(e) => return refuse(g, e),
            }
        } else {
            None
        };
        for (table, row, ops) in &rows {
            convert::txn_apply(g.txn(), table, row, ops);
        }
        drop(rows);
        let branches = span.branches(&home);
        let idle = span.idle(&home);
        let home_read = span.read(&home);
        let (txn, durability, ticket) = g.commit();
        let me = Arc::clone(self);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let metrics = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            // Branches opened that did nothing aren't prepared (they'd refuse): their
            // sessions end (audit120's S3), beside the round, not before it (a silent node
            // there mustn't spend the client's deadline).
            if !idle.is_empty() {
                let d = driver.for_round();
                tokio::spawn(async move { d.abort(&idle).await });
            }
            let outcome = match decisions {
                Some(decisions) => {
                    me.spanning_write(txn, durability, &decisions, &gate, &driver, &branches)
                        .await
                }
                None => {
                    me.spanning_read(txn, home_read, &gate, &driver, &branches)
                        .await
                }
            };
            ticket.finish(outcome.clone());
            drop(budget);
            // The branches its decide round applied, recorded for the decision row's GC,
            // after the reply (D329 2b).
            let applied: Vec<String> = match &outcome {
                Ok(r) => r
                    .parts
                    .iter()
                    .skip(1)
                    .filter(|p| p.applied)
                    .map(|p| p.partition.clone())
                    .collect(),
                Err(_) => Vec::new(),
            };
            if tx.send(outcome).is_err() {
                metrics.replies_lost.fetch_add(1, Ordering::Relaxed);
            }
            if !applied.is_empty()
                && let Err(e) = me.ack_decision(&driver.xid, &applied).await
            {
                eprintln!(
                    "phdb serve: recording a transaction's branch acks: {}",
                    e.message
                );
            }
        });
        rx.await.unwrap_or_else(|_| {
            Err(ApiError::outcome_unknown(
                "the commit's task ended without an outcome",
            ))
        })
    }

    /// The home's decision table (D329). Created through the gate if it's missing (a write
    /// to the file: never past a fence's drain), the first time a spanning transaction
    /// begins on the partition.
    pub(crate) async fn decisions_table(&self) -> Result<Table, ApiError> {
        use super::cluster::spanning::{DECISION_FAMILY, DECISIONS};
        let db = self.part()?.db.clone();
        if let Ok(t) = db.table(DECISIONS).and_then(|t| t.open()) {
            return Ok(t);
        }
        let held = match &self.gate {
            Some(gate) => Some(gate.enter()?),
            None => None,
        };
        let table = self
            .blocking(move || {
                Ok(db
                    .table(DECISIONS)?
                    .family(DECISION_FAMILY, pigeonhole::Family::default())
                    .create_if_missing()?)
            })
            .await;
        drop(held);
        table
    }

    /// A spanning commit that wrote: every branch prepares, then the commit point, then the
    /// decides. Aborts and decides run on a budget of their own ([`Driver::for_round`]),
    /// since the prepare round may have used the client's (audit120's S1); the answer waits
    /// for the decides only until the client's deadline.
    ///
    /// [`Driver::for_round`]: super::cluster::spanning::Driver::for_round
    async fn spanning_write(
        &self,
        mut txn: pigeonhole::Transaction,
        durability: Option<pigeonhole::Durability>,
        decisions: &Table,
        gate: &Arc<super::gate::WriteGate>,
        driver: &super::cluster::spanning::Driver,
        branches: &[String],
    ) -> super::sessions::Outcome {
        use super::cluster::spanning::{DECISION_FAMILY, DECISION_QUALIFIER, committed_value};
        let conflict = |why: String| ApiError::from_code(pigeonhole::ErrorCode::Conflict, &why);
        let abort = || async { driver.for_round().abort(branches).await };
        if let Err(e) = driver.check_feature().await {
            abort().await;
            return Err(e);
        }
        let (_, refused) = driver.prepare(branches, false).await;
        if let Some(e) = refused {
            abort().await;
            // Never committed: the commit point wasn't reached, so any refusal, a timeout
            // too, is safe to retry.
            return Err(conflict(format!(
                "a branch refused its prepare ({}): {}",
                e.code, e.message
            )));
        }
        #[cfg(test)]
        {
            use super::cluster::spanning::seams;
            if seams::at(&driver.xid, seams::Step::CommitPoint).await {
                return Err(ApiError::outcome_unknown(
                    "abandoned before the commit point",
                ));
            }
        }
        // The commit point: the decision row read (absent: no abort marker) and written in
        // the home's own commit, through its gate, at GroupSync or stronger.
        let epoch = super::sessions::spanning_parts(&driver.xid).map_or(0, |(_, e, _)| e);
        let row = driver.xid.as_bytes().to_vec();
        let marker = match txn
            .get_async(decisions, &row, DECISION_FAMILY, DECISION_QUALIFIER)
            .await
        {
            Ok(m) => m,
            Err(e) => {
                abort().await;
                return Err(e.into());
            }
        };
        if marker.is_some() {
            abort().await;
            return Err(conflict(
                "the transaction was decided without its home (an abort marker)".into(),
            ));
        }
        txn.put(
            decisions,
            &row,
            DECISION_FAMILY,
            DECISION_QUALIFIER,
            &committed_value(epoch, branches),
        );
        // Its records and feed events carry the xid, as every branch's share does (D329 2b):
        // a subscriber groups the commit's shares by it. A home-only commit too (F3).
        txn.tag_xid(driver.xid.as_bytes());
        let held = match gate.enter() {
            Ok(h) => h,
            Err(e) => {
                abort().await;
                return Err(e);
            }
        };
        let d = durability
            .unwrap_or(pigeonhole::Durability::GroupSync)
            .max(pigeonhole::Durability::GroupSync);
        let committed = match tokio::spawn(txn.commit_with_async(d)).await {
            Ok(r) => r.map_err(ApiError::from),
            Err(e) => Err(ApiError::outcome_unknown(format!(
                "the commit's task failed: {e}"
            ))),
        }
        .and_then(|c| held.committed().map(|()| c));
        drop(held);
        let info = match committed {
            Ok(c) => c,
            // Unknown: the branches stay in doubt, and the resolver settles them from the
            // decision row (D329 PR 2b).
            Err(e) if e.code == "OutcomeUnknown" => return Err(e),
            Err(e) => {
                abort().await;
                return Err(e);
            }
        };
        #[cfg(test)]
        {
            use super::cluster::spanning::seams;
            if seams::at(&driver.xid, seams::Step::Decide).await {
                return Err(ApiError::outcome_unknown("abandoned before the decides"));
            }
        }
        let home = proto::TxnPart {
            partition: driver.home.to_string(),
            seqno: info.seqno,
            applied: true,
        };
        // The decides on their own budget, in a task of their own: they go on to every
        // branch whatever the client's deadline; the answer waits for them until then.
        let decides = {
            let (d, b) = (driver.for_round(), branches.to_vec());
            tokio::spawn(async move { d.decide(&b, true).await })
        };
        let decided = tokio::time::timeout_at(driver.deadline, decides).await;
        let mut parts = vec![home];
        match decided {
            Ok(Ok(decided)) => parts.extend(decided),
            // Not all answered in time (or the round's task failed): each is committed, and
            // its resolver applies it if this round doesn't.
            _ => parts.extend(branches.iter().map(|p| proto::TxnPart {
                partition: p.clone(),
                seqno: 0,
                applied: false,
            })),
        }
        Ok(proto::TxnCommitResponse {
            commit: Some(convert::commit_info(info)),
            parts,
        })
    }

    /// A spanning commit that wrote nothing: every partition that read holds its reads, the
    /// home too, then a release round confirms that every hold lasted (`Conflict` if not).
    async fn spanning_read(
        &self,
        txn: pigeonhole::Transaction,
        home_read: bool,
        gate: &Arc<super::gate::WriteGate>,
        driver: &super::cluster::spanning::Driver,
        branches: &[String],
    ) -> super::sessions::Outcome {
        let seqno = txn.snapshot_seqno();
        let lost = || {
            ApiError::from_code(
                pigeonhole::ErrorCode::Conflict,
                "a read of the transaction changed or lost its hold before the commit (a \
                 partition's lease lapsed, it moved or it reopened): retry it",
            )
        };
        // The home's own hold first: a branch's taken after it overlaps it. A home whose
        // read call failed holds nothing (`InvalidArgument`: no reads).
        let home = if home_read {
            let term = gate.serving_term()?;
            match self
                .blocking(move || txn.hold_reads().map_err(ApiError::from))
                .await
            {
                Ok(hold) => Some((hold, term)),
                Err(e) if e.code == "InvalidArgument" => None,
                Err(e) => return Err(e),
            }
        } else {
            drop(txn);
            None
        };
        let (held, refused) = driver.prepare(branches, true).await;
        #[cfg(test)]
        {
            use super::cluster::spanning::seams;
            if seams::at(&driver.xid, seams::Step::Release).await {
                // Abandoned: the branches' holds are never released (they expire).
                return Err(lost());
            }
        }
        let round = driver.for_round();
        let lasted = refused.is_none() && round.release(&held).await;
        let home_lasted = match home {
            Some((hold, term)) => {
                let lasted = self
                    .blocking(move || hold.release().map_err(ApiError::from))
                    .await
                    .unwrap_or(false);
                lasted && gate.serving_term().is_ok_and(|t| t == term)
            }
            None => true,
        };
        if refused.is_some() {
            // Release whatever was held; the answer is the refusal's.
            let _ = round.release(&held).await;
        }
        if lasted && home_lasted {
            Ok(proto::TxnCommitResponse {
                commit: Some(proto::CommitInfo {
                    seqno,
                    durability: 0,
                }),
                parts: Vec::new(),
            })
        } else {
            Err(lost())
        }
    }

    fn open_snapshot(
        &self,
        r: &proto::OpenSnapshotRequest,
        caller: &Caller,
    ) -> Result<proto::OpenSnapshotResponse, ApiError> {
        let snapshot = if r.durable {
            self.part()?.db.durable_snapshot()?
        } else {
            self.part()?.db.snapshot()?
        };
        let seqno = snapshot.seqno();
        let snapshot_id = self.sessions.open_snapshot(caller.principal, snapshot)?;
        Ok(proto::OpenSnapshotResponse { snapshot_id, seqno })
    }

}

/// A read-only spanning transaction's hold on `partition` lost (D329): released, the
/// partition's lease lapsed or changed term, it moved, or this node restarted.
fn lost_hold(partition: &str) -> ApiError {
    ApiError::from_code(
        pigeonhole::ErrorCode::Conflict,
        &format!(
            "the transaction's hold of its reads on partition {partition:?} didn't last to \
             its commit: retry it"
        ),
    )
}
