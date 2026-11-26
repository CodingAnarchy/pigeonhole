use super::*;

/// How a partition's engine threads were placed when it opened (D317 ticket 8a, perf287's
/// P1): its shard driver threads and compaction threads, each pinned to a CPU of its own,
/// confined to the node's compaction CPUs (ICR 0134), or unpinned.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Pinning {
    pub shards: usize,
    pub shards_pinned: usize,
    pub compaction: usize,
    pub compaction_pinned: usize,
    pub compaction_confined: usize,
}

impl Pinning {
    /// `db`'s, opened with `compaction` compaction threads.
    pub(crate) fn of(db: &Pigeonhole, compaction: usize) -> Self {
        let m = db.engine_metrics();
        let started = usize::try_from(m.threads_started).unwrap_or(usize::MAX);
        Self {
            shards: started.saturating_sub(compaction),
            shards_pinned: db.pinned_shard_cpus().len(),
            compaction,
            compaction_pinned: db.pinned_compaction_cpus().len(),
            compaction_confined: usize::try_from(m.threads_confined).unwrap_or(usize::MAX),
        }
    }

    pub(crate) fn shards_unpinned(&self) -> usize {
        self.shards.saturating_sub(self.shards_pinned)
    }

    /// Compaction threads neither pinned nor confined: on any CPU, a shard's or the
    /// network's included.
    pub(crate) fn compaction_unpinned(&self) -> usize {
        self.compaction
            .saturating_sub(self.compaction_pinned + self.compaction_confined)
    }
}

/// What a node opens on the database it serves: the engine, the table cache, the indexes,
/// the feeds, the memory store and the bulk loads (D317 ticket 2, cli692's S1). The node's
/// own state (connections, budgets, pools, sessions, limits, metrics) stays on [`Service`].
pub(crate) struct Partition {
    pub(crate) db: Pigeonhole,
    /// Read on every call, changed only when a table is first used: a snapshot swapped
    /// whole, never a lock (perf287's note 3 on D313).
    pub(super) tables: ArcSwap<TableCache>,
    /// The feed fan-out's hubs (D313 R2).
    pub(crate) feeds: Arc<super::super::feed::Feeds>,
    /// How its engine's threads were placed when it opened (D317 ticket 8a's pinning
    /// check): a cluster node's gauges.
    pub(crate) pinning: Pinning,
    /// Its sampled load, for the cluster balancer (D332): request CPU and read keys.
    pub(crate) load: Arc<super::super::cluster::load::PartitionLoad>,
    /// Its map replicas' acknowledgements, on its owner (D335): `Replication.FollowAcked`
    /// streams record them, its writes are timed against them.
    pub(crate) acks: Arc<super::super::acks::ReplicaAcks>,
    /// Its engine memory (D317 ticket 8a's `--engine-memory`): as resolved at open, then as a
    /// node's re-split at another partition's open or close changed its caches (#1761). A
    /// cluster node's gauges, and the next re-split's starting point.
    engine_memory: std::sync::Mutex<Option<super::super::placement::Memory>>,
    /// `--backup-dir`, and the backup downloads kept for resumes.
    ///
    /// **The node's one** (simp724's N1 on #1711): `Backups::new` clears
    /// `.phdb-serve-downloads` at startup and counts `--max-backups` itself. A second one per
    /// partition (several partitions on one node, D317 8a; a partition opened at runtime,
    /// ticket 6) would delete another partition's live downloads and make the limit per
    /// partition. Partitions share the node's `Arc`.
    pub(super) backups: Arc<super::super::backup::Backups>,
    /// The vector indexes open, and their rebalancing threads.
    pub(crate) indexes: Arc<super::super::indexes::Indexes>,
    /// `Vectors.BulkLoad`'s one load at a time, on a thread of its own.
    pub(crate) bulk: super::super::bulk::BulkLoads,
    /// The text indexes open, and the running rebuild.
    pub(crate) texts: Arc<super::super::texts::Texts>,
    /// The memory store (`--memory`), if the server serves one: opened when a call needs
    /// it, and again after the catalog or the engine changes (#1618). Set once: at start on
    /// a single node, and on a cluster partition when the map first places the store on it
    /// (#1915), which it then does for good.
    pub(crate) memory: std::sync::OnceLock<Arc<super::super::memslot::MemSlot>>,
    /// Held across a table's create or drop and the cache update after it, so the cache ends
    /// at the live table whatever the order (simp724's C1 on #1527).
    ///
    /// **Lock order: a blocking-pool place first, then this.** A `/mcp` create holds its
    /// request's pool thread when it takes this, so an async create or drop takes its pool
    /// place before this, never while holding it (ai1249's B2 on #1666).
    pub(crate) schema: tokio::sync::Mutex<()>,
    /// The engine's metrics on `/metrics`, under D316's names (D323): the same reading and
    /// carrying as `pigeonhole-otel`'s OpenTelemetry instruments, so its counters stay
    /// monotonic across an automatic recovery and a replica's re-bootstrap. A node that only
    /// routes has none (D317 ticket 2).
    pub(super) engine_metrics: pigeonhole_otel::Collector,
    /// The catalog's generation: bumped by a create, a drop, and a replica's catalog
    /// change. A `/mcp` session that saw an older one checks its tool list (ai1249's M2 on
    /// D313: `create_table` in one session can bring `nearest` to another).
    pub(super) catalog: AtomicU64,
}

/// What a partition is opened with, beside its database: [`Service::new`]'s and
/// [`Service::for_partition`]'s, so each part of a [`Partition`] is built once.
pub(crate) struct PartitionConfig {
    /// `--backup-dir`'s backups: the node's one, which a cluster node's partitions share.
    pub(crate) backups: Arc<super::super::backup::Backups>,
    /// The CPUs the threads the server starts run on, off the shards': those of vector
    /// indexes (rebalancers, bulk loads: they run k-means, perf287's B2 on 7b2), the
    /// backups' and `Replication.Follow`'s (a replica catching up reads and encodes at full
    /// speed, cli692's F1 on #1583).
    pub(crate) placement: Option<Arc<super::super::placement::Placement>>,
    /// How its engine's threads were placed when it opened.
    pub(crate) pinning: Pinning,
    /// Its engine memory as resolved at open: a cluster node's partitions have one.
    pub(crate) engine_memory: Option<super::super::placement::Memory>,
}

impl PartitionConfig {
    /// A partition with `backups`, its threads not placed.
    pub(crate) fn new(backups: Arc<super::super::backup::Backups>) -> Self {
        Self {
            backups,
            placement: None,
            pinning: Pinning::default(),
            engine_memory: None,
        }
    }
}

impl Partition {
    /// Ends the threads that use the database, in order, before it closes: a bulk load's
    /// (its input has ended or was stopped, so it writes what came, and rebalances inline if
    /// its index does), a running text rebuild's (it finishes first, ai1249's S1 on #1587),
    /// then the vector indexes' rebalancers. One copy for a server's shutdown and a cluster
    /// partition's close.
    pub(crate) fn close_users(&self) {
        // D335: writes waiting for replica acks answer `OutcomeUnknown` now.
        self.acks.close();
        self.bulk.close();
        self.texts.close();
        self.indexes.close();
        // The memory store's handles go, and with them its background loops (#1915).
        if let Some(memory) = self.memory.get() {
            memory.clear();
        }
    }

    /// Its engine memory now (`--engine-memory`'s share), `None` without one.
    pub(crate) fn engine_memory(&self) -> Option<super::super::placement::Memory> {
        *self
            .engine_memory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Resizes its engine's block and row caches to `memory`'s, and records it (#1761: a
    /// node re-splitting `--engine-memory` as partitions open and close). The memtables stay
    /// as opened: `memory` carries them unchanged. Blocks while a shrink evicts: call it off
    /// async workers.
    pub(crate) fn resize_caches(&self, memory: super::super::placement::Memory) {
        let bytes = |b: u64| usize::try_from(b).unwrap_or(usize::MAX);
        self.db
            .set_cache_capacities(bytes(memory.block_cache), bytes(memory.row_cache));
        *self
            .engine_memory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(memory);
    }

    /// What a node opens on `db`, before any table is used: partition `id` of a cluster
    /// node, whose engine metrics carry it as `pigeonhole.partition` (D317 ticket 8b). Each
    /// part is built once, from `config` and the partition's write `gate`.
    pub(super) fn new(
        db: Pigeonhole,
        id: Option<&str>,
        limits: &Limits,
        feeds: &super::super::feed::FeedShared,
        gate: Option<&Arc<super::super::gate::WriteGate>>,
        config: PartitionConfig,
    ) -> Self {
        let metrics = match id {
            Some(id) => pigeonhole_otel::Config::new().attribute(PARTITION_ATTRIBUTE, id),
            None => pigeonhole_otel::Config::new(),
        };
        // The node's one `Backups` keeps the first placement it's given (a `OnceLock`): a
        // cluster node's partitions all pass the node's, so the later calls change nothing.
        if let Some(p) = &config.placement {
            config.backups.place(p);
        }
        let text_placement = config.placement.clone();
        Self {
            tables: ArcSwap::from_pointee(TableCache::default()),
            pinning: config.pinning,
            load: Arc::default(),
            acks: Arc::default(),
            engine_memory: std::sync::Mutex::new(config.engine_memory),
            backups: config.backups,
            indexes: Arc::new(super::super::indexes::Indexes::new(
                db.clone(),
                config.placement.clone(),
                gate.cloned(),
            )),
            bulk: super::super::bulk::BulkLoads::new(config.placement),
            texts: Arc::new(super::super::texts::Texts::new(
                db.clone(),
                gate.cloned(),
                text_placement,
            )),
            memory: std::sync::OnceLock::new(),
            schema: tokio::sync::Mutex::new(()),
            catalog: AtomicU64::new(0),
            engine_metrics: metrics.collector(&db),
            feeds: super::super::feed::Feeds::new(db.clone(), feed_limits(limits), feeds),
            db,
        }
    }
}
