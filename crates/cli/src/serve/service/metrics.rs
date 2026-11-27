use super::*;

pub(crate) const RPCS: [&str; 55] = [
    "Data.Get",
    "Data.MultiGet",
    "Data.ReadRow",
    "Data.Mutate",
    "Data.CheckAndMutate",
    "Data.Batch",
    "Data.MutateReturning",
    "Data.Scan",
    "Transactions.Begin",
    "Transactions.Get",
    "Transactions.GetRow",
    "Transactions.Write",
    "Transactions.Commit",
    "Transactions.Abort",
    "Snapshots.Open",
    "Snapshots.Release",
    "Feed.Subscribe",
    "Schema.ListTables",
    "Schema.CreateTable",
    "Schema.DropTable",
    "Schema.DescribeTable",
    "Admin.Info",
    "Admin.Flush",
    "Admin.Compact",
    "Admin.Backup",
    "Admin.BackupDownload",
    "Admin.Metrics",
    "Graph.Traverse",
    "Vectors.Nearest",
    "Vectors.CreateIndex",
    "Vectors.Insert",
    "Vectors.Delete",
    "Vectors.Search",
    "Vectors.Verify",
    "Vectors.BulkLoad",
    "Admin.Promote",
    "Replication.Follow",
    "Text.CreateIndex",
    "Text.Put",
    "Text.Delete",
    "Text.Search",
    "Text.Rebuild",
    "Text.Verify",
    "Memory.Remember",
    "Memory.Recall",
    "Memory.MarkUsed",
    "Memory.Supersede",
    "Memory.Link",
    "Memory.Forget",
    "Memory.Watch",
    // A spanning recall's steps (#1915): served for another node's recall, never a
    // client's call.
    "Memory.RecallPool",
    "Memory.RecallBodies",
    // A text search's rounds across an index's partitions (#1722 PR 2b): served for
    // another node's search, never a client's call.
    "Text.Stats",
    "Text.SearchPart",
    "Transactions.Outcome",
];

/// The outcomes a request is counted under: `ok`, every code ICR 0112's table names, and
/// `other` for a code a newer engine adds.
pub(crate) const OUTCOMES: [&str; 50] = [
    "ok",
    "Io",
    "Corruption",
    "WriterLocked",
    "ShmVersionMismatch",
    "ShmUnavailable",
    "UnsupportedFormat",
    "NetworkFilesystem",
    "TableNotFound",
    "TableExists",
    "FamilyNotFound",
    "FamilyExists",
    "UnknownMergeOperator",
    "MergeFailed",
    "Conflict",
    "ReadOnly",
    "KeyTooLarge",
    "ValueTooLarge",
    "NoSpace",
    "InvalidArgument",
    "Unsupported",
    "Closed",
    "NoReaderSlot",
    "RecordTooLarge",
    "Busy",
    "SnapshotExpired",
    "WouldDeadlock",
    "BatchTooLarge",
    "OutcomeUnknown",
    "NoWriter",
    "UnsupportedFeature",
    "FeedExpired",
    "Unauthenticated",
    "PermissionDenied",
    "NotFound",
    "Overloaded",
    "RequestTooLarge",
    "DeadlineExceeded",
    "SessionBusy",
    "TransactionExpired",
    "TransactionFinished",
    "FailedPrecondition",
    "ReplicaBehind",
    "Unavailable",
    "Internal",
    // A cluster node's (D317, ICR 0117), counted apart so an operator sees them (cli692's
    // F1 on ticket 2).
    "WrongPartition",
    "CrossPartition",
    "PartitionUnavailable",
    "Fenced",
    "other",
];

/// The duration histogram's bucket bounds, in microseconds: 100 µs to 10 s.
const BUCKETS_US: [u64; 16] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 5_000_000, 10_000_000,
];

/// Request counts by RPC and outcome, each RPC's durations, sheds, and the replies lost
/// after a write's outcome was known. Fixed tables of atomics: a few relaxed adds per
/// request, no lock and no allocation (perf287's F3 on #1501). Ticket 9 moves these onto
/// D316's names.
#[derive(Debug)]
pub(crate) struct Metrics {
    requests: [[AtomicU64; OUTCOMES.len()]; RPCS.len()],
    /// Per RPC: the count in each bucket (not cumulative; the last is past the last bound).
    durations: [[AtomicU64; BUCKETS_US.len() + 1]; RPCS.len()],
    /// Per RPC: the durations' sum, in microseconds.
    duration_sum_us: [AtomicU64; RPCS.len()],
    pub(crate) shed: AtomicU64,
    pub(crate) replies_lost: AtomicU64,
    /// Feed subscriptions ended at `--stream-idle-ms`: a client that left an event untaken
    /// that long, on a single node or a cluster (the merge task's take clock, #1798 (a)).
    /// A cluster subscription's parts aren't counted: the subscription is, once.
    pub(crate) feed_idle_drops: AtomicU64,
    /// Cluster subscriptions' merges running now, on the feed runtime (#1798): each cluster
    /// feed hub's, and each subscriber's own.
    pub(crate) cluster_merges: AtomicU64,
    /// Cluster subscriptions on a cluster feed hub now (#1798's B).
    pub(crate) cluster_hub_members: AtomicU64,
    /// Cluster subscriptions that fell off their hub's ring onto a merge of their own, and
    /// that rejoined it (#1798's B).
    pub(crate) cluster_fell_behind: AtomicU64,
    pub(crate) cluster_rejoined: AtomicU64,
    /// Cluster subscriptions that waited for a `--max-catch-up` slot after falling behind.
    pub(crate) cluster_catch_up_waits: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            requests: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            durations: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            duration_sum_us: std::array::from_fn(|_| AtomicU64::new(0)),
            shed: AtomicU64::new(0),
            replies_lost: AtomicU64::new(0),
            feed_idle_drops: AtomicU64::new(0),
            cluster_merges: AtomicU64::new(0),
            cluster_hub_members: AtomicU64::new(0),
            cluster_fell_behind: AtomicU64::new(0),
            cluster_rejoined: AtomicU64::new(0),
            cluster_catch_up_waits: AtomicU64::new(0),
        }
    }
}

impl Metrics {
    /// Counts one request of RPC `rpc` (`ServiceOp::index`) with outcome `code` (`None`:
    /// ok) that took `took`: from its decoded request to its reply, or to a stream's end.
    pub(crate) fn count(&self, rpc: usize, code: Option<&str>, took: Duration) {
        let outcome = match code {
            None => 0,
            Some(c) => OUTCOMES
                .iter()
                .position(|o| *o == c)
                .unwrap_or(OUTCOMES.len() - 1),
        };
        let Some(row) = self.requests.get(rpc) else {
            return;
        };
        row[outcome].fetch_add(1, Ordering::Relaxed);
        let us = u64::try_from(took.as_micros()).unwrap_or(u64::MAX);
        let bucket = BUCKETS_US.partition_point(|&b| b < us);
        self.durations[rpc][bucket].fetch_add(1, Ordering::Relaxed);
        self.duration_sum_us[rpc].fetch_add(us, Ordering::Relaxed);
    }

    /// Prometheus text: the server's counters. The engine's metrics, `async_sync_reads`
    /// (perf287's note 2 on D313) among them, are `Partition::engine_metrics`'.
    pub(crate) fn render(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        let _ = writeln!(out, "# TYPE pigeonhole_serve_requests_total counter");
        for (rpc, row) in RPCS.iter().zip(&self.requests) {
            for (outcome, n) in OUTCOMES.iter().zip(row) {
                let n = n.load(Ordering::Relaxed);
                if n > 0 {
                    let _ = writeln!(
                        out,
                        "pigeonhole_serve_requests_total{{rpc=\"{rpc}\",outcome=\"{outcome}\"}} {n}"
                    );
                }
            }
        }
        let _ = writeln!(
            out,
            "# TYPE pigeonhole_serve_request_duration_seconds histogram"
        );
        for (rpc, (buckets, sum)) in RPCS
            .iter()
            .zip(self.durations.iter().zip(&self.duration_sum_us))
        {
            let mut seen = 0;
            let mut lines = String::new();
            for (i, n) in buckets.iter().enumerate() {
                seen += n.load(Ordering::Relaxed);
                let le = BUCKETS_US
                    .get(i)
                    .map_or_else(|| "+Inf".to_owned(), |&b| (b as f64 / 1e6).to_string());
                let _ = writeln!(
                    lines,
                    "pigeonhole_serve_request_duration_seconds_bucket{{rpc=\"{rpc}\",le=\"{le}\"}} {seen}"
                );
            }
            if seen == 0 {
                continue;
            }
            out.push_str(&lines);
            let sum = sum.load(Ordering::Relaxed) as f64 / 1e6;
            let _ = writeln!(
                out,
                "pigeonhole_serve_request_duration_seconds_sum{{rpc=\"{rpc}\"}} {sum}"
            );
            let _ = writeln!(
                out,
                "pigeonhole_serve_request_duration_seconds_count{{rpc=\"{rpc}\"}} {seen}"
            );
        }
        let _ = writeln!(out, "# TYPE pigeonhole_serve_shed_total counter");
        let _ = writeln!(
            out,
            "pigeonhole_serve_shed_total {}",
            self.shed.load(Ordering::Relaxed)
        );
        let _ = writeln!(out, "# TYPE pigeonhole_serve_replies_lost_total counter");
        let _ = writeln!(
            out,
            "pigeonhole_serve_replies_lost_total {}",
            self.replies_lost.load(Ordering::Relaxed)
        );
        let _ = writeln!(out, "# TYPE pigeonhole_serve_feed_idle_drops_total counter");
        let _ = writeln!(
            out,
            "pigeonhole_serve_feed_idle_drops_total {}",
            self.feed_idle_drops.load(Ordering::Relaxed)
        );
        let _ = writeln!(out, "# TYPE pigeonhole_serve_feed_cluster_merges gauge");
        let _ = writeln!(
            out,
            "pigeonhole_serve_feed_cluster_merges {}",
            self.cluster_merges.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# TYPE pigeonhole_serve_feed_cluster_hub_members gauge"
        );
        let _ = writeln!(
            out,
            "pigeonhole_serve_feed_cluster_hub_members {}",
            self.cluster_hub_members.load(Ordering::Relaxed)
        );
        for (name, n) in [
            ("fell_behind", &self.cluster_fell_behind),
            ("rejoined", &self.cluster_rejoined),
            ("catch_up_waits", &self.cluster_catch_up_waits),
        ] {
            let _ = writeln!(
                out,
                "# TYPE pigeonhole_serve_feed_cluster_{name}_total counter"
            );
            let _ = writeln!(
                out,
                "pigeonhole_serve_feed_cluster_{name}_total {}",
                n.load(Ordering::Relaxed)
            );
        }
        out
    }
}

/// The session table's gauges (D313 §3): open sessions by kind, and the oldest open one's age,
/// since a held transaction or snapshot holds back compaction's version GC and blob GC. A
/// client that leaks sessions shows up here. Ticket 9 moves these onto D316's names.
pub(crate) fn render_sessions(out: &mut String, tables: &[&super::super::sessions::Sessions]) {
    use std::fmt::Write as _;
    // Every table on the node: a cluster node's partitions each have their own (D317 ticket
    // 8a), counted together as the caps count them (cli692's F1 on #1718).
    let now = Instant::now();
    let mut s = super::super::sessions::Stats::default();
    for t in tables {
        let one = t.stats(now);
        s.transactions += one.transactions;
        s.snapshots += one.snapshots;
        s.tombstones += one.tombstones;
        s.mcp += one.mcp;
        s.oldest = s.oldest.max(one.oldest);
    }
    let _ = writeln!(out, "# TYPE pigeonhole_serve_sessions gauge");
    for (kind, n) in [
        ("transaction", s.transactions),
        ("snapshot", s.snapshots),
        ("tombstone", s.tombstones),
        ("mcp", s.mcp),
    ] {
        let _ = writeln!(out, "pigeonhole_serve_sessions{{kind=\"{kind}\"}} {n}");
    }
    let _ = writeln!(out, "# TYPE pigeonhole_serve_oldest_session_seconds gauge");
    let _ = writeln!(
        out,
        "pigeonhole_serve_oldest_session_seconds {:.3}",
        s.oldest.as_secs_f64()
    );
}

/// `--embed-local`'s candle variables left unset at startup (#2061), one gauge per variable:
/// 1 while the server runs without it, as its startup warning said. Only with `--embed-local`.
pub(crate) fn render_candle(out: &mut String, bits: u8) {
    use std::fmt::Write as _;
    if bits & 0x80 == 0 {
        return;
    }
    let _ = writeln!(out, "# TYPE pigeonhole_serve_embed_env_unset gauge");
    for (i, var) in super::super::CANDLE_VARS.iter().enumerate() {
        let _ = writeln!(
            out,
            "pigeonhole_serve_embed_env_unset{{variable=\"{var}\"}} {}",
            (bits >> i) & 1
        );
    }
}

/// `Vectors.BulkLoad`'s gauges (perf287's B3 on 7b2): the running load's phase, and the
/// vectors and message bytes handed to it, an upper bound on what it holds.
fn render_bulk(out: &mut String, parts: &[(Option<&str>, &Partition)]) {
    for (name, read) in [
        (
            "pigeonhole_serve_bulk_load_phase",
            (|g: &super::super::bulk::Gauges| g.phase.load(Ordering::Relaxed)) as fn(&_) -> u64,
        ),
        ("pigeonhole_serve_bulk_load_vectors", |g| {
            g.vectors.load(Ordering::Relaxed)
        }),
        ("pigeonhole_serve_bulk_load_bytes", |g| {
            g.bytes.load(Ordering::Relaxed)
        }),
        ("pigeonhole_serve_bulk_load_reserved_bytes", |g| {
            g.reserved.load(Ordering::Relaxed)
        }),
    ] {
        render_family(
            out,
            name,
            "gauge",
            parts
                .iter()
                .map(|(id, part)| (*id, read(&part.bulk.gauges))),
        );
    }
}

/// `--text-pack`'s counters (#1585): each partition's background pack.
fn render_text_packs(out: &mut String, parts: &[(Option<&str>, &Partition)]) {
    type Read = fn(&super::super::texts::PackCounters) -> u64;
    let families: [(&str, Read); 4] = [
        ("pigeonhole_serve_text_pack_passes_total", |c| {
            c.passes.load(Ordering::Relaxed)
        }),
        ("pigeonhole_serve_text_pack_postings_total", |c| {
            c.postings.load(Ordering::Relaxed)
        }),
        ("pigeonhole_serve_text_pack_refused_total", |c| {
            c.refused.load(Ordering::Relaxed)
        }),
        ("pigeonhole_serve_text_pack_conflicts_total", |c| {
            c.conflicts.load(Ordering::Relaxed)
        }),
    ];
    for (name, read) in families {
        render_family(
            out,
            name,
            "counter",
            parts
                .iter()
                .map(|(id, part)| (*id, read(&part.texts.packs))),
        );
    }
}

/// A cluster node's partitions' unpinned engine threads (D317 ticket 8a's pinning check):
/// shard drivers and compaction threads apart (cli692's note on the 8a plan). A single
/// node's output doesn't change: they're labelled partitions' only.
fn render_pinning(out: &mut String, parts: &[(Option<&str>, &Partition)]) {
    let labelled = || parts.iter().filter(|(id, _)| id.is_some());
    render_family(
        out,
        "pigeonhole_serve_partition_unpinned_shard_threads",
        "gauge",
        labelled().map(|(id, p)| (*id, p.pinning.shards_unpinned() as u64)),
    );
    render_family(
        out,
        "pigeonhole_serve_partition_unpinned_compaction_threads",
        "gauge",
        labelled().map(|(id, p)| (*id, p.pinning.compaction_unpinned() as u64)),
    );
    // Each partition's engine memory as resolved (perf287's A3 on the 8a plan: 8b's gate
    // checks the totals).
    for (name, read) in [
        (
            "pigeonhole_serve_partition_memtable_bytes",
            (|m: &super::super::placement::Memory| m.memtables()) as fn(&_) -> u64,
        ),
        ("pigeonhole_serve_partition_block_cache_bytes", |m| {
            m.block_cache
        }),
        ("pigeonhole_serve_partition_row_cache_bytes", |m| {
            m.row_cache
        }),
    ] {
        render_family(
            out,
            name,
            "gauge",
            labelled().filter_map(|(id, p)| p.engine_memory().map(|m| (*id, read(&m)))),
        );
    }
}

/// `pigeonhole_cluster_txn_in_doubt_shares{partition}` and
/// `pigeonhole_cluster_txn_in_doubt_wal_bytes{partition}` (D329, ICR 0148): each of a cluster
/// node's partitions' shares of transactions across partitions held in doubt now, and the WAL
/// bytes they keep (what `--txn-max-in-doubt` and `--txn-max-in-doubt-wal-bytes` limit). The
/// same reading as `Status`'s: the engine's in-doubt registry, under its lock for an instant.
/// Nothing on a single node, which has no spanning transactions.
fn render_in_doubt(out: &mut String, parts: &[(Option<&str>, &Partition)]) {
    use std::fmt::Write as _;
    let rows: Vec<(&str, usize, u64)> = parts
        .iter()
        .filter_map(|(id, p)| {
            let id = (*id)?;
            let shares = p.db.in_doubt_shares();
            Some((id, shares.len(), shares.iter().map(|s| s.2).sum()))
        })
        .collect();
    if rows.is_empty() {
        return;
    }
    let _ = writeln!(
        out,
        "# HELP pigeonhole_cluster_txn_in_doubt_shares Shares of transactions across \
         partitions this partition holds in doubt now: prepared, their decision not yet \
         known here (D329).\n# TYPE pigeonhole_cluster_txn_in_doubt_shares gauge"
    );
    for (id, n, _) in &rows {
        let _ = writeln!(
            out,
            "pigeonhole_cluster_txn_in_doubt_shares{{partition=\"{id}\"}} {n}"
        );
    }
    let _ = writeln!(
        out,
        "# HELP pigeonhole_cluster_txn_in_doubt_wal_bytes The WAL bytes this partition's \
         in-doubt shares keep (D329).\n# TYPE pigeonhole_cluster_txn_in_doubt_wal_bytes gauge"
    );
    for (id, _, bytes) in &rows {
        let _ = writeln!(
            out,
            "pigeonhole_cluster_txn_in_doubt_wal_bytes{{partition=\"{id}\"}} {bytes}"
        );
    }
}

/// One serve metric's `# TYPE` once (the exposition format allows it once), then a sample
/// per partition: labelled `pigeonhole_partition` on a cluster node (D317 ticket 8b, as the
/// engine's `pigeonhole.partition`), unlabelled on a single node. No partitions, no lines.
fn render_family<'a>(
    out: &mut String,
    name: &str,
    kind: &str,
    samples: impl Iterator<Item = (Option<&'a str>, u64)>,
) {
    use std::fmt::Write as _;
    let mut samples = samples.peekable();
    if samples.peek().is_none() {
        return;
    }
    let _ = writeln!(out, "# TYPE {name} {kind}");
    for (id, v) in samples {
        match id {
            Some(id) => {
                let _ = write!(out, "{name}{{pigeonhole_partition=\"");
                escape_label(out, id);
                let _ = writeln!(out, "\"}} {v}");
            }
            None => {
                let _ = writeln!(out, "{name} {v}");
            }
        }
    }
}

/// `v` as a Prometheus label value: backslash, quote and newline escaped.
fn escape_label(out: &mut String, v: &str) {
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
}

/// The feed fan-out's gauges and counters (D313 R2; perf287's notes on ticket 5).
pub(crate) fn render_feeds(out: &mut String, parts: &[(Option<&str>, &Partition)]) {
    type Read = fn(&super::super::feed::Feeds) -> u64;
    let families: [(&str, &str, Read); 9] = [
        ("pigeonhole_serve_feed_hubs", "gauge", |f| {
            f.gauges().hubs as u64
        }),
        ("pigeonhole_serve_feed_ring_bytes", "gauge", |f| {
            f.gauges().ring_bytes as u64
        }),
        ("pigeonhole_serve_feed_catching_up", "gauge", |f| {
            f.gauges().catching_up as u64
        }),
        (
            "pigeonhole_serve_feed_catch_up_waits_total",
            "counter",
            |f| f.stats.catch_up_waits.load(Ordering::Relaxed),
        ),
        ("pigeonhole_serve_feed_fell_behind_total", "counter", |f| {
            f.stats.fell_behind.load(Ordering::Relaxed)
        }),
        ("pigeonhole_serve_feed_rejoined_total", "counter", |f| {
            f.stats.rejoined.load(Ordering::Relaxed)
        }),
        // #1689's fast path: hubs on the feed runtime beside their one client, and moves.
        ("pigeonhole_serve_feed_solo_hubs", "gauge", |f| {
            f.gauges().solo_hubs as u64
        }),
        ("pigeonhole_serve_feed_hub_moves_total", "counter", |f| {
            f.stats.hub_moves.load(Ordering::Relaxed)
        }),
        ("pigeonhole_serve_feed_hub_failures_total", "counter", |f| {
            f.stats.hub_failures.load(Ordering::Relaxed)
        }),
    ];
    for (name, kind, read) in families {
        render_family(
            out,
            name,
            kind,
            parts.iter().map(|(id, part)| (*id, read(&part.feeds))),
        );
    }
}

impl Service {
    /// The metrics as Prometheus text: `/metrics` and `Admin.Metrics` alike, for `parts`,
    /// the partitions served here. A single node passes its one, unlabelled; a cluster node
    /// passes the router's ([`Router::metrics_text`](super::super::router::Router::metrics_text)),
    /// each labelled by its id (D317 ticket 8b); a node that only routes, none.
    ///
    /// `sessions` are the node's session tables: its own, and each partition's on a cluster
    /// node (D317 ticket 8a).
    pub(crate) fn metrics_text(
        &self,
        parts: &[(Option<&str>, &Partition)],
        sessions: &[&super::super::sessions::Sessions],
    ) -> String {
        let mut text = self.metrics.render();
        self.connections.render(&mut text);
        self.replies.render(&mut text);
        render_sessions(&mut text, sessions);
        render_feeds(&mut text, parts);
        // The hub runtime's threads, the node's (#1719): every partition's hubs share it.
        if let Some(p) = &self.placement {
            use std::fmt::Write as _;
            let _ = writeln!(text, "# TYPE pigeonhole_serve_feed_hub_threads gauge");
            let _ = writeln!(text, "pigeonhole_serve_feed_hub_threads {}", p.hub_threads);
        }
        render_candle(
            &mut text,
            super::super::CANDLE_UNSET.load(Ordering::Relaxed),
        );
        render_bulk(&mut text, parts);
        if self.text_rebuilds.packs() {
            render_text_packs(&mut text, parts);
        }
        render_pinning(&mut text, parts);
        render_in_doubt(&mut text, parts);
        if let Some(replica) = &self.replica {
            replica.render(&mut text);
        }
        // Reads each database on this thread, at most once per 100 ms: `engine_metrics` and
        // `recovery_stats` (atomic loads), and `info`, which walks the current catalog, takes
        // the file's length and the pager's allocation stats under its lock for an instant,
        // with no other I/O (ai1249's question, perf287's nit 2 on #1687). Every partition's
        // under one `# HELP`/`# TYPE` per metric (ICR 0133).
        let collectors: Vec<_> = parts.iter().map(|(_, p)| &p.engine_metrics).collect();
        pigeonhole_otel::Collector::render_prometheus_all(&collectors, &mut text);
        text
    }

    /// A single node's partition for [`metrics_text`](Self::metrics_text), unlabelled.
    pub(crate) fn single_parts(&self) -> Vec<(Option<&str>, &Partition)> {
        self.part.iter().map(|p| (None, p)).collect()
    }
}
