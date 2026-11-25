use super::*;

/// `Cluster` calls a node runs at once: map reads and installs, pushes and pulls are few.
pub(super) const CLUSTER_CALLS: usize = 16;

/// `phdb serve --cluster DIR`.
pub(crate) fn run(
    cli: &crate::args::Cli,
    args: &ServeArgs,
    dir: &Path,
    stderr: &mut dyn std::io::Write,
) -> Result<ExitStatus, Failure> {
    // Per-file flags have no meaning across partitions until a ticket defines them
    // (cli692's M1).
    let refused = [
        (
            args.replica_of.is_some(),
            "--replica-of",
            "#1436's promotion path in D317 §6",
        ),
        (
            args.create,
            "--create",
            "D317's ticket 2 (partitions are created by phdb cluster init-partition)",
        ),
    ];
    if let Some((_, flag, ticket)) = refused.iter().find(|(set, _, _)| *set) {
        return Err(Failure::usage(&format!(
            "{flag} can't be used with --cluster yet: it comes with {ticket}"
        )));
    }
    let id = args.node_id.clone().unwrap_or_default();
    if !map::id_ok(&id) {
        return Err(Failure::usage(
            "--node-id is 1-64 of [a-z0-9_-], not starting with -",
        ));
    }
    if !dir.is_dir() {
        return Err(Failure::usage(&format!(
            "--cluster {}: not a directory",
            dir.display()
        )));
    }
    let tokens = super::super::tokens(args)?;
    let tls = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => {
            Some(tls::acceptor(cert, key).map_err(|e| Failure::usage(&format!("TLS: {e}")))?)
        }
        _ => None,
    };
    config::check_bind(
        args.listen,
        tls.is_some(),
        tokens.any(),
        args.insecure_no_tls_no_auth,
    )
    .map_err(|e| Failure::usage(&e))?;
    let token = match &args.cluster_token_file {
        Some(path) => Some(
            std::fs::read_to_string(path)
                .map_err(|e| Failure::io(&format!("read {}", path.display()), &e))?
                .trim()
                .to_owned(),
        ),
        None => std::env::var("PHDB_CLUSTER_TOKEN").ok(),
    }
    .filter(|t| !t.is_empty());
    let ca = match &args.cluster_ca {
        Some(path) => Some(tonic::transport::Certificate::from_pem(
            std::fs::read(path)
                .map_err(|e| Failure::io(&format!("read {}", path.display()), &e))?,
        )),
        None => None,
    };
    // A renewal round has to fit in a third of a lease, with a push timeout to spare for a
    // node that's down (D326).
    if args.cluster_lease_ms == 0
        || args.cluster_lease_ms < args.cluster_push_timeout_ms.max(1).saturating_mul(3)
    {
        return Err(Failure::usage(&format!(
            "--cluster-lease-ms ({}) must be at least 3 times --cluster-push-timeout-ms ({}): \
             a lease is renewed every third of it, and a renewal can wait a push timeout for \
             a node that's down",
            args.cluster_lease_ms, args.cluster_push_timeout_ms
        )));
    }
    let _lock = lock_dir(dir)?;
    // Without TLS the node dials its peers in the clear, with its cluster token: only on
    // loopback (cli692's R3), whether or not it requires tokens itself.
    let loopback_only = tls.is_none();
    let mut state = MapState::default();
    if let Some(m) = read_map(dir).map_err(|e| Failure::usage(&e))? {
        map::validate(&m, loopback_only)
            .map_err(|e| Failure::usage(&format!("{}: {e}", dir.join(MAP_FILE).display())))?;
        let json = map::canonical_json(&m);
        state.map = Some((map::canonical(&m), json));
    }
    let version = state.version();
    let voted = read_voted(dir).map_err(|e| Failure::usage(&format!("{}: {e}", dir.display())))?;
    let node = Node::new(
        dir,
        &id,
        loopback_only,
        tokens,
        Peers {
            token,
            scheme: if tls.is_some() { "https" } else { "http" },
            ca,
            push_timeout: Duration::from_millis(args.cluster_push_timeout_ms.max(1)),
            lease: Duration::from_millis(args.cluster_lease_ms),
            failover_after: Duration::from_millis(match args.cluster_failover_after_ms {
                0 => args.cluster_lease_ms.saturating_mul(2),
                ms => ms,
            }),
            failover_max_lag: (args.cluster_failover_max_lag_ms > 0)
                .then(|| Duration::from_millis(args.cluster_failover_max_lag_ms)),
            balance_window: Duration::from_secs(args.balance_window),
            load_interval: load::INTERVAL,
            prune_interval: super::range_prune::INTERVAL,
        },
        state,
        voted,
    );
    node.load_fences().map_err(|e| Failure::usage(&e))?;
    // The partitions the map gives this node, in id order, with their epochs: none before
    // a first map, or when the map gives none (the node only routes).
    let mine: Vec<(String, u64)> = {
        let routes = node.routes();
        match routes.as_ref() {
            None => Vec::new(),
            Some(r) => {
                let mut mine: Vec<(String, u64)> = r
                    .partitions_of(&id)
                    .map(|p| (p.to_owned(), r.owner(p).map_or(0, |o| o.epoch)))
                    .collect();
                mine.sort_unstable();
                mine
            }
        }
    };
    let cluster_id = node.state().map.as_ref().map(|(m, _)| m.cluster_id.clone());
    // The node's budget (D317 ticket 8a, §10, perf287's P1): `--shards` and
    // `--compaction-threads` are the node's totals, split across its partitions.
    let total_shards = args
        .shards
        .unwrap_or_else(super::super::placement::default_shards);
    let total_compaction = args
        .compaction_threads
        .unwrap_or_else(|| super::super::placement::default_compaction_threads(total_shards));
    let map_now = node.state().map.as_ref().map(|(m, _)| m.clone());
    let (shares, memories) = start_budget(
        map_now.as_ref(),
        &mine,
        total_shards,
        total_compaction,
        args.engine_memory,
    )?;
    // With several partitions, the node's compaction CPUs are reserved before any engine
    // opens: the CPUs right after all the partitions' shards in the pin order, and every
    // partition's compaction threads are confined to them (ICR 0134). Engines opened one
    // after another would otherwise pin shard, shard, compaction for each, interleaving the
    // shards' cores, so the network side's mask spanned both L3 domains of the AX102 and
    // its threads ran at 0.70 IPC against 0.91 with one partition (perf287's g75 on #1482).
    let reserved = (mine.len() > 1)
        .then(|| super::super::placement::compaction_reserve(&shares, total_compaction))
        .flatten();
    // What every partition's database takes, at the start and when one opens later.
    let served = super::super::served_options(cli, args);
    let opened = open_partitions(
        dir,
        cluster_id.as_deref(),
        &mine,
        false,
        Budget {
            shares: &shares,
            reserved: reserved.as_deref(),
            // Unset, the library's own defaults, exactly as before.
            memories: memories.as_deref(),
        },
        |share, memory| {
            let feed = args.change_feed.map(|t| {
                let all = shares.iter().map(|s| s.shards).sum();
                super::super::placement::feed_share(t, share.shards, all)
            });
            open::options_for(&served, share, memory, feed, false)
        },
        stderr,
    )?;
    if let Some(total) = args.change_feed {
        let parts: Vec<(&str, &pigeonhole::Pigeonhole)> =
            opened.iter().map(|o| (o.id.as_str(), &o.db)).collect();
        open::log_feed(total, &parts, "at start");
    }
    let dbs: Vec<&pigeonhole::Pigeonhole> = opened.iter().map(|o| &o.db).collect();
    let mut placement = if dbs.is_empty() {
        super::super::placement::Placement::routes_only(args.io_threads, args.blocking_threads)
    } else {
        super::super::placement::Placement::plan(
            &dbs,
            args.io_threads,
            args.blocking_threads,
            args.feed_threads,
            args.feed_hub_threads,
            args.affinity == super::super::config::Affinity::On,
        )
    };
    // Compaction threads confined to the node's reserved CPUs run there as pinned ones
    // would: the startup line counts those CPUs too (perf287's 8b run).
    if let Some(r) = reserved.as_deref()
        && opened.iter().any(|o| o.pinning.compaction_confined > 0)
    {
        placement.confined_compaction(r);
    }
    let placement = Arc::new(placement);
    let limits = args.limits();
    // D329: a decision record must outlive any transaction that could still ask for it
    // (audit120's S2 on the design).
    let floor =
        limits.leases.txn_lifetime + limits.request_timeout + std::time::Duration::from_secs(60);
    if limits.txn_outcome_retention < floor {
        return Err(Failure::usage(&format!(
            "--txn-outcome-retention {:?} is below --txn-lifetime-ms + the request timeout + 1 \
             minute ({floor:?}): a transaction's outcome must outlive its session",
            limits.txn_outcome_retention
        )));
    }
    let metrics = Arc::new(super::super::service::Metrics::default());
    // The node's service, which serves no partition itself: its budgets, pools, session
    // caps and metrics are every partition's (D317 ticket 8a), as is its one `Backups`
    // (simp724's N1 on #1711).
    let service = super::super::service::Service::routing_only(
        limits,
        placement.blocking_threads,
        Arc::clone(&metrics),
        Some(Arc::clone(&placement)),
    )
    .memory_opt(None, args.memory_calls.unwrap_or(0))
    .text_pack(args.text_pack);
    // `--memory` (#1915): each partition the map places the store on opens its own, with
    // this node's embedder and options, when a reconcile finds it placed. The map may place
    // it later; it can't place another store.
    let recipe = super::super::memory_recipe(args, &placement)?;
    if let (Some(r), Ok(map)) = (&recipe, node.get_map())
        && let Some(m) = &map.memory
        && m.store != r.name
    {
        return Err(Failure::usage(&format!(
            "--memory {}: the cluster map places memory store {:?}, not this one",
            r.name, m.store
        )));
    }
    let backups = super::super::backup::Backups::new(args.backup_dir.as_deref(), args.max_backups)
        .map_err(|e| Failure::usage(&e))?;
    let parts: Vec<(String, super::super::service::Service)> = opened
        .iter()
        .map(|o| -> Result<_, Failure> {
            // Fenced from the start (D317 ticket 6), until a peer's map confirms it.
            let epoch = mine.iter().find(|(p, _)| *p == o.id).map_or(0, |(_, e)| *e);
            let gate = node.new_gate(&o.id);
            // D331: its range gate, before it serves.
            node.arm_ranges(&gate, &o.id, epoch, &o.db, || ())
                .map_err(|e| Failure::usage(&format!("partition {}: {e}", o.id)))?;
            let config = super::super::service::PartitionConfig {
                backups: Arc::clone(&backups),
                placement: Some(Arc::clone(&placement)),
                pinning: o.pinning,
                engine_memory: Some(o.memory),
            };
            let part = service.for_partition(o.db.clone(), &o.id, gate, config);
            // A text index rebuild a fence or a shutdown stopped carries on once the
            // partition takes writes (#1756).
            part.resume_text_rebuilds();
            Ok((o.id.clone(), part))
        })
        .collect::<Result<_, _>>()?;
    let serving = if mine.is_empty() {
        format!(
            "no partition, routing only, as cluster node {id} of {}; map version {version}",
            dir.display()
        )
    } else {
        let list: Vec<String> = mine
            .iter()
            .map(
                |(p, epoch)| match (node.fence_of(p), node.awaiting_peers(p)) {
                    (Some(crate::serve::gate::Fence::Operator), true) => format!(
                        "{p} (epoch {epoch}, fenced: phdb cluster fence, and waiting for its \
                         lease)"
                    ),
                    (Some(crate::serve::gate::Fence::Operator), false) => {
                        format!("{p} (epoch {epoch}, fenced: phdb cluster fence)")
                    }
                    (Some(_), _) => {
                        format!("{p} (epoch {epoch}, waiting for its lease)")
                    }
                    (None, _) => format!("{p} (epoch {epoch})"),
                },
            )
            .collect();
        format!(
            "partition{} {} as cluster node {id} of {}; map version {version}",
            if mine.len() > 1 { "s" } else { "" },
            list.join(", "),
            dir.display()
        )
    };
    let shared = Arc::new(super::super::Shared {
        router: super::super::router::Router::cluster(service, Arc::clone(&node), parts)
            .max_uploads(args.max_uploads)
            .range_moves(
                args.move_rate_bytes,
                args.move_retention_cap,
                args.max_move_followers,
            )
            .memory_recipe(recipe),
        // The data calls' tokens: the same file the node's own calls read.
        tokens: super::super::tokens(args)?,
        limits,
        metrics,
        feed_runtimes: std::sync::OnceLock::new(),
    });
    // A partition a map gives the node while it serves opens as the start's do (D317
    // ticket 6), with its share of the same totals.
    // Its partitions' schema follows the map from here (D317 ticket 5): reconciled now, and
    // on every map it takes.
    open::attach(
        &node,
        open::Server {
            shared: Arc::downgrade(&shared),
            placement: Arc::clone(&placement),
            backups: Arc::clone(&backups),
            served,
            shards: total_shards,
            compaction: total_compaction,
            engine_memory: args.engine_memory,
            change_feed: args.change_feed,
            reserved: reserved.clone(),
            replica_run: args.replica_run.max(1),
        },
    );
    // A node that restarts with a map takes a newer one from its peers, then pushes; and
    // D329's resolver starts once it serves, as do the range moves it follows as a target
    // (D331 PR 4b), each from its progress rows.
    let resolver = spanning::start_resolver(
        Arc::downgrade(&shared),
        shared.limits.txn_resolve_delay,
        spanning::sweep_every(shared.limits.txn_outcome_retention),
    );
    let sync = start_sync(&node, version);
    let follows = Arc::clone(&node);
    let start: Option<Box<dyn FnOnce() + Send>> = Some(Box::new(move || {
        if let Some(sync) = sync {
            sync();
        }
        resolver();
        follows.start_range_follows();
    }));
    super::super::serve_shared(
        super::super::Serving {
            args,
            shared,
            placement,
            tls,
            serving,
            replica: None,
            start,
        },
        stderr,
    )
}

/// The node's start split of its budget across `mine`, the partitions the map gives it
/// (D317 ticket 8a): each one's share of `shards` and `compaction`, and, when
/// `--engine-memory` or the map sets any, each one's memory: the map's `memory_bytes` off
/// the top, the rest of `engine_memory` in equal shares. A restart re-splits it across
/// every partition the map gives the node, which is how a node a run-time open refused
/// for memory takes that partition (D317 ticket 6).
pub(crate) fn start_budget(
    map: Option<&ClusterMap>,
    mine: &[(String, u64)],
    shards: usize,
    compaction: usize,
    engine_memory: Option<u64>,
) -> Result<
    (
        Vec<super::super::placement::Share>,
        Option<Vec<super::super::placement::Memory>>,
    ),
    Failure,
> {
    let shares = super::super::placement::split(shards, compaction, mine.len());
    let overrides: Vec<u64> = mine
        .iter()
        .map(|(p, _)| {
            map.and_then(|m| m.partitions.iter().find(|q| q.id == *p))
                .map_or(0, |q| q.memory_bytes)
        })
        .collect();
    let set_memory = engine_memory.is_some() || overrides.iter().any(|&m| m > 0);
    let memories = super::super::placement::memory(
        engine_memory,
        &shares
            .iter()
            .zip(&overrides)
            .map(|(s, &m)| (s.shards, m))
            .collect::<Vec<_>>(),
    )
    .map_err(|(i, why)| match i.and_then(|i| mine.get(i)) {
        Some((p, _)) => Failure::usage(&format!("partition {p}'s engine memory: {why}")),
        None => Failure::usage(&format!("--engine-memory: {why}")),
    })?;
    Ok((shares, set_memory.then_some(memories)))
}

/// How a node's partitions share its budget as they open (D317 ticket 8a).
#[derive(Clone, Copy)]
pub(super) struct Budget<'a> {
    /// Each partition's threads, in the partitions' order.
    pub(super) shares: &'a [super::super::placement::Share],
    /// The node's reserved compaction CPUs, with several partitions and pinning.
    pub(super) reserved: Option<&'a [usize]>,
    /// Each partition's memory, when `--engine-memory` or the map sets any.
    pub(super) memories: Option<&'a [super::super::placement::Memory]>,
}

/// One partition opened on this node (D317 ticket 8a).
pub(super) struct Opened {
    pub(super) id: String,
    pub(super) db: pigeonhole::Pigeonhole,
    pub(super) pinning: super::super::service::Pinning,
    pub(super) memory: super::super::placement::Memory,
}

/// Opens the partitions `mine` (with their epochs) in order, each checked against the map
/// and given its share of the node's budget (D317 ticket 8a): partitions past the node's
/// compaction total share the compaction CPUs the earlier ones pinned (ICR 0134,
/// perf287's A1). Each one's thread placement is printed; an engine with threads unpinned
/// where pinning works is a warning. A partition that fails to open fails the node's start,
/// after closing those already open (cli692's M2).
pub(super) fn open_partitions(
    dir: &Path,
    cluster_id: Option<&str>,
    mine: &[(String, u64)],
    replica: bool,
    budget: Budget<'_>,
    options: impl Fn(
        super::super::placement::Share,
        Option<super::super::placement::Memory>,
    ) -> pigeonhole::Options,
    stderr: &mut dyn std::io::Write,
) -> Result<Vec<Opened>, Failure> {
    let Some(cluster_id) = cluster_id else {
        return Ok(Vec::new());
    };
    let mut opened: Vec<Opened> = Vec::new();
    // A partition that fails to open fails the node, closing those already open (M2).
    let fail = |opened: Vec<Opened>, e: Failure| {
        for o in opened {
            let _ = o.db.close();
        }
        Err(e)
    };
    let mut compaction_cpus: Vec<usize> = Vec::new();
    let Budget {
        shares,
        reserved,
        memories,
    } = budget;
    for (i, ((id, epoch), share)) in mine.iter().zip(shares).enumerate() {
        let planned = memories.map(|m| m[i]);
        let open = |memory: Option<super::super::placement::Memory>, set: &[usize]| {
            let mut o = options(*share, memory);
            // The node's reserved compaction CPUs, for every partition; without them (one
            // partition, or no pinning), only the partitions past the node's compaction total
            // share the earlier ones' (perf287's A1).
            match reserved {
                Some(r) if share.compaction > 0 => o = o.compaction_cpu_set(r.to_vec()),
                Some(_) => {}
                None if share.confined && !set.is_empty() => {
                    o = o.compaction_cpu_set(set.to_vec());
                }
                None => {}
            }
            if replica {
                open_replica_copy(dir, cluster_id, id, o).map(Some)
            } else {
                open_partition(dir, cluster_id, id, *epoch, o)
            }
        };
        let mut db = match open(planned, &compaction_cpus) {
            Ok(Some(db)) => db,
            // Stamped past the map: a restore or relocation onto this node that stopped
            // before its map was set. Not this map's copy, so not served, but the node
            // starts: the rerun needs it, and the map that names that epoch opens it
            // (audit120's P2-C4). Status shows the epochs.
            Ok(None) => {
                let _ = writeln!(
                    stderr,
                    "phdb serve: partition {id}: its file is stamped at an epoch above the \
                     map's {epoch} (a restore or relocation that stopped before its map was \
                     set): left unopened until a map names that epoch. Run the restore or \
                     relocation again"
                );
                continue;
            }
            Err(e) => return fail(opened, e),
        };
        // A partition whose change feed recorded a shard count keeps it, whatever its share
        // (ICR 0132): its memory is then resized for the count it has, and reopened with it
        // (cli692's F2 on #1718), so its memtables stay within its share.
        let actual = db.info().map_or(share.shards, |i| i.shards);
        let memory = match planned {
            Some(m) if m.shards != actual => {
                let bytes = m.bytes();
                let resized = match super::super::placement::Memory::of(bytes, actual) {
                    Ok(r) => r,
                    Err(least) => {
                        let _ = db.close();
                        return fail(
                            opened,
                            Failure::usage(&format!(
                                "partition {id}: its change feed keeps {actual} shards, and its \
                                 {bytes} bytes of engine memory can't cover their memtables: it \
                                 needs at least {least}"
                            )),
                        );
                    }
                };
                if let Err(e) = db.close() {
                    return fail(opened, Failure::db("close the partition to resize it", &e));
                }
                db = match open(Some(resized), &compaction_cpus) {
                    Ok(Some(db)) => db,
                    Ok(None) => {
                        return fail(
                            opened,
                            Failure::usage(&format!(
                                "partition {id}'s file changed epoch while it reopened"
                            )),
                        );
                    }
                    Err(e) => return fail(opened, e),
                };
                resized
            }
            Some(m) => m,
            None => super::super::placement::Memory::defaults(actual),
        };
        compaction_cpus.extend(db.pinned_compaction_cpus());
        let pinning = super::super::service::Pinning::of(&db, share.compaction);
        let _ = writeln!(
            stderr,
            "phdb serve: partition {id}: {} shard threads ({} pinned), {} compaction threads \
             ({} pinned, {} on the node's compaction CPUs); {} bytes of memtables ({} per \
             shard), a {}-byte block cache and a {}-byte row cache",
            pinning.shards,
            pinning.shards_pinned,
            pinning.compaction,
            pinning.compaction_pinned,
            pinning.compaction_confined,
            memory.memtables(),
            memory.memtable_per_shard,
            memory.block_cache,
            memory.row_cache,
        );
        // Where pinning works at all (Linux), an engine left unpinned is worth saying: its
        // threads can land on another partition's shards or the network side's CPUs.
        if cfg!(target_os = "linux")
            && (pinning.shards_unpinned() > 0 || pinning.compaction_unpinned() > 0)
        {
            let _ = writeln!(
                stderr,
                "phdb serve: WARNING: partition {id}: {} shard and {} compaction threads \
                 run unpinned ({}): give the node fewer --shards or --compaction-threads, or \
                 more CPUs",
                pinning.shards_unpinned(),
                pinning.compaction_unpinned(),
                db.engine_metrics()
                    .unpinned_reason
                    .unwrap_or("the OS refused some pins"),
            );
        }
        opened.push(Opened {
            id: id.clone(),
            db,
            pinning,
            memory,
        });
    }
    Ok(opened)
}

/// For a node that starts with a map at `version`: what runs once it serves, taking its
/// peers' newer maps ([`Node::sync_on_start`]). Until that's done, the node changes no map
/// as the writer (D317 amendment 1), so a writer back after a forced recovery takes the
/// forced map, which no longer names it, before anything else. `None` for a node without a
/// map yet.
pub(crate) fn start_sync(node: &Arc<Node>, version: u64) -> Option<Box<dyn FnOnce() + Send>> {
    (version > 0).then(|| {
        node.synced
            .store(false, std::sync::atomic::Ordering::Release);
        let n = Arc::clone(node);
        Box::new(move || {
            tokio::spawn(async move {
                n.sync_on_start().await;
                // Its replica copies (D328) open as a running node opens them, now that it
                // serves and holds its peers' newest map.
                n.open_missing().await;
            });
        }) as Box<dyn FnOnce() + Send>
    })
}

/// The options a cluster node opens its partition with: `served`, the options every
/// database the server opens takes ([`served_options`](super::super::served_options): the shard
/// poll, WAL preallocation, `--shm-dir` and `--change-feed`, as a single node's), with its
/// share of the node's budget (D317 ticket 8a). A partition whose feed is on keeps it
/// without the flag (D317 ticket 7), and its feed's recorded shard count.
pub(crate) fn partition_options(
    served: pigeonhole::Options,
    share: super::super::placement::Share,
) -> pigeonhole::Options {
    // Its share of the node's budget (D317 ticket 8a): the shard count as a default, so a
    // partition whose feed recorded a count keeps it (ICR 0132), never resharding it.
    served
        .default_shards(share.shards)
        .compaction_cores(share.compaction)
}

/// Opens the database at `path` for writing, retrying a moment while a read-only open of
/// the file holds its writer lock (an operator's `phdb info`, another node's tooling): 50 ms,
/// doubling, about 1.5 s in all, then the refusal stands (#1770).
pub(crate) fn open_retrying(
    path: &Path,
    options: &pigeonhole::Options,
) -> pigeonhole::Result<pigeonhole::Pigeonhole> {
    let mut wait = std::time::Duration::from_millis(50);
    loop {
        match pigeonhole::Pigeonhole::open(path, options.clone()) {
            Err(e)
                if e.code() == pigeonhole::ErrorCode::WriterLocked && wait.as_millis() <= 800 =>
            {
                std::thread::sleep(wait);
                wait *= 2;
            }
            opened => return opened,
        }
    }
}

/// Opens this node's replica copy of `partition` in `dir` (D328): `DIR/P.phdb`, created
/// empty when missing, for its follower's bootstrap to fill. A file that's this cluster's
/// and partition's at any epoch (a copy keeps the epoch it bootstrapped at until its
/// follower records the owner's), or has no identity row yet, is the copy. One of another
/// cluster or partition is refused, and so is one that isn't a replica's and has tables: a
/// moved-away owner's file, which a bootstrap would replace, so `drop-partition` must move
/// it aside first (a single node's `--replica-of` refuses one so too).
fn open_replica_copy(
    dir: &Path,
    cluster_id: &str,
    partition: &str,
    options: pigeonhole::Options,
) -> Result<pigeonhole::Pigeonhole, Failure> {
    stamp::finish_install(dir, partition).map_err(|e| Failure::usage(&e.message))?;
    let path = dir.join(format!("{partition}.phdb"));
    let db = open_retrying(&path, &options)
        .map_err(|e| Failure::db("open the partition's replica copy", &e))?;
    let refuse = |db: pigeonhole::Pigeonhole, why: String| {
        let _ = db.close();
        Err(Failure::usage(&format!(
            "{} can't be this node's replica of partition {partition:?}: {why}",
            path.display()
        )))
    };
    match identity::read(&db) {
        Ok(Some(id)) if id.cluster_id != cluster_id || id.partition_id != partition => {
            return refuse(
                db,
                format!(
                    "it's partition {:?} of cluster {:?}",
                    id.partition_id, id.cluster_id
                ),
            );
        }
        Ok(_) => {}
        Err(e) => return refuse(db, e),
    }
    let is_replica = match db.replicated_seqno() {
        Ok(applied) => applied.is_some(),
        Err(e) => return refuse(db, format!("reading its replica state: {e}")),
    };
    if !is_replica && !db.tables().is_empty() {
        return refuse(
            db,
            "it isn't a replica's copy and has tables (an owner's file, kept after the \
             partition moved away): move it aside with phdb cluster drop-partition first"
                .to_owned(),
        );
    }
    Ok(db)
}

/// D331 PR 3: a restored file (its stamp left the clip marker) is clipped to the map in
/// `dir` before it serves ([`moves::clip_restored`]); the marker stays until the clip is done,
/// so a clip that stopped runs again at the next open.
fn clip_if_restored(
    dir: &Path,
    db: &pigeonhole::Pigeonhole,
    partition: &str,
) -> Result<(), String> {
    if !crate::serve::ranges::clip_pending(db)? {
        return Ok(());
    }
    let map = read_map(dir)?.ok_or("no cluster map to clip to")?;
    let c = moves::clip_restored(db, &map, partition)?;
    eprintln!(
        "phdb serve: partition {partition}: the restored file is clipped to the map: {} rows \
         outside its ranges deleted{}",
        c.deleted,
        if c.in_doubt == 0 {
            String::new()
        } else {
            format!(
                "; {} cluster transaction(s) in doubt here could still write outside them, so \
                 the clip runs again at the next open: decide them (phdb cluster txn decide), \
                 then reopen the partition",
                c.in_doubt
            )
        }
    );
    Ok(())
}

/// Opens partition `partition`'s file in `dir`, checking its identity row against the map:
/// this cluster's, this partition's, at the map's epoch. A file that says otherwise was
/// stamped for another cluster or partition, or the map moved past it: refused, not served.
/// `None` for this cluster's and partition's file at an epoch **above** the map's: a stamp
/// whose map didn't land yet, which isn't a reason to fail the node (audit120's P2-C4).
pub(super) fn open_partition(
    dir: &Path,
    cluster_id: &str,
    partition: &str,
    epoch: u64,
    options: pigeonhole::Options,
) -> Result<Option<pigeonhole::Pigeonhole>, Failure> {
    // An install a crash interrupted is finished first: the partition never opens with its
    // marker there (D317 ticket 6, feed1248's F1).
    stamp::finish_install(dir, partition).map_err(|e| Failure::usage(&e.message))?;
    let path = dir.join(format!("{partition}.phdb"));
    if !path.exists() {
        return Err(Failure::usage(&format!(
            "the map gives this node partition {partition}, but {} doesn't exist: create it \
             with phdb cluster init-partition",
            path.display()
        )));
    }
    let db = open_retrying(&path, &options).map_err(|e| Failure::db("open the partition", &e))?;
    let want = identity::Identity {
        cluster_id: cluster_id.to_owned(),
        partition_id: partition.to_owned(),
        epoch,
    };
    match identity::read(&db) {
        Ok(Some(id)) if id == want => match clip_if_restored(dir, &db, partition) {
            Ok(()) => Ok(Some(db)),
            Err(e) => {
                let _ = db.close();
                Err(Failure::usage(&format!(
                    "partition {partition:?}: clipping the restored file to the map: {e}"
                )))
            }
        },
        Ok(Some(id))
            if id.cluster_id == want.cluster_id
                && id.partition_id == want.partition_id
                && id.epoch > epoch =>
        {
            let _ = db.close();
            Ok(None)
        }
        // D331 (simp724's B3): just below a map that hands a range of it over, at that epoch:
        // a crash after the map's persist and before the source's stamp. The open finishes
        // the hand-over (the moved-away row, then the stamp) before it serves. The map's
        // epoch is exactly the hand-over's from the file's: one above, or past a promote's
        // floor (#2070, ai1249's review), so a file staler than that isn't served.
        Ok(Some(id))
            if id.cluster_id == want.cluster_id
                && id.partition_id == want.partition_id
                && id.epoch < epoch
                && let Ok(Some(map)) = read_map(dir)
                && epoch == id.epoch.max(promote::promote_floor(&map, partition)) + 1
                && let Some(m) = moves::handing_over(&map, partition) =>
        {
            match moves::finish_hand_over(&db, m, epoch, dir) {
                Ok(_) => Ok(Some(db)),
                Err(e) => {
                    let _ = db.close();
                    Err(Failure::usage(&format!(
                        "partition {partition:?}: finishing move {:?}'s hand-over: {e}",
                        m.id
                    )))
                }
            }
        }
        found => {
            let _ = db.close();
            let found = match found {
                Ok(Some(id)) => format!(
                    "partition {:?} of cluster {:?} at epoch {}",
                    id.partition_id, id.cluster_id, id.epoch
                ),
                Ok(None) => "no identity row (run phdb cluster init-partition)".to_owned(),
                Err(e) => e,
            };
            Err(Failure::usage(&format!(
                "{} isn't partition {partition:?} of cluster {cluster_id:?} at the map's epoch \
                 {epoch}: it says {found}",
                path.display()
            )))
        }
    }
}

/// Fills `lp`'s in-doubt fields (D329, `LocalPartition` 21–25) from `db`'s shares held in
/// doubt, with each one's reason as the resolver last found it.
pub(super) fn in_doubt(
    lp: &mut LocalPartition,
    db: &pigeonhole::Pigeonhole,
    partition: &str,
    spanning: Option<&spanning::Spanning>,
) {
    let mut shares = db.in_doubt_shares();
    if shares.is_empty() {
        return;
    }
    // The oldest first.
    shares.sort_by_key(|s| std::cmp::Reverse(s.1));
    let ms = |d: std::time::Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    lp.in_doubt = u32::try_from(shares.len()).unwrap_or(u32::MAX);
    lp.in_doubt_oldest_xid = String::from_utf8_lossy(&shares[0].0).into_owned();
    lp.in_doubt_oldest_age_ms = ms(shares[0].1);
    lp.in_doubt_wal_bytes = shares.iter().map(|s| s.2).sum();
    lp.in_doubt_shares = shares
        .iter()
        .map(|(xid, age, _)| proto::InDoubtShare {
            xid: String::from_utf8_lossy(xid).into_owned(),
            age_ms: ms(*age),
            reason: spanning
                .and_then(|s| s.reason(partition, xid))
                .unwrap_or_else(|| "its decision hasn't arrived yet".to_owned()),
        })
        .collect();
}

/// The partitions whose owner, epoch or history `new` changes from `old` (all of them for a
/// first map): what a copy's follower must look up again (D330 amendment 2).
pub(super) fn moved_partitions(old: Option<&ClusterMap>, new: &ClusterMap) -> Vec<String> {
    // By id, not a scan per partition (perf2's nit on #2069): this runs under the state lock.
    let before: HashMap<&str, &crate::serve::proto::Partition> = old
        .map(|o| o.partitions.iter().map(|q| (q.id.as_str(), q)).collect())
        .unwrap_or_default();
    new.partitions
        .iter()
        .filter(|p| {
            before.get(p.id.as_str()).is_none_or(|q| {
                q.node != p.node || q.epoch != p.epoch || q.history_epoch != p.history_epoch
            })
        })
        .map(|p| p.id.clone())
        .collect()
}
