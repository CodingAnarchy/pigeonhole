//! Opening a partition on a running node (D317 ticket 6, §3): a map that gives a node a
//! partition it doesn't have open opens it, fenced, and that map, naming the node at the
//! file's epoch, unfences it: the node serves it at once, its schema the map's.

use super::*;
use crate::serve::proto::{
    CreateTableRequest, FamilySpec, LocalPartitionState, TablePlacementRequest,
    schema_client::SchemaClient,
};

#[test]
fn a_map_that_gives_a_node_a_partition_opens_it_and_it_serves() {
    // a serves p1, b p2; c only routes, with p3's file in its directory.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let mut map = the_map(1, &ports);
    map.partitions.retain(|p| p.id != "p3");
    let dirs = [test_dir(), test_dir(), test_dir()];
    // One shard: a light open, so the wait below measures the open path, not a slow
    // engine under the whole module's load.
    test_partition_with(&dirs[2], "p3", pigeonhole::Options::default().shards(1))
        .close()
        .unwrap();
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for ((l, dir), (id, local)) in
        listeners
            .into_iter()
            .zip(&dirs)
            .zip([("a", Some("p1")), ("b", Some("p2")), ("c", None)])
    {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
        let (stop, db) = data_node_tokens(&rt, dir, id, local, l, "");
        stops.push(stop);
        dbs.extend(db);
    }
    let [a, b, c] = ports;
    rt.block_on(async {
        // Version 2 gives c partition p3, through a, the writer.
        client(a)
            .await
            .set_map(SetMapRequest {
                force: false,
                expected_map_version: 1,
                map: Some(the_map(2, &ports)),
                copies: Vec::new(),
            })
            .await
            .unwrap();
        eventually("c to open p3 and serve it", async || {
            client(c)
                .await
                .status(StatusRequest {})
                .await
                .unwrap()
                .into_inner()
                .partitions
                .iter()
                .any(|lp| lp.id == "p3" && lp.state() == LocalPartitionState::Serving)
        })
        .await;
        // A table placed on p3 alone, through b: c's p3 takes it, and its rows.
        let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{b}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        SchemaClient::new(channel)
            .create_table(CreateTableRequest {
                name: "items".into(),
                families: vec![FamilySpec {
                    name: "f".into(),
                    ..Default::default()
                }],
                if_missing: false,
                placement: Some(TablePlacementRequest {
                    group: String::new(),
                    splits: Vec::new(),
                    partitions: vec!["p3".into()],
                }),
            })
            .await
            .unwrap();
        // c holds p3: a write through a goes to c, which answers for the table only once
        // it has reconciled to the writer's map (the call answers before then, #1778).
        let want = client(a)
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner()
            .map_version;
        eventually("c's p3 to reconcile to the map with items", async || {
            client(c)
                .await
                .status(StatusRequest {})
                .await
                .unwrap()
                .into_inner()
                .partitions
                .iter()
                .any(|lp| lp.id == "p3" && lp.reconciled_version == want)
        })
        .await;
        let mut w = put_row("pen", "1");
        w.row.as_mut().unwrap().table = "items".into();
        data(a)
            .await
            .mutate(crate::serve::proto::MutateRequest {
                writes: Some(w),
                durability: 0,
            })
            .await
            .unwrap();
        let got = data(b)
            .await
            .get(crate::serve::proto::GetRequest {
                table: "items".into(),
                row: b"pen".to_vec().into(),
                family: "f".into(),
                qualifier: b"q".to_vec().into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(got.cell.map(|c| c.value.to_vec()), Some(b"1".to_vec()));
    });
    drop(stops);
    for db in dbs {
        let _ = db.close();
    }
}

/// A partition opened while the node serves pins its shards on the free cores (the
/// coordinator's mask directive): never on an open partition's shard CPU or its SMT
/// sibling. One whose shards don't fit there is refused, not pinned onto a sibling or left
/// unpinned. Linux only (elsewhere nothing pins); alone in its process, since pin claims
/// are process-wide (#1741).
#[cfg(target_os = "linux")]
#[test]
fn a_partition_opened_while_serving_pins_off_the_others_cores_or_is_refused() {
    run_alone(
        module_path!(),
        "a_partition_opened_while_serving_pins_off_the_others_cores_or_is_refused",
        opened_off_the_others_cores_alone,
    );
}

#[cfg(target_os = "linux")]
fn opened_off_the_others_cores_alone() {
    use crate::serve::placement::{Placement, siblings};
    let dir = test_dir();
    let one = || pigeonhole::Options::default().shards(1);
    let p1 = test_partition_with(&dir, "p1", one());
    let held = p1.pinned_shard_cpus();
    if held.is_empty() {
        println!("skipped: nothing pins here");
        return;
    }
    // The node's placement as `run` plans it around p1's shard.
    let placement = Placement::plan(&[&p1], Some(2), Some(2), None, None, true);
    let Some(free) = placement.masks.blocking.clone() else {
        println!("skipped: too few free cores for a mask");
        return;
    };
    // p3 fits on the free cores; p4 has as many shards as they hold, one of which p3 takes.
    test_partition_with(&dir, "p3", one()).close().unwrap();
    test_partition_with(
        &dir,
        "p4",
        pigeonhole::Options::default().shards(free.len()),
    )
    .close()
    .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let part = |id: &str| Partition {
        id: id.into(),
        node: "a".into(),
        epoch: 1,
        memory_bytes: 0,
        history_epoch: 0,
    };
    let mut map = ClusterMap {
        cluster_id: "c1".into(),
        map_version: 1,
        nodes: vec![MapNode {
            id: "a".into(),
            address: format!("127.0.0.1:{port}"),
        }],
        partitions: vec![part("p1")],
        tables: vec![TablePlacement {
            name: "users".into(),
            group: String::new(),
            families: Vec::new(),
            splits: Vec::new(),
            partitions: vec!["p1".into()],
            created: 0,
        }],
    };
    super::super::write_map(&dir, &super::super::map::canonical_json(&map)).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (stop, dbs, node) = data_node_placed(
        &rt,
        &dir,
        "a",
        vec![("p1", Some(p1))],
        listener,
        "",
        crate::serve::server_tests::limits(),
        false,
        placement,
        None,
    );
    map.map_version = 2;
    map.partitions.extend([part("p3"), part("p4")]);
    let shared = node.opener.get().unwrap().shared.upgrade().unwrap();
    // p1's feed side on runtimes of its own, entered as the server's are but with no park or
    // unpark hooks, so only the hub's and delivery's own refreshes can move them (perf287's
    // P1 on #1755: a busy hub may never park).
    let feed_rt = {
        let p = Arc::clone(&node.opener.get().unwrap().placement);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("phdb-test-feed")
            .on_thread_start(move || p.enter_feed())
            .enable_all()
            .build()
            .unwrap()
    };
    let hub_rt = {
        let p = Arc::clone(&node.opener.get().unwrap().placement);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("phdb-test-hub")
            .on_thread_start(move || p.enter_hub())
            .enable_all()
            .build()
            .unwrap()
    };
    for part in shared.router.parts() {
        part.feeds
            .run_on(feed_rt.handle().clone(), hub_rt.handle().clone());
    }
    // A subscriber to p1's `users`, and writes to it, so the hub reads and delivers.
    let busy = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let _subscriber = rt.spawn(async move {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut s = crate::serve::proto::feed_client::FeedClient::new(channel)
            .subscribe(crate::serve::proto::SubscribeRequest {
                tables: vec!["users".into()],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        while let Ok(Some(_)) = s.message().await {}
    });
    let writer = {
        let busy = Arc::clone(&busy);
        rt.spawn(async move {
            let mut i = 0u64;
            while busy.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = data(port)
                    .await
                    .mutate(crate::serve::proto::MutateRequest {
                        writes: Some(put_row(&format!("a{i}"), "1")),
                        durability: 0,
                    })
                    .await;
                i += 1;
            }
        })
    };
    // The process's CPUs, which an unmasked thread gets back.
    let process: Vec<usize> = {
        let set = nix::sched::sched_getaffinity(nix::unistd::Pid::from_raw(0)).unwrap();
        (0..nix::sched::CpuSet::count())
            .filter(|&c| set.is_set(c).unwrap_or(false))
            .collect()
    };
    // A thread's CPUs, by its name.
    let cpus_of = |name: &str| -> Option<Vec<usize>> {
        for task in std::fs::read_dir("/proc/self/task").ok()?.flatten() {
            let comm = std::fs::read_to_string(task.path().join("comm")).unwrap_or_default();
            if comm.trim() == name {
                let tid: i32 = task.file_name().to_string_lossy().parse().ok()?;
                let set = nix::sched::sched_getaffinity(nix::unistd::Pid::from_raw(tid)).ok()?;
                return Some(
                    (0..nix::sched::CpuSet::count())
                        .filter(|&c| set.is_set(c).unwrap_or(false))
                        .collect(),
                );
            }
        }
        None
    };
    // A thread of the server's own, on the free cores before p3 opens: it takes the mask
    // the open reapplies at its next refresh.
    let placement = Arc::clone(&node.opener.get().unwrap().placement);
    let (go, wait) = std::sync::mpsc::channel::<()>();
    let worker = {
        let placement = Arc::clone(&placement);
        std::thread::spawn(move || {
            placement.enter();
            wait.recv().unwrap();
            crate::serve::placement::refresh();
            let set = nix::sched::sched_getaffinity(nix::unistd::Pid::from_raw(0)).unwrap();
            (0..nix::sched::CpuSet::count())
                .filter(|&c| set.is_set(c).unwrap_or(false))
                .collect::<Vec<usize>>()
        })
    };
    rt.block_on(async {
        client(port)
            .await
            .set_map(SetMapRequest {
                force: false,
                expected_map_version: 1,
                map: Some(map),
                copies: Vec::new(),
            })
            .await
            .unwrap();
        eventually("p3 to open", async || shared.router.partition("p3").is_ok()).await;
        let mut taken: Vec<usize> = held.iter().flat_map(|&c| siblings(c)).collect();
        taken.extend(&held);
        let new = shared
            .router
            .partition("p3")
            .unwrap()
            .db
            .pinned_shard_cpus();
        assert_eq!(new.len(), 1, "p3's shard pinned");
        for c in &new {
            assert!(
                !taken.contains(c),
                "p3's shard on {c}: p1's {held:?} with siblings {taken:?}"
            );
            assert!(
                free.contains(c),
                "p3's shard on {c}, off the free cores {free:?}"
            );
        }
        // The masks were reapplied without p3's shard and its siblings, and a thread of
        // ours takes them.
        // Where p3's shard and its siblings leave fewer than two free cores (a 4-CPU
        // runner with SMT), the masks fall back to none, and the threads get the process's
        // CPUs back, as at start.
        let now = placement.current();
        let blocking = match &now.blocking {
            Some(free) => {
                let mut off: Vec<usize> = new.iter().flat_map(|&c| siblings(c)).collect();
                off.extend(&new);
                assert!(
                    free.iter().all(|c| !off.contains(c) && !taken.contains(c)),
                    "the free cores {free:?} after p3's shard {new:?}"
                );
                free.clone()
            }
            None => process.clone(),
        };
        go.send(()).unwrap();
        assert_eq!(worker.join().unwrap(), blocking);
        // The feed side's thread, busy delivering p1's commits (with one client the hub
        // reads on the feed runtime too, #1689), takes the new mask by its own refresh.
        let feed = now.feed.clone().unwrap_or_else(|| process.clone());
        eventually("the busy feed thread to take the new mask", async || {
            cpus_of("phdb-test-feed").as_ref() == Some(&feed)
        })
        .await;
        busy.store(false, std::sync::atomic::Ordering::Relaxed);
        let _ = writer.await;
        let e = node.open_part("p4").await.unwrap_err();
        assert_eq!(e.code, "FailedPrecondition", "{}", e.message);
        assert!(
            e.message.contains("don't fit on this node's free cores"),
            "{}",
            e.message
        );
        assert!(shared.router.partition("p4").is_err());
    });
    drop(shared);
    drop(stop);
    for db in dbs {
        let _ = db.close();
    }
}

/// A run-time open gets what the open partitions leave of `--engine-memory` (perf287's P2 on
/// #1755). At start they split all of it, so the open is refused, saying to restart the
/// node; restarting that node alone re-splits it across both, and the partition serves (the
/// coordinator's recovery requirement).
#[test]
fn an_open_past_the_engine_memory_is_refused_and_a_restart_serves_it() {
    use crate::serve::proto::LocalPartitionState;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = test_dir();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let part = |id: &str| Partition {
        id: id.into(),
        node: "a".into(),
        epoch: 1,
        memory_bytes: 0,
        history_epoch: 0,
    };
    let mut map = ClusterMap {
        cluster_id: "c1".into(),
        map_version: 1,
        nodes: vec![MapNode {
            id: "a".into(),
            address: format!("127.0.0.1:{port}"),
        }],
        partitions: vec![part("p1")],
        tables: vec![TablePlacement {
            name: "users".into(),
            group: String::new(),
            families: Vec::new(),
            splits: Vec::new(),
            partitions: vec!["p1".into()],
            created: 0,
        }],
    };
    super::super::write_map(&dir, &super::super::map::canonical_json(&map)).unwrap();
    const BUDGET: u64 = 4 << 20;
    let start = |listener, locals, restart| {
        data_node_placed(
            &rt,
            &dir,
            "a",
            locals,
            listener,
            "",
            crate::serve::server_tests::limits(),
            restart,
            crate::serve::placement::Placement::routes_only(Some(2), Some(2)),
            Some(BUDGET),
        )
    };
    let p1 = test_partition_with(&dir, "p1", pigeonhole::Options::default().shards(1));
    let (stop, _, node) = start(listener, vec![("p1", Some(p1.clone()))], false);
    test_partition_with(&dir, "p3", pigeonhole::Options::default().shards(1))
        .close()
        .unwrap();
    map.map_version = 2;
    map.partitions.push(part("p3"));
    rt.block_on(async {
        client(port)
            .await
            .set_map(SetMapRequest {
                force: false,
                expected_map_version: 1,
                map: Some(map),
                copies: Vec::new(),
            })
            .await
            .unwrap();
        let e = node.open_part("p3").await.unwrap_err();
        assert_eq!(e.code, "FailedPrecondition", "{}", e.message);
        assert!(
            e.message
                .contains("Restart this node to re-split --engine-memory"),
            "{}",
            e.message
        );
    });
    // The node restarts alone: the map already gives it p3.
    drop(node);
    let _ = stop.send(());
    rt.block_on(async {
        eventually("the node to stop", async || {
            tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_err()
        })
        .await;
    });
    let listener = std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
    let (_stop, dbs, _node) = start(listener, vec![("p1", Some(p1)), ("p3", None)], true);
    rt.block_on(async {
        eventually("p3 to serve after the restart", async || {
            client(port)
                .await
                .status(StatusRequest {})
                .await
                .unwrap()
                .into_inner()
                .partitions
                .iter()
                .any(|lp| lp.id == "p3" && lp.state() == LocalPartitionState::Serving)
        })
        .await;
    });
    // Each took half of the budget, as `run` splits it.
    let (_, memories) = super::super::start_budget(
        None,
        &[("p1".into(), 1), ("p3".into(), 1)],
        2,
        0,
        Some(BUDGET),
    )
    .unwrap_or_else(|f| panic!("{}", f.message()));
    let total: u64 = memories.unwrap().iter().map(|m| m.bytes()).sum();
    assert!(total <= BUDGET, "{total}");
    for db in dbs {
        let _ = db.close();
    }
}

/// Three nodes (a: p1, b: p2, c routes only) with p3's one-shard file in c's directory.
struct P3OnC {
    rt: tokio::runtime::Runtime,
    ports: [u16; 3],
    stops: Vec<tokio::sync::oneshot::Sender<()>>,
    dbs: Vec<pigeonhole::Pigeonhole>,
    dirs: [PathBuf; 3],
}

fn three_with_p3_on_c() -> P3OnC {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let mut map = the_map(1, &ports);
    map.partitions.retain(|p| p.id != "p3");
    let dirs = [test_dir(), test_dir(), test_dir()];
    test_partition_with(&dirs[2], "p3", pigeonhole::Options::default().shards(1))
        .close()
        .unwrap();
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for ((l, dir), (id, local)) in
        listeners
            .into_iter()
            .zip(&dirs)
            .zip([("a", Some("p1")), ("b", Some("p2")), ("c", None)])
    {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
        let (stop, db) = data_node_tokens(&rt, dir, id, local, l, "");
        stops.push(stop);
        dbs.extend(db);
    }
    P3OnC {
        rt,
        ports,
        stops,
        dbs,
        dirs,
    }
}

async fn give_c_p3(ports: &[u16; 3]) {
    client(ports[0])
        .await
        .set_map(SetMapRequest {
            force: false,
            expected_map_version: 1,
            map: Some(the_map(2, ports)),
            copies: Vec::new(),
        })
        .await
        .unwrap();
}

async fn p3_on_c(c: u16) -> Option<crate::serve::proto::LocalPartition> {
    client(c)
        .await
        .status(StatusRequest {})
        .await
        .unwrap()
        .into_inner()
        .partitions
        .into_iter()
        .find(|lp| lp.id == "p3")
}

/// #1770: a partition being opened shows as opening in Status, which doesn't read its file:
/// a read-only open there would refuse the node's own open.
#[test]
fn a_partition_being_opened_shows_opening_and_status_leaves_its_file_alone() {
    use crate::serve::proto::LocalPartitionState;
    let P3OnC {
        rt,
        ports,
        stops,
        dbs,
        dirs,
    } = three_with_p3_on_c();
    let c = ports[2];
    let (release, wait) = std::sync::mpsc::channel::<()>();
    super::super::open::HOLD_OPEN
        .lock()
        .unwrap()
        .push((dirs[2].clone(), wait));
    rt.block_on(async {
        give_c_p3(&ports).await;
        eventually("p3 to be opening", async || {
            p3_on_c(c)
                .await
                .is_some_and(|lp| lp.state() == LocalPartitionState::Opening)
        })
        .await;
        let lp = p3_on_c(c).await.unwrap();
        assert!(lp.ok && lp.error.is_empty(), "{lp:?}");
        release.send(()).unwrap();
        eventually("p3 to serve", async || {
            p3_on_c(c)
                .await
                .is_some_and(|lp| lp.state() == LocalPartitionState::Serving)
        })
        .await;
    });
    drop(stops);
    for db in dbs {
        let _ = db.close();
    }
}

/// #1770: an open the map drives retries a moment when a read-only open of the file holds
/// its lock (an operator's `phdb info`, a Status from before #1770), rather than leaving the
/// partition unopened until the next map.
#[test]
fn an_open_retries_while_a_read_only_open_holds_the_file() {
    use crate::serve::proto::LocalPartitionState;
    let P3OnC {
        rt,
        ports,
        stops,
        dbs,
        dirs,
    } = three_with_p3_on_c();
    let c = ports[2];
    let reader = pigeonhole::Pigeonhole::open(
        dirs[2].join("p3.phdb"),
        pigeonhole::Options::default().read_only(true),
    )
    .unwrap();
    rt.block_on(async {
        give_c_p3(&ports).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        drop(reader);
        eventually("p3 to serve once the reader is gone", async || {
            p3_on_c(c)
                .await
                .is_some_and(|lp| lp.state() == LocalPartitionState::Serving)
        })
        .await;
    });
    drop(stops);
    for db in dbs {
        let _ = db.close();
    }
}

/// Creates table `items` (family `f`) placed on partition `p` alone, through node `port`.
async fn create_items_on(port: u16, p: &str) {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    SchemaClient::new(channel)
        .create_table(CreateTableRequest {
            name: "items".into(),
            families: vec![FamilySpec {
                name: "f".into(),
                ..Default::default()
            }],
            if_missing: false,
            placement: Some(TablePlacementRequest {
                group: String::new(),
                splits: Vec::new(),
                partitions: vec![p.into()],
            }),
        })
        .await
        .unwrap();
}

/// Partition `p`'s line in node `port`'s Status, if it has one.
async fn part_on(port: u16, p: &str) -> Option<crate::serve::proto::LocalPartition> {
    client(port)
        .await
        .status(StatusRequest {})
        .await
        .unwrap()
        .into_inner()
        .partitions
        .into_iter()
        .find(|lp| lp.id == p)
}

/// audit120's F6: a partition's open reconciled every open partition of its node to the
/// map it began with, so a table created on another of them while the engine opened was
/// dropped, with its acknowledged rows. b serves p2 and opens p3 (held); `items` is made on
/// p2 and written meanwhile; the row is still there once p3 serves.
#[test]
fn a_table_created_while_another_partition_opens_keeps_its_rows() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let mut map = the_map(1, &ports);
    map.partitions.retain(|p| p.id != "p3");
    let dirs = [test_dir(), test_dir(), test_dir()];
    test_partition_with(&dirs[1], "p3", pigeonhole::Options::default().shards(1))
        .close()
        .unwrap();
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for ((l, dir), (id, local)) in
        listeners
            .into_iter()
            .zip(&dirs)
            .zip([("a", Some("p1")), ("b", Some("p2")), ("c", None)])
    {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
        let (stop, db) = data_node_tokens(&rt, dir, id, local, l, "");
        stops.push(stop);
        dbs.extend(db);
    }
    let [a, b, _] = ports;
    let (release, wait) = std::sync::mpsc::channel::<()>();
    super::super::open::HOLD_OPEN
        .lock()
        .unwrap()
        .push((dirs[1].clone(), wait));
    rt.block_on(async {
        // Version 2 gives b partition p3; its open holds.
        let mut v2 = the_map(2, &ports);
        for p in &mut v2.partitions {
            if p.id == "p3" {
                p.node = "b".into();
            }
        }
        client(a)
            .await
            .set_map(SetMapRequest {
                force: false,
                expected_map_version: 1,
                map: Some(v2),
                copies: Vec::new(),
            })
            .await
            .unwrap();
        eventually("b's p3 to be opening", async || {
            part_on(b, "p3")
                .await
                .is_some_and(|lp| lp.state() == LocalPartitionState::Opening)
        })
        .await;
        // Meanwhile, `items` on p2, and a row in it.
        create_items_on(a, "p2").await;
        let want = version(a).await;
        eventually("b's p2 to reconcile to the map with items", async || {
            part_on(b, "p2")
                .await
                .is_some_and(|lp| lp.reconciled_version == want)
        })
        .await;
        let mut w = put_row("pen", "1");
        w.row.as_mut().unwrap().table = "items".into();
        data(a)
            .await
            .mutate(crate::serve::proto::MutateRequest {
                writes: Some(w),
                durability: 0,
            })
            .await
            .unwrap();
        release.send(()).unwrap();
        eventually("b's p3 to serve", async || {
            part_on(b, "p3")
                .await
                .is_some_and(|lp| lp.state() == LocalPartitionState::Serving)
        })
        .await;
        // Serving: the open, its reconcile included, has returned, and the map confirmed it.
        for p in ["p2", "p3"] {
            let lp = part_on(b, p).await.unwrap();
            assert_eq!(lp.reconciled_version, want, "{lp:?}");
        }
        let got = data(b)
            .await
            .get(crate::serve::proto::GetRequest {
                table: "items".into(),
                row: b"pen".to_vec().into(),
                family: "f".into(),
                qualifier: b"q".to_vec().into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(got.cell.map(|c| c.value.to_vec()), Some(b"1".to_vec()));
    });
    drop(stops);
    for db in dbs {
        let _ = db.close();
    }
}

/// audit120's P2-C2: a partition opened on the map's writer was confirmed with the map its
/// open began with. A map set at the writer meanwhile (here a plain version step) made that
/// one older than the node's, so the confirm did nothing, and the newer map's own pass
/// skipped the partition as already open: it stayed fenced "awaiting peer map" with no peer
/// to push one. a is the writer; it opens p3 (held before its confirm) while version 3
/// lands, and must serve p3 once released.
#[test]
fn a_partition_opened_on_the_writer_serves_though_another_map_lands_before_its_confirm() {
    // Bounded (#1865): it hung on CI when its hold stopped a runtime worker, and `eventually`
    // bounds only how often it asks, not a call that never answers.
    bounded(Duration::from_secs(120), || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        another_map_lands_before_the_confirm(&rt);
    });
}

fn another_map_lands_before_the_confirm(rt: &tokio::runtime::Runtime) {
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let mut map = the_map(1, &ports);
    map.partitions.retain(|p| p.id != "p3");
    let dirs = [test_dir(), test_dir(), test_dir()];
    test_partition_with(&dirs[0], "p3", pigeonhole::Options::default().shards(1))
        .close()
        .unwrap();
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for ((l, dir), (id, local)) in
        listeners
            .into_iter()
            .zip(&dirs)
            .zip([("a", Some("p1")), ("b", Some("p2")), ("c", None)])
    {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
        let (stop, db) = data_node_tokens(rt, dir, id, local, l, "");
        stops.push(stop);
        dbs.extend(db);
    }
    let a = ports[0];
    let (release, wait) = std::sync::mpsc::channel::<()>();
    super::super::open::HOLD_CONFIRM
        .lock()
        .unwrap()
        .push((dirs[0].clone(), wait));
    let with_p3_on_a = |version: u64| {
        let mut m = the_map(version, &ports);
        for p in &mut m.partitions {
            if p.id == "p3" {
                p.node = "a".into();
            }
        }
        m
    };
    rt.block_on(async {
        // Version 2 gives a (the writer) partition p3: it opens, and holds before its confirm.
        client(a)
            .await
            .set_map(SetMapRequest {
                force: false,
                expected_map_version: 1,
                map: Some(with_p3_on_a(2)),
                copies: Vec::new(),
            })
            .await
            .unwrap();
        eventually("a's p3 to be open, fenced since it opened", async || {
            part_on(a, "p3")
                .await
                .is_some_and(|lp| lp.state() == LocalPartitionState::Fenced)
        })
        .await;
        // Version 3 lands at a meanwhile.
        client(a)
            .await
            .set_map(SetMapRequest {
                force: false,
                expected_map_version: 2,
                map: Some(with_p3_on_a(3)),
                copies: Vec::new(),
            })
            .await
            .unwrap();
        assert_eq!(version(a).await, 3);
        release.send(()).unwrap();
        eventually("a's p3 to serve", async || {
            part_on(a, "p3")
                .await
                .is_some_and(|lp| lp.state() == LocalPartitionState::Serving)
        })
        .await;
    });
    drop(stops);
    for db in dbs {
        let _ = db.close();
    }
}
