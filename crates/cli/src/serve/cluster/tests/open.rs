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
    );
    map.map_version = 2;
    map.partitions.extend([part("p3"), part("p4")]);
    let shared = node.opener.get().unwrap().shared.upgrade().unwrap();
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
        let now = placement.current();
        let blocking = now.blocking.clone().expect("still masked");
        let mut off: Vec<usize> = new.iter().flat_map(|&c| siblings(c)).collect();
        off.extend(&new);
        assert!(
            blocking
                .iter()
                .all(|c| !off.contains(c) && !taken.contains(c)),
            "the free cores {blocking:?} after p3's shard {new:?}"
        );
        go.send(()).unwrap();
        assert_eq!(worker.join().unwrap(), blocking);
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
