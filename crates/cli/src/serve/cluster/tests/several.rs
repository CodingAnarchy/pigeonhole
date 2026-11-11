//! A node holding several partitions (D317 ticket 8a): each served by its own service,
//! sharing the node's; every call kind through every entry node; session caps per node;
//! the open's checks (the node's budget split, a partition that fails to open).

use super::*;
use crate::serve::proto::{
    FeedEvent, KeyRange, MultiGetRequest, OpenSnapshotRequest, RoutingKey, ScanRequest, Selection,
    SubscribeRequest, feed_client::FeedClient, feed_event, key_range, routing_key,
};

/// The map: `users` split at "g" and "m" over p1, p2 and p3, with node a holding p1 and p3
/// (not adjacent) and b holding p2; c only routes.
fn two_on_a(ports: &[u16; 3]) -> ClusterMap {
    let mut m = three_way(ports);
    for p in &mut m.partitions {
        if p.id == "p3" {
            p.node = "a".into();
        }
    }
    m
}

/// The three nodes over [`two_on_a`], with `limits`: their runtime, ports, stops and
/// partition databases (a's p1 and p3, then b's p2).
struct Several {
    rt: tokio::runtime::Runtime,
    ports: [u16; 3],
    stops: Vec<tokio::sync::oneshot::Sender<()>>,
    dbs: Vec<pigeonhole::Pigeonhole>,
}

impl Several {
    fn start(limits: crate::serve::config::Limits) -> Self {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let listeners: Vec<std::net::TcpListener> = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let ports: Vec<u16> = listeners
            .iter()
            .map(|l| l.local_addr().unwrap().port())
            .collect();
        let ports = [ports[0], ports[1], ports[2]];
        let map = two_on_a(&ports);
        let mut stops = Vec::new();
        let mut dbs = Vec::new();
        for (l, (id, locals)) in
            listeners
                .into_iter()
                .zip([("a", vec!["p1", "p3"]), ("b", vec!["p2"]), ("c", vec![])])
        {
            let dir = test_dir();
            super::super::write_map(&dir, &super::super::map::canonical_json(&map)).unwrap();
            let locals = locals.into_iter().map(|p| (p, None)).collect();
            let (stop, opened) = data_node_many(&rt, &dir, id, locals, l, "", limits);
            stops.push(stop);
            dbs.extend(opened);
        }
        Self {
            rt,
            ports,
            stops,
            dbs,
        }
    }
}

impl Drop for Several {
    fn drop(&mut self) {
        self.stops.clear();
        for db in self.dbs.drain(..) {
            let _ = db.close();
        }
    }
}

fn cell(db: &pigeonhole::Pigeonhole, row: &str) -> Option<Vec<u8>> {
    db.table("users")
        .unwrap()
        .open()
        .unwrap()
        .get(row.as_bytes(), "f", b"q")
        .unwrap()
        .map(|c| c.value().to_vec())
}

#[test]
fn a_node_serves_each_of_its_partitions_through_any_entry_node() {
    let t = Several::start(crate::serve::server_tests::limits());
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        // One row in each partition, written through each node, read through each.
        for (entry, row) in [(c, "alice"), (a, "hank"), (b, "zed")] {
            put(entry, row, row).await;
        }
        for entry in [a, b, c] {
            for row in ["alice", "hank", "zed"] {
                assert_eq!(
                    get_row(entry, row).await.unwrap(),
                    Some(row.into()),
                    "{row}"
                );
            }
        }
        // p1 and p3 are both a's, but two partitions: a batch across them is refused.
        let e = data(c)
            .await
            .batch(crate::serve::proto::BatchRequest {
                rows: vec![put_row("bob", "x"), put_row("yan", "y")],
                durability: 0,
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "CrossPartition", "{e}");
        // A fan-out scan and multi-get read all three, in key order.
        let mut pages = data(c)
            .await
            .scan(ScanRequest {
                table: "users".into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let mut keys = Vec::new();
        while let Some(page) = pages.message().await.unwrap() {
            keys.extend(page.rows.iter().map(|r| r.key.to_vec()));
        }
        assert_eq!(keys, [b"alice".to_vec(), b"hank".to_vec(), b"zed".to_vec()]);
        let got = data(b)
            .await
            .multi_get(MultiGetRequest {
                table: "users".into(),
                keys: vec![b"zed".to_vec().into(), b"alice".to_vec().into()],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let keys: Vec<_> = got.rows.iter().map(|r| r.key.to_vec()).collect();
        assert_eq!(keys, [b"alice".to_vec(), b"zed".to_vec()]);
    });
    // Each row is in its own partition's file only, a's two included.
    let (p1, p3, p2) = (&t.dbs[0], &t.dbs[1], &t.dbs[2]);
    assert_eq!(cell(p1, "alice"), Some(b"alice".to_vec()));
    assert_eq!(cell(p3, "zed"), Some(b"zed".to_vec()));
    assert_eq!(cell(p2, "hank"), Some(b"hank".to_vec()));
    for (db, other) in [(p1, "zed"), (p3, "alice"), (p1, "hank"), (p3, "hank")] {
        assert_eq!(cell(db, other), None, "{other}");
    }
}

#[test]
fn sessions_and_snapshots_live_on_their_own_partition_of_a_node() {
    // Three sessions per token on a node, across its partitions.
    let mut limits = crate::serve::server_tests::limits();
    limits.leases.max_per_token = 3;
    let t = Several::start(limits);
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        put(c, "zed", "1").await;
        put(c, "alice", "1").await;
        // A transaction on p3, a's second partition, through every node.
        let id = begin(b, "zed", None).await.unwrap();
        assert!(id.starts_with("p3:"), "{id}");
        txn_write(c, &id, vec![put_row("zoe", "2")], None)
            .await
            .unwrap();
        // The same id under a's other partition is no session: each has its own table.
        let as_p1 = id.replacen("p3:", "p1:", 1);
        let e = commit(a, &as_p1, None).await.unwrap_err();
        assert_eq!(code(&e), "NotFound", "{e}");
        commit(a, &id, None).await.unwrap();
        assert_eq!(get_row(b, "zoe").await.unwrap(), Some(b"2".to_vec()));
        // A snapshot on p1, read through a scan at it from another node (a's p1 pinned for
        // the stream), after a write it mustn't see.
        let snap = snapshots(c)
            .await
            .open(OpenSnapshotRequest {
                durable: false,
                route: Some(RoutingKey {
                    key: Some(routing_key::Key::Partition("p1".into())),
                }),
            })
            .await
            .unwrap()
            .into_inner()
            .snapshot_id;
        put(a, "alice", "2").await;
        let mut pages = data(b)
            .await
            .scan(ScanRequest {
                table: "users".into(),
                range: Some(KeyRange {
                    range: Some(key_range::Range::Prefix(b"al".to_vec().into())),
                }),
                selection: Some(Selection {
                    snapshot_id: snap.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let mut values = Vec::new();
        while let Some(page) = pages.message().await.unwrap() {
            values.extend(
                page.rows
                    .iter()
                    .flat_map(|r| r.cells.iter().map(|c| c.value.to_vec())),
            );
        }
        assert_eq!(values, [b"1".to_vec()]);
        // a holds, for this token: p3's committed transaction's record, p1's snapshot, and
        // now a transaction on p1. That's a's cap of 3, across both partitions, so another
        // on p3 is refused; b's p2 counts on its own.
        let _p1 = begin(c, "alice", None).await.unwrap();
        let e = begin(b, "zed", None).await.unwrap_err();
        assert_eq!(code(&e), "Overloaded", "{e}");
        assert!(e.message().contains("per-token"), "{e}");
        begin(c, "hank", None).await.unwrap();
    });
}

#[test]
fn a_feed_through_any_node_follows_every_partition_of_a_node() {
    let t = Several::start(crate::serve::server_tests::limits());
    let [a, _, c] = t.ports;
    t.rt.block_on(async {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{c}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut events = FeedClient::new(channel)
            .subscribe(SubscribeRequest {
                tables: vec!["users".into()],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        for row in ["alice", "hank", "zed"] {
            put(a, row, "1").await;
        }
        let mut seen = std::collections::BTreeSet::new();
        while seen.len() < 3 {
            let e: FeedEvent = tokio::time::timeout(Duration::from_secs(10), events.message())
                .await
                .expect("a commit within 10 s")
                .unwrap()
                .unwrap();
            if let Some(feed_event::Event::Commit(_)) = e.event {
                seen.insert(e.partition);
            }
        }
        assert_eq!(seen.into_iter().collect::<Vec<_>>(), ["p1", "p2", "p3"]);
    });
}

#[test]
fn a_nodes_metrics_label_each_of_its_partitions() {
    let t = Several::start(crate::serve::server_tests::limits());
    let text = t.rt.block_on(async {
        let channel =
            tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{}", t.ports[0]))
                .unwrap()
                .connect()
                .await
                .unwrap();
        crate::serve::proto::admin_client::AdminClient::new(channel)
            .metrics(crate::serve::proto::Empty {})
            .await
            .unwrap()
            .into_inner()
            .text
    });
    for p in ["p1", "p3"] {
        let label = format!("pigeonhole_partition=\"{p}\"");
        for family in [
            "pigeonhole_serve_partition_unpinned_shard_threads",
            "pigeonhole_serve_partition_unpinned_compaction_threads",
            "pigeonhole_serve_partition_memtable_bytes",
            "pigeonhole_serve_partition_block_cache_bytes",
            "pigeonhole_serve_partition_row_cache_bytes",
        ] {
            assert!(
                text.lines()
                    .any(|l| l.starts_with(family) && l.contains(&label)),
                "{family} for {p}:\n{text}"
            );
        }
    }
    assert!(!text.contains("pigeonhole_partition=\"p2\""), "{text}");
}

#[test]
fn a_partition_that_fails_to_open_fails_the_node_and_closes_the_others() {
    // cli692's M2: p1 opens, p3's file is missing; the node refuses to start, naming p3,
    // and p1 is closed again (so it opens once more, where an open handle would hold it).
    let dir = test_dir();
    test_partition(&dir, "p1").close().unwrap();
    let mine = [("p1".to_owned(), 1), ("p3".to_owned(), 1)];
    let shares = crate::serve::placement::split(2, 2, 2);
    let mut log = Vec::new();
    let Err(e) = super::super::open_partitions(
        &dir,
        Some("c1"),
        &mine,
        &shares,
        &[crate::serve::placement::Memory::defaults(1); 2],
        |_, _| pigeonhole::Options::default(),
        &mut log,
    ) else {
        panic!("a missing partition opened");
    };
    let e = format!("{e:?}");
    assert!(e.contains("p3"), "{e}");
    pigeonhole::Pigeonhole::open(dir.join("p1.phdb"), pigeonhole::Options::default())
        .unwrap()
        .close()
        .unwrap();
}

#[test]
fn a_partitions_share_of_the_budget_keeps_its_feeds_shard_count() {
    // ICR 0132 through 8a's split: a partition whose change feed recorded 3 shards keeps
    // them under a share of 1, rather than being resharded and its feed's history ended.
    let dir = test_dir();
    let db = pigeonhole::Pigeonhole::open(
        dir.join("p1.phdb"),
        pigeonhole::Options::default()
            .create_if_missing(true)
            .shards(3)
            .change_feed(pigeonhole::FeedRetention::bytes(1 << 20)),
    )
    .unwrap();
    identity::write(
        &db,
        &identity::Identity {
            cluster_id: "c1".into(),
            partition_id: "p1".into(),
            epoch: 1,
        },
    )
    .unwrap();
    db.close().unwrap();
    let share = crate::serve::placement::split(1, 1, 1)[0];
    assert_eq!(share.shards, 1);
    let mine = [("p1".to_owned(), 1)];
    let mut log = Vec::new();
    let opened = super::super::open_partitions(
        &dir,
        Some("c1"),
        &mine,
        &[share],
        &[crate::serve::placement::Memory::defaults(1)],
        |share, _| {
            super::super::partition_options(
                crate::serve::config::ShardSpin::Micros(0),
                None,
                None,
                share,
            )
        },
        &mut log,
    )
    .unwrap();
    assert_eq!(opened[0].db.info().unwrap().shards, 3);
    // The startup line reports its threads.
    let log = String::from_utf8(log).unwrap();
    assert!(log.contains("phdb serve: partition p1: "), "{log}");
    for o in opened {
        o.db.close().unwrap();
    }
}
