//! Three cluster nodes on loopback ports, in one process (D317 ticket 1): the map is set on
//! one, pushed to the others, compared-and-set, refused when it reshapes a placement or goes
//! back, pulled by a node that missed it, kept across a restart, reported as divergent, and
//! `Status` reads a partition's identity row.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::proto::{
    ClusterMap, GetMapRequest, Node as MapNode, Partition, SetMapRequest, SplitKey, StatusRequest,
    TablePlacement, split_key,
};
use super::{MapState, Node, Peers, identity, read_map, routes};
use crate::serve::auth::Tokens;

fn test_dir() -> PathBuf {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "phdb-cluster-{}-{nanos}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A node over `dir` (its persisted map loaded, as `run` loads it), without tokens or TLS.
fn node(dir: &Path, id: &str) -> Arc<Node> {
    let mut state = MapState::default();
    if let Some(m) = read_map(dir).unwrap() {
        let json = super::map::canonical_json(&m);
        state.map = Some((super::map::canonical(&m), json));
    }
    Node::new(
        dir,
        id,
        true,
        Tokens::parse("", "none").unwrap(),
        Peers {
            token: None,
            scheme: "http",
            ca: None,
            push_timeout: Duration::from_millis(500),
        },
        state,
    )
}

/// Serves `node` on `listener` until the sender is dropped or fired.
fn serve(
    rt: &tokio::runtime::Runtime,
    node: &Arc<Node>,
    listener: std::net::TcpListener,
) -> tokio::sync::oneshot::Sender<()> {
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let app = routes(Arc::clone(node), None, 16 << 20);
    rt.spawn(async move {
        listener.set_nonblocking(true).unwrap();
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        crate::serve::listener::serve(
            listener,
            None,
            app,
            async {
                let _ = stopped.await;
            },
            Duration::from_millis(100),
            None,
            crate::serve::listener::Bounds {
                connections: crate::serve::listener::Connections::new(64),
                first_byte: Duration::from_secs(5),
            },
        )
        .await;
    });
    stop
}

fn text(s: &str) -> SplitKey {
    SplitKey {
        key: Some(split_key::Key::Text(s.into())),
    }
}

/// The map: nodes a, b and c at `ports`, partitions p1 (a), p2 (b), p3 (c); `users` split
/// at "m" over p1 and p2.
fn the_map(version: u64, ports: &[u16; 3]) -> ClusterMap {
    ClusterMap {
        cluster_id: "c1".into(),
        map_version: version,
        nodes: ["a", "b", "c"]
            .iter()
            .zip(ports)
            .map(|(id, port)| MapNode {
                id: (*id).into(),
                address: format!("127.0.0.1:{port}"),
            })
            .collect(),
        partitions: [("p1", "a"), ("p2", "b"), ("p3", "c")]
            .iter()
            .map(|(id, node)| Partition {
                id: (*id).into(),
                node: (*node).into(),
                epoch: 1,
            })
            .collect(),
        tables: vec![TablePlacement {
            name: "users".into(),
            group: String::new(),
            families: Vec::new(),
            splits: vec![text("m")],
            partitions: vec!["p1".into(), "p2".into()],
        }],
    }
}

async fn client(
    port: u16,
) -> super::proto::cluster_client::ClusterClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    super::proto::cluster_client::ClusterClient::new(channel)
}

async fn version(port: u16) -> u64 {
    client(port)
        .await
        .status(StatusRequest {})
        .await
        .unwrap()
        .into_inner()
        .map_version
}

/// Waits for `cond` (a node's state settling after a background push or pull).
async fn eventually(what: &str, mut cond: impl AsyncFnMut() -> bool) {
    for _ in 0..100 {
        if cond().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{what}: never happened");
}

#[test]
fn a_map_set_on_one_node_reaches_the_others_and_survives_restarts() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let dirs = [test_dir(), test_dir(), test_dir()];
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let mut listeners = listeners.into_iter();
    let a = node(&dirs[0], "a");
    let b = node(&dirs[1], "b");
    let _stop_a = serve(&rt, &a, listeners.next().unwrap());
    let _stop_b = serve(&rt, &b, listeners.next().unwrap());
    // Node c is down: its port is bound but nothing answers, so a push to it times out.
    let c_listener = listeners.next().unwrap();

    rt.block_on(async {
        // The first map: compare-and-set from version 0. b takes the push; c misses it.
        let r = client(ports[0])
            .await
            .set_map(SetMapRequest {
                expected_map_version: 0,
                map: Some(the_map(1, &ports)),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.map_version, 1);
        let pushed: Vec<(&str, bool)> = r.pushed.iter().map(|p| (p.node.as_str(), p.ok)).collect();
        assert_eq!(pushed, [("b", true), ("c", false)], "{r:?}");
        assert_eq!(version(ports[1]).await, 1);
        assert_eq!(read_map(&dirs[1]).unwrap().unwrap().map_version, 1);

        // A stale compare-and-set is a Conflict (ABORTED), not applied.
        let e = client(ports[1])
            .await
            .set_map(SetMapRequest {
                expected_map_version: 0,
                map: Some(the_map(2, &ports)),
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::Aborted, "{e:?}");
        assert!(e.message().contains("version 1"), "{e:?}");
        assert_eq!(version(ports[1]).await, 1);

        // Reshaping a placement is refused; so is a map that goes back.
        let mut reshaped = the_map(2, &ports);
        reshaped.tables[0].splits = vec![text("n")];
        let e = client(ports[0])
            .await
            .set_map(SetMapRequest {
                expected_map_version: 1,
                map: Some(reshaped),
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e:?}");
        assert!(e.message().contains("fixed once written"), "{e:?}");

        // A divergent map (version 1, different content) pushed to b is refused, and b
        // reports who pushed it.
        let mut other = the_map(1, &ports);
        other.partitions[2].epoch = 2;
        let mut req = tonic::Request::new(super::proto::PushMapRequest { map: Some(other) });
        req.metadata_mut()
            .insert(super::NODE_ID_HEADER, "x".parse().unwrap());
        let e = client(ports[1]).await.push_map(req).await.unwrap_err();
        assert!(e.message().contains(super::DIVERGENT), "{e:?}");
        let s = client(ports[1])
            .await
            .status(StatusRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(s.divergent, ["x"]);

        // Version 2 (a relocation of p3, with a new epoch): pushed to b, missed by c.
        let mut v2 = the_map(2, &ports);
        v2.partitions[2].epoch = 2;
        client(ports[0])
            .await
            .set_map(SetMapRequest {
                expected_map_version: 1,
                map: Some(v2.clone()),
            })
            .await
            .unwrap();
        assert_eq!(version(ports[1]).await, 2);
        // A newer map clears the divergence.
        assert!(
            client(ports[1])
                .await
                .status(StatusRequest {})
                .await
                .unwrap()
                .into_inner()
                .divergent
                .is_empty()
        );
    });

    // Node c comes up with version 1 on disk (an older copy), and catches up from its
    // peers at start.
    std::fs::write(
        dirs[2].join(super::MAP_FILE),
        super::map::canonical_json(&the_map(1, &ports)),
    )
    .unwrap();
    let c = node(&dirs[2], "c");
    let _stop_c = serve(&rt, &c, c_listener);
    rt.block_on(async {
        let n = Arc::clone(&c);
        n.sync_on_start().await;
        assert_eq!(version(ports[2]).await, 2);
        assert_eq!(read_map(&dirs[2]).unwrap().unwrap().map_version, 2);
    });

    // A peer's call saying its map is newer makes the node pull: set version 3 on a while
    // c is unreachable to the push (a timeout too short to connect), then call c as a.
    rt.block_on(async {
        let mut v3 = the_map(3, &ports);
        v3.partitions[2].epoch = 2;
        // Installed on a alone (no push), as if c had missed it.
        a.push_map(v3, None).unwrap();
        assert_eq!(version(ports[2]).await, 2);
        let mut req = tonic::Request::new(GetMapRequest {});
        req.metadata_mut()
            .insert(super::MAP_VERSION_HEADER, "3".parse().unwrap());
        req.metadata_mut()
            .insert(super::NODE_ID_HEADER, "a".parse().unwrap());
        client(ports[2]).await.get_map(req).await.unwrap();
        eventually("c pulls version 3 from a", async || {
            version(ports[2]).await == 3
        })
        .await;
    });

    // A restart keeps the map.
    let b2 = node(&dirs[1], "b");
    assert_eq!(b2.state().version(), 2);
    for d in &dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn status_reads_a_partitions_identity_row_and_checks_it_against_the_map() {
    let dir = test_dir();
    let db = pigeonhole::Pigeonhole::open(
        dir.join("p1.phdb"),
        pigeonhole::Options::default().create_if_missing(true),
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
    // Another partition, or another cluster, is refused; the epoch may rise, never fall.
    let other = |c: &str, p: &str, epoch| identity::Identity {
        cluster_id: c.into(),
        partition_id: p.into(),
        epoch,
    };
    assert!(identity::write(&db, &other("c1", "p2", 1)).is_err());
    assert!(identity::write(&db, &other("c2", "p1", 1)).is_err());
    identity::write(&db, &other("c1", "p1", 2)).unwrap();
    assert!(identity::write(&db, &other("c1", "p1", 1)).is_err());
    db.close().unwrap();
    // A file with no identity row.
    pigeonhole::Pigeonhole::open(
        dir.join("p9.phdb"),
        pigeonhole::Options::default().create_if_missing(true),
    )
    .unwrap()
    .close()
    .unwrap();

    let n = node(&dir, "a");
    let mut m = the_map(1, &[7001, 7002, 7003]);
    // p2 placed here too, with no file in the directory.
    m.partitions[1].node = "a".into();
    n.push_map(m, None).unwrap();
    let s = n.status();
    assert_eq!((s.node_id.as_str(), s.map_version), ("a", 1));
    let parts: Vec<(&str, &str, u64, bool, &str)> = s
        .partitions
        .iter()
        .map(|p| {
            (
                p.id.as_str(),
                p.file.as_str(),
                p.epoch,
                p.ok,
                p.error.as_str(),
            )
        })
        .collect();
    // p1's file is at epoch 2 while the map says 1: reported, not ok.
    assert_eq!(parts[0].0, "p1");
    assert_eq!(parts[0].1, "p1.phdb", "relative to the directory");
    assert_eq!(parts[0].2, 2);
    assert!(!parts[0].3 && parts[0].4.contains("epoch"), "{parts:?}");
    assert_eq!(parts[1].0, "p9");
    assert!(
        !parts[1].3 && parts[1].4.contains("init-partition"),
        "{parts:?}"
    );
    // The map's partition with no file is reported, not left out (cli692's N1).
    assert_eq!(parts.len(), 3, "{parts:?}");
    assert_eq!((parts[2].0, parts[2].1), ("p2", "p2.phdb"));
    assert!(
        !parts[2].3 && parts[2].4.contains("has no p2.phdb"),
        "{parts:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_node_that_missed_a_drop_and_a_recreate_still_catches_up() {
    // v2 dropped `users` and p3 (at epoch 5); v3 created `users` again with other splits and
    // p3 back at epoch 1. Each step was a valid change where it was set; a node still at v1
    // takes v3 from a peer all the same (cli692's R1), and refuses only another cluster's
    // map or one no newer.
    let dir = test_dir();
    let n = node(&dir, "a");
    let ports = [7001, 7002, 7003];
    let mut v1 = the_map(1, &ports);
    v1.partitions[2].epoch = 5;
    n.push_map(v1, None).unwrap();
    let mut v3 = the_map(3, &ports);
    v3.tables[0].splits = vec![text("q")];
    assert_eq!(n.push_map(v3.clone(), Some("b")).unwrap().map_version, 3);
    assert_eq!(read_map(&dir).unwrap().unwrap(), super::map::canonical(&v3));
    assert!(n.push_map(the_map(2, &ports), Some("b")).is_err());
    let mut other = the_map(4, &ports);
    other.cluster_id = "c2".into();
    assert!(n.push_map(other, Some("b")).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_cluster_token_alone_pushes_and_an_admin_token_alone_sets() {
    use crate::serve::auth::{Scope, Tokens};
    let dir = test_dir();
    let mut n = Arc::try_unwrap(node(&dir, "a")).unwrap();
    n.tokens = Tokens::parse("admin adm1n\ncluster n0de\nread r3ad\n", "t").unwrap();
    let who = |t: &str| n.tokens.caller(t).unwrap();
    let readers = super::READERS;
    // PushMap: cluster only.
    assert!(n.allowed(&who("n0de"), &[Scope::Cluster], "push").is_ok());
    assert!(n.allowed(&who("adm1n"), &[Scope::Cluster], "push").is_err());
    // SetMap: admin only.
    assert!(n.allowed(&who("adm1n"), &[Scope::Admin], "set").is_ok());
    assert!(n.allowed(&who("n0de"), &[Scope::Admin], "set").is_err());
    assert!(n.allowed(&who("r3ad"), &[Scope::Admin], "set").is_err());
    // GetMap and Status: read (so admin) or cluster.
    for t in ["r3ad", "adm1n", "n0de"] {
        assert!(n.allowed(&who(t), readers, "read").is_ok(), "{t}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A data-serving cluster node in this process (D317 ticket 2): its map in `dir`, the
/// partition `local` (stamped and holding table `users`) if it has one, served on
/// `listener` through the same router `phdb serve --cluster` builds.
fn data_node(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    id: &str,
    local: Option<&str>,
    listener: std::net::TcpListener,
) -> (
    tokio::sync::oneshot::Sender<()>,
    Option<pigeonhole::Pigeonhole>,
) {
    data_node_with(
        rt,
        dir,
        id,
        local,
        listener,
        crate::serve::server_tests::limits(),
    )
}

/// [`data_node`] with its own limits.
fn data_node_with(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    id: &str,
    local: Option<&str>,
    listener: std::net::TcpListener,
    limits: crate::serve::config::Limits,
) -> (
    tokio::sync::oneshot::Sender<()>,
    Option<pigeonhole::Pigeonhole>,
) {
    use crate::serve::{Shared, router::Router, service::Metrics, service::Service};
    let node = node(dir, id);
    let metrics = Arc::new(Metrics::default());
    let db = local.map(|p| {
        let db = pigeonhole::Pigeonhole::open(
            dir.join(format!("{p}.phdb")),
            pigeonhole::Options::default().create_if_missing(true),
        )
        .unwrap();
        identity::write(
            &db,
            &identity::Identity {
                cluster_id: "c1".into(),
                partition_id: p.into(),
                epoch: 1,
            },
        )
        .unwrap();
        db.table("users")
            .unwrap()
            .family("f", pigeonhole::Family::default())
            .create_if_missing()
            .unwrap();
        db
    });
    let service = match &db {
        Some(db) => Service::new(db.clone(), limits, 2, Arc::clone(&metrics)),
        None => Service::routing_only(limits, 2, Arc::clone(&metrics)),
    };
    let shared = Arc::new(Shared {
        router: Router::cluster(service, node, local.map(str::to_owned)),
        tokens: Tokens::parse("", "none").unwrap(),
        limits,
        metrics,
    });
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let app = crate::serve::http::router(shared);
    rt.spawn(async move {
        listener.set_nonblocking(true).unwrap();
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        crate::serve::listener::serve(
            listener,
            None,
            app,
            async {
                let _ = stopped.await;
            },
            Duration::from_millis(100),
            None,
            crate::serve::listener::Bounds {
                connections: crate::serve::listener::Connections::new(64),
                first_byte: Duration::from_secs(5),
            },
        )
        .await;
    });
    (stop, db)
}

async fn data(port: u16) -> super::proto::data_client::DataClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    super::proto::data_client::DataClient::new(channel)
}

fn put_row(row: &str, value: &str) -> super::proto::RowWrites {
    use super::proto::{Put, RowKey, Value, Write, value, write};
    super::proto::RowWrites {
        row: Some(RowKey {
            table: "users".into(),
            row: row.as_bytes().to_vec().into(),
        }),
        writes: vec![Write {
            op: Some(write::Op::Put(Put {
                family: "f".into(),
                qualifier: b"q".to_vec().into(),
                timestamp: None,
                value: Some(Value {
                    kind: Some(value::Kind::Bytes(value.as_bytes().to_vec().into())),
                }),
            })),
        }],
    }
}

async fn get_row(port: u16, row: &str) -> Result<Option<Vec<u8>>, tonic::Status> {
    let reply = data(port)
        .await
        .get(super::proto::GetRequest {
            table: "users".into(),
            row: row.as_bytes().to_vec().into(),
            family: "f".into(),
            qualifier: b"q".to_vec().into(),
            ..Default::default()
        })
        .await?;
    Ok(reply.into_inner().cell.map(|c| c.value.to_vec()))
}

/// The code a status carries in its `ErrorDetail`.
fn code(status: &tonic::Status) -> String {
    crate::serve::errors::ApiError::from_status(status).map_or_else(String::new, |e| e.code)
}

#[test]
fn a_cluster_routes_and_forwards_single_key_calls_to_the_partition_that_holds_them() {
    // a serves p1 (users < "m"), b serves p2 (users >= "m"), c serves nothing and routes.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let mut map = the_map(1, &[ports[0], ports[1], ports[2]]);
    map.partitions.retain(|p| p.id != "p3");
    let dirs: Vec<PathBuf> = (0..3).map(|_| test_dir()).collect();
    for dir in &dirs {
        super::write_map(dir, &super::map::canonical_json(&map)).unwrap();
    }
    let mut it = listeners.into_iter();
    let (stop_a, db_a) = data_node(&rt, &dirs[0], "a", Some("p1"), it.next().unwrap());
    let (stop_b, db_b) = data_node(&rt, &dirs[1], "b", Some("p2"), it.next().unwrap());
    let (_stop_c, _) = data_node(&rt, &dirs[2], "c", None, it.next().unwrap());
    let (a, b, c) = (ports[0], ports[1], ports[2]);
    rt.block_on(async {
        // Writes through the router land in their partitions; reads through any node agree.
        for (entry, row, value) in [(c, "alice", "1"), (c, "zed", "2"), (a, "nina", "3")] {
            data(entry)
                .await
                .mutate(super::proto::MutateRequest {
                    writes: Some(put_row(row, value)),
                    durability: 0,
                })
                .await
                .unwrap_or_else(|e| panic!("{row} through {entry}: {e}"));
        }
        for port in [a, b, c] {
            assert_eq!(get_row(port, "alice").await.unwrap(), Some(b"1".to_vec()));
            assert_eq!(get_row(port, "zed").await.unwrap(), Some(b"2".to_vec()));
            assert_eq!(get_row(port, "nina").await.unwrap(), Some(b"3".to_vec()));
        }
        // A batch across partitions is refused before anything is written.
        let e = data(c)
            .await
            .batch(super::proto::BatchRequest {
                rows: vec![put_row("bob", "x"), put_row("yan", "y")],
                durability: 0,
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "CrossPartition", "{e}");
        assert_eq!(get_row(c, "bob").await.unwrap(), None);
        assert_eq!(get_row(c, "yan").await.unwrap(), None);
        // A batch inside one partition is forwarded whole.
        data(a)
            .await
            .batch(super::proto::BatchRequest {
                rows: vec![put_row("xavi", "4"), put_row("yan", "5")],
                durability: 0,
            })
            .await
            .unwrap();
        assert_eq!(get_row(b, "yan").await.unwrap(), Some(b"5".to_vec()));
        // A scan fans out across the partitions (ticket 4a), even from a node with none.
        let pages = scan_pages(
            c,
            super::proto::ScanRequest {
                table: "users".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let keys: Vec<_> = pages
            .iter()
            .flat_map(|p| p.rows.iter().map(|r| r.key.to_vec()))
            .collect();
        assert!(keys.windows(2).all(|w| w[0] < w[1]), "{keys:?}");
        assert!(keys.contains(&b"alice".to_vec()) && keys.contains(&b"zed".to_vec()));
        // Calls a later ticket brings are refused, naming it.
        let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{c}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let e = super::proto::graph_client::GraphClient::new(channel)
            .traverse(super::proto::TraverseRequest {
                table: "users".into(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "Unsupported", "{e}");
        assert!(e.message().contains("ticket 4"), "{e}");
    });
    // Each row is in its partition's file, and only there.
    let cell = |db: &pigeonhole::Pigeonhole, row: &str| {
        db.table("users")
            .unwrap()
            .open()
            .unwrap()
            .get(row.as_bytes(), "f", b"q")
            .unwrap()
            .map(|c| c.value().to_vec())
    };
    let (db_a, db_b) = (db_a.unwrap(), db_b.unwrap());
    assert_eq!(cell(&db_a, "alice"), Some(b"1".to_vec()));
    assert_eq!(cell(&db_a, "zed"), None);
    assert_eq!(cell(&db_b, "zed"), Some(b"2".to_vec()));
    assert_eq!(cell(&db_b, "alice"), None);
    // The owner of p2 stops: a read through the router is PartitionUnavailable (retry-safe),
    // and a write refused before it reached the owner is too.
    drop(stop_b);
    rt.block_on(async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let e = get_row(c, "zed").await.unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        let e = data(c)
            .await
            .mutate(super::proto::MutateRequest {
                writes: Some(put_row("zoe", "6")),
                durability: 0,
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        // p1's rows are still served.
        assert_eq!(get_row(c, "alice").await.unwrap(), Some(b"1".to_vec()));
    });
    drop(stop_a);
    let _ = db_a.close();
    let _ = db_b.close();
}

#[test]
fn a_node_whose_map_moved_a_partition_away_answers_wrong_partition_and_the_sender_catches_up() {
    // v1: p1 on a, p2 on b, c routes. v2 moves p2 to a (epoch 2) and reaches a and b only.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let mut v1 = the_map(1, &[ports[0], ports[1], ports[2]]);
    v1.partitions.retain(|p| p.id != "p3");
    let dirs: Vec<PathBuf> = (0..3).map(|_| test_dir()).collect();
    for dir in &dirs {
        super::write_map(dir, &super::map::canonical_json(&v1)).unwrap();
    }
    let mut it = listeners.into_iter();
    let (_stop_a, db_a) = data_node(&rt, &dirs[0], "a", Some("p1"), it.next().unwrap());
    let (_stop_b, db_b) = data_node(&rt, &dirs[1], "b", Some("p2"), it.next().unwrap());
    let (_stop_c, _) = data_node(&rt, &dirs[2], "c", None, it.next().unwrap());
    let (a, b, c) = (ports[0], ports[1], ports[2]);
    let mut v2 = v1.clone();
    v2.map_version = 2;
    for p in &mut v2.partitions {
        if p.id == "p2" {
            p.node = "a".into();
            p.epoch = 2;
        }
    }
    rt.block_on(async {
        for port in [a, b] {
            client(port)
                .await
                .push_map(super::proto::PushMapRequest {
                    map: Some(v2.clone()),
                })
                .await
                .unwrap();
        }
        assert_eq!(version(c).await, 1);
        // c routes "zed" to b by its v1. b's v2 says p2 is a's: WrongPartition, though b
        // still has p2's file open, so b writes nothing. c pulls v2 from b and routes to a,
        // whose v2 gives it p2, which it hasn't open (relocation is ticket 6).
        let e = data(c)
            .await
            .mutate(super::proto::MutateRequest {
                writes: Some(put_row("zed", "moved")),
                durability: 0,
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        assert!(e.message().contains("ticket 6"), "{e}");
        assert_eq!(version(c).await, 2, "c pulled the newer map");
        // A direct call to b is refused the same way: its map moved p2 away.
        let e = get_row(b, "zed").await.unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        // p1 is still served wherever the call enters.
        data(c)
            .await
            .mutate(super::proto::MutateRequest {
                writes: Some(put_row("alice", "1")),
                durability: 0,
            })
            .await
            .unwrap();
        assert_eq!(get_row(b, "alice").await.unwrap(), Some(b"1".to_vec()));
    });
    let db_b = db_b.unwrap();
    assert_eq!(
        db_b.table("users")
            .unwrap()
            .open()
            .unwrap()
            .get(b"zed", "f", b"q")
            .unwrap()
            .map(|c| c.value().to_vec()),
        None,
        "the node p2 moved away from wrote nothing"
    );
    let _ = db_b.close();
    let _ = db_a.map(pigeonhole::Pigeonhole::close);
}

/// The cluster model test's random source: splitmix64, seeded and reproducible.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[test]
fn a_seeded_workload_through_random_entry_nodes_answers_as_one_database_does() {
    // D317 §12's model test, single-partition half: every single-key call and every
    // one-partition batch answers as a single database would, whichever node it enters;
    // every cross-partition batch is refused with nothing written. The oracle is a map.
    let seed: u64 = std::env::var("PHDB_CLUSTER_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        });
    eprintln!("cluster model seed: {seed} (PHDB_CLUSTER_SEED={seed} replays it)");
    let mut rng = Rng(seed);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let mut map = the_map(1, &[ports[0], ports[1], ports[2]]);
    map.partitions.retain(|p| p.id != "p3");
    let dirs: Vec<PathBuf> = (0..3).map(|_| test_dir()).collect();
    for dir in &dirs {
        super::write_map(dir, &super::map::canonical_json(&map)).unwrap();
    }
    let mut it = listeners.into_iter();
    let (_a, db_a) = data_node(&rt, &dirs[0], "a", Some("p1"), it.next().unwrap());
    let (_b, db_b) = data_node(&rt, &dirs[1], "b", Some("p2"), it.next().unwrap());
    let (_c, _) = data_node(&rt, &dirs[2], "c", None, it.next().unwrap());
    // Rows on both sides of the split at "m".
    let rows: Vec<String> = ["a", "f", "k", "lz", "m", "n", "s", "z"]
        .iter()
        .flat_map(|p| (0..3).map(move |i| format!("{p}{i}")))
        .collect();
    let part = |row: &str| {
        if row.as_bytes() < b"m".as_slice() {
            1
        } else {
            2
        }
    };
    let mut model: std::collections::HashMap<String, String> = Default::default();
    let ops = std::env::var("PIGEONHOLE_STRESS_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300u64);
    rt.block_on(async {
        let mut clients = Vec::new();
        for &port in &ports {
            clients.push(data(port).await);
        }
        for i in 0..ops {
            let entry = rng.below(3) as usize;
            let c = &mut clients[entry];
            let at = format!("seed {seed}, op {i}, entry node {entry}");
            match rng.below(4) {
                0 | 1 => {
                    let row = &rows[rng.below(rows.len() as u64) as usize];
                    let value = format!("v{i}");
                    c.mutate(super::proto::MutateRequest {
                        writes: Some(put_row(row, &value)),
                        durability: 0,
                    })
                    .await
                    .unwrap_or_else(|e| panic!("{at}: put {row}: {e}"));
                    model.insert(row.clone(), value);
                }
                2 => {
                    let row = &rows[rng.below(rows.len() as u64) as usize];
                    let got = c
                        .get(super::proto::GetRequest {
                            table: "users".into(),
                            row: row.as_bytes().to_vec().into(),
                            family: "f".into(),
                            qualifier: b"q".to_vec().into(),
                            ..Default::default()
                        })
                        .await
                        .unwrap_or_else(|e| panic!("{at}: get {row}: {e}"))
                        .into_inner()
                        .cell
                        .map(|c| String::from_utf8(c.value.to_vec()).unwrap());
                    assert_eq!(got.as_ref(), model.get(row), "{at}: get {row}");
                }
                _ => {
                    // A batch of two to four rows: one partition commits whole, two refuse.
                    let n = 2 + rng.below(3) as usize;
                    let picked: Vec<String> = (0..n)
                        .map(|_| rows[rng.below(rows.len() as u64) as usize].clone())
                        .collect();
                    let value = format!("b{i}");
                    let reply = c
                        .batch(super::proto::BatchRequest {
                            rows: picked.iter().map(|r| put_row(r, &value)).collect(),
                            durability: 0,
                        })
                        .await;
                    let one = picked.iter().all(|r| part(r) == part(&picked[0]));
                    match reply {
                        Ok(_) if one => {
                            for r in &picked {
                                model.insert(r.clone(), value.clone());
                            }
                        }
                        Err(e) if !one => {
                            assert_eq!(code(&e), "CrossPartition", "{at}: {e}");
                        }
                        other => panic!("{at}: batch {picked:?}: {other:?}"),
                    }
                }
            }
        }
    });
    // Every row is in its own partition's file only, holding the model's value.
    let read = |db: &pigeonhole::Pigeonhole, row: &str| {
        db.table("users")
            .unwrap()
            .open()
            .unwrap()
            .get(row.as_bytes(), "f", b"q")
            .unwrap()
            .map(|c| String::from_utf8(c.value().to_vec()).unwrap())
    };
    let (db_a, db_b) = (db_a.unwrap(), db_b.unwrap());
    for row in &rows {
        let (home, other) = if part(row) == 1 {
            (&db_a, &db_b)
        } else {
            (&db_b, &db_a)
        };
        assert_eq!(
            read(home, row).as_ref(),
            model.get(row),
            "seed {seed}: {row}"
        );
        assert_eq!(
            read(other, row),
            None,
            "seed {seed}: {row} in the wrong file"
        );
    }
    let _ = db_a.close();
    let _ = db_b.close();
}

#[test]
fn a_partition_opens_with_the_servers_shard_poll() {
    // cli692's M1 on #1686: a cluster node's partition takes `--shard-spin` as a single node's
    // database does, so its default runs no poll (D198 amendment 4).
    let dir = test_dir();
    let options = super::partition_options(crate::serve::config::ShardSpin::Micros(0), None)
        .create_if_missing(true);
    let db = pigeonhole::Pigeonhole::open(dir.join("p1.phdb"), options).unwrap();
    let t = db
        .table("users")
        .unwrap()
        .family("f", pigeonhole::Family::default())
        .create_if_missing()
        .unwrap();
    for i in 0..200u32 {
        t.mutate(format!("r{i}").as_bytes())
            .put("f", b"q", b"v")
            .commit()
            .unwrap();
    }
    let spin = db
        .internal_metrics()
        .into_iter()
        .find(|(n, _)| *n == "shard_spin_time_nanos")
        .map_or(0, |(_, v)| v);
    assert_eq!(spin, 0, "a partition opened with --shard-spin 0 polled");
    drop(t);
    db.close().unwrap();
}

// ---- D317 ticket 4a: fan-out scans and multi-gets ----

/// The three-way map: `users` split at "g" and "m" over p1 (a), p2 (b) and p3 (c).
fn three_way(ports: &[u16; 3]) -> ClusterMap {
    let mut m = the_map(1, ports);
    m.tables[0].splits = vec![text("g"), text("m")];
    m.tables[0].partitions = vec!["p1".into(), "p2".into(), "p3".into()];
    m
}

/// Three data nodes over [`three_way`], each serving its partition; their stop handles,
/// ports and partition databases.
fn three_nodes(
    rt: &tokio::runtime::Runtime,
) -> (
    Vec<tokio::sync::oneshot::Sender<()>>,
    Vec<u16>,
    Vec<pigeonhole::Pigeonhole>,
) {
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let map = three_way(&[ports[0], ports[1], ports[2]]);
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for ((l, id), p) in listeners
        .into_iter()
        .zip(["a", "b", "c"])
        .zip(["p1", "p2", "p3"])
    {
        let dir = test_dir();
        super::write_map(&dir, &super::map::canonical_json(&map)).unwrap();
        // Room for the stalled-stream test's scans: each holds a slot on its entry node and
        // one on the partition's.
        let limits = crate::serve::config::Limits {
            max_scans: 64,
            inflight_bytes: 256 << 20,
            // Longer than the stalled-stream test's bound, so a stall can't clear itself by
            // idling out.
            stream_idle: Duration::from_secs(30),
            ..crate::serve::server_tests::limits()
        };
        let (stop, db) = data_node_with(rt, &dir, id, Some(p), l, limits);
        stops.push(stop);
        dbs.push(db.unwrap());
    }
    (stops, ports, dbs)
}

/// A row's cells as a test compares them: qualifier and value, in the API's order.
type Cells = Vec<(Vec<u8>, Vec<u8>)>;

/// A multi-get page as a test compares it: its rows, and its `bytes`.
type Page = (Vec<(Vec<u8>, Cells)>, u64);

/// Rows (`key`, cells) from a reply's rows, a row a page cut joined with its rest.
fn joined(rows: impl IntoIterator<Item = super::proto::Row>) -> Vec<(Vec<u8>, Cells)> {
    let mut out: Vec<(Vec<u8>, Cells)> = Vec::new();
    for r in rows {
        let cells: Cells = r
            .cells
            .iter()
            .map(|c| (c.qualifier.to_vec(), c.value.to_vec()))
            .collect();
        match out.last_mut() {
            Some((k, have)) if *k == r.key[..] => have.extend(cells),
            _ => out.push((r.key.to_vec(), cells)),
        }
    }
    out
}

/// A library row as a test compares it.
fn lib_row(row: &pigeonhole::Row) -> (Vec<u8>, Cells) {
    (
        row.key().to_vec(),
        row.view()
            .iter()
            .map(|e| (e.qualifier.to_vec(), e.cell.value().to_vec()))
            .collect(),
    )
}

/// `row`'s writes: `n` cells `q0`.. with values of the given lengths.
fn cells_row(row: &[u8], values: &[usize], tag: u64) -> super::proto::RowWrites {
    use super::proto::{Put, RowKey, Value, Write, value, write};
    super::proto::RowWrites {
        row: Some(RowKey {
            table: "users".into(),
            row: row.to_vec().into(),
        }),
        writes: values
            .iter()
            .enumerate()
            .map(|(i, len)| Write {
                op: Some(write::Op::Put(Put {
                    family: "f".into(),
                    qualifier: format!("q{i}").into_bytes().into(),
                    timestamp: None,
                    value: Some(Value {
                        kind: Some(value::Kind::Bytes(
                            format!("{tag}:{}", "v".repeat(*len)).into_bytes().into(),
                        )),
                    }),
                })),
            })
            .collect(),
    }
}

/// A scan's pages from `port`, all of them.
async fn scan_pages(
    port: u16,
    r: super::proto::ScanRequest,
) -> Result<Vec<super::proto::ScanPage>, tonic::Status> {
    let mut stream = data(port).await.scan(r).await?.into_inner();
    let mut pages = Vec::new();
    while let Some(p) = stream.message().await? {
        pages.push(p);
    }
    Ok(pages)
}

#[test]
fn fanned_out_scans_and_multi_gets_answer_as_one_database_does() {
    // D317 §12's model test, fan-out half (ticket 4a), against one database holding every
    // row: scans over every kind of range, with row limits and byte budgets, resumed from
    // every page's `next` through another entry node; multi-gets of random key sets, page by
    // page, which must match the single database's pages exactly (the budget's merge across
    // partitions, simp724's S1 and M1).
    let seed: u64 = std::env::var("PHDB_CLUSTER_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        });
    eprintln!("cluster fan-out seed: {seed} (PHDB_CLUSTER_SEED={seed} replays it)");
    let mut rng = Rng(seed);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, dbs) = three_nodes(&rt);
    let oracle_dir = test_dir();
    let oracle = pigeonhole::Pigeonhole::open(
        oracle_dir.join("oracle.phdb"),
        pigeonhole::Options::default().create_if_missing(true),
    )
    .unwrap();
    let t = oracle
        .table("users")
        .unwrap()
        .family("f", pigeonhole::Family::default())
        .create_if_missing()
        .unwrap();
    // Rows on every side of both splits, the split keys themselves among them.
    let mut keys: Vec<Vec<u8>> = ["a", "c", "f", "g", "g0", "k", "lz", "m", "m0", "s", "z"]
        .iter()
        .flat_map(|p| (0..3).map(move |i| format!("{p}{i}").into_bytes()))
        .collect();
    keys.extend([b"g".to_vec(), b"m".to_vec(), b"".to_vec()]);
    keys.sort();
    keys.dedup();
    rt.block_on(async {
        for (i, key) in keys.iter().enumerate() {
            let values: Vec<usize> = (0..1 + rng.below(4))
                .map(|_| 1 + rng.below(120) as usize)
                .collect();
            let w = cells_row(key, &values, i as u64);
            let entry = ports[rng.below(3) as usize];
            data(entry)
                .await
                .mutate(super::proto::MutateRequest {
                    writes: Some(w.clone()),
                    durability: 0,
                })
                .await
                .unwrap_or_else(|e| panic!("seed {seed}: put {key:?}: {e}"));
            let mut m = t.mutate(key);
            for (q, len) in values.iter().enumerate() {
                m = m.put(
                    "f",
                    format!("q{q}").as_bytes(),
                    format!("{i}:{}", "v".repeat(*len)).as_bytes(),
                );
            }
            m.commit().unwrap();
        }
    });
    let pick = |rng: &mut Rng| keys[rng.below(keys.len() as u64) as usize].clone();
    let budgets = [300u64, 450, 700, 1500, 0];
    rt.block_on(async {
        // Scans.
        for trial in 0..60 {
            use super::proto::{Bound, Bounds, KeyRange, ScanRequest, key_range::Range};
            use std::ops::Bound as B;
            let at = format!("seed {seed}, scan {trial}");
            let (range, lo, hi): (Option<KeyRange>, B<Vec<u8>>, B<Vec<u8>>) = match rng.below(3) {
                0 => (None, B::Unbounded, B::Unbounded),
                1 => {
                    let p: &[u8] =
                        [&b""[..], b"f", b"g", b"l", b"m", b"z", b"q"][rng.below(7) as usize];
                    (
                        Some(KeyRange {
                            range: Some(Range::Prefix(p.to_vec().into())),
                        }),
                        B::Included(p.to_vec()),
                        B::Excluded(p.to_vec()),
                    )
                }
                _ => {
                    let (mut a, mut b) = (pick(&mut rng), pick(&mut rng));
                    if a > b {
                        std::mem::swap(&mut a, &mut b);
                    }
                    let (ai, bi) = (rng.below(2) == 0, rng.below(2) == 0);
                    (
                        Some(KeyRange {
                            range: Some(Range::Bounds(Bounds {
                                start: Some(Bound {
                                    key: a.clone().into(),
                                    inclusive: ai,
                                }),
                                end: Some(Bound {
                                    key: b.clone().into(),
                                    inclusive: bi,
                                }),
                            })),
                        }),
                        if ai { B::Included(a) } else { B::Excluded(a) },
                        if bi { B::Included(b) } else { B::Excluded(b) },
                    )
                }
            };
            let page_bytes = budgets[rng.below(budgets.len() as u64) as usize];
            let limit = [0u64, 0, 1, 3, 5, 9, 20][rng.below(7) as usize];
            // The oracle's rows.
            let scan = match (&range, &lo) {
                (
                    Some(KeyRange {
                        range: Some(Range::Prefix(p)),
                    }),
                    _,
                ) => t.scan_prefix(p),
                _ => t.scan_bounds(
                    match &lo {
                        B::Included(k) => B::Included(&k[..]),
                        B::Excluded(k) => B::Excluded(&k[..]),
                        B::Unbounded => B::Unbounded,
                    },
                    match &hi {
                        B::Included(k) => B::Included(&k[..]),
                        B::Excluded(k) => B::Excluded(&k[..]),
                        B::Unbounded => B::Unbounded,
                    },
                ),
            };
            let scan = if limit > 0 { scan.limit(limit) } else { scan };
            let want: Vec<(Vec<u8>, Cells)> =
                scan.iter().unwrap().map(|r| lib_row(&r.unwrap())).collect();
            let req = |after: Vec<u8>| ScanRequest {
                table: "users".into(),
                range: range.clone(),
                limit,
                page_bytes,
                after: after.into(),
                ..Default::default()
            };
            // The whole stream from one entry node.
            let entry = ports[rng.below(3) as usize];
            let pages = scan_pages(entry, req(Vec::new()))
                .await
                .unwrap_or_else(|e| panic!("{at}: {e}"));
            let last = pages.last().unwrap_or_else(|| panic!("{at}: no pages"));
            assert!(
                last.next.is_empty(),
                "{at}: the stream ended with a position"
            );
            // An empty `next` means only the scan's end, never a partition's (simp724's M1).
            for (i, p) in pages[..pages.len() - 1].iter().enumerate() {
                assert!(
                    !p.next.is_empty(),
                    "{at}: page {i} of {} says the scan ended",
                    pages.len()
                );
            }
            for p in &pages {
                assert!(
                    page_bytes == 0 || p.bytes <= page_bytes,
                    "{at}: page over budget"
                );
            }
            assert_eq!(
                joined(pages.iter().flat_map(|p| p.rows.clone())),
                want,
                "{at}: range {range:?}, limit {limit}, page_bytes {page_bytes}"
            );
            // Without a limit, a resume from every page's `next`, through another node,
            // reads exactly the rest (simp724's M1).
            if limit == 0 {
                for (i, p) in pages.iter().enumerate() {
                    if p.next.is_empty() {
                        continue;
                    }
                    let other = ports[rng.below(3) as usize];
                    let rest = scan_pages(other, req(p.next.to_vec()))
                        .await
                        .unwrap_or_else(|e| panic!("{at}: resume after page {i}: {e}"));
                    let mut from_here: Vec<_> =
                        pages[..=i].iter().flat_map(|p| p.rows.clone()).collect();
                    from_here.extend(rest.iter().flat_map(|p| p.rows.clone()));
                    assert_eq!(joined(from_here), want, "{at}: resumed after page {i}");
                }
            }
        }
        // A limit that ends exactly at a partition's end (simp724's N2): the scan ends there,
        // with an empty `next`, from every entry node and at every budget.
        let in_p1 = keys
            .iter()
            .filter(|k| k.as_slice() < b"g".as_slice())
            .count() as u64;
        for &port in &ports {
            for page_bytes in [300, 0] {
                let pages = scan_pages(
                    port,
                    super::proto::ScanRequest {
                        table: "users".into(),
                        limit: in_p1,
                        page_bytes,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
                let got = joined(pages.iter().flat_map(|p| p.rows.clone()));
                assert_eq!(got.len() as u64, in_p1, "seed {seed}: limit {in_p1}");
                assert!(got.iter().all(|(k, _)| k.as_slice() < b"g".as_slice()));
                assert!(
                    pages.last().unwrap().next.is_empty(),
                    "seed {seed}: limit at the boundary"
                );
            }
        }
        // Multi-gets: the single database's pages, exactly.
        for trial in 0..60 {
            use super::proto::MultiGetRequest;
            let at = format!("seed {seed}, multi-get {trial}");
            let n = 1 + rng.below(12);
            let mut asked: Vec<Vec<u8>> = (0..n).map(|_| pick(&mut rng)).collect();
            if rng.below(3) == 0 {
                asked.push(b"nope".to_vec());
            }
            let page_bytes = budgets[rng.below(budgets.len() as u64) as usize];
            let set = pigeonhole::KeySet::from_unsorted(asked.iter().map(|k| &k[..]));
            let mut want: Vec<Page> = Vec::new();
            let mut pos: Option<pigeonhole::KeysPosition> = None;
            loop {
                let mut read = t.keys(&set);
                if let Some(p) = &pos {
                    read = read.resume_after(p);
                }
                let page = read
                    .read_within(if page_bytes == 0 { 1 << 20 } else { page_bytes })
                    .unwrap();
                want.push((page.rows().iter().map(lib_row).collect(), page.bytes()));
                match page.resume() {
                    Some(p) => pos = Some(p.clone()),
                    None => break,
                }
            }
            let mut after = Vec::new();
            for (i, (rows, bytes)) in want.iter().enumerate() {
                let entry = ports[rng.below(3) as usize];
                let got = data(entry)
                    .await
                    .multi_get(MultiGetRequest {
                        table: "users".into(),
                        keys: asked.iter().map(|k| k.clone().into()).collect(),
                        page_bytes,
                        after: after.clone().into(),
                        ..Default::default()
                    })
                    .await
                    .unwrap_or_else(|e| panic!("{at}, page {i}: {e}"))
                    .into_inner();
                let got_rows: Vec<_> = got
                    .rows
                    .iter()
                    .map(|r| {
                        (
                            r.key.to_vec(),
                            r.cells
                                .iter()
                                .map(|c| (c.qualifier.to_vec(), c.value.to_vec()))
                                .collect::<Cells>(),
                        )
                    })
                    .collect();
                assert_eq!(
                    (&got_rows, got.bytes),
                    (rows, *bytes),
                    "{at}, page {i} of {asked:?} at {page_bytes}"
                );
                assert_eq!(
                    got.next.is_empty(),
                    i + 1 == want.len(),
                    "{at}, page {i}'s next"
                );
                after = got.next.to_vec();
            }
        }
        // A single node's position is refused by name on a cluster.
        let node_pos = t
            .scan_prefix(b"")
            .read_within(300)
            .unwrap()
            .resume()
            .unwrap()
            .to_bytes();
        let e = scan_pages(
            ports[0],
            super::proto::ScanRequest {
                table: "users".into(),
                after: node_pos.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(code(&e), "InvalidArgument", "{e}");
        assert!(e.message().contains("single node"), "{e}");
    });
    for db in dbs {
        let _ = db.close();
    }
    drop(t);
    let _ = oracle.close();
}

#[test]
fn a_partition_that_cant_be_reached_fails_a_fan_out_read_unless_it_may_be_skipped() {
    // D317 §7's owner decision: a missing partition fails the read with
    // PartitionUnavailable; with allow_partial it's skipped and named in `missing`.
    use super::proto::{MultiGetRequest, ScanRequest};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (mut stops, ports, dbs) = three_nodes(&rt);
    let rows: [&[u8]; 3] = [b"a1", b"h1", b"x1"];
    rt.block_on(async {
        for (i, r) in rows.iter().enumerate() {
            data(ports[0])
                .await
                .mutate(super::proto::MutateRequest {
                    writes: Some(cells_row(r, &[5], i as u64)),
                    durability: 0,
                })
                .await
                .unwrap();
        }
    });
    // Node b, holding p2 (["g", "m")), stops.
    let _ = stops.remove(1).send(());
    rt.block_on(async {
        eventually("node b to stop", async || {
            tokio::net::TcpStream::connect(("127.0.0.1", ports[1]))
                .await
                .is_err()
        })
        .await;
        let scan = |allow_partial| ScanRequest {
            table: "users".into(),
            allow_partial,
            ..Default::default()
        };
        // By default: the stream fails with the partition's error (its first page may come
        // first: p1's row).
        let mut stream = data(ports[0])
            .await
            .scan(scan(false))
            .await
            .unwrap()
            .into_inner();
        let e = loop {
            match stream.message().await {
                Ok(Some(_)) => {}
                Ok(None) => panic!("the scan ended without p2"),
                Err(e) => break e,
            }
        };
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        // With allow_partial: p1's and p3's rows, and p2's range in `missing`, once.
        let pages = scan_pages(ports[2], scan(true)).await.unwrap();
        let keys: Vec<Vec<u8>> = pages
            .iter()
            .flat_map(|p| p.rows.iter().map(|r| r.key.to_vec()))
            .collect();
        assert_eq!(keys, [b"a1".to_vec(), b"x1".to_vec()]);
        let missing: Vec<_> = pages.iter().flat_map(|p| p.missing.clone()).collect();
        assert_eq!(missing.len(), 1, "{missing:?}");
        assert_eq!(
            (
                missing[0].partition.as_str(),
                &missing[0].start[..],
                &missing[0].end[..]
            ),
            ("p2", &b"g"[..], &b"m"[..])
        );
        assert!(pages.last().unwrap().next.is_empty());
        // A multi-get the same way.
        let mg = |allow_partial| MultiGetRequest {
            table: "users".into(),
            keys: rows.iter().map(|r| r.to_vec().into()).collect(),
            allow_partial,
            ..Default::default()
        };
        let e = data(ports[0]).await.multi_get(mg(false)).await.unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        let got = data(ports[2])
            .await
            .multi_get(mg(true))
            .await
            .unwrap()
            .into_inner();
        let keys: Vec<Vec<u8>> = got.rows.iter().map(|r| r.key.to_vec()).collect();
        assert_eq!(keys, [b"a1".to_vec(), b"x1".to_vec()]);
        assert_eq!(got.missing.len(), 1);
        assert_eq!(got.missing[0].partition, "p2");
        assert!(got.next.is_empty());
    });
    for db in dbs {
        let _ = db.close();
    }
}

#[test]
fn scans_whose_clients_stop_reading_dont_hold_up_another_scan_to_the_same_node() {
    // simp724's S1 on #1698: a forwarded scan's unread pages count against its HTTP/2
    // connection's window too, so streams sharing a connection stall one another once a
    // few stop reading. Each forwarded stream has a connection of its own: sixteen scans
    // from node a over p2 (on node b) whose clients read nothing, and another scan from a
    // over p2 still reads to its end.
    use super::proto::{KeyRange, ScanRequest, key_range::Range};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, dbs) = three_nodes(&rt);
    rt.block_on(async {
        // About 12 MiB in p2 (["g", "m")): more than a connection's window, many times over.
        let mut c = data(ports[1]).await;
        for i in 0..192 {
            c.mutate(super::proto::MutateRequest {
                writes: Some(cells_row(format!("h{i:03}").as_bytes(), &[64 << 10], i)),
                durability: 0,
            })
            .await
            .unwrap();
        }
        let scan = || ScanRequest {
            table: "users".into(),
            range: Some(KeyRange {
                range: Some(Range::Prefix(b"h".to_vec().into())),
            }),
            page_bytes: 1 << 20,
            ..Default::default()
        };
        // The stalled clients (each opens its scan, takes its first page, and stops), then
        // another scan to its end, all well within --stream-idle-ms (30 s here): on a shared
        // connection the stalled streams would hold up the others until they idled out.
        let stalled = tokio::time::timeout(Duration::from_secs(10), async {
            let mut stalled = Vec::new();
            for _ in 0..16 {
                let mut s = data(ports[0])
                    .await
                    .scan(scan())
                    .await
                    .unwrap()
                    .into_inner();
                s.message().await.unwrap().unwrap();
                stalled.push(s);
            }
            // Their streams fill what windows they can.
            tokio::time::sleep(Duration::from_millis(300)).await;
            let pages = scan_pages(ports[0], scan()).await.unwrap();
            let rows: usize = pages.iter().map(|p| p.rows.len()).sum();
            assert!(rows >= 192, "{rows}");
            stalled
        })
        .await
        .expect("a scan stalled behind scans whose clients stopped reading");
        drop(stalled);
    });
    for db in dbs {
        let _ = db.close();
    }
}
