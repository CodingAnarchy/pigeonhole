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
    node_with(dir, id, "")
}

/// [`node`] with the token file `tokens` (empty: none); with tokens, it forwards with
/// [`NODE_TOKEN`], which `tokens` lists with cluster scope.
fn node_with(dir: &Path, id: &str, tokens: &str) -> Arc<Node> {
    let mut state = MapState::default();
    if let Some(m) = read_map(dir).unwrap() {
        let json = super::map::canonical_json(&m);
        state.map = Some((super::map::canonical(&m), json));
    }
    Node::new(
        dir,
        id,
        true,
        Tokens::parse(tokens, "tokens").unwrap(),
        Peers {
            token: (!tokens.is_empty()).then(|| NODE_TOKEN.to_owned()),
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
    data_node_with(rt, dir, id, local, listener, "")
}

/// The cluster token the tests' nodes forward with, when they have tokens.
const NODE_TOKEN: &str = "n0de-secret";

/// [`data_node`] with the token file `tokens` (empty: none), for its clients and its peers.
fn data_node_with(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    id: &str,
    local: Option<&str>,
    listener: std::net::TcpListener,
    tokens: &str,
) -> (
    tokio::sync::oneshot::Sender<()>,
    Option<pigeonhole::Pigeonhole>,
) {
    use crate::serve::{Shared, router::Router, service::Metrics, service::Service};
    let node = node_with(dir, id, tokens);
    let limits = crate::serve::server_tests::limits();
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
        tokens: Tokens::parse(tokens, "tokens").unwrap(),
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
        // Calls a later ticket brings are refused, naming it.
        let e = data(c)
            .await
            .scan(super::proto::ScanRequest {
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

/// A three-node cluster (a: p1, b: p2, c routes only) whose nodes list `tokens` (empty:
/// none; `tokens_c` for c, if given): its runtime, ports, stops and partitions' files.
struct Three {
    rt: tokio::runtime::Runtime,
    ports: [u16; 3],
    stops: Vec<tokio::sync::oneshot::Sender<()>>,
    dbs: Vec<pigeonhole::Pigeonhole>,
}

impl Three {
    fn start(tokens: [&str; 3]) -> Self {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let listeners: Vec<std::net::TcpListener> = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let ports: Vec<u16> = listeners
            .iter()
            .map(|l| l.local_addr().unwrap().port())
            .collect();
        let ports = [ports[0], ports[1], ports[2]];
        let mut map = the_map(1, &ports);
        map.partitions.retain(|p| p.id != "p3");
        let mut stops = Vec::new();
        let mut dbs = Vec::new();
        for ((l, (id, local)), tokens) in listeners
            .into_iter()
            .zip([("a", Some("p1")), ("b", Some("p2")), ("c", None)])
            .zip(tokens)
        {
            let dir = test_dir();
            super::write_map(&dir, &super::map::canonical_json(&map)).unwrap();
            let (stop, db) = data_node_with(&rt, &dir, id, local, l, tokens);
            stops.push(stop);
            dbs.extend(db);
        }
        Self {
            rt,
            ports,
            stops,
            dbs,
        }
    }
}

impl Drop for Three {
    fn drop(&mut self) {
        self.stops.clear();
        for db in self.dbs.drain(..) {
            let _ = db.close();
        }
    }
}

/// `m` with the bearer `token`, if any.
fn with<T>(m: T, token: Option<&str>) -> tonic::Request<T> {
    let mut r = tonic::Request::new(m);
    if let Some(t) = token {
        r.metadata_mut()
            .insert("authorization", format!("Bearer {t}").parse().unwrap());
    }
    r
}

async fn txns(
    port: u16,
) -> super::proto::transactions_client::TransactionsClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    super::proto::transactions_client::TransactionsClient::new(channel)
}

async fn snapshots(
    port: u16,
) -> super::proto::snapshots_client::SnapshotsClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    super::proto::snapshots_client::SnapshotsClient::new(channel)
}

fn route_row(row: &str) -> Option<super::proto::RoutingKey> {
    Some(super::proto::RoutingKey {
        key: Some(super::proto::routing_key::Key::Row(super::proto::RowKey {
            table: "users".into(),
            row: row.as_bytes().to_vec().into(),
        })),
    })
}

async fn begin(port: u16, row: &str, token: Option<&str>) -> Result<String, tonic::Status> {
    let r = super::proto::BeginRequest {
        durability: 0,
        route: route_row(row),
    };
    Ok(txns(port)
        .await
        .begin(with(r, token))
        .await?
        .into_inner()
        .txn_id)
}

async fn txn_write(
    port: u16,
    id: &str,
    rows: Vec<super::proto::RowWrites>,
    token: Option<&str>,
) -> Result<(), tonic::Status> {
    let r = super::proto::TxnWriteRequest {
        txn_id: id.into(),
        rows,
    };
    txns(port).await.write(with(r, token)).await.map(drop)
}

async fn commit(port: u16, id: &str, token: Option<&str>) -> Result<u64, tonic::Status> {
    let r = super::proto::TxnCommitRequest {
        txn_id: id.into(),
        rows: Vec::new(),
    };
    let reply = txns(port).await.commit(with(r, token)).await?.into_inner();
    Ok(reply.commit.map_or(0, |c| c.seqno))
}

async fn txn_get(port: u16, id: &str, row: &str) -> Result<Option<Vec<u8>>, tonic::Status> {
    let r = super::proto::TxnGetRequest {
        txn_id: id.into(),
        table: "users".into(),
        row: row.as_bytes().to_vec().into(),
        family: "f".into(),
        qualifier: b"q".to_vec().into(),
    };
    let reply = txns(port).await.get(r).await?.into_inner();
    Ok(reply.cell.map(|c| c.value.to_vec()))
}

async fn get_at(port: u16, row: &str, snapshot: &str) -> Result<Option<Vec<u8>>, tonic::Status> {
    let reply = data(port)
        .await
        .get(super::proto::GetRequest {
            table: "users".into(),
            row: row.as_bytes().to_vec().into(),
            family: "f".into(),
            qualifier: b"q".to_vec().into(),
            snapshot_id: snapshot.into(),
            ..Default::default()
        })
        .await?;
    Ok(reply.into_inner().cell.map(|c| c.value.to_vec()))
}

async fn put(port: u16, row: &str, value: &str) {
    data(port)
        .await
        .mutate(super::proto::MutateRequest {
            writes: Some(put_row(row, value)),
            durability: 0,
        })
        .await
        .unwrap_or_else(|e| panic!("put {row} through {port}: {e}"));
}

#[test]
fn a_transaction_lives_on_its_partitions_node_and_any_node_serves_its_calls() {
    // a serves p1 (users < "m"), b serves p2 (users >= "m"), c only routes.
    let t = Three::start(["", "", ""]);
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        // `route` is required on a cluster.
        let e = txns(c)
            .await
            .begin(super::proto::BeginRequest::default())
            .await
            .unwrap_err();
        assert_eq!(code(&e), "InvalidArgument", "{e}");
        // Begun through c by a row of p2: the id names p2, and any node serves its calls.
        let id = begin(c, "zed", None).await.unwrap();
        assert!(id.starts_with("p2:"), "{id}");
        txn_write(a, &id, vec![put_row("yan", "1")], None)
            .await
            .unwrap();
        // A row of another partition is refused, before anything is buffered or written.
        let e = txn_write(
            c,
            &id,
            vec![put_row("yo", "2"), put_row("alice", "2")],
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(code(&e), "CrossPartition", "{e}");
        assert!(
            e.message().contains("p1") && e.message().contains("p2"),
            "{e}"
        );
        let e = txn_get(b, &id, "alice").await.unwrap_err();
        assert_eq!(code(&e), "CrossPartition", "{e}");
        let first = commit(b, &id, None).await.unwrap();
        assert_eq!(get_row(c, "yan").await.unwrap(), Some(b"1".to_vec()));
        assert_eq!(get_row(c, "yo").await.unwrap(), None);
        assert_eq!(get_row(c, "alice").await.unwrap(), None);
        // A Commit sent again, through another node, answers the first outcome (B1).
        assert_eq!(commit(a, &id, None).await.unwrap(), first);
        // A conflicting pair through different nodes: the second to commit is refused.
        put(c, "zed", "0").await;
        let (t1, t2) = (
            begin(a, "zed", None).await.unwrap(),
            begin(b, "zz", None).await.unwrap(),
        );
        for (entry, id) in [(a, &t1), (c, &t2)] {
            assert_eq!(
                txn_get(entry, id, "zed").await.unwrap(),
                Some(b"0".to_vec())
            );
            txn_write(entry, id, vec![put_row("zed", id)], None)
                .await
                .unwrap();
        }
        commit(c, &t1, None).await.unwrap();
        let e = commit(a, &t2, None).await.unwrap_err();
        assert_eq!(code(&e), "Conflict", "{e}");
        assert_eq!(get_row(b, "zed").await.unwrap(), Some(t1.into_bytes()));
        // An id in a single node's form, or of a partition the map doesn't list: NotFound.
        for bad in [
            "0123456789abcdef0123456789abcdef",
            "p9:0123456789abcdef0123456789abcdef",
        ] {
            let e = commit(c, bad, None).await.unwrap_err();
            assert_eq!(code(&e), "NotFound", "{bad}: {e}");
        }
    });
}

#[test]
fn a_snapshot_reads_its_own_partition_through_any_node() {
    let t = Three::start(["", "", ""]);
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        put(a, "zed", "1").await;
        put(a, "alice", "1").await;
        let id = snapshots(c)
            .await
            .open(super::proto::OpenSnapshotRequest {
                durable: false,
                route: Some(super::proto::RoutingKey {
                    key: Some(super::proto::routing_key::Key::Partition("p2".into())),
                }),
            })
            .await
            .unwrap()
            .into_inner()
            .snapshot_id;
        assert!(id.starts_with("p2:"), "{id}");
        put(c, "zed", "2").await;
        for entry in [a, b, c] {
            assert_eq!(
                get_at(entry, "zed", &id).await.unwrap(),
                Some(b"1".to_vec())
            );
        }
        // Another partition's row, a multi-get or a scan that reaches one: CrossPartition.
        let e = get_at(a, "alice", &id).await.unwrap_err();
        assert_eq!(code(&e), "CrossPartition", "{e}");
        let at = Some(super::proto::Selection {
            snapshot_id: id.clone(),
            ..Default::default()
        });
        let e = data(c)
            .await
            .scan(super::proto::ScanRequest {
                table: "users".into(),
                selection: at.clone(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "CrossPartition", "{e}");
        let e = data(c)
            .await
            .multi_get(super::proto::MultiGetRequest {
                table: "users".into(),
                keys: vec![b"zed".to_vec().into(), b"alice".to_vec().into()],
                selection: at.clone(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "CrossPartition", "{e}");
        // Within it, they read the snapshot, on its node (through another one once ticket 4's
        // forwards land).
        let reply = data(b)
            .await
            .multi_get(super::proto::MultiGetRequest {
                table: "users".into(),
                keys: vec![b"zed".to_vec().into()],
                selection: at.clone(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.rows.len(), 1);
        let mut pages = data(b)
            .await
            .scan(super::proto::ScanRequest {
                table: "users".into(),
                range: Some(super::proto::KeyRange {
                    range: Some(super::proto::key_range::Range::Prefix(b"z".to_vec().into())),
                }),
                selection: at.clone(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let mut rows = 0;
        while let Some(page) = pages.message().await.unwrap() {
            rows += page.rows.len();
        }
        assert_eq!(rows, 1);
        // Released through another node: gone everywhere.
        snapshots(b)
            .await
            .release(super::proto::ReleaseSnapshotRequest {
                snapshot_id: id.clone(),
            })
            .await
            .unwrap();
        let e = get_at(c, "zed", &id).await.unwrap_err();
        assert_eq!(code(&e), "NotFound", "{e}");
    });
}

#[test]
fn a_session_is_its_tokens_on_every_node() {
    // a and c list every token; b, p2's node, doesn't list t0ken-c.
    let all = format!("write t0ken-a\nwrite t0ken-b\nwrite t0ken-c\ncluster {NODE_TOKEN}\n");
    let b_tokens = format!("write t0ken-a\nwrite t0ken-b\ncluster {NODE_TOKEN}\n");
    let t = Three::start([&all, &b_tokens, &all]);
    let [a, _, c] = t.ports;
    t.rt.block_on(async {
        let id = begin(a, "zed", Some("t0ken-a")).await.unwrap();
        // Another token's session, through another node: as on a single node, not found.
        let e = txn_write(c, &id, vec![put_row("yan", "1")], Some("t0ken-b"))
            .await
            .unwrap_err();
        assert_eq!(code(&e), "NotFound", "{e}");
        // Its own token, through any node.
        txn_write(c, &id, vec![put_row("yan", "1")], Some("t0ken-a"))
            .await
            .unwrap();
        commit(c, &id, Some("t0ken-a")).await.unwrap();
        // A token the partition's node doesn't list: refused there, and the digest isn't
        // in the message.
        let e = begin(c, "zed", Some("t0ken-c")).await.unwrap_err();
        assert_eq!(code(&e), "PermissionDenied", "{e}");
        let digest: String = <sha2::Sha256 as sha2::Digest>::digest(b"t0ken-c")
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert!(!e.message().contains(&digest[..16]), "{e}");
        // On p1, whose node lists it, it's served.
        begin(c, "alice", Some("t0ken-c")).await.unwrap();
    });
}
