//! Three cluster nodes on loopback ports, in one process (D317 ticket 1): the map is set on
//! one, pushed to the others, compared-and-set, refused when it reshapes a placement or goes
//! back, pulled by a node that missed it, kept across a restart, reported as divergent, and
//! `Status` reads a partition's identity row.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::proto::{
    ClusterMap, GetMapRequest, Node as MapNode, Partition, PushMapRequest, SetMapRequest, SplitKey,
    StatusRequest, TablePlacement, split_key,
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
                history_epoch: 0,
                id: (*id).into(),
                node: (*node).into(),
                epoch: 1,
                memory_bytes: 0,
            })
            .collect(),
        tables: vec![TablePlacement {
            name: "users".into(),
            group: String::new(),
            families: Vec::new(),
            splits: vec![text("m")],
            partitions: vec!["p1".into(), "p2".into()],
            created: 0,
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
fn a_forced_first_map_starts_each_history_at_its_epoch() {
    // simp724's S1 on #1729: a restore's forced map onto a fresh node carries the backup
    // map's old `history_epoch`, which the restored files no longer hold. The node derives it.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let dir = test_dir();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let n = node(&dir, "a");
    let _stop = serve(&rt, &n, listener);
    rt.block_on(async {
        let mut m = the_map(1, &[port, port + 1, port + 2]);
        m.partitions[0].epoch = 7;
        m.partitions[0].history_epoch = 3;
        client(port)
            .await
            .set_map(SetMapRequest {
                expected_map_version: 0,
                map: Some(m),
                copies: Vec::new(),
                force: true,
            })
            .await
            .unwrap();
        let got = client(port)
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        let p1 = &got.partitions[0];
        assert_eq!((p1.id.as_str(), p1.epoch, p1.history_epoch), ("p1", 7, 7));
    });
}

#[test]
fn a_map_set_derives_each_partitions_history_and_its_peers_take_it() {
    // D317 ticket 6: `history_epoch` is derived where a map is set (`SetMapRequest.copies`
    // marks a relocation by copy), and peers take it as set.
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
    let nodes = [
        node(&dirs[0], "a"),
        node(&dirs[1], "b"),
        node(&dirs[2], "c"),
    ];
    let _stops: Vec<_> = nodes
        .iter()
        .zip(listeners)
        .map(|(n, l)| serve(&rt, n, l))
        .collect();
    rt.block_on(async {
        let set = async |expected: u64, map: ClusterMap, copies: &[&str]| {
            client(ports[0])
                .await
                .set_map(SetMapRequest {
                    expected_map_version: expected,
                    map: Some(map),
                    copies: copies.iter().map(|c| (*c).to_owned()).collect(),
                    force: false,
                })
                .await
        };
        let p3 = async |port: u16| {
            let m = client(port)
                .await
                .get_map(GetMapRequest {})
                .await
                .unwrap()
                .into_inner();
            let p = m.partitions.iter().find(|p| p.id == "p3").unwrap();
            (p.node.clone(), p.epoch, p.history_epoch)
        };
        // A first map relocates nothing, so it can't mark a copy.
        let e = set(0, the_map(1, &ports), &["p3"]).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e:?}");
        // A first map's histories start at its epochs, whatever it says (simp724's S1 on
        // #1729): the_map sends 0.
        set(0, the_map(1, &ports), &[]).await.unwrap();
        assert_eq!(p3(ports[0]).await, ("c".into(), 1, 1));

        // p3 copied from c to b at epoch 2: its history is kept, on every node.
        let mut v2 = the_map(2, &ports);
        v2.partitions[2].node = "b".into();
        v2.partitions[2].epoch = 2;
        set(1, v2.clone(), &["p3"]).await.unwrap();
        for port in ports {
            assert_eq!(p3(port).await, ("b".into(), 2, 1), "node at {port}");
        }

        // A mark on a change that isn't a move by one epoch is refused, and nothing changes.
        let mut v3 = v2.clone();
        v3.map_version = 3;
        v3.partitions[2].node = "a".into();
        v3.partitions[2].epoch = 4;
        let e = set(2, v3.clone(), &["p3"]).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e:?}");
        assert!(e.message().contains("marked as copied"), "{e:?}");
        assert_eq!(p3(ports[0]).await, ("b".into(), 2, 1));

        // Unmarked (a promotion or a restore), the move starts another history, whatever
        // the map sent says.
        v3.partitions[2].history_epoch = 0;
        set(2, v3, &[]).await.unwrap();
        for port in ports {
            assert_eq!(p3(port).await, ("a".into(), 4, 4), "node at {port}");
        }
    });
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
                copies: Vec::new(),
                force: false,
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
        let e = client(ports[0])
            .await
            .set_map(SetMapRequest {
                copies: Vec::new(),
                force: false,
                expected_map_version: 0,
                map: Some(the_map(2, &ports)),
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::Aborted, "{e:?}");
        assert!(e.message().contains("version 1"), "{e:?}");
        assert_eq!(version(ports[1]).await, 1);
        // Only the map's writer, a (the smallest id), sets a map (D317 amendment 1): b
        // answers WrongPartition naming it.
        let e = client(ports[1])
            .await
            .set_map(SetMapRequest {
                copies: Vec::new(),
                force: false,
                expected_map_version: 1,
                map: Some(the_map(2, &ports)),
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "WrongPartition", "{e:?}");
        assert!(e.message().contains("node a"), "{e:?}");
        assert_eq!(version(ports[1]).await, 1);

        // Reshaping a placement is refused; so is a map that goes back.
        let mut reshaped = the_map(2, &ports);
        reshaped.tables[0].splits = vec![text("n")];
        let e = client(ports[0])
            .await
            .set_map(SetMapRequest {
                copies: Vec::new(),
                force: false,
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
                copies: Vec::new(),
                force: false,
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

/// The cluster token the tests' nodes forward with, when they have tokens.
const NODE_TOKEN: &str = "n0de-secret";

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
    data_node_full(rt, dir, id, local, listener, "", limits, None, false)
}

/// [`data_node_with`], serving `db` if given (the same node's database again, after its
/// server stopped: a restart as the network sees it) rather than opening one. Partitions
/// keep a change feed (D317 ticket 7).
fn data_node_on(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    id: &str,
    local: Option<&str>,
    listener: std::net::TcpListener,
    limits: crate::serve::config::Limits,
    db: Option<pigeonhole::Pigeonhole>,
) -> (
    tokio::sync::oneshot::Sender<()>,
    Option<pigeonhole::Pigeonhole>,
) {
    data_node_full(rt, dir, id, local, listener, "", limits, db, false)
}

/// [`data_node`] with the token file `tokens` (empty: none), for its clients and its peers.
fn data_node_tokens(
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
    let limits = crate::serve::server_tests::limits();
    data_node_full(rt, dir, id, local, listener, tokens, limits, None, false)
}

/// [`data_node`] with the token file `tokens` and under `limits`.
fn data_node_limits(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    id: &str,
    local: Option<&str>,
    listener: std::net::TcpListener,
    tokens: &str,
    limits: crate::serve::config::Limits,
) -> (
    tokio::sync::oneshot::Sender<()>,
    Option<pigeonhole::Pigeonhole>,
) {
    data_node_full(rt, dir, id, local, listener, tokens, limits, None, false)
}

/// Every test data node: `tokens` (empty: none), `limits`, and `db` if given.
#[allow(clippy::too_many_arguments)]
fn data_node_full(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    id: &str,
    local: Option<&str>,
    listener: std::net::TcpListener,
    tokens: &str,
    limits: crate::serve::config::Limits,
    db: Option<pigeonhole::Pigeonhole>,
    restart: bool,
) -> (
    tokio::sync::oneshot::Sender<()>,
    Option<pigeonhole::Pigeonhole>,
) {
    let locals: Vec<(&str, Option<pigeonhole::Pigeonhole>)> =
        local.map(|p| (p, db)).into_iter().collect();
    let (stop, mut dbs) = data_node_many(rt, dir, id, locals, listener, tokens, limits, restart);
    (stop, dbs.pop())
}

/// Runs `body` for the test `test` in module `module` (its `module_path!()`) in a child
/// process of the test binary filtered `--exact` to that test alone, so the process-wide
/// CPU pin claims are its own whatever other tests pin (#1741). In the child, it runs
/// `body`. The parent checks the child ran it: a filter that matches nothing passes too.
#[cfg(target_os = "linux")]
fn run_alone(module: &str, test: &str, body: fn()) {
    const ALONE: &str = "PHDB_PIN_TEST_ALONE";
    if std::env::var_os(ALONE).is_some() {
        return body();
    }
    // The test's path without the crate's name, as the harness filters it.
    let name = format!("{}::{test}", module.split_once("::").unwrap().1);
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &name, "--nocapture", "--test-threads=1"])
        .env(ALONE, "1")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "the test alone failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(stdout.contains("1 passed"), "{stdout}");
}

/// A test partition's file in `dir`: created, stamped as partition `p` of cluster `c1` at
/// epoch 1, with a change feed and the `users` table.
fn test_partition(dir: &Path, p: &str) -> pigeonhole::Pigeonhole {
    test_partition_with(dir, p, pigeonhole::Options::default())
}

/// [`test_partition`], created with `options` (a shard count, say). Its change feed records
/// the shard count, which every later open keeps (ICR 0132).
fn test_partition_with(
    dir: &Path,
    p: &str,
    options: pigeonhole::Options,
) -> pigeonhole::Pigeonhole {
    let db = pigeonhole::Pigeonhole::open(
        dir.join(format!("{p}.phdb")),
        options
            .create_if_missing(true)
            .change_feed(pigeonhole::FeedRetention::bytes(16 << 20)),
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
    users(&db);
    db
}

/// A test data node serving `locals` (each partition's database given, or opened by
/// [`test_partition`]): the node's service and one per partition made from it, sharing one
/// `Backups`, as `cluster::run` builds them (D317 ticket 8a, 8b). Returns its stop handle and
/// the partitions' databases, in `locals`' order. `restart`: the node starts with a map, and
/// takes its peers' newer ones, as `run` has it.
#[allow(clippy::too_many_arguments)]
fn data_node_many(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    id: &str,
    locals: Vec<(&str, Option<pigeonhole::Pigeonhole>)>,
    listener: std::net::TcpListener,
    tokens: &str,
    limits: crate::serve::config::Limits,
    restart: bool,
) -> (
    tokio::sync::oneshot::Sender<()>,
    Vec<pigeonhole::Pigeonhole>,
) {
    // Unpinned: the rig's partitions share the test process's CPUs.
    let placement = crate::serve::placement::Placement::routes_only(Some(2), Some(2));
    let (stop, dbs, _) = data_node_placed(
        rt, dir, id, locals, listener, tokens, limits, restart, placement, None,
    );
    (stop, dbs)
}

/// [`data_node_many`] with `placement` for the partitions it opens while it serves (D317
/// ticket 6), returning its node too. `engine_memory`: the node's `--engine-memory`, split
/// across `locals` at start as `run` splits it ([`super::start_budget`]), and what a
/// partition opened later gets what's left of.
#[allow(clippy::too_many_arguments)]
fn data_node_placed(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    id: &str,
    locals: Vec<(&str, Option<pigeonhole::Pigeonhole>)>,
    listener: std::net::TcpListener,
    tokens: &str,
    limits: crate::serve::config::Limits,
    restart: bool,
    placement: crate::serve::placement::Placement,
    engine_memory: Option<u64>,
) -> (
    tokio::sync::oneshot::Sender<()>,
    Vec<pigeonhole::Pigeonhole>,
    Arc<Node>,
) {
    use crate::serve::{Shared, router::Router, service::Metrics, service::Service};
    let node = node_with(dir, id, tokens);
    // `DIR/fenced.json`, as `run` loads it before any partition opens (D317 ticket 6).
    node.load_fences().unwrap();
    let metrics = Arc::new(Metrics::default());
    let service = Service::routing_only(limits, 2, Arc::clone(&metrics));
    let backup_dir = dir.join(format!("backups-{id}"));
    std::fs::create_dir_all(&backup_dir).unwrap();
    let backups = crate::serve::backup::Backups::new(Some(&backup_dir), 1).unwrap();
    let mut dbs = Vec::new();
    let mut parts = Vec::new();
    // Each partition's memory as `run` plans it, one shard each.
    let mine: Vec<(String, u64)> = locals.iter().map(|(p, _)| ((*p).to_owned(), 1)).collect();
    let (_, memories) = super::start_budget(
        node.get_map().ok().as_ref(),
        &mine,
        mine.len().max(1),
        0,
        engine_memory,
    )
    .unwrap_or_else(|f| panic!("{}", f.message()));
    for (i, (p, db)) in locals.into_iter().enumerate() {
        let db = db.unwrap_or_else(|| test_partition(dir, p));
        let pinning = crate::serve::service::Pinning::default();
        let memory = memories
            .as_ref()
            .map_or_else(|| crate::serve::placement::Memory::defaults(1), |m| m[i]);
        // Fenced as `run` opens it (D317 ticket 6). A restarted node waits for its peers'
        // maps to unfence it; a fresh one stands for a node already serving.
        let gate = node.open_gate(p, 1);
        if !restart {
            gate.unfence();
        }
        parts.push((
            p.to_owned(),
            service.for_partition(db.clone(), p, &backups, pinning, memory, gate),
        ));
        dbs.push(db);
    }
    let shared = Arc::new(Shared {
        router: Router::cluster(service, Arc::clone(&node), parts),
        tokens: Tokens::parse(tokens, "tokens").unwrap(),
        limits,
        metrics,
        feed_runtimes: std::sync::OnceLock::new(),
    });
    // Partitions a map gives it while it serves open as `run` has them.
    super::open::attach_opener(
        &node,
        super::open::Opener {
            shared: Arc::downgrade(&shared),
            placement: Arc::new(placement),
            backups: Arc::clone(&backups),
            options: Box::new(|share, _| {
                pigeonhole::Options::default()
                    .default_shards(share.shards)
                    .compaction_cores(share.compaction)
            }),
            shards: 1,
            compaction: 0,
            engine_memory,
        },
    );
    super::schema::attach(&node, &shared);
    // A restart with a map: the node takes its peers' newer maps, as `run` has it.
    let start = if restart {
        super::start_sync(&node, node.get_map().map_or(0, |m| m.map_version))
    } else {
        None
    };
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
    if let Some(start) = start {
        let _in = rt.enter();
        start();
    }
    (stop, dbs, node)
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

/// The test table `users`: cells in `f`, edges in `out`, 4-dimension vectors in `emb`.
fn users(db: &pigeonhole::Pigeonhole) -> pigeonhole::Table {
    let spec =
        pigeonhole::VectorSpec::new(4, pigeonhole::Element::F32, pigeonhole::Metric::L2).unwrap();
    db.table("users")
        .unwrap()
        .family("f", pigeonhole::Family::default())
        .family("out", pigeonhole::Family::default())
        .family("emb", pigeonhole::Family::vector(spec))
        .create_if_missing()
        .unwrap()
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
        let e = super::proto::vectors_client::VectorsClient::new(channel)
            .search(super::proto::VectorSearchRequest {
                index: "users".into(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "Unsupported", "{e}");
        assert!(e.message().contains("Vectors.Nearest"), "{e}");
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
    // D317 §12's model test, single-partition half: every single-key call, every
    // one-partition batch, and every transaction and snapshot (ticket 3) answers as a single
    // database would, whichever node each call enters; every cross-partition batch, and
    // every row outside a session's partition, is refused with nothing written. The oracle
    // is a map.
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
    // Every session counts against the caps until its record expires, as on a single node,
    // and the workload's are all one (anonymous) caller's: room for every one it opens.
    let mut limits = crate::serve::server_tests::limits();
    limits.leases.max_sessions = 1 << 16;
    limits.leases.max_per_token = 1 << 16;
    let node = |dir: &PathBuf, id, local, l| data_node_limits(&rt, dir, id, local, l, "", limits);
    let (_a, db_a) = node(&dirs[0], "a", Some("p1"), it.next().unwrap());
    let (_b, db_b) = node(&dirs[1], "b", Some("p2"), it.next().unwrap());
    let (_c, _) = node(&dirs[2], "c", None, it.next().unwrap());
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
        let mut tx = Vec::new();
        let mut snaps = Vec::new();
        for &port in &ports {
            clients.push(data(port).await);
            tx.push(txns(port).await);
            snaps.push(snapshots(port).await);
        }
        let read_req = |row: &str| super::proto::GetRequest {
            table: "users".into(),
            row: row.as_bytes().to_vec().into(),
            family: "f".into(),
            qualifier: b"q".to_vec().into(),
            ..Default::default()
        };
        let value_of = |r: super::proto::GetResponse| {
            r.cell.map(|c| String::from_utf8(c.value.to_vec()).unwrap())
        };
        for i in 0..ops {
            let entry = rng.below(3) as usize;
            let at = format!("seed {seed}, op {i}, entry node {entry}");
            let kind = rng.below(6);
            if kind == 4 {
                // A transaction, each call through a random node: reads of its partition see
                // its snapshot, a row of the other is CrossPartition, and the commit applies
                // its writes, unless a put to a row it read came between (Conflict).
                let home = rows[rng.below(rows.len() as u64) as usize].clone();
                let id = tx[entry]
                    .begin(super::proto::BeginRequest {
                        durability: 0,
                        route: route_row(&home),
                    })
                    .await
                    .unwrap_or_else(|e| panic!("{at}: begin at {home}: {e}"))
                    .into_inner()
                    .txn_id;
                assert!(id.starts_with(&format!("p{}:", part(&home))), "{at}: {id}");
                let seen = model.clone();
                let (mut read, mut wrote) = (Vec::new(), Vec::new());
                for step in 0..1 + rng.below(4) {
                    let via = rng.below(3) as usize;
                    let row = rows[rng.below(rows.len() as u64) as usize].clone();
                    let mine = part(&row) == part(&home);
                    let at = format!("{at}, step {step} via {via}");
                    if rng.below(2) == 0 && !wrote.contains(&row) {
                        let got = tx[via]
                            .get(super::proto::TxnGetRequest {
                                txn_id: id.clone(),
                                table: "users".into(),
                                row: row.as_bytes().to_vec().into(),
                                family: "f".into(),
                                qualifier: b"q".to_vec().into(),
                            })
                            .await;
                        match got {
                            Ok(r) if mine => {
                                assert_eq!(
                                    value_of(r.into_inner()).as_ref(),
                                    seen.get(&row),
                                    "{at}: txn get {row}"
                                );
                                read.push(row);
                            }
                            Err(e) if !mine => assert_eq!(code(&e), "CrossPartition", "{at}: {e}"),
                            other => panic!("{at}: txn get {row}: {other:?}"),
                        }
                    } else {
                        let reply = tx[via]
                            .write(super::proto::TxnWriteRequest {
                                txn_id: id.clone(),
                                rows: vec![put_row(&row, &format!("t{i}"))],
                            })
                            .await;
                        match reply {
                            Ok(_) if mine => wrote.push(row),
                            Err(e) if !mine => assert_eq!(code(&e), "CrossPartition", "{at}: {e}"),
                            other => panic!("{at}: txn write {row}: {other:?}"),
                        }
                    }
                }
                // Sometimes a put to a row it read comes first; sometimes it aborts.
                let interfere = !read.is_empty() && rng.below(3) == 0;
                if interfere {
                    let row = &read[rng.below(read.len() as u64) as usize];
                    clients[rng.below(3) as usize]
                        .mutate(super::proto::MutateRequest {
                            writes: Some(put_row(row, &format!("x{i}"))),
                            durability: 0,
                        })
                        .await
                        .unwrap_or_else(|e| panic!("{at}: put {row}: {e}"));
                    model.insert(row.clone(), format!("x{i}"));
                }
                let via = rng.below(3) as usize;
                if rng.below(4) == 0 {
                    tx[via]
                        .abort(super::proto::TxnAbortRequest { txn_id: id.clone() })
                        .await
                        .unwrap_or_else(|e| panic!("{at}: abort via {via}: {e}"));
                    continue;
                }
                let reply = tx[via]
                    .commit(super::proto::TxnCommitRequest {
                        txn_id: id.clone(),
                        rows: Vec::new(),
                    })
                    .await;
                // A transaction without writes commits as a consistent read, whatever came
                // between.
                let interfere = interfere && !wrote.is_empty();
                match reply {
                    Ok(_) if !interfere => {
                        for row in wrote {
                            model.insert(row, format!("t{i}"));
                        }
                    }
                    Err(e) if interfere => assert_eq!(code(&e), "Conflict", "{at}: {e}"),
                    other => panic!("{at}: commit via {via} (interfered: {interfere}): {other:?}"),
                }
                continue;
            }
            if kind == 5 {
                // A snapshot opened through one node, read through others after a put: its
                // partition's rows as they were, another partition's CrossPartition.
                let home = rows[rng.below(rows.len() as u64) as usize].clone();
                let id = snaps[entry]
                    .open(super::proto::OpenSnapshotRequest {
                        durable: false,
                        route: route_row(&home),
                    })
                    .await
                    .unwrap_or_else(|e| panic!("{at}: open at {home}: {e}"))
                    .into_inner()
                    .snapshot_id;
                let seen = model.clone();
                let row = &rows[rng.below(rows.len() as u64) as usize];
                clients[rng.below(3) as usize]
                    .mutate(super::proto::MutateRequest {
                        writes: Some(put_row(row, &format!("s{i}"))),
                        durability: 0,
                    })
                    .await
                    .unwrap_or_else(|e| panic!("{at}: put {row}: {e}"));
                model.insert(row.clone(), format!("s{i}"));
                for _ in 0..2 {
                    let via = rng.below(3) as usize;
                    let row = &rows[rng.below(rows.len() as u64) as usize];
                    let mut req = read_req(row);
                    req.snapshot_id = id.clone();
                    match clients[via].get(req).await {
                        Ok(r) if part(row) == part(&home) => assert_eq!(
                            value_of(r.into_inner()).as_ref(),
                            seen.get(row),
                            "{at}: get {row} at the snapshot via {via}"
                        ),
                        Err(e) if part(row) != part(&home) => {
                            assert_eq!(code(&e), "CrossPartition", "{at}: {e}");
                        }
                        other => panic!("{at}: get {row} at the snapshot via {via}: {other:?}"),
                    }
                }
                snaps[rng.below(3) as usize]
                    .release(super::proto::ReleaseSnapshotRequest { snapshot_id: id })
                    .await
                    .unwrap_or_else(|e| panic!("{at}: release: {e}"));
                continue;
            }
            let c = &mut clients[entry];
            match kind {
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
    let share = crate::serve::placement::split(2, 1, 1)[0];
    let options = super::partition_options(
        crate::serve::config::ShardSpin::Micros(0),
        None,
        None,
        share,
    )
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
            let (stop, db) = data_node_tokens(&rt, &dir, id, local, l, tokens);
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
        // Within it, they read the snapshot through any node: a multi-get forwarded as a
        // single-key call is, a scan streamed from the snapshot's partition alone.
        let reply = data(a)
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
        let values = |rows: &[super::proto::Row]| -> Vec<(Vec<u8>, Vec<u8>)> {
            rows.iter()
                .flat_map(|r| r.cells.iter().map(|c| (r.key.to_vec(), c.value.to_vec())))
                .collect()
        };
        assert_eq!(values(&reply.rows), [(b"zed".to_vec(), b"1".to_vec())]);
        // A scan at the snapshot, here (b: pinned for the stream) and forwarded (a, c).
        for entry in [a, b, c] {
            let mut pages = data(entry)
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
            let mut got = Vec::new();
            while let Some(page) = pages.message().await.unwrap() {
                got.extend(values(&page.rows));
            }
            assert_eq!(got, [(b"zed".to_vec(), b"1".to_vec())], "via {entry}");
        }
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
    // a and c list every token; b, p2's node, doesn't list t0ken-c, and lists t0ken-d as
    // read where the others list it as write.
    let all = format!(
        "write t0ken-a\nwrite t0ken-b\nwrite t0ken-c\nwrite t0ken-d\ncluster {NODE_TOKEN}\n"
    );
    let b_tokens = format!("write t0ken-a\nwrite t0ken-b\nread t0ken-d\ncluster {NODE_TOKEN}\n");
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
        // The partition's node decides the scope by its own entry (cli692's R1 on #1702):
        // t0ken-d writes to p1, and is refused writing to p2.
        let write = |row: &str| super::proto::MutateRequest {
            writes: Some(put_row(row, "d")),
            durability: 0,
        };
        data(c)
            .await
            .mutate(with(write("alice"), Some("t0ken-d")))
            .await
            .unwrap();
        let e = data(c)
            .await
            .mutate(with(write("zed"), Some("t0ken-d")))
            .await
            .unwrap_err();
        assert_eq!(code(&e), "PermissionDenied", "{e}");
    });
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
    three_nodes_within(rt, 256 << 20)
}

/// [`three_nodes`] with `inflight` bytes of in-flight budget on each node.
fn three_nodes_within(
    rt: &tokio::runtime::Runtime,
    inflight: usize,
) -> (
    Vec<tokio::sync::oneshot::Sender<()>>,
    Vec<u16>,
    Vec<pigeonhole::Pigeonhole>,
) {
    three_nodes_held(rt, inflight, false)
}

/// [`three_nodes_within`], with each node's fanned-out scans held after each partition's
/// page until the partition's next page has taken its share (`hold`, #1753's seam).
fn three_nodes_held(
    rt: &tokio::runtime::Runtime,
    inflight: usize,
    hold: bool,
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
            inflight_bytes: inflight,
            // Longer than the stalled-stream test's bound, so a stall can't clear itself by
            // idling out.
            stream_idle: Duration::from_secs(30),
            hold_after_part_page: hold,
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
    fan_out_model(seed);
}

/// Seeds that failed once, run every time.
#[test]
fn fanned_out_reads_at_seeds_that_failed() {
    // #1698's multi-get page that used its whole budget at a partition's end, with only
    // keys that have no cells left after it: a `next` where a single node gives none.
    fan_out_model(1794362533986056000);
}

#[test]
fn fanned_out_reads_with_no_spare_budget_answer_as_one_database_does() {
    // #1715: with no spare budget a multi-get reads no partition ahead, and its pages must
    // still be a single node's, exactly. Each node's budget is below twice the smallest
    // page budget drawn, so a read ahead (a page's share beside the request's own holding
    // of one) can never be taken: every multi-get here reads its partitions one by one.
    let seed: u64 = std::env::var("PHDB_CLUSTER_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        });
    eprintln!(
        "cluster fan-out seed, no spare budget: {seed} (PHDB_CLUSTER_SEED={seed} replays it)"
    );
    fan_out_model_within(seed, 2199, &[1100, 1500, 2000]);
}

/// #1753: a fanned-out scan of a partition served on its entry node, with a budget too
/// small for two of its pages, finishes. The entry's page used to take a share of its own
/// after the partition's page gave its share back; held there (the seam) until the
/// partition's next page took the bytes, the entry's page waited for them until the request
/// timed out, since only the entry could take that next page. Now the entry's page keeps
/// the partition's share.
#[test]
fn a_scan_of_a_partition_on_its_entry_node_charges_each_page_once() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes_held(&rt, 2199, true);
    // 12 rows of about 420 bytes in p1 (keys below "g"), which the first node serves: pages
    // of 1500 bytes hold 3 rows, and two pages don't fit in 2199 bytes.
    let keys: Vec<Vec<u8>> = (0..12).map(|i| format!("a{i:02}").into_bytes()).collect();
    rt.block_on(async {
        for (i, key) in keys.iter().enumerate() {
            data(ports[0])
                .await
                .mutate(super::proto::MutateRequest {
                    writes: Some(cells_row(key, &[400], i as u64)),
                    durability: 0,
                })
                .await
                .unwrap();
        }
        use super::proto::{KeyRange, ScanRequest, key_range::Range};
        let started = std::time::Instant::now();
        let pages = scan_pages(
            ports[0],
            ScanRequest {
                table: "users".into(),
                range: Some(KeyRange {
                    range: Some(Range::Prefix(b"a".to_vec().into())),
                }),
                page_bytes: 1500,
                ..Default::default()
            },
        )
        .await
        .unwrap_or_else(|e| panic!("after {:?}: {e}", started.elapsed()));
        assert!(pages.len() >= 3, "{} pages", pages.len());
        assert!(
            pages
                .iter()
                .all(|p| p.bytes > 2199 / 2 || p.next.is_empty())
        );
        let got: Vec<Vec<u8>> = pages
            .iter()
            .flat_map(|p| p.rows.iter().map(|r| r.key.to_vec()))
            .collect();
        assert_eq!(got, keys);
    });
}

/// D317 §12's model test, fan-out half (ticket 4a), against one database holding every
/// row: scans over every kind of range, with row limits and byte budgets, resumed from
/// every page's `next` through another entry node; multi-gets of random key sets, page by
/// page, which must match the single database's pages exactly (the budget's merge across
/// partitions, simp724's S1 and M1).
fn fan_out_model(seed: u64) {
    fan_out_model_within(seed, 256 << 20, &[300, 450, 700, 1500, 0]);
}

/// [`fan_out_model`] with `inflight` bytes of in-flight budget on each node, and its reads'
/// `page_bytes` drawn from `budgets` (0: the default page).
fn fan_out_model_within(seed: u64, inflight: usize, budgets: &[u64]) {
    let mut rng = Rng(seed);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, dbs) = three_nodes_within(&rt, inflight);
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
            for page_bytes in [budgets[0], budgets[budgets.len() - 1]] {
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
        // A budget that ends exactly at p1's end: a `next` only when a later key has a
        // cell, past partitions whose keys have none (p2's "h" and p3's "nope" have none).
        let fill = t
            .keys(&pigeonhole::KeySet::from_unsorted([&b"a0"[..], b"f1"]))
            .read_within(1 << 20)
            .unwrap()
            .bytes();
        for (after_p1, rest) in [
            (vec![&b"nope"[..]], &[][..]),
            (vec![&b"h"[..], b"nope"], &[][..]),
            (vec![&b"h"[..], b"s0"], &[&b"s0"[..]][..]),
        ] {
            use super::proto::MultiGetRequest;
            let at = format!("seed {seed}, a budget ending at p1's end, then {after_p1:?}");
            let mut asked: Vec<&[u8]> = vec![b"a0", b"f1"];
            asked.extend(after_p1);
            let req = |after: Vec<u8>, page_bytes| MultiGetRequest {
                table: "users".into(),
                keys: asked.iter().map(|k| k.to_vec().into()).collect(),
                page_bytes,
                after: after.into(),
                ..Default::default()
            };
            for &entry in &ports {
                let page = data(entry)
                    .await
                    .multi_get(req(Vec::new(), fill))
                    .await
                    .unwrap();
                let page = page.into_inner();
                let got: Vec<&[u8]> = page.rows.iter().map(|r| &r.key[..]).collect();
                assert_eq!((got, page.bytes), (vec![&b"a0"[..], b"f1"], fill), "{at}");
                assert_eq!(page.next.is_empty(), rest.is_empty(), "{at}");
                if !rest.is_empty() {
                    // The rest at the default page, which holds it: at `fill`, a longer
                    // row than a0 and f1 together would be cut.
                    let more = data(entry)
                        .await
                        .multi_get(req(page.next.to_vec(), 0))
                        .await
                        .unwrap()
                        .into_inner();
                    let got: Vec<&[u8]> = more.rows.iter().map(|r| &r.key[..]).collect();
                    assert_eq!(got, rest, "{at}: resumed");
                    assert!(more.next.is_empty(), "{at}: resumed");
                }
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
        // a1 -> h1 -> x1 and a1 -> x1, with vectors 0, 1 and 2 along the first axis.
        let edges: [&[&[u8]]; 3] = [&[b"h1", b"x1"], &[b"x1"], &[]];
        for (i, r) in rows.iter().enumerate() {
            let out: Vec<(Vec<u8>, bool)> = edges[i].iter().map(|d| (d.to_vec(), true)).collect();
            data(ports[0])
                .await
                .mutate(super::proto::MutateRequest {
                    writes: Some(graph_row(r, [i as f32, 0.0, 0.0, 0.0], &out)),
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
        // A page that fills its budget at p1's end, with p2 next (simp724's N2 on #1710): p2
        // can't be probed for a cell, so the page resumes there, and the resume skips it.
        let fill = data(ports[2])
            .await
            .multi_get(MultiGetRequest {
                table: "users".into(),
                keys: vec![b"a1".to_vec().into()],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
            .bytes;
        let at_fill = |after: Vec<u8>| MultiGetRequest {
            page_bytes: fill,
            after: after.into(),
            ..mg(true)
        };
        let first = data(ports[2])
            .await
            .multi_get(at_fill(Vec::new()))
            .await
            .unwrap()
            .into_inner();
        let keys: Vec<Vec<u8>> = first.rows.iter().map(|r| r.key.to_vec()).collect();
        assert_eq!(keys, [b"a1".to_vec()]);
        assert!(
            first.missing.is_empty() && !first.next.is_empty(),
            "{first:?}"
        );
        let rest = data(ports[2])
            .await
            .multi_get(MultiGetRequest {
                page_bytes: 0,
                ..at_fill(first.next.to_vec())
            })
            .await
            .unwrap()
            .into_inner();
        let keys: Vec<Vec<u8>> = rest.rows.iter().map(|r| r.key.to_vec()).collect();
        assert_eq!(keys, [b"x1".to_vec()]);
        assert_eq!(rest.missing.len(), 1);
        assert_eq!(rest.missing[0].partition, "p2");
        assert!(rest.next.is_empty());
        // Nearest (ticket 4b): the top 3 of what's reachable, with p2 missing.
        let vectors = || async {
            let channel =
                tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{}", ports[0]))
                    .unwrap()
                    .connect()
                    .await
                    .unwrap();
            super::proto::vectors_client::VectorsClient::new(channel)
        };
        let near = |allow_partial| super::proto::NearestRequest {
            table: "users".into(),
            family: "emb".into(),
            query: Some(super::proto::Vector {
                element: super::proto::Element::F32 as i32,
                dim: 4,
                data: [0f32; 4]
                    .iter()
                    .flat_map(|x| x.to_le_bytes())
                    .collect::<Vec<u8>>()
                    .into(),
            }),
            k: 3,
            allow_partial,
            ..Default::default()
        };
        let e = vectors().await.nearest(near(false)).await.unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        let got = vectors()
            .await
            .nearest(near(true))
            .await
            .unwrap()
            .into_inner();
        let rows: Vec<Vec<u8>> = got.neighbours.iter().map(|n| n.row.to_vec()).collect();
        assert_eq!(rows, [b"a1".to_vec(), b"x1".to_vec()]);
        assert_eq!(got.missing.len(), 1);
        assert_eq!(got.missing[0].partition, "p2");
        // A traversal: hop 1 reads p1 only; hop 2's frontier (h1, x1) skips p2's h1.
        let graph = || async {
            let channel =
                tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{}", ports[2]))
                    .unwrap()
                    .connect()
                    .await
                    .unwrap();
            super::proto::graph_client::GraphClient::new(channel)
        };
        let walk = |allow_partial| super::proto::TraverseRequest {
            table: "users".into(),
            seeds: vec![b"a1".to_vec().into()],
            family: "out".into(),
            hops: 2,
            allow_partial,
            ..Default::default()
        };
        let e = graph().await.traverse(walk(false)).await.unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        let got = graph()
            .await
            .traverse(walk(true))
            .await
            .unwrap()
            .into_inner();
        let first: Vec<Vec<u8>> = got.hops[0].nodes.iter().map(|k| k.to_vec()).collect();
        assert_eq!(first, [b"h1".to_vec(), b"x1".to_vec()]);
        assert_eq!(
            got.hops.len(),
            1,
            "x1 has no edges, and h1's are p2's: {got:?}"
        );
        assert_eq!(got.missing.len(), 1);
        assert_eq!(got.missing[0].partition, "p2");
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

// ---- D317 ticket 4b: fan-out Nearest and Traverse ----

/// A row's writes: a vector in `emb` and edges in `out` (value `yes` or `no`).
fn graph_row(row: &[u8], v: [f32; 4], edges: &[(Vec<u8>, bool)]) -> super::proto::RowWrites {
    use super::proto::{Put, RowKey, Value, Vector, Write, value, write};
    let put = |family: &str, q: Vec<u8>, kind| Write {
        op: Some(write::Op::Put(Put {
            family: family.into(),
            qualifier: q.into(),
            timestamp: None,
            value: Some(Value { kind: Some(kind) }),
        })),
    };
    let data: Vec<u8> = v.iter().flat_map(|x| x.to_le_bytes()).collect();
    let mut writes = vec![put(
        "emb",
        Vec::new(),
        value::Kind::Vector(Vector {
            element: super::proto::Element::F32 as i32,
            dim: 4,
            data: data.into(),
        }),
    )];
    for (d, yes) in edges {
        let v: &[u8] = if *yes { b"yes" } else { b"no" };
        writes.push(put("out", d.clone(), value::Kind::Bytes(v.to_vec().into())));
    }
    super::proto::RowWrites {
        row: Some(RowKey {
            table: "users".into(),
            row: row.to_vec().into(),
        }),
        writes,
    }
}

#[test]
fn fanned_out_nearest_searches_and_traversals_answer_as_one_database_does() {
    let seed: u64 = std::env::var("PHDB_CLUSTER_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        });
    eprintln!("cluster 4b seed: {seed} (PHDB_CLUSTER_SEED={seed} replays it)");
    nearest_traverse_model(seed);
}

/// Seeds that failed once, run every time.
#[test]
fn fanned_out_nearest_and_traversals_at_seeds_that_failed() {
    // #1715: hop pages read at the traversal's own `page_bytes` cut a frontier row across
    // two pages, and the second page's `column_limit` started over, past `per_node_limit`.
    nearest_traverse_model(1794386011110329000);
}

/// D317 §12's model test, ticket 4b, against one database holding every row: nearest
/// searches over every source kind (vectors repeat, so distances tie and the order's row
/// and qualifier tie-breaks decide), cut by budgets; traversals whose edges cross the
/// partitions, with every limit, the byte budget, parents and an edge selection, stats
/// included.
fn nearest_traverse_model(seed: u64) {
    use super::proto::{
        Bound, Bounds, KeyList, KeyRange, NearestRequest, TraverseRequest, ValueFilter, Vector,
        key_range::Range, nearest_request::Source, value_filter,
    };
    let mut rng = Rng(seed);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, dbs) = three_nodes(&rt);
    let oracle_dir = test_dir();
    let oracle = pigeonhole::Pigeonhole::open(
        oracle_dir.join("oracle.phdb"),
        pigeonhole::Options::default().create_if_missing(true),
    )
    .unwrap();
    let t = users(&oracle);
    let mut keys: Vec<Vec<u8>> = ["a", "c", "f", "g", "h", "k", "m", "q", "s", "z"]
        .iter()
        .flat_map(|p| (0..3).map(move |i| format!("{p}{i}").into_bytes()))
        .collect();
    keys.extend([b"g".to_vec(), b"m".to_vec()]);
    keys.sort();
    // A few distinct vectors, so distances tie across partitions.
    let palette: [[f32; 4]; 4] = [
        [0.0, 0.0, 0.0, 0.0],
        [1.0, 0.0, 0.0, 0.0],
        [0.0, 2.0, 0.0, 0.0],
        [1.0, 1.0, 1.0, 1.0],
    ];
    rt.block_on(async {
        for key in &keys {
            let v = palette[rng.below(4) as usize];
            let edges: Vec<(Vec<u8>, bool)> = (0..rng.below(6))
                .map(|_| {
                    (
                        keys[rng.below(keys.len() as u64) as usize].clone(),
                        rng.below(2) == 0,
                    )
                })
                .collect();
            data(ports[rng.below(3) as usize])
                .await
                .mutate(super::proto::MutateRequest {
                    writes: Some(graph_row(key, v, &edges)),
                    durability: 0,
                })
                .await
                .unwrap_or_else(|e| panic!("seed {seed}: put {key:?}: {e}"));
            let mut m = t.mutate(key).put_vector_f32("emb", b"", &v);
            for (d, yes) in &edges {
                m = m.put("out", d, if *yes { b"yes" } else { b"no" });
            }
            m.commit().unwrap();
        }
    });
    let pick = |rng: &mut Rng| keys[rng.below(keys.len() as u64) as usize].clone();
    let budgets = [40u64, 100, 300, 0];
    rt.block_on(async {
        let vectors = |port: u16| async move {
            let channel =
                tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
                    .unwrap()
                    .connect()
                    .await
                    .unwrap();
            super::proto::vectors_client::VectorsClient::new(channel)
        };
        let graph = |port: u16| async move {
            let channel =
                tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
                    .unwrap()
                    .connect()
                    .await
                    .unwrap();
            super::proto::graph_client::GraphClient::new(channel)
        };
        // Nearest searches.
        for trial in 0..60 {
            let at = format!("seed {seed}, nearest {trial}");
            let q = if rng.below(2) == 0 {
                palette[rng.below(4) as usize]
            } else {
                [rng.below(3) as f32, 0.5, rng.below(2) as f32, 0.25]
            };
            let k = 1 + rng.below(20) as u32;
            let page_bytes = budgets[rng.below(budgets.len() as u64) as usize];
            let budget = if page_bytes == 0 { 1 << 20 } else { page_bytes };
            let (source, want) = match rng.below(3) {
                0 => (
                    None,
                    t.scan_prefix(b"")
                        .family("emb")
                        .nearest_f32(&q, k as usize)
                        .run_within(budget),
                ),
                1 => {
                    let (mut a, mut b) = (pick(&mut rng), pick(&mut rng));
                    if a > b {
                        std::mem::swap(&mut a, &mut b);
                    }
                    let (ai, bi) = (rng.below(2) == 0, rng.below(2) == 0);
                    use std::ops::Bound as B;
                    let lo = if ai {
                        B::Included(&a[..])
                    } else {
                        B::Excluded(&a[..])
                    };
                    let hi = if bi {
                        B::Included(&b[..])
                    } else {
                        B::Excluded(&b[..])
                    };
                    (
                        Some(Source::Range(KeyRange {
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
                        })),
                        t.scan_bounds(lo, hi)
                            .family("emb")
                            .nearest_f32(&q, k as usize)
                            .run_within(budget),
                    )
                }
                _ => {
                    let mut some: Vec<Vec<u8>> =
                        (0..1 + rng.below(10)).map(|_| pick(&mut rng)).collect();
                    some.push(b"nope".to_vec());
                    let set = pigeonhole::KeySet::from_unsorted(some.iter().map(|k| &k[..]));
                    (
                        Some(Source::Keys(KeyList {
                            keys: some.iter().map(|k| k.clone().into()).collect(),
                        })),
                        t.keys(&set)
                            .family("emb")
                            .nearest_f32(&q, k as usize)
                            .run_within(budget),
                    )
                }
            };
            let want = want.unwrap();
            let got = vectors(ports[rng.below(3) as usize])
                .await
                .nearest(NearestRequest {
                    table: "users".into(),
                    source: source.clone(),
                    family: "emb".into(),
                    query: Some(Vector {
                        element: super::proto::Element::F32 as i32,
                        dim: 4,
                        data: q
                            .iter()
                            .flat_map(|x| x.to_le_bytes())
                            .collect::<Vec<u8>>()
                            .into(),
                    }),
                    k,
                    page_bytes,
                    ..Default::default()
                })
                .await
                .unwrap_or_else(|e| panic!("{at}: {e}"))
                .into_inner();
            let got_n: Vec<(Vec<u8>, Vec<u8>, f64)> = got
                .neighbours
                .iter()
                .map(|n| (n.row.to_vec(), n.qualifier.to_vec(), n.distance))
                .collect();
            let cut = want.cut_by_budget();
            let want_n: Vec<(Vec<u8>, Vec<u8>, f64)> = want
                .into_neighbours()
                .into_iter()
                .map(|n| (n.row, n.qualifier, n.distance))
                .collect();
            assert_eq!(
                (&got_n, got.cut_by_budget),
                (&want_n, cut),
                "{at}: k {k}, page_bytes {page_bytes}, source {source:?}"
            );
        }
        // Traversals.
        for trial in 0..80 {
            let at = format!("seed {seed}, traverse {trial}");
            let seeds: Vec<Vec<u8>> = (0..1 + rng.below(3)).map(|_| pick(&mut rng)).collect();
            let hops = 1 + rng.below(4) as u32;
            let per_node_limit = [0u32, 0, 1, 2, 3][rng.below(5) as usize];
            let hop_limit = [0u64, 0, 1, 3, 7][rng.below(5) as usize];
            let max_nodes = [0u64, 0, 5, 12][rng.below(4) as usize];
            let page_bytes = [0u64, 0, 60, 200][rng.below(4) as usize];
            let parents = rng.below(2) == 0;
            let filter = rng.below(3);
            let set = pigeonhole::KeySet::from_unsorted(seeds.iter().map(|k| &k[..]));
            let mut w = t
                .traverse(&set)
                .family("out")
                .hops(hops)
                .parents(parents)
                .byte_budget(if page_bytes == 0 { 1 << 20 } else { page_bytes });
            if per_node_limit > 0 {
                w = w.per_node_limit(per_node_limit);
            }
            if hop_limit > 0 {
                w = w.hop_limit(hop_limit);
            }
            if max_nodes > 0 {
                w = w.max_nodes(max_nodes);
            }
            let mut req = TraverseRequest {
                table: "users".into(),
                seeds: seeds.iter().map(|k| k.clone().into()).collect(),
                family: "out".into(),
                hops,
                per_node_limit,
                hop_limit,
                max_nodes,
                parents,
                page_bytes,
                ..Default::default()
            };
            match filter {
                // Edges whose value starts with "y", with the per-node limit counting only
                // them (simp724's point 3).
                1 => {
                    w = w.value_filter(pigeonhole::ValueFilter::Prefix(b"y".to_vec()));
                    req.value_filter = Some(ValueFilter {
                        kind: Some(value_filter::Kind::Prefix(b"y".to_vec().into())),
                    });
                }
                2 => {
                    w = w.qualifier_prefix(b"m");
                    req.qualifier_prefix = b"m".to_vec().into();
                }
                _ => {}
            }
            let want = w.run().unwrap();
            let got = graph(ports[rng.below(3) as usize])
                .await
                .traverse(req.clone())
                .await
                .unwrap_or_else(|e| panic!("{at}: {e}"))
                .into_inner();
            let want_hops: Vec<(Vec<Vec<u8>>, Vec<u32>)> = want
                .hops()
                .iter()
                .map(|h| {
                    (
                        h.nodes().iter().map(<[u8]>::to_vec).collect(),
                        h.parents().map(<[u32]>::to_vec).unwrap_or_default(),
                    )
                })
                .collect();
            let got_hops: Vec<(Vec<Vec<u8>>, Vec<u32>)> = got
                .hops
                .iter()
                .map(|h| {
                    (
                        h.nodes.iter().map(|k| k.to_vec()).collect(),
                        h.parents.clone(),
                    )
                })
                .collect();
            assert_eq!(got_hops, want_hops, "{at}: {req:?}");
            let ws = want.stats();
            let gs = got.stats.unwrap();
            assert_eq!(
                (
                    gs.rows,
                    gs.edges,
                    gs.hops_cut,
                    gs.cut_by_max_nodes,
                    gs.cut_by_budget
                ),
                (
                    ws.rows,
                    ws.edges,
                    ws.hops_cut,
                    ws.cut_by_max_nodes,
                    ws.cut_by_budget
                ),
                "{at}: stats of {req:?}"
            );
        }
    });
    for db in dbs {
        let _ = db.close();
    }
    drop(t);
    let _ = oracle.close();
}

/// Data nodes `n0`, `n1`, … each serving one partition (`q0`, `q1`, …) of `users`, split at
/// `splits`, under `limits`; their stop handles, ports and partition databases.
fn split_nodes(
    rt: &tokio::runtime::Runtime,
    splits: &[&str],
    limits: crate::serve::config::Limits,
) -> (
    Vec<tokio::sync::oneshot::Sender<()>>,
    Vec<u16>,
    Vec<pigeonhole::Pigeonhole>,
) {
    let n = splits.len() + 1;
    let listeners: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let map = ClusterMap {
        cluster_id: "c1".into(),
        map_version: 1,
        nodes: ports
            .iter()
            .enumerate()
            .map(|(i, port)| MapNode {
                id: format!("n{i}"),
                address: format!("127.0.0.1:{port}"),
            })
            .collect(),
        partitions: (0..n)
            .map(|i| Partition {
                history_epoch: 0,
                id: format!("q{i}"),
                node: format!("n{i}"),
                epoch: 1,
                memory_bytes: 0,
            })
            .collect(),
        tables: vec![TablePlacement {
            name: "users".into(),
            group: String::new(),
            families: Vec::new(),
            splits: splits.iter().map(|s| text(s)).collect(),
            partitions: (0..n).map(|i| format!("q{i}")).collect(),
            created: 0,
        }],
    };
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for (i, l) in listeners.into_iter().enumerate() {
        let dir = test_dir();
        super::write_map(&dir, &super::map::canonical_json(&map)).unwrap();
        let (id, p) = (format!("n{i}"), format!("q{i}"));
        let (stop, db) = data_node_with(rt, &dir, &id, Some(&p), l, limits);
        stops.push(stop);
        dbs.push(db.unwrap());
    }
    (stops, ports, dbs)
}

#[test]
fn a_nearest_search_over_many_partitions_merges_their_answers_as_they_arrive() {
    // simp724's S1 on #1713: the merge keeps a running top `k` within the budget as each
    // partition answers, rather than every answer at once. Five partitions, at most two
    // read at once (so answers arrive in varied orders), and `k` past any one partition's
    // answer, against one database's `nearest_f32`, cut by budgets and not.
    use super::proto::{NearestRequest, Vector};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let limits = crate::serve::config::Limits {
        fanout_forwards: 2,
        ..crate::serve::server_tests::limits()
    };
    let (_stops, ports, dbs) = split_nodes(&rt, &["c", "f", "j", "p"], limits);
    let oracle_dir = test_dir();
    let oracle = pigeonhole::Pigeonhole::open(
        oracle_dir.join("oracle.phdb"),
        pigeonhole::Options::default().create_if_missing(true),
    )
    .unwrap();
    let t = users(&oracle);
    // Three rows in each partition, with vectors that tie across them.
    let keys: Vec<String> = ["a", "d", "g", "k", "s"]
        .iter()
        .flat_map(|p| (0..3).map(move |i| format!("{p}{i}")))
        .collect();
    rt.block_on(async {
        for (i, key) in keys.iter().enumerate() {
            let v = [(i % 4) as f32, (i % 3) as f32, 0.0, 0.0];
            data(ports[i % ports.len()])
                .await
                .mutate(super::proto::MutateRequest {
                    writes: Some(graph_row(key.as_bytes(), v, &[])),
                    durability: 0,
                })
                .await
                .unwrap();
            t.mutate(key.as_bytes())
                .put_vector_f32("emb", b"", &v)
                .commit()
                .unwrap();
        }
        let channel =
            tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{}", ports[2]))
                .unwrap()
                .connect()
                .await
                .unwrap();
        let mut vectors = super::proto::vectors_client::VectorsClient::new(channel);
        for q in [[0.0f32, 0.0, 0.0, 0.0], [2.5, 1.0, 0.0, 0.0]] {
            for k in [1u32, 3, 4, 7, 12, 15, 20] {
                for page_bytes in [40u64, 100, 200, 0] {
                    let budget = if page_bytes == 0 { 1 << 20 } else { page_bytes };
                    let want = t
                        .scan_prefix(b"")
                        .family("emb")
                        .nearest_f32(&q, k as usize)
                        .run_within(budget)
                        .unwrap();
                    let got = vectors
                        .nearest(NearestRequest {
                            table: "users".into(),
                            family: "emb".into(),
                            query: Some(Vector {
                                element: super::proto::Element::F32 as i32,
                                dim: 4,
                                data: q
                                    .iter()
                                    .flat_map(|x| x.to_le_bytes())
                                    .collect::<Vec<u8>>()
                                    .into(),
                            }),
                            k,
                            page_bytes,
                            ..Default::default()
                        })
                        .await
                        .unwrap()
                        .into_inner();
                    let got_n: Vec<(Vec<u8>, f64)> = got
                        .neighbours
                        .iter()
                        .map(|n| (n.row.to_vec(), n.distance))
                        .collect();
                    let cut = want.cut_by_budget();
                    let want_n: Vec<(Vec<u8>, f64)> = want
                        .into_neighbours()
                        .into_iter()
                        .map(|n| (n.row, n.distance))
                        .collect();
                    assert_eq!(
                        (&got_n, got.cut_by_budget),
                        (&want_n, cut),
                        "q {q:?}, k {k}, page_bytes {page_bytes}"
                    );
                }
            }
        }
    });
    drop(t);
    oracle.close().unwrap();
    for db in dbs {
        let _ = db.close();
    }
}

// ---- D317 ticket 5: schema in the map ----

#[test]
fn schema_calls_change_the_map_and_every_partition_follows_it() {
    // D317 §5 through the nodes: a create, an added family, a drop and a create again under
    // the same name, each through a different entry node; every node lists and describes
    // the map's tables, and every partition's file follows the map. A hand-made table is
    // left alone, and a create over a hand-made table of its name is reported as taken
    // over. Two creates through two nodes at once both land (simp724's S2).
    use super::proto::{
        CreateTableRequest, DescribeTableRequest, DropTableRequest, FamilySpec, ListTablesRequest,
        TablePlacementRequest, schema_client::SchemaClient,
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, dbs) = three_nodes(&rt);
    // Made by hand in p2's file: one the map never places, one it will.
    for name in ["manual", "collide"] {
        dbs[1]
            .table(name)
            .unwrap()
            .family("f", pigeonhole::Family::default())
            .create()
            .unwrap();
    }
    let has = |db: &pigeonhole::Pigeonhole, t: &str| db.tables().iter().any(|n| n == t);
    let fams = |names: &[&str]| -> Vec<FamilySpec> {
        names
            .iter()
            .map(|n| FamilySpec {
                name: (*n).into(),
                ..Default::default()
            })
            .collect()
    };
    rt.block_on(async {
        let schema = |port: u16| async move {
            let channel =
                tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
                    .unwrap()
                    .connect()
                    .await
                    .unwrap();
            SchemaClient::new(channel)
        };
        let create = |name: &str,
                      families: Vec<FamilySpec>,
                      placement: Option<TablePlacementRequest>,
                      if_missing| CreateTableRequest {
            name: name.into(),
            families,
            if_missing,
            placement,
        };
        let on = |splits: &[&[u8]], partitions: &[&str]| {
            Some(TablePlacementRequest {
                group: String::new(),
                splits: splits.iter().map(|s| s.to_vec().into()).collect(),
                partitions: partitions.iter().map(|p| (*p).into()).collect(),
            })
        };
        // A create through node c: `orders` split at "k" over p1 and p3.
        let made = schema(ports[2])
            .await
            .create_table(create(
                "orders",
                fams(&["f"]),
                on(&[b"k"], &["p1", "p3"]),
                false,
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(made.unreconciled.is_empty(), "{made:?}");
        assert!(has(&dbs[0], "orders") && has(&dbs[2], "orders") && !has(&dbs[1], "orders"));
        for &port in &ports {
            let mut s = schema(port).await;
            let list = s
                .list_tables(ListTablesRequest {})
                .await
                .unwrap()
                .into_inner()
                .tables;
            assert_eq!(list, ["orders", "users"], "node at {port}");
            let d = s
                .describe_table(DescribeTableRequest {
                    name: "orders".into(),
                })
                .await
                .unwrap()
                .into_inner();
            assert_eq!(d.families.len(), 1);
        }
        // Rows of it, written and read through other nodes.
        for (row, port) in [(b"a1".as_slice(), ports[1]), (b"z1".as_slice(), ports[0])] {
            let mut w = put_row(std::str::from_utf8(row).unwrap(), "v");
            w.row.as_mut().unwrap().table = "orders".into();
            data(port)
                .await
                .mutate(super::proto::MutateRequest {
                    writes: Some(w),
                    durability: 0,
                })
                .await
                .unwrap();
        }
        let in_file = |db: &pigeonhole::Pigeonhole, row: &[u8]| {
            db.table("orders")
                .unwrap()
                .open()
                .unwrap()
                .get(row, "f", b"q")
                .unwrap()
                .is_some()
        };
        assert!(in_file(&dbs[0], b"a1") && in_file(&dbs[2], b"z1"));
        // Refused: a create of an existing table, and one the map doesn't place without a
        // placement.
        let e = schema(ports[0])
            .await
            .create_table(create("orders", fams(&["f"]), None, false))
            .await
            .unwrap_err();
        assert_eq!(code(&e), "TableExists", "{e}");
        let e = schema(ports[0])
            .await
            .create_table(create("nowhere", fams(&["f"]), None, false))
            .await
            .unwrap_err();
        assert_eq!(code(&e), "InvalidArgument", "{e}");
        // A family added through node a.
        schema(ports[0])
            .await
            .create_table(create("orders", fams(&["f", "g"]), None, true))
            .await
            .unwrap();
        for db in [&dbs[0], &dbs[2]] {
            let t = db.table("orders").unwrap().open().unwrap();
            assert!(t.families().contains(&"g"));
        }
        // Dropped through node b, and made again on p2 only.
        schema(ports[1])
            .await
            .drop_table(DropTableRequest {
                name: "orders".into(),
            })
            .await
            .unwrap();
        assert!(!has(&dbs[0], "orders") && !has(&dbs[2], "orders"));
        schema(ports[1])
            .await
            .create_table(create("orders", fams(&["f"]), on(&[], &["p2"]), false))
            .await
            .unwrap();
        assert!(has(&dbs[1], "orders") && !has(&dbs[0], "orders") && !has(&dbs[2], "orders"));
        // A create over the hand-made `collide` in p2: the map takes it over, and says so.
        schema(ports[0])
            .await
            .create_table(create("collide", fams(&["f"]), on(&[], &["p2"]), false))
            .await
            .unwrap();
        let status = client(ports[1])
            .await
            .status(StatusRequest {})
            .await
            .unwrap()
            .into_inner();
        let p2 = status.partitions.iter().find(|p| p.id == "p2").unwrap();
        assert_eq!(p2.adopted_tables, ["collide"], "{p2:?}");
        assert_eq!(p2.unmapped_tables, ["manual"], "{p2:?}");
        assert!(p2.reconcile_errors.is_empty(), "{p2:?}");
        assert!(has(&dbs[1], "manual"));
        // Concurrent schema changes through every node (D317 amendment 1): twelve creates
        // at once, four through each node, all land, and every node ends with the same map,
        // with no divergence. Before the single writer, two entry nodes' compare-and-sets
        // each made the same next version, and one table was lost.
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..12usize {
            let (port, part) = (ports[i % 3], ["p1", "p2", "p3"][i % 3]);
            let req = create(&format!("c{i}"), fams(&["f"]), on(&[], &[part]), false);
            tasks.spawn(async move { (i, schema(port).await.create_table(req).await) });
        }
        while let Some(done) = tasks.join_next().await {
            let (i, r) = done.unwrap();
            r.unwrap_or_else(|e| panic!("create c{i}: {e}"));
        }
        let mut maps = Vec::new();
        for &port in &ports {
            eventually("every node to take the last map", async || {
                version(port).await == version(ports[0]).await
            })
            .await;
            let m = client(port)
                .await
                .get_map(GetMapRequest {})
                .await
                .unwrap()
                .into_inner();
            let s = client(port)
                .await
                .status(StatusRequest {})
                .await
                .unwrap()
                .into_inner();
            assert!(s.divergent.is_empty(), "{s:?}");
            maps.push(m);
        }
        assert!(maps.windows(2).all(|w| w[0] == w[1]), "{maps:?}");
        for i in 0..12 {
            let name = format!("c{i}");
            assert!(maps[0].tables.iter().any(|t| t.name == name), "{name}");
            assert!(has(&dbs[i % 3], &name), "{name} in its partition");
        }
    });
    for db in dbs {
        let _ = db.close();
    }
}

#[test]
fn a_writer_that_restarts_with_a_map_sets_none_until_it_has_its_peers_maps() {
    // simp724's N4 on #1720: the synced gate, held. `start_sync` gives what runs once the
    // node serves; until that has run (here: until the test runs it), the writer refuses to
    // set a map, as a writer back from a forced recovery must until it has the forced map.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let dir = test_dir();
    super::write_map(&dir, &super::map::canonical_json(&the_map(1, &ports))).unwrap();
    // a, the writer, restarts with version 1 on disk; b and c are down.
    let a = node(&dir, "a");
    let mut listeners = listeners.into_iter();
    let _stop = serve(&rt, &a, listeners.next().unwrap());
    let start = super::start_sync(&a, 1).expect("a node with a map syncs at start");
    let set = |version: u64| SetMapRequest {
        copies: Vec::new(),
        force: false,
        expected_map_version: version - 1,
        map: Some(the_map(version, &ports)),
    };
    rt.block_on(async {
        let e = client(ports[0]).await.set_map(set(2)).await.unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e:?}");
        assert!(e.message().contains("still taking"), "{e:?}");
        assert_eq!(version(ports[0]).await, 1);
    });
    // The sync runs (its peers are down: each fetch times out), and the gate opens.
    rt.block_on(async {
        start();
        eventually("node a to finish its start", async || {
            client(ports[0]).await.set_map(set(2)).await.is_ok()
        })
        .await;
        assert_eq!(version(ports[0]).await, 2);
    });
}

#[test]
fn a_forced_map_replaces_a_writer_thats_down_and_the_old_writer_follows_it() {
    // D317 amendment 1's recovery: with the writer (a, the smallest id) down, schema calls
    // can't be made; `set-map --force` at another node sets a map without a, above every
    // reachable node's version, and b becomes the writer. A forced map that still names the
    // down writer is refused. When a comes back with its old map, it takes the forced one
    // from its peers and changes no map itself: a schema call through it reaches b.
    use super::proto::{CreateTableRequest, FamilySpec, TablePlacementRequest};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let map = three_way(&ports);
    let dirs = [test_dir(), test_dir(), test_dir()];
    let limits = crate::serve::server_tests::limits();
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for ((l, dir), (id, p)) in
        listeners
            .into_iter()
            .zip(&dirs)
            .zip([("a", "p1"), ("b", "p2"), ("c", "p3")])
    {
        super::write_map(dir, &super::map::canonical_json(&map)).unwrap();
        let (stop, db) = data_node_full(&rt, dir, id, Some(p), l, "", limits, None, false);
        stops.push(Some(stop));
        dbs.push(db);
    }
    let create = |name: &str, partition: &str| CreateTableRequest {
        name: name.into(),
        families: vec![FamilySpec {
            name: "f".into(),
            ..Default::default()
        }],
        if_missing: false,
        placement: Some(TablePlacementRequest {
            group: String::new(),
            splits: Vec::new(),
            partitions: vec![partition.into()],
        }),
    };
    let schema = |port: u16| async move {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        super::proto::schema_client::SchemaClient::new(channel)
    };
    let args = |port: u16| super::cli::NodeArgs {
        node: format!("http://127.0.0.1:{port}"),
        token_file: None,
        ca: None,
    };
    rt.block_on(async {
        // Through c, made by a.
        schema(ports[2])
            .await
            .create_table(create("t0", "p3"))
            .await
            .unwrap();
    });
    // The writer stops.
    let _ = stops[0].take().unwrap().send(());
    let a_db = dbs[0].take();
    rt.block_on(async {
        eventually("node a to stop", async || {
            tokio::net::TcpStream::connect(("127.0.0.1", ports[0]))
                .await
                .is_err()
        })
        .await;
        let e = schema(ports[2])
            .await
            .create_table(create("t1", "p3"))
            .await
            .unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        let current = client(ports[1])
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        // A forced map that still names a as its writer is refused, and changes nothing.
        let Err(e) = super::cli::set_map(&args(ports[1]), current.clone(), true, Vec::new()).await
        else {
            panic!("a forced map naming a writer that's down was set");
        };
        assert!(format!("{e:?}").contains("can't be"), "{e:?}");
        // Without a: p1 moves to b at a new epoch; the version comes out above every
        // reachable node's, whatever the file says.
        let mut forced = current.clone();
        forced.map_version = 1;
        forced.nodes.retain(|n| n.id != "a");
        let history = |m: &ClusterMap, id: &str| {
            let p = m.partitions.iter().find(|p| p.id == id).unwrap();
            (p.epoch, p.history_epoch)
        };
        let p2 = history(&current, "p2");
        for p in &mut forced.partitions {
            if p.id == "p1" {
                p.node = "b".into();
                p.epoch += 1;
                // A history the caller claims is never taken (D317 ticket 6): p1 was
                // promoted, not copied.
                p.history_epoch = 0;
            }
        }
        // A forced map can't mark a copy: a copy is a planned move, through the writer.
        let e = client(ports[1])
            .await
            .set_map(SetMapRequest {
                copies: vec!["p1".into()],
                force: true,
                expected_map_version: 0,
                map: Some(forced.clone()),
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e:?}");
        assert!(
            e.message().contains("can't mark a partition as copied"),
            "{e:?}"
        );
        let p1_epoch = history(&forced, "p1").0;
        let report = match super::cli::set_map(&args(ports[1]), forced, true, Vec::new()).await {
            Ok(r) => r,
            Err(e) => panic!("the forced map: {e:?}"),
        };
        // a couldn't be reached, so the forced epochs weren't checked against its map: the
        // report names it.
        assert_eq!(report.document["unchecked"], serde_json::json!(["a"]));
        let v = current.map_version + 1;
        assert_eq!((version(ports[1]).await, version(ports[2]).await), (v, v));
        // p1's history restarts at its new epoch, on b and on c, which took b's push; p2's,
        // whose epoch didn't move, is kept.
        for &port in &ports[1..] {
            let m = client(port)
                .await
                .get_map(GetMapRequest {})
                .await
                .unwrap()
                .into_inner();
            assert_eq!(history(&m, "p1"), (p1_epoch, p1_epoch), "at {port}");
            assert_eq!(history(&m, "p2"), p2, "at {port}");
        }
        // Schema calls work again, through b, the writer now.
        schema(ports[2])
            .await
            .create_table(create("t1", "p3"))
            .await
            .unwrap();
        assert!(dbs[2].as_ref().unwrap().tables().iter().any(|t| t == "t1"));
    });
    // a comes back with its old map, takes the forced one, and follows it.
    let l = std::net::TcpListener::bind(("127.0.0.1", ports[0])).unwrap();
    let (stop, _) = data_node_full(&rt, &dirs[0], "a", Some("p1"), l, "", limits, a_db, true);
    stops[0] = Some(stop);
    rt.block_on(async {
        let newest = version(ports[1]).await;
        eventually("node a to take the forced map", async || {
            version(ports[0]).await == newest
        })
        .await;
        // It isn't the writer: a set-map there is refused, naming b.
        let at_a = client(ports[0])
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        let e = client(ports[0])
            .await
            .set_map(SetMapRequest {
                copies: Vec::new(),
                force: false,
                expected_map_version: at_a.map_version,
                map: Some(ClusterMap {
                    map_version: at_a.map_version + 1,
                    ..at_a.clone()
                }),
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "WrongPartition", "{e:?}");
        assert!(e.message().contains("node b"), "{e:?}");
        // A schema call through a reaches b. b and c end on one map, none divergent; a,
        // no longer in the map, isn't pushed to, and stays at the forced map: it wrote none.
        schema(ports[0])
            .await
            .create_table(create("t2", "p3"))
            .await
            .unwrap();
        let last = version(ports[1]).await;
        assert_eq!(last, newest + 1);
        assert_eq!(version(ports[0]).await, newest);
        let mut maps = Vec::new();
        for &port in &ports[1..] {
            eventually("every node to take the last map", async || {
                version(port).await == last
            })
            .await;
            let s = client(port)
                .await
                .status(StatusRequest {})
                .await
                .unwrap()
                .into_inner();
            assert!(s.divergent.is_empty(), "{s:?}");
            maps.push(
                client(port)
                    .await
                    .get_map(GetMapRequest {})
                    .await
                    .unwrap()
                    .into_inner(),
            );
        }
        assert!(maps.windows(2).all(|w| w[0] == w[1]), "{maps:?}");
        assert!(maps[0].tables.iter().any(|t| t.name == "t2"));
        assert!(!maps[0].nodes.iter().any(|n| n.id == "a"));
    });
    for db in dbs.into_iter().flatten() {
        let _ = db.close();
    }
}

#[test]
fn a_forced_map_never_reuses_an_epoch_its_node_or_a_reachable_one_has_used() {
    // simp724's F1 and F2 on #1729: a forced map's `history_epoch` is derived against the
    // receiving node's map, so its epochs must be new. The node refuses one that goes back
    // or moves a partition at a kept epoch (F1); `set-map --force` refuses one below, or
    // moved at, an epoch a reachable node has, even when the receiving node is behind (F2).
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes(&rt);
    let args = |port: u16| super::cli::NodeArgs {
        node: format!("http://127.0.0.1:{port}"),
        token_file: None,
        ca: None,
    };
    let with_p3 = |m: &ClusterMap, version: u64, node: &str, epoch: u64| {
        let mut m = m.clone();
        m.map_version = version;
        for p in &mut m.partitions {
            if p.id == "p3" {
                p.node = node.into();
                p.epoch = epoch;
                p.history_epoch = epoch;
            }
        }
        m
    };
    rt.block_on(async {
        let v1 = client(ports[0])
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        // b and c take a map that raised p3's epoch to 2; a doesn't, so it's behind.
        let v2 = with_p3(&v1, 2, "c", 2);
        for &port in &ports[1..] {
            client(port)
                .await
                .push_map(PushMapRequest {
                    map: Some(v2.clone()),
                })
                .await
                .unwrap();
        }
        let force_at = |port: u16, map: ClusterMap| async move {
            client(port)
                .await
                .set_map(SetMapRequest {
                    copies: Vec::new(),
                    force: true,
                    expected_map_version: 0,
                    map: Some(map),
                })
                .await
        };
        // F1, at b: p3's epoch can't go back to 1, and p3 can't move to a at epoch 2.
        for (map, why) in [
            (with_p3(&v2, 3, "c", 1), "epoch went back from 2 to 1"),
            (with_p3(&v2, 3, "a", 2), "without a new epoch"),
        ] {
            let e = force_at(ports[1], map).await.unwrap_err();
            assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e:?}");
            assert!(e.message().contains(why), "{e:?}");
        }
        assert_eq!(version(ports[1]).await, 2, "nothing was set at b");
        // F2, at a, whose own map (p3 at 1 on c) accepts both: b's and c's don't.
        for (map, why) in [
            (with_p3(&v1, 3, "c", 1), "epoch went back from 2 to 1"),
            (with_p3(&v1, 3, "b", 2), "without a new epoch"),
        ] {
            assert_eq!(version(ports[0]).await, 1, "a is still behind");
            let Err(e) = super::cli::set_map(&args(ports[0]), map, true, Vec::new()).await else {
                panic!("a forced map reusing an epoch b and c have was set ({why})");
            };
            let e = format!("{e:?}");
            assert!(
                e.contains("has a newer placement than the forced map"),
                "{e}"
            );
            assert!(e.contains(why), "{e}");
        }
        for &port in &ports {
            assert!(version(port).await <= 2, "no forced map was set");
        }
        // An epoch above every node's is set, and nothing is unchecked: all three answered.
        let r = super::cli::set_map(&args(ports[0]), with_p3(&v1, 3, "b", 3), true, Vec::new())
            .await
            .unwrap();
        assert_eq!(r.document["unchecked"], serde_json::json!([]));
        let m = client(ports[2])
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        let p3 = m.partitions.iter().find(|p| p.id == "p3").unwrap();
        assert_eq!((p3.node.as_str(), p3.epoch, p3.history_epoch), ("b", 3, 3));
    });
}

mod backup;
mod charge;
mod close;
mod feed;
mod fence;
mod metrics;
mod moving;
mod open;
mod several;
mod stamp;
mod upload;
