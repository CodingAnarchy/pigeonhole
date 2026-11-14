//! Fencing (D317 ticket 6, rule 3): a fenced partition's writes answer `Fenced` through
//! every node while its reads go on; a transaction that wrote commits only once it's
//! unfenced; unfence needs the map to name the node at the file's epoch; and after a
//! restart, every partition starts fenced, unfences on a peer's map, and an operator's
//! fence stays.

use super::*;
use crate::serve::proto::{FenceReason, LocalPartition, LocalPartitionState, PartitionRequest};

async fn try_put(port: u16, row: &str, value: &str) -> Result<(), tonic::Status> {
    data(port)
        .await
        .mutate(crate::serve::proto::MutateRequest {
            writes: Some(put_row(row, value)),
            durability: 0,
        })
        .await
        .map(drop)
}

async fn fence(port: u16, p: &str) -> Result<LocalPartition, tonic::Status> {
    client(port)
        .await
        .fence(PartitionRequest {
            partition: p.into(),
            ..Default::default()
        })
        .await
        .map(tonic::Response::into_inner)
}

async fn unfence(port: u16, p: &str) -> Result<LocalPartition, tonic::Status> {
    client(port)
        .await
        .unfence(PartitionRequest {
            partition: p.into(),
            ..Default::default()
        })
        .await
        .map(tonic::Response::into_inner)
}

/// Partition `p`'s line in node `port`'s Status.
async fn local(port: u16, p: &str) -> LocalPartition {
    let s = client(port)
        .await
        .status(StatusRequest {})
        .await
        .unwrap()
        .into_inner();
    s.partitions
        .iter()
        .find(|lp| lp.id == p)
        .unwrap_or_else(|| panic!("no {p} in {s:?}"))
        .clone()
}

async fn state(port: u16, p: &str) -> LocalPartitionState {
    local(port, p).await.state()
}

async fn reason(port: u16, p: &str) -> FenceReason {
    local(port, p).await.fence_reason()
}

#[test]
fn a_fenced_partition_refuses_writes_through_every_node_and_unfence_lifts_it() {
    // a serves p1 (users < "m"), b serves p2; c only routes.
    let t = Three::start(["", "", ""]);
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        put(c, "alice", "1").await;
        assert_eq!(state(a, "p1").await, LocalPartitionState::Serving);
        let lp = fence(a, "p1").await.unwrap();
        assert_eq!(lp.state(), LocalPartitionState::Fenced, "{lp:?}");
        assert_eq!(lp.fence_reason(), FenceReason::Operator, "{lp:?}");
        // `phdb cluster status`: a row per partition, with its state on its node.
        let args = super::super::cli::NodeArgs {
            node: format!("http://127.0.0.1:{c}"),
            token_file: None,
            ca: None,
        };
        let Ok(r) = super::super::cli::status(&args).await else {
            panic!("phdb cluster status failed");
        };
        let col = |name: &str| r.columns.iter().position(|k| k.name == name).unwrap();
        let states: Vec<(String, String)> = r
            .rows
            .iter()
            .map(|row| {
                (
                    row[col("partition")]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    row[col("state")].as_str().unwrap_or_default().to_owned(),
                )
            })
            .collect();
        assert!(
            states.contains(&("p1".into(), "fenced (operator)".into())),
            "{states:?}"
        );
        assert!(
            states.contains(&("p2".into(), "serving".into())),
            "{states:?}"
        );
        // Its node, and a forward from another: refused, nothing written.
        for entry in [a, b, c] {
            let e = try_put(entry, "alice", "2").await.unwrap_err();
            assert_eq!(code(&e), "Fenced", "through {entry}: {e}");
        }
        // Reads go on, and the other partition takes writes.
        assert_eq!(get_row(c, "alice").await.unwrap(), Some(b"1".to_vec()));
        put(c, "zed", "1").await;
        // Fencing again waits again, and answers the same.
        assert_eq!(
            fence(a, "p1").await.unwrap().state(),
            LocalPartitionState::Fenced
        );
        // Only the node holding it fences it.
        let e = fence(b, "p1").await.unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        let lp = unfence(a, "p1").await.unwrap();
        assert_eq!(lp.state(), LocalPartitionState::Serving, "{lp:?}");
        assert_eq!(lp.fence_reason(), FenceReason::Unspecified, "{lp:?}");
        put(b, "alice", "3").await;
        assert_eq!(get_row(c, "alice").await.unwrap(), Some(b"3".to_vec()));
    });
}

#[test]
fn a_transaction_that_wrote_commits_only_once_its_partition_is_unfenced() {
    let t = Three::start(["", "", ""]);
    let [a, _, c] = t.ports;
    t.rt.block_on(async {
        put(c, "bob", "0").await;
        let wrote = begin(c, "bob", None).await.unwrap();
        txn_write(c, &wrote, vec![put_row("bob", "1")], None)
            .await
            .unwrap();
        let read = begin(c, "bob", None).await.unwrap();
        assert_eq!(txn_get(c, &read, "bob").await.unwrap(), Some(b"0".to_vec()));
        fence(a, "p1").await.unwrap();
        // A read-only transaction commits: a consistent read.
        commit(c, &read, None).await.unwrap();
        // One that wrote is refused, and stays open; so is a new write in another.
        let e = commit(c, &wrote, None).await.unwrap_err();
        assert_eq!(code(&e), "Fenced", "{e}");
        let other = begin(c, "bob", None).await.unwrap();
        let e = txn_write(c, &other, vec![put_row("bob", "2")], None)
            .await
            .unwrap_err();
        assert_eq!(code(&e), "Fenced", "{e}");
        assert_eq!(get_row(c, "bob").await.unwrap(), Some(b"0".to_vec()));
        unfence(a, "p1").await.unwrap();
        commit(c, &wrote, None).await.unwrap();
        assert_eq!(get_row(c, "bob").await.unwrap(), Some(b"1".to_vec()));
    });
}

#[test]
fn unfence_is_refused_unless_the_map_names_the_node_at_the_files_epoch() {
    let t = Three::start(["", "", ""]);
    let [a, _, _] = t.ports;
    t.rt.block_on(async {
        fence(a, "p1").await.unwrap();
        // A map with p1 at epoch 2 on a, whose file is still at epoch 1 (as after a stamp
        // elsewhere that a missed): its writes would break rule 2.
        let mut m = client(a)
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        m.map_version += 1;
        m.partitions
            .iter_mut()
            .filter(|p| p.id == "p1")
            .for_each(|p| p.epoch = 2);
        client(a)
            .await
            .set_map(SetMapRequest {
                force: true,
                expected_map_version: 0,
                map: Some(m),
                copies: Vec::new(),
            })
            .await
            .unwrap();
        let e = unfence(a, "p1").await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        assert!(e.message().contains("epoch 1"), "{e}");
        assert_eq!(state(a, "p1").await, LocalPartitionState::Fenced);
    });
}

#[test]
fn after_a_restart_partitions_unfence_on_a_peers_map_and_an_operators_fence_stays() {
    // cli692's S3: the three nodes restart together, a first while its peers don't answer
    // yet, and every partition unfences once a peer's map confirms it; p3, fenced by the
    // operator before the restart, stays fenced until it's unfenced.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let map = three_way(&ports);
    let dirs = [test_dir(), test_dir(), test_dir()];
    let limits = crate::serve::server_tests::limits();
    let nodes = [("a", "p1"), ("b", "p2"), ("c", "p3")];
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for ((l, dir), (id, p)) in listeners.into_iter().zip(&dirs).zip(nodes) {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
        let (stop, db) = data_node_full(&rt, dir, id, Some(p), l, "", limits, None, false);
        stops.push(stop);
        dbs.push(db);
    }
    rt.block_on(async {
        fence(ports[2], "p3").await.unwrap();
    });
    assert_eq!(
        super::super::fence::read_fenced(&dirs[2]).unwrap(),
        ["p3".to_owned()].into()
    );
    // All three stop.
    for stop in stops.drain(..) {
        let _ = stop.send(());
    }
    rt.block_on(async {
        for port in ports {
            eventually("the node to stop", async || {
                tokio::net::TcpStream::connect(("127.0.0.1", port))
                    .await
                    .is_err()
            })
            .await;
        }
    });
    let bind = |port: u16| std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
    // a restarts first: its peers don't answer, so p1 stays fenced.
    let (stop, _) = data_node_full(
        &rt,
        &dirs[0],
        "a",
        Some("p1"),
        bind(ports[0]),
        "",
        limits,
        dbs[0].clone(),
        true,
    );
    stops.push(stop);
    rt.block_on(async {
        assert_eq!(state(ports[0], "p1").await, LocalPartitionState::Fenced);
        assert_eq!(reason(ports[0], "p1").await, FenceReason::AwaitingPeerMap);
        let e = try_put(ports[0], "alice", "1").await.unwrap_err();
        assert_eq!(code(&e), "Fenced", "{e}");
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(state(ports[0], "p1").await, LocalPartitionState::Fenced);
        // An operator's unfence of a start fence asks the peers too, and none answers
        // (cli692's S1); --force lifts it anyway.
        let e = unfence(ports[0], "p1").await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        assert!(e.message().contains("not a majority"), "{e}");
        let lp = client(ports[0])
            .await
            .unfence(PartitionRequest {
                partition: "p1".into(),
                force: true,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(lp.state(), LocalPartitionState::Serving, "{lp:?}");
    });
    // Then b and c: each confirms the others, and a its peers, as their maps answer.
    for i in [1, 2] {
        let (stop, _) = data_node_full(
            &rt,
            &dirs[i],
            nodes[i].0,
            Some(nodes[i].1),
            bind(ports[i]),
            "",
            limits,
            dbs[i].clone(),
            true,
        );
        stops.push(stop);
    }
    rt.block_on(async {
        for (port, p) in [(ports[0], "p1"), (ports[1], "p2")] {
            eventually("a start fence to lift on a peer's map", async || {
                state(port, p).await == LocalPartitionState::Serving
            })
            .await;
        }
        // p1's and p2's rows ("hank" is between the splits "g" and "m").
        put(ports[2], "alice", "1").await;
        put(ports[2], "hank", "1").await;
        // The operator's fence outlived the restart.
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(state(ports[2], "p3").await, LocalPartitionState::Fenced);
        assert_eq!(reason(ports[2], "p3").await, FenceReason::Operator);
        unfence(ports[2], "p3").await.unwrap();
        assert_eq!(state(ports[2], "p3").await, LocalPartitionState::Serving);
        put(ports[0], "zed", "1").await;
    });
    assert!(
        super::super::fence::read_fenced(&dirs[2])
            .unwrap()
            .is_empty()
    );
    drop(stops);
    for db in dbs.into_iter().flatten() {
        let _ = db.close();
    }
}

/// #1767: a partition the node serves shows its identity in Status, read from its own
/// handle; a read-only open of the file is refused while the node holds it for writing,
/// which listed it at epoch 0 with "the writer lock is held".
#[test]
fn status_shows_a_served_partitions_identity_from_its_handle() {
    let t = Three::start(["", "", ""]);
    let [a, b, _] = t.ports;
    t.rt.block_on(async {
        for (port, p) in [(a, "p1"), (b, "p2")] {
            let lp = local(port, p).await;
            assert_eq!(lp.epoch, 1, "{lp:?}");
            assert!(lp.ok, "{lp:?}");
            assert!(lp.error.is_empty(), "{lp:?}");
            assert_eq!(lp.state(), LocalPartitionState::Serving, "{lp:?}");
        }
    });
}

/// audit120's first cluster should: `unfence` reconciled before lifting an operator's fence,
/// and the reconcile skips a partition the operator fenced, so a table created during the
/// fence stayed missing there once it took writes again.
#[test]
fn a_table_created_during_an_operators_fence_is_there_once_unfenced() {
    use crate::serve::proto::{
        CreateTableRequest, FamilySpec, TablePlacementRequest, schema_client::SchemaClient,
    };
    let t = Three::start(["", "", ""]);
    let [a, b, _] = t.ports;
    t.rt.block_on(async {
        fence(b, "p2").await.unwrap();
        let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{a}"))
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
                    partitions: vec!["p2".into()],
                }),
            })
            .await
            .unwrap();
        let want = version(a).await;
        eventually("b to take the map with items", async || {
            version(b).await == want
        })
        .await;
        // Fenced by the operator: its files wait for the unfence.
        assert!(local(b, "p2").await.reconciled_version < want);
        let lp = unfence(b, "p2").await.unwrap();
        assert_eq!(lp.state(), LocalPartitionState::Serving, "{lp:?}");
        assert_eq!(local(b, "p2").await.reconciled_version, want);
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
    });
}

/// Three nodes over [`three_way`], a serving p1, b p2 and c p3; their directories, stop
/// handles (taken as each stops) and databases.
struct Rig {
    rt: tokio::runtime::Runtime,
    ports: [u16; 3],
    dirs: [PathBuf; 3],
    stops: Vec<Option<tokio::sync::oneshot::Sender<()>>>,
    dbs: Vec<Option<pigeonhole::Pigeonhole>>,
}

const NODES: [(&str, &str); 3] = [("a", "p1"), ("b", "p2"), ("c", "p3")];

impl Rig {
    fn start() -> Self {
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
        for ((l, dir), (id, p)) in listeners.into_iter().zip(&dirs).zip(NODES) {
            super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
            let (stop, db) = data_node_full(&rt, dir, id, Some(p), l, "", limits, None, false);
            stops.push(Some(stop));
            dbs.push(db);
        }
        Self {
            rt,
            ports,
            dirs,
            stops,
            dbs,
        }
    }

    /// Stops node `i` and waits until its port refuses.
    fn stop(&mut self, i: usize) {
        if let Some(stop) = self.stops[i].take() {
            let _ = stop.send(());
        }
        let port = self.ports[i];
        self.rt.block_on(eventually("the node to stop", async || {
            tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_err()
        }));
    }

    /// Starts node `i` again on its directory, as a restarted node: every partition fenced.
    fn restart(&mut self, i: usize) {
        let l = std::net::TcpListener::bind(("127.0.0.1", self.ports[i])).unwrap();
        let (stop, _) = data_node_full(
            &self.rt,
            &self.dirs[i],
            NODES[i].0,
            Some(NODES[i].1),
            l,
            "",
            crate::serve::server_tests::limits(),
            self.dbs[i].clone(),
            true,
        );
        self.stops[i] = Some(stop);
    }
}

#[test]
fn an_operators_fence_lifts_only_once_a_peers_map_confirms_it() {
    // audit120's P2-F1: `unfence` asked a peer only for a fence since the node's start. An
    // operator's fence, which every move leaves on its old node, lifted on the node's own
    // map alone. With no peer answering, it's refused now, as a start fence is; --force
    // still lifts it.
    let mut t = Rig::start();
    let a = t.ports[0];
    t.rt.block_on(async { fence(a, "p1").await.unwrap() });
    t.stop(1);
    t.stop(2);
    t.rt.block_on(async {
        let e = unfence(a, "p1").await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        assert!(e.message().contains("not a majority"), "{e}");
        assert_eq!(state(a, "p1").await, LocalPartitionState::Fenced);
        let lp = client(a)
            .await
            .unfence(PartitionRequest {
                partition: "p1".into(),
                force: true,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(lp.state(), LocalPartitionState::Serving, "{lp:?}");
    });
}

#[test]
fn a_node_whose_map_is_behind_cant_unfence_a_partition_its_peers_moved() {
    // audit120's P2-F1, the case it guards: p1 is fenced on a by the operator (as a move
    // fences its old node), and a misses the map that moves p1 to c. a's own map still names
    // it for p1, so an unfence there used to succeed: two writers for p1. The peers' newer
    // map now refuses it.
    let mut t = Rig::start();
    let [a, b, _] = t.ports;
    t.rt.block_on(async { fence(a, "p1").await.unwrap() });
    t.stop(0);
    // With a down, the map moves on without it: p1 to c at the next epoch, set at b.
    t.rt.block_on(async {
        let mut m = client(b)
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        m.map_version += 1;
        m.nodes.retain(|n| n.id != "a");
        for p in m.partitions.iter_mut().filter(|p| p.id == "p1") {
            p.node = "c".into();
            p.epoch = 2;
        }
        client(b)
            .await
            .set_map(SetMapRequest {
                force: true,
                expected_map_version: 0,
                map: Some(m),
                copies: Vec::new(),
            })
            .await
            .unwrap();
    });
    // a comes back while its peers are away, so it learns nothing at its start, and they
    // come back with a map that no longer lists a, so nothing pushes it the news.
    t.stop(1);
    t.stop(2);
    t.restart(0);
    t.restart(1);
    t.restart(2);
    t.rt.block_on(async {
        eventually("b to answer with its map", async || {
            client(b)
                .await
                .get_map(GetMapRequest {})
                .await
                .is_ok_and(|m| m.into_inner().map_version == 2)
        })
        .await;
        assert_eq!(state(a, "p1").await, LocalPartitionState::Fenced);
        assert_eq!(reason(a, "p1").await, FenceReason::Operator);
        let e = unfence(a, "p1").await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        assert_ne!(state(a, "p1").await, LocalPartitionState::Serving);
        let e = try_put(a, "alice", "1").await.unwrap_err();
        assert_ne!(code(&e), "", "a write a refused: {e}");
    });
}

#[test]
fn a_peer_that_missed_the_same_move_doesnt_confirm_an_unfence() {
    // simp724's S1 on #1826: the first peer whose map named the node was enough, so a peer
    // that missed the same move confirmed the unfence while another peer's newer map had
    // moved the partition. a and b hold version 1; c holds version 2, which gives p1 to c.
    // The newest answer counts: the unfence on a is refused.
    let mut t = Rig::start();
    let [a, _, c] = t.ports;
    t.rt.block_on(async { fence(a, "p1").await.unwrap() });
    t.stop(0);
    t.stop(1);
    // With a and b down, c alone takes a map that gives it every partition.
    t.rt.block_on(async {
        let mut m = client(c)
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        m.map_version += 1;
        m.nodes.retain(|n| n.id == "c");
        for p in m.partitions.iter_mut().filter(|p| p.node != "c") {
            p.node = "c".into();
            p.epoch += 1;
        }
        client(c)
            .await
            .set_map(SetMapRequest {
                force: true,
                expected_map_version: 0,
                map: Some(m),
                copies: Vec::new(),
            })
            .await
            .unwrap();
    });
    // a and b come back while c is away, so neither learns version 2; c comes back with a
    // map that lists only itself, so it tells nobody.
    t.stop(2);
    t.restart(0);
    t.restart(1);
    // c only once b has settled: a restarted node asks its peers for their maps until its
    // partitions are confirmed, and takes a newer one from any that answers. With c back
    // before b's p2 was confirmed (by a's version 1), b's next round took version 2 from c,
    // and "b missed the move too" no longer held (#1821's Linux run).
    let b = t.ports[1];
    t.rt.block_on(eventually(
        "b's p2 to be confirmed by a, which ends b's asking",
        async || state(b, "p2").await == LocalPartitionState::Serving,
    ));
    t.restart(2);
    t.rt.block_on(async {
        eventually("c to answer with its map", async || {
            client(c)
                .await
                .get_map(GetMapRequest {})
                .await
                .is_ok_and(|m| m.into_inner().map_version == 2)
        })
        .await;
        assert_eq!(version(t.ports[1]).await, 1, "b missed the move too");
        assert_eq!(reason(a, "p1").await, FenceReason::Operator);
        let e = unfence(a, "p1").await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        assert_ne!(state(a, "p1").await, LocalPartitionState::Serving);
    });
}

#[test]
fn a_refused_unfence_leaves_a_closed_partition_closed() {
    // simp724's N1 on #1826: the closed partition reopened before the peer check, so a
    // refused unfence left it open, its files' hashes gone.
    let mut t = Rig::start();
    let a = t.ports[0];
    t.rt.block_on(async {
        fence(a, "p1").await.unwrap();
        client(a)
            .await
            .close(PartitionRequest {
                partition: "p1".into(),
                ..Default::default()
            })
            .await
            .unwrap();
    });
    t.stop(1);
    t.stop(2);
    t.rt.block_on(async {
        let e = unfence(a, "p1").await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        assert_eq!(state(a, "p1").await, LocalPartitionState::Closed);
    });
}

/// Nodes a (p1), b (p2) and c (none), as `data_node_placed` serves them, with their `Node`s:
/// for a test that reaches into a node (its install lock, its reconciles).
struct Served {
    rt: tokio::runtime::Runtime,
    ports: [u16; 3],
    nodes: Vec<Arc<Node>>,
    stops: Vec<tokio::sync::oneshot::Sender<()>>,
    dbs: Vec<pigeonhole::Pigeonhole>,
}

impl Served {
    fn start() -> Self {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let listeners: Vec<std::net::TcpListener> = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
        let mut map = the_map(1, &ports);
        map.partitions.retain(|p| p.id != "p3");
        let mut stops = Vec::new();
        let mut dbs = Vec::new();
        let mut nodes = Vec::new();
        for (l, (id, local)) in
            listeners
                .into_iter()
                .zip([("a", Some("p1")), ("b", Some("p2")), ("c", None)])
        {
            let dir = test_dir();
            super::super::write_map(&dir, &super::super::map::canonical_json(&map)).unwrap();
            let (stop, db, node) = data_node_placed(
                &rt,
                &dir,
                id,
                local.map(|p| (p, None)).into_iter().collect(),
                l,
                "",
                crate::serve::server_tests::limits(),
                false,
                crate::serve::placement::Placement::routes_only(Some(2), Some(2)),
                None,
            );
            stops.push(stop);
            dbs.extend(db);
            nodes.push(node);
        }
        Self {
            rt,
            ports,
            nodes,
            stops,
            dbs,
        }
    }
}

impl Drop for Served {
    fn drop(&mut self) {
        self.stops.clear();
        for db in self.dbs.drain(..) {
            let _ = db.close();
        }
    }
}

#[test]
fn a_map_older_than_the_newest_reconciled_isnt_reconciled() {
    // #1848: `reconcile_locked`'s refusal (audit120's F6). Under `installing`, a map older
    // than the newest one reconciled here is passed over, since it would drop the tables
    // created since; one as new or newer is reconciled. No production path hands it an
    // older map today: it's the guard if one ever does. Seen in the version each reconcile
    // records for a's p1 (what `Status` reports as its reconciled version).
    let t = Served::start();
    let a = &t.nodes[0];
    let base = a.get_map().unwrap();
    let map = |v: u64| ClusterMap {
        map_version: v,
        ..base.clone()
    };
    let reconciled = || a.reconciled.lock().unwrap().get("p1").map(|(v, _)| *v);
    let installing = a.installing.lock().unwrap();
    let mut seen = Vec::new();
    for v in [3, 2, 3, 4, 1] {
        a.reconcile_locked(&installing, &map(v), None);
        seen.push(reconciled());
    }
    drop(installing);
    assert_eq!(seen, [Some(3), Some(3), Some(3), Some(4), Some(4)]);
}

#[test]
fn an_unfence_lifts_the_gate_only_under_the_install_lock() {
    // #1848: the unfence reconciles the partition to the node's map and lifts its gate under
    // `installing`, so a map install (and its schema reconcile) can't land between the two
    // and leave the partition taking writes without that schema (audit120's F6 and its
    // first should). With the lock held, the unfence waits, its gate still fenced; released,
    // it lifts.
    let t = Served::start();
    let (rt, ports, nodes) = (&t.rt, t.ports, &t.nodes);
    let b = ports[1];
    let gate = || nodes[1].fences.gate("p2").expect("p2 is open on b").0;
    rt.block_on(fence(b, "p2")).unwrap();
    assert!(gate().fenced().is_some());

    let installing = nodes[1].installing.lock().unwrap();
    let call = rt.spawn(unfence(b, "p2"));
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        !call.is_finished(),
        "the unfence waits for the install lock"
    );
    assert!(gate().fenced().is_some(), "and its gate is still fenced");
    drop(installing);
    // The timer is made inside the runtime: `timeout` needs one when it's built.
    rt.block_on(async { tokio::time::timeout(Duration::from_secs(30), call).await })
        .expect("the unfence finishes once the lock is free")
        .unwrap()
        .unwrap();
    assert!(gate().fenced().is_none(), "lifted once the lock was free");
}

#[test]
fn an_operators_fence_after_a_restart_still_waits_for_the_peers_and_unfence_asks_them() {
    // #1829: the operator's fence and "not confirmed since it opened" are two flags, so a
    // restarted node's operator-fenced partition reports both, and the peers' confirmation
    // clears only the second. `unfence` without --force is refused while no peer answers,
    // lifts the fence with --force, and once a peer confirms, lifts it plainly.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let map = three_way(&ports);
    let dirs = [test_dir(), test_dir(), test_dir()];
    let limits = crate::serve::server_tests::limits();
    let nodes = [("a", "p1"), ("b", "p2"), ("c", "p3")];
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for ((l, dir), (id, p)) in listeners.into_iter().zip(&dirs).zip(nodes) {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
        let (stop, db) = data_node_full(&rt, dir, id, Some(p), l, "", limits, None, false);
        stops.push(stop);
        dbs.push(db);
    }
    rt.block_on(async {
        fence(ports[1], "p2").await.unwrap();
        fence(ports[2], "p3").await.unwrap();
    });
    for stop in stops.drain(..) {
        let _ = stop.send(());
    }
    rt.block_on(async {
        for port in ports {
            eventually("the node to stop", async || {
                tokio::net::TcpStream::connect(("127.0.0.1", port))
                    .await
                    .is_err()
            })
            .await;
        }
    });
    let bind = |port: u16| std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
    let restart = |i: usize| {
        data_node_full(
            &rt,
            &dirs[i],
            nodes[i].0,
            Some(nodes[i].1),
            bind(ports[i]),
            "",
            limits,
            dbs[i].clone(),
            true,
        )
        .0
    };
    // c alone: p3 is the operator's and unconfirmed. No peer answers, so a plain unfence is
    // refused; --force lifts both.
    stops.push(restart(2));
    rt.block_on(async {
        let lp = local(ports[2], "p3").await;
        assert_eq!(lp.state(), LocalPartitionState::Fenced, "{lp:?}");
        assert_eq!(lp.fence_reason(), FenceReason::Operator, "{lp:?}");
        assert!(
            lp.awaiting_peers,
            "an operator's fence no longer hides the wait: {lp:?}"
        );
        let p3 = super::admin::info(ports[2]).await.partitions.remove(0);
        assert!(p3.awaiting_peers, "Admin.Info says it too: {p3:?}");
        let e = unfence(ports[2], "p3").await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        assert!(e.message().contains("not a majority"), "{e}");
        let lp = client(ports[2])
            .await
            .unfence(PartitionRequest {
                partition: "p3".into(),
                force: true,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(lp.state(), LocalPartitionState::Serving, "{lp:?}");
        assert!(!lp.awaiting_peers, "{lp:?}");
    });
    // a and b: b's p2 is confirmed by its peers' maps, and stays fenced by the operator
    // until a plain unfence, which its peers now confirm.
    stops.push(restart(0));
    stops.push(restart(1));
    rt.block_on(async {
        eventually("b's p2 to be confirmed by its peers", async || {
            !local(ports[1], "p2").await.awaiting_peers
        })
        .await;
        let lp = local(ports[1], "p2").await;
        assert_eq!(lp.state(), LocalPartitionState::Fenced, "{lp:?}");
        assert_eq!(lp.fence_reason(), FenceReason::Operator, "{lp:?}");
        let p2 = super::admin::info(ports[1]).await.partitions.remove(0);
        assert!(!p2.awaiting_peers, "{p2:?}");
        let e = try_put(ports[1], "hank", "1").await.unwrap_err();
        assert_eq!(code(&e), "Fenced", "{e}");
        unfence(ports[1], "p2").await.unwrap();
        assert_eq!(state(ports[1], "p2").await, LocalPartitionState::Serving);
        put(ports[1], "hank", "1").await;
    });
    drop(stops);
    for db in dbs.into_iter().flatten() {
        let _ = db.close();
    }
}

#[test]
fn a_map_that_changes_the_nodes_cant_also_move_a_partition() {
    // #1760, the coordinator's call on audit120's D2: a majority of one set of nodes
    // confirms a partition's owner, so a map set through the writer that changes the nodes
    // and moves (or re-epochs) a partition in one go is refused, naming the reason. The two
    // as separate maps are taken.
    let t = Rig::start();
    let a = t.ports[0];
    let current = || async {
        client(a)
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner()
    };
    let set = |m: ClusterMap| async move {
        let expected = m.map_version - 1;
        client(a)
            .await
            .set_map(SetMapRequest {
                force: false,
                expected_map_version: expected,
                map: Some(m),
                copies: Vec::new(),
            })
            .await
    };
    let add_d = |m: &mut ClusterMap| {
        m.nodes.push(MapNode {
            id: "d".into(),
            address: "127.0.0.1:9".into(),
        });
    };
    let move_p1 = |m: &mut ClusterMap| {
        for p in m.partitions.iter_mut().filter(|p| p.id == "p1") {
            p.node = "c".into();
            p.epoch += 1;
        }
    };
    t.rt.block_on(async {
        let mut both = current().await;
        both.map_version += 1;
        add_d(&mut both);
        move_p1(&mut both);
        let e = set(both).await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        assert!(
            e.message()
                .contains("can't also move or re-epoch a partition (partition \"p1\")"),
            "{e}"
        );
        assert_eq!(current().await.map_version, 1, "nothing was set");

        let mut nodes = current().await;
        nodes.map_version += 1;
        add_d(&mut nodes);
        set(nodes).await.unwrap();
        let mut moved = current().await;
        moved.map_version += 1;
        move_p1(&mut moved);
        set(moved).await.unwrap();
        let m = current().await;
        assert_eq!(m.map_version, 3);
        assert!(m.partitions.iter().any(|p| p.id == "p1" && p.node == "c"));
    });
}
