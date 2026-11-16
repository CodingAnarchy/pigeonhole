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
        // (cli692's S1). --force doesn't make it serve either: that takes a majority's
        // grants (D326, the owner's call).
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
        assert_eq!(lp.state(), LocalPartitionState::Fenced, "{lp:?}");
        assert_eq!(lp.fence_reason(), FenceReason::AwaitingPeerMap, "{lp:?}");
        assert!(lp.awaiting_peers, "{lp:?}");
        assert_eq!(
            code(&try_put(ports[0], "alice", "1").await.unwrap_err()),
            "Fenced"
        );
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
fn a_reconcile_outcome_goes_once_the_map_places_the_partition_elsewhere() {
    // #1830 (W4): a node kept the last reconcile outcome of every partition it ever held.
    // `Status` reads one only for a partition its map places on the node, so an outcome for
    // one the map gives another node is dropped at that map's reconcile, and kept until
    // then (a closed partition the map still places here shows its outcome as before).
    let t = Served::start();
    let a = &t.nodes[0];
    let base = a.get_map().unwrap();
    let map = |v: u64, node: &str| {
        let mut m = ClusterMap {
            map_version: v,
            ..base.clone()
        };
        for p in m.partitions.iter_mut().filter(|p| p.id == "p1") {
            p.node = node.into();
        }
        m
    };
    let reconciled = || a.reconciled.lock().unwrap().get("p1").map(|(v, _)| *v);
    let installing = a.installing.lock().unwrap();
    a.reconcile_locked(&installing, &map(3, "a"), None);
    assert_eq!(reconciled(), Some(3));
    a.reconcile_locked(&installing, &map(4, "c"), None);
    assert_eq!(reconciled(), None, "the map gives p1 to c");
    // A stale map is passed over whole: it brings nothing back.
    a.reconcile_locked(&installing, &map(3, "a"), None);
    assert_eq!(reconciled(), None);
    a.reconcile_locked(&installing, &map(5, "a"), None);
    assert_eq!(reconciled(), Some(5), "placed here again");
    drop(installing);
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
    // lifts only the operator's fence with --force (D326: never the wait for a lease), and
    // once a peer confirms, lifts it plainly.
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
    // refused; --force lifts the operator's fence, but not the wait for a lease (D326).
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
        assert_eq!(lp.state(), LocalPartitionState::Fenced, "{lp:?}");
        assert_eq!(lp.fence_reason(), FenceReason::AwaitingPeerMap, "{lp:?}");
        assert!(lp.awaiting_peers, "{lp:?}");
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

#[test]
fn a_node_change_holds_back_every_later_map_until_a_majority_before_and_after_it_holds_it() {
    // #1899's F4, with audit120's L2: a majority of one set of nodes confirms a partition's
    // owner, so a map that changes the nodes must reach a majority of the nodes before it
    // as well as of those after it before the map changes again. Here b and c are down when
    // d and e join: a, d and e are a majority of the five, but a alone isn't one of the three
    // before. Every later change is refused, another node change included, until b is back.
    let mut t = Rig::start();
    let limits = crate::serve::server_tests::limits();
    let listeners: Vec<std::net::TcpListener> = (0..2)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let new_ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let new_dirs = [test_dir(), test_dir()];
    let mut new_stops = Vec::new();
    for ((l, dir), id) in listeners.into_iter().zip(&new_dirs).zip(["d", "e"]) {
        // New nodes with no map yet: the writer's push gives them their first.
        let (stop, _) = data_node_full(&t.rt, dir, id, None, l, "", limits, None, false);
        new_stops.push(stop);
    }
    t.stop(1);
    t.stop(2);
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
    t.rt.block_on(async {
        let mut grown = current().await;
        grown.map_version += 1;
        for (id, port) in ["d", "e"].into_iter().zip(&new_ports) {
            grown.nodes.push(MapNode {
                id: id.into(),
                address: format!("127.0.0.1:{port}"),
            });
        }
        let r = set(grown).await.unwrap().into_inner();
        for (node, ok) in [("b", false), ("c", false), ("d", true), ("e", true)] {
            assert!(
                r.pushed.iter().any(|p| p.node == node && p.ok == ok),
                "{node}: {r:?}"
            );
        }
        let m = current().await;
        assert_eq!(m.map_version, 2);
        let before: Vec<&str> = m.previous_nodes.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(
            before,
            ["a", "b", "c"],
            "the writer derives the nodes before it"
        );

        // Any change, not only a move: here one that changes nothing else, and another node
        // change (dropping e).
        let mut same = m.clone();
        same.map_version += 1;
        let mut shrunk = same.clone();
        shrunk.nodes.retain(|n| n.id != "e");
        for next in [same, shrunk] {
            let e = set(next).await.unwrap_err();
            assert_eq!(code(&e), "FailedPrecondition", "{e}");
            assert!(
                e.message().contains("(1 of 3)") && e.message().contains("(3 of 5)"),
                "{e}"
            );
        }
        assert_eq!(current().await.map_version, 2, "nothing was set");
    });

    // b back: a and b are a majority of the three, so the next map is taken, and it clears
    // the nodes before (it doesn't change them itself). The writer first saw both
    // majorities hold the change now, so it takes the map only a lease and 1% later
    // (audit120's L1, D326): the nodes before may still hold grants at version 2 that the
    // nodes after never saw.
    t.restart(1);
    t.rt.block_on(async {
        let mut next = current().await;
        next.map_version += 1;
        let since = std::time::Instant::now();
        set(next).await.unwrap();
        assert!(
            since.elapsed() >= super::super::lease::grace(TEST_LEASE),
            "{:?}",
            since.elapsed()
        );
        let m = current().await;
        assert_eq!(m.map_version, 3);
        assert!(m.previous_nodes.is_empty(), "{m:?}");
    });
    drop(new_stops);
}

/// Reads `row` of `users` through node `port`.
async fn try_get(port: u16, row: &str) -> Result<(), tonic::Status> {
    data(port)
        .await
        .get(crate::serve::proto::GetRequest {
            table: "users".into(),
            row: row.as_bytes().to_vec().into(),
            family: "f".into(),
            qualifier: b"q".to_vec().into(),
            ..Default::default()
        })
        .await
        .map(drop)
}

/// Cuts node `i` of `t` off from its peers (both ways), or joins it again.
fn cut(t: &Rig, i: usize, off: bool) {
    let entry = (t.dirs[i].clone(), format!("127.0.0.1:{}", t.ports[i]));
    let mut cut = super::super::CUT_OFF
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    cut.retain(|e| *e != entry);
    if off {
        cut.push(entry);
    }
}

#[test]
fn a_node_cut_off_from_the_majority_stops_serving_within_its_lease() {
    // D326, the owner's rule: an owner cut off from the majority must not keep serving. a
    // restarts, so p1's lease comes from its rounds (a fresh rig node stands for one
    // already serving), then loses b and c while its process runs on: within a lease its
    // writes answer `Fenced`, lapsed, and its reads `PartitionUnavailable`. An operator's
    // `unfence --force` doesn't bring it back (the owner's call); a renewal that reaches
    // the majority again does.
    let mut t = Rig::start();
    t.stop(0);
    t.restart(0);
    let a = t.ports[0];
    t.rt.block_on(async {
        eventually("p1 to serve on a after its restart", async || {
            state(a, "p1").await == LocalPartitionState::Serving
        })
        .await;
        let lp = local(a, "p1").await;
        assert!(!lp.awaiting_peers, "{lp:?}");
        assert!(
            lp.lease_remaining_ms > 0 && lp.lease_remaining_ms <= 1500,
            "{lp:?}"
        );
        try_put(a, "alice", "1").await.unwrap();
        try_get(a, "alice").await.unwrap();
    });

    cut(&t, 0, true);
    let since = std::time::Instant::now();
    t.rt.block_on(async {
        eventually("p1's lease on a to run out", async || {
            reason(a, "p1").await == FenceReason::LeaseLapsed
        })
        .await;
        // Within a lease of its last renewal, which came before the cut.
        assert!(
            since.elapsed() < TEST_LEASE + Duration::from_millis(500),
            "{:?}",
            since.elapsed()
        );
        let lp = local(a, "p1").await;
        assert!(lp.awaiting_peers && lp.lease_remaining_ms == 0, "{lp:?}");
        let e = try_put(a, "alice", "2").await.unwrap_err();
        assert!(e.message().contains("lease"), "{e:?}");
        let e = try_get(a, "alice").await.unwrap_err();
        assert!(
            e.message().contains("no reads") && e.message().contains("ran out"),
            "{e:?}"
        );
        // An operator's fence lifted by force stays without a lease.
        fence(a, "p1").await.unwrap();
        let forced = client(a)
            .await
            .unfence(PartitionRequest {
                partition: "p1".into(),
                force: true,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            forced.fence_reason(),
            FenceReason::LeaseLapsed,
            "{forced:?}"
        );
        assert!(try_put(a, "alice", "3").await.is_err());
    });

    cut(&t, 0, false);
    t.rt.block_on(async {
        eventually("p1 to serve on a again", async || {
            state(a, "p1").await == LocalPartitionState::Serving
        })
        .await;
        try_put(a, "alice", "4").await.unwrap();
        try_get(a, "alice").await.unwrap();
    });
}

#[test]
fn a_restarted_node_grants_nothing_in_its_start_grace_and_a_majority_still_serves() {
    // D326: grants live in memory, so a node that starts grants no lease for L·(1+ρ), its
    // own included, and Status says how long is left. In a cluster of three, the other two
    // are a majority: the restarted node's partition serves once they grant it, before its
    // own grace ends.
    let mut t = Rig::start();
    t.stop(1);
    t.restart(1);
    let b = t.ports[1];
    t.rt.block_on(async {
        let status = client(b)
            .await
            .status(StatusRequest {})
            .await
            .unwrap()
            .into_inner();
        assert!(
            status.grace_remaining_ms > 0 && status.grace_remaining_ms <= 1515,
            "{status:?}"
        );
        eventually("p2 to serve on b", async || {
            state(b, "p2").await == LocalPartitionState::Serving
        })
        .await;
        try_put(b, "hank", "1").await.unwrap();
        eventually("b's grace to end", async || {
            client(b)
                .await
                .status(StatusRequest {})
                .await
                .unwrap()
                .into_inner()
                .grace_remaining_ms
                == 0
        })
        .await;
    });
}

#[test]
fn in_a_two_node_cluster_a_node_stops_serving_within_its_lease_when_the_other_is_down() {
    // D326, the owner's accepted effect: a majority of two is both, and a node can't tell
    // "the other node is down" from "I'm cut off". a restarts so its lease is a real one,
    // then b stops: within a lease, a serves nothing.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let mut map = the_map(1, &ports);
    map.nodes.retain(|n| n.id != "c");
    map.partitions.retain(|p| p.id != "p3");
    let dirs = [test_dir(), test_dir()];
    let limits = crate::serve::server_tests::limits();
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    let mut listeners = listeners.into_iter();
    for (dir, (id, p)) in dirs.iter().zip([("a", "p1"), ("b", "p2")]) {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
        let l = listeners.next().unwrap();
        let (stop, db) = data_node_full(&rt, dir, id, Some(p), l, "", limits, None, false);
        stops.push(Some(stop));
        dbs.push(db);
    }
    let a = ports[0];
    // a restarts on its directory and database: its p1 waits for both nodes' grants.
    let _ = stops[0].take().unwrap().send(());
    rt.block_on(eventually("a to stop", async || {
        tokio::net::TcpStream::connect(("127.0.0.1", a))
            .await
            .is_err()
    }));
    let l = std::net::TcpListener::bind(("127.0.0.1", a)).unwrap();
    let (stop, _) = data_node_full(
        &rt,
        &dirs[0],
        "a",
        Some("p1"),
        l,
        "",
        limits,
        dbs[0].clone(),
        true,
    );
    stops[0] = Some(stop);
    rt.block_on(async {
        eventually("p1 to serve on a", async || {
            state(a, "p1").await == LocalPartitionState::Serving
        })
        .await;
        try_put(a, "alice", "1").await.unwrap();
    });
    let _ = stops[1].take().unwrap().send(());
    let since = std::time::Instant::now();
    rt.block_on(async {
        eventually("p1's lease on a to run out", async || {
            reason(a, "p1").await == FenceReason::LeaseLapsed
        })
        .await;
        assert!(since.elapsed() < TEST_LEASE + Duration::from_millis(500));
        assert!(try_put(a, "alice", "2").await.is_err());
        assert!(try_get(a, "alice").await.is_err());
    });
    drop(stops);
    for db in dbs.into_iter().flatten() {
        let _ = db.close();
    }
}

#[test]
fn a_scan_running_past_its_partitions_lease_ends_with_the_lease_error() {
    // audit120's M2: each page of a scan is a read at its own snapshot, so each needs the
    // partition's lease, not only the scan's start. a restarts so p1's lease is a real one,
    // a scan of p1 through a reads small pages (two rows each), and a is cut off from its
    // peers: once its lease runs out, the scan ends with the lease error rather than reading
    // on.
    const ROWS: usize = 200;
    let mut t = Rig::start();
    t.stop(0);
    t.restart(0);
    let a = t.ports[0];
    let (mut stream, mut rows) = t.rt.block_on(async {
        eventually("p1 to serve on a after its restart", async || {
            state(a, "p1").await == LocalPartitionState::Serving
        })
        .await;
        // 32 KiB a row, so the stream's 6.4 MB can't all sit in the connection's buffers and
        // flow-control windows before the cut: the scan still has pages to read after it.
        let value = "v".repeat(32 << 10);
        for i in 0..ROWS {
            try_put(a, &format!("a{i:03}"), &value).await.unwrap();
        }
        let mut stream = data(a)
            .await
            .scan(crate::serve::proto::ScanRequest {
                table: "users".into(),
                page_bytes: 64 << 10,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let first = stream.message().await.unwrap().expect("a first page");
        assert!(
            !first.rows.is_empty() && first.rows.len() < ROWS,
            "{first:?}"
        );
        (stream, first.rows.len())
    });
    cut(&t, 0, true);
    t.rt.block_on(async {
        eventually("p1's lease on a to run out", async || {
            reason(a, "p1").await == FenceReason::LeaseLapsed
        })
        .await;
        // A page read before the lease ran out may still be on its way; then the error.
        let e = loop {
            match stream.message().await {
                Ok(Some(page)) => rows += page.rows.len(),
                Ok(None) => panic!("the scan ended cleanly after {rows} of {ROWS} rows"),
                Err(e) => break e,
            }
        };
        assert!(rows < ROWS, "{rows}");
        assert_eq!(e.code(), tonic::Code::Unavailable, "{e:?}");
        assert!(e.message().contains("lease"), "{e:?}");
    });
    cut(&t, 0, false);
}

/// The child side of `a_paused_old_owner_never_commits_once_a_new_owner_took_writes`: node
/// b, serving p2 with real lease rounds (a restart), in a process of its own so the parent
/// can stop it with SIGSTOP. Ignored, so a plain run skips it; the parent runs it with
/// `--ignored` and `PHDB_LEASE_CHILD` set to "dir|port".
#[test]
#[ignore = "the child process of a lease pause test"]
fn lease_pause_child() {
    let Ok(spec) = std::env::var("PHDB_LEASE_CHILD") else {
        return;
    };
    let (dir, port) = spec.split_once('|').expect("dir|port");
    let rt = tokio::runtime::Runtime::new().unwrap();
    let l = std::net::TcpListener::bind(("127.0.0.1", port.parse::<u16>().unwrap())).unwrap();
    let _node = data_node_full(
        &rt,
        Path::new(dir),
        "b",
        Some("p2"),
        l,
        "",
        crate::serve::server_tests::limits(),
        None,
        true,
    );
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

/// A child process killed when dropped.
#[cfg(unix)]
struct ChildNode(std::process::Child);

#[cfg(unix)]
impl ChildNode {
    /// Sends it `signal` (`-STOP`, `-CONT`) through `kill(1)`: the crate forbids `unsafe`.
    fn signal(&self, signal: &str) {
        let ok = std::process::Command::new("kill")
            .args([signal, &self.0.id().to_string()])
            .status()
            .unwrap()
            .success();
        assert!(ok, "kill {signal}");
    }
}

#[cfg(unix)]
impl Drop for ChildNode {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[cfg(unix)]
#[test]
fn a_paused_old_owner_never_commits_once_a_new_owner_took_writes() {
    // #1486 and D326, the coordinator's process-pause test: b, p2's owner, runs in a process
    // of its own and is stopped (SIGSTOP) past its lease, so it can't know it's been
    // deposed. p2 is relocated to c (`--old-unreachable`, from a copy), which takes writes.
    // b is resumed (SIGCONT): its writes are refused, or answer `OutcomeUnknown`, or reach
    // c through the newer map. None commits on b's deposed copy.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let [a, b, c] = ports;
    let map = three_way(&ports);
    let dirs = [test_dir(), test_dir(), test_dir()];
    for dir in &dirs {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
    }
    let mut listeners = listeners.into_iter();
    let la = listeners.next().unwrap();
    drop(listeners.next()); // b's port, for the child to take
    let lc = listeners.next().unwrap();
    let child = ChildNode(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "serve::cluster::tests::fence::lease_pause_child",
                "--ignored",
                "--nocapture",
            ])
            .env("PHDB_LEASE_CHILD", format!("{}|{b}", dirs[1].display()))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let limits = crate::serve::server_tests::limits();
    let (_stop_a, _db_a) =
        data_node_full(&rt, &dirs[0], "a", Some("p1"), la, "", limits, None, false);
    let (_stop_c, _db_c) =
        data_node_full(&rt, &dirs[2], "c", Some("p3"), lc, "", limits, None, false);
    // "hank" is p2's (between the splits "g" and "m").
    rt.block_on(async {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while try_put(b, "hank", "1").await.is_err() {
            assert!(
                std::time::Instant::now() < deadline,
                "b's p2 never took a write under its lease"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // b stops, as a paused VM or a stalled process would, past its lease.
    child.signal("-STOP");
    std::thread::sleep(TEST_LEASE * 2);
    // c gets p2 from a copy (the lost-node recipe), and takes writes.
    test_partition(&dirs[2], "p2").close().unwrap();
    let args = super::super::cli::NodeArgs {
        node: format!("http://127.0.0.1:{a}"),
        token_file: None,
        ca: None,
    };
    rt.block_on(async {
        within(super::super::cli::relocate(&args, "p2", "c", true, false))
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while try_put(a, "hank", "new").await.is_err() {
            assert!(
                std::time::Instant::now() < deadline,
                "c's p2 never took writes"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(get_row(c, "hank").await.unwrap(), Some(b"new".to_vec()));
    });

    // b resumes, its lease long gone on its own clock, which counted the pause.
    child.signal("-CONT");
    rt.block_on(async {
        for i in 0..30 {
            let value = format!("old{i}");
            match try_put(b, "hank", &value).await {
                // Refused (no lease, fenced, the partition gone) or unknown: nothing kept on
                // b's copy as a commit after c's.
                Err(_) => {}
                // Taken: only through the newer map, so it's on c, the new owner.
                Ok(()) => assert_eq!(
                    get_row(c, "hank").await.unwrap(),
                    Some(value.into_bytes()),
                    "a write through b that succeeded must be c's"
                ),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // And b's own copy serves nothing.
        let lp = local(b, "p2").await;
        assert_ne!(lp.state(), LocalPartitionState::Serving, "{lp:?}");
    });
    drop(child);
}
