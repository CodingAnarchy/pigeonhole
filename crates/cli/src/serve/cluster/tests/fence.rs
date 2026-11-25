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
    local_as(port, p, None).await
}

/// [`local`], with `token`.
async fn local_as(port: u16, p: &str, token: Option<&str>) -> LocalPartition {
    let s = client(port)
        .await
        .status(with(StatusRequest::default(), token))
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
            own: None,
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
    // Each port held for its node's restart, so a parallel test can't take it meanwhile.
    let held = std::sync::Mutex::new(
        ports
            .iter()
            .map(|&p| (p, HeldPort::take(p)))
            .collect::<std::collections::HashMap<_, _>>(),
    );
    let bind = |port: u16| held.lock().unwrap().remove(&port).unwrap().listen();
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
    /// Every node's token file (empty: none).
    tokens: String,
    /// A stopped node's port, held for its restart ([`HeldPort`]).
    held: [Option<HeldPort>; 3],
}

const NODES: [(&str, &str); 3] = [("a", "p1"), ("b", "p2"), ("c", "p3")];

impl Rig {
    fn start() -> Self {
        Self::start_with("")
    }

    /// [`Rig::start`], each node with the token file `tokens`.
    fn start_with(tokens: &str) -> Self {
        Self::start_p1(tokens, None)
    }

    /// [`Rig::start_with`], p1's database created with `p1` if given (small tablets, say).
    fn start_p1(tokens: &str, p1: Option<pigeonhole::Options>) -> Self {
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
        let mut p1 = p1;
        for ((l, dir), (id, p)) in listeners.into_iter().zip(&dirs).zip(NODES) {
            super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
            let given = p1
                .take()
                .filter(|_| p == "p1")
                .map(|o| test_partition_with(dir, p, o));
            let (stop, db) = data_node_full(&rt, dir, id, Some(p), l, tokens, limits, given, false);
            stops.push(Some(stop));
            dbs.push(db);
        }
        Self {
            rt,
            ports,
            dirs,
            stops,
            dbs,
            tokens: tokens.to_owned(),
            held: [None, None, None],
        }
    }

    /// Stops node `i`, waits until its port refuses, and holds the port for its restart.
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
        self.held[i] = Some(HeldPort::take(port));
    }

    /// Starts node `i` again on its directory, as a restarted node: every partition fenced.
    fn restart(&mut self, i: usize) {
        let l = self.held[i]
            .take()
            .unwrap_or_else(|| HeldPort::take(self.ports[i]))
            .listen();
        let (stop, _) = data_node_full(
            &self.rt,
            &self.dirs[i],
            NODES[i].0,
            Some(NODES[i].1),
            l,
            &self.tokens,
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
    // Each port held for its node's restart, so a parallel test can't take it meanwhile.
    let held = std::sync::Mutex::new(
        ports
            .iter()
            .map(|&p| (p, HeldPort::take(p)))
            .collect::<std::collections::HashMap<_, _>>(),
    );
    let bind = |port: u16| held.lock().unwrap().remove(&port).unwrap().listen();
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
            weight: Default::default(),
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
                weight: Default::default(),
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
    super::super::cut_node(
        &t.dirs[i],
        &format!("127.0.0.1:{}", t.ports[i]),
        &t.dirs,
        off,
    );
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
fn in_a_rolling_upgrade_a_node_beside_builds_without_leases_serves_and_still_lapses_when_cut_off() {
    // #2047: b and c act as 1.2 (no `Cluster.Lease`: `Unimplemented`), and a restarts as
    // the first node upgraded. Each of them holds the round's map, so a counts it as
    // granting, as 1.2 serves: p1 serves on a, within a's own start grace. A peer that
    // can't be reached isn't counted, so a cut off from both still stops within its lease.
    let mut t = Rig::start();
    super::super::fence::PRE_LEASE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .extend([t.dirs[1].clone(), t.dirs[2].clone()]);
    t.stop(0);
    t.restart(0);
    let a = t.ports[0];
    t.rt.block_on(async {
        eventually(
            "p1 to serve on a beside two builds without leases",
            async || state(a, "p1").await == LocalPartitionState::Serving,
        )
        .await;
        let grace = client(a)
            .await
            .status(StatusRequest::default())
            .await
            .unwrap()
            .into_inner()
            .grace_remaining_ms;
        assert!(grace > 0, "p1 served on b's and c's counts alone: {grace}");
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
        assert!(
            since.elapsed() < TEST_LEASE + Duration::from_millis(500),
            "{:?}",
            since.elapsed()
        );
        assert!(try_put(a, "alice", "2").await.is_err());
    });
    cut(&t, 0, false);
    super::super::fence::PRE_LEASE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|d| *d != t.dirs[1] && *d != t.dirs[2]);
}

#[test]
fn a_peer_reporting_leases_whose_lease_call_answers_unimplemented_counts_as_no_grant() {
    // audit120's S1 on #2048: a 1.3 node behind a proxy that doesn't route `Cluster.Lease`
    // answers it `Unimplemented` (an HTTP 404) while its `Status` reports `leases`. It must
    // count as nothing, not as a 1.2 node's grant: b and c are such nodes, so a, restarted,
    // has at most its own grant, never a majority, and p1 stays fenced. Routed again, p1
    // serves.
    let mut t = Rig::start();
    super::super::fence::LEASE_UNROUTED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .extend([t.dirs[1].clone(), t.dirs[2].clone()]);
    t.stop(0);
    t.restart(0);
    let a = t.ports[0];
    t.rt.block_on(async {
        // Past a's start grace, through several rounds.
        tokio::time::sleep(2 * TEST_LEASE).await;
        assert_eq!(state(a, "p1").await, LocalPartitionState::Fenced);
        assert!(try_put(a, "alice", "1").await.is_err());
    });
    super::super::fence::LEASE_UNROUTED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|d| *d != t.dirs[1] && *d != t.dirs[2]);
    t.rt.block_on(async {
        eventually("p1 to serve on a once b and c answer Lease", async || {
            state(a, "p1").await == LocalPartitionState::Serving
        })
        .await;
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
            .status(StatusRequest::default())
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
                .status(StatusRequest::default())
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
        own: None,
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

/// Node `port`'s map.
async fn map_of(port: u16) -> crate::serve::proto::ClusterMap {
    map_of_as(port, None).await
}

/// [`map_of`], with `token`.
async fn map_of_as(port: u16, token: Option<&str>) -> crate::serve::proto::ClusterMap {
    client(port)
        .await
        .get_map(with(crate::serve::proto::GetMapRequest {}, token))
        .await
        .unwrap()
        .into_inner()
}

/// Node `port`'s status.
async fn status_of(port: u16) -> crate::serve::proto::NodeStatus {
    status_of_as(port, None).await
}

/// [`status_of`], with `token`.
async fn status_of_as(port: u16, token: Option<&str>) -> crate::serve::proto::NodeStatus {
    client(port)
        .await
        .status(with(StatusRequest::default(), token))
        .await
        .unwrap()
        .into_inner()
}

/// Sets `map` at node `port` (the writer), over its current version.
async fn set_at(port: u16, map: crate::serve::proto::ClusterMap) -> Result<u64, tonic::Status> {
    set_at_as(port, map, None).await
}

/// [`set_at`], with `token`.
async fn set_at_as(
    port: u16,
    map: crate::serve::proto::ClusterMap,
    token: Option<&str>,
) -> Result<u64, tonic::Status> {
    let expected = map.map_version - 1;
    client(port)
        .await
        .set_map(with(
            crate::serve::proto::SetMapRequest {
                map: Some(map),
                expected_map_version: expected,
                copies: Vec::new(),
                force: false,
            },
            token,
        ))
        .await
        .map(|r| r.into_inner().map_version)
}

/// The map at node `port`, one version on, with failover `on`.
async fn with_failover(port: u16, on: bool) -> crate::serve::proto::ClusterMap {
    let mut m = map_of(port).await;
    m.map_version += 1;
    m.failover = on;
    m
}

/// Restarts every node, so each runs its lease rounds as `run` starts them (a fresh rig
/// node doesn't), and waits until a writes and b grants a's writer lease (D330): the term-0
/// writer, the smallest id, asks for it in its lease rounds. Then until every node's start
/// grace is over: a restarted node grants nothing in it, so a test that cuts a node off
/// sooner leaves the rest short of a majority (a Windows CI failure on #2069).
fn writer_leased(t: &mut Rig) {
    writer_leased_as(t, None);
}

/// [`writer_leased`], asking with `token`.
fn writer_leased_as(t: &mut Rig, token: Option<&str>) {
    for i in 0..3 {
        t.stop(i);
        t.restart(i);
    }
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        writer_lease_held_as(a, b, token).await;
        for port in [a, b, c] {
            eventually("every node's start grace to end", async || {
                status_of_as(port, token).await.grace_remaining_ms == 0
            })
            .await;
        }
    });
}

async fn writer_lease_held_as(a: u16, b: u16, token: Option<&str>) {
    eventually("p1 to serve on a", async || {
        local_as(a, "p1", token).await.state() == LocalPartitionState::Serving
    })
    .await;
    eventually("b to grant a's writer lease", async || {
        status_of_as(b, token).await.writer_lease_remaining_ms > 0
    })
    .await;
    let st = status_of_as(a, token).await;
    assert_eq!((st.writer.as_str(), st.writer_term), ("a", 0), "{st:?}");
    assert!(st.writer_lease_remaining_ms > 0, "{st:?}");
}

#[test]
fn with_failover_on_a_cut_off_writer_is_replaced_and_rejoins_behind_the_elected_map() {
    // D330 PR 1, tests 3 and 4: the writer a is cut off; once its writer lease has been out
    // for the margin, b (the smallest live id) is elected at term 1 from the majority's
    // newest map. a's own change meanwhile, at term 0, stays on a: a late push of it is
    // refused by the term fence, and once a rejoins it takes the elected map, with every
    // partition serving again (simp724's S1 on the plan).
    let mut t = Rig::start();
    writer_leased(&mut t);
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        let on = with_failover(a, true).await;
        set_at(a, on).await.unwrap();
        eventually("c to hold failover on", async || map_of(c).await.failover).await;
    });
    let held = t.rt.block_on(map_of(c));
    cut(&t, 0, true);
    t.rt.block_on(async {
        // L for a's lease, the margin (2·L), and a round or two.
        eventually_within(Duration::from_secs(30), "b to be elected", async || {
            let m = map_of(c).await;
            m.writer == "b" && m.writer_term == 1
        })
        .await;
        let elected = map_of(b).await;
        assert_eq!(
            (
                elected.writer.as_str(),
                elected.writer_term,
                elected.map_version
            ),
            ("b", 1, held.map_version + 1),
            "{elected:?}"
        );
        assert!(elected.failover);
        let st = status_of(c).await;
        assert_eq!((st.writer.as_str(), st.writer_term), ("b", 1), "{st:?}");
        eventually("b to hold its writer lease", async || {
            status_of(b).await.writer_lease_remaining_ms > 0
        })
        .await;
        // a, cut off, changes its own map at term 0, which needs no writer lease: a
        // minority change.
        let mut minority = map_of(a).await;
        assert_eq!(minority.writer_term, 0);
        minority.map_version += 1;
        minority.partitions[0].memory_bytes = 1 << 20;
        set_at(a, minority).await.unwrap();
        // Its push, arriving late at a node that voted for term 1, is refused.
        let late = map_of(a).await;
        let e = client(c)
            .await
            .push_map(crate::serve::proto::PushMapRequest { map: Some(late) })
            .await
            .unwrap_err();
        assert!(e.message().contains("term"), "{e:?}");
    });
    cut(&t, 0, false);
    t.rt.block_on(async {
        eventually_within(
            Duration::from_secs(15),
            "a to take the elected map",
            async || map_of(a).await.writer_term == 1,
        )
        .await;
        // No two maps at one (term, version): every node holds the same one.
        let want = super::super::map::canonical_json(&map_of(b).await);
        for port in t.ports {
            assert_eq!(super::super::map::canonical_json(&map_of(port).await), want);
        }
        assert_eq!(
            map_of(a).await.partitions[0].memory_bytes,
            0,
            "a's change is lost"
        );
        // Every partition serves again, a's included.
        eventually_within(
            Duration::from_secs(10),
            "p1 to serve on a again",
            async || state(a, "p1").await == LocalPartitionState::Serving,
        )
        .await;
        for port in t.ports {
            for row in ["alice", "mike", "zed"] {
                try_put(port, row, "v").await.unwrap();
            }
        }
        // b now sets maps; a refuses one, naming b.
        let e = set_at(a, with_failover(a, true).await).await.unwrap_err();
        assert!(e.message().contains("node b"), "{e:?}");
        set_at(b, with_failover(b, false).await).await.unwrap();
    });
}

#[test]
fn with_failover_off_no_node_takes_over_and_a_forced_map_fences_the_old_writer() {
    // D330 C2 (the coordinator's rule): with failover off, nothing elects a writer. The
    // operator's forced map raises the term (simp724's S2), so the old writer, back, takes
    // it and can't change the map.
    let mut t = Rig::start();
    writer_leased(&mut t);
    let [a, b, c] = t.ports;
    cut(&t, 0, true);
    t.rt.block_on(async {
        // Well past L and the margin.
        tokio::time::sleep(TEST_LEASE * 5).await;
        for port in [b, c] {
            let m = map_of(port).await;
            assert_eq!((m.writer.as_str(), m.writer_term), ("", 0), "{m:?}");
        }
        let mut forced = map_of(b).await;
        forced.writer = "b".into();
        super::super::cli::set_map(&super::backup::args(b), forced, true, Vec::new())
            .await
            .unwrap();
        for port in [b, c] {
            eventually("the forced map", async || {
                let m = map_of(port).await;
                m.writer == "b" && m.writer_term == 1
            })
            .await;
        }
    });
    cut(&t, 0, false);
    t.rt.block_on(async {
        eventually_within(
            Duration::from_secs(15),
            "a to take the forced map",
            async || map_of(a).await.writer_term == 1,
        )
        .await;
        let e = set_at(a, with_failover(a, false).await).await.unwrap_err();
        assert!(e.message().contains("node b"), "{e:?}");
        let st = status_of(a).await;
        assert_eq!((st.writer.as_str(), st.writer_term), ("b", 1), "{st:?}");
    });
}

// D330 PR 2: partition failover by the map's writer (design §8 tests 1, 2, 6 and 7, and
// the plan's S1 and S3), on a rig whose nodes run their lease rounds: a writes the map,
// c owns p3 (rows from "m"), and b holds p3's replica copy.

/// The map at the writer (`port`) with p3's replicas as `replicas`, set there; retried
/// while a node that just started still takes its peers' maps.
async fn p3_replicas(port: u16, replicas: &[&str]) {
    let mut last = None;
    for _ in 0..100 {
        let mut m = map_of(port).await;
        m.map_version += 1;
        for p in m.partitions.iter_mut().filter(|p| p.id == "p3") {
            p.replicas = replicas.iter().map(|r| (*r).to_owned()).collect();
        }
        match set_at(port, m).await {
            Ok(_) => return,
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("set p3's replicas: {last:?}");
}

/// Node `port`'s replica copy of `p`, if it reports one.
async fn replica_row(port: u16, p: &str) -> Option<LocalPartition> {
    status_of(port)
        .await
        .partitions
        .into_iter()
        .find(|lp| lp.id == p && lp.role() == crate::serve::proto::PartitionRole::Replica)
}

/// A leased rig with p3's replica copies on `replicas` (b first) following c past a write of
/// "zed", and failover `on`.
fn failover_rig_with(on: bool, replicas: &[&str]) -> Rig {
    let mut t = Rig::start();
    writer_leased(&mut t);
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        p3_replicas(a, replicas).await;
        if on {
            set_at(a, with_failover(a, true).await).await.unwrap();
        }
        eventually("p3 to serve on c", async || {
            state(c, "p3").await == LocalPartitionState::Serving
        })
        .await;
        try_put(a, "zed", "v1").await.unwrap();
        eventually_within(
            Duration::from_secs(15),
            "b's copy of p3 to follow c",
            async || {
                replica_row(b, "p3").await.is_some_and(|lp| {
                    lp.replica_state() == crate::serve::proto::ReplicaState::Following
                        && lp.replica_of == "c"
                        && lp.replica_applied_seqno > 0
                })
            },
        )
        .await;
        if replicas.contains(&"a") {
            eventually_within(
                Duration::from_secs(15),
                "a's copy of p3 to follow c",
                async || {
                    replica_row(a, "p3").await.is_some_and(|lp| {
                        lp.replica_state() == crate::serve::proto::ReplicaState::Following
                            && lp.replica_of == "c"
                    })
                },
            )
            .await;
        }
        eventually("c to hold failover's map", async || {
            map_of(c).await.failover == on
        })
        .await;
        // The writer has seen c renew p3 since it started (after its start grace): without
        // that, it fails nothing over by design (simp724's B1 on #2069), and a test that cuts
        // c sooner raced it (a macOS CI failure on #2087).
        eventually_within(Duration::from_secs(10), "a to grant c's p3", async || {
            status_of(a)
                .await
                .grants
                .iter()
                .any(|g| g.partition == "p3" && g.holder == "c")
        })
        .await;
    });
    t
}

/// [`failover_rig_with`] b's copy alone.
fn failover_rig(on: bool) -> Rig {
    failover_rig_with(on, &["b"])
}

/// Whether the writer at `port` reports p3 down and not failed over for a reason that
/// starts with `why`.
async fn blocked_for(port: u16, why: &str) -> bool {
    status_of(port)
        .await
        .failover_blocked
        .get("p3")
        .is_some_and(|r| r.starts_with(why))
}

/// The failover margin and a few rounds past the owner's lease: within it, a down
/// partition is failed over (design §5's timeline at the test lease).
const FAILOVER_WINDOW: Duration = Duration::from_secs(30);

#[test]
fn a_killed_owner_is_failed_over_to_its_fresh_replica() {
    // §8 test 1, and simp724's B1: c dies between its renewals; once a's grant to it has
    // been out for the margin, a promotes the freshest copy of p3 (b's or its own), records
    // the event, and p3 takes writes through every node there, with what c committed
    // before it died. The other copy follows the new owner within a second of the map
    // (D330 amendment 2: the map wakes its follower).
    let t = failover_rig_with(true, &["b", "a"]);
    let [a, b, _] = t.ports;
    let before = t.rt.block_on(map_of(a));
    let epoch = before
        .partitions
        .iter()
        .find(|p| p.id == "p3")
        .unwrap()
        .epoch;
    // Dead: unreachable, and its rounds reach no one (a rig node's tasks outlive its
    // server, so stopping it alone would leave its lease rounds running).
    cut(&t, 2, true);
    t.rt.block_on(async {
        eventually_within(FAILOVER_WINDOW, "p3 to fail over", async || {
            map_of(a)
                .await
                .partitions
                .iter()
                .any(|p| p.id == "p3" && p.node != "c")
        })
        .await;
        let failed_over = tokio::time::Instant::now();
        let m = map_of(a).await;
        let p3 = m.partitions.iter().find(|p| p.id == "p3").unwrap().clone();
        let (to, to_port, other, other_port) = if p3.node == "b" {
            ("b", b, "a", a)
        } else {
            ("a", a, "b", b)
        };
        // The other copy, woken by the map, follows the new owner at once rather than
        // after its retries' pause.
        eventually("the other copy to follow the new owner", async || {
            replica_row(other_port, "p3")
                .await
                .is_some_and(|lp| lp.replica_of == to)
        })
        .await;
        assert!(
            failed_over.elapsed() < Duration::from_secs(1) + TEST_LEASE,
            "the other copy took {:?} to follow {to}",
            failed_over.elapsed()
        );
        assert_eq!((p3.epoch, p3.failover_count), (epoch + 1, 1), "{p3:?}");
        assert_eq!(p3.replicas, vec![other.to_owned()], "{p3:?}");
        let e = m.failovers.last().expect("an event");
        assert_eq!(
            (
                e.kind(),
                e.state(),
                e.partition.as_str(),
                e.from.as_str(),
                e.to.as_str()
            ),
            (
                crate::serve::proto::FailoverKind::Partition,
                crate::serve::proto::FailoverState::Done,
                "p3",
                "c",
                to
            ),
            "{e:?}"
        );
        assert_eq!(e.to_epoch, epoch + 1);
        // The owner's frontier as the copy last heard it: 0 when its follower hadn't heard one
        // since it last reconnected (an unknown, not a loss of 0).
        assert!(
            e.applied_seqno > 0 && (e.primary_seqno == 0 || e.primary_seqno >= e.applied_seqno),
            "{e:?}"
        );
        eventually_within(
            Duration::from_secs(10),
            "p3 to serve on its new node",
            async || state(to_port, "p3").await == LocalPartitionState::Serving,
        )
        .await;
        for port in [a, b] {
            eventually("a write to p3 through each node", async || {
                try_put(port, "zed", "v2").await.is_ok()
            })
            .await;
        }
        assert_eq!(get_row(a, "zed").await.unwrap(), Some(b"v2".to_vec()));
        assert!(
            !status_of(a).await.failover_blocked.contains_key("p3"),
            "nothing blocked once it's failed over"
        );
    });
}

#[test]
fn with_failover_off_a_dead_owner_stays_put_and_the_writer_says_why() {
    // §8 test 6: the map's failover off, c dies: nothing is promoted, and the writer's
    // status says why.
    let t = failover_rig(false);
    let a = t.ports[0];
    cut(&t, 2, true);
    t.rt.block_on(async {
        eventually_within(FAILOVER_WINDOW, "a to report p3 down", async || {
            blocked_for(a, "failover off").await
        })
        .await;
        let m = map_of(a).await;
        let p3 = m.partitions.iter().find(|p| p.id == "p3").unwrap();
        assert_eq!((p3.node.as_str(), p3.failover_count), ("c", 0), "{p3:?}");
        assert!(m.failovers.is_empty(), "{:?}", m.failovers);
    });
}

#[test]
fn an_owner_cut_off_from_the_writer_alone_is_not_failed_over() {
    // §8 test 7, with simp724's B2: c can't reach a or a c, but b still grants c its lease,
    // so c serves p3; a, the writer, sees b's grant and waits.
    let t = failover_rig(true);
    let [a, _, c] = t.ports;
    let pair = |from: usize, to: u16| (t.dirs[from].clone(), format!("127.0.0.1:{to}"));
    super::super::CUT_BETWEEN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .extend([pair(0, c), pair(2, a)]);
    t.rt.block_on(async {
        eventually_within(FAILOVER_WINDOW, "a to wait on b's grant to c", async || {
            blocked_for(a, "waiting: node b still grants its lease to node c").await
        })
        .await;
        // Past the window: still c's, serving.
        tokio::time::sleep(TEST_LEASE * 3).await;
        let m = map_of(a).await;
        let p3 = m.partitions.iter().find(|p| p.id == "p3").unwrap();
        assert_eq!((p3.node.as_str(), p3.failover_count), ("c", 0), "{p3:?}");
        assert_eq!(state(c, "p3").await, LocalPartitionState::Serving);
    });
    let (ca, cc) = (pair(0, c), pair(2, a));
    super::super::CUT_BETWEEN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|e| *e != ca && *e != cc);
}

#[test]
fn a_newly_elected_writer_doesnt_fail_over_owners_that_renew() {
    // simp724's S3: b, elected when a is cut off, holds no grant history from a's term;
    // c renews p3 with b throughout, so b fails nothing over. a's own p1, which no one
    // renews now, is reported (it has no replica), not moved.
    let t = failover_rig(true);
    let [_, b, _] = t.ports;
    cut(&t, 0, true);
    t.rt.block_on(async {
        eventually_within(FAILOVER_WINDOW, "b to be elected", async || {
            let m = map_of(b).await;
            m.writer == "b" && m.writer_term == 1
        })
        .await;
        // The election is recorded (§6).
        let m = map_of(b).await;
        let e = m.failovers.last().expect("the election's event");
        assert_eq!(
            (e.kind(), e.from.as_str(), e.to.as_str(), e.writer_term),
            (crate::serve::proto::FailoverKind::Writer, "a", "b", 1),
            "{e:?}"
        );
        eventually_within(FAILOVER_WINDOW, "b to report a's p1", async || {
            status_of(b)
                .await
                .failover_blocked
                .get("p1")
                .is_some_and(|r| r == "no replicas")
        })
        .await;
        tokio::time::sleep(TEST_LEASE * 3).await;
        let m = map_of(b).await;
        let p3 = m.partitions.iter().find(|p| p.id == "p3").unwrap();
        assert_eq!((p3.node.as_str(), p3.failover_count), ("c", 0), "{p3:?}");
        assert!(
            !status_of(b).await.failover_blocked.contains_key("p3"),
            "p3 isn't down"
        );
    });
    cut(&t, 0, false);
}

#[test]
fn a_promote_stopped_after_its_stamp_is_finished_from_its_pending_event() {
    // The plan's S1, finish only (F4): a stamps b's copy for p3, then stops before its map,
    // as a crash there leaves it. The map holds the promote pending; the next failover check
    // finds b's copy stamped at the pending epoch and finishes it, without a second stamp
    // or a second event.
    let t = failover_rig(true);
    let [a, b, _] = t.ports;
    super::super::failover::STOP_AFTER_STAMP
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(t.dirs[0].clone());
    cut(&t, 2, true);
    t.rt.block_on(async {
        eventually_within(FAILOVER_WINDOW, "the promote to be pending", async || {
            map_of(a).await.failovers.iter().any(|e| {
                e.partition == "p3"
                    && e.state() == crate::serve::proto::FailoverState::Pending
                    && e.to == "b"
            })
        })
        .await;
        eventually_within(FAILOVER_WINDOW, "p3 to fail over to b", async || {
            let m = map_of(a).await;
            m.partitions.iter().any(|p| p.id == "p3" && p.node == "b")
                && m.failovers.iter().any(|e| {
                    e.partition == "p3" && e.state() == crate::serve::proto::FailoverState::Done
                })
        })
        .await;
        let m = map_of(a).await;
        let events = m
            .failovers
            .iter()
            .filter(|e| e.partition == "p3")
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 1, "one promote, finished: {events:?}");
        let p3 = m.partitions.iter().find(|p| p.id == "p3").unwrap();
        assert_eq!(
            (p3.epoch, p3.failover_count),
            (events[0].to_epoch, 1),
            "{p3:?}"
        );
        eventually_within(Duration::from_secs(10), "p3 to serve on b", async || {
            state(b, "p3").await == LocalPartitionState::Serving
        })
        .await;
    });
}

#[test]
fn a_black_holed_peer_holds_up_neither_renewals_nor_an_election_nor_a_failover() {
    // #2056's election stall (simp724's S1 on the PR 2 plan): a peer that takes a
    // connection and never answers `Status`. c can't reach a or b (its renewals stop) but
    // they still connect to it, and its `Status` hangs. a's p1 and b's p2 serve throughout,
    // and a fails p3 over within the window: its survey counts c as unknown, not granting.
    let t = failover_rig(true);
    let [a, b, c] = t.ports;
    let pair = |from: usize, to: u16| (t.dirs[from].clone(), format!("127.0.0.1:{to}"));
    super::super::CUT_BETWEEN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .extend([pair(2, a), pair(2, b)]);
    super::super::HOLD_STATUS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(t.dirs[2].clone());
    t.rt.block_on(async {
        let deadline = tokio::time::Instant::now() + FAILOVER_WINDOW;
        loop {
            assert_eq!(
                state(a, "p1").await,
                LocalPartitionState::Serving,
                "a's renewals"
            );
            assert_eq!(
                state(b, "p2").await,
                LocalPartitionState::Serving,
                "b's renewals"
            );
            if map_of(a)
                .await
                .partitions
                .iter()
                .any(|p| p.id == "p3" && p.node == "b")
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "p3 wasn't failed over: {:?}",
                status_of(a).await.failover_blocked
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let _ = c;
    });
    super::super::HOLD_STATUS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|d| *d != t.dirs[2]);
    let (ca, cb) = (pair(2, a), pair(2, b));
    super::super::CUT_BETWEEN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|e| *e != ca && *e != cb);
}

#[test]
fn a_black_holed_peer_doesnt_hold_up_an_election_or_the_voters_renewals() {
    // #2056's election stall: a, the writer, is cut off; c's `Status` hangs. b's survey
    // counts c as unknown within its deadline: b is elected, and its p2 serves throughout.
    let t = failover_rig(true);
    let [_, b, _] = t.ports;
    super::super::HOLD_STATUS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(t.dirs[2].clone());
    cut(&t, 0, true);
    t.rt.block_on(async {
        let deadline = tokio::time::Instant::now() + FAILOVER_WINDOW;
        loop {
            assert_eq!(
                state(b, "p2").await,
                LocalPartitionState::Serving,
                "b's renewals"
            );
            let m = map_of(b).await;
            if m.writer == "b" && m.writer_term == 1 {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "b wasn't elected");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    cut(&t, 0, false);
    super::super::HOLD_STATUS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|d| *d != t.dirs[2]);
}

/// The map at the writer (`port`) with partition `p`'s replicas as `replicas`, retried as
/// [`p3_replicas`].
async fn replicas_of(port: u16, p: &str, replicas: &[&str]) {
    let mut last = None;
    for _ in 0..100 {
        let mut m = map_of(port).await;
        m.map_version += 1;
        for q in m.partitions.iter_mut().filter(|q| q.id == p) {
            q.replicas = replicas.iter().map(|r| (*r).to_owned()).collect();
        }
        match set_at(port, m).await {
            Ok(_) => return,
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("set {p}'s replicas: {last:?}");
}

#[test]
fn when_the_writer_that_owns_a_partition_dies_the_next_writer_fails_it_over() {
    // simp724's B1 on #2069: a, the writer and p1's owner, dies. b is elected; its own grant
    // of p1 to a (b is one of p1's grantors) dates a's last renewal, whatever b's office time,
    // so b's copy of p1 is fresh enough and b fails p1 over to it.
    let mut t = Rig::start();
    writer_leased(&mut t);
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        replicas_of(a, "p1", &["b"]).await;
        set_at(a, with_failover(a, true).await).await.unwrap();
        try_put(a, "alice", "v1").await.unwrap();
        eventually_within(
            Duration::from_secs(15),
            "b's copy of p1 to follow a",
            async || {
                replica_row(b, "p1").await.is_some_and(|lp| {
                    lp.replica_state() == crate::serve::proto::ReplicaState::Following
                        && lp.replica_of == "a"
                        && lp.replica_applied_seqno > 0
                })
            },
        )
        .await;
        eventually("c to hold failover's map", async || {
            map_of(c).await.failover
        })
        .await;
    });
    cut(&t, 0, true);
    t.rt.block_on(async {
        eventually_within(FAILOVER_WINDOW * 2, "p1 to fail over to b", async || {
            let m = map_of(b).await;
            m.writer == "b" && m.partitions.iter().any(|p| p.id == "p1" && p.node == "b")
        })
        .await;
        eventually_within(Duration::from_secs(10), "p1 to serve on b", async || {
            state(b, "p1").await == LocalPartitionState::Serving
        })
        .await;
        assert_eq!(get_row(b, "alice").await.unwrap(), Some(b"v1".to_vec()));
    });
    cut(&t, 0, false);
}

#[test]
fn an_owner_that_answers_but_reaches_no_one_is_failed_over() {
    // The owner's lease lapsed (it can't reach its grantors) but it still answers Status, as
    // Fenced awaiting its peers: that's no operator's fence, so the writer fails it over.
    let t = failover_rig(true);
    let [a, b, c] = t.ports;
    let pair = |from: usize, to: u16| (t.dirs[from].clone(), format!("127.0.0.1:{to}"));
    super::super::CUT_BETWEEN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .extend([pair(2, a), pair(2, b)]);
    t.rt.block_on(async {
        eventually_within(FAILOVER_WINDOW, "p3 to fail over to b", async || {
            map_of(a)
                .await
                .partitions
                .iter()
                .any(|p| p.id == "p3" && p.node == "b")
        })
        .await;
        let _ = c;
    });
    let (ca, cb) = (pair(2, a), pair(2, b));
    super::super::CUT_BETWEEN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|e| *e != ca && *e != cb);
}

#[test]
fn an_owner_cut_off_from_every_node_takes_no_write_once_failed_over_and_serves_nothing_healed() {
    // §8 test 2, the split-brain case: c, p3's owner, is cut off from every node but alive.
    // The grant round, not the survey, keeps two owners from serving: once b serves p3, no
    // write through c is taken; healed, c takes the new map, serves nothing for p3, and a
    // write through it lands on b.
    let t = failover_rig(true);
    let [a, b, c] = t.ports;
    cut(&t, 2, true);
    t.rt.block_on(async {
        eventually_within(FAILOVER_WINDOW, "p3 to fail over to b", async || {
            map_of(a)
                .await
                .partitions
                .iter()
                .any(|p| p.id == "p3" && p.node == "b")
        })
        .await;
        eventually_within(Duration::from_secs(10), "p3 to serve on b", async || {
            state(b, "p3").await == LocalPartitionState::Serving
        })
        .await;
        try_put(a, "zed", "new").await.unwrap();
        // c, alone, has no lease: every write through it now is refused.
        for i in 0..20 {
            let r = try_put(c, "zed", &format!("old{i}")).await;
            assert!(
                r.is_err(),
                "c took a write after b became p3's owner: {r:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(get_row(a, "zed").await.unwrap(), Some(b"new".to_vec()));
    });
    cut(&t, 2, false);
    t.rt.block_on(async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let m = map_of(c).await;
            if m.partitions.iter().any(|p| p.id == "p3" && p.node == "b") {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "c never took the new map: c holds version {} (term {}), a {}; c's status {:?}",
                m.map_version,
                m.writer_term,
                map_of(a).await.map_version,
                status_of(c).await
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let lp =
            status_of(c).await.partitions.into_iter().find(|lp| {
                lp.id == "p3" && lp.role() != crate::serve::proto::PartitionRole::Replica
            });
        assert!(
            lp.as_ref()
                .is_none_or(|lp| lp.state() != LocalPartitionState::Serving),
            "c serves p3 after it moved: {lp:?}"
        );
        eventually("a write through c to land on b", async || {
            try_put(c, "zed", "healed").await.is_ok()
        })
        .await;
        assert_eq!(get_row(b, "zed").await.unwrap(), Some(b"healed".to_vec()));
    });
}

#[test]
fn a_blocked_partition_is_surveyed_again_only_when_its_block_can_have_ended() {
    // perf287's S1 on #2069: while test 7's cut holds, p3 stays blocked ("still grants");
    // the writer surveys again when that grant can have run out (simp724's nit on #2069,
    // #2073), not on every lease round.
    let t = failover_rig(true);
    let [a, _, c] = t.ports;
    let pair = |from: usize, to: u16| (t.dirs[from].clone(), format!("127.0.0.1:{to}"));
    super::super::CUT_BETWEEN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .extend([pair(0, c), pair(2, a)]);
    let surveys = || {
        super::super::failover::SURVEYS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|(d, _)| *d == t.dirs[0])
            .map_or(0, |(_, n)| *n)
    };
    t.rt.block_on(async {
        eventually_within(FAILOVER_WINDOW, "a to wait on b's grant to c", async || {
            blocked_for(a, "waiting: node b still grants its lease to node c").await
        })
        .await;
        let before = surveys();
        let held = TEST_LEASE * 4;
        tokio::time::sleep(held).await;
        let more = surveys() - before;
        // b renews its grant to c every third of a lease, so at each survey it has between
        // 2L/3 and L left: one survey per 2L/3 at most, six in 4L, plus one at an edge (a
        // Linux CI run on #2087 saw six against the bound of five #2073 set). A survey on
        // every lease round would have made twelve.
        assert!(
            more <= 7,
            "{more} surveys in {held:?} of a blocked partition"
        );
        assert!(blocked_for(a, "waiting: node b").await, "still reported");
    });
    let (ac, ca) = (pair(0, c), pair(2, a));
    super::super::CUT_BETWEEN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|e| *e != ac && *e != ca);
}

#[test]
fn a_node_stranded_by_a_lost_vote_takes_the_live_map_once_the_writer_adopts_its_term() {
    // #2073: with a, the writer, cut off, c passes its pre-vote but its vote requests are
    // dropped, so it records its own vote and loses (b holds no election). Healed, a is the
    // writer again, at the lower term, and c's term fence refuses its next map. a adopts the
    // term above c's vote, and c takes the map and serves p3 again. Before a adopts it
    // (held off by a seam), c, refusing every push, mustn't stop a's and b's leases: a
    // fence-refused push counts as no answer, not as behind (relaxing audit120's F2 on
    // #1899).
    let t = failover_rig(true);
    let [a, b, c] = t.ports;
    // p3 opts out of failover, so it's still c's when c takes the map: while c is stranded
    // its lease lapses, and the writer would otherwise fail it over to b.
    t.rt.block_on(async {
        let mut m = map_of(a).await;
        m.map_version += 1;
        for p in m.partitions.iter_mut().filter(|p| p.id == "p3") {
            p.set_failover(crate::serve::proto::FailoverMode::Off);
        }
        set_at(a, m).await.unwrap();
    });
    let seams = || {
        [
            (&super::super::NO_ELECTION, t.dirs[1].clone()),
            (&super::super::DROP_VOTES, t.dirs[2].clone()),
        ]
    };
    for (seam, dir) in seams() {
        seam.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(dir);
    }
    super::super::NO_ADOPT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(t.dirs[0].clone());
    cut(&t, 0, true);
    let voted = t.rt.block_on(async {
        eventually_within(FAILOVER_WINDOW, "c to lose an election", async || {
            status_of(c).await.writer_term > 0
        })
        .await;
        status_of(c).await.writer_term
    });
    for (seam, dir) in seams() {
        seam.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|d| *d != dir);
    }
    cut(&t, 0, false);
    t.rt.block_on(async {
        // a's next map, once it holds its writer lease again: c's term fence refuses it.
        let mut last = None;
        let mut changed = false;
        for _ in 0..200 {
            let mut m = map_of(a).await;
            m.map_version += 1;
            match set_at(a, m).await {
                Ok(_) => {
                    changed = true;
                    break;
                }
                Err(e) => last = Some(e),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(changed, "a changed no map: {last:?}");
        // Still stranded, held there by the seam: a and b keep their partition leases, and a
        // its writer lease, past any lease from before the change.
        eventually_within(Duration::from_secs(10), "p1 to serve on a", async || {
            state(a, "p1").await == LocalPartitionState::Serving
        })
        .await;
        tokio::time::sleep(TEST_LEASE * 3).await;
        let (ours, theirs) = (map_of(a).await, map_of(c).await);
        assert!(
            theirs.map_version < ours.map_version,
            "c took a's map: {theirs:?}"
        );
        assert_eq!(
            state(a, "p1").await,
            LocalPartitionState::Serving,
            "p1 on a"
        );
        assert_eq!(
            state(b, "p2").await,
            LocalPartitionState::Serving,
            "p2 on b"
        );
        let st = status_of(a).await;
        assert!(st.writer_lease_remaining_ms > 0, "a's writer lease: {st:?}");
        super::super::NO_ADOPT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|d| *d != t.dirs[0]);
        eventually_within(Duration::from_secs(20), "c to take a's map", async || {
            let (ours, theirs) = (map_of(a).await, map_of(c).await);
            ours.writer == "a"
                && ours.writer_term > voted
                && (theirs.writer_term, theirs.map_version) == (ours.writer_term, ours.map_version)
        })
        .await;
        let st = status_of(c).await;
        assert!(st.writer_term <= map_of(c).await.writer_term, "{st:?}");
        eventually_within(Duration::from_secs(10), "p3 to serve on c", async || {
            state(c, "p3").await == LocalPartitionState::Serving
        })
        .await;
        eventually("a write through c", async || {
            try_put(c, "zed", "after").await.is_ok()
        })
        .await;
        // The election is recorded: a WRITER event from a to a, above c's vote.
        let m = map_of(a).await;
        assert!(
            m.failovers.iter().any(|e| {
                e.kind() == crate::serve::proto::FailoverKind::Writer
                    && e.from == "a"
                    && e.to == "a"
                    && e.writer_term > voted
            }),
            "{:?}",
            m.failovers
        );
    });
}

/// `m` one version on with a range move of `users` `[x, y)` from p3 to p1 at COPYING.
fn with_p3_move(mut m: crate::serve::proto::ClusterMap) -> crate::serve::proto::ClusterMap {
    use crate::serve::proto::{MoveKind, MovePhase, RangeMove};
    m.map_version += 1;
    m.moves.push(RangeMove {
        id: "mv3".into(),
        kind: MoveKind::Range as i32,
        table: "users".into(),
        start: Some(text("x")),
        end: Some(text("y")),
        from: "p3".into(),
        to: "p1".into(),
        phase: MovePhase::Copying as i32,
        ..Default::default()
    });
    m
}

/// Whether node `port` holds `p` as a copy a promote stamped at `epoch`: its own file, no
/// longer a replica.
async fn stamped_at(port: u16, p: &str, epoch: u64) -> bool {
    status_of(port).await.partitions.iter().any(|lp| {
        lp.id == p && lp.role() != crate::serve::proto::PartitionRole::Replica && lp.epoch == epoch
    })
}

#[test]
fn a_pending_promote_whose_owner_came_back_is_abandoned_and_the_partition_moves_again() {
    // #2070 (simp724's ruling, the coordinator's test): a stamps b's copy for p3 and stops
    // before its map, as the writer dying there leaves it, and every finish stops there too
    // while this test runs. c, p3's owner, comes back and serves. a abandons the promote: p3
    // stays on c at epoch 1, status names b's orphan copy, and a range move of p3 (which N1
    // on #2075 refuses while a promote of it is pending: map.rs's unit test) is allowed.
    use crate::serve::proto::FailoverState as S;
    let t = failover_rig(true);
    let [a, _, c] = t.ports;
    let b = t.ports[1];
    super::super::failover::STOP_AFTER_STAMP
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .extend(std::iter::repeat_n(t.dirs[0].clone(), 1_000));
    cut(&t, 2, true);
    t.rt.block_on(async {
        let mut epoch = 0;
        eventually_within(
            FAILOVER_WINDOW,
            "b's copy stamped, its promote pending",
            async || {
                let m = map_of(a).await;
                let Some(e) = m
                    .failovers
                    .iter()
                    .find(|e| e.partition == "p3" && e.state() == S::Pending && e.to == "b")
                else {
                    return false;
                };
                epoch = e.to_epoch;
                stamped_at(b, "p3", epoch).await
            },
        )
        .await;
    });
    cut(&t, 2, false);
    t.rt.block_on(async {
        eventually_within(FAILOVER_WINDOW, "the promote abandoned", async || {
            map_of(a)
                .await
                .failovers
                .iter()
                .any(|e| e.partition == "p3" && e.state() == S::Abandoned && e.to == "b")
        })
        .await;
        let m = map_of(a).await;
        let p3 = m.partitions.iter().find(|p| p.id == "p3").unwrap();
        assert_eq!(
            (p3.node.as_str(), p3.epoch, p3.failover_count),
            ("c", 1, 0),
            "{p3:?}"
        );
        eventually_within(Duration::from_secs(10), "p3 to serve on c", async || {
            state(c, "p3").await == LocalPartitionState::Serving
        })
        .await;
        eventually("status to name b's orphan copy", async || {
            status_of(a)
                .await
                .failover_blocked
                .get("p3")
                .is_some_and(|why| why.starts_with("an unfinished promote stamped node b"))
        })
        .await;
        let mut last = None;
        for _ in 0..100 {
            match set_at(a, with_p3_move(map_of(a).await)).await {
                Ok(_) => {
                    last = None;
                    break;
                }
                Err(e) => last = Some(e),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(last.is_none(), "a move of p3 once abandoned: {last:?}");
    });
    super::super::failover::STOP_AFTER_STAMP
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|d| *d != t.dirs[0]);
}

#[test]
fn an_orphaned_stamped_copy_bootstraps_again_once_added_back() {
    // Amendment 1's S2 end to end (#2070), on three nodes. c, p3's owner, can't send (a
    // one-way cut): its renewals stop, but it still answers and grants, so a keeps its
    // majority. a stamps b's copy and stops before its map (a writer dying there), and b is
    // then cut off. a fails p3 over to its own copy at an epoch above b's pending one,
    // abandoning b's. b comes back and is added back to p3's replicas: its stamped copy isn't
    // taken for a replica until the operator drops it; then b's copy bootstraps again from
    // a, and a's write reads back on every node.
    use crate::serve::proto::FailoverState as S;
    let t = failover_rig_with(true, &["b", "a"]);
    let [a, b, c] = t.ports;
    // Held, not one shot (#2184's CI): a's next pass would resume b's stamped promote and
    // finish it, as D330 does, if it came before b's cut below. a's passes stop after the
    // stamp until a has given b's promote up (#2209's CI, below).
    let hold = |on: bool| {
        let mut held = super::super::failover::HOLD_AFTER_STAMP
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        held.retain(|d| *d != t.dirs[0]);
        if on {
            held.push(t.dirs[0].clone());
        }
    };
    hold(true);
    let pair = |from: usize, to: u16| (t.dirs[from].clone(), format!("127.0.0.1:{to}"));
    let one_way = [pair(2, a), pair(2, b)];
    super::super::CUT_BETWEEN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .extend(one_way.clone());
    let pending = t.rt.block_on(async {
        let mut epoch = 0;
        eventually_within(
            FAILOVER_WINDOW,
            "b's copy stamped, its promote pending",
            async || {
                let m = map_of(a).await;
                let Some(e) = m
                    .failovers
                    .iter()
                    .find(|e| e.partition == "p3" && e.state() == S::Pending && e.to == "b")
                else {
                    return false;
                };
                epoch = e.to_epoch;
                stamped_at(b, "p3", epoch).await
            },
        )
        .await;
        epoch
    });
    cut(&t, 1, true);
    // The hold ends only once a has given b's promote up for one of its own (#2209's CI): a
    // pass that surveyed b before the cut could still resume b's stamped promote, and
    // released at once it got past the stamp and finished it on a and c. Passes run one at
    // a time and a held one ends at the stamp, so the pass that writes a's pending promote
    // surveyed after the cut, and none that saw b's stamp is left when the hold ends.
    t.rt.block_on(async {
        eventually_why(
            FAILOVER_WINDOW,
            "b's promote given up for one to a",
            async || {
                let m = map_of(a).await;
                let of = |to: &str, state: S| {
                    m.failovers
                        .iter()
                        .any(|e| e.partition == "p3" && e.to == to && e.state() == state)
                };
                if of("b", S::Abandoned) && of("a", S::Pending) {
                    return Ok(());
                }
                Err(format!(
                    "map version {} has p3's events {:?}; a's failover_blocked {:?}",
                    m.map_version,
                    m.failovers
                        .iter()
                        .filter(|e| e.partition == "p3")
                        .map(|e| (e.to.clone(), e.to_epoch, e.state()))
                        .collect::<Vec<_>>(),
                    status_of(a).await.failover_blocked
                ))
            },
        )
        .await;
    });
    hold(false);
    t.rt.block_on(async {
        // Three times on CI (#2166, #2170, #2184) a never failed over within the window, and
        // once (#2209) a finished b's promote instead: a's pass had resumed it before the cut
        // (now held above until a gives it up). The reason a gives, and its p3 rows, say which
        // step stalled if it happens again.
        eventually_why(FAILOVER_WINDOW, "p3 to fail over to a", async || {
            let m = map_of(a).await;
            let p3 = m.partitions.iter().find(|p| p.id == "p3");
            if p3.is_some_and(|p| p.node == "a" && p.epoch > pending) {
                return Ok(());
            }
            let st = status_of(a).await;
            let rows: Vec<_> = st.partitions.iter().filter(|lp| lp.id == "p3").collect();
            Err(format!(
                "map version {} has p3 {p3:?}; a's failover_blocked {:?}; a's p3 rows {rows:?}",
                m.map_version, st.failover_blocked
            ))
        })
        .await;
        let m = map_of(a).await;
        assert!(
            m.failovers
                .iter()
                .any(|e| e.partition == "p3" && e.to == "b" && e.state() == S::Abandoned),
            "b's promote abandoned: {:?}",
            m.failovers
        );
        eventually_within(Duration::from_secs(10), "p3 to serve on a", async || {
            state(a, "p3").await == LocalPartitionState::Serving
        })
        .await;
        try_put(a, "zed", "v2").await.unwrap();
    });
    cut(&t, 1, false);
    super::super::CUT_BETWEEN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|e| !one_way.contains(e));
    t.rt.block_on(async {
        p3_replicas(a, &["b"]).await;
        // b's stamped copy isn't taken for a replica: it's an owner's file at another
        // epoch, which only the operator drops (simp724's N1 on #2069).
        tokio::time::sleep(TEST_LEASE).await;
        assert!(
            replica_row(b, "p3")
                .await
                .is_none_or(|lp| lp.replica_of != "a"),
            "b's orphan copy opened as a replica"
        );
        client(b)
            .await
            .drop_partition(crate::serve::proto::PartitionRequest {
                partition: "p3".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        // The replica opens on the next map that lists it.
        p3_replicas(a, &["b"]).await;
        let until = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            // A fresh replica copy of a's history, bootstrapping from a. (The rig's nodes run
            // no --change-feed, so a's promoted copy has no feed for it to follow: following
            // is D328's tests'.)
            let row = replica_row(b, "p3").await;
            if row.as_ref().is_some_and(|lp| {
                lp.replica_of == "a"
                    && matches!(
                        lp.replica_state(),
                        crate::serve::proto::ReplicaState::Bootstrapping
                            | crate::serve::proto::ReplicaState::OwnerFeedOff
                            | crate::serve::proto::ReplicaState::Following
                    )
            }) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "b's copy to bootstrap again from a: {row:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // Nothing diverged: a's write is the one every node reads.
        for port in [a, b, c] {
            eventually("zed to read v2", async || {
                get_row(port, "zed")
                    .await
                    .is_ok_and(|v| v.as_deref() == Some(b"v2".as_slice()))
            })
            .await;
        }
    });
}

#[test]
fn a_new_writer_finishes_or_aborts_a_whole_partition_move_its_old_writer_left() {
    // D332 PR 2 (the coordinator's test): `phdb cluster move p3 --to b` stops at COPYING,
    // then (a second run) at HANDING_OVER, and a, the writer, is stopped each time, so a new
    // writer is elected (D330). Its map still holds the NODE record. Through the new
    // writer, a rerun of the move from COPYING finishes it (a fresh record, the copy and
    // match again); from HANDING_OVER `--abort` ends it, and p3 serves on c again once
    // unfenced. p3 stays closed on c throughout, so no failover takes it.
    use crate::serve::cluster::moving::{STOP, Step};
    use crate::serve::proto::MovePhase;
    for (step, finish) in [(Step::Closed, true), (Step::Stamped, false)] {
        let mut t = failover_rig(true);
        let [_, b, c] = t.ports;
        let args = super::super::cli::NodeArgs {
            own: None,
            node: format!("http://127.0.0.1:{c}"),
            token_file: None,
            ca: None,
        };
        let record = async |port: u16| {
            super::super::moving::record_of(&map_of(port).await, "p3")
                .map(|r| (r.id.clone(), super::super::map::phase(r)))
        };
        let at = if finish {
            MovePhase::Copying
        } else {
            MovePhase::HandingOver
        };
        let first = t.rt.block_on(async {
            STOP.lock().unwrap().push((args.node.clone(), step));
            within(super::super::moving::run(&args, "p3", "b", false, None))
                .await
                .err()
                .unwrap_or_else(|| panic!("{step:?}: not stopped"));
            let r = record(c).await.expect("recorded");
            assert_eq!(r.1, at, "{step:?}");
            r.0
        });
        t.stop(0);
        t.rt.block_on(async {
            eventually_within(FAILOVER_WINDOW, "a new writer", async || {
                let m = map_of(c).await;
                m.writer != "a" && m.writer_term > 0
            })
            .await;
            assert_eq!(
                record(c).await.map(|r| r.0),
                Some(first.clone()),
                "{step:?}: the elected map keeps the record"
            );
            if finish {
                let r = within(super::super::moving::run(&args, "p3", "b", false, None))
                    .await
                    .unwrap_or_else(|e| panic!("{step:?}: the rerun: {}", e.message()));
                assert_ne!(
                    r.document["recorded"],
                    serde_json::json!(first),
                    "a fresh record"
                );
                let m = map_of(c).await;
                let p3 = m.partitions.iter().find(|p| p.id == "p3").unwrap();
                assert_eq!(p3.node, "b", "{step:?}");
                assert!(record(c).await.is_none(), "{step:?}: the record went");
                eventually_within(Duration::from_secs(10), "p3 to serve on b", async || {
                    state(b, "p3").await == LocalPartitionState::Serving
                })
                .await;
            } else {
                within(super::super::moving::abort(&args, "p3"))
                    .await
                    .unwrap_or_else(|e| panic!("{step:?}: the abort: {}", e.message()));
                assert!(record(c).await.is_none(), "{step:?}: the record went");
                client(c)
                    .await
                    .unfence(crate::serve::proto::PartitionRequest {
                        partition: "p3".into(),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                eventually_within(Duration::from_secs(10), "p3 to serve on c", async || {
                    state(c, "p3").await == LocalPartitionState::Serving
                })
                .await;
            }
        });
    }
}

/// The balancer's tests' token file: an admin token beside the nodes' cluster token, so
/// the map's writer runs its moves with the cluster token alone (audit120's M1 on #2103).
fn balancer_tokens() -> String {
    format!("admin {BALANCER_ADMIN}\ncluster {NODE_TOKEN}\n")
}

const BALANCER_ADMIN: &str = "adm1n-bal";

/// Changes the map at the writer `a` by `change` (retrying while its lease comes back),
/// with the balancer's pass at 100 ms on every node (D332 PR 3's tests), asking with
/// `token`. The new map's version.
///
/// Every node's partitions report a load of 0 ([`load::LOAD_FOR`]), unless the test
/// registered its own first (`None` keeps the measured load): these tests aren't about load,
/// and a measured one made a lone partition "too hot" on a slow runner, and never cold
/// enough to merge (#2164's Windows red).
///
/// [`load::LOAD_FOR`]: super::super::load::LOAD_FOR
async fn balancer_map(
    t: &Rig,
    token: Option<&str>,
    change: impl Fn(&mut crate::serve::proto::ClusterMap),
) -> u64 {
    for dir in &t.dirs {
        let mut pass = super::super::balancing::PASS_FOR
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !pass.iter().any(|(d, _)| d == dir) {
            pass.push((dir.clone(), Duration::from_millis(100)));
        }
        drop(pass);
        let mut load = super::super::load::LOAD_FOR
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !load.iter().any(|(d, _)| d == dir) {
            load.push((dir.clone(), Some(0.0)));
        }
    }
    let a = t.ports[0];
    let mut last = None;
    for _ in 0..100 {
        let mut m = map_of_as(a, token).await;
        m.map_version += 1;
        change(&mut m);
        match set_at_as(a, m, token).await {
            Ok(v) => return v,
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("set the balancer: {last:?}");
}

/// Sets the map's balancer to `mode`: [`balancer_map`].
async fn balancer_mode(t: &Rig, mode: crate::serve::proto::BalancerMode) -> u64 {
    balancer_map(t, None, |m| m.set_balancer(mode)).await
}

/// Turns the balancer on over a skewed cluster: node c weighs ten, so a and b each hold ten
/// times c's share of the bytes per unit of weight (1.43x the mean), over a `size_ratio`
/// of 1.2 after one pass. The move goes to c; the dwell is long, so it's the only one. No
/// partition is under a 1-byte `partition_min_bytes`, so none of these idle partitions is
/// merged meanwhile.
async fn balance_skewed(t: &Rig, token: Option<&str>) -> u64 {
    balancer_map(t, token, |m| {
        for n in m.nodes.iter_mut().filter(|n| n.id == "c") {
            n.weight = 10;
        }
        m.balance = Some(crate::serve::proto::BalanceBounds {
            size_ratio: 1.2,
            passes: 1,
            dwell_ms: 600_000,
            partition_min_bytes: 1,
            ..Default::default()
        });
        m.set_balancer(crate::serve::proto::BalancerMode::On);
    })
    .await
}

/// Waits until the writer at `port` reports a balancer state `ok` takes, up to `limit`;
/// the panic names the last one.
async fn balancer_until(port: u16, limit: Duration, ok: impl Fn(&str) -> bool) {
    let until = tokio::time::Instant::now() + limit;
    loop {
        let st = status_of(port).await.balancer;
        if ok(&st) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "the writer's balancer state: {st:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Waits up to `limit` until the map at `port` has `done`, asking with `token`; the panic
/// shows the writer's balancer state, the moves and the events.
async fn map_until(
    port: u16,
    token: Option<&str>,
    limit: Duration,
    what: &str,
    done: impl Fn(&crate::serve::proto::ClusterMap) -> bool,
) -> crate::serve::proto::ClusterMap {
    let until = tokio::time::Instant::now() + limit;
    loop {
        let m = map_of_as(port, token).await;
        if done(&m) {
            return m;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "{what}: the writer's balancer state {:?}; moves {:?}; events {:?}",
            status_of_as(port, token).await.balancer,
            m.moves,
            m.balancer_events
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The `bal-` record in `m`, with its partition and phase.
fn bal_record(
    m: &crate::serve::proto::ClusterMap,
) -> Option<(String, String, crate::serve::proto::MovePhase)> {
    m.moves
        .iter()
        .find(|r| r.id.starts_with("bal-"))
        .map(|r| (r.id.clone(), r.from.clone(), super::super::map::phase(r)))
}

#[test]
fn a_paused_or_idle_balancer_changes_no_map() {
    // simp724's M2 and perf287's should-fix 1 on the D332 PR 3 plan: a pass that does
    // nothing writes nothing to the map; its reason is in the writer's status. Paused, it
    // says so; on, an even cluster is within bounds, and ten passes and more leave the
    // map's version where it was.
    //
    // On the nodes' measured loads, not pinned ones (the coordinator on #2164's flake fix):
    // the one balancer test that keeps the real measure under the pass. An idle rig does
    // nothing whatever it measures: no partition has a tablet boundary to split at.
    let mut t = Rig::start();
    super::super::load::LOAD_FOR
        .lock()
        .unwrap()
        .extend(t.dirs.iter().map(|d| (d.clone(), None)));
    writer_leased(&mut t);
    let a = t.ports[0];
    t.rt.block_on(async {
        balancer_mode(&t, crate::serve::proto::BalancerMode::Paused).await;
        balancer_until(a, Duration::from_secs(15), |s| s == "paused").await;
        let v = balancer_mode(&t, crate::serve::proto::BalancerMode::On).await;
        balancer_until(a, Duration::from_secs(15), |s| s.starts_with("skip: ")).await;
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        let m = map_of(a).await;
        assert_eq!(
            m.map_version,
            v,
            "the balancer changed the map: {:?}",
            status_of(a).await.balancer
        );
    });
}

#[test]
fn a_skewed_cluster_balances_by_a_whole_move_with_tokens_configured() {
    use crate::serve::proto::BalancerEventKind;
    // simp724's S2 and audit120's M1 on #2103: every node has tokens, and the writer runs
    // its move with its cluster token alone, its map changes set in process. Node c weighs
    // ten, so a and b are over their size bounds: the balancer moves one of their
    // partitions (p1 from a or p2 from b, whichever measures bigger) to c. One MOVE event,
    // the partition's history, the record gone, it serving on c; nothing else moves within
    // the dwell.
    let mut t = Rig::start_with(&balancer_tokens());
    let tok = Some(BALANCER_ADMIN);
    writer_leased_as(&mut t, tok);
    let [a, _, c] = t.ports;
    t.rt.block_on(async {
        balance_skewed(&t, tok).await;
        let on_c = |m: &crate::serve::proto::ClusterMap| {
            m.partitions
                .iter()
                .find(|p| ["p1", "p2"].contains(&p.id.as_str()) && p.node == "c")
                .map(|p| p.id.clone())
        };
        let m = map_until(
            a,
            tok,
            Duration::from_secs(30),
            "p1 or p2 moved to c",
            |m| m.moves.is_empty() && on_c(m).is_some(),
        )
        .await;
        let moved = on_c(&m).unwrap();
        let from = if moved == "p1" { "a" } else { "b" };
        let moves: Vec<_> = m
            .balancer_events
            .iter()
            .filter(|e| e.kind() == BalancerEventKind::Move)
            .collect();
        assert_eq!(moves.len(), 1, "{:?}", m.balancer_events);
        assert_eq!(
            (
                moves[0].partition.as_str(),
                moves[0].from.as_str(),
                moves[0].to.as_str()
            ),
            (moved.as_str(), from, "c")
        );
        let p = m.partitions.iter().find(|p| p.id == moved).unwrap();
        assert_eq!(p.moves_history.len(), 1, "{p:?}");
        eventually_within(Duration::from_secs(10), "it to serve on c", async || {
            local_as(c, &moved, tok).await.state() == LocalPartitionState::Serving
        })
        .await;
        // Within the dwell: no second move.
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        let m = map_of_as(a, tok).await;
        assert!(m.moves.is_empty(), "{:?}", m.moves);
        assert_eq!(
            m.balancer_events
                .iter()
                .filter(|e| e.kind() == BalancerEventKind::Move)
                .count(),
            1
        );
    });
}

/// simp724's M1 on the D332 PR 3 plan: a `bal-` move of p1 to c stopped after its stamp
/// (HANDING_OVER), as an earlier writer would leave it. The writer's balancer doesn't take
/// it up: it aborts it. The record goes, c's copy goes aside, p1 is unfenced and serves on
/// a again, and an ABORT event is recorded. With `tokens`, under the cluster token alone
/// (audit120's M1 on #2103).
fn an_earlier_writers_move_is_aborted(tokens: &str) {
    an_earlier_writers_move_is_aborted_with(tokens, false);
}

/// [`an_earlier_writers_move_is_aborted`]; with `unfence_fails`, the abort's unfence fails
/// once (#2151): the record is already gone, so the balancer keeps the unfence and its next
/// pass sends it again. Before the fix nothing retried, and p1 stayed closed on a.
fn an_earlier_writers_move_is_aborted_with(tokens: &str, unfence_fails: bool) {
    use crate::serve::cluster::moving::{STOP, Start, Step};
    use crate::serve::proto::{BalancerEventKind, MovePhase};
    let mut t = Rig::start_with(tokens);
    let tok = (!tokens.is_empty()).then_some(BALANCER_ADMIN);
    writer_leased_as(&mut t, tok);
    let [a, _, c] = t.ports;
    let args = super::super::cli::NodeArgs {
        node: format!("http://127.0.0.1:{c}"),
        token_file: None,
        ca: None,
        own: tok.map(|tok| {
            Box::new(super::super::cli::Own {
                token: Some(tok.to_owned()),
                ..Default::default()
            })
        }),
    };
    t.rt.block_on(async {
        STOP.lock()
            .unwrap()
            .push((args.node.clone(), Step::Stamped));
        let start = Start {
            prefix: "bal",
            ..Start::default()
        };
        within(super::super::moving::run_as(
            &args, "p1", "c", false, None, &start,
        ))
        .await
        .err()
        .expect("stopped after the stamp");
        let (id, from, phase) = bal_record(&map_of_as(a, tok).await).expect("recorded");
        assert_eq!((from.as_str(), phase), ("p1", MovePhase::HandingOver));
        if unfence_fails {
            // Whichever node is the writer: only its balancer takes its entry.
            let mut fail = super::super::balancing::FAIL_UNFENCE.lock().unwrap();
            fail.extend(t.dirs.iter().cloned());
        }
        balancer_map(&t, tok, |m| {
            m.set_balancer(crate::serve::proto::BalancerMode::On);
        })
        .await;
        // The map's record of it, not the writer's state: "aborted" lasts one pass, and the
        // next pass's reason replaces it.
        map_until(a, tok, Duration::from_secs(20), "the move aborted", |m| {
            bal_record(m).is_none()
                && m.balancer_events
                    .iter()
                    .any(|e| e.kind() == BalancerEventKind::Abort && e.move_id == id)
        })
        .await;
        assert!(!t.dirs[2].join("p1.phdb").exists(), "c's copy went aside");
        eventually_within(
            Duration::from_secs(if unfence_fails { 30 } else { 10 }),
            "p1 to serve on a again",
            async || local_as(a, "p1", tok).await.state() == LocalPartitionState::Serving,
        )
        .await;
        if unfence_fails {
            let fail = super::super::balancing::FAIL_UNFENCE.lock().unwrap();
            let armed = t.dirs.iter().filter(|d| fail.contains(d)).count();
            assert_eq!(armed, 2, "the writer's balancer took its fault");
        }
    });
    if unfence_fails {
        let mut fail = super::super::balancing::FAIL_UNFENCE.lock().unwrap();
        fail.retain(|d| !t.dirs.contains(d));
    }
}

#[test]
fn a_balancer_abort_whose_unfence_failed_unfences_on_a_later_pass() {
    an_earlier_writers_move_is_aborted_with("", true);
}

#[test]
fn a_balancer_move_an_earlier_writer_left_before_its_commit_point_is_aborted() {
    an_earlier_writers_move_is_aborted("");
}

#[test]
fn a_balancer_move_an_earlier_writer_left_is_aborted_with_tokens_configured() {
    an_earlier_writers_move_is_aborted(&balancer_tokens());
}

#[test]
fn a_balancer_move_an_earlier_writer_left_past_its_commit_point_is_finished() {
    use crate::serve::proto::{BalancerEventKind, MovePhase};
    // The plan's test: a `bal-` move of p1 to c stopped once the map gave p1 to c
    // (HANDED_OVER), as an earlier writer would leave it. The writer's balancer finishes it:
    // the record goes, p1 serves on c, and nothing is aborted.
    use crate::serve::cluster::moving::{STOP, Start, Step};
    let mut t = Rig::start();
    writer_leased(&mut t);
    let [a, _, c] = t.ports;
    let args = super::super::cli::NodeArgs {
        node: format!("http://127.0.0.1:{c}"),
        token_file: None,
        ca: None,
        own: None,
    };
    t.rt.block_on(async {
        STOP.lock().unwrap().push((args.node.clone(), Step::Mapped));
        let start = Start {
            prefix: "bal",
            ..Start::default()
        };
        within(super::super::moving::run_as(
            &args, "p1", "c", false, None, &start,
        ))
        .await
        .err()
        .expect("stopped after the relocation map");
        let (_, _, phase) = bal_record(&map_of(a).await).expect("recorded");
        assert_eq!(phase, MovePhase::HandedOver);
        balancer_mode(&t, crate::serve::proto::BalancerMode::On).await;
        let m = map_until(a, None, Duration::from_secs(20), "the move finished", |m| {
            bal_record(m).is_none()
        })
        .await;
        assert!(
            m.partitions.iter().any(|p| p.id == "p1" && p.node == "c"),
            "{:?}",
            m.partitions
        );
        assert!(
            !m.balancer_events
                .iter()
                .any(|e| e.kind() == BalancerEventKind::Abort),
            "{:?}",
            m.balancer_events
        );
        eventually_within(Duration::from_secs(10), "p1 to serve on c", async || {
            state(c, "p1").await == LocalPartitionState::Serving
        })
        .await;
    });
}

#[test]
fn a_writer_cut_off_at_term_0_doesnt_advance_its_balancer_move() {
    use crate::serve::proto::{BalancerEventKind, MovePhase};
    // simp724's M1 on #2103 (the plan's Amendment 2): the writer's lease bounds its
    // balancer moves at every term, term 0 included. The balancer's move to c (of p1 from a
    // or p2 from b: both are over their bound, and which goes first depends on their
    // measured sizes) is held just before its stamp; the writer a is cut off and its writer
    // lease runs out; released, the move stamps c's copy but can't set its next map, so the
    // record stays at COPYING and the partition where it was. Joined again, the writer
    // aborts it (it's before its commit point).
    use crate::serve::cluster::moving::HOLD_BEFORE_STAMP;
    let mut t = Rig::start();
    writer_leased(&mut t);
    let a = t.ports[0];
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    HOLD_BEFORE_STAMP.lock().unwrap().push((
        format!("http://127.0.0.1:{a}"),
        reached_tx,
        release_rx,
    ));
    t.rt.block_on(async {
        assert_eq!(map_of(a).await.writer_term, 0);
        balance_skewed(&t, None).await;
        within(reached_rx)
            .await
            .expect("the move reached its stamp");
        let (_, _, phase) = bal_record(&map_of(a).await).expect("recorded");
        assert_eq!(phase, MovePhase::Copying);
    });
    cut(&t, 0, true);
    t.rt.block_on(async {
        eventually_within(
            Duration::from_secs(10),
            "a's writer lease to run out",
            async || status_of(a).await.writer_lease_remaining_ms == 0,
        )
        .await;
    });
    let (id, from, home) = t.rt.block_on(async {
        let m = map_of(a).await;
        let (id, from, _) = bal_record(&m).expect("recorded");
        // Where the moved partition was when the move was recorded.
        let home = m
            .partitions
            .iter()
            .find(|p| p.id == from)
            .map(|p| p.node.clone())
            .expect("the moved partition is listed");
        release_tx.send(()).expect("the move waits");
        // Passes enough for the move to try its next map, and the writer to try an abort.
        tokio::time::sleep(Duration::from_millis(2_000)).await;
        let m = map_of(a).await;
        let (still, _, phase) = bal_record(&m).expect("still recorded");
        assert_eq!((still.as_str(), phase), (id.as_str(), MovePhase::Copying));
        assert!(
            m.partitions.iter().any(|p| p.id == from && p.node == home),
            "{:?}",
            m.partitions
        );
        (id, from, home)
    });
    cut(&t, 0, false);
    t.rt.block_on(async {
        map_until(a, None, Duration::from_secs(30), "the move aborted", |m| {
            bal_record(m).is_none()
                && m.balancer_events
                    .iter()
                    .any(|e| e.kind() == BalancerEventKind::Abort && e.move_id == id)
        })
        .await;
        let port = t.ports[["a", "b", "c"]
            .iter()
            .position(|n| *n == home)
            .expect("a node of the rig")];
        eventually_within(
            Duration::from_secs(10),
            "the partition to serve where it was",
            async || state(port, &from).await == LocalPartitionState::Serving,
        )
        .await;
    });
}

#[test]
fn a_partition_with_a_share_in_doubt_isnt_moved_by_the_balancer_with_failover_off() {
    // audit120's M1 on 1.3.0: the balancer never moves a partition with spanning shares in
    // doubt. It reads the count from the owner's lease renewals, which carried it only with
    // failover on. With failover off, p1's share is prepared and in doubt: a commit homed on
    // c is held at its commit point, so the resolver leaves it, since its home has it in
    // progress. b and c weigh ten, so only a is over its bound, and p1 is its one candidate.
    // The balancer says so and moves nothing.
    use crate::serve::cluster::spanning::seams::{Step, pause};
    use crate::serve::proto::BalancerEventKind;
    let mut t = Rig::start();
    writer_leased(&mut t);
    let [a, _, c] = t.ports;
    t.rt.block_on(async {
        assert!(!map_of(a).await.failover, "failover is off");
        // c's p3 (the transaction's home) under its lease, as writer_leased waits only for a.
        eventually_within(Duration::from_secs(10), "p3 to serve on c", async || {
            state(c, "p3").await == LocalPartitionState::Serving
        })
        .await;
        let id = super::spanning::begin_spanning(c, "zed").await.unwrap();
        super::txn_write(
            c,
            &id,
            vec![super::put_row("alice", "1"), super::put_row("zed", "1")],
            None,
        )
        .await
        .unwrap();
        let p = pause(&id, Step::CommitPoint);
        let commit = {
            let id = id.clone();
            tokio::spawn(async move { super::spanning::commit_parts(c, &id, Vec::new()).await })
        };
        p.reached.notified().await;
        // A renewal or two, so the writer holds p1's count before the balancer turns on.
        tokio::time::sleep(super::TEST_LEASE * 2).await;
        balancer_map(&t, None, |m| {
            for n in m.nodes.iter_mut().filter(|n| n.id != "a") {
                n.weight = 10;
            }
            m.balance = Some(crate::serve::proto::BalanceBounds {
                size_ratio: 1.2,
                passes: 1,
                dwell_ms: 600_000,
                // No partition counts as small, so no merge decision takes the reason.
                partition_min_bytes: 1,
                ..Default::default()
            });
            m.set_balancer(crate::serve::proto::BalancerMode::On);
        })
        .await;
        balancer_until(a, Duration::from_secs(20), |s| {
            s.starts_with("skip: node a is over its bound, but no partition on it can move")
        })
        .await;
        // Passes more, and still nothing moved.
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        let m = map_of(a).await;
        assert!(
            !m.balancer_events
                .iter()
                .any(|e| e.kind() == BalancerEventKind::Move),
            "{:?}",
            m.balancer_events
        );
        assert!(
            m.partitions.iter().any(|q| q.id == "p1" && q.node == "a"),
            "{:?}",
            m.partitions
        );
        // The commit goes on.
        p.go.send(false).unwrap();
        let _ = commit.await;
    });
}

#[test]
fn a_grant_from_before_the_balancer_was_on_still_says_a_share_is_in_doubt() {
    // simp724 on #2127: the balancer reads a partition's in-doubt count from the writer's
    // last grant of it, ended or not. A grant renewed before the balancer was switched on
    // must say so too, or the first pass after the switch, with an operator's passes 1,
    // could move the partition. p2's share is held in doubt (a commit homed on c, held at
    // its commit point). A few renewals later b's renewals to a are cut, so a's last grant
    // of p2 is one from before the switch. Then the balancer comes on with only b over its
    // bound: it reports that b can't move anything, and p2 stays on b.
    use crate::serve::cluster::spanning::seams::{Step, pause};
    use crate::serve::proto::BalancerEventKind;
    let mut t = Rig::start();
    writer_leased(&mut t);
    let [a, b, c] = t.ports;
    t.rt.block_on(async {
        assert!(!map_of(a).await.failover, "failover is off");
        for (port, p) in [(b, "p2"), (c, "p3")] {
            eventually_within(Duration::from_secs(10), "p2 and p3 to serve", async || {
                state(port, p).await == LocalPartitionState::Serving
            })
            .await;
        }
        let id = super::spanning::begin_spanning(c, "zed").await.unwrap();
        super::txn_write(
            c,
            &id,
            vec![super::put_row("hank", "1"), super::put_row("zed", "1")],
            None,
        )
        .await
        .unwrap();
        let p = pause(&id, Step::CommitPoint);
        let commit = {
            let id = id.clone();
            tokio::spawn(async move { super::spanning::commit_parts(c, &id, Vec::new()).await })
        };
        p.reached.notified().await;
        // A renewal or two with the share in doubt, while the balancer is still off; then
        // b's renewals to a stop, so a's last grant of p2 is one of those.
        tokio::time::sleep(super::TEST_LEASE * 2).await;
        let cut = (t.dirs[1].clone(), format!("127.0.0.1:{a}"));
        super::super::CUT_BETWEEN.lock().unwrap().push(cut.clone());
        tokio::time::sleep(Duration::from_millis(600)).await;
        balancer_map(&t, None, |m| {
            for n in m.nodes.iter_mut().filter(|n| n.id != "b") {
                n.weight = 10;
            }
            m.balance = Some(crate::serve::proto::BalanceBounds {
                size_ratio: 1.2,
                passes: 1,
                dwell_ms: 600_000,
                partition_min_bytes: 1,
                ..Default::default()
            });
            m.set_balancer(crate::serve::proto::BalancerMode::On);
        })
        .await;
        balancer_until(a, Duration::from_secs(20), |s| {
            s.starts_with("skip: node b is over its bound, but no partition on it can move")
        })
        .await;
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        let m = map_of(a).await;
        assert!(
            !m.balancer_events
                .iter()
                .any(|e| e.kind() == BalancerEventKind::Move),
            "{:?}",
            m.balancer_events
        );
        assert!(
            m.partitions.iter().any(|q| q.id == "p2" && q.node == "b"),
            "{:?}",
            m.partitions
        );
        super::super::CUT_BETWEEN
            .lock()
            .unwrap()
            .retain(|e| *e != cut);
        p.go.send(false).unwrap();
        let _ = commit.await;
    });
}

/// A put of `row` through `port`, as `token`: `Ok` once acknowledged.
async fn put_as(
    port: u16,
    row: &str,
    value: &str,
    token: Option<&str>,
) -> Result<(), tonic::Status> {
    data(port)
        .await
        .mutate(with(
            crate::serve::proto::MutateRequest {
                writes: Some(put_row(row, value)),
                durability: 0,
            },
            token,
        ))
        .await
        .map(drop)
}

/// `row`'s cell through `port`, as `token`.
async fn get_as(port: u16, row: &str, token: Option<&str>) -> Option<Vec<u8>> {
    data(port)
        .await
        .get(with(
            crate::serve::proto::GetRequest {
                table: "users".into(),
                row: row.as_bytes().to_vec().into(),
                family: "f".into(),
                qualifier: b"q".to_vec().into(),
                ..Default::default()
            },
            token,
        ))
        .await
        .unwrap_or_else(|e| panic!("get {row} through {port}: {e}"))
        .into_inner()
        .cell
        .map(|c| c.value.to_vec())
}

/// The map's family for `users` (a range move copies its families' rows), as `token`.
async fn with_family(t: &Rig, token: Option<&str>) -> u64 {
    balancer_map(t, token, |m| {
        m.tables[0].families = vec![crate::serve::proto::FamilySpec {
            name: "f".into(),
            ..Default::default()
        }];
    })
    .await
}

#[test]
fn two_small_neighbours_merge_and_the_emptied_source_is_dropped_with_tokens_configured() {
    use crate::serve::proto::BalancerEventKind;
    // D332 PR 5's design §7.2: two small, cold neighbours merge (an idle partition's load,
    // under a hundredth of `partition_max_load`, is cold), and the emptied source is dropped once nothing waits for it. p3 is pinned, so
    // p1 merges into its neighbour p2: its range moves as `bal-` records, with one MERGE
    // event and p1's MERGE history entry. p1 then stays listed, holding nothing, while its
    // moved-away row waits for the change feed (24 h, pruned here as #2131's tests do); the
    // balancer's next passes retry its removal, and it leaves the map, the table one range
    // on p2.
    let mut t = Rig::start_with(&balancer_tokens());
    let tok = Some(BALANCER_ADMIN);
    writer_leased_as(&mut t, tok);
    let a = t.ports[0];
    t.rt.block_on(async {
        balancer_map(&t, tok, |m| {
            for p in m.partitions.iter_mut().filter(|p| p.id == "p3") {
                p.pinned = true;
            }
            m.balance = Some(crate::serve::proto::BalanceBounds {
                passes: 1,
                dwell_ms: 600_000,
                ..Default::default()
            });
            m.set_balancer(crate::serve::proto::BalancerMode::On);
        })
        .await;
        let m = map_until(
            a,
            tok,
            Duration::from_secs(120),
            "p1 emptied into p2",
            |m| {
                m.moves.is_empty()
                    && !super::super::map::placed(m, "p1")
                    && m.partitions.iter().any(|p| p.id == "p1")
            },
        )
        .await;
        let merges: Vec<_> = m
            .balancer_events
            .iter()
            .filter(|e| e.kind() == BalancerEventKind::Merge)
            .collect();
        assert_eq!(merges.len(), 1, "{:?}", m.balancer_events);
        assert_eq!(
            (merges[0].partition.as_str(), merges[0].to.as_str()),
            ("p1", "p2")
        );
        let p1 = m.partitions.iter().find(|p| p.id == "p1").unwrap();
        assert_eq!(
            p1.moves_history
                .last()
                .map(crate::serve::proto::MoveHistory::kind),
            Some(BalancerEventKind::Merge),
            "{p1:?}"
        );
        assert!(
            super::super::map::inside(&m.tables[0], b"", Some(b"m"), "p2"),
            "{:?}",
            m.tables[0]
        );
        // p1's moved-away rows pruned, as the 24 h grace would; the next pass drops it. Its
        // removal closes p1's database, so a read here may find it closed: p1 is going.
        let db_a = t.dbs[0].clone().unwrap();
        let m = map_until(a, tok, Duration::from_secs(60), "p1 dropped", |m| {
            for row in crate::serve::ranges::read_moved(&db_a).unwrap_or_default() {
                let _ = crate::serve::ranges::prune_row(
                    &db_a,
                    crate::serve::ranges::Pruned::Moved,
                    &row,
                );
            }
            !m.partitions.iter().any(|p| p.id == "p1")
        })
        .await;
        assert!(
            super::super::map::inside(&m.tables[0], b"", Some(b"m"), "p2"),
            "{:?}",
            m.tables[0]
        );
    });
}

#[test]
fn an_emptied_sources_stopped_removal_doesnt_halt_the_balancer() {
    // audit120's S2 on #2137: p1, merged into p2 and waiting on its change feed, is fenced
    // by an operator, so its removal stops (an operator's fence is the operator's to lift).
    // The pass goes on to decide all the same: `state` is a skip that names the stop.
    let mut t = Rig::start();
    writer_leased(&mut t);
    let a = t.ports[0];
    t.rt.block_on(async {
        balancer_map(&t, None, |m| {
            for p in m.partitions.iter_mut().filter(|p| p.id == "p3") {
                p.pinned = true;
            }
            m.balance = Some(crate::serve::proto::BalanceBounds {
                passes: 1,
                dwell_ms: 600_000,
                ..Default::default()
            });
            m.set_balancer(crate::serve::proto::BalancerMode::On);
        })
        .await;
        map_until(
            a,
            None,
            Duration::from_secs(120),
            "p1 emptied into p2",
            |m| {
                m.moves.is_empty()
                    && !super::super::map::placed(m, "p1")
                    && m.partitions.iter().any(|p| p.id == "p1")
            },
        )
        .await;
        fence(a, "p1").await.unwrap();
        balancer_until(a, Duration::from_secs(30), |st| {
            st.starts_with("skip:") && st.contains("the removal of p1, merged away, stopped")
        })
        .await;
        // Paused, `state` still names the stop (audit120's nit on #2137).
        balancer_mode(&t, crate::serve::proto::BalancerMode::Paused).await;
        balancer_until(a, Duration::from_secs(30), |st| {
            st.starts_with("paused; ") && st.contains("the removal of p1, merged away, stopped")
        })
        .await;
        // Still listed: nothing removed it past the operator's fence.
        assert!(
            map_of_as(a, None)
                .await
                .partitions
                .iter()
                .any(|p| p.id == "p1")
        );
    });
}

#[test]
fn a_big_partition_splits_at_its_halving_tablet_onto_a_cold_node_with_tokens_configured() {
    use crate::serve::proto::BalancerEventKind;
    // D332 PR 5's design §7.2: every node has tokens, and the writer runs the split with its
    // cluster token alone. p2 and p3 are pinned and the size bound tiny, so p1 alone is a
    // split candidate. p1's database splits its tablets at 16 KiB (the engine's test hook,
    // the coordinator's ruling on #2137), so its users rows fill real tablets, and the
    // balancer cuts [cut, g) to a new partition p1-<v> on b or c, under writes. One SPLIT
    // event naming users; p1's history has it and the new partition's is empty; every
    // acknowledged write reads back; nothing else splits within the dwell.
    let small = pigeonhole::Options::default()
        .tablet_changes(true)
        .tablet_balance(Duration::from_millis(20), 1 << 20, 16 << 10);
    let mut t = Rig::start_p1(&balancer_tokens(), Some(small));
    let tok = Some(BALANCER_ADMIN);
    writer_leased_as(&mut t, tok);
    let a = t.ports[0];
    let acked: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>> = Default::default();
    // Writes answered `Unknown`: the hand-over raises p1's epoch, so its lease on a ends
    // for a round, and a commit in flight then may or may not be kept.
    let unknown: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>> = Default::default();
    t.rt.block_on(async {
        with_family(&t, tok).await;
        // About 400 KiB of users rows in p1 that don't compress (hex from a fixed xorshift),
        // flushed: SST bytes past the tablets' 16 KiB. Values of 1 KiB stay in the SSTs (a
        // large one is kept apart, and a tablet's bytes count its SSTs only).
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let mut value = || {
            (0..64)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    format!("{x:016x}")
                })
                .collect::<String>()
        };
        // Two flushes, so the engine's size split (at the middle of its SSTs' bounds) cuts
        // between the b rows and the e rows, where the writes below split evenly too.
        let db = t.dbs[0].clone().unwrap();
        for prefix in ["b", "e"] {
            for i in 0..200 {
                put_as(a, &format!("{prefix}{i:03}"), &value(), tok)
                    .await
                    .unwrap();
            }
            db.flush().unwrap();
        }
        let until = tokio::time::Instant::now() + Duration::from_secs(30);
        while db
            .tablets()
            .iter()
            .filter(|t| t.table == "users" && !t.start.is_empty())
            .count()
            == 0
        {
            assert!(
                tokio::time::Instant::now() < until,
                "p1's users rows in two or more tablets: tablets {:?}; refusals {:?}",
                db.tablets()
                    .iter()
                    .map(|t| (t.table.clone(), t.start.clone(), t.bytes))
                    .collect::<Vec<_>>(),
                db.tablet_refusals()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        for i in 0..20 {
            put_as(a, &format!("b{i:02}"), "0", tok).await.unwrap();
            put_as(a, &format!("e{i:02}"), "0", tok).await.unwrap();
        }
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer = {
            let (acked, stop) = (std::sync::Arc::clone(&acked), std::sync::Arc::clone(&stop));
            let unknown = std::sync::Arc::clone(&unknown);
            tokio::spawn(async move {
                let mut i = 0u32;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    for row in [format!("b{:02}", i % 20), format!("e{:02}", i % 20)] {
                        match put_as(a, &row, &i.to_string(), tok).await {
                            Ok(()) => acked.lock().unwrap().push((row, i.to_string())),
                            Err(e) if e.code() == tonic::Code::Unknown => {
                                unknown.lock().unwrap().push((row, i.to_string()));
                            }
                            // Refused: nothing was written.
                            Err(_) => {}
                        }
                    }
                    i += 1;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
        };
        balancer_map(&t, tok, |m| {
            for p in m.partitions.iter_mut().filter(|p| p.id != "p1") {
                p.pinned = true;
            }
            m.balance = Some(crate::serve::proto::BalanceBounds {
                partition_max_bytes: 1 << 10,
                partition_min_bytes: 1,
                passes: 1,
                dwell_ms: 600_000,
                ..Default::default()
            });
            m.set_balancer(crate::serve::proto::BalancerMode::On);
        })
        .await;
        let new_of = |m: &crate::serve::proto::ClusterMap| {
            m.partitions
                .iter()
                .find(|p| {
                    p.id.strip_prefix("p1-")
                        .is_some_and(|v| v.bytes().all(|b| b.is_ascii_digit()))
                })
                .cloned()
        };
        let m = map_until(a, tok, Duration::from_secs(60), "p1 split", |m| {
            m.moves.is_empty() && new_of(m).is_some()
        })
        .await;
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        writer.await.unwrap();
        let new = new_of(&m).unwrap();
        assert_ne!(new.node, "a", "{new:?}");
        assert!(new.moves_history.is_empty(), "{new:?}");
        let splits: Vec<_> = m
            .balancer_events
            .iter()
            .filter(|e| e.kind() == BalancerEventKind::Split)
            .collect();
        assert_eq!(splits.len(), 1, "{:?}", m.balancer_events);
        assert_eq!(
            (
                splits[0].partition.as_str(),
                splits[0].table.as_str(),
                splits[0].to.as_str()
            ),
            ("p1", "users", new.node.as_str())
        );
        let p1 = m.partitions.iter().find(|p| p.id == "p1").unwrap();
        assert_eq!(
            p1.moves_history
                .iter()
                .map(crate::serve::proto::MoveHistory::kind)
                .collect::<Vec<_>>(),
            vec![BalancerEventKind::Split],
            "{p1:?}"
        );
        // The cut is a real tablet boundary inside p1's [.., g).
        let cut = super::super::map::table_ranges(&m.tables[0])
            .into_iter()
            .find(|(_, _, p)| *p == new.id)
            .map(|(lo, _, _)| lo)
            .unwrap();
        assert!(
            !cut.is_empty() && cut.as_slice() < b"g".as_slice(),
            "{cut:?}"
        );
        assert!(super::super::map::inside(
            &m.tables[0],
            &cut,
            Some(b"g"),
            &new.id
        ));
        assert!(super::super::map::inside(
            &m.tables[0],
            b"",
            Some(&cut),
            "p1"
        ));
        let mut last = std::collections::HashMap::new();
        for (row, value) in acked.lock().unwrap().iter() {
            last.insert(row.clone(), value.clone());
        }
        assert!(last.len() >= 2, "{last:?}");
        for (row, value) in &last {
            // The last acknowledged write, or a later one whose outcome was unknown.
            let read = get_as(a, row, tok).await;
            let later: Vec<String> = unknown
                .lock()
                .unwrap()
                .iter()
                .filter(|(r, v)| {
                    r == row && v.parse::<u32>().unwrap() > value.parse::<u32>().unwrap()
                })
                .map(|(_, v)| v.clone())
                .collect();
            assert!(
                read.as_deref() == Some(value.as_bytes())
                    || later.iter().any(|v| read.as_deref() == Some(v.as_bytes())),
                "row {row}: read {read:?}, last acknowledged {value}, unknown since {later:?}"
            );
        }
        // Within the dwell: no second split.
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        let m = map_of_as(a, tok).await;
        assert!(m.moves.is_empty(), "{:?}", m.moves);
        assert_eq!(
            m.balancer_events
                .iter()
                .filter(|e| e.kind() == BalancerEventKind::Split)
                .count(),
            1
        );
    });
}

#[test]
fn an_earlier_writers_split_before_its_hand_over_is_aborted_and_leaves_no_partition() {
    use crate::serve::proto::BalancerEventKind;
    // D332 PR 5's design §7.3 (simp724's M1): a `bal-` split of p1 at d onto c, recorded at
    // COPYING with its new partition and no driver left, as an earlier writer would leave
    // it. The writer's balancer doesn't take it up: it aborts it. The record and the new
    // partition go together, an ABORT event names the move, and p1 serves the range as
    // before.
    let mut t = Rig::start();
    writer_leased(&mut t);
    let a = t.ports[0];
    let args = super::super::cli::NodeArgs {
        own: None,
        node: format!("http://127.0.0.1:{a}"),
        token_file: None,
        ca: None,
    };
    t.rt.block_on(async {
        with_family(&t, None).await;
        for i in 0..20 {
            put_as(a, &format!("e{i:02}"), "0", None).await.unwrap();
        }
        let spec = super::super::range_move::SplitSpec {
            table: "users".into(),
            from: "p1".into(),
            key: b"d".to_vec(),
            node: "c".into(),
            id: None,
        };
        let (id, new) = super::super::range_move::begin_split(
            &args,
            &spec,
            &super::super::moving::Start {
                prefix: "bal",
                ..Default::default()
            },
        )
        .await
        .unwrap();
        balancer_mode(&t, crate::serve::proto::BalancerMode::On).await;
        let m = map_until(a, None, Duration::from_secs(60), "the split aborted", |m| {
            m.moves.is_empty()
                && m.balancer_events
                    .iter()
                    .any(|e| e.kind() == BalancerEventKind::Abort && e.move_id == id)
        })
        .await;
        assert!(
            !m.partitions.iter().any(|p| p.id == new),
            "{:?}",
            m.partitions
        );
        assert!(super::super::map::inside(
            &m.tables[0],
            b"a",
            Some(b"g"),
            "p1"
        ));
        put_as(a, "e05", "after", None).await.unwrap();
        assert_eq!(get_as(a, "e05", None).await, Some(b"after".to_vec()));
    });
}

#[test]
fn an_earlier_writers_split_past_its_hand_over_is_finished() {
    use crate::serve::proto::{BalancerEventKind, MovePhase};
    // D332 PR 5's design §7.3: a `bal-` split of p1 at d onto c stopped right after its
    // HANDED_OVER change landed (range_move::STOP), as an earlier writer would leave it. The
    // writer's balancer finishes it (range_move::resume): the record goes, the new
    // partition holds [d, g), the rows read back, and nothing is aborted.
    let mut t = Rig::start();
    writer_leased(&mut t);
    let a = t.ports[0];
    let args = super::super::cli::NodeArgs {
        own: None,
        node: format!("http://127.0.0.1:{a}"),
        token_file: None,
        ca: None,
    };
    t.rt.block_on(async {
        with_family(&t, None).await;
        for i in 0..20 {
            put_as(a, &format!("e{i:02}"), &i.to_string(), None)
                .await
                .unwrap();
        }
        let spec = super::super::range_move::SplitSpec {
            table: "users".into(),
            from: "p1".into(),
            key: b"d".to_vec(),
            node: "c".into(),
            id: None,
        };
        super::super::range_move::STOP
            .lock()
            .unwrap()
            .push((args.node.clone(), MovePhase::HandedOver));
        let stopped = super::super::range_move::split_as(
            &args,
            &spec,
            Some(Duration::from_secs(120)),
            &super::super::moving::Start {
                prefix: "bal",
                ..Default::default()
            },
        )
        .await;
        assert!(stopped.is_err(), "the split ran past HANDED_OVER");
        let m = map_of(a).await;
        let r = m
            .moves
            .iter()
            .find(|r| r.id.starts_with("bal-"))
            .cloned()
            .unwrap_or_else(|| panic!("{:?}", m.moves));
        assert_eq!(super::super::map::phase(&r), MovePhase::HandedOver);
        balancer_mode(&t, crate::serve::proto::BalancerMode::On).await;
        let m = map_until(
            a,
            None,
            Duration::from_secs(60),
            "the split finished",
            |m| m.moves.is_empty(),
        )
        .await;
        assert!(
            m.partitions.iter().any(|p| p.id == r.to && p.node == "c"),
            "{:?}",
            m.partitions
        );
        assert!(super::super::map::inside(
            &m.tables[0],
            b"d",
            Some(b"g"),
            &r.to
        ));
        assert!(
            !m.balancer_events
                .iter()
                .any(|e| e.kind() == BalancerEventKind::Abort),
            "{:?}",
            m.balancer_events
        );
        for i in 0..20 {
            assert_eq!(
                get_as(a, &format!("e{i:02}"), None).await,
                Some(i.to_string().into_bytes()),
                "e{i:02}"
            );
        }
    });
}
