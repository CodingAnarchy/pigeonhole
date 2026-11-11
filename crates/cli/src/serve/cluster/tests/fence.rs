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
        })
        .await
        .map(tonic::Response::into_inner)
}

async fn unfence(port: u16, p: &str) -> Result<LocalPartition, tonic::Status> {
    client(port)
        .await
        .unfence(PartitionRequest {
            partition: p.into(),
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
