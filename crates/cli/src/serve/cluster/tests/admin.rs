//! `Admin.Info`, `Flush` and `Compact` on a cluster node (#1852, ICR 0117 amendment): each
//! node answers for the partitions it holds, never forwarding. Info lists them with their
//! state as `Cluster.Status` has it and names the map's others; Flush and Compact act on
//! every open one, or on the one named.

use super::*;
use crate::serve::proto::{
    FenceReason, InfoResponse, LocalPartitionState, MaintenanceRequest, PartitionRequest,
    admin_client::AdminClient,
};

async fn admin(port: u16) -> AdminClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    AdminClient::new(channel)
}

pub(super) async fn info(port: u16) -> InfoResponse {
    admin(port)
        .await
        .info(crate::serve::proto::Empty {})
        .await
        .unwrap()
        .into_inner()
}

fn named(partition: &str) -> MaintenanceRequest {
    MaintenanceRequest {
        partition: partition.into(),
    }
}

#[test]
fn info_flush_and_compact_cover_the_nodes_own_partitions() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let mut map = the_map(1, &ports);
    map.partitions.retain(|p| p.id != "p3");
    let dirs = [test_dir(), test_dir(), test_dir()];
    let mut stops = Vec::new();
    for ((l, dir), (id, local)) in
        listeners
            .into_iter()
            .zip(&dirs)
            .zip([("a", Some("p1")), ("b", Some("p2")), ("c", None)])
    {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
        let (stop, _) = data_node_tokens(&rt, dir, id, local, l, "");
        stops.push(stop);
    }
    let [a, b, c] = ports;
    rt.block_on(async {
        put(c, "alice", "1").await;

        // Info: each node its own partition, the map's others named, the node's own totals.
        let i = info(a).await;
        let ids: Vec<&str> = i.partitions.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(ids, ["p1"], "{i:?}");
        assert_eq!(i.other_partitions, ["p2"], "{i:?}");
        let p1 = &i.partitions[0];
        assert_eq!(p1.state(), LocalPartitionState::Serving, "{p1:?}");
        assert!(p1.file_bytes > 0 && p1.path.ends_with("p1.phdb"), "{p1:?}");
        assert_eq!(p1.tables, ["users"], "{p1:?}");
        assert_eq!(i.file_bytes, p1.file_bytes);
        assert_eq!(i.path, dirs[0].display().to_string());
        assert_eq!(i.tables, ["users"], "the map's tables: {i:?}");
        let i = info(b).await;
        assert_eq!(
            i.partitions
                .iter()
                .map(|p| p.id.as_str())
                .collect::<Vec<_>>(),
            ["p2"]
        );
        assert_eq!(i.other_partitions, ["p1"]);
        // A node the map gives no partition only routes: nothing of its own.
        let i = info(c).await;
        assert!(i.partitions.is_empty() && i.file_bytes == 0, "{i:?}");
        assert_eq!(i.other_partitions, ["p1", "p2"]);

        // Flush and Compact: every open partition, or the one named; never forwarded.
        let mut admin_a = admin(a).await;
        admin_a.flush(named("")).await.unwrap();
        admin_a.compact(named("")).await.unwrap();
        admin_a.flush(named("p1")).await.unwrap();
        admin_a.compact(named("p1")).await.unwrap();
        let e = admin_a.flush(named("p2")).await.unwrap_err();
        assert_eq!(code(&e), "WrongPartition", "{e}");
        let e = admin_a.compact(named("nope")).await.unwrap_err();
        assert_eq!(code(&e), "NotFound", "{e}");
        // A routing-only node has nothing to flush, and that's not an error.
        admin(c).await.flush(named("")).await.unwrap();

        // Fenced: Info says so and why; its files still flush.
        client(a)
            .await
            .fence(PartitionRequest {
                partition: "p1".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let p1 = info(a).await.partitions.remove(0);
        assert_eq!(
            (p1.state(), p1.fence_reason()),
            (LocalPartitionState::Fenced, FenceReason::Operator),
            "{p1:?}"
        );
        admin_a.flush(named("p1")).await.unwrap();

        // Closed: Info shows it with no size; an all-partition flush passes it over, and
        // one naming it answers PartitionUnavailable.
        client(a)
            .await
            .close(PartitionRequest {
                partition: "p1".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let p1 = info(a).await.partitions.remove(0);
        assert_eq!(p1.state(), LocalPartitionState::Closed, "{p1:?}");
        assert!(p1.file_bytes == 0 && p1.tables.is_empty(), "{p1:?}");
        admin_a.flush(named("")).await.unwrap();
        let e = admin_a.flush(named("p1")).await.unwrap_err();
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");

        // A map that moves p2 to a, while b still has p2's copy open (simp724's N1): b's Info
        // lists what the map gives it, which is nothing now, and its Flush agrees, leaving the
        // old copy alone; a lists p2, which it hasn't opened.
        let mut v2 = map.clone();
        v2.map_version = 2;
        for p in &mut v2.partitions {
            if p.id == "p2" {
                p.node = "a".into();
                p.epoch = 2;
            }
        }
        for port in [a, b] {
            client(port)
                .await
                .push_map(crate::serve::proto::PushMapRequest {
                    map: Some(v2.clone()),
                })
                .await
                .unwrap();
        }
        let i = info(b).await;
        assert!(i.partitions.is_empty() && i.file_bytes == 0, "{i:?}");
        assert_eq!(i.other_partitions, ["p1", "p2"], "{i:?}");
        admin(b).await.flush(named("")).await.unwrap();
        let e = admin(b).await.flush(named("p2")).await.unwrap_err();
        assert_eq!(code(&e), "WrongPartition", "{e}");
        let i = info(a).await;
        let states: Vec<(&str, LocalPartitionState)> = i
            .partitions
            .iter()
            .map(|p| (p.id.as_str(), p.state()))
            .collect();
        assert_eq!(
            states,
            [
                ("p1", LocalPartitionState::Closed),
                ("p2", LocalPartitionState::Unspecified)
            ],
            "{i:?}"
        );
    });
    drop(stops);
}
