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
        // The passes before the install, so the wait below is for the pass it starts.
        let before = super::super::open::missing_passes(&dirs[0]);
        for port in [a, b] {
            client(port)
                .await
                .push_map(crate::serve::proto::PushMapRequest {
                    map: Some(v2.clone()),
                })
                .await
                .unwrap();
        }
        // a's install opens p2 in the background, and it fails on the missing file: wait
        // it out, or a's Info below can catch p2 while it is still marked opening.
        eventually("a's background open of p2 to run", async || {
            super::super::open::missing_passes(&dirs[0]) > before
        })
        .await;
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

#[test]
fn a_moves_calls_take_a_nodes_cluster_token_and_still_refuse_a_clients() {
    // The coordinator's scope ruling (D332 PR 3, D331 PR 5): the map's writer runs a move
    // with its own cluster token, so Fence, Unfence, Close, DropPartition, BackupDownload and
    // PartitionUpload take Scope::Cluster beside Admin. A client's write token is still
    // refused each one.
    let tokens = format!("write t0ken-w\nadmin t0ken-adm\ncluster {NODE_TOKEN}\n");
    let t = Three::start([&tokens, &tokens, &tokens]);
    let a = t.ports[0];
    let req = || PartitionRequest {
        partition: "p9".into(),
        ..Default::default()
    };
    t.rt.block_on(async {
        for token in [NODE_TOKEN, "t0ken-w"] {
            let refused = |r: Result<(), tonic::Status>, what: &str| {
                let denied = r
                    .as_ref()
                    .is_err_and(|e| e.code() == tonic::Code::PermissionDenied);
                assert_eq!(
                    denied,
                    token == "t0ken-w",
                    "{what} with {}: {r:?}",
                    if token == NODE_TOKEN {
                        "the cluster token"
                    } else {
                        "a write token"
                    }
                );
            };
            let c = || client(a);
            refused(
                c().await.fence(with(req(), Some(token))).await.map(drop),
                "Fence",
            );
            refused(
                c().await.close(with(req(), Some(token))).await.map(drop),
                "Close",
            );
            refused(
                c().await.unfence(with(req(), Some(token))).await.map(drop),
                "Unfence",
            );
            refused(
                c().await
                    .drop_partition(with(req(), Some(token)))
                    .await
                    .map(drop),
                "DropPartition",
            );
            refused(
                admin(a)
                    .await
                    .backup_download(with(
                        crate::serve::proto::BackupDownloadRequest {
                            partition: "p1".into(),
                            fenced_file: Some("p1.phdb".into()),
                            ..Default::default()
                        },
                        Some(token),
                    ))
                    .await
                    .map(drop),
                "BackupDownload",
            );
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            drop(tx);
            refused(
                admin(a)
                    .await
                    .partition_upload(with(super::upload::Chunks(rx), Some(token)))
                    .await
                    .map(drop),
                "PartitionUpload",
            );
        }
        // Upload status: the cluster token, not a client's (audit120's S2).
        for (token, denied) in [(NODE_TOKEN, false), ("t0ken-w", true)] {
            let r = admin(a)
                .await
                .partition_upload_status(with(
                    crate::serve::proto::PartitionUploadStatusRequest {
                        upload_id: "0123456789abcdef".repeat(2),
                        partition: "p1".into(),
                    },
                    Some(token),
                ))
                .await;
            assert_eq!(
                r.as_ref()
                    .is_err_and(|e| e.code() == tonic::Code::PermissionDenied),
                denied,
                "PartitionUploadStatus with {token}: {r:?}"
            );
        }
        // Still admin only for the cluster token (audit120's S2): a download that isn't a
        // partition's closed set or a replica's bootstrap.
        let e = admin(a)
            .await
            .backup_download(with(
                crate::serve::proto::BackupDownloadRequest::default(),
                Some(NODE_TOKEN),
            ))
            .await
            .unwrap_err();
        assert_eq!(
            e.code(),
            tonic::Code::PermissionDenied,
            "a whole download: {e}"
        );
        // A named partition's download that's neither a closed set nor a replica's bootstrap
        // (audit120's N1), and a forced unfence (simp724): admin only.
        let e = admin(a)
            .await
            .backup_download(with(
                crate::serve::proto::BackupDownloadRequest {
                    partition: "p1".into(),
                    ..Default::default()
                },
                Some(NODE_TOKEN),
            ))
            .await
            .unwrap_err();
        assert_eq!(
            e.code(),
            tonic::Code::PermissionDenied,
            "a named whole download: {e}"
        );
        let e = client(a)
            .await
            .unfence(with(
                PartitionRequest {
                    partition: "p1".into(),
                    force: true,
                },
                Some(NODE_TOKEN),
            ))
            .await
            .unwrap_err();
        assert_eq!(
            e.code(),
            tonic::Code::PermissionDenied,
            "a forced unfence: {e}"
        );
        assert!(
            e.message().contains("a forced unfence is an admin's"),
            "{e}"
        );
        // A restore is an admin's upload: the cluster token is refused one (simp724's S1).
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.send(crate::serve::proto::PartitionUploadChunk {
            header: Some(crate::serve::proto::PartitionUploadHeader {
                partition: "p1".into(),
                purpose: crate::serve::proto::PartitionUploadPurpose::Restore as i32,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        drop(tx);
        let e = admin(a)
            .await
            .partition_upload(with(super::upload::Chunks(rx), Some(NODE_TOKEN)))
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::PermissionDenied, "{e}");
        assert!(
            e.message().contains("a restore is an admin's upload"),
            "{e}"
        );
    });
}
