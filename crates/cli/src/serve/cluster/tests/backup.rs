//! Cluster backup (D317 ticket 8b, §9): `phdb cluster backup` takes every partition's copy
//! through its owner, never forwarded, checks each against the map, and keeps the map read
//! at its end when only tables were created or dropped meanwhile (simp724's A2); a node
//! refuses a partition it doesn't hold, and a cluster node's backup must name one.

use super::*;
use crate::serve::cluster::backup::{backup, backup_then};
use crate::serve::cluster::cli::NodeArgs;
use crate::serve::proto::{BackupDownloadRequest, admin_client::AdminClient};

pub(super) fn args(port: u16) -> NodeArgs {
    NodeArgs {
        node: format!("http://127.0.0.1:{port}"),
        token_file: None,
        ca: None,
    }
}

async fn admin(port: u16) -> AdminClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    AdminClient::new(channel)
}

/// Writes `n` rows of `users` through node a, spread over the three partitions.
pub(super) fn load(rt: &tokio::runtime::Runtime, port: u16, n: usize) -> Vec<Vec<u8>> {
    let rows: Vec<Vec<u8>> = (0..n)
        .map(|i| format!("{}{i:03}", ["a", "h", "x"][i % 3]).into_bytes())
        .collect();
    rt.block_on(async {
        for (i, r) in rows.iter().enumerate() {
            data(port)
                .await
                .mutate(super::super::proto::MutateRequest {
                    writes: Some(cells_row(r, &[3], i as u64)),
                    durability: 0,
                })
                .await
                .unwrap();
        }
    });
    rows
}

/// The rows of `users` in a backup's file, opened read-only.
fn rows_in(path: &Path) -> (Option<identity::Identity>, Vec<Vec<u8>>) {
    let db =
        pigeonhole::Pigeonhole::open(path, pigeonhole::Options::default().read_only(true)).unwrap();
    let id = identity::read(&db).unwrap();
    let rows = db
        .table("users")
        .unwrap()
        .open()
        .unwrap()
        .scan_prefix(b"")
        .iter()
        .unwrap()
        .map(|r| r.unwrap().key().to_vec())
        .collect();
    let _ = db.close();
    (id, rows)
}

#[test]
fn a_cluster_backup_holds_every_partitions_rows_each_through_its_owner() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes(&rt);
    let rows = load(&rt, ports[0], 30);
    let out = test_dir().join("backup");
    // Through node c: each copy comes from its owner, whichever node is asked.
    let report = rt
        .block_on(backup(&args(ports[2]), &out, false, 2))
        .unwrap();
    assert_eq!(report.document["partitions"], 3, "{:?}", report.document);
    let manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["map_version"], 1);
    let map =
        super::super::map::parse_json(&std::fs::read_to_string(out.join("map.json")).unwrap())
            .unwrap();
    assert_eq!(map.map_version, 1);
    let mut all = Vec::new();
    for (p, entry) in ["p1", "p2", "p3"]
        .iter()
        .zip(manifest["partitions"].as_array().unwrap())
    {
        assert_eq!(entry["partition"], *p);
        let (id, got) = rows_in(&out.join(format!("{p}.phdb")));
        let id = id.expect("the copy carries its identity");
        assert_eq!(
            (id.cluster_id.as_str(), id.partition_id.as_str(), id.epoch),
            ("c1", *p, 1)
        );
        // Every row in the copy is one the map places in that partition.
        for r in &got {
            let want = match r[0] {
                b'a' => "p1",
                b'h' => "p2",
                _ => "p3",
            };
            assert_eq!(
                want,
                *p,
                "row {:?} in {p}'s copy",
                r.escape_ascii().to_string()
            );
        }
        all.extend(got);
    }
    all.sort();
    let mut want = rows;
    want.sort();
    assert_eq!(all, want, "the copies hold every row once");
    assert!(
        !std::fs::read_dir(&out).unwrap().any(|e| e
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".partial")),
        "no partial copy is left"
    );
    // A second backup into the same directory is refused: it isn't empty.
    let e = rt
        .block_on(backup(&args(ports[0]), &out, false, 2))
        .err()
        .expect("refused");
    assert!(e.message().contains("isn't empty"), "{}", e.message());
}

#[test]
fn a_node_backs_up_only_a_partition_it_holds_named() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes(&rt);
    rt.block_on(async {
        let ask = |partition: &str| BackupDownloadRequest {
            partition: partition.into(),
            ..Default::default()
        };
        let first = |port: u16, r: BackupDownloadRequest| async move {
            let mut s = admin(port).await.backup_download(r).await?.into_inner();
            s.message().await.map(|_| ())
        };
        // Another node's partition: never forwarded.
        let e = first(ports[0], ask("p2")).await.unwrap_err();
        assert_eq!(code(&e), "WrongPartition", "{e:?}");
        // None named, even on a node holding one.
        let e = first(ports[0], ask("")).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e:?}");
        // One the map doesn't have.
        let e = first(ports[0], ask("p9")).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::NotFound, "{e:?}");
        // Its own: the last chunk names what the copy holds.
        let mut s = admin(ports[0])
            .await
            .backup_download(ask("p1"))
            .await
            .unwrap()
            .into_inner();
        let last = loop {
            let c = s.message().await.unwrap().expect("a last chunk");
            if c.total != 0 {
                break c;
            }
            assert!(c.partition.is_empty(), "identity only in the last chunk");
        };
        assert_eq!(
            (
                last.partition.as_str(),
                last.cluster_id.as_str(),
                last.epoch,
                last.map_version
            ),
            ("p1", "c1", 1, 1)
        );
        // A resume names the partition its copy is of.
        let e = first(
            ports[0],
            BackupDownloadRequest {
                partition: "p1".into(),
                backup_id: last.backup_id.clone(),
                ..Default::default()
            },
        )
        .await;
        assert!(e.is_ok(), "{e:?}");
    });
}

#[test]
fn a_table_created_during_a_backup_is_in_the_map_it_keeps() {
    // simp724's A2: the map at the end (V′) is kept when only tables changed, so a restore's
    // reconcile neither drops a table a copy holds nor recreates one empty.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes(&rt);
    load(&rt, ports[0], 6);
    let out = test_dir().join("backup");
    let report = rt
        .block_on(backup_then(&args(ports[0]), &out, false, 3, async || {
            let mut m = three_way(&[ports[0], ports[1], ports[2]]);
            m.map_version = 2;
            m.tables.push(TablePlacement {
                name: "orders".into(),
                group: String::new(),
                families: Vec::new(),
                splits: Vec::new(),
                partitions: vec!["p1".into()],
                created: 0,
            });
            client(ports[0])
                .await
                .set_map(SetMapRequest {
                    force: false,
                    expected_map_version: 1,
                    map: Some(m),
                })
                .await
                .unwrap();
        }))
        .unwrap();
    let manifest = &report.document["manifest"];
    assert_eq!(
        (
            manifest["map_version_at_start"].as_u64(),
            manifest["map_version"].as_u64()
        ),
        (Some(1), Some(2))
    );
    let map =
        super::super::map::parse_json(&std::fs::read_to_string(out.join("map.json")).unwrap())
            .unwrap();
    assert!(map.tables.iter().any(|t| t.name == "orders"), "V′ is kept");
}

#[test]
fn a_copy_that_isnt_the_maps_partition_at_its_epoch_fails_the_backup() {
    // The node's file says epoch 2 while the map says 1: a relocation or restore the map
    // hasn't caught up with. The backup names it and leaves no manifest.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, dbs) = three_nodes(&rt);
    identity::write(
        &dbs[1],
        &identity::Identity {
            cluster_id: "c1".into(),
            partition_id: "p2".into(),
            epoch: 2,
        },
    )
    .unwrap();
    let out = test_dir().join("backup");
    let e = rt
        .block_on(backup(&args(ports[0]), &out, false, 3))
        .err()
        .expect("refused");
    assert!(
        e.message()
            .contains("partition \"p2\" of cluster \"c1\" at epoch 2"),
        "{}",
        e.message()
    );
    assert!(
        !out.join("manifest.json").exists(),
        "no manifest: the backup isn't whole"
    );
}
