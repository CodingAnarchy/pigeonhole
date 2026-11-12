//! `Cluster.Stamp` (D317 ticket 6): a partition's files uploaded to a node install there
//! and take the next epoch; an interrupted install finishes on the next stamp; a stale file
//! is refused without `restore`; and a map that then gives the node the partition at that
//! epoch opens and serves it.

use sha2::{Digest, Sha256};

use super::upload::upload;
use super::*;
use crate::serve::proto::{
    LocalPartition, LocalPartitionState, PartitionFile, PartitionUploadHeader,
    PartitionUploadPurpose, StampRequest,
};

async fn stamp(
    port: u16,
    from: u64,
    to: u64,
    restore: bool,
) -> Result<LocalPartition, tonic::Status> {
    client(port)
        .await
        .stamp(StampRequest {
            cluster_id: "c1".into(),
            partition: "p3".into(),
            from_epoch: from,
            to_epoch: to,
            restore,
            restore_from_cluster_id: String::new(),
        })
        .await
        .map(tonic::Response::into_inner)
}

/// Uploads every file of the database at `main` to `port` as partition `p`'s set at epoch 1.
async fn upload_set(port: u16, p: &str, main: &Path, ids: u8) -> Vec<PartitionFile> {
    let files: Vec<(PartitionFile, Vec<u8>)> = pigeonhole::Pigeonhole::database_files(main)
        .unwrap()
        .iter()
        .map(|f| {
            let data = std::fs::read(f).unwrap();
            let name = pigeonhole::Pigeonhole::database_file_suffix(main, f).unwrap();
            (
                PartitionFile {
                    name: name.to_owned(),
                    bytes: data.len() as u64,
                    sha256: Sha256::digest(&data).to_vec().into(),
                },
                data,
            )
        })
        .collect();
    let manifest: Vec<PartitionFile> = files.iter().map(|(f, _)| f.clone()).collect();
    for (i, (file, data)) in files.iter().enumerate() {
        let header = PartitionUploadHeader {
            partition: p.into(),
            cluster_id: "c1".into(),
            epoch: 1,
            upload_id: format!("{ids:02x}{i:02x}").repeat(8),
            purpose: PartitionUploadPurpose::Relocate as i32,
            file: Some(file.clone()),
            manifest: manifest.clone(),
        };
        let chunks: Vec<(u64, &[u8])> = data
            .chunks(512 << 10)
            .enumerate()
            .map(|(k, c)| ((k * (512 << 10)) as u64, c))
            .collect();
        let chunks = if chunks.is_empty() {
            vec![(0, &data[..])]
        } else {
            chunks
        };
        // Eight chunks a stream (the helper's channel holds eight before the call starts),
        // each resuming where the last ended.
        let mut last = None;
        for group in chunks.chunks(8) {
            last = Some(upload(port, &header, group).await.unwrap());
        }
        let r = last.unwrap();
        assert!(r.complete, "{r:?}");
    }
    manifest
}

#[test]
fn an_uploaded_set_installs_with_the_next_epoch_and_the_map_then_serves_it() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let mut map = the_map(1, &ports);
    map.partitions.retain(|p| p.id != "p3");
    let dirs = [test_dir(), test_dir(), test_dir()];
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for ((l, dir), (id, local)) in
        listeners
            .into_iter()
            .zip(&dirs)
            .zip([("a", Some("p1")), ("b", Some("p2")), ("c", None)])
    {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
        let (stop, db) = data_node_tokens(&rt, dir, id, local, l, "");
        stops.push(stop);
        dbs.extend(db);
    }
    // p3's files, as another node closed them.
    let source = test_dir();
    // One shard: one WAL file to upload and open.
    let p3 = test_partition_with(&source, "p3", pigeonhole::Options::default().shards(1));
    p3.close().unwrap();
    let main = source.join("p3.phdb");
    let c = ports[2];
    let installed = dirs[2].join("p3.phdb");
    rt.block_on(async {
        // Nothing staged and no file: refused.
        let e = stamp(c, 1, 2, false).await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        // An epoch skipped: refused.
        let e = stamp(c, 1, 3, false).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e}");
        // Staged, then an install that fails after its first rename, as a crash would
        // leave it: the marker stays, and the next stamp finishes it.
        let manifest = upload_set(c, "p3", &main, 1).await;
        *super::super::stamp::FAIL_AFTER_FIRST_RENAME.lock().unwrap() = Some(dirs[2].clone());
        let e = stamp(c, 1, 2, false).await.unwrap_err();
        assert!(e.message().contains("after the first rename"), "{e}");
        assert!(dirs[2].join(".phdb-incoming/p3.installing").exists());
        let lp = stamp(c, 1, 2, false).await.unwrap();
        assert_eq!(lp.files, manifest, "the reply carries the set installed");
        assert!(!dirs[2].join(".phdb-incoming/p3.installing").exists());
        let id = super::super::identity::read_file(&installed)
            .unwrap()
            .unwrap();
        assert_eq!((id.cluster_id.as_str(), id.epoch), ("c1", 2));
        // The staging holds nothing of p3's any more (N2).
        let left: Vec<String> = std::fs::read_dir(dirs[2].join(".phdb-incoming"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("p3."))
            .collect();
        assert!(left.is_empty(), "{left:?}");
        // Again: done already, nothing installed.
        assert!(stamp(c, 1, 2, false).await.unwrap().files.is_empty());
        // A stale file (epoch 2, the map's 3): refused, unless it's a restore.
        let e = stamp(c, 3, 4, false).await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        assert!(e.message().contains("at epoch 2"), "{e}");
        stamp(c, 3, 4, true).await.unwrap();
        // The map gives c partition p3 at epoch 4: it opens and serves it.
        let mut m = the_map(2, &ports);
        m.partitions
            .iter_mut()
            .filter(|p| p.id == "p3")
            .for_each(|p| p.epoch = 4);
        client(ports[0])
            .await
            .set_map(SetMapRequest {
                force: false,
                expected_map_version: 1,
                map: Some(m),
                copies: Vec::new(),
            })
            .await
            .unwrap();
        eventually("c to serve p3", async || {
            client(c)
                .await
                .status(StatusRequest {})
                .await
                .unwrap()
                .into_inner()
                .partitions
                .iter()
                .any(|lp| lp.id == "p3" && lp.state() == LocalPartitionState::Serving)
        })
        .await;
        // Open now: a stamp is refused.
        let e = stamp(c, 4, 5, false).await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
    });
    drop(stops);
    for db in dbs {
        let _ = db.close();
    }
}

/// `phdb cluster relocate` end to end: p1, fenced and closed on a, its files uploaded to c,
/// moves to c, which serves its rows and takes its writes; run again, it's done; a's old
/// files then drop.
#[test]
fn relocate_moves_a_closed_partition_to_the_node_holding_its_files() {
    use crate::serve::proto::PartitionRequest;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let mut map = the_map(1, &ports);
    map.partitions.retain(|p| p.id != "p3");
    let dirs = [test_dir(), test_dir(), test_dir()];
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for ((l, dir), (id, local)) in
        listeners
            .into_iter()
            .zip(&dirs)
            .zip([("a", Some("p1")), ("b", Some("p2")), ("c", None)])
    {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
        let (stop, db) = data_node_tokens(&rt, dir, id, local, l, "");
        stops.push(stop);
        dbs.extend(db);
    }
    let [a, b, c] = ports;
    let args = super::super::cli::NodeArgs {
        node: format!("http://127.0.0.1:{b}"),
        token_file: None,
        ca: None,
    };
    let req = || PartitionRequest {
        partition: "p1".into(),
        ..Default::default()
    };
    rt.block_on(async {
        put(b, "alice", "1").await;
        // Not fenced on a: refused.
        let e = within(super::super::cli::relocate(&args, "p1", "c", false, false))
            .await
            .err()
            .expect("refused");
        assert!(
            e.message().contains("isn't fenced or closed"),
            "{}",
            e.message()
        );
        // --old-unreachable still asks a, which answers and serves p1 (cli692's S3).
        let e = within(super::super::cli::relocate(&args, "p1", "c", true, false))
            .await
            .err()
            .expect("refused");
        assert!(
            e.message().contains("answers and serves"),
            "{}",
            e.message()
        );
        // A copy is taken from the old node's closed files, so it's never unreachable.
        let e = within(super::super::cli::relocate(&args, "p1", "c", true, true))
            .await
            .err()
            .expect("refused");
        assert!(e.message().contains("--copy needs"), "{}", e.message());
        client(a).await.fence(req()).await.unwrap();
        client(a).await.close(req()).await.unwrap();
        upload_set(c, "p1", &dirs[0].join("p1.phdb"), 2).await;
        // Its files were copied whole (`--copy`): p1 keeps its feed history (D317 ticket 6).
        let r = within(super::super::cli::relocate(&args, "p1", "c", false, true))
            .await
            .unwrap();
        assert_eq!(r.document["epoch"], 2, "{:?}", r.document);
        assert_eq!(r.document["copied"], true, "{:?}", r.document);
        for port in ports {
            let m = client(port)
                .await
                .get_map(crate::serve::proto::GetMapRequest {})
                .await
                .unwrap()
                .into_inner();
            let p1 = m.partitions.iter().find(|p| p.id == "p1").unwrap();
            assert_eq!(
                (p1.node.as_str(), p1.epoch, p1.history_epoch),
                ("c", 2, 0),
                "on {port}: the copy keeps p1's history (0: the first map's, unmoved); a \
                 move that isn't a copy starts another at 2"
            );
        }
        assert!(r.document["installed_files"].as_u64().unwrap() >= 1);
        eventually("c to serve p1", async || {
            get_row(b, "alice").await.ok().flatten() == Some(b"1".to_vec())
        })
        .await;
        // Writes through a go to c now.
        put(a, "alice", "2").await;
        assert_eq!(get_row(c, "alice").await.unwrap(), Some(b"2".to_vec()));
        // Again: done.
        let r = within(super::super::cli::relocate(&args, "p1", "c", false, true))
            .await
            .unwrap();
        assert!(r.document.contains_key("done"), "{:?}", r.document);
        // a's old copy goes aside.
        client(a).await.drop_partition(req()).await.unwrap();
        assert!(!dirs[0].join("p1.phdb").exists());
    });
    drop(stops);
    for db in dbs {
        let _ = db.close();
    }
}

/// A `phdb cluster move` run again after an earlier one stamped the new node but died
/// before the map moved: it finishes the move, but can't compare the set again (the stamp
/// installs nothing this time), so it says the copy isn't re-verified and keeps the old
/// node's files even with `--drop-source`.
#[test]
fn a_move_run_again_after_its_stamp_finishes_but_drops_nothing() {
    use crate::serve::proto::PartitionRequest;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let mut map = the_map(1, &ports);
    map.partitions.retain(|p| p.id != "p3");
    let dirs = [test_dir(), test_dir(), test_dir()];
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for ((l, dir), (id, local)) in
        listeners
            .into_iter()
            .zip(&dirs)
            .zip([("a", Some("p1")), ("b", Some("p2")), ("c", None)])
    {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
        let (stop, db) = data_node_tokens(&rt, dir, id, local, l, "");
        stops.push(stop);
        dbs.extend(db);
    }
    let [a, b, c] = ports;
    let args = super::super::cli::NodeArgs {
        node: format!("http://127.0.0.1:{b}"),
        token_file: None,
        ca: None,
    };
    let req = || PartitionRequest {
        partition: "p1".into(),
        ..Default::default()
    };
    rt.block_on(async {
        put(b, "alice", "1").await;
        // The earlier run, up to its stamp: fenced and closed on a, copied and stamped on c.
        client(a).await.fence(req()).await.unwrap();
        client(a).await.close(req()).await.unwrap();
        eventually("p1's files to be hashed", async || {
            client(a)
                .await
                .status(StatusRequest {})
                .await
                .unwrap()
                .into_inner()
                .partitions
                .iter()
                .any(|lp| lp.id == "p1" && !lp.files.is_empty())
        })
        .await;
        upload_set(c, "p1", &dirs[0].join("p1.phdb"), 3).await;
        let lp = client(c)
            .await
            .stamp(StampRequest {
                cluster_id: "c1".into(),
                partition: "p1".into(),
                from_epoch: 1,
                to_epoch: 2,
                restore: false,
                restore_from_cluster_id: String::new(),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(!lp.files.is_empty(), "installed: {lp:?}");

        let r = within(super::super::moving::run(&args, "p1", "c", true, None))
            .await
            .unwrap();
        assert_eq!(r.document["verified"], false, "{:?}", r.document);
        let source = r.document["source"].as_str().unwrap();
        assert!(source.contains("not re-verified"), "{source}");
        assert!(source.contains("drop-partition p1"), "{source}");
        assert!(!r.document.contains_key("dropped"), "{:?}", r.document);
        assert!(dirs[0].join("p1.phdb").exists(), "a's files stay");
        // The move itself finished: c serves p1, with its history kept.
        assert_eq!(get_row(b, "alice").await.unwrap(), Some(b"1".to_vec()));
        let m = client(b)
            .await
            .get_map(crate::serve::proto::GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        let p1 = m.partitions.iter().find(|p| p.id == "p1").unwrap();
        assert_eq!((p1.node.as_str(), p1.epoch, p1.history_epoch), ("c", 2, 0));
    });
    drop(stops);
    for db in dbs {
        let _ = db.close();
    }
}
