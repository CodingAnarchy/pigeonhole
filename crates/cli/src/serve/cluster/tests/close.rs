//! `phdb cluster close` (D317 amendment 2): a fenced partition closes, its calls answer
//! `PartitionUnavailable`, `Status` shows it closed with its files' hashes once they're
//! taken, and unfence reopens it with its data.

use super::*;
use crate::serve::proto::{LocalPartition, LocalPartitionState, PartitionRequest};

async fn close(port: u16, p: &str) -> Result<LocalPartition, tonic::Status> {
    client(port)
        .await
        .close(PartitionRequest {
            partition: p.into(),
            ..Default::default()
        })
        .await
        .map(tonic::Response::into_inner)
}

async fn local(port: u16, p: &str) -> LocalPartition {
    client(port)
        .await
        .status(StatusRequest {})
        .await
        .unwrap()
        .into_inner()
        .partitions
        .into_iter()
        .find(|lp| lp.id == p)
        .unwrap()
}

#[test]
fn a_fenced_partition_closes_with_its_files_hashed_and_unfence_reopens_it() {
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
    let [a, _, c] = ports;
    rt.block_on(async {
        put(c, "alice", "1").await;
        let e = close(a, "p1").await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        assert!(e.message().contains("fence it first"), "{e}");
        client(a)
            .await
            .fence(PartitionRequest {
                partition: "p1".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let lp = close(a, "p1").await.unwrap();
        assert_eq!(lp.state(), LocalPartitionState::Closed, "{lp:?}");
        // Its calls, here and forwarded, answer PartitionUnavailable.
        for entry in [a, c] {
            let e = get_row(entry, "alice").await.unwrap_err();
            assert_eq!(code(&e), "PartitionUnavailable", "through {entry}: {e}");
        }
        eventually("p1's files to be hashed", async || {
            !local(a, "p1").await.files.is_empty()
        })
        .await;
        let lp = local(a, "p1").await;
        assert_eq!(lp.state(), LocalPartitionState::Closed, "{lp:?}");
        assert!(lp.error.is_empty(), "{lp:?}");
        let main = lp
            .files
            .iter()
            .find(|f| f.name.is_empty())
            .expect("the main file");
        let on_disk = std::fs::read(dirs[0].join("p1.phdb")).unwrap();
        assert_eq!(main.bytes, on_disk.len() as u64);
        assert_eq!(
            main.sha256.to_vec(),
            <sha2::Sha256 as sha2::Digest>::digest(&on_disk).to_vec()
        );
        // Again: the same answer, nothing done.
        assert_eq!(close(a, "p1").await.unwrap().files, lp.files);
        // Each closed file streams as it lies, the main file (`""`) included, matching its
        // listed size and hash (D317 ticket 6: what a move copies).
        for f in &lp.files {
            let mut chunks = super::upload::admin(a)
                .await
                .backup_download(crate::serve::proto::BackupDownloadRequest {
                    partition: "p1".into(),
                    fenced_file: Some(f.name.clone()),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            let mut got = Vec::new();
            while let Some(c) = chunks.message().await.unwrap() {
                got.extend_from_slice(&c.data);
            }
            assert_eq!(got.len() as u64, f.bytes, "{:?}", f.name);
            assert_eq!(
                <sha2::Sha256 as sha2::Digest>::digest(&got).to_vec(),
                f.sha256.to_vec(),
                "{:?}",
                f.name
            );
            let mut path = dirs[0].join("p1.phdb").into_os_string();
            path.push(&f.name);
            assert_eq!(got, std::fs::read(path).unwrap(), "{:?}", f.name);
        }
        let e = super::upload::admin(a)
            .await
            .backup_download(crate::serve::proto::BackupDownloadRequest {
                partition: "p1".into(),
                fenced_file: Some("-wal-99".into()),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::NotFound, "{e:?}");
        assert!(e.message().contains("has no file \"-wal-99\""), "{e:?}");
        // Unfence reopens it, and its rows are there.
        let lp = client(a)
            .await
            .unfence(PartitionRequest {
                partition: "p1".into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(lp.state(), LocalPartitionState::Serving, "{lp:?}");
        assert!(lp.files.is_empty(), "{lp:?}");
        assert_eq!(get_row(c, "alice").await.unwrap(), Some(b"1".to_vec()));
        put(c, "alice", "2").await;
        assert_eq!(get_row(a, "alice").await.unwrap(), Some(b"2".to_vec()));
    });
    drop(stops);
    for db in dbs {
        let _ = db.close();
    }
}

/// A call already running on a partition when it closes ends with `PartitionUnavailable`,
/// which a client routes again on, not the engine's `Closed` (S5): here, a cluster
/// partition's service whose database closed under it.
#[test]
fn a_call_on_a_partition_closed_under_it_answers_partition_unavailable() {
    use crate::serve::service::{Service, ServiceOp};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = test_dir();
    let db = test_partition(&dir, "p1");
    let service = Service::routing_only(
        crate::serve::server_tests::limits(),
        2,
        Arc::new(crate::serve::service::Metrics::default()),
    );
    let backups = crate::serve::backup::Backups::new(None, 1).unwrap();
    let part = service.for_partition(
        db.clone(),
        "p1",
        &backups,
        crate::serve::service::Pinning::default(),
        crate::serve::placement::Memory::defaults(1),
        crate::serve::gate::WriteGate::opened("p1", false),
    );
    db.close().unwrap();
    let get = ServiceOp::Get(super::super::proto::GetRequest {
        table: "users".into(),
        row: b"alice".to_vec().into(),
        family: "f".into(),
        qualifier: b"q".to_vec().into(),
        ..Default::default()
    });
    let e = rt
        .block_on(part.call(get, &crate::serve::auth::Caller::anonymous(), None))
        .unwrap_err();
    assert_eq!(e.code, "PartitionUnavailable", "{}", e.message);
    assert!(e.message.contains("closed on this node"), "{}", e.message);
}

/// `drop-partition`: refused while the map still places the partition on the node, then
/// its files move to `.phdb-dropped/`, nothing deleted.
#[test]
fn drop_partition_moves_a_closed_partitions_files_aside_once_the_map_moved_it() {
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
    let a = ports[0];
    let drop_p1 = async || {
        client(a)
            .await
            .drop_partition(PartitionRequest {
                partition: "p1".into(),
                ..Default::default()
            })
            .await
            .map(tonic::Response::into_inner)
    };
    rt.block_on(async {
        client(a)
            .await
            .fence(PartitionRequest {
                partition: "p1".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        close(a, "p1").await.unwrap();
        let e = drop_p1().await.unwrap_err();
        assert_eq!(code(&e), "FailedPrecondition", "{e}");
        assert!(e.message().contains("still places"), "{e}");
        assert!(dirs[0].join("p1.phdb").exists());
        // The map moves p1 to b (its files there are the relocation's business).
        let mut m = the_map(2, &ports);
        m.partitions.retain(|p| p.id != "p3");
        m.partitions
            .iter_mut()
            .filter(|p| p.id == "p1")
            .for_each(|p| {
                p.node = "b".into();
                p.epoch = 2;
            });
        client(a)
            .await
            .set_map(SetMapRequest {
                force: false,
                expected_map_version: 1,
                map: Some(m),
                copies: Vec::new(),
            })
            .await
            .unwrap();
        let lp = drop_p1().await.unwrap();
        assert_eq!(lp.epoch, 1, "{lp:?}");
        assert!(lp.file.starts_with(".phdb-dropped/p1-1-"), "{lp:?}");
        assert!(!dirs[0].join("p1.phdb").exists());
        assert!(dirs[0].join(&lp.file).join("p1.phdb").is_file());
        // Nothing of p1 left to drop.
        let e = drop_p1().await.unwrap_err();
        assert_eq!(code(&e), "NotFound", "{e}");
        // A drop that stopped after the main file (as before cli692's N6, or a crash):
        // a retry moves the sidecars that are left.
        std::fs::write(dirs[0].join("p1.phdb-wal-0"), b"left").unwrap();
        std::fs::write(dirs[0].join("p1.phdb-feed"), b"left").unwrap();
        let lp = drop_p1().await.unwrap();
        assert!(lp.file.starts_with(".phdb-dropped/p1-0-"), "{lp:?}");
        assert!(dirs[0].join(&lp.file).join("p1.phdb-feed").is_file());
        assert!(!dirs[0].join("p1.phdb-wal-0").exists());
    });
    drop(stops);
    for db in dbs {
        let _ = db.close();
    }
}

/// cli692's N1 on #1769: while a close is between taking the partition out of the parts and
/// closing its database, a read of the file is refused by the writer lock. Status then
/// shows the partition closed, with no error: the lock is the close's own, and transient.
#[test]
fn a_partition_closing_shows_closed_without_the_writer_locks_error() {
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
    let a = ports[0];
    let (release, wait) = std::sync::mpsc::channel::<()>();
    *super::super::close::HOLD_BEFORE_DB_CLOSE.lock().unwrap() = Some((dirs[0].clone(), wait));
    rt.block_on(async {
        client(a)
            .await
            .fence(PartitionRequest {
                partition: "p1".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let closing = tokio::spawn(async move { close(a, "p1").await });
        eventually("p1 to be closing", async || {
            local(a, "p1").await.state() == LocalPartitionState::Closed
        })
        .await;
        // Held before its database closes: the file is still locked.
        let lp = local(a, "p1").await;
        assert!(lp.ok, "{lp:?}");
        assert!(lp.error.is_empty(), "{lp:?}");
        assert_eq!(lp.epoch, 1, "{lp:?}");
        release.send(()).unwrap();
        let lp = closing.await.unwrap().unwrap();
        assert_eq!(lp.state(), LocalPartitionState::Closed, "{lp:?}");
        assert!(lp.ok && lp.error.is_empty(), "{lp:?}");
        // Closed now, the file's own read stands: a finding shows (cli692's S1). A forced
        // map at epoch 2 for p1, whose file is still at 1.
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
        let lp = local(a, "p1").await;
        assert_eq!(lp.state(), LocalPartitionState::Closed, "{lp:?}");
        assert!(!lp.ok, "{lp:?}");
        assert!(lp.error.contains("epoch 2"), "{lp:?}");
    });
    drop(stops);
    for db in dbs {
        let _ = db.close();
    }
}
