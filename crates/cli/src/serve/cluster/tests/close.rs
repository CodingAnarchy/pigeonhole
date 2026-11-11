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
        // Unfence reopens it, and its rows are there.
        let lp = client(a)
            .await
            .unfence(PartitionRequest {
                partition: "p1".into(),
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
