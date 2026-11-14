//! `phdb cluster restore` (D317 ticket 8b, §9): a backup put back holds the backup's rows and
//! not the writes since, each partition at a new epoch on its node, the map's tables exactly
//! as the backup had them; it's refused while a partition serves or is open where it goes;
//! and a feed token from before it answers `FeedExpired`.

use prost::bytes::Bytes;

use super::backup::{args, load};
use super::*;
use crate::serve::cluster::backup::backup;
use crate::serve::cluster::restore::{RestoreArgs, restore};
use crate::serve::proto::{
    LocalPartitionState, MutateRequest, PartitionRequest, SubscribeRequest,
    feed_client::FeedClient, feed_event::Event, subscribe_request::Start,
};

const NODES: [(&str, &str); 3] = [("a", "p1"), ("b", "p2"), ("c", "p3")];

fn restore_args(dir: &Path) -> RestoreArgs {
    RestoreArgs {
        dir: dir.to_path_buf(),
        map: None,
        cluster_id: None,
        old_unreachable: false,
        parallel: 2,
    }
}

/// A token of a subscription to `users` through `port`, once it has delivered a commit.
async fn a_token(port: u16) -> Bytes {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut s = FeedClient::new(channel.clone())
        .subscribe(SubscribeRequest {
            tables: vec!["users".into()],
            start: Some(Start::FromOldest(true)),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    loop {
        let e = tokio::time::timeout(Duration::from_secs(10), s.message())
            .await
            .expect("a commit within 10 s")
            .unwrap()
            .expect("the stream ended");
        if matches!(e.event, Some(Event::Commit(_))) && !e.token.is_empty() {
            return e.token;
        }
    }
}

async fn on_each(ports: &[u16], call: &str) -> Vec<Result<(), tonic::Status>> {
    let mut out = Vec::new();
    for (port, (_, p)) in ports.iter().zip(NODES) {
        let req = PartitionRequest {
            partition: p.into(),
            ..Default::default()
        };
        let mut c = client(*port).await;
        let r = match call {
            "fence" => c.fence(req).await.map(|_| ()),
            "close" => c.close(req).await.map(|_| ()),
            _ => unreachable!(),
        };
        out.push(r);
    }
    out
}

async fn keys(port: u16) -> Vec<Vec<u8>> {
    scan_pages(
        port,
        super::super::proto::ScanRequest {
            table: "users".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .iter()
    .flat_map(|p| p.rows.iter().map(|r| r.key.to_vec()))
    .collect()
}

#[test]
fn a_restore_puts_the_backup_back_at_new_epochs_and_old_tokens_expire() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes(&rt);
    let (a, c) = (ports[0], ports[2]);
    let mut rows = load(&rt, a, 30);
    rows.sort();
    let token = rt.block_on(a_token(a));
    let out = test_dir().join("backup");
    rt.block_on(backup(&args(c), &out, false, 2)).unwrap();
    let backed_up =
        super::super::map::parse_json(&std::fs::read_to_string(out.join("map.json")).unwrap())
            .unwrap();
    rt.block_on(async {
        // Writes after the backup: the restore loses them.
        for (i, r) in ["b-later", "n-later", "y-later"].iter().enumerate() {
            data(a)
                .await
                .mutate(MutateRequest {
                    writes: Some(cells_row(r.as_bytes(), &[3], 100 + i as u64)),
                    durability: 0,
                })
                .await
                .unwrap();
        }
        assert_eq!(keys(c).await.len(), rows.len() + 3);

        // Serving: refused, nothing changed.
        let e = restore(&args(a), &restore_args(&out)).await.err().unwrap();
        assert!(
            e.message().contains("fence it there first"),
            "{}",
            e.message()
        );
        // Fenced, but open where each is restored to (its own node): refused.
        assert!(on_each(&ports, "fence").await.iter().all(Result::is_ok));
        let e = restore(&args(a), &restore_args(&out)).await.err().unwrap();
        assert!(
            e.message().contains("close it there first"),
            "{}",
            e.message()
        );
        // Closed: restored.
        assert!(on_each(&ports, "close").await.iter().all(Result::is_ok));
        let report = restore(&args(a), &restore_args(&out)).await.unwrap();
        assert_eq!(report.document["partitions"], 3, "{:?}", report.document);
        assert_eq!(report.document["missed"], 0, "{:?}", report.document);

        for (port, (_, p)) in ports.iter().zip(NODES) {
            eventually(&format!("{p} to serve again"), async || {
                client(*port)
                    .await
                    .status(StatusRequest {})
                    .await
                    .unwrap()
                    .into_inner()
                    .partitions
                    .iter()
                    .any(|lp| lp.id == p && lp.state() == LocalPartitionState::Serving)
            })
            .await;
        }
        // The backup's rows, and none written since.
        assert_eq!(keys(c).await, rows);
        // Each partition at epoch 2 in a newer map, its tables exactly the backup's (their
        // `created` kept: another would make each node drop the table and make it again).
        let m = client(c)
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        assert!(m.map_version > backed_up.map_version, "{}", m.map_version);
        assert!(
            m.partitions.iter().all(|p| p.epoch == 2),
            "{:?}",
            m.partitions
        );
        assert_eq!(m.tables, backed_up.tables);
        // The restored cluster takes writes.
        data(a)
            .await
            .mutate(MutateRequest {
                writes: Some(cells_row(b"after", &[3], 200)),
                durability: 0,
            })
            .await
            .unwrap();
        assert!(keys(c).await.contains(&b"after".to_vec()));
        // A token from before the restore: expired.
        let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{a}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let e = FeedClient::new(channel)
            .subscribe(SubscribeRequest {
                tables: vec!["users".into()],
                start: Some(Start::Resume(token)),
                ..Default::default()
            })
            .await
            .expect_err("an old token is refused");
        assert_eq!(code(&e), "FeedExpired", "{e}");
    });
}

#[test]
fn a_restore_refuses_a_copy_that_isnt_the_manifests() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes(&rt);
    load(&rt, ports[0], 6);
    let out = test_dir().join("backup");
    rt.block_on(backup(&args(ports[0]), &out, false, 2))
        .unwrap();
    // One byte of p2's copy changed: refused before any node is asked.
    let p2 = out.join("p2.phdb");
    let mut bytes = std::fs::read(&p2).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    std::fs::write(&p2, bytes).unwrap();
    let e = rt
        .block_on(restore(&args(ports[0]), &restore_args(&out)))
        .err()
        .unwrap();
    assert!(
        e.message()
            .contains("isn't the manifest's copy of partition p2"),
        "{}",
        e.message()
    );
    // A map with a partition the backup hasn't: refused.
    let mut m =
        super::super::map::parse_json(&std::fs::read_to_string(out.join("map.json")).unwrap())
            .unwrap();
    let mut p4 = m.partitions[0].clone();
    p4.id = "p4".into();
    m.partitions.push(p4);
    let edited = out.join("edited.json");
    std::fs::write(&edited, super::super::map::canonical_json(&m)).unwrap();
    let e = rt
        .block_on(restore(
            &args(ports[0]),
            &RestoreArgs {
                map: Some(edited.clone()),
                ..restore_args(&out)
            },
        ))
        .err()
        .unwrap();
    assert!(
        e.message().contains("partitions") && e.message().contains("aren't the backup's"),
        "{}",
        e.message()
    );
    // A map whose table differs (another `created`): refused, as it would drop the table's data.
    let mut m =
        super::super::map::parse_json(&std::fs::read_to_string(out.join("map.json")).unwrap())
            .unwrap();
    m.tables[0].created += 1;
    std::fs::write(&edited, super::super::map::canonical_json(&m)).unwrap();
    let e = rt
        .block_on(restore(
            &args(ports[0]),
            &RestoreArgs {
                map: Some(edited),
                ..restore_args(&out)
            },
        ))
        .err()
        .unwrap();
    assert!(
        e.message().contains("tables aren't the backup's"),
        "{}",
        e.message()
    );
}

fn epochs(m: &ClusterMap) -> Vec<u64> {
    m.partitions.iter().map(|p| p.epoch).collect()
}

async fn the_live_map(port: u16) -> ClusterMap {
    client(port)
        .await
        .get_map(GetMapRequest {})
        .await
        .unwrap()
        .into_inner()
}

/// simp724's S1 on #1764: a restore that failed after its stamps, run again, neither
/// uploads nor installs again; one that failed after its map, run again, only unfences.
#[test]
fn a_restore_run_again_skips_what_an_earlier_run_did() {
    use crate::serve::cluster::restore::FAIL_AT;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes(&rt);
    let (a, c) = (ports[0], ports[2]);
    let mut rows = load(&rt, a, 12);
    rows.sort();
    let out = test_dir().join("backup");
    rt.block_on(backup(&args(c), &out, false, 2)).unwrap();
    let progress = out.join("restore-progress.json");
    rt.block_on(async {
        // Stopped after every stamp, before the map.
        assert!(on_each(&ports, "fence").await.iter().all(Result::is_ok));
        assert!(on_each(&ports, "close").await.iter().all(Result::is_ok));
        *FAIL_AT.lock().unwrap() = Some((out.clone(), "stamped"));
        let e = restore(&args(a), &restore_args(&out)).await.err().unwrap();
        assert!(e.message().contains("after stamped"), "{}", e.message());
        assert!(progress.exists());
        assert_eq!(epochs(&the_live_map(c).await), [1, 1, 1]);
        // Again: nothing uploaded or installed a second time; the map, then served.
        let report = restore(&args(a), &restore_args(&out)).await.unwrap();
        assert_eq!(
            report.document["uploaded_bytes"], 0,
            "{:?}",
            report.document
        );
        for r in report.document["restored"].as_array().unwrap() {
            assert_eq!(r["installed"], "by an earlier run", "{r}");
            assert_eq!(r["epoch"], 2, "{r}");
        }
        assert!(
            !progress.exists(),
            "a finished restore leaves no progress file"
        );
        assert_eq!(epochs(&the_live_map(c).await), [2, 2, 2]);
        assert_eq!(keys(c).await, rows);

        // Again from the same backup, stopped after the map: the partitions are open on
        // their nodes, fenced. Run again, it only unfences: no epoch 4.
        assert!(on_each(&ports, "fence").await.iter().all(Result::is_ok));
        assert!(on_each(&ports, "close").await.iter().all(Result::is_ok));
        *FAIL_AT.lock().unwrap() = Some((out.clone(), "mapped"));
        let e = restore(&args(a), &restore_args(&out)).await.err().unwrap();
        assert!(e.message().contains("after mapped"), "{}", e.message());
        assert_eq!(epochs(&the_live_map(c).await), [3, 3, 3]);
        let report = restore(&args(a), &restore_args(&out)).await.unwrap();
        assert_eq!(
            report.document["resumed"], "the map was set by an earlier run",
            "{:?}",
            report.document
        );
        assert_eq!(epochs(&the_live_map(c).await), [3, 3, 3]);
        assert!(!progress.exists());
        for (port, (_, p)) in ports.iter().zip(NODES) {
            let serving = client(*port)
                .await
                .status(StatusRequest {})
                .await
                .unwrap()
                .into_inner()
                .partitions
                .iter()
                .any(|lp| lp.id == p && lp.state() == LocalPartitionState::Serving);
            assert!(serving, "{p} serves");
        }
        assert_eq!(keys(c).await, rows);
    });
}

/// simp724's S3 on #1764: a backup whose directory takes no writes (a read-only snapshot)
/// still restores, with a warning that a run again would redo it.
#[test]
fn a_restore_from_a_backup_it_cant_write_to_warns_and_goes_on() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes(&rt);
    let (a, c) = (ports[0], ports[2]);
    let mut rows = load(&rt, a, 6);
    rows.sort();
    let out = test_dir().join("backup");
    rt.block_on(backup(&args(c), &out, false, 2)).unwrap();
    // Every progress write fails, as on a read-only directory (and as root too).
    std::fs::create_dir(out.join("restore-progress.json.tmp")).unwrap();
    rt.block_on(async {
        assert!(on_each(&ports, "fence").await.iter().all(Result::is_ok));
        assert!(on_each(&ports, "close").await.iter().all(Result::is_ok));
        let report = restore(&args(a), &restore_args(&out)).await.unwrap();
        let warning = report.document["warning"].as_str().unwrap_or_default();
        assert!(
            warning.contains("redoes the whole restore"),
            "{:?}",
            report.document
        );
        assert!(!out.join("restore-progress.json").exists());
        assert_eq!(epochs(&the_live_map(c).await), [2, 2, 2]);
        assert_eq!(keys(c).await, rows);
    });
}

/// audit120's B1 on #1872, a restore's half: b's install of p2's restored set stopped after
/// its first rename, its marker left. Run again, the restore uploads nothing to b (b says the
/// install is in progress) and goes on to the stamp, which finishes it.
#[test]
fn a_restore_run_again_over_an_interrupted_install_finishes_it() {
    use crate::serve::cluster::stamp::FAIL_AFTER_FIRST_RENAME;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dirs = [test_dir(), test_dir(), test_dir()];
    let (_stops, ports, _dbs) = three_nodes_in(&rt, &dirs);
    let (a, c) = (ports[0], ports[2]);
    let mut rows = load(&rt, a, 12);
    rows.sort();
    let out = test_dir().join("backup");
    rt.block_on(backup(&args(c), &out, false, 2)).unwrap();
    let marker = dirs[1].join(".phdb-incoming/p2.installing");
    rt.block_on(async {
        assert!(on_each(&ports, "fence").await.iter().all(Result::is_ok));
        assert!(on_each(&ports, "close").await.iter().all(Result::is_ok));
        FAIL_AFTER_FIRST_RENAME
            .lock()
            .unwrap()
            .push(dirs[1].clone());
        let e = restore(&args(a), &restore_args(&out)).await.err().unwrap();
        assert!(
            e.message().contains("after the first rename"),
            "{}",
            e.message()
        );
        assert!(marker.exists(), "b's install's marker is left");

        let report = restore(&args(a), &restore_args(&out))
            .await
            .unwrap_or_else(|e| panic!("the rerun failed: {}", e.message()));
        assert!(!marker.exists(), "the stamp finished b's install");
        assert_eq!(
            epochs(&the_live_map(c).await),
            [2, 2, 2],
            "{:?}",
            report.document
        );
        assert_eq!(keys(c).await, rows);
    });
}
