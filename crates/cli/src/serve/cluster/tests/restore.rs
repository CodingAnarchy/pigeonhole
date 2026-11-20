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
        partition: None,
        to: None,
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
                    .status(StatusRequest::default())
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
                map: Some(edited.clone()),
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
    // So is one with an index the backup's map doesn't place (#1722): its reconcile would
    // make it again, empty.
    let mut m =
        super::super::map::parse_json(&std::fs::read_to_string(out.join("map.json")).unwrap())
            .unwrap();
    m.indexes.push(crate::serve::proto::IndexPlacement {
        name: "v".into(),
        follows: m.tables[0].name.clone(),
        ..Default::default()
    });
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
        e.message().contains("indexes aren't the backup's"),
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
        FAIL_AT.lock().unwrap().push((out.clone(), "stamped"));
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
        FAIL_AT.lock().unwrap().push((out.clone(), "mapped"));
        let e = restore(&args(a), &restore_args(&out)).await.err().unwrap();
        assert!(e.message().contains("after mapped"), "{}", e.message());
        // A map set answers once a majority holds it: c may take it a push later (#2015,
        // a Windows run read c's map before it had).
        eventually("c to hold the restore's map, at epoch 3", async || {
            epochs(&the_live_map(c).await) == [3, 3, 3]
        })
        .await;
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
                .status(StatusRequest::default())
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

/// `--partition P`'s arguments for [`restore`] (#1832).
fn part_args(dir: &Path, p: &str, to: Option<&str>) -> RestoreArgs {
    RestoreArgs {
        partition: Some(p.into()),
        to: to.map(Into::into),
        ..restore_args(dir)
    }
}

/// `call` on partition `p` at `port`.
async fn on(port: u16, p: &str, call: &str) -> Result<(), tonic::Status> {
    let req = PartitionRequest {
        partition: p.into(),
        ..Default::default()
    };
    let mut c = client(port).await;
    match call {
        "fence" => c.fence(req).await.map(|_| ()),
        "close" => c.close(req).await.map(|_| ()),
        "unfence" => c.unfence(req).await.map(|_| ()),
        _ => unreachable!(),
    }
}

/// Whether `p` serves on the node at `port`.
async fn serves(port: u16, p: &str) -> bool {
    client(port)
        .await
        .status(StatusRequest::default())
        .await
        .unwrap()
        .into_inner()
        .partitions
        .iter()
        .any(|lp| lp.id == p && lp.state() == LocalPartitionState::Serving)
}

async fn put_row(port: u16, row: &str, tag: u64) {
    data(port)
        .await
        .mutate(MutateRequest {
            writes: Some(cells_row(row.as_bytes(), &[3], tag)),
            durability: 0,
        })
        .await
        .unwrap();
}

/// p3 (on c) raised past the backup's epoch by a relocation in place, and serving again.
async fn relocate_p3_in_place(c: u16) {
    on(c, "p3", "fence").await.unwrap();
    on(c, "p3", "close").await.unwrap();
    super::super::cli::relocate(&args(c), "p3", "c", false, false)
        .await
        .unwrap();
    for _ in 0..100 {
        if on(c, "p3", "unfence").await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    eventually("p3 to serve again after its relocation", async || {
        serves(c, "p3").await
    })
    .await;
}

/// #1832: one partition put back from a backup into the live cluster, after its epoch moved
/// past the backup's: its rows are the backup's, the other partitions keep their later
/// writes, and it serves at a new epoch with a new history. Refused while it serves, or is
/// open where it goes.
#[test]
fn a_partition_restored_alone_holds_the_backups_rows_and_the_others_keep_theirs() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes(&rt);
    let (a, c) = (ports[0], ports[2]);
    let mut rows = load(&rt, a, 30);
    rows.sort();
    let out = test_dir().join("backup");
    rt.block_on(backup(&args(c), &out, false, 2)).unwrap();
    rt.block_on(async {
        // After the backup: a write to p1 (kept) and to p3 (lost), and p3 relocated in place
        // to epoch 2, past the backup's 1, then written again (lost).
        put_row(a, "b-later", 100).await;
        put_row(a, "y-later", 101).await;
        relocate_p3_in_place(c).await;
        put_row(a, "z-later", 102).await;
        let before = the_live_map(c).await;
        assert_eq!(epochs(&before), [1, 1, 2]);

        // Serving: refused.
        let e = restore(&args(a), &part_args(&out, "p3", None))
            .await
            .err()
            .unwrap();
        assert!(
            e.message().contains("fence it there first"),
            "{}",
            e.message()
        );
        // Fenced, open on c, where it goes: refused.
        on(c, "p3", "fence").await.unwrap();
        let e = restore(&args(a), &part_args(&out, "p3", None))
            .await
            .err()
            .unwrap();
        assert!(
            e.message().contains("close it there first"),
            "{}",
            e.message()
        );
        // Closed: restored, at epoch 3 (past the live map's 2).
        on(c, "p3", "close").await.unwrap();
        let report = restore(&args(a), &part_args(&out, "p3", None))
            .await
            .unwrap();
        assert_eq!(report.document["epoch"], 3, "{:?}", report.document);
        assert_eq!(report.document["backup_epoch"], 1, "{:?}", report.document);
        assert_eq!(report.document["node"], "c", "{:?}", report.document);
        assert!(!out.join("restore-progress-p3.json").exists());
        eventually("p3 to serve", async || serves(c, "p3").await).await;

        // The backup's rows, p1's later write kept, p3's later writes gone.
        let mut want = rows.clone();
        want.push(b"b-later".to_vec());
        want.sort();
        assert_eq!(keys(c).await, want);
        // p3 alone moved: a new epoch and history; p1 and p2 and the tables as they were.
        let m = the_live_map(c).await;
        assert!(m.map_version > before.map_version);
        assert_eq!(epochs(&m), [1, 1, 3]);
        let p3 = m.partitions.iter().find(|p| p.id == "p3").unwrap();
        assert_eq!((p3.node.as_str(), p3.history_epoch), ("c", 3));
        assert_eq!(m.tables, before.tables);
        // It takes writes.
        put_row(a, "y-after", 200).await;
        assert!(keys(c).await.contains(&b"y-after".to_vec()));
    });
}

/// #1832: `--to` another node, with p3's node stopped (`--old-unreachable`).
#[test]
fn a_partition_restored_alone_to_another_node_while_its_node_is_down() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (mut stops, ports, _dbs) = three_nodes(&rt);
    let (a, b) = (ports[0], ports[1]);
    let mut rows = load(&rt, a, 30);
    rows.sort();
    let out = test_dir().join("backup");
    rt.block_on(backup(&args(a), &out, false, 2)).unwrap();
    rt.block_on(async {
        put_row(a, "y-later", 100).await;
        // c's process stops: its p3 can't be fenced or closed.
        let _ = stops.remove(2).send(());
        // Until c refuses, not a fixed sleep (#2105).
        stopped(ports[2]).await;
        let e = restore(&args(a), &part_args(&out, "p3", Some("b")))
            .await
            .err()
            .unwrap();
        assert!(e.message().contains("--old-unreachable"), "{}", e.message());
        let report = restore(
            &args(a),
            &RestoreArgs {
                old_unreachable: true,
                ..part_args(&out, "p3", Some("b"))
            },
        )
        .await
        .unwrap();
        assert_eq!(report.document["node"], "b", "{:?}", report.document);
        assert_eq!(report.document["epoch"], 2, "{:?}", report.document);
        assert_eq!(
            report.document["close_and_drop_on"][0]["node"], "c",
            "{:?}",
            report.document
        );
        eventually("p3 to serve on b", async || serves(b, "p3").await).await;
        assert_eq!(keys(a).await, rows);
        let m = the_live_map(a).await;
        let p3 = m.partitions.iter().find(|p| p.id == "p3").unwrap();
        assert_eq!((p3.node.as_str(), p3.epoch), ("b", 2));
    });
}

/// #1832: refused for a partition the backup lacks, with `--map`, and once p3's range of a
/// table changed since the backup; a split moved elsewhere in the table isn't a change of
/// p3's range (it gets as far as the serving check).
#[test]
fn a_partition_restore_is_refused_where_it_cant_be_put_back_alone() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes(&rt);
    let (a, c) = (ports[0], ports[2]);
    load(&rt, a, 6);
    let out = test_dir().join("backup");
    rt.block_on(backup(&args(c), &out, false, 2)).unwrap();
    rt.block_on(async {
        let e = restore(&args(a), &part_args(&out, "p9", None))
            .await
            .err()
            .unwrap();
        assert!(
            e.message().contains("no partition \"p9\""),
            "{}",
            e.message()
        );
        let e = restore(
            &args(a),
            &RestoreArgs {
                map: Some(out.join("map.json")),
                ..part_args(&out, "p3", None)
            },
        )
        .await
        .err()
        .unwrap();
        assert!(e.message().contains("--map"), "{}", e.message());

        // Forced maps reshape `users`: first g to h (p1 and p2's bound), then m to n (p3's).
        let reshape = async |at: &str, key: &str| {
            let mut m = the_live_map(c).await;
            m.map_version += 1;
            let i = m.tables[0]
                .splits
                .iter()
                .position(|k| *k == text(at))
                .unwrap();
            m.tables[0].splits[i] = text(key);
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
        };
        reshape("g", "h").await;
        let e = restore(&args(a), &part_args(&out, "p3", None))
            .await
            .err()
            .unwrap();
        assert!(
            e.message().contains("fence it there first"),
            "{}",
            e.message()
        );
        reshape("m", "n").await;
        let e = restore(&args(a), &part_args(&out, "p3", None))
            .await
            .err()
            .unwrap();
        assert!(
            e.message().contains("key range of table \"users\" changed"),
            "{}",
            e.message()
        );
    });
}

/// #1832, simp724's R2: a run stopped after its stamp, run again, reuses the stamp (no
/// upload); one stopped there whose map then moved on (a concurrent change, here a forced
/// map raising p3's epoch) stamps again past it; one stopped after its map only waits and
/// unfences. The rows are the backup's at each step.
#[test]
fn a_partition_restore_run_again_reuses_its_stamp_only_while_it_fits() {
    use crate::serve::cluster::cli::RESTORE_STOP_BEFORE_MAP;
    use crate::serve::cluster::restore::FAIL_AT;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes(&rt);
    let (a, c) = (ports[0], ports[2]);
    let mut rows = load(&rt, a, 12);
    rows.sort();
    let out = test_dir().join("backup");
    rt.block_on(backup(&args(c), &out, false, 2)).unwrap();
    let progress = out.join("restore-progress-p3.json");
    rt.block_on(async {
        on(c, "p3", "fence").await.unwrap();
        on(c, "p3", "close").await.unwrap();
        // Stopped after the stamp at 2, before the map; run again: the stamp is reused.
        *RESTORE_STOP_BEFORE_MAP.lock().unwrap() = Some(args(a).node);
        let e = restore(&args(a), &part_args(&out, "p3", None))
            .await
            .err()
            .unwrap();
        assert!(e.message().contains("the test's fault"), "{}", e.message());
        assert!(progress.exists());
        let report = restore(&args(a), &part_args(&out, "p3", None))
            .await
            .unwrap();
        assert_eq!(report.document["epoch"], 2, "{:?}", report.document);
        assert_eq!(report.document["uploaded"], false, "{:?}", report.document);
        eventually("p3 to serve at 2", async || serves(c, "p3").await).await;
        assert_eq!(keys(c).await, rows);

        // Again, stopped after the stamp at 3; then the map moves on (p3 at 5, forced).
        on(c, "p3", "fence").await.unwrap();
        on(c, "p3", "close").await.unwrap();
        *RESTORE_STOP_BEFORE_MAP.lock().unwrap() = Some(args(a).node);
        restore(&args(a), &part_args(&out, "p3", None))
            .await
            .err()
            .unwrap();
        let mut m = the_live_map(c).await;
        m.map_version += 1;
        m.partitions
            .iter_mut()
            .find(|p| p.id == "p3")
            .unwrap()
            .epoch = 5;
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
        // c holds p3's file at 3, which the map's 5 doesn't name: it tries the open once the
        // map lands (`Opening`, which a restore refuses), and leaves it closed.
        eventually("p3 not opening on c", async || {
            client(c)
                .await
                .status(StatusRequest::default())
                .await
                .unwrap()
                .into_inner()
                .partitions
                .iter()
                .any(|lp| {
                    lp.id == "p3"
                        && !matches!(
                            lp.state(),
                            LocalPartitionState::Opening | LocalPartitionState::Serving
                        )
                })
        })
        .await;
        // Run again, stopped after its map: the stamp at 3 doesn't fit, so it stamps at 6.
        FAIL_AT.lock().unwrap().push((out.clone(), "mapped"));
        let e = restore(&args(a), &part_args(&out, "p3", None))
            .await
            .err()
            .unwrap();
        assert!(
            e.message().contains("the test's fault after mapped"),
            "{}",
            e.message()
        );
        let m = the_live_map(c).await;
        assert_eq!(epochs(&m), [1, 1, 6]);
        // And again: only the waits and the unfence are left.
        let report = restore(&args(a), &part_args(&out, "p3", None))
            .await
            .unwrap();
        assert_eq!(report.document["epoch"], 6, "{:?}", report.document);
        assert!(
            report.document.contains_key("resumed"),
            "{:?}",
            report.document
        );
        assert!(!progress.exists());
        eventually("p3 to serve at 6", async || serves(c, "p3").await).await;
        assert_eq!(keys(c).await, rows);
    });
}

/// #1832, simp724's N1: a copy of p3 stamped past the map's epoch, as a relocation that
/// failed after its stamp leaves one (here c's closed p3, stamped from 1 to 2 with no map
/// change). The restore to b passes it, at 3, so no two histories share epoch 2.
#[test]
fn a_partition_restore_passes_every_copys_epoch_not_only_the_maps() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes(&rt);
    let (a, b, c) = (ports[0], ports[1], ports[2]);
    let mut rows = load(&rt, a, 12);
    rows.sort();
    let out = test_dir().join("backup");
    rt.block_on(backup(&args(c), &out, false, 2)).unwrap();
    rt.block_on(async {
        on(c, "p3", "fence").await.unwrap();
        on(c, "p3", "close").await.unwrap();
        let m = the_live_map(c).await;
        client(c)
            .await
            .stamp(super::super::proto::StampRequest {
                cluster_id: m.cluster_id.clone(),
                partition: "p3".into(),
                from_epoch: 1,
                to_epoch: 2,
                restore: false,
                restore_from_cluster_id: String::new(),
                promote: false,
                replica_epoch: 0,
                create: false,
            })
            .await
            .unwrap();
        assert_eq!(epochs(&the_live_map(c).await), [1, 1, 1]);
        let report = restore(&args(a), &part_args(&out, "p3", Some("b")))
            .await
            .unwrap();
        assert_eq!(report.document["epoch"], 3, "{:?}", report.document);
        eventually("p3 to serve on b", async || serves(b, "p3").await).await;
        assert_eq!(keys(a).await, rows);
    });
}
