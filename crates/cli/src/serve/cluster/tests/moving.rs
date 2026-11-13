//! `phdb cluster move` stopped after each step, as a killed process would be, then run
//! again: the rerun finishes the move from where it was. Up to the map, it copies and
//! compares the old node's closed set itself, whatever an earlier run left on the new node,
//! and then drops the old node's files. After the map, it says where they are and how to
//! drop them.

use super::*;
use crate::serve::cluster::moving::{STOP, Step};

/// Three nodes: a serves p1, b p2, c nothing. A row of p1's, through b.
struct Cluster {
    rt: tokio::runtime::Runtime,
    ports: [u16; 3],
    dirs: [PathBuf; 3],
    stops: Vec<tokio::sync::oneshot::Sender<()>>,
    dbs: Vec<pigeonhole::Pigeonhole>,
    args: super::super::cli::NodeArgs,
}

impl Cluster {
    fn start() -> Self {
        let t = Self::start_with(1, false);
        t.rt.block_on(put(t.ports[1], "alice", "1"));
        t
    }

    /// The cluster with p1 at `p1_epoch` in the map (its file is at 1), and a's p1 fenced
    /// as a node's start leaves it (`restarted`), else serving. No row.
    fn start_with(p1_epoch: u64, restarted: bool) -> Self {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let listeners: Vec<std::net::TcpListener> = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
        let mut map = the_map(1, &ports);
        map.partitions.retain(|p| p.id != "p3");
        map.partitions[0].epoch = p1_epoch;
        let dirs = [test_dir(), test_dir(), test_dir()];
        let mut stops = Vec::new();
        let mut dbs = Vec::new();
        for ((l, dir), (id, local)) in listeners.into_iter().zip(&dirs).zip([
            ("a", Some("p1")),
            ("b", Some("p2")),
            ("c", None),
        ]) {
            super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
            let limits = crate::serve::server_tests::limits();
            let restart = restarted && id == "a";
            let (stop, db) = data_node_full(&rt, dir, id, local, l, "", limits, None, restart);
            stops.push(stop);
            dbs.extend(db);
        }
        let args = super::super::cli::NodeArgs {
            node: format!("http://127.0.0.1:{}", ports[1]),
            token_file: None,
            ca: None,
        };
        Self {
            rt,
            ports,
            dirs,
            stops,
            dbs,
            args,
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for stop in self.stops.drain(..) {
            let _ = stop.send(());
        }
        for db in self.dbs.drain(..) {
            let _ = db.close();
        }
    }
}

#[test]
fn a_move_stopped_after_any_step_finishes_when_run_again() {
    for step in [
        Step::Closed,
        Step::Copied,
        Step::Stamped,
        Step::Mapped,
        Step::Serving,
    ] {
        let t = Cluster::start();
        let [a, b, _] = t.ports;
        t.rt.block_on(async {
            STOP.lock().unwrap().push((t.args.node.clone(), step));
            let e = within(super::super::moving::run(&t.args, "p1", "c", true, None))
                .await
                .err()
                .unwrap_or_else(|| panic!("{step:?}: not stopped"));
            assert!(
                e.message().contains("stopped after"),
                "{step:?}: {}",
                e.message()
            );

            let r = within(super::super::moving::run(&t.args, "p1", "c", true, None))
                .await
                .unwrap_or_else(|e| panic!("{step:?}: the rerun failed: {}", e.message()));
            let doc = &r.document;
            let a_kept = t.dirs[0].join("p1.phdb").exists();
            match step {
                // Up to the map: the rerun copies (what's left to send, or all of it
                // again once the stamp installed the set), stamps and checks the copy
                // itself, so it drops a's files.
                Step::Closed | Step::Copied | Step::Stamped => {
                    assert_eq!(doc["verified"], true, "{step:?}: {doc:?}");
                    let source = doc["source"].as_str().unwrap();
                    assert!(source.starts_with("moved aside"), "{step:?}: {source}");
                    assert!(!a_kept, "{step:?}: a's files went aside");
                    for f in doc["copied"].as_array().unwrap() {
                        match step {
                            // Every upload was complete and still staged: nothing sent.
                            Step::Copied => assert_eq!(f["sent"], 0, "{step:?}: {f}"),
                            // The earlier stamp installed the set: all of it again.
                            Step::Stamped => assert_eq!(f["sent"], f["bytes"], "{step:?}: {f}"),
                            _ => {}
                        }
                    }
                }
                // After the map: done, and the advice names a's closed copy and its drop.
                Step::Mapped | Step::Serving => {
                    let done = doc["done"].as_str().unwrap();
                    assert!(done.contains("nothing was dropped"), "{step:?}: {done}");
                    assert!(
                        done.contains(&format!(
                            "phdb cluster drop-partition p1 --node http://127.0.0.1:{a}"
                        )),
                        "{step:?}: {done}"
                    );
                    assert!(a_kept, "{step:?}: a's files stay");
                }
            }
            // Whatever the step: c serves p1, with its rows and its history.
            eventually("c to serve p1", async || {
                get_row(b, "alice").await.ok().flatten() == Some(b"1".to_vec())
            })
            .await;
            let m = client(b)
                .await
                .get_map(crate::serve::proto::GetMapRequest {})
                .await
                .unwrap()
                .into_inner();
            let p1 = m.partitions.iter().find(|p| p.id == "p1").unwrap();
            assert_eq!(
                (p1.node.as_str(), p1.epoch, p1.history_epoch),
                ("c", 2, 0),
                "{step:?}"
            );
        });
    }
}

#[test]
fn a_move_s_wait_for_the_new_node_gives_up_at_its_timeout() {
    // c holds no p1 and never will: the wait for it to serve p1 ends at the limit, with the
    // step, c's last answer and where the move leaves p1 (#1784).
    let t = Cluster::start();
    let c = t.ports[2];
    t.rt.block_on(async {
        let at = super::super::moving::At::new(&t.args, "c", &format!("127.0.0.1:{c}"))
            .await
            .unwrap();
        let start = std::time::Instant::now();
        let e = within(super::super::moving::serving(
            &at,
            "p1",
            2,
            Some(Duration::from_secs(3)),
        ))
        .await
        .expect_err("c never serves p1");
        let took = start.elapsed();
        assert!(took >= Duration::from_secs(3), "{took:?}");
        assert!(took < Duration::from_secs(30), "{took:?}");
        assert_eq!(e.status(), crate::ExitStatus::ERROR);
        let m = e.message();
        assert!(
            m.starts_with(
                "gave up waiting for node \"c\" to serve p1 at epoch 2 after 3s (--timeout): \
                 not listed yet. The map already gives p1 to node \"c\""
            ),
            "{m}"
        );
        assert!(
            m.contains(&format!("phdb cluster status --node http://127.0.0.1:{c}")),
            "{m}"
        );
    });
}

#[test]
fn a_node_serving_an_older_epoch_isnt_taken_for_the_move_done() {
    // a serves p1 at epoch 1: a wait for it at epoch 2 goes on, and says so when it gives up
    // (simp724's N1 on #1787).
    let t = Cluster::start();
    let a = t.ports[0];
    t.rt.block_on(async {
        let at = super::super::moving::At::new(&t.args, "a", &format!("127.0.0.1:{a}"))
            .await
            .unwrap();
        let e = within(super::super::moving::serving(
            &at,
            "p1",
            2,
            Some(Duration::from_secs(3)),
        ))
        .await
        .expect_err("a serves epoch 1, not 2");
        assert!(
            e.message()
                .contains("to serve p1 at epoch 2 after 3s (--timeout): serving at epoch 1."),
            "{}",
            e.message()
        );
    });
}

#[test]
fn a_rerun_after_an_unfence_serves_the_writes_made_since_the_first_copy() {
    // audit120's F1: a move dies after its stamp, c holding a copy at epoch 2. The operator
    // backs out (unfence on a, which the map still names), and a takes a write. A later
    // move must not take c's earlier copy for this one's: it copies a's closed set again,
    // so the write is there once c serves p1.
    let t = Cluster::start();
    let [a, b, _] = t.ports;
    t.rt.block_on(async {
        STOP.lock()
            .unwrap()
            .push((t.args.node.clone(), Step::Stamped));
        let e = within(super::super::moving::run(&t.args, "p1", "c", false, None))
            .await
            .err()
            .expect("stopped after the stamp");
        assert!(e.message().contains("stopped after"), "{}", e.message());

        client(a)
            .await
            .unfence(crate::serve::proto::PartitionRequest {
                partition: "p1".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        put(b, "bob", "2").await;

        let r = within(super::super::moving::run(&t.args, "p1", "c", false, None))
            .await
            .unwrap_or_else(|e| panic!("the rerun failed: {}", e.message()));
        assert_eq!(r.document["node"], "c", "{:?}", r.document);
        assert_eq!(
            get_row(b, "bob").await.unwrap(),
            Some(b"2".to_vec()),
            "the write a acknowledged between the runs"
        );
        assert_eq!(r.document["verified"], true, "{:?}", r.document);
        assert_eq!(get_row(b, "alice").await.unwrap(), Some(b"1".to_vec()));
    });
}

#[test]
fn a_partition_moved_back_to_a_node_that_held_it_takes_writes_there() {
    // audit120's F2: the move away fenced p1 on a, in a's fenced.json. Moved back, p1 is a
    // new copy at a new epoch: that fence isn't its own, and it serves writes once a peer's
    // map confirms it, with no unfence by hand.
    let t = Cluster::start();
    let [a, b, _] = t.ports;
    let wait = Some(Duration::from_secs(20));
    t.rt.block_on(async {
        within(super::super::moving::run(&t.args, "p1", "c", false, wait))
            .await
            .unwrap_or_else(|e| panic!("the move to c failed: {}", e.message()));
        put(b, "bob", "2").await;
        let r = within(super::super::moving::run(&t.args, "p1", "a", false, wait))
            .await
            .unwrap_or_else(|e| panic!("the move back to a failed: {}", e.message()));
        assert_eq!(r.document["epoch"], 3, "{:?}", r.document);
        put(b, "carol", "3").await;
        for (row, value) in [("alice", "1"), ("bob", "2"), ("carol", "3")] {
            assert_eq!(
                get_row(a, row).await.unwrap(),
                Some(value.as_bytes().to_vec()),
                "{row}"
            );
        }
        assert!(
            !super::super::fence::read_fenced(&t.dirs[0])
                .unwrap()
                .contains("p1"),
            "the earlier move's fence went with its copy"
        );
    });
}

#[test]
fn a_move_persists_its_fence_on_a_partition_fenced_since_its_node_started() {
    // audit120's F3: p1 is fenced on a since a started, which nothing persists (here no
    // peer's map confirms it: the map has p1 at epoch 2, a's file at 1). The move fences
    // it itself, so a restart of a mid-move can't lift the fence on a peer's map, which
    // names a until the move's last step.
    let t = Cluster::start_with(2, true);
    t.rt.block_on(async {
        // The move ends at the close's report of that epoch; its fence came first.
        let e = within(super::super::moving::run(&t.args, "p1", "c", false, None))
            .await
            .err()
            .expect("the close reports the file's epoch");
        assert!(
            e.message().contains("the map has it at epoch 2"),
            "{}",
            e.message()
        );
        assert_eq!(
            super::super::fence::read_fenced(&t.dirs[0]).unwrap(),
            ["p1".to_owned()].into()
        );
    });
}

#[test]
fn a_close_by_hand_persists_the_fence_of_a_partition_fenced_since_its_node_started() {
    // simp724's S1 on #1800: p1, fenced on a since a started, is closed by hand, so a move
    // finds it closed and fences nothing itself. The close persisted the fence: a restart
    // of a mid-move reopens p1 under it, and no peer's map lifts it.
    let t = Cluster::start_with(2, true);
    let a = t.ports[0];
    t.rt.block_on(async {
        client(a)
            .await
            .close(crate::serve::proto::PartitionRequest {
                partition: "p1".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            super::super::fence::read_fenced(&t.dirs[0]).unwrap(),
            ["p1".to_owned()].into()
        );
    });
}

/// audit120's S-C5: a relocation that leaves the partition open on its old node (fenced,
/// not closed) must not leave a live subscription following that copy. The old node's
/// component ends with `WrongPartition` once it takes the map, and the subscription opens
/// the partition where the map puts it, from its position, waiting for the new node to
/// finish opening it (held here) rather than ending with `PartitionUnavailable`. Before
/// the fix the subscriptions went on with progress events from the old copy, which takes
/// no more writes.
///
/// c's copy is of a's files as the move closed them; a then opened p1 again, a session the
/// copy doesn't hold, and took a write in it (the stale copy `relocate --copy` trusts). So a
/// subscription whose position is still before that session carries on at c, and one whose
/// position is in it ends with `FeedExpired`.
#[test]
fn a_relocation_that_leaves_the_old_copy_open_moves_its_subscriptions_along() {
    use crate::serve::proto::feed_client::FeedClient;
    use crate::serve::proto::{FeedEvent, PartitionRequest, SubscribeRequest, feed_event::Event};
    let t = Cluster::start();
    let [a, b, c] = t.ports;
    let p1 = || PartitionRequest {
        partition: "p1".into(),
        ..Default::default()
    };
    let subscribe = async |allow_partial: bool, prefix: &[u8]| {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{c}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        FeedClient::new(channel)
            .subscribe(SubscribeRequest {
                tables: vec!["users".into()],
                allow_partial,
                prefix: prefix.to_vec().into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
    };
    // The commit of `row`, or the error that ended the stream.
    let commit_of = async |s: &mut tonic::Streaming<FeedEvent>, row: &str| loop {
        let e = match s.message().await {
            Ok(e) => e.expect("the stream ended without an error"),
            Err(e) => return Err(e),
        };
        if let Some(Event::Commit(c)) = &e.event
            && c.changes.iter().any(|ch| ch.row.as_ref() == row.as_bytes())
        {
            return Ok(());
        }
    };
    let ten = Duration::from_secs(10);
    t.rt.block_on(async {
        // Before the move: a subscription that sees a commit of p1's on a. With
        // `allow_partial`, the move's close of p1 on a leaves it waiting, not ended. Rows
        // under "a" only, so a's later write in its new session isn't delivered to it, and
        // its position stays before that session (a progress event would move it, after the
        // 10 s quiet interval, longer than this test).
        let mut before = subscribe(true, b"a").await;
        put(b, "amy", "1").await;
        tokio::time::timeout(ten, commit_of(&mut before, "amy"))
            .await
            .expect("amy's commit arrives")
            .unwrap();

        // c holds a checked copy of p1 at the next epoch: a move that stopped after its
        // stamp. Then p1 opens on a again (a new session), takes a write, and is fenced
        // there, open.
        STOP.lock()
            .unwrap()
            .push((t.args.node.clone(), Step::Stamped));
        let stopped = within(super::super::moving::run(&t.args, "p1", "c", false, None)).await;
        assert!(stopped.is_err(), "the move stops after its stamp");
        client(a).await.unfence(p1()).await.unwrap();
        // One that starts now, and sees the write in a's new session.
        let mut after = subscribe(false, b"").await;
        put(b, "bob", "1").await;
        tokio::time::timeout(ten, commit_of(&mut after, "bob"))
            .await
            .expect("bob's commit arrives")
            .unwrap();
        client(a).await.fence(p1()).await.unwrap();

        // The map moves p1 to c, keeping its history; a never closed it. c's open of p1 is
        // held a while, so the subscriptions find it opening.
        let (release, wait) = std::sync::mpsc::channel::<()>();
        *super::super::open::HOLD_OPEN.lock().unwrap() = Some((t.dirs[2].clone(), wait));
        within(super::super::cli::relocate(&t.args, "p1", "c", false, true))
            .await
            .unwrap();
        let status = client(a)
            .await
            .status(StatusRequest {})
            .await
            .unwrap()
            .into_inner();
        assert!(
            status.partitions.iter().any(|lp| lp.id == "p1"),
            "p1 stays open on a: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
        release.send(()).unwrap();

        // p1's next write, on c.
        eventually("c takes p1's writes", async || {
            data(b)
                .await
                .mutate(crate::serve::proto::MutateRequest {
                    writes: Some(put_row("ann", "2")),
                    durability: 0,
                })
                .await
                .is_ok()
        })
        .await;
        // The one from before carries on at c.
        tokio::time::timeout(Duration::from_secs(20), commit_of(&mut before, "ann"))
            .await
            .expect("the subscription from before still follows p1's old copy on a")
            .expect("the subscription from before carries on at c");
        // The one in a's new session ends: c's copy doesn't hold it. Not with
        // `PartitionUnavailable` from c's open, which it waited out.
        let ended = tokio::time::timeout(Duration::from_secs(20), commit_of(&mut after, "ann"))
            .await
            .expect("the subscription from a's new session still follows p1's old copy on a")
            .expect_err("its position is past c's copy");
        assert_eq!(code(&ended), "FeedExpired", "{ended}");
    });
}
