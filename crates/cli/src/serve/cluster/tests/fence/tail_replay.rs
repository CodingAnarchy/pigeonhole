//! D336 PR 3a: an old owner's tail, replayed into its partition's new owner. c owns p3 with
//! b's copy following it; b's copy stops applying (the `HOLD_APPLIES` seam, still hearing
//! c), so what c commits then is c's alone; c is cut off and failed over to b, and the
//! event records c's tail PENDING. Once c is back, `phdb cluster replay-tail p3` replays
//! it on b from c's file (`Cluster.ReadTail`) and sets the tail DONE.

use super::*;
use crate::serve::proto::{
    CreateTableRequest, DropTableRequest, FamilySpec, TablePlacementRequest, TailState,
    schema_client::SchemaClient,
};

/// A schema client of the node at `port`.
async fn schema(port: u16) -> SchemaClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    SchemaClient::new(channel)
}

/// Creates table `name` (family `f`) wholly on p3, through the writer at `port`.
async fn create_on_p3(port: u16, name: &str) {
    schema(port)
        .await
        .create_table(CreateTableRequest {
            name: name.into(),
            families: vec![FamilySpec {
                name: "f".into(),
                ..Default::default()
            }],
            if_missing: false,
            placement: Some(TablePlacementRequest {
                group: String::new(),
                splits: Vec::new(),
                partitions: vec!["p3".into()],
            }),
        })
        .await
        .unwrap();
}

/// Drops table `name` through the writer at `port`.
async fn drop_table(port: u16, name: &str) {
    schema(port)
        .await
        .drop_table(DropTableRequest { name: name.into() })
        .await
        .unwrap();
}

/// Puts `value` in `table`'s row `row` (`f:q`) through `port`, retried while a node that
/// just took a map still reconciles it: the commit's seqno.
async fn put_in(port: u16, table: &str, row: &str, value: &str) -> u64 {
    let mut w = put_row(row, value);
    w.row.as_mut().unwrap().table = table.into();
    let mut last = None;
    for _ in 0..100 {
        match data(port)
            .await
            .mutate(crate::serve::proto::MutateRequest {
                replication: 0,
                writes: Some(w.clone()),
                durability: 0,
            })
            .await
        {
            Ok(r) => return r.into_inner().commit.map_or(0, |c| c.seqno),
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("put {table}/{row}: {last:?}");
}

/// `table`'s row `row` (`f:q`) through `port`.
async fn get_in(port: u16, table: &str, row: &str) -> Result<Option<Vec<u8>>, tonic::Status> {
    let reply = data(port)
        .await
        .get(crate::serve::proto::GetRequest {
            table: table.into(),
            row: row.as_bytes().to_vec().into(),
            family: "f".into(),
            qualifier: b"q".to_vec().into(),
            ..Default::default()
        })
        .await?;
    Ok(reply.into_inner().cell.map(|c| c.value.to_vec()))
}

/// p3's partition failover event in `m`.
fn p3_event(m: &crate::serve::proto::ClusterMap) -> Option<crate::serve::proto::FailoverEvent> {
    m.failovers
        .iter()
        .rfind(|e| e.kind() == crate::serve::proto::FailoverKind::Partition && e.partition == "p3")
        .cloned()
}

/// The rig with `tail_replay` adopted, tables `names` made on p3, and b's copy caught up
/// with c and then holding its applies: whatever c commits from here is its tail.
fn tail_rig(names: &[&str]) -> Rig {
    // Every node keeps a feed: a promoted copy too, which the crash tests subscribe to.
    let t = failover_rig_from(Rig::start_feed(16 << 20), true, &["b"]);
    let [a, b, _] = t.ports;
    t.rt.block_on(async {
        eventually("tail_replay to be adopted", async || {
            map_of(a)
                .await
                .adopted_features
                .iter()
                .any(|f| f == crate::serve::cluster::TAIL_REPLAY_FEATURE)
        })
        .await;
        let mut last = 0;
        for name in names {
            create_on_p3(a, name).await;
            last = put_in(a, name, "base", "v0").await;
        }
        // Caught up: b's copy has applied the last of them (with the tables' schema before).
        eventually_within(
            Duration::from_secs(15),
            "b's copy to catch up with c",
            async || {
                replica_row(b, "p3")
                    .await
                    .is_some_and(|lp| lp.replica_applied_seqno >= last)
            },
        )
        .await;
    });
    crate::serve::replica::HOLD_APPLIES
        .lock()
        .unwrap()
        .push(t.dirs[1].clone());
    t
}

/// Cuts c off until p3 fails over to b, with c's tail PENDING; then lifts b's hold, joins c
/// again and closes c's p3 there. Returns the event.
fn fail_over_to_b(t: &Rig) -> crate::serve::proto::FailoverEvent {
    let a = t.ports[0];
    cut(t, 2, true);
    let e = t.rt.block_on(async {
        eventually_within(FAILOVER_WINDOW * 2, "p3 to fail over to b", async || {
            map_of(a)
                .await
                .partitions
                .iter()
                .any(|p| p.id == "p3" && p.node == "b")
        })
        .await;
        p3_event(&map_of(a).await).expect("p3's event")
    });
    crate::serve::replica::HOLD_APPLIES
        .lock()
        .unwrap()
        .retain(|d| *d != t.dirs[1]);
    cut(t, 2, false);
    // c, back, keeps p3 open as a fenced owner until it's closed: its file is the tail's
    // source only once closed (M2), and a replay until then stops on that, naming the close.
    let c = t.ports[2];
    t.rt.block_on(eventually_within(
        Duration::from_secs(20),
        "a replay to stop on c's open p3",
        async || {
            replay_tail(a, false).await.is_ok_and(|r| {
                r["note"]
                    .as_str()
                    .unwrap()
                    .contains("phdb cluster close p3")
            })
        },
    ));
    t.rt.block_on(eventually_within(
        Duration::from_secs(20),
        "c to close its p3",
        async || {
            client(c)
                .await
                .close(crate::serve::proto::PartitionRequest {
                    partition: "p3".into(),
                    ..Default::default()
                })
                .await
                .is_ok()
        },
    ));
    assert_eq!(e.tail_state(), TailState::Pending, "{e:?}");
    assert_eq!(e.from, "c", "{e:?}");
    assert!(!e.fork_token.is_empty(), "{e:?}");
    e
}

/// `phdb cluster replay-tail p3` through a, with `dry_run`: the report's summary.
async fn replay_tail(
    a: u16,
    dry_run: bool,
) -> Result<std::collections::BTreeMap<&'static str, serde_json::Value>, String> {
    let args = super::super::super::cli::NodeArgs {
        own: None,
        node: format!("http://127.0.0.1:{a}"),
        token_file: None,
        ca: None,
    };
    super::super::super::replay_cli::run(
        &args,
        "p3",
        None,
        dry_run,
        false,
        Some(Duration::from_secs(20)),
    )
    .await
    .map(|r| r.summary.into_iter().collect())
    .map_err(|f| format!("{f:?}"))
}

/// Replays p3's tail through a until the command sets it DONE (c's file may take a moment
/// to close once c is back): the last report.
async fn replay_until_done(a: u16) -> std::collections::BTreeMap<&'static str, serde_json::Value> {
    let mut last = None;
    for _ in 0..60 {
        match replay_tail(a, false).await {
            Ok(r) if r["tail_state"] == "DONE" => return r,
            other => last = Some(other),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("the tail never reached DONE: {last:?}");
}

/// The coordinator's test (both ways): a table dropped and made again inside the tail, and
/// one dropped and made again after the failover. The old owner's commits to the first
/// table before its drop are skipped as dropped (the source's pre-scan gives them
/// `created` 0); its commit after the table was made again is replayed; every commit to the
/// second is skipped, since the new owner's table isn't the one they wrote. A plain table's
/// tail commit is replayed; a dry run first applies nothing and leaves the tail PENDING.
#[test]
fn a_tail_replays_into_the_new_owner_and_skips_tables_dropped_inside_or_after_it() {
    let t = tail_rig(&["t0", "inside", "after"]);
    let a = t.ports[0];
    t.rt.block_on(async {
        put_in(a, "t0", "k1", "v1").await;
        put_in(a, "inside", "gone", "x").await;
        drop_table(a, "inside").await;
        create_on_p3(a, "inside").await;
        put_in(a, "inside", "again", "y").await;
        put_in(a, "after", "late", "z").await;
    });
    let e = fail_over_to_b(&t);
    t.rt.block_on(async {
        drop_table(a, "after").await;
        create_on_p3(a, "after").await;
        // Not on b: the tail is c's alone.
        assert_eq!(get_in(a, "t0", "k1").await.unwrap(), None);
        // A dry run judges the tail and changes nothing.
        let mut dry = None;
        for _ in 0..60 {
            match replay_tail(a, true).await {
                Ok(r) if r["running"] == false && r["note"] == "" => {
                    dry = Some(r);
                    break;
                }
                _ => tokio::time::sleep(Duration::from_millis(250)).await,
            }
        }
        let dry = dry.expect("a dry run to finish");
        assert_eq!(dry["tail_state"], "PENDING", "{dry:?}");
        assert!(dry["applied"].as_u64().unwrap() >= 2, "{dry:?}");
        assert_eq!(
            get_in(a, "t0", "k1").await.unwrap(),
            None,
            "a dry run applies nothing"
        );
        let r = replay_until_done(a).await;
        assert_eq!(r["refused"], 0, "{r:?}");
        assert_eq!(get_in(a, "t0", "k1").await.unwrap(), Some(b"v1".to_vec()));
        assert_eq!(
            get_in(a, "inside", "again").await.unwrap(),
            Some(b"y".to_vec())
        );
        assert_eq!(
            get_in(a, "inside", "gone").await.unwrap(),
            None,
            "written before its table's drop in the tail"
        );
        assert_eq!(
            get_in(a, "after", "late").await.unwrap(),
            None,
            "written to a table dropped after the failover"
        );
        assert_eq!(get_in(a, "t0", "base").await.unwrap(), Some(b"v0".to_vec()));
        let done = p3_event(&map_of(a).await).unwrap();
        assert_eq!(
            (done.to_epoch, done.tail_state()),
            (e.to_epoch, TailState::Done),
            "{done:?}"
        );
        assert!(done.replayed_through > 0, "{done:?}");
        assert!(done.fork_token.is_empty(), "{done:?}");
        // The owner's fork row for the event goes once no PENDING tail names it.
        let b_dir = t.dirs[1].clone();
        eventually("b to prune p3's fork row", async || {
            crate::serve::cluster::tail_replay::seams::PRUNED
                .lock()
                .unwrap()
                .iter()
                .any(|(d, p, gone)| *d == b_dir && p == "p3" && gone.contains(&e.to_epoch))
        })
        .await;
        // Nothing PENDING is left to replay.
        let again = replay_tail(a, false).await.unwrap_err();
        assert!(again.contains("no PENDING"), "{again}");
    });
}

/// Where [`share_tail`] stops the first replay run (a seam): as a crash there would.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum StopAt {
    /// Between (b)'s drop of the copy's prepare and the apply.
    AfterDrop,
    /// Right after a tail commit's apply, with its mark.
    AfterApply,
}

/// The home's (p1, on a) spanning transaction whose p3 share is in c's tail: committed
/// whole before the failover, so its home's decision row says COMMITTED. With `in_doubt`,
/// b's p3 then holds a prepare of the same xid (Amendment 5's (b): sent by hand, as a
/// replicated prepare will once D335 PR 4 lands); without, the replay asks the home (a).
/// The first replay run stops at `stop`; the rerun finishes the tail.
fn share_tail(in_doubt: bool, stop: StopAt) {
    use crate::serve::cluster::tail_replay::seams;
    use crate::serve::proto::{
        BranchBegin, BranchPrepare, ForwardRequest, SubscribeRequest, TxnWriteRequest,
        feed_event::Event, forward_request::Op,
    };
    let t = tail_rig(&["t0"]);
    let [a, b, c] = t.ports;
    let xid = t.rt.block_on(async {
        let r = crate::serve::proto::BeginRequest {
            durability: 0,
            route: route_row("alice"),
            spans_partitions: true,
        };
        let xid = txns(a).await.begin(r).await.unwrap().into_inner().txn_id;
        let mut share = put_row("kx", "vx");
        share.row.as_mut().unwrap().table = "t0".into();
        txn_write(a, &xid, vec![put_row("alice", "home"), share], None)
            .await
            .unwrap();
        commit(a, &xid, None).await.unwrap();
        xid
    });
    let e = fail_over_to_b(&t);
    let b_dir = t.dirs[1].clone();
    let asked = |dir: &PathBuf| {
        seams::ASKED_HOME
            .lock()
            .unwrap()
            .iter()
            .filter(|(d, _)| d == dir)
            .count()
    };
    t.rt.block_on(async {
        // c's file, closed, is the tail's source.
        eventually_within(Duration::from_secs(20), "c to keep p3's tail", async || {
            status_of(c)
                .await
                .partitions
                .iter()
                .any(|lp| lp.id == "p3" && lp.tail_kept.is_some())
        })
        .await;
        // Every commit from here on t0, through a: the replayed share's must be the only
        // event of the xid (the coordinator's condition (1): the drop reaches no one).
        let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{a}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut sub = crate::serve::proto::feed_client::FeedClient::new(channel)
            .subscribe(SubscribeRequest {
                tables: vec!["t0".into()],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        if in_doubt {
            let fwd = |op: Op| ForwardRequest {
                partition: "p3".into(),
                op: Some(op),
                caller: Some(crate::serve::proto::ForwardedCaller {
                    scope: "write".into(),
                    all_namespaces: true,
                    ..Default::default()
                }),
                ..Default::default()
            };
            let mut stale = put_row("kx", "stale");
            stale.row.as_mut().unwrap().table = "t0".into();
            for op in [
                Op::BranchBegin(BranchBegin {
                    xid: xid.clone(),
                    renew: false,
                }),
                Op::TxnWrite(TxnWriteRequest {
                    txn_id: xid.clone(),
                    rows: vec![stale],
                }),
                Op::BranchPrepare(BranchPrepare {
                    xid: xid.clone(),
                    read_only: false,
                }),
            ] {
                client(b).await.forward(fwd(op)).await.unwrap();
            }
            let lp = status_of(b)
                .await
                .partitions
                .into_iter()
                .find(|lp| lp.id == "p3");
            assert!(
                lp.as_ref()
                    .is_some_and(|lp| lp.in_doubt_shares.iter().any(|s| s.xid == xid)),
                "{lp:?}"
            );
        }
        let armed = match stop {
            StopAt::AfterDrop => &seams::STOP_AFTER_DROP,
            StopAt::AfterApply => &seams::STOP_AFTER_APPLY,
        };
        armed.lock().unwrap().push(b_dir.clone());
        // The first run: retried until it reaches the seam (c's source may not answer yet).
        let mut first = None;
        for _ in 0..60 {
            let r = replay_tail(a, false).await;
            if !armed.lock().unwrap().contains(&b_dir) {
                first = Some(r.unwrap());
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let first = first.expect("the first run to reach the seam");
        assert_eq!(first["tail_state"], "PENDING", "{first:?}");
        assert!(
            first["note"].as_str().unwrap().contains("the test's stop"),
            "{first:?}"
        );
        let lp = status_of(b)
            .await
            .partitions
            .into_iter()
            .find(|lp| lp.id == "p3");
        assert!(
            lp.as_ref()
                .is_some_and(|lp| lp.in_doubt_shares.iter().all(|s| s.xid != xid)),
            "the drop leaves nothing in doubt: {lp:?}"
        );
        let applied_first = get_in(a, "t0", "kx").await.unwrap();
        match stop {
            StopAt::AfterDrop => assert_eq!(applied_first, None, "dropped, not yet applied"),
            StopAt::AfterApply => {
                let lp = status_of(b)
                    .await
                    .partitions
                    .into_iter()
                    .find(|lp| lp.id == "p3");
                assert_eq!(
                    applied_first,
                    Some(b"vx".to_vec()),
                    "{first:?} {:?}",
                    lp.and_then(|lp| lp.replay)
                );
            }
        }
        let before = asked(&b_dir);
        if in_doubt {
            assert_eq!(before, 0, "(b) asks no home");
        } else {
            assert_eq!(before, 1, "(a) asks the home");
        }
        let r = replay_until_done(a).await;
        assert_eq!(r["refused"], 0, "{r:?}");
        assert_eq!(get_in(a, "t0", "kx").await.unwrap(), Some(b"vx".to_vec()));
        match stop {
            // The rerun finds the share no longer in doubt and the mark unmoved: (a).
            StopAt::AfterDrop => assert_eq!(asked(&b_dir), before + 1),
            // The mark passed it: the rerun asks the home nothing.
            StopAt::AfterApply => assert_eq!(asked(&b_dir), before, "the rerun asked the home"),
        }
        let done = p3_event(&map_of(a).await).unwrap();
        assert_eq!(
            (done.to_epoch, done.tail_state()),
            (e.to_epoch, TailState::Done),
            "{done:?}"
        );
        // The xid's events on t0: the replayed share alone, once, with its origin.
        let mut mine = Vec::new();
        while let Ok(Some(ev)) = tokio::time::timeout(Duration::from_secs(2), sub.message())
            .await
            .map(Result::unwrap)
        {
            if ev.xid == xid.as_bytes() {
                mine.push(ev);
            }
        }
        assert_eq!(mine.len(), 1, "{mine:?}");
        assert!(
            matches!(mine[0].event, Some(Event::Commit(_))) && mine[0].replayed.is_some(),
            "{mine:?}"
        );
    });
    seams::ASKED_HOME
        .lock()
        .unwrap()
        .retain(|(d, _)| *d != b_dir);
}

/// Amendment 5's (b), the coordinator's crash-between test: a crash between the drop of
/// the copy's prepare and the apply leaves the share decided nowhere here and the mark
/// unmoved; the rerun asks the home (a) and applies it once. The drop reaches no feed
/// subscriber (condition (1)) and writes no decision row a resolver could read.
#[test]
fn a_crash_between_the_drop_of_a_prepare_in_doubt_and_its_apply_applies_the_share_once() {
    share_tail(true, StopAt::AfterDrop);
}

/// Amendment 5's (b): a crash after the drop-then-apply's commit; the rerun asks no home.
#[test]
fn a_crash_after_a_dropped_prepares_apply_reruns_without_asking_the_home() {
    share_tail(true, StopAt::AfterApply);
}

/// Amendment 5's (a): a share asked of its home, applied, then a crash; the rerun asks no
/// home again and applies nothing twice.
#[test]
fn a_crash_after_a_homes_share_applies_reruns_without_asking_the_home_again() {
    share_tail(false, StopAt::AfterApply);
}

/// The map at the writer (`a`), a version on and changed by `f`, set there.
async fn edited(
    a: u16,
    f: &dyn Fn(&mut crate::serve::proto::ClusterMap),
) -> Result<u64, tonic::Status> {
    let mut m = map_of(a).await;
    m.map_version += 1;
    f(&mut m);
    set_at(a, m).await
}

/// The writer's checks while c's tail is PENDING: a hand-made DONE is refused, with no mark
/// (map validation) or with one the owner never reported (C4); a map placing p3 on c, as
/// replica or owner, is refused (M1); a map dropping c is refused (C3). Once the replay sets
/// it DONE, c may hold a copy of p3 again.
#[test]
fn a_pending_tail_refuses_a_hand_made_done_and_maps_that_place_its_partition_on_the_old_node() {
    use crate::serve::proto::ClusterMap;
    let t = tail_rig(&["t0"]);
    let a = t.ports[0];
    t.rt.block_on(async {
        put_in(a, "t0", "k1", "v1").await;
    });
    let e = fail_over_to_b(&t);
    t.rt.block_on(async {
        let done = |through: u64| {
            move |m: &mut ClusterMap| {
                for f in m
                    .failovers
                    .iter_mut()
                    .filter(|f| f.partition == "p3" && f.to_epoch == e.to_epoch)
                {
                    f.set_tail_state(TailState::Done);
                    f.fork_token = Default::default();
                    f.replayed_through = through;
                }
            }
        };
        let err = edited(a, &done(0)).await.unwrap_err();
        assert!(err.message().contains("without a replay's mark"), "{err:?}");
        let err = edited(a, &done(1_000)).await.unwrap_err();
        assert!(
            err.message().contains("reports no finished replay"),
            "{err:?}"
        );
        let on_c = |replica: bool| {
            move |m: &mut ClusterMap| {
                for p in m.partitions.iter_mut().filter(|p| p.id == "p3") {
                    if replica {
                        p.replicas.push("c".into());
                    } else {
                        p.node = "c".into();
                        p.epoch += 1;
                    }
                }
            }
        };
        for replica in [true, false] {
            let err = edited(a, &on_c(replica)).await.unwrap_err();
            assert!(
                err.message().contains("old owner's tail pending"),
                "replica {replica}: {err:?}"
            );
        }
        let err = edited(a, &|m: &mut ClusterMap| {
            m.nodes.retain(|n| n.id != "c");
            for p in &mut m.partitions {
                p.replicas.retain(|r| r != "c");
            }
        })
        .await
        .unwrap_err();
        assert!(
            err.message()
                .contains("holds partition \"p3\"'s old owner's tail"),
            "{err:?}"
        );
        // The node's own net (M1): a stamp sent to c over its kept file, as a move's or a
        // restore's would be, is refused there whatever map its sender read.
        let err = client(t.ports[2])
            .await
            .stamp(crate::serve::proto::StampRequest {
                cluster_id: map_of(a).await.cluster_id,
                partition: "p3".into(),
                from_epoch: e.from_epoch,
                to_epoch: e.to_epoch + 10,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(
            err.message().contains("holds its old owner's tail"),
            "{err:?}"
        );
        let still = p3_event(&map_of(a).await).unwrap();
        assert_eq!(still.tail_state(), TailState::Pending, "{still:?}");
        // The replay's DONE lifts M1.
        replay_until_done(a).await;
        assert_eq!(get_in(a, "t0", "k1").await.unwrap(), Some(b"v1".to_vec()));
        edited(a, &on_c(true)).await.unwrap();
    });
}
