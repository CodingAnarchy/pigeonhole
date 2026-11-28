//! Read replicas of a cluster partition (D328, #1851), PR 1: the map lists them, a node
//! holds a replica copy that bootstraps from the owner and follows its feed under the
//! owner's lease, and a map that drops it closes it.

use super::*;
use crate::serve::proto::{
    CreateTableRequest, FamilySpec, LocalPartition, PartitionRole, ReplicaState,
    TablePlacementRequest, schema_client::SchemaClient,
};

/// Node `port`'s status entry for its replica copy of `p`, if it reports one.
async fn copy_of(port: u16, p: &str) -> Option<LocalPartition> {
    client(port)
        .await
        .status(StatusRequest::default())
        .await
        .unwrap()
        .into_inner()
        .partitions
        .into_iter()
        .find(|lp| lp.id == p && lp.role == PartitionRole::Replica as i32)
}

/// Sets the writer's (a's) map with `p`'s replicas as `replicas`, retried while a node that
/// just started is still taking its peers' maps.
pub(super) async fn set_replicas(a: u16, p: &str, replicas: &[&str]) {
    let mut last = None;
    for _ in 0..100 {
        let mut m = client(a)
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        let expected = m.map_version;
        m.map_version += 1;
        for q in m.partitions.iter_mut().filter(|q| q.id == p) {
            q.replicas = replicas.iter().map(|r| (*r).into()).collect();
        }
        match client(a)
            .await
            .set_map(SetMapRequest {
                force: false,
                expected_map_version: expected,
                map: Some(m),
                copies: Vec::new(),
            })
            .await
        {
            Ok(_) => return,
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("set the map: {last:?}");
}

/// Table `r` on p3 alone, through the writer at `port`: retried while a node that just
/// started is still taking its peers' maps (the restarted-node rig).
async fn table_on_p3(port: u16) {
    for _ in 0..100 {
        match try_table_on_p3(port).await {
            Ok(()) => return,
            Err(e) if e.code() == tonic::Code::Unavailable => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("{e:?}"),
        }
    }
    panic!("table r was never created on p3");
}

async fn try_table_on_p3(port: u16) -> Result<(), tonic::Status> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    SchemaClient::new(channel)
        .create_table(CreateTableRequest {
            name: "r".into(),
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
        .map(|_| ())
}

/// How many bootstraps the follower of the replica copy in `dir` has finished.
fn bootstraps_in(dir: &Path) -> usize {
    crate::serve::replica::BOOTSTRAPS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .filter(|p| p.starts_with(dir))
        .count()
}

/// How long a new replica copy may take to reach Following after its bootstrap (#2219's
/// Windows CI: 5 s wasn't enough on a loaded runner). A dial to a port that isn't listening
/// yet takes about 2 s on Windows' runners, and a follow that fails pauses before its next
/// try, doubling up to the follower's MAX_BACKOFF (10 s).
const FOLLOW: Duration = Duration::from_secs(30);

/// A replica copy's row as a wait's reason: its state, owner, progress and last error.
fn copy_why(lp: Option<&LocalPartition>) -> String {
    lp.map_or_else(
        || "no copy row".to_owned(),
        |lp| {
            format!(
                "{:?} of {:?}, applied {} of {}, lag {} ms, error {:?}",
                lp.replica_state(),
                lp.replica_of,
                lp.replica_applied_seqno,
                lp.replica_primary_seqno,
                lp.replica_lag_ms,
                lp.error
            )
        },
    )
}

/// Three nodes in fresh directories, with `r` on p3 (c's), a row in it, and a's replica
/// copy of p3 following c past that row: the directories, ports and stop handles.
fn following(
    rt: &tokio::runtime::Runtime,
) -> (
    [PathBuf; 3],
    Vec<u16>,
    Vec<tokio::sync::oneshot::Sender<()>>,
    Vec<pigeonhole::Pigeonhole>,
) {
    let dirs = [test_dir(), test_dir(), test_dir()];
    let (stops, ports, dbs) = three_nodes_in(rt, &dirs);
    rt.block_on(async {
        table_on_p3(ports[0]).await;
        let first = put_in(ports[1], "r", "k1").await;
        set_replicas(ports[0], "p3", &["a"]).await;
        eventually_why(FOLLOW, "a's replica of p3 to follow c", async || {
            let lp = copy_of(ports[0], "p3").await;
            if lp.as_ref().is_some_and(|lp| {
                lp.replica_state() == ReplicaState::Following
                    && lp.replica_of == "c"
                    && lp.replica_applied_seqno >= first
            }) {
                return Ok(());
            }
            Err(format!("past {first}: {}", copy_why(lp.as_ref())))
        })
        .await;
    });
    (dirs, ports, stops, dbs)
}

/// The node args of a `phdb cluster` command through node `port`.
fn args_at(port: u16) -> super::super::cli::NodeArgs {
    super::super::cli::NodeArgs {
        own: None,
        node: format!("http://127.0.0.1:{port}"),
        token_file: None,
        ca: None,
    }
}

/// [`put_in`], retried while the partition's new node may still be opening it after a move,
/// or holds it fenced until its lease, for up to 30 s, as the fencing tests' writes are.
async fn put_after_move(port: u16, table: &str, row: &str) -> u64 {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut w = put_row(row, "v");
        w.row.as_mut().unwrap().table = table.into();
        match data(port)
            .await
            .mutate(crate::serve::proto::MutateRequest {
                replication: 0,
                writes: Some(w),
                durability: 0,
            })
            .await
        {
            Ok(r) => return r.into_inner().commit.map_or(0, |c| c.seqno),
            // Still opening there, or fenced until its lease: the same put again (one row,
            // one value, so a retry of an unknown outcome is the same write).
            Err(e) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the moved partition never took a write: {e:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn put_in(port: u16, table: &str, row: &str) -> u64 {
    let mut w = put_row(row, "v");
    w.row.as_mut().unwrap().table = table.into();
    data(port)
        .await
        .mutate(crate::serve::proto::MutateRequest {
            replication: 0,
            writes: Some(w),
            durability: 0,
        })
        .await
        .unwrap()
        .into_inner()
        .commit
        .map_or(0, |c| c.seqno)
}

#[test]
fn a_replica_copy_bootstraps_follows_and_closes_when_the_map_drops_it() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes(&rt);
    rt.block_on(async {
        // `r` on p3 alone (node c), with a row before any replica exists.
        table_on_p3(ports[0]).await;
        let first = put_in(ports[1], "r", "k1").await;
        // a holds p3's replica: it bootstraps from c and follows.
        set_replicas(ports[0], "p3", &["a"]).await;
        eventually(
            "a's replica of p3 to follow c past the first write",
            async || {
                copy_of(ports[0], "p3").await.is_some_and(|lp| {
                    lp.replica_state() == ReplicaState::Following
                        && lp.replica_of == "c"
                        && lp.replica_applied_seqno >= first
                })
            },
        )
        .await;
        // A write after it: the copy applies it.
        let second = put_in(ports[2], "r", "k2").await;
        eventually("a's replica of p3 to apply the second write", async || {
            copy_of(ports[0], "p3")
                .await
                .is_some_and(|lp| lp.replica_applied_seqno >= second && lp.ok)
        })
        .await;
        // Admin.Info lists the copy beside a's own p1, with its follower; /metrics labels
        // the follower's gauges with p3, each family's `# TYPE` once.
        let channel =
            tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{}", ports[0]))
                .unwrap()
                .connect()
                .await
                .unwrap();
        let mut admin = crate::serve::proto::admin_client::AdminClient::new(channel);
        let info = admin
            .info(crate::serve::proto::Empty {})
            .await
            .unwrap()
            .into_inner();
        let p3 = info.partitions.iter().find(|p| p.id == "p3").unwrap();
        let replica = p3.replica.as_ref().expect("the copy's follower");
        assert_eq!(replica.state, "following", "{p3:?}");
        assert!(replica.applied_primary_seqno >= second, "{p3:?}");
        assert!(p3.tables.iter().any(|t| t == "r"), "{p3:?}");
        let p1 = info.partitions.iter().find(|p| p.id == "p1").unwrap();
        assert!(p1.replica.is_none(), "{p1:?}");
        let text = admin
            .metrics(crate::serve::proto::Empty {})
            .await
            .unwrap()
            .into_inner()
            .text;
        assert!(
            text.contains("pigeonhole_replica_following{pigeonhole_partition=\"p3\"} 1"),
            "{text}"
        );
        assert_eq!(
            text.matches("# TYPE pigeonhole_replica_lag_seconds gauge")
                .count(),
            1,
            "{text}"
        );
        // The owner and the others report their roles; no other node holds a copy.
        assert!(copy_of(ports[1], "p3").await.is_none());
        let c = client(ports[2])
            .await
            .status(StatusRequest::default())
            .await
            .unwrap()
            .into_inner();
        let p3 = c.partitions.iter().find(|lp| lp.id == "p3").unwrap();
        assert_eq!(p3.role(), PartitionRole::Owner, "{p3:?}");
        // The map drops it: a closes its copy (its file stays).
        set_replicas(ports[0], "p3", &[]).await;
        eventually("a's replica of p3 to close", async || {
            copy_of(ports[0], "p3").await.is_none()
        })
        .await;
    });
}

#[test]
fn the_map_checks_each_partitions_replicas() {
    let ports = [1, 2, 3];
    let with = |replicas: &[&str]| {
        let mut m = the_map(1, &ports);
        m.partitions[2].replicas = replicas.iter().map(|r| (*r).into()).collect();
        super::super::map::validate(&m, false)
    };
    with(&["a", "b"]).unwrap();
    let e = with(&["z"]).unwrap_err();
    assert!(e.contains("doesn't list"), "{e}");
    let e = with(&["c"]).unwrap_err();
    assert!(e.contains("owner"), "{e}");
    let e = with(&["a", "a"]).unwrap_err();
    assert!(e.contains("twice"), "{e}");
}

#[test]
fn a_stamp_onto_a_node_holding_the_partitions_replica_is_refused() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = three_nodes(&rt);
    rt.block_on(async {
        set_replicas(ports[0], "p3", &["a"]).await;
        eventually("a's replica of p3 to open", async || {
            copy_of(ports[0], "p3").await.is_some()
        })
        .await;
        // As a move of p3 to a would stamp it there, whatever map the caller read.
        let e = client(ports[0])
            .await
            .stamp(crate::serve::proto::StampRequest {
                cluster_id: "c1".into(),
                partition: "p3".into(),
                from_epoch: 1,
                to_epoch: 2,
                restore: false,
                restore_from_cluster_id: String::new(),
                promote: false,
                replica_epoch: 0,
                create: false,
                create_move: String::new(),
                create_version: 0,
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e:?}");
        assert!(e.message().contains("replica copy"), "{e:?}");
    });
}

/// Three nodes as [`three_nodes_in`] makes them, each started as a restarted node (D326), so
/// their partitions hold real leases of [`TEST_LEASE`], renewed by their rounds, rather than
/// a standing one.
fn three_leased(
    rt: &tokio::runtime::Runtime,
    dirs: &[PathBuf; 3],
) -> (Vec<tokio::sync::oneshot::Sender<()>>, Vec<u16>) {
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let map = three_way(&[ports[0], ports[1], ports[2]]);
    let mut stops = Vec::new();
    for (((l, id), p), dir) in listeners
        .into_iter()
        .zip(["a", "b", "c"])
        .zip(["p1", "p2", "p3"])
        .zip(dirs)
    {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
        let (stop, _) = data_node_full(
            rt,
            dir,
            id,
            Some(p),
            l,
            "",
            crate::serve::server_tests::limits(),
            None,
            true,
        );
        stops.push(stop);
    }
    (stops, ports)
}

/// simp724's M1: the owner serves `Follow` only under P's lease. Cut off from the other
/// nodes (its clients' connections up, and the replica's stream already open), c loses p3's
/// lease, and that ends the stream: the copy stops following and its lag grows past a
/// lease, rather than a deposed owner feeding it on.
#[test]
fn a_cut_off_owners_follow_stream_ends_with_its_lease() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dirs = [test_dir(), test_dir(), test_dir()];
    let (_stops, ports) = three_leased(&rt, &dirs);
    let c_at = format!("127.0.0.1:{}", ports[2]);
    rt.block_on(async {
        set_replicas(ports[0], "p3", &["a"]).await;
        eventually_why(FOLLOW, "a's replica of p3 to follow c", async || {
            let lp = copy_of(ports[0], "p3").await;
            if lp.as_ref().is_some_and(|lp| {
                lp.replica_state() == ReplicaState::Following && lp.replica_lag_ms < 1000
            }) {
                return Ok(());
            }
            Err(copy_why(lp.as_ref()))
        })
        .await;
        super::super::cut_node(&dirs[2], &c_at, &dirs, true);
        let lease = u64::try_from(TEST_LEASE.as_millis()).unwrap();
        eventually("c's stream to end with p3's lease", async || {
            copy_of(ports[0], "p3").await.is_some_and(|lp| {
                lp.replica_state() != ReplicaState::Following && lp.replica_lag_ms > lease
            })
        })
        .await;
    });
    super::super::cut_node(&dirs[2], &c_at, &dirs, false);
}

/// #2162 item 4, feed1248's point 2 on #2184: a planned promote fences its owner again only
/// when the operator fenced it. c, cut off from its peers, is fenced without its lease (as
/// awaiting its peers' map, or for its lapsed lease): the promote goes on, and says its
/// count is a lower bound for that reason.
#[test]
fn a_planned_promotion_from_an_owner_fenced_without_its_lease_counts_a_lower_bound() {
    use crate::serve::proto::{FenceReason, LocalPartitionState, StatusRequest};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dirs = [test_dir(), test_dir(), test_dir()];
    let (_stops, ports) = three_leased(&rt, &dirs);
    let c_at = format!("127.0.0.1:{}", ports[2]);
    rt.block_on(async {
        set_replicas(ports[0], "p3", &["a"]).await;
        eventually_why(FOLLOW, "a's replica of p3 to follow c", async || {
            let lp = copy_of(ports[0], "p3").await;
            if lp.as_ref().is_some_and(|lp| {
                lp.replica_state() == ReplicaState::Following && lp.replica_lag_ms < 1000
            }) {
                return Ok(());
            }
            Err(copy_why(lp.as_ref()))
        })
        .await;
        super::super::cut_node(&dirs[2], &c_at, &dirs, true);
        eventually_why(
            Duration::from_secs(10),
            "c's p3 to be fenced without its lease, not by the operator",
            async || {
                let r = client(ports[2])
                    .await
                    .status(StatusRequest::default())
                    .await
                    .map_err(|s| format!("c's status: {s:?}"))?;
                let lp = r
                    .into_inner()
                    .partitions
                    .into_iter()
                    .find(|lp| lp.id == "p3");
                match lp {
                    Some(lp)
                        if lp.state() == LocalPartitionState::Fenced
                            && lp.fence_reason() != FenceReason::Operator =>
                    {
                        Ok(())
                    }
                    other => Err(format!("c's p3: {other:?}")),
                }
            },
        )
        .await;
        let r = within(super::super::promote::promote(
            &args_at(ports[0]),
            "p3",
            "a",
            false,
            None,
        ))
        .await
        .unwrap();
        assert!(
            r.document["lost_commits_counted"].as_str().is_some_and(
                |s| s.starts_with("a lower bound: the owner's fence isn't the operator's")
            ),
            "{:?}",
            r.document
        );
    });
    super::super::cut_node(&dirs[2], &c_at, &dirs, false);
}

/// Design test 2: a move of p3 with its files (`phdb cluster move`) keeps its history, so
/// a's follower resumes from its token on the new owner, b, with no second bootstrap, and
/// applies the writes made there.
#[test]
fn a_replica_copy_follows_a_moved_owner_without_bootstrapping_again() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (dirs, ports, _stops, _dbs) = following(&rt);
    assert_eq!(bootstraps_in(&dirs[0]), 1, "the first bootstrap");
    rt.block_on(async {
        within(super::super::moving::run(
            &args_at(ports[0]),
            "p3",
            "b",
            true,
            None,
        ))
        .await
        .unwrap();
        let after = put_after_move(ports[0], "r", "k2").await;
        // The copy retried its old owner while the partition moved, its pause doubling up
        // to the follower's MAX_BACKOFF (10 s); the map that moved it wakes the copy (D330
        // amendment 2), so `eventually`'s 5 s, not a wait past that pause (#2015).
        eventually(
            "a's replica of p3 to follow b past a write there",
            async || {
                copy_of(ports[0], "p3").await.is_some_and(|lp| {
                    lp.replica_state() == ReplicaState::Following
                        && lp.replica_of == "b"
                        && lp.replica_applied_seqno >= after
                })
            },
        )
        .await;
    });
    assert_eq!(bootstraps_in(&dirs[0]), 1, "it resumed: no bootstrap since");
}

/// Design test 3: a relocate without copy gives p3 another history (`history_epoch` moves
/// to the new epoch), so a's follower bootstraps again from the new owner rather than
/// resuming, though the files are the same database, and then follows it.
#[test]
fn a_replica_copy_bootstraps_again_after_a_relocate_without_copy() {
    use crate::serve::proto::PartitionRequest;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (dirs, ports, _stops, _dbs) = following(&rt);
    let [_, b, c] = [ports[0], ports[1], ports[2]];
    rt.block_on(async {
        let req = || PartitionRequest {
            partition: "p3".into(),
            ..Default::default()
        };
        client(c).await.fence(req()).await.unwrap();
        client(c).await.close(req()).await.unwrap();
        super::stamp::upload_set(b, "p3", &dirs[2].join("p3.phdb"), 2).await;
        let r = within(super::super::cli::relocate(
            &args_at(ports[0]),
            "p3",
            "b",
            false,
            false,
        ))
        .await
        .unwrap();
        assert_eq!(r.document["epoch"], 2, "{:?}", r.document);
        let after = put_after_move(ports[0], "r", "k2").await;
        // The copy retried its old owner while the partition moved, its pause doubling up
        // to the follower's MAX_BACKOFF (10 s); the map that moved it wakes the copy (D330
        // amendment 2), so `eventually`'s 5 s, not a wait past that pause (#2015).
        eventually(
            "a's replica of p3 to follow b past a write there",
            async || {
                copy_of(ports[0], "p3").await.is_some_and(|lp| {
                    lp.replica_state() == ReplicaState::Following
                        && lp.replica_of == "b"
                        && lp.replica_applied_seqno >= after
                })
            },
        )
        .await;
    });
    assert_eq!(bootstraps_in(&dirs[0]), 2, "it bootstrapped again from b");
}

/// Three nodes as [`three_way`] places them, c's p3 without a change feed (`phdb serve`
/// without `--change-feed`): its directories, ports, stop handles and databases.
fn feedless_p3(
    rt: &tokio::runtime::Runtime,
) -> (
    [PathBuf; 3],
    Vec<u16>,
    Vec<tokio::sync::oneshot::Sender<()>>,
    Vec<pigeonhole::Pigeonhole>,
) {
    let dirs = [test_dir(), test_dir(), test_dir()];
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let map = three_way(&[ports[0], ports[1], ports[2]]);
    // c's p3 without a change feed: `phdb serve` without --change-feed.
    let p3 = pigeonhole::Pigeonhole::open(
        dirs[2].join("p3.phdb"),
        pigeonhole::Options::default()
            .create_if_missing(true)
            .wal_segment_size(1 << 20),
    )
    .unwrap();
    super::super::identity::write(
        &p3,
        &super::super::identity::Identity {
            cluster_id: "c1".into(),
            partition_id: "p3".into(),
            epoch: 1,
        },
    )
    .unwrap();
    let mut p3 = Some(p3);
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for (((l, id), p), dir) in listeners
        .into_iter()
        .zip(["a", "b", "c"])
        .zip(["p1", "p2", "p3"])
        .zip(&dirs)
    {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
        let db = if id == "c" { p3.take() } else { None };
        let (stop, db) = data_node_full(
            rt,
            dir,
            id,
            Some(p),
            l,
            "",
            crate::serve::server_tests::limits(),
            db,
            false,
        );
        stops.push(stop);
        dbs.extend(db);
    }
    (dirs, ports, stops, dbs)
}

/// S1: a replica follows its owner's change feed, so an owner started without one answers
/// its bootstrap with why, and the copy reports `OWNER_FEED_OFF` (not merely behind) with
/// that reason, rather than a bare engine error.
#[test]
fn a_replica_copy_of_a_partition_whose_owner_has_no_feed_says_so() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dirs, ports, stops, dbs) = feedless_p3(&rt);
    rt.block_on(async {
        set_replicas(ports[0], "p3", &["a"]).await;
        let mut last = None;
        for _ in 0..100 {
            last = copy_of(ports[0], "p3").await;
            if last.as_ref().is_some_and(|lp| {
                lp.replica_state() == ReplicaState::OwnerFeedOff
                    && lp.error.contains("--change-feed")
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("a's replica of p3 never said c's feed is off: {last:?}");
    });
    drop(stops);
    for db in dbs {
        let _ = db.close();
    }
}

/// The owner tries a replica's opaque token against each partition it owns (simp724's S2 on
/// #1974): with p2 and p3 both on b, a's replica of p3 is tried against p2 first (in id
/// order), whose "the token is another database's" moves on to p3. So this follows only
/// while `follow_owned`'s match still finds the engine's wording, and pins it.
#[test]
fn a_replica_copy_follows_an_owner_holding_several_partitions() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dirs = [test_dir(), test_dir(), test_dir()];
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let mut map = three_way(&[ports[0], ports[1], ports[2]]);
    for p in map.partitions.iter_mut().filter(|p| p.id == "p3") {
        p.node = "b".into();
    }
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for ((l, id), dir) in listeners.into_iter().zip(["a", "b", "c"]).zip(&dirs) {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
        let locals: Vec<(&str, Option<pigeonhole::Pigeonhole>)> = match id {
            "a" => vec![("p1", None)],
            "b" => vec![("p2", None), ("p3", None)],
            _ => Vec::new(),
        };
        let (stop, db) = data_node_many(
            &rt,
            dir,
            id,
            locals,
            l,
            "",
            crate::serve::server_tests::limits(),
            false,
        );
        stops.push(stop);
        dbs.extend(db);
    }
    rt.block_on(async {
        table_on_p3(ports[0]).await;
        let first = put_in(ports[2], "r", "k1").await;
        set_replicas(ports[0], "p3", &["a"]).await;
        eventually("a's replica of p3 to follow b past a write", async || {
            copy_of(ports[0], "p3").await.is_some_and(|lp| {
                lp.replica_state() == ReplicaState::Following
                    && lp.replica_of == "b"
                    && lp.replica_applied_seqno >= first
            })
        })
        .await;
    });
    assert_eq!(
        bootstraps_in(&dirs[0]),
        1,
        "one bootstrap, then it followed"
    );
    drop(stops);
    for db in dbs {
        let _ = db.close();
    }
}

/// A `Get` of `row` in `r` through node `port`, with the replica headers: the bound
/// (`max_lag_ms`) and, if given, the read-your-writes seqno. Returns the value read and the
/// `served-by` and `served-seqno` response headers.
async fn read_through(
    port: u16,
    row: &str,
    max_lag_ms: Option<u64>,
    min_seqno: Option<u64>,
) -> Result<(Option<Vec<u8>>, Option<String>, Option<u64>), tonic::Status> {
    let mut req = tonic::Request::new(crate::serve::proto::GetRequest {
        table: "r".into(),
        row: row.as_bytes().to_vec().into(),
        family: "f".into(),
        qualifier: b"q".to_vec().into(),
        ..Default::default()
    });
    if let Some(n) = max_lag_ms {
        req.metadata_mut().insert(
            crate::serve::replica::READ_REPLICA_MAX_LAG_MS,
            n.to_string().parse().unwrap(),
        );
    }
    if let Some(s) = min_seqno {
        req.metadata_mut().insert(
            crate::serve::replica::MIN_PRIMARY_SEQNO,
            s.to_string().parse().unwrap(),
        );
    }
    let resp = data(port).await.get(req).await?;
    let header = |name| {
        resp.metadata()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    let by = header(crate::serve::replica::SERVED_BY);
    let seqno = header(crate::serve::replica::SERVED_SEQNO).and_then(|v| v.parse().ok());
    let value = resp.into_inner().cell.map(|c| c.value.to_vec());
    Ok((value, by, seqno))
}

/// D328 PR 2a: a read that allows staleness is served by p3's replica on a, whichever node
/// it enters by (a holds the copy; b and c forward to it), and says so; without the header,
/// it's the owner's, as before, with no served headers.
#[test]
fn a_bounded_read_is_served_by_a_replica_and_says_so() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dirs, ports, _stops, _dbs) = following(&rt);
    rt.block_on(async {
        let first = put_in(ports[1], "r", "k1").await;
        eventually("a's copy to apply the write", async || {
            copy_of(ports[0], "p3")
                .await
                .is_some_and(|lp| lp.replica_applied_seqno >= first)
        })
        .await;
        for port in ports.iter().copied() {
            let (value, by, seqno) = read_through(port, "k1", Some(60_000), None).await.unwrap();
            assert_eq!(value.as_deref(), Some(&b"v"[..]), "through {port}");
            assert_eq!(by.as_deref(), Some("replica"), "through {port}");
            assert!(
                seqno.is_some_and(|s| s >= first),
                "through {port}: {seqno:?}"
            );
        }
        let (value, by, seqno) = read_through(ports[1], "k1", None, None).await.unwrap();
        assert_eq!(value.as_deref(), Some(&b"v"[..]));
        assert_eq!((by, seqno), (None, None), "no header, no replica");
        // Read-your-writes: a write, then a read through b with its seqno sees it, from
        // the replica once it has applied it (or the owner, if the wait ran out).
        let second = put_in(ports[1], "r", "k2").await;
        let (value, _, seqno) = read_through(ports[1], "k2", Some(60_000), Some(second))
            .await
            .unwrap();
        assert_eq!(value.as_deref(), Some(&b"v"[..]));
        assert!(seqno.is_some_and(|s| s >= second), "{seqno:?}");
    });
}

/// cli692 on #2276: a replica whose forwarded calls' lane is full refuses a read with
/// `Overloaded`, which sends it on to the owner, as a replica that's behind does, rather
/// than failing it.
#[test]
fn a_replica_with_its_lane_full_leaves_the_read_to_the_owner() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (dirs, ports, _stops, _dbs) = following(&rt);
    let a = dirs[0].clone();
    let max = crate::serve::server_tests::limits().max_concurrent;
    struct LetGo(PathBuf);
    impl Drop for LetGo {
        fn drop(&mut self) {
            super::super::HOLD_FORWARDS
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .retain(|d| *d != self.0);
        }
    }
    let held_on_a = || {
        super::super::HELD_FORWARDS
            .lock()
            .unwrap()
            .iter()
            .filter(|d| **d == a)
            .count()
    };
    rt.block_on(async {
        eventually("a's copy to serve the read first", async || {
            read_through(ports[1], "k1", Some(60_000), None)
                .await
                .is_ok_and(|(_, by, _)| by.as_deref() == Some("replica"))
        })
        .await;
        super::super::HOLD_FORWARDS.lock().unwrap().push(a.clone());
        let let_go = LetGo(a.clone());
        let c = client(ports[0]).await;
        let held: Vec<_> = (0..max)
            .map(|_| {
                let mut c = c.clone();
                tokio::spawn(async move {
                    c.forward(crate::serve::proto::ForwardRequest {
                        partition: "p3".into(),
                        ..Default::default()
                    })
                    .await
                })
            })
            .collect();
        eventually("a's clients' lane to be full", async || held_on_a() == max).await;
        let (value, by, _) = tokio::time::timeout(
            Duration::from_secs(5),
            read_through(ports[1], "k1", Some(60_000), None),
        )
        .await
        .expect("the read answers within 5 s")
        .unwrap();
        assert_eq!(value.as_deref(), Some(&b"v"[..]));
        assert_eq!(
            by.as_deref(),
            Some("owner"),
            "the replica's refusal falls back"
        );
        drop(let_go);
        for h in held {
            let _ = tokio::time::timeout(Duration::from_secs(5), h)
                .await
                .expect("a forward let go answers within 5 s")
                .unwrap();
        }
    });
}

/// A replica that isn't fresh (its owner has no change feed, so it never follows) leaves a
/// bounded read to the owner, which says so; the next read skips it for its back-off.
#[test]
fn a_replica_that_isnt_fresh_leaves_the_read_to_the_owner() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dirs, ports, stops, dbs) = feedless_p3(&rt);
    rt.block_on(async {
        table_on_p3(ports[0]).await;
        let first = put_in(ports[1], "r", "k1").await;
        set_replicas(ports[0], "p3", &["a"]).await;
        eventually("a's copy of p3 to open", async || {
            copy_of(ports[0], "p3").await.is_some()
        })
        .await;
        for _ in 0..2 {
            let (value, by, seqno) = read_through(ports[1], "k1", Some(60_000), None)
                .await
                .unwrap();
            assert_eq!(value.as_deref(), Some(&b"v"[..]));
            assert_eq!(by.as_deref(), Some("owner"));
            assert!(seqno.is_some_and(|s| s >= first), "{seqno:?}");
        }
    });
    drop(stops);
    for db in dbs {
        let _ = db.close();
    }
}

/// A read across partitions can't take one partition's read-your-writes seqno (D328 §3).
#[test]
fn a_read_across_partitions_with_a_seqno_is_refused() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dirs, ports, _stops, _dbs) = following(&rt);
    rt.block_on(async {
        let mut req = tonic::Request::new(crate::serve::proto::MultiGetRequest {
            table: "r".into(),
            keys: vec![b"a".to_vec().into(), b"z".to_vec().into()],
            ..Default::default()
        });
        req.metadata_mut().insert(
            crate::serve::replica::READ_REPLICA_MAX_LAG_MS,
            "1000".parse().unwrap(),
        );
        req.metadata_mut().insert(
            crate::serve::replica::MIN_PRIMARY_SEQNO,
            "1".parse().unwrap(),
        );
        let e = data(ports[1]).await.multi_get(req).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e:?}");
    });
}

/// simp724's M1 on #1989: an idle owner's heartbeats name the same durable frontier, and
/// each keeps its copy fresh. After 3 s with no writes, a read allowing 1.5 s is still the
/// replica's.
#[test]
fn an_idle_owners_replica_stays_fresh() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dirs, ports, _stops, _dbs) = following(&rt);
    rt.block_on(async {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let (value, by, _) = read_through(ports[1], "k1", Some(1500), None)
            .await
            .unwrap();
        assert_eq!(value.as_deref(), Some(&b"v"[..]));
        assert_eq!(by.as_deref(), Some("replica"));
    });
}

/// The map as node `port` has it: P's node, epoch, history epoch and replicas.
async fn placed(port: u16, p: &str) -> (String, u64, u64, Vec<String>) {
    let m = client(port)
        .await
        .get_map(GetMapRequest {})
        .await
        .unwrap()
        .into_inner();
    let q = m.partitions.into_iter().find(|q| q.id == p).unwrap();
    (q.node, q.epoch, q.history_epoch, q.replicas)
}

/// D328 PR 3: a planned promotion. With p3 closed on its owner c (which releases its
/// lease), `phdb cluster promote p3 --to a` makes a's replica the owner at the next epoch
/// with a new history: the rows c had are there, writes land on a, and nothing is lost.
#[test]
fn a_planned_promotion_makes_the_replica_the_owner() {
    use crate::serve::proto::PartitionRequest;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dirs, ports, _stops, _dbs) = following(&rt);
    rt.block_on(async {
        let last = put_in(ports[1], "r", "k2").await;
        eventually("a's copy to apply k2", async || {
            copy_of(ports[0], "p3")
                .await
                .is_some_and(|lp| lp.replica_applied_seqno >= last)
        })
        .await;
        let req = || PartitionRequest {
            partition: "p3".into(),
            ..Default::default()
        };
        client(ports[2]).await.fence(req()).await.unwrap();
        client(ports[2]).await.close(req()).await.unwrap();
        let r = within(super::super::promote::promote(
            &args_at(ports[0]),
            "p3",
            "a",
            false,
            None,
        ))
        .await
        .unwrap();
        assert_eq!(r.document["epoch"], 2, "{:?}", r.document);
        assert_eq!(r.document["lost_commits"], 0, "{:?}", r.document);
        // A closed owner streams nothing, so the count is a lower bound (#2162 item 4).
        assert!(
            r.document["lost_commits_counted"]
                .as_str()
                .is_some_and(|s| s.starts_with("a lower bound: the owner was closed")),
            "{:?}",
            r.document
        );
        // The promote waited for a to serve p3 (#2035): a read on a at once, no retry.
        let (value, _, _) = read_through(ports[1], "k2", None, None).await.unwrap();
        assert_eq!(value.as_deref(), Some(&b"v"[..]));
        let (node, epoch, history, replicas) = placed(ports[1], "p3").await;
        assert_eq!((node.as_str(), epoch, history), ("a", 2, 2));
        assert!(replicas.is_empty(), "{replicas:?}");
        // The rows the owner had, and a write after it, on a.
        let after = put_after_move(ports[1], "r", "k3").await;
        assert!(after > 0);
        for row in ["k1", "k2", "k3"] {
            let (value, _, _) = read_through(ports[1], row, None, None).await.unwrap();
            assert_eq!(value.as_deref(), Some(&b"v"[..]), "{row}");
        }
        // Run again: done.
        let again = within(super::super::promote::promote(
            &args_at(ports[0]),
            "p3",
            "a",
            false,
            None,
        ))
        .await
        .unwrap();
        assert!(again.document.contains_key("done"), "{:?}", again.document);
    });
}

/// #2162 item 4: with the owner fenced (not closed) it still streams, so the promote waits
/// for the copy to hear it after the fence. A write just before the fence, which the copy
/// may not have had when the promote began, is applied, and the loss is counted exactly.
/// The write is `Buffered` (feed1248's S1 on #2184): the owner's feed reports it durable
/// only at its background sync, which the fence waits for, so its durable frontier is
/// final once the fence answers.
#[test]
fn a_planned_promotion_from_a_fenced_owner_counts_the_loss_exactly() {
    use crate::serve::proto::{Durability, MutateRequest, PartitionRequest};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dirs, ports, _stops, dbs) = following(&rt);
    rt.block_on(async {
        let mut w = put_row("k2", "v");
        w.row.as_mut().unwrap().table = "r".into();
        data(ports[1])
            .await
            .mutate(MutateRequest {
                replication: 0,
                writes: Some(w),
                durability: Durability::Buffered as i32,
            })
            .await
            .unwrap();
        let req = PartitionRequest {
            partition: "p3".into(),
            ..Default::default()
        };
        client(ports[2]).await.fence(req).await.unwrap();
        // c's p3 (`dbs[2]`): nothing it answered is still held from its feed.
        let (frontier, visible) = (dbs[2].replication_frontier(), dbs[2].visible_seqno());
        assert!(frontier >= visible, "{frontier} < {visible}");
        let r = within(super::super::promote::promote(
            &args_at(ports[0]),
            "p3",
            "a",
            false,
            None,
        ))
        .await
        .unwrap();
        assert!(
            r.document["lost_commits_counted"]
                .as_str()
                .is_some_and(|s| s.starts_with("exact")),
            "{:?}",
            r.document
        );
        assert_eq!(r.document["lost_commits"], 0, "{:?}", r.document);
        let (value, _, _) = read_through(ports[1], "k2", None, None).await.unwrap();
        assert_eq!(value.as_deref(), Some(&b"v"[..]));
    });
}

/// A promotion to a node that holds no replica of the partition is refused, and so is one
/// whose owner still serves it.
#[test]
fn a_promotion_needs_a_replica_and_an_owner_that_stopped_serving() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dirs, ports, _stops, _dbs) = following(&rt);
    rt.block_on(async {
        let e = within(super::super::promote::promote(
            &args_at(ports[0]),
            "p3",
            "a",
            false,
            None,
        ))
        .await
        .err()
        .expect("the owner still serves p3");
        assert!(
            e.message().contains("isn't fenced or closed"),
            "{}",
            e.message()
        );
        let e = within(super::super::promote::promote(
            &args_at(ports[0]),
            "p3",
            "b",
            true,
            None,
        ))
        .await
        .err()
        .expect("b holds no replica of p3");
        assert!(
            e.message().contains("answers and serves") || e.message().contains("no copy"),
            "{}",
            e.message()
        );
    });
}

/// `--old-unreachable`: c, p3's owner, stops (real 1.5 s leases here). The promotion goes
/// ahead, reports the loss as unknown, and a serves p3 once c's lease has run out.
#[test]
fn a_promotion_with_the_owner_unreachable_reports_the_loss_as_unknown() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dirs = [test_dir(), test_dir(), test_dir()];
    let (mut stops, ports) = three_leased(&rt, &dirs);
    rt.block_on(async {
        table_on_p3(ports[0]).await;
        let first = put_after_move(ports[1], "r", "k1").await;
        set_replicas(ports[0], "p3", &["a"]).await;
        eventually("a's copy to apply k1", async || {
            copy_of(ports[0], "p3")
                .await
                .is_some_and(|lp| lp.replica_applied_seqno >= first)
        })
        .await;
    });
    let _ = stops.remove(2).send(());
    rt.block_on(stopped(ports[2]));
    // A stopped process renews nothing. This rig's node c keeps its lease task while the
    // process lives, so its outbound calls are cut as well: the promote waits out the
    // others' grants to it, which a renewing owner would keep live (#2152).
    let down: Vec<_> = [ports[0], ports[1]]
        .iter()
        .map(|p| (dirs[2].clone(), format!("127.0.0.1:{p}")))
        .collect();
    super::super::CUT_BETWEEN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .extend(down.clone());
    rt.block_on(async {
        let r = within(super::super::promote::promote(
            &args_at(ports[0]),
            "p3",
            "a",
            true,
            None,
        ))
        .await
        .unwrap();
        assert!(
            r.document["lost_commits"]
                .as_str()
                .is_some_and(|s| s.starts_with("unknown")),
            "{:?}",
            r.document
        );
        let after = put_after_move(ports[1], "r", "k2").await;
        assert!(after > 0);
        let (value, _, _) = read_through(ports[1], "k1", None, None).await.unwrap();
        assert_eq!(value.as_deref(), Some(&b"v"[..]));
    });
    super::super::CUT_BETWEEN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|e| !down.contains(e));
}

/// S4 (D328 PR 3): a move of p3 onto a, which holds p3's replica, takes a out of p3's
/// replicas and closes its copy first, then moves p3 there as any move does: its rows and
/// history come along, and the copy's file went aside.
#[test]
fn a_move_onto_a_replicas_node_takes_it_out_of_the_replicas_first() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dirs, ports, _stops, _dbs) = following(&rt);
    rt.block_on(async {
        let (_, _, history_before, _) = placed(ports[1], "p3").await;
        within(super::super::moving::run(
            &args_at(ports[0]),
            "p3",
            "a",
            true,
            None,
        ))
        .await
        .unwrap();
        let (node, epoch, history, replicas) = placed(ports[1], "p3").await;
        assert_eq!((node.as_str(), epoch), ("a", 2));
        assert_eq!(history, history_before, "a move keeps p3's history");
        assert!(replicas.is_empty(), "{replicas:?}");
        let after = put_after_move(ports[1], "r", "k2").await;
        assert!(after > 0);
        let (value, _, _) = read_through(ports[1], "k1", None, None).await.unwrap();
        assert_eq!(value.as_deref(), Some(&b"v"[..]));
    });
}

/// A promotion that stopped after its stamp, before its map (a crash there): a's file is
/// at the next epoch, a primary, out of p3's replicas, and the map still gives p3 to c.
/// Run again, it finishes: no second stamp, and the map gives a the partition.
#[test]
fn a_promotion_stopped_after_its_stamp_finishes_when_run_again() {
    use crate::serve::proto::PartitionRequest;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dirs, ports, _stops, _dbs) = following(&rt);
    rt.block_on(async {
        let req = || PartitionRequest {
            partition: "p3".into(),
            ..Default::default()
        };
        client(ports[2]).await.fence(req()).await.unwrap();
        client(ports[2]).await.close(req()).await.unwrap();
        let args = args_at(ports[0]);
        super::super::promote::STOP_AFTER_STAMP
            .lock()
            .unwrap()
            .push(args.node.clone());
        let e = within(super::super::promote::promote(
            &args, "p3", "a", false, None,
        ))
        .await
        .err()
        .expect("stopped");
        assert!(
            e.message().contains("after the promote's stamp"),
            "{}",
            e.message()
        );
        let (node, epoch, _, replicas) = placed(ports[1], "p3").await;
        assert_eq!((node.as_str(), epoch), ("c", 1), "the map didn't move");
        assert!(replicas.is_empty(), "{replicas:?}");
        let r = within(super::super::promote::promote(
            &args, "p3", "a", false, None,
        ))
        .await
        .unwrap();
        assert_eq!(r.document["epoch"], 2, "{:?}", r.document);
        let (node, epoch, history, _) = placed(ports[1], "p3").await;
        assert_eq!((node.as_str(), epoch, history), ("a", 2, 2));
        // The promote waited for a to serve p3 (#2035): read at once, no retry.
        let (value, _, _) = read_through(ports[1], "k1", None, None).await.unwrap();
        assert_eq!(value.as_deref(), Some(&b"v"[..]));
    });
}

/// #2156: a promotion that stopped between the copy's promote and its identity (a crash
/// there) leaves a primary at the copy's bootstrap epoch, with the promote's intent beside
/// it. Without that intent the file is refused, as one the map moved past; with it, the
/// rerun writes the identity and finishes.
#[test]
fn a_promotion_stopped_between_its_promote_and_its_identity_finishes_when_run_again() {
    use crate::serve::proto::PartitionRequest;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (dirs, ports, _stops, _dbs) = following(&rt);
    rt.block_on(async {
        let req = || PartitionRequest {
            partition: "p3".into(),
            ..Default::default()
        };
        client(ports[2]).await.fence(req()).await.unwrap();
        client(ports[2]).await.close(req()).await.unwrap();
        let args = args_at(ports[0]);
        super::super::stamp::STOP_AFTER_PROMOTE
            .lock()
            .unwrap()
            .push(dirs[0].clone());
        let e = within(super::super::promote::promote(
            &args, "p3", "a", false, None,
        ))
        .await
        .err()
        .expect("stopped");
        assert!(
            e.message()
                .contains("between the copy's promote and its identity"),
            "{}",
            e.message()
        );
        let (node, epoch, _, _) = placed(ports[1], "p3").await;
        assert_eq!((node.as_str(), epoch), ("c", 1), "the map didn't move");
        let intent = super::super::stamp::promote_intent(&dirs[0], "p3");
        assert!(intent.exists(), "{}", intent.display());
        // Without its intent, the primary at the bootstrap epoch is refused.
        let aside = intent.with_extension("aside");
        std::fs::rename(&intent, &aside).unwrap();
        let e = within(super::super::promote::promote(
            &args, "p3", "a", false, None,
        ))
        .await
        .err()
        .expect("refused");
        assert!(e.message().contains("already a primary"), "{}", e.message());
        std::fs::rename(&aside, &intent).unwrap();
        let r = within(super::super::promote::promote(
            &args, "p3", "a", false, None,
        ))
        .await
        .unwrap();
        assert_eq!(r.document["epoch"], 2, "{:?}", r.document);
        let (node, epoch, history, _) = placed(ports[1], "p3").await;
        assert_eq!((node.as_str(), epoch, history), ("a", 2, 2));
        assert!(!intent.exists(), "the intent is removed once done");
        let (value, _, _) = read_through(ports[1], "k1", None, None).await.unwrap();
        assert_eq!(value.as_deref(), Some(&b"v"[..]));
    });
}

/// simp724's M1 on #2007: a promote that stopped after its stamp doesn't overwrite a map
/// someone changed since. Here a set-map put a back in p3's replicas: the rerun refuses,
/// says what it found, and the map stays as that set-map left it.
#[test]
fn a_promotion_doesnt_overwrite_a_map_changed_since_its_stamp() {
    use crate::serve::proto::PartitionRequest;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dirs, ports, _stops, _dbs) = following(&rt);
    rt.block_on(async {
        let req = || PartitionRequest {
            partition: "p3".into(),
            ..Default::default()
        };
        client(ports[2]).await.fence(req()).await.unwrap();
        client(ports[2]).await.close(req()).await.unwrap();
        let args = args_at(ports[0]);
        super::super::promote::STOP_AFTER_STAMP
            .lock()
            .unwrap()
            .push(args.node.clone());
        let e = within(super::super::promote::promote(
            &args, "p3", "a", false, None,
        ))
        .await
        .err()
        .expect("stopped");
        assert!(
            e.message().contains("after the promote's stamp"),
            "{}",
            e.message()
        );
        set_replicas(ports[0], "p3", &["a"]).await;
        let e = within(super::super::promote::promote(
            &args, "p3", "a", false, None,
        ))
        .await
        .err()
        .expect("the map changed");
        assert!(e.message().contains("the map changed"), "{}", e.message());
        let (node, epoch, _, replicas) = placed(ports[1], "p3").await;
        assert_eq!((node.as_str(), epoch), ("c", 1));
        assert_eq!(replicas, vec!["a".to_owned()]);
    });
}

/// simp724's M1 on #2007, the rerun's side: a promote stopped after its stamp, then a
/// set-map moved p3's epoch on (p3 still on c). The rerun reads the new epoch, so a's file
/// looks like an unstamped copy at it; the stamp refuses a file that's already a primary, and
/// the map stays as the set-map left it.
#[test]
fn a_promotion_rerun_refuses_once_the_map_moved_past_its_stamp() {
    use crate::serve::proto::PartitionRequest;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dirs, ports, _stops, _dbs) = following(&rt);
    rt.block_on(async {
        let req = || PartitionRequest {
            partition: "p3".into(),
            ..Default::default()
        };
        client(ports[2]).await.fence(req()).await.unwrap();
        client(ports[2]).await.close(req()).await.unwrap();
        let args = args_at(ports[0]);
        super::super::promote::STOP_AFTER_STAMP
            .lock()
            .unwrap()
            .push(args.node.clone());
        let e = within(super::super::promote::promote(
            &args, "p3", "a", false, None,
        ))
        .await
        .err()
        .expect("stopped");
        assert!(
            e.message().contains("after the promote's stamp"),
            "{}",
            e.message()
        );
        // A set-map: p3's epoch 1 -> 2, still on c.
        let mut m = client(ports[0])
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        let expected = m.map_version;
        m.map_version += 1;
        for q in m.partitions.iter_mut().filter(|q| q.id == "p3") {
            q.epoch += 1;
        }
        client(ports[0])
            .await
            .set_map(SetMapRequest {
                force: false,
                expected_map_version: expected,
                map: Some(m),
                copies: Vec::new(),
            })
            .await
            .unwrap();
        eventually("a to hold p3 at epoch 2 in its map", async || {
            placed(ports[1], "p3").await.1 == 2
        })
        .await;
        let e = within(super::super::promote::promote(
            &args, "p3", "a", false, None,
        ))
        .await
        .err()
        .expect("a's file is already a primary");
        assert!(e.message().contains("already a primary"), "{}", e.message());
        let (node, epoch, _, replicas) = placed(ports[1], "p3").await;
        assert_eq!((node.as_str(), epoch), ("c", 2), "the set-map's map stays");
        assert!(replicas.is_empty(), "{replicas:?}");
    });
}

/// #2035: a promote returns only once the promoted node serves the partition, as a move
/// does. a's open of p3 is held for a second (the seam): the promote is still waiting when
/// the hold is let go, and a read on a right after it returns needs no retry.
#[test]
fn a_promotion_returns_once_the_new_owner_serves() {
    use crate::serve::proto::PartitionRequest;
    use std::sync::atomic::{AtomicBool, Ordering};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (dirs, ports, _stops, _dbs) = following(&rt);
    let released = std::sync::Arc::new(AtomicBool::new(false));
    rt.block_on(async {
        let req = || PartitionRequest {
            partition: "p3".into(),
            ..Default::default()
        };
        client(ports[2]).await.fence(req()).await.unwrap();
        client(ports[2]).await.close(req()).await.unwrap();
        let (release, wait) = std::sync::mpsc::channel::<()>();
        super::super::open::HOLD_OPEN
            .lock()
            .unwrap()
            .push((dirs[0].clone(), wait));
        let flag = released.clone();
        let holder = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(1));
            flag.store(true, Ordering::SeqCst);
            let _ = release.send(());
        });
        let r = within(super::super::promote::promote(
            &args_at(ports[0]),
            "p3",
            "a",
            false,
            None,
        ))
        .await
        .unwrap();
        assert!(
            released.load(Ordering::SeqCst),
            "the promote returned while a's open of p3 was held: {:?}",
            r.document
        );
        holder.join().unwrap();
        let (value, _, _) = read_through(ports[0], "k1", None, None).await.unwrap();
        assert_eq!(value.as_deref(), Some(&b"v"[..]));
    });
}

/// #2035: with `--timeout`, a promote whose node doesn't serve in time gives up, says what
/// it waited for, and leaves the map giving that node the partition (as a move's wait does).
#[test]
fn a_promotion_gives_up_waiting_at_its_timeout() {
    use crate::serve::proto::PartitionRequest;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (dirs, ports, _stops, _dbs) = following(&rt);
    rt.block_on(async {
        let req = || PartitionRequest {
            partition: "p3".into(),
            ..Default::default()
        };
        client(ports[2]).await.fence(req()).await.unwrap();
        client(ports[2]).await.close(req()).await.unwrap();
        let (release, wait) = std::sync::mpsc::channel::<()>();
        super::super::open::HOLD_OPEN
            .lock()
            .unwrap()
            .push((dirs[0].clone(), wait));
        let e = within(super::super::promote::promote(
            &args_at(ports[0]),
            "p3",
            "a",
            false,
            Some(Duration::from_secs(1)),
        ))
        .await
        .err()
        .expect("a's open of p3 is held past the timeout");
        assert!(
            e.message()
                .contains("gave up waiting for node \"a\" to serve p3 at epoch 2"),
            "{}",
            e.message()
        );
        let (node, epoch, _, _) = placed(ports[1], "p3").await;
        assert_eq!((node.as_str(), epoch), ("a", 2), "the map gives a p3");
        drop(release);
        eventually("a to serve p3 once the hold is let go", async || {
            read_through(ports[0], "k1", None, None)
                .await
                .is_ok_and(|(value, _, _)| value.as_deref() == Some(&b"v"[..]))
        })
        .await;
    });
}

/// Every row of a scan of `r` through node `port`, with the staleness bound, a page at most
/// `page_bytes`: the rows' keys in order, and each page's `served_seqno`.
async fn scan_through(port: u16, max_lag_ms: u64, page_bytes: u64) -> (Vec<Vec<u8>>, Vec<u64>) {
    let mut req = tonic::Request::new(crate::serve::proto::ScanRequest {
        table: "r".into(),
        page_bytes,
        ..Default::default()
    });
    req.metadata_mut().insert(
        crate::serve::replica::READ_REPLICA_MAX_LAG_MS,
        max_lag_ms.to_string().parse().unwrap(),
    );
    let mut stream = data(port).await.scan(req).await.unwrap().into_inner();
    let (mut keys, mut served) = (Vec::new(), Vec::new());
    while let Some(page) = stream.message().await.unwrap() {
        served.push(page.served_seqno);
        for row in page.rows {
            if keys.last() != Some(&row.key.to_vec()) {
                keys.push(row.key.to_vec());
            }
        }
    }
    (keys, served)
}

/// D328 PR 2b: a scan sent with the bound reads p3's pages from its replica, each page
/// with the seqno it was read at; and when the replica falls behind partway (the seam),
/// the rest of p3 comes from the owner, from the last page's position: every row once, in
/// order.
#[test]
fn a_bounded_scan_reads_a_replica_and_goes_on_at_the_owner_if_it_falls_behind() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (dirs, ports, _stops, _dbs) = following(&rt);
    rt.block_on(async {
        let mut last = 0;
        for i in 0..40 {
            last = put_in(ports[1], "r", &format!("k{i:03}")).await;
        }
        eventually("a's copy to apply the rows", async || {
            copy_of(ports[0], "p3")
                .await
                .is_some_and(|lp| lp.replica_applied_seqno >= last)
        })
        .await;
        let want: Vec<Vec<u8>> = (0..40).map(|i| format!("k{i:03}").into_bytes()).collect();
        // k1 from `following` sorts first.
        let want = std::iter::once(b"k1".to_vec())
            .chain(want)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let (keys, served) = scan_through(ports[1], 60_000, 256).await;
        assert_eq!(keys, want);
        assert!(served.iter().any(|&s| s >= last), "{served:?}");
        // The replica falls behind after 2 pages: the rest is the owner's.
        crate::serve::service::BEHIND_AFTER_PAGES
            .lock()
            .unwrap()
            .push((dirs[0].clone(), 2));
        let (keys, _) = scan_through(ports[1], 60_000, 256).await;
        assert_eq!(keys, want, "every row once, in order");
    });
    crate::serve::service::BEHIND_AFTER_PAGES
        .lock()
        .unwrap()
        .retain(|(d, _)| *d != dirs[0]);
}

/// D328 PR 2b: with p3's owner (c) stopped, reads that allow staleness still answer from
/// p3's replica on a (fresh within the bound for a while yet): a multi-get's part, a scan's
/// pages and a get. The same multi-get without the header can't be served.
#[test]
fn bounded_reads_answer_from_the_replica_while_the_owner_is_down() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dirs, ports, mut stops, _dbs) = following(&rt);
    // Fresh first (a bounded read is the replica's), then the owner goes.
    rt.block_on(eventually("a's copy to be fresh", async || {
        read_through(ports[1], "k1", Some(60_000), None)
            .await
            .is_ok_and(|(_, by, _)| by.as_deref() == Some("replica"))
    }));
    let _ = stops.remove(2).send(());
    rt.block_on(stopped(ports[2]));
    rt.block_on(async {
        let multi_get = |bound: bool| {
            let mut req = tonic::Request::new(crate::serve::proto::MultiGetRequest {
                table: "r".into(),
                keys: vec![b"k1".to_vec().into()],
                ..Default::default()
            });
            if bound {
                req.metadata_mut().insert(
                    crate::serve::replica::READ_REPLICA_MAX_LAG_MS,
                    "60000".parse().unwrap(),
                );
            }
            req
        };
        let got = data(ports[1])
            .await
            .multi_get(multi_get(true))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(got.rows.len(), 1, "{got:?}");
        let (keys, _) = scan_through(ports[1], 60_000, 4096).await;
        assert_eq!(keys, vec![b"k1".to_vec()]);
        let (value, by, _) = read_through(ports[1], "k1", Some(60_000), None)
            .await
            .unwrap();
        assert_eq!(
            (value.as_deref(), by.as_deref()),
            (Some(&b"v"[..]), Some("replica"))
        );
        assert!(
            data(ports[1])
                .await
                .multi_get(multi_get(false))
                .await
                .is_err(),
            "without the header it's the stopped owner's"
        );
    });
}

/// simp724's S1 on #2001: a scan never goes back in time across a source change. a and b
/// both hold p3's replica; b's copy stops applying (a seam: still fresh within the read's
/// bound), and rows written then are a's only. a falls behind
/// after 2 pages (the seam): b's first page would be older than what a sent, so b is left
/// too, and the owner finishes. Every row once, in order, and no page older than one before.
#[test]
fn a_scan_never_goes_back_in_time_across_a_source_change() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (dirs, ports, _stops, _dbs) = following(&rt);
    rt.block_on(async {
        set_replicas(ports[0], "p3", &["a", "b"]).await;
        let first = put_in(ports[2], "r", "k0").await;
        eventually("b's copy to apply k0", async || {
            copy_of(ports[1], "p3")
                .await
                .is_some_and(|lp| lp.replica_applied_seqno >= first)
        })
        .await;
        // Fresh first: an idle owner's heartbeats name the frontier b has applied through.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        // b's copy applies nothing from here: rows after it are a's (and c's) only.
        crate::serve::replica::HOLD_APPLIES
            .lock()
            .unwrap()
            .push(dirs[1].clone());
        let mut last = 0;
        for i in 0..40 {
            last = put_in(ports[2], "r", &format!("k{i:03}")).await;
        }
        eventually("a's copy to apply the rows", async || {
            copy_of(ports[0], "p3")
                .await
                .is_some_and(|lp| lp.replica_applied_seqno >= last)
        })
        .await;
        crate::serve::service::BEHIND_AFTER_PAGES
            .lock()
            .unwrap()
            .push((dirs[0].clone(), 2));
        let (keys, served) = scan_through(ports[2], 60_000, 256).await;
        let want: std::collections::BTreeSet<Vec<u8>> = std::iter::once(b"k0".to_vec())
            .chain(std::iter::once(b"k1".to_vec()))
            .chain((0..40).map(|i| format!("k{i:03}").into_bytes()))
            .collect();
        assert_eq!(keys, want.into_iter().collect::<Vec<_>>());
        assert!(
            served.windows(2).all(|w| w[0] <= w[1] || w[1] == 0),
            "a page older than one before it: {served:?}"
        );
    });
    crate::serve::replica::HOLD_APPLIES
        .lock()
        .unwrap()
        .retain(|d| *d != dirs[1]);
    crate::serve::service::BEHIND_AFTER_PAGES
        .lock()
        .unwrap()
        .retain(|(d, _)| *d != dirs[0]);
}

/// D331 (cli692's M2): a range move's hand-over raises its source's epoch on the same node
/// and keeps its history, so the source's replica copy keeps following it, with no second
/// bootstrap. Each step through the writer's `SetMap`, which checks it as a change.
#[test]
fn a_hand_over_keeps_the_sources_replica_following_without_a_bootstrap() {
    use crate::serve::proto::{MoveKind, MovePhase, RangeMove, SplitKey, split_key};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (dirs, ports, _stops, _dbs) = following(&rt);
    assert_eq!(bootstraps_in(&dirs[0]), 1, "the first bootstrap");
    let key = |s: &str| SplitKey {
        key: Some(split_key::Key::Text(s.into())),
    };
    rt.block_on(async {
        let a = ports[0];
        // `r`'s range from "m" on, from p3 (c's) to p2 (b's).
        step(a, &|m, _| {
            m.moves.push(RangeMove {
                id: "mv1".into(),
                kind: MoveKind::Range as i32,
                table: "r".into(),
                start: Some(key("m")),
                end: None,
                from: "p3".into(),
                to: "p2".into(),
                phase: MovePhase::Copying as i32,
                ..Default::default()
            });
        })
        .await;
        for phase in [MovePhase::CatchingUp, MovePhase::HandingOver] {
            step(a, &|m, _| m.moves[0].phase = phase as i32).await;
        }
        step(a, &|m, version| {
            let t = m.tables.iter_mut().find(|t| t.name == "r").unwrap();
            *t = super::super::map::handed_over(t, b"m", None, "p2");
            let p3 = m.partitions.iter_mut().find(|p| p.id == "p3").unwrap();
            p3.epoch += 1;
            m.moves[0].phase = MovePhase::HandedOver as i32;
            m.moves[0].handed_over_term = m.writer_term;
            m.moves[0].handed_over_version = version;
        })
        .await;
        // p3 is at its new epoch on c, its history kept, and takes writes again.
        let after = put_after_move(a, "r", "k2").await;
        eventually_within(
            Duration::from_secs(30),
            "a's replica of p3 to follow c past a write at its new epoch",
            async || {
                copy_of(a, "p3").await.is_some_and(|lp| {
                    lp.replica_state() == ReplicaState::Following
                        && lp.replica_of == "c"
                        && lp.replica_applied_seqno >= after
                })
            },
        )
        .await;
        let m = client(a)
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        let p3 = m.partitions.iter().find(|p| p.id == "p3").unwrap();
        assert!(
            p3.epoch > p3.history_epoch,
            "{p3:?}: history kept across the raise"
        );
    });
    assert_eq!(
        bootstraps_in(&dirs[0]),
        1,
        "it went on following: no bootstrap since"
    );
}

/// Sets the writer's map, changed by `f` (given the new version), retried while a node
/// is still taking its peers' maps.
async fn step(a: u16, f: &dyn Fn(&mut ClusterMap, u64)) {
    let mut last = None;
    for _ in 0..100 {
        let mut m = client(a)
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        let expected = m.map_version;
        m.map_version += 1;
        f(&mut m, expected + 1);
        match client(a)
            .await
            .set_map(SetMapRequest {
                force: false,
                expected_map_version: expected,
                map: Some(m),
                copies: Vec::new(),
            })
            .await
        {
            Ok(_) => return,
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("set the map: {last:?}");
}

/// D331 PR 3 (simp724's S4, cli692's M1): a move's rows reach the replica copies of both its
/// sides through their followers. A copy of the source gets its moved-away row (and keeps
/// the range's span from the map until it does), and a copy of the target its serving row,
/// which it serves the range from.
#[test]
fn a_moves_rows_reach_the_copies_of_both_its_sides() {
    use crate::serve::proto::{MoveKind, MovePhase, RangeMove, SplitKey, split_key};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (dirs, ports, _stops, _dbs) = following(&rt);
    let key = |s: &str| SplitKey {
        key: Some(split_key::Key::Text(s.into())),
    };
    let rows_in = |dir: &Path| {
        crate::serve::replica::ARMED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .rev()
            .find(|(p, _, _)| p.starts_with(dir))
            .map(|(_, moved, serving)| (*moved, *serving))
    };
    rt.block_on(async {
        let (a, c) = (ports[0], ports[2]);
        // c holds a copy of p2 (b's): the target's.
        set_replicas(a, "p2", &["c"]).await;
        eventually_within(
            Duration::from_secs(30),
            "c's replica of p2 to follow b",
            async || {
                copy_of(c, "p2")
                    .await
                    .is_some_and(|lp| lp.replica_state() == ReplicaState::Following)
            },
        )
        .await;
        // `r` from "m" on, from p3 (c's; a holds its copy) to p2.
        step(a, &|m, _| {
            m.moves.push(RangeMove {
                id: "mv1".into(),
                kind: MoveKind::Range as i32,
                table: "r".into(),
                start: Some(key("m")),
                end: None,
                from: "p3".into(),
                to: "p2".into(),
                phase: MovePhase::Copying as i32,
                ..Default::default()
            });
        })
        .await;
        for phase in [MovePhase::CatchingUp, MovePhase::HandingOver] {
            step(a, &|m, _| m.moves[0].phase = phase as i32).await;
        }
        step(a, &|m, version| {
            let t = m.tables.iter_mut().find(|t| t.name == "r").unwrap();
            *t = super::super::map::handed_over(t, b"m", None, "p2");
            let p3 = m.partitions.iter_mut().find(|p| p.id == "p3").unwrap();
            p3.epoch += 1;
            m.moves[0].phase = MovePhase::HandedOver as i32;
            m.moves[0].handed_over_term = m.writer_term;
            m.moves[0].handed_over_version = version;
        })
        .await;
        step(a, &|m, _| m.moves[0].phase = MovePhase::Cleaning as i32).await;
        step(a, &|m, _| m.moves.clear()).await;
        // Writes on both owners move their copies' followers past the rows.
        put_after_move(a, "r", "k2").await;
        put_after_move(a, "r", "zz").await;
        eventually_within(
            Duration::from_secs(30),
            "a's copy of p3 to hold the moved-away row, c's of p2 the serving row",
            async || {
                rows_in(&dirs[0]).is_some_and(|(moved, _)| moved == 1)
                    && rows_in(&dirs[2]).is_some_and(|(_, serving)| serving == 1)
            },
        )
        .await;
    });
}

/// #2154 (the coordinator's ruling): a target's failover past its move's commit point goes
/// only to a replica copy that provably holds the copied range. A copy of the target reports,
/// in `Status`, what its own progress rows show: the copy done, and the source's seqno the
/// catch-up passed. The target writes those rows in the commits that carry the data, so
/// `target_copy_holds` accepts a copy caught up through the move's `final_seqno`, and
/// refuses one short of it.
#[test]
fn a_targets_copy_reports_its_moves_progress_from_its_own_rows() {
    use crate::serve::proto::{MoveKind, MovePhase, RangeMove, SplitKey, split_key};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dirs, ports, _stops, _dbs) = following(&rt);
    let key = |s: &str| SplitKey {
        key: Some(split_key::Key::Text(s.into())),
    };
    rt.block_on(async {
        let (a, c) = (ports[0], ports[2]);
        // c holds a copy of p2 (b's), the move's target.
        set_replicas(a, "p2", &["c"]).await;
        eventually_within(
            Duration::from_secs(30),
            "c's replica of p2 to follow b",
            async || {
                copy_of(c, "p2")
                    .await
                    .is_some_and(|lp| lp.replica_state() == ReplicaState::Following)
            },
        )
        .await;
        put_after_move(a, "r", "zz").await;
        // `r` from "m" on, from p3 (c's) into p2: copied, then caught up.
        step(a, &|m, _| {
            m.moves.push(RangeMove {
                id: "mv1".into(),
                kind: MoveKind::Range as i32,
                table: "r".into(),
                start: Some(key("m")),
                end: None,
                from: "p3".into(),
                to: "p2".into(),
                phase: MovePhase::Copying as i32,
                ..Default::default()
            });
        })
        .await;
        put_after_move(a, "r", "zy").await;
        // The owner's follower first: b copies and catches up past the write.
        let owner = async || {
            client(ports[1])
                .await
                .status(StatusRequest::default())
                .await
                .unwrap()
                .into_inner()
                .partitions
                .into_iter()
                .find(|lp| {
                    lp.id == "p2" && lp.role() != crate::serve::proto::PartitionRole::Replica
                })
                .and_then(|lp| lp.moves.into_iter().find(|s| s.id == "mv1"))
        };
        eventually_within(
            Duration::from_secs(60),
            "b's follower to copy mv1",
            async || owner().await.is_some_and(|s| s.copy_done),
        )
        .await;
        // A write in the range past the copy: its seqno is past every chunk's snapshot, so the
        // catch-up commits it, with its token row (#2209's linux red: a write the copy's
        // snapshot already held left the catch-up nothing to commit, and its row is stored
        // only with a commit, so the replica's row stayed behind the owner's in-memory count).
        let source = put_after_move(a, "r", "zx").await;
        eventually_within(
            Duration::from_secs(60),
            "b's follower to catch up past the write",
            async || {
                owner()
                    .await
                    .is_some_and(|s| s.copy_done && s.source_applied_seqno >= source)
            },
        )
        .await;
        let shown = async || {
            copy_of(c, "p2")
                .await
                .and_then(|lp| lp.moves.into_iter().find(|s| s.id == "mv1"))
        };
        eventually_within(
            Duration::from_secs(60),
            "c's copy of p2 to show mv1's copy done and caught up past the write",
            async || {
                shown()
                    .await
                    .is_some_and(|s| s.copy_done && s.source_applied_seqno >= source)
            },
        )
        .await;
        let row = copy_of(c, "p2").await.unwrap();
        let mut m = client(a)
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        let mv1 = m.moves.iter().position(|r| r.id == "mv1").unwrap();
        m.moves[mv1].phase = MovePhase::HandedOver as i32;
        m.moves[mv1].final_seqno = source;
        assert_eq!(
            super::super::promote::target_copy_holds(&m, "p2", &row),
            Ok(())
        );
        // A final seqno the copy hasn't passed: no.
        m.moves[mv1].final_seqno = shown().await.unwrap().source_applied_seqno + 1_000;
        assert!(super::super::promote::target_copy_holds(&m, "p2", &row).is_err());
        step(a, &|m, _| m.moves.clear()).await;
    });
}

/// Node `port`'s own row for `p` (not a replica copy's), if it reports one.
async fn owned_row(port: u16, p: &str) -> Option<LocalPartition> {
    client(port)
        .await
        .status(StatusRequest::default())
        .await
        .unwrap()
        .into_inner()
        .partitions
        .into_iter()
        .find(|lp| lp.id == p && lp.role == PartitionRole::Owner as i32)
}

/// Node `port`'s `/metrics` text, through `Admin.Metrics`.
async fn metrics_at(port: u16) -> String {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    crate::serve::proto::admin_client::AdminClient::new(channel)
        .metrics(crate::serve::proto::Empty {})
        .await
        .unwrap()
        .into_inner()
        .text
}

#[test]
fn a_replica_copy_acknowledges_what_it_applied_to_its_owner() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dirs, ports, _stops, _dbs) = following(&rt);
    rt.block_on(async {
        // A write on c's p3 after the copy follows: the copy applies it and acknowledges it
        // (D335 PR 1), and c's row says so.
        let second = put_in(ports[2], "r", "k2").await;
        eventually_why(
            FOLLOW,
            "c to hold a's acknowledgement of the write",
            async || {
                let owner = owned_row(ports[2], "p3").await;
                let copy = copy_of(ports[0], "p3").await;
                let acked = owner
                    .as_ref()
                    .and_then(|lp| lp.acked_position)
                    .map_or(0, |p| p.seqno);
                let applied = copy
                    .as_ref()
                    .and_then(|lp| lp.replica_applied_position)
                    .map_or(0, |p| p.seqno);
                let healthy = owner.as_ref().map_or(0, |lp| lp.healthy_replicas);
                if healthy == 1 && acked >= second && applied >= second {
                    return Ok(());
                }
                Err(format!(
                    "past {second}: c's acked {acked} with {healthy} healthy; the copy's \
                 position {applied}, {}",
                    copy_why(copy.as_ref())
                ))
            },
        )
        .await;
        // Never past what the copy stored.
        let owner = owned_row(ports[2], "p3").await.unwrap();
        let copy = copy_of(ports[0], "p3").await.unwrap();
        assert!(
            owner.acked_position.unwrap().seqno <= copy.replica_applied_seqno,
            "{owner:?} {copy:?}"
        );
        // /metrics on the owner: the replica's gauge, and the write timed to its ack.
        let text = metrics_at(ports[2]).await;
        assert!(
            text.contains("pigeonhole_cluster_replica_acked_seqno{partition=\"p3\",replica=\"a\"}"),
            "{text}"
        );
        assert!(
            text.contains("pigeonhole_cluster_replica_ack_wait_seconds_count{partition=\"p3\"}"),
            "{text}"
        );
        // The map drops the copy: its stream ends, and nothing counts.
        set_replicas(ports[0], "p3", &[]).await;
        eventually_why(FOLLOW, "c to count no replica of p3", async || {
            let owner = owned_row(ports[2], "p3").await;
            match owner {
                Some(lp) if lp.healthy_replicas == 0 && lp.acked_position.is_none() => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })
        .await;
    });
}

#[test]
fn a_replica_copy_of_an_owner_without_acknowledgements_follows_as_before() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dirs = [test_dir(), test_dir(), test_dir()];
    // c, p3's owner, acts as a build before D335: FollowAcked is UNIMPLEMENTED there.
    super::super::OLD_FOLLOW
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(dirs[2].clone());
    let (_stops, ports, _dbs) = three_nodes_in(&rt, &dirs);
    rt.block_on(async {
        table_on_p3(ports[0]).await;
        set_replicas(ports[0], "p3", &["a"]).await;
        let first = put_in(ports[1], "r", "k1").await;
        eventually_why(
            FOLLOW,
            "a's replica of p3 to follow c with Follow",
            async || {
                let lp = copy_of(ports[0], "p3").await;
                if lp.as_ref().is_some_and(|lp| {
                    lp.replica_state() == ReplicaState::Following
                        && lp.replica_applied_seqno >= first
                }) {
                    return Ok(());
                }
                Err(format!("past {first}: {}", copy_why(lp.as_ref())))
            },
        )
        .await;
        // It applies, but acknowledges nothing: c counts no replica.
        let owner = owned_row(ports[2], "p3").await.unwrap();
        assert_eq!(
            (owner.healthy_replicas, owner.acked_position),
            (0, None),
            "{owner:?}"
        );
    });
    super::super::OLD_FOLLOW
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|d| *d != dirs[2]);
}

/// Sets `p`'s `replica_acks` to `k` through the writer at `a` (D335 PR 2), retried while a
/// node that just started is still taking its peers' maps. `InvalidArgument` is the map's
/// refusal, answered at once.
pub(super) async fn set_acks(a: u16, p: &str, k: u32) -> Result<(), tonic::Status> {
    let mut last = None;
    for _ in 0..100 {
        let mut m = client(a)
            .await
            .get_map(GetMapRequest {})
            .await
            .unwrap()
            .into_inner();
        let expected = m.map_version;
        m.map_version += 1;
        for q in m.partitions.iter_mut().filter(|q| q.id == p) {
            q.replica_acks = k;
        }
        match client(a)
            .await
            .set_map(SetMapRequest {
                force: false,
                expected_map_version: expected,
                map: Some(m),
                copies: Vec::new(),
            })
            .await
        {
            Ok(_) => return Ok(()),
            Err(e) if e.code() == tonic::Code::InvalidArgument => return Err(e),
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(last.unwrap())
}

/// A write of row `row` in table `r` through node `port`, with the request's `replication`
/// and `durability`, and `x-pigeonhole-replication` set to `header` if any: its commit.
async fn write_acked(
    port: u16,
    row: &str,
    replication: crate::serve::proto::ReplicationMode,
    durability: crate::serve::proto::Durability,
    header: Option<&str>,
) -> Result<crate::serve::proto::CommitInfo, tonic::Status> {
    let mut w = put_row(row, "v");
    w.row.as_mut().unwrap().table = "r".into();
    let mut req = tonic::Request::new(crate::serve::proto::MutateRequest {
        writes: Some(w),
        durability: durability as i32,
        replication: replication as i32,
    });
    if let Some(h) = header {
        req.metadata_mut()
            .insert(crate::serve::acks::REPLICATION_HEADER, h.parse().unwrap());
    }
    data(port)
        .await
        .mutate(req)
        .await
        .map(|r| r.into_inner().commit.unwrap())
}

/// The API error code a status carries.
fn code_of(s: &tonic::Status) -> String {
    crate::serve::errors::ApiError::from_status(s).map_or_else(|| format!("{s:?}"), |e| e.code)
}

#[test]
fn a_write_on_a_partition_with_replica_acks_answers_once_its_replica_acknowledged_it() {
    use crate::serve::proto::{Durability, ReplicationMode};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dirs, ports, _stops, _dbs) = following(&rt);
    rt.block_on(async {
        // More acknowledgements than p3 has replicas: the map is refused.
        let e = set_acks(ports[0], "p3", 2).await.unwrap_err();
        assert!(e.message().contains("replica count, 1"), "{e:?}");
        set_acks(ports[0], "p3", 1).await.unwrap();
        // Through b, which forwards it to c: answered once a acknowledged it, so c's
        // frontier is already past it. `None` is raised to `GroupSync`.
        let c = write_acked(
            ports[1],
            "k2",
            ReplicationMode::Default,
            Durability::None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(c.replica_acks, 1, "{c:?}");
        assert_eq!(c.durability, Durability::GroupSync as i32, "{c:?}");
        let owner = owned_row(ports[2], "p3").await.unwrap();
        assert!(
            owner.acked_position.unwrap().seqno >= c.seqno,
            "{owner:?} {c:?}"
        );
        assert_eq!(owner.healthy_replicas, 1, "{owner:?}");
        // Opted down, by the request's field or by the header through b's forward: answered
        // without the wait, its durability as asked.
        let c = write_acked(
            ports[1],
            "k3",
            ReplicationMode::Async,
            Durability::None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            (c.replica_acks, c.durability),
            (0, Durability::None as i32),
            "{c:?}"
        );
        let c = write_acked(
            ports[1],
            "k4",
            ReplicationMode::Default,
            Durability::Unspecified,
            Some("ASYNC"),
        )
        .await
        .unwrap();
        assert_eq!(c.replica_acks, 0, "{c:?}");
        let e = write_acked(
            ports[1],
            "k5",
            ReplicationMode::Default,
            Durability::Unspecified,
            Some("sometimes"),
        )
        .await
        .unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e:?}");
        // A transaction's commit waits too, before its outcome is recorded; one that opted
        // down doesn't.
        for (row, replication, want) in [
            ("t1", ReplicationMode::Default, 1),
            ("t2", ReplicationMode::Async, 0),
        ] {
            let id = txns(ports[1])
                .await
                .begin(crate::serve::proto::BeginRequest {
                    durability: 0,
                    route: Some(crate::serve::proto::RoutingKey {
                        key: Some(crate::serve::proto::routing_key::Key::Row(
                            crate::serve::proto::RowKey {
                                table: "r".into(),
                                row: row.as_bytes().to_vec().into(),
                            },
                        )),
                    }),
                    spans_partitions: false,
                })
                .await
                .unwrap()
                .into_inner()
                .txn_id;
            let mut w = put_row(row, "v");
            w.row.as_mut().unwrap().table = "r".into();
            let c = txns(ports[1])
                .await
                .commit(crate::serve::proto::TxnCommitRequest {
                    txn_id: id,
                    rows: vec![w],
                    replication: replication as i32,
                })
                .await
                .unwrap()
                .into_inner()
                .commit
                .unwrap();
            assert_eq!(c.replica_acks, want, "{row}: {c:?}");
        }
        // A map install that doesn't touch p3's replicas leaves a's stream counted (A1):
        // the next write is acknowledged without a's follow starting again.
        set_acks(ports[0], "p3", 1).await.unwrap();
        let c = write_acked(
            ports[2],
            "k6",
            ReplicationMode::Default,
            Durability::Unspecified,
            None,
        )
        .await
        .unwrap();
        assert_eq!(c.replica_acks, 1, "{c:?}");
        let text = metrics_at(ports[2]).await;
        for counter in [
            "pigeonhole_cluster_replica_ack_timeouts_total{partition=\"p3\"} 0",
            "pigeonhole_cluster_replica_ack_refused_total{partition=\"p3\"} 0",
        ] {
            assert!(text.contains(counter), "{counter}: {text}");
        }
    });
}

#[test]
fn a_write_whose_replica_stops_acknowledging_answers_unknown_and_later_ones_are_refused() {
    use crate::serve::proto::{Durability, ReplicationMode};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (dirs, ports, _stops, _dbs) = following(&rt);
    let write = async |row: &str| {
        write_acked(
            ports[2],
            row,
            ReplicationMode::Default,
            Durability::Unspecified,
            None,
        )
        .await
    };
    let hold = |on: bool| {
        let mut held = super::super::HOLD_ACKS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        held.retain(|d| *d != dirs[2]);
        if on {
            held.push(dirs[2].clone());
        }
    };
    rt.block_on(async {
        set_acks(ports[0], "p3", 1).await.unwrap();
        assert_eq!(write("k2").await.unwrap().replica_acks, 1);
        // c stops counting a's acknowledgements: a write waits for the timeout, then
        // answers `OutcomeUnknown` (it's committed on c).
        hold(true);
        let started = std::time::Instant::now();
        let e = write("k3").await.unwrap_err();
        assert_eq!(code_of(&e), "OutcomeUnknown", "{e:?}");
        assert!(
            started.elapsed() >= super::TEST_REPLICA_ACK_TIMEOUT,
            "{e:?}"
        );
        // That write stays uncovered: once it has waited past the stall window with no
        // acknowledgement, a is unhealthy and c refuses writes (REFUSE), retry-safe.
        eventually_why(FOLLOW, "c to refuse writes on p3", async || {
            match write("k4").await {
                Err(e) if code_of(&e) == "Unavailable" => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })
        .await;
        let owner = owned_row(ports[2], "p3").await.unwrap();
        assert_eq!(owner.healthy_replicas, 0, "{owner:?}");
        let text = metrics_at(ports[2]).await;
        assert!(
            text.contains("pigeonhole_cluster_replica_ack_timeouts_total{partition=\"p3\"} "),
            "{text}"
        );
        assert!(
            !text.contains("pigeonhole_cluster_replica_ack_refused_total{partition=\"p3\"} 0"),
            "{text}"
        );
        // a's acknowledgements count again: the held one covers the unknown write, a is
        // healthy, and writes are acknowledged.
        hold(false);
        eventually_why(
            FOLLOW,
            "c to take writes on p3 again",
            async || match write("k5").await {
                Ok(c) if c.replica_acks == 1 => Ok(()),
                other => Err(format!("{other:?}")),
            },
        )
        .await;
    });
    hold(false);
}
