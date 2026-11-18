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
        .status(StatusRequest {})
        .await
        .unwrap()
        .into_inner()
        .partitions
        .into_iter()
        .find(|lp| lp.id == p && lp.role == PartitionRole::Replica as i32)
}

/// Sets the writer's (a's) map with `p`'s replicas as `replicas`, retried while a node that
/// just started is still taking its peers' maps.
async fn set_replicas(a: u16, p: &str, replicas: &[&str]) {
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
        eventually("a's replica of p3 to follow c", async || {
            copy_of(ports[0], "p3").await.is_some_and(|lp| {
                lp.replica_state() == ReplicaState::Following
                    && lp.replica_of == "c"
                    && lp.replica_applied_seqno >= first
            })
        })
        .await;
    });
    (dirs, ports, stops, dbs)
}

/// The node args of a `phdb cluster` command through node `port`.
fn args_at(port: u16) -> super::super::cli::NodeArgs {
    super::super::cli::NodeArgs {
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
            .status(StatusRequest {})
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
    let entry = (dirs[2].clone(), format!("127.0.0.1:{}", ports[2]));
    rt.block_on(async {
        set_replicas(ports[0], "p3", &["a"]).await;
        eventually("a's replica of p3 to follow c", async || {
            copy_of(ports[0], "p3").await.is_some_and(|lp| {
                lp.replica_state() == ReplicaState::Following && lp.replica_lag_ms < 1000
            })
        })
        .await;
        super::super::CUT_OFF
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(entry.clone());
        let lease = u64::try_from(TEST_LEASE.as_millis()).unwrap();
        eventually("c's stream to end with p3's lease", async || {
            copy_of(ports[0], "p3").await.is_some_and(|lp| {
                lp.replica_state() != ReplicaState::Following && lp.replica_lag_ms > lease
            })
        })
        .await;
    });
    super::super::CUT_OFF
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|e| *e != entry);
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
    // Sets the writer's map, changed by `f` (given the new version), retried while a node
    // is still taking its peers' maps.
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
