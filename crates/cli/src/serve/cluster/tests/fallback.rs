//! D335 PR 2b: a `replica_acks` partition's ASYNC fallback (§7), through three nodes: c
//! owns p3, a follows it as its replica, and b only grants.

use super::replicas::{
    FOLLOW, code_of, copy_of, copy_why, metrics_at, owned_row, put_in, set_replicas, table_on_p3,
    three_leased, write_acked,
};
use super::*;
use crate::serve::proto::{
    DegradedState, Durability, GetMapRequest, ReplicaAckPolicy, ReplicationMode, SetMapRequest,
    StatusRequest,
};

/// The replicas tests' `following`, on [`three_leased`] nodes: real lease rounds, which
/// the fallback's records ride on (a standing test lease runs none). c owns p3, a follows
/// it as its replica, and b only grants.
fn following(
    rt: &tokio::runtime::Runtime,
) -> (
    [PathBuf; 3],
    Vec<u16>,
    Vec<tokio::sync::oneshot::Sender<()>>,
) {
    let dirs = [test_dir(), test_dir(), test_dir()];
    let (stops, ports) = three_leased(rt, &dirs);
    follow_p3(rt, &ports);
    (dirs, ports, stops)
}

/// p3 on its lease at c, and a's replica of it following.
fn follow_p3(rt: &tokio::runtime::Runtime, ports: &[u16]) {
    rt.block_on(async {
        eventually_why(
            FOLLOW,
            "c to serve p3 on its lease",
            async || match owned_row(ports[2], "p3").await {
                Some(lp) if lp.lease_remaining_ms > 0 => Ok(()),
                other => Err(format!("{other:?}")),
            },
        )
        .await;
        table_on_p3(ports[0]).await;
        let first = put_in(ports[1], "r", "k1").await;
        set_replicas(ports[0], "p3", &["a"]).await;
        eventually_why(FOLLOW, "a's replica of p3 to follow c", async || {
            let lp = copy_of(ports[0], "p3").await;
            if lp.as_ref().is_some_and(|lp| {
                lp.replica_state() == crate::serve::proto::ReplicaState::Following
                    && lp.replica_of == "c"
                    && lp.replica_applied_seqno >= first
            }) {
                return Ok(());
            }
            Err(format!("past {first}: {}", copy_why(lp.as_ref())))
        })
        .await;
    });
}

/// Three leased nodes, each on its own runtime and keeping its database, so one can be
/// stopped and started again on its port as a restarted node.
struct Restartable {
    dirs: [PathBuf; 3],
    ports: Vec<u16>,
    runtimes: Vec<Option<tokio::runtime::Runtime>>,
    stops: Vec<Option<tokio::sync::oneshot::Sender<()>>>,
    dbs: Vec<Option<pigeonhole::Pigeonhole>>,
}

fn node_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

impl Restartable {
    const IDS: [&str; 3] = ["a", "b", "c"];
    const PARTS: [&str; 3] = ["p1", "p2", "p3"];

    fn start() -> Self {
        let dirs = [test_dir(), test_dir(), test_dir()];
        let listeners: Vec<std::net::TcpListener> = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let ports: Vec<u16> = listeners
            .iter()
            .map(|l| l.local_addr().unwrap().port())
            .collect();
        let map = three_way(&[ports[0], ports[1], ports[2]]);
        let mut me = Self {
            dirs,
            ports,
            runtimes: Vec::new(),
            stops: Vec::new(),
            dbs: Vec::new(),
        };
        for (i, l) in listeners.into_iter().enumerate() {
            super::super::write_map(&me.dirs[i], &super::super::map::canonical_json(&map)).unwrap();
            let rt = node_runtime();
            let (stop, db) = data_node_full(
                &rt,
                &me.dirs[i],
                Self::IDS[i],
                Some(Self::PARTS[i]),
                l,
                "",
                crate::serve::server_tests::limits(),
                None,
                true,
            );
            me.runtimes.push(Some(rt));
            me.stops.push(Some(stop));
            me.dbs.push(db);
        }
        me
    }

    /// Stops node `i` (its server and its tasks), then starts it again on its port with
    /// its database, as a restarted node.
    fn restart(&mut self, i: usize, client: &tokio::runtime::Runtime) {
        let _ = self.stops[i].take().expect("running").send(());
        self.runtimes[i]
            .take()
            .expect("running")
            .shutdown_background();
        let port = self.ports[i];
        client.block_on(stopped(port));
        let listener = HeldPort::take(port).listen();
        listener.set_nonblocking(true).unwrap();
        let rt = node_runtime();
        let (stop, _) = data_node_full(
            &rt,
            &self.dirs[i],
            Self::IDS[i],
            Some(Self::PARTS[i]),
            listener,
            "",
            crate::serve::server_tests::limits(),
            self.dbs[i].clone(),
            true,
        );
        self.runtimes[i] = Some(rt);
        self.stops[i] = Some(stop);
    }
}

/// Sets partition `p`'s `replica_acks` and `replica_ack_policy` through node `a`, retrying
/// a map that moved meanwhile.
async fn set_policy(
    a: u16,
    p: &str,
    k: u32,
    policy: ReplicaAckPolicy,
) -> Result<(), tonic::Status> {
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
            q.replica_ack_policy = policy as i32;
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

/// Node `port`'s record of `p`'s fallback, as a grantor: its state, and where it began.
async fn record_at(port: u16, p: &str) -> (DegradedState, Option<u64>) {
    let status = client(port)
        .await
        .status(StatusRequest::default())
        .await
        .unwrap()
        .into_inner();
    status
        .degraded
        .into_iter()
        .find(|r| r.partition == p)
        .map_or((DegradedState::Unknown, None), |r| {
            (r.state(), r.degraded_from.map(|f| f.seqno))
        })
}

/// Holds (or lets go) the owner's count of its replicas' acknowledgements, as if they
/// stopped.
fn hold(dir: &Path, on: bool) {
    let mut held = super::super::HOLD_ACKS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    held.retain(|d| d != dir);
    if on {
        held.push(dir.to_owned());
    }
}

/// Fails (or lets go) the grantor's writes of its fallback records: it echoes none.
fn fail_records(dir: &Path, on: bool) {
    let mut failing = super::super::seams::FAIL_DEGRADED_WRITES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    failing.retain(|d| d != dir);
    if on {
        failing.push(dir.to_owned());
    }
}

/// §7's ASYNC, end to end: with p3's replica stopped, c answers writes without its
/// acknowledgement only once a majority of the lease's grantors durably recorded where the
/// fallback began. With a and b failing their record writes, c's own record alone isn't a
/// majority, and writes answer `OutcomeUnknown`. Once a's acknowledgements count again, the
/// fallback ends through a recorded clear, and writes are covered again.
#[test]
fn an_async_partition_answers_uncovered_only_once_a_majority_recorded_its_fallback() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (dirs, ports, _stops) = following(&rt);
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
    rt.block_on(async {
        set_policy(ports[0], "p3", 1, ReplicaAckPolicy::Async)
            .await
            .unwrap();
        assert_eq!(write("k2").await.unwrap().replica_acks, 1);
        // Every grantor recorded p3 CLEAR at its epoch, on its first grant since k = 1.
        for &port in &ports {
            eventually_why(
                FOLLOW,
                "every node to record p3 clear",
                async || match record_at(port, "p3").await {
                    (DegradedState::Clear, None) => Ok(()),
                    other => Err(format!("{other:?}")),
                },
            )
            .await;
        }
        // a and b can't record: c's record alone is no majority, so a write past the
        // stall answers OutcomeUnknown, never uncovered.
        fail_records(&dirs[0], true);
        fail_records(&dirs[1], true);
        hold(&dirs[2], true);
        let e = write("k3").await.unwrap_err();
        assert_eq!(code_of(&e), "OutcomeUnknown", "{e:?}");
        // Once a is unhealthy, the next write enters the fallback: taken, never refused,
        // and never answered uncovered while no majority recorded it.
        eventually_why(FOLLOW, "c to enter the fallback", async || {
            let w = write("k4").await;
            match &w {
                Ok(c) => panic!("answered with no majority's record: {c:?}"),
                Err(e) if code_of(e) != "OutcomeUnknown" => panic!("{e:?}"),
                Err(_) => {}
            }
            match owned_row(ports[2], "p3").await {
                Some(lp) if lp.degraded_from.is_some() => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })
        .await;
        let e = write("k4b").await.unwrap_err();
        assert_eq!(code_of(&e), "OutcomeUnknown", "no majority yet: {e:?}");
        assert_eq!(record_at(ports[0], "p3").await.0, DegradedState::Clear);
        // They record again: the next round's majority echoes it, and writes are answered
        // without the replica's acknowledgement.
        fail_records(&dirs[0], false);
        fail_records(&dirs[1], false);
        eventually_why(
            FOLLOW,
            "c to answer writes uncovered",
            async || match write("k5").await {
                Ok(c) if c.replica_acks == 0 => Ok(()),
                other => Err(format!("{other:?}")),
            },
        )
        .await;
        let from = owned_row(ports[2], "p3")
            .await
            .and_then(|lp| lp.degraded_from)
            .unwrap()
            .seqno;
        for &port in &ports {
            assert_eq!(
                record_at(port, "p3").await,
                (DegradedState::Flagged, Some(from)),
                "node at {port}"
            );
        }
        let text = metrics_at(ports[2]).await;
        assert!(
            text.contains("pigeonhole_cluster_replica_ack_degraded{partition=\"p3\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("pigeonhole_cluster_replica_ack_degraded_total{partition=\"p3\"} 1"),
            "{text}"
        );
        // a counts again: the frontier passes every write answered uncovered, the clear is
        // recorded, and writes are covered again.
        hold(&dirs[2], false);
        eventually_why(
            FOLLOW,
            "c to leave the fallback",
            async || match owned_row(ports[2], "p3").await {
                Some(lp) if lp.degraded_from.is_none() => Ok(()),
                other => Err(format!("{other:?}")),
            },
        )
        .await;
        for &port in &ports {
            eventually_why(
                FOLLOW,
                "every node to record p3 clear again",
                async || match record_at(port, "p3").await {
                    (DegradedState::Clear, None) => Ok(()),
                    other => Err(format!("{other:?}")),
                },
            )
            .await;
        }
        assert_eq!(write("k6").await.unwrap().replica_acks, 1);
    });
    hold(&dirs[2], false);
    fail_records(&dirs[0], false);
    fail_records(&dirs[1], false);
}

/// REFUSE (the default, and an explicit one) is 2a's: a short partition refuses writes,
/// and enters no fallback.
#[test]
fn a_refuse_partition_refuses_and_records_no_fallback() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (dirs, ports, _stops) = following(&rt);
    rt.block_on(async {
        set_policy(ports[0], "p3", 1, ReplicaAckPolicy::Refuse)
            .await
            .unwrap();
        hold(&dirs[2], true);
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
        let e = write("k3").await.unwrap_err();
        assert_eq!(code_of(&e), "OutcomeUnknown", "{e:?}");
        eventually_why(FOLLOW, "c to refuse writes on p3", async || {
            match write("k4").await {
                Err(e) if code_of(&e) == "Unavailable" => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })
        .await;
        let lp = owned_row(ports[2], "p3").await.unwrap();
        assert!(lp.degraded_from.is_none(), "{lp:?}");
        assert_eq!(record_at(ports[0], "p3").await.0, DegradedState::Clear);
    });
    hold(&dirs[2], false);
}

/// simp724's P1: an owner that restarts in its fallback resumes it from its own grantor
/// record, with every write it had taken as possibly answered uncovered, and ends it only
/// once its replica holds them all, through a recorded clear.
#[test]
fn an_owner_restarted_in_its_fallback_resumes_it_and_ends_it_once_covered() {
    let client = node_runtime();
    let mut nodes = Restartable::start();
    follow_p3(&client, &nodes.ports);
    let (ports, c_dir) = (nodes.ports.clone(), nodes.dirs[2].clone());
    let write = async |row: String| {
        write_acked(
            ports[2],
            &row,
            ReplicationMode::Default,
            Durability::Unspecified,
            None,
        )
        .await
    };
    let from = client.block_on(async {
        set_policy(ports[0], "p3", 1, ReplicaAckPolicy::Async)
            .await
            .unwrap();
        hold(&c_dir, true);
        let n = std::sync::atomic::AtomicUsize::new(0);
        eventually_why(FOLLOW, "c to answer writes uncovered", async || {
            let i = n.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            match write(format!("w{i}")).await {
                Ok(c) if c.replica_acks == 0 => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })
        .await;
        let (state, from) = record_at(ports[2], "p3").await;
        assert_eq!(state, DegradedState::Flagged);
        from.unwrap()
    });
    nodes.restart(2, &client);
    client.block_on(async {
        // Its own record puts it back in the fallback at open, from the same start.
        eventually_why(
            FOLLOW,
            "c to reopen p3 in its fallback",
            async || match owned_row(ports[2], "p3").await {
                Some(lp) if lp.degraded_from.as_ref().map(|f| f.seqno) == Some(from) => Ok(()),
                other => Err(format!("{other:?}")),
            },
        )
        .await;
        // a counts again. Its new stream acknowledges once something new is applied: the
        // next write waits for that, as any write once a is healthy, and covers every
        // write c had. Then the clear lands everywhere.
        hold(&c_dir, false);
        let n = std::sync::atomic::AtomicUsize::new(0);
        eventually_why(FOLLOW, "a write on c to be covered again", async || {
            let i = n.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            match write(format!("x{i}")).await {
                Ok(c) if c.replica_acks == 1 => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })
        .await;
        eventually_why(
            FOLLOW,
            "c to leave its fallback",
            async || match owned_row(ports[2], "p3").await {
                Some(lp) if lp.degraded_from.is_none() => Ok(()),
                other => Err(format!("{other:?}")),
            },
        )
        .await;
        for &port in &ports {
            eventually_why(
                FOLLOW,
                "every node to record p3 clear",
                async || match record_at(port, "p3").await {
                    (DegradedState::Clear, None) => Ok(()),
                    other => Err(format!("{other:?}")),
                },
            )
            .await;
        }
        assert_eq!(write("after".into()).await.unwrap().replica_acks, 1);
    });
    hold(&c_dir, false);
}
