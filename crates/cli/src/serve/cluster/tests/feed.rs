//! Cluster feeds (D317 ticket 7, §12's feeds line): a subscription through any node delivers
//! every partition's commits once each, each partition in order, with checkpoint tokens at
//! most `MAX_EVERY` events apart; a resume through another node with `skip_through` delivers
//! the rest with no repeat and no gap; a partition that goes away ends the stream, or with
//! `allow_partial` is reported and comes back where it left off; a subscriber that stops
//! reading doesn't hold up forwarded calls (simp724's S1).

use std::collections::{BTreeMap, BTreeSet};

use prost::bytes::Bytes;

use super::*;
use crate::serve::cluster::feed::MAX_EVERY;
use crate::serve::proto::{
    FeedEvent, MutateRequest, PartitionSeqno, SubscribeRequest, feed_client::FeedClient,
    feed_event::Event, subscribe_request::Start,
};

/// Nodes a (p1: users < "m"), b (p2: users >= "m") and c (routes only), on loopback, each
/// on a runtime of its own, so stopping one ends its connections as a process exit would
/// (the listener's own shutdown leaves open streams running). The test's client runs on
/// `rt`.
struct Three {
    rt: tokio::runtime::Runtime,
    nodes: Vec<Option<tokio::runtime::Runtime>>,
    ports: Vec<u16>,
    dirs: Vec<PathBuf>,
    stops: Vec<Option<tokio::sync::oneshot::Sender<()>>>,
    dbs: Vec<Option<pigeonhole::Pigeonhole>>,
}

const IDS: [&str; 3] = ["a", "b", "c"];
const PARTS: [Option<&str>; 3] = [Some("p1"), Some("p2"), None];

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

impl Three {
    fn start() -> Self {
        let rt = runtime();
        let listeners: Vec<std::net::TcpListener> = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let ports: Vec<u16> = listeners
            .iter()
            .map(|l| l.local_addr().unwrap().port())
            .collect();
        let mut map = the_map(1, &[ports[0], ports[1], ports[2]]);
        map.partitions.retain(|p| p.id != "p3");
        let dirs: Vec<PathBuf> = (0..3).map(|_| test_dir()).collect();
        for dir in &dirs {
            super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
        }
        let mut stops = Vec::new();
        let mut dbs = Vec::new();
        let mut nodes = Vec::new();
        for (i, l) in listeners.into_iter().enumerate() {
            let node_rt = runtime();
            let (stop, db) = data_node(&node_rt, &dirs[i], IDS[i], PARTS[i], l);
            stops.push(Some(stop));
            dbs.push(db);
            nodes.push(Some(node_rt));
        }
        Self {
            rt,
            nodes,
            ports,
            dirs,
            stops,
            dbs,
        }
    }

    /// Stops node `i`: its server and every connection and stream it had. Its database
    /// stays open, for [`Three::restart`].
    fn stop(&mut self, i: usize) {
        let _ = self.stops[i].take().expect("running").send(());
        self.nodes[i].take().expect("running").shutdown_background();
    }

    /// Serves node `i` again on its port, with its database.
    fn restart(&mut self, i: usize) {
        let addr: std::net::SocketAddr = format!("127.0.0.1:{}", self.ports[i]).parse().unwrap();
        let node_rt = runtime();
        // The old listener is gone with its runtime; its port may linger in TIME_WAIT.
        let listener = node_rt.block_on(async {
            for _ in 0..100 {
                let socket = tokio::net::TcpSocket::new_v4().unwrap();
                socket.set_reuseaddr(true).unwrap();
                if socket.bind(addr).is_ok() {
                    return socket.listen(64).unwrap().into_std().unwrap();
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            panic!("port {addr} never came free");
        });
        let (stop, _) = data_node_on(
            &node_rt,
            &self.dirs[i],
            IDS[i],
            PARTS[i],
            listener,
            crate::serve::server_tests::limits(),
            self.dbs[i].clone(),
        );
        self.stops[i] = Some(stop);
        self.nodes[i] = Some(node_rt);
    }
}

impl Drop for Three {
    fn drop(&mut self) {
        for rt in self.nodes.iter_mut().filter_map(Option::take) {
            rt.shutdown_background();
        }
        for db in self.dbs.iter_mut().filter_map(Option::take) {
            let _ = db.close();
        }
    }
}

async fn feed(port: u16) -> FeedClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    FeedClient::new(channel)
}

async fn subscribe(
    port: u16,
    req: SubscribeRequest,
) -> Result<tonic::Streaming<FeedEvent>, tonic::Status> {
    Ok(feed(port).await.subscribe(req).await?.into_inner())
}

fn users() -> SubscribeRequest {
    SubscribeRequest {
        tables: vec!["users".into()],
        ..Default::default()
    }
}

async fn put(port: u16, row: &str, value: &str) {
    data(port)
        .await
        .mutate(MutateRequest {
            writes: Some(put_row(row, value)),
            durability: 0,
        })
        .await
        .unwrap();
}

/// A commit as the client saw it.
#[derive(Debug, Clone)]
struct Seen {
    partition: String,
    seqno: u64,
    rows: Vec<String>,
    token: Option<Bytes>,
}

/// Reads until `n` commits have arrived, within 10 s each, with the other events in order.
async fn commits(s: &mut tonic::Streaming<FeedEvent>, n: usize) -> (Vec<Seen>, Vec<FeedEvent>) {
    let (mut seen, mut other) = (Vec::new(), Vec::new());
    while seen.len() < n {
        let e = tokio::time::timeout(Duration::from_secs(10), s.message())
            .await
            .unwrap_or_else(|_| panic!("only {} of {n} commits arrived", seen.len()))
            .unwrap()
            .expect("the stream ended");
        match &e.event {
            Some(Event::Commit(c)) => seen.push(Seen {
                partition: e.partition.clone(),
                seqno: c.seqno,
                rows: c
                    .changes
                    .iter()
                    .map(|ch| String::from_utf8(ch.row.to_vec()).unwrap())
                    .collect(),
                token: (!e.token.is_empty()).then(|| e.token.clone()),
            }),
            _ => other.push(e),
        }
    }
    (seen, other)
}

/// Each partition's commits arrive in increasing seqno order.
fn in_order(seen: &[Seen]) {
    let mut last: BTreeMap<&str, u64> = BTreeMap::new();
    for s in seen {
        assert!(!s.partition.is_empty(), "{s:?}");
        let prev = last.insert(&s.partition, s.seqno);
        assert!(
            prev.is_none_or(|p| p < s.seqno),
            "{s:?} after seqno {prev:?}"
        );
    }
}

/// Rows `prefix0..prefixN`, alternating between the partitions (rows < "m" are p1's).
fn rows(prefix: &str, n: usize) -> Vec<String> {
    (0..n)
        .map(|i| format!("{}{prefix}{i:03}", if i % 2 == 0 { "a" } else { "z" }))
        .collect()
}

#[test]
fn a_subscription_through_any_node_delivers_every_partitions_commits_once_each_in_order() {
    let t = Three::start();
    let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
    t.rt.block_on(async {
        // c serves no partition, so both parts are remote; a has p1 here and p2 remote.
        for entry in [c, a] {
            let mut s = subscribe(entry, users()).await.unwrap();
            let want = rows(&format!("e{entry}-"), 40);
            for (i, row) in want.iter().enumerate() {
                put([a, b, c][i % 3], row, "v").await;
            }
            let (seen, other) = commits(&mut s, want.len()).await;
            in_order(&seen);
            let got: BTreeSet<&str> = seen
                .iter()
                .flat_map(|s| s.rows.iter().map(String::as_str))
                .collect();
            assert_eq!(got, want.iter().map(String::as_str).collect(), "once each");
            for s in &seen {
                let want = if s.rows[0].as_str() < "m" { "p1" } else { "p2" };
                assert_eq!(s.partition, want, "{s:?}");
            }
            // Checkpoints: the first commit, then at most MAX_EVERY apart.
            assert!(seen[0].token.is_some(), "the first commit is a checkpoint");
            let mut gap = 0;
            for s in &seen {
                gap = if s.token.is_some() { 0 } else { gap + 1 };
                assert!(gap < MAX_EVERY, "{gap} commits without a checkpoint");
            }
            assert!(
                other
                    .iter()
                    .all(|e| matches!(e.event, Some(Event::Progress(_)))),
                "{other:?}"
            );
        }
    });
}

#[test]
fn a_resume_through_another_node_delivers_the_rest_with_no_repeat_and_no_gap() {
    let t = Three::start();
    let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
    t.rt.block_on(async {
        let first = rows("r", 60);
        for row in &first {
            put(c, row, "v").await;
        }
        // Checkpoints 8 events apart, so the client's resume has events to repeat (at this
        // test's token sizes the server's own choice is every event).
        let mut s = subscribe(
            c,
            SubscribeRequest {
                start: Some(Start::FromOldest(true)),
                checkpoint_every: 8,
                ..users()
            },
        )
        .await
        .unwrap();
        let (seen, _) = commits(&mut s, first.len()).await;
        drop(s);
        // The client handled the first 45, then stopped; its last checkpoint is at or before.
        let handled = 45;
        let at = (0..handled)
            .rev()
            .find(|&i| seen[i].token.is_some())
            .expect("a checkpoint");
        let token = seen[at].token.clone().unwrap();
        let mut last: BTreeMap<String, u64> = BTreeMap::new();
        for s in &seen[..handled] {
            last.insert(s.partition.clone(), s.seqno);
        }
        let more = rows("s", 20);
        for row in &more {
            put(a, row, "v").await;
        }
        // The rows the client hasn't handled: the oracle.
        let rest: BTreeSet<String> = seen[handled..]
            .iter()
            .flat_map(|s| s.rows.clone())
            .chain(more.iter().cloned())
            .collect();
        // With skip_through, through a: exactly the rest.
        let mut s = subscribe(
            a,
            SubscribeRequest {
                start: Some(Start::Resume(token.clone())),
                skip_through: last
                    .iter()
                    .map(|(p, &seqno)| PartitionSeqno {
                        partition: p.clone(),
                        seqno,
                    })
                    .collect(),
                ..users()
            },
        )
        .await
        .unwrap();
        let (again, _) = commits(&mut s, rest.len()).await;
        in_order(&again);
        let got: Vec<String> = again.iter().flat_map(|s| s.rows.clone()).collect();
        assert_eq!(got.len(), rest.len(), "no repeat: {got:?}");
        assert_eq!(got.into_iter().collect::<BTreeSet<_>>(), rest, "no gap");
        // Without, through b: at most MAX_EVERY repeats, all recognised by (partition, seqno).
        let mut s = subscribe(
            b,
            SubscribeRequest {
                start: Some(Start::Resume(token)),
                ..users()
            },
        )
        .await
        .unwrap();
        let repeats = handled - at - 1;
        assert!(repeats > 0 && (repeats as u64) < 8, "{repeats}");
        let (again, _) = commits(&mut s, repeats + rest.len()).await;
        in_order(&again);
        let fresh: BTreeSet<String> = again
            .iter()
            .filter(|s| last.get(&s.partition).is_none_or(|&l| s.seqno > l))
            .flat_map(|s| s.rows.clone())
            .collect();
        assert_eq!(fresh, rest, "the dedupe rule leaves exactly the rest");
    });
}

#[test]
fn a_subscription_refuses_what_it_cant_follow() {
    let t = Three::start();
    let (a, c) = (t.ports[0], t.ports[2]);
    t.rt.block_on(async {
        put(a, "alice", "1").await;
        let mut s = subscribe(
            c,
            SubscribeRequest {
                start: Some(Start::FromOldest(true)),
                ..users()
            },
        )
        .await
        .unwrap();
        let (seen, _) = commits(&mut s, 1).await;
        let token = seen[0].token.clone().unwrap();
        let bad: Vec<(SubscribeRequest, &str)> = vec![
            (
                SubscribeRequest {
                    tables: vec!["nope".into()],
                    ..Default::default()
                },
                "NotFound",
            ),
            (
                SubscribeRequest {
                    skip_through: vec![PartitionSeqno {
                        partition: "p1".into(),
                        seqno: 1,
                    }],
                    ..users()
                },
                "InvalidArgument",
            ),
            (
                SubscribeRequest {
                    start: Some(Start::Resume(token.clone())),
                    skip_through: vec![PartitionSeqno {
                        partition: "p9".into(),
                        seqno: 1,
                    }],
                    ..users()
                },
                "InvalidArgument",
            ),
            // Another subscription's token: another prefix.
            (
                SubscribeRequest {
                    start: Some(Start::Resume(token.clone())),
                    prefix: b"a".to_vec().into(),
                    ..users()
                },
                "InvalidArgument",
            ),
            // A single node's token, or no token at all.
            (
                SubscribeRequest {
                    start: Some(Start::Resume(Bytes::from_static(&[1, 2, 3]))),
                    ..users()
                },
                "InvalidArgument",
            ),
            (
                SubscribeRequest {
                    checkpoint_every: MAX_EVERY as u32 + 1,
                    ..users()
                },
                "InvalidArgument",
            ),
            (
                SubscribeRequest {
                    start: Some(Start::FromSnapshot("s1".into())),
                    ..users()
                },
                "Unsupported",
            ),
        ];
        for (req, want) in bad {
            let Err(e) = subscribe(c, req.clone()).await else {
                panic!("{req:?} wasn't refused");
            };
            assert_eq!(code(&e), want, "{req:?}: {e}");
        }
    });
}

#[test]
fn skip_through_past_a_checkpoints_reach_is_refused_not_a_silent_gap() {
    // perf287's cap: a client can't have handled more than MAX_EVERY commits after a
    // checkpoint, so a skip that would pass over more is a client's mistake.
    let t = Three::start();
    let (a, c) = (t.ports[0], t.ports[2]);
    t.rt.block_on(async {
        put(a, "a000", "v").await;
        let mut s = subscribe(
            c,
            SubscribeRequest {
                start: Some(Start::FromOldest(true)),
                checkpoint_every: 1,
                ..users()
            },
        )
        .await
        .unwrap();
        let (seen, _) = commits(&mut s, 1).await;
        let token = seen[0].token.clone().unwrap();
        for i in 1..=(MAX_EVERY + 2) {
            put(a, &format!("a{i:03}"), "v").await;
        }
        let mut s = subscribe(
            c,
            SubscribeRequest {
                start: Some(Start::Resume(token)),
                skip_through: vec![PartitionSeqno {
                    partition: "p1".into(),
                    seqno: u64::MAX,
                }],
                ..users()
            },
        )
        .await
        .unwrap();
        let e = loop {
            match tokio::time::timeout(Duration::from_secs(10), s.message()).await {
                Ok(Ok(Some(e))) => assert!(
                    !matches!(e.event, Some(Event::Commit(_))),
                    "a commit passed through the skip: {e:?}"
                ),
                Ok(Err(e)) => break e,
                other => panic!("{other:?}"),
            }
        };
        assert_eq!(code(&e), "InvalidArgument", "{e}");
        assert!(e.message().contains("skip_through"), "{e}");
    });
}

#[test]
fn a_partition_that_goes_away_ends_the_stream_or_with_allow_partial_comes_back_in_place() {
    let mut t = Three::start();
    let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
    let partial = SubscribeRequest {
        allow_partial: true,
        ..users()
    };
    let (mut strict, mut lenient) = t.rt.block_on(async {
        (
            subscribe(c, users()).await.unwrap(),
            subscribe(c, partial.clone()).await.unwrap(),
        )
    });
    let before = rows("b", 10);
    t.rt.block_on(async {
        for row in &before {
            put(c, row, "v").await;
        }
        commits(&mut strict, before.len()).await;
        commits(&mut lenient, before.len()).await;
    });
    t.stop(1);
    t.rt.block_on(async {
        // The default: the stream ends, naming the partition.
        let e = loop {
            match tokio::time::timeout(Duration::from_secs(10), strict.message()).await {
                Ok(Ok(Some(_))) => {}
                Ok(Err(e)) => break e,
                other => panic!("{other:?}"),
            }
        };
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        assert!(e.message().contains("p2"), "{e}");
        // A new subscription can't open p2 ...
        let Err(e) = subscribe(c, users()).await else {
            panic!("a subscription opened p2 with its node down");
        };
        assert_eq!(code(&e), "PartitionUnavailable", "{e}");
        // ... unless its prefix keeps it to p1, or it allows partial results.
        let mut narrow = subscribe(
            c,
            SubscribeRequest {
                prefix: b"a".to_vec().into(),
                ..users()
            },
        )
        .await
        .unwrap();
        let mut late = subscribe(c, partial.clone()).await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(10), late.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match first.event {
            Some(Event::PartitionState(st)) => {
                assert_eq!(
                    (st.partition.as_str(), st.available),
                    ("p2", false),
                    "{st:?}"
                );
                assert_eq!(st.reason, "PartitionUnavailable");
            }
            other => panic!("{other:?}"),
        }
        // The open stream says p2 went away, and p1 carries on.
        put(a, "aduring", "v").await;
        let (seen, other) = commits(&mut lenient, 1).await;
        assert_eq!(seen[0].rows, ["aduring"]);
        assert!(
            other.iter().any(|e| matches!(
                &e.event,
                Some(Event::PartitionState(st)) if st.partition == "p2" && !st.available
            )),
            "{other:?}"
        );
        assert_eq!(commits(&mut narrow, 1).await.0[0].rows, ["aduring"]);
    });
    t.restart(1);
    let after = rows("c", 10);
    t.rt.block_on(async {
        for row in &after {
            put(b, row, "v").await;
        }
        // p2 is back: reported, and its commits continue in order with none missed.
        let (seen, other) = commits(&mut lenient, after.len()).await;
        assert!(
            other.iter().any(|e| matches!(
                &e.event,
                Some(Event::PartitionState(st)) if st.partition == "p2" && st.available
            )),
            "{other:?}"
        );
        let got: BTreeSet<String> = seen.iter().flat_map(|s| s.rows.clone()).collect();
        assert_eq!(got, after.iter().cloned().collect(), "{seen:?}");
    });
}

#[test]
fn a_subscriber_that_stops_reading_doesnt_hold_up_forwarded_calls() {
    // simp724's S1: a subscription's parts on b have a connection of their own, so the
    // flow-control window stalled subscribers fill is never one forwarded calls use. One
    // stalled stream can't fill a connection's window (hyper's 2 MiB a stream, 5 MiB a
    // connection), so four do: on one shared connection they'd hold up every call on it,
    // as they do when the reads below share theirs (checked while writing this test).
    let t = Three::start();
    let (b, c) = (t.ports[1], t.ports[2]);
    t.rt.block_on(async {
        let mut stalled = Vec::new();
        for _ in 0..4 {
            let req = SubscribeRequest {
                prefix: b"z".to_vec().into(),
                ..users()
            };
            stalled.push(subscribe(c, req).await.unwrap());
        }
        // About 8 MB to p2: more than each subscriber's buffers and windows hold.
        let value = "x".repeat(16 << 10);
        for i in 0..512 {
            put(b, &format!("z{i:04}"), &value).await;
        }
        // Forwarded through c to b, the node the stalled stream comes from.
        for i in 0..20 {
            let started = std::time::Instant::now();
            let got = tokio::time::timeout(Duration::from_secs(2), get_row(c, &format!("z{i:04}")))
                .await
                .expect("a forwarded read waited behind the stalled subscriber")
                .unwrap();
            assert_eq!(got.map(|v| v.len()), Some(16 << 10));
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "{:?}",
                started.elapsed()
            );
        }
        drop(stalled);
    });
}

#[test]
fn a_partition_whose_node_never_answers_doesnt_hold_up_the_others() {
    // simp724's S1 on #1700: b accepts connections but never answers, so every open of p2
    // waits out the request timeout (5 s here). Those opens run in the background: p1's
    // commits keep arriving promptly across several retry rounds.
    let rt = runtime();
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let mut map = the_map(1, &[ports[0], ports[1], ports[2]]);
    map.partitions.retain(|p| p.id != "p3");
    let dirs: Vec<PathBuf> = (0..3).map(|_| test_dir()).collect();
    for dir in &dirs {
        super::super::write_map(dir, &super::super::map::canonical_json(&map)).unwrap();
    }
    let mut it = listeners.into_iter();
    let (_a, db_a) = data_node(&rt, &dirs[0], "a", Some("p1"), it.next().unwrap());
    // Bound, listening, never served: connections complete and then hear nothing.
    let _silent_b = it.next().unwrap();
    let (_c, _) = data_node(&rt, &dirs[2], "c", None, it.next().unwrap());
    let (a, c) = (ports[0], ports[2]);
    rt.block_on(async {
        let mut s = subscribe(
            c,
            SubscribeRequest {
                allow_partial: true,
                ..users()
            },
        )
        .await
        .unwrap();
        let first = tokio::time::timeout(Duration::from_secs(10), s.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            matches!(&first.event, Some(Event::PartitionState(st)) if st.partition == "p2" && !st.available),
            "{first:?}"
        );
        // Across about three retry rounds of 5 s opens, and at least 20 rows, however long
        // the puts take (ai1249's N1 on #1724), within 30 s.
        let started = std::time::Instant::now();
        let mut i = 0;
        while started.elapsed() < Duration::from_secs(12) || i < 20 {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "only {i} rows in 30 s: p1's puts are stalling"
            );
            let row = format!("a{i:04}");
            // The bound is on the delivery, from the commit's return: what p2's opens could
            // hold up. The commit itself (a group sync on a, which a runner's disk can stall
            // for seconds) is outside it, and reported with it (#1723).
            let wrote = std::time::Instant::now();
            put(a, &row, "v").await;
            let (committed, put_took) = (std::time::Instant::now(), wrote.elapsed());
            let (seen, _) = commits(&mut s, 1).await;
            assert_eq!(seen[0].rows, std::slice::from_ref(&row));
            let delivered = committed.elapsed();
            assert!(
                delivered < Duration::from_secs(1),
                "p1's commit {row} arrived {delivered:?} after it returned (its put took \
                 {put_took:?}), behind p2's opens"
            );
            i += 1;
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    });
    rt.shutdown_background();
    let _ = db_a.map(pigeonhole::Pigeonhole::close);
}

/// `phdb cluster move` (D317 ticket 6): p1 moves from a to c with its files copied whole, so
/// a token taken before the move resumes after it, through another node, with no gap and no
/// repeat. `--drop-source` moves a's files aside once the copy checks out. Run again, the
/// move is done.
#[test]
fn a_moved_partition_keeps_its_feed_and_a_token_from_before_resumes_after() {
    let t = Three::start();
    let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
    let args = super::super::cli::NodeArgs {
        node: format!("http://127.0.0.1:{b}"),
        token_file: None,
        ca: None,
    };
    t.rt.block_on(async {
        let mut s = subscribe(c, users()).await.unwrap();
        let before = rows("before", 8);
        for row in &before {
            put(c, row, "v").await;
        }
        let (seen, _) = commits(&mut s, before.len()).await;
        drop(s);
        // The last checkpoint, and the commits after it that a resume delivers.
        let at = seen.iter().rposition(|x| x.token.is_some()).unwrap();
        let token = seen[at].token.clone().unwrap();
        let mut want: Vec<String> = seen[at + 1..].iter().flat_map(|x| x.rows.clone()).collect();

        let r = within(super::super::moving::run(&args, "p1", "c", true, None))
            .await
            .unwrap();
        assert_eq!(r.document["verified"], true, "{:?}", r.document);
        assert_eq!(r.document["epoch"], 2, "{:?}", r.document);
        let source = r.document["source"].as_str().unwrap();
        assert!(source.starts_with("moved aside"), "{source}");
        let copied: Vec<&str> = r.document["copied"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();
        assert!(copied.contains(&""), "the main file: {copied:?}");
        assert!(copied.contains(&"-feed"), "the feed's file: {copied:?}");
        assert!(!t.dirs[0].join("p1.phdb").exists(), "a's files went aside");
        assert!(t.dirs[2].join("p1.phdb").exists());

        // Writes reach p1 on c, through a.
        let after = rows("after", 8);
        for row in &after {
            put(a, row, "v").await;
        }
        want.extend(after.iter().cloned());

        // The token from before the move resumes through b: every commit after it, once.
        let mut s = subscribe(
            b,
            SubscribeRequest {
                start: Some(Start::Resume(token)),
                ..users()
            },
        )
        .await
        .unwrap();
        let (seen, _) = commits(&mut s, want.len()).await;
        in_order(&seen);
        let mut got: Vec<String> = seen.iter().flat_map(|x| x.rows.clone()).collect();
        got.sort();
        want.sort();
        assert_eq!(got, want, "no gap and no repeat across the move");
        if let Ok(Ok(Some(e))) = tokio::time::timeout(Duration::from_millis(500), s.message()).await
        {
            assert!(
                !matches!(e.event, Some(Event::Commit(_))),
                "nothing more: {e:?}"
            );
        }

        // Again: done.
        let r = within(super::super::moving::run(&args, "p1", "c", true, None))
            .await
            .unwrap();
        assert!(r.document.contains_key("done"), "{:?}", r.document);
    });
}

/// #1848: a component that took its partition's service just before a close took the
/// partition out of the node's parts, and subscribed after the close's `end_parts`, ended
/// with the closing database's own error: one `allow_partial` doesn't wait out, so the whole
/// subscription ended. It's `PartitionUnavailable` now, and the subscription goes on with
/// the partition reported unavailable. The forwarded subscribe is held at that point.
#[test]
fn a_component_that_subscribes_as_its_partition_closes_leaves_an_allow_partial_stream_going() {
    // Bounded: a hold that never releases fails the test rather than hanging it (#1865).
    bounded(Duration::from_secs(120), || {
        use crate::serve::proto::PartitionRequest;
        let t = Three::start();
        let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        crate::serve::cluster::feed::HOLD_SUBSCRIBE
            .lock()
            .unwrap()
            .push(((t.dirs[1].clone(), "p2".into()), wait));
        let p2 = || PartitionRequest {
            partition: "p2".into(),
            ..Default::default()
        };
        t.rt.block_on(async {
            // The subscription's open of p2 on b holds, with p2's service taken.
            let opening = tokio::spawn(subscribe(
                c,
                SubscribeRequest {
                    allow_partial: true,
                    ..users()
                },
            ));
            eventually("b's subscribe of p2 to be held", async || {
                crate::serve::cluster::feed::HOLD_SUBSCRIBE
                    .lock()
                    .unwrap()
                    .is_empty()
            })
            .await;
            // Meanwhile p2 closes on b, its database included.
            client(b).await.fence(p2()).await.unwrap();
            client(b).await.close(p2()).await.unwrap();
            eventually("p2's database to close on b", async || {
                client(b)
                    .await
                    .status(StatusRequest {})
                    .await
                    .unwrap()
                    .into_inner()
                    .partitions
                    .iter()
                    .any(|lp| lp.id == "p2" && !lp.files.is_empty())
            })
            .await;
            release.send(()).unwrap();
            let mut s = opening
                .await
                .unwrap()
                .unwrap_or_else(|e| panic!("the subscription ended: {e}"));
            // p2 reported unavailable, and p1 carries on.
            let mut gone = false;
            put(a, "aafter", "v").await;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            let mut got_p1 = false;
            while !(gone && got_p1) {
                let e = tokio::time::timeout_at(deadline, s.message())
                    .await
                    .expect("p2's state and p1's commit")
                    .unwrap_or_else(|e| panic!("the subscription ended: {e}"))
                    .expect("the stream ended");
                match e.event {
                    Some(Event::PartitionState(st)) if st.partition == "p2" => {
                        assert!(!st.available, "{st:?}");
                        assert_eq!(st.reason, "PartitionUnavailable", "{st:?}");
                        gone = true;
                    }
                    Some(Event::Commit(cm))
                        if cm.changes.iter().any(|ch| &ch.row[..] == b"aafter") =>
                    {
                        got_p1 = true;
                    }
                    _ => {}
                }
            }
        });
    });
}

#[test]
fn a_component_of_a_partition_the_map_no_longer_lists_ends() {
    // #1848: `end_moved_feeds` for a partition still open here that the map no longer lists
    // at all (tested before only for one the map gives another node). A forced map that drops
    // p3 and narrows `users` to p1 and p2 reaches c, where p3 stays open: c's component of a
    // subscription to `users` ends rather than following a copy no map owns, and the
    // subscription (no `allow_partial`) ends with an error.
    bounded(Duration::from_secs(120), || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_stops, ports, _dbs) = three_nodes(&rt);
        let c = ports[2];
        rt.block_on(async {
            let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{c}"))
                .unwrap()
                .connect()
                .await
                .unwrap();
            let mut s = FeedClient::new(channel)
                .subscribe(SubscribeRequest {
                    tables: vec!["users".into()],
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            // p3's component delivers: a commit of "zed" (>= "m").
            put(c, "zed", "1").await;
            loop {
                let e = tokio::time::timeout(Duration::from_secs(10), s.message())
                    .await
                    .expect("zed's commit within 10 s")
                    .unwrap()
                    .expect("the stream ended");
                if let Some(Event::Commit(cm)) = &e.event
                    && cm.changes.iter().any(|ch| ch.row.as_ref() == b"zed")
                {
                    break;
                }
            }

            let mut m = client(c)
                .await
                .get_map(GetMapRequest {})
                .await
                .unwrap()
                .into_inner();
            m.map_version += 1;
            m.partitions.retain(|p| p.id != "p3");
            m.tables[0].splits.truncate(1);
            m.tables[0].partitions.retain(|p| p != "p3");
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
            let ended = loop {
                match tokio::time::timeout(Duration::from_secs(20), s.message())
                    .await
                    .expect("the subscription ends once c takes the map")
                {
                    Ok(Some(_)) => {}
                    Ok(None) => panic!("the stream ended without an error"),
                    Err(e) => break e,
                }
            };
            assert_ne!(code(&ended), "", "{ended}");
        });
    });
}

#[test]
fn a_component_that_subscribes_as_its_partition_moves_doesnt_follow_the_old_copy() {
    // #1848: `still_owner`'s race (#1805). b's subscribe of p2 holds with p2's service taken;
    // meanwhile a map gives p2 to c and leaves b's copy open, and b's install ends p2's
    // components before this one is there to end. Released, the component finds the map no
    // longer names b and refuses, so the `allow_partial` subscription looks for p2 at c
    // (which never opens it) and reports it unavailable, while p1 carries on. Without the
    // check the component registers on b's old copy and follows it quietly.
    bounded(Duration::from_secs(120), || {
        let t = Three::start();
        let (a, c) = (t.ports[0], t.ports[2]);
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        crate::serve::cluster::feed::HOLD_SUBSCRIBE
            .lock()
            .unwrap()
            .push(((t.dirs[1].clone(), "p2".into()), wait));
        t.rt.block_on(async {
            let opening = tokio::spawn(subscribe(
                c,
                SubscribeRequest {
                    allow_partial: true,
                    ..users()
                },
            ));
            eventually("b's subscribe of p2 to be held", async || {
                crate::serve::cluster::feed::HOLD_SUBSCRIBE
                    .lock()
                    .unwrap()
                    .is_empty()
            })
            .await;
            // The writer (a) gives p2 to c at the next epoch; b keeps its copy open.
            let mut m = client(a)
                .await
                .get_map(GetMapRequest {})
                .await
                .unwrap()
                .into_inner();
            let version = m.map_version;
            for p in m.partitions.iter_mut().filter(|p| p.id == "p2") {
                p.node = "c".into();
                p.epoch += 1;
            }
            m.map_version += 1;
            client(a)
                .await
                .set_map(SetMapRequest {
                    force: false,
                    expected_map_version: version,
                    map: Some(m),
                    copies: Vec::new(),
                })
                .await
                .unwrap();
            let want = version + 1;
            eventually("b to take the map", async || {
                client(t.ports[1])
                    .await
                    .get_map(GetMapRequest {})
                    .await
                    .is_ok_and(|r| r.into_inner().map_version == want)
            })
            .await;
            release.send(()).unwrap();
            let mut s = opening
                .await
                .unwrap()
                .unwrap_or_else(|e| panic!("the subscription ended: {e}"));
            // p2 reported unavailable (its new node never serves it), and p1 carries on.
            put(a, "aafter", "v").await;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            let (mut gone, mut got_p1) = (false, false);
            while !(gone && got_p1) {
                let e = tokio::time::timeout_at(deadline, s.message())
                    .await
                    .expect("p2's state and p1's commit")
                    .unwrap_or_else(|e| panic!("the subscription ended: {e}"))
                    .expect("the stream ended");
                match e.event {
                    Some(Event::PartitionState(st)) if st.partition == "p2" => {
                        assert!(!st.available, "{st:?}");
                        gone = true;
                    }
                    Some(Event::Commit(cm))
                        if cm.changes.iter().any(|ch| &ch.row[..] == b"aafter") =>
                    {
                        got_p1 = true;
                    }
                    _ => {}
                }
            }
        });
    });
}

#[test]
fn a_moved_component_whose_new_node_never_serves_gives_up_at_the_request_timeout() {
    // #1848: the moved reopen's retry loop (audit120's S-C5) giving up. A map gives p2 to c,
    // which has no copy and never serves it; b ends its component as moved. The subscription
    // (no `allow_partial`) tries c again for one request timeout (5 s in the tests), not
    // ending at c's first `PartitionUnavailable`, and then ends with it.
    bounded(Duration::from_secs(120), || {
        let t = Three::start();
        let (a, c) = (t.ports[0], t.ports[2]);
        t.rt.block_on(async {
            let mut s = subscribe(c, users())
                .await
                .unwrap_or_else(|e| panic!("subscribe: {e}"));
            // p2's component delivers first.
            put(a, "zed", "1").await;
            loop {
                let e = tokio::time::timeout(Duration::from_secs(10), s.message())
                    .await
                    .expect("zed's commit within 10 s")
                    .unwrap()
                    .expect("the stream ended");
                if let Some(Event::Commit(cm)) = &e.event
                    && cm.changes.iter().any(|ch| ch.row.as_ref() == b"zed")
                {
                    break;
                }
            }
            let mut m = client(a)
                .await
                .get_map(GetMapRequest {})
                .await
                .unwrap()
                .into_inner();
            let version = m.map_version;
            for p in m.partitions.iter_mut().filter(|p| p.id == "p2") {
                p.node = "c".into();
                p.epoch += 1;
            }
            m.map_version += 1;
            let moved = tokio::time::Instant::now();
            client(a)
                .await
                .set_map(SetMapRequest {
                    force: false,
                    expected_map_version: version,
                    map: Some(m),
                    // Marked as copied, so p2 keeps its history and the subscription's
                    // position stays good at c: only c's never serving it can end it.
                    copies: vec!["p2".into()],
                })
                .await
                .unwrap();
            let ended = loop {
                match tokio::time::timeout(Duration::from_secs(30), s.message())
                    .await
                    .expect("the subscription ends")
                {
                    Ok(Some(_)) => {}
                    Ok(None) => panic!("the stream ended without an error"),
                    Err(e) => break e,
                }
            };
            let took = moved.elapsed();
            assert_eq!(code(&ended), "PartitionUnavailable", "{ended}");
            assert!(
                took >= Duration::from_secs(4),
                "tried c again for the request timeout, not once: {took:?} ({ended})"
            );
        });
    });
}
