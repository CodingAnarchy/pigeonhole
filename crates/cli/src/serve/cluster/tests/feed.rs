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
        Self::start_with(crate::serve::server_tests::limits())
    }

    /// [`Three::start`] with `limits` on every node.
    fn start_with(limits: crate::serve::config::Limits) -> Self {
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
            let (stop, db) = data_node_with(&node_rt, &dirs[i], IDS[i], PARTS[i], l, limits);
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

/// A gauge or counter on node `port`'s `/metrics` (`Admin.Metrics`).
async fn metric(port: u16, name: &str) -> u64 {
    use crate::serve::proto::{Empty, admin_client::AdminClient};
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let text = AdminClient::new(channel)
        .metrics(Empty {})
        .await
        .unwrap()
        .into_inner()
        .text;
    text.lines()
        .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
        .unwrap_or_else(|| panic!("{name} isn't in:\n{text}"))
}

/// Waits up to 10 s for `name` on `port` to satisfy `ok`.
async fn metric_until(port: u16, name: &str, ok: impl Fn(u64) -> bool) -> u64 {
    for _ in 0..200 {
        let v = metric(port, name).await;
        if ok(v) {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "{name} on {port} never got there: {}",
        metric(port, name).await
    );
}

const MERGES: &str = "pigeonhole_serve_feed_cluster_merges";
const MEMBERS: &str = "pigeonhole_serve_feed_cluster_hub_members";

/// Each commit's rows, once each, in each partition's order: what every reader of a
/// subscription over `want` gets.
fn exactly(seen: &[Seen], want: &[String]) {
    in_order(seen);
    let got: Vec<&str> = seen
        .iter()
        .flat_map(|s| s.rows.iter().map(String::as_str))
        .collect();
    let set: BTreeSet<&str> = got.iter().copied().collect();
    assert_eq!(set.len(), got.len(), "a row twice");
    assert_eq!(set, want.iter().map(String::as_str).collect(), "every row");
}

#[test]
fn one_requests_subscribers_share_one_merge_and_anothers_get_their_own() {
    // #1798's B: subscribers of one request (and one token) share one merge, however many;
    // another request has its own. The last to leave ends its hub and its parts.
    let t = Three::start();
    let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
    t.rt.block_on(async {
        let mut shared = Vec::new();
        for _ in 0..3 {
            shared.push(subscribe(c, users()).await.unwrap());
        }
        metric_until(c, MEMBERS, |n| n == 3).await;
        assert_eq!(
            metric(c, MERGES).await,
            1,
            "one merge for three subscribers"
        );
        let mut narrow = subscribe(
            c,
            SubscribeRequest {
                prefix: b"a".to_vec().into(),
                ..users()
            },
        )
        .await
        .unwrap();
        metric_until(c, MEMBERS, |n| n == 4).await;
        assert_eq!(metric(c, MERGES).await, 2, "another request, another merge");
        let want = rows("share-", 30);
        for (i, row) in want.iter().enumerate() {
            put([a, b, c][i % 3], row, "v").await;
        }
        for s in &mut shared {
            let (seen, _) = commits(s, want.len()).await;
            exactly(&seen, &want);
            assert!(seen[0].token.is_some(), "the first commit is a checkpoint");
        }
        let (seen, _) = commits(&mut narrow, want.len() / 2).await;
        let a_rows: Vec<String> = want
            .iter()
            .filter(|r| r.starts_with('a'))
            .cloned()
            .collect();
        exactly(&seen, &a_rows);
        drop((shared, narrow));
        metric_until(c, MERGES, |n| n == 0).await;
        assert_eq!(metric(c, MEMBERS).await, 0);
    });
}

#[test]
fn a_joiner_passes_over_a_local_commit_made_while_it_joined() {
    // simp724's Q2 on #1798's B: a `Now` joiner takes the ring's head, then each local
    // partition's seqno, and a local commit between the two is passed over, as a single
    // node's `Now` passes over what predates it (F5 on #1518). a serves p1.
    let t = Three::start();
    let a = t.ports[0];
    t.rt.block_on(async {
        let mut first = subscribe(a, users()).await.unwrap();
        metric_until(a, MEMBERS, |n| n == 1).await;
        let (go, wait) = tokio::sync::oneshot::channel();
        crate::serve::cluster::hub::HOLD_JOIN
            .lock()
            .unwrap()
            .push((t.dirs[0].clone(), wait));
        let joining = tokio::spawn(subscribe(a, users()));
        // Joined (counted), then held between its head and its seqnos.
        metric_until(a, MEMBERS, |n| n == 2).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        put(a, "a-between", "v").await;
        // On the ring, past the joiner's head.
        let (seen, _) = commits(&mut first, 1).await;
        assert_eq!(seen[0].rows, ["a-between"]);
        go.send(()).unwrap();
        let mut second = joining.await.unwrap().unwrap();
        put(a, "a-after", "v").await;
        let (seen, _) = commits(&mut second, 1).await;
        assert_eq!(
            seen[0].rows,
            ["a-after"],
            "not the commit from before its seqno"
        );
    });
}

/// A value large enough that a few hundred commits overrun a client's HTTP/2 window
/// (hyper's 2 MiB a stream), so one that stops reading holds events untaken.
fn big() -> String {
    "v".repeat(16 << 10)
}

/// Limits with a tiny ring, so a subscriber that stops reading falls off it.
fn small_ring() -> crate::serve::config::Limits {
    let mut l = crate::serve::server_tests::limits();
    l.feed_ring_bytes = 4096;
    l.feed_client_events = 1;
    l.stream_idle = Duration::from_secs(600);
    l
}

#[test]
fn a_member_off_the_ring_catches_up_on_its_own_merge_and_rejoins_with_nothing_lost_or_repeated() {
    // #1798's B, §2.4: c routes only, so its hub merges two remote partitions, interleaved.
    // A subscriber that reads nothing while 300 commits pass falls off c's small ring, goes
    // on from its last checkpoint on a merge of its own, and rejoins the ring.
    // Checkpoints 8 commits apart: its last one trails what it delivered, so its own merge
    // delivers some of it again, which it passes over by its own positions.
    let t = Three::start_with(small_ring());
    let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
    let every8 = || SubscribeRequest {
        checkpoint_every: 8,
        ..users()
    };
    t.rt.block_on(async {
        let mut fast = subscribe(c, every8()).await.unwrap();
        let mut slow = subscribe(c, every8()).await.unwrap();
        metric_until(c, MEMBERS, |n| n == 2).await;
        let value = big();
        let want = rows("fb-", 300);
        for (i, row) in want.iter().enumerate() {
            put([a, b][i % 2], row, &value).await;
        }
        let (seen, _) = commits(&mut fast, want.len()).await;
        exactly(&seen, &want);
        let (seen, _) = commits(&mut slow, want.len()).await;
        exactly(&seen, &want);
        assert!(metric(c, "pigeonhole_serve_feed_cluster_fell_behind_total").await >= 1);
        // Caught up: back on the ring, and what comes next arrives once.
        metric_until(c, "pigeonhole_serve_feed_cluster_rejoined_total", |n| {
            n >= 1
        })
        .await;
        let more = rows("more-", 10);
        for (i, row) in more.iter().enumerate() {
            put([a, b][i % 2], row, "v").await;
        }
        let (seen, _) = commits(&mut slow, more.len()).await;
        exactly(&seen, &more);
        assert_eq!(metric(c, MERGES).await, 1, "only the hub's merge again");
    });
}

#[test]
fn a_fallen_member_waits_for_a_catch_up_slot_delivering_nothing_then_goes_on() {
    // simp724's S2 on #1798's B: a member off its ring needs a --max-catch-up slot for its
    // own merge. With the one slot held by another, it delivers nothing until the slot is
    // free, then everything, once.
    let mut l = small_ring();
    l.max_catch_up = 1;
    let t = Three::start_with(l);
    let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
    t.rt.block_on(async {
        let mut holder = subscribe(c, users()).await.unwrap();
        let mut waiter = subscribe(c, users()).await.unwrap();
        metric_until(c, MEMBERS, |n| n == 2).await;
        let value = big();
        let want = rows("slot-", 300);
        for (i, row) in want.iter().enumerate() {
            put([a, b][i % 2], row, &value).await;
        }
        // `holder` reads a little: off the ring, onto its own merge with the slot, which
        // fills its buffer again while it stops reading.
        let (mut held, _) = commits(&mut holder, 100).await;
        metric_until(c, "pigeonhole_serve_feed_cluster_fell_behind_total", |n| {
            n >= 1
        })
        .await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        // `waiter` gets what its buffer held, then nothing: no slot.
        let mut got = Vec::new();
        while let Ok(Some(e)) = tokio::time::timeout(Duration::from_secs(1), waiter.message())
            .await
            .map(Result::unwrap)
        {
            got.push(e);
        }
        let commits_got = got
            .iter()
            .filter(|e| matches!(e.event, Some(Event::Commit(_))))
            .count();
        assert!(
            commits_got < want.len(),
            "it waits for a slot: {commits_got}"
        );
        // `holder` catches up and rejoins, giving the slot back; `waiter` then goes on.
        let (rest, _) = commits(&mut holder, want.len() - held.len()).await;
        held.extend(rest);
        exactly(&held, &want);
        let (rest, _) = commits(&mut waiter, want.len() - commits_got).await;
        let mut seen: Vec<Seen> = got
            .iter()
            .filter_map(|e| match &e.event {
                Some(Event::Commit(c)) => Some(Seen {
                    partition: e.partition.clone(),
                    seqno: c.seqno,
                    rows: c
                        .changes
                        .iter()
                        .map(|ch| String::from_utf8(ch.row.to_vec()).unwrap())
                        .collect(),
                    token: None,
                }),
                _ => None,
            })
            .collect();
        seen.extend(rest);
        exactly(&seen, &want);
    });
}

#[test]
fn a_key_past_the_hub_limit_runs_its_own_merge() {
    // simp724's S1 on #1798's B: hubs are capped at --max-feed-hubs; a key past it runs a
    // merge of its own rather than being refused.
    let mut l = crate::serve::server_tests::limits();
    l.max_feed_hubs = 1;
    let t = Three::start_with(l);
    let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
    t.rt.block_on(async {
        let mut shared = subscribe(c, users()).await.unwrap();
        let mut own = subscribe(
            c,
            SubscribeRequest {
                prefix: b"a".to_vec().into(),
                ..users()
            },
        )
        .await
        .unwrap();
        metric_until(c, MERGES, |n| n == 2).await;
        assert_eq!(
            metric(c, MEMBERS).await,
            1,
            "one on the hub, one on its own merge"
        );
        let want = rows("cap-", 20);
        for (i, row) in want.iter().enumerate() {
            put([a, b][i % 2], row, "v").await;
        }
        let (seen, _) = commits(&mut shared, want.len()).await;
        exactly(&seen, &want);
        let (seen, _) = commits(&mut own, want.len() / 2).await;
        let a_rows: Vec<String> = want
            .iter()
            .filter(|r| r.starts_with('a'))
            .cloned()
            .collect();
        exactly(&seen, &a_rows);
    });
}

#[test]
fn a_member_that_stops_reading_is_dropped_without_holding_up_the_others() {
    // A hub never waits for a member: one that stops reading ends at its idle limit, and
    // the others read on meanwhile.
    let mut l = crate::serve::server_tests::limits();
    l.stream_idle = Duration::from_millis(500);
    l.feed_client_events = 1;
    let t = Three::start_with(l);
    let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
    t.rt.block_on(async {
        let mut reader = subscribe(c, users()).await.unwrap();
        let _stopped = subscribe(c, users()).await.unwrap();
        metric_until(c, MEMBERS, |n| n == 2).await;
        let value = big();
        let want = rows("idle-", 200);
        // The reader reads as the commits come.
        let n = want.len();
        let reading = tokio::spawn(async move {
            let seen = commits(&mut reader, n).await.0;
            (reader, seen)
        });
        for (i, row) in want.iter().enumerate() {
            put([a, b][i % 2], row, &value).await;
        }
        let (_reader, seen) = reading.await.unwrap();
        exactly(&seen, &want);
        metric_until(c, "pigeonhole_serve_feed_idle_drops_total", |n| n == 1).await;
        metric_until(c, MEMBERS, |n| n == 1).await;
    });
}

#[test]
fn a_joiner_that_found_its_hub_as_it_ended_opens_a_new_one() {
    // simp724's S3 on #1884: a joiner that found its key's hub just before the hub's merge
    // ended has no head yet, so it looks again and opens a new hub, rather than end with the
    // other subscribers' error. Here the hub ends because p2's node stops; it's back before
    // the joiner goes on.
    let mut t = Three::start();
    let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
    let (mut first, joining, go) = t.rt.block_on(async {
        let first = subscribe(c, users()).await.unwrap();
        metric_until(c, MEMBERS, |n| n == 1).await;
        let (go, wait) = tokio::sync::oneshot::channel();
        crate::serve::cluster::hub::HOLD_READY
            .lock()
            .unwrap()
            .push((t.dirs[2].clone(), wait));
        let joining = tokio::spawn(subscribe(c, users()));
        // Found the hub, and held before it joins.
        tokio::time::sleep(Duration::from_millis(300)).await;
        (first, joining, go)
    });
    t.stop(1);
    t.rt.block_on(async {
        // The hub's merge ends: its subscriber's stream with it.
        loop {
            match tokio::time::timeout(Duration::from_secs(10), first.message()).await {
                Ok(Ok(Some(_))) => {}
                Ok(Err(e)) => {
                    assert_eq!(code(&e), "PartitionUnavailable", "{e}");
                    break;
                }
                other => panic!("{other:?}"),
            }
        }
    });
    t.restart(1);
    t.rt.block_on(async {
        // b serves again.
        put(b, "z-back", "v").await;
        go.send(()).unwrap();
        let mut second = joining
            .await
            .unwrap()
            .expect("a new hub, not the ended one's error");
        put(a, "a-new", "v").await;
        put(b, "z-new", "v").await;
        let (seen, _) = commits(&mut second, 2).await;
        let rows: BTreeSet<&str> = seen
            .iter()
            .flat_map(|s| s.rows.iter().map(String::as_str))
            .collect();
        assert_eq!(rows, ["a-new", "z-new"].into_iter().collect());
    });
}
