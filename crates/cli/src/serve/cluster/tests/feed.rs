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
    /// A stopped node's port, held for its restart ([`HeldPort`], #2105).
    held: Vec<Option<HeldPort>>,
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
            held: (0..3).map(|_| None).collect(),
        }
    }

    /// Stops node `i`: its server and every connection and stream it had. Its database
    /// stays open, for [`Three::restart`].
    fn stop(&mut self, i: usize) {
        let _ = self.stops[i].take().expect("running").send(());
        self.nodes[i].take().expect("running").shutdown_background();
        // Until its port refuses, so the next call doesn't reach it mid-shutdown; then the
        // port is held for its restart, so a parallel test can't take it (#2105).
        let port = self.ports[i];
        self.rt.block_on(stopped(port));
        self.held[i] = Some(HeldPort::take(port));
    }

    /// The stopped node `i`'s port to listen on again: the one held, or taken now.
    fn listener(&mut self, i: usize) -> std::net::TcpListener {
        let l = self.held[i]
            .take()
            .unwrap_or_else(|| HeldPort::take(self.ports[i]))
            .listen();
        l.set_nonblocking(true).unwrap();
        l
    }

    /// Serves node `i` again on its port, with its database.
    fn restart(&mut self, i: usize) {
        let listener = self.listener(i);
        let node_rt = runtime();
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

    /// Starts node `i` again as a restarted node (D326): its partitions wait for a lease
    /// from its rounds, which it then renews, rather than standing for a node that has
    /// served all along.
    fn restart_leased(&mut self, i: usize) {
        let listener = self.listener(i);
        let node_rt = runtime();
        let (stop, _) = data_node_full(
            &node_rt,
            &self.dirs[i],
            IDS[i],
            PARTS[i],
            listener,
            "",
            crate::serve::server_tests::limits(),
            self.dbs[i].clone(),
            true,
        );
        self.stops[i] = Some(stop);
        self.nodes[i] = Some(node_rt);
    }

    /// Cuts node `i` off from its peers both ways (its lease rounds and pushes), or joins it
    /// again; its clients still reach it.
    fn cut(&self, i: usize, off: bool) {
        super::super::cut_node(
            &self.dirs[i],
            &format!("127.0.0.1:{}", self.ports[i]),
            &self.dirs,
            off,
        );
    }
}

/// Puts `row` through `port` until it's taken: a restarted node's partition takes writes
/// once it holds its lease.
async fn put_when_leased(port: u16, row: &str) {
    eventually("the partition to take writes under its lease", async || {
        data(port)
            .await
            .mutate(MutateRequest {
                writes: Some(put_row(row, "v")),
                durability: 0,
            })
            .await
            .is_ok()
    })
    .await;
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
        // The client handled a few events past a checkpoint, then stopped. Where checkpoints
        // fall depends on when the member moves from its own merge to its hub's ring (each
        // counts 8 from its own start), so the stop is placed from the ones it got: 4 past
        // the first at or after event 30, before the next one 8 on.
        let at = (30..first.len())
            .find(|&i| seen[i].token.is_some())
            .expect("a checkpoint");
        let handled = at + 4;
        assert!(
            seen[at + 1..handled].iter().all(|x| x.token.is_none()),
            "checkpoints 8 apart"
        );
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
        own: None,
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
    // And one byte, so every event goes alone, not in a batch (#1737's F3-0).
    l.feed_client_bytes = 1;
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
    // And one byte, so every event goes alone, not in a batch (#1737's F3-0).
    l.feed_client_bytes = 1;
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
                    .status(StatusRequest::default())
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
fn a_component_of_a_partition_the_map_no_longer_lists_ends_as_unavailable() {
    // #1848: `end_moved_feeds` for a partition still open here that the map no longer lists
    // at all (tested before only for one the map gives another node). A forced map that drops
    // p3 and narrows `users` to p1 and p2 reaches c, where p3 stays open: c's component of a
    // subscription to `users` ends rather than following a copy no map owns. Its reopen finds
    // no owner: `PartitionUnavailable` naming the drop (#1896), not `Internal`. A
    // subscription without `allow_partial` ends with it; one with it reports p3 gone and
    // goes on with p1 and p2.
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
            let mut feeds = FeedClient::new(channel);
            let mut s = feeds
                .subscribe(SubscribeRequest {
                    tables: vec!["users".into()],
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            let mut lenient = feeds
                .subscribe(SubscribeRequest {
                    tables: vec!["users".into()],
                    allow_partial: true,
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            // p3's component delivers: a commit of "zed" (>= "m").
            put(c, "zed", "1").await;
            for s in [&mut s, &mut lenient] {
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
            // c's component ended as moved, and its reopen finds no owner for p3 in the map.
            assert_eq!(code(&ended), "PartitionUnavailable", "{ended}");
            assert!(
                ended
                    .message()
                    .contains("no longer lists partition \"p3\": a newer map dropped it"),
                "{ended}"
            );
            // The lenient subscription reports p3 gone, with the reason, and goes on.
            loop {
                let e = tokio::time::timeout(Duration::from_secs(20), lenient.message())
                    .await
                    .expect("p3's state within 20 s")
                    .unwrap()
                    .expect("the lenient stream ended");
                if let Some(Event::PartitionState(st)) = &e.event
                    && st.partition == "p3"
                {
                    assert!(!st.available, "{st:?}");
                    assert_eq!(st.reason, "PartitionUnavailable", "{st:?}");
                    assert!(st.message.contains("a newer map dropped it"), "{st:?}");
                    break;
                }
            }
            put(c, "alice", "2").await;
            loop {
                let e = tokio::time::timeout(Duration::from_secs(10), lenient.message())
                    .await
                    .expect("alice's commit within 10 s")
                    .unwrap()
                    .expect("the lenient stream ended");
                if let Some(Event::Commit(cm)) = &e.event
                    && cm.changes.iter().any(|ch| ch.row.as_ref() == b"alice")
                {
                    break;
                }
            }
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

#[test]
fn a_node_cut_off_at_its_restart_ends_its_partitions_subscriptions() {
    // feed1248's F2 on D326: a lease-less node's subscriptions wait only while its rounds
    // reach a majority. a restarts cut off from its peers, so its rounds reach none: a
    // subscription through it ends with PartitionUnavailable rather than waiting for ever.
    let mut t = Three::start();
    let a = t.ports[0];
    t.stop(0);
    t.cut(0, true);
    t.restart_leased(0);
    t.rt.block_on(async {
        let e = match subscribe(a, users()).await {
            Err(e) => e,
            Ok(mut s) => tokio::time::timeout(Duration::from_secs(10), s.message())
                .await
                .expect("the subscription ends within a round or two")
                .expect_err("it ends with an error"),
        };
        assert_eq!(e.code(), tonic::Code::Unavailable, "{e:?}");
        assert!(e.message().contains("lease"), "{e:?}");
    });
    t.cut(0, false);
}

#[test]
fn a_partitions_open_subscription_ends_when_its_lease_lapses() {
    // feed1248's F3 and F5 on D326: a lease that runs out ends the partition's feed
    // components, a quiet one included (nothing to deliver carries it), so a subscription
    // through another node ends with PartitionUnavailable rather than hanging while a new
    // owner may take writes.
    let mut t = Three::start();
    let (a, c) = (t.ports[0], t.ports[2]);
    t.stop(0);
    t.restart_leased(0);
    t.rt.block_on(async {
        put_when_leased(a, "alice").await;
        let mut s = subscribe(c, users()).await.unwrap();
        put(c, "amy", "v").await;
        let (seen, _) = commits(&mut s, 1).await;
        assert_eq!(seen[0].rows, ["amy"]);
        t.cut(0, true);
        let e = loop {
            match tokio::time::timeout(TEST_LEASE * 4, s.message())
                .await
                .expect("the subscription ends within a few leases")
            {
                Ok(Some(e)) => assert!(!matches!(e.event, Some(Event::Commit(_))), "{e:?}"),
                Ok(None) => panic!("the stream ended cleanly"),
                Err(e) => break e,
            }
        };
        assert_eq!(e.code(), tonic::Code::Unavailable, "{e:?}");
        assert!(e.message().contains("lease"), "{e:?}");
    });
    t.cut(0, false);
}

#[test]
fn a_planned_move_with_its_release_lost_resumes_on_the_new_node_once_its_lease_starts() {
    // feed1248's §6 test on D326 PR 2: a's p1 holds a real lease, and its release when the
    // move closes p1 never reaches its peers. The open subscription ends as the move closes
    // p1 (no allow_partial); resumed at once from its last checkpoint through b, its p1
    // component waits on c for c's lease, and then delivers every commit after the
    // checkpoint once, with no gap.
    let mut t = Three::start();
    let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
    t.stop(0);
    t.restart_leased(0);
    super::super::fence::DROP_RELEASES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(t.dirs[0].clone());
    let args = super::super::cli::NodeArgs {
        own: None,
        node: format!("http://127.0.0.1:{b}"),
        token_file: None,
        ca: None,
    };
    t.rt.block_on(async {
        put_when_leased(a, "aaa").await;
        let mut s = subscribe(c, users()).await.unwrap();
        let before = rows("before", 8);
        for row in &before {
            put(c, row, "v").await;
        }
        let (seen, _) = commits(&mut s, before.len()).await;
        let at = seen.iter().rposition(|x| x.token.is_some()).unwrap();
        let token = seen[at].token.clone().unwrap();
        let mut want: Vec<String> = seen[at + 1..].iter().flat_map(|x| x.rows.clone()).collect();

        let r = within(super::super::moving::run(&args, "p1", "c", true, None))
            .await
            .unwrap();
        assert_eq!(r.document["epoch"], 2, "{:?}", r.document);
        let mut s = subscribe(
            b,
            SubscribeRequest {
                start: Some(Start::Resume(token)),
                ..users()
            },
        )
        .await
        .unwrap();
        let after = rows("after", 8);
        for row in &after {
            put(a, row, "v").await;
        }
        want.extend(after.iter().cloned());
        let (seen, _) = commits(&mut s, want.len()).await;
        in_order(&seen);
        let mut got: Vec<String> = seen.iter().flat_map(|x| x.rows.clone()).collect();
        got.sort();
        want.sort();
        assert_eq!(got, want, "no gap and no repeat across the move");
    });
    super::super::fence::DROP_RELEASES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|d| *d != t.dirs[0]);
}

/// A subscription over a connection of its own with 64 KiB HTTP/2 windows, so a client that
/// stops reading leaves the server's events untaken after a few dozen, not 2 MiB.
async fn narrow(port: u16, req: SubscribeRequest) -> tonic::Streaming<FeedEvent> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .initial_stream_window_size(64 << 10)
        .initial_connection_window_size(64 << 10)
        .connect()
        .await
        .unwrap();
    FeedClient::new(channel)
        .subscribe(req)
        .await
        .unwrap()
        .into_inner()
}

/// A commit event as the client saw it, or `None` for another event.
fn seen_of(e: &FeedEvent) -> Option<Seen> {
    match &e.event {
        Some(Event::Commit(c)) => Some(Seen {
            partition: e.partition.clone(),
            seqno: c.seqno,
            rows: c
                .changes
                .iter()
                .map(|ch| String::from_utf8(ch.row.to_vec()).unwrap())
                .collect(),
            token: (!e.token.is_empty()).then(|| e.token.clone()),
        }),
        _ => None,
    }
}

/// The commits that arrive until none does for `quiet`.
async fn drain_for(s: &mut tonic::Streaming<FeedEvent>, quiet: Duration) -> Vec<Seen> {
    let mut out = Vec::new();
    while let Ok(Ok(Some(e))) = tokio::time::timeout(quiet, s.message()).await {
        out.extend(seen_of(&e));
    }
    out
}

/// Writes `n` commits of 1 KiB values, alternating partitions, and adds their rows to `want`.
async fn write_kib(ports: [u16; 2], tag: &str, n: usize, want: &mut Vec<String>) {
    let value = "v".repeat(1024);
    for (i, row) in rows(tag, n).into_iter().enumerate() {
        put(ports[i % 2], &row, &value).await;
        want.push(row);
    }
}

const FELL_BEHIND: &str = "pigeonhole_serve_feed_cluster_fell_behind_total";
const REJOINED: &str = "pigeonhole_serve_feed_cluster_rejoined_total";
const CATCH_UP_WAITS: &str = "pigeonhole_serve_feed_cluster_catch_up_waits_total";

#[test]
fn a_hub_members_ring_passes_arrive_in_order_each_with_its_token_and_resume_from_any() {
    // #2014: a member hands its connection each ring pass as one batch. One that reads
    // nothing while 120 commits pass holds its buffer full, so each of its next passes takes
    // many entries. It still gets every commit once, in order, all from the ring, the first a
    // checkpoint; and a resume from a commit's token gets exactly the rest.
    let mut l = crate::serve::server_tests::limits();
    l.feed_client_bytes = 16 << 10;
    l.stream_idle = Duration::from_secs(600);
    let t = Three::start_with(l);
    let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
    t.rt.block_on(async {
        let mut s = narrow(c, users()).await;
        metric_until(c, MEMBERS, |n| n == 1).await;
        let mut want = Vec::new();
        write_kib([a, b], "bt-", 120, &mut want).await;
        let (seen, _) = commits(&mut s, want.len()).await;
        exactly(&seen, &want);
        assert!(seen[0].token.is_some(), "the first commit is a checkpoint");
        assert_eq!(
            metric(c, FELL_BEHIND).await,
            0,
            "every commit from the ring"
        );
        // A client that handled through commit `k` resumes from its last token, passing over
        // what it has by its positions (a stream's first checkpoint trails its commit).
        for k in [0, 37, 81, seen.len() - 2] {
            let token = seen[..=k]
                .iter()
                .rev()
                .find_map(|x| x.token.clone())
                .unwrap();
            let mut last: BTreeMap<String, u64> = BTreeMap::new();
            for x in &seen[..=k] {
                last.insert(x.partition.clone(), x.seqno);
            }
            let mut r = subscribe(
                a,
                SubscribeRequest {
                    start: Some(Start::Resume(token)),
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
            let rest: Vec<String> = seen[k + 1..].iter().flat_map(|x| x.rows.clone()).collect();
            let (again, _) = commits(&mut r, rest.len()).await;
            exactly(&again, &rest);
        }
    });
}

#[test]
fn a_hub_members_batch_that_waits_while_the_ring_overruns_it_misses_and_repeats_nothing() {
    // simp724's (2) on #1737's F3-0, for a cluster hub (#2014): with a few-entry ring, a
    // member's batch waits for room while the ring overruns it. Its entries are held, so the
    // batch goes whole; it then falls behind, catches up on a merge of its own from its last
    // checkpoint, and rejoins.
    let mut l = small_ring();
    l.feed_client_events = super::super::super::feed::CLIENT_EVENTS;
    l.feed_client_bytes = 8 << 10;
    let t = Three::start_with(l);
    let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
    t.rt.block_on(async {
        let mut fast = narrow(c, users()).await;
        let mut slow = narrow(c, users()).await;
        metric_until(c, MEMBERS, |n| n == 2).await;
        let mut want = Vec::new();
        write_kib([a, b], "ov-", 300, &mut want).await;
        for s in [&mut fast, &mut slow] {
            let (seen, _) = commits(s, want.len()).await;
            exactly(&seen, &want);
        }
        assert!(metric(c, FELL_BEHIND).await >= 1, "it fell off the ring");
        metric_until(c, MERGES, |n| n == 1).await;
        assert!(metric(c, REJOINED).await >= 1);
    });
}

/// #1737's per-feed liveness check for a cluster hub's members (#2014, simp724): `behind`
/// members fall off the ring and are kept off it, `live` others are drained, while writes go
/// on. Every round, each one behind delivers a commit within 2 s. Once writes stop, every
/// member delivers every commit once, in order, and each one behind rejoins the ring. None
/// waits for a catch-up slot.
fn hub_members_kept_behind_keep_moving(behind: usize, live: usize) {
    let mut l = crate::serve::server_tests::limits();
    l.feed_ring_bytes = 4096;
    l.feed_client_bytes = 4 << 10;
    l.max_catch_up = 8;
    l.stream_idle = Duration::from_secs(600);
    let t = Three::start_with(l);
    let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
    let n = behind + live;
    t.rt.block_on(async {
        let mut subs = Vec::new();
        for _ in 0..n {
            subs.push(narrow(c, users()).await);
        }
        metric_until(c, MEMBERS, |m| m == n as u64).await;
        let mut want = Vec::new();
        let mut got: Vec<Vec<Seen>> = vec![Vec::new(); n];
        // The first `behind` take nothing while 200 commits overrun the ring and their
        // windows.
        for chunk in 0..20 {
            write_kib([a, b], &format!("f{chunk:02}-"), 10, &mut want).await;
            for i in behind..n {
                got[i].extend(drain_for(&mut subs[i], Duration::from_millis(20)).await);
            }
        }
        // Then each round writes 10 and takes at most 4 from each one behind, so none of them
        // catches up while writes go on: each must still move.
        for round in 0..30 {
            write_kib([a, b], &format!("w{round:02}-"), 10, &mut want).await;
            for i in 0..behind {
                let before = got[i].len();
                while got[i].len() < before + 4 {
                    match tokio::time::timeout(Duration::from_secs(2), subs[i].message()).await {
                        Ok(Ok(Some(e))) => got[i].extend(seen_of(&e)),
                        _ => break,
                    }
                }
                assert!(
                    got[i].len() > before,
                    "member {i} delivered nothing for 2 s in round {round}"
                );
            }
            for i in behind..n {
                got[i].extend(drain_for(&mut subs[i], Duration::from_millis(20)).await);
            }
        }
        let merges = metric(c, MERGES).await;
        assert!(
            merges > behind as u64,
            "the members behind read merges of their own to the end of the writes: {merges}"
        );
        // Writes stop: each member gets the rest.
        for (i, s) in subs.iter_mut().enumerate() {
            let (rest, _) = commits(s, want.len() - got[i].len()).await;
            got[i].extend(rest);
            exactly(&got[i], &want);
        }
        metric_until(c, MERGES, |m| m == 1).await;
        assert!(metric(c, FELL_BEHIND).await >= behind as u64);
        assert!(metric(c, REJOINED).await >= behind as u64);
        assert_eq!(
            metric(c, CATCH_UP_WAITS).await,
            0,
            "no member waited for a slot"
        );
    });
}

#[test]
fn hub_members_kept_behind_while_writes_go_on_keep_moving_and_rejoin() {
    hub_members_kept_behind_keep_moving(4, 2);
}

#[test]
fn a_lone_hub_member_kept_behind_among_live_ones_keeps_moving_and_rejoins() {
    // simp724: the path with no other catch-up traffic around it.
    hub_members_kept_behind_keep_moving(1, 3);
}

#[test]
fn a_member_ended_idle_mid_pass_resumes_from_its_last_token_with_nothing_lost_or_repeated() {
    // simp724 on #2014: a pass of 1 KiB events goes out in budget-sized parts (4 KiB), so a
    // member whose client stops reading can end at its idle limit with part of a pass
    // queued. That ends the member: what was queued goes out, then the stream ends, and a
    // resume from the client's last token gets exactly the rest, its first commit a
    // checkpoint again.
    let mut l = crate::serve::server_tests::limits();
    l.feed_client_bytes = 4 << 10;
    l.stream_idle = Duration::from_millis(500);
    let t = Three::start_with(l);
    let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
    t.rt.block_on(async {
        let mut s = narrow(c, users()).await;
        metric_until(c, MEMBERS, |n| n == 1).await;
        let mut want = Vec::new();
        write_kib([a, b], "id-", 200, &mut want).await;
        // The client reads nothing until the member has ended at its idle limit: reading
        // sooner would keep it going.
        metric_until(c, "pigeonhole_serve_feed_idle_drops_total", |n| n == 1).await;
        let mut seen = Vec::new();
        // What was queued, then a single node's `DeadlineExceeded` (#2021).
        let end = loop {
            match tokio::time::timeout(Duration::from_secs(10), s.message())
                .await
                .expect("the stream ends at its idle limit")
            {
                Ok(Some(e)) => seen.extend(seen_of(&e)),
                Ok(None) => panic!("a clean end, after {} commits", seen.len()),
                Err(status) => break status,
            }
        };
        assert_eq!(end.code(), tonic::Code::DeadlineExceeded, "{end:?}");
        assert!(end.message().contains("--stream-idle-ms"), "{end:?}");
        assert!(
            !seen.is_empty() && seen.len() < want.len(),
            "{} of {} before the end",
            seen.len(),
            want.len()
        );
        assert!(seen[0].token.is_some(), "the first commit is a checkpoint");
        in_order(&seen);
        let token = seen.iter().rev().find_map(|x| x.token.clone()).unwrap();
        let mut last: BTreeMap<String, u64> = BTreeMap::new();
        for x in &seen {
            last.insert(x.partition.clone(), x.seqno);
        }
        let mut r = subscribe(
            c,
            SubscribeRequest {
                start: Some(Start::Resume(token)),
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
        let (rest, _) = commits(&mut r, want.len() - seen.len()).await;
        assert!(
            rest[0].token.is_some(),
            "the resumed stream's first commit is a checkpoint"
        );
        seen.extend(rest);
        exactly(&seen, &want);
    });
}

#[test]
fn a_spanning_commits_events_carry_its_xid_on_every_partition_and_no_other_commit_does() {
    // D329 2b: each partition's share of a spanning commit, the home's included, is
    // delivered with the transaction's id, so a subscriber can group them; a home-only
    // opted-in commit carries it too (feed1248's F3). A plain commit carries none, and an
    // aborted spanning transaction delivers nothing.
    use super::spanning::{abort, begin_spanning, commit_parts};
    let t = Three::start();
    let (a, b, c) = (t.ports[0], t.ports[1], t.ports[2]);
    t.rt.block_on(async {
        let mut s = subscribe(c, users()).await.unwrap();
        put(a, "plain", "v").await;
        // Home p2 (zed), branch p1 (alice).
        let id = begin_spanning(c, "zed").await.unwrap();
        commit_parts(c, &id, vec![put_row("alice", "1"), put_row("zed", "1")])
            .await
            .unwrap();
        let gone = begin_spanning(c, "zoe").await.unwrap();
        txn_write(
            c,
            &gone,
            vec![put_row("abe", "1"), put_row("zoe", "1")],
            None,
        )
        .await
        .unwrap();
        abort(c, &gone).await.unwrap();
        let home_only = begin_spanning(b, "zack").await.unwrap();
        commit_parts(b, &home_only, vec![put_row("zack", "1")])
            .await
            .unwrap();
        let mut got = Vec::new();
        while got.len() < 4 {
            let e = tokio::time::timeout(Duration::from_secs(10), s.message())
                .await
                .expect("a commit within 10 s")
                .unwrap()
                .expect("the stream ended");
            if let Some(Event::Commit(c)) = &e.event {
                for ch in &c.changes {
                    got.push((
                        String::from_utf8(ch.row.to_vec()).unwrap(),
                        e.partition.clone(),
                        String::from_utf8(e.xid.to_vec()).unwrap(),
                    ));
                }
            }
        }
        got.sort();
        let row = |r: &str, p: &str, x: &str| (r.to_owned(), p.to_owned(), x.to_owned());
        assert_eq!(
            got,
            [
                row("alice", "p1", &id),
                row("plain", "p2", ""),
                row("zack", "p2", &home_only),
                row("zed", "p2", &id),
            ]
        );
        // Nothing of the aborted one follows.
        let quiet = tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                match s.message().await {
                    Ok(Some(FeedEvent {
                        event: Some(Event::Commit(c)),
                        ..
                    })) => return c,
                    Ok(Some(_)) => continue,
                    other => panic!("{other:?}"),
                }
            }
        })
        .await;
        assert!(quiet.is_err(), "an aborted transaction delivered {quiet:?}");
    });
}

/// D331 PR 3b: `users` `[c, f)` moving from p1 to p2 at `phase` (none: done), as map
/// `version`, with the move's last seqno `final_seqno` from HANDING_OVER on.
fn moving_users(
    ports: &[u16],
    version: u64,
    phase: Option<crate::serve::proto::MovePhase>,
    final_seqno: u64,
) -> ClusterMap {
    use crate::serve::proto::{MoveKind, MovePhase, RangeMove};
    let mut m = the_map(version, &[ports[0], ports[1], ports[2]]);
    m.partitions.retain(|p| p.id != "p3");
    let handed = phase.is_none_or(super::super::map::committed);
    if handed {
        m.tables[0] = super::super::map::handed_over(&m.tables[0], b"c", Some(b"f"), "p2");
        m.partitions[0].epoch = 2;
    }
    if let Some(phase) = phase {
        m.moves.push(RangeMove {
            id: "mv1".into(),
            kind: MoveKind::Range as i32,
            table: "users".into(),
            start: Some(text("c")),
            end: Some(text("f")),
            from: "p1".into(),
            to: "p2".into(),
            phase: phase as i32,
            final_seqno: if phase == MovePhase::HandingOver || super::super::map::committed(phase) {
                final_seqno
            } else {
                0
            },
            handed_over_version: if handed { 5 } else { 0 },
            ..Default::default()
        });
    }
    m
}

/// D331 PR 3b (feed1248's race and point 2): a key's writes on a range's source come before
/// its writes on the target in a cluster subscription across the hand-over, however the
/// parts are read. A resume from a token before the source's last write, with the source's
/// node down: the target's write after the hand-over isn't delivered (its part is held at
/// the move's barrier) until the source comes back and delivers up to the move's last seqno.
/// No barrier ever reaches the client. Mutant: give the target's component no barrier, and
/// the target's write goes out while the source is down (the every-start rule's own mutant,
/// a start past `t_from`, is `serve::service`'s `barriers_for` test).
#[test]
fn a_keys_writes_on_the_source_come_before_its_writes_on_the_target_across_a_hand_over() {
    use crate::serve::proto::MovePhase;
    bounded(Duration::from_secs(180), || {
        let mut t = Three::start();
        let ports = t.ports.clone();
        let c = ports[2];
        let (token, final_seqno) = t.rt.block_on(async {
            let mut s = subscribe(
                c,
                SubscribeRequest {
                    checkpoint_every: 1,
                    ..users()
                },
            )
            .await
            .unwrap();
            put_when_leased(c, "d").await;
            let (first, _) = commits(&mut s, 1).await;
            put(c, "d", "2").await;
            let (last, _) = commits(&mut s, 1).await;
            (first[0].token.clone().unwrap(), last[0].seqno)
        });
        t.rt.block_on(async {
            for (version, phase) in [
                (2, MovePhase::Copying),
                (3, MovePhase::CatchingUp),
                (4, MovePhase::HandingOver),
                (5, MovePhase::HandedOver),
                (6, MovePhase::Cleaning),
            ] {
                for port in &ports {
                    client(*port)
                        .await
                        .push_map(crate::serve::proto::PushMapRequest {
                            map: Some(moving_users(&ports, version, Some(phase), final_seqno)),
                        })
                        .await
                        .unwrap();
                }
            }
            // The range's write on p2 once it serves it.
            eventually("p2 takes the range's writes", async || {
                data(c)
                    .await
                    .mutate(MutateRequest {
                        writes: Some(put_row("d", "3")),
                        durability: 0,
                    })
                    .await
                    .is_ok()
            })
            .await;
        });
        // The source's node goes down; the resume reads p2 alone for now.
        t.stop(0);
        t.rt.block_on(async {
            let mut r = subscribe(
                c,
                SubscribeRequest {
                    start: Some(Start::Resume(token.clone())),
                    allow_partial: true,
                    checkpoint_every: 1,
                    ..users()
                },
            )
            .await
            .unwrap();
            let mut early = Vec::new();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while let Ok(Ok(Some(e))) = tokio::time::timeout_at(deadline, r.message()).await {
                assert!(
                    !matches!(e.event, Some(Event::MoveBarrier(_))),
                    "a barrier reached the client"
                );
                if let Some(Event::Commit(ch)) = &e.event {
                    early.extend(
                        ch.changes
                            .iter()
                            .map(|x| (e.partition.clone(), x.row.to_vec())),
                    );
                }
            }
            assert!(
                !early.iter().any(|(_, row)| row == b"d"),
                "p2's write to d went out with the source down: {early:?}"
            );
            drop(r);
        });
        t.restart(0);
        t.rt.block_on(async {
            let mut r = subscribe(
                c,
                SubscribeRequest {
                    start: Some(Start::Resume(token)),
                    allow_partial: true,
                    checkpoint_every: 1,
                    ..users()
                },
            )
            .await
            .unwrap();
            let mut ds = Vec::new();
            while ds.len() < 2 {
                let e = tokio::time::timeout(Duration::from_secs(20), r.message())
                    .await
                    .expect("d's writes arrive")
                    .unwrap()
                    .expect("the stream goes on");
                assert!(
                    !matches!(e.event, Some(Event::MoveBarrier(_))),
                    "a barrier reached the client"
                );
                if let Some(Event::Commit(ch)) = &e.event
                    && ch.changes.iter().any(|x| x.row.as_ref() == b"d")
                {
                    ds.push((e.partition.clone(), ch.seqno));
                }
            }
            assert_eq!(ds[0], ("p1".to_owned(), final_seqno), "{ds:?}");
            assert_eq!(ds[1].0, "p2", "{ds:?}");
        });
    });
}

/// [`subscribe`], retried while a partition is between leases (a hand-over's epoch raise asks
/// its source's lease again): `PartitionUnavailable` is retry-safe, as clients treat it.
async fn subscribe_when_leased(port: u16, req: SubscribeRequest) -> tonic::Streaming<FeedEvent> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        match subscribe(port, req.clone()).await {
            Ok(s) => return s,
            Err(e)
                if code(&e) == "PartitionUnavailable" && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("{e}"),
        }
    }
}

/// Table `orders` on p1 alone, through the writer at `port`, retried while a node that just
/// started is still taking its peers' maps.
async fn orders_on_p1(port: u16) {
    table_on(port, "orders", "p1").await;
}

/// Table `name` on `partition` alone, as [`orders_on_p1`].
async fn table_on(port: u16, name: &str, partition: &str) {
    use crate::serve::proto::{
        CreateTableRequest, FamilySpec, TablePlacementRequest, schema_client::SchemaClient,
    };
    eventually("table orders on p1", async || {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        SchemaClient::new(channel)
            .create_table(CreateTableRequest {
                name: name.into(),
                families: vec![FamilySpec {
                    name: "f".into(),
                    ..Default::default()
                }],
                if_missing: true,
                placement: Some(TablePlacementRequest {
                    group: String::new(),
                    splits: Vec::new(),
                    partitions: vec![partition.into()],
                }),
            })
            .await
            .is_ok()
    })
    .await;
}

/// Puts `row` of `orders` through `port`, retried until it's taken: its commit's seqno.
async fn put_order(port: u16, row: &str) -> u64 {
    put_in(port, "orders", row).await
}

/// Puts `row` of `table` through `port`, as [`put_order`].
async fn put_in(port: u16, table: &str, row: &str) -> u64 {
    let mut out = 0;
    eventually("a row to be written", async || {
        let mut w = put_row(row, "v");
        w.row.as_mut().unwrap().table = table.into();
        match data(port)
            .await
            .mutate(MutateRequest {
                writes: Some(w),
                durability: 0,
            })
            .await
        {
            Ok(r) => {
                out = r.into_inner().commit.map_or(0, |c| c.seqno);
                true
            }
            Err(_) => false,
        }
    })
    .await;
    out
}

/// Takes `orders` `[start, ..)` from p1 to p2 through every phase (pushed to every node),
/// with the move's last seqno `final_seqno`, ending at CLEANING.
async fn move_orders(ports: &[u16], start: &str, final_seqno: u64) {
    move_orders_to(
        ports,
        start,
        final_seqno,
        crate::serve::proto::MovePhase::Cleaning,
    )
    .await;
}

/// [`move_orders`] up to phase `last`.
async fn move_orders_to(
    ports: &[u16],
    start: &str,
    final_seqno: u64,
    last: crate::serve::proto::MovePhase,
) {
    use crate::serve::proto::{GetMapRequest, MoveKind, MovePhase, PushMapRequest, RangeMove};
    let base = client(ports[0])
        .await
        .get_map(GetMapRequest {})
        .await
        .unwrap()
        .into_inner();
    let mut version = base.map_version;
    for phase in [
        MovePhase::Copying,
        MovePhase::CatchingUp,
        MovePhase::HandingOver,
        MovePhase::HandedOver,
        MovePhase::Cleaning,
    ]
    .into_iter()
    .filter(|p| *p <= last)
    {
        version += 1;
        let mut m = base.clone();
        m.map_version = version;
        let handed = super::super::map::committed(phase);
        if handed {
            let t = m.tables.iter_mut().find(|t| t.name == "orders").unwrap();
            *t = super::super::map::handed_over(t, start.as_bytes(), None, "p2");
            m.partitions
                .iter_mut()
                .find(|p| p.id == "p1")
                .unwrap()
                .epoch += 1;
        }
        m.moves = vec![RangeMove {
            id: "mvo".into(),
            kind: MoveKind::Range as i32,
            table: "orders".into(),
            start: (!start.is_empty()).then(|| text(start)),
            end: None,
            from: "p1".into(),
            to: "p2".into(),
            phase: phase as i32,
            final_seqno: if phase == MovePhase::HandingOver || super::super::map::committed(phase) {
                final_seqno
            } else {
                0
            },
            handed_over_version: if handed { base.map_version + 4 } else { 0 },
            ..Default::default()
        }];
        for port in ports {
            client(*port)
                .await
                .push_map(PushMapRequest {
                    map: Some(m.clone()),
                })
                .await
                .unwrap();
        }
    }
}

/// D331 PR 3b (Q2, feed1248's M2 and M3, and S3 on the code): a running subscription that
/// doesn't read the partition a move gave its table a range of ends with a checkpoint, then
/// `FeedRerouted`, a hub member and a member on a merge of its own alike; each resume from
/// its checkpoint reads the new partition from its hand-over on, with no barrier sent.
#[test]
fn a_move_to_a_partition_the_subscription_doesnt_read_reroutes_it_and_its_resume_reads_it() {
    bounded(Duration::from_secs(180), || {
        let t = Three::start();
        let ports = t.ports.clone();
        let (a, c) = (ports[0], ports[2]);
        t.rt.block_on(async {
            orders_on_p1(a).await;
            let req = |start| SubscribeRequest {
                tables: vec!["orders".into()],
                checkpoint_every: 1,
                start,
                ..Default::default()
            };
            let mut s = subscribe(c, req(None)).await.unwrap();
            put_order(c, "a1").await;
            let (seen, _) = commits(&mut s, 1).await;
            assert_eq!(seen[0].partition, "p1");
            // A second subscriber from the first's token: a merge of its own (feed1248's S3),
            // not the hub's.
            let mut own = subscribe(c, req(Some(Start::Resume(seen[0].token.clone().unwrap()))))
                .await
                .unwrap();
            let last = put_order(c, "a2").await;
            let mut tokens = Vec::new();
            for sub in [&mut s, &mut own] {
                let (seen, _) = commits(sub, 1).await;
                assert_eq!(seen[0].rows, ["a2"], "{seen:?}");
                tokens.push(seen[0].token.clone().expect("a commit carries a token"));
            }
            move_orders(&ports, "m", last).await;
            // Each stream ends: its last event carries a token, then FeedRerouted.
            for (sub, token) in [&mut s, &mut own].into_iter().zip(tokens.iter_mut()) {
                let end = loop {
                    match tokio::time::timeout(Duration::from_secs(20), sub.message())
                        .await
                        .expect("the merge notices the map")
                    {
                        Ok(Some(e)) => {
                            assert!(
                                !matches!(e.event, Some(Event::MoveBarrier(_))),
                                "a barrier reached the client"
                            );
                            if !e.token.is_empty() {
                                *token = e.token.clone();
                            }
                        }
                        Ok(None) => panic!("ended without an error"),
                        Err(e) => break e,
                    }
                };
                assert_eq!(code(&end), "FeedRerouted", "{end}");
            }
            // p2's write after the hand-over reaches both resumes, with no barrier.
            put_order(c, "zz").await;
            for token in tokens {
                let mut r = subscribe_when_leased(c, req(Some(Start::Resume(token)))).await;
                let (seen, other) = commits(&mut r, 1).await;
                assert_eq!(seen[0].partition, "p2", "{seen:?}");
                assert_eq!(seen[0].rows, ["zz"]);
                assert!(
                    !other
                        .iter()
                        .any(|e| matches!(e.event, Some(Event::MoveBarrier(_)))),
                    "a barrier reached the client"
                );
            }
        });
    });
}

/// D331 PR 3b (cli692's call): a move that takes all of a table from a partition doesn't
/// expire a token from before it. The source serves the table up to the move's last seqno
/// and its part ends there, then the target's writes follow.
#[test]
fn a_token_from_before_a_whole_table_move_resumes_through_the_source_then_the_target() {
    bounded(Duration::from_secs(180), || {
        let t = Three::start();
        let ports = t.ports.clone();
        let (a, c) = (ports[0], ports[2]);
        t.rt.block_on(async {
            orders_on_p1(a).await;
            let mut s = subscribe(
                c,
                SubscribeRequest {
                    tables: vec!["orders".into()],
                    checkpoint_every: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            put_order(c, "k1").await;
            let (first, _) = commits(&mut s, 1).await;
            let token = first[0].token.clone().unwrap();
            let last = put_order(c, "k2").await;
            drop(s);
            move_orders(&ports, "", last).await;
            put_order(c, "k3").await;
            let mut r = subscribe_when_leased(
                c,
                SubscribeRequest {
                    tables: vec!["orders".into()],
                    checkpoint_every: 1,
                    start: Some(Start::Resume(token)),
                    ..Default::default()
                },
            )
            .await;
            let (seen, other) = commits(&mut r, 2).await;
            assert_eq!(
                seen.iter()
                    .map(|s| (s.partition.as_str(), s.rows[0].as_str()))
                    .collect::<Vec<_>>(),
                [("p1", "k2"), ("p2", "k3")]
            );
            assert!(
                !other
                    .iter()
                    .any(|e| matches!(e.event, Some(Event::MoveBarrier(_)))),
                "a barrier reached the client"
            );
        });
    });
}

/// D331 PR 3b (cli692's M1): between a move's HANDED_OVER and CLEANING the target serves none
/// of the range yet and has no hand-over position: a subscription isn't rerouted then, and a
/// resume that would gain the target is refused retry-safe (`PartitionUnavailable`), not
/// expired. From CLEANING the subscription is rerouted, and its resume reads the target.
#[test]
fn between_the_hand_over_and_cleaning_a_resume_waits_rather_than_expiring() {
    use crate::serve::proto::{GetMapRequest, MovePhase, PushMapRequest};
    bounded(Duration::from_secs(180), || {
        let t = Three::start();
        let ports = t.ports.clone();
        let (a, c) = (ports[0], ports[2]);
        t.rt.block_on(async {
            orders_on_p1(a).await;
            let req = |start| SubscribeRequest {
                tables: vec!["orders".into()],
                checkpoint_every: 1,
                start,
                ..Default::default()
            };
            let mut s = subscribe(c, req(None)).await.unwrap();
            let last = put_order(c, "a1").await;
            let (seen, _) = commits(&mut s, 1).await;
            let token = seen[0].token.clone().unwrap();
            move_orders_to(&ports, "m", last, MovePhase::HandedOver).await;
            // Not rerouted: nothing ends it within a while.
            assert!(
                tokio::time::timeout(Duration::from_millis(1500), s.message())
                    .await
                    .is_err(),
                "rerouted before CLEANING"
            );
            // A resume gaining p2 now asks again, retry-safe.
            let Err(e) = subscribe(c, req(Some(Start::Resume(token.clone())))).await else {
                panic!("refused before CLEANING");
            };
            assert_eq!(code(&e), "PartitionUnavailable", "{e}");
            // CLEANING: rerouted, and the resume reads p2.
            let m = client(a)
                .await
                .get_map(GetMapRequest {})
                .await
                .unwrap()
                .into_inner();
            let mut cleaning = m.clone();
            cleaning.map_version += 1;
            cleaning.moves[0].phase = MovePhase::Cleaning as i32;
            for port in &ports {
                client(*port)
                    .await
                    .push_map(PushMapRequest {
                        map: Some(cleaning.clone()),
                    })
                    .await
                    .unwrap();
            }
            let end = loop {
                match tokio::time::timeout(Duration::from_secs(20), s.message())
                    .await
                    .expect("the merge notices CLEANING")
                {
                    Ok(Some(_)) => {}
                    Ok(None) => panic!("ended without an error"),
                    Err(e) => break e,
                }
            };
            assert_eq!(code(&end), "FeedRerouted", "{end}");
            put_order(c, "zz").await;
            let mut r = subscribe_when_leased(c, req(Some(Start::Resume(token)))).await;
            let (seen, _) = commits(&mut r, 1).await;
            assert_eq!(seen[0].partition, "p2", "{seen:?}");
        });
    });
}

/// D331 PR 3b (feed1248's M1): a token from before a move took all of `orders` off p1, of a
/// subscription of `orders` and `items` (on `items_on`), resumes with nothing lost and
/// nothing expired. With `items` on p1 too, p1's part reads `orders` up to the move's last
/// seqno beside `items`; on another partition, p1 leaves `items` out rather than refusing.
fn whole_table_move_beside_another_table(items_on: &str) {
    let items_on = items_on.to_owned();
    bounded(Duration::from_secs(180), move || {
        let t = Three::start();
        let ports = t.ports.clone();
        let (a, c) = (ports[0], ports[2]);
        t.rt.block_on(async {
            orders_on_p1(a).await;
            table_on(a, "items", &items_on).await;
            let req = |start| SubscribeRequest {
                tables: vec!["items".into(), "orders".into()],
                checkpoint_every: 1,
                start,
                ..Default::default()
            };
            let mut s = subscribe(c, req(None)).await.unwrap();
            put_in(c, "items", "i1").await;
            let (first, _) = commits(&mut s, 1).await;
            let token = first[0].token.clone().unwrap();
            drop(s);
            let last = put_order(c, "k2").await;
            move_orders(&ports, "", last).await;
            put_order(c, "k3").await;
            let mut r = subscribe_when_leased(c, req(Some(Start::Resume(token)))).await;
            let (seen, other) = commits(&mut r, 2).await;
            assert_eq!(
                seen.iter()
                    .map(|s| (s.partition.as_str(), s.rows[0].as_str()))
                    .collect::<Vec<_>>(),
                [("p1", "k2"), ("p2", "k3")]
            );
            assert!(
                !other
                    .iter()
                    .any(|e| matches!(e.event, Some(Event::MoveBarrier(_)))),
                "a barrier reached the client"
            );
        });
    });
}

#[test]
fn a_resume_across_a_whole_table_move_reads_the_moved_table_beside_one_that_stays() {
    whole_table_move_beside_another_table("p1");
}

#[test]
fn a_resume_across_a_whole_table_move_leaves_out_a_table_never_on_the_source() {
    whole_table_move_beside_another_table("p2");
}

/// D331 PR 3b (feed1248's M3): a subscription connected across a move of all of `orders`
/// off p1 retires p1's part once it has read up to the move's last seqno, with no
/// reconnect: its checkpoints leave p1 out, so a resume from them never needs p1's
/// moved-away row (which PR 5 prunes). At CLEANING it's rerouted, and the resume reads p2.
#[test]
fn a_subscription_connected_across_a_whole_table_move_retires_the_source_part() {
    use crate::serve::cluster::token::ClusterToken;
    use crate::serve::proto::{GetMapRequest, MovePhase, PushMapRequest};
    bounded(Duration::from_secs(180), || {
        let t = Three::start();
        let ports = t.ports.clone();
        let (a, c) = (ports[0], ports[2]);
        t.rt.block_on(async {
            orders_on_p1(a).await;
            let req = |start| SubscribeRequest {
                tables: vec!["orders".into()],
                checkpoint_every: 1,
                start,
                ..Default::default()
            };
            let mut s = subscribe(c, req(None)).await.unwrap();
            let last = put_order(c, "k1").await;
            let (seen, _) = commits(&mut s, 1).await;
            let mut token = seen[0].token.clone().unwrap();
            move_orders_to(&ports, "", last, MovePhase::HandedOver).await;
            // p1's part ends at its next event (quiet progress at the latest): a checkpoint
            // without p1 follows.
            let reads = |b: &Bytes| {
                ClusterToken::decode(b)
                    .unwrap()
                    .components
                    .iter()
                    .any(|c| c.partition == "p1")
            };
            let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
            while reads(&token) {
                let e = tokio::time::timeout_at(deadline, s.message())
                    .await
                    .expect("p1's part retires")
                    .unwrap()
                    .expect("the stream stays open");
                assert!(
                    !matches!(e.event, Some(Event::MoveBarrier(_))),
                    "a barrier reached the client"
                );
                if !e.token.is_empty() {
                    token = e.token.clone();
                }
            }
            // CLEANING: rerouted, and the resume reads p2 (no p1 in its token).
            let m = client(a)
                .await
                .get_map(GetMapRequest {})
                .await
                .unwrap()
                .into_inner();
            let mut cleaning = m.clone();
            cleaning.map_version += 1;
            cleaning.moves[0].phase = MovePhase::Cleaning as i32;
            for port in &ports {
                client(*port)
                    .await
                    .push_map(PushMapRequest {
                        map: Some(cleaning.clone()),
                    })
                    .await
                    .unwrap();
            }
            let end = loop {
                match tokio::time::timeout(Duration::from_secs(20), s.message())
                    .await
                    .expect("the merge notices CLEANING")
                {
                    Ok(Some(e)) => {
                        if !e.token.is_empty() {
                            token = e.token.clone();
                        }
                    }
                    Ok(None) => panic!("ended without an error"),
                    Err(e) => break e,
                }
            };
            assert_eq!(code(&end), "FeedRerouted", "{end}");
            assert!(!reads(&token), "the last checkpoint still reads p1");
            put_order(c, "k3").await;
            let mut r = subscribe_when_leased(c, req(Some(Start::Resume(token)))).await;
            let (seen, other) = commits(&mut r, 1).await;
            assert_eq!(seen[0].partition, "p2", "{seen:?}");
            assert_eq!(seen[0].rows, ["k3"]);
            assert!(
                !other
                    .iter()
                    .any(|e| matches!(e.event, Some(Event::MoveBarrier(_)))),
                "a barrier reached the client"
            );
        });
    });
}
