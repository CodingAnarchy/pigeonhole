//! A cluster subscription's idle limit, its merge a task on the feed runtime handing the
//! client batches (#1798 (a)): a client that leaves events untaken loses its subscription,
//! its `--max-streams` place and its parts at `--stream-idle-ms`, counted once; a client that
//! reads a quiet subscription keeps it, however long nothing happens.

use super::*;
use crate::serve::proto::{
    Empty, MutateRequest, SubscribeRequest, admin_client::AdminClient, feed_client::FeedClient,
    feed_event::Event,
};

const IDLE: Duration = Duration::from_millis(400);

/// Nodes a (p1), b (p2) and c (routes only, one `--max-streams` place), with a short idle
/// limit everywhere.
fn nodes(
    rt: &tokio::runtime::Runtime,
) -> (
    Vec<tokio::sync::oneshot::Sender<()>>,
    [u16; 3],
    Vec<pigeonhole::Pigeonhole>,
) {
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let mut map = the_map(1, &ports);
    map.partitions.retain(|p| p.id != "p3");
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for (l, (id, local)) in
        listeners
            .into_iter()
            .zip([("a", Some("p1")), ("b", Some("p2")), ("c", None)])
    {
        let dir = test_dir();
        super::super::write_map(&dir, &super::super::map::canonical_json(&map)).unwrap();
        let mut limits = crate::serve::server_tests::limits();
        limits.stream_idle = IDLE;
        if id == "c" {
            limits.max_streams = 1;
        }
        let (stop, db) = data_node_limits(rt, &dir, id, local, l, "", limits);
        stops.push(stop);
        dbs.extend(db);
    }
    (stops, ports, dbs)
}

async fn subscribe(
    port: u16,
) -> Result<tonic::Streaming<crate::serve::proto::FeedEvent>, tonic::Status> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    Ok(FeedClient::new(channel)
        .subscribe(SubscribeRequest {
            tables: vec!["users".into()],
            ..Default::default()
        })
        .await?
        .into_inner())
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

async fn idle_drops(port: u16) -> u64 {
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
        .find_map(|l| l.strip_prefix("pigeonhole_serve_feed_idle_drops_total "))
        .map(|v| v.trim().parse().unwrap())
        .expect("the counter is rendered")
}

#[test]
fn a_client_reading_a_quiet_subscription_keeps_it_past_the_idle_limit() {
    // simp724's point 1: a client with nothing waiting for it is waiting for a commit, so a
    // reader of a quiet subscription is never idle, with the limit (400 ms) far below the
    // quiet interval (10 s).
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = nodes(&rt);
    let [a, _, c] = ports;
    rt.block_on(async {
        let mut s = subscribe(c).await.unwrap();
        // Ten limits with no commit, reading all along.
        let until = tokio::time::Instant::now() + IDLE * 10;
        while let Ok(m) = tokio::time::timeout_at(until, s.message()).await {
            let e = m.expect("no error while reading").expect("not ended");
            assert!(!matches!(e.event, Some(Event::Commit(_))), "{e:?}");
        }
        put(a, "alice", "1").await;
        let commit = loop {
            let e = tokio::time::timeout(Duration::from_secs(10), s.message())
                .await
                .expect("the commit arrives")
                .unwrap()
                .expect("not ended");
            if let Some(Event::Commit(c)) = e.event {
                break c;
            }
        };
        assert_eq!(commit.changes[0].row, b"alice".to_vec());
        assert_eq!(idle_drops(c).await, 0);
    });
}

#[test]
fn a_client_that_stops_reading_loses_its_subscription_and_slot_at_the_idle_limit() {
    // simp724's R1: a client whose window is full leaves the merge's batches untaken, so the
    // merge task ends it: its `--max-streams` place (c has one) and its parts are freed, and
    // it's counted once.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_stops, ports, _dbs) = nodes(&rt);
    let [a, _, c] = ports;
    rt.block_on(async {
        let stuck = subscribe(c).await.unwrap();
        // c's one place is taken.
        let Err(e) = subscribe(c).await else {
            panic!("no place left");
        };
        assert_eq!(e.code(), tonic::Code::Unavailable, "{e:?}");
        assert!(e.message().contains("--max-streams"), "{e:?}");
        // Far more than the HTTP/2 windows and the client's buffers hold, never read.
        let big = "x".repeat(4 << 10);
        for i in 0..600 {
            put(a, &format!("a{i:04}"), &big).await;
        }
        tokio::time::sleep(IDLE * 4).await;
        assert_eq!(idle_drops(c).await, 1, "the stuck subscription, once");
        // Its place is free: a new subscription gets it.
        let fresh = subscribe(c).await;
        assert!(fresh.is_ok(), "{:?}", fresh.err());
        drop(stuck);
    });
}
