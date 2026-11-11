//! A fanned-out read's pages count against `--inflight-bytes` on its entry node (#1715): the
//! request's holding, taken before any read and handed to its reply as its charge, so a
//! second read waits for room (and answers `Overloaded` past its deadline) where a single
//! node's reply would.

use super::*;
use crate::serve::proto::{
    Element, Empty, MultiGetRequest, NearestRequest, TraverseRequest, Vector,
    admin_client::AdminClient, graph_client::GraphClient, vectors_client::VectorsClient,
};

/// The node at `port`'s charged reply bytes now (`pigeonhole_serve_inflight_reply_bytes`).
async fn reply_bytes(port: u16) -> u64 {
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
        .find_map(|l| l.strip_prefix("pigeonhole_serve_inflight_reply_bytes "))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("no reply-bytes gauge in:\n{text}"))
}

async fn channel(port: u16) -> tonic::transport::Channel {
    tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

/// A fanned-out read of each kind, whose holding is about `page` bytes (a nearest search's
/// two of `page / 2`): over p1 (node a) and p2, or over p1 alone (`p1_only`).
async fn read(port: u16, kind: usize, page: u64, p1_only: bool) -> Result<(), tonic::Status> {
    use super::super::super::proto::{KeyList, nearest_request::Source};
    let ch = channel(port).await;
    let keys: Vec<prost::bytes::Bytes> = if p1_only {
        vec![b"a1".to_vec().into()]
    } else {
        vec![b"a1".to_vec().into(), b"x1".to_vec().into()]
    };
    match kind {
        0 => super::super::super::proto::data_client::DataClient::new(ch)
            .multi_get(MultiGetRequest {
                table: "users".into(),
                keys,
                page_bytes: page,
                ..Default::default()
            })
            .await
            .map(|_| ()),
        1 => VectorsClient::new(ch)
            .nearest(NearestRequest {
                table: "users".into(),
                family: "emb".into(),
                query: Some(Vector {
                    element: Element::F32 as i32,
                    dim: 4,
                    data: [0f32; 4]
                        .iter()
                        .flat_map(|x| x.to_le_bytes())
                        .collect::<Vec<u8>>()
                        .into(),
                }),
                k: 3,
                source: p1_only.then(|| Source::Keys(KeyList { keys })),
                page_bytes: page / 2,
                ..Default::default()
            })
            .await
            .map(|_| ()),
        _ => GraphClient::new(ch)
            .traverse(TraverseRequest {
                table: "users".into(),
                // From a2, which has no edges, a traversal reads p1 alone.
                seeds: vec![
                    if p1_only {
                        b"a2".to_vec()
                    } else {
                        b"a1".to_vec()
                    }
                    .into(),
                ],
                family: "out".into(),
                hops: 2,
                page_bytes: page,
                ..Default::default()
            })
            .await
            .map(|_| ()),
    }
}

/// Node a over p1 under `limits`, with b (p2) bound and never answering: a fanned-out read
/// from a holds its budget while it waits for p2, up to its deadline.
fn with_silent_b(
    rt: &tokio::runtime::Runtime,
    limits: crate::serve::config::Limits,
) -> (
    Vec<tokio::sync::oneshot::Sender<()>>,
    u16,
    std::net::TcpListener,
    pigeonhole::Pigeonhole,
) {
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let map = the_map(1, &ports);
    let mut it = listeners.into_iter();
    let dir = test_dir();
    super::super::write_map(&dir, &super::super::map::canonical_json(&map)).unwrap();
    let (stop, db) = data_node_with(rt, &dir, "a", Some("p1"), it.next().unwrap(), limits);
    let db = db.unwrap();
    rt.block_on(async {
        let mut d = data(ports[0]).await;
        d.mutate(super::super::super::proto::MutateRequest {
            writes: Some(graph_row(
                b"a1",
                [0.0; 4],
                &[(b"a2".to_vec(), true), (b"x1".to_vec(), true)],
            )),
            durability: 0,
        })
        .await
        .unwrap();
    });
    (vec![stop], ports[0], it.next().unwrap(), db)
}

#[test]
fn a_fanned_out_read_holds_its_pages_budget_and_a_second_one_waits_for_room() {
    // The first read of each kind holds its page budget on a while p2's node never answers,
    // until its deadline; the gauge shows it. A second read (of p1 alone) finds no room, so
    // it waits for the first's holding to come back and then answers. Before #1715 the pages
    // weren't counted: the second read answered at once.
    for kind in 0..3 {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let page = 80_000u64;
        let limits = crate::serve::config::Limits {
            inflight_bytes: 100_000,
            request_timeout: Duration::from_millis(1500),
            ..crate::serve::server_tests::limits()
        };
        let (_stops, a, _silent_b, db) = with_silent_b(&rt, limits);
        rt.block_on(async {
            let started = std::time::Instant::now();
            let first = tokio::spawn(read(a, kind, page, false));
            eventually("the first read to hold its budget", async || {
                reply_bytes(a).await >= page
            })
            .await;
            // Later than the first by more than the gap between their deadlines' ends could
            // close: the first's holding comes back (at its deadline, 1.5 s) well within the
            // second's.
            tokio::time::sleep(Duration::from_millis(600)).await;
            read(a, kind, page, true)
                .await
                .unwrap_or_else(|e| panic!("kind {kind}: {e}"));
            // It answered only once the first one's deadline gave its holding back; with
            // nothing holding it, it would have answered at once, at about 0.6 s.
            assert!(
                started.elapsed() >= Duration::from_millis(1400),
                "kind {kind}: the second read didn't wait ({:?} after the first began)",
                started.elapsed()
            );
            let first = first.await.unwrap();
            assert!(first.is_err(), "kind {kind}: {first:?}");
            eventually("the holdings to be given back", async || {
                reply_bytes(a).await == 0
            })
            .await;
        });
        drop(db);
    }
}

#[test]
fn a_read_whose_budget_is_more_than_the_servers_runs_alone_and_two_take_turns() {
    // cli692's S2 on #1715: a holding is clamped to the whole of `--inflight-bytes`, so one
    // read asking for more than it runs; two of them take turns, and both answer.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let limits = crate::serve::config::Limits {
        inflight_bytes: 50_000,
        ..crate::serve::server_tests::limits()
    };
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let map = the_map(1, &ports);
    let mut dbs = Vec::new();
    let mut stops = Vec::new();
    for ((l, id), p) in listeners
        .into_iter()
        .zip(["a", "b", "c"])
        .zip(["p1", "p2", "p3"])
    {
        let dir = test_dir();
        super::super::write_map(&dir, &super::super::map::canonical_json(&map)).unwrap();
        let (stop, db) = data_node_with(&rt, &dir, id, Some(p), l, limits);
        stops.push(stop);
        dbs.push(db);
    }
    rt.block_on(async {
        for (row, port) in [(b"a1", ports[0]), (b"x1", ports[1])] {
            data(port)
                .await
                .mutate(super::super::super::proto::MutateRequest {
                    writes: Some(graph_row(row, [0.0; 4], &[])),
                    durability: 0,
                })
                .await
                .unwrap();
        }
        for kind in 0..3 {
            read(ports[0], kind, 80_000, false)
                .await
                .unwrap_or_else(|e| panic!("kind {kind}, alone: {e}"));
            let (x, y) = tokio::join!(
                read(ports[0], kind, 80_000, false),
                read(ports[0], kind, 80_000, false)
            );
            x.unwrap_or_else(|e| panic!("kind {kind}: {e}"));
            y.unwrap_or_else(|e| panic!("kind {kind}: {e}"));
        }
        assert_eq!(reply_bytes(ports[0]).await, 0);
    });
    for db in dbs.into_iter().flatten() {
        let _ = db.close();
    }
}
