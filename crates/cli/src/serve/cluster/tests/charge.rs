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
            // The first one's own answer: p2 never answered by its deadline.
            let e = first.await.unwrap().unwrap_err();
            assert_eq!(code(&e), "PartitionUnavailable", "kind {kind}: {e}");
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

/// The JSON mapping's bytes: standard base64, padded.
fn b64(s: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for c in s.chunks(3) {
        let b = [c[0], *c.get(1).unwrap_or(&0), *c.get(2).unwrap_or(&0)];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= c.len() {
                out.push(A[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// Three live data nodes over [`the_map`] (`users` split at "m": p1 on a, p2 on b) under
/// `limits`; their stops, ports and partition databases.
fn live_nodes(
    rt: &tokio::runtime::Runtime,
    limits: crate::serve::config::Limits,
) -> (
    Vec<tokio::sync::oneshot::Sender<()>>,
    [u16; 3],
    Vec<pigeonhole::Pigeonhole>,
) {
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: [u16; 3] = std::array::from_fn(|i| listeners[i].local_addr().unwrap().port());
    let map = the_map(1, &ports);
    let mut stops = Vec::new();
    let mut dbs = Vec::new();
    for ((l, id), p) in listeners
        .into_iter()
        .zip(["a", "b", "c"])
        .zip(["p1", "p2", "p3"])
    {
        let dir = test_dir();
        super::super::write_map(&dir, &super::super::map::canonical_json(&map)).unwrap();
        let (stop, db) = data_node_with(rt, &dir, id, Some(p), l, limits);
        stops.push(stop);
        dbs.push(db.unwrap());
    }
    (stops, ports, dbs)
}

#[test]
fn a_wait_for_room_that_outlasts_the_deadline_answers_overloaded() {
    // cli692's N1 on #1730: a fanned-out multi-get's HTTP/JSON reply that its client never
    // reads keeps its charge (the holding it took over) until written or idled out, so a
    // second fanned-out read finds no room within its deadline and answers `Overloaded`.
    use std::io::Write as _;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let limits = crate::serve::config::Limits {
        inflight_bytes: 16 << 20,
        request_timeout: Duration::from_millis(400),
        stream_idle: Duration::from_secs(30),
        ..crate::serve::server_tests::limits()
    };
    let (_stops, ports, dbs) = live_nodes(&rt, limits);
    // Rows of about 1 MB (two cells, each write under the 1 MiB message limit), four in
    // p1 and four in p2.
    let rows: Vec<String> = (0..4)
        .map(|i| format!("a{i}"))
        .chain((0..4).map(|i| format!("h{i}")))
        .collect();
    rt.block_on(async {
        for r in &rows {
            data(ports[0])
                .await
                .mutate(super::super::super::proto::MutateRequest {
                    writes: Some(cells_row(r.as_bytes(), &[480_000, 480_000], 0)),
                    durability: 0,
                })
                .await
                .unwrap();
        }
    });
    let keys: Vec<String> = rows.iter().map(|r| b64(r.as_bytes())).collect();
    let body = serde_json::to_vec(
        &serde_json::json!({"table": "users", "keys": keys, "page_bytes": 8 << 20}),
    )
    .unwrap();
    let mut stuck = std::net::TcpStream::connect(("127.0.0.1", ports[0])).unwrap();
    write!(
        stuck,
        "POST /v1/Data/MultiGet HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .unwrap();
    stuck.write_all(&body).unwrap();
    rt.block_on(async {
        // About 10 MB of JSON (the reply's charge fitted to its body) stays charged while
        // unread, leaving less than a second read's 8 MiB holding.
        eventually("the unread reply's charge", async || {
            reply_bytes(ports[0]).await > 8 << 20
        })
        .await;
        let e = super::super::super::proto::data_client::DataClient::new(channel(ports[0]).await)
            .multi_get(MultiGetRequest {
                table: "users".into(),
                keys: rows.iter().map(|r| r.as_bytes().to_vec().into()).collect(),
                page_bytes: 8 << 20,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "Overloaded", "{e}");
    });
    // The client goes: its reply's charge comes back.
    drop(stuck);
    rt.block_on(async {
        eventually("the charge back", async || reply_bytes(ports[0]).await == 0).await;
    });
    for db in dbs {
        let _ = db.close();
    }
}

#[test]
fn a_traversal_whose_visited_set_leaves_no_room_for_a_page_ends_cut() {
    // cli692's N2 on #1730: once the visited set leaves less of the budget than a hop page,
    // the next hop can't be read charged, so the traversal ends after the hop it has, cut
    // as a byte budget cuts it. With room, the same traversal isn't cut.
    for (inflight, cut) in [(20_000usize, true), (16 << 20, false)] {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let limits = crate::serve::config::Limits {
            inflight_bytes: inflight,
            ..crate::serve::server_tests::limits()
        };
        let (_stops, ports, dbs) = live_nodes(&rt, limits);
        // A hub in p1 with 250 edges to rows in p1 (which have none).
        let edges: Vec<(Vec<u8>, bool)> = (0..250)
            .map(|i| (format!("b{i:03}").into_bytes(), true))
            .collect();
        let got = rt.block_on(async {
            data(ports[0])
                .await
                .mutate(super::super::super::proto::MutateRequest {
                    writes: Some(graph_row(b"a1", [0.0; 4], &edges)),
                    durability: 0,
                })
                .await
                .unwrap();
            let got = GraphClient::new(channel(ports[0]).await)
                .traverse(TraverseRequest {
                    table: "users".into(),
                    seeds: vec![b"a1".to_vec().into()],
                    family: "out".into(),
                    hops: 3,
                    page_bytes: 8_000,
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            eventually("the budget back", async || reply_bytes(ports[0]).await == 0).await;
            got
        });
        assert_eq!(got.hops.len(), 1, "budget {inflight}: {got:?}");
        assert_eq!(got.hops[0].nodes.len(), 250, "budget {inflight}");
        assert_eq!(got.stats.unwrap().cut_by_budget, cut, "budget {inflight}");
        for db in dbs {
            let _ = db.close();
        }
    }
}

#[test]
fn fanned_out_reads_through_every_node_under_a_small_budget_all_answer() {
    // perf287's g78 on #1730: with every node an entry and the budget filled by entry
    // holdings, a forwarded partition read that needed budget on its owner waited on
    // holdings that waited on it, and nothing answered. A fan-out part is charged once, at
    // its entry: the owner serves it with no budget, so every read here answers.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let page = 80_000u64;
    let limits = crate::serve::config::Limits {
        // Two holdings fill a node.
        inflight_bytes: 160_000,
        request_timeout: Duration::from_secs(5),
        ..crate::serve::server_tests::limits()
    };
    let (_stops, ports, dbs) = live_nodes(&rt, limits);
    rt.block_on(async {
        for (row, port, edges) in [
            (&b"a1"[..], ports[0], vec![(b"x1".to_vec(), true)]),
            (&b"x1"[..], ports[1], vec![(b"a1".to_vec(), true)]),
        ] {
            data(port)
                .await
                .mutate(super::super::super::proto::MutateRequest {
                    writes: Some(graph_row(row, [0.0; 4], &edges)),
                    durability: 0,
                })
                .await
                .unwrap();
        }
        let mut reads = tokio::task::JoinSet::new();
        for &port in &ports {
            for kind in 0..3 {
                for _ in 0..6 {
                    reads.spawn(async move { (port, kind, read(port, kind, page, false).await) });
                }
            }
        }
        while let Some(done) = reads.join_next().await {
            let (port, kind, r) = done.unwrap();
            r.unwrap_or_else(|e| panic!("kind {kind} through {port}: {e}"));
        }
        for &port in &ports {
            eventually("every holding back", async || reply_bytes(port).await == 0).await;
        }
    });
    for db in dbs {
        let _ = db.close();
    }
}

#[test]
fn a_client_token_cant_send_a_fan_out_part_past_the_budget() {
    // The coordinator's condition on #1730's fix: `fanout_part` skips the owner's budget, so
    // only a peer may send it. `Cluster.Forward` takes cluster scope: a client's token is
    // refused before the flag is read.
    use super::super::super::proto::{
        ForwardRequest, cluster_client::ClusterClient, forward_request,
    };
    let all = format!("write t0ken-a\ncluster {NODE_TOKEN}\n");
    let t = Three::start([&all, &all, &all]);
    t.rt.block_on(async {
        let req = ForwardRequest {
            map_version: 1,
            caller: None,
            fanout_part: true,
            op: Some(forward_request::Op::MultiGet(MultiGetRequest {
                table: "users".into(),
                keys: vec![b"a1".to_vec().into()],
                page_bytes: 8 << 20,
                ..Default::default()
            })),
        };
        let e = ClusterClient::new(channel(t.ports[0]).await)
            .forward(with(req, Some("t0ken-a")))
            .await
            .unwrap_err();
        assert_eq!(code(&e), "PermissionDenied", "{e}");
    });
}
