use super::*;

// ---- Unary read replies charged to --inflight-bytes (#1567) ----

use std::io::{Read, Write};
use std::net::TcpStream;

/// A server whose in-flight budget is `budget` bytes, its waits `timeout`, and a client that
/// takes nothing of a reply for `idle` loses it; `t` holds `rows` rows of 1 MiB each.
fn budgeted(budget: usize, timeout: Duration, idle: Duration, rows: usize) -> Server {
    let mut l = limits();
    l.inflight_bytes = budget;
    l.request_timeout = timeout;
    l.stream_idle = idle;
    let s = Server::start("", l);
    let t = s
        .shared
        .router
        .service
        .db
        .table("t")
        .unwrap()
        .open()
        .unwrap();
    for i in 0..rows {
        t.mutate(format!("big{i}").as_bytes())
            .put("f", b"q", &vec![b'x'; 1 << 20])
            .commit()
            .unwrap();
    }
    s
}

/// `MultiGet` of every big row, up to 8 MiB, as HTTP/JSON.
fn big_multi_get(rows: usize) -> Json {
    let keys: Vec<String> = (0..rows)
        .map(|i| b64(format!("big{i}").as_bytes()))
        .collect();
    json!({"table": "t", "keys": keys, "page_bytes": 8 << 20})
}

/// The same, sent on a raw connection whose reply is never read.
fn stuck_multi_get(s: &Server, rows: usize) -> TcpStream {
    let body = serde_json::to_vec(&big_multi_get(rows)).unwrap();
    let mut c = TcpStream::connect(s.url.trim_start_matches("http://")).unwrap();
    write!(
        c,
        "POST /v1/Data/MultiGet HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .unwrap();
    c.write_all(&body).unwrap();
    c
}

fn reply_bytes(s: &Server) -> u64 {
    s.shared
        .router
        .service
        .replies
        .bytes
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// A reply holds its share until it's written: a client that doesn't read keeps a large
/// reply charged, so another large read meanwhile gets `Overloaded` (the bound), and a read
/// once it's written gets its share back.
#[test]
fn a_reply_holds_its_share_until_written_and_a_burst_past_the_budget_is_refused() {
    let s = budgeted(
        20 << 20,
        Duration::from_millis(400),
        Duration::from_secs(30),
        8,
    );
    let full = s.shared.router.service.inflight_available();
    let stuck = stuck_multi_get(&s, 8);
    // About 8 MiB as JSON (base64, 4/3) stays charged while unread.
    wait_for("the stuck reply's charge", || reply_bytes(&s) > 8 << 20);
    let held = reply_bytes(&s);
    assert!(held < 20 << 20, "{held}");
    let (status, body) = s.post("MultiGet", &big_multi_get(8), None);
    assert_eq!(
        (status, body["code"].as_str()),
        (503, Some("Overloaded")),
        "{body}"
    );
    // A small read fits beside it.
    let (status, body) = s.post(
        "Get",
        &json!({"table": "t", "row": b64(b"big0"), "family": "f", "qualifier": b64(b"q")}),
        None,
    );
    assert_eq!(status, 200, "{}", body.to_string().len());
    // The stuck client reads its reply: the share comes back.
    let mut stuck = stuck;
    stuck
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut buf = vec![0u8; 1 << 20];
    let mut got = 0usize;
    while got < held as usize {
        match stuck.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) => panic!("reading the reply: {e}"),
        }
    }
    wait_for("the charge back", || reply_bytes(&s) == 0);
    wait_for("the budget whole", || {
        s.shared.router.service.inflight_available() == full
    });
    let (status, _) = s.post("MultiGet", &big_multi_get(8), None);
    assert_eq!(status, 200);
}

/// cli692's D1 on the design: a client that never reads its reply loses it after
/// `--stream-idle-ms`, and the share comes back, so a stuck client can't pin the budget.
#[test]
fn a_reply_its_client_never_reads_is_dropped_after_the_idle_limit() {
    let idle = Duration::from_millis(500);
    let s = budgeted(20 << 20, Duration::from_millis(400), idle, 8);
    let _stuck = stuck_multi_get(&s, 8);
    wait_for("the stuck reply's charge", || reply_bytes(&s) > 8 << 20);
    let since = std::time::Instant::now();
    let deadline = since + Duration::from_secs(10);
    while reply_bytes(&s) > 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the stuck reply kept its share"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(since.elapsed() >= idle / 2, "{:?}", since.elapsed());
    let drops = s
        .shared
        .router
        .service
        .replies
        .idle_drops
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(drops, 1);
    assert!(
        s.shared
            .router
            .service
            .metrics_text()
            .contains("pigeonhole_serve_reply_idle_drops_total 1")
    );
    let (status, _) = s.post("MultiGet", &big_multi_get(8), None);
    assert_eq!(status, 200, "a large read after the drop");
}

/// A small reply under a large `page_bytes` gives the rest of its reservation back once
/// built (gRPC trims to the message, HTTP/JSON to the body), and nothing once written.
#[test]
fn a_small_reply_keeps_only_its_own_size() {
    let s = budgeted(64 << 20, Duration::from_secs(5), Duration::from_secs(30), 0);
    let value = json!({"bytes": b64(b"small")});
    assert_eq!(s.post("Mutate", &put("r", "f", "q", value), None).0, 200);
    let mg = json!({"table": "t", "keys": [b64(b"r")], "page_bytes": 8 << 20});
    let (status, body) = s.post("MultiGet", &mg, None);
    assert_eq!(status, 200, "{body}");
    let mut grpc = s.grpc();
    let got = s
        .block_on(grpc.multi_get(proto::MultiGetRequest {
            table: "t".into(),
            keys: vec![b"r".to_vec().into()],
            page_bytes: 8 << 20,
            ..Default::default()
        }))
        .unwrap();
    assert_eq!(got.into_inner().rows.len(), 1);
    wait_for("every charge back", || reply_bytes(&s) == 0);
}

/// cli692's D1 over gRPC: an HTTP/2 client that doesn't open its window. Its reply is
/// handed to the transport in slices, so the server holds about one beyond the window. After
/// `--stream-idle-ms` the rest and the share are dropped, with the window still shut, so
/// the budget comes back. The stream ends with `DEADLINE_EXCEEDED` once the client opens
/// its window (or with the connection): until then nothing polls its body.
#[test]
fn a_grpc_reply_whose_window_stays_shut_is_dropped_after_the_idle_limit() {
    use prost::Message;
    let idle = Duration::from_millis(500);
    let s = budgeted(20 << 20, Duration::from_millis(400), idle, 8);
    let addr = s.url.trim_start_matches("http://").to_owned();
    let req = proto::MultiGetRequest {
        table: "t".into(),
        keys: (0..8)
            .map(|i| format!("big{i}").into_bytes().into())
            .collect(),
        page_bytes: 8 << 20,
        ..Default::default()
    };
    let mut framed = vec![0u8];
    framed.extend_from_slice(&u32::try_from(req.encoded_len()).unwrap().to_be_bytes());
    framed.extend_from_slice(&req.encode_to_vec());
    let replies = Arc::clone(&s.shared.router.service.replies);
    let bytes = move || replies.bytes.load(std::sync::atomic::Ordering::Relaxed);
    // Bounded: a regression that never ends the stream fails here instead of hanging.
    let (status, grpc_status, drops_while_shut) = s.block_on(async move {
        tokio::time::timeout(Duration::from_secs(30), async move {
            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let (client, conn) = h2::client::handshake(tcp).await.unwrap();
            tokio::spawn(async move {
                let _ = conn.await;
            });
            let mut client = client.ready().await.unwrap();
            let request = axum::http::Request::post("/pigeonhole.v1.Data/MultiGet")
                .header("content-type", "application/grpc")
                .header("te", "trailers")
                .body(())
                .unwrap();
            let (response, mut send) = client.send_request(request, false).unwrap();
            send.send_data(framed.into(), true).unwrap();
            let response = response.await.unwrap();
            let status = response.status();
            let mut body = response.into_body();
            // Take what fits in the window, releasing none of it: the window stays shut.
            let mut taken = 0usize;
            let first = body.data().await.expect("a first frame").unwrap();
            taken += first.len();
            // The share comes back with the window shut.
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while bytes() > 0 {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the stalled reply kept its share"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let drops_while_shut = true;
            // Open the window: the body is polled again, and ends.
            let _ = body.flow_control().release_capacity(taken);
            while let Some(chunk) = body.data().await {
                let n = chunk.map(|c| c.len()).unwrap_or(0);
                let _ = body.flow_control().release_capacity(n);
            }
            let trailers = body.trailers().await.ok().flatten();
            let grpc_status = trailers
                .as_ref()
                .and_then(|t| t.get("grpc-status"))
                .map(|v| v.to_str().unwrap_or("").to_owned());
            (status, grpc_status, drops_while_shut)
        })
        .await
        .expect("the stream ended within 30 s")
    });
    assert_eq!(status, 200);
    assert!(drops_while_shut);
    assert_eq!(grpc_status.as_deref(), Some("4"), "DEADLINE_EXCEEDED");
    assert_eq!(reply_bytes(&s), 0, "the share came back");
    assert_eq!(
        s.shared
            .router
            .service
            .replies
            .idle_drops
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

/// cli692's R1 on #1661: an HTTP/2 client whose initial window is 0, which receives none of
/// its reply. The share still comes back after `--stream-idle-ms`. The watchdog starts when
/// the body is wrapped, so this holds even if the body is never polled. Today's hyper polls
/// it once anyway, so this pins the outcome rather than telling the two starts apart.
#[test]
fn a_reply_to_a_client_with_a_zero_initial_window_is_dropped_after_the_idle_limit() {
    use prost::Message;
    let idle = Duration::from_millis(500);
    let s = budgeted(20 << 20, Duration::from_millis(400), idle, 8);
    let addr = s.url.trim_start_matches("http://").to_owned();
    let req = proto::MultiGetRequest {
        table: "t".into(),
        keys: (0..8)
            .map(|i| format!("big{i}").into_bytes().into())
            .collect(),
        page_bytes: 8 << 20,
        ..Default::default()
    };
    let mut framed = vec![0u8];
    framed.extend_from_slice(&u32::try_from(req.encoded_len()).unwrap().to_be_bytes());
    framed.extend_from_slice(&req.encode_to_vec());
    let replies = Arc::clone(&s.shared.router.service.replies);
    let bytes = move || replies.bytes.load(std::sync::atomic::Ordering::Relaxed);
    s.block_on(async move {
        tokio::time::timeout(Duration::from_secs(30), async move {
            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let (client, conn) = h2::client::Builder::new()
                .initial_window_size(0)
                .handshake::<_, axum::body::Bytes>(tcp)
                .await
                .unwrap();
            tokio::spawn(async move {
                let _ = conn.await;
            });
            let mut client = client.ready().await.unwrap();
            let request = axum::http::Request::post("/pigeonhole.v1.Data/MultiGet")
                .header("content-type", "application/grpc")
                .header("te", "trailers")
                .body(())
                .unwrap();
            let (response, mut send) = client.send_request(request, false).unwrap();
            send.send_data(framed.into(), true).unwrap();
            let response = response.await.unwrap();
            assert_eq!(response.status(), 200);
            // The reply is charged, and its body is never polled: the window is 0.
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while bytes() == 0 {
                assert!(std::time::Instant::now() < deadline, "never charged");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            // The share comes back anyway.
            while bytes() > 0 {
                assert!(
                    std::time::Instant::now() < deadline,
                    "a never-polled reply kept its share"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            drop(response);
        })
        .await
        .expect("within 30 s");
    });
    assert_eq!(
        s.shared
            .router
            .service
            .replies
            .idle_drops
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

/// The g46 revision: a read within the default page builds first and takes its share after.
/// With the budget held by an unread reply, one whose share isn't free goes the strict way
/// (its reply dropped, the cap reserved, the read run again), and is counted as a re-read;
/// at the deadline it's `Overloaded`.
#[test]
fn a_default_page_read_that_finds_the_budget_full_goes_the_strict_way() {
    let s = budgeted(
        10 << 20,
        Duration::from_millis(400),
        Duration::from_secs(30),
        8,
    );
    let t = s
        .shared
        .router
        .service
        .db
        .table("t")
        .unwrap()
        .open()
        .unwrap();
    for i in 0..2 {
        t.mutate(format!("mid{i}").as_bytes())
            .put("f", b"q", &vec![b'y'; 400 << 10])
            .commit()
            .unwrap();
    }
    let _stuck = stuck_multi_get(&s, 8);
    wait_for("the stuck reply's charge", || reply_bytes(&s) > 8 << 20);
    let rereads = || {
        s.shared
            .router
            .service
            .replies
            .rereads
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    assert_eq!(rereads(), 0);
    // Two 400 KiB rows at the default page: their JSON share (twice about 800 KiB) isn't
    // free beside the stuck reply's (about 9.4 MiB of the 10).
    let (status, body) = s.post(
        "MultiGet",
        &json!({"table": "t", "keys": [b64(b"mid0"), b64(b"mid1")]}),
        None,
    );
    assert_eq!(
        (status, body["code"].as_str()),
        (503, Some("Overloaded")),
        "held {}",
        reply_bytes(&s)
    );
    assert_eq!(rereads(), 1, "the strict path after the build");
    // A small reply takes no share at all, however full the budget.
    let value = json!({"bytes": b64(b"tiny")});
    assert_eq!(s.post("Mutate", &put("tiny", "f", "q", value), None).0, 200);
    let (status, body) = s.post(
        "MultiGet",
        &json!({"table": "t", "keys": [b64(b"tiny")]}),
        None,
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(rereads(), 1);
}

/// cli692's B1 on #1661: a strict reservation that's cancelled while it waits (the request
/// dropped: its client gone, or its deadline) leaves nothing behind. A later read within
/// the default page still builds first and takes its share, with no re-read.
#[test]
fn a_cancelled_waiting_reservation_changes_nothing_after() {
    let s = budgeted(10 << 20, Duration::from_secs(5), Duration::from_secs(30), 8);
    let t = s
        .shared
        .router
        .service
        .db
        .table("t")
        .unwrap()
        .open()
        .unwrap();
    for i in 0..2 {
        t.mutate(format!("mid{i}").as_bytes())
            .put("f", b"q", &vec![b'y'; 400 << 10])
            .commit()
            .unwrap();
    }
    let mut stuck = stuck_multi_get(&s, 8);
    wait_for("the stuck reply's charge", || reply_bytes(&s) > 8 << 20);
    let held = reply_bytes(&s);
    // A large-page read waits for its 8 MiB, and is dropped while it waits.
    let service = &s.shared.router.service;
    let waited = s.block_on(async {
        tokio::time::timeout(
            Duration::from_millis(200),
            service.call(
                ServiceOp::MultiGet(proto::MultiGetRequest {
                    table: "t".into(),
                    keys: vec![b"big0".to_vec().into()],
                    page_bytes: 8 << 20,
                    ..Default::default()
                }),
                &Caller::anonymous(),
                None,
            ),
        )
        .await
    });
    assert!(waited.is_err(), "the reservation was waiting when dropped");
    // The stuck client reads its reply: the budget is free again.
    stuck
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut buf = vec![0u8; 1 << 20];
    let mut got = 0usize;
    while got < held as usize {
        match stuck.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) => panic!("reading the reply: {e}"),
        }
    }
    wait_for("the charge back", || reply_bytes(&s) == 0);
    // A default-page read over one slice: built first, its share taken, no re-read.
    let (status, _) = s.post(
        "MultiGet",
        &json!({"table": "t", "keys": [b64(b"mid0"), b64(b"mid1")]}),
        None,
    );
    assert_eq!(status, 200);
    assert_eq!(
        service
            .replies
            .rereads
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "a read after a cancelled reservation went the strict way"
    );
}
