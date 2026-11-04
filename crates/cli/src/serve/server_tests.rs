//! `phdb serve` end to end: a server on a loopback port, called over gRPC and HTTP/JSON.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pigeonhole::{Family, Options, Pigeonhole};
use prost::Message;
use serde_json::{Value as Json, json};

use super::auth::{Caller, Tokens};
use super::config::Limits;
use super::proto::{self, data_client::DataClient};
use super::router::Router;
use super::service::{Metrics, Service, ServiceOp};
use super::{Shared, http, listener};

struct Server {
    url: String,
    shared: Arc<Shared>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    runtime: Option<tokio::runtime::Runtime>,
    dir: PathBuf,
}

fn limits() -> Limits {
    Limits {
        max_request_bytes: 1 << 20,
        inflight_bytes: 16 << 20,
        max_concurrent: 64,
        max_scans: 4,
        request_timeout: Duration::from_secs(5),
        stream_idle: Duration::from_secs(5),
        leases: super::sessions::Leases {
            txn_idle: Duration::from_secs(30),
            txn_lifetime: Duration::from_secs(300),
            snapshot_idle: Duration::from_secs(60),
            snapshot_lifetime: Duration::from_secs(600),
            max_sessions: 64,
            max_per_token: 16,
        },
    }
}

impl Server {
    fn start(tokens: &str, limits: Limits) -> Self {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "phdb-serve-{}-{nanos}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Pigeonhole::open(
            dir.join("t.phdb"),
            Options::default().create_if_missing(true).shards(1),
        )
        .unwrap();
        db.table("t")
            .unwrap()
            .family("f", Family::default())
            .family("n", Family::counter())
            .create()
            .unwrap();
        let metrics = Arc::new(Metrics::default());
        let shared = Arc::new(Shared {
            router: Router::single(Service::new(db, limits, 2, Arc::clone(&metrics))),
            tokens: Tokens::parse(tokens, "test").unwrap(),
            limits,
            metrics,
        });
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = http::router(Arc::clone(&shared));
        runtime.spawn(listener::serve(
            listener,
            None,
            app,
            async {
                let _ = stopped.await;
            },
            Duration::from_secs(2),
        ));
        Self {
            url,
            shared,
            stop: Some(stop),
            runtime: Some(runtime),
            dir,
        }
    }

    fn post(&self, method: &str, body: &Json, token: Option<&str>) -> (u16, Json) {
        self.rpc(&format!("Data/{method}"), body, token)
    }

    /// `POST /v1/{path}`, e.g. `Transactions/Begin`.
    fn rpc(&self, path: &str, body: &Json, token: Option<&str>) -> (u16, Json) {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        let mut req = agent.post(format!("{}/v1/{path}", self.url));
        if let Some(t) = token {
            req = req.header("Authorization", format!("Bearer {t}"));
        }
        let mut resp = req.send_json(body).unwrap();
        let status = resp.status().as_u16();
        let body: Json = resp.body_mut().read_json().unwrap_or(Json::Null);
        (status, body)
    }

    fn block_on<F: Future>(&self, f: F) -> F::Output {
        self.runtime.as_ref().unwrap().block_on(f)
    }

    fn grpc(&self) -> DataClient<tonic::transport::Channel> {
        let url = self.url.clone();
        self.block_on(async move {
            DataClient::new(
                tonic::transport::Channel::from_shared(url)
                    .unwrap()
                    .connect()
                    .await
                    .unwrap(),
            )
        })
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(s) = self.stop.take() {
            let _ = s.send(());
        }
        if let Some(rt) = self.runtime.take() {
            rt.shutdown_timeout(Duration::from_secs(5));
        }
        let _ = self.shared.router.service.db.clone().close();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn b64(s: &[u8]) -> String {
    // The JSON mapping's bytes: standard base64, padded.
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for c in s.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        for i in 0..4 {
            if i <= c.len() {
                out.push(A[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn put(row: &str, family: &str, qualifier: &str, value: Json) -> Json {
    json!({"writes": {"row": {"table": "t", "row": b64(row.as_bytes())},
        "writes": [{"put": {"family": family, "qualifier": b64(qualifier.as_bytes()), "value": value}}]}})
}

#[test]
fn http_json_writes_and_reads_a_cell() {
    let s = Server::start("", limits());
    let (status, body) = s.post(
        "Mutate",
        &put("r1", "f", "q", json!({"bytes": b64(b"hello")})),
        None,
    );
    assert_eq!(status, 200, "{body}");
    assert!(
        body["commit"]["seqno"].is_string(),
        "64-bit integers are strings: {body}"
    );
    let (status, body) = s.post(
        "Get",
        &json!({"table": "t", "row": b64(b"r1"), "family": "f", "qualifier": b64(b"q")}),
        None,
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["cell"]["value"], b64(b"hello"));
    // A cell that isn't there is an empty reply, not an error.
    let (status, body) = s.post(
        "Get",
        &json!({"table": "t", "row": b64(b"nope"), "family": "f", "qualifier": b64(b"q")}),
        None,
    );
    assert_eq!((status, body.get("cell").is_none()), (200, true), "{body}");
}

#[test]
fn errors_carry_their_stable_code_in_both_protocols() {
    let s = Server::start("", limits());
    // HTTP: the status and the ErrorDetail body.
    let (status, body) = s.post(
        "Get",
        &json!({"table": "missing", "row": b64(b"r"), "family": "f", "qualifier": b64(b"q")}),
        None,
    );
    assert_eq!(status, 404, "{body}");
    assert_eq!(body["code"], "TableNotFound");
    assert!(body.get("retry_safe").is_none(), "not retry-safe: {body}");
    let (status, body) = s.post("Nope", &json!({}), None);
    assert_eq!((status, body["code"].as_str()), (404, Some("NotFound")));
    // gRPC: the status code and the ErrorDetail in its details.
    let mut client = s.grpc();
    let err = s
        .block_on(client.get(proto::GetRequest {
            table: "missing".into(),
            row: b"r".to_vec().into(),
            ..Default::default()
        }))
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
    #[derive(Clone, PartialEq, Message)]
    struct Status {
        #[prost(int32, tag = "1")]
        code: i32,
        #[prost(string, tag = "2")]
        message: String,
        #[prost(message, repeated, tag = "3")]
        details: Vec<AnyMsg>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct AnyMsg {
        #[prost(string, tag = "1")]
        type_url: String,
        #[prost(bytes = "vec", tag = "2")]
        value: Vec<u8>,
    }
    let status = Status::decode(err.details()).unwrap();
    let detail = proto::ErrorDetail::decode(status.details[0].value.as_slice()).unwrap();
    assert_eq!(detail.code, "TableNotFound");
}

#[test]
fn grpc_covers_the_data_writes() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    let row = |r: &str, writes: Vec<proto::Write>| proto::RowWrites {
        row: Some(proto::RowKey {
            table: "t".into(),
            row: r.as_bytes().to_vec().into(),
        }),
        writes,
    };
    let put = |q: &str, v: &[u8]| proto::Write {
        op: Some(proto::write::Op::Put(proto::Put {
            family: "f".into(),
            qualifier: q.as_bytes().to_vec().into(),
            timestamp: None,
            value: Some(proto::Value {
                kind: Some(proto::value::Kind::Bytes(v.to_vec().into())),
            }),
        })),
    };
    let incr = |n: i64| proto::Write {
        op: Some(proto::write::Op::Incr(proto::Incr {
            family: "n".into(),
            qualifier: b"c".to_vec().into(),
            timestamp: None,
            delta: n,
        })),
    };
    // CheckAndMutate: absent, so it applies; then present, so it doesn't.
    let absent = proto::Condition {
        kind: Some(proto::condition::Kind::Absent(proto::Column {
            family: "f".into(),
            qualifier: b"lock".to_vec().into(),
        })),
    };
    let cm = |c: &mut DataClient<_>| {
        let req = proto::CheckAndMutateRequest {
            writes: Some(row("job", vec![put("lock", b"me")])),
            condition: Some(absent.clone()),
            durability: 0,
        };
        s.block_on(c.check_and_mutate(req)).unwrap().into_inner()
    };
    assert!(cm(&mut c).applied);
    let second = cm(&mut c);
    assert!(!second.applied && second.commit.is_none());
    // Batch across rows.
    let batch = proto::BatchRequest {
        rows: vec![
            row("a", vec![put("x", b"1")]),
            row("b", vec![put("x", b"2")]),
        ],
        durability: proto::Durability::Sync as i32,
    };
    let committed = s
        .block_on(c.batch(batch))
        .unwrap()
        .into_inner()
        .commit
        .unwrap();
    assert_eq!(committed.durability, proto::Durability::Sync as i32);
    // MutateReturning hands back the counter after the commit.
    for want in [3, 5] {
        let req = proto::MutateRequest {
            writes: Some(row("ctr", vec![incr(if want == 3 { 3 } else { 2 })])),
            durability: 0,
        };
        let r = s.block_on(c.mutate_returning(req)).unwrap().into_inner();
        assert_eq!(
            r.cells[0].value,
            Some(proto::returned_cell::Value::I64(want))
        );
    }
    // MultiGet: any order and duplicates in, key order out.
    let mg = proto::MultiGetRequest {
        table: "t".into(),
        keys: vec![
            b"b".to_vec().into(),
            b"a".to_vec().into(),
            b"b".to_vec().into(),
        ],
        ..Default::default()
    };
    let rows = s.block_on(c.multi_get(mg)).unwrap().into_inner().rows;
    assert_eq!(
        rows.iter().map(|r| r.key.to_vec()).collect::<Vec<_>>(),
        [b"a".to_vec(), b"b".to_vec()]
    );
}

#[test]
fn a_wide_row_reads_in_pages_cut_by_bytes() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    let writes: Vec<proto::Write> = (0..40)
        .map(|i| proto::Write {
            op: Some(proto::write::Op::Put(proto::Put {
                family: "f".into(),
                qualifier: format!("q{i:03}").into_bytes().into(),
                timestamp: None,
                value: Some(proto::Value {
                    kind: Some(proto::value::Kind::Bytes(vec![b'x'; 1000].into())),
                }),
            })),
        })
        .collect();
    let rw = proto::RowWrites {
        row: Some(proto::RowKey {
            table: "t".into(),
            row: b"wide".to_vec().into(),
        }),
        writes,
    };
    s.block_on(c.mutate(proto::MutateRequest {
        writes: Some(rw),
        durability: 0,
    }))
    .unwrap();
    let (mut after, mut seen, mut pages) = (Vec::new(), Vec::new(), 0);
    loop {
        let req = proto::ReadRowRequest {
            table: "t".into(),
            row: b"wide".to_vec().into(),
            page_bytes: 8000,
            after: after.clone().into(),
            ..Default::default()
        };
        let page = s.block_on(c.read_row(req)).unwrap().into_inner();
        pages += 1;
        assert!(
            page.bytes <= 8000 + 8,
            "a page stays within its budget: {}",
            page.bytes
        );
        seen.extend(
            page.row
                .unwrap()
                .cells
                .into_iter()
                .map(|c| c.qualifier.to_vec()),
        );
        if page.next.is_empty() {
            break;
        }
        after = page.next.to_vec();
    }
    assert!(pages >= 5, "40 KB of cells in 8 KB pages: {pages}");
    assert_eq!(
        seen,
        (0..40)
            .map(|i| format!("q{i:03}").into_bytes())
            .collect::<Vec<_>>(),
        "every cell once, in order"
    );
}

#[test]
fn tokens_and_scopes_decide_who_may_do_what() {
    let s = Server::start("read r3ad\nwrite wr1te\n", limits());
    let write = put("r", "f", "q", json!({"bytes": b64(b"v")}));
    let get = json!({"table": "t", "row": b64(b"r"), "family": "f", "qualifier": b64(b"q")});
    assert_eq!(s.post("Get", &get, None).0, 401, "a token is required");
    assert_eq!(s.post("Get", &get, Some("guess")).0, 401);
    let (status, body) = s.post("Mutate", &write, Some("r3ad"));
    assert_eq!(
        (status, body["code"].as_str()),
        (403, Some("PermissionDenied")),
        "{body}"
    );
    assert_eq!(s.post("Mutate", &write, Some("wr1te")).0, 200);
    assert_eq!(s.post("Get", &get, Some("r3ad")).0, 200);
    // Health needs no token; metrics do.
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    assert_eq!(
        agent
            .get(format!("{}/v1/health", s.url))
            .call()
            .unwrap()
            .status()
            .as_u16(),
        200
    );
    assert_eq!(
        agent
            .get(format!("{}/metrics", s.url))
            .call()
            .unwrap()
            .status()
            .as_u16(),
        401
    );
    let mut m = agent
        .get(format!("{}/metrics", s.url))
        .header("Authorization", "Bearer r3ad")
        .call()
        .unwrap();
    let text = m.body_mut().read_to_string().unwrap();
    assert!(
        text.contains("pigeonhole_serve_requests_total{rpc=\"Data.Mutate\",outcome=\"ok\"} 1"),
        "{text}"
    );
    assert!(text.contains("pigeonhole_async_sync_reads_total"), "{text}");
}

#[test]
fn a_request_over_the_size_limit_is_refused() {
    let mut l = limits();
    l.max_request_bytes = 1024;
    let s = Server::start("", l);
    let (status, body) = s.post(
        "Mutate",
        &put("r", "f", "q", json!({"bytes": b64(&[7u8; 4096])})),
        None,
    );
    assert_eq!(
        (status, body["code"].as_str()),
        (413, Some("RequestTooLarge")),
        "{body}"
    );
}

#[test]
fn a_write_whose_client_went_away_still_commits() {
    // simp724's B2 on D313: the commit is its own task; dropping the request loses only the
    // reply, which is logged and counted.
    let s = Server::start("", limits());
    let service = &s.shared.router.service;
    let get = || {
        ServiceOp::Get(proto::GetRequest {
            table: "t".into(),
            row: b"gone".to_vec().into(),
            family: "f".into(),
            qualifier: b"q".to_vec().into(),
            ..Default::default()
        })
    };
    // The table is opened once first, so the write reaches its spawn on the first poll.
    s.block_on(service.call(get(), &Caller::anonymous(), None))
        .unwrap();
    let write = ServiceOp::Mutate(proto::MutateRequest {
        writes: Some(proto::RowWrites {
            row: Some(proto::RowKey {
                table: "t".into(),
                row: b"gone".to_vec().into(),
            }),
            writes: vec![proto::Write {
                op: Some(proto::write::Op::Put(proto::Put {
                    family: "f".into(),
                    qualifier: b"q".to_vec().into(),
                    timestamp: None,
                    value: Some(proto::Value {
                        kind: Some(proto::value::Kind::Bytes(b"landed".to_vec().into())),
                    }),
                })),
            }],
        }),
        durability: 0,
    });
    s.block_on(async {
        let caller = Caller::anonymous();
        let call = service.call(write, &caller, None);
        tokio::select! {
            biased;
            _ = call => panic!("the write answered before its first await"),
            () = std::future::ready(()) => {}
        }
    });
    // The request is gone; the write isn't.
    let landed = (0..100).any(|_| {
        std::thread::sleep(Duration::from_millis(20));
        let r = s.block_on(service.call(get(), &Caller::anonymous(), None)).unwrap();
        matches!(r, super::service::ServiceReply::Get(g) if g.cell.as_ref().is_some_and(|c| &c.value[..] == b"landed"))
    });
    assert!(landed, "the detached commit landed");
    // The task counts the lost reply once its commit returns, which can be just after the
    // write became visible.
    let counted = (0..100).any(|_| {
        std::thread::sleep(Duration::from_millis(10));
        s.shared.metrics.replies_lost.load(Ordering::Relaxed) == 1
    });
    assert!(counted, "the lost reply is counted");
    // simp724's S1 on #1501: the write held its bytes until it finished, then gave them
    // back; none leaked when its request went away.
    assert_eq!(service.inflight_available(), limits().inflight_bytes);
}

#[test]
fn phdb_serve_refuses_a_reachable_address_without_tls_and_tokens() {
    // The bind rule runs before the database opens: the file needn't exist.
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let status = crate::run(
        [
            "phdb",
            "serve",
            "/nonexistent/never-opened.phdb",
            "--listen",
            "0.0.0.0:0",
        ],
        &mut out,
        &mut err,
    );
    let err = String::from_utf8_lossy(&err);
    assert_eq!(status, crate::ExitStatus::USAGE, "{err}");
    assert!(
        err.contains("refusing to serve") && err.contains("--insecure-no-tls-no-auth"),
        "{err}"
    );
}

#[test]
fn tls_files_that_dont_load_are_a_clear_error() {
    let e = super::tls::acceptor(
        std::path::Path::new("/nonexistent/cert.pem"),
        std::path::Path::new("/nonexistent/key.pem"),
    )
    .err()
    .unwrap();
    // The message isn't printed on failure: CodeQL reads a string from loading a
    // certificate as sensitive (rust/cleartext-logging), though it holds only the path.
    assert!(
        e.contains("/nonexistent/cert.pem"),
        "the error names the file"
    );
}

/// Opens a raw connection and sends `head` (an HTTP/1.1 request's lines and headers).
fn raw(url: &str, head: &str) -> std::net::TcpStream {
    use std::io::Write as _;
    let addr = url.trim_start_matches("http://");
    let mut tcp = std::net::TcpStream::connect(addr).unwrap();
    tcp.write_all(head.as_bytes()).unwrap();
    tcp
}

#[test]
fn health_and_metrics_answer_while_requests_are_shed() {
    // perf287's F4 on #1501: one slot, held by a request whose body never finishes.
    let mut l = limits();
    l.max_concurrent = 1;
    let s = Server::start("", l);
    let _held = raw(
        &s.url,
        "POST /v1/Data/Get HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{",
    );
    std::thread::sleep(Duration::from_millis(200));
    let (status, body) = s.post(
        "Get",
        &json!({"table": "t", "row": b64(b"r"), "family": "f", "qualifier": b64(b"q")}),
        None,
    );
    assert_eq!(
        (status, body["code"].as_str()),
        (503, Some("Overloaded")),
        "{body}"
    );
    assert_eq!(body["retry_safe"], true);
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    assert_eq!(
        agent
            .get(format!("{}/v1/health", s.url))
            .call()
            .unwrap()
            .status()
            .as_u16(),
        200
    );
    let mut m = agent.get(format!("{}/metrics", s.url)).call().unwrap();
    assert_eq!(m.status().as_u16(), 200);
    assert!(
        m.body_mut()
            .read_to_string()
            .unwrap()
            .contains("pigeonhole_serve_shed_total 1")
    );
}

#[test]
fn a_body_without_a_length_is_refused_before_it_is_read() {
    // perf287's F2 on #1501: the budget is reserved from Content-Length, so a chunked body
    // (no length) is refused up front.
    use std::io::Read as _;
    let s = Server::start("", limits());
    let mut tcp = raw(
        &s.url,
        "POST /v1/Data/Get HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n2\r\n{}\r\n0\r\n\r\n",
    );
    let mut reply = String::new();
    let _ = tcp.read_to_string(&mut reply);
    assert!(reply.starts_with("HTTP/1.1 400"), "{reply}");
    assert!(reply.contains("Content-Length"), "{reply}");
}

/// Rows `r00`..`r29` with three 400-byte cells each, and `x1` outside the `r` prefix.
fn seed_rows(s: &Server, c: &mut DataClient<tonic::transport::Channel>) {
    let mut rows: Vec<String> = (0..30).map(|i| format!("r{i:02}")).collect();
    rows.push("x1".into());
    for row in rows {
        let writes = (0..3)
            .map(|q| proto::Write {
                op: Some(proto::write::Op::Put(proto::Put {
                    family: "f".into(),
                    qualifier: format!("q{q}").into_bytes().into(),
                    timestamp: None,
                    value: Some(proto::Value {
                        kind: Some(proto::value::Kind::Bytes(vec![b'v'; 400].into())),
                    }),
                })),
            })
            .collect();
        s.block_on(c.mutate(proto::MutateRequest {
            writes: Some(proto::RowWrites {
                row: Some(proto::RowKey {
                    table: "t".into(),
                    row: row.into_bytes().into(),
                }),
                writes,
            }),
            durability: 0,
        }))
        .unwrap();
    }
}

fn scan_request(page_bytes: u64, limit: u64, after: Vec<u8>) -> proto::ScanRequest {
    proto::ScanRequest {
        table: "t".into(),
        range: Some(proto::KeyRange {
            range: Some(proto::key_range::Range::Prefix(b"r".to_vec().into())),
        }),
        limit,
        page_bytes,
        after: after.into(),
        ..Default::default()
    }
}

/// Pages' rows joined: a row a page cut continues at the start of the next page.
fn join(pages: &[proto::ScanPage]) -> Vec<(Vec<u8>, Vec<Vec<u8>>)> {
    let mut out: Vec<(Vec<u8>, Vec<Vec<u8>>)> = Vec::new();
    for page in pages {
        for row in &page.rows {
            let cells = row.cells.iter().map(|c| c.qualifier.to_vec());
            match out.last_mut() {
                Some((key, have)) if key[..] == row.key[..] => have.extend(cells),
                _ => out.push((row.key.to_vec(), cells.collect())),
            }
        }
    }
    out
}

fn grpc_scan(
    s: &Server,
    c: &mut DataClient<tonic::transport::Channel>,
    req: proto::ScanRequest,
) -> Vec<proto::ScanPage> {
    s.block_on(async {
        let mut stream = c.scan(req).await.unwrap().into_inner();
        let mut pages = Vec::new();
        while let Some(page) = stream.message().await.unwrap() {
            pages.push(page);
        }
        pages
    })
}

fn whole_rows(n: usize) -> Vec<(Vec<u8>, Vec<Vec<u8>>)> {
    (0..n)
        .map(|i| {
            (
                format!("r{i:02}").into_bytes(),
                (0..3).map(|q| format!("q{q}").into_bytes()).collect(),
            )
        })
        .collect()
}

#[test]
fn a_scan_streams_every_row_once_in_byte_budgeted_pages() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    let pages = grpc_scan(&s, &mut c, scan_request(1000, 0, Vec::new()));
    assert!(pages.len() >= 10, "36 KB in 1 KB pages: {}", pages.len());
    let (last, rest) = pages.split_last().unwrap();
    assert!(last.next.is_empty(), "the last page has no position");
    assert!(rest.iter().all(|p| !p.next.is_empty()), "the others do");
    assert!(
        rest.iter().all(|p| p.bytes <= 1000 + 450),
        "only one column may go over a page's budget"
    );
    assert_eq!(
        join(&pages),
        whole_rows(30),
        "the prefix's rows, whole, in order"
    );
}

#[test]
fn a_scans_limit_counts_rows_across_pages_and_resumes() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    // Pages cut inside rows: a continued row isn't counted twice.
    let pages = grpc_scan(&s, &mut c, scan_request(1000, 7, Vec::new()));
    assert_eq!(join(&pages), whole_rows(7));
    assert!(pages.last().unwrap().next.is_empty());
    // A client resumes from a page's position with the rows it still wants: a row the
    // position is inside finishes without counting, as the client has already seen it.
    let first = grpc_scan(&s, &mut c, scan_request(1000, 0, Vec::new()));
    let resume = first[0].next.to_vec();
    let mut resumed = vec![first[0].clone()];
    resumed[0].next = Default::default();
    let seen = join(&resumed).len();
    assert!(
        join(&resumed).last().unwrap().1.len() < 3,
        "the first page ends inside a row, so the resume continues it"
    );
    resumed.extend(grpc_scan(&s, &mut c, scan_request(1000, 3, resume)));
    assert_eq!(join(&resumed), whole_rows(seen + 3));
}

#[test]
fn a_limit_holds_when_the_row_a_position_is_inside_is_deleted() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    // A position inside a row, as a 1 KB page leaves it.
    let first = grpc_scan(&s, &mut c, scan_request(1000, 0, Vec::new()));
    let mut taken = vec![first[0].clone()];
    taken[0].next = Default::default();
    let (inside, cells) = join(&taken).last().unwrap().clone();
    assert!(cells.len() < 3, "the first page ends inside a row");
    // That row is deleted before the next page (no snapshot_id): the resumed page asks for
    // one row more than remain, and every row it brings is new (simp724's B1 on #1510).
    s.block_on(c.mutate(proto::MutateRequest {
        writes: Some(proto::RowWrites {
            row: Some(proto::RowKey {
                table: "t".into(),
                row: inside.clone().into(),
            }),
            writes: vec![proto::Write {
                op: Some(proto::write::Op::DeleteRow(proto::DeleteRow {})),
            }],
        }),
        durability: 0,
    }))
    .unwrap();
    let pages = grpc_scan(&s, &mut c, scan_request(100_000, 3, first[0].next.to_vec()));
    let at = whole_rows(30)
        .iter()
        .position(|(k, _)| *k == inside)
        .unwrap();
    assert_eq!(
        join(&pages),
        whole_rows(at + 4)[at + 1..],
        "exactly the limit's rows, after the deleted one"
    );
    assert!(
        pages.last().unwrap().next.is_empty(),
        "and the scan finished"
    );
}

/// A scan straight through the service: its pages are taken only when the test polls.
fn direct_scan(s: &Server, req: proto::ScanRequest) -> super::service::ScanPages {
    let service = &s.shared.router.service;
    match s
        .block_on(service.call(ServiceOp::Scan(req), &Caller::anonymous(), None))
        .unwrap()
    {
        super::service::ServiceReply::Scan(pages) => pages,
        _ => unreachable!(),
    }
}

/// The scan's next item, or `None` if none comes within `wait`.
fn next_within(
    s: &Server,
    pages: &mut super::service::ScanPages,
    wait: Duration,
) -> Option<Option<Result<proto::ScanPage, super::errors::ApiError>>> {
    use futures_core::Stream as _;
    s.block_on(async {
        tokio::time::timeout(
            wait,
            std::future::poll_fn(|cx| std::pin::Pin::new(&mut *pages).poll_next(cx)),
        )
        .await
    })
    .ok()
}

fn wait_for(what: &str, mut ok: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !ok() {
        assert!(std::time::Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn scan_pages_hold_the_in_flight_budget_until_taken() {
    // Room for one page, not two: a full page of a 1 KB budget is over 1 KB (perf287's G1
    // on #1510).
    let mut l = limits();
    l.inflight_bytes = 1500;
    let s = Server::start("", l);
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    let service = &s.shared.router.service;
    let mut a = direct_scan(&s, scan_request(1000, 0, Vec::new()));
    wait_for("scan a's first page takes its budget", || {
        service.inflight_available() < 1500
    });
    // Scan b's first page waits for the budget a's unread page holds.
    let mut b = direct_scan(&s, scan_request(1000, 0, Vec::new()));
    assert!(
        next_within(&s, &mut b, Duration::from_millis(300)).is_none(),
        "b waits while a's page holds the budget"
    );
    // Once a's page is taken, its budget goes to b, which waited first.
    assert!(matches!(
        next_within(&s, &mut a, Duration::from_secs(5)),
        Some(Some(Ok(_)))
    ));
    assert!(matches!(
        next_within(&s, &mut b, Duration::from_secs(5)),
        Some(Some(Ok(_)))
    ));
    drop((a, b));
    wait_for("both scans' budget comes back", || {
        service.inflight_available() == 1500
    });
}

#[test]
fn a_client_that_stops_reading_loses_its_scan_after_the_idle_timeout() {
    let mut l = limits();
    l.max_scans = 1;
    l.stream_idle = Duration::from_millis(200);
    let s = Server::start("", l);
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    let service = &s.shared.router.service;
    let mut stalled = direct_scan(&s, scan_request(1000, 0, Vec::new()));
    let Some(Some(Ok(first))) = next_within(&s, &mut stalled, Duration::from_secs(5)) else {
        panic!("the first page");
    };
    assert!(!first.next.is_empty());
    // The client stops reading: its queued page and budget are taken back, its slot freed,
    // and the stream ends with DeadlineExceeded (perf287's G2 on #1510).
    std::thread::sleep(Duration::from_millis(600));
    wait_for("the stalled scan's budget comes back", || {
        service.inflight_available() == limits().inflight_bytes
    });
    let Some(Some(Err(e))) = next_within(&s, &mut stalled, Duration::from_secs(5)) else {
        panic!("the stream ends with an error");
    };
    assert_eq!(e.code, "DeadlineExceeded", "{e:?}");
    assert!(e.message.contains("--stream-idle-ms"), "{e:?}");
    assert!(matches!(
        next_within(&s, &mut stalled, Duration::from_secs(1)),
        Some(None)
    ));
    // The client resumes from the last page it took, in another scan: the slot is free.
    let mut pages = vec![first.clone()];
    pages.extend(grpc_scan(
        &s,
        &mut c,
        scan_request(1000, 0, first.next.to_vec()),
    ));
    assert_eq!(join(&pages), whole_rows(30));
    let text = s.shared.metrics.render(&service.db);
    assert!(
        text.contains(
            r#"pigeonhole_serve_requests_total{rpc="Data.Scan",outcome="DeadlineExceeded"} 1"#
        ),
        "{text}"
    );
}

#[test]
fn a_scan_over_http_is_ndjson_or_server_sent_events() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    let body = json!({"table": "t", "range": {"prefix": b64(b"r")}, "page_bytes": "1000"});
    let decode = |page: &Json| -> proto::ScanPage { serde_json::from_value(page.clone()).unwrap() };

    let mut resp = agent
        .post(format!("{}/v1/Data/Scan", s.url))
        .send_json(&body)
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/x-ndjson"
    );
    let text = resp.body_mut().read_to_string().unwrap();
    let pages: Vec<proto::ScanPage> = text
        .lines()
        .map(|l| decode(&serde_json::from_str(l).unwrap()))
        .collect();
    assert!(pages.len() >= 10);
    assert_eq!(join(&pages), whole_rows(30));

    let mut resp = agent
        .post(format!("{}/v1/Data/Scan", s.url))
        .header("Accept", "text/event-stream")
        .send_json(&body)
        .unwrap();
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "text/event-stream"
    );
    let text = resp.body_mut().read_to_string().unwrap();
    let events: Vec<&str> = text.split("\n\n").filter(|e| !e.is_empty()).collect();
    assert!(events.iter().all(|e| e.starts_with("data: ")), "{text}");
    let pages: Vec<proto::ScanPage> = events
        .iter()
        .map(|e| decode(&serde_json::from_str(&e["data: ".len()..]).unwrap()))
        .collect();
    assert_eq!(join(&pages), whole_rows(30));

    // A bad request fails the call before any stream starts.
    let (status, err) = s.post(
        "Scan",
        &json!({"table": "t", "after": b64(b"not a position")}),
        None,
    );
    assert_eq!(
        (status, err["code"].as_str()),
        (400, Some("InvalidArgument")),
        "{err}"
    );
    let (status, err) = s.post("Scan", &json!({"table": "nope"}), None);
    assert_eq!(err["code"], "TableNotFound", "{status} {err}");
}

#[test]
fn scans_past_the_scan_limit_are_refused_until_one_ends() {
    let mut l = limits();
    l.max_scans = 1;
    let s = Server::start("", l);
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    let mut held = s.block_on(async {
        let mut stream = c
            .scan(scan_request(1000, 0, Vec::new()))
            .await
            .unwrap()
            .into_inner();
        stream.message().await.unwrap().unwrap();
        stream
    });
    let refused = s
        .block_on(c.scan(scan_request(1000, 0, Vec::new())))
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unavailable, "{refused:?}");
    assert!(refused.message().contains("scan limit"), "{refused:?}");
    // The held scan's stream stays readable to its end; once it ends, its slot is free.
    let rest = s.block_on(async {
        let mut n = 0;
        while held.message().await.unwrap().is_some() {
            n += 1;
        }
        n
    });
    assert!(rest > 0);
    drop(held);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match s.block_on(c.scan(scan_request(100_000, 0, Vec::new()))) {
            Ok(r) => {
                let mut stream = r.into_inner();
                s.block_on(async { while stream.message().await.unwrap().is_some() {} });
                break;
            }
            Err(e) if std::time::Instant::now() < deadline => {
                assert_eq!(e.code(), tonic::Code::Unavailable);
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => panic!("the slot never came back: {e:?}"),
        }
    }
}

#[test]
fn grpc_reserves_the_budget_before_decoding_and_honors_grpc_timeout() {
    use tower::ServiceExt as _;
    let mut l = limits();
    l.request_timeout = Duration::from_millis(500);
    let s = Server::start("", l);
    let service = &s.shared.router.service;
    // A raw gRPC call through the router: tonic's client enforces `grpc-timeout` itself, so
    // it would race the server's own answer.
    let call = |timeout: Option<&str>| {
        let msg = proto::GetRequest {
            table: "t".into(),
            row: b"r".to_vec().into(),
            family: "f".into(),
            qualifier: b"q".to_vec().into(),
            ..Default::default()
        }
        .encode_to_vec();
        let mut framed = vec![0];
        framed.extend_from_slice(&(msg.len() as u32).to_be_bytes());
        framed.extend_from_slice(&msg);
        let mut req = axum::http::Request::post("/pigeonhole.v1.Data/Get")
            .header("content-type", "application/grpc")
            .header("te", "trailers");
        if let Some(t) = timeout {
            req = req.header("grpc-timeout", t);
        }
        let req = req.body(axum::body::Body::from(framed)).unwrap();
        let app = http::router(Arc::clone(&s.shared));
        let resp = s.block_on(app.oneshot(req)).unwrap();
        // An error before the handler is a trailers-only reply: the status is in the headers.
        resp.headers()
            .get("grpc-status")
            .map(|v| v.to_str().unwrap().to_owned())
    };
    let all = s.block_on(service.reserve(l.inflight_bytes)).unwrap();
    // The client's deadline is shorter than the server's: DeadlineExceeded (4).
    assert_eq!(call(Some("100m")).as_deref(), Some("4"));
    // No client deadline: the server's request timeout, then Overloaded (UNAVAILABLE, 14).
    assert_eq!(call(None).as_deref(), Some("14"));
    drop(all);
    let mut c = s.grpc();
    let get = proto::GetRequest {
        table: "t".into(),
        row: b"r".to_vec().into(),
        family: "f".into(),
        qualifier: b"q".to_vec().into(),
        ..Default::default()
    };
    s.block_on(c.get(get)).unwrap();
    // Every reservation came back.
    assert_eq!(service.inflight_available(), l.inflight_bytes);
}

#[test]
fn metrics_include_each_rpcs_durations() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    grpc_scan(&s, &mut c, scan_request(0, 0, Vec::new()));
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    // A scan is counted when its task ends, just after its last page.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let text = loop {
        let text = agent
            .get(format!("{}/metrics", s.url))
            .call()
            .unwrap()
            .body_mut()
            .read_to_string()
            .unwrap();
        if text.contains("rpc=\"Data.Scan\",outcome=\"ok\"") || std::time::Instant::now() > deadline
        {
            break text;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(
        text.contains("pigeonhole_serve_requests_total{rpc=\"Data.Mutate\",outcome=\"ok\"} 31"),
        "{text}"
    );
    assert!(
        text.contains(
            "pigeonhole_serve_request_duration_seconds_bucket{rpc=\"Data.Mutate\",le=\"+Inf\"} 31"
        ),
        "{text}"
    );
    assert!(
        text.contains("pigeonhole_serve_request_duration_seconds_count{rpc=\"Data.Scan\"} 1"),
        "{text}"
    );
    assert!(
        !text.contains("duration_seconds_count{rpc=\"Data.Get\"}"),
        "an RPC never called has no histogram: {text}"
    );
}

// ---- Transactions and Snapshots (D313 ticket 4) ----

/// A transaction's writes to `row`: `f:q = value`.
fn txn_rows(row: &str, value: &[u8]) -> Json {
    json!([{"row": {"table": "t", "row": b64(row.as_bytes())},
        "writes": [{"put": {"family": "f", "qualifier": b64(b"q"), "value": {"bytes": b64(value)}}}]}])
}

fn begin(s: &Server, token: Option<&str>) -> String {
    let (status, body) = s.rpc("Transactions/Begin", &json!({}), token);
    assert_eq!(status, 200, "{body}");
    body["txn_id"].as_str().unwrap().to_owned()
}

/// The cell's value at `row`'s `f:q`, base64 as the JSON mapping gives it.
fn cell(s: &Server, row: &str, snapshot: Option<&str>) -> Option<String> {
    let mut get =
        json!({"table": "t", "row": b64(row.as_bytes()), "family": "f", "qualifier": b64(b"q")});
    if let Some(id) = snapshot {
        get["snapshot_id"] = json!(id);
    }
    let (status, body) = s.post("Get", &get, None);
    assert_eq!(status, 200, "{body}");
    body["cell"]["value"].as_str().map(str::to_owned)
}

#[test]
fn a_commit_sent_again_answers_the_first_ones_outcome() {
    let s = Server::start("", limits());
    let id = begin(&s, None);
    let (status, body) = s.rpc(
        "Transactions/Write",
        &json!({"txn_id": id, "rows": txn_rows("a", b"1")}),
        None,
    );
    assert_eq!(status, 200, "{body}");
    let commit = json!({"txn_id": id, "rows": txn_rows("b", b"2")});
    let (status, first) = s.rpc("Transactions/Commit", &commit, None);
    assert_eq!(status, 200, "{first}");
    // The reply is lost; the client sends Commit again, rows and all: the tombstone answers
    // the first commit's outcome, and nothing commits twice (simp724's B1 on D313).
    let (status, again) = s.rpc("Transactions/Commit", &commit, None);
    assert_eq!((status, &again), (200, &first));
    assert_eq!(cell(&s, "a", None), Some(b64(b"1")));
    assert_eq!(cell(&s, "b", None), Some(b64(b"2")));
    // Any other call names the outcome without claiming one of its own.
    for (rpc, body) in [
        ("Transactions/Abort", json!({"txn_id": id})),
        (
            "Transactions/Get",
            json!({"txn_id": id, "table": "t", "row": b64(b"a"), "family": "f", "qualifier": b64(b"q")}),
        ),
    ] {
        let (status, e) = s.rpc(rpc, &body, None);
        assert_eq!(
            (status, e["code"].as_str()),
            (409, Some("TransactionFinished")),
            "{e}"
        );
        assert!(
            e["message"]
                .as_str()
                .unwrap()
                .contains("committed at seqno"),
            "{e}"
        );
    }

    // A conflict is recorded the same way.
    let id = begin(&s, None);
    let get = json!({"txn_id": id, "table": "t", "row": b64(b"a"), "family": "f", "qualifier": b64(b"q")});
    let (status, read) = s.rpc("Transactions/Get", &get, None);
    assert_eq!(status, 200, "{read}");
    assert_eq!(
        s.post(
            "Mutate",
            &put("a", "f", "q", json!({"bytes": b64(b"other")})),
            None
        )
        .0,
        200
    );
    let commit = json!({"txn_id": id, "rows": txn_rows("a", b"mine")});
    for _ in 0..2 {
        let (status, e) = s.rpc("Transactions/Commit", &commit, None);
        assert_eq!((status, e["code"].as_str()), (409, Some("Conflict")), "{e}");
    }
    assert_eq!(cell(&s, "a", None), Some(b64(b"other")));
}

#[test]
fn a_commit_whose_client_went_away_still_lands_and_is_recorded() {
    let s = Server::start("", limits());
    let service = &s.shared.router.service;
    let caller = Caller::anonymous();
    // The table is opened once first, so the commit reaches its spawn on the first poll.
    assert!(cell(&s, "gone", None).is_none());
    let id = match s
        .block_on(service.call(ServiceOp::Begin(Default::default()), &caller, None))
        .unwrap()
    {
        super::service::ServiceReply::Begin(b) => b.txn_id,
        _ => unreachable!(),
    };
    let rows: Vec<proto::RowWrites> = serde_json::from_value(txn_rows("gone", b"landed")).unwrap();
    let commit = ServiceOp::Commit(proto::TxnCommitRequest {
        txn_id: id.clone(),
        rows,
    });
    s.block_on(async {
        let call = service.call(commit, &caller, None);
        tokio::select! {
            biased;
            _ = call => panic!("the commit answered before its first await"),
            () = std::future::ready(()) => {}
        }
    });
    // The request is gone; the commit isn't, and the session holds its outcome: a Commit
    // sent again gets SessionBusy while it runs, then the tombstone.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let info = loop {
        let again = ServiceOp::Commit(proto::TxnCommitRequest {
            txn_id: id.clone(),
            rows: Vec::new(),
        });
        match s.block_on(service.call(again, &caller, None)) {
            Ok(super::service::ServiceReply::Commit(r)) => break r.commit.unwrap(),
            Err(e) if e.code == "SessionBusy" && std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            other => panic!("{other:?}"),
        }
    };
    assert!(info.seqno > 0);
    assert_eq!(cell(&s, "gone", None), Some(b64(b"landed")));
    wait_for("the lost reply is counted", || {
        s.shared.metrics.replies_lost.load(Ordering::Relaxed) == 1
    });
}

#[test]
fn transaction_expired_comes_only_before_a_commit() {
    let mut l = limits();
    l.leases.txn_idle = Duration::from_millis(300);
    let s = Server::start("", l);
    let idle = begin(&s, None);
    let committed = begin(&s, None);
    let (status, body) = s.rpc(
        "Transactions/Commit",
        &json!({"txn_id": committed, "rows": txn_rows("c", b"1")}),
        None,
    );
    assert_eq!(status, 200, "{body}");
    std::thread::sleep(Duration::from_millis(450));
    // The idle one expired before any Commit: safe to start again from Begin.
    let (status, e) = s.rpc("Transactions/Commit", &json!({"txn_id": idle}), None);
    assert_eq!(
        (status, e["code"].as_str()),
        (409, Some("TransactionExpired")),
        "{e}"
    );
    assert_eq!(e["retry_from"], "RETRY_FROM_BEGIN", "{e}");
    // The committed one's tombstone expired too: NotFound, which for a client that sent
    // Commit means "outcome unknown", never TransactionExpired ("not committed").
    let (status, e) = s.rpc("Transactions/Commit", &json!({"txn_id": committed}), None);
    assert_eq!((status, e["code"].as_str()), (404, Some("NotFound")), "{e}");
    assert_eq!(cell(&s, "c", None), Some(b64(b"1")));
}

#[test]
fn sessions_belong_to_their_token_and_read_tokens_only_read() {
    let s = Server::start("read r3ad\nwrite wr1te\n", limits());
    let mine = begin(&s, Some("wr1te"));
    // Another token's use of the id is NotFound, never PermissionDenied: ids can't be probed.
    let (status, e) = s.rpc("Transactions/Abort", &json!({"txn_id": mine}), Some("r3ad"));
    assert_eq!((status, e["code"].as_str()), (404, Some("NotFound")), "{e}");
    // A read token may begin, read, and commit a transaction with no writes...
    let reader = begin(&s, Some("r3ad"));
    let (status, body) = s.rpc(
        "Transactions/Commit",
        &json!({"txn_id": reader}),
        Some("r3ad"),
    );
    assert_eq!(status, 200, "{body}");
    // ...but not write in one.
    let reader = begin(&s, Some("r3ad"));
    for (rpc, body) in [
        (
            "Transactions/Write",
            json!({"txn_id": reader, "rows": txn_rows("x", b"1")}),
        ),
        (
            "Transactions/Commit",
            json!({"txn_id": reader, "rows": txn_rows("x", b"1")}),
        ),
    ] {
        let (status, e) = s.rpc(rpc, &body, Some("r3ad"));
        assert_eq!(
            (status, e["code"].as_str()),
            (403, Some("PermissionDenied")),
            "{rpc}: {e}"
        );
    }
    let (status, body) = s.rpc(
        "Transactions/Abort",
        &json!({"txn_id": reader}),
        Some("r3ad"),
    );
    assert_eq!(status, 200, "{body}");
    let (status, body) = s.rpc(
        "Transactions/Abort",
        &json!({"txn_id": mine}),
        Some("wr1te"),
    );
    assert_eq!(status, 200, "{body}");
    // Aborted: the id is gone.
    let (status, _) = s.rpc(
        "Transactions/Abort",
        &json!({"txn_id": mine}),
        Some("wr1te"),
    );
    assert_eq!(status, 404);
}

#[test]
fn reads_at_a_pinned_snapshot_agree_until_it_is_released() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    let (status, opened) = s.rpc("Snapshots/Open", &json!({}), None);
    assert_eq!(status, 200, "{opened}");
    let id = opened["snapshot_id"].as_str().unwrap().to_owned();
    assert!(opened["seqno"].as_str().unwrap().parse::<u64>().unwrap() > 0);
    // Changes after the snapshot: a row deleted, one added.
    assert_eq!(
        s.post(
            "Mutate",
            &put("r00", "f", "q", json!({"bytes": b64(b"new")})),
            None
        )
        .0,
        200
    );
    assert_eq!(
        s.post(
            "Mutate",
            &put("r99", "f", "q0", json!({"bytes": b64(b"new")})),
            None
        )
        .0,
        200
    );
    // A Get and a Scan at the snapshot see it as it was, over every page.
    let mut get = json!({"table": "t", "row": b64(b"r00"), "family": "f", "qualifier": b64(b"q0"), "snapshot_id": id});
    let (_, body) = s.post("Get", &get, None);
    assert_eq!(body["cell"]["value"], b64(&[b'v'; 400]), "{body}");
    let mut req = scan_request(1000, 0, Vec::new());
    req.selection = Some(proto::Selection {
        snapshot_id: id.clone(),
        ..Default::default()
    });
    let pages = grpc_scan(&s, &mut c, req.clone());
    assert_eq!(join(&pages), whole_rows(30), "r99 isn't in the snapshot");
    // A Begin's snapshot seqno can be compared with the pinned one's.
    let (_, b) = s.rpc("Transactions/Begin", &json!({}), None);
    assert!(
        b["snapshot_seqno"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > opened["seqno"].as_str().unwrap().parse::<u64>().unwrap()
    );
    // Released: later reads at it are NotFound, including a stream's next page.
    let (status, _) = s.rpc("Snapshots/Release", &json!({"snapshot_id": id}), None);
    assert_eq!(status, 200);
    get["qualifier"] = json!(b64(b"q1"));
    let (status, e) = s.post("Get", &get, None);
    assert_eq!((status, e["code"].as_str()), (404, Some("NotFound")), "{e}");
    let err = s.block_on(c.scan(req)).unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound, "{err:?}");
    let text = s.shared.metrics.render(&s.shared.router.service.db);
    let mut text = text;
    super::service::render_sessions(&mut text, &s.shared.router.service.sessions);
    assert!(
        text.contains(r#"pigeonhole_serve_sessions{kind="transaction"} 1"#),
        "{text}"
    );
    assert!(
        text.contains(r#"pigeonhole_serve_sessions{kind="snapshot"} 0"#),
        "{text}"
    );
}

#[test]
fn a_write_list_that_fails_partway_buffers_none_of_it() {
    // simp724's T1 on #1511: two good writes, then a vector whose data doesn't fit its
    // dimension. The client gets InvalidArgument ("not applied"), so nothing may be buffered.
    let s = Server::start("", limits());
    let mut rows = txn_rows("p1", b"1");
    rows.as_array_mut()
        .unwrap()
        .extend(txn_rows("p2", b"2").as_array().unwrap().iter().cloned());
    rows.as_array_mut().unwrap().push(json!({
        "row": {"table": "t", "row": b64(b"p3")},
        "writes": [{"put": {"family": "f", "qualifier": b64(b"q"),
            "value": {"vector": {"element": "ELEMENT_F32", "dim": 2, "data": b64(&[0; 3])}}}}]
    }));
    for rpc in ["Transactions/Write", "Transactions/Commit"] {
        let id = begin(&s, None);
        let (status, e) = s.rpc(rpc, &json!({"txn_id": id, "rows": rows}), None);
        assert_eq!(
            (status, e["code"].as_str()),
            (400, Some("InvalidArgument")),
            "{rpc}: {e}"
        );
        // The session is still open, and a Commit with no rows commits nothing of it.
        let (status, body) = s.rpc("Transactions/Commit", &json!({"txn_id": id}), None);
        assert_eq!(status, 200, "{rpc}: {body}");
        assert_eq!(cell(&s, "p1", None), None, "{rpc}");
        assert_eq!(cell(&s, "p2", None), None, "{rpc}");
    }
}
