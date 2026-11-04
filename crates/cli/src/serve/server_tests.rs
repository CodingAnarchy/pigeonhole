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
        request_timeout: Duration::from_secs(5),
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
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        let mut req = agent.post(format!("{}/v1/Data/{method}", self.url));
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
