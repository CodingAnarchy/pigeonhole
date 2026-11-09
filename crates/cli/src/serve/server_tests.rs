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
    /// Leave the directory behind when dropped (a replica restarted on it).
    keep_dir: bool,
}

/// A fresh directory for a server's files, with its `backups`.
fn test_dir() -> PathBuf {
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
    std::fs::create_dir_all(dir.join("backups")).unwrap();
    dir
}

fn limits() -> Limits {
    Limits {
        max_request_bytes: 1 << 20,
        inflight_bytes: 16 << 20,
        max_concurrent: 64,
        max_scans: 4,
        max_streams: 16,
        max_feed_hubs: 8,
        max_catch_up: 4,
        feed_ring_bytes: 1 << 20,
        feed_quiet: Duration::from_secs(10),
        feed_client_events: super::feed::CLIENT_EVENTS,
        feed_client_bytes: super::feed::CLIENT_BYTES,
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
        Self::start_with(tokens, limits, 1)
    }

    /// A server on a database of `shards` shards, its change feed on for every table.
    fn start_with(tokens: &str, limits: Limits, shards: usize) -> Self {
        Self::start_custom(tokens, limits, shards, |s| s)
    }

    /// `start_with`, its service changed by `f` before it serves.
    fn start_custom(
        tokens: &str,
        limits: Limits,
        shards: usize,
        f: impl FnOnce(Service) -> Service,
    ) -> Self {
        let dir = test_dir();
        let db = Pigeonhole::open(
            dir.join("t.phdb"),
            Options::default()
                .create_if_missing(true)
                .shards(shards)
                .change_feed(pigeonhole::FeedRetention::bytes(64 << 20)),
        )
        .unwrap();
        db.table("t")
            .unwrap()
            .family("f", Family::default())
            .family("n", Family::counter())
            .create()
            .unwrap();
        Self::serve(dir, db, tokens, limits, None, f)
    }

    /// A replica server (#1436) of the server at `primary`, on `dir`'s `t.phdb` (an empty
    /// database if there is none), sending `token` to the primary.
    fn start_replica(dir: PathBuf, primary: &str, token: Option<&str>) -> Self {
        std::fs::create_dir_all(dir.join("backups")).unwrap();
        let db = Pigeonhole::open(
            dir.join("t.phdb"),
            Options::default().create_if_missing(true),
        )
        .unwrap();
        let replica = super::replica::Replica::new(super::replica::ReplicaConfig {
            primary: primary.to_owned(),
            token: token.map(str::to_owned),
            run_events: 4,
        });
        Self::serve(dir, db, "", limits(), Some(replica), |s| s)
    }

    fn serve(
        dir: PathBuf,
        db: Pigeonhole,
        tokens: &str,
        limits: Limits,
        replica: Option<Arc<super::replica::Replica>>,
        f: impl FnOnce(Service) -> Service,
    ) -> Self {
        let metrics = Arc::new(Metrics::default());
        let mut service = f(Service::new(db, limits, 2, Arc::clone(&metrics))
            .backups(Some(&dir.join("backups")), 1)
            .unwrap());
        if let Some(r) = &replica {
            service = service.replica(Arc::clone(r));
        }
        let shared = Arc::new(Shared {
            router: Router::single(service),
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
            None,
        ));
        if let Some(r) = &replica {
            let _guard = runtime.enter();
            r.spawn(Arc::clone(&shared));
        }
        Self {
            url,
            shared,
            stop: Some(stop),
            runtime: Some(runtime),
            dir,
            keep_dir: false,
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
        // Names the call: a test makes several, and a transport error says nothing of which
        // (#1601, a Windows ConnectionAborted).
        let mut resp = req
            .send_json(body)
            .unwrap_or_else(|e| panic!("POST /v1/{path}: {e:?}"));
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
            if let Some(r) = &self.shared.router.service.replica {
                rt.block_on(r.stop());
            }
            rt.shutdown_timeout(Duration::from_secs(5));
        }
        self.shared.router.service.bulk.close();
        self.shared.router.service.texts.close();
        let _ = self.shared.router.service.db.clone().close();
        if !self.keep_dir {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
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
    // The test drives the expiry itself, with `sweep` at a chosen instant: the reaper runs
    // on the wall clock and, with a short lease on a slow runner, can reap the expired
    // marker before the test reads it (#1531). With a lease this long, it never does. The
    // sweep's instant is past both the idle lease and `limits()`'s 300 s transaction
    // lifetime, so either would expire them.
    let mut l = limits();
    l.leases.txn_idle = Duration::from_secs(600);
    let s = Server::start("", l);
    let idle = begin(&s, None);
    let committed = begin(&s, None);
    let (status, body) = s.rpc(
        "Transactions/Commit",
        &json!({"txn_id": committed, "rows": txn_rows("c", b"1")}),
        None,
    );
    assert_eq!(status, 200, "{body}");
    let lapsed = std::time::Instant::now() + Duration::from_secs(601);
    assert_eq!(
        s.shared.router.service.sessions.sweep(lapsed),
        2,
        "both lapse"
    );
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
fn an_aggregate_vector_input_is_refused_as_each_path_always_refused_it() {
    // The coordinator on #1555's N1: a transaction decodes an aggregate's input before it
    // checks for a vector, so malformed vector elements get the element error; Mutate refuses
    // a vector input before decoding it. Both are InvalidArgument; the messages differ.
    let s = Server::start("", limits());
    let bad = json!({"vector": {"element": "ELEMENT_F32", "dim": 2, "data": b64(&[0; 3])}});
    let write = json!({"aggregate": {"family": "f", "qualifier": b64(b"q"), "input": bad}});
    let id = begin(&s, None);
    let rows = json!([{"row": {"table": "t", "row": b64(b"r")}, "writes": [write.clone()]}]);
    let (status, e) = s.rpc(
        "Transactions/Commit",
        &json!({"txn_id": id, "rows": rows}),
        None,
    );
    assert_eq!(
        (status, e["code"].as_str()),
        (400, Some("InvalidArgument")),
        "{e}"
    );
    assert_eq!(
        e["message"], "a vector of 2 elements needs 8 bytes of data, not 3",
        "the element error, as before: {e}"
    );
    let (status, e) = s.post(
        "Mutate",
        &json!({"writes": {"row": {"table": "t", "row": b64(b"r")}, "writes": [write]}}),
        None,
    );
    assert_eq!(
        (status, e["code"].as_str()),
        (400, Some("InvalidArgument")),
        "{e}"
    );
    assert_eq!(
        e["message"], "an aggregate input is an i64, u64, f64 or bytes",
        "{e}"
    );
}

// ---- Graph.Traverse and Vectors.Nearest (D313 ticket 7) ----

/// `g`, an edge family `e`: a -> b, a -> c, b -> d, and c -> x:skip (an edge `qualifier_prefix`
/// b"x" alone keeps).
fn graph(s: &Server) -> pigeonhole::Table {
    let db = &s.shared.router.service.db;
    let g = db
        .table("g")
        .unwrap()
        .family("e", Family::default())
        .create()
        .unwrap();
    for (from, to) in [
        (&b"a"[..], &b"b"[..]),
        (b"a", b"c"),
        (b"b", b"d"),
        (b"c", b"x:skip"),
    ] {
        g.mutate(from).put("e", to, b"").commit().unwrap();
    }
    g
}

fn hops_of(r: &Json) -> Vec<Vec<String>> {
    r["hops"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| {
            h["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| n.as_str().unwrap().to_owned())
                .collect()
        })
        .collect()
}

#[test]
fn traverse_walks_an_edge_family_hop_by_hop() {
    let s = Server::start("", limits());
    let g = graph(&s);
    let (status, r) = s.rpc(
        "Graph/Traverse",
        &json!({"table": "g", "family": "e", "seeds": [b64(b"a")], "hops": 2, "parents": true}),
        None,
    );
    assert_eq!(status, 200, "{r}");
    assert_eq!(
        hops_of(&r),
        [vec![b64(b"b"), b64(b"c")], vec![b64(b"d"), b64(b"x:skip")]]
    );
    // Each node's source, as an index into the previous hop (the seeds for hop 1).
    assert_eq!(r["hops"][0]["parents"], json!([0, 0]));
    assert_eq!(r["hops"][1]["parents"], json!([0, 1]));
    // A uint64, so a string in proto3's JSON mapping.
    assert_eq!(r["stats"]["edges"], json!("4"), "{r}");
    // The edge selection: only qualifiers under `x`.
    let (_, r) = s.rpc(
        "Graph/Traverse",
        &json!({"table": "g", "family": "e", "seeds": [b64(b"c")],
                "qualifier_prefix": b64(b"x")}),
        None,
    );
    assert_eq!(hops_of(&r), [vec![b64(b"x:skip")]]);
    // At a pinned snapshot: an edge added after it isn't followed.
    let (_, opened) = s.rpc("Snapshots/Open", &json!({}), None);
    let id = opened["snapshot_id"].as_str().unwrap().to_owned();
    g.mutate(b"d").put("e", b"later", b"").commit().unwrap();
    let walk = |snapshot: Option<&str>| {
        let mut req = json!({"table": "g", "family": "e", "seeds": [b64(b"b")], "hops": 2});
        if let Some(id) = snapshot {
            req["snapshot_id"] = json!(id);
        }
        hops_of(&s.rpc("Graph/Traverse", &req, None).1)
    };
    assert_eq!(walk(Some(&id)), [vec![b64(b"d")]]);
    assert_eq!(walk(None), [vec![b64(b"d")], vec![b64(b"later")]]);
    // A reply is byte-budgeted (simp724's L1 on #1565): a node counts its key and 16 bytes,
    // so 20 bytes keep one of hop 1's two nodes, and the traversal ends there.
    let (_, r) = s.rpc(
        "Graph/Traverse",
        &json!({"table": "g", "family": "e", "seeds": [b64(b"a")], "hops": 2,
                "page_bytes": 20}),
        None,
    );
    assert_eq!(hops_of(&r).len(), 1, "{r}");
    assert_eq!(hops_of(&r)[0].len(), 1, "{r}");
    assert_eq!(r["stats"]["cut_by_budget"], json!(true), "{r}");
    // Over gRPC.
    let got = s.block_on(async {
        let channel = tonic::transport::Channel::from_shared(s.url.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        proto::graph_client::GraphClient::new(channel)
            .traverse(proto::TraverseRequest {
                table: "g".into(),
                family: "e".into(),
                seeds: vec![b"a".to_vec().into()],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
    });
    assert_eq!(got.hops.len(), 1);
    assert_eq!(got.hops[0].nodes, [b"b".to_vec(), b"c".to_vec()]);
}

#[test]
fn traverse_refuses_what_it_cannot_walk() {
    let s = Server::start("", limits());
    graph(&s);
    for (req, code) in [
        (
            json!({"table": "g", "family": "e", "seeds": []}),
            "InvalidArgument",
        ),
        (
            json!({"table": "g", "family": "e", "seeds": [b64(b"a")], "hops": 65}),
            "InvalidArgument",
        ),
        (
            json!({"table": "nope", "family": "e", "seeds": [b64(b"a")]}),
            "TableNotFound",
        ),
        (
            json!({"table": "g", "family": "nope", "seeds": [b64(b"a")]}),
            "FamilyNotFound",
        ),
    ] {
        let (status, e) = s.rpc("Graph/Traverse", &req, None);
        assert_eq!(e["code"].as_str(), Some(code), "{req}: {status} {e}");
    }
}

/// `v`, an `f32` L2 vector family `emb` of dim 2: rows a [0, 1], b [0, 2] and c [0, 3].
fn vectors(s: &Server) -> pigeonhole::Table {
    let db = &s.shared.router.service.db;
    let spec =
        pigeonhole::VectorSpec::new(2, pigeonhole::Element::F32, pigeonhole::Metric::L2).unwrap();
    let v = db
        .table("v")
        .unwrap()
        .family("emb", Family::vector(spec))
        .family("f", Family::default())
        .create()
        .unwrap();
    for (row, y) in [(&b"a"[..], 1.0f32), (b"b", 2.0), (b"c", 3.0)] {
        v.mutate(row)
            .put_vector_f32("emb", b"", &[0.0, y])
            .commit()
            .unwrap();
    }
    v
}

fn f32_query(xs: &[f32]) -> Json {
    let data: Vec<u8> = xs.iter().flat_map(|x| x.to_le_bytes()).collect();
    json!({"element": "ELEMENT_F32", "dim": xs.len(), "data": b64(&data)})
}

fn rows_of(r: &Json) -> Vec<String> {
    r["neighbours"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["row"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn nearest_ranks_a_ranges_or_a_key_lists_vectors() {
    let s = Server::start("", limits());
    vectors(&s);
    let q = f32_query(&[0.0, 0.0]);
    let (status, r) = s.rpc(
        "Vectors/Nearest",
        &json!({"table": "v", "family": "emb", "query": q, "k": 2}),
        None,
    );
    assert_eq!(status, 200, "{r}");
    assert_eq!(rows_of(&r), [b64(b"a"), b64(b"b")]);
    assert!(
        r["neighbours"][0]["distance"].as_f64().unwrap()
            < r["neighbours"][1]["distance"].as_f64().unwrap()
    );
    // A key list, and a range.
    let (_, r) = s.rpc(
        "Vectors/Nearest",
        &json!({"table": "v", "family": "emb", "query": q, "k": 5,
                "keys": {"keys": [b64(b"c"), b64(b"b")]}}),
        None,
    );
    assert_eq!(rows_of(&r), [b64(b"b"), b64(b"c")]);
    let (_, r) = s.rpc(
        "Vectors/Nearest",
        &json!({"table": "v", "family": "emb", "query": q, "k": 5,
                "range": {"prefix": b64(b"c")}}),
        None,
    );
    assert_eq!(rows_of(&r), [b64(b"c")]);
    // D308's budget: each neighbour is its 1-byte row, an empty qualifier and 16 bytes.
    let (_, r) = s.rpc(
        "Vectors/Nearest",
        &json!({"table": "v", "family": "emb", "query": q, "k": 3, "page_bytes": 40}),
        None,
    );
    assert_eq!(rows_of(&r), [b64(b"a"), b64(b"b")]);
    assert_eq!(r["cut_by_budget"], json!(true), "{r}");
}

#[test]
fn nearest_refuses_a_query_it_cannot_run() {
    let s = Server::start("", limits());
    vectors(&s);
    let q = f32_query(&[0.0, 0.0]);
    let half = json!({"element": "ELEMENT_F16", "dim": 2, "data": b64(&[0; 4])});
    let i8q = json!({"element": "ELEMENT_I8", "dim": 2, "data": b64(&[0; 2])});
    for (req, code) in [
        (
            json!({"table": "v", "family": "emb", "query": q, "k": 0}),
            "InvalidArgument",
        ),
        // A ranking keeps `k` candidates before the reply's budget cuts it: k is capped.
        (
            json!({"table": "v", "family": "emb", "query": q, "k": 10_001}),
            "InvalidArgument",
        ),
        (
            json!({"table": "v", "family": "emb", "k": 1}),
            "InvalidArgument",
        ),
        (
            json!({"table": "v", "family": "emb", "query": half, "k": 1}),
            "InvalidArgument",
        ),
        (
            json!({"table": "v", "family": "emb", "query": i8q, "k": 1}),
            "InvalidArgument",
        ),
        (
            json!({"table": "v", "family": "f", "query": q, "k": 1}),
            "InvalidArgument",
        ),
        (
            json!({"table": "nope", "family": "emb", "query": q, "k": 1}),
            "TableNotFound",
        ),
    ] {
        let (status, e) = s.rpc("Vectors/Nearest", &req, None);
        assert_eq!(e["code"].as_str(), Some(code), "{req}: {status} {e}");
    }
}

// ---- Vectors' IVF index (D313 ticket 7b1, ICR 0100) ----

fn index_spec(rebalance: &str) -> Json {
    json!({"name": "ix", "spec": {"dim": 2, "element": "ELEMENT_F32", "metric": "METRIC_L2"},
           "rebalance": rebalance})
}

fn hit_ids(r: &Json) -> Vec<String> {
    r["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["id"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn an_ivf_index_is_created_written_searched_and_verified() {
    let s = Server::start("", limits());
    let (status, e) = s.rpc("Vectors/CreateIndex", &index_spec("REBALANCE_INLINE"), None);
    assert_eq!(status, 200, "{e}");
    // A second create is TableExists, unless `if_missing`.
    let (_, e) = s.rpc("Vectors/CreateIndex", &index_spec("REBALANCE_INLINE"), None);
    assert_eq!(e["code"].as_str(), Some("TableExists"), "{e}");
    let mut again = index_spec("REBALANCE_INLINE");
    again["if_missing"] = json!(true);
    assert_eq!(s.rpc("Vectors/CreateIndex", &again, None).0, 200);
    for (id, y) in [("a", 1.0f32), ("b", 2.0), ("c", 3.0), ("x:d", 4.0)] {
        let (status, e) = s.rpc(
            "Vectors/Insert",
            &json!({"index": "ix", "id": b64(id.as_bytes()), "vector": f32_query(&[0.0, y])}),
            None,
        );
        assert_eq!(status, 200, "{id}: {e}");
    }
    let q = f32_query(&[0.0, 0.0]);
    let (status, r) = s.rpc(
        "Vectors/Search",
        &json!({"index": "ix", "query": q, "k": 2, "stats": true}),
        None,
    );
    assert_eq!(status, 200, "{r}");
    assert_eq!(hit_ids(&r), [b64(b"a"), b64(b"b")]);
    assert!(r["stats"]["lists"].as_u64().unwrap() >= 1, "{r}");
    // Only ids under a prefix.
    let (_, r) = s.rpc(
        "Vectors/Search",
        &json!({"index": "ix", "query": q, "k": 3, "id_prefix": b64(b"x:")}),
        None,
    );
    assert_eq!(hit_ids(&r), [b64(b"x:d")]);
    assert!(
        r.get("stats").is_none_or(Json::is_null),
        "stats only when asked: {r}"
    );
    // A delete, then the index's own check.
    assert_eq!(
        s.rpc(
            "Vectors/Delete",
            &json!({"index": "ix", "id": b64(b"b")}),
            None
        )
        .0,
        200
    );
    let (_, r) = s.rpc(
        "Vectors/Search",
        &json!({"index": "ix", "query": q, "k": 2}),
        None,
    );
    assert_eq!(hit_ids(&r), [b64(b"a"), b64(b"c")]);
    let (status, v) = s.rpc("Vectors/Verify", &json!({"index": "ix"}), None);
    assert_eq!((status, &v["vectors"]), (200, &json!("3")), "{v}");
    // Refused: another element type, k out of range, an index that doesn't exist.
    let i8v = json!({"element": "ELEMENT_I8", "dim": 2, "data": b64(&[0; 2])});
    for (path, req, code) in [
        (
            "Vectors/Insert",
            json!({"index": "ix", "id": b64(b"e"), "vector": i8v}),
            "InvalidArgument",
        ),
        (
            "Vectors/Search",
            json!({"index": "ix", "query": q, "k": 0}),
            "InvalidArgument",
        ),
        (
            "Vectors/Search",
            json!({"index": "ix", "query": q, "k": 10_001}),
            "InvalidArgument",
        ),
        (
            "Vectors/Search",
            json!({"index": "nope", "query": q, "k": 1}),
            "TableNotFound",
        ),
    ] {
        let (status, e) = s.rpc(path, &req, None);
        assert_eq!(e["code"].as_str(), Some(code), "{path} {req}: {status} {e}");
    }
    // Over gRPC.
    let verified = s.block_on(async {
        let channel = tonic::transport::Channel::from_shared(s.url.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        proto::vectors_client::VectorsClient::new(channel)
            .verify(proto::VectorVerifyRequest { index: "ix".into() })
            .await
            .unwrap()
            .into_inner()
    });
    assert_eq!(verified.vectors, 3);
}

#[test]
fn a_background_index_rebalances_on_a_server_thread_that_close_ends() {
    // The coordinator on 7b1: the server runs a background index's rebalancer itself, so
    // its shutdown can wait for the thread before it closes the database.
    let s = Server::start("", limits());
    let indexes = &s.shared.router.service.indexes;
    assert_eq!(
        s.rpc(
            "Vectors/CreateIndex",
            &index_spec("REBALANCE_BACKGROUND"),
            None
        )
        .0,
        200
    );
    assert_eq!(
        indexes.rebalancing(),
        1,
        "one thread for the background index"
    );
    for i in 0..50u32 {
        let (status, e) = s.rpc(
            "Vectors/Insert",
            &json!({"index": "ix", "id": b64(format!("v{i:03}").as_bytes()),
                    "vector": f32_query(&[i as f32, 1.0])}),
            None,
        );
        assert_eq!(status, 200, "{e}");
    }
    let started = std::time::Instant::now();
    indexes.close();
    assert_eq!(indexes.rebalancing(), 0, "close waited for the thread");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the thread saw its index close within one idle wait: {:?}",
        started.elapsed()
    );
    // After close, nothing opens again (ai1249's M2 on #1573): a call still queued on the
    // blocking pool at shutdown, or a request, is Closed, and starts no rebalancer.
    let late = indexes.get("ix");
    assert_eq!(late.err().map(|e| e.code), Some("Closed".to_owned()));
    let (_, e) = s.rpc(
        "Vectors/Search",
        &json!({"index": "ix", "query": f32_query(&[0.0, 1.0]), "k": 1}),
        None,
    );
    assert_eq!(e["code"].as_str(), Some("Closed"), "{e}");
    assert_eq!(indexes.rebalancing(), 0, "no thread after close");
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

// ---- Vectors.BulkLoad (D313 ticket 7b2) ----

/// A BulkLoad message of `n` vectors of `dim` f32s, ids `v{first..}`; the first message
/// names the index.
fn bulk_message(
    index: &str,
    expected: u64,
    first: usize,
    n: usize,
    dim: usize,
) -> proto::VectorBulkLoadRequest {
    let items = (first..first + n)
        .map(|i| {
            let mut v = vec![0.0f32; dim];
            v[0] = i as f32;
            proto::VectorItem {
                id: format!("v{i:05}").into_bytes().into(),
                vector: Some(proto::Vector {
                    element: proto::Element::F32 as i32,
                    dim: dim as u32,
                    data: v
                        .iter()
                        .flat_map(|x| x.to_le_bytes())
                        .collect::<Vec<u8>>()
                        .into(),
                }),
            }
        })
        .collect();
    proto::VectorBulkLoadRequest {
        index: index.into(),
        expected,
        items,
    }
}

fn vectors_client(s: &Server) -> proto::vectors_client::VectorsClient<tonic::transport::Channel> {
    let channel = s.block_on(async {
        tonic::transport::Channel::from_shared(s.url.clone())
            .unwrap()
            .connect()
            .await
            .unwrap()
    });
    proto::vectors_client::VectorsClient::new(channel)
}

fn verified(s: &Server, index: &str) -> Json {
    let (status, v) = s.rpc("Vectors/Verify", &json!({"index": index}), None);
    assert_eq!(status, 200, "{v}");
    // The JSON mapping leaves a zero count out.
    json!(v["vectors"].as_str().unwrap_or("0"))
}

#[test]
fn a_bulk_load_fills_an_empty_index_over_grpc_and_http() {
    let s = Server::start("", limits());
    assert_eq!(
        s.rpc("Vectors/CreateIndex", &index_spec("REBALANCE_INLINE"), None)
            .0,
        200
    );
    // gRPC: three messages; only the first names the index.
    let mut client = vectors_client(&s);
    let messages = vec![
        bulk_message("ix", 300, 0, 100, 2),
        bulk_message("", 0, 100, 100, 2),
        bulk_message("", 0, 200, 100, 2),
    ];
    s.block_on(client.bulk_load(tonic::codegen::tokio_stream::iter(messages)))
        .unwrap();
    assert_eq!(verified(&s, "ix"), json!("300"));
    let (_, r) = s.rpc(
        "Vectors/Search",
        &json!({"index": "ix", "query": f32_query(&[0.0, 0.0]), "k": 2}),
        None,
    );
    assert_eq!(hit_ids(&r), [b64(b"v00000"), b64(b"v00001")]);
    // The index isn't empty now: the crate refuses a second load.
    let again =
        s.block_on(
            client.bulk_load(tonic::codegen::tokio_stream::iter(vec![bulk_message(
                "ix", 1, 0, 1, 2,
            )])),
        );
    assert_eq!(again.unwrap_err().code(), tonic::Code::InvalidArgument);

    // HTTP: NDJSON, the gRPC messages as JSON lines, blank lines skipped.
    let mut spec = index_spec("REBALANCE_INLINE");
    spec["name"] = json!("ix2");
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    let ndjson = |lines: &[proto::VectorBulkLoadRequest]| {
        let mut body = String::new();
        for m in lines {
            body.push_str(&serde_json::to_string(m).unwrap());
            body.push_str("\n\n");
        }
        body
    };
    let post = |body: &[u8]| {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        let mut resp = agent
            .post(format!("{}/v1/Vectors/BulkLoad", s.url))
            .header("Content-Type", "application/x-ndjson")
            .send(body)
            .unwrap();
        let status = resp.status().as_u16();
        let body: Json = resp.body_mut().read_json().unwrap_or(Json::Null);
        (status, body)
    };
    let body = ndjson(&[
        bulk_message("ix2", 50, 0, 30, 2),
        bulk_message("", 0, 30, 20, 2),
    ]);
    let (status, r) = post(body.as_bytes());
    assert_eq!(status, 200, "{r}");
    assert_eq!(verified(&s, "ix2"), json!("50"));

    // Refused: an empty body, an unknown index, a line past --max-request-bytes (with no
    // newline, so it can't be split), and a vector of another element type, after which
    // the vectors before it are loaded.
    let (_, e) = post(b"");
    assert_eq!(e["code"].as_str(), Some("InvalidArgument"), "{e}");
    let (_, e) = post(ndjson(&[bulk_message("nope", 1, 0, 1, 2)]).as_bytes());
    assert_eq!(e["code"].as_str(), Some("TableNotFound"), "{e}");
    let (_, e) = post(&vec![b' '; (1 << 20) + 2]);
    assert_eq!(e["code"].as_str(), Some("InvalidArgument"), "{e}");
    let mut spec = index_spec("REBALANCE_INLINE");
    spec["name"] = json!("ix3");
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    let mut mixed = bulk_message("ix3", 3, 0, 3, 2);
    mixed.items[2].vector = Some(proto::Vector {
        element: proto::Element::I8 as i32,
        dim: 2,
        data: vec![0u8; 2].into(),
    });
    let (_, e) = post(ndjson(&[mixed]).as_bytes());
    assert_eq!(e["code"].as_str(), Some("InvalidArgument"), "{e}");
    assert_eq!(verified(&s, "ix3"), json!("2"));
    // A vector of another dimension in a later message (ai1249's M1): the server ends the
    // input there, so the vectors before it are written and searchable, not dropped with
    // the crate's waiting batches.
    let mut spec = index_spec("REBALANCE_INLINE");
    spec["name"] = json!("ix4");
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    let mut second = bulk_message("", 0, 50, 12, 2);
    second.items[10] = bulk_message("", 0, 60, 1, 3).items.remove(0);
    let (_, e) = post(ndjson(&[bulk_message("ix4", 62, 0, 50, 2), second]).as_bytes());
    assert_eq!(e["code"].as_str(), Some("InvalidArgument"), "{e}");
    assert!(
        e["message"]
            .as_str()
            .unwrap()
            .contains("vectors before it were loaded"),
        "{e}"
    );
    assert_eq!(verified(&s, "ix4"), json!("60"));
    let (_, r) = s.rpc(
        "Vectors/Search",
        &json!({"index": "ix4", "query": f32_query(&[59.0, 0.0]), "k": 1}),
        None,
    );
    assert_eq!(hit_ids(&r), [b64(b"v00059")]);
    // Every message's budget came back.
    assert_eq!(
        s.shared.router.service.inflight_available(),
        limits().inflight_bytes
    );
}

#[test]
fn a_slow_loader_holds_back_a_fast_client() {
    // The coordinator on 7b2, and perf287's (A): a loader that stops after its first vector
    // stops the server reading the stream, so the client's sends stall, and the server holds
    // the budget of two messages at most.
    let (started_tx, started) = std::sync::mpsc::channel::<()>();
    let (gate, gate_rx) = std::sync::mpsc::channel::<()>();
    let started_tx = std::sync::Mutex::new(started_tx);
    let gate_rx = std::sync::Mutex::new(gate_rx);
    let s = Server::start_custom("", limits(), 1, move |mut svc| {
        // Only the first load is held.
        let held = std::sync::atomic::AtomicBool::new(false);
        svc.bulk.loader = Arc::new(move |index, expected, input| {
            if held.swap(true, Ordering::Relaxed) {
                return super::bulk::load(index, expected, input);
            }
            let first = input.next();
            started_tx.lock().unwrap().send(()).unwrap();
            let _ = gate_rx.lock().unwrap().recv();
            let mut rest = first.into_iter().chain(input);
            super::bulk::load(index, expected, &mut rest)
        });
        svc
    });
    // Rebound after the server, so a failed assertion drops the gate first: the server's
    // drop waits for the loader thread, which then ends instead of waiting on the gate.
    let gate = gate;
    let mut spec = index_spec("REBALANCE_INLINE");
    spec["spec"]["dim"] = json!(1024);
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    // 200 messages of 16 vectors of 1,024 f32s: 64 KiB each, 12.8 MiB in all.
    const MESSAGES: usize = 200;
    const EACH: usize = 16;
    let message_bytes = bulk_message("", 0, 0, EACH, 1024).encoded_len();
    let sent = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&sent);
    let messages = tonic::codegen::tokio_stream::StreamExt::map(
        tonic::codegen::tokio_stream::iter(0..MESSAGES),
        move |i| {
            counted.fetch_add(1, Ordering::Relaxed);
            let index = if i == 0 { "ix" } else { "" };
            bulk_message(index, (MESSAGES * EACH) as u64, i * EACH, EACH, 1024)
        },
    );
    let mut client = vectors_client(&s);
    let load = s
        .runtime
        .as_ref()
        .unwrap()
        .spawn(async move { client.bulk_load(messages).await });
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    let stalled = sent.load(Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(1000));
    assert_eq!(
        sent.load(Ordering::Relaxed),
        stalled,
        "the client's sends stalled"
    );
    assert!(
        (stalled as usize) < MESSAGES / 2,
        "the transport's windows hold a few messages, not the stream: {stalled} sent"
    );
    let service = &s.shared.router.service;
    let reserved = limits().inflight_bytes - service.inflight_available();
    assert!(
        reserved <= 2 * message_bytes + 64,
        "the server holds two messages' budget at most: {reserved} bytes, {message_bytes} each"
    );
    let g = &service.bulk.gauges;
    assert_eq!(g.phase.load(Ordering::Relaxed), 1, "receiving");
    assert!(g.vectors.load(Ordering::Relaxed) >= EACH as u64);
    // A second load waits for the one-load slot, here past its client's deadline.
    let mut spec2 = index_spec("REBALANCE_INLINE");
    spec2["name"] = json!("ix2");
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec2, None).0, 200);
    let mut second = vectors_client(&s);
    let mut req = tonic::Request::new(tonic::codegen::tokio_stream::iter(vec![bulk_message(
        "ix2", 1, 0, 1, 2,
    )]));
    req.set_timeout(Duration::from_millis(300));
    assert!(
        s.block_on(second.bulk_load(req)).is_err(),
        "the slot is taken"
    );
    // Released, the loader takes the rest, and the client sends it.
    gate.send(()).unwrap();
    s.block_on(load).unwrap().unwrap();
    assert_eq!(sent.load(Ordering::Relaxed) as usize, MESSAGES);
    assert_eq!(verified(&s, "ix"), json!((MESSAGES * EACH).to_string()));
    assert_eq!(service.inflight_available(), limits().inflight_bytes);
    assert_eq!(g.phase.load(Ordering::Relaxed), 0, "no load");
    // The slot is free again, at once.
    s.block_on(
        second.bulk_load(tonic::codegen::tokio_stream::iter(vec![bulk_message(
            "ix2", 1, 0, 1, 2,
        )])),
    )
    .unwrap();
    assert_eq!(verified(&s, "ix2"), json!("1"));
}

#[test]
fn a_shutdown_stops_a_running_load_between_messages() {
    // The coordinator on #1580: a load can't hold the server past its grace period. Here
    // the client sends one message, then nothing: the loader waits for the next, and the
    // shutdown wakes it. The load ends with what it took, and its client hears Closed.
    let s = Server::start("", limits());
    assert_eq!(
        s.rpc("Vectors/CreateIndex", &index_spec("REBALANCE_INLINE"), None)
            .0,
        200
    );
    let messages = tonic::codegen::tokio_stream::StreamExt::chain(
        tonic::codegen::tokio_stream::iter(vec![bulk_message("ix", 100, 0, 10, 2)]),
        tonic::codegen::tokio_stream::pending(),
    );
    let mut client = vectors_client(&s);
    let load = s
        .runtime
        .as_ref()
        .unwrap()
        .spawn(async move { client.bulk_load(messages).await });
    let bulk = &s.shared.router.service.bulk;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while bulk.gauges.vectors.load(Ordering::Relaxed) < 10 {
        assert!(
            std::time::Instant::now() < deadline,
            "the load never took the message"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    bulk.stop();
    let err = s.block_on(load).unwrap().unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");
    assert!(err.message().contains("interrupted"), "{err:?}");
    assert_eq!(verified(&s, "ix"), json!("10"), "what it took is loaded");
    assert_eq!(bulk.gauges.phase.load(Ordering::Relaxed), 0, "no load");
    // No load starts once the server is stopping.
    let mut spec = index_spec("REBALANCE_INLINE");
    spec["name"] = json!("ix2");
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    let mut client = vectors_client(&s);
    let err = s
        .block_on(
            client.bulk_load(tonic::codegen::tokio_stream::iter(vec![bulk_message(
                "ix2", 1, 0, 1, 2,
            )])),
        )
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");
    assert_eq!(verified(&s, "ix2"), json!("0"));
    assert_eq!(
        s.shared.router.service.inflight_available(),
        limits().inflight_bytes
    );
}

// ---- Text (D313 ticket 7c, D309, ICR 0105) ----

fn text_hits(r: &Json) -> Vec<String> {
    r["hits"]
        .as_array()
        .map(|h| {
            h.iter()
                .map(|h| h["id"].as_str().unwrap().to_owned())
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn a_text_index_is_created_written_searched_rebuilt_and_verified() {
    let s = Server::start("", limits());
    let spec = json!({"name": "tx", "source_table": "t", "source_family": "f",
                      "source_qualifier": b64(b"body"), "stop_words": true});
    let (status, e) = s.rpc("Text/CreateIndex", &spec, None);
    assert_eq!(status, 200, "{e}");
    let (_, e) = s.rpc("Text/CreateIndex", &spec, None);
    assert_eq!(e["code"].as_str(), Some("TableExists"), "{e}");
    let mut again = spec.clone();
    again["if_missing"] = json!(true);
    assert_eq!(s.rpc("Text/CreateIndex", &again, None).0, 200);
    for (id, text) in [
        ("doc:1", "We chose Postgres in the March meeting"),
        ("doc:2", "The meeting moved to April"),
        ("note:3", "State of the Art"),
    ] {
        let (status, e) = s.rpc(
            "Text/Put",
            &json!({"index": "tx", "id": b64(id.as_bytes()), "text": text}),
            None,
        );
        assert_eq!(status, 200, "{id}: {e}");
    }
    // The source column holds the text: Put writes through the index.
    let (_, r) = s.post(
        "Get",
        &json!({"table": "t", "row": b64(b"doc:2"), "family": "f",
                                       "qualifier": b64(b"body")}),
        None,
    );
    assert!(r["cell"].is_object(), "{r}");
    // The syntax.
    let (status, r) = s.rpc(
        "Text/Search",
        &json!({"index": "tx", "syntax": "postgres \"march meeting\" meeting", "k": 10}),
        None,
    );
    assert_eq!(status, 200, "{r}");
    assert_eq!(text_hits(&r), [b64(b"doc:1"), b64(b"doc:2")]);
    // Clauses, by field name: `term` as given (so "March" misses a lowercased index), `text`
    // and a phrase's text tokenised (so "March Meeting" matches), with highlights whose
    // clause indexes are the request's.
    let (status, r) = s.rpc(
        "Text/Search",
        &json!({"index": "tx", "k": 10, "highlight": true, "clauses": {"clauses": [
            {"term": "March"},
            {"phrase": {"text": "March Meeting"}},
            {"fuzzy": {"term": "postgress", "fuzziness": "FUZZINESS_AUTO"}},
            {"text": "April"},
        ]}}),
        None,
    );
    assert_eq!(status, 200, "{r}");
    assert_eq!(text_hits(&r), [b64(b"doc:1"), b64(b"doc:2")]);
    let top = &r["hits"][0];
    assert_eq!(
        top["text"].as_str(),
        Some("We chose Postgres in the March meeting")
    );
    let clauses: Vec<u64> = top["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["clause"].as_u64().unwrap_or(0))
        .collect();
    assert!(
        clauses.contains(&1) && clauses.contains(&2) && !clauses.contains(&0),
        "{top}"
    );
    let phrase = top["matches"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["clause"].as_u64() == Some(1))
        .unwrap();
    let (start, end) = (
        phrase["start"].as_str().unwrap().parse::<usize>().unwrap(),
        phrase["end"].as_str().unwrap().parse::<usize>().unwrap(),
    );
    assert_eq!(
        &"We chose Postgres in the March meeting"[start..end],
        "March meeting"
    );
    // A phrase of only stop words, or of nothing, matches nothing.
    for text in ["the", ""] {
        let (status, r) = s.rpc(
            "Text/Search",
            &json!({"index": "tx", "k": 10,
                    "clauses": {"clauses": [{"phrase": {"text": text}}]}}),
            None,
        );
        assert_eq!(status, 200, "{text:?}: {r}");
        assert!(text_hits(&r).is_empty(), "{text:?}: {r}");
    }
    // Only ids under a prefix.
    let (_, r) = s.rpc(
        "Text/Search",
        &json!({"index": "tx", "syntax": "state meeting", "k": 10, "id_prefix": b64(b"note:")}),
        None,
    );
    assert_eq!(text_hits(&r), [b64(b"note:3")]);
    // Refused: a repeated clause (it would shift the clauses' indexes), k out of range, a
    // bad query, a slop past 65,535, no query, an index that doesn't exist.
    for (req, code) in [
        (
            json!({"index": "tx", "k": 1, "clauses": {"clauses": [{"term": "a"}, {"text": "b"}, {"term": "a"}]}}),
            "InvalidArgument",
        ),
        (
            json!({"index": "tx", "k": 0, "syntax": "a"}),
            "InvalidArgument",
        ),
        (
            json!({"index": "tx", "k": 10_001, "syntax": "a"}),
            "InvalidArgument",
        ),
        (
            json!({"index": "tx", "k": 1, "syntax": "\"unclosed"}),
            "InvalidArgument",
        ),
        (
            json!({"index": "tx", "k": 1, "clauses": {"clauses": [{"phrase": {"text": "a b", "slop": 70_000}}]}}),
            "InvalidArgument",
        ),
        (json!({"index": "tx", "k": 1}), "InvalidArgument"),
        (
            json!({"index": "nope", "k": 1, "syntax": "a"}),
            "TableNotFound",
        ),
    ] {
        let (status, e) = s.rpc("Text/Search", &req, None);
        assert_eq!(e["code"].as_str(), Some(code), "{req}: {status} {e}");
    }
    // A delete, then the index's own check and a rebuild that changes nothing.
    assert_eq!(
        s.rpc(
            "Text/Delete",
            &json!({"index": "tx", "id": b64(b"doc:2")}),
            None
        )
        .0,
        200
    );
    let (_, r) = s.rpc(
        "Text/Search",
        &json!({"index": "tx", "syntax": "meeting", "k": 10}),
        None,
    );
    assert_eq!(text_hits(&r), [b64(b"doc:1")]);
    let (status, v) = s.rpc("Text/Verify", &json!({"index": "tx"}), None);
    assert_eq!(status, 200, "{v}");
    assert_eq!(v["documents"], json!("2"), "{v}");
    assert!(v.get("forward_mismatch").is_none_or(|m| m == "0"), "{v}");
    assert!(v.get("source_mismatch").is_none_or(|m| m == "0"), "{v}");
    let (status, r) = s.rpc("Text/Rebuild", &json!({"index": "tx"}), None);
    assert_eq!(status, 200, "{r}");
    assert_eq!(r["documents"], json!("2"), "{r}");
    assert!(r.get("changed").is_none_or(|c| c == "0"), "{r}");
    // A catalog change on a replica drops the text handles with the rest (#1436); the next
    // call opens the index again by name.
    let service = &s.shared.router.service;
    assert!(service.texts.cached("tx").is_some());
    service.tables_changed();
    assert!(service.texts.cached("tx").is_none());
    let (_, r) = s.rpc(
        "Text/Search",
        &json!({"index": "tx", "syntax": "meeting", "k": 10}),
        None,
    );
    assert_eq!(text_hits(&r), [b64(b"doc:1")]);
    // Shutdown's close waits for a rebuild, and refuses one after it (ai1249's S1).
    let texts = &s.shared.router.service.texts;
    texts.close();
    let (_, e) = s.rpc("Text/Rebuild", &json!({"index": "tx"}), None);
    assert_eq!(e["code"].as_str(), Some("Closed"), "{e}");
    // Over gRPC.
    let hits = s.block_on(async {
        let channel = tonic::transport::Channel::from_shared(s.url.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        proto::text_client::TextClient::new(channel)
            .search(proto::TextSearchRequest {
                index: "tx".into(),
                k: 5,
                query: Some(proto::text_search_request::Query::Syntax("postgres".into())),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
            .hits
    });
    assert_eq!(hits.len(), 1);
    assert_eq!(&hits[0].id[..], b"doc:1");
}

// ---- Feed.Subscribe (D313 ticket 5, R2) ----

/// A feed subscription straight through the service: events are taken only when polled.
fn direct_feed(s: &Server, req: proto::SubscribeRequest) -> super::feed::FeedEvents {
    match s
        .block_on(s.shared.router.service.call(
            ServiceOp::Subscribe(req),
            &Caller::anonymous(),
            None,
        ))
        .unwrap()
    {
        super::service::ServiceReply::Feed(events) => events,
        _ => unreachable!(),
    }
}

/// The next event within `wait`.
fn next_event(
    s: &Server,
    events: &mut super::feed::FeedEvents,
    wait: Duration,
) -> Option<Result<proto::FeedEvent, super::errors::ApiError>> {
    use futures_core::Stream as _;
    s.block_on(async {
        tokio::time::timeout(
            wait,
            std::future::poll_fn(|cx| std::pin::Pin::new(&mut *events).poll_next(cx)),
        )
        .await
    })
    .ok()
    .flatten()
}

fn subscribe(tables: &[&str], prefix: &[u8]) -> proto::SubscribeRequest {
    proto::SubscribeRequest {
        tables: tables.iter().map(|t| (*t).to_owned()).collect(),
        prefix: prefix.to_vec().into(),
        ..Default::default()
    }
}

/// Commits a put of `row`'s `f:q` in table `t`.
fn write_row(s: &Server, row: &[u8]) -> u64 {
    let t = s
        .shared
        .router
        .service
        .db
        .table("t")
        .unwrap()
        .open()
        .unwrap();
    t.mutate(row).put("f", b"q", b"v").commit().unwrap().seqno
}

/// A commit event's (seqno, rows), or `None` for a progress event.
fn commit_of(e: &proto::FeedEvent) -> Option<(u64, Vec<Vec<u8>>)> {
    match &e.event {
        Some(proto::feed_event::Event::Commit(c)) => Some((
            c.seqno,
            c.changes.iter().map(|ch| ch.row.to_vec()).collect(),
        )),
        _ => None,
    }
}

/// Every commit event until none arrives for `quiet`.
fn drain(
    s: &Server,
    events: &mut super::feed::FeedEvents,
    quiet: Duration,
) -> Vec<(u64, Vec<Vec<u8>>, Vec<u8>)> {
    let mut out = Vec::new();
    while let Some(e) = next_event(s, events, quiet) {
        let e = e.unwrap();
        if let Some((seqno, rows)) = commit_of(&e) {
            out.push((seqno, rows, e.token.to_vec()));
        }
    }
    out
}

#[test]
fn a_feed_streams_commits_under_its_prefix_and_resumes_from_a_token() {
    let s = Server::start("", limits());
    let mut events = direct_feed(&s, subscribe(&["t"], b"a"));
    let a1 = write_row(&s, b"a1");
    write_row(&s, b"b1"); // passed over: no row under the prefix
    let a2 = write_row(&s, b"a2");
    let got = drain(&s, &mut events, Duration::from_millis(300));
    assert_eq!(
        got.iter()
            .map(|(n, r, _)| (*n, r.clone()))
            .collect::<Vec<_>>(),
        [(a1, vec![b"a1".to_vec()]), (a2, vec![b"a2".to_vec()])]
    );
    // From the first event's token: the stream starts right after it, on its own feed, and
    // rejoins the shared one once it reaches it.
    let mut resumed = direct_feed(
        &s,
        proto::SubscribeRequest {
            start: Some(proto::subscribe_request::Start::Resume(
                got[0].2.clone().into(),
            )),
            ..subscribe(&["t"], b"a")
        },
    );
    let a3 = write_row(&s, b"a3");
    let again = drain(&s, &mut resumed, Duration::from_millis(300));
    assert_eq!(
        again.iter().map(|(n, _, _)| *n).collect::<Vec<_>>(),
        [a2, a3],
        "after a1, once each"
    );
    assert!(
        s.shared
            .router
            .service
            .feeds
            .stats
            .rejoined
            .load(Ordering::Relaxed)
            >= 1
    );
    let text = s.shared.metrics.render(&s.shared.router.service.db);
    let mut text = text;
    super::service::render_feeds(&mut text, &s.shared.router.service.feeds);
    assert!(text.contains("pigeonhole_serve_feed_hubs 1"), "{text}");
}

#[test]
fn a_quiet_feed_moves_its_token_with_a_progress_event() {
    let mut l = limits();
    l.feed_quiet = Duration::from_millis(150);
    let s = Server::start("", l);
    let mut events = direct_feed(&s, subscribe(&["t"], b"a"));
    write_row(&s, b"b1");
    // Only commits outside the prefix: after a quiet interval, a progress event carries the
    // token past them (perf287's S2), the token of the last commit it covers.
    let e = next_event(&s, &mut events, Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(
        matches!(e.event, Some(proto::feed_event::Event::Progress(_))),
        "{e:?}"
    );
    // Resuming from it starts after b1.
    let mut resumed = direct_feed(
        &s,
        proto::SubscribeRequest {
            start: Some(proto::subscribe_request::Start::Resume(e.token.clone())),
            ..subscribe(&["t"], b"")
        },
    );
    let b2 = write_row(&s, b"b2");
    let got = drain(&s, &mut resumed, Duration::from_millis(300));
    assert_eq!(got.iter().map(|(n, _, _)| *n).collect::<Vec<_>>(), [b2]);
}

#[test]
fn a_laggard_dropped_between_two_passed_over_commits_misses_nothing() {
    // feed1248's (2) on ticket 5: a client that falls off the ring, where the commits around
    // its position are ones its prefix passes over, goes on from its own token and rejoins
    // with nothing missed or repeated.
    let mut l = limits();
    l.feed_ring_bytes = 2048; // a few entries
    // A one-event buffer: a client that takes nothing stops reading the ring at once.
    l.feed_client_events = 1;
    let s = Server::start("", l);
    let mut slow = direct_feed(&s, subscribe(&["t"], b"a"));
    let mut want = vec![write_row(&s, b"a0")];
    let first = next_event(&s, &mut slow, Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(commit_of(&first).unwrap().0, want[0]);
    // While it takes nothing: two commits it passes over, then enough to push them off.
    write_row(&s, b"b1");
    write_row(&s, b"b2");
    for i in 0..40u32 {
        let row = if i % 3 == 0 {
            format!("a{i}")
        } else {
            format!("b{i}")
        };
        let n = write_row(&s, row.as_bytes());
        if i % 3 == 0 {
            want.push(n);
        }
    }
    let rest = drain(&s, &mut slow, Duration::from_millis(500));
    assert_eq!(
        rest.iter().map(|(n, _, _)| *n).collect::<Vec<_>>(),
        want[1..],
        "every commit under the prefix, once, in order"
    );
    let stats = &s.shared.router.service.feeds.stats;
    assert!(
        stats.fell_behind.load(Ordering::Relaxed) >= 1,
        "it fell off the ring"
    );
}

/// A client that takes nothing: its buffer fills, then the stream ends with
/// `DeadlineExceeded` after `--stream-idle-ms`. The events it buffered come first, in order.
fn stalled_feed(client_events: usize, client_bytes: usize, writes: usize) -> (Vec<u64>, Vec<u64>) {
    let mut l = limits();
    l.stream_idle = Duration::from_millis(300);
    l.feed_client_events = client_events;
    l.feed_client_bytes = client_bytes;
    let s = Server::start("", l);
    let mut stalled = direct_feed(&s, subscribe(&["t"], b""));
    let written: Vec<u64> = (0..writes)
        .map(|i| write_row(&s, format!("r{i:04}").as_bytes()))
        .collect();
    std::thread::sleep(Duration::from_millis(900));
    let mut got = Vec::new();
    loop {
        match next_event(&s, &mut stalled, Duration::from_secs(5)) {
            Some(Ok(e)) => got.extend(commit_of(&e).map(|(n, _)| n)),
            Some(Err(e)) => {
                assert_eq!(e.code, "DeadlineExceeded", "{e:?}");
                assert!(e.message.contains("--stream-idle-ms"), "{e:?}");
                break;
            }
            None => panic!("the stream ends with an error, after {} events", got.len()),
        }
    }
    assert!(next_event(&s, &mut stalled, Duration::from_secs(1)).is_none());
    (got, written)
}

#[test]
fn a_feed_client_that_takes_nothing_fills_its_buffer_then_times_out() {
    let (got, written) = stalled_feed(super::feed::CLIENT_EVENTS, super::feed::CLIENT_BYTES, 100);
    assert_eq!(
        got,
        written[..super::feed::CLIENT_EVENTS],
        "the full buffer, in order"
    );
}

#[test]
fn a_feed_clients_buffer_is_bounded_by_bytes_too() {
    // About 100 bytes an event: a 1 KiB buffer holds far fewer than 64 of them.
    let (got, written) = stalled_feed(super::feed::CLIENT_EVENTS, 1024, 100);
    assert!(
        !got.is_empty() && got.len() < 16,
        "{} events buffered",
        got.len()
    );
    assert_eq!(got, written[..got.len()], "in order");
}

#[test]
fn grpc_feed_events_match_the_services_shared_encoding_or_not() {
    // Encode-once (#1397 ticket 5): a client with no prefix gets each ring entry as bytes
    // encoded once for all such clients; a prefixed one gets its own message. Over the wire
    // both are exactly what a direct subscriber sees, tokens included.
    let s = Server::start("", limits());
    let url = s.url.clone();
    let grpc = |prefix: &[u8]| {
        let req = subscribe(&["t"], prefix);
        let url = url.clone();
        s.block_on(async move {
            let channel = tonic::transport::Channel::from_shared(url)
                .unwrap()
                .connect()
                .await
                .unwrap();
            proto::feed_client::FeedClient::new(channel)
                .subscribe(req)
                .await
                .unwrap()
                .into_inner()
        })
    };
    let mut all = grpc(b"");
    let mut under_a = grpc(b"a");
    let mut direct = direct_feed(&s, subscribe(&["t"], b""));
    for row in [&b"a1"[..], b"b1", b"a2"] {
        write_row(&s, row);
    }
    let take = |stream: &mut tonic::Streaming<proto::FeedEvent>, n: usize| {
        s.block_on(async {
            let mut out = Vec::new();
            while out.len() < n {
                let e = tokio::time::timeout(Duration::from_secs(5), stream.message())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                if commit_of(&e).is_some() {
                    out.push(e);
                }
            }
            out
        })
    };
    let want: Vec<proto::FeedEvent> = (0..3)
        .map(|_| {
            next_event(&s, &mut direct, Duration::from_secs(5))
                .unwrap()
                .unwrap()
        })
        .collect();
    assert_eq!(take(&mut all, 3), want, "no prefix: the shared encoding");
    let a: Vec<proto::FeedEvent> = take(&mut under_a, 2);
    assert_eq!(
        a.iter().map(|e| commit_of(e).unwrap()).collect::<Vec<_>>(),
        [&want[0], &want[2]].map(|e| commit_of(e).unwrap()),
        "a prefix: its own messages"
    );
    assert_eq!(a[0].token, want[0].token);
    assert_eq!(a[1].token, want[2].token);
}

#[test]
fn feed_streams_match_a_direct_feed_across_fall_offs() {
    // feed1248's (6): each client's stream, ring and own-stream catch-up alike, delivers
    // exactly what a direct FeedStream on the same spec does. Seeded; prints the seed.
    let seed: u64 = std::env::var("PHDB_FEED_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        });
    eprintln!("feed_streams_match_a_direct_feed_across_fall_offs seed={seed}");
    let mut rng = seed | 1;
    let mut rand = move |n: u64| {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng % n
    };
    let mut l = limits();
    l.feed_ring_bytes = 2048;
    // One-event buffers, so a stalled round takes clients off the ring.
    l.feed_client_events = 1;
    l.max_catch_up = 2;
    // A client stalls for runs of rounds by design; on a slow runner a run outlasted the 5 s
    // idle limit and the stream ended DeadlineExceeded (Windows, #1592's CI). Idling out has
    // its own tests: here a stall only takes a client off the ring.
    l.stream_idle = Duration::from_secs(600);
    let s = Server::start_with("", l, 2);
    let prefixes: [&[u8]; 4] = [b"", b"a", b"b", b"ab"];
    let t = s
        .shared
        .router
        .service
        .db
        .table("t")
        .unwrap()
        .open()
        .unwrap();
    let mut direct: Vec<pigeonhole::Feed> = prefixes
        .iter()
        .map(|p| t.feed().prefix(p).open().unwrap())
        .collect();
    let mut clients: Vec<super::feed::FeedEvents> = prefixes
        .iter()
        .map(|p| direct_feed(&s, subscribe(&["t"], p)))
        .collect();
    let mut got: Vec<Vec<u64>> = vec![Vec::new(); prefixes.len()];
    for round in 0..30 {
        for _ in 0..rand(8) {
            let mut m = t.mutate(&[b'a' + rand(3) as u8, b'a' + rand(3) as u8]);
            for _ in 0..1 + rand(2) {
                m = m.put("f", format!("q{}", rand(4)).as_bytes(), &[0u8; 64]);
            }
            m.commit().unwrap();
        }
        // Some clients read now; others stall this round (and may fall off the ring). In
        // rounds 10 to 14 every client stalls, so more fall off than there are catch-up slots.
        for (i, c) in clients.iter_mut().enumerate() {
            let stall = (10..15).contains(&round) || rand(2) == 0;
            if !stall || round == 29 {
                for (n, _, _) in drain(
                    &s,
                    c,
                    Duration::from_millis(if round == 29 { 600 } else { 20 }),
                ) {
                    got[i].push(n);
                }
            }
        }
    }
    for (i, d) in direct.iter_mut().enumerate() {
        let mut want = Vec::new();
        while let Some(c) = d.next(Duration::from_millis(100)).unwrap() {
            want.push(c.seqno());
        }
        assert_eq!(got[i], want, "prefix {:?}, seed {seed}", prefixes[i]);
    }
    let stats = &s.shared.router.service.feeds.stats;
    let (fell, rejoined, waits) = (
        stats.fell_behind.load(Ordering::Relaxed),
        stats.rejoined.load(Ordering::Relaxed),
        stats.catch_up_waits.load(Ordering::Relaxed),
    );
    eprintln!("fell behind {fell}, rejoined {rejoined}, catch-up waits {waits}");
    assert!(
        fell > 0 && rejoined > 0,
        "the run took clients off the ring and back, seed {seed}"
    );
    drop(t);
}

#[test]
fn feed_streams_over_http_carry_their_token_as_the_sse_id_and_resume_from_it() {
    let s = Server::start("", limits());
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_global(Some(Duration::from_millis(800)))
        .build()
        .into();
    // A stream doesn't end on its own: write first, read what arrives before the timeout.
    let read = |last_event_id: Option<&str>| -> String {
        let mut req = agent
            .post(format!("{}/v1/Feed/Subscribe", s.url))
            .header("Accept", "text/event-stream");
        if let Some(id) = last_event_id {
            req = req.header("Last-Event-ID", id);
        }
        let body = json!({"tables": ["t"], "from_oldest": true});
        let mut resp = req.send_json(&body).unwrap();
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "text/event-stream"
        );
        let mut text = String::new();
        let _ = std::io::Read::read_to_string(&mut resp.body_mut().as_reader(), &mut text);
        text
    };
    let first = write_row(&s, b"r1");
    let second = write_row(&s, b"r2");
    let text = read(None);
    let events: Vec<&str> = text
        .split("\n\n")
        .filter(|e| e.starts_with("id: "))
        .collect();
    assert!(events.len() >= 2, "{text}");
    let id = events[0]
        .lines()
        .next()
        .unwrap()
        .strip_prefix("id: ")
        .unwrap();
    let data: Json = serde_json::from_str(
        events[0]
            .lines()
            .find_map(|l| l.strip_prefix("data: "))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(data["token"], id, "the id is the event's token");
    assert_eq!(data["commit"]["seqno"], first.to_string());
    // Last-Event-ID wins over the body's from_oldest: the stream starts after r1.
    let text = read(Some(id));
    let data: Json =
        serde_json::from_str(text.lines().find_map(|l| l.strip_prefix("data: ")).unwrap()).unwrap();
    assert_eq!(data["commit"]["seqno"], second.to_string(), "{text}");
}

#[test]
fn feed_streams_and_hubs_are_capped() {
    let mut l = limits();
    l.max_streams = 2;
    l.max_feed_hubs = 1;
    let s = Server::start("", l);
    s.shared
        .router
        .service
        .db
        .table("u")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    let _a = direct_feed(&s, subscribe(&["t"], b""));
    // Another table set needs a second hub.
    let e = s
        .block_on(s.shared.router.service.call(
            ServiceOp::Subscribe(subscribe(&["u"], b"")),
            &Caller::anonymous(),
            None,
        ))
        .unwrap_err();
    assert!(e.message.contains("--max-feed-hubs"), "{e:?}");
    // The same set shares the hub; a third stream is past --max-streams.
    let _b = direct_feed(&s, subscribe(&["t"], b"x"));
    let e = s
        .block_on(s.shared.router.service.call(
            ServiceOp::Subscribe(subscribe(&["t"], b"")),
            &Caller::anonymous(),
            None,
        ))
        .unwrap_err();
    assert!(e.message.contains("--max-streams"), "{e:?}");
    let e = s.block_on(s.shared.router.service.call(
        ServiceOp::Subscribe(subscribe(&[], b"")),
        &Caller::anonymous(),
        None,
    ));
    assert!(e.is_err());
}

#[test]
fn catch_up_streams_past_the_cap_wait_their_turn() {
    // perf287's (2) on ticket 5: at most --max-catch-up clients read their own feed at once;
    // the rest wait, and are counted, then deliver everything all the same.
    let mut l = limits();
    l.max_catch_up = 1;
    // A one-event buffer: the first, untaken, keeps its catch-up slot while the second
    // asks for it. With a deeper one it could buffer all five, rejoin and free the slot
    // first.
    l.feed_client_events = 1;
    // Not an idle-limit test: out of reach of a slow runner (#1531).
    l.stream_idle = Duration::from_secs(600);
    let s = Server::start("", l);
    let written: Vec<u64> = (0..5)
        .map(|i| write_row(&s, format!("r{i}").as_bytes()))
        .collect();
    let oldest = || proto::SubscribeRequest {
        start: Some(proto::subscribe_request::Start::FromOldest(true)),
        ..subscribe(&["t"], b"")
    };
    let mut first = direct_feed(&s, oldest());
    let mut second = direct_feed(&s, oldest());
    let feeds = &s.shared.router.service.feeds;
    wait_for("the second waits for the one slot", || {
        feeds.stats.catch_up_waits.load(Ordering::Relaxed) == 1
    });
    // Either client may hold the slot: their tasks race for it. Whichever does gets it back
    // only once it has caught up and rejoined the ring (up to a REJOIN_TICK later), and
    // with a one-event buffer it catches up only as its events are taken. So take from both
    // streams in turn, never one to the end first: reading the waiting one first would
    // starve the holder (#1531: on Windows CI, nothing moved for 10 s). Collect until every
    // commit is in, or a long deadline passes.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut got = [Vec::new(), Vec::new()];
    while got.iter().any(|g| g.len() < written.len()) && std::time::Instant::now() < deadline {
        for (i, events) in [&mut first, &mut second].into_iter().enumerate() {
            if got[i].len() < written.len()
                && let Some(e) = next_event(&s, events, Duration::from_millis(50))
            {
                got[i].extend(commit_of(&e.unwrap()).map(|(n, _)| n));
            }
        }
    }
    assert_eq!(got[0], written, "the first stream");
    assert_eq!(got[1], written, "the second stream");
}

#[test]
fn caught_up_clients_rejoin_a_quiet_table_set_while_other_tables_write() {
    // simp724's F1 on #1518: with one catch-up slot, clients resuming from a token on a table
    // nobody writes must still get back on the ring, as other tables' writes move the
    // feed's position, and hand the slot on, so none starves.
    let mut l = limits();
    l.max_catch_up = 1;
    let s = Server::start("", l);
    let db = &s.shared.router.service.db;
    db.table("u")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    let u = db.table("u").unwrap().open().unwrap();
    // A token on `t` from an earlier stream.
    let mut first = direct_feed(&s, subscribe(&["t"], b""));
    let r1 = write_row(&s, b"r1");
    let token = next_event(&s, &mut first, Duration::from_secs(5))
        .unwrap()
        .unwrap()
        .token;
    drop(first);
    let resume = || proto::SubscribeRequest {
        start: Some(proto::subscribe_request::Start::Resume(token.clone())),
        ..subscribe(&["t"], b"")
    };
    let mut a = direct_feed(&s, resume());
    let mut b = direct_feed(&s, resume());
    let mut c = direct_feed(
        &s,
        proto::SubscribeRequest {
            start: Some(proto::subscribe_request::Start::FromOldest(true)),
            ..subscribe(&["t"], b"")
        },
    );
    // Only `u` is written: `t` stays quiet.
    for i in 0..20u32 {
        u.mutate(&i.to_be_bytes())
            .put("f", b"q", b"v")
            .commit()
            .unwrap();
    }
    let feeds = &s.shared.router.service.feeds;
    // The Oldest client delivers r1 first; then all three are on the ring.
    let got_c = drain(&s, &mut c, Duration::from_millis(300));
    assert_eq!(got_c.iter().map(|(n, _, _)| *n).collect::<Vec<_>>(), [r1]);
    wait_for("all three rejoin the ring, the slot handed on", || {
        feeds.stats.rejoined.load(Ordering::Relaxed) >= 3 && feeds.gauges().catching_up == 0
    });
    // A write to `t` reaches each once.
    let r2 = write_row(&s, b"r2");
    for events in [&mut a, &mut b, &mut c] {
        let got = drain(&s, events, Duration::from_millis(300));
        assert_eq!(got.iter().map(|(n, _, _)| *n).collect::<Vec<_>>(), [r2]);
    }
    drop(u);
}

#[test]
fn a_now_subscriber_gets_a_write_made_right_after_it_subscribed() {
    // simp724's F5 on #1518: a Now start's position and its `after` are taken together at
    // subscribe, so a commit pushed before the client's task first runs is still its.
    let s = Server::start("", limits());
    // A subscriber that keeps the hub warm.
    let _keep = direct_feed(&s, subscribe(&["t"], b""));
    write_row(&s, b"warm");
    for i in 0..100u32 {
        let row = format!("n{i:03}");
        let mut c = direct_feed(&s, subscribe(&["t"], row.as_bytes()));
        let n = write_row(&s, row.as_bytes());
        let e = next_event(&s, &mut c, Duration::from_secs(5))
            .unwrap_or_else(|| panic!("round {i}: the write never arrived"))
            .unwrap();
        assert_eq!(commit_of(&e).map(|(s, _)| s), Some(n), "round {i}");
    }
}

// ---- Schema (D313 ticket 6) ----

#[test]
fn schema_creates_describes_lists_and_drops_tables() {
    let s = Server::start("", limits());
    let spec = json!({"name": "m", "if_missing": false, "families": [
        {"name": "h", "aggregate": {"kind": "AGGREGATION_KIND_HISTOGRAM", "histogram_bounds": [1.0, 10.0, 100.0]}},
        {"name": "b", "aggregate": {"kind": "AGGREGATION_KIND_BLOOM", "bloom_bits": 1024, "bloom_hashes": 3}},
        {"name": "v", "vector": {"dim": 4, "element": "ELEMENT_F32", "metric": "METRIC_COSINE"}},
        {"name": "o", "max_versions": 3, "ttl_micros": "60000000", "bloom_bits": 10,
         "compression": {"codec": "COMPRESSION_CODEC_ZSTD", "zstd_level": 5},
         "cache_priority": "PRIORITY_HIGH", "compaction": "COMPACTION_STYLE_TIERED"}
    ]});
    let (status, created) = s.rpc("Schema/CreateTable", &spec, None);
    assert_eq!(status, 200, "{created}");
    let (_, described) = s.rpc("Schema/DescribeTable", &json!({"name": "m"}), None);
    assert_eq!(created, described);
    let fam = |n: &str| {
        described["families"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["name"] == n)
            .unwrap()
            .clone()
    };
    assert_eq!(
        fam("h")["aggregate"]["histogram_bounds"],
        json!([1.0, 10.0, 100.0])
    );
    assert_eq!(fam("b")["aggregate"]["bloom_hashes"], 3);
    assert_eq!(fam("v")["vector"]["metric"], "METRIC_COSINE");
    let o = fam("o");
    assert_eq!(
        (o["max_versions"].as_u64(), o["ttl_micros"].as_str()),
        (Some(3), Some("60000000"))
    );
    assert_eq!(o["compression"]["zstd_level"], 5);
    assert_eq!(
        (o["cache_priority"].as_str(), o["compaction"].as_str()),
        (Some("PRIORITY_HIGH"), Some("COMPACTION_STYLE_TIERED"))
    );
    // A described table creates again as itself: the spec round-trips.
    let mut again = described.clone();
    again["name"] = json!("m2");
    let (status, made) = s.rpc("Schema/CreateTable", &again, None);
    assert_eq!(status, 200, "{made}");
    let mut renamed = made.clone();
    renamed["name"] = json!("m");
    assert_eq!(renamed, described);
    // if_missing opens the table and adds only the families it lacks.
    let (status, grown) = s.rpc(
        "Schema/CreateTable",
        &json!({"name": "m", "if_missing": true, "families": [{"name": "o", "max_versions": 9}, {"name": "x"}]}),
        None,
    );
    assert_eq!(status, 200, "{grown}");
    let names: Vec<&str> = grown["families"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"x") && names.len() == 5, "{grown}");
    let o = grown["families"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"] == "o")
        .unwrap();
    assert_eq!(
        o["max_versions"], 3,
        "an existing family keeps its stored options"
    );
    // Without if_missing, an existing table is TableExists.
    let (status, e) = s.rpc("Schema/CreateTable", &json!({"name": "m"}), None);
    assert_eq!(
        (status, e["code"].as_str()),
        (409, Some("TableExists")),
        "{e}"
    );
    // Listed, over the RPC and GET /v1/tables; then dropped.
    let (_, list) = s.rpc("Schema/ListTables", &json!({}), None);
    assert_eq!(list["tables"], json!(["m", "m2", "t"]));
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    let got: Json = agent
        .get(format!("{}/v1/tables", s.url))
        .call()
        .unwrap()
        .body_mut()
        .read_json()
        .unwrap();
    assert_eq!(got, list);
    assert_eq!(
        s.rpc("Schema/DropTable", &json!({"name": "m2"}), None).0,
        200
    );
    let (status, e) = s.rpc("Schema/DescribeTable", &json!({"name": "m2"}), None);
    assert_eq!(
        (status, e["code"].as_str()),
        (404, Some("TableNotFound")),
        "{e}"
    );
    // A bad spec is InvalidArgument and creates nothing.
    let (status, e) = s.rpc(
        "Schema/CreateTable",
        &json!({"name": "bad", "families": [{"name": "a", "aggregate": {}}]}),
        None,
    );
    assert_eq!(
        (status, e["code"].as_str()),
        (400, Some("InvalidArgument")),
        "{e}"
    );
    assert_eq!(
        s.rpc("Schema/DescribeTable", &json!({"name": "bad"}), None)
            .0,
        404
    );
}

#[test]
fn schema_changes_need_their_scope() {
    let s = Server::start("read r3ad\nwrite wr1te\nadmin adm1n\n", limits());
    let create = json!({"name": "n", "families": [{"name": "f"}]});
    let (status, e) = s.rpc("Schema/CreateTable", &create, Some("r3ad"));
    assert_eq!(
        (status, e["code"].as_str()),
        (403, Some("PermissionDenied")),
        "{e}"
    );
    assert_eq!(s.rpc("Schema/CreateTable", &create, Some("wr1te")).0, 200);
    assert_eq!(
        s.rpc("Schema/DescribeTable", &json!({"name": "n"}), Some("r3ad"))
            .0,
        200
    );
    // Dropping a table drops its data: admin only.
    let (status, e) = s.rpc("Schema/DropTable", &json!({"name": "n"}), Some("wr1te"));
    assert_eq!(
        (status, e["code"].as_str()),
        (403, Some("PermissionDenied")),
        "{e}"
    );
    assert_eq!(
        s.rpc("Schema/DropTable", &json!({"name": "n"}), Some("adm1n"))
            .0,
        200
    );
}

#[test]
fn a_table_dropped_and_created_again_is_a_new_table_to_its_feeds() {
    // feed1248's R2 on #1518, deferred to ticket 6: hubs are keyed by the table's id, and
    // the service's handle for a name is replaced when it's dropped and created again, so
    // a subscriber after that follows the new table, never the old one's hub.
    let s = Server::start("", limits());
    let create = json!({"name": "r", "families": [{"name": "f"}]});
    assert_eq!(s.rpc("Schema/CreateTable", &create, None).0, 200);
    let put_r = |row: &str| {
        let (status, body) = s.post(
            "Mutate",
            &json!({"writes": {"row": {"table": "r", "row": b64(row.as_bytes())},
                "writes": [{"put": {"family": "f", "qualifier": b64(b"q"), "value": {"bytes": b64(b"v")}}}]}}),
            None,
        );
        assert_eq!(status, 200, "{body}");
        body["commit"]["seqno"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    let mut old = direct_feed(&s, subscribe(&["r"], b""));
    let first = put_r("a");
    assert_eq!(
        drain(&s, &mut old, Duration::from_millis(500))
            .iter()
            .map(|(n, _, _)| *n)
            .collect::<Vec<_>>(),
        [first]
    );
    // Dropped and created again under the same name, while the old stream is open.
    assert_eq!(
        s.rpc("Schema/DropTable", &json!({"name": "r"}), None).0,
        200
    );
    assert_eq!(s.rpc("Schema/CreateTable", &create, None).0, 200);
    let mut new = direct_feed(&s, subscribe(&["r"], b""));
    let second = put_r("b");
    assert_eq!(
        drain(&s, &mut new, Duration::from_millis(500))
            .iter()
            .map(|(n, _, _)| *n)
            .collect::<Vec<_>>(),
        [second],
        "the new table's commits reach a new subscriber"
    );
    assert!(s.shared.router.service.feeds.gauges().hubs >= 1);
    // The old stream never gets the new table's commit: it follows the dropped table, and
    // ends with an error rather than go on silently.
    let rest: Vec<_> =
        std::iter::from_fn(|| next_event(&s, &mut old, Duration::from_millis(500))).collect();
    assert!(
        rest.iter().all(|e| e
            .as_ref()
            .map_or(true, |e| commit_of(e).is_none_or(|(n, _)| n != second))),
        "{rest:?}"
    );
}

// ---- Admin (D313 ticket 6) ----

#[test]
fn admin_reports_maintains_and_answers_health_without_a_token() {
    let s = Server::start("read r3ad\nadmin adm1n\n", limits());
    // Health needs no token, over HTTP's alias, its RPC path, and gRPC.
    let (status, h) = s.rpc("Admin/Health", &json!({}), None);
    assert_eq!((status, h["status"].as_str()), (200, Some("ok")), "{h}");
    let mut admin = s.block_on(async {
        proto::admin_client::AdminClient::new(
            tonic::transport::Channel::from_shared(s.url.clone())
                .unwrap()
                .connect()
                .await
                .unwrap(),
        )
    });
    let h = s
        .block_on(admin.health(proto::Empty {}))
        .unwrap()
        .into_inner();
    assert_eq!(h.status, "ok");
    // Info and Metrics with read scope; maintenance with admin only.
    let (status, info) = s.rpc("Admin/Info", &json!({}), Some("r3ad"));
    assert_eq!(status, 200, "{info}");
    assert_eq!(info["tables"], json!(["t"]));
    assert_eq!(info["version"], env!("CARGO_PKG_VERSION"));
    assert!(info["path"].as_str().unwrap().ends_with("t.phdb"), "{info}");
    let (status, m) = s.rpc("Admin/Metrics", &json!({}), Some("r3ad"));
    assert_eq!(status, 200);
    assert!(
        m["text"]
            .as_str()
            .unwrap()
            .contains("pigeonhole_serve_requests_total"),
        "{m}"
    );
    for rpc in ["Admin/Flush", "Admin/Compact"] {
        let (status, e) = s.rpc(rpc, &json!({}), Some("r3ad"));
        assert_eq!(
            (status, e["code"].as_str()),
            (403, Some("PermissionDenied")),
            "{rpc}: {e}"
        );
        assert_eq!(s.rpc(rpc, &json!({}), Some("adm1n")).0, 200, "{rpc}");
    }
}

/// The rows of `t` a backup file holds, and whether it has `r1`'s `f:q`.
fn rows_in_backup(path: &std::path::Path) -> (usize, bool) {
    let db = Pigeonhole::open(path, Options::default()).unwrap();
    let t = db.table("t").unwrap().open().unwrap();
    let rows = t
        .scan_prefix(b"")
        .read_within(1 << 30)
        .unwrap()
        .rows()
        .len();
    let r1 = t.get(b"r1", "f", b"q").unwrap().is_some();
    drop(t);
    db.close().unwrap();
    (rows, r1)
}

#[test]
fn a_backup_lands_inside_the_backup_dir_only() {
    let s = Server::start("", limits());
    assert_eq!(
        s.post(
            "Mutate",
            &put("r1", "f", "q", json!({"bytes": b64(b"v")})),
            None
        )
        .0,
        200
    );
    let (status, b) = s.rpc("Admin/Backup", &json!({"path": "one.phdb"}), None);
    assert_eq!(status, 200, "{b}");
    let file = s.dir.join("backups/one.phdb");
    assert_eq!(
        b["bytes"].as_str().unwrap().parse::<u64>().unwrap(),
        std::fs::metadata(&file).unwrap().len()
    );
    assert_eq!(rows_in_backup(&file), (1, true));
    // A subdirectory is made inside --backup-dir.
    assert_eq!(
        s.rpc("Admin/Backup", &json!({"path": "nightly/two.phdb"}), None)
            .0,
        200
    );
    assert_eq!(
        rows_in_backup(&s.dir.join("backups/nightly/two.phdb")),
        (1, true)
    );
    for (path, why) in [
        ("one.phdb", "exists"),
        ("../out.phdb", "escapes"),
        ("/tmp/x.phdb", "absolute"),
    ] {
        let (status, e) = s.rpc("Admin/Backup", &json!({"path": path}), None);
        assert_eq!(
            (status, e["code"].as_str()),
            (400, Some("InvalidArgument")),
            "{why}: {e}"
        );
    }
}

#[test]
fn a_backup_download_streams_resumes_and_checks_out() {
    use sha2::{Digest, Sha256};
    let s = Server::start("", limits());
    for i in 0..50u32 {
        let value = json!({"bytes": b64(&vec![b'v'; 4096])});
        assert_eq!(
            s.post("Mutate", &put(&format!("r{i:02}"), "f", "q", value), None)
                .0,
            200
        );
    }
    let mut admin = s.block_on(async {
        proto::admin_client::AdminClient::new(
            tonic::transport::Channel::from_shared(s.url.clone())
                .unwrap()
                .connect()
                .await
                .unwrap(),
        )
    });
    let chunks =
        |req: proto::BackupDownloadRequest,
         admin: &mut proto::admin_client::AdminClient<tonic::transport::Channel>| {
            s.block_on(async {
                let mut stream = admin.backup_download(req).await?.into_inner();
                let mut out = Vec::new();
                while let Some(c) = stream.message().await? {
                    out.push(c);
                }
                Ok::<_, tonic::Status>(out)
            })
        };
    let all = chunks(Default::default(), &mut admin).unwrap();
    let last = all.last().unwrap();
    let bytes: Vec<u8> = all.iter().flat_map(|c| c.data.to_vec()).collect();
    assert_eq!(last.total, bytes.len() as u64);
    assert_eq!(
        &last.sha256[..],
        &Sha256::digest(&bytes)[..],
        "the last chunk's SHA-256 is the file's"
    );
    assert!(
        all.iter()
            .all(|c| c.data.len() <= 1 << 20 && c.backup_id == last.backup_id)
    );
    let copy = s.dir.join("download.phdb");
    std::fs::write(&copy, &bytes).unwrap();
    assert_eq!(rows_in_backup(&copy).0, 50);
    // A resume from an offset gives the rest of the same file.
    let half = bytes.len() as u64 / 2;
    let rest = chunks(
        proto::BackupDownloadRequest {
            backup_id: last.backup_id.clone(),
            offset: half,
            ..Default::default()
        },
        &mut admin,
    )
    .unwrap();
    assert_eq!(rest[0].offset, half);
    let tail: Vec<u8> = rest.iter().flat_map(|c| c.data.to_vec()).collect();
    assert_eq!(tail, bytes[half as usize..]);
    // An id the server doesn't keep is NotFound.
    let e = chunks(
        proto::BackupDownloadRequest {
            backup_id: "0".repeat(32),
            ..Default::default()
        },
        &mut admin,
    )
    .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound, "{e:?}");
    // Over HTTP: the bytes, with the id and SHA-256 as headers; Range resumes, 206.
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    let mut resp = agent
        .post(format!("{}/v1/Admin/BackupDownload", s.url))
        .send_json(json!({}))
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/octet-stream"
    );
    let id = resp
        .headers()
        .get("x-pigeonhole-backup-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let sha = resp
        .headers()
        .get("x-pigeonhole-backup-sha256")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let body = resp
        .body_mut()
        .with_config()
        .limit(u64::MAX)
        .read_to_vec()
        .unwrap();
    let hex: String = Sha256::digest(&body)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(hex, sha);
    let mut resp = agent
        .post(format!("{}/v1/Admin/BackupDownload?backup_id={id}", s.url))
        .header("Range", "bytes=100-")
        .send_json(json!({}))
        .unwrap();
    assert_eq!(resp.status().as_u16(), 206);
    let tail = resp
        .body_mut()
        .with_config()
        .limit(u64::MAX)
        .read_to_vec()
        .unwrap();
    assert_eq!(tail, body[100..]);
}

#[test]
fn racing_creates_and_drops_leave_the_cache_on_the_live_table() {
    // simp724's C1 on #1527: a create whose cache update ran after a drop and a newer create
    // could leave the name on the dropped table. Creates and drops are now serialized with
    // their cache updates (`Service::schema`), so whatever the order, a table that exists
    // takes writes. A smoke test only: the race's window (between a create's blocking call
    // and its cache update) is too small to hit without a hold, so it passes without the
    // lock too. The fix holds by construction.
    let s = Server::start("", limits());
    let service = &s.shared.router.service;
    let caller = Caller::anonymous();
    let create = || {
        ServiceOp::CreateTable(proto::CreateTableRequest {
            name: "c".into(),
            families: vec![proto::FamilySpec {
                name: "f".into(),
                ..Default::default()
            }],
            if_missing: true,
        })
    };
    let drop_c = || ServiceOp::DropTable(proto::DropTableRequest { name: "c".into() });
    let put_c = || {
        ServiceOp::Mutate(proto::MutateRequest {
            writes: serde_json::from_value(json!({"row": {"table": "c", "row": b64(b"r")},
                "writes": [{"put": {"family": "f", "qualifier": b64(b"q"), "value": {"bytes": b64(b"v")}}}]}))
            .unwrap(),
            durability: 0,
        })
    };
    for round in 0..30 {
        s.block_on(async {
            let (a, b, c) = tokio::join!(
                service.call(create(), &caller, None),
                service.call(drop_c(), &caller, None),
                service.call(create(), &caller, None),
            );
            // A drop of a table not there yet fails; the creates always succeed.
            assert!(
                a.is_ok() && c.is_ok(),
                "round {round}: {:?} {:?}",
                a.err(),
                c.err()
            );
            let _ = b;
        });
        if service.db.tables().iter().any(|t| t == "c") {
            let put = s.block_on(service.call(put_c(), &caller, None));
            assert!(
                put.is_ok(),
                "round {round}: the cache is on a dropped table: {:?}",
                put.err()
            );
        }
    }
}

// ---- Replication (#1436) ----

/// Waits until the replica's `Admin.Info` replica section passes `ok`, and returns it.
fn wait_replica(r: &Server, what: &str, ok: impl Fn(&Json) -> bool) -> Json {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let (status, info) = r.rpc("Admin/Info", &json!({}), None);
        assert_eq!(status, 200, "{info}");
        if ok(&info["replica"]) {
            return info["replica"].clone();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the replica never got {what}: {info}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The replica has applied everything the primary has.
fn caught_up(primary: &Server, token: Option<&str>) -> impl Fn(&Json) -> bool {
    let (_, info) = primary.rpc("Admin/Info", &json!({}), token);
    assert!(
        info.get("replica").is_none(),
        "a primary has no replica section: {info}"
    );
    let seqno = primary.shared.router.service.db.visible_seqno();
    move |r: &Json| {
        r["state"] == "following"
            && r["applied_primary_seqno"]
                .as_str()
                .and_then(|s| s.parse::<u64>().ok())
                .is_some_and(|a| a >= seqno)
            && r.get("lag_ms").is_none()
    }
}

fn get_value(s: &Server, table: &str, row: &str, token: Option<&str>) -> Json {
    let (status, body) = s.post(
        "Get",
        &json!({"table": table, "row": b64(row.as_bytes()), "family": "f", "qualifier": b64(b"q")}),
        token,
    );
    assert_eq!(status, 200, "{body}");
    body["cell"]["value"].clone()
}

#[test]
fn a_replica_bootstraps_follows_and_refuses_writes() {
    let p = Server::start("admin s3cret", limits());
    let tok = Some("s3cret");
    for i in 0..20 {
        let value = json!({"bytes": b64(format!("v{i}").as_bytes())});
        assert_eq!(
            p.post("Mutate", &put(&format!("r{i}"), "f", "q", value), tok)
                .0,
            200
        );
    }
    let r = Server::start_replica(test_dir(), &p.url, Some("s3cret"));
    wait_replica(&r, "caught up after its bootstrap", caught_up(&p, tok));
    assert_eq!(get_value(&r, "t", "r7", None), b64(b"v7"));

    // Commits after the bootstrap follow, a new table and its writes in commit order.
    for i in 20..40 {
        let value = json!({"bytes": b64(format!("v{i}").as_bytes())});
        assert_eq!(
            p.post("Mutate", &put(&format!("r{i}"), "f", "q", value), tok)
                .0,
            200
        );
    }
    let (status, body) = p.rpc(
        "Schema/CreateTable",
        &json!({"name": "u", "families": [{"name": "f"}]}),
        tok,
    );
    assert_eq!(status, 200, "{body}");
    let mut w = put("x", "f", "q", json!({"bytes": b64(b"in u")}));
    w["writes"]["row"]["table"] = json!("u");
    assert_eq!(p.post("Mutate", &w, tok).0, 200);
    // The primary's durable feed: a sync makes them durable at once.
    assert_eq!(p.rpc("Admin/Flush", &json!({}), tok).0, 200);
    let info = wait_replica(&r, "the later commits", caught_up(&p, tok));
    assert_eq!(info["primary"], p.url);
    assert_eq!(get_value(&r, "t", "r39", None), b64(b"v39"));
    assert_eq!(get_value(&r, "u", "x", None), b64(b"in u"));
    // A table dropped and made again on the primary: the replica's reads (whose handle for
    // the name was cached) see the new one.
    assert_eq!(p.rpc("Schema/DropTable", &json!({"name": "u"}), tok).0, 200);
    let (status, body) = p.rpc(
        "Schema/CreateTable",
        &json!({"name": "u", "families": [{"name": "f"}]}),
        tok,
    );
    assert_eq!(status, 200, "{body}");
    let mut w = put("x2", "f", "q", json!({"bytes": b64(b"in the new u")}));
    w["writes"]["row"]["table"] = json!("u");
    assert_eq!(p.post("Mutate", &w, tok).0, 200);
    assert_eq!(p.rpc("Admin/Flush", &json!({}), tok).0, 200);
    wait_replica(&r, "the new table", caught_up(&p, tok));
    assert_eq!(get_value(&r, "u", "x2", None), b64(b"in the new u"));
    assert_eq!(get_value(&r, "u", "x", None), Json::Null);

    // Writes and schema changes are refused, naming the primary.
    let (status, body) = r.post(
        "Mutate",
        &put("y", "f", "q", json!({"bytes": b64(b"no")})),
        None,
    );
    assert_eq!(
        (status, body["code"].as_str()),
        (409, Some("ReadOnly")),
        "{body}"
    );
    assert!(body["message"].as_str().unwrap().contains(&p.url), "{body}");
    let (status, body) = r.rpc("Schema/DropTable", &json!({"name": "u"}), None);
    assert_eq!(
        (status, body["code"].as_str()),
        (409, Some("ReadOnly")),
        "{body}"
    );
    // A transaction's Commit with writes reaches the engine's gate, and says the same.
    let id = begin(&r, None);
    let (status, body) = r.rpc(
        "Transactions/Commit",
        &json!({"txn_id": id, "rows": txn_rows("z", b"no")}),
        None,
    );
    assert_eq!(status, 409, "{body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("replica"),
        "{body}"
    );

    // The lag gauges.
    let (_, m) = r.rpc("Admin/Metrics", &json!({}), None);
    let text = m["text"].as_str().unwrap();
    assert!(text.contains("pigeonhole_replica_following 1"), "{text}");
    assert!(text.contains("pigeonhole_replica_lag_commits"), "{text}");

    // Every write RPC, whatever its body: the gate is the call's scope (cli692's F2).
    for rpc in [
        "Data/Mutate",
        "Data/CheckAndMutate",
        "Data/Batch",
        "Data/MutateReturning",
        "Transactions/Write",
        "Schema/CreateTable",
        "Schema/DropTable",
        "Vectors/CreateIndex",
        "Vectors/Insert",
        "Vectors/Delete",
        "Vectors/BulkLoad",
        "Text/CreateIndex",
        "Text/Put",
        "Text/Delete",
        "Text/Rebuild",
    ] {
        let (status, body) = r.rpc(rpc, &json!({}), None);
        assert_eq!(
            (status, body["code"].as_str()),
            (409, Some("ReadOnly")),
            "{rpc}: {body}"
        );
    }

    // Caught up means no lag: the primary reports its durable frontier, which a replica can
    // reach, not its visible seqno (cli692's F5).
    let info = wait_replica(&r, "caught up again", caught_up(&p, tok));
    assert!(info.get("lag_commits").is_none(), "{info}");

    // A replica is no bootstrap source and can't be followed (no cascades).
    let (status, body) = r.rpc("Admin/BackupDownload", &json!({"replica": true}), None);
    assert_eq!(
        (status, body["code"].as_str()),
        (409, Some("FailedPrecondition")),
        "{body}"
    );
}

#[test]
fn a_restarted_replica_resumes_and_a_new_primary_bootstraps_it_again() {
    let p = Server::start("", limits());
    for i in 0..10 {
        let value = json!({"bytes": b64(format!("a{i}").as_bytes())});
        assert_eq!(
            p.post("Mutate", &put(&format!("r{i}"), "f", "q", value), None)
                .0,
            200
        );
    }
    let dir = test_dir();
    let mut r = Server::start_replica(dir.clone(), &p.url, None);
    wait_replica(&r, "caught up", caught_up(&p, None));
    r.keep_dir = true;
    drop(r);

    // Restarted, it resumes from its stored token: no second bootstrap, and what the primary
    // wrote meanwhile arrives.
    for i in 10..20 {
        let value = json!({"bytes": b64(format!("a{i}").as_bytes())});
        assert_eq!(
            p.post("Mutate", &put(&format!("r{i}"), "f", "q", value), None)
                .0,
            200
        );
    }
    assert_eq!(p.rpc("Admin/Flush", &json!({}), None).0, 200);
    let downloads = || std::fs::read_dir(p.dir.join("backups")).map_or(0, |d| d.count());
    let before = downloads();
    let mut r = Server::start_replica(dir.clone(), &p.url, None);
    wait_replica(&r, "caught up after its restart", caught_up(&p, None));
    assert_eq!(get_value(&r, "t", "r15", None), b64(b"a15"));
    assert_eq!(downloads(), before, "a restart takes no bootstrap copy");
    r.keep_dir = true;
    drop(r);

    // Another database at a URL: the replica's token is another database's, so it
    // bootstraps again from the new one rather than apply its history.
    let q = Server::start("", limits());
    let value = json!({"bytes": b64(b"from q")});
    assert_eq!(q.post("Mutate", &put("q1", "f", "q", value), None).0, 200);
    let r = Server::start_replica(dir, &q.url, None);
    wait_replica(&r, "caught up with the new primary", caught_up(&q, None));
    assert_eq!(get_value(&r, "t", "q1", None), b64(b"from q"));
    assert_eq!(
        get_value(&r, "t", "r15", None),
        Json::Null,
        "the old copy is gone"
    );
}

#[test]
fn promote_makes_a_replica_take_writes() {
    let p = Server::start("", limits());
    let value = json!({"bytes": b64(b"v")});
    assert_eq!(p.post("Mutate", &put("r1", "f", "q", value), None).0, 200);
    // On a primary, Promote is refused.
    let (status, body) = p.rpc("Admin/Promote", &json!({}), None);
    assert_eq!(
        (status, body["code"].as_str()),
        (409, Some("FailedPrecondition")),
        "{body}"
    );

    let r = Server::start_replica(test_dir(), &p.url, None);
    wait_replica(&r, "caught up", caught_up(&p, None));
    let (status, body) = r.rpc("Admin/Promote", &json!({}), None);
    assert_eq!(status, 200, "{body}");
    assert!(
        body["primary_token"]
            .as_str()
            .is_some_and(|t| !t.is_empty()),
        "{body}"
    );
    // A primary now: writes are taken, and Info has no replica section.
    let value = json!({"bytes": b64(b"mine")});
    assert_eq!(r.post("Mutate", &put("r2", "f", "q", value), None).0, 200);
    assert_eq!(get_value(&r, "t", "r2", None), b64(b"mine"));
    let (_, info) = r.rpc("Admin/Info", &json!({}), None);
    assert!(info.get("replica").is_none(), "{info}");
    // It follows no more: the old primary's writes don't arrive.
    let value = json!({"bytes": b64(b"late")});
    assert_eq!(p.post("Mutate", &put("r3", "f", "q", value), None).0, 200);
    assert_eq!(p.rpc("Admin/Flush", &json!({}), None).0, 200);
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(get_value(&r, "t", "r3", None), Json::Null);
    let (status, _) = r.rpc("Admin/Promote", &json!({}), None);
    assert_eq!(status, 409, "promoted once");
}

/// An IVF index made and written on the primary is searched and verified on a replica, and
/// the replica refuses its writes like any other (#1436 with #1573's index RPCs).
#[test]
#[cfg_attr(
    windows,
    ignore = "#1595: the bootstrap download hangs intermittently on Windows"
)]
fn a_replica_serves_ivf_index_reads_and_refuses_its_writes() {
    let p = Server::start("", limits());
    for (name, mode) in [("ix", "REBALANCE_INLINE"), ("bg", "REBALANCE_BACKGROUND")] {
        let mut spec = index_spec(mode);
        spec["name"] = json!(name);
        let (status, e) = p.rpc("Vectors/CreateIndex", &spec, None);
        assert_eq!(status, 200, "{e}");
        for (id, y) in [("a", 1.0f32), ("b", 2.0), ("c", 3.0)] {
            let (status, e) = p.rpc(
                "Vectors/Insert",
                &json!({"index": name, "id": b64(id.as_bytes()), "vector": f32_query(&[0.0, y])}),
                None,
            );
            assert_eq!(status, 200, "{id}: {e}");
        }
    }
    let r = Server::start_replica(test_dir(), &p.url, None);
    wait_replica(&r, "caught up", caught_up(&p, None));
    for name in ["ix", "bg"] {
        let q = f32_query(&[0.0, 0.0]);
        let (status, hits) = r.rpc(
            "Vectors/Search",
            &json!({"index": name, "query": q, "k": 2}),
            None,
        );
        assert_eq!(status, 200, "{name}: {hits}");
        assert_eq!(hit_ids(&hits), [b64(b"a"), b64(b"b")], "{name}");
        let (status, v) = r.rpc("Vectors/Verify", &json!({"index": name}), None);
        assert_eq!(status, 200, "{name}: {v}");
        for (rpc, body) in [
            (
                "Vectors/Insert",
                json!({"index": name, "id": b64(b"z"), "vector": f32_query(&[1.0, 1.0])}),
            ),
            ("Vectors/Delete", json!({"index": name, "id": b64(b"a")})),
        ] {
            let (status, e) = r.rpc(rpc, &body, None);
            assert_eq!(
                (status, e["code"].as_str()),
                (409, Some("ReadOnly")),
                "{rpc}: {e}"
            );
        }
    }
    let (status, e) = r.rpc("Vectors/CreateIndex", &index_spec("REBALANCE_INLINE"), None);
    assert_eq!((status, e["code"].as_str()), (409, Some("ReadOnly")), "{e}");
    // Writes on the primary after the index was opened on the replica still arrive.
    let (status, e) = p.rpc(
        "Vectors/Insert",
        &json!({"index": "ix", "id": b64(b"n"), "vector": f32_query(&[0.0, 0.5])}),
        None,
    );
    assert_eq!(status, 200, "{e}");
    assert_eq!(p.rpc("Admin/Flush", &json!({}), None).0, 200);
    wait_replica(&r, "the later insert", caught_up(&p, None));
    let q = f32_query(&[0.0, 0.0]);
    let (_, hits) = r.rpc(
        "Vectors/Search",
        &json!({"index": "ix", "query": q, "k": 1}),
        None,
    );
    assert_eq!(hit_ids(&hits), [b64(b"n")]);
}

/// Two `Admin.Promote` calls at once: one promotes, the other is refused (cli692's F4). A
/// background index rebalances on a replica only once it's promoted, and the catalog
/// changes that drop the server's index handles don't pile up rebalancer threads (F3).
#[test]
fn concurrent_promotes_promote_once_and_rebalancers_start_after() {
    let p = Server::start("", limits());
    let mut spec = index_spec("REBALANCE_BACKGROUND");
    spec["name"] = json!("bg");
    assert_eq!(p.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    let (status, e) = p.rpc(
        "Vectors/Insert",
        &json!({"index": "bg", "id": b64(b"a"), "vector": f32_query(&[0.0, 1.0])}),
        None,
    );
    assert_eq!(status, 200, "{e}");
    let r = Server::start_replica(test_dir(), &p.url, None);
    wait_replica(&r, "caught up", caught_up(&p, None));
    let search = |s: &Server| {
        let q = f32_query(&[0.0, 0.0]);
        let (status, hits) = s.rpc(
            "Vectors/Search",
            &json!({"index": "bg", "query": q, "k": 1}),
            None,
        );
        assert_eq!(status, 200, "{hits}");
        assert_eq!(hit_ids(&hits), [b64(b"a")]);
    };
    let indexes = &r.shared.router.service.indexes;
    search(&r);
    assert_eq!(indexes.rebalancing(), 0, "no rebalancer on a replica");

    let statuses: Vec<(u16, Json)> = std::thread::scope(|scope| {
        let calls: Vec<_> = (0..2)
            .map(|_| scope.spawn(|| r.rpc("Admin/Promote", &json!({}), None)))
            .collect();
        calls.into_iter().map(|c| c.join().unwrap()).collect()
    });
    let ok = statuses.iter().filter(|(s, _)| *s == 200).count();
    assert_eq!(ok, 1, "exactly one promote: {statuses:?}");
    let refused = statuses.iter().find(|(s, _)| *s != 200).unwrap();
    assert_eq!(
        (refused.0, refused.1["code"].as_str()),
        (409, Some("FailedPrecondition")),
        "{refused:?}"
    );

    // A primary now: the index's rebalancer runs. Catalog changes forget the handles, and
    // each next search reopens it; the forgotten rebalancers end, so one is left.
    search(&r);
    assert_eq!(indexes.rebalancing(), 1);
    for _ in 0..3 {
        r.shared.router.service.tables_changed();
        search(&r);
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while indexes.rebalancing() > 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "{} rebalancers after three forgets",
            indexes.rebalancing()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A replica's own feed subscribers when a bootstrap swaps a new copy in: their tokens are
/// the old copy's database's, and they end with an error rather than silently starting
/// over on the new one (cli692's N3). Driven as the replica's loop does it, on one server.
#[test]
fn a_swap_ends_the_old_copy_s_feed_streams_with_an_error() {
    let s = Server::start("", limits());
    let other = Server::start("", limits());
    write_row(&other, b"theirs");
    let db = &s.shared.router.service.db;
    let side = s.dir.join("side.phdb");
    {
        let theirs = &other.shared.router.service.db;
        let snap = theirs.durable_snapshot().unwrap();
        theirs.backup_at(&snap, &side).unwrap();
    }
    let mut events = direct_feed(&s, subscribe(&["t"], b""));
    write_row(&s, b"ours");
    let got = drain(&s, &mut events, Duration::from_millis(300));
    assert_eq!(got.len(), 1, "{got:?}");
    let token = got[0].2.clone();

    db.replace_with(&side).unwrap();
    s.shared.router.service.engine_replaced();

    // The open stream: an error, never an event of the new copy.
    let mut ended = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while ended.is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "the stream never ended"
        );
        match next_event(&s, &mut events, Duration::from_millis(200)) {
            Some(Ok(e)) => assert!(commit_of(&e).is_none(), "an event after the swap: {e:?}"),
            Some(Err(e)) => ended = Some(e),
            None => {}
        }
    }
    let ended = ended.unwrap();
    // A resubscribe from the old token is refused the same way.
    let resumed = s.block_on(s.shared.router.service.call(
        ServiceOp::Subscribe(proto::SubscribeRequest {
            start: Some(proto::subscribe_request::Start::Resume(token.into())),
            ..subscribe(&["t"], b"")
        }),
        &Caller::anonymous(),
        None,
    ));
    let refused = match resumed {
        Err(e) => e,
        Ok(super::service::ServiceReply::Feed(mut events)) => {
            match next_event(&s, &mut events, Duration::from_secs(5)) {
                Some(Err(e)) => e,
                other => panic!("a resume from the old copy's token: {other:?}"),
            }
        }
        Ok(_) => unreachable!(),
    };
    eprintln!(
        "ended: {} {}; resume: {} {}",
        ended.code, ended.message, refused.code, refused.message
    );
    for e in [&ended, &refused] {
        assert!(
            ["FeedExpired", "InvalidArgument"].contains(&e.code.as_str()),
            "{}: {}",
            e.code,
            e.message
        );
    }
}
