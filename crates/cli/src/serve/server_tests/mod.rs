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

// The serve modules the area files reach as `super::<module>`.
use super::{bulk, cluster, errors, feed, replica, service, tls};

mod admin_tests;
mod charge_tests;
mod cluster_tests;
mod connection_tests;
mod data_tests;
mod feed_tests;
mod http1_tests;
mod mcp_tests;
mod memory_tests;
mod replica_tests;
mod session_tests;
mod text_tests;
mod vector_tests;

thread_local! {
    /// The blocking pool of the servers this thread starts (`--blocking-threads`).
    static BLOCKING_THREADS: std::cell::Cell<usize> = const { std::cell::Cell::new(2) };
}

/// The servers this thread starts get a blocking pool of `n` threads.
fn blocking_threads(n: usize) {
    BLOCKING_THREADS.with(|b| b.set(n));
}

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

pub(super) fn limits() -> Limits {
    Limits {
        max_request_bytes: 1 << 20,
        inflight_bytes: 16 << 20,
        max_concurrent: 64,
        max_connections: 64,
        max_scans: 4,
        max_streams: 16,
        max_feed_hubs: 8,
        max_catch_up: 4,
        fanout_forwards: 8,
        feed_ring_bytes: 1 << 20,
        bulk_load_bytes: 1 << 30,
        feed_quiet: Duration::from_secs(10),
        feed_client_events: super::feed::CLIENT_EVENTS,
        feed_client_bytes: super::feed::CLIENT_BYTES,
        request_timeout: Duration::from_secs(5),
        stream_idle: Duration::from_secs(5),
        hold_after_part_page: false,
        leases: super::sessions::Leases {
            txn_idle: Duration::from_secs(30),
            txn_lifetime: Duration::from_secs(300),
            snapshot_idle: Duration::from_secs(60),
            snapshot_lifetime: Duration::from_secs(600),
            max_sessions: 64,
            max_per_token: 16,
            mcp_idle: Duration::from_secs(60),
            mcp_lifetime: Duration::from_secs(600),
        },
        mcp: crate::mcp::Limits {
            result_bytes: 32 << 10,
            value_bytes: 4 << 10,
            feed_bytes: None,
            cache_bytes: 8 << 20,
        },
        mcp_session_requests: 8,
    }
}

/// Where a test server listens.
#[derive(Clone, Copy)]
enum Bind {
    /// `127.0.0.1`, as every other server and endpoint in these tests.
    Shared,
    /// `[::1]`, which no other test dials, else `127.0.0.2`, else `127.0.0.1`.
    /// Tests in this process dial ports that were theirs and are free again: a replica
    /// reconnecting to a stopped primary, a cluster peer, an endpoint bound and dropped to
    /// be refused. A port the OS hands out again can bring such a connection to a new
    /// server; one that counts its connections then counts it (#1668).
    Apart,
}

impl Bind {
    async fn listener(self) -> tokio::net::TcpListener {
        if let Bind::Apart = self {
            // `127.0.0.2` where IPv6 loopback is missing: Linux and Windows route all of
            // 127/8 (macOS always has `::1`). Only past both is it the shared address
            // (cli692's N1 on #1669).
            for addr in ["[::1]:0", "127.0.0.2:0"] {
                if let Ok(l) = tokio::net::TcpListener::bind(addr).await {
                    return l;
                }
            }
            eprintln!("no loopback address apart from 127.0.0.1: listening there (#1668)");
        }
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap()
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

    /// A server whose backup downloads' copies are slow (#1611): opening a copy's file takes
    /// `delay`, through a filesystem the test wraps around the real one ([`SlowCopies`]),
    /// and then fails if `fail`.
    fn start_slow_copies(delay: Duration, fail: bool) -> Self {
        let dir = test_dir();
        let vfs: pigeonhole_io::VfsRef = Arc::new(SlowCopies {
            inner: pigeonhole_io::pread::PreadVfs::new(0),
            delay,
            fail,
        });
        let db = Pigeonhole::open(
            dir.join("t.phdb"),
            Options::default()
                .create_if_missing(true)
                .shards(1)
                .change_feed(pigeonhole::FeedRetention::bytes(64 << 20))
                .vfs(vfs),
        )
        .unwrap();
        db.table("t")
            .unwrap()
            .family("f", Family::default())
            .create()
            .unwrap();
        Self::serve(dir, db, "", limits(), None, |s| s)
    }

    /// `start_with`, its service changed by `f` before it serves.
    fn start_custom(
        tokens: &str,
        limits: Limits,
        shards: usize,
        f: impl FnOnce(Service) -> Service,
    ) -> Self {
        Self::start_at(Bind::Shared, tokens, limits, shards, f)
    }

    /// [`start`](Self::start) on a loopback address no other test dials, where there is
    /// one: for a test that counts the connections the server accepts (#1668).
    fn start_apart(limits: Limits) -> Self {
        Self::start_at(Bind::Apart, "", limits, 1, |s| s)
    }

    fn start_at(
        bind: Bind,
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
        Self::serve_at(bind, dir, db, tokens, limits, None, f)
    }

    /// A replica server (#1436) of the server at `primary`, on `dir`'s `t.phdb` (an empty
    /// database if there is none), sending `token` to the primary.
    fn start_replica(dir: PathBuf, primary: &str, token: Option<&str>) -> Self {
        Self::start_replica_with(dir, primary, token, limits())
    }

    /// `start_replica` with its own limits.
    fn start_replica_with(
        dir: PathBuf,
        primary: &str,
        token: Option<&str>,
        limits: Limits,
    ) -> Self {
        Self::start_replica_custom(dir, primary, token, limits, Options::default(), |s| s)
    }

    /// `start_replica_with`, its database opened with `options` and its service changed by
    /// `f` before it serves.
    fn start_replica_custom(
        dir: PathBuf,
        primary: &str,
        token: Option<&str>,
        limits: Limits,
        options: Options,
        f: impl FnOnce(Service) -> Service,
    ) -> Self {
        std::fs::create_dir_all(dir.join("backups")).unwrap();
        let db = Pigeonhole::open(dir.join("t.phdb"), options.create_if_missing(true)).unwrap();
        let replica = super::replica::Replica::new(super::replica::ReplicaConfig {
            primary: primary.to_owned(),
            token: token.map(str::to_owned),
            run_events: 4,
        });
        Self::serve(dir, db, "", limits, Some(replica), f)
    }

    fn serve(
        dir: PathBuf,
        db: Pigeonhole,
        tokens: &str,
        limits: Limits,
        replica: Option<Arc<super::replica::Replica>>,
        f: impl FnOnce(Service) -> Service,
    ) -> Self {
        Self::serve_at(Bind::Shared, dir, db, tokens, limits, replica, f)
    }

    fn serve_at(
        bind: Bind,
        dir: PathBuf,
        db: Pigeonhole,
        tokens: &str,
        limits: Limits,
        replica: Option<Arc<super::replica::Replica>>,
        f: impl FnOnce(Service) -> Service,
    ) -> Self {
        let metrics = Arc::new(Metrics::default());
        let threads = BLOCKING_THREADS.with(std::cell::Cell::get);
        let mut service = f(Service::new(db, limits, threads, Arc::clone(&metrics))
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
            feed_runtimes: std::sync::OnceLock::new(),
        });
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let listener = runtime.block_on(bind.listener());
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
            listener::Bounds {
                connections: Arc::clone(&shared.router.service.connections),
                first_byte: limits.request_timeout,
            },
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
        self.rpc_with(path, body, token, &[])
    }

    /// `rpc` with more request headers.
    fn rpc_with(
        &self,
        path: &str,
        body: &Json,
        token: Option<&str>,
        headers: &[(&str, &str)],
    ) -> (u16, Json) {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        let mut req = agent.post(format!("{}/v1/{path}", self.url));
        if let Some(t) = token {
            req = req.header("Authorization", format!("Bearer {t}"));
        }
        for (name, value) in headers {
            req = req.header(*name, *value);
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
        self.shared.router.service.part().unwrap().bulk.close();
        self.shared.router.service.part().unwrap().texts.close();
        let _ = self
            .shared
            .router
            .service
            .part()
            .unwrap()
            .db
            .clone()
            .close();
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

fn wait_for(what: &str, mut ok: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !ok() {
        assert!(std::time::Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

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

fn f32_query(xs: &[f32]) -> Json {
    let data: Vec<u8> = xs.iter().flat_map(|x| x.to_le_bytes()).collect();
    json!({"element": "ELEMENT_F32", "dim": xs.len(), "data": b64(&data)})
}

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

fn verified(s: &Server, index: &str) -> Json {
    let (status, v) = s.rpc("Vectors/Verify", &json!({"index": index}), None);
    assert_eq!(status, 200, "{v}");
    // The JSON mapping leaves a zero count out.
    json!(v["vectors"].as_str().unwrap_or("0"))
}

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
        .part()
        .unwrap()
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

/// Waits until the replica's `Admin.Info` replica section passes `ok`, and returns it.
fn wait_replica(r: &Server, what: &str, mut ok: impl FnMut(&Json) -> bool) -> Json {
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
    let seqno = primary
        .shared
        .router
        .service
        .part()
        .unwrap()
        .db
        .visible_seqno();
    move |r: &Json| {
        r["state"] == "following"
            // A 64-bit field is a string, and absent when 0.
            && r["applied_primary_seqno"]
                .as_str()
                .map_or(Some(0), |s| s.parse::<u64>().ok())
                .is_some_and(|a| a >= seqno)
            && r.get("lag_ms").is_none()
    }
}

/// The real filesystem, but opening a file in the server's download directory (a backup
/// download's copy) first waits `delay`: a copy that takes a while to write (#1611). With
/// `fail`, the open then fails: a copy that fails after its download's first heartbeat.
#[derive(Debug)]
struct SlowCopies {
    inner: pigeonhole_io::VfsRef,
    delay: Duration,
    fail: bool,
}

/// What a failing [`SlowCopies`] says.
const REFUSED: &str = "the test's filesystem refuses the copy";

impl pigeonhole_io::Vfs for SlowCopies {
    fn open(
        &self,
        path: &std::path::Path,
        opts: pigeonhole_io::OpenOptions,
    ) -> pigeonhole_io::Result<pigeonhole_io::FileRef> {
        if path
            .parent()
            .is_some_and(|d| d.ends_with(super::backup::DOWNLOADS))
        {
            std::thread::sleep(self.delay);
            if self.fail {
                return Err(pigeonhole_io::Error::new(
                    pigeonhole_io::ErrorKind::Other,
                    REFUSED,
                ));
            }
        }
        self.inner.open(path, opts)
    }
    fn remove(&self, path: &std::path::Path) -> pigeonhole_io::Result<()> {
        self.inner.remove(path)
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> pigeonhole_io::Result<()> {
        self.inner.rename(from, to)
    }
    fn exists(&self, path: &std::path::Path) -> pigeonhole_io::Result<bool> {
        self.inner.exists(path)
    }
    fn list_dir(&self, dir: &std::path::Path) -> pigeonhole_io::Result<Vec<PathBuf>> {
        self.inner.list_dir(dir)
    }
    fn sync_dir(&self, dir: &std::path::Path) -> pigeonhole_io::Result<()> {
        self.inner.sync_dir(dir)
    }
    fn submit_sync_dir(&self, dir: &std::path::Path) -> pigeonhole_io::Completion<()> {
        self.inner.submit_sync_dir(dir)
    }
    fn open_shared(
        &self,
        name: &str,
        dir: Option<&std::path::Path>,
        len: u64,
        mode: pigeonhole_io::SharedOpen,
    ) -> pigeonhole_io::Result<pigeonhole_io::SharedRegion> {
        self.inner.open_shared(name, dir, len, mode)
    }
    fn remove_shared(
        &self,
        name: &str,
        dir: Option<&std::path::Path>,
    ) -> pigeonhole_io::Result<()> {
        self.inner.remove_shared(name, dir)
    }
    fn now_micros(&self) -> u64 {
        self.inner.now_micros()
    }
    fn monotonic_nanos(&self) -> u64 {
        self.inner.monotonic_nanos()
    }
    fn current_process(&self) -> pigeonhole_io::ProcessId {
        self.inner.current_process()
    }
    fn attach_thread(&self) {
        self.inner.attach_thread();
    }
    fn random_u64(&self) -> u64 {
        self.inner.random_u64()
    }
}
