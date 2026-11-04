//! The reference pattern for sharing one Pigeonhole database across processes and hosts
//! (#1397 step 1, D313). Read it beside the guide's page, `docs/guide/serving.md`.
//!
//! One process owns the database file and serves a small HTTP API. Workers on any host call
//! it. The API is made of **business operations, not a generic database protocol**: each
//! endpoint runs its logic next to the data, with the whole Pigeonhole API in-process
//! (transactions with `transact`'s retry loop, conditional writes, the change feed). The
//! example's operations are a job queue's:
//!
//! - `POST /jobs`, `GET /jobs/{id}` and a paged `GET /jobs?after=` (byte-budgeted scans);
//! - `POST /jobs/{id}/claim`, `…/renew` and `…/complete`: a lease with a fencing token, via
//!   conditional writes on a TTL family;
//! - `POST /accounts/{id}`, `GET /accounts/{id}` and `POST /transfer`: an optimistic
//!   transaction with retry, made idempotent by an operation id;
//! - `GET /events`: the change feed as server-sent events, resumable with `Last-Event-ID`.
//!
//! [`Client`] is the matching HTTP client the worker binary and the tests use.

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use pigeonhole::nonblocking::FeedStream;
use pigeonhole::{
    Condition, ErrorCode, Family, FeedRetention, FeedToken, Options, Pigeonhole, RetryPolicy,
    ScanPosition, Table, ValueFilter,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

mod client;

/// The guide page's code samples (`docs/guide/serving.md`), compiled as doctests of this crate.
#[cfg(doctest)]
#[doc = include_str!("../../../docs/guide/serving.md")]
struct Guide;
pub use client::{Claim, Client, ClientError, Events, JobSummary, Page, StreamedEvent};

/// The server's settings.
#[derive(Debug, Clone)]
pub struct Config {
    /// The database file. It's created if missing.
    pub path: PathBuf,
    /// How long a claim lasts without a renewal: the lease family's TTL.
    pub lease_ttl: Duration,
    /// A bearer token every request must carry, or `None` for none (loopback development
    /// only: put TLS and auth in front of anything else, see the guide).
    pub token: Option<String>,
}

/// The application: the database and its tables, shared by every request.
#[derive(Debug)]
pub struct App {
    db: Pigeonhole,
    jobs: Table,
    /// The `accounts` table.
    ledger: Table,
    ops: Table,
    token: Option<String>,
}

// The schema. `jobs`: `spec:body`, `state:status` ("queued" or "done"), `state:result`, and
// `lease:holder`, whose value is the current claim's fencing token. `accounts`:
// `bal:balance` (an i64). `ops`: one row per applied transfer.
const SPEC: &str = "spec";
const STATE: &str = "state";
const LEASE: &str = "lease";
const BAL: &str = "bal";
const OP: &str = "op";
const BODY: &[u8] = b"body";
const STATUS: &[u8] = b"status";
const RESULT: &[u8] = b"result";
const HOLDER: &[u8] = b"holder";
const BALANCE: &[u8] = b"balance";
const APPLIED: &[u8] = b"applied";
const QUEUED: &[u8] = b"queued";
const DONE: &[u8] = b"done";

/// The largest page `GET /jobs` returns, in D308's bytes.
const MAX_PAGE_BYTES: u64 = 1 << 20;

impl App {
    /// Opens (or creates) the database and its tables. Blocking: call it before serving.
    pub fn open(config: &Config) -> pigeonhole::Result<Arc<Self>> {
        // The change feed backs `GET /events`.
        let db = Pigeonhole::open(
            &config.path,
            Options::default().change_feed(FeedRetention::bytes(64 << 20)),
        )?;
        let jobs = db
            .table("jobs")?
            .family(SPEC, Family::default())
            .family(STATE, Family::default())
            // A claim that isn't renewed expires: its cell reads as absent after the TTL.
            .family(
                LEASE,
                Family::default().max_versions(1).ttl(config.lease_ttl),
            )
            .create_if_missing()?;
        let ledger = db
            .table("accounts")?
            .family(BAL, Family::default())
            .create_if_missing()?;
        let ops = db
            .table("ops")?
            .family(OP, Family::default())
            .create_if_missing()?;
        Ok(Arc::new(Self {
            db,
            jobs,
            ledger,
            ops,
            token: config.token.clone(),
        }))
    }

    /// Closes the database cleanly, once the server has stopped, and reports the close's
    /// error. If something still holds the app (a request still finishing, or a stream),
    /// this returns `Ok` without closing: the database then closes when the last handle
    /// drops, which is not a clean `close()` and reports no error. Stop serving first, as
    /// the server binary does, so this is the last handle.
    pub fn close(self: Arc<Self>) -> pigeonhole::Result<()> {
        match Arc::try_unwrap(self) {
            Ok(app) => app.db.close(),
            Err(_) => Ok(()),
        }
    }
}

/// The HTTP API over `app`.
pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/jobs", post(create_job).get(list_jobs))
        .route("/jobs/{id}", get(get_job))
        .route("/jobs/{id}/claim", post(claim_job))
        .route("/jobs/{id}/renew", post(renew_job))
        .route("/jobs/{id}/complete", post(complete_job))
        .route("/accounts/{id}", post(open_account).get(get_account))
        .route("/transfer", post(transfer))
        .route("/events", get(events))
        .layer(middleware::from_fn_with_state(app.clone(), require_token))
        .with_state(app)
}

/// Serves `app` on `listener` until `shutdown` resolves, then lets in-flight requests finish.
pub async fn serve(
    listener: tokio::net::TcpListener,
    app: Arc<App>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(listener, router(app))
        .with_graceful_shutdown(shutdown)
        .await
}

// ---- Auth ---------------------------------------------------------------------------------

/// The bearer-token check, when a token is configured. A real deployment puts TLS in front
/// (a reverse proxy, or `phdb serve`, which does both); see the guide.
async fn require_token(State(app): State<Arc<App>>, req: Request, next: Next) -> Response {
    if let Some(want) = &app.token {
        let got = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        if !got.is_some_and(|g| constant_time_eq(g.as_bytes(), want.as_bytes())) {
            return ApiError::new(
                StatusCode::UNAUTHORIZED,
                "Unauthorized",
                "a bearer token is required",
            )
            .into_response();
        }
    }
    next.run(req).await
}

/// Compares without an early exit, so the time taken doesn't reveal a matching prefix.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

// ---- Errors -------------------------------------------------------------------------------

/// An error response: an HTTP status and a JSON body with a stable `code`.
#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }
}

impl From<pigeonhole::Error> for ApiError {
    /// The database's errors as HTTP. What matters most is telling a client whether a write
    /// may have committed: `Conflict` and `Busy` didn't, `OutcomeUnknown` may have.
    fn from(e: pigeonhole::Error) -> Self {
        let (status, code) = match e.code() {
            ErrorCode::Conflict => (StatusCode::CONFLICT, "Conflict"),
            ErrorCode::Busy => (StatusCode::SERVICE_UNAVAILABLE, "Busy"),
            ErrorCode::InvalidArgument => (StatusCode::BAD_REQUEST, "InvalidArgument"),
            ErrorCode::OutcomeUnknown => (StatusCode::INTERNAL_SERVER_ERROR, "OutcomeUnknown"),
            ErrorCode::Closed => (StatusCode::SERVICE_UNAVAILABLE, "Closed"),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "Internal"),
        };
        Self::new(status, code, e.message())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut resp = (
            self.status,
            Json(json!({ "code": self.code, "message": self.message })),
        )
            .into_response();
        if self.code == "Busy" {
            resp.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        resp
    }
}

type ApiResult<T> = Result<T, ApiError>;

/// Runs a blocking database call off the async workers. `transact`, `read_within` and the
/// other sync-only calls go through here; commits, gets and row reads use their async forms.
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> ApiResult<T> + Send + 'static,
) -> ApiResult<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal", e.to_string()))?
}

// ---- Jobs ---------------------------------------------------------------------------------

#[derive(Deserialize)]
struct NewJob {
    id: String,
    spec: String,
}

/// `POST /jobs {id, spec}`: queues a job, once. A second create of the same id is 409.
async fn create_job(State(app): State<Arc<App>>, Json(job): Json<NewJob>) -> ApiResult<Response> {
    let absent = Condition::Absent {
        family: STATE.into(),
        qualifier: STATUS.to_vec(),
    };
    let committed = app
        .jobs
        .mutate(job.id.as_bytes())
        .put(SPEC, BODY, job.spec.as_bytes())
        .put(STATE, STATUS, QUEUED)
        .commit_if_async(&absent)
        .await?;
    match committed {
        Some(info) => {
            Ok((StatusCode::CREATED, Json(json!({ "seqno": info.seqno }))).into_response())
        }
        None => Err(ApiError::new(
            StatusCode::CONFLICT,
            "Exists",
            "the job already exists",
        )),
    }
}

/// One job, as the API returns it.
fn job_json(id: &[u8], row: &pigeonhole::Row) -> serde_json::Value {
    let text = |f: &str, q: &[u8]| {
        row.get(f, q)
            .map(|c| String::from_utf8_lossy(c.value()).into_owned())
    };
    json!({
        "id": String::from_utf8_lossy(id),
        "spec": text(SPEC, BODY),
        "status": text(STATE, STATUS),
        "result": text(STATE, RESULT),
        "holder": text(LEASE, HOLDER),
    })
}

/// `GET /jobs/{id}`.
async fn get_job(State(app): State<Arc<App>>, Path(id): Path<String>) -> ApiResult<Response> {
    match app.jobs.row(id.as_bytes()).read_async().await? {
        Some(row) => Ok(Json(job_json(id.as_bytes(), &row)).into_response()),
        None => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "NotFound",
            "no such job",
        )),
    }
}

#[derive(Deserialize)]
struct PageQuery {
    /// The previous page's `next`.
    after: Option<String>,
    /// The page's size in D308's bytes (at most 1 MiB).
    bytes: Option<u64>,
}

/// `GET /jobs?after=&bytes=`: a page of jobs in key order, cut to a byte budget, with the
/// position to continue from. The position is stateless: any server process, now or after a
/// restart, resumes from it.
///
/// A page cut by its budget can end inside a row, and the next page starts with the rest of
/// that row (D308), so the same id can end one page and start the next with other columns.
/// The list is advisory either way: a claim is a conditional write, so acting on a stale or
/// partial entry costs at most a refused claim.
async fn list_jobs(State(app): State<Arc<App>>, Query(q): Query<PageQuery>) -> ApiResult<Response> {
    let after =
        match &q.after {
            Some(hex) => Some(ScanPosition::from_bytes(&from_hex(hex)?).map_err(|e| {
                ApiError::new(StatusCode::BAD_REQUEST, "InvalidArgument", e.message())
            })?),
            None => None,
        };
    let budget = q.bytes.unwrap_or(64 << 10).min(MAX_PAGE_BYTES);
    let jobs = app.jobs.clone();
    blocking(move || {
        let mut scan = jobs.scan_prefix(b"").families([STATE, LEASE]);
        if let Some(p) = &after {
            scan = scan.resume_after(p);
        }
        let page = scan.read_within(budget)?;
        let rows: Vec<_> = page.rows().iter().map(|r| job_json(r.key(), r)).collect();
        let next = page.resume().map(|p| to_hex(&p.to_bytes()));
        Ok(Json(json!({ "jobs": rows, "next": next })).into_response())
    })
    .await
}

#[derive(Deserialize)]
struct ClaimRequest {
    worker: String,
    /// A random id the worker makes for this claim and reuses only to retry it.
    claim_id: String,
}

#[derive(Deserialize)]
struct Fenced {
    fence: String,
    #[serde(default)]
    result: Option<String>,
}

fn holder_is(fence: &str) -> Condition {
    Condition::Value {
        family: LEASE.into(),
        qualifier: HOLDER.to_vec(),
        filter: ValueFilter::Equals(fence.as_bytes().to_vec()),
    }
}

/// `POST /jobs/{id}/claim {worker}`: takes the job's lease, or 409 if someone holds it.
///
/// The claim is a conditional write on the lease column: it lands only if the column is
/// absent, which it is when nobody claimed the job or the last claim expired (the lease
/// family's TTL). The lease's value is a fencing token unique to this claim, `worker/claim_id`,
/// returned to the worker: every later write for the job (renew, complete) is conditioned on
/// the lease still holding that exact token. A worker that stalls past its TTL then can't
/// overwrite the work of whoever claimed the job after it, even though it still thinks it
/// holds the lease.
///
/// The claim id comes from the worker so a claim can be retried (simp724's L1 on #1491): a
/// worker whose claim reply was lost sends the same request again, and gets its own token
/// back if the first attempt landed, or `Claimed` if someone else holds the job. Without
/// it, a lost reply would leave the job held by a lease nobody knows they own, until the
/// TTL.
async fn claim_job(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    Json(req): Json<ClaimRequest>,
) -> ApiResult<Response> {
    if req.claim_id.is_empty() || req.worker.contains('/') {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "a claim needs a claim_id, and a worker name without '/'",
        ));
    }
    let fence = format!("{}/{}", req.worker, req.claim_id);
    let unclaimed = Condition::Absent {
        family: LEASE.into(),
        qualifier: HOLDER.to_vec(),
    };
    let claimed = app
        .jobs
        .mutate(id.as_bytes())
        .put(LEASE, HOLDER, fence.as_bytes())
        .commit_if_async(&unclaimed)
        .await?;
    if claimed.is_none() {
        // Someone holds the lease. If it's this very claim (a retry after the first
        // attempt's reply was lost), the worker gets its token back; anyone else is refused.
        let holder = app.jobs.get_async(id.as_bytes(), LEASE, HOLDER).await?;
        if holder.is_none_or(|c| c.value() != fence.as_bytes()) {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "Claimed",
                "another worker holds the job",
            ));
        }
    }
    // A condition tests one column, so whether the job still needs doing is checked after
    // taking the lease: a done (or missing) job gives the lease straight back.
    let status = app
        .jobs
        .get_async(id.as_bytes(), STATE, STATUS)
        .await?
        .map(|c| c.value().to_vec());
    if status.as_deref() != Some(QUEUED) {
        app.jobs
            .mutate(id.as_bytes())
            .delete_column(LEASE, HOLDER)
            .commit_if_async(&holder_is(&fence))
            .await?;
        return match status {
            Some(_) => Err(ApiError::new(
                StatusCode::CONFLICT,
                "Done",
                "the job is already done",
            )),
            None => Err(ApiError::new(
                StatusCode::NOT_FOUND,
                "NotFound",
                "no such job",
            )),
        };
    }
    Ok(Json(json!({ "fence": fence })).into_response())
}

/// `POST /jobs/{id}/renew {fence}`: extends the claim. A fresh put of the same token, so
/// the TTL counts from now. It fails (409 `Lost`) once the claim has expired, even if
/// nobody else claimed the job: the expired cell is invisible to the condition.
async fn renew_job(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    Json(req): Json<Fenced>,
) -> ApiResult<Response> {
    let renewed = app
        .jobs
        .mutate(id.as_bytes())
        .put(LEASE, HOLDER, req.fence.as_bytes())
        .commit_if_async(&holder_is(&req.fence))
        .await?;
    match renewed {
        Some(_) => Ok(StatusCode::NO_CONTENT.into_response()),
        None => Err(lost()),
    }
}

/// `POST /jobs/{id}/complete {fence, result}`: records the result and releases the lease,
/// only if the lease still holds this claim's token.
async fn complete_job(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    Json(req): Json<Fenced>,
) -> ApiResult<Response> {
    let result = req.result.unwrap_or_default();
    let completed = app
        .jobs
        .mutate(id.as_bytes())
        .put(STATE, STATUS, DONE)
        .put(STATE, RESULT, result.as_bytes())
        .delete_column(LEASE, HOLDER)
        .commit_if_async(&holder_is(&req.fence))
        .await?;
    match completed {
        Some(info) => Ok(Json(json!({ "seqno": info.seqno })).into_response()),
        None => Err(lost()),
    }
}

fn lost() -> ApiError {
    ApiError::new(
        StatusCode::CONFLICT,
        "Lost",
        "the claim expired or another worker took the job",
    )
}

// ---- Accounts and transfers ---------------------------------------------------------------

#[derive(Deserialize)]
struct OpenAccount {
    balance: i64,
}

/// `POST /accounts/{id} {balance}`: opens an account with its first balance, once.
async fn open_account(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    Json(req): Json<OpenAccount>,
) -> ApiResult<Response> {
    let absent = Condition::Absent {
        family: BAL.into(),
        qualifier: BALANCE.to_vec(),
    };
    let opened = app
        .ledger
        .mutate(id.as_bytes())
        .put_i64(BAL, BALANCE, req.balance)
        .commit_if_async(&absent)
        .await?;
    match opened {
        Some(_) => Ok(StatusCode::CREATED.into_response()),
        None => Err(ApiError::new(
            StatusCode::CONFLICT,
            "Exists",
            "the account already exists",
        )),
    }
}

/// `GET /accounts/{id}`.
async fn get_account(State(app): State<Arc<App>>, Path(id): Path<String>) -> ApiResult<Response> {
    match app.ledger.get_async(id.as_bytes(), BAL, BALANCE).await? {
        Some(c) => Ok(Json(json!({ "balance": c.as_i64() })).into_response()),
        None => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "NotFound",
            "no such account",
        )),
    }
}

#[derive(Deserialize)]
struct Transfer {
    /// The client's id for this transfer, the same on every retry of it.
    op_id: String,
    from: String,
    to: String,
    amount: i64,
}

/// Why a transfer's body stopped: the database's error, or the business rule.
enum TransferError {
    Db(pigeonhole::Error),
    NoAccount,
    Insufficient,
}

impl From<pigeonhole::Error> for TransferError {
    fn from(e: pigeonhole::Error) -> Self {
        Self::Db(e)
    }
}

/// `POST /transfer {op_id, from, to, amount}`: moves money between two accounts atomically.
///
/// The body runs in `transact`, which retries it on `Conflict` with backoff: the retry loop
/// lives here, next to the data, not in every client. It's **idempotent by `op_id`**: the
/// same transaction that moves the money records the op id, and a transfer whose op id is
/// already recorded returns `applied: false` without moving anything. A client whose request
/// failed with an unknown outcome (a timeout, a dropped connection, or `OutcomeUnknown`)
/// simply sends the same request again.
async fn transfer(State(app): State<Arc<App>>, Json(t): Json<Transfer>) -> ApiResult<Response> {
    if t.amount <= 0 || t.from == t.to {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "a positive amount between two accounts",
        ));
    }
    let app2 = app.clone();
    blocking(move || {
        let (ledger, ops) = (&app2.ledger, &app2.ops);
        let outcome = app2.db.transact(&RetryPolicy::default(), |txn| {
            if txn.get(ops, t.op_id.as_bytes(), OP, APPLIED)?.is_some() {
                return Ok(false);
            }
            let balance = |txn: &mut pigeonhole::Transaction, who: &str| {
                txn.get(ledger, who.as_bytes(), BAL, BALANCE)
                    .map(|c| c.and_then(|c| c.as_i64()))
            };
            let (Some(from), Some(to)) = (balance(txn, &t.from)?, balance(txn, &t.to)?) else {
                return Err(TransferError::NoAccount);
            };
            if from < t.amount {
                return Err(TransferError::Insufficient);
            }
            txn.put_i64(ledger, t.from.as_bytes(), BAL, BALANCE, from - t.amount)
                .put_i64(ledger, t.to.as_bytes(), BAL, BALANCE, to + t.amount)
                .put(ops, t.op_id.as_bytes(), OP, APPLIED, b"1");
            Ok(true)
        });
        match outcome {
            Ok((applied, _)) => Ok(Json(json!({ "applied": applied })).into_response()),
            Err(TransferError::NoAccount) => Err(ApiError::new(
                StatusCode::NOT_FOUND,
                "NotFound",
                "no such account",
            )),
            Err(TransferError::Insufficient) => Err(ApiError::new(
                StatusCode::CONFLICT,
                "Insufficient",
                "not enough in the account",
            )),
            Err(TransferError::Db(e)) => Err(e.into()),
        }
    })
    .await
}

// ---- Events -------------------------------------------------------------------------------

#[derive(Deserialize)]
struct EventsQuery {
    /// Where to resume, as an earlier event's `id` (`Last-Event-ID` does the same).
    after: Option<String>,
}

/// `GET /events`: every commit to `jobs`, as server-sent events. Each event's `id` is the
/// feed's token after it, so an SSE client that reconnects with `Last-Event-ID` continues
/// exactly after the last event it saw.
async fn events(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<EventsQuery>,
) -> ApiResult<Response> {
    let resume = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .or(q.after);
    let mut builder = app.jobs.feed();
    if let Some(hex) = &resume {
        builder = builder.resume(&FeedToken::from_bytes(from_hex(hex)?));
    }
    let feed = builder.open()?;
    let stream = CommitEvents {
        feed: FeedStream::new(feed),
        done: false,
    };
    Ok(Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response())
}

/// The feed's commits as SSE events. The stream ends after the feed's error (for example
/// `FeedExpired`: the client resumed from a position the history no longer holds), which it
/// sends as one last `error` event.
struct CommitEvents {
    feed: FeedStream,
    done: bool,
}

/// One commit in an event's data.
#[derive(Serialize)]
struct CommitJson {
    seqno: u64,
    changes: Vec<ChangeJson>,
}

#[derive(Serialize)]
struct ChangeJson {
    row: String,
    family: String,
    qualifier: String,
    delete: bool,
}

impl futures_core::Stream for CommitEvents {
    type Item = Result<Event, std::convert::Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.feed).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(commit))) => {
                let data = CommitJson {
                    seqno: commit.seqno(),
                    changes: commit
                        .changes()
                        .iter()
                        .map(|c| ChangeJson {
                            row: String::from_utf8_lossy(c.row()).into_owned(),
                            family: c.family().to_owned(),
                            qualifier: String::from_utf8_lossy(c.qualifier()).into_owned(),
                            delete: c.kind().is_delete(),
                        })
                        .collect(),
                };
                let id = to_hex(self.feed.token().as_bytes());
                let event = Event::default()
                    .id(id)
                    .event("commit")
                    .json_data(&data)
                    .unwrap_or_default();
                Poll::Ready(Some(Ok(event)))
            }
            Poll::Ready(Some(Err(e))) => {
                self.done = true;
                let event = Event::default()
                    .event("error")
                    .json_data(json!({ "code": format!("{:?}", e.code()), "message": e.message() }))
                    .unwrap_or_default();
                Poll::Ready(Some(Ok(event)))
            }
        }
    }
}

// ---- Tokens as text -----------------------------------------------------------------------

/// Positions and feed tokens are bytes; the API carries them as lowercase hex.
fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn from_hex(s: &str) -> ApiResult<Vec<u8>> {
    let bad = || {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "not a position this API gave out",
        )
    };
    if !s.len().is_multiple_of(2) {
        return Err(bad());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            s.get(i..i + 2)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or_else(bad)
        })
        .collect()
}
