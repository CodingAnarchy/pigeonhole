//! The shared service layer (D313 §2): every protocol's calls end here, as one typed
//! operation (`ServiceOp`) answered by one typed reply. gRPC and HTTP/JSON only decode into
//! these and encode out of them, so neither wraps the other.
//!
//! **Writes run detached** (simp724's B2): each commit is spawned as its own task, and the
//! request awaits its outcome. A client that goes away loses only the reply; the commit lands
//! or fails as it would have, and a lost reply is logged and counted.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use pigeonhole::{KeySet, KeysPosition, Pigeonhole, RowPosition, ScanPosition, Table};
use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use super::auth::{Caller, Scope};
use super::config::Limits;
use super::convert;
use super::errors::ApiError;
use super::proto;
use super::sessions::{SnapshotGuard, Taken};

/// A page's size when a request leaves it at 0 (D313 §4: 1 MiB), and the cap on a request's
/// own.
const DEFAULT_PAGE_BYTES: u64 = 1 << 20;
const MAX_PAGE_BYTES: u64 = 8 << 20;

/// One call, decoded. It holds its request by value: routing moves it, never copies it
/// (perf287 on D317's seam).
#[derive(Debug)]
pub(crate) enum ServiceOp {
    Get(proto::GetRequest),
    MultiGet(proto::MultiGetRequest),
    ReadRow(proto::ReadRowRequest),
    Mutate(proto::MutateRequest),
    CheckAndMutate(proto::CheckAndMutateRequest),
    Batch(proto::BatchRequest),
    MutateReturning(proto::MutateRequest),
    Scan(proto::ScanRequest),
    Begin(proto::BeginRequest),
    TxnGet(proto::TxnGetRequest),
    TxnGetRow(proto::TxnGetRowRequest),
    TxnWrite(proto::TxnWriteRequest),
    Commit(proto::TxnCommitRequest),
    Abort(proto::TxnAbortRequest),
    OpenSnapshot(proto::OpenSnapshotRequest),
    ReleaseSnapshot(proto::ReleaseSnapshotRequest),
    Subscribe(proto::SubscribeRequest),
    ListTables(proto::ListTablesRequest),
    CreateTable(proto::CreateTableRequest),
    DropTable(proto::DropTableRequest),
    DescribeTable(proto::DescribeTableRequest),
    Info(proto::Empty),
    Flush(proto::Empty),
    Compact(proto::Empty),
    Backup(proto::BackupRequest),
    BackupDownload(proto::BackupDownloadRequest),
    Metrics(proto::Empty),
    Traverse(proto::TraverseRequest),
    Nearest(proto::NearestRequest),
}

/// One call's answer. Replies are moved once, from the service to the encoder, so the
/// Get variant's inline cell isn't boxed: boxing would cost an allocation on every Get to
/// shrink an enum nobody stores (perf287 on D317's seam).
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum ServiceReply {
    Get(proto::GetResponse),
    MultiGet(proto::MultiGetResponse),
    ReadRow(proto::ReadRowResponse),
    Mutate(proto::MutateResponse),
    CheckAndMutate(proto::CheckAndMutateResponse),
    MutateReturning(proto::MutateReturningResponse),
    Scan(ScanPages),
    Begin(proto::BeginResponse),
    Commit(proto::TxnCommitResponse),
    OpenSnapshot(proto::OpenSnapshotResponse),
    Empty(proto::Empty),
    Feed(super::feed::FeedEvents),
    Tables(proto::ListTablesResponse),
    Table(proto::TableResponse),
    Info(proto::InfoResponse),
    Backup(proto::BackupResponse),
    Download(super::backup::Download),
    Metrics(proto::MetricsResponse),
    Traverse(proto::TraverseResponse),
    Nearest(proto::NearestResponse),
}

/// A page waiting for its client, with its share of the in-flight budget (perf287's G1 on
/// #1510). The scan task keeps a handle on it, so a client that stops reading loses the page
/// and the budget comes back (G2): the page is taken out of the slot by whichever comes first,
/// the client or the task's idle timeout.
type PageSlot = Arc<std::sync::Mutex<Option<(proto::ScanPage, OwnedSemaphorePermit)>>>;

/// A scan's pages as the scan task reads them, then its error if it failed. The task reads a
/// page ahead at most, so a slow client holds back the reading, not the memory. A page's
/// budget is released when the page is handed to the transport (this stream yields it).
#[derive(Debug)]
pub(crate) struct ScanPages {
    pages: mpsc::Receiver<PageSlot>,
    /// The error the stream ends with, sent before `pages` closes.
    end: Option<oneshot::Receiver<ApiError>>,
}

impl futures_core::Stream for ScanPages {
    type Item = Result<proto::ScanPage, ApiError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.pages.poll_recv(cx) {
                Poll::Ready(Some(slot)) => {
                    let taken = slot.lock().ok().and_then(|mut s| s.take());
                    if let Some((page, budget)) = taken {
                        drop(budget);
                        return Poll::Ready(Some(Ok(page)));
                    }
                    // The task took it back (idle timeout): its error follows.
                }
                Poll::Ready(None) => break,
                Poll::Pending => return Poll::Pending,
            }
        }
        let Some(end) = self.end.as_mut() else {
            return Poll::Ready(None);
        };
        let out = match Pin::new(end).poll(cx) {
            Poll::Ready(Ok(e)) => Some(Err(e)),
            Poll::Ready(Err(_)) => None,
            Poll::Pending => return Poll::Pending,
        };
        self.end = None;
        Poll::Ready(out)
    }
}

impl ServiceOp {
    /// The RPC's index in `RPCS`.
    pub(crate) fn index(&self) -> usize {
        match self {
            Self::Get(_) => 0,
            Self::MultiGet(_) => 1,
            Self::ReadRow(_) => 2,
            Self::Mutate(_) => 3,
            Self::CheckAndMutate(_) => 4,
            Self::Batch(_) => 5,
            Self::MutateReturning(_) => 6,
            Self::Scan(_) => SCAN,
            Self::Begin(_) => 8,
            Self::TxnGet(_) => 9,
            Self::TxnGetRow(_) => 10,
            Self::TxnWrite(_) => 11,
            Self::Commit(_) => 12,
            Self::Abort(_) => 13,
            Self::OpenSnapshot(_) => 14,
            Self::ReleaseSnapshot(_) => 15,
            Self::Subscribe(_) => FEED,
            Self::ListTables(_) => 17,
            Self::CreateTable(_) => 18,
            Self::DropTable(_) => 19,
            Self::DescribeTable(_) => 20,
            Self::Info(_) => 21,
            Self::Flush(_) => 22,
            Self::Compact(_) => 23,
            Self::Backup(_) => 24,
            Self::BackupDownload(_) => DOWNLOAD,
            Self::Metrics(_) => 26,
            Self::Traverse(_) => 27,
            Self::Nearest(_) => 28,
        }
    }

    /// The RPC's name, `Service.Method`, for logs.
    pub(crate) fn name(&self) -> &'static str {
        RPCS[self.index()]
    }

    fn scope(&self) -> Scope {
        match self {
            // A transaction's reads, and its Commit (one that carries writes is checked
            // for write scope where it's answered: a read-only Commit is a consistent read).
            Self::Get(_)
            | Self::MultiGet(_)
            | Self::ReadRow(_)
            | Self::Scan(_)
            | Self::Begin(_)
            | Self::TxnGet(_)
            | Self::TxnGetRow(_)
            | Self::Commit(_)
            | Self::Abort(_)
            | Self::OpenSnapshot(_)
            | Self::ReleaseSnapshot(_)
            | Self::Subscribe(_)
            | Self::ListTables(_)
            | Self::DescribeTable(_)
            | Self::Info(_)
            | Self::Metrics(_)
            | Self::Traverse(_)
            | Self::Nearest(_) => Scope::Read,
            // Maintenance and backups: admin (ICR 0112's Admin).
            Self::Flush(_) | Self::Compact(_) | Self::Backup(_) | Self::BackupDownload(_) => {
                Scope::Admin
            }
            // Dropping a table drops its data: admin, as ICR 0112's Schema says.
            Self::DropTable(_) => Scope::Admin,
            Self::CreateTable(_) => Scope::Write,
            Self::Mutate(_)
            | Self::CheckAndMutate(_)
            | Self::Batch(_)
            | Self::MutateReturning(_)
            | Self::TxnWrite(_) => Scope::Write,
        }
    }

    /// The decoded request's size, which the in-flight bytes budget counts.
    fn size(&self) -> usize {
        match self {
            Self::Get(r) => r.encoded_len(),
            Self::MultiGet(r) => r.encoded_len(),
            Self::ReadRow(r) => r.encoded_len(),
            Self::Mutate(r) | Self::MutateReturning(r) => r.encoded_len(),
            Self::CheckAndMutate(r) => r.encoded_len(),
            Self::Batch(r) => r.encoded_len(),
            Self::Scan(r) => r.encoded_len(),
            Self::Begin(r) => r.encoded_len(),
            Self::TxnGet(r) => r.encoded_len(),
            Self::TxnGetRow(r) => r.encoded_len(),
            Self::TxnWrite(r) => r.encoded_len(),
            Self::Commit(r) => r.encoded_len(),
            Self::Abort(r) => r.encoded_len(),
            Self::OpenSnapshot(r) => r.encoded_len(),
            Self::ReleaseSnapshot(r) => r.encoded_len(),
            Self::Subscribe(r) => r.encoded_len(),
            Self::ListTables(r) => r.encoded_len(),
            Self::CreateTable(r) => r.encoded_len(),
            Self::DropTable(r) => r.encoded_len(),
            Self::DescribeTable(r) => r.encoded_len(),
            Self::Info(_) | Self::Flush(_) | Self::Compact(_) | Self::Metrics(_) => 0,
            Self::Backup(r) => r.encoded_len(),
            Self::BackupDownload(r) => r.encoded_len(),
            Self::Traverse(r) => r.encoded_len(),
            Self::Nearest(r) => r.encoded_len(),
        }
    }
}

/// `Data.Scan`'s index in `RPCS`: its outcome is counted when its stream ends.
pub(crate) const SCAN: usize = 7;

/// The RPCs, by `ServiceOp::index`.
/// `Feed.Subscribe`'s index in `RPCS`: counted when its stream ends, as a scan's is.
pub(crate) const FEED: usize = 16;

/// `Admin.BackupDownload`'s index in `RPCS`: counted when its stream ends.
pub(crate) const DOWNLOAD: usize = 25;

pub(crate) const RPCS: [&str; 29] = [
    "Data.Get",
    "Data.MultiGet",
    "Data.ReadRow",
    "Data.Mutate",
    "Data.CheckAndMutate",
    "Data.Batch",
    "Data.MutateReturning",
    "Data.Scan",
    "Transactions.Begin",
    "Transactions.Get",
    "Transactions.GetRow",
    "Transactions.Write",
    "Transactions.Commit",
    "Transactions.Abort",
    "Snapshots.Open",
    "Snapshots.Release",
    "Feed.Subscribe",
    "Schema.ListTables",
    "Schema.CreateTable",
    "Schema.DropTable",
    "Schema.DescribeTable",
    "Admin.Info",
    "Admin.Flush",
    "Admin.Compact",
    "Admin.Backup",
    "Admin.BackupDownload",
    "Admin.Metrics",
    "Graph.Traverse",
    "Vectors.Nearest",
];

/// The outcomes a request is counted under: `ok`, every code ICR 0112's table names, and
/// `other` for a code a newer engine adds.
pub(crate) const OUTCOMES: [&str; 43] = [
    "ok",
    "Io",
    "Corruption",
    "WriterLocked",
    "ShmVersionMismatch",
    "ShmUnavailable",
    "UnsupportedFormat",
    "NetworkFilesystem",
    "TableNotFound",
    "TableExists",
    "FamilyNotFound",
    "FamilyExists",
    "UnknownMergeOperator",
    "MergeFailed",
    "Conflict",
    "ReadOnly",
    "KeyTooLarge",
    "ValueTooLarge",
    "NoSpace",
    "InvalidArgument",
    "Unsupported",
    "Closed",
    "NoReaderSlot",
    "RecordTooLarge",
    "Busy",
    "SnapshotExpired",
    "WouldDeadlock",
    "BatchTooLarge",
    "OutcomeUnknown",
    "NoWriter",
    "UnsupportedFeature",
    "FeedExpired",
    "Unauthenticated",
    "PermissionDenied",
    "NotFound",
    "Overloaded",
    "RequestTooLarge",
    "DeadlineExceeded",
    "SessionBusy",
    "TransactionExpired",
    "TransactionFinished",
    "Internal",
    "other",
];

/// The duration histogram's bucket bounds, in microseconds: 100 µs to 10 s.
const BUCKETS_US: [u64; 16] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 5_000_000, 10_000_000,
];

/// Request counts by RPC and outcome, each RPC's durations, sheds, and the replies lost
/// after a write's outcome was known. Fixed tables of atomics: a few relaxed adds per
/// request, no lock and no allocation (perf287's F3 on #1501). Ticket 9 moves these onto
/// D316's names.
#[derive(Debug)]
pub(crate) struct Metrics {
    requests: [[AtomicU64; OUTCOMES.len()]; RPCS.len()],
    /// Per RPC: the count in each bucket (not cumulative; the last is past the last bound).
    durations: [[AtomicU64; BUCKETS_US.len() + 1]; RPCS.len()],
    /// Per RPC: the durations' sum, in microseconds.
    duration_sum_us: [AtomicU64; RPCS.len()],
    pub(crate) shed: AtomicU64,
    pub(crate) replies_lost: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            requests: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            durations: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            duration_sum_us: std::array::from_fn(|_| AtomicU64::new(0)),
            shed: AtomicU64::new(0),
            replies_lost: AtomicU64::new(0),
        }
    }
}

impl Metrics {
    /// Counts one request of RPC `rpc` (`ServiceOp::index`) with outcome `code` (`None`:
    /// ok) that took `took`: from its decoded request to its reply, or to a stream's end.
    pub(crate) fn count(&self, rpc: usize, code: Option<&str>, took: Duration) {
        let outcome = match code {
            None => 0,
            Some(c) => OUTCOMES
                .iter()
                .position(|o| *o == c)
                .unwrap_or(OUTCOMES.len() - 1),
        };
        let Some(row) = self.requests.get(rpc) else {
            return;
        };
        row[outcome].fetch_add(1, Ordering::Relaxed);
        let us = u64::try_from(took.as_micros()).unwrap_or(u64::MAX);
        let bucket = BUCKETS_US.partition_point(|&b| b < us);
        self.durations[rpc][bucket].fetch_add(1, Ordering::Relaxed);
        self.duration_sum_us[rpc].fetch_add(us, Ordering::Relaxed);
    }

    /// Prometheus text: the server's counters, and the engine's `async_sync_reads`
    /// (perf287's note 2 on D313).
    pub(crate) fn render(&self, db: &Pigeonhole) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        let _ = writeln!(out, "# TYPE pigeonhole_serve_requests_total counter");
        for (rpc, row) in RPCS.iter().zip(&self.requests) {
            for (outcome, n) in OUTCOMES.iter().zip(row) {
                let n = n.load(Ordering::Relaxed);
                if n > 0 {
                    let _ = writeln!(
                        out,
                        "pigeonhole_serve_requests_total{{rpc=\"{rpc}\",outcome=\"{outcome}\"}} {n}"
                    );
                }
            }
        }
        let _ = writeln!(
            out,
            "# TYPE pigeonhole_serve_request_duration_seconds histogram"
        );
        for (rpc, (buckets, sum)) in RPCS
            .iter()
            .zip(self.durations.iter().zip(&self.duration_sum_us))
        {
            let mut seen = 0;
            let mut lines = String::new();
            for (i, n) in buckets.iter().enumerate() {
                seen += n.load(Ordering::Relaxed);
                let le = BUCKETS_US
                    .get(i)
                    .map_or_else(|| "+Inf".to_owned(), |&b| (b as f64 / 1e6).to_string());
                let _ = writeln!(
                    lines,
                    "pigeonhole_serve_request_duration_seconds_bucket{{rpc=\"{rpc}\",le=\"{le}\"}} {seen}"
                );
            }
            if seen == 0 {
                continue;
            }
            out.push_str(&lines);
            let sum = sum.load(Ordering::Relaxed) as f64 / 1e6;
            let _ = writeln!(
                out,
                "pigeonhole_serve_request_duration_seconds_sum{{rpc=\"{rpc}\"}} {sum}"
            );
            let _ = writeln!(
                out,
                "pigeonhole_serve_request_duration_seconds_count{{rpc=\"{rpc}\"}} {seen}"
            );
        }
        let _ = writeln!(out, "# TYPE pigeonhole_serve_shed_total counter");
        let _ = writeln!(
            out,
            "pigeonhole_serve_shed_total {}",
            self.shed.load(Ordering::Relaxed)
        );
        let _ = writeln!(out, "# TYPE pigeonhole_serve_replies_lost_total counter");
        let _ = writeln!(
            out,
            "pigeonhole_serve_replies_lost_total {}",
            self.replies_lost.load(Ordering::Relaxed)
        );
        let _ = writeln!(out, "# TYPE pigeonhole_async_sync_reads_total counter");
        let _ = writeln!(
            out,
            "pigeonhole_async_sync_reads_total {}",
            db.engine_metrics().async_sync_reads
        );
        out
    }
}

/// The session table's gauges (D313 §3): open sessions by kind, and the oldest open one's age,
/// since a held transaction or snapshot holds back compaction's version GC and blob GC. A
/// client that leaks sessions shows up here. Ticket 9 moves these onto D316's names.
pub(crate) fn render_sessions(out: &mut String, sessions: &super::sessions::Sessions) {
    use std::fmt::Write as _;
    let s = sessions.stats(Instant::now());
    let _ = writeln!(out, "# TYPE pigeonhole_serve_sessions gauge");
    for (kind, n) in [
        ("transaction", s.transactions),
        ("snapshot", s.snapshots),
        ("tombstone", s.tombstones),
    ] {
        let _ = writeln!(out, "pigeonhole_serve_sessions{{kind=\"{kind}\"}} {n}");
    }
    let _ = writeln!(out, "# TYPE pigeonhole_serve_oldest_session_seconds gauge");
    let _ = writeln!(
        out,
        "pigeonhole_serve_oldest_session_seconds {:.3}",
        s.oldest.as_secs_f64()
    );
}

/// The feed fan-out's gauges and counters (D313 R2; perf287's notes on ticket 5).
pub(crate) fn render_feeds(out: &mut String, feeds: &super::feed::Feeds) {
    use std::fmt::Write as _;
    let g = feeds.gauges();
    let st = &feeds.stats;
    for (name, kind, v) in [
        ("pigeonhole_serve_feed_hubs", "gauge", g.hubs as u64),
        (
            "pigeonhole_serve_feed_ring_bytes",
            "gauge",
            g.ring_bytes as u64,
        ),
        (
            "pigeonhole_serve_feed_catching_up",
            "gauge",
            g.catching_up as u64,
        ),
        (
            "pigeonhole_serve_feed_catch_up_waits_total",
            "counter",
            st.catch_up_waits.load(Ordering::Relaxed),
        ),
        (
            "pigeonhole_serve_feed_fell_behind_total",
            "counter",
            st.fell_behind.load(Ordering::Relaxed),
        ),
        (
            "pigeonhole_serve_feed_rejoined_total",
            "counter",
            st.rejoined.load(Ordering::Relaxed),
        ),
        (
            "pigeonhole_serve_feed_hub_failures_total",
            "counter",
            st.hub_failures.load(Ordering::Relaxed),
        ),
    ] {
        let _ = writeln!(out, "# TYPE {name} {kind}");
        let _ = writeln!(out, "{name} {v}");
    }
}

/// The database, its open tables, and the limits every call passes through.
pub(crate) struct Service {
    pub(crate) db: Pigeonhole,
    /// Read on every call, changed only when a table is first used: a snapshot swapped
    /// whole, never a lock (perf287's note 3 on D313).
    tables: ArcSwap<HashMap<String, Table>>,
    /// The in-flight bytes budget (R3), in bytes.
    inflight: Arc<Semaphore>,
    /// Blocking-pool calls running at once.
    blocking: Arc<Semaphore>,
    /// Scans streaming at once (`--max-scans`).
    scans: Arc<Semaphore>,
    /// Transactions and pinned snapshots between requests (D313 §3).
    pub(crate) sessions: Arc<super::sessions::Sessions>,
    /// Feed and watch streams at once (`--max-streams`): a stream outlives its request's
    /// `--max-concurrent` slot, as a scan does.
    streams: Arc<Semaphore>,
    /// The feed fan-out's hubs (D313 R2).
    pub(crate) feeds: Arc<super::feed::Feeds>,
    /// `--backup-dir`, and the backup downloads kept for resumes.
    backups: Arc<super::backup::Backups>,
    /// Held across a table's create or drop and the cache update after it, so the cache ends
    /// at the live table whatever the order (simp724's C1 on #1527).
    schema: tokio::sync::Mutex<()>,
    /// Scan page reads at once: half the blocking pool, at least 1, so scans can't take
    /// every blocking thread from the other calls (perf287's G3 on #1510).
    scan_reads: Arc<Semaphore>,
    limits: Limits,
    pub(crate) metrics: Arc<Metrics>,
}

impl std::fmt::Debug for Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Service")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl Service {
    pub(crate) fn new(
        db: Pigeonhole,
        limits: Limits,
        blocking_threads: usize,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            db: db.clone(),
            tables: ArcSwap::from_pointee(HashMap::new()),
            inflight: Arc::new(Semaphore::new(
                limits.inflight_bytes.min(Semaphore::MAX_PERMITS),
            )),
            blocking: Arc::new(Semaphore::new(blocking_threads.max(1))),
            scans: Arc::new(Semaphore::new(limits.max_scans.max(1))),
            sessions: super::sessions::Sessions::new(limits.leases),
            streams: Arc::new(Semaphore::new(limits.max_streams.max(1))),
            backups: super::backup::Backups::new(None, 1).expect("no directory to check"),
            schema: tokio::sync::Mutex::new(()),
            feeds: super::feed::Feeds::new(
                db.clone(),
                super::feed::FeedLimits {
                    ring_bytes: limits.feed_ring_bytes,
                    max_hubs: limits.max_feed_hubs,
                    max_catch_up: limits.max_catch_up,
                    quiet: limits.feed_quiet,
                    stream_idle: limits.stream_idle,
                    client_events: limits.feed_client_events,
                    client_bytes: limits.feed_client_bytes,
                },
            ),
            scan_reads: Arc::new(Semaphore::new((blocking_threads / 2).max(1))),
            limits,
            metrics,
        }
    }

    /// Takes backups in `dir` (`--backup-dir`), at most `max_backups` at once. Without a
    /// directory, `Backup` and `BackupDownload` are refused. Fails at startup if the
    /// directory, or its download directory, isn't usable.
    pub(crate) fn backups(
        mut self,
        dir: Option<&std::path::Path>,
        max_backups: usize,
    ) -> Result<Self, String> {
        self.backups = super::backup::Backups::new(dir, max_backups)?;
        Ok(self)
    }

    /// The in-flight budget's free bytes, for tests to check nothing leaked.
    #[cfg(test)]
    pub(crate) fn inflight_available(&self) -> usize {
        self.inflight.available_permits()
    }

    /// Takes `bytes` of the in-flight budget, waiting at most the request timeout: for a
    /// front-end that knows a request's size before reading it (HTTP's `Content-Length`).
    pub(crate) async fn reserve(&self, bytes: usize) -> Result<OwnedSemaphorePermit, ApiError> {
        self.budget(bytes).await
    }

    /// `reserve`, waiting at most `wait`.
    pub(crate) async fn reserve_within(
        &self,
        bytes: usize,
        wait: Duration,
    ) -> Result<OwnedSemaphorePermit, ApiError> {
        budget_within(&self.inflight, bytes, wait).await
    }

    /// Answers `op` for `caller`: the scope check, the in-flight bytes budget (unless the
    /// front-end already reserved it, `prepaid`), then the call.
    pub(crate) async fn call(
        &self,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<OwnedSemaphorePermit>,
    ) -> Result<ServiceReply, ApiError> {
        if caller.scope < op.scope() {
            return Err(ApiError::permission_denied(&format!("call {}", op.name())));
        }
        let size = op.size();
        if size > self.limits.max_request_bytes {
            return Err(ApiError::request_too_large(self.limits.max_request_bytes));
        }
        // The budget is held until the reply is built (R3). Waiting for it counts against
        // the request's deadline; nothing has been submitted while it waits. Both front-ends
        // reserve it before reading the message (perf287's F2 on #1501): HTTP from
        // `Content-Length`, gRPC from the message header (`grpc::prepay`). It's taken here
        // only for a call that comes without one.
        // Reads hold the budget until their reply is built; a write hands it to its
        // detached task, so a client that goes away doesn't free bytes the write still
        // holds (simp724's S1 on #1501).
        let budget = match prepaid {
            Some(p) => p,
            None => self.budget(size).await?,
        };
        match op {
            // A read that fails on `Io` changed nothing, so it may be sent
            // again (`retry_from` REQUEST, ICR 0112).
            ServiceOp::Get(r) => {
                let reply = self.get(r, caller).await.map(ServiceReply::Get);
                drop(budget);
                reply.map_err(ApiError::for_read)
            }
            ServiceOp::MultiGet(r) => {
                let reply = self.multi_get(r, caller).await.map(ServiceReply::MultiGet);
                drop(budget);
                reply.map_err(ApiError::for_read)
            }
            ServiceOp::ReadRow(r) => {
                let reply = self.read_row(r, caller).await.map(ServiceReply::ReadRow);
                drop(budget);
                reply.map_err(ApiError::for_read)
            }
            ServiceOp::Scan(r) => self
                .scan(r, caller, budget)
                .await
                .map(ServiceReply::Scan)
                .map_err(ApiError::for_read),
            ServiceOp::Mutate(r) => self.mutate(r, budget).await.map(ServiceReply::Mutate),
            ServiceOp::CheckAndMutate(r) => self
                .check_and_mutate(r, budget)
                .await
                .map(ServiceReply::CheckAndMutate),
            ServiceOp::Batch(r) => self.batch(r, budget).await.map(ServiceReply::Mutate),
            ServiceOp::MutateReturning(r) => self
                .mutate_returning(r, budget)
                .await
                .map(ServiceReply::MutateReturning),
            ServiceOp::Begin(r) => {
                drop(budget);
                self.begin(r, caller).map(ServiceReply::Begin)
            }
            ServiceOp::TxnGet(r) => {
                let reply = self.txn_get(r, caller).await.map(ServiceReply::Get);
                drop(budget);
                reply.map_err(ApiError::for_read)
            }
            ServiceOp::TxnGetRow(r) => {
                let reply = self.txn_get_row(r, caller).await.map(ServiceReply::ReadRow);
                drop(budget);
                reply.map_err(ApiError::for_read)
            }
            ServiceOp::TxnWrite(r) => {
                let reply = self.txn_write(r, caller).await.map(ServiceReply::Empty);
                drop(budget);
                reply
            }
            ServiceOp::Commit(r) => self
                .commit(r, caller, budget)
                .await
                .map(ServiceReply::Commit),
            ServiceOp::Abort(r) => {
                drop(budget);
                self.abort(&r, caller).map(ServiceReply::Empty)
            }
            ServiceOp::OpenSnapshot(r) => {
                drop(budget);
                self.open_snapshot(&r, caller)
                    .map(ServiceReply::OpenSnapshot)
            }
            ServiceOp::ListTables(_) => {
                drop(budget);
                let mut tables = self.db.tables();
                tables.sort();
                Ok(ServiceReply::Tables(proto::ListTablesResponse { tables }))
            }
            ServiceOp::CreateTable(r) => {
                let reply = self.create_table(r).await.map(ServiceReply::Table);
                drop(budget);
                reply
            }
            ServiceOp::DropTable(r) => {
                drop(budget);
                self.drop_table(r.name)
                    .await
                    .map(|()| ServiceReply::Empty(proto::Empty {}))
            }
            ServiceOp::Info(_) => {
                drop(budget);
                let db = self.db.clone();
                let info = self.blocking(move || Ok(db.info()?)).await?;
                let mut tables = self.db.tables();
                tables.sort();
                Ok(ServiceReply::Info(proto::InfoResponse {
                    path: self.db.path().display().to_string(),
                    file_bytes: info.file_bytes,
                    free_bytes: info.free_bytes.unwrap_or(0),
                    tables,
                    version: env!("CARGO_PKG_VERSION").to_owned(),
                }))
            }
            ServiceOp::Flush(_) => {
                drop(budget);
                self.db.flush_async().await?;
                Ok(ServiceReply::Empty(proto::Empty {}))
            }
            ServiceOp::Compact(_) => {
                drop(budget);
                self.db.compact_async().await?;
                Ok(ServiceReply::Empty(proto::Empty {}))
            }
            ServiceOp::Backup(r) => {
                drop(budget);
                let (db, backups) = (self.db.clone(), Arc::clone(&self.backups));
                // On a thread of its own, one backup at a time (perf287's D1 on #1527).
                let bytes = self
                    .backups
                    .on_backup_thread(move || {
                        let path = backups.prepare_target(&r.path)?;
                        if r.durable {
                            db.backup_durable(&path)?;
                        } else {
                            db.backup(&path)?;
                        }
                        let len = std::fs::metadata(&path).map_or(0, |m| m.len());
                        Ok(len)
                    })
                    .await?;
                Ok(ServiceReply::Backup(proto::BackupResponse { bytes }))
            }
            ServiceOp::BackupDownload(r) => {
                drop(budget);
                self.backup_download(r).await.map(ServiceReply::Download)
            }
            ServiceOp::Metrics(_) => {
                drop(budget);
                Ok(ServiceReply::Metrics(proto::MetricsResponse {
                    text: self.metrics_text(),
                }))
            }
            ServiceOp::DescribeTable(r) => {
                drop(budget);
                let table = self.table(&r.name).await?;
                Ok(ServiceReply::Table(describe(&table)))
            }
            ServiceOp::Traverse(r) => {
                drop(budget);
                self.traverse(r, caller)
                    .await
                    .map(ServiceReply::Traverse)
                    .map_err(ApiError::for_read)
            }
            ServiceOp::Nearest(r) => {
                drop(budget);
                self.nearest(r, caller)
                    .await
                    .map(ServiceReply::Nearest)
                    .map_err(ApiError::for_read)
            }
            ServiceOp::Subscribe(r) => self
                .subscribe(r, caller, budget)
                .await
                .map(ServiceReply::Feed)
                .map_err(ApiError::for_read),
            ServiceOp::ReleaseSnapshot(r) => {
                drop(budget);
                self.sessions
                    .release(&r.snapshot_id, caller.token)
                    .map(|()| ServiceReply::Empty(proto::Empty {}))
            }
        }
    }

    async fn budget(&self, size: usize) -> Result<OwnedSemaphorePermit, ApiError> {
        budget_within(&self.inflight, size, self.limits.request_timeout).await
    }

    /// The table `name`, opened on first use. A miss opens it on the blocking pool (an open
    /// is a sync call, D196) and swaps in a new map.
    ///
    /// Its `or_insert` never displaces a handle a create put in. Creates and drops are
    /// serialized with their cache updates (`schema`), so the only stale entry it can leave
    /// is a handle resolved just before a drop, for a name with no table: its errors match
    /// the catalog, and the next create replaces it.
    async fn table(&self, name: &str) -> Result<Table, ApiError> {
        if let Some(t) = self.tables.load().get(name) {
            return Ok(t.clone());
        }
        let db = self.db.clone();
        let owned = name.to_owned();
        let table = self
            .blocking(move || db.table(&owned)?.open().map_err(ApiError::from))
            .await?;
        self.tables.rcu(|m| {
            let mut m = HashMap::clone(m);
            m.entry(name.to_owned()).or_insert_with(|| table.clone());
            m
        });
        Ok(table)
    }

    /// `Schema.CreateTable`: the table with its families, created, or with `if_missing`
    /// opened with the families it lacks added. The service's handle for the name is
    /// replaced, so later calls (and feed hubs, keyed by its id) use this table.
    async fn create_table(
        &self,
        r: proto::CreateTableRequest,
    ) -> Result<proto::TableResponse, ApiError> {
        if r.name.is_empty() {
            return Err(ApiError::invalid("a table needs a name"));
        }
        let families = r
            .families
            .iter()
            .map(|f| Ok((f.name.clone(), convert::family_in(f)?)))
            .collect::<Result<Vec<_>, ApiError>>()?;
        let _schema = self.schema.lock().await;
        let db = self.db.clone();
        let (name, if_missing) = (r.name.clone(), r.if_missing);
        let table = self
            .blocking(move || {
                let mut b = db.table(&name)?;
                for (n, f) in families {
                    b = b.family(&n, f);
                }
                Ok(if if_missing {
                    b.create_if_missing()?
                } else {
                    b.create()?
                })
            })
            .await?;
        self.tables.rcu(|m| {
            let mut m = HashMap::clone(m);
            m.insert(r.name.clone(), table.clone());
            m
        });
        Ok(describe(&table))
    }

    /// `Schema.DropTable`: the table and its data go, and so does the service's handle for
    /// the name. A table created again under it is a new table, with a new id: its feed hubs
    /// are new too, and those on the old one end with their engine feeds.
    async fn drop_table(&self, name: String) -> Result<(), ApiError> {
        let _schema = self.schema.lock().await;
        let db = self.db.clone();
        let owned = name.clone();
        let dropped = self.blocking(move || Ok(db.drop_table(&owned)?)).await;
        // Whatever the outcome, the next use resolves the name again.
        self.tables.rcu(|m| {
            let mut m = HashMap::clone(m);
            m.remove(&name);
            m
        });
        dropped
    }

    /// The metrics as Prometheus text: `/metrics` and `Admin.Metrics` alike.
    pub(crate) fn metrics_text(&self) -> String {
        let mut text = self.metrics.render(&self.db);
        render_sessions(&mut text, &self.sessions);
        render_feeds(&mut text, &self.feeds);
        text
    }

    /// `Admin.BackupDownload`: a fresh backup (hashed, then kept for resumes), or a kept one
    /// from an offset, streamed in chunks of up to 1 MiB on its own task. It holds a
    /// `--max-streams` slot until it ends, and each send waits at most `--stream-idle-ms`.
    async fn backup_download(
        &self,
        r: proto::BackupDownloadRequest,
    ) -> Result<super::backup::Download, ApiError> {
        use super::backup;
        let Ok(slot) = Arc::clone(&self.streams).try_acquire_owned() else {
            return Err(ApiError::overloaded(
                "the server is at its concurrent stream limit (--max-streams)",
            ));
        };
        let (id, kept) = if r.backup_id.is_empty() {
            let (db, durable) = (self.db.clone(), r.durable);
            // The backup and its hash run on a thread of their own, never the blocking pool
            // (perf287's D1), and the copy is the table's from the start (E1).
            self.backups
                .fresh_download(move |p| {
                    if durable {
                        db.backup_durable(p)?;
                    } else {
                        db.backup(p)?;
                    }
                    Ok(())
                })
                .await?
        } else {
            (r.backup_id.clone(), self.backups.open(&r.backup_id)?)
        };
        if r.offset > kept.total {
            self.backups.release(&id);
            return Err(ApiError::invalid(format!(
                "offset {} is past the backup's {} bytes",
                r.offset, kept.total
            )));
        }
        let (tx, rx) = mpsc::channel(1);
        let (backups, pool, idle) = (
            Arc::clone(&self.backups),
            Arc::clone(&self.blocking),
            self.limits.stream_idle,
        );
        let metrics = Arc::clone(&self.metrics);
        let started = Instant::now();
        let (task_id, task_kept, start) = (id.clone(), kept.clone(), r.offset);
        tokio::spawn(async move {
            let mut at = start;
            let outcome: Result<(), ApiError> = async {
                loop {
                    let path = task_kept.path.clone();
                    let data = blocking(&pool, move || backup::read_chunk(&path, at)).await?;
                    let next = at + data.len() as u64;
                    let last = next >= task_kept.total;
                    let chunk = proto::BackupChunk {
                        data: data.into(),
                        total: if last { task_kept.total } else { 0 },
                        backup_id: task_id.clone(),
                        offset: at,
                        sha256: if last {
                            task_kept.sha256.to_vec().into()
                        } else {
                            Default::default()
                        },
                    };
                    match tokio::time::timeout(idle, tx.send(Ok(chunk))).await {
                        Ok(Ok(())) => {}
                        // The client went away; it resumes with the id.
                        Ok(Err(_)) => return Ok(()),
                        Err(_) => {
                            return Err(ApiError::deadline(
                                "the client took no backup chunk for --stream-idle-ms; resume                                  with backup_id and the offset it reached",
                            ));
                        }
                    }
                    if last {
                        return Ok(());
                    }
                    at = next;
                }
            }
            .await;
            metrics.count(
                DOWNLOAD,
                outcome.as_ref().err().map(|e| e.code.as_str()),
                started.elapsed(),
            );
            if let Err(e) = outcome {
                let _ = tx.try_send(Err(e));
            }
            backups.release(&task_id);
            drop(slot);
        });
        Ok(super::backup::Download {
            id,
            total: kept.total,
            sha256: kept.sha256,
            offset: r.offset,
            chunks: rx,
        })
    }

    /// Runs a sync-only call on the blocking pool, at most `--blocking-threads` at once.
    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
    ) -> Result<T, ApiError> {
        blocking(&self.blocking, f).await
    }

    /// Runs a write as its own task and awaits its outcome (B2). If the request goes away
    /// first, the task carries on, and its outcome is logged and counted.
    async fn detached<T: Send + std::fmt::Debug + 'static>(
        &self,
        rpc: &'static str,
        budget: OwnedSemaphorePermit,
        write: impl Future<Output = Result<T, ApiError>> + Send + 'static,
    ) -> Result<T, ApiError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let metrics = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            let outcome = write.await;
            // The write is done: its bytes leave the budget now, not when its request did.
            drop(budget);
            if let Err(lost) = tx.send(outcome) {
                metrics.replies_lost.fetch_add(1, Ordering::Relaxed);
                match lost {
                    Ok(v) => eprintln!(
                        "phdb serve: {rpc}'s client went away; the write committed: {v:?}"
                    ),
                    Err(e) => eprintln!(
                        "phdb serve: {rpc}'s client went away; the write failed: {} ({})",
                        e.code, e.message
                    ),
                }
            }
        });
        rx.await.unwrap_or_else(|_| {
            Err(ApiError::internal(
                "a write's task ended without an outcome",
            ))
        })
    }

    async fn get(
        &self,
        r: proto::GetRequest,
        caller: &Caller,
    ) -> Result<proto::GetResponse, ApiError> {
        let pinned = self.pinned(&r.snapshot_id, caller)?;
        let table = self.table(&r.table).await?;
        let cell = match &pinned {
            Some(p) => {
                table
                    .get_at_async(p.snapshot(), &r.row, &r.family, &r.qualifier)
                    .await?
            }
            None => table.get_async(&r.row, &r.family, &r.qualifier).await?,
        };
        drop(pinned);
        Ok(proto::GetResponse {
            cell: cell.map(|c| convert::cell(&r.family, &r.qualifier, &c, r.aggregate.as_ref())),
        })
    }

    /// On the blocking pool: `KeysRead::read_within` is sync-only (D196). TODO(#398, #1286):
    /// async once multi-get pages have an async form, so a multi-get holds no blocking thread.
    async fn multi_get(
        &self,
        r: proto::MultiGetRequest,
        caller: &Caller,
    ) -> Result<proto::MultiGetResponse, ApiError> {
        let pinned = self.pinned_selection(r.selection.as_ref(), caller)?;
        let table = self.table(&r.table).await?;
        let budget = page_bytes(r.page_bytes);
        self.blocking(move || {
            let at = pinned.as_ref().map(|p| p.snapshot().clone());
            let select = convert::Select::new(r.selection.as_ref())?.at(at);
            let keys = KeySet::from_unsorted(r.keys.iter().map(|k| &k[..]));
            let mut read = select.keys(table.keys(&keys))?;
            let after;
            if !r.after.is_empty() {
                after = KeysPosition::from_bytes(&r.after).map_err(ApiError::from)?;
                read = read.resume_after(&after);
            }
            let page = read.read_within(budget)?;
            let q = select.aggregate();
            Ok(proto::MultiGetResponse {
                rows: page
                    .rows()
                    .iter()
                    .map(|row| convert::row(row.view(), q))
                    .collect(),
                next: page
                    .resume()
                    .map(|p| p.to_bytes().into())
                    .unwrap_or_default(),
                bytes: page.bytes(),
            })
        })
        .await
    }

    /// `Graph.Traverse` (D304): the hops from the seeds over one edge family, at the pinned
    /// snapshot `snapshot_id` names, or now. Run on the request's task (`run_async` never
    /// blocks on a block it can fetch), so it holds no blocking thread.
    async fn traverse(
        &self,
        r: proto::TraverseRequest,
        caller: &Caller,
    ) -> Result<proto::TraverseResponse, ApiError> {
        if r.seeds.is_empty() {
            return Err(ApiError::invalid(
                "a traversal starts from at least one seed",
            ));
        }
        if r.hops > 64 {
            return Err(ApiError::invalid("hops is 1 to 64"));
        }
        let pinned = self.pinned(&r.snapshot_id, caller)?;
        let table = self.table(&r.table).await?;
        let seeds = KeySet::from_unsorted(r.seeds.iter().map(|k| &k[..]));
        let mut t = convert::edges(table.traverse(&seeds).family(&r.family), &r)?
            .hops(r.hops.max(1))
            .parents(r.parents);
        if let Some(p) = &pinned {
            t = t.snapshot(p.snapshot());
        }
        if r.per_node_limit > 0 {
            t = t.per_node_limit(r.per_node_limit);
        }
        if r.hop_limit > 0 {
            t = t.hop_limit(r.hop_limit);
        }
        if r.max_nodes > 0 {
            t = t.max_nodes(r.max_nodes);
        }
        let found = t.run_async().await?;
        let stats = found.stats();
        Ok(proto::TraverseResponse {
            hops: found
                .hops()
                .iter()
                .map(|h| proto::Hop {
                    nodes: h.nodes().iter().map(|k| k.to_vec().into()).collect(),
                    parents: h.parents().map(<[u32]>::to_vec).unwrap_or_default(),
                })
                .collect(),
            stats: Some(proto::TraverseStats {
                rows: stats.rows,
                edges: stats.edges,
                hops_cut: stats.hops_cut,
                cut_by_max_nodes: stats.cut_by_max_nodes,
            }),
        })
    }

    /// `Vectors.Nearest` (D296, D303): the exact top `k` of one vector family over a key
    /// range or a key list, at the pinned snapshot `snapshot_id` names, or now, cut to
    /// `page_bytes` by D308's measure when it's set. Run on the request's task, as `Traverse`.
    async fn nearest(
        &self,
        r: proto::NearestRequest,
        caller: &Caller,
    ) -> Result<proto::NearestResponse, ApiError> {
        use proto::nearest_request::Source;
        if r.k == 0 {
            return Err(ApiError::invalid("k is at least 1"));
        }
        let query = r
            .query
            .as_ref()
            .ok_or_else(|| ApiError::invalid("a nearest search needs a query vector"))?;
        let query = convert::nearest_query(query)?;
        let pinned = self.pinned(&r.snapshot_id, caller)?;
        let table = self.table(&r.table).await?;
        let at = pinned.as_ref().map(|p| p.snapshot().clone());
        let select = convert::Select::new(None)?.at(at);
        let k = r.k as usize;
        let search = match &r.source {
            Some(Source::Keys(keys)) => {
                let keys = KeySet::from_unsorted(keys.keys.iter().map(|k| &k[..]));
                let read = select.keys(table.keys(&keys))?.family(&r.family);
                query.on_keys(read, k)
            }
            Some(Source::Range(range)) => {
                query.on_scan(select.scan(&table, Some(range))?.family(&r.family), k)
            }
            None => query.on_scan(select.scan(&table, None)?.family(&r.family), k),
        };
        let (found, cut_by_budget) = if r.page_bytes == 0 {
            (search.run_async().await?, false)
        } else {
            let page = search.run_within_async(r.page_bytes).await?;
            let cut = page.cut_by_budget();
            (page.into_neighbours(), cut)
        };
        Ok(proto::NearestResponse {
            neighbours: found
                .into_iter()
                .map(|n| proto::Neighbour {
                    row: n.row.into(),
                    qualifier: n.qualifier.into(),
                    distance: n.distance,
                    timestamp: n.ts,
                })
                .collect(),
            cut_by_budget,
        })
    }

    /// A row's selected cells, a page at a time: whole columns, at most `page_bytes` by
    /// D308's measure (perf287's S1) and `page_cells` cells, resumed from `after`. The
    /// first column always comes back, so a page makes progress even when one column is
    /// larger than the budget. On the blocking pool, since row pages are sync-only (D196).
    /// TODO(#398, #1286): `read_async`-based paging, so a row read holds no blocking thread.
    async fn read_row(
        &self,
        r: proto::ReadRowRequest,
        caller: &Caller,
    ) -> Result<proto::ReadRowResponse, ApiError> {
        let pinned = self.pinned_selection(r.selection.as_ref(), caller)?;
        let table = self.table(&r.table).await?;
        let budget = page_bytes(r.page_bytes);
        let cells_cap = if r.page_cells == 0 {
            usize::MAX
        } else {
            r.page_cells as usize
        };
        self.blocking(move || {
            let at = pinned.as_ref().map(|p| p.snapshot().clone());
            let select = convert::Select::new(r.selection.as_ref())?.at(at);
            let mut read = select.row(table.row(&r.row))?;
            let after;
            if !r.after.is_empty() {
                after = RowPosition::from_bytes(&r.after).map_err(ApiError::from)?;
                read = read.resume_after(&after);
            }
            let q = select.aggregate();
            // One column per engine page, so the byte budget is checked at every column
            // boundary: a page is cut before the column that wouldn't fit, and only a
            // single column larger than the budget goes over it (D308's rule).
            let mut pages = read.pages(1)?;
            let mut out: Option<proto::Row> = None;
            let (mut bytes, mut cells) = (0u64, 0usize);
            let mut next = Vec::new();
            let mut more = false;
            while let Some(page) = pages.next()? {
                let part = convert::row(page, q);
                let part_bytes: u64 = part.cells.iter().map(convert::cell_bytes).sum();
                let first = out.is_none();
                if !first && (bytes + part_bytes > budget || cells + part.cells.len() > cells_cap) {
                    more = true;
                    break;
                }
                bytes += part_bytes;
                cells += part.cells.len();
                match &mut out {
                    None => out = Some(part),
                    Some(row) => row.cells.extend(part.cells),
                }
                next = pages.position().map(|p| p.to_bytes()).unwrap_or_default();
                if bytes >= budget || cells >= cells_cap {
                    more = true;
                    break;
                }
            }
            if let Some(row) = &out {
                bytes += row.key.len() as u64;
            }
            Ok(proto::ReadRowResponse {
                row: out,
                next: if more {
                    next.into()
                } else {
                    Default::default()
                },
                bytes,
            })
        })
        .await
    }

    /// A scan as a stream of pages (ICR 0112): each page is one `read_within` on the
    /// blocking pool (D196: scan pages are sync-only), resumed from the last, until the range
    /// or `limit` runs out. Each page is a new read, at its own snapshot (simp724's D1 on
    /// #1510), unless the selection names a pinned one, which the stream holds. The request is checked before the stream starts, so a bad one fails the call;
    /// a failure later ends the stream with the error, a panic in the task as `Internal`. The
    /// scan holds a `--max-scans` slot until its stream ends: a stream outlives its request's
    /// `--max-concurrent` slot. TODO(#398, #1286): async scan pages, so a page read holds
    /// no blocking thread.
    async fn scan(
        &self,
        r: proto::ScanRequest,
        caller: &Caller,
        budget: OwnedSemaphorePermit,
    ) -> Result<ScanPages, ApiError> {
        let Ok(slot) = Arc::clone(&self.scans).try_acquire_owned() else {
            return Err(ApiError::overloaded(
                "the server is at its concurrent scan limit",
            ));
        };
        convert::Select::new(r.selection.as_ref())?;
        // A pinned snapshot is held for the stream's life, and checked before each page: a
        // page that starts after its `Release` gets NotFound.
        let pinned = self.pinned_selection(r.selection.as_ref(), caller)?;
        let after = if r.after.is_empty() {
            None
        } else {
            Some(ScanPosition::from_bytes(&r.after).map_err(ApiError::from)?)
        };
        let table = self.table(&r.table).await?;
        // The request's bytes are decoded and checked: the stream holds pages, not them.
        drop(budget);
        let (tx, rx) = mpsc::channel(1);
        let (end_tx, end_rx) = oneshot::channel();
        let task = ScanTask {
            table,
            r,
            after,
            pinned,
            pool: Arc::clone(&self.blocking),
            reads: Arc::clone(&self.scan_reads),
            inflight: Arc::clone(&self.inflight),
            limits: self.limits,
            pages: tx,
        };
        let metrics = Arc::clone(&self.metrics);
        let started = Instant::now();
        tokio::spawn(async move {
            // Its own task, so a panic is a `JoinError` here, and the stream ends with
            // `Internal` rather than looking like a clean end (simp724's B1 on #1510).
            let outcome = match tokio::spawn(task.run()).await {
                Ok(outcome) => outcome.map_err(ApiError::for_read),
                Err(e) => Err(ApiError::internal(format!("the scan failed: {e}"))),
            };
            metrics.count(
                SCAN,
                outcome.as_ref().err().map(|e| e.code.as_str()),
                started.elapsed(),
            );
            drop(slot);
            if let Err(e) = outcome {
                let _ = end_tx.send(e);
            }
        });
        Ok(ScanPages {
            pages: rx,
            end: Some(end_rx),
        })
    }

    /// The pinned snapshot `id` names, held for one read; `None` for an empty id (now).
    fn pinned(&self, id: &str, caller: &Caller) -> Result<Option<SnapshotGuard>, ApiError> {
        if id.is_empty() {
            return Ok(None);
        }
        self.sessions.snapshot(id, caller.token).map(Some)
    }

    fn pinned_selection(
        &self,
        s: Option<&proto::Selection>,
        caller: &Caller,
    ) -> Result<Option<SnapshotGuard>, ApiError> {
        self.pinned(s.map_or("", |s| s.snapshot_id.as_str()), caller)
    }

    /// `Feed.Subscribe` (D313 R2): the client's tables resolved once (so its hub and any
    /// catch-up stream follow the same table ids), its start checked, and its stream handed to
    /// the fan-out. It holds a `--max-streams` slot until it ends.
    async fn subscribe(
        &self,
        r: proto::SubscribeRequest,
        caller: &Caller,
        budget: OwnedSemaphorePermit,
    ) -> Result<super::feed::FeedEvents, ApiError> {
        use super::feed::Start;
        use proto::subscribe_request::Start as S;
        let Ok(slot) = Arc::clone(&self.streams).try_acquire_owned() else {
            return Err(ApiError::overloaded(
                "the server is at its concurrent stream limit (--max-streams)",
            ));
        };
        if r.tables.is_empty() {
            return Err(ApiError::invalid("a subscription names at least one table"));
        }
        let mut names = r.tables.clone();
        names.sort();
        names.dedup();
        let mut tables = Vec::with_capacity(names.len());
        for name in &names {
            tables.push(self.table(name).await?);
        }
        let start = match r.start {
            None | Some(S::FromOldest(false)) => Start::Now,
            Some(S::FromOldest(true)) => Start::Oldest,
            Some(S::FromSnapshot(id)) => Start::Snapshot(
                self.sessions
                    .snapshot(&id, caller.token)?
                    .snapshot()
                    .clone(),
            ),
            Some(S::Resume(token)) => {
                Start::Token(pigeonhole::FeedToken::from_bytes(token.to_vec()))
            }
        };
        drop(budget);
        let metrics = Arc::clone(&self.metrics);
        let started = Instant::now();
        self.feeds.subscribe(
            super::feed::Spec {
                tables,
                prefix: r.prefix.to_vec(),
                visible: r.visible,
                start,
            },
            slot,
            move |e| metrics.count(FEED, e.map(|e| e.code.as_str()), started.elapsed()),
        )
    }

    /// `Transactions.Begin`: a transaction at a snapshot taken now, held for the caller.
    fn begin(
        &self,
        r: proto::BeginRequest,
        caller: &Caller,
    ) -> Result<proto::BeginResponse, ApiError> {
        let txn = self.db.transaction()?;
        let snapshot_seqno = txn.snapshot_seqno();
        let txn_id = self
            .sessions
            .begin(caller.token, txn, convert::durability(r.durability))?;
        Ok(proto::BeginResponse {
            txn_id,
            snapshot_seqno,
        })
    }

    /// Takes transaction `id` for a call that only an open transaction answers.
    fn open_txn(&self, id: &str, caller: &Caller) -> Result<super::sessions::TxnGuard, ApiError> {
        match self.sessions.take(id, caller.token)? {
            Taken::Open(g) => Ok(g),
            Taken::Done(outcome) => Err(ApiError::transaction_finished(&outcome)),
        }
    }

    /// A request's rows, ready to buffer: each row's table opened and every write checked
    /// and decoded (`convert::txn_ops`), all before the transaction is taken out. So a missing
    /// table or a malformed write fails the call with nothing buffered (simp724's T1 on
    /// #1511), and buffering the rest can't fail on the server's side.
    async fn txn_rows<'r>(
        &self,
        rows: &'r [proto::RowWrites],
    ) -> Result<Vec<(Table, &'r [u8], Vec<convert::TxnOp<'r>>)>, ApiError> {
        let mut out = Vec::with_capacity(rows.len());
        for rw in rows {
            let key = rw
                .row
                .as_ref()
                .ok_or_else(|| ApiError::invalid("each row's writes name its table and row"))?;
            let ops = convert::txn_ops(&rw.writes)?;
            out.push((self.table(&key.table).await?, &key.row[..], ops));
        }
        Ok(out)
    }

    async fn txn_get(
        &self,
        r: proto::TxnGetRequest,
        caller: &Caller,
    ) -> Result<proto::GetResponse, ApiError> {
        let table = self.table(&r.table).await?;
        let mut g = self.open_txn(&r.txn_id, caller)?;
        let cell = g
            .txn()
            .get_async(&table, &r.row, &r.family, &r.qualifier)
            .await?;
        drop(g);
        Ok(proto::GetResponse {
            cell: cell.map(|c| convert::cell(&r.family, &r.qualifier, &c, None)),
        })
    }

    /// On the blocking pool, as `get_row` is sync-only (D196): the transaction is lent to the
    /// blocking call and back, and a panic there expires the session (uncommitted).
    async fn txn_get_row(
        &self,
        r: proto::TxnGetRowRequest,
        caller: &Caller,
    ) -> Result<proto::ReadRowResponse, ApiError> {
        let table = self.table(&r.table).await?;
        let mut g = self.open_txn(&r.txn_id, caller)?;
        let mut txn = g.lend();
        let (txn, row) = self
            .blocking(move || {
                let families: Vec<&str> = r.families.iter().map(String::as_str).collect();
                let row = txn
                    .get_row(&table, &r.row, &families)
                    .map(|row| row.map(|row| convert::row(row, None)));
                Ok((txn, row))
            })
            .await?;
        g.restore(txn);
        drop(g);
        let row = row?;
        let bytes = row.as_ref().map_or(0, row_bytes);
        Ok(proto::ReadRowResponse {
            row,
            next: Default::default(),
            bytes,
        })
    }

    /// `Transactions.Write`: buffers the writes, all or none (`txn_rows`). What only the
    /// engine can check (a family, a size) fails the `Commit`, as in-process.
    async fn txn_write(
        &self,
        r: proto::TxnWriteRequest,
        caller: &Caller,
    ) -> Result<proto::Empty, ApiError> {
        let rows = self.txn_rows(&r.rows).await?;
        let mut g = self.open_txn(&r.txn_id, caller)?;
        for (table, row, ops) in &rows {
            convert::txn_apply(g.txn(), table, row, ops);
        }
        Ok(proto::Empty {})
    }

    /// `Transactions.Commit`, detached from the request (D313 B2): the commit runs in its own
    /// task, which records the outcome as the session's tombstone (B1), so a client that goes
    /// away loses only the reply. A repeated `Commit` answers the tombstone.
    async fn commit(
        &self,
        r: proto::TxnCommitRequest,
        caller: &Caller,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::TxnCommitResponse, ApiError> {
        if !r.rows.is_empty() && caller.scope < Scope::Write {
            return Err(ApiError::permission_denied("commit writes"));
        }
        let rows = self.txn_rows(&r.rows).await?;
        let mut g = match self.sessions.take(&r.txn_id, caller.token)? {
            Taken::Open(g) => g,
            Taken::Done(outcome) => {
                return outcome.map(|c| proto::TxnCommitResponse {
                    commit: Some(convert::commit_info(c)),
                });
            }
        };
        for (table, row, ops) in &rows {
            convert::txn_apply(g.txn(), table, row, ops);
        }
        drop(rows);
        let (txn, durability, ticket) = g.commit();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let metrics = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            let commit = match durability {
                Some(d) => txn.commit_with_async(d),
                None => txn.commit_async(),
            };
            // Its own task, so a panic still records an outcome (unknown), never nothing.
            let outcome = match tokio::spawn(commit).await {
                Ok(r) => r.map_err(ApiError::from),
                Err(e) => Err(ApiError::outcome_unknown(format!(
                    "the commit's task failed: {e}"
                ))),
            };
            ticket.finish(outcome.clone());
            drop(budget);
            if tx.send(outcome).is_err() {
                metrics.replies_lost.fetch_add(1, Ordering::Relaxed);
            }
        });
        let outcome = rx.await.unwrap_or_else(|_| {
            Err(ApiError::outcome_unknown(
                "the commit's task ended without an outcome",
            ))
        })?;
        Ok(proto::TxnCommitResponse {
            commit: Some(convert::commit_info(outcome)),
        })
    }

    fn abort(&self, r: &proto::TxnAbortRequest, caller: &Caller) -> Result<proto::Empty, ApiError> {
        self.open_txn(&r.txn_id, caller)?.abort();
        Ok(proto::Empty {})
    }

    fn open_snapshot(
        &self,
        r: &proto::OpenSnapshotRequest,
        caller: &Caller,
    ) -> Result<proto::OpenSnapshotResponse, ApiError> {
        let snapshot = if r.durable {
            self.db.durable_snapshot()?
        } else {
            self.db.snapshot()?
        };
        let seqno = snapshot.seqno();
        let snapshot_id = self.sessions.open_snapshot(caller.token, snapshot)?;
        Ok(proto::OpenSnapshotResponse { snapshot_id, seqno })
    }

    async fn mutate(
        &self,
        r: proto::MutateRequest,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::MutateResponse, ApiError> {
        let rw = r
            .writes
            .ok_or_else(|| ApiError::invalid("Mutate needs writes"))?;
        let key = rw
            .row
            .ok_or_else(|| ApiError::invalid("Mutate's writes name a row"))?;
        let table = self.table(&key.table).await?;
        let durability = convert::durability(r.durability);
        let info = self
            .detached("Data.Mutate", budget, async move {
                let mut m = table.mutate(&key.row);
                if let Some(d) = durability {
                    m = m.durability(d);
                }
                Ok(convert::mutation(m, &rw.writes)?.commit_async().await?)
            })
            .await?;
        Ok(proto::MutateResponse {
            commit: Some(convert::commit_info(info)),
        })
    }

    async fn check_and_mutate(
        &self,
        r: proto::CheckAndMutateRequest,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::CheckAndMutateResponse, ApiError> {
        let rw = r
            .writes
            .ok_or_else(|| ApiError::invalid("CheckAndMutate needs writes"))?;
        let key = rw
            .row
            .ok_or_else(|| ApiError::invalid("CheckAndMutate's writes name a row"))?;
        let condition = convert::condition(r.condition.as_ref())?;
        let table = self.table(&key.table).await?;
        let durability = convert::durability(r.durability);
        let info = self
            .detached("Data.CheckAndMutate", budget, async move {
                let mut m = table.mutate(&key.row);
                if let Some(d) = durability {
                    m = m.durability(d);
                }
                Ok(convert::mutation(m, &rw.writes)?
                    .commit_if_async(&condition)
                    .await?)
            })
            .await?;
        Ok(proto::CheckAndMutateResponse {
            applied: info.is_some(),
            commit: info.map(convert::commit_info),
        })
    }

    async fn batch(
        &self,
        r: proto::BatchRequest,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::MutateResponse, ApiError> {
        let mut rows = Vec::with_capacity(r.rows.len());
        for rw in r.rows {
            let key = rw
                .row
                .ok_or_else(|| ApiError::invalid("each batch row names its table and row"))?;
            let table = self.table(&key.table).await?;
            rows.push((table, key.row, rw.writes));
        }
        let durability = convert::durability(r.durability);
        let db = self.db.clone();
        let info = self
            .detached("Data.Batch", budget, async move {
                let mut b = db.write_batch();
                for (table, row, writes) in &rows {
                    convert::batch(&mut b, table, row, writes)?;
                }
                Ok(match durability {
                    Some(d) => b.commit_with_async(d).await?,
                    None => b.commit_async().await?,
                })
            })
            .await?;
        Ok(proto::MutateResponse {
            commit: Some(convert::commit_info(info)),
        })
    }

    async fn mutate_returning(
        &self,
        r: proto::MutateRequest,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::MutateReturningResponse, ApiError> {
        let rw = r
            .writes
            .ok_or_else(|| ApiError::invalid("MutateReturning needs writes"))?;
        let key = rw
            .row
            .ok_or_else(|| ApiError::invalid("MutateReturning's writes name a row"))?;
        let table = self.table(&key.table).await?;
        let durability = convert::durability(r.durability);
        let returned = self
            .detached("Data.MutateReturning", budget, async move {
                let mut m = table.mutate(&key.row);
                if let Some(d) = durability {
                    m = m.durability(d);
                }
                Ok(convert::mutation(m, &rw.writes)?
                    .commit_returning_async()
                    .await?)
            })
            .await?;
        use proto::returned_cell::Value;
        let cells = returned
            .iter()
            .map(|(family, qualifier, ts, v)| proto::ReturnedCell {
                family: family.to_owned(),
                qualifier: qualifier.to_vec().into(),
                timestamp: ts,
                // The commit succeeded: a read error here is that cell's value, not the
                // call's error (D314).
                value: Some(match v {
                    Ok(Some(n)) => Value::I64(n),
                    Ok(None) => Value::Absent(proto::Empty {}),
                    Err(e) => Value::Error(ApiError::from_engine(&e).detail()),
                }),
            })
            .collect();
        Ok(proto::MutateReturningResponse {
            commit: Some(convert::commit_info(returned.info)),
            cells,
        })
    }
}

fn page_bytes(asked: u64) -> u64 {
    if asked == 0 {
        DEFAULT_PAGE_BYTES
    } else {
        asked.min(MAX_PAGE_BYTES)
    }
}

/// Runs a sync-only call on the blocking pool, holding one of `pool`'s permits.
async fn blocking<T: Send + 'static>(
    pool: &Arc<Semaphore>,
    f: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
) -> Result<T, ApiError> {
    let _permit = Arc::clone(pool)
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal("the blocking pool closed"))?;
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| ApiError::internal(format!("a blocking call failed: {e}")))?
}

/// A scan's reading side, run as its own task.
struct ScanTask {
    table: Table,
    r: proto::ScanRequest,
    after: Option<ScanPosition>,
    pinned: Option<SnapshotGuard>,
    pool: Arc<Semaphore>,
    reads: Arc<Semaphore>,
    inflight: Arc<Semaphore>,
    limits: Limits,
    pages: mpsc::Sender<PageSlot>,
}

impl ScanTask {
    /// Reads the pages into `pages` until the scan ends or the client goes away.
    ///
    /// The row `limit` spans pages. A page cut inside a row resumes in that row, and the
    /// engine's `limit` counts the continued row again, so a read after a row asks for one
    /// more row than remain. A page that comes back with too many is cut here: it is the
    /// scan's last page, so no position is needed past the cut. That includes a page whose
    /// continued row was deleted between pages, so all its rows are new (simp724's B1 on
    /// #1510). A page with exactly the rows that remain ends the scan unless its budget
    /// stopped it (`cut_by_budget`), which may have cut its last row; the next read finishes
    /// that row.
    ///
    /// Each page waits, in turn: for the client to take the page before it (at most
    /// `--stream-idle-ms`, perf287's G2), for one of the scans' blocking threads (G3), and for
    /// its share of the in-flight budget (G1, at most the request timeout).
    async fn run(self) -> Result<(), ApiError> {
        let Self {
            table,
            r,
            mut after,
            pinned,
            pool,
            reads,
            inflight,
            limits,
            pages,
        } = self;
        let r = Arc::new(r);
        let budget = page_bytes(r.page_bytes);
        let mut remaining = (r.limit > 0).then_some(r.limit);
        // The row the last page ended in (or the client's position is in): a page that
        // starts with it continues that row.
        let mut last_row: Option<Vec<u8>> = after.as_ref().map(|p| p.row().to_vec());
        // The page the client hasn't taken yet, for the idle timeout to take back.
        let mut queued: Option<PageSlot> = None;
        loop {
            let Some(space) = room(&pages, &mut queued, limits.stream_idle).await? else {
                return Ok(());
            };
            let limit = remaining.map(|n| n.saturating_add(u64::from(last_row.is_some())));
            if let Some(p) = &pinned {
                p.check()?;
            }
            let at = pinned.as_ref().map(|p| p.snapshot().clone());
            let (table, req, pos) = (table.clone(), Arc::clone(&r), after.take());
            let thread = Arc::clone(&reads)
                .acquire_owned()
                .await
                .map_err(|_| ApiError::internal("the scan threads closed"))?;
            let (mut rows, mut next, mut bytes, cut) = blocking(&pool, move || {
                let select = convert::Select::new(req.selection.as_ref())?.at(at);
                let mut scan = select.scan(&table, req.range.as_ref())?;
                if let Some(n) = limit {
                    scan = scan.limit(n);
                }
                if let Some(p) = &pos {
                    scan = scan.resume_after(p);
                }
                let page = scan.read_within(budget)?;
                let q = select.aggregate();
                let rows: Vec<proto::Row> = page
                    .rows()
                    .iter()
                    .map(|row| convert::row(row.view(), q))
                    .collect();
                Ok((
                    rows,
                    page.resume().cloned(),
                    page.bytes(),
                    page.cut_by_budget(),
                ))
            })
            .await?;
            drop(thread);
            let continued = matches!(
                (&last_row, rows.first()),
                (Some(p), Some(first)) if first.key[..] == p[..]
            );
            if let Some(n) = remaining {
                let new_rows = rows.len() as u64 - u64::from(continued);
                if new_rows > n {
                    // More rows than remain: the ones past the limit go, and every row kept
                    // is whole, since another row followed it.
                    rows.truncate(
                        usize::try_from(n).unwrap_or(usize::MAX) + usize::from(continued),
                    );
                    bytes = rows.iter().map(row_bytes).sum();
                    next = None;
                } else if new_rows == n && !cut {
                    // The limit (or the range's end) stopped the page, after a whole row.
                    next = None;
                }
                // Otherwise the budget stopped the page, perhaps inside its last row: the
                // next read (limit 1, for the continued row) finishes it.
                remaining = Some(n.saturating_sub(new_rows));
            }
            if let Some(last) = rows.last() {
                last_row = Some(last.key.to_vec());
            }
            // An empty page with a position (a budget too small for anything) ends the
            // stream too, with the position, rather than reading the same nothing again: only
            // an empty `next` says the scan finished (simp724's D2 on #1510).
            let end = next.is_none() || rows.is_empty();
            let page = proto::ScanPage {
                rows,
                next: next
                    .as_ref()
                    .map(|p| p.to_bytes().into())
                    .unwrap_or_default(),
                bytes,
            };
            // The page's share of the budget, capped so one scan can't take it all: a page
            // over `--max-request-bytes` (one huge cell) takes the cap.
            let share = usize::try_from(page.bytes)
                .unwrap_or(usize::MAX)
                .min(limits.max_request_bytes);
            let paid = budget_within(&inflight, share, limits.request_timeout).await?;
            let slot: PageSlot = Arc::new(std::sync::Mutex::new(Some((page, paid))));
            space.send(Arc::clone(&slot));
            queued = Some(slot);
            if end {
                // The stream ends once the client has the last page, or the idle timeout
                // takes it back: an unread page doesn't keep its budget.
                room(&pages, &mut queued, limits.stream_idle).await?;
                return Ok(());
            }
            after = next;
        }
    }
}

/// Room in `pages` for the next page, once the client has taken the last one; `None` if the
/// client went away. A client that stays connected without reading for `idle` loses its
/// unread page (`queued`), whose budget comes back, and the stream ends with
/// `DeadlineExceeded`: the last page it did take has the position to resume from.
async fn room<'a>(
    pages: &'a mpsc::Sender<PageSlot>,
    queued: &mut Option<PageSlot>,
    idle: Duration,
) -> Result<Option<mpsc::Permit<'a, PageSlot>>, ApiError> {
    match tokio::time::timeout(idle, pages.reserve()).await {
        Ok(Ok(room)) => Ok(Some(room)),
        Ok(Err(_)) => Ok(None),
        Err(_) => {
            if let Some(slot) = queued.take() {
                drop(slot.lock().ok().and_then(|mut s| s.take()));
            }
            Err(ApiError::deadline(format!(
                "the client took no scan page for --stream-idle-ms ({} ms); resume from the \
                 last page's next",
                idle.as_millis()
            )))
        }
    }
}

/// A table as `Schema` describes it: its families with every option as stored.
fn describe(t: &Table) -> proto::TableResponse {
    proto::TableResponse {
        name: t.name().to_owned(),
        families: t
            .families()
            .into_iter()
            .filter_map(|f| t.family_info(f))
            .map(|i| convert::family_out(&i))
            .collect(),
    }
}

/// A row's size by D308's measure as the converted cells give it: its key and its cells.
fn row_bytes(row: &proto::Row) -> u64 {
    row.key.len() as u64 + row.cells.iter().map(convert::cell_bytes).sum::<u64>()
}

/// Takes `size` bytes of `inflight`, waiting at most `wait`.
async fn budget_within(
    inflight: &Arc<Semaphore>,
    size: usize,
    wait: Duration,
) -> Result<OwnedSemaphorePermit, ApiError> {
    let n = u32::try_from(size.max(1)).unwrap_or(u32::MAX);
    match tokio::time::timeout(wait, Arc::clone(inflight).acquire_many_owned(n)).await {
        Ok(Ok(p)) => Ok(p),
        Ok(Err(_)) => Err(ApiError::internal("the in-flight budget closed")),
        Err(_) => Err(ApiError::overloaded(
            "the server's in-flight request bytes stayed full past the deadline",
        )),
    }
}
