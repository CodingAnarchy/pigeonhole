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
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};

use super::auth::{Caller, Scope};
use super::config::Limits;
use super::convert;
use super::errors::ApiError;
use super::proto;
use super::sessions::{SnapshotGuard, Taken};

/// A page's size when a request leaves it at 0 (D313 §4: 1 MiB), and the cap on a request's
/// own.
const DEFAULT_PAGE_BYTES: u64 = 1 << 20;
pub(crate) const MAX_PAGE_BYTES: u64 = 8 << 20;

/// `Vectors.Nearest`'s largest `k`: the engine keeps `k` candidates while it ranks, before
/// the reply's byte budget cuts it (simp724's L1 on #1565).
pub(crate) const MAX_NEAREST_K: u32 = 10_000;

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
    CreateIndex(proto::CreateVectorIndexRequest),
    VectorInsert(proto::VectorInsertRequest),
    VectorDelete(proto::VectorDeleteRequest),
    VectorSearch(proto::VectorSearchRequest),
    VectorVerify(proto::VectorVerifyRequest),
    /// A client stream: its first message's index and count, and its messages after.
    VectorBulkLoad(super::bulk::BulkLoadOp),
    Promote(proto::Empty),
    Follow(proto::FollowRequest),
    TextCreateIndex(proto::CreateTextIndexRequest),
    TextPut(proto::TextPutRequest),
    TextDelete(proto::TextDeleteRequest),
    TextSearch(proto::TextSearchRequest),
    TextRebuild(proto::TextIndexRequest),
    TextVerify(proto::TextIndexRequest),
    Remember(proto::RememberRequest),
    Recall(proto::RecallRequest),
    MarkUsed(proto::MarkUsedRequest),
    Supersede(proto::SupersedeRequest),
    Link(proto::LinkRequest),
    Forget(proto::ForgetRequest),
    MemoryWatch(proto::MemoryWatchRequest),
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
    VectorSearch(proto::VectorSearchResponse),
    VectorVerify(proto::VectorVerifyResponse),
    Promote(proto::PromoteResponse),
    Follow(super::replica::FollowBatches),
    TextSearch(proto::TextSearchResponse),
    TextRebuild(proto::TextRebuildResponse),
    TextVerify(proto::TextVerifyResponse),
    Remember(proto::RememberResponse),
    Recall(proto::RecallResponse),
    MarkUsed(proto::MarkUsedResponse),
    Forget(proto::ForgetResponse),
    MemoryWatch(super::memory::MemoryEvents),
    /// A unary read's reply with its share of `--inflight-bytes` (#1567): held until the
    /// reply's body has been written ([`super::charge`]).
    Charged(Box<ServiceReply>, super::charge::Charge),
}

impl ServiceReply {
    /// The reply, and its charge if it has one.
    pub(crate) fn uncharge(self) -> (ServiceReply, Option<super::charge::Charge>) {
        match self {
            ServiceReply::Charged(r, c) => (*r, Some(c)),
            r => (r, None),
        }
    }

    /// A charged reply's message size, encoded: what a gRPC reply is charged (#1567).
    fn encoded_len(&self) -> usize {
        use prost::Message;
        match self {
            ServiceReply::Get(r) => r.encoded_len(),
            ServiceReply::MultiGet(r) => r.encoded_len(),
            ServiceReply::ReadRow(r) => r.encoded_len(),
            ServiceReply::Traverse(r) => r.encoded_len(),
            ServiceReply::Nearest(r) => r.encoded_len(),
            ServiceReply::Recall(r) => r.encoded_len(),
            _ => 0,
        }
    }
}

/// A page waiting for its client, with its share of the in-flight budget (perf287's G1 on
/// #1510). The scan task keeps a handle on it, so a client that stops reading loses the page
/// and the budget comes back (G2): the page is taken out of the slot by whichever comes first,
/// the client or the task's idle timeout.
pub(crate) type PageSlot = Arc<std::sync::Mutex<Option<(proto::ScanPage, OwnedSemaphorePermit)>>>;

/// A scan's pages as the scan task reads them, then its error if it failed. The task reads a
/// page ahead at most, so a slow client holds back the reading, not the memory. A page's
/// budget is released when the page is handed to the transport (this stream yields it).
#[derive(Debug)]
pub(crate) struct ScanPages {
    pages: mpsc::Receiver<PageSlot>,
    /// The error the stream ends with, sent before `pages` closes.
    end: Option<oneshot::Receiver<ApiError>>,
}

impl ScanPages {
    /// A stream of the pages `pages` sends, then the error `end` sends, if any: for a
    /// stream built elsewhere, a cluster's fanned-out scan (D317 ticket 4).
    pub(crate) fn new(pages: mpsc::Receiver<PageSlot>, end: oneshot::Receiver<ApiError>) -> Self {
        Self {
            pages,
            end: Some(end),
        }
    }

    /// The next page with its share of the in-flight budget, which the caller then holds: a
    /// fanned-out scan's entry node sends a local partition's page on as its own, with the
    /// share it already has (#1753). Given back and taken again, the share could go to the
    /// partition's next page in between, and the entry's page would then wait for bytes held
    /// by the page only the entry could take.
    pub(crate) fn poll_next_charged(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(proto::ScanPage, OwnedSemaphorePermit), ApiError>>> {
        loop {
            match self.pages.poll_recv(cx) {
                Poll::Ready(Some(slot)) => {
                    let taken = slot.lock().ok().and_then(|mut s| s.take());
                    if let Some(page) = taken {
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

impl futures_core::Stream for ScanPages {
    type Item = Result<proto::ScanPage, ApiError>;

    /// The next page; its share of the budget goes back as it's handed to the transport.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .poll_next_charged(cx)
            .map(|next| next.map(|page| page.map(|(page, _share)| page)))
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
            Self::CreateIndex(_) => 29,
            Self::VectorInsert(_) => 30,
            Self::VectorDelete(_) => 31,
            Self::VectorSearch(_) => 32,
            Self::VectorVerify(_) => 33,
            Self::VectorBulkLoad(_) => 34,
            Self::Promote(_) => 35,
            Self::Follow(_) => FOLLOW,
            Self::TextCreateIndex(_) => 37,
            Self::TextPut(_) => 38,
            Self::TextDelete(_) => 39,
            Self::TextSearch(_) => 40,
            Self::TextRebuild(_) => 41,
            Self::TextVerify(_) => 42,
            Self::Remember(_) => 43,
            Self::Recall(_) => 44,
            Self::MarkUsed(_) => 45,
            Self::Supersede(_) => 46,
            Self::Link(_) => 47,
            Self::Forget(_) => 48,
            Self::MemoryWatch(_) => MEMORY_WATCH,
        }
    }

    /// The RPC's name, `Service.Method`, for logs.
    /// What [`Service::admit`] checks this call against.
    pub(crate) fn admit(&self) -> Admit {
        Admit {
            what: self.name(),
            scope: self.scope(),
            writes: self.writes(),
            size: self.size(),
        }
    }

    pub(crate) fn name(&self) -> &'static str {
        RPCS[self.index()]
    }

    pub(crate) fn scope(&self) -> Scope {
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
            | Self::Nearest(_)
            | Self::VectorSearch(_)
            | Self::VectorVerify(_)
            | Self::TextSearch(_)
            | Self::TextVerify(_)
            | Self::Recall(_)
            | Self::MemoryWatch(_) => Scope::Read,
            // An index is a table: creating one is a write, as `Schema.CreateTable` is.
            Self::CreateIndex(_)
            | Self::VectorInsert(_)
            | Self::VectorDelete(_)
            | Self::VectorBulkLoad(_)
            // A text index is a table too; a rebuild rewrites it from its source.
            | Self::TextCreateIndex(_)
            | Self::TextPut(_)
            | Self::TextDelete(_)
            | Self::TextRebuild(_)
            // Memory writes, `Forget` included: forgetting is ordinary agent behaviour, and
            // a token's namespaces bound its reach (ai1249 on D313 §7).
            | Self::Remember(_)
            | Self::MarkUsed(_)
            | Self::Supersede(_)
            | Self::Link(_)
            | Self::Forget(_) => Scope::Write,
            // Maintenance and backups: admin (ICR 0112's Admin).
            // A replica's bootstrap and follow read everything, ids and all (#1436).
            Self::Flush(_)
            | Self::Compact(_)
            | Self::Backup(_)
            | Self::BackupDownload(_)
            | Self::Promote(_)
            | Self::Follow(_) => Scope::Admin,
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
    pub(crate) fn size(&self) -> usize {
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
            Self::Info(_)
            | Self::Flush(_)
            | Self::Compact(_)
            | Self::Metrics(_)
            | Self::Promote(_) => 0,
            Self::Backup(r) => r.encoded_len(),
            Self::BackupDownload(r) => r.encoded_len(),
            Self::Traverse(r) => r.encoded_len(),
            Self::Nearest(r) => r.encoded_len(),
            Self::CreateIndex(r) => r.encoded_len(),
            Self::VectorInsert(r) => r.encoded_len(),
            Self::VectorDelete(r) => r.encoded_len(),
            Self::VectorSearch(r) => r.encoded_len(),
            Self::VectorVerify(r) => r.encoded_len(),
            // The messages hold their own budget, each until the loader takes it.
            Self::VectorBulkLoad(op) => op.index.len() + 16,
            Self::Follow(r) => r.encoded_len(),
            Self::TextCreateIndex(r) => r.encoded_len(),
            Self::TextPut(r) => r.encoded_len(),
            Self::TextDelete(r) => r.encoded_len(),
            Self::TextSearch(r) => r.encoded_len(),
            Self::TextRebuild(r) | Self::TextVerify(r) => r.encoded_len(),
            Self::Remember(r) => r.encoded_len(),
            Self::Recall(r) => r.encoded_len(),
            Self::MarkUsed(r) => r.encoded_len(),
            Self::Supersede(r) => r.encoded_len(),
            Self::Link(r) => r.encoded_len(),
            Self::Forget(r) => r.encoded_len(),
            Self::MemoryWatch(r) => r.encoded_len(),
        }
    }

    /// Whether the call writes or changes the schema: a replica refuses it (#1436). Derived
    /// from the scope, so a new write RPC is covered without being listed here (cli692's F2
    /// on #1583): every write-scope RPC writes, and `DropTable` is the one admin-scope
    /// write. A transaction's `Commit` is read scope; one with writes is refused by the
    /// engine's gate (R6).
    pub(crate) fn writes(&self) -> bool {
        self.scope() == Scope::Write || matches!(self, Self::DropTable(_))
    }
}

/// `Data.Scan`'s index in `RPCS`: its outcome is counted when its stream ends.
pub(crate) const SCAN: usize = 7;

/// The RPCs, by `ServiceOp::index`.
/// `Feed.Subscribe`'s index in `RPCS`: counted when its stream ends, as a scan's is.
pub(crate) const FEED: usize = 16;

/// What [`Service::admit`] checks a call against: its name (for the refusal's message), the
/// scope it needs, whether it writes, and its request's size.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Admit {
    pub what: &'static str,
    pub scope: Scope,
    pub writes: bool,
    pub size: usize,
}

/// `Admin.BackupDownload`'s index in `RPCS`: counted when its stream ends.
pub(crate) const DOWNLOAD: usize = 25;

/// How often a download sends a heartbeat chunk while its copy is written (#1611): well
/// inside any client's idle limit.
const HEARTBEAT: Duration = Duration::from_secs(1);

/// The engine metrics' attribute naming a cluster node's partition (D317 ticket 8b).
pub(crate) const PARTITION_ATTRIBUTE: &str = "pigeonhole.partition";

/// A backup's partition named on a single node, which has none (D317 ticket 8b).
fn single_node_partition() -> ApiError {
    ApiError::invalid("this server isn't a cluster node: a backup names no partition here")
}

/// A last chunk's identity fields (D317 ticket 8b), empty on a single node.
fn identity_fields(id: Option<&super::backup::CopyIdentity>) -> proto::BackupChunk {
    let Some(id) = id else {
        return proto::BackupChunk::default();
    };
    proto::BackupChunk {
        partition: id.partition.clone(),
        cluster_id: id.cluster_id.clone(),
        epoch: id.epoch,
        map_version: id.map_version,
        ..Default::default()
    }
}

/// A download's offset past its copy's end.
/// Sends `kept`'s bytes from `start` in [`backup::CHUNK`](super::backup::CHUNK) chunks with
/// `send`, the last carrying the total, the SHA-256 and the identity: a download's copy, or a
/// closed partition's file (D317 ticket 6). Stops early, `Ok`, once the client went away.
async fn send_kept<F, Fut>(
    kept: &super::backup::Kept,
    start: u64,
    id: &str,
    pool: &Arc<Semaphore>,
    send: F,
) -> Result<(), ApiError>
where
    F: Fn(proto::BackupChunk) -> Fut,
    Fut: Future<Output = Result<bool, ApiError>>,
{
    let mut at = start;
    loop {
        let path = kept.path.clone();
        let data = blocking(pool, move || super::backup::read_chunk(&path, at)).await?;
        // A file that isn't its listed size any more (a closed partition's file, which goes
        // as it lies, changed after its close) fails the download: short, an empty read
        // would never reach `total`; long, bytes past it would go out (#1729).
        let left = kept.total - at;
        if (data.is_empty() && left > 0) || data.len() as u64 > left {
            return Err(ApiError::failed_precondition(format!(
                "the file changed after it was taken: it isn't its {} bytes any more (a \
                 closed partition's file: close it again)",
                kept.total
            )));
        }
        let next = at + data.len() as u64;
        let last = next >= kept.total;
        let chunk = proto::BackupChunk {
            data: data.into(),
            total: if last { kept.total } else { 0 },
            backup_id: id.to_owned(),
            offset: at,
            sha256: if last {
                kept.sha256.to_vec().into()
            } else {
                Default::default()
            },
            feed_token: kept.feed_token.clone().into(),
            ..identity_fields(last.then_some(kept.identity.as_ref()).flatten())
        };
        if !send(chunk).await? || last {
            return Ok(());
        }
        at = next;
    }
}

fn past_the_copy(offset: u64, total: u64) -> ApiError {
    ApiError::invalid(format!(
        "offset {offset} is past the backup's {total} bytes"
    ))
}

/// `Replication.Follow`'s index in `RPCS`: counted when its stream ends.
pub(crate) const FOLLOW: usize = 36;

/// `Memory.Watch`'s index in `RPCS`.
pub(crate) const MEMORY_WATCH: usize = 49;

pub(crate) const RPCS: [&str; 50] = [
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
    "Vectors.CreateIndex",
    "Vectors.Insert",
    "Vectors.Delete",
    "Vectors.Search",
    "Vectors.Verify",
    "Vectors.BulkLoad",
    "Admin.Promote",
    "Replication.Follow",
    "Text.CreateIndex",
    "Text.Put",
    "Text.Delete",
    "Text.Search",
    "Text.Rebuild",
    "Text.Verify",
    "Memory.Remember",
    "Memory.Recall",
    "Memory.MarkUsed",
    "Memory.Supersede",
    "Memory.Link",
    "Memory.Forget",
    "Memory.Watch",
];

/// The outcomes a request is counted under: `ok`, every code ICR 0112's table names, and
/// `other` for a code a newer engine adds.
pub(crate) const OUTCOMES: [&str; 50] = [
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
    "FailedPrecondition",
    "ReplicaBehind",
    "Unavailable",
    "Internal",
    // A cluster node's (D317, ICR 0117), counted apart so an operator sees them (cli692's
    // F1 on ticket 2).
    "WrongPartition",
    "CrossPartition",
    "PartitionUnavailable",
    "Fenced",
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

    /// Prometheus text: the server's counters. The engine's metrics, `async_sync_reads`
    /// (perf287's note 2 on D313) among them, are `Partition::engine_metrics`'.
    pub(crate) fn render(&self) -> String {
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
        out
    }
}

/// The session table's gauges (D313 §3): open sessions by kind, and the oldest open one's age,
/// since a held transaction or snapshot holds back compaction's version GC and blob GC. A
/// client that leaks sessions shows up here. Ticket 9 moves these onto D316's names.
pub(crate) fn render_sessions(out: &mut String, tables: &[&super::sessions::Sessions]) {
    use std::fmt::Write as _;
    // Every table on the node: a cluster node's partitions each have their own (D317 ticket
    // 8a), counted together as the caps count them (cli692's F1 on #1718).
    let now = Instant::now();
    let mut s = super::sessions::Stats::default();
    for t in tables {
        let one = t.stats(now);
        s.transactions += one.transactions;
        s.snapshots += one.snapshots;
        s.tombstones += one.tombstones;
        s.mcp += one.mcp;
        s.oldest = s.oldest.max(one.oldest);
    }
    let _ = writeln!(out, "# TYPE pigeonhole_serve_sessions gauge");
    for (kind, n) in [
        ("transaction", s.transactions),
        ("snapshot", s.snapshots),
        ("tombstone", s.tombstones),
        ("mcp", s.mcp),
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

/// `Vectors.BulkLoad`'s gauges (perf287's B3 on 7b2): the running load's phase, and the
/// vectors and message bytes handed to it, an upper bound on what it holds.
fn render_bulk(out: &mut String, parts: &[(Option<&str>, &Partition)]) {
    for (name, read) in [
        (
            "pigeonhole_serve_bulk_load_phase",
            (|g: &super::bulk::Gauges| g.phase.load(Ordering::Relaxed)) as fn(&_) -> u64,
        ),
        ("pigeonhole_serve_bulk_load_vectors", |g| {
            g.vectors.load(Ordering::Relaxed)
        }),
        ("pigeonhole_serve_bulk_load_bytes", |g| {
            g.bytes.load(Ordering::Relaxed)
        }),
    ] {
        render_family(
            out,
            name,
            "gauge",
            parts
                .iter()
                .map(|(id, part)| (*id, read(&part.bulk.gauges))),
        );
    }
}

/// A cluster node's partitions' unpinned engine threads (D317 ticket 8a's pinning check):
/// shard drivers and compaction threads apart (cli692's note on the 8a plan). A single
/// node's output doesn't change: they're labelled partitions' only.
fn render_pinning(out: &mut String, parts: &[(Option<&str>, &Partition)]) {
    let labelled = || parts.iter().filter(|(id, _)| id.is_some());
    render_family(
        out,
        "pigeonhole_serve_partition_unpinned_shard_threads",
        "gauge",
        labelled().map(|(id, p)| (*id, p.pinning.shards_unpinned() as u64)),
    );
    render_family(
        out,
        "pigeonhole_serve_partition_unpinned_compaction_threads",
        "gauge",
        labelled().map(|(id, p)| (*id, p.pinning.compaction_unpinned() as u64)),
    );
    // Each partition's engine memory as resolved (perf287's A3 on the 8a plan: 8b's gate
    // checks the totals).
    for (name, read) in [
        (
            "pigeonhole_serve_partition_memtable_bytes",
            (|m: &super::placement::Memory| m.memtables()) as fn(&_) -> u64,
        ),
        ("pigeonhole_serve_partition_block_cache_bytes", |m| {
            m.block_cache
        }),
        ("pigeonhole_serve_partition_row_cache_bytes", |m| {
            m.row_cache
        }),
    ] {
        render_family(
            out,
            name,
            "gauge",
            labelled().filter_map(|(id, p)| p.engine_memory.as_ref().map(|m| (*id, read(m)))),
        );
    }
}

/// One serve metric's `# TYPE` once (the exposition format allows it once), then a sample
/// per partition: labelled `pigeonhole_partition` on a cluster node (D317 ticket 8b, as the
/// engine's `pigeonhole.partition`), unlabelled on a single node. No partitions, no lines.
fn render_family<'a>(
    out: &mut String,
    name: &str,
    kind: &str,
    samples: impl Iterator<Item = (Option<&'a str>, u64)>,
) {
    use std::fmt::Write as _;
    let mut samples = samples.peekable();
    if samples.peek().is_none() {
        return;
    }
    let _ = writeln!(out, "# TYPE {name} {kind}");
    for (id, v) in samples {
        match id {
            Some(id) => {
                let _ = write!(out, "{name}{{pigeonhole_partition=\"");
                escape_label(out, id);
                let _ = writeln!(out, "\"}} {v}");
            }
            None => {
                let _ = writeln!(out, "{name} {v}");
            }
        }
    }
}

/// `v` as a Prometheus label value: backslash, quote and newline escaped.
fn escape_label(out: &mut String, v: &str) {
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
}

/// The feed fan-out's gauges and counters (D313 R2; perf287's notes on ticket 5).
pub(crate) fn render_feeds(out: &mut String, parts: &[(Option<&str>, &Partition)]) {
    type Read = fn(&super::feed::Feeds) -> u64;
    let families: [(&str, &str, Read); 9] = [
        ("pigeonhole_serve_feed_hubs", "gauge", |f| {
            f.gauges().hubs as u64
        }),
        ("pigeonhole_serve_feed_ring_bytes", "gauge", |f| {
            f.gauges().ring_bytes as u64
        }),
        ("pigeonhole_serve_feed_catching_up", "gauge", |f| {
            f.gauges().catching_up as u64
        }),
        (
            "pigeonhole_serve_feed_catch_up_waits_total",
            "counter",
            |f| f.stats.catch_up_waits.load(Ordering::Relaxed),
        ),
        ("pigeonhole_serve_feed_fell_behind_total", "counter", |f| {
            f.stats.fell_behind.load(Ordering::Relaxed)
        }),
        ("pigeonhole_serve_feed_rejoined_total", "counter", |f| {
            f.stats.rejoined.load(Ordering::Relaxed)
        }),
        // #1689's fast path: hubs on the feed runtime beside their one client, and moves.
        ("pigeonhole_serve_feed_solo_hubs", "gauge", |f| {
            f.gauges().solo_hubs as u64
        }),
        ("pigeonhole_serve_feed_hub_moves_total", "counter", |f| {
            f.stats.hub_moves.load(Ordering::Relaxed)
        }),
        ("pigeonhole_serve_feed_hub_failures_total", "counter", |f| {
            f.stats.hub_failures.load(Ordering::Relaxed)
        }),
    ];
    for (name, kind, read) in families {
        render_family(
            out,
            name,
            kind,
            parts.iter().map(|(id, part)| (*id, read(&part.feeds))),
        );
    }
}

/// The service's open tables by name, and the generation
/// [`tables_changed`](Service::tables_changed) bumps (#1632).
#[derive(Clone, Default)]
struct TableCache {
    generation: u64,
    map: HashMap<String, Table>,
}

/// The feed fan-out's limits, from the server's.
fn feed_limits(limits: &Limits) -> super::feed::FeedLimits {
    super::feed::FeedLimits {
        ring_bytes: limits.feed_ring_bytes,
        max_hubs: limits.max_feed_hubs,
        max_catch_up: limits.max_catch_up,
        quiet: limits.feed_quiet,
        stream_idle: limits.stream_idle,
        client_events: limits.feed_client_events,
        client_bytes: limits.feed_client_bytes,
        solo_after: super::feed::SOLO_AFTER,
    }
}

/// The database, its open tables, and the limits every call passes through.
pub(crate) struct Service {
    /// The in-flight bytes budget (R3), in bytes.
    inflight: Arc<Semaphore>,
    /// Blocking-pool calls running at once.
    blocking: Arc<Semaphore>,
    /// Scans streaming at once (`--max-scans`).
    scans: Arc<Semaphore>,
    /// Transactions and pinned snapshots between requests (D313 §3).
    pub(crate) sessions: Arc<super::sessions::Sessions>,
    /// The node's session counts, shared by its partitions' tables (D317 ticket 8a).
    session_caps: Arc<super::sessions::Caps>,
    /// The node's feed limits' shared state, every partition's feeds' (D317 ticket 8a).
    feed_shared: super::feed::FeedShared,
    /// Feed and watch streams at once (`--max-streams`): a stream outlives its request's
    /// `--max-concurrent` slot, as a scan does.
    streams: Arc<Semaphore>,
    /// `Text.Rebuild`s at once: one, on a thread of its own.
    text_rebuilds: Arc<Semaphore>,
    /// Memory calls at once: a sub-limit of the blocking pool, so a slow embedding endpoint
    /// can't take every blocking thread (ai1249's M6 on D313 §7).
    memory_calls: Arc<Semaphore>,
    /// Scan page reads at once: half the blocking pool, at least 1, so scans can't take
    /// every blocking thread from the other calls (perf287's G3 on #1510).
    scan_reads: Arc<Semaphore>,
    limits: Limits,
    pub(crate) metrics: Arc<Metrics>,
    /// The open connections against `--max-connections` (#1604): the listener counts them,
    /// the metrics show them.
    pub(crate) connections: Arc<super::listener::Connections>,
    /// Set on a replica server (`--replica-of`, #1436). A whole-node mode today, refused with
    /// `--cluster`; a cluster's replicas are per partition, from D317 ticket 8.
    pub(crate) replica: Option<Arc<super::replica::Replica>>,
    /// Where the server's own threads run (none in tests).
    placement: Option<Arc<super::placement::Placement>>,
    /// The unary read replies' charges (#1567).
    pub(crate) replies: Arc<super::charge::ReplyStats>,
    /// A cluster partition's write gate (D317 ticket 6): its writes answer `Fenced` while
    /// it's fenced. The node's fences hold it too. `None` on a single node and on the
    /// node's routing service.
    gate: Option<Arc<super::gate::WriteGate>>,
    /// The partition served here: the database and what's opened on it (D317's S1, cli692's
    /// split of node and partition state). `None` on a cluster node the map gives no
    /// partition, which only routes (D317 ticket 2); always `Some` on a single node.
    part: Option<Partition>,
}

/// How a partition's engine threads were placed when it opened (D317 ticket 8a, perf287's
/// P1): its shard driver threads and compaction threads, each pinned to a CPU of its own,
/// confined to the node's compaction CPUs (ICR 0134), or unpinned.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Pinning {
    pub shards: usize,
    pub shards_pinned: usize,
    pub compaction: usize,
    pub compaction_pinned: usize,
    pub compaction_confined: usize,
}

impl Pinning {
    /// `db`'s, opened with `compaction` compaction threads.
    pub(crate) fn of(db: &Pigeonhole, compaction: usize) -> Self {
        let m = db.engine_metrics();
        let started = usize::try_from(m.threads_started).unwrap_or(usize::MAX);
        Self {
            shards: started.saturating_sub(compaction),
            shards_pinned: db.pinned_shard_cpus().len(),
            compaction,
            compaction_pinned: db.pinned_compaction_cpus().len(),
            compaction_confined: usize::try_from(m.threads_confined).unwrap_or(usize::MAX),
        }
    }

    pub(crate) fn shards_unpinned(&self) -> usize {
        self.shards.saturating_sub(self.shards_pinned)
    }

    /// Compaction threads neither pinned nor confined: on any CPU, a shard's or the
    /// network's included.
    pub(crate) fn compaction_unpinned(&self) -> usize {
        self.compaction
            .saturating_sub(self.compaction_pinned + self.compaction_confined)
    }
}

/// What a node opens on the database it serves: the engine, the table cache, the indexes,
/// the feeds, the memory store and the bulk loads (D317 ticket 2, cli692's S1). The node's
/// own state (connections, budgets, pools, sessions, limits, metrics) stays on [`Service`].
pub(crate) struct Partition {
    pub(crate) db: Pigeonhole,
    /// Read on every call, changed only when a table is first used: a snapshot swapped
    /// whole, never a lock (perf287's note 3 on D313).
    tables: ArcSwap<TableCache>,
    /// The feed fan-out's hubs (D313 R2).
    pub(crate) feeds: Arc<super::feed::Feeds>,
    /// How its engine's threads were placed when it opened (D317 ticket 8a's pinning
    /// check): a cluster node's gauges.
    pub(crate) pinning: Pinning,
    /// Its engine memory as resolved at open (D317 ticket 8a's `--engine-memory`): a cluster
    /// node's gauges.
    pub(crate) engine_memory: Option<super::placement::Memory>,
    /// `--backup-dir`, and the backup downloads kept for resumes.
    ///
    /// **The node's one** (simp724's N1 on #1711): `Backups::new` clears
    /// `.phdb-serve-downloads` at startup and counts `--max-backups` itself. A second one per
    /// partition (several partitions on one node, D317 8a; a partition opened at runtime,
    /// ticket 6) would delete another partition's live downloads and make the limit per
    /// partition. Partitions share the node's `Arc`.
    backups: Arc<super::backup::Backups>,
    /// The vector indexes open, and their rebalancing threads.
    pub(crate) indexes: Arc<super::indexes::Indexes>,
    /// `Vectors.BulkLoad`'s one load at a time, on a thread of its own.
    pub(crate) bulk: super::bulk::BulkLoads,
    /// The text indexes open, and the running rebuild.
    pub(crate) texts: Arc<super::texts::Texts>,
    /// The memory store (`--memory`), if the server serves one: opened when a call needs
    /// it, and again after the catalog or the engine changes (#1618).
    memory: Option<Arc<super::memslot::MemSlot>>,
    /// Held across a table's create or drop and the cache update after it, so the cache ends
    /// at the live table whatever the order (simp724's C1 on #1527).
    ///
    /// **Lock order: a blocking-pool place first, then this.** A `/mcp` create holds its
    /// request's pool thread when it takes this, so an async create or drop takes its pool
    /// place before this, never while holding it (ai1249's B2 on #1666).
    pub(crate) schema: tokio::sync::Mutex<()>,
    /// The engine's metrics on `/metrics`, under D316's names (D323): the same reading and
    /// carrying as `pigeonhole-otel`'s OpenTelemetry instruments, so its counters stay
    /// monotonic across an automatic recovery and a replica's re-bootstrap. A node that only
    /// routes has none (D317 ticket 2).
    engine_metrics: pigeonhole_otel::Collector,
    /// The catalog's generation: bumped by a create, a drop, and a replica's catalog
    /// change. A `/mcp` session that saw an older one checks its tool list (ai1249's M2 on
    /// D313: `create_table` in one session can bring `nearest` to another).
    catalog: AtomicU64,
}

impl Partition {
    /// What a node opens on `db`, before any table is used: partition `id` of a cluster
    /// node, whose engine metrics carry it as `pigeonhole.partition` (D317 ticket 8b).
    fn new(
        db: Pigeonhole,
        id: Option<&str>,
        limits: &Limits,
        feeds: &super::feed::FeedShared,
    ) -> Self {
        let metrics = match id {
            Some(id) => pigeonhole_otel::Config::new().attribute(PARTITION_ATTRIBUTE, id),
            None => pigeonhole_otel::Config::new(),
        };
        Self {
            tables: ArcSwap::from_pointee(TableCache::default()),
            pinning: Pinning::default(),
            engine_memory: None,
            backups: super::backup::Backups::new(None, 1).expect("no directory to check"),
            indexes: Arc::new(super::indexes::Indexes::new(db.clone(), None, None)),
            bulk: super::bulk::BulkLoads::default(),
            texts: Arc::new(super::texts::Texts::new(db.clone())),
            memory: None,
            schema: tokio::sync::Mutex::new(()),
            catalog: AtomicU64::new(0),
            engine_metrics: metrics.collector(&db),
            feeds: super::feed::Feeds::new(db.clone(), feed_limits(limits), feeds),
            db,
        }
    }
}

impl std::fmt::Debug for Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Service")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl Service {
    /// The partition served here. A cluster node the map gives no partition only routes:
    /// the router forwards or refuses every call that needs one before it reaches the
    /// service, so this answers only a call that slipped past it, a server bug. `Internal`,
    /// not a retryable code, so a client doesn't loop on it (cli692's note 1 on d0c6312a).
    pub(crate) fn part(&self) -> Result<&Partition, ApiError> {
        self.part_opt().ok_or_else(|| {
            ApiError::internal("this cluster node only routes and serves no partition")
        })
    }

    /// `--request-timeout-ms`: the bound a forwarded call gets (D317 ticket 2).
    /// The in-flight bytes budget (R3), which a cluster's fanned-out scan pages count
    /// against (D317 ticket 4).
    pub(crate) fn inflight(&self) -> &Arc<Semaphore> {
        &self.inflight
    }

    pub(crate) fn limits(&self) -> &Limits {
        &self.limits
    }

    pub(crate) fn request_timeout(&self) -> std::time::Duration {
        self.limits.request_timeout
    }

    /// The partition served here, if any: for the node's own work (health, metrics,
    /// shutdown), which a node that only routes does without one.
    pub(crate) fn part_opt(&self) -> Option<&Partition> {
        self.part.as_ref()
    }

    /// The partition, to set a test's hold on it before the server starts.
    #[cfg(test)]
    pub(crate) fn part_mut(&mut self) -> &mut Partition {
        self.part
            .as_mut()
            .expect("a test server serves its partition")
    }

    /// A node serving `db`.
    pub(crate) fn new(
        db: Pigeonhole,
        limits: Limits,
        blocking_threads: usize,
        metrics: Arc<Metrics>,
    ) -> Self {
        let feeds = super::feed::FeedShared::new(limits.max_catch_up);
        let part = Partition::new(db, None, &limits, &feeds);
        Self::node(Some(part), limits, blocking_threads, metrics, feeds)
    }

    /// Partition `id` on `db`, one of this cluster node's (D317 ticket 8a): its own database,
    /// tables, indexes, feeds and session table (ids `<id>:<hex>`), sharing the node's
    /// budgets, pools, connections, metrics, limits and session caps, and the node's one
    /// `backups` (simp724's N1 on #1711). Calls are admitted once, by whichever service
    /// takes them (cli692's Q1).
    pub(crate) fn for_partition(
        &self,
        db: Pigeonhole,
        id: &str,
        backups: &Arc<super::backup::Backups>,
        pinning: Pinning,
        memory: super::placement::Memory,
        gate: Arc<super::gate::WriteGate>,
    ) -> Self {
        let mut part = Partition::new(db, Some(id), &self.limits, &self.feed_shared);
        part.indexes = Arc::new(super::indexes::Indexes::new(
            part.db.clone(),
            None,
            Some(Arc::clone(&gate)),
        ));
        part.backups = Arc::clone(backups);
        part.pinning = pinning;
        part.engine_memory = Some(memory);
        Self {
            inflight: Arc::clone(&self.inflight),
            blocking: Arc::clone(&self.blocking),
            scans: Arc::clone(&self.scans),
            sessions: super::sessions::Sessions::sharing(
                self.limits.leases,
                Some(id.to_owned()),
                Arc::clone(&self.session_caps),
            ),
            session_caps: Arc::clone(&self.session_caps),
            feed_shared: self.feed_shared.clone(),
            streams: Arc::clone(&self.streams),
            text_rebuilds: Arc::clone(&self.text_rebuilds),
            memory_calls: Arc::clone(&self.memory_calls),
            scan_reads: Arc::clone(&self.scan_reads),
            limits: self.limits,
            metrics: Arc::clone(&self.metrics),
            connections: Arc::clone(&self.connections),
            replica: None,
            placement: None,
            replies: Arc::clone(&self.replies),
            gate: Some(gate),
            part: Some(part),
        }
    }

    /// A cluster node the map gives no partition: it only routes (D317 ticket 2).
    pub(crate) fn routing_only(
        limits: Limits,
        blocking_threads: usize,
        metrics: Arc<Metrics>,
    ) -> Self {
        let feeds = super::feed::FeedShared::new(limits.max_catch_up);
        Self::node(None, limits, blocking_threads, metrics, feeds)
    }

    fn node(
        part: Option<Partition>,
        limits: Limits,
        blocking_threads: usize,
        metrics: Arc<Metrics>,
        feed_shared: super::feed::FeedShared,
    ) -> Self {
        let session_caps = Arc::new(super::sessions::Caps::default());
        Self {
            feed_shared,
            connections: super::listener::Connections::new(limits.max_connections),
            replies: Arc::new(super::charge::ReplyStats::default()),
            inflight: Arc::new(Semaphore::new(
                limits.inflight_bytes.min(Semaphore::MAX_PERMITS),
            )),
            blocking: Arc::new(Semaphore::new(blocking_threads.max(1))),
            scans: Arc::new(Semaphore::new(limits.max_scans.max(1))),
            sessions: super::sessions::Sessions::sharing(
                limits.leases,
                None,
                Arc::clone(&session_caps),
            ),
            session_caps,
            streams: Arc::new(Semaphore::new(limits.max_streams.max(1))),
            text_rebuilds: Arc::new(Semaphore::new(1)),
            memory_calls: Arc::new(Semaphore::new((blocking_threads / 2).max(1))),
            scan_reads: Arc::new(Semaphore::new((blocking_threads / 2).max(1))),
            limits,
            metrics,
            replica: None,
            placement: None,
            gate: None,
            part,
        }
    }

    /// Makes this server a replica of `replica`'s primary: writes are refused until it is
    /// promoted. The loop is started by the caller once the server runs.
    pub(crate) fn replica(mut self, replica: Arc<super::replica::Replica>) -> Self {
        self.replica = Some(replica);
        // A replica's rebalancer could only fail (its writes are refused, R6): none runs
        // until a promote (cli692's F3 on #1583).
        if let Some(part) = &self.part {
            part.indexes.pause_rebalancing(true);
        }
        self
    }

    /// Read-your-writes on a replica (#1577): with the client's `min_primary_seqno` (the
    /// `x-pigeonhole-min-primary-seqno` header), a read waits, until `deadline` at most, for
    /// the replica to apply the primary's commits through it. Reads only: a write is refused
    /// on a replica anyway, and `Info` and `Metrics` answer at once (a monitor must see a
    /// lagging replica). A server that isn't a replica, or was promoted, ignores it. The wait
    /// holds the request's slot and budget, as any read in progress does.
    pub(crate) async fn await_primary(
        &self,
        op: &ServiceOp,
        min_primary_seqno: Option<u64>,
        deadline: super::replica::ReadDeadline,
    ) -> Result<(), ApiError> {
        let (Some(seqno), Some(replica)) = (min_primary_seqno, &self.replica) else {
            return Ok(());
        };
        if op.scope() != Scope::Read || matches!(op, ServiceOp::Info(_) | ServiceOp::Metrics(_)) {
            return Ok(());
        }
        replica.wait_applied(seqno, deadline).await
    }

    /// `Admin.Info`'s replica section.
    fn replica_info(&self) -> Option<proto::ReplicaInfo> {
        self.replica.as_ref().and_then(|r| r.info())
    }

    /// The catalog changed under the service (a replica applied a primary's change): the
    /// cached table, vector index and text index handles go, and the next call on each name
    /// opens it
    /// again.
    ///
    /// The memory store is opened again too (#1618). Its clear is synchronous: the replica
    /// publishes the seqno it applied only after this returns, so a read waiting for that
    /// seqno opens the store afresh (cli692's D2).
    pub(crate) fn tables_changed(&self) {
        self.handles_changed();
        if let Some(memory) = self.part.as_ref().and_then(|p| p.memory.as_ref()) {
            memory.clear();
        }
    }

    /// The cached table, vector index and text index handles go: a replica's catalog change,
    /// or a `DropTable` here (#1588), whose name may have been an index's. The next call on
    /// each name opens it again. A forgotten background index's rebalancer sees its index
    /// close and ends. An open running across this keeps nothing (#1632).
    fn handles_changed(&self) {
        let Some(part) = &self.part else { return };
        part.catalog.fetch_add(1, Ordering::Release);
        part.tables.rcu(|c| TableCache {
            generation: c.generation + 1,
            map: HashMap::new(),
        });
        part.indexes.forget();
        part.texts.forget();
    }

    /// The served database was replaced under the handle (#1436's bootstrap): the table
    /// cache, the feed hubs, the memory store's watches and the sessions are of the old
    /// engine, and go.
    pub(crate) fn engine_replaced(&self) {
        self.tables_changed();
        if let Some(part) = &self.part {
            if let Some(memory) = &part.memory {
                memory.replaced();
            }
            part.feeds.fail_all();
        }
        self.sessions.drop_idle();
    }

    /// Takes backups in `dir` (`--backup-dir`), at most `max_backups` at once. Without a
    /// directory, `Backup` and `BackupDownload` are refused. Fails at startup if the
    /// directory, or its download directory, isn't usable.
    pub(crate) fn backups(
        mut self,
        dir: Option<&std::path::Path>,
        max_backups: usize,
    ) -> Result<Self, String> {
        if let Some(part) = &mut self.part {
            part.backups = super::backup::Backups::new(dir, max_backups)?;
            if let Some(p) = &self.placement {
                part.backups.place(p);
            }
        }
        Ok(self)
    }

    /// Runs the threads the server starts on `placement`'s CPUs, off the shards': those of
    /// vector indexes (rebalancers, bulk loads: they run k-means, perf287's B2 on 7b2) and
    /// `Replication.Follow`'s (a replica catching up reads and encodes at full speed,
    /// cli692's F1 on #1583).
    pub(crate) fn placement(mut self, placement: &Arc<super::placement::Placement>) -> Self {
        let paused = self.replica.is_some();
        if let Some(part) = &mut self.part {
            let indexes = super::indexes::Indexes::new(
                part.db.clone(),
                Some(Arc::clone(placement)),
                self.gate.clone(),
            );
            // A replica's pause survives the rebuild, whichever is set first.
            indexes.pause_rebalancing(paused);
            part.indexes = Arc::new(indexes);
            part.bulk.placement = Some(Arc::clone(placement));
            part.backups.place(placement);
        }
        self.placement = Some(Arc::clone(placement));
        self
    }

    /// Serves the store `slot` holds, if any, as the `Memory` service, with at most `calls`
    /// memory calls on the blocking pool at once (0: half the pool, at least 1).
    pub(crate) fn memory_opt(
        mut self,
        slot: Option<super::memslot::MemSlot>,
        calls: usize,
    ) -> Self {
        let Some(slot) = slot else { return self };
        if let Some(part) = &mut self.part {
            part.memory = Some(Arc::new(slot));
        }
        if calls > 0 {
            self.memory_calls = Arc::new(Semaphore::new(calls));
        }
        self
    }

    /// The table cache's generation, for a test to run [`table`](Self::table)'s steps with a
    /// catalog change between them (#1632).
    #[cfg(test)]
    pub(crate) fn table_generation(&self) -> u64 {
        self.part.as_ref().map_or(0, |p| p.tables.load().generation)
    }

    /// Whether the table cache holds a handle for `name`.
    #[cfg(test)]
    pub(crate) fn table_cached(&self, name: &str) -> bool {
        self.part
            .as_ref()
            .is_some_and(|p| p.tables.load().map.contains_key(name))
    }

    /// The memory store's slot: `/mcp`'s memory tools (8c-3b), and a test's inspection of
    /// the store it holds (#1640).
    pub(crate) fn memory_slot(&self) -> Option<&Arc<super::memslot::MemSlot>> {
        self.part.as_ref()?.memory.as_ref()
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
    /// front-end already reserved it, `prepaid`), then the call. A replica refuses writes
    /// with the primary's URL in the error.
    pub(crate) async fn call(
        &self,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<OwnedSemaphorePermit>,
    ) -> Result<ServiceReply, ApiError> {
        // A mask a partition's open or close published (D317 ticket 6), taken by a busy
        // worker that hasn't parked since.
        super::placement::refresh();
        let budget = self.admit(&op.admit(), caller, prepaid).await?;
        // A fenced partition's writes (D317 ticket 6), once admitted, so a fence's drain
        // never waits behind a write's wait for the budget. Held until the call answers;
        // a write that runs on past its request extends it.
        let held = match &self.gate {
            Some(gate) if op.writes() => Some(gate.enter()?),
            _ => None,
        };
        let reply = self.answer(op, caller, budget).await;
        drop(held);
        match (reply, self.replica.as_ref().filter(|r| r.refuses_writes())) {
            // A transaction's Commit with writes: the engine's gate (R6) refused it.
            (Err(e), Some(replica)) if e.code == "ReadOnly" => {
                Err(ApiError::replica_read_only(replica.primary()))
            }
            // A cluster partition closed under the call (D317 ticket 6's close, S5): the
            // partition is unavailable here, and the client routes again.
            (Err(e), _)
                if e.code == "Closed"
                    && let Some(gate) = &self.gate =>
            {
                Err(ApiError::partition_unavailable(format!(
                    "partition {:?} closed on this node while the call ran: {}",
                    gate.partition(),
                    e.message
                )))
            }
            (reply, _) => reply,
        }
    }

    /// The partition's write gate, on a cluster partition's service.
    pub(crate) fn gate(&self) -> Option<&Arc<super::gate::WriteGate>> {
        self.gate.as_ref()
    }

    /// Every call's rules, before it runs, in one place (D313 §7 as amended by ticket 8c):
    /// gRPC and HTTP calls come through [`call`](Self::call), and `/mcp`'s tools call it
    /// directly. In order:
    /// - **a replica's write refusal:** a write a caller may make is refused with `ReadOnly`
    ///   naming the primary, rather than a permission error;
    /// - **the caller's scope:** `PermissionDenied` below it;
    /// - **the request's size:** `--max-request-bytes`;
    /// - **the in-flight budget** (R3): `size` bytes, held by the returned permit until the
    ///   reply is built. Waiting for it counts against the request's deadline; nothing has
    ///   been submitted while it waits. Both front-ends reserve it before reading the
    ///   message (perf287's F2 on #1501): HTTP from `Content-Length`, gRPC from the message
    ///   header (`grpc::prepay`). It's taken here only for a call that comes without one.
    ///   Reads hold it until their reply is built; a write hands it to its detached task,
    ///   so a client that goes away doesn't free bytes the write still holds (simp724's S1
    ///   on #1501).
    pub(crate) async fn admit(
        &self,
        call: &Admit,
        caller: &Caller,
        prepaid: Option<OwnedSemaphorePermit>,
    ) -> Result<OwnedSemaphorePermit, ApiError> {
        self.check(call, caller)?;
        match prepaid {
            Some(p) => Ok(p),
            None => self.budget(call.size).await,
        }
    }

    /// [`admit`](Self::admit)'s checks without its reservation: a `/mcp` tool call, whose
    /// request's bytes are its POST's, already reserved (ai1249's S1 on #1666).
    pub(crate) fn check(&self, call: &Admit, caller: &Caller) -> Result<(), ApiError> {
        if let Some(replica) = self.replica.as_ref().filter(|r| r.refuses_writes())
            && call.writes
            && caller.scope.allows(call.scope)
        {
            return Err(ApiError::replica_read_only(replica.primary()));
        }
        if !caller.scope.allows(call.scope) {
            return Err(ApiError::permission_denied(&format!("call {}", call.what)));
        }
        if call.size > self.limits.max_request_bytes {
            return Err(ApiError::request_too_large(self.limits.max_request_bytes));
        }
        Ok(())
    }

    async fn answer(
        &self,
        op: ServiceOp,
        caller: &Caller,
        budget: OwnedSemaphorePermit,
    ) -> Result<ServiceReply, ApiError> {
        match op {
            // A read that fails on `Io` changed nothing, so it may be sent
            // again (`retry_from` REQUEST, ICR 0112).
            // The unary reads charge their replies to the budget (#1567, revised after g46):
            // see `read_charged`. The request's share goes first, so a read never holds it
            // while it waits for its reply's.
            ServiceOp::Get(r) => {
                drop(budget);
                // One cell, with no cap to reserve against: charged after the read, waiting
                // for its size if the budget is full (never read twice).
                let reply = self
                    .get(r, caller)
                    .await
                    .map(ServiceReply::Get)
                    .map_err(ApiError::for_read)?;
                self.charge_built(caller, reply).await
            }
            ServiceOp::MultiGet(r) => {
                drop(budget);
                let cap = cap(r.page_bytes);
                self.read_charged(caller, cap, r, |r| async move {
                    self.multi_get(r, caller)
                        .await
                        .map(|(reply, r)| (ServiceReply::MultiGet(reply), r))
                })
                .await
            }
            ServiceOp::ReadRow(r) => {
                drop(budget);
                let cap = cap(r.page_bytes);
                self.read_charged(caller, cap, r, |r| async move {
                    self.read_row(r, caller)
                        .await
                        .map(|(reply, r)| (ServiceReply::ReadRow(reply), r))
                })
                .await
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
                let tables = self.user_tables();
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
                let db = self.part()?.db.clone();
                let info = self.blocking(move || Ok(db.info()?)).await?;
                let tables = self.user_tables();
                Ok(ServiceReply::Info(proto::InfoResponse {
                    path: self.part()?.db.path().display().to_string(),
                    file_bytes: info.file_bytes,
                    free_bytes: info.free_bytes.unwrap_or(0),
                    tables,
                    version: env!("CARGO_PKG_VERSION").to_owned(),
                    replica: self.replica_info(),
                }))
            }
            ServiceOp::Flush(_) => {
                drop(budget);
                self.part()?.db.flush_async().await?;
                Ok(ServiceReply::Empty(proto::Empty {}))
            }
            ServiceOp::Compact(_) => {
                drop(budget);
                self.part()?.db.compact_async().await?;
                Ok(ServiceReply::Empty(proto::Empty {}))
            }
            ServiceOp::Backup(r) => {
                drop(budget);
                if !r.partition.is_empty() {
                    return Err(single_node_partition());
                }
                self.backup(self.part()?, r).await.map(ServiceReply::Backup)
            }
            ServiceOp::BackupDownload(r) => {
                drop(budget);
                if !r.partition.is_empty() {
                    return Err(single_node_partition());
                }
                if r.fenced_file.is_some() {
                    return Err(ApiError::invalid(
                        "fenced_file is a cluster node's (D317 ticket 6): a single node's \
                         download is a backup",
                    ));
                }
                self.backup_download(self.part()?, r, None)
                    .await
                    .map(ServiceReply::Download)
            }
            ServiceOp::Promote(_) => {
                drop(budget);
                let Some(replica) = &self.replica else {
                    return Err(ApiError::failed_precondition(
                        "this server isn't a replica (--replica-of)",
                    ));
                };
                let primary_token = replica.promote(&self.part()?.db).await?;
                // A primary now: background indexes rebalance again, from handles opened
                // afresh (cli692's F3 on #1583).
                self.part()?.indexes.forget();
                self.part()?.indexes.pause_rebalancing(false);
                // The memory store opens as the writer now, at once, created if missing
                // (cli692's D3 on #1618). Its failure doesn't fail the promote (the database
                // is what's promoted): it's the replica's last error, and memory calls answer
                // it until the next clear.
                if let Some(memory) = &self.part()?.memory {
                    memory.promote();
                    let slot = Arc::clone(memory);
                    let opened = self.blocking(move || slot.get()).await;
                    let failed = match opened {
                        Ok(super::memslot::Entry::Failed(e)) | Err(e) => Some(e.message),
                        Ok(_) => None,
                    };
                    if let Some(e) = failed {
                        eprintln!(
                            "phdb serve: after the promote, memory store {}: {e}",
                            memory.name()
                        );
                        replica.note_error(format!("memory store {}: {e}", memory.name()));
                    }
                }
                Ok(ServiceReply::Promote(proto::PromoteResponse {
                    primary_token: primary_token.into(),
                }))
            }
            ServiceOp::Follow(r) => {
                drop(budget);
                self.follow(r).map(ServiceReply::Follow)
            }
            ServiceOp::Metrics(_) => {
                drop(budget);
                // A single node: a cluster node answers it in the router (D317 ticket 8b).
                Ok(ServiceReply::Metrics(proto::MetricsResponse {
                    text: self.metrics_text(&self.single_parts(), &[&self.sessions]),
                }))
            }
            ServiceOp::DescribeTable(r) => {
                drop(budget);
                let table = self.table(&r.name).await?;
                Ok(ServiceReply::Table(describe(&table)))
            }
            ServiceOp::Traverse(r) => {
                drop(budget);
                let cap = cap(r.page_bytes);
                self.read_charged(caller, cap, r, |r| async move {
                    let reply = self.traverse(&r, caller).await?;
                    Ok((ServiceReply::Traverse(reply), r))
                })
                .await
            }
            ServiceOp::Nearest(r) => {
                drop(budget);
                let cap = cap(r.page_bytes);
                self.read_charged(caller, cap, r, |r| async move {
                    let reply = self.nearest(&r, caller).await?;
                    Ok((ServiceReply::Nearest(reply), r))
                })
                .await
            }
            ServiceOp::CreateIndex(r) => {
                drop(budget);
                self.create_index(r).await.map(ServiceReply::Empty)
            }
            ServiceOp::VectorInsert(r) => {
                self.vector_insert(r, budget).await.map(ServiceReply::Empty)
            }
            ServiceOp::VectorDelete(r) => {
                self.vector_delete(r, budget).await.map(ServiceReply::Empty)
            }
            ServiceOp::VectorSearch(r) => {
                drop(budget);
                self.vector_search(r)
                    .await
                    .map(ServiceReply::VectorSearch)
                    .map_err(ApiError::for_read)
            }
            ServiceOp::VectorVerify(r) => {
                drop(budget);
                self.vector_verify(r)
                    .await
                    .map(ServiceReply::VectorVerify)
                    .map_err(ApiError::for_read)
            }
            ServiceOp::TextCreateIndex(r) => {
                drop(budget);
                self.text_create_index(r).await.map(ServiceReply::Empty)
            }
            ServiceOp::TextPut(r) => self.text_put(r, budget).await.map(ServiceReply::Empty),
            ServiceOp::TextDelete(r) => self.text_delete(r, budget).await.map(ServiceReply::Empty),
            ServiceOp::TextSearch(r) => {
                drop(budget);
                self.text_search(r)
                    .await
                    .map(ServiceReply::TextSearch)
                    .map_err(ApiError::for_read)
            }
            ServiceOp::TextRebuild(r) => {
                drop(budget);
                self.text_rebuild(r).await.map(ServiceReply::TextRebuild)
            }
            ServiceOp::TextVerify(r) => {
                drop(budget);
                self.text_verify(r)
                    .await
                    .map(ServiceReply::TextVerify)
                    .map_err(ApiError::for_read)
            }
            ServiceOp::Remember(r) => self
                .remember(r, caller, budget)
                .await
                .map(ServiceReply::Remember),
            ServiceOp::Recall(r) => {
                drop(budget);
                let (reply, charge) = self.recall(r, caller).await.map_err(ApiError::for_read)?;
                let reply = ServiceReply::Recall(reply);
                match charge {
                    Some(charge) => Ok(self.charged(caller, reply, charge)),
                    // A budget within the default page: charged once built (never run twice).
                    None => self.charge_built(caller, reply).await,
                }
            }
            ServiceOp::MarkUsed(r) => {
                let ids = super::memory::ids(&r.ids)?;
                self.memory_write("Memory.MarkUsed", caller, budget, move |core, scope| {
                    let m = core.mark_used(scope, &ids)?;
                    Ok(proto::MarkUsedResponse {
                        marked: m.marked as u64,
                    })
                })
                .await
                .map(ServiceReply::MarkUsed)
            }
            ServiceOp::Supersede(r) => {
                let (old, new) = (super::memory::id(&r.old)?, super::memory::id(&r.new)?);
                self.memory_write("Memory.Supersede", caller, budget, move |core, scope| {
                    core.supersede(scope, &old, &new)?;
                    Ok(proto::Empty {})
                })
                .await
                .map(ServiceReply::Empty)
            }
            ServiceOp::Link(r) => {
                let (from, to) = (super::memory::id(&r.from)?, super::memory::id(&r.to)?);
                let ty = super::memory::link_type(r.link_type.as_ref())?;
                let weight = r.weight;
                self.memory_write("Memory.Link", caller, budget, move |core, scope| {
                    core.link(scope, &from, &to, ty, weight)?;
                    Ok(proto::Empty {})
                })
                .await
                .map(ServiceReply::Empty)
            }
            ServiceOp::Forget(r) => {
                let ids = super::memory::ids(&r.ids)?;
                self.memory_write("Memory.Forget", caller, budget, move |core, scope| {
                    // Serve answers the failure; the deletes before it stand.
                    let done = core.forget(scope, &ids)?;
                    if let Some((e, _left)) = done.failed {
                        return Err(e.into());
                    }
                    Ok(proto::ForgetResponse {
                        forgotten: done.deleted,
                    })
                })
                .await
                .map(ServiceReply::Forget)
            }
            ServiceOp::MemoryWatch(r) => {
                drop(budget);
                self.memory_watch(r, caller)
                    .await
                    .map(ServiceReply::MemoryWatch)
                    .map_err(ApiError::for_read)
            }
            ServiceOp::VectorBulkLoad(op) => {
                drop(budget);
                let index = self.index(&op.index).await?;
                self.part()?
                    .bulk
                    .run(index, op, self.gate.as_ref())
                    .await
                    .map(|()| ServiceReply::Empty(proto::Empty {}))
            }
            ServiceOp::Subscribe(r) => self
                .subscribe(r, caller, budget)
                .await
                .map(ServiceReply::Feed)
                .map_err(ApiError::for_read),
            ServiceOp::ReleaseSnapshot(r) => {
                drop(budget);
                self.sessions
                    .release(&r.snapshot_id, caller.principal)
                    .map(|()| ServiceReply::Empty(proto::Empty {}))
            }
        }
    }

    /// A unary read, charged (#1567, revised after g46).
    ///
    /// - **A cap within the default page ([`FAST_CAP`]):** the reply is built first, then
    ///   [`try_charge`](Self::try_charge)d. If its share isn't free now, the strict path: the
    ///   built reply goes, the cap is reserved (waiting up to the request timeout, else
    ///   `Overloaded`), and the read runs again under it. What's built unaccounted is at
    ///   most `--max-concurrent` × the default page (cli692's H1).
    /// - **A larger cap:** reserved before the read, as before g46, since its builds run on
    ///   the Tokio workers and would otherwise be bounded only by `--max-concurrent` × 8 MiB.
    ///
    /// The read takes the request and gives it back with its reply, so the fast path never
    /// copies it; only the strict path reads it a second time (perf287's g54 profile).
    async fn read_charged<R, Fut>(
        &self,
        caller: &Caller,
        cap: usize,
        mut r: R,
        read: impl Fn(R) -> Fut,
    ) -> Result<ServiceReply, ApiError>
    where
        Fut: Future<Output = Result<(ServiceReply, R), ApiError>>,
    {
        if cap <= FAST_CAP {
            let (reply, back) = read(r).await.map_err(ApiError::for_read)?;
            let (reply, settled) = self.try_charge(caller, reply);
            if settled {
                return Ok(reply);
            }
            drop(reply);
            r = back;
            self.replies.rereads.fetch_add(1, Ordering::Relaxed);
        }
        let charge = self.reserve_reply(caller, cap).await?;
        let (reply, _) = read(r).await.map_err(ApiError::for_read)?;
        Ok(self.charged(caller, reply, charge))
    }

    /// A built reply with its share: taken now if it's free, else waited for (its size,
    /// within the request timeout, else `Overloaded`). For a reply too costly to build twice
    /// (a recall) or with no cap to reserve against (`Get`).
    pub(crate) async fn charge_built(
        &self,
        caller: &Caller,
        reply: ServiceReply,
    ) -> Result<ServiceReply, ApiError> {
        let (reply, settled) = self.try_charge(caller, reply);
        if settled {
            return Ok(reply);
        }
        let charge = self
            .reserve_reply(caller, reply.encoded_len() + super::charge::GRPC_FRAME)
            .await?;
        Ok(self.charged(caller, reply, charge))
    }

    /// `reply` with its share, taken now if it's free: `(reply, true)` when it's settled
    /// (charged, or needing no charge), `(reply, false)` when its share isn't free.
    ///
    /// - **A reply within one slice takes none:** it goes to the transport whole, so its
    ///   share would be given back at the response anyway. So the hot small-read path does
    ///   no budget work (g46's readrow-small).
    /// - **HTTP/JSON's share** is twice the protobuf's, fitted to the rendered body later.
    /// - **A waiting reservation keeps its place:** Tokio's semaphore gives a queued
    ///   `acquire_many` every free permit and each release to the queue first, so this
    ///   `try_acquire` can't overtake it.
    fn try_charge(&self, caller: &Caller, reply: ServiceReply) -> (ServiceReply, bool) {
        let size = reply.encoded_len() + super::charge::GRPC_FRAME;
        let want = if caller.json {
            size.saturating_mul(2)
        } else {
            size
        };
        if want <= super::charge::PIECE {
            return (reply, true);
        }
        match super::charge::Charge::try_reserve(
            &self.inflight,
            self.limits.inflight_bytes,
            want,
            &self.replies,
        ) {
            Some(charge) => (ServiceReply::Charged(Box::new(reply), charge), true),
            None => (reply, false),
        }
    }

    /// A unary read's reply share on the strict path (#1567): `cap` bytes, twice that for
    /// an HTTP/JSON reply (cli692's D2), within the request timeout, else `Overloaded`.
    async fn reserve_reply(
        &self,
        caller: &Caller,
        cap: usize,
    ) -> Result<super::charge::Charge, ApiError> {
        let want = if caller.json {
            cap.saturating_mul(2)
        } else {
            cap
        };
        super::charge::Charge::reserve(
            &self.inflight,
            self.limits.inflight_bytes,
            want,
            self.limits.request_timeout,
            &self.replies,
        )
        .await
    }

    /// A fanned-out read's part (D317, #1715): the same `MultiGet` or `Nearest` read a call
    /// makes (its scope check, blocking-pool slot and per-call limits included), for a
    /// request whose entry node holds the budget for its page ([`Self::fanout_hold`]): no
    /// budget here, neither admission nor a reply charge. The fan-out calls it for its local
    /// part, and the router for a part a peer forwarded (`fanout_part`).
    pub(crate) async fn fanout_part(
        &self,
        op: ServiceOp,
        caller: &Caller,
    ) -> Result<ServiceReply, ApiError> {
        // The caller's scope, as admission checks it; the budget is the fanning-out node's.
        self.check(&op.admit(), caller)?;
        match op {
            ServiceOp::MultiGet(r) => self
                .multi_get(r, caller)
                .await
                .map(|(reply, _)| ServiceReply::MultiGet(reply))
                .map_err(ApiError::for_read),
            ServiceOp::Nearest(r) => self
                .nearest(&r, caller)
                .await
                .map(ServiceReply::Nearest)
                .map_err(ApiError::for_read),
            _ => Err(ApiError::internal(
                "a fan-out's local part is a multi-get or a nearest search",
            )),
        }
    }

    /// A fanned-out read's holding (#1715): `bytes` of the in-flight budget (at most the
    /// whole of it, so a lone request runs), waiting at most `wait`, else `Overloaded` as a
    /// reply's share does.
    pub(crate) async fn fanout_hold(
        &self,
        bytes: usize,
        wait: Duration,
    ) -> Result<super::charge::Charge, ApiError> {
        super::charge::Charge::reserve(
            &self.inflight,
            self.limits.inflight_bytes,
            bytes,
            wait,
            &self.replies,
        )
        .await
    }

    /// [`Self::fanout_hold`] only if the bytes are free now: a read ahead, or a further
    /// concurrent part, on spare budget.
    pub(crate) fn fanout_try_hold(&self, bytes: usize) -> Option<super::charge::Charge> {
        super::charge::Charge::try_reserve(
            &self.inflight,
            self.limits.inflight_bytes,
            bytes,
            &self.replies,
        )
    }

    /// A fanned-out read's reply, charged with its holding (#1715): fitted to the reply's
    /// share as [`Self::reserve_reply`] sizes it (twice for HTTP/JSON), so the reply's
    /// charge takes over the pages' with no gap and no second reservation. Only an
    /// HTTP/JSON share larger than the holding waits, at most `wait`.
    pub(crate) async fn fanout_reply(
        &self,
        caller: &Caller,
        reply: ServiceReply,
        mut holding: super::charge::Charge,
        wait: Duration,
    ) -> Result<ServiceReply, ApiError> {
        let size = reply.encoded_len() + super::charge::GRPC_FRAME;
        let want = if caller.json {
            size.saturating_mul(2)
        } else {
            size
        };
        holding.fit(want, self.limits.inflight_bytes, wait).await?;
        Ok(self.charged(caller, reply, holding))
    }

    /// `reply` with its charge: a gRPC reply's trimmed to its encoded message now; an
    /// HTTP/JSON one's fitted to its rendered body by the front-end.
    fn charged(
        &self,
        caller: &Caller,
        reply: ServiceReply,
        mut charge: super::charge::Charge,
    ) -> ServiceReply {
        if !caller.json {
            charge.trim(reply.encoded_len() + super::charge::GRPC_FRAME);
        }
        ServiceReply::Charged(Box::new(reply), charge)
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
    ///
    /// A replica's applied catalog changes don't take `schema`: there, an open that
    /// [`tables_changed`](Self::tables_changed) overtook keeps nothing and opens again,
    /// since it may have resolved the name before a drop and create (#1632).
    pub(crate) async fn table(&self, name: &str) -> Result<Table, ApiError> {
        loop {
            let generation = match self.cached(name) {
                Ok(t) => return Ok(t),
                Err(generation) => generation,
            };
            reserved(name)?;
            let db = self.part()?.db.clone();
            let owned = name.to_owned();
            let table = self
                .blocking(move || db.table(&owned)?.open().map_err(ApiError::from))
                .await?;
            if let Some(t) = self.keep_table(name, &table, generation) {
                return Ok(t);
            }
        }
    }

    /// [`table`](Self::table) on a blocking thread the caller already holds (a `/mcp`
    /// request's): a miss opens the table here, never on a second pool thread, which a full
    /// pool would never give (ai1249's B1 on #1666).
    pub(crate) fn table_blocking(&self, name: &str) -> Result<Table, ApiError> {
        loop {
            let generation = match self.cached(name) {
                Ok(t) => return Ok(t),
                Err(generation) => generation,
            };
            reserved(name)?;
            let table = self.part()?.db.table(name)?.open()?;
            if let Some(t) = self.keep_table(name, &table, generation) {
                return Ok(t);
            }
        }
    }

    /// The cached handle for `name`, or the cache's generation to open it at.
    fn cached(&self, name: &str) -> Result<Table, u64> {
        // No partition, no cache: the open after it answers that.
        let Some(part) = &self.part else {
            return Err(0);
        };
        let cache = part.tables.load();
        cache.map.get(name).cloned().ok_or(cache.generation)
    }

    /// Keeps `table` under `name` unless a handle is there already (then that one is
    /// returned). `None`, keeping nothing, if [`tables_changed`](Self::tables_changed) came
    /// after `generation`, the one the open started at (#1632).
    pub(crate) fn keep_table(&self, name: &str, table: &Table, generation: u64) -> Option<Table> {
        let mut kept = None;
        self.part.as_ref()?.tables.rcu(|c| {
            if c.generation != generation {
                kept = None;
                return Arc::clone(c);
            }
            let mut c = TableCache::clone(c);
            kept = Some(
                c.map
                    .entry(name.to_owned())
                    .or_insert_with(|| table.clone())
                    .clone(),
            );
            Arc::new(c)
        });
        kept
    }

    /// `Schema.CreateTable`: the table with its families, created, or with `if_missing`
    /// opened with the families it lacks added. The service's handle for the name is
    /// replaced, so later calls (and feed hubs, keyed by its id) use this table.
    async fn create_table(
        &self,
        r: proto::CreateTableRequest,
    ) -> Result<proto::TableResponse, ApiError> {
        reserved(&r.name)?;
        if r.name.is_empty() {
            return Err(ApiError::invalid("a table needs a name"));
        }
        if r.placement.is_some() {
            return Err(ApiError::invalid(
                "a placement is for a cluster node's map (D317): a single node holds every row",
            ));
        }
        let families = r
            .families
            .iter()
            .map(|f| Ok((f.name.clone(), convert::family_in(f)?)))
            .collect::<Result<Vec<_>, ApiError>>()?;
        let table = self.create_with(r.name, families, r.if_missing).await?;
        Ok(describe(&table))
    }

    /// Creates table `name` with `families`, or with `if_missing` opens it with the families
    /// it lacks added: `Schema.CreateTable`'s work, and `/mcp`'s `create_table` tool's. The
    /// service's handle for the name is replaced, so later calls (and feed hubs, keyed by
    /// its id) use this table.
    pub(crate) async fn create_with(
        &self,
        name: String,
        families: Vec<(String, pigeonhole::Family)>,
        if_missing: bool,
    ) -> Result<Table, ApiError> {
        reserved(&name)?;
        // The pool place before `schema` (its lock order).
        let place = pool_place(&self.blocking).await?;
        let _schema = self.part()?.schema.lock().await;
        let db = self.part()?.db.clone();
        let owned = name.clone();
        let table = held(place, move || create_in(&db, &owned, families, if_missing)).await?;
        self.created(name, &table);
        Ok(table)
    }

    /// [`create_with`](Self::create_with) on a blocking thread the caller already holds (a
    /// `/mcp` request's), for the reason [`table_blocking`](Self::table_blocking) gives.
    pub(crate) fn create_blocking(
        &self,
        name: &str,
        families: Vec<(String, pigeonhole::Family)>,
        if_missing: bool,
    ) -> Result<Table, ApiError> {
        reserved(name)?;
        let _schema = self.part()?.schema.blocking_lock();
        let table = create_in(&self.part()?.db, name, families, if_missing)?;
        self.created(name.to_owned(), &table);
        Ok(table)
    }

    /// A created table's handle replaces the name's, and the catalog moves.
    fn created(&self, name: String, table: &Table) {
        let Some(part) = &self.part else { return };
        part.tables.rcu(|c| {
            let mut c = TableCache::clone(c);
            c.map.insert(name.clone(), table.clone());
            c
        });
        part.catalog.fetch_add(1, Ordering::Release);
    }

    /// The user's tables, sorted: the cluster's own (`__phdb_cluster`, ICR 0117) aren't.
    pub(crate) fn user_tables(&self) -> Vec<String> {
        let Some(part) = &self.part else {
            return Vec::new();
        };
        let mut tables = part.db.tables();
        tables.retain(|t| !t.starts_with(super::cluster::map::RESERVED_PREFIX));
        tables.sort();
        tables
    }

    /// The catalog's generation: it moves with every create and drop.
    pub(crate) fn catalog(&self) -> u64 {
        self.part
            .as_ref()
            .map_or(0, |p| p.catalog.load(Ordering::Acquire))
    }

    /// `Schema.DropTable`: the table and its data go, and so does the service's handle for
    /// the name. A table created again under it is a new table, with a new id: its feed hubs
    /// are new too, and those on the old one end with their engine feeds.
    async fn drop_table(&self, name: String) -> Result<(), ApiError> {
        reserved(&name)?;
        // The memory store this server serves keeps its data in its own tables: dropping one
        // would leave the slot over a dropped table, and a restart would re-create the store
        // empty (cli692's N1 on #1650, #1651). The crate names them.
        if let Some(memory) = &self.part()?.memory {
            let store = memory.name();
            if pigeonhole_memory::Store::table_names(store).contains(&name) {
                return Err(ApiError::failed_precondition(format!(
                    "table {name:?} belongs to the memory store {store} this server serves \
                     (--memory {store}): stop the server to drop it"
                )));
            }
        }
        // The pool place before `schema` (its lock order).
        let place = pool_place(&self.blocking).await?;
        let _schema = self.part()?.schema.lock().await;
        let db = self.part()?.db.clone();
        let dropped = held(place, move || Ok(db.drop_table(&name)?)).await;
        // Whatever the outcome, the next use resolves every name again: the table's handle,
        // and a vector or text index's if it was one, with its rebalancer (#1588). Every
        // cached handle goes, not only the name's: drops are rare, and an open of the name
        // already running keeps nothing (#1632).
        self.handles_changed();
        dropped
    }

    /// The metrics as Prometheus text: `/metrics` and `Admin.Metrics` alike, for `parts`,
    /// the partitions served here. A single node passes its one, unlabelled; a cluster node
    /// passes the router's ([`Router::metrics_text`](super::router::Router::metrics_text)),
    /// each labelled by its id (D317 ticket 8b); a node that only routes, none.
    ///
    /// `sessions` are the node's session tables: its own, and each partition's on a cluster
    /// node (D317 ticket 8a).
    pub(crate) fn metrics_text(
        &self,
        parts: &[(Option<&str>, &Partition)],
        sessions: &[&super::sessions::Sessions],
    ) -> String {
        let mut text = self.metrics.render();
        self.connections.render(&mut text);
        self.replies.render(&mut text);
        render_sessions(&mut text, sessions);
        render_feeds(&mut text, parts);
        // The hub runtime's threads, the node's (#1719): every partition's hubs share it.
        if let Some(p) = &self.placement {
            use std::fmt::Write as _;
            let _ = writeln!(text, "# TYPE pigeonhole_serve_feed_hub_threads gauge");
            let _ = writeln!(text, "pigeonhole_serve_feed_hub_threads {}", p.hub_threads);
        }
        render_bulk(&mut text, parts);
        render_pinning(&mut text, parts);
        if let Some(replica) = &self.replica {
            replica.render(&mut text);
        }
        // Reads each database on this thread, at most once per 100 ms: `engine_metrics` and
        // `recovery_stats` (atomic loads), and `info`, which walks the current catalog, takes
        // the file's length and the pager's allocation stats under its lock for an instant,
        // with no other I/O (ai1249's question, perf287's nit 2 on #1687). Every partition's
        // under one `# HELP`/`# TYPE` per metric (ICR 0133).
        let collectors: Vec<_> = parts.iter().map(|(_, p)| &p.engine_metrics).collect();
        pigeonhole_otel::Collector::render_prometheus_all(&collectors, &mut text);
        text
    }

    /// A single node's partition for [`metrics_text`](Self::metrics_text), unlabelled.
    pub(crate) fn single_parts(&self) -> Vec<(Option<&str>, &Partition)> {
        self.part.iter().map(|p| (None, p)).collect()
    }

    /// `Replication.Follow` (#1436): under a `--max-streams` slot, on a thread of its own.
    fn follow(&self, r: proto::FollowRequest) -> Result<super::replica::FollowBatches, ApiError> {
        if self.replica_info().is_some() {
            // No cascading replicas in 1.2.0 (simp724's P2 on #1436).
            return Err(ApiError::failed_precondition(
                "a replica can't be followed: follow its primary",
            ));
        }
        let Ok(slot) = Arc::clone(&self.streams).try_acquire_owned() else {
            return Err(ApiError::overloaded(
                "the server is at its concurrent stream limit (--max-streams)",
            ));
        };
        super::replica::follow(
            &self.part()?.db,
            r.token.to_vec(),
            slot,
            self.limits.stream_idle,
            Arc::clone(&self.metrics),
            self.placement.clone(),
        )
    }

    /// `Admin.Backup` of `part`: on a thread of its own, one backup at a time (perf287's D1
    /// on #1527).
    pub(crate) async fn backup(
        &self,
        part: &Partition,
        r: proto::BackupRequest,
    ) -> Result<proto::BackupResponse, ApiError> {
        let (db, backups) = (part.db.clone(), Arc::clone(&part.backups));
        let bytes = part
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
        Ok(proto::BackupResponse { bytes })
    }

    /// `Admin.BackupDownload`: a fresh backup (hashed, then kept for resumes), or a kept one
    /// from an offset, streamed in chunks of up to 1 MiB on its own task. It holds a
    /// `--max-streams` slot until it ends, and each send waits at most `--stream-idle-ms`.
    ///
    /// It answers at once (#1611): while the copy is written, the task sends a heartbeat (a
    /// chunk with no data, at the requested offset, with the id and, for a replica, the feed
    /// token; `total` 0 and no `sha256`, so a client's last-chunk check never fires on one)
    /// at once and then every `HEARTBEAT`, so a client bounds every wait by its idle
    /// limit and resumes a break during the copy with the id. HTTP waits for the copy
    /// instead: it sends the size and hash as headers.
    ///
    /// `map_version` is a cluster node's (D317 ticket 8b): the copy is of one snapshot, whose
    /// identity row it reads and sends in the last chunk, with the map version.
    pub(crate) async fn backup_download(
        &self,
        part: &Partition,
        r: proto::BackupDownloadRequest,
        map_version: Option<u64>,
    ) -> Result<super::backup::Download, ApiError> {
        use super::backup::{Copy, CopyIdentity, Taken};
        let Ok(slot) = Arc::clone(&self.streams).try_acquire_owned() else {
            return Err(ApiError::overloaded(
                "the server is at its concurrent stream limit (--max-streams)",
            ));
        };
        if r.replica && self.replica_info().is_some() {
            // No cascading replicas in 1.2.0 (simp724's P2 on #1436): this copy would carry
            // this server's own replication state.
            return Err(ApiError::failed_precondition(
                "a replica can't be a bootstrap source: bootstrap from its primary",
            ));
        }
        let (id, copy) = if r.backup_id.is_empty() {
            let (db, durable, replica) = (part.db.clone(), r.durable, r.replica);
            // The backup and its hash run on a thread of their own, never the blocking pool
            // (perf287's D1), and the copy is the table's from the start (E1).
            part.backups.fresh_download(move |p, started| {
                if let Some(map_version) = map_version {
                    // A cluster node's copy: one snapshot for the copy and the identity it
                    // names, so the last chunk says exactly what the file holds.
                    let snap = if durable {
                        db.durable_snapshot()?
                    } else {
                        db.snapshot()?
                    };
                    let id = super::cluster::identity::read_at(&db, &snap)
                        .map_err(ApiError::internal)?
                        .ok_or_else(|| {
                            ApiError::internal("the partition's file has no identity row")
                        })?;
                    db.backup_at(&snap, p)?;
                    return Ok(Taken {
                        feed_token: Vec::new(),
                        identity: Some(CopyIdentity {
                            partition: id.partition_id,
                            cluster_id: id.cluster_id,
                            epoch: id.epoch,
                            map_version,
                        }),
                    });
                }
                if replica {
                    // A replica's bootstrap (#1436): a durable snapshot's copy, and the
                    // token of a feed of every table from that same snapshot, sent with the
                    // heartbeats once it's known.
                    let snap = db.durable_snapshot()?;
                    let token = db.feed().from_snapshot(&snap).open()?.token();
                    let token = token.as_bytes().to_vec();
                    started(token.clone());
                    db.backup_at(&snap, p)?;
                    return Ok(Taken {
                        feed_token: token,
                        identity: None,
                    });
                }
                if durable {
                    db.backup_durable(p)?;
                } else {
                    db.backup(p)?;
                }
                Ok(Taken::default())
            })?
        } else {
            (r.backup_id.clone(), part.backups.open(&r.backup_id)?)
        };
        // A kept copy: an offset past it is refused now. One still being written is checked
        // once it's ready.
        if let Copy::Ready(kept) = &*copy.borrow()
            && r.offset > kept.total
        {
            part.backups.release(&id);
            return Err(past_the_copy(r.offset, kept.total));
        }
        let (tx, rx) = mpsc::channel(1);
        let (backups, pool, idle) = (
            Arc::clone(&part.backups),
            Arc::clone(&self.blocking),
            self.limits.stream_idle,
        );
        let metrics = Arc::clone(&self.metrics);
        let started = Instant::now();
        let (task_id, start, mut task_copy) = (id.clone(), r.offset, copy.clone());
        let want = r.partition;
        tokio::spawn(async move {
            // Sends `chunk` within the idle limit; `false` once the client went away (it
            // resumes with the id).
            let send = |chunk: proto::BackupChunk| {
                let tx = tx.clone();
                async move {
                    match tokio::time::timeout(idle, tx.send(Ok(chunk))).await {
                        Ok(Ok(())) => Ok(true),
                        Ok(Err(_)) => Ok(false),
                        Err(_) => Err(ApiError::deadline(
                            "the client took no backup chunk for --stream-idle-ms; resume \
                             with backup_id and the offset it reached",
                        )),
                    }
                }
            };
            let outcome: Result<(), ApiError> = async {
                // The copy, with heartbeats while it's written (#1611).
                let kept = loop {
                    let state = task_copy.borrow_and_update().clone();
                    let feed_token = match state {
                        Copy::Ready(kept) => break kept,
                        Copy::Failed(e) => return Err(e),
                        Copy::Writing { feed_token } => feed_token,
                    };
                    let beat = proto::BackupChunk {
                        data: Default::default(),
                        total: 0,
                        backup_id: task_id.clone(),
                        offset: start,
                        sha256: Default::default(),
                        feed_token: feed_token.into(),
                        ..Default::default()
                    };
                    if !send(beat).await? {
                        return Ok(());
                    }
                    tokio::select! {
                        changed = task_copy.changed() => {
                            // The table dropped the copy without a last state: never expected
                            // (it says why it failed first).
                            if changed.is_err()
                                && matches!(*task_copy.borrow(), Copy::Writing { .. })
                            {
                                return Err(ApiError::internal("the backup's copy vanished"));
                            }
                        }
                        () = tokio::time::sleep(HEARTBEAT) => {}
                    }
                };
                if start > kept.total {
                    return Err(past_the_copy(start, kept.total));
                }
                // A resume names the partition its copy is of (D317 ticket 8b).
                let of = kept.identity.as_ref().map_or("", |i| i.partition.as_str());
                if of != want {
                    return Err(ApiError::invalid(format!(
                        "backup {task_id} is of partition {of:?}, not {want:?}"
                    )));
                }
                send_kept(&kept, start, &task_id, &pool, send).await
            }
            .await;
            metrics.count(
                DOWNLOAD,
                outcome.as_ref().err().map(|e| e.code.as_str()),
                started.elapsed(),
            );
            // Within the idle limit, as a chunk: a heartbeat may still sit in the channel, and
            // a dropped error would end the stream as if it were whole (cli692's R1 on #1628).
            if let Err(e) = outcome {
                let _ = tokio::time::timeout(idle, tx.send(Err(e))).await;
            }
            backups.release(&task_id);
            drop(slot);
        });
        Ok(super::backup::Download {
            id,
            offset: r.offset,
            copy,
            chunks: rx,
        })
    }

    /// `Admin.BackupDownload` of a closed partition's file (D317 ticket 6, `fenced_file`):
    /// `file`'s bytes at `path` as they lie, from `r.offset`, with no backup taken. The
    /// partition is closed, so the file is final, and `file` carries its size and SHA-256 as
    /// hashed at its close. Holds a `--max-streams` slot, as a download does.
    pub(crate) async fn fenced_download(
        &self,
        path: std::path::PathBuf,
        file: proto::PartitionFile,
        r: proto::BackupDownloadRequest,
    ) -> Result<super::backup::Download, ApiError> {
        use super::backup::{Copy, Kept};
        if r.durable || r.replica || !r.backup_id.is_empty() {
            return Err(ApiError::invalid(
                "a fenced file is sent as it lies: durable, replica and backup_id don't apply",
            ));
        }
        let sha256: [u8; 32] = file.sha256.as_ref().try_into().map_err(|_| {
            ApiError::internal(format!("fenced file {:?} has no SHA-256", file.name))
        })?;
        let kept = Kept {
            path,
            total: file.bytes,
            sha256,
            feed_token: Vec::new(),
            identity: None,
        };
        if r.offset > kept.total {
            return Err(past_the_copy(r.offset, kept.total));
        }
        let Ok(slot) = Arc::clone(&self.streams).try_acquire_owned() else {
            return Err(ApiError::overloaded(
                "the server is at its concurrent stream limit (--max-streams)",
            ));
        };
        let (tx, rx) = mpsc::channel(1);
        let (pool, idle) = (Arc::clone(&self.blocking), self.limits.stream_idle);
        let metrics = Arc::clone(&self.metrics);
        let started = Instant::now();
        let (start, task_kept) = (r.offset, kept.clone());
        tokio::spawn(async move {
            let send = |chunk: proto::BackupChunk| {
                let tx = tx.clone();
                async move {
                    match tokio::time::timeout(idle, tx.send(Ok(chunk))).await {
                        Ok(Ok(())) => Ok(true),
                        Ok(Err(_)) => Ok(false),
                        Err(_) => Err(ApiError::deadline(
                            "the client took no chunk for --stream-idle-ms; resume from the \
                             offset it reached",
                        )),
                    }
                }
            };
            let outcome = send_kept(&task_kept, start, "", &pool, send).await;
            metrics.count(
                DOWNLOAD,
                outcome.as_ref().err().map(|e| e.code.as_str()),
                started.elapsed(),
            );
            if let Err(e) = outcome {
                let _ = tokio::time::timeout(idle, tx.send(Err(e))).await;
            }
            drop(slot);
        });
        Ok(super::backup::Download {
            id: String::new(),
            offset: r.offset,
            copy: watch::channel(Copy::Ready(kept)).1,
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

    /// Runs `f` on the blocking pool, within its limit: a `/mcp` request's messages, whose
    /// tools make sync database calls.
    pub(crate) async fn run_blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
    ) -> Result<T, ApiError> {
        self.blocking(f).await
    }

    /// A `--max-streams` place, now or `Overloaded`: a feed or watch stream's, or a `/mcp`
    /// `feed_poll`'s for its wait.
    pub(crate) fn stream_slot(&self) -> Result<OwnedSemaphorePermit, ApiError> {
        Arc::clone(&self.streams).try_acquire_owned().map_err(|_| {
            ApiError::overloaded("the server is at its concurrent stream limit (--max-streams)")
        })
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
        // The fence's drain waits for the write, not just its request (`call` admitted it).
        let held = self.gate.as_ref().map(super::gate::WriteGate::extend);
        tokio::spawn(async move {
            let outcome = write.await;
            // The write is done: its bytes leave the budget now, not when its request did.
            drop(budget);
            drop(held);
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
    ) -> Result<(proto::MultiGetResponse, proto::MultiGetRequest), ApiError> {
        let pinned = self.pinned_selection(r.selection.as_ref(), caller)?;
        let table = self.table(&r.table).await?;
        let budget = page_bytes(r.page_bytes);
        // The request comes back with the reply, for a strict path's second read (#1661).
        self.blocking(move || {
            let reply = (|| -> Result<proto::MultiGetResponse, ApiError> {
                let at = pinned.as_ref().map(|p| p.snapshot().clone());
                let select = convert::Select::new(r.selection.as_ref())?.at(at);
                let keys = KeySet::from_unsorted(r.keys.iter().map(|k| &k[..]));
                let mut read = select.keys(table.keys(&keys))?;
                let after;
                if !r.after.is_empty() {
                    super::cluster::position::refuse_on_a_single_node(&r.after)?;
                    after = KeysPosition::from_bytes(&r.after).map_err(ApiError::from)?;
                    read = read.resume_after(&after);
                }
                let page = read.read_within(budget)?;
                let q = select.aggregate();
                Ok(proto::MultiGetResponse {
                    missing: Vec::new(),
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
            })()?;
            Ok((reply, r))
        })
        .await
    }

    /// `Graph.Traverse` (D304): the hops from the seeds over one edge family, at the pinned
    /// snapshot `snapshot_id` names, or now, cut to `page_bytes` of nodes (D308; 0 is the
    /// server's default, and the server caps it), so a reply is bounded as a `MultiGet`'s is
    /// (simp724's L1 on #1565). Run on the request's task (`run_async` never blocks on a block
    /// it can fetch), so it holds no blocking thread.
    async fn traverse(
        &self,
        r: &proto::TraverseRequest,
        caller: &Caller,
    ) -> Result<proto::TraverseResponse, ApiError> {
        if r.seeds.is_empty() {
            return Err(ApiError::invalid(
                "a traversal starts from at least one seed",
            ));
        }
        if r.hops > 64 {
            return Err(ApiError::invalid("hops is at most 64 (0 is 1)"));
        }
        let pinned = self.pinned(&r.snapshot_id, caller)?;
        let table = self.table(&r.table).await?;
        let seeds = KeySet::from_unsorted(r.seeds.iter().map(|k| &k[..]));
        let mut t = convert::edges(table.traverse(&seeds).family(&r.family), r)?
            .hops(r.hops.max(1))
            .parents(r.parents)
            .byte_budget(page_bytes(r.page_bytes));
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
                cut_by_budget: stats.cut_by_budget,
            }),
            missing: Vec::new(),
        })
    }

    /// `Vectors.Nearest` (D296, D303): the exact top `k` (at most [`MAX_NEAREST_K`]) of one
    /// vector family over a key range or a key list, at the pinned snapshot `snapshot_id`
    /// names, or now, cut to `page_bytes` by D308's measure (0 is the server's default, and
    /// the server caps it), so a reply is bounded as a `MultiGet`'s is. Run on the request's
    /// task, as `Traverse`.
    async fn nearest(
        &self,
        r: &proto::NearestRequest,
        caller: &Caller,
    ) -> Result<proto::NearestResponse, ApiError> {
        use proto::nearest_request::Source;
        if r.k == 0 || r.k > MAX_NEAREST_K {
            return Err(ApiError::invalid(format!("k is 1 to {MAX_NEAREST_K}")));
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
        let page = search.run_within_async(page_bytes(r.page_bytes)).await?;
        let cut_by_budget = page.cut_by_budget();
        let found = page.into_neighbours();
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
            missing: Vec::new(),
        })
    }

    /// The IVF index `name`, from the cache, or opened on the blocking pool on a miss.
    async fn index(&self, name: &str) -> Result<Arc<pigeonhole_vector::Index>, ApiError> {
        if let Some(i) = self.part()?.indexes.cached(name) {
            return Ok(i);
        }
        let indexes = Arc::clone(&self.part()?.indexes);
        let name = name.to_owned();
        self.blocking(move || indexes.get(&name)).await
    }

    /// `Vectors.CreateIndex` (ICR 0100): a new index, or with `if_missing` an existing one.
    async fn create_index(
        &self,
        r: proto::CreateVectorIndexRequest,
    ) -> Result<proto::Empty, ApiError> {
        let spec = r
            .spec
            .as_ref()
            .ok_or_else(|| ApiError::invalid("an index needs a vector spec"))?;
        let spec = convert::vector_spec_in(spec)?;
        let mut options = pigeonhole_vector::IndexOptions::new();
        if let Some(n) = r.target_list_len {
            options = options.target_list_len(n);
        }
        if let Some(n) = r.nprobe {
            options = options.nprobe(n);
        }
        if let Some(n) = r.reassign_neighbours {
            options = options.reassign_neighbours(n);
        }
        match proto::Rebalance::try_from(r.rebalance).unwrap_or(proto::Rebalance::Unspecified) {
            proto::Rebalance::Unspecified => {}
            proto::Rebalance::Inline => {
                options = options.rebalance(pigeonhole_vector::Rebalance::Inline);
            }
            proto::Rebalance::Background => {
                options = options.rebalance(pigeonhole_vector::Rebalance::Background);
            }
        }
        let indexes = Arc::clone(&self.part()?.indexes);
        self.blocking(move || indexes.create(&r.name, spec, options, r.if_missing))
            .await?;
        Ok(proto::Empty {})
    }

    /// `Vectors.Insert`: one vector under `id`, in the index's element type. A write: it runs
    /// detached, on the blocking pool (an insert is a transaction, and may rebalance inline).
    async fn vector_insert(
        &self,
        r: proto::VectorInsertRequest,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::Empty, ApiError> {
        let v = r
            .vector
            .as_ref()
            .ok_or_else(|| ApiError::invalid("an insert needs a vector"))?;
        let v = convert::elements(v)?;
        let index = self.index(&r.index).await?;
        let pool = Arc::clone(&self.blocking);
        self.detached("Vectors.Insert", budget, async move {
            blocking(&pool, move || {
                use convert::Elements as V;
                let id = &r.id[..];
                match &v {
                    V::F32(x) => index.insert_f32(id, x),
                    V::I8(x) => index.insert_i8(id, x),
                    V::Bits(x) => index.insert_bits(id, x),
                    V::F16(x) => index.insert_f16(id, x),
                    V::Bf16(x) => index.insert_bf16(id, x),
                }?;
                Ok(())
            })
            .await
        })
        .await?;
        Ok(proto::Empty {})
    }

    /// `Vectors.Delete`: the vector under `id`, if any. Detached, on the blocking pool.
    async fn vector_delete(
        &self,
        r: proto::VectorDeleteRequest,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::Empty, ApiError> {
        let index = self.index(&r.index).await?;
        let pool = Arc::clone(&self.blocking);
        self.detached("Vectors.Delete", budget, async move {
            blocking(&pool, move || {
                index.delete(&r.id)?;
                Ok(())
            })
            .await
        })
        .await?;
        Ok(proto::Empty {})
    }

    /// `Vectors.Search`: the `k` (at most [`MAX_NEAREST_K`]) nearest ids, on the request's
    /// task (`run_async_with_stats`), holding no blocking thread once the index is open.
    async fn vector_search(
        &self,
        r: proto::VectorSearchRequest,
    ) -> Result<proto::VectorSearchResponse, ApiError> {
        if r.k == 0 || r.k > MAX_NEAREST_K {
            return Err(ApiError::invalid(format!("k is 1 to {MAX_NEAREST_K}")));
        }
        let q = r
            .query
            .as_ref()
            .ok_or_else(|| ApiError::invalid("a search needs a query vector"))?;
        let q = convert::elements(q)?;
        let index = self.index(&r.index).await?;
        let k = r.k as usize;
        let mut s = {
            use convert::Elements as V;
            match &q {
                V::F32(x) => index.search_f32(x, k),
                V::I8(x) => index.search_i8(x, k),
                V::Bits(x) => index.search_bits(x, k),
                V::F16(x) => index.search_f16(x, k),
                V::Bf16(x) => index.search_bf16(x, k),
            }
        };
        if r.nprobe > 0 {
            s = s.nprobe(r.nprobe);
        }
        if !r.id_prefix.is_empty() {
            s = s.id_prefix(&r.id_prefix);
        }
        if r.max_probe > 0 {
            s = s.max_probe(r.max_probe);
        }
        let (hits, stats) = s.run_async_with_stats().await?;
        Ok(proto::VectorSearchResponse {
            hits: hits
                .into_iter()
                .map(|h| proto::VectorHit {
                    id: h.id.into(),
                    distance: h.distance,
                })
                .collect(),
            stats: r.stats.then_some(proto::VectorSearchStats {
                lists_probed: stats.lists_probed,
                lists: stats.lists,
            }),
        })
    }

    /// The text index `name`, from the cache, or opened on the blocking pool on a miss.
    async fn text_index(&self, name: &str) -> Result<Arc<pigeonhole_text::Index>, ApiError> {
        if let Some(i) = self.part()?.texts.cached(name) {
            return Ok(i);
        }
        let texts = Arc::clone(&self.part()?.texts);
        let name = name.to_owned();
        self.blocking(move || texts.get(&name)).await
    }

    /// `Text.CreateIndex` (ICR 0105): a new index over its source column, or with
    /// `if_missing` an existing one, as it is.
    async fn text_create_index(
        &self,
        r: proto::CreateTextIndexRequest,
    ) -> Result<proto::Empty, ApiError> {
        let options = super::texts::options(&r)?;
        let table = self.table(&r.source_table).await?;
        let source = pigeonhole_text::Source::new(&table, &r.source_family, &r.source_qualifier);
        let texts = Arc::clone(&self.part()?.texts);
        self.blocking(move || texts.create(&r.name, source, options, r.if_missing))
            .await?;
        Ok(proto::Empty {})
    }

    /// `Text.Put`: writes the document's text to the source column and indexes it, in one
    /// transaction. A write: detached, on the blocking pool.
    async fn text_put(
        &self,
        r: proto::TextPutRequest,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::Empty, ApiError> {
        let index = self.text_index(&r.index).await?;
        let pool = Arc::clone(&self.blocking);
        self.detached("Text.Put", budget, async move {
            blocking(&pool, move || {
                index.put(&r.id, &r.text)?;
                Ok(())
            })
            .await
        })
        .await?;
        Ok(proto::Empty {})
    }

    /// `Text.Delete`: the document's text and its postings, if any. Detached, as `Put`.
    async fn text_delete(
        &self,
        r: proto::TextDeleteRequest,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::Empty, ApiError> {
        let index = self.text_index(&r.index).await?;
        let pool = Arc::clone(&self.blocking);
        self.detached("Text.Delete", budget, async move {
            blocking(&pool, move || {
                index.delete(&r.id)?;
                Ok(())
            })
            .await
        })
        .await?;
        Ok(proto::Empty {})
    }

    /// `Text.Search`: the `k` (at most [`MAX_NEAREST_K`]) best documents by BM25, with
    /// their text and matches on request. On the blocking pool: a search is sync (D309).
    async fn text_search(
        &self,
        r: proto::TextSearchRequest,
    ) -> Result<proto::TextSearchResponse, ApiError> {
        if r.k == 0 || r.k > MAX_NEAREST_K {
            return Err(ApiError::invalid(format!("k is 1 to {MAX_NEAREST_K}")));
        }
        let query = super::texts::query(&r)?;
        let index = self.text_index(&r.index).await?;
        self.blocking(move || {
            let mut s = index.search(query, r.k as usize).highlight(r.highlight);
            if !r.id_prefix.is_empty() {
                s = s.id_prefix(r.id_prefix.to_vec());
            }
            let hits = s.run()?;
            Ok(proto::TextSearchResponse {
                hits: hits.into_iter().map(super::texts::hit).collect(),
            })
        })
        .await
    }

    /// `Text.Rebuild`: rewrites the index from its source column. One at a time, on a
    /// thread of its own off the shards' cores, never the blocking pool (perf287's D1: a
    /// rebuild reads the whole source). A write: it finishes if the client goes away.
    async fn text_rebuild(
        &self,
        r: proto::TextIndexRequest,
    ) -> Result<proto::TextRebuildResponse, ApiError> {
        let index = self.text_index(&r.index).await?;
        let Ok(slot) = Arc::clone(&self.text_rebuilds).try_acquire_owned() else {
            return Err(ApiError::overloaded(
                "the server is already rebuilding a text index",
            ));
        };
        let placement = self.part()?.bulk.placement.clone();
        let (tx, rx) = oneshot::channel();
        // One library call that rewrites the whole index: a fence waits for it to end
        // (within its deadline), since it can't stop part way.
        let held = self.gate.as_ref().map(super::gate::WriteGate::extend);
        self.part()?.texts.spawn_rebuild(move || {
            if let Some(p) = placement {
                p.enter();
            }
            let r = index.rebuild();
            drop(index);
            drop(held);
            drop(slot);
            let _ = tx.send(r);
        })?;
        let stats = rx
            .await
            .map_err(|_| ApiError::internal("the rebuild's thread ended early"))??;
        Ok(proto::TextRebuildResponse {
            documents: stats.documents,
            changed: stats.changed,
            invalid_utf8: stats.invalid_utf8,
        })
    }

    /// `Text.Verify`: checks the index against itself and its source, on the blocking pool
    /// (as `Vectors.Verify` does).
    async fn text_verify(
        &self,
        r: proto::TextIndexRequest,
    ) -> Result<proto::TextVerifyResponse, ApiError> {
        let index = self.text_index(&r.index).await?;
        self.blocking(move || {
            let s = index.verify()?;
            Ok(proto::TextVerifyResponse {
                documents: s.documents,
                terms: s.terms,
                postings: s.postings,
                forward_mismatch: s.forward_mismatch,
                source_mismatch: s.source_mismatch,
            })
        })
        .await
    }

    /// `Vectors.Verify`: checks the index's lists against its vectors, on the blocking pool.
    async fn vector_verify(
        &self,
        r: proto::VectorVerifyRequest,
    ) -> Result<proto::VectorVerifyResponse, ApiError> {
        let index = self.index(&r.index).await?;
        self.blocking(move || {
            let stats = index.verify()?;
            Ok(proto::VectorVerifyResponse {
                vectors: stats.vectors,
                lists: stats.list_lens.len() as u64,
            })
        })
        .await
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
    ) -> Result<(proto::ReadRowResponse, proto::ReadRowRequest), ApiError> {
        let pinned = self.pinned_selection(r.selection.as_ref(), caller)?;
        let table = self.table(&r.table).await?;
        let budget = page_bytes(r.page_bytes);
        let cells_cap = if r.page_cells == 0 {
            usize::MAX
        } else {
            r.page_cells as usize
        };
        // The request comes back with the reply, for a strict path's second read (#1661).
        self.blocking(move || {
            let reply = (|| -> Result<proto::ReadRowResponse, ApiError> {
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
                    if !first
                        && (bytes + part_bytes > budget || cells + part.cells.len() > cells_cap)
                    {
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
            })()?;
            Ok((reply, r))
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
        let slot = self.scan_slot()?;
        convert::Select::new(r.selection.as_ref())?;
        // A pinned snapshot is held for the stream's life, and checked before each page: a
        // page that starts after its `Release` gets NotFound.
        let pinned = self.pinned_selection(r.selection.as_ref(), caller)?;
        let opener = self.scan_opener(&r.table).await?;
        // The request's bytes are decoded and checked: the stream holds pages, not them.
        drop(budget);
        opener.open(r, Some(slot), pinned, Some(Arc::clone(&self.metrics)))
    }

    /// A `--max-scans` slot, or `Overloaded`.
    pub(crate) fn scan_slot(&self) -> Result<OwnedSemaphorePermit, ApiError> {
        Arc::clone(&self.scans)
            .try_acquire_owned()
            .map_err(|_| ApiError::overloaded("the server is at its concurrent scan limit"))
    }

    /// What a scan of `table` needs, owned, so the scan can start later on a task of its own:
    /// a cluster's fanned-out scan resolves its local partition's table at the request, and
    /// starts reading it when the scan reaches that partition (D317 ticket 4a).
    pub(crate) async fn scan_opener(&self, table: &str) -> Result<ScanOpener, ApiError> {
        Ok(ScanOpener {
            table: self.table(table).await?,
            pool: Arc::clone(&self.blocking),
            reads: Arc::clone(&self.scan_reads),
            inflight: Arc::clone(&self.inflight),
            limits: self.limits,
        })
    }

    /// The pinned snapshot `id` names, held for one read; `None` for an empty id (now).
    fn pinned(&self, id: &str, caller: &Caller) -> Result<Option<SnapshotGuard>, ApiError> {
        if id.is_empty() {
            return Ok(None);
        }
        self.sessions.snapshot(id, caller.principal).map(Some)
    }

    pub(crate) fn pinned_selection(
        &self,
        s: Option<&proto::Selection>,
        caller: &Caller,
    ) -> Result<Option<SnapshotGuard>, ApiError> {
        self.pinned(s.map_or("", |s| s.snapshot_id.as_str()), caller)
    }

    /// The memory store, opened on the blocking pool if the slot holds none (#1618), or
    /// `Unsupported` without `--memory`. On a replica without the store: `ReplicaBehind`
    /// while its copy is still arriving, else `Unavailable` (the primary hasn't created it,
    /// or the replica hasn't applied that yet; ICR 0112).
    async fn memory_core(&self) -> Result<Arc<crate::memcore::MemCore>, ApiError> {
        let slot = self.part()?.memory.clone().ok_or_else(|| {
            ApiError::unsupported("this server has no memory store (phdb serve --memory NAME)")
        })?;
        let entry = match slot.cached() {
            Some(entry) => entry,
            None => {
                // Under the memory sub-limit (M6): a call waiting for another's open waits on
                // the semaphore, not in a blocking-pool thread (cli692's R2 on #1639).
                let slot = Arc::clone(&slot);
                self.memory_blocking(move || slot.get()).await?
            }
        };
        self.entry_core(&slot, entry)
    }

    /// [`memory_core`](Self::memory_core) on a blocking thread that already holds a memory
    /// place (M6) and a pool place: a `/mcp` memory tool's (8c-3b). It opens the store here
    /// if need be.
    pub(crate) fn memory_core_blocking(&self) -> Result<Arc<crate::memcore::MemCore>, ApiError> {
        let slot = self.part()?.memory.as_ref().ok_or_else(|| {
            ApiError::unsupported("this server has no memory store (phdb serve --memory NAME)")
        })?;
        let entry = slot.get()?;
        self.entry_core(slot, entry)
    }

    /// The core a slot's entry holds, or why there's none.
    fn entry_core(
        &self,
        slot: &super::memslot::MemSlot,
        entry: super::memslot::Entry,
    ) -> Result<Arc<crate::memcore::MemCore>, ApiError> {
        use super::memslot::Entry;
        match entry {
            Entry::Core(core) => Ok(core),
            Entry::Failed(e) => Err(e),
            Entry::Missing => {
                let name = slot.name();
                match self.replica.as_ref().and_then(|r| r.settling()) {
                    Some(state) => Err(ApiError::replica_behind(format!(
                        "this replica is {state}: memory store {name} isn't on it yet"
                    ))),
                    None => Err(ApiError::unavailable(format!(
                        "memory store {name} isn't on this replica yet: the primary hasn't \
                         created it, or the replica hasn't applied that yet"
                    ))),
                }
            }
        }
    }

    /// `f` on the blocking pool under the memory sub-limit (M6). The wait for a memory slot
    /// counts against the request timeout.
    async fn memory_blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
    ) -> Result<T, ApiError> {
        let slot = self.memory_place().await?;
        let r = self.blocking(f).await;
        drop(slot);
        r
    }

    /// A place under the memory sub-limit (M6), waited for within the request timeout. Taken
    /// before a blocking-pool place, always: `/mcp` takes it before its request's pool thread
    /// too, so the two are never waited for in the other order.
    pub(crate) async fn memory_place(&self) -> Result<OwnedSemaphorePermit, ApiError> {
        budget_within(&self.memory_calls, 1, self.limits.request_timeout)
            .await
            .map_err(|_| ApiError::overloaded("the server's memory calls are all busy"))
    }

    /// A place under the memory sub-limit now, or `Overloaded`: for a call already on a
    /// blocking-pool thread, which mustn't wait for one (`/mcp`'s fallback, ai1249's R1 on
    /// #1679).
    pub(crate) fn memory_try_place(&self) -> Result<OwnedSemaphorePermit, ApiError> {
        Arc::clone(&self.memory_calls)
            .try_acquire_owned()
            .map_err(|_| ApiError::overloaded("the server's memory calls are all busy"))
    }

    /// A memory write: detached (its real outcome even if the client goes away), on the
    /// blocking pool under the memory sub-limit, with the caller's namespaces.
    async fn memory_write<T: Send + std::fmt::Debug + 'static>(
        &self,
        rpc: &'static str,
        caller: &Caller,
        budget: OwnedSemaphorePermit,
        f: impl FnOnce(&crate::memcore::MemCore, crate::memcore::Scope<'_>) -> Result<T, ApiError>
        + Send
        + 'static,
    ) -> Result<T, ApiError> {
        let core = self.memory_core().await?;
        let namespaces = caller.namespaces.clone();
        let (calls, pool, wait) = (
            Arc::clone(&self.memory_calls),
            Arc::clone(&self.blocking),
            self.limits.request_timeout,
        );
        self.detached(rpc, budget, async move {
            let _slot = budget_within(&calls, 1, wait)
                .await
                .map_err(|_| ApiError::overloaded("the server's memory calls are all busy"))?;
            blocking(&pool, move || {
                f(&core, crate::memcore::Scope(namespaces.as_deref()))
            })
            .await
        })
        .await
    }

    /// `Memory.Remember`: a memory in its namespace, embedded by the server if it has an
    /// embedder. Detached, as every write.
    async fn remember(
        &self,
        r: proto::RememberRequest,
        caller: &Caller,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::RememberResponse, ApiError> {
        let m = super::memory::remember(r)?;
        self.memory_write("Memory.Remember", caller, budget, move |core, scope| {
            let (done, _) = core.remember(scope, m, None)?;
            Ok(super::memory::remembered(&done))
        })
        .await
    }

    /// `Memory.Recall`: the best memories for a query, within a byte budget (the server's
    /// default when the request sets none, so the reply is bounded).
    async fn recall(
        &self,
        r: proto::RecallRequest,
        caller: &Caller,
    ) -> Result<(proto::RecallResponse, Option<super::charge::Charge>), ApiError> {
        let core = self.memory_core().await?;
        // A budget above the default page reserves its share before a memory-call slot
        // (cli692's points on #1567): a recall waiting for budget doesn't hold one. A smaller
        // one is charged once built.
        let cap = usize::try_from(core.reply_cap(r.byte_budget)).unwrap_or(usize::MAX);
        let charge = if cap > FAST_CAP {
            Some(self.reserve_reply(caller, cap).await?)
        } else {
            None
        };
        let a = super::memory::recall(r)?;
        let namespaces = caller.namespaces.clone();
        let reply = self
            .memory_blocking(move || {
                let (hits, _stats) =
                    core.recall(crate::memcore::Scope(namespaces.as_deref()), a)?;
                Ok(proto::RecallResponse {
                    hits: hits.into_iter().map(super::memory::hit).collect(),
                })
            })
            .await?;
        Ok((reply, charge))
    }

    /// `Memory.Watch`: new memories in some namespaces, under a `--max-streams` slot.
    async fn memory_watch(
        &self,
        r: proto::MemoryWatchRequest,
        caller: &Caller,
    ) -> Result<super::memory::MemoryEvents, ApiError> {
        // Before the store: a swap from here on ends the watch.
        let swaps = self.part()?.memory.as_ref().map(|m| m.swaps());
        let core = self.memory_core().await?;
        let Some(swaps) = swaps else {
            return Err(ApiError::unsupported(
                "this server has no memory store (phdb serve --memory NAME)",
            ));
        };
        let Ok(slot) = Arc::clone(&self.streams).try_acquire_owned() else {
            return Err(ApiError::overloaded(
                "the server is at its concurrent stream limit (--max-streams)",
            ));
        };
        super::memory::watch(
            core,
            crate::memcore::Scope(caller.namespaces.as_deref()),
            r,
            slot,
            Arc::clone(&self.blocking),
            self.limits.stream_idle,
            swaps,
        )
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
        if r.allow_partial || !r.skip_through.is_empty() || r.checkpoint_every != 0 {
            return Err(ApiError::invalid(
                "allow_partial, skip_through and checkpoint_every are for a cluster \
                 subscription (D317), and this server isn't a cluster node",
            ));
        }
        drop(budget);
        self.subscribe_part(r, caller, 0, false).await
    }

    /// A subscription of this server's database: a single node's `Feed.Subscribe`, or one
    /// partition's part of a cluster subscription (D317 ticket 7), whose `skip` passes over
    /// commits the client already handled ([`super::feed::Spec::skip`]) and which
    /// `announce`s its start ([`super::feed::Spec::announce`]). Holds a `--max-streams`
    /// place either way.
    pub(crate) async fn subscribe_part(
        &self,
        r: proto::SubscribeRequest,
        caller: &Caller,
        skip: u64,
        announce: bool,
    ) -> Result<super::feed::FeedEvents, ApiError> {
        use super::feed::Start;
        use proto::subscribe_request::Start as S;
        let slot = self.stream_slot()?;
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
                    .snapshot(&id, caller.principal)?
                    .snapshot()
                    .clone(),
            ),
            Some(S::Resume(token)) => {
                Start::Token(pigeonhole::FeedToken::from_bytes(token.to_vec()))
            }
        };
        let metrics = Arc::clone(&self.metrics);
        let started = Instant::now();
        self.part()?.feeds.subscribe(
            super::feed::Spec {
                tables,
                prefix: r.prefix.to_vec(),
                visible: r.visible,
                start,
                skip,
                announce,
            },
            slot,
            move |e| metrics.count(FEED, e.map(|e| e.code.as_str()), started.elapsed()),
        )
    }

    /// The feed fan-out's limits: a cluster subscription's buffer and idle limit are a
    /// single client's (D317 ticket 7), on a node that only routes too.
    pub(crate) fn feed_limits(&self) -> super::feed::FeedLimits {
        feed_limits(&self.limits)
    }

    /// `Transactions.Begin`: a transaction at a snapshot taken now, held for the caller.
    fn begin(
        &self,
        r: proto::BeginRequest,
        caller: &Caller,
    ) -> Result<proto::BeginResponse, ApiError> {
        let txn = self.part()?.db.transaction()?;
        let snapshot_seqno = txn.snapshot_seqno();
        let txn_id =
            self.sessions
                .begin(caller.principal, txn, convert::durability(r.durability))?;
        Ok(proto::BeginResponse {
            txn_id,
            snapshot_seqno,
        })
    }

    /// Takes transaction `id` for a call that only an open transaction answers.
    fn open_txn(&self, id: &str, caller: &Caller) -> Result<super::sessions::TxnGuard, ApiError> {
        match self.sessions.take(id, caller.principal)? {
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
        if !rows.is_empty() {
            g.wrote_now();
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
        if !r.rows.is_empty() && !caller.scope.allows(Scope::Write) {
            return Err(ApiError::permission_denied("commit writes"));
        }
        let rows = self.txn_rows(&r.rows).await?;
        let mut g = match self.sessions.take(&r.txn_id, caller.principal)? {
            Taken::Open(g) => g,
            Taken::Done(outcome) => {
                return outcome.map(|c| proto::TxnCommitResponse {
                    commit: Some(convert::commit_info(c)),
                });
            }
        };
        // A commit that writes is a write to a fenced partition (D317 ticket 6); a
        // read-only one is a consistent read. Refused, the transaction stays open.
        let held = match &self.gate {
            Some(gate) if g.wrote() || !rows.is_empty() => Some(gate.enter()?),
            _ => None,
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
            drop(held);
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
            self.part()?.db.durable_snapshot()?
        } else {
            self.part()?.db.snapshot()?
        };
        let seqno = snapshot.seqno();
        let snapshot_id = self.sessions.open_snapshot(caller.principal, snapshot)?;
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
        let db = self.part()?.db.clone();
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

/// The largest reply cap a unary read builds before it reserves (#1567, cli692's H1): the
/// default page. A larger one reserves its cap first.
const FAST_CAP: usize = DEFAULT_PAGE_BYTES as usize;

/// A page-budgeted read's reply cap, as the read applies it (#1567).
fn cap(asked: u64) -> usize {
    usize::try_from(page_bytes(asked)).unwrap_or(usize::MAX)
}

pub(crate) fn page_bytes(asked: u64) -> u64 {
    if asked == 0 {
        DEFAULT_PAGE_BYTES
    } else {
        asked.min(MAX_PAGE_BYTES)
    }
}

/// Runs a sync-only call on the blocking pool, holding one of `pool`'s permits.
pub(crate) async fn blocking<T: Send + 'static>(
    pool: &Arc<Semaphore>,
    f: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
) -> Result<T, ApiError> {
    held(pool_place(pool).await?, f).await
}

/// One of `pool`'s permits: a place on the blocking pool.
async fn pool_place(pool: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit, ApiError> {
    Arc::clone(pool)
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal("the blocking pool closed"))
}

/// Runs a sync-only call on the blocking pool, in the place `_place` holds.
async fn held<T: Send + 'static>(
    _place: OwnedSemaphorePermit,
    f: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
) -> Result<T, ApiError> {
    tokio::task::spawn_blocking(|| {
        // A reused pool thread takes a mask published since it started (D317 ticket 6).
        super::placement::refresh();
        f()
    })
    .await
    .map_err(|e| ApiError::internal(format!("a blocking call failed: {e}")))?
}

/// A table's scan, ready to start: see [`Service::scan_opener`].
pub(crate) struct ScanOpener {
    table: Table,
    pool: Arc<Semaphore>,
    reads: Arc<Semaphore>,
    inflight: Arc<Semaphore>,
    limits: Limits,
}

impl ScanOpener {
    /// Starts `r`'s scan: its pages, read one ahead at most by a task of its own, then its
    /// error if it fails. A failure before the first page (a bad position) is returned here.
    /// The scan holds `slot` (a `--max-scans` slot) until its stream ends; one partition's
    /// part of a cluster scan holds none, since the fanned-out stream holds it. With
    /// `metrics`, its outcome is counted as a `Data.Scan` when it ends.
    pub(crate) fn open(
        &self,
        r: proto::ScanRequest,
        slot: Option<OwnedSemaphorePermit>,
        pinned: Option<SnapshotGuard>,
        metrics: Option<Arc<Metrics>>,
    ) -> Result<ScanPages, ApiError> {
        let after = if r.after.is_empty() {
            None
        } else {
            super::cluster::position::refuse_on_a_single_node(&r.after)?;
            Some(ScanPosition::from_bytes(&r.after).map_err(ApiError::from)?)
        };
        let (tx, rx) = mpsc::channel(1);
        let (end_tx, end_rx) = oneshot::channel();
        let task = ScanTask {
            table: self.table.clone(),
            r,
            after,
            pinned,
            pool: Arc::clone(&self.pool),
            reads: Arc::clone(&self.reads),
            inflight: Arc::clone(&self.inflight),
            limits: self.limits,
            pages: tx,
        };
        let started = Instant::now();
        tokio::spawn(async move {
            // Its own task, so a panic is a `JoinError` here, and the stream ends with
            // `Internal` rather than looking like a clean end (simp724's B1 on #1510).
            let outcome = match tokio::spawn(task.run()).await {
                Ok(outcome) => outcome.map_err(ApiError::for_read),
                Err(e) => Err(ApiError::internal(format!("the scan failed: {e}"))),
            };
            if let Some(metrics) = metrics {
                metrics.count(
                    SCAN,
                    outcome.as_ref().err().map(|e| e.code.as_str()),
                    started.elapsed(),
                );
            }
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
                missing: Vec::new(),
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
pub(crate) async fn room<'a>(
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
        unreconciled: Vec::new(),
    }
}

/// A row's size by D308's measure as the converted cells give it: its key and its cells.
fn row_bytes(row: &proto::Row) -> u64 {
    row.key.len() as u64 + row.cells.iter().map(convert::cell_bytes).sum::<u64>()
}

/// Takes `size` bytes of `inflight`, waiting at most `wait`.
pub(crate) async fn budget_within(
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

/// Creates table `name` with `families` (with `if_missing`, opens it with the families it
/// lacks added): a sync call, on a blocking thread. An empty name is refused here for every
/// caller (`Schema.CreateTable` refuses it earlier, before its families are read).
fn create_in(
    db: &Pigeonhole,
    name: &str,
    families: Vec<(String, pigeonhole::Family)>,
    if_missing: bool,
) -> Result<Table, ApiError> {
    if name.is_empty() {
        return Err(ApiError::invalid("empty table name"));
    }
    let mut b = db.table(name)?;
    for (n, f) in families {
        b = b.family(&n, f);
    }
    Ok(if if_missing {
        b.create_if_missing()?
    } else {
        b.create()?
    })
}

/// Refuses a name with the cluster's reserved prefix (`__phdb_`, ICR 0117): its tables
/// (`__phdb_cluster`, a partition's identity) are the server's own, never a user call's.
fn reserved(name: &str) -> Result<(), ApiError> {
    if name.starts_with(super::cluster::map::RESERVED_PREFIX) {
        return Err(ApiError::invalid(format!(
            "table {name:?}: the {} prefix is reserved",
            super::cluster::map::RESERVED_PREFIX
        )));
    }
    Ok(())
}
