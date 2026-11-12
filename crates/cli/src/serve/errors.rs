//! Errors as ICR 0112's table says: one stable `code`, a gRPC status and an HTTP status per
//! code, and whether (and from where) the request may be retried. Every error the server
//! sends is built here, so the table has one implementation.

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use pigeonhole::ErrorCode;
use prost::Message;

use super::proto::{ErrorDetail, RetryFrom};

/// An error response.
#[derive(Debug, Clone)]
pub(crate) struct ApiError {
    pub code: String,
    pub code_num: u32,
    pub message: String,
    pub grpc: tonic::Code,
    pub http: StatusCode,
    pub retry_from: RetryFrom,
    pub retry_after_ms: u32,
}

/// The default pause `Busy` and `Overloaded` suggest.
const RETRY_AFTER_MS: u32 = 100;

impl ApiError {
    fn new(
        code: &str,
        code_num: u32,
        message: impl Into<String>,
        grpc: tonic::Code,
        http: StatusCode,
    ) -> Self {
        Self {
            code: code.to_owned(),
            code_num,
            message: message.into(),
            grpc,
            http,
            retry_from: RetryFrom::Unspecified,
            retry_after_ms: 0,
        }
    }

    fn retry(mut self, from: RetryFrom) -> Self {
        self.retry_from = from;
        self
    }

    fn after(mut self, ms: u32) -> Self {
        self.retry_after_ms = ms;
        self
    }

    /// An engine error, by ICR 0112's table. Which codes guarantee a write wasn't applied
    /// is fixed there (simp724's E1): everything else on a write is outcome-unknown, so not
    /// retry-safe.
    pub(crate) fn from_engine(e: &pigeonhole::Error) -> Self {
        Self::from_code(e.code(), e.message())
    }

    /// An error with `code` and `message`, mapped as [`from_engine`](Self::from_engine) maps
    /// an engine error: for the companion crates' errors, which carry a code of the same
    /// table (`pigeonhole_vector::Error::code`).
    pub(crate) fn from_code(code: ErrorCode, message: &str) -> Self {
        use tonic::Code as G;
        let (grpc, http, retry) = match code {
            ErrorCode::Conflict => (G::Aborted, StatusCode::CONFLICT, RetryFrom::Begin),
            ErrorCode::Busy => (
                G::Unavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                RetryFrom::Request,
            ),
            ErrorCode::SnapshotExpired => (G::Aborted, StatusCode::CONFLICT, RetryFrom::Begin),
            ErrorCode::Closed => (
                G::Unavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                RetryFrom::Request,
            ),
            ErrorCode::OutcomeUnknown => (
                G::Unknown,
                StatusCode::INTERNAL_SERVER_ERROR,
                RetryFrom::Unspecified,
            ),
            ErrorCode::TableNotFound | ErrorCode::FamilyNotFound => {
                (G::NotFound, StatusCode::NOT_FOUND, RetryFrom::Unspecified)
            }
            ErrorCode::TableExists | ErrorCode::FamilyExists => (
                G::AlreadyExists,
                StatusCode::CONFLICT,
                RetryFrom::Unspecified,
            ),
            ErrorCode::InvalidArgument
            | ErrorCode::KeyTooLarge
            | ErrorCode::ValueTooLarge
            | ErrorCode::RecordTooLarge
            | ErrorCode::BatchTooLarge => (
                G::InvalidArgument,
                StatusCode::BAD_REQUEST,
                RetryFrom::Unspecified,
            ),
            ErrorCode::ReadOnly | ErrorCode::NoWriter | ErrorCode::WouldDeadlock => (
                G::FailedPrecondition,
                StatusCode::CONFLICT,
                RetryFrom::Unspecified,
            ),
            ErrorCode::FeedExpired => (G::OutOfRange, StatusCode::GONE, RetryFrom::Unspecified),
            ErrorCode::Unsupported
            | ErrorCode::UnsupportedFeature
            | ErrorCode::UnsupportedFormat
            | ErrorCode::UnknownMergeOperator => (
                G::Unimplemented,
                StatusCode::NOT_IMPLEMENTED,
                RetryFrom::Unspecified,
            ),
            ErrorCode::NoSpace => (
                G::ResourceExhausted,
                StatusCode::INSUFFICIENT_STORAGE,
                RetryFrom::Unspecified,
            ),
            ErrorCode::Corruption => (
                G::DataLoss,
                StatusCode::INTERNAL_SERVER_ERROR,
                RetryFrom::Unspecified,
            ),
            ErrorCode::WriterLocked
            | ErrorCode::ShmVersionMismatch
            | ErrorCode::ShmUnavailable
            | ErrorCode::NetworkFilesystem
            | ErrorCode::NoReaderSlot => (
                G::Unavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                RetryFrom::Unspecified,
            ),
            // Io, MergeFailed, and any code a newer engine adds: maybe applied, for a write.
            _ => (
                G::Internal,
                StatusCode::INTERNAL_SERVER_ERROR,
                RetryFrom::Unspecified,
            ),
        };
        let mut err =
            Self::new(&format!("{code:?}"), code as u32, message, grpc, http).retry(retry);
        if code == ErrorCode::Busy {
            err = err.after(RETRY_AFTER_MS);
        }
        err
    }

    /// The same error for a read: a read that failed with `Io` wrote nothing and may pass
    /// on a second try, so it may be sent again (simp724 on #1501). Writes keep "maybe
    /// applied". `MergeFailed` stays no-retry for a read too: a merge operator failing on
    /// stored data fails the same way again (simp724 on #1510).
    pub(crate) fn for_read(mut self) -> Self {
        if self.retry_from == RetryFrom::Unspecified && self.code == "Io" {
            self.retry_from = RetryFrom::Request;
        }
        self
    }

    // ---- Server codes (ICR 0112's table, below the engine's) ----

    pub(crate) fn unauthenticated() -> Self {
        Self::new(
            "Unauthenticated",
            0,
            "a bearer token is required",
            tonic::Code::Unauthenticated,
            StatusCode::UNAUTHORIZED,
        )
    }

    /// A forwarded call whose token the node it's sent to doesn't list (D317 ticket 3). The
    /// digest is never in it (cli692's S1 on ticket 3).
    pub(crate) fn unlisted_token() -> Self {
        Self::new(
            "PermissionDenied",
            0,
            "the call was forwarded for a token this node doesn't list: a cluster's nodes \
             list the same tokens"
                .to_owned(),
            tonic::Code::PermissionDenied,
            StatusCode::FORBIDDEN,
        )
    }

    pub(crate) fn permission_denied(what: &str) -> Self {
        Self::new(
            "PermissionDenied",
            0,
            format!("this token's scope can't {what}"),
            tonic::Code::PermissionDenied,
            StatusCode::FORBIDDEN,
        )
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self::new(
            "NotFound",
            0,
            message,
            tonic::Code::NotFound,
            StatusCode::NOT_FOUND,
        )
    }

    /// A call this server's state refuses (a replica's write, `Promote` on a primary): as
    /// the engine's `ReadOnly`, it changed nothing and fails the same way again.
    pub(crate) fn failed_precondition(message: impl Into<String>) -> Self {
        Self::new(
            "FailedPrecondition",
            0,
            message,
            tonic::Code::FailedPrecondition,
            StatusCode::CONFLICT,
        )
    }

    /// A read with `x-pigeonhole-min-primary-seqno` that the replica couldn't serve in time
    /// (#1577): nothing was read, so it may be sent again (to this replica, or the primary).
    pub(crate) fn replica_behind(message: impl Into<String>) -> Self {
        Self::new(
            "ReplicaBehind",
            0,
            message,
            tonic::Code::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
        )
        .retry(RetryFrom::Request)
        .after(RETRY_AFTER_MS)
    }

    /// A write to a replica server (#1436): the engine's `ReadOnly`, naming the primary
    /// that takes the writes.
    pub(crate) fn replica_read_only(primary: &str) -> Self {
        Self::new(
            "ReadOnly",
            ErrorCode::ReadOnly as u32,
            format!("this server is a replica of {primary}: send writes to the primary"),
            tonic::Code::FailedPrecondition,
            StatusCode::CONFLICT,
        )
    }

    pub(crate) fn overloaded(message: impl Into<String>) -> Self {
        Self::new(
            "Overloaded",
            0,
            message,
            tonic::Code::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
        )
        .retry(RetryFrom::Request)
        .after(RETRY_AFTER_MS)
    }

    /// A service the server depends on failed or can't be reached (today the embedding
    /// endpoint, `--embed-url`): nothing was applied, and the request may be sent again
    /// (ICR 0112's `Unavailable`, #1617).
    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self::new(
            "Unavailable",
            0,
            message,
            tonic::Code::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
        )
        .retry(RetryFrom::Request)
        .after(RETRY_AFTER_MS)
    }

    pub(crate) fn request_too_large(limit: usize) -> Self {
        Self::new(
            "RequestTooLarge",
            0,
            format!("the request is larger than --max-request-bytes ({limit})"),
            tonic::Code::ResourceExhausted,
            StatusCode::PAYLOAD_TOO_LARGE,
        )
    }

    /// The client's own deadline (`grpc-timeout`) passed while the request waited for the
    /// server's limits, before anything was submitted.
    pub(crate) fn deadline_exceeded() -> Self {
        Self::deadline("the request waited past its deadline for the server's limits")
    }

    /// `DeadlineExceeded` with its own message: a scan whose client stopped reading for
    /// `--stream-idle-ms` (perf287's G2 on #1510), resumable from its last page's `next`.
    pub(crate) fn deadline(message: impl Into<String>) -> Self {
        Self::new(
            "DeadlineExceeded",
            0,
            message,
            tonic::Code::DeadlineExceeded,
            StatusCode::GATEWAY_TIMEOUT,
        )
        .retry(RetryFrom::Request)
    }

    /// A second call on a transaction while one is running, or any call while its `Commit`
    /// runs (D313 B2): this call did nothing, so it may be sent again once the first answers.
    pub(crate) fn session_busy() -> Self {
        Self::new(
            "SessionBusy",
            0,
            "another call on this transaction is running; send this one when it answers",
            tonic::Code::FailedPrecondition,
            StatusCode::CONFLICT,
        )
        .retry(RetryFrom::Request)
    }

    /// The transaction's lease ran out before any `Commit` arrived (D313 B1): nothing was
    /// written, so the client starts again from `Begin`.
    pub(crate) fn transaction_expired() -> Self {
        Self::new(
            "TransactionExpired",
            0,
            "the transaction expired before any Commit; begin a new one",
            tonic::Code::Aborted,
            StatusCode::CONFLICT,
        )
        .retry(RetryFrom::Begin)
    }

    /// A call other than `Commit` on a transaction that has received `Commit`: the session
    /// is its tombstone (D313 B1), and `Commit` answers the recorded outcome, which this
    /// error's message names too. Not retry-safe: whatever the call wanted is past.
    pub(crate) fn transaction_finished(outcome: &Result<pigeonhole::CommitInfo, Self>) -> Self {
        let what = match outcome {
            Ok(c) => format!("committed at seqno {}", c.seqno),
            Err(e) => format!("its Commit failed with {}", e.code),
        };
        Self::new(
            "TransactionFinished",
            0,
            format!("the transaction has received Commit ({what}); Commit answers its outcome"),
            tonic::Code::FailedPrecondition,
            StatusCode::CONFLICT,
        )
    }

    /// Something this server doesn't do as configured (a backup to disk without
    /// `--backup-dir`): the engine's `Unsupported` code, not applied, no retry.
    pub(crate) fn unsupported(message: impl Into<String>) -> Self {
        Self::new(
            "Unsupported",
            ErrorCode::Unsupported as u32,
            message,
            tonic::Code::Unimplemented,
            StatusCode::NOT_IMPLEMENTED,
        )
    }

    /// A commit whose outcome the server couldn't learn (its task ended without one).
    pub(crate) fn outcome_unknown(message: impl Into<String>) -> Self {
        Self::new(
            "OutcomeUnknown",
            ErrorCode::OutcomeUnknown as u32,
            message,
            tonic::Code::Unknown,
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    }

    /// D317 (ICR 0117): this node's map no longer gives it the key a forwarded call names.
    /// The sending node pulls the newer map and forwards once more; a client that routes
    /// directly refreshes its map. `map_version` is this node's.
    pub(crate) fn wrong_partition(map_version: u64, message: impl Into<String>) -> Self {
        Self::new(
            "WrongPartition",
            0,
            format!(
                "{} (this node's map is version {map_version})",
                message.into()
            ),
            tonic::Code::FailedPrecondition,
            StatusCode::CONFLICT,
        )
        .retry(RetryFrom::Request)
    }

    /// D317 (ICR 0117): a write or batch whose rows are in more than one partition, refused
    /// before anything is written. Not retryable as written: split it.
    pub(crate) fn cross_partition(message: impl Into<String>) -> Self {
        Self::new(
            "CrossPartition",
            0,
            message,
            tonic::Code::FailedPrecondition,
            StatusCode::CONFLICT,
        )
    }

    /// D317 ticket 6: a write to a partition fenced on its node (`phdb cluster fence`, or a
    /// restarted node's partition before it has a peer's map), refused before anything is
    /// written. Not retried automatically: the partition is being moved or recovered, and
    /// takes writes again once it's unfenced or at its new node.
    pub(crate) fn fenced(message: impl Into<String>) -> Self {
        Self::new(
            "Fenced",
            0,
            message,
            tonic::Code::FailedPrecondition,
            StatusCode::CONFLICT,
        )
    }

    /// D317 (ICR 0117): the partition's node couldn't be reached, and nothing was sent to
    /// it, so a write is safe to retry too (B3: a forward that failed after it was sent
    /// answers `OutcomeUnknown` instead).
    pub(crate) fn partition_unavailable(message: impl Into<String>) -> Self {
        Self::new(
            "PartitionUnavailable",
            0,
            message,
            tonic::Code::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
        )
        .retry(RetryFrom::Request)
        .after(RETRY_AFTER_MS)
    }

    /// A malformed request: missing a required field, or a value out of range.
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::new(
            "InvalidArgument",
            ErrorCode::InvalidArgument as u32,
            message,
            tonic::Code::InvalidArgument,
            StatusCode::BAD_REQUEST,
        )
    }

    /// A bug or an unexpected failure in the server: for a write, maybe applied.
    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(
            "Internal",
            0,
            message,
            tonic::Code::Internal,
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    }

    pub(crate) fn detail(&self) -> ErrorDetail {
        ErrorDetail {
            code: self.code.clone(),
            code_num: self.code_num,
            message: self.message.clone(),
            retry_safe: self.retry_from != RetryFrom::Unspecified,
            retry_after_ms: self.retry_after_ms,
            retry_from: self.retry_from as i32,
        }
    }

    /// The error a peer's status carries, as [`to_status`](Self::to_status) wrote it (D317
    /// ticket 2: a forwarded call's answer passes back as the owner gave it). `None` for a
    /// status without an `ErrorDetail`, a transport failure that no server sent.
    pub(crate) fn from_status(status: &tonic::Status) -> Option<Self> {
        let rpc = RpcStatus::decode(status.details()).ok()?;
        let detail = rpc
            .details
            .iter()
            .find(|a| a.type_url == "type.googleapis.com/pigeonhole.v1.ErrorDetail")
            .and_then(|a| ErrorDetail::decode(a.value.as_slice()).ok())?;
        let grpc = status.code();
        Some(Self {
            http: http_for(grpc, &detail.code),
            code: detail.code,
            code_num: detail.code_num,
            message: detail.message,
            grpc,
            retry_from: RetryFrom::try_from(detail.retry_from).unwrap_or(RetryFrom::Unspecified),
            retry_after_ms: detail.retry_after_ms,
        })
    }

    /// As a gRPC status, with the `google.rpc.Status` details ICR 0112 promises: the
    /// `ErrorDetail`, and a `RetryInfo` when there's a pause to suggest.
    pub(crate) fn to_status(&self) -> tonic::Status {
        let mut details = vec![Any {
            type_url: "type.googleapis.com/pigeonhole.v1.ErrorDetail".into(),
            value: self.detail().encode_to_vec(),
        }];
        if self.retry_after_ms > 0 {
            let delay = RpcDuration {
                seconds: i64::from(self.retry_after_ms / 1000),
                nanos: i32::try_from((self.retry_after_ms % 1000) * 1_000_000).unwrap_or(0),
            };
            details.push(Any {
                type_url: "type.googleapis.com/google.rpc.RetryInfo".into(),
                value: RetryInfo {
                    retry_delay: Some(delay),
                }
                .encode_to_vec(),
            });
        }
        let status = RpcStatus {
            code: self.grpc as i32,
            message: self.message.clone(),
            details,
        };
        tonic::Status::with_details(
            self.grpc,
            self.message.clone(),
            status.encode_to_vec().into(),
        )
    }
}

impl From<pigeonhole::Error> for ApiError {
    fn from(e: pigeonhole::Error) -> Self {
        Self::from_engine(&e)
    }
}

/// The vector index's errors keep their code: an engine error as the engine's, the crate's
/// own (`InvalidArgument`, `Conflict`, `Corruption`) under the same table.
impl From<pigeonhole_vector::Error> for ApiError {
    fn from(e: pigeonhole_vector::Error) -> Self {
        match e {
            pigeonhole_vector::Error::Db(e) => Self::from_engine(&e),
            e => Self::from_code(e.code(), &e.to_string()),
        }
    }
}

/// The text index's errors keep their code too: an engine error as the engine's, the crate's
/// own (`InvalidArgument` for a bad query, `Busy` during a repair, `Corruption`) under the
/// same table.
impl From<pigeonhole_text::Error> for ApiError {
    fn from(e: pigeonhole_text::Error) -> Self {
        match e {
            pigeonhole_text::Error::Db(e) => Self::from_engine(&e),
            e => Self::from_code(e.code(), &e.to_string()),
        }
    }
}

impl From<ApiError> for tonic::Status {
    fn from(e: ApiError) -> Self {
        e.to_status()
    }
}

impl IntoResponse for ApiError {
    /// As HTTP/JSON: the status, the `ErrorDetail` as the body, and `Retry-After`.
    fn into_response(self) -> Response {
        let body = serde_json::to_vec(&self.detail()).unwrap_or_default();
        let mut resp = (self.http, body).into_response();
        resp.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        if self.retry_after_ms > 0 {
            let secs = self.retry_after_ms.div_ceil(1000).max(1);
            if let Ok(v) = HeaderValue::from_str(&secs.to_string()) {
                resp.headers_mut().insert(header::RETRY_AFTER, v);
            }
        }
        resp
    }
}

// `google.rpc.Status` and the two details it carries, encoded by hand: three small
// messages don't need another crate.

#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<Any>,
}

#[derive(Clone, PartialEq, Message)]
struct Any {
    #[prost(string, tag = "1")]
    type_url: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct RetryInfo {
    #[prost(message, optional, tag = "1")]
    retry_delay: Option<RpcDuration>,
}

#[derive(Clone, PartialEq, Message)]
struct RpcDuration {
    #[prost(int64, tag = "1")]
    seconds: i64,
    #[prost(int32, tag = "2")]
    nanos: i32,
}

/// The HTTP status this server gives `grpc` (every code maps to one but `RESOURCE_EXHAUSTED`,
/// which `code` tells apart), for an error a peer answered over gRPC.
fn http_for(grpc: tonic::Code, code: &str) -> StatusCode {
    use tonic::Code as G;
    match grpc {
        G::Ok => StatusCode::OK,
        G::InvalidArgument => StatusCode::BAD_REQUEST,
        G::NotFound => StatusCode::NOT_FOUND,
        G::AlreadyExists | G::Aborted | G::FailedPrecondition => StatusCode::CONFLICT,
        G::PermissionDenied => StatusCode::FORBIDDEN,
        G::Unauthenticated => StatusCode::UNAUTHORIZED,
        G::ResourceExhausted if code == "RequestTooLarge" => StatusCode::PAYLOAD_TOO_LARGE,
        G::ResourceExhausted => StatusCode::INSUFFICIENT_STORAGE,
        G::OutOfRange => StatusCode::GONE,
        G::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        G::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        G::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::{Any, ApiError, RetryFrom, RpcStatus};
    use pigeonhole::ErrorCode;
    use prost::Message;

    #[test]
    fn the_retry_rules_match_icr_0112() {
        let safe = |e: &ApiError| (e.detail().retry_safe, e.retry_from);
        // A conflict reruns the transaction; a busy shard resends after a pause.
        let conflict = ApiError::overloaded("x");
        assert_eq!(safe(&conflict), (true, RetryFrom::Request));
        assert_eq!(conflict.retry_after_ms, 100);
        assert_eq!(
            safe(&ApiError::deadline_exceeded()),
            (true, RetryFrom::Request)
        );
        for e in [
            ApiError::internal("x"),
            ApiError::invalid("x"),
            ApiError::unauthenticated(),
        ] {
            assert_eq!(safe(&e), (false, RetryFrom::Unspecified), "{}", e.code);
        }
        assert_eq!(
            ApiError::invalid("x").code_num,
            ErrorCode::InvalidArgument as u32
        );
    }

    #[test]
    fn a_read_that_failed_on_io_may_be_sent_again() {
        let io = || {
            ApiError::new(
                "Io",
                ErrorCode::Io as u32,
                "x",
                tonic::Code::Unavailable,
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
            )
        };
        // A write that failed on Io may have applied; the same failure on a read changed
        // nothing (ICR 0112).
        assert_eq!(io().retry_from, RetryFrom::Unspecified);
        assert_eq!(io().for_read().retry_from, RetryFrom::Request);
        // Other codes keep their rule; MergeFailed fails the same way again, read or write.
        assert_eq!(
            ApiError::invalid("x").for_read().retry_from,
            RetryFrom::Unspecified
        );
        let merge = ApiError::new(
            "MergeFailed",
            ErrorCode::MergeFailed as u32,
            "x",
            tonic::Code::Internal,
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        );
        assert_eq!(merge.for_read().retry_from, RetryFrom::Unspecified);
    }

    #[test]
    fn a_status_carries_the_error_detail() {
        let status = ApiError::overloaded("shed").to_status();
        assert_eq!(status.code(), tonic::Code::Unavailable);
        let rpc = RpcStatus::decode(status.details()).unwrap();
        assert_eq!(rpc.details.len(), 2, "ErrorDetail and RetryInfo");
        let Any { type_url, value } = &rpc.details[0];
        assert_eq!(type_url, "type.googleapis.com/pigeonhole.v1.ErrorDetail");
        let detail = super::ErrorDetail::decode(value.as_slice()).unwrap();
        assert_eq!(
            (detail.code.as_str(), detail.retry_safe),
            ("Overloaded", true)
        );
        assert_eq!(
            rpc.details[1].type_url,
            "type.googleapis.com/google.rpc.RetryInfo"
        );
    }

    #[test]
    fn an_error_survives_a_trip_through_a_peers_status() {
        // D317 ticket 2: what an owner answers is what the forwarding node answers.
        for e in [
            ApiError::wrong_partition(7, "not here"),
            ApiError::cross_partition("two partitions"),
            ApiError::partition_unavailable("down"),
            ApiError::overloaded("busy"),
            ApiError::outcome_unknown("lost"),
            ApiError::request_too_large(10),
            ApiError::invalid("bad"),
            ApiError::not_found("gone"),
            ApiError::permission_denied("write"),
            ApiError::from_code(ErrorCode::Conflict, "lost the race"),
        ] {
            let back = ApiError::from_status(&e.to_status()).expect("a detail");
            assert_eq!(
                (
                    &back.code,
                    back.code_num,
                    &back.message,
                    back.grpc,
                    back.http,
                    back.retry_from,
                    back.retry_after_ms
                ),
                (
                    &e.code,
                    e.code_num,
                    &e.message,
                    e.grpc,
                    e.http,
                    e.retry_from,
                    e.retry_after_ms
                ),
                "{}",
                e.code
            );
        }
        // A transport failure carries no detail: no server answered it.
        assert!(ApiError::from_status(&tonic::Status::unavailable("connection reset")).is_none());
    }
}
