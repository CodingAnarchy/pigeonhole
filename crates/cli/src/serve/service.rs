//! The shared service layer (D313 §2): every protocol's calls end here, as one typed
//! operation (`ServiceOp`) answered by one typed reply. gRPC and HTTP/JSON only decode into
//! these and encode out of them, so neither wraps the other.
//!
//! **Writes run detached** (simp724's B2): each commit is spawned as its own task, and the
//! request awaits its outcome. A client that goes away loses only the reply; the commit lands
//! or fails as it would have, and a lost reply is logged and counted.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use pigeonhole::{KeySet, KeysPosition, Pigeonhole, RowPosition, Table};
use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::auth::{Caller, Scope};
use super::config::Limits;
use super::convert;
use super::errors::ApiError;
use super::proto;

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
        }
    }

    /// The RPC's name, `Service.Method`, for logs.
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Get(_) => "Data.Get",
            Self::MultiGet(_) => "Data.MultiGet",
            Self::ReadRow(_) => "Data.ReadRow",
            Self::Mutate(_) => "Data.Mutate",
            Self::CheckAndMutate(_) => "Data.CheckAndMutate",
            Self::Batch(_) => "Data.Batch",
            Self::MutateReturning(_) => "Data.MutateReturning",
        }
    }

    fn scope(&self) -> Scope {
        match self {
            Self::Get(_) | Self::MultiGet(_) | Self::ReadRow(_) => Scope::Read,
            _ => Scope::Write,
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
        }
    }
}

/// The RPCs, by `ServiceOp::index`.
pub(crate) const RPCS: [&str; 7] = [
    "Data.Get",
    "Data.MultiGet",
    "Data.ReadRow",
    "Data.Mutate",
    "Data.CheckAndMutate",
    "Data.Batch",
    "Data.MutateReturning",
];

/// The outcomes a request is counted under: `ok`, every code ICR 0112's table names, and
/// `other` for a code a newer engine adds.
pub(crate) const OUTCOMES: [&str; 40] = [
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
    "Internal",
    "other",
];

/// Request counts by RPC and outcome, sheds, and the replies lost after a write's outcome
/// was known. A fixed table of atomics: one relaxed add per request, no lock and no
/// allocation (perf287's F3 on #1501). Ticket 9 moves these onto D316's names.
#[derive(Debug)]
pub(crate) struct Metrics {
    requests: [[AtomicU64; OUTCOMES.len()]; RPCS.len()],
    pub(crate) shed: AtomicU64,
    pub(crate) replies_lost: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            requests: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            shed: AtomicU64::new(0),
            replies_lost: AtomicU64::new(0),
        }
    }
}

impl Metrics {
    /// Counts one request of RPC `rpc` (`ServiceOp::index`) with outcome `code` (`None`: ok).
    pub(crate) fn count(&self, rpc: usize, code: Option<&str>) {
        let outcome = match code {
            None => 0,
            Some(c) => OUTCOMES
                .iter()
                .position(|o| *o == c)
                .unwrap_or(OUTCOMES.len() - 1),
        };
        if let Some(row) = self.requests.get(rpc) {
            row[outcome].fetch_add(1, Ordering::Relaxed);
        }
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
            db,
            tables: ArcSwap::from_pointee(HashMap::new()),
            inflight: Arc::new(Semaphore::new(
                limits.inflight_bytes.min(Semaphore::MAX_PERMITS),
            )),
            blocking: Arc::new(Semaphore::new(blocking_threads.max(1))),
            limits,
            metrics,
        }
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
        // the request's deadline; nothing has been submitted while it waits. HTTP reserves
        // it from `Content-Length` before reading the body; gRPC's is taken here, after
        // tonic decoded the message, until ticket 3b reserves it from the HTTP/2 message
        // length (perf287's F2 on #1501). Meanwhile each connection's HTTP/2 streams are
        // capped (`listener::MAX_STREAMS`).
        // Reads hold the budget until their reply is built; a write hands it to its
        // detached task, so a client that goes away doesn't free bytes the write still
        // holds (simp724's S1 on #1501).
        let budget = match prepaid {
            Some(p) => p,
            None => self.budget(size).await?,
        };
        match op {
            ServiceOp::Get(r) => {
                let reply = self.get(r).await.map(ServiceReply::Get);
                drop(budget);
                reply
            }
            ServiceOp::MultiGet(r) => {
                let reply = self.multi_get(r).await.map(ServiceReply::MultiGet);
                drop(budget);
                reply
            }
            ServiceOp::ReadRow(r) => {
                let reply = self.read_row(r).await.map(ServiceReply::ReadRow);
                drop(budget);
                reply
            }
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
        }
    }

    async fn budget(&self, size: usize) -> Result<OwnedSemaphorePermit, ApiError> {
        let n = u32::try_from(size.max(1)).unwrap_or(u32::MAX);
        match tokio::time::timeout(
            self.limits.request_timeout,
            Arc::clone(&self.inflight).acquire_many_owned(n),
        )
        .await
        {
            Ok(Ok(p)) => Ok(p),
            Ok(Err(_)) => Err(ApiError::internal("the in-flight budget closed")),
            Err(_) => Err(ApiError::overloaded(
                "the server's in-flight request bytes stayed full past the deadline",
            )),
        }
    }

    /// The table `name`, opened on first use. A miss opens it on the blocking pool (an open
    /// is a sync call, D196) and swaps in a new map.
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

    /// Runs a sync-only call on the blocking pool, at most `--blocking-threads` at once.
    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
    ) -> Result<T, ApiError> {
        let _permit = Arc::clone(&self.blocking)
            .acquire_owned()
            .await
            .map_err(|_| ApiError::internal("the blocking pool closed"))?;
        tokio::task::spawn_blocking(f)
            .await
            .map_err(|e| ApiError::internal(format!("a blocking call failed: {e}")))?
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

    async fn get(&self, r: proto::GetRequest) -> Result<proto::GetResponse, ApiError> {
        if !r.snapshot_id.is_empty() {
            return Err(ApiError::not_found("no such snapshot"));
        }
        let table = self.table(&r.table).await?;
        let cell = table.get_async(&r.row, &r.family, &r.qualifier).await?;
        Ok(proto::GetResponse {
            cell: cell.map(|c| convert::cell(&r.family, &r.qualifier, &c, r.aggregate.as_ref())),
        })
    }

    /// On the blocking pool: `KeysRead::read_within` is sync-only (D196). TODO(#398, #1286):
    /// async once multi-get pages have an async form, so a multi-get holds no blocking thread.
    async fn multi_get(
        &self,
        r: proto::MultiGetRequest,
    ) -> Result<proto::MultiGetResponse, ApiError> {
        let table = self.table(&r.table).await?;
        let budget = page_bytes(r.page_bytes);
        self.blocking(move || {
            let select = convert::Select::new(r.selection.as_ref())?;
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

    /// A row's selected cells, a page at a time: whole columns, at most `page_bytes` by
    /// D308's measure (perf287's S1) and `page_cells` cells, resumed from `after`. The
    /// first column always comes back, so a page makes progress even when one column is
    /// larger than the budget. On the blocking pool, since row pages are sync-only (D196).
    /// TODO(#398, #1286): `read_async`-based paging, so a row read holds no blocking thread.
    async fn read_row(&self, r: proto::ReadRowRequest) -> Result<proto::ReadRowResponse, ApiError> {
        let table = self.table(&r.table).await?;
        let budget = page_bytes(r.page_bytes);
        let cells_cap = if r.page_cells == 0 {
            usize::MAX
        } else {
            r.page_cells as usize
        };
        self.blocking(move || {
            let select = convert::Select::new(r.selection.as_ref())?;
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
