//! Fan-out reads across a table's partitions (D317 §7, ticket 4a): `Data.Scan` and
//! `Data.MultiGet` on a cluster node.
//!
//! Partitions hold disjoint key ranges in order, so both reads are concatenations in
//! partition order, and both answer what one database holding every partition would:
//! - **A scan** reads one partition at a time (no next-partition prefetch in v1, perf287),
//!   passing each partition's pages through. A page's `next` is a cluster position
//!   ([`super::position`]): the issuing partition and its own position, or, at a partition's
//!   end with more to read, the next partition's start. An empty `next` is the scan's end.
//! - **A multi-get** splits its keys by partition and merges the partitions' pages in order
//!   under one byte budget. The first partition reads with what's left of the budget; the next
//!   one is read ahead with the whole budget, and read again with what's left only if its page
//!   doesn't fit (simp724's S1: at most about twice `page_bytes` is in flight).
//! - **A partition that can't be reached** fails the read with `PartitionUnavailable`, or,
//!   with `allow_partial`, is skipped and named in `missing`.
//!
//! A read with a `snapshot_id` reads its snapshot's partition only (D317 ticket 3): a
//! multi-get is routed as a single-key call, a scan streams that one partition.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};

use super::forward;
use super::position::{Kind, Position};
use super::route::{PartRange, Routes};
use crate::serve::auth::Caller;
use crate::serve::config::Limits;
use crate::serve::errors::ApiError;
use crate::serve::proto::{
    self, ForwardRequest, ForwardScanRequest, MissingRange, MultiGetRequest, MultiGetResponse,
    ScanPage, ScanRequest, forward_request,
};
use crate::serve::router::{Cluster, Router, not_open};
use crate::serve::service::{
    PageSlot, ScanOpener, ScanPages, ServiceOp, ServiceReply, budget_within, page_bytes, room,
};
use tokio::sync::Semaphore;

/// Where one partition is read.
enum Place {
    Here,
    /// Its node's address, and the map version the read goes with.
    There(String, u64),
}

/// The cluster side of a fan-out, owned, so a scan's task can outlive its request.
#[derive(Clone)]
struct Reach {
    node: Arc<super::Node>,
    local: Option<String>,
    forwarder: Arc<forward::Forwarder>,
    /// A scan stream's opening's bound: the request timeout, for each partition in turn.
    timeout: std::time::Duration,
    /// A multi-get's: the request's deadline from its arrival, which every forward and map
    /// pull of it shares (simp724's N1 on #1698, as #1686's forwards do).
    deadline: Option<tokio::time::Instant>,
}

impl Reach {
    fn of(router: &Router, cluster: &Cluster) -> Self {
        Self {
            node: Arc::clone(&cluster.node),
            local: cluster.local.clone(),
            forwarder: Arc::clone(&cluster.forwarder),
            timeout: router.service.request_timeout(),
            deadline: None,
        }
    }

    /// How long the next forward or pull may take: what's left of the deadline, or the
    /// timeout.
    fn within(&self) -> Result<std::time::Duration, ApiError> {
        match self.deadline {
            None => Ok(self.timeout),
            Some(d) => d
                .checked_duration_since(tokio::time::Instant::now())
                .filter(|d| !d.is_zero())
                .ok_or_else(ApiError::deadline_exceeded),
        }
    }

    /// The routes now, or `PartitionUnavailable` before a map.
    fn routes(&self) -> Result<Arc<Routes>, ApiError> {
        self.node
            .routes()
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| ApiError::partition_unavailable("this cluster node has no map yet"))
    }

    /// Where `partition` is read from this node.
    fn place(&self, routes: &Routes, partition: &str) -> Result<Place, ApiError> {
        let owner = routes.owner(partition).ok_or_else(|| {
            ApiError::internal(format!("the map lists no node for partition {partition:?}"))
        })?;
        if owner.node == self.node.id() {
            return if self.local.as_deref() == Some(partition) {
                Ok(Place::Here)
            } else {
                Err(not_open(partition))
            };
        }
        Ok(Place::There(owner.address.clone(), routes.map_version()))
    }

    /// Pulls the map from the node that said it has a newer one (a `WrongPartition`).
    async fn catch_up(
        &self,
        routes: &Routes,
        partition: &str,
        address: &str,
    ) -> Result<(), ApiError> {
        let node = routes
            .owner(partition)
            .map(|o| o.node.clone())
            .unwrap_or_default();
        tokio::time::timeout(
            self.within()?,
            self.node.pull_now(&node, address, routes.map_version()),
        )
        .await
        .map_err(|_| ApiError::deadline_exceeded())?
        .map_err(|why| {
            ApiError::partition_unavailable(format!(
                "node {node} has a newer cluster map, which this node couldn't take: {why}"
            ))
        })
    }
}

/// `table`'s partitions, or `TableNotFound`-like refusal for a table the map doesn't place.
fn ranges_of(routes: &Routes, table: &str) -> Result<Vec<PartRange>, ApiError> {
    routes
        .ranges(table)
        .ok_or_else(|| ApiError::not_found(format!("table {table:?} isn't in the cluster map")))
}

/// The partition `after` names, as an index into `ranges`.
fn start_of(
    ranges: &[PartRange],
    after: Option<&Position>,
    table: &str,
) -> Result<usize, ApiError> {
    let Some(after) = after else { return Ok(0) };
    ranges
        .iter()
        .position(|r| r.partition == after.partition)
        .ok_or_else(|| {
            ApiError::invalid(format!(
                "the position names partition {:?}, which table {table:?} doesn't have",
                after.partition
            ))
        })
}

fn missing(table: &str, r: &PartRange) -> MissingRange {
    MissingRange {
        partition: r.partition.clone(),
        table: table.to_owned(),
        start: r.start.clone().into(),
        end: r.end.clone().unwrap_or_default().into(),
    }
}

/// Whether `e` lets an `allow_partial` read skip the partition.
fn skippable(e: &ApiError) -> bool {
    e.code == "PartitionUnavailable"
}

/// `[lo, hi)` of a scan's range, for choosing partitions: a prefix's rows, or the bounds'
/// keys (an exclusive start or an inclusive end only widens the choice, never narrows it).
fn span(range: Option<&proto::KeyRange>) -> (Vec<u8>, Option<Vec<u8>>) {
    use proto::key_range::Range;
    match range.and_then(|r| r.range.as_ref()) {
        Some(Range::Prefix(p)) => (p.to_vec(), successor(p)),
        Some(Range::Bounds(b)) => (
            b.start.as_ref().map(|s| s.key.to_vec()).unwrap_or_default(),
            b.end.as_ref().map(|e| {
                let mut k = e.key.to_vec();
                if e.inclusive {
                    k.push(0);
                }
                k
            }),
        ),
        None => (Vec::new(), None),
    }
}

/// The least key above every key with `prefix`; `None` when there's none (all `0xff`).
fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut k = prefix.to_vec();
    while let Some(last) = k.pop() {
        if last < 0xff {
            k.push(last + 1);
            return Some(k);
        }
    }
    None
}

type Read<'a> = Pin<Box<dyn Future<Output = Result<MultiGetResponse, ApiError>> + Send + 'a>>;

/// Awaits `cur`, polling `ahead` meanwhile so the read ahead makes progress; its result, if
/// it finishes first, is kept in `ahead_done`.
async fn race(
    cur: Read<'_>,
    ahead: &mut Option<Read<'_>>,
    ahead_done: &mut Option<Result<MultiGetResponse, ApiError>>,
) -> Result<MultiGetResponse, ApiError> {
    let mut cur = cur;
    loop {
        match ahead.as_mut() {
            Some(a) if ahead_done.is_none() => {
                tokio::select! {
                    r = &mut cur => return r,
                    r = a => *ahead_done = Some(r),
                }
            }
            _ => return cur.await,
        }
    }
}

impl Router {
    /// One partition's page of a multi-get: here, or forwarded to its node (once more after
    /// a `WrongPartition`, with the newer map).
    async fn multi_get_part(
        &self,
        reach: &Reach,
        partition: &str,
        r: MultiGetRequest,
        caller: &Caller,
    ) -> Result<MultiGetResponse, ApiError> {
        let mut pulled = false;
        loop {
            let routes = reach.routes()?;
            let reply = match reach.place(&routes, partition)? {
                Place::Here => {
                    let reply = self
                        .service
                        .call(ServiceOp::MultiGet(r), caller, None)
                        .await?;
                    return match reply.uncharge().0 {
                        ServiceReply::MultiGet(m) => Ok(m),
                        _ => Err(ApiError::internal("a multi-get answered another reply")),
                    };
                }
                Place::There(address, map_version) => {
                    let req = ForwardRequest {
                        map_version,
                        caller: Some(forward::forwarded_caller(caller)?),
                        op: Some(forward_request::Op::MultiGet(r.clone())),
                    };
                    let sent = reach
                        .forwarder
                        .send(&address, req, reach.within()?, false)
                        .await;
                    match sent {
                        Err(e) if e.code == "WrongPartition" && !pulled => {
                            pulled = true;
                            reach.catch_up(&routes, partition, &address).await?;
                            continue;
                        }
                        other => forward::reply_of(other?)?,
                    }
                }
            };
            return match reply {
                ServiceReply::MultiGet(m) => Ok(m),
                _ => Err(ApiError::internal(
                    "a forwarded multi-get answered another reply",
                )),
            };
        }
    }

    /// `Data.MultiGet` on a cluster node (D317 ticket 4a).
    pub(crate) async fn fan_multi_get(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<OwnedSemaphorePermit>,
        deadline: tokio::time::Instant,
    ) -> Result<ServiceReply, ApiError> {
        // The caller's scope and the request's size, as for any call (its bytes are held to
        // the end).
        let _request = self.service.admit(&op.admit(), caller, prepaid).await?;
        let ServiceOp::MultiGet(r) = op else {
            return Err(ApiError::internal("fan_multi_get takes a multi-get"));
        };
        if r.selection
            .as_ref()
            .is_some_and(|s| !s.snapshot_id.is_empty())
        {
            return Err(ApiError::internal(
                "a multi-get at a snapshot is routed to its partition, not fanned out",
            ));
        }
        let reach = Reach {
            deadline: Some(deadline),
            ..Reach::of(self, cluster)
        };
        let routes = reach.routes()?;
        let ranges = ranges_of(&routes, &r.table)?;
        let after = Position::from_bytes(&r.after, Kind::Keys)?;
        let start = start_of(&ranges, after.as_ref(), &r.table)?;
        // The keys in order, once each, as a single node reads them (simp724's N1).
        let mut keys = r.keys.clone();
        keys.sort_unstable();
        keys.dedup();
        // Each partition from the position on, with its keys; one with none is skipped,
        // except the position's own (its page may continue a row).
        let groups: Vec<(&PartRange, Vec<prost::bytes::Bytes>)> = ranges[start..]
            .iter()
            .enumerate()
            .map(|(i, range)| {
                let mine = keys.iter().filter(|k| range.holds(k)).cloned().collect();
                (i, range, mine)
            })
            .filter(
                |(i, _, mine): &(usize, &PartRange, Vec<prost::bytes::Bytes>)| {
                    !mine.is_empty()
                        || (*i == 0 && after.as_ref().is_some_and(|a| a.inner.is_some()))
                },
            )
            .map(|(_, range, mine)| (range, mine))
            .collect();
        let budget = page_bytes(r.page_bytes);
        let sub =
            |keys: Vec<prost::bytes::Bytes>, inner: Option<&Vec<u8>>, page: u64| MultiGetRequest {
                table: r.table.clone(),
                keys,
                selection: r.selection.clone(),
                page_bytes: page,
                after: inner.cloned().unwrap_or_default().into(),
                allow_partial: false,
            };
        let inner_of = |range: &PartRange| {
            after
                .as_ref()
                .filter(|a| a.partition == range.partition)
                .and_then(|a| a.inner.as_ref())
        };
        let mut rows = Vec::new();
        let mut missed = Vec::new();
        let mut left = budget;
        let mut next: Option<Position> = None;
        let mut ahead: Option<Read<'_>> = None;
        let mut ahead_done = None;
        for (i, (range, mine)) in groups.iter().enumerate() {
            if left == 0 {
                // The budget ended exactly at a partition's end (simp724's M1).
                next = Some(Position {
                    partition: range.partition.clone(),
                    inner: None,
                });
                break;
            }
            let inner = inner_of(range);
            // This partition's page: the read ahead (with the whole budget), or a read with
            // what's left of it.
            let cur: Read<'_> = match (ahead.take(), ahead_done.take()) {
                (_, Some(done)) => Box::pin(async move { done }),
                (Some(f), None) => f,
                (None, None) => Box::pin(self.multi_get_part(
                    &reach,
                    &range.partition,
                    sub(mine.clone(), inner, left),
                    caller,
                )),
            };
            if let Some((next_range, next_keys)) = groups.get(i + 1) {
                ahead = Some(Box::pin(self.multi_get_part(
                    &reach,
                    &next_range.partition,
                    sub(next_keys.clone(), inner_of(next_range), budget),
                    caller,
                )));
            }
            let mut page = match race(cur, &mut ahead, &mut ahead_done).await {
                Ok(page) => page,
                Err(e) if r.allow_partial && skippable(&e) => {
                    missed.push(missing(&r.table, range));
                    continue;
                }
                Err(e) => return Err(e),
            };
            if page.bytes > left {
                // Read ahead with the whole budget, and it doesn't fit: read again with
                // what's left, so the cut is the partition's own (a single node's).
                let again = Box::pin(self.multi_get_part(
                    &reach,
                    &range.partition,
                    sub(mine.clone(), inner, left),
                    caller,
                ));
                page = match race(again, &mut ahead, &mut ahead_done).await {
                    Ok(page) => page,
                    Err(e) if r.allow_partial && skippable(&e) => {
                        missed.push(missing(&r.table, range));
                        continue;
                    }
                    Err(e) => return Err(e),
                };
            }
            left = left.saturating_sub(page.bytes);
            rows.extend(page.rows);
            if !page.next.is_empty() {
                next = Some(Position {
                    partition: range.partition.clone(),
                    inner: Some(page.next.to_vec()),
                });
                break;
            }
        }
        let reply = MultiGetResponse {
            rows,
            next: next
                .map(|p| p.to_bytes(Kind::Keys))
                .unwrap_or_default()
                .into(),
            bytes: budget - left,
            missing: missed,
        };
        self.service
            .charge_built(caller, ServiceReply::MultiGet(reply))
            .await
    }

    /// `Data.Scan` on a cluster node (D317 ticket 4a): a stream of the partitions' pages in
    /// key order, holding one `--max-scans` slot for its life, and counted as one
    /// `Data.Scan` when it ends.
    pub(crate) async fn fan_scan(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<OwnedSemaphorePermit>,
    ) -> Result<ServiceReply, ApiError> {
        let request = self.service.admit(&op.admit(), caller, prepaid).await?;
        let ServiceOp::Scan(r) = op else {
            return Err(ApiError::internal("fan_scan takes a scan"));
        };
        crate::serve::convert::Select::new(r.selection.as_ref())?;
        let reach = Reach::of(self, cluster);
        let routes = reach.routes()?;
        // A scan at a snapshot reads the snapshot's partition only (D317 ticket 3):
        // `CrossPartition` if its range reaches another.
        let only = match crate::serve::router::snapshot_of(r.selection.as_ref()) {
            Some(_) => Some(crate::serve::router::route(
                &routes,
                &ServiceOp::Scan(r.clone()),
            )?),
            None => None,
        };
        let ranges = ranges_of(&routes, &r.table)?;
        let after = Position::from_bytes(&r.after, Kind::Scan)?;
        let start = start_of(&ranges, after.as_ref(), &r.table)?;
        let (lo, hi) = span(r.range.as_ref());
        let parts: Vec<PartRange> = ranges[start..]
            .iter()
            .filter(|p| p.meets(&lo, hi.as_deref()))
            .filter(|p| only.as_ref().is_none_or(|o| *o == p.partition))
            .cloned()
            .collect();
        // Here, the snapshot is pinned for the stream's life, as on a single node; on its
        // node, the forwarded scan pins it there.
        let pinned = match &only {
            Some(p) if reach.local.as_deref() == Some(p.as_str()) => self
                .service
                .pinned_selection(r.selection.as_ref(), caller)?,
            _ => None,
        };
        // The local partition's table, resolved now: its reading starts when the scan gets
        // there.
        let here = match parts
            .iter()
            .find(|p| reach.local.as_deref() == Some(p.partition.as_str()))
        {
            Some(_) => Some(self.service.scan_opener(&r.table).await?),
            None => None,
        };
        let slot = self.service.scan_slot()?;
        drop(request);
        let (tx, rx) = mpsc::channel(1);
        let (end_tx, end_rx) = oneshot::channel();
        let job = ScanJob {
            reach,
            here,
            pinned: std::sync::Mutex::new(pinned),
            caller: caller.clone(),
            r,
            after,
            parts,
            limits: *self.service.limits(),
            inflight: Arc::clone(self.service.inflight()),
        };
        let metrics = Arc::clone(&self.service.metrics);
        let started = std::time::Instant::now();
        tokio::spawn(async move {
            let outcome = match tokio::spawn(job.run(tx)).await {
                Ok(outcome) => outcome.map_err(ApiError::for_read),
                Err(e) => Err(ApiError::internal(format!("the scan failed: {e}"))),
            };
            metrics.count(
                crate::serve::service::SCAN,
                outcome.as_ref().err().map(|e| e.code.as_str()),
                started.elapsed(),
            );
            drop(slot);
            if let Err(e) = outcome {
                let _ = end_tx.send(e);
            }
        });
        Ok(ServiceReply::Scan(ScanPages::new(rx, end_rx)))
    }

    /// A partition's part of a scan another node fans out (D317 ticket 4a): served only if
    /// this node's map gives it `partition` and it has it open, as
    /// [`serve_forwarded`](Self::serve_forwarded) checks a key.
    pub(crate) async fn serve_forwarded_scan(
        &self,
        partition: &str,
        r: ScanRequest,
        caller: &Caller,
    ) -> Result<ScanPages, ApiError> {
        let Some(cluster) = &self.cluster else {
            return Err(ApiError::unsupported("this node isn't a cluster node"));
        };
        {
            let routes = cluster.node.routes();
            let Some(routes) = routes.as_ref() else {
                return Err(ApiError::wrong_partition(
                    0,
                    "this node has no cluster map yet",
                ));
            };
            let mine = routes
                .owner(partition)
                .is_some_and(|o| o.node == cluster.node.id());
            if !mine {
                return Err(ApiError::wrong_partition(
                    routes.map_version(),
                    format!("this node doesn't serve partition {partition:?}"),
                ));
            }
            if cluster.local.as_deref() != Some(partition) {
                return Err(not_open(partition));
            }
            // At a snapshot, checked again here: its range stays in its partition, and that's
            // this one (D317 ticket 3).
            if crate::serve::router::snapshot_of(r.selection.as_ref()).is_some() {
                let p = crate::serve::router::route(routes, &ServiceOp::Scan(r.clone()))?;
                if p != partition {
                    return Err(ApiError::cross_partition(format!(
                        "the scan's snapshot is partition {p}'s, not {partition}'s"
                    )));
                }
            }
        }
        match self.service.call(ServiceOp::Scan(r), caller, None).await? {
            ServiceReply::Scan(pages) => Ok(pages),
            _ => Err(ApiError::internal("a scan answered another reply")),
        }
    }
}

/// One partition's pages: its own scan's here, or its node's stream.
enum PartPages {
    Here(ScanPages),
    /// The stream, its node's address, and its connection's lease (dropped after the stream).
    There(
        Box<tonic::Streaming<ScanPage>>,
        String,
        forward::StreamLease,
    ),
}

impl PartPages {
    async fn next(&mut self) -> Option<Result<ScanPage, ApiError>> {
        match self {
            Self::Here(pages) => {
                std::future::poll_fn(|cx| {
                    futures_core::Stream::poll_next(Pin::new(&mut *pages), cx)
                })
                .await
            }
            Self::There(stream, address, lease) => match stream.message().await {
                Ok(Some(page)) => Some(Ok(page)),
                Ok(None) => {
                    lease.ended();
                    None
                }
                Err(status) => Some(Err(forward::stream_error(&status, address))),
            },
        }
    }
}

/// A fanned-out scan's reading side, run as its own task.
struct ScanJob {
    reach: Reach,
    /// A scan at a snapshot whose partition is here: the snapshot, held for its stream
    /// (D317 ticket 3).
    pinned: std::sync::Mutex<Option<crate::serve::sessions::SnapshotGuard>>,
    /// The local partition's scan, if the range meets it.
    here: Option<ScanOpener>,
    caller: Caller,
    r: ScanRequest,
    after: Option<Position>,
    /// The partitions the range meets, from the position's on, in key order.
    parts: Vec<PartRange>,
    limits: Limits,
    inflight: Arc<Semaphore>,
}

impl ScanJob {
    /// One partition's pages, here or on its node (once more after a `WrongPartition`,
    /// with the newer map).
    async fn open(&self, partition: &str, r: ScanRequest) -> Result<PartPages, ApiError> {
        let mut pulled = false;
        loop {
            let routes = self.reach.routes()?;
            match self.reach.place(&routes, partition)? {
                Place::Here => {
                    let here = self.here.as_ref().ok_or_else(|| {
                        ApiError::internal("no local scan for the local partition")
                    })?;
                    let pinned = self
                        .pinned
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .take();
                    return here.open(r, None, pinned, None).map(PartPages::Here);
                }
                Place::There(address, map_version) => {
                    let req = ForwardScanRequest {
                        map_version,
                        caller: Some(forward::forwarded_caller(&self.caller)?),
                        partition: partition.to_owned(),
                        scan: Some(r.clone()),
                    };
                    match self
                        .reach
                        .forwarder
                        .scan(&address, req, self.reach.timeout)
                        .await
                    {
                        Ok((s, lease)) => return Ok(PartPages::There(Box::new(s), address, lease)),
                        Err(e) if e.code == "WrongPartition" && !pulled => {
                            pulled = true;
                            self.reach.catch_up(&routes, partition, &address).await?;
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }
    }

    /// Sends the partitions' pages, in order, until the scan ends or the client goes away.
    ///
    /// Each partition gets the request with the rest of the row `limit` (rows, as on a single
    /// node: a row a page cut counts once, when its first cells are sent), and only the
    /// position's own partition gets its position. A page keeps its rows and `bytes`; its
    /// `next` becomes a cluster position. A partition whose stream ends with a position (a
    /// budget too small for anything) ends the scan there, as on a single node.
    // `send!` sets `sent_any` each time; its last expansion's is read by no one.
    #[allow(unused_assignments)]
    async fn run(self, pages: mpsc::Sender<PageSlot>) -> Result<(), ApiError> {
        let limits = self.limits;
        let mut remaining = (self.r.limit > 0).then_some(self.r.limit);
        let mut queued: Option<PageSlot> = None;
        let mut missed: Vec<MissingRange> = Vec::new();
        let mut sent_any = false;
        let table = self.r.table.clone();
        let allow_partial = self.r.allow_partial;
        // Sends one page, waiting for the client to take the one before it, with its share
        // of the in-flight budget held until the client takes it.
        macro_rules! send {
            ($page:expr) => {{
                let page: ScanPage = $page;
                let Some(space) = room(&pages, &mut queued, limits.stream_idle).await? else {
                    return Ok(());
                };
                let share = usize::try_from(page.bytes)
                    .unwrap_or(usize::MAX)
                    .min(limits.max_request_bytes);
                let paid = budget_within(&self.inflight, share, limits.request_timeout).await?;
                let slot: PageSlot = Arc::new(std::sync::Mutex::new(Some((page, paid))));
                space.send(Arc::clone(&slot));
                queued = Some(slot);
                sent_any = true;
            }};
        }
        for (i, part) in self.parts.iter().enumerate() {
            if remaining == Some(0) {
                break;
            }
            let inner = self
                .after
                .as_ref()
                .filter(|a| a.partition == part.partition)
                .and_then(|a| a.inner.clone());
            // The row the position is in: a page starting with it continues it, and doesn't
            // count against the limit again.
            let mut last_row: Option<Vec<u8>> = inner
                .as_deref()
                .and_then(|b| pigeonhole::ScanPosition::from_bytes(b).ok())
                .map(|p| p.row().to_vec());
            let sub = ScanRequest {
                after: inner.unwrap_or_default().into(),
                limit: remaining.unwrap_or(0),
                allow_partial: false,
                ..self.r.clone()
            };
            let mut stream = match self.open(&part.partition, sub).await {
                Ok(s) => s,
                Err(e) if allow_partial && skippable(&e) => {
                    missed.push(missing(&table, part));
                    continue;
                }
                Err(e) => return Err(e),
            };
            let more = i + 1 < self.parts.len();
            let mut from_here = false;
            let mut ended_at: Option<bool> = None;
            while let Some(page) = stream.next().await {
                let page = match page {
                    Ok(page) => page,
                    // Unreachable before any of its pages: skipped. Partway, the stream ends
                    // with the error, and the last page's `next` resumes it.
                    Err(e) if allow_partial && skippable(&e) && !from_here => {
                        missed.push(missing(&table, part));
                        break;
                    }
                    Err(e) => return Err(e),
                };
                from_here = true;
                let continued = matches!(
                    (&last_row, page.rows.first()),
                    (Some(p), Some(first)) if first.key[..] == p[..]
                );
                if let Some(n) = remaining.as_mut() {
                    let new_rows = page.rows.len() as u64 - u64::from(continued);
                    *n = n.saturating_sub(new_rows);
                }
                if let Some(last) = page.rows.last() {
                    last_row = Some(last.key.to_vec());
                }
                let done_here = page.next.is_empty();
                let next = if !done_here {
                    Some(Position {
                        partition: part.partition.clone(),
                        inner: Some(page.next.to_vec()),
                    })
                } else if more && remaining != Some(0) {
                    self.parts.get(i + 1).map(|p| Position {
                        partition: p.partition.clone(),
                        inner: None,
                    })
                } else {
                    None
                };
                // A partition's last page with nothing in it, when another partition
                // follows, says nothing the page before didn't: it isn't sent.
                let silent = page.rows.is_empty() && done_here && next.is_some();
                if !silent || !missed.is_empty() {
                    send!(ScanPage {
                        rows: page.rows,
                        next: next
                            .map(|p| p.to_bytes(Kind::Scan))
                            .unwrap_or_default()
                            .into(),
                        bytes: page.bytes,
                        missing: std::mem::take(&mut missed),
                    });
                }
                ended_at = Some(done_here);
            }
            if ended_at == Some(false) {
                // The partition's stream ended with a position (a budget too small for
                // anything): the scan ends there, and resumes from it.
                room(&pages, &mut queued, limits.stream_idle).await?;
                return Ok(());
            }
        }
        // Skipped ranges not yet reported, or a scan that sent nothing: a last, empty page.
        if !missed.is_empty() || !sent_any {
            send!(ScanPage {
                rows: Vec::new(),
                next: Default::default(),
                bytes: 0,
                missing: missed,
            });
        }
        room(&pages, &mut queued, limits.stream_idle).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{span, successor};
    use crate::serve::proto::{Bound, Bounds, KeyRange, key_range::Range};

    #[test]
    fn a_ranges_span_covers_every_row_it_can_hold() {
        assert_eq!(successor(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(successor(b"a\xff"), Some(b"b".to_vec()));
        assert_eq!(successor(b"\xff\xff"), None);
        let prefix = KeyRange {
            range: Some(Range::Prefix(b"m".to_vec().into())),
        };
        assert_eq!(span(Some(&prefix)), (b"m".to_vec(), Some(b"n".to_vec())));
        let bounds = |start: Option<(&[u8], bool)>, end: Option<(&[u8], bool)>| KeyRange {
            range: Some(Range::Bounds(Bounds {
                start: start.map(|(k, inclusive)| Bound {
                    key: k.to_vec().into(),
                    inclusive,
                }),
                end: end.map(|(k, inclusive)| Bound {
                    key: k.to_vec().into(),
                    inclusive,
                }),
            })),
        };
        // An inclusive end reaches its own key.
        assert_eq!(
            span(Some(&bounds(Some((b"c", false)), Some((b"m", true))))),
            (b"c".to_vec(), Some(b"m\x00".to_vec()))
        );
        assert_eq!(
            span(Some(&bounds(None, Some((b"m", false))))),
            (Vec::new(), Some(b"m".to_vec()))
        );
        assert_eq!(span(None), (Vec::new(), None));
    }
}
