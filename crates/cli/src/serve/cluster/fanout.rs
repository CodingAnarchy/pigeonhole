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
    NearestRequest, NearestResponse, ScanPage, ScanRequest, forward_request,
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
    /// The first of `groups` (each from its start) whose keys have a cell, after a page
    /// that used its whole budget: probed with a one-byte budget, which returns an empty
    /// page with a position when a cell is there. The first group's read ahead answers for
    /// it when there is one. A partition that can't be read is taken to have one, so the
    /// resume reads it and reports what it answers.
    ///
    /// The probes run one after another: a page ends this way rarely, and the first probe is
    /// usually the read ahead. If this path ever shows up in a profile, they could run
    /// together under `--fanout-forwards`, as a nearest search's partitions do (simp724's N1
    /// on #1710).
    async fn first_with_cells<'a>(
        &'a self,
        reach: &'a Reach,
        groups: &[(&PartRange, Vec<prost::bytes::Bytes>)],
        caller: &Caller,
        ahead: &mut Option<Read<'a>>,
        ahead_done: &mut Option<Result<MultiGetResponse, ApiError>>,
        sub: &impl Fn(Vec<prost::bytes::Bytes>, Option<&Vec<u8>>, u64) -> MultiGetRequest,
    ) -> Option<String> {
        for (i, (range, mine)) in groups.iter().enumerate() {
            let page = match (i, ahead_done.take(), ahead.take()) {
                (0, Some(done), _) => done,
                (0, None, Some(f)) => f.await,
                _ => {
                    self.multi_get_part(reach, &range.partition, sub(mine.clone(), None, 1), caller)
                        .await
                }
            };
            match page {
                Ok(p) if p.rows.is_empty() && p.next.is_empty() => {}
                _ => return Some(range.partition.clone()),
            }
        }
        None
    }

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
                // The request's holding covers this page (#1715): no charge of its own.
                Place::Here => {
                    return match self
                        .service
                        .fanout_part(ServiceOp::MultiGet(r), caller)
                        .await?
                    {
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
        // The caller's scope and the request's size, as for any call. Its bytes go before
        // the read, as a single node's multi-get gives them back (cli692 on #1715's plan).
        let request = self.service.admit(&op.admit(), caller, prepaid).await?;
        drop(request);
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
        // The request's holding (#1715): its whole budget, taken before any read, so the
        // rows merged so far and the current read's share (what's left of the budget)
        // always fit in it, and no read waits for budget while the request holds some. It
        // becomes the reply's charge.
        let holding = self
            .service
            .fanout_hold(cap_of(r.page_bytes), reach.within()?)
            .await?;
        let reply = self.merge_keys(&reach, r, caller).await?;
        self.service
            .fanout_reply(
                caller,
                ServiceReply::MultiGet(reply),
                holding,
                reach.within()?,
            )
            .await
    }

    /// A multi-get's page across the partitions, as [`fan_multi_get`](Self::fan_multi_get)
    /// answers it, without its admission or its reply's charge: a traversal's hops read
    /// their edges through it (D317 ticket 4b).
    async fn merge_keys(
        &self,
        reach: &Reach,
        r: MultiGetRequest,
        caller: &Caller,
    ) -> Result<MultiGetResponse, ApiError> {
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
        // The read ahead's share of the budget, held while its page is out or waiting.
        let mut ahead_share: Option<super::super::charge::Charge> = None;
        for (i, (range, mine)) in groups.iter().enumerate() {
            if left == 0 {
                // The budget ended exactly at a partition's end (simp724's M1). A single
                // node gives a position only when a cell is left that didn't fit, so this
                // page resumes at the first later partition with a cell for the keys, and
                // has no `next` when none has one (#1698's seed 1794362533986056000).
                next = self
                    .first_with_cells(
                        reach,
                        &groups[i..],
                        caller,
                        &mut ahead,
                        &mut ahead_done,
                        &sub,
                    )
                    .await
                    .map(|partition| Position {
                        partition,
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
                    reach,
                    &range.partition,
                    sub(mine.clone(), inner, left),
                    caller,
                )),
            };
            // A page read ahead becomes the current one. It was read with the whole budget,
            // more than what's left of the holding may cover, so its share stays until it's
            // merged or read again (this iteration's end).
            let _cur_share = ahead_share.take();
            // The next partition is read ahead only on spare budget (#1715): its whole page,
            // taken now or not at all, so a read ahead never waits for budget.
            if let Some((next_range, next_keys)) = groups.get(i + 1)
                && let Some(share) = self.service.fanout_try_hold(cap_of(budget))
            {
                ahead_share = Some(share);
                ahead = Some(Box::pin(self.multi_get_part(
                    reach,
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
                    reach,
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
        Ok(MultiGetResponse {
            rows,
            next: next
                .map(|p| p.to_bytes(Kind::Keys))
                .unwrap_or_default()
                .into(),
            bytes: budget - left,
            missing: missed,
        })
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

/// A boxed future of a fan-out's.
type Boxed<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Runs `futs`, at most `n` at once (`--fanout-forwards`), handing each result to `each`
/// with its index as it arrives, so a merge holds only what it keeps (simp724's S1 on
/// #1713). An error from `each` ends the run, dropping the reads still going.
///
/// One read always runs: its share is the request's own (#1715). Each further concurrent
/// read needs a share `spare` gives on spare budget, without waiting; when there's none,
/// the queued reads wait for a running one to finish. Shares go back once the queue is
/// empty and fewer reads are running. So the reads never wait for budget while the request
/// holds some (cli692 on #1715's plan).
async fn bounded<'a, T, S>(
    futs: Vec<Boxed<'a, T>>,
    n: usize,
    mut spare: impl FnMut() -> Option<S>,
    mut each: impl FnMut(usize, T) -> Result<(), ApiError>,
) -> Result<(), ApiError> {
    let mut queue = futs.into_iter().enumerate().peekable();
    let mut running: Vec<(usize, Boxed<'a, T>)> = Vec::new();
    let mut shares: Vec<S> = Vec::new();
    std::future::poll_fn(|cx| {
        loop {
            while running.len() < n.max(1) && queue.peek().is_some() {
                // The first read runs on the request's share; each other needs one more.
                if running.len() > shares.len() {
                    match spare() {
                        Some(share) => shares.push(share),
                        None => break,
                    }
                }
                running.extend(queue.next());
            }
            if queue.peek().is_none() {
                shares.truncate(running.len().saturating_sub(1));
            }
            // Done only once every read has run: the queue too, not just this batch.
            if running.is_empty() {
                return std::task::Poll::Ready(Ok(()));
            }
            let mut done = false;
            let mut i = 0;
            while i < running.len() {
                if let std::task::Poll::Ready(v) = running[i].1.as_mut().poll(cx) {
                    let (at, _) = running.swap_remove(i);
                    each(at, v)?;
                    done = true;
                } else {
                    i += 1;
                }
            }
            if !done {
                return std::task::Poll::Pending;
            }
        }
    })
    .await
}

/// A scan range's bounds as `Bound`s (a prefix as `[prefix, successor)`), `None` for no
/// bound.
fn bounds_of(range: Option<&proto::KeyRange>) -> (Option<proto::Bound>, Option<proto::Bound>) {
    use proto::key_range::Range;
    match range.and_then(|r| r.range.as_ref()) {
        Some(Range::Prefix(p)) => (
            Some(proto::Bound {
                key: p.clone(),
                inclusive: true,
            }),
            successor(p).map(|k| proto::Bound {
                key: k.into(),
                inclusive: false,
            }),
        ),
        Some(Range::Bounds(b)) => (b.start.clone(), b.end.clone()),
        None => (None, None),
    }
}

/// `range` clipped to partition `p`'s rows, as bounds (D317 ticket 4b): a `Nearest`'s
/// range sent to one partition, which routes and checks it by its bounds.
fn clipped(range: Option<&proto::KeyRange>, p: &PartRange) -> proto::KeyRange {
    let (start, end) = bounds_of(range);
    let start = match start {
        Some(b) if b.key[..] >= p.start[..] => Some(b),
        _ => Some(proto::Bound {
            key: p.start.clone().into(),
            inclusive: true,
        }),
    };
    let end = match (&p.end, end) {
        (None, end) => end,
        // The partition's end comes first: an exclusive bound there. An inclusive end at
        // `k` reaches `k`'s row, so it stays only if it's below the partition's end.
        (Some(e), Some(b)) if b.key[..] < e[..] => Some(b),
        (Some(e), _) => Some(proto::Bound {
            key: e.clone().into(),
            inclusive: false,
        }),
    };
    proto::KeyRange {
        range: Some(proto::key_range::Range::Bounds(proto::Bounds {
            start,
            end,
        })),
    }
}

/// A read's budget as bytes of the in-flight budget: `page_bytes` as a read applies it.
fn cap_of(page: u64) -> usize {
    usize::try_from(page_bytes(page)).unwrap_or(usize::MAX)
}

/// A neighbour's size by D308's measure: its row, its qualifier and 16 bytes.
fn neighbour_bytes(n: &proto::Neighbour) -> u64 {
    (n.row.len() + n.qualifier.len()) as u64 + 16
}

/// `kept` and `more` merged by the library's total order (distance, then row, then
/// qualifier), cut to the first `k` that fit `budget` plus the one after them that didn't,
/// if any. Distances are never NaN (the query is checked, stored vectors are finite), and
/// every node computes them with the same code.
fn merge_nearest(
    mut kept: Vec<proto::Neighbour>,
    more: Vec<proto::Neighbour>,
    k: usize,
    budget: u64,
) -> Vec<proto::Neighbour> {
    kept.extend(more);
    kept.sort_by(|a, b| {
        a.distance
            .total_cmp(&b.distance)
            .then_with(|| a.row.cmp(&b.row))
            .then_with(|| a.qualifier.cmp(&b.qualifier))
    });
    let mut used = 0;
    let mut keep = 0;
    for n in &kept {
        if keep == k {
            break;
        }
        keep += 1;
        used += neighbour_bytes(n);
        if used > budget {
            // This one didn't fit: kept as the one after the prefix.
            break;
        }
    }
    kept.truncate(keep);
    kept
}

/// A traversal's `max_nodes` when the request sets none: the library's default
/// (`pigeonhole::Traverse::max_nodes`, 10,000,000), which a single node's traversal takes.
const DEFAULT_MAX_NODES: u64 = 10_000_000;

/// What a traversal's visited set holds per node, as the library sizes it: the key and
/// about 48 bytes.
const VISITED_NODE_BYTES: usize = 48;

impl Router {
    /// One partition's exact top `k`: here, or forwarded to its node (once more after a
    /// `WrongPartition`).
    async fn nearest_part(
        &self,
        reach: &Reach,
        partition: &str,
        r: NearestRequest,
        caller: &Caller,
    ) -> Result<NearestResponse, ApiError> {
        let mut pulled = false;
        loop {
            let routes = reach.routes()?;
            let reply = match reach.place(&routes, partition)? {
                // The request's holding covers this answer (#1715): no charge of its own.
                Place::Here => {
                    self.service
                        .fanout_part(ServiceOp::Nearest(r), caller)
                        .await?
                }
                Place::There(address, map_version) => {
                    let req = ForwardRequest {
                        map_version,
                        caller: Some(forward::forwarded_caller(caller)?),
                        op: Some(forward_request::Op::Nearest(r.clone())),
                    };
                    match reach
                        .forwarder
                        .send(&address, req, reach.within()?, false)
                        .await
                    {
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
                ServiceReply::Nearest(n) => Ok(n),
                _ => Err(ApiError::internal(
                    "a nearest search answered another reply",
                )),
            };
        }
    }

    /// `Vectors.Nearest` on a cluster node (D317 ticket 4b): every partition the source
    /// meets answers its exact top `k` under the same `page_bytes`, and the merge keeps the
    /// first `k` by the library's total order (distance, row, qualifier), then cuts to the
    /// budget. A partition's answer is a prefix of its own ranking, and a neighbour the
    /// global prefix keeps never needs more than the budget's bytes of its partition's, so
    /// the merge is a single node's answer.
    pub(crate) async fn fan_nearest(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<OwnedSemaphorePermit>,
        deadline: tokio::time::Instant,
    ) -> Result<ServiceReply, ApiError> {
        use proto::nearest_request::Source;
        let request = self.service.admit(&op.admit(), caller, prepaid).await?;
        drop(request);
        let ServiceOp::Nearest(r) = op else {
            return Err(ApiError::internal("fan_nearest takes a nearest search"));
        };
        if !r.snapshot_id.is_empty() {
            return Err(ApiError::unsupported(
                "a nearest search at a pinned snapshot on a cluster: a snapshot is one \
                 partition's",
            ));
        }
        let max_k = crate::serve::service::MAX_NEAREST_K;
        if r.k == 0 || r.k > max_k {
            return Err(ApiError::invalid(format!("k is 1 to {max_k}")));
        }
        if r.query.is_none() {
            return Err(ApiError::invalid("a nearest search needs a query vector"));
        }
        let reach = Reach {
            deadline: Some(deadline),
            ..Reach::of(self, cluster)
        };
        let routes = reach.routes()?;
        let ranges = ranges_of(&routes, &r.table)?;
        // Each partition's part of the source: its keys, or the range clipped to it.
        let mut parts: Vec<(PartRange, NearestRequest)> = Vec::new();
        for p in &ranges {
            let source = match &r.source {
                Some(Source::Keys(list)) => {
                    let mut mine: Vec<_> =
                        list.keys.iter().filter(|k| p.holds(k)).cloned().collect();
                    if mine.is_empty() {
                        continue;
                    }
                    mine.sort_unstable();
                    mine.dedup();
                    Source::Keys(proto::KeyList { keys: mine })
                }
                Some(Source::Range(range)) => {
                    let (lo, hi) = span(Some(range));
                    if !p.meets(&lo, hi.as_deref()) {
                        continue;
                    }
                    Source::Range(clipped(Some(range), p))
                }
                None => Source::Range(clipped(None, p)),
            };
            parts.push((
                p.clone(),
                NearestRequest {
                    source: Some(source),
                    allow_partial: false,
                    ..r.clone()
                },
            ));
        }
        type Answer = Result<NearestResponse, ApiError>;
        let reads: Vec<Pin<Box<dyn Future<Output = Answer> + Send + '_>>> = parts
            .iter()
            .map(|(p, sub)| {
                Box::pin(self.nearest_part(&reach, &p.partition, sub.clone(), caller))
                    as Pin<Box<dyn Future<Output = Answer> + Send + '_>>
            })
            .collect();
        // The running merge: the first `k` so far that fit the budget, and the next one,
        // which didn't (it decides `cut_by_budget`). A neighbour past that next one can
        // never come back into the prefix, since more answers only push it further down.
        let k = r.k as usize;
        let budget = page_bytes(r.page_bytes);
        let mut kept: Vec<proto::Neighbour> = Vec::new();
        let mut some_cut = false;
        let mut missed = Vec::new();
        // The request's holding (#1715), taken before any read: the result's bound (one
        // budget, the merge keeping at most it and one neighbour) and one read's answer
        // (another). Further concurrent reads run only on spare budget (`bounded`), so no
        // read waits for budget while the request holds some. It becomes the reply's charge.
        let page = cap_of(r.page_bytes);
        let holding = self
            .service
            .fanout_hold(page.saturating_mul(2), reach.within()?)
            .await?;
        bounded(
            reads,
            self.service.limits().fanout_forwards,
            || self.service.fanout_try_hold(page),
            |i, answer| match answer {
                Ok(a) => {
                    some_cut |= a.cut_by_budget;
                    kept = merge_nearest(std::mem::take(&mut kept), a.neighbours, k, budget);
                    Ok(())
                }
                Err(e) if r.allow_partial && skippable(&e) => {
                    missed.push(missing(&r.table, &parts[i].0));
                    Ok(())
                }
                Err(e) => Err(e),
            },
        )
        .await?;
        // `kept` is the prefix plus, when the budget stopped it short of `k`, the neighbour
        // that didn't fit.
        let mut used = 0;
        let mut neighbours = Vec::new();
        let mut cut = false;
        for n in kept {
            if neighbours.len() == k {
                break;
            }
            let size = neighbour_bytes(&n);
            if used + size > budget {
                cut = true;
                break;
            }
            used += size;
            neighbours.push(n);
        }
        // `missing` in partition order, whatever order the answers came in.
        missed.sort_by(|a: &MissingRange, b: &MissingRange| a.start.cmp(&b.start));
        let reply = NearestResponse {
            // A partition cut short held more within its top `k`: the merge would have
            // stopped for the budget at or before them.
            cut_by_budget: cut || (neighbours.len() < k && some_cut),
            neighbours,
            missing: missed,
        };
        self.service
            .fanout_reply(
                caller,
                ServiceReply::Nearest(reply),
                holding,
                reach.within()?,
            )
            .await
    }

    /// `Graph.Traverse` on a cluster node (D317 ticket 4b): each hop runs here, by the
    /// library's rules exactly ([`pigeonhole::Traverse`]). The frontier's edges are one
    /// fanned-out multi-get (the edge selection, `column_limit` = `per_node_limit`, which
    /// counts edges the selection keeps, as the traversal's limit does), read in (source,
    /// qualifier) order; a destination not visited before is new once, ranked by its first
    /// edge; `hop_limit`, `max_nodes` and the byte budget cut that ranking, and the reading
    /// stops where a single node's would, so `stats` match too. Each hop reads at its own
    /// time, each partition at its own snapshot. One deadline covers the whole traversal,
    /// and the visited set counts against `--inflight-bytes`.
    pub(crate) async fn fan_traverse(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<OwnedSemaphorePermit>,
        deadline: tokio::time::Instant,
    ) -> Result<ServiceReply, ApiError> {
        let request = self.service.admit(&op.admit(), caller, prepaid).await?;
        drop(request);
        let ServiceOp::Traverse(r) = op else {
            return Err(ApiError::internal("fan_traverse takes a traversal"));
        };
        if !r.snapshot_id.is_empty() {
            return Err(ApiError::unsupported(
                "a traversal at a pinned snapshot on a cluster: a snapshot is one partition's",
            ));
        }
        if r.seeds.is_empty() {
            return Err(ApiError::invalid(
                "a traversal starts from at least one seed",
            ));
        }
        if r.hops > 64 {
            return Err(ApiError::invalid("hops is at most 64 (0 is 1)"));
        }
        let reach = Reach {
            deadline: Some(deadline),
            ..Reach::of(self, cluster)
        };
        let by_hop = if r.hop_limit == 0 {
            u64::MAX
        } else {
            r.hop_limit
        };
        let max_nodes = if r.max_nodes == 0 {
            DEFAULT_MAX_NODES
        } else {
            r.max_nodes
        };
        let mut bytes_left = page_bytes(r.page_bytes);
        let node_extra: u64 = if r.parents { 4 } else { 0 };
        let selection = proto::Selection {
            families: vec![r.family.clone()],
            qualifier_prefix: r.qualifier_prefix.clone(),
            qualifier_start: r.qualifier_start.clone(),
            qualifier_end: r.qualifier_end.clone(),
            time_range: r.time_range,
            value_filter: r.value_filter.clone(),
            column_limit: r.per_node_limit,
            ..Default::default()
        };
        let mut frontier: Vec<prost::bytes::Bytes> = r.seeds.clone();
        frontier.sort_unstable();
        frontier.dedup();
        let mut visited: std::collections::HashSet<Vec<u8>> =
            frontier.iter().map(|k| k.to_vec()).collect();
        let mut held: Vec<OwnedSemaphorePermit> = Vec::new();
        let hold = |nodes: &[prost::bytes::Bytes]| {
            nodes
                .iter()
                .map(|k| k.len() + VISITED_NODE_BYTES)
                .sum::<usize>()
        };
        // The traversal's holding (#1715): one hop page's share, held for the whole
        // traversal and reading every hop's pages, which are read at the traversal's own
        // `page_bytes` (cli692's Q1). It's taken with the seeds' visited share in one wait,
        // holding nothing. It becomes the reply's charge.
        let page = page_bytes(r.page_bytes);
        let mut holding = Some(
            self.service
                .fanout_hold(
                    cap_of(r.page_bytes).saturating_add(hold(&frontier)),
                    reach.within()?,
                )
                .await?,
        );
        // The visited set's bytes held apart from the holding, and the seeds', which are in
        // the holding until it's first given back.
        let mut held_bytes = 0usize;
        let mut seeds_in_holding = hold(&frontier);
        let mut stats = proto::TraverseStats::default();
        let mut hops = Vec::new();
        let mut missed: Vec<MissingRange> = Vec::new();
        let hops_asked = r.hops.max(1);
        for at_hop in 0..hops_asked {
            let room = max_nodes.saturating_sub(visited.len() as u64);
            if room == 0 {
                stats.cut_by_max_nodes = true;
                break;
            }
            let limit = usize::try_from(by_hop.min(room)).unwrap_or(usize::MAX);
            // The hop: destinations kept, each with the frontier index of its first edge.
            let mut kept: Vec<(prost::bytes::Bytes, u32)> = Vec::new();
            let mut seen: std::collections::HashSet<prost::bytes::Bytes> = Default::default();
            let (mut full, mut cut_by_bytes, mut used) = (false, false, 0u64);
            let mut after = prost::bytes::Bytes::new();
            let mut last_row: Option<prost::bytes::Bytes> = None;
            // The current row's edges read so far, across pages.
            let mut row_edges = 0u64;
            'pages: loop {
                let page = self
                    .merge_keys(
                        &reach,
                        MultiGetRequest {
                            table: r.table.clone(),
                            keys: frontier.clone(),
                            selection: Some(selection.clone()),
                            page_bytes: page,
                            after: after.clone(),
                            allow_partial: r.allow_partial,
                        },
                        caller,
                    )
                    .await?;
                for m in page.missing {
                    if !missed.iter().any(|x| x.partition == m.partition) {
                        missed.push(m);
                    }
                }
                for row in page.rows {
                    let src = frontier
                        .binary_search(&row.key)
                        .map_err(|_| ApiError::internal("an edge's row isn't in the frontier"))?;
                    if last_row.as_ref() != Some(&row.key) {
                        stats.rows += 1;
                        last_row = Some(row.key.clone());
                        row_edges = 0;
                    }
                    for cell in row.cells {
                        // A row a page cut and the next resumed starts its `column_limit`
                        // over, so the limit is kept here across pages, as one read keeps it.
                        if r.per_node_limit > 0 && row_edges >= u64::from(r.per_node_limit) {
                            break;
                        }
                        row_edges += 1;
                        stats.edges += 1;
                        let dest = cell.qualifier;
                        if visited.contains(&dest[..]) || seen.contains(&dest) {
                            continue;
                        }
                        if kept.len() >= limit {
                            full = true;
                            break 'pages;
                        }
                        let size = dest.len() as u64 + 16 + node_extra;
                        if used + size > bytes_left {
                            cut_by_bytes = true;
                            break 'pages;
                        }
                        used += size;
                        seen.insert(dest.clone());
                        kept.push((dest, u32::try_from(src).unwrap_or(u32::MAX)));
                    }
                }
                if page.next.is_empty() {
                    break;
                }
                after = page.next;
            }
            if full {
                stats.hops_cut += 1;
                if room < by_hop {
                    stats.cut_by_max_nodes = true;
                }
            }
            bytes_left = bytes_left.saturating_sub(used);
            kept.sort_unstable();
            let nodes: Vec<prost::bytes::Bytes> = kept.iter().map(|(k, _)| k.clone()).collect();
            let hop = proto::Hop {
                parents: if r.parents {
                    kept.iter().map(|(_, s)| *s).collect()
                } else {
                    Vec::new()
                },
                nodes: nodes.clone(),
            };
            if cut_by_bytes {
                // The budget's hop is the last: its in-budget nodes, then nothing more.
                stats.cut_by_budget = true;
                if !nodes.is_empty() {
                    hops.push(hop);
                }
                break;
            }
            if nodes.is_empty() {
                break;
            }
            // The new nodes' visited share: on spare budget if it's there. Otherwise the
            // hop holding goes back while it waits, and is taken again after, so the
            // traversal never waits for budget while it holds a page's (cli692's P1 on
            // #1715). Retaking it still waits while the visited set is held: that set is
            // the traversal's state, with no bound worth reserving up front.
            let want = hold(&nodes);
            let share = match u32::try_from(want.max(1)).ok().and_then(|n| {
                Arc::clone(self.service.inflight())
                    .try_acquire_many_owned(n)
                    .ok()
            }) {
                Some(share) => share,
                None => {
                    // The seeds' share goes with the holding, so it's waited for here too.
                    drop(holding.take());
                    let want_now = want + std::mem::take(&mut seeds_in_holding);
                    let share =
                        budget_within(self.service.inflight(), want_now, reach.within()?).await?;
                    held_bytes += want_now - want;
                    let room = self
                        .service
                        .limits()
                        .inflight_bytes
                        .saturating_sub(held_bytes + want);
                    // A hop page's share again if the visited set leaves room for one
                    // (cli692's N2 on #1730): otherwise none, and the traversal ends below,
                    // since a page read without its share is what #1715 prevents (and a
                    // smaller page might not hold a single edge).
                    if cap_of(r.page_bytes) <= room {
                        holding = Some(
                            self.service
                                .fanout_hold(cap_of(r.page_bytes), reach.within()?)
                                .await?,
                        );
                    }
                    share
                }
            };
            held_bytes += want;
            held.push(share);
            if holding.is_none() {
                // The visited set leaves less of the budget than a hop page: the next hop
                // can't be read charged, so the traversal ends here, cut as a byte budget
                // cuts it if there was a next hop to read.
                hops.push(hop);
                stats.cut_by_budget |= at_hop + 1 < hops_asked;
                break;
            }
            visited.extend(nodes.iter().map(|k| k.to_vec()));
            frontier = nodes;
            hops.push(hop);
        }
        drop(held);
        let reply = proto::TraverseResponse {
            hops,
            stats: Some(stats),
            missing: missed,
        };
        // With the visited set given back, a traversal cut for room takes its reply's
        // share now.
        let holding = match holding {
            Some(h) => h,
            None => self.service.fanout_hold(1, reach.within()?).await?,
        };
        self.service
            .fanout_reply(
                caller,
                ServiceReply::Traverse(reply),
                holding,
                reach.within()?,
            )
            .await
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
