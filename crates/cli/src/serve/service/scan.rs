use super::*;

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
    /// Starts a scan's reading side and gives the stream of its pages: a single node's
    /// scan, or a cluster's fanned-out one (D317 ticket 4). `run` gets the sender its pages
    /// go to. `slot` is the scan's `--max-scans` place, held until the reading ends. With
    /// `metrics`, the outcome is counted as a `Data.Scan` when it ends.
    pub(crate) fn spawn<F>(
        run: impl FnOnce(mpsc::Sender<PageSlot>) -> F,
        slot: Option<OwnedSemaphorePermit>,
        metrics: Option<Arc<Metrics>>,
    ) -> Self
    where
        F: Future<Output = Result<(), ApiError>> + Send + 'static,
    {
        let (tx, rx) = mpsc::channel(1);
        let (end_tx, end_rx) = oneshot::channel();
        let run = run(tx);
        let started = Instant::now();
        tokio::spawn(async move {
            // Its own task, so a panic is a `JoinError` here, and the stream ends with
            // `Internal` rather than looking like a clean end (simp724's B1 on #1510).
            let outcome = match tokio::spawn(run).await {
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
        Self {
            pages: rx,
            end: Some(end_rx),
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

/// The largest reply cap a unary read builds before it reserves (#1567, cli692's H1): the
/// default page. A larger one reserves its cap first.
pub(super) const FAST_CAP: usize = DEFAULT_PAGE_BYTES as usize;

/// A page-budgeted read's reply cap, as the read applies it (#1567).
pub(super) fn cap(asked: u64) -> usize {
    usize::try_from(page_bytes(asked)).unwrap_or(usize::MAX)
}

pub(crate) fn page_bytes(asked: u64) -> u64 {
    if asked == 0 {
        DEFAULT_PAGE_BYTES
    } else {
        asked.min(MAX_PAGE_BYTES)
    }
}

/// A hold seam (tests): runs, once, what a test armed for `table` between a page's two reads
/// (MultiGet's and Scan's read again), as a write landing there would (#1848). Process-wide
/// and keyed by table name only: each test that arms it uses a table name of its own.
#[cfg(test)]
pub(crate) static BETWEEN_READS: std::sync::Mutex<Vec<BetweenReads>> =
    std::sync::Mutex::new(Vec::new());

/// [`BETWEEN_READS`]' entry: the table, and what runs between its page's reads.
#[cfg(test)]
pub(crate) type BetweenReads = (String, Box<dyn FnOnce() + Send>);

#[cfg(test)]
fn between_reads(table: &str) {
    let mut armed = BETWEEN_READS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(i) = armed.iter().position(|(t, _)| t == table) {
        let (_, f) = armed.swap_remove(i);
        drop(armed);
        f();
    }
}

/// A page of `table` within `budget` bytes, with its rows converted: MultiGet's and Scan's.
///
/// `read` reads a page within the bytes it is given, first `budget`. The rows are converted
/// a column at a time, stopping at the first that doesn't fit with its aggregate view, and
/// the page is then read again to end there (audit120's F4 and its S1 on #1812): no more
/// than the budget and one column is ever built. The columns that fit are fewer than the
/// page's, so each read keeps fewer; `min` keeps that true even if the measures ever differ.
#[cfg_attr(not(test), allow(unused_variables))]
pub(super) fn page_within<P>(
    table: &str,
    budget: u64,
    kinds: &convert::Kinds<'_>,
    q: Option<&proto::AggregateQuery>,
    mut read: impl FnMut(u64) -> Result<P, ApiError>,
    rows: impl Fn(&P) -> &[pigeonhole::Row],
) -> Result<(P, Vec<proto::Row>), ApiError> {
    let mut within = budget;
    loop {
        let page = read(within)?;
        match convert::rows_within(rows(&page), kinds, q, budget) {
            Ok(rows) => return Ok((page, rows)),
            Err(cut) => {
                within = cut.min(within.saturating_sub(1));
                #[cfg(test)]
                between_reads(table);
            }
        }
    }
}

/// A hold seam (tests): a scan of `table` waits, once, after it hands a page to its stream,
/// until the armed sender sends or is dropped. A scan task the client's connection drains at
/// once ends, and gives back its `--max-scans` slot, before a client that reads nothing more
/// can ask for another scan (#1947); held, its slot stays taken. The wait is on the scan's own
/// task, so the server keeps taking calls. Process-wide and keyed by table name only: each
/// test that arms it uses a table name of its own.
#[cfg(test)]
pub(crate) static HOLD_AFTER_PAGE: std::sync::Mutex<Vec<(String, oneshot::Receiver<()>)>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
async fn hold_after_page(table: &str) {
    let armed = {
        let mut armed = HOLD_AFTER_PAGE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        armed
            .iter()
            .position(|(t, _)| t == table)
            .map(|i| armed.swap_remove(i).1)
    };
    if let Some(release) = armed {
        let _ = release.await;
    }
}

/// A table's scan, ready to start: see [`Service::scan_opener`].
/// The tests' seam (D328 PR 2b): a replica copy whose database is under one of these
/// directories reads this many more scan pages, then answers `ReplicaBehind`, as a copy
/// that fell behind mid-scan would.
#[cfg(test)]
pub(crate) static BEHIND_AFTER_PAGES: std::sync::Mutex<Vec<(std::path::PathBuf, usize)>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn behind_after_pages(db: &std::path::Path) -> Result<(), ApiError> {
    let mut seam = BEHIND_AFTER_PAGES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some((_, left)) = seam.iter_mut().find(|(dir, _)| db.starts_with(dir)) {
        if *left == 0 {
            return Err(ApiError::replica_behind(
                "fell behind mid-scan (a test's seam)",
            ));
        }
        *left -= 1;
    }
    Ok(())
}

/// What each page of a cluster partition's scan checks, before it takes its snapshot and
/// again before it's sent.
#[derive(Clone, Debug)]
pub(crate) enum PageCheck {
    /// The partition's lease, on its owner (D326).
    Lease(Arc<super::super::gate::WriteGate>),
    /// A replica copy's freshness within the client's bound in ms (D328 PR 2b, S2): a page
    /// past it ends the stream with `ReplicaBehind`, and the entry node continues from the
    /// last page's position elsewhere.
    Replica(Arc<super::super::replica::Replica>, u64),
}

impl PageCheck {
    fn check(&self) -> Result<(), ApiError> {
        match self {
            Self::Lease(gate) => gate.serving(),
            Self::Replica(replica, max_lag_ms) => replica.within(*max_lag_ms),
        }
    }

    /// The seqno a page was read at, after it (`ScanPage.served_seqno`): a replica's applied
    /// seqno, or the owner's visible seqno.
    fn served_seqno(&self, db: &pigeonhole::Pigeonhole) -> u64 {
        match self {
            Self::Lease(_) => db.visible_seqno(),
            Self::Replica(replica, _) => replica.applied_seqno(),
        }
    }
}

pub(crate) struct ScanOpener {
    table: Table,
    /// A cluster partition's per-page check: its lease on its owner (D326), or a replica
    /// copy's bound (D328).
    check: Option<PageCheck>,
    /// The partition's database, for a page's snapshot when the request pins none.
    db: pigeonhole::Pigeonhole,
    pool: Arc<Semaphore>,
    reads: Arc<Semaphore>,
    inflight: Arc<Semaphore>,
    limits: Limits,
}

impl ScanOpener {
    /// The opener of a replica copy's scan, each page checked against the client's bound
    /// (D328 PR 2b) in place of a lease.
    pub(crate) fn bounded(
        mut self,
        replica: Arc<super::super::replica::Replica>,
        max_lag_ms: u64,
    ) -> Self {
        self.check = Some(PageCheck::Replica(replica, max_lag_ms));
        self
    }

    /// Starts `r`'s scan: its pages, read one ahead at most by a task of its own, then its
    /// error if it fails. A failure before the first page (a bad position) is returned here.
    /// The scan holds `slot` (a `--max-scans` slot) until its task ends: once the server's
    /// stream has taken its last page (or the idle limit, or a failure, ends it), not once the
    /// client has read it. One partition's part of a cluster scan holds none, since the
    /// fanned-out stream holds it.
    /// With `metrics`, its outcome is counted as a `Data.Scan` when it ends.
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
            super::super::cluster::position::refuse_on_a_single_node(&r.after)?;
            Some(ScanPosition::from_bytes(&r.after).map_err(ApiError::from)?)
        };
        let task = |pages| {
            ScanTask {
                table: self.table.clone(),
                check: self.check.clone(),
                db: self.db.clone(),
                r,
                after,
                pinned,
                pool: Arc::clone(&self.pool),
                reads: Arc::clone(&self.reads),
                inflight: Arc::clone(&self.inflight),
                limits: self.limits,
                pages,
            }
            .run()
        };
        Ok(ScanPages::spawn(task, slot, metrics))
    }
}

/// A scan's reading side, run as its own task.
struct ScanTask {
    table: Table,
    check: Option<PageCheck>,
    db: pigeonhole::Pigeonhole,
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
            check,
            db,
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
            // A cluster partition's lease, for each page (D326, audit120's M2): before it
            // takes the page's snapshot, and again once the page is read, before it's sent
            // (S3). A page is a read at a snapshot of its own unless the request pins one.
            if let Some(c) = &check {
                c.check()?;
            }
            let pinned_at = pinned.as_ref().map(|p| p.snapshot().clone());
            // For the page's served seqno, after it's read: a cluster partition's only.
            let page_db = check.as_ref().map(|_| db.clone());
            let (table, db, req, pos) = (table.clone(), db.clone(), Arc::clone(&r), after.take());
            let thread = Arc::clone(&reads)
                .acquire_owned()
                .await
                .map_err(|_| ApiError::internal("the scan threads closed"))?;
            let (mut rows, mut next, mut bytes, cut) = blocking(&pool, move || {
                // One snapshot for the page's reads, as MultiGet's (#1848), when it can be
                // read again.
                let at = match pinned_at {
                    Some(s) => Some(s),
                    None if convert::Kinds::new(&table).any_view() => Some(db.snapshot()?),
                    None => None,
                };
                let select = convert::Select::new(req.selection.as_ref())?.at(at);
                let q = select.aggregate();
                let kinds = convert::Kinds::new(&table);
                // Converted a column at a time, stopping at the first that doesn't fit with its
                // aggregate view, then read again to end the page there (audit120's F4 and its
                // S1 on #1812): no more than the budget and one column is ever built.
                let (page, rows) = page_within(
                    &req.table,
                    budget,
                    &kinds,
                    q,
                    |within| {
                        let mut scan = select.scan(&table, req.range.as_ref())?;
                        if let Some(n) = limit {
                            scan = scan.limit(n);
                        }
                        if let Some(p) = &pos {
                            scan = scan.resume_after(p);
                        }
                        Ok(scan.read_within(within)?)
                    },
                    |page| page.rows(),
                )?;
                let bytes = rows.iter().map(row_bytes).sum();
                Ok((rows, page.resume().cloned(), bytes, page.cut_by_budget()))
            })
            .await?;
            drop(thread);
            if let Some(c) = &check {
                c.check()?;
            }
            #[cfg(test)]
            if let (Some(PageCheck::Replica(..)), Some(db)) = (&check, &page_db) {
                behind_after_pages(db.path())?;
            }
            let served_seqno = match (&check, &page_db) {
                (Some(c), Some(db)) => c.served_seqno(db),
                _ => 0,
            };
            let continued = continues(last_row.as_deref(), &rows);
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
                served_seqno,
            };
            queued = Some(queue_page(space, page, None, &inflight, &limits).await?);
            #[cfg(test)]
            hold_after_page(&r.table).await;
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

/// Queues `page` for the client in `space`, with its share of the in-flight budget, and
/// gives the slot it waits in. The share is `paid` when the caller already holds one for
/// the page (#1753), else one taken here: the page's bytes, capped so one scan can't take
/// the whole budget (a page over `--max-request-bytes`, one huge cell, takes the cap),
/// waited for at most the request timeout.
pub(crate) async fn queue_page(
    space: mpsc::Permit<'_, PageSlot>,
    page: proto::ScanPage,
    paid: Option<OwnedSemaphorePermit>,
    inflight: &Arc<Semaphore>,
    limits: &Limits,
) -> Result<PageSlot, ApiError> {
    let paid = match paid {
        Some(paid) => paid,
        None => {
            let share = usize::try_from(page.bytes)
                .unwrap_or(usize::MAX)
                .min(limits.max_request_bytes);
            budget_within(inflight, share, limits.request_timeout).await?
        }
    };
    let slot: PageSlot = Arc::new(std::sync::Mutex::new(Some((page, paid))));
    space.send(Arc::clone(&slot));
    Ok(slot)
}

/// Whether a page of `rows` continues `last_row`, the row the page before ended in (or the
/// client's position is in): its first row is that row, which was counted already.
pub(crate) fn continues(last_row: Option<&[u8]>, rows: &[proto::Row]) -> bool {
    matches!((last_row, rows.first()), (Some(p), Some(first)) if first.key[..] == *p)
}

/// A row's size by D308's measure as the converted cells give it: its key and its cells.
pub(super) fn row_bytes(row: &proto::Row) -> u64 {
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

impl Service {
    /// A row's selected cells, a page at a time: whole columns, at most `page_bytes` by
    /// D308's measure (perf287's S1) and `page_cells` cells, resumed from `after`. The
    /// first column always comes back, so a page makes progress even when one column is
    /// larger than the budget. Sync, since row pages are sync-only (D196): a page within the
    /// default budget on the light pool (#1880), a larger one on the blocking pool.
    /// TODO(#398, #1286): `read_async`-based paging, so a row read holds no blocking thread.
    pub(super) async fn read_row(
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
        let read = move || {
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
                let kinds = convert::Kinds::new(&table);
                // One column per engine page, so the byte budget is checked at every column
                // boundary: a page is cut before the column that wouldn't fit, and only a
                // single column larger than the budget goes over it (D308's rule).
                let mut pages = read.pages(1)?;
                let mut out: Option<proto::Row> = None;
                let (mut bytes, mut cells) = (0u64, 0usize);
                let mut next = Vec::new();
                let mut more = false;
                while let Some(page) = pages.next()? {
                    let part = convert::row(page, &kinds, q);
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
        };
        // A page within the default budget is a small, bounded read: the light pool, on the
        // siblings too. A larger one stays off them (perf287's note on #1880). The pool is
        // tried before any await, so the future holds either the reply or the blocking call,
        // never the closure twice: held across both, it grew every call's dispatch future,
        // and moving it cost Get and Mutate about 550 instructions of memcpy a call (x86
        // callgrind on #1972; boxing it instead cost each ReadRow about 10K).
        let lite = if budget <= DEFAULT_PAGE_BYTES {
            self.lite.try_run(read)
        } else {
            Err(read)
        };
        match lite {
            Ok(reply) => reply
                .await
                .map_err(|_| ApiError::internal("the light pool's call was lost"))?,
            Err(read) => self.blocking(read).await,
        }
    }

    /// A scan as a stream of pages (ICR 0112): each page is one `read_within` on the
    /// blocking pool (D196: scan pages are sync-only), resumed from the last, until the range
    /// or `limit` runs out. Each page is a new read, at its own snapshot (simp724's D1 on
    /// #1510), unless the selection names a pinned one, which the stream holds. The request is checked before the stream starts, so a bad one fails the call;
    /// a failure later ends the stream with the error, a panic in the task as `Internal`. The
    /// scan holds a `--max-scans` slot until the server's stream has taken its last page (or
    /// the idle limit ends it), not until the client has read it: a stream outlives its
    /// request's `--max-concurrent` slot. TODO(#398, #1286): async scan pages, so a page read
    /// holds no blocking thread.
    pub(super) async fn scan(
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

    /// A scan of a cluster replica copy (D328 PR 2b): as [`Service::call`] answers one,
    /// with each page checked against the client's bound `max_lag_ms` in place of a lease
    /// (S2). Never at a snapshot: the router sends those to the owner.
    pub(crate) async fn replica_scan(
        &self,
        r: proto::ScanRequest,
        caller: &Caller,
        max_lag_ms: u64,
    ) -> Result<ScanPages, ApiError> {
        let replica = self
            .replica
            .clone()
            .ok_or_else(|| ApiError::internal("a replica scan on a service that isn't one"))?;
        let op = ServiceOp::Scan(r);
        let budget = self.admit(&op.admit(), caller, None).await?;
        self.copy_ranges(&op)?;
        let ServiceOp::Scan(r) = op else {
            unreachable!("made a scan just above")
        };
        let slot = self.scan_slot()?;
        convert::Select::new(r.selection.as_ref())?;
        let opener = self
            .scan_opener(&r.table)
            .await?
            .bounded(replica, max_lag_ms);
        drop(budget);
        opener.open(r, Some(slot), None, Some(Arc::clone(&self.metrics)))
    }

    /// D331 PR 3: on a cluster replica copy (it has no `WriteGate`), the copy's range gate,
    /// which its follower keeps: a copy of a move's target serves the range once it has
    /// applied the target's serving row, and a copy of its source refuses a moved range as
    /// the source does. One load, `None` on every other service.
    #[inline]
    pub(super) fn copy_ranges(&self, op: &ServiceOp) -> Result<(), ApiError> {
        if let Some(replica) = &self.replica
            && let Some(ranges) = &*replica.ranges().load()
        {
            ranges.check(op, false)?;
        }
        Ok(())
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
            check: self.gate.clone().map(PageCheck::Lease),
            db: self.part()?.db.clone(),
            pool: Arc::clone(&self.blocking),
            reads: Arc::clone(&self.scan_reads),
            inflight: Arc::clone(&self.inflight),
            limits: self.limits,
        })
    }

    /// The pinned snapshot `id` names, held for one read; `None` for an empty id (now).
    pub(super) fn pinned(
        &self,
        id: &str,
        caller: &Caller,
    ) -> Result<Option<SnapshotGuard>, ApiError> {
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
}
