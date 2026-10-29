//! Async reads (#42, D196, ICR 0014): point gets and row reads as futures that never block
//! their executor thread on a block the cache does not hold.
//!
//! A latest read in a writer process first runs at the call, in cache-only mode, on the
//! caller's thread under the view's `arc-swap` guard, as the sync read does: a memtable or
//! cache hit resolves there, and the future only hands the result over. It holds no
//! engine, view or copied key, so concurrent async readers share no reference count
//! (#709). Otherwise a poll runs the ordinary read in cache-only mode. When the read misses
//! a block (`Error::WouldBlock`), the future submits that block's read through the VFS
//! (`submit_read`, a completion that wakes it), admits the verified block to the cache
//! when it completes, keeps it pinned, and reads again: the blocks it fetched are hits now.
//! A future's read point (view and seqno) is taken on its first poll and held, so every
//! attempt reads the same data.
//!
//! A future falls back to one synchronous attempt, counted in `Metrics::async_sync_reads`,
//! when the cache cannot keep a fetched block (capacity 0, or a block larger than a cache
//! shard) or after `MAX_FETCHES` fetches. A separated value is fetched the same way (its
//! extent header, then its record, in pieces when it spans extents); one too large to cache
//! is read synchronously and counted (D196, #398): at the call, when the attempt there
//! reaches it (`try_latest_now`), else inside the future.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use pigeonhole_cache::BlockHandle;
use pigeonhole_format::{FamilyId, Seqno, TableId, Timestamp};
use pigeonhole_io::Completion;
use pigeonhole_sst::Fetch;

use crate::engine::Inner;
use crate::read::{self, CellData, ReadSpec, RowSink, as_async_read, get_in};
use crate::snapshot::{Snapshot, View};
use crate::{Error, Result};

/// Fetches one read makes before it reads synchronously instead. A point get or row read
/// misses at most a few blocks per SST it opens (footer, top index, filters, properties,
/// an index partition, a data block), so a read that keeps missing is pathological (a cache
/// too small for its working set).
const MAX_FETCHES: u32 = 64;

/// Expired-snapshot retries in a reader process (as `get_latest`).
const READER_EXPIRED_RETRIES: u32 = 8;

/// Where a read reads: the latest view through an unpinned seqno (writer process, as
/// `get_latest`), or a snapshot.
enum At {
    Latest { view: Arc<View>, seqno: Seqno },
    Snapshot(Snapshot),
}

impl At {
    fn view(&self) -> &Arc<View> {
        match self {
            At::Latest { view, .. } => view,
            At::Snapshot(s) => &s.view,
        }
    }

    fn seqno(&self) -> Seqno {
        match self {
            At::Latest { seqno, .. } => *seqno,
            At::Snapshot(s) => s.seqno,
        }
    }

    fn checked<T>(&self, r: Result<T>) -> Result<T> {
        match self {
            At::Latest { .. } => r,
            At::Snapshot(s) => s.checked(r),
        }
    }

    /// The row cache a latest read at this point goes through (D201, D281): the writer's for
    /// the latest view, and for a snapshot its generation's reader cache in a reader process
    /// (#946), else the writer's.
    fn row_cache<'a>(&'a self, inner: &'a Inner) -> Option<&'a crate::row_cache::RowCaches> {
        match self {
            At::Latest { .. } => inner.shared.row_cache.as_ref(),
            At::Snapshot(s) => s.reader_row_cache().or(inner.shared.row_cache.as_ref()),
        }
    }
}

/// What one attempt of a read gets: the read point and, for a latest read, the row cache
/// with the check a fill must pass first (the snapshot still protects what was read).
pub(crate) struct Attempt<'a> {
    pub view: &'a Arc<View>,
    pub seqno: Seqno,
    pub now: Timestamp,
    pub cache_only: bool,
    pub row_cache: Option<&'a crate::row_cache::RowCaches>,
    at: &'a At,
}

impl Attempt<'_> {
    /// Whether a fill may be stored: always in the writer; in a reader process, while its
    /// snapshot is current (checked after the read).
    pub(crate) fn still_valid(&self) -> bool {
        self.at.checked(Ok(())).is_ok()
    }
}

/// The fetch-and-retry loop shared by the read futures.
struct Reading {
    inner: Arc<Inner>,
    /// `None` until the first poll (as of now), or the snapshot given.
    at: Option<At>,
    /// Taken with the read point, for TTL.
    now: Timestamp,
    /// Whether `at` was given (a snapshot read) rather than taken.
    given: bool,
    fetch: Option<(Completion, Box<Fetch>)>,
    /// Blocks fetched for this read, held until it resolves.
    pinned: Vec<BlockHandle>,
    fetches: u32,
    /// Read synchronously from now on (counted).
    sync: bool,
    expired: u32,
}

impl Reading {
    /// Starts with the fetch a latest read at the call missed on, if any (#709): the read
    /// the cursor already submitted is resumed, not submitted again, and the first poll
    /// waits on it instead of running the read once more just to find the same miss.
    fn with_missed(mut self, missed: MissedRead) -> Self {
        if let Some(m) = missed.0 {
            let Missed {
                mut fetch,
                view,
                seqno,
                now,
            } = *m;
            // The read point of the call: its view keeps the SST the read is in flight in
            // (and so the extent it reads) from being retired and reused before the read
            // lands; a view taken later need not hold it (perf287, #1086).
            self.at = Some(At::Latest { view, seqno });
            self.now = now;
            self.fetches = 1;
            let completion = fetch.start();
            self.fetch = Some((completion, fetch));
        }
        self
    }

    fn new(inner: Arc<Inner>, snapshot: Option<Snapshot>) -> Self {
        let given = snapshot.is_some();
        Self {
            inner,
            at: snapshot.map(At::Snapshot),
            now: 0,
            given,
            fetch: None,
            pinned: Vec::new(),
            fetches: 0,
            sync: false,
            expired: 0,
        }
    }

    /// The read point as of now: the latest view, as `get_latest` reads it (D188, #315), or
    /// a pinned snapshot in a reader process or under a steady stream of commits.
    fn take_point(&self) -> Result<At> {
        if !self.inner.is_reader()
            && let Some((view, seqno)) = self.inner.latest_view()?
        {
            return Ok(At::Latest {
                view: arc_swap::Guard::into_inner(view),
                seqno,
            });
        }
        self.inner.snapshot().map(At::Snapshot)
    }

    /// Runs `attempt` until it does not miss.
    fn poll_read<T>(
        &mut self,
        cx: &mut Context<'_>,
        mut attempt: impl FnMut(&Attempt<'_>) -> Result<T>,
    ) -> Poll<Result<T>> {
        // A forked child's copy of the engine reads nothing (#901).
        if let Err(e) = self.inner.check_process() {
            return Poll::Ready(Err(e));
        }
        loop {
            if let Some((completion, fetch)) = &mut self.fetch {
                let buf = match Pin::new(completion).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(r) => r,
                };
                let admitted = buf.map_err(Error::from).and_then(|b| Ok(fetch.admit(b)?));
                let kept = fetch.is_kept();
                self.fetch = None;
                match admitted {
                    Ok(h) => self.pinned.extend(h),
                    Err(e) => return Poll::Ready(Err(e)),
                }
                if !kept {
                    // The cache keeps nothing: one synchronous attempt instead.
                    self.go_sync();
                }
            }
            if self.at.is_none() {
                match self.take_point() {
                    Ok(at) => {
                        self.at = Some(at);
                        self.now = self.inner.shared.vfs.now_micros();
                    }
                    Err(e) => return Poll::Ready(Err(e)),
                }
            } else if self.now == 0 {
                self.now = self.inner.shared.vfs.now_micros();
            }
            let Some(at) = self.at.as_ref() else {
                unreachable!("taken above");
            };
            let cache_only = !self.sync;
            let point = Attempt {
                view: at.view(),
                seqno: at.seqno(),
                now: self.now,
                cache_only,
                // A snapshot read given by the caller never goes through a row cache.
                row_cache: if self.given {
                    None
                } else {
                    at.row_cache(&self.inner)
                },
                at,
            };
            let (r, sync_reads) = as_async_read(cache_only, || attempt(&point));
            if sync_reads > 0 {
                self.inner
                    .shared
                    .async_sync_reads
                    .fetch_add(sync_reads, Ordering::Relaxed);
            }
            match at.checked(r) {
                Err(Error::WouldBlock(mut fetch)) => {
                    self.fetches += 1;
                    if self.fetches > MAX_FETCHES {
                        // Drops `fetch`, maybe with its read in flight (#709). That is safe: a
                        // dropped `Completion` leaves the buffer with the backend until the
                        // device is done with it (pigeonhole-io's completion contract, #976).
                        self.go_sync();
                        continue;
                    }
                    // The read the cursor submitted when it missed, if it did (#709).
                    let completion = fetch.start();
                    self.fetch = Some((completion, fetch));
                }
                Err(Error::SnapshotExpired)
                    if !self.given && self.expired < READER_EXPIRED_RETRIES =>
                {
                    // A reader process's snapshot a writer restart expired: a fresh one.
                    self.expired += 1;
                    self.at = None;
                }
                other => {
                    self.pinned.clear();
                    return Poll::Ready(other);
                }
            }
        }
    }

    fn go_sync(&mut self) {
        if !self.sync {
            self.sync = true;
            self.inner
                .shared
                .async_sync_reads
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// The fetch a latest read at the call missed on (#709), for the future to take over:
/// `None` when the call did not run the read (a reader process, or no read point yet), or
/// the miss carried no fetch. Opaque outside the engine: from
/// [`Engine::try_read_row_latest`](crate::Engine::try_read_row_latest) to
/// [`Engine::read_row_latest_async_after`](crate::Engine::read_row_latest_async_after).
#[derive(Debug, Default)]
pub struct MissedRead(Option<Box<Missed>>);

/// What [`MissedRead`] carries: the fetch, and the read point of the call that missed.
#[derive(Debug)]
struct Missed {
    fetch: Box<Fetch>,
    view: Arc<View>,
    seqno: Seqno,
    now: Timestamp,
}

/// A latest read at the call, in a writer process (#709): `attempt` runs once in cache-only
/// mode under the view's guard, as the sync read reads, with the writer's row cache. Its
/// result, an error included; or, when it would wait on the device (a page-cache miss the
/// cursor could not read in place), in a reader process, or when the visible seqno kept
/// moving, the miss: the future takes it over with its read in flight and the call's read
/// point ([`Reading::with_missed`]), so the result is as of the call, as a hit's is.
pub(crate) fn try_latest_now<T>(
    inner: &Inner,
    attempt: impl FnOnce(
        &Arc<View>,
        Seqno,
        Timestamp,
        Option<&crate::row_cache::RowCaches>,
    ) -> Result<T>,
) -> std::result::Result<Result<T>, MissedRead> {
    if inner.is_reader() {
        return Err(MissedRead::default());
    }
    let (view, seqno) = match inner.latest_view() {
        Ok(Some(point)) => point,
        Ok(None) => return Err(MissedRead::default()),
        Err(e) => return Ok(Err(e)),
    };
    let now = inner.shared.vfs.now_micros();
    let (r, sync_reads) = as_async_read(true, || {
        attempt(&view, seqno, now, inner.shared.row_cache.as_ref())
    });
    if sync_reads > 0 {
        inner
            .shared
            .async_sync_reads
            .fetch_add(sync_reads, Ordering::Relaxed);
    }
    match r {
        Err(Error::WouldBlock(fetch)) => Err(MissedRead(Some(Box::new(Missed {
            fetch,
            view: arc_swap::Guard::into_inner(view),
            seqno,
            now,
        })))),
        other => Ok(other),
    }
}

/// One attempt of a point get at a read point, through the row cache when given (D201;
/// a reader process's, #946).
#[allow(clippy::too_many_arguments)]
fn get_attempt(
    view: &Arc<View>,
    seqno: Seqno,
    now: Timestamp,
    cache_only: bool,
    row_cache: Option<&crate::row_cache::RowCaches>,
    table: TableId,
    family: FamilyId,
    row: &[u8],
    qualifier: &[u8],
) -> Result<Option<CellData>> {
    if let Some(rc) = row_cache
        && let Some(hit) =
            crate::row_cache::get_cached(rc, view, seqno, now, table, family, row, qualifier)
    {
        return Ok(hit);
    }
    if cache_only {
        get_in::<true>(
            view,
            seqno,
            || now,
            table,
            family,
            row,
            qualifier,
            || Arc::clone(view),
        )
    } else {
        get_in::<false>(
            view,
            seqno,
            || now,
            table,
            family,
            row,
            qualifier,
            || Arc::clone(view),
        )
    }
}

/// A point get as a future ([`Engine::get_latest_async`](crate::Engine::get_latest_async),
/// [`Engine::get_async`](crate::Engine::get_async)). Dropping it is always safe.
#[must_use = "a read's result is lost unless it is polled"]
pub struct GetFuture {
    state: GetState,
}

enum GetState {
    /// Resolved at the call (#709); `None` once taken.
    Done(Option<Result<Option<CellData>>>),
    Reading {
        reading: Reading,
        table: TableId,
        family: FamilyId,
        row: Vec<u8>,
        qualifier: Vec<u8>,
    },
}

impl std::fmt::Debug for GetFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.state {
            GetState::Done(_) => f.debug_struct("GetFuture").field("done", &true).finish(),
            GetState::Reading {
                reading,
                table,
                family,
                ..
            } => f
                .debug_struct("GetFuture")
                .field("table", table)
                .field("family", family)
                .field("fetches", &reading.fetches)
                .finish(),
        }
    }
}

impl GetFuture {
    /// A latest get (`snapshot` `None`) runs at the call first (#709): a hit resolves here.
    pub(crate) fn new(
        inner: &Arc<Inner>,
        snapshot: Option<Snapshot>,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
    ) -> Self {
        let mut missed = MissedRead::default();
        if snapshot.is_none() {
            match try_latest_now(inner, |view, seqno, now, rc| {
                get_attempt(view, seqno, now, true, rc, table, family, row, qualifier)
            }) {
                Ok(r) => {
                    return Self {
                        state: GetState::Done(Some(r)),
                    };
                }
                Err(m) => missed = m,
            }
        }
        Self {
            state: GetState::Reading {
                reading: Reading::new(Arc::clone(inner), snapshot).with_missed(missed),
                table,
                family,
                row: row.to_vec(),
                qualifier: qualifier.to_vec(),
            },
        }
    }
}

impl Future for GetFuture {
    type Output = Result<Option<CellData>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            GetState::Done(r) => Poll::Ready(r.take().unwrap_or_else(|| {
                Err(Error::InvalidArgument(
                    "get polled after it resolved".into(),
                ))
            })),
            GetState::Reading {
                reading,
                table,
                family,
                row,
                qualifier,
            } => {
                let (table, family) = (*table, *family);
                reading.poll_read(cx, |p| {
                    get_attempt(
                        p.view,
                        p.seqno,
                        p.now,
                        p.cache_only,
                        p.row_cache,
                        table,
                        family,
                        row,
                        qualifier,
                    )
                })
            }
        }
    }
}

/// A row read as a future, resolving to its sink when the row has a visible cell
/// ([`Engine::read_row_latest_async`](crate::Engine::read_row_latest_async),
/// [`Engine::read_row_async`](crate::Engine::read_row_async)). Dropping it is always safe.
#[must_use = "a read's result is lost unless it is polled"]
pub struct RowFuture<S> {
    reading: Reading,
    table: TableId,
    row: Vec<u8>,
    families: Vec<FamilyId>,
    spec: ReadSpec,
    sink: Option<S>,
    /// The sink as given, empty: each attempt starts from a copy of it, so cells pushed
    /// before a miss are dropped.
    empty: S,
}

impl<S> std::fmt::Debug for RowFuture<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowFuture")
            .field("table", &self.table)
            .field("fetches", &self.reading.fetches)
            .finish()
    }
}

impl<S: RowSink + Clone> RowFuture<S> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        inner: Arc<Inner>,
        snapshot: Option<Snapshot>,
        missed: MissedRead,
        table: TableId,
        row: &[u8],
        families: &[FamilyId],
        spec: &ReadSpec,
        sink: S,
    ) -> Self {
        Self {
            reading: Reading::new(inner, snapshot).with_missed(missed),
            table,
            row: row.to_vec(),
            families: families.to_vec(),
            spec: spec.clone(),
            empty: sink.clone(),
            sink: Some(sink),
        }
    }
}

impl<S: RowSink + Clone + Unpin> Future for RowFuture<S> {
    type Output = Result<Option<S>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let Some(sink) = this.sink.as_mut() else {
            return Poll::Ready(Err(Error::InvalidArgument(
                "row read polled after it resolved".into(),
            )));
        };
        let (table, row, families, spec, empty) = (
            this.table,
            &this.row,
            &this.families,
            &this.spec,
            &this.empty,
        );
        // A latest read goes through the row cache, as the sync one does (D201; a reader
        // process's, #946).
        let r = this.reading.poll_read(cx, |p| {
            let (view, seqno, now, cache_only) = (p.view, p.seqno, p.now, p.cache_only);
            sink.clone_from(empty);
            let families = crate::engine::families_in_order(view, table, families)?;
            if let Some(rc) = p.row_cache
                && crate::row_cache::serves_spec(spec)
            {
                return crate::row_cache::read_row_cached(
                    rc,
                    view,
                    seqno,
                    table,
                    row,
                    &families,
                    spec,
                    now,
                    cache_only,
                    sink,
                    || p.still_valid(),
                );
            }
            read::read_row_into(
                view, seqno, table, row, &families, spec, now, cache_only, sink,
            )
        });
        match r {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(true)) => Poll::Ready(Ok(this.sink.take())),
            Poll::Ready(Ok(false)) => {
                this.sink = None;
                Poll::Ready(Ok(None))
            }
            Poll::Ready(Err(e)) => {
                this.sink = None;
                Poll::Ready(Err(e))
            }
        }
    }
}
