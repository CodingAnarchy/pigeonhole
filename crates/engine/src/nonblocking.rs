//! Async reads (#42, D196, ICR 0014): point gets and row reads as futures that never block
//! their executor thread on a block the cache does not hold.
//!
//! A poll runs the ordinary read in cache-only mode. Memtable and cache hits resolve on the
//! first poll. When the read misses a block (`Error::WouldBlock`), the future submits that
//! block's read through the VFS (`submit_read`, a completion that wakes it), admits the
//! verified block to the cache when it completes, keeps it pinned, and reads again: the
//! blocks it fetched are hits now. The read point (view and seqno) is taken on the first
//! poll and held, so every attempt reads the same data.
//!
//! A read falls back to one synchronous attempt, counted in `Metrics::async_sync_reads`,
//! when the cache cannot keep a fetched block (capacity 0, or a block larger than a cache
//! shard) or after `MAX_FETCHES` fetches. A separated value is fetched the same way (its
//! extent header, then its record, in pieces when it spans extents); one too large to cache
//! is read synchronously and counted (D196, #398).

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
            && let Some((view, seqno)) = self.inner.latest_view()
        {
            return Ok(At::Latest {
                view: arc_swap::Guard::into_inner(view),
                seqno,
            });
        }
        self.inner.snapshot().map(At::Snapshot)
    }

    /// Runs `attempt(view, seqno, now, cache_only)` until it does not miss.
    fn poll_read<T>(
        &mut self,
        cx: &mut Context<'_>,
        mut attempt: impl FnMut(&Arc<View>, Seqno, Timestamp, bool) -> Result<T>,
    ) -> Poll<Result<T>> {
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
            let (r, sync_reads) = as_async_read(cache_only, || {
                attempt(at.view(), at.seqno(), self.now, cache_only)
            });
            if sync_reads > 0 {
                self.inner
                    .shared
                    .async_sync_reads
                    .fetch_add(sync_reads, Ordering::Relaxed);
            }
            match at.checked(r) {
                Err(Error::WouldBlock(fetch)) => {
                    self.fetches += 1;
                    if self.fetches > MAX_FETCHES {
                        self.go_sync();
                        continue;
                    }
                    let completion = fetch.submit();
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

/// A point get as a future ([`Engine::get_latest_async`](crate::Engine::get_latest_async),
/// [`Engine::get_async`](crate::Engine::get_async)). Dropping it is always safe.
#[must_use = "a read does nothing unless polled"]
pub struct GetFuture {
    reading: Reading,
    table: TableId,
    family: FamilyId,
    row: Vec<u8>,
    qualifier: Vec<u8>,
}

impl std::fmt::Debug for GetFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GetFuture")
            .field("table", &self.table)
            .field("family", &self.family)
            .field("fetches", &self.reading.fetches)
            .finish()
    }
}

impl GetFuture {
    pub(crate) fn new(
        inner: Arc<Inner>,
        snapshot: Option<Snapshot>,
        table: TableId,
        family: FamilyId,
        row: &[u8],
        qualifier: &[u8],
    ) -> Self {
        Self {
            reading: Reading::new(inner, snapshot),
            table,
            family,
            row: row.to_vec(),
            qualifier: qualifier.to_vec(),
        }
    }
}

impl Future for GetFuture {
    type Output = Result<Option<CellData>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let (table, family, row, qualifier) = (this.table, this.family, &this.row, &this.qualifier);
        this.reading.poll_read(cx, |view, seqno, now, cache_only| {
            if cache_only {
                get_in::<true>(view, seqno, now, table, family, row, qualifier, || {
                    Arc::clone(view)
                })
            } else {
                get_in::<false>(view, seqno, now, table, family, row, qualifier, || {
                    Arc::clone(view)
                })
            }
        })
    }
}

/// A row read as a future, resolving to its sink when the row has a visible cell
/// ([`Engine::read_row_latest_async`](crate::Engine::read_row_latest_async),
/// [`Engine::read_row_async`](crate::Engine::read_row_async)). Dropping it is always safe.
#[must_use = "a read does nothing unless polled"]
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
    pub(crate) fn new(
        inner: Arc<Inner>,
        snapshot: Option<Snapshot>,
        table: TableId,
        row: &[u8],
        families: &[FamilyId],
        spec: &ReadSpec,
        sink: S,
    ) -> Self {
        Self {
            reading: Reading::new(inner, snapshot),
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
        let r = this.reading.poll_read(cx, |view, seqno, now, cache_only| {
            sink.clone_from(empty);
            let families = crate::engine::families_in_order(view, table, families)?;
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
