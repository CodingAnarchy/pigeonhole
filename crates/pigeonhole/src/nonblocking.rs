//! The async front door (Phase 3, #42): the same operations as the sync API, over the same
//! engine, as futures: commits, gets, row reads and scan streams.
//!
//! Futures depend only on `std::task`, so they run on any executor (Tokio, smol, a custom
//! one); none of them spawns a thread or uses `spawn_blocking`. A commit future is woken by
//! the shard that applies it and by the watermark that makes it visible; a read future by
//! the completion of the block read it is waiting for.
//!
//! **Reads.** [`Table::get_async`](crate::Table::get_async) and
//! [`RowRead::read_async`](crate::RowRead::read_async) resolve to owned results
//! ([`Cell`], [`Row`]), safe to hold across `.await`. Memtable and
//! cache hits resolve on the first poll, as cheaply as the sync calls. A block the cache
//! does not hold is read through the VFS's asynchronous reads, and the read runs again once
//! it is cached; the read point is taken on the first poll, so every attempt sees the same
//! data. Dropping a read future is always safe. Two cases read synchronously inside the
//! future, counted by [`Pigeonhole::async_sync_reads`](crate::Pigeonhole::async_sync_reads)
//! (D196, #398): a separated value too large to cache (above an eighth of the block cache,
//! at most 1 MiB), and a block the cache cannot keep (a cache of size 0).
//!
//! **Commits.** `commit_async` and `commit_with_async` submit at the call, then resolve when
//! the record meets the requested durability and is visible, exactly when the sync `commit`
//! would return. Async and sync committers join the same commit groups. Dropping the future
//! after the call does **not** roll the commit back: it lands or fails atomically either
//! way. A [`CommitTicket`] is also a future (`ticket.await`).
//!
//! ```
//! use pigeonhole::{Durability, Family, Options, Pigeonhole};
//!
//! # fn main() -> pigeonhole::Result<()> {
//! # let dir = pigeonhole::doc_support::temp_dir();
//! let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(2))?;
//! let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
//! let commit = t.mutate(b"r").put("f", b"q", b"v").commit_async();
//! // Poll it on any executor; here, a minimal one.
//! let info = pigeonhole::doc_support::block_on(commit)?;
//! assert!(info.seqno > 0);
//! # db.close()?;
//! # Ok(())
//! # }
//! ```

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::task::{Context, Poll};

use pigeonhole_format::Durability;

use crate::cell::RowBuf;
use crate::write::{Submitted, TicketState};
use crate::{
    Cell, CommitInfo, CommitTicket, Error, Result, Row, RowMutation, Transaction, WriteBatch,
};
use pigeonhole_engine::ScanCursor;

/// A submitted commit's result, as a future: resolves when the commit meets its durability
/// level and is visible (see the [module docs](self)). Dropping it does not roll the commit
/// back.
#[derive(Debug)]
#[must_use = "dropping a commit future does not cancel the commit, but its result is lost"]
pub struct CommitFuture {
    state: State,
}

#[derive(Debug)]
enum State {
    /// Submitted, not resolved.
    Pending(Submitted),
    /// Resolved without waiting (refused before submission, or a ticket already done);
    /// `None` once taken.
    Ready(Option<Result<CommitInfo>>),
}

impl CommitFuture {
    fn new(submitted: Result<Submitted>) -> Self {
        Self {
            state: match submitted {
                Ok(s) => State::Pending(s),
                Err(e) => State::Ready(Some(Err(e))),
            },
        }
    }
}

impl Future for CommitFuture {
    type Output = Result<CommitInfo>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            State::Pending(s) => match Pin::new(&mut s.pending).poll(cx) {
                Poll::Ready(r) => {
                    let out = s.outcome(r);
                    self.state = State::Ready(None);
                    Poll::Ready(out)
                }
                Poll::Pending => Poll::Pending,
            },
            State::Ready(r) => Poll::Ready(r.take().unwrap_or_else(|| {
                Err(Error::new(
                    crate::ErrorCode::InvalidArgument,
                    "commit future polled after it resolved",
                ))
            })),
        }
    }
}

impl IntoFuture for CommitTicket {
    type Output = Result<CommitInfo>;
    type IntoFuture = CommitFuture;

    fn into_future(self) -> CommitFuture {
        CommitFuture {
            state: match self.into_state() {
                TicketState::Pending(s) => State::Pending(s),
                TicketState::Done(r) => State::Ready(Some(r)),
            },
        }
    }
}

impl RowMutation<'_> {
    /// [`commit`](RowMutation::commit), as a future: submits now, resolves when the commit is
    /// durable at its level and visible. Dropping the future does not roll it back.
    pub fn commit_async(self) -> CommitFuture {
        CommitFuture::new(self.submit())
    }
}

impl WriteBatch {
    /// [`commit`](WriteBatch::commit), as a future (writer default durability). Dropping the
    /// future does not roll it back.
    pub fn commit_async(self) -> CommitFuture {
        CommitFuture::new(self.submit(None))
    }

    /// [`commit_with`](WriteBatch::commit_with), as a future. Dropping the future does not
    /// roll it back.
    pub fn commit_with_async(self, durability: Durability) -> CommitFuture {
        CommitFuture::new(self.submit(Some(durability)))
    }
}

impl Transaction {
    /// [`commit`](Transaction::commit), as a future; resolves to
    /// [`ErrorCode::Conflict`](crate::ErrorCode::Conflict) on a conflicting commit. Dropping
    /// the future does not roll it back: it commits or aborts atomically either way.
    pub fn commit_async(self) -> CommitFuture {
        CommitFuture::new(self.submit(None))
    }

    /// [`commit_with`](Transaction::commit_with), as a future. Dropping the future does not
    /// roll it back.
    pub fn commit_with_async(self, durability: Durability) -> CommitFuture {
        CommitFuture::new(self.submit(Some(durability)))
    }
}

/// A get as a future, resolving to an owned [`Cell`] ([`Table::get_async`](crate::Table::get_async)).
#[derive(Debug)]
#[must_use = "a read does nothing unless polled"]
pub struct GetFuture {
    inner: std::result::Result<pigeonhole_engine::GetFuture, Option<Error>>,
}

impl GetFuture {
    pub(crate) fn new(started: Result<pigeonhole_engine::GetFuture>) -> Self {
        Self {
            inner: started.map_err(Some),
        }
    }
}

impl Future for GetFuture {
    type Output = Result<Option<Cell>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.inner {
            Ok(f) => Pin::new(f)
                .poll(cx)
                .map(|r| r.map(|c| c.map(Cell::from_data)).map_err(Error::from)),
            Err(e) => Poll::Ready(Err(e.take().unwrap_or_else(polled_after_done))),
        }
    }
}

/// A row read as a future, resolving to an owned [`Row`], or `None` if the row has no
/// matching cell ([`RowRead::read_async`](crate::RowRead::read_async)).
#[derive(Debug)]
#[must_use = "a read does nothing unless polled"]
pub struct RowFuture {
    inner: std::result::Result<pigeonhole_engine::RowFuture<RowBuf>, Option<Error>>,
    key: Vec<u8>,
}

impl RowFuture {
    pub(crate) fn new(started: Result<pigeonhole_engine::RowFuture<RowBuf>>, key: Vec<u8>) -> Self {
        Self {
            inner: started.map_err(Some),
            key,
        }
    }
}

impl Future for RowFuture {
    type Output = Result<Option<Row>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        match &mut this.inner {
            Ok(f) => Pin::new(f).poll(cx).map(|r| match r {
                Ok(Some(mut buf)) => {
                    buf.key = std::mem::take(&mut this.key);
                    Ok(Some(Row::new(buf)))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(e.into()),
            }),
            Err(e) => Poll::Ready(Err(e.take().unwrap_or_else(polled_after_done))),
        }
    }
}

fn polled_after_done() -> Error {
    Error::new(
        crate::ErrorCode::InvalidArgument,
        "a read future polled after it resolved",
    )
}

/// A scan as a [`Stream`](futures_core::Stream) of owned [`Row`]s ([`Scan::stream`](crate::Scan::stream)).
/// Borrows its table as a lifetime, as [`RowIter`](crate::RowIter) does. Dropping it is
/// always safe.
#[must_use = "a stream does nothing unless polled"]
pub struct RowStream<'t> {
    inner: std::result::Result<(ScanCursor, RowBuf), Option<Error>>,
    done: bool,
    _table: std::marker::PhantomData<&'t crate::Table>,
}

impl std::fmt::Debug for RowStream<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowStream")
            .field("done", &self.done)
            .finish()
    }
}

impl RowStream<'_> {
    pub(crate) fn new(started: Result<(ScanCursor, RowBuf)>, limit_zero: bool) -> Self {
        Self {
            inner: started.map_err(Some),
            done: limit_zero,
            _table: std::marker::PhantomData,
        }
    }
}

impl futures_core::Stream for RowStream<'_> {
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.done {
            return Poll::Ready(None);
        }
        let (cursor, buf) = match &mut this.inner {
            Ok(parts) => parts,
            Err(e) => {
                this.done = true;
                return Poll::Ready(e.take().map(Err));
            }
        };
        let item = match cursor.poll_next_row(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Ok(false)) => None,
            Poll::Ready(Ok(true)) => match cursor.counted(|c| crate::read::fill_row(c, buf)) {
                Ok(()) => {
                    let next = buf.empty_like();
                    Some(Ok(Row::new(std::mem::replace(buf, next))))
                }
                Err(e) => Some(Err(e)),
            },
            Poll::Ready(Err(e)) => Some(Err(e.into())),
        };
        if !matches!(item, Some(Ok(_))) {
            this.done = true;
        }
        Poll::Ready(item)
    }
}
