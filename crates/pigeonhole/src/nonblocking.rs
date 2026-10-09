//! The async front door (Phase 3, #42): the same operations as the sync API, over the same
//! engine, as futures. Built so far: commits. Gets, row reads and scan streams follow.
//!
//! Futures depend only on `std::task`, so they run on any executor (Tokio, smol, a custom
//! one); none of them spawns a thread or uses `spawn_blocking`. A commit future is woken by
//! the shard that applies it and by the watermark that makes it visible.
//!
//! **Commits.** `commit_async` and `commit_with_async` submit at the call, then resolve when
//! the record meets the requested durability and is visible, exactly when the sync `commit`
//! would return. Async and sync committers join the same commit groups. Dropping the future
//! after the call does **not** roll the commit back: it lands or fails atomically either
//! way. A [`CommitTicket`](crate::CommitTicket) is also a future (`ticket.await`).
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

use crate::write::{Submitted, TicketState};
use crate::{CommitInfo, CommitTicket, Error, Result, RowMutation, Transaction, WriteBatch};

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
