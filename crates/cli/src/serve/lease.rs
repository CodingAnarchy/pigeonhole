//! A cluster partition's stream delivery under its lease (D326 PR 2, feed1248's F2, F3 and
//! F5): a feed component's ([`super::feed::FeedEvents`]) or a memory watch's
//! ([`super::memory::MemoryEvents`], #1915), delivered only while the partition holds a
//! lease. Lifted out of the feed's events for the memory watch (#1915 PR 2b): one check for
//! both.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use super::errors::ApiError;
use super::gate::{Delivery, WriteGate};

/// A partition's stream's lease check. Every stream of the partition on this node reads
/// through one of these, so the check is one clock read per event delivered, and nothing on
/// a single node.
pub(crate) struct DeliveryLease {
    gate: Arc<WriteGate>,
    /// Wakes the stream at the lease's end while it has nothing to deliver, or for its next
    /// look while it waits for a lease: a quiet partition's streams still end on time.
    /// Tokio's timer doesn't count a suspend and the lease clock does, so after one the timer
    /// may fire late; that's safe, since every poll checks the lease itself.
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
    /// It ended with the lease's error.
    ended: bool,
}

/// How often a stream waiting for its partition's first lease looks again.
const LEASE_WAIT: Duration = Duration::from_millis(100);

/// What a leased stream gives its lease check (simp724's M1 and M2 on #1915 PR 2b).
pub(crate) trait LeaseHooks {
    /// The stream's own end, if it has one now, without taking its events.
    fn poll_own_end(&mut self, cx: &mut Context<'_>) -> Poll<Option<ApiError>>;
    /// Stamps the stream's take clock: a look while it waits for a first lease counts as a
    /// take. Only that: the lease check reads no take. A batch of events the connection
    /// holds under back-pressure is stamped at its first and last event (and at each while a
    /// send waits for room), so a stamp at a batch's first event never stands for its rest
    /// (simp724 on #1737's F3-0).
    fn took(&self);
}

/// What a stream does on this poll, by its lease alone.
enum Look {
    /// It holds a lease: deliver.
    Deliver,
    /// It ended already: nothing more.
    Ended,
    /// No lease yet: wait.
    Wait,
    /// The lease lapsed, or the node can't reach a majority: end with this.
    End(ApiError),
}

impl DeliveryLease {
    /// A check of `gate`'s lease. Refused at once where it lapsed or the node doesn't reach
    /// a majority, so a stream never starts there; one that has had no lease yet waits for
    /// it (feed1248's F2).
    pub(crate) fn new(gate: Arc<WriteGate>) -> Result<Self, ApiError> {
        if let Delivery::End(e) = gate.delivery() {
            return Err(e);
        }
        Ok(Self {
            gate,
            timer: None,
            ended: false,
        })
    }

    /// The check before a poll of the stream's events: `None` to poll them, or what the
    /// poll answers instead. The lease is read once per poll: one clock read per event
    /// (feed1248's N1). Without a lease, the stream's [`LeaseHooks`] apply:
    /// - `poll_own_end`, the stream's own end, comes through first. A map that moves the
    ///   partition away lapses its lease and ends its streams with `WrongPartition` at the
    ///   same moment, and the client must see that, not the lease's `PartitionUnavailable`
    ///   (audit120's S-C5; simp724's M1 on #1915 PR 2b). Events not yet delivered go; a
    ///   resume continues from the last one delivered.
    /// - `took` stamps the stream's take clock on each look while it waits for a first
    ///   lease, so an idle limit that counts from the client's last take (`--stream-idle-ms`)
    ///   doesn't end it for a wait the server imposes (feed1248's S1; simp724's M2). A client
    ///   that went away still ends it: its channel closes.
    pub(crate) fn before<T>(
        &mut self,
        cx: &mut Context<'_>,
        stream: &mut impl LeaseHooks,
    ) -> Option<Poll<Option<Result<T, ApiError>>>> {
        let lease_end = match self.look() {
            Look::Deliver => return None,
            Look::Ended => return Some(Poll::Ready(None)),
            Look::Wait => None,
            Look::End(e) => Some(e),
        };
        let end = match stream.poll_own_end(cx) {
            Poll::Ready(Some(e)) => Some(e),
            _ => lease_end,
        };
        if let Some(e) = end {
            self.ended = true;
            return Some(Poll::Ready(Some(Err(e))));
        }
        stream.took();
        self.wake_at(tokio::time::Instant::now() + LEASE_WAIT, cx);
        Some(Poll::Pending)
    }

    /// After a poll of the stream's events: with nothing to deliver, woken at the lease's
    /// end, so the stream ends then even with no event to carry it (feed1248's F3). A
    /// renewal before then only wakes it early.
    pub(crate) fn after(&mut self, pending: bool, cx: &mut Context<'_>) {
        if pending {
            let left = self.gate.lease_remaining().max(Duration::from_millis(1));
            self.wake_at(tokio::time::Instant::now() + left, cx);
        }
    }

    fn look(&self) -> Look {
        if self.ended {
            return Look::Ended;
        }
        match self.gate.delivery() {
            Delivery::Serve => Look::Deliver,
            Delivery::Wait => Look::Wait,
            Delivery::End(e) => Look::End(e),
        }
    }

    /// Wakes `cx` at `deadline`, or at once if that's passed. The timer is reset only when
    /// it has fired or its deadline moves by more than a millisecond (feed1248's N2): a
    /// lease's end stays put between renewals, so most polls only register the waker.
    fn wake_at(&mut self, deadline: tokio::time::Instant, cx: &mut Context<'_>) {
        let timer = self
            .timer
            .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(deadline)));
        let at = timer.deadline();
        let moved = at
            .checked_duration_since(deadline)
            .or_else(|| deadline.checked_duration_since(at))
            .is_some_and(|d| d > Duration::from_millis(1));
        if timer.is_elapsed() || moved {
            timer.as_mut().reset(deadline);
        }
        if timer.as_mut().poll(cx).is_ready() {
            cx.waker().wake_by_ref();
        }
    }
}
