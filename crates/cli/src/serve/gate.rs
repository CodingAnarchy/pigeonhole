//! A cluster partition's write gate (D317 ticket 6, the write side of rule 3): while P is
//! fenced, its writes answer `Fenced`, and [`WriteGate::drained`] returns once every write
//! admitted before the fence has finished.
//!
//! An in-flight count, a flag and a [`Notify`] (cli692's suggestion on the plan), not a
//! lock: a write's admission is two atomic operations on the partition's gate and one on
//! its release. A write increments the count, then reads the flag; a fence sets the flag,
//! then reads the count. Both `SeqCst`, so at least one of them sees the other: a write the
//! fence didn't wait for saw the flag and answered `Fenced` without writing.
//!
//! Lock order: a write takes the gate after the service's own admission (scope, size, the
//! in-flight budget), so a fence's drain never waits behind a write still waiting for
//! budget, and holds it until its commit has finished, past its reply when the write runs
//! detached from its request.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

use super::errors::ApiError;

/// Why a partition is fenced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fence {
    /// `phdb cluster fence`: kept in `fenced.json` across a restart, lifted only by
    /// `phdb cluster unfence`.
    Operator,
    /// Opened at the node's start, waiting for a peer's map that still names this node for
    /// the partition at its file's epoch (auto-unfence).
    Start,
}

const OPEN: u8 = 0;
const OPERATOR: u8 = 1;
const START: u8 = 2;

/// One partition's gate, shared by its service and the node's fences.
#[derive(Debug)]
pub(crate) struct WriteGate {
    /// The partition's id, for the refusal's message.
    partition: String,
    /// Writes admitted and not yet finished.
    inflight: AtomicUsize,
    /// `OPEN`, or why it's fenced.
    fence: AtomicU8,
    /// Woken when the last write finishes while fenced.
    drained: Notify,
}

impl WriteGate {
    pub(crate) fn new(partition: &str, fence: Option<Fence>) -> Arc<Self> {
        Arc::new(Self {
            partition: partition.to_owned(),
            inflight: AtomicUsize::new(0),
            fence: AtomicU8::new(encode(fence)),
            drained: Notify::new(),
        })
    }

    /// Admits a write, or answers `Fenced`. The returned hold keeps the write counted until
    /// it's dropped.
    pub(crate) fn enter(self: &Arc<Self>) -> Result<Held, ApiError> {
        self.inflight.fetch_add(1, Ordering::SeqCst);
        let held = Held(Arc::clone(self));
        match decode(self.fence.load(Ordering::SeqCst)) {
            None => Ok(held),
            Some(why) => {
                drop(held);
                Err(fenced(&self.partition, why))
            }
        }
    }

    /// The partition's id.
    pub(crate) fn partition(&self) -> &str {
        &self.partition
    }

    /// Why the partition is fenced, `None` when it isn't.
    pub(crate) fn fenced(&self) -> Option<Fence> {
        decode(self.fence.load(Ordering::SeqCst))
    }

    /// Fences the partition for an operator: writes admitted from now answer `Fenced`. It
    /// replaces a start fence, which a gate only ever has from [`WriteGate::new`].
    pub(crate) fn fence(&self) {
        self.fence.store(OPERATOR, Ordering::SeqCst);
    }

    /// Lifts the fence, whatever its reason.
    pub(crate) fn unfence(&self) {
        self.fence.store(OPEN, Ordering::SeqCst);
    }

    /// Lifts a start fence only: an operator's stays.
    pub(crate) fn unfence_start(&self) -> bool {
        self.fence
            .compare_exchange(START, OPEN, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Another hold for a write [`enter`](Self::enter) admitted and that's still held: the
    /// task a write continues in after its request (a detached write, a commit, a bulk
    /// load's thread). Never refused, since the write was admitted; only sound while that
    /// admission's hold is alive, which keeps a fence's drain from finishing meanwhile.
    pub(crate) fn extend(self: &Arc<Self>) -> Held {
        self.inflight.fetch_add(1, Ordering::SeqCst);
        Held(Arc::clone(self))
    }

    /// Counts work that runs whatever the fence (the schema reconcile, which a start fence
    /// must not stop), then reads the fence: `enter`'s order, so a fence's drain either waits
    /// for it or it sees the fence. The caller decides from the fence whether to go on.
    pub(crate) fn hold(self: &Arc<Self>) -> (Held, Option<Fence>) {
        self.inflight.fetch_add(1, Ordering::SeqCst);
        let held = Held(Arc::clone(self));
        (held, self.fenced())
    }

    /// Writes admitted and not yet finished.
    pub(crate) fn inflight(&self) -> usize {
        self.inflight.load(Ordering::SeqCst)
    }

    /// Waits, at most `wait`, until no admitted write is running: after
    /// [`fence`](Self::fence), every write admitted before it has finished. `Err` with the
    /// count still running when `wait` ends first, or when the fence was lifted meanwhile
    /// (count 0 then means only that).
    pub(crate) async fn drained(&self, wait: Duration) -> Result<(), usize> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let woken = self.drained.notified();
            tokio::pin!(woken);
            // Registered before the count is read, so a release in between wakes it.
            woken.as_mut().enable();
            if self.fenced().is_none() {
                return Err(self.inflight());
            }
            let n = self.inflight();
            if n == 0 {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, woken).await.is_err() {
                return Err(self.inflight());
            }
        }
    }
}

/// An admitted write, counted until dropped.
#[derive(Debug)]
pub(crate) struct Held(Arc<WriteGate>);

impl Drop for Held {
    fn drop(&mut self) {
        if self.0.inflight.fetch_sub(1, Ordering::SeqCst) == 1
            && self.0.fence.load(Ordering::SeqCst) != OPEN
        {
            self.0.drained.notify_waiters();
        }
    }
}

fn encode(fence: Option<Fence>) -> u8 {
    match fence {
        None => OPEN,
        Some(Fence::Operator) => OPERATOR,
        Some(Fence::Start) => START,
    }
}

fn decode(v: u8) -> Option<Fence> {
    match v {
        OPERATOR => Some(Fence::Operator),
        START => Some(Fence::Start),
        _ => None,
    }
}

/// The `Fenced` answer to a write on `partition`.
fn fenced(partition: &str, why: Fence) -> ApiError {
    ApiError::fenced(match why {
        Fence::Operator => format!(
            "partition {partition:?} is fenced on this node (phdb cluster fence): it takes no \
             writes until it's unfenced or relocated; nothing was written"
        ),
        Fence::Start => format!(
            "partition {partition:?} is fenced while this node starts, until a peer's map \
             confirms it's still this node's; nothing was written"
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_open_gate_admits_and_a_fenced_one_refuses() {
        let g = WriteGate::new("p1", None);
        let held = g.enter().unwrap();
        assert_eq!(g.inflight(), 1);
        g.fence();
        let e = g.enter().unwrap_err();
        assert_eq!(e.code, "Fenced");
        // The refused write isn't counted.
        assert_eq!(g.inflight(), 1);
        drop(held);
        assert_eq!(g.inflight(), 0);
        g.unfence();
        drop(g.enter().unwrap());
    }

    #[test]
    fn a_start_fence_never_replaces_an_operators() {
        let g = WriteGate::new("p1", Some(Fence::Operator));
        assert!(!g.unfence_start());
        assert_eq!(g.fenced(), Some(Fence::Operator));
        let g = WriteGate::new("p1", Some(Fence::Start));
        g.fence();
        assert_eq!(g.fenced(), Some(Fence::Operator));
        let g = WriteGate::new("p1", Some(Fence::Start));
        assert!(g.unfence_start());
        assert_eq!(g.fenced(), None);
    }

    #[tokio::test]
    async fn drained_waits_for_writes_admitted_before_the_fence_and_their_extensions() {
        let g = WriteGate::new("p1", None);
        let held = g.enter().unwrap();
        let detached = g.extend();
        drop(held);
        g.fence();
        assert_eq!(
            g.drained(Duration::from_millis(20)).await,
            Err(1),
            "the detached write still runs"
        );
        let waiting = {
            let g = Arc::clone(&g);
            tokio::spawn(async move { g.drained(Duration::from_secs(30)).await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(detached);
        assert_eq!(waiting.await.unwrap(), Ok(()));
    }

    /// A write and a fence racing: either the write was admitted and the fence's drain
    /// waits for it, or it answered `Fenced`. Never admitted and missed.
    #[test]
    fn a_racing_write_is_either_waited_for_or_refused() {
        for _ in 0..2000 {
            let g = WriteGate::new("p1", None);
            let writer = {
                let g = Arc::clone(&g);
                std::thread::spawn(move || g.enter().ok())
            };
            g.fence();
            let seen = g.inflight();
            let held = writer.join().unwrap();
            if held.is_some() {
                // Admitted: it read the flag before the fence's store, so its increment
                // came before the fence's read of the count, which saw it.
                assert_eq!(seen, 1, "an admitted write the fence's read missed");
            }
        }
    }
}
