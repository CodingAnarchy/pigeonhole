//! A cluster partition's gate (D317 ticket 6, the write side of rule 3, and D326's lease):
//! P serves only while it holds a lease from a majority of the map's nodes, and takes
//! writes only while it also isn't fenced by the operator. [`WriteGate::drained`] returns
//! once every write admitted before a fence has finished.
//!
//! An in-flight count, a flag, the lease's end and a [`Notify`] (cli692's suggestion on the
//! plan), not a lock: a write's admission is two atomic operations on the partition's gate,
//! a clock read and one more load, and one atomic operation on its release. A write
//! increments the count, then reads the flag; a fence sets the flag, then reads the count.
//! Both `SeqCst`, so at least one of them sees the other: a write the fence didn't wait for
//! saw the flag and answered `Fenced` without writing.
//!
//! **The lease (D326):** `lease_end` is a time on the node's [`LeaseClock`], which counts
//! time the machine was suspended. A renewal raises it; a map that moves the partition away
//! clears it. Nothing fires when it passes: every check reads the clock. A write is checked
//! at admission and again once its commit returns ([`Held::committed`]): one that finished
//! past the lease's end answers `OutcomeUnknown`, since whether anyone reads it depends on
//! whether this node gets the lease back. A read is checked once it has taken its snapshot.
//!
//! Lock order: a write takes the gate after the service's own admission (scope, size, the
//! in-flight budget), so a fence's drain never waits behind a write still waiting for
//! budget, and holds it until its commit has finished, past its reply when the write runs
//! detached from its request.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

use super::errors::ApiError;

/// A node's lease clock (D326): nanoseconds on a clock that counts time the machine was
/// suspended ([`pigeonhole_io::sys::boot_clock_ns`]), shared by the node's gates and its
/// grants; and whether the node's last lease round reached a majority, which decides what a
/// partition without a lease does with its feeds (feed1248's F2). Tests move one node's
/// clock on with a seam of their own, so nodes in the same process don't share a skew.
#[derive(Debug)]
pub(crate) struct LeaseClock {
    /// Added to the clock (tests): a suspend, or a pause between a write's admission and
    /// its commit.
    #[cfg(test)]
    skew: AtomicU64,
    /// The node's last lease round reached a majority of its map's nodes (true until one
    /// didn't).
    majority: std::sync::atomic::AtomicBool,
}

impl Default for LeaseClock {
    fn default() -> Self {
        Self {
            #[cfg(test)]
            skew: AtomicU64::new(0),
            majority: std::sync::atomic::AtomicBool::new(true),
        }
    }
}

impl LeaseClock {
    /// Records whether the node's last lease round reached a majority.
    pub(crate) fn set_majority(&self, reached: bool) {
        self.majority.store(reached, Ordering::SeqCst);
    }

    /// Whether the node's last lease round reached a majority.
    pub(crate) fn majority(&self) -> bool {
        self.majority.load(Ordering::SeqCst)
    }

    /// Now, in nanoseconds since an arbitrary start.
    #[inline]
    pub(crate) fn now(&self) -> u64 {
        let now = pigeonhole_io::sys::boot_clock_ns();
        #[cfg(test)]
        let now = now.saturating_add(self.skew.load(Ordering::SeqCst));
        now
    }

    /// Moves this clock on by `by` (tests).
    #[cfg(test)]
    pub(crate) fn advance(&self, by: Duration) {
        let ns = u64::try_from(by.as_nanos()).unwrap_or(u64::MAX);
        self.skew.fetch_add(ns, Ordering::SeqCst);
    }
}

/// Why a partition is fenced, as a write's refusal and `Status` report it: the operator's
/// fence when it's set, else the missing lease. The gate keeps the two apart (#1829); this
/// is their reading, the operator's first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fence {
    /// `phdb cluster fence`: kept in `fenced.json` across a restart, lifted only by
    /// `phdb cluster unfence`.
    Operator,
    /// No lease since it opened here: a majority of the map's nodes have yet to grant it
    /// (D326, as rule 3's confirmation was).
    Unconfirmed,
    /// Its lease ran out: its renewals stopped reaching a majority (D326).
    Lapsed,
}

/// What a partition's feed components do now (feed1248's F2, D326 PR 2).
#[derive(Debug)]
pub(crate) enum Delivery {
    /// It holds a lease: they deliver.
    Serve,
    /// It has held none since it opened here, and the node's last round reached a majority
    /// (the peers answered, granting or withholding with time left: a move's new node before
    /// its lease, or one waiting out an old owner's grants). They wait.
    Wait,
    /// Its lease lapsed, or the node's last round didn't reach a majority (a node cut off,
    /// restarted or not): they end, and new subscribes are refused, with this.
    End(ApiError),
}

/// No operator's fence.
const OPEN: u8 = 0;
/// The operator's fence (`phdb cluster fence`, `fenced.json`).
const OPERATOR: u8 = 1;

/// One partition's gate, shared by its service and the node's fences.
#[derive(Debug)]
pub(crate) struct WriteGate {
    /// The partition's id, for the refusal's message.
    partition: String,
    /// Writes admitted and not yet finished.
    inflight: AtomicUsize,
    /// `OPERATOR`, set or clear.
    fence: AtomicU8,
    /// When the lease ends, on `clock`: 0 when the partition has held none since it opened
    /// here (or the map moved it away). It serves only while the clock is before it.
    lease_end: AtomicU64,
    /// Whether it has held a lease since it opened here: a lapsed lease and one never held
    /// read differently in `Status`.
    leased_once: std::sync::atomic::AtomicBool,
    /// The node's lease clock.
    clock: Arc<LeaseClock>,
    /// Woken when the last write finishes while fenced.
    drained: Notify,
}

impl WriteGate {
    /// A cluster partition's gate as it opens on a node: with no lease until a majority of
    /// the map's nodes grant one (D326), and fenced by the operator as well when `operator`
    /// (`fenced.json` lists it). `clock` is the node's lease clock.
    pub(crate) fn opened(partition: &str, operator: bool, clock: Arc<LeaseClock>) -> Arc<Self> {
        Arc::new(Self {
            partition: partition.to_owned(),
            inflight: AtomicUsize::new(0),
            fence: AtomicU8::new(if operator { OPERATOR } else { OPEN }),
            lease_end: AtomicU64::new(0),
            leased_once: std::sync::atomic::AtomicBool::new(false),
            clock,
            drained: Notify::new(),
        })
    }

    /// Admits a write, or answers `Fenced`: the operator's fence, or no live lease. The
    /// returned hold keeps the write counted until it's dropped; check it again with
    /// [`Held::committed`] once the commit returns.
    #[inline]
    pub(crate) fn enter(self: &Arc<Self>) -> Result<Held, ApiError> {
        self.inflight.fetch_add(1, Ordering::SeqCst);
        let held = Held(Arc::clone(self));
        match self.fenced() {
            None => Ok(held),
            Some(why) => {
                drop(held);
                Err(fenced(&self.partition, why))
            }
        }
    }

    /// Whether the partition holds a live lease now.
    #[inline]
    pub(crate) fn leased(&self) -> bool {
        self.clock.now() < self.lease_end.load(Ordering::SeqCst)
    }

    /// A read's check, once it has taken its snapshot (audit120's S3): `PartitionUnavailable`
    /// without a live lease, so the client's router tries again where the map now puts it.
    #[inline]
    pub(crate) fn serving(&self) -> Result<(), ApiError> {
        if self.leased() {
            Ok(())
        } else {
            Err(unleased(&self.partition, self.leased_once()))
        }
    }

    /// The partition's id.
    pub(crate) fn partition(&self) -> &str {
        &self.partition
    }

    /// Why the partition takes no writes, `None` when it does: the operator's fence first,
    /// then the missing lease.
    pub(crate) fn fenced(&self) -> Option<Fence> {
        if self.fence.load(Ordering::SeqCst) & OPERATOR != 0 {
            Some(Fence::Operator)
        } else if self.leased() {
            None
        } else if self.leased_once() {
            Some(Fence::Lapsed)
        } else {
            Some(Fence::Unconfirmed)
        }
    }

    /// Whether the operator's fence is set.
    pub(crate) fn operator_fenced(&self) -> bool {
        self.fence.load(Ordering::SeqCst) & OPERATOR != 0
    }

    /// Whether the partition holds no live lease, whatever the operator's fence says
    /// (`Status`'s `awaiting_peers`).
    pub(crate) fn awaiting_peers(&self) -> bool {
        !self.leased()
    }

    /// What its feed components do now ([`Delivery`]): one clock read and two loads.
    #[inline]
    pub(crate) fn delivery(&self) -> Delivery {
        if self.leased() {
            Delivery::Serve
        } else if self.leased_once() || !self.clock.majority() {
            Delivery::End(unleased(&self.partition, self.leased_once()))
        } else {
            Delivery::Wait
        }
    }

    /// Whether it held a lease since it opened here.
    fn leased_once(&self) -> bool {
        self.leased_once.load(Ordering::SeqCst)
    }

    /// The time left on the lease, zero without one.
    pub(crate) fn lease_remaining(&self) -> Duration {
        let end = self.lease_end.load(Ordering::SeqCst);
        Duration::from_nanos(end.saturating_sub(self.clock.now()))
    }

    /// Fences the partition for an operator: writes admitted from now answer `Fenced`. Its
    /// lease is untouched: its reads go on while it holds one.
    pub(crate) fn fence(&self) {
        self.fence.fetch_or(OPERATOR, Ordering::SeqCst);
    }

    /// A lease round's grant reached a majority: the lease runs until `end` on the node's
    /// clock, unless it already runs later. Whether the partition held no live lease
    /// before (it starts serving now).
    pub(crate) fn renew(&self, end: u64) -> bool {
        let was = self.leased();
        self.lease_end.fetch_max(end, Ordering::SeqCst);
        self.leased_once.store(true, Ordering::SeqCst);
        !was
    }

    /// The map no longer names this node for the partition at its file's epoch: its lease
    /// ends now (audit120's S1). It reads as never held, since no renewal can bring it back.
    pub(crate) fn lapse(&self) {
        self.lease_end.store(0, Ordering::SeqCst);
        self.leased_once.store(false, Ordering::SeqCst);
    }

    /// Lifts the operator's fence: an operator's `unfence`. The partition takes writes once
    /// it also holds a lease.
    pub(crate) fn unfence(&self) {
        self.fence.fetch_and(!OPERATOR, Ordering::SeqCst);
    }

    /// Another hold for a write [`enter`](Self::enter) admitted and that's still held: the
    /// task a write continues in after its request (a detached write, a commit, a bulk
    /// load's thread). Never refused, since the write was admitted; only sound while that
    /// admission's hold is alive, which keeps a fence's drain from finishing meanwhile.
    pub(crate) fn extend(self: &Arc<Self>) -> Held {
        self.inflight.fetch_add(1, Ordering::SeqCst);
        Held(Arc::clone(self))
    }

    /// Counts work that runs whatever the fence (the schema reconcile, which a missing lease
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
    /// count still running when `wait` ends first, or when the operator's fence was lifted
    /// meanwhile (count 0 then means only that).
    pub(crate) async fn drained(&self, wait: Duration) -> Result<(), usize> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let woken = self.drained.notified();
            tokio::pin!(woken);
            // Registered before the count is read, so a release in between wakes it.
            woken.as_mut().enable();
            if !self.operator_fenced() {
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

impl Held {
    /// The write's check once its commit returned (D326): `OutcomeUnknown` when the lease
    /// ended before it did. The write is in this node's file, but whether anyone reads it
    /// depends on whether this node gets the lease back: a pause between admission and
    /// commit (a stalled fsync, a suspend) can't be bounded by any margin at admission.
    #[inline]
    pub(crate) fn committed(&self) -> Result<(), ApiError> {
        if self.0.leased() {
            Ok(())
        } else {
            Err(ApiError::outcome_unknown(format!(
                "partition {:?}'s lease on this node ended before its commit was acknowledged: \
                 the write may or may not be kept. Read it back from the partition's node \
                 before you retry",
                self.0.partition
            )))
        }
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        // Only a fence's drain waits (`drained`), and it waits only behind the operator's
        // fence.
        if self.0.inflight.fetch_sub(1, Ordering::SeqCst) == 1 && self.0.operator_fenced() {
            self.0.drained.notify_waiters();
        }
    }
}

/// The `Fenced` answer to a write on `partition`.
fn fenced(partition: &str, why: Fence) -> ApiError {
    ApiError::fenced(match why {
        Fence::Operator => format!(
            "partition {partition:?} is fenced on this node (phdb cluster fence): it takes no \
             writes until it's unfenced or relocated; nothing was written"
        ),
        Fence::Unconfirmed => format!(
            "partition {partition:?} has no lease on this node yet: a majority of the cluster \
             map's nodes have yet to grant it (it opened here since they last did); nothing \
             was written"
        ),
        Fence::Lapsed => format!(
            "partition {partition:?}'s lease on this node ran out: its renewals stopped \
             reaching a majority of the cluster map's nodes; nothing was written"
        ),
    })
}

/// The `PartitionUnavailable` answer to a read on `partition` without a lease.
fn unleased(partition: &str, once: bool) -> ApiError {
    ApiError::partition_unavailable(if once {
        format!(
            "partition {partition:?}'s lease on this node ran out: its renewals stopped \
             reaching a majority of the cluster map's nodes, so it serves no reads"
        )
    } else {
        format!(
            "partition {partition:?} has no lease on this node yet: a majority of the cluster \
             map's nodes have yet to grant it, so it serves no reads"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A gate with a lease for an hour.
    fn open(partition: &str) -> Arc<WriteGate> {
        let clock = Arc::new(LeaseClock::default());
        let g = WriteGate::opened(partition, false, Arc::clone(&clock));
        assert!(g.renew(clock.now() + 3_600_000_000_000));
        g
    }

    #[test]
    fn an_open_gate_admits_and_a_fenced_one_refuses() {
        let g = open("p1");
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
    fn the_operators_fence_and_the_lease_are_separate() {
        // #1829, as D326 has it: an operator-fenced partition that opens has no lease
        // either, and each clears on its own: a lease leaves the operator's fence, and the
        // gate admits writes only with a lease and no operator's fence.
        let clock = Arc::new(LeaseClock::default());
        let hour = 3_600_000_000_000;
        let g = WriteGate::opened("p1", true, Arc::clone(&clock));
        assert_eq!(g.fenced(), Some(Fence::Operator));
        assert!(g.awaiting_peers());
        assert_eq!(g.serving().unwrap_err().code, "PartitionUnavailable");
        assert!(g.renew(clock.now() + hour), "it had none");
        assert!(!g.renew(clock.now() + hour), "a renewal");
        assert!(!g.awaiting_peers());
        g.serving().unwrap();
        assert_eq!(
            g.fenced(),
            Some(Fence::Operator),
            "the operator's fence stays"
        );
        assert_eq!(g.enter().unwrap_err().code, "Fenced");
        g.unfence();
        assert_eq!(g.fenced(), None);
        drop(g.enter().unwrap());

        // No lease alone: a grant opens it.
        let g = WriteGate::opened("p1", false, Arc::clone(&clock));
        assert_eq!(g.fenced(), Some(Fence::Unconfirmed));
        assert!(g.enter().unwrap_err().message.contains("no lease"));
        // An operator's fence on top reads as the operator's, and leaves the lease missing.
        g.fence();
        assert_eq!(g.fenced(), Some(Fence::Operator));
        assert!(g.awaiting_peers());
        // An operator's unfence lifts only its fence: never a lease (D326, the owner's call
        // on `unfence --force`).
        g.unfence();
        assert_eq!(g.fenced(), Some(Fence::Unconfirmed));
    }

    #[test]
    fn a_lease_runs_out_on_the_clock_and_a_write_past_it_is_outcome_unknown() {
        // D326: the lease ends with no timer, on the node's clock. A write admitted under
        // it whose commit returns past its end answers `OutcomeUnknown` (a suspend or a
        // stalled fsync between the two); one admitted after answers `Fenced`, lapsed; a
        // read answers `PartitionUnavailable`.
        let clock = Arc::new(LeaseClock::default());
        let g = WriteGate::opened("p1", false, Arc::clone(&clock));
        g.renew(clock.now() + 1_000_000_000);
        let held = g.enter().unwrap();
        held.committed().unwrap();
        assert!(g.lease_remaining() > Duration::from_millis(900));
        clock.advance(Duration::from_secs(2));
        let e = held.committed().unwrap_err();
        assert_eq!(e.code, "OutcomeUnknown", "{}", e.message);
        drop(held);
        assert_eq!(g.fenced(), Some(Fence::Lapsed));
        assert!(g.enter().unwrap_err().message.contains("ran out"));
        assert_eq!(g.serving().unwrap_err().code, "PartitionUnavailable");
        assert_eq!(g.lease_remaining(), Duration::ZERO);
        // A renewal brings it back; a map that moves it away ends it, as never held.
        g.renew(clock.now() + 1_000_000_000);
        g.serving().unwrap();
        g.lapse();
        assert_eq!(g.fenced(), Some(Fence::Unconfirmed));
        // A renewal never shortens a lease.
        g.renew(clock.now() + 1_000_000_000);
        g.renew(clock.now() + 10);
        assert!(g.lease_remaining() > Duration::from_millis(900));
    }

    #[tokio::test]
    async fn drained_waits_for_writes_admitted_before_the_fence_and_their_extensions() {
        let g = open("p1");
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
            let g = open("p1");
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
