//! One shard's published load and its slot reservations (#97, #103, #128; #134 step 3:
//! loom-modelled).
//!
//! - **Load publication.** The owning shard stores its write rate and busy fraction (D217),
//!   then the time it
//!   published it with `Release` ([`LoadSlot::publish`]). A shard balancing reads the time
//!   with `Acquire` and, when it is fresh, the rate ([`LoadSlot::read_fresh`]): what it
//!   reads is that publication's rate or a later one, never an older one.
//! - **Slot reservations.** A tablet change that will hand a shard tablets reserves the
//!   `(tablet, family)` slots on it first, with an atomic add ([`LoadSlot::reserve`])
//!   that returns what was reserved before, and gives them back if that would pass the
//!   limit ([`LoadSlot::release`]). So concurrent changes never commit a shard past
//!   `max_slots` together. The caller reserves on every receiving shard, all or none.
//! - **Skew claims (D223).** A shard handing another load for write skew claims it first
//!   ([`LoadSlot::claim`], a compare-and-swap), and the claim holds until the receiver has
//!   published a dwell's worth (10) more balancer passes. Two shards balancing at once
//!   never both shed into the same cold shard: the second sees the claim and picks another.

#[cfg(engine_loom)]
use loom::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
#[cfg(not(engine_loom))]
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// One shard's load as of its last balancer interval, read by every other shard.
#[derive(Debug, Default)]
#[repr(align(64))]
pub(crate) struct LoadSlot {
    /// Moving average of the rows written per interval.
    writes: AtomicU64,
    /// Moving average of the fraction of the time the shard was busy, in millionths (D217).
    busy: AtomicU64,
    /// `(tablet, family)` slots promised to changes in flight that will hand this shard
    /// tablets: every change checks them with the slots in the view, so concurrent moves
    /// never take a shard past `max_slots` (#103).
    reserved: AtomicUsize,
    /// When it was published (monotonic nanoseconds; 0 = never).
    at: AtomicU64,
    /// The owning shard's balancer passes as of its last publication.
    passes: AtomicU64,
    /// The pass (of the owning shard, as `passes`) until which a skew change handing it load
    /// holds it (D223): until then, no other skew change picks it.
    claimed: AtomicU64,
}

impl LoadSlot {
    /// The owning shard publishes its write rate and busy fraction (millionths, D217) as of
    /// `now` (nonzero).
    pub(crate) fn publish(&self, writes: u64, busy: u64, now: u64, passes: u64) {
        self.writes.store(writes, Ordering::Relaxed);
        self.busy.store(busy, Ordering::Relaxed);
        self.passes.store(passes, Ordering::Relaxed);
        self.at.store(now, Ordering::Release);
    }

    /// The published rate if it was published within `stale` of `now`.
    pub(crate) fn read_fresh(&self, now: u64, stale: u64) -> Option<u64> {
        let at = self.at.load(Ordering::Acquire);
        (at != 0 && now.saturating_sub(at) <= stale).then(|| self.writes.load(Ordering::Relaxed))
    }

    /// The published busy fraction (millionths) if it was published within `stale` of
    /// `now`: like [`LoadSlot::read_fresh`], that publication's or a later one.
    pub(crate) fn read_busy_fresh(&self, now: u64, stale: u64) -> Option<u64> {
        let at = self.at.load(Ordering::Acquire);
        (at != 0 && now.saturating_sub(at) <= stale).then(|| self.busy.load(Ordering::Relaxed))
    }

    /// Whether no skew change holds this shard (D223): a balancer may pick it.
    pub(crate) fn claimable(&self) -> bool {
        self.passes.load(Ordering::Relaxed) >= self.claimed.load(Ordering::Acquire)
    }

    /// Claims this shard for a skew change handing it load, until it has published `dwell`
    /// more passes. Fails while another claim holds it, or when one lands first.
    pub(crate) fn claim(&self, dwell: u64) -> bool {
        let passes = self.passes.load(Ordering::Relaxed);
        let held = self.claimed.load(Ordering::Acquire);
        passes >= held
            && self
                .claimed
                .compare_exchange(held, passes + dwell, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }

    /// Slots reserved on this shard by changes in flight.
    pub(crate) fn reserved(&self) -> usize {
        self.reserved.load(Ordering::Acquire)
    }

    /// Reserves `n` slots; returns what was reserved before. The caller releases them
    /// if that leaves no room.
    pub(crate) fn reserve(&self, n: usize) -> usize {
        self.reserved.fetch_add(n, Ordering::AcqRel)
    }

    /// Gives back `n` reserved slots.
    pub(crate) fn release(&self, n: usize) {
        self.reserved.fetch_sub(n, Ordering::AcqRel);
    }
}

#[cfg(all(test, engine_loom))]
mod loom_tests {
    use super::LoadSlot;
    use loom::sync::Arc;
    use loom::thread;

    #[test]
    fn loom_a_fresh_load_is_never_older_than_its_publication() {
        // The shard publishes twice while another shard balancing reads: a publication time
        // it reads comes with that publication's rate or a later one, never the rate from
        // before it.
        loom::model(|| {
            let slot = Arc::new(LoadSlot::default());
            let owner = {
                let slot = Arc::clone(&slot);
                thread::spawn(move || {
                    slot.publish(10, 0, 1, 1);
                    slot.publish(20, 0, 2, 2);
                })
            };
            for _ in 0..2 {
                // A zero window reads only the publication made at exactly `now`.
                if let Some(w) = slot.read_fresh(1, 0) {
                    assert!(w == 10 || w == 20, "rate {w} older than its publication");
                }
                if let Some(w) = slot.read_fresh(2, 0) {
                    assert_eq!(w, 20, "an older rate under a newer time");
                }
            }
            owner.join().unwrap();
            assert_eq!(slot.read_fresh(2, 10), Some(20));
        });
    }

    /// `ShardState::reserve_slots`: reserves on each shard in turn, and gives everything
    /// back if one has no room (`room` slots each, none in the view).
    fn reserve_all(slots: &[Arc<LoadSlot>; 2], order: [usize; 2], room: usize) -> bool {
        let mut held = Vec::new();
        for i in order {
            let before = slots[i].reserve(1);
            held.push(i);
            if before + 1 > room {
                for &j in &held {
                    slots[j].release(1);
                }
                return false;
            }
        }
        true
    }

    #[test]
    fn loom_concurrent_changes_never_pass_the_slot_limit() {
        // Two tablet changes each reserve one slot on both shards, in opposite orders, with
        // room for one each: at most one succeeds, and the counters end at what the
        // successful one holds (a refused change gives back all it reserved).
        loom::model(|| {
            let slots = Arc::new([Arc::new(LoadSlot::default()), Arc::new(LoadSlot::default())]);
            let a = {
                let slots = Arc::clone(&slots);
                thread::spawn(move || reserve_all(&slots, [0, 1], 1))
            };
            let b = reserve_all(&slots, [1, 0], 1);
            let a = a.join().unwrap();
            assert!(!(a && b), "both changes took the last slot");
            let held = usize::from(a || b);
            for s in slots.iter() {
                assert_eq!(s.reserved(), held, "a refused change kept a reservation");
            }
        });
    }

    #[test]
    fn loom_two_skew_changes_never_both_claim_one_shard() {
        // Two shards balancing at once claim the same cold shard while it publishes a pass:
        // at most one claim holds it until it has made a dwell of passes.
        loom::model(|| {
            let slot = Arc::new(LoadSlot::default());
            let claimers: Vec<_> = (0..2)
                .map(|_| {
                    let slot = Arc::clone(&slot);
                    thread::spawn(move || slot.claim(10))
                })
                .collect();
            slot.publish(0, 0, 1, 1);
            let won = claimers
                .into_iter()
                .map(|t| t.join().unwrap())
                .filter(|c| *c)
                .count();
            assert_eq!(won, 1, "{won} claims hold one shard");
            assert!(!slot.claimable());
        });
    }
}

#[cfg(all(test, not(engine_loom)))]
mod tests {
    use super::LoadSlot;

    #[test]
    fn a_publication_is_read_while_fresh_and_reservations_count() {
        let slot = LoadSlot::default();
        assert_eq!(slot.read_fresh(5, 10), None, "never published");
        slot.publish(7, 0, 5, 1);
        assert_eq!(slot.read_fresh(10, 10), Some(7));
        assert_eq!(slot.read_fresh(20, 10), None, "stale");
        assert_eq!(slot.reserve(2), 0);
        assert_eq!(slot.reserve(1), 2);
        slot.release(2);
        assert_eq!(slot.reserved(), 1);
    }

    #[test]
    fn a_claim_holds_a_shard_for_a_dwell_of_its_passes() {
        let slot = LoadSlot::default();
        slot.publish(7, 0, 5, 3);
        assert!(slot.claimable());
        assert!(slot.claim(10));
        assert!(!slot.claimable() && !slot.claim(10), "held until pass 13");
        slot.publish(7, 0, 6, 12);
        assert!(!slot.claimable());
        slot.publish(7, 0, 7, 13);
        assert!(slot.claimable() && slot.claim(10));
    }
}
