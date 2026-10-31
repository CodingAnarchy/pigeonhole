//! The engine's one seqno and default-timestamp allocator (D271). Every seqno a writer
//! reserves (a group's, a cross-shard commit's id, its decision's) comes from here, in the
//! writer's process, with the default timestamps that go with it, so a later seqno never
//! carries an earlier default timestamp on any shard. shm `NEXT_SEQNO` mirrors the seqnos
//! (`ShmRegion::advance_next_seqno`, a `fetch_max` after each reservation) for readers.
//!
//! `H` is the largest default timestamp handed out; `F` says the last reservation was a
//! cross-shard decision, so a group reserved in the same µs takes `H + 1` and cannot tie
//! with the decision's cells (D271, the owner's ruling on the same-µs tie).
//!
//! It also holds every decided cross-shard seqno until its last participant applied
//! (D278), published as shm `CROSS_HELD` under the same lock, so whichever shard thread
//! applies last releases it.

use std::collections::BTreeSet;
use std::sync::{Mutex, PoisonError};

use pigeonhole_format::{Seqno, Timestamp};
use pigeonhole_shm::ShmRegion;

#[derive(Debug, Default)]
struct State {
    /// The next seqno to reserve.
    next: Seqno,
    /// The largest default timestamp handed out.
    h: Timestamp,
    /// The next stamp must be strictly above `H`: the last reservation was a decision's, or a
    /// tablet changed owner since.
    decided: bool,
    /// The drift gauge: the largest lead of `H` over the clock at any reservation.
    max_lead: u64,
    /// Decided cross-shard seqnos not yet applied on every participant (D278).
    held: BTreeSet<Seqno>,
}

impl State {
    /// Publishes the lowest held seqno (`u64::MAX` when none) as shm `CROSS_HELD`: under
    /// the lock, so stores never land out of order.
    fn publish_held(&self, shm: &ShmRegion) {
        shm.publish_cross_held(self.held.first().copied().unwrap_or(u64::MAX));
    }
}

/// `(S, H, F)` and the decided seqnos held from readers, under one lock.
#[derive(Debug, Default)]
pub(crate) struct Reservation {
    state: Mutex<State>,
}

impl Reservation {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Starts at seqno `next` (at open, once replay has passed every recovered seqno), before
    /// anything at open reserves one.
    pub(crate) fn seed_seqno(&self, next: Seqno) {
        self.lock().next = next;
    }

    /// Raises `H` to `h` (at open: the manifest's floor and every replayed timestamp, not the
    /// clock: every reservation takes `now` into account itself, and a decision just after
    /// the reopen must not be pushed past it).
    pub(crate) fn seed_ts(&self, h: Timestamp) {
        let mut st = self.lock();
        st.h = st.h.max(h);
    }

    /// The next seqno to reserve: a lower bound for every later reservation (FORMAT §11.3's
    /// pending watermark), and the seqno ceiling the manifest records.
    pub(crate) fn next_seqno(&self) -> Seqno {
        self.lock().next
    }

    /// The largest default timestamp handed out (the manifest's `ts_floor`).
    pub(crate) fn last_ts(&self) -> Timestamp {
        self.lock().h
    }

    /// The drift gauge (D271, ICR 0064): how far `H` is ahead of the clock `now`, and the
    /// largest such lead at any reservation since open, in µs. One shard reserving more
    /// than one group a µs, or a decision with a group after it in the same µs, moves `H`
    /// past the clock.
    pub(crate) fn lead(&self, now: Timestamp) -> (u64, u64) {
        let st = self.lock();
        (st.h.saturating_sub(now), st.max_lead)
    }

    /// A group's `n` seqnos and the first of `k` default timestamps (`T0 .. T0 + k`): `T0`
    /// above `H` after a decision, and above this shard's own last stamp `last`, so a
    /// shard's stamps strictly increase (D271 variant B).
    ///
    /// The group's hold, `shard`'s pending watermark `min(watermark, first)`, is stored
    /// (Release) under the lock, before the mirror passes `first` (FORMAT §11.3, D300): every
    /// `fetch_max` past it comes after it, and `first`, the allocator's next seqno, is above
    /// every seqno a reader can see, so the store never lowers the visible seqno. `watermark`
    /// is the shard's lowest unapplied seqno before this group (`u64::MAX` when none).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reserve_group(
        &self,
        n: u64,
        k: u64,
        now: Timestamp,
        last: Timestamp,
        shm: &ShmRegion,
        shard: u32,
        watermark: Seqno,
    ) -> (Seqno, Timestamp) {
        let (first, t0) = {
            let mut st = self.lock();
            let first = st.next;
            shm.publish_pending(shard, watermark.min(first));
            st.next += n;
            let t0 = now.max(st.h + u64::from(st.decided)).max(last + 1);
            st.h = st.h.max(t0 + k.max(1) - 1);
            st.decided = false;
            st.max_lead = st.max_lead.max(st.h.saturating_sub(now));
            (first, t0)
        };
        shm.advance_next_seqno(first + n);
        (first, t0)
    }

    /// One seqno with no timestamp: a cross-shard commit's id (D263), never applied.
    pub(crate) fn reserve_id(&self, shm: &ShmRegion) -> Seqno {
        let first = {
            let mut st = self.lock();
            let first = st.next;
            st.next += 1;
            first
        };
        shm.advance_next_seqno(first + 1);
        first
    }

    /// A cross-shard decision's seqno and timestamp: strictly above every default timestamp
    /// handed out before it. The seqno is held from readers (shm `CROSS_HELD`) until
    /// [`release`](Self::release): published before the `NEXT_SEQNO` mirror passes it, so
    /// a reader that sees the mirror past it sees the hold too (FORMAT §11.3).
    pub(crate) fn reserve_decision(&self, now: Timestamp, shm: &ShmRegion) -> (Seqno, Timestamp) {
        let (s, t) = {
            let mut st = self.lock();
            let s = st.next;
            st.next += 1;
            let t = now.max(st.h + 1);
            st.h = t;
            st.decided = true;
            st.max_lead = st.max_lead.max(t - now);
            st.held.insert(s);
            st.publish_held(shm);
            (s, t)
        };
        shm.advance_next_seqno(s + 1);
        (s, t)
    }

    /// Every participant of the decision at `seqno` applied its share (D278): readers may see
    /// it. Called once per decision, from the shard thread that applied last.
    pub(crate) fn release(&self, seqno: Seqno, shm: &ShmRegion) {
        let mut st = self.lock();
        let held = st.held.remove(&seqno);
        debug_assert!(held, "decision {seqno} released twice or never held");
        st.publish_held(shm);
    }

    /// The lowest decided seqno still held (`u64::MAX` when none): the shard's watermarks
    /// stay below it (its durable one and its checkpoint's view of what readers may see).
    #[cfg(test)]
    pub(crate) fn lowest_held(&self) -> Seqno {
        self.lock().held.first().copied().unwrap_or(u64::MAX)
    }

    /// A tablet changes owner: the next stamp anywhere is strictly above every one handed out,
    /// so above the old owner's last one on the tablet (D11's floor that travels with a
    /// tablet), as after a decision.
    pub(crate) fn raise(&self) {
        self.lock().decided = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shm() -> ShmRegion {
        let mut config = pigeonhole_shm::ShmConfig::new(1);
        config.arena_bytes = 2 << 20;
        ShmRegion::in_memory([7; 16], &config)
    }

    #[test]
    fn default_timestamps_never_go_back_with_the_seqno() {
        let (r, shm) = (Reservation::default(), shm());
        r.seed_seqno(10);
        r.seed_ts(100);
        // Two shards' groups, a decision, and a group in the same µs after it.
        let (s1, t1) = r.reserve_group(2, 2, 50, 0, &shm, 0, u64::MAX);
        let (s2, t2) = r.reserve_group(1, 1, 50, 0, &shm, 0, u64::MAX);
        let (s3, t3) = r.reserve_decision(50, &shm);
        let (s4, t4) = r.reserve_group(1, 1, 50, 0, &shm, 0, u64::MAX);
        assert_eq!((s1, s2, s3, s4), (10, 12, 13, 14));
        // A group's k stamps are t1, t1 + 1; the next group may share H (another shard),
        // the decision is strictly above, and the group after it strictly above that.
        assert_eq!((t1, t2), (100, 101));
        assert_eq!(t3, 102);
        assert_eq!(t4, 103);
        assert_eq!(shm.next_seqno(), 15);
        // The clock read 50 at each reservation: `H` ran ahead of it by 53 at the last.
        assert_eq!(r.lead(50), (53, 53));
        assert_eq!(r.lead(200), (0, 53));
    }

    #[test]
    fn a_shard_never_repeats_its_own_stamp_and_the_clock_leads_when_ahead() {
        let (r, shm) = (Reservation::default(), shm());
        r.seed_seqno(1);
        let (_, a) = r.reserve_group(1, 1, 500, 0, &shm, 0, u64::MAX);
        let (_, b) = r.reserve_group(1, 1, 500, a, &shm, 0, u64::MAX);
        let (_, c) = r.reserve_group(1, 1, 900, b, &shm, 0, u64::MAX);
        assert_eq!((a, b, c), (500, 501, 900));
    }

    #[test]
    fn a_decision_is_held_from_snapshots_until_released_in_any_order() {
        let (r, shm) = (Reservation::default(), shm());
        r.seed_seqno(5);
        let (a, _) = r.reserve_decision(10, &shm);
        let (b, _) = r.reserve_decision(10, &shm);
        let (g, _) = r.reserve_group(1, 1, 10, 0, &shm, 0, u64::MAX);
        assert_eq!((a, b, g, shm.next_seqno()), (5, 6, 7, 8));
        // The group's hold was stored as it was reserved (D300), above the decisions'.
        assert_eq!(shm.visible_seqno(), a - 1);
        shm.publish_pending(0, u64::MAX); // the group applied
        assert_eq!(r.lowest_held(), a);
        assert_eq!(shm.visible_seqno(), a - 1);
        // The later decision finishing first frees nothing yet.
        r.release(b, &shm);
        assert_eq!(shm.visible_seqno(), a - 1);
        r.release(a, &shm);
        assert_eq!(r.lowest_held(), u64::MAX);
        assert_eq!(shm.visible_seqno(), g);
        assert_eq!(shm.durable_seqno(), g);
    }
}
