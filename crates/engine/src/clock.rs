//! The engine's one seqno and default-timestamp allocator (D271). Every seqno a writer
//! reserves (a group's, a cross-shard commit's id, its decision's) comes from here, in the
//! writer's process, with the default timestamps that go with it, so a later seqno never
//! carries an earlier default timestamp on any shard. shm `NEXT_SEQNO` mirrors the seqnos
//! (`ShmRegion::advance_next_seqno`, a `fetch_max` after each reservation) for readers.
//!
//! `H` is the largest default timestamp handed out; `F` says the last reservation was a
//! cross-shard decision, so a group reserved in the same µs takes `H + 1` and cannot tie
//! with the decision's cells (D271, the owner's ruling on the same-µs tie).

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

use pigeonhole_format::{Seqno, Timestamp};
use pigeonhole_io::AtomicWord128;
use pigeonhole_shm::ShmRegion;

/// `F`, kept in `H`'s top bit (default timestamps are µs, far below it).
const DECIDED: u64 = 1 << 63;

thread_local! {
    /// The word as this thread last saw it: a reservation's first guess (#888). Stale only
    /// when another thread reserved since, which costs one more exchange.
    static GUESS: Cell<u128> = const { Cell::new(0) };
}

/// `(S, H, F)` in one 16-byte word (#888, ICR 0069): `S` the low half, `H` the high half
/// with `F` its top bit. Every change is one compare-exchange of the whole word, so the
/// three move together as they did under D271's lock.
fn pack(next: Seqno, h: Timestamp, decided: bool) -> u128 {
    // A real assert (perf287's S2): `H` reaching `F`'s bit would flip `F` silently. Every
    // `H` comes from the clock (µs) or from replay, which refuses one this large
    // (`check_replayed_ts`).
    assert!(h < DECIDED, "a default timestamp at or past 2^63: {h}");
    u128::from(next) | (u128::from(h | if decided { DECIDED } else { 0 }) << 64)
}

/// A replayed commit timestamp or the manifest's `ts_floor`, as `H` may hold it: below `F`'s
/// bit. Every one came from this allocator (µs), so a larger one means a damaged file.
pub(crate) fn check_replayed_ts(ts: Timestamp) -> crate::Result<Timestamp> {
    if ts >= DECIDED {
        return Err(crate::Error::Corruption(format!(
            "a default timestamp at or past 2^63 ({ts}): no allocator hands one out"
        )));
    }
    Ok(ts)
}

fn unpack(w: u128) -> (Seqno, Timestamp, bool) {
    let hi = (w >> 64) as u64;
    (w as u64, hi & !DECIDED, hi & DECIDED != 0)
}

/// The allocator's state: `(S, H, F)` in one word changed by compare-exchange (#888), and
/// the drift gauge beside it.
#[derive(Debug, Default)]
pub(crate) struct Reservation {
    /// `S`, the next seqno to reserve; `H`, the largest default timestamp handed out; `F`,
    /// the next stamp must be strictly above `H` (the last reservation was a decision's, or
    /// a tablet changed owner since).
    word: AtomicWord128,
    /// The drift gauge: the largest lead of `H` over the clock at any reservation. Not in
    /// the word (ICR 0064): written only when the lead grows.
    max_lead: AtomicU64,
}

impl Reservation {
    /// Changes the word by `f` (`(S, H, F)` in, the new one and a result out) in one
    /// exchange, retried with what it found until none raced it. `f` may run more than once.
    fn update<R>(&self, f: impl Fn(Seqno, Timestamp, bool) -> (Seqno, Timestamp, bool, R)) -> R {
        let mut cur = GUESS.get();
        loop {
            let (next, h, decided) = unpack(cur);
            let (next2, h2, decided2, r) = f(next, h, decided);
            let new = pack(next2, h2, decided2);
            match self.word.compare_exchange(cur, new) {
                Ok(_) => {
                    GUESS.set(new);
                    return r;
                }
                Err(actual) => cur = actual,
            }
        }
    }

    fn read(&self) -> (Seqno, Timestamp, bool) {
        let w = self.word.load();
        GUESS.set(w);
        unpack(w)
    }

    fn note_lead(&self, h: Timestamp, now: Timestamp) {
        let lead = h.saturating_sub(now);
        if lead > self.max_lead.load(Ordering::Relaxed) {
            self.max_lead.fetch_max(lead, Ordering::Relaxed);
        }
    }

    /// Starts at seqno `next` (at open, once replay has passed every recovered seqno), before
    /// anything at open reserves one.
    pub(crate) fn seed_seqno(&self, next: Seqno) {
        self.update(|_, h, d| (next, h, d, ()));
    }

    /// Raises `H` to `h` (at open: the manifest's floor and every replayed timestamp, not the
    /// clock: every reservation takes `now` into account itself, and a decision just after
    /// the reopen must not be pushed past it). Held below `F`'s bit: every one of them came
    /// from this allocator, so only a damaged file could carry more.
    pub(crate) fn seed_ts(&self, h: Timestamp) {
        let h = h.min(DECIDED - 1);
        self.update(|s, cur, d| (s, cur.max(h), d, ()));
    }

    /// The next seqno to reserve: a lower bound for every later reservation (FORMAT §11.3's
    /// pending watermark), and the seqno ceiling the manifest records.
    pub(crate) fn next_seqno(&self) -> Seqno {
        self.read().0
    }

    /// The largest default timestamp handed out (the manifest's `ts_floor`).
    pub(crate) fn last_ts(&self) -> Timestamp {
        self.read().1
    }

    /// The drift gauge (D271, ICR 0064): how far `H` is ahead of the clock `now`, and the
    /// largest such lead at any reservation since open, in µs. One shard reserving more
    /// than one group a µs, or a decision with a group after it in the same µs, moves `H`
    /// past the clock.
    pub(crate) fn lead(&self, now: Timestamp) -> (u64, u64) {
        let h = self.read().1;
        (h.saturating_sub(now), self.max_lead.load(Ordering::Relaxed))
    }

    /// A group's `n` seqnos and the first of `k` default timestamps (`T0 .. T0 + k`): `T0`
    /// above `H` after a decision, and above this shard's own last stamp `last`, so a
    /// shard's stamps strictly increase (D271 variant B).
    pub(crate) fn reserve_group(
        &self,
        n: u64,
        k: u64,
        now: Timestamp,
        last: Timestamp,
        shm: &ShmRegion,
    ) -> (Seqno, Timestamp) {
        let (first, t0, h) = self.update(|next, h, decided| {
            let t0 = now.max(h + u64::from(decided)).max(last + 1);
            let h2 = h.max(t0 + k.max(1) - 1);
            (next + n, h2, false, (next, t0, h2))
        });
        self.note_lead(h, now);
        shm.advance_next_seqno(first + n);
        (first, t0)
    }

    /// One seqno with no timestamp: a cross-shard commit's id (D263), never applied.
    pub(crate) fn reserve_id(&self, shm: &ShmRegion) -> Seqno {
        let first = self.update(|next, h, d| (next + 1, h, d, next));
        shm.advance_next_seqno(first + 1);
        first
    }

    /// A cross-shard decision's seqno and timestamp: strictly above every default timestamp
    /// handed out before it.
    pub(crate) fn reserve_decision(&self, now: Timestamp, shm: &ShmRegion) -> (Seqno, Timestamp) {
        let (s, t) = self.update(|next, h, _| {
            let t = now.max(h + 1);
            (next + 1, t, true, (next, t))
        });
        self.note_lead(t, now);
        shm.advance_next_seqno(s + 1);
        (s, t)
    }

    /// A tablet changes owner: the next stamp anywhere is strictly above every one handed out,
    /// so above the old owner's last one on the tablet (D11's floor that travels with a
    /// tablet), as after a decision.
    pub(crate) fn raise(&self) {
        self.update(|s, h, _| (s, h, true, ()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_replayed_timestamp_at_or_past_the_flag_bit_is_corruption() {
        // perf287's S2 on #933: `H` shares its word with `F`, so replay refuses a timestamp
        // that would reach `F`'s bit instead of letting it flip the flag.
        assert_eq!(check_replayed_ts(DECIDED - 1).unwrap(), DECIDED - 1);
        assert!(matches!(
            check_replayed_ts(DECIDED),
            Err(crate::Error::Corruption(_))
        ));
        assert!(matches!(
            check_replayed_ts(u64::MAX),
            Err(crate::Error::Corruption(_))
        ));
    }

    #[test]
    #[should_panic(expected = "a default timestamp at or past 2^63")]
    fn packing_a_timestamp_into_the_flag_bit_panics_in_every_build() {
        let _ = pack(1, DECIDED, false);
    }

    #[test]
    fn the_word_round_trips_s_h_and_f() {
        for (s, h, f) in [(0, 0, false), (7, 9, true), (u64::MAX, DECIDED - 1, true)] {
            assert_eq!(unpack(pack(s, h, f)), (s, h, f));
        }
    }

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
        let (s1, t1) = r.reserve_group(2, 2, 50, 0, &shm);
        let (s2, t2) = r.reserve_group(1, 1, 50, 0, &shm);
        let (s3, t3) = r.reserve_decision(50, &shm);
        let (s4, t4) = r.reserve_group(1, 1, 50, 0, &shm);
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
        let (_, a) = r.reserve_group(1, 1, 500, 0, &shm);
        let (_, b) = r.reserve_group(1, 1, 500, a, &shm);
        let (_, c) = r.reserve_group(1, 1, 900, b, &shm);
        assert_eq!((a, b, c), (500, 501, 900));
    }
}
