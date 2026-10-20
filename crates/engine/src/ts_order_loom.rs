//! Loom model of the timestamp order between a cross-shard commit's coordinator and a
//! participant's own stamping (D245, D263; `RUSTFLAGS="--cfg engine_loom" cargo test
//! --release -p pigeonhole-engine --lib loom`).
//!
//! The protocol's shared state, with the orderings the engine uses:
//! - `NEXT_SEQNO` (`shm::Region::reserve_seqnos`): one `fetch_add(AcqRel)` per reservation.
//! - `ts_coord_raises[p]` (`Shared`): the coordinator's `fetch_max(T, Release)` at the
//!   decision, before it reserves the commit's seqno `S`; the participant's `load(Acquire)`
//!   when it stamps (`fold_coord_raise`).
//! - `ts_floors[p]`: the participant's published floor (`store(Release)` after stamping),
//!   which a coordinator reads (`Acquire`) to pick `T`.
//!
//! What the atomics alone leave unordered (a straddle, two crossing decisions) is between
//! different rows: while a share is undecided, a single on one of its rows waits and a
//! PREPARE writing one is refused (`overlaps_undecided`, D263), so one cell's versions keep
//! seqno order whatever these models reach.

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU64, Ordering};
use loom::thread;

/// The coordinator's own floor before it picks.
const COORD_FLOOR: u64 = 5;

/// `decide_commit`: picks `T` above the participant's published floor and raise and its own
/// floor, raises the participant to `T`, then reserves `S`.
fn coordinate(next: &AtomicU64, floor: &AtomicU64, raise: &AtomicU64, own: u64) -> (u64, u64) {
    let t = own
        .max(floor.load(Ordering::Acquire))
        .max(raise.load(Ordering::Acquire))
        + 1;
    raise.fetch_max(t, Ordering::Release);
    let s = next.fetch_add(1, Ordering::AcqRel);
    (s, t)
}

/// `run_group` for one single: reserves, then stamps above the floor and the raise, then
/// publishes the floor.
fn stamp_single(next: &AtomicU64, floor: &AtomicU64, raise: &AtomicU64, local: u64) -> (u64, u64) {
    let s = next.fetch_add(1, Ordering::AcqRel);
    let ts = local.max(raise.load(Ordering::Acquire)) + 1;
    floor.store(ts, Ordering::Release);
    (s, ts)
}

#[test]
fn loom_a_later_seqno_never_stamps_below_the_coordinators_timestamp() {
    // Raise→reserve against reserve→stamp: whatever the interleaving, a single the
    // participant reserves after the commit's seqno stamps above its timestamp. One reserved
    // before may straddle (stamped above `T` once the coordinator has read the floor): the
    // model reaches it, and D263 allows it, since that single is on a row the share does
    // not write (a single on one of its rows waits for the decision).
    use std::sync::atomic::AtomicBool as StdBool;
    static STRADDLED: StdBool = StdBool::new(false);
    loom::model(|| {
        let next = Arc::new(AtomicU64::new(1));
        let floor = Arc::new(AtomicU64::new(0));
        let raise = Arc::new(AtomicU64::new(0));
        let participant = {
            let (next, floor, raise) = (Arc::clone(&next), Arc::clone(&floor), Arc::clone(&raise));
            thread::spawn(move || stamp_single(&next, &floor, &raise, 0))
        };
        let (s, t) = coordinate(&next, &floor, &raise, COORD_FLOOR);
        let (ps, pts) = participant.join().unwrap();
        if ps > s {
            assert!(pts > t, "seqno {ps} > {s} stamped at {pts} <= {t}");
        } else if pts > t {
            STRADDLED.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    });
    assert!(
        STRADDLED.load(std::sync::atomic::Ordering::Relaxed),
        "the model never reached the straddle"
    );
}

#[test]
fn loom_crossing_decisions_are_reachable_and_ordered_by_seqno_otherwise() {
    // Two coordinators decide concurrently: one can read the floors before the other's raise
    // and reserve after its seqno (S1 < S2, T1 > T2). D263 allows that between shares on
    // different rows (two shares on one row never are undecided together at a participant,
    // which refuses the second PREPARE). Without the crossing, seqno and timestamp agree.
    use std::sync::atomic::AtomicBool as StdBool;
    static CROSSED: StdBool = StdBool::new(false);
    loom::model(|| {
        let next = Arc::new(AtomicU64::new(1));
        let floor = Arc::new(AtomicU64::new(0));
        let raise = Arc::new(AtomicU64::new(0));
        let other = {
            let (next, floor, raise) = (Arc::clone(&next), Arc::clone(&floor), Arc::clone(&raise));
            thread::spawn(move || coordinate(&next, &floor, &raise, COORD_FLOOR + 1))
        };
        let a = coordinate(&next, &floor, &raise, COORD_FLOOR);
        let b = other.join().unwrap();
        let (lo, hi) = if a.0 < b.0 { (a, b) } else { (b, a) };
        if lo.1 > hi.1 {
            // The later reservation read the raise before the earlier one's fetch_max.
            CROSSED.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        assert_ne!(lo.0, hi.0);
    });
    assert!(
        CROSSED.load(std::sync::atomic::Ordering::Relaxed),
        "the model never crossed"
    );
}
