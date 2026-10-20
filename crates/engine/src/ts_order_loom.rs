//! Loom model of the timestamp order between a cross-shard commit's coordinator, a shard
//! handing over a tablet, and a participant's own stamping (D11, D245, D263, D264;
//! `RUSTFLAGS="--cfg engine_loom" cargo test --release -p pigeonhole-engine --lib loom`).
//!
//! The protocol's shared state, with the orderings the engine uses:
//! - `NEXT_SEQNO` (`shm::Region::reserve_seqnos`): one `fetch_add(AcqRel)` per reservation.
//! - `ts_raises[p]` (`Shared`, D264): the coordinator's `fetch_max(T, Release)` at the
//!   decision, before it reserves the commit's seqno `S`, and a tablet's old owner's
//!   `fetch_max(floor, AcqRel)` before the move is published; the participant's
//!   `load(Acquire)` once per group after its reservation (`fold_raises`).
//! - the view (`Shared::view`, an `ArcSwap`): published (Release) after the move's raise,
//!   loaded (Acquire) by the new owner before it admits the moved tablet's rows.
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
fn loom_crossing_decisions_are_reachable() {
    // Two coordinators decide concurrently: one can read the floors before the other's raise
    // and reserve after its seqno (S1 < S2, T1 > T2). D263 allows that between shares on
    // different rows (two shares on one row never are undecided together at a participant,
    // which refuses the second PREPARE).
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

#[test]
fn loom_a_moved_tablets_floor_and_a_decision_share_one_raise() {
    // D264: one raise per shard carries both a tablet move's floor and a coordinator's
    // timestamp. The old owner raises, then publishes the move; the new owner admits the
    // tablet's rows only once it sees the published view, reserves, then folds the raise.
    // Whatever the interleaving with a concurrent decision, such a single stamps above the
    // moved floor, and above the decision's timestamp when its seqno is later.
    use std::sync::atomic::AtomicBool as StdBool;
    const MOVED_FLOOR: u64 = 20;
    static ROUTED: StdBool = StdBool::new(false);
    loom::model(|| {
        let next = Arc::new(AtomicU64::new(1));
        let floor = Arc::new(AtomicU64::new(0));
        let raise = Arc::new(AtomicU64::new(0));
        let view = Arc::new(AtomicU64::new(0));
        let mover = {
            let (raise, view) = (Arc::clone(&raise), Arc::clone(&view));
            thread::spawn(move || {
                raise.fetch_max(MOVED_FLOOR, Ordering::AcqRel);
                view.store(1, Ordering::Release);
            })
        };
        let coordinator = {
            let (next, floor, raise) = (Arc::clone(&next), Arc::clone(&floor), Arc::clone(&raise));
            thread::spawn(move || coordinate(&next, &floor, &raise, COORD_FLOOR))
        };
        // The new owner: a single on the moved tablet runs here only under the new view.
        let routed = view.load(Ordering::Acquire) == 1;
        let stamped = routed.then(|| stamp_single(&next, &floor, &raise, 0));
        mover.join().unwrap();
        let (s, t) = coordinator.join().unwrap();
        if let Some((ps, pts)) = stamped {
            ROUTED.store(true, std::sync::atomic::Ordering::Relaxed);
            assert!(
                pts > MOVED_FLOOR,
                "stamped {pts} at or below the moved floor"
            );
            if ps > s {
                assert!(pts > t, "seqno {ps} > {s} stamped at {pts} <= {t}");
            }
        }
    });
    assert!(
        ROUTED.load(std::sync::atomic::Ordering::Relaxed),
        "the model never routed the single to the new owner"
    );
}
