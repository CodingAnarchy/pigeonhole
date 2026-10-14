//! Loom model of D245's timestamp order between a cross-shard commit's coordinator and a
//! participant's own stamping (`RUSTFLAGS="--cfg engine_loom" cargo test --release -p
//! pigeonhole-engine --lib loom`).
//!
//! The protocol's shared state, with the orderings the engine uses:
//! - `NEXT_SEQNO` (`shm::Region::reserve_seqnos`): one `fetch_add(AcqRel)` per reservation.
//! - `ts_coord_raises[p]` (`Shared`): the coordinator's `fetch_max(T, Release)` before it
//!   reserves; the participant's `load(Acquire)` when it stamps (`default_ts`).
//! - `ts_floors[p]`: the participant's published floor (`store(Release)` after stamping),
//!   which a coordinator reads (`Acquire`) to pick `T`.
//!
//! The participant's check on a PREPARE `(S, T)` is modelled by [`refused`] over the
//! `(seqno, ts)` it stamped or routed, as `ShardState::floor_refusal` does.

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU64, Ordering};
use loom::thread;

/// The coordinator's own floor before it picks.
const COORD_FLOOR: u64 = 5;

/// `start_coordination`: picks `T` above the participant's published floor and raise and
/// its own floor, raises the participant to `T`, then reserves `S`.
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

/// `ShardState::floor_refusal` without the move raise: refused when a lower seqno has a
/// timestamp above `t` (side b) or a higher one has a timestamp below it (side a).
fn refused(history: &[(u64, u64)], s: u64, t: u64) -> bool {
    history
        .iter()
        .any(|&(hs, ht)| (hs < s && ht > t) || (hs > s && ht < t))
}

#[test]
fn loom_a_later_seqno_never_stamps_below_the_coordinators_timestamp() {
    // Raise→reserve against reserve→stamp: whatever the interleaving, a single the
    // participant reserves after the coordinator's seqno stamps above its timestamp. One
    // reserved before may straddle (stamped above `T` once the coordinator has read the
    // floor); the participant's check then refuses the PREPARE, so the order holds either
    // way.
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
        }
        let lands = !refused(&[(ps, pts)], s, t);
        if lands {
            // Applied: the participant's writes keep their timestamps in seqno order.
            assert!((ps < s) == (pts <= t), "({ps}, {pts}) against ({s}, {t})");
        } else {
            assert!(
                ps < s && pts > t,
                "refused without a straddle: ({ps}, {pts}) ({s}, {t})"
            );
            STRADDLED.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    });
    assert!(
        STRADDLED.load(std::sync::atomic::Ordering::Relaxed),
        "the model never reached the straddle"
    );
}

#[test]
fn loom_crossing_coordinators_are_refused_whichever_arrives_second() {
    // Two coordinators pick and reserve concurrently: one can read the floors before the
    // other's raise and reserve after its seqno (S1 < S2, T1 > T2). The participant then
    // refuses whichever PREPARE it routes second, and the one it keeps never inverts.
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
        let crossed = (a.0 < b.0) != (a.1 <= b.1);
        for (first, second) in [(a, b), (b, a)] {
            let mut history = Vec::new();
            assert!(!refused(&history, first.0, first.1));
            history.push(first);
            let second_refused = refused(&history, second.0, second.1);
            assert_eq!(second_refused, crossed, "{first:?} then {second:?}");
        }
        if crossed {
            CROSSED.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    });
    assert!(
        CROSSED.load(std::sync::atomic::Ordering::Relaxed),
        "the model never crossed"
    );
}
