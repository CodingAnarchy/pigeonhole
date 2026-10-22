//! Loom model of D271's one allocator against the shm mirror and a reader's visible seqno
//! (`RUSTFLAGS="--cfg engine_loom" cargo test --release -p pigeonhole-engine --lib loom`).
//!
//! The protocol's shared state, with the orderings the engine uses:
//! - the allocator (`Shared::clock`): `(next, h, decided)` under a lock;
//! - each writer's pending watermark (shm): stored Release, first a lower bound read from
//!   the allocator, then the reservation's first seqno, then `u64::MAX` once applied;
//! - the mirror (shm `NEXT_SEQNO`): `fetch_max(first + n, AcqRel)` after the reservation;
//! - a reader's `visible_seqno`: `min(mirror, every pending) − 1`, each loaded Acquire.
//!
//! The claims:
//! - a reader never sees a seqno visible before the writer that reserved it applied it;
//! - default timestamps never go back as seqnos go up, and a decision's is strictly above
//!   every stamp before it.

use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

struct Clock {
    next: u64,
    h: u64,
    decided: bool,
}

struct Shared {
    clock: Mutex<Clock>,
    mirror: AtomicU64,
    pending: [AtomicU64; 2],
    applied: [AtomicBool; 4],
    stamps: Mutex<Vec<(u64, u64, bool)>>,
}

/// Writer `i`: one reservation (a group of one when `!decision`, else a decision), stamped
/// at clock `now`, then applied.
fn write(sh: &Shared, i: usize, decision: bool, now: u64) {
    let lb = sh.clock.lock().unwrap().next;
    sh.pending[i].store(lb, Ordering::Release);
    let (s, t) = {
        let mut c = sh.clock.lock().unwrap();
        let s = c.next;
        c.next += 1;
        let t = if decision {
            let t = now.max(c.h + 1);
            c.h = t;
            c.decided = true;
            t
        } else {
            let t = now.max(c.h + u64::from(c.decided));
            c.h = c.h.max(t);
            c.decided = false;
            t
        };
        (s, t)
    };
    sh.pending[i].store(s, Ordering::Release);
    sh.mirror.fetch_max(s + 1, Ordering::AcqRel);
    sh.stamps.lock().unwrap().push((s, t, decision));
    sh.applied[s as usize].store(true, Ordering::Release);
    sh.pending[i].store(u64::MAX, Ordering::Release);
}

fn visible(sh: &Shared) -> u64 {
    let mut lowest = sh.mirror.load(Ordering::Acquire);
    for p in &sh.pending {
        lowest = lowest.min(p.load(Ordering::Acquire));
    }
    lowest.saturating_sub(1)
}

#[test]
fn loom_the_mirror_never_shows_an_unapplied_seqno_and_stamps_follow_seqnos() {
    loom::model(|| {
        let sh = Arc::new(Shared {
            clock: Mutex::new(Clock {
                next: 1,
                h: 0,
                decided: false,
            }),
            mirror: AtomicU64::new(1),
            pending: [AtomicU64::new(u64::MAX), AtomicU64::new(u64::MAX)],
            applied: [
                AtomicBool::new(true),
                AtomicBool::new(false),
                AtomicBool::new(false),
                AtomicBool::new(false),
            ],
            stamps: Mutex::new(Vec::new()),
        });
        // A group and a decision in the same µs, on two shards.
        let a = {
            let sh = Arc::clone(&sh);
            thread::spawn(move || write(&sh, 0, false, 5))
        };
        let b = {
            let sh = Arc::clone(&sh);
            thread::spawn(move || write(&sh, 1, true, 5))
        };
        // A reader process.
        let v = visible(&sh);
        for s in 1..=v {
            assert!(
                sh.applied[s as usize].load(Ordering::Acquire),
                "seqno {s} visible (v = {v}) before it was applied"
            );
        }
        a.join().unwrap();
        b.join().unwrap();
        let mut stamps = sh.stamps.lock().unwrap().clone();
        stamps.sort_unstable();
        for w in stamps.windows(2) {
            let ((_, t0, _), (_, t1, d1)) = (w[0], w[1]);
            assert!(t1 >= t0, "{stamps:?}");
            if d1 {
                assert!(t1 > t0, "a decision tied an earlier stamp: {stamps:?}");
            }
        }
        // A group after a decision in the same µs is above it (the flag).
        if let [(_, _, true), (_, t1, false)] = stamps[..] {
            assert!(t1 > stamps[0].1, "{stamps:?}");
        }
    });
}
