//! Loom model of D271's one allocator against the shm mirror and a reader's visible seqno
//! (`RUSTFLAGS="--cfg engine_loom" cargo test --release -p pigeonhole-engine --lib loom`).
//!
//! The protocol's shared state, with the orderings the engine uses:
//! - the allocator (`Shared::clock`): `(next, h, decided)` under a lock;
//! - each writer's pending watermark (shm): stored Release, first a lower bound read from
//!   the mirror (never past the allocator's next seqno), then the reservation's first
//!   seqno, then `u64::MAX` once applied;
//! - the mirror (shm `NEXT_SEQNO`): `fetch_max(first + n, AcqRel)` after the reservation;
//! - a reader's `visible_seqno`: `min(mirror, every pending) − 1`, each loaded Acquire.
//!
//! The claims:
//! - a reader never sees a seqno visible before the writer that reserved it applied it;
//! - default timestamps never go back as seqnos go up, and a decision's is strictly above
//!   every stamp before it.
//!
//! The second model is D278's: a decision's seqno is held in the allocator (shm
//! `CROSS_HELD`, stored Release under the lock before the mirror passes it), its two
//! participants count down `AcqRel` once each applied its share, and the one that reaches
//! zero releases it under the lock, from either thread. A reader (`min(mirror, CROSS_HELD)`
//! − 1, Acquire both) never sees the seqno before both shares are applied.

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
    /// Each writer's `(seqno, timestamp)`, read after the joins.
    stamps: [[AtomicU64; 2]; 2],
}

/// Writer `i`: one reservation (a group of one when `!decision`, else a decision), stamped
/// at clock `now`, then applied.
fn write(sh: &Shared, i: usize, decision: bool, now: u64) {
    let lb = sh.mirror.load(Ordering::Acquire);
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
    sh.stamps[i][0].store(s, Ordering::Relaxed);
    sh.stamps[i][1].store(t, Ordering::Relaxed);
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

/// Set when some explored execution's reader saw a seqno visible, so the check above is not
/// vacuous (outside loom: it spans executions).
static SAW_PROGRESS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[test]
fn loom_the_mirror_never_shows_an_unapplied_seqno_and_stamps_follow_seqnos() {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(|| {
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
            stamps: Default::default(),
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
        // A reader process, on a thread of its own.
        let r = {
            let sh = Arc::clone(&sh);
            thread::spawn(move || {
                let v = visible(&sh);
                for s in 1..=v {
                    assert!(
                        sh.applied[s as usize].load(Ordering::Acquire),
                        "seqno {s} visible (v = {v}) before it was applied"
                    );
                }
                if v > 0 {
                    SAW_PROGRESS.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            })
        };
        a.join().unwrap();
        b.join().unwrap();
        r.join().unwrap();
        // Writer 0 reserved a group, writer 1 a decision, in the same µs.
        let stamp = |i: usize| {
            (
                sh.stamps[i][0].load(Ordering::Relaxed),
                sh.stamps[i][1].load(Ordering::Relaxed),
            )
        };
        let ((sg, tg), (sd, td)) = (stamp(0), stamp(1));
        // The decision is strictly above the group if after it, and the group strictly above
        // the decision if after it (the flag): never a tie, and never out of seqno order.
        if sg < sd {
            assert!(td > tg, "group ({sg}, {tg}), decision ({sd}, {td})");
        } else {
            assert!(tg > td, "decision ({sd}, {td}), group ({sg}, {tg})");
        }
    });
    assert!(
        SAW_PROGRESS.load(std::sync::atomic::Ordering::Relaxed),
        "no explored execution's reader saw a seqno visible"
    );
}

struct Held {
    /// The allocator's next seqno and the held decisions, under one lock.
    clock: Mutex<(u64, Vec<u64>)>,
    mirror: AtomicU64,
    cross_held: AtomicU64,
    /// The `Decide` message to the participant: the decision's seqno, 0 before.
    decide: AtomicU64,
    /// Each participant's share applied.
    shares: [AtomicBool; 2],
    remaining: AtomicU64,
    /// Which participant released (1 + index), checked after the joins: exactly once.
    released_by: AtomicU64,
    /// The decision's seqno, set under `clock`'s lock as it is reserved (0 before), and the
    /// third thread's group applied (it has seqno 1 or 2).
    decision: AtomicU64,
    group_applied: AtomicBool,
    /// The group's shard's pending watermark (FORMAT §11.3 steps 1 and 3).
    group_pending: AtomicU64,
}

fn publish_held(sh: &Held, held: &[u64]) {
    let lowest = held.iter().copied().min().unwrap_or(u64::MAX);
    sh.cross_held.store(lowest, Ordering::Release);
}

/// Participant `i` applied its share of decision `s`: counts down, and the last releases.
fn applied(sh: &Held, i: usize, s: u64) {
    sh.shares[i].store(true, Ordering::Release);
    if sh.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
        let mut c = sh.clock.lock().unwrap();
        c.1.retain(|&x| x != s);
        publish_held(sh, &c.1);
        drop(c);
        let before = sh.released_by.swap(1 + i as u64, Ordering::Relaxed);
        assert_eq!(before, 0, "released twice");
    }
}

static SAW_RELEASE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[test]
fn loom_a_decision_is_visible_only_once_its_last_participant_released_it() {
    held_model(true);
    assert!(
        SAW_RELEASE.load(std::sync::atomic::Ordering::Relaxed),
        "no explored execution's reader saw the decision visible"
    );
}

/// perf287's M1: a reader that loads `CROSS_HELD` before `NEXT_SEQNO` can see no hold, then
/// a mirror past a decision made in between, and take the decision as visible.
#[test]
#[should_panic(expected = "decision visible half applied")]
fn loom_a_reader_loading_the_hold_before_the_mirror_sees_a_held_decision() {
    held_model(false);
}

/// perf287's optional model on D278: a group on another shard reserves after the decision
/// and moves the mirror past it, with no lower bound published by the decider (the publish
/// D278 dropped). The reader still never sees the decision before both shares are applied.
#[test]
fn loom_a_group_passing_the_mirror_beyond_a_held_decision_does_not_expose_it() {
    held_model_with(true, true);
}

/// D278's hold against a reader of `min(mirror, CROSS_HELD) − 1`, loading the mirror first
/// (FORMAT §11.3) or, to show the order matters, the hold first.
fn held_model(mirror_first: bool) {
    held_model_with(mirror_first, false);
}

/// As [`held_model`], with `group`: a third thread reserves a group's seqno under the lock,
/// with its own pending watermark as §11.3 steps 1 and 3 have it, and advances the mirror
/// past it.
fn held_model_with(mirror_first: bool, group: bool) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(if group { 2 } else { 3 });
    model.check(move || {
        let sh = Arc::new(Held {
            clock: Mutex::new((1, Vec::new())),
            mirror: AtomicU64::new(1),
            cross_held: AtomicU64::new(u64::MAX),
            decide: AtomicU64::new(0),
            shares: [AtomicBool::new(false), AtomicBool::new(false)],
            remaining: AtomicU64::new(2),
            released_by: AtomicU64::new(0),
            decision: AtomicU64::new(0),
            group_applied: AtomicBool::new(false),
            group_pending: AtomicU64::new(u64::MAX),
        });
        // The coordinator decides, sends the decision, then applies its own share: the
        // participant may apply first or last.
        let coord = {
            let sh = Arc::clone(&sh);
            thread::spawn(move || {
                let s = {
                    let mut c = sh.clock.lock().unwrap();
                    let s = c.0;
                    c.0 += 1;
                    c.1.push(s);
                    sh.decision.store(s, Ordering::Relaxed);
                    publish_held(&sh, &c.1);
                    s
                };
                sh.mirror.fetch_max(s + 1, Ordering::AcqRel);
                sh.decide.store(s, Ordering::Release);
                applied(&sh, 0, s);
            })
        };
        let part = {
            let sh = Arc::clone(&sh);
            thread::spawn(move || {
                let s = loop {
                    let s = sh.decide.load(Ordering::Acquire);
                    if s != 0 {
                        break s;
                    }
                    thread::yield_now();
                };
                applied(&sh, 1, s);
            })
        };
        let g = group.then(|| {
            let sh = Arc::clone(&sh);
            thread::spawn(move || {
                // §11.3: a lower bound first, then the reservation, then applied.
                let lb = sh.mirror.load(Ordering::Acquire);
                sh.group_pending.store(lb, Ordering::Release);
                let s = {
                    let mut c = sh.clock.lock().unwrap();
                    let s = c.0;
                    c.0 += 1;
                    s
                };
                sh.group_pending.store(s, Ordering::Release);
                sh.mirror.fetch_max(s + 1, Ordering::AcqRel);
                sh.group_applied.store(true, Ordering::Release);
                sh.group_pending.store(u64::MAX, Ordering::Release);
            })
        });
        let r = {
            let sh = Arc::clone(&sh);
            thread::spawn(move || {
                let (next, held) = if mirror_first {
                    let next = sh.mirror.load(Ordering::Acquire);
                    (next, sh.cross_held.load(Ordering::Acquire))
                } else {
                    let held = sh.cross_held.load(Ordering::Acquire);
                    (sh.mirror.load(Ordering::Acquire), held)
                };
                let v = next
                    .min(held)
                    .min(sh.group_pending.load(Ordering::Acquire))
                    .saturating_sub(1);
                // Which seqno is the decision's, read under the lock after `v` (taking it now
                // cannot change what the loads above saw): 0 means not reserved yet, so `v`
                // covers only the group's.
                let d = {
                    let _c = sh.clock.lock().unwrap();
                    sh.decision.load(Ordering::Relaxed)
                };
                for s in 1..=v {
                    if s == d {
                        for share in &sh.shares {
                            assert!(
                                share.load(Ordering::Acquire),
                                "decision visible half applied"
                            );
                        }
                        SAW_RELEASE.store(true, std::sync::atomic::Ordering::Relaxed);
                    } else {
                        assert!(
                            group && sh.group_applied.load(Ordering::Acquire),
                            "seqno {s} visible unapplied"
                        );
                    }
                }
            })
        };
        coord.join().unwrap();
        part.join().unwrap();
        if let Some(g) = g {
            g.join().unwrap();
        }
        r.join().unwrap();
        assert_ne!(sh.released_by.load(Ordering::Relaxed), 0, "never released");
        assert_eq!(sh.cross_held.load(Ordering::Relaxed), u64::MAX);
    });
}

/// D286: the coordinator sends `Decide` before its DECISION is written, and counts down
/// twice: once for its own share (as a participant) and once when the DECISION is written.
/// A reader never sees the decision before both shares are applied and the DECISION written,
/// and the release happens exactly once, by whichever count is last. Here the coordinator
/// applies its own share inline before it counts its DECISION, as `on_prepared`'s
/// self-`Decide` does; the other participant's share may apply before or after either.
#[test]
fn loom_an_early_decision_is_visible_only_once_its_decision_is_written() {
    early_decide_model(true, false);
}

/// As above, with the coordinator's own share applied on another thread: a share parked on a
/// held header (D279) applies only when its group settles, so the DECISION's count can come
/// first, before any share applies, and the last share's count then releases and answers
/// (perf287's review of #989). Loom explores every order of the three count-downs.
#[test]
fn loom_an_early_decision_counted_before_any_share_applies_releases_once() {
    early_decide_model(true, true);
}

/// Without the DECISION's count, the shares release the decision while its DECISION is
/// unwritten.
#[test]
#[should_panic(expected = "visible before its DECISION was written")]
fn loom_an_early_decision_without_the_decisions_count_is_visible_unwritten() {
    early_decide_model(false, false);
}

fn early_decide_model(count_decision: bool, own_share_parked: bool) {
    let mut model = loom::model::Builder::new();
    // Four threads with the parked share: a bound of 2 keeps the model within loom's branch
    // budget while still letting each count-down come first.
    model.preemption_bound = Some(if own_share_parked { 2 } else { 3 });
    model.check(move || {
        let sh = Arc::new(Held {
            clock: Mutex::new((1, Vec::new())),
            mirror: AtomicU64::new(1),
            cross_held: AtomicU64::new(u64::MAX),
            decide: AtomicU64::new(0),
            shares: [AtomicBool::new(false), AtomicBool::new(false)],
            remaining: AtomicU64::new(if count_decision { 3 } else { 2 }),
            released_by: AtomicU64::new(0),
            decision: AtomicU64::new(0),
            group_applied: AtomicBool::new(false),
            group_pending: AtomicU64::new(u64::MAX),
        });
        let written = Arc::new(AtomicBool::new(false));
        // The coordinator: decides, sends `Decide` at once, applies its own share (counting
        // down as a participant), then writes its DECISION and counts down for it.
        let coord = {
            let (sh, written) = (Arc::clone(&sh), Arc::clone(&written));
            thread::spawn(move || {
                let s = {
                    let mut c = sh.clock.lock().unwrap();
                    let s = c.0;
                    c.0 += 1;
                    c.1.push(s);
                    sh.decision.store(s, Ordering::Relaxed);
                    publish_held(&sh, &c.1);
                    s
                };
                sh.mirror.fetch_max(s + 1, Ordering::AcqRel);
                sh.decide.store(s, Ordering::Release);
                // Parked variant: both shares apply on threads of their own, started once
                // `Decide` is out (no spinning), so the DECISION's count below may come
                // before either.
                let appliers: Vec<_> = if own_share_parked {
                    (0..2)
                        .map(|i| {
                            let sh = Arc::clone(&sh);
                            thread::spawn(move || applied(&sh, i, s))
                        })
                        .collect()
                } else {
                    applied(&sh, 0, s);
                    Vec::new()
                };
                written.store(true, Ordering::Release);
                if count_decision && sh.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                    let mut c = sh.clock.lock().unwrap();
                    c.1.retain(|&x| x != s);
                    publish_held(&sh, &c.1);
                    drop(c);
                    let before = sh.released_by.swap(3, Ordering::Relaxed);
                    assert_eq!(before, 0, "released twice");
                }
                for a in appliers {
                    a.join().unwrap();
                }
            })
        };
        let part = (!own_share_parked).then(|| {
            let sh = Arc::clone(&sh);
            thread::spawn(move || {
                let s = loop {
                    let s = sh.decide.load(Ordering::Acquire);
                    if s != 0 {
                        break s;
                    }
                    thread::yield_now();
                };
                applied(&sh, 1, s);
            })
        });
        let r = {
            let (sh, written) = (Arc::clone(&sh), Arc::clone(&written));
            thread::spawn(move || {
                let next = sh.mirror.load(Ordering::Acquire);
                let held = sh.cross_held.load(Ordering::Acquire);
                let v = next.min(held).saturating_sub(1);
                let d = {
                    let _c = sh.clock.lock().unwrap();
                    sh.decision.load(Ordering::Relaxed)
                };
                if d != 0 && v >= d {
                    for share in &sh.shares {
                        assert!(
                            share.load(Ordering::Acquire),
                            "decision visible half applied"
                        );
                    }
                    assert!(
                        written.load(Ordering::Acquire),
                        "visible before its DECISION was written"
                    );
                }
            })
        };
        coord.join().unwrap();
        if let Some(part) = part {
            part.join().unwrap();
        }
        r.join().unwrap();
        assert_ne!(sh.released_by.load(Ordering::Relaxed), 0, "never released");
        assert_eq!(sh.cross_held.load(Ordering::Relaxed), u64::MAX);
    });
}
