//! SCRATCH (perf2, cross-shard p99 investigation; never merge): what an engine-owned shard
//! thread's turns spend their time on. Each piece of a turn (a message, a group, a task
//! slice) adds its time under a kind; a turn of `LONG_NS` or more is credited to the kind
//! that took most of it. Log-scale buckets (4 per power of two), in nanoseconds.

use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// The kinds a turn's time is split into (the engine tags its own; the runtime tags the rest).
pub const NAMES: [&str; 46] = [
    "msg Commit",
    "msg Coordinate",
    "msg Prepare",
    "msg Prepared",
    "msg Decide",
    "msg Applied",
    "msg SyncDone",
    "msg Flushed",
    "msg CompactionDone",
    "msg Maintain",
    "msg Wake/Released",
    "msg Unpin",
    "msg ShareFlushed/Settled/Created",
    "msg Checkpointed",
    "msg Kick/PinSlice",
    "msg other",
    "run_group total (info)",
    "rg admit+stamp",
    "rg wal",
    "rg apply",
    "rg publish memtables",
    "rg pin/index",
    "rg freeze",
    "rg spawn flush",
    "rg resolve+spares",
    "rg other/early return",
    "end_batch rest",
    "task flush",
    "task compaction",
    "task spare",
    "task manifest",
    "task clock timer",
    "task untagged",
    "turn reap io",
    "turn unattributed",
    "fl (info) up to Ok",
    "fl (info) retire roots + memtables",
    "fl (info) reclaim_retired",
    "fl (info) prune_flushed_slots",
    "fl (info) report_shares_flushed",
    "fl (info) advance_checkpoint",
    "fl (info) to tail",
    "fl (info) check_flush_waiters",
    "fl (info) spawn_flush",
    "fl (info) maintain",
    "fl (info) progress_op + close",
];
/// A task slice whose task named no kind.
pub const TASK_UNTAGGED: usize = 32;
const REAP: usize = 33;
const UNATTRIBUTED: usize = 34;
const K: usize = NAMES.len();
const B: usize = 260;
/// Turns at least this long are credited to their dominant kind.
pub const LONG_NS: [u64; 3] = [100_000, 200_000, 400_000];

static PIECES: [[AtomicU64; B]; K] = [const { [const { AtomicU64::new(0) }; B] }; K];
static TOTAL_NS: [AtomicU64; K] = [const { AtomicU64::new(0) }; K];
static TURNS: [AtomicU64; B] = [const { AtomicU64::new(0) }; B];
/// `LONG[t][k]`: turns of at least `LONG_NS[t]` whose largest share was kind `k`.
static LONG: [[AtomicU64; K]; 3] = [const { [const { AtomicU64::new(0) }; K] }; 3];
/// `LONG_SHARE[t][k]`: the summed dominant kind's time in those turns (ns).
static LONG_SHARE: [[AtomicU64; K]; 3] = [const { [const { AtomicU64::new(0) }; K] }; 3];

thread_local! {
    static TURN: RefCell<[u64; K]> = const { RefCell::new([0; K]) };
    static TASK_KIND: Cell<usize> = const { Cell::new(TASK_UNTAGGED) };
}

fn bucket(ns: u64) -> usize {
    if ns == 0 {
        return 0;
    }
    let l = 63 - ns.leading_zeros() as usize;
    let frac = if l >= 2 {
        ((ns >> (l - 2)) & 3) as usize
    } else {
        0
    };
    (1 + l * 4 + frac).min(B - 1)
}

/// The upper bound of bucket `b` (ns).
pub fn upper(b: usize) -> u64 {
    if b == 0 {
        return 0;
    }
    let l = (b - 1) / 4;
    let frac = ((b - 1) % 4) as u64;
    if l < 2 {
        return 1 << (l + 1);
    }
    (1u64 << l) + ((frac + 1) << (l - 2))
}

/// Adds `ns` of the current turn to `kind`.
pub fn add(kind: usize, ns: u64) {
    PIECES[kind][bucket(ns)].fetch_add(1, Relaxed);
    TOTAL_NS[kind].fetch_add(ns, Relaxed);
    TURN.with(|t| t.borrow_mut()[kind] += ns);
}

/// Records `ns` under `kind` without counting it in the turn (an aggregate of other pieces).
pub fn add_info(kind: usize, ns: u64) {
    PIECES[kind][bucket(ns)].fetch_add(1, Relaxed);
    TOTAL_NS[kind].fetch_add(ns, Relaxed);
}

/// A task's `run` names its kind; the runtime reads it after the slice.
pub fn set_task_kind(kind: usize) {
    TASK_KIND.with(|c| c.set(kind));
}

pub(crate) fn take_task_kind() -> usize {
    TASK_KIND.with(|c| c.replace(TASK_UNTAGGED))
}

pub(crate) fn add_reap(ns: u64) {
    add(REAP, ns);
}

/// Ends a turn of `total` ns: records it, and credits a long one to its dominant kind.
pub(crate) fn end_turn(total: u64) {
    TURNS[bucket(total)].fetch_add(1, Relaxed);
    let parts = TURN.with(|t| std::mem::replace(&mut *t.borrow_mut(), [0; K]));
    let sum: u64 = parts.iter().sum();
    let rest = total.saturating_sub(sum);
    PIECES[UNATTRIBUTED][bucket(rest)].fetch_add(1, Relaxed);
    TOTAL_NS[UNATTRIBUTED].fetch_add(rest, Relaxed);
    if total < LONG_NS[0] {
        return;
    }
    let mut parts = parts;
    parts[UNATTRIBUTED] = rest;
    let (k, &v) = parts.iter().enumerate().max_by_key(|(_, v)| **v).unwrap();
    for (t, &l) in LONG_NS.iter().enumerate() {
        if total >= l {
            LONG[t][k].fetch_add(1, Relaxed);
            LONG_SHARE[t][k].fetch_add(v, Relaxed);
        }
    }
}

/// A snapshot (and reset) of everything recorded.
pub struct Report {
    /// Per kind: name, bucket counts, total ns.
    pub pieces: Vec<(&'static str, Vec<u64>, u64)>,
    /// Turn-length bucket counts.
    pub turns: Vec<u64>,
    /// `long[t][k]` and `long_share[t][k]` for `LONG_NS[t]`.
    pub long: Vec<Vec<u64>>,
    /// The dominant kind's summed time in those turns (ns).
    pub long_share: Vec<Vec<u64>>,
}

/// Takes the report and clears it.
pub fn take() -> Report {
    Report {
        pieces: (0..K)
            .map(|k| {
                (
                    NAMES[k],
                    PIECES[k].iter().map(|a| a.swap(0, Relaxed)).collect(),
                    TOTAL_NS[k].swap(0, Relaxed),
                )
            })
            .collect(),
        turns: TURNS.iter().map(|a| a.swap(0, Relaxed)).collect(),
        long: LONG
            .iter()
            .map(|r| r.iter().map(|a| a.swap(0, Relaxed)).collect())
            .collect(),
        long_share: LONG_SHARE
            .iter()
            .map(|r| r.iter().map(|a| a.swap(0, Relaxed)).collect())
            .collect(),
    }
}
