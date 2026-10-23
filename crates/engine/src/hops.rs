//! SCRATCH (#708 hop measurement, not for merge): per-hop times of committed cross-shard
//! commits, 1 in 4 sampled by id. Each step keeps its latest time over the participants;
//! a commit's gaps are added up when its coordinator has every `Applied`, and printed every
//! 2^13 commits.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

pub(crate) const COORD: usize = 0;
pub(crate) const PREP_RECV: usize = 1;
pub(crate) const PREP_GROUP: usize = 2;
pub(crate) const PREP_VALIDATED: usize = 3;
pub(crate) const PREPARED_SENT: usize = 4;
pub(crate) const PREPARED_RECV: usize = 5;
pub(crate) const DECIDE_SENT: usize = 6;
pub(crate) const DECIDE_RECV: usize = 7;
pub(crate) const APPLIED_SENT: usize = 8;
pub(crate) const APPLIED_RECV: usize = 9;
const N: usize = 10;
const NAMES: [&str; N - 1] = [
    "coord->prepare_recv",
    "prepare_recv->group_admits",
    "group_admits->validated",
    "validated->prepared_sent",
    "prepared_sent->prepared_recv",
    "prepared_recv->decide_sent",
    "decide_sent->decide_recv",
    "decide_recv->applied_sent",
    "applied_sent->applied_recv",
];

fn now() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_nanos() as u64 + 1
}

fn map() -> &'static Mutex<HashMap<u64, [u64; N]>> {
    static M: OnceLock<Mutex<HashMap<u64, [u64; N]>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

static SUM: [AtomicU64; N] = [const { AtomicU64::new(0) }; N];
static MAX: [AtomicU64; N] = [const { AtomicU64::new(0) }; N];
// log2 buckets of each gap, for a p99.
static HIST: [[AtomicU64; 40]; N] = [const { [const { AtomicU64::new(0) }; 40] }; N];
static DONE: AtomicU64 = AtomicU64::new(0);
/// SCRATCH: validation entries visited in memtables and in SSTs, and validations run.
pub(crate) static VAL: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];

/// SCRATCH (#914 C1): when the coordinator's own share is prepared (its vote), and when
/// the remote participant builds its group and finishes validating, per commit.
pub(crate) const V_A_SELF: usize = 0;
pub(crate) const V_B_GROUP: usize = 1;
pub(crate) const V_B_VAL: usize = 2;
pub(crate) const V_B_LOGGED: usize = 3;
fn votes() -> &'static Mutex<HashMap<u64, [u64; 4]>> {
    static M: OnceLock<Mutex<HashMap<u64, [u64; 4]>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}
/// Vote-in-time counts against B's group build, B's validation end and B's write end,
/// assuming a 1.8 µs A->B hop; then the sum of (vote arrival - B's group build), and n.
static VOTE: [AtomicU64; 6] = [const { AtomicU64::new(0) }; 6];
static VOTE_LATE_HIST: [AtomicU64; 40] = [const { AtomicU64::new(0) }; 40];
const HOP_NS: u64 = 1_800;

pub(crate) fn vote(id: u64, ev: usize) {
    if !enabled() || id % 4 != 0 {
        return;
    }
    let t = now();
    let mut m = votes().lock().unwrap();
    let e = m.entry(id).or_insert([0; 4]);
    if e[ev] == 0 {
        e[ev] = t;
    }
}

fn vote_finish(id: u64) {
    let Some(v) = votes().lock().unwrap().remove(&id) else {
        return;
    };
    if v.iter().any(|t| *t == 0) {
        return;
    }
    let arrive = v[V_A_SELF] + HOP_NS;
    for (i, ev) in [V_B_GROUP, V_B_VAL, V_B_LOGGED].into_iter().enumerate() {
        if arrive <= v[ev] {
            VOTE[i].fetch_add(1, Ordering::Relaxed);
        }
    }
    let late = arrive.saturating_sub(v[V_B_GROUP]);
    let b = (64 - late.leading_zeros() as usize).min(39);
    VOTE_LATE_HIST[b].fetch_add(1, Ordering::Relaxed);
    VOTE[3].fetch_add(arrive.saturating_sub(v[V_B_GROUP]), Ordering::Relaxed);
    VOTE[4].fetch_add(v[V_B_GROUP].saturating_sub(arrive), Ordering::Relaxed);
    VOTE[5].fetch_add(1, Ordering::Relaxed);
}

fn vote_line() -> String {
    let n = VOTE[5].load(Ordering::Relaxed).max(1);
    let pct = |i: usize| VOTE[i].load(Ordering::Relaxed) as f64 * 100.0 / n as f64;
    let mut late = String::new();
    for b in 0..40 {
        let c = VOTE_LATE_HIST[b].load(Ordering::Relaxed);
        if c > 0 {
            late += &format!(" <{}ns:{:.1}%", 1u64 << b, c as f64 * 100.0 / n as f64);
        }
    }
    format!(
        "VOTE n={n} in_time_vs_B_group={:.1}% vs_B_validated={:.1}% vs_B_logged={:.1}% mean_late_us={:.2} mean_early_us={:.2} late_hist:{late}",
        pct(0),
        pct(1),
        pct(2),
        VOTE[3].load(Ordering::Relaxed) as f64 / n as f64 / 1e3,
        VOTE[4].load(Ordering::Relaxed) as f64 / n as f64 / 1e3,
    )
}

pub(crate) fn enabled() -> bool {
    static E: OnceLock<bool> = OnceLock::new();
    *E.get_or_init(|| std::env::var_os("PH_HOPS").is_some())
}

pub(crate) fn mark(id: u64, ev: usize) {
    if !enabled() || id % 4 != 0 {
        return;
    }
    let t = now();
    let mut m = map().lock().unwrap();
    let e = m.entry(id).or_insert([0; N]);
    if t > e[ev] {
        e[ev] = t;
    }
}

pub(crate) fn drop_id(id: u64) {
    if !enabled() || id % 4 != 0 {
        return;
    }
    votes().lock().unwrap().remove(&id);
    map().lock().unwrap().remove(&id);
}

pub(crate) fn finish(id: u64) {
    if !enabled() || id % 4 != 0 {
        return;
    }
    vote_finish(id);
    let Some(e) = map().lock().unwrap().remove(&id) else {
        return;
    };
    if e.iter().any(|t| *t == 0) {
        return;
    }
    for i in 0..N - 1 {
        let d = e[i + 1].saturating_sub(e[i]);
        SUM[i].fetch_add(d, Ordering::Relaxed);
        MAX[i].fetch_max(d, Ordering::Relaxed);
        let b = (64 - d.leading_zeros() as usize).min(39);
        HIST[i][b].fetch_add(1, Ordering::Relaxed);
    }
    let total = e[N - 1] - e[0];
    SUM[N - 1].fetch_add(total, Ordering::Relaxed);
    let n = DONE.fetch_add(1, Ordering::Relaxed) + 1;
    if n % (1 << 13) == 0 {
        let mut out = format!("HOPS n={n} total_mean_us={:.1} val_mem_entries={} val_sst_entries={} validations={}", SUM[N - 1].load(Ordering::Relaxed) as f64 / n as f64 / 1e3, VAL[0].load(Ordering::Relaxed), VAL[1].load(Ordering::Relaxed), VAL[2].load(Ordering::Relaxed));
        for i in 0..N - 1 {
            let mean = SUM[i].load(Ordering::Relaxed) as f64 / n as f64 / 1e3;
            let mut seen = 0;
            let mut p99 = 0u64;
            for b in 0..40 {
                seen += HIST[i][b].load(Ordering::Relaxed);
                if seen * 100 >= n * 99 {
                    p99 = 1u64 << b;
                    break;
                }
            }
            out += &format!(" | {} mean={mean:.1}us p99<{:.0}us", NAMES[i], p99 as f64 / 1e3);
        }
        eprintln!("{out}");
        eprintln!("{}", vote_line());
    }
}
