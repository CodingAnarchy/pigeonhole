//! SCRATCH #1206 (not for merge): a blocking single-shard commit's stages, by whether its
//! submit found the shard's driver asleep. Stamps are the VFS monotonic clock.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub const STAGES: [&str; 12] = [
    "client route+send",
    "inbox (submitted->picked)",
    "pending (picked->group)",
    "admission (group->admitted)",
    "wal (admitted->written)",
    "apply (written->applied)",
    "settle (applied->settled)",
    "hand-back+wake (settled->woken)",
    "visibility wait",
    "client return",
    "TOTAL client",
    "engine (submitted->settled)",
];
const FINE: usize = 4000;
const N: usize = FINE + 64;
static H: [[[AtomicU64; N]; 12]; 2] =
    [const { [const { [const { AtomicU64::new(0) }; N] }; 12] }; 2];
static ON: AtomicBool = AtomicBool::new(false);

thread_local! {
    /// Shard side: [submitted, picked, group, admitted, written, applied, settled].
    pub(crate) static SHARD: std::cell::Cell<[u64; 7]> = const { std::cell::Cell::new([0; 7]) };
    /// Client side: the settled commit's shard stamps.
    pub(crate) static SETTLED: std::cell::Cell<[u64; 7]> = const { std::cell::Cell::new([0; 7]) };
    /// Client side: [woken, visible].
    pub(crate) static CLIENT: std::cell::Cell<[u64; 2]> = const { std::cell::Cell::new([0; 2]) };
}

fn bucket(ns: u64) -> usize {
    if ns < 1_000_000 {
        (ns / 250) as usize
    } else {
        (FINE + (ns / 1_000_000) as usize).min(N - 1)
    }
}
fn floor_us(b: usize) -> f64 {
    if b < FINE {
        b as f64 * 0.25
    } else {
        ((b - FINE) * 1000) as f64
    }
}

/// Starts (and clears) or stops recording.
pub fn scratch_stages_record(on: bool) {
    if on {
        for c in TDOM.iter().chain(TSUM.iter()) {
            for x in c {
                x.store(0, Ordering::Relaxed);
            }
        }
        for x in &TN {
            x.store(0, Ordering::Relaxed);
        }
        for c in &H {
            for s in c {
                for b in s {
                    b.store(0, Ordering::Relaxed);
                }
            }
        }
    }
    ON.store(on, Ordering::Relaxed);
}

pub(crate) fn record(asleep: bool, t0: u64, sent: u64, s: [u64; 7], c: [u64; 2], ret: u64) {
    if !ON.load(Ordering::Relaxed) || s[6] == 0 {
        return;
    }
    let d = |a: u64, b: u64| b.saturating_sub(a);
    let v = [
        d(t0, sent),
        d(s[0], s[1]),
        d(s[1], s[2]),
        d(s[2], s[3]),
        d(s[3], s[4]),
        d(s[4], s[5]),
        d(s[5], s[6]),
        d(s[6], c[0]),
        d(c[0], c[1]),
        d(c[1], ret),
        d(t0, ret),
        d(s[0], s[6]),
    ];
    for (i, x) in v.iter().enumerate() {
        H[usize::from(asleep)][i][bucket(*x)].fetch_add(1, Ordering::Relaxed);
    }
    // SCRATCH #1460: a tail commit's dominant stage, and its stages' sums.
    if v[10] > TAIL_NS {
        let parts: u64 = v[..10].iter().sum();
        let mut w = [0u64; 11];
        w[..10].copy_from_slice(&v[..10]);
        w[10] = v[10].saturating_sub(parts);
        let dom = (0..11).max_by_key(|&i| w[i]).unwrap_or(10);
        let c = usize::from(asleep);
        TDOM[c][dom].fetch_add(1, Ordering::Relaxed);
        for (i, x) in w.iter().enumerate() {
            TSUM[c][i].fetch_add(*x, Ordering::Relaxed);
        }
        TN[c].fetch_add(1, Ordering::Relaxed);
    }
}

const TAIL_NS: u64 = 150_000;
static TDOM: [[AtomicU64; 11]; 2] = [const { [const { AtomicU64::new(0) }; 11] }; 2];
static TSUM: [[AtomicU64; 11]; 2] = [const { [const { AtomicU64::new(0) }; 11] }; 2];
static TN: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];

/// SCRATCH #1460: commits over 150 us by their largest stage.
pub fn scratch_tail_report() {
    for (c, name) in ["awake", "asleep"].iter().enumerate() {
        let n = TN[c].load(Ordering::Relaxed);
        eprintln!("SCRATCH-TAIL {name}: {n} commits over 150 us");
        for i in 0..11 {
            let st = if i < 10 { STAGES[i] } else { "unstamped (total - stages)" };
            eprintln!(
                "SCRATCH-TAIL {name} | {st:32} | dominant {:7} | mean {:8.1} us",
                TDOM[c][i].load(Ordering::Relaxed),
                TSUM[c][i].load(Ordering::Relaxed) as f64 / n.max(1) as f64 / 1000.0
            );
        }
    }
}

fn pct(h: &[u64], q: f64) -> f64 {
    let total: u64 = h.iter().sum();
    if total == 0 {
        return f64::NAN;
    }
    let target = ((total as f64 * q).ceil() as u64).max(1);
    let mut seen = 0;
    for (b, n) in h.iter().enumerate() {
        seen += n;
        if seen >= target {
            return floor_us(b);
        }
    }
    f64::NAN
}

/// Prints p50/p90/p99/p99.9 per stage and class to stderr.
pub fn scratch_stages_report() {
    for (k, name) in ["awake", "asleep", "all"].iter().enumerate() {
        let get = |i: usize| -> Vec<u64> {
            (0..N)
                .map(|b| match k {
                    2 => H[0][i][b].load(Ordering::Relaxed) + H[1][i][b].load(Ordering::Relaxed),
                    _ => H[k][i][b].load(Ordering::Relaxed),
                })
                .collect()
        };
        let n: u64 = get(10).iter().sum();
        eprintln!("SCRATCH-STAGES {name}: {n} commits");
        for (i, st) in STAGES.iter().enumerate() {
            let h = get(i);
            eprintln!(
                "SCRATCH-STAGES {name} | {st:32} | p50 {:8.2} | p90 {:8.2} | p99 {:8.2} | p999 {:8.2}",
                pct(&h, 0.5),
                pct(&h, 0.9),
                pct(&h, 0.99),
                pct(&h, 0.999)
            );
        }
    }
}

// ---- outer stages (one Instant clock): bench op start, pigeonhole commit entry, engine ----
pub const OUTER: [&str; 5] = [
    "build (op start->pigeonhole commit)",
    "pigeonhole pre-engine",
    "engine (Instant)",
    "post-engine (engine return->op end)",
    "TOTAL op (bench)",
];
static H2: [[[AtomicU64; N]; 5]; 2] =
    [const { [const { [const { AtomicU64::new(0) }; N] }; 5] }; 2];
static BASE: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
fn inow() -> u64 {
    BASE.get_or_init(std::time::Instant::now)
        .elapsed()
        .as_nanos() as u64
}
type Last = Option<(bool, u64, u64, [u64; 7], [u64; 2], u64)>;
thread_local! {
    /// [op start, pigeonhole commit entry, engine entry, engine return].
    static O: std::cell::Cell<[u64; 4]> = const { std::cell::Cell::new([0; 4]) };
    static LAST: std::cell::Cell<Last> = const { std::cell::Cell::new(None) };
}
pub fn scratch_op_start() {
    LAST.with(|l| l.set(None));
    O.with(|o| o.set([inow(), 0, 0, 0]));
}
pub fn scratch_mark_commit() {
    O.with(|o| {
        let mut v = o.get();
        v[1] = inow();
        o.set(v)
    });
}
pub(crate) fn engine_mark(i: usize) {
    O.with(|o| {
        let mut v = o.get();
        v[i] = inow();
        o.set(v)
    });
}
pub(crate) fn stash(asleep: bool, t0: u64, sent: u64, s: [u64; 7], c: [u64; 2], ret: u64) {
    LAST.with(|l| l.set(Some((asleep, t0, sent, s, c, ret))));
}
pub fn scratch_op_end() {
    let Some((asleep, t0, sent, s, c, ret)) = LAST.with(|l| l.take()) else {
        return;
    };
    if !ON.load(Ordering::Relaxed) || s[6] == 0 {
        return;
    }
    record(asleep, t0, sent, s, c, ret);
    let o = O.with(|o| o.get());
    let end = inow();
    if o[0] == 0 || o[1] == 0 || o[2] == 0 || o[3] == 0 {
        return;
    }
    let d = |a: u64, b: u64| b.saturating_sub(a);
    let v = [
        d(o[0], o[1]),
        d(o[1], o[2]),
        d(o[2], o[3]),
        d(o[3], end),
        d(o[0], end),
    ];
    for (i, x) in v.iter().enumerate() {
        H2[usize::from(asleep)][i][bucket(*x)].fetch_add(1, Ordering::Relaxed);
    }
}
pub fn scratch_outer_report() {
    for (k, name) in ["awake", "asleep"].iter().enumerate() {
        for (i, st) in OUTER.iter().enumerate() {
            let h: Vec<u64> = (0..N)
                .map(|b| H2[k][i][b].load(Ordering::Relaxed))
                .collect();
            eprintln!(
                "SCRATCH-OUTER {name} | {st:38} | p50 {:8.2} | p90 {:8.2} | p99 {:8.2} | p999 {:8.2}",
                pct(&h, 0.5),
                pct(&h, 0.9),
                pct(&h, 0.99),
                pct(&h, 0.999)
            );
        }
    }
}
