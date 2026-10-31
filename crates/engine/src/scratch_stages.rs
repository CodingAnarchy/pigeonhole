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
