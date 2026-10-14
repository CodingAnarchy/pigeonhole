//! SCRATCH (perf2, cross-shard p99 investigation; never merge): phase histograms of
//! commits. Log-scale buckets (4 per power of two), in nanoseconds.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// Phases: client-side (the blocking wait), then the coordinator's.
pub const NAMES: [&str; 8] = [
    "single: wait for reply",
    "single: reply -> visible",
    "cross: wait for reply",
    "cross: reply -> visible",
    "cross: submit -> coordinate",
    "cross: coordinate -> all prepared",
    "cross: prepared -> decide sent (COMMIT logged)",
    "cross: decide sent -> all applied",
];
const B: usize = 260;
static H: [[AtomicU64; B]; 8] = [const { [const { AtomicU64::new(0) }; B] }; 8];

thread_local! {
    static CROSS: Cell<bool> = const { Cell::new(false) };
}

/// The calling thread's next commits are cross-shard (`true`) or not.
pub fn set_cross(cross: bool) {
    CROSS.with(|c| c.set(cross));
}

pub(crate) fn is_cross() -> bool {
    CROSS.with(|c| c.get())
}

fn bucket(ns: u64) -> usize {
    if ns == 0 {
        return 0;
    }
    let l = 63 - ns.leading_zeros() as usize;
    let frac = if l >= 2 { ((ns >> (l - 2)) & 3) as usize } else { 0 };
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

pub(crate) fn record(phase: usize, ns: u64) {
    H[phase][bucket(ns)].fetch_add(1, Relaxed);
}

/// Every phase's bucket counts (and clears them).
pub fn take() -> Vec<(&'static str, Vec<u64>)> {
    NAMES
        .iter()
        .enumerate()
        .map(|(i, n)| (*n, H[i].iter().map(|a| a.swap(0, Relaxed)).collect()))
        .collect()
}
