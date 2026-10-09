//! A read-path microbenchmark (#287): one hot row of about 3,500 cells whose hot qualifiers
//! carry many overwritten versions, half flushed to SSTs and half still in the memtable,
//! among 10,000 cold rows of 20 cells. Times row reads of the hot row and 10-row scans
//! through it, in process, and prints p10/p50/p90. Far less noisy than a full
//! `phdb-bench` run, so it suits changes worth a few percent.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example hotrow -- [ITERATIONS] [DIR]
//! ```
//!
//! The store is created in a fresh subdirectory of `DIR` (default: the system temp
//! directory) and removed at the end. `HOT_FLUSH=1` flushes the memtable before reading (all
//! versions in L0 SSTs); `HOT_COMPACT=1` compacts fully (one version per column), the floor
//! for the same cells.
//!
//! Every state is built deterministically (`support/driver.rs`): one application-owned shard,
//! only the flushes and compactions above, and the reads start with the shard idle. As
//! written, the hot row is in the memtable and one L0 SST.
#[path = "support/driver.rs"]
mod driver;
#[path = "support/measure.rs"]
mod measure;
use measure::Measured;

use std::ops::Bound;
use std::time::Instant;

use pigeonhole::{Durability, Family, Options};

/// One measured iteration: a row read of the hot row and a 10-row scan through it, counted by
/// callgrind while its `Measured` guard lives (`support/measure.rs`). Returns the
/// cells each read saw and their times in nanoseconds.
#[inline(never)]
fn hotrow_iteration(t: &pigeonhole::Table, hot: &[u8]) -> (usize, usize, u64, u64) {
    let _measured = Measured::start();
    let t0 = Instant::now();
    let row = t.row(hot).family("f").read().unwrap().unwrap();
    let row_cells = row.iter().count();
    let row_t = t0.elapsed().as_nanos() as u64;
    let t0 = Instant::now();
    let mut it = t
        .scan_bounds(Bound::Included(&b"h:04995"[..]), Bound::Unbounded)
        .limit(10)
        .iter()
        .unwrap();
    let mut scan_cells = 0;
    while let Some(r) = it.next_ref().unwrap() {
        scan_cells += std::hint::black_box(r.iter().count());
    }
    (row_cells, scan_cells, row_t, t0.elapsed().as_nanos() as u64)
}

fn main() {
    let iters: usize = std::env::args()
        .nth(1)
        .map_or(2000, |s| s.parse().expect("ITERATIONS"));
    let base = std::env::args()
        .nth(2)
        .map_or_else(std::env::temp_dir, std::path::PathBuf::from);
    let dir = base.join(format!("phdb-hotrow-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the store directory");
    let (db, driver) = driver::Driver::open(
        &dir.join("hot.phdb"),
        Options::default()
            .durability(Durability::Buffered)
            .block_cache(256 << 20),
    );
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default().max_versions(1).bloom_bits(10))
        .create_if_missing()
        .unwrap();
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    let mut rnd = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let val = [7u8; 100];
    // Neighbour rows of 20 cells, and the hot row h:5000.
    for r in 0..10_000u32 {
        let mut m = t.mutate(format!("h:{r:05}").as_bytes());
        for q in 0..20u32 {
            m = m.put(
                "f",
                format!("q{:05}", (r * 7 + q * 13) % 10_000).as_bytes(),
                &val,
            );
        }
        m.commit().unwrap();
    }
    let hot = b"h:05000";
    let put_hot = |n: usize, rnd: &mut dyn FnMut() -> u64| {
        for _ in 0..n {
            let mut m = t.mutate(hot);
            for _ in 0..2 {
                // Skewed: half the writes go to 50 hot qualifiers.
                let q = if rnd().is_multiple_of(2) {
                    rnd() % 50
                } else {
                    rnd() % 3500
                };
                m = m.put("f", format!("q{q:05}").as_bytes(), &val);
            }
            m.commit().unwrap();
        }
    };
    put_hot(10_000, &mut rnd);
    db.flush().unwrap();
    put_hot(10_000, &mut rnd);
    if std::env::var_os("HOT_COMPACT").is_some() {
        db.compact().unwrap();
    }
    if std::env::var_os("HOT_FLUSH").is_some() {
        db.flush().unwrap();
    }
    // Read everything once, outside the measured iterations, so they measure steady-state
    // reads (every block cached), not first-touch block loads.
    let mut it = t.scan_prefix(b"").versions(0).iter().unwrap();
    while let Some(r) = it.next_ref().unwrap() {
        std::hint::black_box(r.iter().count());
    }
    drop(it);
    driver.wait_idle();
    eprintln!("setup done"); // `sample` the process from here to profile the reads.
    // `SHAPE_SETUP_ONLY=1`: the setup alone, no measured iteration (the script checks that
    // callgrind then counts nothing inside `hotrow_iteration`).
    let iters = if setup_only() { 0 } else { iters };
    let (cells, read_cells, mut row_ns, mut scan_ns) = driver::on_fresh_thread(|| {
        let mut cells = 0;
        let mut read_cells = 0;
        let mut row_ns = Vec::with_capacity(iters);
        let mut scan_ns = Vec::with_capacity(iters);
        for _ in 0..iters {
            let (row_cells, scan_cells, row_t, scan_t) = hotrow_iteration(&t, hot);
            cells = row_cells;
            read_cells += (row_cells + scan_cells) as u64;
            row_ns.push(row_t);
            scan_ns.push(scan_t);
        }
        (cells, read_cells, row_ns, scan_ns)
    });
    // For instruction counts (`scripts/instructions-per-cell.sh`): cells read in all.
    eprintln!("cells read {read_cells}");
    row_ns.sort_unstable();
    scan_ns.sort_unstable();
    let p = |v: &[u64], q: f64| {
        v.get((v.len().saturating_sub(1) as f64 * q) as usize)
            .map_or(0.0, |&ns| ns as f64 / 1000.0)
    };
    println!(
        "cells {cells} | row read p10 {:.1} p50 {:.1} p90 {:.1} µs | scan p10 {:.1} p50 {:.1} p90 {:.1} µs",
        p(&row_ns, 0.1),
        p(&row_ns, 0.5),
        p(&row_ns, 0.9),
        p(&scan_ns, 0.1),
        p(&scan_ns, 0.5),
        p(&scan_ns, 0.9)
    );
    drop(t);
    db.close().unwrap();
    driver.join();
    std::fs::remove_dir_all(&dir).ok();
}

/// Whether to run the setup only (`SHAPE_SETUP_ONLY` set), for the measurement guard of
/// `scripts/instructions-per-cell.sh`.
fn setup_only() -> bool {
    std::env::var_os("SHAPE_SETUP_ONLY").is_some()
}
