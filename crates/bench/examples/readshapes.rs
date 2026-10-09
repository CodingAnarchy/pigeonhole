//! Read-path microbenchmarks across read shapes (#287), so a read-path change is measured on
//! more than the wide hot row of `hotrow.rs`. One shape per run:
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example readshapes -- SHAPE [ITERATIONS] [DIR]
//! ```
//!
//! The store: 20,000 narrow rows of 8 cells (`r:00000`..), the first half flushed to SSTs and
//! the second half still in the memtable. Column `q0` of every row has 3 versions in family
//! `f` (which keeps 4), and family `c` (a counter family) holds a counter per 10th row,
//! incremented 5 times, partly before the flush. A second table, `l`, holds the same 20,000
//! rows of 8 cells fully compacted: its bottom level is several SSTs. Shapes:
//!
//! - `get-mem`, `get-sst`: point gets of an existing cell in the memtable or SST half;
//! - `get-miss`: point gets of rows that do not exist;
//! - `row`: row reads of one narrow row (8 cells);
//! - `scan`: 200-row range scans (1,600 cells);
//! - `rows`: row reads of one narrow row (8 cells), counted per row: the fixed cost of a
//!   row read (snapshot, routing, sources) as much as its cells;
//! - `short-scans`: 5-row range scans, counted per row: a scan's setup amortized over few
//!   rows;
//! - `level-scans`: the 5-row scans of `short-scans` on table `l`, counted per row: a scan
//!   over a level of several SSTs, of which it reads one or two;
//! - `scan-filtered`: the same scans with a qualifier prefix (one cell per row);
//! - `versions`: row reads of `q0` with `versions(3)`;
//! - `counter`: point gets of counter cells (operands folded on read).
//!
//! It follows the shape-binary contract of `scripts/instructions-per-cell.sh`: each measured
//! iteration is one `#[inline(never)]` function named `shape_*` that counts its work with a
//! `Measured` guard (`support/measure.rs`), and on stderr the run prints `units N`: one per
//! point get (hit or miss), one per row for `rows`, `short-scans` and `level-scans`, one per
//! returned cell
//! otherwise. A full scan before the
//! measured iterations warms the block cache, so every shape measures steady-state reads.
//!
//! The store is built deterministically (`support/driver.rs`): one application-owned shard,
//! only the setup's own flush, and the measured reads start with the shard idle.
#[path = "support/driver.rs"]
mod driver;
#[path = "support/measure.rs"]
mod measure;
use measure::Measured;

use std::ops::Bound;

use pigeonhole::{Durability, Family, Options, Table};

const ROWS: u32 = 20_000;
const CELLS: u32 = 8;

/// A fixed-seed xorshift, so every run reads the same keys.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u32) -> u32 {
        (self.next() % u64::from(n)) as u32
    }
}

/// `prefix` and `r` in five digits, without allocating (keys are built inside the measured
/// loops).
fn key(prefix: u8, r: u32) -> [u8; 7] {
    let mut k = [prefix, b':', 0, 0, 0, 0, 0];
    let mut r = r;
    for d in k[2..].iter_mut().rev() {
        *d = b'0' + (r % 10) as u8;
        r /= 10;
    }
    k
}

fn row_key(r: u32) -> [u8; 7] {
    key(b'r', r)
}

#[inline(never)]
fn shape_get(t: &Table, rng: &mut Rng, base: u32) -> usize {
    let _measured = Measured::start();
    let mut n = 0;
    for _ in 0..500 {
        let r = base + rng.below(ROWS / 2);
        let q = [b'q', b'0' + (1 + rng.below(CELLS - 1)) as u8];
        let hit = t.get(&row_key(r), "f", &q).unwrap();
        std::hint::black_box(hit.map(|c| c.value().len()));
        n += 1;
    }
    n
}

#[inline(never)]
fn shape_get_miss(t: &Table, rng: &mut Rng) -> usize {
    let _measured = Measured::start();
    let mut n = 0;
    for _ in 0..500 {
        let miss = t.get(&key(b'x', rng.below(ROWS)), "f", b"q1").unwrap();
        std::hint::black_box(miss.is_some());
        n += 1;
    }
    n
}

#[inline(never)]
fn shape_row(t: &Table, rng: &mut Rng) -> usize {
    let _measured = Measured::start();
    let mut n = 0;
    for _ in 0..200 {
        let row = t.row(&row_key(rng.below(ROWS))).family("f").read().unwrap();
        n += std::hint::black_box(row.map_or(0, |r| r.iter().count()));
    }
    n
}

#[inline(never)]
fn shape_rows(t: &Table, rng: &mut Rng) -> usize {
    let _measured = Measured::start();
    let mut n = 0;
    for _ in 0..200 {
        let row = t.row(&row_key(rng.below(ROWS))).family("f").read().unwrap();
        n += usize::from(std::hint::black_box(
            row.is_some_and(|r| r.iter().count() == 8),
        ));
    }
    n
}

#[inline(never)]
fn shape_short_scans(t: &Table, rng: &mut Rng) -> usize {
    let _measured = Measured::start();
    let mut n = 0;
    for _ in 0..40 {
        let start = row_key(rng.below(ROWS - 5));
        let scan = t
            .scan_bounds(Bound::Included(&start[..]), Bound::Unbounded)
            .family("f")
            .limit(5);
        let mut it = scan.iter().unwrap();
        while let Some(r) = it.next_ref().unwrap() {
            n += usize::from(std::hint::black_box(r.iter().count()) > 0);
        }
    }
    n
}

#[inline(never)]
fn shape_scan(t: &Table, rng: &mut Rng, filtered: bool) -> usize {
    let _measured = Measured::start();
    let mut n = 0;
    for _ in 0..4 {
        let start = row_key(rng.below(ROWS - 200));
        let mut scan = t.scan_bounds(Bound::Included(&start[..]), Bound::Unbounded);
        scan = scan.family("f").limit(200);
        if filtered {
            scan = scan.qualifier_prefix(b"q3");
        }
        let mut it = scan.iter().unwrap();
        while let Some(r) = it.next_ref().unwrap() {
            n += std::hint::black_box(r.iter().count());
        }
    }
    n
}

#[inline(never)]
fn shape_versions(t: &Table, rng: &mut Rng) -> usize {
    let _measured = Measured::start();
    let mut n = 0;
    for _ in 0..300 {
        let row = t
            .row(&row_key(rng.below(ROWS)))
            .family("f")
            .qualifier_prefix(b"q0")
            .versions(3)
            .read()
            .unwrap();
        n += std::hint::black_box(row.map_or(0, |r| r.iter().count()));
    }
    n
}

#[inline(never)]
fn shape_counter(t: &Table, rng: &mut Rng) -> usize {
    let _measured = Measured::start();
    let mut n = 0;
    for _ in 0..500 {
        let r = 10 * rng.below(ROWS / 10);
        let c = t.get(&row_key(r), "c", b"n").unwrap();
        std::hint::black_box(c.and_then(|c| c.as_i64()));
        n += 1;
    }
    n
}

/// Reads every row of both families once (outside the measured functions), so every block
/// is in the cache and the shapes measure steady-state reads, not first-touch block loads.
fn warm(t: &Table) {
    for family in ["f", "c"] {
        let mut it = t
            .scan_prefix(b"")
            .family(family)
            .versions(0)
            .iter()
            .unwrap();
        while let Some(r) = it.next_ref().unwrap() {
            std::hint::black_box(r.iter().count());
        }
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let shape = args.next().expect("SHAPE");
    let iters: usize = args.next().map_or(200, |s| s.parse().expect("ITERATIONS"));
    let base = args
        .next()
        .map_or_else(std::env::temp_dir, std::path::PathBuf::from);
    let dir = base.join(format!("phdb-readshapes-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the store directory");
    let (db, driver) = driver::Driver::open(
        &dir.join("shapes.phdb"),
        Options::default()
            .durability(Durability::Buffered)
            .block_cache(256 << 20),
    );
    // Table `l` first, compacted while it is the only table, so table `t` below is built
    // exactly as without it.
    let l = db
        .table("l")
        .unwrap()
        .family("f", Family::default().bloom_bits(10))
        .create_if_missing()
        .unwrap();
    for r in 0..ROWS {
        let key = row_key(r);
        let mut m = l.mutate(&key);
        for q in 0..CELLS {
            m = m.put("f", format!("q{q}").as_bytes(), &[7u8; 40]);
        }
        m.commit().unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default().max_versions(4).bloom_bits(10))
        .family("c", Family::counter().bloom_bits(10))
        .create_if_missing()
        .unwrap();
    let val = [7u8; 40];
    let write = |rows: std::ops::Range<u32>, incrs: u32| {
        for r in rows {
            let key = row_key(r);
            let mut m = t.mutate(&key);
            for q in 0..CELLS {
                m = m.put("f", format!("q{q}").as_bytes(), &val);
            }
            m.commit().unwrap();
            // Two more versions of `q0`.
            for v in 0..2u8 {
                t.mutate(&key).put("f", b"q0", &[v; 40]).commit().unwrap();
            }
            if r % 10 == 0 {
                for _ in 0..incrs {
                    t.mutate(&key).incr("c", b"n", 1).commit().unwrap();
                }
            }
        }
    };
    write(0..ROWS / 2, 3);
    db.flush().unwrap();
    write(ROWS / 2..ROWS, 3);
    // Two more increments of every counter, in the memtable.
    for r in (0..ROWS).step_by(10) {
        t.mutate(&row_key(r)).incr("c", b"n", 2).commit().unwrap();
    }
    warm(&t);
    let mut it = l.scan_prefix(b"").family("f").iter().unwrap();
    while let Some(r) = it.next_ref().unwrap() {
        std::hint::black_box(r.iter().count());
    }
    drop(it);
    driver.wait_idle();
    eprintln!("setup done");

    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut units = 0;
    // `SHAPE_SETUP_ONLY=1`: the setup alone, no measured iteration (the script checks that
    // callgrind then counts nothing inside the `shape_` functions).
    let iters = if std::env::var_os("SHAPE_SETUP_ONLY").is_some() {
        0
    } else {
        iters
    };
    units += driver::on_fresh_thread(|| {
        let mut units = 0;
        for _ in 0..iters {
            units += match shape.as_str() {
                "get-mem" => shape_get(&t, &mut rng, ROWS / 2),
                "get-sst" => shape_get(&t, &mut rng, 0),
                "get-miss" => shape_get_miss(&t, &mut rng),
                "row" => shape_row(&t, &mut rng),
                "scan" => shape_scan(&t, &mut rng, false),
                "rows" => shape_rows(&t, &mut rng),
                "short-scans" => shape_short_scans(&t, &mut rng),
                "level-scans" => shape_short_scans(&l, &mut rng),
                "scan-filtered" => shape_scan(&t, &mut rng, true),
                "versions" => shape_versions(&t, &mut rng),
                "counter" => shape_counter(&t, &mut rng),
                other => panic!("unknown shape {other}"),
            };
        }
        units
    });
    // For instruction counts (`scripts/instructions-per-cell.sh`): units read in all.
    eprintln!("units {units}");
    drop(t);
    drop(l);
    db.close().unwrap();
    driver.join();
    std::fs::remove_dir_all(&dir).ok();
}
