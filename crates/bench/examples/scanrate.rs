//! Ordered row scan, single family, from cache (spec Goals: > 1 GB/s decoded per core, #29).
//! One thread scans a whole table that the block cache (or the memtable) holds. Decoded bytes
//! are the row key, qualifier and value of every cell the scan returns.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example scanrate -- rate [SHAPE[:PLACE]]... [PASSES]
//! cargo run --release -p pigeonhole-bench --example scanrate -- SHAPE [ITERATIONS] [DIR]
//! ```
//!
//! `rate` times full scans and prints one JSON line per shape: decoded bytes, cells, and the
//! best and median pass in GB/s (10^9 bytes) and cells/s. Shapes: `narrow` (200,000 rows of
//! 8 cells of 100 B), `wide` (2,000 rows of 1,000 cells of 100 B), `small` (400,000 rows of
//! 8 cells of 16 B), `large` (50,000 rows of 4 cells of 1 KiB); places: `sst` (flushed and
//! compacted, every block in the block cache; the default) or `mem` (in the memtable). With
//! no shape it runs every shape in both places; PASSES defaults to 5.
//!
//! Otherwise it is a shape binary for `scripts/instructions-per-cell.sh` (one measured
//! `shape_scan` per iteration, `units N` on stderr: one per returned cell), with the same
//! shapes at a size callgrind sets up quickly, compacted: `scan-narrow` (5,000 rows of
//! 8 cells of 100 B), `scan-small` (5,000 rows of 8 cells of 16 B) and `scan-wide` (40 rows
//! of 1,000 cells of 100 B). Each iteration scans the whole table once. Unlike
//! `readshapes`'s `scan` (memtable and SST merged, old versions skipped), these read one
//! source and one version per column: the common case of a hot, compacted table.
//!
//! The store is built deterministically (`support/driver.rs`): one application-owned shard,
//! only the setup's own flush and compaction, and the measured scans start with the shard
//! idle, on a fresh thread.
#[path = "support/driver.rs"]
mod driver;
#[path = "support/measure.rs"]
mod measure;
use measure::Measured;

use std::ops::Bound;
use std::path::Path;
use std::time::Instant;

use pigeonhole::{Durability, Family, Options, Pigeonhole, Table};

/// The qualifier selection a scan pushes down (D22).
#[derive(Clone, Copy)]
enum Filter {
    None,
    /// Qualifiers starting with these bytes.
    Prefix(&'static [u8]),
    /// Qualifiers below these bytes.
    Below(&'static [u8]),
}

#[derive(Clone, Copy)]
struct Shape {
    name: &'static str,
    rows: u32,
    cells: u32,
    value: usize,
    filter: Filter,
}

const RATE_SHAPES: [Shape; 9] = [
    Shape {
        name: "narrow",
        rows: 200_000,
        cells: 8,
        value: 100,
        filter: Filter::None,
    },
    Shape {
        name: "wide",
        rows: 2_000,
        cells: 1_000,
        value: 100,
        filter: Filter::None,
    },
    Shape {
        name: "small",
        rows: 400_000,
        cells: 8,
        value: 16,
        filter: Filter::None,
    },
    Shape {
        name: "large",
        rows: 50_000,
        cells: 4,
        value: 1024,
        filter: Filter::None,
    },
    Shape {
        name: "narrow-q1",
        rows: 200_000,
        cells: 8,
        value: 100,
        filter: Filter::Prefix(b"q0003"),
    },
    Shape {
        name: "narrow-half",
        rows: 200_000,
        cells: 8,
        value: 100,
        filter: Filter::Below(b"q0004"),
    },
    Shape {
        name: "wide-q10",
        rows: 2_000,
        cells: 1_000,
        value: 100,
        filter: Filter::Prefix(b"q01"),
    },
    Shape {
        name: "wide-q1",
        rows: 2_000,
        cells: 1_000,
        value: 100,
        filter: Filter::Prefix(b"q0500"),
    },
    Shape {
        name: "wide-half",
        rows: 2_000,
        cells: 1_000,
        value: 100,
        filter: Filter::Below(b"q0500"),
    },
];

const COUNT_SHAPES: [Shape; 7] = [
    Shape {
        name: "scan-narrow",
        rows: 5_000,
        cells: 8,
        value: 100,
        filter: Filter::None,
    },
    Shape {
        name: "scan-small",
        rows: 5_000,
        cells: 8,
        value: 16,
        filter: Filter::None,
    },
    Shape {
        name: "scan-wide",
        rows: 40,
        cells: 1_000,
        value: 100,
        filter: Filter::None,
    },
    Shape {
        name: "scan-narrow-q1",
        rows: 5_000,
        cells: 8,
        value: 100,
        filter: Filter::Prefix(b"q0003"),
    },
    Shape {
        name: "scan-narrow-half",
        rows: 5_000,
        cells: 8,
        value: 100,
        filter: Filter::Below(b"q0004"),
    },
    Shape {
        name: "scan-wide-q10",
        rows: 40,
        cells: 1_000,
        value: 100,
        filter: Filter::Prefix(b"q01"),
    },
    Shape {
        name: "scan-wide-q1",
        rows: 40,
        cells: 1_000,
        value: 100,
        filter: Filter::Prefix(b"q0500"),
    },
];

/// One full scan with `filter` pushed down: decoded bytes and cells.
fn scan(t: &Table, filter: Filter) -> (u64, u64) {
    let (mut bytes, mut cells) = (0u64, 0u64);
    let mut scan = t.scan_prefix(b"").family("f");
    scan = match filter {
        Filter::None => scan,
        Filter::Prefix(p) => scan.qualifier_prefix(p),
        Filter::Below(b) => scan.qualifier_bounds(Bound::Unbounded, Bound::Excluded(b)),
    };
    let mut it = scan.iter().unwrap();
    while let Some(r) = it.next_ref().unwrap() {
        let key = r.key().len() as u64;
        for c in r.iter() {
            bytes += key + c.qualifier.len() as u64 + c.cell.value().len() as u64;
            cells += 1;
        }
    }
    std::hint::black_box((bytes, cells))
}

/// One measured full scan (callgrind counts only this); its returned cells.
#[inline(never)]
fn shape_scan(t: &Table, filter: Filter) -> usize {
    let _measured = Measured::start();
    scan(t, filter).1 as usize
}

/// A store holding `shape` in table `t`, family `f` (one version per column): compacted when
/// `compact`, else in the memtable.
fn build(dir: &Path, shape: Shape, compact: bool) -> (Pigeonhole, driver::Driver, Table) {
    let (db, driver) = driver::Driver::open(
        &dir.join("scan.phdb"),
        Options::default()
            .durability(Durability::Buffered)
            .block_cache(2 << 30),
    );
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    let value = vec![7u8; shape.value];
    let quals: Vec<Vec<u8>> = (0..shape.cells)
        .map(|q| format!("q{q:04}").into_bytes())
        .collect();
    for r in 0..shape.rows {
        let key = format!("r:{r:08}");
        let mut m = t.mutate(key.as_bytes());
        for q in &quals {
            m = m.put("f", q, &value);
        }
        m.commit().unwrap();
    }
    if compact {
        db.flush().unwrap();
        db.compact().unwrap();
    }
    driver.wait_idle();
    scan(&t, Filter::None); // every block in the cache
    driver.wait_idle();
    (db, driver, t)
}

fn close(db: Pigeonhole, driver: driver::Driver, t: Table, dir: &Path) {
    drop(t);
    db.close().unwrap();
    driver.join();
    std::fs::remove_dir_all(dir).ok();
}

fn rate(shape: Shape, place: &str, passes: usize) {
    let dir = std::env::temp_dir().join(format!(
        "phdb-scanrate-{}-{}",
        std::process::id(),
        shape.name
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let (db, driver, t) = build(&dir, shape, place == "sst");
    let (mut secs, (bytes, cells)) = driver::on_fresh_thread(|| {
        let mut secs = Vec::with_capacity(passes);
        let mut out = scan(&t, shape.filter);
        for _ in 0..passes {
            let start = Instant::now();
            out = scan(&t, shape.filter);
            secs.push(start.elapsed().as_secs_f64());
        }
        (secs, out)
    });
    secs.sort_by(f64::total_cmp);
    let (best, median) = (secs[0], secs[secs.len() / 2]);
    println!(
        "{{\"shape\":\"{}\",\"place\":\"{place}\",\"rows\":{},\"cells_per_row\":{},\
         \"value_len\":{},\"decoded_bytes\":{bytes},\"cells\":{cells},\"passes\":{passes},\
         \"best_gb_s\":{:.3},\"median_gb_s\":{:.3},\"median_cells_s\":{:.0},\"median_ms\":{:.2}}}",
        shape.name,
        shape.rows,
        shape.cells,
        shape.value,
        bytes as f64 / best / 1e9,
        bytes as f64 / median / 1e9,
        cells as f64 / median,
        median * 1e3,
    );
    close(db, driver, t, &dir);
}

fn main() {
    let mut args = std::env::args().skip(1);
    let first = args.next().expect("rate or SHAPE");
    if first == "rate" {
        let mut passes = 5;
        let mut picks = Vec::new();
        for a in args {
            match a.parse() {
                Ok(n) => passes = n,
                Err(_) => picks.push(a),
            }
        }
        if picks.is_empty() {
            picks = RATE_SHAPES
                .iter()
                .flat_map(|s| [format!("{}:sst", s.name), format!("{}:mem", s.name)])
                .collect();
        }
        for p in picks {
            let (name, place) = p.split_once(':').unwrap_or((&p, "sst"));
            assert!(place == "sst" || place == "mem", "unknown place {place}");
            let shape = RATE_SHAPES
                .iter()
                .find(|s| s.name == name)
                .unwrap_or_else(|| panic!("unknown shape {name}"));
            rate(*shape, place, passes.max(1));
        }
        return;
    }
    let shape = *COUNT_SHAPES
        .iter()
        .find(|s| s.name == first)
        .unwrap_or_else(|| panic!("unknown shape {first}"));
    let iters: usize = args.next().map_or(4, |s| s.parse().expect("ITERATIONS"));
    let base = args
        .next()
        .map_or_else(std::env::temp_dir, std::path::PathBuf::from);
    let dir = base.join(format!("phdb-scanrate-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the store directory");
    let (db, driver, t) = build(&dir, shape, true);
    eprintln!("setup done");
    // `SHAPE_SETUP_ONLY=1`: the setup alone (the script checks callgrind then counts nothing).
    let iters = if std::env::var_os("SHAPE_SETUP_ONLY").is_some() {
        0
    } else {
        iters
    };
    let units = driver::on_fresh_thread(|| {
        (0..iters)
            .map(|_| shape_scan(&t, shape.filter))
            .sum::<usize>()
    });
    eprintln!("units {units}");
    close(db, driver, t, &dir);
}
