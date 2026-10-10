//! YCSB-style shapes for `scripts/instructions-per-cell.sh` (Phase 3 latency gate): the
//! per-operation cost of `phdb-bench`'s `ycsb-c` and `ycsb-a` workloads, on a store laid
//! out the way a running YCSB store is.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example ycsb -- SHAPE ITERATIONS DIR
//! ```
//!
//! | Shape | One iteration | Units (`units N` on stderr) |
//! |---|---|---|
//! | `ycsb-c` | 200 YCSB-C reads: all ten fields of a Zipfian row | operations |
//! | `ycsb-a` | 200 YCSB-A operations: half reads as above, half one-field updates (`Buffered`) | operations |
//!
//! The store: the bench's `ycsb` family (`max_versions(1)`, 10-bit bloom), loaded with the
//! workload's 20,000 records of ten 100-byte fields, flushed and compacted into the last
//! level. Then the first 20,000 YCSB-A operations run and are flushed to an L0 SST, and the
//! next 10,000 stay in the memtable, so a read merges the memtable, L0 and the last level as
//! in a running store. Row reads of every record, in key order, warm the block cache.
//!
//! The shapes are deterministic: every write takes an explicit timestamp from a counter, and
//! the warm-up reads in a fixed order (see `WRITE_TS` and the warm-up). Two runs of one binary
//! count within about 0.002%; before, they differed by up to 1%. The operations come from
//! the bench's own generator (`pigeonhole_bench::Workload`, seed `0x5EED`), the same
//! streams `phdb-bench` runs.
//!
//! The measured operations cycle through a fixed pool of 20,000 generated in the setup, so
//! the setup does not grow with ITERATIONS (the macOS count takes a difference of two runs).
//! Only the measured work runs inside functions named `shape_*`, counted with a `Measured`
//! guard (`support/measure.rs`), and the shard's side of each update is counted with it
//! (`support/shards.rs`).
#[path = "support/measure.rs"]
mod measure;
#[path = "support/shards.rs"]
mod shards;
use measure::Measured;
use shards::Shards;

use std::sync::atomic::{AtomicU64, Ordering};

use pigeonhole::{Family, Table};
use pigeonhole_bench::{BenchOp, Workload, WorkloadConfig, WorkloadKind};

const RECORDS: u64 = 20_000;
/// YCSB-A operations run before the flush to L0, then after it (left in the memtable).
const WARM_FLUSHED: usize = 20_000;
const WARM_MEMTABLE: usize = 10_000;
/// Measured operations the shapes cycle through.
const POOL: usize = 20_000;
const PER_ITERATION: usize = 200;

fn config(kind: WorkloadKind, operations: usize) -> WorkloadConfig {
    WorkloadConfig {
        records: RECORDS,
        operations: operations as u64,
        // Only the time-series workload reads the clock; fixed anyway, for repeatability.
        epoch_micros: 1_700_000_000_000_000,
        ..WorkloadConfig::small(kind)
    }
}

/// Every write takes an explicit timestamp from this counter, so the store's bytes are the
/// same on every run: with commit timestamps from the clock, adjacent versions of a column
/// share a varying number of key bytes, block cuts move, and the reads' instruction counts
/// with them (about 0.2% for `ycsb-c`, 0.5% for `ycsb-a`). The counter only rises, so each
/// write is the newest version, as a commit timestamp would make it. The difference from
/// `phdb-bench`'s puts: the commit takes no clock reading for its timestamp.
static WRITE_TS: AtomicU64 = AtomicU64::new(1_700_000_000_000_000);

/// One operation through the public API, as `phdb-bench`'s Pigeonhole runner does it,
/// except that a put takes the next [`WRITE_TS`]. Returns the cells read.
fn execute(t: &Table, op: &BenchOp) -> usize {
    match op {
        BenchOp::GetRow { row, family } => {
            let row = t.row(row).family(family).read().unwrap();
            let mut n = 0;
            for e in row.iter().flat_map(|r| r.iter()) {
                std::hint::black_box(e.cell.value());
                n += 1;
            }
            n
        }
        BenchOp::Put { row, family, cells } => {
            let mut m = t.mutate(row);
            for (q, v) in cells {
                m = m.put_at(family, q, WRITE_TS.fetch_add(1, Ordering::Relaxed), v);
            }
            m.commit().unwrap();
            0
        }
        other => panic!("not a YCSB-A or YCSB-C operation: {other:?}"),
    }
}

#[inline(never)]
fn shape_ycsb(t: &Table, ops: &[BenchOp]) -> usize {
    let _measured = Measured::start();
    let mut cells = 0;
    for op in ops {
        cells += execute(t, op);
    }
    std::hint::black_box(cells);
    ops.len()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let usage = "usage: ycsb SHAPE ITERATIONS DIR";
    let shape = args.next().expect(usage);
    let iters: usize = args.next().expect(usage).parse().expect("ITERATIONS");
    let base = std::path::PathBuf::from(args.next().expect(usage));
    let kind = match shape.as_str() {
        "ycsb-c" => WorkloadKind::YcsbC,
        "ycsb-a" => WorkloadKind::YcsbA,
        other => panic!("unknown shape {other}: ycsb-c or ycsb-a"),
    };
    let dir = base.join(format!("phdb-ycsb-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the store directory");
    let (db, shards) = Shards::open(&dir.join("y.phdb"));
    let t = db
        .table("bench")
        .unwrap()
        .family(
            pigeonhole_bench::YCSB_FAMILY,
            Family::default().max_versions(1).bloom_bits(10),
        )
        .create_if_missing()
        .unwrap();

    let mut a = Workload::new(config(
        WorkloadKind::YcsbA,
        WARM_FLUSHED + WARM_MEMTABLE + POOL,
    ));
    for op in a.load_ops() {
        execute(&t, &op);
    }
    db.flush().unwrap();
    db.compact().unwrap();
    let mut stream = a.run_ops();
    for op in stream.by_ref().take(WARM_FLUSHED) {
        execute(&t, &op);
    }
    db.flush().unwrap();
    for op in stream.by_ref().take(WARM_MEMTABLE) {
        execute(&t, &op);
    }
    let pool: Vec<BenchOp> = match kind {
        WorkloadKind::YcsbA => stream.collect(),
        _ => Workload::new(config(kind, POOL)).run_ops().collect(),
    };
    assert_eq!(pool.len(), POOL);
    // Warm the block cache with row reads in key order. A scan would admit blocks through
    // readahead completions, whose order varies with timing, and a hash table's probe
    // lengths depend on the order its keys arrived (the setup's compaction does the same, so
    // its blocks are not the ones the reads look up).
    for op in Workload::new(config(WorkloadKind::YcsbC, 0)).load_ops() {
        if let BenchOp::Put { row, family, .. } = op {
            execute(&t, &BenchOp::GetRow { row, family });
        }
    }
    eprintln!("setup done");

    // `SHAPE_SETUP_ONLY=1`: the setup alone, without the measured work (the script checks
    // that callgrind then counts nothing inside the `shape_` functions).
    let mut units = 0;
    if std::env::var_os("SHAPE_SETUP_ONLY").is_none() {
        let chunks: Vec<&[BenchOp]> = pool.chunks(PER_ITERATION).cycle().take(iters).collect();
        shards.measure(&mut || {
            for chunk in &chunks {
                units += shape_ycsb(&t, chunk);
            }
        });
    }
    eprintln!("units {units}");
    drop(t);
    db.close().unwrap();
    shards.join();
    std::fs::remove_dir_all(&dir).ok();
}
