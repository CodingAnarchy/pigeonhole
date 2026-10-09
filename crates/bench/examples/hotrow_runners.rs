//! hotrow's data shape through a bench runner (`pigeonhole`, `sqlite`), to count what each
//! store spends per cell on the same row read and scan (#287): one hot row of about 3,500
//! cells whose hot qualifiers were overwritten many times, among 10,000 cold rows of 20 cells.
//! Each iteration reads the hot row (`GetRow`) and scans 10 rows through it (`Scan`).
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --features sqlite --example hotrow_runners -- \
//!     sqlite|pigeonhole [ITERATIONS] [DIR]
//! ```
//!
//! Run it at two iteration counts under `/usr/bin/time -l` (macOS) or `perf stat` (Linux) and
//! divide the difference in instructions retired by the difference in iterations: that
//! removes the setup.
use std::time::Instant;

use pigeonhole_bench::{BenchOp, PigeonholeRunner, Runner, SPARSE_FAMILY, SqliteRunner};

fn main() {
    let mut args = std::env::args().skip(1);
    let store = args.next().expect("sqlite or pigeonhole");
    let iters: usize = args.next().map_or(2000, |s| s.parse().expect("ITERATIONS"));
    let base = args
        .next()
        .map_or_else(std::env::temp_dir, std::path::PathBuf::from);
    let dir = base.join(format!("phdb-hotrow-{store}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the store directory");
    let mut runner: Box<dyn Runner> = match store.as_str() {
        "sqlite" => Box::new(SqliteRunner::default()),
        "pigeonhole" => Box::new(PigeonholeRunner::default()),
        other => panic!("unknown store {other}"),
    };
    runner.open(&dir).expect("open");
    let val = vec![7u8; 100];
    let put = |runner: &mut Box<dyn Runner>, row: &[u8], quals: Vec<u64>| {
        let cells = quals
            .into_iter()
            .map(|q| (format!("q{q:05}").into_bytes(), val.clone()))
            .collect();
        runner
            .execute(&BenchOp::Put {
                row: row.to_vec(),
                family: SPARSE_FAMILY,
                cells,
            })
            .expect("put");
    };
    for r in 0..10_000u64 {
        let quals = (0..20u64).map(|q| (r * 7 + q * 13) % 10_000).collect();
        put(&mut runner, format!("h:{r:05}").as_bytes(), quals);
    }
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    let mut rnd = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let hot = b"h:05000".to_vec();
    for _ in 0..20_000 {
        let quals = (0..2)
            .map(|_| {
                if rnd().is_multiple_of(2) {
                    rnd() % 50
                } else {
                    rnd() % 3500
                }
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        put(&mut runner, &hot, quals);
    }
    eprintln!("setup done");
    let get_row = BenchOp::GetRow {
        row: hot.clone(),
        family: SPARSE_FAMILY,
    };
    let scan = BenchOp::Scan {
        start: b"h:04995".to_vec(),
        len: 10,
    };
    let t0 = Instant::now();
    for _ in 0..iters {
        runner.execute(&get_row).expect("row read");
        runner.execute(&scan).expect("scan");
    }
    let per = t0.elapsed() / iters as u32;
    println!("{store}: {iters} iterations, {per:?} each");
    runner.close().expect("close");
    std::fs::remove_dir_all(&dir).ok();
}
