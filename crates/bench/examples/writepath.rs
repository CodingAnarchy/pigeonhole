//! A write-path microbenchmark (#287), one shape per run, for
//! `scripts/instructions-per-cell.sh`:
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example writepath -- SHAPE ITERATIONS DIR
//! ```
//!
//! | Shape | One iteration | Units (`units N` on stderr) |
//! |---|---|---|
//! | `commit-one` | 500 commits of one 100-byte cell | commits |
//! | `commit-sixteen` | 125 commits of 16 cells in one row | commits |
//! | `commit-overwrite` | 500 commits of one 100-byte cell over one of 2,000 existing rows (`ycsb-a`'s write) | commits |
//! | `commit-at` | 500 timestamped appends of one 100-byte cell to a new row of a family with a TTL (`time-series-ttl`'s write) | commits |
//! | `flush` | 2,000 entries flushed from the memtable to one SST | entries |
//! | `compact` | 2,000 entries compacted from 3 L0 SSTs into the last level | entries |
//!
//! Only the measured work runs inside functions named `shape_*`, each counting its thread's
//! work with a `Measured` guard (`support/measure.rs`); the commits a flush or compaction needs are
//! written first, outside them. The shards are application-owned (`support/shards.rs`), so
//! the commit, flush and compaction work they do is counted along with the caller's.
//!
//! On macOS the script counts whole processes at two iteration counts (`time -l`). For
//! `flush` and `compact` the setup commits grow with ITERATIONS too, so that difference
//! includes them: only callgrind measures those two shapes alone. The commit shapes are
//! clean either way.
#[path = "support/measure.rs"]
mod measure;
#[path = "support/shards.rs"]
mod shards;
use measure::Measured;
use shards::Shards;

use std::time::Duration;

use pigeonhole::{Family, Pigeonhole, Table};

const VALUE: [u8; 100] = [7; 100];

fn main() {
    let mut args = std::env::args().skip(1);
    let usage = "usage: writepath SHAPE ITERATIONS DIR";
    let shape = args.next().expect(usage);
    let iters: usize = args.next().expect(usage).parse().expect("ITERATIONS");
    let base = std::path::PathBuf::from(args.next().expect(usage));
    let dir = base.join(format!("phdb-writepath-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the store directory");
    let (db, shards) = Shards::open(&dir.join("w.phdb"));
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    let quals: Vec<Vec<u8>> = (0..16).map(|q| format!("q{q:02}").into_bytes()).collect();
    let rows = |prefix: &str, n: usize| -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| format!("{prefix}:{i:010}").into_bytes())
            .collect()
    };
    // `SHAPE_SETUP_ONLY=1`: the setup alone, without the measured work (the script checks
    // that callgrind then counts nothing inside the `shape_` functions).
    let setup_only = std::env::var_os("SHAPE_SETUP_ONLY").is_some();
    let measure = |f: &mut (dyn FnMut() + Send)| {
        if !setup_only {
            shards.measure(f);
        }
    };
    let units = match shape.as_str() {
        "commit-one" => {
            let rows = rows("one", 500 * iters);
            measure(&mut || shape_commit_one(&t, &rows));
            rows.len()
        }
        "commit-sixteen" => {
            let rows = rows("six", 125 * iters);
            measure(&mut || shape_commit_sixteen(&t, &rows, &quals));
            rows.len()
        }
        "commit-overwrite" => {
            // A fixed set of rows, written first; the measured commits overwrite them in a
            // scattered order, so each lands inside the memtable's existing keys.
            let set = rows("ow", 2_000);
            for row in &set {
                t.mutate(row).put("f", b"q", &VALUE).commit().unwrap();
            }
            let order: Vec<Vec<u8>> = (0..500 * iters)
                .map(|i| set[i * 7_919 % set.len()].clone())
                .collect();
            measure(&mut || shape_commit_one(&t, &order));
            order.len()
        }
        "commit-at" => {
            // A table of its own, so the other shapes' setup is unchanged. TTL, no FIFO
            // compaction: its expiry timer would fire on wall time.
            let m = db
                .table("m")
                .unwrap()
                .family("m", Family::default().ttl(Duration::from_secs(86_400)))
                .create_if_missing()
                .unwrap();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;
            // One entity's newest points, newest first in row order (as the bench's keys).
            let points: Vec<(Vec<u8>, u64)> = (0..500 * iters as u64)
                .map(|i| {
                    let ts = now - 1_000_000 + i;
                    (format!("ts:e0001:{:016x}", u64::MAX - ts).into_bytes(), ts)
                })
                .collect();
            measure(&mut || shape_commit_at(&m, &points));
            points.len()
        }
        "flush" => {
            let rows = rows("six", 125 * iters);
            commit_sixteen(&t, &rows, &quals);
            measure(&mut || shape_flush(&db));
            rows.len() * 16
        }
        "compact" => {
            // Three L0 SSTs, below the compaction trigger (4): nothing compacts before the
            // measured compaction.
            let rows = rows("six", 125 * iters);
            for part in rows.chunks(rows.len().div_ceil(3)) {
                commit_sixteen(&t, part, &quals);
                db.flush().unwrap();
            }
            measure(&mut || shape_compact(&db));
            rows.len() * 16
        }
        other => panic!(
            "unknown shape {other}: commit-one, commit-sixteen, commit-overwrite, commit-at, \
             flush or compact"
        ),
    };
    eprintln!("units {}", if setup_only { 0 } else { units });
    drop(t);
    db.close().unwrap();
    shards.join();
    std::fs::remove_dir_all(&dir).ok();
}

#[inline(never)]
fn shape_commit_one(t: &Table, rows: &[Vec<u8>]) {
    let _measured = Measured::start();
    for row in rows {
        t.mutate(row).put("f", b"q", &VALUE).commit().unwrap();
    }
}

#[inline(never)]
fn shape_commit_at(m: &Table, points: &[(Vec<u8>, u64)]) {
    let _measured = Measured::start();
    for (row, ts) in points {
        m.mutate(row)
            .put_at("m", b"v", *ts, &VALUE)
            .commit()
            .unwrap();
    }
}

#[inline(never)]
fn shape_commit_sixteen(t: &Table, rows: &[Vec<u8>], quals: &[Vec<u8>]) {
    let _measured = Measured::start();
    commit_sixteen(t, rows, quals);
}

fn commit_sixteen(t: &Table, rows: &[Vec<u8>], quals: &[Vec<u8>]) {
    for row in rows {
        let mut m = t.mutate(row);
        for q in quals {
            m = m.put("f", q, &VALUE);
        }
        m.commit().unwrap();
    }
}

#[inline(never)]
fn shape_flush(db: &Pigeonhole) {
    let _measured = Measured::start();
    db.flush().unwrap();
}

#[inline(never)]
fn shape_compact(db: &Pigeonhole) {
    let _measured = Measured::start();
    db.compact().unwrap();
}
