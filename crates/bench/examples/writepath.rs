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
//! | `commit-put-previous` | `commit-overwrite`'s commits through `commit_returning`, each asking for the cell's previous value (get-and-set, D314 amendment 1): a point read per commit | commits |
//! | `commit-if-returning` | `commit-overwrite`'s commits through `commit_if_returning` on the cell existing, which always holds (compare-and-set-then-read, D314 amendment 2): the condition's read, and the cell read again as of the commit | commits |
//! | `commit-async` | 500 commits of one 100-byte cell through `commit_async`, each awaited (an uncontended async commit, D210) | commits |
//! | `commit-at` | 500 timestamped appends of one 100-byte cell to a new row of a family with a TTL (`time-series-ttl`'s write) | commits |
//! | `commit-incr` | 500 blind `incr`s of one counter, its base in an SST | commits |
//! | `commit-incr-returning` | the same through `commit_returning` (D314): the fold from the active memtable, a checkpoint every 8 | commits |
//! | `commit-incr-returning-spread` | 500 `commit_returning` increments of 500 counters, each once, their bases in an SST: every fold is a full read, then a checkpoint | commits |
//! | `flush` | 2,000 entries flushed from the memtable to one SST | entries |
//! | `compact` | 2,000 entries compacted from 3 L0 SSTs into the last level, with inline I/O and a stepped clock (`support/inline_io.rs`, #794) | entries |
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
#[path = "support/inline_io.rs"]
mod inline_io;
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
    measure::fix_hash_seeds();
    let mut args = std::env::args().skip(1);
    let usage = "usage: writepath SHAPE ITERATIONS DIR";
    let shape = args.next().expect(usage);
    let iters: usize = args.next().expect(usage).parse().expect("ITERATIONS");
    let base = std::path::PathBuf::from(args.next().expect(usage));
    let dir = base.join(format!("phdb-writepath-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the store directory");
    // `compact` reads its input SSTs ahead and runs as a background task: on the default
    // backend, which thread completed a read and how many slices the task took depended on
    // timing (#794). Inline I/O and a stepped clock keep its count the same on every run.
    let vfs = (shape == "compact").then(inline_io::InlineVfs::vfs);
    let (db, shards) = Shards::open(&dir.join("w.phdb"), vfs);
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
        "commit-async" => {
            let rows = rows("asy", 500 * iters);
            measure(&mut || shape_commit_async(&t, &rows));
            rows.len()
        }
        "commit-sixteen" => {
            let rows = rows("six", 125 * iters);
            measure(&mut || shape_commit_sixteen(&t, &rows, &quals));
            rows.len()
        }
        "commit-overwrite" | "commit-put-previous" | "commit-if-returning" => {
            // A fixed set of rows, written first; the measured commits overwrite them in a
            // scattered order, so each lands inside the memtable's existing keys.
            let set = rows("ow", 2_000);
            for row in &set {
                t.mutate(row).put("f", b"q", &VALUE).commit().unwrap();
            }
            let order: Vec<Vec<u8>> = (0..500 * iters)
                .map(|i| set[i * 7_919 % set.len()].clone())
                .collect();
            if shape == "commit-put-previous" {
                measure(&mut || shape_commit_put_previous(&t, &order));
            } else if shape == "commit-if-returning" {
                measure(&mut || shape_commit_if_returning(&t, &order));
            } else {
                measure(&mut || shape_commit_one(&t, &order));
            }
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
        "commit-incr" | "commit-incr-returning" | "commit-incr-returning-spread" => {
            // A counter table of its own (D314), so the other shapes' setup is unchanged. Each
            // cell is incremented once and flushed first, so its base is in an SST.
            let k = db
                .table("k")
                .unwrap()
                .family("c", Family::counter())
                .create_if_missing()
                .unwrap();
            let spread = shape.ends_with("-spread");
            let cells = if spread {
                rows("sp", 500 * iters)
            } else {
                rows("hot", 1)
            };
            for row in &cells {
                k.mutate(row).incr("c", b"n", 1).commit().unwrap();
            }
            db.flush().unwrap();
            let order: Vec<Vec<u8>> = (0..500 * iters)
                .map(|i| cells[i % cells.len()].clone())
                .collect();
            if shape.starts_with("commit-incr-returning") {
                measure(&mut || shape_commit_incr_returning(&k, &order));
            } else {
                measure(&mut || shape_commit_incr(&k, &order));
            }
            order.len()
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
            let mut ts = 1_700_000_000_000_000;
            for part in rows.chunks(rows.len().div_ceil(3)) {
                commit_sixteen_at(&t, part, &quals, &mut ts);
                db.flush().unwrap();
            }
            measure(&mut || shape_compact(&db));
            rows.len() * 16
        }
        other => panic!(
            "unknown shape {other}: commit-one, commit-sixteen, commit-overwrite, \
             commit-put-previous, commit-if-returning, commit-at, \
             commit-async, commit-incr, commit-incr-returning, commit-incr-returning-spread, \
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
fn shape_commit_put_previous(t: &Table, rows: &[Vec<u8>]) {
    let _measured = Measured::start();
    let mut n = 0usize;
    for row in rows {
        let r = t
            .mutate(row)
            .put("f", b"q", &VALUE)
            .return_previous("f", b"q")
            .commit_returning()
            .unwrap();
        n += r
            .previous("f", b"q")
            .unwrap()
            .map_or(0, |c| c.value().len());
    }
    std::hint::black_box(n);
}

#[inline(never)]
fn shape_commit_if_returning(t: &Table, rows: &[Vec<u8>]) {
    let exists = pigeonhole::Condition::Exists {
        family: "f".into(),
        qualifier: b"q".to_vec(),
    };
    let _measured = Measured::start();
    let mut n = 0usize;
    for row in rows {
        match t
            .mutate(row)
            .put("f", b"q", &VALUE)
            .commit_if_returning(&exists)
            .unwrap()
        {
            pigeonhole::Checked::Applied(r) => {
                n += r.current("f", b"q").unwrap().map_or(0, |c| c.value().len());
            }
            pigeonhole::Checked::NotApplied(_) => unreachable!("every row was written"),
        }
    }
    std::hint::black_box(n);
}

#[inline(never)]
fn shape_commit_async(t: &Table, rows: &[Vec<u8>]) {
    let _measured = Measured::start();
    for row in rows {
        pigeonhole::doc_support::block_on(t.mutate(row).put("f", b"q", &VALUE).commit_async())
            .unwrap();
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
fn shape_commit_incr(k: &Table, rows: &[Vec<u8>]) {
    let _measured = Measured::start();
    for row in rows {
        k.mutate(row).incr("c", b"n", 1).commit().unwrap();
    }
}

#[inline(never)]
fn shape_commit_incr_returning(k: &Table, rows: &[Vec<u8>]) {
    let _measured = Measured::start();
    let mut sum = 0i64;
    for row in rows {
        let r = k.mutate(row).incr("c", b"n", 1).commit_returning().unwrap();
        sum = sum.wrapping_add(r.value("c", b"n").unwrap().unwrap());
    }
    std::hint::black_box(sum);
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

/// [`commit_sixteen`] with explicit timestamps from `ts` on, one per row: the setup of a
/// shape whose measured work reads what it wrote. Commit timestamps come from the clock, and
/// their bytes in the keys set how well the blocks compress: the `compact` shape varied by
/// 1.4% with them, all in decompressing the same 66 blocks.
fn commit_sixteen_at(t: &Table, rows: &[Vec<u8>], quals: &[Vec<u8>], ts: &mut u64) {
    for row in rows {
        let mut m = t.mutate(row);
        for q in quals {
            m = m.put_at("f", q, *ts, &VALUE);
        }
        *ts += 1;
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
