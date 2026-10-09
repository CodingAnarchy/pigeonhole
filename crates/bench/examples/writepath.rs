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
//! | `flush` | 2,000 entries flushed from the memtable to one SST | entries |
//! | `compact` | 2,000 entries compacted from 3 L0 SSTs into the last level | entries |
//!
//! Only the measured work runs inside functions named `shape_*` (callgrind counts them
//! with `--toggle-collect='*shape_*'`); the commits a flush or compaction needs are
//! written first, outside them. The shards are application-owned and each runs on a
//! thread of its own, inside `shape_run_shard` while a shape is measured, so the commit,
//! flush and compaction work the shards do is counted along with the caller's. One shard,
//! tablet changes off and a memtable that holds every write keep the work deterministic.
//!
//! On macOS the script counts whole processes at two iteration counts (`time -l`). For
//! `flush` and `compact` the setup commits grow with ITERATIONS too, so that difference
//! includes them: only callgrind measures those two shapes alone. The commit shapes are
//! clean either way.
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use pigeonhole::{Durability, Family, Options, Pigeonhole, Shard, Table};

const VALUE: [u8; 100] = [7; 100];

fn main() {
    let mut args = std::env::args().skip(1);
    let usage = "usage: writepath SHAPE ITERATIONS DIR";
    let shape = args.next().expect(usage);
    let iters: usize = args.next().expect(usage).parse().expect("ITERATIONS");
    let base = std::path::PathBuf::from(args.next().expect(usage));
    let dir = base.join(format!("phdb-writepath-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the store directory");
    let measuring = Arc::new(AtomicBool::new(false));
    let (db, drivers) = open(&dir.join("w.phdb"), &measuring);
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
    let measure = |f: &mut dyn FnMut()| {
        if setup_only {
            return;
        }
        measuring.store(true, Ordering::Release);
        f();
        measuring.store(false, Ordering::Release);
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
        other => panic!("unknown shape {other}: commit-one, commit-sixteen, flush or compact"),
    };
    eprintln!("units {}", if setup_only { 0 } else { units });
    drop(t);
    db.close().unwrap();
    for d in drivers {
        d.join().unwrap();
    }
    std::fs::remove_dir_all(&dir).ok();
}

/// Opens with application-owned shards, each driven by a thread of its own.
fn open(path: &Path, measuring: &Arc<AtomicBool>) -> (Pigeonhole, Vec<thread::JoinHandle<()>>) {
    let (db, shards) = Pigeonhole::open_application_owned(
        path,
        Options::default()
            .shards(1)
            .tablet_changes(false)
            .durability(Durability::Buffered)
            .memtable_budget(1 << 30)
            .block_cache(256 << 20),
    )
    .expect("open");
    let drivers = shards
        .into_iter()
        .map(|shard| {
            let measuring = Arc::clone(measuring);
            thread::spawn(move || drive(shard, &measuring))
        })
        .collect();
    (db, drivers)
}

/// A shard's loop (see `Pigeonhole::open_application_owned`), inside `shape_run_shard`
/// while a shape is measured.
fn drive(mut shard: Shard, measuring: &AtomicBool) {
    let me = thread::current();
    shard.set_wakeup(Box::new(move || me.unpark()));
    loop {
        if measuring.load(Ordering::Acquire) {
            shape_run_shard(&mut shard);
        } else {
            run_shard(&mut shard);
        }
        if let Some(closed) = shard.closed() {
            closed.unwrap();
            return;
        }
        match shard.next_wakeup() {
            Some(due) => thread::park_timeout(due),
            None => thread::park(),
        }
    }
}

// The two loops must not compile to the same code: rustc merges identical functions, and
// then the setup's shard work would be counted as the shape's. They differ in their slice
// budget, which changes nothing measured.
#[inline(never)]
fn shape_run_shard(shard: &mut Shard) {
    while shard.run_once(Duration::from_micros(200)) {}
}

#[inline(never)]
fn run_shard(shard: &mut Shard) {
    while shard.run_once(Duration::from_micros(1000)) {}
}

#[inline(never)]
fn shape_commit_one(t: &Table, rows: &[Vec<u8>]) {
    for row in rows {
        t.mutate(row).put("f", b"q", &VALUE).commit().unwrap();
    }
}

#[inline(never)]
fn shape_commit_sixteen(t: &Table, rows: &[Vec<u8>], quals: &[Vec<u8>]) {
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
    db.flush().unwrap();
}

#[inline(never)]
fn shape_compact(db: &Pigeonhole) {
    db.compact().unwrap();
}
