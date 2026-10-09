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
//! Only the measured work runs inside functions named `shape_*`, each counting its thread's
//! work with a `Measured` guard (`support/measure.rs`); the commits a flush or compaction needs are
//! written first, outside them. The shards are application-owned and each runs on a
//! thread of its own, inside `shape_run_shard` while a shape is measured, so the commit,
//! flush and compaction work the shards do is counted along with the caller's. One shard,
//! tablet changes off and a memtable that holds every write keep the work deterministic.
//!
//! On macOS the script counts whole processes at two iteration counts (`time -l`). For
//! `flush` and `compact` the setup commits grow with ITERATIONS too, so that difference
//! includes them: only callgrind measures those two shapes alone. The commit shapes are
//! clean either way.
#[path = "support/measure.rs"]
mod measure;
use measure::Measured;

use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
    let (db, drivers, ctl) = open(&dir.join("w.phdb"));
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
    // The measured work starts and ends with every shard idle: what the setup left running
    // finishes outside `shape_run_shard`, and what the measured work leaves running (the
    // shard's side of the last commit, say) finishes inside it, before the close.
    //
    // The measured work runs on threads spawned for it, the shards' side and the caller's:
    // glibc gives a new thread its own allocator arena and cache, so its allocations do not
    // depend on the heap the setup left, which varies with thread timing (perf287's #354
    // found up to 3%). The setup's shard threads stay alive meanwhile, so a measured thread
    // cannot take over one of their arenas.
    let measure = |f: &mut (dyn FnMut() + Send)| {
        if setup_only {
            return;
        }
        ctl.wait_idle();
        ctl.measuring.store(true, Ordering::Release);
        ctl.wake();
        // Every shard is with its measured thread before the measured work starts.
        while ctl.handed.load(Ordering::Acquire) < ctl.shards {
            thread::yield_now();
        }
        thread::scope(|s| s.spawn(f).join().expect("the measured thread panicked"));
        ctl.wait_idle();
        ctl.measuring.store(false, Ordering::Release);
        ctl.wake();
        while ctl.handed.load(Ordering::Acquire) > 0 {
            thread::yield_now();
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

/// What the shard threads share with the main thread.
#[derive(Default)]
struct Ctl {
    /// Whether a shape is being measured: each shard is then driven, in `shape_run_shard`,
    /// by a thread spawned for the measurement.
    measuring: AtomicBool,
    /// Shard threads running (not parked).
    busy: AtomicUsize,
    /// The number of shards.
    shards: usize,
    /// Shards with their measured thread.
    handed: AtomicUsize,
    /// The threads driving the shards now, to wake when `measuring` changes.
    drivers: Mutex<Vec<thread::Thread>>,
}

impl Ctl {
    /// Wakes every thread driving a shard.
    fn wake(&self) {
        for t in self.drivers.lock().unwrap().iter() {
            t.unpark();
        }
    }

    /// Waits until every shard thread is parked: nothing left to run.
    fn wait_idle(&self) {
        while self.busy.load(Ordering::Acquire) > 0 {
            thread::yield_now();
        }
    }
}

/// Opens with application-owned shards, each driven by a thread of its own.
fn open(path: &Path) -> (Pigeonhole, Vec<thread::JoinHandle<()>>, Arc<Ctl>) {
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
    let ctl = Arc::new(Ctl {
        shards: shards.len(),
        ..Ctl::default()
    });
    let drivers = shards
        .into_iter()
        .map(|shard| {
            let ctl = Arc::clone(&ctl);
            ctl.busy.fetch_add(1, Ordering::AcqRel);
            thread::spawn(move || run(shard, &ctl))
        })
        .collect();
    (db, drivers, ctl)
}

/// A shard's thread: drives it through the setup, hands it to a thread spawned for the
/// measurement (waiting, alive, until it comes back), then drives it to the close.
fn run(mut shard: Shard, ctl: &Ctl) {
    loop {
        let Some(next) = drive(shard, ctl, false) else {
            return;
        };
        shard = next;
        let back = thread::scope(|s| {
            s.spawn(|| {
                ctl.busy.fetch_add(1, Ordering::AcqRel);
                ctl.handed.fetch_add(1, Ordering::AcqRel);
                let back = drive(shard, ctl, true);
                ctl.handed.fetch_sub(1, Ordering::AcqRel);
                back
            })
            .join()
            .expect("a measured shard thread panicked")
        });
        let Some(back) = back else {
            return;
        };
        shard = back;
        ctl.busy.fetch_add(1, Ordering::AcqRel);
    }
}

/// A shard's loop (see `Pigeonhole::open_application_owned`) on this thread, in
/// `shape_run_shard` if `measured`, until the shard closes (`None`) or `measuring` changes
/// (the shard, idle, for the next thread). `busy` counts the thread while it runs (the
/// caller counted it in).
fn drive(mut shard: Shard, ctl: &Ctl, measured: bool) -> Option<Shard> {
    let me = thread::current();
    let wake = me.clone();
    shard.set_wakeup(Box::new(move || wake.unpark()));
    ctl.drivers.lock().unwrap().push(me.clone());
    let driven = loop {
        if measured {
            shape_run_shard(&mut shard);
        } else {
            run_shard(&mut shard);
        }
        if let Some(closed) = shard.closed() {
            closed.unwrap();
            break None;
        }
        if ctl.measuring.load(Ordering::Acquire) != measured {
            break Some(shard);
        }
        ctl.busy.fetch_sub(1, Ordering::AcqRel);
        match shard.next_wakeup() {
            Some(due) => thread::park_timeout(due),
            None => thread::park(),
        }
        ctl.busy.fetch_add(1, Ordering::AcqRel);
    };
    ctl.drivers.lock().unwrap().retain(|t| t.id() != me.id());
    ctl.busy.fetch_sub(1, Ordering::AcqRel);
    driven
}

// The two loops must not compile to the same code: rustc merges identical functions, and
// then the setup's shard work would be counted as the shape's. They differ in their slice
// budget, which changes nothing measured.
#[inline(never)]
fn shape_run_shard(shard: &mut Shard) {
    let _measured = Measured::start();
    while shard.run_once(Duration::from_micros(200)) {}
}

#[inline(never)]
fn run_shard(shard: &mut Shard) {
    while shard.run_once(Duration::from_micros(1000)) {}
}

#[inline(never)]
fn shape_commit_one(t: &Table, rows: &[Vec<u8>]) {
    let _measured = Measured::start();
    for row in rows {
        t.mutate(row).put("f", b"q", &VALUE).commit().unwrap();
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
