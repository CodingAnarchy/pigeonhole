//! Where a commit's latency goes (#64): the same buffered one-field overwrites (`ycsb-a`'s
//! write) from one client to one shard, under four ways of running the shard. The
//! differences split a commit's latency into its CPU path and its thread handoff.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example commitpath -- [COMMITS] [DIR]
//! ```
//!
//! | Mode | The shard runs on | The client waits by |
//! |---|---|---|
//! | `threads` | the engine's own shard thread (`Engine::open`, as `phdb-bench` runs), with its default spin before parking (D198) | `Engine::commit`: its default spin, then parking |
//! | `threads-nospin` | as `threads`, with both spin windows 0 (the engine before D198) | parking |
//! | `park` | an application thread that parks between groups, woken by the engine | parking |
//! | `spin` | an application thread that never parks | spinning on the commit's future |
//! | `spin-shard` | an application thread that never parks | parking |
//! | `spin-client` | an application thread that parks between groups | spinning on the commit's future |
//! | `inline` | the client's own thread, right after submitting (`run_once` until the commit resolves) | — |
//!
//! `spin` minus `inline` is the cost of crossing threads with no sleeping on either side;
//! `park` minus `spin` is the wakeups, and `spin-shard` and `spin-client` split them by side
//! (the shard woken by a submit, the client woken by its commit's completion). `inline` is the floor a commit could reach on the
//! caller's thread (what a write-group leader would pay).
//!
//! The store: one shard, tablet changes off, a memtable no run fills, 50,000 rows of one
//! 100-byte field preloaded; each measured commit overwrites one uniformly chosen row.
//! Prints p50, p99, p99.9 and mean per mode, in microseconds.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use pigeonhole_engine::{
    Durability, Engine, EngineOptions, EngineShard, FamilyOptions, TableInfo, ValueRef, WriteBatch,
};

const ROWS: u64 = 50_000;
const VALUE: [u8; 100] = [7; 100];

/// The engine's defaults, except one shard and a memtable no run fills. The modes that
/// spin by hand turn the engine's own spinning off (`spins: false`).
fn options(spins: bool) -> EngineOptions {
    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(0));
    if !spins {
        o.commit_spin_nanos = 0;
        o.shard_spin_nanos = 0;
    }
    o.create_if_missing = true;
    o.shards = 1;
    o.tablet_changes = false;
    o.memtable_budget = 1 << 30;
    o.durability = Durability::Buffered;
    o
}

fn row(i: u64) -> Vec<u8> {
    format!("user{:016x}", i.wrapping_mul(0x9E37_79B9_7F4A_7C15)).into_bytes()
}

fn batch(t: &TableInfo, i: u64) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        &row(i),
        b"field0",
        None,
        ValueRef::Bytes(&VALUE),
    )
    .unwrap();
    wb
}

/// A seeded xorshift.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// Runs `commits` overwrites through `commit` (after a warmup of a tenth as many) and
/// returns the sorted latencies.
fn measure(t: &TableInfo, commits: usize, mut commit: impl FnMut(WriteBatch)) -> Vec<Duration> {
    let mut rng = Rng(0x5EED);
    for _ in 0..commits / 10 {
        commit(batch(t, rng.below(ROWS)));
    }
    let mut lat = Vec::with_capacity(commits);
    for _ in 0..commits {
        let wb = batch(t, rng.below(ROWS));
        let start = Instant::now();
        commit(wb);
        lat.push(start.elapsed());
    }
    lat.sort();
    lat
}

/// Prints a row and returns the p50.
fn report(mode: &str, lat: &[Duration]) -> Duration {
    let at = |q: f64| lat[((lat.len() as f64 * q) as usize).min(lat.len() - 1)];
    let mean = lat.iter().sum::<Duration>() / lat.len() as u32;
    let us = |d: Duration| d.as_secs_f64() * 1e6;
    println!(
        "| {mode} | {:.2} | {:.2} | {:.2} | {:.2} |",
        us(at(0.50)),
        us(at(0.99)),
        us(at(0.999)),
        us(mean)
    );
    at(0.50)
}

fn create(db: &Engine) -> Arc<TableInfo> {
    db.create_table(
        "t",
        &[("f".into(), FamilyOptions::default().max_versions(1))],
    )
    .unwrap()
}

fn load(t: &TableInfo, mut commit: impl FnMut(WriteBatch)) {
    for i in 0..ROWS {
        commit(batch(t, i));
    }
}

/// Polls `f` without a waker until it resolves, running `between` after each pending poll.
fn poll_until<F: std::future::Future + Unpin>(mut f: F, mut between: impl FnMut()) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(r) = std::pin::Pin::new(&mut f).poll(&mut cx) {
            return r;
        }
        between();
    }
}

/// Returns the p50.
fn engine_threads(dir: &Path, commits: usize, spins: bool) -> Duration {
    let mode = if spins { "threads" } else { "threads-nospin" };
    let db = Engine::open(&dir.join(format!("{mode}.phdb")), options(spins)).unwrap();
    let t = create(&db);
    load(&t, |wb| {
        db.commit(wb, None).unwrap();
    });
    let lat = measure(&t, commits, |wb| {
        db.commit(wb, None).unwrap();
    });
    let p50 = report(mode, &lat);
    db.close().unwrap();
    p50
}

/// A shard on its own thread that parks between groups unless `shard_spins`; the client
/// parks on its commit unless `client_spins`.
fn driven(dir: &Path, commits: usize, shard_spins: bool, client_spins: bool) {
    let mode = match (shard_spins, client_spins) {
        (false, false) => "park",
        (true, true) => "spin",
        (true, false) => "spin-shard",
        (false, true) => "spin-client",
    };
    let (db, mut shards) =
        Engine::open_application_owned(&dir.join(format!("{mode}.phdb")), options(false)).unwrap();
    let spin = shard_spins;
    let mut shard: EngineShard = shards.remove(0);
    let stop = Arc::new(AtomicBool::new(false));
    let driver = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let me = std::thread::current();
            shard.set_wakeup(Box::new(move || me.unpark()));
            loop {
                while shard.run_once(u64::MAX) {}
                if let Some(closed) = shard.closed() {
                    closed.unwrap();
                    return;
                }
                if !spin && !stop.load(Ordering::Acquire) {
                    std::thread::park_timeout(Duration::from_millis(10));
                }
            }
        })
    };
    let t = create(&db);
    let commit = |wb: WriteBatch| {
        if client_spins {
            poll_until(db.submit(wb, None).unwrap(), std::hint::spin_loop).unwrap();
        } else {
            db.commit(wb, None).unwrap();
        }
    };
    load(&t, commit);
    let lat = measure(&t, commits, commit);
    report(mode, &lat);
    stop.store(true, Ordering::Release);
    db.close().unwrap();
    driver.join().unwrap();
}

/// The client runs the shard itself after each submit.
fn inline(dir: &Path, commits: usize) {
    let (db, mut shards) =
        Engine::open_application_owned(&dir.join("inline.phdb"), options(false)).unwrap();
    let mut shard = shards.remove(0);
    while shard.run_once(u64::MAX) {}
    let t = create(&db);
    let mut commit = |wb: WriteBatch| {
        let pending = db.submit(wb, None).unwrap();
        poll_until(pending, || {
            shard.run_once(u64::MAX);
        })
        .unwrap();
    };
    load(&t, &mut commit);
    let lat = measure(&t, commits, &mut commit);
    report("inline", &lat);
    db.close().unwrap();
    while shard.closed().is_none() {
        shard.run_once(u64::MAX);
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let commits: usize = args.next().map_or(200_000, |s| s.parse().expect("COMMITS"));
    let base = args
        .next()
        .map_or_else(std::env::temp_dir, std::path::PathBuf::from);
    let dir = base.join(format!("phdb-commitpath-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the store directory");
    println!("| mode | p50 µs | p99 µs | p99.9 µs | mean µs |");
    println!("|---|--:|--:|--:|--:|");
    let spinning = engine_threads(&dir, commits, true);
    let parking = engine_threads(&dir, commits, false);
    driven(&dir, commits, false, false);
    driven(&dir, commits, true, true);
    driven(&dir, commits, true, false);
    driven(&dir, commits, false, true);
    inline(&dir, commits);
    std::fs::remove_dir_all(&dir).ok();
    // The one wall-clock check of the default spin (D198; the instruction shapes run with
    // the client's spin off, since its poll count depends on timing): with the defaults, a
    // buffered commit's median must not be clearly worse than without spinning. The margin
    // allows for a shared runner's noise.
    let (s, p) = (spinning.as_secs_f64() * 1e6, parking.as_secs_f64() * 1e6);
    println!();
    if s > p * 1.25 {
        println!("FAIL: p50 {s:.2} µs with the default spin against {p:.2} µs without (D198)");
        std::process::exit(1);
    }
    println!("ok: p50 {s:.2} µs with the default spin against {p:.2} µs without (D198)");
}
