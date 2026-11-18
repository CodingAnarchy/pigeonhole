//! D329 PR 2a (ICR 0140 amendment 1): read holds under concurrent writes and tablet changes,
//! randomized and seeded (perf2's condition on 2a; the coordinator's scope).
//!
//! Several threads share one engine-owned database with a small row set over two tables:
//! - **holders** read a few cells, hold them (`Txn::hold_reads`), wait a moment and release;
//! - **writers** write those rows, as single-shard commits and as cross-shard commits over
//!   both tables;
//! - **a mover** moves the tables' tablets between shards and splits them.
//!
//! **The invariant**, checked after every run: when `release()` answers `true`, no write to
//! a held row was admitted while the hold was in place. A write admitted then has a seqno
//! above the hold's snapshot (one at or below it was visible when the hold validated, and
//! one in flight then would have failed the hold with `Conflict`) and completes before the
//! release is called. So a write to a held row with `seqno > snapshot` that completed before
//! the release was called is a violation.
//!
//! #2050 and #2058 were both interleavings of a share's reads with tablet changes, missed by
//! the hand-written tests; this is where holds meet the same boundary. The seed fixes each
//! thread's choices; the threads' interleaving is the machine's, so a failing seed is rerun
//! a few times (`PIGEONHOLE_SEED=<seed> PIGEONHOLE_SEEDS=1`). Swept as
//! `-f test=it -f filter=read_holds::`.

use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use pigeonhole_engine::{
    Engine, FamilyOptions, PendingMaintenance, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

use super::common;

const ROWS: u64 = 8;
const SHARDS: usize = 3;
const OPS: usize = 120;

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

fn row(i: u64) -> Vec<u8> {
    format!("r{i}").into_bytes()
}

/// A write that committed: its table (0 or 1), row, seqno, and when it completed.
struct Written {
    table: usize,
    row: u64,
    seqno: u64,
    done: Instant,
}

/// A hold that was taken: its cells, its snapshot, when it was in place and when its
/// release was called, and whether the release said it lasted.
struct Held {
    cells: Vec<(usize, u64)>,
    snapshot: u64,
    taken_at: Instant,
    released_at: Instant,
    lasted: bool,
}

/// A write refused (`Busy` or `Conflict`): its cells, and when it was submitted and
/// answered.
struct Refused {
    cells: Vec<(usize, u64)>,
    start: Instant,
    end: Instant,
}

/// Polls a tablet change to its end (or 5 s): whether it went through (`Busy` for a held
/// or undecided row's tablet is expected now and then).
fn finish(mut m: PendingMaintenance) -> bool {
    let mut cx = Context::from_waker(Waker::noop());
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Poll::Ready(r) = std::pin::Pin::new(&mut m).poll(&mut cx) {
            return r.is_ok();
        }
        std::thread::sleep(Duration::from_micros(200));
    }
    false
}

/// One seed's run: holds taken, holds whose release said they lasted, tablet changes that
/// went through, and writes refused while a hold on one of their rows was in place.
fn run(seed: u64) -> (usize, usize, usize, usize) {
    let vfs = SimVfs::new(seed);
    let mut o = common::options(Arc::clone(&vfs), SHARDS, 4 << 20);
    o.tablet_changes = true;
    let path = format!("/db/holds-{seed}.phdb");
    let db = Arc::new(Engine::open(Path::new(&path), o).unwrap());
    let tables: Arc<[Arc<TableInfo>; 2]> = Arc::new([
        db.create_table("t0", &[("f".into(), FamilyOptions::default())])
            .unwrap(),
        db.create_table("t1", &[("f".into(), FamilyOptions::default())])
            .unwrap(),
    ]);
    // The tables on different shards, so a write over both is cross-shard.
    let here = db.shard_of(tables[0].id, b"r0").expect("routed");
    if db.shard_of(tables[1].id, b"r0") == Some(here) {
        finish(
            db.move_tablet_pending(tables[1].id, b"r0", ((here + 1) % SHARDS) as u16)
                .unwrap(),
        );
    }
    let written: Arc<Mutex<Vec<Written>>> = Arc::default();
    let refused: Arc<Mutex<Vec<Refused>>> = Arc::default();
    let held: Arc<Mutex<Vec<Held>>> = Arc::default();
    let mut threads = Vec::new();

    // Two writers: singles and cross-shard commits over both tables.
    for w in 0..2u64 {
        let (db, tables, written, refused) = (
            Arc::clone(&db),
            Arc::clone(&tables),
            Arc::clone(&written),
            Arc::clone(&refused),
        );
        threads.push(std::thread::spawn(move || {
            let mut rng = Rng::new(seed ^ (0x100 + w));
            for i in 0..OPS {
                let mut cells = vec![(rng.below(2) as usize, rng.below(ROWS))];
                if rng.chance(40) {
                    let other = 1 - cells[0].0;
                    cells.push((other, rng.below(ROWS)));
                }
                let mut wb = WriteBatch::new();
                let value = format!("w{w}-{i}");
                for &(t, r) in &cells {
                    let f = tables[t].family("f").unwrap().id;
                    wb.put(
                        tables[t].id,
                        f,
                        &row(r),
                        b"q",
                        None,
                        ValueRef::Bytes(value.as_bytes()),
                    )
                    .unwrap();
                }
                let start = Instant::now();
                let answer = db.commit(wb, Some(Durability::Buffered));
                if answer.is_err() {
                    refused.lock().unwrap().push(Refused {
                        cells: cells.clone(),
                        start,
                        end: Instant::now(),
                    });
                }
                if let Ok(info) = answer {
                    let done = Instant::now();
                    let mut log = written.lock().unwrap();
                    for (t, r) in cells {
                        log.push(Written {
                            table: t,
                            row: r,
                            seqno: info.seqno,
                            done,
                        });
                    }
                }
            }
        }));
    }

    // Two holders.
    for h in 0..2u64 {
        let (db, tables, held) = (Arc::clone(&db), Arc::clone(&tables), Arc::clone(&held));
        threads.push(std::thread::spawn(move || {
            let mut rng = Rng::new(seed ^ (0x200 + h));
            for _ in 0..OPS {
                let n = 1 + rng.below(3);
                let mut cells = Vec::new();
                for _ in 0..n {
                    let c = (rng.below(2) as usize, rng.below(ROWS));
                    if !cells.contains(&c) {
                        cells.push(c);
                    }
                }
                let mut txn = db.begin().unwrap();
                let snapshot = txn.snapshot().seqno();
                for &(t, r) in &cells {
                    let f = tables[t].family("f").unwrap().id;
                    let _ = txn.get(tables[t].id, f, &row(r), b"q").unwrap();
                }
                let taking = txn.hold_reads();
                let taken_at = Instant::now();
                let Ok(hold) = taking else {
                    // `Conflict` (a row written since the snapshot, or under a share) or
                    // `Busy` (a tablet moving): nothing held.
                    continue;
                };
                std::thread::sleep(Duration::from_micros(rng.below(1500)));
                let released_at = Instant::now();
                let lasted = hold.release();
                held.lock().unwrap().push(Held {
                    cells,
                    snapshot,
                    taken_at,
                    released_at,
                    lasted,
                });
            }
        }));
    }

    // The mover: moves and splits the tables' tablets.
    let changed: Arc<std::sync::atomic::AtomicUsize> = Arc::default();
    {
        let (db, tables, changed) = (Arc::clone(&db), Arc::clone(&tables), Arc::clone(&changed));
        threads.push(std::thread::spawn(move || {
            let mut rng = Rng::new(seed ^ 0x300);
            for _ in 0..OPS / 4 {
                let t = &tables[rng.below(2) as usize];
                let at = row(1 + rng.below(ROWS - 1));
                let change = if rng.chance(30) {
                    db.split_tablet_pending(t.id, &at)
                } else {
                    db.move_tablet_pending(t.id, &at, rng.below(SHARDS as u64) as u16)
                };
                if let Ok(m) = change
                    && finish(m)
                {
                    changed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                std::thread::sleep(Duration::from_micros(rng.below(2000)));
            }
        }));
    }

    for t in threads {
        t.join().unwrap();
    }

    let written = written.lock().unwrap();
    let held = held.lock().unwrap();
    let lasted = held.iter().filter(|h| h.lasted).count();
    for h in held.iter().filter(|h| h.lasted) {
        for w in written.iter() {
            if h.cells.contains(&(w.table, w.row)) && w.seqno > h.snapshot && w.done < h.released_at
            {
                panic!(
                    "seed {seed}: a write to held row t{}/r{} at seqno {} (above the hold's \
                     snapshot {}) completed before the release, which answered true",
                    w.table, w.row, w.seqno, h.snapshot
                );
            }
        }
    }
    eprintln!(
        "seed {seed}: {} holds taken, {lasted} lasted, {} writes",
        held.len(),
        written.len()
    );
    // Writes refused wholly inside a hold on one of their rows: the holds and writers met.
    let refused = refused.lock().unwrap();
    let met = refused
        .iter()
        .filter(|w| {
            held.iter().any(|h| {
                h.taken_at <= w.start
                    && w.end <= h.released_at
                    && w.cells.iter().any(|c| h.cells.contains(c))
            })
        })
        .count();
    let out = (
        held.len(),
        lasted,
        changed.load(std::sync::atomic::Ordering::Relaxed),
        met,
    );
    drop((written, held, refused));
    db.close().unwrap();
    out
}

#[test]
fn read_holds_keep_their_rows_under_writes_and_tablet_changes() {
    let env = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let first = env("PIGEONHOLE_SEED", 1);
    let (mut taken, mut lasted, mut changed, mut met) = (0, 0, 0, 0);
    for seed in first..first + env("PIGEONHOLE_SEEDS", 4) {
        eprintln!("read_holds seed {seed}");
        let (t, l, c, m) = run(seed);
        eprintln!("seed {seed}: {c} tablet changes went through, {m} writes refused inside a hold");
        (taken, lasted, changed, met) = (taken + t, lasted + l, changed + c, met + m);
    }
    // Not vacuous (perf2): holds were taken and lasted, beside tablet changes that went
    // through. A release answers `false` only for a close or a recovery (a tablet change
    // over a held row is given up instead), which `external.rs` covers.
    assert!(taken > 0 && lasted > 0, "no hold lasted: {taken} taken");
    assert!(
        changed > 0,
        "no tablet change went through beside the holds"
    );
    // And they met: some write was refused wholly inside a hold on one of its rows, so the
    // invariant didn't pass by holds and writes never overlapping (perf2).
    assert!(
        met > 0,
        "no write was refused while a hold on its row was in place"
    );
}
