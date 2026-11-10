//! Caller-runs (D321, `EngineOptions::caller_runs`): with real client
//! threads on engine-owned shards, a committing thread runs its idle shard's turn itself.
//! Sixteen clients mix what a caller may run (`Buffered` and `None` single-shard commits)
//! with what it must leave to the driver (`GroupSync` commits, cross-shard commits,
//! `check_and_mutate`), so callers' turns stop at refused messages and hand the rest over.
//! Every client owns its rows, so the model is each row's last write: after the run every
//! row reads back exactly that, and no commit hangs (a watchdog names what each client was
//! waiting on if none returns for a minute).
//!
//! `PIGEONHOLE_STRESS_OPS` sets the operations per client (default 400; the cross-shard test
//! runs ten times that). `PIGEONHOLE_IO=uring` runs them on io_uring (Linux).

#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, Predicate, ValueRef, WriteBatch};
use pigeonhole_format::{Durability, FamilyId, TableId};

const SHARDS: usize = 4;
const TABLES: usize = 8;
const CLIENTS: usize = 16;
const ROWS_PER_CLIENT: u64 = 16;
const STALL: Duration = Duration::from_secs(60);

fn options() -> EngineOptions {
    let vfs: pigeonhole_io::VfsRef = match std::env::var("PIGEONHOLE_IO").as_deref() {
        #[cfg(target_os = "linux")]
        Ok("uring") => pigeonhole_io::uring::UringVfs::new().expect("io_uring"),
        _ => pigeonhole_io::pread::PreadVfs::new(0),
    };
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = SHARDS;
    o.memtable_budget = 16 << 20;
    // Table `t` lands on shard `t % SHARDS`: the two-table commits below cross shards.
    o.tablet_changes = false;
    o.caller_runs = true;
    o
}

fn put(wb: &mut WriteBatch, t: (TableId, FamilyId), row: &[u8], value: &[u8]) {
    wb.put(t.0, t.1, row, b"q", None, ValueRef::Bytes(value))
        .unwrap();
}

/// Client `c`'s row `r`: its own, so the last value written to it is the client's.
fn row(c: usize, r: u64) -> Vec<u8> {
    format!("c{c:02}r{r:02}").into_bytes()
}

/// Per (table, row), the last value a client wrote there.
type LastWrites = Vec<((usize, Vec<u8>), Vec<u8>)>;

/// One client's run. Returns, per (table, row), the last value it wrote there.
fn client(
    db: &Engine,
    tables: &[(TableId, FamilyId)],
    c: usize,
    ops: u64,
    doing: &Mutex<String>,
    done: &AtomicU64,
) -> LastWrites {
    let mut last: std::collections::BTreeMap<(usize, Vec<u8>), Vec<u8>> = Default::default();
    for i in 0..ops {
        let n = i * CLIENTS as u64 + c as u64;
        let x = (n % TABLES as u64) as usize;
        let y = (x + 1) % TABLES;
        let r = row(c, n % ROWS_PER_CLIENT);
        let value = n.to_le_bytes().to_vec();
        let kind = n % 10;
        *doing.lock().unwrap() = format!("op {i}: kind {kind} on table {x}");
        match kind {
            // What a caller may run: single-shard, no sync.
            0..=4 => {
                let d = if kind.is_multiple_of(2) {
                    Durability::Buffered
                } else {
                    Durability::None
                };
                let mut wb = WriteBatch::new();
                put(&mut wb, tables[x], &r, &value);
                db.commit(wb, Some(d)).unwrap();
                last.insert((x, r), value);
            }
            // A durable single-shard commit: the driver's.
            5 | 6 => {
                let mut wb = WriteBatch::new();
                put(&mut wb, tables[x], &r, &value);
                db.commit(wb, Some(Durability::GroupSync)).unwrap();
                last.insert((x, r), value);
            }
            // A cross-shard commit: the coordinator's driver runs it.
            7 | 8 => {
                let mut wb = WriteBatch::new();
                put(&mut wb, tables[x], &r, &value);
                put(&mut wb, tables[y], &r, &value);
                db.commit(wb, Some(Durability::Buffered)).unwrap();
                last.insert((x, r.clone()), value.clone());
                last.insert((y, r), value);
            }
            // A conditional commit (it reads at apply): the driver's.
            _ => {
                let pred = Predicate::Absent {
                    family: tables[x].1,
                    qualifier: b"never-written".to_vec(),
                };
                let mut wb = WriteBatch::new();
                put(&mut wb, tables[x], &r, &value);
                let (applied, _) = db
                    .check_and_mutate(tables[x].0, &r, &pred, wb, Some(Durability::Buffered))
                    .unwrap();
                assert!(applied, "client {c} op {i}: the predicate holds");
                last.insert((x, r), value);
            }
        }
        done.fetch_add(1, Ordering::Relaxed);
    }
    *doing.lock().unwrap() = "finished".into();
    last.into_iter().collect()
}

#[test]
fn callers_running_their_shards_lose_no_write_and_hang_nothing() {
    let ops: u64 = std::env::var("PIGEONHOLE_STRESS_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(400);
    let dir = std::env::temp_dir().join(format!("phdb-caller-runs-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = Engine::open(&dir.join("callers.phdb"), options()).unwrap();
    let tables: Vec<(TableId, FamilyId)> = (0..TABLES)
        .map(|t| {
            let info = db
                .create_table(&format!("t{t}"), &[("f".into(), FamilyOptions::default())])
                .unwrap();
            (info.id, info.families[0].id)
        })
        .collect();
    let doing: Vec<Mutex<String>> = (0..CLIENTS).map(|_| Mutex::new(String::new())).collect();
    let done = AtomicU64::new(0);
    let finished = AtomicBool::new(false);
    let writes = std::thread::scope(|s| {
        let clients: Vec<_> = (0..CLIENTS)
            .map(|c| {
                let (db, tables, doing, done) = (&db, &tables, &doing, &done);
                s.spawn(move || client(db, tables, c, ops, &doing[c], done))
            })
            .collect();
        s.spawn(|| {
            let (mut last, mut since) = (0, Instant::now());
            while !finished.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(200));
                let now = done.load(Ordering::Relaxed);
                if now != last {
                    (last, since) = (now, Instant::now());
                } else if since.elapsed() > STALL {
                    let what: Vec<String> = doing
                        .iter()
                        .enumerate()
                        .map(|(c, d)| format!("client {c}: {}", d.lock().unwrap()))
                        .collect();
                    panic!(
                        "no commit returned for {STALL:?} after {now} ops:\n{}",
                        what.join("\n")
                    );
                }
            }
        });
        let writes: Vec<_> = clients
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        finished.store(true, Ordering::Release);
        writes
    });
    assert_eq!(done.load(Ordering::Relaxed), ops * CLIENTS as u64);
    let turns = db.caller_turns();
    assert!(turns > 0, "no client ran its shard's turn");
    eprintln!("caller turns: {turns} for {} ops", ops * CLIENTS as u64);
    // Work a shard had when its driver's park ran out the backstop, with no notify for it, is
    // a wake the caller-to-driver handshake lost (#1502 Phase B's hang, which the backstop now turns
    // into a 100 ms stall rather than a hang): there must be none.
    assert_eq!(
        db.metrics().caller_backstop_wakes,
        0,
        "a shard had work no notify signalled when its driver's park backstop ran out: a lost wake"
    );
    let snap = db.snapshot().unwrap();
    for ((t, r), want) in &writes {
        let got = db
            .get(&snap, tables[*t].0, tables[*t].1, r, b"q")
            .unwrap()
            .map(|c| match c.value() {
                ValueRef::Bytes(b) => b.to_vec(),
                other => panic!("table {t} row {r:?}: not bytes: {other:?}"),
            });
        assert_eq!(got.as_deref(), Some(&want[..]), "table {t} row {r:?}");
    }
    drop(snap);
    db.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

/// Cross-shard contention under caller-runs, with idle gaps (#1502's re-run counted 2, 9 and
/// 2 backstop wakes on buffered cross-shard-contention, and none elsewhere). The bench row's
/// shape: one table per shard, hot rows every client shares, half the commits writing one row
/// and half one row in each of 2-4 tables, all `Buffered`, with adaptive drivers and tablet
/// changes on as in the bench. So PREPAREs meet rows other commits hold, refusals and their
/// wakes run (D245, D249), and callers' single-shard turns interleave with the drivers' 2PC
/// rounds. Every `PAUSE` commits all clients stop together for longer than the backstop, so
/// every driver's park runs it out, and the next burst lands right as the timers fire: the
/// count must tell a wake that arrived late (a notify found the shard announced asleep) from
/// a lost one. Before `ShardCell::check_backstop`, this test counted one or two a run, every
/// one a delivered wake. No commit may hang, and no wake may be lost.
#[test]
fn callers_amid_contended_cross_shard_commits_lose_no_wake() {
    const XSHARDS: usize = 16;
    const XCLIENTS: usize = 16;
    const HOT: u64 = 64;
    const PAUSE: u64 = 200;
    let ops: u64 = std::env::var("PIGEONHOLE_STRESS_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(400)
        * 10;
    let dir = std::env::temp_dir().join(format!("phdb-caller-runs-xshard-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut o = options();
    o.shards = XSHARDS;
    o.adaptive_drivers = true;
    o.tablet_changes = true;
    // `CpuProfile::Embedded`'s adaptive spin (#1201).
    o.shard_spin_cap_nanos = 1_000_000;
    let barrier = std::sync::Barrier::new(XCLIENTS);
    let db = Engine::open(&dir.join("xshard.phdb"), o).unwrap();
    let tables: Vec<(TableId, FamilyId)> = (0..XSHARDS)
        .map(|t| {
            let info = db
                .create_table(&format!("x{t}"), &[("f".into(), FamilyOptions::default())])
                .unwrap();
            (info.id, info.families[0].id)
        })
        .collect();
    let doing: Vec<Mutex<String>> = (0..XCLIENTS).map(|_| Mutex::new(String::new())).collect();
    let done = AtomicU64::new(0);
    let finished = AtomicBool::new(false);
    std::thread::scope(|s| {
        let clients: Vec<_> = (0..XCLIENTS)
            .map(|c| {
                let (db, tables, doing, done) = (&db, &tables, &doing[c], &done);
                let barrier = &barrier;
                s.spawn(move || {
                    // A small xorshift per client: the run's shape, not its seed, matters.
                    let mut x = 0x9e37_79b9_7f4a_7c15u64 ^ (c as u64 + 1);
                    let mut next = move || {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        x
                    };
                    for i in 0..ops {
                        let r = next();
                        let parts = if r & 1 == 0 { 1 } else { 2 + (r >> 1) % 3 } as usize;
                        let first = (r >> 8) as usize % XSHARDS;
                        *doing.lock().unwrap() = format!("op {i}: {parts} part(s) from {first}");
                        let mut wb = WriteBatch::new();
                        for k in 0..parts {
                            let row = format!("hot{:02}", next() % HOT).into_bytes();
                            put(
                                &mut wb,
                                tables[(first + k) % XSHARDS],
                                &row,
                                &i.to_le_bytes(),
                            );
                        }
                        db.commit(wb, Some(Durability::Buffered)).unwrap();
                        done.fetch_add(1, Ordering::Relaxed);
                        // Idle gaps past the drivers' 100 ms park backstop, so parks reach
                        // it and the hand-back's wakes are tested from a parked driver.
                        if i % PAUSE == PAUSE - 1 {
                            barrier.wait();
                            std::thread::sleep(Duration::from_millis(110 + (r % 40)));
                        }
                    }
                    *doing.lock().unwrap() = "finished".into();
                })
            })
            .collect();
        s.spawn(|| {
            let (mut last, mut since) = (0, Instant::now());
            while !finished.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(200));
                let now = done.load(Ordering::Relaxed);
                if now != last {
                    (last, since) = (now, Instant::now());
                } else if since.elapsed() > STALL {
                    let what: Vec<String> = doing
                        .iter()
                        .enumerate()
                        .map(|(c, d)| format!("client {c}: {}", d.lock().unwrap()))
                        .collect();
                    panic!(
                        "no commit returned for {STALL:?} after {now} ops:\n{}",
                        what.join("\n")
                    );
                }
            }
        });
        for h in clients {
            h.join().unwrap();
        }
        finished.store(true, Ordering::Release);
    });
    let m = db.metrics();
    eprintln!(
        "caller turns: {} for {} ops; backstop wakes {}",
        db.caller_turns(),
        ops * XCLIENTS as u64,
        m.caller_backstop_wakes
    );
    assert_eq!(
        m.caller_backstop_wakes, 0,
        "a shard had work no notify signalled when its driver's park backstop ran out: a lost wake"
    );
    db.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}
