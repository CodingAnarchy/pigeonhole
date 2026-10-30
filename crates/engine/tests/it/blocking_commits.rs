//! Blocking commits from many threads never hang (#590). The model harness drives commits
//! through the non-blocking API (a seed must replay), so the blocking waits (`Engine::commit`,
//! `check_and_mutate`, `Txn::commit`) get their real-thread coverage here: engine-owned shards
//! on real files, client threads mixing cross-shard commits, conditional commits and
//! transactions at every durability. A watchdog fails the test, naming what each client was
//! waiting on, if no commit returns for a minute.
//!
//! `PIGEONHOLE_STRESS_OPS` sets the operations per client (default 500); loop it with a
//! large value to hunt a rare lost wake.

#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyOptions, Predicate, ValueRef, WriteBatch,
};
use pigeonhole_format::{Durability, FamilyId, TableId};

const SHARDS: usize = 4;
const TABLES: usize = 8;
const CLIENTS: usize = 8;
/// How long the clients may go without a single commit returning.
const STALL: Duration = Duration::from_secs(60);

fn options() -> EngineOptions {
    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(0));
    o.create_if_missing = true;
    o.shards = SHARDS;
    o.memtable_budget = 16 << 20;
    // Table `t` lands on shard `t % SHARDS`, so the commits below cross shards as written.
    o.tablet_changes = false;
    o
}

fn put(wb: &mut WriteBatch, t: (TableId, FamilyId), row: &[u8], value: &[u8]) {
    wb.put(t.0, t.1, row, b"q", None, ValueRef::Bytes(value))
        .unwrap();
}

/// One client's operations: plain cross-shard and single-shard commits, conditional commits
/// and two-shard transactions, rotating durability. Conflicts and refused predicates are
/// expected outcomes; any other error fails the test.
fn client(
    db: &Engine,
    tables: &[(TableId, FamilyId)],
    c: usize,
    ops: u64,
    doing: &Mutex<String>,
    done: &AtomicU64,
) {
    const DURABILITY: [Durability; 3] = [
        Durability::Buffered,
        Durability::GroupSync,
        Durability::Sync,
    ];
    for i in 0..ops {
        let n = i * CLIENTS as u64 + c as u64;
        let d = DURABILITY[(n % 3) as usize];
        let x = (n % TABLES as u64) as usize;
        let y = (x + 1 + (n / TABLES as u64 % 3) as usize) % TABLES;
        let row = (n % 64).to_be_bytes();
        let kind = n % 5;
        *doing.lock().unwrap() = format!("op {i}: kind {kind} on tables {x},{y} {d:?}");
        match kind {
            0 | 1 => {
                let mut wb = WriteBatch::new();
                put(&mut wb, tables[x], &row, &n.to_le_bytes());
                put(&mut wb, tables[y], &row, &n.to_le_bytes());
                db.commit(wb, Some(d)).unwrap();
            }
            2 => {
                let pred = Predicate::Absent {
                    family: tables[x].1,
                    qualifier: format!("lock{}", n % 8).into_bytes(),
                };
                let mut wb = WriteBatch::new();
                put(&mut wb, tables[x], &row, b"cas");
                db.check_and_mutate(tables[x].0, &row, &pred, wb, Some(d))
                    .unwrap();
            }
            3 => {
                let mut txn = db.begin().unwrap();
                txn.get(tables[x].0, tables[x].1, &row, b"q").unwrap();
                txn.get(tables[y].0, tables[y].1, &row, b"q").unwrap();
                put(txn.batch(), tables[x], &row, b"txn");
                put(txn.batch(), tables[y], &row, b"txn");
                match txn.commit(Some(d)) {
                    Ok(_) | Err(Error::Conflict) => {}
                    Err(e) => panic!("client {c} op {i}: transaction failed: {e}"),
                }
            }
            _ => {
                let mut wb = WriteBatch::new();
                put(&mut wb, tables[x], &row, &n.to_le_bytes());
                db.commit(wb, Some(d)).unwrap();
            }
        }
        done.fetch_add(1, Ordering::Relaxed);
    }
    *doing.lock().unwrap() = "finished".into();
}

#[test]
fn blocking_commits_from_many_threads_never_hang() {
    let ops: u64 = std::env::var("PIGEONHOLE_STRESS_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);
    let dir = std::env::temp_dir().join(format!("phdb-blocking-commits-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = Engine::open(&dir.join("blocking.phdb"), options()).unwrap();
    let tables: Vec<(TableId, FamilyId)> = (0..TABLES)
        .map(|t| {
            let info = db
                .create_table(&format!("t{t}"), &[("f".into(), FamilyOptions::default())])
                .unwrap();
            (info.id, info.families[0].id)
        })
        .collect();
    let doing: Arc<Vec<Mutex<String>>> =
        Arc::new((0..CLIENTS).map(|_| Mutex::new(String::new())).collect());
    let done = AtomicU64::new(0);
    let finished = AtomicBool::new(false);
    std::thread::scope(|s| {
        let clients: Vec<_> = (0..CLIENTS)
            .map(|c| {
                let (db, tables, doing, done) = (&db, &tables, &doing, &done);
                s.spawn(move || client(db, tables, c, ops, &doing[c], done))
            })
            .collect();
        // The watchdog: some commit must return at least once a minute.
        s.spawn(|| {
            let (mut last, mut since) = (0, Instant::now());
            while !finished.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(200));
                let now = done.load(Ordering::Relaxed);
                if now != last {
                    (last, since) = (now, Instant::now());
                } else if since.elapsed() > STALL {
                    let waiting: Vec<String> = doing.iter().map(|d| d.lock().unwrap().clone()).collect();
                    eprintln!("no commit returned for {STALL:?} after {now} commits; clients: {waiting:#?}");
                    // A hung commit never returns, so the scope would never end: abort.
                    std::process::abort();
                }
            }
        });
        for c in clients {
            c.join().unwrap();
        }
        finished.store(true, Ordering::Release);
    });
    assert_eq!(done.load(Ordering::Relaxed), ops * CLIENTS as u64);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
