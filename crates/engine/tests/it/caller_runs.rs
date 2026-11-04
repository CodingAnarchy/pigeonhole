//! Caller-runs (#1176, the prototype behind `EngineOptions::caller_runs`): with real client
//! threads on engine-owned shards, a committing thread runs its idle shard's turn itself.
//! Sixteen clients mix what a caller may run (`Buffered` and `None` single-shard commits)
//! with what it must leave to the driver (`GroupSync` commits, cross-shard commits,
//! `check_and_mutate`), so callers' turns stop at refused messages and hand the rest over.
//! Every client owns its rows, so the model is each row's last write: after the run every
//! row reads back exactly that, and no commit hangs (a watchdog names what each client was
//! waiting on if none returns for a minute).
//!
//! `PIGEONHOLE_STRESS_OPS` sets the operations per client (default 400).

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
    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(0));
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
