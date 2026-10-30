//! A close after concurrent cross-shard commits finishes (#585). Commits in flight together
//! whose coordinators are each other's participants can log each other's PREPARE before
//! their own COMMIT; the close (and a reopen's close) must still finish.
//!
//! Real files and engine-owned threads, with client threads committing concurrently: the
//! interleaving that crosses the records comes from real concurrency. A watchdog fails the
//! test if the close does not return.

#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::{Durability, FamilyId, TableId};

const SHARDS: usize = 4;
const TABLES: usize = 8;
const CLIENTS: u64 = 8;
const COMMITS: u64 = 20_000;

fn options() -> EngineOptions {
    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(0));
    o.create_if_missing = true;
    o.shards = SHARDS;
    o.memtable_budget = 16 << 20;
    o.tablet_changes = false;
    o
}

/// Runs `f` (a close) on its own thread and fails if it does not return within a minute.
fn within_a_minute(what: &str, f: impl FnOnce() + Send + 'static) {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    rx.recv_timeout(Duration::from_secs(60))
        .unwrap_or_else(|_| panic!("{what} did not finish within a minute (#585)"));
}

#[test]
fn a_close_after_concurrent_cross_shard_commits_finishes() {
    let dir = std::env::temp_dir().join(format!("phdb-close-crossed-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("crossed.phdb");
    let db = Engine::open(&path, options()).unwrap();
    // Table `t` lands on shard `t % SHARDS` (tablet changes off), two tables per shard.
    let tables: Vec<(TableId, FamilyId)> = (0..TABLES)
        .map(|t| {
            let info = db
                .create_table(&format!("t{t}"), &[("f".into(), FamilyOptions::default())])
                .unwrap();
            (info.id, info.families[0].id)
        })
        .collect();
    std::thread::scope(|s| {
        for c in 0..CLIENTS {
            let (db, tables) = (&db, &tables);
            s.spawn(move || {
                let mut i = c;
                while i < COMMITS {
                    // Two rows on different tables, the first (the coordinator) rotating, so
                    // every shard coordinates commits the others take part in.
                    let x = (i % TABLES as u64) as usize;
                    let y = (x + 1 + (i / TABLES as u64 % 3) as usize) % TABLES;
                    let mut wb = WriteBatch::new();
                    for t in [x, y] {
                        wb.put(
                            tables[t].0,
                            tables[t].1,
                            &i.to_be_bytes(),
                            b"q",
                            None,
                            ValueRef::Bytes(&[7u8; 100]),
                        )
                        .unwrap();
                    }
                    db.commit(wb, Some(Durability::Buffered)).unwrap();
                    i += CLIENTS;
                }
            });
        }
    });
    within_a_minute("the close", move || db.close().unwrap());
    // And the close after reopening that database (replaying nothing: it closed cleanly).
    within_a_minute("the reopen's close", move || {
        Engine::open(Path::new(&path), options())
            .unwrap()
            .close()
            .unwrap();
    });
    let _ = std::fs::remove_dir_all(&dir);
}
