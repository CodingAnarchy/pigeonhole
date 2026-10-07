//! Opening is cheap and always possible (#143).
//!
//! - An open used to zero-fill a 64 MiB WAL segment per shard and prepare two more in the
//!   background, so every open of a small database wrote 192 MiB per shard. It now writes
//!   one frame per stream, and spares are prepared only once a stream is half way through
//!   its first segment.
//! - Replay used to hold everything recovered in the arenas, so a database that crashed
//!   with more unflushed data than its reopened shards' arenas hold (fewer shards, or a
//!   smaller budget) failed to open with `InvalidArgument`. Replay now spills recovered
//!   memtables to SSTs when an arena runs short.

mod common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, FamilyId, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::{Durability, TableId};
use pigeonhole_io::sim::{CrashKind, SimOp, SimVfs};

const DB: &str = "/db/data.phdb";

/// Bytes written by the recorded operations.
fn written(vfs: &SimVfs) -> u64 {
    vfs.recorded_ops()
        .iter()
        .map(|op| match op {
            SimOp::Write { len, .. } => *len,
            _ => 0,
        })
        .sum()
}

#[test]
fn open_and_close_of_a_small_database_write_little() {
    let vfs = SimVfs::new(143);
    let mut o = common::options(Arc::clone(&vfs), 2, 64 << 20);
    // The defaults: 64 MiB segments, two spares.
    o.wal = Default::default();
    let db = Engine::open(Path::new(DB), o.clone()).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    db.close().unwrap();
    drop(db);

    vfs.record_ops();
    let db = Engine::open(Path::new(DB), o.clone()).unwrap();
    let at_open = written(&vfs);
    for i in 0..100u32 {
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            t.families[0].id,
            &i.to_be_bytes(),
            b"q",
            None,
            ValueRef::Bytes(b"v"),
        )
        .unwrap();
        db.commit(wb, Some(Durability::GroupSync)).unwrap();
    }
    db.close().unwrap();
    let total = written(&vfs);
    eprintln!("open wrote {at_open} bytes; open, 100 commits and close wrote {total}");
    // Before #143: 128 MiB at open (a zero-filled segment per stream) and 384 MiB more in
    // the background (two spares each).
    assert!(at_open <= 1 << 20, "open wrote {at_open} bytes");
    assert!(
        total <= 4 << 20,
        "open, 100 commits and close wrote {total} bytes"
    );
}

/// `shards` shards, one table each, `per_table` bytes unflushed in each, then a power loss.
/// Reopens with `reopen_shards` shards and a `reopen_budget` arena each, and checks every row.
fn crash_with_unflushed(
    seed: u64,
    shards: usize,
    per_table: usize,
    reopen_shards: usize,
    reopen_budget: u64,
) {
    let vfs = SimVfs::new(seed);
    let mut o = common::options(Arc::clone(&vfs), shards, 4 << 20);
    // Nothing freezes by size: everything stays in the arenas until the crash.
    o.memtable_freeze_bytes = 4 << 20;
    o.wal_pin_bytes = u64::MAX;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let tables: Vec<_> = (0..shards)
        .map(|i| {
            db.create_table(&format!("t{i}"), &[("f".into(), FamilyOptions::default())])
                .unwrap()
        })
        .collect();
    let value = vec![9u8; 1000];
    let rows = (per_table / value.len()) as u32;
    for t in &tables {
        for r in 0..rows {
            let mut wb = WriteBatch::new();
            put(&mut wb, t.id, t.families[0].id, &r.to_be_bytes(), &value);
            db.commit(wb, Some(Durability::GroupSync)).unwrap();
        }
    }
    assert_eq!(
        db.metrics().flushes,
        0,
        "the data must be unflushed at the crash"
    );
    vfs.crash(CrashKind::Power);
    drop(db);

    let reopen = common::options(Arc::clone(&vfs), reopen_shards, reopen_budget);
    let db = Engine::open(Path::new(DB), reopen.clone())
        .unwrap_or_else(|e| panic!("seed {seed}: reopen failed: {e}"));
    let check = |db: &Engine| {
        for t in &tables {
            for r in 0..rows {
                let got = db
                    .get_latest(t.id, t.families[0].id, &r.to_be_bytes(), b"q")
                    .unwrap()
                    .map(|c| common::value_bytes(c.value()));
                assert_eq!(
                    got.as_deref(),
                    Some(&value[..]),
                    "seed {seed}: table {} row {r}",
                    t.name
                );
            }
        }
    };
    check(&db);
    // It keeps working: a write, a clean close, a reopen.
    let mut wb = WriteBatch::new();
    put(
        &mut wb,
        tables[0].id,
        tables[0].families[0].id,
        b"after",
        b"v",
    );
    db.commit(wb, None).unwrap();
    db.close().unwrap();
    drop(db);
    let db = Engine::open(Path::new(DB), reopen).unwrap();
    check(&db);
    db.close().unwrap();
}

fn put(wb: &mut WriteBatch, table: TableId, family: FamilyId, row: &[u8], v: &[u8]) {
    wb.put(table, family, row, b"q", None, ValueRef::Bytes(v))
        .unwrap();
}

#[test]
fn a_crashed_database_reopens_with_fewer_shards() {
    // 4 × 2.5 MB unflushed over four 4 MiB arenas, reopened as one shard with one arena.
    crash_with_unflushed(1431, 4, 2_500_000, 1, 4 << 20);
}

#[test]
fn a_crashed_database_reopens_with_a_smaller_budget() {
    // The same layout, but every arena a quarter of the size it crashed with.
    crash_with_unflushed(1432, 2, 2_500_000, 2, 1 << 20);
}
