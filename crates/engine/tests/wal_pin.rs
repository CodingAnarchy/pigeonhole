//! A slot written once and never again must not pin its stream's checkpoint (#137): once a
//! stream holds more than its limit of bytes the checkpoint cannot pass, the shard flushes
//! the slots its oldest records wrote (and asks the participants of its oldest cross-shard
//! COMMITs to flush their shares), so the WAL, the shard's log and the next open's replay
//! stay bounded.
//!
//! Application-owned with every shard driven by the test, so each run is deterministic.

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, PendingCommit, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::{CrashKind, SimVfs};
use pigeonhole_io::{OpenOptions, Vfs};

const DB: &str = "/db/data.phdb";
const BUDGET: u64 = 4 << 20;
const HOT_COMMITS: u32 = 20_000;
const VALUE: usize = 1000;

fn options(vfs: &Arc<SimVfs>, shards: usize) -> EngineOptions {
    common::options(Arc::clone(vfs), shards, BUDGET)
}

/// Bytes in every WAL stream file.
fn wal_bytes(vfs: &SimVfs) -> u64 {
    let mut total = 0;
    for p in vfs.list_dir(Path::new("/db")).unwrap() {
        if p.to_string_lossy().contains("-wal-") {
            total += vfs.open(&p, OpenOptions::read()).unwrap().len().unwrap();
        }
    }
    total
}

/// Runs every shard until `pending` resolves.
fn drive(shards: &mut [EngineShard], mut pending: PendingCommit) {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..1_000_000 {
        for s in shards.iter_mut() {
            s.run_once(u64::MAX);
        }
        match Pin::new(&mut pending).poll(&mut cx) {
            Poll::Ready(r) => {
                r.unwrap();
                return;
            }
            Poll::Pending => {}
        }
    }
    panic!("the commit never resolved");
}

fn put(
    batch: &mut WriteBatch,
    table: pigeonhole_format::TableId,
    family: u32,
    row: &[u8],
    v: &[u8],
) {
    batch
        .put(
            table,
            pigeonhole_format::FamilyId(family),
            row,
            b"q",
            None,
            ValueRef::Bytes(v),
        )
        .unwrap();
}

/// One shard: a single write to family `cold`, then `HOT_COMMITS` 1 KB commits to family
/// `hot`. Returns the WAL bytes at the end.
fn single_shard(cold: bool, seed: u64) -> u64 {
    let vfs = SimVfs::new(seed);
    let db = Engine::open(Path::new(DB), options(&vfs, 1)).unwrap();
    let t = db
        .create_table(
            "t",
            &[
                ("cold".into(), FamilyOptions::default()),
                ("hot".into(), FamilyOptions::default()),
            ],
        )
        .unwrap();
    db.close().unwrap();
    drop(db);

    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), options(&vfs, 1)).unwrap();
    let cold_f = t.family("cold").unwrap().id.0;
    let hot_f = t.family("hot").unwrap().id.0;
    if cold {
        let mut wb = WriteBatch::new();
        put(&mut wb, t.id, cold_f, b"cold-row", b"cold-value");
        drive(
            &mut shards,
            db.submit(wb, Some(Durability::GroupSync)).unwrap(),
        );
    }
    let v = vec![7u8; VALUE];
    let mut max_wal = 0;
    for i in 0..HOT_COMMITS {
        let mut wb = WriteBatch::new();
        put(&mut wb, t.id, hot_f, &i.to_be_bytes(), &v);
        drive(
            &mut shards,
            db.submit(wb, Some(Durability::GroupSync)).unwrap(),
        );
        if i % 1000 == 0 {
            max_wal = max_wal.max(wal_bytes(&vfs));
        }
    }
    max_wal = max_wal.max(wal_bytes(&vfs));

    // The forced flush loses nothing: a power loss now keeps the cold write.
    vfs.crash(CrashKind::Power);
    drop(shards);
    drop(db);
    let db = Engine::open(Path::new(DB), options(&vfs, 1)).unwrap();
    let got = db
        .get_latest(t.id, pigeonhole_format::FamilyId(cold_f), b"cold-row", b"q")
        .unwrap()
        .map(|c| common::value_bytes(c.value()));
    assert_eq!(got.as_deref(), cold.then_some(&b"cold-value"[..]));
    db.close().unwrap();
    max_wal
}

#[test]
fn a_cold_slot_does_not_pin_the_wal() {
    let without = single_shard(false, 137);
    let with = single_shard(true, 137);
    eprintln!("WAL bytes: {without} without a cold write, {with} with one");
    // Before #137: ~24 MB with the cold write (every hot byte kept), ~1 MB without. The
    // limit is twice the memtable budget; allow a budget more for spares and the segment
    // in progress.
    assert!(
        with <= 3 * BUDGET,
        "one cold write pinned the WAL: {with} bytes (without it: {without})"
    );
}

#[test]
fn an_idle_participant_does_not_pin_the_coordinators_wal() {
    let vfs = SimVfs::new(1371);
    let db = Engine::open(Path::new(DB), options(&vfs, 2)).unwrap();
    // Tablet 1 → shard 1, tablet 2 → shard 0.
    let a = db
        .create_table("a", &[("cold".into(), FamilyOptions::default())])
        .unwrap();
    let b = db
        .create_table(
            "b",
            &[
                ("cold".into(), FamilyOptions::default()),
                ("hot".into(), FamilyOptions::default()),
            ],
        )
        .unwrap();
    db.close().unwrap();
    drop(db);

    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), options(&vfs, 2)).unwrap();
    // One cross-shard commit: shard 0 coordinates, shard 1 holds a share and then idles.
    let mut wb = WriteBatch::new();
    put(
        &mut wb,
        a.id,
        a.family("cold").unwrap().id.0,
        b"x",
        b"cold-a",
    );
    put(
        &mut wb,
        b.id,
        b.family("cold").unwrap().id.0,
        b"x",
        b"cold-b",
    );
    drive(
        &mut shards,
        db.submit(wb, Some(Durability::GroupSync)).unwrap(),
    );
    let hot_f = b.family("hot").unwrap().id.0;
    let v = vec![7u8; VALUE];
    for i in 0..HOT_COMMITS {
        let mut wb = WriteBatch::new();
        put(&mut wb, b.id, hot_f, &i.to_be_bytes(), &v);
        drive(
            &mut shards,
            db.submit(wb, Some(Durability::GroupSync)).unwrap(),
        );
    }
    let wal = wal_bytes(&vfs);
    eprintln!("WAL bytes over both streams: {wal}");
    assert!(
        wal <= 4 * BUDGET,
        "an idle participant pinned the coordinator's WAL: {wal} bytes"
    );

    vfs.crash(CrashKind::Power);
    drop(shards);
    drop(db);
    let db = Engine::open(Path::new(DB), options(&vfs, 2)).unwrap();
    for (t, want) in [(&a, b"cold-a"), (&b, b"cold-b")] {
        let got = db
            .get_latest(t.id, t.family("cold").unwrap().id, b"x", b"q")
            .unwrap()
            .map(|c| common::value_bytes(c.value()));
        assert_eq!(got.as_deref(), Some(&want[..]));
    }
    db.close().unwrap();
}
