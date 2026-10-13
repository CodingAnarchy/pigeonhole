//! The instance's health check (ICR 0040): `Engine::is_failed` and `Engine::failure` report
//! the first failure, with its shard and cause, for each way an instance fails.

mod gate;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, EngineOptions, Failure, FailureCause, FamilyOptions, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::VfsRef;

const DB: &str = "/db/h.phdb";

fn options(vfs: VfsRef, shards: usize) -> EngineOptions {
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = shards;
    o.pin_threads = false;
    o.compaction_threads = 0;
    o.memtable_budget = 4 << 20;
    o.wal.segment_size = 256 << 10;
    o.wal.spare_segments = 1;
    o
}

fn wal(stream: u32) -> PathBuf {
    pigeonhole_wal::stream_path(Path::new(DB), pigeonhole_format::StreamId(stream))
}

fn put(t: &TableInfo, row: &str) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row.as_bytes(),
        b"q",
        None,
        ValueRef::Bytes(b"v"),
    )
    .unwrap();
    wb
}

fn table(db: &Engine, name: &str) -> Arc<TableInfo> {
    db.create_table(name, &[("f".into(), FamilyOptions::default())])
        .unwrap()
}

/// The shard owning `t`: commit to it and look at the stream the record went to.
fn shard_of(db: &Engine, t: &TableInfo) -> u32 {
    db.record_history(true);
    db.take_appended();
    db.commit(put(t, "probe"), Some(Durability::Buffered))
        .unwrap();
    u32::from(db.take_appended().last().expect("one record").stream)
}

fn failed(shard: u32, cause: FailureCause) -> Option<Failure> {
    Some(Failure { shard, cause })
}

#[test]
fn a_healthy_instance_reports_no_failure() {
    let (vfs, _gate) = gate::vfs(1500);
    let db = Engine::open(Path::new(DB), options(vfs, 2)).unwrap();
    let t = table(&db, "t");
    db.commit(put(&t, "a"), Some(Durability::Sync)).unwrap();
    assert!(!db.is_failed());
    assert_eq!(db.failure(), None);
    db.close().unwrap();
}

#[test]
fn a_panicked_shard_is_reported_as_a_panic() {
    let (vfs, gate) = gate::vfs(1501);
    let db = Engine::open(Path::new(DB), options(vfs, 2)).unwrap();
    let t = table(&db, "t");
    let s = shard_of(&db, &t);
    gate.panic(&wal(s));
    let db2 = Arc::clone(&db);
    let t2 = Arc::clone(&t);
    let committed = gate::within(10, move || {
        db2.commit(put(&t2, "boom"), Some(Durability::Sync))
    })
    .expect("a commit to the panicked shard hung");
    // The shard died with the commit in flight, maybe after writing it: the outcome is the
    // log's, and the failure is recorded by the time the commit returns.
    assert!(
        matches!(committed, Err(pigeonhole_engine::Error::OutcomeUnknown)),
        "{committed:?}"
    );
    assert!(db.is_failed());
    assert_eq!(db.failure(), failed(s, FailureCause::Panic));
    let _ = gate::within(10, move || db.close()).expect("close hung after a shard panic");
}

#[test]
fn a_failed_wal_sync_is_reported_as_io_and_the_first_failure_stays() {
    let (vfs, gate) = gate::vfs(1502);
    let db = Engine::open(Path::new(DB), options(vfs, 2)).unwrap();
    let t = table(&db, "t");
    let s = shard_of(&db, &t);
    gate.fail(&wal(s));
    let db2 = Arc::clone(&db);
    let t2 = Arc::clone(&t);
    let committed = gate::within(10, move || {
        db2.commit(put(&t2, "lost"), Some(Durability::Sync))
    })
    .expect("a commit with a failing sync hung");
    assert!(committed.is_err(), "{committed:?}");
    assert_eq!(db.failure(), failed(s, FailureCause::Io));
    // A later failure (an apply on another shard) does not replace the first.
    db.fail_next_apply();
    let other = table(&db, "other");
    let _ = db.commit(put(&other, "x"), Some(Durability::Buffered));
    assert_eq!(db.failure(), failed(s, FailureCause::Io));
    let _ = gate::within(10, move || db.close()).expect("close hung");
}

#[test]
fn a_failed_apply_is_reported_as_internal() {
    let (vfs, _gate) = gate::vfs(1503);
    let db = Engine::open(Path::new(DB), options(vfs, 2)).unwrap();
    let t = table(&db, "t");
    let s = shard_of(&db, &t);
    db.fail_next_apply();
    // Logged, then not applied: the outcome is the log's (ICR 0038).
    let committed = db.commit(put(&t, "torn"), Some(Durability::Buffered));
    assert!(
        matches!(committed, Err(pigeonhole_engine::Error::OutcomeUnknown)),
        "{committed:?}"
    );
    assert!(db.is_failed());
    assert_eq!(db.failure(), failed(s, FailureCause::Internal));
    let _ = gate::within(10, move || db.close()).expect("close hung");
}

#[test]
fn a_dropped_application_owned_driver_is_reported() {
    let (vfs, _gate) = gate::vfs(1504);
    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), options(vfs, 2)).unwrap();
    assert!(!db.is_failed());
    // Shard 1's driver goes before the close.
    drop(shards.pop());
    assert!(db.is_failed());
    assert_eq!(db.failure(), failed(1, FailureCause::DriverDropped));
    let _ = db.close();
    drop(shards);
}

#[test]
fn a_failed_manifest_commit_is_reported_against_the_main_file() {
    let (vfs, gate) = gate::vfs(1505);
    let db = Engine::open(Path::new(DB), options(vfs, 2)).unwrap();
    table(&db, "t");
    gate.fail(Path::new(DB));
    let created = db.create_table("u", &[("f".into(), FamilyOptions::default())]);
    assert!(created.is_err(), "{created:?}");
    assert_eq!(db.failure(), failed(Failure::MAIN_FILE, FailureCause::Io));
    let _ = gate::within(10, move || db.close()).expect("close hung");
}

#[test]
fn a_failed_tablet_change_is_reported_as_internal() {
    let (vfs, _gate) = gate::vfs(1507);
    let db = Engine::open(Path::new(DB), options(vfs, 2)).unwrap();
    let t = table(&db, "t");
    let s = shard_of(&db, &t);
    db.commit(put(&t, "a"), Some(Durability::Buffered)).unwrap();
    db.fail_next_tablet_freeze();
    let split = db.split_table(t.id, vec![b"m".to_vec()], &[s as u16, s as u16]);
    assert!(split.is_err(), "{split:?}");
    assert!(db.is_failed());
    assert_eq!(db.failure(), failed(s, FailureCause::Internal));
    let _ = gate::within(10, move || db.close()).expect("close hung");
}
