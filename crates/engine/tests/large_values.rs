//! Values above the inline limit (#230): separated into blob files when the batch is routed,
//! read back through every path, released when the commit is refused, and swept at open
//! when the commit was lost. A test hook shrinks the inline limit so small values take the
//! path (the real limit is tens of MiB; `huge_value.rs` runs one real round trip).

mod common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyOptions, Predicate, TableInfo, ValuePredicate, ValueRef,
    WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::Vfs;
use pigeonhole_io::sim::{CrashKind, SimVfs};

const DB: &str = "/db/data.phdb";
/// Payload bytes a commit carries inline in these tests.
const LIMIT: usize = 200;

fn options(vfs: &Arc<SimVfs>) -> EngineOptions {
    let mut o = common::options(Arc::clone(vfs), 1, 16 << 20);
    o.compaction.l0_trigger = u32::MAX;
    o.compaction.level_base_bytes = u64::MAX;
    o
}

fn open(vfs: &Arc<SimVfs>) -> Arc<Engine> {
    let db = Engine::open(Path::new(DB), options(vfs)).unwrap();
    db.set_inline_value_limit(LIMIT);
    db
}

/// Row `i`'s value: above the limit, except every fourth row.
fn value(i: u32, generation: u8) -> Vec<u8> {
    let len = if i.is_multiple_of(4) {
        50
    } else {
        300 + i as usize * 7
    };
    let mut v = vec![b'a' + generation; len];
    v[..4].copy_from_slice(&i.to_le_bytes());
    v
}

fn row(i: u32) -> Vec<u8> {
    format!("row{i:04}").into_bytes()
}

fn put(db: &Engine, t: &TableInfo, rows: std::ops::Range<u32>, generation: u8, d: Durability) {
    for i in rows {
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            t.families[0].id,
            &row(i),
            b"q",
            None,
            ValueRef::Bytes(&value(i, generation)),
        )
        .unwrap();
        db.commit(wb, Some(d)).unwrap();
    }
}

fn assert_reads(db: &Engine, t: &TableInfo, rows: u32, expected: impl Fn(u32) -> Vec<u8>) {
    let f = t.families[0].id;
    let snap = db.snapshot().unwrap();
    for i in 0..rows {
        let got = db.get(&snap, t.id, f, &row(i), b"q").unwrap().unwrap();
        assert_eq!(got.value(), ValueRef::Bytes(&expected(i)), "row {i}");
    }
}

/// Waits (bounded) until nothing the pager holds is unreferenced: a view a shard holds for a
/// moment keeps retired extents until it goes.
fn wait_for_unreferenced_zero(db: &Engine) {
    for _ in 0..2_000 {
        if db.unreferenced_bytes() == 0 {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    panic!("{} bytes still unreferenced", db.unreferenced_bytes());
}

/// Waits (bounded) for the manifest to drop blob files a refused commit released.
fn wait_for_blob_files(db: &Engine, n: usize) {
    for _ in 0..10_000 {
        if db.blob_files().len() == n {
            return;
        }
        std::thread::yield_now();
    }
    panic!("blob files: {:?}, expected {n}", db.blob_files());
}

#[test]
fn values_above_the_inline_limit_round_trip() {
    let vfs = SimVfs::new(230);
    let db = open(&vfs);
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    put(&db, &t, 0..40, 0, Durability::Sync);
    let files = db.blob_files().len();
    assert_eq!(files, 30, "one blob file per commit with a large value");
    // In the memtables: the pointers resolve, and the files' bytes are what they hold.
    db.check_blob_accounting().unwrap();
    assert_reads(&db, &t, 40, |i| value(i, 0));
    db.flush().unwrap();
    db.check_blob_accounting().unwrap();
    assert_reads(&db, &t, 40, |i| value(i, 0));
    // Overwritten and compacted: the old values' files go.
    put(&db, &t, 0..40, 1, Durability::Buffered);
    db.flush().unwrap();
    db.compact(None).unwrap();
    db.check_blob_accounting().unwrap();
    assert_reads(&db, &t, 40, |i| value(i, 1));
    db.close().unwrap();
    let db = open(&vfs);
    let t = db.table("t").unwrap();
    db.check_blob_accounting().unwrap();
    assert_reads(&db, &t, 40, |i| value(i, 1));
    db.close().unwrap();
}

#[test]
fn a_refused_commit_releases_its_blob_files() {
    let vfs = SimVfs::new(231);
    let db = open(&vfs);
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    put(&db, &t, 1..2, 0, Durability::Sync);
    assert_eq!(db.blob_files().len(), 1);

    // An optimistic transaction that conflicts.
    let mut txn = db.begin().unwrap();
    txn.get(t.id, f, &row(1), b"q").unwrap();
    txn.batch()
        .put(t.id, f, &row(1), b"q", None, ValueRef::Bytes(&value(1, 5)))
        .unwrap();
    put(&db, &t, 1..2, 1, Durability::Sync);
    assert!(matches!(txn.commit(None), Err(Error::Conflict)));
    db.check_blob_accounting().unwrap();
    wait_for_blob_files(&db, 2);

    // A `check_and_mutate` whose predicate is false.
    let mut wb = WriteBatch::new();
    wb.put(t.id, f, &row(1), b"q", None, ValueRef::Bytes(&value(1, 6)))
        .unwrap();
    let predicate = Predicate::Value {
        family: f,
        qualifier: b"q".to_vec(),
        predicate: ValuePredicate::Equals(b"no such value".to_vec()),
    };
    assert!(
        !db.check_and_mutate(t.id, &row(1), &predicate, wb, None)
            .unwrap()
            .0
    );
    wait_for_blob_files(&db, 2);
    db.check_blob_accounting().unwrap();
    let snap = db.snapshot().unwrap();
    let got = db.get(&snap, t.id, f, &row(1), b"q").unwrap().unwrap();
    assert_eq!(
        got.value(),
        ValueRef::Bytes(&value(1, 1)),
        "the committed value stays"
    );
    drop(snap);
    db.close().unwrap();
}

#[test]
fn a_lost_commit_leaves_a_blob_file_the_open_sweeps() {
    // A `Durability::None` commit writes no WAL record: its value's blob file is in the
    // manifest, but a power loss before a flush loses the only pointer into it.
    let vfs = SimVfs::new(232);
    let db = open(&vfs);
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    put(&db, &t, 1..2, 0, Durability::Sync);
    put(&db, &t, 2..4, 0, Durability::None);
    assert_eq!(db.blob_files().len(), 3);
    vfs.crash(CrashKind::Power);
    drop(db);
    let db = open(&vfs);
    let t = db.table("t").unwrap();
    assert_eq!(
        db.blob_files().len(),
        1,
        "the lost commits' files are swept"
    );
    db.check_blob_accounting().unwrap();
    let snap = db.snapshot().unwrap();
    let f = t.families[0].id;
    assert_eq!(
        db.get(&snap, t.id, f, &row(1), b"q")
            .unwrap()
            .unwrap()
            .value(),
        ValueRef::Bytes(&value(1, 0))
    );
    assert!(db.get(&snap, t.id, f, &row(2), b"q").unwrap().is_none());
    drop(snap);
    db.shrink().unwrap();
    // A shard may hold an older view for a moment (scoring its slots after a commit), and
    // what that view keeps stays retired until it goes; it is reclaimed then, with nothing
    // else running (`tests/reclaim.rs`).
    wait_for_unreferenced_zero(&db);
    db.close().unwrap();
}

#[test]
fn a_merge_operand_above_the_limit_is_refused() {
    let vfs = SimVfs::new(233);
    let db = open(&vfs);
    let family = FamilyOptions::default().merge_operator("pigeonhole.i64_add");
    let t = db.create_table("t", &[("f".into(), family)]).unwrap();
    let mut wb = WriteBatch::new();
    wb.merge(
        t.id,
        t.families[0].id,
        b"r",
        b"q",
        ValueRef::Bytes(&[7; 1000]),
    )
    .unwrap();
    assert!(matches!(db.commit(wb, None), Err(Error::ValueTooLarge)));
    assert!(db.blob_files().is_empty());
    db.close().unwrap();
}

#[test]
fn a_commit_that_fails_after_its_wal_append_keeps_its_blob_files() {
    // The batch is logged, then its apply fails with `Busy` (as an arena miscount would):
    // the error is one a refused commit also returns, but the record is in the WAL, so the
    // value's file must stay for the replay to find.
    let vfs = SimVfs::new(234);
    let db = open(&vfs);
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    db.fail_next_apply();
    let mut wb = WriteBatch::new();
    wb.put(t.id, f, &row(1), b"q", None, ValueRef::Bytes(&value(1, 0)))
        .unwrap();
    assert!(matches!(
        db.commit(wb, Some(Durability::Sync)),
        Err(Error::Busy)
    ));
    assert_eq!(db.blob_files().len(), 1, "the logged commit's file stays");
    // A power loss: the synced record is replayed (a close of the poisoned shard could
    // move its checkpoint past the record instead).
    vfs.crash(CrashKind::Power);
    drop(db);
    let db = open(&vfs);
    assert_eq!(db.blob_files().len(), 1);
    db.check_blob_accounting().unwrap();
    let snap = db.snapshot().unwrap();
    let got = db.get(&snap, t.id, f, &row(1), b"q").unwrap().unwrap();
    assert_eq!(
        got.value(),
        ValueRef::Bytes(&value(1, 0)),
        "the replay applied it"
    );
    drop(snap);
    db.close().unwrap();
}

#[test]
fn a_value_the_same_commit_collapse_drops_releases_its_blob_file() {
    // D34: an explicit timestamp equal to the commit's own collides with a default one, and
    // only the shard (which assigns the commit timestamp) can tell. The displaced value's
    // file goes, and the bytes counted live are the winner's.
    let vfs = SimVfs::new(235);
    let db = open(&vfs);
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    put(&db, &t, 2..3, 0, Durability::Sync);
    // A default timestamp is the simulated clock at submission, once past the shard's floor.
    let floor = db.max_ts_floor();
    if floor >= vfs.now_micros() {
        vfs.advance(1_000 * (floor - vfs.now_micros() + 1));
    }
    let now = vfs.now_micros();
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        f,
        &row(1),
        b"q",
        Some(now),
        ValueRef::Bytes(&value(1, 0)),
    )
    .unwrap();
    wb.put(t.id, f, &row(1), b"q", None, ValueRef::Bytes(&value(1, 1)))
        .unwrap();
    db.commit(wb, Some(Durability::Sync)).unwrap();
    // The displaced value's file is released; the winner's and row 2's stay.
    wait_for_blob_files(&db, 2);
    db.check_blob_accounting().unwrap();
    let snap = db.snapshot().unwrap();
    let got = db.get(&snap, t.id, f, &row(1), b"q").unwrap().unwrap();
    assert_eq!(got.timestamp(), now, "the timestamps collided");
    assert_eq!(
        got.value(),
        ValueRef::Bytes(&value(1, 1)),
        "the later put wins"
    );
    drop(snap);
    db.close().unwrap();
    let db = open(&vfs);
    db.check_blob_accounting().unwrap();
    assert_eq!(db.blob_files().len(), 2);
    db.close().unwrap();
}
