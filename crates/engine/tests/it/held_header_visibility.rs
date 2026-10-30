//! D227 with D200: a commit is visible once applied, except in a group whose records the WAL
//! wrote under a held-back segment header. The full segment's sync is in flight, the
//! successor's header waits for it, and a process crash then loses those records with the
//! header (`WalStream::written` stays at the full segment's end). A read must not see such a
//! commit, or a process crash (which loses no visible commit, D227) would take back what it
//! saw. Deterministic: one shard driven by hand, the simulator's I/O deferred until the test
//! completes it, a fresh run per crash point.

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::{CrashKind, SimVfs};

const DB: &str = "/db/data.phdb";
/// Three 32 KiB frames: a few commits fill a segment, and values up to 32 KiB stay inline.
const SEGMENT: u64 = 3 * 32 * 1024;
const COMMITS: u32 = 40;

/// Runs the shard until it has nothing left to do.
fn idle(shard: &mut EngineShard) {
    while shard.run_once(u64::MAX) {}
}

fn open(vfs: &Arc<SimVfs>) -> (Arc<Engine>, Vec<EngineShard>) {
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.wal.segment_size = SEGMENT;
    o.wal.spare_segments = 1;
    Engine::open_application_owned(Path::new(DB), o).unwrap()
}

fn put(t: &TableInfo, i: u32, value: &[u8]) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        &i.to_be_bytes(),
        b"q",
        None,
        ValueRef::Bytes(value),
    )
    .unwrap();
    wb
}

fn visible(db: &Engine, t: &TableInfo, i: u32) -> bool {
    let snap = db.snapshot().unwrap();
    db.get(&snap, t.id, t.families[0].id, &i.to_be_bytes(), b"q")
        .unwrap()
        .is_some()
}

/// Commits `0..last` with their I/O completed, then commit `last` with its turn run but no
/// I/O completed. If a read sees `last`, a process crash must keep it. Returns whether the
/// read saw it.
fn run(last: u32) -> bool {
    let vfs = SimVfs::new(u64::from(last));
    let (db, mut shards) = open(&vfs);
    let shard = &mut shards[0];
    let t = db
        .create_table("t", &[("f".to_owned(), FamilyOptions::default())])
        .unwrap();
    idle(shard);
    vfs.set_deferred_io(true);
    // Incompressible, so the WAL record keeps its size.
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    let value: Vec<u8> = (0..4000)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect();
    for i in 0..last {
        drop(
            db.submit(put(&t, i, &value), Some(Durability::Buffered))
                .unwrap(),
        );
        idle(shard);
        vfs.complete_all_io();
        idle(shard);
    }
    drop(
        db.submit(put(&t, last, &value), Some(Durability::Buffered))
            .unwrap(),
    );
    idle(shard);
    let seen = visible(&db, &t, last);
    vfs.crash(CrashKind::Process);
    drop(shards);
    drop(db);
    vfs.set_deferred_io(false);
    vfs.complete_all_io();
    let (db, mut shards) = open(&vfs);
    let shard = &mut shards[0];
    idle(shard);
    let t = db.table("t").expect("the table survives");
    if seen {
        assert!(
            visible(&db, &t, last),
            "commit {last} was visible to a read but did not survive a process crash"
        );
    }
    seen
}

#[test]
fn a_commit_under_a_held_segment_header_is_not_visible_until_the_header_is_written() {
    // Commits that land in a full segment's successor while its sync is in flight are the
    // ones a read must not see yet; every other Buffered commit is visible after its turn.
    let hidden: Vec<u32> = (0..COMMITS).filter(|&last| !run(last)).collect();
    assert!(
        !hidden.is_empty(),
        "no commit landed under a held segment header (the window was never reached)"
    );
    eprintln!("hidden under a held header at commits {hidden:?}");
}
