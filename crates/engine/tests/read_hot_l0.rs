//! #759: a slot whose readers keep probing its L0 files compacts L0 at half the trigger (2
//! files of 4), while a slot nobody reads keeps the full trigger. Storage is `SimVfs`
//! behind the test gate (`tests/gate`), on a real clock, with engine-owned shard threads.

mod gate;

use std::path::Path;
use std::time::{Duration, Instant};

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, TableInfo, ValueRef, WriteBatch};
use pigeonhole_format::Durability;

const DB: &str = "/db/read_hot.phdb";

fn write(db: &Engine, t: &TableInfo, round: u32) {
    let mut wb = WriteBatch::new();
    for row in 0..64u32 {
        let v = format!("v{round}");
        wb.put(
            t.id,
            t.families[0].id,
            format!("row{row:04}").as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(v.as_bytes()),
        )
        .unwrap();
    }
    db.commit(wb, Some(Durability::Buffered)).unwrap();
}

#[test]
fn a_read_hot_slot_compacts_l0_at_two_files_and_a_write_only_one_waits_for_four() {
    let (vfs, _gate) = gate::vfs(759);
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.memtable_budget = 1 << 20;
    o.wal.segment_size = 256 << 10;
    o.wal.spare_segments = 1;
    o.tablet_changes = false;
    assert_eq!(o.compaction.l0_trigger, 4);
    assert!(o.compaction.read_hot_l0);
    let db = Engine::open(Path::new(DB), o).unwrap();
    let hot = db
        .create_table("hot", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let cold = db
        .create_table("cold", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let l0 = |t: &TableInfo| {
        db.sst_levels()
            .into_iter()
            .filter(|s| s.0 == t.id && s.1 == 0)
            .count()
    };
    write(&db, &hot, 1);
    write(&db, &cold, 1);
    db.flush().unwrap();
    assert_eq!((l0(&hot), l0(&cold)), (1, 1));
    // Reads of `hot` probe its L0 file (sampled 1 in 16, so read well past that).
    let snap = db.snapshot().unwrap();
    for i in 0..2000u32 {
        let row = format!("row{:04}", i % 64);
        assert!(
            db.get(&snap, hot.id, hot.families[0].id, row.as_bytes(), b"q")
                .unwrap()
                .is_some()
        );
    }
    drop(snap);
    write(&db, &hot, 2);
    write(&db, &cold, 2);
    db.flush().unwrap();
    // `hot`'s two L0 files compact into L1; `cold`'s stay (2 of 4).
    let deadline = Instant::now() + Duration::from_secs(20);
    while l0(&hot) != 0 {
        assert!(
            Instant::now() < deadline,
            "the read-hot slot kept {} L0 files",
            l0(&hot)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(l0(&cold), 2, "a slot nobody reads keeps the full trigger");
}

/// Reads `t`'s 64 rows often enough that its L0 counts as read-hot (probes are sampled 1 in 16).
fn read_hot(db: &Engine, t: &TableInfo) {
    let snap = db.snapshot().unwrap();
    for i in 0..2000u32 {
        let row = format!("row{:04}", i % 64);
        assert!(
            db.get(&snap, t.id, t.families[0].id, row.as_bytes(), b"q")
                .unwrap()
                .is_some()
        );
    }
}

#[test]
fn a_read_hot_slot_waits_while_another_shard_compacts() {
    let (vfs, _gate) = gate::vfs(7591);
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.memtable_budget = 1 << 20;
    o.wal.segment_size = 256 << 10;
    o.wal.spare_segments = 1;
    o.tablet_changes = false;
    let db = Engine::open(Path::new("/db/read_hot_cap.phdb"), o).unwrap();
    // A write-only table and a read-hot one, on different shards.
    let mut tables: Vec<(std::sync::Arc<TableInfo>, usize)> = Vec::new();
    for i in 0..16 {
        let t = db
            .create_table(&format!("t{i}"), &[("f".into(), FamilyOptions::default())])
            .unwrap();
        let shard = db.shard_of(t.id, b"row0000").unwrap();
        tables.push((t, shard));
    }
    let (writer, ws) = tables.remove(0);
    let (hot, _) = tables
        .into_iter()
        .find(|(_, s)| *s != ws)
        .expect("a table on the other shard");
    let l0 = |t: &TableInfo| {
        db.sst_levels()
            .into_iter()
            .filter(|s| s.0 == t.id && s.1 == 0)
            .count()
    };
    let wait = |what: &str, done: &dyn Fn() -> bool| {
        let deadline = Instant::now() + Duration::from_secs(20);
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(10));
        }
    };
    // The write-only table's fourth L0 file starts a compaction, held before its first slice.
    db.hold_compactions(true);
    for r in 1..=4 {
        write(&db, &writer, r);
        db.flush().unwrap();
    }
    wait("the write-only compaction", &|| db.compaction_held());
    // The read-hot table reaches its lower trigger (2 files) while that compaction runs.
    write(&db, &hot, 1);
    db.flush().unwrap();
    read_hot(&db, &hot);
    write(&db, &hot, 2);
    db.flush().unwrap();
    std::thread::sleep(Duration::from_millis(300));
    // Only the write-only compaction is held: let it run and finish.
    db.release_held_compaction();
    wait("the write-only compaction to finish", &|| l0(&writer) == 0);
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        !db.compaction_held(),
        "the read-hot slot started a compaction while another shard's ran"
    );
    assert_eq!(l0(&hot), 2);
    // With nothing else running, its next flush compacts it at the lower trigger.
    read_hot(&db, &hot);
    write(&db, &hot, 3);
    db.flush().unwrap();
    db.hold_compactions(false);
    wait("the read-hot compaction", &|| l0(&hot) == 0);
}
