//! #833: the writer frees the slot of a reader process that is gone. A killed reader's pin
//! used to hold the writer's retired memtables (and its view versions, extents and GC
//! bound) until the next writer generation; a live reader's pin is never touched.

mod common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyOptions, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::ProcessId;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";
const WRITER: ProcessId = ProcessId {
    pid: 1,
    start_time: 1,
};
const READER: ProcessId = ProcessId {
    pid: 2,
    start_time: 1,
};
const OTHER_READER: ProcessId = ProcessId {
    pid: 3,
    start_time: 1,
};

fn options(vfs: &Arc<SimVfs>) -> EngineOptions {
    let mut o = common::options(Arc::clone(vfs), 1, 2 << 20);
    o.memtable_freeze_bytes = 1 << 20;
    o.tablet_changes = false;
    o
}

fn write_rows(db: &Engine, t: &TableInfo, range: std::ops::Range<u32>) {
    for i in range {
        let mut wb = WriteBatch::new();
        let row = format!("row{i:05}");
        wb.put(
            t.id,
            t.families[0].id,
            row.as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&[(i % 251) as u8; 300]),
        )
        .unwrap();
        db.commit(wb, Some(Durability::Buffered)).unwrap();
    }
}

/// More than the liveness-check interval (250 ms).
const PAST_INTERVAL: u64 = 300_000_000;

#[test]
fn a_killed_readers_slot_is_freed_and_a_live_readers_is_kept() {
    let vfs = SimVfs::new(833);
    vfs.enter_process(WRITER);
    let db = Engine::open(Path::new(DB), options(&vfs)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    write_rows(&db, &t, 0..500);

    // Two reader processes, each holding a snapshot (and so a pin) of the current view.
    vfs.enter_process(READER);
    let doomed = Engine::open_reader(Path::new(DB), options(&vfs)).unwrap();
    let doomed_snap = doomed.snapshot().unwrap();
    vfs.enter_process(OTHER_READER);
    let live = Engine::open_reader(Path::new(DB), options(&vfs)).unwrap();
    let live_snap = live.snapshot().unwrap();
    vfs.enter_process(WRITER);
    let (Some((_, pinned_view)), _) = db.reader_pin_and_view() else {
        panic!("the readers pin nothing");
    };

    // The writer retires memtables the pins cover, and checks liveness past its interval:
    // both readers are alive, so both pins stay.
    write_rows(&db, &t, 500..1500);
    db.flush().unwrap();
    vfs.advance(PAST_INTERVAL);
    write_rows(&db, &t, 1500..1600);
    db.flush().unwrap();
    assert_eq!(
        db.reader_pin_and_view().0.map(|(_, v)| v),
        Some(pinned_view),
        "a live reader's pin was dropped"
    );

    // The doomed reader is killed (SIGKILL: it runs no destructors, so its handles are
    // forgotten, not dropped). The live reader moves on to a new snapshot, so the only pin at
    // the old view is the dead one.
    vfs.crash_process(READER);
    std::mem::forget(doomed_snap);
    std::mem::forget(doomed);
    vfs.enter_process(OTHER_READER);
    drop(live_snap);
    let live_snap = live.snapshot().unwrap();
    vfs.enter_process(WRITER);
    let (Some((_, live_view)), _) = db.reader_pin_and_view() else {
        panic!("the live reader pins nothing");
    };
    assert_eq!(
        live_view, pinned_view,
        "before the writer notices, the dead pin holds"
    );

    // Past the interval the writer's next reclamation frees the dead reader's slot; the
    // oldest pin is the live reader's newer view.
    vfs.advance(PAST_INTERVAL);
    write_rows(&db, &t, 1600..1700);
    db.flush().unwrap();
    let (pin, view) = db.reader_pin_and_view();
    let (_, oldest) = pin.expect("the live reader still pins");
    assert!(
        oldest > pinned_view && oldest <= view,
        "the dead reader still pins view {pinned_view}: oldest {oldest}, current {view}"
    );

    // A new reader process claims a slot and reads; the live reader still reads.
    vfs.enter_process(ProcessId {
        pid: 4,
        start_time: 1,
    });
    let next = Engine::open_reader(Path::new(DB), options(&vfs)).unwrap();
    let snap = next.snapshot().unwrap();
    assert!(
        next.get(&snap, t.id, t.families[0].id, b"row01650", b"q")
            .unwrap()
            .is_some()
    );
    drop(snap);
    drop(next);
    vfs.enter_process(OTHER_READER);
    assert!(
        live.get(&live_snap, t.id, t.families[0].id, b"row00001", b"q")
            .unwrap()
            .is_some()
    );
    drop(live_snap);
    drop(live);
    vfs.enter_process(WRITER);
    db.close().unwrap();
}

/// #834 review, R1b: a reader judged dead while it is alive (a pid `/proc` hides, another
/// PID namespace) loses its slot, and that costs it an expired snapshot, never wrong rows or
/// another reader's slot: its reads through the old snapshot fail `SnapshotExpired`, its
/// next snapshot claims a fresh slot without touching the pins of the reader that took the
/// old one, and dropping it leaves that reader's slot alone.
#[test]
fn a_falsely_dead_reader_expires_and_never_touches_the_slot_reused_under_it() {
    let vfs = SimVfs::new(834);
    vfs.enter_process(WRITER);
    let db = Engine::open(Path::new(DB), options(&vfs)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let (tid, fid) = (t.id, t.families[0].id);
    write_rows(&db, &t, 0..500);

    vfs.enter_process(READER);
    let zombie = Engine::open_reader(Path::new(DB), options(&vfs)).unwrap();
    let old = zombie.snapshot().unwrap();
    assert!(
        zombie
            .get(&old, tid, fid, b"row00001", b"q")
            .unwrap()
            .is_some()
    );

    // The writer comes to believe the reader is dead (its handles keep working) and frees
    // its slot at the next reclamation past the interval.
    vfs.kill_process(READER);
    vfs.enter_process(WRITER);
    vfs.advance(PAST_INTERVAL);
    write_rows(&db, &t, 500..1500);
    db.flush().unwrap();
    assert_eq!(db.reader_pin_and_view().0, None, "the slot was not freed");

    // Its reads through the old snapshot now fail, typed.
    vfs.enter_process(READER);
    assert!(matches!(
        zombie.get(&old, tid, fid, b"row00001", b"q"),
        Err(Error::SnapshotExpired)
    ));

    // Another reader takes the freed slot and pins the current view.
    vfs.enter_process(OTHER_READER);
    let other = Engine::open_reader(Path::new(DB), options(&vfs)).unwrap();
    let other_snap = other.snapshot().unwrap();
    vfs.enter_process(WRITER);
    let (Some(other_pin), _) = db.reader_pin_and_view() else {
        panic!("the new reader pins nothing");
    };

    // The zombie's next snapshot claims a fresh slot and reads; the other reader's pin is
    // untouched.
    vfs.enter_process(READER);
    let fresh = zombie.snapshot().unwrap();
    assert!(
        zombie
            .get(&fresh, tid, fid, b"row01400", b"q")
            .unwrap()
            .is_some()
    );
    vfs.enter_process(WRITER);
    assert_eq!(db.reader_pin_and_view().0, Some(other_pin));

    // Dropping every zombie handle releases only what is still its own.
    vfs.enter_process(READER);
    drop(old);
    drop(fresh);
    drop(zombie);
    vfs.enter_process(WRITER);
    assert_eq!(
        db.reader_pin_and_view().0,
        Some(other_pin),
        "the zombie's drop freed or cleared the other reader's slot"
    );
    vfs.enter_process(OTHER_READER);
    assert!(
        other
            .get(&other_snap, tid, fid, b"row01400", b"q")
            .unwrap()
            .is_some()
    );
    drop(other_snap);
    drop(other);
    vfs.enter_process(WRITER);
    db.close().unwrap();
}
