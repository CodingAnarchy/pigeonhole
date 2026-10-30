//! #833: the writer frees the slot of a reader process that is gone. A killed reader's pin
//! used to hold the writer's retired memtables (and its view versions, extents and GC
//! bound) until the next writer generation; a live reader's pin is never touched (judged by
//! its slot's lock since #693 PR 3, so whatever its pid looks like).

use crate::common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, TableInfo, ValueRef, WriteBatch};
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

/// #693 PR 3: liveness is the slot's lock, not the pid. A reader whose pid the writer cannot
/// see as alive (a pid `/proc` hides, another PID namespace: here a process the simulator
/// reports dead while its handles keep working) keeps its slot. Its pin holds past every
/// reclamation, its reads through the old snapshot keep working, and its drop frees the slot.
/// (Under the pid probe, #834 review R1b, such a reader lost its slot and its snapshots
/// expired.)
#[test]
fn a_reader_whose_pid_looks_dead_keeps_its_slot() {
    let vfs = SimVfs::new(834);
    vfs.enter_process(WRITER);
    let db = Engine::open(Path::new(DB), options(&vfs)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let (tid, fid) = (t.id, t.families[0].id);
    write_rows(&db, &t, 0..500);

    vfs.enter_process(READER);
    let hidden = Engine::open_reader(Path::new(DB), options(&vfs)).unwrap();
    let old = hidden.snapshot().unwrap();
    vfs.enter_process(WRITER);
    let (Some(pinned), _) = db.reader_pin_and_view() else {
        panic!("the reader pins nothing");
    };

    // Its handle still holds the slot's liveness lock (whatever its pid says): past the
    // interval, reclamations keep the slot and its pin.
    for round in 0..3u32 {
        vfs.advance(PAST_INTERVAL);
        write_rows(&db, &t, 500 + round * 300..800 + round * 300);
        db.flush().unwrap();
    }
    assert_eq!(
        db.reader_pin_and_view().0,
        Some(pinned),
        "a live reader's slot was freed"
    );

    // Its old snapshot still reads, and a new one reads the newest rows.
    vfs.enter_process(READER);
    assert!(
        hidden
            .get(&old, tid, fid, b"row00001", b"q")
            .unwrap()
            .is_some()
    );
    assert!(
        hidden
            .get(&old, tid, fid, b"row01300", b"q")
            .unwrap()
            .is_none(),
        "the old snapshot sees a later row"
    );
    let fresh = hidden.snapshot().unwrap();
    assert!(
        hidden
            .get(&fresh, tid, fid, b"row01300", b"q")
            .unwrap()
            .is_some()
    );

    // Dropping it frees the slot.
    drop(old);
    drop(fresh);
    drop(hidden);
    vfs.enter_process(WRITER);
    assert_eq!(
        db.reader_pin_and_view().0,
        None,
        "the dropped reader still pins"
    );
    db.close().unwrap();
}
