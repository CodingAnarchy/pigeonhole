//! Per-shard visibility (#717 option (b2), `proto/717b`): a single-shard commit is visible
//! once its own shard applied it, whatever other shards hold; a cross-shard commit is still
//! shown whole or not at all, and the held-slot overflow falls back to a global floor.

#![cfg(any(target_os = "linux", target_os = "macos"))]

mod gate;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, TableInfo, ValueRef, WriteBatch};
use pigeonhole_format::Durability;

const DB: &str = "/db/cut.phdb";

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

/// A cross-shard batch with a row of `first` (whose shard coordinates it) and one of `second`.
fn cross(first: &TableInfo, second: &TableInfo, row: &str) -> WriteBatch {
    let mut wb = put(first, row);
    wb.put(
        second.id,
        second.families[0].id,
        row.as_bytes(),
        b"q",
        None,
        ValueRef::Bytes(b"v"),
    )
    .unwrap();
    wb
}

fn has(db: &Engine, snap: &pigeonhole_engine::Snapshot, t: &TableInfo, row: &str) -> bool {
    db.get(snap, t.id, t.families[0].id, row.as_bytes(), b"q")
        .unwrap()
        .is_some()
}

/// Opens a `shards`-shard engine over a gate VFS and creates one table per shard, returned
/// by shard index.
fn open(seed: u64, shards: usize) -> (Arc<Engine>, Arc<gate::Gate>, Vec<Arc<TableInfo>>) {
    let (vfs, gate) = gate::vfs(seed);
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = shards;
    o.pin_threads = false;
    o.memtable_budget = 4 << 20;
    o.wal.segment_size = 256 << 10;
    o.wal.spare_segments = 1;
    let db = Engine::open(Path::new(DB), o).unwrap();
    db.record_history(true);
    let mut by_shard: Vec<Option<Arc<TableInfo>>> = vec![None; shards];
    for i in 0..64 {
        if by_shard.iter().all(Option::is_some) {
            break;
        }
        let t = db
            .create_table(&format!("t{i}"), &[("f".into(), FamilyOptions::default())])
            .unwrap();
        db.take_appended();
        db.commit(put(&t, "probe"), Some(Durability::Buffered))
            .unwrap();
        let shard = db.take_appended().last().expect("one record").stream as usize;
        if by_shard[shard].is_none() {
            by_shard[shard] = Some(t);
        }
    }
    let tables = by_shard
        .into_iter()
        .map(|t| t.expect("a table on every shard"))
        .collect();
    (db, gate, tables)
}

/// A single on shard A returns, and is read, while a cross-shard commit shard B coordinates
/// is held (its seqno reserved, its COMMIT sync in flight); the held commit's cells are
/// absent on both shards and its seqno is excluded by the cut.
#[test]
fn a_single_is_visible_on_its_shard_while_another_shards_cross_shard_commit_is_held() {
    let (db, gate, tables) = open(2401, 2);
    let (a, b) = (Arc::clone(&tables[0]), Arc::clone(&tables[1]));
    gate.hold(&wal(1));
    let held = db
        .submit(cross(&b, &a, "held"), Some(Durability::Sync))
        .unwrap();
    gate.wait_held(&wal(1));
    // Today the single waits for the held seqno (D19); now it returns at once.
    let (db2, a2) = (Arc::clone(&db), Arc::clone(&a));
    let info = gate::within(5, move || {
        db2.commit(put(&a2, "single"), Some(Durability::None))
            .unwrap()
    })
    .expect("the single returned while the cross-shard commit stayed held");
    let snap = db.snapshot().unwrap();
    assert!(has(&db, &snap, &a, "single"), "read-your-writes");
    assert!(!has(&db, &snap, &a, "held"));
    assert!(!has(&db, &snap, &b, "held"));
    assert!(
        !snap.cut().excluded().is_empty(),
        "the held seqno is excluded: {:?}",
        snap.cut()
    );
    assert!(
        snap.cut().includes(info.shard, info.seqno),
        "{:?}",
        snap.cut()
    );
    assert!(
        snap.floor() < info.seqno,
        "the floor stays below the held seqno: floor {} single {}",
        snap.floor(),
        info.seqno
    );
    drop(snap);
    gate.release();
    held.wait().unwrap();
    let snap = db.snapshot().unwrap();
    assert!(has(&db, &snap, &a, "single"));
    assert!(has(&db, &snap, &a, "held"));
    assert!(has(&db, &snap, &b, "held"));
    assert!(snap.cut().excluded().is_empty());
    drop(snap);
    db.close().unwrap();
}

/// A cut paused between its passes, or inside its second pass, while the cross-shard commit
/// it straddles is released: it shows both shares or neither, with the coordinator on either
/// shard (the pass order is fixed, so the coordinator's line is loaded first or second).
#[test]
fn a_cross_shard_commit_released_during_a_cut_is_shown_whole_or_not_at_all() {
    for (pause_at, coordinator) in [(0u8, 0usize), (0, 1), (1, 0), (1, 1)] {
        let (db, gate, tables) = open(2500 + u64::from(pause_at) * 2 + coordinator as u64, 2);
        let (c, p) = (
            Arc::clone(&tables[coordinator]),
            Arc::clone(&tables[1 - coordinator]),
        );
        let row = format!("held{pause_at}{coordinator}");
        gate.hold(&wal(coordinator as u32));
        let held = db
            .submit(cross(&c, &p, &row), Some(Durability::Sync))
            .unwrap();
        gate.wait_held(&wal(coordinator as u32));
        db.commit(put(&p, "before"), Some(Durability::Buffered))
            .unwrap();
        // The cut pauses (after pass 1, or after shard 0's component in pass 2) until the
        // commit was released and acknowledged.
        let (paused_tx, paused_rx) = mpsc::channel::<()>();
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let hook = Box::new(move || {
            paused_tx.send(()).unwrap();
            go_rx.recv().unwrap();
        });
        if pause_at == 0 {
            db.after_snapshot_marks(hook);
        } else {
            db.mid_snapshot_own(hook);
        }
        let db2 = Arc::clone(&db);
        let snapper = std::thread::spawn(move || db2.snapshot().unwrap());
        paused_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the cut reached the hook");
        gate.release();
        held.wait().unwrap();
        go_tx.send(()).unwrap();
        let snap = snapper.join().unwrap();
        let on_c = has(&db, &snap, &c, &row);
        let on_p = has(&db, &snap, &p, &row);
        assert_eq!(
            on_c,
            on_p,
            "pause {pause_at}, coordinator shard {coordinator}: torn ({on_c}, {on_p}) at {:?}",
            snap.cut()
        );
        assert!(
            has(&db, &snap, &p, "before"),
            "an acknowledged single is in the cut"
        );
        drop(snap);
        let snap = db.snapshot().unwrap();
        assert!(has(&db, &snap, &c, &row) && has(&db, &snap, &p, &row));
        drop(snap);
        db.close().unwrap();
    }
}

/// With more cross-shard commits held by one coordinator than its slots, the oldest one
/// becomes a floor for every shard (the mark): a single elsewhere waits for it, as under
/// D19, and cuts count the fallback. Once the commits release, everything is visible.
#[test]
fn more_held_commits_than_slots_fall_back_to_the_floor() {
    const HELD: usize = 15;
    let (db, gate, tables) = open(2600, 3);
    let (participant, coordinator, bystander) = (
        Arc::clone(&tables[0]),
        Arc::clone(&tables[1]),
        Arc::clone(&tables[2]),
    );
    // Every PREPARE on the participant waits for its sync: the coordinator holds all of them.
    gate.hold(&wal(0));
    let mut held = Vec::new();
    for i in 0..HELD {
        held.push(
            db.submit(
                cross(&coordinator, &participant, &format!("h{i}")),
                Some(Durability::Sync),
            )
            .unwrap(),
        );
    }
    gate.wait_held(&wal(0));
    let before = db.overflow_cuts();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let excluded = loop {
        let snap = db.snapshot().unwrap();
        let excluded = snap.cut().excluded().len();
        drop(snap);
        if db.overflow_cuts() > before {
            break excluded;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no overflow after 5 s: {excluded} excluded"
        );
        std::thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(excluded, 13, "the slots are full");
    // A single on the third shard waits behind the mark.
    let (db2, t2) = (Arc::clone(&db), Arc::clone(&bystander));
    let waiter = std::thread::spawn(move || db2.commit(put(&t2, "single"), Some(Durability::None)));
    std::thread::sleep(Duration::from_millis(300));
    assert!(!waiter.is_finished(), "the single returned under the mark");
    gate.release();
    for h in held {
        h.wait().unwrap();
    }
    waiter.join().unwrap().unwrap();
    let snap = db.snapshot().unwrap();
    assert!(snap.cut().excluded().is_empty());
    assert!(has(&db, &snap, &bystander, "single"));
    for i in 0..HELD {
        assert!(has(&db, &snap, &coordinator, &format!("h{i}")));
        assert!(has(&db, &snap, &participant, &format!("h{i}")));
    }
    drop(snap);
    db.close().unwrap();
}
