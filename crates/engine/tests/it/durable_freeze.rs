//! D227, ICR 0037: a memtable freezes only once everything in it is durable, so a flush
//! never collapses a version a durable snapshot may still read. With another shard's sync
//! held, a flush of a memtable holding versions newer than the durable seqno waits: frozen
//! at the visible seqno instead, its GC would keep only the newest version, and a durable
//! snapshot taken meanwhile would find the older one gone.

use crate::gate;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, TableInfo, ValueRef, WriteBatch};
use pigeonhole_format::Durability;

const DB: &str = "/db/freeze.phdb";

fn wal(stream: u32) -> PathBuf {
    pigeonhole_wal::stream_path(Path::new(DB), pigeonhole_format::StreamId(stream))
}

fn put(t: &TableInfo, row: &str, v: &[u8]) -> WriteBatch {
    put_at(t, row, None, v)
}

/// At an explicit timestamp: versions at one timestamp are what flush GC collapses to the
/// newest at each read point.
fn put_at(t: &TableInfo, row: &str, ts: Option<u64>, v: &[u8]) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row.as_bytes(),
        b"q",
        ts,
        ValueRef::Bytes(v),
    )
    .unwrap();
    wb
}

fn read(db: &Engine, snap: &pigeonhole_engine::Snapshot, t: &TableInfo) -> Option<Vec<u8>> {
    db.get(snap, t.id, t.families[0].id, b"k", b"q")
        .unwrap()
        .map(|c| match c.value() {
            ValueRef::Bytes(b) => b.to_vec(),
            other => panic!("{other:?}"),
        })
}

#[test]
fn a_flush_waits_for_the_durable_seqno_and_keeps_what_durable_snapshots_read() {
    let (vfs, gate) = gate::vfs(2272);
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.tablet_changes = false;
    let db = Arc::new(Engine::open(Path::new(DB), o).unwrap());
    let mut tables = Vec::new();
    for name in ["a", "b"] {
        let t = db
            .create_table(name, &[("f".into(), FamilyOptions::default())])
            .unwrap();
        db.record_history(true);
        db.take_appended();
        db.commit(put(&t, "probe", b"p"), Some(Durability::Buffered))
            .unwrap();
        let shard = u32::from(db.take_appended().last().expect("one record").stream);
        tables.push((t, shard));
    }
    let [(a, _), (b, sb)] = <[_; 2]>::try_from(tables).unwrap();

    const TS: Option<u64> = Some(1_000);
    for v in [b"v1", b"v2"] {
        db.commit(put_at(&a, "k", TS, v), Some(Durability::Buffered))
            .unwrap();
    }
    // Shard B's sync is held: the durable seqno stays below its commit, and below v3.
    gate.hold(&wal(sb));
    let held = db
        .submit(put(&b, "x", b"held"), Some(Durability::GroupSync))
        .unwrap();
    gate.wait_held(&wal(sb));
    db.commit(put_at(&a, "k", TS, b"v3"), Some(Durability::Buffered))
        .unwrap();

    // The flush waits on shard B's own held sync either way; shard A's part is what counts.
    // Frozen at the visible seqno, shard A's flush would write only v3 (one timestamp, no
    // snapshot between) and a durable snapshot below v3 would find nothing.
    let flusher = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || db.flush())
    };
    std::thread::sleep(Duration::from_millis(300));
    let durable = db.durable_snapshot().unwrap();
    assert_eq!(
        read(&db, &durable, &a).as_deref(),
        Some(&b"v2"[..]),
        "a flush collapsed a version a durable snapshot reads"
    );
    drop(durable);

    gate.release();
    gate::within(10, move || flusher.join())
        .expect("the flush hung")
        .unwrap()
        .unwrap();
    held.wait().unwrap();
    let durable = db.durable_snapshot().unwrap();
    assert_eq!(read(&db, &durable, &a).as_deref(), Some(&b"v3"[..]));
    drop(durable);
    db.close().unwrap();
}
