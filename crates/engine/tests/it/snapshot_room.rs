//! #1212: held snapshots pin whole generations of memtables. Each snapshot's view holds
//! every slot's memtables as of when it was taken, and a memtable holds at least one arena
//! chunk, so snapshots held across flushes pin `snapshots × slots` chunks however little
//! the memtables hold. With 256 chunks per arena, 8 such snapshots on a shard of 32 slots
//! took the whole arena, and writes stalled into `Busy` until one dropped.

use crate::common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/snapshot_room.phdb";

fn options(vfs: &Arc<SimVfs>) -> EngineOptions {
    let mut o = common::options(Arc::clone(vfs), 1, 1 << 20);
    o.tablet_changes = false;
    // A stall that would last for good fails in a second of simulated time, not 30.
    o.write_stall_timeout_nanos = 1_000_000_000;
    o
}

#[test]
fn snapshots_held_across_flushes_leave_room_for_writes() {
    let vfs = SimVfs::new(1212);
    let db = Engine::open(Path::new(DB), options(&vfs)).unwrap();
    // 64 slots on the one shard: two tables of 32 families, created after the open (so the
    // open sized its chunks for none of them, as an application's later tables are).
    let families: Vec<(String, FamilyOptions)> = (0..32)
        .map(|i| (format!("f{i}"), FamilyOptions::default()))
        .collect();
    let tables: Vec<_> = ["a", "b"]
        .iter()
        .map(|name| db.create_table(name, &families).unwrap())
        .collect();
    let write_all = |round: u32| {
        let mut wb = WriteBatch::new();
        for t in &tables {
            for f in &t.families {
                let v = round.to_le_bytes();
                wb.put(t.id, f.id, b"row", b"q", None, ValueRef::Bytes(&v))
                    .unwrap();
            }
        }
        db.commit(wb, None)
    };
    // Eight snapshots, each over a different generation of every slot's memtables.
    let mut held = Vec::new();
    for round in 0..8 {
        write_all(round).unwrap_or_else(|e| panic!("round {round}: {e}"));
        held.push(db.snapshot().unwrap());
        db.flush().unwrap_or_else(|e| panic!("flush {round}: {e}"));
    }
    // The next writes need a fresh chunk for every slot: they must fit beside the pinned
    // generations, not wait for a snapshot to drop.
    for round in 8..12 {
        write_all(round).unwrap_or_else(|e| panic!("round {round} beside 8 snapshots: {e}"));
        db.flush().unwrap();
    }
    // Every snapshot still reads its own generation.
    for (round, snap) in held.iter().enumerate() {
        let cell = db
            .get(snap, tables[1].id, tables[1].families[31].id, b"row", b"q")
            .unwrap()
            .expect("the cell");
        assert_eq!(cell.value(), ValueRef::Bytes(&(round as u32).to_le_bytes()));
    }
    drop(held);
    db.close().unwrap();
}
