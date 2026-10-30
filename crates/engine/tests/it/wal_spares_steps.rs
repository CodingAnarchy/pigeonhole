//! D35: a shard prepares its WAL spare slots in short steps. The zero-fill writes and the
//! sync go to the I/O backend, and the task waits for them blocked rather than writing on
//! the shard thread. One 64 MiB slot zero-filled and synced in one step held a shard for
//! about 30 ms on the reference machine. Deterministic: one shard driven by hand, the
//! simulator's I/O deferred until the test completes it.

use std::path::Path;

use pigeonhole_engine::{Engine, EngineOptions, EngineShard, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::{SimOp, SimVfs};

const DB: &str = "/db/data.phdb";
/// Four zero-fill writes per slot (the WAL writes 1 MiB per call).
const SEGMENT: u64 = 4 << 20;
const ZERO_CHUNK: u64 = 1 << 20;
/// The preparation's writes in flight at most (four), with the commit's WAL write.
const MAX_IN_FLIGHT: usize = 5;

/// Runs the shard until it has nothing left to do.
fn idle(shard: &mut EngineShard) {
    while shard.run_once(u64::MAX) {}
}

/// Zero-fill writes of a spare slot the simulated device ran since recording started: whole
/// 1 MiB writes at a slot-aligned offset past the first segment.
fn zero_fills(vfs: &SimVfs) -> usize {
    vfs.recorded_ops()
        .iter()
        .filter(|op| {
            matches!(op, SimOp::Write { offset, len, .. }
                if *len == ZERO_CHUNK && *offset >= SEGMENT && offset % ZERO_CHUNK == 0)
        })
        .count()
}

#[test]
fn spare_slots_are_zero_filled_by_submitted_writes() {
    let vfs = SimVfs::new(35);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.wal.segment_size = SEGMENT;
    o.wal.spare_segments = 1;
    let (engine, mut shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
    let shard = &mut shards[0];
    let t = engine
        .create_table("t", &[("f".to_owned(), FamilyOptions::default())])
        .unwrap();
    idle(shard);
    vfs.set_deferred_io(true);
    vfs.record_ops();

    // Fill the first segment past half way, so the shard starts preparing a spare, and
    // then some. A zero-fill write reaches the device only when the test completes I/O,
    // never during a shard turn (the shard thread does not write), and a turn leaves at most
    // a step's worth in flight.
    let value = vec![7u8; 64 << 10];
    let mut max_in_flight = 0;
    for i in 0..48u32 {
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            t.families[0].id,
            &i.to_be_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&value),
        )
        .unwrap();
        drop(engine.submit(wb, Some(Durability::Buffered)).unwrap());
        let before = zero_fills(&vfs);
        idle(shard);
        assert_eq!(
            zero_fills(&vfs),
            before,
            "commit {i}: a zero-fill ran on the shard thread"
        );
        max_in_flight = max_in_flight.max(vfs.io_in_flight());
        vfs.complete_all_io();
    }
    // Then the preparation finishes: the slot's four writes, and its sync.
    for _ in 0..16 {
        idle(shard);
        max_in_flight = max_in_flight.max(vfs.io_in_flight());
        vfs.complete_all_io();
    }
    assert_eq!(zero_fills(&vfs), 4, "the spare slot was zero-filled");
    assert!(
        vfs.recorded_ops()
            .iter()
            .any(|op| matches!(op, SimOp::Sync { metadata: true, .. })),
        "the prepared slot was synced"
    );
    assert!(
        max_in_flight <= MAX_IN_FLIGHT,
        "{max_in_flight} operations in flight"
    );
}
