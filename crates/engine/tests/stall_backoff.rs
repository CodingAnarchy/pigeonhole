//! Issue #141: stall, retry and backoff edge cases from the #90 review, on a moving clock.
//! Storage is `SimVfs` behind the test gate (`tests/gate`), on a real clock; one table's
//! SST blocks are made unreadable by a marker in its values (no compression), so its
//! compactions fail while every other table's succeed.

mod gate;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pigeonhole_engine::{
    Engine, EngineOptions, FamilyOptions, PickerOptions, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_format::compress::Compression;

const DB: &str = "/db/stall.phdb";

/// In every value of a table whose compactions must fail.
const MARK: &[u8] = b"#141-unreadable-block-marker#";

fn options(vfs: pigeonhole_io::VfsRef) -> EngineOptions {
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.memtable_budget = 1 << 20;
    o.memtable_freeze_bytes = 8 << 10;
    o.block_cache_bytes = 0;
    o.wal.segment_size = 256 << 10;
    o.wal.spare_segments = 1;
    let mut c = PickerOptions::default();
    c.l0_trigger = 2;
    c.level_base_bytes = 48 << 10;
    c.level_multiplier = 2;
    c.max_levels = 4;
    c.target_sst_bytes = 64 << 10;
    o.compaction = c;
    o
}

fn table(db: &Engine, name: &str) -> Arc<TableInfo> {
    let mut f = FamilyOptions::default();
    f.compression = Compression::None;
    db.create_table(name, &[("f".into(), f)]).unwrap()
}

/// Writes row `i` of `t` (rows repeat, so compactions rewrite them); `marked` values carry
/// `MARK`.
fn write(db: &Engine, t: &TableInfo, i: u32, marked: bool) {
    let mut value = if marked { MARK.to_vec() } else { Vec::new() };
    let mut x = u64::from(i).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    value.extend((0..1024).map(|_| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x as u8
    }));
    let mut wb = WriteBatch::new();
    let row = format!("row{:05}", i % 97);
    wb.put(
        t.id,
        t.families[0].id,
        row.as_bytes(),
        b"q",
        None,
        ValueRef::Bytes(&value),
    )
    .unwrap();
    db.commit(wb, Some(Durability::None)).unwrap();
}

/// Writes marked rows to `t` until a background compaction of it has failed; returns the
/// next row number.
fn until_a_compaction_fails(db: &Engine, gate: &gate::Gate, t: &TableInfo) -> u32 {
    let mut i = 0;
    while gate.read_failures().is_empty() {
        assert!(i < 20_000, "no compaction failed: {:?}", db.metrics());
        write(db, t, i, true);
        i += 1;
    }
    i
}

/// 5-6 5.3: a background compaction fails once; the device recovers; a later `compact()`
/// succeeds. It used to return the old background failure.
#[test]
fn compact_does_not_report_an_earlier_background_failure() {
    let (vfs, gate) = gate::vfs(1411);
    let db = Engine::open(Path::new(DB), options(vfs)).unwrap();
    let a = table(&db, "a");
    gate.fail_reads_containing(Some(MARK));
    until_a_compaction_fails(&db, &gate, &a);
    gate.fail_reads_containing(None);
    db.compact(None)
        .expect("compact() after the device recovered reported a stale background failure");
    db.close().unwrap();
}

/// 1-2 F5: under steady writes, a compaction that keeps failing is retried on its backoff
/// timer (1 s, then 2 s), not after every admitted group or flush.
#[test]
fn steady_writes_do_not_cut_a_failing_compactions_backoff_short() {
    let (vfs, gate) = gate::vfs(1412);
    let db = Engine::open(Path::new(DB), options(vfs)).unwrap();
    let a = table(&db, "a");
    gate.fail_reads_containing(Some(MARK));
    let mut i = until_a_compaction_fails(&db, &gate, &a);
    let first = gate.read_failures()[0];
    // 2.5 s of writes after the first failure: the backoff allows one retry (at 1 s).
    while first.elapsed() < Duration::from_millis(2_500) {
        write(&db, &a, i, true);
        i += 1;
    }
    let failures = gate.read_failures().len();
    assert!(
        failures <= 4,
        "{failures} failed compaction reads in 2.5 s of writes ({i} commits): the backoff \
         was cut short"
    );
    assert!(failures >= 2, "the backoff timer never retried");
    gate.fail_reads_containing(None);
    db.close().unwrap();
}

/// 5-6 5.7: one slot whose compactions always fail does not stop compaction for the rest of
/// the shard. It used to be picked again (it stays the most urgent) after every backoff.
#[test]
fn a_slot_that_keeps_failing_does_not_stop_the_others_compacting() {
    let (vfs, gate) = gate::vfs(1413);
    let db = Engine::open(Path::new(DB), options(vfs)).unwrap();
    let a = table(&db, "a");
    let b = table(&db, "b");
    gate.fail_reads_containing(Some(MARK));
    // Twice as many writes to `a`: it is always the most urgent slot.
    let mut i = 0u32;
    while gate.read_failures().is_empty() {
        assert!(
            i < 20_000,
            "no compaction of `a` failed: {:?}",
            db.metrics()
        );
        write(&db, &a, i, true);
        write(&db, &a, i + 1, true);
        write(&db, &b, i, false);
        i += 2;
    }
    let compacted = db.metrics().compactions;
    let deadline = Instant::now() + Duration::from_secs(10);
    while db.metrics().compactions == compacted {
        assert!(
            Instant::now() < deadline,
            "no other slot compacted in 10 s while `a` kept failing ({} failures)",
            gate.read_failures().len()
        );
        write(&db, &a, i, true);
        write(&db, &a, i + 1, true);
        write(&db, &b, i, false);
        i += 2;
    }
    gate.fail_reads_containing(None);
    db.close().unwrap();
}
