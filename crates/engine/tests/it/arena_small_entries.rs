//! #1403: a commit of many small entries, more than the shard's arena has chunks, with one
//! entry that spans chunks, is admitted. Small entries pack into a memtable's chunks, so
//! the chunks they can take before the large entry allocates are bounded by their bytes,
//! not their count. Charging each a whole chunk made the run check (#141) ask for a block
//! of two chunks to survive more allocations than the arena has chunks: refused room even
//! on an empty arena, the commit failed `Busy` at every stall timeout, for ever (a long
//! memory's postings in a bulk `remember` on the AX102).

use std::time::{Duration, Instant};

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;

#[test]
fn a_commit_with_more_small_entries_than_arena_chunks_and_one_large_one_commits() {
    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(0));
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.tablet_changes = false;
    // A 2 MiB arena (the budget's shared-memory region) of 1 KiB chunks: 2,048 chunks.
    o.memtable_budget = 1 << 20;
    o.arena_chunk_bytes = Some(1024);
    // Fail fast where the old charge refused room: `Busy` after this long.
    o.write_stall_timeout_nanos = 2_000_000_000;
    let dir = std::env::temp_dir().join(format!("phdb-arena-small-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = Engine::open(&dir.join("db.phdb"), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let (table, family) = (t.id, t.families[0].id);

    // 2,500 small entries, more than the arena's chunks (about 130 bytes each, about 650 KB
    // with their worst-case waste, well within its bytes), and one of 1,500 bytes, which
    // needs a run of two chunks.
    let mut wb = WriteBatch::new();
    for i in 0..2_500u32 {
        wb.put(
            table,
            family,
            format!("row{i:05}").as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&i.to_be_bytes()),
        )
        .unwrap();
    }
    wb.put(
        table,
        family,
        b"wide",
        b"q",
        None,
        ValueRef::Bytes(&[7u8; 1_500]),
    )
    .unwrap();

    let started = Instant::now();
    let r = db.commit(wb, Some(Durability::Buffered));
    assert!(
        r.is_ok(),
        "the commit fits an empty arena, so it is admitted: got {r:?} after {:?}",
        started.elapsed()
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "admitted at once, not after a room wait"
    );
    let snap = db.snapshot().unwrap();
    assert!(
        db.get(&snap, table, family, b"wide", b"q")
            .unwrap()
            .is_some()
            && db
                .get(&snap, table, family, b"row02499", b"q")
                .unwrap()
                .is_some()
    );
    drop(snap);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
