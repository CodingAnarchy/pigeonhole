//! Issue #232: an idle FIFO-by-time family drops an SST when it expires, with no flush or
//! other write to start the shard's maintenance. Storage is `SimVfs` behind the test gate
//! (`tests/gate`), on a real clock, with engine-owned shard threads.

mod gate;

use std::path::Path;
use std::time::{Duration, Instant};

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_format::manifest::CompactionStyle;

const DB: &str = "/db/fifo.phdb";
const TTL: Duration = Duration::from_millis(300);

#[test]
fn an_idle_fifo_family_drops_ssts_when_they_expire() {
    let (vfs, _gate) = gate::vfs(232);
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.memtable_budget = 1 << 20;
    o.wal.segment_size = 256 << 10;
    o.wal.spare_segments = 1;
    o.tablet_changes = false;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let family = FamilyOptions::default()
        .compaction(CompactionStyle::FifoByTime)
        .ttl_micros(TTL.as_micros() as u64);
    let t = db.create_table("fifo", &[("f".into(), family)]).unwrap();
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        b"row",
        b"q",
        None,
        ValueRef::Bytes(b"v"),
    )
    .unwrap();
    db.commit(wb, Some(Durability::Buffered)).unwrap();
    let written = Instant::now();
    db.flush().unwrap();
    let ssts = || db.sst_levels().into_iter().filter(|s| s.0 == t.id).count();
    assert_eq!(ssts(), 1);
    // Nothing else happens: the SST goes once it expires, not before and not much after.
    let gone = loop {
        if ssts() == 0 {
            break written.elapsed();
        }
        assert!(
            written.elapsed() < TTL * 10,
            "the expired SST was never dropped"
        );
        std::thread::sleep(Duration::from_millis(5));
    };
    assert!(gone >= TTL, "dropped after {gone:?}, before the TTL");
    db.close().unwrap();
}
