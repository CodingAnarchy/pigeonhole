//! ICR 0033 (D217): `ShardStats::busy_nanos` and `Metrics::shard_busy_nanos` count the time an
//! engine-owned shard thread spends in work turns, not spinning or parked.

#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::time::{Duration, Instant};

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;

fn busy(db: &Engine) -> Vec<u64> {
    db.shard_stats().iter().map(|s| s.busy_nanos).collect()
}

#[test]
fn work_counts_busy_time_and_idling_does_not() {
    let dir = std::env::temp_dir().join(format!("phdb-busy-nanos-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(0));
    o.create_if_missing = true;
    o.shards = 2;
    o.durability = Durability::Buffered;
    let db = Engine::open(&dir.join("busy.phdb"), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;

    // Work: commits, all to the table's one tablet.
    let before = busy(&db);
    let start = Instant::now();
    for i in 0..2000u32 {
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            f,
            format!("r{i:05}").as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&[7; 100]),
        )
        .unwrap();
        db.commit(wb, None).unwrap();
    }
    let elapsed = start.elapsed().as_nanos() as u64;
    let worked = busy(&db);
    let gained: Vec<u64> = worked.iter().zip(&before).map(|(a, b)| a - b).collect();
    assert!(
        gained.iter().any(|&g| g > 0),
        "2000 commits counted no busy time: {gained:?}"
    );
    for g in &gained {
        // A shard is busy for at most the time that passed (and a little slack for the
        // turn that was running when the readings were taken).
        assert!(*g <= elapsed + 50_000_000, "busy {g} ns over {elapsed} ns");
    }
    let total = db.metrics().shard_busy_nanos;
    assert!(
        total >= worked.iter().sum::<u64>(),
        "the metric sums the shards"
    );

    // Idle: spinning and parking count nothing; only balancer passes run.
    std::thread::sleep(Duration::from_millis(50));
    let idle_from = busy(&db);
    let pause = Duration::from_millis(500);
    std::thread::sleep(pause);
    let idle_to = busy(&db);
    for (a, b) in idle_to.iter().zip(&idle_from) {
        assert!(
            a - b < pause.as_nanos() as u64 / 5,
            "an idle shard counted {} ns busy in {pause:?}",
            a - b
        );
    }
    db.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}
