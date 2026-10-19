//! D256 and D257: the balancer's pool term and its commits in flight come from the real
//! counters, not only from the test overrides (ctr274's review of #737): on an engine-owned
//! store with one compaction thread, a flush runs on the pool and the busy fraction the
//! balancer publishes exceeds its turn-only fraction (with the pool counter read as zero
//! the two would be equal), the handled commits leave the shard thread spinning (counted
//! and reported, no term since D257), and one blocking client's commits read at most about
//! one commit in flight.

#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::time::{Duration, Instant};

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;

#[test]
fn the_spin_and_pool_terms_lift_the_published_busy_above_the_turns() {
    let dir = std::env::temp_dir().join(format!("phdb-spin-pool-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(0));
    o.create_if_missing = true;
    o.shards = 1;
    o.compaction_threads = 1;
    o.pin_threads = false;
    o.durability = Durability::Buffered;
    o.balance_interval_nanos = 10_000_000;
    let db = Engine::open(&dir.join("terms.phdb"), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    // Commits keep the thread spinning after each; a flush puts a pool slice on its account.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut i = 0u32;
    let (busy, turn) = loop {
        for _ in 0..200 {
            let mut wb = WriteBatch::new();
            wb.put(
                t.id,
                f,
                format!("r{i:06}").as_bytes(),
                b"q",
                None,
                ValueRef::Bytes(&[7; 256]),
            )
            .unwrap();
            db.commit(wb, None).unwrap();
            i += 1;
        }
        db.flush().unwrap();
        std::thread::sleep(Duration::from_millis(30));
        let stats = &db.shard_stats()[0];
        if let Some((busy, turn)) = db.shard_busy_terms(0)
            && stats.pool_busy_nanos > 0
            && stats.spin_time_nanos > 0
            && busy > turn
        {
            break (busy, turn);
        }
        assert!(
            Instant::now() < deadline,
            "after 10 s: terms {:?}, pool {} spin {} turns {}",
            db.shard_busy_terms(0),
            stats.pool_busy_nanos,
            stats.spin_time_nanos,
            stats.busy_nanos
        );
    };
    assert!(busy > turn, "busy {busy} turn {turn}");
    // D257: one blocking client is at most one commit in flight, and the counter moved.
    let stats = &db.shard_stats()[0];
    assert!(
        stats.commit_inflight_nanos > 0,
        "no commit in-flight time counted"
    );
    let inflight = db.shard_inflight(0).expect("published");
    assert!(
        inflight > 0.0 && inflight <= 1.05,
        "one blocking client read {inflight} commits in flight"
    );
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
