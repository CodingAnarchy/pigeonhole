//! ICR 0027: `Metrics::shard_idle` counts an engine-owned shard's idle parks and the wakes
//! that end them, and `Metrics::commit_parks` the blocking commit waits that parked.

#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::time::Duration;

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;

#[test]
fn commits_after_idle_pauses_count_parks_and_wakes() {
    let dir = std::env::temp_dir().join(format!("phdb-idle-counts-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(0));
    o.create_if_missing = true;
    o.shards = 1;
    o.durability = Durability::Buffered;
    // The client parks at once, so a commit that finds the shard asleep parks too.
    o.commit_spin_nanos = 0;
    let db = Engine::open(&dir.join("idle.phdb"), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    // Pauses far past the shard's 50 µs spin: the shard has parked before `before`, and
    // parks again after each commit, before `after`.
    let pause = || std::thread::sleep(Duration::from_millis(20));
    pause();
    let before = db.metrics();
    const COMMITS: u64 = 5;
    for i in 0..COMMITS {
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            f,
            format!("r{i}").as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(b"v"),
        )
        .unwrap();
        db.commit(wb, None).unwrap();
        pause();
    }
    let after = db.metrics();
    let parks = after.shard_idle.0 - before.shard_idle.0;
    let wakes = after.shard_idle.1 - before.shard_idle.1;
    // Each commit found the shard parked and woke it.
    assert!(
        parks >= COMMITS,
        "{parks} parks for {COMMITS} commits after idle pauses"
    );
    assert!(
        wakes >= COMMITS,
        "{wakes} wakes for {COMMITS} commits after idle pauses"
    );
    assert!(
        after.commit_parks > before.commit_parks,
        "no commit wait parked"
    );
    db.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}
