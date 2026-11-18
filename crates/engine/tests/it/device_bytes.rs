//! D332 (ICR 0142): `Metrics::device_read_bytes` and `device_write_bytes` count what the
//! database's files read and write. Writes rise under commits and a flush, reads under
//! reads the block cache can't serve, and neither moves while the database is idle.

use std::time::Duration;

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;

fn bytes(db: &Engine) -> (u64, u64) {
    let m = db.metrics();
    (m.device_read_bytes, m.device_write_bytes)
}

#[test]
fn device_bytes_rise_with_writes_and_uncached_reads_and_not_when_idle() {
    let dir = Fresh::new();
    let dir = &dir.0;
    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(0));
    o.create_if_missing = true;
    o.shards = 1;
    o.durability = Durability::Sync;
    // Every read goes to the file.
    o.block_cache_bytes = 0;
    let db = Engine::open(&dir.join("device.phdb"), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;

    let before = bytes(&db);
    for i in 0..200u32 {
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            f,
            format!("r{i:05}").as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&[7; 1000]),
        )
        .unwrap();
        db.commit(wb, None).unwrap();
    }
    db.flush().unwrap();
    let written = bytes(&db);
    // 200 KB of values went to the WAL (the SST holds them compressed: they're all 7s).
    assert!(
        written.1 - before.1 >= 200_000,
        "write bytes {before:?} -> {written:?}"
    );

    // Idle: nothing moves (allowing for a background pass already in flight).
    std::thread::sleep(Duration::from_millis(300));
    let idle0 = bytes(&db);
    std::thread::sleep(Duration::from_millis(500));
    let idle1 = bytes(&db);
    assert!(
        idle1.0 - idle0.0 < 64 << 10 && idle1.1 - idle0.1 < 64 << 10,
        "idle {idle0:?} -> {idle1:?}"
    );

    let snap = db.snapshot().unwrap();
    for i in 0..200u32 {
        assert!(
            db.get(&snap, t.id, f, format!("r{i:05}").as_bytes(), b"q")
                .unwrap()
                .is_some()
        );
    }
    let read = bytes(&db);
    assert!(
        read.0 - idle1.0 >= 200_000,
        "uncached reads counted {idle1:?} -> {read:?}"
    );
    drop(snap);
    db.close().unwrap();
}

/// A directory of the test's own, never an earlier run's (pid and clock in its name),
/// removed on drop, a failure's included.
struct Fresh(std::path::PathBuf);

impl Fresh {
    fn new() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let d =
            std::env::temp_dir().join(format!("phdb-device-bytes-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        Self(d)
    }
}

impl Drop for Fresh {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
