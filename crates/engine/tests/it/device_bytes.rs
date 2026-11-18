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

#[test]
fn every_byte_of_every_file_is_counted() {
    // The coordinator's check on #2063: the device bytes must cover every file the database
    // works through, not only its main file. On the simulator, which tallies every byte its
    // files read and write, the database being its only user, the engine's counts equal
    // the simulator's exactly: a file opened past the counting wrapper (a WAL stream, the
    // pager, the feed sidecar or its reader) would show as a difference.
    use pigeonhole_engine::{FeedOptions, FeedSpec, FeedStart, Next};
    let vfs = pigeonhole_io::sim::SimVfs::new(0x332);
    let mut o = crate::common::options(std::sync::Arc::clone(&vfs), 4, 4 << 20);
    // Every read goes to the files.
    o.block_cache_bytes = 0;
    let mut feed = FeedOptions::default();
    feed.retention_bytes = 1 << 20;
    o.feed = Some(feed);
    let db = Engine::open(std::path::Path::new("/counted.phdb"), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    let mut reader = db
        .feed(FeedSpec {
            tables: vec![t.id],
            prefix: Vec::new(),
            durable: true,
            start: FeedStart::Now,
        })
        .unwrap();
    // Rows across every shard's tablets and WAL stream, past a segment's size (rollovers).
    for i in 0..1_500u32 {
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            f,
            format!("r{:08x}", i.wrapping_mul(0x9E37_79B9)).as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&[(i % 251) as u8; 600]),
        )
        .unwrap();
        db.commit(wb, Some(Durability::Sync)).unwrap();
    }
    // The feed's reader reads them back through the sidecar.
    let mut seen = 0;
    while seen < 100 {
        match reader.wait(Duration::from_secs(10)).unwrap() {
            Next::Commit(_) => seen += 1,
            other => panic!("the feed's commit: {other:?}"),
        }
    }
    db.flush().unwrap();
    db.compact(Some(t.id)).unwrap();
    let snap = db.snapshot().unwrap();
    for i in (0..1_500u32).step_by(7) {
        let row = format!("r{:08x}", i.wrapping_mul(0x9E37_79B9));
        assert!(
            db.get(&snap, t.id, f, row.as_bytes(), b"q")
                .unwrap()
                .is_some()
        );
    }
    drop(snap);
    drop(reader);
    db.close().unwrap();
    let counted = bytes(&db);
    let moved = vfs.bytes_moved();
    assert!(counted.0 > 0 && counted.1 > 0, "{counted:?}");
    assert_eq!(
        counted, moved,
        "the engine counted (read, written) {counted:?}, the files moved {moved:?}"
    );
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
