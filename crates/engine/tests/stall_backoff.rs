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
    // Short backoffs, so each test runs in well under a second: a failed compaction's slot
    // waits 20 ms (doubling), a failed flush 2 ms (doubling to 200 ms).
    o.compaction_backoff_nanos = BACKOFF.as_nanos() as u64;
    o.flush_backoff_nanos = 2_000_000;
    o
}

/// The compaction backoff's base in these tests.
const BACKOFF: Duration = Duration::from_millis(20);

/// How long a writer makes no progress before a test takes it as stalled.
const STALLED: Duration = Duration::from_millis(100);

fn table(db: &Engine, name: &str) -> Arc<TableInfo> {
    let f = FamilyOptions {
        compression: Compression::None,
        ..FamilyOptions::default()
    };
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
/// timer (20 ms, 40 ms, 80 ms, ... here), not after every admitted group or flush.
#[test]
fn steady_writes_do_not_cut_a_failing_compactions_backoff_short() {
    let (vfs, gate) = gate::vfs(1412);
    let db = Engine::open(Path::new(DB), options(vfs)).unwrap();
    let a = table(&db, "a");
    gate.fail_reads_containing(Some(MARK));
    let mut i = until_a_compaction_fails(&db, &gate, &a);
    let first = gate.read_failures()[0];
    // Writes for 12.5 backoff bases after the first failure: the backoff allows three
    // retries (after 1, 3 and 7 bases).
    while first.elapsed() < BACKOFF * 25 / 2 {
        write(&db, &a, i, true);
        i += 1;
    }
    let failures = gate.read_failures().len();
    assert!(
        failures <= 4,
        "{failures} failed compaction reads in {:?} of writes ({i} commits): the backoff \
         was cut short",
        BACKOFF * 25 / 2
    );
    assert!(failures >= 2, "the backoff timer never retried");
    // Each failed attempt is counted (nothing else reports a background failure).
    assert_eq!(db.metrics().compaction_failures, failures as u64);
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

/// 1-2 F4 / 5-6 5.2: flushes keep failing (a write error that does not poison the pager)
/// while a writer waits for arena room on a moving clock. They are retried on a backoff
/// (2 ms doubling to 200 ms here), not back to back until the stall timeout; once the device
/// recovers, the next retry frees room and the writer goes on.
#[test]
fn a_failing_flush_during_a_room_wait_backs_off() {
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    let (vfs, gate) = gate::vfs(1414);
    let mut o = options(vfs);
    o.memtable_budget = 256 << 10;
    o.write_stall_timeout_nanos = 30_000_000_000;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let a = table(&db, "a");
    gate.fail_writes_containing(Some(MARK));
    let (stop, done) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicU32::new(0)),
    );
    let writer = {
        let (db, a, stop, done) = (
            Arc::clone(&db),
            Arc::clone(&a),
            Arc::clone(&stop),
            Arc::clone(&done),
        );
        std::thread::spawn(move || {
            let mut i = 0;
            while !stop.load(Ordering::Acquire) {
                write(&db, &a, i, true);
                i += 1;
                done.store(i, Ordering::Release);
            }
        })
    };
    // Wait until the writer stalls: no commit for a while (no flush can free the arena).
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last = (u32::MAX, Instant::now());
    while last.1.elapsed() < STALLED {
        assert!(Instant::now() < deadline, "the writer never stalled");
        let n = done.load(Ordering::Acquire);
        if n != last.0 {
            last = (n, Instant::now());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!gate.write_failures().is_empty(), "no flush failed");
    let before = gate.write_failures().len();
    std::thread::sleep(Duration::from_millis(200));
    let retries = gate.write_failures().len() - before;
    // With a 2 ms base, at most about seven retries fit 200 ms (2, 4, ... 128 ms).
    assert!(
        retries <= 8,
        "{retries} failed flushes in 200 ms of a room wait: retried back to back"
    );
    assert!(
        db.metrics().flush_failures >= 1,
        "failed flushes are counted"
    );
    // The device recovers: the next retry (at most 200 ms away) frees room.
    gate.fail_writes_containing(None);
    let stalled_at = done.load(Ordering::Acquire);
    let deadline = Instant::now() + Duration::from_secs(5);
    while done.load(Ordering::Acquire) == stalled_at {
        assert!(Instant::now() < deadline, "the writer never resumed");
        std::thread::sleep(Duration::from_millis(10));
    }
    stop.store(true, Ordering::Release);
    writer.join().unwrap();
    db.close().unwrap();
}

/// 1-2 F3: a writer waits for arena room that snapshots pin; another thread drops the
/// snapshots. Nothing announces the freed room, so the wait used to last until the stall
/// timeout (30 s here); a re-check timer sees it within about 100 ms. (A starved freeze's
/// wait arms the same re-check timer, `RoomWait::arm_recheck`.)
#[test]
fn room_freed_by_a_snapshot_drop_on_another_thread_ends_the_wait() {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    let (vfs, _gate) = gate::vfs(1415);
    let mut o = options(vfs);
    o.memtable_budget = 256 << 10;
    o.write_stall_timeout_nanos = 30_000_000_000;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let a = table(&db, "a");
    let snaps = Arc::new(Mutex::new(Vec::new()));
    let (stop, done) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicU32::new(0)),
    );
    let writer = {
        let (db, a, snaps, stop, done) = (
            Arc::clone(&db),
            Arc::clone(&a),
            Arc::clone(&snaps),
            Arc::clone(&stop),
            Arc::clone(&done),
        );
        std::thread::spawn(move || {
            let mut i = 0;
            while !stop.load(Ordering::Acquire) {
                write(&db, &a, i, false);
                i += 1;
                done.store(i, Ordering::Release);
                // Every commit is followed by a snapshot, which pins its view's memtables.
                if let Ok(mut s) = snaps.lock()
                    && !stop.load(Ordering::Acquire)
                {
                    s.push(db.snapshot().unwrap());
                }
            }
        })
    };
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last = (u32::MAX, Instant::now());
    while last.1.elapsed() < STALLED {
        assert!(Instant::now() < deadline, "the writer never stalled");
        let n = done.load(Ordering::Acquire);
        if n != last.0 {
            last = (n, Instant::now());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    // This thread drops the snapshots; nothing else happens on the shard.
    let stalled_at = done.load(Ordering::Acquire);
    stop.store(true, Ordering::Release);
    let dropped = Instant::now();
    snaps.lock().unwrap().clear();
    while done.load(Ordering::Acquire) == stalled_at {
        assert!(
            dropped.elapsed() < Duration::from_secs(5),
            "the wait outlived the snapshots that pinned the arena"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    writer.join().unwrap();
    db.close().unwrap();
}

/// On a frozen clock the flush backoff's timer gives up instead of firing (within
/// microseconds when the shard is driven in a loop). A flush that keeps failing must then
/// wait for the next flush trigger: retrying it each time the timer gives up looped inside
/// `run_once` (whose slice never ends on a frozen clock), allocating as it went, until the
/// model suite's runner ran out of memory (#141 review). Run under a watchdog: that loop
/// never returns.
#[test]
fn a_failing_flush_is_not_retried_in_a_loop_on_a_frozen_clock() {
    let retried = gate::within(10, || {
        let (vfs, gate) = gate::frozen_vfs(1416);
        let (db, mut shards) =
            Engine::open_application_owned(Path::new(DB), options(Arc::clone(&vfs))).unwrap();
        let shard = &mut shards[0];
        let a = table(&db, "a");
        gate.fail_writes_containing(Some(MARK));
        let mut i = 0u32;
        while db.metrics().flush_failures == 0 {
            assert!(i < 2_000, "no flush failed");
            let mut wb = WriteBatch::new();
            let mut value = MARK.to_vec();
            value.extend_from_slice(&[i as u8; 1024]);
            wb.put(
                a.id,
                a.families[0].id,
                format!("row{i:05}").as_bytes(),
                b"q",
                None,
                ValueRef::Bytes(&value),
            )
            .unwrap();
            drop(db.submit(wb, Some(Durability::None)).unwrap());
            while shard.run_once(u64::MAX) {}
            i += 1;
        }
        let failed = db.metrics().flush_failures;
        // Nothing new to flush and nothing waiting: drive the idle shard hard.
        for _ in 0..5_000 {
            shard.run_once(u64::MAX);
        }
        let retried = db.metrics().flush_failures - failed;
        gate.fail_writes_containing(None);
        drop(shards);
        let _ = db.close();
        retried
    })
    .expect("a failing flush was retried in a loop on a frozen clock (run_once never returned)");
    assert!(
        retried <= 2,
        "{retried} flush retries while the shard idled on a frozen clock"
    );
}
