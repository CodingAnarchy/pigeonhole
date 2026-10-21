//! Automatic recovery (`Options::auto_recover`, D265, ICR 0050, #620): a failed instance is
//! closed keeping its writer lock, reopened on it and swapped in behind every handle. On the
//! gate VFS (`tests/gate`), which fails, holds or panics one file's syncs.

mod gate;

use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use pigeonhole::{
    ErrorCode, FailureCause, Family, Options, Pigeonhole, ReaderOptions, Recovery, Table,
};
use pigeonhole_format::StreamId;
use pigeonhole_io::VfsRef;

const DB: &str = "/db/r.phdb";

fn options(vfs: VfsRef, recovery: Recovery) -> Options {
    Options::default()
        .shards(2)
        .pin_threads(false)
        .compaction_cores(1)
        .memtable_budget(4 << 20)
        .wal_segment_size(512 << 10)
        .write_stall_timeout(Duration::from_secs(10))
        .vfs(vfs)
        .auto_recover(recovery)
}

fn open(vfs: &VfsRef, recovery: Recovery) -> (Pigeonhole, Table) {
    let db = Pigeonhole::open(DB, options(Arc::clone(vfs), recovery)).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    (db, t)
}

fn wal(t: &Table, row: &[u8]) -> std::path::PathBuf {
    let shard = t.shard_of(row).expect("a writer's table");
    pigeonhole_wal::stream_path(Path::new(DB), StreamId(shard as u32))
}

fn put(t: &Table, row: &str) -> pigeonhole::Result<()> {
    t.mutate(row.as_bytes())
        .put("f", b"q", b"v")
        .commit()
        .map(|_| ())
}

/// A put with a value of `len` bytes (to fill WAL segments).
fn put_sized(t: &Table, row: &str, len: usize) -> pigeonhole::Result<()> {
    t.mutate(row.as_bytes())
        .put("f", b"q", &vec![7u8; len])
        .commit()
        .map(|_| ())
}

/// Holds the submitted writes of the stream `row`'s shard logs to (a new table is one
/// tablet on one shard, so every row lands there), then writes past half a segment on it
/// (about 330 KiB of 512 KiB, well short of a whole one: a rollover would wait for the held
/// spare and hang the fill) so its spare preparation (D35) submits a zero-fill, which stays
/// held: the shard's next close waits for it (#824), so a recovery is held at the failed
/// engine's close until `Gate::release_writes`.
fn hold_recovery(gate: &gate::Gate, t: &Table, row: &str) {
    let path = wal(t, row.as_bytes());
    gate.hold_writes(&path);
    for i in 0..300 {
        put_sized(t, &format!("fill{i:03}"), 1 << 10).unwrap();
    }
    gate.wait_held_write(&path);
}

fn has(t: &Table, row: &str) -> bool {
    t.get(row.as_bytes(), "f", b"q").unwrap().is_some()
}

/// Polls `pred` up to `secs` seconds.
fn wait_until(secs: u64, pred: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !pred() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    true
}

/// Fails the stream of `row`'s shard: the next synced commit to it fails and the shard dies
/// with `Io`. Heals the stream right after, so the reopen's replay syncs succeed.
fn fail_shard(gate: &gate::Gate, t: &Table, row: &str) -> pigeonhole::Error {
    let path = wal(t, row.as_bytes());
    gate.fail(&path);
    let e = put(t, row).expect_err("a commit with a failing sync");
    gate.heal(&path);
    e
}

/// An I/O failure is recovered in place: the handle works again on a new engine
/// (generation 2), what was taken from the old one expires with `SnapshotExpired`, the data
/// survives, and the stats say so.
#[test]
fn an_io_failure_is_recovered_in_place() {
    let (vfs, gate) = gate::vfs(2001);
    let (db, t) = open(&vfs, Recovery::Io);
    for i in 0..32 {
        put(&t, &format!("row{i:02}")).unwrap();
    }
    let snap = db.snapshot().unwrap();
    let mut txn = db.transaction().unwrap();
    assert!(
        txn.get(&t, b"row00", "f", b"q").unwrap().is_some(),
        "the transaction reads before the failure"
    );
    let mut scan = t.scan_prefix(b"row").iter().unwrap();
    assert!(
        scan.next().unwrap().is_ok(),
        "the scan steps before the failure"
    );
    assert_eq!(db.recovery_stats().generation, 1);

    let e = fail_shard(&gate, &t, "lost");
    assert!(
        matches!(
            e.code(),
            ErrorCode::OutcomeUnknown | ErrorCode::Closed | ErrorCode::Io
        ),
        "{e:?}"
    );
    assert!(
        wait_until(20, || !db.is_failed()
            && db.recovery_stats().recoveries == 1),
        "the supervisor did not recover: {:?}, failed={}",
        db.recovery_stats(),
        db.is_failed()
    );
    let stats = db.recovery_stats();
    assert_eq!(
        (stats.generation, stats.recoveries, stats.gave_up),
        (2, 1, false)
    );
    assert_eq!(stats.last_failure.map(|f| f.cause), Some(FailureCause::Io));
    assert!(
        db.failure().is_none(),
        "the recovered instance has no failure"
    );

    // The same handles work on the new engine.
    for i in 0..32 {
        assert!(
            has(&t, &format!("row{i:02}")),
            "row{i:02} after the recovery"
        );
    }
    put(&t, "after").unwrap();
    assert!(has(&t, "after"));
    assert!(matches!(put(&t, "lost").map(|_| ()), Ok(())));
    // What was taken from the old engine expires.
    assert_eq!(
        t.get_at(&snap, b"row00", "f", b"q").unwrap_err().code(),
        ErrorCode::SnapshotExpired
    );
    assert_eq!(
        txn.get(&t, b"row01", "f", b"q").unwrap_err().code(),
        ErrorCode::SnapshotExpired
    );
    assert_eq!(
        scan.next().unwrap().unwrap_err().code(),
        ErrorCode::SnapshotExpired
    );
    let snap = db.snapshot().unwrap();
    assert!(t.get_at(&snap, b"after", "f", b"q").unwrap().is_some());
    drop((snap, scan, txn));
    db.close().unwrap();
    let (vfs2, _gate) = (vfs, gate);
    let (db, t) = open(&vfs2, Recovery::Off);
    assert!(
        has(&t, "row31") && has(&t, "after"),
        "after a clean close and reopen"
    );
    db.close().unwrap();
}

/// With the option off the instance stays failed, as it did before D265.
#[test]
fn recovery_off_leaves_the_instance_failed() {
    let (vfs, gate) = gate::vfs(2002);
    let (db, t) = open(&vfs, Recovery::Off);
    put(&t, "a").unwrap();
    let _ = fail_shard(&gate, &t, "lost");
    assert!(db.is_failed());
    std::thread::sleep(Duration::from_millis(300));
    assert!(db.is_failed(), "nothing recovers it");
    assert_eq!(db.recovery_stats().recoveries, 0);
    assert_eq!(put(&t, "b").unwrap_err().code(), ErrorCode::Closed);
    assert!(has(&t, "a"), "reads keep working on a failed instance");
    assert!(db.close().is_err(), "unclean");
}

/// `Recovery::Io` leaves a panicked shard failed; `Recovery::Any` recovers it.
#[test]
fn a_panic_is_recovered_only_with_any() {
    for (mode, recovers) in [(Recovery::Io, false), (Recovery::Any, true)] {
        let (vfs, gate) = gate::vfs(2003);
        let (db, t) = open(&vfs, mode);
        put(&t, "a").unwrap();
        let path = wal(&t, b"boom");
        gate.panic(&path);
        let _ = put(&t, "boom");
        assert!(wait_until(5, || db.failure().is_some()));
        assert_eq!(db.failure().map(|f| f.cause), Some(FailureCause::Panic));
        if recovers {
            assert!(
                wait_until(20, || !db.is_failed()),
                "{mode:?}: not recovered: {:?}",
                db.recovery_stats()
            );
            assert_eq!(db.recovery_stats().recoveries, 1);
            put(&t, "b").unwrap();
            db.close().unwrap();
        } else {
            std::thread::sleep(Duration::from_millis(300));
            assert!(db.is_failed(), "{mode:?}: stays failed");
            assert_eq!(db.recovery_stats().recoveries, 0);
            assert!(!db.recovery_stats().gave_up, "not covered is not giving up");
            assert!(db.close().is_err());
        }
    }
}

/// A reopen that keeps failing (the disk still fails) ends with the supervisor giving up
/// after its tries: the instance stays failed, `recovery_stats` says so, commits get
/// `Closed`.
#[test]
fn a_reopen_that_keeps_failing_gives_up() {
    let (vfs, gate) = gate::vfs(2004);
    let (db, t) = open(&vfs, Recovery::Io);
    put(&t, "a").unwrap();
    let path = wal(&t, b"lost");
    gate.fail(&path);
    let _ = put(&t, "lost");
    // Still failing: every reopen's replay sync fails with `Io`, retried with the backoff
    // (0, 1, 4 s) until the budget is spent.
    assert!(
        wait_until(30, || db.recovery_stats().gave_up),
        "{:?}",
        db.recovery_stats()
    );
    assert!(db.is_failed());
    assert_eq!(db.failure().map(|f| f.cause), Some(FailureCause::Io));
    assert_eq!(db.recovery_stats().recoveries, 0);
    assert_eq!(put(&t, "b").unwrap_err().code(), ErrorCode::Closed);
    gate.heal(&path);
    // The failed engine was closed by the recovery already (unclean); this close finds it
    // closed and returns Ok, as a second close does.
    let _ = db.close();
    // The locks were released with the give-up: a plain open works.
    let (db, t) = open(&vfs, Recovery::Off);
    assert!(has(&t, "a"));
    db.close().unwrap();
}

/// The budget: three recoveries in ten minutes; the fourth failure is not recovered.
#[test]
fn the_fourth_failure_in_the_window_is_not_recovered() {
    let (vfs, gate) = gate::vfs(2005);
    let (db, t) = open(&vfs, Recovery::Io);
    for n in 1..=3 {
        let _ = fail_shard(&gate, &t, &format!("lost{n}"));
        assert!(
            wait_until(30, || !db.is_failed()
                && db.recovery_stats().recoveries == n),
            "recovery {n}: {:?}",
            db.recovery_stats()
        );
        put(&t, &format!("after{n}")).unwrap();
    }
    let _ = fail_shard(&gate, &t, "lost4");
    assert!(
        wait_until(5, || db.recovery_stats().gave_up),
        "{:?}",
        db.recovery_stats()
    );
    assert!(db.is_failed());
    assert_eq!(db.recovery_stats().recoveries, 3);
    assert_eq!(put(&t, "after4").unwrap_err().code(), ErrorCode::Closed);
    assert!(db.close().is_err());
}

/// A commit that arrives while the recovery runs waits for the swap, bounded by the
/// write-stall timeout: `Busy` past it (not committed), and once the recovery completes it
/// succeeds; a read without a snapshot is served by the failed engine's last view, or waits
/// the same way. The recovery is held at the failed engine's close, which waits for a held
/// zero-fill write of its streams (#824).
#[test]
fn operations_wait_for_the_swap_bounded_by_the_stall_timeout() {
    let (vfs, gate) = gate::vfs(2006);
    let db = Pigeonhole::open(
        DB,
        options(Arc::clone(&vfs), Recovery::Io).write_stall_timeout(Duration::from_millis(300)),
    )
    .unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    put(&t, "a").unwrap();
    hold_recovery(&gate, &t, "lost");
    let _ = fail_shard(&gate, &t, "lost");
    assert!(db.is_failed(), "the old engine is still the current one");
    // A commit and a read without a snapshot wait, then time out with `Busy`.
    let started = Instant::now();
    let e = put(&t, "during").unwrap_err();
    assert_eq!(e.code(), ErrorCode::Busy, "{e:?}");
    assert!(
        started.elapsed() >= Duration::from_millis(250),
        "{:?}",
        started.elapsed()
    );
    // A read without a snapshot is answered from the failed engine's last view while that
    // still stands (the swap, which routes stale handles to the new engine, comes before
    // the old view is released), or waits for the swap like the commit once it cannot be.
    let got = t.get(b"a", "f", b"q");
    assert!(
        matches!(&got, Ok(Some(_))) || matches!(&got, Err(e) if e.code() == ErrorCode::Busy),
        "{got:?} stats={:?}",
        db.recovery_stats()
    );
    // Not given up: the recovery is still running.
    assert!(!db.recovery_stats().gave_up);
    gate.release_writes();
    assert!(
        wait_until(20, || !db.is_failed()
            && db.recovery_stats().recoveries == 1),
        "{:?}",
        db.recovery_stats()
    );
    put(&t, "during").unwrap();
    assert!(has(&t, "a") && has(&t, "during"));
    db.close().unwrap();
}

/// `close()` during a recovery waits for it to settle, then closes what the handle ends on.
#[test]
fn close_during_a_recovery_settles_it_first() {
    let (vfs, gate) = gate::vfs(2007);
    let (db, t) = open(&vfs, Recovery::Io);
    put(&t, "a").unwrap();
    hold_recovery(&gate, &t, "lost");
    let _ = fail_shard(&gate, &t, "lost");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(db.close());
    });
    assert!(
        rx.recv_timeout(Duration::from_millis(500)).is_err(),
        "the close returned while the recovery was held"
    );
    gate.release_writes();
    let closed = rx
        .recv_timeout(Duration::from_secs(20))
        .expect("the close did not return once the recovery settled");
    let _ = closed;
    drop(t);
    let (db, t) = open(&vfs, Recovery::Off);
    assert!(has(&t, "a"));
    db.close().unwrap();
}

/// The writer lock is held across the swap: a second writer in the same process fails
/// `WriterLocked` during a held recovery, and a reader opened before keeps reading after.
#[test]
fn the_writer_lock_is_held_across_the_swap_and_a_reader_keeps_reading() {
    let (vfs, gate) = gate::vfs(2008);
    let (db, t) = open(&vfs, Recovery::Io);
    put(&t, "a").unwrap();
    let reader =
        Pigeonhole::open_reader(DB, ReaderOptions::default().vfs(Arc::clone(&vfs))).unwrap();
    let rt = reader.table("t").unwrap();
    assert!(rt.get(b"a", "f", b"q").unwrap().is_some());
    hold_recovery(&gate, &t, "lost");
    let _ = fail_shard(&gate, &t, "lost");
    assert!(db.is_failed(), "held at the old engine's close");
    let second = Pigeonhole::open(DB, options(Arc::clone(&vfs), Recovery::Off));
    assert_eq!(
        second.err().map(|e| e.code()),
        Some(ErrorCode::WriterLocked),
        "a second writer during the recovery"
    );
    gate.release_writes();
    assert!(wait_until(20, || !db.is_failed()
        && db.recovery_stats().recoveries == 1));
    put(&t, "b").unwrap();
    assert!(
        wait_until(5, || rt.get(b"b", "f", b"q").is_ok_and(|c| c.is_some())),
        "the reader re-attaches to the new generation"
    );
    drop(rt);
    drop(reader);
    db.close().unwrap();
}

/// Application-owned mode refuses the option: the application drives, closes and reopens.
#[test]
fn application_owned_mode_refuses_the_option() {
    let (vfs, _gate) = gate::vfs(2009);
    let e = Pigeonhole::open_application_owned(DB, options(vfs, Recovery::Io)).unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e:?}");
}

/// A failed engine's close that does not return within the recovery's deadline (four
/// write-stall timeouts) ends the recovery: the handle stays failed, commits get `Closed`,
/// and the handover that returns later is dropped, so the locks go and a plain open works.
#[test]
fn a_close_that_does_not_return_in_time_gives_the_recovery_up() {
    let (vfs, gate) = gate::vfs(2010);
    let db = Pigeonhole::open(
        DB,
        options(Arc::clone(&vfs), Recovery::Io).write_stall_timeout(Duration::from_millis(100)),
    )
    .unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    put(&t, "a").unwrap();
    hold_recovery(&gate, &t, "lost");
    let _ = fail_shard(&gate, &t, "lost");
    assert!(
        wait_until(5, || db.recovery_stats().gave_up),
        "{:?}",
        db.recovery_stats()
    );
    assert!(db.is_failed());
    assert_eq!(put(&t, "b").unwrap_err().code(), ErrorCode::Closed);
    assert_eq!(db.recovery_stats().recoveries, 0);
    // The close returns once the write lands; the supervisor drops the handover.
    gate.release_writes();
    let _ = db.close();
    assert!(
        wait_until(10, || Pigeonhole::open(
            DB,
            options(Arc::clone(&vfs), Recovery::Off)
        )
        .map(|db| db.close().is_ok())
        .unwrap_or(false)),
        "a plain open after the give-up"
    );
}
