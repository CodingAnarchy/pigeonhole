//! Reader-process limits as documented errors, and the writer's reader metrics (#693 PR 5):
//! `Options::reader_slots` (ICR 0055) bounds attached readers and one more fails
//! `NoReaderSlot` at once; a killed reader's slot comes back; `engine_metrics()` reports
//! the slots in use, the slots reclaimed, and how far the oldest reader snapshot lags.

use std::sync::Arc;
use std::time::{Duration, Instant};

use pigeonhole::{ErrorCode, Family, Options, Pigeonhole, ReaderOptions, Table};
use pigeonhole_io::ProcessId;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/readers.phdb";
const WRITER: ProcessId = ProcessId {
    pid: 1,
    start_time: 1,
};

fn reader_process(pid: u32) -> ProcessId {
    ProcessId { pid, start_time: 1 }
}

fn open(vfs: &Arc<SimVfs>, slots: u32) -> (Pigeonhole, Table) {
    vfs.enter_process(WRITER);
    let db = Pigeonhole::open(
        DB,
        Options::default()
            .vfs(Arc::clone(vfs) as _)
            .shards(1)
            .pin_threads(false)
            .memtable_budget(4 << 20)
            .reader_slots(slots),
    )
    .unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    (db, t)
}

fn reader(vfs: &Arc<SimVfs>, pid: u32) -> pigeonhole::Result<pigeonhole::PigeonholeReader> {
    vfs.enter_process(reader_process(pid));
    let r = Pigeonhole::open_reader(DB, ReaderOptions::default().vfs(Arc::clone(vfs) as _));
    vfs.enter_process(WRITER);
    r
}

fn put(t: &Table, range: std::ops::Range<u32>) {
    for i in range {
        t.mutate(format!("r{i:06}").as_bytes())
            .put("f", b"q", &[1; 100])
            .commit()
            .unwrap();
    }
}

#[test]
fn reader_slots_bound_attached_readers_and_one_more_fails_at_once() {
    let vfs = SimVfs::new(55);
    let (db, t) = open(&vfs, 2);
    put(&t, 0..10);
    let a = reader(&vfs, 10).unwrap();
    let b = reader(&vfs, 11).unwrap();
    let m = db.engine_metrics();
    assert_eq!((m.reader_slots_active, m.reader_slots), (2, 2));

    // All taken: an error at once, not a wait. The readers open with their own default
    // configuration (128 slots): the count comes from the region header the writer built,
    // so a writer's count, older or newer default alike, is what binds them.
    let start = Instant::now();
    let e = reader(&vfs, 12).unwrap_err();
    assert_eq!(e.code(), ErrorCode::NoReaderSlot, "{e}");
    assert!(start.elapsed() < Duration::from_secs(1));

    // A reader closing frees its slot for the next.
    drop(a);
    let c = reader(&vfs, 12).unwrap();
    assert_eq!(db.engine_metrics().reader_slots_active, 2);
    drop((b, c));
    assert_eq!(db.engine_metrics().reader_slots_active, 0);

    // The count is clamped to at least one.
    drop(t);
    db.close().unwrap();
    let (db, t) = open(&vfs, 0);
    assert_eq!(db.engine_metrics().reader_slots, 1);
    drop(t);
    db.close().unwrap();
}

#[test]
fn a_killed_readers_slot_comes_back_and_is_counted() {
    let vfs = SimVfs::new(56);
    let (db, t) = open(&vfs, 1);
    put(&t, 0..10);
    let doomed = reader(&vfs, 20).unwrap();
    assert_eq!(
        reader(&vfs, 21).unwrap_err().code(),
        ErrorCode::NoReaderSlot
    );
    // Killed: it runs no destructors, so its handle is forgotten.
    vfs.crash_process(reader_process(20));
    std::mem::forget(doomed);
    // Past the writer's 250 ms liveness interval, its next reclamation frees the slot.
    vfs.advance(300_000_000);
    put(&t, 10..20);
    db.flush().unwrap();
    let m = db.engine_metrics();
    assert_eq!((m.reader_slots_active, m.reader_slots_reclaimed), (0, 1));
    let next = reader(&vfs, 21).unwrap();
    drop(next);
    drop(t);
    db.close().unwrap();
}

#[test]
fn the_oldest_reader_snapshots_lag_is_reported() {
    let vfs = SimVfs::new(57);
    let (db, t) = open(&vfs, 4);
    put(&t, 0..10);
    let r = reader(&vfs, 30).unwrap();
    vfs.enter_process(reader_process(30));
    let snap = r.snapshot().unwrap();
    vfs.enter_process(WRITER);
    let m = db.engine_metrics();
    assert_eq!(m.reader_pin_seqno_lag, 0);
    assert!(m.reader_pin_view_lag <= 1, "{}", m.reader_pin_view_lag);

    // The writer commits and publishes new views; the held snapshot falls behind.
    put(&t, 10..50);
    db.flush().unwrap();
    let m = db.engine_metrics();
    assert!(m.reader_pin_seqno_lag >= 40, "{}", m.reader_pin_seqno_lag);
    assert!(m.reader_pin_view_lag >= 1, "{}", m.reader_pin_view_lag);

    // A fresh snapshot (the old one dropped) catches the pin up.
    vfs.enter_process(reader_process(30));
    drop(snap);
    let snap = r.snapshot().unwrap();
    vfs.enter_process(WRITER);
    let m = db.engine_metrics();
    // No commit since the snapshot; a background flush or compaction may have published
    // one more view meanwhile.
    assert_eq!(m.reader_pin_seqno_lag, 0);
    assert!(m.reader_pin_view_lag <= 1, "{}", m.reader_pin_view_lag);
    vfs.enter_process(reader_process(30));
    drop(snap);
    drop(r);
    vfs.enter_process(WRITER);
    drop(t);
    db.close().unwrap();
}

/// #844 (ICR 0057): with no writer running (the last one closed), `open_reader` fails with
/// `NoWriter`, not `Io`. A file that does not exist is still `Io`: only a missing writer
/// region is `NoWriter`.
#[test]
fn a_reader_with_no_writer_running_gets_no_writer() {
    let vfs = SimVfs::new(844);
    let missing = reader(&vfs, 40).unwrap_err();
    assert_eq!(missing.code(), ErrorCode::Io, "no file at all: {missing}");
    let (db, t) = open(&vfs, 4);
    put(&t, 0..10);
    drop(t);
    db.close().unwrap();
    let after = reader(&vfs, 41).unwrap_err();
    assert_eq!(after.code(), ErrorCode::NoWriter, "{after}");
    assert!(after.message().contains("no writer is running"), "{after}");
}
