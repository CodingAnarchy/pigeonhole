//! The async front door's commits (#42, D196): `commit_async`, `commit_with_async`,
//! `Transaction::commit_async`, and `CommitTicket`. Each resolves exactly as the sync
//! commit returns, works from any executor (here a minimal one, and plain threads), drops
//! without rolling back, and polls without blocking from an application-owned shard's own
//! event loop (D88).
#![cfg(feature = "async")]

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures_core::Stream;
use pigeonhole::doc_support::block_on;
use pigeonhole::nonblocking::CommitFuture;
use pigeonhole::{Durability, ErrorCode, Family, Options, Pigeonhole, Shard, Table};
use pigeonhole_io::sim::SimVfs;

fn sim_options(vfs: &Arc<SimVfs>) -> Options {
    Options::default()
        .vfs(Arc::clone(vfs) as _)
        .shards(2)
        .memtable_budget(4 << 20)
        .wal_segment_size(256 << 10)
}

fn table(db: &Pigeonhole) -> Table {
    db.table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap()
}

fn value(t: &Table, row: &[u8]) -> Option<Vec<u8>> {
    t.get(row, "f", b"q").unwrap().map(|c| c.value().to_vec())
}

#[test]
fn async_commits_resolve_as_the_sync_ones_return() {
    let vfs = SimVfs::new(4201);
    let db = Pigeonhole::open("/db/a.phdb", sim_options(&vfs)).unwrap();
    let t = table(&db);
    // Sync and async interleaved: seqnos rise, and each commit is visible when it resolves.
    let mut last = 0;
    for i in 0..20u32 {
        let row = format!("r{i:03}");
        let v = i.to_le_bytes();
        let info = if i % 2 == 0 {
            block_on(t.mutate(row.as_bytes()).put("f", b"q", &v).commit_async()).unwrap()
        } else {
            t.mutate(row.as_bytes())
                .put("f", b"q", &v)
                .commit()
                .unwrap()
        };
        assert!(info.seqno > last);
        last = info.seqno;
        assert_eq!(
            value(&t, row.as_bytes()),
            Some(v.to_vec()),
            "visible at resolve"
        );
    }
    // Write batches, both durability forms.
    let mut wb = db.write_batch();
    wb.put(&t, b"wa", "f", b"q", b"1");
    let info = block_on(wb.commit_async()).unwrap();
    assert!(info.seqno > last);
    let mut wb = db.write_batch();
    wb.put(&t, b"wb", "f", b"q", b"2");
    let info = block_on(wb.commit_with_async(Durability::Buffered)).unwrap();
    assert_eq!(info.durability, Durability::Buffered);
    assert_eq!(value(&t, b"wb"), Some(b"2".to_vec()));
    db.close().unwrap();
}

#[test]
fn async_commit_errors_match_the_sync_ones() {
    let vfs = SimVfs::new(4202);
    let db = Pigeonhole::open("/db/e.phdb", sim_options(&vfs)).unwrap();
    let t = table(&db);
    // A builder error resolves on the first poll, without submitting anything.
    let mut fut = t.mutate(b"r").put("nope", b"q", b"v").commit_async();
    let mut cx = Context::from_waker(Waker::noop());
    match Pin::new(&mut fut).poll(&mut cx) {
        Poll::Ready(Err(e)) => assert_eq!(e.code(), ErrorCode::FamilyNotFound),
        other => panic!("expected FamilyNotFound at once, got {other:?}"),
    }
    let sync = t.mutate(b"r").put("nope", b"q", b"v").commit().unwrap_err();
    assert_eq!(sync.code(), ErrorCode::FamilyNotFound);
    // A transaction conflict, as the sync commit reports it.
    t.mutate(b"x").put("f", b"q", b"0").commit().unwrap();
    let mut txn = db.transaction().unwrap();
    let _ = txn.get(&t, b"x", "f", b"q").unwrap();
    txn.put(&t, b"x", "f", b"q", b"1");
    t.mutate(b"x").put("f", b"q", b"2").commit().unwrap();
    let e = block_on(txn.commit_async()).unwrap_err();
    assert_eq!(e.code(), ErrorCode::Conflict);
    let mut txn = db.transaction().unwrap();
    txn.put(&t, b"y", "f", b"q", b"1");
    block_on(txn.commit_with_async(Durability::Buffered)).unwrap();
    assert_eq!(value(&t, b"y"), Some(b"1".to_vec()));
    // After close: refused, as the sync commit is.
    let mut wb = db.write_batch();
    wb.put(&t, b"z", "f", b"q", b"1");
    db.close().unwrap();
    assert_eq!(
        block_on(wb.commit_async()).unwrap_err().code(),
        ErrorCode::Closed
    );
}

#[test]
fn a_dropped_commit_future_still_commits() {
    let vfs = SimVfs::new(4203);
    let db = Pigeonhole::open("/db/d.phdb", sim_options(&vfs)).unwrap();
    let t = table(&db);
    // Submitted at the call; dropped without a single poll.
    drop(t.mutate(b"dropped").put("f", b"q", b"v").commit_async());
    let mut wb = db.write_batch();
    wb.put(&t, b"dropped-batch", "f", b"q", b"w");
    drop(wb.commit_with_async(Durability::Buffered));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while value(&t, b"dropped").is_none() || value(&t, b"dropped-batch").is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "a dropped commit never landed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    db.close().unwrap();
}

#[test]
fn a_ticket_is_waited_on_checked_or_awaited() {
    let vfs = SimVfs::new(4204);
    let db = Pigeonhole::open("/db/t.phdb", sim_options(&vfs)).unwrap();
    let t = table(&db);
    let ticket = |v: &[u8]| {
        let mut wb = db.write_batch();
        wb.put(&t, b"r", "f", b"q", v);
        wb.commit_with_ticket(Durability::Buffered).unwrap()
    };
    // Checked without blocking until it resolves; then the same result every time.
    let mut checked = ticket(b"1");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let info = loop {
        if let Some(r) = checked.try_result() {
            break r.unwrap();
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(checked.seqno(), Some(info.seqno));
    assert_eq!(checked.try_result().unwrap().unwrap(), info);
    assert_eq!(checked.wait().unwrap(), info);
    // Waited on, and awaited.
    let waited = ticket(b"2").wait().unwrap();
    assert!(waited.seqno > info.seqno);
    let awaited = block_on(ticket(b"3").into_future()).unwrap();
    assert!(awaited.seqno > waited.seqno);
    assert_eq!(value(&t, b"r"), Some(b"3".to_vec()));
    // A builder error comes back from `commit_with_ticket` itself.
    let mut wb = db.write_batch();
    wb.put(&t, b"r", "nope", b"q", b"v");
    assert_eq!(
        wb.commit_with_ticket(Durability::Buffered)
            .unwrap_err()
            .code(),
        ErrorCode::FamilyNotFound
    );
    db.close().unwrap();
}

#[test]
fn async_commits_from_many_threads_join_the_same_groups() {
    fn assert_send<T: Send + 'static>() {}
    assert_send::<CommitFuture>();
    assert_send::<pigeonhole::CommitTicket>();
    let vfs = SimVfs::new(4205);
    let db = Pigeonhole::open("/db/m.phdb", sim_options(&vfs)).unwrap();
    let t = table(&db);
    std::thread::scope(|s| {
        for w in 0..4u32 {
            let t = &t;
            s.spawn(move || {
                for i in 0..50u32 {
                    let row = format!("w{w}-{i:03}");
                    let commit = t.mutate(row.as_bytes()).put("f", b"q", b"v");
                    if i % 2 == 0 {
                        block_on(commit.commit_async()).unwrap();
                    } else {
                        commit.commit().unwrap();
                    }
                }
            });
        }
    });
    assert_eq!(t.scan_prefix(b"w").iter().unwrap().count(), 200);
    db.close().unwrap();
}

/// D88: a thread that drives an application-owned shard must not block on a commit; it
/// polls the future from its event loop between `run_once` calls, and the commit resolves.
#[test]
fn a_commit_future_resolves_on_its_shards_own_event_loop() {
    let vfs = SimVfs::new(4206);
    let (db, mut shards) =
        Pigeonhole::open_application_owned("/db/app.phdb", sim_options(&vfs).shards(1)).unwrap();
    let mut shard: Shard = shards.remove(0);
    // Setup (table creation waits on the shard) with a helper thread driving it.
    let stop = Arc::new(AtomicBool::new(false));
    let driver = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                shard.run_once(Duration::from_millis(1));
            }
            shard
        })
    };
    let t = table(&db);
    stop.store(true, Ordering::Release);
    let mut shard = driver.join().unwrap();
    // This thread is now the event loop: submit, then poll between slices of shard work.
    let mut fut = t.mutate(b"loop").put("f", b"q", b"v").commit_async();
    let mut cx = Context::from_waker(Waker::noop());
    let mut polls = 0;
    let info = loop {
        if let Poll::Ready(r) = Pin::new(&mut fut).poll(&mut cx) {
            break r.unwrap();
        }
        polls += 1;
        assert!(
            polls < 100_000,
            "the commit never resolved on its event loop"
        );
        shard.run_once(Duration::from_millis(1));
    };
    assert!(info.seqno > 0);
    assert_eq!(value(&t, b"loop"), Some(b"v".to_vec()));
    db.close().unwrap();
    while shard.closed().is_none() {
        shard.run_once(Duration::from_millis(1));
    }
}

// ---- Async reads (#42 PR 2a, D196, ICR 0014) ----

/// A waker that counts its wakes.
struct Count(std::sync::atomic::AtomicUsize);

impl std::task::Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// Polls `fut` to completion with the simulated device deferring I/O: returns the result,
/// how many polls returned `Pending`, and whether the waker was woken before each re-poll.
fn drive_deferred<F: Future + Unpin>(vfs: &SimVfs, mut fut: F) -> (F::Output, usize) {
    let count = Arc::new(Count(std::sync::atomic::AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    let mut pending = 0;
    loop {
        if let Poll::Ready(out) = Pin::new(&mut fut).poll(&mut cx) {
            return (out, pending);
        }
        pending += 1;
        assert!(pending < 1000, "the read never resolved");
        assert!(vfs.io_in_flight() > 0, "pending with no I/O in flight");
        let woken = count.0.load(Ordering::Relaxed);
        vfs.complete_all_io();
        assert!(
            count.0.load(Ordering::Relaxed) > woken,
            "the completion did not wake the read"
        );
    }
}

/// A database with `rows` rows in SSTs only (flushed, then reopened: the block cache is
/// cold and no SST is open yet), and a family `big` whose values are separated.
fn cold_db(vfs: &Arc<SimVfs>, path: &str, rows: u32, options: Options) -> Pigeonhole {
    {
        let db = Pigeonhole::open(path, options.clone()).unwrap();
        let t = db
            .table("t")
            .unwrap()
            .family("f", Family::default())
            .family("big", Family::default().blob_threshold(64))
            .create_if_missing()
            .unwrap();
        for i in 0..rows {
            let row = format!("r{i:04}");
            t.mutate(row.as_bytes())
                .put("f", b"q", &i.to_le_bytes())
                .put("f", b"z", b"zz")
                .put("big", b"v", &[i as u8; 200])
                .commit()
                .unwrap();
        }
        db.flush().unwrap();
        db.close().unwrap();
    }
    let _ = vfs;
    Pigeonhole::open(path, options).unwrap()
}

#[test]
fn a_memtable_or_cache_hit_resolves_on_the_first_poll() {
    let vfs = SimVfs::new(4207);
    let db = Pigeonhole::open("/db/h.phdb", sim_options(&vfs)).unwrap();
    let t = table(&db);
    t.mutate(b"r").put("f", b"q", b"mem").commit().unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let mut get = t.get_async(b"r", "f", b"q");
    match Pin::new(&mut get).poll(&mut cx) {
        Poll::Ready(Ok(Some(c))) => assert_eq!(c.value(), b"mem"),
        other => panic!("a memtable hit must resolve at once: {other:?}"),
    }
    let mut miss = t.get_async(b"absent", "f", b"q");
    assert!(matches!(
        Pin::new(&mut miss).poll(&mut cx),
        Poll::Ready(Ok(None))
    ));
    let mut row = t.row(b"r").read_async();
    match Pin::new(&mut row).poll(&mut cx) {
        Poll::Ready(Ok(Some(r))) => assert_eq!(r.get("f", b"q").unwrap().value(), b"mem"),
        other => panic!("a memtable row must resolve at once: {other:?}"),
    }
    db.close().unwrap();
}

#[test]
fn a_cold_get_waits_on_io_and_reads_what_the_sync_get_reads() {
    let vfs = SimVfs::new(4208);
    let db = cold_db(&vfs, "/db/c.phdb", 300, sim_options(&vfs));
    let t = db.table("t").unwrap().open().unwrap();
    vfs.set_deferred_io(true);
    let (got, pending) = drive_deferred(&vfs, t.get_async(b"r0123", "f", b"q"));
    assert!(pending > 0, "a cold get resolved without waiting on I/O");
    assert_eq!(got.unwrap().unwrap().value(), &123u32.to_le_bytes());
    // Now cached: the same get resolves at once, and a missing column too.
    let mut cx = Context::from_waker(Waker::noop());
    let mut again = t.get_async(b"r0123", "f", b"q");
    assert!(matches!(
        Pin::new(&mut again).poll(&mut cx),
        Poll::Ready(Ok(Some(_)))
    ));
    let (none, _) = drive_deferred(&vfs, t.get_async(b"r0123", "f", b"absent"));
    assert!(none.unwrap().is_none());
    // Every row through both paths agrees.
    for i in (0..300).step_by(37) {
        let row = format!("r{i:04}");
        let (a, _) = drive_deferred(&vfs, t.get_async(row.as_bytes(), "f", b"q"));
        let a = a.unwrap().map(|c| c.value().to_vec());
        vfs.set_deferred_io(false);
        let s = t
            .get(row.as_bytes(), "f", b"q")
            .unwrap()
            .map(|c| c.value().to_vec());
        vfs.set_deferred_io(true);
        assert_eq!(a, s, "row {row}");
    }
    assert_eq!(db.async_sync_reads(), 0, "no block was read synchronously");
    vfs.set_deferred_io(false);
    db.close().unwrap();
}

#[test]
fn a_cold_row_read_waits_on_io_and_matches_the_sync_read() {
    let vfs = SimVfs::new(4209);
    let db = cold_db(&vfs, "/db/rr.phdb", 300, sim_options(&vfs));
    let t = db.table("t").unwrap().open().unwrap();
    vfs.set_deferred_io(true);
    let (row, pending) = drive_deferred(&vfs, t.row(b"r0200").family("f").read_async());
    assert!(pending > 0);
    let row = row.unwrap().unwrap();
    vfs.set_deferred_io(false);
    let sync = t.row(b"r0200").family("f").read().unwrap().unwrap();
    let cells = |r: &pigeonhole::Row| {
        r.view()
            .iter()
            .map(|e| {
                (
                    e.family.to_owned(),
                    e.qualifier.to_vec(),
                    e.cell.value().to_vec(),
                )
            })
            .collect::<Vec<_>>()
    };
    let sync_cells: Vec<_> = sync
        .iter()
        .map(|e| {
            (
                e.family.to_owned(),
                e.qualifier.to_vec(),
                e.cell.value().to_vec(),
            )
        })
        .collect();
    assert_eq!(cells(&row), sync_cells);
    assert_eq!(row.key(), b"r0200");
    assert_eq!(db.async_sync_reads(), 0);
    db.close().unwrap();
}

#[test]
fn dropping_a_read_future_mid_io_is_safe() {
    let vfs = SimVfs::new(4210);
    let db = cold_db(&vfs, "/db/dr.phdb", 100, sim_options(&vfs));
    let t = db.table("t").unwrap().open().unwrap();
    vfs.set_deferred_io(true);
    let mut cx = Context::from_waker(Waker::noop());
    let mut get = t.get_async(b"r0042", "f", b"q");
    assert!(Pin::new(&mut get).poll(&mut cx).is_pending());
    let mut row = t.row(b"r0043").read_async();
    assert!(Pin::new(&mut row).poll(&mut cx).is_pending());
    drop((get, row));
    vfs.complete_all_io();
    vfs.set_deferred_io(false);
    // The database is unaffected, and closes (no view or block left pinned).
    assert_eq!(
        t.get(b"r0042", "f", b"q").unwrap().unwrap().value(),
        &42u32.to_le_bytes()
    );
    db.close().unwrap();
}

#[test]
fn a_separated_value_waits_on_io_and_a_cache_that_keeps_nothing_reads_synchronously() {
    let vfs = SimVfs::new(4211);
    let db = cold_db(&vfs, "/db/sv.phdb", 50, sim_options(&vfs));
    let t = db.table("t").unwrap().open().unwrap();
    // A separated value (#42 PR 2b): its extent header and its record are fetched
    // asynchronously, then cached; no synchronous read.
    vfs.set_deferred_io(true);
    let (v, pending) = drive_deferred(&vfs, t.get_async(b"r0007", "big", b"v"));
    assert!(
        pending > 0,
        "a cold separated value resolved without waiting on I/O"
    );
    assert_eq!(v.unwrap().unwrap().value(), &[7u8; 200]);
    let (row, _) = drive_deferred(&vfs, t.row(b"r0008").read_async());
    let row = row.unwrap().unwrap();
    assert_eq!(row.get("big", b"v").unwrap().value(), &[8u8; 200]);
    vfs.set_deferred_io(false);
    assert_eq!(
        db.async_sync_reads(),
        0,
        "a separated value was read synchronously"
    );
    db.close().unwrap();
    // A cache that keeps nothing: the fetched block is not found again, so the read goes
    // synchronous once (counted) instead of fetching forever.
    let vfs = SimVfs::new(4212);
    let db = cold_db(&vfs, "/db/nc.phdb", 50, sim_options(&vfs).block_cache(0));
    let t = db.table("t").unwrap().open().unwrap();
    let got = block_on(t.get_async(b"r0011", "f", b"q")).unwrap().unwrap();
    assert_eq!(got.value(), &11u32.to_le_bytes());
    assert!(db.async_sync_reads() > 0);
    db.close().unwrap();
}

/// A table with one large separated value per row (`len` bytes, all `i as u8`), flushed and
/// reopened cold.
fn cold_blobs(path: &str, rows: u32, len: usize, options: Options) -> Pigeonhole {
    {
        let db = Pigeonhole::open(path, options.clone()).unwrap();
        let t = db
            .table("t")
            .unwrap()
            .family("big", Family::default().blob_threshold(64))
            .create_if_missing()
            .unwrap();
        for i in 0..rows {
            t.mutate(format!("r{i:04}").as_bytes())
                .put("big", b"v", &vec![i as u8; len])
                .commit()
                .unwrap();
        }
        db.flush().unwrap();
        db.close().unwrap();
    }
    Pigeonhole::open(path, options).unwrap()
}

#[test]
fn a_separated_value_spanning_blob_extents_is_fetched_in_pieces() {
    // 100 KiB records in 64 KiB blob extents: most span two extents, so their fetch is two
    // reads joined into one completion.
    let vfs = SimVfs::new(4213);
    let db = cold_blobs("/db/span.phdb", 6, 100 << 10, sim_options(&vfs));
    let t = db.table("t").unwrap().open().unwrap();
    vfs.set_deferred_io(true);
    for i in 0..6u32 {
        let row = format!("r{i:04}");
        let (v, pending) = drive_deferred(&vfs, t.get_async(row.as_bytes(), "big", b"v"));
        assert!(
            pending > 0 || i > 0,
            "the first cold record resolved without I/O"
        );
        let v = v.unwrap().unwrap();
        assert_eq!(v.value().len(), 100 << 10, "row {row}");
        assert!(v.value().iter().all(|&b| b == i as u8), "row {row}");
    }
    vfs.set_deferred_io(false);
    assert_eq!(db.async_sync_reads(), 0);
    db.close().unwrap();
}

#[test]
fn a_separated_value_too_large_to_cache_reads_synchronously_and_counts() {
    // A 1 MiB block cache caches records up to 128 KiB (an eighth): a 200 KiB value is read
    // synchronously inside the async get, and counted (D196 option (a), #398).
    let vfs = SimVfs::new(4214);
    let db = cold_blobs(
        "/db/big.phdb",
        2,
        200 << 10,
        sim_options(&vfs).block_cache(1 << 20),
    );
    let t = db.table("t").unwrap().open().unwrap();
    let v = block_on(t.get_async(b"r0001", "big", b"v"))
        .unwrap()
        .unwrap();
    assert_eq!(v.value(), &vec![1u8; 200 << 10][..]);
    assert!(
        db.async_sync_reads() > 0,
        "the oversized record's read was not counted"
    );
    db.close().unwrap();
}

// ---- Scan streams (#42 PR 3, D196) ----

/// Polls the stream's next item with the simulated device deferring I/O (completing it
/// whenever the stream is pending); returns the item and how many polls were pending.
fn next_deferred<S: futures_core::Stream + Unpin>(
    vfs: &SimVfs,
    s: &mut S,
) -> (Option<S::Item>, usize) {
    let count = Arc::new(Count(std::sync::atomic::AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    let mut pending = 0;
    loop {
        if let Poll::Ready(item) = Pin::new(&mut *s).poll_next(&mut cx) {
            return (item, pending);
        }
        pending += 1;
        assert!(pending < 10_000, "the stream never produced an item");
        assert!(vfs.io_in_flight() > 0, "pending with no I/O in flight");
        vfs.complete_all_io();
    }
}

type Cells = Vec<(Vec<u8>, String, Vec<u8>, Vec<u8>)>;

fn row_cells(r: &pigeonhole::Row) -> Cells {
    r.view()
        .iter()
        .map(|e| {
            (
                r.key().to_vec(),
                e.family.to_owned(),
                e.qualifier.to_vec(),
                e.cell.value().to_vec(),
            )
        })
        .collect()
}

fn sync_scan(t: &Table, prefix: &[u8]) -> Cells {
    t.scan_prefix(prefix)
        .iter()
        .unwrap()
        .flat_map(|r| row_cells(&r.unwrap()))
        .collect()
}

#[test]
fn a_memtable_scan_stream_yields_what_the_iterator_does() {
    let vfs = SimVfs::new(4215);
    let db = Pigeonhole::open("/db/sm.phdb", sim_options(&vfs)).unwrap();
    let t = table(&db);
    for i in 0..200u32 {
        t.mutate(format!("r{i:04}").as_bytes())
            .put("f", b"q", &i.to_le_bytes())
            .commit()
            .unwrap();
    }
    let mut s = t.scan_prefix(b"r").stream();
    let mut got = Cells::new();
    while let (Some(row), _) = next_deferred(&vfs, &mut s) {
        got.extend(row_cells(&row.unwrap()));
    }
    assert_eq!(got, sync_scan(&t, b"r"));
    // limit(0) yields nothing; a bad family is the first item.
    let (none, _) = next_deferred(&vfs, &mut t.scan_prefix(b"r").limit(0).stream());
    assert!(none.is_none());
    let mut bad = t.scan_prefix(b"r").family("nope").stream();
    let (first, _) = next_deferred(&vfs, &mut bad);
    assert_eq!(
        first.unwrap().unwrap_err().code(),
        ErrorCode::FamilyNotFound
    );
    assert!(next_deferred(&vfs, &mut bad).0.is_none());
    let (limited, _) = {
        let mut s = t.scan_prefix(b"r").limit(3).stream();
        let mut n = 0;
        while let (Some(r), _) = next_deferred(&vfs, &mut s) {
            r.unwrap();
            n += 1;
        }
        (n, ())
    };
    assert_eq!(limited, 3);
    db.close().unwrap();
}

#[test]
fn a_cold_scan_stream_waits_on_io_and_yields_what_the_iterator_does() {
    let vfs = SimVfs::new(4216);
    let db = cold_db(&vfs, "/db/sc.phdb", 2000, sim_options(&vfs));
    let t = db.table("t").unwrap().open().unwrap();
    vfs.set_deferred_io(true);
    let mut s = t.scan_prefix(b"r").family("f").stream();
    let mut got = Cells::new();
    let mut pending_total = 0;
    loop {
        let (row, pending) = next_deferred(&vfs, &mut s);
        pending_total += pending;
        match row {
            Some(r) => got.extend(row_cells(&r.unwrap())),
            None => break,
        }
    }
    assert!(pending_total > 0, "a cold scan never waited on I/O");
    vfs.set_deferred_io(false);
    let want: Cells = t
        .scan_prefix(b"r")
        .family("f")
        .iter()
        .unwrap()
        .flat_map(|r| row_cells(&r.unwrap()))
        .collect();
    assert_eq!(got.len(), want.len());
    assert_eq!(got, want);
    assert_eq!(
        db.async_sync_reads(),
        0,
        "a forward scan read a block synchronously"
    );
    db.close().unwrap();
}

#[test]
fn dropping_a_scan_stream_mid_io_is_safe() {
    let vfs = SimVfs::new(4217);
    let db = cold_db(&vfs, "/db/sd.phdb", 500, sim_options(&vfs));
    let t = db.table("t").unwrap().open().unwrap();
    vfs.set_deferred_io(true);
    let mut s = t.scan_prefix(b"r").stream();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(Pin::new(&mut s).poll_next(&mut cx).is_pending());
    drop(s);
    vfs.complete_all_io();
    vfs.set_deferred_io(false);
    assert_eq!(t.scan_prefix(b"r").iter().unwrap().count(), 500);
    db.close().unwrap();
}

/// Multi-threaded executors (Tokio's `spawn`) need `Send` futures; a scan stream borrows its
/// table, so it is `Send` whenever the borrow is.
#[test]
fn futures_and_streams_are_send() {
    fn send<T: Send>(_: &T) {}
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("send.phdb"), Options::default()).unwrap();
    let t = table(&db);
    send(&t.get_async(b"r", "f", b"q"));
    send(&t.row(b"r").read_async());
    send(&t.scan_prefix(b"r").stream());
    send(&t.mutate(b"r").put("f", b"q", b"v").commit_async());
    let mut wb = db.write_batch();
    wb.put(&t, b"r", "f", b"q", b"v");
    send(
        &wb.commit_with_ticket(Durability::Sync)
            .unwrap()
            .into_future(),
    );
    db.close().unwrap();
}
