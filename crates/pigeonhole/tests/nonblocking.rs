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
