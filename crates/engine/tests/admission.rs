//! Async commit admission (D210) at the engine: a commit larger than the bound goes alone,
//! one queued behind it waits for it to resolve, and a closing engine admits every waiter.

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{Engine, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/admission.phdb";

fn open(
    seed: u64,
    bound: u64,
) -> (
    std::sync::Arc<Engine>,
    std::sync::Arc<pigeonhole_engine::TableInfo>,
) {
    let vfs = SimVfs::new(seed);
    let mut o = common::options(vfs, 1, 1 << 20);
    o.async_commit_bound = bound;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    (db, t)
}

fn batch(t: &pigeonhole_engine::TableInfo, row: &[u8], len: usize) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row,
        b"q",
        None,
        ValueRef::Bytes(&vec![7u8; len]),
    )
    .unwrap();
    wb
}

/// Polls with `poll` until it is ready, with a real waker (the shards run on their own
/// threads here), for at most about 100 s.
fn ready<T>(what: &str, mut poll: impl FnMut(&mut Context<'_>) -> Poll<T>) -> T {
    struct Unpark(std::thread::Thread);
    impl std::task::Wake for Unpark {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(std::sync::Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    for _ in 0..10_000 {
        if let Poll::Ready(v) = poll(&mut cx) {
            return v;
        }
        std::thread::park_timeout(std::time::Duration::from_millis(10));
    }
    panic!("{what}: never ready");
}

#[test]
fn a_commit_larger_than_the_bound_goes_alone() {
    let (db, t) = open(2201, 1_000);
    // 4 KiB against a 1,000-byte bound: admitted at once, since the shard holds nothing.
    let big = batch(&t, b"big", 4096);
    let admit = db.admit(&big);
    assert!(admit.is_admitted());
    assert!(db.admission_inflight(0) > 4096);
    // Anything after it waits until it resolves.
    let small = batch(&t, b"small", 8);
    let mut waiting = db.admit(&small);
    assert!(!waiting.is_admitted());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(waiting.poll_admit(&mut cx).is_pending());
    let pending = db.submit(big, Some(Durability::Buffered)).unwrap();
    admit.attach(&pending);
    let mut pending = Box::pin(pending);
    ready("the small commit's admission", |cx| waiting.poll_admit(cx));
    // The big commit's charge came back before the small one was admitted. Its future may
    // still be pending for a moment: the shard returns the charge (`on_settled`) before it
    // marks the commit done.
    ready("the big commit", |cx| Pin::new(&mut pending).poll(cx)).unwrap();
    let pending = db.submit(small, Some(Durability::Buffered)).unwrap();
    waiting.attach(&pending);
    pending.wait().unwrap();
    assert_eq!(db.admission_inflight(0), 0);
    db.close().unwrap();
}

#[test]
fn closing_admits_every_waiter_and_their_submissions_fail_closed() {
    let (db, t) = open(2202, 1_000);
    // A charge held without submitting keeps the next one waiting.
    let held = db.admit(&batch(&t, b"held", 900));
    assert!(held.is_admitted());
    let second = batch(&t, b"second", 900);
    let mut waiting = db.admit(&second);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(waiting.poll_admit(&mut cx).is_pending());
    db.close().unwrap();
    assert!(waiting.poll_admit(&mut cx).is_ready());
    assert!(db.submit(second, Some(Durability::Buffered)).is_err());
    drop(held);
    assert_eq!(db.admission_inflight(0), 0, "a dropped charge comes back");
}

#[test]
fn a_waiting_async_submission_submits_itself_once_admitted() {
    // Each async form handed back while its shard has no room is a pending commit that
    // submits once admitted: when polled (a commit, a conditional commit, a transaction) or
    // when waited on (a blocking wait).
    let (db, t) = open(2203, 1_000);
    // Larger than the bound: admitted alone, and nothing fits beside it.
    let held = db.admit(&batch(&t, b"held", 1_000));
    assert!(held.is_admitted());
    let held_bytes = db.admission_inflight(0);
    let durable = Some(Durability::Buffered);
    let mut commit = Box::pin(db.submit_async(batch(&t, b"commit", 8), durable).unwrap());
    let absent = pigeonhole_engine::Predicate::Absent {
        family: t.families[0].id,
        qualifier: b"q".to_vec(),
    };
    let mut check = Box::pin(
        db.submit_check_and_mutate_async(t.id, b"check", &absent, batch(&t, b"check", 8), durable)
            .unwrap(),
    );
    let mut txn = db.begin().unwrap();
    txn.batch()
        .put(
            t.id,
            t.families[0].id,
            b"txn",
            b"q",
            None,
            ValueRef::Bytes(b"v"),
        )
        .unwrap();
    let mut txn = Box::pin(txn.submit_async(durable).unwrap());
    let blocking = db.submit_async(batch(&t, b"blocking", 8), durable).unwrap();
    // All four wait behind the held charge, and none has submitted.
    let mut cx = Context::from_waker(Waker::noop());
    assert!(commit.as_mut().poll(&mut cx).is_pending());
    assert!(check.as_mut().poll(&mut cx).is_pending());
    assert!(txn.as_mut().poll(&mut cx).is_pending());
    assert_eq!(db.admission_inflight(0), held_bytes, "only the held charge");
    drop(held);
    let committed = ready("the commit", |cx| commit.as_mut().poll(cx)).unwrap();
    let (applied, info) = ready("the conditional commit", |cx| check.as_mut().poll(cx)).unwrap();
    assert!(applied && info.is_some(), "the row was absent");
    let txn_info = ready("the transaction", |cx| txn.as_mut().poll(cx)).unwrap();
    let blocked = blocking.wait().unwrap();
    assert!(committed.seqno > 0 && txn_info.seqno > 0 && blocked.seqno > 0);
    assert_eq!(db.admission_inflight(0), 0, "every charge came back");
    db.close().unwrap();
}

#[test]
fn pending_commits_stay_send_and_sync() {
    // Public types: a waiting state must not take an auto trait away (semver).
    fn send_sync<T: Send + Sync>() {}
    send_sync::<pigeonhole_engine::PendingCommit>();
    send_sync::<pigeonhole_engine::PendingCheck>();
}

#[test]
fn a_full_shard_does_not_hold_another_shards_async_commits() {
    // D232: each shard's charges are its own. With shard 0 at its bound, an async commit to
    // a row shard 1 owns submits at once, and its charge comes back to shard 1 alone.
    let vfs = SimVfs::new(2205);
    let mut o = common::options(vfs, 2, 1 << 20);
    o.async_commit_bound = 1_000;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    db.split_table(t.id, vec![b"m".to_vec()], &[0, 1]).unwrap();
    assert_eq!(db.shard_of(t.id, b"a"), Some(0));
    assert_eq!(db.shard_of(t.id, b"z"), Some(1));
    let held = db.admit(&batch(&t, b"a", 1_000));
    assert!(held.is_admitted());
    // Shard 0 has no room: a commit to it waits.
    let mut waiting = db.admit(&batch(&t, b"b", 8));
    assert!(!waiting.is_admitted());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(waiting.poll_admit(&mut cx).is_pending());
    // Shard 1 is empty: a commit to it is charged there and submitted at once.
    let durable = Some(Durability::Buffered);
    let pending = db.submit_async(batch(&t, b"z", 8), durable).unwrap();
    assert!(db.admission_inflight(1) > 0);
    pending.wait().unwrap();
    assert_eq!(
        db.admission_inflight(1),
        0,
        "the charge came back to shard 1"
    );
    // Shard 0 still holds its charge, and its waiter still waits.
    assert!(db.admission_inflight(0) >= 1_000);
    assert!(waiting.poll_admit(&mut cx).is_pending());
    drop(held);
    assert!(waiting.poll_admit(&mut cx).is_ready());
    drop(waiting);
    assert_eq!(db.admission_inflight(0), 0);
    db.close().unwrap();
}
