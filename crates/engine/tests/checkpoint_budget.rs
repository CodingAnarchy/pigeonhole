//! #763: a shard pops at most `CHECKPOINT_POP_BUDGET` (64) unneeded records from its log per
//! batch, the rest on the batches a kick brings, so a flush or a settle that frees thousands
//! of records no longer holds a turn (and the shard's pending watermark) for milliseconds.
//!
//! Application-owned, one shard driven by the test, so every turn is observable.

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{Engine, EngineShard, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";
/// Large enough that nothing flushes until the test asks.
const BUDGET: u64 = 64 << 20;
const COMMITS: u64 = 1000;
const POP_BUDGET: u64 = 64;

/// Runs the shard until `pending` resolves.
fn drive<F: Future + Unpin>(shards: &mut [EngineShard], mut pending: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..1_000_000 {
        for s in shards.iter_mut() {
            s.run_once(u64::MAX);
        }
        if let Poll::Ready(r) = Pin::new(&mut pending).poll(&mut cx) {
            return r;
        }
    }
    panic!("never resolved");
}

#[test]
fn a_flush_that_frees_a_thousand_records_pops_them_over_several_batches() {
    let vfs = SimVfs::new(763);
    let db = Engine::open(Path::new(DB), common::options(Arc::clone(&vfs), 1, BUDGET)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    db.close().unwrap();
    drop(db);

    let (db, mut shards) =
        Engine::open_application_owned(Path::new(DB), common::options(Arc::clone(&vfs), 1, BUDGET))
            .unwrap();
    let f = t.families[0].id;
    for i in 0..COMMITS {
        let mut wb = WriteBatch::new();
        wb.put(t.id, f, &i.to_be_bytes(), b"q", None, ValueRef::Bytes(b"v"))
            .unwrap();
        drive(
            &mut shards,
            db.submit(wb, Some(Durability::Buffered)).unwrap(),
        )
        .unwrap();
    }
    // Every record stays until its memtable flushes.
    assert_eq!(db.logged_records(0), COMMITS);

    // The flush's install frees them all at once; the checkpoint pops a budget per batch,
    // the rest on the batches its kicks bring, while the flush resolves and after.
    let flush = db.submit_flush().unwrap();
    drive(&mut shards, flush).unwrap();
    for _ in 0..100_000 {
        if db.logged_records(0) == 0 {
            break;
        }
        shards[0].run_once(u64::MAX);
    }
    assert_eq!(
        db.logged_records(0),
        0,
        "records the checkpoint never passed"
    );
    let largest = db.max_checkpoint_pops(0);
    assert!(
        largest <= POP_BUDGET,
        "a batch popped {largest} records, more than the budget of {POP_BUDGET}"
    );
    assert_eq!(
        largest, POP_BUDGET,
        "no batch reached the budget: nothing tested"
    );
    db.close().unwrap();
}
