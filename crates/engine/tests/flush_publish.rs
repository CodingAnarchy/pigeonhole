//! A flushed memtable never stays in a view beside its own SST (issue #131): a merge operand
//! would be counted twice there. A shard publishes its memtable piece while a manifest
//! commit on another thread publishes the SST of one of those memtables; the shard's piece
//! must not put the memtable back. The test thread plays that other thread: it holds the
//! manifest exclusion, so the shard's flush request waits in the queue, and commits it at
//! the start of the shard's next view publish.

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{Engine, EngineOptions, EngineShard, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

fn ready<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
    match Pin::new(f).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

#[test]
fn a_flush_published_while_a_shard_publishes_its_memtables_counts_each_operand_once() {
    let vfs = SimVfs::new(131);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.reader_slots = 4;
    let (engine, mut shards) =
        Engine::open_application_owned(Path::new("/db/data.phdb"), o).expect("open");
    let mut shard: EngineShard = shards.remove(0);
    let step = |shard: &mut EngineShard| {
        for _ in 0..64 {
            let now = pigeonhole_io::Vfs::monotonic_nanos(&*vfs);
            if !shard.run_once(now + 1_000_000) {
                break;
            }
        }
    };
    let counter = FamilyOptions::default().merge_operator("pigeonhole.i64_add");
    let t = engine
        .create_table(
            "t",
            &[
                ("counter".into(), counter),
                ("f".into(), FamilyOptions::default()),
            ],
        )
        .expect("table");
    let (c, f) = (t.families[0].id, t.families[1].id);

    // One operand, in the counter family's memtable.
    let mut b = WriteBatch::new();
    b.merge(t.id, c, b"row", b"q", ValueRef::I64(5)).unwrap();
    let mut pc = engine.submit(b, Some(Durability::Buffered)).unwrap();
    step(&mut shard);
    assert!(ready(&mut pc).expect("committed").is_ok());

    // The memtable is frozen and written to an SST; its manifest request waits for the
    // exclusion this thread holds.
    assert!(engine.hold_manifest());
    let _flush = engine.flush_pending().unwrap();
    step(&mut shard);

    // The shard's next publish (its new memtable for family `f`) starts while the flush's
    // commit has not published yet; that commit publishes before the shard's piece lands.
    let e = Arc::clone(&engine);
    engine.before_next_view_publish(Box::new(move || e.drain_manifest()));
    let mut b = WriteBatch::new();
    b.put(t.id, f, b"row", b"q", None, ValueRef::Bytes(b"x"))
        .unwrap();
    let mut pc = engine.submit(b, Some(Durability::Buffered)).unwrap();
    step(&mut shard);
    engine.release_manifest();
    step(&mut shard);
    assert!(ready(&mut pc).expect("committed").is_ok());

    let snap = engine.snapshot().unwrap();
    let cell = engine
        .get(&snap, t.id, c, b"row", b"q")
        .unwrap()
        .expect("the counter");
    assert_eq!(
        cell.value(),
        ValueRef::I64(5),
        "the flushed memtable is read beside its SST"
    );
    drop(snap);
    engine.close().unwrap();
    step(&mut shard);
}
