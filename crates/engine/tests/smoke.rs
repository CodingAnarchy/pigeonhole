//! Basic application-owned flows with a step budget, so a hang fails fast.

mod common;

use std::task::Poll;

use common::{Store, families, poll_commit};
use pigeonhole_format::Durability;
use pigeonhole_io::Vfs;
use pigeonhole_sim::{ModelOp, Sim};

fn put(row: &str, q: &str, v: &[u8]) -> ModelOp {
    ModelOp::Put {
        table: common::table_of(row.as_bytes()).into(),
        row: row.into(),
        family: "f".into(),
        qualifier: q.into(),
        ts: None,
        value: v.to_vec(),
    }
}

fn commit(
    store: &mut Store,
    vfs: &std::sync::Arc<pigeonhole_io::sim::SimVfs>,
    ops: &[ModelOp],
    d: Durability,
) -> pigeonhole_engine::CommitInfo {
    let batch = store.batch(ops).unwrap();
    let mut pc = store.engine.submit(batch, Some(d)).unwrap();
    for _ in 0..10_000 {
        match poll_commit(&mut pc) {
            Poll::Ready(r) => return r.unwrap(),
            Poll::Pending => store.step_shards(vfs.monotonic_nanos()),
        }
    }
    panic!("commit did not resolve in 10k steps");
}

#[test]
fn single_shard_commit_and_read() {
    let sim = Sim::new(1);
    let vfs = sim.vfs();
    let mut store = Store::open(&vfs, 1, 4 << 20, &families()).unwrap();
    let info = commit(
        &mut store,
        &vfs,
        &[put("r", "q", b"v")],
        Durability::GroupSync,
    );
    assert_eq!(info.seqno, 1);
    let snap = store.engine.snapshot().unwrap();
    assert_eq!(snap.seqno(), 1);
    let cell = store.get(&snap, b"r", "f", b"q").unwrap().unwrap();
    assert_eq!(cell.value, b"v");
    let info = commit(&mut store, &vfs, &[put("r", "q", b"w")], Durability::None);
    assert_eq!(info.seqno, 2);
    let snap = store.engine.snapshot().unwrap();
    assert_eq!(
        store.get(&snap, b"r", "f", b"q").unwrap().unwrap().value,
        b"w"
    );
}

#[test]
fn cross_shard_commit_is_atomic() {
    let sim = Sim::new(2);
    let vfs = sim.vfs();
    let mut store = Store::open(&vfs, 4, 4 << 20, &families()).unwrap();
    // Find two rows on different shards (rows spread over tables, tables over shards).
    let view = store.engine.snapshot().unwrap().view().clone();
    let shard_of = |r: &str| {
        let t = store.tables[common::table_of(r.as_bytes())].id;
        view.tablets().route(t, r.as_bytes()).unwrap().1
    };
    let a = "row000000";
    let mut b = None;
    for i in 1..100 {
        let r = format!("row{i:06}");
        if shard_of(&r) != shard_of(a) {
            b = Some(r);
            break;
        }
    }
    let b = b.expect("a row on another shard");
    let info = commit(
        &mut store,
        &vfs,
        &[put(a, "q", b"1"), put(&b, "q", b"2")],
        Durability::GroupSync,
    );
    assert_eq!(info.seqno, 1);
    let snap = store.engine.snapshot().unwrap();
    assert_eq!(
        store
            .get(&snap, a.as_bytes(), "f", b"q")
            .unwrap()
            .unwrap()
            .value,
        b"1"
    );
    assert_eq!(
        store
            .get(&snap, b.as_bytes(), "f", b"q")
            .unwrap()
            .unwrap()
            .value,
        b"2"
    );
    let info = commit(
        &mut store,
        &vfs,
        &[put(a, "q", b"3"), put(&b, "q", b"4")],
        Durability::Buffered,
    );
    assert_eq!(info.seqno, 2);
}
