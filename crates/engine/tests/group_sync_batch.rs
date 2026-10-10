//! D207 (#412): at most one group sync in flight per stream. Commits that need a group sync
//! while one is in flight batch behind it, and the next sync covers them all, so concurrent
//! `GroupSync` commits share syncs instead of each starting one (which only contended on the
//! file: a sync counts once every older one finished, D58). Deterministic: one shard driven
//! by hand, the simulator's I/O deferred until the test completes it.

use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    CommitInfo, Engine, EngineOptions, EngineShard, Error, FamilyOptions, PendingCommit, TableInfo,
    ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::{SimOp, SimVfs};

const DB: &str = "/db/data.phdb";
const CLIENTS: usize = 16;

fn poll(pc: &mut PendingCommit) -> Poll<Result<CommitInfo, Error>> {
    Pin::new(pc).poll(&mut Context::from_waker(Waker::noop()))
}

fn batch(t: &TableInfo, i: usize) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        format!("row{i:03}").as_bytes(),
        b"q",
        None,
        ValueRef::Bytes(b"value"),
    )
    .unwrap();
    wb
}

/// Runs the shard until it has nothing left to do.
fn idle(shard: &mut EngineShard) {
    while shard.run_once(u64::MAX) {}
}

/// `sync_data` calls the simulated device ran since recording started (the WAL's group
/// syncs: open, flushes and spares use `sync_all`).
fn data_syncs(vfs: &SimVfs) -> usize {
    vfs.recorded_ops()
        .iter()
        .filter(|op| {
            matches!(
                op,
                SimOp::Sync {
                    metadata: false,
                    ..
                }
            )
        })
        .count()
}

#[test]
fn concurrent_group_sync_commits_batch_behind_the_syncs_in_flight() {
    // The platform's depth (D207: 1 on macOS, 2 elsewhere), or the measurement override.
    let depth = pigeonhole_engine::group_sync_depth() as usize;
    if depth == 0 {
        // Unlimited: every group syncs on its own, as before D207.
        return;
    }
    let vfs = SimVfs::new(207);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    let (engine, mut shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
    let shard = &mut shards[0];
    let t = engine
        .create_table("t", &[("f".to_owned(), FamilyOptions::default())])
        .unwrap();
    idle(shard);
    vfs.set_deferred_io(true);
    vfs.record_ops();

    // The clients commit one at a time, each in a group of its own: the first `depth` groups
    // each start a sync, which stays in flight; the rest batch behind them.
    let mut pending = Vec::new();
    for i in 0..CLIENTS {
        pending.push(
            engine
                .submit(batch(&t, i), Some(Durability::GroupSync))
                .unwrap(),
        );
        idle(shard);
    }
    assert_eq!(data_syncs(&vfs), 0, "nothing synced yet");
    assert!(
        pending.iter_mut().all(|pc| poll(pc).is_pending()),
        "none acknowledged before a sync"
    );

    // The syncs in flight complete: their commits are acknowledged; the batched ones are not
    // (their records came after them), and one sync for all of them starts.
    vfs.complete_all_io();
    idle(shard);
    for (i, pc) in pending[..depth].iter_mut().enumerate() {
        assert!(matches!(poll(pc), Poll::Ready(Ok(_))), "commit {i}");
    }
    assert!(
        pending[depth..].iter_mut().all(|pc| poll(pc).is_pending()),
        "a batched commit acknowledged before the sync that covers it"
    );
    assert_eq!(data_syncs(&vfs), depth);

    // The batched sync completes: every commit is acknowledged, after depth + 1 syncs.
    vfs.complete_all_io();
    idle(shard);
    for (i, pc) in pending.iter_mut().enumerate() {
        assert!(matches!(poll(pc), Poll::Ready(Ok(_))), "commit {i}");
    }
    assert_eq!(
        data_syncs(&vfs),
        depth + 1,
        "{CLIENTS} commits shared {} syncs",
        depth + 1
    );
}
