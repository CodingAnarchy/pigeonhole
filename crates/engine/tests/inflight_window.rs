//! #922: a commit round counts in the balancer's commits-in-flight term (D257) only for the
//! part of it inside the interval it ends in. A round that stays in flight over several
//! balancer passes once added its whole span to the interval it ended in, so one blocking
//! client read many commits in flight (`spin_pool_terms` saw it as a flake when a sync took
//! longer than an interval). Deterministic: one application-owned shard driven by hand on
//! the simulator's clock, its I/O deferred until the test completes it.

use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    CommitInfo, Engine, EngineOptions, EngineShard, Error, FamilyOptions, PendingCommit, ValueRef,
    WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";
const INTERVAL: u64 = 10_000_000;

fn poll(pc: &mut PendingCommit) -> Poll<Result<CommitInfo, Error>> {
    Pin::new(pc).poll(&mut Context::from_waker(Waker::noop()))
}

/// Runs the shard until it has nothing left to do.
fn idle(shard: &mut EngineShard) {
    while shard.run_once(u64::MAX) {}
}

/// Ends a balancer interval: `INTERVAL` passes, then a pass (forced, so an idle shard's
/// backoff never stretches the interval).
fn pass(engine: &Engine, vfs: &SimVfs, shard: &mut EngineShard) {
    vfs.advance(INTERVAL);
    let _pass = engine.balance_pending().unwrap();
    idle(shard);
}

/// Seed-replay: `PIGEONHOLE_SEED` picks the simulator's seed (the run's I/O order is the
/// seed's; the clock is the test's).
#[test]
fn a_round_in_flight_over_many_passes_counts_one_commit_at_most() {
    let seed = std::env::var("PIGEONHOLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(922);
    eprintln!("seed {seed}");
    let vfs = SimVfs::new(seed);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.durability = Durability::Sync;
    o.balance_interval_nanos = INTERVAL;
    let (engine, mut shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
    let shard = &mut shards[0];
    let t = engine
        .create_table("t", &[("f".to_owned(), FamilyOptions::default())])
        .unwrap();
    idle(shard);
    // A first pass, so the next interval has a start.
    pass(&engine, &vfs, shard);

    // One commit whose sync stays in flight over 30 balancer intervals.
    vfs.set_deferred_io(true);
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        b"row",
        b"q",
        None,
        ValueRef::Bytes(b"value"),
    )
    .unwrap();
    let mut pc = engine.submit(wb, None).unwrap();
    idle(shard);
    for _ in 0..30 {
        pass(&engine, &vfs, shard);
        assert!(poll(&mut pc).is_pending(), "acknowledged before its sync");
    }

    // It completes, part way into an interval; the pass that ends that interval counts at
    // most the part of the round inside it: at most one commit in flight.
    vfs.set_deferred_io(false);
    for _ in 0..100 {
        if !poll(&mut pc).is_pending() {
            break;
        }
        vfs.complete_all_io();
        vfs.advance(1_000);
        idle(shard);
    }
    assert!(matches!(poll(&mut pc), Poll::Ready(Ok(_))), "seed {seed}");
    pass(&engine, &vfs, shard);
    let inflight = engine.shard_inflight(0).expect("published");
    assert!(
        inflight > 0.0 && inflight <= 1.0,
        "seed {seed}: one blocking client read {inflight} commits in flight"
    );
}
