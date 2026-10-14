//! D254: a flush that puts many cross-shard shares in SSTs tells each coordinator in one
//! `SharesFlushed` message, and a coordinator tells each participant the commits it settled
//! in one `CommitsSettled`, not one message per share or commit (#716: ~100K messages per
//! flush install took ~20 ms on the shard thread). The reports still settle every commit,
//! and a power loss after them keeps every write.
//!
//! Application-owned with every shard driven by the test, so each run is deterministic.

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{Engine, EngineOptions, EngineShard, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::{Durability, FamilyId};
use pigeonhole_io::sim::{CrashKind, SimVfs};

const DB: &str = "/db/data.phdb";
/// Large enough that nothing flushes until the test asks.
const BUDGET: u64 = 64 << 20;
const COMMITS: u32 = 600;

fn options(vfs: &Arc<SimVfs>) -> EngineOptions {
    common::options(Arc::clone(vfs), 3, BUDGET)
}

/// Runs every shard until `pending` resolves.
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

/// Runs every shard until all of them go idle.
fn settle(shards: &mut [EngineShard]) {
    for _ in 0..100_000 {
        let mut more = false;
        for s in shards.iter_mut() {
            more |= s.run_once(u64::MAX);
        }
        if !more {
            return;
        }
    }
    panic!("the shards never went idle");
}

#[test]
fn a_flush_reports_its_shares_in_one_message_per_coordinator() {
    let vfs = SimVfs::new(254);
    let db = Engine::open(Path::new(DB), options(&vfs)).unwrap();
    let family = || vec![("f".into(), FamilyOptions::default())];
    // Tablet n is on shard n % 3: c1 on shard 1, p on shard 2, c0 on shard 0.
    let c1 = db.create_table("c1", &family()).unwrap();
    let p = db.create_table("p", &family()).unwrap();
    let c0 = db.create_table("c0", &family()).unwrap();
    db.close().unwrap();
    drop(db);

    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), options(&vfs)).unwrap();
    let f = |t: &pigeonhole_engine::TableInfo| t.families[0].id;
    let put = |wb: &mut WriteBatch, t: &pigeonhole_engine::TableInfo, row: &[u8]| {
        wb.put(t.id, f(t), row, b"q", None, ValueRef::Bytes(row))
            .unwrap();
    };
    // Each commit writes its coordinator's table first (the first row's shard coordinates),
    // then a row on shard 2: half are coordinated by shard 0, half by shard 1.
    for i in 0..COMMITS {
        let row = i.to_be_bytes();
        let mut wb = WriteBatch::new();
        put(&mut wb, if i % 2 == 0 { &c0 } else { &c1 }, &row);
        put(&mut wb, &p, &row);
        drive(
            &mut shards,
            db.submit(wb, Some(Durability::Buffered)).unwrap(),
        )
        .unwrap();
    }
    let before: Vec<_> = (0..3).map(|s| db.share_reports(s)).collect();
    assert_eq!(
        before,
        vec![(0, 0); 3],
        "a share reported before anything flushed"
    );

    // Shard 2 logged a PREPARE per commit, which its checkpoint can pass only once the
    // commit's coordinator reports it settled.
    assert_eq!(db.logged_records(2), u64::from(COMMITS));

    let flush = db.submit_flush().unwrap();
    drive(&mut shards, flush).unwrap();
    settle(&mut shards);

    let after: Vec<_> = (0..3).map(|s| db.share_reports(s)).collect();
    // Each coordinator heard of its 300 shares on shard 2, and shard 2 that each of its 600
    // commits settled: every share and commit was reported once.
    let seqnos: u64 = after.iter().map(|(_, s)| s).sum();
    assert_eq!(
        seqnos,
        2 * u64::from(COMMITS),
        "reports per shard: {after:?}"
    );
    assert_eq!(after[0].1, u64::from(COMMITS / 2), "{after:?}");
    assert_eq!(after[1].1, u64::from(COMMITS / 2), "{after:?}");
    assert_eq!(after[2].1, u64::from(COMMITS), "{after:?}");
    // One flush of shard 2 reports to each coordinator once; each coordinator settles its
    // commits in one or two batches (on that report, or on its own flush). One message per
    // share or commit was 1200.
    let msgs: u64 = after.iter().map(|(m, _)| m).sum();
    assert!(
        msgs <= 6,
        "{msgs} report messages for {seqnos} reports, per shard (messages, seqnos): {after:?}"
    );

    // Every commit settled: the participant's checkpoint passed its PREPAREs, and each
    // coordinator's passed its PREPAREs and COMMITs.
    let logged: Vec<_> = (0..3).map(|s| db.logged_records(s)).collect();
    assert_eq!(
        logged,
        vec![0; 3],
        "records the checkpoints have not passed"
    );

    // The settled commits survive a power loss whole.
    vfs.crash(CrashKind::Power);
    drop(shards);
    drop(db);
    let db = Engine::open(Path::new(DB), options(&vfs)).unwrap();
    let get = |t: &pigeonhole_engine::TableInfo, row: &[u8]| {
        db.get_latest(t.id, FamilyId(f(t).0), row, b"q")
            .unwrap()
            .map(|c| common::value_bytes(c.value()))
    };
    for i in 0..COMMITS {
        let row = i.to_be_bytes();
        let c = if i % 2 == 0 { &c0 } else { &c1 };
        assert_eq!(get(c, &row).as_deref(), Some(&row[..]), "commit {i}");
        assert_eq!(get(&p, &row).as_deref(), Some(&row[..]), "commit {i}");
    }
    db.close().unwrap();
}
