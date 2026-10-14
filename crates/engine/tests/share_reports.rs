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

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, TableInfo, ValueRef, WriteBatch,
};
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

/// The engine's `REPORT_SLICE`: the most share reports a coordinator counts in one turn.
const SLICE: u64 = 4096;

/// Two shards: `n` cross-shard commits that shard 0 coordinates, each with a share on shard
/// 1. Returns the tables (coordinator's, participant's).
fn two_shard_commits(
    vfs: &Arc<SimVfs>,
    n: u32,
) -> (
    Arc<Engine>,
    Vec<EngineShard>,
    Arc<TableInfo>,
    Arc<TableInfo>,
) {
    let o = || common::options(Arc::clone(vfs), 2, BUDGET);
    let db = Engine::open(Path::new(DB), o()).unwrap();
    let family = || vec![("f".into(), FamilyOptions::default())];
    // Tablet n is on shard n % 2: p on shard 1, c on shard 0.
    let p = db.create_table("p", &family()).unwrap();
    let c = db.create_table("c", &family()).unwrap();
    db.close().unwrap();
    drop(db);
    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), o()).unwrap();
    for i in 0..n {
        let row = i.to_be_bytes();
        let mut wb = WriteBatch::new();
        for t in [&c, &p] {
            wb.put(
                t.id,
                t.families[0].id,
                &row,
                b"q",
                None,
                ValueRef::Bytes(&row),
            )
            .unwrap();
        }
        drive(
            &mut shards,
            db.submit(wb, Some(Durability::Buffered)).unwrap(),
        )
        .unwrap();
    }
    (db, shards, c, p)
}

/// Every row of both tables reads back after a reopen.
fn check_rows(
    vfs: &Arc<SimVfs>,
    n: u32,
    c: &pigeonhole_engine::TableInfo,
    p: &pigeonhole_engine::TableInfo,
) {
    let db = Engine::open(Path::new(DB), common::options(Arc::clone(vfs), 2, BUDGET)).unwrap();
    for i in 0..n {
        let row = i.to_be_bytes();
        for t in [c, p] {
            let got = db
                .get_latest(t.id, t.families[0].id, &row, b"q")
                .unwrap()
                .map(|v| common::value_bytes(v.value()));
            assert_eq!(got.as_deref(), Some(&row[..]), "row {i} of {}", t.name);
        }
    }
    db.close().unwrap();
}

#[test]
fn a_large_report_is_counted_a_slice_per_turn() {
    // One flush of shard 1 reports 100K shares to shard 0 in one message (#716's ~100K per
    // flush): the coordinator counts them at most `SLICE` a turn, not in one 50 ms turn.
    const N: u32 = 100_000;
    let vfs = SimVfs::new(2541);
    let (db, mut shards, ..) = two_shard_commits(&vfs, N);
    let flush = db.submit_flush().unwrap();
    drive(&mut shards, flush).unwrap();
    settle(&mut shards);
    let (msgs, seqnos) = db.share_reports(0);
    assert_eq!(seqnos, u64::from(N), "shard 0 counted every report");
    assert!(msgs <= 2, "{msgs} SharesFlushed messages for one flush");
    assert_eq!(
        db.report_slice_max(0),
        SLICE,
        "the most reports shard 0 counted in one turn"
    );
    let logged: Vec<_> = (0..2).map(|s| db.logged_records(s)).collect();
    assert_eq!(
        logged,
        vec![0; 2],
        "records the checkpoints have not passed"
    );
}

/// Two shards with `PARTIAL` commits, flushed: shard 1's report reaches shard 0, which
/// has counted only some of it when this returns.
fn partly_counted(
    vfs: &Arc<SimVfs>,
) -> (
    Arc<Engine>,
    Vec<EngineShard>,
    Arc<TableInfo>,
    Arc<TableInfo>,
) {
    let (db, mut shards, c, p) = two_shard_commits(vfs, PARTIAL);
    let _flush = db.submit_flush().unwrap();
    // Both shards run (shard 1's freeze waits for shard 0's visibility), a turn each,
    // until shard 0 has the report and has counted some of it.
    for _ in 0..100_000 {
        shards[1].run_once(u64::MAX);
        shards[0].run_once(u64::MAX);
        if db.share_reports(0).1 > 0 {
            break;
        }
    }
    let (msgs, counted) = db.share_reports(0);
    assert_eq!(msgs, 1, "shard 1's one report");
    assert!(
        counted > 0 && counted < u64::from(PARTIAL),
        "shard 0 counted {counted} of {PARTIAL} reports"
    );
    assert_eq!(
        db.logged_records(1),
        u64::from(PARTIAL),
        "shard 1's PREPAREs wait"
    );
    (db, shards, c, p)
}

/// Eight slices: more than one turn counts.
const PARTIAL: u32 = 8 * 4096;

#[test]
fn a_close_with_reports_still_queued_counts_them_and_is_clean() {
    let vfs = SimVfs::new(2542);
    let (db, mut shards, c, p) = partly_counted(&vfs);
    db.close().unwrap();
    let mut finished = false;
    for _ in 0..100_000 {
        let mut more = false;
        for s in shards.iter_mut() {
            more |= s.run_once(u64::MAX);
        }
        if !more && db.close_finished() {
            finished = true;
            break;
        }
    }
    assert!(finished, "the close never finished (a hang)");
    assert!(
        matches!(shards[0].closed(), Some(Ok(()))),
        "the close was not clean: {:?}",
        shards[0].closed()
    );
    assert_eq!(
        db.share_reports(0).1,
        u64::from(PARTIAL),
        "every report counted"
    );
    let logged: Vec<_> = (0..2).map(|s| db.logged_records(s)).collect();
    assert_eq!(
        logged,
        vec![0; 2],
        "records the checkpoints have not passed"
    );
    drop(shards);
    drop(db);
    check_rows(&vfs, PARTIAL, &c, &p);
}

#[test]
fn a_crash_with_reports_still_queued_replays_every_commit() {
    let vfs = SimVfs::new(2543);
    let (db, shards, c, p) = partly_counted(&vfs);
    vfs.crash(CrashKind::Power);
    drop(shards);
    drop(db);
    // Replay re-reports the flushed shares at start: the queue lost in the crash is not
    // needed. Then a clean close passes every record.
    check_rows(&vfs, PARTIAL, &c, &p);
    check_rows(&vfs, PARTIAL, &c, &p);
}
