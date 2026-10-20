//! #763: the flush-time bookkeeping is budgeted per batch. A coordinator counts and settles
//! at most `SETTLE_BUDGET` (16) seqnos of its received `SharesFlushed` reports per batch, and
//! a participant's report scan examines at most `REPORT_SCAN_BUDGET` (256) unreported shares
//! per batch; the rest follow on the batches a kick brings. After a pack → regrow the shards
//! flush out of phase, and these turns, 3-5 ms each, then hit one shard after another all
//! cycle long; bounded, a staggered flush schedule costs what a clustered one does.
//!
//! Application-owned with every shard driven by the test, so each run is deterministic.

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{Engine, EngineShard, FamilyOptions, TableInfo, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";
/// Large enough that nothing flushes until the test asks.
const BUDGET: u64 = 64 << 20;
const SETTLE_BUDGET: u64 = 16;
const REPORT_SCAN_BUDGET: u64 = 256;

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

/// Runs every shard until all of them stay idle: a shard's batch can send a peer polled
/// earlier in the same round a message (a report chunk, a settle), so one quiet round is not
/// enough; three in a row are.
fn settle(shards: &mut [EngineShard]) {
    let mut quiet = 0;
    for _ in 0..1_000_000 {
        let mut more = false;
        for s in shards.iter_mut() {
            more |= s.run_once(u64::MAX);
        }
        quiet = if more { 0 } else { quiet + 1 };
        if quiet == 3 {
            return;
        }
    }
    panic!("the shards never went idle");
}

/// A store of three shards with a table on each (tablet n on shard n % 3): `c0` on shard 0,
/// `p` on shard 2. Commits writing `c0` then `p` are coordinated by shard 0 with shard 2 as
/// the participant.
type Opened = (
    Arc<SimVfs>,
    Arc<Engine>,
    Vec<EngineShard>,
    Arc<TableInfo>,
    Arc<TableInfo>,
);

fn open(seed: u64) -> Opened {
    let vfs = SimVfs::new(seed);
    let db = Engine::open(Path::new(DB), common::options(Arc::clone(&vfs), 3, BUDGET)).unwrap();
    let family = || vec![("f".into(), FamilyOptions::default())];
    let _c1 = db.create_table("c1", &family()).unwrap();
    let p = db.create_table("p", &family()).unwrap();
    let c0 = db.create_table("c0", &family()).unwrap();
    db.close().unwrap();
    drop(db);
    let (db, shards) =
        Engine::open_application_owned(Path::new(DB), common::options(Arc::clone(&vfs), 3, BUDGET))
            .unwrap();
    (vfs, db, shards, c0, p)
}

fn commit_cross(db: &Engine, shards: &mut [EngineShard], c0: &TableInfo, p: &TableInfo, n: u32) {
    commit_cross_from(db, shards, c0, p, 0, n);
}

/// `n` cross-shard commits on rows `from..from + n`.
fn commit_cross_from(
    db: &Engine,
    shards: &mut [EngineShard],
    c0: &TableInfo,
    p: &TableInfo,
    from: u32,
    n: u32,
) {
    for i in from..from + n {
        let row = i.to_be_bytes();
        let mut wb = WriteBatch::new();
        wb.put(
            c0.id,
            c0.families[0].id,
            &row,
            b"q",
            None,
            ValueRef::Bytes(&row),
        )
        .unwrap();
        wb.put(
            p.id,
            p.families[0].id,
            &row,
            b"q",
            None,
            ValueRef::Bytes(&row),
        )
        .unwrap();
        drive(shards, db.submit(wb, Some(Durability::Buffered)).unwrap()).unwrap();
    }
}

#[test]
fn a_report_of_a_thousand_shares_settles_over_several_batches() {
    const COMMITS: u32 = 1000;
    let (_vfs, db, mut shards, c0, p) = open(7631);
    commit_cross(&db, &mut shards, &c0, &p, COMMITS);
    assert_eq!(db.logged_records(2), u64::from(COMMITS));

    // Shard 2's flush reports its shares to shard 0, which settles them 16 per batch and tells
    // shard 2 once the run ends (1000 settles are under one `SETTLE_SEND_EVERY`).
    let flush = db.submit_flush().unwrap();
    drive(&mut shards, flush).unwrap();
    settle(&mut shards);

    let logged: Vec<_> = (0..3).map(|s| db.logged_records(s)).collect();
    assert_eq!(logged, vec![0; 3], "records the checkpoints never passed");
    let largest = db.max_settled_per_batch(0);
    assert!(
        largest <= SETTLE_BUDGET,
        "a batch settled {largest} reports, more than the budget of {SETTLE_BUDGET}"
    );
    assert_eq!(
        largest, SETTLE_BUDGET,
        "no batch reached the budget: nothing tested"
    );
    // Every commit still settled once, in a few messages (D254).
    let (msgs, seqnos) = db.share_reports(2);
    assert_eq!(
        seqnos,
        u64::from(COMMITS),
        "{msgs} messages carried {seqnos} seqnos"
    );
    // The sliced run told shard 2 once (D254's one message per run), not once per batch,
    // which would be 1000 / 16 = 63.
    assert!(
        msgs <= 2,
        "{msgs} CommitsSettled messages for one report run of {seqnos} seqnos"
    );
    db.close().unwrap();
}

#[test]
fn a_flush_of_thousands_of_shares_reports_them_in_chunks() {
    const COMMITS: u32 = 2500;
    let (_vfs, db, mut shards, c0, p) = open(7632);
    commit_cross(&db, &mut shards, &c0, &p, COMMITS);

    // Shard 2's scan examines 256 shares per batch, ten chunks, and reports to shard 0 once
    // the pass ends: one message per coordinator per pass (D254), not one per chunk.
    let flush = db.submit_flush().unwrap();
    drive(&mut shards, flush).unwrap();
    settle(&mut shards);

    let logged: Vec<_> = (0..3).map(|s| db.logged_records(s)).collect();
    assert_eq!(logged, vec![0; 3], "records the checkpoints never passed");
    let largest = db.max_report_scan_per_batch(2);
    assert!(
        largest <= REPORT_SCAN_BUDGET,
        "a batch scanned {largest} shares, more than the budget of {REPORT_SCAN_BUDGET}"
    );
    assert_eq!(
        largest, REPORT_SCAN_BUDGET,
        "no batch reached the budget: nothing tested"
    );
    let (msgs, seqnos) = db.share_reports(0);
    assert_eq!(
        seqnos,
        u64::from(COMMITS),
        "{msgs} messages carried {seqnos} seqnos"
    );
    // One message per coordinator per pass (D254), not one per chunk, which would be 10.
    assert!(
        msgs <= 2,
        "{msgs} report messages for {seqnos} shares scanned in chunks of {REPORT_SCAN_BUDGET}: one per pass"
    );
    db.close().unwrap();
}

/// #763's review: a flush that lands while a pass is in progress makes shares *before* the
/// cursor reportable (the pass's swap-removes brought them forward, and they were not in SSTs
/// when the pass examined them). On a store that then goes quiet, nothing else looks at them,
/// so the pass must restart when a slot moved mid-pass. Here the first flush's pass runs 235
/// chunks; more commits land during it, a second flush covers them, and the store goes quiet.
#[test]
fn a_flush_landing_mid_pass_reports_the_shares_before_the_cursor() {
    const COMMITS: u32 = 60_000;
    const LATE: u32 = 4;
    let (_vfs, db, mut shards, c0, p) = open(7633);
    commit_cross(&db, &mut shards, &c0, &p, COMMITS);

    // Flush A: drive it only until shard 2's pass has run its first chunk.
    let mut flush_a = db.submit_flush().unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let mut chunked = false;
    for _ in 0..1_000_000 {
        for s in shards.iter_mut() {
            s.run_once(u64::MAX);
        }
        if db.max_report_scan_per_batch(2) >= REPORT_SCAN_BUDGET {
            chunked = true;
            break;
        }
        assert!(
            Pin::new(&mut flush_a).poll(&mut cx).is_pending(),
            "the flush resolved before the pass ran a chunk"
        );
    }
    assert!(chunked, "the pass never ran a chunk");

    // Late commits, driven to completion while the pass continues (235 chunks against the
    // few batches these take): their shares append, and the pass's swap-removes bring them
    // before the cursor, not in SSTs yet. Flush B then puts them in SSTs, still mid-pass.
    commit_cross_from(&db, &mut shards, &c0, &p, COMMITS, LATE);
    let (_, reported) = db.share_reports(0);
    assert!(
        reported < u64::from(COMMITS),
        "the first pass had reported everything ({reported}) before the second flush: the test's timing premise failed"
    );
    let flush_b = db.submit_flush().unwrap();
    drive(&mut shards, flush_b).unwrap();
    drive(&mut shards, flush_a).unwrap();
    settle(&mut shards);

    // Every share reported and every PREPARE and COMMIT checkpointed, with no further
    // manifest commit to prompt another pass.
    let logged: Vec<_> = (0..3).map(|s| db.logged_records(s)).collect();
    assert_eq!(logged, vec![0; 3], "records the checkpoints never passed");
    // The restart sent what the first pass had found before flush B landed (the chunks before
    // the restart), then the new pass sent the rest: two messages, not one held to the end.
    let (msgs, seqnos) = db.share_reports(0);
    assert_eq!(seqnos, u64::from(COMMITS + LATE));
    assert!(
        msgs >= 2,
        "{msgs} report messages: the restart did not send the reports the pass held"
    );
    db.close().unwrap();
}
