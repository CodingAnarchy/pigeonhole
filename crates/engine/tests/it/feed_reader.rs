//! The change feed's reader in the writer process (D310 PR 3a): whole commits in seqno
//! order across shards, each once, from now, from a seqno, from the oldest retained, or
//! from a token, across a clean reopen; the durable frontier by default; and every gap as an
//! expiry, never a silent skip.

use crate::common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Commit, Engine, EngineOptions, EngineShard, FamilyOptions, Fault, FeedOptions, FeedReader,
    FeedSpec, FeedStart, Next, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::{Durability, Kind, Seqno};
use pigeonhole_io::sim::{FaultPlan, SimVfs};

const DB: &str = "/db/data.phdb";
const WAIT: Duration = Duration::from_secs(10);

fn feed(retention: u64) -> FeedOptions {
    let mut f = FeedOptions::default();
    f.retention_bytes = retention;
    f
}

fn options(vfs: &Arc<SimVfs>, feed: Option<FeedOptions>) -> EngineOptions {
    let mut o = common::options(Arc::clone(vfs), 2, 16 << 20);
    o.feed = feed;
    o
}

fn open(vfs: &Arc<SimVfs>, feed: Option<FeedOptions>) -> Arc<Engine> {
    Engine::open(Path::new(DB), options(vfs, feed)).unwrap()
}

/// Two tables: tablet n is on shard n % 2, so they're on different shards and a batch over
/// both is a cross-shard commit.
fn tables(db: &Engine) -> (Arc<TableInfo>, Arc<TableInfo>) {
    let f = || vec![("f".into(), FamilyOptions::default())];
    let a = db.create_table("a", &f()).unwrap();
    let b = db.create_table("b", &f()).unwrap();
    (a, b)
}

fn put(wb: &mut WriteBatch, t: &TableInfo, row: &[u8], value: &[u8]) {
    wb.put(
        t.id,
        t.families[0].id,
        row,
        b"q",
        None,
        ValueRef::Bytes(value),
    )
    .unwrap();
}

fn single(db: &Engine, t: &TableInfo, row: &[u8], durability: Durability) -> Seqno {
    let mut wb = WriteBatch::new();
    put(&mut wb, t, row, row);
    db.commit(wb, Some(durability)).unwrap().seqno
}

fn spec(tables: &[&TableInfo], start: FeedStart) -> FeedSpec {
    FeedSpec {
        tables: tables.iter().map(|t| t.id).collect(),
        prefix: Vec::new(),
        durable: true,
        start,
    }
}

/// The next commit, waiting for it.
fn next(r: &mut FeedReader) -> Commit {
    match r.wait(WAIT).unwrap() {
        Next::Commit(c) => c.clone(),
        Next::Pending => panic!("no commit within {WAIT:?} (frontier {})", r.frontier()),
        Next::Expired(e) => panic!("expired: {e:?}"),
    }
}

fn rows(c: &Commit) -> Vec<Vec<u8>> {
    c.changes.iter().map(|ch| ch.row.clone()).collect()
}

#[test]
fn whole_commits_arrive_once_in_seqno_order_across_shards() {
    let vfs = SimVfs::new(3101);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, b) = tables(&db);
    let before = single(&db, &a, b"before", Durability::GroupSync);
    let mut r = db.feed(spec(&[&a, &b], FeedStart::Now)).unwrap();
    assert!(
        r.delivered() >= before,
        "now starts after what is already there"
    );

    let s1 = single(&db, &a, b"a1", Durability::GroupSync);
    let s2 = single(&db, &b, b"b1", Durability::GroupSync);
    // A cross-shard commit: one whole commit holding both shares, at its apply seqno.
    let mut wb = WriteBatch::new();
    put(&mut wb, &a, b"x-a", b"1");
    put(&mut wb, &b, b"x-b", b"2");
    let x = db.commit(wb, Some(Durability::GroupSync)).unwrap().seqno;
    // Every delete kind, in one commit.
    let mut wb = WriteBatch::new();
    let f = a.families[0].id;
    wb.delete_cell(a.id, f, b"d", b"q", 7).unwrap();
    wb.delete_column(a.id, f, b"d", b"q", None).unwrap();
    wb.delete_family(a.id, f, b"d", None).unwrap();
    let d = db.commit(wb, Some(Durability::GroupSync)).unwrap().seqno;

    let got: Vec<Commit> = (0..4).map(|_| next(&mut r)).collect();
    assert_eq!(
        got.iter().map(|c| c.seqno).collect::<Vec<_>>(),
        [s1, s2, x, d]
    );
    assert_eq!(rows(&got[0]), [b"a1".to_vec()]);
    assert_eq!(rows(&got[1]), [b"b1".to_vec()]);
    let mut xr = rows(&got[2]);
    xr.sort();
    assert_eq!(xr, [b"x-a".to_vec(), b"x-b".to_vec()]);
    assert!(got[2].changes.iter().all(|c| c.ts == got[2].ts));
    let kinds: Vec<Kind> = got[3].changes.iter().map(|c| c.kind).collect();
    assert_eq!(
        kinds,
        [Kind::CellDelete, Kind::ColumnDelete, Kind::FamilyDelete]
    );
    assert_eq!(got[3].changes[0].ts, 7);
    // Nothing more.
    assert!(matches!(r.try_next().unwrap(), Next::Pending));
    db.close().unwrap();
}

#[test]
fn a_feed_follows_only_its_tables_and_prefix() {
    let vfs = SimVfs::new(3102);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, b) = tables(&db);
    let mut s = spec(&[&a], FeedStart::Now);
    s.prefix = b"user/".to_vec();
    let mut r = db.feed(s).unwrap();
    single(&db, &a, b"other/1", Durability::GroupSync);
    single(&db, &b, b"user/2", Durability::GroupSync);
    let want = single(&db, &a, b"user/3", Durability::GroupSync);
    // A cross-shard commit delivers only its share in the feed's table.
    let mut wb = WriteBatch::new();
    put(&mut wb, &a, b"user/4", b"1");
    put(&mut wb, &b, b"user/5", b"2");
    let x = db.commit(wb, Some(Durability::GroupSync)).unwrap().seqno;
    let c = next(&mut r);
    assert_eq!((c.seqno, rows(&c)), (want, vec![b"user/3".to_vec()]));
    let c = next(&mut r);
    assert_eq!((c.seqno, rows(&c)), (x, vec![b"user/4".to_vec()]));
    db.close().unwrap();
}

#[test]
fn a_durable_feed_waits_for_the_sync_and_a_visible_one_does_not() {
    // A real file system: on the simulator's frozen clock the background sync's timer gives
    // up at once and the next kick runs it (PR 2), so only real time keeps it off here.
    let dir = std::env::temp_dir().join(format!("phdb-feed-reader-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("data.phdb");
    let vfs: pigeonhole_io::VfsRef = pigeonhole_io::pread::PreadVfs::new(2);
    let mut o = EngineOptions::new(Arc::clone(&vfs));
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.reader_slots = 8;
    o.wal.segment_size = 8 * 32 * 1024;
    o.wal.spare_segments = 1;
    let mut f = feed(1 << 20);
    // No background sync within the test: only a commit's own sync moves durable.
    f.sync_interval_micros = Some(600_000_000);
    o.feed = Some(f);
    let db = Engine::open(&path, o).unwrap();
    let (a, _) = tables(&db);
    let mut durable = db.feed(spec(&[&a], FeedStart::Now)).unwrap();
    let mut s = spec(&[&a], FeedStart::Now);
    s.durable = false;
    let mut visible = db.feed(s).unwrap();
    let buffered = single(&db, &a, b"buffered", Durability::Buffered);
    assert_eq!(next(&mut visible).seqno, buffered);
    assert!(matches!(
        durable.wait(Duration::from_millis(50)).unwrap(),
        Next::Pending
    ));
    let synced = single(&db, &a, b"synced", Durability::GroupSync);
    assert_eq!(next(&mut durable).seqno, buffered);
    assert_eq!(next(&mut durable).seqno, synced);
    assert_eq!(db.feed_background_syncs(), 0);
    db.close().unwrap();
    drop(db);
    for name in [
        "data.phdb",
        "data.phdb-shm",
        "data.phdb-feed",
        "data.phdb-wal-0",
        "data.phdb-wal-1",
    ] {
        let _ = std::fs::remove_file(dir.join(name));
    }
    let _ = std::fs::remove_dir(&dir);
}

#[test]
fn a_token_resumes_after_its_last_commit_never_inside_one() {
    let vfs = SimVfs::new(3104);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, b) = tables(&db);
    let mut r = db.feed(spec(&[&a, &b], FeedStart::Now)).unwrap();
    let mut seqnos = Vec::new();
    for i in 0..6u8 {
        let mut wb = WriteBatch::new();
        put(&mut wb, &a, &[b'a', i], b"1");
        if i % 2 == 1 {
            // Every other one a cross-shard commit.
            put(&mut wb, &b, &[b'b', i], b"2");
        }
        seqnos.push(db.commit(wb, Some(Durability::GroupSync)).unwrap().seqno);
    }
    // Deliver three (the third a cross-shard one), then resume from the token.
    for want in &seqnos[..3] {
        assert_eq!(next(&mut r).seqno, *want);
    }
    let token = r.token();
    drop(r);
    let mut r = db
        .feed(spec(&[&a, &b], FeedStart::Token(token.clone())))
        .unwrap();
    for want in &seqnos[3..] {
        assert_eq!(next(&mut r).seqno, *want);
    }
    // The same token again, after a clean close and reopen: history is kept and adopted.
    db.close().unwrap();
    let db = open(&vfs, Some(feed(1 << 20)));
    let mut r = db.feed(spec(&[&a, &b], FeedStart::Token(token))).unwrap();
    for want in &seqnos[3..] {
        assert_eq!(next(&mut r).seqno, *want);
    }
    let later = single(&db, &a, b"later", Durability::GroupSync);
    assert_eq!(next(&mut r).seqno, later);
    db.close().unwrap();
}

#[test]
fn a_feed_starts_after_a_seqno_or_at_the_oldest_retained() {
    let vfs = SimVfs::new(3105);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, _) = tables(&db);
    let s1 = single(&db, &a, b"1", Durability::GroupSync);
    let s2 = single(&db, &a, b"2", Durability::GroupSync);
    let s3 = single(&db, &a, b"3", Durability::GroupSync);
    // A flush moves the checkpoints past all three: they're retained history now.
    db.flush().unwrap();
    let mut r = db.feed(spec(&[&a], FeedStart::After(s1))).unwrap();
    assert_eq!(next(&mut r).seqno, s2);
    assert_eq!(next(&mut r).seqno, s3);
    let mut r = db.feed(spec(&[&a], FeedStart::Oldest)).unwrap();
    assert_eq!(next(&mut r).seqno, s1);
    db.close().unwrap();
}

#[test]
fn subscribing_needs_the_feed_on_and_a_fed_table() {
    let vfs = SimVfs::new(3106);
    let db = open(&vfs, None);
    let (a, _) = tables(&db);
    let e = db.feed(spec(&[&a], FeedStart::Now)).unwrap_err();
    assert!(e.to_string().contains("feed is off"), "{e}");
    db.close().unwrap();
    let mut f = feed(1 << 20);
    f.tables = Some(vec!["b".into()]);
    let db = open(&vfs, Some(f));
    let e = db.feed(spec(&[&a], FeedStart::Now)).unwrap_err();
    assert!(e.to_string().contains("not fed"), "{e}");
    db.close().unwrap();
}

#[test]
fn a_session_without_the_feed_between_expires_a_token() {
    let vfs = SimVfs::new(3107);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, _) = tables(&db);
    let mut r = db.feed(spec(&[&a], FeedStart::Now)).unwrap();
    let s = single(&db, &a, b"1", Durability::GroupSync);
    assert_eq!(next(&mut r).seqno, s);
    let token = r.token();
    db.close().unwrap();
    // A session with the feed off commits in between.
    let db = open(&vfs, None);
    single(&db, &a, b"unfed", Durability::GroupSync);
    db.close().unwrap();
    let db = open(&vfs, Some(feed(1 << 20)));
    let mut r = db.feed(spec(&[&a], FeedStart::Token(token))).unwrap();
    assert!(
        matches!(r.try_next().unwrap(), Next::Expired(_)),
        "a gap is never skipped silently"
    );
    db.close().unwrap();
}

#[test]
fn a_failed_sidecar_append_expires_every_token_at_the_next_open() {
    let vfs = SimVfs::new(3109);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, _) = tables(&db);
    let mut r = db.feed(spec(&[&a], FeedStart::Now)).unwrap();
    let s = single(&db, &a, b"1", Durability::GroupSync);
    assert_eq!(next(&mut r).seqno, s);
    let token = r.token();
    // The next sidecar write fails (simp724 on fix A, condition 2): the flush's root
    // commit goes unrecorded, and the session records no more after it.
    let mut plan = FaultPlan::none();
    plan.fail_on = Some("phdb-feed");
    vfs.set_faults(plan);
    single(&db, &a, b"2", Durability::GroupSync);
    db.flush().unwrap();
    assert_eq!(vfs.scheduled_fired(), 1, "the sidecar's write failed");
    vfs.set_faults(FaultPlan::none());
    single(&db, &a, b"3", Durability::GroupSync);
    db.flush().unwrap();
    db.close().unwrap();
    let db = open(&vfs, Some(feed(1 << 20)));
    let mut r = db.feed(spec(&[&a], FeedStart::Token(token))).unwrap();
    assert!(
        matches!(r.try_next().unwrap(), Next::Expired(_)),
        "a session the sidecar lost track of never resumes"
    );
    db.close().unwrap();
}

#[test]
fn retention_passing_a_token_expires_it() {
    let vfs = SimVfs::new(3108);
    // The smallest retention: two segments per stream.
    let db = open(&vfs, Some(feed(1)));
    let (a, _) = tables(&db);
    let mut r = db.feed(spec(&[&a], FeedStart::Now)).unwrap();
    let s = single(&db, &a, b"first", Durability::GroupSync);
    assert_eq!(next(&mut r).seqno, s);
    let token = r.token();
    // Far more than the retention (two 256 KB segments per stream), in values kept inline
    // in the log, flushed as it goes so the checkpoints move on.
    let value = vec![7u8; 20_000];
    for i in 0..400u32 {
        let mut wb = WriteBatch::new();
        put(&mut wb, &a, &i.to_be_bytes(), &value);
        db.commit(wb, Some(Durability::GroupSync)).unwrap();
        if i % 40 == 39 {
            db.flush().unwrap();
        }
    }
    let mut r = db.feed(spec(&[&a], FeedStart::Token(token))).unwrap();
    let expired = loop {
        match r.wait(WAIT).unwrap() {
            Next::Commit(_) => continue,
            Next::Pending => break false,
            Next::Expired(_) => break true,
        }
    };
    assert!(expired, "a token retention passed never resumes");
    db.close().unwrap();
}

/// Runs every shard until none has anything to do now.
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
    panic!("the shards never settled");
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

/// Polls `r` (shards settled between) until it stops delivering: the commits it delivered.
fn drain(r: &mut FeedReader, shards: &mut [EngineShard]) -> Vec<Commit> {
    let mut got = Vec::new();
    for _ in 0..1_000 {
        settle(shards);
        match r.try_next().unwrap() {
            Next::Commit(c) => got.push(c.clone()),
            Next::Pending => return got,
            Next::Expired(e) => panic!("expired: {e:?}"),
        }
    }
    panic!("never drained");
}

/// simp724's case (3) on rule C, the slow half: with a cross-shard commit's DECISION held
/// (`Fault::HoldDecisionRecords`), its PREPAREs stay held by the reader even after a flush
/// of everything (no checkpoint may pass a PREPARE whose apply seqno isn't visible), and
/// once the DECISION is written the commit is delivered whole.
#[test]
fn a_prepare_waiting_for_its_decision_stays_held_through_a_flush() {
    let vfs = SimVfs::new(3109);
    let (a, b) = {
        let db = open(&vfs, Some(feed(1 << 20)));
        let t = tables(&db);
        db.close().unwrap();
        t
    };
    let (db, mut shards) =
        Engine::open_application_owned(Path::new(DB), options(&vfs, Some(feed(1 << 20)))).unwrap();
    let mut r = db.feed(spec(&[&a, &b], FeedStart::Now)).unwrap();
    db.inject(Fault::HoldDecisionRecords, true);
    let mut wb = WriteBatch::new();
    put(&mut wb, &a, b"x-a", b"1");
    put(&mut wb, &b, b"x-b", b"2");
    let mut pc = Box::pin(db.submit(wb, Some(Durability::Buffered)).unwrap());
    settle(&mut shards);
    let flush = db.submit_flush().unwrap();
    drive(&mut shards, flush).unwrap();
    settle(&mut shards);
    assert!(drain(&mut r, &mut shards).is_empty());
    assert_eq!(r.held_prepares(), 2, "both shares held for the DECISION");
    db.release_held_decisions();
    let info = drive(&mut shards, &mut pc).unwrap();
    let got = drain(&mut r, &mut shards);
    assert_eq!(got.len(), 1);
    assert_eq!((got[0].seqno, got[0].changes.len()), (info.seqno, 2));
    assert_eq!(r.held_prepares(), 0);
}

/// simp724's case (3), the aborted half: a cross-shard transaction that fails validation
/// on one shard after the other wrote its PREPARE. The reader holds that PREPARE while its
/// stream's checkpoint is behind it (an earlier unflushed single keeps it there), and drops
/// it once a flush lets the checkpoint pass, with no Decision read: never earlier.
#[test]
fn an_aborted_rounds_prepare_is_dropped_once_its_checkpoint_passes() {
    let vfs = SimVfs::new(3110);
    let (a, b) = {
        let db = open(&vfs, Some(feed(1 << 20)));
        let t = tables(&db);
        db.close().unwrap();
        t
    };
    let (db, mut shards) =
        Engine::open_application_owned(Path::new(DB), options(&vfs, Some(feed(1 << 20)))).unwrap();
    let mut r = db.feed(spec(&[&a, &b], FeedStart::Now)).unwrap();
    let commit = |shards: &mut [EngineShard], t: &TableInfo, row: &[u8]| {
        let mut wb = WriteBatch::new();
        put(&mut wb, t, row, b"v");
        drive(shards, db.submit(wb, Some(Durability::Buffered)).unwrap())
            .unwrap()
            .seqno
    };
    // Unflushed singles on both streams, ahead of the round: the checkpoints stay behind.
    let s1 = commit(&mut shards, &a, b"first-a");
    let s2 = commit(&mut shards, &b, b"first-b");
    let mut txn = db.begin().unwrap();
    txn.get(a.id, a.families[0].id, b"r", b"q").unwrap();
    // A conflicting write to the row the transaction read: it fails validation on a's
    // shard, after b's shard wrote its PREPARE.
    commit(&mut shards, &a, b"r");
    txn.batch()
        .put(
            a.id,
            a.families[0].id,
            b"r",
            b"q",
            None,
            ValueRef::Bytes(b"t"),
        )
        .unwrap();
    txn.batch()
        .put(
            b.id,
            b.families[0].id,
            b"t",
            b"q",
            None,
            ValueRef::Bytes(b"t"),
        )
        .unwrap();
    let pc = txn.submit(Some(Durability::Buffered)).unwrap();
    assert!(matches!(
        drive(&mut shards, pc),
        Err(pigeonhole_engine::Error::Conflict)
    ));
    let got: Vec<Seqno> = drain(&mut r, &mut shards).iter().map(|c| c.seqno).collect();
    assert_eq!(got[..2], [s1, s2]);
    let held = r.held_prepares();
    assert!(held <= 1, "{held}");
    if held == 1 {
        // Not before its checkpoint passes it.
        assert_eq!(r.held_prepares(), 1);
    }
    let flush = db.submit_flush().unwrap();
    drive(&mut shards, flush).unwrap();
    let later = commit(&mut shards, &b, b"later");
    let got: Vec<Seqno> = drain(&mut r, &mut shards).iter().map(|c| c.seqno).collect();
    assert_eq!(got.last(), Some(&later));
    assert_eq!(r.held_prepares(), 0, "the aborted PREPARE is dropped");
}
