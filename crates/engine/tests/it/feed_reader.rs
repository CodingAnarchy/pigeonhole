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
        Next::Catalog(e) => panic!("a catalog event in a per-table feed: {e:?}"),
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
            Next::Commit(_) | Next::Catalog(_) => continue,
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
            Next::Catalog(e) => panic!("a catalog event in a per-table feed: {e:?}"),
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
    aborted_round(false);
}

/// The aborted round, then a flush; with `quiet`, no commit after it.
fn aborted_round(quiet: bool) {
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
    if quiet {
        assert_eq!(
            held, 1,
            "the round's PREPARE is held (the case this checks)"
        );
    }
    if held == 1 {
        // Not before its checkpoint passes it.
        assert_eq!(r.held_prepares(), 1);
    }
    let flush = db.submit_flush().unwrap();
    drive(&mut shards, flush).unwrap();
    if quiet {
        // No commit after the flush (feed1248's P1 on #1449): the checkpoint moved with the
        // frontier still, and the PREPARE goes all the same.
        assert!(drain(&mut r, &mut shards).is_empty());
        assert_eq!(r.held_prepares(), 0, "the aborted PREPARE is dropped");
        return;
    }
    let later = commit(&mut shards, &b, b"later");
    let got: Vec<Seqno> = drain(&mut r, &mut shards).iter().map(|c| c.seqno).collect();
    assert_eq!(got.last(), Some(&later));
    assert_eq!(r.held_prepares(), 0, "the aborted PREPARE is dropped");
}

/// [`an_aborted_rounds_prepare_is_dropped_once_its_checkpoint_passes`] with no commit after
/// the flush: a quiet writer's checkpoint still drops the PREPARE (#1449).
#[test]
fn an_aborted_rounds_prepare_is_dropped_on_a_quiet_writer() {
    aborted_round(true);
}

/// #1448: a poll reads the streams only when the frontier moved since they were last read
/// to their ends (every commit moves it). An idle feed reads nothing more, also after a
/// commit to a table it doesn't follow, and still delivers the next commit it does follow.
#[test]
fn an_idle_feed_reads_the_streams_once_per_frontier() {
    let vfs = SimVfs::new(3148);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, b) = tables(&db);
    let c = db
        .create_table("c", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let mut r = db.feed(spec(&[&a, &b], FeedStart::Now)).unwrap();
    let s1 = single(&db, &a, b"a1", Durability::GroupSync);
    assert_eq!(next(&mut r).seqno, s1);
    let idle = |r: &mut FeedReader| {
        let reads = r.stream_reads();
        for _ in 0..100 {
            assert!(matches!(r.try_next().unwrap(), Next::Pending));
        }
        assert_eq!(r.stream_reads(), reads, "an idle poll read the streams");
    };
    idle(&mut r);

    // A commit outside the feed moves the frontier: the streams are read once more, and
    // nothing is delivered.
    let other = single(&db, &c, b"c1", Durability::GroupSync);
    let reads = r.stream_reads();
    for _ in 0..1_000 {
        assert!(matches!(r.try_next().unwrap(), Next::Pending));
        if r.frontier() >= other {
            break;
        }
    }
    assert!(r.frontier() >= other, "the frontier never reached {other}");
    assert!(
        r.stream_reads() > reads,
        "the moved frontier didn't read the streams"
    );
    idle(&mut r);

    let s2 = single(&db, &b, b"b1", Durability::GroupSync);
    assert_eq!(next(&mut r).seqno, s2);
    idle(&mut r);
    db.close().unwrap();
}

/// Every commit a feed delivers until nothing more comes within a short wait.
fn drain_seqnos(r: &mut FeedReader) -> Vec<Seqno> {
    let mut got = Vec::new();
    loop {
        match r.wait(Duration::from_millis(200)).unwrap() {
            Next::Commit(c) => got.push(c.seqno),
            Next::Pending => return got,
            Next::Expired(e) => panic!("expired: {e:?}"),
            Next::Catalog(e) => panic!("a catalog event in a per-table feed: {e:?}"),
        }
    }
}

/// #1465: `Oldest` after reopens starts at the oldest earlier session still kept, in every
/// stream (simp724's cases). Session 1 opens with a cross-shard commit, then commits on both
/// shards; session 2 starts with a cross-shard commit too, so stream 1's only record in it
/// is a PREPARE, then commits only on shard 0; session 3 subscribes from the oldest. Every
/// commit arrives once, in order, from the first. Mutation-checked: a bound of the first id
/// (not one less) loses the first commit, and one taken from singles only loses the
/// session's opening cross-shard commit.
#[test]
fn oldest_after_reopens_starts_at_the_oldest_session_kept() {
    let vfs = SimVfs::new(3165);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, b) = tables(&db);
    db.close().unwrap();
    // The tables' creation is the first session: session 1 is this one, opening with a
    // cross-shard commit.
    let db = open(&vfs, Some(feed(1 << 20)));
    let cross = |db: &Engine, row: &[u8]| {
        let mut wb = WriteBatch::new();
        put(&mut wb, &a, row, row);
        put(&mut wb, &b, row, row);
        db.commit(wb, Some(Durability::GroupSync)).unwrap().seqno
    };
    let mut made = vec![
        cross(&db, b"x1"),
        single(&db, &a, b"a1", Durability::GroupSync),
        single(&db, &b, b"b1", Durability::GroupSync),
    ];
    db.close().unwrap();

    let db = open(&vfs, Some(feed(1 << 20)));
    made.push(cross(&db, b"x2"));
    made.push(single(&db, &a, b"a2", Durability::GroupSync));
    made.push(single(&db, &a, b"a2'", Durability::Buffered));
    db.close().unwrap();

    let db = open(&vfs, Some(feed(1 << 20)));
    let mut r = db.feed(spec(&[&a, &b], FeedStart::Oldest)).unwrap();
    assert_eq!(
        drain_seqnos(&mut r),
        made,
        "every session's commits, from the first"
    );
    // And a commit of this session after them.
    let now = single(&db, &b, b"b3", Durability::GroupSync);
    assert_eq!(next(&mut r).seqno, now);
    drop(r);
    db.close().unwrap();

    // Past a feed-off open the earlier sessions aren't this history: `Oldest` starts at
    // this open, as before, and doesn't expire.
    open(&vfs, None).close().unwrap();
    let db = open(&vfs, Some(feed(1 << 20)));
    let mut r = db.feed(spec(&[&a, &b], FeedStart::Oldest)).unwrap();
    assert!(
        drain_seqnos(&mut r).is_empty(),
        "nothing from before the unfed open"
    );
    let later = single(&db, &a, b"a4", Durability::GroupSync);
    assert_eq!(next(&mut r).seqno, later);
    drop(r);
    db.close().unwrap();
}

/// The sidecar's entries, as a writer left them.
fn sidecar_entries(vfs: &Arc<SimVfs>) -> Vec<pigeonhole_format::feed::FeedEntry> {
    use pigeonhole_format::feed::{FEED_SUFFIX, FeedHeader, decode_entries};
    let path = format!("{DB}{FEED_SUFFIX}");
    use pigeonhole_io::Vfs;
    let file = vfs
        .open(Path::new(&path), pigeonhole_io::OpenOptions::read())
        .unwrap();
    let mut bytes = vec![0u8; file.len().unwrap() as usize];
    file.read_at(&mut bytes, 0).unwrap();
    decode_entries(&bytes[FeedHeader::LEN..]).0
}

/// D315 PR 2: a catalog change with the change feed on appends a `Catalog` sidecar entry,
/// its edits in order and its `seqno_ceiling` at most the first commit to the new table,
/// just before its root commit's Root entry; and its hold on the frontier is released, so
/// a feed of every table delivers that commit. A drop records its `DropTable` the same way.
#[test]
fn a_catalog_change_appends_its_entry_before_its_root_and_releases_its_hold() {
    use pigeonhole_format::feed::FeedEntry;
    use pigeonhole_format::manifest::Edit;
    let vfs = SimVfs::new(3151);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, _b) = tables(&db);
    let mut all = db.feed(spec(&[], FeedStart::Now)).unwrap();
    let c = db
        .create_table("c", &[("g".into(), FamilyOptions::default())])
        .unwrap();
    let s = single(&db, &c, b"c1", Durability::GroupSync);
    match all.wait(WAIT).unwrap() {
        Next::Catalog(ev) => assert!(ev.seqno_ceiling <= s, "{ev:?}"),
        _ => panic!("the create first"),
    }
    assert_eq!(next(&mut all).seqno, s, "the hold was released");

    let entries = sidecar_entries(&vfs);
    let at = entries
        .iter()
        .rposition(|e| matches!(e, FeedEntry::Catalog { edits, .. } if edits.iter().any(|e| matches!(e, Edit::CreateTable { name, .. } if name == "c"))))
        .expect("the create's Catalog entry");
    let FeedEntry::Catalog {
        session,
        sequence,
        seqno_ceiling,
        edits,
        ..
    } = &entries[at]
    else {
        unreachable!()
    };
    assert!(
        *seqno_ceiling <= s,
        "ceiling {seqno_ceiling} after the table's first commit {s}"
    );
    assert!(
        matches!(edits[..], [Edit::CreateTable { table, .. }, Edit::PutFamily { table: f, .. }] if table == c.id && f == c.id),
        "{edits:?}"
    );
    assert!(
        matches!(&entries[at + 1], FeedEntry::Root { session: r, sequence: q } if r == session && q == sequence),
        "the Root entry right after it: {:?}",
        entries.get(at + 1)
    );

    db.drop_table(a.id).unwrap();
    let entries = sidecar_entries(&vfs);
    let drop_at = entries
        .iter()
        .rposition(|e| matches!(e, FeedEntry::Catalog { edits, .. } if edits.iter().any(|e| matches!(e, Edit::DropTable { table } if *table == a.id))))
        .expect("the drop's Catalog entry");
    assert!(matches!(entries[drop_at + 1], FeedEntry::Root { .. }));
    // The frontier still moves past the drop.
    let later = single(&db, &c, b"c2", Durability::GroupSync);
    match all.wait(WAIT).unwrap() {
        Next::Catalog(ev) => {
            assert!(matches!(ev.edits[..], [Edit::DropTable { table }] if table == a.id))
        }
        _ => panic!("the drop first"),
    }
    assert_eq!(next(&mut all).seqno, later);
    drop(all);
    db.close().unwrap();
}

/// simp724's B1 on #1493 (D315): a catalog change whose family needs a new incompat
/// feature bit (the file's first aggregate family) makes the pager commit the bit's roots
/// first; its `Catalog` entry still belongs to the root commit that holds its edits, the
/// last before `create_table` returns, not to one of the bit's, so the bit's Root entries
/// come before it. Mutation-checked: staging before `flag_features` puts it on the bit's
/// root.
#[test]
fn a_catalog_entry_is_on_its_own_root_when_a_feature_bit_commits_first() {
    use pigeonhole_format::feed::FeedEntry;
    use pigeonhole_format::manifest::{Edit, FamilyKind};
    let vfs = SimVfs::new(3152);
    let db = open(&vfs, Some(feed(1 << 20)));
    let agg = FamilyOptions::default()
        .merge_operator("pigeonhole.i64_min")
        .kind(FamilyKind::Min);
    let before = sidecar_entries(&vfs).len();
    let t = db.create_table("agg", &[("m".into(), agg)]).unwrap();
    let entries = sidecar_entries(&vfs);
    // simp724's nit: the bit's Root entries come first, then the create's Catalog entry.
    let new = &entries[before..];
    let at = new
        .iter()
        .position(|e| matches!(e, FeedEntry::Catalog { .. }))
        .expect("the create's Catalog entry");
    assert!(
        new[..at]
            .iter()
            .any(|e| matches!(e, FeedEntry::Root { .. })),
        "the feature bit's Root entries before the create's: {new:?}"
    );
    let catalog = entries
        .iter()
        .filter_map(|e| match e {
            FeedEntry::Catalog {
                sequence, edits, ..
            } if edits
                .iter()
                .any(|e| matches!(e, Edit::CreateTable { table, .. } if *table == t.id)) =>
            {
                Some(*sequence)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let last_root = entries
        .iter()
        .rev()
        .find_map(|e| match e {
            FeedEntry::Root { sequence, .. } => Some(*sequence),
            _ => None,
        })
        .expect("a Root entry");
    assert_eq!(
        catalog,
        [last_root],
        "the create's root, not a feature bit's: {entries:?}"
    );
    db.close().unwrap();
}

/// simp724 on #1493 (D315 2b): once the session's sidecar appends stop, no catalog change
/// can get its `Catalog` entry, so the catalog hold freezes at the first change's ceiling
/// for the rest of the session. A feed of every table in the writer's process expires; a
/// table's feed carries on, since only a feed of every table's frontier reads the hold
/// (simp724's F2 on #1515); writes go on. Two ways in: the change's own `Catalog` append
/// fails, or an earlier plain Root append did. The freeze itself, which a feed of every
/// table in a reader process waits at, is `feed_reader_process`'s.
#[test]
fn a_stopped_sidecar_freezes_the_catalog_hold() {
    for catalog_append_fails in [true, false] {
        let vfs = SimVfs::new(3161);
        let db = open(&vfs, Some(feed(1 << 20)));
        let (a, _b) = tables(&db);
        let mut all = db.feed(spec(&[], FeedStart::Now)).unwrap();
        let mut one = db.feed(spec(&[&a], FeedStart::Now)).unwrap();
        let below = single(&db, &a, b"below", Durability::GroupSync);
        assert_eq!(next(&mut one).seqno, below);
        assert_eq!(next(&mut all).seqno, below);
        let mut plan = FaultPlan::none();
        plan.fail_on = Some("phdb-feed");
        vfs.set_faults(plan);
        if !catalog_append_fails {
            // A flush's plain Root entry fails first.
            single(&db, &a, b"flushed", Durability::GroupSync);
            db.flush().unwrap();
            assert_eq!(next(&mut one).seqno, below + 1);
        }
        db.create_table("c", &[("g".into(), FamilyOptions::default())])
            .unwrap();
        assert_eq!(
            vfs.scheduled_fired(),
            1,
            "{catalog_append_fails}: one failed write"
        );
        vfs.set_faults(FaultPlan::none());
        let above = single(&db, &a, b"above", Durability::GroupSync);
        assert_eq!(
            next(&mut one).seqno,
            above,
            "{catalog_append_fails}: a table's feed doesn't wait for the hold"
        );
        loop {
            match all.wait(WAIT).unwrap() {
                Next::Commit(c) => assert!(c.seqno < above, "{catalog_append_fails}: {c:?}"),
                Next::Expired(_) => break,
                other => panic!("{catalog_append_fails}: {other:?}"),
            }
        }
        // Writes go on, and a later catalog change takes no hold of its own: one at a
        // larger ceiling would let the frontier pass the frozen one.
        let t = db
            .create_table("d", &[("g".into(), FamilyOptions::default())])
            .unwrap();
        single(&db, &t, b"d", Durability::GroupSync);
        let later = single(&db, &a, b"later", Durability::GroupSync);
        assert_eq!(next(&mut one).seqno, later);
        drop((all, one));
        db.close().unwrap();
    }
}

/// simp724's F1 on #1515 (D315 2b): an `After(s)` start passes over a catalog change with a
/// ceiling at or below `s` even when the change's `Catalog` entry is appended after the
/// feed opened. The catalog hold bounds a feed of every table's frontier, not visible
/// seqnos, so a commit at or above the ceiling can be visible, and a snapshot taken, while
/// the root observer is still to append. Mutation-checked: applying the bound only to the
/// entries in the file at open delivers the change.
#[test]
fn an_after_start_passes_over_a_change_whose_entry_lands_after_it() {
    use pigeonhole_format::feed::FeedEntry;
    let vfs = SimVfs::new(3171);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, _b) = tables(&db);
    db.delay_root_observer(Duration::from_millis(800));
    let create = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || {
            db.create_table("c", &[("g".into(), FamilyOptions::default())])
                .unwrap()
        })
    };
    // The create holds its ceiling and waits in the observer; a commit after it is visible.
    std::thread::sleep(Duration::from_millis(200));
    let s = single(&db, &a, b"past", Durability::GroupSync);
    let mut r = db.feed(spec(&[], FeedStart::After(s))).unwrap();
    create.join().unwrap();
    db.delay_root_observer(Duration::ZERO);
    let ceiling = sidecar_entries(&vfs)
        .iter()
        .find_map(|e| match e {
            FeedEntry::Catalog {
                seqno_ceiling,
                edits,
                ..
            } if edits.iter().any(|e| {
                matches!(e, pigeonhole_format::manifest::Edit::CreateTable { name, .. } if name == "c")
            }) =>
            {
                Some(*seqno_ceiling)
            }
            _ => None,
        })
        .expect("the create's entry");
    assert!(
        ceiling <= s,
        "the start must be past the change: {ceiling} > {s}"
    );
    let after = single(&db, &a, b"after", Durability::GroupSync);
    match r.wait(WAIT).unwrap() {
        Next::Commit(c) => assert_eq!(c.seqno, after),
        other => panic!("only the commit after the start: {other:?}"),
    }
    drop(r);
    db.close().unwrap();
}

/// simp724's F1b on #1515 (D315 2b): a token resumes with its start's bound, so a resume
/// passes over exactly what the start did. The race is F1's: a commit at or above a change's
/// ceiling is visible while the change's entry isn't appended yet, and the token is taken
/// then, before any poll.
/// - **`After(s)` past the ceiling:** the start passes the change over, and so does the
///   resume (with the bound reset, it delivered the change).
/// - **`Now`:** the view doesn't reflect the change yet, so the feed delivers it, and so
///   must a resume from a token taken before that. A bound taken from the token's
///   delivered seqno would pass it over.
#[test]
fn a_resume_passes_over_what_its_start_did() {
    use pigeonhole_format::feed::FeedEntry;
    for after in [true, false] {
        let vfs = SimVfs::new(3172);
        let db = open(&vfs, Some(feed(1 << 20)));
        let (a, _b) = tables(&db);
        db.delay_root_observer(Duration::from_millis(800));
        let create = {
            let db = Arc::clone(&db);
            std::thread::spawn(move || {
                db.create_table("c", &[("g".into(), FamilyOptions::default())])
                    .unwrap()
            })
        };
        std::thread::sleep(Duration::from_millis(200));
        let s = single(&db, &a, b"past", Durability::GroupSync);
        let start = if after {
            FeedStart::After(s)
        } else {
            FeedStart::Now
        };
        let token = db.feed(spec(&[], start)).unwrap().token();
        create.join().unwrap();
        db.delay_root_observer(Duration::ZERO);
        let ceiling = sidecar_entries(&vfs)
            .iter()
            .find_map(|e| match e {
                FeedEntry::Catalog {
                    seqno_ceiling,
                    edits,
                    ..
                } if edits.iter().any(|e| {
                    matches!(e, pigeonhole_format::manifest::Edit::CreateTable { name, .. } if name == "c")
                }) =>
                {
                    Some(*seqno_ceiling)
                }
                _ => None,
            })
            .expect("the create's entry");
        assert!(
            ceiling <= s,
            "{after}: the start must be past the change: {ceiling} > {s}"
        );
        let later = single(&db, &a, b"after", Durability::GroupSync);
        let mut r = db.feed(spec(&[], FeedStart::Token(token))).unwrap();
        if !after {
            match r.wait(WAIT).unwrap() {
                Next::Catalog(ev) => assert_eq!(ev.seqno_ceiling, ceiling),
                other => panic!("now: the change its view didn't reflect first: {other:?}"),
            }
        }
        match r.wait(WAIT).unwrap() {
            Next::Commit(c) => assert_eq!(c.seqno, later, "{after}"),
            other => panic!("{after}: the commit after: {other:?}"),
        }
        drop(r);
        db.close().unwrap();
    }
}

/// ICR 0114 amendment (simp724's F1 on #1518): a feed's position moves while only other
/// tables write, and is never below `delivered`. Before the first poll it is the start.
#[test]
fn a_position_moves_while_other_tables_write() {
    let vfs = SimVfs::new(3181);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, b) = tables(&db);
    let mut r = db.feed(spec(&[&a], FeedStart::Now)).unwrap();
    assert_eq!(
        r.position().seqno,
        r.delivered(),
        "the start, before a poll"
    );
    let s1 = single(&db, &a, b"a1", Durability::GroupSync);
    assert_eq!(next(&mut r).seqno, s1);
    let mut last = 0;
    for i in 0..3u8 {
        last = single(&db, &b, &[b'b', i], Durability::GroupSync);
    }
    // A durable feed's frontier follows the syncs: poll until it's past `b`'s commits.
    let deadline = std::time::Instant::now() + WAIT;
    while r.position().seqno < last {
        assert!(matches!(r.try_next().unwrap(), Next::Pending));
        assert!(
            std::time::Instant::now() < deadline,
            "the position stayed at {:?}",
            r.position()
        );
        std::thread::yield_now();
    }
    assert!(r.position().seqno >= r.delivered());
    assert_eq!(
        r.position().catalog,
        0,
        "a feed of some tables has no catalog position"
    );
    // The next commit of the feed's own is above it.
    let s2 = single(&db, &a, b"a2", Durability::GroupSync);
    assert!(s2 > r.position().seqno);
    assert_eq!(next(&mut r).seqno, s2);
    drop(r);
    db.close().unwrap();
}

/// ICR 0114 amendment: a feed of every table's position never passes a commit read and not
/// yet delivered, nor a catalog change loaded and not yet delivered; once the change is
/// delivered, its sequence is the position's catalog part.
#[test]
fn a_position_never_passes_what_is_still_to_deliver() {
    let vfs = SimVfs::new(3182);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, _b) = tables(&db);
    let mut r = db.feed(spec(&[], FeedStart::Now)).unwrap();
    let a1 = single(&db, &a, b"a1", Durability::GroupSync);
    let a2 = single(&db, &a, b"a2", Durability::GroupSync);
    let c = db
        .create_table("c", &[("g".into(), FamilyOptions::default())])
        .unwrap();
    let c1 = single(&db, &c, b"c1", Durability::GroupSync);
    // Wait until the frontier covers all of it, so one poll reads everything.
    let deadline = std::time::Instant::now() + WAIT;
    while db.feed_frontier(true) < c1 {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    let got = match r.wait(WAIT).unwrap() {
        Next::Commit(cm) => cm.seqno,
        other => panic!("{other:?}"),
    };
    assert_eq!(got, a1);
    assert_eq!(r.position().seqno, a1, "a2 is read and not delivered");
    assert_eq!(next(&mut r).seqno, a2);
    let p = r.position();
    let sequence = match r.wait(WAIT).unwrap() {
        Next::Catalog(ev) => {
            assert!(
                p.seqno < ev.seqno_ceiling,
                "{p:?} past the change at {}",
                ev.seqno_ceiling
            );
            ev.sequence
        }
        other => panic!("the create: {other:?}"),
    };
    let p = r.position();
    assert_eq!(p.catalog, sequence);
    assert!(p.seqno < c1, "c1 is still to deliver: {p:?}");
    assert_eq!(next(&mut r).seqno, c1);
    assert!(r.position().covers(&p) && r.position() != p);
    drop(r);
    db.close().unwrap();
}

/// simp724's P1 on #1520 (ICR 0114 amendment): a feed of every table's position states its
/// catalog changes by sequence only. A `Now` start past the ceiling of a change not yet
/// published (F1b's race) has a seqno at or above that ceiling while the change is still to
/// deliver: the catalog part stays below its sequence until it is delivered.
#[test]
fn a_position_s_catalog_part_covers_a_change_only_once_delivered() {
    let vfs = SimVfs::new(3183);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, _b) = tables(&db);
    db.delay_root_observer(Duration::from_millis(800));
    let create = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || {
            db.create_table("c", &[("g".into(), FamilyOptions::default())])
                .unwrap()
        })
    };
    std::thread::sleep(Duration::from_millis(200));
    let s = single(&db, &a, b"past", Durability::GroupSync);
    let mut r = db.feed(spec(&[], FeedStart::Now)).unwrap();
    let before = r.position();
    assert!(before.seqno >= s, "{before:?}");
    create.join().unwrap();
    db.delay_root_observer(Duration::ZERO);
    let (ceiling, sequence) = match r.wait(WAIT).unwrap() {
        Next::Catalog(ev) => (ev.seqno_ceiling, ev.sequence),
        other => panic!("the change its view didn't reflect: {other:?}"),
    };
    assert!(
        ceiling <= s,
        "the start must be past the change: {ceiling} > {s}"
    );
    assert!(
        before.catalog < sequence,
        "the change was covered by sequence before it was delivered: {before:?}"
    );
    let after = r.position();
    assert_eq!(after.catalog, sequence);
    assert!(after.covers(&before));
    drop(r);
    db.close().unwrap();
}

/// D316 A4 (#1523; perf287's L1, feed1248's P1): a progress sink follows the feed's covered
/// position, so a caught-up feed reads as caught up even when the database's seqnos moved
/// past it with nothing for it to deliver. Here that's three `None` commits to a table the
/// feed doesn't carry (no fed record) and an aborted cross-shard round, whose id seqno no
/// record holds. Once the feed drains, the sink is the visible seqno: before a flush and
/// after one.
#[test]
fn a_progress_sink_reaches_the_visible_seqno_on_a_caught_up_feed() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let vfs = SimVfs::new(3111);
    let (a, b) = {
        let db = open(&vfs, Some(feed(1 << 20)));
        let t = tables(&db);
        db.close().unwrap();
        t
    };
    let (db, mut shards) =
        Engine::open_application_owned(Path::new(DB), options(&vfs, Some(feed(1 << 20)))).unwrap();
    // A visible feed of `a` alone, so a caught-up position is the visible seqno.
    let mut r = db
        .feed(FeedSpec {
            durable: false,
            ..spec(&[&a], FeedStart::Now)
        })
        .unwrap();
    let sink = Arc::new(AtomicU64::new(0));
    r.set_progress(Arc::clone(&sink));
    let commit = |shards: &mut [EngineShard], t: &TableInfo, row: &[u8], d: Durability| {
        let mut wb = WriteBatch::new();
        put(&mut wb, t, row, b"v");
        drive(shards, db.submit(wb, Some(d)).unwrap())
            .unwrap()
            .seqno
    };
    let fed = commit(&mut shards, &a, b"fed", Durability::Buffered);
    for i in 0..3u8 {
        commit(&mut shards, &b, &[b'n', i], Durability::None);
    }
    // An aborted cross-shard round (as `aborted_round`): validation fails on a's shard after
    // b's shard wrote its PREPARE, and the round's id seqno is never applied.
    let mut txn = db.begin().unwrap();
    txn.get(a.id, a.families[0].id, b"r", b"q").unwrap();
    commit(&mut shards, &a, b"r", Durability::Buffered);
    for (t, row) in [(&a, &b"r"[..]), (&b, &b"t"[..])] {
        txn.batch()
            .put(
                t.id,
                t.families[0].id,
                row,
                b"q",
                None,
                ValueRef::Bytes(b"t"),
            )
            .unwrap();
    }
    let pc = txn.submit(Some(Durability::Buffered)).unwrap();
    assert!(matches!(
        drive(&mut shards, pc),
        Err(pigeonhole_engine::Error::Conflict)
    ));
    let got: Vec<Seqno> = drain(&mut r, &mut shards).iter().map(|c| c.seqno).collect();
    assert_eq!(got.first(), Some(&fed), "{got:?}");
    let visible = db.visible_seqno();
    assert!(
        visible > *got.last().unwrap(),
        "seqnos past the feed's last commit: {got:?}"
    );
    assert_eq!(
        sink.load(Ordering::Relaxed),
        visible,
        "a caught-up feed covers up to the visible seqno"
    );
    let flush = db.submit_flush().unwrap();
    drive(&mut shards, flush).unwrap();
    assert!(drain(&mut r, &mut shards).is_empty());
    assert_eq!(
        sink.load(Ordering::Relaxed),
        db.visible_seqno(),
        "after the flush"
    );
}

/// D315 amendment (#1504, simp724's R1-R4): a process crash around a root commit's
/// superblock write never expires a durable feed. Held at each point with a seam, then
/// crashed, reopened and resumed from a token taken before:
/// - between the intent and the superblock: the root never landed, and the open cuts the
///   intent (with a create's `Catalog` entry) off, so a create never happened and isn't
///   delivered;
/// - between the superblock and the Root entry (#1504's own window): the root landed, and
///   the open adopts the intent as its Root, so a create is delivered once.
///
/// For a flush's root and a catalog change's alike, and reopened either with the feed's
/// options or with none (`feed_inherit` and an unset shard count, as `phdb` and any tool
/// open): the intent the crash left last is still the last session's, so the open inherits
/// the feed rather than turning it off (the 1.2.0 audit's S-E1).
#[test]
fn a_process_crash_around_a_root_commit_never_expires_a_durable_feed() {
    use pigeonhole_io::sim::CrashKind;
    let cases = [(false, false), (true, false), (false, true), (true, true)];
    for (i, (inherit, (landed, create))) in [false, true]
        .into_iter()
        .flat_map(|inherit| cases.into_iter().map(move |c| (inherit, c)))
        .enumerate()
    {
        let what = format!("superblock landed {landed}, create {create}, inherit {inherit}");
        let vfs = SimVfs::new(3190 + i as u64);
        let db = open(&vfs, Some(feed(1 << 20)));
        let (a, _b) = tables(&db);
        let mut r = db.feed(spec(&[], FeedStart::Now)).unwrap();
        let s1 = single(&db, &a, b"before", Durability::GroupSync);
        assert_eq!(next(&mut r).seqno, s1);
        let token = r.token();
        drop(r);
        let hold = Duration::from_millis(1500);
        if landed {
            db.delay_root_observer(hold);
        } else {
            db.delay_root_intent(hold);
        }
        let commit = {
            let db = Arc::clone(&db);
            std::thread::spawn(move || {
                if create {
                    let _ = db.create_table("c", &[("g".into(), FamilyOptions::default())]);
                } else {
                    let _ = db.flush();
                }
            })
        };
        std::thread::sleep(Duration::from_millis(400));
        vfs.crash(CrashKind::Process);
        commit.join().unwrap();
        drop(db);

        let db = if inherit {
            let mut o = options(&vfs, None);
            o.feed_inherit = true;
            o.shards = 0;
            Engine::open(Path::new(DB), o).unwrap()
        } else {
            open(&vfs, Some(feed(1 << 20)))
        };
        assert_eq!(
            db.table("c").is_some(),
            create && landed,
            "{what}: the create happened iff its superblock landed"
        );
        let a = db.table("a").unwrap();
        let s2 = single(&db, &a, b"after", Durability::GroupSync);
        let mut r = db.feed(spec(&[], FeedStart::Token(token))).unwrap();
        let mut got = Vec::new();
        loop {
            match r.wait(WAIT).unwrap() {
                Next::Commit(c) => {
                    got.push(format!("c{}", c.seqno));
                    if c.seqno == s2 {
                        break;
                    }
                }
                Next::Catalog(ev) => got.push(format!("catalog{}", ev.edits.len())),
                Next::Expired(e) => panic!("{what}: the durable feed expired: {e:?}"),
                Next::Pending => panic!("{what}: nothing within {WAIT:?}: {got:?}"),
            }
        }
        let catalogs = got.iter().filter(|g| g.starts_with("catalog")).count();
        assert_eq!(
            catalogs,
            usize::from(create && landed),
            "{what}: the create delivered once iff it happened: {got:?}"
        );
        drop(r);
        db.close().unwrap();
    }
}

/// D315 amendment (#1504): a trailing `RootIntent` whose sequence is the superblock's but
/// whose checksum isn't (another root of that sequence, such as one an older build wrote
/// without the feed) is never adopted: the open leaves it, and a token from before expires
/// rather than skipping that root's commits. Mutation-checked: adopting on the sequence
/// alone resumes the token.
#[test]
fn an_intent_naming_another_root_of_its_sequence_is_never_adopted() {
    use pigeonhole_format::feed::{FEED_SUFFIX, FeedEntry};
    use pigeonhole_io::Vfs;
    let vfs = SimVfs::new(3195);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, _b) = tables(&db);
    let mut r = db.feed(spec(&[&a], FeedStart::Now)).unwrap();
    let s1 = single(&db, &a, b"1", Durability::GroupSync);
    assert_eq!(next(&mut r).seqno, s1);
    let token = r.token();
    drop(r);
    db.close().unwrap();
    // The last session's last entry is its close's Root at the superblock's sequence N.
    // Forge what a session leaves when it crashed after its intent for N, before its
    // superblock, and a build without the feed then wrote another root N: in place of that
    // Root entry, an intent for N with another checksum. Adopting it on the sequence alone
    // would make a clean chain that silently skips root N.
    let last = match sidecar_entries(&vfs).last() {
        Some(FeedEntry::Root { sequence, .. }) => *sequence,
        other => panic!("{other:?}"),
    };
    let path = format!("{DB}{FEED_SUFFIX}");
    let f = vfs
        .open(
            Path::new(&path),
            pigeonhole_io::OpenOptions::read_write_create(),
        )
        .unwrap();
    let mut bytes = Vec::new();
    FeedEntry::RootIntent {
        sequence: last,
        checksum: 0xdead_beef,
    }
    .encode(&mut bytes);
    // A Root entry and an intent have the same framed length, and the Root is the last
    // entry: the file's last bytes.
    let at = f.len().unwrap() - bytes.len() as u64;
    f.write_at(&bytes, at).unwrap();
    f.sync_all().unwrap();
    drop(f);
    assert!(matches!(
        sidecar_entries(&vfs).last(),
        Some(FeedEntry::RootIntent { .. })
    ));
    let db = open(&vfs, Some(feed(1 << 20)));
    let mut r = db.feed(spec(&[&a], FeedStart::Token(token))).unwrap();
    assert!(
        matches!(r.try_next().unwrap(), Next::Expired(_)),
        "an intent that names another root is a break, never adopted"
    );
    drop(r);
    db.close().unwrap();
}

/// simp724's R4 on #1504 (D315 amendment): around a root commit, a power loss either
/// expires a durable feed or keeps its whole history: never a silent gap. The intent is not
/// synced, so a power loss can lose it (the sim's power crash keeps only synced data) or,
/// on a real device, tear it; both read as a torn tail, cut off. A root whose superblock
/// landed then has no entry, so the chain breaks and the token expires; one that didn't has
/// nothing to lose. An intent mid-file (a build that doesn't resolve intents opened and
/// appended after the crash) breaks the chain too. Whatever happens, a feed that resumes
/// delivers every durable commit after its token.
#[test]
fn a_power_loss_around_a_root_commit_expires_or_keeps_the_whole_history() {
    use pigeonhole_format::feed::{FEED_SUFFIX, FeedEntry};
    use pigeonhole_io::Vfs;
    use pigeonhole_io::sim::CrashKind;
    #[derive(Debug, Clone, Copy)]
    enum Damage {
        /// The sim's power crash, held between the intent and the superblock.
        PowerBeforeSuperblock,
        /// The sim's power crash, held between the superblock and the Root entry.
        PowerAfterSuperblock,
        /// The trailing intent cut in half (a torn write), then a process crash.
        TornIntent,
        /// A Session entry appended after the trailing intent, as an older build's open does.
        IntentMidFile,
    }
    for (i, damage) in [
        Damage::PowerBeforeSuperblock,
        Damage::PowerAfterSuperblock,
        Damage::TornIntent,
        Damage::IntentMidFile,
    ]
    .into_iter()
    .enumerate()
    {
        let vfs = SimVfs::new(3200 + i as u64);
        let db = open(&vfs, Some(feed(1 << 20)));
        let (a, _b) = tables(&db);
        let mut r = db.feed(spec(&[&a], FeedStart::Now)).unwrap();
        let s1 = single(&db, &a, b"before", Durability::GroupSync);
        assert_eq!(next(&mut r).seqno, s1);
        let token = r.token();
        drop(r);
        let mid = single(&db, &a, b"mid", Durability::Sync);
        let hold = Duration::from_millis(1500);
        match damage {
            Damage::PowerBeforeSuperblock => db.delay_root_intent(hold),
            _ => db.delay_root_observer(hold),
        }
        let flush = {
            let db = Arc::clone(&db);
            std::thread::spawn(move || {
                let _ = db.flush();
            })
        };
        std::thread::sleep(Duration::from_millis(400));
        match damage {
            Damage::PowerBeforeSuperblock | Damage::PowerAfterSuperblock => {
                vfs.crash(CrashKind::Power)
            }
            Damage::TornIntent | Damage::IntentMidFile => {
                // The intent is the sidecar's last entry now, its superblock written.
                let path = format!("{DB}{FEED_SUFFIX}");
                let f = vfs
                    .open(
                        Path::new(&path),
                        pigeonhole_io::OpenOptions::read_write_create(),
                    )
                    .unwrap();
                assert!(
                    matches!(
                        sidecar_entries(&vfs).last(),
                        Some(FeedEntry::RootIntent { .. })
                    ),
                    "{damage:?}: held with the intent last"
                );
                let len = f.len().unwrap();
                if matches!(damage, Damage::TornIntent) {
                    f.set_len(len - 12).unwrap();
                } else {
                    let mut bytes = Vec::new();
                    FeedEntry::Session {
                        session: 0x5e55,
                        opened_at: 0,
                        retention_bytes: 1 << 20,
                        retention_age_secs: 0,
                        streams: 2,
                        tables: pigeonhole_format::feed::FeedTables::All,
                    }
                    .encode(&mut bytes);
                    f.write_at(&bytes, len).unwrap();
                }
                drop(f);
                vfs.crash(CrashKind::Process);
            }
        }
        flush.join().unwrap();
        drop(db);
        let db = open(&vfs, Some(feed(1 << 20)));
        let a = db.table("a").unwrap();
        let s2 = single(&db, &a, b"after", Durability::GroupSync);
        let mut r = db.feed(spec(&[&a], FeedStart::Token(token))).unwrap();
        let mut got = Vec::new();
        let expired = loop {
            match r.wait(WAIT).unwrap() {
                Next::Commit(c) => {
                    got.push(c.seqno);
                    if c.seqno == s2 {
                        break false;
                    }
                }
                Next::Expired(_) => break true,
                other => panic!("{damage:?}: {other:?} after {got:?}"),
            }
        };
        if !expired {
            assert!(
                got.contains(&mid) && got.ends_with(&[s2]),
                "{damage:?}: resumed, so with the whole history: {got:?} (mid {mid}, after {s2})"
            );
        }
        // Where the root landed and its entry was lost, torn or buried, the token must
        // expire: resuming would skip that session's later roots unseen.
        if !matches!(damage, Damage::PowerBeforeSuperblock) {
            assert!(expired, "{damage:?}: a root whose entry is gone is a break");
        }
        drop(r);
        db.close().unwrap();
    }
}

/// Rewrites the sidecar as `f` maps its entries (after the header), for forged cases.
fn rewrite_sidecar(
    vfs: &Arc<SimVfs>,
    f: impl FnOnce(Vec<pigeonhole_format::feed::FeedEntry>) -> Vec<pigeonhole_format::feed::FeedEntry>,
) {
    use pigeonhole_format::feed::{FEED_SUFFIX, FeedHeader};
    use pigeonhole_io::Vfs;
    let path = format!("{DB}{FEED_SUFFIX}");
    let file = vfs
        .open(
            Path::new(&path),
            pigeonhole_io::OpenOptions::read_write_create(),
        )
        .unwrap();
    let mut head = [0u8; FeedHeader::LEN];
    file.read_at(&mut head, 0).unwrap();
    let mut bytes = head.to_vec();
    for e in f(sidecar_entries(vfs)) {
        e.encode(&mut bytes);
    }
    file.set_len(0).unwrap();
    file.write_at(&bytes, 0).unwrap();
    file.sync_all().unwrap();
}

/// A catalog edit for a forged `Catalog` entry.
fn forged_create(id: u32) -> pigeonhole_format::manifest::Edit {
    pigeonhole_format::manifest::Edit::CreateTable {
        table: pigeonhole_format::TableId(id),
        name: format!("forged{id}"),
    }
}

/// simp724's F1 on #1526: a lone `Catalog` entry at the sidecar's end, for the root one past
/// the superblock's (a power loss kept it and lost its intent, or the intent's append
/// failed and the commit too), is cut off by the next open, as its intent would be. The
/// root never landed. A feed of every table then goes on to the changes after it.
/// Mutation-checked: without the cut, the entry is followed by the new session's, and the
/// reader expires (the next test's rule) instead of delivering the later create.
#[test]
fn a_lone_trailing_catalog_entry_past_the_superblock_is_cut() {
    use pigeonhole_format::feed::FeedEntry;
    let vfs = SimVfs::new(3211);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, _b) = tables(&db);
    let mut r = db.feed(spec(&[], FeedStart::Now)).unwrap();
    let s1 = single(&db, &a, b"1", Durability::GroupSync);
    assert_eq!(next(&mut r).seqno, s1);
    let token = r.token();
    drop(r);
    db.close().unwrap();
    rewrite_sidecar(&vfs, |mut entries| {
        let (session, last) = match entries.last() {
            Some(FeedEntry::Root { session, sequence }) => (*session, *sequence),
            other => panic!("{other:?}"),
        };
        entries.push(FeedEntry::Catalog {
            session,
            sequence: last + 1,
            seqno_ceiling: s1 + 1,
            timestamp: 0,
            edits: vec![forged_create(900)],
            names: Vec::new(),
        });
        entries
    });
    let db = open(&vfs, Some(feed(1 << 20)));
    let c = db
        .create_table("c", &[("g".into(), FamilyOptions::default())])
        .unwrap();
    let s2 = single(&db, &c, b"c", Durability::GroupSync);
    let mut r = db.feed(spec(&[], FeedStart::Token(token))).unwrap();
    let mut got = Vec::new();
    loop {
        match r.wait(WAIT).unwrap() {
            Next::Catalog(ev) => got.push(format!("{:?}", ev.edits[0])),
            Next::Commit(cm) if cm.seqno == s2 => break,
            Next::Commit(_) => {}
            other => panic!("the lone entry cut, the feed goes on: {other:?} after {got:?}"),
        }
    }
    assert_eq!(got.len(), 1, "the real create only: {got:?}");
    assert!(got[0].contains("\"c\""), "{got:?}");
    drop(r);
    db.close().unwrap();
}

/// simp724's F1 on #1526: a `Catalog` entry followed by neither its Root nor its own intent
/// can never get its Root. The chain doesn't break on it (it is the session's own), so a
/// reader that waited there would deliver every later commit and no later catalog change: a
/// silent gap. It expires instead. Mutation-checked: waiting there, as for a pending one,
/// passes the later create silently.
#[test]
fn a_catalog_entry_that_can_never_get_its_root_expires_a_feed_of_every_table() {
    use pigeonhole_format::feed::FeedEntry;
    let vfs = SimVfs::new(3212);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, _b) = tables(&db);
    let mut r = db.feed(spec(&[], FeedStart::Now)).unwrap();
    let s1 = single(&db, &a, b"1", Durability::GroupSync);
    assert_eq!(next(&mut r).seqno, s1);
    let token = r.token();
    drop(r);
    db.close().unwrap();
    // Before the session's last Root entry: an orphan Catalog of a root that never landed.
    rewrite_sidecar(&vfs, |mut entries| {
        let at = entries.len() - 1;
        let (session, last) = match &entries[at] {
            FeedEntry::Root { session, sequence } => (*session, *sequence),
            other => panic!("{other:?}"),
        };
        entries.insert(
            at,
            FeedEntry::Catalog {
                session,
                sequence: last + 7,
                seqno_ceiling: s1 + 1,
                timestamp: 0,
                edits: vec![forged_create(901)],
                names: Vec::new(),
            },
        );
        entries
    });
    let db = open(&vfs, Some(feed(1 << 20)));
    let mut r = db.feed(spec(&[], FeedStart::Token(token))).unwrap();
    let c = db
        .create_table("c", &[("g".into(), FamilyOptions::default())])
        .unwrap();
    single(&db, &c, b"c", Durability::GroupSync);
    loop {
        match r.wait(WAIT).unwrap() {
            Next::Expired(_) => break,
            Next::Commit(_) => {}
            other => panic!("an unrootable catalog entry must expire the feed: {other:?}"),
        }
    }
    drop(r);
    db.close().unwrap();
}

/// simp724's R1 and F2 on #1526 (FORMAT §10.4's torn rule): a Root entry mid-file that
/// fails its CRC (a power loss tore its overwrite in place) is the end of the history, and
/// the open cuts there, later roots and all: the superblock is past the cut, so the chain
/// breaks and a token from before expires. Never a cut that continues silently.
#[test]
fn a_damaged_root_entry_mid_file_expires_rather_than_cuts_silently() {
    use pigeonhole_format::feed::{FEED_SUFFIX, FeedEntry, FeedHeader, decode_entries_at};
    use pigeonhole_io::Vfs;
    let vfs = SimVfs::new(3213);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, _b) = tables(&db);
    let mut r = db.feed(spec(&[&a], FeedStart::Now)).unwrap();
    let s1 = single(&db, &a, b"1", Durability::GroupSync);
    assert_eq!(next(&mut r).seqno, s1);
    let token = r.token();
    drop(r);
    // Several roots after the token, each recorded with an intent overwritten in place.
    for i in 0..3u8 {
        single(&db, &a, &[b'f', i], Durability::GroupSync);
        db.flush().unwrap();
    }
    db.close().unwrap();
    let path = format!("{DB}{FEED_SUFFIX}");
    let f = vfs
        .open(
            Path::new(&path),
            pigeonhole_io::OpenOptions::read_write_create(),
        )
        .unwrap();
    let mut bytes = vec![0u8; f.len().unwrap() as usize];
    f.read_at(&mut bytes, 0).unwrap();
    let (entries, _) = decode_entries_at(&bytes[FeedHeader::LEN..]);
    let roots: Vec<usize> = entries
        .iter()
        .filter(|(_, e)| matches!(e, FeedEntry::Root { .. }))
        .map(|(at, _)| *at)
        .collect();
    assert!(roots.len() >= 4, "{entries:?}");
    // A Root entry with roots after it: flip a payload byte, its CRC now fails.
    let at = FeedHeader::LEN + roots[roots.len() - 3] + 8 + 3;
    f.write_at(&[bytes[at] ^ 0xff], at as u64).unwrap();
    f.sync_all().unwrap();
    drop(f);
    let db = open(&vfs, Some(feed(1 << 20)));
    let mut r = db.feed(spec(&[&a], FeedStart::Token(token))).unwrap();
    let a = db.table("a").unwrap();
    single(&db, &a, b"after", Durability::GroupSync);
    loop {
        match r.wait(WAIT).unwrap() {
            Next::Expired(_) => break,
            Next::Commit(_) => {}
            other => panic!("a damaged Root entry mid-file must expire: {other:?}"),
        }
    }
    drop(r);
    db.close().unwrap();
}

/// A waker that counts its wakes.
#[derive(Default)]
struct Wakes(std::sync::atomic::AtomicUsize);

impl std::task::Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// #1417 (D315's catalog hold): an async feed of every table that a catalog change's hold
/// keeps back (`held`: the frontier is below the watermark) waits on the feed's hold list,
/// and the change's release wakes it. Without that wake it slept until some other hold let
/// go, which a workload of only `GroupSync` commits may never do.
#[test]
fn a_catalog_change_s_release_wakes_a_feed_it_held_back() {
    let vfs = SimVfs::new(3221);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, _b) = tables(&db);
    let mut r = db.feed(spec(&[], FeedStart::Now)).unwrap();
    // The create's root commit waits in its observer, its hold up meanwhile.
    db.delay_root_observer(Duration::from_millis(600));
    let create = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || {
            db.create_table("c", &[("g".into(), FamilyOptions::default())])
                .unwrap()
        })
    };
    std::thread::sleep(Duration::from_millis(150));
    // A commit at or above the hold's ceiling: visible, but held from the feed's frontier.
    let s = single(&db, &a, b"held", Durability::GroupSync);
    let wakes = Arc::new(Wakes::default());
    let waker = std::task::Waker::from(Arc::clone(&wakes));
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        r.poll_next(&mut cx).is_pending(),
        "held back by the create's hold"
    );
    create.join().unwrap();
    db.delay_root_observer(Duration::ZERO);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while wakes.0.load(std::sync::atomic::Ordering::SeqCst) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the hold's release never woke the feed"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    // And it then delivers: the create, then the commit.
    let mut got = Vec::new();
    while got.len() < 2 {
        match r.wait(WAIT).unwrap() {
            Next::Catalog(_) => got.push("catalog".to_string()),
            Next::Commit(c) => got.push(format!("c{}", c.seqno)),
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(got, ["catalog".to_string(), format!("c{s}")]);
    drop(r);
    db.close().unwrap();
}

/// simp724's W1 on #1533: when a catalog change's hold freezes (its Root write fails, so the
/// session's sidecar appends stop), the change's release still wakes the hold list, so an
/// async feed of every table it kept back re-polls and expires, on a quiet database with no
/// other write to wake it. Mutation-checked: without the wake on the frozen path the feed
/// is never woken.
#[test]
fn a_frozen_catalog_hold_wakes_the_feed_it_keeps_back_to_expire() {
    let vfs = SimVfs::new(3223);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, _b) = tables(&db);
    let mut r = db.feed(spec(&[], FeedStart::Now)).unwrap();
    // The create's intent is written; its root commit then waits in the observer, the hold up.
    db.delay_root_observer(Duration::from_millis(600));
    let create = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || {
            let _ = db.create_table("c", &[("g".into(), FamilyOptions::default())]);
        })
    };
    std::thread::sleep(Duration::from_millis(150));
    single(&db, &a, b"held", Durability::GroupSync);
    let wakes = Arc::new(Wakes::default());
    let waker = std::task::Waker::from(Arc::clone(&wakes));
    {
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(
            r.poll_next(&mut cx).is_pending(),
            "held back by the create's hold"
        );
    }
    // The observer's Root write fails: the session's appends stop, the hold freezes.
    let mut plan = FaultPlan::none();
    plan.fail_on = Some("phdb-feed");
    vfs.set_faults(plan);
    create.join().unwrap();
    vfs.set_faults(FaultPlan::none());
    db.delay_root_observer(Duration::ZERO);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while wakes.0.load(std::sync::atomic::Ordering::SeqCst) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the frozen hold's release never woke the feed"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let mut cx = std::task::Context::from_waker(&waker);
    match r.poll_next(&mut cx) {
        std::task::Poll::Ready(Ok(Next::Expired(_))) => {}
        other => panic!(
            "woken, it expires: {:?}",
            other.map(|n| n.map(|n| format!("{n:?}")))
        ),
    }
    drop(r);
    db.close().unwrap();
}

/// The next catalog change, waiting at most `within`; panics on a commit or anything else.
fn catalog_within(r: &mut FeedReader, within: Duration) -> pigeonhole_engine::CatalogEvent {
    let start = std::time::Instant::now();
    match r.wait(within).unwrap() {
        Next::Catalog(ev) => ev.clone(),
        other => panic!(
            "no catalog change within {within:?} ({:?}): {other:?}",
            start.elapsed()
        ),
    }
}

/// #1515's follow-up (D315): on an idle database, a feed of every table delivers each
/// catalog change at once, though no seqno moves: a create, then a drop, with no commit
/// anywhere. A blocking wait is woken by the change's release; the poll that follows loads
/// the sidecar though the frontier didn't move. Mutation-checked: without the idle load, the
/// drop waits for the next commit.
#[test]
fn a_feed_of_every_table_delivers_catalog_changes_on_an_idle_database() {
    let vfs = SimVfs::new(3231);
    let db = open(&vfs, Some(feed(1 << 20)));
    let _ = tables(&db);
    let mut r = db.feed(spec(&[], FeedStart::Now)).unwrap();
    // Waiting before the change: it must wake the wait, not only be seen by a later call.
    let creator = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            db.create_table("c", &[("g".into(), FamilyOptions::default())])
                .unwrap()
        })
    };
    let ev = catalog_within(&mut r, Duration::from_secs(3));
    let c = creator.join().unwrap().id;
    assert!(
        matches!(ev.edits[0], pigeonhole_format::manifest::Edit::CreateTable { table, .. } if table == c)
    );
    let dropper = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            db.drop_table(c).unwrap();
        })
    };
    let ev = catalog_within(&mut r, Duration::from_secs(3));
    dropper.join().unwrap();
    assert!(
        matches!(ev.edits[..], [pigeonhole_format::manifest::Edit::DropTable { table }] if table == c)
    );
    drop(r);
    db.close().unwrap();
}

/// The async form of the test above: a pending `poll_next` on a feed of every table is woken
/// by a catalog change on an idle database, and the next poll delivers it.
#[test]
fn an_async_feed_of_every_table_is_woken_by_a_catalog_change_on_an_idle_database() {
    let vfs = SimVfs::new(3232);
    let db = open(&vfs, Some(feed(1 << 20)));
    let _ = tables(&db);
    let mut r = db.feed(spec(&[], FeedStart::Now)).unwrap();
    let c = db
        .create_table("c", &[("g".into(), FamilyOptions::default())])
        .unwrap();
    catalog_within(&mut r, Duration::from_secs(3));
    let wakes = Arc::new(Wakes::default());
    let waker = std::task::Waker::from(Arc::clone(&wakes));
    {
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(r.poll_next(&mut cx).is_pending());
    }
    db.drop_table(c.id).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while wakes.0.load(std::sync::atomic::Ordering::SeqCst) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the drop never woke the feed"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let mut cx = std::task::Context::from_waker(&waker);
    match r.poll_next(&mut cx) {
        Poll::Ready(Ok(Next::Catalog(ev))) => assert!(matches!(
            ev.edits[..],
            [pigeonhole_format::manifest::Edit::DropTable { .. }]
        )),
        other => panic!("{:?}", other.map(|n| n.map(|n| format!("{n:?}")))),
    }
    drop(r);
    db.close().unwrap();
}

/// D320 (#1598): a reader far behind keeps at most its read-ahead budget of records read but
/// not delivered, and still delivers every commit once, whole and in order, with its
/// position never going back; a token taken while it's capped resumes the rest.
#[test]
fn a_reader_far_behind_keeps_its_read_ahead_within_its_budget() {
    const BUDGET: u64 = 8 << 10;
    let vfs = SimVfs::new(3201);
    let db = open(&vfs, Some(feed(16 << 20)));
    let (a, b) = tables(&db);
    let mut r = db.feed(spec(&[&a, &b], FeedStart::Now)).unwrap();
    r.set_read_ahead(BUDGET);
    // The backlog: singles on both shards and cross-shard rounds, about 100 KiB of changes.
    let value = [7u8; 100];
    let mut want = Vec::new();
    for i in 0..400u32 {
        let mut wb = WriteBatch::new();
        let row = format!("r{i:04}");
        if i % 5 == 0 {
            put(&mut wb, &a, format!("{row}a").as_bytes(), &value);
            put(&mut wb, &b, format!("{row}b").as_bytes(), &value);
        } else {
            put(
                &mut wb,
                if i % 2 == 0 { &a } else { &b },
                row.as_bytes(),
                &value,
            );
        }
        want.push(db.commit(wb, Some(Durability::GroupSync)).unwrap().seqno);
    }
    let reads_before = r.stream_reads();
    let mut got = Vec::new();
    let mut position = r.position().seqno;
    let mut token = None;
    while got.len() < want.len() {
        let c = next(&mut r);
        assert!(
            r.read_ahead_bytes() <= BUDGET,
            "kept {} bytes over the budget",
            r.read_ahead_bytes()
        );
        // Only rounds in flight across the cutoff are remembered as passed over, never every
        // round of the backlog (simp724's S1 on #1622); this backlog's rounds ran one at a
        // time.
        assert!(
            r.skipped_rounds() <= 1,
            "{} rounds remembered as passed over",
            r.skipped_rounds()
        );
        let p = r.position().seqno;
        assert!(
            p >= position,
            "the position went back from {position} to {p}"
        );
        position = p;
        got.push(c.seqno);
        if got.len() == 150 {
            token = Some(r.token());
        }
    }
    assert_eq!(got, want);
    // The cap bit: the backlog took many passes, each a rewind.
    let reads = r.stream_reads() - reads_before;
    assert!(reads > 5, "only {reads} read passes: the cap never bit");
    assert!(matches!(r.try_next().unwrap(), Next::Pending));
    drop(r);
    // From the token taken while capped: the rest, nothing skipped.
    let mut r = db
        .feed(spec(&[&a, &b], FeedStart::Token(token.unwrap())))
        .unwrap();
    r.set_read_ahead(BUDGET);
    let rest: Vec<Seqno> = (150..want.len()).map(|_| next(&mut r).seqno).collect();
    assert_eq!(rest, want[150..]);
    drop(r);
    db.close().unwrap();
}

/// D320: a feed of every table delivers a catalog change only once every commit below its
/// ceiling is delivered, even while the read-ahead cap passed some of them over: the change
/// waits for the cutoff as well as the frontier.
#[test]
fn a_capped_feed_of_every_table_delivers_a_catalog_change_after_the_commits_before_it() {
    let vfs = SimVfs::new(3202);
    let db = open(&vfs, Some(feed(16 << 20)));
    let (a, _b) = tables(&db);
    let mut r = db.feed(spec(&[], FeedStart::Now)).unwrap();
    // About one record: every pass keeps the next commit or two.
    r.set_read_ahead(300);
    let before: Vec<Seqno> = (0..6u32)
        .map(|i| single(&db, &a, format!("b{i}").as_bytes(), Durability::GroupSync))
        .collect();
    db.create_table("c", &[("g".into(), FamilyOptions::default())])
        .unwrap();
    let after: Vec<Seqno> = (0..3u32)
        .map(|i| single(&db, &a, format!("a{i}").as_bytes(), Durability::GroupSync))
        .collect();
    let mut order = Vec::new();
    while order.len() < before.len() + 1 + after.len() {
        match r.wait(WAIT).unwrap() {
            Next::Commit(c) => order.push(Some(c.seqno)),
            Next::Catalog(ev) => {
                assert!(ev.seqno_ceiling > before[5], "{ev:?}");
                order.push(None);
            }
            Next::Pending => panic!("nothing within {WAIT:?} after {order:?}"),
            Next::Expired(e) => panic!("expired: {e:?}"),
        }
    }
    let want: Vec<Option<Seqno>> = before
        .iter()
        .copied()
        .map(Some)
        .chain([None])
        .chain(after.iter().copied().map(Some))
        .collect();
    assert_eq!(order, want);
    drop(r);
    db.close().unwrap();
}
