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

/// A token's writer session: bytes 29..37 (after the version byte, the database id, the
/// stream count and the last seqno delivered; `TokenHeader::parse`).
fn token_session(token: &[u8]) -> u64 {
    // The layout these offsets hold for (version 1, the only one 1.1.x writes): a new one
    // fails here rather than being misread.
    assert!(
        matches!(token.first(), Some(1)),
        "a token layout this test doesn't know: {token:?}"
    );
    u64::from_le_bytes(token[29..37].try_into().unwrap())
}

#[test]
fn a_token_across_a_writer_restart_names_the_later_session_whatever_its_id() {
    // #1810: session ids are random, and the reader kept the larger one it read through, so
    // a token that crossed a reopen named the older session about half the time. A resume
    // on a copy that holds that session but not the later one then passed the history
    // check and went on past records it never had. It must name the later session.
    for seed in 3810u64..3874 {
        let vfs = SimVfs::new(seed);
        let db = open(&vfs, Some(feed(1 << 20)));
        let (a, _b) = tables(&db);
        let mut r = db.feed(spec(&[&a], FeedStart::Now)).unwrap();
        let first = single(&db, &a, b"one", Durability::GroupSync);
        assert_eq!(next(&mut r).seqno, first);
        let token = r.token();
        let s1 = token_session(&token);
        drop(r);
        db.close().unwrap();

        let db = open(&vfs, Some(feed(1 << 20)));
        // The reopen's session, as a feed that read only it names it.
        let mut fresh = db.feed(spec(&[&a], FeedStart::Now)).unwrap();
        let second = single(&db, &a, b"two", Durability::GroupSync);
        assert_eq!(next(&mut fresh).seqno, second);
        let s2 = token_session(&fresh.token());
        assert_ne!(s1, s2, "seed {seed}: a reopen is a new session");
        drop(fresh);
        if s2 > s1 {
            // Either rule names s2: try another seed for the case that tells them apart.
            db.close().unwrap();
            continue;
        }
        // From the first token: through "two", which the later, smaller session wrote.
        let mut r = db.feed(spec(&[&a], FeedStart::Token(token))).unwrap();
        assert_eq!(next(&mut r).seqno, second);
        assert_eq!(
            token_session(&r.token()),
            s2,
            "seed {seed}: the token names the later session ({s2:#x}), not the larger id \
             ({s1:#x})"
        );
        db.close().unwrap();
        return;
    }
    panic!("no seed in range drew a smaller id for the later session");
}

/// A framed sidecar entry: `len` u32, CRC32C u32, then the payload (FORMAT §10).
fn frame(payload: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(8 + payload.len());
    f.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    f.extend_from_slice(&pigeonhole_format::checksum::crc32c(payload).to_le_bytes());
    f.extend_from_slice(payload);
    f
}

/// A 1.2 build's `RootIntent` (type 5): the new superblock's `sequence` and checksum.
fn root_intent_1_2(sequence: u64, checksum: u64) -> Vec<u8> {
    let mut p = vec![5u8];
    p.extend_from_slice(&sequence.to_le_bytes());
    p.extend_from_slice(&checksum.to_le_bytes());
    frame(&p)
}

/// A 1.2 build's `Catalog` (type 4), as 1.2.0 wrote it for a `CreateTable` of
/// `feed_tokens` with one family `t` (cli692's v1.2.0 corpus, `feed_catalog`), with its
/// `session` and `sequence` set for this file.
fn catalog_1_2(session: u64, sequence: u64) -> Vec<u8> {
    const FRAMED: &str = "5e000000ce78949204847d748c7c55f5c6030000000000000001000000000000000000000000000000020000000110010000000b666565645f746f6b656e730325010000000100000001740103004000000a0000000000000000000000000010000000010000";
    let bytes: Vec<u8> = (0..FRAMED.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&FRAMED[i..i + 2], 16).unwrap())
        .collect();
    let mut p = bytes[8..].to_vec();
    assert_eq!(p[0], 4, "a Catalog entry");
    p[1..9].copy_from_slice(&session.to_le_bytes());
    p[9..17].copy_from_slice(&sequence.to_le_bytes());
    frame(&p)
}

/// A rollback from 1.2 after a crash inside a root commit (the owner's 1.1.3 item, cli692's
/// note on 1.2's sidecar): 1.2 left its intent, and maybe the root's Catalog entry, as the
/// sidecar's last entries. This build opens without feed options, as `phdb` or a tool does
/// (`feed_inherit`, shards unset), and:
/// - keeps the feed on: its last session is still the one before the 1.2 entries;
/// - resolves the intent as 1.2 does, so no intent is left in front of its own entry, where
///   a later 1.2 open would read it as a break and expire the feed: the root landed (the
///   superblock has the intent's sequence and checksum), so the intent became its Root;
///   or it didn't, so the intent went, with that root's Catalog entry;
/// - and a token from before resumes, unless a 1.2 Catalog entry stays in the history, which
///   this build can't read (it expires, as 1.2's changelog says).
#[test]
fn a_rollback_from_1_2_after_a_crash_in_a_root_commit_keeps_the_feed_and_resolves_its_intent() {
    use pigeonhole_format::feed::{FEED_SUFFIX, FeedEntry, FeedHeader, decode_entries};
    use pigeonhole_io::{Vfs, VfsRef};
    // (the root landed, a Catalog entry with it)
    for (landed, catalog) in [(true, false), (true, true), (false, false), (false, true)] {
        let what = format!("landed {landed}, catalog {catalog}");
        let vfs = SimVfs::new(3811);
        let db = open(&vfs, Some(feed(1 << 20)));
        let (a, _b) = tables(&db);
        let mut r = db.feed(spec(&[&a], FeedStart::Now)).unwrap();
        let first = single(&db, &a, b"one", Durability::GroupSync);
        assert_eq!(next(&mut r).seqno, first, "{what}");
        let token = r.token();
        drop(r);
        db.close().unwrap();

        // The superblock the next open reads, and the sidecar its close left: the last entry
        // is the close's Root for that superblock.
        let (sequence, checksum) = {
            let v: VfsRef = vfs.clone();
            let p = pigeonhole_pager::Pager::open(&v, Path::new(DB), false).unwrap();
            (p.sequence(), p.checksum())
        };
        let path = format!("{DB}{FEED_SUFFIX}");
        let file = vfs
            .open(
                Path::new(&path),
                pigeonhole_io::OpenOptions::read_write_create(),
            )
            .unwrap();
        let mut bytes = vec![0u8; file.len().unwrap() as usize];
        file.read_at(&mut bytes, 0).unwrap();
        let (entries, _) = decode_entries(&bytes[FeedHeader::LEN..]);
        let Some(&FeedEntry::Root {
            session,
            sequence: last,
        }) = entries.last()
        else {
            panic!("{what}: {entries:?}");
        };
        assert_eq!(last, sequence, "{what}");
        // What a 1.2 writer's crash leaves: for a root that landed, its Root entry not yet
        // written over its intent (with the root's Catalog entry before it); for one that
        // didn't, its intent (and Catalog) after the last Root, for the next sequence.
        let mut tail = Vec::new();
        let at = if landed {
            if catalog {
                tail.extend(catalog_1_2(session, sequence));
            }
            tail.extend(root_intent_1_2(sequence, checksum));
            bytes.len() - 25
        } else {
            if catalog {
                tail.extend(catalog_1_2(session, sequence + 1));
            }
            tail.extend(root_intent_1_2(sequence + 1, checksum ^ 1));
            bytes.len()
        };
        file.set_len(at as u64).unwrap();
        file.write_at(&tail, at as u64).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let mut o = options(&vfs, None);
        o.feed_inherit = true;
        o.shards = 0;
        let db = Engine::open(Path::new(DB), o).unwrap();
        let file = vfs
            .open(Path::new(&path), pigeonhole_io::OpenOptions::read())
            .unwrap();
        let mut after = vec![0u8; file.len().unwrap() as usize];
        file.read_at(&mut after, 0).unwrap();
        let (entries, end) = decode_entries(&after[FeedHeader::LEN..]);
        assert_eq!(FeedHeader::LEN + end, after.len(), "{what}: nothing torn");
        assert!(
            !entries.contains(&FeedEntry::Unknown(5)),
            "{what}: no intent left in front of this open's entry: {entries:?}"
        );
        let n = entries.len();
        assert!(
            matches!(entries[n - 1], FeedEntry::Session { .. }),
            "{what}: the open inherited the feed: {entries:?}"
        );
        let before = if landed && catalog {
            vec![FeedEntry::Unknown(4), FeedEntry::Root { session, sequence }]
        } else {
            vec![FeedEntry::Root { session, sequence }]
        };
        assert_eq!(
            entries[n - 1 - before.len()..n - 1],
            before[..],
            "{what}: {entries:?}"
        );

        let a = db.table("a").unwrap();
        let second = single(&db, &a, b"two", Durability::GroupSync);
        let mut r = db.feed(spec(&[&a], FeedStart::Token(token))).unwrap();
        if landed && catalog {
            // The 1.2 Catalog entry stays in the history, which this build can't read: the
            // token expires as any token past the history's start does (`FeedExpired` at the
            // public API), never an error that reads as corruption.
            match r.wait(WAIT).unwrap() {
                Next::Expired(e) => {
                    eprintln!("{what}: expired: {}", e.reason);
                    assert!(
                        !e.reason.to_lowercase().contains("corrupt"),
                        "{what}: {}",
                        e.reason
                    );
                }
                other => panic!("{what}: across a 1.2 catalog change: {other:?}"),
            }
        } else {
            assert_eq!(next(&mut r).seqno, second, "{what}");
        }
        drop(r);
        db.close().unwrap();
    }
}

#[test]
fn a_token_taken_before_any_record_names_the_session_and_a_gap_expires_it() {
    // audit120's P2-C6: a feed that had read nothing yet handed out a token naming session
    // 0, and a resume runs the history check only for a nonzero session. Here a session
    // with the feed off writes in between, as in
    // `a_session_without_the_feed_between_expires_a_token`, but the token predates every
    // record: it must name its session and expire all the same.
    let vfs = SimVfs::new(3881);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, _) = tables(&db);
    let token = db.feed(spec(&[&a], FeedStart::Now)).unwrap().token();
    assert_ne!(
        token_session(&token),
        0,
        "a token names its session from the open"
    );
    db.close().unwrap();
    let db = open(&vfs, None);
    single(&db, &a, b"unfed", Durability::GroupSync);
    db.close().unwrap();
    let db = open(&vfs, Some(feed(1 << 20)));
    let mut r = db.feed(spec(&[&a], FeedStart::Token(token))).unwrap();
    assert!(
        matches!(r.try_next().unwrap(), Next::Expired(_)),
        "a gap is never skipped silently, whenever the token was taken"
    );
    db.close().unwrap();
}

#[test]
fn an_oldest_start_from_an_earlier_session_names_it_before_any_record() {
    // perf2's remainder of audit120's P2-C6: `Oldest` served from an earlier session still
    // kept whole (#1465) left the token's session at 0 until a record was read, so a token
    // taken there skipped the history check, as P2-C6's did. It names the session it starts
    // in, and a feed-off session written in between expires it.
    let vfs = SimVfs::new(3882);
    let db = open(&vfs, Some(feed(1 << 20)));
    let (a, b) = tables(&db);
    db.close().unwrap();
    // The earlier session, with commits: `Oldest` starts in it later.
    let db = open(&vfs, Some(feed(1 << 20)));
    let mut r = db.feed(spec(&[&a, &b], FeedStart::Now)).unwrap();
    let one = single(&db, &a, b"one", Durability::GroupSync);
    assert_eq!(next(&mut r).seqno, one);
    let earlier = token_session(&r.token());
    drop(r);
    single(&db, &b, b"two", Durability::GroupSync);
    db.close().unwrap();

    let db = open(&vfs, Some(feed(1 << 20)));
    let r = db.feed(spec(&[&a, &b], FeedStart::Oldest)).unwrap();
    let token = r.token();
    assert_eq!(
        token_session(&token),
        earlier,
        "a token from an earlier-session start names that session before any record"
    );
    drop(r);
    db.close().unwrap();
    // A session with the feed off writes in between.
    let db = open(&vfs, None);
    single(&db, &a, b"unfed", Durability::GroupSync);
    db.close().unwrap();
    let db = open(&vfs, Some(feed(1 << 20)));
    let mut r = db.feed(spec(&[&a, &b], FeedStart::Token(token))).unwrap();
    assert!(
        matches!(r.try_next().unwrap(), Next::Expired(_)),
        "a gap is never skipped silently, whenever the token was taken"
    );
    drop(r);
    db.close().unwrap();
}
