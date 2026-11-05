//! The change feed's written watermark and lazy flush (D310 PR 2): a fed table's
//! `Durability::None` commits stay lazy, the shard holds the feed's frontier below them
//! (shm `WRITTEN`, a hold like `pending`) until a write passes them, and the lazy flush
//! writes them within the interval *T* (10 ms) when nothing stronger does. The timer is
//! armed only while something is held: an application-owned shard sees it in its next
//! wakeup, and an idle one does not.

use crate::common;

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use pigeonhole_engine::{
    CommitInfo, Engine, EngineOptions, EngineShard, FamilyOptions, FeedOptions, PendingCommit,
    TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::VfsRef;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";
/// The lazy flush's interval.
const T: Duration = Duration::from_millis(10);

fn feed(tables: Option<Vec<String>>) -> FeedOptions {
    let mut f = FeedOptions::default();
    f.retention_bytes = 1 << 20;
    f.tables = tables;
    f
}

/// A feed of every table whose background sync runs after `sync`.
fn feed_syncing_after(sync: Duration) -> FeedOptions {
    let mut f = feed(None);
    f.sync_interval_micros = Some(sync.as_micros() as u64);
    f
}

fn sim_options(vfs: &Arc<SimVfs>, feed: Option<FeedOptions>) -> EngineOptions {
    let mut o = common::options(Arc::clone(vfs), 2, 16 << 20);
    o.feed = feed;
    o
}

fn table(db: &Engine, name: &str) -> TableInfo {
    (*db.create_table(name, &[("f".into(), FamilyOptions::default())])
        .unwrap())
    .clone()
}

fn batch(t: &TableInfo, row: &[u8]) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row,
        b"q",
        None,
        ValueRef::Bytes(b"v"),
    )
    .unwrap();
    wb
}

#[test]
fn without_the_feed_the_frontier_is_the_visible_seqno() {
    let vfs = SimVfs::new(1310);
    let db = Engine::open(Path::new(DB), sim_options(&vfs, None)).unwrap();
    let t = table(&db, "t");
    let info = db.commit(batch(&t, b"a"), Some(Durability::None)).unwrap();
    assert!(db.feed_frontier(false) >= info.seqno);
    db.close().unwrap();
}

#[test]
fn a_none_commit_on_a_fed_table_reaches_the_frontier_through_the_lazy_flush() {
    let vfs = SimVfs::new(1311);
    let mut o = sim_options(&vfs, Some(feed(None)));
    o.shards = 1;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = table(&db, "t");
    let mut last = 0;
    for i in 0..20u32 {
        last = db
            .commit(batch(&t, &i.to_be_bytes()), Some(Durability::None))
            .unwrap()
            .seqno;
    }
    // The simulator's clock stands still: the timer gives up and the shard flushes on
    // its kick, so the frontier gets there without any stronger commit.
    let start = Instant::now();
    while db.feed_frontier(false) < last {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "the lazy flush never ran"
        );
        std::thread::yield_now();
    }
    // A durable feed passes them only once a sync covers them: here the background sync,
    // whose timer, like the flush's, gives up on the simulator's frozen clock, or the
    // `GroupSync` commit's (the real-clock tests below tell them apart).
    let synced = db
        .commit(batch(&t, b"sync"), Some(Durability::GroupSync))
        .unwrap();
    // One shard holds the table, so its sync covers every record.
    let start = Instant::now();
    while db.feed_frontier(true) < synced.seqno {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "the sync never released them"
        );
        std::thread::yield_now();
    }
    db.close().unwrap();
}

#[test]
fn a_stronger_commit_carries_the_none_records_at_once() {
    let vfs = SimVfs::new(1312);
    let mut o = sim_options(&vfs, Some(feed(None)));
    o.shards = 1;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = table(&db, "t");
    let none = db.commit(batch(&t, b"a"), Some(Durability::None)).unwrap();
    let strong = db
        .commit(batch(&t, b"b"), Some(Durability::Buffered))
        .unwrap();
    assert!(strong.seqno > none.seqno);
    // The Buffered group's write carried the buffered `None` record: nothing is held once
    // it resolved.
    assert!(db.feed_frontier(false) >= strong.seqno);
    db.close().unwrap();
}

// ---- real files and a real clock: the application-owned shard and its wakeups ----

fn real_vfs() -> VfsRef {
    match std::env::var("PIGEONHOLE_IO").as_deref() {
        #[cfg(target_os = "linux")]
        Ok("uring") => pigeonhole_io::uring::UringVfs::new_application_owned().expect("io_uring"),
        _ => pigeonhole_io::pread::PreadVfs::new(2),
    }
}

/// A temporary directory of its own per test, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let d = std::env::temp_dir().join(format!("phdb-feed-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        Self(d)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn real_options(vfs: &VfsRef, feed: Option<FeedOptions>) -> EngineOptions {
    let mut o = EngineOptions::new(Arc::clone(vfs));
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.memtable_budget = 16 << 20;
    o.reader_slots = 8;
    o.wal.segment_size = 8 * 32 * 1024;
    o.wal.spare_segments = 1;
    // No balancer: its pass would be the shard's only other timer.
    o.tablet_changes = false;
    o.feed = feed;
    o
}

/// How long [`drive`] waits for a commit: bounded by time, not turns, since a commit on the
/// real VFS waits on real I/O (a `GroupSync` commit's fsync on the pool), which a slow host
/// (Windows CI's `FlushFileBuffers`) can hold for longer than a million idle turns take
/// (#1325, as #1289).
const DRIVE_LIMIT: Duration = Duration::from_secs(60);

/// Runs the shard until `pending` resolves, yielding between turns; on a timeout, fails
/// with where the commit stood, so a stuck wait reads differently from a slow one.
fn drive(db: &Engine, shard: &mut EngineShard, mut pending: PendingCommit) -> CommitInfo {
    let mut cx = Context::from_waker(Waker::noop());
    let start = Instant::now();
    let mut turns = 0u64;
    loop {
        shard.run_once(u64::MAX);
        turns += 1;
        if let Poll::Ready(r) = Pin::new(&mut pending).poll(&mut cx) {
            return r.unwrap();
        }
        if start.elapsed() > DRIVE_LIMIT {
            panic!(
                "the commit never resolved: {turns} turns over {:?}; feed frontier {} \
                 visible, {} durable; {} background syncs",
                start.elapsed(),
                db.feed_frontier(false),
                db.feed_frontier(true),
                db.feed_background_syncs(),
            );
        }
        std::thread::yield_now();
    }
}

/// Runs the shard until it has nothing to do now.
fn settle(shard: &mut EngineShard) {
    while shard.run_once(u64::MAX) {}
}

#[test]
fn an_application_owned_shard_flushes_on_its_next_wakeup_alone() {
    let dir = TempDir::new("wakeup");
    let path = dir.0.join("w.phdb");
    let vfs = real_vfs();
    // The background sync well after the flush, so the flush is what moves the visible
    // frontier.
    let t_sync = Duration::from_millis(200);
    let (db, mut shards) =
        Engine::open_application_owned(&path, real_options(&vfs, Some(feed_syncing_after(t_sync))))
            .unwrap();
    let shard = &mut shards[0];
    let t = table(&db, "t");
    settle(shard);
    // Idle, with nothing held: no flush deadline.
    assert!(
        shard.next_deadline().is_none(),
        "an idle shard sleeps without a deadline: {:?}",
        shard.next_deadline()
    );

    let start = Instant::now();
    let info = drive(
        &db,
        shard,
        db.submit(batch(&t, b"a"), Some(Durability::None)).unwrap(),
    );
    settle(shard);
    if start.elapsed() < T {
        assert!(db.feed_frontier(false) < info.seqno, "held until a write");
        let due = shard.next_deadline().expect("the lazy flush's deadline");
        let wait = due.saturating_sub(vfs.monotonic_nanos());
        assert!(wait <= T.as_nanos() as u64, "{wait} ns");
    }
    // Driven only by its wakeups: sleep until the next deadline, then run.
    while db.feed_frontier(false) < info.seqno {
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "the frontier never passed the None commit"
        );
        if let Some(due) = shard.next_deadline() {
            let wait = due.saturating_sub(vfs.monotonic_nanos());
            std::thread::sleep(Duration::from_nanos(wait));
        }
        settle(shard);
    }
    // Written but not synced: the durable frontier waits for the background sync, the
    // shard's only deadline now; then nothing is held and there is no deadline at all.
    if start.elapsed() < t_sync {
        assert!(db.feed_frontier(true) < info.seqno);
    }
    while db.feed_frontier(true) < info.seqno {
        assert!(start.elapsed() < Duration::from_secs(5), "never synced");
        if let Some(due) = shard.next_deadline() {
            std::thread::sleep(Duration::from_nanos(
                due.saturating_sub(vfs.monotonic_nanos()),
            ));
        } else {
            std::thread::sleep(Duration::from_millis(1));
        }
        settle(shard);
    }
    settle(shard);
    assert!(
        shard.next_deadline().is_none(),
        "{:?}",
        shard.next_deadline()
    );

    db.close().unwrap();
    while shard.closed().is_none() {
        shard.run_once(u64::MAX);
    }
}

#[test]
fn an_unfed_table_holds_nothing() {
    let dir = TempDir::new("unfed");
    let path = dir.0.join("w.phdb");
    let vfs = real_vfs();
    {
        let (db, mut shards) =
            Engine::open_application_owned(&path, real_options(&vfs, None)).unwrap();
        table(&db, "fed");
        table(&db, "other");
        db.close().unwrap();
        while shards[0].closed().is_none() {
            shards[0].run_once(u64::MAX);
        }
    }
    let (db, mut shards) = Engine::open_application_owned(
        &path,
        real_options(&vfs, Some(feed(Some(vec!["fed".into()])))),
    )
    .unwrap();
    let shard = &mut shards[0];
    let other = (*db.table("other").unwrap()).clone();
    settle(shard);
    let info = drive(
        &db,
        shard,
        db.submit(batch(&other, b"a"), Some(Durability::None))
            .unwrap(),
    );
    settle(shard);
    assert!(
        db.feed_frontier(false) >= info.seqno,
        "no hold for an unfed table"
    );
    assert!(shard.next_deadline().is_none());
    db.close().unwrap();
    while shard.closed().is_none() {
        shard.run_once(u64::MAX);
    }
}

/// Opens an application-owned engine on real files with one shard.
fn open_real(dir: &TempDir, vfs: &VfsRef, feed: FeedOptions) -> (Arc<Engine>, EngineShard) {
    let (db, mut shards) =
        Engine::open_application_owned(&dir.0.join("w.phdb"), real_options(vfs, Some(feed)))
            .unwrap();
    let shard = shards.remove(0);
    (db, shard)
}

fn close_real(db: &Engine, shard: &mut EngineShard) {
    db.close().unwrap();
    while shard.closed().is_none() {
        shard.run_once(u64::MAX);
    }
}

#[test]
fn a_durable_feed_waits_for_a_sync_on_buffered_commits() {
    // simp724's M1 on #1297: D227 counts a `Buffered` commit durable once written, so the
    // durable feed needs the synced hold to stay below it until a sync covers it.
    let dir = TempDir::new("buffered");
    let vfs = real_vfs();
    // A background sync too far off to run here: only the `GroupSync` commit syncs.
    let (db, mut shard) = open_real(&dir, &vfs, feed_syncing_after(Duration::from_secs(600)));
    let t = table(&db, "t");
    settle(&mut shard);
    let info = drive(
        &db,
        &mut shard,
        db.submit(batch(&t, b"a"), Some(Durability::Buffered))
            .unwrap(),
    );
    settle(&mut shard);
    assert!(db.feed_frontier(false) >= info.seqno, "visible at once");
    assert!(
        db.feed_frontier(true) < info.seqno,
        "not durable until synced"
    );
    let synced = drive(
        &db,
        &mut shard,
        db.submit(batch(&t, b"b"), Some(Durability::GroupSync))
            .unwrap(),
    );
    settle(&mut shard);
    assert!(
        db.feed_frontier(true) >= synced.seqno,
        "the group's sync covered it"
    );
    assert_eq!(
        db.feed_background_syncs(),
        0,
        "a covering sync needs none of its own"
    );
    assert_eq!(
        db.group_syncs(),
        1,
        "the GroupSync commit's sync, counted once"
    );
    assert!(db.shard_stats().iter().all(|s| s.feed_retained_taken == 0));
    close_real(&db, &mut shard);
}

#[test]
fn the_background_sync_moves_the_durable_feed_on_a_buffered_only_table() {
    let dir = TempDir::new("tsync");
    let vfs = real_vfs();
    let t_sync = Duration::from_millis(20);
    let (db, mut shard) = open_real(&dir, &vfs, feed_syncing_after(t_sync));
    let t = table(&db, "t");
    settle(&mut shard);
    assert!(shard.next_deadline().is_none(), "idle: no sync deadline");
    let start = Instant::now();
    let info = drive(
        &db,
        &mut shard,
        db.submit(batch(&t, b"a"), Some(Durability::Buffered))
            .unwrap(),
    );
    settle(&mut shard);
    if start.elapsed() < t_sync {
        assert!(db.feed_frontier(true) < info.seqno);
        assert!(
            shard.next_deadline().is_some(),
            "the background sync's deadline"
        );
    }
    // Driven only by its wakeups.
    while db.feed_frontier(true) < info.seqno {
        assert!(start.elapsed() < Duration::from_secs(5), "never synced");
        if let Some(due) = shard.next_deadline() {
            std::thread::sleep(Duration::from_nanos(
                due.saturating_sub(vfs.monotonic_nanos()),
            ));
        } else {
            std::thread::sleep(Duration::from_millis(1));
        }
        settle(&mut shard);
    }
    assert!(db.feed_background_syncs() >= 1);
    assert_eq!(db.group_syncs(), 0, "only the feed's own syncs ran");
    settle(&mut shard);
    assert!(shard.next_deadline().is_none(), "nothing held: no deadline");
    close_real(&db, &mut shard);
}

#[test]
fn nothing_the_durable_frontier_passed_is_lost_to_a_power_loss() {
    // simp724's M1 test: commit at mixed levels, note the durable frontier at each step,
    // crash, and every commit at or below the last frontier noted is there after the reopen.
    for seed in 0..8u64 {
        let vfs = SimVfs::new(1320 + seed);
        let mut o = sim_options(&vfs, Some(feed(None)));
        o.shards = 1;
        let db = Engine::open(Path::new(DB), o).unwrap();
        let t = table(&db, "t");
        let mut committed: Vec<(u64, Vec<u8>)> = Vec::new();
        let mut frontier = 0;
        for i in 0..60u32 {
            let level = match (u64::from(i) * 7 + seed) % 5 {
                0 => Durability::GroupSync,
                1 | 2 => Durability::Buffered,
                _ => Durability::None,
            };
            let row = format!("r{i:03}").into_bytes();
            let info = db.commit(batch(&t, &row), Some(level)).unwrap();
            committed.push((info.seqno, row));
            frontier = frontier.max(db.feed_frontier(true));
        }
        vfs.crash(pigeonhole_io::sim::CrashKind::Power);
        drop(db);
        let mut o = sim_options(&vfs, Some(feed(None)));
        o.shards = 1;
        let db = Engine::open(Path::new(DB), o).unwrap();
        let t = (*db.table("t").unwrap()).clone();
        for (seqno, row) in committed.iter().filter(|(s, _)| *s <= frontier) {
            let got = db.get_latest(t.id, t.families[0].id, row, b"q").unwrap();
            assert!(
                got.is_some(),
                "seed {seed}: commit {seqno} at or below the durable frontier {frontier} was lost"
            );
        }
        db.close().unwrap();
    }
}

/// A waker that counts its wakes.
#[derive(Default)]
struct WakeCount(std::sync::atomic::AtomicUsize);

impl std::task::Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn an_async_feed_held_back_by_the_synced_hold_is_woken_when_the_shard_lets_it_go() {
    // D310 PR 5a: a durable feed's frontier stays below a `Buffered` commit until a sync
    // covers it (the SYNCED hold), though the durable watermark passed it. `poll_next` leaves
    // its waker with the hold, and the shard wakes it as the hold goes.
    use pigeonhole_engine::{FeedSpec, FeedStart, Next};
    let dir = TempDir::new("async-hold");
    let vfs = real_vfs();
    let (db, mut shard) = open_real(&dir, &vfs, feed_syncing_after(Duration::from_secs(600)));
    let t = table(&db, "t");
    settle(&mut shard);
    let mut feed = db
        .feed(FeedSpec {
            tables: vec![t.id],
            prefix: Vec::new(),
            durable: true,
            start: FeedStart::Now,
        })
        .unwrap();
    let count = Arc::new(WakeCount::default());
    let waker = Waker::from(Arc::clone(&count));
    let held = drive(
        &db,
        &mut shard,
        db.submit(batch(&t, b"a"), Some(Durability::Buffered))
            .unwrap(),
    );
    settle(&mut shard);
    assert!(
        db.feed_frontier(true) < held.seqno,
        "held until a sync covers it"
    );
    {
        let mut cx = Context::from_waker(&waker);
        assert!(
            feed.poll_next(&mut cx).is_pending(),
            "pending, its waker left with the hold"
        );
    }
    let before = count.0.load(std::sync::atomic::Ordering::SeqCst);
    // A `GroupSync` commit's sync covers the held record: the hold goes, the task is woken.
    let synced = drive(
        &db,
        &mut shard,
        db.submit(batch(&t, b"b"), Some(Durability::GroupSync))
            .unwrap(),
    );
    settle(&mut shard);
    assert!(db.feed_frontier(true) >= synced.seqno);
    assert!(
        count.0.load(std::sync::atomic::Ordering::SeqCst) > before,
        "the hold's release (or the publish with it) woke the task"
    );
    let mut cx = Context::from_waker(&waker);
    match feed.poll_next(&mut cx) {
        Poll::Ready(Ok(Next::Commit(c))) => assert_eq!(c.seqno, held.seqno),
        other => panic!("{:?}", other.map(|r| r.map(|n| format!("{n:?}")))),
    }
    drop(feed);
    close_real(&db, &mut shard);
}

/// #1415 with D310 PR 5a: once a sync is in flight when another record is logged, its
/// completion raises the synced hold to that record instead of letting it go. An async
/// durable feed the hold keeps back must be woken by the rise too, not only by a release to
/// `u64::MAX`, or under steady load it is never woken. On the real VFS: `a` is held, the
/// feed's waker is left with the hold, `b` is logged while a group sync covering `a` is in
/// flight (an attempt that misses the window starts over), and the sync's completion must
/// wake the task. Mutation-checked: waking only on a release to `u64::MAX`, it fails.
#[test]
fn an_async_durable_feed_is_woken_when_the_synced_hold_rises() {
    use pigeonhole_engine::{FeedSpec, FeedStart, Next};
    let dir = TempDir::new("async-rise");
    let vfs = real_vfs();
    let (db, mut shard) = open_real(&dir, &vfs, feed_syncing_after(Duration::from_secs(600)));
    let t = table(&db, "t");
    settle(&mut shard);
    let mut feed = db
        .feed(FeedSpec {
            tables: vec![t.id],
            prefix: Vec::new(),
            durable: true,
            start: FeedStart::Now,
        })
        .unwrap();
    let count = Arc::new(WakeCount::default());
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    let mut caught = false;
    for attempt in 0..50u32 {
        let a = drive(
            &db,
            &mut shard,
            db.submit(batch(&t, b"a"), Some(Durability::Buffered))
                .unwrap(),
        )
        .seqno;
        settle(&mut shard);
        // Everything before `a` delivered; the task then waits on the hold over `a`.
        loop {
            match feed.poll_next(&mut cx) {
                Poll::Pending => break,
                Poll::Ready(Ok(Next::Commit(c))) => assert!(c.seqno < a, "{} not held", c.seqno),
                Poll::Ready(other) => panic!("{:?}", other.map(|n| format!("{n:?}"))),
            }
        }
        let g = log_only(&db, &mut shard, batch(&t, b"g"), Durability::GroupSync);
        let b = log_only(&db, &mut shard, batch(&t, b"b"), Durability::Buffered);
        if db.feed_frontier(true) >= a {
            // The sync finished before `b` was logged: not the race. Again.
            drive(&db, &mut shard, b);
            drive(&db, &mut shard, g);
            settle(&mut shard);
            continue;
        }
        let before = count.0.load(std::sync::atomic::Ordering::SeqCst);
        let b = drive(&db, &mut shard, b).seqno;
        let g = drive(&db, &mut shard, g).seqno;
        settle(&mut shard);
        let f = db.feed_frontier(true);
        assert!(
            f >= g && f < b,
            "the hold rose to `b`, not past it (attempt {attempt}): frontier {f}, a {a}, g {g}, b {b}"
        );
        assert!(
            count.0.load(std::sync::atomic::Ordering::SeqCst) > before,
            "the hold's rise past `a` woke the task (attempt {attempt})"
        );
        // What the rise let through: `a`, after any `b` an earlier attempt left held (its
        // sync was done before that `b` was logged, and the background sync is 600 s away).
        let mut got = Vec::new();
        loop {
            match feed.poll_next(&mut cx) {
                Poll::Pending => break,
                Poll::Ready(Ok(Next::Commit(c))) => got.push(c.seqno),
                Poll::Ready(other) => panic!("{:?}", other.map(|n| format!("{n:?}"))),
            }
        }
        assert!(
            got.contains(&a) && got.iter().all(|&s| s < b),
            "the rise delivered `a` and nothing from `b` on (attempt {attempt}): {got:?}, a {a}, b {b}"
        );
        caught = true;
        break;
    }
    assert!(caught, "never logged `b` while a sync was in flight");
    drop(feed);
    close_real(&db, &mut shard);
}

/// Submits `batch` and runs one turn, so its group is logged (and a group sync submitted)
/// without waiting for it to resolve.
fn log_only(db: &Engine, shard: &mut EngineShard, wb: WriteBatch, d: Durability) -> PendingCommit {
    let pending = db.submit(wb, Some(d)).unwrap();
    shard.run_once(u64::MAX);
    pending
}

/// #1415: a sync covers what was logged when it was submitted. A fed `Buffered` record
/// logged while that sync is in flight must not keep the synced hold below what it covered,
/// or under steady load the durable frontier never moves. On the real VFS, whose syncs run
/// on the I/O pool: `b` is logged while a sync covering `a` is in flight (an attempt that
/// misses that window, the sync done first, starts over), for a group sync and for the
/// feed's background sync. Mutation-checked: with the hold one pair again, both fail.
#[test]
fn a_record_logged_during_a_sync_never_holds_back_what_the_sync_covered() {
    for group in [true, false] {
        let dir = TempDir::new(if group { "race-group" } else { "race-bg" });
        let vfs = real_vfs();
        // The group case keeps the background sync out of it.
        let t_sync = if group {
            Duration::from_secs(600)
        } else {
            Duration::from_millis(20)
        };
        let (db, mut shard) = open_real(&dir, &vfs, feed_syncing_after(t_sync));
        let t = table(&db, "t");
        settle(&mut shard);
        let mut caught = false;
        for attempt in 0..50u32 {
            let a = drive(
                &db,
                &mut shard,
                db.submit(batch(&t, b"a"), Some(Durability::Buffered))
                    .unwrap(),
            )
            .seqno;
            let syncs = db.feed_background_syncs();
            // A sync covering `a`, then `b` logged at once, while it is (usually) in flight.
            let g = if group {
                Some(log_only(
                    &db,
                    &mut shard,
                    batch(&t, b"g"),
                    Durability::GroupSync,
                ))
            } else {
                let start = Instant::now();
                while db.feed_background_syncs() == syncs {
                    assert!(
                        start.elapsed() < Duration::from_secs(5),
                        "no background sync"
                    );
                    shard.run_once(u64::MAX);
                }
                None
            };
            let b = log_only(&db, &mut shard, batch(&t, b"b"), Durability::Buffered);
            if db.feed_frontier(true) >= a {
                // The sync finished before `b` was logged: not the race. Again.
                drive(&db, &mut shard, b);
                if let Some(g) = g {
                    drive(&db, &mut shard, g);
                }
                settle(&mut shard);
                continue;
            }
            let b = drive(&db, &mut shard, b).seqno;
            if let Some(g) = g {
                // Its resolution means its sync's completion was handled.
                let g = drive(&db, &mut shard, g).seqno;
                settle(&mut shard);
                let f = db.feed_frontier(true);
                assert!(
                    f >= g && f < b,
                    "the group sync covered `a` and `g`, not `b` (attempt {attempt}): \
                     frontier {f}, a {a}, g {g}, b {b}"
                );
            } else {
                // The first sync's completion moves the frontier past `a` but not `b`; the
                // next background sync covers `b`. With the hold one pair, it would jump.
                let (start, mut between) = (Instant::now(), false);
                loop {
                    let f = db.feed_frontier(true);
                    between |= f >= a && f < b;
                    if f >= b {
                        break;
                    }
                    assert!(
                        start.elapsed() < Duration::from_secs(5),
                        "never synced: {f}"
                    );
                    shard.run_once(u64::MAX);
                }
                assert!(
                    between,
                    "the frontier passed `a` only with `b` (attempt {attempt}): a {a}, b {b}"
                );
            }
            caught = true;
            break;
        }
        // Best effort on real files: a fast sync may finish before `b` is logged every time
        // (#1570's Windows run). The held-sync test below sets the race up every time.
        if !caught {
            eprintln!(
                "real files (group {group}): never logged `b` while a sync was in flight; \
                 a_record_logged_during_a_held_sync_never_holds_back_what_the_sync_covered \
                 covers it"
            );
        }
        close_real(&db, &mut shard);
    }
}

/// [`SimVfs::hold_file_syncs`] while alive.
struct HeldSyncs<'a>(&'a SimVfs);

impl<'a> HeldSyncs<'a> {
    fn new(vfs: &'a SimVfs) -> Self {
        vfs.hold_file_syncs(true);
        Self(vfs)
    }
}

impl Drop for HeldSyncs<'_> {
    fn drop(&mut self) {
        self.0.hold_file_syncs(false);
    }
}

/// [`a_record_logged_during_a_sync_never_holds_back_what_the_sync_covered`] on the simulator,
/// with the sync held in flight ([`SimVfs::hold_file_syncs`]), so `b` is always logged while
/// the sync covering `a` is still out: the race that test can only hope to hit on real files.
#[test]
fn a_record_logged_during_a_held_sync_never_holds_back_what_the_sync_covered() {
    for group in [true, false] {
        let vfs = SimVfs::new(1340 + u64::from(group));
        vfs.set_deferred_io(true);
        let _device = vfs.complete_io_in_background();
        // The group case keeps the background sync out of it.
        let t_sync = if group {
            Duration::from_secs(600)
        } else {
            Duration::from_millis(20)
        };
        let mut o = sim_options(&vfs, Some(feed_syncing_after(t_sync)));
        o.shards = 1;
        let (db, mut shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
        let mut shard = shards.remove(0);
        let t = table(&db, "t");
        settle(&mut shard);
        // Syncs held from before `a`, so the one covering it stays in flight; released on
        // the way out whatever happens, so a failed check can't leave the close waiting.
        let syncs = db.feed_background_syncs();
        let hold = HeldSyncs::new(&vfs);
        let a = drive(
            &db,
            &mut shard,
            db.submit(batch(&t, b"a"), Some(Durability::Buffered))
                .unwrap(),
        )
        .seqno;
        // A sync covering `a`, held in flight.
        let g = if group {
            Some(log_only(
                &db,
                &mut shard,
                batch(&t, b"g"),
                Durability::GroupSync,
            ))
        } else {
            for _ in 0..1000 {
                if db.feed_background_syncs() > syncs {
                    break;
                }
                vfs.advance(t_sync.as_nanos() as u64);
                shard.run_once(u64::MAX);
            }
            assert!(db.feed_background_syncs() > syncs, "no background sync");
            None
        };
        let b = log_only(&db, &mut shard, batch(&t, b"b"), Durability::Buffered);
        assert!(
            db.feed_frontier(true) < a,
            "the held sync can't have covered `a` (group {group})"
        );
        let b = drive(&db, &mut shard, b).seqno;
        drop(hold);
        if let Some(g) = g {
            let g = drive(&db, &mut shard, g).seqno;
            settle(&mut shard);
            let f = db.feed_frontier(true);
            assert!(
                f >= g && f < b,
                "the group sync covered `a` and `g`, not `b`: frontier {f}, a {a}, g {g}, b {b}"
            );
        } else {
            // The held sync's completion moves the frontier past `a` but not `b`; the next
            // background sync covers `b`. With the hold one pair, it would jump.
            let mut between = false;
            for _ in 0..10_000 {
                let f = db.feed_frontier(true);
                between |= f >= a && f < b;
                if f >= b {
                    break;
                }
                shard.run_once(u64::MAX);
                if db.feed_frontier(true) == f {
                    vfs.advance(1_000_000);
                }
            }
            assert!(db.feed_frontier(true) >= b, "never synced `b`");
            assert!(
                between,
                "the frontier passed `a` only with `b`: a {a}, b {b}"
            );
        }
        close_real(&db, &mut shard);
    }
}

/// #1417: the blocking wait, held back by the SYNCED hold, parks on the hold list rather than
/// polling every millisecond (it polls a few times in 500 ms, not about 500), and the hold's
/// release wakes it: a `GroupSync` commit's sync covers the held record. On the real VFS, as
/// the async test above.
#[test]
fn a_held_blocking_wait_parks_until_the_hold_lets_go() {
    use pigeonhole_engine::{FeedSpec, FeedStart, Next};
    let dir = TempDir::new("blocking-hold");
    let vfs = real_vfs();
    let (db, mut shard) = open_real(&dir, &vfs, feed_syncing_after(Duration::from_secs(600)));
    let t = table(&db, "t");
    settle(&mut shard);
    let mut feed = db
        .feed(FeedSpec {
            tables: vec![t.id],
            prefix: Vec::new(),
            durable: true,
            start: FeedStart::Now,
        })
        .unwrap();
    let held = drive(
        &db,
        &mut shard,
        db.submit(batch(&t, b"a"), Some(Durability::Buffered))
            .unwrap(),
    );
    settle(&mut shard);
    assert!(
        db.feed_frontier(true) < held.seqno,
        "held until a sync covers it"
    );
    let before = feed.polls();
    assert!(matches!(
        feed.wait(Duration::from_millis(500)).unwrap(),
        Next::Pending
    ));
    let polls = feed.polls() - before;
    assert!(
        polls < 20,
        "{polls} polls while held: it re-polled instead of parking"
    );
    // A waiter on another thread; the hold's release wakes it.
    let waiter = std::thread::spawn(move || {
        let start = std::time::Instant::now();
        let got = match feed.wait(Duration::from_secs(60)).unwrap() {
            Next::Commit(c) => c.seqno,
            other => panic!("{other:?}"),
        };
        (got, start.elapsed())
    });
    std::thread::sleep(Duration::from_millis(100));
    drive(
        &db,
        &mut shard,
        db.submit(batch(&t, b"b"), Some(Durability::GroupSync))
            .unwrap(),
    );
    settle(&mut shard);
    let (got, took) = waiter.join().unwrap();
    assert_eq!(got, held.seqno);
    assert!(
        took < Duration::from_secs(10),
        "woken by the release, not the 600 s backstop: {took:?}"
    );
    close_real(&db, &mut shard);
}
