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

/// Runs the shard until `pending` resolves.
fn drive(shard: &mut EngineShard, mut pending: PendingCommit) -> CommitInfo {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..1_000_000 {
        shard.run_once(u64::MAX);
        if let Poll::Ready(r) = Pin::new(&mut pending).poll(&mut cx) {
            return r.unwrap();
        }
    }
    panic!("the commit never resolved");
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
