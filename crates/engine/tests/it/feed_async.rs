//! The change feed's async form (D310 PR 5a): `FeedReader::poll_next` registers the task's
//! waker and is woken, never spinning, when the frontier can move: by the watermark the
//! feed follows, or, where the feed's own holds keep it back, by the shard as it lets a
//! hold go (that path is in `feed_written.rs`, which holds a feed's SYNCED hold for real). A
//! reader process's feed is `Unsupported` there (the sync `wait` serves it).

use crate::common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use common::poll_commit;
use pigeonhole_engine::{
    Engine, EngineShard, Error, FamilyOptions, FeedOptions, FeedReader, FeedSpec, FeedStart, Next,
    TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::{Durability, Seqno};
use pigeonhole_io::ProcessId;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";

/// A waker that counts its wakes.
#[derive(Default)]
struct Count(AtomicUsize);

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn open(vfs: &Arc<SimVfs>) -> (Arc<Engine>, Vec<EngineShard>, TableInfo) {
    let mut o = common::options(Arc::clone(vfs), 1, 4 << 20);
    let mut f = FeedOptions::default();
    f.retention_bytes = 4 << 20;
    o.feed = Some(f);
    let (db, shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
    let t = (*db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap())
    .clone();
    (db, shards, t)
}

fn settle(shards: &mut [EngineShard]) {
    while shards
        .iter_mut()
        .fold(false, |more, s| more | s.run_once(u64::MAX))
    {}
}

fn commit(
    db: &Engine,
    shards: &mut [EngineShard],
    t: &TableInfo,
    row: &[u8],
    d: Durability,
) -> Seqno {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row,
        b"q",
        None,
        ValueRef::Bytes(row),
    )
    .unwrap();
    let mut pc = db.submit(wb, Some(d)).unwrap();
    loop {
        if let Poll::Ready(r) = poll_commit(&mut pc) {
            return r.unwrap().seqno;
        }
        for s in shards.iter_mut() {
            s.run_once(u64::MAX);
        }
    }
}

fn spec(t: &TableInfo, durable: bool) -> FeedSpec {
    FeedSpec {
        tables: vec![t.id],
        prefix: Vec::new(),
        durable,
        start: FeedStart::Now,
    }
}

/// `poll_next` once: the commit's seqno, `None` for `Pending`.
fn poll(feed: &mut FeedReader, waker: &Waker) -> Option<Seqno> {
    let mut cx = Context::from_waker(waker);
    match feed.poll_next(&mut cx) {
        Poll::Ready(Ok(Next::Commit(c))) => Some(c.seqno),
        Poll::Ready(Ok(Next::Pending)) => panic!("Ready(Pending)"),
        Poll::Ready(Ok(Next::Expired(e))) => panic!("expired: {e:?}"),
        Poll::Ready(Err(e)) => panic!("{e}"),
        Poll::Pending => None,
    }
}

#[test]
fn a_pending_feed_is_woken_by_the_commit_it_waits_for() {
    let vfs = SimVfs::new(5001);
    let (db, mut shards, t) = open(&vfs);
    settle(&mut shards);
    let count = Arc::new(Count::default());
    let waker = Waker::from(Arc::clone(&count));
    let mut feed = db.feed(spec(&t, true)).unwrap();
    assert_eq!(poll(&mut feed, &waker), None, "nothing yet");
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
    let s = commit(&db, &mut shards, &t, b"one", Durability::GroupSync);
    settle(&mut shards);
    assert!(
        count.0.load(Ordering::SeqCst) > 0,
        "the commit woke the task"
    );
    assert_eq!(poll(&mut feed, &waker), Some(s));
    drop(feed);
    db.close().unwrap();
    settle(&mut shards);
}

#[test]
fn an_async_feed_in_a_reader_process_is_unsupported() {
    let vfs = SimVfs::new(5003);
    vfs.enter_process(ProcessId {
        pid: 1,
        start_time: 1,
    });
    let (db, mut shards, t) = open(&vfs);
    settle(&mut shards);
    vfs.enter_process(ProcessId {
        pid: 2,
        start_time: 1,
    });
    let r =
        Engine::open_reader(Path::new(DB), common::options(Arc::clone(&vfs), 1, 4 << 20)).unwrap();
    let mut feed = r.feed(spec(&t, true)).unwrap();
    let waker = Waker::from(Arc::new(Count::default()));
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        feed.poll_next(&mut cx),
        Poll::Ready(Err(Error::Unsupported(_)))
    ));
    drop(feed);
    drop(r);
    vfs.enter_process(ProcessId {
        pid: 1,
        start_time: 1,
    });
    db.close().unwrap();
    settle(&mut shards);
}
