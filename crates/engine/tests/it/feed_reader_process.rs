//! The change feed in a reader process (D310 PR 4): it follows the writer's feed through
//! shared memory (the frontier, and each stream's head, floor and checkpoint, layout
//! version 8), delivers the same whole commits in the same order as a writer-process feed,
//! and across a writer restart fails with `SnapshotExpired`, after which a feed resumed from
//! its token on the new writer goes on with nothing missed or repeated.

use crate::common;

use std::path::Path;
use std::sync::Arc;
use std::task::Poll;

use common::poll_commit;
use pigeonhole_engine::{
    Engine, EngineShard, Error, FamilyOptions, FeedOptions, FeedReader, FeedSpec, FeedStart, Next,
    TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::{Durability, Seqno};
use pigeonhole_io::ProcessId;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";
const WRITER: ProcessId = ProcessId {
    pid: 1,
    start_time: 1,
};
const READER: ProcessId = ProcessId {
    pid: 2,
    start_time: 1,
};

struct Writer {
    db: Arc<Engine>,
    shards: Vec<EngineShard>,
}

impl Writer {
    fn open(vfs: &Arc<SimVfs>, feed: bool) -> Self {
        Self::open_retaining(vfs, feed.then_some(4 << 20))
    }

    /// With the feed on and `retention` bytes of history (the smallest is two segments
    /// per stream).
    fn open_retaining(vfs: &Arc<SimVfs>, retention: Option<u64>) -> Self {
        let mut o = common::options(Arc::clone(vfs), 2, 4 << 20);
        if let Some(bytes) = retention {
            let mut f = FeedOptions::default();
            f.retention_bytes = bytes;
            o.feed = Some(f);
        }
        let (db, shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
        Writer { db, shards }
    }

    fn run(&mut self) -> bool {
        let mut more = false;
        for s in &mut self.shards {
            more |= s.run_once(u64::MAX);
        }
        more
    }

    fn settle(&mut self) {
        while self.run() {}
    }

    /// Commits `rows` of `t` (several tables' rows in one batch make a cross-shard commit).
    fn commit(&mut self, rows: &[(&TableInfo, &[u8])]) -> Seqno {
        self.commit_values(&rows.iter().map(|&(t, r)| (t, r, r)).collect::<Vec<_>>())
    }

    /// Commits `(table, row, value)` triples in one batch.
    fn commit_values(&mut self, rows: &[(&TableInfo, &[u8], &[u8])]) -> Seqno {
        let mut wb = WriteBatch::new();
        for (t, row, value) in rows {
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
        let mut pc = self.db.submit(wb, Some(Durability::GroupSync)).unwrap();
        for _ in 0..100_000 {
            if let Poll::Ready(r) = poll_commit(&mut pc) {
                let seqno = r.unwrap().seqno;
                self.settle();
                return seqno;
            }
            self.run();
        }
        panic!("commit did not finish");
    }

    fn flush(&mut self) {
        let mut m = self.db.flush_pending().unwrap();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        while std::pin::Pin::new(&mut m).poll(&mut cx).is_pending() {
            self.run();
        }
    }

    fn close(mut self) {
        self.db.close().unwrap();
        while self.run() {}
    }
}

/// Two tables on different shards (tablet n is on shard n % 2).
fn tables(db: &Engine) -> (TableInfo, TableInfo) {
    let f = || vec![("f".into(), FamilyOptions::default())];
    let a = (*db.create_table("a", &f()).unwrap()).clone();
    let b = (*db.create_table("b", &f()).unwrap()).clone();
    (a, b)
}

fn spec(tables: &[&TableInfo], start: FeedStart) -> FeedSpec {
    FeedSpec {
        tables: tables.iter().map(|t| t.id).collect(),
        prefix: Vec::new(),
        durable: true,
        start,
    }
}

fn reader(vfs: &Arc<SimVfs>) -> Arc<Engine> {
    Engine::open_reader(Path::new(DB), common::options(Arc::clone(vfs), 2, 4 << 20)).unwrap()
}

/// Every commit the feed has ready now: (seqno, rows).
/// Commits as (seqno, sorted rows).
type Got = Vec<(Seqno, Vec<Vec<u8>>)>;

fn ready(feed: &mut FeedReader) -> Result<Got, Error> {
    let mut out = Vec::new();
    loop {
        match feed.try_next()? {
            Next::Commit(c) => {
                let mut rows: Vec<Vec<u8>> = c.changes.iter().map(|ch| ch.row.clone()).collect();
                rows.sort();
                out.push((c.seqno, rows));
            }
            Next::Pending => return Ok(out),
            Next::Expired(e) => panic!("expired: {e:?}"),
            Next::Catalog(e) => panic!("a catalog event in a per-table feed: {e:?}"),
        }
    }
}

#[test]
fn a_reader_process_follows_the_writers_feed_in_commit_order() {
    let vfs = SimVfs::new(4101);
    vfs.enter_process(WRITER);
    let mut w = Writer::open(&vfs, true);
    let (a, b) = tables(&w.db);
    w.settle();
    vfs.enter_process(READER);
    let r = reader(&vfs);
    let mut feed = r.feed(spec(&[&a, &b], FeedStart::Now)).unwrap();
    let mut want = Vec::new();
    let mut got = Vec::new();
    for i in 0..40u32 {
        vfs.enter_process(WRITER);
        let row = format!("r{i:02}").into_bytes();
        // Every fourth a cross-shard commit over both tables.
        let seqno = if i % 4 == 3 {
            let s = w.commit(&[(&a, &row), (&b, &row)]);
            want.push((s, vec![row.clone(), row.clone()]));
            s
        } else {
            let s = w.commit(&[(&a, &row)]);
            want.push((s, vec![row.clone()]));
            s
        };
        assert!(seqno > 0);
        if i % 5 == 4 {
            // A flush moves the checkpoint: the reader process sees it in shared memory.
            let m = w.db.flush_pending().unwrap();
            let mut m = m;
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            while std::pin::Pin::new(&mut m).poll(&mut cx).is_pending() {
                w.run();
            }
        }
        vfs.enter_process(READER);
        got.extend(ready(&mut feed).unwrap());
    }
    assert_eq!(got, want, "every commit once, whole, in commit order");
    drop(feed);
    vfs.enter_process(WRITER);
    w.close();
}

#[test]
fn a_writer_restart_expires_the_attachment_and_the_token_continues_on_the_new_writer() {
    let vfs = SimVfs::new(4102);
    vfs.enter_process(WRITER);
    let mut w = Writer::open(&vfs, true);
    let (a, _) = tables(&w.db);
    w.settle();
    vfs.enter_process(READER);
    let r = reader(&vfs);
    let mut feed = r.feed(spec(&[&a], FeedStart::Now)).unwrap();
    vfs.enter_process(WRITER);
    let first = w.commit(&[(&a, b"one")]);
    vfs.enter_process(READER);
    assert_eq!(ready(&mut feed).unwrap(), [(first, vec![b"one".to_vec()])]);
    let token = feed.token();

    // The writer closes and a new one opens and commits: the reader's attachment is stale.
    vfs.enter_process(WRITER);
    w.close();
    let mut w = Writer::open(&vfs, true);
    let a = (*w.db.table("a").unwrap()).clone();
    let second = w.commit(&[(&a, b"two")]);
    vfs.enter_process(READER);
    assert!(
        matches!(feed.try_next(), Err(Error::SnapshotExpired)),
        "a feed never reads on past its writer's restart"
    );
    // Resumed from its token: re-attached to the new writer, nothing missed or repeated.
    let mut feed = r.feed(spec(&[&a], FeedStart::Token(token))).unwrap();
    assert_eq!(ready(&mut feed).unwrap(), [(second, vec![b"two".to_vec()])]);
    drop(feed);
    vfs.enter_process(WRITER);
    w.close();
}

#[test]
fn a_reader_process_needs_a_feeding_writer_and_starts_after_a_seqno() {
    let vfs = SimVfs::new(4103);
    vfs.enter_process(WRITER);
    let mut w = Writer::open(&vfs, false);
    let (a, _) = tables(&w.db);
    w.settle();
    vfs.enter_process(READER);
    let r = reader(&vfs);
    assert!(
        matches!(
            r.feed(spec(&[&a], FeedStart::Now)),
            Err(Error::InvalidArgument(_))
        ),
        "the writer's feed is off"
    );
    drop(r);
    vfs.enter_process(WRITER);
    w.close();

    let mut w = Writer::open(&vfs, true);
    let a = (*w.db.table("a").unwrap()).clone();
    let before = w.commit(&[(&a, b"before")]);
    let after = w.commit(&[(&a, b"after")]);
    vfs.enter_process(READER);
    let r = reader(&vfs);
    // From the floor, skipping what the seqno covers.
    let mut feed = r.feed(spec(&[&a], FeedStart::After(before))).unwrap();
    assert_eq!(
        ready(&mut feed).unwrap(),
        [(after, vec![b"after".to_vec()])]
    );
    drop(feed);
    vfs.enter_process(WRITER);
    w.close();
}

/// simp724's B1 on #1352: a reader process asked to start after a seqno whose later commits
/// retention already recycled has lost history, and expires (`FEED_BOUND`), never skipping
/// them; the writer's own feed expires the same way (`start_at`). The oldest retained start
/// still works there.
#[test]
fn a_reader_process_start_past_recycled_history_expires() {
    let vfs = SimVfs::new(4104);
    vfs.enter_process(WRITER);
    // The smallest retention: two segments per stream.
    let mut w = Writer::open_retaining(&vfs, Some(1));
    let (a, _) = tables(&w.db);
    w.settle();
    let snap = w.commit(&[(&a, b"snapshot")]);
    // Far past the retention, flushed as it goes so the checkpoints move on: the commits just
    // after `snap` are recycled.
    let big = vec![7u8; 20_000];
    for i in 0..300u32 {
        let row = format!("r{i:03}");
        w.commit_values(&[(&a, row.as_bytes(), &big)]);
        if i % 30 == 29 {
            w.flush();
        }
    }
    vfs.enter_process(READER);
    let r = reader(&vfs);
    let mut feed = r.feed(spec(&[&a], FeedStart::After(snap))).unwrap();
    assert!(
        matches!(feed.try_next().unwrap(), Next::Expired(_)),
        "history past the snapshot is gone: never skipped silently"
    );
    // From the oldest retained start (`FEED_BOUND`): not expired, and it goes on with what
    // follows. Here everything retained is at or below that bound (the last flush
    // checkpointed it all), so the next commit is the first delivered.
    let mut feed = r.feed(spec(&[&a], FeedStart::Oldest)).unwrap();
    assert!(matches!(feed.try_next().unwrap(), Next::Pending));
    vfs.enter_process(WRITER);
    let next = w.commit(&[(&a, b"next")]);
    vfs.enter_process(READER);
    assert_eq!(ready(&mut feed).unwrap(), [(next, vec![b"next".to_vec()])]);
    drop(feed);
    drop(r);
    vfs.enter_process(WRITER);
    w.close();
}

/// simp724's B2 on #1352: a writer restart between the poll's stale check and its stream
/// reads is `SnapshotExpired` (resubscribe from the token), never a permanent gap expiry,
/// and the token then continues on the new writer.
#[test]
fn a_writer_restart_during_a_poll_is_snapshot_expired_never_a_gap() {
    let vfs = SimVfs::new(4105);
    vfs.enter_process(WRITER);
    let mut w = Writer::open(&vfs, true);
    let (a, _) = tables(&w.db);
    w.settle();
    vfs.enter_process(READER);
    let r = reader(&vfs);
    let mut feed = r.feed(spec(&[&a], FeedStart::Now)).unwrap();
    vfs.enter_process(WRITER);
    let first = w.commit(&[(&a, b"one")]);
    vfs.enter_process(READER);
    assert_eq!(ready(&mut feed).unwrap(), [(first, vec![b"one".to_vec()])]);
    let token = feed.token();
    // The restart lands inside the next poll, after its stale check.
    let writer = Arc::new(std::sync::Mutex::new(Some(w)));
    let second = Arc::new(std::sync::Mutex::new(0));
    {
        let (writer, second, vfs) = (Arc::clone(&writer), Arc::clone(&second), Arc::clone(&vfs));
        r.on_next(
            pigeonhole_engine::HookSite::ReaderFeedReads,
            Box::new(move || {
                vfs.enter_process(WRITER);
                let mut slot = writer.lock().unwrap();
                slot.take().unwrap().close();
                let mut w = Writer::open(&vfs, true);
                let a = (*w.db.table("a").unwrap()).clone();
                *second.lock().unwrap() = w.commit(&[(&a, b"two")]);
                *slot = Some(w);
                vfs.enter_process(READER);
            }),
        );
    }
    assert!(
        matches!(feed.try_next(), Err(Error::SnapshotExpired)),
        "a restart mid-poll is the writer's, not a gap"
    );
    let mut feed = r.feed(spec(&[&a], FeedStart::Token(token))).unwrap();
    let two = *second.lock().unwrap();
    assert_eq!(ready(&mut feed).unwrap(), [(two, vec![b"two".to_vec()])]);
    drop(feed);
    drop(r);
    vfs.enter_process(WRITER);
    writer.lock().unwrap().take().unwrap().close();
}

/// A value separated into a blob file at commit time is delivered whole in a reader
/// process, through its own view (simp724's question on #1352: such a file is in the
/// manifest before the commit is logged; a lagging view waits rather than expiring).
#[test]
fn a_reader_process_delivers_a_separated_value() {
    let vfs = SimVfs::new(4106);
    vfs.enter_process(WRITER);
    let mut w = Writer::open(&vfs, true);
    let (a, _) = tables(&w.db);
    w.settle();
    vfs.enter_process(READER);
    let r = reader(&vfs);
    let mut feed = r.feed(spec(&[&a], FeedStart::Now)).unwrap();
    vfs.enter_process(WRITER);
    let large = vec![9u8; 2 << 20];
    let seqno = w.commit_values(&[(&a, b"big", &large)]);
    vfs.enter_process(READER);
    let c = loop {
        match feed.try_next().unwrap() {
            Next::Commit(c) => break c.clone(),
            Next::Pending => continue,
            Next::Expired(e) => panic!("expired: {e:?}"),
            Next::Catalog(e) => panic!("a catalog event in a per-table feed: {e:?}"),
        }
    };
    assert_eq!(c.seqno, seqno);
    let value = &c.changes[0].value;
    assert_eq!(
        value.len(),
        large.len() + 1,
        "the tag byte and the whole value"
    );
    assert!(value[1..] == large[..]);
    drop(feed);
    drop(r);
    vfs.enter_process(WRITER);
    w.close();
}
