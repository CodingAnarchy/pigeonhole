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
        let mut o = common::options(Arc::clone(vfs), 2, 4 << 20);
        if feed {
            let mut f = FeedOptions::default();
            f.retention_bytes = 4 << 20;
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
        let mut wb = WriteBatch::new();
        for (t, row) in rows {
            wb.put(
                t.id,
                t.families[0].id,
                row,
                b"q",
                None,
                ValueRef::Bytes(row),
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
fn a_reader_process_needs_a_feeding_writer_and_starts_after_a_seqno_not_the_oldest() {
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
    assert!(matches!(
        r.feed(spec(&[&a], FeedStart::Oldest)),
        Err(Error::Unsupported(_))
    ));
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
