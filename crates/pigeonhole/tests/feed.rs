//! The public change feed (D310 PR 3b): `Table::feed`, whole commits with named, typed
//! changes, tokens across a reopen, snapshots, prefixes, and `ErrorCode::FeedExpired`.

use std::path::Path;
use std::time::Duration;

use pigeonhole::{
    ChangeKind, Commit, ErrorCode, Family, Feed, FeedRetention, FeedToken, Options, Pigeonhole,
    Table, Value,
};

const WAIT: Duration = Duration::from_secs(10);

fn fed() -> Options {
    Options::default()
        .shards(2)
        .change_feed(FeedRetention::bytes(16 << 20))
}

fn tables(db: &Pigeonhole) -> (Table, Table) {
    let a = db
        .table("a")
        .unwrap()
        .family("f", Family::default())
        .family("n", Family::counter())
        .create_if_missing()
        .unwrap();
    let b = db
        .table("b")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    (a, b)
}

fn next(feed: &mut Feed) -> Commit {
    feed.next(WAIT)
        .unwrap()
        .expect("a commit within the wait")
        .clone()
}

#[test]
fn changes_arrive_named_typed_and_whole() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("db.phdb"), fed()).unwrap();
    let (a, b) = tables(&db);
    let mut feed = a.feed().table(&b).open().unwrap();

    let put = a.mutate(b"r1").put("f", b"q", b"v").commit().unwrap();
    let c = next(&mut feed);
    assert_eq!(c.seqno(), put.seqno);
    assert!(c.timestamp() > 0);
    let ch = &c.changes()[0];
    assert_eq!(
        (ch.table(), ch.family(), ch.kind()),
        ("a", "f", ChangeKind::Put)
    );
    assert_eq!((ch.row(), ch.qualifier()), (&b"r1"[..], &b"q"[..]));
    assert_eq!(ch.value(), Some(Value::Bytes(b"v")));
    assert_eq!(ch.timestamp(), c.timestamp(), "the commit's timestamp");

    // A counter input is a merge with its typed operand.
    a.mutate(b"r1").incr("n", b"hits", 3).commit().unwrap();
    let c = next(&mut feed);
    let ch = &c.changes()[0];
    assert_eq!((ch.family(), ch.kind()), ("n", ChangeKind::Merge));
    assert!(
        matches!(ch.value(), Some(Value::I64(3) | Value::Varint(3))),
        "{:?}",
        ch.value()
    );

    // Every delete kind, in the order the commit wrote them, and none has a value.
    a.mutate(b"r1")
        .delete_cell("f", b"q", 7)
        .delete_column("f", b"q2")
        .delete_family("f")
        .commit()
        .unwrap();
    let c = next(&mut feed);
    let kinds: Vec<ChangeKind> = c.changes().iter().map(|ch| ch.kind()).collect();
    assert_eq!(
        kinds,
        [
            ChangeKind::DeleteCell,
            ChangeKind::DeleteColumn,
            ChangeKind::DeleteFamily
        ]
    );
    assert!(
        c.changes()
            .iter()
            .all(|ch| ch.kind().is_delete() && ch.value().is_none())
    );
    assert_eq!(c.changes()[0].timestamp(), 7);

    // One commit over both tables (on both shards here) arrives as one commit.
    let mut batch = db.write_batch();
    batch.put(&a, b"x", "f", b"q", b"1");
    batch.put(&b, b"y", "f", b"q", b"2");
    let both = batch.commit().unwrap();
    let c = next(&mut feed);
    assert_eq!(c.seqno(), both.seqno);
    let mut got: Vec<(&str, &[u8])> = c
        .changes()
        .iter()
        .map(|ch| (ch.table(), ch.row()))
        .collect();
    got.sort();
    assert_eq!(got, [("a", &b"x"[..]), ("b", &b"y"[..])]);
    assert!(feed.try_next().unwrap().is_none());
    drop(feed);
    db.close().unwrap();
}

#[test]
fn a_token_resumes_after_a_reopen_and_a_snapshot_starts_after_what_it_sees() {
    let dir = pigeonhole::doc_support::temp_dir();
    let path = dir.join("db.phdb");
    let db = Pigeonhole::open(&path, fed()).unwrap();
    let (a, _) = tables(&db);
    a.mutate(b"before").put("f", b"q", b"0").commit().unwrap();
    let snap = db.snapshot().unwrap();
    let mut feed = a.feed().from_snapshot(&snap).open().unwrap();
    let one = a.mutate(b"one").put("f", b"q", b"1").commit().unwrap();
    assert_eq!(
        next(&mut feed).seqno(),
        one.seqno,
        "not the commit the snapshot saw"
    );
    let token = FeedToken::from_bytes(feed.token().as_bytes().to_vec());
    let two = a.mutate(b"two").put("f", b"q", b"2").commit().unwrap();
    drop(feed);
    db.close().unwrap();

    let db = Pigeonhole::open(&path, fed()).unwrap();
    let a = db.table("a").unwrap().open().unwrap();
    let mut feed = a.feed().resume(&token).open().unwrap();
    let c = next(&mut feed);
    assert_eq!(c.seqno(), two.seqno);
    assert_eq!(c.changes()[0].row(), b"two");
    assert!(feed.try_next().unwrap().is_none());
    drop(feed);
    db.close().unwrap();
}

#[test]
fn a_prefix_follows_only_its_rows() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("db.phdb"), fed()).unwrap();
    let (a, _) = tables(&db);
    let mut feed = a.feed().prefix(b"user:").visible().open().unwrap();
    a.mutate(b"order:1").put("f", b"q", b"x").commit().unwrap();
    let mine = a.mutate(b"user:1").put("f", b"q", b"y").commit().unwrap();
    let c = next(&mut feed);
    assert_eq!(
        c.seqno(),
        mine.seqno,
        "a commit with nothing under the prefix is passed over"
    );
    drop(feed);
    db.close().unwrap();
}

#[test]
fn subscribing_needs_the_feed_and_a_token_of_this_database() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("off.phdb"), Options::default()).unwrap();
    let (a, _) = tables(&db);
    let e = a.feed().open().unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}");
    db.close().unwrap();

    let db = Pigeonhole::open(dir.join("some.phdb"), Options::default()).unwrap();
    tables(&db);
    db.close().unwrap();
    let db = Pigeonhole::open(dir.join("some.phdb"), fed().feed_tables(["a"])).unwrap();
    let b = db.table("b").unwrap().open().unwrap();
    let e = b.feed().open().unwrap_err();
    assert_eq!(
        e.code(),
        ErrorCode::InvalidArgument,
        "a table the feed doesn't follow: {e}"
    );
    let a = db.table("a").unwrap().open().unwrap();
    let e = a
        .feed()
        .resume(&FeedToken::from_bytes(b"not a token".to_vec()))
        .open()
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}");
    db.close().unwrap();
}

/// cli692's S1 on #1321: a table or snapshot from another database is refused, as every
/// other API refuses one, not followed under the wrong name or from another log's seqno.
#[test]
fn a_table_or_snapshot_from_another_database_is_refused() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("one.phdb"), fed()).unwrap();
    let other = Pigeonhole::open(dir.join("two.phdb"), fed()).unwrap();
    let (a, _) = tables(&db);
    // The same names, so the same table ids: only the database tells them apart.
    let (foreign, _) = tables(&other);
    let e = a.feed().table(&foreign).open().unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}");
    assert!(e.message().contains("another database"), "{e}");
    let snap = other.snapshot().unwrap();
    let e = a.feed().from_snapshot(&snap).open().unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}");
    assert!(e.message().contains("another database"), "{e}");
    // The first refusal stands whatever follows it.
    let mine = db.snapshot().unwrap();
    let e = a
        .feed()
        .table(&foreign)
        .from_snapshot(&mine)
        .open()
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}");
    drop((snap, mine));
    other.close().unwrap();
    db.close().unwrap();
}

#[test]
fn an_open_without_the_feed_between_expires_a_token() {
    let dir = pigeonhole::doc_support::temp_dir();
    let path = dir.join("db.phdb");
    let db = Pigeonhole::open(&path, fed()).unwrap();
    let (a, _) = tables(&db);
    let mut feed = a.feed().open().unwrap();
    a.mutate(b"r").put("f", b"q", b"1").commit().unwrap();
    next(&mut feed);
    let token = feed.token();
    drop(feed);
    db.close().unwrap();
    expire_between(&path, token);
}

/// ICR 0109 (D310 amendment (d)): a read-only open between two fed opens, with no feed
/// options and another shard count, writes no session, so the token still resumes, and
/// what was committed after it arrives.
#[test]
fn a_read_only_open_between_keeps_a_token() {
    let dir = pigeonhole::doc_support::temp_dir();
    let path = dir.join("db.phdb");
    let db = Pigeonhole::open(&path, fed()).unwrap();
    let (a, _) = tables(&db);
    let mut feed = a.feed().open().unwrap();
    a.mutate(b"r").put("f", b"q", b"1").commit().unwrap();
    next(&mut feed);
    let token = feed.token();
    a.mutate(b"after").put("f", b"q", b"2").commit().unwrap();
    drop(feed);
    db.close().unwrap();

    // A tool reads: as `phdb info` or `phdb dump` does.
    let ro = Pigeonhole::open(&path, Options::default().shards(1).read_only(true)).unwrap();
    assert!(ro.info().unwrap().read_only);
    let t = ro.table("a").unwrap().open().unwrap();
    assert!(t.get(b"after", "f", b"q").unwrap().is_some());
    let e = t.mutate(b"x").put("f", b"q", b"v").commit().unwrap_err();
    assert_eq!(e.code(), ErrorCode::ReadOnly, "{e}");
    drop(t);
    ro.close().unwrap();

    let db = Pigeonhole::open(&path, fed()).unwrap();
    let a = db.table("a").unwrap().open().unwrap();
    let mut feed = a.feed().resume(&token).open().unwrap();
    let c = feed.next(WAIT).unwrap().expect("the commit after the token");
    assert_eq!(c.changes()[0].row(), b"after");
    drop(feed);
    db.close().unwrap();
}

/// Writes through an open with the feed off, then resumes `token`: it has expired, and stays
/// so.
fn expire_between(path: &Path, token: FeedToken) {
    let db = Pigeonhole::open(path, Options::default().shards(2)).unwrap();
    let a = db.table("a").unwrap().open().unwrap();
    a.mutate(b"unfed").put("f", b"q", b"2").commit().unwrap();
    db.close().unwrap();
    let db = Pigeonhole::open(path, fed()).unwrap();
    let a = db.table("a").unwrap().open().unwrap();
    let mut feed = a.feed().resume(&token).open().unwrap();
    for _ in 0..2 {
        let e = feed.next(WAIT).unwrap_err();
        assert_eq!(e.code(), ErrorCode::FeedExpired, "{e}");
        assert_eq!(e.code() as u32, 31);
        assert!(e.message().contains("expired"), "{e}");
    }
    drop(feed);
    db.close().unwrap();
}
