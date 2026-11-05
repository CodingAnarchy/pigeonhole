//! The public change feed (D310 PR 3b): `Table::feed`, whole commits with named, typed
//! changes, tokens across a reopen, snapshots, prefixes, and `ErrorCode::FeedExpired`.

use std::path::Path;
use std::time::Duration;

use pigeonhole::{
    CatalogEdit, ChangeKind, Commit, ErrorCode, Family, Feed, FeedEvent, FeedRetention, FeedToken,
    Options, Pigeonhole, Table, Value,
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
fn an_open_with_the_feed_off_between_expires_a_token() {
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
    let c = feed
        .next(WAIT)
        .unwrap()
        .expect("the commit after the token");
    assert_eq!(c.changes()[0].row(), b"after");
    drop(feed);
    db.close().unwrap();
}

/// Writes through an open with the feed turned off, then resumes `token`: it has expired,
/// and stays so.
fn expire_between(path: &Path, token: FeedToken) {
    let db = Pigeonhole::open(path, Options::default().shards(2).change_feed_off()).unwrap();
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

/// D310's amendment (the owner, 2026-11-02): a tool's open that mentions neither the feed nor
/// the shard count (`phdb load`, say) inherits both from the app's last session, so the app's
/// token resumes past it, and the tool's commit is in the feed like any other.
#[test]
fn an_open_that_does_not_mention_the_feed_inherits_it_and_keeps_the_token() {
    let dir = pigeonhole::doc_support::temp_dir();
    let path = dir.join("db.phdb");
    // Three shards: unlikely to be any machine's default, so the tool's unset count is
    // really inherited.
    let app = || fed().shards(3).feed_tables(["a"]);
    // The tables first (a new database: nothing to inherit, the feed is off).
    let db = Pigeonhole::open(&path, Options::default().shards(3)).unwrap();
    assert!(db.info().unwrap().feed.is_none());
    tables(&db);
    db.close().unwrap();
    let db = Pigeonhole::open(&path, app()).unwrap();
    let a = db.table("a").unwrap().open().unwrap();
    let mut feed = a.feed().open().unwrap();
    a.mutate(b"app1").put("f", b"q", b"1").commit().unwrap();
    next(&mut feed);
    let token = feed.token();
    drop(feed);
    db.close().unwrap();

    // The tool: plain options, no shard count.
    let db = Pigeonhole::open(&path, Options::default()).unwrap();
    let info = db.info().unwrap();
    assert!(info.feed.is_some(), "the feed is inherited: {info:?}");
    let a = db.table("a").unwrap().open().unwrap();
    let tool = a.mutate(b"tool").put("f", b"q", b"t").commit().unwrap();
    db.close().unwrap();

    let db = Pigeonhole::open(&path, app()).unwrap();
    let a = db.table("a").unwrap().open().unwrap();
    let mut feed = a.feed().resume(&token).open().unwrap();
    let c = next(&mut feed);
    assert_eq!(
        (c.seqno(), c.changes()[0].row()),
        (tool.seqno, &b"tool"[..])
    );
    let after = a.mutate(b"app2").put("f", b"q", b"2").commit().unwrap();
    assert_eq!(next(&mut feed).seqno(), after.seqno);
    drop(feed);
    db.close().unwrap();
}

/// Resharding on purpose (an explicit, different shard count) still ends the history, with
/// the reason given.
#[test]
fn an_explicit_new_shard_count_expires_the_token() {
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
    let db = Pigeonhole::open(&path, Options::default().shards(1)).unwrap();
    db.close().unwrap();
    let db = Pigeonhole::open(&path, fed()).unwrap();
    let a = db.table("a").unwrap().open().unwrap();
    let mut feed = a.feed().resume(&token).open().unwrap();
    let e = feed.next(WAIT).unwrap_err();
    assert_eq!(e.code(), ErrorCode::FeedExpired, "{e}");
    drop(feed);
    db.close().unwrap();
}

/// cli692 on #1344: if every table the last session fed was dropped since, an open that
/// doesn't mention the feed has nothing to inherit, and records the feed off.
#[test]
fn an_open_inherits_nothing_once_every_fed_table_is_dropped() {
    let dir = pigeonhole::doc_support::temp_dir();
    let path = dir.join("db.phdb");
    let db = Pigeonhole::open(&path, Options::default().shards(2)).unwrap();
    tables(&db);
    db.close().unwrap();
    let db = Pigeonhole::open(&path, fed().feed_tables(["a"])).unwrap();
    db.drop_table("a").unwrap();
    db.close().unwrap();
    let db = Pigeonhole::open(&path, Options::default()).unwrap();
    assert!(
        db.info().unwrap().feed.is_none(),
        "no fed table left: the feed is off"
    );
    db.close().unwrap();
    // And the open after that inherits nothing either (it recorded `fed: none`).
    let db = Pigeonhole::open(&path, Options::default()).unwrap();
    assert!(db.info().unwrap().feed.is_none());
    db.close().unwrap();
}

#[test]
fn the_feed_s_syncs_are_counted_beside_the_group_syncs() {
    use pigeonhole::Durability;
    let dir = pigeonhole::doc_support::temp_dir();
    // The feed off: a group sync counts, the feed's syncs stay 0.
    let db = Pigeonhole::open(dir.join("off.phdb"), Options::default().shards(1)).unwrap();
    let (a, _) = tables(&db);
    a.mutate(b"r")
        .put("f", b"q", b"1")
        .durability(Durability::GroupSync)
        .commit()
        .unwrap();
    let m = db.engine_metrics();
    assert!(m.group_syncs >= 1);
    assert_eq!((m.feed_background_syncs, m.feed_sidecar_syncs), (0, 0));
    db.close().unwrap();

    // The feed on: the catalog changes' sidecar entries are synced.
    let db = Pigeonhole::open(dir.join("on.phdb"), fed()).unwrap();
    let (a, _) = tables(&db);
    a.mutate(b"r")
        .put("f", b"q", b"1")
        .durability(Durability::GroupSync)
        .commit()
        .unwrap();
    let deadline = std::time::Instant::now() + WAIT;
    while db.engine_metrics().feed_sidecar_syncs == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    let m = db.engine_metrics();
    assert!(m.group_syncs >= 1);
    assert!(
        m.feed_sidecar_syncs >= 1,
        "the table creations' sidecar entries"
    );
    db.close().unwrap();
}

/// #1465 (ai1's repro): `from_oldest` after a reopen starts at the oldest retained history,
/// not at the reopen. Two shards, small segments; small values (one segment per session)
/// and 4 KiB ones (several). After one reopen and after a second (two earlier sessions),
/// with the feed inherited or set again, every commit is delivered once, in order. A token
/// from before the close still resumes after it.
#[test]
fn from_oldest_after_a_reopen_starts_at_the_oldest_retained_history() {
    for big in [false, true] {
        let dir = pigeonhole::doc_support::temp_dir();
        let path = dir.join("f.phdb");
        let opts = || {
            Options::default()
                .shards(2)
                .pin_threads(false)
                .wal_segment_size(256 << 10)
        };
        let fed = || opts().change_feed(FeedRetention::bytes(4 << 20));
        let oldest = |db: &Pigeonhole| -> Vec<u64> {
            let t = db.table("events").unwrap().open().unwrap();
            let mut f = t.feed().from_oldest().open().unwrap();
            let mut got = Vec::new();
            while let Some(c) = f.next(Duration::from_millis(300)).unwrap() {
                got.push(c.seqno());
            }
            got
        };
        let commit = |t: &Table, i: u64| {
            let v = if big {
                format!("event {i}").repeat(400)
            } else {
                format!("event {i}")
            };
            t.mutate(format!("r{:02}", i % 7).as_bytes())
                .put("e", b"what", v.as_bytes())
                .commit()
                .unwrap();
        };
        let db = Pigeonhole::open(&path, fed()).unwrap();
        let t = db
            .table("events")
            .unwrap()
            .family("e", Family::default())
            .create()
            .unwrap();
        for i in 0..300 {
            commit(&t, i);
        }
        let first = oldest(&db);
        assert_eq!(first.len(), 300, "big {big}: before the close");
        let token = {
            let mut f = t.feed().from_oldest().open().unwrap();
            for _ in 0..100 {
                f.next(WAIT).unwrap().unwrap();
            }
            f.token()
        };
        drop(t);
        db.close().unwrap();

        // Inherited: the open doesn't mention the feed.
        let db = Pigeonhole::open(&path, opts()).unwrap();
        assert_eq!(oldest(&db), first, "big {big}: after a reopen");
        {
            let t = db.table("events").unwrap().open().unwrap();
            let mut f = t.feed().resume(&token).open().unwrap();
            let mut n = 0;
            while f.next(Duration::from_millis(300)).unwrap().is_some() {
                n += 1;
            }
            assert_eq!(n, 200, "big {big}: the token resumes");
            for i in 300..350 {
                commit(&t, i);
            }
        }
        let both = oldest(&db);
        assert_eq!(both.len(), 350, "big {big}: both sessions");
        db.close().unwrap();

        // Set again, a third session: two earlier ones to start from.
        let db = Pigeonhole::open(&path, fed()).unwrap();
        let got = oldest(&db);
        assert_eq!(got, both, "big {big}: after a second reopen");
        assert!(got.windows(2).all(|w| w[0] < w[1]), "in order, once each");
        db.close().unwrap();
    }
}

/// #1471 (ai1249's repro): reopened with another `wal_segment_size`, the feed keeps its
/// retention in bytes. An existing WAL file keeps its own slot size, so retention counted
/// in the configured size kept too little (a larger size: `from_oldest` lost the first
/// session) or far too much (a smaller one). Two sessions on 256 KiB slots, then a third
/// open with the default size and one with 64 KiB: `from_oldest` still starts at the first
/// commit, and the retention in effect is the requested 4 MiB.
#[test]
fn a_changed_wal_segment_size_keeps_the_feed_s_retention() {
    for third in [None, Some(64u64 << 10)] {
        let dir = pigeonhole::doc_support::temp_dir();
        let path = dir.join("f.phdb");
        let opts = |size: Option<u64>| {
            let o = Options::default().shards(2).pin_threads(false);
            match size {
                Some(s) => o.wal_segment_size(s),
                None => o,
            }
        };
        let small = Some(256u64 << 10);
        let mut first = 0;
        let mut token = None;
        for session in 0..2u64 {
            let o = opts(small);
            let db = Pigeonhole::open(
                &path,
                if session == 0 {
                    o.change_feed(FeedRetention::bytes(4 << 20))
                } else {
                    o
                },
            )
            .unwrap();
            let t = db
                .table("events")
                .unwrap()
                .family("e", Family::default())
                .create_if_missing()
                .unwrap();
            if session == 0 {
                token = Some(t.feed().open().unwrap().token());
            }
            for i in 0..150u64 {
                let v = format!("event {i:03} ").repeat(180);
                let c = t
                    .mutate(format!("r{:02}", i % 7).as_bytes())
                    .put("e", b"what", v.as_bytes())
                    .commit()
                    .unwrap();
                if session == 0 && i == 0 {
                    first = c.seqno;
                }
                if i % 50 == 49 {
                    db.flush().unwrap();
                }
            }
            drop(t);
            db.close().unwrap();
        }
        let db = Pigeonhole::open(&path, opts(third)).unwrap();
        let info = db.info().unwrap().feed.expect("the feed is inherited");
        assert_eq!(
            info.retention_bytes,
            4 << 20,
            "{third:?}: the retention in effect, in the files' own slots"
        );
        let t = db.table("events").unwrap().open().unwrap();
        // A token from before the first commit still resumes: its history was kept.
        let mut f = t.feed().resume(token.as_ref().unwrap()).open().unwrap();
        let c = f.next(WAIT).unwrap().expect("the token's history");
        assert_eq!(
            c.seqno(),
            first,
            "{third:?}: the token resumes at the first commit"
        );
        drop(f);
        // And from the oldest (#1465) starts there too.
        let mut f = t.feed().from_oldest().open().unwrap();
        let c = f.next(WAIT).unwrap().expect("history");
        assert_eq!(c.seqno(), first, "{third:?}: from the first session");
        drop(f);
        drop(t);
        db.close().unwrap();
    }
}

/// D315 PR 1: a feed of every table (`Pigeonhole::feed`) follows every table, including one
/// created after it opened, in commit order with names, and keeps its prefix. It needs the
/// feed on for every table, takes no `table`, and its history ends at an open with a table
/// list: a token from before such an open expires.
#[test]
fn a_feed_of_every_table_follows_tables_created_later() {
    let dir = pigeonhole::doc_support::temp_dir();
    let path = dir.join("all.phdb");
    let db = Pigeonhole::open(&path, fed()).unwrap();
    let (a, b) = tables(&db);
    let mut feed = db.feed().open().unwrap();
    let mut keyed = db.feed().prefix(b"k").open().unwrap();
    let s1 = a.mutate(b"k1").put("f", b"q", b"1").commit().unwrap();
    let s2 = b.mutate(b"x2").put("f", b"q", b"2").commit().unwrap();
    let c = db
        .table("c")
        .unwrap()
        .family("g", Family::default())
        .create()
        .unwrap();
    let s3 = c.mutate(b"k3").put("g", b"q", b"3").commit().unwrap();
    let got: Vec<(u64, String, String)> = (0..3)
        .map(|_| {
            let c = next(&mut feed);
            let ch = &c.changes()[0];
            (c.seqno(), ch.table().to_string(), ch.family().to_string())
        })
        .collect();
    assert_eq!(
        got,
        [
            (s1.seqno, "a".into(), "f".into()),
            (s2.seqno, "b".into(), "f".into()),
            (s3.seqno, "c".into(), "g".into())
        ]
    );
    let keyed: Vec<u64> = (0..2).map(|_| next(&mut keyed).seqno()).collect();
    assert_eq!(keyed, [s1.seqno, s3.seqno], "the prefix still applies");
    let token = feed.token();

    // A table is refused: the feed already follows every table.
    let e = db.feed().table(&a).open().unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument);
    drop((feed, a, b, c));
    db.close().unwrap();

    // An open with a table list: a feed of every table is refused there, and it ends the
    // history, so the token expires at the next open that feeds every table.
    let db = Pigeonhole::open(&path, fed().feed_tables(["a"])).unwrap();
    let e = db.feed().open().unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}");
    let a = db.table("a").unwrap().open().unwrap();
    a.mutate(b"k4").put("f", b"q", b"4").commit().unwrap();
    drop(a);
    db.close().unwrap();
    let db = Pigeonhole::open(&path, fed()).unwrap();
    let mut f = db.feed().resume(&token).open().unwrap();
    let e = f.next(WAIT).unwrap_err();
    assert_eq!(e.code(), ErrorCode::FeedExpired, "{e}");
    drop(f);
    db.close().unwrap();
}

/// simp724's S1 on #1487 (D315): a feed of every table resumes only from a token of one. A
/// table's feed sits past commits of other tables it never followed, so resuming every
/// table from its token would skip them: refused. The other way round is harmless, and an
/// all-tables token resumes an all-tables feed.
#[test]
fn a_feed_of_every_table_resumes_only_from_its_own_kind_of_token() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("tok.phdb"), fed()).unwrap();
    let (a, b) = tables(&db);
    let mut one = a.feed().open().unwrap();
    let mut all = db.feed().open().unwrap();
    let s1 = a.mutate(b"r1").put("f", b"q", b"1").commit().unwrap();
    let s2 = b.mutate(b"r2").put("f", b"q", b"2").commit().unwrap();
    assert_eq!(next(&mut one).seqno(), s1.seqno);
    assert_eq!(next(&mut all).seqno(), s1.seqno);
    let (table_token, all_token) = (one.token(), all.token());

    let e = db.feed().resume(&table_token).open().unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument, "{e}");

    let mut again = db.feed().resume(&all_token).open().unwrap();
    assert_eq!(
        next(&mut again).seqno(),
        s2.seqno,
        "every table, from where it was"
    );
    let s3 = a.mutate(b"r3").put("f", b"q", b"3").commit().unwrap();
    let mut sub = a.feed().resume(&all_token).open().unwrap();
    assert_eq!(
        next(&mut sub).seqno(),
        s3.seqno,
        "a table's feed from an all-tables token"
    );
    drop((one, all, again, sub, a, b));
    db.close().unwrap();
}

/// One event, as a short string: `c<seqno>:<table>/<family>` for a commit (its first
/// change), `+t[f,g]`, `+t/f` or `-t` for a catalog change's edits.
fn event(feed: &mut Feed) -> String {
    match feed
        .next_event(WAIT)
        .unwrap()
        .expect("an event within the wait")
    {
        FeedEvent::Commit(c) => {
            let ch = &c.changes()[0];
            format!("c{}:{}/{}", c.seqno(), ch.table(), ch.family())
        }
        FeedEvent::Catalog(change) => change
            .edits()
            .iter()
            .map(|e| match e {
                CatalogEdit::TableCreated {
                    table, families, ..
                } => {
                    let f: Vec<&str> = families.iter().map(|f| f.name.as_str()).collect();
                    format!("+{table}[{}]", f.join(","))
                }
                CatalogEdit::FamilyAdded { table, family, .. } => {
                    format!("+{table}/{}", family.name)
                }
                CatalogEdit::TableDropped { table, .. } => format!("-{table}"),
                other => panic!("an edit this test doesn't know: {other:?}"),
            })
            .collect::<Vec<_>>()
            .join(" "),
        other => panic!("an event this test doesn't know: {other:?}"),
    }
}

/// D315 PR 2b: `next_event` on a feed of every table delivers each catalog change in its
/// place among the commits: a created table with its families, an added family, a dropped
/// table, each named; a catalog change's seqno sits above every commit before it and at or
/// below every commit after. `next` passes the changes over but still names the commits of
/// a family added later, and a table's own feed never sees a catalog change.
#[test]
fn a_feed_of_every_table_delivers_catalog_changes_in_commit_order() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("events.phdb"), fed()).unwrap();
    let (a, _b) = tables(&db);
    let mut events = db.feed().open().unwrap();
    let mut commits = db.feed().open().unwrap();
    let mut placed = db.feed().open().unwrap();
    let mut one = a.feed().open().unwrap();
    let s1 = a.mutate(b"r1").put("f", b"q", b"1").commit().unwrap();
    let c = db
        .table("c")
        .unwrap()
        .family("g", Family::default())
        .family("n", Family::counter())
        .create()
        .unwrap();
    let s2 = c.mutate(b"r2").put("g", b"q", b"2").commit().unwrap();
    let c = db
        .table("c")
        .unwrap()
        .family("h", Family::default())
        .open()
        .unwrap();
    let s3 = c.mutate(b"r3").put("h", b"q", b"3").commit().unwrap();
    drop(c);
    db.drop_table("c").unwrap();
    let s4 = a.mutate(b"r4").put("f", b"q", b"4").commit().unwrap();

    let got: Vec<String> = (0..7).map(|_| event(&mut events)).collect();
    assert_eq!(
        got,
        [
            format!("c{}:a/f", s1.seqno),
            "+c[g,n]".into(),
            format!("c{}:c/g", s2.seqno),
            "+c/h".into(),
            format!("c{}:c/h", s3.seqno),
            "-c".into(),
            format!("c{}:a/f", s4.seqno),
        ]
    );
    assert!(events.try_next_event().unwrap().is_none(), "nothing more");

    // The seqnos place each change between the commits around it.
    let seqnos: Vec<(bool, u64)> = (0..7)
        .map(
            |_| match placed.next_event(WAIT).unwrap().expect("an event") {
                FeedEvent::Commit(c) => (false, c.seqno()),
                FeedEvent::Catalog(change) => (true, change.seqno()),
                other => panic!("{other:?}"),
            },
        )
        .collect();
    for w in seqnos.windows(3) {
        if let [(false, before), (true, change), (false, after)] = *w {
            assert!(before < change && change <= after, "{seqnos:?}");
        }
    }

    let got: Vec<String> = (0..4)
        .map(|_| {
            let c = next(&mut commits);
            let ch = &c.changes()[0];
            format!("c{}:{}/{}", c.seqno(), ch.table(), ch.family())
        })
        .collect();
    assert_eq!(
        got,
        [
            format!("c{}:a/f", s1.seqno),
            format!("c{}:c/g", s2.seqno),
            format!("c{}:c/h", s3.seqno),
            format!("c{}:a/f", s4.seqno),
        ],
        "`next` passes the catalog changes over"
    );
    for want in [s1.seqno, s4.seqno] {
        match one.next_event(WAIT).unwrap().expect("a commit") {
            FeedEvent::Commit(c) => assert_eq!(c.seqno(), want),
            other => panic!("a table's feed delivered {other:?}"),
        }
    }
    drop((events, commits, placed, one, a));
    db.close().unwrap();
}

/// D315 PR 2b: a token resumes just past the last event, a commit or a catalog change,
/// across a reopen; and a table that existed before the feed opened and was dropped after
/// the token is still named on resume, though the catalog no longer holds it (the `Catalog`
/// entry's names). A feed opened now passes over the changes before it, so the tables that
/// already exist are not created again.
#[test]
fn a_token_resumes_just_past_the_last_event_and_names_a_dropped_table() {
    let dir = pigeonhole::doc_support::temp_dir();
    let path = dir.join("resume.phdb");
    let db = Pigeonhole::open(&path, fed()).unwrap();
    let (a, b) = tables(&db);
    let mut feed = db.feed().open().unwrap();
    let s1 = a.mutate(b"r1").put("f", b"q", b"1").commit().unwrap();
    assert_eq!(event(&mut feed), format!("c{}:a/f", s1.seqno));
    let after_commit = feed.token();
    drop(b);
    db.drop_table("b").unwrap();
    let c = db
        .table("c")
        .unwrap()
        .family("g", Family::default())
        .create()
        .unwrap();
    let s2 = c.mutate(b"r2").put("g", b"q", b"2").commit().unwrap();
    assert_eq!(event(&mut feed), "-b");
    let after_change = feed.token();
    drop((feed, a, c));
    db.close().unwrap();

    let db = Pigeonhole::open(&path, fed()).unwrap();
    for (token, want) in [
        (&after_commit, vec!["-b".to_string(), "+c[g]".into()]),
        (&after_change, vec!["+c[g]".to_string()]),
    ] {
        let mut f = db.feed().resume(token).open().unwrap();
        let mut got: Vec<String> = want.iter().map(|_| event(&mut f)).collect();
        got.push(event(&mut f));
        let mut want = want.clone();
        want.push(format!("c{}:c/g", s2.seqno));
        assert_eq!(got, want);
        assert!(f.try_next_event().unwrap().is_none(), "nothing seen twice");
    }
    db.close().unwrap();
}

/// D315 PR 2b: a feed of every table from a snapshot delivers exactly the catalog changes
/// the snapshot didn't see: read the snapshot, then follow the feed.
#[test]
fn a_feed_of_every_table_from_a_snapshot_starts_with_the_changes_it_did_not_see() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("snap.phdb"), fed()).unwrap();
    let (a, _b) = tables(&db);
    a.mutate(b"r0").put("f", b"q", b"0").commit().unwrap();
    let before = db.snapshot().unwrap();
    let c = db
        .table("c")
        .unwrap()
        .family("g", Family::default())
        .create()
        .unwrap();
    let after = db.snapshot().unwrap();
    let s1 = c.mutate(b"r1").put("g", b"q", b"1").commit().unwrap();
    let mut from_before = db.feed().from_snapshot(&before).open().unwrap();
    let mut from_after = db.feed().from_snapshot(&after).open().unwrap();
    drop((before, after));
    let commit = format!("c{}:c/g", s1.seqno);
    assert_eq!(
        [event(&mut from_before), event(&mut from_before)],
        ["+c[g]".to_string(), commit.clone()]
    );
    assert_eq!(event(&mut from_after), commit);
    assert!(from_after.try_next_event().unwrap().is_none());
    drop((from_before, from_after, a, c));
    db.close().unwrap();
}

/// ICR 0114 amendment (for `phdb serve`'s hubs, #1518): `Feed::position` covers every
/// delivered commit and moves while only other tables write, and a table created again
/// under its old name has another `feed_id`.
#[test]
fn a_feed_s_position_and_a_table_s_feed_id() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("pos.phdb"), fed()).unwrap();
    let (a, b) = tables(&db);
    let mut feed = a.feed().open().unwrap();
    let start = feed.position();
    let s1 = a.mutate(b"r1").put("f", b"q", b"1").commit().unwrap();
    assert_eq!(next(&mut feed).seqno(), s1.seqno);
    assert!(feed.position().seqno() >= s1.seqno);
    assert!(feed.position().covers(&start));
    let other = b.mutate(b"r2").put("f", b"q", b"2").commit().unwrap();
    let deadline = std::time::Instant::now() + WAIT;
    while feed.position().seqno() < other.seqno {
        assert!(feed.try_next().unwrap().is_none(), "nothing of a's");
        assert!(
            std::time::Instant::now() < deadline,
            "{:?}",
            feed.position()
        );
        std::thread::yield_now();
    }
    assert_eq!(feed.position().catalog_sequence(), 0);

    let id = b.feed_id();
    assert_ne!(id, a.feed_id());
    drop(b);
    db.drop_table("b").unwrap();
    let b = db
        .table("b")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    assert_ne!(b.feed_id(), id, "ids are never reused");
    drop((feed, a, b));
    db.close().unwrap();
}
