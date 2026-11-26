//! D336 (ICR 0147), the hidden `__unstable-tail-replay` API: what a partition's new owner
//! replays an old owner's tail commit with. A logged batch tagged with its origin, a
//! transaction's changes as their primary logged them beside its ordinary writes, and its
//! raw row read with delete markers.
#![cfg(feature = "__unstable-tail-replay")]

use std::time::Duration;

use pigeonhole::ChangeKind;
use pigeonhole::{ErrorCode, Family, FeedRetention, Options, Pigeonhole, Table};

fn open(dir: &std::path::Path) -> (Pigeonhole, Table) {
    let opts = Options::default()
        .shards(2)
        .change_feed(FeedRetention::bytes(16 << 20));
    let db = Pigeonhole::open(dir.join("t.phdb"), opts).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .family("n", Family::counter())
        .create_if_missing()
        .unwrap();
    (db, t)
}

/// A logged batch tagged with an xid: its commit's feed event delivers the tag, as a
/// spanning commit's does.
#[test]
fn a_tagged_logged_batch_delivers_its_xid() {
    let dir = pigeonhole::doc_support::temp_dir();
    let (db, t) = open(&dir);
    let mut feed = t.feed().open().unwrap();
    let mut b = db.logged_batch();
    b.push(&t, b"r", "f", ChangeKind::Put, b"q", 500, &[0x00, b'v'])
        .tag_xid(b"replay:h1:7");
    let info = b.commit().unwrap();
    let c = feed
        .next(Duration::from_secs(10))
        .unwrap()
        .expect("the replayed commit")
        .clone();
    assert_eq!(c.seqno(), info.seqno);
    assert_eq!(c.xid(), Some(&b"replay:h1:7"[..]));
    assert_eq!(c.changes()[0].timestamp(), 500);
    drop((feed, t));
    db.close().unwrap();
}

/// simp724 on D336 Amendment 3: a tail column delete at its timestamp, in a transaction,
/// leaves a version the new owner wrote since, which `delete_column` at the commit's
/// timestamp would remove. The transaction's ordinary writes beside it are rewritten as any
/// transaction's: a counter increment is prepared, not taken as logged.
#[test]
fn a_logged_delete_beside_ordinary_writes_keeps_a_newer_version() {
    let dir = pigeonhole::doc_support::temp_dir();
    let (db, t) = open(&dir);
    t.mutate(b"r")
        .put_at("f", b"q", 900, b"newer")
        .commit()
        .unwrap();
    t.mutate(b"r").incr("n", b"c", 2).commit().unwrap();
    let mut txn = db.transaction().unwrap();
    txn.push_logged(&t, b"r", "f", ChangeKind::DeleteColumn, b"q", 500, &[])
        .push_logged(&t, b"s", "f", ChangeKind::Put, b"q", 400, &[0x00, b'x'])
        .incr(&t, b"r", "n", b"c", 3)
        .put(&t, b"o", "f", b"q", b"ordinary");
    txn.commit().unwrap();
    let cell = t
        .get(b"r", "f", b"q")
        .unwrap()
        .expect("the newer version stays");
    assert_eq!((cell.value(), cell.timestamp()), (&b"newer"[..], 900));
    let s = t.get(b"s", "f", b"q").unwrap().unwrap();
    assert_eq!((s.value(), s.timestamp()), (&b"x"[..], 400));
    assert_eq!(t.get(b"r", "n", b"c").unwrap().unwrap().as_i64(), Some(5));
    assert_eq!(
        t.get(b"o", "f", b"q").unwrap().unwrap().value(),
        b"ordinary"
    );
    drop(t);
    db.close().unwrap();
}

/// simp724's must on D336 Amendment 3: the raw read shows a delete marker a plain read
/// hides, so a tail put below it is seen to be superseded.
#[test]
fn the_raw_row_read_shows_a_delete_marker() {
    let dir = pigeonhole::doc_support::temp_dir();
    let (db, t) = open(&dir);
    t.mutate(b"d")
        .put_at("f", b"q", 100, b"v")
        .commit()
        .unwrap();
    t.mutate(b"d").delete_column("f", b"q").commit().unwrap();
    let mut txn = db.transaction().unwrap();
    assert!(txn.get_row(&t, b"d", &["f"]).unwrap().is_none(), "hidden");
    let cells = txn.read_row_logged(&t, b"d").unwrap();
    let marker = cells
        .iter()
        .find(|c| c.kind == ChangeKind::DeleteColumn)
        .unwrap_or_else(|| panic!("a delete marker: {cells:?}"));
    assert_eq!(
        (marker.family.as_str(), &marker.qualifier[..]),
        ("f", &b"q"[..])
    );
    assert!(marker.ts > 100, "{marker:?}");
    drop((txn, t));
    db.close().unwrap();
}

/// The raw read counts as a read of the whole row: a write to another column of it after
/// the snapshot fails the transaction's commit, so a replay decides again on a rerun.
#[test]
fn a_write_to_the_row_after_the_raw_read_conflicts() {
    let dir = pigeonhole::doc_support::temp_dir();
    let (db, t) = open(&dir);
    t.mutate(b"r").put("f", b"a", b"v").commit().unwrap();
    let mut txn = db.transaction().unwrap();
    txn.read_row_logged(&t, b"r").unwrap();
    t.mutate(b"r")
        .put("f", b"other", b"racing")
        .commit()
        .unwrap();
    txn.push_logged(&t, b"r", "f", ChangeKind::Put, b"a", 50, &[0x00, b'x']);
    let e = txn.commit().unwrap_err();
    assert_eq!(e.code(), ErrorCode::Conflict, "{e}");
    drop(t);
    db.close().unwrap();
}

/// A transaction of logged changes alone commits them as logged: kind, timestamp and value.
#[test]
fn a_transaction_of_logged_changes_alone_commits() {
    let dir = pigeonhole::doc_support::temp_dir();
    let (db, t) = open(&dir);
    let mut txn = db.transaction().unwrap();
    txn.push_logged(&t, b"only", "f", ChangeKind::Put, b"q", 7, &[0x00, b'y']);
    txn.commit().unwrap();
    let c = t.get(b"only", "f", b"q").unwrap().unwrap();
    assert_eq!((c.value(), c.timestamp()), (&b"y"[..], 7));
    drop(t);
    db.close().unwrap();
}
