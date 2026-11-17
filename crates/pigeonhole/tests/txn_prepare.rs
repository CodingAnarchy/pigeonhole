//! D329 (ICR 0140), the hidden `__unstable-txn-prepare` API: a transaction prepared for an
//! external coordinator is in doubt across a close and a reopen, and decided by the caller.
#![cfg(feature = "__unstable-txn-prepare")]

use pigeonhole::{Durability, Family, Options, Pigeonhole};

#[test]
fn a_prepared_transaction_is_decided_after_a_reopen() {
    let dir = pigeonhole::doc_support::temp_dir();
    let path = dir.join("p.phdb");
    {
        let db = Pigeonhole::open(&path, Options::default().shards(2)).unwrap();
        let t = db
            .table("t")
            .unwrap()
            .family("f", Family::default())
            .create_if_missing()
            .unwrap();
        let mut txn = db.transaction().unwrap();
        for row in [&b"a"[..], b"m", b"z"] {
            txn.put(&t, row, "f", b"q", b"v");
        }
        let p = txn.prepare(b"home:h1-01", Durability::GroupSync).unwrap();
        assert!(p.seqno > 0);
        assert!(
            t.get(b"a", "f", b"q").unwrap().is_none(),
            "in doubt: not visible"
        );
        drop(t);
        db.close().unwrap();
    }
    let db = Pigeonhole::open(&path, Options::default().shards(2)).unwrap();
    let doubt: Vec<Vec<u8>> = db.in_doubt().into_iter().map(|(x, _)| x).collect();
    assert_eq!(doubt, [b"home:h1-01".to_vec()]);
    assert!(db.decide(b"home:h1-01", true).unwrap().is_some());
    assert_eq!(
        db.decide(b"home:h1-01", true).unwrap(),
        None,
        "decided already"
    );
    let t = db.table("t").unwrap().open().unwrap();
    assert_eq!(t.get(b"m", "f", b"q").unwrap().unwrap().value(), b"v");
    drop(t);
    db.close().unwrap();
}

/// cli692's M2 on D329 and perf2's E1 (ICR 0140): a share in doubt never holds a feed back.
/// Commits after it arrive while it waits, it arrives at its decision's seqno once committed,
/// and an abort is passed over with no event.
#[test]
fn a_share_in_doubt_never_holds_the_feed_back() {
    use pigeonhole::FeedRetention;
    let wait = std::time::Duration::from_secs(10);
    let dir = pigeonhole::doc_support::temp_dir();
    let opts = Options::default()
        .shards(2)
        .change_feed(FeedRetention::bytes(16 << 20));
    let db = Pigeonhole::open(dir.join("f.phdb"), opts).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    let mut feed = t.feed().open().unwrap();
    let mut txn = db.transaction().unwrap();
    txn.put(&t, b"held", "f", b"q", b"in doubt");
    txn.prepare(b"x:h1-01", Durability::GroupSync).unwrap();
    // Later commits arrive while the share is in doubt.
    let later = t.mutate(b"after").put("f", b"q", b"v").commit().unwrap();
    let c = feed
        .next(wait)
        .unwrap()
        .expect("a later commit arrives")
        .clone();
    assert_eq!(c.seqno(), later.seqno);
    assert_eq!(c.changes()[0].row(), b"after");
    // Committed: the share arrives, at its decision's seqno, above the later commit.
    let decided = db.decide(b"x:h1-01", true).unwrap().unwrap();
    let c = feed.next(wait).unwrap().expect("the decided share").clone();
    assert_eq!(c.seqno(), decided.seqno);
    assert_eq!(c.changes()[0].row(), b"held");
    // Aborted: nothing arrives for it, and the next commit does.
    let mut txn = db.transaction().unwrap();
    txn.put(&t, b"dropped", "f", b"q", b"never");
    txn.prepare(b"x:h1-02", Durability::GroupSync).unwrap();
    db.decide(b"x:h1-02", false).unwrap().unwrap();
    let last = t.mutate(b"last").put("f", b"q", b"v").commit().unwrap();
    let c = feed
        .next(wait)
        .unwrap()
        .expect("the commit after the abort")
        .clone();
    assert_eq!(c.seqno(), last.seqno);
    assert_eq!(c.changes()[0].row(), b"last");
    drop((feed, t));
    db.close().unwrap();
}
