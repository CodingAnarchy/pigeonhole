//! D329 engine PR 1 (ICR 0140): transactions prepared for an external coordinator. A
//! prepare logs and holds the shares, and its decision waits for `Engine::decide`: a commit
//! applies them, an abort drops them, and a second decide answers `None`.

mod common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, EngineOptions, Error, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";

fn owned(vfs: Arc<SimVfs>, shards: usize) -> EngineOptions {
    let mut o = common::options(vfs, shards, 4 << 20);
    o.pin_threads = false;
    o
}

fn get_bytes(db: &Engine, t: &pigeonhole_engine::TableInfo, row: &[u8]) -> Option<Vec<u8>> {
    let snap = db.snapshot().unwrap();
    let f = t.family("f").unwrap().id;
    db.get(&snap, t.id, f, row, b"q")
        .unwrap()
        .map(|c| common::value_bytes(c.value()))
}

/// A transaction that reads `read` and writes `value` to each of `rows`.
fn txn(
    db: &Engine,
    t: &pigeonhole_engine::TableInfo,
    read: &[u8],
    rows: &[&[u8]],
    value: &[u8],
) -> pigeonhole_engine::Txn {
    let f = t.family("f").unwrap().id;
    let mut txn = db.begin().unwrap();
    let _ = txn.get(t.id, f, read, b"q").unwrap();
    for row in rows {
        txn.batch()
            .put(t.id, f, row, b"q", None, ValueRef::Bytes(value))
            .unwrap();
    }
    txn
}

#[test]
fn a_prepared_transaction_waits_for_its_decision_then_commits_or_aborts() {
    // Two shards, rows spread over both: the shares are on each.
    let vfs = SimVfs::new(21);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let rows: Vec<Vec<u8>> = (0..16).map(|i| format!("row{i:02}").into_bytes()).collect();
    let rows: Vec<&[u8]> = rows.iter().map(Vec::as_slice).collect();

    // Prepared: nothing visible, the xid in doubt.
    let p = txn(&db, &t, b"row00", &rows, b"one")
        .prepare(b"x1", Durability::GroupSync)
        .unwrap();
    assert!(p.seqno > 0);
    assert!(rows.iter().all(|r| get_bytes(&db, &t, r).is_none()));
    let doubt: Vec<Vec<u8>> = db.in_doubt().into_iter().map(|(x, _)| x).collect();
    assert_eq!(doubt, [b"x1".to_vec()]);
    // The same xid again: refused while it's in doubt.
    assert!(matches!(
        txn(&db, &t, b"row00", &rows[..1], b"dup").prepare(b"x1", Durability::GroupSync),
        Err(Error::InvalidArgument(_))
    ));

    // Committed: every share applied, at a seqno above the id.
    let c = db.decide(b"x1", true).unwrap().expect("in doubt");
    assert!(c.seqno > p.seqno);
    assert!(
        rows.iter()
            .all(|r| get_bytes(&db, &t, r).as_deref() == Some(&b"one"[..]))
    );
    assert!(db.in_doubt().is_empty());
    // Decided already: `None`, idempotent.
    assert_eq!(db.decide(b"x1", true).unwrap(), None);
    assert_eq!(db.decide(b"x1", false).unwrap(), None);

    // Aborted: nothing applied, the rows as they were.
    txn(&db, &t, b"row00", &rows, b"two")
        .prepare(b"x2", Durability::GroupSync)
        .unwrap();
    assert!(db.decide(b"x2", false).unwrap().is_some());
    assert!(
        rows.iter()
            .all(|r| get_bytes(&db, &t, r).as_deref() == Some(&b"one"[..]))
    );
    assert!(db.in_doubt().is_empty());

    // A plain commit after both: its rows are free (nothing held), and it's visible.
    let mut wb = WriteBatch::new();
    let f = t.family("f").unwrap().id;
    wb.put(t.id, f, b"row00", b"q", None, ValueRef::Bytes(b"three"))
        .unwrap();
    db.commit(wb, Some(Durability::GroupSync)).unwrap();
    assert_eq!(get_bytes(&db, &t, b"row00").as_deref(), Some(&b"three"[..]));
    db.close().unwrap();
}

#[test]
fn a_prepare_that_read_a_row_since_written_conflicts() {
    let vfs = SimVfs::new(22);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.family("f").unwrap().id;
    let x = txn(&db, &t, b"acct", &[b"other"], b"v");
    let mut wb = WriteBatch::new();
    wb.put(t.id, f, b"acct", b"q", None, ValueRef::Bytes(b"changed"))
        .unwrap();
    db.commit(wb, None).unwrap();
    assert!(matches!(
        x.prepare(b"x3", Durability::GroupSync),
        Err(Error::Conflict)
    ));
    assert!(db.in_doubt().is_empty());
    db.close().unwrap();
}
