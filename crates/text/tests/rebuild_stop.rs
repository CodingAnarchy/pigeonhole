//! #1756: a rebuild that its step stops (`rebuild_with`), and resumes from the cursor it
//! keeps in the index (`resume_rebuild_with`); `rebuild` and `repair` start over and drop
//! the cursor (ai1249's musts on the design), and an already-right index costs one cursor
//! commit a page (ai1249's Q2).

mod common;

use std::collections::BTreeMap;
use std::ops::ControlFlow;

use common::*;
use pigeonhole::{Pigeonhole, Table};
use pigeonhole_text::{Index, IndexOptions, Query};

/// More than two of the rebuild's pages (256 documents each).
const DOCS: usize = 600;

fn doc(i: usize) -> Vec<u8> {
    format!("d{i:04}").into_bytes()
}

/// `DOCS` source rows written around the index, so a rebuild has every one to index: the
/// texts by document.
fn bypassed(docs: &Table, seed: u64) -> BTreeMap<Vec<u8>, String> {
    let mut rng = Rng::new(seed);
    let mut model = BTreeMap::new();
    for i in 0..DOCS {
        let t = text(&mut rng, 10);
        docs.mutate(&doc(i))
            .put("body", b"text", t.as_bytes())
            .commit()
            .unwrap();
        model.insert(doc(i), t);
    }
    model
}

/// A rebuild stopped once it has visited `n` documents.
fn stop_after(index: &Index, n: u64) {
    let r = index
        .rebuild_with(|s| {
            if s.documents >= n {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .unwrap();
    assert!(r.stopped, "{r:?}");
    assert_eq!(r.documents, n);
    assert!(index.rebuild_pending().unwrap());
}

/// The index answers as a brute-force BM25 over `model` does, and verifies.
fn matches(index: &Index, model: &BTreeMap<Vec<u8>, String>, ctx: &str) {
    check(index, ctx);
    assert_eq!(
        index.verify().unwrap().documents,
        model.len() as u64,
        "{ctx}"
    );
    let bm = Bm25::new(model);
    for terms in [&["memory"][..], &["agent", "store"]] {
        let got = index
            .search(Query::new().text(terms.join(" ")), 10_000)
            .run()
            .unwrap();
        same_scores(&got, &bm.scores(terms, &[]), &format!("{ctx}: {terms:?}"));
    }
}

fn setup() -> (pigeonhole::doc_support::TempDir, Pigeonhole, Table, Index) {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (docs, index) = create(&db, IndexOptions::new());
    (dir, db, docs, index)
}

#[test]
fn a_stopped_rebuild_resumes_to_what_one_full_rebuild_gives() {
    let (_dir, db, docs, index) = setup();
    let model = bypassed(&docs, 1);
    stop_after(&index, 300);
    assert_eq!(Index::pending_rebuilds(&db).unwrap(), ["docs_text"]);
    // Every document the stopped walk visited changed, so its cursor is at the 300th: the
    // resume visits the rest.
    let r = index
        .resume_rebuild_with(|_| ControlFlow::Continue(()))
        .unwrap();
    assert!(!r.stopped, "{r:?}");
    assert_eq!(
        (r.documents, r.changed),
        ((DOCS - 300) as u64, (DOCS - 300) as u64)
    );
    assert!(
        !index.rebuild_pending().unwrap(),
        "the cursor goes at the end"
    );
    assert!(Index::pending_rebuilds(&db).unwrap().is_empty());
    matches(&index, &model, "resumed");
    // A resume with nothing pending does nothing.
    let r = index
        .resume_rebuild_with(|_| ControlFlow::Continue(()))
        .unwrap();
    assert_eq!(r.documents, 0);
    db.close().unwrap();
}

#[test]
fn writes_through_the_index_between_a_stop_and_its_resume_stay_right() {
    let (_dir, db, docs, index) = setup();
    let mut model = bypassed(&docs, 2);
    stop_after(&index, 300);
    // Before the cursor, after it, and a delete before it: through the index.
    for (i, t) in [
        (5, "memory before the cursor"),
        (500, "memory after the cursor"),
    ] {
        index.put(&doc(i), t).unwrap();
        model.insert(doc(i), t.to_owned());
    }
    assert!(index.delete(&doc(7)).unwrap());
    model.remove(&doc(7));
    index
        .resume_rebuild_with(|_| ControlFlow::Continue(()))
        .unwrap();
    matches(&index, &model, "writes between stop and resume");
    db.close().unwrap();
}

#[test]
fn a_rebuild_stopped_before_its_first_page_cursor_is_still_pending() {
    let (_dir, db, _docs, index) = setup();
    // An index already right: no document writes, so no cursor of its own before a page ends.
    let mut model = BTreeMap::new();
    for i in 0..DOCS {
        index.put(&doc(i), "memory agent store").unwrap();
        model.insert(doc(i), "memory agent store".to_owned());
    }
    // Stopped at once: the empty cursor from its start says "from the top".
    stop_after(&index, 1);
    let r = index
        .resume_rebuild_with(|_| ControlFlow::Continue(()))
        .unwrap();
    assert_eq!((r.documents, r.changed), (DOCS as u64, 0));
    assert!(!index.rebuild_pending().unwrap());
    matches(&index, &model, "resumed from the top");
    db.close().unwrap();
}

#[test]
fn a_repair_after_a_stopped_rebuild_walks_everything_and_drops_the_cursor() {
    // ai1249's must 1: the repair's emptying keeps `G`, so a cursor left there would make
    // its walk skip the rows before it.
    let (_dir, db, docs, index) = setup();
    let model = bypassed(&docs, 3);
    stop_after(&index, 300);
    let r = index.repair().unwrap();
    assert_eq!(r.documents, DOCS as u64);
    assert!(!index.rebuild_pending().unwrap());
    matches(&index, &model, "repaired");
    db.close().unwrap();
}

#[test]
fn a_rebuild_after_a_stop_starts_over_and_a_resume_would_not() {
    // ai1249's must 2: rows written around the index before the cursor, after the stop, are
    // what an operator's `rebuild` is for; a resume passes them by.
    let (_dir, db, docs, index) = setup();
    let mut model = bypassed(&docs, 4);
    stop_after(&index, 300);
    for i in [10, 20] {
        let t = format!("memory agent rewritten {i}");
        docs.mutate(&doc(i))
            .put("body", b"text", t.as_bytes())
            .commit()
            .unwrap();
        model.insert(doc(i), t);
    }
    // A resume finishes the walk but leaves those two as they were.
    index
        .resume_rebuild_with(|_| ControlFlow::Continue(()))
        .unwrap();
    assert!(index.verify().unwrap().source_mismatch > 0);
    // Stopped again, then a full rebuild: from the top, so every row is right.
    stop_after(&index, 100);
    let r = index.rebuild().unwrap();
    assert_eq!(r.documents, DOCS as u64);
    assert!(!index.rebuild_pending().unwrap());
    matches(&index, &model, "rebuilt after a stop");
    db.close().unwrap();
}

#[test]
fn rebuilding_an_index_already_right_commits_only_the_page_cursors() {
    // ai1249's Q2: a document already right writes nothing; the cursor costs one commit a
    // full page, plus the empty cursor at the start and its removal at the end.
    let (_dir, db, docs, index) = setup();
    for i in 0..DOCS {
        index.put(&doc(i), "memory agent store").unwrap();
    }
    let _ = docs;
    let before = db.snapshot().unwrap().seqno();
    let r = index.rebuild().unwrap();
    let after = db.snapshot().unwrap().seqno();
    assert_eq!((r.documents, r.changed, r.stopped), (DOCS as u64, 0, false));
    let full_pages = (DOCS / 256) as u64;
    assert_eq!(after - before, full_pages + 2, "{r:?}");
    assert!(!index.rebuild_pending().unwrap());
    db.close().unwrap();
}

/// #1722: `create_unfilled` over a source column with rows makes an empty index with a
/// rebuild from the top pending. A put through it meanwhile is indexed at once, a stopped
/// resume carries on, and the resumed rebuild ends as `create` would have, the cursor gone.
#[test]
fn an_unfilled_index_is_filled_by_resuming_its_pending_rebuild() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let docs = docs(&db);
    let mut model = bypassed(&docs, 1722);
    let index =
        Index::create_unfilled(&db, "docs_text", source(&docs), IndexOptions::new()).unwrap();
    assert!(index.rebuild_pending().unwrap());
    assert_eq!(index.verify().unwrap().documents, 0);
    // A write through the index is there before the fill.
    index.put(b"new", "memory agent store").unwrap();
    model.insert(b"new".to_vec(), "memory agent store".into());
    assert_eq!(index.verify().unwrap().documents, 1);
    // A resume stopped part way, then resumed to the end.
    let r = index
        .resume_rebuild_with(|s| {
            if s.documents >= 300 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .unwrap();
    assert!(r.stopped && index.rebuild_pending().unwrap(), "{r:?}");
    let r = index
        .resume_rebuild_with(|_| ControlFlow::Continue(()))
        .unwrap();
    assert!(!r.stopped, "{r:?}");
    assert!(!index.rebuild_pending().unwrap());
    matches(&index, &model, "filled");
    db.close().unwrap();
}
