//! #1585 (ICR 0143): `Index::pack` and index layout version 2. A packed index answers every
//! search as its unpacked twin does, through writes, deletes and packs in between; the first
//! pack's rules (explicit, refused under attached readers, followed by handles opened
//! before it); a pack that stops and resumes; packs beside rebuilds, repairs and writers.

mod common;

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::*;
use pigeonhole::{Family, Options, Pigeonhole, Table};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_text::{Error, Fuzziness, Hit, Index, IndexOptions, PackOptions, Query};

/// A database on the simulated device: no file, so a test with thousands of commits is
/// quick.
fn sim(seed: u64) -> Pigeonhole {
    let vfs = SimVfs::new(seed);
    Pigeonhole::open(
        "/db/text.phdb",
        Options::default().vfs(vfs as Arc<_>).shards(2),
    )
    .expect("open")
}

/// A source table `name` and an index `{name}_text` over it.
fn pair(db: &Pigeonhole, name: &str, opts: IndexOptions) -> (Table, Index) {
    let t = db
        .table(name)
        .unwrap()
        .family("body", Family::default())
        .create_if_missing()
        .unwrap();
    let index = Index::create(db, &format!("{name}_text"), source(&t), opts).expect("create");
    (t, index)
}

/// The index's state cell and layout, raw.
fn state(db: &Pigeonhole, index: &str) -> (Option<Vec<u8>>, Option<u64>) {
    let rows = db.table(index).unwrap().open().unwrap();
    let cell = |q: &[u8]| rows.get(b"G", "meta", q).unwrap();
    (
        cell(b"repair").map(|c| c.value().to_vec()),
        cell(b"layout").and_then(|c| c.as_u64()),
    )
}

/// A spread of queries over the test vocabulary: terms, several terms, phrases exact and
/// sloppy (a repeated word among them), fuzzy terms, and mixes.
fn queries(rng: &mut Rng) -> Vec<Query> {
    let (a, b, c) = (word(rng), word(rng), word(rng));
    vec![
        Query::new().term(a),
        Query::new().text("the"),
        Query::new().text(format!("{a} {b} {c}")),
        Query::new().phrase([a, b]),
        Query::new().phrase(["the", a]),
        Query::new().phrase(["the", "the"]),
        Query::new().phrase_with_slop([a, b, c], 2),
        Query::new().fuzzy(a, Fuzziness::Auto).term(b),
        Query::new().term(a).phrase([b, "the"]),
    ]
}

/// Both indexes answer every query the same: the same hits with the same scores, at a small
/// `k`, in full, and under id prefixes.
fn same(plain: &Index, packed: &Index, rng: &mut Rng, ctx: &str) {
    for q in queries(rng) {
        for (k, prefix) in [
            (3usize, None),
            (1000, None),
            (1000, Some(&b"d0"[..])),
            (5, Some(&b"d01"[..])),
            (1000, Some(&b"d012"[..])),
        ] {
            let run = |i: &Index| -> Vec<Hit> {
                let mut s = i.search(q.clone(), k);
                if let Some(p) = prefix {
                    s = s.id_prefix(p);
                }
                s.run().unwrap()
            };
            assert_eq!(
                run(packed),
                run(plain),
                "{ctx}: {q:?}, k {k}, prefix {prefix:?}"
            );
        }
    }
}

#[test]
fn a_packed_index_searches_as_its_unpacked_twin_through_writes_deletes_and_packs() {
    for seed in seeds(&[1, 1585]) {
        let ctx = |step: &str| format!("seed {seed}: {step}");
        let db = sim(seed);
        let (_, plain) = pair(&db, "plain", IndexOptions::new());
        let (_, packed) = pair(&db, "packed", IndexOptions::new());
        let mut rng = Rng::new(seed);
        let mut model: BTreeMap<Vec<u8>, String> = BTreeMap::new();
        let put = |rng: &mut Rng, model: &mut BTreeMap<Vec<u8>, String>, doc: Vec<u8>| {
            // "the" in every document: its postings pass one chunk of a pack (512 cells).
            let t = format!("the {}", text(rng, 15));
            plain.put(&doc, &t).unwrap();
            packed.put(&doc, &t).unwrap();
            model.insert(doc, t);
        };
        for i in 0..900 {
            put(&mut rng, &mut model, format!("d{i:04}").into_bytes());
        }
        same(&plain, &packed, &mut rng, &ctx("before any pack"));
        let mut packs = 0;
        for op in 0..400 {
            let doc = format!("d{:04}", rng.below(1100)).into_bytes();
            if rng.below(4) == 0 {
                let had = packed.delete(&doc).unwrap();
                assert_eq!(plain.delete(&doc).unwrap(), had, "{}", ctx("delete"));
                assert_eq!(model.remove(&doc).is_some(), had, "{}", ctx("delete"));
            } else {
                put(&mut rng, &mut model, doc);
            }
            if op % 40 == 0 {
                // A whole pack, or one stopped after a chunk or two (the next carries on).
                let stop = rng.below(3);
                let st = packed
                    .pack_with(PackOptions::new(), |st| {
                        if stop > 0 && st.terms >= stop {
                            ControlFlow::Break(())
                        } else {
                            ControlFlow::Continue(())
                        }
                    })
                    .unwrap();
                assert_eq!(
                    st.stopped,
                    packed.pack_pending().unwrap(),
                    "{}",
                    ctx("stop")
                );
                packs += 1;
            }
            if op % 20 == 0 {
                same(&plain, &packed, &mut rng, &ctx(&format!("op {op}")));
            }
            if op % 100 == 99 {
                check(&plain, &ctx(&format!("plain at op {op}")));
                check(&packed, &ctx(&format!("packed at op {op}")));
            }
        }
        assert!(packs > 5, "{packs}");
        // Against the reference, so the twins aren't wrong together.
        let bm = Bm25::new(&model);
        for terms in [vec!["the"], vec!["memory", "store"], vec!["term", "rank"]] {
            let got = packed
                .search(Query::new().text(terms.join(" ")), 5000)
                .run()
                .unwrap();
            same_scores(&got, &bm.scores(&terms, &[]), &ctx(&format!("{terms:?}")));
        }
        let phrase = vec!["the", "memory"];
        let got = packed
            .search(Query::new().phrase(phrase.clone()), 5000)
            .run()
            .unwrap();
        same_scores(&got, &bm.scores(&[], &[phrase]), &ctx("a phrase"));
        // A finished pack leaves the common terms in blocks, and no tombstone.
        packed.pack().unwrap();
        packed.pack().unwrap();
        let (p, v) = (plain.verify().unwrap(), packed.verify().unwrap());
        assert!(v.is_consistent(), "{v:?}");
        assert_eq!(
            (v.documents, v.terms, v.postings),
            (p.documents, p.terms, p.postings)
        );
        assert_eq!(v.tombstones, 0, "{v:?}");
        assert!(v.packed * 10 > v.postings * 9, "mostly packed: {v:?}");
        assert_eq!((p.packed, p.tombstones), (0, 0), "{p:?}");
        same(&plain, &packed, &mut rng, &ctx("the end"));
        db.close().unwrap();
    }
}

#[test]
fn only_a_pack_moves_an_index_to_version_2_and_older_handles_follow() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_, index) = pair(&db, "docs", IndexOptions::new());
    let older = Index::open(&db, "docs_text").unwrap();
    let mut rng = Rng::new(7);
    for i in 0..200 {
        index
            .put(
                format!("d{i:03}").as_bytes(),
                &format!("the {}", text(&mut rng, 8)),
            )
            .unwrap();
    }
    // Writes, a rebuild and a repair leave it version 1.
    index.rebuild().unwrap();
    index.repair().unwrap();
    assert_eq!(state(&db, "docs_text"), (None, Some(1)));
    assert!(!index.pack_pending().unwrap());
    let before = older.search("the", 1000).run().unwrap();
    assert_eq!(before.len(), 200);

    let st = index.pack().unwrap();
    assert!(!st.stopped);
    assert!(st.postings >= 200, "{st:?}");
    assert_eq!(state(&db, "docs_text"), (Some(b"v2".to_vec()), Some(2)));
    let v = index.verify().unwrap();
    assert!(v.is_consistent() && v.packed >= 200, "{v:?}");
    // The handle opened before the pack reads the blocks, and writes tombstones.
    assert_eq!(older.search("the", 1000).run().unwrap(), before);
    assert!(older.delete(b"d007").unwrap());
    older.put(b"d008", "memory only").unwrap();
    for i in [&index, &older] {
        let hits = i.search("the", 1000).run().unwrap();
        assert_eq!(hits.len(), 198);
        assert!(hits.iter().all(|h| h.id != b"d007" && h.id != b"d008"));
    }
    let v = index.verify().unwrap();
    assert!(v.is_consistent(), "{v:?}");
    assert!(v.tombstones >= 2, "{v:?}");
    // The next pack removes them, with what they hid.
    let st = index.pack().unwrap();
    assert!(st.tombstones >= 2, "{st:?}");
    let v = index.verify().unwrap();
    assert!(v.is_consistent(), "{v:?}");
    assert_eq!(v.tombstones, 0, "{v:?}");
    // A reopened index is still version 2, and searches the same.
    let again = Index::open(&db, "docs_text").unwrap();
    assert_eq!(
        again.search("the memory", 1000).run().unwrap(),
        index.search("the memory", 1000).run().unwrap()
    );
    db.close().unwrap();
}

#[test]
fn the_first_pack_is_refused_while_a_reader_is_attached_unless_allowed() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_, index) = pair(&db, "docs", IndexOptions::new());
    let mut rng = Rng::new(11);
    for i in 0..150 {
        index
            .put(
                format!("d{i:03}").as_bytes(),
                &format!("the {}", text(&mut rng, 8)),
            )
            .unwrap();
    }
    let reader =
        Pigeonhole::open_reader(dir.join("text.phdb"), pigeonhole::ReaderOptions::default())
            .unwrap();
    let read = Index::open_reader(&reader, "docs_text").unwrap();
    let before = read.search("the store", 1000).run().unwrap();
    let e = index.pack().unwrap_err();
    assert!(matches!(e, Error::PackRefused(_)), "{e}");
    assert_eq!(e.code(), pigeonhole::ErrorCode::Busy);
    assert!(e.to_string().contains("reader"), "{e}");
    assert_eq!(state(&db, "docs_text"), (None, Some(1)));
    assert!(!index.pack_pending().unwrap());

    index
        .pack_with(PackOptions::new().with_readers(true), |_| {
            ControlFlow::Continue(())
        })
        .unwrap();
    // The reader's handle, opened before the pack, reads the blocks.
    assert_eq!(read.search("the store", 1000).run().unwrap(), before);
    let v = read.verify().unwrap();
    assert!(v.is_consistent() && v.packed > 0, "{v:?}");
    // Once the index is version 2 a 1.2 reader can't open it, so later packs don't ask.
    index.put(b"new", "the vector").unwrap();
    index.pack().unwrap();
    assert_eq!(
        read.search("vector", 10).run().unwrap(),
        index.search("vector", 10).run().unwrap()
    );
    drop(read);
    drop(reader);
    db.close().unwrap();
}

#[test]
fn a_pack_waits_for_a_rebuild_and_a_rebuild_or_repair_resets_it() {
    let db = sim(13);
    let (_, index) = pair(&db, "docs", IndexOptions::new());
    let mut rng = Rng::new(13);
    let mut model = BTreeMap::new();
    for i in 0..700 {
        let (doc, t) = (
            format!("d{i:03}").into_bytes(),
            format!("the {}", text(&mut rng, 8)),
        );
        index.put(&doc, &t).unwrap();
        model.insert(doc, t);
    }
    // A stopped rebuild refuses a pack, at its start and part way.
    let st = index.rebuild_with(|_| ControlFlow::Break(())).unwrap();
    assert!(st.stopped);
    let e = index.pack().unwrap_err();
    assert!(matches!(e, Error::PackRefused(_)), "{e}");
    assert_eq!(state(&db, "docs_text"), (None, Some(1)));
    index
        .resume_rebuild_with(|_| ControlFlow::Continue(()))
        .unwrap();

    // A stopped pack keeps a cursor; a rebuild from the top drops it.
    let st = index
        .pack_with(PackOptions::new(), |_| ControlFlow::Break(()))
        .unwrap();
    assert!(st.stopped && index.pack_pending().unwrap());
    let packed = index.verify().unwrap().packed;
    assert!(packed > 0);
    // The rebuild compares against the blocks: nothing is rewritten as cells.
    let st = index.rebuild().unwrap();
    assert_eq!((st.documents, st.changed), (700, 0));
    assert!(!index.pack_pending().unwrap());
    assert_eq!(index.verify().unwrap().packed, packed);
    index.pack().unwrap();
    let v = index.verify().unwrap();
    assert!(v.is_consistent(), "{v:?}");
    let bm = Bm25::new(&model);
    let got = index
        .search(Query::new().text("the store"), 5000)
        .run()
        .unwrap();
    same_scores(&got, &bm.scores(&["the", "store"], &[]), "after the packs");

    // A bypassed change under a packed posting is put right by a rebuild, as a cell.
    let docs = db.table("docs").unwrap().open().unwrap();
    docs.mutate(b"d005")
        .put("body", b"text", b"vector vector")
        .commit()
        .unwrap();
    model.insert(b"d005".to_vec(), "vector vector".into());
    assert!(!index.verify().unwrap().is_consistent());
    assert_eq!(index.rebuild().unwrap().changed, 1);
    let v = index.verify().unwrap();
    assert!(v.is_consistent(), "{v:?}");
    let bm = Bm25::new(&model);
    for terms in [["the", "store"], ["vector", "the"]] {
        let got = index
            .search(Query::new().text(terms.join(" ")), 5000)
            .run()
            .unwrap();
        same_scores(&got, &bm.scores(&terms, &[]), "after the rebuild");
    }

    // A repair empties the blocks with the rest, and the index stays version 2.
    index
        .pack_with(PackOptions::new(), |_| ControlFlow::Break(()))
        .unwrap();
    index.repair().unwrap();
    assert!(!index.pack_pending().unwrap());
    assert_eq!(state(&db, "docs_text"), (Some(b"v2".to_vec()), Some(2)));
    let v = index.verify().unwrap();
    assert!(v.is_consistent(), "{v:?}");
    assert_eq!((v.packed, v.tombstones), (0, 0), "{v:?}");
    let got = index
        .search(Query::new().text("the store"), 5000)
        .run()
        .unwrap();
    same_scores(&got, &bm.scores(&["the", "store"], &[]), "after the repair");
    db.close().unwrap();
}

#[test]
fn packs_beside_writers_keep_the_index_exact() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_, index) = pair(&db, "docs", IndexOptions::new());
    for i in 0..300 {
        index
            .put(format!("seed{i:03}").as_bytes(), "the memory store")
            .unwrap();
    }
    index.pack().unwrap();
    let done = AtomicBool::new(false);
    let models: Vec<BTreeMap<Vec<u8>, String>> = std::thread::scope(|s| {
        let packer = {
            let (index, done) = (index.clone(), &done);
            s.spawn(move || {
                let mut packs = 0;
                while !done.load(Ordering::Relaxed) {
                    index.pack().unwrap();
                    packs += 1;
                }
                packs
            })
        };
        // Each writer has its own documents, so its last write of each is the model.
        let writers: Vec<_> = (0..3u64)
            .map(|w| {
                let index = index.clone();
                s.spawn(move || {
                    let mut rng = Rng::new(100 + w);
                    let mut model = BTreeMap::new();
                    for _ in 0..400 {
                        let doc = format!("w{w}-{:03}", rng.below(120)).into_bytes();
                        if rng.below(4) == 0 {
                            index.delete(&doc).unwrap();
                            model.remove(&doc);
                        } else {
                            let t = format!("the {}", text(&mut rng, 10));
                            index.put(&doc, &t).unwrap();
                            model.insert(doc, t);
                        }
                    }
                    model
                })
            })
            .collect();
        let models = writers.into_iter().map(|w| w.join().unwrap()).collect();
        done.store(true, Ordering::Relaxed);
        assert!(packer.join().unwrap() > 0);
        models
    });
    let mut model: BTreeMap<Vec<u8>, String> = (0..300)
        .map(|i| {
            (
                format!("seed{i:03}").into_bytes(),
                "the memory store".to_owned(),
            )
        })
        .collect();
    for m in models {
        model.extend(m);
    }
    check(&index, "after the races");
    index.pack().unwrap();
    let v = index.verify().unwrap();
    assert!(v.is_consistent(), "{v:?}");
    assert_eq!(v.documents, model.len() as u64);
    assert_eq!(v.tombstones, 0, "{v:?}");
    let bm = Bm25::new(&model);
    for terms in [vec!["the"], vec!["memory", "agent"]] {
        let got = index
            .search(Query::new().text(terms.join(" ")), 5000)
            .run()
            .unwrap();
        same_scores(&got, &bm.scores(&terms, &[]), &format!("{terms:?}"));
    }
    let phrase = vec!["the", "memory"];
    let got = index
        .search(Query::new().phrase(phrase.clone()), 5000)
        .run()
        .unwrap();
    same_scores(&got, &bm.scores(&[], &[phrase]), "a phrase");
    db.close().unwrap();
}

#[test]
fn an_index_without_positions_packs_too() {
    let db = sim(17);
    let (_, plain) = pair(&db, "plain", IndexOptions::new().positions(false));
    let (_, packed) = pair(&db, "packed", IndexOptions::new().positions(false));
    let mut rng = Rng::new(17);
    for i in 0..400 {
        let (doc, t) = (format!("d{i:04}").into_bytes(), text(&mut rng, 12));
        plain.put(&doc, &t).unwrap();
        packed.put(&doc, &t).unwrap();
        if i == 250 {
            packed.pack().unwrap();
        }
    }
    for i in (0..400).step_by(7) {
        let doc = format!("d{i:04}").into_bytes();
        assert_eq!(packed.delete(&doc).unwrap(), plain.delete(&doc).unwrap());
    }
    for q in ["the", "memory store agent", "term~ the"] {
        assert_eq!(
            packed.search(q, 1000).run().unwrap(),
            plain.search(q, 1000).run().unwrap(),
            "{q}"
        );
    }
    packed.pack().unwrap();
    let v = packed.verify().unwrap();
    assert!(
        v.is_consistent() && v.packed > 0 && v.tombstones == 0,
        "{v:?}"
    );
    for q in ["the", "memory store agent", "term~ the"] {
        assert_eq!(
            packed.search(q, 3).run().unwrap(),
            plain.search(q, 3).run().unwrap(),
            "{q}"
        );
    }
    db.close().unwrap();
}

#[test]
fn an_edit_that_keeps_a_terms_frequency_leaves_no_cell_a_pack_cannot_reach() {
    let db = sim(19);
    let (_, index) = pair(&db, "docs", IndexOptions::new());
    for i in 0..40 {
        index
            .put(format!("d{i:02}").as_bytes(), "the memory store")
            .unwrap();
    }
    index.pack().unwrap();
    // The same words in another order: every term keeps its frequency and the document its
    // length, so only the positions change. A version 2 writer writes the posting cell
    // beside the position cell, and a pack, which walks posting cells, takes both.
    index.put(b"d05", "store the memory").unwrap();
    assert_eq!(
        ids_of(
            &index
                .search(Query::new().phrase(["store", "the"]), 10)
                .run()
                .unwrap()
        ),
        [b"d05".to_vec()]
    );
    index.pack().unwrap();
    let rows = db.table("docs_text").unwrap().open().unwrap();
    let mut cells = rows
        .scan_prefix(b"P")
        .families(["post", "pos"])
        .cells()
        .unwrap();
    let mut left = Vec::new();
    while let Some((row, e)) = cells.next().unwrap() {
        left.push((row.to_vec(), e.family.to_owned(), e.qualifier.to_vec()));
    }
    assert!(left.is_empty(), "cells a pack left behind: {left:?}");
    let v = index.verify().unwrap();
    assert!(v.is_consistent(), "{v:?}");
    assert_eq!((v.packed, v.tombstones), (120, 0), "{v:?}");
    assert_eq!(
        ids_of(
            &index
                .search(Query::new().phrase(["store", "the"]), 10)
                .run()
                .unwrap()
        ),
        [b"d05".to_vec()]
    );
    assert_eq!(
        index
            .search(Query::new().phrase(["the", "memory"]), 100)
            .run()
            .unwrap()
            .len(),
        40
    );
    db.close().unwrap();
}

#[test]
fn list_names_the_text_indexes_and_no_other_table() {
    let db = sim(7);
    assert!(Index::list(&db).expect("list").is_empty());
    // `pair` makes a source table and its index: only the index is listed.
    let (_, a) = pair(&db, "a", IndexOptions::new());
    let (_, _b) = pair(&db, "b", IndexOptions::new().positions(false));
    let mut names = Index::list(&db).expect("list");
    names.sort();
    assert_eq!(names, ["a_text", "b_text"]);
    // Packed or not.
    a.put(b"d1", "one two").expect("put");
    a.pack().expect("pack");
    let mut names = Index::list(&db).expect("list");
    names.sort();
    assert_eq!(names, ["a_text", "b_text"]);
}

fn ids_of(hits: &[Hit]) -> Vec<Vec<u8>> {
    hits.iter().map(|h| h.id.clone()).collect()
}
