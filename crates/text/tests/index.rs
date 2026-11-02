//! D309 PR 1 (#1247, ICR 0105): the index against a brute-force BM25 over its source texts,
//! with positions and exact phrases; snapshots; the no-ops; stale hits and the bypass
//! contract; `rebuild`, `repair` and `verify`; concurrency; normalisation; tokenisers; the
//! query syntax.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use common::*;
use pigeonhole::RetryPolicy;
use pigeonhole_text::{
    Error, Index, IndexOptions, Normalization, Query, QueryErrorKind, Token, Tokenizer,
};

fn ids(hits: &[pigeonhole_text::Hit]) -> Vec<&[u8]> {
    hits.iter().map(|h| h.id.as_slice()).collect()
}

#[test]
fn searches_equal_a_brute_force_bm25_through_puts_overwrites_and_deletes() {
    for seed in seeds(&[1, 2, 3]) {
        let ctx = |step: &str| format!("seed {seed}: {step}");
        let dir = pigeonhole::doc_support::temp_dir();
        let db = open(&dir);
        let (_docs, index) = create(&db, IndexOptions::new());
        let mut rng = Rng::new(seed);
        let mut model: BTreeMap<Vec<u8>, String> = BTreeMap::new();
        for op in 0..300 {
            let doc = format!("d{}", rng.below(40)).into_bytes();
            if rng.below(5) == 0 {
                let had = index.delete(&doc).unwrap();
                assert_eq!(had, model.remove(&doc).is_some(), "{}", ctx("delete"));
            } else {
                let t = text(&mut rng, 16);
                index.put(&doc, &t).unwrap();
                model.insert(doc, t);
            }
            if op % 50 == 49 {
                check(&index, &ctx(&format!("op {op}")));
            }
            if op % 10 != 9 {
                continue;
            }
            let bm = Bm25::new(&model);
            // Terms, then a phrase drawn from a document, then both through the syntax.
            let terms: Vec<&str> = (0..1 + rng.below(3)).map(|_| word(&mut rng)).collect();
            let got = index
                .search(Query::new().text(terms.join(" ")), 1000)
                .run()
                .unwrap();
            same_scores(
                &got,
                &bm.scores(&terms, &[]),
                &ctx(&format!("terms {terms:?}")),
            );
            let Some(src) = model
                .values()
                .nth(rng.below(model.len().max(1) as u64) as usize)
            else {
                continue;
            };
            let w = words(src);
            if w.len() < 2 {
                continue;
            }
            let at = rng.below(w.len() as u64 - 1) as usize;
            let n = (2 + rng.below(2) as usize).min(w.len() - at);
            let phrase: Vec<&str> = w[at..at + n].iter().map(String::as_str).collect();
            let got = index
                .search(Query::new().phrase(phrase.clone()), 1000)
                .run()
                .unwrap();
            same_scores(
                &got,
                &bm.scores(&[], std::slice::from_ref(&phrase)),
                &ctx(&format!("phrase {phrase:?}")),
            );
            let syntax = format!("{} \"{}\"", terms[0], phrase.join(" "));
            let got = index.search(syntax.as_str(), 1000).run().unwrap();
            same_scores(
                &got,
                &bm.scores(&terms[..1], &[phrase]),
                &ctx(&format!("syntax {syntax}")),
            );
            // The top k is the full ranking's prefix.
            let top = index.search(syntax.as_str(), 3).run().unwrap();
            let full = index.search(syntax.as_str(), 1000).run().unwrap();
            assert_eq!(top, full[..full.len().min(3)], "{}", ctx("top k"));
        }
        check(&index, &ctx("end"));
        db.close().unwrap();
    }
}

#[test]
fn a_search_at_an_older_snapshot_sees_the_index_as_of_it() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new());
    index.put(b"a", "postgres meeting").unwrap();
    let before = db.snapshot().unwrap();
    index.put(b"a", "vector feed").unwrap();
    index.put(b"b", "postgres store").unwrap();
    let then = index
        .search("postgres", 10)
        .snapshot(&before)
        .run()
        .unwrap();
    assert_eq!(ids(&then), [&b"a"[..]]);
    let now = index.search("postgres", 10).run().unwrap();
    assert_eq!(ids(&now), [&b"b"[..]]);
    db.close().unwrap();
}

#[test]
fn an_identical_put_and_a_delete_of_nothing_change_nothing() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new());
    index.put(b"a", "memory store memory").unwrap();
    index.put(b"e", "").unwrap();
    let before = index.verify().unwrap();
    let scores = index.search("memory store", 10).run().unwrap();
    index.put(b"a", "memory store memory").unwrap();
    index.put(b"e", "").unwrap();
    assert!(!index.delete(b"nothing").unwrap());
    assert_eq!(index.verify().unwrap(), before);
    assert_eq!(index.search("memory store", 10).run().unwrap(), scores);
    assert_eq!(before.documents, 2, "an empty document is indexed too");
    db.close().unwrap();
}

#[test]
fn an_empty_document_keeps_the_document_count_exact() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new());
    let docs_now = |want: u64, step: &str| {
        let v = index.verify().unwrap();
        assert!(v.is_consistent(), "{step}: {v:?}");
        assert_eq!(v.documents, want, "{step}");
    };
    index.put(b"e", "").unwrap();
    docs_now(1, "an empty document is a document");
    index.put(b"e", "").unwrap();
    docs_now(1, "putting it again doesn't count it twice");
    index.put(b"e", "  ,  ").unwrap();
    docs_now(1, "text with no words is empty too");
    index.put(b"e", "memory store").unwrap();
    docs_now(1, "words replace the empty text");
    index.put(b"e", "").unwrap();
    docs_now(1, "and empty text replaces them");
    assert!(index.search("memory", 10).run().unwrap().is_empty());
    assert!(index.delete(b"e").unwrap());
    docs_now(0, "deleted");
    assert!(!index.delete(b"e").unwrap());
    docs_now(0, "deleted twice");
    db.close().unwrap();
}

#[test]
fn a_hit_whose_source_was_deleted_around_the_index_is_skipped_and_counted() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (docs, index) = create(&db, IndexOptions::new());
    index.put(b"a", "postgres postgres").unwrap();
    index.put(b"b", "postgres").unwrap();
    // A bypass: the source row deleted directly.
    docs.mutate(b"a").delete_row().commit().unwrap();
    let (hits, stats) = index.search("postgres", 10).run_with_stats().unwrap();
    assert_eq!(ids(&hits), [&b"b"[..]]);
    assert_eq!(stats.stale, 1);
    let v = index.verify().unwrap();
    assert!(v.source_mismatch > 0, "{v:?}");
    let r = index.repair().unwrap();
    assert_eq!(r.documents, 1);
    check(&index, "after repair");
    let (hits, stats) = index.search("postgres", 10).run_with_stats().unwrap();
    assert_eq!((ids(&hits), stats.stale), (vec![&b"b"[..]], 0));
    db.close().unwrap();
}

#[test]
fn a_bypass_then_the_same_text_stays_stale_until_repair() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (docs, index) = create(&db, IndexOptions::new());
    index.put(b"a", "old words").unwrap();
    docs.mutate(b"a")
        .put("body", b"text", b"new words")
        .commit()
        .unwrap();
    // The text now matches the source cell, and F{a} is present: a no-op (D309 rev 5).
    index.put(b"a", "new words").unwrap();
    assert!(index.verify().unwrap().source_mismatch > 0);
    assert_eq!(ids(&index.search("old", 10).run().unwrap()), [&b"a"[..]]);
    index.repair().unwrap();
    check(&index, "after repair");
    assert!(index.search("old", 10).run().unwrap().is_empty());
    assert_eq!(ids(&index.search("new", 10).run().unwrap()), [&b"a"[..]]);
    db.close().unwrap();
}

#[test]
fn create_backfills_a_source_column_and_rebuild_then_changes_nothing() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let t = docs(&db);
    let mut rng = Rng::new(7);
    let mut model = BTreeMap::new();
    for i in 0..60 {
        let doc = format!("d{i:02}").into_bytes();
        let s = text(&mut rng, 12);
        t.mutate(&doc)
            .put("body", b"text", s.as_bytes())
            .commit()
            .unwrap();
        model.insert(doc, s);
    }
    t.mutate(b"bad")
        .put("body", b"text", &[0xff, 0xfe])
        .commit()
        .unwrap();
    t.mutate(b"other")
        .put("body", b"title", b"not the column")
        .commit()
        .unwrap();
    let index = Index::create(&db, "docs_text", source(&t), IndexOptions::new()).unwrap();
    let v = index.verify().unwrap();
    assert!(v.is_consistent(), "{v:?}");
    assert_eq!((v.documents, v.invalid_utf8), (60, 1));
    let bm = Bm25::new(&model);
    let got = index.search("memory agent", 1000).run().unwrap();
    same_scores(&got, &bm.scores(&["memory", "agent"], &[]), "backfilled");
    let r = index.rebuild().unwrap();
    assert_eq!((r.documents, r.changed, r.invalid_utf8), (61, 0, 1));
    db.close().unwrap();
}

#[test]
fn concurrent_writes_keep_the_counters_exact() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new());
    // One document, eight writers: one retries at a time, and the last text wins.
    std::thread::scope(|s| {
        for i in 0..8 {
            let index = index.clone();
            s.spawn(move || {
                for j in 0..20 {
                    index.put(b"same", &format!("memory w{i} r{j}")).unwrap();
                }
            });
        }
    });
    check(&index, "one document");
    // Different documents sharing hub terms: no conflicts on the counters.
    std::thread::scope(|s| {
        for i in 0..8 {
            let index = index.clone();
            s.spawn(move || {
                for j in 0..20 {
                    index
                        .put(format!("d{i}-{j}").as_bytes(), "memory store the agent")
                        .unwrap();
                }
            });
        }
    });
    check(&index, "shared terms");
    let (hits, _) = index.search("store", 1000).run_with_stats().unwrap();
    assert_eq!(hits.len(), 160);
    // A delete racing a put of the same document, and a rebuild racing both.
    std::thread::scope(|s| {
        let a = index.clone();
        s.spawn(move || {
            for j in 0..50 {
                a.put(b"race", &format!("feed text {j}")).unwrap();
            }
        });
        let b = index.clone();
        s.spawn(move || {
            for _ in 0..50 {
                b.delete(b"race").unwrap();
            }
        });
        let c = index.clone();
        s.spawn(move || {
            for _ in 0..3 {
                c.rebuild().unwrap();
            }
        });
    });
    check(&index, "races");
    db.close().unwrap();
}

#[test]
fn writes_and_searches_wait_out_a_repair() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new());
    index.put(b"a", "postgres").unwrap();
    // An interrupted repair leaves its flag set (as a crash mid-repair would).
    let rows = db.table("docs_text").unwrap().open().unwrap();
    rows.mutate(b"G")
        .put("meta", b"repair", b"")
        .commit()
        .unwrap();
    assert!(matches!(index.put(b"b", "x"), Err(Error::Busy)));
    assert!(matches!(index.delete(b"a"), Err(Error::Busy)));
    assert!(matches!(
        index.search("postgres", 10).run(),
        Err(Error::Busy)
    ));
    index.repair().unwrap();
    check(&index, "repaired");
    assert_eq!(
        ids(&index.search("postgres", 10).run().unwrap()),
        [&b"a"[..]]
    );
    db.close().unwrap();
}

#[test]
fn without_positions_a_phrase_is_refused_and_terms_still_work() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new().positions(false));
    index.put(b"a", "march meeting").unwrap();
    check(&index, "no positions");
    assert!(matches!(
        index.search(r#""march meeting""#, 10).run(),
        Err(Error::InvalidArgument(_))
    ));
    assert_eq!(ids(&index.search("march", 10).run().unwrap()), [&b"a"[..]]);
    // A one-word phrase is a term.
    assert_eq!(
        ids(&index.search(r#""march""#, 10).run().unwrap()),
        [&b"a"[..]]
    );
    db.close().unwrap();
}

#[test]
fn a_document_over_max_terms_is_refused_before_anything_is_written() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (docs, index) = create(&db, IndexOptions::new().max_terms(3));
    assert!(matches!(
        index.put(b"a", "w x y z"),
        Err(Error::InvalidArgument(_))
    ));
    assert!(docs.get(b"a", "body", b"text").unwrap().is_none());
    index.put(b"a", "w x y y").unwrap();
    check(&index, "under the cap");
    db.close().unwrap();
}

#[test]
fn stop_words_keep_their_positions_in_phrases() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new().stop_words(true));
    index.put(b"gap", "state of the art").unwrap();
    index.put(b"tight", "state art").unwrap();
    let hits = index.search(r#""state of the art""#, 10).run().unwrap();
    assert_eq!(ids(&hits), [&b"gap"[..]]);
    // A builder phrase's words are consecutive, with no gaps.
    let hits = index
        .search(Query::new().phrase(["state", "art"]), 10)
        .run()
        .unwrap();
    assert_eq!(ids(&hits), [&b"tight"[..]]);
    assert!(index.search("the", 10).run().unwrap().is_empty());
    db.close().unwrap();
}

#[test]
fn normalisation_matches_any_composition_and_builder_terms_are_normalised() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let t = docs(&db);
    let nfc = Index::create(&db, "nfc", source(&t), IndexOptions::new()).unwrap();
    let none = Index::create(
        &db,
        "none",
        source(&t),
        IndexOptions::new().normalization(Normalization::None),
    )
    .unwrap();
    let nfkc = Index::create(
        &db,
        "nfkc",
        source(&t),
        IndexOptions::new().normalization(Normalization::Nfkc),
    )
    .unwrap();
    let composed = "caf\u{e9} \u{fb01}le";
    let decomposed = "cafe\u{301}";
    // One source column, three indexes: write through each in one transaction.
    db.transact(&RetryPolicy::default(), |tx| {
        nfc.put_in(tx, b"a", composed)?;
        Ok::<_, Error>(())
    })
    .unwrap();
    for i in [&none, &nfkc] {
        i.rebuild().unwrap();
    }
    for i in [&nfc, &none, &nfkc] {
        check(i, "normalised");
    }
    let found = |i: &Index, q: Query| !i.search(q, 10).run().unwrap().is_empty();
    assert!(found(&nfc, Query::new().text(decomposed)));
    assert!(!found(&none, Query::new().text(decomposed)));
    assert!(
        found(&nfc, Query::new().term(decomposed)),
        "a builder term is normalised"
    );
    assert!(
        found(&nfkc, Query::new().text("file")),
        "NFKC folds the ligature"
    );
    assert!(!found(&nfc, Query::new().text("file")));
    // Full-width: tokenised text is lowercased after NFKC; a builder term isn't.
    nfkc.put(b"w", "\u{ff21}").unwrap();
    assert!(found(&nfkc, Query::new().text("a")));
    assert!(!found(&nfkc, Query::new().term("\u{ff21}")));
    assert!(found(&nfkc, Query::new().term("a")));
    db.close().unwrap();
}

/// Splits on spaces, keeping case.
struct Spaces(u64);

impl Tokenizer for Spaces {
    fn name(&self) -> &str {
        "spaces"
    }
    fn fingerprint(&self) -> u64 {
        self.0
    }
    fn tokens(&self, text: &str, out: &mut dyn FnMut(Token<'_>)) {
        let mut at = 0;
        for (i, w) in text.split(' ').enumerate() {
            if !w.is_empty() {
                out(Token::new(w, i as u32, at..at + w.len()));
            }
            at += w.len() + 1;
        }
    }
}

#[test]
fn an_index_opens_only_with_its_own_tokeniser() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let t = docs(&db);
    let index = Index::create_with_tokenizer(
        &db,
        "custom",
        source(&t),
        IndexOptions::new(),
        Box::new(Spaces(1)),
    )
    .unwrap();
    index.put(b"a", "Case Kept").unwrap();
    assert_eq!(ids(&index.search("Case", 10).run().unwrap()), [&b"a"[..]]);
    assert!(index.search("case", 10).run().unwrap().is_empty());
    assert!(matches!(
        Index::open(&db, "custom"),
        Err(Error::Mismatch(_))
    ));
    assert!(matches!(
        Index::open_with_tokenizer(&db, "custom", Box::new(Spaces(2))),
        Err(Error::Mismatch(_))
    ));
    let again = Index::open_with_tokenizer(&db, "custom", Box::new(Spaces(1))).unwrap();
    check(&again, "reopened");
    let builtin = Index::create(&db, "builtin", source(&t), IndexOptions::new()).unwrap();
    assert!(matches!(
        Index::open_with_tokenizer(&db, "builtin", Box::new(Spaces(1))),
        Err(Error::Mismatch(_))
    ));
    drop(builtin);
    db.close().unwrap();
}

#[test]
fn a_reopened_index_keeps_its_options_and_postings() {
    let dir = pigeonhole::doc_support::temp_dir();
    {
        let db = open(&dir);
        let (_docs, index) = create(&db, IndexOptions::new().k1(0.9).b(0.4).positions(false));
        index.put(b"a", "memory store").unwrap();
        db.close().unwrap();
    }
    let db = open(&dir);
    let index = Index::open(&db, "docs_text").unwrap();
    assert_eq!(
        index.options(),
        IndexOptions::new().k1(0.9).b(0.4).positions(false)
    );
    assert_eq!(ids(&index.search("store", 10).run().unwrap()), [&b"a"[..]]);
    check(&index, "reopened");
    db.close().unwrap();
}

#[test]
fn query_strings_are_parsed_when_the_search_runs() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new());
    index.put(b"a", "postgres").unwrap();
    for (q, at, reason) in [
        (r#"postgres "open"#, 9, QueryErrorKind::UnclosedQuote),
        (r#""a b"~x"#, 5, QueryErrorKind::Slop),
        (r#""a~ b""#, 2, QueryErrorKind::FuzzyInPhrase),
        ("postgres~3", 8, QueryErrorKind::Distance),
    ] {
        match index.search(q, 10).run() {
            Err(Error::InvalidQuery {
                at: a, reason: r, ..
            }) => {
                assert_eq!((a, r), (at, reason), "{q}")
            }
            other => panic!("{q}: {other:?}"),
        }
    }
    // Fuzzy terms and slop parse, and wait for their PRs.
    assert!(matches!(
        index.search("postgres~", 10).run(),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        index.search(r#""a b"~2"#, 10).run(),
        Err(Error::InvalidArgument(_))
    ));
    assert!(index.search("", 10).run().unwrap().is_empty());
    assert!(index.search("postgres", 0).run().unwrap().is_empty());
    db.close().unwrap();
}

#[test]
fn put_in_joins_the_callers_transaction() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (docs, index) = create(&db, IndexOptions::new());
    let index = Arc::new(index);
    // Given up: nothing of the index's lands either.
    let r = db.transact(&RetryPolicy::default(), |tx| {
        tx.put(&docs, b"a", "body", b"title", b"t");
        index.put_in(tx, b"a", "postgres")?;
        Err::<(), _>(Error::InvalidArgument("the application gave up".into()))
    });
    assert!(r.is_err());
    assert!(index.search("postgres", 10).run().unwrap().is_empty());
    assert!(docs.get(b"a", "body", b"title").unwrap().is_none());
    check(&index, "given up");
    db.transact(&RetryPolicy::default(), |tx| {
        tx.put(&docs, b"a", "body", b"title", b"t");
        index.put_in(tx, b"a", "postgres")?;
        Ok::<_, Error>(())
    })
    .unwrap();
    assert_eq!(
        ids(&index.search("postgres", 10).run().unwrap()),
        [&b"a"[..]]
    );
    check(&index, "committed");
    db.close().unwrap();
}
