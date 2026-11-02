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
    Error, Fuzziness, Index, IndexOptions, Normalization, Query, QueryErrorKind, Token, Tokenizer,
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

/// #1291: phrases and fuzzy terms run on normalised terms, a decomposed document matches a
/// composed query (the reverse of the test above), and a reopen, as the writer and as a reader
/// process, keeps the index's setting.
#[test]
fn normalised_phrases_fuzzy_terms_and_reopens_match() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let t = docs(&db);
    let nfc = Index::create(&db, "nfc", source(&t), IndexOptions::new()).unwrap();
    let nfkc = Index::create(
        &db,
        "nfkc",
        source(&t),
        IndexOptions::new().normalization(Normalization::Nfkc),
    )
    .unwrap();
    // A decomposed "café" and an "office" with the `ﬁ` ligature.
    nfc.put(b"d", "the cafe\u{301} of\u{fb01}ce opens").unwrap();
    nfkc.rebuild().unwrap();
    for i in [&nfc, &nfkc] {
        check(i, "normalised");
    }
    let found = |i: &Index, q: Query| !i.search(q, 10).run().unwrap().is_empty();
    let composed = "caf\u{e9}";
    // The reverse direction: a decomposed document, a composed query.
    assert!(found(&nfc, Query::new().text(composed)));
    // Phrases: a quoted one and a builder one, composed against the decomposed source.
    assert!(
        !nfc.search("\"caf\u{e9} of\u{fb01}ce\"", 10)
            .run()
            .unwrap()
            .is_empty()
    );
    assert!(found(&nfc, Query::new().phrase([composed, "of\u{fb01}ce"])));
    // NFKC folds the ligature inside a phrase too; NFC keeps it, so the plain word misses.
    assert!(found(&nfkc, Query::new().phrase([composed, "office"])));
    assert!(
        !nfkc
            .search("\"caf\u{e9} office\"", 10)
            .run()
            .unwrap()
            .is_empty()
    );
    assert!(!found(&nfc, Query::new().phrase([composed, "office"])));
    // Fuzzy terms: one edit from the NFC term "café" (a precomposed `é` is one character, so
    // `a` for it is one substitution), and one edit from NFKC's "office" ("offica"), which is
    // two from NFC's five-character "ofﬁce".
    assert!(found(&nfc, Query::new().fuzzy("cafa", Fuzziness::One)));
    assert!(found(&nfkc, Query::new().fuzzy("offica", Fuzziness::One)));
    assert!(!found(&nfc, Query::new().fuzzy("offica", Fuzziness::One)));
    drop((nfc, nfkc, t));
    db.close().unwrap();

    // Reopened, the setting comes from the index: the writer's handle and a reader's.
    let db = open(&dir);
    let nfc = Index::open(&db, "nfc").unwrap();
    let nfkc = Index::open(&db, "nfkc").unwrap();
    assert_eq!(nfc.options().get_normalization(), Normalization::Nfc);
    assert_eq!(nfkc.options().get_normalization(), Normalization::Nfkc);
    assert!(found(&nfc, Query::new().text("cafe\u{301}")));
    assert!(found(&nfkc, Query::new().text("office")));
    let reader = pigeonhole::Pigeonhole::open_reader(
        dir.join("text.phdb"),
        pigeonhole::ReaderOptions::default(),
    )
    .unwrap();
    let read = Index::open_reader(&reader, "nfkc").unwrap();
    assert!(!read.search("office", 10).run().unwrap().is_empty());
    drop((read, reader, nfc, nfkc));
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
    // A fuzzy word parses and runs (D309 PR 4).
    assert_eq!(index.search("postgress~", 10).run().unwrap().len(), 1);
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

/// Whether some placement of `phrase`'s words in `text` (distinct positions for a repeated
/// word) spans at most `slop` once each word's offset is taken off: the match rule.
fn sloppy_rule(phrase: &[&str], text: &str, slop: u16) -> bool {
    let w = words(text);
    fn go(p: &[&str], w: &[String], i: usize, picked: &mut Vec<usize>, slop: i64) -> bool {
        if i == p.len() {
            let rel: Vec<i64> = picked
                .iter()
                .zip(0i64..)
                .map(|(&x, o)| x as i64 - o)
                .collect();
            return rel.iter().max().unwrap() - rel.iter().min().unwrap() <= slop;
        }
        (0..w.len()).any(|x| {
            if w[x] != p[i] || picked.contains(&x) {
                return false;
            }
            picked.push(x);
            let ok = go(p, w, i + 1, picked, slop);
            picked.pop();
            ok
        })
    }
    go(phrase, &w, 0, &mut Vec::new(), i64::from(slop))
}

#[test]
fn a_sloppy_phrase_matches_the_documents_the_rule_allows() {
    for seed in seeds(&[11, 12]) {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = open(&dir);
        let (_docs, index) = create(&db, IndexOptions::new());
        let mut rng = Rng::new(seed);
        let mut model: BTreeMap<Vec<u8>, String> = BTreeMap::new();
        for i in 0..60 {
            let t = text(&mut rng, 10);
            index.put(format!("d{i:02}").as_bytes(), &t).unwrap();
            model.insert(format!("d{i:02}").into_bytes(), t);
        }
        for q in 0..60 {
            let n = 2 + rng.below(2) as usize;
            let mut phrase: Vec<&str> = (0..n).map(|_| word(&mut rng)).collect();
            phrase.dedup();
            if phrase.len() < 2 {
                continue;
            }
            let slop = rng.below(4) as u16;
            let got = index
                .search(Query::new().phrase_with_slop(phrase.clone(), slop), 1000)
                .run()
                .unwrap();
            let hits: std::collections::BTreeSet<&[u8]> = ids(&got).into_iter().collect();
            for (doc, t) in &model {
                assert_eq!(
                    hits.contains(doc.as_slice()),
                    sloppy_rule(&phrase, t, slop),
                    "seed {seed}, query {q}: {phrase:?}~{slop} on {t:?}"
                );
            }
            // Slop 0 through the sloppy syntax is the exact phrase.
            if slop == 0 {
                let exact = index
                    .search(Query::new().phrase(phrase.clone()), 1000)
                    .run()
                    .unwrap();
                assert_eq!(got, exact, "seed {seed}, query {q}");
            }
        }
        check(&index, "sloppy");
        db.close().unwrap();
    }
}

#[test]
fn closer_sloppy_matches_score_higher_and_the_syntax_equals_the_builder() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new());
    // Equal lengths, so only the match's distance differs.
    index.put(b"0", "memory store x x").unwrap();
    index.put(b"1", "memory x store x").unwrap();
    index.put(b"2", "memory x x store").unwrap();
    index.put(b"r", "store memory x x").unwrap();
    let hits = index.search(r#""memory store"~2"#, 10).run().unwrap();
    assert_eq!(ids(&hits), [&b"0"[..], b"1", b"2", b"r"]);
    assert!(hits[1].score > hits[2].score, "distance 1 beats 2");
    assert_eq!(hits[2].score, hits[3].score, "reversed is distance 2");
    let built = index
        .search(Query::new().phrase_with_slop(["memory", "store"], 2), 10)
        .run()
        .unwrap();
    assert_eq!(built, hits);
    // Slop 1 drops the two at distance 2.
    let near = index.search(r#""memory store"~1"#, 10).run().unwrap();
    assert_eq!(ids(&near), [&b"0"[..], b"1"]);
    // An exact match scores the same with and without slop.
    let exact = index.search(r#""memory store""#, 10).run().unwrap();
    let sloppy0 = index.search(r#""memory store"~0"#, 10).run().unwrap();
    assert_eq!(exact, sloppy0);
    assert_eq!(exact[0].score, hits[0].score);
    db.close().unwrap();
}

#[test]
fn a_repeated_phrase_word_needs_two_occurrences() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new());
    index.put(b"one", "the cat sat").unwrap();
    index.put(b"two", "the cat sat on the mat").unwrap();
    let hits = index.search(r#""the cat the"~4"#, 10).run().unwrap();
    assert_eq!(ids(&hits), [&b"two"[..]]);
    db.close().unwrap();
}

/// Checks a top-`k` search against the reference's scores: the hits are the reference's best
/// `k` (by score, then id), with the same scores. Scores within 1e-9 count as tied, so either
/// side of a tie may make the cut.
#[test]
fn maxscore_returns_the_exact_top_k_at_small_k() {
    for seed in seeds(&[21, 22, 23]) {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = open(&dir);
        let (_docs, index) = create(&db, IndexOptions::new());
        let mut rng = Rng::new(seed);
        let mut model: BTreeMap<Vec<u8>, String> = BTreeMap::new();
        for i in 0..300 {
            let doc = format!("d{i:03}").into_bytes();
            let t = text(&mut rng, 24);
            index.put(&doc, &t).unwrap();
            model.insert(doc, t);
        }
        // Some deletes and overwrites, so the bounds' aggregates over-estimate (M1).
        for _ in 0..40 {
            let doc = format!("d{:03}", rng.below(300)).into_bytes();
            if rng.below(2) == 0 {
                index.delete(&doc).unwrap();
                model.remove(&doc);
            } else {
                let t = text(&mut rng, 4);
                index.put(&doc, &t).unwrap();
                model.insert(doc, t);
            }
        }
        let bm = Bm25::new(&model);
        for q in 0..80 {
            let n = 1 + rng.below(4) as usize;
            let terms: Vec<&str> = (0..n).map(|_| word(&mut rng)).collect();
            let phrases: Vec<Vec<&str>> = if rng.below(3) == 0 {
                vec![vec![word(&mut rng), word(&mut rng)]]
            } else {
                Vec::new()
            };
            let mut query = Query::new().text(terms.join(" "));
            for p in &phrases {
                query = query.phrase(p.clone());
            }
            let want = bm.scores(&terms, &phrases);
            for k in [1, 3, 10] {
                let got = index.search(query.clone(), k).run().unwrap();
                same_top_k(
                    &got,
                    &want,
                    k,
                    &format!("seed {seed}, query {q} {terms:?} {phrases:?}, k {k}"),
                );
            }
        }
        check(&index, "maxscore");
        db.close().unwrap();
    }
}

#[test]
fn an_id_prefix_restricts_the_search_and_keeps_its_scores() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new());
    let mut rng = Rng::new(31);
    for ns in ["agent:alice\0", "agent:alice2\0", "shared\0"] {
        for i in 0..40 {
            index
                .put(format!("{ns}m{i:02}").as_bytes(), &text(&mut rng, 12))
                .unwrap();
        }
    }
    let query = r#"memory agent "memory store""#;
    let all = index.search(query, 1000).run().unwrap();
    for ns in ["agent:alice\0", "agent:alice2\0", "shared\0"] {
        let want: Vec<_> = all
            .iter()
            .filter(|h| h.id.starts_with(ns.as_bytes()))
            .cloned()
            .collect();
        let got = index.search(query, 1000).id_prefix(ns).run().unwrap();
        assert_eq!(got, want, "{ns:?}");
        // At a small k, the prefix's own top k.
        let top = index.search(query, 3).id_prefix(ns).run().unwrap();
        assert_eq!(top, want[..want.len().min(3)], "{ns:?} top 3");
    }
    // The terminator matters: `agent:alice` alone also matches `agent:alice2`'s ids.
    let loose = index
        .search(query, 1000)
        .id_prefix("agent:alice")
        .run()
        .unwrap();
    assert!(loose.iter().any(|h| h.id.starts_with(b"agent:alice2")));
    assert!(
        index
            .search(query, 10)
            .id_prefix("nobody\0")
            .run()
            .unwrap()
            .is_empty()
    );
    db.close().unwrap();
}

#[test]
fn a_reader_process_searches_what_the_writer_committed() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let t = docs(&db);
    let index = Index::create(&db, "docs_text", source(&t), IndexOptions::new()).unwrap();
    let custom = Index::create_with_tokenizer(
        &db,
        "custom",
        source(&t),
        IndexOptions::new(),
        Box::new(Spaces(7)),
    )
    .unwrap();
    let mut rng = Rng::new(41);
    for i in 0..50 {
        index
            .put(format!("d{i:02}").as_bytes(), &text(&mut rng, 12))
            .unwrap();
    }
    custom.rebuild().unwrap();
    let reader = pigeonhole::Pigeonhole::open_reader(
        dir.join("text.phdb"),
        pigeonhole::ReaderOptions::default(),
    )
    .unwrap();
    let read = Index::open_reader(&reader, "docs_text").unwrap();
    assert_eq!(read.options(), index.options());
    assert_eq!(read.source(), index.source());
    for q in [
        r#"memory store "memory store""#,
        "agent",
        r#""the agent"~2 search"#,
    ] {
        let w = index.search(q, 7).run().unwrap();
        let r = read.search(q, 7).run().unwrap();
        assert_eq!(r, w, "{q}");
        let r = read.search(q, 100).id_prefix("d1").run().unwrap();
        assert_eq!(
            r,
            index.search(q, 100).id_prefix("d1").run().unwrap(),
            "{q} with a prefix"
        );
    }
    let v = read.verify().unwrap();
    assert!(v.is_consistent(), "{v:?}");
    assert_eq!(v.documents, 50);
    // A later commit shows in the reader's next search.
    index.put(b"new", "vector feed vector").unwrap();
    assert_eq!(ids(&read.search("vector", 1).run().unwrap()), [&b"new"[..]]);
    // A repair in the writer makes the reader's searches Busy.
    let rows = db.table("docs_text").unwrap().open().unwrap();
    rows.mutate(b"G")
        .put("meta", b"repair", b"")
        .commit()
        .unwrap();
    assert!(matches!(read.search("vector", 1).run(), Err(Error::Busy)));
    index.repair().unwrap();
    assert_eq!(ids(&read.search("vector", 1).run().unwrap()), [&b"new"[..]]);
    // The tokeniser checks hold at a reader's open too. (Both indexes share the source
    // column, so the writes above through `index` bypassed `custom`: rebuild it first.)
    custom.rebuild().unwrap();
    assert!(matches!(
        Index::open_reader(&reader, "custom"),
        Err(Error::Mismatch(_))
    ));
    let c = Index::open_reader_with_tokenizer(&reader, "custom", Box::new(Spaces(7))).unwrap();
    assert!(c.verify().unwrap().is_consistent());
    assert!(matches!(
        Index::open_reader(&reader, "docs"),
        Err(Error::InvalidArgument(_))
    ));
    // And the writer's open of a table that isn't an index.
    assert!(matches!(
        Index::open(&db, "docs"),
        Err(Error::InvalidArgument(_))
    ));
    drop(reader);
    db.close().unwrap();
}
