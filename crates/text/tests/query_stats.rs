//! #1722 PR 2a (ICR 0138): `QueryStats`, a search's ranking statistics as a value. A
//! collection split across indexes, searched with the parts' merged statistics, scores as
//! one index over every document; a search given its own statistics is the search; and the
//! encoding round-trips and refuses what it can't read.

mod common;

use std::collections::BTreeMap;

use common::*;
use pigeonhole::{Family, Pigeonhole};
use pigeonhole_text::{Error, Hit, Index, IndexOptions, QueryStats, Search, Source};

/// An index named `name` over its own source table, so each part's documents are its own.
fn index_on(db: &Pigeonhole, name: &str) -> Index {
    let t = db
        .table(name)
        .unwrap()
        .family("body", Family::default())
        .create_if_missing()
        .unwrap();
    Index::create(
        db,
        &format!("{name}_text"),
        Source::new(&t, "body", b"text"),
        IndexOptions::new(),
    )
    .expect("create")
}

/// Two parts and a whole: each document in one part (by `split`) and in the whole.
struct Split {
    _dir: pigeonhole::doc_support::TempDir,
    parts: [Index; 2],
    whole: Index,
}

impl Split {
    fn new(docs: &[(Vec<u8>, String)], split: impl Fn(&[u8]) -> usize) -> Split {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = open(&dir);
        let parts = [index_on(&db, "a"), index_on(&db, "b")];
        let whole = index_on(&db, "whole");
        for (id, text) in docs {
            parts[split(id)].put(id, text).unwrap();
            whole.put(id, text).unwrap();
        }
        Split {
            _dir: dir,
            parts,
            whole,
        }
    }

    fn pack(&self) {
        for i in self.parts.iter().chain([&self.whole]) {
            i.pack().unwrap();
        }
    }

    /// The parts' statistics for the search `make` builds, merged.
    fn merged(&self, make: &dyn Fn(&Index) -> Search<'_>) -> QueryStats {
        QueryStats::merge(self.parts.iter().map(|p| make(p).query_stats().unwrap())).unwrap()
    }

    /// The parts searched with their merged statistics, the hits merged by score
    /// descending then id, cut at `k`.
    fn search(&self, make: &dyn Fn(&Index) -> Search<'_>, k: usize) -> Vec<Hit> {
        let stats = self.merged(make);
        let mut hits: Vec<Hit> = Vec::new();
        for p in &self.parts {
            hits.extend(make(p).stats(stats.clone()).run().unwrap());
        }
        hits.sort_by(|x, y| y.score.total_cmp(&x.score).then_with(|| x.id.cmp(&y.id)));
        hits.truncate(k);
        hits
    }
}

/// Equal up to float rounding: each part adds a document's clauses in its own order.
fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0)
}

/// `got` (the parts merged) against `whole`'s search with every match (`all`) and its top
/// `k` (`top`): the same scores in the same places, and each hit's score the whole's for
/// that document. Ids at a place may differ only where scores tie.
fn same(got: &[Hit], top: &[Hit], all: &[Hit], ctx: &str) {
    let all: BTreeMap<&[u8], f64> = all.iter().map(|h| (h.id.as_slice(), h.score)).collect();
    assert_eq!(got.len(), top.len(), "{ctx}: hits");
    for (g, w) in got.iter().zip(top) {
        assert!(
            close(g.score, w.score),
            "{ctx}: {} scored {} against {}",
            String::from_utf8_lossy(&g.id),
            g.score,
            w.score
        );
        let whole = all
            .get(g.id.as_slice())
            .unwrap_or_else(|| panic!("{ctx}: {:?} isn't a match", g.id));
        assert!(close(g.score, *whole), "{ctx}: {:?}", g.id);
    }
}

/// `f`, typed as a search builder (a closure returning a borrow of its argument needs it).
fn mk<F: Fn(&Index) -> Search<'_>>(f: F) -> F {
    f
}

fn corpus(seed: u64, n: usize) -> Vec<(Vec<u8>, String)> {
    let mut rng = Rng::new(seed);
    (0..n)
        .map(|i| {
            // Two tenants, and a third of the documents a second's: skewed parts.
            let tenant = if rng.below(3) == 0 { "t1" } else { "t2" };
            (format!("{tenant}:{i:05}").into_bytes(), text(&mut rng, 12))
        })
        .collect()
}

const QUERIES: &[&str] = &[
    "memory",
    "store agent",
    "postgres meeting march",
    "\"memory store\"",
    "\"the memory\"~2 store",
    "memroy~1",
    "stor~1 agnet~1 the",
    "vector \"chose index\" serch~1",
];

#[test]
fn a_split_index_with_merged_statistics_scores_as_one_index() {
    for seed in seeds(&[1, 2, 3]) {
        let docs = corpus(seed, 300);
        // By tenant: the parts' own statistics differ, which is what the merge is for.
        let split = Split::new(&docs, |id| usize::from(id.starts_with(b"t1")));
        for packed in [false, true] {
            if packed {
                split.pack();
            }
            for q in QUERIES {
                for prefix in [None, Some(&b"t1:"[..]), Some(&b"t2:001"[..])] {
                    let ctx = format!("seed {seed} packed {packed} {q:?} prefix {prefix:?}");
                    let make = mk(|i: &Index| {
                        let s = i.search(*q, 10);
                        match prefix {
                            Some(p) => s.id_prefix(p),
                            None => s,
                        }
                    });
                    let make_all = mk(|i: &Index| {
                        let s = i.search(*q, 1000);
                        match prefix {
                            Some(p) => s.id_prefix(p),
                            None => s,
                        }
                    });
                    let all = make_all(&split.whole).run().unwrap();
                    same(
                        &split.search(&make_all, 1000),
                        &all,
                        &all,
                        &format!("{ctx} all"),
                    );
                    let top = make(&split.whole).run().unwrap();
                    same(&split.search(&make, 10), &top, &all, &format!("{ctx} top"));
                    let stats = split.merged(&make);
                    assert_eq!(stats.documents(), docs.len() as u64, "{ctx}");
                    assert!(!(0..4).any(|c| stats.cut(c)), "{ctx}: no cut");
                }
            }
        }
    }
}

#[test]
fn own_statistics_give_the_search_itself() {
    let docs = corpus(7, 200);
    let split = Split::new(&docs, |_| 0);
    let index = &split.whole;
    for q in QUERIES {
        for highlight in [false, true] {
            let make = || index.search(*q, 10).highlight(highlight).id_prefix(b"t2:");
            let stats = make().query_stats().unwrap();
            let want = make().run_with_stats().unwrap();
            assert_eq!(
                make().stats(stats.clone()).run_with_stats().unwrap(),
                want,
                "{q}"
            );
            #[cfg(feature = "async")]
            {
                let given = make().stats(stats.clone());
                let got = pigeonhole::doc_support::block_on(given.run_with_stats_async());
                assert_eq!(got.unwrap(), want, "{q} async");
                let read = pigeonhole::doc_support::block_on(make().query_stats_async());
                assert_eq!(
                    read.unwrap().to_bytes(),
                    stats.to_bytes(),
                    "{q} async stats"
                );
            }
        }
    }
}

#[test]
fn a_term_in_one_part_only_scores_there_and_matches_nothing_elsewhere() {
    let mut docs = corpus(11, 100);
    docs.push((b"t1:zebra".to_vec(), "zebra memory".into()));
    docs.push((b"t1:zebra2".to_vec(), "zebra zebra".into()));
    let split = Split::new(&docs, |id| usize::from(id.starts_with(b"t1")));
    for q in ["zebra", "zebra memory", "\"zebra memory\"", "zebar~1"] {
        let make = mk(|i: &Index| i.search(q, 10));
        let stats = split.merged(&make);
        // The part without the term finds nothing of it, and fails nothing.
        let other = make(&split.parts[0]).stats(stats).run().unwrap();
        assert!(other.iter().all(|h| !h.id.starts_with(b"t1:zebra")), "{q}");
        let all = make(&split.whole).run().unwrap();
        same(&split.search(&make, 10), &all, &all, q);
    }
}

#[test]
fn a_fuzzy_term_cut_in_the_parts_is_flagged_after_the_merge() {
    // audit120's shape: `max_expansions` 2, every term one edit from `abcd`. Each part keeps
    // its two most frequent and cuts `abcz`, which across both parts is the most frequent.
    let mut docs = Vec::new();
    let mut add = |part: &str, term: &str, n: usize| {
        for i in 0..n {
            docs.push((format!("{part}:{term}{i}").into_bytes(), term.to_owned()));
        }
    };
    add("a", "abca", 3);
    add("a", "abcb", 3);
    add("a", "abcz", 2);
    add("b", "abcc", 3);
    add("b", "abce", 3);
    add("b", "abcz", 2);
    add("b", "memory", 1);
    let split = Split::new(&docs, |id| usize::from(id.starts_with(b"b")));
    let make = mk(|i: &Index| i.search("abcd~1 memory", 10).max_expansions(2));
    for p in &split.parts {
        assert!(make(p).query_stats().unwrap().cut(0));
    }
    let stats = split.merged(&make);
    assert!(stats.cut(0), "the fuzzy clause");
    assert!(!stats.cut(1), "a term clause is never cut");
    assert_eq!(stats.documents(), docs.len() as u64);
    // The merge kept the parts' terms, re-cut to 2: `abca` and `abcb` (3 each, bytewise).
    let (hits, st) = make(&split.parts[0])
        .stats(stats.clone())
        .run_with_stats()
        .unwrap();
    assert!(st.fuzzy[0].cut);
    assert_eq!(st.fuzzy[0].expansions, 2);
    assert_eq!(hits.len(), 6, "{hits:?}");
    assert!(
        hits.iter()
            .all(|h| h.text.is_none() && !h.id.starts_with(b"a:abcz"))
    );
}

#[test]
fn statistics_round_trip_their_bytes_and_refuse_what_they_cannot_read() {
    let docs = corpus(5, 100);
    let split = Split::new(&docs, |id| usize::from(id.starts_with(b"t1")));
    let make = mk(|i: &Index| i.search("vector \"chose index\" serch~1 stor~2", 10));
    let stats = split.merged(&make);
    let bytes = stats.to_bytes();
    let back = QueryStats::from_bytes(&bytes).unwrap();
    assert_eq!(back.to_bytes(), bytes);
    for p in &split.parts {
        assert_eq!(
            make(p).stats(back.clone()).run().unwrap(),
            make(p).stats(stats.clone()).run().unwrap()
        );
    }
    let invalid = |r: pigeonhole_text::Result<QueryStats>, what: &str| match r {
        Err(Error::InvalidArgument(m)) => m,
        other => panic!("{what}: {other:?}"),
    };
    let mut newer = bytes.clone();
    newer[0] = 2;
    let m = invalid(QueryStats::from_bytes(&newer), "a newer version");
    assert!(m.contains("version 2"), "{m}");
    for n in 0..bytes.len() {
        invalid(QueryStats::from_bytes(&bytes[..n]), "cut short");
    }
    let mut longer = bytes.clone();
    longer.push(0);
    invalid(QueryStats::from_bytes(&longer), "trailing bytes");
}

#[test]
fn statistics_of_another_query_are_refused() {
    let docs = corpus(9, 50);
    let split = Split::new(&docs, |id| usize::from(id.starts_with(b"t1")));
    let index = &split.parts[0];
    let memory = index.search("memory memroy~1", 10).query_stats().unwrap();
    let refused = |r: pigeonhole_text::Result<Vec<Hit>>, what: &str| {
        assert!(matches!(r, Err(Error::InvalidArgument(_))), "{what}: {r:?}");
    };
    refused(
        index.search("store", 10).stats(memory.clone()).run(),
        "another term",
    );
    refused(
        index.search("memory", 10).stats(memory.clone()).run(),
        "a fuzzy term missing",
    );
    refused(
        index
            .search("memory memroy~1", 10)
            .max_expansions(3)
            .stats(memory.clone())
            .run(),
        "other fuzzy options",
    );
    refused(
        index
            .search("memory memroy~2", 10)
            .stats(memory.clone())
            .run(),
        "other edits",
    );
    let store = index.search("store", 10).query_stats().unwrap();
    assert!(matches!(
        QueryStats::merge([memory.clone(), store]),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        QueryStats::merge(Vec::new()),
        Err(Error::InvalidArgument(_))
    ));
    // A search with nothing to match reads nothing, given statistics or not.
    assert!(index.search("", 10).stats(memory).run().unwrap().is_empty());
}
