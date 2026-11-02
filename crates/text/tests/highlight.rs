//! D309 PR 5 (#1247, ICR 0105): highlighting against a brute-force matcher over the source
//! texts (terms, text, exact and sloppy phrases, fuzzy terms; occurrences), multi-byte and
//! normalised text, an index without positions, and `snippet`.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use common::*;
use pigeonhole_text::{Fuzziness, IndexOptions, Match, Query, snippet};

/// The words of ASCII `text` as the built-in tokeniser gives them, with byte ranges: runs of
/// letters and digits, lowercased, at consecutive positions.
fn spans(text: &str) -> Vec<(String, Range<usize>)> {
    let mut out = Vec::new();
    let mut start = None;
    for (i, c) in text
        .char_indices()
        .chain(std::iter::once((text.len(), ' ')))
    {
        match (c.is_ascii_alphanumeric(), start) {
            (true, None) => start = Some(i),
            (false, Some(s)) => {
                out.push((text[s..i].to_lowercase(), s..i));
                start = None;
            }
            _ => {}
        }
    }
    out
}

/// What the reference finds for one clause, as (range, occurrence) pairs.
fn terms(sp: &[(String, Range<usize>)], want: &BTreeSet<&str>) -> Vec<(Range<usize>, u32)> {
    sp.iter()
        .filter(|(t, _)| want.contains(t.as_str()))
        .zip(0u32..)
        .map(|((_, r), o)| (r.clone(), o))
        .collect()
}

fn exact(sp: &[(String, Range<usize>)], phrase: &[&str]) -> Vec<(Range<usize>, u32)> {
    (0..sp.len())
        .filter(|&i| {
            i + phrase.len() <= sp.len() && phrase.iter().zip(&sp[i..]).all(|(a, b)| *a == b.0)
        })
        .zip(0u32..)
        .map(|(i, o)| (sp[i].1.start..sp[i + phrase.len() - 1].1.end, o))
        .collect()
}

/// A hit's matches for `clause`, as (range, occurrence), by position.
fn of(ms: &[Match], clause: usize) -> Vec<(Range<usize>, u32)> {
    ms.iter()
        .filter(|m| m.clause == clause)
        .map(|m| (m.range.clone(), m.occurrence))
        .collect()
}

#[test]
fn matches_equal_a_brute_force_over_the_source_texts() {
    for seed in seeds(&[41, 42, 43]) {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = open(&dir);
        let (_docs, index) = create(&db, IndexOptions::new());
        let mut rng = Rng::new(seed);
        let mut corpus: BTreeMap<Vec<u8>, String> = BTreeMap::new();
        for i in 0..80 {
            let doc = format!("d{i:02}").into_bytes();
            let t = text(&mut rng, 20);
            index.put(&doc, &t).unwrap();
            corpus.insert(doc, t);
        }
        for q in 0..60 {
            // Clause 0: a term; 1: text of two words; 2: an exact phrase from a document;
            // 3: the same phrase with slop 2; 4: a fuzzy term.
            let t0 = word(&mut rng);
            let (t1, t2) = (word(&mut rng), word(&mut rng));
            let src = corpus
                .values()
                .nth(rng.below(corpus.len() as u64) as usize)
                .unwrap();
            let w = words(src);
            let phrase: Vec<String> = if w.len() >= 2 {
                let at = rng.below(w.len() as u64 - 1) as usize;
                w[at..at + 2].to_vec()
            } else {
                vec!["postgres".into(), "meeting".into()]
            };
            let mut typo: Vec<char> = word(&mut rng).chars().collect();
            let i = rng.below(typo.len() as u64) as usize;
            typo[i] = 'x';
            let typo: String = typo.into_iter().collect();
            let query = Query::new()
                .term(t0)
                .text(format!("{t1} {t2}"))
                .phrase(phrase.clone())
                .phrase_with_slop(phrase.clone(), 2)
                .fuzzy(typo.clone(), Fuzziness::One);
            let ctx = format!("seed {seed}, query {q}: {query:?}");
            let (hits, stats) = index
                .search(query.clone(), 1000)
                .highlight(true)
                .run_with_stats()
                .unwrap();
            let unhl = index.search(query.clone(), 1000).run().unwrap();
            assert_eq!(hits.len(), unhl.len(), "{ctx}");
            // The fuzzy clause's expansions are what the stats say were found; with this
            // small vocabulary nothing is cut, so they're every term within one edit.
            assert!(stats.fuzzy.iter().all(|f| !f.cut), "{ctx}");
            let vocab: BTreeSet<String> = corpus.values().flat_map(|t| words(t)).collect();
            let exps: BTreeSet<&str> = vocab
                .iter()
                .filter(|v| {
                    let (a, b): (Vec<char>, Vec<char>) =
                        (typo.chars().collect(), v.chars().collect());
                    let short = a.len().min(b.len());
                    let d = osa1(&a, &b);
                    d == 0 || (d == 1 && 1 < short)
                })
                .map(String::as_str)
                .collect();
            for (h, u) in hits.iter().zip(&unhl) {
                assert_eq!(
                    (&h.id, h.score),
                    (&u.id, u.score),
                    "{ctx}: highlighting changes nothing else"
                );
                assert!(u.text.is_none() && u.matches.is_empty(), "{ctx}");
                let text = h.text.as_deref().expect("highlighted");
                assert_eq!(text, corpus[&h.id], "{ctx}");
                let sp = spans(text);
                let ctx = format!("{ctx}, {:?} in {text:?}", String::from_utf8_lossy(&h.id));
                assert_eq!(
                    of(&h.matches, 0),
                    terms(&sp, &BTreeSet::from([t0])),
                    "{ctx}: term"
                );
                assert_eq!(
                    of(&h.matches, 1),
                    terms(&sp, &BTreeSet::from([t1, t2])),
                    "{ctx}: text"
                );
                let p: Vec<&str> = phrase.iter().map(String::as_str).collect();
                let ex = exact(&sp, &p);
                assert_eq!(of(&h.matches, 2), ex, "{ctx}: exact phrase");
                // A sloppy match lists one range per phrase word, each on that word, sharing
                // an occurrence; every exact occurrence's words are among them.
                let sloppy = of(&h.matches, 3);
                let by_occ: BTreeMap<u32, Vec<Range<usize>>> =
                    sloppy.iter().fold(BTreeMap::new(), |mut m, (r, o)| {
                        m.entry(*o).or_default().push(r.clone());
                        m
                    });
                for rs in by_occ.values() {
                    assert_eq!(rs.len(), 2, "{ctx}: {rs:?}");
                    let mut ws: Vec<String> =
                        rs.iter().map(|r| text[r.clone()].to_lowercase()).collect();
                    let mut want = phrase.clone();
                    ws.sort();
                    want.sort();
                    assert_eq!(ws, want, "{ctx}: sloppy words");
                }
                assert_eq!(
                    by_occ.is_empty(),
                    ex.is_empty() && !within(&sp, &p, 2),
                    "{ctx}: sloppy"
                );
                assert_eq!(of(&h.matches, 4), terms(&sp, &exps), "{ctx}: fuzzy");
                // Matches come by position.
                assert!(
                    h.matches
                        .windows(2)
                        .all(|w| w[0].range.start <= w[1].range.start),
                    "{ctx}"
                );
            }
        }
        db.close().unwrap();
    }
}

/// Whether two phrase words occur within `slop` position moves (both orders), on distinct
/// positions: the sloppy rule for a two-word phrase.
fn within(sp: &[(String, Range<usize>)], p: &[&str], slop: i64) -> bool {
    (0..sp.len()).any(|i| {
        (0..sp.len()).any(|j| {
            i != j
                && sp[i].0 == p[0]
                && sp[j].0 == p[1]
                && ((j as i64 - 1) - i as i64).abs() <= slop
        })
    })
}

/// The optimal-string-alignment distance, capped at 2 (enough for one-edit checks).
fn osa1(a: &[char], b: &[char]) -> usize {
    let mut d = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for (i, row) in d.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, cell) in d[0].iter_mut().enumerate() {
        *cell = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let mut v = (d[i - 1][j] + 1)
                .min(d[i][j - 1] + 1)
                .min(d[i - 1][j - 1] + usize::from(a[i - 1] != b[j - 1]));
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                v = v.min(d[i - 2][j - 2] + 1);
            }
            d[i][j] = v;
        }
    }
    d[a.len()][b.len()].min(2)
}

#[test]
fn ranges_are_bytes_of_the_text_as_stored() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new());
    // Decomposed in the source: the index holds the NFC form, the range covers the source's
    // bytes.
    let text = "Le Cafe\u{301} CRÈME, café";
    index.put(b"a", text).unwrap();
    let hits = index
        .search("café crème~", 10)
        .highlight(true)
        .run()
        .unwrap();
    let h = &hits[0];
    let got: Vec<(&str, usize, u32)> = h
        .matches
        .iter()
        .map(|m| (&text[m.range.clone()], m.clause, m.occurrence))
        .collect();
    assert_eq!(
        got,
        [("Cafe\u{301}", 0, 0), ("CRÈME", 1, 0), ("café", 0, 1)]
    );
    db.close().unwrap();
}

#[test]
fn an_index_without_positions_highlights_terms_and_fuzzy_terms() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new().positions(false));
    index.put(b"a", "postgres and postgress").unwrap();
    let hits = index
        .search("postgres postgress~1", 10)
        .highlight(true)
        .run()
        .unwrap();
    let got: Vec<(Range<usize>, usize, u32)> = hits[0]
        .matches
        .iter()
        .map(|m| (m.range.clone(), m.clause, m.occurrence))
        .collect();
    assert_eq!(got, [(0..8, 0, 0), (0..8, 1, 0), (13..22, 1, 1)]);
    db.close().unwrap();
}

#[test]
fn a_phrase_keeps_its_stop_words_and_a_snippet_frames_the_best_matches() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new().stop_words(true));
    let text = "notes: the state of the art in search, then the state of art, and more notes";
    index.put(b"a", text).unwrap();
    let hits = index
        .search(r#""state of the art" notes"#, 10)
        .highlight(true)
        .run()
        .unwrap();
    let h = &hits[0];
    let phrase: Vec<&str> = h
        .matches
        .iter()
        .filter(|m| m.clause == 0)
        .map(|m| &text[m.range.clone()])
        .collect();
    // The second "state of art" lacks the stop word's position, so it isn't the phrase.
    assert_eq!(phrase, ["state of the art"]);
    let s = snippet(text, &h.matches, 30);
    assert_eq!(&text[s.range.clone()], "notes: the state of the art in");
    assert_eq!(s.matches.len(), 2);
    db.close().unwrap();
}
