//! D309 PR 4 (#1247, ICR 0105): fuzzy terms against a brute-force edit-distance scan of every
//! term the corpus holds: the expansions (transpositions, multi-byte characters,
//! `prefix_length`, the `max_expansions` cut and its order), their boosts, the per-clause
//! maximum, and MaxScore's top `k`.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::*;
use pigeonhole_text::{Error, Fuzziness, IndexOptions, Query};

/// Words with close neighbours, some multi-byte (lowercase and NFC, so the built-in
/// tokeniser keeps each one whole).
const VOCAB: &[&str] = &[
    "postgres",
    "postgre",
    "postgress",
    "potsgres",
    "progress",
    "cafe",
    "café",
    "caff",
    "cafés",
    "naive",
    "naïve",
    "native",
    "ab",
    "ba",
    "abc",
    "acb",
    "a",
    "b",
    "λόγος",
    "λογος",
    "λόγοι",
    "meeting",
    "meting",
    "melting",
    "mating",
    "store",
    "stores",
    "stare",
    "tore",
    "shore",
];

fn text(rng: &mut Rng) -> String {
    let n = 1 + rng.below(8);
    (0..n)
        .map(|_| {
            let a = rng.below(VOCAB.len() as u64);
            let b = rng.below(VOCAB.len() as u64);
            VOCAB[a.min(b) as usize]
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// The optimal-string-alignment distance over characters: Levenshtein, plus an adjacent
/// transposition as one edit (what the automaton computes).
fn osa(a: &str, b: &str) -> usize {
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    let mut d = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for (i, row) in d.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, cell) in d[0].iter_mut().enumerate() {
        *cell = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            let mut v = (d[i - 1][j] + 1)
                .min(d[i][j - 1] + 1)
                .min(d[i - 1][j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                v = v.min(d[i - 2][j - 2] + 1);
            }
            d[i][j] = v;
        }
    }
    d[a.len()][b.len()]
}

/// The reference: BM25 (k1 1.2, b 0.75, Lucene's idf) over space-separated words.
struct Model<'a> {
    docs: BTreeMap<&'a [u8], Vec<&'a str>>,
    avglen: f64,
}

impl<'a> Model<'a> {
    fn new(corpus: &'a BTreeMap<Vec<u8>, String>) -> Self {
        let docs: BTreeMap<&[u8], Vec<&str>> = corpus
            .iter()
            .map(|(d, t)| (d.as_slice(), t.split(' ').collect()))
            .collect();
        let total: usize = docs.values().map(Vec::len).sum();
        let avglen = (total as f64 / docs.len().max(1) as f64).max(f64::MIN_POSITIVE);
        Model { docs, avglen }
    }

    fn df(&self, t: &str) -> usize {
        self.docs.values().filter(|w| w.contains(&t)).count()
    }

    fn idf(&self, df: usize) -> f64 {
        let (n, df) = (self.docs.len() as f64, df as f64);
        (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
    }

    fn tf_factor(&self, tf: f64, len: f64) -> f64 {
        tf * 2.2 / (tf + 1.2 * (0.25 + 0.75 * len / self.avglen))
    }

    fn edits(q: &str, f: Fuzziness) -> usize {
        match f {
            Fuzziness::One => 1,
            Fuzziness::Two => 2,
            _ => match q.chars().count() {
                0..=2 => 0,
                3..=5 => 1,
                _ => 2,
            },
        }
    }

    /// The expansions by brute force: every term within the edits, sharing the prefix,
    /// boosted above 0; ordered and cut. `(term, distance, df, boost)` each, and the cut flag.
    fn expansions(
        &self,
        q: &str,
        f: Fuzziness,
        prefix: usize,
        max: usize,
    ) -> (Vec<(String, usize, usize, f64)>, bool) {
        let edits = Self::edits(q, f);
        let terms: BTreeSet<&str> = self.docs.values().flatten().copied().collect();
        let qc = q.chars().count();
        let qp: String = q.chars().take(prefix).collect();
        let mut out: Vec<(String, usize, usize, f64)> = terms
            .into_iter()
            .filter_map(|t| {
                // The fixed prefix: the query's first `prefix` characters (all of a shorter
                // query's) start the term.
                if !t.starts_with(qp.as_str()) {
                    return None;
                }
                let d = osa(&q[qp.len()..], &t[qp.len()..]);
                if d > edits {
                    return None;
                }
                let shorter = qc.min(t.chars().count());
                let boost = if d == 0 {
                    1.0
                } else if d < shorter {
                    1.0 - d as f64 / shorter as f64
                } else {
                    return None;
                };
                Some((t.to_owned(), d, self.df(t), boost))
            })
            .collect();
        out.sort_by(|a, b| {
            a.1.cmp(&b.1)
                .then(b.2.cmp(&a.2))
                .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
        });
        let cut = out.len() > max;
        out.truncate(max);
        (out, cut)
    }

    /// One fuzzy clause's scores: each document's best expansion.
    fn fuzzy(&self, exps: &[(String, usize, usize, f64)]) -> BTreeMap<Vec<u8>, f64> {
        let mut out: BTreeMap<Vec<u8>, f64> = BTreeMap::new();
        for (t, _, df, boost) in exps {
            let w = boost * self.idf(*df);
            for (d, words) in &self.docs {
                let tf = words.iter().filter(|x| *x == t).count();
                if tf > 0 {
                    let s = w * self.tf_factor(tf as f64, words.len() as f64);
                    let e = out.entry(d.to_vec()).or_insert(0.0);
                    *e = e.max(s);
                }
            }
        }
        out
    }

    fn term(&self, t: &str) -> BTreeMap<Vec<u8>, f64> {
        let df = self.df(t);
        if df == 0 {
            return BTreeMap::new();
        }
        self.fuzzy(&[(t.to_owned(), 0, df, 1.0)])
    }
}

fn add(into: &mut BTreeMap<Vec<u8>, f64>, from: BTreeMap<Vec<u8>, f64>) {
    for (d, s) in from {
        *into.entry(d).or_default() += s;
    }
}

/// A vocabulary word with up to two random edits (some of them transpositions).
fn misspell(rng: &mut Rng) -> String {
    let mut w: Vec<char> = VOCAB[rng.below(VOCAB.len() as u64) as usize]
        .chars()
        .collect();
    let alphabet: Vec<char> = "abcegorstλόγ".chars().collect();
    for _ in 0..rng.below(3) {
        let i = rng.below(w.len() as u64 + 1) as usize;
        match rng.below(4) {
            0 => w.insert(i, alphabet[rng.below(alphabet.len() as u64) as usize]),
            1 if i < w.len() => {
                w.remove(i);
            }
            2 if i < w.len() => w[i] = alphabet[rng.below(alphabet.len() as u64) as usize],
            _ if i + 1 < w.len() => w.swap(i, i + 1),
            _ => {}
        }
    }
    if w.is_empty() {
        w.push('a');
    }
    w.into_iter().collect()
}

#[test]
fn fuzzy_terms_equal_a_brute_force_scan_of_every_term() {
    for seed in seeds(&[31, 32, 33]) {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = open(&dir);
        let (_docs, index) = create(&db, IndexOptions::new());
        let mut rng = Rng::new(seed);
        let mut corpus: BTreeMap<Vec<u8>, String> = BTreeMap::new();
        for i in 0..120 {
            let doc = format!("d{i:03}").into_bytes();
            let t = text(&mut rng);
            index.put(&doc, &t).unwrap();
            corpus.insert(doc, t);
        }
        // Deletes, so some terms' `df` falls to 0 and they stop being expansions.
        for _ in 0..30 {
            let doc = format!("d{:03}", rng.below(120)).into_bytes();
            index.delete(&doc).unwrap();
            corpus.remove(&doc);
        }
        let m = Model::new(&corpus);
        for q in 0..150 {
            let fq = misspell(&mut rng);
            let f = [Fuzziness::Auto, Fuzziness::One, Fuzziness::Two][rng.below(3) as usize];
            let prefix = [0, 0, 1, 2][rng.below(4) as usize];
            let max = [1, 2, 3, 50][rng.below(4) as usize];
            let ctx = format!("seed {seed}, query {q}: {fq:?} {f:?} prefix {prefix} max {max}");
            let (exps, cut) = m.expansions(&fq, f, prefix, max);
            let mut want = m.fuzzy(&exps);
            // Half the queries add a term clause, so the fuzzy clause sums with another.
            let term = (rng.below(2) == 0).then(|| VOCAB[rng.below(VOCAB.len() as u64) as usize]);
            let mut query = Query::new().fuzzy(fq.clone(), f);
            if let Some(t) = term {
                query = query.term(t);
                add(&mut want, m.term(t));
            }
            let search = || {
                index
                    .search(query.clone(), 1000)
                    .prefix_length(prefix)
                    .max_expansions(max)
            };
            let (got, stats) = search().run_with_stats().unwrap();
            same_scores(&got, &want, &ctx);
            let st = &stats.fuzzy;
            assert_eq!(st.len(), 1, "{ctx}: {st:?}");
            assert_eq!(
                (
                    st[0].clause,
                    st[0].term.as_str(),
                    st[0].expansions,
                    st[0].cut
                ),
                (0, fq.as_str(), exps.len(), cut),
                "{ctx}: {exps:?}"
            );
            for k in [1, 3] {
                let got = index
                    .search(query.clone(), k)
                    .prefix_length(prefix)
                    .max_expansions(max)
                    .run()
                    .unwrap();
                same_top_k(&got, &want, k, &format!("{ctx}, k {k}"));
            }
        }
        check(&index, "fuzzy");
        db.close().unwrap();
    }
}

#[test]
fn a_document_with_three_misspellings_does_not_outrank_one_with_the_term() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new());
    index.put(b"exact", "postgres store").unwrap();
    index
        .put(b"typos", "postgre postgress potsgres store")
        .unwrap();
    index.put(b"other", "meeting").unwrap();
    let hits = index
        .search(Query::new().fuzzy("postgres", Fuzziness::Auto), 10)
        .run()
        .unwrap();
    let ids: Vec<&[u8]> = hits.iter().map(|h| h.id.as_slice()).collect();
    assert_eq!(ids, [&b"exact"[..], b"typos"]);
    assert!(hits[0].score > hits[1].score);
    db.close().unwrap();
}

#[test]
fn the_syntax_tokenises_a_fuzzy_word() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new());
    index.put(b"a", "Postgres for e-mail").unwrap();
    index.put(b"b", "progress").unwrap();
    // `Postgress~` is lowercased as documents are: the builder's term is as given.
    let parsed = index.search("Postgress~", 10).run().unwrap();
    let built = index
        .search(Query::new().fuzzy("postgress", Fuzziness::Auto), 10)
        .run()
        .unwrap();
    assert_eq!(parsed, built);
    assert_eq!(parsed.len(), 1);
    assert!(
        index
            .search(Query::new().fuzzy("Postgress", Fuzziness::One), 10)
            .run()
            .unwrap()
            .is_empty()
    );
    // A word that tokenises into two terms makes each one fuzzy, under the one clause.
    let (hits, stats) = index.search("e-mial~1", 10).run_with_stats().unwrap();
    assert_eq!(hits.len(), 1);
    let terms: Vec<(usize, &str)> = stats
        .fuzzy
        .iter()
        .map(|f| (f.clause, f.term.as_str()))
        .collect();
    assert_eq!(terms, [(0, "e"), (0, "mial")]);
    // Short words: `Auto` allows no edit below 3 characters, and a whole-term replacement
    // never matches (`e~1` would otherwise match any one-letter term).
    assert!(index.search("fo~", 10).run().unwrap().is_empty());
    assert_eq!(index.search("fro~", 10).run().unwrap().len(), 1);
    db.close().unwrap();
}

#[test]
fn max_expansions_of_zero_is_refused_and_an_empty_dictionary_matches_nothing() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = open(&dir);
    let (_docs, index) = create(&db, IndexOptions::new());
    let (hits, stats) = index.search("postgres~", 10).run_with_stats().unwrap();
    assert!(hits.is_empty());
    assert!(stats.fuzzy.is_empty(), "no documents: nothing expands");
    index.put(b"a", "postgres").unwrap();
    assert!(matches!(
        index.search("postgres~", 10).max_expansions(0).run(),
        Err(Error::InvalidArgument(_))
    ));
    // A prefix longer than the term fixes all of it: only edits after it, so appends.
    let hits = index
        .search(Query::new().fuzzy("postgr", Fuzziness::Two), 10)
        .prefix_length(10)
        .run()
        .unwrap();
    assert_eq!(hits.len(), 1);
    db.close().unwrap();
}
