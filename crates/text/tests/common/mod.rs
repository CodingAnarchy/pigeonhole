//! Shared helpers: a seeded generator, a database, and a brute-force BM25 over the source
//! texts to test the index against (D309 §8, PR 1).
#![allow(dead_code)]

use std::collections::BTreeMap;

use pigeonhole::{Family, Options, Pigeonhole, Table};
use pigeonhole_text::{Index, IndexOptions, Source};

/// The seeds a test runs: `TEXT_SEED` alone when set, else `defaults`.
pub fn seeds(defaults: &[u64]) -> Vec<u64> {
    match std::env::var("TEXT_SEED") {
        Ok(s) => vec![s.parse().expect("TEXT_SEED is a u64")],
        Err(_) => defaults.to_vec(),
    }
}

/// xorshift64*: deterministic per seed.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// A small vocabulary, drawn with a skew so some words are hubs.
pub const VOCAB: [&str; 24] = [
    "the", "memory", "store", "agent", "postgres", "meeting", "march", "chose", "index", "search",
    "phrase", "query", "row", "table", "shard", "write", "read", "commit", "log", "feed", "vector",
    "text", "rank", "term",
];

pub fn word(rng: &mut Rng) -> &'static str {
    // Skewed: the low indices come up far more often.
    let a = rng.below(VOCAB.len() as u64);
    let b = rng.below(VOCAB.len() as u64);
    VOCAB[a.min(b) as usize]
}

pub fn text(rng: &mut Rng, max_words: u64) -> String {
    let n = rng.below(max_words + 1);
    let words: Vec<String> = (0..n)
        .map(|_| {
            let w = word(rng);
            // Some capitals and punctuation, which the tokeniser drops or folds.
            match rng.below(6) {
                0 => w.to_uppercase(),
                1 => format!("{w},"),
                _ => w.to_owned(),
            }
        })
        .collect();
    words.join(" ")
}

pub fn open(dir: &pigeonhole::doc_support::TempDir) -> Pigeonhole {
    Pigeonhole::open(dir.join("text.phdb"), Options::default().shards(2)).expect("open")
}

/// The source table `docs`, family `body`; the indexed column is `body:text`.
pub fn docs(db: &Pigeonhole) -> Table {
    db.table("docs")
        .unwrap()
        .family("body", Family::default())
        .create_if_missing()
        .unwrap()
}

pub fn source(docs: &Table) -> Source {
    Source::new(docs, "body", b"text")
}

pub fn create(db: &Pigeonhole, opts: IndexOptions) -> (Table, Index) {
    let t = docs(db);
    let index = Index::create(db, "docs_text", source(&t), opts).expect("create");
    (t, index)
}

/// The words of `text` as the built-in tokeniser gives them for ASCII text: runs of letters
/// and digits, lowercased.
pub fn words(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect()
}

/// BM25 with Lucene's idf over `corpus` (doc id → text), k1 1.2, b 0.75: the reference.
pub struct Bm25<'a> {
    pub corpus: &'a BTreeMap<Vec<u8>, String>,
    toks: BTreeMap<&'a [u8], Vec<String>>,
    avglen: f64,
}

impl<'a> Bm25<'a> {
    pub fn new(corpus: &'a BTreeMap<Vec<u8>, String>) -> Self {
        let toks: BTreeMap<&[u8], Vec<String>> = corpus
            .iter()
            .map(|(d, t)| (d.as_slice(), words(t)))
            .collect();
        let total: usize = toks.values().map(Vec::len).sum();
        let avglen = (total as f64 / corpus.len().max(1) as f64).max(f64::MIN_POSITIVE);
        Bm25 {
            corpus,
            toks,
            avglen,
        }
    }

    fn idf(&self, df: usize) -> f64 {
        let (n, df) = (self.corpus.len() as f64, df as f64);
        (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
    }

    fn tf_factor(&self, tf: f64, len: f64) -> f64 {
        tf * 2.2 / (tf + 1.2 * (0.25 + 0.75 * len / self.avglen))
    }

    /// Scores of a disjunction of distinct `terms` and exact `phrases`.
    pub fn scores(&self, terms: &[&str], phrases: &[Vec<&str>]) -> BTreeMap<Vec<u8>, f64> {
        let mut out: BTreeMap<Vec<u8>, f64> = BTreeMap::new();
        let mut terms: Vec<&str> = terms.to_vec();
        terms.sort_unstable();
        terms.dedup();
        for t in terms {
            let df = self
                .toks
                .values()
                .filter(|w| w.iter().any(|x| x == t))
                .count();
            if df == 0 {
                continue;
            }
            let idf = self.idf(df);
            for (d, w) in &self.toks {
                let tf = w.iter().filter(|x| *x == t).count();
                if tf > 0 {
                    *out.entry(d.to_vec()).or_default() +=
                        idf * self.tf_factor(tf as f64, w.len() as f64);
                }
            }
        }
        for p in phrases {
            let dfs: Vec<usize> = p
                .iter()
                .map(|t| {
                    self.toks
                        .values()
                        .filter(|w| w.iter().any(|x| x == t))
                        .count()
                })
                .collect();
            if dfs.contains(&0) {
                continue;
            }
            let idf: f64 = dfs.iter().map(|&df| self.idf(df)).sum();
            for (d, w) in &self.toks {
                let freq = (0..w.len())
                    .filter(|&i| {
                        i + p.len() <= w.len() && p.iter().zip(&w[i..]).all(|(a, b)| a == b)
                    })
                    .count();
                if freq > 0 {
                    *out.entry(d.to_vec()).or_default() +=
                        idf * self.tf_factor(freq as f64, w.len() as f64);
                }
            }
        }
        out
    }
}

/// Asserts `index` is consistent with itself and the source column.
pub fn check(index: &Index, ctx: &str) {
    let st = index
        .verify()
        .unwrap_or_else(|e| panic!("{ctx}: verify: {e}"));
    assert!(st.is_consistent(), "{ctx}: {st:?}");
}

/// Compares a search's full result with the reference's scores.
pub fn same_scores(got: &[pigeonhole_text::Hit], want: &BTreeMap<Vec<u8>, f64>, ctx: &str) {
    let got_map: BTreeMap<Vec<u8>, f64> = got.iter().map(|h| (h.id.clone(), h.score)).collect();
    assert_eq!(
        got_map.keys().collect::<Vec<_>>(),
        want.keys().collect::<Vec<_>>(),
        "{ctx}: documents"
    );
    for (d, w) in want {
        let g = got_map[d];
        assert!(
            (g - w).abs() <= 1e-9 * w.abs().max(1.0),
            "{ctx}: {d:?} scored {g}, want {w}"
        );
    }
    assert!(
        got.windows(2)
            .all(|p| p[0].score > p[1].score || (p[0].score == p[1].score && p[0].id < p[1].id)),
        "{ctx}: order"
    );
}
