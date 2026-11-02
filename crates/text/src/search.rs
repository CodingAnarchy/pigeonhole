//! Search (D309 §4): disjunctive BM25 over terms and exact phrases, term at a time.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use pigeonhole::{Snapshot, Table};

use crate::error::{Error, Result, corrupt};
use crate::index::Index;
use crate::layout::*;
use crate::query::{Clause, Query};

/// One search result.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Hit {
    /// The document's id: its source row's key.
    pub id: Vec<u8>,
    /// Its BM25 score.
    pub score: f64,
}

/// What a search met besides its hits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SearchStats {
    /// Hits skipped because their source cell is gone: the source was deleted around the
    /// index (a bypass, D309 §3). The next candidate took each one's place.
    pub stale: u64,
}

/// A search to run ([`Index::search`]).
#[derive(Debug)]
#[must_use = "a search does nothing until it runs"]
pub struct Search<'a> {
    index: &'a Index,
    query: Query,
    k: usize,
    snapshot: Option<&'a Snapshot>,
}

/// A phrase as the evaluator matches it: each term with its offset from the first.
type Phrase = Vec<(String, u32)>;

impl<'a> Search<'a> {
    pub(crate) fn new(index: &'a Index, query: Query, k: usize) -> Self {
        Search {
            index,
            query,
            k,
            snapshot: None,
        }
    }

    /// Searches at `snapshot` rather than the latest state.
    pub fn snapshot(mut self, snapshot: &'a Snapshot) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    /// The top `k` hits: by score descending, then id ascending.
    ///
    /// `InvalidQuery` for a query string that didn't parse; `InvalidArgument` for a phrase
    /// on an index without positions. It fails with `Busy` while a [`repair`](Index::repair)
    /// runs: retry after it finishes.
    pub fn run(self) -> Result<Vec<Hit>> {
        self.run_with_stats().map(|(hits, _)| hits)
    }

    /// [`run`](Self::run), with what the search met.
    pub fn run_with_stats(self) -> Result<(Vec<Hit>, SearchStats)> {
        self.query.check()?;
        let index = self.index;
        let inn = &*index.inner;
        let owned;
        let s = match self.snapshot {
            Some(s) => s,
            None => {
                owned = inn.db.snapshot()?;
                &owned
            }
        };
        let t = &inn.table;
        if t.get_at(s, GLOBAL, META, REPAIR)?.is_some() {
            return Err(Error::Busy);
        }
        let mut stats = SearchStats::default();
        let (terms, phrases) = self.resolve()?;
        if self.k == 0 || (terms.is_empty() && phrases.is_empty()) {
            return Ok((Vec::new(), stats));
        }
        let counter = |q: &[u8]| -> Result<i64> {
            Ok(t.get_at(s, STATS_ROW, STATS, q)?
                .and_then(|c| c.as_i64())
                .unwrap_or(0))
        };
        let n = counter(DOCS)?;
        if n <= 0 {
            return Ok((Vec::new(), stats));
        }
        let bm = Bm25 {
            n: n as f64,
            avglen: (counter(LEN)?.max(0) as f64 / n as f64).max(f64::MIN_POSITIVE),
            k1: inn.opts.get_k1(),
            b: inn.opts.get_b(),
        };
        let mut df_of: HashMap<String, i64> = HashMap::new();
        for term in terms.iter().chain(phrases.iter().flatten().map(|(t, _)| t)) {
            if !df_of.contains_key(term) {
                let df = t
                    .get_at(s, &post_row(term), DF, N)?
                    .and_then(|c| c.as_i64())
                    .unwrap_or(0);
                df_of.insert(term.clone(), df);
            }
        }
        let mut scores: HashMap<Vec<u8>, f64> = HashMap::new();
        for term in &terms {
            let df = df_of[term];
            if df <= 0 {
                continue;
            }
            let idf = bm.idf(df);
            let mut cells = t.row(&post_row(term)).family(POST).snapshot(s).cells()?;
            while let Some(e) = cells.next()? {
                let (tf, len) = unposting(
                    e.cell
                        .as_u64()
                        .ok_or_else(|| corrupt("a posting isn't a u64"))?,
                );
                *scores.entry(e.qualifier.to_vec()).or_default() +=
                    idf * bm.tf_factor(f64::from(tf), f64::from(len));
            }
        }
        for phrase in &phrases {
            score_phrase(t, s, &bm, &df_of, phrase, &mut scores)?;
        }
        let mut ranked: Vec<(Vec<u8>, f64)> = scores.into_iter().collect();
        ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let mut hits = Vec::with_capacity(self.k.min(ranked.len()));
        let (fam, q) = (inn.source.family.as_str(), inn.source.qualifier.as_slice());
        for (id, score) in ranked {
            if hits.len() == self.k {
                break;
            }
            if inn.src.get_at(s, &id, fam, q)?.is_none() {
                stats.stale += 1;
                continue;
            }
            hits.push(Hit { id, score });
        }
        Ok((hits, stats))
    }

    /// The query's distinct terms and phrases, normalised and tokenised as the clauses say.
    /// A term a clause yields twice counts once; a one-term phrase is a term.
    fn resolve(&self) -> Result<(BTreeSet<String>, Vec<Phrase>)> {
        let index = self.index;
        let cap = index.inner.opts.get_max_term_bytes() as usize;
        let mut terms = BTreeSet::new();
        let mut phrases: Vec<Phrase> = Vec::new();
        let mut add_phrase = |p: Phrase, terms: &mut BTreeSet<String>| match p.len() {
            0 => {}
            1 => {
                terms.insert(p.into_iter().next().expect("one").0);
            }
            _ => {
                if !phrases.contains(&p) {
                    phrases.push(p);
                }
            }
        };
        for clause in self.query.clauses() {
            match clause {
                Clause::Term(t) => {
                    let t = index.normalize_term(t);
                    // A builder term over the cap can't match: the index dropped such words.
                    if !t.is_empty() && t.len() <= cap {
                        terms.insert(t.into_owned());
                    }
                }
                Clause::Text(text) => {
                    terms.extend(index.query_tokens(text).into_iter().map(|(t, _)| t));
                }
                Clause::Phrase { words, slop } => {
                    refuse_slop(*slop)?;
                    let p: Phrase = words
                        .iter()
                        .zip(0u32..)
                        .map(|(w, i)| (index.normalize_term(w).into_owned(), i))
                        .collect();
                    // A phrase with a word that can't be a term matches nothing.
                    if p.iter().all(|(w, _)| !w.is_empty() && w.len() <= cap) {
                        add_phrase(p, &mut terms);
                    }
                }
                Clause::QuotedPhrase { text, slop } => {
                    refuse_slop(*slop)?;
                    let toks = index.query_tokens(text);
                    let first = toks.first().map_or(0, |(_, p)| *p);
                    add_phrase(
                        toks.into_iter().map(|(t, p)| (t, p - first)).collect(),
                        &mut terms,
                    );
                }
                Clause::Fuzzy { .. } => {
                    return Err(Error::InvalidArgument(
                        "fuzzy terms are not supported yet: they come in D309 PR 4 (#1247)".into(),
                    ));
                }
            }
        }
        if !phrases.is_empty() && !index.inner.opts.get_positions() {
            return Err(Error::InvalidArgument(
                "the index has no positions, which a phrase needs".into(),
            ));
        }
        Ok((terms, phrases))
    }
}

fn refuse_slop(slop: u16) -> Result<()> {
    if slop > 0 {
        return Err(Error::InvalidArgument(
            "phrase slop is not supported yet: it comes in D309 PR 2 (#1290)".into(),
        ));
    }
    Ok(())
}

/// BM25 with Lucene's idf (D309 §4).
struct Bm25 {
    n: f64,
    avglen: f64,
    k1: f64,
    b: f64,
}

impl Bm25 {
    fn idf(&self, df: i64) -> f64 {
        let df = df as f64;
        (1.0 + (self.n - df + 0.5) / (df + 0.5)).ln()
    }

    fn tf_factor(&self, tf: f64, len: f64) -> f64 {
        tf * (self.k1 + 1.0) / (tf + self.k1 * (1.0 - self.b + self.b * len / self.avglen))
    }
}

/// Adds an exact phrase's scores: Lucene's PhraseQuery, with the phrase as one pseudo-term
/// whose frequency is its occurrences and whose idf is the sum of its terms' (D309 §4.3).
fn score_phrase(
    t: &Table,
    s: &Snapshot,
    bm: &Bm25,
    df_of: &HashMap<String, i64>,
    phrase: &Phrase,
    scores: &mut HashMap<Vec<u8>, f64>,
) -> Result<()> {
    if phrase.iter().any(|(term, _)| df_of[term] <= 0) {
        return Ok(());
    }
    let idf: f64 = phrase.iter().map(|(term, _)| bm.idf(df_of[term])).sum();
    // Lead with the rarest term; the others narrow its documents.
    let lead = (0..phrase.len())
        .min_by_key(|&i| df_of[&phrase[i].0])
        .expect("a phrase has terms");
    let mut by_term: BTreeMap<&str, BTreeMap<Vec<u8>, Vec<u32>>> = BTreeMap::new();
    let lead_term = phrase[lead].0.as_str();
    by_term.insert(lead_term, read_positions(t, s, lead_term, None)?);
    for (term, _) in phrase {
        if by_term.contains_key(term.as_str()) {
            continue;
        }
        let keep = &by_term[lead_term];
        let got = read_positions(t, s, term, Some(keep))?;
        by_term.insert(term, got);
    }
    let lead_off = phrase[lead].1;
    for (doc, lead_ps) in &by_term[lead_term] {
        let mut freq = 0u32;
        'start: for &p in lead_ps {
            for (term, off) in phrase {
                let Some(at) = (p + off).checked_sub(lead_off) else {
                    continue 'start;
                };
                let found = by_term[term.as_str()]
                    .get(doc)
                    .is_some_and(|ps| ps.binary_search(&at).is_ok());
                if !found {
                    continue 'start;
                }
            }
            freq += 1;
        }
        if freq == 0 {
            continue;
        }
        let len = match t.get_at(s, &post_row(lead_term), POST, doc)? {
            Some(c) => unposting(c.as_u64().ok_or_else(|| corrupt("a posting isn't a u64"))?).1,
            None => return Err(corrupt("a position has no posting")),
        };
        *scores.entry(doc.clone()).or_default() +=
            idf * bm.tf_factor(f64::from(freq), f64::from(len));
    }
    Ok(())
}

/// A term's positions by document, for the documents in `keep` (all when `None`).
fn read_positions(
    t: &Table,
    s: &Snapshot,
    term: &str,
    keep: Option<&BTreeMap<Vec<u8>, Vec<u32>>>,
) -> Result<BTreeMap<Vec<u8>, Vec<u32>>> {
    let mut out = BTreeMap::new();
    let mut cells = t.row(&post_row(term)).family(POS).snapshot(s).cells()?;
    while let Some(e) = cells.next()? {
        if keep.is_some_and(|k| !k.contains_key(e.qualifier)) {
            continue;
        }
        let mut ps = Vec::new();
        decode_positions(e.cell.value(), &mut ps)?;
        out.insert(e.qualifier.to_vec(), ps);
    }
    Ok(out)
}
