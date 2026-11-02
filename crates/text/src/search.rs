//! Search (D309 §4): disjunctive BM25 over terms, phrases and fuzzy terms, by MaxScore.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use pigeonhole::Snapshot;

use crate::error::{Error, Result, corrupt};
use crate::fuzzy::{Expansion, edits, expand};
use crate::index::{Index, Rows};
use crate::layout::*;
use crate::maxscore::{Bm25, Scored};
use crate::query::{Clause, Fuzziness, Query};

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
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SearchStats {
    /// Hits skipped because their source cell is gone: the source was deleted around the
    /// index (a bypass, D309 §3). The next candidate took each one's place.
    pub stale: u64,
    /// Each fuzzy term's expansions, in the order the query's clauses yield them (a fuzzy
    /// term two clauses yield is listed once, under the first).
    pub fuzzy: Vec<FuzzyStats>,
}

/// One fuzzy term's expansions ([`SearchStats::fuzzy`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct FuzzyStats {
    /// The clause that yielded it: an index into [`Query::clauses`].
    pub clause: usize,
    /// The fuzzy term, normalised (and for [`Clause::FuzzyText`], tokenised).
    pub term: String,
    /// The terms it expanded to.
    pub expansions: usize,
    /// Whether [`Search::max_expansions`] left matching terms out.
    pub cut: bool,
}

/// [`Search::max_expansions`]' default (D309 §4.4).
const DEFAULT_MAX_EXPANSIONS: usize = 50;

/// A fuzzy term as the search expands it.
#[derive(Debug, PartialEq)]
struct FuzzyTerm {
    clause: usize,
    term: String,
    edits: u8,
}

/// A search to run ([`Index::search`]).
#[derive(Debug)]
#[must_use = "a search does nothing until it runs"]
pub struct Search<'a> {
    index: &'a Index,
    query: Query,
    k: usize,
    snapshot: Option<&'a Snapshot>,
    prefix: Option<Vec<u8>>,
    max_expansions: usize,
    prefix_length: usize,
}

/// A phrase as the evaluator matches it: each term with its offset from the first, and the
/// position moves a match may take (0: exact).
#[derive(Debug, PartialEq)]
struct Phrase {
    terms: Vec<(String, u32)>,
    slop: u16,
}

impl<'a> Search<'a> {
    pub(crate) fn new(index: &'a Index, query: Query, k: usize) -> Self {
        Search {
            index,
            query,
            k,
            snapshot: None,
            prefix: None,
            max_expansions: DEFAULT_MAX_EXPANSIONS,
            prefix_length: 0,
        }
    }

    /// The most terms each fuzzy term expands to: 50 by default (D309 §4.4). When more
    /// match, the closest are kept, then the most frequent, then bytewise, so the choice is
    /// deterministic; [`SearchStats::fuzzy`] says which terms were cut. It applies to every
    /// fuzzy term of the query. A search with 0 is refused (`InvalidArgument`).
    pub fn max_expansions(mut self, n: usize) -> Self {
        self.max_expansions = n;
        self
    }

    /// How many leading characters of each fuzzy term must match exactly: 0 by default.
    /// A few narrow the walk over a large dictionary to the terms sharing them. It applies
    /// to every fuzzy term of the query.
    pub fn prefix_length(mut self, n: usize) -> Self {
        self.prefix_length = n;
        self
    }

    /// Searches at `snapshot` rather than the latest state.
    pub fn snapshot(mut self, snapshot: &'a Snapshot) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    /// Only documents whose id starts with `prefix` (for example a namespace, with its
    /// terminator: `agent:alice\0`, so `agent:alice2` isn't included).
    ///
    /// The ranking statistics stay the whole index's (`N`, each term's `df`, the average
    /// length), so scores aren't comparable to those of an index holding only these
    /// documents. Fewer than `k` hits may come back. An empty prefix restricts nothing, and
    /// calling this again replaces the prefix: the last call wins.
    pub fn id_prefix(mut self, prefix: impl Into<Vec<u8>>) -> Self {
        self.prefix = Some(prefix.into());
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
        if self.max_expansions == 0 {
            return Err(Error::InvalidArgument(
                "max_expansions must be at least 1".into(),
            ));
        }
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
        let (terms, phrases, fuzzy) = self.resolve()?;
        if self.k == 0 || (terms.is_empty() && phrases.is_empty() && fuzzy.is_empty()) {
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
        for term in terms
            .iter()
            .chain(phrases.iter().flat_map(|p| p.terms.iter().map(|(t, _)| t)))
        {
            if !df_of.contains_key(term) {
                let df = t
                    .get_at(s, &post_row(term), DF, N)?
                    .and_then(|c| c.as_i64())
                    .unwrap_or(0);
                df_of.insert(term.clone(), df);
            }
        }
        // Each fuzzy term's expansions, from the whole dictionary (not only `id_prefix`'s
        // documents), as the ranking statistics are.
        let mut expanded: Vec<Vec<Expansion>> = Vec::with_capacity(fuzzy.len());
        for f in &fuzzy {
            let (exps, cut) = expand(
                t,
                s,
                &f.term,
                f.edits,
                self.prefix_length,
                self.max_expansions,
            )?;
            stats.fuzzy.push(FuzzyStats {
                clause: f.clause,
                term: f.term.clone(),
                expansions: exps.len(),
                cut,
            });
            expanded.push(exps);
        }
        let fuzzy_stats = std::mem::take(&mut stats.fuzzy);
        let prefix = self.prefix.as_deref();
        // The phrases, scored up front (D309 §4.2): their bounds are their actual maxima.
        let mut phrase_scores = Vec::with_capacity(phrases.len());
        for phrase in &phrases {
            phrase_scores.push(score_phrase(t, s, &bm, &df_of, phrase, prefix)?);
        }
        let (fam, q) = (inn.source.family.as_str(), inn.source.qualifier.as_slice());
        // A stale hit (its source deleted around the index) is skipped and the next one takes
        // its place, so a run that met stale hits runs again for that many more.
        let mut want = self.k;
        loop {
            let mut clauses = Vec::with_capacity(terms.len() + phrases.len());
            for term in &terms {
                let df = df_of[term];
                if df > 0 {
                    clauses.push(Scored::term(t, s, &bm, term, df, prefix)?);
                }
            }
            for exps in &expanded {
                if !exps.is_empty() {
                    // Lucene's blended statistics (TopTermsBlendedFreqScoringRewrite): every
                    // kept expansion scores with the largest df among them, so a rare
                    // misspelling's high idf can't outrank the common word it misspells.
                    let df = exps.iter().map(|e| e.df).max().expect("not empty");
                    let e: Vec<(&str, i64, f64)> = exps
                        .iter()
                        .map(|e| (e.term.as_str(), df, e.boost))
                        .collect();
                    clauses.push(Scored::fuzzy(t, s, &bm, &e, prefix)?);
                }
            }
            for ps in &phrase_scores {
                clauses.push(Scored::phrase(ps.clone()));
            }
            let ranked = crate::maxscore::top_k(t, s, &bm, clauses, want)?;
            let complete = ranked.len() < want;
            let mut hits = Vec::with_capacity(self.k);
            stats = SearchStats {
                fuzzy: fuzzy_stats.clone(),
                ..SearchStats::default()
            };
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
            if hits.len() == self.k || complete {
                return Ok((hits, stats));
            }
            want = self.k + stats.stale as usize;
        }
    }

    /// The query's distinct terms, phrases and fuzzy terms, normalised and tokenised as the
    /// clauses say. A term a clause yields twice counts once; a one-term phrase is a term.
    fn resolve(&self) -> Result<(BTreeSet<String>, Vec<Phrase>, Vec<FuzzyTerm>)> {
        let index = self.index;
        let cap = index.inner.opts.get_max_term_bytes() as usize;
        let mut terms = BTreeSet::new();
        let mut phrases: Vec<Phrase> = Vec::new();
        let mut fuzzy: Vec<FuzzyTerm> = Vec::new();
        let mut add_fuzzy = |clause: usize, term: String, fuzziness: Fuzziness| {
            if term.is_empty() {
                return;
            }
            let edits = edits(&term, fuzziness);
            if !fuzzy.iter().any(|f| f.term == term && f.edits == edits) {
                fuzzy.push(FuzzyTerm {
                    clause,
                    term,
                    edits,
                });
            }
        };
        let mut add_phrase = |p: Phrase, terms: &mut BTreeSet<String>| match p.terms.len() {
            0 => {}
            // A one-term phrase is that term, whatever its slop.
            1 => {
                terms.insert(p.terms.into_iter().next().expect("one").0);
            }
            _ => {
                if !phrases.contains(&p) {
                    phrases.push(p);
                }
            }
        };
        for (at, clause) in self.query.clauses().iter().enumerate() {
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
                    let p = Phrase {
                        terms: words
                            .iter()
                            .zip(0u32..)
                            .map(|(w, i)| (index.normalize_term(w).into_owned(), i))
                            .collect(),
                        slop: *slop,
                    };
                    // A phrase with a word that can't be a term matches nothing.
                    if p.terms.iter().all(|(w, _)| !w.is_empty() && w.len() <= cap) {
                        add_phrase(p, &mut terms);
                    }
                }
                Clause::QuotedPhrase { text, slop } => {
                    let toks = index.query_tokens(text);
                    let first = toks.first().map_or(0, |(_, p)| *p);
                    add_phrase(
                        Phrase {
                            terms: toks.into_iter().map(|(t, p)| (t, p - first)).collect(),
                            slop: *slop,
                        },
                        &mut terms,
                    );
                }
                Clause::Fuzzy { term, fuzziness } => {
                    add_fuzzy(at, index.normalize_term(term).into_owned(), *fuzziness);
                }
                Clause::FuzzyText { text, fuzziness } => {
                    for (term, _) in index.query_tokens(text) {
                        add_fuzzy(at, term, *fuzziness);
                    }
                }
            }
        }
        if !phrases.is_empty() && !index.inner.opts.get_positions() {
            return Err(Error::InvalidArgument(
                "the index has no positions, which a phrase needs".into(),
            ));
        }
        Ok((terms, phrases, fuzzy))
    }
}

/// Adds a phrase's scores: Lucene's phrase scoring, the phrase as one pseudo-term whose idf
/// is the sum of its terms' (D309 §4.3). Its frequency is its exact occurrences at slop 0,
/// and its sloppy frequency (`Σ 1 / (distance + 1)` over its matches, D309 rev 6 §B) above
/// that.
fn score_phrase(
    t: &Rows,
    s: &Snapshot,
    bm: &Bm25,
    df_of: &HashMap<String, i64>,
    phrase: &Phrase,
    prefix: Option<&[u8]>,
) -> Result<BTreeMap<Vec<u8>, f64>> {
    let mut scores = BTreeMap::new();
    let terms = &phrase.terms;
    if terms.iter().any(|(term, _)| df_of[term] <= 0) {
        return Ok(scores);
    }
    let idf: f64 = terms.iter().map(|(term, _)| bm.idf(df_of[term])).sum();
    // Lead with the rarest term; the others narrow its documents.
    let lead = (0..terms.len())
        .min_by_key(|&i| df_of[&terms[i].0])
        .expect("a phrase has terms");
    let mut by_term: BTreeMap<&str, BTreeMap<Vec<u8>, Vec<u32>>> = BTreeMap::new();
    let lead_term = terms[lead].0.as_str();
    by_term.insert(lead_term, read_positions(t, s, lead_term, None, prefix)?);
    for (term, _) in terms {
        if by_term.contains_key(term.as_str()) {
            continue;
        }
        let keep = &by_term[lead_term];
        let got = read_positions(t, s, term, Some(keep), prefix)?;
        by_term.insert(term, got);
    }
    // A repeated word's terms share one group, so the sloppy matcher keeps them apart.
    let groups: Vec<usize> = terms
        .iter()
        .map(|(term, _)| terms.iter().position(|(t, _)| t == term).expect("itself"))
        .collect();
    let lead_off = terms[lead].1;
    for (doc, lead_ps) in &by_term[lead_term] {
        let lists: Option<Vec<&[u32]>> = terms
            .iter()
            .map(|(term, _)| by_term[term.as_str()].get(doc).map(Vec::as_slice))
            .collect();
        // A document without every term can't hold the phrase.
        let Some(lists) = lists else {
            continue;
        };
        let freq = if phrase.slop == 0 {
            let mut n = 0u32;
            'start: for &p in lead_ps {
                for ((_, off), ps) in terms.iter().zip(&lists) {
                    let Some(at) = (p + off).checked_sub(lead_off) else {
                        continue 'start;
                    };
                    if ps.binary_search(&at).is_err() {
                        continue 'start;
                    }
                }
                n += 1;
            }
            f64::from(n)
        } else {
            let walks: Vec<(&[u32], u32, usize)> = lists
                .iter()
                .zip(terms)
                .zip(&groups)
                .map(|((ps, (_, off)), g)| (*ps, *off, *g))
                .collect();
            crate::sloppy::sloppy_freq(&walks, phrase.slop)
        };
        if freq == 0.0 {
            continue;
        }
        let len = match t.get_at(s, &post_row(lead_term), POST, doc)? {
            Some(c) => unposting(c.as_u64().ok_or_else(|| corrupt("a posting isn't a u64"))?).1,
            None => return Err(corrupt("a position has no posting")),
        };
        scores.insert(doc.clone(), idf * bm.tf_factor(freq, f64::from(len)));
    }
    Ok(scores)
}

/// A term's positions by document, for the documents in `keep` (all when `None`).
fn read_positions(
    t: &Rows,
    s: &Snapshot,
    term: &str,
    keep: Option<&BTreeMap<Vec<u8>, Vec<u32>>>,
    prefix: Option<&[u8]>,
) -> Result<BTreeMap<Vec<u8>, Vec<u32>>> {
    let mut out = BTreeMap::new();
    let mut read = t.row(&post_row(term)).family(POS).snapshot(s);
    if let Some(p) = prefix {
        read = read.qualifier_prefix(p);
    }
    let mut cells = read.cells()?;
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
