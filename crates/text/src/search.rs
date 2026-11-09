//! Search (D309 §4): disjunctive BM25 over terms, phrases and fuzzy terms, by MaxScore.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use pigeonhole::Snapshot;

use crate::error::{Error, Result, corrupt};
use crate::fuzzy::{Expansion, edits, expand};
use crate::highlight::{Match, Want, find};
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
    /// With [`Search::highlight`], the document's source text; `None` otherwise.
    pub text: Option<String>,
    /// With [`Search::highlight`], every occurrence of every query clause in
    /// [`text`](Self::text), by position; empty otherwise. [`snippet`](crate::snippet)
    /// picks a window of them. They're computed from the source text as it is at the
    /// search's snapshot: if the source drifted from the index (a write around it, which
    /// [`verify`](crate::Index::verify) reports), they describe the current text, which may
    /// not be what the hit was scored on.
    pub matches: Vec<Match>,
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
    /// The fuzzy term as it was expanded: normalised, and for [`Clause::FuzzyText`]
    /// tokenised and case-folded as documents are. Not the text as typed.
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
    /// The clauses that yield it, the first for [`SearchStats::fuzzy`].
    clauses: Vec<usize>,
    term: String,
    edits: u8,
}

/// A query's clauses resolved ([`Search::resolve`]): its distinct terms, phrases and fuzzy
/// terms, and with highlighting, what each clause looks for.
#[derive(Default)]
struct Resolved {
    terms: BTreeSet<String>,
    phrases: Vec<Phrase>,
    fuzzy: Vec<FuzzyTerm>,
    wants: Vec<(usize, Want)>,
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
    highlight: bool,
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
            highlight: false,
        }
    }

    /// Fills each hit's [`text`](Hit::text) and [`matches`](Hit::matches) (D309 §4.5).
    ///
    /// The matches are recomputed by tokenising each hit's source text again, as it was
    /// indexed, so they always match the text returned. The source cell is read for every
    /// hit anyway, so this costs one tokenising pass per hit, and nothing when it's off.
    /// Offsets are UTF-8 byte offsets. An index without positions still highlights terms
    /// and fuzzy terms.
    pub fn highlight(mut self, on: bool) -> Self {
        self.highlight = on;
        self
    }

    /// The most terms each fuzzy term expands to: 50 by default (D309 §4.4). When more
    /// match, the closest are kept, then the most frequent, then bytewise, so the choice is
    /// deterministic; [`SearchStats::fuzzy`] says which terms were cut. It applies to every
    /// fuzzy term of the query. A search with 0 is refused (`InvalidArgument`).
    pub fn max_expansions(mut self, n: usize) -> Self {
        self.max_expansions = n;
        self
    }

    /// How many leading characters (not bytes) of each fuzzy term must match exactly: 0 by
    /// default. A few narrow the walk over a large dictionary to the terms sharing them. It
    /// applies to every fuzzy term of the query.
    ///
    /// At or above a term's length, the whole term is fixed: it matches itself and the
    /// terms that extend it by up to its edits (`postgr`, two edits, matches `postgres`),
    /// as Lucene's prefix does.
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
        let mut r = self.resolve()?;
        if self.k == 0 || (r.terms.is_empty() && r.phrases.is_empty() && r.fuzzy.is_empty()) {
            return Ok((Vec::new(), SearchStats::default()));
        }
        let counter = |q: &[u8]| -> Result<i64> {
            Ok(t.get_at(s, STATS_ROW, STATS, q)?
                .and_then(|c| c.as_i64())
                .unwrap_or(0))
        };
        let n = counter(DOCS)?;
        if n <= 0 {
            return Ok((Vec::new(), SearchStats::default()));
        }
        let bm = Bm25 {
            n: n as f64,
            avglen: (counter(LEN)?.max(0) as f64 / n as f64).max(f64::MIN_POSITIVE),
            k1: inn.opts.get_k1(),
            b: inn.opts.get_b(),
        };
        let mut df_of: HashMap<String, i64> = HashMap::new();
        for term in r.terms.iter().chain(
            r.phrases
                .iter()
                .flat_map(|p| p.terms.iter().map(|(t, _)| t)),
        ) {
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
        let mut expanded: Vec<Vec<Expansion>> = Vec::with_capacity(r.fuzzy.len());
        let mut fuzzy_stats = Vec::with_capacity(r.fuzzy.len());
        for f in &r.fuzzy {
            let (exps, cut) = expand(
                t,
                s,
                &f.term,
                f.edits,
                self.prefix_length,
                self.max_expansions,
            )?;
            if self.highlight {
                let set: BTreeSet<String> = exps.iter().map(|e| e.term.clone()).collect();
                for &c in &f.clauses {
                    r.wants.push((c, Want::Terms(set.clone())));
                }
            }
            fuzzy_stats.push(FuzzyStats {
                clause: f.clauses[0],
                term: f.term.clone(),
                expansions: exps.len(),
                cut,
            });
            expanded.push(exps);
        }
        let prefix = self.prefix.as_deref();
        // The phrases, scored up front (D309 §4.2): their bounds are their actual maxima.
        let mut phrase_scores = Vec::with_capacity(r.phrases.len());
        for phrase in &r.phrases {
            phrase_scores.push(score_phrase(t, s, &bm, &df_of, phrase, prefix)?);
        }
        let (fam, q) = (inn.source.family.as_str(), inn.source.qualifier.as_slice());
        // A stale hit (its source deleted around the index) is skipped and the next one takes
        // its place, so a run that met stale hits runs again for that many more.
        let mut want = self.k;
        loop {
            let mut clauses = Vec::with_capacity(r.terms.len() + r.phrases.len());
            for term in &r.terms {
                let df = df_of[term];
                if df > 0 {
                    clauses.push(Scored::term(t, s, &bm, term, df, prefix)?);
                }
            }
            for exps in &expanded {
                if !exps.is_empty() {
                    // Lucene's blended statistics (TopTermsBlendedFreqScoringRewrite): every
                    // kept expansion scores with the largest df among them, so a rare
                    // misspelling's idf no longer lifts it above the common word it misspells
                    // (term frequency and length still count).
                    let df = exps.iter().map(|e| e.df).max().expect("not empty");
                    let e: Vec<(&str, i64, u64, f64)> = exps
                        .iter()
                        .map(|e| (e.term.as_str(), df, e.df.max(0) as u64, e.boost))
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
            let mut stats = SearchStats {
                fuzzy: fuzzy_stats.clone(),
                ..SearchStats::default()
            };
            for (id, score) in ranked {
                if hits.len() == self.k {
                    break;
                }
                let Some(cell) = inn.src.get_at(s, &id, fam, q)? else {
                    stats.stale += 1;
                    continue;
                };
                hits.push(self.hit(&r, id, score, cell.value()));
            }
            if hits.len() == self.k || complete {
                return Ok((hits, stats));
            }
            want = self.k + stats.stale as usize;
        }
    }

    /// A hit on `id`, its source holding `value`: with its text and matches when
    /// highlighting.
    fn hit(&self, r: &Resolved, id: Vec<u8>, score: f64, value: &[u8]) -> Hit {
        let (text, matches) = match std::str::from_utf8(value) {
            Ok(text) if self.highlight => {
                let matches = find(&self.index.spans(text), &r.wants);
                (Some(text.to_owned()), matches)
            }
            _ => (None, Vec::new()),
        };
        Hit {
            id,
            score,
            text,
            matches,
        }
    }

    /// [`run`](Self::run) as a future that never blocks on the device (#1560, ICR 0124): the
    /// same hits. Its reads are fetched as it's polled (D196), and it yields after a bounded
    /// amount of work, so a hub term's walk doesn't hold an executor thread.
    ///
    /// ```
    /// use pigeonhole::{Family, Options, Pigeonhole};
    /// use pigeonhole_text::{Index, IndexOptions, Source};
    ///
    /// # fn main() -> pigeonhole_text::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
    /// let docs = db.table("docs")?.family("body", Family::default()).create_if_missing()?;
    /// let index = Index::create(&db, "docs_text", Source::new(&docs, "body", b"text"),
    ///     IndexOptions::new())?;
    /// index.put(b"d1", "the memory store")?;
    /// index.put(b"d2", "a vector store")?;
    /// let hits = pigeonhole::doc_support::block_on(index.search("memory", 10).run_async())?;
    /// assert_eq!(hits.len(), 1);
    /// assert_eq!(hits[0].id, b"d1");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// As [`run`](Self::run).
    #[cfg(feature = "async")]
    pub async fn run_async(self) -> Result<Vec<Hit>> {
        self.run_with_stats_async().await.map(|(hits, _)| hits)
    }

    /// [`run_with_stats`](Self::run_with_stats) as a future, as
    /// [`run_async`](Self::run_async).
    ///
    /// # Errors
    /// As [`run`](Self::run).
    #[cfg(feature = "async")]
    pub async fn run_with_stats_async(self) -> Result<(Vec<Hit>, SearchStats)> {
        use crate::maxscore::{Nonblocking, Polling, TopK};
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
        if t.get_at_async(s, GLOBAL, META, REPAIR).await?.is_some() {
            return Err(Error::Busy);
        }
        let mut r = self.resolve()?;
        if self.k == 0 || (r.terms.is_empty() && r.phrases.is_empty() && r.fuzzy.is_empty()) {
            return Ok((Vec::new(), SearchStats::default()));
        }
        let counter = |q: &'static [u8]| async move {
            Ok::<i64, Error>(
                t.get_at_async(s, STATS_ROW, STATS, q)
                    .await?
                    .and_then(|c| c.as_i64())
                    .unwrap_or(0),
            )
        };
        let n = counter(DOCS).await?;
        if n <= 0 {
            return Ok((Vec::new(), SearchStats::default()));
        }
        let bm = Bm25 {
            n: n as f64,
            avglen: (counter(LEN).await?.max(0) as f64 / n as f64).max(f64::MIN_POSITIVE),
            k1: inn.opts.get_k1(),
            b: inn.opts.get_b(),
        };
        let mut df_of: HashMap<String, i64> = HashMap::new();
        // Collected first: an iterator chain with closures held across an `.await` makes the
        // future not `Send`.
        let terms: Vec<String> = r
            .terms
            .iter()
            .chain(
                r.phrases
                    .iter()
                    .flat_map(|p| p.terms.iter().map(|(t, _)| t)),
            )
            .cloned()
            .collect();
        for term in terms {
            if let std::collections::hash_map::Entry::Vacant(e) = df_of.entry(term) {
                let df = t
                    .get_at_async(s, &post_row(e.key()), DF, N)
                    .await?
                    .and_then(|c| c.as_i64())
                    .unwrap_or(0);
                e.insert(df);
            }
        }
        let mut expanded: Vec<Vec<Expansion>> = Vec::with_capacity(r.fuzzy.len());
        let mut fuzzy_stats = Vec::with_capacity(r.fuzzy.len());
        for f in &r.fuzzy {
            let (exps, cut) = crate::fuzzy::expand_async(
                t,
                s,
                &f.term,
                f.edits,
                self.prefix_length,
                self.max_expansions,
            )
            .await?;
            if self.highlight {
                let set: BTreeSet<String> = exps.iter().map(|e| e.term.clone()).collect();
                for &c in &f.clauses {
                    r.wants.push((c, Want::Terms(set.clone())));
                }
            }
            fuzzy_stats.push(FuzzyStats {
                clause: f.clauses[0],
                term: f.term.clone(),
                expansions: exps.len(),
                cut,
            });
            expanded.push(exps);
        }
        let prefix = self.prefix.as_deref();
        let mut phrase_scores = Vec::with_capacity(r.phrases.len());
        for phrase in &r.phrases {
            phrase_scores.push(score_phrase_async(t, s, &bm, &df_of, phrase, prefix).await?);
        }
        let (fam, q) = (inn.source.family.as_str(), inn.source.qualifier.as_slice());
        let mut want = self.k;
        loop {
            let mut clauses: Vec<Scored<'_, Nonblocking>> =
                Vec::with_capacity(r.terms.len() + r.phrases.len());
            for term in &r.terms {
                let df = df_of[term];
                if df > 0 {
                    let w = crate::maxscore::walk_async(
                        t,
                        s,
                        &bm,
                        term,
                        bm.idf(df),
                        df.max(0) as u64,
                        prefix,
                    )
                    .await?;
                    clauses.push(Scored::of_term(w));
                }
            }
            for exps in &expanded {
                if !exps.is_empty() {
                    // Lucene's blended statistics, as `run_with_stats`.
                    let df = exps.iter().map(|e| e.df).max().expect("not empty");
                    let mut walks = Vec::with_capacity(exps.len());
                    for e in exps {
                        walks.push(
                            crate::maxscore::walk_async(
                                t,
                                s,
                                &bm,
                                &e.term,
                                e.boost * bm.idf(df),
                                e.df.max(0) as u64,
                                prefix,
                            )
                            .await?,
                        );
                    }
                    clauses.push(Scored::of_fuzzy(walks));
                }
            }
            for ps in &phrase_scores {
                clauses.push(Scored::phrase(ps.clone()));
            }
            let mut run = TopK::new(clauses, want);
            std::future::poll_fn(|cx| run.run(t, s, &bm, &mut Polling::new(cx))).await?;
            let ranked = run.finish();
            let complete = ranked.len() < want;
            let mut hits = Vec::with_capacity(self.k);
            let mut stats = SearchStats {
                fuzzy: fuzzy_stats.clone(),
                ..SearchStats::default()
            };
            for (id, score) in ranked {
                if hits.len() == self.k {
                    break;
                }
                let Some(cell) = inn.src.get_at_async(s, &id, fam, q).await? else {
                    stats.stale += 1;
                    continue;
                };
                hits.push(self.hit(&r, id, score, cell.value()));
            }
            if hits.len() == self.k || complete {
                return Ok((hits, stats));
            }
            want = self.k + stats.stale as usize;
        }
    }

    /// The query's distinct terms, phrases and fuzzy terms, normalised and tokenised as the
    /// clauses say. A term a clause yields twice counts once; a one-term phrase is a term.
    /// With [`highlight`](Self::highlight), also what each clause looks for (its fuzzy
    /// terms' expansions are added once they're known).
    fn resolve(&self) -> Result<Resolved> {
        let index = self.index;
        let cap = index.inner.opts.get_max_term_bytes() as usize;
        let mut r = Resolved::default();
        let mut add_fuzzy = |clause: usize, term: String, fuzziness: Fuzziness| {
            if term.is_empty() {
                return;
            }
            let edits = edits(&term, fuzziness);
            match r
                .fuzzy
                .iter_mut()
                .find(|f| f.term == term && f.edits == edits)
            {
                Some(f) => {
                    if !f.clauses.contains(&clause) {
                        f.clauses.push(clause);
                    }
                }
                None => r.fuzzy.push(FuzzyTerm {
                    clauses: vec![clause],
                    term,
                    edits,
                }),
            }
        };
        let mut want = |clause: usize, w: Want| {
            if self.highlight {
                r.wants.push((clause, w));
            }
        };
        let mut add_phrase = |p: Phrase, terms: &mut BTreeSet<String>| match p.terms.len() {
            0 => {}
            // A one-term phrase is that term, whatever its slop.
            1 => {
                terms.insert(p.terms.into_iter().next().expect("one").0);
            }
            _ => {
                if !r.phrases.contains(&p) {
                    r.phrases.push(p);
                }
            }
        };
        for (at, clause) in self.query.clauses().iter().enumerate() {
            match clause {
                Clause::Term(t) => {
                    let t = index.normalize_term(t);
                    // A builder term over the cap can't match: the index dropped such words.
                    if !t.is_empty() && t.len() <= cap {
                        want(at, Want::Terms(BTreeSet::from([t.to_string()])));
                        r.terms.insert(t.into_owned());
                    }
                }
                Clause::Text(text) => {
                    let ts: BTreeSet<String> = index
                        .query_tokens(text)
                        .into_iter()
                        .map(|(t, _)| t)
                        .collect();
                    if !ts.is_empty() {
                        want(at, Want::Terms(ts.clone()));
                    }
                    r.terms.extend(ts);
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
                        if let Some(w) = p.want() {
                            want(at, w);
                        }
                        add_phrase(p, &mut r.terms);
                    }
                }
                Clause::QuotedPhrase { text, slop } => {
                    let toks = index.query_tokens(text);
                    let first = toks.first().map_or(0, |(_, p)| *p);
                    let p = Phrase {
                        terms: toks.into_iter().map(|(t, p)| (t, p - first)).collect(),
                        slop: *slop,
                    };
                    if let Some(w) = p.want() {
                        want(at, w);
                    }
                    add_phrase(p, &mut r.terms);
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
        if !r.phrases.is_empty() && !index.inner.opts.get_positions() {
            return Err(Error::InvalidArgument(
                "the index has no positions, which a phrase needs".into(),
            ));
        }
        Ok(r)
    }
}

impl Phrase {
    /// What highlighting looks for: a one-term phrase is that term.
    fn want(&self) -> Option<Want> {
        match self.terms.len() {
            0 => None,
            1 => Some(Want::Terms(BTreeSet::from([self.terms[0].0.clone()]))),
            _ => Some(Want::Phrase {
                terms: self.terms.clone(),
                slop: self.slop,
            }),
        }
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
    let Some(plan) = PhrasePlan::new(bm, df_of, phrase) else {
        return Ok(scores);
    };
    let terms = &phrase.terms;
    let mut by_term: BTreeMap<&str, BTreeMap<Vec<u8>, Vec<u32>>> = BTreeMap::new();
    let lead_term = terms[plan.lead].0.as_str();
    by_term.insert(lead_term, read_positions(t, s, lead_term, None, prefix)?);
    for (term, _) in terms {
        if by_term.contains_key(term.as_str()) {
            continue;
        }
        let keep = &by_term[lead_term];
        let got = read_positions(t, s, term, Some(keep), prefix)?;
        by_term.insert(term, got);
    }
    for (doc, lead_ps) in &by_term[lead_term] {
        let Some(freq) = plan.freq(phrase, &by_term, doc, lead_ps) else {
            continue;
        };
        let len = match t.get_at(s, &post_row(lead_term), POST, doc)? {
            Some(c) => c.as_u64(),
            None => return Err(corrupt("a position has no posting")),
        };
        scores.insert(doc.clone(), plan.score(bm, freq, len)?);
    }
    Ok(scores)
}

/// [`score_phrase`] as a future (#1560): the same positions and lengths, read without
/// blocking.
#[cfg(feature = "async")]
async fn score_phrase_async(
    t: &Rows,
    s: &Snapshot,
    bm: &Bm25,
    df_of: &HashMap<String, i64>,
    phrase: &Phrase,
    prefix: Option<&[u8]>,
) -> Result<BTreeMap<Vec<u8>, f64>> {
    let mut scores = BTreeMap::new();
    let Some(plan) = PhrasePlan::new(bm, df_of, phrase) else {
        return Ok(scores);
    };
    let terms = &phrase.terms;
    let mut by_term: BTreeMap<&str, BTreeMap<Vec<u8>, Vec<u32>>> = BTreeMap::new();
    let lead_term = terms[plan.lead].0.as_str();
    let lead = read_positions_async(t, s, lead_term, None, prefix).await?;
    by_term.insert(lead_term, lead);
    for (term, _) in terms {
        if by_term.contains_key(term.as_str()) {
            continue;
        }
        let got = read_positions_async(t, s, term, Some(&by_term[lead_term]), prefix).await?;
        by_term.insert(term, got);
    }
    for (doc, lead_ps) in &by_term[lead_term] {
        let Some(freq) = plan.freq(phrase, &by_term, doc, lead_ps) else {
            continue;
        };
        let len = match t.get_at_async(s, &post_row(lead_term), POST, doc).await? {
            Some(c) => c.as_u64(),
            None => return Err(corrupt("a position has no posting")),
        };
        scores.insert(doc.clone(), plan.score(bm, freq, len)?);
    }
    Ok(scores)
}

/// What a phrase scores with, past its reads: its idf (the sum of its terms'), its lead term
/// (the rarest: the others narrow its documents), and each term's group (a repeated word's
/// terms share one, so the sloppy matcher keeps them apart).
struct PhrasePlan {
    idf: f64,
    lead: usize,
    groups: Vec<usize>,
}

impl PhrasePlan {
    /// `None` when a term isn't in the index: no document can hold the phrase.
    fn new(bm: &Bm25, df_of: &HashMap<String, i64>, phrase: &Phrase) -> Option<Self> {
        let terms = &phrase.terms;
        if terms.iter().any(|(term, _)| df_of[term] <= 0) {
            return None;
        }
        let idf: f64 = terms.iter().map(|(term, _)| bm.idf(df_of[term])).sum();
        let lead = (0..terms.len())
            .min_by_key(|&i| df_of[&terms[i].0])
            .expect("a phrase has terms");
        let groups = terms
            .iter()
            .map(|(term, _)| terms.iter().position(|(t, _)| t == term).expect("itself"))
            .collect();
        Some(PhrasePlan { idf, lead, groups })
    }

    /// The phrase's frequency in `doc`, from its terms' positions there; `None` when it
    /// doesn't occur (a term is missing, or no match).
    fn freq(
        &self,
        phrase: &Phrase,
        by_term: &BTreeMap<&str, BTreeMap<Vec<u8>, Vec<u32>>>,
        doc: &[u8],
        lead_ps: &[u32],
    ) -> Option<f64> {
        let terms = &phrase.terms;
        let lists: Option<Vec<&[u32]>> = terms
            .iter()
            .map(|(term, _)| by_term[term.as_str()].get(doc).map(Vec::as_slice))
            .collect();
        // A document without every term can't hold the phrase.
        let lists = lists?;
        let lead_off = terms[self.lead].1;
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
                .zip(&self.groups)
                .map(|((ps, (_, off)), g)| (*ps, *off, *g))
                .collect();
            crate::sloppy::sloppy_freq(&walks, phrase.slop)
        };
        (freq != 0.0).then_some(freq)
    }

    /// The phrase's score in a document where it occurs `freq` times, from the lead term's
    /// posting there (its value: frequency and length).
    fn score(&self, bm: &Bm25, freq: f64, posting: Option<u64>) -> Result<f64> {
        let len = unposting(posting.ok_or_else(|| corrupt("a posting isn't a u64"))?).1;
        Ok(self.idf * bm.tf_factor(freq, f64::from(len)))
    }
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

/// [`read_positions`] as a future: the position row walked with [`RowCells::next_async`].
#[cfg(feature = "async")]
async fn read_positions_async(
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
    while let Some(e) = cells.next_async().await? {
        if keep.is_some_and(|k| !k.contains_key(e.qualifier)) {
            continue;
        }
        let mut ps = Vec::new();
        decode_positions(e.cell.value(), &mut ps)?;
        out.insert(e.qualifier.to_vec(), ps);
    }
    Ok(out)
}
