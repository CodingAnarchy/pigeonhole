//! Search (D309 §4): disjunctive BM25 over terms, phrases and fuzzy terms, by MaxScore.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use pigeonhole::Snapshot;

use crate::error::{Error, Result, corrupt};
use crate::fuzzy::{edits, expand};
use crate::highlight::{Match, Want, find};
use crate::index::{Index, Rows};
use crate::layout::*;
use crate::maxscore::{Blocking, Bm25, Cells, Layout, Packed, Scored, sync};
use crate::merged::{Merged, Side, Tier};
use crate::query::{Clause, Fuzziness, Query};
use crate::stats::{Fuzzy, QueryStats, Stats, other_query};

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

impl Resolved {
    /// Whether no clause can match anything.
    fn is_empty(&self) -> bool {
        self.terms.is_empty() && self.phrases.is_empty() && self.fuzzy.is_empty()
    }

    /// Its terms, then its phrases' terms (a term may come more than once).
    fn all_terms(&self) -> impl Iterator<Item = &String> {
        self.terms.iter().chain(
            self.phrases
                .iter()
                .flat_map(|p| p.terms.iter().map(|(t, _)| t)),
        )
    }
}

/// The `df`s of a search's terms and phrase terms: the one each scores with, and its
/// postings in this index, which a walk steps through. They're the same numbers (`here` is
/// `None`) unless the search was given statistics ([`Search::stats`], ICR 0138).
#[derive(Clone, Copy)]
struct Dfs<'a> {
    score: &'a HashMap<String, i64>,
    here: Option<&'a HashMap<String, i64>>,
}

impl Dfs<'_> {
    /// The `df` `term` scores with.
    fn score(&self, term: &str) -> i64 {
        self.score[term]
    }

    /// `term`'s postings here.
    fn here(&self, term: &str) -> i64 {
        self.here.unwrap_or(self.score)[term]
    }

    /// Both, with one lookup when they're the same.
    fn both(&self, term: &str) -> (i64, i64) {
        let score = self.score[term];
        (score, self.here.map_or(score, |h| h[term]))
    }
}

/// What a search given statistics reads of its own index (ICR 0138): the postings here of
/// each term they hold, and of each fuzzy term's given expansions, in their order. A term
/// can have a `df` in the statistics and no postings here.
struct Here {
    df: HashMap<String, i64>,
    fuzzy: Vec<Vec<i64>>,
}

impl Here {
    fn read(t: &Rows, s: &Snapshot, given: &Stats) -> Result<Here> {
        let mut df = HashMap::with_capacity(given.df.len());
        for term in given.df.keys() {
            df.insert(term.clone(), term_df(t, s, term)?);
        }
        let mut fuzzy = Vec::with_capacity(given.fuzzy.len());
        for f in &given.fuzzy {
            let mut ns = Vec::with_capacity(f.expansions.len());
            for e in &f.expansions {
                ns.push(term_df(t, s, &e.term)?);
            }
            fuzzy.push(ns);
        }
        Ok(Here { df, fuzzy })
    }

    #[cfg(feature = "async")]
    async fn read_async(t: &Rows, s: &Snapshot, given: &Stats) -> Result<Here> {
        let mut df = HashMap::with_capacity(given.df.len());
        let terms: Vec<&String> = given.df.keys().collect();
        for term in terms {
            df.insert(term.clone(), term_df_async(t, s, term).await?);
        }
        let mut fuzzy = Vec::with_capacity(given.fuzzy.len());
        for f in &given.fuzzy {
            let mut ns = Vec::with_capacity(f.expansions.len());
            for e in &f.expansions {
                ns.push(term_df_async(t, s, &e.term).await?);
            }
            fuzzy.push(ns);
        }
        Ok(Here { df, fuzzy })
    }
}

/// `term`'s document frequency: its posting row's `df` counter.
fn term_df(t: &Rows, s: &Snapshot, term: &str) -> Result<i64> {
    Ok(t.get_at(s, &post_row(term), DF, N)?
        .and_then(|c| c.as_i64())
        .unwrap_or(0))
}

/// [`term_df`] as a future.
#[cfg(feature = "async")]
async fn term_df_async(t: &Rows, s: &Snapshot, term: &str) -> Result<i64> {
    Ok(t.get_at_async(s, &post_row(term), DF, N)
        .await?
        .and_then(|c| c.as_i64())
        .unwrap_or(0))
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
    /// The statistics to score with ([`Search::stats`]), or `None` for its own.
    given: Option<QueryStats>,
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
            given: None,
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
    /// on an index without positions, or for [`stats`](Self::stats) of another query. It
    /// fails with `Busy` while a [`repair`](Index::repair) runs: retry after it finishes.
    pub fn run(self) -> Result<Vec<Hit>> {
        self.run_with_stats().map(|(hits, _)| hits)
    }

    /// Scores with `stats` instead of this index's own statistics (#1722, ICR 0138): the
    /// merged statistics of every index a collection is split into, so each part's scores
    /// are the whole collection's ([`QueryStats`] has the steps). Each fuzzy term expands
    /// to the terms `stats` kept, whatever this index's own walk would keep.
    ///
    /// The statistics must be of this search's query, read with the same
    /// [`max_expansions`](Self::max_expansions) and [`prefix_length`](Self::prefix_length),
    /// or the search fails with `InvalidArgument`.
    pub fn stats(mut self, stats: QueryStats) -> Self {
        self.given = Some(stats);
        self
    }

    /// This index's statistics for the query (#1722, ICR 0138): its document count and
    /// length, each term's document frequency and each fuzzy term's expansions, all read at
    /// one snapshot (the search's, or the latest). [`QueryStats`] says what they're for.
    ///
    /// They're the whole index's: `k`, [`id_prefix`](Self::id_prefix),
    /// [`highlight`](Self::highlight) and [`stats`](Self::stats) don't change them. The
    /// fuzzy options do.
    ///
    /// # Errors
    /// As [`run`](Self::run).
    pub fn query_stats(self) -> Result<QueryStats> {
        self.check()?;
        let index = self.index;
        let owned;
        let s = match self.snapshot {
            Some(s) => s,
            None => {
                owned = index.inner.db.snapshot()?;
                &owned
            }
        };
        let packed = match index.inner.table.get_at(s, GLOBAL, META, REPAIR)? {
            None => false,
            Some(c) => crate::index::packed_state(c.value())?,
        };
        let t = index.rows(packed)?;
        let mut r = self.resolve()?;
        Ok(QueryStats(self.gather(t, s, &mut r)?))
    }

    /// [`run`](Self::run), with what the search met.
    pub fn run_with_stats(self) -> Result<(Vec<Hit>, SearchStats)> {
        self.check()?;
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
        // The index's state, in one read ([`REPAIR`]): a repair running, or the layout.
        let packed = match inn.table.get_at(s, GLOBAL, META, REPAIR)? {
            None => false,
            Some(c) => crate::index::packed_state(c.value())?,
        };
        let t = index.rows(packed)?;
        let mut r = self.resolve()?;
        if self.k == 0 || r.is_empty() {
            return Ok((Vec::new(), SearchStats::default()));
        }
        // The statistics it scores with: its own, or the ones it was given.
        let given = self.given.as_ref().map(|g| &g.0);
        let gathered;
        let stats = match given {
            Some(g) => {
                self.matches(&r, g)?;
                g
            }
            None => {
                gathered = self.gather(t, s, &mut r)?;
                &gathered
            }
        };
        let Some(bm) = stats.bm25(inn.opts.get_k1(), inn.opts.get_b()) else {
            return Ok((Vec::new(), SearchStats::default()));
        };
        // Given statistics: the terms' postings here, which its walks step through.
        let here = match given {
            Some(g) => Some(Here::read(t, s, g)?),
            None => None,
        };
        let dfs = Dfs {
            score: &stats.df,
            here: here.as_ref().map(|h| &h.df),
        };
        let postings = here.as_ref().map(|h| h.fuzzy.as_slice());
        let fuzzy_stats = self.fuzzy_stats(&mut r, stats);
        let prefix = self.prefix.as_deref();
        // The phrases, scored up front (D309 §4.2): their bounds are their actual maxima.
        let mut phrase_scores = Vec::with_capacity(r.phrases.len());
        for phrase in &r.phrases {
            phrase_scores.push(score_phrase(t, s, &bm, dfs, phrase, prefix, packed)?);
        }
        let (fam, q) = (inn.source.family.as_str(), inn.source.qualifier.as_slice());
        // A stale hit (its source deleted around the index) is skipped and the next one takes
        // its place, so a run that met stale hits runs again for that many more.
        let mut want = self.k;
        loop {
            // One document loop per layout ([`Layout`]): a version 1 index's is the code it
            // was.
            let ranked = match packed {
                true => rank::<Packed>(
                    t,
                    s,
                    &bm,
                    &r.terms,
                    dfs,
                    &stats.fuzzy,
                    postings,
                    &phrase_scores,
                    prefix,
                    want,
                )?,
                false => rank::<Cells>(
                    t,
                    s,
                    &bm,
                    &r.terms,
                    dfs,
                    &stats.fuzzy,
                    postings,
                    &phrase_scores,
                    prefix,
                    want,
                )?,
            };
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

    /// The checks before any read: the query parses, and `max_expansions` isn't 0.
    fn check(&self) -> Result<()> {
        self.query.check()?;
        if self.max_expansions == 0 {
            return Err(Error::InvalidArgument(
                "max_expansions must be at least 1".into(),
            ));
        }
        Ok(())
    }

    /// The index's statistics for the resolved query `r`, at `s`. Its fuzzy terms move
    /// into them.
    fn gather(&self, t: &Rows, s: &Snapshot, r: &mut Resolved) -> Result<Stats> {
        let counter = |q: &[u8]| -> Result<i64> {
            Ok(t.get_at(s, STATS_ROW, STATS, q)?
                .and_then(|c| c.as_i64())
                .unwrap_or(0))
        };
        let docs = counter(DOCS)?;
        let len = counter(LEN)?;
        let mut df: HashMap<String, i64> = HashMap::new();
        for term in r.all_terms() {
            if !df.contains_key(term) {
                df.insert(term.clone(), term_df(t, s, term)?);
            }
        }
        // Each fuzzy term's expansions, from the whole dictionary (not only `id_prefix`'s
        // documents), as the ranking statistics are.
        let mut fuzzy = Vec::with_capacity(r.fuzzy.len());
        for f in std::mem::take(&mut r.fuzzy) {
            let (expansions, cut) = expand(
                t,
                s,
                &f.term,
                f.edits,
                self.prefix_length,
                self.max_expansions,
            )?;
            fuzzy.push(Fuzzy {
                clauses: f.clauses,
                term: f.term,
                edits: f.edits,
                expansions,
                cut,
            });
        }
        Ok(Stats {
            docs,
            len,
            max_expansions: self.max_expansions,
            prefix_length: self.prefix_length,
            df,
            fuzzy,
        })
    }

    /// `InvalidArgument` unless `given` holds the statistics of the resolved query `r`,
    /// read with this search's fuzzy options.
    fn matches(&self, r: &Resolved, given: &Stats) -> Result<()> {
        let terms: BTreeSet<&String> = r.all_terms().collect();
        let same = given.max_expansions == self.max_expansions
            && given.prefix_length == self.prefix_length
            && terms.len() == given.df.len()
            && terms.iter().all(|t| given.df.contains_key(*t))
            && given.fuzzy.len() == r.fuzzy.len()
            && given
                .fuzzy
                .iter()
                .zip(&r.fuzzy)
                .all(|(g, f)| g.term == f.term && g.edits == f.edits && g.clauses == f.clauses);
        match same {
            true => Ok(()),
            false => Err(other_query()),
        }
    }

    /// What [`SearchStats::fuzzy`] reports of `stats`' fuzzy terms; with highlighting, each
    /// one's expansions are added to what its clauses look for.
    fn fuzzy_stats(&self, r: &mut Resolved, stats: &Stats) -> Vec<FuzzyStats> {
        let mut out = Vec::with_capacity(stats.fuzzy.len());
        for f in &stats.fuzzy {
            if self.highlight {
                let set: BTreeSet<String> = f.expansions.iter().map(|e| e.term.clone()).collect();
                for &c in &f.clauses {
                    r.wants.push((c, Want::Terms(set.clone())));
                }
            }
            out.push(FuzzyStats {
                clause: f.clauses[0],
                term: f.term.clone(),
                expansions: f.expansions.len(),
                cut: f.cut,
            });
        }
        out
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

    /// [`query_stats`](Self::query_stats) as a future, as [`run_async`](Self::run_async).
    ///
    /// # Errors
    /// As [`run`](Self::run).
    #[cfg(feature = "async")]
    pub async fn query_stats_async(self) -> Result<QueryStats> {
        self.check()?;
        let index = self.index;
        let owned;
        let s = match self.snapshot {
            Some(s) => s,
            None => {
                owned = index.inner.db.snapshot()?;
                &owned
            }
        };
        let packed = match index
            .inner
            .table
            .get_at_async(s, GLOBAL, META, REPAIR)
            .await?
        {
            None => false,
            Some(c) => crate::index::packed_state(c.value())?,
        };
        let t = index.rows(packed)?;
        let mut r = self.resolve()?;
        let mut budget = crate::maxscore::Budget::new();
        Ok(QueryStats(
            self.gather_async(t, s, &mut r, &mut budget).await?,
        ))
    }

    /// [`run_with_stats`](Self::run_with_stats) as a future, as
    /// [`run_async`](Self::run_async).
    ///
    /// # Errors
    /// As [`run`](Self::run).
    #[cfg(feature = "async")]
    pub async fn run_with_stats_async(self) -> Result<(Vec<Hit>, SearchStats)> {
        self.check()?;
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
        let packed = match inn.table.get_at_async(s, GLOBAL, META, REPAIR).await? {
            None => false,
            Some(c) => crate::index::packed_state(c.value())?,
        };
        let t = index.rows(packed)?;
        let mut r = self.resolve()?;
        if self.k == 0 || r.is_empty() {
            return Ok((Vec::new(), SearchStats::default()));
        }
        // One step budget for the search's work outside MaxScore (ai1249's R1 on #1642): the
        // fuzzy walks, the phrase position walks and their lengths. Shared, so a poll does
        // at most a budget's work across them, not a budget per loop.
        let mut budget = crate::maxscore::Budget::new();
        let given = self.given.as_ref().map(|g| &g.0);
        let gathered;
        let stats = match given {
            Some(g) => {
                self.matches(&r, g)?;
                g
            }
            None => {
                gathered = self.gather_async(t, s, &mut r, &mut budget).await?;
                &gathered
            }
        };
        let Some(bm) = stats.bm25(inn.opts.get_k1(), inn.opts.get_b()) else {
            return Ok((Vec::new(), SearchStats::default()));
        };
        let here = match given {
            Some(g) => Some(Here::read_async(t, s, g).await?),
            None => None,
        };
        let dfs = Dfs {
            score: &stats.df,
            here: here.as_ref().map(|h| &h.df),
        };
        let postings = here.as_ref().map(|h| h.fuzzy.as_slice());
        let fuzzy_stats = self.fuzzy_stats(&mut r, stats);
        let prefix = self.prefix.as_deref();
        let mut phrase_scores = Vec::with_capacity(r.phrases.len());
        for phrase in &r.phrases {
            phrase_scores.push(
                score_phrase_async(t, s, &bm, dfs, phrase, prefix, packed, &mut budget).await?,
            );
        }
        let (fam, q) = (inn.source.family.as_str(), inn.source.qualifier.as_slice());
        let mut want = self.k;
        loop {
            let ranked = match packed {
                true => {
                    rank_async::<Packed>(
                        t,
                        s,
                        &bm,
                        &r.terms,
                        dfs,
                        &stats.fuzzy,
                        postings,
                        &phrase_scores,
                        prefix,
                        want,
                    )
                    .await?
                }
                false => {
                    rank_async::<Cells>(
                        t,
                        s,
                        &bm,
                        &r.terms,
                        dfs,
                        &stats.fuzzy,
                        postings,
                        &phrase_scores,
                        prefix,
                        want,
                    )
                    .await?
                }
            };
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

    /// [`gather`](Self::gather) as a future: the fuzzy walks spend `budget`.
    #[cfg(feature = "async")]
    async fn gather_async(
        &self,
        t: &Rows,
        s: &Snapshot,
        r: &mut Resolved,
        budget: &mut crate::maxscore::Budget,
    ) -> Result<Stats> {
        let counter = |q: &'static [u8]| async move {
            Ok::<i64, Error>(
                t.get_at_async(s, STATS_ROW, STATS, q)
                    .await?
                    .and_then(|c| c.as_i64())
                    .unwrap_or(0),
            )
        };
        let docs = counter(DOCS).await?;
        let len = counter(LEN).await?;
        let mut df: HashMap<String, i64> = HashMap::new();
        // Collected first: an iterator chain with closures held across an `.await` makes the
        // future not `Send`.
        let terms: Vec<String> = r.all_terms().cloned().collect();
        for term in terms {
            if let std::collections::hash_map::Entry::Vacant(e) = df.entry(term) {
                let n = term_df_async(t, s, e.key()).await?;
                e.insert(n);
            }
        }
        let mut fuzzy = Vec::with_capacity(r.fuzzy.len());
        for f in std::mem::take(&mut r.fuzzy) {
            let (expansions, cut) = crate::fuzzy::expand_async(
                t,
                s,
                &f.term,
                f.edits,
                self.prefix_length,
                self.max_expansions,
                budget,
            )
            .await?;
            fuzzy.push(Fuzzy {
                clauses: f.clauses,
                term: f.term,
                edits: f.edits,
                expansions,
                cut,
            });
        }
        Ok(Stats {
            docs,
            len,
            max_expansions: self.max_expansions,
            prefix_length: self.prefix_length,
            df,
            fuzzy,
        })
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

/// The top `want` documents of the query's clauses, by MaxScore, over the layout `L`. A
/// term, or a fuzzy term's expansion, with no postings here adds no clause.
#[allow(clippy::too_many_arguments)]
#[inline(always)]
fn rank<L: Layout>(
    t: &Rows,
    s: &Snapshot,
    bm: &Bm25,
    terms: &BTreeSet<String>,
    dfs: Dfs<'_>,
    fuzzy: &[Fuzzy],
    postings: Option<&[Vec<i64>]>,
    phrase_scores: &[BTreeMap<Vec<u8>, f64>],
    prefix: Option<&[u8]>,
    want: usize,
) -> Result<Vec<(Vec<u8>, f64)>> {
    let mut clauses = Vec::with_capacity(terms.len() + phrase_scores.len());
    for term in terms {
        let (df, here) = dfs.both(term);
        if here > 0 {
            clauses.push(Scored::<Blocking, L>::term(
                t,
                s,
                bm,
                term,
                df,
                here as u64,
                prefix,
            )?);
        }
    }
    for (i, f) in fuzzy.iter().enumerate() {
        let e = expansions(f, i, postings);
        if !e.is_empty() {
            clauses.push(Scored::fuzzy(t, s, bm, &e, prefix)?);
        }
    }
    for ps in phrase_scores {
        clauses.push(Scored::phrase(ps.clone()));
    }
    crate::maxscore::top_k(t, s, bm, clauses, want)
}

/// The `i`th fuzzy term's expansions with postings here, as [`Scored::fuzzy`] takes them:
/// `(term, df, postings, boost)`. Lucene's blended statistics
/// (TopTermsBlendedFreqScoringRewrite): every kept expansion scores with the largest `df`
/// among them, so a rare misspelling's idf no longer lifts it above the common word it
/// misspells (term frequency and length still count).
fn expansions<'f>(
    f: &'f Fuzzy,
    i: usize,
    postings: Option<&[Vec<i64>]>,
) -> Vec<(&'f str, i64, u64, f64)> {
    let Some(df) = f.expansions.iter().map(|e| e.df).max() else {
        return Vec::new();
    };
    f.expansions
        .iter()
        .enumerate()
        .filter_map(|(j, e)| {
            let here = postings.map_or(e.df, |p| p[i][j]);
            (here > 0).then_some((e.term.as_str(), df, here as u64, e.boost))
        })
        .collect()
}

/// [`rank`] as a future (#1560).
#[cfg(feature = "async")]
#[allow(clippy::too_many_arguments)]
async fn rank_async<L: Layout>(
    t: &Rows,
    s: &Snapshot,
    bm: &Bm25,
    terms: &BTreeSet<String>,
    dfs: Dfs<'_>,
    fuzzy: &[Fuzzy],
    postings: Option<&[Vec<i64>]>,
    phrase_scores: &[BTreeMap<Vec<u8>, f64>],
    prefix: Option<&[u8]>,
    want: usize,
) -> Result<Vec<(Vec<u8>, f64)>> {
    use crate::maxscore::{Nonblocking, Polling, TopK};
    let mut clauses: Vec<Scored<'_, Nonblocking, L>> =
        Vec::with_capacity(terms.len() + phrase_scores.len());
    for term in terms {
        let (df, here) = dfs.both(term);
        if here > 0 {
            let w = crate::maxscore::walk_async(t, s, bm, term, bm.idf(df), here as u64, prefix)
                .await?;
            clauses.push(Scored::of_term(w));
        }
    }
    for (i, f) in fuzzy.iter().enumerate() {
        let e = expansions(f, i, postings);
        if !e.is_empty() {
            let mut walks = Vec::with_capacity(e.len());
            for (term, df, postings, boost) in e {
                walks.push(
                    crate::maxscore::walk_async(
                        t,
                        s,
                        bm,
                        term,
                        boost * bm.idf(df),
                        postings,
                        prefix,
                    )
                    .await?,
                );
            }
            clauses.push(Scored::of_fuzzy(walks));
        }
    }
    for ps in phrase_scores {
        clauses.push(Scored::phrase(ps.clone()));
    }
    let mut run = TopK::new(clauses, want);
    std::future::poll_fn(|cx| run.run(t, s, bm, &mut Polling::new(cx))).await?;
    Ok(run.finish())
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
///
/// A join in document order (#1586): the lead term's position row is walked, and each other
/// term is brought to the lead's document ([`Joined`]). Positions are decoded only for a
/// document every term has, into buffers reused from one document to the next.
fn score_phrase(
    t: &Rows,
    s: &Snapshot,
    bm: &Bm25,
    dfs: Dfs<'_>,
    phrase: &Phrase,
    prefix: Option<&[u8]>,
    packed: bool,
) -> Result<BTreeMap<Vec<u8>, f64>> {
    let Some(plan) = PhrasePlan::new(bm, dfs, phrase) else {
        return Ok(BTreeMap::new());
    };
    let row = post_row(&phrase.terms[plan.lead].0);
    let cells = cells_of(t, s, &row, POS, prefix).cells()?;
    if packed {
        let lead = MergedLead::new(t, s, &row, cells, prefix)?;
        join::<Packed>(t, s, bm, dfs, phrase, prefix, &plan, lead)
    } else {
        join::<Cells>(t, s, bm, dfs, phrase, prefix, &plan, cells)
    }
}

/// The lead term's positions in document order: its position cells in a version 1 index
/// (the walk the join had before #1585, monomorphized to the same code), the merged stream
/// in a version 2 one.
trait Lead {
    /// A posting of the lead's, lent until the next step.
    type At<'a>: At
    where
        Self: 'a;

    /// The next document with the term.
    fn next(&mut self) -> Result<Option<Self::At<'_>>>;
}

/// One posting of a [`Lead`]: its document and its encoded positions there.
trait At {
    fn doc(&self) -> &[u8];
    fn positions(&self) -> &[u8];
}

impl At for pigeonhole::CellEntry<'_> {
    #[inline(always)]
    fn doc(&self) -> &[u8] {
        self.qualifier
    }

    #[inline(always)]
    fn positions(&self) -> &[u8] {
        self.cell.value()
    }
}

impl At for (&[u8], &[u8]) {
    fn doc(&self) -> &[u8] {
        self.0
    }

    fn positions(&self) -> &[u8] {
        self.1
    }
}

impl<'t> Lead for pigeonhole::RowCells<'t> {
    type At<'a>
        = pigeonhole::CellEntry<'a>
    where
        Self: 'a;

    #[inline(always)]
    fn next(&mut self) -> Result<Option<Self::At<'_>>> {
        Ok(pigeonhole::RowCells::next(self)?)
    }
}

/// A version 2 index's lead: its position cells over its blocks.
struct MergedLead<'t> {
    cells: pigeonhole::RowCells<'t>,
    merged: Merged<'t>,
    /// The posting handed out last, passed at the next call.
    on: Option<Tier>,
}

impl<'t> MergedLead<'t> {
    fn new(
        t: &'t Rows,
        s: &Snapshot,
        row: &[u8],
        cells: pigeonhole::RowCells<'t>,
        prefix: Option<&[u8]>,
    ) -> Result<Self> {
        Ok(MergedLead {
            cells,
            merged: Merged::new(t, s, row, POS, prefix)?,
            on: None,
        })
    }
}

impl<'t> Lead for MergedLead<'t> {
    type At<'a>
        = (&'a [u8], &'a [u8])
    where
        Self: 'a;

    fn next(&mut self) -> Result<Option<Self::At<'_>>> {
        if let Some(tier) = self.on.take() {
            self.merged.skip(tier);
        }
        sync(
            self.merged
                .settle::<Blocking>(&mut self.cells, &mut (), None),
        )?;
        self.on = self.merged.head();
        Ok(self
            .on
            .map(|tier| (self.merged.id(tier), self.merged.positions(tier))))
    }
}

impl MergedLead<'_> {
    /// [`Lead::next`] without blocking.
    #[cfg(feature = "async")]
    async fn next_async(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        use crate::maxscore::{Nonblocking, Polling};
        if let Some(tier) = self.on.take() {
            self.merged.skip(tier);
        }
        let (merged, cells) = (&mut self.merged, &mut self.cells);
        std::future::poll_fn(|cx| merged.settle::<Nonblocking>(cells, &mut Polling::new(cx), None))
            .await?;
        self.on = self.merged.head();
        Ok(self
            .on
            .map(|tier| (self.merged.id(tier), self.merged.positions(tier))))
    }
}

/// [`score_phrase`]'s join, over the lead's stream.
#[allow(clippy::too_many_arguments)]
fn join<L: Layout>(
    t: &Rows,
    s: &Snapshot,
    bm: &Bm25,
    dfs: Dfs<'_>,
    phrase: &Phrase,
    prefix: Option<&[u8]>,
    plan: &PhrasePlan,
    mut lead: impl Lead,
) -> Result<BTreeMap<Vec<u8>, f64>> {
    let mut scores = BTreeMap::new();
    let lead_term = phrase.terms[plan.lead].0.as_str();
    let mut others = plan.others::<L>(dfs, phrase, prefix);
    // The lead's postings, for the length of each document the phrase is in.
    let mut lens = Joined::<L>::new(lead_term, POST, dfs.here(lead_term), None, prefix);
    let mut lists: Vec<Vec<u32>> = vec![Vec::new(); others.len() + 1];
    'docs: while let Some(e) = lead.next()? {
        let doc = e.doc();
        for (o, list) in others.iter_mut().zip(&mut lists[1..]) {
            // A document without every term can't hold the phrase.
            if !o.at(t, s, doc)? {
                continue 'docs;
            }
            decode_positions(&o.value, list)?;
        }
        decode_positions(e.positions(), &mut lists[0])?;
        let Some(freq) = plan.freq(phrase, &lists) else {
            continue;
        };
        if !lens.at(t, s, doc)? {
            return Err(corrupt("a position has no posting"));
        }
        scores.insert(doc.to_vec(), plan.score(bm, freq, lens.number)?);
    }
    Ok(scores)
}

/// [`score_phrase`] as a future (#1560): the same join, read without blocking.
#[cfg(feature = "async")]
#[allow(clippy::too_many_arguments)]
async fn score_phrase_async(
    t: &Rows,
    s: &Snapshot,
    bm: &Bm25,
    dfs: Dfs<'_>,
    phrase: &Phrase,
    prefix: Option<&[u8]>,
    packed: bool,
    budget: &mut crate::maxscore::Budget,
) -> Result<BTreeMap<Vec<u8>, f64>> {
    let Some(plan) = PhrasePlan::new(bm, dfs, phrase) else {
        return Ok(BTreeMap::new());
    };
    let row = post_row(&phrase.terms[plan.lead].0);
    let cells = cells_of(t, s, &row, POS, prefix).cells()?;
    if packed {
        let lead = AsyncLead::Merged(MergedLead::new(t, s, &row, cells, prefix)?);
        join_async::<Packed>(t, s, bm, dfs, phrase, prefix, &plan, lead, budget).await
    } else {
        let lead = AsyncLead::Cells(cells);
        join_async::<Cells>(t, s, bm, dfs, phrase, prefix, &plan, lead, budget).await
    }
}

/// The async join's lead: a [`Lead`] by a match. A trait's async method with a lent item
/// gives a future that isn't `Send` for every lifetime, which `run_async` promises.
// One a phrase, held in the search's future: boxing the larger variant would only add an
// allocation.
#[cfg(feature = "async")]
#[allow(clippy::large_enum_variant)]
enum AsyncLead<'t> {
    Cells(pigeonhole::RowCells<'t>),
    Merged(MergedLead<'t>),
}

/// One posting of an [`AsyncLead`].
#[cfg(feature = "async")]
enum AsyncAt<'a> {
    Cell(pigeonhole::CellEntry<'a>),
    Merged((&'a [u8], &'a [u8])),
}

#[cfg(feature = "async")]
impl AsyncLead<'_> {
    async fn next(&mut self) -> Result<Option<AsyncAt<'_>>> {
        Ok(match self {
            AsyncLead::Cells(cells) => cells.next_async().await?.map(AsyncAt::Cell),
            AsyncLead::Merged(lead) => lead.next_async().await?.map(AsyncAt::Merged),
        })
    }
}

#[cfg(feature = "async")]
impl AsyncAt<'_> {
    fn doc(&self) -> &[u8] {
        match self {
            AsyncAt::Cell(e) => e.doc(),
            AsyncAt::Merged(e) => e.doc(),
        }
    }

    fn positions(&self) -> &[u8] {
        match self {
            AsyncAt::Cell(e) => e.positions(),
            AsyncAt::Merged(e) => e.positions(),
        }
    }
}

/// [`join`] as a future.
#[cfg(feature = "async")]
#[allow(clippy::too_many_arguments)]
async fn join_async<'t, L: Layout>(
    t: &'t Rows,
    s: &Snapshot,
    bm: &Bm25,
    dfs: Dfs<'_>,
    phrase: &Phrase,
    prefix: Option<&[u8]>,
    plan: &PhrasePlan,
    mut lead: AsyncLead<'t>,
    budget: &mut crate::maxscore::Budget,
) -> Result<BTreeMap<Vec<u8>, f64>> {
    let mut scores = BTreeMap::new();
    let lead_term = phrase.terms[plan.lead].0.as_str();
    let mut others = plan.others::<L>(dfs, phrase, prefix);
    let mut lens = Joined::<L>::new(lead_term, POST, dfs.here(lead_term), None, prefix);
    let mut lists: Vec<Vec<u32>> = vec![Vec::new(); others.len() + 1];
    'docs: loop {
        budget.spend(1).await;
        let Some(e) = lead.next().await? else {
            break;
        };
        let doc = e.doc();
        for (o, list) in others.iter_mut().zip(&mut lists[1..]) {
            if !o.at_async(t, s, doc, budget).await? {
                continue 'docs;
            }
            decode_positions(&o.value, list)?;
        }
        decode_positions(e.positions(), &mut lists[0])?;
        let Some(freq) = plan.freq(phrase, &lists) else {
            continue;
        };
        // Its reads spend the budget themselves: a cached get resolves at once, and
        // without one a phrase over a hub term's documents would never yield (ai1249's R1).
        if !lens.at_async(t, s, doc, budget).await? {
            return Err(corrupt("a position has no posting"));
        }
        scores.insert(doc.to_vec(), plan.score(bm, freq, lens.number)?);
    }
    Ok(scores)
}

/// The read of a posting row's `family`, restricted to ids starting with `prefix`.
fn cells_of<'t>(
    t: &'t Rows,
    s: &Snapshot,
    row: &[u8],
    family: &'static str,
    prefix: Option<&[u8]>,
) -> pigeonhole::RowRead<'t> {
    let read = t.row(row).family(family).snapshot(s);
    match prefix {
        Some(p) => read.qualifier_prefix(p),
        None => read,
    }
}

/// One of a phrase's posting-row families in the join (#1586): a term's cells in the
/// documents the lead's walk asks for, which come in ascending order. The other terms'
/// positions are read this way, and so are the lead's own postings, for the length of a
/// document the phrase is in.
///
/// Reached by point reads or by the row's own walk, whichever costs less, at a point read's
/// price of [`LOOKUP_IN_STEPS`](crate::maxscore::LOOKUP_IN_STEPS) steps (perf287's measure,
/// #1368). So a rare lead beside a common word reads the common word's row at the lead's
/// documents only, and two common words are merged a step at a time.
/// - **Another term's positions:** the lead's `df` says how many documents will be asked
///   for, so the choice is made up front.
/// - **The lead's postings:** asked for only where the phrase occurs, which isn't known, so
///   by MaxScore's rule for a non-essential clause (`Walk::lookup`, ski-rental): point reads
///   until they'd cost more than stepping through what's left of the row, then the walk for
///   good.
struct Joined<'t, L: Layout> {
    row: Vec<u8>,
    family: &'static str,
    prefix: Option<Vec<u8>>,
    /// The row's walk, opened when the join switches to it.
    cells: Option<pigeonhole::RowCells<'t>>,
    /// The document the walk stands on, once it has stepped. A buffer, reused.
    doc: Vec<u8>,
    /// The cell of the document last found ([`at`](Self::at) returned `true`): its value
    /// (a buffer, reused), and the value as a number, if it is one.
    value: Vec<u8>,
    number: Option<u64>,
    /// Whether the walk has run off the row's end.
    ended: bool,
    /// The row's postings (the term's `df`), the steps the walk has taken, and the point
    /// reads done.
    postings: u64,
    stepped: u64,
    lookups: u64,
    advancing: bool,
    /// In a version 2 index: the row's blocks under its cells, opened at the first document
    /// asked for.
    merged: Option<Box<Merged<'t>>>,
    /// The layout, as a type ([`Layout`]): a version 1 join is the code it was.
    layout: std::marker::PhantomData<L>,
}

impl<'t, L: Layout> Joined<'t, L> {
    /// `asked` is how many documents the join will ask for, where that's known: the
    /// lead's `df`, for another term's positions. Then the choice between point reads and
    /// the walk is made here, once, and the walk isn't paid for after point reads already
    /// were (ski-rental's price for not knowing). `None` for the lead's own postings, asked
    /// for only where the phrase occurs.
    ///
    /// Under an `id_prefix` the lead's `df` counts documents outside the prefix too, so
    /// `asked` over-estimates and the walk can be chosen where point reads would have cost
    /// less: slower then, never a different result (ai1249's S1 on #1997).
    fn new(
        term: &str,
        family: &'static str,
        postings: i64,
        asked: Option<i64>,
        prefix: Option<&[u8]>,
    ) -> Self {
        let postings = postings.max(0) as u64;
        let advancing = asked.is_some_and(|n| {
            (n.max(0) as u64).saturating_mul(crate::maxscore::LOOKUP_IN_STEPS) > postings
        });
        Joined {
            row: post_row(term),
            family,
            prefix: prefix.map(<[u8]>::to_vec),
            cells: None,
            doc: Vec::new(),
            value: Vec::new(),
            number: None,
            ended: false,
            postings,
            stepped: 0,
            lookups: 0,
            advancing,
            merged: None,
            layout: std::marker::PhantomData,
        }
    }

    /// Whether a cell of this join's family is a tombstone (a version 2 index's deleted
    /// posting): no posting, and it hides the block's.
    fn tombstone(&self, value: &[u8], number: Option<u64>) -> bool {
        if self.family == POST {
            number == Some(TOMBSTONE)
        } else {
            value.is_empty()
        }
    }

    /// Opens the row's blocks, once (a version 2 index).
    fn open_merged(&mut self, t: &'t Rows, s: &Snapshot) -> Result<()> {
        if self.merged.is_none() {
            self.merged = Some(Box::new(Merged::new(
                t,
                s,
                &self.row,
                self.family,
                self.prefix.as_deref(),
            )?));
        }
        Ok(())
    }

    /// Keeps the block tier's posting for `doc`, if it stands on it.
    fn keep_block(&mut self, doc: &[u8]) -> bool {
        let m = self.merged.as_ref().expect("opened");
        if !m.block_has(doc) {
            return false;
        }
        self.value.clear();
        if self.family == POST {
            self.number = m.post(Tier::Block).ok();
        } else {
            self.value.extend_from_slice(m.positions(Tier::Block));
            self.number = None;
        }
        true
    }

    /// [`at`](Self::at) in a version 2 index. The block tier always advances to `doc` (a
    /// block passed costs a key compare); the cell tier is reached by point reads or by its
    /// walk, by the rule it had. The cell wins, and a tombstone means no posting.
    ///
    /// So in point-read mode the block tier still steps: through a hub term's blocks, one
    /// key compare each and no decode. That is the cheap side, and not a thing to turn
    /// into point reads (ai1249 on #2009).
    fn at_merged(&mut self, t: &'t Rows, s: &Snapshot, doc: &[u8]) -> Result<bool> {
        self.open_merged(t, s)?;
        let m = self.merged.as_mut().expect("opened above");
        sync(m.blocks_to::<Blocking>(&mut (), doc))?;
        if !self.walks() {
            if let Some(c) = t.get_at(s, &self.row, self.family, doc)? {
                if self.tombstone(c.value(), c.as_u64()) {
                    return Ok(false);
                }
                Self::keep(&mut self.value, &mut self.number, (c.value(), c.as_u64()));
                return Ok(true);
            }
            return Ok(self.keep_block(doc));
        }
        self.open(t, s)?;
        let cells = self.cells.as_mut().expect("opened above");
        let m = self.merged.as_mut().expect("opened above");
        sync(m.cells_to::<Blocking>(cells, &mut (), doc))?;
        self.stepped = m.stepped;
        if m.cell_side(doc) == Side::Here {
            return Ok(match self.family == POST {
                true => match m.cell_post()? {
                    Some(v) => {
                        self.number = Some(v);
                        true
                    }
                    None => false,
                },
                false => match m.cell_positions() {
                    Some(ps) => {
                        self.value.clear();
                        self.value.extend_from_slice(ps);
                        true
                    }
                    None => false,
                },
            });
        }
        Ok(self.keep_block(doc))
    }

    /// [`at_merged`](Self::at_merged) without blocking.
    #[cfg(feature = "async")]
    async fn at_merged_async(
        &mut self,
        t: &'t Rows,
        s: &Snapshot,
        doc: &[u8],
        budget: &mut crate::maxscore::Budget,
    ) -> Result<bool> {
        use crate::maxscore::{Nonblocking, Polling};
        self.open_merged(t, s)?;
        let m = self.merged.as_mut().expect("opened above");
        std::future::poll_fn(|cx| m.blocks_to::<Nonblocking>(&mut Polling::new(cx), doc)).await?;
        if !self.walks() {
            budget.spend(crate::maxscore::LOOKUP_STEPS).await;
            if let Some(c) = t.get_at_async(s, &self.row, self.family, doc).await? {
                if self.tombstone(c.value(), c.as_u64()) {
                    return Ok(false);
                }
                Self::keep(&mut self.value, &mut self.number, (c.value(), c.as_u64()));
                return Ok(true);
            }
            return Ok(self.keep_block(doc));
        }
        budget.spend(1).await;
        self.open(t, s)?;
        let cells = self.cells.as_mut().expect("opened above");
        let m = self.merged.as_mut().expect("opened above");
        std::future::poll_fn(|cx| m.cells_to::<Nonblocking>(cells, &mut Polling::new(cx), doc))
            .await?;
        self.stepped = m.stepped;
        if m.cell_side(doc) == Side::Here {
            return Ok(match self.family == POST {
                true => match m.cell_post()? {
                    Some(v) => {
                        self.number = Some(v);
                        true
                    }
                    None => false,
                },
                false => match m.cell_positions() {
                    Some(ps) => {
                        self.value.clear();
                        self.value.extend_from_slice(ps);
                        true
                    }
                    None => false,
                },
            });
        }
        Ok(self.keep_block(doc))
    }

    /// Whether the next document is reached by the row's walk: decided once per document,
    /// and for good once it says yes.
    fn walks(&mut self) -> bool {
        if !self.advancing {
            self.lookups += 1;
            let left = self.postings.saturating_sub(self.stepped);
            self.advancing = self
                .lookups
                .saturating_mul(crate::maxscore::LOOKUP_IN_STEPS)
                > left;
        }
        self.advancing
    }

    /// Whether the walk has to move to reach `doc`: not once it has ended, or stands at or
    /// past `doc`.
    fn behind(&self, doc: &[u8]) -> bool {
        !self.ended && (self.cells.is_none() || self.doc.as_slice() < doc)
    }

    /// Whether the walk, standing at or past `doc`, stands on it.
    fn here(&self, doc: &[u8]) -> bool {
        !self.ended && self.doc.as_slice() == doc
    }

    /// Keeps the cell found: its value, and the value as a number.
    fn keep(value: &mut Vec<u8>, number: &mut Option<u64>, found: (&[u8], Option<u64>)) {
        value.clear();
        value.extend_from_slice(found.0);
        *number = found.1;
    }

    /// Opens the row's walk, once.
    fn open(&mut self, t: &'t Rows, s: &Snapshot) -> Result<()> {
        if self.cells.is_none() {
            let cells = cells_of(t, s, &self.row, self.family, self.prefix.as_deref()).cells()?;
            self.cells = Some(cells);
        }
        Ok(())
    }

    /// Whether the term has a cell for `doc`, kept in `value` and `number` if so. `doc`s
    /// ascend.
    fn at(&mut self, t: &'t Rows, s: &Snapshot, doc: &[u8]) -> Result<bool> {
        if L::MERGED {
            return self.at_merged(t, s, doc);
        }
        if !self.walks() {
            return Ok(match t.get_at(s, &self.row, self.family, doc)? {
                Some(c) => {
                    Self::keep(&mut self.value, &mut self.number, (c.value(), c.as_u64()));
                    true
                }
                None => false,
            });
        }
        if self.behind(doc) {
            // A posting passed costs a key compare: only the one stood on is copied.
            self.open(t, s)?;
            let cells = self.cells.as_mut().expect("opened above");
            loop {
                self.stepped += 1;
                let Some(e) = cells.next()? else {
                    self.ended = true;
                    break;
                };
                if e.qualifier < doc {
                    continue;
                }
                self.doc.clear();
                self.doc.extend_from_slice(e.qualifier);
                Self::keep(
                    &mut self.value,
                    &mut self.number,
                    (e.cell.value(), e.cell.as_u64()),
                );
                break;
            }
        }
        Ok(self.here(doc))
    }

    /// [`at`](Self::at) without blocking: point reads as futures, and the walk by seeking
    /// (a cold row then reads only the blocks it lands in, perf287's T1 on #1560). A seek
    /// counts one step, so the switch to walking comes a little later than the sync join's.
    #[cfg(feature = "async")]
    async fn at_async(
        &mut self,
        t: &'t Rows,
        s: &Snapshot,
        doc: &[u8],
        budget: &mut crate::maxscore::Budget,
    ) -> Result<bool> {
        if L::MERGED {
            return self.at_merged_async(t, s, doc, budget).await;
        }
        if !self.walks() {
            budget.spend(crate::maxscore::LOOKUP_STEPS).await;
            return Ok(
                match t.get_at_async(s, &self.row, self.family, doc).await? {
                    Some(c) => {
                        Self::keep(&mut self.value, &mut self.number, (c.value(), c.as_u64()));
                        true
                    }
                    None => false,
                },
            );
        }
        if self.behind(doc) {
            budget.spend(1).await;
            self.open(t, s)?;
            let cells = self.cells.as_mut().expect("opened above");
            let (family, at) = (self.family, &mut self.doc);
            let (value, number) = (&mut self.value, &mut self.number);
            // Only when behind `doc`, so the seek never stands on `doc` already (a seek
            // from a posting lands after it).
            let found = std::future::poll_fn(|cx| match cells.poll_seek(cx, family, doc) {
                std::task::Poll::Ready(Ok(Some(e))) => {
                    at.clear();
                    at.extend_from_slice(e.qualifier);
                    Self::keep(value, number, (e.cell.value(), e.cell.as_u64()));
                    std::task::Poll::Ready(Ok(true))
                }
                std::task::Poll::Ready(Ok(None)) => std::task::Poll::Ready(Ok(false)),
                std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(e)),
                std::task::Poll::Pending => std::task::Poll::Pending,
            })
            .await?;
            self.stepped += 1;
            self.ended = !found;
        }
        Ok(self.here(doc))
    }
}

/// What a phrase scores with, past its reads: its idf (the sum of its terms'), its lead term
/// (the rarest: the others narrow its documents), and each term's group (a repeated word's
/// terms share one, so the sloppy matcher keeps them apart).
struct PhrasePlan {
    idf: f64,
    lead: usize,
    groups: Vec<usize>,
    /// Each term's place in the join's position lists: 0 for the lead term, then the other
    /// distinct terms in the phrase's order. A repeated word's terms share one.
    slots: Vec<usize>,
}

impl PhrasePlan {
    /// `None` when a term has no postings here: no document here can hold the phrase. The
    /// idf is from the `df`s it scores with, and the lead the term with the fewest postings
    /// here.
    fn new(bm: &Bm25, dfs: Dfs<'_>, phrase: &Phrase) -> Option<Self> {
        let terms = &phrase.terms;
        if terms.iter().any(|(term, _)| dfs.here(term) <= 0) {
            return None;
        }
        let idf: f64 = terms.iter().map(|(term, _)| bm.idf(dfs.score(term))).sum();
        let lead = (0..terms.len())
            .min_by_key(|&i| dfs.here(&terms[i].0))
            .expect("a phrase has terms");
        let groups = terms
            .iter()
            .map(|(term, _)| terms.iter().position(|(t, _)| t == term).expect("itself"))
            .collect();
        let mut distinct = vec![terms[lead].0.as_str()];
        let slots = terms
            .iter()
            .map(|(term, _)| {
                distinct.iter().position(|t| t == term).unwrap_or_else(|| {
                    distinct.push(term);
                    distinct.len() - 1
                })
            })
            .collect();
        Some(PhrasePlan {
            idf,
            lead,
            groups,
            slots,
        })
    }

    /// The join's other terms: the distinct terms besides the lead, in `slots`' order.
    fn others<'t, L: Layout>(
        &self,
        dfs: Dfs<'_>,
        phrase: &Phrase,
        prefix: Option<&[u8]>,
    ) -> Vec<Joined<'t, L>> {
        let mut others: Vec<Joined<'t, L>> = Vec::new();
        for ((term, _), &slot) in phrase.terms.iter().zip(&self.slots) {
            if slot == others.len() + 1 {
                let asked = Some(dfs.here(&phrase.terms[self.lead].0));
                others.push(Joined::new(term, POS, dfs.here(term), asked, prefix));
            }
        }
        others
    }

    /// The phrase's frequency in a document every term is in, from the join's position
    /// lists there (`slots`); `None` when it doesn't occur.
    fn freq(&self, phrase: &Phrase, by_slot: &[Vec<u32>]) -> Option<f64> {
        let terms = &phrase.terms;
        let lead_ps = by_slot[0].as_slice();
        let lists = || self.slots.iter().map(|&slot| by_slot[slot].as_slice());
        let lead_off = terms[self.lead].1;
        let freq = if phrase.slop == 0 {
            let mut n = 0u32;
            'start: for &p in lead_ps {
                for ((_, off), ps) in terms.iter().zip(lists()) {
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
            let walks: Vec<(&[u32], u32, usize)> = lists()
                .zip(terms)
                .zip(&self.groups)
                .map(|((ps, (_, off)), g)| (ps, *off, *g))
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
