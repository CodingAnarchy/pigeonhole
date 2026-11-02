//! MaxScore (Turtle and Flood), doc at a time (D309 §4.2): the exact top `k`, reading only
//! the postings that can still change it.
//!
//! Every clause has an upper bound on what it can add to one document's score:
//! - a term clause: `idf · tf_factor(maxtf, minlen)`, from its posting row's `maxtf` and
//!   `minlen` aggregates. Those only ever over-estimate (M1), so the bound is safe;
//! - a fuzzy clause: the largest of its expansions' term bounds, each times its boost.
//!   Inside it, once `k` are held, an expansion that can't lift a document to the threshold
//!   (even with every other clause at its bound) is dropped, from the walk or from the
//!   lookups (#1333);
//! - a phrase clause, scored up front: its largest actual score.
//!
//! Clauses are taken in ascending order of bound. While fewer than `k` documents are held,
//! every clause is essential. After that, the longest prefix of clauses whose bounds sum
//! strictly below the `k`-th score is non-essential: a document matching only those can't
//! enter the top `k`. Candidates come from the essential clauses' postings, in document
//! order. A candidate's non-essential contributions are looked up only while its partial
//! score plus what's left could still reach the `k`-th score.
//!
//! **Exactness.** A document's final score is its contributions summed in the clauses'
//! fixed order (terms in order, then phrases), which is the sum an unpruned search computes.
//! So the result is the exact top `k`, with exactly the same scores. Pruning tests a strict
//! bound, inflated by a relative 1e-9 so a bound rounded the other way never prunes a
//! document that belongs.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};

use pigeonhole::{RowCells, Snapshot};

use crate::index::Rows;

use crate::error::{Result, corrupt};
use crate::layout::*;

/// A point read's cost in posting steps (perf287's ski-rental rule, #1368): measured at
/// 100,000 documents on a laptop, on one hub term's posting row, a `get_at` of a posting
/// takes about 860 ns and a step through the row about 60 ns. That's with the index in the
/// block cache, the conservative end: a cold read costs more steps, so a real index switches
/// to advancing a little late, never early. A wrong value costs a constant factor at most.
const LOOKUP_IN_STEPS: u64 = 14;

/// A relative slack on every bound (above), so float rounding never prunes a document that
/// belongs.
const SLACK: f64 = 1e-9;

/// BM25 with Lucene's idf (D309 §4).
pub(crate) struct Bm25 {
    pub(crate) n: f64,
    pub(crate) avglen: f64,
    pub(crate) k1: f64,
    pub(crate) b: f64,
}

impl Bm25 {
    pub(crate) fn idf(&self, df: i64) -> f64 {
        let df = df as f64;
        (1.0 + (self.n - df + 0.5) / (df + 0.5)).ln()
    }

    pub(crate) fn tf_factor(&self, tf: f64, len: f64) -> f64 {
        tf * (self.k1 + 1.0) / (tf + self.k1 * (1.0 - self.b + self.b * len / self.avglen))
    }
}

/// A term's posting row, walked in document order. A posting contributes
/// `weight · tf_factor`: the weight is the term's idf, times its boost in a fuzzy clause.
pub(crate) struct Walk<'t> {
    row: Vec<u8>,
    weight: f64,
    /// `weight · tf_factor(maxtf, minlen)`: the most it adds to a document.
    bound: f64,
    /// Boxed: a walk is far larger than a phrase clause.
    cells: Box<RowCells<'t>>,
    /// The posting the walk stands on: its document and contribution.
    cur: Option<(Vec<u8>, f64)>,
    /// The postings the row holds (its `df`), and how many the walk has stepped past. With
    /// `id_prefix` the walk reads only part of the row, so `df` over-estimates what's left:
    /// it switches to advancing a little later than it could, which costs a little and never
    /// changes a result.
    postings: u64,
    stepped: u64,
    /// Lookups done by point reads, and whether the walk has switched to advancing.
    lookups: u64,
    advancing: bool,
}

impl<'t> Walk<'t> {
    /// The walk over `term`'s postings (restricted to doc ids starting with `prefix`), and
    /// its bound: `weight · tf_factor(maxtf, minlen)`. `postings` is the term's `df`.
    fn new(
        t: &'t Rows,
        s: &Snapshot,
        bm: &Bm25,
        term: &str,
        weight: f64,
        postings: u64,
        prefix: Option<&[u8]>,
    ) -> Result<(Self, f64)> {
        let row = post_row(term);
        let agg = |family: &str| -> Result<Option<i64>> {
            Ok(t.get_at(s, &row, family, BOUND)?.and_then(|c| c.as_i64()))
        };
        // MaxScore's bound (M1): the largest frequency at the shortest length any posting had.
        // Without the aggregates (they're written with every posting, so never in practice),
        // `k1 + 1` bounds the tf factor for any frequency and length.
        let factor = match (agg(MAXTF)?, agg(MINLEN)?) {
            (Some(maxtf), Some(minlen)) => bm.tf_factor(maxtf.max(0) as f64, minlen.max(0) as f64),
            _ => bm.k1 + 1.0,
        };
        let mut read = t.row(&row).family(POST).snapshot(s);
        if let Some(p) = prefix {
            read = read.qualifier_prefix(p);
        }
        let mut w = Walk {
            row,
            weight,
            bound: weight * factor,
            cells: Box::new(read.cells()?),
            cur: None,
            postings,
            stepped: 0,
            lookups: 0,
            advancing: false,
        };
        w.step(bm)?;
        let b = w.bound;
        Ok((w, b))
    }

    fn doc(&self) -> Option<&[u8]> {
        self.cur.as_ref().map(|(d, _)| d.as_slice())
    }

    fn step(&mut self, bm: &Bm25) -> Result<()> {
        self.stepped += 1;
        self.cur = match self.cells.next()? {
            Some(e) => {
                let (tf, len) = unposting(
                    e.cell
                        .as_u64()
                        .ok_or_else(|| corrupt("a posting isn't a u64"))?,
                );
                Some((
                    e.qualifier.to_vec(),
                    self.weight * bm.tf_factor(f64::from(tf), f64::from(len)),
                ))
            }
            None => None,
        };
        Ok(())
    }

    /// The walk's contribution to `doc`, for a non-essential clause; `doc`s come in
    /// ascending order. Point reads first, then, once their cost passes stepping through
    /// the rest of the row (lookups × [`LOOKUP_IN_STEPS`] > the postings left), advancing
    /// to `doc` for good: the ski-rental rule (perf287 on #1355), never much more than twice
    /// the better of the two, without knowing up front how many lookups will come.
    fn lookup(&mut self, t: &Rows, s: &Snapshot, bm: &Bm25, doc: &[u8]) -> Result<f64> {
        if !self.advancing {
            self.lookups += 1;
            let left = self.postings.saturating_sub(self.stepped);
            self.advancing = self.lookups.saturating_mul(LOOKUP_IN_STEPS) > left;
        }
        if self.advancing {
            while self.doc().is_some_and(|d| d < doc) {
                self.step(bm)?;
            }
            return Ok(match &self.cur {
                Some((d, c)) if d.as_slice() == doc => *c,
                _ => 0.0,
            });
        }
        match t.get_at(s, &self.row, POST, doc)? {
            Some(c) => {
                let (tf, len) =
                    unposting(c.as_u64().ok_or_else(|| corrupt("a posting isn't a u64"))?);
                Ok(self.weight * bm.tf_factor(f64::from(tf), f64::from(len)))
            }
            None => Ok(0.0),
        }
    }
}

/// A clause as MaxScore runs it.
pub(crate) enum Clause<'t> {
    /// A term's posting row.
    Term(Walk<'t>),
    /// A fuzzy term's expansions (D309 §4.4): a document's contribution is the largest of
    /// theirs, not their sum.
    Fuzzy {
        /// The expansions still walked. One whose bound falls below what a document would
        /// need from it is dropped ([`Scored::drop_below`], #1333).
        walks: Vec<Walk<'t>>,
        /// The document the clause stands on (the walks' smallest) and its contribution.
        cur: Option<(Vec<u8>, f64)>,
    },
    /// A phrase, scored up front: its documents and their scores, in document order.
    Phrase {
        scores: Vec<(Vec<u8>, f64)>,
        at: usize,
    },
}

/// One clause: what it is, and its bound.
pub(crate) struct Scored<'t> {
    pub(crate) clause: Clause<'t>,
    pub(crate) bound: f64,
}

impl<'t> Scored<'t> {
    /// A term clause over its posting row (restricted to doc ids starting with `prefix`).
    pub(crate) fn term(
        t: &'t Rows,
        s: &Snapshot,
        bm: &Bm25,
        term: &str,
        df: i64,
        prefix: Option<&[u8]>,
    ) -> Result<Self> {
        let (walk, bound) = Walk::new(t, s, bm, term, bm.idf(df), df.max(0) as u64, prefix)?;
        Ok(Scored {
            clause: Clause::Term(walk),
            bound: bound * (1.0 + SLACK),
        })
    }

    /// A fuzzy clause over its expansions: `(term, df, postings, boost)` each, `df` the one
    /// its idf scores with (the search passes the expansions' blended df) and `postings`
    /// the term's own.
    pub(crate) fn fuzzy(
        t: &'t Rows,
        s: &Snapshot,
        bm: &Bm25,
        expansions: &[(&str, i64, u64, f64)],
        prefix: Option<&[u8]>,
    ) -> Result<Self> {
        let mut walks = Vec::with_capacity(expansions.len());
        let mut bound = 0.0f64;
        for &(term, df, postings, boost) in expansions {
            let (walk, b) = Walk::new(t, s, bm, term, boost * bm.idf(df), postings, prefix)?;
            walks.push(walk);
            bound = bound.max(b);
        }
        let mut c = Scored {
            clause: Clause::Fuzzy { walks, cur: None },
            bound: bound * (1.0 + SLACK),
        };
        c.settle();
        Ok(c)
    }

    /// A fuzzy clause's document: its walks' smallest, with the largest contribution there.
    fn settle(&mut self) {
        if let Clause::Fuzzy { walks, cur, .. } = &mut self.clause {
            *cur = walks.iter().filter_map(|w| w.doc()).min().map(|doc| {
                let best = walks
                    .iter()
                    .filter_map(|w| w.cur.as_ref())
                    .filter(|(d, _)| d.as_slice() == doc)
                    .map(|(_, c)| *c)
                    .fold(0.0, f64::max);
                (doc.to_vec(), best)
            });
        }
    }

    /// A phrase clause from its scores.
    pub(crate) fn phrase(scores: BTreeMap<Vec<u8>, f64>) -> Self {
        let bound = scores.values().copied().fold(0.0, f64::max) * (1.0 + SLACK);
        Scored {
            clause: Clause::Phrase {
                scores: scores.into_iter().collect(),
                at: 0,
            },
            bound,
        }
    }

    /// The document the clause stands on, if any.
    fn doc(&self) -> Option<&[u8]> {
        match &self.clause {
            Clause::Term(w) => w.doc(),
            Clause::Fuzzy { cur, .. } => cur.as_ref().map(|(d, _)| d.as_slice()),
            Clause::Phrase { scores, at } => scores.get(*at).map(|(d, _)| d.as_slice()),
        }
    }

    /// The contribution where it stands.
    fn here(&self) -> f64 {
        match &self.clause {
            Clause::Term(Walk { cur, .. }) | Clause::Fuzzy { cur, .. } => {
                cur.as_ref().map_or(0.0, |(_, c)| *c)
            }
            Clause::Phrase { scores, at } => scores.get(*at).map_or(0.0, |(_, c)| *c),
        }
    }

    /// Moves to the next document.
    fn step(&mut self, bm: &Bm25) -> Result<()> {
        match &mut self.clause {
            Clause::Term(w) => w.step(bm)?,
            Clause::Fuzzy { walks, cur, .. } => {
                let Some((doc, _)) = cur.take() else {
                    return Ok(());
                };
                for w in walks.iter_mut() {
                    if w.doc() == Some(doc.as_slice()) {
                        w.step(bm)?;
                    }
                }
                self.settle();
            }
            Clause::Phrase { at, .. } => *at += 1,
        }
        Ok(())
    }

    /// The clause's contribution to `doc`, for a non-essential clause: `doc`s come in
    /// ascending order ([`Walk::lookup`]).
    fn lookup(&mut self, t: &Rows, s: &Snapshot, bm: &Bm25, doc: &[u8]) -> Result<f64> {
        match &mut self.clause {
            Clause::Term(w) => w.lookup(t, s, bm, doc),
            Clause::Fuzzy { walks, .. } => {
                let mut best = 0.0f64;
                for w in walks.iter_mut() {
                    best = best.max(w.lookup(t, s, bm, doc)?);
                }
                Ok(best)
            }
            Clause::Phrase { scores, .. } => Ok(scores
                .binary_search_by(|(d, _)| d.as_slice().cmp(doc))
                .map_or(0.0, |i| scores[i].1)),
        }
    }
}

impl Scored<'_> {
    /// Drops a fuzzy clause's expansions whose bound is below `cut`: the `k`-th held score
    /// less every other clause's bound (#1333). That's MaxScore for a clause that takes the
    /// largest of its expansions rather than their sum.
    ///
    /// **Exact.** A document whose clause value comes from a dropped expansion `e` scores
    /// at most (the other clauses' bounds) + `bound(e)`, below the threshold, so it can't
    /// enter the top `k`. So every document that does enter takes its maximum from a kept
    /// expansion, and its score is the same. The threshold only rises, so a drop is final.
    /// Dropped expansions aren't looked up either: by the same bound, they can't change
    /// the score of a document that enters.
    fn drop_below(&mut self, cut: f64) {
        let Clause::Fuzzy { walks, .. } = &mut self.clause else {
            return;
        };
        let before = walks.len();
        walks.retain(|w| w.bound * (1.0 + SLACK) >= cut);
        if walks.len() != before {
            self.settle();
        }
    }
}

/// A held result, ordered so the worst is greatest (a lower score, or an equal score with a
/// larger id): the top of a [`BinaryHeap`] is the one a better candidate replaces.
struct Held(f64, Vec<u8>);

impl PartialEq for Held {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Held {}

impl PartialOrd for Held {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Held {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .0
            .total_cmp(&self.0)
            .then_with(|| self.1.cmp(&other.1))
    }
}

/// The top `k` documents of `clauses` (in their fixed order), by score descending then id
/// ascending, with their exact scores.
pub(crate) fn top_k(
    t: &Rows,
    s: &Snapshot,
    bm: &Bm25,
    mut clauses: Vec<Scored<'_>>,
    k: usize,
) -> Result<Vec<(Vec<u8>, f64)>> {
    debug_assert!(k > 0, "the search returns before MaxScore at k = 0");
    let n = clauses.len();
    // Ascending bound; `prefix[i]` sums the first `i` bounds in that order.
    let mut by_bound: Vec<usize> = (0..n).collect();
    by_bound.sort_by(|&a, &b| clauses[a].bound.total_cmp(&clauses[b].bound));
    let mut prefix = vec![0.0; n + 1];
    for (i, &c) in by_bound.iter().enumerate() {
        prefix[i + 1] = prefix[i] + clauses[c].bound;
    }
    // The held documents, the worst on top.
    let mut held: BinaryHeap<Held> = BinaryHeap::with_capacity(k + 1);
    // How many of `by_bound`'s first clauses are non-essential.
    let mut skip = 0;
    // The threshold the non-essential prefix and the fuzzy cuts were last taken at.
    let mut cut_at = f64::NEG_INFINITY;
    let mut contrib = vec![0.0f64; n];
    let mut has = vec![false; n];
    loop {
        let threshold = (held.len() == k).then(|| held.peek().expect("k > 0 held").0);
        // Only when the threshold has risen since the last pass: nothing else moves the
        // non-essential prefix or the fuzzy cuts (perf287 and simp724 on #1353).
        if let Some(th) = threshold.filter(|&th| th > cut_at) {
            cut_at = th;
            while skip < n && prefix[skip + 1] < th {
                skip += 1;
            }
            // Inside every fuzzy clause, essential or not, the expansions that can't lift a
            // document to the threshold even with every other clause at its bound (#1333).
            // A non-essential one is then looked up over fewer expansions.
            let total = prefix[n];
            for c in clauses.iter_mut() {
                let cut = th - (total - c.bound);
                c.drop_below(cut);
            }
        }
        let essential = &by_bound[skip..];
        let Some(doc) = essential
            .iter()
            .filter_map(|&c| clauses[c].doc())
            .min()
            .map(<[u8]>::to_vec)
        else {
            break;
        };
        has.iter_mut().for_each(|h| *h = false);
        let mut partial = 0.0;
        for &c in essential {
            if clauses[c].doc() == Some(doc.as_slice()) {
                contrib[c] = clauses[c].here();
                has[c] = true;
                partial += contrib[c];
                clauses[c].step(bm)?;
            }
        }
        // The non-essential clauses, largest bound first, while the document can still make it.
        let mut pruned = false;
        for i in (0..skip).rev() {
            let th = threshold.expect("clauses are non-essential only with k held");
            if partial + prefix[i + 1] < th * (1.0 - SLACK) {
                pruned = true;
                break;
            }
            let c = by_bound[i];
            let v = clauses[c].lookup(t, s, bm, &doc)?;
            if v != 0.0 {
                contrib[c] = v;
                has[c] = true;
                partial += v;
            }
        }
        if pruned {
            continue;
        }
        // The exact score: contributions in the clauses' fixed order.
        let mut score = 0.0;
        for c in 0..n {
            if has[c] {
                score += contrib[c];
            }
        }
        let entry = Held(score, doc);
        if held.len() < k {
            held.push(entry);
        } else if entry < *held.peek().expect("k > 0 held") {
            // Better than the worst held: it takes the worst one's place.
            held.pop();
            held.push(entry);
        }
    }
    // Best first: score descending, then id ascending (`Held` orders the worst greatest).
    let mut out = held.into_vec();
    out.sort();
    Ok(out.into_iter().map(|Held(s, d)| (d, s)).collect())
}
