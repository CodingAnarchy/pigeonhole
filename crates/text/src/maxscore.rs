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
//! **Solo expansions** (#1363). An essential fuzzy clause walks only the expansions that
//! could carry a document into the top `k` with nothing but the non-essential clauses beside
//! them: bound plus the non-essential bounds reaching the `k`-th score. The others can't
//! bring a document in alone, so their postings aren't walked. Where another essential clause
//! has made a document a candidate, they're looked up, if it can still make it.
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
/// 100,000 documents on a laptop, on one hub term's posting row (67,972 postings), a
/// `get_at` of a posting takes about 870 ns, and a step about 60 ns: 14.4 steps. Since #1378
/// an advancing walk's step past a posting is a key compare, about 52 ns (16.6 steps), but
/// 16 measured no faster than 14 on the fuzzy kinds, so the constant stays. That's with
/// the index in the block cache, the conservative end: a cold read costs more steps, so a
/// real index switches to advancing a little late, never early. A modestly wrong value
/// costs a constant factor (ski-rental's bound assumes the price is right; one off by a
/// factor `f` costs up to about `f` times more, perf287 on #1377).
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
        self.skip_while(bm, |_| false)
    }

    /// Steps to the first posting after the current one whose document `skip` doesn't
    /// accept, and stands there. A skipped posting costs a key compare: only the posting
    /// stood on is copied and decoded (#1378). So a corrupt value in a skipped posting
    /// doesn't fail the search, as with the postings a point lookup never reads.
    /// [`Index::verify`](crate::Index::verify) decodes every posting (perf287 on #1398).
    fn skip_while(&mut self, bm: &Bm25, skip: impl Fn(&[u8]) -> bool) -> Result<()> {
        loop {
            self.stepped += 1;
            let Some(e) = self.cells.next()? else {
                self.cur = None;
                return Ok(());
            };
            if skip(e.qualifier) {
                continue;
            }
            let (tf, len) = unposting(
                e.cell
                    .as_u64()
                    .ok_or_else(|| corrupt("a posting isn't a u64"))?,
            );
            self.cur = Some((
                e.qualifier.to_vec(),
                self.weight * bm.tf_factor(f64::from(tf), f64::from(len)),
            ));
            return Ok(());
        }
    }

    /// Steps past `doc`: a walk that starts being walked again (#1363) stands after the
    /// documents already decided.
    fn pass(&mut self, bm: &Bm25, doc: &[u8]) -> Result<()> {
        if self.doc().is_some_and(|d| d <= doc) {
            self.skip_while(bm, |d| d <= doc)?;
        }
        Ok(())
    }

    /// The walk's contribution to `doc`, for a non-essential clause or an essential fuzzy
    /// clause's unwalked expansion (#1363); `doc`s come in
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
            if self.doc().is_some_and(|d| d < doc) {
                self.skip_while(bm, |d| d < doc)?;
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
        /// The expansions still in play, largest bound first. One whose bound falls below
        /// what a document would need from it is dropped ([`Scored::drop_below`], #1333).
        walks: Vec<Walk<'t>>,
        /// How many of `walks` are walked (#1363): the rest can't bring a document in on
        /// their own, and are only looked up ([`Scored::set_solo`]).
        solo: usize,
        /// The document the clause stands on (the walked ones' smallest) and its
        /// contribution there.
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
        walks.sort_by(|a, b| b.bound.total_cmp(&a.bound));
        let solo = walks.len();
        let mut c = Scored {
            clause: Clause::Fuzzy {
                walks,
                solo,
                cur: None,
            },
            bound: bound * (1.0 + SLACK),
        };
        c.settle();
        Ok(c)
    }

    /// A fuzzy clause's document: its walked expansions' smallest, with the largest
    /// contribution there.
    fn settle(&mut self) {
        if let Clause::Fuzzy { walks, solo, cur } = &mut self.clause {
            let walks = &walks[..*solo];
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
            Clause::Fuzzy { walks, solo, cur } => {
                let Some((doc, _)) = cur.take() else {
                    return Ok(());
                };
                for w in walks[..*solo].iter_mut() {
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
    /// For an essential fuzzy clause, walks only the expansions whose bound reaches `need`:
    /// the `k`-th held score less the non-essential clauses' bounds, and less the other
    /// essential fuzzy clauses' unwalked bounds (#1363).
    ///
    /// **Exact.** A document that only unwalked expansions match, among the essential
    /// clauses, scores at most their largest bounds summed over the clauses plus the
    /// non-essential bounds, below the threshold, so it can't enter the top `k` and needn't
    /// be a candidate. A candidate gets their
    /// contribution by lookup ([`unwalked`](Self::unwalked)), so its score is the same.
    /// `need` can fall as well as rise (a clause turning non-essential adds its bound), so
    /// an expansion can be walked again. It then steps past `last`, the last document
    /// decided, which every candidate from now on follows.
    fn set_solo(&mut self, bm: &Bm25, need: f64, last: Option<&[u8]>) -> Result<()> {
        let Clause::Fuzzy { walks, solo, .. } = &mut self.clause else {
            return Ok(());
        };
        let n = walks
            .iter()
            .take_while(|w| w.bound * (1.0 + SLACK) >= need * (1.0 - SLACK))
            .count();
        if n == *solo {
            return Ok(());
        }
        if let Some(last) = last {
            for w in &mut walks[*solo..n.max(*solo)] {
                w.pass(bm, last)?;
            }
        }
        *solo = n;
        self.settle();
        Ok(())
    }

    /// The most an essential fuzzy clause's looked-up expansions add to a document: their
    /// largest bound, or 0 when every expansion is walked.
    fn unwalked(&self) -> f64 {
        match &self.clause {
            Clause::Fuzzy { walks, solo, .. } => {
                walks.get(*solo).map_or(0.0, |w| w.bound * (1.0 + SLACK))
            }
            _ => 0.0,
        }
    }

    /// An essential fuzzy clause's looked-up expansions' contribution to `doc` (#1363): the
    /// largest of theirs.
    fn lookup_unwalked(&mut self, t: &Rows, s: &Snapshot, bm: &Bm25, doc: &[u8]) -> Result<f64> {
        let Clause::Fuzzy { walks, solo, .. } = &mut self.clause else {
            return Ok(0.0);
        };
        let mut best = 0.0f64;
        for w in walks[*solo..].iter_mut() {
            best = best.max(w.lookup(t, s, bm, doc)?);
        }
        Ok(best)
    }

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
        // Largest bound first, so the dropped ones are the last.
        walks.retain(|w| w.bound * (1.0 + SLACK) >= cut);
        if walks.len() != before {
            if let Clause::Fuzzy { walks, solo, .. } = &mut self.clause {
                *solo = (*solo).min(walks.len());
            }
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
    // Each clause's contribution to the candidate, `None` for one that doesn't match it.
    let mut contrib: Vec<Option<f64>> = vec![None; n];
    // The last document decided: a walk that starts being walked again steps past it. It's
    // the candidate's own buffer, reused.
    let mut last: Vec<u8> = Vec::new();
    let mut decided = false;
    // Whether an essential fuzzy clause has unwalked expansions: only then does a candidate
    // pay for the gains below (single fuzzy clauses never do, since #1333's cut already drops
    // what they'd leave unwalked).
    let mut unwalked = false;
    'docs: loop {
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
            // Inside every essential fuzzy clause, the expansions walked (#1363). They share
            // one budget: a document only unwalked expansions match, across every essential
            // fuzzy clause, scores below their largest bounds summed plus the non-essential
            // bounds, and that sum stays below the threshold.
            let mut budget = th - prefix[skip];
            unwalked = false;
            for &c in &by_bound[skip..] {
                clauses[c].set_solo(bm, budget, decided.then_some(last.as_slice()))?;
                let up = clauses[c].unwalked();
                budget -= up;
                unwalked |= up > 0.0;
            }
        }
        let essential = &by_bound[skip..];
        let Some(next) = essential.iter().filter_map(|&c| clauses[c].doc()).min() else {
            break;
        };
        // The candidate, in `last`'s buffer: once it's decided, it's the last document
        // decided, which the next pass's re-walk steps past. No allocation per candidate; a
        // document that enters the top `k` is copied.
        last.clear();
        last.extend_from_slice(next);
        decided = true;
        let doc: &[u8] = &last;
        contrib.fill(None);
        let mut partial = 0.0;
        for &c in essential {
            if clauses[c].doc() == Some(doc) {
                let v = clauses[c].here();
                contrib[c] = Some(v);
                partial += v;
                clauses[c].step(bm)?;
            }
        }
        // The essential fuzzy clauses' unwalked expansions (#1363): each can raise its
        // clause's contribution to at most their largest bound.
        let mut gains: f64 = if unwalked {
            essential
                .iter()
                .map(|&c| (clauses[c].unwalked() - contrib[c].unwrap_or(0.0)).max(0.0))
                .sum()
        } else {
            0.0
        };
        for &c in essential.iter().filter(|_| unwalked) {
            let up = clauses[c].unwalked();
            let here = contrib[c].unwrap_or(0.0);
            if up <= here {
                continue;
            }
            let th = threshold.expect("expansions are unwalked only with k held");
            if partial + gains + prefix[skip] < th * (1.0 - SLACK) {
                continue 'docs;
            }
            gains -= up - here;
            let v = clauses[c].lookup_unwalked(t, s, bm, doc)?;
            if v > here {
                contrib[c] = Some(v);
                partial += v - here;
            }
        }
        // The non-essential clauses, largest bound first, while the document can still make it.
        for i in (0..skip).rev() {
            let th = threshold.expect("clauses are non-essential only with k held");
            if partial + prefix[i + 1] < th * (1.0 - SLACK) {
                continue 'docs;
            }
            let c = by_bound[i];
            let v = clauses[c].lookup(t, s, bm, doc)?;
            if v != 0.0 {
                contrib[c] = Some(v);
                partial += v;
            }
        }
        // The exact score: contributions in the clauses' fixed order.
        let score = contrib.iter().flatten().fold(0.0, |sum, v| sum + v);
        if held.len() < k {
            held.push(Held(score, doc.to_vec()));
        } else {
            // Better than the worst held (`Held`'s order: a higher score, or an equal score
            // and a smaller id): it takes the worst one's place.
            let worst = held.peek().expect("k > 0 held");
            let better = match score.total_cmp(&worst.0) {
                Ordering::Greater => true,
                Ordering::Equal => doc < worst.1.as_slice(),
                Ordering::Less => false,
            };
            if better {
                held.pop();
                held.push(Held(score, doc.to_vec()));
            }
        }
    }
    // Best first: score descending, then id ascending (`Held` orders the worst greatest).
    let mut out = held.into_vec();
    out.sort();
    Ok(out.into_iter().map(|Held(s, d)| (d, s)).collect())
}
