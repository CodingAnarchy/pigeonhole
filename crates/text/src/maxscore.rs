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
use std::task::Poll;

use pigeonhole::{CellEntry, RowCells, Snapshot};

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


// ---- Where the reads come from (#1560) ----

/// MaxScore's reads: one core, run by the sync search over [`Blocking`] and by the async one
/// over [`Nonblocking`] (#1560 §3.2). Each read point returns `Poll`; `Blocking`'s are
/// `#[inline(always)]` and always `Ready`, so monomorphized, the sync core is the loop it
/// was before (its `Pending` arms are dead code). A `Pending` changes no visible state
/// ([`RowCells::poll_next`]): the core is resumable at every read point.
pub(crate) trait Reads {
    /// What a read point polls with: nothing for [`Blocking`] (a zero-sized `()`, so the
    /// sync core passes no extra argument), the task's context and the poll's step budget
    /// for [`Nonblocking`].
    type Cx<'a, 'b>
    where
        'b: 'a;
    /// A walk's point lookup in flight: nothing for [`Blocking`].
    type Get: Default;
    /// Whether a read can return `Pending`: a constant, so its branches compile out of the
    /// sync core.
    const ASYNC: bool;

    /// The walk's next posting.
    fn next<'c>(
        cells: &'c mut RowCells<'_>,
        cx: &mut Self::Cx<'_, '_>,
    ) -> Poll<pigeonhole::Result<Option<CellEntry<'c>>>>;

    /// The walk's first posting at or after `doc` (never back): an async walk's skip, which
    /// reads only the block it lands in (perf287's T1). The sync walk steps instead.
    fn seek<'c>(
        cells: &'c mut RowCells<'_>,
        cx: &mut Self::Cx<'_, '_>,
        doc: &[u8],
    ) -> Poll<pigeonhole::Result<Option<CellEntry<'c>>>>;

    /// `doc`'s posting in `row` by a point read: `None` without one, else its value as a
    /// `u64` (`None` for a value that isn't one).
    fn get(
        t: &Rows,
        s: &Snapshot,
        row: &[u8],
        doc: &[u8],
        get: &mut Self::Get,
        cx: &mut Self::Cx<'_, '_>,
    ) -> Poll<pigeonhole::Result<Option<Option<u64>>>>;

    /// Whether a point lookup is in flight: the next lookup resumes it.
    fn get_pending(get: &Self::Get) -> bool;

    /// Counts `steps` of work against the poll's budget: `true` when the poll has spent it,
    /// and should yield (the task is woken first).
    fn spend(cx: &mut Self::Cx<'_, '_>, steps: u32) -> bool;
}

/// The sync search's reads: today's calls.
pub(crate) struct Blocking;

impl Reads for Blocking {
    type Cx<'a, 'b>
        = ()
    where
        'b: 'a;
    type Get = ();
    const ASYNC: bool = false;

    #[inline(always)]
    fn next<'c>(
        cells: &'c mut RowCells<'_>,
        (): &mut (),
    ) -> Poll<pigeonhole::Result<Option<CellEntry<'c>>>> {
        Poll::Ready(cells.next())
    }

    #[inline(always)]
    fn seek<'c>(
        _: &'c mut RowCells<'_>,
        (): &mut (),
        _: &[u8],
    ) -> Poll<pigeonhole::Result<Option<CellEntry<'c>>>> {
        unreachable!("a sync walk skips by stepping")
    }

    #[inline(always)]
    fn get(
        t: &Rows,
        s: &Snapshot,
        row: &[u8],
        doc: &[u8],
        (): &mut (),
        (): &mut (),
    ) -> Poll<pigeonhole::Result<Option<Option<u64>>>> {
        Poll::Ready(t.get_at(s, row, POST, doc).map(|c| c.map(|c| c.as_u64())))
    }

    #[inline(always)]
    fn get_pending((): &()) -> bool {
        false
    }

    #[inline(always)]
    fn spend((): &mut (), _: u32) -> bool {
        false
    }
}

/// Steps an async search does per poll before it yields (#1560): about a millisecond of
/// warm work, so a hub-term walk doesn't hold its executor thread. A lookup counts
/// [`LOOKUP_IN_STEPS`].
#[cfg(feature = "async")]
pub(crate) const TEXT_POLL_STEPS: u32 = 4096;

/// The async search's reads: [`RowCells::poll_next`] and `poll_seek`, and point reads as
/// futures held until they resolve.
#[cfg(feature = "async")]
pub(crate) struct Nonblocking;

/// A poll of the async search: the task's context, and the steps left before it yields.
#[cfg(feature = "async")]
pub(crate) struct Polling<'a, 'b> {
    pub(crate) cx: &'a mut std::task::Context<'b>,
    pub(crate) left: u32,
}

#[cfg(feature = "async")]
impl<'a, 'b> Polling<'a, 'b> {
    pub(crate) fn new(cx: &'a mut std::task::Context<'b>) -> Self {
        Polling {
            cx,
            left: TEXT_POLL_STEPS,
        }
    }
}

#[cfg(feature = "async")]
impl Reads for Nonblocking {
    type Cx<'a, 'b>
        = Polling<'a, 'b>
    where
        'b: 'a;
    type Get = Option<pigeonhole::nonblocking::GetFuture>;
    const ASYNC: bool = true;

    fn next<'c>(
        cells: &'c mut RowCells<'_>,
        cx: &mut Polling<'_, '_>,
    ) -> Poll<pigeonhole::Result<Option<CellEntry<'c>>>> {
        cells.poll_next(cx.cx)
    }

    fn seek<'c>(
        cells: &'c mut RowCells<'_>,
        cx: &mut Polling<'_, '_>,
        doc: &[u8],
    ) -> Poll<pigeonhole::Result<Option<CellEntry<'c>>>> {
        cells.poll_seek(cx.cx, POST, doc)
    }

    fn get(
        t: &Rows,
        s: &Snapshot,
        row: &[u8],
        doc: &[u8],
        get: &mut Self::Get,
        cx: &mut Polling<'_, '_>,
    ) -> Poll<pigeonhole::Result<Option<Option<u64>>>> {
        use std::future::Future;
        let fut = get.get_or_insert_with(|| t.get_at_async(s, row, POST, doc));
        match std::pin::Pin::new(fut).poll(cx.cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(r) => {
                *get = None;
                Poll::Ready(r.map(|c| c.map(|c| c.as_u64())))
            }
        }
    }

    fn get_pending(get: &Self::Get) -> bool {
        get.is_some()
    }

    fn spend(cx: &mut Polling<'_, '_>, steps: u32) -> bool {
        if cx.left > steps {
            cx.left -= steps;
            return false;
        }
        cx.cx.waker().wake_by_ref();
        true
    }
}

/// `Poll<Result<T>>` to `T`, returning a `Pending` or an error.
macro_rules! ready {
    ($e:expr) => {
        match $e {
            Poll::Ready(Ok(v)) => v,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e.into())),
            Poll::Pending => return Poll::Pending,
        }
    };
}

/// A term's posting row, walked in document order. A posting contributes
/// `weight · tf_factor`: the weight is the term's idf, times its boost in a fuzzy clause.
pub(crate) struct Walk<'t, R: Reads> {
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
    /// changes a result. An async walk's seek counts one step, so it switches later still.
    postings: u64,
    stepped: u64,
    /// Lookups done by point reads, and whether the walk has switched to advancing.
    lookups: u64,
    advancing: bool,
    /// A point lookup in flight (async only).
    get: R::Get,
}

/// MaxScore's bound factor for a term (M1): the largest frequency at the shortest length any
/// posting had, from the row's `maxtf` and `minlen` aggregates. Without them (they're written
/// with every posting, so never in practice), `k1 + 1` bounds the tf factor for any frequency
/// and length.
pub(crate) fn bound_factor(bm: &Bm25, maxtf: Option<i64>, minlen: Option<i64>) -> f64 {
    match (maxtf, minlen) {
        (Some(maxtf), Some(minlen)) => bm.tf_factor(maxtf.max(0) as f64, minlen.max(0) as f64),
        _ => bm.k1 + 1.0,
    }
}

impl<'t> Walk<'t, Blocking> {
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
        let factor = bound_factor(bm, agg(MAXTF)?, agg(MINLEN)?);
        let mut read = t.row(&row).family(POST).snapshot(s);
        if let Some(p) = prefix {
            read = read.qualifier_prefix(p);
        }
        let mut w = Walk::opened(row, weight, factor, read.cells()?, postings);
        match w.step(bm, &mut ()) {
            Poll::Ready(r) => r?,
            Poll::Pending => unreachable!("sync reads are always ready"),
        }
        let b = w.bound;
        Ok((w, b))
    }
}

impl<'t, R: Reads> Walk<'t, R> {
    /// A walk over `row`'s postings in `cells`, not yet on its first (call `step`).
    pub(crate) fn opened(
        row: Vec<u8>,
        weight: f64,
        factor: f64,
        cells: RowCells<'t>,
        postings: u64,
    ) -> Self {
        Walk {
            row,
            weight,
            bound: weight * factor,
            cells: Box::new(cells),
            cur: None,
            postings,
            stepped: 0,
            lookups: 0,
            advancing: false,
            get: R::Get::default(),
        }
    }

    /// The walk's bound.
    pub(crate) fn bound(&self) -> f64 {
        self.bound
    }

    fn doc(&self) -> Option<&[u8]> {
        self.cur.as_ref().map(|(d, _)| d.as_slice())
    }

    pub(crate) fn step(&mut self, bm: &Bm25, cx: &mut R::Cx<'_, '_>) -> Poll<Result<()>> {
        self.skip_while(bm, cx, |_| false)
    }

    /// Stands `cur` on `e`, the posting stepped to (`None`: the row is done). On the walk's
    /// fields, not `self`: `e` borrows its cells.
    fn hold(
        cur: &mut Option<(Vec<u8>, f64)>,
        weight: f64,
        bm: &Bm25,
        e: Option<CellEntry<'_>>,
    ) -> Result<()> {
        let Some(e) = e else {
            *cur = None;
            return Ok(());
        };
        let (tf, len) = unposting(
            e.cell
                .as_u64()
                .ok_or_else(|| corrupt("a posting isn't a u64"))?,
        );
        *cur = Some((
            e.qualifier.to_vec(),
            weight * bm.tf_factor(f64::from(tf), f64::from(len)),
        ));
        Ok(())
    }

    /// Steps to the first posting after the current one whose document `skip` doesn't
    /// accept, and stands there. A skipped posting costs a key compare: only the posting
    /// stood on is copied and decoded (#1378). So a corrupt value in a skipped posting
    /// doesn't fail the search, as with the postings a point lookup never reads.
    /// [`Index::verify`](crate::Index::verify) decodes every posting (perf287 on #1398).
    fn skip_while(
        &mut self,
        bm: &Bm25,
        cx: &mut R::Cx<'_, '_>,
        skip: impl Fn(&[u8]) -> bool,
    ) -> Poll<Result<()>> {
        loop {
            if R::spend(cx, 1) {
                return Poll::Pending;
            }
            let e = ready!(R::next(&mut self.cells, cx));
            self.stepped += 1;
            match e {
                Some(e) if skip(e.qualifier) => continue,
                e => return Poll::Ready(Self::hold(&mut self.cur, self.weight, bm, e)),
            }
        }
    }

    /// Stands on the first posting at or after `doc`, or after it with `past`: by stepping
    /// in a sync walk, by seeking in an async one (perf287's T1: a cold walk then reads only
    /// the blocks it lands in). A seek from a posting at `doc` lands after it (`poll_seek`
    /// never moves back), so a repeated seek, as on a re-poll, is the same step.
    fn advance(
        &mut self,
        bm: &Bm25,
        cx: &mut R::Cx<'_, '_>,
        doc: &[u8],
        past: bool,
    ) -> Poll<Result<()>> {
        if !R::ASYNC {
            return if past {
                self.skip_while(bm, cx, |d| d <= doc)
            } else {
                self.skip_while(bm, cx, |d| d < doc)
            };
        }
        loop {
            if R::spend(cx, 1) {
                return Poll::Pending;
            }
            let e = ready!(R::seek(&mut self.cells, cx, doc));
            self.stepped += 1;
            match e {
                Some(e) if past && e.qualifier == doc => continue,
                e => return Poll::Ready(Self::hold(&mut self.cur, self.weight, bm, e)),
            }
        }
    }

    /// Steps past `doc`: a walk that starts being walked again (#1363) stands after the
    /// documents already decided.
    fn pass(&mut self, bm: &Bm25, cx: &mut R::Cx<'_, '_>, doc: &[u8]) -> Poll<Result<()>> {
        if self.doc().is_some_and(|d| d <= doc) {
            ready!(self.advance(bm, cx, doc, true));
        }
        Poll::Ready(Ok(()))
    }

    /// The walk's contribution to `doc`, for a non-essential clause or an essential fuzzy
    /// clause's unwalked expansion (#1363); `doc`s come in
    /// ascending order. Point reads first, then, once their cost passes stepping through
    /// the rest of the row (lookups × [`LOOKUP_IN_STEPS`] > the postings left), advancing
    /// to `doc` for good: the ski-rental rule (perf287 on #1355), never much more than twice
    /// the better of the two, without knowing up front how many lookups will come.
    ///
    /// Async: a point read in flight is resumed first, so a re-poll finishes the lookup it
    /// started rather than deciding again.
    fn lookup(
        &mut self,
        t: &Rows,
        s: &Snapshot,
        bm: &Bm25,
        cx: &mut R::Cx<'_, '_>,
        doc: &[u8],
    ) -> Poll<Result<f64>> {
        if !R::get_pending(&self.get) {
            if !self.advancing {
                if R::spend(cx, LOOKUP_IN_STEPS as u32) {
                    return Poll::Pending;
                }
                self.lookups += 1;
                let left = self.postings.saturating_sub(self.stepped);
                self.advancing = self.lookups.saturating_mul(LOOKUP_IN_STEPS) > left;
            }
            if self.advancing {
                if self.doc().is_some_and(|d| d < doc) {
                    ready!(self.advance(bm, cx, doc, false));
                }
                return Poll::Ready(Ok(match &self.cur {
                    Some((d, c)) if d.as_slice() == doc => *c,
                    _ => 0.0,
                }));
            }
        }
        match ready!(R::get(t, s, &self.row, doc, &mut self.get, cx)) {
            Some(v) => {
                let (tf, len) = unposting(v.ok_or_else(|| corrupt("a posting isn't a u64"))?);
                Poll::Ready(Ok(self.weight * bm.tf_factor(f64::from(tf), f64::from(len))))
            }
            None => Poll::Ready(Ok(0.0)),
        }
    }
}

/// A clause as MaxScore runs it.
pub(crate) enum Clause<'t, R: Reads> {
    /// A term's posting row.
    Term(Walk<'t, R>),
    /// A fuzzy term's expansions (D309 §4.4): a document's contribution is the largest of
    /// theirs, not their sum.
    Fuzzy {
        /// The expansions still in play, largest bound first. One whose bound falls below
        /// what a document would need from it is dropped ([`Scored::drop_below`], #1333).
        walks: Vec<Walk<'t, R>>,
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
pub(crate) struct Scored<'t, R: Reads> {
    pub(crate) clause: Clause<'t, R>,
    pub(crate) bound: f64,
}

/// Where a lookup over several expansions stands (an async lookup resumes there): the next
/// expansion, and the largest contribution so far.
#[derive(Clone, Copy, Default)]
struct Progress {
    next: usize,
    best: f64,
}

impl<'t> Scored<'t, Blocking> {
    /// A term clause over its posting row (restricted to doc ids starting with `prefix`).
    pub(crate) fn term(
        t: &'t Rows,
        s: &Snapshot,
        bm: &Bm25,
        term: &str,
        df: i64,
        prefix: Option<&[u8]>,
    ) -> Result<Self> {
        let (walk, _) = Walk::new(t, s, bm, term, bm.idf(df), df.max(0) as u64, prefix)?;
        Ok(Scored::of_term(walk))
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
        for &(term, df, postings, boost) in expansions {
            let (walk, _) = Walk::new(t, s, bm, term, boost * bm.idf(df), postings, prefix)?;
            walks.push(walk);
        }
        Ok(Scored::of_fuzzy(walks))
    }
}

impl<'t, R: Reads> Scored<'t, R> {
    /// A term clause over its walk, which stands on its first posting.
    pub(crate) fn of_term(walk: Walk<'t, R>) -> Self {
        let bound = walk.bound;
        Scored {
            clause: Clause::Term(walk),
            bound: bound * (1.0 + SLACK),
        }
    }

    /// A fuzzy clause over its expansions' walks, each on its first posting.
    pub(crate) fn of_fuzzy(mut walks: Vec<Walk<'t, R>>) -> Self {
        let bound = walks.iter().map(|w| w.bound).fold(0.0f64, f64::max);
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
        c
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

    /// Moves to the next document. A fuzzy clause steps each walked expansion standing on its
    /// document, then settles: its document stays put until every one has stepped, so a
    /// re-poll after a `Pending` steps only those still on it.
    fn step(&mut self, bm: &Bm25, cx: &mut R::Cx<'_, '_>) -> Poll<Result<()>> {
        match &mut self.clause {
            Clause::Term(w) => ready!(w.step(bm, cx)),
            Clause::Fuzzy { walks, solo, cur } => {
                let Some((doc, _)) = cur.as_ref() else {
                    return Poll::Ready(Ok(()));
                };
                for w in walks[..*solo].iter_mut() {
                    if w.doc() == Some(doc.as_slice()) {
                        ready!(w.step(bm, cx));
                    }
                }
                self.settle();
            }
            Clause::Phrase { at, .. } => *at += 1,
        }
        Poll::Ready(Ok(()))
    }

    /// The clause's contribution to `doc`, for a non-essential clause: `doc`s come in
    /// ascending order ([`Walk::lookup`]). `at` holds a fuzzy clause's progress across its
    /// expansions, for a re-poll.
    fn lookup(
        &mut self,
        t: &Rows,
        s: &Snapshot,
        bm: &Bm25,
        cx: &mut R::Cx<'_, '_>,
        doc: &[u8],
        at: &mut Progress,
    ) -> Poll<Result<f64>> {
        match &mut self.clause {
            Clause::Term(w) => w.lookup(t, s, bm, cx, doc),
            Clause::Fuzzy { walks, .. } => Self::lookup_each(walks, t, s, bm, cx, doc, at),
            Clause::Phrase { scores, .. } => Poll::Ready(Ok(scores
                .binary_search_by(|(d, _)| d.as_slice().cmp(doc))
                .map_or(0.0, |i| scores[i].1))),
        }
    }

    /// The largest of `walks`' contributions to `doc`, resuming at `at`.
    fn lookup_each(
        walks: &mut [Walk<'t, R>],
        t: &Rows,
        s: &Snapshot,
        bm: &Bm25,
        cx: &mut R::Cx<'_, '_>,
        doc: &[u8],
        at: &mut Progress,
    ) -> Poll<Result<f64>> {
        while let Some(w) = walks.get_mut(at.next) {
            let v = ready!(w.lookup(t, s, bm, cx, doc));
            at.best = at.best.max(v);
            at.next += 1;
        }
        let best = at.best;
        *at = Progress::default();
        Poll::Ready(Ok(best))
    }

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
    /// decided, which every candidate from now on follows. A re-poll passes again: a walk
    /// already past `last` doesn't move.
    fn set_solo(
        &mut self,
        bm: &Bm25,
        cx: &mut R::Cx<'_, '_>,
        need: f64,
        last: Option<&[u8]>,
    ) -> Poll<Result<()>> {
        let Clause::Fuzzy { walks, solo, .. } = &mut self.clause else {
            return Poll::Ready(Ok(()));
        };
        let n = walks
            .iter()
            .take_while(|w| w.bound * (1.0 + SLACK) >= need * (1.0 - SLACK))
            .count();
        if n == *solo {
            return Poll::Ready(Ok(()));
        }
        if let Some(last) = last {
            for w in &mut walks[*solo..n.max(*solo)] {
                ready!(w.pass(bm, cx, last));
            }
        }
        *solo = n;
        self.settle();
        Poll::Ready(Ok(()))
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
    /// largest of theirs, resuming at `at`.
    fn lookup_unwalked(
        &mut self,
        t: &Rows,
        s: &Snapshot,
        bm: &Bm25,
        cx: &mut R::Cx<'_, '_>,
        doc: &[u8],
        at: &mut Progress,
    ) -> Poll<Result<f64>> {
        let Clause::Fuzzy { walks, solo, .. } = &mut self.clause else {
            return Poll::Ready(Ok(0.0));
        };
        Self::lookup_each(&mut walks[*solo..], t, s, bm, cx, doc, at)
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

/// Where [`TopK::run`] stands in its document loop: what a `Pending` leaves it at, so the
/// next poll resumes there (#1560). Each read point is resumable on its own ([`Reads`]); this
/// keeps what a candidate has done between them.
#[derive(Clone, Copy)]
enum At {
    /// Choosing the next candidate (after the threshold's re-cut, if it rose).
    Doc,
    /// Re-cutting: the essential fuzzy clauses' walked expansions, from `by_bound[i]`, with
    /// the budget left and whether one is unwalked so far.
    Cut { i: usize, budget: f64, unwalked: bool },
    /// Stepping the essential clauses on the candidate, from `by_bound[i]`.
    Step { i: usize, partial: f64 },
    /// The essential fuzzy clauses' unwalked expansions, from `by_bound[i]`; `looking` once
    /// `by_bound[i]`'s gain is taken and its lookup started.
    Gains {
        i: usize,
        partial: f64,
        gains: f64,
        looking: bool,
    },
    /// The non-essential clauses, from `by_bound[i - 1]` down.
    Rest { i: usize, partial: f64 },
}

/// MaxScore's document loop as a resumable run (#1560 §3.2): the sync search runs it to the
/// end over [`Blocking`], the async one polls it over [`Nonblocking`]. Its state lives here
/// between polls; [`TopK::run`] copies the scalars into locals and writes them back only on
/// `Pending` (perf287's T2), so the sync loop keeps them in registers.
pub(crate) struct TopK<'t, R: Reads> {
    clauses: Vec<Scored<'t, R>>,
    k: usize,
    /// Clause indexes by ascending bound; `prefix[i]` sums the first `i` bounds in that order.
    by_bound: Vec<usize>,
    prefix: Vec<f64>,
    /// The held documents, the worst on top.
    held: BinaryHeap<Held>,
    /// How many of `by_bound`'s first clauses are non-essential.
    skip: usize,
    /// The threshold the non-essential prefix and the fuzzy cuts were last taken at.
    cut_at: f64,
    /// Each clause's contribution to the candidate, `None` for one that doesn't match it.
    contrib: Vec<Option<f64>>,
    /// The candidate, then the last document decided: a walk that starts being walked again
    /// steps past it. One buffer, reused.
    last: Vec<u8>,
    decided: bool,
    /// Whether an essential fuzzy clause has unwalked expansions: only then does a candidate
    /// pay for the gains (single fuzzy clauses never do, since #1333's cut already drops what
    /// they'd leave unwalked).
    unwalked: bool,
    at: At,
    /// A lookup's progress across a fuzzy clause's expansions.
    lookup: Progress,
}

impl<'t, R: Reads> TopK<'t, R> {
    pub(crate) fn new(clauses: Vec<Scored<'t, R>>, k: usize) -> Self {
        debug_assert!(k > 0, "the search returns before MaxScore at k = 0");
        let n = clauses.len();
        let mut by_bound: Vec<usize> = (0..n).collect();
        by_bound.sort_by(|&a, &b| clauses[a].bound.total_cmp(&clauses[b].bound));
        let mut prefix = vec![0.0; n + 1];
        for (i, &c) in by_bound.iter().enumerate() {
            prefix[i + 1] = prefix[i] + clauses[c].bound;
        }
        TopK {
            clauses,
            k,
            by_bound,
            prefix,
            held: BinaryHeap::with_capacity(k + 1),
            skip: 0,
            cut_at: f64::NEG_INFINITY,
            contrib: vec![None; n],
            last: Vec::new(),
            decided: false,
            unwalked: false,
            at: At::Doc,
            lookup: Progress::default(),
        }
    }

    /// The top `k` documents of the clauses (in their fixed order), by score descending then
    /// id ascending, with their exact scores.
    pub(crate) fn finish(self) -> Vec<(Vec<u8>, f64)> {
        // Best first: score descending, then id ascending (`Held` orders the worst greatest).
        let mut out = self.held.into_vec();
        out.sort();
        out.into_iter().map(|Held(s, d)| (d, s)).collect()
    }

    /// Runs the document loop until it's done (`Ready(Ok)`), or, async, until a read is
    /// pending or the poll's budget is spent (`Pending`, resumed by the next call).
    pub(crate) fn run(
        &mut self,
        t: &Rows,
        s: &Snapshot,
        bm: &Bm25,
        cx: &mut R::Cx<'_, '_>,
    ) -> Poll<Result<()>> {
        let n = self.clauses.len();
        let k = self.k;
        let (mut skip, mut cut_at, mut decided, mut unwalked, mut at, mut lookup) = (
            self.skip,
            self.cut_at,
            self.decided,
            self.unwalked,
            self.at,
            self.lookup,
        );
        // A read's `Pending`: the scalars go back to `self` for the next poll (T2).
        macro_rules! poll {
            ($e:expr) => {
                match $e {
                    Poll::Ready(Ok(v)) => v,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => {
                        self.skip = skip;
                        self.cut_at = cut_at;
                        self.decided = decided;
                        self.unwalked = unwalked;
                        self.at = at;
                        self.lookup = lookup;
                        return Poll::Pending;
                    }
                }
            };
        }
        'docs: loop {
            let threshold = (self.held.len() == k).then(|| self.held.peek().expect("k > 0 held").0);
            if let At::Doc = at {
                // Only when the threshold has risen since the last pass: nothing else moves
                // the non-essential prefix or the fuzzy cuts (perf287 and simp724 on #1353).
                if let Some(th) = threshold.filter(|&th| th > cut_at) {
                    cut_at = th;
                    while skip < n && self.prefix[skip + 1] < th {
                        skip += 1;
                    }
                    // Inside every fuzzy clause, essential or not, the expansions that can't
                    // lift a document to the threshold even with every other clause at its
                    // bound (#1333). A non-essential one is then looked up over fewer
                    // expansions.
                    let total = self.prefix[n];
                    for c in self.clauses.iter_mut() {
                        let cut = th - (total - c.bound);
                        c.drop_below(cut);
                    }
                    at = At::Cut {
                        i: skip,
                        budget: th - self.prefix[skip],
                        unwalked: false,
                    };
                }
            }
            // Inside every essential fuzzy clause, the expansions walked (#1363). They share
            // one budget: a document only unwalked expansions match, across every essential
            // fuzzy clause, scores below their largest bounds summed plus the non-essential
            // bounds, and that sum stays below the threshold.
            if let At::Cut {
                mut i,
                mut budget,
                unwalked: mut uw,
            } = at
            {
                while i < n {
                    let c = self.by_bound[i];
                    let last = decided.then_some(self.last.as_slice());
                    poll!(self.clauses[c].set_solo(bm, cx, budget, last));
                    let up = self.clauses[c].unwalked();
                    budget -= up;
                    uw |= up > 0.0;
                    i += 1;
                    at = At::Cut {
                        i,
                        budget,
                        unwalked: uw,
                    };
                }
                unwalked = uw;
                at = At::Doc;
            }
            if let At::Doc = at {
                let Some(next) = self.by_bound[skip..]
                    .iter()
                    .filter_map(|&c| self.clauses[c].doc())
                    .min()
                else {
                    break;
                };
                // The candidate, in `last`'s buffer: once it's decided, it's the last
                // document decided, which the next pass's re-walk steps past. No allocation
                // per candidate; a document that enters the top `k` is copied.
                self.last.clear();
                self.last.extend_from_slice(next);
                decided = true;
                self.contrib.fill(None);
                at = At::Step {
                    i: skip,
                    partial: 0.0,
                };
            }
            if let At::Step { mut i, mut partial } = at {
                while i < n {
                    let c = self.by_bound[i];
                    if self.clauses[c].doc() == Some(self.last.as_slice()) {
                        // Taken once: a re-poll of the step finds it taken.
                        if self.contrib[c].is_none() {
                            let v = self.clauses[c].here();
                            self.contrib[c] = Some(v);
                            partial += v;
                            at = At::Step { i, partial };
                        }
                        poll!(self.clauses[c].step(bm, cx));
                    }
                    i += 1;
                    at = At::Step { i, partial };
                }
                // The essential fuzzy clauses' unwalked expansions (#1363): each can raise
                // its clause's contribution to at most their largest bound.
                let gains: f64 = if unwalked {
                    self.by_bound[skip..]
                        .iter()
                        .map(|&c| {
                            (self.clauses[c].unwalked() - self.contrib[c].unwrap_or(0.0)).max(0.0)
                        })
                        .sum()
                } else {
                    0.0
                };
                at = At::Gains {
                    i: if unwalked { skip } else { n },
                    partial,
                    gains,
                    looking: false,
                };
            }
            if let At::Gains {
                mut i,
                mut partial,
                mut gains,
                mut looking,
            } = at
            {
                while i < n {
                    let c = self.by_bound[i];
                    let here = self.contrib[c].unwrap_or(0.0);
                    if !looking {
                        let up = self.clauses[c].unwalked();
                        if up <= here {
                            i += 1;
                            continue;
                        }
                        let th = threshold.expect("expansions are unwalked only with k held");
                        if partial + gains + self.prefix[skip] < th * (1.0 - SLACK) {
                            at = At::Doc;
                            continue 'docs;
                        }
                        gains -= up - here;
                        looking = true;
                        at = At::Gains {
                            i,
                            partial,
                            gains,
                            looking,
                        };
                    }
                    let v = poll!(self.clauses[c].lookup_unwalked(
                        t,
                        s,
                        bm,
                        cx,
                        &self.last,
                        &mut lookup
                    ));
                    if v > here {
                        self.contrib[c] = Some(v);
                        partial += v - here;
                    }
                    looking = false;
                    i += 1;
                    at = At::Gains {
                        i,
                        partial,
                        gains,
                        looking,
                    };
                }
                at = At::Rest { i: skip, partial };
            }
            if let At::Rest { mut i, mut partial } = at {
                // The non-essential clauses, largest bound first, while the document can
                // still make it.
                while i > 0 {
                    let th = threshold.expect("clauses are non-essential only with k held");
                    if partial + self.prefix[i] < th * (1.0 - SLACK) {
                        at = At::Doc;
                        continue 'docs;
                    }
                    let c = self.by_bound[i - 1];
                    let v = poll!(self.clauses[c].lookup(
                        t,
                        s,
                        bm,
                        cx,
                        &self.last,
                        &mut lookup
                    ));
                    if v != 0.0 {
                        self.contrib[c] = Some(v);
                        partial += v;
                    }
                    i -= 1;
                    at = At::Rest { i, partial };
                }
                at = At::Doc;
            }
            // The exact score: contributions in the clauses' fixed order.
            let score = self.contrib.iter().flatten().fold(0.0, |sum, v| sum + v);
            let doc: &[u8] = &self.last;
            if self.held.len() < k {
                self.held.push(Held(score, doc.to_vec()));
            } else {
                // Better than the worst held (`Held`'s order: a higher score, or an equal
                // score and a smaller id): it takes the worst one's place.
                let worst = self.held.peek().expect("k > 0 held");
                let better = match score.total_cmp(&worst.0) {
                    Ordering::Greater => true,
                    Ordering::Equal => doc < worst.1.as_slice(),
                    Ordering::Less => false,
                };
                if better {
                    self.held.pop();
                    self.held.push(Held(score, doc.to_vec()));
                }
            }
        }
        self.skip = skip;
        self.cut_at = cut_at;
        self.decided = decided;
        self.unwalked = unwalked;
        self.at = at;
        self.lookup = lookup;
        Poll::Ready(Ok(()))
    }
}

/// The top `k` documents of `clauses` (in their fixed order), by score descending then id
/// ascending, with their exact scores: the sync search's run of [`TopK`].
pub(crate) fn top_k(
    t: &Rows,
    s: &Snapshot,
    bm: &Bm25,
    clauses: Vec<Scored<'_, Blocking>>,
    k: usize,
) -> Result<Vec<(Vec<u8>, f64)>> {
    let mut run = TopK::new(clauses, k);
    match run.run(t, s, bm, &mut ()) {
        Poll::Ready(r) => r?,
        Poll::Pending => unreachable!("sync reads are always ready"),
    }
    Ok(run.finish())
}
