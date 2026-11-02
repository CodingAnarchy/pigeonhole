//! MaxScore (Turtle and Flood), doc at a time (D309 §4.2): the exact top `k`, reading only
//! the postings that can still change it.
//!
//! Every clause has an upper bound on what it can add to one document's score:
//! - a term clause: `idf · tf_factor(maxtf, minlen)`, from its posting row's `maxtf` and
//!   `minlen` aggregates. Those only ever over-estimate (M1), so the bound is safe;
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

use std::collections::BTreeMap;

use pigeonhole::{RowCells, Snapshot, Table};

use crate::error::{Result, corrupt};
use crate::layout::*;

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

/// A clause as MaxScore runs it.
pub(crate) enum Clause<'t> {
    /// A term's posting row, walked in document order.
    Term {
        row: Vec<u8>,
        idf: f64,
        /// Boxed: a walk is far larger than a phrase clause.
        cells: Box<RowCells<'t>>,
        /// The posting the walk stands on: its document and contribution.
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
        t: &'t Table,
        s: &Snapshot,
        bm: &Bm25,
        term: &str,
        df: i64,
        prefix: Option<&[u8]>,
    ) -> Result<Self> {
        let row = post_row(term);
        let idf = bm.idf(df);
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
        let mut c = Scored {
            clause: Clause::Term {
                row,
                idf,
                cells: Box::new(read.cells()?),
                cur: None,
            },
            bound: idf * factor * (1.0 + SLACK),
        };
        c.step(bm)?;
        Ok(c)
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
            Clause::Term { cur, .. } => cur.as_ref().map(|(d, _)| d.as_slice()),
            Clause::Phrase { scores, at } => scores.get(*at).map(|(d, _)| d.as_slice()),
        }
    }

    /// The contribution where it stands.
    fn here(&self) -> f64 {
        match &self.clause {
            Clause::Term { cur, .. } => cur.as_ref().map_or(0.0, |(_, c)| *c),
            Clause::Phrase { scores, at } => scores.get(*at).map_or(0.0, |(_, c)| *c),
        }
    }

    /// Moves to the next document.
    fn step(&mut self, bm: &Bm25) -> Result<()> {
        match &mut self.clause {
            Clause::Term {
                idf, cells, cur, ..
            } => {
                *cur = match cells.next()? {
                    Some(e) => {
                        let (tf, len) = unposting(
                            e.cell
                                .as_u64()
                                .ok_or_else(|| corrupt("a posting isn't a u64"))?,
                        );
                        Some((
                            e.qualifier.to_vec(),
                            *idf * bm.tf_factor(f64::from(tf), f64::from(len)),
                        ))
                    }
                    None => None,
                };
            }
            Clause::Phrase { at, .. } => *at += 1,
        }
        Ok(())
    }

    /// The clause's contribution to `doc`, looked up out of order (a non-essential clause).
    fn lookup(&self, t: &Table, s: &Snapshot, bm: &Bm25, doc: &[u8]) -> Result<f64> {
        match &self.clause {
            Clause::Term { row, idf, .. } => match t.get_at(s, row, POST, doc)? {
                Some(c) => {
                    let (tf, len) =
                        unposting(c.as_u64().ok_or_else(|| corrupt("a posting isn't a u64"))?);
                    Ok(*idf * bm.tf_factor(f64::from(tf), f64::from(len)))
                }
                None => Ok(0.0),
            },
            Clause::Phrase { scores, .. } => Ok(scores
                .binary_search_by(|(d, _)| d.as_slice().cmp(doc))
                .map_or(0.0, |i| scores[i].1)),
        }
    }
}

/// The top `k` documents of `clauses` (in their fixed order), by score descending then id
/// ascending, with their exact scores.
pub(crate) fn top_k(
    t: &Table,
    s: &Snapshot,
    bm: &Bm25,
    mut clauses: Vec<Scored<'_>>,
    k: usize,
) -> Result<Vec<(Vec<u8>, f64)>> {
    let n = clauses.len();
    // Ascending bound; `prefix[i]` sums the first `i` bounds in that order.
    let mut by_bound: Vec<usize> = (0..n).collect();
    by_bound.sort_by(|&a, &b| clauses[a].bound.total_cmp(&clauses[b].bound));
    let mut prefix = vec![0.0; n + 1];
    for (i, &c) in by_bound.iter().enumerate() {
        prefix[i + 1] = prefix[i] + clauses[c].bound;
    }
    // The held documents: (score, id), worst first by (score ascending, id descending).
    let mut held: Vec<(f64, Vec<u8>)> = Vec::new();
    let worse =
        |a: &(f64, Vec<u8>), b: &(f64, Vec<u8>)| a.0.total_cmp(&b.0).then_with(|| b.1.cmp(&a.1));
    // How many of `by_bound`'s first clauses are non-essential.
    let mut skip = 0;
    let mut contrib = vec![0.0f64; n];
    let mut has = vec![false; n];
    loop {
        let threshold = (held.len() == k).then(|| held[0].0);
        if let Some(th) = threshold {
            while skip < n && prefix[skip + 1] < th {
                skip += 1;
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
        let entry = (score, doc);
        if held.len() < k {
            held.push(entry);
            held.sort_by(worse);
        } else if worse(&held[0], &entry).is_lt() {
            held[0] = entry;
            held.sort_by(worse);
        }
    }
    held.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    Ok(held.into_iter().map(|(s, d)| (d, s)).collect())
}
