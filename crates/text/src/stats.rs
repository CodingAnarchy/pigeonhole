//! A search's ranking statistics as a value (#1722, ICR 0138): what one index's search
//! scores with, read on its own ([`Search::query_stats`](crate::Search::query_stats)),
//! merged across the indexes a collection is split into ([`QueryStats::merge`]), and handed
//! back to each one's search ([`Search::stats`](crate::Search::stats)).
//!
//! A search always runs this way: a single index's search reads its statistics, then ranks
//! with them, so a search given another index's numbers runs the same code.

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::fuzzy::{Expansion, finish};
use crate::maxscore::Bm25;

/// [`QueryStats::to_bytes`]' version, its first byte.
const VERSION: u8 = 1;

/// The statistics one search ranks with: the document count and total length, each query
/// term's document frequency, and each fuzzy term's expansions. Opaque: they come from
/// [`Search::query_stats`](crate::Search::query_stats) or [`merge`](Self::merge), and go to
/// [`Search::stats`](crate::Search::stats).
///
/// This is how a text index split across several databases (a cluster's partitions) is
/// searched with the scores one index over every document would give:
/// 1. each part's [`query_stats`](crate::Search::query_stats), every part included, each
///    read at one snapshot of its database;
/// 2. [`merge`](Self::merge) of them all;
/// 3. each part's search with the merged statistics ([`stats`](crate::Search::stats));
/// 4. the parts' hits merged by score descending, then id ascending, and cut at `k`.
///
/// Scores are then the ones one index would compute, up to float rounding in the order a
/// document's clauses are added. Two limits:
/// - **A fuzzy term whose expansions were cut** ([`cut`](Self::cut)) may keep other terms
///   than one index would: a part leaves out a term it ranks past
///   [`max_expansions`](crate::Search::max_expansions), and the merge can't bring it back.
///   A larger `max_expansions` makes a cut rarer.
/// - **The parts' snapshots are separate**, so a write landing between steps 1 and 3 is in
///   one step and not the other.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole};
/// use pigeonhole_text::{Index, IndexOptions, QueryStats, Source};
///
/// # fn main() -> pigeonhole_text::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let docs = db.table("docs")?.family("body", Family::default()).create_if_missing()?;
/// let index = |name: &str| {
///     Index::create(&db, name, Source::new(&docs, "body", b"text"), IndexOptions::new())
/// };
/// // One collection split in two, and the same documents in one index.
/// let (a, b, whole) = (index("a")?, index("b")?, index("whole")?);
/// for (id, text) in [(&b"d1"[..], "memory store"), (b"d2", "vector store")] {
///     a.put(id, text)?;
///     whole.put(id, text)?;
/// }
/// for (id, text) in [(&b"d3"[..], "memory memory"), (b"d4", "store")] {
///     b.put(id, text)?;
///     whole.put(id, text)?;
/// }
/// let q = "memory store";
/// let stats = QueryStats::merge([a.search(q, 10).query_stats()?, b.search(q, 10).query_stats()?])?;
/// assert_eq!(stats.documents(), 4);
/// let mut hits = a.search(q, 10).stats(stats.clone()).run()?;
/// hits.extend(b.search(q, 10).stats(stats).run()?);
/// hits.sort_by(|x, y| y.score.total_cmp(&x.score).then_with(|| x.id.cmp(&y.id)));
/// let one = whole.search(q, 10).run()?;
/// assert_eq!(hits.len(), one.len());
/// for (h, o) in hits.iter().zip(&one) {
///     assert_eq!(h.id, o.id);
///     assert!((h.score - o.score).abs() <= 1e-9 * o.score.abs());
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct QueryStats(pub(crate) Stats);

/// [`QueryStats`]' contents.
#[derive(Debug, Clone)]
pub(crate) struct Stats {
    /// The documents, and their lengths summed (the `S` row's counters).
    pub(crate) docs: i64,
    pub(crate) len: i64,
    /// The search's fuzzy options, which decide the expansions: statistics merge only with
    /// others read under the same ones.
    pub(crate) max_expansions: usize,
    pub(crate) prefix_length: usize,
    /// Each query term's `df`, and each phrase term's.
    pub(crate) df: HashMap<String, i64>,
    /// Each fuzzy term's expansions, in the order the query's clauses yield them.
    pub(crate) fuzzy: Vec<Fuzzy>,
}

/// A fuzzy term's part of [`Stats`].
#[derive(Debug, Clone)]
pub(crate) struct Fuzzy {
    /// The clauses that yield it.
    pub(crate) clauses: Vec<usize>,
    pub(crate) term: String,
    pub(crate) edits: u8,
    /// Its expansions in [`finish`]'s order, each `df` the statistic it scores with.
    pub(crate) expansions: Vec<Expansion>,
    /// Whether `max_expansions` left matching terms out, here or in any part merged.
    pub(crate) cut: bool,
}

/// The error for statistics that don't belong to the search or to each other.
pub(crate) fn other_query() -> Error {
    Error::InvalidArgument(
        "the query statistics are for another query or other fuzzy options".into(),
    )
}

impl Stats {
    /// BM25 over these statistics, or `None` for no documents (nothing matches).
    pub(crate) fn bm25(&self, k1: f64, b: f64) -> Option<Bm25> {
        (self.docs > 0).then(|| {
            let n = self.docs as f64;
            Bm25 {
                n,
                avglen: (self.len.max(0) as f64 / n).max(f64::MIN_POSITIVE),
                k1,
                b,
            }
        })
    }

    /// Whether `other` holds the statistics of the same terms and fuzzy terms, read under
    /// the same fuzzy options.
    pub(crate) fn same_query(&self, other: &Stats) -> bool {
        self.max_expansions == other.max_expansions
            && self.prefix_length == other.prefix_length
            && self.df.len() == other.df.len()
            && self.df.keys().all(|t| other.df.contains_key(t))
            && self.fuzzy.len() == other.fuzzy.len()
            && self
                .fuzzy
                .iter()
                .zip(&other.fuzzy)
                .all(|(f, g)| f.term == g.term && f.edits == g.edits && f.clauses == g.clauses)
    }

    /// Adds `other`'s counts to these, and its expansions to each fuzzy term's (not cut
    /// again: [`QueryStats::merge`] does that once, at the end).
    fn add(&mut self, other: Stats) -> Result<()> {
        if !self.same_query(&other) {
            return Err(other_query());
        }
        self.docs = self.docs.saturating_add(other.docs);
        self.len = self.len.saturating_add(other.len);
        for (term, df) in other.df {
            let mine = self.df.get_mut(&term).expect("the same terms");
            *mine = mine.saturating_add(df);
        }
        for (f, g) in self.fuzzy.iter_mut().zip(other.fuzzy) {
            f.cut |= g.cut;
            let at: HashMap<String, usize> = f
                .expansions
                .iter()
                .enumerate()
                .map(|(i, e)| (e.term.clone(), i))
                .collect();
            for e in g.expansions {
                match at.get(&e.term) {
                    Some(&i) => {
                        let mine = &mut f.expansions[i];
                        // A term's distance from the fuzzy term (so its boost) is the same
                        // wherever it's read.
                        if mine.distance != e.distance {
                            return Err(other_query());
                        }
                        mine.df = mine.df.saturating_add(e.df);
                    }
                    None => f.expansions.push(e),
                }
            }
        }
        Ok(())
    }
}

impl QueryStats {
    /// The statistics of every part together: the counts summed, and each fuzzy term's
    /// expansions joined and cut again to
    /// [`max_expansions`](crate::Search::max_expansions) by the search's own order (the
    /// closest, then the highest summed `df`, then bytewise).
    ///
    /// # Errors
    /// `InvalidArgument` for no parts, or parts of different queries or fuzzy options.
    pub fn merge(parts: impl IntoIterator<Item = QueryStats>) -> Result<QueryStats> {
        let mut parts = parts.into_iter();
        let Some(QueryStats(mut all)) = parts.next() else {
            return Err(Error::InvalidArgument(
                "there are no query statistics to merge".into(),
            ));
        };
        for QueryStats(part) in parts {
            all.add(part)?;
        }
        for f in &mut all.fuzzy {
            let (kept, cut) = finish(std::mem::take(&mut f.expansions), all.max_expansions);
            f.expansions = kept;
            f.cut |= cut;
        }
        Ok(QueryStats(all))
    }

    /// The documents they count.
    pub fn documents(&self) -> u64 {
        self.0.docs.max(0) as u64
    }

    /// Whether a fuzzy term of the query's clause `clause` (an index into
    /// [`Query::clauses`](crate::Query::clauses)) had its expansions cut, in any part
    /// merged or by the merge. `false` for a clause that isn't fuzzy.
    pub fn cut(&self, clause: usize) -> bool {
        self.0
            .fuzzy
            .iter()
            .any(|f| f.cut && f.clauses.contains(&clause))
    }

    /// A versioned encoding, for sending to another process: its first byte is the version.
    pub fn to_bytes(&self) -> Vec<u8> {
        let s = &self.0;
        let mut out = vec![VERSION];
        put_i64(&mut out, s.docs);
        put_i64(&mut out, s.len);
        put_len(&mut out, s.max_expansions);
        put_len(&mut out, s.prefix_length);
        let mut terms: Vec<(&String, &i64)> = s.df.iter().collect();
        terms.sort();
        put_len(&mut out, terms.len());
        for (term, df) in terms {
            put_str(&mut out, term);
            put_i64(&mut out, *df);
        }
        put_len(&mut out, s.fuzzy.len());
        for f in &s.fuzzy {
            put_str(&mut out, &f.term);
            out.push(f.edits);
            out.push(u8::from(f.cut));
            put_len(&mut out, f.clauses.len());
            for &c in &f.clauses {
                put_len(&mut out, c);
            }
            put_len(&mut out, f.expansions.len());
            for e in &f.expansions {
                put_str(&mut out, &e.term);
                out.push(e.distance);
                put_i64(&mut out, e.df);
                out.extend_from_slice(&e.boost.to_bits().to_le_bytes());
            }
        }
        out
    }

    /// Statistics from [`to_bytes`](Self::to_bytes).
    ///
    /// # Errors
    /// `InvalidArgument` for a version this build doesn't read (a newer build's), or bytes
    /// that don't parse.
    pub fn from_bytes(bytes: &[u8]) -> Result<QueryStats> {
        let mut r = Reader(bytes);
        let version = r.u8()?;
        if version != VERSION {
            return Err(Error::InvalidArgument(format!(
                "query statistics of version {version}; this build reads version {VERSION}"
            )));
        }
        let docs = r.i64()?;
        let len = r.i64()?;
        let max_expansions = r.len()?;
        let prefix_length = r.len()?;
        if max_expansions == 0 {
            return Err(unparsed("max_expansions is 0"));
        }
        let mut df = HashMap::new();
        for _ in 0..r.len()? {
            let term = r.string()?;
            if df.insert(term, r.i64()?).is_some() {
                return Err(unparsed("a term is listed twice"));
            }
        }
        let mut fuzzy = Vec::new();
        for _ in 0..r.len()? {
            let term = r.string()?;
            let edits = r.u8()?;
            let cut = match r.u8()? {
                0 => false,
                1 => true,
                _ => return Err(unparsed("a cut flag isn't 0 or 1")),
            };
            if edits > 2 {
                return Err(unparsed("a fuzzy term allows more than 2 edits"));
            }
            let mut clauses = Vec::new();
            for _ in 0..r.len()? {
                clauses.push(r.len()?);
            }
            let mut expansions = Vec::new();
            for _ in 0..r.len()? {
                let term = r.string()?;
                let distance = r.u8()?;
                let df = r.i64()?;
                let boost = f64::from_bits(r.u64()?);
                if distance > edits || !boost.is_finite() {
                    return Err(unparsed("an expansion's distance or boost is out of range"));
                }
                expansions.push(Expansion {
                    term,
                    distance,
                    df,
                    boost,
                });
            }
            fuzzy.push(Fuzzy {
                clauses,
                term,
                edits,
                expansions,
                cut,
            });
        }
        if !r.0.is_empty() {
            return Err(unparsed("bytes follow the statistics"));
        }
        Ok(QueryStats(Stats {
            docs,
            len,
            max_expansions,
            prefix_length,
            df,
            fuzzy,
        }))
    }
}

/// The error for bytes [`QueryStats::from_bytes`] can't parse.
fn unparsed(what: &str) -> Error {
    Error::InvalidArgument(format!("query statistics don't parse: {what}"))
}

fn put_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_len(out: &mut Vec<u8>, n: usize) {
    out.extend_from_slice(&(n as u64).to_le_bytes());
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_len(out, s.len());
    out.extend_from_slice(s.as_bytes());
}

/// [`QueryStats::from_bytes`]' cursor.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            return Err(unparsed("they end early"));
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(self.u64()? as i64)
    }

    /// A length, count or option. Nothing is allocated by a count (each item is read in
    /// turn), so a corrupt one fails when the bytes run out.
    fn len(&mut self) -> Result<usize> {
        usize::try_from(self.u64()?).map_err(|_| unparsed("a length is out of range"))
    }

    fn string(&mut self) -> Result<String> {
        let n = self.len()?;
        String::from_utf8(self.take(n)?.to_vec()).map_err(|_| unparsed("a term isn't UTF-8"))
    }
}
