//! `Text`'s BM25 indexes (D313 ticket 7c, D309, ICR 0105): the open handles, by name, and
//! the requests' query and option mapping.
//!
//! - **One handle per index**, opened on first use (on the blocking pool: an open reads the
//!   index's metadata) and kept until the server stops. An index has no thread of its own.
//! - **Clauses map by field name** (ICR 0105's amendment for 7c): a `term` field is matched as
//!   given (`Query::term`, `Query::fuzzy`); a `text` field is tokenised as documents are
//!   (`Query::text`, and a phrase's text as `Query::quoted_phrase`, the syntax's `"..."~N`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use pigeonhole::Pigeonhole;
use pigeonhole_text::{Fuzziness, Index, IndexOptions, Normalization, Query, Source};

use super::errors::ApiError;
use super::proto;

#[derive(Debug)]
pub(crate) struct Texts {
    db: Pigeonhole,
    open: Mutex<HashMap<String, Arc<Index>>>,
}

impl Texts {
    pub(crate) fn new(db: Pigeonhole) -> Self {
        Self {
            db,
            open: Mutex::new(HashMap::new()),
        }
    }

    fn handles(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<Index>>> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The index `name` if this server has it open: no I/O, so on any thread.
    pub(crate) fn cached(&self, name: &str) -> Option<Arc<Index>> {
        self.handles().get(name).cloned()
    }

    /// The index `name`, opened if this server hasn't yet. Sync: call it on the blocking
    /// pool. The open runs outside the cache's lock; two first calls racing both open it,
    /// and the first to finish is kept.
    pub(crate) fn get(&self, name: &str) -> Result<Arc<Index>, ApiError> {
        if let Some(i) = self.cached(name) {
            return Ok(i);
        }
        let index = Index::open(&self.db, name)?;
        Ok(self.keep(name, index))
    }

    /// Creates the index `name` over `source`, or with `if_missing` opens an existing one,
    /// whatever its source and options: `if_missing` compares neither. Sync, as `get`.
    pub(crate) fn create(
        &self,
        name: &str,
        source: Source,
        options: IndexOptions,
        if_missing: bool,
    ) -> Result<(), ApiError> {
        if if_missing && self.cached(name).is_some() {
            return Ok(());
        }
        let index = match Index::create(&self.db, name, source, options) {
            Ok(i) => i,
            Err(e) if if_missing && e.code() == pigeonhole::ErrorCode::TableExists => {
                Index::open(&self.db, name)?
            }
            Err(e) => return Err(e.into()),
        };
        self.keep(name, index);
        Ok(())
    }

    fn keep(&self, name: &str, index: Index) -> Arc<Index> {
        Arc::clone(
            self.handles()
                .entry(name.to_owned())
                .or_insert_with(|| Arc::new(index)),
        )
    }
}

/// A request's index options: unset ones take the library's defaults.
pub(crate) fn options(r: &proto::CreateTextIndexRequest) -> Result<IndexOptions, ApiError> {
    let mut o = IndexOptions::new();
    if let Some(k1) = r.k1 {
        o = o.k1(k1);
    }
    if let Some(b) = r.b {
        o = o.b(b);
    }
    if let Some(on) = r.positions {
        o = o.positions(on);
    }
    if let Some(on) = r.stop_words {
        o = o.stop_words(on);
    }
    if let Some(n) = r.max_terms {
        o = o.max_terms(n);
    }
    if let Some(n) = r.max_term_bytes {
        o = o.max_term_bytes(n);
    }
    o = match proto::Normalization::try_from(r.normalization) {
        Ok(proto::Normalization::Unspecified) => o,
        Ok(proto::Normalization::Nfc) => o.normalization(Normalization::Nfc),
        Ok(proto::Normalization::Nfkc) => o.normalization(Normalization::Nfkc),
        Ok(proto::Normalization::None) => o.normalization(Normalization::None),
        Err(_) => return Err(ApiError::invalid("an unknown normalization")),
    };
    Ok(o)
}

/// A search's query: the syntax, parsed at once so a bad one is `InvalidArgument` before the
/// search, or the clauses in the request's order. A repeated clause is refused: the library
/// counts it once, which would shift `TextMatch.clause`, the clause's index in the request
/// (ai1249's D1 on 7c).
pub(crate) fn query(r: &proto::TextSearchRequest) -> Result<Query, ApiError> {
    use proto::text_clause::Clause as C;
    use proto::text_search_request::Query as Q;
    match &r.query {
        None => Err(ApiError::invalid(
            "a search needs a query: `syntax` or `clauses`",
        )),
        Some(Q::Syntax(s)) => Ok(Query::parse(s)?),
        Some(Q::Clauses(q)) => {
            let mut out = Query::new();
            for (i, c) in q.clauses.iter().enumerate() {
                let before = out.clauses().len();
                out = match &c.clause {
                    Some(C::Term(t)) => out.term(t.as_str()),
                    Some(C::Text(t)) => out.text(t.as_str()),
                    Some(C::Phrase(p)) => {
                        let slop = u16::try_from(p.slop)
                            .map_err(|_| ApiError::invalid("a phrase's slop is at most 65,535"))?;
                        out.quoted_phrase(p.text.as_str(), slop)
                    }
                    Some(C::Fuzzy(f)) => out.fuzzy(f.term.as_str(), fuzziness(f.fuzziness)?),
                    None => return Err(ApiError::invalid(format!("clause {i} is empty"))),
                };
                if out.clauses().len() == before {
                    return Err(ApiError::invalid(format!(
                        "clause {i} repeats an earlier clause"
                    )));
                }
            }
            Ok(out)
        }
    }
}

fn fuzziness(f: i32) -> Result<Fuzziness, ApiError> {
    Ok(match proto::Fuzziness::try_from(f) {
        Ok(proto::Fuzziness::Unspecified | proto::Fuzziness::Auto) => Fuzziness::Auto,
        Ok(proto::Fuzziness::One) => Fuzziness::One,
        Ok(proto::Fuzziness::Two) => Fuzziness::Two,
        Err(_) => return Err(ApiError::invalid("an unknown fuzziness")),
    })
}

/// A hit as the proto has it.
pub(crate) fn hit(h: pigeonhole_text::Hit) -> proto::TextHit {
    proto::TextHit {
        id: h.id.into(),
        score: h.score,
        text: h.text.unwrap_or_default(),
        matches: h
            .matches
            .into_iter()
            .map(|m| proto::TextMatch {
                clause: u32::try_from(m.clause).unwrap_or(u32::MAX),
                occurrence: m.occurrence,
                start: m.range.start as u64,
                end: m.range.end as u64,
            })
            .collect(),
    }
}
