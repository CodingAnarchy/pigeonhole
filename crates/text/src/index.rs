//! The index (D309 §2, §3): create and open, writes through the index, `rebuild`, `repair`
//! and `verify`.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use pigeonhole::{Family, Pigeonhole, RetryPolicy, Snapshot, Table, Transaction};
use unicode_normalization::{IsNormalized, UnicodeNormalization, is_nfc_quick, is_nfkc_quick};

use crate::error::{Error, Result, corrupt};
use crate::layout::*;
use crate::query::Query;
use crate::search::Search;
use crate::tokenizer::{Standard, Tokenizer};

/// A full-text index over one source column (D309).
///
/// Clones share one handle. Every write to the source column must go through the index
/// ([`put_in`](Index::put_in), [`delete_in`](Index::delete_in), or their convenience forms):
/// a direct write or a row delete leaves the index stale until [`repair`](Index::repair).
/// That's the contract the consistency guarantee rests on (D309 §3, M3).
#[derive(Clone)]
pub struct Index {
    pub(crate) inner: Arc<Inner>,
}

impl std::fmt::Debug for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Index")
            .field("table", &self.inner.table.name())
            .field("source", &self.inner.source)
            .field("options", &self.inner.opts)
            .field("tokenizer", &self.inner.tok.name())
            .finish()
    }
}

pub(crate) struct Inner {
    pub(crate) db: Pigeonhole,
    pub(crate) table: Table,
    pub(crate) src: Table,
    pub(crate) source: Source,
    pub(crate) opts: IndexOptions,
    pub(crate) tok: Box<dyn Tokenizer>,
    /// The built-in tokeniser lowercases again after NFKC (D309 rev 6 §A.2).
    builtin: bool,
}

/// A document's terms, with their positions, and its length in terms.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Analysed {
    pub(crate) terms: BTreeMap<String, Vec<u32>>,
    pub(crate) len: u32,
}

/// What the index holds for a document: `F{doc}`.
#[derive(Debug, Default)]
struct Forward {
    present: bool,
    len: u32,
    terms: BTreeMap<Vec<u8>, u32>,
}

/// What [`Index::verify`] found, at one snapshot. Mismatches are counted, never fixed:
/// [`Index::repair`] fixes them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct IndexStats {
    /// Indexed documents (`F` rows).
    pub documents: u64,
    /// Distinct terms with postings.
    pub terms: u64,
    /// Postings.
    pub postings: u64,
    /// Counters that disagree with the postings and forward rows (`df`, the document count
    /// and the corpus length), and MaxScore bounds below a posting.
    pub counter_drift: u64,
    /// Forward rows whose terms or length disagree with their postings or positions.
    pub forward_mismatch: u64,
    /// Documents whose source text doesn't tokenise to their forward row, or that the index
    /// holds with no source text (a bypass of the index, D309 §3).
    pub source_mismatch: u64,
    /// Source cells that aren't valid UTF-8 (skipped).
    pub invalid_utf8: u64,
}

impl IndexStats {
    /// Whether every check agreed.
    pub fn is_consistent(&self) -> bool {
        self.counter_drift == 0 && self.forward_mismatch == 0 && self.source_mismatch == 0
    }
}

/// What [`Index::rebuild`] or [`Index::repair`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RebuildStats {
    /// Source documents read.
    pub documents: u64,
    /// Documents whose index rows changed.
    pub changed: u64,
    /// Source cells that aren't valid UTF-8 (skipped).
    pub invalid_utf8: u64,
}

/// A transaction body's failure: the database's error (retried as `transact` retries), or
/// the index's own.
enum Tx {
    Db(pigeonhole::Error),
    Fail(Error),
}

impl From<pigeonhole::Error> for Tx {
    fn from(e: pigeonhole::Error) -> Self {
        Tx::Db(e)
    }
}

impl From<Error> for Tx {
    fn from(e: Error) -> Self {
        match e {
            Error::Db(e) => Tx::Db(e),
            e => Tx::Fail(e),
        }
    }
}

impl From<Tx> for Error {
    fn from(t: Tx) -> Self {
        match t {
            Tx::Db(e) => Error::Db(e),
            Tx::Fail(e) => e,
        }
    }
}

/// How [`Index::apply`] treats what's stored.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// `put_in`: writes the source cell, and trusts `F{doc}` for the old postings.
    Put,
    /// `rebuild`: leaves the source cell, and compares every posting and position cell with
    /// what it should be (D309 rev 5).
    Rebuild,
}

impl Index {
    /// Creates the index `name` (a new table) over `source`, with the built-in tokeniser,
    /// and backfills it from the source column ([`rebuild`](Self::rebuild)).
    ///
    /// The source column and every option are stored with the index, so an
    /// [`open`](Self::open) can't disagree with them. `TableExists` if the table is there.
    pub fn create(
        db: &Pigeonhole,
        name: &str,
        source: Source,
        options: IndexOptions,
    ) -> Result<Index> {
        let opts = options.checked()?;
        let tok = Box::new(Standard {
            stop_words: opts.get_stop_words(),
        });
        Self::create_inner(db, name, source, opts, tok, true)
    }

    /// [`create`](Self::create) with a custom tokeniser. Its name and fingerprint are stored,
    /// and [`open_with_tokenizer`](Self::open_with_tokenizer) refuses another.
    /// `IndexOptions::stop_words` doesn't apply to it.
    pub fn create_with_tokenizer(
        db: &Pigeonhole,
        name: &str,
        source: Source,
        options: IndexOptions,
        tokenizer: Box<dyn Tokenizer>,
    ) -> Result<Index> {
        if tokenizer.name() == Standard::NAME {
            return Err(Error::InvalidArgument(format!(
                "a custom tokeniser can't be named {:?}, the built-in one's name",
                Standard::NAME
            )));
        }
        Self::create_inner(db, name, source, options.checked()?, tokenizer, false)
    }

    fn create_inner(
        db: &Pigeonhole,
        name: &str,
        source: Source,
        opts: IndexOptions,
        tok: Box<dyn Tokenizer>,
        builtin: bool,
    ) -> Result<Index> {
        if source.table == name {
            return Err(Error::InvalidArgument(
                "an index can't be its own source table".into(),
            ));
        }
        let src = db.table(&source.table)?.open()?;
        if src.family_info(&source.family).is_none() {
            return Err(Error::InvalidArgument(format!(
                "the source table {:?} has no family {:?}",
                source.table, source.family
            )));
        }
        let max1 = || Family::default().max_versions(1);
        let mut builder = db
            .table(name)?
            .family(POST, max1())
            .family(DF, Family::counter())
            .family(MAXTF, Family::aggregate(pigeonhole::Aggregation::Max))
            .family(MINLEN, Family::aggregate(pigeonhole::Aggregation::Min))
            .family(TERMS, max1())
            .family(META, max1())
            .family(STATS, Family::counter());
        if opts.get_positions() {
            builder = builder.family(POS, max1());
        }
        let table = builder.create()?;
        let (major, minor, update) = unicode_normalization::UNICODE_VERSION;
        table
            .mutate(GLOBAL)
            .put_u64(META, LAYOUT, LAYOUT_VERSION)
            .put(META, OPTS, &opts.encode())
            .put(META, SOURCE, &source.encode())
            .put(META, TOKENIZER, tok.name().as_bytes())
            .put_u64(META, FINGERPRINT, tok.fingerprint())
            // For diagnostics only: NFC and NFKC are stable for assigned characters.
            .put(
                META,
                NORMALIZER_UNICODE,
                format!("{major}.{minor}.{update}").as_bytes(),
            )
            .commit()?;
        let index = Index {
            inner: Arc::new(Inner {
                db: db.clone(),
                table,
                src,
                source,
                opts,
                tok,
                builtin,
            }),
        };
        index.rebuild_inner(false)?;
        Ok(index)
    }

    /// Opens the index `name`, with the built-in tokeniser. Everything else is read back
    /// from the index. `Mismatch` if it was created with a custom tokeniser, or with the
    /// built-in one at another version (a dependency bump that changed its word boundaries:
    /// [`repair`](Self::repair) it into a new index).
    pub fn open(db: &Pigeonhole, name: &str) -> Result<Index> {
        Self::open_inner(db, name, None)
    }

    /// Opens the index `name` with the custom tokeniser it was created with: `Mismatch` if
    /// the name or fingerprint differs.
    pub fn open_with_tokenizer(
        db: &Pigeonhole,
        name: &str,
        tokenizer: Box<dyn Tokenizer>,
    ) -> Result<Index> {
        Self::open_inner(db, name, Some(tokenizer))
    }

    fn open_inner(
        db: &Pigeonhole,
        name: &str,
        custom: Option<Box<dyn Tokenizer>>,
    ) -> Result<Index> {
        let table = db.table(name)?.open()?;
        let not = |why: &str| {
            Error::InvalidArgument(format!("table {name:?} is not a text index: {why}"))
        };
        let g = |q: &[u8]| {
            table
                .get(GLOBAL, META, q)
                .map(|c| c.map(|c| c.value().to_vec()))
        };
        match table.get(GLOBAL, META, LAYOUT)?.and_then(|c| c.as_u64()) {
            Some(LAYOUT_VERSION) => {}
            Some(v) => return Err(not(&format!("layout version {v} is unknown"))),
            None => return Err(not("it has no layout")),
        }
        let opts = g(OPTS)?
            .and_then(|b| IndexOptions::decode(&b))
            .ok_or_else(|| not("its options don't parse"))?;
        let source = g(SOURCE)?
            .and_then(|b| Source::decode(&b))
            .ok_or_else(|| not("its source doesn't parse"))?;
        let stored_name = g(TOKENIZER)?.ok_or_else(|| not("it names no tokeniser"))?;
        let stored_fp = table
            .get(GLOBAL, META, FINGERPRINT)?
            .and_then(|c| c.as_u64())
            .ok_or_else(|| not("it has no tokeniser fingerprint"))?;
        let builtin = custom.is_none();
        let tok: Box<dyn Tokenizer> = match custom {
            Some(t) => t,
            None => Box::new(Standard {
                stop_words: opts.get_stop_words(),
            }),
        };
        if stored_name != tok.name().as_bytes() {
            return Err(Error::Mismatch(format!(
                "the index was created with the tokeniser {:?}, not {:?}",
                String::from_utf8_lossy(&stored_name),
                tok.name()
            )));
        }
        if stored_fp != tok.fingerprint() {
            return Err(Error::Mismatch(format!(
                "the tokeniser {:?} has another fingerprint than the index was created with \
                 (its options or Unicode version changed): repair the index into a new one",
                tok.name()
            )));
        }
        let src = db.table(&source.table)?.open()?;
        Ok(Index {
            inner: Arc::new(Inner {
                db: db.clone(),
                table,
                src,
                source,
                opts,
                tok,
                builtin,
            }),
        })
    }

    /// The index's options.
    pub fn options(&self) -> IndexOptions {
        self.inner.opts
    }

    /// The column the index is built over.
    pub fn source(&self) -> &Source {
        &self.inner.source
    }

    /// The index's table.
    pub fn table(&self) -> &Table {
        &self.inner.table
    }

    /// Writes `text` as the document `doc`'s source text, and indexes it, inside the
    /// caller's transaction: the source cell, its postings and the statistics commit
    /// together, with the caller's own writes.
    ///
    /// Reads `F{doc}`, the source cell and the repair flag: two writes of one document
    /// conflict, and writes of different documents don't (every statistic is a blind
    /// counter). Putting the text the index already holds for `doc` writes nothing.
    ///
    /// **A bypass, then the same text:** if the source cell was changed directly (around the
    /// index) and the same new text is then put through it, the index still holds the
    /// bypassed state until [`repair`](Self::repair): the text matches, so nothing is
    /// written. That's the bypass contract (D309 §3).
    ///
    /// Like any `transact` body it may run more than once per commit. `InvalidArgument` for
    /// a document over [`IndexOptions::max_terms`] distinct terms, before anything is
    /// written; `Busy` during a [`repair`](Self::repair).
    pub fn put_in(&self, txn: &mut Transaction, doc: &[u8], text: &str) -> Result<()> {
        self.check_not_repairing(txn)?;
        let inn = &*self.inner;
        let stored = txn
            .get(&inn.src, doc, &inn.source.family, &inn.source.qualifier)?
            .map(|c| c.value() == text.as_bytes());
        let old = self.read_forward(txn, doc)?;
        if old.present && stored == Some(true) {
            return Ok(());
        }
        let new = self.analyse(text)?;
        txn.put(
            &inn.src,
            doc,
            &inn.source.family,
            &inn.source.qualifier,
            text.as_bytes(),
        );
        self.apply(txn, doc, &old, &new, Mode::Put)?;
        Ok(())
    }

    /// Deletes the document `doc`: its source cell and its index rows, inside the caller's
    /// transaction. `false` (and no counter changes) if the index didn't hold it; its source
    /// cell is deleted either way. `Busy` during a [`repair`](Self::repair).
    pub fn delete_in(&self, txn: &mut Transaction, doc: &[u8]) -> Result<bool> {
        self.check_not_repairing(txn)?;
        let inn = &*self.inner;
        let old = self.read_forward(txn, doc)?;
        txn.delete_column(&inn.src, doc, &inn.source.family, &inn.source.qualifier);
        if !old.present {
            return Ok(false);
        }
        // Deletes only: `apply` would also write `F{doc}`'s cells, which the row delete
        // below would meet at the same seqno.
        let t = &inn.table;
        for term in old.terms.keys() {
            let row = [POST_PREFIX, term].concat();
            txn.delete_column(t, &row, POST, doc);
            if inn.opts.get_positions() {
                txn.delete_column(t, &row, POS, doc);
            }
            txn.incr(t, &row, DF, N, -1);
        }
        txn.delete_row(t, &fwd_row(doc))
            .incr(t, STATS_ROW, STATS, DOCS, -1)
            .incr(t, STATS_ROW, STATS, LEN, -i64::from(old.len));
        Ok(true)
    }

    /// [`put_in`](Self::put_in) in its own transaction, retried on conflicts with
    /// `RetryPolicy::default()` (use `put_in` for any other policy).
    pub fn put(&self, doc: &[u8], text: &str) -> Result<()> {
        self.inner
            .db
            .transact(&RetryPolicy::default(), |tx| {
                self.put_in(tx, doc, text).map_err(Tx::from)
            })
            .map(|_| ())
            .map_err(Error::from)
    }

    /// [`delete_in`](Self::delete_in) in its own transaction, retried on conflicts with
    /// `RetryPolicy::default()`.
    pub fn delete(&self, doc: &[u8]) -> Result<bool> {
        self.inner
            .db
            .transact(&RetryPolicy::default(), |tx| {
                self.delete_in(tx, doc).map_err(Tx::from)
            })
            .map(|(b, _)| b)
            .map_err(Error::from)
    }

    /// The top `k` documents for `query`, as a [`Search`] to run. A `&str` is parsed with
    /// the query syntax ([`Query`]) when the search runs, and a parse error is returned
    /// then (`InvalidQuery`): use [`Query::parse`] to check a string first.
    pub fn search(&self, query: impl Into<Query>, k: usize) -> Search<'_> {
        Search::new(self, query.into(), k)
    }

    /// Indexes every document of the source column, each in its own transaction, while
    /// writes go on (D309 §3, M2): a backfill, or a correction of documents the index holds
    /// differently from their source text. Each document is diffed against what the index
    /// holds (`F{doc}` and every posting and position cell), so a document already right
    /// changes nothing.
    ///
    /// It never removes a document the source no longer has (a bypassed delete):
    /// [`repair`](Self::repair) does. Its counters (`df`, the document count and the corpus
    /// length) trust `F{doc}` as the record of what was indexed, so after a write around the
    /// index's own rows (not just the source column), use `repair`, which starts from an
    /// emptied table. `Busy` during a repair.
    pub fn rebuild(&self) -> Result<RebuildStats> {
        self.rebuild_inner(false)
    }

    /// Rebuilds the index from an emptied table (D309 §3, M2): drops every index row, then
    /// [`rebuild`](Self::rebuild)s. Writes through the index and searches of it fail with
    /// `Busy` until it finishes. If it's interrupted (a crash), the index stays `Busy` until
    /// `repair` is run again.
    pub fn repair(&self) -> Result<RebuildStats> {
        let inn = &*self.inner;
        let t = &inn.table;
        // The flag first: every write in flight read it, so they conflict and retry into Busy.
        t.mutate(GLOBAL).put(META, REPAIR, b"").commit()?;
        let s = inn.db.snapshot()?;
        for prefix in [POST_PREFIX, FWD_PREFIX, STATS_ROW] {
            let mut keys = Vec::new();
            for row in t.scan_prefix(prefix).snapshot(&s).iter()? {
                keys.push(row?.key().to_vec());
            }
            for chunk in keys.chunks(256) {
                let mut batch = inn.db.write_batch();
                for k in chunk {
                    batch.delete_row(t, k);
                }
                batch.commit()?;
            }
        }
        let stats = self.rebuild_inner(true)?;
        t.mutate(GLOBAL).delete_column(META, REPAIR).commit()?;
        Ok(stats)
    }

    fn rebuild_inner(&self, repairing: bool) -> Result<RebuildStats> {
        let inn = &*self.inner;
        let s = inn.db.snapshot()?;
        let mut stats = RebuildStats::default();
        let q = inn.source.qualifier.as_slice();
        let docs = inn
            .src
            .scan_prefix(b"")
            .family(&inn.source.family)
            .qualifier_bounds(std::ops::Bound::Included(q), std::ops::Bound::Included(q))
            .snapshot(&s)
            .iter()?;
        for row in docs {
            let doc = row?.key().to_vec();
            stats.documents += 1;
            let ((changed, bad), _) = inn.db.transact(&RetryPolicy::default(), |tx| {
                if !repairing {
                    self.check_not_repairing(tx)?;
                }
                // The current text, not the snapshot's: a put since the scan wins.
                let text = match tx.get(&inn.src, &doc, &inn.source.family, q)? {
                    None => return Ok((false, false)),
                    Some(c) => match std::str::from_utf8(c.value()) {
                        Ok(t) => t.to_owned(),
                        Err(_) => return Ok((false, true)),
                    },
                };
                let old = self.read_forward(tx, &doc)?;
                let new = self.analyse(&text)?;
                let changed = self.apply(tx, &doc, &old, &new, Mode::Rebuild)?;
                Ok::<_, Tx>((changed, false))
            })?;
            stats.changed += u64::from(changed);
            stats.invalid_utf8 += u64::from(bad);
        }
        Ok(stats)
    }

    fn check_not_repairing(&self, txn: &mut Transaction) -> Result<()> {
        match txn.get(&self.inner.table, GLOBAL, META, REPAIR)? {
            Some(_) => Err(Error::Busy),
            None => Ok(()),
        }
    }

    fn read_forward(&self, txn: &mut Transaction, doc: &[u8]) -> Result<Forward> {
        let mut f = Forward::default();
        let Some(row) = txn.get_row(&self.inner.table, &fwd_row(doc), &[TERMS, META])? else {
            return Ok(f);
        };
        for e in row.iter() {
            match (e.family, e.qualifier) {
                (META, PRESENT) => f.present = true,
                (META, DOC_LEN) => f.len = read_u32(e.cell.as_u64(), "a document length")?,
                (TERMS, term) => {
                    let tf = read_u32(e.cell.as_u64(), "a forward term frequency")?;
                    f.terms.insert(term.to_vec(), tf);
                }
                _ => {}
            }
        }
        Ok(f)
    }

    /// Writes the difference between what the index holds for `doc` (`old`) and `new`.
    /// Returns whether anything was written.
    fn apply(
        &self,
        tx: &mut Transaction,
        doc: &[u8],
        old: &Forward,
        new: &Analysed,
        mode: Mode,
    ) -> Result<bool> {
        let inn = &*self.inner;
        let t = &inn.table;
        let positions = inn.opts.get_positions();
        let fwd = fwd_row(doc);
        let mut changed = false;
        let mut buf = Vec::new();
        let len_changed = !old.present || old.len != new.len;
        for (term, ps) in &new.terms {
            let row = post_row(term);
            let tf = u32::try_from(ps.len()).unwrap_or(u32::MAX);
            let old_tf = old.terms.get(term.as_bytes()).copied();
            let value = posting(tf, new.len);
            let write_post = match mode {
                Mode::Put => old_tf != Some(tf) || len_changed,
                Mode::Rebuild => {
                    tx.get(t, &row, POST, doc)?.and_then(|c| c.as_u64()) != Some(value)
                }
            };
            if write_post {
                tx.put_u64(t, &row, POST, doc, value)
                    .aggregate(t, &row, MAXTF, BOUND, i64::from(tf))
                    .aggregate(t, &row, MINLEN, BOUND, i64::from(new.len));
                changed = true;
            }
            if positions {
                let enc = encode_positions(ps);
                let write_pos = match mode {
                    Mode::Put => true,
                    Mode::Rebuild => match tx.get(t, &row, POS, doc)? {
                        Some(c) => decode_positions(c.value(), &mut buf).is_err() || buf != *ps,
                        None => true,
                    },
                };
                if write_pos {
                    tx.put(t, &row, POS, doc, &enc);
                    changed = true;
                }
            }
            if old_tf.is_none() {
                tx.incr(t, &row, DF, N, 1);
            }
            if old_tf != Some(tf) {
                tx.put_u64(t, &fwd, TERMS, term.as_bytes(), u64::from(tf));
                changed = true;
            }
        }
        for term in old.terms.keys() {
            let Ok(term) = std::str::from_utf8(term) else {
                return Err(corrupt("a forward row's term isn't UTF-8"));
            };
            if new.terms.contains_key(term) {
                continue;
            }
            let row = post_row(term);
            tx.delete_column(t, &row, POST, doc);
            if positions {
                tx.delete_column(t, &row, POS, doc);
            }
            tx.incr(t, &row, DF, N, -1)
                .delete_column(t, &fwd, TERMS, term.as_bytes());
            changed = true;
        }
        if !old.present {
            tx.put(t, &fwd, META, PRESENT, b"")
                .incr(t, STATS_ROW, STATS, DOCS, 1);
            changed = true;
        }
        if len_changed {
            tx.put_u64(t, &fwd, META, DOC_LEN, u64::from(new.len)).incr(
                t,
                STATS_ROW,
                STATS,
                LEN,
                i64::from(new.len) - i64::from(old.len),
            );
            changed = true;
        }
        Ok(changed)
    }

    /// Tokenises and normalises `text`: its terms with their positions, and its length.
    pub(crate) fn analyse(&self, text: &str) -> Result<Analysed> {
        let inn = &*self.inner;
        let cap = inn.opts.get_max_term_bytes() as usize;
        let mut a = Analysed::default();
        inn.tok.tokens(text, &mut |tok| {
            let term = self.normalize_token(tok.text);
            if term.is_empty() || term.len() > cap {
                return;
            }
            a.len = a.len.saturating_add(1);
            let ps = a.terms.entry(term.into_owned()).or_default();
            if ps.last() != Some(&tok.position) {
                ps.push(tok.position);
            }
        });
        if a.terms.len() > inn.opts.get_max_terms() as usize {
            return Err(Error::InvalidArgument(format!(
                "a document has {} distinct terms, above max_terms {}",
                a.terms.len(),
                inn.opts.get_max_terms()
            )));
        }
        Ok(a)
    }

    /// Query text's terms with their positions, tokenised and normalised as documents are,
    /// in position order. An over-cap token is dropped and keeps its position, as in a
    /// document.
    pub(crate) fn query_tokens(&self, text: &str) -> Vec<(String, u32)> {
        let cap = self.inner.opts.get_max_term_bytes() as usize;
        let mut out = Vec::new();
        self.inner.tok.tokens(text, &mut |tok| {
            let term = self.normalize_token(tok.text);
            if !term.is_empty() && term.len() <= cap {
                out.push((term.into_owned(), tok.position));
            }
        });
        out
    }

    /// A token as the index stores it: normalised, and for the built-in tokeniser under
    /// NFKC, lowercased again (a compatibility form can fold to an uppercase letter).
    pub(crate) fn normalize_token<'a>(&self, s: &'a str) -> Cow<'a, str> {
        let n = normalize(self.inner.opts.get_normalization(), s);
        if self.inner.builtin
            && self.inner.opts.get_normalization() == Normalization::Nfkc
            && n.chars().any(char::is_uppercase)
        {
            return Cow::Owned(n.chars().flat_map(char::to_lowercase).collect());
        }
        n
    }

    /// A builder term as the index compares it: normalised, never tokenised, case-folded
    /// or capped (D309 rev 6 §A.4).
    pub(crate) fn normalize_term<'a>(&self, s: &'a str) -> Cow<'a, str> {
        normalize(self.inner.opts.get_normalization(), s)
    }

    /// Checks the index's rows against each other and against the source column, at one
    /// snapshot (D309 §3, M2). Mismatches are counted in the result, not returned as errors;
    /// `Corruption` only if a row doesn't parse.
    pub fn verify(&self) -> Result<IndexStats> {
        let inn = &*self.inner;
        let t = &inn.table;
        let s = inn.db.snapshot()?;
        let mut st = IndexStats::default();
        // Forward rows.
        let mut fwd: BTreeMap<Vec<u8>, Forward> = BTreeMap::new();
        for row in t
            .scan_prefix(FWD_PREFIX)
            .families([TERMS, META])
            .snapshot(&s)
            .iter()?
        {
            let row = row?;
            let mut f = Forward::default();
            for e in row.view().iter() {
                match (e.family, e.qualifier) {
                    (META, PRESENT) => f.present = true,
                    (META, DOC_LEN) => f.len = read_u32(e.cell.as_u64(), "a document length")?,
                    (TERMS, term) => {
                        f.terms.insert(
                            term.to_vec(),
                            read_u32(e.cell.as_u64(), "a term frequency")?,
                        );
                    }
                    _ => {}
                }
            }
            fwd.insert(row.key()[FWD_PREFIX.len()..].to_vec(), f);
        }
        st.documents = fwd.values().filter(|f| f.present).count() as u64;
        st.forward_mismatch += fwd.values().filter(|f| !f.present).count() as u64;
        // Postings, positions and per-term counters, a term row at a time.
        let mut seen: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
        let mut fams = vec![POST, DF, MAXTF, MINLEN];
        if inn.opts.get_positions() {
            fams.push(POS);
        }
        let mut buf = Vec::new();
        for row in t
            .scan_prefix(POST_PREFIX)
            .families(fams)
            .snapshot(&s)
            .iter()?
        {
            let row = row?;
            let term = row.key()[POST_PREFIX.len()..].to_vec();
            let (mut df, mut maxtf, mut minlen) = (0i64, i64::MIN, i64::MAX);
            let (mut max_seen, mut min_seen) = (0i64, i64::MAX);
            let mut post_docs: BTreeMap<Vec<u8>, u32> = BTreeMap::new();
            let mut pos_docs: BTreeMap<Vec<u8>, u32> = BTreeMap::new();
            for e in row.view().iter() {
                match e.family {
                    POST => {
                        let (tf, len) = unposting(
                            e.cell
                                .as_u64()
                                .ok_or_else(|| corrupt("a posting isn't a u64"))?,
                        );
                        max_seen = max_seen.max(i64::from(tf));
                        min_seen = min_seen.min(i64::from(len));
                        post_docs.insert(e.qualifier.to_vec(), tf);
                        *seen.entry(e.qualifier.to_vec()).or_default() += 1;
                        let f = fwd.get(e.qualifier);
                        if f.map(|f| (f.terms.get(&term).copied(), f.len)) != Some((Some(tf), len))
                        {
                            st.forward_mismatch += 1;
                        }
                    }
                    POS => {
                        decode_positions(e.cell.value(), &mut buf)?;
                        pos_docs.insert(e.qualifier.to_vec(), buf.len() as u32);
                    }
                    DF => df = e.cell.as_i64().unwrap_or(0),
                    MAXTF => maxtf = e.cell.as_i64().unwrap_or(i64::MIN),
                    MINLEN => minlen = e.cell.as_i64().unwrap_or(i64::MAX),
                    _ => {}
                }
            }
            let n = post_docs.len() as i64;
            st.postings += n as u64;
            if n > 0 {
                st.terms += 1;
            }
            if df != n {
                st.counter_drift += 1;
            }
            if n > 0 && (maxtf < max_seen || minlen > min_seen) {
                st.counter_drift += 1;
            }
            // Each posting has its positions, as many as its frequency, and no more.
            if inn.opts.get_positions() && pos_docs != post_docs {
                st.forward_mismatch += 1;
            }
        }
        // Each forward row's terms all have postings.
        for (doc, f) in &fwd {
            let have = seen.get(doc).copied().unwrap_or(0);
            if have != f.terms.len() {
                st.forward_mismatch += 1;
            }
        }
        // The corpus counters.
        let counter = |q: &[u8]| -> Result<i64> {
            Ok(t.get_at(&s, STATS_ROW, STATS, q)?
                .and_then(|c| c.as_i64())
                .unwrap_or(0))
        };
        if counter(DOCS)? != st.documents as i64 {
            st.counter_drift += 1;
        }
        let total: i64 = fwd
            .values()
            .filter(|f| f.present)
            .map(|f| i64::from(f.len))
            .sum();
        if counter(LEN)? != total {
            st.counter_drift += 1;
        }
        // The source column against the forward rows (and positions, by re-tokenising).
        self.verify_source(&s, &fwd, &mut st)?;
        Ok(st)
    }

    fn verify_source(
        &self,
        s: &Snapshot,
        fwd: &BTreeMap<Vec<u8>, Forward>,
        st: &mut IndexStats,
    ) -> Result<()> {
        let inn = &*self.inner;
        let q = inn.source.qualifier.as_slice();
        let mut sourced = BTreeSet::new();
        let mut buf = Vec::new();
        for row in inn
            .src
            .scan_prefix(b"")
            .family(&inn.source.family)
            .qualifier_bounds(std::ops::Bound::Included(q), std::ops::Bound::Included(q))
            .snapshot(s)
            .iter()?
        {
            let row = row?;
            let doc = row.key().to_vec();
            let Some(cell) = row.get(&inn.source.family, q) else {
                continue;
            };
            sourced.insert(doc.clone());
            let Ok(text) = std::str::from_utf8(cell.value()) else {
                st.invalid_utf8 += 1;
                continue;
            };
            let a = self.analyse(text);
            let Some(f) = fwd.get(&doc).filter(|f| f.present) else {
                st.source_mismatch += 1;
                continue;
            };
            let Ok(a) = a else {
                st.source_mismatch += 1;
                continue;
            };
            let same_terms = a.len == f.len
                && a.terms.len() == f.terms.len()
                && a.terms
                    .iter()
                    .all(|(t, ps)| f.terms.get(t.as_bytes()).copied() == Some(ps.len() as u32));
            if !same_terms {
                st.source_mismatch += 1;
                continue;
            }
            if inn.opts.get_positions() {
                for (term, ps) in &a.terms {
                    let stored = inn.table.get_at(s, &post_row(term), POS, &doc)?;
                    let ok = match stored {
                        Some(c) => decode_positions(c.value(), &mut buf).is_ok() && buf == *ps,
                        None => false,
                    };
                    if !ok {
                        st.source_mismatch += 1;
                        break;
                    }
                }
            }
        }
        st.source_mismatch += fwd
            .iter()
            .filter(|(d, f)| f.present && !sourced.contains(*d))
            .count() as u64;
        Ok(())
    }
}

fn read_u32(v: Option<u64>, what: &str) -> Result<u32> {
    v.and_then(|v| u32::try_from(v).ok())
        .ok_or_else(|| Error::Corruption(format!("{what} isn't a u32")))
}

/// `s` in the normalisation form `n`, borrowed when it's already in form (all ASCII is).
pub(crate) fn normalize(n: Normalization, s: &str) -> Cow<'_, str> {
    match n {
        Normalization::None => Cow::Borrowed(s),
        Normalization::Nfc => match is_nfc_quick(s.chars()) {
            IsNormalized::Yes => Cow::Borrowed(s),
            _ => Cow::Owned(s.nfc().collect()),
        },
        Normalization::Nfkc => match is_nfkc_quick(s.chars()) {
            IsNormalized::Yes => Cow::Borrowed(s),
            _ => Cow::Owned(s.nfkc().collect()),
        },
    }
}
