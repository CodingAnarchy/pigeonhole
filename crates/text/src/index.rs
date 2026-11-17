//! The index (D309 §2, §3): create and open, writes through the index, `rebuild`, `repair`
//! and `verify`.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Bound, ControlFlow};
use std::sync::{Arc, OnceLock};

use pigeonhole::{
    CellRef, Family, Pigeonhole, PigeonholeReader, ReadTable, RetryPolicy, RowRead, Scan, Snapshot,
    Table, Transaction,
};
use unicode_normalization::{IsNormalized, UnicodeNormalization, is_nfc_quick, is_nfkc_quick};

use crate::block::{Block, Builder, MAX_POSTINGS, Posting};
use crate::error::{Error, Result, corrupt};
use crate::layout::*;
use crate::merged::find_block;
use crate::query::Query;
use crate::search::Search;
use crate::tokenizer::{Standard, Tokenizer};

/// Documents one page of a rebuild reads under one snapshot before their transactions run
/// (#1514): its view lives for the page's scan only.
const REBUILD_PAGE: u64 = 256;

/// The most posting cells one chunk of a pack takes: one transaction (#1585).
const PACK_CHUNK: usize = 512;
/// The fewest postings a pack makes a block of where the term has no block near them: fewer
/// stay cells until more have gathered, so a term that grows a few postings between packs
/// isn't cut into blocks of a few.
const PACK_MIN_RUN: usize = 32;

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
    /// The read side: a writer's handles, or a reader process's ([`ReadIndex`]).
    pub(crate) db: Handle,
    pub(crate) table: Rows,
    pub(crate) src: Rows,
    /// The write side: only an [`Index`] has one.
    writer: Option<Writer>,
    pub(crate) source: Source,
    pub(crate) opts: IndexOptions,
    pub(crate) tok: Box<dyn Tokenizer>,
    /// The built-in tokeniser lowercases again after NFKC (D309 rev 6 §A.2).
    builtin: bool,
    /// `table` opened again, for a handle older than the index's first pack: a table handle
    /// knows the families of when it was opened, and the first pack adds [`POSTB`].
    fresh: OnceLock<Rows>,
}

/// An [`Index`]'s write handles.
struct Writer {
    db: Pigeonhole,
    table: Table,
    src: Table,
    /// `table` with the block family ([`Inner::fresh`]'s write side).
    fresh: OnceLock<Table>,
}

/// A database handle for reads: a writer's, or a reader process's.
pub(crate) enum Handle {
    W(Pigeonhole),
    R(PigeonholeReader),
}

impl Handle {
    pub(crate) fn snapshot(&self) -> pigeonhole::Result<Snapshot> {
        match self {
            Handle::W(d) => d.snapshot(),
            Handle::R(d) => d.snapshot(),
        }
    }
}

/// A table handle for reads: a writer's [`Table`], or a reader process's [`ReadTable`].
pub(crate) enum Rows {
    W(Table),
    R(ReadTable),
}

impl Rows {
    pub(crate) fn name(&self) -> &str {
        match self {
            Rows::W(t) => t.name(),
            Rows::R(t) => t.name(),
        }
    }

    pub(crate) fn family_info(&self, family: &str) -> Option<pigeonhole::FamilyInfo> {
        match self {
            Rows::W(t) => t.family_info(family),
            Rows::R(t) => t.family_info(family),
        }
    }

    pub(crate) fn get(
        &self,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> pigeonhole::Result<Option<CellRef<'_>>> {
        match self {
            Rows::W(t) => t.get(row, family, qualifier),
            Rows::R(t) => t.get(row, family, qualifier),
        }
    }

    pub(crate) fn get_at(
        &self,
        s: &Snapshot,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> pigeonhole::Result<Option<CellRef<'_>>> {
        match self {
            Rows::W(t) => t.get_at(s, row, family, qualifier),
            Rows::R(t) => t.get_at(s, row, family, qualifier),
        }
    }

    /// [`Rows::get_at`] as a future (#1560): a cache or memtable hit resolves at its first
    /// poll.
    #[cfg(feature = "async")]
    pub(crate) fn get_at_async(
        &self,
        s: &Snapshot,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> pigeonhole::nonblocking::GetFuture {
        match self {
            Rows::W(t) => t.get_at_async(s, row, family, qualifier),
            Rows::R(t) => t.get_at_async(s, row, family, qualifier),
        }
    }

    pub(crate) fn row(&self, row: &[u8]) -> RowRead<'_> {
        match self {
            Rows::W(t) => t.row(row),
            Rows::R(t) => t.row(row),
        }
    }

    pub(crate) fn scan_bounds(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Scan<'_> {
        match self {
            Rows::W(t) => t.scan_bounds(start, end),
            Rows::R(t) => t.scan_bounds(start, end),
        }
    }

    pub(crate) fn scan_prefix(&self, prefix: &[u8]) -> Scan<'_> {
        match self {
            Rows::W(t) => t.scan_prefix(prefix),
            Rows::R(t) => t.scan_prefix(prefix),
        }
    }
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
    /// Postings held in packed blocks (a version 2 index, [`Index::pack`]); the rest of
    /// `postings` are cells.
    pub packed: u64,
    /// Deleted postings not yet removed by a pack (a version 2 index). Information, not a
    /// mismatch: they cost a search a step each until the next pack.
    pub tombstones: u64,
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
    /// Whether the step of [`Index::rebuild_with`] or [`Index::resume_rebuild_with`] stopped
    /// the walk before its end (#1756). The index is consistent, and a later
    /// [`Index::resume_rebuild_with`] carries on.
    pub stopped: bool,
}

/// How [`Index::pack_with`] runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PackOptions {
    with_readers: bool,
}

impl PackOptions {
    /// The defaults: an index's first pack is refused while reader processes are attached.
    pub fn new() -> Self {
        Self::default()
    }

    /// Lets an index's first pack run while reader processes are attached to the database
    /// file. Default off.
    ///
    /// The first pack moves the index to layout version 2, which a 1.2 build can't read: one
    /// that has the index open goes on running and fails its searches with `Busy`. The
    /// database records that a reader is attached, not which build it is, so the pack
    /// refuses any ([`Error::PackRefused`]). Set this when every attached reader is 1.3 or
    /// later.
    pub fn with_readers(mut self, allow: bool) -> Self {
        self.with_readers = allow;
        self
    }
}

/// What [`Index::pack`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PackStats {
    /// Terms it took posting cells of.
    pub terms: u64,
    /// Postings it moved from cells into blocks.
    pub postings: u64,
    /// Deleted postings it removed (tombstones, with the packed postings they hid).
    pub tombstones: u64,
    /// Whether the step of [`Index::pack_with`] stopped it before the end. The index is
    /// consistent, and the next pack carries on from where it stopped.
    pub stopped: bool,
}

/// What a pack's chunk found to do.
struct PackWork {
    term: Vec<u8>,
    /// The term's posting cells past the cursor: id, timestamp, value.
    cells: Vec<(Vec<u8>, u64, u64)>,
    /// Their position cells: id to timestamp and value.
    positions: BTreeMap<Vec<u8>, (u64, Vec<u8>)>,
    /// The blocks their ids fall among, as read: qualifier and value.
    blocks: Vec<(Vec<u8>, Vec<u8>)>,
}

/// A posting a pack holds between reading and writing.
struct Packed {
    tf: u32,
    len: u32,
    positions: Vec<u8>,
}

/// Whether the index is version 2, from its state cell's value ([`REPAIR`]): `Busy` while a
/// repair runs.
pub(crate) fn packed_state(value: &[u8]) -> Result<bool> {
    if value == PACKED {
        Ok(true)
    } else {
        Err(Error::Busy)
    }
}

/// Which walk [`Index::rebuild_inner`] makes.
enum Walk {
    /// A rebuild, which keeps a cursor; from the row after `from`, if it resumes (#1756).
    Rebuild { from: Option<Vec<u8>> },
    /// A repair's, from an emptied table: no cursor, read or written.
    Repair,
}

/// How a page of a rebuild ended.
enum Paged {
    /// Its step stopped the walk.
    Stopped,
    /// Every document done; whether the last one's transaction wrote the cursor.
    Done { cursor_at_last: bool },
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
        Self::create_inner(db, name, source, opts, tok, true, true)
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
        Self::create_inner(db, name, source, options.checked()?, tokenizer, false, true)
    }

    fn create_inner(
        db: &Pigeonhole,
        name: &str,
        source: Source,
        opts: IndexOptions,
        tok: Box<dyn Tokenizer>,
        builtin: bool,
        fill: bool,
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
        let mut config = table
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
            );
        // Unfilled: a rebuild from the top is pending (the empty cursor), with the config,
        // and marked as the fill (#1722 PR 2c).
        if !fill {
            config = config
                .put(META, REBUILD_AFTER, b"")
                .put(META, UNFILLED, b"");
        }
        config.commit()?;
        let index = Index {
            inner: Arc::new(Inner {
                db: Handle::W(db.clone()),
                table: Rows::W(table.clone()),
                src: Rows::W(src.clone()),
                writer: Some(Writer {
                    db: db.clone(),
                    table,
                    src,
                    fresh: OnceLock::new(),
                }),
                source,
                opts,
                tok,
                builtin,
                fresh: OnceLock::new(),
            }),
        };
        if fill {
            index.rebuild_inner(Walk::Rebuild { from: None }, |_| ControlFlow::Continue(()))?;
        }
        Ok(index)
    }

    /// The text indexes in `db` whose source column is in the table `table` (#1916): the
    /// indexes dropping that table would leave behind, unable to write, rebuild or find
    /// their documents. Sorted. A table without the index's families is skipped without a
    /// read; one with them is read for its source, whatever its tokeniser.
    pub fn over_table(db: &Pigeonhole, table: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for name in db.tables() {
            if name == table {
                continue;
            }
            // A table dropped since the listing has nothing to leave behind.
            let rows = match db.table(&name).and_then(|t| t.open()) {
                Ok(t) => Rows::W(t),
                Err(e) if e.code() == pigeonhole::ErrorCode::TableNotFound => continue,
                Err(e) => return Err(e.into()),
            };
            if [POST, DF, TERMS, META, STATS]
                .iter()
                .any(|f| rows.family_info(f).is_none())
            {
                continue;
            }
            let found = rows
                .get(GLOBAL, META, SOURCE)?
                .and_then(|c| Source::decode(c.value()));
            if found.is_some_and(|s| s.table == table) {
                out.push(name);
            }
        }
        out.sort();
        Ok(out)
    }

    /// [`create`](Self::create) without the backfill (#1722): the index starts empty, with a
    /// rebuild from the top pending ([`rebuild_pending`](Self::rebuild_pending) is true), which
    /// [`resume_rebuild_with`](Self::resume_rebuild_with) then runs in steps. Writes through
    /// the index meanwhile are indexed at once. For a server that fills an index in the
    /// background, a document at a time, rather than inside the create (a cluster's
    /// partitions create their indexes when they take the map). Until the rebuild finishes,
    /// the index lacks the source rows written before the create.
    pub fn create_unfilled(
        db: &Pigeonhole,
        name: &str,
        source: Source,
        options: IndexOptions,
    ) -> Result<Index> {
        let opts = options.checked()?;
        let tok = Box::new(Standard {
            stop_words: opts.get_stop_words(),
        });
        Self::create_inner(db, name, source, opts, tok, true, false)
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
        let rows = Rows::W(table.clone());
        let (source, opts, tok, builtin) = load(&rows, name, custom)?;
        let src = db.table(&source.table)?.open()?;
        Ok(Index {
            inner: Arc::new(Inner {
                db: Handle::W(db.clone()),
                table: rows,
                src: Rows::W(src.clone()),
                writer: Some(Writer {
                    db: db.clone(),
                    table,
                    src,
                    fresh: OnceLock::new(),
                }),
                source,
                opts,
                tok,
                builtin,
                fresh: OnceLock::new(),
            }),
        })
    }

    /// Opens the index `name` from a reader process, with the built-in tokeniser: a
    /// [`ReadIndex`], which searches and verifies but doesn't write. `Mismatch` as for
    /// [`open`](Self::open).
    pub fn open_reader(reader: &PigeonholeReader, name: &str) -> Result<ReadIndex> {
        ReadIndex::open_inner(reader, name, None)
    }

    /// [`open_reader`](Self::open_reader) with the custom tokeniser the index was created
    /// with.
    pub fn open_reader_with_tokenizer(
        reader: &PigeonholeReader,
        name: &str,
        tokenizer: Box<dyn Tokenizer>,
    ) -> Result<ReadIndex> {
        ReadIndex::open_inner(reader, name, Some(tokenizer))
    }

    /// The write handles: an [`Index`] is always built with them.
    fn w(&self) -> &Writer {
        self.inner
            .writer
            .as_ref()
            .expect("an Index is built with its write handles")
    }

    /// The index's table for reads: with the block family in a version 2 index, which a
    /// handle opened before the first pack gets by opening the table again, once.
    #[inline]
    pub(crate) fn rows(&self, packed: bool) -> Result<&Rows> {
        if !packed {
            return Ok(&self.inner.table);
        }
        self.rows_packed()
    }

    /// [`rows`](Self::rows) for a version 2 index, out of line.
    #[inline(never)]
    fn rows_packed(&self) -> Result<&Rows> {
        let inn = &*self.inner;
        if inn.table.family_info(POSTB).is_some() {
            return Ok(&inn.table);
        }
        if let Some(rows) = inn.fresh.get() {
            return Ok(rows);
        }
        let rows = match &inn.db {
            Handle::W(db) => Rows::W(db.table(inn.table.name())?.open()?),
            Handle::R(db) => Rows::R(db.table(inn.table.name())?),
        };
        if rows.family_info(POSTB).is_none() {
            return Err(corrupt("a version 2 index has no block family"));
        }
        Ok(inn.fresh.get_or_init(|| rows))
    }

    /// The index's table for writes of blocks, and reads of them in a transaction: the block
    /// family is added if the table lacks it (a pack's first step).
    fn block_table(&self) -> Result<&Table> {
        let w = self.w();
        if w.table.family_info(POSTB).is_some() {
            return Ok(&w.table);
        }
        if let Some(t) = w.fresh.get() {
            return Ok(t);
        }
        let t =
            w.db.table(w.table.name())?
                .family(POSTB, Family::default().max_versions(1))
                .open()?;
        Ok(w.fresh.get_or_init(|| t))
    }

    /// The index's options.
    pub fn options(&self) -> IndexOptions {
        self.inner.opts
    }

    /// The column the index is built over.
    pub fn source(&self) -> &Source {
        &self.inner.source
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
    /// written. It fails with `Busy` while a [`repair`](Self::repair) runs: retry after it
    /// finishes.
    pub fn put_in(&self, txn: &mut Transaction, doc: &[u8], text: &str) -> Result<()> {
        let packed = self.state_in(txn)?;
        let inn = &*self.inner;
        let w = self.w();
        let stored = txn
            .get(&w.src, doc, &inn.source.family, &inn.source.qualifier)?
            .map(|c| c.value() == text.as_bytes());
        let old = self.read_forward(txn, doc)?;
        if old.present && stored == Some(true) {
            return Ok(());
        }
        let new = self.analyse(text)?;
        txn.put(
            &w.src,
            doc,
            &inn.source.family,
            &inn.source.qualifier,
            text.as_bytes(),
        );
        self.apply(txn, doc, &old, &new, Mode::Put, packed)?;
        Ok(())
    }

    /// Deletes the document `doc`: its source cell and its index rows, inside the caller's
    /// transaction. `false` (and no counter changes) if the index didn't hold it; its source
    /// cell is deleted either way. It fails with `Busy` while a [`repair`](Self::repair) runs:
    /// retry after it finishes.
    pub fn delete_in(&self, txn: &mut Transaction, doc: &[u8]) -> Result<bool> {
        let packed = self.state_in(txn)?;
        let inn = &*self.inner;
        let w = self.w();
        let old = self.read_forward(txn, doc)?;
        txn.delete_column(&w.src, doc, &inn.source.family, &inn.source.qualifier);
        if !old.present {
            return Ok(false);
        }
        // Deletes only: `apply` would also write `F{doc}`'s cells, which the row delete
        // below would meet at the same seqno.
        let t = &w.table;
        for term in old.terms.keys() {
            let row = [POST_PREFIX, term].concat();
            Self::unpost(txn, t, &row, doc, inn.opts.get_positions(), packed);
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
        self.w()
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
        self.w()
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
    /// emptied table. It fails with `Busy` while a repair runs.
    ///
    /// It starts from the first source row, and drops the cursor a stopped
    /// [`rebuild_with`](Self::rebuild_with) left: it covers every source row as of the call.
    pub fn rebuild(&self) -> Result<RebuildStats> {
        self.rebuild_with(|_| ControlFlow::Continue(()))
    }

    /// [`rebuild`](Self::rebuild), calling `step` after each document's transaction with the
    /// counts so far (#1756). A `Break` stops the walk there: the index is consistent (each
    /// document is its own transaction), the result has `stopped` set, and
    /// [`resume_rebuild_with`](Self::resume_rebuild_with) carries on from the cursor the walk
    /// keeps in the index. The cursor is written once a page, and in the transaction of any
    /// document that writes anyway, so a document already right still writes nothing, and a
    /// resume redoes at most one page of documents. A walk costs two commits more: the empty
    /// cursor at its start, and the cursor's removal at its end.
    ///
    /// Like `rebuild`, it starts from the first source row, replacing any earlier cursor with
    /// an empty one ("from the top") in its first commit: a stop or a crash before its first
    /// page's cursor still leaves it pending.
    pub fn rebuild_with(
        &self,
        step: impl FnMut(&RebuildStats) -> ControlFlow<()>,
    ) -> Result<RebuildStats> {
        // A stopped pack's cursor goes too: the rebuild writes cells behind it, and the next
        // pack starts over (#1585).
        self.w()
            .table
            .mutate(GLOBAL)
            .put(META, REBUILD_AFTER, b"")
            .delete_column(META, PACK_AFTER)
            .commit()?;
        self.rebuild_inner(Walk::Rebuild { from: None }, step)
    }

    /// Carries on a rebuild that [`rebuild_with`](Self::rebuild_with)'s step stopped, after
    /// the last row its cursor names (#1756); nothing if none is pending. It ends as that
    /// rebuild would have: documents before the cursor were reconciled when it visited them,
    /// and writes through the index have kept them so since. A write around the index to a
    /// row before the cursor is missed, as it would be by one rebuild that had passed that
    /// row: call [`rebuild`](Self::rebuild) (which starts over) or [`repair`](Self::repair)
    /// after such writes, never this.
    pub fn resume_rebuild_with(
        &self,
        step: impl FnMut(&RebuildStats) -> ControlFlow<()>,
    ) -> Result<RebuildStats> {
        let Some(from) = self.rebuild_cursor()? else {
            return Ok(RebuildStats::default());
        };
        // An empty cursor: stopped before its first page's, so from the top.
        let from = (!from.is_empty()).then_some(from);
        self.rebuild_inner(Walk::Rebuild { from }, step)
    }

    /// The text indexes in `db`, by name (#1585): for a server that packs them in the
    /// background. A catalog listing: each table with the index's families, none of them
    /// opened as an index. A table dropped since the catalog was listed is passed over.
    pub fn list(db: &Pigeonhole) -> Result<Vec<String>> {
        Ok(Self::index_tables(db)?
            .into_iter()
            .map(|(n, _)| n)
            .collect())
    }

    /// The tables of `db` that have a text index's families, with their names.
    fn index_tables(db: &Pigeonhole) -> Result<Vec<(String, Table)>> {
        let mut out = Vec::new();
        for name in db.tables() {
            let t = match db.table(&name).and_then(|b| b.open()) {
                Ok(t) => t,
                Err(e) if e.code() == pigeonhole::ErrorCode::TableNotFound => continue,
                Err(e) => return Err(e.into()),
            };
            let families = t.families();
            if [POST, DF, TERMS, META, STATS]
                .iter()
                .all(|f| families.contains(f))
            {
                out.push((name, t));
            }
        }
        Ok(out)
    }

    /// The text indexes in `db` with a stopped rebuild pending (#1756): for a server to
    /// resume them. A point read per table with the index's families; a table dropped since
    /// the catalog was listed is passed over.
    pub fn pending_rebuilds(db: &Pigeonhole) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for (name, t) in Self::index_tables(db)? {
            if t.get(GLOBAL, META, REBUILD_AFTER)?.is_some() {
                out.push(name);
            }
        }
        Ok(out)
    }

    /// Whether a stopped rebuild is pending (a cursor is kept): one point read.
    pub fn rebuild_pending(&self) -> Result<bool> {
        Ok(self.rebuild_cursor()?.is_some())
    }

    /// Whether the index was created unfilled ([`create_unfilled`](Self::create_unfilled))
    /// and no walk of its whole source has finished since (#1722 PR 2c): one point read.
    /// Until then it lacks source rows written before its create, so a search's answer is
    /// short. [`rebuild_pending`](Self::rebuild_pending) is true then too, but also while
    /// a client's [`rebuild`](Self::rebuild) runs or is stopped part way, when every
    /// document is indexed, some perhaps with postings the rebuild hasn't reconciled yet.
    pub fn fill_pending(&self) -> Result<bool> {
        Ok(self.inner.table.get(GLOBAL, META, UNFILLED)?.is_some())
    }

    fn rebuild_cursor(&self) -> Result<Option<Vec<u8>>> {
        Ok(self
            .w()
            .table
            .get(GLOBAL, META, REBUILD_AFTER)?
            .map(|c| c.value().to_vec()))
    }

    /// Packs the index's postings into blocks (#1585, ICR 0143): each term's posting cells,
    /// one a document, are moved into blocks of up to 128 postings under them, which a
    /// search of a common word reads several times faster. Searches and writes go on while
    /// it runs, and see the same postings throughout.
    ///
    /// **Nothing else packs an index.** The first pack moves the index to layout version 2,
    /// which a 1.2 build can't open: pack only once you won't roll back to 1.2. A 1.2
    /// build that already has the index open fails its searches and writes with `Busy`
    /// from then on. So the first pack is refused ([`Error::PackRefused`]) while reader
    /// processes are attached to the file, unless [`PackOptions::with_readers`] allows it.
    ///
    /// **What it does.** Chunk by chunk (a transaction each), it takes a term's posting
    /// cells, merges them with the blocks their ids fall among, writes the blocks again, and
    /// removes the cells it read (each only if it is still as read, so a write that lands
    /// meanwhile stays, and wins). Deleted postings ([`IndexStats::tombstones`]) are removed
    /// with what they hid. A few postings with no block near them stay cells until more have
    /// gathered. Writes after a pack are cells again, until the next one.
    ///
    /// It carries on from where a stopped pack left off ([`pack_with`](Self::pack_with)),
    /// and otherwise starts at the first term. `PackRefused` while a rebuild is pending
    /// ([`rebuild_pending`](Self::rebuild_pending)): finish the rebuild first. `Busy` while
    /// a repair runs.
    pub fn pack(&self) -> Result<PackStats> {
        self.pack_with(PackOptions::new(), |_| ControlFlow::Continue(()))
    }

    /// [`pack`](Self::pack) with `options`, calling `step` after each chunk with the counts
    /// so far. A `Break` stops it there: the index is consistent, the result has `stopped`
    /// set, [`pack_pending`](Self::pack_pending) is true, and the next pack carries on from
    /// the cursor it keeps in the index.
    pub fn pack_with(
        &self,
        options: PackOptions,
        mut step: impl FnMut(&PackStats) -> ControlFlow<()>,
    ) -> Result<PackStats> {
        self.pack_begin(options)?;
        let mut stats = PackStats::default();
        let (mut term, mut at) = (None, None);
        while self.pack_chunk(&mut stats, &mut term, &mut at)? {
            if step(&stats).is_break() {
                stats.stopped = true;
                break;
            }
        }
        Ok(stats)
    }

    /// Whether a stopped pack is pending (a cursor is kept): one point read.
    pub fn pack_pending(&self) -> Result<bool> {
        Ok(self.w().table.get(GLOBAL, META, PACK_AFTER)?.is_some())
    }

    /// A pack's first commit: the index becomes version 2 if it wasn't, and a pass's cursor
    /// is put (empty: from the top) unless a stopped pack left one.
    fn pack_begin(&self, options: PackOptions) -> Result<()> {
        let w = self.w();
        if w.table.get(GLOBAL, META, REPAIR)?.is_none() && !options.with_readers {
            // Version 1 still. The check and the commit below aren't one step: a reader that
            // attaches between them isn't seen (the guide's rule covers that, not this).
            let readers = w.db.engine_metrics().reader_slots_active;
            if readers > 0 {
                return Err(Error::PackRefused(format!(
                    "{readers} reader process(es) are attached to the database file, and the \
                     first pack moves the index to a layout a 1.2 build can't read. Close \
                     them (a reader that exited without closing holds its slot until the \
                     writer reclaims it), or allow it with PackOptions::with_readers once \
                     every reader is 1.3 or later"
                )));
            }
        }
        // The block family, before anything can name it.
        self.block_table()?;
        let t = &w.table;
        w.db.transact(&RetryPolicy::default(), |tx| {
            if tx.get(t, GLOBAL, META, REBUILD_AFTER)?.is_some() {
                return Err(Tx::Fail(Error::PackRefused(
                    "a rebuild is pending: finish it first".into(),
                )));
            }
            if !self.state_in(tx)? {
                tx.put_u64(t, GLOBAL, META, LAYOUT, LAYOUT_PACKED)
                    .put(t, GLOBAL, META, REPAIR, PACKED);
            }
            if tx.get(t, GLOBAL, META, PACK_AFTER)?.is_none() {
                tx.put(t, GLOBAL, META, PACK_AFTER, b"");
            }
            Ok(())
        })?;
        Ok(())
    }

    /// One chunk of a pack: `false` once the pass is done (or another pack finished it).
    /// `term` is the term the last chunk was in, for the count of terms.
    ///
    /// `at` is how far the pass has read past the stored cursor (with that cursor): chunks
    /// with nothing to write, which are most terms of a real vocabulary (a few postings
    /// each, left as cells), commit nothing, the cursor included. A pass that stops there
    /// reads them again. After 64 such chunks it returns, so the caller's step still runs.
    fn pack_chunk(
        &self,
        stats: &mut PackStats,
        term: &mut Option<Vec<u8>>,
        at: &mut Option<(Vec<u8>, Vec<u8>)>,
    ) -> Result<bool> {
        let w = self.w();
        let t = self.block_table()?;
        let rows = self.rows(true)?;
        let positions = self.inner.opts.get_positions();
        // The most cells the chunk takes: cut when its transaction runs out of retries, so
        // a pack beside steady writers of the same documents still gets through.
        let mut limit = PACK_CHUNK;
        let mut idle = 0;
        loop {
            let Some(cursor) = t.get(GLOBAL, META, PACK_AFTER)?.map(|c| c.value().to_vec()) else {
                return Ok(false);
            };
            let from = match at {
                Some((stored, read)) if *stored == cursor => read.clone(),
                _ => cursor.clone(),
            };
            // The chunk's cells and blocks at one snapshot, dropped before the transaction
            // (#1514).
            let work = {
                let s = w.db.snapshot()?;
                self.pack_read(rows, &s, &from, limit)?
            };
            let Some(work) = work else {
                // The pass is done: the cursor goes, unless it moved (another pack's).
                w.db.transact(&RetryPolicy::default(), |tx| {
                    let same = tx
                        .get(t, GLOBAL, META, PACK_AFTER)?
                        .is_some_and(|c| c.value() == cursor.as_slice());
                    if same {
                        tx.delete_column(t, GLOBAL, META, PACK_AFTER);
                    }
                    Ok::<_, Tx>(())
                })?;
                return Ok(false);
            };
            let plan = pack_plan(&work, positions, limit)?;
            let row = [POST_PREFIX, &work.term[..]].concat();
            let last = &work.cells.last().expect("a chunk has cells").0;
            let next = pack_cursor(&work.term, last);
            if plan.posts.is_empty() && plan.positions.is_empty() && plan.blocks == work.blocks {
                *at = Some((cursor, next));
                idle += 1;
                if idle == 64 {
                    return Ok(true);
                }
                continue;
            }
            let done = w.db.transact(&RetryPolicy::default(), |tx| {
                if !self.state_in(tx)? {
                    return Err(Tx::Fail(corrupt("a pack met a version 1 index")));
                }
                if tx.get(t, GLOBAL, META, REBUILD_AFTER)?.is_some() {
                    return Err(Tx::Fail(Error::PackRefused(
                        "a rebuild is pending: finish it first".into(),
                    )));
                }
                // The cursor and the blocks read are read again here (ai1249's C1): only a
                // pack and a rebuild's reset write them, so two packs, or a pack and a
                // reset, can't leave overlapping blocks. An ordinary write never conflicts
                // on them.
                let same = tx
                    .get(t, GLOBAL, META, PACK_AFTER)?
                    .is_some_and(|c| c.value() == cursor.as_slice());
                if !same {
                    return Ok(false);
                }
                for (q, v) in &work.blocks {
                    let same = tx
                        .get(t, &row, POSTB, q)?
                        .is_some_and(|c| c.value() == v.as_slice());
                    if !same {
                        return Ok(false);
                    }
                }
                for (q, _) in &work.blocks {
                    if !plan.blocks.iter().any(|(nq, _)| nq == q) {
                        tx.delete_column(t, &row, POSTB, q);
                    }
                }
                for (q, v) in &plan.blocks {
                    if !work.blocks.iter().any(|(oq, ov)| oq == q && ov == v) {
                        tx.put(t, &row, POSTB, q, v);
                    }
                }
                // Each cell is read again here and deleted only if it is still the version
                // the chunk read. One written since stays, and hides the block's posting,
                // which is then the older one; one written before this commits makes it
                // conflict and run again. (Not a delete of the version read, by timestamp:
                // that uncovers the column's older versions, a posting the tombstone or the
                // overwrite had replaced.)
                for (family, cells) in [(POST, &plan.posts), (POS, &plan.positions)] {
                    for (id, ts) in cells {
                        let same = tx
                            .get(t, &row, family, id)?
                            .is_some_and(|c| c.timestamp() == *ts);
                        if same {
                            tx.delete_column(t, &row, family, id);
                        }
                    }
                }
                tx.put(t, GLOBAL, META, PACK_AFTER, &next);
                Ok::<_, Tx>(true)
            });
            let done = match done.map_err(Error::from) {
                Ok((done, _)) => done,
                Err(e) if e.code() == pigeonhole::ErrorCode::Conflict && limit > 8 => {
                    limit = (limit / 8).max(8);
                    continue;
                }
                Err(e) => return Err(e),
            };
            *at = None;
            if done {
                if term.as_deref() != Some(&work.term[..]) {
                    stats.terms += 1;
                    *term = Some(work.term);
                }
                stats.postings += plan.packed;
                stats.tombstones += plan.tombstones;
                return Ok(true);
            }
        }
    }

    /// The next chunk after `cursor`, at `s`: the next term with posting cells, up to
    /// `limit` of them, their position cells, and the blocks their ids fall among.
    /// `None` when no posting cell is left.
    fn pack_read(
        &self,
        rows: &Rows,
        s: &Snapshot,
        cursor: &[u8],
        limit: usize,
    ) -> Result<Option<PackWork>> {
        let cells_of = |term: &[u8], after: Option<&[u8]>| -> Result<Vec<(Vec<u8>, u64, u64)>> {
            let row = [POST_PREFIX, term].concat();
            let start = after.map_or(Bound::Unbounded, Bound::Excluded);
            let mut cells = rows
                .row(&row)
                .family(POST)
                .qualifier_bounds(start, Bound::Unbounded)
                .snapshot(s)
                .cells()?;
            let mut out = Vec::new();
            while out.len() < limit {
                let Some(e) = cells.next()? else {
                    break;
                };
                let v = e
                    .cell
                    .as_u64()
                    .ok_or_else(|| corrupt("a posting isn't a u64"))?;
                out.push((e.qualifier.to_vec(), e.cell.timestamp(), v));
            }
            Ok(out)
        };
        let at = unpack_cursor(cursor)?;
        let mut found = None;
        if let Some((term, doc)) = at {
            let cells = cells_of(term, Some(doc))?;
            if !cells.is_empty() {
                found = Some((term.to_vec(), cells));
            }
        }
        if found.is_none() {
            // The next term's row with a posting cell.
            let after = at.map(|(term, _)| [POST_PREFIX, term].concat());
            let start = match &after {
                Some(row) => Bound::Excluded(row.as_slice()),
                None => Bound::Included(POST_PREFIX),
            };
            let end = [POST_PREFIX[0] + 1];
            let next = {
                let mut scan = rows
                    .scan_bounds(start, Bound::Excluded(&end))
                    .family(POST)
                    .snapshot(s)
                    .limit(1)
                    .iter()?;
                match scan.next() {
                    Some(row) => Some(row?.key()[POST_PREFIX.len()..].to_vec()),
                    None => None,
                }
            };
            let Some(term) = next else {
                return Ok(None);
            };
            let cells = cells_of(&term, None)?;
            if cells.is_empty() {
                return Err(corrupt(
                    "a term's row lost its posting cells within a snapshot",
                ));
            }
            found = Some((term, cells));
        }
        let (term, cells) = found.expect("set above");
        let row = [POST_PREFIX, &term[..]].concat();
        let (first, last) = (&cells[0].0, &cells[cells.len() - 1].0);
        let mut positions = BTreeMap::new();
        if self.inner.opts.get_positions() {
            let mut read = rows
                .row(&row)
                .family(POS)
                .qualifier_bounds(Bound::Included(first), Bound::Included(last))
                .snapshot(s)
                .cells()?;
            while let Some(e) = read.next()? {
                positions.insert(
                    e.qualifier.to_vec(),
                    (e.cell.timestamp(), e.cell.value().to_vec()),
                );
            }
        }
        // A block is named by its last id: from the first one that can hold `first`, while
        // a block starts at or before `last`.
        let mut blocks = Vec::new();
        let mut block = Block::default();
        let mut read = rows
            .row(&row)
            .family(POSTB)
            .qualifier_bounds(Bound::Included(first), Bound::Unbounded)
            .snapshot(s)
            .cells()?;
        while let Some(e) = read.next()? {
            block.decode(e.cell.value())?;
            if block.id(0) > last.as_slice() {
                break;
            }
            blocks.push((e.qualifier.to_vec(), e.cell.value().to_vec()));
        }
        Ok(Some(PackWork {
            term,
            cells,
            positions,
            blocks,
        }))
    }

    /// Rebuilds the index from an emptied table (D309 §3, M2): drops every index row, then
    /// [`rebuild`](Self::rebuild)s. Writes through the index and searches of it fail
    /// with `Busy` until it finishes. If it's interrupted (a crash), the index stays `Busy` until
    /// `repair` is run again.
    pub fn repair(&self) -> Result<RebuildStats> {
        let w = self.w();
        let t = &w.table;
        // The flag first: every write in flight read it, so they conflict and retry into Busy.
        // A stopped rebuild's cursor goes with it: the repair walks the whole source, and a
        // resume after it would skip rows of the emptied index (ai1249 on #1756).
        // A version 2 index stays one: its emptied rows have no blocks, and its state cell
        // says so again at the end. A pack's cursor goes with the rebuild's.
        let packed = t.get(GLOBAL, META, LAYOUT)?.and_then(|c| c.as_u64()) == Some(LAYOUT_PACKED);
        t.mutate(GLOBAL)
            .put(META, REPAIR, b"")
            .delete_column(META, REBUILD_AFTER)
            .delete_column(META, PACK_AFTER)
            .commit()?;
        for prefix in [POST_PREFIX, FWD_PREFIX, STATS_ROW] {
            // The keys first, under a snapshot and an iterator dropped before anything below
            // commits (#1514): held across this thread's own commits, they would pin the
            // memtables a commit waiting for room needs, until the write-stall timeout. With
            // the repair flag set, no write lands meanwhile.
            let keys = {
                let s = w.db.snapshot()?;
                let mut keys = Vec::new();
                for row in t.scan_prefix(prefix).snapshot(&s).iter()? {
                    keys.push(row?.key().to_vec());
                }
                keys
            };
            for chunk in keys.chunks(256) {
                let mut batch = w.db.write_batch();
                for k in chunk {
                    batch.delete_row(t, k);
                }
                batch.commit()?;
            }
        }
        let stats = self.rebuild_inner(Walk::Repair, |_| ControlFlow::Continue(()))?;
        if packed {
            t.mutate(GLOBAL).put(META, REPAIR, PACKED).commit()?;
        } else {
            t.mutate(GLOBAL).delete_column(META, REPAIR).commit()?;
        }
        Ok(stats)
    }

    /// The walk over the source: a rebuild's, which keeps a cursor (from `from`, if it
    /// resumes), or a repair's, which keeps none (`rebuild_inner`).
    fn rebuild_inner(
        &self,
        walk: Walk,
        mut step: impl FnMut(&RebuildStats) -> ControlFlow<()>,
    ) -> Result<RebuildStats> {
        let inn = &*self.inner;
        let w = self.w();
        let mut stats = RebuildStats::default();
        let q = inn.source.qualifier.as_slice();
        let (repairing, mut after) = match walk {
            Walk::Repair => (true, None),
            Walk::Rebuild { from } => (false, from),
        };
        loop {
            // A page of documents, under a snapshot and an iterator dropped before the page's
            // transactions commit (#1514): held across them, they would pin the memtables a
            // commit waiting for room needs, until the write-stall timeout. Each transaction
            // reads the document's current text itself, so the snapshot only chooses which
            // documents to visit.
            let page: Vec<Vec<u8>> = {
                let s = w.db.snapshot()?;
                let start = after.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
                let mut page = Vec::new();
                for row in inn
                    .src
                    .scan_bounds(start, Bound::Unbounded)
                    .family(&inn.source.family)
                    .qualifier_bounds(Bound::Included(q), Bound::Included(q))
                    .snapshot(&s)
                    .limit(REBUILD_PAGE)
                    .iter()?
                {
                    page.push(row?.key().to_vec());
                }
                page
            };
            let Some(last) = page.last().cloned() else {
                return self.rebuilt(repairing, stats);
            };
            let full = page.len() as u64 == REBUILD_PAGE;
            let cursor_at_last = match self.rebuild_page(&page, repairing, &mut stats, &mut step)? {
                Paged::Stopped => {
                    stats.stopped = true;
                    return Ok(stats);
                }
                Paged::Done { cursor_at_last } => cursor_at_last,
            };
            if !full {
                return self.rebuilt(repairing, stats);
            }
            // The page's cursor, unless its last document's transaction wrote it: a resume
            // redoes at most the next page (ai1249's Q2 on #1756).
            if !repairing && !cursor_at_last {
                w.table
                    .mutate(GLOBAL)
                    .put(META, REBUILD_AFTER, &last)
                    .commit()?;
            }
            after = Some(last);
        }
    }

    /// A walk that reached the source's end: a rebuild's cursor goes, and so does an
    /// unfilled index's mark (#1722 PR 2c), a repair's too: every source row has been
    /// visited.
    fn rebuilt(&self, repairing: bool, stats: RebuildStats) -> Result<RebuildStats> {
        let cursor = !repairing && self.rebuild_pending()?;
        let unfilled = self.fill_pending()?;
        if cursor || unfilled {
            let mut m = self.w().table.mutate(GLOBAL);
            if cursor {
                m = m.delete_column(META, REBUILD_AFTER);
            }
            if unfilled {
                m = m.delete_column(META, UNFILLED);
            }
            m.commit()?;
        }
        Ok(stats)
    }

    /// Re-indexes each of `docs`, one transaction each (`rebuild_inner`'s page), calling
    /// `step` after each. A rebuild's document that writes anyway also writes the cursor.
    fn rebuild_page(
        &self,
        docs: &[Vec<u8>],
        repairing: bool,
        stats: &mut RebuildStats,
        step: &mut impl FnMut(&RebuildStats) -> ControlFlow<()>,
    ) -> Result<Paged> {
        let inn = &*self.inner;
        let w = self.w();
        let q = inn.source.qualifier.as_slice();
        let mut cursor_at_last = false;
        for doc in docs {
            stats.documents += 1;
            let ((changed, bad), _) = w.db.transact(&RetryPolicy::default(), |tx| {
                // A repair's rows were emptied, blocks included: it writes as version 1 does.
                let packed = !repairing && self.state_in(tx)?;
                // The current text, not the snapshot's: a put since the scan wins.
                let text = match tx.get(&w.src, doc, &inn.source.family, q)? {
                    None => return Ok((false, false)),
                    Some(c) => match std::str::from_utf8(c.value()) {
                        Ok(t) => t.to_owned(),
                        Err(_) => return Ok((false, true)),
                    },
                };
                let old = self.read_forward(tx, doc)?;
                let new = self.analyse(&text)?;
                let changed = self.apply(tx, doc, &old, &new, Mode::Rebuild, packed)?;
                if changed && !repairing {
                    tx.put(&w.table, GLOBAL, META, REBUILD_AFTER, doc);
                }
                Ok::<_, Tx>((changed, false))
            })?;
            stats.changed += u64::from(changed);
            stats.invalid_utf8 += u64::from(bad);
            cursor_at_last = changed && !repairing;
            if step(stats).is_break() {
                return Ok(Paged::Stopped);
            }
        }
        Ok(Paged::Done { cursor_at_last })
    }

    /// The index's state, read in the transaction ([`REPAIR`]): whether it is version 2, or
    /// `Busy` while a repair runs. Every write through the index reads it, so a repair's
    /// start and an index's first pack both make the writes in flight conflict and run
    /// again under the new state.
    fn state_in(&self, txn: &mut Transaction) -> Result<bool> {
        match txn.get(&self.w().table, GLOBAL, META, REPAIR)? {
            Some(c) => packed_state(c.value()),
            None => Ok(false),
        }
    }

    /// Removes `doc`'s posting from `row`. In a version 1 index its cells are deleted. In a
    /// version 2 index they are overwritten with tombstones, since a packed block may hold
    /// the posting too: a blind write, as the delete is (the writer doesn't look).
    fn unpost(
        txn: &mut Transaction,
        t: &Table,
        row: &[u8],
        doc: &[u8],
        positions: bool,
        packed: bool,
    ) {
        if packed {
            txn.put_u64(t, row, POST, doc, TOMBSTONE);
            if positions {
                txn.put(t, row, POS, doc, b"");
            }
        } else {
            txn.delete_column(t, row, POST, doc);
            if positions {
                txn.delete_column(t, row, POS, doc);
            }
        }
    }

    /// `doc`'s posting in `row`'s packed blocks, for a rebuild's compare: its value and its
    /// encoded positions. The block is found by a read outside the transaction (a
    /// transaction reads single cells) and then read inside it: `None` unless the two agree,
    /// so a pack that rewrites the block before this transaction commits makes it conflict,
    /// and one that already has makes the rebuild write the posting as a cell, which is
    /// right either way.
    fn stored_in_block(
        &self,
        tx: &mut Transaction,
        row: &[u8],
        doc: &[u8],
        block: &mut Block,
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let Some(found) = find_block(self.rows(true)?, None, row, doc, block)? else {
            return Ok(None);
        };
        let Some(at) = found.at else {
            return Ok(None);
        };
        let same = tx
            .get(self.block_table()?, row, POSTB, &found.qualifier)?
            .is_some_and(|c| c.value() == found.value.as_slice());
        if !same {
            return Ok(None);
        }
        let (tf, len) = block.posting(at);
        Ok(Some((posting(tf, len), block.positions(at).to_vec())))
    }

    fn read_forward(&self, txn: &mut Transaction, doc: &[u8]) -> Result<Forward> {
        let mut f = Forward::default();
        let Some(row) = txn.get_row(&self.w().table, &fwd_row(doc), &[TERMS, META])? else {
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
        packed: bool,
    ) -> Result<bool> {
        let inn = &*self.inner;
        let w = self.w();
        let t = &w.table;
        let positions = inn.opts.get_positions();
        let fwd = fwd_row(doc);
        let mut changed = false;
        let mut buf = Vec::new();
        let mut block = Block::default();
        let len_changed = !old.present || old.len != new.len;
        for (term, ps) in &new.terms {
            let row = post_row(term);
            let tf = u32::try_from(ps.len()).unwrap_or(u32::MAX);
            let old_tf = old.terms.get(term.as_bytes()).copied();
            let value = posting(tf, new.len);
            let enc = positions.then(|| encode_positions(ps));
            let (mut write_post, write_pos) = match mode {
                Mode::Put => (old_tf != Some(tf) || len_changed, positions),
                Mode::Rebuild => {
                    let post = tx.get(t, &row, POST, doc)?.map(|c| c.as_u64());
                    let pos = match positions {
                        true => tx.get(t, &row, POS, doc)?.map(|c| c.value().to_vec()),
                        false => None,
                    };
                    // What the cells don't say, the blocks may (a version 2 index): a
                    // posting that is right there is left there.
                    let below = match packed && (post.is_none() || (positions && pos.is_none())) {
                        true => self.stored_in_block(tx, &row, doc, &mut block)?,
                        false => None,
                    };
                    let post = match post {
                        Some(v) => v,
                        None => below.as_ref().map(|(v, _)| *v),
                    };
                    let pos = pos.or(below.map(|(_, ps)| ps));
                    let pos_wrong = positions
                        && match pos {
                            // An empty list is a tombstone.
                            Some(b) if !b.is_empty() => {
                                decode_positions(&b, &mut buf).is_err() || buf != *ps
                            }
                            _ => true,
                        };
                    (post != Some(value), pos_wrong)
                }
            };
            // In a version 2 index a position cell never stands without its posting cell,
            // so a pack, which walks the posting cells, meets every cell there is.
            if packed && write_pos {
                write_post = true;
            }
            if write_post {
                tx.put_u64(t, &row, POST, doc, value)
                    .aggregate(t, &row, MAXTF, BOUND, i64::from(tf))
                    .aggregate(t, &row, MINLEN, BOUND, i64::from(new.len));
                changed = true;
            }
            if write_pos {
                tx.put(t, &row, POS, doc, enc.as_deref().expect("positions are on"));
                changed = true;
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
            Self::unpost(tx, t, &row, doc, positions, packed);
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

    /// A text's tokens as the index holds them, with their byte ranges (highlighting,
    /// D309 §4.5): the terms `analyse` indexes, in token order.
    pub(crate) fn spans(&self, text: &str) -> crate::highlight::Spans {
        let cap = self.inner.opts.get_max_term_bytes() as usize;
        let mut out = Vec::new();
        self.inner.tok.tokens(text, &mut |tok| {
            let term = self.normalize_token(tok.text);
            if !term.is_empty() && term.len() <= cap {
                out.push((term.into_owned(), tok.position, tok.range.clone()));
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
        let s = inn.db.snapshot()?;
        // A repair running isn't an error here: its rows are checked as they stand.
        let packed = inn
            .table
            .get_at(&s, GLOBAL, META, REPAIR)?
            .is_some_and(|c| c.value() == PACKED);
        let t = self.rows(packed)?;
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
        if packed {
            fams.push(POSTB);
        }
        let mut buf = Vec::new();
        let mut block = Block::default();
        for row in t
            .scan_prefix(POST_PREFIX)
            .families(fams)
            .snapshot(&s)
            .iter()?
        {
            let row = row?;
            let term = row.key()[POST_PREFIX.len()..].to_vec();
            let (mut df, mut maxtf, mut minlen) = (0i64, i64::MIN, i64::MAX);
            // The merged view (a version 2 index): the blocks' postings, then the cells
            // over them, a tombstone removing what it hides.
            let mut post_docs: BTreeMap<Vec<u8>, (u32, u32)> = BTreeMap::new();
            let mut pos_docs: BTreeMap<Vec<u8>, u32> = BTreeMap::new();
            let mut last: Option<Vec<u8>> = None;
            for e in row.view().iter().filter(|e| e.family == POSTB) {
                block.decode(e.cell.value())?;
                if block.id(block.len() - 1) != e.qualifier
                    || last.as_deref().is_some_and(|l| l >= block.id(0))
                {
                    return Err(corrupt("a term's posting blocks overlap or are misnamed"));
                }
                last = Some(e.qualifier.to_vec());
                st.packed += block.len() as u64;
                for i in 0..block.len() {
                    post_docs.insert(block.id(i).to_vec(), block.posting(i));
                    if inn.opts.get_positions() {
                        decode_positions(block.positions(i), &mut buf)?;
                        pos_docs.insert(block.id(i).to_vec(), buf.len() as u32);
                    }
                }
            }
            for e in row.view().iter() {
                match e.family {
                    POST => {
                        let v = e
                            .cell
                            .as_u64()
                            .ok_or_else(|| corrupt("a posting isn't a u64"))?;
                        if packed && v == TOMBSTONE {
                            st.tombstones += 1;
                            st.packed -= u64::from(post_docs.remove(e.qualifier).is_some());
                        } else {
                            let under = post_docs.insert(e.qualifier.to_vec(), unposting(v));
                            st.packed -= u64::from(under.is_some());
                        }
                    }
                    POS => {
                        if packed && e.cell.value().is_empty() {
                            pos_docs.remove(e.qualifier);
                        } else {
                            decode_positions(e.cell.value(), &mut buf)?;
                            pos_docs.insert(e.qualifier.to_vec(), buf.len() as u32);
                        }
                    }
                    DF => df = e.cell.as_i64().unwrap_or(0),
                    MAXTF => maxtf = e.cell.as_i64().unwrap_or(i64::MIN),
                    MINLEN => minlen = e.cell.as_i64().unwrap_or(i64::MAX),
                    _ => {}
                }
            }
            let (mut max_seen, mut min_seen) = (0i64, i64::MAX);
            for (doc, &(tf, len)) in &post_docs {
                max_seen = max_seen.max(i64::from(tf));
                min_seen = min_seen.min(i64::from(len));
                *seen.entry(doc.clone()).or_default() += 1;
                let f = fwd.get(doc);
                if f.map(|f| (f.terms.get(&term).copied(), f.len)) != Some((Some(tf), len)) {
                    st.forward_mismatch += 1;
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
            if inn.opts.get_positions()
                && (pos_docs.len() != post_docs.len()
                    || pos_docs
                        .iter()
                        .any(|(d, n)| post_docs.get(d).map(|p| p.0) != Some(*n)))
            {
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
        self.verify_source(t, packed, &s, &fwd, &mut st)?;
        Ok(st)
    }

    fn verify_source(
        &self,
        t: &Rows,
        packed: bool,
        s: &Snapshot,
        fwd: &BTreeMap<Vec<u8>, Forward>,
        st: &mut IndexStats,
    ) -> Result<()> {
        let inn = &*self.inner;
        let q = inn.source.qualifier.as_slice();
        let mut sourced = BTreeSet::new();
        let mut buf = Vec::new();
        let mut block = Block::default();
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
                    let row = post_row(term);
                    let ok = match t.get_at(s, &row, POS, &doc)? {
                        Some(c) => decode_positions(c.value(), &mut buf).is_ok() && buf == *ps,
                        // Without a cell, the block's (a version 2 index).
                        None if packed => find_block(t, Some(s), &row, &doc, &mut block)?
                            .and_then(|f| f.at)
                            .is_some_and(|at| {
                                decode_positions(block.positions(at), &mut buf).is_ok()
                                    && buf == *ps
                            }),
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

/// What a pack's chunk writes: the blocks that replace the ones it read, and the cells it
/// consumed, each with the timestamp it was read at.
struct PackPlan {
    blocks: Vec<(Vec<u8>, Vec<u8>)>,
    posts: Vec<(Vec<u8>, u64)>,
    positions: Vec<(Vec<u8>, u64)>,
    /// Postings moved from cells into blocks, and tombstones removed.
    packed: u64,
    tombstones: u64,
}

/// Merges a chunk's cells into the blocks they fall among, and cuts the result into blocks
/// again. A cell replaces the block's posting for its document; a tombstone removes it.
fn pack_plan(work: &PackWork, with_positions: bool, limit: usize) -> Result<PackPlan> {
    let mut run: BTreeMap<Vec<u8>, Packed> = BTreeMap::new();
    let mut block = Block::default();
    let mut last: Option<Vec<u8>> = None;
    for (_, value) in &work.blocks {
        block.decode(value)?;
        if last.as_deref().is_some_and(|l| l >= block.id(0)) {
            return Err(corrupt("a term's posting blocks overlap"));
        }
        last = Some(block.id(block.len() - 1).to_vec());
        for i in 0..block.len() {
            let (tf, len) = block.posting(i);
            run.insert(
                block.id(i).to_vec(),
                Packed {
                    tf,
                    len,
                    positions: block.positions(i).to_vec(),
                },
            );
        }
    }
    let mut plan = PackPlan {
        blocks: Vec::new(),
        posts: Vec::new(),
        positions: Vec::new(),
        packed: 0,
        tombstones: 0,
    };
    // A few postings with no block among them wait as cells for more. A full chunk never
    // waits: its term has more than a block's worth.
    let live = work
        .cells
        .iter()
        .filter(|(_, _, v)| *v != TOMBSTONE)
        .count();
    let wait = work.blocks.is_empty() && live < PACK_MIN_RUN && work.cells.len() < limit;
    for (id, ts, value) in &work.cells {
        let pos = work.positions.get(id);
        if *value == TOMBSTONE {
            run.remove(id);
            plan.posts.push((id.clone(), *ts));
            if let Some((pts, _)) = pos {
                plan.positions.push((id.clone(), *pts));
            }
            plan.tombstones += 1;
            continue;
        }
        if wait {
            continue;
        }
        let (tf, len) = unposting(*value);
        // Its positions: its cell's, else what the block held for it. A posting with
        // neither (drift, which `verify` reports) stays a cell.
        let positions = match pos {
            Some((_, b)) if !b.is_empty() => b.clone(),
            _ if !with_positions => Vec::new(),
            _ => match run.get(id) {
                Some(p) if pos.is_none() => p.positions.clone(),
                _ => continue,
            },
        };
        run.insert(id.clone(), Packed { tf, len, positions });
        plan.posts.push((id.clone(), *ts));
        if let Some((pts, _)) = pos {
            plan.positions.push((id.clone(), *pts));
        }
        plan.packed += 1;
    }
    // Even blocks: as many as the postings need, each with its share, so none is left
    // under half full beside a full one. A block still closes early at its byte cap.
    let n = run.len();
    let share = n.div_ceil(n.div_ceil(MAX_POSTINGS).max(1)).max(1);
    let mut builder = Builder::new(with_positions);
    for (id, p) in &run {
        builder.push(Posting {
            id,
            tf: p.tf,
            len: p.len,
            positions: &p.positions,
        });
        if builder.len() >= share || builder.full() {
            plan.blocks.extend(builder.finish());
        }
    }
    plan.blocks.extend(builder.finish());
    Ok(plan)
}

/// Reads an index's `G` row and checks it: its source, options, and the tokeniser to use
/// (the built-in one, or `custom`), which must match the stored name and fingerprint.
fn load(
    rows: &Rows,
    name: &str,
    custom: Option<Box<dyn Tokenizer>>,
) -> Result<(Source, IndexOptions, Box<dyn Tokenizer>, bool)> {
    let not =
        |why: &str| Error::InvalidArgument(format!("table {name:?} is not a text index: {why}"));
    // A table without the index's families isn't one (a read of a missing family would fail
    // as the database's error, not as this one).
    if [POST, DF, TERMS, META, STATS]
        .iter()
        .any(|f| rows.family_info(f).is_none())
    {
        return Err(not("it lacks the index's families"));
    }
    let g = |q: &[u8]| {
        rows.get(GLOBAL, META, q)
            .map(|c| c.map(|c| c.value().to_vec()))
    };
    match rows.get(GLOBAL, META, LAYOUT)?.and_then(|c| c.as_u64()) {
        Some(LAYOUT_VERSION | LAYOUT_PACKED) => {}
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
    let stored_fp = rows
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
    Ok((source, opts, tok, builtin))
}

/// A text index opened from a reader process ([`PigeonholeReader`]): searches and
/// [`verify`](ReadIndex::verify) at the reader's snapshots, and no writes (D309 rev 5 §10).
///
/// The writer process writes the index ([`Index`]). A reader process opens it by name, and
/// sees what the writer has committed, as any read there does. A search while the writer
/// runs [`Index::repair`] fails with `Busy`.
#[derive(Clone)]
pub struct ReadIndex {
    index: Index,
}

impl std::fmt::Debug for ReadIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadIndex")
            .field("table", &self.index.inner.table.name())
            .field("source", &self.index.inner.source)
            .field("options", &self.index.inner.opts)
            .finish()
    }
}

impl ReadIndex {
    fn open_inner(
        reader: &PigeonholeReader,
        name: &str,
        custom: Option<Box<dyn Tokenizer>>,
    ) -> Result<ReadIndex> {
        let rows = Rows::R(reader.table(name)?);
        let (source, opts, tok, builtin) = load(&rows, name, custom)?;
        let src = Rows::R(reader.table(&source.table)?);
        Ok(ReadIndex {
            index: Index {
                inner: Arc::new(Inner {
                    db: Handle::R(reader.clone()),
                    table: rows,
                    src,
                    writer: None,
                    source,
                    opts,
                    tok,
                    builtin,
                    fresh: OnceLock::new(),
                }),
            },
        })
    }

    /// The `k` best documents for `query`, as [`Index::search`] gives them, at the
    /// reader's snapshot.
    ///
    /// While the writer process runs [`Index::repair`], the search fails with
    /// [`Error::Busy`](crate::Error::Busy). That's transient: retry after the repair
    /// finishes, and the search succeeds on the repaired index.
    pub fn search(&self, query: impl Into<Query>, k: usize) -> Search<'_> {
        self.index.search(query, k)
    }

    /// Checks the index's rows, as [`Index::verify`] does, at the reader's snapshot.
    pub fn verify(&self) -> Result<IndexStats> {
        self.index.verify()
    }

    /// The index's options.
    pub fn options(&self) -> IndexOptions {
        self.index.options()
    }

    /// The column the index is built over.
    pub fn source(&self) -> &Source {
        self.index.source()
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
