//! `Pigeonhole::Text` and `Pigeonhole::Vector` (#697 PR 3b, ICR 0111): the full-text index
//! (`pigeonhole-text`, D309) and the vector index (`pigeonhole-vector`, D306), as the PR 3
//! design note on #697 and its reviews set them out.
//!
//! - **Text.** `Query`, `Index` and `ReadIndex`. A query is a `Query` or a String in the
//!   query syntax. Custom tokenizers aren't bound: the engine calls a tokenizer inside its
//!   own calls, where a Ruby block can't run (ai1249 and perf2's Q2).
//! - **Vector.** `Index`, `ReadIndex`, `Placed` (one transaction's writes, for
//!   `after_commit`) and `Rebalancer` (driven by a Ruby thread, each call in slices with the
//!   GVL released; no `set_wakeup`, for Q2's reason).
//!
//! Results come back as Arrays, which `lib/pigeonhole/index.rb` turns into frozen `Data`.

use std::sync::Mutex;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::{Duration, Instant};

use magnus::{Error, RArray, RString, Ruby, TryConvert, Value, prelude::*};
use pigeonhole::ErrorCode;
use pigeonhole_text as text;
use pigeonhole_vector as vidx;

use crate::classes::{
    Database, Placed, Reader, Rebalancer, Snapshot, Table, TextIndex, TextQuery, TextReadIndex,
    Transaction, VectorBulkLoad, VectorIndex, VectorReadIndex, VectorSpec,
};
use crate::convert::{Bytes, Secs, arg_error, class_name, frozen, lock, type_error};
use crate::error::{spent, to_rb, with_code};
use crate::fork::ForkSafe;
use crate::gvl::without_gvl;
use crate::read::snapshot_arg;
use crate::vector::{Vector, sym_name, vector};

/// The longest a wait here runs with the GVL released before it checks for an interrupt.
const SLICE: Duration = Duration::from_millis(100);

// ---- Errors --------------------------------------------------------------------------------

fn text_err(ruby: &Ruby, e: text::Error) -> Error {
    match e {
        text::Error::Db(e) => to_rb(ruby, &e),
        e => with_code(ruby, e.code(), &e.to_string()),
    }
}

fn vec_err(ruby: &Ruby, e: vidx::Error) -> Error {
    match e {
        vidx::Error::Db(e) => to_rb(ruby, &e),
        e => with_code(ruby, e.code(), &e.to_string()),
    }
}

/// Runs a text index call with the GVL released.
fn tcall<F, R>(ruby: &Ruby, f: F) -> Result<R, Error>
where
    F: FnOnce() -> text::Result<R> + Send,
    R: Send,
{
    without_gvl(ruby, f)?.map_err(|e| text_err(ruby, e))
}

/// Runs a vector index call with the GVL released.
fn vcall<F, R>(ruby: &Ruby, f: F) -> Result<R, Error>
where
    F: FnOnce() -> vidx::Result<R> + Send,
    R: Send,
{
    without_gvl(ruby, f)?.map_err(|e| vec_err(ruby, e))
}

/// Runs `f` inside `txn` with the GVL released; `f`'s own error converts with `conv`.
fn in_txn<F, R, E: Send>(
    ruby: &Ruby,
    txn: &Transaction,
    f: F,
    conv: impl FnOnce(&Ruby, E) -> Error,
) -> Result<R, Error>
where
    F: FnOnce(&mut pigeonhole::Transaction) -> Result<R, E> + Send,
    R: Send,
{
    let m = txn.0.get(ruby)?;
    match without_gvl(ruby, move || lock(m).as_mut().map(f))? {
        Some(r) => r.map_err(|e| conv(ruby, e)),
        None => Err(spent(ruby, crate::db::TXN_SPENT)),
    }
}

fn invalid(ruby: &Ruby, m: impl Into<String>) -> Error {
    with_code(ruby, ErrorCode::InvalidArgument, &m.into())
}

// ---- Text ----------------------------------------------------------------------------------

fn normalization(ruby: &Ruby, v: Value) -> Result<text::Normalization, Error> {
    Ok(match sym_name(ruby, v, "normalization")?.as_str() {
        "nfc" => text::Normalization::Nfc,
        "nfkc" => text::Normalization::Nfkc,
        "none" => text::Normalization::None,
        n => {
            return Err(invalid(
                ruby,
                format!("normalization :{n}: :nfc, :nfkc or :none"),
            ));
        }
    })
}

fn normalization_name(n: text::Normalization) -> &'static str {
    match n {
        text::Normalization::Nfkc => "nfkc",
        text::Normalization::None => "none",
        _ => "nfc",
    }
}

fn fuzziness(ruby: &Ruby, v: Value) -> Result<text::Fuzziness, Error> {
    if let Ok(n) = i64::try_convert(v)
        && !v.is_kind_of(ruby.class_float())
    {
        return match n {
            1 => Ok(text::Fuzziness::One),
            2 => Ok(text::Fuzziness::Two),
            _ => Err(invalid(ruby, format!("fuzziness {n}: :auto, 1 or 2"))),
        };
    }
    match sym_name(ruby, v, "fuzziness")?.as_str() {
        "auto" => Ok(text::Fuzziness::Auto),
        f => Err(invalid(ruby, format!("fuzziness :{f}: :auto, 1 or 2"))),
    }
}

impl TextQuery {
    /// An empty query: add clauses with `term`, `text`, `phrase` and `fuzzy`.
    pub(crate) fn new() -> Self {
        Self(text::Query::new())
    }

    /// The query `s` in the query syntax (`InvalidArgument` naming where it fails).
    pub(crate) fn parse(ruby: &Ruby, s: String) -> Result<Self, Error> {
        text::Query::parse(&s)
            .map(Self)
            .map_err(|e| text_err(ruby, e))
    }

    /// This query and one more term, matched as it is (after normalization).
    pub(crate) fn term(&self, t: String) -> Self {
        Self(self.0.clone().term(t))
    }

    /// This query and free text, tokenized as documents are.
    pub(crate) fn text(&self, t: String) -> Self {
        Self(self.0.clone().text(t))
    }

    /// `native_phrase(words, slop)`: this query and a phrase.
    pub(crate) fn phrase(&self, words: Vec<String>, slop: u16) -> Self {
        Self(self.0.clone().phrase_with_slop(words, slop))
    }

    /// `native_fuzzy(term, fuzziness)`.
    pub(crate) fn fuzzy(ruby: &Ruby, rb_self: &Self, t: String, f: Value) -> Result<Self, Error> {
        Ok(Self(rb_self.0.clone().fuzzy(t, fuzziness(ruby, f)?)))
    }

    pub(crate) fn eq(&self, other: Value) -> bool {
        <&TextQuery>::try_convert(other).is_ok_and(|o| o.0 == self.0)
    }

    pub(crate) fn inspect(&self) -> String {
        format!("#<Pigeonhole::Text::Query {:?}>", self.0.clauses())
    }
}

/// A query argument: a `Query`, or a String in the query syntax (parsed when the search
/// runs, so a syntax error raises then).
fn query_arg(ruby: &Ruby, v: Value) -> Result<text::Query, Error> {
    if let Ok(q) = <&TextQuery>::try_convert(v) {
        return Ok(q.0.clone());
    }
    match RString::from_value(v) {
        Some(s) => Ok(text::Query::from(s.to_string()?)),
        None => Err(type_error(
            ruby,
            format!(
                "a query is a Pigeonhole::Text::Query or a String, not {}",
                class_name(v)
            ),
        )),
    }
}

/// `[k1, b, positions, stop_words, normalization, max_terms, max_term_bytes]`.
fn text_options(ruby: &Ruby, o: &text::IndexOptions) -> Result<RArray, Error> {
    let a = ruby.ary_new_capa(7);
    a.push(o.get_k1())?;
    a.push(o.get_b())?;
    a.push(o.get_positions())?;
    a.push(o.get_stop_words())?;
    a.push(ruby.to_symbol(normalization_name(o.get_normalization())))?;
    a.push(o.get_max_terms())?;
    a.push(o.get_max_term_bytes())?;
    Ok(a)
}

/// `[table, family, qualifier]`.
fn text_source(ruby: &Ruby, s: &text::Source) -> Result<RArray, Error> {
    let a = ruby.ary_new_capa(3);
    a.push(s.table())?;
    a.push(s.family())?;
    a.push(frozen(ruby, s.qualifier()))?;
    Ok(a)
}

/// `[documents, terms, postings, forward_mismatch, source_mismatch, counter_drift,
/// invalid_utf8, consistent]`.
fn text_stats(ruby: &Ruby, s: &text::IndexStats) -> Result<RArray, Error> {
    let a = ruby.ary_new_capa(8);
    a.push(s.documents)?;
    a.push(s.terms)?;
    a.push(s.postings)?;
    a.push(s.forward_mismatch)?;
    a.push(s.source_mismatch)?;
    a.push(s.counter_drift)?;
    a.push(s.invalid_utf8)?;
    a.push(s.is_consistent())?;
    Ok(a)
}

/// `[documents, changed, invalid_utf8]`.
fn rebuild_stats(ruby: &Ruby, s: &text::RebuildStats) -> Result<RArray, Error> {
    let a = ruby.ary_new_capa(3);
    a.push(s.documents)?;
    a.push(s.changed)?;
    a.push(s.invalid_utf8)?;
    Ok(a)
}

/// `[[hit, ...], [[clause, term, expansions, cut], ...], stale]`, a hit being `[id, score,
/// text, [[clause, occurrence, start, end], ...]]`.
fn text_hits(
    ruby: &Ruby,
    (hits, stats): (Vec<text::Hit>, text::SearchStats),
) -> Result<RArray, Error> {
    let out = ruby.ary_new_capa(hits.len());
    for h in hits {
        let a = ruby.ary_new_capa(4);
        a.push(frozen(ruby, &h.id))?;
        a.push(h.score)?;
        a.push(h.text.as_deref().map(|t| {
            let s = ruby.str_new(t);
            s.freeze();
            s
        }))?;
        let ms = ruby.ary_new_capa(h.matches.len());
        for m in &h.matches {
            ms.push((m.clause, m.occurrence, m.range.start, m.range.end))?;
        }
        a.push(ms)?;
        out.push(a)?;
    }
    let fuzzy = ruby.ary_new_capa(stats.fuzzy.len());
    for f in &stats.fuzzy {
        fuzzy.push((f.clause, f.term.clone(), f.expansions, f.cut))?;
    }
    let r = ruby.ary_new_capa(3);
    r.push(out)?;
    r.push(fuzzy)?;
    r.push(stats.stale)?;
    Ok(r)
}

/// `search`'s arguments after the query and `k`: `[id_prefix, snapshot, highlight,
/// prefix_length, max_expansions]`.
struct TextSearchArgs {
    id_prefix: Option<Vec<u8>>,
    snapshot: Option<pigeonhole::Snapshot>,
    highlight: bool,
    prefix_length: Option<usize>,
    max_expansions: Option<usize>,
}

impl TextSearchArgs {
    fn from(ruby: &Ruby, a: RArray) -> Result<Self, Error> {
        if a.len() != 5 {
            return Err(arg_error(ruby, "native_search takes 5 options"));
        }
        let id_prefix: Option<Bytes> = a.entry(0)?;
        let snapshot: Option<Value> = a.entry(1)?;
        let snapshot = snapshot.map(<&Snapshot>::try_convert).transpose()?;
        Ok(Self {
            id_prefix: id_prefix.map(|b| b.0),
            snapshot: snapshot_arg(ruby, snapshot)?,
            highlight: a.entry(2)?,
            prefix_length: a.entry(3)?,
            max_expansions: a.entry(4)?,
        })
    }

    fn run(self, s: text::Search<'_>) -> text::Result<(Vec<text::Hit>, text::SearchStats)> {
        let mut s = s.highlight(self.highlight);
        if let Some(p) = self.id_prefix {
            s = s.id_prefix(p);
        }
        if let Some(n) = self.prefix_length {
            s = s.prefix_length(n);
        }
        if let Some(n) = self.max_expansions {
            s = s.max_expansions(n);
        }
        match &self.snapshot {
            Some(snap) => s.snapshot(snap).run_with_stats(),
            None => s.run_with_stats(),
        }
    }
}

impl TextIndex {
    fn index(&self, ruby: &Ruby) -> Result<&text::Index, Error> {
        self.0.get(ruby)
    }

    /// `native_create(db, name, table, family, qualifier, k1, b, positions, stop_words,
    /// normalization, max_terms, max_term_bytes)`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create(
        ruby: &Ruby,
        db: &Database,
        name: String,
        table: &Table,
        family: String,
        qualifier: Bytes,
        options: RArray,
    ) -> Result<Self, Error> {
        if options.len() != 7 {
            return Err(arg_error(ruby, "native_create takes 7 options"));
        }
        let mut o = text::IndexOptions::new();
        if let Some(k1) = options.entry::<Option<f64>>(0)? {
            o = o.k1(k1);
        }
        if let Some(b) = options.entry::<Option<f64>>(1)? {
            o = o.b(b);
        }
        if let Some(p) = options.entry::<Option<bool>>(2)? {
            o = o.positions(p);
        }
        if let Some(s) = options.entry::<Option<bool>>(3)? {
            o = o.stop_words(s);
        }
        if let Some(n) = options.entry::<Option<Value>>(4)? {
            o = o.normalization(normalization(ruby, n)?);
        }
        if let Some(n) = options.entry::<Option<u32>>(5)? {
            o = o.max_terms(n);
        }
        if let Some(n) = options.entry::<Option<u32>>(6)? {
            o = o.max_term_bytes(n);
        }
        let source = text::Source::new(table.writer(ruby)?, &family, &qualifier.0);
        let p = db.db(ruby)?;
        let i = tcall(ruby, move || text::Index::create(&p, &name, source, o))?;
        Ok(Self(ForkSafe::new(i)))
    }

    pub(crate) fn open(ruby: &Ruby, db: &Database, name: String) -> Result<Self, Error> {
        let p = db.db(ruby)?;
        let i = tcall(ruby, move || text::Index::open(&p, &name))?;
        Ok(Self(ForkSafe::new(i)))
    }

    pub(crate) fn open_reader(
        ruby: &Ruby,
        reader: &Reader,
        name: String,
    ) -> Result<TextReadIndex, Error> {
        let r = reader.db(ruby)?;
        let i = tcall(ruby, move || text::Index::open_reader(&r, &name))?;
        Ok(TextReadIndex(ForkSafe::new(i)))
    }

    /// `put(id, text)`: indexes `text` as document `id` (replacing what it held), and
    /// writes it to the source cell, in one transaction.
    pub(crate) fn put(ruby: &Ruby, rb_self: &Self, id: Bytes, t: String) -> Result<(), Error> {
        let i = rb_self.index(ruby)?.clone();
        tcall(ruby, move || i.put(&id.0, &t))
    }

    pub(crate) fn put_in(
        ruby: &Ruby,
        rb_self: &Self,
        txn: &Transaction,
        id: Bytes,
        t: String,
    ) -> Result<(), Error> {
        let i = rb_self.index(ruby)?.clone();
        in_txn(ruby, txn, move |tx| i.put_in(tx, &id.0, &t), text_err)
    }

    pub(crate) fn delete(ruby: &Ruby, rb_self: &Self, id: Bytes) -> Result<bool, Error> {
        let i = rb_self.index(ruby)?.clone();
        tcall(ruby, move || i.delete(&id.0))
    }

    pub(crate) fn delete_in(
        ruby: &Ruby,
        rb_self: &Self,
        txn: &Transaction,
        id: Bytes,
    ) -> Result<bool, Error> {
        let i = rb_self.index(ruby)?.clone();
        in_txn(ruby, txn, move |tx| i.delete_in(tx, &id.0), text_err)
    }

    pub(crate) fn rebuild(ruby: &Ruby, rb_self: &Self) -> Result<RArray, Error> {
        let i = rb_self.index(ruby)?.clone();
        rebuild_stats(ruby, &tcall(ruby, move || i.rebuild())?)
    }

    pub(crate) fn repair(ruby: &Ruby, rb_self: &Self) -> Result<RArray, Error> {
        let i = rb_self.index(ruby)?.clone();
        rebuild_stats(ruby, &tcall(ruby, move || i.repair())?)
    }

    pub(crate) fn verify(ruby: &Ruby, rb_self: &Self) -> Result<RArray, Error> {
        let i = rb_self.index(ruby)?.clone();
        text_stats(ruby, &tcall(ruby, move || i.verify())?)
    }

    pub(crate) fn options(ruby: &Ruby, rb_self: &Self) -> Result<RArray, Error> {
        text_options(ruby, &rb_self.index(ruby)?.options())
    }

    pub(crate) fn source(ruby: &Ruby, rb_self: &Self) -> Result<RArray, Error> {
        text_source(ruby, rb_self.index(ruby)?.source())
    }

    /// `native_search(query, k, [id_prefix, snapshot, highlight, prefix_length,
    /// max_expansions])`.
    pub(crate) fn search(
        ruby: &Ruby,
        rb_self: &Self,
        query: Value,
        k: usize,
        opts: RArray,
    ) -> Result<RArray, Error> {
        let q = query_arg(ruby, query)?;
        let a = TextSearchArgs::from(ruby, opts)?;
        let i = rb_self.index(ruby)?.clone();
        text_hits(ruby, tcall(ruby, move || a.run(i.search(q, k)))?)
    }
}

impl TextReadIndex {
    fn index(&self, ruby: &Ruby) -> Result<&text::ReadIndex, Error> {
        self.0.get(ruby)
    }

    pub(crate) fn verify(ruby: &Ruby, rb_self: &Self) -> Result<RArray, Error> {
        let i = rb_self.index(ruby)?.clone();
        text_stats(ruby, &tcall(ruby, move || i.verify())?)
    }

    pub(crate) fn options(ruby: &Ruby, rb_self: &Self) -> Result<RArray, Error> {
        text_options(ruby, &rb_self.index(ruby)?.options())
    }

    pub(crate) fn source(ruby: &Ruby, rb_self: &Self) -> Result<RArray, Error> {
        text_source(ruby, rb_self.index(ruby)?.source())
    }

    pub(crate) fn search(
        ruby: &Ruby,
        rb_self: &Self,
        query: Value,
        k: usize,
        opts: RArray,
    ) -> Result<RArray, Error> {
        let q = query_arg(ruby, query)?;
        let a = TextSearchArgs::from(ruby, opts)?;
        let i = rb_self.index(ruby)?.clone();
        text_hits(ruby, tcall(ruby, move || a.run(i.search(q, k)))?)
    }
}

// ---- Vector --------------------------------------------------------------------------------

fn rebalance(ruby: &Ruby, v: Value) -> Result<vidx::Rebalance, Error> {
    Ok(match sym_name(ruby, v, "rebalance")?.as_str() {
        "inline" => vidx::Rebalance::Inline,
        "background" => vidx::Rebalance::Background,
        r => {
            return Err(invalid(
                ruby,
                format!("rebalance :{r}: :inline or :background"),
            ));
        }
    })
}

fn rebalance_name(r: vidx::Rebalance) -> &'static str {
    match r {
        vidx::Rebalance::Background => "background",
        _ => "inline",
    }
}

/// `native_create`'s options: `[nprobe, target_list_len, reassign_neighbours, rebalance]`.
fn vector_options(ruby: &Ruby, a: RArray) -> Result<vidx::IndexOptions, Error> {
    if a.len() != 4 {
        return Err(arg_error(ruby, "native_create takes 4 options"));
    }
    let mut o = vidx::IndexOptions::new();
    if let Some(n) = a.entry::<Option<u32>>(0)? {
        o = o.nprobe(n);
    }
    if let Some(n) = a.entry::<Option<u32>>(1)? {
        o = o.target_list_len(n);
    }
    if let Some(n) = a.entry::<Option<u32>>(2)? {
        o = o.reassign_neighbours(n);
    }
    if let Some(r) = a.entry::<Option<Value>>(3)? {
        o = o.rebalance(rebalance(ruby, r)?);
    }
    Ok(o)
}

/// A vector argument for an index of `spec`: the forms `put_vector` takes. A half index
/// takes `f32` values, which the index rounds.
fn vector_arg(ruby: &Ruby, spec: &pigeonhole::VectorSpec, v: Value) -> Result<Vector, Error> {
    vector(ruby, v, spec.element())
}

/// `[[id, distance], ...]`, and `[lists_probed, lists]`.
fn vector_hits(
    ruby: &Ruby,
    (hits, stats): (Vec<vidx::Hit>, vidx::SearchStats),
) -> Result<RArray, Error> {
    let out = ruby.ary_new_capa(hits.len());
    for h in hits {
        out.push((frozen(ruby, &h.id), h.distance))?;
    }
    let r = ruby.ary_new_capa(2);
    r.push(out)?;
    r.push((stats.lists_probed, stats.lists))?;
    Ok(r)
}

/// `search`'s options after the query and `k`: `[nprobe, parallel, snapshot, id_prefix,
/// max_probe]`.
struct VectorSearchArgs {
    nprobe: Option<u32>,
    parallel: Option<usize>,
    snapshot: Option<pigeonhole::Snapshot>,
    id_prefix: Option<Vec<u8>>,
    max_probe: Option<u32>,
}

impl VectorSearchArgs {
    fn from(ruby: &Ruby, a: RArray) -> Result<Self, Error> {
        if a.len() != 5 {
            return Err(arg_error(ruby, "native_search takes 5 options"));
        }
        let snapshot: Option<Value> = a.entry(2)?;
        let snapshot = snapshot.map(<&Snapshot>::try_convert).transpose()?;
        let id_prefix: Option<Bytes> = a.entry(3)?;
        Ok(Self {
            nprobe: a.entry(0)?,
            parallel: a.entry(1)?,
            snapshot: snapshot_arg(ruby, snapshot)?,
            id_prefix: id_prefix.map(|b| b.0),
            max_probe: a.entry(4)?,
        })
    }

    fn run(self, s: vidx::Search<'_>) -> vidx::Result<(Vec<vidx::Hit>, vidx::SearchStats)> {
        let mut s = s;
        if let Some(n) = self.nprobe {
            s = s.nprobe(n);
        }
        if let Some(n) = self.parallel {
            s = s.parallel(n);
        }
        if let Some(n) = self.max_probe {
            s = s.max_probe(n);
        }
        let prefix = self.id_prefix;
        if let Some(p) = &prefix {
            s = s.id_prefix(p);
        }
        match &self.snapshot {
            Some(snap) => s.snapshot(snap).run_with_stats(),
            None => s.run_with_stats(),
        }
    }
}

/// Runs a search of `query` on `$index` (an `Index` or a `ReadIndex`), by the query's form.
macro_rules! vsearch {
    ($index:expr, $query:expr, $k:expr, $args:expr) => {{
        let (i, q, k, a) = ($index, $query, $k, $args);
        match q {
            Vector::F32(v) => a.run(i.search_f32(&v, k)),
            Vector::I8(v) => a.run(i.search_i8(&v, k)),
            Vector::Bits(v) => a.run(i.search_bits(&v, k)),
        }
    }};
}

/// `[vectors, [list length, ...]]`.
fn vector_stats(ruby: &Ruby, s: &vidx::IndexStats) -> Result<RArray, Error> {
    let a = ruby.ary_new_capa(2);
    a.push(s.vectors)?;
    a.push(ruby.ary_from_iter(s.list_lens.iter().copied()))?;
    Ok(a)
}

impl VectorIndex {
    fn index(&self, ruby: &Ruby) -> Result<&vidx::Index, Error> {
        self.0.get(ruby)
    }

    /// `native_create(db, name, spec, [nprobe, target_list_len, reassign_neighbours,
    /// rebalance], with_rebalancer)`: the index, or `[index, rebalancer]`.
    pub(crate) fn create(
        ruby: &Ruby,
        db: &Database,
        name: String,
        spec: &VectorSpec,
        options: RArray,
        with_rebalancer: bool,
    ) -> Result<Value, Error> {
        let o = vector_options(ruby, options)?;
        let spec = spec.0;
        let p = db.db(ruby)?;
        if with_rebalancer {
            let (i, r) = vcall(ruby, move || {
                vidx::Index::create_with_rebalancer(&p, &name, spec, o)
            })?;
            Ok(pair(ruby, i, r)?.as_value())
        } else {
            let i = vcall(ruby, move || vidx::Index::create(&p, &name, spec, o))?;
            Ok(ruby.obj_wrap(Self(ForkSafe::new(i))).as_value())
        }
    }

    /// `native_open(db, name, with_rebalancer)`.
    pub(crate) fn open(
        ruby: &Ruby,
        db: &Database,
        name: String,
        with_rebalancer: bool,
    ) -> Result<Value, Error> {
        let p = db.db(ruby)?;
        if with_rebalancer {
            let (i, r) = vcall(ruby, move || vidx::Index::open_with_rebalancer(&p, &name))?;
            Ok(pair(ruby, i, r)?.as_value())
        } else {
            let i = vcall(ruby, move || vidx::Index::open(&p, &name))?;
            Ok(ruby.obj_wrap(Self(ForkSafe::new(i))).as_value())
        }
    }

    pub(crate) fn open_reader(
        ruby: &Ruby,
        reader: &Reader,
        name: String,
    ) -> Result<VectorReadIndex, Error> {
        let r = reader.db(ruby)?;
        let i = vcall(ruby, move || vidx::Index::open_reader(&r, &name))?;
        Ok(VectorReadIndex(ForkSafe::new(i)))
    }

    pub(crate) fn spec(ruby: &Ruby, rb_self: &Self) -> Result<VectorSpec, Error> {
        Ok(VectorSpec(rb_self.index(ruby)?.spec()))
    }

    pub(crate) fn rebalance(ruby: &Ruby, rb_self: &Self) -> Result<magnus::Symbol, Error> {
        Ok(ruby.to_symbol(rebalance_name(
            rb_self.index(ruby)?.options().rebalance_mode(),
        )))
    }

    pub(crate) fn max_list_len(ruby: &Ruby, rb_self: &Self) -> Result<u64, Error> {
        Ok(rb_self.index(ruby)?.max_list_len())
    }

    pub(crate) fn min_list_len(ruby: &Ruby, rb_self: &Self) -> Result<u64, Error> {
        Ok(rb_self.index(ruby)?.min_list_len())
    }

    /// `insert(id, vector)`: inserts or replaces `id`'s vector, in its own transaction.
    pub(crate) fn insert(ruby: &Ruby, rb_self: &Self, id: Bytes, v: Value) -> Result<(), Error> {
        let i = rb_self.index(ruby)?.clone();
        let v = vector_arg(ruby, &i.spec(), v)?;
        vcall(ruby, move || match v {
            Vector::F32(v) => i.insert_f32(&id.0, &v),
            Vector::I8(v) => i.insert_i8(&id.0, &v),
            Vector::Bits(v) => i.insert_bits(&id.0, &v),
        })
    }

    pub(crate) fn delete(ruby: &Ruby, rb_self: &Self, id: Bytes) -> Result<bool, Error> {
        let i = rb_self.index(ruby)?.clone();
        vcall(ruby, move || i.delete(&id.0))
    }

    /// `insert_in(txn, placed, id, vector)`: as `insert`, in `txn`, recorded in `placed`.
    pub(crate) fn insert_in(
        ruby: &Ruby,
        rb_self: &Self,
        txn: &Transaction,
        placed: &Placed,
        id: Bytes,
        v: Value,
    ) -> Result<(), Error> {
        let i = rb_self.index(ruby)?.clone();
        let v = vector_arg(ruby, &i.spec(), v)?;
        let p = placed.0.get(ruby)?;
        in_txn(
            ruby,
            txn,
            move |tx| match lock(p).as_mut() {
                None => Ok(None),
                Some(placed) => match v {
                    Vector::F32(v) => i.insert_f32_in(tx, placed, &id.0, &v),
                    Vector::I8(v) => i.insert_i8_in(tx, placed, &id.0, &v),
                    Vector::Bits(v) => i.insert_bits_in(tx, placed, &id.0, &v),
                }
                .map(Some),
            },
            vec_err,
        )?
        .ok_or_else(|| spent(ruby, PLACED_SPENT))
    }

    /// `delete_in(txn, placed, id)`: whether `id` had a vector.
    pub(crate) fn delete_in(
        ruby: &Ruby,
        rb_self: &Self,
        txn: &Transaction,
        placed: &Placed,
        id: Bytes,
    ) -> Result<bool, Error> {
        let i = rb_self.index(ruby)?.clone();
        let p = placed.0.get(ruby)?;
        in_txn(
            ruby,
            txn,
            move |tx| match lock(p).as_mut() {
                None => Ok(None),
                Some(placed) => i.delete_in(tx, placed, &id.0).map(Some),
            },
            vec_err,
        )?
        .ok_or_else(|| spent(ruby, PLACED_SPENT))
    }

    /// `after_commit(placed)`: splits or merges what the transaction's writes left out of
    /// bounds. It uses `placed` up.
    pub(crate) fn after_commit(ruby: &Ruby, rb_self: &Self, placed: &Placed) -> Result<(), Error> {
        let i = rb_self.index(ruby)?.clone();
        let p = lock(placed.0.get(ruby)?)
            .take()
            .ok_or_else(|| spent(ruby, PLACED_SPENT))?;
        vcall(ruby, move || i.after_commit(p))
    }

    pub(crate) fn verify(ruby: &Ruby, rb_self: &Self) -> Result<RArray, Error> {
        let i = rb_self.index(ruby)?.clone();
        vector_stats(ruby, &vcall(ruby, move || i.verify())?)
    }

    /// `native_search(query, k, [nprobe, parallel, snapshot, id_prefix, max_probe])`.
    pub(crate) fn search(
        ruby: &Ruby,
        rb_self: &Self,
        query: Value,
        k: usize,
        opts: RArray,
    ) -> Result<RArray, Error> {
        let i = rb_self.index(ruby)?.clone();
        let q = vector_arg(ruby, &i.spec(), query)?;
        let a = VectorSearchArgs::from(ruby, opts)?;
        vector_hits(ruby, vcall(ruby, move || vsearch!(&i, q, k, a))?)
    }

    /// `native_bulk_load(expected)`: a loader that `push` feeds batches of `[id, vector]`
    /// pairs to and `finish` ends (ai1249's P2 on #697: the pairs stream to the index a
    /// batch at a time, never held whole).
    pub(crate) fn bulk_load(
        ruby: &Ruby,
        rb_self: &Self,
        expected: u64,
    ) -> Result<VectorBulkLoad, Error> {
        let i = rb_self.index(ruby)?.clone();
        let spec = i.spec();
        let (tx, rx) = mpsc::sync_channel::<Vec<(Vec<u8>, Vector)>>(2);
        let worker = std::thread::Builder::new()
            .name("pigeonhole-bulk-load".into())
            .spawn(move || load(&i, spec, expected, rx))
            .map_err(|e| arg_error(ruby, format!("can't start the bulk load thread: {e}")))?;
        Ok(VectorBulkLoad(ForkSafe::new(Mutex::new(Some(BulkState {
            spec,
            tx,
            worker,
        })))))
    }
}

/// The loader thread: one bulk load over every batch the Ruby side sends, in order, until
/// it closes the channel.
fn load(
    i: &vidx::Index,
    spec: pigeonhole::VectorSpec,
    expected: u64,
    rx: Receiver<Vec<(Vec<u8>, Vector)>>,
) -> vidx::Result<vidx::BulkStats> {
    let batches = rx.into_iter().flatten();
    // The index's element decides the form (perf2's N4 on #1434): `push` converted every
    // pair by it, and an empty input still reaches the matching load.
    match spec.element() {
        pigeonhole::Element::I8 => i.bulk_load_i8(
            expected,
            batches.filter_map(|(id, v)| match v {
                Vector::I8(v) => Some((id, v)),
                _ => {
                    // `push` converts every pair by the index's spec, so all have one form.
                    debug_assert!(false, "a bulk load's pairs have one form");
                    None
                }
            }),
        ),
        pigeonhole::Element::Bit => i.bulk_load_bits(
            expected,
            batches.filter_map(|(id, v)| match v {
                Vector::Bits(v) => Some((id, v)),
                _ => {
                    // `push` converts every pair by the index's spec, so all have one form.
                    debug_assert!(false, "a bulk load's pairs have one form");
                    None
                }
            }),
        ),
        _ => i.bulk_load_f32(
            expected,
            batches.filter_map(|(id, v)| match v {
                Vector::F32(v) => Some((id, v)),
                _ => {
                    // `push` converts every pair by the index's spec, so all have one form.
                    debug_assert!(false, "a bulk load's pairs have one form");
                    None
                }
            }),
        ),
    }
}

/// A bulk load in progress: the batches' channel and its thread. Dropped without `finish`
/// (a `BulkLoad` collected unfinished), the channel closes and the loader commits what it
/// got before ending on its own (ai1249's N2 on #1434); `bulk_load` always finishes.
pub(crate) struct BulkState {
    spec: pigeonhole::VectorSpec,
    tx: SyncSender<Vec<(Vec<u8>, Vector)>>,
    worker: std::thread::JoinHandle<vidx::Result<vidx::BulkStats>>,
}

const LOAD_SPENT: &str = "the bulk load is finished";

impl VectorBulkLoad {
    /// `push(pairs)`: one batch of `[id, vector]` pairs, converted now and sent to the
    /// loader. It waits, with the GVL released, while the loader is two batches behind.
    pub(crate) fn push(ruby: &Ruby, rb_self: &Self, pairs: RArray) -> Result<(), Error> {
        let m = rb_self.0.get(ruby)?;
        let (spec, tx) = match lock(m).as_ref() {
            Some(s) => (s.spec, s.tx.clone()),
            None => return Err(spent(ruby, LOAD_SPENT)),
        };
        let mut batch = Vec::with_capacity(pairs.len());
        for p in pairs.into_iter() {
            let bad = || type_error(ruby, "bulk_load takes [id, vector] pairs");
            let p = RArray::from_value(p).ok_or_else(bad)?;
            if p.len() != 2 {
                return Err(bad());
            }
            let id: Bytes = p.entry(0)?;
            let v: Value = p.entry(1)?;
            batch.push((id.0, vector_arg(ruby, &spec, v)?));
        }
        let mut batch = Some(batch);
        loop {
            let b = batch.take().expect("sent once");
            let t = tx.clone();
            // Up to a slice of tries with the GVL released, not a GVL round trip per
            // millisecond while k-means keeps the channel full (perf2's N3 on #1434).
            match without_gvl(ruby, move || {
                let start = Instant::now();
                let mut b = b;
                loop {
                    match t.try_send(b) {
                        Ok(()) => return Ok(()),
                        Err(mpsc::TrySendError::Disconnected(_)) => return Err(None),
                        Err(mpsc::TrySendError::Full(back)) => {
                            if start.elapsed() >= SLICE {
                                return Err(Some(back));
                            }
                            b = back;
                            std::thread::sleep(Duration::from_millis(1));
                        }
                    }
                }
            })? {
                Ok(()) => return Ok(()),
                // The loader stopped, so it failed (its input never ended): raise its error
                // now rather than convert the rest of the input for nothing (perf2's S1 on
                // #1434). It has returned, so the join is immediate.
                Err(None) => return Self::fail(ruby, m),
                Err(Some(b)) => {
                    ruby.thread_check_ints()?;
                    batch = Some(b);
                }
            }
        }
    }

    /// The loader's error, once it has stopped early; the load is then finished.
    fn fail(ruby: &Ruby, m: &Mutex<Option<BulkState>>) -> Result<(), Error> {
        let Some(BulkState { tx, worker, .. }) = lock(m).take() else {
            return Err(spent(ruby, LOAD_SPENT));
        };
        drop(tx);
        match join_sliced(ruby, worker)? {
            Err(e) => Err(vec_err(ruby, e)),
            Ok(_) => Err(crate::error::uncoded(
                ruby,
                "Error",
                "the bulk load thread ended before its input did",
            )),
        }
    }

    /// `finish`: ends the load and returns `[vectors, lists]`.
    pub(crate) fn finish(ruby: &Ruby, rb_self: &Self) -> Result<(u64, u32), Error> {
        let state = lock(rb_self.0.get(ruby)?)
            .take()
            .ok_or_else(|| spent(ruby, LOAD_SPENT))?;
        let BulkState { tx, worker, .. } = state;
        drop(tx);
        let stats = join_sliced(ruby, worker)?;
        let s = stats.map_err(|e| vec_err(ruby, e))?;
        Ok((s.vectors, s.lists))
    }
}

/// Joins `worker` in slices with interrupt checks between them. An interrupt doesn't end
/// the join: a bulk load can't stop midway, and returning would leave the loader running
/// detached with its index (ai1249's B1 on #1434, #1425's shape). It's recorded, the join
/// goes on until the loader ends (it has only the batches already sent), and then it's
/// raised.
fn join_sliced<T: Send + 'static>(
    ruby: &Ruby,
    worker: std::thread::JoinHandle<T>,
) -> Result<T, Error> {
    let worker = Mutex::new(Some(worker));
    let mut interrupted: Option<Error> = None;
    loop {
        let w = &worker;
        // `without_gvl` raises an interrupt already pending instead of running: recorded too.
        let done = match without_gvl(ruby, move || {
            let start = Instant::now();
            loop {
                let finished = lock(w)
                    .as_ref()
                    .is_none_or(std::thread::JoinHandle::is_finished);
                if finished || start.elapsed() >= SLICE {
                    return finished;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }) {
            Ok(done) => done,
            Err(e) => {
                interrupted.get_or_insert(e);
                false
            }
        };
        if done {
            let h = lock(&worker).take().expect("present until joined");
            let out = h
                .join()
                .map_err(|_| crate::error::uncoded(ruby, "Error", "the bulk load thread panicked"));
            return match interrupted {
                Some(e) => Err(e),
                None => out,
            };
        }
        if interrupted.is_none()
            && let Err(e) = ruby.thread_check_ints()
        {
            interrupted = Some(e);
        }
    }
}

/// A mutex whose waiters a holder looping in slices gives way to, as `Feed`'s state does
/// (#1411): `std`'s mutex isn't fair, so a thread that locks again right after each slice
/// starves the others. On Linux, `closed?` waited out a whole 3 s `run_once` that way, where
/// macOS happened to let it in (#1434's CI).
pub(crate) struct Fair<T> {
    m: Mutex<T>,
    wanted: std::sync::atomic::AtomicUsize,
}

impl<T> Fair<T> {
    fn new(v: T) -> Self {
        Self {
            m: Mutex::new(v),
            wanted: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// The value, counted in `wanted` until the lock is ours.
    fn acquire(&self) -> std::sync::MutexGuard<'_, T> {
        use std::sync::atomic::Ordering;
        self.wanted.fetch_add(1, Ordering::SeqCst);
        let g = lock(&self.m);
        self.wanted.fetch_sub(1, Ordering::SeqCst);
        g
    }

    /// Waits, with the GVL released, until nobody else wants the lock or `limit` passes.
    fn give_way(&self, ruby: &Ruby, limit: Duration) -> Result<(), Error>
    where
        T: Send,
    {
        use std::sync::atomic::Ordering;
        if self.wanted.load(Ordering::SeqCst) == 0 {
            return Ok(());
        }
        without_gvl(ruby, || {
            let start = Instant::now();
            while self.wanted.load(Ordering::SeqCst) > 0 && start.elapsed() < limit {
                std::thread::sleep(Duration::from_millis(1));
            }
        })
    }
}

/// `[index, rebalancer]`.
fn pair(ruby: &Ruby, i: vidx::Index, r: vidx::Rebalancer) -> Result<RArray, Error> {
    let a = ruby.ary_new_capa(2);
    a.push(VectorIndex(ForkSafe::new(i)))?;
    a.push(Rebalancer(ForkSafe::new(Fair::new(r))))?;
    Ok(a)
}

const PLACED_SPENT: &str = "the Placed was already passed to after_commit";

impl Placed {
    /// A record of one transaction's writes to one index: make it inside the transaction's
    /// block, so a retried attempt starts with a fresh one (ai1249's P1 on #697).
    pub(crate) fn new() -> Self {
        Self(ForkSafe::new(Mutex::new(Some(vidx::Placed::new()))))
    }
}

impl VectorReadIndex {
    fn index(&self, ruby: &Ruby) -> Result<&vidx::ReadIndex, Error> {
        self.0.get(ruby)
    }

    pub(crate) fn spec(ruby: &Ruby, rb_self: &Self) -> Result<VectorSpec, Error> {
        Ok(VectorSpec(rb_self.index(ruby)?.spec()))
    }

    pub(crate) fn search(
        ruby: &Ruby,
        rb_self: &Self,
        query: Value,
        k: usize,
        opts: RArray,
    ) -> Result<RArray, Error> {
        let i = rb_self.index(ruby)?.clone();
        let q = vector_arg(ruby, &i.spec(), query)?;
        let a = VectorSearchArgs::from(ruby, opts)?;
        vector_hits(ruby, vcall(ruby, move || vsearch!(&i, q, k, a))?)
    }
}

impl Rebalancer {
    /// `run_once(budget)`: splits and merges marked lists for up to `budget` seconds (at
    /// least one); whether to call again soon. The GVL is released; one split isn't
    /// interruptible midway.
    pub(crate) fn run_once(ruby: &Ruby, rb_self: &Self, budget: Secs) -> Result<bool, Error> {
        let m = rb_self.0.get(ruby)?;
        // In slices of at most `SLICE`, with an interrupt check between them, so Ctrl-C or
        // `Thread#raise` waits at most a slice (and one split) rather than the whole budget
        // (perf2's S3 on #1434). Each call is whole, so stopping between them is safe.
        // The crate's answer means "it did some work, or work is left", so the call's is
        // whether any slice said so, not the last one's (perf2's R1 on #1434: a call that
        // drained the work in its first slice returned false).
        let deadline = Instant::now().checked_add(budget.0);
        let mut any = false;
        loop {
            let left = deadline.map_or(SLICE, |d| {
                d.saturating_duration_since(Instant::now()).min(SLICE)
            });
            let more = vcall(ruby, move || m.acquire().run_once(left))?;
            any |= more;
            if !more || deadline.is_some_and(|d| Instant::now() >= d) {
                return Ok(any);
            }
            m.give_way(ruby, SLICE)?;
            ruby.thread_check_ints()?;
        }
    }

    /// `wait(timeout)`: until a write marks a list, the index's last handle drops, or
    /// `timeout` seconds pass; in slices with an interrupt check between them.
    pub(crate) fn wait(ruby: &Ruby, rb_self: &Self, timeout: Secs) -> Result<(), Error> {
        let m = rb_self.0.get(ruby)?;
        // A timeout too large for an `Instant` waits without end (perf2's N2 on #1434).
        let deadline = Instant::now().checked_add(timeout.0);
        loop {
            let left = deadline.map_or(SLICE, |d| d.saturating_duration_since(Instant::now()));
            if left.is_zero() {
                return Ok(());
            }
            let slice = left.min(SLICE);
            let woke = without_gvl(ruby, move || {
                let start = Instant::now();
                let mut r = m.acquire();
                r.wait(slice);
                // Woken before the slice ended: a write marked a list, or the index closed.
                start.elapsed() < slice || r.closed()
            })?;
            if woke {
                return Ok(());
            }
            m.give_way(ruby, SLICE)?;
            ruby.thread_check_ints()?;
        }
    }

    /// Whether every index handle is gone. A `run_once` or `wait` on another thread holds
    /// the rebalancer for up to a slice, so the lock is taken with the GVL released (perf2's
    /// S3 on #1434: holding the GVL there stalled every Ruby thread).
    pub(crate) fn is_closed(ruby: &Ruby, rb_self: &Self) -> Result<bool, Error> {
        let m = rb_self.0.get(ruby)?;
        if let Ok(r) = m.m.try_lock() {
            return Ok(r.closed());
        }
        // Counted as wanted, so a looping `run_once` or `wait` lets it in after its slice.
        without_gvl(ruby, move || m.acquire().closed())
    }
}
