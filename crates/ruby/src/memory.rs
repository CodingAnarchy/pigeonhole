//! `Pigeonhole::Memory` (#697 PR 3a, ICR 0111; D311): the `pigeonhole-memory` store, as
//! Python's `pigeonhole.memory` binds it (ICR 0107), with a Ruby block as the consolidator.
//!
//! The option types (`EmbeddingModel`, `Weights`, `Expand`, `ForgetPolicy`,
//! `ConsolidatePolicy`, `NearDedup`, `Memory`) and `MemoryId` are native values holding no
//! Ruby object. What a call returns comes back as Arrays, which
//! `lib/pigeonhole/memory.rb` turns into frozen `Data`, as PR 2's results are.
//!
//! **Lifecycle.** A `Store` holds its handle in a slot that `close` (or leaving its block)
//! empties, and `Database#close` closes the stores opened on it first, so their forgetter
//! threads stop (simp724's P1 on #1340, as in Python). A call already running keeps its own
//! clone of the handle and ends with the database's `Closed` when the database goes.
//!
//! **The consolidator (the owner's O3, the channel pattern).** `consolidate` runs the pass
//! on a Rust thread that holds no Ruby value. For each group, that thread sends the group's
//! memories over a channel and waits for the answer. The Ruby thread waits with the GVL
//! released in 100 ms slices, with an interrupt check between them (as `Feed#next`), calls
//! the block with the GVL held, and sends its memories back. Whatever ends the wait early (an
//! exception from the block, `break`/`throw`/`return` out of it, an interrupt) stops the
//! worker and joins it before the method returns (perf2's C1 on #697). Under a fiber
//! scheduler, `ConsolidateLoop#run_once` waits for the groups through the fiber bridge
//! instead, so the thread's other fibers run meanwhile (#1574 PR 3, `fiber`).

use std::collections::VecDeque;
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Condvar, Mutex, RwLock, Weak};
use std::task::Waker;
use std::time::Duration;

use magnus::{Error, RArray, RHash, RString, Ruby, TryConvert, Value, prelude::*, r_hash::ForEach};
use pigeonhole::ErrorCode;
use pigeonhole_memory as mem;

use crate::classes::{ConsolidateLoop, Watch};
use crate::classes::{
    ConsolidatePolicy, Database, EmbeddingModel, Expand, ForgetPolicy, IvfOptions, Memory,
    MemoryId, NearDedup, ReadStore, Reader, Snapshot, Store, Transaction, Weights,
};
use crate::convert::{Secs, arg_error, class_name, lock, type_error};
use crate::error::{spent, to_rb, with_code};
use crate::fork::ForkSafe;
use crate::gvl::without_gvl;
use crate::read::snapshot_arg;
use crate::vector::{element, element_name, sym_name};

#[cfg(unix)]
#[path = "memory_fiber.rs"]
pub(crate) mod fiber;

/// The longest the Ruby thread waits for the consolidation worker with the GVL released
/// before it checks for an interrupt.
const SLICE: Duration = Duration::from_millis(100);

/// A writable store's handle; `None` once closed.
pub(crate) type StoreSlot = RwLock<Option<mem::Store>>;
/// A reader's store handle; `None` once closed.
pub(crate) type ReadSlot = RwLock<Option<Arc<mem::ReadStore>>>;

/// The stores opened on a database or reader, for its `close` to close first.
pub(crate) type Stores<S> = Mutex<Vec<Weak<S>>>;

fn read<T>(l: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write<T>(l: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Empties the slots in `stores` (their database is closing), dropping the handles with the
/// GVL released: the last handle's drop stops the store's forgetter thread.
pub(crate) fn close_stores<T: Send>(
    ruby: &Ruby,
    stores: Vec<Weak<RwLock<Option<T>>>>,
) -> Result<(), Error> {
    let handles: Vec<T> = stores
        .iter()
        .filter_map(Weak::upgrade)
        .filter_map(|s| write(&s).take())
        .collect();
    without_gvl(ruby, move || drop(handles))
}

/// Records `slot` in `stores`, unless `open` says the database is already closed.
fn adopt<T>(
    ruby: &Ruby,
    stores: &Stores<RwLock<Option<T>>>,
    open: impl FnOnce() -> bool,
    slot: &Arc<RwLock<Option<T>>>,
) -> Result<(), Error> {
    let mut stores = lock(stores);
    if !open() {
        return Err(with_code(ruby, ErrorCode::Closed, "the database is closed"));
    }
    stores.retain(|w| w.strong_count() > 0);
    stores.push(Arc::downgrade(slot));
    Ok(())
}

fn closed(ruby: &Ruby) -> Error {
    with_code(ruby, ErrorCode::Closed, "the memory store is closed")
}

fn memory_module(ruby: &Ruby) -> Result<magnus::RModule, Error> {
    let m: magnus::RModule = ruby.class_object().const_get("Pigeonhole")?;
    m.const_get("Memory")
}

/// A memory crate error as its Ruby exception: the code's class, `ModelMismatch` for a model
/// mismatch.
fn err(ruby: &Ruby, e: mem::Error) -> Error {
    match e {
        mem::Error::Db(e) => to_rb(ruby, &e),
        mem::Error::Mismatch(m) => {
            let make = || -> Result<Error, Error> {
                let class: magnus::ExceptionClass =
                    memory_module(ruby)?.const_get("ModelMismatch")?;
                let e = class.new_instance((
                    format!("embedding model mismatch: {m}"),
                    "InvalidArgument",
                    ErrorCode::InvalidArgument as u32,
                ))?;
                Ok(Error::from(e))
            };
            make().unwrap_or_else(|e| e)
        }
        mem::Error::LoopTakenOver(name) => {
            let make = || -> Result<Error, Error> {
                let class: magnus::ExceptionClass =
                    memory_module(ruby)?.const_get("LoopTakenOver")?;
                let e = class.new_instance((
                    format!(
                        "another consolidation loop named {name:?} took over: stop this one \
                         (don't retry it or make a new one in its place)"
                    ),
                    "Conflict",
                    ErrorCode::Conflict as u32,
                ))?;
                Ok(Error::from(e))
            };
            make().unwrap_or_else(|e| e)
        }
        e => with_code(ruby, e.code(), &e.to_string()),
    }
}

trait OrMem<T> {
    fn or_mem(self, ruby: &Ruby) -> Result<T, Error>;
}

impl<T> OrMem<T> for mem::Result<T> {
    fn or_mem(self, ruby: &Ruby) -> Result<T, Error> {
        self.map_err(|e| err(ruby, e))
    }
}

/// Runs `f` on the memory store's own error type with the GVL released.
fn call<F, R>(ruby: &Ruby, f: F) -> Result<R, Error>
where
    F: FnOnce() -> mem::Result<R> + Send,
    R: Send,
{
    without_gvl(ruby, f)?.or_mem(ruby)
}

/// Runs `f` inside `txn` with the GVL released.
fn in_txn<F, R>(ruby: &Ruby, txn: &Transaction, f: F) -> Result<R, Error>
where
    F: FnOnce(&mut pigeonhole::Transaction) -> mem::Result<R> + Send,
    R: Send,
{
    let m = txn.0.get(ruby)?;
    match without_gvl(ruby, move || lock(m).as_mut().map(f))? {
        Some(r) => r.or_mem(ruby),
        None => Err(spent(ruby, crate::db::TXN_SPENT)),
    }
}

fn invalid(ruby: &Ruby, m: impl Into<String>) -> Error {
    with_code(ruby, ErrorCode::InvalidArgument, &m.into())
}

/// A frozen UTF-8 String.
fn text(ruby: &Ruby, s: &str) -> RString {
    let s = ruby.str_new(s);
    s.freeze();
    s
}

/// A frozen Array of Floats.
fn floats(ruby: &Ruby, v: &[f32]) -> RArray {
    let a = ruby.ary_from_iter(v.iter().map(|x| f64::from(*x)));
    a.freeze();
    a
}

fn frozen_ary(a: RArray) -> RArray {
    a.freeze();
    a
}

// ---- Small types ----------------------------------------------------------------------

impl MemoryId {
    /// Parses the text form (`InvalidArgument` if it isn't one).
    pub(crate) fn new(ruby: &Ruby, s: String) -> Result<Self, Error> {
        s.parse().map(Self).or_mem(ruby)
    }

    /// From the byte form.
    pub(crate) fn from_bytes(ruby: &Ruby, b: crate::convert::Bytes) -> Result<Self, Error> {
        mem::MemoryId::from_bytes(&b.0).map(Self).or_mem(ruby)
    }

    pub(crate) fn to_bytes(ruby: &Ruby, rb_self: &Self) -> RString {
        crate::convert::binary(ruby, &rb_self.0.to_bytes())
    }

    pub(crate) fn namespace(&self) -> String {
        self.0.namespace().to_owned()
    }

    pub(crate) fn created_us(&self) -> u64 {
        self.0.created_us()
    }

    pub(crate) fn to_s(&self) -> String {
        self.0.to_string()
    }

    pub(crate) fn inspect(&self) -> String {
        format!("#<Pigeonhole::Memory::MemoryId {}>", self.0)
    }

    pub(crate) fn eq(&self, other: Value) -> bool {
        <&MemoryId>::try_convert(other).is_ok_and(|o| o.0 == self.0)
    }

    pub(crate) fn hash(&self) -> i64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::hash::DefaultHasher::new();
        self.0.hash(&mut h);
        h.finish().cast_signed()
    }

    /// `<=>`: ids order by namespace, then time; nil for a non-id.
    pub(crate) fn cmp(&self, other: Value) -> Option<i64> {
        <&MemoryId>::try_convert(other)
            .ok()
            .map(|o| match self.0.cmp(&o.0) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
                std::cmp::Ordering::Greater => 1,
            })
    }
}

fn id_value(id: mem::MemoryId) -> MemoryId {
    MemoryId(id)
}

/// A `MemoryId` or its text form.
fn id_arg(ruby: &Ruby, v: Value) -> Result<mem::MemoryId, Error> {
    if let Ok(id) = <&MemoryId>::try_convert(v) {
        return Ok(id.0.clone());
    }
    match RString::from_value(v) {
        Some(s) => s.to_string()?.parse().or_mem(ruby),
        None => Err(type_error(
            ruby,
            format!("expected a MemoryId or String, not {}", class_name(v)),
        )),
    }
}

fn opt_id(ruby: &Ruby, v: Option<Value>) -> Result<Option<mem::MemoryId>, Error> {
    v.filter(|v| !v.is_nil())
        .map(|v| id_arg(ruby, v))
        .transpose()
}

fn ids_arg(ruby: &Ruby, a: RArray) -> Result<Vec<mem::MemoryId>, Error> {
    a.into_iter().map(|v| id_arg(ruby, v)).collect()
}

fn kind(ruby: &Ruby, v: Value) -> Result<mem::Kind, Error> {
    Ok(match sym_name(ruby, v, "a kind")?.as_str() {
        "episodic" => mem::Kind::Episodic,
        "semantic" => mem::Kind::Semantic,
        "procedural" => mem::Kind::Procedural,
        k => {
            return Err(invalid(
                ruby,
                format!("kind :{k}: :episodic, :semantic or :procedural"),
            ));
        }
    })
}

fn kind_name(k: mem::Kind) -> &'static str {
    match k {
        mem::Kind::Episodic => "episodic",
        mem::Kind::Semantic => "semantic",
        mem::Kind::Procedural => "procedural",
        _ => "unknown",
    }
}

/// A link type: an Integer from 1 to 3, or an application's from 16 to 255.
fn link_type(ruby: &Ruby, v: Value) -> Result<mem::LinkType, Error> {
    let bad = || {
        invalid(
            ruby,
            "a link type is 1 to 3, or an application's from 16 to 255",
        )
    };
    if !v.is_kind_of(ruby.class_integer()) {
        return Err(type_error(
            ruby,
            format!("a link type is an Integer, not {}", class_name(v)),
        ));
    }
    let n = i64::try_convert(v).map_err(|_| bad())?;
    match u8::try_from(n).map_err(|_| bad())? {
        1 => Ok(mem::LinkType::DERIVED_FROM),
        2 => Ok(mem::LinkType::SUPERSEDES),
        3 => Ok(mem::LinkType::SIMILAR_TO),
        n => mem::LinkType::custom(n).or_mem(ruby),
    }
}

/// An embedding: an Array of Numerics.
fn embedding(ruby: &Ruby, v: Option<RArray>) -> Result<Option<Vec<f32>>, Error> {
    v.map(|a| {
        a.into_iter()
            .map(|x| {
                if !x.is_kind_of(ruby.class_numeric()) {
                    return Err(type_error(
                        ruby,
                        format!("an embedding holds Numerics, not {}", class_name(x)),
                    ));
                }
                #[allow(clippy::cast_possible_truncation)]
                f64::try_convert(x).map(|f| f as f32)
            })
            .collect()
    })
    .transpose()
}

impl EmbeddingModel {
    /// `new(id, dim, element)`: `element` is `:f32`, `:f16` or `:bf16`.
    pub(crate) fn new(ruby: &Ruby, id: String, dim: u32, el: Value) -> Result<Self, Error> {
        mem::EmbeddingModel::new(id, dim, element(ruby, el)?)
            .map(Self)
            .or_mem(ruby)
    }

    pub(crate) fn id(&self) -> String {
        self.0.id().to_owned()
    }

    pub(crate) fn dim(&self) -> u32 {
        self.0.dim()
    }

    pub(crate) fn element(ruby: &Ruby, rb_self: &Self) -> magnus::Symbol {
        ruby.to_symbol(element_name(rb_self.0.element()))
    }

    fn key(&self) -> (&str, u32, &'static str) {
        (self.0.id(), self.0.dim(), element_name(self.0.element()))
    }

    pub(crate) fn eq(&self, other: Value) -> bool {
        <&EmbeddingModel>::try_convert(other).is_ok_and(|o| o.key() == self.key())
    }

    pub(crate) fn hash(&self) -> i64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::hash::DefaultHasher::new();
        self.key().hash(&mut h);
        h.finish().cast_signed()
    }

    pub(crate) fn inspect(&self) -> String {
        let (id, dim, el) = self.key();
        format!("#<Pigeonhole::Memory::EmbeddingModel id={id:?} dim={dim} element=:{el}>")
    }
}

impl Weights {
    /// `native_new(vector, keyword, recency, importance, uses, graph, consolidated)`, each nil
    /// for its default.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn native_new(
        vector: Option<f64>,
        keyword: Option<f64>,
        recency: Option<f64>,
        importance: Option<f64>,
        uses: Option<f64>,
        graph: Option<f64>,
        consolidated: Option<f64>,
    ) -> Self {
        let mut w = mem::Weights::default();
        for (f, v) in [
            (&mut w.vector, vector),
            (&mut w.keyword, keyword),
            (&mut w.recency, recency),
            (&mut w.importance, importance),
            (&mut w.uses, uses),
            (&mut w.graph, graph),
            (&mut w.consolidated, consolidated),
        ] {
            if let Some(v) = v {
                *f = v;
            }
        }
        Self(w)
    }

    /// `[vector, keyword, recency, importance, uses, graph, consolidated]`.
    pub(crate) fn to_a(&self) -> (f64, f64, f64, f64, f64, f64, f64) {
        let w = &self.0;
        (
            w.vector,
            w.keyword,
            w.recency,
            w.importance,
            w.uses,
            w.graph,
            w.consolidated,
        )
    }

    pub(crate) fn eq(&self, other: Value) -> bool {
        <&Weights>::try_convert(other).is_ok_and(|o| o.0 == self.0)
    }

    pub(crate) fn hash(&self) -> i64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::hash::DefaultHasher::new();
        let (a, b, c, d, e, f, g) = self.to_a();
        [a, b, c, d, e, f, g].map(f64::to_bits).hash(&mut h);
        h.finish().cast_signed()
    }
}

impl Expand {
    /// `native_new(hops, max)`: a negative `hops` or `max` is `InvalidArgument`.
    pub(crate) fn native_new(ruby: &Ruby, hops: i64, max: Option<i64>) -> Result<Self, Error> {
        let hops = u32::try_from(hops).map_err(|_| {
            invalid(
                ruby,
                format!("Expand: hops {hops}, from 1 to {}", mem::Expand::MAX_HOPS),
            )
        })?;
        let mut x = mem::Expand::hops(hops);
        if let Some(m) = max {
            let m = usize::try_from(m)
                .map_err(|_| invalid(ruby, format!("Expand: max {m}, 0 or more")))?;
            x = x.max(m);
        }
        Ok(Self(x))
    }

    pub(crate) fn hops(&self) -> u32 {
        self.0.hops
    }

    pub(crate) fn max(&self) -> usize {
        self.0.max
    }

    pub(crate) fn eq(&self, other: Value) -> bool {
        <&Expand>::try_convert(other).is_ok_and(|o| o.0 == self.0)
    }

    pub(crate) fn hash(&self) -> i64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::hash::DefaultHasher::new();
        (self.0.hops, self.0.max).hash(&mut h);
        h.finish().cast_signed()
    }
}

impl ForgetPolicy {
    /// `native_new(min_retention, max_age, half_life, use_weight, max_per_pass, interval)`:
    /// `max_age` is a Hash of kind to seconds.
    pub(crate) fn native_new(
        ruby: &Ruby,
        min_retention: Option<f64>,
        max_age: Option<RHash>,
        half_life: Option<Secs>,
        use_weight: Option<f64>,
        max_per_pass: Option<usize>,
        interval: Option<Secs>,
    ) -> Result<Self, Error> {
        let mut p = mem::ForgetPolicy::new();
        if let Some(r) = min_retention {
            p = p.min_retention(r);
        }
        if let Some(ages) = max_age {
            let mut pairs = Vec::new();
            ages.foreach(|k: Value, d: Secs| {
                pairs.push((kind(ruby, k)?, d.0));
                Ok(ForEach::Continue)
            })?;
            for (k, d) in pairs {
                p = p.max_age(k, d);
            }
        }
        if let Some(d) = half_life {
            p = p.half_life(d.0);
        }
        if let Some(w) = use_weight {
            p = p.use_weight(w);
        }
        if let Some(n) = max_per_pass {
            p = p.max_per_pass(n);
        }
        if let Some(d) = interval {
            p = p.interval(d.0);
        }
        Ok(Self(p))
    }

    pub(crate) fn min_retention(&self) -> Option<f64> {
        self.0.get_min_retention()
    }

    /// Each kind's maximum age in seconds, for the kinds that have one.
    pub(crate) fn max_age(ruby: &Ruby, rb_self: &Self) -> Result<RHash, Error> {
        let h = ruby.hash_new();
        for k in [
            mem::Kind::Episodic,
            mem::Kind::Semantic,
            mem::Kind::Procedural,
        ] {
            if let Some(d) = rb_self.0.get_max_age(k) {
                h.aset(ruby.to_symbol(kind_name(k)), d.as_secs_f64())?;
            }
        }
        h.freeze();
        Ok(h)
    }

    pub(crate) fn half_life(&self) -> f64 {
        self.0.get_half_life().as_secs_f64()
    }

    pub(crate) fn use_weight(&self) -> f64 {
        self.0.get_use_weight()
    }

    pub(crate) fn max_per_pass(&self) -> usize {
        self.0.get_max_per_pass()
    }

    pub(crate) fn interval(&self) -> f64 {
        self.0.get_interval().as_secs_f64()
    }

    pub(crate) fn eq(&self, other: Value) -> bool {
        <&ForgetPolicy>::try_convert(other).is_ok_and(|o| o.0 == self.0)
    }

    pub(crate) fn inspect(&self) -> String {
        format!("#<Pigeonhole::Memory::ForgetPolicy {:?}>", self.0)
    }
}

fn source_policy(ruby: &Ruby, v: Value) -> Result<mem::SourcePolicy, Error> {
    Ok(match sym_name(ruby, v, "sources")?.as_str() {
        "keep" => mem::SourcePolicy::Keep,
        "delete" => mem::SourcePolicy::Delete,
        "supersede" => mem::SourcePolicy::Supersede,
        s => {
            return Err(invalid(
                ruby,
                format!("sources :{s}: :keep, :delete or :supersede"),
            ));
        }
    })
}

impl ConsolidatePolicy {
    /// `native_new(min_age, group_similarity, min_group, max_group, max_groups, sources)`.
    pub(crate) fn native_new(
        ruby: &Ruby,
        min_age: Option<Secs>,
        group_similarity: Option<f32>,
        min_group: Option<usize>,
        max_group: Option<usize>,
        max_groups: Option<usize>,
        sources: Option<Value>,
    ) -> Result<Self, Error> {
        let mut p = mem::ConsolidatePolicy::new();
        if let Some(d) = min_age {
            p = p.min_age(d.0);
        }
        if let Some(s) = group_similarity {
            p = p.group_similarity(s);
        }
        if let Some(n) = min_group {
            p = p.min_group(n);
        }
        if let Some(n) = max_group {
            p = p.max_group(n);
        }
        if let Some(n) = max_groups {
            p = p.max_groups(n);
        }
        if let Some(s) = sources {
            p = p.sources(source_policy(ruby, s)?);
        }
        Ok(Self(p))
    }

    pub(crate) fn min_age(&self) -> f64 {
        self.0.get_min_age().as_secs_f64()
    }

    pub(crate) fn group_similarity(&self) -> f32 {
        self.0.get_group_similarity()
    }

    pub(crate) fn min_group(&self) -> usize {
        self.0.get_min_group()
    }

    pub(crate) fn max_group(&self) -> usize {
        self.0.get_max_group()
    }

    pub(crate) fn max_groups(&self) -> usize {
        self.0.get_max_groups()
    }

    pub(crate) fn sources(ruby: &Ruby, rb_self: &Self) -> magnus::Symbol {
        ruby.to_symbol(match rb_self.0.get_sources() {
            mem::SourcePolicy::Delete => "delete",
            mem::SourcePolicy::Supersede => "supersede",
            _ => "keep",
        })
    }

    pub(crate) fn eq(&self, other: Value) -> bool {
        <&ConsolidatePolicy>::try_convert(other).is_ok_and(|o| o.0 == self.0)
    }

    pub(crate) fn inspect(&self) -> String {
        format!("#<Pigeonhole::Memory::ConsolidatePolicy {:?}>", self.0)
    }
}

impl NearDedup {
    /// `native_new(similarity, policy)`: `policy` is `:link`, `:skip` or `:merge`.
    pub(crate) fn native_new(ruby: &Ruby, similarity: f32, policy: Value) -> Result<Self, Error> {
        let p = match sym_name(ruby, policy, "policy")?.as_str() {
            "link" => mem::DedupPolicy::Link,
            "skip" => mem::DedupPolicy::Skip,
            "merge" => mem::DedupPolicy::Merge,
            p => {
                return Err(invalid(
                    ruby,
                    format!("policy :{p}: :link, :skip or :merge"),
                ));
            }
        };
        Ok(Self(mem::NearDedup::new(similarity).policy(p)))
    }

    pub(crate) fn similarity(&self) -> f32 {
        self.0.get_similarity()
    }

    fn policy_name(&self) -> &'static str {
        match self.0.get_policy() {
            mem::DedupPolicy::Skip => "skip",
            mem::DedupPolicy::Merge => "merge",
            _ => "link",
        }
    }

    pub(crate) fn policy(ruby: &Ruby, rb_self: &Self) -> magnus::Symbol {
        ruby.to_symbol(rb_self.policy_name())
    }

    pub(crate) fn eq(&self, other: Value) -> bool {
        <&NearDedup>::try_convert(other).is_ok_and(|o| o.0 == self.0)
    }

    pub(crate) fn inspect(&self) -> String {
        format!(
            "#<Pigeonhole::Memory::NearDedup similarity={} policy=:{}>",
            self.0.get_similarity(),
            self.policy_name()
        )
    }
}

impl IvfOptions {
    /// `native_new(min_namespace_len, nprobe, target_list_len)`, each nil for the crate's
    /// default (D311 T8).
    pub(crate) fn native_new(
        min_namespace_len: Option<u64>,
        nprobe: Option<u32>,
        target_list_len: Option<u32>,
    ) -> Self {
        let mut o = mem::IvfOptions::new();
        if let Some(n) = min_namespace_len {
            o = o.min_namespace_len(n);
        }
        if let Some(n) = nprobe {
            o = o.nprobe(n);
        }
        if let Some(n) = target_list_len {
            o = o.target_list_len(n);
        }
        Self(o)
    }

    pub(crate) fn min_namespace_len(&self) -> u64 {
        self.0.get_min_namespace_len()
    }

    pub(crate) fn nprobe(&self) -> Option<u32> {
        self.0.get_nprobe()
    }

    pub(crate) fn target_list_len(&self) -> Option<u32> {
        self.0.get_target_list_len()
    }

    pub(crate) fn eq(&self, other: Value) -> bool {
        <&IvfOptions>::try_convert(other).is_ok_and(|o| o.0 == self.0)
    }

    pub(crate) fn inspect(&self) -> String {
        format!("#<Pigeonhole::Memory::IvfOptions {:?}>", self.0)
    }
}

/// A memory from `remember`'s arguments; `links` is an Array of `[id, link type, weight]`.
#[allow(clippy::too_many_arguments)]
fn build(
    ruby: &Ruby,
    text: String,
    emb: Option<RArray>,
    kind_: Option<Value>,
    importance: Option<f64>,
    source: Option<String>,
    session: Option<String>,
    links: Option<RArray>,
    pinned: bool,
) -> Result<mem::Memory, Error> {
    let mut m = mem::Memory::new(text);
    if let Some(e) = embedding(ruby, emb)? {
        m = m.embedding(e);
    }
    if let Some(k) = kind_ {
        m = m.kind(kind(ruby, k)?);
    }
    if let Some(i) = importance {
        m = m.importance(i);
    }
    if let Some(s) = source {
        m = m.source(s);
    }
    if let Some(s) = session {
        m = m.session(s);
    }
    for l in links.map(RArray::into_iter).into_iter().flatten() {
        let bad = || type_error(ruby, "links: an Array of [id, link type, weight] Arrays");
        let l = RArray::from_value(l).ok_or_else(bad)?;
        if l.len() != 3 {
            return Err(bad());
        }
        let (to, ty, w): (Value, Value, f32) = (l.entry(0)?, l.entry(1)?, l.entry(2)?);
        m = m.link(id_arg(ruby, to)?, link_type(ruby, ty)?, w);
    }
    Ok(m.pinned(pinned))
}

impl Memory {
    /// `native_new(text, embedding, kind, importance, source, session, links, pinned)`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn native_new(
        ruby: &Ruby,
        text: String,
        emb: Option<RArray>,
        kind_: Option<Value>,
        importance: Option<f64>,
        source: Option<String>,
        session: Option<String>,
        links: Option<RArray>,
        pinned: bool,
    ) -> Result<Self, Error> {
        let inner = build(
            ruby,
            text.clone(),
            emb,
            kind_,
            importance,
            source,
            session,
            links,
            pinned,
        )?;
        Ok(Self { text, inner })
    }

    pub(crate) fn text(&self) -> String {
        self.text.clone()
    }

    pub(crate) fn inspect(&self) -> String {
        format!("#<Pigeonhole::Memory::Memory {:?}>", self.text)
    }
}

// ---- Results, as Arrays for memory.rb ----------------------------------------------

/// `[id, outcome]`.
/// `mark_used`'s result: `[marked, [missing id, ...]]`.
fn marked(ruby: &Ruby, m: mem::Marked) -> Result<RArray, Error> {
    let missing = ruby.ary_from_iter(m.missing.into_iter().map(id_value));
    let a = ruby.ary_new_capa(2);
    a.push(m.marked)?;
    a.push(frozen_ary(missing))?;
    Ok(a)
}

fn remembered(ruby: &Ruby, r: mem::Remembered) -> Result<RArray, Error> {
    let outcome = match &r {
        mem::Remembered::New(_) => "new",
        mem::Remembered::Merged(_) => "merged",
        mem::Remembered::Similar(_) => "similar",
        _ => "unknown",
    };
    let a = ruby.ary_new_capa(2);
    a.push(id_value(r.id().clone()))?;
    a.push(ruby.to_symbol(outcome))?;
    Ok(a)
}

/// `[[id, type, weight], ...]`.
fn links_value(ruby: &Ruby, ls: &[mem::Link]) -> Result<RArray, Error> {
    let out = ruby.ary_new_capa(ls.len());
    for l in ls {
        out.push((id_value(l.id.clone()), l.ty.get(), l.weight))?;
    }
    Ok(frozen_ary(out))
}

/// `[id, text, kind, importance, uses, last_used_us, session, pinned, consolidated,
/// superseded_by, sources, links, backlinks, embedding]`.
fn stored(ruby: &Ruby, m: &mem::StoredMemory) -> Result<RArray, Error> {
    let a = ruby.ary_new_capa(15);
    a.push(id_value(m.id.clone()))?;
    a.push(text(ruby, &m.text))?;
    a.push(ruby.to_symbol(kind_name(m.kind)))?;
    a.push(m.importance)?;
    a.push(m.uses)?;
    a.push(m.last_used_us)?;
    a.push(m.session.as_deref().map(|s| text(ruby, s)))?;
    a.push(m.pinned)?;
    a.push(m.consolidated)?;
    a.push(m.superseded_by.clone().map(id_value))?;
    let sources = ruby.ary_new_capa(m.sources.len());
    for s in &m.sources {
        sources.push((text(ruby, &s.name), s.at_us))?;
    }
    a.push(frozen_ary(sources))?;
    a.push(links_value(ruby, &m.links)?)?;
    a.push(links_value(ruby, &m.backlinks)?)?;
    a.push(m.embedding.as_deref().map(|e| floats(ruby, e)))?;
    a.push(m.summary)?;
    Ok(a)
}

fn signal(s: Option<mem::Signal>) -> Option<(u32, f64)> {
    s.map(|s| (s.rank, s.value))
}

/// `[[hit, ...], stats]`, a hit being `[id, text, kind, importance, created_us,
/// superseded_by, score, signals, embedding]` and `signals` `[vector, keyword, recency,
/// importance, uses, graph]` (each nil or `[rank, value]`).
fn hits(
    ruby: &Ruby,
    (hs, stats): (Vec<mem::RecallHit>, mem::RecallStats),
) -> Result<RArray, Error> {
    let out = ruby.ary_new_capa(hs.len());
    for h in hs {
        let a = ruby.ary_new_capa(9);
        a.push(id_value(h.id))?;
        a.push(text(ruby, &h.text))?;
        a.push(ruby.to_symbol(kind_name(h.kind)))?;
        a.push(h.importance)?;
        a.push(h.created_us)?;
        a.push(h.superseded_by.map(id_value))?;
        a.push(h.score)?;
        a.push(h.signals.map(|s| {
            (
                signal(s.vector),
                signal(s.keyword),
                signal(s.recency),
                signal(s.importance),
                signal(s.uses),
                signal(s.graph),
            )
        }))?;
        a.push(h.embedding.as_deref().map(|e| floats(ruby, e)))?;
        out.push(a)?;
    }
    let s = (
        stats.candidates,
        stats.truncated,
        stats.expanded,
        stats.expansion_truncated,
        stats.ivf_namespaces,
        stats.ivf_fallbacks,
    );
    let r = ruby.ary_new_capa(2);
    r.push(out)?;
    r.push(s)?;
    Ok(r)
}

/// `[memories, no_content, stale_dedup, broken_links, missing_embeddings,
/// dangling_superseded, text_index_inconsistent]`.
fn stats(ruby: &Ruby, s: &mem::StoreStats) -> Result<RArray, Error> {
    let a = ruby.ary_new_capa(7);
    a.push(s.memories)?;
    a.push(s.no_content)?;
    a.push(s.stale_dedup)?;
    a.push(s.broken_links)?;
    a.push(s.missing_embeddings)?;
    a.push(s.dangling_superseded)?;
    a.push(s.text_index_inconsistent)?;
    a.push(s.ivf_mismatched)?;
    a.push(s.ivf_counter_drift)?;
    Ok(a)
}

/// `[groups, committed, skipped, created, merged, sources, resume]`.
fn consolidate_pass(ruby: &Ruby, p: mem::ConsolidatePass) -> Result<RArray, Error> {
    let a = ruby.ary_new_capa(7);
    a.push(p.groups)?;
    a.push(p.committed)?;
    a.push(p.skipped)?;
    a.push(p.created)?;
    a.push(p.merged)?;
    a.push(p.sources)?;
    a.push(p.resume.map(id_value))?;
    Ok(a)
}

// ---- Recall ---------------------------------------------------------------------------

/// `recall`'s arguments, converted while the GVL is held.
struct RecallArgs {
    namespaces: Vec<String>,
    text: Option<String>,
    embedding: Option<Vec<f32>>,
    k: Option<usize>,
    candidates: Option<usize>,
    byte_budget: Option<u64>,
    snapshot: Option<pigeonhole::Snapshot>,
    now_us: Option<u64>,
    half_life: Option<Duration>,
    fusion: mem::Fusion,
    weights: Option<mem::Weights>,
    include_superseded: bool,
    session: Option<String>,
    with_signals: bool,
    with_embeddings: bool,
    expand: Option<mem::Expand>,
    exact: bool,
    nprobe: Option<u32>,
}

impl RecallArgs {
    fn run(self, r: mem::Recall<'_>) -> mem::Result<(Vec<mem::RecallHit>, mem::RecallStats)> {
        let mut r = r
            .include_superseded(self.include_superseded)
            .with_signals(self.with_signals)
            .with_embeddings(self.with_embeddings)
            .fusion(self.fusion);
        if let Some(t) = self.text {
            r = r.text(t);
        }
        if let Some(e) = self.embedding {
            r = r.embedding(e);
        }
        if let Some(k) = self.k {
            r = r.k(k);
        }
        if let Some(n) = self.candidates {
            r = r.candidates(n);
        }
        if let Some(b) = self.byte_budget {
            r = r.byte_budget(b);
        }
        if let Some(u) = self.now_us {
            r = r.now_us(u);
        }
        if let Some(d) = self.half_life {
            r = r.half_life(d);
        }
        if let Some(w) = self.weights {
            r = r.weights(w);
        }
        if let Some(x) = self.expand {
            r = r.expand(x);
        }
        if self.exact {
            r = r.exact(true);
        }
        if let Some(n) = self.nprobe {
            r = r.nprobe(n);
        }
        if let Some(s) = self.session {
            r = r.session(s);
        }
        match &self.snapshot {
            Some(s) => r.snapshot(s).run_with_stats(),
            None => r.run_with_stats(),
        }
    }
}

/// `recall`'s 16 arguments, given as one Array (more than a method takes), converted
/// while the GVL is held.
fn recall_args(ruby: &Ruby, a: RArray) -> Result<RecallArgs, Error> {
    if a.len() != 18 {
        return Err(arg_error(ruby, "native_recall takes 18 arguments"));
    }
    let namespaces: Value = a.entry(0)?;
    let namespaces = match RArray::from_value(namespaces) {
        Some(a) => a
            .into_iter()
            .map(String::try_convert)
            .collect::<Result<Vec<_>, _>>()?,
        None => vec![String::try_convert(namespaces)?],
    };
    let fusion: Value = a.entry(9)?;
    let fusion = match sym_name(ruby, fusion, "fusion")?.as_str() {
        "rrf" => mem::Fusion::Rrf,
        "linear" => mem::Fusion::Linear,
        f => return Err(invalid(ruby, format!("fusion :{f}: :rrf or :linear"))),
    };
    let snapshot: Option<Value> = a.entry(6)?;
    let snapshot = snapshot.map(<&Snapshot>::try_convert).transpose()?;
    let weights: Option<Value> = a.entry(10)?;
    let weights = weights
        .map(|w| <&Weights>::try_convert(w).map(|w| w.0))
        .transpose()?;
    let expand: Option<Value> = a.entry(15)?;
    let expand = expand
        .map(|x| <&Expand>::try_convert(x).map(|x| x.0))
        .transpose()?;
    let half_life: Option<Secs> = a.entry(8)?;
    Ok(RecallArgs {
        namespaces,
        text: a.entry(1)?,
        embedding: embedding(ruby, a.entry(2)?)?,
        k: a.entry(3)?,
        candidates: a.entry(4)?,
        byte_budget: a.entry(5)?,
        snapshot: snapshot_arg(ruby, snapshot)?,
        now_us: a.entry(7)?,
        half_life: half_life.map(|d| d.0),
        fusion,
        weights,
        include_superseded: a.entry(11)?,
        session: a.entry(12)?,
        with_signals: a.entry(13)?,
        with_embeddings: a.entry(14)?,
        expand,
        exact: a.entry(16)?,
        nprobe: a.entry(17)?,
    })
}

// ---- The consolidator --------------------------------------------------------------------

/// Why the Ruby side gave no memories for a group: that group's pass stops.
#[derive(Debug)]
struct Stopped;

impl std::fmt::Display for Stopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the consolidator block failed")
    }
}

impl std::error::Error for Stopped {}

/// The Ruby side's answer for one group.
enum Answer {
    /// The block's memories.
    Memories(Vec<mem::Memory>),
    /// The block raised: the group's pass failed.
    Failed,
    /// The group was refused, not answered (an interrupt, or an earlier failure in a
    /// `consolidate`): a `ConsolidateLoop` keeps its namespace due (#1442's
    /// `ConsolidateStopped`).
    Refused,
}

/// One drive's channels: groups to the Ruby thread, answers back.
type Channels = (GroupTx, Receiver<Answer>);

/// The groups the worker sends the Ruby thread: waited for in slices with the GVL released,
/// or, under a fiber scheduler, polled by a fiber op ([`fiber`]). Closed once the worker's
/// sender is dropped, which the worker does as its work ends (or panics).
#[derive(Default)]
pub(crate) struct Groups {
    q: Mutex<GroupQueue>,
    more: Condvar,
}

#[derive(Default)]
struct GroupQueue {
    items: VecDeque<Vec<mem::StoredMemory>>,
    closed: bool,
    /// A fiber op's waker while it waits ([`fiber`]).
    waker: Option<Waker>,
}

/// What a wait for a group found.
enum Recv {
    Group(Vec<mem::StoredMemory>),
    /// Nothing yet.
    Empty,
    /// The worker's sender is gone and every group taken.
    Closed,
}

impl Groups {
    /// The next group, waiting up to `d` for one.
    fn recv_timeout(&self, d: Duration) -> Recv {
        let mut g = lock(&self.q);
        if g.items.is_empty() && !g.closed && !d.is_zero() {
            g = self
                .more
                .wait_timeout(g, d)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        match g.items.pop_front() {
            Some(group) => Recv::Group(group),
            None if g.closed => Recv::Closed,
            None => Recv::Empty,
        }
    }

    /// Waits up to `d` for a group or the close, taking nothing.
    #[cfg_attr(not(unix), allow(dead_code))]
    fn wait_ready(&self, d: Duration) {
        let g = lock(&self.q);
        if g.items.is_empty() && !g.closed {
            let _ = self.more.wait_timeout(g, d);
        }
    }

    /// Whether a group or the close is there to take; if not, `cx`'s waker is kept for the
    /// next send or the close.
    #[cfg_attr(not(unix), allow(dead_code))]
    fn poll_ready(&self, cx: &mut std::task::Context<'_>) -> bool {
        let mut g = lock(&self.q);
        if g.items.is_empty() && !g.closed {
            g.waker = Some(cx.waker().clone());
            return false;
        }
        true
    }

    fn push(&self, item: Option<Vec<mem::StoredMemory>>) {
        let waker = {
            let mut g = lock(&self.q);
            match item {
                Some(group) => g.items.push_back(group),
                None => g.closed = true,
            }
            g.waker.take()
        };
        self.more.notify_all();
        if let Some(w) = waker {
            w.wake();
        }
    }
}

/// The worker's end of [`Groups`]: dropping it closes them.
pub(crate) struct GroupTx(Arc<Groups>);

impl GroupTx {
    fn send(&self, group: Vec<mem::StoredMemory>) {
        self.0.push(Some(group));
    }
}

impl Drop for GroupTx {
    fn drop(&mut self) {
        self.0.push(None);
    }
}

/// Empties a consolidator's slot when dropped: the worker holds one, so its sender goes as
/// its work returns or panics, and the Ruby side sees the groups closed at once.
struct EmptySlot(Arc<SlotConsolidator>);

impl Drop for EmptySlot {
    fn drop(&mut self) {
        let channels = lock(&self.0.slot).take();
        drop(channels);
    }
}

/// The crate's `Consolidator`, answered by the Ruby thread over the channels `drive`
/// installs for each call (a `ConsolidateLoop` keeps one across its steps). With none
/// installed, a group fails.
#[derive(Default)]
pub(crate) struct SlotConsolidator {
    slot: Mutex<Option<Channels>>,
}

impl mem::Consolidator for SlotConsolidator {
    fn consolidate(
        &self,
        group: &[mem::StoredMemory],
    ) -> Result<Vec<mem::Memory>, Box<dyn std::error::Error + Send + Sync>> {
        let slot = lock(&self.slot);
        let Some((to_ruby, answers)) = slot.as_ref() else {
            return Err(Box::new(Stopped));
        };
        to_ruby.send(group.to_vec());
        match answers.recv() {
            Ok(Answer::Memories(memories)) => Ok(memories),
            Ok(Answer::Failed) => Err(Box::new(Stopped)),
            // Refused, or the Ruby side is gone: stopped rather than failed.
            Ok(Answer::Refused) | Err(_) => Err(Box::new(mem::ConsolidateStopped)),
        }
    }
}

/// How `drive` treats a `StandardError` from the block.
#[derive(Clone, Copy, PartialEq, Eq)]
enum OnRaise {
    /// It stops the pass: later groups are refused (`consolidate`).
    StopAll,
    /// It fails that group's namespace only, and later groups still get the block
    /// (`ConsolidateLoop#run_once`, which reports it in `failed`).
    PerGroup,
}

/// What `drive` returns once the worker has ended: its result, the `StandardError`s the
/// block raised (with the namespace of the group each was for), and the jump or interrupt
/// to raise after the caller has tidied up, if any.
struct Driven<R> {
    out: R,
    raised: Vec<(String, Error)>,
    jump: Option<Error>,
}

/// How `drive` waits for the worker's groups.
#[derive(Clone, Copy)]
enum Waiter<'a> {
    /// With the GVL released, in slices with interrupt checks between them.
    Blocking,
    /// Through this thread's fiber bridge (the Ruby `FiberBridge` and its native core), so
    /// the thread's other fibers run meanwhile (#1574 PR 3).
    #[cfg(unix)]
    Fiber(Value, &'a crate::fiber::FiberBridge),
    #[cfg(not(unix))]
    #[allow(dead_code)]
    Never(std::marker::PhantomData<&'a ()>),
}

/// Runs `work` on a thread of its own, answering its consolidator's groups with `block`
/// on this thread, by the owner's channel pattern (O3): the Ruby thread waits with the GVL
/// released in 100 ms slices, with interrupt checks between them (or under a fiber
/// scheduler, through the fiber bridge: `waiter`), and calls the block with the GVL held.
/// However the wait ends, the worker is joined before this returns (ai1249's B1 and perf2's
/// S1 on #1425), so no thread outlives the call.
///
/// `interrupter`, for a loop's step, is tripped as soon as a jump or interrupt is recorded,
/// so the step stops waiting for new memories within about 50 ms instead of at its timeout
/// (perf2's S1 on #1441, #1447's `LoopInterrupter`).
#[allow(clippy::too_many_arguments)]
fn drive<R: Send + 'static>(
    ruby: &Ruby,
    slot: &Arc<SlotConsolidator>,
    block: magnus::block::Proc,
    on_raise: OnRaise,
    name: &str,
    interrupter: Option<&mem::LoopInterrupter>,
    waiter: Waiter<'_>,
    work: impl FnOnce() -> R + Send + 'static,
) -> Result<Driven<R>, Error> {
    let trip = || {
        if let Some(i) = interrupter {
            i.interrupt();
        }
    };
    let groups = Arc::new(Groups::default());
    let (answer_tx, answers) = mpsc::channel::<Answer>();
    *lock(&slot.slot) = Some((GroupTx(Arc::clone(&groups)), answers));
    // The worker empties the slot as soon as its work returns (or panics), dropping the
    // sender, so the wait below sees the groups closed at once rather than at the end of a
    // slice (ai1249's N1 on #1441), and a fiber's wait ends at all.
    let worker_slot = EmptySlot(Arc::clone(slot));
    let run = move || {
        let _empty = worker_slot;
        work()
    };
    let worker = match std::thread::Builder::new().name(name.into()).spawn(run) {
        Ok(w) => w,
        Err(e) => {
            lock(&slot.slot).take();
            return Err(arg_error(
                ruby,
                format!("can't start the {name} thread: {e}"),
            ));
        }
    };
    let mut raised: Vec<(String, Error)> = Vec::new();
    let mut jump: Option<Error> = None;
    #[cfg(unix)]
    let mut reraised = false;
    loop {
        let msg = match waiter {
            Waiter::Blocking => {
                let rx = Arc::clone(&groups);
                // `without_gvl` raises an interrupt already pending instead of waiting:
                // recorded, and the loop keeps draining until the worker ends.
                match without_gvl(ruby, move || rx.recv_timeout(SLICE)) {
                    Ok(msg) => msg,
                    Err(e) => {
                        jump.get_or_insert(e);
                        trip();
                        continue;
                    }
                }
            }
            #[cfg(unix)]
            Waiter::Fiber(bridge, core) => match groups.recv_timeout(Duration::ZERO) {
                Recv::Empty => {
                    // Parks this fiber until a group comes or the worker ends. Raised into
                    // meanwhile (a stop, a timeout), it's recorded as an interrupt is, and
                    // the loop goes on waiting through the bridge until the worker has
                    // ended: no thread outlives the call (§4 of the design note on #1574).
                    // Raised into again after that (a scheduler that re-raises at every
                    // yield), it waits in slices with the GVL released instead, so it can't
                    // spin through the scheduler (cli692's N2 on #1614).
                    let waited = if reraised {
                        let g = Arc::clone(&groups);
                        without_gvl(ruby, move || g.wait_ready(SLICE))
                    } else {
                        fiber::await_groups(ruby, bridge, core, &groups)
                    };
                    if let Err(e) = waited {
                        reraised = jump.is_some();
                        jump.get_or_insert(e);
                        trip();
                    }
                    continue;
                }
                msg => msg,
            },
            #[cfg(not(unix))]
            Waiter::Never(_) => unreachable!(),
        };
        match msg {
            Recv::Group(group) => {
                let refuse = jump.is_some() || (on_raise == OnRaise::StopAll && !raised.is_empty());
                let reply = if refuse {
                    Answer::Refused
                } else {
                    match call_block(ruby, block, &group) {
                        Ok(memories) => Answer::Memories(memories),
                        Err(Early::Raised(e)) => {
                            let ns = group
                                .first()
                                .map(|m| m.id.namespace().to_owned())
                                .unwrap_or_default();
                            raised.push((ns, e));
                            Answer::Failed
                        }
                        Err(Early::Jump(e)) => {
                            jump = Some(e);
                            trip();
                            Answer::Refused
                        }
                    }
                };
                // A send fails only once the worker is gone, which `is_finished` then shows.
                let _ = answer_tx.send(reply);
            }
            Recv::Empty => {
                if worker.is_finished() {
                    break;
                }
                if jump.is_none()
                    && let Err(e) = ruby.thread_check_ints()
                {
                    // The worker is inside the engine; its next group is refused.
                    jump = Some(e);
                    trip();
                }
            }
            Recv::Closed => break,
        }
    }
    // The worker has ended (or is ending): the join doesn't wait on the engine. If an
    // interrupt is pending, `without_gvl` raises it without running the join; the join then
    // runs with the GVL held.
    let worker = Mutex::new(Some(worker));
    let joined = match without_gvl(ruby, || {
        lock(&worker).take().map(std::thread::JoinHandle::join)
    }) {
        Ok(j) => j,
        Err(e) => {
            jump.get_or_insert(e);
            lock(&worker).take().map(std::thread::JoinHandle::join)
        }
    };
    lock(&slot.slot).take();
    match joined {
        Some(Ok(out)) => Ok(Driven { out, raised, jump }),
        _ => Err(crate::error::uncoded(
            ruby,
            "Error",
            &format!("the {name} thread panicked"),
        )),
    }
}

/// The block's answer as memories, or the `TypeError` for anything but an Array of
/// `Memory`. The `TypeError` is a built instance, so `ConsolidateError#cause` can hold it.
fn answer(ruby: &Ruby, out: Value) -> Result<Vec<mem::Memory>, Error> {
    let bad = || {
        let msg = format!(
            "a consolidator block returns an Array of Pigeonhole::Memory::Memory, not {}",
            class_name(out)
        );
        match ruby.exception_type_error().new_instance((msg,)) {
            Ok(e) => Error::from(e),
            Err(e) => e,
        }
    };
    let a = RArray::from_value(out).ok_or_else(bad)?;
    a.into_iter()
        .map(|m| {
            <&Memory>::try_convert(m)
                .map(|m| m.inner.clone())
                .map_err(|_| bad())
        })
        .collect()
}

/// How the Ruby side of a pass ended early.
enum Early {
    /// A `StandardError` from the block (or from converting its answer):
    /// `ConsolidateError`.
    Raised(Error),
    /// A non-local exit (`break`, `throw`, `return`), any other exception (`Interrupt`,
    /// `Timeout`'s), or an interrupt while waiting: re-raised as is.
    Jump(Error),
}

// ---- Store ----------------------------------------------------------------------------

impl Store {
    /// The handle, cloned: a call never holds the slot's lock while it runs (perf2's C4 on
    /// #697), so `close` during a call returns at once, and the call ends with its clone.
    fn handle(&self, ruby: &Ruby) -> Result<mem::Store, Error> {
        read(self.0.get(ruby)?).clone().ok_or_else(|| closed(ruby))
    }

    fn adopt(ruby: &Ruby, db: &Database, s: mem::Store) -> Result<Self, Error> {
        let slot = Arc::new(RwLock::new(Some(s)));
        let state = db.0.get(ruby)?;
        adopt(ruby, &db.1, || read(state).is_some(), &slot)?;
        Ok(Self(ForkSafe::new(slot)))
    }

    /// `native_create(db, name, embedding, forget, episodic_ttl, near_dedup, normalization)`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create(
        ruby: &Ruby,
        db: &Database,
        name: String,
        embedding: Option<&EmbeddingModel>,
        forget: Option<&ForgetPolicy>,
        episodic_ttl: Option<Secs>,
        near_dedup: Option<&NearDedup>,
        normalization: Option<Value>,
        ivf: Option<&IvfOptions>,
    ) -> Result<Self, Error> {
        let mut o = mem::StoreOptions::new();
        if let Some(m) = embedding {
            o = o.embedding(m.0.clone());
        }
        if let Some(p) = forget {
            o = o.forget(p.0.clone());
        }
        if let Some(d) = episodic_ttl {
            o = o.episodic_ttl(d.0);
        }
        if let Some(n) = near_dedup {
            o = o.near_dedup(n.0);
        }
        if let Some(n) = normalization {
            o = o.normalization(normalization_arg(ruby, n)?);
        }
        if let Some(i) = ivf {
            o = o.ivf(i.0);
        }
        let p = db.db(ruby)?;
        let s = call(ruby, move || mem::Store::create(&p, &name, o))?;
        Self::adopt(ruby, db, s)
    }

    /// `native_open(db, name, model)`: `model` nil opens without a model.
    pub(crate) fn open(
        ruby: &Ruby,
        db: &Database,
        name: String,
        model: Option<String>,
    ) -> Result<Self, Error> {
        let p = db.db(ruby)?;
        let s = call(ruby, move || match model {
            Some(m) => mem::Store::open(&p, &name, &m),
            None => mem::Store::open_without_embeddings(&p, &name),
        })?;
        Self::adopt(ruby, db, s)
    }

    /// `native_open_reader(reader, name, model)`.
    pub(crate) fn open_reader(
        ruby: &Ruby,
        reader: &Reader,
        name: String,
        model: Option<String>,
    ) -> Result<ReadStore, Error> {
        let r = reader.db(ruby)?;
        let s = call(ruby, move || match model {
            Some(m) => mem::Store::open_reader(&r, &name, &m),
            None => mem::Store::open_reader_without_embeddings(&r, &name),
        })?;
        let slot = Arc::new(RwLock::new(Some(Arc::new(s))));
        let state = reader.0.get(ruby)?;
        adopt(ruby, &reader.1, || read(state).is_some(), &slot)?;
        Ok(ReadStore(ForkSafe::new(slot)))
    }

    /// Closes this handle; the store's forgetter thread stops once every handle is closed.
    pub(crate) fn close(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        if rb_self.0.forked() {
            return Ok(());
        }
        let s = write(rb_self.0.get(ruby)?).take();
        without_gvl(ruby, move || drop(s))
    }

    pub(crate) fn is_closed(ruby: &Ruby, rb_self: &Self) -> Result<bool, Error> {
        Ok(read(rb_self.0.get(ruby)?).is_none())
    }

    pub(crate) fn embedding_model(
        ruby: &Ruby,
        rb_self: &Self,
    ) -> Result<Option<EmbeddingModel>, Error> {
        Ok(rb_self
            .handle(ruby)?
            .embedding_model()
            .cloned()
            .map(EmbeddingModel))
    }

    /// The store's IVF index options, if it has one (D311 T8).
    pub(crate) fn ivf(ruby: &Ruby, rb_self: &Self) -> Result<Option<IvfOptions>, Error> {
        Ok(rb_self.handle(ruby)?.ivf().map(IvfOptions))
    }

    pub(crate) fn normalization(ruby: &Ruby, rb_self: &Self) -> Result<magnus::Symbol, Error> {
        Ok(ruby.to_symbol(normalization_name(rb_self.handle(ruby)?.normalization())))
    }

    /// `native_remember(ns, text, embedding, kind, importance, source, session, links,
    /// pinned)`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn remember(
        ruby: &Ruby,
        rb_self: &Self,
        ns: String,
        text_: String,
        emb: Option<RArray>,
        kind_: Option<Value>,
        importance: Option<f64>,
        source: Option<String>,
        session: Option<String>,
        links: Option<RArray>,
        pinned: bool,
    ) -> Result<RArray, Error> {
        let m = build(
            ruby, text_, emb, kind_, importance, source, session, links, pinned,
        )?;
        let s = rb_self.handle(ruby)?;
        let r = call(ruby, move || s.remember(&ns, m))?;
        remembered(ruby, r)
    }

    /// `native_remember_in(txn, ns, text, ...)`: as `remember`, inside `txn`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn remember_in(
        ruby: &Ruby,
        rb_self: &Self,
        txn: &Transaction,
        ns: String,
        text_: String,
        emb: Option<RArray>,
        kind_: Option<Value>,
        importance: Option<f64>,
        source: Option<String>,
        session: Option<String>,
        links: Option<RArray>,
        pinned: bool,
    ) -> Result<RArray, Error> {
        let m = build(
            ruby, text_, emb, kind_, importance, source, session, links, pinned,
        )?;
        let s = rb_self.handle(ruby)?;
        let r = in_txn(ruby, txn, move |t| s.remember_in(t, &ns, m))?;
        remembered(ruby, r)
    }

    pub(crate) fn delete(ruby: &Ruby, rb_self: &Self, id: Value) -> Result<bool, Error> {
        let id = id_arg(ruby, id)?;
        let s = rb_self.handle(ruby)?;
        call(ruby, move || s.delete(&id))
    }

    pub(crate) fn delete_in(
        ruby: &Ruby,
        rb_self: &Self,
        txn: &Transaction,
        id: Value,
    ) -> Result<bool, Error> {
        let id = id_arg(ruby, id)?;
        let s = rb_self.handle(ruby)?;
        in_txn(ruby, txn, move |t| s.delete_in(t, &id))
    }

    /// `native_link(from, to, link_type, weight)`.
    pub(crate) fn link(
        ruby: &Ruby,
        rb_self: &Self,
        from: Value,
        to: Value,
        ty: Value,
        weight: f32,
    ) -> Result<(), Error> {
        let (a, b, ty) = (id_arg(ruby, from)?, id_arg(ruby, to)?, link_type(ruby, ty)?);
        let s = rb_self.handle(ruby)?;
        call(ruby, move || s.link(&a, &b, ty, weight))
    }

    pub(crate) fn supersede(
        ruby: &Ruby,
        rb_self: &Self,
        old: Value,
        new: Value,
    ) -> Result<(), Error> {
        let (a, b) = (id_arg(ruby, old)?, id_arg(ruby, new)?);
        let s = rb_self.handle(ruby)?;
        call(ruby, move || s.supersede(&a, &b))
    }

    /// `native_mark_used(ids)`: `[marked, [missing id, ...]]`.
    pub(crate) fn mark_used(ruby: &Ruby, rb_self: &Self, ids: RArray) -> Result<RArray, Error> {
        let ids = ids_arg(ruby, ids)?;
        let s = rb_self.handle(ruby)?;
        let m = call(ruby, move || s.mark_used(&ids))?;
        marked(ruby, m)
    }

    pub(crate) fn get(ruby: &Ruby, rb_self: &Self, id: Value) -> Result<Option<RArray>, Error> {
        let id = id_arg(ruby, id)?;
        let s = rb_self.handle(ruby)?;
        call(ruby, move || s.get(&id))?
            .map(|m| stored(ruby, &m))
            .transpose()
    }

    pub(crate) fn verify(ruby: &Ruby, rb_self: &Self) -> Result<RArray, Error> {
        let s = rb_self.handle(ruby)?;
        stats(ruby, &call(ruby, move || s.verify())?)
    }

    /// `native_recall(namespaces, text, embedding, k, candidates, byte_budget, snapshot,
    /// now_us, half_life, fusion, weights, include_superseded, session, with_signals,
    /// with_embeddings, expand)`: `[[hit, ...], stats]`.
    pub(crate) fn recall(ruby: &Ruby, rb_self: &Self, args: RArray) -> Result<RArray, Error> {
        let a = recall_args(ruby, args)?;
        let s = rb_self.handle(ruby)?;
        let out = call(ruby, move || {
            let ns: Vec<&str> = a.namespaces.iter().map(String::as_str).collect();
            let r = s.recall(&ns);
            a.run(r)
        })?;
        hits(ruby, out)
    }

    /// `native_forget(policy, after, now_us)`: `[examined, deleted, protected, resume]`.
    pub(crate) fn forget(
        ruby: &Ruby,
        rb_self: &Self,
        policy: Option<&ForgetPolicy>,
        after: Option<Value>,
        now_us: Option<u64>,
    ) -> Result<RArray, Error> {
        let policy = policy.map(|p| p.0.clone());
        let after = opt_id(ruby, after)?;
        let s = rb_self.handle(ruby)?;
        let p = call(ruby, move || {
            let mut f = s.forget();
            if let Some(p) = &policy {
                f = f.policy(p);
            }
            if let Some(a) = &after {
                f = f.after(a);
            }
            if let Some(u) = now_us {
                f = f.now_us(u);
            }
            f.run()
        })?;
        let a = ruby.ary_new_capa(4);
        a.push(p.examined)?;
        a.push(p.deleted)?;
        a.push(p.protected)?;
        a.push(p.resume.map(id_value))?;
        Ok(a)
    }

    /// `native_forget_in(txn, id, now_us)`: `:deleted`, `:protected`, `:not_due` or `:gone`.
    pub(crate) fn forget_in(
        ruby: &Ruby,
        rb_self: &Self,
        txn: &Transaction,
        id: Value,
        now_us: u64,
    ) -> Result<magnus::Symbol, Error> {
        let id = id_arg(ruby, id)?;
        let s = rb_self.handle(ruby)?;
        let f = in_txn(ruby, txn, move |t| s.forget_in(t, &id, now_us))?;
        Ok(ruby.to_symbol(match f {
            mem::Forgot::Deleted => "deleted",
            mem::Forgot::Protected => "protected",
            mem::Forgot::NotDue => "not_due",
            mem::Forgot::Gone => "gone",
            _ => "unknown",
        }))
    }

    pub(crate) fn forget_policy(
        ruby: &Ruby,
        rb_self: &Self,
    ) -> Result<Option<ForgetPolicy>, Error> {
        let s = rb_self.handle(ruby)?;
        Ok(call(ruby, move || s.forget_policy())?.map(ForgetPolicy))
    }

    pub(crate) fn set_forget_policy(
        ruby: &Ruby,
        rb_self: &Self,
        policy: Option<&ForgetPolicy>,
    ) -> Result<(), Error> {
        let p = policy.map(|p| p.0.clone());
        let s = rb_self.handle(ruby)?;
        call(ruby, move || s.set_forget_policy(p))
    }

    pub(crate) fn near_dedup(ruby: &Ruby, rb_self: &Self) -> Result<Option<NearDedup>, Error> {
        let s = rb_self.handle(ruby)?;
        Ok(call(ruby, move || s.near_dedup())?.map(NearDedup))
    }

    pub(crate) fn set_near_dedup(
        ruby: &Ruby,
        rb_self: &Self,
        near: Option<&NearDedup>,
    ) -> Result<(), Error> {
        let n = near.map(|n| n.0);
        let s = rb_self.handle(ruby)?;
        call(ruby, move || s.set_near_dedup(n))
    }

    /// `native_near_duplicate(ns, text, embedding)`: `[id, similarity]` or nil.
    pub(crate) fn near_duplicate(
        ruby: &Ruby,
        rb_self: &Self,
        ns: String,
        text_: String,
        emb: Option<RArray>,
    ) -> Result<Option<(MemoryId, f32)>, Error> {
        let mut m = mem::Memory::new(text_);
        if let Some(e) = embedding(ruby, emb)? {
            m = m.embedding(e);
        }
        let s = rb_self.handle(ruby)?;
        Ok(call(ruby, move || s.near_duplicate(&ns, &m))?.map(|(id, sim)| (id_value(id), sim)))
    }

    /// `native_consolidate(ns, policy, after, now_us, consolidator)`: `[pass, nil]` when the
    /// pass ran to its end, `[pass, exception]` when the consolidator raised (memory.rb
    /// raises `ConsolidateError` with it as the cause). A non-local exit from it, or an
    /// interrupt, propagates once the worker has stopped and been joined.
    ///
    /// The consolidator comes as a Proc argument, not a block: an exception this method
    /// builds (`Closed`, say) would otherwise get the block passed to its `initialize`.
    pub(crate) fn consolidate(
        ruby: &Ruby,
        rb_self: &Self,
        ns: String,
        policy: Option<&ConsolidatePolicy>,
        after: Option<Value>,
        now_us: Option<u64>,
        block: magnus::block::Proc,
    ) -> Result<RArray, Error> {
        let policy = policy.map(|p| p.0.clone());
        let after = opt_id(ruby, after)?;
        let s = rb_self.handle(ruby)?;
        let slot = Arc::new(SlotConsolidator::default());
        let consolidator = Arc::clone(&slot);
        let d = drive(
            ruby,
            &slot,
            block,
            OnRaise::StopAll,
            "pigeonhole-consolidate",
            None,
            Waiter::Blocking,
            move || {
                let mut run = s.consolidate(&ns, &*consolidator);
                if let Some(p) = &policy {
                    run = run.policy(p);
                }
                if let Some(a) = &after {
                    run = run.after(a);
                }
                if let Some(u) = now_us {
                    run = run.now_us(u);
                }
                run.run()
            },
        )?;
        if let Some(e) = d.jump {
            return Err(e);
        }
        let pass = match d.out {
            Ok(pass) => pass,
            Err(mem::Error::Consolidate { pass, .. }) if !d.raised.is_empty() => pass,
            Err(e) => return Err(err(ruby, e)),
        };
        let out = ruby.ary_new_capa(2);
        out.push(consolidate_pass(ruby, pass)?)?;
        match d.raised.into_iter().next() {
            Some((_, e)) => out.push(e.value().unwrap_or_else(|| ruby.qnil().as_value()))?,
            None => out.push(ruby.qnil())?,
        }
        Ok(out)
    }
}

/// Calls the block with one group; its answer, or how it ended early.
fn call_block(
    ruby: &Ruby,
    consolidator: magnus::block::Proc,
    group: &[mem::StoredMemory],
) -> Result<Vec<mem::Memory>, Early> {
    let arg = || -> Result<RArray, Error> {
        let a = ruby.ary_new_capa(group.len());
        for m in group {
            a.push(stored(ruby, m)?)?;
        }
        Ok(a)
    };
    let a = arg().map_err(Early::Raised)?;
    // A `StandardError` from the block becomes `ConsolidateError`. Anything else (a
    // non-local exit, `Interrupt`, `Timeout`'s and `Thread#raise`'s interrupts,
    // `SystemExit`) propagates as it is, once the worker has stopped: what `rescue => e`
    // would let through.
    let out: Value = consolidator.call((a,)).map_err(|e| match e.value() {
        Some(v) if v.is_kind_of(ruby.exception_standard_error()) => Early::Raised(e),
        _ => Early::Jump(e),
    })?;
    answer(ruby, out).map_err(Early::Raised)
}

fn normalization_arg(ruby: &Ruby, v: Value) -> Result<mem::Normalization, Error> {
    Ok(match sym_name(ruby, v, "normalization")?.as_str() {
        "nfc" => mem::Normalization::Nfc,
        "nfkc" => mem::Normalization::Nfkc,
        "none" => mem::Normalization::None,
        n => {
            return Err(invalid(
                ruby,
                format!("normalization :{n}: :nfc, :nfkc or :none"),
            ));
        }
    })
}

fn normalization_name(n: mem::Normalization) -> &'static str {
    match n {
        mem::Normalization::Nfc => "nfc",
        mem::Normalization::Nfkc => "nfkc",
        mem::Normalization::None => "none",
        // A setting a later engine adds (ai1249's N2 on #1425).
        _ => "unknown",
    }
}

impl ReadStore {
    fn store(&self, ruby: &Ruby) -> Result<Arc<mem::ReadStore>, Error> {
        read(self.0.get(ruby)?).clone().ok_or_else(|| closed(ruby))
    }

    pub(crate) fn close(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        if rb_self.0.forked() {
            return Ok(());
        }
        let s = write(rb_self.0.get(ruby)?).take();
        without_gvl(ruby, move || drop(s))
    }

    pub(crate) fn is_closed(ruby: &Ruby, rb_self: &Self) -> Result<bool, Error> {
        Ok(read(rb_self.0.get(ruby)?).is_none())
    }

    pub(crate) fn embedding_model(
        ruby: &Ruby,
        rb_self: &Self,
    ) -> Result<Option<EmbeddingModel>, Error> {
        Ok(rb_self
            .store(ruby)?
            .embedding_model()
            .cloned()
            .map(EmbeddingModel))
    }

    /// The store's IVF index options, if it has one (D311 T8).
    pub(crate) fn ivf(ruby: &Ruby, rb_self: &Self) -> Result<Option<IvfOptions>, Error> {
        Ok(rb_self.store(ruby)?.ivf().map(IvfOptions))
    }

    pub(crate) fn normalization(ruby: &Ruby, rb_self: &Self) -> Result<magnus::Symbol, Error> {
        Ok(ruby.to_symbol(normalization_name(rb_self.store(ruby)?.normalization())))
    }

    pub(crate) fn get(ruby: &Ruby, rb_self: &Self, id: Value) -> Result<Option<RArray>, Error> {
        let id = id_arg(ruby, id)?;
        let s = rb_self.store(ruby)?;
        call(ruby, move || s.get(&id))?
            .map(|m| stored(ruby, &m))
            .transpose()
    }

    pub(crate) fn verify(ruby: &Ruby, rb_self: &Self) -> Result<RArray, Error> {
        let s = rb_self.store(ruby)?;
        stats(ruby, &call(ruby, move || s.verify())?)
    }

    pub(crate) fn recall(ruby: &Ruby, rb_self: &Self, args: RArray) -> Result<RArray, Error> {
        let a = recall_args(ruby, args)?;
        let s = rb_self.store(ruby)?;
        let out = call(ruby, move || {
            let ns: Vec<&str> = a.namespaces.iter().map(String::as_str).collect();
            let r = s.recall(&ns);
            a.run(r)
        })?;
        hits(ruby, out)
    }
}

// ---- Watch (T9 part 1) -------------------------------------------------------------------

fn event_kind(e: &mem::MemoryEvent) -> &'static str {
    match e {
        mem::MemoryEvent::Remembered(_) => "remembered",
        mem::MemoryEvent::Summarized(_) => "summarized",
        mem::MemoryEvent::Superseded { .. } => "superseded",
        mem::MemoryEvent::Deleted(_) => "deleted",
        mem::MemoryEvent::Updated(_) => "updated",
        _ => "unknown",
    }
}

/// `[seqno, timestamp_us, [[kind, id, by], ...]]`.
fn memory_commit(ruby: &Ruby, c: &mem::MemoryCommit) -> Result<RArray, Error> {
    let events = ruby.ary_new_capa(c.events.len());
    for e in &c.events {
        let by = match e {
            mem::MemoryEvent::Superseded { by, .. } => Some(id_value(by.clone())),
            _ => None,
        };
        events.push((ruby.to_symbol(event_kind(e)), id_value(e.id().clone()), by))?;
    }
    let a = ruby.ary_new_capa(3);
    a.push(c.seqno)?;
    a.push(c.timestamp_us)?;
    a.push(events)?;
    Ok(a)
}

/// A watch's builder arguments, `[namespaces, from_snapshot, resume, visible]`, applied.
fn open_watch(
    ruby: &Ruby,
    b: impl FnOnce(&[&str]) -> mem::WatchBuilder,
    a: RArray,
) -> Result<Watch, Error> {
    if a.len() != 4 {
        return Err(arg_error(ruby, "native_watch takes 4 arguments"));
    }
    let ns: Vec<String> = a.entry(0)?;
    let snap: Option<Value> = a.entry(1)?;
    let snap = snap.map(<&Snapshot>::try_convert).transpose()?;
    let snap = snapshot_arg(ruby, snap)?;
    let resume: Option<crate::convert::Bytes> = a.entry(2)?;
    let visible: bool = a.entry(3)?;
    let refs: Vec<&str> = ns.iter().map(String::as_str).collect();
    let mut w = b(&refs);
    if let Some(s) = &snap {
        w = w.from_snapshot(s);
    }
    if let Some(r) = resume {
        w = w.resume(&mem::WatchToken::from_bytes(r.0));
    }
    if visible {
        w = w.visible();
    }
    let watch = call(ruby, move || w.open())?;
    Ok(Watch(ForkSafe::new(crate::waits::Waits::new(WatchSource(
        watch,
    )))))
}

impl Store {
    /// `native_watch([namespaces, from_snapshot, resume, visible])`.
    pub(crate) fn watch(ruby: &Ruby, rb_self: &Self, a: RArray) -> Result<Watch, Error> {
        let s = rb_self.handle(ruby)?;
        open_watch(ruby, |ns| s.watch(ns), a)
    }
}

impl ReadStore {
    /// `native_watch([namespaces, from_snapshot, resume, visible])`, in a reader process.
    pub(crate) fn watch(ruby: &Ruby, rb_self: &Self, a: RArray) -> Result<Watch, Error> {
        let s = rb_self.store(ruby)?;
        open_watch(ruby, |ns| s.watch(ns), a)
    }
}

/// A memory watch as a stream's source (`crate::waits`), as the change feed is.
pub(crate) struct WatchSource(mem::Watch);

impl crate::waits::Source for WatchSource {
    type Item = mem::MemoryCommit;
    type Error = mem::Error;
    const NAME: &'static str = "watch";

    fn next(&mut self, d: Duration) -> mem::Result<Option<mem::MemoryCommit>> {
        self.0.next(d)
    }

    fn try_next(&mut self) -> mem::Result<Option<mem::MemoryCommit>> {
        self.0.try_next()
    }

    fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<mem::Result<mem::MemoryCommit>> {
        self.0.poll_next(cx)
    }

    fn token(&self) -> Vec<u8> {
        self.0.token().as_bytes().to_vec()
    }

    fn oldest_retained(&self) -> Option<u64> {
        self.0.oldest_retained()
    }

    fn to_rb(ruby: &Ruby, item: mem::MemoryCommit) -> Result<Value, Error> {
        memory_commit(ruby, &item).map(|a| a.as_value())
    }

    fn error(ruby: &Ruby, e: mem::Error) -> Error {
        err(ruby, e)
    }
}

impl Watch {
    /// `native_next(timeout)`: as `Feed#next`, in slices with an interrupt check between
    /// them, giving way to other threads that want the watch (#1411).
    pub(crate) fn next(
        ruby: &Ruby,
        rb_self: &Self,
        timeout: Option<Secs>,
    ) -> Result<Option<RArray>, Error> {
        rb_self
            .0
            .get(ruby)?
            .wait(ruby, timeout.map(|t| t.0))?
            .map(|c| memory_commit(ruby, &c))
            .transpose()
    }

    pub(crate) fn try_next(ruby: &Ruby, rb_self: &Self) -> Result<Option<RArray>, Error> {
        rb_self
            .0
            .get(ruby)?
            .try_next(ruby)?
            .map(|c| memory_commit(ruby, &c))
            .transpose()
    }

    /// `native_next_op(bridge)`: the next commit with memory events, as a fiber op (#1574
    /// PR 2). The writer process only: in a reader process it raises `Unsupported`.
    #[cfg(unix)]
    pub(crate) fn next_op(
        ruby: &Ruby,
        rb_self: &Self,
        bridge: &crate::fiber::FiberBridge,
    ) -> Result<crate::fiber::FiberOp, Error> {
        rb_self.0.get(ruby)?.next_op(ruby, bridge)
    }

    /// Where the watch is, as bytes, for `Store#watch(resume:)`.
    pub(crate) fn token(ruby: &Ruby, rb_self: &Self) -> Result<RString, Error> {
        let t = rb_self.0.get(ruby)?.token(ruby)?;
        Ok(crate::convert::binary(ruby, &t))
    }

    pub(crate) fn oldest_retained(ruby: &Ruby, rb_self: &Self) -> Result<Option<u64>, Error> {
        rb_self.0.get(ruby)?.oldest_retained(ruby)
    }

    pub(crate) fn close(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        rb_self.0.get(ruby)?.close(ruby)
    }

    pub(crate) fn is_closed(ruby: &Ruby, rb_self: &Self) -> Result<bool, Error> {
        rb_self.0.get(ruby)?.is_closed(ruby)
    }
}

// ---- ConsolidateLoop (T9 part 2) ---------------------------------------------------------

/// A loop and the consolidator its steps answer through; `None` while a step runs on its
/// worker, or once closed.
pub(crate) struct LoopState {
    lp: mem::ConsolidateLoop,
    slot: Arc<SlotConsolidator>,
}

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

impl Store {
    /// `native_consolidate_loop(name, policy, namespaces, clock_offset_us)`.
    pub(crate) fn consolidate_loop(
        ruby: &Ruby,
        rb_self: &Self,
        name: String,
        policy: Option<&ConsolidatePolicy>,
        namespaces: Option<Vec<String>>,
        clock_offset_us: Option<i64>,
    ) -> Result<ConsolidateLoop, Error> {
        let s = rb_self.handle(ruby)?;
        let slot = Arc::new(SlotConsolidator::default());
        let consolidator: Arc<dyn mem::Consolidator> = slot.clone();
        let mut lp = s.consolidate_loop(&name, consolidator);
        if let Some(p) = policy {
            lp = lp.policy(&p.0);
        }
        if let Some(ns) = &namespaces {
            let refs: Vec<&str> = ns.iter().map(String::as_str).collect();
            lp = lp.namespaces(&refs);
        }
        // A Ruby clock can't run inside a step (it's called with the GVL released), so the
        // binding offers a fixed offset from the system clock, enough for tests.
        if let Some(off) = clock_offset_us {
            lp = lp.clock(move || now_us().saturating_add_signed(off));
        }
        Ok(ConsolidateLoop(ForkSafe::new(Arc::new(Mutex::new(
            LoopSlot::Idle(Box::new(LoopState { lp, slot })),
        )))))
    }
}

/// Where a `ConsolidateLoop` is: ready, in a step on another thread, in a fiber's wait for
/// new memories (with its waker, for `close`), closed, or lost to a worker panic (perf2's
/// N1 on #1441: each says why a call can't run).
pub(crate) enum LoopSlot {
    Idle(Box<LoopState>),
    Running,
    #[cfg_attr(not(unix), allow(dead_code))]
    Waiting(Option<Waker>),
    Closed,
    Lost,
}

impl LoopSlot {
    /// The loop, if it's ready; else why it isn't.
    fn take(&mut self, ruby: &Ruby) -> Result<LoopState, Error> {
        match std::mem::replace(self, LoopSlot::Running) {
            LoopSlot::Idle(s) => Ok(*s),
            other => {
                let why = other.why();
                *self = other;
                Err(spent(ruby, why))
            }
        }
    }

    fn why(&self) -> &'static str {
        match self {
            LoopSlot::Running => "a step of this consolidation loop is running on another thread",
            LoopSlot::Waiting(_) => "a fiber's step of this consolidation loop is waiting",
            LoopSlot::Closed => "the consolidation loop is closed",
            LoopSlot::Lost => "the consolidation loop's thread panicked: the loop is gone",
            LoopSlot::Idle(_) => "the consolidation loop is ready",
        }
    }
}

impl ConsolidateLoop {
    /// `native_run_once(timeout, block)`: one step on a worker thread, its consolidator
    /// answered by `block` here (the channel pattern). `[events, [[ns, pass], ...],
    /// [[ns, exception], ...], next_due, token]`. A `StandardError` from the block fails
    /// that namespace only (it's in `failed`); anything else stops the step and is raised.
    pub(crate) fn run_once(
        ruby: &Ruby,
        rb_self: &Self,
        timeout: Secs,
        block: magnus::block::Proc,
    ) -> Result<RArray, Error> {
        Self::step(ruby, rb_self, timeout.0, 0, block, Waiter::Blocking)
    }

    /// A step: reads for up to `timeout`, counting `waited` events a fiber's wait read
    /// before it, then runs the due passes, waiting for their groups as `waiter` says.
    fn step(
        ruby: &Ruby,
        rb_self: &Self,
        timeout: Duration,
        waited: usize,
        block: magnus::block::Proc,
        waiter: Waiter<'_>,
    ) -> Result<RArray, Error> {
        let m = rb_self.0.get(ruby)?;
        let LoopState { mut lp, slot } = lock(m).take(ruby)?;
        let interrupter = lp.interrupter();
        let d = drive(
            ruby,
            &slot,
            block,
            OnRaise::PerGroup,
            "pigeonhole-consolidate-loop",
            Some(&interrupter),
            waiter,
            move || {
                let mut step = lp.run_once(timeout);
                if let Ok(s) = &mut step {
                    s.events += waited;
                }
                (lp, step)
            },
        );
        let d = match d {
            Ok(d) => d,
            // The worker panicked: the loop went with it.
            Err(e) => {
                *lock(m) = LoopSlot::Lost;
                return Err(e);
            }
        };
        let (lp, step) = d.out;
        let state = Box::new(LoopState { lp, slot });
        let closed = {
            let mut g = lock(m);
            if matches!(*g, LoopSlot::Running) {
                *g = LoopSlot::Idle(state);
                None
            } else {
                // A `close` while the step ran leaves the loop closed. The loop (its watch
                // and store handle) is dropped with the GVL released, as `close` drops it
                // (perf2's nit on #1441).
                Some(state)
            }
        };
        // The recorded jump wins over an interrupt the drop's `without_gvl` might raise
        // (ai1249's nit on #1452: "the interrupt takes effect").
        let dropped = if closed.is_some() {
            without_gvl(ruby, move || drop(closed))
        } else {
            Ok(())
        };
        if let Some(e) = d.jump {
            return Err(e);
        }
        dropped?;
        let step = step.or_mem(ruby)?;
        let passes = ruby.ary_new_capa(step.passes.len());
        for (ns, p) in step.passes {
            passes.push((ns, consolidate_pass(ruby, p)?))?;
        }
        let mut raised = d.raised;
        let failed = ruby.ary_new_capa(step.failed.len());
        for (ns, e) in step.failed {
            // The block's own exception for its namespace, when that's why it failed.
            let exc = match raised.iter().position(|(n, _)| *n == ns) {
                Some(i) if matches!(e, mem::Error::Consolidate { .. }) => raised.swap_remove(i).1,
                _ => err(ruby, e),
            };
            failed.push((ns, exc.value().unwrap_or_else(|| ruby.qnil().as_value())))?;
        }
        let stopped = ruby.ary_new_capa(step.stopped.len());
        for (ns, p) in step.stopped {
            stopped.push((ns, consolidate_pass(ruby, p)?))?;
        }
        let a = ruby.ary_new_capa(7);
        a.push(step.events)?;
        a.push(passes)?;
        a.push(failed)?;
        a.push(stopped)?;
        a.push(step.interrupted)?;
        a.push(step.next_due)?;
        a.push(crate::convert::binary(ruby, step.token.as_bytes()))?;
        Ok(a)
    }

    /// The µs time the next pass falls due, if any.
    pub(crate) fn next_due(ruby: &Ruby, rb_self: &Self) -> Result<Option<u64>, Error> {
        match &*lock(rb_self.0.get(ruby)?) {
            LoopSlot::Idle(s) => Ok(s.lp.next_due()),
            other => Err(spent(ruby, other.why())),
        }
    }

    /// Closes the loop. A step running on another thread finishes, and the loop is then
    /// dropped instead of kept.
    pub(crate) fn close(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        let s = {
            let mut g = lock(rb_self.0.get(ruby)?);
            match std::mem::replace(&mut *g, LoopSlot::Closed) {
                LoopSlot::Lost => {
                    *g = LoopSlot::Lost;
                    None
                }
                LoopSlot::Idle(s) => Some(s),
                // A fiber's wait: woken, it finds the loop closed and raises `Closed`
                // (#1606's lesson), dropping the loop itself.
                LoopSlot::Waiting(Some(w)) => {
                    w.wake();
                    None
                }
                _ => None,
            }
        };
        without_gvl(ruby, move || drop(s))
    }
}
