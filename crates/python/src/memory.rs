//! `pigeonhole.memory` (D311 T6, #1340): the `pigeonhole-memory` store, one to one with ICR
//! 0107, plus a consolidator written in Python.
//!
//! The classes are registered in the native submodule `pigeonhole._native._memory`, which
//! `pigeonhole/memory.py` re-exports. Every store call runs with the GIL released; a Python
//! consolidator takes it back only for its own call.
//!
//! **Lifecycle (simp724's P1 on #1340).** A `Store` holds its handle in a slot that
//! `Store.close()` (or leaving its `with` block) empties: the store's forgetter thread stops
//! within its current slice once the last handle drops. `Database.close()` closes the stores
//! opened on it first. Afterwards every call raises `pigeonhole.Closed`, and a call already
//! running ends with the database's `Closed` (a forget or consolidation's open transaction is
//! dropped whole).

use std::sync::{Arc, RwLock};

use pigeonhole::ErrorCode;
use pigeonhole_memory as mem;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::convert::duration;
use crate::db::{Database, Reader, Transaction};
use crate::error::{InvalidArgument, MergeFailed, to_py, with_code};
use crate::read::{Snapshot, snapshot_arg};
use crate::vector::{element, element_name};

pyo3::create_exception!(
    pigeonhole.memory,
    ModelMismatch,
    InvalidArgument,
    "The store is pinned to another embedding model than the one named (`InvalidArgument`)."
);

pyo3::create_exception!(
    pigeonhole.memory,
    ConsolidateError,
    MergeFailed,
    "The consolidator failed, which stopped the pass (`MergeFailed`): `consolidated` is the \
     pass so far, whose groups stay committed, and `__cause__` the consolidator's exception."
);

/// A writable store's handle; `None` once closed.
pub(crate) type StoreSlot = RwLock<Option<mem::Store>>;
/// A reader's store handle; `None` once closed.
pub(crate) type ReadSlot = RwLock<Option<Arc<mem::ReadStore>>>;

/// Closes a store's slot (its database is closing).
pub(crate) fn close_slot(s: &StoreSlot) {
    s.write().expect("not poisoned").take();
}

fn closed() -> PyErr {
    with_code(ErrorCode::Closed, "the memory store is closed")
}

/// A memory crate error as its Python exception: the code's class, `ModelMismatch` for a
/// model mismatch.
fn err(e: mem::Error) -> PyErr {
    match e {
        mem::Error::Db(e) => to_py(e),
        mem::Error::Mismatch(m) => Python::attach(|py| {
            let e = PyErr::new::<ModelMismatch, _>(format!("embedding model mismatch: {m}"));
            let v = e.value(py);
            let _ = v.setattr("code", "InvalidArgument");
            let _ = v.setattr("code_num", ErrorCode::InvalidArgument as u32);
            e
        }),
        e => with_code(e.code(), &e.to_string()),
    }
}

trait OrMem<T> {
    fn or_mem(self) -> PyResult<T>;
}

impl<T> OrMem<T> for mem::Result<T> {
    fn or_mem(self) -> PyResult<T> {
        self.map_err(err)
    }
}

fn invalid(m: impl Into<String>) -> PyErr {
    with_code(ErrorCode::InvalidArgument, &m.into())
}

// ---- Small types ----------------------------------------------------------------------

/// A memory's id: its text form is RFC 4648 base32 of the byte form (ICR 0107).
#[pyclass(module = "pigeonhole.memory", frozen, eq, hash, ord)]
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct MemoryId(pub(crate) mem::MemoryId);

#[pymethods]
impl MemoryId {
    /// Parses the text form (`InvalidArgument` if it isn't one).
    #[new]
    fn new(s: &str) -> PyResult<Self> {
        s.parse().map(Self).or_mem()
    }

    /// From the byte form (`InvalidArgument` if it isn't one).
    #[staticmethod]
    fn from_bytes(b: &[u8]) -> PyResult<Self> {
        mem::MemoryId::from_bytes(b).map(Self).or_mem()
    }

    /// The byte form.
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    /// Its namespace.
    #[getter]
    fn namespace(&self) -> &str {
        self.0.namespace()
    }

    /// When it was made, in µs since the Unix epoch.
    #[getter]
    fn created_us(&self) -> u64 {
        self.0.created_us()
    }

    fn __str__(&self) -> String {
        self.0.to_string()
    }

    fn __repr__(&self) -> String {
        format!("MemoryId('{}')", self.0)
    }
}

/// A `MemoryId` or its text form.
fn id_arg(ob: &Bound<'_, PyAny>) -> PyResult<mem::MemoryId> {
    if let Ok(id) = ob.cast::<MemoryId>() {
        return Ok(id.get().0.clone());
    }
    match ob.extract::<String>() {
        Ok(s) => s.parse().or_mem(),
        Err(_) => Err(PyTypeError::new_err(format!(
            "expected a MemoryId or str, not {}",
            ob.get_type().name()?
        ))),
    }
}

fn ids_arg(ob: &Bound<'_, PyAny>) -> PyResult<Vec<mem::MemoryId>> {
    if ob.is_instance_of::<pyo3::types::PyString>() {
        return Err(PyTypeError::new_err("expected a list of ids, not a str"));
    }
    ob.try_iter()?.map(|x| id_arg(&x?)).collect()
}

fn kind(s: &str) -> PyResult<mem::Kind> {
    Ok(match s {
        "episodic" => mem::Kind::Episodic,
        "semantic" => mem::Kind::Semantic,
        "procedural" => mem::Kind::Procedural,
        _ => {
            return Err(invalid(format!(
                "kind {s:?}: episodic, semantic or procedural"
            )));
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

fn link_type(n: u8) -> PyResult<mem::LinkType> {
    match n {
        1 => Ok(mem::LinkType::DERIVED_FROM),
        2 => Ok(mem::LinkType::SUPERSEDES),
        3 => Ok(mem::LinkType::SIMILAR_TO),
        n => mem::LinkType::custom(n).or_mem(),
    }
}

/// The embedding model a store is pinned to.
#[pyclass(module = "pigeonhole.memory", frozen)]
#[derive(Clone)]
pub(crate) struct EmbeddingModel(mem::EmbeddingModel);

#[pymethods]
impl EmbeddingModel {
    /// `id` (non-empty), `dim`, and `element`: f32, f16 or bf16 (`pigeonhole.Element` or its
    /// string).
    #[new]
    fn new(id: String, dim: u32, element_: &Bound<'_, PyAny>) -> PyResult<Self> {
        mem::EmbeddingModel::new(id, dim, element(element_)?)
            .map(Self)
            .or_mem()
    }

    #[getter]
    fn id(&self) -> &str {
        self.0.id()
    }

    #[getter]
    fn dim(&self) -> u32 {
        self.0.dim()
    }

    #[getter]
    fn element(&self) -> &'static str {
        element_name(self.0.element())
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.0.id() == other.0.id()
            && self.0.dim() == other.0.dim()
            && self.0.element() == other.0.element()
    }

    fn __repr__(&self) -> String {
        format!(
            "EmbeddingModel({:?}, {}, {:?})",
            self.0.id(),
            self.0.dim(),
            element_name(self.0.element())
        )
    }
}

/// Recall's signal weights (every one 1 by default, `consolidated` 0.5).
#[pyclass(module = "pigeonhole.memory", frozen)]
#[derive(Clone)]
pub(crate) struct Weights(mem::Weights);

#[pymethods]
impl Weights {
    #[new]
    #[pyo3(signature = (*, vector=None, keyword=None, recency=None, importance=None, uses=None, consolidated=None))]
    fn new(
        vector: Option<f64>,
        keyword: Option<f64>,
        recency: Option<f64>,
        importance: Option<f64>,
        uses: Option<f64>,
        consolidated: Option<f64>,
    ) -> Self {
        let mut w = mem::Weights::default();
        let set = |f: &mut f64, v: Option<f64>| {
            if let Some(v) = v {
                *f = v;
            }
        };
        set(&mut w.vector, vector);
        set(&mut w.keyword, keyword);
        set(&mut w.recency, recency);
        set(&mut w.importance, importance);
        set(&mut w.uses, uses);
        set(&mut w.consolidated, consolidated);
        Self(w)
    }

    #[getter]
    fn vector(&self) -> f64 {
        self.0.vector
    }
    #[getter]
    fn keyword(&self) -> f64 {
        self.0.keyword
    }
    #[getter]
    fn recency(&self) -> f64 {
        self.0.recency
    }
    #[getter]
    fn importance(&self) -> f64 {
        self.0.importance
    }
    #[getter]
    fn uses(&self) -> f64 {
        self.0.uses
    }
    #[getter]
    fn consolidated(&self) -> f64 {
        self.0.consolidated
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    fn __repr__(&self) -> String {
        let w = &self.0;
        format!(
            "Weights(vector={}, keyword={}, recency={}, importance={}, uses={}, consolidated={})",
            w.vector, w.keyword, w.recency, w.importance, w.uses, w.consolidated
        )
    }
}

/// A store's forget policy (T2). Built from keywords; the defaults are the crate's.
#[pyclass(module = "pigeonhole.memory", frozen)]
#[derive(Clone)]
pub(crate) struct ForgetPolicy(mem::ForgetPolicy);

#[pymethods]
impl ForgetPolicy {
    /// `max_age` maps a kind (`Kind` or its string) to a duration.
    #[new]
    #[pyo3(signature = (*, min_retention=None, max_age=None, half_life=None, use_weight=None, max_per_pass=None, interval=None))]
    fn new(
        min_retention: Option<f64>,
        max_age: Option<&Bound<'_, PyDict>>,
        half_life: Option<&Bound<'_, PyAny>>,
        use_weight: Option<f64>,
        max_per_pass: Option<usize>,
        interval: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let mut p = mem::ForgetPolicy::new();
        if let Some(r) = min_retention {
            p = p.min_retention(r);
        }
        if let Some(ages) = max_age {
            for (k, d) in ages.iter() {
                p = p.max_age(kind(&k.extract::<String>()?)?, duration(&d)?);
            }
        }
        if let Some(d) = half_life {
            p = p.half_life(duration(d)?);
        }
        if let Some(w) = use_weight {
            p = p.use_weight(w);
        }
        if let Some(n) = max_per_pass {
            p = p.max_per_pass(n);
        }
        if let Some(d) = interval {
            p = p.interval(duration(d)?);
        }
        Ok(Self(p))
    }

    #[getter]
    fn min_retention(&self) -> Option<f64> {
        self.0.get_min_retention()
    }

    /// Each kind's maximum age, for the kinds that have one.
    #[getter]
    fn max_age(&self) -> Vec<(&'static str, std::time::Duration)> {
        [
            mem::Kind::Episodic,
            mem::Kind::Semantic,
            mem::Kind::Procedural,
        ]
        .into_iter()
        .filter_map(|k| self.0.get_max_age(k).map(|d| (kind_name(k), d)))
        .collect()
    }

    #[getter]
    fn half_life(&self) -> std::time::Duration {
        self.0.get_half_life()
    }
    #[getter]
    fn use_weight(&self) -> f64 {
        self.0.get_use_weight()
    }
    #[getter]
    fn max_per_pass(&self) -> usize {
        self.0.get_max_per_pass()
    }
    #[getter]
    fn interval(&self) -> std::time::Duration {
        self.0.get_interval()
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    fn __repr__(&self) -> String {
        format!("ForgetPolicy({:?})", self.0)
    }
}

/// How `Store.consolidate` picks and folds groups (T3).
#[pyclass(module = "pigeonhole.memory", frozen)]
#[derive(Clone)]
pub(crate) struct ConsolidatePolicy(mem::ConsolidatePolicy);

fn source_policy(s: &str) -> PyResult<mem::SourcePolicy> {
    Ok(match s {
        "keep" => mem::SourcePolicy::Keep,
        "delete" => mem::SourcePolicy::Delete,
        "supersede" => mem::SourcePolicy::Supersede,
        _ => return Err(invalid(format!("sources {s:?}: keep, delete or supersede"))),
    })
}

#[pymethods]
impl ConsolidatePolicy {
    #[new]
    #[pyo3(signature = (*, min_age=None, group_similarity=None, min_group=None, max_group=None, max_groups=None, sources=None))]
    fn new(
        min_age: Option<&Bound<'_, PyAny>>,
        group_similarity: Option<f32>,
        min_group: Option<usize>,
        max_group: Option<usize>,
        max_groups: Option<usize>,
        sources: Option<&str>,
    ) -> PyResult<Self> {
        let mut p = mem::ConsolidatePolicy::new();
        if let Some(d) = min_age {
            p = p.min_age(duration(d)?);
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
            p = p.sources(source_policy(s)?);
        }
        Ok(Self(p))
    }

    #[getter]
    fn min_age(&self) -> std::time::Duration {
        self.0.get_min_age()
    }
    #[getter]
    fn group_similarity(&self) -> f32 {
        self.0.get_group_similarity()
    }
    #[getter]
    fn min_group(&self) -> usize {
        self.0.get_min_group()
    }
    #[getter]
    fn max_group(&self) -> usize {
        self.0.get_max_group()
    }
    #[getter]
    fn max_groups(&self) -> usize {
        self.0.get_max_groups()
    }
    #[getter]
    fn sources(&self) -> &'static str {
        match self.0.get_sources() {
            mem::SourcePolicy::Delete => "delete",
            mem::SourcePolicy::Supersede => "supersede",
            _ => "keep",
        }
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    fn __repr__(&self) -> String {
        format!("ConsolidatePolicy({:?})", self.0)
    }
}

/// A store's near-duplicate dedup (T3).
#[pyclass(module = "pigeonhole.memory", frozen)]
#[derive(Clone)]
pub(crate) struct NearDedup(mem::NearDedup);

#[pymethods]
impl NearDedup {
    /// Near duplicates at `similarity` (cosine, above 0 and at most 1) or above; `policy` is
    /// `"link"` (default), `"skip"` or `"merge"`.
    #[new]
    #[pyo3(signature = (similarity, *, policy="link"))]
    fn new(similarity: f32, policy: &str) -> PyResult<Self> {
        let p = match policy {
            "link" => mem::DedupPolicy::Link,
            "skip" => mem::DedupPolicy::Skip,
            "merge" => mem::DedupPolicy::Merge,
            _ => return Err(invalid(format!("policy {policy:?}: link, skip or merge"))),
        };
        Ok(Self(mem::NearDedup::new(similarity).policy(p)))
    }

    #[getter]
    fn similarity(&self) -> f32 {
        self.0.get_similarity()
    }

    #[getter]
    fn policy(&self) -> &'static str {
        match self.0.get_policy() {
            mem::DedupPolicy::Skip => "skip",
            mem::DedupPolicy::Merge => "merge",
            _ => "link",
        }
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    fn __repr__(&self) -> String {
        format!(
            "NearDedup({}, policy={:?})",
            self.0.get_similarity(),
            self.policy()
        )
    }
}

/// A memory to write: what a consolidator returns (`Store.remember` takes the same keywords).
#[pyclass(module = "pigeonhole.memory", frozen)]
#[derive(Clone)]
pub(crate) struct Memory {
    text: String,
    inner: mem::Memory,
}

/// Builds a memory from `remember`'s keywords.
#[allow(clippy::too_many_arguments)]
fn build(
    text: String,
    embedding: Option<Vec<f32>>,
    kind_: Option<&str>,
    importance: Option<f64>,
    source: Option<String>,
    session: Option<String>,
    links: Option<&Bound<'_, PyAny>>,
    pinned: bool,
) -> PyResult<mem::Memory> {
    let mut m = mem::Memory::new(text);
    if let Some(e) = embedding {
        m = m.embedding(e);
    }
    if let Some(k) = kind_ {
        m = m.kind(kind(k)?);
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
    if let Some(ls) = links {
        for l in ls.try_iter()? {
            let l = l?;
            let (to, ty, w): (Bound<'_, PyAny>, u8, f32) = l.extract().map_err(|_| {
                PyTypeError::new_err("links: a list of (id, link type, weight) tuples")
            })?;
            m = m.link(id_arg(&to)?, link_type(ty)?, w);
        }
    }
    Ok(m.pinned(pinned))
}

#[pymethods]
impl Memory {
    #[new]
    #[pyo3(signature = (text, *, embedding=None, kind=None, importance=None, source=None, session=None, links=None, pinned=false))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        text: String,
        embedding: Option<Vec<f32>>,
        kind: Option<&str>,
        importance: Option<f64>,
        source: Option<String>,
        session: Option<String>,
        links: Option<&Bound<'_, PyAny>>,
        pinned: bool,
    ) -> PyResult<Self> {
        let inner = build(
            text.clone(),
            embedding,
            kind,
            importance,
            source,
            session,
            links,
            pinned,
        )?;
        Ok(Self { text, inner })
    }

    #[getter]
    fn text(&self) -> &str {
        &self.text
    }

    fn __repr__(&self) -> String {
        format!("Memory({:?})", self.text)
    }
}

/// What `remember` did: `outcome` is `"new"`, `"merged"` (an exact duplicate) or
/// `"similar"` (a near duplicate under `NearDedup(policy="skip")`: nothing written, and `id`
/// is the match).
#[pyclass(module = "pigeonhole.memory", frozen, get_all)]
pub(crate) struct Remembered {
    id: MemoryId,
    outcome: &'static str,
}

impl From<mem::Remembered> for Remembered {
    fn from(r: mem::Remembered) -> Self {
        let outcome = match &r {
            mem::Remembered::New(_) => "new",
            mem::Remembered::Merged(_) => "merged",
            mem::Remembered::Similar(_) => "similar",
            _ => "unknown",
        };
        Self {
            id: MemoryId(r.id().clone()),
            outcome,
        }
    }
}

#[pymethods]
impl Remembered {
    fn __repr__(&self) -> String {
        format!("Remembered(id='{}', outcome={:?})", self.id.0, self.outcome)
    }
}

/// What `mark_used` did: `missing` are ids that no longer exist.
#[pyclass(module = "pigeonhole.memory", frozen, get_all)]
pub(crate) struct Marked {
    marked: usize,
    missing: Vec<MemoryId>,
}

/// A link: the linked memory, its type (an int) and weight.
#[pyclass(module = "pigeonhole.memory", frozen, get_all)]
#[derive(Clone)]
pub(crate) struct Link {
    id: MemoryId,
    #[pyo3(name = "type")]
    ty: u8,
    weight: f32,
}

impl From<&mem::Link> for Link {
    fn from(l: &mem::Link) -> Self {
        Self {
            id: MemoryId(l.id.clone()),
            ty: l.ty.get(),
            weight: l.weight,
        }
    }
}

/// Where a memory came from, and when it was first seen (µs).
#[pyclass(module = "pigeonhole.memory", frozen, get_all)]
#[derive(Clone)]
pub(crate) struct Source {
    name: String,
    at_us: u64,
}

/// A stored memory, as `get` reads it.
#[pyclass(module = "pigeonhole.memory", frozen, get_all)]
pub(crate) struct StoredMemory {
    id: MemoryId,
    text: String,
    kind: &'static str,
    importance: f64,
    uses: u64,
    last_used_us: Option<u64>,
    session: Option<String>,
    pinned: bool,
    consolidated: bool,
    superseded_by: Option<MemoryId>,
    sources: Vec<Source>,
    links: Vec<Link>,
    backlinks: Vec<Link>,
    embedding: Option<Vec<f32>>,
}

impl From<mem::StoredMemory> for StoredMemory {
    fn from(m: mem::StoredMemory) -> Self {
        Self {
            id: MemoryId(m.id),
            text: m.text,
            kind: kind_name(m.kind),
            importance: m.importance,
            uses: m.uses,
            last_used_us: m.last_used_us,
            session: m.session,
            pinned: m.pinned,
            consolidated: m.consolidated,
            superseded_by: m.superseded_by.map(MemoryId),
            sources: m
                .sources
                .iter()
                .map(|s| Source {
                    name: s.name.clone(),
                    at_us: s.at_us,
                })
                .collect(),
            links: m.links.iter().map(Link::from).collect(),
            backlinks: m.backlinks.iter().map(Link::from).collect(),
            embedding: m.embedding,
        }
    }
}

#[pymethods]
impl StoredMemory {
    fn __repr__(&self) -> String {
        format!("StoredMemory(id='{}', text={:?})", self.id.0, self.text)
    }
}

/// `verify`'s counts.
#[pyclass(module = "pigeonhole.memory", frozen, get_all)]
pub(crate) struct StoreStats {
    memories: u64,
    no_content: u64,
    stale_dedup: u64,
    broken_links: u64,
    missing_embeddings: u64,
    dangling_superseded: u64,
    text_index_inconsistent: bool,
}

#[pymethods]
impl StoreStats {
    /// Whether every count of a problem is zero.
    fn is_consistent(&self) -> bool {
        self.no_content == 0
            && self.stale_dedup == 0
            && self.broken_links == 0
            && self.missing_embeddings == 0
            && self.dangling_superseded == 0
            && !self.text_index_inconsistent
    }
}

impl From<mem::StoreStats> for StoreStats {
    fn from(s: mem::StoreStats) -> Self {
        Self {
            memories: s.memories,
            no_content: s.no_content,
            stale_dedup: s.stale_dedup,
            broken_links: s.broken_links,
            missing_embeddings: s.missing_embeddings,
            dangling_superseded: s.dangling_superseded,
            text_index_inconsistent: s.text_index_inconsistent,
        }
    }
}

/// One signal of a hit: its rank among the candidates that have it (1 is best) and value.
#[pyclass(module = "pigeonhole.memory", frozen, get_all)]
#[derive(Clone)]
pub(crate) struct Signal {
    rank: u32,
    value: f64,
}

/// A hit's signals (`recall(with_signals=True)`): each `None` when the hit lacks it.
#[pyclass(module = "pigeonhole.memory", frozen, get_all)]
pub(crate) struct Signals {
    vector: Option<Signal>,
    keyword: Option<Signal>,
    recency: Option<Signal>,
    importance: Option<Signal>,
    uses: Option<Signal>,
}

fn signal(s: Option<mem::Signal>) -> Option<Signal> {
    s.map(|s| Signal {
        rank: s.rank,
        value: s.value,
    })
}

/// One recalled memory.
#[pyclass(module = "pigeonhole.memory", frozen, get_all)]
pub(crate) struct RecallHit {
    id: MemoryId,
    text: String,
    kind: &'static str,
    importance: f64,
    created_us: u64,
    superseded_by: Option<MemoryId>,
    score: f64,
    signals: Option<Py<Signals>>,
    embedding: Option<Vec<f32>>,
}

#[pymethods]
impl RecallHit {
    fn __repr__(&self) -> String {
        format!(
            "RecallHit(id='{}', score={}, text={:?})",
            self.id.0, self.score, self.text
        )
    }
}

/// What a recall did: `candidates` scored, and whether the byte budget cut the hits.
#[pyclass(module = "pigeonhole.memory", frozen, get_all)]
pub(crate) struct RecallStats {
    candidates: usize,
    truncated: bool,
}

/// A forget pass's counts, and where to resume (`Store.forget(after=...)`).
#[pyclass(module = "pigeonhole.memory", frozen, get_all)]
pub(crate) struct ForgetPass {
    examined: u64,
    deleted: u64,
    protected: u64,
    resume: Option<MemoryId>,
}

/// A consolidation pass's counts, and where to resume (`Store.consolidate(after=...)`).
#[pyclass(module = "pigeonhole.memory", frozen, get_all)]
pub(crate) struct ConsolidatePass {
    groups: u64,
    committed: u64,
    skipped: u64,
    created: u64,
    merged: u64,
    sources: u64,
    resume: Option<MemoryId>,
}

impl From<mem::ConsolidatePass> for ConsolidatePass {
    fn from(p: mem::ConsolidatePass) -> Self {
        Self {
            groups: p.groups,
            committed: p.committed,
            skipped: p.skipped,
            created: p.created,
            merged: p.merged,
            sources: p.sources,
            resume: p.resume.map(MemoryId),
        }
    }
}

// ---- Recall -------------------------------------------------------------------------

/// `recall`'s keywords, converted before the GIL is released.
struct RecallArgs {
    namespaces: Vec<String>,
    text: Option<String>,
    embedding: Option<Vec<f32>>,
    k: Option<usize>,
    candidates: Option<usize>,
    byte_budget: Option<u64>,
    snapshot: Option<pigeonhole::Snapshot>,
    now_us: Option<u64>,
    half_life: Option<std::time::Duration>,
    fusion: Option<mem::Fusion>,
    weights: Option<mem::Weights>,
    include_superseded: bool,
    session: Option<String>,
    with_signals: bool,
    with_embeddings: bool,
}

impl RecallArgs {
    fn run(self, r: mem::Recall<'_>) -> mem::Result<(Vec<mem::RecallHit>, mem::RecallStats)> {
        let mut r = r
            .include_superseded(self.include_superseded)
            .with_signals(self.with_signals)
            .with_embeddings(self.with_embeddings);
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
        if let Some(f) = self.fusion {
            r = r.fusion(f);
        }
        if let Some(w) = self.weights {
            r = r.weights(w);
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

#[allow(clippy::too_many_arguments)]
fn recall_args(
    namespaces: &Bound<'_, PyAny>,
    text: Option<String>,
    embedding: Option<Vec<f32>>,
    k: Option<usize>,
    candidates: Option<usize>,
    byte_budget: Option<u64>,
    snapshot: Option<&Bound<'_, Snapshot>>,
    now_us: Option<u64>,
    half_life: Option<&Bound<'_, PyAny>>,
    fusion: &str,
    weights: Option<&Bound<'_, Weights>>,
    include_superseded: bool,
    session: Option<String>,
    with_signals: bool,
    with_embeddings: bool,
) -> PyResult<RecallArgs> {
    let namespaces: Vec<String> = if namespaces.is_instance_of::<pyo3::types::PyString>() {
        vec![namespaces.extract()?]
    } else {
        namespaces.extract()?
    };
    let fusion = match fusion {
        "rrf" => mem::Fusion::Rrf,
        "linear" => mem::Fusion::Linear,
        _ => return Err(invalid(format!("fusion {fusion:?}: rrf or linear"))),
    };
    Ok(RecallArgs {
        namespaces,
        text,
        embedding,
        k,
        candidates,
        byte_budget,
        snapshot: snapshot_arg(snapshot)?,
        now_us,
        half_life: half_life.map(duration).transpose()?,
        fusion: Some(fusion),
        weights: weights.map(|w| w.get().0),
        include_superseded,
        session,
        with_signals,
        with_embeddings,
    })
}

fn hits(
    py: Python<'_>,
    (hs, stats): (Vec<mem::RecallHit>, mem::RecallStats),
) -> PyResult<(Vec<RecallHit>, RecallStats)> {
    let hs = hs
        .into_iter()
        .map(|h| {
            let signals = h
                .signals
                .map(|s| {
                    Py::new(
                        py,
                        Signals {
                            vector: signal(s.vector),
                            keyword: signal(s.keyword),
                            recency: signal(s.recency),
                            importance: signal(s.importance),
                            uses: signal(s.uses),
                        },
                    )
                })
                .transpose()?;
            Ok(RecallHit {
                id: MemoryId(h.id),
                text: h.text,
                kind: kind_name(h.kind),
                importance: h.importance,
                created_us: h.created_us,
                superseded_by: h.superseded_by.map(MemoryId),
                score: h.score,
                signals,
                embedding: h.embedding,
            })
        })
        .collect::<PyResult<_>>()?;
    Ok((
        hs,
        RecallStats {
            candidates: stats.candidates,
            truncated: stats.truncated,
        },
    ))
}

// ---- The Python consolidator ------------------------------------------------------------

/// A consolidator's failure: the Python exception it raised (or the `TypeError` for what it
/// returned), kept whole for `ConsolidateError.__cause__`.
#[derive(Debug)]
struct PyFailure(PyErr);

impl std::fmt::Display for PyFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Python::attach(|py| {
            let v = self.0.value(py);
            let name = v
                .get_type()
                .name()
                .map(|n| n.to_string())
                .unwrap_or_default();
            match v.str() {
                Ok(s) => write!(f, "{name}: {s}"),
                Err(_) => write!(f, "{name}"),
            }
        })
    }
}

impl std::error::Error for PyFailure {}

/// A Python callable as the crate's `Consolidator`: the GIL is taken for the call only.
struct PyConsolidator(Py<PyAny>);

impl mem::Consolidator for PyConsolidator {
    fn consolidate(
        &self,
        group: &[mem::StoredMemory],
    ) -> Result<Vec<mem::Memory>, Box<dyn std::error::Error + Send + Sync>> {
        Python::attach(|py| {
            let fail =
                |e: PyErr| -> Box<dyn std::error::Error + Send + Sync> { Box::new(PyFailure(e)) };
            let items = group
                .iter()
                .map(|m| Py::new(py, StoredMemory::from(m.clone())))
                .collect::<PyResult<Vec<_>>>()
                .map_err(fail)?;
            let list = PyList::new(py, items).map_err(fail)?;
            let out = self.0.bind(py).call1((list,)).map_err(fail)?;
            let returned = || {
                PyTypeError::new_err(format!(
                    "a consolidator returns a list of pigeonhole.memory.Memory, not {}",
                    out.get_type()
                        .name()
                        .map(|n| n.to_string())
                        .unwrap_or_default()
                ))
            };
            if out.is_instance_of::<pyo3::types::PyString>() {
                return Err(fail(returned()));
            }
            let mut memories = Vec::new();
            for m in out.try_iter().map_err(|_| fail(returned()))? {
                let m = m.map_err(fail)?;
                let m = m.cast::<Memory>().map_err(|_| fail(returned()))?;
                memories.push(m.get().inner.clone());
            }
            Ok(memories)
        })
    }
}

/// `ConsolidateError` for a pass the consolidator stopped.
fn consolidate_error(
    py: Python<'_>,
    error: Box<dyn std::error::Error + Send + Sync>,
    pass: mem::ConsolidatePass,
) -> PyErr {
    let message = format!("the consolidator failed: {error}");
    let e = PyErr::new::<ConsolidateError, _>(message);
    let v = e.value(py);
    let _ = v.setattr("code", "MergeFailed");
    let _ = v.setattr("code_num", ErrorCode::MergeFailed as u32);
    if let Ok(p) = Py::new(py, ConsolidatePass::from(pass)) {
        let _ = v.setattr("consolidated", p);
    }
    match error.downcast::<PyFailure>() {
        Ok(cause) => e.set_cause(py, Some(cause.0)),
        Err(_) => e.set_cause(py, None),
    }
    e
}

// ---- Store ----------------------------------------------------------------------------

/// A memory store in the writer process (ICR 0107): `Store.create` or `Store.open`.
#[pyclass(module = "pigeonhole.memory", frozen)]
pub(crate) struct Store(Arc<StoreSlot>);

impl Store {
    fn handle(&self) -> PyResult<mem::Store> {
        self.0
            .read()
            .expect("not poisoned")
            .clone()
            .ok_or_else(closed)
    }

    fn adopt(db: &Database, s: mem::Store) -> Self {
        let slot = Arc::new(RwLock::new(Some(s)));
        db.adopt_store(&slot);
        Self(slot)
    }
}

#[pymethods]
impl Store {
    /// Creates the store `name` (tables `mem_NAME` and `mem_NAME_text`) and opens it.
    /// `embedding` pins it to a model; without one, recall is by keyword and recency.
    /// `forget` (or the `episodic_ttl` shorthand) is its forget policy, run on a thread of the
    /// crate's own; `near_dedup` its near-duplicate dedup.
    #[staticmethod]
    #[pyo3(signature = (db, name, *, embedding=None, forget=None, episodic_ttl=None, near_dedup=None))]
    fn create(
        py: Python<'_>,
        db: &Bound<'_, Database>,
        name: String,
        embedding: Option<&Bound<'_, EmbeddingModel>>,
        forget: Option<&Bound<'_, ForgetPolicy>>,
        episodic_ttl: Option<&Bound<'_, PyAny>>,
        near_dedup: Option<&Bound<'_, NearDedup>>,
    ) -> PyResult<Self> {
        let mut o = mem::StoreOptions::new();
        if let Some(m) = embedding {
            o = o.embedding(m.get().0.clone());
        }
        if let Some(p) = forget {
            o = o.forget(p.get().0.clone());
        }
        if let Some(d) = episodic_ttl {
            o = o.episodic_ttl(duration(d)?);
        }
        if let Some(n) = near_dedup {
            o = o.near_dedup(n.get().0);
        }
        let p = db.get().db()?;
        let s = py
            .detach(move || mem::Store::create(&p, &name, o))
            .or_mem()?;
        Ok(Self::adopt(db.get(), s))
    }

    /// Opens the store `name` for a caller embedding with `model` (`ModelMismatch` for
    /// another model, `InvalidArgument` if the store has none), or with `model=None` without
    /// a model (on a pinned store, that handle can't `remember` or `supersede`).
    #[staticmethod]
    #[pyo3(signature = (db, name, *, model))]
    fn open(
        py: Python<'_>,
        db: &Bound<'_, Database>,
        name: String,
        model: Option<String>,
    ) -> PyResult<Self> {
        let p = db.get().db()?;
        let s = py
            .detach(move || match model {
                Some(m) => mem::Store::open(&p, &name, &m),
                None => mem::Store::open_without_embeddings(&p, &name),
            })
            .or_mem()?;
        Ok(Self::adopt(db.get(), s))
    }

    /// Opens the store `name` from a reader process: recall, `get` and `verify` only.
    #[staticmethod]
    #[pyo3(signature = (reader, name, *, model))]
    fn open_reader(
        py: Python<'_>,
        reader: &Bound<'_, Reader>,
        name: String,
        model: Option<String>,
    ) -> PyResult<ReadStore> {
        let r = reader.get().db()?;
        let s = py
            .detach(move || match model {
                Some(m) => mem::Store::open_reader(&r, &name, &m),
                None => mem::Store::open_reader_without_embeddings(&r, &name),
            })
            .or_mem()?;
        let slot = Arc::new(RwLock::new(Some(Arc::new(s))));
        reader.get().adopt_store(&slot);
        Ok(ReadStore(slot))
    }

    /// Closes this handle: later calls raise `Closed`. The store's forgetter thread stops
    /// once every handle to the store is closed.
    fn close(&self) {
        close_slot(&self.0);
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &self,
        _ty: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _tb: &Bound<'_, PyAny>,
    ) -> bool {
        self.close();
        false
    }

    /// The model the store is pinned to, if any.
    #[getter]
    fn embedding_model(&self) -> PyResult<Option<EmbeddingModel>> {
        Ok(self
            .handle()?
            .embedding_model()
            .cloned()
            .map(EmbeddingModel))
    }

    /// Remembers one memory in namespace `ns`, in one transaction. An exact duplicate (after
    /// trimming and collapsing whitespace) of a live memory merges into it; with near dedup
    /// set, a near duplicate is handled by its policy.
    #[pyo3(signature = (ns, text, *, embedding=None, kind=None, importance=None, source=None, session=None, links=None, pinned=false))]
    #[allow(clippy::too_many_arguments)]
    fn remember(
        &self,
        py: Python<'_>,
        ns: String,
        text: String,
        embedding: Option<Vec<f32>>,
        kind: Option<&str>,
        importance: Option<f64>,
        source: Option<String>,
        session: Option<String>,
        links: Option<&Bound<'_, PyAny>>,
        pinned: bool,
    ) -> PyResult<Remembered> {
        let m = build(
            text, embedding, kind, importance, source, session, links, pinned,
        )?;
        let s = self.handle()?;
        let r = py.detach(move || s.remember(&ns, m)).or_mem()?;
        Ok(r.into())
    }

    /// `remember` inside `txn`, committed with it. It skips near dedup, whose check reads
    /// outside a transaction: call `near_duplicate` first if you need it. It reads in the
    /// transaction, so it's safe inside `Database.transact`, whose retry re-reads.
    #[pyo3(signature = (txn, ns, text, *, embedding=None, kind=None, importance=None, source=None, session=None, links=None, pinned=false))]
    #[allow(clippy::too_many_arguments)]
    fn remember_in(
        &self,
        py: Python<'_>,
        txn: &Bound<'_, Transaction>,
        ns: String,
        text: String,
        embedding: Option<Vec<f32>>,
        kind: Option<&str>,
        importance: Option<f64>,
        source: Option<String>,
        session: Option<String>,
        links: Option<&Bound<'_, PyAny>>,
        pinned: bool,
    ) -> PyResult<Remembered> {
        let m = build(
            text, embedding, kind, importance, source, session, links, pinned,
        )?;
        let s = self.handle()?;
        let r = txn
            .get()
            .with_txn(py, move |t| s.remember_in(t, &ns, m))?
            .or_mem()?;
        Ok(r.into())
    }

    /// Deletes a memory; `False` if it didn't exist.
    fn delete(&self, py: Python<'_>, id: &Bound<'_, PyAny>) -> PyResult<bool> {
        let id = id_arg(id)?;
        let s = self.handle()?;
        py.detach(move || s.delete(&id)).or_mem()
    }

    /// `delete` inside `txn`.
    fn delete_in(
        &self,
        py: Python<'_>,
        txn: &Bound<'_, Transaction>,
        id: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let id = id_arg(id)?;
        let s = self.handle()?;
        txn.get()
            .with_txn(py, move |t| s.delete_in(t, &id))?
            .or_mem()
    }

    /// Links `from_` to `to` (one namespace) with a link type (`LinkType` or an int, custom
    /// ones from 16) and a weight from 0 to 1.
    #[pyo3(signature = (from_, to, link_type, weight=1.0))]
    fn link(
        &self,
        py: Python<'_>,
        from_: &Bound<'_, PyAny>,
        to: &Bound<'_, PyAny>,
        link_type: u8,
        weight: f32,
    ) -> PyResult<()> {
        let (a, b, ty) = (id_arg(from_)?, id_arg(to)?, self::link_type(link_type)?);
        let s = self.handle()?;
        py.detach(move || s.link(&a, &b, ty, weight)).or_mem()
    }

    /// Marks `old` replaced by `new`: recall leaves `old` out unless `include_superseded`.
    fn supersede(
        &self,
        py: Python<'_>,
        old: &Bound<'_, PyAny>,
        new: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (a, b) = (id_arg(old)?, id_arg(new)?);
        let s = self.handle()?;
        py.detach(move || s.supersede(&a, &b)).or_mem()
    }

    /// Counts a use of each memory.
    fn mark_used(&self, py: Python<'_>, ids: &Bound<'_, PyAny>) -> PyResult<Marked> {
        let ids = ids_arg(ids)?;
        let s = self.handle()?;
        let m = py.detach(move || s.mark_used(&ids)).or_mem()?;
        Ok(Marked {
            marked: m.marked,
            missing: m.missing.into_iter().map(MemoryId).collect(),
        })
    }

    /// One memory, or `None`.
    fn get(&self, py: Python<'_>, id: &Bound<'_, PyAny>) -> PyResult<Option<StoredMemory>> {
        let id = id_arg(id)?;
        let s = self.handle()?;
        Ok(py.detach(move || s.get(&id)).or_mem()?.map(Into::into))
    }

    /// The store's consistency counts.
    fn verify(&self, py: Python<'_>) -> PyResult<StoreStats> {
        let s = self.handle()?;
        Ok(py.detach(move || s.verify()).or_mem()?.into())
    }

    /// The memories best matching `text` and/or `embedding` in `namespaces` (a list, or one
    /// str), best first, at one snapshot.
    #[pyo3(signature = (namespaces, *, text=None, embedding=None, k=None, candidates=None, byte_budget=None, snapshot=None, now_us=None, half_life=None, fusion="rrf", weights=None, include_superseded=false, session=None, with_signals=false, with_embeddings=false))]
    #[allow(clippy::too_many_arguments)]
    fn recall(
        &self,
        py: Python<'_>,
        namespaces: &Bound<'_, PyAny>,
        text: Option<String>,
        embedding: Option<Vec<f32>>,
        k: Option<usize>,
        candidates: Option<usize>,
        byte_budget: Option<u64>,
        snapshot: Option<&Bound<'_, Snapshot>>,
        now_us: Option<u64>,
        half_life: Option<&Bound<'_, PyAny>>,
        fusion: &str,
        weights: Option<&Bound<'_, Weights>>,
        include_superseded: bool,
        session: Option<String>,
        with_signals: bool,
        with_embeddings: bool,
    ) -> PyResult<Vec<RecallHit>> {
        self.recall_with_stats(
            py,
            namespaces,
            text,
            embedding,
            k,
            candidates,
            byte_budget,
            snapshot,
            now_us,
            half_life,
            fusion,
            weights,
            include_superseded,
            session,
            with_signals,
            with_embeddings,
        )
        .map(|(h, _)| h)
    }

    /// `recall`, with its `RecallStats`.
    #[pyo3(signature = (namespaces, *, text=None, embedding=None, k=None, candidates=None, byte_budget=None, snapshot=None, now_us=None, half_life=None, fusion="rrf", weights=None, include_superseded=false, session=None, with_signals=false, with_embeddings=false))]
    #[allow(clippy::too_many_arguments)]
    fn recall_with_stats(
        &self,
        py: Python<'_>,
        namespaces: &Bound<'_, PyAny>,
        text: Option<String>,
        embedding: Option<Vec<f32>>,
        k: Option<usize>,
        candidates: Option<usize>,
        byte_budget: Option<u64>,
        snapshot: Option<&Bound<'_, Snapshot>>,
        now_us: Option<u64>,
        half_life: Option<&Bound<'_, PyAny>>,
        fusion: &str,
        weights: Option<&Bound<'_, Weights>>,
        include_superseded: bool,
        session: Option<String>,
        with_signals: bool,
        with_embeddings: bool,
    ) -> PyResult<(Vec<RecallHit>, RecallStats)> {
        let a = recall_args(
            namespaces,
            text,
            embedding,
            k,
            candidates,
            byte_budget,
            snapshot,
            now_us,
            half_life,
            fusion,
            weights,
            include_superseded,
            session,
            with_signals,
            with_embeddings,
        )?;
        let s = self.handle()?;
        let out = py
            .detach(move || {
                let ns: Vec<&str> = a.namespaces.iter().map(String::as_str).collect();
                let r = s.recall(&ns);
                a.run(r)
            })
            .or_mem()?;
        hits(py, out)
    }

    /// One forget pass (T2): under `policy` if given, else the store's persisted one
    /// (`InvalidArgument` with neither); `after` resumes where an earlier pass stopped.
    #[pyo3(signature = (*, policy=None, after=None, now_us=None))]
    fn forget(
        &self,
        py: Python<'_>,
        policy: Option<&Bound<'_, ForgetPolicy>>,
        after: Option<&Bound<'_, PyAny>>,
        now_us: Option<u64>,
    ) -> PyResult<ForgetPass> {
        let policy = policy.map(|p| p.get().0.clone());
        let after = after.map(id_arg).transpose()?;
        let s = self.handle()?;
        let p = py
            .detach(move || {
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
            })
            .or_mem()?;
        Ok(ForgetPass {
            examined: p.examined,
            deleted: p.deleted,
            protected: p.protected,
            resume: p.resume.map(MemoryId),
        })
    }

    /// Re-evaluates one memory under the persisted policy in `txn`: `"deleted"`,
    /// `"protected"`, `"not_due"` or `"gone"`. Only `"deleted"` wrote anything; none of them
    /// is an error, so a retry loop (`Database.transact`) shouldn't treat them as one.
    fn forget_in(
        &self,
        py: Python<'_>,
        txn: &Bound<'_, Transaction>,
        id: &Bound<'_, PyAny>,
        now_us: u64,
    ) -> PyResult<&'static str> {
        let id = id_arg(id)?;
        let s = self.handle()?;
        let f = txn
            .get()
            .with_txn(py, move |t| s.forget_in(t, &id, now_us))?
            .or_mem()?;
        Ok(match f {
            mem::Forgot::Deleted => "deleted",
            mem::Forgot::Protected => "protected",
            mem::Forgot::NotDue => "not_due",
            mem::Forgot::Gone => "gone",
            _ => "unknown",
        })
    }

    /// The store's persisted forget policy, if any.
    fn forget_policy(&self, py: Python<'_>) -> PyResult<Option<ForgetPolicy>> {
        let s = self.handle()?;
        Ok(py
            .detach(move || s.forget_policy())
            .or_mem()?
            .map(ForgetPolicy))
    }

    /// Changes the persisted forget policy (`None` turns forgetting off).
    fn set_forget_policy(
        &self,
        py: Python<'_>,
        policy: Option<&Bound<'_, ForgetPolicy>>,
    ) -> PyResult<()> {
        let p = policy.map(|p| p.get().0.clone());
        let s = self.handle()?;
        py.detach(move || s.set_forget_policy(p)).or_mem()
    }

    /// The store's near-duplicate dedup, if any.
    fn near_dedup(&self, py: Python<'_>) -> PyResult<Option<NearDedup>> {
        let s = self.handle()?;
        Ok(py.detach(move || s.near_dedup()).or_mem()?.map(NearDedup))
    }

    /// Sets the near-duplicate dedup (`None` turns it off; a store without a model refuses
    /// one).
    fn set_near_dedup(&self, py: Python<'_>, near: Option<&Bound<'_, NearDedup>>) -> PyResult<()> {
        let n = near.map(|n| n.get().0);
        let s = self.handle()?;
        py.detach(move || s.set_near_dedup(n)).or_mem()
    }

    /// The live memory of `ns` nearest a memory of `text` and `embedding`, with its
    /// similarity, if it reaches the store's near dedup similarity: what `remember` checks
    /// (`remember_in` doesn't).
    #[pyo3(signature = (ns, text, *, embedding=None))]
    fn near_duplicate(
        &self,
        py: Python<'_>,
        ns: String,
        text: String,
        embedding: Option<Vec<f32>>,
    ) -> PyResult<Option<(MemoryId, f32)>> {
        let mut m = mem::Memory::new(text);
        if let Some(e) = embedding {
            m = m.embedding(e);
        }
        let s = self.handle()?;
        Ok(py
            .detach(move || s.near_duplicate(&ns, &m))
            .or_mem()?
            .map(|(id, sim)| (MemoryId(id), sim)))
    }

    /// One consolidation pass over `ns` (T3): `consolidator(group)` gets each group of
    /// episodic memories (a list of `StoredMemory`) and returns a list of `Memory` to keep in
    /// their place, written as semantic (unless procedural) in the group's session.
    ///
    /// The consolidator runs outside every transaction, with no store lock held, so it may
    /// call the store (`recall` for context, say) and take its time. A group that changes
    /// before its commit is skipped, not retried (`skipped` counts it; the next pass finds it
    /// again). An exception from it, `KeyboardInterrupt` and `SystemExit` included, or a
    /// return that isn't a list of `Memory`, stops the pass with `ConsolidateError`: its
    /// `consolidated` is the pass so far, whose groups stay committed, and its `__cause__` the
    /// exception.
    #[pyo3(signature = (ns, consolidator, *, policy=None, after=None, now_us=None))]
    fn consolidate(
        &self,
        py: Python<'_>,
        ns: String,
        consolidator: Py<PyAny>,
        policy: Option<&Bound<'_, ConsolidatePolicy>>,
        after: Option<&Bound<'_, PyAny>>,
        now_us: Option<u64>,
    ) -> PyResult<ConsolidatePass> {
        if !consolidator.bind(py).is_callable() {
            return Err(PyTypeError::new_err("consolidator must be callable"));
        }
        let policy = policy.map(|p| p.get().0.clone());
        let after = after.map(id_arg).transpose()?;
        let s = self.handle()?;
        let c = PyConsolidator(consolidator);
        let out = py.detach(move || {
            let mut run = s.consolidate(&ns, &c);
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
        });
        match out {
            Ok(p) => Ok(p.into()),
            Err(mem::Error::Consolidate { error, pass }) => Err(consolidate_error(py, error, pass)),
            Err(e) => Err(err(e)),
        }
    }
}

/// A memory store in a reader process: `recall`, `get` and `verify`, at the reader's
/// snapshots.
#[pyclass(module = "pigeonhole.memory", frozen)]
pub(crate) struct ReadStore(Arc<ReadSlot>);

impl ReadStore {
    fn store(&self) -> PyResult<Arc<mem::ReadStore>> {
        self.0
            .read()
            .expect("not poisoned")
            .clone()
            .ok_or_else(closed)
    }
}

#[pymethods]
impl ReadStore {
    /// Closes this handle: later calls raise `Closed`.
    fn close(&self) {
        self.0.write().expect("not poisoned").take();
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &self,
        _ty: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _tb: &Bound<'_, PyAny>,
    ) -> bool {
        self.close();
        false
    }

    #[getter]
    fn embedding_model(&self) -> PyResult<Option<EmbeddingModel>> {
        Ok(self.store()?.embedding_model().cloned().map(EmbeddingModel))
    }

    fn get(&self, py: Python<'_>, id: &Bound<'_, PyAny>) -> PyResult<Option<StoredMemory>> {
        let id = id_arg(id)?;
        let s = self.store()?;
        Ok(py.detach(move || s.get(&id)).or_mem()?.map(Into::into))
    }

    fn verify(&self, py: Python<'_>) -> PyResult<StoreStats> {
        let s = self.store()?;
        Ok(py.detach(move || s.verify()).or_mem()?.into())
    }

    #[pyo3(signature = (namespaces, *, text=None, embedding=None, k=None, candidates=None, byte_budget=None, snapshot=None, now_us=None, half_life=None, fusion="rrf", weights=None, include_superseded=false, session=None, with_signals=false, with_embeddings=false))]
    #[allow(clippy::too_many_arguments)]
    fn recall(
        &self,
        py: Python<'_>,
        namespaces: &Bound<'_, PyAny>,
        text: Option<String>,
        embedding: Option<Vec<f32>>,
        k: Option<usize>,
        candidates: Option<usize>,
        byte_budget: Option<u64>,
        snapshot: Option<&Bound<'_, Snapshot>>,
        now_us: Option<u64>,
        half_life: Option<&Bound<'_, PyAny>>,
        fusion: &str,
        weights: Option<&Bound<'_, Weights>>,
        include_superseded: bool,
        session: Option<String>,
        with_signals: bool,
        with_embeddings: bool,
    ) -> PyResult<Vec<RecallHit>> {
        self.recall_with_stats(
            py,
            namespaces,
            text,
            embedding,
            k,
            candidates,
            byte_budget,
            snapshot,
            now_us,
            half_life,
            fusion,
            weights,
            include_superseded,
            session,
            with_signals,
            with_embeddings,
        )
        .map(|(h, _)| h)
    }

    #[pyo3(signature = (namespaces, *, text=None, embedding=None, k=None, candidates=None, byte_budget=None, snapshot=None, now_us=None, half_life=None, fusion="rrf", weights=None, include_superseded=false, session=None, with_signals=false, with_embeddings=false))]
    #[allow(clippy::too_many_arguments)]
    fn recall_with_stats(
        &self,
        py: Python<'_>,
        namespaces: &Bound<'_, PyAny>,
        text: Option<String>,
        embedding: Option<Vec<f32>>,
        k: Option<usize>,
        candidates: Option<usize>,
        byte_budget: Option<u64>,
        snapshot: Option<&Bound<'_, Snapshot>>,
        now_us: Option<u64>,
        half_life: Option<&Bound<'_, PyAny>>,
        fusion: &str,
        weights: Option<&Bound<'_, Weights>>,
        include_superseded: bool,
        session: Option<String>,
        with_signals: bool,
        with_embeddings: bool,
    ) -> PyResult<(Vec<RecallHit>, RecallStats)> {
        let a = recall_args(
            namespaces,
            text,
            embedding,
            k,
            candidates,
            byte_budget,
            snapshot,
            now_us,
            half_life,
            fusion,
            weights,
            include_superseded,
            session,
            with_signals,
            with_embeddings,
        )?;
        let s = self.store()?;
        let out = py
            .detach(move || {
                let ns: Vec<&str> = a.namespaces.iter().map(String::as_str).collect();
                let r = s.recall(&ns);
                a.run(r)
            })
            .or_mem()?;
        hits(py, out)
    }
}

/// Registers the submodule's classes on `m`.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add_class::<MemoryId>()?;
    m.add_class::<EmbeddingModel>()?;
    m.add_class::<Weights>()?;
    m.add_class::<ForgetPolicy>()?;
    m.add_class::<ConsolidatePolicy>()?;
    m.add_class::<NearDedup>()?;
    m.add_class::<Memory>()?;
    m.add_class::<Remembered>()?;
    m.add_class::<Marked>()?;
    m.add_class::<Link>()?;
    m.add_class::<Source>()?;
    m.add_class::<StoredMemory>()?;
    m.add_class::<StoreStats>()?;
    m.add_class::<Signal>()?;
    m.add_class::<Signals>()?;
    m.add_class::<RecallHit>()?;
    m.add_class::<RecallStats>()?;
    m.add_class::<ForgetPass>()?;
    m.add_class::<ConsolidatePass>()?;
    m.add_class::<Store>()?;
    m.add_class::<ReadStore>()?;
    m.add("ModelMismatch", py.get_type::<ModelMismatch>())?;
    m.add("ConsolidateError", py.get_type::<ConsolidateError>())?;
    Ok(())
}
