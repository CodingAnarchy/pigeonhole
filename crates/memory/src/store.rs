//! The store (D311 §2, §4, ICR 0107): create and open with the pinned embedding model,
//! `remember` with exact dedup, `get`, `delete`, `link`, `supersede`, `mark_used`, `verify`.

use std::collections::{BTreeMap, BTreeSet};

use pigeonhole::{
    Aggregation, Element, Family, Metric, Pigeonhole, RetryPolicy, Table, Transaction, VectorSpec,
};
use pigeonhole_text::{Index, IndexOptions, Source};

use crate::error::{Error, Result, corrupt};
use crate::layout::*;

/// The embedding model a store is pinned to (owner, 2026-11-02; ICR 0107's amendment on
/// #1251): its id, dimension and element type. Recorded at create; every handle opened for
/// embeddings names the id, and every embedding is checked against the dimension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingModel {
    id: String,
    dim: u32,
    element: Element,
}

impl EmbeddingModel {
    /// A model `id` (the name the application knows it by, such as
    /// `"nomic-embed-text-v1.5"`) producing `dim`-dimension vectors, stored as `element`:
    /// `F32`, `F16` or `BF16`. `I8` and `Bit` are refused: their queries need a scale the
    /// store doesn't hold (D303).
    pub fn new(id: impl Into<String>, dim: u32, element: Element) -> Result<Self> {
        let id = id.into();
        if id.is_empty() {
            return Err(Error::InvalidArgument("a model id can't be empty".into()));
        }
        if !matches!(element, Element::F32 | Element::F16 | Element::BF16) {
            return Err(Error::InvalidArgument(format!(
                "a store's embeddings are F32, F16 or BF16, not {element:?}"
            )));
        }
        VectorSpec::new(dim, element, Metric::Cosine)?;
        Ok(EmbeddingModel { id, dim, element })
    }

    /// The model's id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The embeddings' dimension.
    pub fn dim(&self) -> u32 {
        self.dim
    }

    /// The element type they're stored as.
    pub fn element(&self) -> Element {
        self.element
    }

    fn spec(&self) -> VectorSpec {
        VectorSpec::new(self.dim, self.element, Metric::Cosine).expect("checked at new")
    }
}

/// A store's options, fixed at create.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct StoreOptions {
    embedding: Option<EmbeddingModel>,
}

impl StoreOptions {
    /// The defaults: no embedding model, so recall is keyword and recency only.
    pub fn new() -> Self {
        StoreOptions::default()
    }

    /// Pins the store to `model`: its memories carry embeddings of that model.
    pub fn embedding(mut self, model: EmbeddingModel) -> Self {
        self.embedding = Some(model);
        self
    }
}

/// A memory's kind (D311 §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Kind {
    /// Something that happened: the default.
    #[default]
    Episodic,
    /// A fact or summary.
    Semantic,
    /// How to do something.
    Procedural,
}

impl Kind {
    fn code(self) -> u8 {
        match self {
            Kind::Episodic => 0,
            Kind::Semantic => 1,
            Kind::Procedural => 2,
        }
    }

    fn from_code(c: u8) -> Result<Self> {
        Ok(match c {
            0 => Kind::Episodic,
            1 => Kind::Semantic,
            2 => Kind::Procedural,
            _ => return Err(corrupt("a memory's kind")),
        })
    }
}

/// A link's type: the application's (for example "about" or "follows"), from 16 up; the
/// crate's own are below 16 (frozen in ICR 0107).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LinkType(pub u8);

impl LinkType {
    /// A consolidated memory to each of its sources (T3).
    pub const DERIVED_FROM: LinkType = LinkType(1);
    /// The new memory to the one it supersedes ([`Store::supersede`]).
    pub const SUPERSEDES: LinkType = LinkType(2);
    /// A near duplicate to its match (T3's `DedupPolicy::Link`).
    pub const SIMILAR_TO: LinkType = LinkType(3);
}

/// A memory to write ([`Store::remember`]): a builder, so it can grow (D311 §10a).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Memory {
    text: String,
    embedding: Option<Vec<f32>>,
    kind: Kind,
    importance: f64,
    source: Option<String>,
    session: Option<String>,
    links: Vec<(MemoryId, LinkType, f32)>,
    pinned: bool,
}

impl Memory {
    /// A memory of `text`, episodic, with importance 0.5.
    pub fn new(text: impl Into<String>) -> Self {
        Memory {
            text: text.into(),
            embedding: None,
            kind: Kind::Episodic,
            importance: 0.5,
            source: None,
            session: None,
            links: Vec::new(),
            pinned: false,
        }
    }

    /// Its embedding, from the store's model. Required in a store pinned to a model.
    pub fn embedding(mut self, e: impl Into<Vec<f32>>) -> Self {
        self.embedding = Some(e.into());
        self
    }

    /// Its kind.
    pub fn kind(mut self, kind: Kind) -> Self {
        self.kind = kind;
        self
    }

    /// Its importance, from 0 to 1 (clamped). A dedup merge keeps the larger.
    pub fn importance(mut self, importance: f64) -> Self {
        self.importance = importance;
        self
    }

    /// A provenance entry: the application's source id (a tool call id, a URL).
    pub fn source(mut self, id: impl Into<String>) -> Self {
        self.source = Some(id.into());
        self
    }

    /// The session it belongs to (a filter in recall, not part of its key).
    pub fn session(mut self, s: impl Into<String>) -> Self {
        self.session = Some(s.into());
        self
    }

    /// A link to the existing memory `to`, of type `ty`, with weight `weight`.
    pub fn link(mut self, to: MemoryId, ty: LinkType, weight: f32) -> Self {
        self.links.push((to, ty, weight));
        self
    }

    /// Never forgotten (T2).
    pub fn pinned(mut self, pinned: bool) -> Self {
        self.pinned = pinned;
        self
    }
}

/// What [`Store::remember`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Remembered {
    /// A new memory.
    New(MemoryId),
    /// The text was already remembered (exact dedup): merged into that memory, which kept
    /// the larger importance, gained the source, and counted a use.
    Merged(MemoryId),
}

impl Remembered {
    /// The memory written or merged into.
    pub fn id(&self) -> &MemoryId {
        match self {
            Remembered::New(id) | Remembered::Merged(id) => id,
        }
    }
}

/// A stored memory ([`Store::get`]).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct StoredMemory {
    /// Its id.
    pub id: MemoryId,
    /// Its text.
    pub text: String,
    /// Its kind.
    pub kind: Kind,
    /// Its importance, from 0 to 1.
    pub importance: f64,
    /// How many times it was used ([`Store::mark_used`]) or merged into.
    pub uses: u64,
    /// When it was last used, in µs, if ever.
    pub last_used_us: Option<u64>,
    /// Its session.
    pub session: Option<String>,
    /// Whether it's pinned.
    pub pinned: bool,
    /// The memory that superseded it, if one did.
    pub superseded_by: Option<MemoryId>,
    /// Its sources, each with its first-seen time (µs).
    pub sources: Vec<(String, u64)>,
    /// Its outgoing links.
    pub links: Vec<(MemoryId, LinkType, f32)>,
    /// Its incoming links.
    pub backlinks: Vec<(MemoryId, LinkType, f32)>,
    /// Its embedding, if it has one.
    pub embedding: Option<Vec<f32>>,
}

/// What [`Store::verify`] found. Mismatches are counted, not returned as errors.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StoreStats {
    /// Memories.
    pub memories: u64,
    /// Memory rows without `content:text`.
    pub no_content: u64,
    /// Dedup rows naming a memory that's gone, or whose normalised text hashes elsewhere.
    pub stale_dedup: u64,
    /// Links without their backlink (or the reverse), or with an end that's gone.
    pub broken_links: u64,
    /// Memories missing an embedding in a store pinned to a model.
    pub missing_embeddings: u64,
    /// The text index's own check failed ([`pigeonhole_text::IndexStats::is_consistent`]).
    pub text_index_inconsistent: bool,
}

impl StoreStats {
    /// No mismatch at all.
    pub fn is_consistent(&self) -> bool {
        self.no_content == 0
            && self.stale_dedup == 0
            && self.broken_links == 0
            && self.missing_embeddings == 0
            && !self.text_index_inconsistent
    }
}

/// A memory store, in the writer process: one table plus its text index (D311 §2).
pub struct Store {
    db: Pigeonhole,
    table: Table,
    index: Index,
    /// The store's pinned model.
    model: Option<EmbeddingModel>,
    /// Whether this handle was opened with the store's model (always so for a store
    /// without one).
    embeds: bool,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("table", &self.table.name())
            .field("model", &self.model)
            .field("embeds", &self.embeds)
            .finish()
    }
}

fn table_name(name: &str) -> String {
    format!("mem_{name}")
}

fn index_name(name: &str) -> String {
    format!("mem_{name}_text")
}

impl Store {
    /// Creates the store `name`: the table `mem_{name}` and its text index
    /// `mem_{name}_text`, the index without positions (recall queries are rarely phrases;
    /// owner, 2026-11-02). `TableExists` if the table is there.
    pub fn create(db: &Pigeonhole, name: &str, options: StoreOptions) -> Result<Store> {
        let mut b = db
            .table(&table_name(name))?
            .family(CONTENT, Family::default())
            .family(META, Family::default())
            .family(SOURCES, Family::default())
            .family(STATS, Family::counter())
            .family(PEAK, Family::aggregate(Aggregation::Max))
            .family(LINKS, Family::default())
            .family(BACKLINKS, Family::default());
        if let Some(m) = &options.embedding {
            b = b.family(EMB, Family::vector(m.spec()));
        }
        let table = b.create()?;
        let mut g = table
            .mutate(GLOBAL)
            .put_u64(META, LAYOUT, LAYOUT_VERSION)
            .put(META, TEXT_INDEX, index_name(name).as_bytes());
        if let Some(m) = &options.embedding {
            g = g
                .put(META, MODEL_ID, m.id.as_bytes())
                .put_u64(META, MODEL_DIM, u64::from(m.dim))
                .put(META, MODEL_ELEMENT, &[element_code(m.element)]);
        }
        g.commit()?;
        let index = Index::create(
            db,
            &index_name(name),
            Source::new(&table, CONTENT, TEXT),
            IndexOptions::new().positions(false),
        )?;
        Ok(Store {
            db: db.clone(),
            table,
            index,
            model: options.embedding,
            embeds: true,
        })
    }

    /// Opens the store `name` for a caller embedding with `model_id`, compared byte for
    /// byte with the store's. `Mismatch` if it's another model; `InvalidArgument` if the
    /// store has none (use [`open_without_embeddings`](Self::open_without_embeddings)).
    pub fn open(db: &Pigeonhole, name: &str, model_id: &str) -> Result<Store> {
        let mut s = Self::open_inner(db, name)?;
        match &s.model {
            Some(m) if m.id == model_id => {}
            Some(m) => {
                return Err(Error::Mismatch(format!(
                    "the store {name} is pinned to {:?}, not {model_id:?}",
                    m.id
                )));
            }
            None => {
                return Err(Error::InvalidArgument(format!(
                    "the store {name} has no embedding model: open it with open_without_embeddings"
                )));
            }
        }
        s.embeds = true;
        Ok(s)
    }

    /// Opens the store `name` without an embedding model. On a store pinned to one, the
    /// handle refuses every write that creates a memory or changes its content
    /// (`remember`, `supersede`), so every memory there keeps its embedding; it allows
    /// `delete`, `link` and `mark_used`, and recalls by keyword and recency.
    pub fn open_without_embeddings(db: &Pigeonhole, name: &str) -> Result<Store> {
        let mut s = Self::open_inner(db, name)?;
        s.embeds = s.model.is_none();
        Ok(s)
    }

    fn open_inner(db: &Pigeonhole, name: &str) -> Result<Store> {
        let table = db.table(&table_name(name))?.open()?;
        let not_store =
            || Error::InvalidArgument(format!("{} is not a memory store", table.name()));
        for f in [CONTENT, META, SOURCES, STATS, PEAK, LINKS, BACKLINKS] {
            if table.family_info(f).is_none() {
                return Err(not_store());
            }
        }
        let g = |q: &[u8]| -> Result<Option<Vec<u8>>> {
            Ok(table.get(GLOBAL, META, q)?.map(|c| c.value().to_vec()))
        };
        let layout = table
            .get(GLOBAL, META, LAYOUT)?
            .and_then(|c| c.as_u64())
            .ok_or_else(not_store)?;
        if layout != LAYOUT_VERSION {
            return Err(Error::InvalidArgument(format!(
                "{} has store layout {layout}, and this build reads {LAYOUT_VERSION}",
                table.name()
            )));
        }
        let model = match g(MODEL_ID)? {
            None => None,
            Some(id) => {
                let id = String::from_utf8(id).map_err(|_| corrupt("the model id"))?;
                let dim = table
                    .get(GLOBAL, META, MODEL_DIM)?
                    .and_then(|c| c.as_u64())
                    .ok_or_else(|| corrupt("the model's dimension"))?;
                let element = g(MODEL_ELEMENT)?
                    .and_then(|e| e.first().copied())
                    .and_then(element_from_code)
                    .ok_or_else(|| corrupt("the model's element"))?;
                Some(EmbeddingModel::new(id, dim as u32, element)?)
            }
        };
        let index = Index::open(db, &index_name(name))?;
        Ok(Store {
            db: db.clone(),
            table,
            index,
            model,
            embeds: false,
        })
    }

    /// The model the store is pinned to, if any.
    pub fn embedding_model(&self) -> Option<&EmbeddingModel> {
        self.model.as_ref()
    }

    /// Remembers `memory` in namespace `ns`, in one transaction (D311 §4): the memory row,
    /// the text index, the dedup row, and both directions of each link. An exact duplicate
    /// (after trimming and collapsing whitespace) of a live, unsuperseded memory merges into
    /// it instead.
    pub fn remember(&self, ns: &str, memory: Memory) -> Result<Remembered> {
        self.check_memory(ns, &memory)?;
        let now = now_us();
        let rid = new_rid(now)?;
        let (r, _) = self.db.transact(&RetryPolicy::default(), |txn| {
            self.remember_with(txn, ns, &memory, rid, now)
        })?;
        Ok(r)
    }

    /// [`remember`](Self::remember) inside the caller's transaction.
    pub fn remember_in(
        &self,
        txn: &mut Transaction,
        ns: &str,
        memory: Memory,
    ) -> Result<Remembered> {
        self.check_memory(ns, &memory)?;
        let now = now_us();
        let rid = new_rid(now)?;
        self.remember_with(txn, ns, &memory, rid, now)
    }

    fn check_memory(&self, ns: &str, m: &Memory) -> Result<()> {
        check_namespace(ns)?;
        if self.model.is_some() && !self.embeds {
            return Err(Error::InvalidArgument(
                "this handle was opened without the store's embedding model, so it can't add \
                 memories: open it with Store::open and the model's id"
                    .into(),
            ));
        }
        match (&m.embedding, &self.model) {
            (Some(_), None) => {
                return Err(Error::InvalidArgument(
                    "the store has no embedding model, so it takes no embeddings".into(),
                ));
            }
            (None, Some(model)) => {
                return Err(Error::InvalidArgument(format!(
                    "the store is pinned to {:?}: every memory needs its embedding",
                    model.id
                )));
            }
            (Some(e), Some(model)) if e.len() != model.dim as usize => {
                return Err(Error::InvalidArgument(format!(
                    "an embedding of {} dimensions, and the store's model has {}",
                    e.len(),
                    model.dim
                )));
            }
            _ => {}
        }
        if normalize(&m.text).is_empty() && m.embedding.is_none() {
            return Err(Error::InvalidArgument(
                "a memory needs text or an embedding".into(),
            ));
        }
        if !m.importance.is_finite() {
            return Err(Error::InvalidArgument("importance must be finite".into()));
        }
        for (to, _, w) in &m.links {
            if to.namespace() != ns {
                return Err(Error::InvalidArgument(format!(
                    "a link from namespace {ns:?} to {:?}: links stay within a namespace",
                    to.namespace()
                )));
            }
            if !w.is_finite() {
                return Err(Error::InvalidArgument(
                    "a link's weight must be finite".into(),
                ));
            }
        }
        Ok(())
    }

    fn remember_with(
        &self,
        txn: &mut Transaction,
        ns: &str,
        m: &Memory,
        rid: [u8; 16],
        now: u64,
    ) -> Result<Remembered> {
        let t = &self.table;
        let norm = normalize(&m.text);
        let importance = (m.importance.clamp(0.0, 1.0) * PPM).round() as i64;
        let dedup = (!norm.is_empty()).then(|| dedup_row(ns, &content_hash(&norm)));
        // Exact dedup (M1): the dedup row, then the memory it names, both read here.
        if let Some(d) = &dedup
            && let Some(target) = txn.get(t, d, META, RID)?.map(|c| c.value().to_vec())
        {
            let text = txn
                .get(t, &target, CONTENT, TEXT)?
                .map(|c| c.value().to_vec());
            let superseded = txn.get(t, &target, META, SUPERSEDED_BY)?.is_some();
            let same = text
                .as_deref()
                .and_then(|b| std::str::from_utf8(b).ok())
                .is_some_and(|s| normalize(s) == norm);
            if same && !superseded {
                let id = MemoryId::from_row(&target).ok_or_else(|| corrupt("a dedup row's rid"))?;
                txn.aggregate(t, &target, PEAK, IMPORTANCE, importance)
                    .incr(t, &target, STATS, USES, 1);
                if let Some(src) = &m.source {
                    self.add_source(txn, &target, src, now)?;
                }
                return Ok(Remembered::Merged(id));
            }
            // Gone, superseded, or a hash collision: insert as new below. A collision
            // leaves the dedup row alone (it names a live memory of other text).
            if text.is_some() && !same {
                return self.insert(txn, ns, m, rid, now, importance, None);
            }
        }
        self.insert(txn, ns, m, rid, now, importance, dedup.as_deref())
    }

    #[allow(clippy::too_many_arguments)]
    fn insert(
        &self,
        txn: &mut Transaction,
        ns: &str,
        m: &Memory,
        rid: [u8; 16],
        now: u64,
        importance: i64,
        dedup: Option<&[u8]>,
    ) -> Result<Remembered> {
        let t = &self.table;
        let id = MemoryId::new(ns, rid);
        let row = id.row();
        // The links' targets first (M4): an absent one fails the write before anything.
        for (to, _, _) in &m.links {
            if txn.get(t, &to.row(), CONTENT, TEXT)?.is_none() {
                return Err(Error::NotFound(format!("a link's target {to}")));
            }
        }
        self.index.put_in(txn, &row, &m.text)?;
        txn.put(t, &row, META, KIND, &[m.kind.code()])
            .aggregate(t, &row, PEAK, IMPORTANCE, importance);
        if let Some(s) = &m.session {
            txn.put(t, &row, META, SESSION, s.as_bytes());
        }
        if m.pinned {
            txn.put(t, &row, META, PINNED, b"1");
        }
        if let Some(e) = &m.embedding {
            txn.put_vector_f32(t, &row, EMB, V, e);
        }
        if let Some(src) = &m.source {
            txn.put_u64(t, &row, SOURCES, src.as_bytes(), now);
        }
        if let Some(d) = dedup {
            txn.put(t, d, META, RID, &row);
        }
        for (to, ty, w) in &m.links {
            write_link(txn, t, &row, &to.row(), *ty, *w);
        }
        Ok(Remembered::New(id))
    }

    /// A source column, if the memory doesn't have it yet (its first-seen time stays).
    fn add_source(&self, txn: &mut Transaction, row: &[u8], src: &str, now: u64) -> Result<()> {
        let t = &self.table;
        if txn.get(t, row, SOURCES, src.as_bytes())?.is_none() {
            txn.put_u64(t, row, SOURCES, src.as_bytes(), now);
        }
        Ok(())
    }

    /// The memory `id`, or `None` if it doesn't exist.
    pub fn get(&self, id: &MemoryId) -> Result<Option<StoredMemory>> {
        let Some(row) = self.table.row(&id.row()).read()? else {
            return Ok(None);
        };
        read_memory(id.clone(), &row.to_owned())
    }

    /// Deletes the memory `id` (D311 §4), in one transaction: its row through the text
    /// index, its dedup row, and both directions of every link. `false` if it didn't exist.
    pub fn delete(&self, id: &MemoryId) -> Result<bool> {
        let (r, _) = self
            .db
            .transact(&RetryPolicy::default(), |txn| self.delete_in(txn, id))?;
        Ok(r)
    }

    /// [`delete`](Self::delete) inside the caller's transaction.
    pub fn delete_in(&self, txn: &mut Transaction, id: &MemoryId) -> Result<bool> {
        let t = &self.table;
        let row = id.row();
        let Some(text) = txn.get(t, &row, CONTENT, TEXT)?.map(|c| c.value().to_vec()) else {
            return Ok(false);
        };
        let (links, backlinks) = {
            let r = txn.get_row(t, &row, &[LINKS, BACKLINKS])?;
            let mut out = (Vec::new(), Vec::new());
            if let Some(r) = r {
                for e in r.iter() {
                    match e.family {
                        LINKS => out.0.push(e.qualifier.to_vec()),
                        BACKLINKS => out.1.push(e.qualifier.to_vec()),
                        _ => {}
                    }
                }
            }
            out
        };
        // The dedup row, if it names this memory.
        let norm = std::str::from_utf8(&text)
            .map(normalize)
            .unwrap_or_default();
        if !norm.is_empty() {
            let d = dedup_row(id.namespace(), &content_hash(&norm));
            if txn
                .get(t, &d, META, RID)?
                .is_some_and(|c| c.value() == row.as_slice())
            {
                txn.delete_row(t, &d);
            }
        }
        for to in &links {
            txn.delete_column(t, to, BACKLINKS, &row);
        }
        for from in &backlinks {
            txn.delete_column(t, from, LINKS, &row);
        }
        self.index.delete_in(txn, &row)?;
        txn.delete_row(t, &row);
        Ok(true)
    }

    /// Links `from` to `to` with type `ty` and weight `weight`, both directions in one
    /// transaction, after reading both ends' `content:text` (M4). `NotFound` naming the end
    /// that's gone.
    pub fn link(&self, from: &MemoryId, to: &MemoryId, ty: LinkType, weight: f32) -> Result<()> {
        if from.namespace() != to.namespace() {
            return Err(Error::InvalidArgument(
                "links stay within a namespace".into(),
            ));
        }
        if !weight.is_finite() {
            return Err(Error::InvalidArgument(
                "a link's weight must be finite".into(),
            ));
        }
        self.db.transact(&RetryPolicy::default(), |txn| {
            let t = &self.table;
            for end in [from, to] {
                if txn.get(t, &end.row(), CONTENT, TEXT)?.is_none() {
                    return Err(Error::NotFound(format!("the memory {end}")));
                }
            }
            write_link(txn, t, &from.row(), &to.row(), ty, weight);
            Ok(())
        })?;
        Ok(())
    }

    /// Marks `old` superseded by `new` (D311 §2, M4): sets `old`'s `superseded_by` and
    /// `valid_to`, and links `new → old` with [`LinkType::SUPERSEDES`]. Recall leaves `old`
    /// out by default. `NotFound` if either is gone.
    pub fn supersede(&self, old: &MemoryId, new: &MemoryId) -> Result<()> {
        if self.model.is_some() && !self.embeds {
            return Err(Error::InvalidArgument(
                "this handle was opened without the store's embedding model, so it can't \
                 supersede memories"
                    .into(),
            ));
        }
        if old.namespace() != new.namespace() || old == new {
            return Err(Error::InvalidArgument(
                "supersede takes two memories of one namespace".into(),
            ));
        }
        let now = now_us();
        self.db.transact(&RetryPolicy::default(), |txn| {
            let t = &self.table;
            for end in [old, new] {
                if txn.get(t, &end.row(), CONTENT, TEXT)?.is_none() {
                    return Err(Error::NotFound(format!("the memory {end}")));
                }
            }
            txn.put(t, &old.row(), META, SUPERSEDED_BY, &new.row())
                .put_u64(t, &old.row(), META, VALID_TO, now);
            write_link(txn, t, &new.row(), &old.row(), LinkType::SUPERSEDES, 1.0);
            Ok(())
        })?;
        Ok(())
    }

    /// Counts a use of each of `ids` (M5): reads each one's `content:text`, then writes the
    /// blind `uses` increment and the `last_used` max. A deleted id is skipped, never
    /// resurrected. Returns how many were marked.
    pub fn mark_used(&self, ids: &[MemoryId]) -> Result<usize> {
        let now = now_us() as i64;
        let (n, _) = self.db.transact(&RetryPolicy::default(), |txn| {
            let t = &self.table;
            let mut n = 0;
            let mut seen = BTreeSet::new();
            for id in ids {
                let row = id.row();
                if !seen.insert(row.clone()) {
                    continue;
                }
                if txn.get(t, &row, CONTENT, TEXT)?.is_none() {
                    continue;
                }
                txn.incr(t, &row, STATS, USES, 1)
                    .aggregate(t, &row, PEAK, LAST_USED, now);
                n += 1;
            }
            Ok::<_, Error>(n)
        })?;
        Ok(n)
    }

    /// Checks the store against itself and its text index, at one snapshot (D311 T1):
    /// every memory row has content, every dedup row names a live memory with that text,
    /// every link has its backlink with both ends live, and the text index agrees.
    pub fn verify(&self) -> Result<StoreStats> {
        let t = &self.table;
        let s = self.db.snapshot()?;
        let mut st = StoreStats::default();
        // key → normalised text, for live memories.
        let mut texts: BTreeMap<Vec<u8>, String> = BTreeMap::new();
        let mut links: BTreeSet<(Vec<u8>, Vec<u8>, Vec<u8>)> = BTreeSet::new();
        let mut backlinks: BTreeSet<(Vec<u8>, Vec<u8>, Vec<u8>)> = BTreeSet::new();
        let mut dedups: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for row in t.scan_prefix(b"").snapshot(&s).iter()? {
            let row = row?;
            let key = row.key().to_vec();
            if key == GLOBAL {
                continue;
            }
            if MemoryId::from_row(&key).is_some() {
                st.memories += 1;
                let mut has_text = false;
                let mut has_emb = false;
                for e in row.view().iter() {
                    match e.family {
                        CONTENT if e.qualifier == TEXT => {
                            has_text = true;
                            let text = std::str::from_utf8(e.cell.value())
                                .map_err(|_| corrupt("a memory's text"))?;
                            texts.insert(key.clone(), normalize(text));
                        }
                        EMB => has_emb = true,
                        LINKS => {
                            links.insert((
                                key.clone(),
                                e.qualifier.to_vec(),
                                e.cell.value().to_vec(),
                            ));
                        }
                        BACKLINKS => {
                            backlinks.insert((
                                e.qualifier.to_vec(),
                                key.clone(),
                                e.cell.value().to_vec(),
                            ));
                        }
                        _ => {}
                    }
                }
                if !has_text {
                    st.no_content += 1;
                }
                if self.model.is_some() && !has_emb {
                    st.missing_embeddings += 1;
                }
            } else if let Some(c) = row.view().get(META, RID) {
                dedups.push((key, c.value().to_vec()));
            } else {
                return Err(corrupt("a row that is neither a memory nor a dedup row"));
            }
        }
        for (key, target) in &dedups {
            let ok = texts.get(target).is_some_and(|norm| {
                let ns_len = key.windows(2).position(|w| w == DEDUP_TAG);
                ns_len.is_some_and(|at| {
                    dedup_row(
                        std::str::from_utf8(&key[..at]).unwrap_or(""),
                        &content_hash(norm),
                    ) == *key
                })
            });
            if !ok {
                st.stale_dedup += 1;
            }
        }
        // Each link (from, to, value) must have the backlink (from, to, value), and the
        // reverse; both ends live.
        for l in links.symmetric_difference(&backlinks) {
            let _ = l;
            st.broken_links += 1;
        }
        for (from, to, _) in &links {
            if !texts.contains_key(from) || !texts.contains_key(to) {
                st.broken_links += 1;
            }
        }
        st.text_index_inconsistent = !self.index.verify()?.is_consistent();
        Ok(st)
    }
}

fn write_link(txn: &mut Transaction, t: &Table, from: &[u8], to: &[u8], ty: LinkType, w: f32) {
    let mut v = [0u8; 5];
    v[0] = ty.0;
    v[1..].copy_from_slice(&w.to_le_bytes());
    txn.put(t, from, LINKS, to, &v)
        .put(t, to, BACKLINKS, from, &v);
}

fn read_link(q: &[u8], v: &[u8]) -> Result<(MemoryId, LinkType, f32)> {
    let id = MemoryId::from_row(q).ok_or_else(|| corrupt("a link's key"))?;
    if v.len() != 5 {
        return Err(corrupt("a link's value"));
    }
    let w = f32::from_le_bytes(v[1..].try_into().expect("4"));
    Ok((id, LinkType(v[0]), w))
}

fn element_code(e: Element) -> u8 {
    match e {
        Element::F16 => 1,
        Element::BF16 => 2,
        _ => 0,
    }
}

fn element_from_code(c: u8) -> Option<Element> {
    Some(match c {
        0 => Element::F32,
        1 => Element::F16,
        2 => Element::BF16,
        _ => return None,
    })
}

/// A memory from its row.
fn read_memory(id: MemoryId, row: &pigeonhole::Row) -> Result<Option<StoredMemory>> {
    let view = row.view();
    let Some(text) = view.get(CONTENT, TEXT) else {
        return Ok(None);
    };
    let text = std::str::from_utf8(text.value())
        .map_err(|_| corrupt("a memory's text"))?
        .to_owned();
    let kind = match view.get(META, KIND) {
        Some(c) => Kind::from_code(*c.value().first().ok_or_else(|| corrupt("a kind"))?)?,
        None => Kind::Episodic,
    };
    let importance = view
        .get(PEAK, IMPORTANCE)
        .and_then(|c| c.as_i64())
        .map_or(0.0, |p| p as f64 / PPM);
    let uses = view
        .get(STATS, USES)
        .and_then(|c| c.as_i64())
        .unwrap_or(0)
        .max(0) as u64;
    let last_used_us = view
        .get(PEAK, LAST_USED)
        .and_then(|c| c.as_i64())
        .map(|v| v.max(0) as u64);
    let session = view
        .get(META, SESSION)
        .map(|c| String::from_utf8_lossy(c.value()).into_owned());
    let pinned = view.get(META, PINNED).is_some();
    let superseded_by = view
        .get(META, SUPERSEDED_BY)
        .map(|c| MemoryId::from_row(c.value()).ok_or_else(|| corrupt("superseded_by")))
        .transpose()?;
    let embedding = view
        .get(EMB, V)
        .and_then(|c| c.as_vector().and_then(|v| v.to_f32_vec()));
    let mut sources = Vec::new();
    let mut links = Vec::new();
    let mut backlinks = Vec::new();
    for e in view.iter() {
        match e.family {
            SOURCES => sources.push((
                String::from_utf8_lossy(e.qualifier).into_owned(),
                e.cell.as_u64().unwrap_or(0),
            )),
            LINKS => links.push(read_link(e.qualifier, e.cell.value())?),
            BACKLINKS => backlinks.push(read_link(e.qualifier, e.cell.value())?),
            _ => {}
        }
    }
    Ok(Some(StoredMemory {
        id,
        text,
        kind,
        importance,
        uses,
        last_used_us,
        session,
        pinned,
        superseded_by,
        sources,
        links,
        backlinks,
        embedding,
    }))
}
