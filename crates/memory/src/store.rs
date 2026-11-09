//! The store (D311 §2, §4, ICR 0107): create and open with the pinned embedding model,
//! `remember` with exact dedup, `get`, `delete`, `link`, `supersede`, `mark_used`, `verify`.

use std::collections::{BTreeMap, BTreeSet};

use pigeonhole::{
    Aggregation, Element, Family, Metric, Pigeonhole, PigeonholeReader, ReadTable, RetryPolicy,
    Table, Transaction, VectorSpec,
};
use pigeonhole_text::{Index, IndexOptions, Normalization, ReadIndex};

use crate::dedup::{DedupPolicy, Near, NearDedup};
use crate::forget::{ForgetPolicy, Forgetter, Signal, UserToken};
use crate::ivf::{Ivf, IvfOptions, ivf_name};
use crate::recall::{Idx, Recall, Rows, Snap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};

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

    pub(crate) fn spec(&self) -> VectorSpec {
        VectorSpec::new(self.dim, self.element, Metric::Cosine).expect("checked at new")
    }
}

/// A store's options, fixed at create (the forget policy can change later:
/// [`Store::set_forget_policy`]).
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct StoreOptions {
    embedding: Option<EmbeddingModel>,
    forget: Option<ForgetPolicy>,
    near_dedup: Option<NearDedup>,
    normalization: Normalization,
    ivf: Option<IvfOptions>,
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

    /// The store's forget policy (T2): the [`Forgetter`] runs it, and
    /// [`Store::forget`] uses it by default. It replaces any policy set before, including an
    /// [`episodic_ttl`](Self::episodic_ttl): call that after this to add one.
    pub fn forget(mut self, policy: ForgetPolicy) -> Self {
        self.forget = Some(policy);
        self
    }

    /// Near-duplicate dedup (T3): see [`NearDedup`]. Needs an
    /// [`embedding`](Self::embedding) model. It can change later
    /// ([`Store::set_near_dedup`]).
    pub fn near_dedup(mut self, n: NearDedup) -> Self {
        self.near_dedup = Some(n);
        self
    }

    /// The Unicode normalisation of the store's text index, which keyword recall matches by:
    /// [`Normalization::Nfc`] by default. [`Normalization::Nfkc`] also folds ligatures and
    /// full-width forms into their plain ones, which suits memories gathered from mixed
    /// sources (PDF and OCR text, East Asian content). Fixed at create: the store reads it
    /// back from the index ([`Store::normalization`]). To change it, create a new store with
    /// the other setting and remember the old store's memories into it. The full-text
    /// guide's "Which normalisation to pick" has the trade-offs (#1291).
    pub fn normalization(mut self, n: Normalization) -> Self {
        self.normalization = n;
        self
    }

    /// IVF recall for large namespaces (D311 T8): the store keeps a `pigeonhole-vector`
    /// index of its embeddings (`mem_{name}_ivf`) beside `emb:v`, written in each memory's
    /// own transaction, and recall searches it for a namespace with at least
    /// [`IvfOptions::min_namespace_len`] memories with an embedding (an exact search below
    /// that). It needs an [`embedding`](Self::embedding) model, and costs a second copy of
    /// each embedding. Fixed at create: [`Store::ivf`] reads it back, and a store created
    /// without it never has the index.
    pub fn ivf(mut self, options: IvfOptions) -> Self {
        self.ivf = Some(options);
        self
    }

    /// Shorthand: episodic memories are forgotten `age` after their creation, as
    /// [`ForgetPolicy::max_age`] with [`Kind::Episodic`]. After [`forget`](Self::forget), it
    /// adds the age to that policy; before it, `forget` replaces it.
    pub fn episodic_ttl(mut self, age: std::time::Duration) -> Self {
        let p = self.forget.take().unwrap_or_default();
        self.forget = Some(p.max_age(Kind::Episodic, age));
        self
    }
}

/// How to open a store ([`Store::open_with`], [`Store::create_with`]).
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct OpenOptions {
    model: Option<String>,
    without_embeddings: bool,
    own_forgetter: bool,
    own_rebalancer: bool,
}

impl OpenOptions {
    /// The defaults: name the model with [`model`](Self::model), or open
    /// [`without_embeddings`](Self::without_embeddings).
    pub fn new() -> Self {
        Self::default()
    }

    /// The embedding model the caller embeds with, as [`Store::open`] checks it.
    pub fn model(mut self, id: impl Into<String>) -> Self {
        self.model = Some(id.into());
        self
    }

    /// No embedding model, as [`Store::open_without_embeddings`] opens.
    pub fn without_embeddings(mut self) -> Self {
        self.without_embeddings = true;
        self
    }

    /// No thread of the crate's own: the application takes the store's [`Forgetter`] with
    /// [`Store::take_forgetter`] and runs it itself (an application-owned database, D284).
    pub fn own_forgetter(mut self) -> Self {
        self.own_forgetter = true;
        self
    }

    /// No IVF rebalancing thread of the crate's own: for a store with an IVF index
    /// ([`StoreOptions::ivf`]), the application takes its [`Rebalancer`](pigeonhole_vector::Rebalancer)
    /// with [`Store::take_rebalancer`] and runs it itself (an application-owned database,
    /// D284), or drops it where nothing may write in the background (a replica, #1618). The
    /// index is opened as [`pigeonhole_vector::Index::open_with_rebalancer`] opens it.
    pub fn own_rebalancer(mut self) -> Self {
        self.own_rebalancer = true;
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
    pub(crate) fn code(self) -> u8 {
        match self {
            Kind::Episodic => 0,
            Kind::Semantic => 1,
            Kind::Procedural => 2,
        }
    }

    pub(crate) fn from_code(c: u8) -> Result<Self> {
        Ok(match c {
            0 => Kind::Episodic,
            1 => Kind::Semantic,
            2 => Kind::Procedural,
            _ => return Err(corrupt("a memory's kind")),
        })
    }
}

/// A link's type: the application's (for example "about" or "follows"), from 16 up; the
/// crate's own are below 16 (frozen in ICR 0107). A read returns whatever is stored, a
/// reserved value this build doesn't name included.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LinkType(u8);

impl LinkType {
    /// The first value an application may use.
    pub const FIRST_CUSTOM: u8 = 16;

    /// The application's link type `n`: 16 or above (`InvalidArgument` below, where the
    /// crate's own types live).
    pub fn custom(n: u8) -> Result<LinkType> {
        if n < Self::FIRST_CUSTOM {
            return Err(Error::InvalidArgument(format!(
                "link types below {} are the crate's; an application's start at {}",
                Self::FIRST_CUSTOM,
                Self::FIRST_CUSTOM
            )));
        }
        Ok(LinkType(n))
    }

    /// The type's stored value.
    pub fn get(self) -> u8 {
        self.0
    }

    /// A consolidated memory to each of its sources (T3).
    pub const DERIVED_FROM: LinkType = LinkType(1);
    /// The new memory to the one it supersedes ([`Store::supersede`]).
    pub const SUPERSEDES: LinkType = LinkType(2);
    /// A near duplicate to its match (T3's `DedupPolicy::Link`).
    pub const SIMILAR_TO: LinkType = LinkType(3);
}

/// A link to or from a memory ([`StoredMemory::links`], [`StoredMemory::backlinks`]).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Link {
    /// The memory at the other end.
    pub id: MemoryId,
    /// Its type.
    pub ty: LinkType,
    /// Its weight, from 0 to 1.
    pub weight: f32,
}

/// A provenance entry ([`StoredMemory::sources`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Source {
    /// The application's source id (a tool call id, a URL).
    pub name: String,
    /// When the store first saw it, in µs.
    pub at_us: u64,
}

/// What [`Store::mark_used`] did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Marked {
    /// How many memories were marked used.
    pub marked: usize,
    /// The ids that no longer exist (deleted), never resurrected: drop them from a working
    /// set.
    pub missing: Vec<MemoryId>,
}

/// A memory to write ([`Store::remember`]): a builder, so it can grow (D311 §10a).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Memory {
    pub(crate) text: String,
    pub(crate) embedding: Option<Vec<f32>>,
    pub(crate) kind: Kind,
    importance: f64,
    source: Option<String>,
    pub(crate) session: Option<String>,
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

    /// Its embedding, from the store's model. Required in a store pinned to a model. Into
    /// an `F16` or `BF16` model it's rounded to the element when stored (to nearest, ties
    /// to even, as `put_vector_f32` writes a half family).
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

    /// A link to the existing memory `to` in the same namespace, of type `ty`, with weight
    /// `weight` from 0 to 1 (`InvalidArgument` otherwise, NaN included).
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
    /// The text was already remembered (exact dedup), or a near duplicate was under
    /// [`DedupPolicy::Merge`]: merged into that memory, which kept the larger importance,
    /// gained the source, and counted a use.
    Merged(MemoryId),
    /// A near duplicate under [`DedupPolicy::Skip`] (T3): nothing was written, and this is
    /// the memory it's near.
    Similar(MemoryId),
}

impl Remembered {
    /// The memory written, merged into, or, for [`Similar`](Remembered::Similar), the near
    /// duplicate that stopped the write: a memory this call didn't write. Code that links or
    /// pins the returned id acts on the match once a store turns on [`DedupPolicy::Skip`].
    pub fn id(&self) -> &MemoryId {
        match self {
            Remembered::New(id) | Remembered::Merged(id) | Remembered::Similar(id) => id,
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
    /// Whether a consolidation took it in ([`Store::consolidate`], T3).
    pub consolidated: bool,
    /// Whether a consolidator wrote it, as a group's summary (T9, `meta:summary`). Memories
    /// from before T9 read `false`.
    pub summary: bool,
    /// The memory that superseded it, if one did.
    pub superseded_by: Option<MemoryId>,
    /// Its sources, each with its first-seen time (µs).
    pub sources: Vec<Source>,
    /// Its outgoing links.
    pub links: Vec<Link>,
    /// Its incoming links.
    pub backlinks: Vec<Link>,
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
    /// Memories whose `superseded_by` names a memory that's gone.
    pub dangling_superseded: u64,
    /// The text index's own check failed ([`pigeonhole_text::IndexStats::is_consistent`]).
    pub text_index_inconsistent: bool,
    /// For a store with an IVF index ([`StoreOptions::ivf`]): memories with an embedding
    /// that aren't in the index ([`ivf_missing`](Self::ivf_missing)) plus index vectors
    /// with no memory with an embedding ([`ivf_stale`](Self::ivf_stale)); 0 when the index
    /// holds exactly the embedded memories. The index's own row checks
    /// (`pigeonhole_vector::Index::verify_at`) run too, and fail `verify` as
    /// [`Error::Vector`] if they find corruption. Checked in the writer process only.
    ///
    /// Everything is read at `verify`'s one snapshot, so it holds under concurrent writes
    /// and [`is_consistent`](Self::is_consistent) counts it (#1427).
    pub ivf_mismatched: u64,
    /// Memories with an embedding that the IVF index doesn't hold (#1427). Part of
    /// [`ivf_mismatched`](Self::ivf_mismatched).
    pub ivf_missing: u64,
    /// IVF index vectors whose id isn't a memory with an embedding (#1427). Part of
    /// [`ivf_mismatched`](Self::ivf_mismatched).
    pub ivf_stale: u64,
    /// Namespaces whose counter (memories with an embedding, which chooses exact or IVF
    /// recall) differs from a count of them. It's advisory: drift changes only which path a
    /// recall takes, so it's reported apart from the mismatches above, and
    /// [`is_consistent`](Self::is_consistent) leaves it out.
    pub ivf_counter_drift: u64,
}

impl StoreStats {
    /// No mismatch at all.
    pub fn is_consistent(&self) -> bool {
        self.no_content == 0
            && self.stale_dedup == 0
            && self.broken_links == 0
            && self.missing_embeddings == 0
            && self.dangling_superseded == 0
            && !self.text_index_inconsistent
            && self.ivf_mismatched == 0
    }
}

/// A memory store, in the writer process: one table plus its text index (D311 §2). Clone it
/// to share it: clones are cheap and share everything (the database handle, the pinned model,
/// the persisted forget policy and the one [`Forgetter`], which stops when the last clone
/// drops); nothing is cached per clone, so a write through one is seen by all.
#[derive(Clone)]
pub struct Store {
    db: Pigeonhole,
    table: Table,
    index: Index,
    /// The IVF index, for a store created with [`StoreOptions::ivf`] (D311 T8).
    ivf: Option<Ivf>,
    /// The store's pinned model.
    model: Option<EmbeddingModel>,
    /// Whether this handle was opened with the store's model (always so for a store
    /// without one).
    embeds: bool,
    /// The application's handles: `None` for the forgetter's own, which mustn't keep it alive.
    user: Option<Arc<UserToken>>,
    shared: Arc<Shared>,
}

/// What a store's handles share: the forgetter's signal, and its forgetter while nobody runs
/// it.
struct Shared {
    signal: Arc<Signal>,
    forgetter: Mutex<Option<Forgetter>>,
    /// Whether a forgetter runs (the crate's thread) or was handed out.
    started: AtomicBool,
    own: bool,
    /// The IVF index's writes awaiting [`after_commit`](pigeonhole_vector::Index::after_commit)
    /// (D311 T8): each write's own record, made in its transaction's body, handed on once a
    /// write call's transaction returns. `after_commit` is harmless after a rollback, so a
    /// rolled-back write's record costs a count read at most.
    ///
    /// The list is shared by the store's handles, so one thread's settle can take another's
    /// record before that one commits: its `after_commit` then reads the counts too early,
    /// which only delays a split to the list's next write. At most [`IVF_PENDING_MAX`]
    /// records wait, the oldest dropped past it (simp724's R1 on #1424): a caller writing only
    /// through `remember_in`/`delete_in` never settles, and a dropped record only delays a
    /// split the same way.
    ivf_pending: Mutex<std::collections::VecDeque<pigeonhole_vector::Placed>>,
    /// The IVF index's rebalancer, kept for [`Store::take_rebalancer`] when opened with
    /// [`OpenOptions::own_rebalancer`].
    rebalancer: Mutex<Option<pigeonhole_vector::Rebalancer>>,
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

/// The most IVF write records a store keeps waiting to settle ([`Shared::ivf_pending`]).
pub(crate) const IVF_PENDING_MAX: usize = 1_024;

fn table_name(name: &str) -> String {
    format!("mem_{name}")
}

fn index_name(name: &str) -> String {
    format!("mem_{name}_text")
}

impl Store {
    /// The tables a store named `name` keeps its data in: its own, its text index's, and its
    /// IVF index's (a table that exists only with [`StoreOptions::ivf`]). For an application
    /// that must leave a store's tables alone: `phdb serve --memory` refuses to drop them
    /// (#1651).
    ///
    /// ```
    /// use pigeonhole_memory::Store;
    ///
    /// let names = Store::table_names("notes");
    /// assert!(names.contains(&"mem_notes".to_owned()));
    /// assert_eq!(names.len(), 3);
    /// ```
    pub fn table_names(name: &str) -> Vec<String> {
        vec![table_name(name), index_name(name), ivf_name(name)]
    }

    /// Creates the store `name`: the table `mem_{name}` and its text index
    /// `mem_{name}_text`, the index without positions (recall queries are rarely phrases;
    /// owner, 2026-11-02), and opens it with its own model (as [`open`](Self::open) would).
    /// With a forget policy, the crate's [`Forgetter`] thread starts. `TableExists` if the
    /// table is there.
    pub fn create(db: &Pigeonhole, name: &str, options: StoreOptions) -> Result<Store> {
        let open = match &options.embedding {
            Some(m) => OpenOptions::new().model(m.id.clone()),
            None => OpenOptions::new().without_embeddings(),
        };
        Self::create_with(db, name, options, open)
    }

    /// [`create`](Self::create), opened as `open` says ([`open_with`](Self::open_with)).
    pub fn create_with(
        db: &Pigeonhole,
        name: &str,
        store: StoreOptions,
        open: OpenOptions,
    ) -> Result<Store> {
        if let Some(p) = &store.forget {
            p.check()?;
        }
        if let Some(o) = &store.ivf {
            o.check()?;
            if store.embedding.is_none() {
                return Err(Error::InvalidArgument(
                    "an IVF index is of embeddings, and the store has no model".into(),
                ));
            }
        }
        if let Some(n) = &store.near_dedup {
            n.check()?;
            if store.embedding.is_none() {
                return Err(Error::InvalidArgument(
                    "near dedup compares embeddings, and the store has no model".into(),
                ));
            }
        }
        let mut b = db
            .table(&table_name(name))?
            .family(CONTENT, Family::default())
            .family(META, Family::default())
            .family(SOURCES, Family::default())
            .family(STATS, Family::counter())
            .family(PEAK, Family::aggregate(Aggregation::Max))
            .family(LINKS, Family::default())
            .family(BACKLINKS, Family::default());
        if let Some(m) = &store.embedding {
            b = b.family(EMB, Family::vector(m.spec()));
        }
        let table = b.create()?;
        let mut g = table
            .mutate(GLOBAL)
            .put_u64(META, LAYOUT, LAYOUT_VERSION)
            .put(META, TEXT_INDEX, index_name(name).as_bytes());
        if let Some(m) = &store.embedding {
            g = g
                .put(META, MODEL_ID, m.id.as_bytes())
                .put_u64(META, MODEL_DIM, u64::from(m.dim))
                .put(META, MODEL_ELEMENT, &[element_code(m.element)]);
        }
        if let Some(p) = &store.forget {
            g = g.put(META, FORGET, &p.encode());
        }
        if let Some(n) = &store.near_dedup {
            g = g.put(META, NEAR_DEDUP, &n.encode());
        }
        if let (Some(o), Some(m)) = (&store.ivf, &store.embedding) {
            let mut v = pigeonhole_vector::IndexOptions::new()
                .rebalance(pigeonhole_vector::Rebalance::Background);
            if let Some(n) = o.get_nprobe() {
                v = v.nprobe(n);
            }
            if let Some(n) = o.get_target_list_len() {
                v = v.target_list_len(n);
            }
            // Created before `G` names it, so a store whose `G` has `ivf` has the index.
            // With `own_rebalancer`, no crate thread starts here either: the rebalancer the
            // open below hands out takes from the same queue.
            if open.own_rebalancer {
                drop(pigeonhole_vector::Index::create_with_rebalancer(
                    db,
                    &ivf_name(name),
                    m.spec(),
                    v,
                )?);
            } else {
                drop(pigeonhole_vector::Index::create(
                    db,
                    &ivf_name(name),
                    m.spec(),
                    v,
                )?);
            }
            g = g.put(META, IVF, &o.encode());
        }
        g.commit()?;
        Index::create(
            db,
            &index_name(name),
            pigeonhole_text::Source::new(&table, CONTENT, TEXT),
            IndexOptions::new()
                .positions(false)
                .normalization(store.normalization),
        )?;
        Self::open_with(db, name, open)
    }

    /// Opens the store `name` for a caller embedding with `model_id`, compared byte for
    /// byte with the store's. `Mismatch` if it's another model; `InvalidArgument` if the
    /// store has none (use [`open_without_embeddings`](Self::open_without_embeddings)).
    /// With a forget policy, the crate's [`Forgetter`] thread starts.
    pub fn open(db: &Pigeonhole, name: &str, model_id: &str) -> Result<Store> {
        Self::open_with(db, name, OpenOptions::new().model(model_id))
    }

    /// Opens the store `name` without an embedding model. On a store pinned to one, the
    /// handle refuses every write that creates a memory or changes its content
    /// (`remember`, `supersede`), so every memory there keeps its embedding; it allows
    /// `delete`, `link` and `mark_used`, and recalls by keyword and recency.
    pub fn open_without_embeddings(db: &Pigeonhole, name: &str) -> Result<Store> {
        Self::open_with(db, name, OpenOptions::new().without_embeddings())
    }

    /// Opens the store `name` as `options` say: with the caller's model
    /// ([`OpenOptions::model`]) or without one, and with the crate's forgetter thread or the
    /// application's ([`OpenOptions::own_forgetter`], then
    /// [`take_forgetter`](Self::take_forgetter)). `InvalidArgument` if `options` name neither
    /// a model nor `without_embeddings`, or both.
    pub fn open_with(db: &Pigeonhole, name: &str, options: OpenOptions) -> Result<Store> {
        let table = db.table(&table_name(name))?.open()?;
        let model = load(Rows::W(&table))?;
        let embeds = match (&options.model, options.without_embeddings) {
            (Some(id), false) => {
                check_model(&model, name, id)?;
                true
            }
            (None, true) => model.is_none(),
            _ => {
                return Err(Error::InvalidArgument(
                    "OpenOptions takes a model or without_embeddings, one of the two".into(),
                ));
            }
        };
        let index = Index::open(db, &index_name(name))?;
        let mut rebalancer = None;
        let ivf = match table.get(GLOBAL, META, IVF)? {
            Some(c) => {
                let index = if options.own_rebalancer {
                    let (index, r) =
                        pigeonhole_vector::Index::open_with_rebalancer(db, &ivf_name(name))?;
                    rebalancer = Some(r);
                    index
                } else {
                    pigeonhole_vector::Index::open(db, &ivf_name(name))?
                };
                Some(Ivf::W(index, IvfOptions::decode(c.value())?))
            }
            None => None,
        };
        let s = Store::assemble(db, table, index, ivf, model, embeds, options.own_forgetter);
        *s.shared
            .rebalancer
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = rebalancer;
        s.start_forgetter_if_due()?;
        Ok(s)
    }

    fn assemble(
        db: &Pigeonhole,
        table: Table,
        index: Index,
        ivf: Option<Ivf>,
        model: Option<EmbeddingModel>,
        embeds: bool,
        own: bool,
    ) -> Store {
        let signal = Arc::new(Signal::default());
        Store {
            db: db.clone(),
            table,
            index,
            ivf,
            model,
            embeds,
            user: Some(Arc::new(UserToken(Arc::clone(&signal)))),
            shared: Arc::new(Shared {
                signal,
                forgetter: Mutex::new(None),
                started: AtomicBool::new(false),
                own,
                ivf_pending: Mutex::new(std::collections::VecDeque::new()),
                rebalancer: Mutex::new(None),
            }),
        }
    }

    /// Makes the store's forgetter once: on the crate's thread when the store has a policy
    /// (unless the application owns it), or kept for [`take_forgetter`](Self::take_forgetter).
    fn start_forgetter_if_due(&self) -> Result<()> {
        if self.shared.own {
            if !self.shared.started.swap(true, Ordering::AcqRel) {
                *self
                    .shared
                    .forgetter
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = Some(self.make_forgetter());
            }
            return Ok(());
        }
        if self.forget_policy()?.is_none() || self.shared.started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        crate::forget::spawn(self.make_forgetter())
            .map_err(|e| Error::InvalidArgument(format!("starting the forgetter: {e}")))
    }

    fn make_forgetter(&self) -> Forgetter {
        let mut inner = self.clone();
        inner.user = None;
        let user = self.user.as_ref().map_or_else(Weak::new, Arc::downgrade);
        Forgetter::new(inner, user, Arc::clone(&self.shared.signal))
    }

    /// The store's [`Forgetter`], for an application that runs it itself: opened with
    /// [`OpenOptions::own_forgetter`]. `None` otherwise, or once taken.
    pub fn take_forgetter(&self) -> Option<Forgetter> {
        self.shared
            .forgetter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// The IVF index's [`Rebalancer`](pigeonhole_vector::Rebalancer), for an application that
    /// runs it itself: a store with an IVF index opened with [`OpenOptions::own_rebalancer`].
    /// `None` otherwise, or once taken.
    pub fn take_rebalancer(&self) -> Option<pigeonhole_vector::Rebalancer> {
        self.shared
            .rebalancer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// The store's forget policy, if it has one.
    pub fn forget_policy(&self) -> Result<Option<ForgetPolicy>> {
        match self.table.get(GLOBAL, META, FORGET)? {
            Some(c) => Ok(Some(ForgetPolicy::decode(c.value())?)),
            None => Ok(None),
        }
    }

    /// Sets the store's forget policy (`None` turns forgetting off). The forgetter reads it
    /// before each pass, so it takes effect at the next pass; setting one wakes this
    /// process's forgetter, and starts the crate's thread if none runs yet.
    pub fn set_forget_policy(&self, policy: Option<ForgetPolicy>) -> Result<()> {
        match &policy {
            Some(p) => {
                p.check()?;
                self.table
                    .mutate(GLOBAL)
                    .put(META, FORGET, &p.encode())
                    .commit()?;
            }
            None => {
                self.table
                    .mutate(GLOBAL)
                    .delete_column(META, FORGET)
                    .commit()?;
            }
        }
        if policy.is_some() {
            self.start_forgetter_if_due()?;
            self.shared.signal.notify();
        }
        Ok(())
    }

    pub(crate) fn db(&self) -> &Pigeonhole {
        &self.db
    }

    pub(crate) fn table(&self) -> &Table {
        &self.table
    }

    /// Recalls memories from `namespaces` (D311 §3): each an exact namespace, as written
    /// (no prefixes or patterns), fused together into one ranking. At least one is needed:
    /// an empty list fails `run` with `InvalidArgument`. Set `text`, an `embedding`, or both,
    /// then [`run`](Recall::run). On a handle opened without the store's model, recall is
    /// by keyword and recency only.
    pub fn recall(&self, namespaces: &[&str]) -> Recall<'_> {
        Recall::new(
            Rows::W(&self.table),
            Idx::W(&self.index),
            self.ivf.as_ref(),
            self.model.as_ref().filter(|_| self.embeds),
            Snap::W(&self.db),
            namespaces,
        )
    }

    /// The model the store is pinned to, if any.
    pub fn embedding_model(&self) -> Option<&EmbeddingModel> {
        self.model.as_ref()
    }

    /// Watches the store's memory events in `namespaces` (every namespace when empty)
    /// through the database's change feed (D311 T9): see [`Watch`](crate::Watch). The feed
    /// must be on and follow the store's table (`mem_{name}`); `open` says so otherwise.
    ///
    /// One namespace is filtered by the feed itself. Several, or none (every namespace),
    /// read every commit of the store's table and filter here, so a watch of a few
    /// namespaces in a busy store costs as much as a watch of all of them (simp724's N2 on
    /// #1430).
    pub fn watch(&self, namespaces: &[&str]) -> crate::WatchBuilder {
        crate::watch::WatchBuilder::new(self.table.feed(), namespaces)
    }

    /// A feed-driven consolidation loop named `name` (D311 T9): see
    /// [`ConsolidateLoop`](crate::ConsolidateLoop). The name keys its saved position in the
    /// store: non-empty, without `:`.
    ///
    /// The loop owns what it needs (a handle on the store, the consolidator, its clock), so
    /// it can be kept across calls or moved to a thread of its own.
    pub fn consolidate_loop(
        &self,
        name: &str,
        consolidator: std::sync::Arc<dyn crate::Consolidator>,
    ) -> crate::ConsolidateLoop {
        crate::feed_loop::ConsolidateLoop::new(self, name, consolidator)
    }

    /// The store's IVF options, if it was created with [`StoreOptions::ivf`].
    pub fn ivf(&self) -> Option<IvfOptions> {
        self.ivf.as_ref().map(Ivf::options)
    }

    /// The Unicode normalisation of the store's text index, set at create
    /// ([`StoreOptions::normalization`]).
    pub fn normalization(&self) -> Normalization {
        self.index.options().get_normalization()
    }

    /// Remembers `memory` in namespace `ns`, in one transaction (D311 §4): the memory row,
    /// the text index, the dedup row, and both directions of each link. An exact duplicate
    /// (after trimming and collapsing whitespace) of a live, unsuperseded memory merges into
    /// it instead.
    ///
    /// With [near dedup](NearDedup), the nearest live memory is looked up first, outside the
    /// transaction, and a close enough one is skipped, merged into or linked to.
    pub fn remember(&self, ns: &str, memory: Memory) -> Result<Remembered> {
        let p = self.prepare_remember(ns, memory)?;
        #[cfg(feature = "__unstable-hooks")]
        if p.near.is_some() {
            crate::hooks::hold(crate::hooks::Hold::RememberAfterNear);
        }
        let r = self.db.transact(&RetryPolicy::default(), |txn| {
            self.remember_prepared_in(txn, &p)
        });
        self.settle();
        Ok(r?.0)
    }

    /// The part of [`remember`](Self::remember) before its transaction (ICR 0123): checks
    /// the memory, runs the near-dedup lookup if the store has near dedup (a read outside
    /// any transaction), and fixes the new memory's id and time. Then commit
    /// [`remember_prepared_in`](Self::remember_prepared_in) in a transaction of your own,
    /// retrying it as you like (each attempt writes the same memory), and
    /// [`settle`](Self::settle) after it commits. That's `remember`, step by step, for a
    /// caller with its own retry loop (#1479's asyncio forms).
    pub fn prepare_remember(&self, ns: &str, memory: Memory) -> Result<PreparedMemory> {
        self.check_memory(ns, &memory)?;
        let near = match self.near_dedup()? {
            Some(setting) => self.near(ns, &memory, setting)?,
            None => None,
        };
        let now = now_us();
        let rid = new_rid(now)?;
        Ok(PreparedMemory {
            ns: ns.to_owned(),
            memory,
            rid,
            now,
            near,
        })
    }

    /// One attempt of [`remember`](Self::remember)'s transaction, in the caller's
    /// transaction: exact dedup, and the near-dedup outcome `prepare` found (ICR 0123).
    pub fn remember_prepared_in(
        &self,
        txn: &mut Transaction,
        p: &PreparedMemory,
    ) -> Result<Remembered> {
        self.remember_with(txn, &p.ns, &p.memory, p.rid, p.now, p.near.as_ref())
    }

    /// [`remember`](Self::remember) inside the caller's transaction, with exact dedup but
    /// no near dedup: that check reads outside a transaction, so it couldn't be part of the
    /// caller's. Call [`near_duplicate`](Self::near_duplicate) first to do it yourself.
    pub fn remember_in(
        &self,
        txn: &mut Transaction,
        ns: &str,
        memory: Memory,
    ) -> Result<Remembered> {
        self.check_memory(ns, &memory)?;
        let now = now_us();
        let rid = new_rid(now)?;
        self.remember_with(txn, ns, &memory, rid, now, None)
    }

    pub(crate) fn check_memory(&self, ns: &str, m: &Memory) -> Result<()> {
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
            check_weight(*w)?;
        }
        Ok(())
    }

    pub(crate) fn remember_with(
        &self,
        txn: &mut Transaction,
        ns: &str,
        m: &Memory,
        rid: [u8; 16],
        now: u64,
        near: Option<&Near>,
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
                return self.insert_near(txn, ns, m, rid, now, importance, None, near);
            }
        }
        self.insert_near(txn, ns, m, rid, now, importance, dedup.as_deref(), near)
    }

    /// A new memory, unless a near duplicate's policy says otherwise (T3).
    #[allow(clippy::too_many_arguments)]
    fn insert_near(
        &self,
        txn: &mut Transaction,
        ns: &str,
        m: &Memory,
        rid: [u8; 16],
        now: u64,
        importance: i64,
        dedup: Option<&[u8]>,
        near: Option<&Near>,
    ) -> Result<Remembered> {
        let t = &self.table;
        let Some(n) = near else {
            return self.insert(txn, ns, m, rid, now, importance, dedup);
        };
        let id = || MemoryId::from_row(&n.row).ok_or_else(|| corrupt("a near match's row"));
        // The match read here, for every policy (M1's rule for a near match; simp724's F1 on
        // #1386 for Skip): gone or superseded since the check, it's ignored, and the memory
        // is written as new.
        let live = txn.get(t, &n.row, CONTENT, TEXT)?.is_some()
            && txn.get(t, &n.row, META, SUPERSEDED_BY)?.is_none();
        match n.policy {
            DedupPolicy::Skip if live => Ok(Remembered::Similar(id()?)),
            DedupPolicy::Merge if live => {
                txn.aggregate(t, &n.row, PEAK, IMPORTANCE, importance)
                    .incr(t, &n.row, STATS, USES, 1);
                if let Some(src) = &m.source {
                    self.add_source(txn, &n.row, src, now)?;
                }
                Ok(Remembered::Merged(id()?))
            }
            DedupPolicy::Link if live => {
                let r = self.insert(txn, ns, m, rid, now, importance, dedup)?;
                let w = n.similarity.clamp(0.0, 1.0);
                write_link(txn, t, &r.id().row(), &n.row, LinkType::SIMILAR_TO, w);
                Ok(r)
            }
            _ => self.insert(txn, ns, m, rid, now, importance, dedup),
        }
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
            self.ivf_insert(txn, ns, &row, e)?;
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
    pub(crate) fn add_source(
        &self,
        txn: &mut Transaction,
        row: &[u8],
        src: &str,
        now: u64,
    ) -> Result<()> {
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
        let r = self
            .db
            .transact(&RetryPolicy::default(), |txn| self.delete_in(txn, id));
        self.settle();
        Ok(r?.0)
    }

    /// [`delete`](Self::delete) inside the caller's transaction.
    ///
    /// Every removal of a memory goes through here (`forget` and consolidation's `Delete`
    /// too), so it's also where the IVF index and its namespace counter follow (D311 T8). A
    /// future removal path, a column TTL say, must call it rather than delete rows itself
    /// (simp724 on #1342).
    ///
    /// It reads at `txn`'s snapshot, so a memory this same transaction remembered isn't
    /// there yet: `false`, and nothing deleted (neither the memory nor, in an IVF store, its
    /// vector).
    pub fn delete_in(&self, txn: &mut Transaction, id: &MemoryId) -> Result<bool> {
        let t = &self.table;
        let row = id.row();
        let Some(text) = txn.get(t, &row, CONTENT, TEXT)?.map(|c| c.value().to_vec()) else {
            return Ok(false);
        };
        let (links, backlinks, superseded) = {
            let r = txn.get_row(t, &row, &[LINKS, BACKLINKS])?;
            let mut out = (Vec::new(), Vec::new(), Vec::new());
            if let Some(r) = r {
                for e in r.iter() {
                    match e.family {
                        LINKS => {
                            if e.cell.value().first() == Some(&LinkType::SUPERSEDES.0) {
                                out.2.push(e.qualifier.to_vec());
                            }
                            out.0.push(e.qualifier.to_vec());
                        }
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
        // A memory this one superseded is recalled again (simp724's G1 on #1362): its
        // `superseded_by`, read here, is cleared if it names this memory.
        for to in &superseded {
            if txn
                .get(t, to, META, SUPERSEDED_BY)?
                .is_some_and(|c| c.value() == row.as_slice())
            {
                txn.delete_column(t, to, META, SUPERSEDED_BY)
                    .delete_column(t, to, META, VALID_TO);
            }
        }
        for to in &links {
            txn.delete_column(t, to, BACKLINKS, &row);
        }
        for from in &backlinks {
            txn.delete_column(t, from, LINKS, &row);
        }
        self.index.delete_in(txn, &row)?;
        self.ivf_delete(txn, id.namespace(), &row)?;
        txn.delete_row(t, &row);
        Ok(true)
    }

    /// The IVF index's half of a memory's insert (D311 T8): its vector in the index, in the
    /// memory's transaction, and the namespace counter's blind increment.
    fn ivf_insert(&self, txn: &mut Transaction, ns: &str, row: &[u8], e: &[f32]) -> Result<()> {
        let Some(Ivf::W(index, _)) = &self.ivf else {
            return Ok(());
        };
        let mut placed = pigeonhole_vector::Placed::new();
        index.insert_f32_in(txn, &mut placed, row, e)?;
        crate::ivf::counted(txn, &self.table, ns, 1);
        self.park(placed);
        Ok(())
    }

    /// The IVF index's half of a memory's delete: the counter goes down only when the index
    /// held the memory's vector (a memory without an embedding has none).
    fn ivf_delete(&self, txn: &mut Transaction, ns: &str, row: &[u8]) -> Result<()> {
        let Some(Ivf::W(index, _)) = &self.ivf else {
            return Ok(());
        };
        let mut placed = pigeonhole_vector::Placed::new();
        if index.delete_in(txn, &mut placed, row)? {
            crate::ivf::counted(txn, &self.table, ns, -1);
            self.park(placed);
        }
        Ok(())
    }

    fn park(&self, placed: pigeonhole_vector::Placed) {
        let mut q = self
            .shared
            .ivf_pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if q.len() >= IVF_PENDING_MAX {
            q.pop_front();
        }
        q.push_back(placed);
    }

    /// The IVF records waiting to settle (an inspection helper for `hooks`).
    #[cfg(feature = "__unstable-hooks")]
    pub(crate) fn ivf_pending_len(&self) -> usize {
        self.shared
            .ivf_pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Hands the IVF index's parked writes to its `after_commit`, which splits or merges the
    /// lists they left out of bounds (in background mode, marks them for the rebalancer):
    /// what [`remember`](Self::remember) and [`delete`](Self::delete) do after their
    /// transaction. Call it after committing a transaction of your own that ran
    /// [`remember_in`](Self::remember_in) or [`delete_in`](Self::delete_in) (ICR 0123): with
    /// inline rebalance it may commit splits itself, so hold no snapshot across it (#1513).
    ///
    /// Its errors are dropped: the memories are committed, and a missed rebalance waits for
    /// the next write to those lists (simp724 on #1342: an `after_commit` failure mustn't
    /// fail a write). Without settling, nothing is wrong for recall: the vectors are in
    /// their lists, and a list just stays over its bound until a later write settles it. A
    /// store without an IVF index does nothing here.
    pub fn settle(&self) {
        let Some(Ivf::W(index, _)) = &self.ivf else {
            return;
        };
        let pending = std::mem::take(
            &mut *self
                .shared
                .ivf_pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for p in pending {
            let _ = index.after_commit(p);
        }
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
        check_weight(weight)?;
        self.db.transact(&RetryPolicy::default(), |txn| {
            self.link_in(txn, from, to, ty, weight)
        })?;
        Ok(())
    }

    /// [`link`](Self::link) inside the caller's transaction (ICR 0123): it reads both ends'
    /// `content:text` in `txn`, so a concurrent delete of either conflicts at commit.
    pub fn link_in(
        &self,
        txn: &mut Transaction,
        from: &MemoryId,
        to: &MemoryId,
        ty: LinkType,
        weight: f32,
    ) -> Result<()> {
        if from.namespace() != to.namespace() {
            return Err(Error::InvalidArgument(
                "links stay within a namespace".into(),
            ));
        }
        check_weight(weight)?;
        let t = &self.table;
        for end in [from, to] {
            if txn.get(t, &end.row(), CONTENT, TEXT)?.is_none() {
                return Err(Error::NotFound(format!("the memory {end}")));
            }
        }
        write_link(txn, t, &from.row(), &to.row(), ty, weight);
        Ok(())
    }

    /// Marks `old` superseded by `new` (D311 §2, M4): sets `old`'s `superseded_by` and
    /// `valid_to`, and links `new → old` with [`LinkType::SUPERSEDES`]. Recall leaves `old`
    /// out by default. `NotFound` if either is gone.
    ///
    /// A memory is superseded once: `old`'s `superseded_by` is read in the transaction, so
    /// two concurrent supersedes of it conflict and one retries. Superseding it again with
    /// the same `new` does nothing; with another, it's `InvalidArgument` naming the current
    /// one (supersede that one instead: facts change along a chain). Deleting `new` clears
    /// `old`'s `superseded_by`, so `old` is recalled again.
    pub fn supersede(&self, old: &MemoryId, new: &MemoryId) -> Result<()> {
        self.check_supersede(old, new)?;
        let now = now_us();
        self.db.transact(&RetryPolicy::default(), |txn| {
            self.supersede_at(txn, old, new, now)
        })?;
        Ok(())
    }

    /// [`supersede`](Self::supersede) inside the caller's transaction (ICR 0123), with
    /// `valid_to` the time of the call.
    pub fn supersede_in(
        &self,
        txn: &mut Transaction,
        old: &MemoryId,
        new: &MemoryId,
    ) -> Result<()> {
        self.check_supersede(old, new)?;
        self.supersede_at(txn, old, new, now_us())
    }

    fn check_supersede(&self, old: &MemoryId, new: &MemoryId) -> Result<()> {
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
        Ok(())
    }

    /// The supersede's reads and writes in `txn`, `valid_to` being `now`.
    fn supersede_at(
        &self,
        txn: &mut Transaction,
        old: &MemoryId,
        new: &MemoryId,
        now: u64,
    ) -> Result<()> {
        let t = &self.table;
        for end in [old, new] {
            if txn.get(t, &end.row(), CONTENT, TEXT)?.is_none() {
                return Err(Error::NotFound(format!("the memory {end}")));
            }
        }
        if let Some(cur) = txn
            .get(t, &old.row(), META, SUPERSEDED_BY)?
            .map(|c| c.value().to_vec())
        {
            if cur == new.row() {
                return Ok(());
            }
            let cur = MemoryId::from_row(&cur).ok_or_else(|| corrupt("superseded_by"))?;
            return Err(Error::InvalidArgument(format!(
                "{old} is already superseded by {cur}: supersede that one instead"
            )));
        }
        supersede_in(txn, t, &old.row(), &new.row(), now);
        Ok(())
    }

    /// Counts a use of each of `ids` (M5): reads each one's `content:text`, then writes the
    /// blind `uses` increment and the `last_used` max. A deleted id is skipped, never
    /// resurrected, and reported in [`Marked::missing`]. A repeated id counts once.
    pub fn mark_used(&self, ids: &[MemoryId]) -> Result<Marked> {
        let now = now_us() as i64;
        let (m, _) = self.db.transact(&RetryPolicy::default(), |txn| {
            self.mark_used_at(txn, ids, now)
        })?;
        Ok(m)
    }

    /// [`mark_used`](Self::mark_used) inside the caller's transaction (ICR 0123), at the
    /// time of the call.
    pub fn mark_used_in(&self, txn: &mut Transaction, ids: &[MemoryId]) -> Result<Marked> {
        self.mark_used_at(txn, ids, now_us() as i64)
    }

    fn mark_used_at(&self, txn: &mut Transaction, ids: &[MemoryId], now: i64) -> Result<Marked> {
        let t = &self.table;
        let mut m = Marked::default();
        let mut seen = BTreeSet::new();
        for id in ids {
            let row = id.row();
            if !seen.insert(row.clone()) {
                continue;
            }
            if txn.get(t, &row, CONTENT, TEXT)?.is_none() {
                m.missing.push(id.clone());
                continue;
            }
            txn.incr(t, &row, STATS, USES, 1)
                .aggregate(t, &row, PEAK, LAST_USED, now);
            m.marked += 1;
        }
        Ok(m)
    }

    /// Checks the store against itself and its text index, at one snapshot (D311 T1):
    /// every memory row has content, every dedup row names a live memory with that text,
    /// every link has its backlink with both ends live, and the text index agrees.
    pub fn verify(&self) -> Result<StoreStats> {
        let s = self.db.snapshot()?;
        let text_ok = self.index.verify()?.is_consistent();
        // The index's own rows at `verify`'s snapshot; its ids are compared there too (#1427).
        let ivf_index = match &self.ivf {
            Some(Ivf::W(index, _)) => {
                index.verify_at(&s)?;
                Some(index)
            }
            _ => None,
        };
        let ivf = self.ivf.is_some();
        verify(
            Rows::W(&self.table),
            &s,
            self.model.as_ref(),
            text_ok,
            ivf,
            ivf_index,
        )
    }
}

/// A memory store opened from a reader process (D311 §5): recall, `get` and `verify` at
/// the reader's snapshots, and no writes.
pub struct ReadStore {
    reader: PigeonholeReader,
    table: ReadTable,
    index: ReadIndex,
    ivf: Option<Ivf>,
    model: Option<EmbeddingModel>,
    embeds: bool,
}

impl std::fmt::Debug for ReadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadStore")
            .field("table", &self.table.name())
            .field("model", &self.model)
            .field("embeds", &self.embeds)
            .finish()
    }
}

impl Store {
    /// Opens the store `name` from a reader process, for a caller embedding with
    /// `model_id` (as [`open`](Store::open) checks it).
    pub fn open_reader(reader: &PigeonholeReader, name: &str, model_id: &str) -> Result<ReadStore> {
        let mut s = Self::open_reader_inner(reader, name)?;
        check_model(&s.model, name, model_id)?;
        s.embeds = true;
        Ok(s)
    }

    /// Opens the store `name` from a reader process without an embedding model: recall is
    /// by keyword and recency.
    pub fn open_reader_without_embeddings(
        reader: &PigeonholeReader,
        name: &str,
    ) -> Result<ReadStore> {
        let mut s = Self::open_reader_inner(reader, name)?;
        s.embeds = s.model.is_none();
        Ok(s)
    }

    fn open_reader_inner(reader: &PigeonholeReader, name: &str) -> Result<ReadStore> {
        let table = reader.table(&table_name(name))?;
        let model = load(Rows::R(&table))?;
        let index = Index::open_reader(reader, &index_name(name))?;
        let ivf = match Rows::R(&table).global(IVF)? {
            Some(b) => Some(Ivf::R(
                pigeonhole_vector::Index::open_reader(reader, &ivf_name(name))?,
                IvfOptions::decode(&b)?,
            )),
            None => None,
        };
        Ok(ReadStore {
            reader: reader.clone(),
            table,
            index,
            ivf,
            model,
            embeds: false,
        })
    }
}

impl ReadStore {
    /// The model the store is pinned to, if any.
    pub fn embedding_model(&self) -> Option<&EmbeddingModel> {
        self.model.as_ref()
    }

    /// [`Store::watch`], from a reader process: the writer's memory events, from its feed
    /// history (D310). Call [`Watch::next`](crate::Watch::next) on a thread of its own: it
    /// blocks.
    pub fn watch(&self, namespaces: &[&str]) -> crate::WatchBuilder {
        crate::watch::WatchBuilder::new(self.table.feed(), namespaces)
    }

    /// The store's IVF options ([`Store::ivf`]).
    pub fn ivf(&self) -> Option<IvfOptions> {
        self.ivf.as_ref().map(Ivf::options)
    }

    /// The Unicode normalisation of the store's text index ([`Store::normalization`]).
    pub fn normalization(&self) -> Normalization {
        self.index.options().get_normalization()
    }

    /// Recalls memories, as [`Store::recall`] does, at the reader's snapshots. A search
    /// while the writer's text index is being repaired fails with `Busy`.
    pub fn recall(&self, namespaces: &[&str]) -> Recall<'_> {
        Recall::new(
            Rows::R(&self.table),
            Idx::R(&self.index),
            self.ivf.as_ref(),
            self.model.as_ref().filter(|_| self.embeds),
            Snap::R(&self.reader),
            namespaces,
        )
    }

    /// The memory `id`, or `None` if it doesn't exist.
    pub fn get(&self, id: &MemoryId) -> Result<Option<StoredMemory>> {
        let Some(row) = self.table.row(&id.row()).read()? else {
            return Ok(None);
        };
        read_memory(id.clone(), &row.to_owned())
    }

    /// [`Store::verify`], at the reader's snapshot.
    pub fn verify(&self) -> Result<StoreStats> {
        let s = self.reader.snapshot()?;
        let text_ok = self.index.verify()?.is_consistent();
        let ivf = self.ivf.is_some();
        verify(
            Rows::R(&self.table),
            &s,
            self.model.as_ref(),
            text_ok,
            ivf,
            None,
        )
    }
}

/// `open`'s model check.
fn check_model(model: &Option<EmbeddingModel>, name: &str, model_id: &str) -> Result<()> {
    match model {
        Some(m) if m.id == model_id => Ok(()),
        Some(m) => Err(Error::Mismatch(format!(
            "the store {name} is pinned to {:?}, not {model_id:?}",
            m.id
        ))),
        None => Err(Error::InvalidArgument(format!(
            "the store {name} has no embedding model: open it without embeddings"
        ))),
    }
}

/// The store's families and `G` row, checked; its pinned model.
fn load(rows: Rows<'_>) -> Result<Option<EmbeddingModel>> {
    let not_store = || Error::InvalidArgument(format!("{} is not a memory store", rows.name()));
    for f in [CONTENT, META, SOURCES, STATS, PEAK, LINKS, BACKLINKS] {
        if !rows.has_family(f) {
            return Err(not_store());
        }
    }
    let layout = rows.global_u64(LAYOUT)?.ok_or_else(not_store)?;
    if layout != LAYOUT_VERSION {
        return Err(Error::InvalidArgument(format!(
            "{} has store layout {layout}, and this build reads {LAYOUT_VERSION}",
            rows.name()
        )));
    }
    Ok(match rows.global(MODEL_ID)? {
        None => None,
        Some(id) => {
            let id = String::from_utf8(id).map_err(|_| corrupt("the model id"))?;
            let dim = rows
                .global_u64(MODEL_DIM)?
                .ok_or_else(|| corrupt("the model's dimension"))?;
            let element = rows
                .global(MODEL_ELEMENT)?
                .and_then(|e| e.first().copied())
                .and_then(element_from_code)
                .ok_or_else(|| corrupt("the model's element"))?;
            Some(EmbeddingModel::new(id, dim as u32, element)?)
        }
    })
}

/// [`Store::verify`]'s checks, over either kind of table.
fn verify(
    rows: Rows<'_>,
    s: &pigeonhole::Snapshot,
    model: Option<&EmbeddingModel>,
    text_ok: bool,
    ivf: bool,
    ivf_index: Option<&pigeonhole_vector::Index>,
) -> Result<StoreStats> {
    let mut v = Scanned::default();
    rows.scan_all(s, |key, row| {
        v.row(key, row, model.is_some(), ivf, ivf_index.is_some())
    })?;
    v.check_rows();
    v.st.text_index_inconsistent = !text_ok;
    if ivf {
        v.check_ivf(s, ivf_index)?;
    }
    Ok(v.st)
}

/// A link as `(from, to, value)`.
type LinkKey = (Vec<u8>, Vec<u8>, Vec<u8>);

/// What [`verify`]'s scan collects, and the stats it's counted so far.
#[derive(Default)]
struct Scanned {
    st: StoreStats,
    /// Per namespace: memories with an embedding (D311 T8).
    embedded: BTreeMap<Vec<u8>, i64>,
    /// The rows of memories with an embedding: the ids the IVF index must hold (#1427).
    embedded_rows: BTreeSet<Vec<u8>>,
    /// Per namespace: the embedded counter's value.
    counters: BTreeMap<Vec<u8>, i64>,
    texts: BTreeMap<Vec<u8>, String>,
    links: BTreeSet<LinkKey>,
    /// Backlinks, keyed the way their link is.
    backlinks: BTreeSet<LinkKey>,
    dedups: Vec<(Vec<u8>, Vec<u8>)>,
    superseded_by: Vec<Vec<u8>>,
}

impl Scanned {
    /// One row of the scan.
    fn row(
        &mut self,
        key: &[u8],
        row: &pigeonhole::RowRef<'_>,
        has_model: bool,
        ivf: bool,
        ivf_ids: bool,
    ) -> Result<()> {
        if key == GLOBAL {
            return Ok(());
        }
        if let Some(id) = MemoryId::from_row(key) {
            self.st.memories += 1;
            let mut has_text = false;
            let mut has_emb = false;
            for e in row.iter() {
                match e.family {
                    CONTENT if e.qualifier == TEXT => {
                        has_text = true;
                        let text = std::str::from_utf8(e.cell.value())
                            .map_err(|_| corrupt("a memory's text"))?;
                        self.texts.insert(key.to_vec(), normalize(text));
                    }
                    EMB => has_emb = true,
                    META if e.qualifier == SUPERSEDED_BY => {
                        self.superseded_by.push(e.cell.value().to_vec());
                    }
                    LINKS => {
                        self.links.insert((
                            key.to_vec(),
                            e.qualifier.to_vec(),
                            e.cell.value().to_vec(),
                        ));
                    }
                    BACKLINKS => {
                        self.backlinks.insert((
                            e.qualifier.to_vec(),
                            key.to_vec(),
                            e.cell.value().to_vec(),
                        ));
                    }
                    _ => {}
                }
            }
            if !has_text {
                self.st.no_content += 1;
            }
            if has_model && !has_emb {
                self.st.missing_embeddings += 1;
            }
            if has_emb {
                if ivf_ids {
                    self.embedded_rows.insert(key.to_vec());
                }
                *self
                    .embedded
                    .entry(id.namespace().as_bytes().to_vec())
                    .or_default() += 1;
            }
            return Ok(());
        }
        if ivf && let Some(ns) = key.strip_suffix(COUNT_TAG) {
            let n = row
                .get(STATS, EMBEDDED)
                .and_then(|c| c.as_i64())
                .unwrap_or(0);
            self.counters.insert(ns.to_vec(), n);
        } else if let Some(c) = row.get(META, RID) {
            self.dedups.push((key.to_vec(), c.value().to_vec()));
        } else {
            return Err(corrupt("a row that is neither a memory nor a dedup row"));
        }
        Ok(())
    }

    /// The checks across rows: dedup rows, links and supersessions.
    fn check_rows(&mut self) {
        let texts = &self.texts;
        for (key, target) in &self.dedups {
            let ok = texts.get(target).is_some_and(|norm| {
                key.windows(2)
                    .position(|w| w == DEDUP_TAG)
                    .is_some_and(|at| {
                        std::str::from_utf8(&key[..at])
                            .is_ok_and(|ns| dedup_row(ns, &content_hash(norm)) == *key)
                    })
            });
            if !ok {
                self.st.stale_dedup += 1;
            }
        }
        // Each link must have its backlink, and the reverse; both ends live.
        self.st.broken_links += self.links.symmetric_difference(&self.backlinks).count() as u64;
        for (from, to, _) in &self.links {
            if !texts.contains_key(from) || !texts.contains_key(to) {
                self.st.broken_links += 1;
            }
        }
        self.st.dangling_superseded = self
            .superseded_by
            .iter()
            .filter(|s| !texts.contains_key(*s))
            .count() as u64;
    }

    /// The IVF checks: counters against the embedded memories, and the index's ids.
    fn check_ivf(
        &mut self,
        s: &pigeonhole::Snapshot,
        ivf_index: Option<&pigeonhole_vector::Index>,
    ) -> Result<()> {
        let (embedded, counters) = (&self.embedded, &self.counters);
        let all: BTreeSet<&Vec<u8>> = embedded.keys().chain(counters.keys()).collect();
        self.st.ivf_counter_drift = all
            .into_iter()
            .filter(|ns| {
                embedded.get(*ns).copied().unwrap_or(0) != counters.get(*ns).copied().unwrap_or(0)
            })
            .count() as u64;
        let Some(index) = ivf_index else {
            return Ok(());
        };
        // Both sides in ascending byte order: a merge walk counts each side's ids the
        // other lacks, streaming the index's ids against the embedded memories' rows.
        let mut rows = self.embedded_rows.iter().peekable();
        let (mut missing, mut stale) = (0u64, 0u64);
        index.ids_at(s, |id| {
            while rows.next_if(|r| r.as_slice() < id).is_some() {
                missing += 1;
            }
            if rows.next_if(|r| r.as_slice() == id).is_none() {
                stale += 1;
            }
        })?;
        self.st.ivf_missing = missing + rows.count() as u64;
        self.st.ivf_stale = stale;
        self.st.ivf_mismatched = self.st.ivf_missing + self.st.ivf_stale;
        Ok(())
    }
}

/// Marks the memory at `old` superseded by the one at `new` since `valid_to`, with the
/// `new → old` link: the writes [`Store::supersede`] makes once its reads pass.
pub(crate) fn supersede_in(
    txn: &mut Transaction,
    t: &Table,
    old: &[u8],
    new: &[u8],
    valid_to: u64,
) {
    txn.put(t, old, META, SUPERSEDED_BY, new)
        .put_u64(t, old, META, VALID_TO, valid_to);
    write_link(txn, t, new, old, LinkType::SUPERSEDES, 1.0);
}

pub(crate) fn write_link(
    txn: &mut Transaction,
    t: &Table,
    from: &[u8],
    to: &[u8],
    ty: LinkType,
    w: f32,
) {
    let mut v = [0u8; 5];
    v[0] = ty.0;
    v[1..].copy_from_slice(&w.to_le_bytes());
    txn.put(t, from, LINKS, to, &v)
        .put(t, to, BACKLINKS, from, &v);
}

fn read_link(q: &[u8], v: &[u8]) -> Result<Link> {
    let id = MemoryId::from_row(q).ok_or_else(|| corrupt("a link's key"))?;
    if v.len() != 5 {
        return Err(corrupt("a link's value"));
    }
    let w = f32::from_le_bytes(v[1..].try_into().expect("4"));
    Ok(Link {
        id,
        ty: LinkType(v[0]),
        weight: w,
    })
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
pub(crate) fn read_memory(id: MemoryId, row: &pigeonhole::Row) -> Result<Option<StoredMemory>> {
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
    let consolidated = view.get(META, CONSOLIDATED).is_some();
    let summary = view.get(META, SUMMARY).is_some();
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
            SOURCES => sources.push(Source {
                name: String::from_utf8_lossy(e.qualifier).into_owned(),
                at_us: e.cell.as_u64().unwrap_or(0),
            }),
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
        consolidated,
        summary,
        superseded_by,
        sources,
        links,
        backlinks,
        embedding,
    }))
}

/// A link's weight: from 0 to 1 (NaN refused).
fn check_weight(w: f32) -> Result<()> {
    if (0.0..=1.0).contains(&w) {
        Ok(())
    } else {
        Err(Error::InvalidArgument(format!(
            "a link's weight is from 0 to 1, not {w}"
        )))
    }
}

/// A memory [`Store::prepare_remember`] checked and placed: its near-dedup lookup done and
/// its id and time fixed, for [`Store::remember_prepared_in`] (ICR 0123).
#[derive(Debug)]
pub struct PreparedMemory {
    ns: String,
    memory: Memory,
    rid: [u8; 16],
    now: u64,
    near: Option<Near>,
}
