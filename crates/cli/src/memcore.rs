//! The memory store's operations, one implementation for every front-end (D313 §7, ticket 8):
//! `phdb serve`'s `Memory` service (8a) and, with 8b, `phdb mcp`'s memory tools. Synchronous
//! and free of Tokio, so `phdb mcp` without `serve` keeps none; `serve` runs these on its
//! blocking pool under the memory sub-limit (ai1249's M6).
//!
//! **The namespace limit lives here** (ai1249's M4, the owner's Q6): a token may carry
//! `ns=…`, and every operation checks it against the namespaces the call names, the
//! namespace each memory id carries ([`MemoryId::namespace`]), and, for a watch, the
//! namespaces it would see. A call outside the limit is refused whole, never trimmed.

use std::sync::Arc;

use pigeonhole::{ErrorCode, Pigeonhole, RetryPolicy};
use pigeonhole_memory::{
    EmbeddingModel, Kind, LinkType, Marked, Memory as NewMemory, MemoryId, OpenOptions, RecallHit,
    RecallStats, Remembered, Store, StoredMemory,
};

use crate::exit::Failure;
use crate::mcp::{EmbedFault, Endpoint};

/// A memory operation's failure, by what the front-ends answer.
#[derive(Debug)]
pub(crate) struct MemError {
    pub kind: MemErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemErrorKind {
    /// The request itself: bad arguments, a model mismatch.
    Invalid,
    /// Outside the caller's namespaces.
    Denied,
    /// The store or an engine call failed with this code.
    Engine(ErrorCode),
    /// The embedding endpoint couldn't be reached, or answered that it's failing or busy:
    /// nothing was applied, and the call may be retried (ai1249's R2 on #1617).
    Unavailable,
    /// The embedding endpoint took longer than its timeout: nothing was applied.
    Deadline,
    /// The embedding endpoint's answer wasn't an embedding the store can take: the
    /// server's configuration (its URL, key or model), not the request.
    Internal,
}

impl MemError {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self {
            kind: MemErrorKind::Invalid,
            message: message.into(),
        }
    }

    fn of_kind(kind: MemErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn denied(message: impl Into<String>) -> Self {
        Self {
            kind: MemErrorKind::Denied,
            message: message.into(),
        }
    }

    pub(crate) fn store_error(context: &str, e: &pigeonhole_memory::Error) -> Self {
        Self {
            kind: MemErrorKind::Engine(e.code()),
            message: format!("{context}: {e}"),
        }
    }
}

type Result<T> = std::result::Result<T, MemError>;

/// The namespaces a caller may touch: `None` for all of them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Scope<'a>(pub Option<&'a [String]>);

impl Scope<'_> {
    /// `ns` is one of the caller's.
    pub(crate) fn check(&self, ns: &str) -> Result<()> {
        match self.0 {
            Some(allowed) if !allowed.iter().any(|a| a == ns) => Err(MemError::denied(format!(
                "namespace {ns:?} is outside this token's namespaces"
            ))),
            _ => Ok(()),
        }
    }

    /// Each of `namespaces` is the caller's; with none named ("all"), the caller must have
    /// no limit. One outside the limit refuses the call, rather than being dropped from it
    /// (ai1249's M4 on ticket 8).
    pub(crate) fn check_all(&self, namespaces: &[String]) -> Result<()> {
        if namespaces.is_empty() && self.0.is_some() {
            return Err(MemError::denied(
                "this token is limited to some namespaces: name them",
            ));
        }
        namespaces.iter().try_for_each(|ns| self.check(ns))
    }

    /// The memory `id`'s namespace is the caller's.
    fn check_id(&self, id: &MemoryId) -> Result<()> {
        self.check(id.namespace())
    }
}

/// Which side of a search a text's embedding is for. A remote endpoint embeds both the same
/// way; a local model (#1345) may not, so the call says which.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EmbedKind {
    /// A memory being remembered.
    Passage,
    /// A recall's query.
    Query,
}

/// What computes embeddings from text: today a remote endpoint (`--embed-url`). #1345's
/// local model becomes another variant without touching the calls (ai1249).
pub(crate) enum Embedder {
    Remote(Endpoint),
}

impl Embedder {
    /// The model it computes with.
    pub(crate) fn model(&self) -> &str {
        match self {
            Self::Remote(e) => e.model(),
        }
    }

    /// `text`'s embedding. The bound on the call is the endpoint's own timeout, so a slow
    /// endpoint never leaves a thread stuck past it (ai1249 on ticket 8).
    pub(crate) fn embed(&self, text: &str, kind: EmbedKind) -> Result<Vec<f32>> {
        let _ = kind;
        match self {
            Self::Remote(e) => e.embed_classified(text).map_err(|(fault, f)| {
                let kind = match fault {
                    EmbedFault::Timeout => MemErrorKind::Deadline,
                    EmbedFault::Unreachable => MemErrorKind::Unavailable,
                    EmbedFault::Rejected => MemErrorKind::Invalid,
                    EmbedFault::BadReply => MemErrorKind::Internal,
                };
                MemError::of_kind(kind, f.message())
            }),
        }
    }
}

/// A memory to remember, decoded.
pub(crate) struct Remember {
    pub namespace: String,
    pub text: String,
    pub embedding: Option<Vec<f32>>,
    pub kind: Option<Kind>,
    pub importance: Option<f64>,
    pub source: Option<String>,
    pub session: Option<String>,
    pub pinned: bool,
    pub links: Vec<(MemoryId, LinkType, f32)>,
}

/// A recall, decoded.
pub(crate) struct RecallArgs {
    pub namespaces: Vec<String>,
    pub text: Option<String>,
    pub embedding: Option<Vec<f32>>,
    pub k: usize,
    /// 0: the server's default budget.
    pub byte_budget: u64,
    pub expand_hops: u32,
    pub exact: bool,
    pub include_superseded: bool,
    pub session: Option<String>,
    pub with_embeddings: bool,
    pub with_signals: bool,
}

/// The most hits a recall returns.
pub(crate) const MAX_K: usize = 1000;

/// How a writer creates a missing store: its options, from the embedder (whose probe gives
/// the dimension).
pub(crate) type Create = dyn Fn(Option<&Embedder>) -> std::result::Result<pigeonhole_memory::StoreOptions, Failure>
    + Send
    + Sync;

/// Why [`MemCore::open_in`] failed.
#[derive(Debug)]
pub(crate) enum OpenError {
    /// The store is pinned to another model than the server embeds with (or names).
    Pin(Failure),
    /// Making a missing store's options failed (the embedder's dimension probe, say).
    Create(Failure),
    /// The store's open or create failed.
    Store(pigeonhole_memory::Error),
}

/// One memory store, as a server or tool set opens it.
pub(crate) struct MemCore {
    store: Store,
    db: Pigeonhole,
    embedder: Option<Arc<Embedder>>,
    /// A recall's budget when it asks for none, and the largest it may ask for: a recall's
    /// reply is bounded (the coordinator on #1567).
    default_budget: u64,
    max_budget: u64,
}

impl MemCore {
    /// Opens the store `name`, creating it with `create` if it doesn't exist. An existing
    /// store must be pinned to `expected` (the embedder's model, or the one the caller
    /// names), or have no model when none is expected. Without `create` (a replica, #1618),
    /// a missing store is `Ok(None)`, and the store starts no thread of
    /// the crate's: its forgetter and IVF rebalancer are taken and dropped, since nothing may
    /// write in the background there.
    pub(crate) fn open_in(
        db: &Pigeonhole,
        name: &str,
        embedder: Option<Arc<Embedder>>,
        expected: Option<&str>,
        create: Option<&Create>,
        budget: (u64, u64),
    ) -> std::result::Result<Option<Self>, OpenError> {
        let exists = db.tables().iter().any(|t| *t == format!("mem_{name}"));
        let store = if exists {
            // A probe reads the pinned model; it owns its forgetter and rebalancer, so it
            // starts no thread.
            let probe = Store::open_with(
                db,
                name,
                OpenOptions::new()
                    .without_embeddings()
                    .own_forgetter()
                    .own_rebalancer(),
            )
            .map_err(OpenError::Store)?;
            let pinned = probe.embedding_model().cloned();
            drop(probe.take_forgetter());
            drop(probe.take_rebalancer());
            drop(probe);
            let expected = embedder.as_deref().map(Embedder::model).or(expected);
            check_pin(name, pinned.as_ref(), expected).map_err(OpenError::Pin)?;
            let options = match &pinned {
                Some(m) => OpenOptions::new().model(m.id()),
                None => OpenOptions::new().without_embeddings(),
            };
            if create.is_some() {
                Store::open_with(db, name, options)
            } else {
                Store::open_with(db, name, options.own_forgetter().own_rebalancer()).inspect(|s| {
                    // Dropped: a forgetter kept in the store would hold the store.
                    drop(s.take_forgetter());
                    drop(s.take_rebalancer());
                })
            }
            .map_err(OpenError::Store)?
        } else {
            let Some(create) = create else {
                return Ok(None);
            };
            let options = create(embedder.as_deref()).map_err(OpenError::Create)?;
            Store::create(db, name, options).map_err(OpenError::Store)?
        };
        Ok(Some(Self {
            store,
            db: db.clone(),
            embedder,
            default_budget: budget.0,
            max_budget: budget.1.max(budget.0),
        }))
    }

    /// The store's pinned model.
    pub(crate) fn model(&self) -> Option<&EmbeddingModel> {
        self.store.embedding_model()
    }

    /// The store, for a watch (serve's stream reads its feed itself).
    pub(crate) fn store(&self) -> &Store {
        &self.store
    }

    /// The embedding a call uses: the embedder's for `text`, or the caller's (checked
    /// against the pinned model), or none.
    fn embedding(
        &self,
        given: Option<Vec<f32>>,
        text: Option<&str>,
        kind: EmbedKind,
    ) -> Result<Option<Vec<f32>>> {
        if let Some(e) = &self.embedder {
            if given.is_some() {
                return Err(MemError::invalid(
                    "this server computes the embeddings: send text only",
                ));
            }
            let Some(text) = text else { return Ok(None) };
            let v = e.embed(text, kind)?;
            if let Some(m) = self.model()
                && v.len() != m.dim() as usize
            {
                return Err(MemError::of_kind(
                    MemErrorKind::Internal,
                    format!(
                        "the embedding endpoint returned {} dimensions; the store is pinned to {} ({})",
                        v.len(),
                        m.dim(),
                        m.id()
                    ),
                ));
            }
            return Ok(Some(v));
        }
        let Some(v) = given else { return Ok(None) };
        let Some(m) = self.model() else {
            return Err(MemError::invalid(
                "this store has no embedding model: send text only",
            ));
        };
        if v.len() != m.dim() as usize {
            return Err(MemError::invalid(format!(
                "embedding: {} numbers; the store's model ({}) has {} dimensions",
                v.len(),
                m.id(),
                m.dim()
            )));
        }
        Ok(Some(v))
    }

    /// Remembers a memory in its namespace, and with `supersedes`, marks the old one
    /// superseded (in the same namespace only). The supersede is a second transaction; its
    /// failure is returned beside the stored id.
    pub(crate) fn remember(
        &self,
        scope: Scope<'_>,
        r: Remember,
        supersedes: Option<MemoryId>,
    ) -> Result<(Remembered, Option<MemError>)> {
        scope.check(&r.namespace)?;
        if let Some(old) = &supersedes
            && old.namespace() != r.namespace
        {
            return Err(MemError::invalid(format!(
                "supersedes: {old} is in namespace {:?}, not {:?}",
                old.namespace(),
                r.namespace
            )));
        }
        for (to, _, _) in &r.links {
            scope.check_id(to)?;
        }
        let mut m = NewMemory::new(r.text.as_str());
        match self.embedding(r.embedding, Some(&r.text), EmbedKind::Passage)? {
            Some(v) => m = m.embedding(v),
            None => {
                if let Some(model) = self.model() {
                    return Err(MemError::invalid(format!(
                        "this store is pinned to the embedding model {:?}: a memory needs its \
                         embedding",
                        model.id()
                    )));
                }
            }
        }
        if let Some(k) = r.kind {
            m = m.kind(k);
        }
        if let Some(i) = r.importance {
            if !(0.0..=1.0).contains(&i) {
                return Err(MemError::invalid("importance: a number from 0 to 1"));
            }
            m = m.importance(i);
        }
        if let Some(s) = r.source {
            m = m.source(s);
        }
        if let Some(s) = r.session {
            m = m.session(s);
        }
        m = m.pinned(r.pinned);
        for (to, ty, w) in r.links {
            m = m.link(to, ty, w);
        }
        let ns = r.namespace.as_str();
        // With `supersedes`, near dedup mustn't stop the write: `remember_in` skips it.
        let done = match &supersedes {
            Some(_) => self
                .db
                .transact(&RetryPolicy::default(), |txn| {
                    self.store.remember_in(txn, ns, m.clone())
                })
                .map(|(r, _)| r),
            None => self.store.remember(ns, m),
        }
        .map_err(|e| MemError::store_error("remember", &e))?;
        let mut supersede_error = None;
        if let Some(old) = supersedes
            && old != *done.id()
            && let Err(e) = self.store.supersede(&old, done.id())
        {
            supersede_error = Some(MemError::store_error("supersede", &e));
        }
        Ok((done, supersede_error))
    }

    /// The memories nearest the query, by the store's ranking, within the byte budget.
    pub(crate) fn recall(
        &self,
        scope: Scope<'_>,
        a: RecallArgs,
    ) -> Result<(Vec<RecallHit>, RecallStats)> {
        if a.namespaces.is_empty() {
            return Err(MemError::invalid("a recall names its namespaces"));
        }
        scope.check_all(&a.namespaces)?;
        if a.k == 0 || a.k > MAX_K {
            return Err(MemError::invalid(format!("k is 1 to {MAX_K}")));
        }
        let budget = match a.byte_budget {
            0 => self.default_budget,
            b if b > self.max_budget => {
                return Err(MemError::invalid(format!(
                    "byte_budget is at most {} on this server",
                    self.max_budget
                )));
            }
            b => b,
        };
        let embedding = if self.model().is_some() || self.embedder.is_some() {
            self.embedding(a.embedding, a.text.as_deref(), EmbedKind::Query)?
        } else {
            self.embedding(a.embedding, None, EmbedKind::Query)?
        };
        if a.text.is_none() && embedding.is_none() {
            return Err(MemError::invalid("a recall needs text or an embedding"));
        }
        let ns: Vec<&str> = a.namespaces.iter().map(String::as_str).collect();
        let mut r = self
            .store
            .recall(&ns)
            .k(a.k)
            .byte_budget(budget)
            .include_superseded(a.include_superseded)
            .exact(a.exact)
            .with_embeddings(a.with_embeddings)
            .with_signals(a.with_signals);
        if a.expand_hops > 0 {
            r = r.expand(pigeonhole_memory::Expand::hops(a.expand_hops));
        }
        if let Some(t) = a.text {
            r = r.text(t);
        }
        if let Some(e) = embedding {
            r = r.embedding(e);
        }
        if let Some(s) = a.session {
            r = r.session(s);
        }
        r.run_with_stats()
            .map_err(|e| MemError::store_error("recall", &e))
    }

    /// Marks memories used: their ids' namespaces must be the caller's.
    pub(crate) fn mark_used(&self, scope: Scope<'_>, ids: &[MemoryId]) -> Result<Marked> {
        ids.iter().try_for_each(|id| scope.check_id(id))?;
        self.store
            .mark_used(ids)
            .map_err(|e| MemError::store_error("mark_used", &e))
    }

    /// `new` supersedes `old`.
    pub(crate) fn supersede(&self, scope: Scope<'_>, old: &MemoryId, new: &MemoryId) -> Result<()> {
        scope.check_id(old)?;
        scope.check_id(new)?;
        self.store
            .supersede(old, new)
            .map_err(|e| MemError::store_error("supersede", &e))
    }

    /// Links `from` to `to`.
    pub(crate) fn link(
        &self,
        scope: Scope<'_>,
        from: &MemoryId,
        to: &MemoryId,
        ty: LinkType,
        weight: f32,
    ) -> Result<()> {
        scope.check_id(from)?;
        scope.check_id(to)?;
        if ty == LinkType::SUPERSEDES {
            return Err(MemError::invalid(
                "a supersedes link: use Supersede, which keeps recall in step",
            ));
        }
        self.store
            .link(from, to, ty, weight)
            .map_err(|e| MemError::store_error("link", &e))
    }

    /// Deletes memories now: all their namespaces are checked before any is deleted. Stops at
    /// the first failure, the deletes before it standing; returns how many were deleted and
    /// the ids that didn't exist.
    pub(crate) fn forget(
        &self,
        scope: Scope<'_>,
        ids: &[MemoryId],
    ) -> Result<(u64, Vec<MemoryId>)> {
        ids.iter().try_for_each(|id| scope.check_id(id))?;
        let mut deleted = 0;
        let mut missing = Vec::new();
        for id in ids {
            match self.store.delete(id) {
                Ok(true) => deleted += 1,
                Ok(false) => missing.push(id.clone()),
                Err(e) => {
                    return Err(MemError::store_error(
                        &format!("forget (after {deleted} deleted)"),
                        &e,
                    ));
                }
            }
        }
        Ok((deleted, missing))
    }

    /// A memory, for a watch's event.
    pub(crate) fn get(&self, id: &MemoryId) -> Result<Option<StoredMemory>> {
        self.store
            .get(id)
            .map_err(|e| MemError::store_error("get", &e))
    }
}

/// The store's model against the embedder's: an endpoint of another model would ruin recall.
fn check_pin(
    name: &str,
    pinned: Option<&EmbeddingModel>,
    expected: Option<&str>,
) -> std::result::Result<(), Failure> {
    match (pinned, expected) {
        (Some(m), Some(id)) if m.id() != id => Err(Failure::usage(&format!(
            "memory store {name} is pinned to the embedding model {:?}, not {id:?}: embeddings \
             of another model would ruin its recall",
            m.id()
        ))),
        (None, Some(id)) => Err(Failure::usage(&format!(
            "memory store {name} has no embedding model, so it can't take {id:?}'s embeddings"
        ))),
        _ => Ok(()),
    }
}
