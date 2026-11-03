//! The agent memory tools (D311 T7): `phdb mcp --memory NAME` serves a `pigeonhole-memory`
//! store through six tools, plus the `memory://guide` resource. The store's tables are
//! `mem_NAME` and `mem_NAME_text` (ICR 0107, T1).
//!
//! | Tool | What it does |
//! |---|---|
//! | `memory_remember` | Stores a memory (exact duplicates merge), optionally superseding one. |
//! | `memory_recall` | The memories best matching a text and/or an embedding, within a byte budget. |
//! | `memory_mark_used` | Counts a use of memories that helped, which keeps them recalled. |
//! | `memory_supersede` | Marks a memory replaced by a newer one, so recall leaves it out. |
//! | `memory_link` | A weighted, typed link between two memories of a namespace. |
//! | `memory_forget` | Deletes memories. |
//!
//! **Embeddings.** The store is pinned to one embedding model when it's created (its id and
//! dimension), and refuses vectors that don't fit. Mixing models silently ruins vector
//! recall, so a client-supplied `embedding` must also name its `embedding_model`, which must
//! be the store's. With `--embed-url`, the server embeds the text itself and refuses a
//! client's vectors. A store without a model recalls by keyword and recency only.

use pigeonhole::{Element, Pigeonhole, RetryPolicy};
use pigeonhole_memory::{
    EmbeddingModel, Kind, LinkType, Memory as NewMemory, MemoryId, OpenOptions, ReadStore,
    Remembered, Store, StoreOptions,
};
use serde_json::{Map, Value, json};

use super::embed::Endpoint;
use super::tools::{Args, Outcome, tool};
use crate::args::MemoryArgs;
use crate::exit::Failure;
use crate::open::Db;

/// The most hits one recall may ask for, and its default.
const MAX_K: u64 = 100;
const DEFAULT_K: u64 = 10;
/// The smallest recall budget a call may ask for, in bytes.
const MIN_RECALL_BYTES: u64 = 256;
/// The most ids one call takes.
const MAX_IDS: usize = 1_000;
/// The namespace a call without one uses.
const DEFAULT_NAMESPACE: &str = "default";

/// The guide served as `memory://guide`, and the MCP guide's memory section.
pub(super) const GUIDE: &str = include_str!("memory_guide.md");

/// The store, in the database's role.
enum Handle {
    Writer(Store),
    Reader(ReadStore),
}

/// The memory tools over one store.
pub(super) struct Memory {
    name: String,
    store: Handle,
    /// Whether writes can succeed.
    writable: bool,
    /// The database, as the writer: `memory_remember` with `supersedes` remembers in a
    /// transaction of its own.
    db: Option<Pigeonhole>,
    /// `--embed-url`: the server embeds text itself.
    endpoint: Option<Endpoint>,
    /// The default recall budget (`--max-result-bytes`).
    result_bytes: usize,
}

impl Memory {
    /// Opens (or, writable, creates) the store `--memory` names, or `None` without the flag.
    /// Notes on the store go to `stderr`.
    pub(super) fn open(
        db: &Db,
        args: &MemoryArgs,
        writable: bool,
        result_bytes: usize,
        stderr: &mut dyn std::io::Write,
    ) -> Result<Option<Self>, Failure> {
        let Some(name) = args.memory.clone() else {
            return Ok(None);
        };
        let endpoint = match &args.embed_url {
            Some(url) => Some(Endpoint::new(
                url,
                args.embed_model.as_deref().unwrap_or_default(),
                args.embed_key_env.as_deref(),
            )?),
            None => None,
        };
        // The model the caller expects: the endpoint's, or the client's `--memory-model`.
        let expected = endpoint
            .as_ref()
            .map(|e| e.model().to_owned())
            .or_else(|| args.memory_model.clone());
        let fail = |e: pigeonhole_memory::Error| failure(&format!("memory store {name}"), e);
        let exists = db.tables().iter().any(|t| *t == format!("mem_{name}"));
        let store = match db {
            Db::Writer(p) if exists => {
                // A probe reads the pinned model; it owns its forgetter, so it starts no
                // thread (ai1249's M2 on #1389). Then the real handle, as the expected caller.
                let probe = Store::open_with(
                    p,
                    &name,
                    OpenOptions::new().without_embeddings().own_forgetter(),
                )
                .map_err(fail)?;
                let pinned = probe.embedding_model().cloned();
                drop(probe);
                check_pin(&name, pinned.as_ref(), expected.as_deref(), args)?;
                Handle::Writer(
                    match pinned {
                        Some(m) => Store::open(p, &name, m.id()),
                        None => Store::open_without_embeddings(p, &name),
                    }
                    .map_err(fail)?,
                )
            }
            Db::Writer(p) if writable => {
                let mut options = StoreOptions::new();
                let model = match (&endpoint, &args.memory_model, args.memory_dim) {
                    (Some(e), _, _) => {
                        // The dimension is the endpoint's: one probe embedding tells it.
                        let dim = e.embed("dimension probe")?.len();
                        Some((e.model().to_owned(), dim as u32))
                    }
                    (None, Some(id), Some(dim)) => Some((id.clone(), dim)),
                    _ => None,
                };
                if let Some((id, dim)) = &model {
                    let element = match args.memory_element.as_str() {
                        "f16" => Element::F16,
                        "bf16" => Element::BF16,
                        _ => Element::F32,
                    };
                    options = options
                        .embedding(EmbeddingModel::new(id.clone(), *dim, element).map_err(fail)?);
                }
                if let Some(hours) = args.memory_episodic_ttl {
                    options = options
                        .episodic_ttl(std::time::Duration::from_secs(hours.saturating_mul(3600)));
                }
                if let Some(n) = &args.memory_normalization {
                    options = options.normalization(normalization(n));
                }
                let store = Store::create(p, &name, options).map_err(fail)?;
                let _ = writeln!(
                    stderr,
                    "phdb mcp: created memory store {name} ({})",
                    match &model {
                        Some((id, dim)) => format!("embedding model {id}, {dim} dimensions"),
                        None => "no embedding model: recall is by keyword and recency, and \
                                 the store can never take embeddings"
                            .into(),
                    }
                );
                Handle::Writer(store)
            }
            Db::Writer(_) => {
                return Err(Failure::usage(&format!(
                    "--memory {name}: the store doesn't exist, and the database is read-only"
                )));
            }
            Db::Reader(r) => {
                if !exists {
                    return Err(Failure::usage(&format!(
                        "--memory {name}: the store doesn't exist (a reader can't create it)"
                    )));
                }
                let probe = Store::open_reader_without_embeddings(r, &name).map_err(fail)?;
                let pinned = probe.embedding_model().cloned();
                check_pin(&name, pinned.as_ref(), expected.as_deref(), args)?;
                match pinned {
                    Some(m) => {
                        drop(probe);
                        Handle::Reader(Store::open_reader(r, &name, m.id()).map_err(fail)?)
                    }
                    None => Handle::Reader(probe),
                }
            }
        };
        // The normalisation is fixed at create: a flag naming another one for an existing
        // store fails at startup, as a model that doesn't match does (#1405).
        if let Some(want) = &args.memory_normalization {
            let have = match &store {
                Handle::Writer(s) => s.normalization(),
                Handle::Reader(s) => s.normalization(),
            };
            if have != normalization(want) {
                return Err(Failure::usage(&format!(
                    "--memory-normalization {want}: memory store {name}'s text index is {}, \
                     fixed when it was created",
                    normalization_name(have)
                )));
            }
        }
        if exists && (args.memory_episodic_ttl.is_some() || args.memory_dim.is_some()) {
            let _ = writeln!(
                stderr,
                "phdb mcp: memory store {name} exists: --memory-episodic-ttl and --memory-dim \
                 apply only to a new store"
            );
        }
        let writable = writable && matches!(store, Handle::Writer(_));
        let db = match db {
            Db::Writer(p) => Some(p.clone()),
            Db::Reader(_) => None,
        };
        Ok(Some(Self {
            name,
            store,
            writable,
            db,
            endpoint,
            result_bytes,
        }))
    }

    /// The store's pinned model.
    fn model(&self) -> Option<&EmbeddingModel> {
        match &self.store {
            Handle::Writer(s) => s.embedding_model(),
            Handle::Reader(s) => s.embedding_model(),
        }
    }

    fn writer(&self) -> Result<&Store, Failure> {
        match &self.store {
            Handle::Writer(s) if self.writable => Ok(s),
            _ => Err(Failure::read_only("reader")),
        }
    }

    /// The memory paragraph of the server's instructions.
    pub(super) fn instructions(&self) -> String {
        let embeddings = match (&self.endpoint, self.model()) {
            (Some(_), _) => {
                "The server computes embeddings: send text only, never an embedding.".to_owned()
            }
            (None, Some(m)) => format!(
                "The store is pinned to the embedding model {:?} ({} dimensions): send an \
                 embedding only if it comes from exactly that model, with embedding_model set \
                 to its id; otherwise send text only and recall falls back to keywords.",
                m.id(),
                m.dim()
            ),
            (None, None) => {
                "The store has no embedding model: send text only; recall is by keyword and \
                 recency."
                    .to_owned()
            }
        };
        let writes = if self.writable {
            " Remember durable facts, decisions and preferences, not chatter or what the \
             conversation already holds; one fact per memory. Use a namespace per user or \
             project, and importance near 1 only for what must not be lost. When a fact \
             changes, remember the new one with supersedes set to the old one's id rather than \
             storing a contradiction. After using recalled memories, call memory_mark_used on \
             the ones that helped."
        } else {
            " The store is read-only here: memory_recall only."
        };
        format!(
            "Long-term memory (store {:?}): recall before acting on anything the user may have \
             told you before, with max_bytes sized to the context you can spare.{writes} \
             {embeddings} Read memory://guide for the details.",
            self.name
        )
    }

    /// The memory tools, as `tools/list` gives them.
    pub(super) fn list(&self) -> Vec<Value> {
        let mut tools = vec![self.recall_tool()];
        if self.writable {
            tools.extend([
                self.remember_tool(),
                mark_used_tool(),
                supersede_tool(),
                link_tool(),
                forget_tool(),
            ]);
        }
        tools
    }

    /// A memory tool's call, or `None` for a name that isn't one.
    pub(super) fn call(&self, name: &str, args: &Map<String, Value>) -> Option<Outcome> {
        let a = Args(args);
        Some(match name {
            "memory_remember" => self.remember(a),
            "memory_recall" => self.recall(a),
            "memory_mark_used" => self.mark_used(a),
            "memory_supersede" => self.supersede(a),
            "memory_link" => self.link(a),
            "memory_forget" => self.forget(a),
            _ => return None,
        })
    }

    /// Drops the store (and its forgetter) before the database closes.
    pub(super) fn close(self) {
        drop(self.store);
        drop(self.db);
    }

    /// The embedding a call uses: the endpoint's for `text`, or the client's (checked
    /// against the pinned model), or none.
    fn embedding(&self, a: &Args<'_>, text: Option<&str>) -> Result<Option<Vec<f32>>, Failure> {
        let given = a.get("embedding");
        let named = a.opt_str("embedding_model")?;
        if let Some(e) = &self.endpoint {
            if given.is_some() || named.is_some() {
                return Err(Failure::usage(
                    "this server computes the embeddings (--embed-url): send text only",
                ));
            }
            let Some(text) = text else {
                return Ok(None);
            };
            let v = e.embed(text)?;
            if let Some(m) = self.model()
                && v.len() != m.dim() as usize
            {
                return Err(Failure::input(format!(
                    "the embedding endpoint returned {} dimensions; the store is pinned to {} \
                     ({})",
                    v.len(),
                    m.dim(),
                    m.id()
                )));
            }
            return Ok(Some(v));
        }
        let Some(given) = given else {
            if named.is_some() {
                return Err(Failure::usage("embedding_model without an embedding"));
            }
            return Ok(None);
        };
        let Some(model) = self.model() else {
            return Err(Failure::usage(
                "this store has no embedding model: send text only",
            ));
        };
        match named {
            Some(id) if id == model.id() => {}
            Some(id) => {
                return Err(Failure::usage(&format!(
                    "embedding_model {id:?}: this store is pinned to {:?}; embeddings of \
                     another model would ruin its recall, so send text only",
                    model.id()
                )));
            }
            None => {
                return Err(Failure::usage(&format!(
                    "an embedding needs embedding_model, the id of the model that made it \
                     (this store's is {:?})",
                    model.id()
                )));
            }
        }
        let v = vector(given)?;
        if v.len() != model.dim() as usize {
            return Err(Failure::usage(&format!(
                "embedding: {} numbers; the store's model has {} dimensions",
                v.len(),
                model.dim()
            )));
        }
        Ok(Some(v))
    }

    fn remember(&self, a: Args<'_>) -> Outcome {
        a.only(&[
            "namespace",
            "text",
            "embedding",
            "embedding_model",
            "kind",
            "importance",
            "source",
            "session",
            "pinned",
            "links",
            "supersedes",
        ])?;
        let store = self.writer()?;
        let ns = namespace(&a)?;
        let text = a.str("text")?;
        let supersedes = a.opt_str("supersedes")?.map(memory_id).transpose()?;
        // Before the write: a memory supersedes one of its own namespace only (ai1249's M1).
        if let Some(old) = &supersedes
            && old.namespace() != ns
        {
            return Err(Failure::usage(&format!(
                "supersedes: {old} is in namespace {:?}, not {ns:?}",
                old.namespace()
            )));
        }
        let mut m = NewMemory::new(text);
        match self.embedding(&a, Some(text))? {
            Some(v) => m = m.embedding(v),
            None => {
                if let Some(model) = self.model() {
                    return Err(Failure::usage(&format!(
                        "this store is pinned to the embedding model {:?}: a memory needs its \
                         embedding (with embedding_model)",
                        model.id()
                    )));
                }
            }
        }
        if let Some(k) = a.opt_str("kind")? {
            m = m.kind(kind(k)?);
        }
        if let Some(v) = a.get("importance") {
            let i = v
                .as_f64()
                .filter(|i| (0.0..=1.0).contains(i))
                .ok_or_else(|| Failure::usage("importance: a number from 0 to 1"))?;
            m = m.importance(i);
        }
        if let Some(s) = a.opt_str("source")? {
            m = m.source(s);
        }
        if let Some(s) = a.opt_str("session")? {
            m = m.session(s);
        }
        m = m.pinned(a.bool_or("pinned", false)?);
        for l in a.list("links")? {
            let Value::Object(l) = l else {
                return Err(Failure::usage("links: a list of {to, type, weight}"));
            };
            let l = Args(l);
            l.only(&["to", "type", "weight"])
                .map_err(|f| f.within("links"))?;
            m = m.link(memory_id(l.str("to")?)?, link_type(&l)?, weight(&l)?);
        }
        // A correction is near what it corrects, so with `supersedes` near dedup mustn't stop
        // the write (simp724's S1 on #1389): `remember_in` skips it, keeping exact dedup.
        let done = match (&supersedes, &self.db) {
            (Some(_), Some(db)) => db
                .transact(&RetryPolicy::default(), |txn| {
                    store.remember_in(txn, ns, m.clone())
                })
                .map(|(r, _)| r),
            _ => store.remember(ns, m),
        }
        .map_err(|e| failure("remember", e))?;
        let (outcome, id) = match &done {
            Remembered::New(id) => ("new", id),
            Remembered::Merged(id) => ("merged", id),
            Remembered::Similar(id) => ("similar", id),
            _ => ("other", done.id()),
        };
        let mut out = Map::new();
        out.insert("id".into(), json!(id.to_string()));
        out.insert("outcome".into(), json!(outcome));
        if let Some(old) = supersedes {
            // A second transaction: a crash between the two leaves `old` unsuperseded. A
            // failure returns the stored memory's id with the error (simp724's S2), so the
            // agent can retry the supersede alone.
            let mut superseded = old != *id;
            if superseded && let Err(e) = store.supersede(&old, id) {
                superseded = false;
                out.insert("supersede_error".into(), failure("supersede", e).json());
            }
            out.insert("superseded".into(), json!(superseded));
        }
        Ok(out)
    }

    fn recall(&self, a: Args<'_>) -> Outcome {
        a.only(&[
            "namespace",
            "namespaces",
            "text",
            "embedding",
            "embedding_model",
            "k",
            "session",
            "include_superseded",
            "max_bytes",
        ])?;
        let mut namespaces = a.str_list("namespaces")?;
        if namespaces.is_empty() {
            namespaces.push(namespace(&a)?.to_owned());
        } else if a.get("namespace").is_some() {
            return Err(Failure::usage("namespace or namespaces, not both"));
        }
        let text = a.opt_str("text")?;
        // Keyword-only recall on a pinned store is fine: the vector signal just has no rank.
        let embedding = if self.model().is_some() || self.endpoint.is_some() {
            self.embedding(&a, text)?
        } else {
            self.embedding(&a, None)?
        };
        if text.is_none() && embedding.is_none() {
            return Err(Failure::usage("memory_recall needs text or an embedding"));
        }
        let k = a.u64_or("k", DEFAULT_K, 1, MAX_K)?;
        let max_bytes = a.u64_or(
            "max_bytes",
            self.result_bytes as u64,
            MIN_RECALL_BYTES,
            u64::MAX,
        )?;
        let ns: Vec<&str> = namespaces.iter().map(String::as_str).collect();
        let mut r = match &self.store {
            Handle::Writer(s) => s.recall(&ns),
            Handle::Reader(s) => s.recall(&ns),
        };
        r = r
            .k(k as usize)
            .byte_budget(max_bytes)
            .include_superseded(a.bool_or("include_superseded", false)?);
        if let Some(t) = text {
            r = r.text(t);
        }
        if let Some(e) = embedding {
            r = r.embedding(e);
        }
        if let Some(s) = a.opt_str("session")? {
            r = r.session(s);
        }
        let (hits, stats) = r.run_with_stats().map_err(|e| failure("recall", e))?;
        let hits: Vec<Value> = hits
            .into_iter()
            .map(|h| {
                let mut o = json!({
                    "id": h.id.to_string(),
                    "text": h.text,
                    "kind": kind_name(h.kind),
                    "importance": h.importance,
                    "created_us": h.created_us,
                    "score": h.score,
                });
                if let Some(by) = h.superseded_by {
                    o["superseded_by"] = json!(by.to_string());
                }
                o
            })
            .collect();
        Ok(super::tools::object(
            json!({"hits": hits, "truncated": stats.truncated}),
        ))
    }

    fn mark_used(&self, a: Args<'_>) -> Outcome {
        a.only(&["ids"])?;
        let store = self.writer()?;
        let ids = ids(&a, "ids")?;
        let m = store.mark_used(&ids).map_err(|e| failure("mark_used", e))?;
        let missing: Vec<String> = m.missing.iter().map(ToString::to_string).collect();
        Ok(super::tools::object(
            json!({"marked": m.marked, "missing": missing}),
        ))
    }

    fn supersede(&self, a: Args<'_>) -> Outcome {
        a.only(&["old", "new"])?;
        let store = self.writer()?;
        let old = memory_id(a.str("old")?)?;
        let new = memory_id(a.str("new")?)?;
        store
            .supersede(&old, &new)
            .map_err(|e| failure("supersede", e))?;
        Ok(Map::new())
    }

    fn link(&self, a: Args<'_>) -> Outcome {
        a.only(&["from", "to", "type", "weight"])?;
        let store = self.writer()?;
        let from = memory_id(a.str("from")?)?;
        let to = memory_id(a.str("to")?)?;
        store
            .link(&from, &to, link_type(&a)?, weight(&a)?)
            .map_err(|e| failure("link", e))?;
        Ok(Map::new())
    }

    fn forget(&self, a: Args<'_>) -> Outcome {
        a.only(&["ids"])?;
        let store = self.writer()?;
        let ids = ids(&a, "ids")?;
        let mut deleted = 0;
        let mut missing = Vec::new();
        for (i, id) in ids.iter().enumerate() {
            match store.delete(id) {
                Ok(true) => deleted += 1,
                Ok(false) => missing.push(id.to_string()),
                Err(e) => {
                    // The deletes before it stand: report them, the error, and what's left
                    // (simp724's S2 on #1389).
                    let left: Vec<String> = ids[i..].iter().map(ToString::to_string).collect();
                    return Ok(super::tools::object(json!({
                        "deleted": deleted,
                        "missing": missing,
                        "error": failure("forget", e).json(),
                        "not_deleted": left,
                    })));
                }
            }
        }
        Ok(super::tools::object(
            json!({"deleted": deleted, "missing": missing}),
        ))
    }

    fn remember_tool(&self) -> Value {
        let embedding = self.embedding_note();
        tool(
            "memory_remember",
            "Remember",
            &format!(
                "Stores one memory: a durable fact, decision, preference or lesson worth \
                 recalling in a later conversation, not chatter or what this conversation \
                 already holds. Keep it to one self-contained fact. An exact duplicate of a \
                 live memory merges into it (outcome \"merged\"). When a fact changes, set \
                 supersedes to the old memory's id: recall then leaves the old one out. That's \
                 a second step after the write: if it fails, the new memory is still stored, \
                 superseded is false and supersede_error says why (retry with \
                 memory_supersede). {embedding} Returns the memory's id."
            ),
            json!({
                "type": "object",
                "properties": {
                    "namespace": namespace_schema(),
                    "text": {"type": "string", "description": "The memory, as a self-contained statement."},
                    "embedding": embedding_schema(),
                    "embedding_model": embedding_model_schema(),
                    "kind": {
                        "type": "string",
                        "enum": ["episodic", "semantic", "procedural"],
                        "description": "episodic (default): something that happened; semantic: a fact or preference; procedural: how to do something.",
                    },
                    "importance": {
                        "type": "number", "minimum": 0, "maximum": 1,
                        "description": "0.5 by default; near 1 only for what must not be lost.",
                    },
                    "source": {"type": "string", "description": "Where it came from (a conversation or document id)."},
                    "session": {"type": "string", "description": "The conversation or task it belongs to."},
                    "pinned": {"type": "boolean", "description": "Never forgotten by the store's forget policy."},
                    "links": {
                        "type": "array",
                        "description": "Links to memories of the same namespace.",
                        "items": link_item_schema(),
                    },
                    "supersedes": {"type": "string", "description": "The id of a memory this one replaces."},
                },
                "required": ["text"],
                "additionalProperties": false,
            }),
            false,
        )
    }

    fn recall_tool(&self) -> Value {
        let embedding = self.embedding_note();
        tool(
            "memory_recall",
            "Recall",
            &format!(
                "The memories best matching a query, best first: call it before acting on \
                 anything the user may have told you before. Ranks by keywords, embedding \
                 similarity, recency, importance and use. Set max_bytes to the context you can \
                 spare: the hits stop there (truncated says so). Superseded memories are left \
                 out unless include_superseded. After using hits, call memory_mark_used on the \
                 ones that helped. {embedding}"
            ),
            json!({
                "type": "object",
                "properties": {
                    "namespace": namespace_schema(),
                    "namespaces": {
                        "type": "array", "items": {"type": "string"},
                        "description": "Several namespaces at once, instead of namespace.",
                    },
                    "text": {"type": "string", "description": "The query."},
                    "embedding": embedding_schema(),
                    "embedding_model": embedding_model_schema(),
                    "k": {"type": "integer", "minimum": 1, "maximum": MAX_K, "description": "The most hits (10 by default)."},
                    "session": {"type": "string", "description": "Only memories of this session."},
                    "include_superseded": {"type": "boolean", "description": "Also return superseded memories, with superseded_by."},
                    "max_bytes": {
                        "type": "integer", "minimum": MIN_RECALL_BYTES,
                        "description": format!("The hits' size budget, about, in bytes ({} by default).", self.result_bytes),
                    },
                },
                "additionalProperties": false,
            }),
            true,
        )
    }

    /// What a tool description says about embeddings.
    fn embedding_note(&self) -> String {
        match (&self.endpoint, self.model()) {
            (Some(_), _) => "The server computes the embedding: send text only.".into(),
            (None, Some(m)) => format!(
                "The store is pinned to the embedding model {:?} ({} dimensions): an embedding \
                 must come from exactly that model, with embedding_model set to {:?}; never \
                 send another model's.",
                m.id(),
                m.dim(),
                m.id()
            ),
            (None, None) => "The store has no embedding model: send text only.".into(),
        }
    }
}

/// `--memory-normalization`'s value (clap admits only these three).
fn normalization(flag: &str) -> pigeonhole_memory::Normalization {
    match flag {
        "nfkc" => pigeonhole_memory::Normalization::Nfkc,
        "none" => pigeonhole_memory::Normalization::None,
        _ => pigeonhole_memory::Normalization::Nfc,
    }
}

/// A normalisation as `--memory-normalization` names it.
fn normalization_name(n: pigeonhole_memory::Normalization) -> &'static str {
    match n {
        pigeonhole_memory::Normalization::Nfc => "nfc",
        pigeonhole_memory::Normalization::Nfkc => "nfkc",
        pigeonhole_memory::Normalization::None => "none",
        _ => "another normalisation",
    }
}

/// The store's pin against the flags: `--embed-model`/`--memory-model` must name the pinned
/// model, and `--memory-dim` its dimension.
fn check_pin(
    name: &str,
    pinned: Option<&EmbeddingModel>,
    expected: Option<&str>,
    args: &MemoryArgs,
) -> Result<(), Failure> {
    match (pinned, expected) {
        (Some(m), Some(id)) if m.id() != id => Err(Failure::usage(&format!(
            "memory store {name} is pinned to the embedding model {:?}, not {id:?}: embeddings \
             of another model would ruin its recall",
            m.id()
        ))),
        (Some(m), _) if args.memory_dim.is_some_and(|d| d != m.dim()) => {
            Err(Failure::usage(&format!(
                "memory store {name} is pinned to {} dimensions, not {}",
                m.dim(),
                args.memory_dim.unwrap_or_default()
            )))
        }
        (None, Some(id)) => Err(Failure::usage(&format!(
            "memory store {name} has no embedding model, so it can't take {id:?}'s embeddings"
        ))),
        _ => Ok(()),
    }
}

/// A memory crate error as a tool failure, keeping its code.
fn failure(context: &str, e: pigeonhole_memory::Error) -> Failure {
    match e {
        pigeonhole_memory::Error::Db(e) => Failure::db(context, &e),
        e => Failure::coded(e.code(), format!("{context}: {e}")),
    }
}

fn namespace<'a>(a: &'a Args<'_>) -> Result<&'a str, Failure> {
    Ok(a.opt_str("namespace")?.unwrap_or(DEFAULT_NAMESPACE))
}

fn memory_id(s: &str) -> Result<MemoryId, Failure> {
    s.parse()
        .map_err(|_| Failure::usage(&format!("{s:?} isn't a memory id")))
}

fn ids(a: &Args<'_>, name: &str) -> Result<Vec<MemoryId>, Failure> {
    let ids = a.str_list(name)?;
    if ids.is_empty() {
        return Err(Failure::usage(&format!("{name}: at least one id")));
    }
    if ids.len() > MAX_IDS {
        return Err(Failure::usage(&format!("{name}: at most {MAX_IDS} ids")));
    }
    ids.iter().map(|s| memory_id(s)).collect()
}

fn vector(v: &Value) -> Result<Vec<f32>, Failure> {
    let Value::Array(xs) = v else {
        return Err(Failure::usage("embedding: a list of numbers"));
    };
    xs.iter()
        .map(|x| {
            x.as_f64()
                .map(|f| f as f32)
                .filter(|f| f.is_finite())
                .ok_or_else(|| Failure::usage("embedding: a list of numbers"))
        })
        .collect()
}

fn kind(s: &str) -> Result<Kind, Failure> {
    Ok(match s {
        "episodic" => Kind::Episodic,
        "semantic" => Kind::Semantic,
        "procedural" => Kind::Procedural,
        _ => {
            return Err(Failure::usage(&format!(
                "kind {s:?}: episodic, semantic or procedural"
            )));
        }
    })
}

fn kind_name(k: Kind) -> &'static str {
    match k {
        Kind::Episodic => "episodic",
        Kind::Semantic => "semantic",
        Kind::Procedural => "procedural",
        _ => "other",
    }
}

/// A link's `type`: `derived_from` (default), `similar_to`, or an application's number
/// from 16. `supersedes` is the store's own, made by `memory_supersede`.
fn link_type(a: &Args<'_>) -> Result<LinkType, Failure> {
    match a.get("type") {
        None => Ok(LinkType::DERIVED_FROM),
        Some(Value::String(s)) => match s.as_str() {
            "derived_from" => Ok(LinkType::DERIVED_FROM),
            "similar_to" => Ok(LinkType::SIMILAR_TO),
            "supersedes" => Err(Failure::usage(
                "type supersedes: use memory_supersede, which keeps recall in step",
            )),
            _ => Err(Failure::usage(&format!(
                "type {s:?}: derived_from, similar_to, or a number from {}",
                LinkType::FIRST_CUSTOM
            ))),
        },
        Some(v) => {
            let n = v
                .as_u64()
                .and_then(|n| u8::try_from(n).ok())
                .ok_or_else(|| Failure::usage("type: a name or a number from 16 to 255"))?;
            LinkType::custom(n).map_err(|e| failure("type", e))
        }
    }
}

fn weight(a: &Args<'_>) -> Result<f32, Failure> {
    match a.get("weight") {
        None => Ok(1.0),
        Some(v) => v
            .as_f64()
            .filter(|w| (0.0..=1.0).contains(w))
            .map(|w| w as f32)
            .ok_or_else(|| Failure::usage("weight: a number from 0 to 1")),
    }
}

fn namespace_schema() -> Value {
    json!({
        "type": "string",
        "description": "Whose or what memory: a user, project or agent (\"default\" if omitted).",
    })
}

fn embedding_schema() -> Value {
    json!({
        "type": "array",
        "items": {"type": "number"},
        "description": "The text's embedding, from exactly the store's model (with embedding_model).",
    })
}

fn embedding_model_schema() -> Value {
    json!({
        "type": "string",
        "description": "The id of the model that made embedding: must be the store's.",
    })
}

fn link_item_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "to": {"type": "string", "description": "The linked memory's id."},
            "type": {
                "description": "derived_from (default), similar_to, or an application's number from 16 to 255.",
                "anyOf": [
                    {"type": "string", "enum": ["derived_from", "similar_to"]},
                    {"type": "integer", "minimum": 16, "maximum": 255},
                ],
            },
            "weight": {"type": "number", "minimum": 0, "maximum": 1, "description": "1 by default."},
        },
        "required": ["to"],
        "additionalProperties": false,
    })
}

fn ids_schema(description: &str) -> Value {
    json!({
        "type": "object",
        "properties": {
            "ids": {
                "type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": MAX_IDS,
                "description": description,
            },
        },
        "required": ["ids"],
        "additionalProperties": false,
    })
}

fn mark_used_tool() -> Value {
    tool(
        "memory_mark_used",
        "Mark memories used",
        "Counts a use of each memory that helped with the task (from memory_recall's ids): \
         used memories rank higher in later recalls and are kept longer. Returns how many were \
         marked, and the ids that no longer exist.",
        ids_schema("The memories that helped."),
        false,
    )
}

fn supersede_tool() -> Value {
    tool(
        "memory_supersede",
        "Supersede a memory",
        "Marks old as replaced by new, a newer memory of the same fact: recall leaves old out \
         from then on (unless include_superseded). Use it rather than keeping two memories that \
         contradict each other. A memory is superseded once.",
        json!({
            "type": "object",
            "properties": {
                "old": {"type": "string", "description": "The outdated memory's id."},
                "new": {"type": "string", "description": "The id of the memory that replaces it."},
            },
            "required": ["old", "new"],
            "additionalProperties": false,
        }),
        false,
    )
}

fn link_tool() -> Value {
    let mut schema = link_item_schema();
    schema["properties"]["from"] =
        json!({"type": "string", "description": "The linking memory's id."});
    schema["required"] = json!(["from", "to"]);
    tool(
        "memory_link",
        "Link memories",
        "Links from to to, two memories of one namespace: derived_from (from was drawn from \
         to), similar_to, or an application's type. Linking again replaces the weight.",
        schema,
        false,
    )
}

fn forget_tool() -> Value {
    tool(
        "memory_forget",
        "Forget memories",
        "Deletes memories for good, pinned ones too: when the user asks you to forget \
         something, or a memory is wrong. For a fact that changed, prefer memory_remember with \
         supersedes, which keeps the history. Forgetting a memory that superseded another \
         brings the older one back into recall. Returns how many were deleted and the ids that \
         didn't exist; on an error partway, also error and not_deleted (the deletes before \
         it stand).",
        ids_schema("The memories to delete."),
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_types_and_weights_parse_and_refuse() {
        let m = |v: Value| match v {
            Value::Object(m) => m,
            _ => unreachable!(),
        };
        let types = [
            (json!({}), Some(LinkType::DERIVED_FROM.get())),
            (
                json!({"type": "similar_to"}),
                Some(LinkType::SIMILAR_TO.get()),
            ),
            (json!({"type": 16}), Some(16)),
            (json!({"type": 15}), None),
            (json!({"type": 256}), None),
            (json!({"type": "supersedes"}), None),
            (json!({"type": "nope"}), None),
        ];
        for (args, want) in types {
            let map = m(args.clone());
            let got = link_type(&Args(&map)).ok().map(LinkType::get);
            assert_eq!(got, want, "{args}");
        }
        for (args, ok) in [
            (json!({}), true),
            (json!({"weight": 0}), true),
            (json!({"weight": 1.5}), false),
            (json!({"weight": "x"}), false),
        ] {
            let map = m(args.clone());
            assert_eq!(weight(&Args(&map)).is_ok(), ok, "{args}");
        }
    }

    #[test]
    fn the_guide_covers_the_owners_points() {
        for needle in [
            "memory_mark_used",
            "supersedes",
            "max_bytes",
            "namespace",
            "importance",
            "embedding_model",
            "--embed-url",
        ] {
            assert!(GUIDE.contains(needle), "the guide doesn't mention {needle}");
        }
    }
}
