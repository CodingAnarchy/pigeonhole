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

use std::sync::Arc;

use pigeonhole_memory::{EmbeddingModel, Kind, LinkType, MemoryId, Remembered};
use serde_json::{Map, Value, json};

use super::tools::{Args, Outcome, tool};
use crate::args::MemoryArgs;
use crate::exit::Failure;
use crate::memcore::{
    self, Embedder, MemCore, MemError, MemErrorKind, OpenError, RecallArgs, Remember, Scope,
};
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

/// The memory tools over one store: `phdb mcp`'s arguments and answers around the store's
/// operations in [`MemCore`], the one implementation `phdb serve`'s `Memory` service shares
/// (D313 ticket 8b).
pub(crate) struct Memory {
    name: String,
    core: MemCore,
    /// Whether writes can succeed.
    writable: bool,
    /// `--embed-url`: the server embeds text itself.
    embeds: bool,
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
        // `phdb mcp` has no placement: a local model's threads go where the OS puts them.
        let embedder = Embedder::from_args(
            args,
            super::embed::TIMEOUT,
            crate::memcore::LocalPlace::default(),
        )?
        .map(Arc::new);
        let embeds = embedder.is_some();
        // A recall's budget: `--max-result-bytes` when it asks for none, any larger it asks.
        let budget = (result_bytes as u64, u64::MAX);
        let open = |e: OpenError| match e {
            OpenError::Pin(f) | OpenError::Create(f) => f,
            OpenError::Store(e) => failure(&format!("memory store {name}"), e),
        };
        let exists = db.tables().iter().any(|t| *t == format!("mem_{name}"));
        let core = match db {
            Db::Writer(p) => {
                let create = memcore::create_options(args, &name);
                MemCore::open_in(
                    p,
                    &name,
                    embedder,
                    args.memory_model.as_deref(),
                    writable.then_some(&*create),
                    budget,
                )
                .map_err(open)?
                .ok_or_else(|| {
                    Failure::usage(&format!(
                        "--memory {name}: the store doesn't exist, and the database is read-only"
                    ))
                })?
            }
            Db::Reader(r) => {
                MemCore::open_reader(r, &name, embedder, args.memory_model.as_deref(), budget)
                    .map_err(open)?
                    .ok_or_else(|| {
                        Failure::usage(&format!(
                            "--memory {name}: the store doesn't exist (a reader can't create it)"
                        ))
                    })?
            }
        };
        if !exists {
            let _ = writeln!(
                stderr,
                "phdb mcp: created memory store {name} ({})",
                match core.model() {
                    Some(m) => format!("embedding model {}, {} dimensions", m.id(), m.dim()),
                    None => "no embedding model: recall is by keyword and recency, and the \
                             store can never take embeddings"
                        .into(),
                }
            );
        }
        // An existing store's dimension against `--memory-dim`.
        if let (Some(m), Some(dim)) = (core.model(), args.memory_dim)
            && m.dim() != dim
        {
            return Err(Failure::usage(&format!(
                "memory store {name} is pinned to {} dimensions, not {dim}",
                m.dim()
            )));
        }
        // The normalisation is fixed at create: a flag naming another one for an existing
        // store fails at startup, as a model that doesn't match does (#1405).
        if let Some(want) = &args.memory_normalization
            && core.normalization() != normalization(want)
        {
            return Err(Failure::usage(&format!(
                "--memory-normalization {want}: memory store {name}'s text index is {}, fixed \
                 when it was created",
                normalization_name(core.normalization())
            )));
        }
        if exists && (args.memory_episodic_ttl.is_some() || args.memory_dim.is_some()) {
            let _ = writeln!(
                stderr,
                "phdb mcp: memory store {name} exists: --memory-episodic-ttl and --memory-dim \
                 apply only to a new store"
            );
        }
        let writable = writable && core.writable();
        Ok(Some(Self {
            name,
            core,
            writable,
            embeds,
            result_bytes,
        }))
    }

    /// The store's pinned model.
    fn model(&self) -> Option<&EmbeddingModel> {
        self.core.model()
    }

    fn writer(&self) -> Result<(), Failure> {
        if self.writable {
            Ok(())
        } else {
            Err(Failure::read_only("reader"))
        }
    }

    /// The memory paragraph of the server's instructions.
    pub(super) fn instructions(&self) -> String {
        let embeddings = match (self.embeds, self.model()) {
            (true, _) => {
                "The server computes embeddings: send text only, never an embedding.".to_owned()
            }
            (false, Some(m)) => format!(
                "The store is pinned to the embedding model {:?} ({} dimensions): send an \
                 embedding only if it comes from exactly that model, with embedding_model set \
                 to its id; otherwise send text only and recall falls back to keywords.",
                m.id(),
                m.dim()
            ),
            (false, None) => {
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
        drop(self.core);
    }

    /// A client's embedding, checked as `phdb mcp` documents it: with `--embed-url` there's
    /// none (the server embeds the text); otherwise it must name `embedding_model`, the
    /// store's, and fit its dimension. The store's core checks it again.
    fn client_embedding(&self, a: &Args<'_>) -> Result<Option<Vec<f32>>, Failure> {
        let given = a.get("embedding");
        let named = a.opt_str("embedding_model")?;
        if self.embeds {
            if given.is_some() || named.is_some() {
                return Err(Failure::usage(
                    "this server computes the embeddings (--embed-url or --embed-local): send text only",
                ));
            }
            return Ok(None);
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
        self.writer()?;
        let ns = namespace(&a)?;
        let text = a.str("text")?;
        let supersedes = a.opt_str("supersedes")?.map(memory_id).transpose()?;
        let embedding = self.client_embedding(&a)?;
        if embedding.is_none()
            && !self.embeds
            && let Some(model) = self.model()
        {
            return Err(Failure::usage(&format!(
                "this store is pinned to the embedding model {:?}: a memory needs its \
                 embedding (with embedding_model)",
                model.id()
            )));
        }
        let importance = match a.get("importance") {
            None => None,
            Some(v) => Some(
                v.as_f64()
                    .filter(|i| (0.0..=1.0).contains(i))
                    .ok_or_else(|| Failure::usage("importance: a number from 0 to 1"))?,
            ),
        };
        let mut links = Vec::new();
        for l in a.list("links")? {
            let Value::Object(l) = l else {
                return Err(Failure::usage("links: a list of {to, type, weight}"));
            };
            let l = Args(l);
            l.only(&["to", "type", "weight"])
                .map_err(|f| f.within("links"))?;
            links.push((memory_id(l.str("to")?)?, link_type(&l)?, weight(&l)?));
        }
        let r = Remember {
            namespace: ns.to_owned(),
            text: text.to_owned(),
            embedding,
            kind: a.opt_str("kind")?.map(kind).transpose()?,
            importance,
            source: a.opt_str("source")?.map(str::to_owned),
            session: a.opt_str("session")?.map(str::to_owned),
            pinned: a.bool_or("pinned", false)?,
            links,
        };
        // A correction is near what it corrects, so with `supersedes` the core skips near
        // dedup (simp724's S1 on #1389), keeping exact dedup; the supersede is a second
        // transaction whose failure comes back with the stored id (simp724's S2).
        let (done, supersede_error) = self
            .core
            .remember(ALL, r, supersedes.clone())
            .map_err(mem_failure)?;
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
            let superseded = old != *id && supersede_error.is_none();
            if let Some(e) = supersede_error {
                out.insert("supersede_error".into(), mem_failure(e).json());
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
        let embedding = self.client_embedding(&a)?;
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
        let (hits, stats) = self
            .core
            .recall(
                ALL,
                RecallArgs {
                    namespaces,
                    text: text.map(str::to_owned),
                    embedding,
                    k: k as usize,
                    byte_budget: max_bytes,
                    expand_hops: 0,
                    exact: false,
                    include_superseded: a.bool_or("include_superseded", false)?,
                    session: a.opt_str("session")?.map(str::to_owned),
                    with_embeddings: false,
                    with_signals: false,
                },
            )
            .map_err(mem_failure)?;
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
        self.writer()?;
        let ids = ids(&a, "ids")?;
        let m = self.core.mark_used(ALL, &ids).map_err(mem_failure)?;
        let missing: Vec<String> = m.missing.iter().map(ToString::to_string).collect();
        Ok(super::tools::object(
            json!({"marked": m.marked, "missing": missing}),
        ))
    }

    fn supersede(&self, a: Args<'_>) -> Outcome {
        a.only(&["old", "new"])?;
        self.writer()?;
        let old = memory_id(a.str("old")?)?;
        let new = memory_id(a.str("new")?)?;
        self.core.supersede(ALL, &old, &new).map_err(mem_failure)?;
        Ok(Map::new())
    }

    fn link(&self, a: Args<'_>) -> Outcome {
        a.only(&["from", "to", "type", "weight"])?;
        self.writer()?;
        let from = memory_id(a.str("from")?)?;
        let to = memory_id(a.str("to")?)?;
        self.core
            .link(ALL, &from, &to, link_type(&a)?, weight(&a)?)
            .map_err(mem_failure)?;
        Ok(Map::new())
    }

    fn forget(&self, a: Args<'_>) -> Outcome {
        a.only(&["ids"])?;
        self.writer()?;
        let ids = ids(&a, "ids")?;
        let done = self.core.forget(ALL, &ids).map_err(mem_failure)?;
        let missing: Vec<String> = done.missing.iter().map(ToString::to_string).collect();
        if let Some((e, left)) = done.failed {
            // The deletes before it stand: report them, the error, and what's left
            // (simp724's S2 on #1389).
            let left: Vec<String> = left.iter().map(ToString::to_string).collect();
            return Ok(super::tools::object(json!({
                "deleted": done.deleted,
                "missing": missing,
                "error": mem_failure(e).json(),
                "not_deleted": left,
            })));
        }
        Ok(super::tools::object(
            json!({"deleted": done.deleted, "missing": missing}),
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
        match (self.embeds, self.model()) {
            (true, _) => "The server computes the embedding: send text only.".into(),
            (false, Some(m)) => format!(
                "The store is pinned to the embedding model {:?} ({} dimensions): an embedding \
                 must come from exactly that model, with embedding_model set to {:?}; never \
                 send another model's.",
                m.id(),
                m.dim(),
                m.id()
            ),
            (false, None) => "The store has no embedding model: send text only.".into(),
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
/// `phdb mcp` has no namespace limit: every call may touch every namespace.
const ALL: Scope<'static> = Scope(None);

/// A store operation's failure as `phdb mcp` reports it: the request's arguments as usage,
/// the store's by its code, and the embedding endpoint's, its refusal of the text included,
/// as bad input (exit ERROR), as `phdb mcp` always has: scripts may check the exit code (the
/// coordinator on #1648).
fn mem_failure(e: MemError) -> Failure {
    match e.kind {
        MemErrorKind::Invalid | MemErrorKind::Denied => Failure::usage(&e.message),
        MemErrorKind::Engine(code) => Failure::coded(code, e.message),
        MemErrorKind::Rejected
        | MemErrorKind::Unavailable
        | MemErrorKind::Deadline
        | MemErrorKind::Internal => Failure::input(e.message),
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

    /// The coordinator on #1648 (ai1249's R1): moving onto the shared core changes no code
    /// `phdb mcp` answers. Every embedding-endpoint failure, its refusal of the text
    /// included, stays bad input (exit ERROR); the arguments' faults stay usage.
    #[test]
    fn the_cores_failures_keep_phdb_mcps_codes() {
        let code = |kind| {
            let f = mem_failure(MemError {
                kind,
                message: "m".into(),
            });
            (f.json()["code"].as_str().unwrap().to_owned(), f.status())
        };
        let bad_input = ("BadInput".to_owned(), crate::ExitStatus::ERROR);
        for kind in [
            MemErrorKind::Rejected,
            MemErrorKind::Unavailable,
            MemErrorKind::Deadline,
            MemErrorKind::Internal,
        ] {
            assert_eq!(code(kind), bad_input, "{kind:?}");
        }
        assert_eq!(
            code(MemErrorKind::Invalid),
            ("Usage".to_owned(), crate::ExitStatus::USAGE)
        );
        assert_eq!(
            code(MemErrorKind::Engine(pigeonhole::ErrorCode::TableNotFound)).0,
            "TableNotFound"
        );
    }

    /// 8b's fix, as ai1249's R1 on 8b asks: `phdb mcp` not writable on a writer database
    /// opens the store without its forgetter. On main it opened the store as the writer does,
    /// whose forgetter runs a pass at once and deletes an episodic memory past its TTL from a
    /// session meant to be read-only.
    #[test]
    fn a_read_only_open_on_a_writer_starts_no_forgetter() {
        use pigeonhole_memory::{Memory as NewMemory, OpenOptions, Store, StoreOptions};
        let dir = pigeonhole::doc_support::temp_dir();
        let db = pigeonhole::Pigeonhole::open(
            dir.join("m.phdb"),
            pigeonhole::Options::default().shards(1),
        )
        .unwrap();
        let store = Store::create(
            &db,
            "m",
            StoreOptions::new().episodic_ttl(std::time::Duration::from_secs(1)),
        )
        .unwrap();
        // The store's own forgetter is taken, so nothing deletes until the open under test.
        drop(store.take_forgetter());
        let id = store
            .remember("ns", NewMemory::new("an episode").kind(Kind::Episodic))
            .unwrap()
            .id()
            .clone();
        drop(store);
        // Past its TTL by the time the open below could start a forgetter.
        std::thread::sleep(std::time::Duration::from_millis(1_500));
        let args = MemoryArgs {
            memory: Some("m".into()),
            ..MemoryArgs::default()
        };
        let mut notes = Vec::new();
        let memory = Memory::open(&Db::Writer(db.clone()), &args, false, 1 << 20, &mut notes)
            .unwrap()
            .unwrap();
        // A forgetter's first pass runs as it starts: give one the time it would need.
        std::thread::sleep(std::time::Duration::from_millis(1_500));
        let probe = Store::open_with(
            &db,
            "m",
            OpenOptions::new().without_embeddings().own_forgetter(),
        )
        .unwrap();
        assert!(
            probe.get(&id).unwrap().is_some(),
            "a read-only open deleted an expired memory"
        );
        drop((probe, memory));
        db.close().unwrap();
    }

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
