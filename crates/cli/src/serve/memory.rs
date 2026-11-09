//! `Memory` (D313 ticket 8a, ICR 0112 over ICR 0107): the proto's messages to and from the
//! memory core (`crate::memcore`), which holds the store, the embedder and the namespace
//! checks, and `Watch`'s stream.
//!
//! - **Ids** are a memory's byte form (`MemoryId::to_bytes`); a malformed one is
//!   `InvalidArgument`.
//! - **Embeddings** come in the store's element type or `f32`; `f16` and `bf16` are widened,
//!   since the store takes `f32` and narrows to its element itself.
//! - **`Watch`** runs on a task of its own under a `--max-streams` slot: it reads the store's
//!   watch, reads each new memory (on the blocking pool), and hands events to the client
//!   through a channel of [`EVENTS`], waiting at most `--stream-idle-ms` for room, as a feed
//!   stream does.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use pigeonhole_memory::{Kind, LinkType, MemoryEvent as Event, MemoryId, RecallHit, WatchToken};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use super::convert::{self, Elements};
use super::errors::ApiError;
use super::proto;
use crate::memcore::{MemCore, MemError, MemErrorKind, RecallArgs, Remember, Scope};

/// Events a watch buffers for its client.
const EVENTS: usize = 64;

impl From<MemError> for ApiError {
    fn from(e: MemError) -> Self {
        match e.kind {
            MemErrorKind::Invalid => Self::invalid(e.message),
            MemErrorKind::Denied => Self::permission_denied(&e.message),
            MemErrorKind::Engine(code) => Self::from_code(code, &e.message),
            MemErrorKind::Unavailable => Self::unavailable(e.message),
            MemErrorKind::Deadline => Self::deadline(e.message),
            MemErrorKind::Internal => Self::internal(e.message),
        }
    }
}

pub(crate) fn id(bytes: &[u8]) -> Result<MemoryId, ApiError> {
    MemoryId::from_bytes(bytes).map_err(|e| ApiError::invalid(format!("a memory id: {e}")))
}

pub(crate) fn ids(list: &[prost::bytes::Bytes]) -> Result<Vec<MemoryId>, ApiError> {
    list.iter().map(|b| id(b)).collect()
}

fn kind_in(k: i32) -> Result<Option<Kind>, ApiError> {
    Ok(match proto::MemoryKind::try_from(k) {
        Ok(proto::MemoryKind::Unspecified) => None,
        Ok(proto::MemoryKind::Episodic) => Some(Kind::Episodic),
        Ok(proto::MemoryKind::Semantic) => Some(Kind::Semantic),
        Ok(proto::MemoryKind::Procedural) => Some(Kind::Procedural),
        Err(_) => return Err(ApiError::invalid("an unknown memory kind")),
    })
}

fn kind_out(k: Kind) -> i32 {
    (match k {
        Kind::Episodic => proto::MemoryKind::Episodic,
        Kind::Semantic => proto::MemoryKind::Semantic,
        Kind::Procedural => proto::MemoryKind::Procedural,
        _ => proto::MemoryKind::Unspecified,
    }) as i32
}

/// A link's type: the built-in ones, or a custom number. `supersedes` is refused: Supersede
/// keeps recall in step.
pub(crate) fn link_type(t: Option<&proto::LinkType>) -> Result<LinkType, ApiError> {
    use proto::link_type::Kind as K;
    match t.and_then(|t| t.kind.as_ref()) {
        None => Ok(LinkType::DERIVED_FROM),
        Some(K::Builtin(b)) => match proto::BuiltinLink::try_from(*b) {
            Ok(proto::BuiltinLink::Unspecified | proto::BuiltinLink::DerivedFrom) => {
                Ok(LinkType::DERIVED_FROM)
            }
            Ok(proto::BuiltinLink::SimilarTo) => Ok(LinkType::SIMILAR_TO),
            Ok(proto::BuiltinLink::Supersedes) => Err(ApiError::invalid(
                "a supersedes link: use Supersede, which keeps recall in step",
            )),
            Err(_) => Err(ApiError::invalid("an unknown built-in link type")),
        },
        Some(K::Custom(n)) => u8::try_from(*n)
            .ok()
            .and_then(|n| LinkType::custom(n).ok())
            .ok_or_else(|| {
                ApiError::invalid(format!(
                    "a custom link type is {} to 255",
                    LinkType::FIRST_CUSTOM
                ))
            }),
    }
}

/// An embedding as `f32`s: `f32` as it is, `f16` and `bf16` widened.
fn embedding(v: Option<&proto::Vector>) -> Result<Option<Vec<f32>>, ApiError> {
    let Some(v) = v else { return Ok(None) };
    Ok(Some(match convert::elements(v)? {
        Elements::F32(x) => x,
        Elements::F16(x) => x
            .into_iter()
            .map(|b| half::f16::from_bits(b).to_f32())
            .collect(),
        Elements::Bf16(x) => x
            .into_iter()
            .map(|b| half::bf16::from_bits(b).to_f32())
            .collect(),
        Elements::I8(_) | Elements::Bits(_) => {
            return Err(ApiError::invalid(
                "a memory embedding is f32, or the store's f16 or bf16",
            ));
        }
    }))
}

/// `f32`s as a proto vector.
fn vector_out(v: &[f32]) -> proto::Vector {
    proto::Vector {
        element: proto::Element::F32 as i32,
        dim: v.len() as u32,
        data: v
            .iter()
            .flat_map(|x| x.to_le_bytes())
            .collect::<Vec<u8>>()
            .into(),
    }
}

pub(crate) fn remember(r: proto::RememberRequest) -> Result<Remember, ApiError> {
    let m = r
        .memory
        .ok_or_else(|| ApiError::invalid("Remember needs a memory"))?;
    let links = m
        .links
        .iter()
        .map(|l| Ok((id(&l.to)?, link_type(l.link_type.as_ref())?, l.weight)))
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(Remember {
        namespace: r.namespace,
        embedding: embedding(m.embedding.as_ref())?,
        text: m.text,
        kind: kind_in(m.kind)?,
        importance: m.importance,
        source: Some(m.source).filter(|s| !s.is_empty()),
        session: Some(m.session).filter(|s| !s.is_empty()),
        pinned: m.pinned,
        links,
    })
}

pub(crate) fn remembered(r: &pigeonhole_memory::Remembered) -> proto::RememberResponse {
    use pigeonhole_memory::Remembered as R;
    let outcome = match r {
        R::New(_) => proto::RememberOutcome::New,
        R::Merged(_) => proto::RememberOutcome::Merged,
        R::Similar(_) => proto::RememberOutcome::Similar,
        _ => proto::RememberOutcome::Unspecified,
    };
    proto::RememberResponse {
        id: r.id().to_bytes().into(),
        outcome: outcome as i32,
    }
}

pub(crate) fn recall(r: proto::RecallRequest) -> Result<RecallArgs, ApiError> {
    Ok(RecallArgs {
        embedding: embedding(r.embedding.as_ref())?,
        namespaces: r.namespaces,
        text: Some(r.text).filter(|t| !t.is_empty()),
        k: r.k as usize,
        byte_budget: r.byte_budget,
        expand_hops: r.expand_hops,
        exact: r.exact,
        include_superseded: r.include_superseded,
        session: Some(r.session).filter(|s| !s.is_empty()),
        with_embeddings: r.with_embeddings,
        with_signals: r.with_signals,
    })
}

pub(crate) fn hit(h: RecallHit) -> proto::MemoryHit {
    let mut signals = std::collections::HashMap::new();
    if let Some(s) = &h.signals {
        for (name, sig) in [
            ("vector", &s.vector),
            ("keyword", &s.keyword),
            ("recency", &s.recency),
            ("importance", &s.importance),
            ("uses", &s.uses),
        ] {
            if let Some(sig) = sig {
                signals.insert(name.to_owned(), sig.value);
            }
        }
    }
    proto::MemoryHit {
        id: h.id.to_bytes().into(),
        text: h.text,
        kind: kind_out(h.kind),
        importance: h.importance,
        created: h.created_us,
        superseded: h.superseded_by.is_some(),
        score: h.score,
        embedding: h.embedding.as_deref().map(vector_out),
        signals,
    }
}

/// A watch's events, then its error if it failed.
#[derive(Debug)]
pub(crate) struct MemoryEvents(mpsc::Receiver<Result<proto::MemoryEvent, ApiError>>);

impl Stream for MemoryEvents {
    type Item = Result<proto::MemoryEvent, ApiError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

/// Starts a watch of `namespaces` (all of them the caller's: an empty list is refused for a
/// namespace-scoped token) from `resume`, or from now. The task holds `slot` until it ends.
pub(crate) fn watch(
    core: Arc<MemCore>,
    scope: Scope<'_>,
    r: proto::MemoryWatchRequest,
    slot: OwnedSemaphorePermit,
    pool: Arc<Semaphore>,
    idle: Duration,
) -> Result<MemoryEvents, ApiError> {
    scope.check_all(&r.namespaces)?;
    let ns: Vec<&str> = r.namespaces.iter().map(String::as_str).collect();
    let mut builder = core.store().watch(&ns);
    if !r.resume.is_empty() {
        builder = builder.resume(&WatchToken::from_bytes(r.resume.to_vec()));
    }
    let mut w = builder
        .open()
        .map_err(|e| MemError::store_error("watch", &e))?;
    let (tx, rx) = mpsc::channel(EVENTS);
    tokio::spawn(async move {
        let _slot = slot;
        let send = |e| async {
            match tokio::time::timeout(idle, tx.send(e)).await {
                Ok(Ok(())) => true,
                // The client went away, or took nothing for --stream-idle-ms.
                _ => false,
            }
        };
        loop {
            // A client that went away ends the watch here, and gives its stream slot back,
            // even on a quiet store with no commit to send it (ai1249's R1 on #1617).
            let next = tokio::select! {
                () = tx.closed() => return,
                next = std::future::poll_fn(|cx| w.poll_next(cx)) => next,
            };
            let commit = match next {
                Ok(c) => c,
                Err(e) => {
                    let _ = send(Err(MemError::store_error("watch", &e).into())).await;
                    return;
                }
            };
            let token: prost::bytes::Bytes = w.token().as_bytes().to_vec().into();
            let new: Vec<MemoryId> = commit
                .events
                .iter()
                .filter_map(|e| match e {
                    Event::Remembered(id) => Some(id.clone()),
                    _ => None,
                })
                .collect();
            let events = if new.is_empty() {
                vec![Ok(proto::MemoryEvent {
                    token: token.clone(),
                    event: Some(proto::memory_event::Event::Progress(proto::Progress {})),
                })]
            } else {
                // The new memories' text, kind and time: a read each, on the blocking pool.
                let core = Arc::clone(&core);
                let read = super::service::blocking(&pool, move || {
                    let mut out = Vec::with_capacity(new.len());
                    for id in new {
                        if let Some(m) = core.get(&id)? {
                            out.push(proto::WatchedMemory {
                                namespace: m.id.namespace().to_owned(),
                                id: m.id.to_bytes().into(),
                                text: m.text,
                                kind: kind_out(m.kind),
                                created: m.id.created_us(),
                            });
                        }
                    }
                    Ok(out)
                })
                .await;
                match read {
                    Ok(ms) => ms
                        .into_iter()
                        .map(|m| {
                            Ok(proto::MemoryEvent {
                                token: token.clone(),
                                event: Some(proto::memory_event::Event::Memory(m)),
                            })
                        })
                        .collect(),
                    Err(e) => vec![Err(e)],
                }
            };
            for e in events {
                let failed = e.is_err();
                if !send(e).await || failed {
                    return;
                }
            }
        }
    });
    Ok(MemoryEvents(rx))
}
