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
use super::feed::TakeClock;
use super::lease::{DeliveryLease, LeaseHooks};
use super::proto;
use crate::memcore::{MemCore, MemError, MemErrorKind, RecallArgs, Remember, Scope};

/// Events a watch buffers for its client.
const EVENTS: usize = 64;

impl From<MemError> for ApiError {
    fn from(e: MemError) -> Self {
        match e.kind {
            MemErrorKind::Invalid | MemErrorKind::Rejected => Self::invalid(e.message),
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
pub(crate) fn vector_out(v: &[f32]) -> proto::Vector {
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

/// A watch's events, then its error if it failed. On a cluster partition (a component of a
/// cluster watch, #1915 PR 2b), delivered only under the partition's lease, and ended by the
/// partition's close or move.
pub(crate) struct MemoryEvents {
    events: mpsc::Receiver<Result<proto::MemoryEvent, ApiError>>,
    /// Each take's time: the watch's idle limit (`--stream-idle-ms`) counts from the last.
    taken: Arc<TakeClock>,
    part: Option<Box<Leased>>,
}

/// A component's lease, and its partition's end.
struct Leased {
    lease: DeliveryLease,
    /// The partition's components' end: a close's `PartitionUnavailable`, or a move's
    /// `WrongPartition` ([`super::feed::Feeds::end_parts`]).
    ended: tokio::sync::watch::Receiver<Option<ApiError>>,
}

impl std::fmt::Debug for MemoryEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.part.is_some() {
            "MemoryEvents(component)"
        } else {
            "MemoryEvents"
        })
    }
}

impl MemoryEvents {
    /// Events from a channel another task fills (a cluster watch's merge), stamping `taken`
    /// on each take.
    pub(crate) fn from_channel(
        events: mpsc::Receiver<Result<proto::MemoryEvent, ApiError>>,
        taken: Arc<TakeClock>,
    ) -> Self {
        Self {
            events,
            taken,
            part: None,
        }
    }
}

/// The hooks the lease check calls while the partition holds no lease (simp724's M1, M2).
struct Hooks<'a>(
    &'a mut tokio::sync::watch::Receiver<Option<ApiError>>,
    &'a TakeClock,
);

impl LeaseHooks for Hooks<'_> {
    fn poll_own_end(&mut self, _cx: &mut Context<'_>) -> Poll<Option<ApiError>> {
        // The end is a value set once; a look on each poll (every `LEASE_WAIT` while
        // waiting) sees it.
        if self.0.has_changed().unwrap_or(false) {
            return Poll::Ready(self.0.borrow_and_update().clone());
        }
        Poll::Pending
    }

    fn took(&self) {
        self.1.took();
    }
}

impl Stream for MemoryEvents {
    type Item = Result<proto::MemoryEvent, ApiError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if let Some(part) = this.part.as_mut() {
            let Leased { lease, ended } = &mut **part;
            if let Some(answer) = lease.before(cx, &mut Hooks(ended, &this.taken)) {
                return answer;
            }
        }
        let out = this.events.poll_recv(cx);
        if let Poll::Ready(Some(_)) = out {
            this.taken.took();
        }
        if let Some(part) = this.part.as_mut() {
            part.lease.after(out.is_pending(), cx);
        }
        out
    }
}

/// A cluster partition's part of a watch (#1915 PR 2b): its lease and its end.
pub(crate) struct WatchPart {
    pub(crate) gate: Arc<super::gate::WriteGate>,
    pub(crate) ended: tokio::sync::watch::Receiver<Option<ApiError>>,
}

/// Starts a watch of `namespaces` (all of them the caller's: an empty list is refused for a
/// namespace-scoped token) from `resume`, or from now. The task holds `slot` until it ends.
///
/// With `part`, it's a cluster watch's component (ICR 0139, PR 2b), which a merge reads
/// (here, or over `Cluster.ForwardMemoryWatch`). Its stream then marks commits with
/// `Progress` events, which the merge takes and never passes on: one first, at the position
/// it starts from, and one after each commit with memories, with that commit's token, so
/// the merge moves whole commits ([`super::cluster::memwatch`]).
#[allow(clippy::too_many_arguments)]
pub(crate) fn watch(
    core: Arc<MemCore>,
    scope: Scope<'_>,
    r: proto::MemoryWatchRequest,
    slot: OwnedSemaphorePermit,
    pool: Arc<Semaphore>,
    idle: Duration,
    mut swaps: tokio::sync::watch::Receiver<u64>,
    part: Option<WatchPart>,
) -> Result<MemoryEvents, ApiError> {
    scope.check_all(&r.namespaces)?;
    // Refused at once where the partition's lease lapsed (feed1248's F2).
    let part = part
        .map(|p| -> Result<_, ApiError> {
            Ok(Leased {
                lease: DeliveryLease::new(p.gate)?,
                ended: p.ended,
            })
        })
        .transpose()?;
    let ns: Vec<&str> = r.namespaces.iter().map(String::as_str).collect();
    let mut builder = core.store()?.watch(&ns);
    if !r.resume.is_empty() {
        builder = builder.resume(&WatchToken::from_bytes(r.resume.to_vec()));
    }
    let mut w = builder
        .open()
        .map_err(|e| MemError::store_error("watch", &e))?;
    let marks = part.is_some();
    let mut ended = part.as_ref().map(|p| p.ended.clone());
    let taken = Arc::new(TakeClock::new());
    let (tx, rx) = mpsc::channel(EVENTS);
    let took = Arc::clone(&taken);
    tokio::spawn(async move {
        let _slot = slot;
        // Room in the channel, within `--stream-idle-ms` of the client's last take: a
        // component waiting for its first lease stamps the clock as it looks (simp724's M2),
        // so that wait never trips it.
        let send = |e| async {
            let mut e = Some(e);
            loop {
                match tokio::time::timeout(idle, tx.reserve()).await {
                    Ok(Ok(room)) => {
                        room.send(e.take().expect("sent once"));
                        return true;
                    }
                    // The client went away.
                    Ok(Err(_)) => return false,
                    // It took nothing for --stream-idle-ms.
                    Err(_) if took.last_or_start().elapsed() >= idle => return false,
                    Err(_) => {}
                }
            }
        };
        let progress = |token: &prost::bytes::Bytes| {
            Ok(proto::MemoryEvent {
                token: token.clone(),
                event: Some(proto::memory_event::Event::Progress(proto::Progress {})),
            })
        };
        if marks {
            // The start mark: where the component starts, so the merge's tokens cover it
            // from the first event on.
            let start: prost::bytes::Bytes = w.token().as_bytes().to_vec().into();
            if !send(progress(&start)).await {
                return;
            }
        }
        loop {
            // A client that went away ends the watch here, and gives its stream slot back,
            // even on a quiet store with no commit to send it (ai1249's R1 on #1617).
            let next = tokio::select! {
                biased;
                () = tx.closed() => return,
                // A cluster partition's close or move ends its components, with its own
                // error, before the database closes under them.
                why = async {
                    // The partition's feeds dropped: nothing ends it this way any more.
                    let Some(rx) = ended.as_mut() else {
                        return std::future::pending().await;
                    };
                    if rx.changed().await.is_err() {
                        return std::future::pending().await;
                    }
                    rx.borrow().clone()
                } => {
                    let why = why.unwrap_or_else(|| {
                        ApiError::internal("the partition's watches ended this one")
                    });
                    let _ = send(Err(why)).await;
                    return;
                }
                // The database was swapped under the store (a replica's re-bootstrap): the
                // watch is of the old copy, and ends rather than go quiet (#1618).
                _ = swaps.changed() => {
                    let _ = send(Err(ApiError::from_code(
                        pigeonhole::ErrorCode::FeedExpired,
                        "the database was replaced (a replica's re-bootstrap): this watch was \
                         of the old copy; watch again, without its token",
                    )))
                    .await;
                    return;
                }
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
            let mut events = if new.is_empty() {
                vec![progress(&token)]
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
            // A component's commit with memories ends with its mark. (One whose memories
            // were all forgotten before the read sends nothing, as on a single node.)
            let memories = matches!(
                events.first(),
                Some(Ok(proto::MemoryEvent {
                    event: Some(proto::memory_event::Event::Memory(_)),
                    ..
                }))
            );
            if marks && memories {
                events.push(progress(&token));
            }
            for e in events {
                let failed = e.is_err();
                if !send(e).await || failed {
                    return;
                }
            }
        }
    });
    Ok(MemoryEvents {
        events: rx,
        taken,
        part: part.map(Box::new),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::gate::{LeaseClock, WriteGate};

    /// A store `notes` in a fresh database with a change feed, and its directory.
    fn core(name: &str) -> (Arc<MemCore>, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("phdb-memwatch-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = pigeonhole::Pigeonhole::open(
            dir.join("m.phdb"),
            pigeonhole::Options::default()
                .create_if_missing(true)
                .shards(1)
                .change_feed(pigeonhole::FeedRetention::bytes(4 << 20)),
        )
        .unwrap();
        let create: Arc<crate::memcore::Create> =
            Arc::new(|_| Ok(pigeonhole_memory::StoreOptions::new()));
        let core = MemCore::open_in(
            &db,
            "notes",
            None,
            None,
            Some(&*create),
            (1 << 20, 8 << 20),
            false,
        )
        .ok()
        .flatten()
        .expect("the store opens");
        (Arc::new(core), dir)
    }

    fn remember(core: &MemCore, text: &str) {
        core.remember(
            Scope(None),
            crate::memcore::Remember {
                namespace: "alice".into(),
                text: text.into(),
                embedding: None,
                kind: None,
                importance: None,
                source: None,
                session: None,
                pinned: false,
                links: Vec::new(),
            },
            None,
        )
        .unwrap();
    }

    /// A cluster partition's component of a watch of "alice", idle limit `idle`.
    fn component(
        core: &Arc<MemCore>,
        gate: &Arc<WriteGate>,
        ended: tokio::sync::watch::Receiver<Option<ApiError>>,
        idle: Duration,
    ) -> Result<MemoryEvents, ApiError> {
        let streams = Arc::new(Semaphore::new(4));
        let (_swap, swaps) = tokio::sync::watch::channel(0);
        // The swap sender lives as long as the test: a dropped one would end the watch.
        std::mem::forget(_swap);
        watch(
            Arc::clone(core),
            Scope(None),
            proto::MemoryWatchRequest {
                namespaces: vec!["alice".into()],
                resume: Default::default(),
            },
            streams.try_acquire_owned().unwrap(),
            Arc::new(Semaphore::new(4)),
            idle,
            swaps,
            Some(WatchPart {
                gate: Arc::clone(gate),
                ended,
            }),
        )
    }

    async fn next(events: &mut MemoryEvents) -> Option<Result<proto::MemoryEvent, ApiError>> {
        std::future::poll_fn(|cx| Pin::new(&mut *events).poll_next(cx)).await
    }

    fn is_memory(e: &Option<Result<proto::MemoryEvent, ApiError>>) -> bool {
        matches!(
            e,
            Some(Ok(proto::MemoryEvent {
                event: Some(proto::memory_event::Event::Memory(_)),
                ..
            }))
        )
    }

    /// #1915 PR 2b (M2, as D326 PR 2's feed components): a memory watch's component on a
    /// cluster partition delivers only under its lease. Without one since it opened, it
    /// waits, delivering nothing, its start mark included; once leased, it delivers its
    /// start mark, then each commit's memories and the commit's end mark; when the lease
    /// runs out, it ends with the lease's error even with nothing to deliver; and a node
    /// whose last round reached no majority refuses it.
    #[tokio::test]
    async fn a_watch_component_waits_for_a_lease_and_ends_when_it_runs_out() {
        let (core, dir) = core("lease");
        let clock = Arc::new(LeaseClock::default());
        let gate = WriteGate::opened("p1", false, Arc::clone(&clock));
        let (_end, ended) = tokio::sync::watch::channel(None);
        let mut events = component(&core, &gate, ended, Duration::from_secs(5)).unwrap();
        remember(&core, "alice ate lunch");
        assert!(
            tokio::time::timeout(Duration::from_millis(300), next(&mut events))
                .await
                .is_err(),
            "nothing before the lease"
        );
        gate.renew(clock.now() + 600_000_000);
        let mut got = Vec::new();
        for _ in 0..3 {
            got.push(
                tokio::time::timeout(Duration::from_secs(5), next(&mut events))
                    .await
                    .expect("delivered once leased"),
            );
        }
        // The start mark, the memory, then its commit's end mark with the memory's token.
        let token = |e: &Option<Result<proto::MemoryEvent, ApiError>>| match e {
            Some(Ok(e)) => e.token.clone(),
            other => panic!("an event: {other:?}"),
        };
        assert!(!is_memory(&got[0]) && is_memory(&got[1]) && !is_memory(&got[2]));
        assert_ne!(token(&got[0]), token(&got[1]));
        assert_eq!(token(&got[1]), token(&got[2]));
        let end = tokio::time::timeout(Duration::from_secs(5), next(&mut events))
            .await
            .expect("woken at the lease's end");
        let Some(Err(e)) = end else {
            panic!("the lease's error, not an event or the end");
        };
        assert_eq!(e.code, "PartitionUnavailable", "{}", e.message);
        assert!(next(&mut events).await.is_none(), "then the end");

        // A node whose last round reached no majority: refused at once.
        clock.set_majority(false);
        let gate = WriteGate::opened("p2", false, Arc::clone(&clock));
        let (_end, ended) = tokio::sync::watch::channel(None);
        let e = component(&core, &gate, ended, Duration::from_secs(5)).unwrap_err();
        assert_eq!(e.code, "PartitionUnavailable", "{}", e.message);
        drop(core);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// simp724's M2 on #1915 PR 2b (feed1248's S1 for the feed): a component waiting for its
    /// first lease takes nothing, and its idle limit counts from the last take. Each look
    /// while it waits counts as one, so a backlog that fills its channel meanwhile isn't
    /// dropped as idle; once leased, every commit is delivered.
    #[tokio::test]
    async fn a_watch_component_waiting_for_its_lease_isnt_dropped_as_idle() {
        let (core, dir) = core("idle");
        let clock = Arc::new(LeaseClock::default());
        let gate = WriteGate::opened("p1", false, Arc::clone(&clock));
        let (_end, ended) = tokio::sync::watch::channel(None);
        let mut events = component(&core, &gate, ended, Duration::from_millis(300)).unwrap();
        // More commits than the channel holds, each a memory and its end mark.
        let n = EVENTS;
        for i in 0..n {
            remember(&core, &format!("alice note {i}"));
        }
        // Four idle limits of waiting, the channel full behind it.
        assert!(
            tokio::time::timeout(Duration::from_millis(1200), next(&mut events))
                .await
                .is_err()
        );
        gate.renew(clock.now() + 3_600_000_000_000);
        let mut memories = 0;
        while memories < n {
            let got = tokio::time::timeout(Duration::from_secs(5), next(&mut events))
                .await
                .unwrap_or_else(|_| panic!("delivered once leased ({memories} so far)"));
            assert!(matches!(got, Some(Ok(_))), "{got:?}");
            memories += usize::from(is_memory(&got));
        }
        drop(core);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// simp724's M1 on #1915 PR 2b (audit120's S-C5 for the feed): a component whose
    /// partition moved away ends with the move's `WrongPartition`, not its lease's
    /// `PartitionUnavailable`, though both are there when it's next polled.
    #[tokio::test]
    async fn a_moved_watch_component_ends_with_wrong_partition() {
        let (core, dir) = core("moved");
        let clock = Arc::new(LeaseClock::default());
        let gate = WriteGate::opened("p1", false, Arc::clone(&clock));
        let (end, ended) = tokio::sync::watch::channel(None);
        gate.renew(clock.now() + 300_000_000);
        let mut events = component(&core, &gate, ended, Duration::from_secs(5)).unwrap();
        let start = tokio::time::timeout(Duration::from_secs(5), next(&mut events))
            .await
            .unwrap();
        assert!(matches!(start, Some(Ok(_))));
        // The lease runs out, and the move's end is sent: the poll after sees both.
        tokio::time::sleep(Duration::from_millis(400)).await;
        end.send_replace(Some(ApiError::wrong_partition(
            2,
            "map version 2 moved partition \"p1\" off this node",
        )));
        let got = next(&mut events).await;
        let Some(Err(e)) = got else {
            panic!("the move's error: {got:?}");
        };
        assert_eq!(e.code, "WrongPartition", "{}", e.message);
        drop(core);
        let _ = std::fs::remove_dir_all(dir);
    }
}
