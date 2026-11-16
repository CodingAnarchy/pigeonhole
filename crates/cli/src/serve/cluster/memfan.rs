//! Agent memory calls whose namespaces are on several partitions (#1915, ICR 0139).
//!
//! - **`Recall`** runs in steps (`pigeonhole_memory`'s [`RecallPool`]): each partition's
//!   pool of candidates with their raw signals, merged and ranked here as one store holding
//!   every namespace would rank them; then the graph expansion hop by hop, each hop's parts
//!   settled together so its bounds are one store's; then the bodies of the best hits.
//!   Exact against one store but for BM25, whose statistics are each partition's, and only
//!   when quiescent: each step reads each partition at a snapshot of its own.
//! - **`MarkUsed` and `Forget`** split: each partition's ids are a call of their own, sent
//!   together, and the counts summed. A part that may have applied makes the call
//!   `OutcomeUnknown` (simp724's S3).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use pigeonhole_memory::{
    Expand, Fusion, MemoryId, RecallBodies, RecallHopPart, RecallPool, Weights,
};
use tokio::sync::OwnedSemaphorePermit;

use super::fanout::{Boxed, Reach, bounded};
use super::route::Routes;
use crate::serve::auth::Caller;
use crate::serve::errors::ApiError;
use crate::serve::proto::{
    self, ForgetRequest, MarkUsedRequest, RecallBodiesRequest, RecallPoolRequest, RecallRequest,
    forward_request,
};
use crate::serve::router::{Cluster, Place, Router, memory_partitions};
use crate::serve::service::{ServiceOp, ServiceReply};

/// The most namespaces a recall across partitions names (simp724's N4 on #1915): its
/// pools' rows grow with them. A recall on one partition keeps a single node's rules.
pub(crate) const MAX_SPAN_NAMESPACES: usize = 64;

/// Bodies asked for past `k`, so a memory forgotten between the steps is backfilled from
/// the next ranks (simp724's nit c on #1915).
const BACKFILL: usize = 4;

/// A step's reply bytes this build can't read: the nodes' builds differ.
fn unreadable(what: &str, e: &pigeonhole_memory::Error) -> ApiError {
    ApiError::failed_precondition(format!(
        "a recall's {what} from another node that this node can't read: {e}"
    ))
}

/// A library error of the merge or ranking, as the call's.
fn lib(e: &pigeonhole_memory::Error) -> ApiError {
    crate::memcore::MemError::store_error("recall", e).into()
}

impl Router {
    /// The partitions a memory call's namespaces are on, if more than one (#1915): such a
    /// call is answered by [`fan_memory`](Self::fan_memory). `None` for a call on one, or
    /// one `route()` refuses, which then says why.
    pub(crate) fn memory_span(
        cluster: &Cluster,
        op: &ServiceOp,
    ) -> Result<Option<BTreeMap<String, Vec<String>>>, ApiError> {
        let routes = cluster.node.routes();
        let Some(routes) = routes.as_ref() else {
            return Ok(None);
        };
        let namespaces: Vec<String> = match op {
            ServiceOp::Recall(r) => r.namespaces.clone(),
            ServiceOp::MarkUsed(r) => id_namespaces(&r.ids)?,
            ServiceOp::Forget(r) => id_namespaces(&r.ids)?,
            _ => return Ok(None),
        };
        let parts = memory_partitions(routes, namespaces.iter().map(String::as_str))?;
        Ok((parts.len() > 1).then_some(parts))
    }

    /// The `memory_tool_fields` gate (#1915 PR 3b, the coordinator's call): a memory call
    /// that relies on those fields needs every node owning a partition it touches to have
    /// the feature. One that answers without it refuses the call (`FailedPrecondition`
    /// naming it), since an older build would ignore `supersedes` or leave a reply field
    /// empty, a wrong answer rather than a thinner one; one that doesn't answer is
    /// unavailable (`PartitionUnavailable`), as for any call to it. `Ok` on a single node.
    pub(crate) async fn memory_fields(&self, op: &ServiceOp) -> Result<(), ApiError> {
        let Some(cluster) = &self.cluster else {
            return Ok(());
        };
        let routes = cluster.node.routes();
        let Some(routes) = routes.as_ref() else {
            return Ok(());
        };
        let (namespaces, what): (Vec<String>, &str) = match op {
            ServiceOp::Remember(r) => (vec![r.namespace.clone()], "a remember with `supersedes`"),
            ServiceOp::Recall(r) => (r.namespaces.clone(), "this recall"),
            ServiceOp::MarkUsed(r) => (id_namespaces(&r.ids)?, "this mark-used"),
            ServiceOp::Forget(r) => (id_namespaces(&r.ids)?, "this forget"),
            _ => return Ok(()),
        };
        let parts = memory_partitions(routes, namespaces.iter().map(String::as_str))?;
        let owners: Vec<(String, String)> = parts
            .keys()
            .filter_map(|p| routes.owner(p).map(|o| (o.node.clone(), o.address.clone())))
            .collect();
        let down = cluster
            .node
            .owners_keep(owners.into_iter(), super::MEMORY_TOOL_FIELDS_FEATURE, what)
            .await?;
        if let Some(node) = down.first() {
            let p = parts
                .keys()
                .find(|p| routes.owner(p).is_some_and(|o| o.node == *node))
                .map_or("", String::as_str);
            return Err(ApiError::partition_unavailable(format!(
                "partition {p} isn't reachable: its node {node} didn't answer"
            )));
        }
        Ok(())
    }

    /// A memory call across partitions (#1915): a recall in steps, or a `MarkUsed` or
    /// `Forget` split by partition.
    pub(crate) async fn fan_memory(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<OwnedSemaphorePermit>,
        deadline: tokio::time::Instant,
    ) -> Result<ServiceReply, ApiError> {
        match op {
            ServiceOp::Recall(_) => {
                self.fan_recall(cluster, op, caller, prepaid, deadline)
                    .await
            }
            op => {
                drop(prepaid);
                self.split_memory_write(cluster, op, caller, deadline).await
            }
        }
    }

    /// `Memory.Recall` across partitions, in steps.
    async fn fan_recall(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<OwnedSemaphorePermit>,
        deadline: tokio::time::Instant,
    ) -> Result<ServiceReply, ApiError> {
        let request = self.service.admit(&op.admit(), caller, prepaid).await?;
        drop(request);
        let ServiceOp::Recall(mut r) = op else {
            return Err(ApiError::internal("fan_recall takes a recall"));
        };
        // The caller's namespaces, here and again on each partition.
        crate::memcore::Scope(caller.namespaces.as_deref()).check_all(&r.namespaces)?;
        let distinct: BTreeSet<&str> = r.namespaces.iter().map(String::as_str).collect();
        if distinct.len() > MAX_SPAN_NAMESPACES {
            return Err(ApiError::invalid(format!(
                "a recall across partitions names at most {MAX_SPAN_NAMESPACES} namespaces, \
                 not {}",
                distinct.len()
            )));
        }
        let max_k = crate::memcore::MAX_K;
        let k = r.k as usize;
        if k == 0 || k > max_k {
            return Err(ApiError::invalid(format!("k is 1 to {max_k}")));
        }
        let recipe = cluster.memory.as_ref().ok_or_else(|| {
            ApiError::unsupported("this node runs without --memory, so it serves no memory calls")
        })?;
        let (default, max) = recipe.budget;
        let budget = crate::memcore::recall_budget(r.byte_budget, default, max)?;
        r.byte_budget = budget;
        // The query is embedded once, here, for every partition. Without an embedder here,
        // each partition embeds the text itself (the guide asks for one backend per cluster).
        if r.embedding.is_none()
            && !r.text.trim().is_empty()
            && let Some(e) = recipe.embedder.clone()
        {
            let text = r.text.clone();
            let v = tokio::task::spawn_blocking(move || {
                e.embed(&text, crate::memcore::EmbedKind::Query)
            })
            .await
            .map_err(|e| ApiError::internal(format!("the embedding task failed: {e}")))??;
            r.embedding = Some(crate::serve::memory::vector_out(&v));
        }
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(1, |d| d.as_micros() as u64)
            .max(1);
        let reach = Reach::until(self, cluster, deadline);
        let routes = reach.routes()?;
        let parts = memory_partitions(&routes, r.namespaces.iter().map(String::as_str))?;
        let of: BTreeMap<String, String> = parts
            .iter()
            .flat_map(|(p, nss)| nss.iter().map(move |ns| (ns.clone(), p.clone())))
            .collect();
        let narrowed = |namespaces: &[String]| RecallRequest {
            namespaces: namespaces.to_vec(),
            ..r.clone()
        };
        // The request's holding, as a fan-out's (#1715): its reply's budget, plus one
        // step's answer; each further concurrent step runs on a spare share.
        let share = usize::try_from(budget).unwrap_or(usize::MAX);
        let holding = self
            .service
            .fanout_hold(share.saturating_mul(2), reach.within()?)
            .await?;
        // 1. The pools.
        let steps = parts
            .iter()
            .map(|(p, nss)| {
                let op = forward_request::Op::RecallPool(RecallPoolRequest {
                    request: Some(narrowed(nss)),
                    now_us,
                    hop: Default::default(),
                });
                (p.clone(), op)
            })
            .collect();
        let pools = self
            .recall_steps(&reach, caller, steps, share, |reply| match reply {
                ServiceReply::RecallPool(p) => {
                    RecallPool::from_bytes(&p.pool).map_err(|e| unreadable("pool", &e))
                }
                _ => Err(ApiError::internal("a recall's pool answered another reply")),
            })
            .await?;
        let mut ranking = RecallPool::merge(pools)
            .map_err(|e| lib(&e))?
            .rank(Fusion::default(), Weights::default());
        // 2. The graph expansion, hop by hop.
        if r.expand_hops > 0 {
            let mut walk = ranking.walk(Expand::hops(r.expand_hops), Some(budget));
            while let Some(hop) = walk.next_hop() {
                let steps = hop
                    .split(|ns| of.get(ns).cloned().unwrap_or_default())
                    .into_iter()
                    .map(|(p, share)| {
                        let nss = parts.get(&p).cloned().unwrap_or_default();
                        let op = forward_request::Op::RecallPool(RecallPoolRequest {
                            request: Some(narrowed(&nss)),
                            now_us,
                            hop: share.to_bytes().into(),
                        });
                        (p, op)
                    })
                    .collect();
                let got = self
                    .recall_steps(&reach, caller, steps, share, |reply| match reply {
                        ServiceReply::RecallPool(p) => {
                            RecallHopPart::from_bytes(&p.pool).map_err(|e| unreadable("hop", &e))
                        }
                        _ => Err(ApiError::internal("a recall's hop answered another reply")),
                    })
                    .await?;
                walk.settle(got).map_err(|e| lib(&e))?;
            }
            ranking = walk.finish();
        }
        // 3. The bodies of the best hits, and a few more.
        let mut by_part: BTreeMap<String, Vec<MemoryId>> = BTreeMap::new();
        for id in ranking.top(k + BACKFILL) {
            if let Some(p) = of.get(id.namespace()) {
                by_part.entry(p.clone()).or_default().push(id);
            }
        }
        let steps = by_part
            .into_iter()
            .map(|(p, ids)| {
                let nss = parts.get(&p).cloned().unwrap_or_default();
                let op = forward_request::Op::RecallBodies(RecallBodiesRequest {
                    request: Some(narrowed(&nss)),
                    ids: ids.iter().map(|id| id.to_bytes().into()).collect(),
                });
                (p, op)
            })
            .collect();
        let bodies = self
            .recall_steps(&reach, caller, steps, share, |reply| match reply {
                ServiceReply::RecallBodies(b) => {
                    RecallBodies::from_bytes(&b.bodies).map_err(|e| unreadable("bodies", &e))
                }
                _ => Err(ApiError::internal(
                    "a recall's bodies answered another reply",
                )),
            })
            .await?;
        // `truncated` is this node's: the merged ranking's budget cut, not a part's (the
        // parts send pools and bodies, never cut answers).
        let (hits, stats) =
            ranking.finish(RecallBodies::merge(bodies), k, Some(budget), r.with_signals);
        let reply = proto::RecallResponse {
            hits: hits.into_iter().map(crate::serve::memory::hit).collect(),
            truncated: stats.truncated,
        };
        self.service
            .fanout_reply(
                caller,
                ServiceReply::Recall(reply),
                holding,
                reach.within()?,
            )
            .await
    }

    /// One step of a spanning recall on each of its partitions, together (at most
    /// `--fanout-forwards` at once, each past the first on a spare `share`): every answer,
    /// read by `read`, in partition order. Any partition's failure fails the recall: there's
    /// no partial memory set (#1915's design).
    async fn recall_steps<T: Send>(
        &self,
        reach: &Reach,
        caller: &Caller,
        steps: Vec<(String, forward_request::Op)>,
        share: usize,
        read: impl Fn(ServiceReply) -> Result<T, ApiError>,
    ) -> Result<Vec<T>, ApiError> {
        let futs: Vec<Boxed<'_, Result<(ServiceReply, bool), ApiError>>> = steps
            .iter()
            .map(|(p, op)| {
                Box::pin(reach.part(p, caller, op.clone()))
                    as Boxed<'_, Result<(ServiceReply, bool), ApiError>>
            })
            .collect();
        let mut out: Vec<Option<T>> = (0..futs.len()).map(|_| None).collect();
        bounded(
            futs,
            self.service.limits().fanout_forwards,
            || self.service.fanout_try_hold(share),
            |i, answer| {
                out[i] = Some(read(answer?.0)?);
                Ok(())
            },
        )
        .await?;
        out.into_iter()
            .map(|t| t.ok_or_else(|| ApiError::internal("a recall's step didn't answer")))
            .collect()
    }

    /// `Memory.MarkUsed` or `Memory.Forget` across partitions: each partition's ids a call
    /// of its own (served here, or forwarded as a write), sent together, the counts summed.
    ///
    /// - **A part refused, or unreachable before its send:** the call fails with that
    ///   part's error.
    /// - **A part whose outcome is unknown** (a forward that failed after its send, or a
    ///   lease that ended under it): the call answers `OutcomeUnknown`, naming it.
    /// - **Either way,** the message names the partitions that applied, and their counts.
    ///   A retry of the whole call is safe: `Forget` skips what's gone, and `MarkUsed` is
    ///   advisory.
    async fn split_memory_write(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        deadline: tokio::time::Instant,
    ) -> Result<ServiceReply, ApiError> {
        let (ids, forget) = match &op {
            ServiceOp::MarkUsed(r) => (&r.ids, false),
            ServiceOp::Forget(r) => (&r.ids, true),
            _ => {
                return Err(ApiError::internal(
                    "split_memory_write takes MarkUsed or Forget",
                ));
            }
        };
        let routes: Arc<Routes> = cluster
            .node
            .routes()
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| ApiError::partition_unavailable("this cluster node has no map yet"))?;
        let mut by_part: BTreeMap<String, Vec<_>> = BTreeMap::new();
        for raw in ids {
            let id = crate::serve::memory::id(raw)?;
            let p = memory_partitions(&routes, [id.namespace()])?
                .into_keys()
                .next()
                .ok_or_else(|| ApiError::internal("a namespace with no partition"))?;
            by_part.entry(p).or_default().push(raw.clone());
        }
        let calls = by_part.into_iter().map(|(p, ids)| {
            let part = if forget {
                ServiceOp::Forget(ForgetRequest { ids })
            } else {
                ServiceOp::MarkUsed(MarkUsedRequest { ids })
            };
            async move { (p, self.memory_part(cluster, part, caller, deadline).await) }
        });
        let answers = join_all(calls).await;
        let mut total = 0u64;
        let mut missing = Vec::new();
        let mut applied = Vec::new();
        let mut unknown = Vec::new();
        let mut failed = None;
        for (p, answer) in answers {
            match answer {
                Ok(ServiceReply::Forget(f)) => {
                    total += f.forgotten;
                    missing.extend(f.missing);
                    applied.push(format!("{p} ({})", f.forgotten));
                }
                Ok(ServiceReply::MarkUsed(m)) => {
                    total += m.marked;
                    missing.extend(m.missing);
                    applied.push(format!("{p} ({})", m.marked));
                }
                Ok(_) => return Err(ApiError::internal("a memory write answered another reply")),
                Err(e) if e.code == "OutcomeUnknown" => unknown.push((p, e)),
                Err(e) => {
                    failed.get_or_insert((p, e));
                }
            }
        }
        let applied = if applied.is_empty() {
            "none".to_owned()
        } else {
            applied.join(", ")
        };
        if let Some((p, e)) = unknown.into_iter().next() {
            return Err(ApiError::outcome_unknown(format!(
                "{} on partition {p} may or may not have applied ({}); applied on: {applied}. \
                 Retrying the whole call is safe",
                op.name(),
                e.message
            )));
        }
        if let Some((p, mut e)) = failed {
            e.message = format!(
                "{} on partition {p}: {}; applied on: {applied}",
                op.name(),
                e.message
            );
            return Err(e);
        }
        Ok(if forget {
            ServiceReply::Forget(proto::ForgetResponse {
                forgotten: total,
                missing,
            })
        } else {
            ServiceReply::MarkUsed(proto::MarkUsedResponse {
                marked: total,
                missing,
            })
        })
    }

    /// One partition's part of a split memory write, as `dispatch` sends a call on one
    /// partition: served here, or forwarded (a write, so a failure after the send is
    /// `OutcomeUnknown`).
    async fn memory_part(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        deadline: tokio::time::Instant,
    ) -> Result<ServiceReply, ApiError> {
        match cluster.target(&op)? {
            Place::Here(service) => service.call(op, caller, None).await,
            Place::There(owner) => {
                Box::pin(self.forward(cluster, op, caller, None, owner, deadline)).await
            }
        }
    }
}

/// The namespaces of a call's ids.
fn id_namespaces(ids: &[prost::bytes::Bytes]) -> Result<Vec<String>, ApiError> {
    ids.iter()
        .map(|raw| crate::serve::memory::id(raw).map(|id| id.namespace().to_owned()))
        .collect()
}

/// Runs `futs` together, each answer in its future's order.
async fn join_all<F: std::future::Future>(futs: impl IntoIterator<Item = F>) -> Vec<F::Output> {
    let mut futs: Vec<_> = futs.into_iter().map(Box::pin).collect();
    let mut out: Vec<Option<F::Output>> = (0..futs.len()).map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for (i, f) in futs.iter_mut().enumerate() {
            if out[i].is_none() {
                match f.as_mut().poll(cx) {
                    std::task::Poll::Ready(v) => out[i] = Some(v),
                    std::task::Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(())
        }
    })
    .await;
    out.into_iter()
        .map(|o| o.expect("every future ran"))
        .collect()
}
