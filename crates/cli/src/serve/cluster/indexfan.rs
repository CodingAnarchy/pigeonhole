//! Calls across a vector or text index's partitions (#1722 PR 1b, ICR 0138).
//!
//! - **`Vectors.Search`** goes to each partition whose range meets `id_prefix` (all of them
//!   without one), with the client's `k`, `nprobe` and `max_probe` applied per partition,
//!   and the entry node keeps the first `k` by `(distance, id)`. Each partition trained its
//!   own centroids, so this isn't one index's answer: `nprobe` means "per partition", and
//!   recall and cost rise with the partitions (perf287's g108). A partition that can't be
//!   reached fails the search (`PartitionUnavailable`); there's no `allow_partial` in v1.
//!   A part's error names its partition.
//! - **`Vectors.Verify` and `Text.Verify`** go to every partition and sum the counts. A
//!   partition whose check fails fails the call, naming it.
//!
//! Each part names its partition (`ForwardRequest.partition`): the request names only the
//! index, and one node can own several of its partitions.

use super::fanout::{Boxed, Reach, bounded};
use super::route::Routes;
use crate::serve::auth::Caller;
use crate::serve::errors::ApiError;
use crate::serve::proto::{self, forward_request};
use crate::serve::router::{Cluster, Router};
use crate::serve::service::{MAX_NEAREST_K, ServiceOp, ServiceReply, page_bytes};
use tokio::sync::OwnedSemaphorePermit;

/// What a verify's part answer holds of the in-flight budget: a handful of counts.
const VERIFY_SHARE: usize = 1024;

/// `index`'s partitions holding ids that start with `prefix`, or `TableNotFound` for an
/// index the map doesn't place (as a write to it answers).
fn partitions(routes: &Routes, index: &str, prefix: &[u8]) -> Result<Vec<String>, ApiError> {
    routes
        .index_partitions(index, prefix)
        .map(|ps| ps.into_iter().map(str::to_owned).collect())
        .ok_or_else(|| {
            ApiError::from_code(
                pigeonhole::ErrorCode::TableNotFound,
                &format!("index {index:?} isn't in the cluster map"),
            )
        })
}

/// `e`, its message naming `partition`.
fn on(partition: &str, mut e: ApiError) -> ApiError {
    e.message = format!("partition {partition}: {}", e.message);
    e
}

/// `kept` and `more` merged by `(distance, id)`, the first `k` kept. Distances are never
/// NaN (the query is checked, stored vectors are finite); `total_cmp` orders them anyway.
fn merge_hits(
    mut kept: Vec<proto::VectorHit>,
    more: Vec<proto::VectorHit>,
    k: usize,
) -> Vec<proto::VectorHit> {
    kept.extend(more);
    kept.sort_by(|a, b| {
        a.distance
            .total_cmp(&b.distance)
            .then_with(|| a.id.cmp(&b.id))
    });
    kept.truncate(k);
    kept
}

impl Router {
    /// A search or check across an index's partitions (#1722 PR 1b).
    pub(crate) async fn fan_index(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<OwnedSemaphorePermit>,
        deadline: tokio::time::Instant,
    ) -> Result<ServiceReply, ApiError> {
        let request = self.service.admit(&op.admit(), caller, prepaid).await?;
        drop(request);
        let reach = Reach::until(self, cluster, deadline);
        match op {
            ServiceOp::VectorSearch(r) => self.fan_vector_search(&reach, r, caller).await,
            op => self.fan_verify(&reach, op, caller).await,
        }
    }

    /// `Vectors.Search` across the partitions `id_prefix` meets: each partition's top `k`,
    /// merged by `(distance, id)`. Ties at the cut are each partition's (list, then id), so
    /// across partitions they're ordered by id among the hits returned, and nothing more.
    async fn fan_vector_search(
        &self,
        reach: &Reach,
        r: proto::VectorSearchRequest,
        caller: &Caller,
    ) -> Result<ServiceReply, ApiError> {
        // Checked once here, rather than refused by every partition.
        if r.k == 0 || r.k > MAX_NEAREST_K {
            return Err(ApiError::invalid(format!("k is 1 to {MAX_NEAREST_K}")));
        }
        if r.query.is_none() {
            return Err(ApiError::invalid("a search needs a query vector"));
        }
        let routes = reach.routes()?;
        let parts = partitions(&routes, &r.index, &r.id_prefix)?;
        // A placement lists its partitions (validated), and a prefix meets at least one.
        debug_assert!(!parts.is_empty(), "index {:?} has no partitions", r.index);
        // The request's holding (#1715): the reply, and one part's answer, each at most `k`
        // hits, estimated as a default read page (the reply's charge is then fitted to its
        // size). Each further concurrent part runs on a spare share.
        let share = usize::try_from(page_bytes(0)).unwrap_or(usize::MAX);
        let holding = self
            .service
            .fanout_hold(share.saturating_mul(2), reach.within()?)
            .await?;
        let futs: Vec<Boxed<'_, Result<(ServiceReply, bool), ApiError>>> = parts
            .iter()
            .map(|p| {
                let op = forward_request::Op::VectorSearch(r.clone());
                Box::pin(reach.part(p, caller, op)) as Boxed<'_, _>
            })
            .collect();
        let k = r.k as usize;
        let mut hits: Vec<proto::VectorHit> = Vec::new();
        let mut stats = proto::VectorSearchStats::default();
        bounded(
            futs,
            self.service.limits().fanout_forwards,
            || self.service.fanout_try_hold(share),
            |i, answer| match answer.map_err(|e| on(&parts[i], e))?.0 {
                ServiceReply::VectorSearch(a) => {
                    if let Some(s) = a.stats {
                        stats.lists_probed += s.lists_probed;
                        stats.lists += s.lists;
                    }
                    hits = merge_hits(std::mem::take(&mut hits), a.hits, k);
                    Ok(())
                }
                _ => Err(ApiError::internal("a vector search answered another reply")),
            },
        )
        .await?;
        let reply = proto::VectorSearchResponse {
            hits,
            stats: r.stats.then_some(stats),
        };
        self.service
            .fanout_reply(
                caller,
                ServiceReply::VectorSearch(reply),
                holding,
                reach.within()?,
            )
            .await
    }

    /// `Vectors.Verify` or `Text.Verify` on every partition of the index, the counts summed.
    async fn fan_verify(
        &self,
        reach: &Reach,
        op: ServiceOp,
        caller: &Caller,
    ) -> Result<ServiceReply, ApiError> {
        let (index, part) = match op {
            ServiceOp::VectorVerify(r) => (r.index.clone(), forward_request::Op::VectorVerify(r)),
            ServiceOp::TextVerify(r) => (r.index.clone(), forward_request::Op::TextVerify(r)),
            _ => {
                return Err(ApiError::internal(
                    "fan_index takes an index's search or check",
                ));
            }
        };
        let routes = reach.routes()?;
        let parts = partitions(&routes, &index, b"")?;
        // A placement lists its partitions (validated), so every check has a part.
        debug_assert!(!parts.is_empty(), "index {index:?} has no partitions");
        let vector_kind = matches!(part, forward_request::Op::VectorVerify(_));
        let holding = self
            .service
            .fanout_hold(VERIFY_SHARE.saturating_mul(2), reach.within()?)
            .await?;
        let futs: Vec<Boxed<'_, Result<(ServiceReply, bool), ApiError>>> = parts
            .iter()
            .map(|p| Box::pin(reach.part(p, caller, part.clone())) as Boxed<'_, _>)
            .collect();
        let mut vector = proto::VectorVerifyResponse::default();
        let mut text = proto::TextVerifyResponse::default();
        bounded(
            futs,
            self.service.limits().fanout_forwards,
            || self.service.fanout_try_hold(VERIFY_SHARE),
            |i, answer| match answer.map_err(|e| on(&parts[i], e))?.0 {
                // Each part answers the kind asked, or the call fails (simp724's N1 on #2016).
                ServiceReply::VectorVerify(a) if vector_kind => {
                    vector.vectors += a.vectors;
                    vector.lists += a.lists;
                    Ok(())
                }
                ServiceReply::TextVerify(a) if !vector_kind => {
                    text.documents += a.documents;
                    text.terms += a.terms;
                    text.postings += a.postings;
                    text.forward_mismatch += a.forward_mismatch;
                    text.source_mismatch += a.source_mismatch;
                    Ok(())
                }
                _ => Err(ApiError::internal("an index check answered another reply")),
            },
        )
        .await?;
        let reply = if vector_kind {
            ServiceReply::VectorVerify(vector)
        } else {
            ServiceReply::TextVerify(text)
        };
        self.service
            .fanout_reply(caller, reply, holding, reach.within()?)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(id: &str, distance: f64) -> proto::VectorHit {
        proto::VectorHit {
            id: id.as_bytes().to_vec().into(),
            distance,
        }
    }

    #[test]
    fn hits_merge_by_distance_then_id_to_the_first_k() {
        let a = vec![hit("b", 0.1), hit("d", 0.5)];
        let b = vec![hit("a", 0.1), hit("c", 0.3), hit("e", 0.9)];
        let got = merge_hits(merge_hits(Vec::new(), a, 3), b, 3);
        let ids: Vec<&[u8]> = got.iter().map(|h| &h.id[..]).collect();
        assert_eq!(ids, [&b"a"[..], b"b", b"c"]);
    }
}
