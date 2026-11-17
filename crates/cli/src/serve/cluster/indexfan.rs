//! Calls across a vector or text index's partitions (#1722 PR 1b, ICR 0138).
//!
//! - **`Vectors.Search`** goes to each partition whose range meets `id_prefix` (all of them
//!   without one), with the client's `k`, `nprobe` and `max_probe` applied per partition,
//!   and the entry node keeps the first `k` by `(distance, id)`. Each partition trained its
//!   own centroids, so this isn't one index's answer: `nprobe` means "per partition", and
//!   recall and cost rise with the partitions (perf287's g108). A partition that can't be
//!   reached fails the search (`PartitionUnavailable`); there's no `allow_partial` in v1.
//!   A part's error names its partition.
//! - **`Text.Search`** runs in two rounds (#1722 PR 2b), so its scores are the ones one
//!   index holding every partition's documents would give. Round 1 asks every partition
//!   of the index for its statistics for the query (`QueryStats`, at one snapshot each),
//!   whatever `id_prefix` says, and the entry node merges them. Round 2 asks each
//!   partition `id_prefix` meets for its top `k`, scored with the merged statistics, and
//!   the entry node keeps the first `k` by `(score desc, id asc)`, the library's order.
//!   An index on one partition skips round 1: its own statistics are the whole index's.
//!   Strict by default: a partition that can't answer either round fails the search. With
//!   `allow_partial`, a partition that's unreachable or still filling is skipped, and
//!   `partial` is set: missing from round 1, the search scores with the statistics of
//!   those that answered; missing from round 2 (or round 1, when `id_prefix` meets it),
//!   its ranges are named in `missing`.
//! - **`Vectors.Verify` and `Text.Verify`** go to every partition and sum the counts. A
//!   partition whose check fails fails the call, naming it.
//! - **`Text.Rebuild`** (#1722 PR 2c) goes to every partition, each rebuilding its own
//!   part of the index as a single node does, and sums the counts. A node rebuilds one
//!   text index at a time, so the partitions one node serves go one after another, and the
//!   nodes at once. A partition whose rebuild fails fails the call, naming it; the
//!   partitions rebuilt by then stand, and a rebuild already running goes on (a write
//!   finishes if its caller goes away). Rebuilding again is safe.
//!
//! Each part names its partition (`ForwardRequest.partition`): the request names only the
//! index, and one node can own several of its partitions.

use std::collections::BTreeSet;

use super::fanout::{Boxed, Reach, bounded, missing, skippable};
use super::route::{Routes, prefix_end};
use crate::serve::auth::Caller;
use crate::serve::errors::ApiError;
use crate::serve::proto::{self, forward_request};
use crate::serve::router::{Cluster, Router};
use crate::serve::service::{MAX_NEAREST_K, ServiceOp, ServiceReply, page_bytes, unreadable_stats};
use pigeonhole_text::QueryStats;
use tokio::sync::OwnedSemaphorePermit;

/// What a verify's part answer holds of the in-flight budget: a handful of counts.
const VERIFY_SHARE: usize = 1024;

/// What a text search's round 1 answer holds of the in-flight budget: its statistics, a
/// count per term plus each fuzzy term's expansions. An estimate: serve searches with the
/// library's `max_expansions` (50, not a request field), so a fuzzy term's expansions are
/// a few KiB, and the query's size bounds its terms.
const STATS_SHARE: usize = 64 * 1024;

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

/// `e`, its message naming the partitions of a node's rebuild left after it, which never
/// started (simp724's N1 on #2041).
fn unstarted(mut e: ApiError, rest: &[String]) -> ApiError {
    if !rest.is_empty() {
        e.message = format!(
            "{}; not started on its node: {}",
            e.message,
            rest.join(", ")
        );
    }
    e
}

/// `e`, its message naming `partition`.
fn on(partition: &str, mut e: ApiError) -> ApiError {
    e.message = format!("partition {partition}: {}", e.message);
    e
}

/// `kept` and `more` merged by `(score desc, id asc)`, a text search's order
/// (`pigeonhole_text`'s), the first `k` kept. Scores are never NaN (a BM25 sum of finite
/// terms); `total_cmp` orders them anyway.
fn merge_text_hits(
    mut kept: Vec<proto::TextHit>,
    more: Vec<proto::TextHit>,
    k: usize,
) -> Vec<proto::TextHit> {
    kept.extend(more);
    kept.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
    kept.truncate(k);
    kept
}

/// Whether a text search's part that failed with `e` is skipped with `allow_partial`: an
/// unreachable partition, or one whose index is still filling (`Unavailable`).
fn skippable_text(e: &ApiError) -> bool {
    skippable(e) || e.code == "Unavailable"
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
            ServiceOp::TextSearch(r) => Box::pin(self.fan_text_search(&reach, r, caller)).await,
            ServiceOp::TextRebuild(r) => Box::pin(self.fan_text_rebuild(&reach, r, caller)).await,
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

    /// `Text.Search` across the index's partitions, in two rounds (the module's docs).
    async fn fan_text_search(
        &self,
        reach: &Reach,
        r: proto::TextSearchRequest,
        caller: &Caller,
    ) -> Result<ServiceReply, ApiError> {
        // Checked once here, rather than refused by every partition.
        if r.k == 0 || r.k > MAX_NEAREST_K {
            return Err(ApiError::invalid(format!("k is 1 to {MAX_NEAREST_K}")));
        }
        crate::serve::texts::query(&r)?;
        let routes = reach.routes()?;
        let all = partitions(&routes, &r.index, b"")?;
        let reached = partitions(&routes, &r.index, &r.id_prefix)?;
        // A placement lists its partitions (validated), and a prefix meets at least one.
        debug_assert!(!reached.is_empty(), "index {:?} has no partitions", r.index);
        // The ranges of a partition `id_prefix` meets, for `missing`.
        let lo = r.id_prefix.to_vec();
        let hi = prefix_end(&lo);
        let ranges = routes.index_ranges(&r.index).unwrap_or_default();
        let missed = |p: &str| -> Vec<proto::MissingRange> {
            ranges
                .iter()
                .filter(|q| q.partition == p && q.meets(&lo, hi.as_deref()))
                .map(|q| missing(&r.index, q))
                .collect()
        };
        // The parts carry the search as the client sent it, less `allow_partial`, which
        // only the entry node acts on.
        let search = proto::TextSearchRequest {
            allow_partial: false,
            ..r.clone()
        };
        // The request's holding (#1715): the reply, and one part's answer, each at most `k`
        // hits, estimated as a default read page (the reply's charge is then fitted to its
        // size). Each further concurrent part runs on a spare share.
        let share = usize::try_from(page_bytes(0)).unwrap_or(usize::MAX);
        let holding = self
            .service
            .fanout_hold(share.saturating_mul(2), reach.within()?)
            .await?;
        // Round 1: every partition's statistics, merged. An index on one partition skips
        // it, its part reading its own.
        let mut down: BTreeSet<String> = BTreeSet::new();
        let stats: prost::bytes::Bytes = if all.len() == 1 {
            prost::bytes::Bytes::new()
        } else {
            let futs: Vec<Boxed<'_, Result<(ServiceReply, bool), ApiError>>> = all
                .iter()
                .map(|p| {
                    let op = forward_request::Op::TextStats(proto::TextStatsRequest {
                        search: Some(search.clone()),
                    });
                    Box::pin(reach.part(p, caller, op)) as Boxed<'_, _>
                })
                .collect();
            let mut parts: Vec<QueryStats> = Vec::with_capacity(all.len());
            bounded(
                futs,
                self.service.limits().fanout_forwards,
                || self.service.fanout_try_hold(STATS_SHARE),
                |i, answer| match answer {
                    Ok((ServiceReply::TextStats(a), _)) => {
                        parts.push(
                            unreadable_stats(QueryStats::from_bytes(&a.stats))
                                .map_err(|e| on(&all[i], e))?,
                        );
                        Ok(())
                    }
                    Ok(_) => Err(ApiError::internal(
                        "a text search's statistics answered another reply",
                    )),
                    Err(e) if r.allow_partial && skippable_text(&e) => {
                        down.insert(all[i].clone());
                        Ok(())
                    }
                    Err(e) => Err(on(&all[i], e)),
                },
            )
            .await?;
            if parts.is_empty() {
                // With `allow_partial`, and no partition answered: nothing to score with.
                let reply = proto::TextSearchResponse {
                    partial: true,
                    missing: reached.iter().flat_map(|p| missed(p)).collect(),
                    ..Default::default()
                };
                return self
                    .service
                    .fanout_reply(
                        caller,
                        ServiceReply::TextSearch(reply),
                        holding,
                        reach.within()?,
                    )
                    .await;
            }
            // Each part was read for the same search, so they merge; bytes from another
            // build that read but don't merge are that build's difference.
            QueryStats::merge(parts)
                .map_err(|e| {
                    ApiError::failed_precondition(format!(
                        "the partitions' statistics for a text search don't merge: {e}"
                    ))
                })?
                .to_bytes()
                .into()
        };
        // Round 2: the top `k` of each partition `id_prefix` meets and round 1 didn't miss.
        let mut gone: Vec<proto::MissingRange> = Vec::new();
        let asked: Vec<&String> = reached
            .iter()
            .filter(|p| {
                let skip = down.contains(*p);
                if skip {
                    gone.extend(missed(p));
                }
                !skip
            })
            .collect();
        let futs: Vec<Boxed<'_, Result<(ServiceReply, bool), ApiError>>> = asked
            .iter()
            .map(|p| {
                let op = forward_request::Op::TextSearch(proto::TextSearchPartRequest {
                    search: Some(search.clone()),
                    stats: stats.clone(),
                });
                Box::pin(reach.part(p, caller, op)) as Boxed<'_, _>
            })
            .collect();
        let k = r.k as usize;
        let mut hits: Vec<proto::TextHit> = Vec::new();
        // Every part scores with the same statistics, so each names the same cut clauses.
        let mut cut: BTreeSet<u32> = BTreeSet::new();
        bounded(
            futs,
            self.service.limits().fanout_forwards,
            || self.service.fanout_try_hold(share),
            |i, answer| match answer {
                Ok((ServiceReply::TextSearch(a), _)) => {
                    cut.extend(a.cut_clauses);
                    hits = merge_text_hits(std::mem::take(&mut hits), a.hits, k);
                    Ok(())
                }
                Ok(_) => Err(ApiError::internal("a text search answered another reply")),
                Err(e) if r.allow_partial && skippable_text(&e) => {
                    gone.extend(missed(asked[i]));
                    Ok(())
                }
                Err(e) => Err(on(asked[i], e)),
            },
        )
        .await?;
        // Short in either way (audit120's should on #2029): a client checking `partial`
        // alone never takes a short answer as whole.
        let reply = proto::TextSearchResponse {
            hits,
            partial: !down.is_empty() || !gone.is_empty(),
            missing: gone,
            cut_clauses: cut.into_iter().collect(),
        };
        self.service
            .fanout_reply(
                caller,
                ServiceReply::TextSearch(reply),
                holding,
                reach.within()?,
            )
            .await
    }

    /// `Text.Rebuild` on every partition of the index, the counts summed (#1722 PR 2c). A
    /// node's one rebuild slot answers a second client's rebuild `Overloaded`, so each
    /// node's partitions go in turn, in the map's order, and the nodes at once.
    async fn fan_text_rebuild(
        &self,
        reach: &Reach,
        r: proto::TextIndexRequest,
        caller: &Caller,
    ) -> Result<ServiceReply, ApiError> {
        let routes = reach.routes()?;
        let mut nodes: Vec<(&str, Vec<String>)> = Vec::new();
        for p in partitions(&routes, &r.index, b"")? {
            // A partition the map doesn't serve goes on its own: its part fails, naming it.
            let node = routes.owner(&p).map_or("", |o| o.node.as_str());
            match nodes.iter_mut().find(|(n, _)| !n.is_empty() && *n == node) {
                Some((_, ps)) => ps.push(p),
                None => nodes.push((node, vec![p])),
            }
        }
        let holding = self
            .service
            .fanout_hold(VERIFY_SHARE.saturating_mul(2), reach.within()?)
            .await?;
        let op = forward_request::Op::TextRebuild(r);
        let futs: Vec<Boxed<'_, Result<proto::TextRebuildResponse, ApiError>>> = nodes
            .iter()
            .map(|(_, ps)| {
                let op = &op;
                Box::pin(async move {
                    let mut sum = proto::TextRebuildResponse::default();
                    for (i, p) in ps.iter().enumerate() {
                        // Past the deadline before it starts, this part and the rest don't.
                        let left = reach.within().map_err(|e| unstarted(e, &ps[i..]))?;
                        let named = |e| unstarted(on(p, e), &ps[i + 1..]);
                        // The same deadline for a part served here as for a forwarded one,
                        // whose rebuild is detached and goes on as a remote one does
                        // (simp724's S1 on #2041).
                        let answer = tokio::time::timeout(left, reach.part(p, caller, op.clone()))
                            .await
                            .map_err(|_| {
                                named(ApiError::outcome_unknown(
                                    "no answer within the request's deadline; the rebuild goes on",
                                ))
                            })?
                            .map_err(named)?;
                        match answer.0 {
                            ServiceReply::TextRebuild(a) => {
                                sum.documents += a.documents;
                                sum.changed += a.changed;
                                sum.invalid_utf8 += a.invalid_utf8;
                            }
                            _ => {
                                return Err(ApiError::internal(
                                    "a text index's rebuild answered another reply",
                                ));
                            }
                        }
                    }
                    Ok(sum)
                }) as Boxed<'_, _>
            })
            .collect();
        let mut total = proto::TextRebuildResponse::default();
        bounded(
            futs,
            self.service.limits().fanout_forwards,
            || self.service.fanout_try_hold(VERIFY_SHARE),
            |_, answer| {
                let a = answer?;
                total.documents += a.documents;
                total.changed += a.changed;
                total.invalid_utf8 += a.invalid_utf8;
                Ok(())
            },
        )
        .await?;
        self.service
            .fanout_reply(
                caller,
                ServiceReply::TextRebuild(total),
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
                    "fan_index takes an index's search, check or rebuild",
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
