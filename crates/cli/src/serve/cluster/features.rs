use super::*;

/// The map fields this build keeps when it decodes and stores a map (`NodeStatus.map_features`,
/// ICR 0138 and 0139). A map's writer adds such a field only once every node reports it: a
/// node that drops it would store a different map at the same version, and read as divergent.
/// Request features its peers may rely on are listed here too (#1915 PR 3, ICR 0139's PR 3
/// section): [`NEAREST_QUALIFIERS_FEATURE`] and [`MEMORY_TOOL_FIELDS_FEATURE`].
pub(crate) const MAP_FEATURES: &[&str] = &[
    MEMORY_FEATURE,
    INDEXES_FEATURE,
    NEAREST_QUALIFIERS_FEATURE,
    MEMORY_TOOL_FIELDS_FEATURE,
    REPLICAS_FEATURE,
    INDEX_BULK_LOAD_FEATURE,
    LEASES_FEATURE,
    WRITER_TERMS_FEATURE,
    RANGE_MOVES_FEATURE,
    MOVE_ABORTING_FEATURE,
    spanning::SPANNING_TXNS_FEATURE,
    BALANCER_FEATURE,
    REPLICA_ACKS_FEATURE,
    REPLICA_ACK_WAIT_FEATURE,
];

/// [`MAP_FEATURES`]' name for D335 (ICR 0146): a build that serves
/// `Replication.FollowAcked`, so its replica copies acknowledge what they applied.
pub(crate) const REPLICA_ACKS_FEATURE: &str = "replica_acks";

/// [`MAP_FEATURES`]' name for D335 PR 2's map field, `Partition.replica_acks`: a node
/// without it would drop the field and read as divergent, so a map sets it only once every
/// node reports it.
pub(crate) const REPLICA_ACK_WAIT_FEATURE: &str = "replica_ack_wait";

/// Whether `map` sets any partition's `replica_acks` (D335): what needs
/// [`REPLICA_ACK_WAIT_FEATURE`] on every node.
pub(crate) fn uses_replica_acks(map: &ClusterMap) -> bool {
    map.partitions.iter().any(|p| p.replica_acks > 0)
}

/// [`MAP_FEATURES`]' name for D332's map fields (`ClusterMap.balancer`, `.balance`,
/// `.balancer_events`, `Partition.pinned`, `.moves_history`, `TablePlacement.pinned_ranges`,
/// `Node.weight`): a node without it would drop them from its copy and read as divergent.
pub(crate) const BALANCER_FEATURE: &str = "balancer";

/// [`MAP_FEATURES`]' name for `ClusterMap.moves` (D331): a node without it would drop the
/// field from its copy, read as divergent, and gate no moving range.
pub(crate) const RANGE_MOVES_FEATURE: &str = "range_moves";

/// [`MAP_FEATURES`]' name for `MOVE_PHASE_ABORTING` (D331 PR 5): a node without it refuses a
/// map holding the phase. Every range move needs it on every node before it starts, so its
/// abort can always be written.
pub(crate) const MOVE_ABORTING_FEATURE: &str = "move_aborting";

/// [`MAP_FEATURES`]' name for a build that serves `Cluster.Lease` (D326 amendment 1, #2048):
/// a peer whose `Lease` answers `Unimplemented` counts as granting only if its `Status`
/// lacks it (a 1.2 node), never when it reports it (a 1.3 node behind something that doesn't
/// route the call, audit120's S1).
pub(crate) const LEASES_FEATURE: &str = "leases";

/// [`MAP_FEATURES`]' name for `ClusterMap.memory` (#1915).
pub(crate) const MEMORY_FEATURE: &str = "memory";

/// [`MAP_FEATURES`]' name for `ClusterMap.indexes` (#1722, ICR 0138): a node without it
/// would drop the field from the map it stores, and read as divergent.
pub(crate) const INDEXES_FEATURE: &str = "indexes";

/// [`MAP_FEATURES`]' name for a build that serves `Cluster.ForwardBulkLoad` (#1722 PR 3, ICR
/// 0138): an entry node checks every node holding a part of the index for it before a
/// `Vectors.BulkLoad` reads anything, so an older one refuses the load up front, not
/// part way.
pub(crate) const INDEX_BULK_LOAD_FEATURE: &str = "index_bulk_load";

/// [`MAP_FEATURES`]' name for a build that applies `NearestRequest.qualifiers` (#1915 PR 3):
/// a node without it would rank every qualifier.
pub(crate) const NEAREST_QUALIFIERS_FEATURE: &str = "nearest_qualifiers";

/// [`MAP_FEATURES`]' name for a build that serves the memory calls' fields for `/mcp`'s
/// memory tools (#1915 PR 3b): `RememberRequest.supersedes`, `RememberResponse.
/// supersede_error`, `RecallResponse.truncated`, `MemoryHit.superseded_by` and the
/// `missing` ids of `MarkUsed` and `Forget`. A node without it would ignore `supersedes`
/// and leave the rest empty.
pub(crate) const MEMORY_TOOL_FIELDS_FEATURE: &str = "memory_tool_fields";

/// How long a peer's answered features ([`Node::owners_keep`]) are taken as its own: a
/// node's build changes only when it restarts.
const PEER_FEATURES_TTL: std::time::Duration = std::time::Duration::from_secs(30);

/// How long a peer that didn't answer the features check is taken as unavailable without
/// asking again: so searches while it's down don't each wait a push timeout for it, yet one
/// back up serves again soon.
const PEER_DOWN_TTL: std::time::Duration = std::time::Duration::from_secs(5);

/// A peer's features as it last answered `Status`, and when (`None`: it didn't answer).
pub(super) type PeerFeatures = (std::time::Instant, Option<Vec<String>>);

/// What a peer said about a feature ([`Node::features_of`]).
pub(crate) enum PeerFeature {
    Has,
    /// It answered without it: an older build.
    Lacks,
    /// It didn't answer (in time): why.
    NoAnswer(String),
}

/// [`MAP_FEATURES`]' name for `Partition.replicas` (D328, #1851).
pub(crate) const REPLICAS_FEATURE: &str = "replicas";

/// Whether `map` places a read replica of any partition (D328).
pub(crate) fn has_replicas(map: &ClusterMap) -> bool {
    map.partitions.iter().any(|p| !p.replicas.is_empty())
}

/// [`MAP_FEATURES`]' name for `ClusterMap.writer`, `.writer_term` and `.failover` (D330): a
/// node without it would drop them from its copy and read as divergent, so a map sets them
/// only once every node reports it.
pub(crate) const WRITER_TERMS_FEATURE: &str = "writer_terms";
impl Node {
    /// `FailedPrecondition` unless every node `map` lists keeps its field `feature`
    /// ([`MAP_FEATURES`]), asked through `Status`: a node that would drop the field stores
    /// another map at the same version, reads as divergent and blocks confirms (#1899's
    /// rule), so `what` is added only once the whole cluster runs a build that keeps it. A
    /// node that doesn't answer counts as one that doesn't: the refusal names it, and a
    /// forced map skips this as it skips the majority.
    pub(super) async fn every_node_keeps(
        self: &Arc<Self>,
        map: &ClusterMap,
        feature: &str,
        what: &str,
    ) -> Result<(), ApiError> {
        // Every node at once, as a confirm round asks them (simp724's n2 on #1920): k nodes
        // that don't answer cost one push timeout, not k.
        let peers = map
            .nodes
            .iter()
            .filter(|n| n.id != self.id)
            .map(|n| (n.id.clone(), n.address.clone()));
        let lacking = self.lacking(peers, feature).await;
        if lacking.is_empty() {
            Ok(())
        } else {
            Err(ApiError::failed_precondition(format!(
                "the map can't add {what} until every node runs a build that keeps it in its \
                 map, or that node would read as divergent: {}. Finish the upgrade first",
                lacking.join(", ")
            )))
        }
    }

    /// The peers of `peers` (id, address) that don't report `feature` in `Status`, each
    /// named with why: an older build, or no answer (in time). Every peer asked at once, as
    /// a confirm round asks them (simp724's n2 on #1920).
    async fn lacking(
        self: &Arc<Self>,
        peers: impl Iterator<Item = (String, String)>,
        feature: &str,
    ) -> Vec<String> {
        let mut lacking: Vec<String> = self
            .features_of(peers, feature, false)
            .await
            .into_iter()
            .filter_map(|(id, f)| match f {
                PeerFeature::Has => None,
                PeerFeature::Lacks => Some(format!("{id} (an older build)")),
                PeerFeature::NoAnswer(why) => Some(format!("{id} ({why})")),
            })
            .collect();
        lacking.sort();
        lacking
    }

    /// What each peer of `peers` (id, address) says about `feature` in `Status`, asked all
    /// at once. With `cached`, an answer from the last [`PEER_FEATURES_TTL`] stands, and a
    /// no-answer from the last [`PEER_DOWN_TTL`].
    pub(crate) async fn features_of(
        self: &Arc<Self>,
        peers: impl Iterator<Item = (String, String)>,
        feature: &str,
        cached: bool,
    ) -> Vec<(String, PeerFeature)> {
        let verdict = |fs: &Option<Vec<String>>| match fs {
            Some(fs) if fs.iter().any(|f| f == feature) => PeerFeature::Has,
            Some(_) => PeerFeature::Lacks,
            None => PeerFeature::NoAnswer("no answer lately".into()),
        };
        let mut asks = tokio::task::JoinSet::new();
        let mut out = Vec::new();
        for (id, address) in peers {
            if cached {
                let known = self
                    .peer_features
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(&id)
                    .filter(|(at, fs)| {
                        let ttl = if fs.is_some() {
                            PEER_FEATURES_TTL
                        } else {
                            PEER_DOWN_TTL
                        };
                        at.elapsed() < ttl
                    })
                    .map(|(_, fs)| verdict(fs));
                if let Some(v) = known {
                    out.push((id, v));
                    continue;
                }
            }
            let node = Arc::clone(self);
            let feature = feature.to_owned();
            asks.spawn(async move {
                let answer = async {
                    let mut client = node.client(&address).await?;
                    let req = node.request(proto::StatusRequest::default())?;
                    client
                        .status(req)
                        .await
                        .map(tonic::Response::into_inner)
                        .map_err(|s| s.message().to_owned())
                };
                let (seen, v) = match tokio::time::timeout(node.peers.push_timeout, answer).await {
                    Ok(Ok(st)) => {
                        let v = if st.map_features.contains(&feature) {
                            PeerFeature::Has
                        } else {
                            PeerFeature::Lacks
                        };
                        (Some(st.map_features), v)
                    }
                    Ok(Err(e)) => (None, PeerFeature::NoAnswer(format!("no answer: {e}"))),
                    Err(_) => (None, PeerFeature::NoAnswer("no answer in time".into())),
                };
                node.peer_features
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(id.clone(), (std::time::Instant::now(), seen));
                (id, v)
            });
        }
        out.extend(asks.join_all().await);
        out
    }

    /// Whether every node among `owners` (other than this one) applies request feature
    /// `feature`, which `what` needs (#1915 PR 3): `FailedPrecondition` naming each that
    /// answers without it (a build before it would answer its part without it), else the
    /// nodes that didn't answer, which the caller treats as unavailable (the coordinator's
    /// call). Answers are cached per peer ([`Node::features_of`]), so a node that's down
    /// doesn't cost each request a push timeout.
    pub(crate) async fn owners_keep(
        self: &Arc<Self>,
        owners: impl Iterator<Item = (String, String)>,
        feature: &str,
        what: &str,
    ) -> Result<Vec<String>, ApiError> {
        let mut seen = std::collections::BTreeMap::new();
        for (id, address) in owners.filter(|(id, _)| *id != self.id) {
            seen.entry(id).or_insert(address);
        }
        let (mut lacking, mut down) = (Vec::new(), Vec::new());
        for (id, f) in self.features_of(seen.into_iter(), feature, true).await {
            match f {
                PeerFeature::Has => {}
                PeerFeature::Lacks => lacking.push(format!("{id} (an older build)")),
                PeerFeature::NoAnswer(_) => down.push(id),
            }
        }
        lacking.sort();
        if lacking.is_empty() {
            Ok(down)
        } else {
            Err(ApiError::failed_precondition(format!(
                "{what} needs every node that holds a part of it to run a build that supports \
                 it, and these don't: {}. Finish the upgrade first, or ask without it",
                lacking.join(", ")
            )))
        }
    }
}
