use super::*;

/// How long a node change's `SetMap` goes on pushing it, in push timeouts, for a majority of
/// both the nodes before it and the nodes after it to hold it (#1899's F4).
pub(super) const JOINT_PUSHES: u32 = 3;

/// Who holds a node change ([`Node::joint`]): of the nodes before it and after it, how many
/// hold it and how many there are, this node included; and why each node that doesn't, as
/// "id: why".
struct Joint {
    before: (usize, usize),
    after: (usize, usize),
    missing: Vec<String>,
}

impl Joint {
    /// A majority of both sets.
    fn held(&self) -> bool {
        self.before.0 * 2 > self.before.1 && self.after.0 * 2 > self.after.1
    }

    /// The refusal of a change while `map`, a node change, isn't held.
    fn refusal(&self, map: &ClusterMap) -> String {
        format!(
            "map version {} changed the cluster's nodes, and a majority of both the nodes \
             before it ({} of {}) and the nodes after it ({} of {}) must hold it before the \
             map changes again (D317 rule 3): {}. Bring those nodes up, or set a map with \
             --force",
            map.map_version,
            self.before.0,
            self.before.1,
            self.after.0,
            self.after.1,
            self.missing.join("; ")
        )
    }
}

/// Where a map [`Node::install`] takes comes from.
#[derive(Clone, Copy)]
pub(super) enum MapSource<'a> {
    /// Set here through the writer (`SetMap`): a valid change from the current map, with
    /// its partitions' `history_epoch` derived (`map::with_history`), keeping the history
    /// of each partition in `SetMapRequest.copies`.
    Change(&'a [String]),
    /// Set here by the forced recovery (`SetMap` with `force`): the same cluster's and newer
    /// (`map::validate_catch_up`), with no epoch going back and no move keeping one
    /// (`map::validate_epochs`), and `history_epoch` derived as for a change that copied
    /// nothing. That's only as new as this node's map: `phdb cluster set-map --force`
    /// checks the epochs against every node it reaches (simp724's F2 on #1729), so a forced
    /// epoch is above every one the cluster used and its history value is new.
    Forced,
    /// A peer's, pushed or pulled: only the same cluster's and newer (cli692's R1), taken
    /// as it arrives, derived where it was set.
    Peer,
}
/// The marker a divergent push's refusal carries, so the pusher records the divergence too.
/// Matched in the refusal's message (cli692's N4): a field of its own (a status detail, say)
/// would be sturdier across versions, once a client other than a node needs it.
pub(super) const DIVERGENT: &str = "divergent map";

/// The marker the term fence's refusal carries (D330), so a writer whose push it refuses
/// looks at the node (#2073). Matched in the message, as [`DIVERGENT`] is.
pub(super) const VOTED_ABOVE: &str = "this node voted for term";

impl Node {
    /// Validates and persists `map` as the node's newest; `check` decides, under the state
    /// lock, whether it may replace the current one. `from` says where the map comes from,
    /// which decides its checks and whether its partitions' `history_epoch` is derived here
    /// ([`MapSource`]). Returns the new version.
    ///
    /// **Blocking** (address resolution, an fsync): run it on a blocking thread
    /// ([`Node::install_async`]). The state lock is held only to check and to publish; the
    /// persist runs under `installing` alone.
    pub(super) fn install(
        &self,
        map: &ClusterMap,
        from: MapSource<'_>,
        check: impl FnOnce(&mut MapState) -> Result<Option<u64>, ApiError>,
    ) -> Result<u64, ApiError> {
        map::validate(map, self.loopback_only).map_err(ApiError::invalid)?;
        // A map that adopted a feature this build lacks: refused, whatever its source.
        let lacked = map::lacked_adopted(map, &super::status::build_features(&self.dir));
        if !lacked.is_empty() {
            return Err(ApiError::failed_precondition(map::below_adopted(&lacked)));
        }
        // A map set here, forced or not, gets its partitions' `history_epoch` derived from
        // the current one, or from none for a first map (D317 ticket 6); a peer's arrives
        // derived.
        let mut derived = None;
        let _one = self
            .installing
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        {
            let mut st = self.state();
            if let Some(same) = check(&mut st)? {
                return Ok(same);
            }
            // The term fence (D330 amendment 1's M1): a map below a term this node voted for
            // is from a writer that has been replaced.
            let voted = self.grants.voted();
            if map.writer_term < voted {
                return Err(ApiError::failed_precondition(format!(
                    "the map is at writer term {}, but {VOTED_ABOVE} {voted}: it's from a writer \
                     that has been replaced (D330)",
                    map.writer_term
                )));
            }
            match &st.map {
                None if !map.nodes.iter().any(|n| n.id == self.id) => {
                    return Err(ApiError::invalid(format!(
                        "this node's first map must name it: node {:?} isn't in the map",
                        self.id
                    )));
                }
                None if matches!(from, MapSource::Change(c) if !c.is_empty()) => {
                    return Err(ApiError::invalid(
                        "a first map can't mark a partition as copied: it relocates nothing",
                    ));
                }
                // A first map set here (a new cluster, or a restore's forced map onto fresh
                // nodes) starts every history at its epoch, whatever the map sent says: a
                // backup's map carries its old values, which its files no longer hold
                // (simp724's S1 on #1729). A peer's arrives derived.
                None => {
                    if !matches!(from, MapSource::Peer) {
                        derived = Some(
                            map::with_history(&ClusterMap::default(), map, &[])
                                .map_err(ApiError::invalid)?,
                        );
                    }
                }
                Some((old, _)) => match from {
                    MapSource::Change(copies) => {
                        map::validate_change(old, map).map_err(ApiError::invalid)?;
                        derived =
                            Some(map::with_history(old, map, copies).map_err(ApiError::invalid)?);
                    }
                    MapSource::Forced => {
                        map::validate_catch_up(old, map).map_err(ApiError::invalid)?;
                        // No epoch goes back and no move keeps one, so no history value is
                        // used twice (simp724's F1 on #1729); the tables needn't step.
                        map::validate_epochs(old, map).map_err(ApiError::invalid)?;
                        derived =
                            Some(map::with_history(old, map, &[]).map_err(ApiError::invalid)?);
                    }
                    MapSource::Peer => {
                        map::validate_catch_up(old, map).map_err(ApiError::invalid)?;
                    }
                },
            }
        }
        #[cfg(test)]
        super::hold_install(&self.dir);
        let canonical = map::canonical(derived.as_ref().unwrap_or(map));
        let json = map::canonical_json(&canonical);
        write_map(&self.dir, &json)
            .map_err(|e| ApiError::internal(format!("persist the cluster map: {e}")))?;
        let version = canonical.map_version;
        // The partition's files take the map's schema before its routes serve (D317 ticket
        // 5): a call routed by this map finds its table, or `TableNotFound` while a node
        // that hasn't taken it yet catches up.
        self.reconcile_locked(&_one, &canonical, None);
        let routes = Arc::new(route::Routes::new(&canonical));
        let mut st = self.state();
        let moved = moved_partitions(st.map.as_ref().map(|(m, _)| m), &canonical);
        st.map = Some((canonical, json));
        // Under the state lock, so the routes never run behind the map `GetMap` answers.
        self.routes.store(Some(routes));
        st.divergent.clear();
        // Versions at or below this one need no pull again (cli692's N3).
        st.pulled.retain(|v| *v > version);
        // A lease the map no longer names this node for ends now (audit120's S1), still
        // under the state lock, so a round reads the new map only once it has (audit120's
        // M3): its request no longer asks for the partition, and frees its grants. The
        // state lock, then the fences' `local` one, as `apply_leases` takes them.
        if let Some((m, _)) = &st.map {
            self.fences.lapse_unnamed(m, &self.id);
        }
        let installed = st.map.as_ref().map(|(m, _)| m.clone());
        drop(st);
        if let Some(m) = &installed {
            self.release_writer_if_replaced(m);
        }
        // A partition the map now gives this node opens (D317 ticket 6, §3).
        self.open_missing_soon();
        self.end_moved_feeds(version);
        // D331: each partition's range gate, and a source's hand-over.
        if let Some(m) = &installed {
            self.apply_moves(m);
        }
        self.wake_copies(&moved);
        Ok(version)
    }

    /// Wakes this node's replica copies of `moved` (D330 amendment 2): their followers look
    /// the owner up in the map now rather than after their pause.
    fn wake_copies(&self, moved: &[String]) {
        if moved.is_empty() {
            return;
        }
        let Some(shared) = self.server.get().and_then(|o| o.shared.upgrade()) else {
            return;
        };
        let Some(cluster) = shared.router.cluster.as_ref() else {
            return;
        };
        let copies = cluster.replicas.load();
        for id in moved {
            if let Some(copy) = copies.get(id) {
                copy.follower.wake();
            }
        }
    }

    /// Ends the cluster feed components of every partition open here that the map,
    /// installed at `version`, gives another node: a relocation that didn't close it here
    /// (a fenced partition stays open) would otherwise leave them following this copy,
    /// which takes no more writes. Their subscriptions open them again where the map puts
    /// them (audit120's S-C5). After the routes are stored, so a component opened meanwhile
    /// finds the new map when it checks it again ([`feed::serve`]).
    fn end_moved_feeds(&self, version: u64) {
        let Some(shared) = self.server.get().and_then(|o| o.shared.upgrade()) else {
            return;
        };
        let Some(cluster) = shared.router.cluster.as_ref() else {
            return;
        };
        let routes = self.routes();
        let Some(routes) = routes.as_ref() else {
            return;
        };
        for (id, service) in cluster.parts.load().iter() {
            if routes.owner(id).is_some_and(|o| o.node == self.id) {
                continue;
            }
            if let Ok(part) = service.part() {
                part.feeds.end_parts(ApiError::wrong_partition(
                    version,
                    format!("map version {version} moved partition {id:?} off this node"),
                ));
            }
        }
    }

    /// [`Node::install`] on a blocking thread.
    async fn install_async(
        self: &Arc<Self>,
        map: ClusterMap,
        copies: Option<Vec<String>>,
        check: impl FnOnce(&mut MapState) -> Result<Option<u64>, ApiError> + Send + 'static,
    ) -> Result<u64, ApiError> {
        let node = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let from = copies
                .as_deref()
                .map_or(MapSource::Forced, MapSource::Change);
            node.install(&map, from, check)
        })
        .await?
    }

    /// `Cluster.SetMap`: compare-and-set on the version, then push to the others. Only the
    /// map's writer, the node with the smallest id in its current map, sets a map (D317
    /// amendment 1); another answers `WrongPartition` naming it, and `phdb cluster set-map`
    /// follows. `force` (the forced recovery, for a writer that's down) skips the writer and
    /// the compare-and-set: the map need only be the same cluster's, at a version above this
    /// node's.
    ///
    /// **A node change (#1899's F4):** a map that changes the nodes carries the nodes before
    /// it (`previous_nodes`), and answers once a majority of those and a majority of its own
    /// hold it, or after [`JOINT_PUSHES`] push timeouts of trying. Until a majority of both
    /// hold it, the writer takes no other change, a schema change included: a majority of
    /// one set confirms a partition's owner (D317 rule 3), so the set before must have moved
    /// on before a later map can count on the set after.
    pub(super) async fn set_map(
        self: &Arc<Self>,
        req: SetMapRequest,
    ) -> Result<SetMapResponse, ApiError> {
        let mut map = req
            .map
            .ok_or_else(|| ApiError::invalid("SetMap needs a map"))?;
        if req.force {
            // A copy is a planned move, never part of a recovery: a forced map's raised
            // epochs all restart their partitions' histories.
            if !req.copies.is_empty() {
                return Err(ApiError::invalid(
                    "a forced map can't mark a partition as copied: relocate by copy through \
                     the map's writer",
                ));
            }
            // A recovery waits for no majority: it's what `--force` means.
            map.previous_nodes.clear();
            // A forced map keeps exactly the features this node's map adopted, never what the
            // map sent says (cli692's B2 on #2237): none without a current map, so a first
            // forced map, a restore's, adopts again from its first set.
            map.adopted_features = self
                .get_map()
                .map(|c| c.adopted_features)
                .unwrap_or_default();
            let version = self.install_async(map, None, |_| Ok(None)).await?;
            let pushed = self.push_all().await;
            return Ok(SetMapResponse {
                map_version: version,
                pushed,
            });
        }
        if let Ok(current) = self.get_map() {
            let w = schema::writer(&current);
            if w != self.id {
                let address = current
                    .nodes
                    .iter()
                    .find(|n| n.id == w)
                    .map(|n| n.address.as_str())
                    .unwrap_or_default();
                return Err(ApiError::wrong_partition(
                    current.map_version,
                    format!(
                        "node {w} (at {address}) is the cluster map's writer: set the map \
                         there, or with --force if it's down"
                    ),
                ));
            }
            if !self.synced.load(std::sync::atomic::Ordering::Acquire) {
                return Err(ApiError::partition_unavailable(
                    "this node is still taking its peers' maps after starting: try again",
                ));
            }
            // The writer and its term are the writer's to keep (D330): an operator's map
            // can't change them, and from term 1 a change needs the writer lease.
            map.writer = current.writer.clone();
            map.writer_term = current.writer_term;
            // The cluster's adopted features are the writer's to keep, as its term is: an
            // operator's map can neither drop nor add one, and every gate below reads the
            // current map's (cli692's B1 on #2237).
            map.adopted_features = current.adopted_features.clone();
            self.await_writer_lease(&current).await?;
            if !map::uses_failover(&current) && map::uses_failover(&map) {
                self.every_node_keeps(&map, WRITER_TERMS_FEATURE, "failover")
                    .await?;
            }
            if let Some(why) = map::moves_with_node_change(&current, &map) {
                return Err(ApiError::failed_precondition(why));
            }
            // A memory store goes into the map only once every node keeps it (#1915).
            if current.memory.is_none() && map.memory.is_some() {
                self.every_node_keeps(&map, MEMORY_FEATURE, "a memory store")
                    .await?;
            }
            // So do read replicas (D328).
            if !has_replicas(&current) && has_replicas(&map) {
                self.every_node_keeps(&map, REPLICAS_FEATURE, "a read replica")
                    .await?;
            }
            // So does the first index placement (#1722).
            if current.indexes.is_empty() && !map.indexes.is_empty() {
                self.every_node_keeps(&map, INDEXES_FEATURE, "an index placement")
                    .await?;
            }
            // So does the first range move (D331).
            if current.moves.is_empty() && !map.moves.is_empty() {
                self.every_node_keeps(&map, RANGE_MOVES_FEATURE, "a range move")
                    .await?;
            }
            // And every range move's start, or an abort's record (D331 PR 5).
            if map::starts_range_move(&current, &map) {
                self.every_node_keeps(&map, MOVE_ABORTING_FEATURE, "a range move")
                    .await?;
            }
            // And the first use of the balancer's fields (D332).
            if !map::uses_balancer(&current) && map::uses_balancer(&map) {
                self.every_node_keeps(&map, BALANCER_FEATURE, "the balancer's fields")
                    .await?;
            }
            // A node the map adds must report each adopted feature, and the writer adopts
            // what every node reports now.
            self.new_nodes_keep_adopted(&current, &map).await?;
            self.adopt_features(&mut map).await;
            if !current.previous_nodes.is_empty() {
                let (joint, _) = self.joint(&current).await;
                if !joint.held() {
                    return Err(ApiError::failed_precondition(joint.refusal(&current)));
                }
                self.joint_settled(&current).await;
            }
            map.previous_nodes = if map::same_nodes(&current, &map) {
                Vec::new()
            } else {
                current.nodes.clone()
            };
        } else {
            // A first map adopts nothing on trust (cli692's B2 on #2237): its features are
            // adopted from the cluster's first all-answering set.
            map.adopted_features.clear();
            // The node's first map: its nodes keep a memory store too, or none goes in.
            if map.memory.is_some() {
                self.every_node_keeps(&map, MEMORY_FEATURE, "a memory store")
                    .await?;
            }
            if has_replicas(&map) {
                self.every_node_keeps(&map, REPLICAS_FEATURE, "a read replica")
                    .await?;
            }
            if !map.indexes.is_empty() {
                self.every_node_keeps(&map, INDEXES_FEATURE, "an index placement")
                    .await?;
            }
            if !map.moves.is_empty() {
                self.every_node_keeps(&map, RANGE_MOVES_FEATURE, "a range move")
                    .await?;
            }
            if map.moves.iter().any(|m| !map::node_move(m)) {
                self.every_node_keeps(&map, MOVE_ABORTING_FEATURE, "a range move")
                    .await?;
            }
            if map::uses_balancer(&map) {
                self.every_node_keeps(&map, BALANCER_FEATURE, "the balancer's fields")
                    .await?;
            }
            if map::uses_failover(&map) || map.writer_term > 0 {
                self.every_node_keeps(&map, WRITER_TERMS_FEATURE, "failover or a writer term")
                    .await?;
            }
            map.previous_nodes.clear();
        }
        let changes_nodes = !map.previous_nodes.is_empty();
        let expected = req.expected_map_version;
        let version = self
            .install_async(map, Some(req.copies), move |st| {
                let current = st.version();
                if expected != current {
                    return Err(ApiError::from_code(
                        ErrorCode::Conflict,
                        &format!(
                            "the cluster map is at version {current}, not {expected}: re-read it and set it again",
                        ),
                    ));
                }
                Ok(None)
            })
            .await?;
        // The lease again before the push (D330). Past it, the change is installed here and
        // not undone: it may still reach the cluster (this writer's next round, if it renews
        // at the same term, pushes it) or be replaced by an elected map. So its outcome is
        // unknown (simp724's S1 on #2056).
        if let Ok(set) = self.get_map()
            && let Err(e) = self.writer_may_change(&set)
        {
            return Err(ApiError::from_code(
                ErrorCode::OutcomeUnknown,
                &format!(
                    "map version {version} is installed on this node, the writer, but its \
                     writer lease ran out before the push ({}). It may still reach the \
                     cluster, or be replaced by an elected map: read the map (phdb cluster \
                     show-map) before setting it again",
                    e.message
                ),
            ));
        }
        let pushed = match self.get_map() {
            Ok(set) if changes_nodes && set.map_version == version => self.joint_wait(&set).await,
            _ => self.push_all().await,
        };
        Ok(SetMapResponse {
            map_version: version,
            pushed,
        })
    }

    /// A node change `map` that a majority of both its node sets hold: waits until a lease
    /// has passed since this writer first saw that (audit120's L1 on #1899's F4, D326). The
    /// nodes before the change may have granted leases at the old version, and the nodes
    /// after it never saw those grants: until they run out, a majority of the new set could
    /// grant a partition to a second node. Recomputed, never remembered across a restart or
    /// a change of writer: such a writer counts from its own check, at most one lease more.
    async fn joint_settled(&self, map: &ClusterMap) {
        let now = self.grants.clock().now();
        let seen = {
            let mut seen = self
                .joint_seen
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            match *seen {
                Some((version, at)) if version == map.map_version => at,
                _ => {
                    *seen = Some((map.map_version, now));
                    now
                }
            }
        };
        let grace = u64::try_from(lease::grace(self.grants.lease()).as_nanos()).unwrap_or(u64::MAX);
        let left = seen.saturating_add(grace).saturating_sub(now);
        if left > 0 {
            tokio::time::sleep(Duration::from_nanos(left)).await;
        }
    }

    /// Records that this writer saw a majority of both of node change `map`'s sets hold it
    /// now, unless it saw that before ([`Node::joint_settled`]).
    fn joint_held(&self, map: &ClusterMap) {
        let now = self.grants.clock().now();
        let mut seen = self
            .joint_seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !matches!(*seen, Some((version, _)) if version == map.map_version) {
            *seen = Some((map.map_version, now));
        }
    }

    /// Pushes a node change `map` to the nodes before it and after it, until a majority of
    /// each holds it or [`JOINT_PUSHES`] push timeouts have passed: the last pushes.
    async fn joint_wait(self: &Arc<Self>, map: &ClusterMap) -> Vec<NodePush> {
        let until = tokio::time::Instant::now() + self.peers.push_timeout * JOINT_PUSHES;
        let mut pause = Duration::from_millis(100);
        loop {
            let (joint, pushed) = self.joint(map).await;
            let left = until.saturating_duration_since(tokio::time::Instant::now());
            if joint.held() {
                self.joint_held(map);
                return pushed;
            }
            if left.is_zero() {
                return pushed;
            }
            tokio::time::sleep(pause.min(left)).await;
            pause = (pause * 2).min(Duration::from_secs(1));
        }
    }

    /// Pushes a node change `map` (one with `previous_nodes`) to every node before it and
    /// after it, and counts who holds it: those that took it, and this node.
    async fn joint(self: &Arc<Self>, map: &ClusterMap) -> (Joint, Vec<NodePush>) {
        let mut all = map.previous_nodes.clone();
        for n in &map.nodes {
            if !all.iter().any(|p| p.id == n.id) {
                all.push(n.clone());
            }
        }
        let pushed = self.push_each(map, &all).await;
        let holds = |id: &str| id == self.id || pushed.iter().any(|p| p.node == id && p.ok);
        let count = |set: &[proto::Node]| (set.iter().filter(|n| holds(&n.id)).count(), set.len());
        let joint = Joint {
            before: count(&map.previous_nodes),
            after: count(&map.nodes),
            missing: pushed
                .iter()
                .filter(|p| !p.ok)
                .map(|p| format!("{}: {}", p.node, p.error))
                .collect(),
        };
        (joint, pushed)
    }
}
