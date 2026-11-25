use super::*;

impl Node {
    /// `Cluster.Status`: the node, its map version, and its directory's partition files.
    pub(super) fn status(&self) -> NodeStatus {
        self.status_with(false)
    }

    /// [`Self::status`]; with `open_only` (D332's survey, `StatusRequest.open_only`), only
    /// the partitions open here: no closed file is opened to read its identity (perf287).
    pub(super) fn status_with(&self, open_only: bool) -> NodeStatus {
        let (version, cluster_id, divergent, map) = {
            let st = self.state();
            (
                st.version(),
                st.map
                    .as_ref()
                    .map(|(m, _)| m.cluster_id.clone())
                    .unwrap_or_default(),
                st.divergent.iter().cloned().collect(),
                st.map.as_ref().map(|(m, _)| m.clone()),
            )
        };
        let mut partitions = Vec::new();
        let mut files: Vec<PathBuf> = std::fs::read_dir(&self.dir)
            .map(|d| {
                d.filter_map(|e| e.ok().map(|e| e.path()))
                    .filter(|p| p.extension().is_some_and(|x| x == "phdb"))
                    .collect()
            })
            .unwrap_or_default();
        files.sort();
        // The partitions this node has open: their identity comes from their own handle,
        // since a read-only open of a file a writer holds is refused (#1767).
        let open: std::collections::BTreeMap<String, crate::serve::router::PartRef> = self
            .server
            .get()
            .and_then(|o| o.shared.upgrade())
            .map(|shared| shared.router.partitions().into_iter().collect())
            .unwrap_or_default();
        let copies = self
            .server
            .get()
            .and_then(|o| o.shared.upgrade())
            .and_then(|shared| {
                shared
                    .router
                    .cluster
                    .as_ref()
                    .map(|c| c.replicas.load_full())
            })
            .unwrap_or_default();
        let spanning = self
            .server
            .get()
            .and_then(|o| o.shared.upgrade())
            .and_then(|shared| {
                shared
                    .router
                    .cluster
                    .as_ref()
                    .map(|c| Arc::clone(&c.spanning))
            });
        for path in files {
            let file = path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let named = path
                .file_stem()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let mut lp = LocalPartition {
                id: named.clone(),
                epoch: 0,
                file,
                ok: false,
                error: String::new(),
                ..Default::default()
            };
            // This node's replica copy (D328): its follower says how it is. Its file is held
            // open, and its identity row is its owner's, so it isn't read here.
            if let Some(copy) = copies.get(&named) {
                let c = copy.follower.copy_status();
                lp.role = proto::PartitionRole::Replica as i32;
                lp.set_replica_state(c.state);
                lp.replica_of = c.owner;
                lp.replica_applied_seqno = c.applied;
                lp.replica_primary_seqno = c.primary_seqno;
                lp.replica_lag_ms = c.lag_ms;
                // The copy's identity epoch: its owner's at the bootstrap (`e_b`), which a
                // promotion checks against P's history (D328 PR 3).
                lp.epoch = copy
                    .service
                    .part()
                    .ok()
                    .and_then(|part| identity::read(&part.db).ok().flatten())
                    .map_or(0, |id| id.epoch);
                lp.ok = c.state != proto::ReplicaState::Behind
                    && c.state != proto::ReplicaState::OwnerFeedOff;
                lp.error = if lp.ok { String::new() } else { c.last_error };
                partitions.push(lp);
                continue;
            }
            // A partition being opened: no read of its file, whose read-only open would refuse
            // the node's own open (#1770). It shows as opening.
            if self.fences.is_opening(&named) {
                lp.set_state(proto::LocalPartitionState::Opening);
                lp.ok = true;
                partitions.push(lp);
                continue;
            }
            let read = match open.get(&named) {
                Some(part) => identity::read(&part.db),
                None if open_only => continue,
                None => identity::read_file(&path),
            };
            match read {
                Ok(None) => lp.error = "no identity row: run phdb cluster init-partition".into(),
                Err(e) => lp.error = e,
                Ok(Some(id)) => {
                    lp.epoch = id.epoch;
                    lp.error = if id.partition_id != named {
                        format!("the file says it's partition {:?}", id.partition_id)
                    } else if !cluster_id.is_empty() && id.cluster_id != cluster_id {
                        format!("the file is in cluster {:?}", id.cluster_id)
                    } else {
                        match map
                            .as_ref()
                            .and_then(|m| m.partitions.iter().find(|p| p.id == named))
                        {
                            Some(p) if p.node != self.id => {
                                format!("the map has it on node {:?}", p.node)
                            }
                            Some(p) if p.epoch != id.epoch => format!(
                                "the map has it at epoch {}, the file at {}",
                                p.epoch, id.epoch
                            ),
                            // A split's new partition is created before the change that
                            // records it: its own line, so a merge's rerun (which drops only
                            // files with exactly the line above) never takes it (audit120's
                            // S-a on #2131).
                            None if map.is_some() => match open.get(&named) {
                                Some(part) => identity::split_mark(&part.db),
                                None => identity::split_mark_file(&path),
                            }
                            .map_or_else(
                                |e| {
                                    format!(
                                        "the map has no such partition (its split mark \
                                         couldn't be read: {e})"
                                    )
                                },
                                |mark| match mark {
                                    Some(mark) => format!(
                                        "the map has no such partition yet: split {:?} \
                                         created it",
                                        mark.move_id
                                    ),
                                    None => "the map has no such partition".into(),
                                },
                            ),
                            _ => String::new(),
                        }
                    };
                    lp.ok = lp.error.is_empty();
                }
            }
            // D329: its shares held in doubt, the oldest first named, and why each waits.
            if let Some(part) = open.get(&named) {
                in_doubt(&mut lp, &part.db, &named, spanning.as_deref());
            }
            partitions.push(lp);
        }
        // Partitions the map places here with no file in DIR: what an operator most needs
        // to see before the node serves them (cli692's N1).
        if let Some(m) = &map {
            for p in m.partitions.iter().filter(|p| p.node == self.id) {
                if !partitions.iter().any(|lp| lp.id == p.id) {
                    partitions.push(LocalPartition {
                        id: p.id.clone(),
                        epoch: p.epoch,
                        file: format!("{}.phdb", p.id),
                        ok: false,
                        error: format!(
                            "the map places it here, but the directory has no {}.phdb",
                            p.id
                        ),
                        ..Default::default()
                    });
                }
            }
        }
        // Each open partition's reconcile (D317 ticket 5), through its own service.
        {
            let reconciled = self
                .reconciled
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            for lp in &mut partitions {
                let here = map.as_ref().is_some_and(|m| {
                    m.partitions
                        .iter()
                        .any(|p| p.id == lp.id && p.node == self.id)
                });
                if here && let Some((v, o)) = reconciled.get(&lp.id) {
                    lp.reconciled_version = *v;
                    lp.reconcile_errors = o.errors.clone();
                    lp.unmapped_tables = o.unmapped.clone();
                    lp.adopted_tables = o.adopted.clone();
                }
            }
        }
        // An owner's file (D328): the map gives the partition to this node.
        for lp in &mut partitions {
            if lp.role == 0
                && map.as_ref().is_some_and(|m| {
                    m.partitions
                        .iter()
                        .any(|p| p.id == lp.id && p.node == self.id)
                })
            {
                lp.role = proto::PartitionRole::Owner as i32;
            }
        }
        // Each open partition's state, and a closed one's files (D317 ticket 6).
        for lp in &mut partitions {
            // One read (simp724's S1 on #1964): a reopen or a close between two would show
            // the partition as neither.
            let id = lp.id.clone();
            if let fence::Now::Served(gate) = self.fences.now(&id, |closed| closed.describe(lp)) {
                let (state, reason) = fence::gate_state(&gate);
                lp.set_state(state);
                lp.set_fence_reason(reason);
                // Both reasons at once (#1829): an operator-fenced partition may still await
                // its lease.
                lp.awaiting_peers = self.awaiting_peers(&id);
                lp.lease_remaining_ms = millis(self.lease_remaining(&id));
                // D332: its load, folded now if an interval has ended.
                if let Some(part) = open.get(&id) {
                    let l = self.partition_load(part);
                    (lp.load_cpu, lp.load_device, lp.load) = (l.cpu, l.device, l.load());
                    // D331 PR 5's plan: absent until the first interval ends.
                    lp.write_bytes_rate = l.write_rate.map(|r| r as u64);
                    // Its size from the file system, no read of the file (perf287): the
                    // path the database opened, not one built from the id (CodeQL on
                    // #2103).
                    lp.file_bytes = std::fs::metadata(part.db.path()).map_or(0, |m| m.len());
                    // D331 PR 6: an owner's partition the map places nothing on (a merge
                    // emptied it): what still keeps it listed.
                    if let Some(m) = &map
                        && m.partitions.iter().any(|p| p.id == id && p.node == self.id)
                        && !super::map::placed(m, &id)
                    {
                        lp.drop_waits = part
                            .service()
                            .drop_waits(super::spanning::now_ms())
                            .unwrap_or_default();
                    }
                }
            }
            // D331: the moves naming it (the follower's lag comes with PR 4's follower).
            if let Some(m) = &map {
                lp.moves = crate::serve::ranges::statuses(m, &id);
                // PR 4b: a source whose move's retention hold lapsed; a target's follower.
                if let Some(copies) = self.range_copies() {
                    for s in lp.moves.iter_mut().filter(|s| s.source) {
                        s.retention_lapsed = copies.lapsed(&s.id);
                    }
                }
                // PR 5: a source's fence seqno, once its fence drained; either side's drop.
                if let Some(fences) = self.range_fences() {
                    for s in lp.moves.iter_mut().filter(|s| s.source) {
                        s.fence_seqno = fences.seqno(&s.id);
                    }
                }
                if let Some(drops) = self.range_drops() {
                    for s in &mut lp.moves {
                        s.dropped = drops.dropped(&s.id);
                    }
                }
                if let Some(follows) = self.range_follows() {
                    for s in lp.moves.iter_mut().filter(|s| !s.source) {
                        if let Some(p) = follows.progress(&s.id) {
                            s.copied_bytes =
                                p.copied_bytes.load(std::sync::atomic::Ordering::Relaxed);
                            s.copy_done = p.copy_done.load(std::sync::atomic::Ordering::Relaxed);
                            s.source_applied_seqno =
                                p.applied.load(std::sync::atomic::Ordering::Acquire);
                            s.follower_error = p
                                .failed
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .clone()
                                .unwrap_or_default();
                            s.follower_waiting = p
                                .waiting
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .clone();
                        }
                    }
                }
            }
        }
        let (writer, writer_term, writer_lease) = map.as_ref().map_or_else(
            || (String::new(), self.grants.voted(), Duration::ZERO),
            |m| {
                (
                    schema::writer(m).to_owned(),
                    self.grants.term_floor(m),
                    self.writer_lease_status(m),
                )
            },
        );
        let (move_rate_bytes, move_retention_cap, max_move_followers) =
            self.range_copies().map_or((0, 0, 0), |s| {
                let (rate, cap, followers) = s.limits();
                (rate, cap, u32::try_from(followers).unwrap_or(u32::MAX))
            });
        NodeStatus {
            move_rate_bytes,
            move_retention_cap,
            max_move_followers,
            balancer: self.balancer_state(),
            writer,
            writer_term,
            writer_lease_remaining_ms: millis(writer_lease),
            failover_blocked: self.failovers.blocked(),
            grants: self.grants.granted(),
            node_id: self.id.clone(),
            map_version: version,
            cluster_id,
            partitions,
            divergent,
            grace_remaining_ms: millis(self.grants.grace_remaining()),
            lease_ms: millis(self.grants.lease()),
            map_features: MAP_FEATURES
                .iter()
                .filter(|&&f| {
                    #[cfg(test)]
                    if f == NEAREST_QUALIFIERS_FEATURE && old_nearest(&self.dir, OldNearest::Status)
                    {
                        return false;
                    }
                    #[cfg(test)]
                    if f == INDEXES_FEATURE
                        && OLD_INDEXES
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .iter()
                            .any(|d| self.dir.starts_with(d))
                    {
                        return false;
                    }
                    #[cfg(test)]
                    if f == INDEX_BULK_LOAD_FEATURE
                        && OLD_BULK_LOAD
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .iter()
                            .any(|d| self.dir.starts_with(d))
                    {
                        return false;
                    }
                    #[cfg(test)]
                    if f == LEASES_FEATURE && fence::pre_lease(&self.dir) {
                        return false;
                    }
                    #[cfg(test)]
                    if f == MEMORY_TOOL_FIELDS_FEATURE
                        && OLD_MEMORY
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .iter()
                            .any(|d| self.dir.starts_with(d))
                    {
                        return false;
                    }
                    let _ = f;
                    true
                })
                .map(|&f| f.to_owned())
                .collect(),
        }
    }
}
