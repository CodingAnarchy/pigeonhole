//! Opening a partition on a running node (D317 ticket 6, §3): one that a map install gives
//! this node, or (with `close`) one reopened by `unfence`. The node's start opens its
//! partitions itself before it serves ([`super::run`]); this is the same open, one partition
//! at a time, while the node serves.
//!
//! Where the open runs (the coordinator, 2026-11-11): a thread's CPU mask is its creator's,
//! so the engine opens on a fresh thread that first takes the narrow free-core mask
//! ([`Placement::enter`]), never on a Tokio worker or a blocking thread. Its unpinned
//! threads inherit that mask, and its shards are planned from it (`pin_plan` plans from the
//! calling thread's affinity set): off every existing shard's core and its SMT siblings.
//! Where the free cores can't hold the new partition's shards, the open is refused, never
//! pinned onto a sibling or left unpinned.

use std::sync::{Arc, Weak};

use super::Node;
use crate::serve::Shared;
use crate::serve::backup::Backups;
use crate::serve::errors::ApiError;
use crate::serve::placement::{Memory, Placement, Share};

/// How this node opens a partition while it serves, set once its server is built
/// ([`attach_opener`]).
pub(crate) struct Opener {
    /// The node's server: its parts, its node-wide service and the feed runtimes. Weak, since
    /// the server holds the node.
    pub(crate) shared: Weak<Shared>,
    pub(crate) placement: Arc<Placement>,
    pub(crate) backups: Arc<Backups>,
    /// A partition's options for its share and memory, as the node's start opens them.
    pub(crate) options: Box<dyn Fn(Share, Option<Memory>) -> pigeonhole::Options + Send + Sync>,
    /// `--shards` and `--compaction-threads`: the node's totals, split across its partitions.
    pub(crate) shards: usize,
    pub(crate) compaction: usize,
    /// `--engine-memory`, split as at the start.
    pub(crate) engine_memory: Option<u64>,
}

impl std::fmt::Debug for Opener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Opener")
            .field("shards", &self.shards)
            .field("compaction", &self.compaction)
            .field("engine_memory", &self.engine_memory)
            .finish_non_exhaustive()
    }
}

/// Lets `node` open partitions while `shared` serves (a node's start, and the test rig's).
pub(crate) fn attach_opener(node: &Arc<Node>, opener: Opener) {
    let _ = node.opener.set(opener);
}

impl Node {
    /// Opens partition `id`, which the map gives this node, while the node serves: under
    /// the node's open lock, with its share of the node's budget recomputed for the new
    /// set of partitions (the open ones keep theirs), fenced (the start's fence, or the
    /// operator's from `fenced.json`) until a map confirms it ([`Node::confirm`]) or it's
    /// unfenced. Its schema is reconciled to the map before it's swapped in. Already open:
    /// nothing to do.
    pub(crate) async fn open_part(self: &Arc<Self>, id: &str) -> Result<(), ApiError> {
        let _one = self.opening.lock().await;
        let opener = self
            .opener
            .get()
            .ok_or_else(|| ApiError::unsupported("this node opens no partition while it serves"))?;
        let shared = opener
            .shared
            .upgrade()
            .ok_or_else(|| ApiError::partition_unavailable("the server is stopping"))?;
        let cluster = shared
            .router
            .cluster
            .as_ref()
            .ok_or_else(|| ApiError::internal("not a cluster node"))?;
        let open_now = cluster.parts.load_full();
        if open_now.contains_key(id) {
            return Ok(());
        }
        let map = self.get_map()?;
        let epoch = match map.partitions.iter().find(|p| p.id == id) {
            Some(p) if p.node == self.id => p.epoch,
            Some(p) => {
                return Err(ApiError::failed_precondition(format!(
                    "the map gives partition {id:?} to node {:?}, not this one",
                    p.node
                )));
            }
            None => {
                return Err(ApiError::not_found(format!(
                    "partition {id:?} isn't in the cluster map"
                )));
            }
        };
        // Its share, as the node's start would split the budget across the new set; the
        // open partitions keep the shares they opened with (resharing an open engine is
        // out of scope, ICR 0117).
        let n = open_now.len() + 1;
        let shares = crate::serve::placement::split(opener.shards, opener.compaction, n);
        let share = shares[n - 1];
        let memory = match opener.engine_memory {
            None => {
                let bytes = map
                    .partitions
                    .iter()
                    .find(|p| p.id == id)
                    .map_or(0, |p| p.memory_bytes);
                (bytes > 0)
                    .then(|| Memory::of(bytes, share.shards))
                    .transpose()
                    .map_err(|least| {
                        ApiError::failed_precondition(format!(
                            "partition {id}'s memory_bytes can't cover {} shards' memtables: it \
                             needs at least {least}",
                            share.shards
                        ))
                    })?
            }
            Some(total) => {
                let overrides: Vec<(usize, u64)> = shares
                    .iter()
                    .enumerate()
                    .map(|(i, s)| {
                        let named = if i + 1 == n {
                            Some(id)
                        } else {
                            open_now.keys().nth(i).map(String::as_str)
                        };
                        let bytes = named
                            .and_then(|q| map.partitions.iter().find(|p| p.id == q))
                            .map_or(0, |p| p.memory_bytes);
                        (s.shards, bytes)
                    })
                    .collect();
                let memories = crate::serve::placement::memory(Some(total), &overrides).map_err(
                    |(_, why)| {
                        ApiError::failed_precondition(format!("partition {id}'s memory: {why}"))
                    },
                )?;
                Some(memories[n - 1])
            }
        };
        // Joined from a blocking thread, so no async worker waits on the open.
        let node = Arc::clone(self);
        let (part, cluster_id) = (id.to_owned(), map.cluster_id.clone());
        let opened = tokio::task::spawn_blocking(move || {
            let Some(opener) = node.opener.get() else {
                return Err(ApiError::internal("the node's opener went away"));
            };
            std::thread::scope(|scope| {
                // A fresh thread on the free-core mask (the coordinator's mask directive):
                // the engine's threads inherit it, and its shards are planned inside it.
                std::thread::Builder::new()
                    .name("phdb-open-partition".into())
                    .spawn_scoped(scope, || {
                        opener.placement.enter();
                        let mut log = Vec::new();
                        let opened = super::open_partitions(
                            &node.dir,
                            Some(&cluster_id),
                            &[(part, epoch)],
                            super::Budget {
                                shares: &[share],
                                reserved: None,
                                memories: memory.as_ref().map(std::slice::from_ref),
                            },
                            |s, m| (opener.options)(s, m),
                            &mut log,
                        );
                        eprint!("{}", String::from_utf8_lossy(&log));
                        opened
                    })
                    .map_err(|e| ApiError::internal(format!("start the partition's open: {e}")))?
                    .join()
                    .map_err(|_| ApiError::internal("the partition's open panicked"))?
                    .map_err(|f| ApiError::failed_precondition(f.message().to_owned()))
            })
        })
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
        let mut opened = opened?
            .pop()
            .ok_or_else(|| ApiError::internal("the open returned no partition"))?;
        // Pinned on the free cores, or not opened: never on an existing shard's sibling, and
        // never unpinned where pinning works.
        if opener.placement.masked() && opened.pinning.shards_unpinned() > 0 {
            let _ = opened.db.close();
            return Err(ApiError::failed_precondition(format!(
                "partition {id}'s {} shards don't fit on this node's free cores (those off \
                 every open partition's shard cores and their SMT siblings): {} of them would \
                 run unpinned; give the node more CPUs, fewer --shards, or move a partition off",
                opened.pinning.shards,
                opened.pinning.shards_unpinned(),
            )));
        }
        // Reopened: its files change from here, so the hashes a close kept go (feed1248).
        self.fences.closed().remove(id);
        let gate = self.open_gate(id, epoch);
        let service = shared
            .router
            .service
            .for_partition(
                opened.db.clone(),
                id,
                &opener.backups,
                opened.pinning,
                opened.memory,
                gate,
            )
            .placement(&opener.placement);
        if let Some(part) = service.part_opt()
            && let Some((feeds, hubs)) = shared.feed_runtimes.get()
        {
            part.feeds.run_on(feeds.clone(), hubs.clone());
        }
        let mut parts = (*cluster.parts.load_full()).clone();
        parts.insert(std::mem::take(&mut opened.id), Arc::new(service));
        cluster.parts.store(Arc::new(parts));
        // Its schema follows the map before it's unfenced (D317 ticket 5).
        let node = Arc::clone(self);
        tokio::task::spawn_blocking(move || node.reconcile(&map))
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        Ok(())
    }

    /// Opens every partition the newest map gives this node that isn't open here, each
    /// unfenced once that map confirms it (a relocation's new owner serves at once). A
    /// partition that fails to open is reported and left to the next map, or the operator.
    pub(crate) async fn open_missing(self: &Arc<Self>) {
        if self.opener.get().is_none() {
            return;
        }
        let Ok(map) = self.get_map() else {
            return;
        };
        let mine: Vec<String> = map
            .partitions
            .iter()
            .filter(|p| p.node == self.id)
            .map(|p| p.id.clone())
            .collect();
        for id in mine {
            // Open, or closed by the operator: a close holds until `unfence`.
            if self.part_state(&id).is_some() || self.fences.closed().contains_key(&id) {
                continue;
            }
            match self.open_part(&id).await {
                Ok(()) => {
                    eprintln!(
                        "phdb serve: partition {id} opened on map version {}",
                        map.map_version
                    );
                    self.confirm(&map);
                }
                Err(e) => eprintln!(
                    "phdb serve: partition {id}: map version {} gives it to this node, and it \
                     didn't open: {}",
                    map.map_version, e.message
                ),
            }
        }
    }

    /// [`open_missing`](Self::open_missing) in a task, after an install, when a server
    /// serves this node.
    pub(crate) fn open_missing_soon(&self) {
        if self.opener.get().is_none() {
            return;
        }
        let (Some(node), Ok(rt)) = (self.me.upgrade(), tokio::runtime::Handle::try_current())
        else {
            return;
        };
        rt.spawn(async move { node.open_missing().await });
    }
}
