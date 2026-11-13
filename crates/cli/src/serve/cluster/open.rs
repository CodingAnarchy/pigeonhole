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

/// Reapplies the server's masks for the shard CPUs of `parts`, the partitions open now
/// (D317 ticket 6), and says so. The runtimes keep the thread counts they started with
/// (Tokio can't resize one): a mask that shrank has more of them per CPU (cli692 on #1745).
pub(crate) fn remask(placement: &Placement, parts: &crate::serve::router::PartMap, why: &str) {
    let mut pinned: Vec<usize> = parts
        .values()
        .filter_map(|s| s.part_opt())
        .flat_map(|p| p.db.pinned_shard_cpus())
        .collect();
    pinned.sort_unstable();
    pinned.dedup();
    let masks = placement.reapply(&pinned);
    // Each runtime's threads against its CPUs, so an operator sees how a shrunken mask
    // packs them (perf287's note 1 on #1755); the counts stay as the server started them.
    let on = |threads: usize, m: &Option<Vec<usize>>| match m {
        Some(cpus) => format!(
            "{threads} on {} CPUs ({})",
            cpus.len(),
            crate::serve::placement::format_cpu_list(cpus)
        ),
        None => format!("{threads} on any CPU"),
    };
    let unmasked = if masks.blocking.is_none() && !pinned.is_empty() {
        ", unmasked: too few free cores off the shards' cores"
    } else {
        ""
    };
    eprintln!(
        "phdb serve: {why}: off the shards' {} CPUs{unmasked}: request workers {}, feed {} \
         and hub {}, blocking {}; the thread counts stay as the server started them (restart \
         to size them again)",
        pinned.len(),
        on(placement.io_threads, &masks.network),
        on(placement.feed_threads, &masks.feed),
        on(placement.hub_threads, &masks.hub),
        on(placement.blocking_threads, &masks.blocking),
    );
}

/// Partition `id` marked as opening on a node, until dropped.
struct Opening<'a> {
    node: &'a Node,
    id: String,
}

impl<'a> Opening<'a> {
    fn mark(node: &'a Node, id: &str) -> Self {
        node.fences
            .opening
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id.to_owned());
        Self {
            node,
            id: id.to_owned(),
        }
    }
}

impl Drop for Opening<'_> {
    fn drop(&mut self) {
        self.node
            .fences
            .opening
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}

/// A hold seam (tests): the next open in each listed cluster directory waits, marked as
/// opening, before its engine opens, until the sender sends or is dropped. One entry per
/// directory, so tests running at once don't take each other's.
#[cfg(test)]
pub(crate) static HOLD_OPEN: std::sync::Mutex<
    Vec<(std::path::PathBuf, std::sync::mpsc::Receiver<()>)>,
> = std::sync::Mutex::new(Vec::new());

/// A hold seam (tests): `open_missing` in each listed cluster directory waits after its next
/// partition opened, before it confirms it, until the sender sends or is dropped.
#[cfg(test)]
pub(crate) static HOLD_CONFIRM: std::sync::Mutex<
    Vec<(std::path::PathBuf, std::sync::mpsc::Receiver<()>)>,
> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn hold_confirm(dir: &std::path::Path) {
    let mut armed = HOLD_CONFIRM
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(i) = armed.iter().position(|(d, _)| d == dir) {
        let (_, wait) = armed.swap_remove(i);
        drop(armed);
        let _ = wait.recv();
    }
}

#[cfg(test)]
fn hold_open(dir: &std::path::Path) {
    let mut armed = HOLD_OPEN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(i) = armed.iter().position(|(d, _)| d == dir) {
        let (_, wait) = armed.swap_remove(i);
        drop(armed);
        let _ = wait.recv();
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
                // What the open partitions leave of --engine-memory, never past it (perf287's
                // P2 on #1755): at start they split all of it, so a node needs headroom for
                // a partition opened later.
                let held: u64 = open_now
                    .values()
                    .filter_map(|s| s.part_opt().and_then(|p| p.engine_memory))
                    .map(|m| m.bytes())
                    .sum();
                let left = total.saturating_sub(held);
                let asked = map
                    .partitions
                    .iter()
                    .find(|p| p.id == id)
                    .map_or(0, |p| p.memory_bytes);
                let bytes = if asked > 0 { asked.min(left) } else { left };
                let memory = Memory::of(bytes, share.shards).map_err(|least| {
                    ApiError::failed_precondition(format!(
                        "partition {id} can't open here: --engine-memory is {total} bytes, the \
                         open partitions hold {held} of it, which leaves {left}, and its {} \
                         shards' memtables need at least {least}. Restart this node to re-split \
                         --engine-memory across its partitions (the map already gives it {id}), \
                         or start it with headroom",
                        share.shards
                    ))
                })?;
                Some(memory)
            }
        };
        // Opening from here: `Status` shows it so, and doesn't read its file (#1770).
        let _opening = Opening::mark(self, id);
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
                        #[cfg(test)]
                        hold_open(&node.dir);
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
        .await?;
        let mut opened = opened?.pop().ok_or_else(|| {
            ApiError::failed_precondition(format!(
                "partition {id}'s file is stamped at an epoch above the map's {epoch}: a \
                 restore or relocation that stopped before its map was set. Run it again"
            ))
        })?;
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
        remask(&opener.placement, &parts, &format!("partition {id} opened"));
        cluster.parts.store(Arc::new(parts));
        // Its schema follows the map before it's unfenced (D317 ticket 5): the node's map
        // now, not the one the open began with, which a schema change may have passed while
        // the engine opened (audit120's F6). The reconcile covers every open partition.
        let node = Arc::clone(self);
        tokio::task::spawn_blocking(move || drop(node.reconcile_current(None))).await?;
        Ok(())
    }

    /// Confirms this node's start-fenced partitions on its current map, after a partition
    /// opened here. Not the map the open began with: a map installed here since (a table
    /// created at the writer, say) makes that one older than the node's, which
    /// [`confirm`](Self::confirm) ignores, and the newer map's own `open_missing` skipped
    /// the partition because it was already open. Nothing else confirms a map set at this
    /// node, so the partition stayed fenced until a peer's push or an `unfence` (audit120's
    /// P2-C2). Read again if yet another map landed meanwhile.
    fn confirm_current(&self) {
        while let Ok(map) = self.get_map() {
            self.confirm(&map);
            if self.state().version() <= map.map_version {
                break;
            }
        }
    }

    /// Opens every partition the newest map gives this node that isn't open here, each
    /// unfenced once the node's map confirms it (a relocation's new owner serves at once). A
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
                    #[cfg(test)]
                    hold_confirm(&self.dir);
                    self.confirm_current();
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
