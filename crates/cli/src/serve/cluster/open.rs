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
use crate::serve::service::Service;

/// The server a node serves through, set once it's built ([`attach`]): what the node needs
/// to reconcile its partitions' schema and to open and close them while it serves.
pub(crate) struct Server {
    /// The node's server: its parts, its node-wide service and the feed runtimes. Weak, since
    /// the server holds the node.
    pub(crate) shared: Weak<Shared>,
    pub(crate) placement: Arc<Placement>,
    pub(crate) backups: Arc<Backups>,
    /// The options every database the server opens takes
    /// ([`served_options`](crate::serve::served_options)); a partition's are these with its
    /// share and memory ([`options_for`]).
    pub(crate) served: pigeonhole::Options,
    /// `--shards` and `--compaction-threads`: the node's totals, split across its partitions.
    pub(crate) shards: usize,
    pub(crate) compaction: usize,
    /// `--engine-memory`, split as at the start.
    pub(crate) engine_memory: Option<u64>,
    /// The node's reserved compaction CPUs, as its start reserved them for several
    /// partitions (ICR 0134); `None` for one partition, none, or no pinning. A partition
    /// opened while serving confines its compaction there (#1888).
    pub(crate) reserved: Option<Vec<usize>>,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("shards", &self.shards)
            .field("compaction", &self.compaction)
            .field("engine_memory", &self.engine_memory)
            .finish_non_exhaustive()
    }
}

/// Reapplies the server's masks for the shard CPUs of `parts`, the partitions open now
/// (D317 ticket 6), and says so. The runtimes keep the thread counts they started with
/// (Tokio can't resize one): a mask that shrank has more of them per CPU (cli692 on #1745).
///
/// Its sweep reads `/proc/self/task` and makes a syscall per thread, so async callers use
/// [`remask_off_async`] (simp724's S1 on #1871).
pub(crate) fn remask(placement: &Placement, parts: &crate::serve::router::PartMap, why: &str) {
    let mut pinned: Vec<usize> = parts
        .values()
        .filter_map(|s| s.part_opt())
        .flat_map(|p| p.db.pinned_shard_cpus())
        .collect();
    pinned.sort_unstable();
    pinned.dedup();
    let previous = placement.current();
    let masks = placement.reapply(&pinned);
    // Every other thread too, the engines' unpinned ones and our long-running ones, at once
    // (#1762): only the engines' own pins, the shards' and compaction's, stay where they are.
    let mut engine_pins: Vec<usize> = parts
        .values()
        .filter_map(|s| s.part_opt())
        .flat_map(|p| {
            let mut cpus = p.db.pinned_shard_cpus();
            cpus.extend(p.db.pinned_compaction_cpus());
            cpus
        })
        .collect();
    engine_pins.sort_unstable();
    engine_pins.dedup();
    let swept = placement.sweep(&engine_pins, &previous);
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
         and hub {}, blocking {}, and {swept} other threads moved to the blocking mask; the \
         thread counts stay as the server started them (restart to size them again)",
        pinned.len(),
        on(placement.io_threads, &masks.network),
        on(placement.feed_threads, &masks.feed),
        on(placement.hub_threads, &masks.hub),
        on(placement.blocking_threads, &masks.blocking),
    );
}

/// [`remask`] on the blocking pool, awaited: an open or close on a request worker never
/// runs the sweep there. A failed task (a panic in the sweep) is logged; the masks were
/// published before it.
pub(crate) async fn remask_off_async(
    placement: &Arc<Placement>,
    parts: &crate::serve::router::PartMap,
    why: String,
) {
    let (placement, parts) = (Arc::clone(placement), parts.clone());
    if let Err(e) = tokio::task::spawn_blocking(move || remask(&placement, &parts, &why)).await {
        eprintln!("phdb serve: the thread mask update failed: {e}");
    }
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
/// partition opened, before it confirms it, until the sender sends or is dropped. The wait
/// is on a blocking thread: on a runtime worker it stopped every task queued behind it on
/// that worker (all of them with one worker), and the test hung instead of failing (#1865).
#[cfg(test)]
pub(crate) static HOLD_CONFIRM: std::sync::Mutex<
    Vec<(std::path::PathBuf, std::sync::mpsc::Receiver<()>)>,
> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
async fn hold_confirm(dir: &std::path::Path) {
    let wait = {
        let mut armed = HOLD_CONFIRM
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(i) = armed.iter().position(|(d, _)| d == dir) else {
            return;
        };
        armed.swap_remove(i).1
    };
    let _ = tokio::task::spawn_blocking(move || wait.recv()).await;
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

/// A partition's options: the server's, with the partition's share of the node's budget and
/// its memory, at the node's start and for a partition opened while it serves.
pub(crate) fn options_for(
    served: &pigeonhole::Options,
    share: Share,
    memory: Option<Memory>,
) -> pigeonhole::Options {
    let options = super::partition_options(served.clone(), share);
    match memory {
        Some(m) => m.apply(options),
        None => options,
    }
}

/// Gives `node` the server it serves through (a node's start, and the test rig's): from here
/// it reconciles its partitions' schema to every map it takes, on the installing thread
/// before the map's routes serve, and opens and closes partitions while it serves. Its
/// partitions are reconciled once now, to the map the node starts with.
pub(crate) fn attach(node: &Arc<Node>, server: Server) {
    let _ = node.server.set(server);
    drop(node.reconcile_current(None));
}

/// Where a partition opened while serving confines its compaction threads (#1888): the
/// node's `reserved` compaction CPUs (several partitions at start, ICR 0134), else the open
/// partitions' own pinned compaction CPUs (one at start), else `None`: a node that held no
/// partition, or where nothing pins, places it as a start with one partition would. The
/// open's share decides the thread count; with none (`--compaction-threads 0`) there's
/// nothing to confine, whatever this returns.
pub(crate) fn confine_compaction(
    reserved: Option<&[usize]>,
    open: impl Iterator<Item = Vec<usize>>,
) -> Option<Vec<usize>> {
    if let Some(r) = reserved {
        return Some(r.to_vec());
    }
    let mut cpus: Vec<usize> = open.flatten().collect();
    cpus.sort_unstable();
    cpus.dedup();
    (!cpus.is_empty()).then_some(cpus)
}

/// One open partition's caches in a node's re-split of `--engine-memory` (#1761): from `old`
/// to `new`, memtables unchanged.
pub(crate) struct Resize {
    pub(crate) id: String,
    pub(crate) service: Arc<Service>,
    pub(crate) old: Memory,
    pub(crate) new: Memory,
}

/// The caches a re-split shrank before a partition's open: grown back to what they were if
/// the open doesn't finish (dropped without [`keep`](Self::keep)). A grow evicts nothing, so
/// the drop does it in place.
struct Resized(Vec<(Arc<Service>, Memory)>);

impl Resized {
    /// Shrinks each cache in `plan` that gets smaller, on the blocking pool, and says so
    /// (`why`), with any partition whose open SSTs pin more of its block cache than its new
    /// share (simp724's note on #1890: those blocks stay until the SSTs close).
    async fn shrink(plan: &[Resize], why: &str) -> Result<Self, ApiError> {
        let shrinks: Vec<(String, Arc<Service>, Memory, Memory)> = plan
            .iter()
            .filter(|r| r.new.caches() < r.old.caches())
            .map(|r| (r.id.clone(), Arc::clone(&r.service), r.old, r.new))
            .collect();
        if shrinks.is_empty() {
            return Ok(Self(Vec::new()));
        }
        // The guard first (simp724's N1 on #1891): a shrink that fails part way (a panic
        // in the blocking task) returns the error, and dropping the guard puts every cache
        // back, a not-yet-shrunk one to the size it already has.
        let guard = Self(
            shrinks
                .iter()
                .map(|(_, s, old, _)| (Arc::clone(s), *old))
                .collect(),
        );
        let why = why.to_owned();
        tokio::task::spawn_blocking(move || apply(&shrinks, &why)).await?;
        Ok(guard)
    }

    /// The open finished: the shrinks stand.
    fn keep(mut self) {
        self.0.clear();
    }

    /// Grows each cache in `plan` that gets larger (or stays the same size with a new
    /// split), on the blocking pool, and says so (`why`): after a partition's open or close.
    pub(crate) async fn grow(plan: &[Resize], why: &str) {
        let grows: Vec<(String, Arc<Service>, Memory, Memory)> = plan
            .iter()
            .filter(|r| r.new.caches() >= r.old.caches())
            .map(|r| (r.id.clone(), Arc::clone(&r.service), r.old, r.new))
            .collect();
        if grows.is_empty() {
            return;
        }
        let why = why.to_owned();
        if let Err(e) = tokio::task::spawn_blocking(move || apply(&grows, &why)).await {
            eprintln!("phdb serve: a cache resize failed: {e}");
        }
    }
}

/// Grows the caches back in place, on whatever thread drops the guard: an async worker, or
/// a cancelled open's. That's fine only because a grow evicts nothing (simp724's N2 on
/// #1891): never add a step here that can block, evict or do I/O.
impl Drop for Resized {
    fn drop(&mut self) {
        for (s, old) in self.0.drain(..) {
            if let Some(p) = s.part_opt() {
                p.resize_caches(old);
            }
        }
    }
}

/// After partition `closed` closed, the partitions left in `parts` take their shares of
/// `--engine-memory` (`total`) back: [`resplit`](crate::serve::placement::resplit) for the
/// smaller set, applied in full (the closed engine's memory is gone, so a grow never passes
/// the total). A split that can't be worked out (a map override past the total, say) leaves
/// the caches as they are, and says so.
pub(crate) async fn resplit_after_close(
    node: &Node,
    total: u64,
    parts: &crate::serve::router::PartMap,
    closed: &str,
) {
    let map = node.get_map().ok();
    let asked = |pid: &str| {
        map.as_ref()
            .and_then(|m| m.partitions.iter().find(|p| p.id == pid))
            .map_or(0, |p| p.memory_bytes)
    };
    let open: Vec<(String, Arc<Service>, Memory, u64)> = parts
        .iter()
        .filter_map(|(pid, s)| {
            let m = s.part_opt()?.engine_memory()?;
            Some((pid.clone(), Arc::clone(s), m, asked(pid)))
        })
        .collect();
    let now: Vec<(Memory, u64)> = open.iter().map(|(_, _, m, o)| (*m, *o)).collect();
    let kept = match crate::serve::placement::resplit(total, &now, None) {
        Ok((kept, _)) => kept,
        Err(why) => {
            eprintln!("phdb serve: partition {closed} closed; --engine-memory not re-split: {why}");
            return;
        }
    };
    let changes: Vec<(String, Arc<Service>, Memory, Memory)> = open
        .into_iter()
        .zip(kept)
        .filter(|((_, _, old, _), new)| new != old)
        .map(|((id, s, old, _), new)| (id, s, old, new))
        .collect();
    if changes.is_empty() {
        return;
    }
    let why = format!("partition {closed} closed");
    if let Err(e) = tokio::task::spawn_blocking(move || apply(&changes, &why)).await {
        eprintln!("phdb serve: a cache resize failed: {e}");
    }
}

/// Resizes each partition's caches to its new memory, in one log line (blocking: a shrink
/// evicts).
fn apply(changes: &[(String, Arc<Service>, Memory, Memory)], why: &str) {
    let mut said = Vec::with_capacity(changes.len());
    for (id, s, old, new) in changes {
        let Some(p) = s.part_opt() else { continue };
        p.resize_caches(*new);
        said.push(format!(
            "{id} caches {} -> {} MiB",
            old.caches() >> 20,
            new.caches() >> 20
        ));
        let pinned = p.db.engine_metrics().block_cache_pinned_bytes;
        if pinned > new.block_cache {
            eprintln!(
                "phdb serve: partition {id}'s open SSTs pin {} MiB of its block cache, more \
                 than its new {} MiB: the cache stays over it by that until they close",
                pinned >> 20,
                new.block_cache >> 20
            );
        }
    }
    eprintln!(
        "phdb serve: {why}: --engine-memory re-split: {} (memtables as opened)",
        said.join(", ")
    );
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
        let server = self
            .server
            .get()
            .ok_or_else(|| ApiError::unsupported("this node opens no partition while it serves"))?;
        let shared = server
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
        let shares = crate::serve::placement::split(server.shards, server.compaction, n);
        let share = shares[n - 1];
        let mut resplit: Vec<Resize> = Vec::new();
        let memory = match server.engine_memory {
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
                // --engine-memory re-split for the new set, as a restart would (#1761): the
                // open partitions keep their memtables and their caches shrink to make room.
                let asked = |pid: &str| {
                    map.partitions
                        .iter()
                        .find(|p| p.id == pid)
                        .map_or(0, |p| p.memory_bytes)
                };
                let open: Vec<(String, Arc<Service>, Memory, u64)> = open_now
                    .iter()
                    .filter_map(|(pid, s)| {
                        let m = s.part_opt()?.engine_memory()?;
                        Some((pid.clone(), Arc::clone(s), m, asked(pid)))
                    })
                    .collect();
                let now: Vec<(Memory, u64)> = open.iter().map(|(_, _, m, o)| (*m, *o)).collect();
                let (kept, memory) =
                    crate::serve::placement::resplit(total, &now, Some((share.shards, asked(id))))
                        .map_err(|why| {
                            ApiError::failed_precondition(format!(
                                "partition {id} can't open here: {why}"
                            ))
                        })?;
                resplit = open
                    .into_iter()
                    .zip(kept)
                    .filter(|((_, _, old, _), new)| new != old)
                    .map(|((id, service, old, _), new)| Resize {
                        id,
                        service,
                        old,
                        new,
                    })
                    .collect();
                memory
            }
        };
        // The open partitions' caches shrink before the new engine allocates, so the node
        // never holds more than --engine-memory; they grow back if the open fails (#1761).
        let shrunk = Resized::shrink(&resplit, &format!("partition {id} opening")).await?;
        // Its compaction on the node's compaction CPUs, as a start would place it (#1888):
        // the start's reserved set; on a node that started with one partition, the open
        // partitions' own compaction CPUs; on one that held none, or where nothing pins,
        // none, and it's placed as a start with one partition is. Never the free cores,
        // which the network side's mask covers. A node without compaction threads gives it
        // none (`split` with 0), so there's nothing to confine.
        let confine = confine_compaction(
            server.reserved.as_deref(),
            open_now
                .values()
                .filter_map(|s| s.part_opt())
                .map(|p| p.db.pinned_compaction_cpus()),
        );
        // Opening from here: `Status` shows it so, and doesn't read its file (#1770).
        let _opening = Opening::mark(self, id);
        // Joined from a blocking thread, so no async worker waits on the open.
        let node = Arc::clone(self);
        let (part, cluster_id) = (id.to_owned(), map.cluster_id.clone());
        let opened = tokio::task::spawn_blocking(move || {
            let Some(server) = node.server.get() else {
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
                        server.placement.enter();
                        let mut log = Vec::new();
                        let opened = super::open_partitions(
                            &node.dir,
                            Some(&cluster_id),
                            &[(part, epoch)],
                            super::Budget {
                                shares: &[share],
                                reserved: confine.as_deref(),
                                memories: memory.as_ref().map(std::slice::from_ref),
                            },
                            |s, m| options_for(&server.served, s, m),
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
        if server.placement.masked() && opened.pinning.shards_unpinned() > 0 {
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
                &server.backups,
                opened.pinning,
                opened.memory,
                gate,
            )
            .placement(&server.placement);
        if let Some(part) = service.part_opt()
            && let Some((feeds, hubs)) = shared.feed_runtimes.get()
        {
            part.feeds.run_on(feeds.clone(), hubs.clone());
        }
        let mut parts = (*cluster.parts.load_full()).clone();
        parts.insert(std::mem::take(&mut opened.id), Arc::new(service));
        remask_off_async(&server.placement, &parts, format!("partition {id} opened")).await;
        cluster.parts.store(Arc::new(parts));
        // Opened: the shrinks stand, and any cache the re-split grew grows now (#1761).
        shrunk.keep();
        Resized::grow(&resplit, &format!("partition {id} opened")).await;
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
        if self.server.get().is_none() {
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
                    hold_confirm(&self.dir).await;
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
        if self.server.get().is_none() {
            return;
        }
        let (Some(node), Ok(rt)) = (self.me.upgrade(), tokio::runtime::Handle::try_current())
        else {
            return;
        };
        rt.spawn(async move { node.open_missing().await });
    }
}
