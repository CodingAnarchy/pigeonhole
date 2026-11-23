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
    /// `--change-feed`, the node's total, split by shards as at the start (#1786).
    pub(crate) change_feed: Option<u64>,
    /// The node's reserved compaction CPUs, as its start reserved them for several
    /// partitions (ICR 0134); `None` for one partition, none, or no pinning. A partition
    /// opened while serving confines its compaction there (#1888).
    pub(crate) reserved: Option<Vec<usize>>,
    /// `--replica-run`: a replica copy's run of events, at most (D328).
    pub(crate) replica_run: usize,
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
        node.fences.set_opening(id, true);
        Self {
            node,
            id: id.to_owned(),
        }
    }
}

impl Drop for Opening<'_> {
    fn drop(&mut self) {
        self.node.fences.set_opening(&self.id, false);
    }
}

/// A hold seam (tests): the next open in each listed cluster directory waits, marked as
/// opening, before its engine opens, until the sender sends or is dropped. One entry per
/// directory, so tests running at once don't take each other's.
#[cfg(test)]
pub(crate) static HOLD_OPEN: std::sync::Mutex<
    Vec<(std::path::PathBuf, std::sync::mpsc::Receiver<()>)>,
> = std::sync::Mutex::new(Vec::new());

/// An inspection seam (tests): how many passes of `open_missing` have finished in each
/// cluster directory. A test that opens a partition itself waits out the pass its map install
/// started, which would otherwise open the same partition just before or after it (#1928).
#[cfg(test)]
static MISSING_PASSES: std::sync::Mutex<Vec<(std::path::PathBuf, u64)>> =
    std::sync::Mutex::new(Vec::new());

/// The passes of `open_missing` finished in cluster directory `dir` ([`MISSING_PASSES`]).
#[cfg(test)]
pub(crate) fn missing_passes(dir: &std::path::Path) -> u64 {
    MISSING_PASSES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .find(|(d, _)| d == dir)
        .map_or(0, |(_, n)| *n)
}

/// Counts a pass of `open_missing` in its directory as it ends, however it ends.
#[cfg(test)]
struct MissingPass<'a>(&'a std::path::Path);

#[cfg(test)]
impl Drop for MissingPass<'_> {
    fn drop(&mut self) {
        let mut passes = MISSING_PASSES
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match passes.iter_mut().find(|(d, _)| d == self.0) {
            Some((_, n)) => *n += 1,
            None => passes.push((self.0.to_path_buf(), 1)),
        }
    }
}

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

/// A partition's options: the server's, with the partition's share of the node's budget, its
/// memory and its share of `--change-feed` (`feed`, bytes), at the node's start and for a
/// partition opened while it serves. `keep_last` (a partition opened while serving) keeps at
/// least the history its feed's last session kept: one moved here with its history keeps it
/// until the node restarts, under a share that may be smaller (#1786, simp724's R1).
pub(crate) fn options_for(
    served: &pigeonhole::Options,
    share: Share,
    memory: Option<Memory>,
    feed: Option<u64>,
    keep_last: bool,
) -> pigeonhole::Options {
    let mut options = super::partition_options(served.clone(), share);
    if let Some(bytes) = feed {
        options = options
            .change_feed(pigeonhole::FeedRetention::bytes(bytes))
            .change_feed_keep_last(keep_last);
    }
    match memory {
        Some(m) => m.apply(options),
        None => options,
    }
}

/// Logs what the node's partitions keep of `--change-feed` (`total`) once they're open
/// (#1786), and `why` says when:
/// - each partition whose history is shorter than its feed's last session asked for: the
///   oldest is gone, and a subscriber or token further back answers `FeedExpired`. It
///   compares a request with what's in effect now, so a small drop can go unsaid, but none
///   is invented (feed1248's N1);
/// - the node over its total: the engine's floor of two WAL segments a stream, or a
///   partition opened while serving that kept the history it came with.
pub(crate) fn log_feed(total: u64, parts: &[(&str, &pigeonhole::Pigeonhole)], why: &str) {
    let mut kept = 0u64;
    for (id, db) in parts {
        let Ok(info) = db.info() else { continue };
        let Some(feed) = info.feed else { continue };
        kept = kept.saturating_add(feed.retention_bytes);
        if let Ok(Some(before)) = db.feed_previous_retention()
            && before > feed.retention_bytes
        {
            eprintln!(
                "phdb serve: partition {id}'s change feed keeps {} bytes of history over its {} \
                 shards, its share of --change-feed {total}, where its last open kept {before}: \
                 the oldest history is gone, and a subscriber or token further back answers \
                 FeedExpired ({why})",
                feed.retention_bytes, info.shards
            );
        }
    }
    if kept > total {
        eprintln!(
            "phdb serve: the node's partitions keep {kept} bytes of change-feed history, over \
             --change-feed {total} ({why}): each keeps at least two WAL segments a shard, and a \
             partition opened while serving keeps the history it came with until the node \
             restarts"
        );
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
        self.open_as(id, false).await
    }

    /// Opens this node's replica copy of partition `id` (D328), which the map lists this
    /// node in `replicas` for, as [`open_part`](Self::open_part) opens an owner's, with these
    /// differences:
    /// - **its file:** `DIR/P.phdb`, created empty if missing (its follower's bootstrap
    ///   fills it). One that isn't a replica's and has tables (an old owner's copy) is
    ///   refused, as a single node's `--replica-of` refuses one: `drop-partition` moves it
    ///   aside;
    /// - **no feed of its own, no lease, no schema reconcile:** its tables and rows arrive
    ///   by its owner's feed, and every write is refused (`ReplicaReadOnly`). Its gate is
    ///   never in the node's lease rounds (simp724's S3);
    /// - **its follower** starts once it's open.
    pub(crate) async fn open_replica(self: &Arc<Self>, id: &str) -> Result<(), ApiError> {
        self.open_as(id, true).await
    }

    async fn open_as(self: &Arc<Self>, id: &str, replica: bool) -> Result<(), ApiError> {
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
        let copies_now = cluster.replicas.load_full();
        if open_now.contains_key(id) || copies_now.contains_key(id) {
            return Ok(());
        }
        let map = self.get_map()?;
        let epoch = match map.partitions.iter().find(|p| p.id == id) {
            Some(p) if replica && p.replicas.contains(&self.id) => p.epoch,
            Some(_) if replica => {
                return Err(ApiError::failed_precondition(format!(
                    "the map doesn't list this node as a replica of partition {id:?}"
                )));
            }
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
        // Replica copies are whole engines too, so they count (D328).
        let n = open_now.len() + copies_now.len() + 1;
        let shares = crate::serve::placement::split(server.shards, server.compaction, n);
        let share = shares[n - 1];
        // Its share of --change-feed is by the new set's shards (#1786).
        let all_shards: usize = shares.iter().map(|s| s.shards).sum();
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
                    .chain(copies_now.iter().map(|(pid, c)| (pid, &c.service)))
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
                            replica,
                            super::Budget {
                                shares: &[share],
                                reserved: confine.as_deref(),
                                memories: memory.as_ref().map(std::slice::from_ref),
                            },
                            |s, m| {
                                // A replica copy keeps no feed of its own (D328).
                                let feed = server.change_feed.filter(|_| !replica).map(|t| {
                                    crate::serve::placement::feed_share(t, s.shards, all_shards)
                                });
                                options_for(&server.served, s, m, feed, true)
                            },
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
        if replica {
            return self
                .serve_replica(id, opened, &shared, cluster, server, (shrunk, &resplit))
                .await;
        }
        // Reopened: its files change from here, so the hashes a close kept go (feed1248),
        // in the step that makes it served.
        let gate = self.open_gate(id, epoch);
        let config = crate::serve::service::PartitionConfig {
            backups: Arc::clone(&server.backups),
            placement: Some(Arc::clone(&server.placement)),
            pinning: opened.pinning,
            engine_memory: Some(opened.memory),
        };
        let service =
            shared
                .router
                .service
                .for_partition(opened.db.clone(), id, Arc::clone(&gate), config);
        // A text index rebuild stopped here or before a move carries on once it takes writes
        // (#1756).
        service.resume_text_rebuilds();
        if let Some(part) = service.part_opt()
            && let Some((feeds, hubs)) = shared.feed_runtimes.get()
        {
            part.feeds.run_on(feeds.clone(), hubs.clone());
        }
        let mut parts = (*cluster.parts.load_full()).clone();
        parts.insert(std::mem::take(&mut opened.id), Arc::new(service));
        remask_off_async(&server.placement, &parts, format!("partition {id} opened")).await;
        // D331: its range gate, from its file's moved-away rows and the map, then it joins
        // the node's parts, both under the install lock (simp724's S1): an install either
        // ran before (its map is the one armed) or runs after (it sees the partition).
        {
            let (node, db, id, slot) = (
                Arc::clone(self),
                opened.db.clone(),
                id.to_owned(),
                Arc::clone(&cluster.parts),
            );
            let parts = Arc::new(parts);
            tokio::task::spawn_blocking(move || {
                node.arm_ranges(&gate, &id, &db, || slot.store(parts))
            })
            .await?
            .map_err(|e| ApiError::internal(format!("partition's range gate: {e}")))?;
        }
        // Opened: the shrinks stand, and any cache the re-split grew grows now (#1761).
        shrunk.keep();
        Resized::grow(&resplit, &format!("partition {id} opened")).await;
        // What the partitions keep of --change-feed now (#1786): the new one's share, or the
        // history it came with, which can put the node over its total until a restart.
        if let Some(total) = server.change_feed {
            let parts = cluster.parts.load_full();
            let why = format!("partition {id} opened");
            tokio::task::spawn_blocking(move || {
                let dbs: Vec<(&str, &pigeonhole::Pigeonhole)> = parts
                    .iter()
                    .filter_map(|(id, s)| s.part_opt().map(|p| (id.as_str(), &p.db)))
                    .collect();
                log_feed(total, &dbs, &why);
            })
            .await?;
        }
        // Its schema follows the map before it's unfenced (D317 ticket 5): the node's map
        // now, not the one the open began with, which a schema change may have passed while
        // the engine opened (audit120's F6). The reconcile covers every open partition.
        let node = Arc::clone(self);
        tokio::task::spawn_blocking(move || drop(node.reconcile_current(None))).await?;
        Ok(())
    }

    /// Serves `opened` as this node's replica copy of partition `id` (D328), its engine
    /// open: a read-only service with its follower, in the node's replica copies (not its
    /// partitions, so no owner's path sees it: lease rounds, reconciles, `Info`'s owners).
    /// Its gate is the node's lease clock's, never registered, so it's never leased.
    async fn serve_replica(
        self: &Arc<Self>,
        id: &str,
        mut opened: super::Opened,
        shared: &Arc<Shared>,
        cluster: &crate::serve::router::Cluster,
        server: &Server,
        (shrunk, resplit): (Resized, &[Resize]),
    ) -> Result<(), ApiError> {
        let gate =
            crate::serve::gate::WriteGate::opened(id, false, Arc::clone(self.grants.clock()));
        let config = crate::serve::service::PartitionConfig {
            backups: Arc::clone(&server.backups),
            placement: Some(Arc::clone(&server.placement)),
            pinning: opened.pinning,
            engine_memory: Some(opened.memory),
        };
        let follower = crate::serve::replica::Replica::of_partition(self, id, server.replica_run);
        let service = Arc::new(
            shared
                .router
                .service
                .for_partition(opened.db.clone(), id, gate, config)
                .replica(Arc::clone(&follower)),
        );
        // The network side's mask leaves its shard cores out too.
        let mut all = (*cluster.parts.load_full()).clone();
        for (pid, c) in cluster.replicas.load().iter() {
            all.insert(pid.clone(), Arc::clone(&c.service));
        }
        all.insert(id.to_owned(), Arc::clone(&service));
        remask_off_async(&server.placement, &all, format!("replica of {id} opened")).await;
        let mut copies = (*cluster.replicas.load_full()).clone();
        copies.insert(
            std::mem::take(&mut opened.id),
            crate::serve::router::ReplicaCopy {
                service: Arc::clone(&service),
                follower: Arc::clone(&follower),
            },
        );
        // D331 PR 3: its range gate, from its file's rows and the map, then it joins the
        // node's copies, under the install lock, as an owner's open does (simp724's S1).
        {
            let (node, follower, db, slot) = (
                Arc::clone(self),
                Arc::clone(&follower),
                opened.db.clone(),
                Arc::clone(&cluster.replicas),
            );
            tokio::task::spawn_blocking(move || {
                let _one = node
                    .installing
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                follower.arm_ranges(&db)?;
                slot.store(Arc::new(copies));
                Ok::<_, String>(())
            })
            .await?
            .map_err(|e| ApiError::internal(format!("the replica copy's range gate: {e}")))?;
        }
        shrunk.keep();
        Resized::grow(resplit, &format!("replica of {id} opened")).await;
        follower.spawn_on(service, shared.limits.stream_idle);
        Ok(())
    }

    /// Closes this node's replica copy of `id` (D328), if it has one: its follower stops
    /// (an apply in flight finishes), and its engine closes. Its file stays, for a later
    /// replica here to resume from, or for `drop-partition` to move aside.
    pub(crate) async fn close_replica(self: &Arc<Self>, id: &str) -> Result<(), ApiError> {
        let _one = self.opening.lock().await;
        let Some(server) = self.server.get() else {
            return Ok(());
        };
        let Some(shared) = server.shared.upgrade() else {
            return Ok(());
        };
        let Some(cluster) = shared.router.cluster.as_ref() else {
            return Ok(());
        };
        let mut copies = (*cluster.replicas.load_full()).clone();
        let Some(copy) = copies.remove(id) else {
            return Ok(());
        };
        cluster.replicas.store(Arc::new(copies));
        copy.follower.stop().await;
        let service = copy.service;
        tokio::task::spawn_blocking(move || match service.part_opt() {
            Some(part) => {
                part.close_users();
                part.db.clone().close()
            }
            None => Ok(()),
        })
        .await?
        .map_err(|e| ApiError::from_engine(&e))?;
        eprintln!("phdb serve: replica of partition {id} closed on this node");
        Ok(())
    }

    /// A partition opened here asks for its lease at once (D326), and the node's renewals
    /// run if they don't yet: a move's new owner serves only once a majority of the map's
    /// nodes hold a map naming it and grant it, so it and the old owner never both serve
    /// (#1760). Spawned: the open answers at once.
    fn confirm_opened(&self) {
        let Some(node) = self.me.upgrade() else {
            return;
        };
        node.renew_leases();
        node.confirm_soon();
    }

    /// Opens every partition the newest map gives this node that isn't open here, each
    /// unfenced once the node's map confirms it (a relocation's new owner serves at once). A
    /// partition that fails to open is reported and left to the next map, or the operator.
    pub(crate) async fn open_missing(self: &Arc<Self>) {
        #[cfg(test)]
        let _pass = MissingPass(&self.dir);
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
        self.sync_replicas(&map).await;
        self.reconcile_split_files(&map).await;
        for id in mine {
            // Open, or closed by the operator: a close holds until `unfence`.
            // One read, so a reopen between two can't make it look like neither.
            if matches!(
                self.fences.now(&id, |_| ()),
                super::fence::Now::Served(_) | super::fence::Now::Closed
            ) {
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
                    self.confirm_opened();
                }
                Err(e) => eprintln!(
                    "phdb serve: partition {id}: map version {} gives it to this node, and it \
                     didn't open: {}",
                    map.map_version, e.message
                ),
            }
        }
    }

    /// Whether this node has a replica copy of partition `id` open (D328).
    pub(crate) fn replica_open(&self, id: &str) -> bool {
        self.server
            .get()
            .and_then(|s| s.shared.upgrade())
            .is_some_and(|sh| {
                sh.router
                    .cluster
                    .as_ref()
                    .is_some_and(|c| c.replicas.load().contains_key(id))
            })
    }

    /// Brings this node's replica copies in line with `map` (D328): each one the map no
    /// longer lists here closes (a map that drops it, or gives this node the partition: a
    /// move or promotion here, simp724's S4), then each it lists here opens. A copy that
    /// fails to open is reported and tried again at the next map.
    async fn sync_replicas(self: &Arc<Self>, map: &crate::serve::proto::ClusterMap) {
        let listed = |id: &str| {
            map.partitions
                .iter()
                .any(|p| p.id == id && p.replicas.contains(&self.id))
        };
        let open: Vec<String> = self
            .server
            .get()
            .and_then(|s| s.shared.upgrade())
            .and_then(|sh| {
                sh.router
                    .cluster
                    .as_ref()
                    .map(|c| c.replicas.load().keys().cloned().collect())
            })
            .unwrap_or_default();
        for id in open.iter().filter(|id| !listed(id)) {
            if let Err(e) = self.close_replica(id).await {
                eprintln!(
                    "phdb serve: replica of partition {id}: map version {} drops it here, and \
                     it didn't close: {}",
                    map.map_version, e.message
                );
            }
        }
        for p in map
            .partitions
            .iter()
            .filter(|p| p.replicas.contains(&self.id))
        {
            if open.contains(&p.id) {
                continue;
            }
            match self.open_replica(&p.id).await {
                Ok(()) => eprintln!(
                    "phdb serve: replica of partition {} opened on map version {}",
                    p.id, map.map_version
                ),
                Err(e) => eprintln!(
                    "phdb serve: replica of partition {}: map version {} lists this node, and \
                     it didn't open: {}",
                    p.id, map.map_version, e.message
                ),
            }
        }
    }

    /// The server's feed delivery runtime, once it set one (`run_on`): where a cluster
    /// subscription's merge runs, off the threads that serve requests (#1798 (a)). `None`
    /// before the server is built, or without a server (some tests): the caller's runtime.
    pub(crate) fn feed_runtime(&self) -> Option<tokio::runtime::Handle> {
        let shared = self.server.get()?.shared.upgrade()?;
        shared.feed_runtimes.get().map(|(feed, _)| feed.clone())
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
