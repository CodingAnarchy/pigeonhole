//! Where the network side's threads run (perf287's R1 on D313): off the CPUs the engine
//! pinned its shard threads to, since a client thread preempting a pinned shard driver is
//! what raised `Buffered` put p99.9 (#1206), and a shard's SMT sibling doubled p99 (#1244).
//! On Linux each runtime's threads get an affinity mask ([`RuntimeMasks`]); elsewhere, or
//! when nothing was pinned, nothing changes.
//!
//! - **The request runtime's workers** (the puts' network threads) run on the free cores
//!   and the shards' SMT siblings, never on a shard's own CPU. Without the siblings, puts
//!   waited on the free CPUs' run queues behind the feed side while the siblings sat idle:
//!   puts were held to 33K/s at 16 hubs × 16 subscribers with 1.35 cores of network threads
//!   waiting (#1734, perf287's g80b), and the coordinator chose the siblings over unmasking.
//! - **The feed and hub runtimes** stay on the free cores only. On the siblings too, they
//!   raised subscriber lag p99 at 16x16 @ 20K from 5.6 ms to 34 ms; on the free cores it
//!   was 8.3 ms, with the same put rate at saturation and a lower put p99 (perf287's g82).
//! - **The server's own heavy threads** (bulk loads, index builds and rebalancers,
//!   `Replication.Follow`, embedding) and the request runtime's blocking pool (scans,
//!   `read_row`, `/mcp` and the other sync client calls) stay on the free cores only:
//!   [`Placement::enter`].
//! - **A thread inherits its creator's mask.** A `std::thread` spawned from a request
//!   worker starts on the shards' siblings too, so a heavy one calls
//!   [`Placement::enter`] first, as the backups, bulk loads, index builds, text rebuilds and
//!   `Replication.Follow` do (ai1249's S2 on #1738). An engine thread started from a
//!   worker inherits the wide mask too, so a partition opened at run time (D317 ticket 6)
//!   must open off the workers.

use std::sync::Arc;

use pigeonhole::Pigeonhole;

/// Each runtime's CPUs, sorted; `None` runs it where the OS puts it (`--affinity off`,
/// nothing pinned, or too few free cores). One function computes them
/// ([`Placement::masks`]), for the start and for a recompute when a cluster node opens or
/// closes a partition (D317 ticket 6).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RuntimeMasks {
    /// The request runtime's workers. Its blocking pool, which runs heavy client calls,
    /// takes `blocking` ([`Placement::enter_network`]).
    pub network: Option<Vec<usize>>,
    /// The server's own heavy threads, which start through [`Placement::enter`], and the
    /// request runtime's blocking pool.
    pub blocking: Option<Vec<usize>>,
    /// The feed delivery runtime: `blocking`'s CPUs today (g82 on #1738).
    pub feed: Option<Vec<usize>>,
    /// The feed hubs' runtime. The same CPUs as `feed` today; a field of its own so the hubs
    /// can be placed apart later.
    pub hub: Option<Vec<usize>>,
}

/// The shard count `phdb serve` gives a database it creates: half the physical cores,
/// leaving the rest for the network side. A user's `--shards` wins.
pub(crate) fn default_shards() -> usize {
    (num_cpus::get_physical() / 2).max(1)
}

/// A node's compaction threads when `--compaction-threads` isn't given: the library's
/// per-database default (D289: a quarter of the physical cores, at least 1, at most one per
/// shard), computed once for the whole node rather than once per partition (perf287's P1 on
/// D317: N partitions at their defaults would ask for N times it).
pub(crate) fn default_compaction_threads(shards: usize) -> usize {
    // The physical cores this process may run on (its affinity mask and CPU quota), as the
    // library counts them for its own default, not the host's (g72 on #1703: under a
    // 12-core taskset on a 16-core box, the host's count gave 4).
    (pigeonhole_io::sys::default_shards() / 4).clamp(1, shards.max(1))
}

/// One partition's share of a node's thread budget (D317 ticket 8a, §10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Share {
    /// Its shard count, given as `default_shards`, so a partition whose change feed recorded
    /// a count keeps it (ICR 0132).
    pub shards: usize,
    /// Its compaction threads.
    pub compaction: usize,
    /// Whether they're confined to the node's compaction CPUs rather than pinned one each
    /// (ICR 0134): partitions past the node's compaction total (perf287's A1).
    pub confined: bool,
}

/// Splits a node's `shards` and `compaction` threads across `partitions` partitions, in
/// their id order: equal shares, the remainder one each to the first, at least one shard
/// each.
///
/// Pinned compaction threads never exceed `compaction` (perf287's A1): with fewer than one
/// each, the first `compaction` partitions get one pinned thread each and the rest one
/// confined to the node's compaction CPUs. `compaction` 0 runs every partition's flushes and
/// compactions on its shard threads, as `compaction_cores(0)` does.
pub(crate) fn split(shards: usize, compaction: usize, partitions: usize) -> Vec<Share> {
    let share = |total: usize, i: usize| total / partitions + usize::from(i < total % partitions);
    (0..partitions)
        .map(|i| {
            let (compaction, confined) = match compaction {
                0 => (0, false),
                c if c >= partitions => (share(c, i), false),
                c => (1, i >= c),
            };
            Share {
                shards: share(shards, i).max(1),
                compaction,
                confined,
            }
        })
        .collect()
}

/// The library's memory per database, by default: a 64 MiB memtable arena per shard, a
/// 256 MiB block cache and a 64 MiB row cache. `--engine-memory` keeps the caches' ratio.
pub(crate) const MEMTABLE_PER_SHARD: u64 = 64 << 20;
const BLOCK_CACHE: u64 = 256 << 20;
const ROW_CACHE: u64 = 64 << 20;

/// The least memtable arena per shard `--engine-memory` gives a partition (cli692's M3 on
/// D317 8a): a share that can't cover it for every shard is refused.
pub(crate) const MEMTABLE_FLOOR: u64 = 128 << 10;

/// A database's engine memory (D317 ticket 8a): its memtable arena per shard, block cache
/// and row cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Memory {
    pub shards: usize,
    pub memtable_per_shard: u64,
    pub block_cache: u64,
    pub row_cache: u64,
}

impl Memory {
    /// The library's defaults for `shards` shards.
    pub(crate) fn defaults(shards: usize) -> Self {
        Self {
            shards,
            memtable_per_shard: MEMTABLE_PER_SHARD,
            block_cache: BLOCK_CACHE,
            row_cache: ROW_CACHE,
        }
    }

    /// `share` bytes for `shards` shards (cli692's M3): the memtables first, at most the
    /// library's arena per shard and at least [`MEMTABLE_FLOOR`]; then the rest split
    /// between the block cache and the row cache in the library's ratio (4:1). `Err` with
    /// the least share that would do.
    pub(crate) fn of(share: u64, shards: usize) -> Result<Self, u64> {
        let shards = shards.max(1);
        let n = shards as u64;
        let per_shard = (share / n).min(MEMTABLE_PER_SHARD);
        if per_shard < MEMTABLE_FLOOR {
            return Err(MEMTABLE_FLOOR * n);
        }
        let rest = share - per_shard * n;
        let block_cache = rest / (BLOCK_CACHE + ROW_CACHE) * BLOCK_CACHE
            + rest % (BLOCK_CACHE + ROW_CACHE) * BLOCK_CACHE / (BLOCK_CACHE + ROW_CACHE);
        Ok(Self {
            shards,
            memtable_per_shard: per_shard,
            block_cache,
            row_cache: rest - block_cache,
        })
    }

    pub(crate) fn memtables(&self) -> u64 {
        self.memtable_per_shard * self.shards as u64
    }

    /// All of it: the memtables and both caches.
    pub(crate) fn bytes(&self) -> u64 {
        self.memtables() + self.block_cache + self.row_cache
    }

    /// `options` with this memory.
    pub(crate) fn apply(&self, options: pigeonhole::Options) -> pigeonhole::Options {
        options
            .memtable_budget(self.memtable_per_shard)
            .block_cache(usize::try_from(self.block_cache).unwrap_or(usize::MAX))
            .row_cache(usize::try_from(self.row_cache).unwrap_or(usize::MAX))
    }
}

/// Each partition's memory from a node's `--engine-memory` (`total`; `None`: unset), given
/// each partition's shard count and the map's `memory_bytes` override (0: none), in order
/// (D317 ticket 8a). Overrides are taken first, and the rest of `total` split equally
/// across the others; unset, the others take the library's defaults. `Err` names the
/// partition (by index) that can't be covered and the least it would need, or no partition
/// when the overrides together exceed the total (cli692's N1 on #1718).
pub(crate) fn memory(
    total: Option<u64>,
    parts: &[(usize, u64)],
) -> Result<Vec<Memory>, (Option<usize>, String)> {
    let overridden: u64 = parts.iter().map(|(_, m)| *m).sum();
    let rest = parts.iter().filter(|(_, m)| *m == 0).count() as u64;
    let equal = match total {
        Some(t) if t < overridden => {
            return Err((
                None,
                format!(
                    "--engine-memory {t} is less than the map's memory_bytes for this node's \
                     partitions ({overridden})"
                ),
            ));
        }
        Some(t) => Some((t - overridden) / rest.max(1)),
        None => None,
    };
    parts
        .iter()
        .enumerate()
        .map(|(i, &(shards, over))| {
            let share = match (over, equal) {
                (0, None) => return Ok(Memory::defaults(shards)),
                (0, Some(e)) => e,
                (m, _) => m,
            };
            Memory::of(share, shards).map_err(|least| {
                (
                    Some(i),
                    format!(
                        "{share} bytes of engine memory can't cover {shards} shards' memtables \
                         at {MEMTABLE_FLOOR} bytes each: it needs at least {least}"
                    ),
                )
            })
        })
        .collect()
}

/// The CPUs a node with several partitions reserves for its compaction threads (D317
/// ticket 8a, perf287's g75): the `compaction` CPUs that come after the partitions' shards,
/// the sum of their `shares` (`split` floors each at one, so with `--shards` below the
/// partition count it's more than `--shards`, cli692's N6 on #1718), in the engine's pin
/// order (one per physical core first, then their SMT siblings), so that every
/// partition's shards, pinned next, take the first ones contiguously and the network side's
/// mask stays in one place (one L3 domain on the AX102), as with one partition. The plan is
/// released at once: the engines claim their shards' CPUs themselves as they open. `None`
/// where pinning is refused (no hard affinity, a CPU quota, too few CPUs).
///
/// A partition whose change feed keeps more shards than its share pins the extra ones past
/// the planned shards, onto the reserved CPUs: rare, and the startup line shows it.
pub(crate) fn compaction_reserve(shares: &[Share], compaction: usize) -> Option<Vec<usize>> {
    let shards: usize = shares.iter().map(|s| s.shards).sum();
    if compaction == 0 {
        return None;
    }
    let plan = pigeonhole_io::sys::pin_plan(shards + compaction).ok()?;
    let reserved: Vec<usize> = (shards..shards + compaction)
        .filter_map(|i| plan.cpu(i))
        .collect();
    drop(plan);
    (reserved.len() == compaction).then_some(reserved)
}

/// The network side's threads and where they may run.
#[derive(Debug, Clone)]
pub(crate) struct Placement {
    pub io_threads: usize,
    pub blocking_threads: usize,
    /// The feed delivery runtime's workers (ticket 5's (A)).
    pub feed_threads: usize,
    /// The feed hubs' runtime's workers: a quarter of the feed threads, at least 1, or
    /// `--feed-hub-threads` (#1719). A runtime of their own, so a hub never queues behind its
    /// clients' tasks (#1397's 16 hubs × 16), placed on the same CPUs, so the OS shares them.
    /// The default keeps writers first (the coordinator's policy on #1719): in g76c, every
    /// count above it cost put p50 13–38% at moderate fan-out load and delivered fewer events
    /// when saturated. A feed-heavy server with CPUs to spare raises it.
    pub hub_threads: usize,
    /// Where each runtime runs (Linux, when the engine pinned any shard).
    pub masks: RuntimeMasks,
    /// The process's CPUs at the start, before any thread was masked, and whether masks are
    /// set at all (`--affinity`): what [`Placement::masks`] recomputes from.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    process: Vec<usize>,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    affinity: bool,
    /// For the startup line: the free cores, outside the shards' cores.
    pub free_cpus: usize,
    /// For the startup line: how many of the shards' SMT siblings the request runtime's
    /// workers also run on.
    pub sibling_cpus: usize,
    /// How the mask was built, for the startup line: `"off the shards' cores"` (their SMT
    /// siblings too), `"unpinned"`, `"unmasked (--affinity off)"`, or `"unmasked (too few
    /// free cores)"`.
    pub mode: &'static str,
    /// Why the server's threads run unmasked though the shards are pinned, for a line of its
    /// own at startup.
    pub note: Option<String>,
    /// The shard CPUs the mask leaves out, every partition's (D317 ticket 8a), and the
    /// compaction CPUs the engines pinned (ICR 0134), which it doesn't: on one node g72 cost
    /// the network side 3x its throughput for them (#1703). For the startup line.
    pub shard_cpus: usize,
    pub compaction_cpus: usize,
    /// The masks the threads take now, which a reapply replaces when a cluster node opens or
    /// closes a partition (D317 ticket 6): shared by every clone.
    live: Arc<Live>,
}

/// The masks reapplied while the server runs (D317 ticket 6, cli692's (b)): published, then
/// the generation bumped, so a thread that sees the new generation loads the new masks.
/// Each thread reapplies its runtime's mask when the generation moved: as a worker parks or
/// unparks, and as a call or blocking task of ours starts on it ([`refresh`]), since a busy
/// worker may not park for a while (the coordinator, 2026-11-11). Thread counts stay as the
/// server started them (Tokio can't resize a runtime): a mask that shrinks leaves its
/// threads more of them per CPU (cli692 on #1745).
#[derive(Debug, Default)]
struct Live {
    masks: arc_swap::ArcSwap<RuntimeMasks>,
    generation: std::sync::atomic::AtomicU64,
    /// The process's CPUs, which a thread masked to `None` gets back.
    process: Vec<usize>,
    /// The threads that follow [`refresh`] (every [`Placement::enter_as`]), by thread ID: the
    /// sweep leaves them to it (#1762, perf287's S1: names don't tell them apart).
    #[cfg(target_os = "linux")]
    registered: std::sync::Mutex<std::collections::HashSet<i32>>,
}

/// Which runtime's mask a thread takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Network,
    Blocking,
    Feed,
    Hub,
}

impl RuntimeMasks {
    fn of(&self, role: Role) -> Option<&[usize]> {
        match role {
            Role::Network => self.network.as_deref(),
            Role::Blocking => self.blocking.as_deref(),
            Role::Feed => self.feed.as_deref(),
            Role::Hub => self.hub.as_deref(),
        }
    }
}

thread_local! {
    /// This thread's placement, its role and the generation of the mask it last applied.
    static THREAD: std::cell::RefCell<Option<(Arc<Live>, Role, u64)>> =
        const { std::cell::RefCell::new(None) };
    /// This thread's entry in [`Live::registered`], removed when the thread exits: Linux
    /// reuses thread IDs, and the blocking pool retires idle threads, so a stale entry
    /// would let the sweep skip an unrelated later thread that got the same ID.
    #[cfg(target_os = "linux")]
    static REGISTERED: std::cell::RefCell<Option<Registered>> =
        const { std::cell::RefCell::new(None) };
}

/// One thread's registration; dropping it (at thread exit, or a new `enter_as`) removes it.
#[cfg(target_os = "linux")]
struct Registered {
    live: Arc<Live>,
    tid: i32,
}

#[cfg(target_os = "linux")]
impl Drop for Registered {
    fn drop(&mut self) {
        self.live
            .registered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.tid);
    }
}

/// Reapplies the calling thread's mask if a reapply published a newer one since it last
/// did: one atomic load against a thread-local when nothing changed. Called as each call
/// and blocking task of ours starts, and from the runtimes' park and unpark hooks. A thread
/// no placement entered does nothing.
pub(crate) fn refresh() {
    THREAD.with_borrow_mut(|t| {
        let Some((live, role, seen)) = t else {
            return;
        };
        let now = live.generation.load(std::sync::atomic::Ordering::Acquire);
        if now != *seen {
            *seen = now;
            apply_to(&live.process, live.masks.load().of(*role));
        }
    });
}

/// The process's thread IDs (`/proc/self/task`).
#[cfg(target_os = "linux")]
fn thread_ids() -> Vec<i32> {
    std::fs::read_dir("/proc/self/task")
        .map(|d| {
            d.filter_map(|e| e.ok()?.file_name().to_str()?.parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

/// Thread `tid`'s CPUs, sorted; `None` when it has gone, or can't be read.
#[cfg(target_os = "linux")]
fn affinity_of(tid: i32) -> Option<Vec<usize>> {
    let set = nix::sched::sched_getaffinity(nix::unistd::Pid::from_raw(tid)).ok()?;
    Some(
        (0..nix::sched::CpuSet::count())
            .filter(|&c| set.is_set(c).unwrap_or(false))
            .collect(),
    )
}

/// Masks thread `tid` to `cpus`; false when it has gone, or the OS refused.
#[cfg(target_os = "linux")]
fn set_affinity_of(tid: i32, cpus: &[usize]) -> bool {
    let mut mask = nix::sched::CpuSet::new();
    for &c in cpus {
        let _ = mask.set(c);
    }
    nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(tid), &mask).is_ok()
}

/// Masks the calling thread to `cpus` (Linux). `None` means where the OS puts it, so the
/// thread gets `process`'s CPUs back: a reapply after a recompute that lands on no
/// mask (D317 ticket 6: `--affinity off`, or too few free cores once a partition opens)
/// must not leave a thread on an old mask that may now hold a new shard's CPU (ai1249's
/// N1 on #1738). Without a process set (another OS, a node that only routes), it leaves
/// the thread be.
fn apply_to(process: &[usize], cpus: Option<&[usize]>) {
    #[cfg(target_os = "linux")]
    {
        let cpus = cpus.unwrap_or(process);
        if cpus.is_empty() {
            return;
        }
        let mut mask = nix::sched::CpuSet::new();
        for &c in cpus {
            let _ = mask.set(c);
        }
        // The calling thread: pid 0. A failure leaves the thread where the OS put it,
        // which is correct, only slower.
        let _ = nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &mask);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (process, cpus);
}

impl Placement {
    /// The threads' counts: given, or by default from the CPUs the shards don't use. The
    /// feed threads' default (a quarter, at least 1) is carved out of the network threads'
    /// default rather than added, so the two runtimes don't oversubscribe the CPUs (perf287's
    /// D2 on #1397). An explicit `--io-threads` is taken as given.
    ///
    /// Without `affinity` (`--affinity off`), the counts are the same but no mask is set: the
    /// threads run where the OS puts them (ICR 0131).
    pub(crate) fn plan(
        dbs: &[&Pigeonhole],
        io: Option<usize>,
        blocking: Option<usize>,
        feed: Option<usize>,
        hubs: Option<usize>,
        affinity: bool,
    ) -> Self {
        // Every engine's pinned shard threads: the union across a node's partitions (D317
        // ticket 8a). Their compaction threads' CPUs are counted, not left out (g72, #1703).
        let mut pinned: Vec<usize> = dbs.iter().flat_map(|db| db.pinned_shard_cpus()).collect();
        pinned.sort_unstable();
        pinned.dedup();
        let compaction_cpus = dbs.iter().map(|db| db.pinned_compaction_cpus().len()).sum();
        let process = process_cpus();
        // The counts and the startup wording; the masks come from [`Placement::masks`]
        // below, the one function a recompute also calls.
        #[cfg(target_os = "linux")]
        let Layout {
            free,
            siblings: sibling_cpus,
            mode,
            note,
            ..
        } = match &process {
            Some(process) => layout(process, &pinned, siblings, affinity),
            None => Layout::unpinned(&pinned),
        };
        #[cfg(not(target_os = "linux"))]
        let Layout {
            free,
            siblings: sibling_cpus,
            mode,
            note,
            ..
        } = Layout::unpinned(&pinned);
        let default = free.max(2);
        // A quarter. perf287 on #1397: a floor of 4 (8 network + 4 feed on a 12-CPU slice)
        // left the network side short and every split run fell behind; 9 + 3 kept up.
        let feed_threads = feed.unwrap_or(free / 4).max(1);
        let mut plan = Self {
            hub_threads: hubs.unwrap_or_else(|| feed_threads.div_ceil(4)).max(1),
            io_threads: io
                .unwrap_or(free.saturating_sub(feed_threads).max(2))
                .max(1),
            blocking_threads: blocking.unwrap_or(default).max(1),
            feed_threads,
            masks: RuntimeMasks::default(),
            process: process.unwrap_or_default(),
            affinity,
            free_cpus: free,
            sibling_cpus,
            mode,
            note,
            shard_cpus: pinned.len(),
            compaction_cpus,
            live: Arc::default(),
        };
        plan.masks = plan.masks(&pinned);
        plan.live = Arc::new(Live {
            masks: arc_swap::ArcSwap::from_pointee(plan.masks.clone()),
            generation: std::sync::atomic::AtomicU64::new(0),
            process: plan.process.clone(),
            #[cfg(target_os = "linux")]
            registered: std::sync::Mutex::default(),
        });
        plan
    }

    /// Each runtime's mask with `pinned` held by shards, from the process's CPUs at the start:
    /// the start's masks for the start's pinned CPUs, and the ones to reapply when a cluster
    /// node's set of open partitions (and so of pinned CPUs) changes (D317 ticket 6). A
    /// reapply masks each thread with [`Placement::apply`], which turns `None` back into the
    /// process's CPUs.
    pub(crate) fn masks(&self, pinned: &[usize]) -> RuntimeMasks {
        #[cfg(target_os = "linux")]
        if !self.process.is_empty() {
            return layout(&self.process, pinned, siblings, self.affinity).masks;
        }
        let _ = pinned;
        RuntimeMasks::default()
    }

    /// A cluster node that serves no partition (D317 ticket 2, cli692's (b)): no shards to
    /// keep off, so every CPU goes to the network and blocking threads, unpinned. It has no
    /// feeds; the feed and hub runtimes keep one idle thread each.
    pub(crate) fn routes_only(io: Option<usize>, blocking: Option<usize>) -> Self {
        let free = std::thread::available_parallelism().map_or(2, std::num::NonZero::get);
        Self {
            io_threads: io.unwrap_or(free).max(1),
            blocking_threads: blocking.unwrap_or(free.max(2)).max(1),
            feed_threads: 1,
            hub_threads: 1,
            masks: RuntimeMasks::default(),
            process: Vec::new(),
            affinity: false,
            free_cpus: free,
            sibling_cpus: 0,
            mode: "unpinned (routes only)",
            note: None,
            shard_cpus: 0,
            compaction_cpus: 0,
            live: Arc::default(),
        }
    }

    /// Recomputes every runtime's mask for `pinned`, the shard CPUs of the partitions open
    /// now, and publishes it: each thread takes it at its next [`refresh`] (D317 ticket 6).
    /// The new masks.
    pub(crate) fn reapply(&self, pinned: &[usize]) -> RuntimeMasks {
        let masks = self.masks(pinned);
        self.live.masks.store(Arc::new(masks.clone()));
        self.live
            .generation
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        masks
    }

    /// The masks the threads take now: the start's, or the last reapply's.
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn current(&self) -> RuntimeMasks {
        (**self.live.masks.load()).clone()
    }

    /// Whether thread `tid` is registered as following [`refresh`].
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn is_registered(&self, tid: i32) -> bool {
        self.live
            .registered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&tid)
    }

    /// Masks the calling thread to `role`'s CPUs now, and records it, so its [`refresh`]
    /// follows later reapplies.
    fn enter_as(&self, role: Role) {
        let now = self
            .live
            .generation
            .load(std::sync::atomic::Ordering::Acquire);
        apply_to(&self.live.process, self.live.masks.load().of(role));
        THREAD.with_borrow_mut(|t| *t = Some((Arc::clone(&self.live), role, now)));
        #[cfg(target_os = "linux")]
        {
            let tid = nix::unistd::gettid().as_raw();
            // Drop any earlier entry first (it removes this tid), then insert.
            REGISTERED.with_borrow_mut(|r| *r = None);
            self.live
                .registered
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(tid);
            REGISTERED.with_borrow_mut(|r| {
                *r = Some(Registered {
                    live: Arc::clone(&self.live),
                    tid,
                });
            });
        }
    }

    /// After a [`reapply`](Self::reapply): moves every thread of the process that doesn't
    /// follow [`refresh`] itself onto the blocking mask (or the process's CPUs when there's
    /// none), so the engines' unpinned threads and the server's long-running ones (a backup,
    /// a bulk load, a rebalancer, a replica's follow, candle's pool) leave CPUs a new
    /// partition's shards were just pinned to (#1762). Linux; elsewhere it does nothing.
    ///
    /// Left alone:
    /// - the threads that follow `refresh()` (registered by thread ID as they enter): they
    ///   take their runtime's mask at their next park, unpark, call or task, as before;
    /// - every thread whose affinity is a subset of `engine_pins`, the open partitions'
    ///   pinned shard and compaction CPUs: the engines' own pins (D235), one CPU or a
    ///   confined set (ICR 0134). This assumes an engine never pins a thread outside those.
    ///
    /// Two passes at most: a second when the thread count changed during the first, then it
    /// stops and says so (perf287's N1). The number of threads it moved.
    pub(crate) fn sweep(&self, engine_pins: &[usize]) -> usize {
        #[cfg(target_os = "linux")]
        {
            let target: Vec<usize> = self
                .live
                .masks
                .load()
                .of(Role::Blocking)
                .map_or_else(|| self.live.process.clone(), <[usize]>::to_vec);
            if target.is_empty() {
                return 0;
            }
            let mut moved = 0;
            let mut seen = None;
            for _ in 0..2 {
                let tids = thread_ids();
                if seen == Some(tids.len()) {
                    return moved;
                }
                seen = Some(tids.len());
                let registered = self
                    .live
                    .registered
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                for tid in tids {
                    if registered.contains(&tid) {
                        continue;
                    }
                    let Some(now) = affinity_of(tid) else {
                        continue;
                    };
                    if now == target || now.iter().all(|c| engine_pins.contains(c)) {
                        continue;
                    }
                    if set_affinity_of(tid, &target) {
                        moved += 1;
                    }
                }
            }
            if seen != Some(thread_ids().len()) {
                eprintln!(
                    "phdb serve: the thread mask sweep stopped after two passes with threads still \
                     starting: one started meanwhile takes its spawner's mask"
                );
            }
            moved
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = engine_pins;
            0
        }
    }

    /// The CPUs the server's own heavy threads may run on, as a CPU list (`2-7,10`), when the
    /// plan pins them: the value for `CANDLE_CPU_MASK`.
    pub(crate) fn cpu_list(&self) -> Option<String> {
        self.masks.blocking.as_deref().map(format_cpu_list)
    }

    /// The request runtime's CPUs, as a CPU list, when they differ from
    /// [`Placement::cpu_list`]'s: for the startup line.
    pub(crate) fn network_cpu_list(&self) -> Option<String> {
        (self.masks.network != self.masks.blocking)
            .then(|| self.masks.network.as_deref().map(format_cpu_list))
            .flatten()
    }

    /// Called on each of the server's own heavy threads as it starts (bulk loads, index
    /// builds and rebalancers, `Replication.Follow`, embedding): the free cores only.
    pub(crate) fn enter(&self) {
        self.enter_as(Role::Blocking);
    }

    /// Called on each request runtime thread as it starts. Tokio runs it on the runtime's
    /// blocking-pool threads as well as its workers, and the pool runs heavy client calls
    /// (`Service::blocking`'s scans, `read_row`, `multi_get`, text and vector calls, `/mcp`,
    /// and the replica's and cluster's sync calls): so the free cores, as
    /// [`Placement::enter`], off the shards' siblings (#1244, ai1249's S1 on #1738). A
    /// worker widens itself to `network` as it first unparks ([`Placement::unpark_network`]).
    pub(crate) fn enter_network(&self) {
        self.enter();
    }

    /// Called each time a request runtime worker unparks: the first time on a thread, it
    /// masks the thread to `network`, the free cores and the shards' siblings. Only workers
    /// unpark (Tokio's blocking pool never calls it), so the blocking pool stays on the free
    /// cores. After the first, it's a thread-local read.
    ///
    /// Nothing may call `tokio::task::block_in_place` (nothing in the workspace or its
    /// dependencies does, ai1249 on #1738): a worker that gives up its core there goes back
    /// to the blocking pool still widened, and a pool thread would then run heavy calls on
    /// the siblings. After the first, it reapplies a newer mask a reapply published (D317
    /// ticket 6), as [`refresh`] does.
    pub(crate) fn unpark_network(&self) {
        let widened = THREAD.with_borrow(|t| matches!(t, Some((_, Role::Network, _))));
        if widened {
            refresh();
        } else {
            self.enter_as(Role::Network);
        }
    }

    /// Counts `cpus`, the node's reserved compaction CPUs its partitions' compaction threads
    /// are confined to (ICR 0134), among the compaction CPUs the mask doesn't leave out, as
    /// pinned ones are.
    pub(crate) fn confined_compaction(&mut self, cpus: &[usize]) {
        self.compaction_cpus += cpus.len();
    }

    /// Whether the server's own threads are masked to the free cores ([`Placement::enter`]):
    /// pinning works here, and the shards are pinned.
    pub(crate) fn masked(&self) -> bool {
        self.masks.blocking.is_some()
    }

    /// Called on each feed runtime worker as it starts.
    pub(crate) fn enter_feed(&self) {
        self.enter_as(Role::Feed);
    }

    /// Called on each feed hub runtime worker as it starts.
    pub(crate) fn enter_hub(&self) {
        self.enter_as(Role::Hub);
    }
}

/// The process's CPUs (Linux's affinity set), read once at the start before any thread is
/// masked; `None` where there's no hard affinity or it can't be read.
fn process_cpus() -> Option<Vec<usize>> {
    #[cfg(target_os = "linux")]
    {
        use nix::sched::{CpuSet, sched_getaffinity};
        let set = sched_getaffinity(nix::unistd::Pid::from_raw(0)).ok()?;
        Some(
            (0..CpuSet::count())
                .filter(|&c| set.is_set(c).unwrap_or(false))
                .collect(),
        )
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// A plan's masks, counts and startup wording.
#[derive(Debug, PartialEq, Eq)]
struct Layout {
    /// Read through [`Placement::masks`], on Linux only.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    masks: RuntimeMasks,
    /// The free cores' CPUs, for the threads' counts.
    free: usize,
    /// The shards' SMT siblings the request runtime's workers also run on.
    siblings: usize,
    mode: &'static str,
    note: Option<String>,
}

impl Layout {
    /// No masks: nothing pinned, or no hard affinity on this OS. The counts take the CPUs no
    /// shard holds.
    fn unpinned(pinned: &[usize]) -> Self {
        Self {
            masks: RuntimeMasks::default(),
            free: std::thread::available_parallelism()
                .map_or(2, std::num::NonZero::get)
                .saturating_sub(pinned.len()),
            siblings: 0,
            mode: "unpinned",
            note: None,
        }
    }
}

/// The [`Layout`] for `process`'s CPUs with `pinned` held by shards: the free cores for the
/// heavy threads, the blocking pool and the feed and hub runtimes, the free cores plus the
/// shards' siblings for the request runtime's workers, and no shard CPU in either.
///
/// Without `affinity` (`--affinity off`), the counts are the same but no mask is set, so
/// `off` differs from `on` only in the masks (R1's acceptance run compares the two). The mode
/// then says so whatever the masks would have been, with no `--shards` advice (perf287's N1
/// on #1697); unpinned stays unpinned.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn layout(
    process: &[usize],
    pinned: &[usize],
    siblings: impl Fn(usize) -> Vec<usize>,
    affinity: bool,
) -> Layout {
    let plan = mask_plan(process, pinned, siblings);
    let mut out = match plan {
        MaskPlan::Unpinned { free } => Layout {
            masks: RuntimeMasks::default(),
            free,
            siblings: 0,
            mode: "unpinned",
            note: None,
        },
        MaskPlan::Cores { cores, siblings } => {
            let mut wide = cores.clone();
            wide.extend_from_slice(&siblings);
            wide.sort_unstable();
            Layout {
                masks: RuntimeMasks {
                    network: Some(wide.clone()),
                    blocking: Some(cores.clone()),
                    feed: Some(cores.clone()),
                    hub: Some(cores.clone()),
                },
                free: cores.len(),
                siblings: siblings.len(),
                mode: "off the shards' cores",
                note: None,
            }
        }
        MaskPlan::TooFewCores { free, cpus, pinned } => Layout {
            masks: RuntimeMasks::default(),
            free,
            siblings: 0,
            mode: "unmasked (too few free cores)",
            note: Some(too_few_cores(cpus, pinned)),
        },
    };
    if !affinity && out.mode != "unpinned" {
        out.masks = RuntimeMasks::default();
        out.siblings = 0;
        out.mode = "unmasked (--affinity off)";
        out.note = None;
    }
    out
}

/// The CPUs the masks leave: the free cores are the process's minus the shards' cores, each
/// pinned CPU and its SMT siblings (perf287's F1 on #1501: the engine pins one shard per
/// physical core first).
#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
enum MaskPlan {
    /// Nothing is pinned: no mask, every CPU free.
    Unpinned { free: usize },
    /// At least two CPUs left off the shards' cores: mask the threads to them, and the
    /// request runtime's workers also to `siblings`, the shards' SMT siblings that no shard
    /// holds (#1734). A sibling shares its shard's core (#1244's doubled p99), so it takes
    /// only the threads the puts wait on, never the heavy threads or the feed side (g82).
    Cores {
        cores: Vec<usize>,
        siblings: Vec<usize>,
    },
    /// Fewer than two CPUs left (a small host, or `--shards` near the core count). No mask:
    /// on the shards' SMT siblings, the server's threads cost the shards more than they do
    /// spread by the OS (g70d on #1397, the coordinator's call on #1697). `free` counts the
    /// CPUs no shard holds, for the threads' counts.
    TooFewCores {
        free: usize,
        cpus: usize,
        pinned: usize,
    },
}

/// [`MaskPlan`] for `process`'s CPUs with `pinned` held by shards, `siblings` giving each
/// CPU's SMT siblings (sysfs on Linux; a function, so tests can give a topology).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn mask_plan(
    process: &[usize],
    pinned: &[usize],
    siblings: impl Fn(usize) -> Vec<usize>,
) -> MaskPlan {
    if pinned.is_empty() {
        return MaskPlan::Unpinned {
            free: process.len(),
        };
    }
    let mut taken: Vec<usize> = pinned.iter().flat_map(|&c| siblings(c)).collect();
    taken.extend_from_slice(pinned);
    let cores: Vec<usize> = process
        .iter()
        .copied()
        .filter(|c| !taken.contains(c))
        .collect();
    if cores.len() >= 2 {
        // sysfs lists a CPU among its own siblings, and with more shards than cores a
        // sibling can be another shard's CPU: neither is free.
        let siblings = process
            .iter()
            .copied()
            .filter(|c| taken.contains(c) && !pinned.contains(c))
            .collect();
        return MaskPlan::Cores { cores, siblings };
    }
    MaskPlan::TooFewCores {
        free: process.iter().filter(|c| !pinned.contains(c)).count(),
        cpus: process.len(),
        pinned: pinned.len(),
    }
}

/// The startup note for [`MaskPlan::TooFewCores`].
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn too_few_cores(cpus: usize, pinned: usize) -> String {
    format!(
        "{pinned} shard threads are pinned across this process's {cpus} CPUs, leaving fewer \
         than two CPUs free of them and their SMT siblings, so the server's threads run \
         unmasked: lower --shards to keep them off the shards' cores"
    )
}

/// `cpu`'s SMT siblings, from sysfs (`0-1` or `0,8` style lists); none if unreadable.
#[cfg(target_os = "linux")]
pub(crate) fn siblings(cpu: usize) -> Vec<usize> {
    let path = format!("/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list");
    std::fs::read_to_string(path)
        .map(|s| parse_cpu_list(s.trim()))
        .unwrap_or_default()
}

/// `cpus` (sorted) as a CPU list, runs joined: `[0, 1, 2, 8]` is `0-2,8`.
pub(crate) fn format_cpu_list(cpus: &[usize]) -> String {
    let mut out = Vec::new();
    let mut i = 0;
    while i < cpus.len() {
        let start = cpus[i];
        while i + 1 < cpus.len() && cpus[i + 1] == cpus[i] + 1 {
            i += 1;
        }
        out.push(if cpus[i] == start {
            start.to_string()
        } else {
            format!("{start}-{}", cpus[i])
        });
        i += 1;
    }
    out.join(",")
}

/// A sysfs CPU list (`0-3,8,10-11`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_cpu_list(list: &str) -> Vec<usize> {
    let mut out = Vec::new();
    for part in list.split(',').filter(|p| !p.is_empty()) {
        match part.split_once('-') {
            Some((a, b)) => {
                if let (Ok(a), Ok(b)) = (a.parse::<usize>(), b.parse::<usize>()) {
                    out.extend(a..=b);
                }
            }
            None => out.extend(part.parse::<usize>().ok()),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{Placement, default_shards};
    use pigeonhole::{Options, Pigeonhole};

    /// D313 R1's mask, on a 4-core, 8-CPU topology (CPU `c`'s sibling is `c ^ 4`): the
    /// shards' cores and their siblings are left out while two cores stay free, and below
    /// that no mask at all, never the shards' siblings (the coordinator on #1697).
    #[test]
    fn the_mask_keeps_off_the_shards_cores_or_sets_none() {
        use super::{MaskPlan, mask_plan};
        let cpus: Vec<usize> = (0..8).collect();
        let siblings = |c: usize| vec![c ^ 4];
        assert_eq!(
            mask_plan(&cpus, &[], siblings),
            MaskPlan::Unpinned { free: 8 }
        );
        assert_eq!(
            mask_plan(&cpus, &[0, 1], siblings),
            MaskPlan::Cores {
                cores: vec![2, 3, 6, 7],
                siblings: vec![4, 5]
            }
        );
        // One core left is still two CPUs, enough for a mask.
        assert_eq!(
            mask_plan(&cpus, &[0, 1, 2], siblings),
            MaskPlan::Cores {
                cores: vec![3, 7],
                siblings: vec![4, 5, 6]
            }
        );
        // Every core held: no mask, never the siblings. The threads' counts take the four
        // CPUs no shard holds.
        assert_eq!(
            mask_plan(&cpus, &[0, 1, 2, 3], siblings),
            MaskPlan::TooFewCores {
                free: 4,
                cpus: 8,
                pinned: 4
            }
        );
        // No SMT: each CPU is a core, and one left is too few.
        assert_eq!(
            mask_plan(&[0, 1, 2, 3], &[0, 1], |_| Vec::new()),
            MaskPlan::Cores {
                cores: vec![2, 3],
                siblings: vec![]
            }
        );
        assert_eq!(
            mask_plan(&[0, 1, 2, 3], &[0, 1, 2], |_| Vec::new()),
            MaskPlan::TooFewCores {
                free: 1,
                cpus: 4,
                pinned: 3
            }
        );
        assert!(super::too_few_cores(8, 3).contains("--shards"));
    }

    /// No mask holds a pinned CPU (ai1249's N2 on #1738): the property itself, beside the
    /// exact lists.
    fn off_the_shards(masks: &super::RuntimeMasks, pinned: &[usize]) {
        for m in [&masks.network, &masks.blocking, &masks.feed, &masks.hub] {
            assert!(
                m.iter().flatten().all(|c| !pinned.contains(c)),
                "{masks:?} holds one of {pinned:?}"
            );
        }
    }

    /// #1734: the request runtime's workers run on the free cores and the shards' SMT
    /// siblings, every other thread on the free cores only (the feed and hub runtimes too,
    /// g82), and no runtime on a shard's CPU.
    /// The box's split (perf287's g80): the server on 0-9,16-25, 4 shards on 0-3, CPU `c`'s
    /// sibling `c ± 16`, listed by sysfs with `c` itself.
    #[test]
    fn the_request_workers_also_take_the_shards_siblings() {
        use super::{RuntimeMasks, format_cpu_list, layout};
        let process: Vec<usize> = (0..10).chain(16..26).collect();
        let sysfs = |c: usize| vec![c % 16, c % 16 + 16];
        let l = layout(&process, &[0, 1, 2, 3], sysfs, true);
        off_the_shards(&l.masks, &[0, 1, 2, 3]);
        let wide: Vec<usize> = (4..10).chain(16..26).collect();
        let cores: Vec<usize> = (4..10).chain(20..26).collect();
        assert_eq!(
            l.masks,
            RuntimeMasks {
                network: Some(wide.clone()),
                blocking: Some(cores.clone()),
                feed: Some(cores.clone()),
                hub: Some(cores.clone()),
            }
        );
        assert_eq!(format_cpu_list(&wide), "4-9,16-25");
        assert_eq!(
            (l.free, l.siblings, l.mode),
            (12, 4, "off the shards' cores")
        );
        // A shard on each of a core's two CPUs: the other's sibling is a shard's CPU, never
        // free (CPU `c`'s sibling `c ^ 4`, sysfs-style).
        let pairs = |c: usize| vec![c & 3, (c & 3) + 4];
        let l = layout(&(0..8).collect::<Vec<_>>(), &[0, 4, 1], pairs, true);
        off_the_shards(&l.masks, &[0, 4, 1]);
        assert_eq!(l.masks.blocking, Some(vec![2, 3, 6, 7]));
        assert_eq!(l.masks.network, Some(vec![2, 3, 5, 6, 7]));
        // A taskset that leaves out a shard's sibling: it never comes back (CPU 4 here).
        let l = layout(&[0, 1, 2, 3, 5, 6, 7], &[0, 1], pairs, true);
        off_the_shards(&l.masks, &[0, 1]);
        assert_eq!(l.masks.blocking, Some(vec![2, 3, 6, 7]));
        assert_eq!(l.masks.network, Some(vec![2, 3, 5, 6, 7]));
        assert_eq!((l.free, l.siblings), (4, 1));
        // `--affinity off`: the same counts, no masks.
        let off = layout(&process, &[0, 1, 2, 3], sysfs, false);
        assert_eq!(off.masks, RuntimeMasks::default());
        assert_eq!(
            (off.free, off.siblings, off.mode),
            (12, 0, "unmasked (--affinity off)")
        );
        // Too few cores: no masks, the siblings included (#1697).
        let few = layout(&(0..8).collect::<Vec<_>>(), &[0, 1, 2, 3], pairs, true);
        assert_eq!(few.masks, RuntimeMasks::default());
        assert_eq!(few.mode, "unmasked (too few free cores)");
        // Nothing pinned: no masks.
        assert_eq!(
            layout(&process, &[], sysfs, true).masks,
            RuntimeMasks::default()
        );
    }

    /// D317 ticket 8a (cli692's M3): a share's memory goes to the memtables first, at most
    /// the library's arena per shard, then to the caches at the library's 4:1; overrides
    /// come off the top, and a share that can't cover the memtables' floor is refused.
    #[test]
    fn engine_memory_splits_memtables_first_then_the_caches() {
        use super::{MEMTABLE_FLOOR, MEMTABLE_PER_SHARD, Memory, memory};
        let mib = 1u64 << 20;
        // Plenty: the arena per shard, and the rest 4:1.
        let m = Memory::of(2048 * mib, 4).unwrap();
        assert_eq!(m.memtable_per_shard, MEMTABLE_PER_SHARD);
        let rest = 2048 * mib - 4 * MEMTABLE_PER_SHARD;
        assert_eq!(
            (m.block_cache, m.row_cache),
            (rest * 4 / 5, rest - rest * 4 / 5)
        );
        assert_eq!(m.bytes(), 2048 * mib);
        // Tight: the memtables take it all, smaller per shard.
        let m = Memory::of(4 * mib, 8).unwrap();
        assert_eq!(
            (m.memtable_per_shard, m.block_cache, m.row_cache),
            (mib / 2, 0, 0)
        );
        // Too little for the floor: refused with the least that would do.
        assert_eq!(Memory::of(MEMTABLE_FLOOR, 2), Err(2 * MEMTABLE_FLOOR));
        // A node's: unset, the defaults; set, equal shares after the map's overrides.
        assert_eq!(memory(None, &[(2, 0)]).unwrap(), [Memory::defaults(2)]);
        let parts = memory(Some(3072 * mib), &[(2, 0), (2, 1024 * mib), (2, 0)]).unwrap();
        assert_eq!(parts[1].bytes(), 1024 * mib);
        assert_eq!(
            (parts[0].bytes(), parts[2].bytes()),
            (1024 * mib, 1024 * mib)
        );
        assert_eq!(parts.iter().map(Memory::bytes).sum::<u64>(), 3072 * mib);
        // Overrides beyond the node's total, or a share below the floor, name the cause.
        assert!(memory(Some(mib), &[(1, 2 * mib)]).is_err());
        let (i, why) = memory(Some(mib), &[(1, 0), (64, 0)]).unwrap_err();
        assert_eq!(i, Some(1), "{why}");
        // Overrides beyond the total are no one partition's.
        assert_eq!(memory(Some(mib), &[(1, 2 * mib)]).unwrap_err().0, None);
    }

    /// D317 ticket 8a: a node's budget splits into equal shares, the remainder to the first
    /// partitions, and pinned compaction threads never exceed the node's total.
    #[test]
    fn the_reserved_compaction_cpus_count_on_the_serving_line() {
        // perf287's 8b run: confined compaction counted nothing, so the line said "0
        // compaction CPUs not" with four of them on 8-11.
        let mut p = Placement::routes_only(Some(2), Some(2));
        assert_eq!(p.compaction_cpus, 0);
        p.confined_compaction(&[8, 9, 10, 11]);
        assert_eq!(p.compaction_cpus, 4);
    }

    #[test]
    fn a_nodes_budget_splits_across_its_partitions() {
        use super::{Share, split};
        let s = |shards, compaction, confined| Share {
            shards,
            compaction,
            confined,
        };
        // One partition: the whole budget.
        assert_eq!(split(8, 3, 1), [s(8, 3, false)]);
        // Remainders go to the first partitions.
        assert_eq!(
            split(8, 3, 3),
            [s(3, 1, false), s(3, 1, false), s(2, 1, false)]
        );
        assert_eq!(split(7, 4, 2), [s(4, 2, false), s(3, 2, false)]);
        // Fewer compaction threads than partitions: one pinned each for the first, one
        // confined each for the rest, so the pinned ones stay at the node's 3.
        let eight = split(8, 3, 8);
        assert!(eight.iter().all(|p| p.shards == 1 && p.compaction == 1));
        assert_eq!(eight.iter().filter(|p| !p.confined).count(), 3);
        assert!(eight[..3].iter().all(|p| !p.confined));
        // Fewer shards than partitions: each still gets one.
        assert!(split(2, 1, 4).iter().all(|p| p.shards == 1));
        // No compaction threads: none anywhere.
        assert!(
            split(4, 0, 2)
                .iter()
                .all(|p| p.compaction == 0 && !p.confined)
        );
        // Totals are kept whenever there's at least one of each per partition.
        for n in 1..=8 {
            let parts = split(16, 8, n);
            assert_eq!(parts.iter().map(|p| p.shards).sum::<usize>(), 16, "{n}");
            assert_eq!(parts.iter().map(|p| p.compaction).sum::<usize>(), 8, "{n}");
        }
    }

    #[test]
    fn a_plan_leaves_threads_for_the_network_side() {
        let dir = std::env::temp_dir().join(format!("phdb-placement-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Pigeonhole::open(
            dir.join("p.phdb"),
            Options::default().create_if_missing(true).shards(1),
        )
        .unwrap();
        let plan = Placement::plan(&[&db], None, None, None, None, true);
        assert!(
            plan.io_threads >= 1 && plan.blocking_threads >= 1 && plan.feed_threads >= 1,
            "{plan:?}"
        );
        // The feed threads come out of the network threads' default, not on top of it.
        if plan.free_cpus >= 4 {
            assert_eq!(
                plan.io_threads + plan.feed_threads,
                plan.free_cpus,
                "{plan:?}"
            );
        }
        let given = Placement::plan(&[&db], Some(3), Some(5), Some(2), Some(7), true);
        assert_eq!(
            (given.io_threads, given.feed_threads, given.hub_threads),
            (3, 2, 7)
        );
        // `--affinity off`: the same threads, with no mask (ICR 0131).
        let off = Placement::plan(&[&db], None, None, None, None, false);
        assert_eq!(
            (
                off.io_threads,
                off.blocking_threads,
                off.feed_threads,
                off.hub_threads,
                off.free_cpus
            ),
            (
                plan.io_threads,
                plan.blocking_threads,
                plan.feed_threads,
                plan.hub_threads,
                plan.free_cpus
            ),
            "{off:?} against {plan:?}"
        );
        assert_eq!(off.cpu_list(), None, "{off:?}");
        assert!(
            ["unpinned", "unmasked (--affinity off)"].contains(&off.mode),
            "{off:?}"
        );
        // The hubs' threads: a quarter of the feed threads, rounded up, at least 1 (#1719 keeps
        // C's default), and `--feed-hub-threads` as given.
        for (feed, hubs) in [(1, 1), (2, 1), (3, 1), (4, 1), (5, 2), (8, 2), (9, 3)] {
            assert_eq!(
                Placement::plan(&[&db], None, None, Some(feed), None, true).hub_threads,
                hubs
            );
        }
        assert_eq!(
            Placement::plan(&[&db], None, None, Some(4), Some(1), true).hub_threads,
            1
        );
        assert_eq!(
            Placement::plan(&[&db], None, None, Some(4), Some(0), true).hub_threads,
            1
        );
        // A recompute for the start's pinned CPUs gives the start's masks (D317 ticket 6's
        // reapply calls it as a partition opens or closes).
        assert_eq!(plan.masks(&db.pinned_shard_cpus()), plan.masks, "{plan:?}");
        assert_eq!(off.masks(&db.pinned_shard_cpus()), off.masks, "{off:?}");
        // The feed and hub runtimes take the free cores' mask (g82).
        assert_eq!(
            (&plan.masks.feed, &plan.masks.hub),
            (&plan.masks.blocking, &plan.masks.blocking),
            "{plan:?}"
        );
        // On Linux, the masks a thread really gets: a request runtime thread starts on the
        // free cores (its blocking pool stays there, ai1249's S1), a worker widens to
        // `network` as it first unparks, a reapply with no shard pinned (the last partition
        // closed, D317 ticket 6) gives it back the process's CPUs at its next refresh, and
        // the feed and hub runtimes' threads run on the free cores.
        #[cfg(target_os = "linux")]
        if let Some(blocking) = plan.masks.blocking.clone() {
            let now = || {
                let set = nix::sched::sched_getaffinity(nix::unistd::Pid::from_raw(0)).unwrap();
                (0..nix::sched::CpuSet::count())
                    .filter(|&c| set.is_set(c).unwrap_or(false))
                    .collect::<Vec<usize>>()
            };
            let process = now();
            let p = plan.clone();
            let feed = std::thread::spawn(move || {
                p.enter_feed();
                let feed = now();
                p.enter_hub();
                (feed, now())
            })
            .join()
            .unwrap();
            assert_eq!(feed, (blocking.clone(), blocking.clone()), "{plan:?}");
            // Last: the reapply changes the plan's masks for every clone.
            let p = plan.clone();
            let (network, after) = std::thread::spawn(move || {
                p.enter_network();
                let start = now();
                p.unpark_network();
                let widened = now();
                p.reapply(&[]);
                super::refresh();
                (start, (widened, now()))
            })
            .join()
            .unwrap();
            assert_eq!(network, blocking, "{plan:?}");
            assert_eq!(Some(after.0), plan.masks.network, "{plan:?}");
            assert_eq!(after.1, process, "{plan:?}");
        }
        // Entering the plan on a thread (the mask, on Linux) never fails the thread.
        for enter in [
            Placement::enter,
            Placement::enter_network,
            Placement::enter_feed,
            Placement::enter_hub,
            Placement::unpark_network,
        ] {
            let p = plan.clone();
            std::thread::spawn(move || enter(&p)).join().unwrap();
        }
        assert!(default_shards() >= 1);
        assert_eq!(super::parse_cpu_list("0-2,8,10-11"), [0, 1, 2, 8, 10, 11]);
        assert_eq!(super::parse_cpu_list("4"), [4]);
        assert!(super::parse_cpu_list("").is_empty());
        assert_eq!(super::format_cpu_list(&[0, 1, 2, 8, 10, 11]), "0-2,8,10-11");
        assert_eq!(super::format_cpu_list(&[4]), "4");
        assert_eq!(super::format_cpu_list(&[]), "");
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
