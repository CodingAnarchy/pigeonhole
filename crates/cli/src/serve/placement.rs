//! Where the network side's threads run (perf287's R1 on D313): off the CPUs the engine
//! pinned its shard threads to, since a client thread preempting a pinned shard driver is
//! what raised `Buffered` put p99.9 (#1206), and a shard's SMT sibling doubled p99 (#1244).
//! On Linux the Tokio workers and blocking threads get an affinity mask of the remaining
//! CPUs; elsewhere, or when nothing was pinned, nothing changes.

use pigeonhole::Pigeonhole;

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
/// partition (by index) that can't be covered and the least it would need.
pub(crate) fn memory(
    total: Option<u64>,
    parts: &[(usize, u64)],
) -> Result<Vec<Memory>, (usize, String)> {
    let overridden: u64 = parts.iter().map(|(_, m)| *m).sum();
    let rest = parts.iter().filter(|(_, m)| *m == 0).count() as u64;
    let equal = match total {
        Some(t) if t < overridden => {
            return Err((
                0,
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
                    i,
                    format!(
                        "{share} bytes of engine memory can't cover {shards} shards' memtables \
                         at {MEMTABLE_FLOOR} bytes each: it needs at least {least}"
                    ),
                )
            })
        })
        .collect()
}

/// The network side's threads and where they may run.
#[derive(Debug, Clone)]
pub(crate) struct Placement {
    pub io_threads: usize,
    pub blocking_threads: usize,
    /// The feed delivery runtime's workers (ticket 5's (A)).
    pub feed_threads: usize,
    /// The feed hubs' runtime's workers: a quarter of the feed threads, at least 1 (#1397's
    /// 16 hubs × 16, where the hubs fell behind their engine feeds queued behind the
    /// clients' tasks on the feed runtime). Placed on the same CPUs, so the OS shares them.
    pub hub_threads: usize,
    /// The CPUs outside the shards' (Linux, when the engine pinned any).
    #[cfg(target_os = "linux")]
    mask: Option<nix::sched::CpuSet>,
    /// For the startup line.
    pub free_cpus: usize,
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
        affinity: bool,
    ) -> Self {
        // Every engine's pinned shard threads: the union across a node's partitions (D317
        // ticket 8a). Their compaction threads' CPUs are counted, not left out (g72, #1703).
        let mut pinned: Vec<usize> = dbs.iter().flat_map(|db| db.pinned_shard_cpus()).collect();
        pinned.sort_unstable();
        pinned.dedup();
        let compaction_cpus = dbs.iter().map(|db| db.pinned_compaction_cpus().len()).sum();
        #[cfg(target_os = "linux")]
        let (mask, free, mode, note) = match linux_mask(&pinned) {
            // The counts come from the CPUs the mask would leave, so `off` differs from `on`
            // only in the mask (R1's acceptance run compares the two).
            // Off, the mode says so whatever the mask would have been, with no `--shards`
            // advice (perf287's N1 on #1697); unpinned stays unpinned.
            (_, free, mode, _) if !affinity && mode != "unpinned" => {
                (None, free, "unmasked (--affinity off)", None)
            }
            planned => planned,
        };
        #[cfg(not(target_os = "linux"))]
        let _ = affinity;
        #[cfg(not(target_os = "linux"))]
        let (free, mode, note) = (
            std::thread::available_parallelism()
                .map_or(2, std::num::NonZero::get)
                .saturating_sub(pinned.len()),
            "unpinned",
            None,
        );
        let default = free.max(2);
        // A quarter. perf287 on #1397: a floor of 4 (8 network + 4 feed on a 12-CPU slice)
        // left the network side short and every split run fell behind; 9 + 3 kept up.
        let feed_threads = feed.unwrap_or(free / 4).max(1);
        Self {
            hub_threads: feed_threads.div_ceil(4).max(1),
            io_threads: io
                .unwrap_or(free.saturating_sub(feed_threads).max(2))
                .max(1),
            blocking_threads: blocking.unwrap_or(default).max(1),
            feed_threads,
            #[cfg(target_os = "linux")]
            mask,
            free_cpus: free,
            mode,
            note,
            shard_cpus: pinned.len(),
            compaction_cpus,
        }
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
            #[cfg(target_os = "linux")]
            mask: None,
            free_cpus: free,
            mode: "unpinned (routes only)",
            note: None,
            shard_cpus: 0,
            compaction_cpus: 0,
        }
    }

    /// The CPUs the network side may run on, as a CPU list (`2-7,10`), when the plan pins
    /// them: the value for `CANDLE_CPU_MASK`.
    pub(crate) fn cpu_list(&self) -> Option<String> {
        #[cfg(target_os = "linux")]
        {
            use nix::sched::CpuSet;
            let mask = self.mask.as_ref()?;
            let cpus: Vec<usize> = (0..CpuSet::count())
                .filter(|&c| mask.is_set(c).unwrap_or(false))
                .collect();
            Some(format_cpu_list(&cpus))
        }
        #[cfg(not(target_os = "linux"))]
        {
            None
        }
    }

    /// Called on each Tokio worker and blocking thread as it starts.
    pub(crate) fn enter(&self) {
        #[cfg(target_os = "linux")]
        if let Some(mask) = &self.mask {
            // The calling thread: pid 0. A failure leaves the thread where the OS put it,
            // which is correct, only slower.
            let _ = nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), mask);
        }
    }
}

/// The CPUs the mask leaves: the process's minus the shards' cores, each pinned CPU and its
/// SMT siblings (perf287's F1 on #1501: the engine pins one shard per physical core first,
/// so a sibling left in would put network threads on a shard's core, #1244).
#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
enum MaskPlan {
    /// Nothing is pinned: no mask, every CPU free.
    Unpinned { free: usize },
    /// At least two CPUs left off the shards' cores: mask the threads to them.
    Cores(Vec<usize>),
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
        return MaskPlan::Cores(cores);
    }
    MaskPlan::TooFewCores {
        free: process.iter().filter(|c| !pinned.contains(c)).count(),
        cpus: process.len(),
        pinned: pinned.len(),
    }
}

/// The mask for this process (Linux), with its free CPUs, mode and note.
#[cfg(target_os = "linux")]
fn linux_mask(
    pinned: &[usize],
) -> (
    Option<nix::sched::CpuSet>,
    usize,
    &'static str,
    Option<String>,
) {
    use nix::sched::{CpuSet, sched_getaffinity};
    let all = std::thread::available_parallelism().map_or(2, std::num::NonZero::get);
    let Ok(set) = sched_getaffinity(nix::unistd::Pid::from_raw(0)) else {
        return (None, all.saturating_sub(pinned.len()), "unpinned", None);
    };
    let process: Vec<usize> = (0..CpuSet::count())
        .filter(|&c| set.is_set(c).unwrap_or(false))
        .collect();
    match mask_plan(&process, pinned, siblings) {
        MaskPlan::Unpinned { free } => (None, free, "unpinned", None),
        MaskPlan::Cores(cores) => {
            let mut mask = CpuSet::new();
            for c in &cores {
                let _ = mask.set(*c);
            }
            (Some(mask), cores.len(), "off the shards' cores", None)
        }
        MaskPlan::TooFewCores { free, cpus, pinned } => (
            None,
            free,
            "unmasked (too few free cores)",
            Some(too_few_cores(cpus, pinned)),
        ),
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
fn siblings(cpu: usize) -> Vec<usize> {
    let path = format!("/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list");
    std::fs::read_to_string(path)
        .map(|s| parse_cpu_list(s.trim()))
        .unwrap_or_default()
}

/// `cpus` (sorted) as a CPU list, runs joined: `[0, 1, 2, 8]` is `0-2,8`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn format_cpu_list(cpus: &[usize]) -> String {
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
            MaskPlan::Cores(vec![2, 3, 6, 7])
        );
        // One core left is still two CPUs, enough for a mask.
        assert_eq!(
            mask_plan(&cpus, &[0, 1, 2], siblings),
            MaskPlan::Cores(vec![3, 7])
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
            MaskPlan::Cores(vec![2, 3])
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

    /// D317 ticket 8a (cli692's M3): a share's memory goes to the memtables first, at most
    /// the library's arena per shard, then to the caches at the library's 4:1; overrides
    /// come off the top, and a share that can't cover the memtables' floor is refused.
    #[test]
    fn engine_memory_splits_memtables_first_then_the_caches() {
        use super::{MEMTABLE_FLOOR, MEMTABLE_PER_SHARD, Memory, memory};
        let mib = 1u64 << 20;
        let total = |m: &Memory| m.memtables() + m.block_cache + m.row_cache;
        // Plenty: the arena per shard, and the rest 4:1.
        let m = Memory::of(2048 * mib, 4).unwrap();
        assert_eq!(m.memtable_per_shard, MEMTABLE_PER_SHARD);
        let rest = 2048 * mib - 4 * MEMTABLE_PER_SHARD;
        assert_eq!(
            (m.block_cache, m.row_cache),
            (rest * 4 / 5, rest - rest * 4 / 5)
        );
        assert_eq!(total(&m), 2048 * mib);
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
        assert_eq!(total(&parts[1]), 1024 * mib);
        assert_eq!(
            (total(&parts[0]), total(&parts[2])),
            (1024 * mib, 1024 * mib)
        );
        assert_eq!(parts.iter().map(total).sum::<u64>(), 3072 * mib);
        // Overrides beyond the node's total, or a share below the floor, name the cause.
        assert!(memory(Some(mib), &[(1, 2 * mib)]).is_err());
        let (i, why) = memory(Some(mib), &[(1, 0), (64, 0)]).unwrap_err();
        assert_eq!(i, 1, "{why}");
    }

    /// D317 ticket 8a: a node's budget splits into equal shares, the remainder to the first
    /// partitions, and pinned compaction threads never exceed the node's total.
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
        let plan = Placement::plan(&[&db], None, None, None, true);
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
        let given = Placement::plan(&[&db], Some(3), Some(5), Some(2), true);
        assert_eq!((given.io_threads, given.feed_threads), (3, 2));
        // `--affinity off`: the same threads, with no mask (ICR 0131).
        let off = Placement::plan(&[&db], None, None, None, false);
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
        // The hubs' threads: a quarter of the feed threads, rounded up, at least 1.
        for (feed, hubs) in [(1, 1), (2, 1), (3, 1), (4, 1), (5, 2), (8, 2), (9, 3)] {
            assert_eq!(
                Placement::plan(&[&db], None, None, Some(feed), true).hub_threads,
                hubs
            );
        }
        // Entering the plan on a thread (the mask, on Linux) never fails the thread.
        let p = plan.clone();
        std::thread::spawn(move || p.enter()).join().unwrap();
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
