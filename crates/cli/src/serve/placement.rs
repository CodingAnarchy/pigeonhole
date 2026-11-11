//! Where the network side's threads run (perf287's R1 on D313): off the CPUs the engine
//! pinned its shard threads to, since a client thread preempting a pinned shard driver is
//! what raised `Buffered` put p99.9 (#1206), and a shard's SMT sibling doubled p99 (#1244).
//! On Linux each runtime's threads get an affinity mask ([`RuntimeMasks`]); elsewhere, or
//! when nothing was pinned, nothing changes.
//!
//! - **The request, feed and hub runtimes** (puts and their delivery) run on the free cores
//!   and the shards' SMT siblings, never on a shard's own CPU. Without the siblings, puts
//!   waited on the free CPUs' run queues behind the feed side while the siblings sat idle:
//!   puts were held to 33K/s at 16 hubs × 16 subscribers with 1.35 cores of network threads
//!   waiting (#1734, perf287's g80b), and the coordinator chose the siblings over unmasking.
//! - **The server's own heavy threads** (bulk loads, index builds and rebalancers,
//!   `Replication.Follow`, embedding) and the request runtime's blocking pool (scans,
//!   `read_row`, `/mcp` and the other sync client calls) stay on the free cores only:
//!   [`Placement::enter`].
//! - **A thread inherits its creator's mask.** A `std::thread` spawned from a request,
//!   feed or hub worker starts on the shards' siblings too, so a heavy one calls
//!   [`Placement::enter`] first, as the backups, bulk loads, index builds, text rebuilds and
//!   `Replication.Follow` do (ai1249's S2 on #1738). An engine thread started from a
//!   worker inherits the wide mask too, so a partition opened at run time (D317 ticket 6)
//!   must open off the workers.

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
    /// The feed delivery runtime.
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
    /// For the startup line: how many of the shards' SMT siblings the request, feed and hub
    /// runtimes also run on.
    pub sibling_cpus: usize,
    /// How the mask was built, for the startup line: `"off the shards' cores"` (their SMT
    /// siblings too), `"unpinned"`, `"unmasked (--affinity off)"`, or `"unmasked (too few
    /// free cores)"`.
    pub mode: &'static str,
    /// Why the server's threads run unmasked though the shards are pinned, for a line of its
    /// own at startup.
    pub note: Option<String>,
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
        db: &Pigeonhole,
        io: Option<usize>,
        blocking: Option<usize>,
        feed: Option<usize>,
        hubs: Option<usize>,
        affinity: bool,
    ) -> Self {
        let pinned = db.pinned_shard_cpus();
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
        };
        plan.masks = plan.masks(&pinned);
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
        self.apply(self.masks.blocking.as_deref());
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
    /// the siblings. A reapply (D317 ticket 6) can't re-widen workers through this hook
    /// either, since `WIDENED` stays set: it masks workers and pool threads apart.
    pub(crate) fn unpark_network(&self) {
        thread_local! {
            static WIDENED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
        }
        if !WIDENED.get() {
            WIDENED.set(true);
            self.apply(self.masks.network.as_deref());
        }
    }

    /// Called on each feed runtime worker as it starts.
    pub(crate) fn enter_feed(&self) {
        self.apply(self.masks.feed.as_deref());
    }

    /// Called on each feed hub runtime worker as it starts.
    pub(crate) fn enter_hub(&self) {
        self.apply(self.masks.hub.as_deref());
    }

    /// Masks the calling thread to `cpus` (Linux). `None` means where the OS puts it, so the
    /// thread gets the process's CPUs back: a reapply after a recompute that lands on no
    /// mask (D317 ticket 6: `--affinity off`, or too few free cores once a partition opens)
    /// must not leave a thread on an old mask that may now hold a new shard's CPU (ai1249's
    /// N1 on #1738). Without a process set (another OS, a node that only routes), it leaves
    /// the thread be.
    pub(crate) fn apply(&self, cpus: Option<&[usize]>) {
        #[cfg(target_os = "linux")]
        {
            let cpus = cpus.unwrap_or(&self.process);
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
        let _ = cpus;
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
    /// The shards' SMT siblings the request, feed and hub runtimes also run on.
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
/// heavy threads, the free cores plus the shards' siblings for the request, feed and hub
/// runtimes, and no shard CPU in either.
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
                    feed: Some(wide.clone()),
                    hub: Some(wide),
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
    /// request, feed and hub runtimes also to `siblings`, the shards' SMT siblings that no
    /// shard holds (#1734). A sibling shares its shard's core (#1244's doubled p99), so it
    /// takes only the runtimes the puts wait on, never the heavy threads.
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

    /// #1734: the request, feed and hub runtimes run on the free cores and the shards' SMT
    /// siblings, the heavy threads on the free cores only, and no runtime on a shard's CPU.
    /// The box's split (perf287's g80): the server on 0-9,16-25, 4 shards on 0-3, CPU `c`'s
    /// sibling `c ± 16`, listed by sysfs with `c` itself.
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

    #[test]
    fn the_request_and_feed_runtimes_also_take_the_shards_siblings() {
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
                feed: Some(wide.clone()),
                hub: Some(wide.clone()),
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

    #[test]
    fn a_plan_leaves_threads_for_the_network_side() {
        let dir = std::env::temp_dir().join(format!("phdb-placement-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Pigeonhole::open(
            dir.join("p.phdb"),
            Options::default().create_if_missing(true).shards(1),
        )
        .unwrap();
        let plan = Placement::plan(&db, None, None, None, None, true);
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
        let given = Placement::plan(&db, Some(3), Some(5), Some(2), Some(7), true);
        assert_eq!(
            (given.io_threads, given.feed_threads, given.hub_threads),
            (3, 2, 7)
        );
        // `--affinity off`: the same threads, with no mask (ICR 0131).
        let off = Placement::plan(&db, None, None, None, None, false);
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
                Placement::plan(&db, None, None, Some(feed), None, true).hub_threads,
                hubs
            );
        }
        assert_eq!(
            Placement::plan(&db, None, None, Some(4), Some(1), true).hub_threads,
            1
        );
        assert_eq!(
            Placement::plan(&db, None, None, Some(4), Some(0), true).hub_threads,
            1
        );
        // A recompute for the start's pinned CPUs gives the start's masks (D317 ticket 6's
        // reapply calls it as a partition opens or closes).
        assert_eq!(plan.masks(&db.pinned_shard_cpus()), plan.masks, "{plan:?}");
        assert_eq!(off.masks(&db.pinned_shard_cpus()), off.masks, "{off:?}");
        // On Linux, the masks a thread really gets: a request runtime thread starts on the
        // free cores (its blocking pool stays there, ai1249's S1), a worker widens to
        // `network` as it first unparks, and `apply(None)` gives back the process's CPUs.
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
            let (network, after) = std::thread::spawn(move || {
                p.enter_network();
                let start = now();
                p.unpark_network();
                let widened = now();
                p.apply(None);
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
