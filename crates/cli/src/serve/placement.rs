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
    /// siblings too), `"off the shards' CPUs"` (too few CPUs to also leave the siblings
    /// out), or `"unpinned"`.
    pub mode: &'static str,
}

impl Placement {
    /// The threads' counts: given, or by default from the CPUs the shards don't use. The
    /// feed threads' default (a quarter, at least 1) is carved out of the network threads'
    /// default rather than added, so the two runtimes don't oversubscribe the CPUs (perf287's
    /// D2 on #1397). An explicit `--io-threads` is taken as given.
    pub(crate) fn plan(
        db: &Pigeonhole,
        io: Option<usize>,
        blocking: Option<usize>,
        feed: Option<usize>,
    ) -> Self {
        let pinned = db.pinned_shard_cpus();
        #[cfg(target_os = "linux")]
        let (mask, free, mode) = linux_mask(&pinned);
        #[cfg(not(target_os = "linux"))]
        let (free, mode) = (
            std::thread::available_parallelism()
                .map_or(2, std::num::NonZero::get)
                .saturating_sub(pinned.len()),
            "unpinned",
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

/// The process's CPUs minus the shards' cores: each pinned CPU and its SMT siblings
/// (perf287's F1 on #1501: the engine pins one shard per physical core first, so a sibling
/// left in would put network threads on a shard's core, #1244). When that leaves fewer than
/// two CPUs, only the pinned CPUs themselves are left out.
#[cfg(target_os = "linux")]
fn linux_mask(pinned: &[usize]) -> (Option<nix::sched::CpuSet>, usize, &'static str) {
    use nix::sched::{CpuSet, sched_getaffinity};
    let count = |set: &CpuSet| {
        (0..CpuSet::count())
            .filter(|&c| set.is_set(c).unwrap_or(false))
            .count()
    };
    let all = std::thread::available_parallelism().map_or(2, std::num::NonZero::get);
    let Ok(process) = sched_getaffinity(nix::unistd::Pid::from_raw(0)) else {
        return (None, all.saturating_sub(pinned.len()), "unpinned");
    };
    if pinned.is_empty() {
        return (None, count(&process), "unpinned");
    }
    let mut cores = process;
    for &cpu in pinned {
        for sibling in siblings(cpu) {
            let _ = cores.unset(sibling);
        }
        let _ = cores.unset(cpu);
    }
    if count(&cores) >= 2 {
        return (Some(cores), count(&cores), "off the shards' cores");
    }
    let mut cpus = process;
    for &cpu in pinned {
        let _ = cpus.unset(cpu);
    }
    match count(&cpus) {
        // Every CPU has a shard: sharing them is all there is.
        0 => (None, 0, "unpinned"),
        n => (Some(cpus), n, "off the shards' CPUs"),
    }
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

    #[test]
    fn a_plan_leaves_threads_for_the_network_side() {
        let dir = std::env::temp_dir().join(format!("phdb-placement-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Pigeonhole::open(
            dir.join("p.phdb"),
            Options::default().create_if_missing(true).shards(1),
        )
        .unwrap();
        let plan = Placement::plan(&db, None, None, None);
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
        let given = Placement::plan(&db, Some(3), Some(5), Some(2));
        assert_eq!((given.io_threads, given.feed_threads), (3, 2));
        // The hubs' threads: a quarter of the feed threads, rounded up, at least 1.
        for (feed, hubs) in [(1, 1), (2, 1), (3, 1), (4, 1), (5, 2), (8, 2), (9, 3)] {
            assert_eq!(
                Placement::plan(&db, None, None, Some(feed)).hub_threads,
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
