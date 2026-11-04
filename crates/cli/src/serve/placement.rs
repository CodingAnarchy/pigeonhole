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
    /// The CPUs outside the shards' (Linux, when the engine pinned any).
    #[cfg(target_os = "linux")]
    mask: Option<nix::sched::CpuSet>,
    /// For the startup line.
    pub free_cpus: usize,
}

impl Placement {
    pub(crate) fn plan(db: &Pigeonhole, io: Option<usize>, blocking: Option<usize>) -> Self {
        let pinned = db.pinned_shard_cpus();
        #[cfg(target_os = "linux")]
        let (mask, free) = linux_mask(&pinned);
        #[cfg(not(target_os = "linux"))]
        let free = std::thread::available_parallelism()
            .map_or(2, std::num::NonZero::get)
            .saturating_sub(pinned.len());
        let default = free.max(2);
        Self {
            io_threads: io.unwrap_or(default).max(1),
            blocking_threads: blocking.unwrap_or(default).max(1),
            #[cfg(target_os = "linux")]
            mask,
            free_cpus: free,
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

/// The process's CPUs minus the pinned ones, and how many that leaves.
#[cfg(target_os = "linux")]
fn linux_mask(pinned: &[usize]) -> (Option<nix::sched::CpuSet>, usize) {
    use nix::sched::{CpuSet, sched_getaffinity};
    let all = std::thread::available_parallelism().map_or(2, std::num::NonZero::get);
    let Ok(mut set) = sched_getaffinity(nix::unistd::Pid::from_raw(0)) else {
        return (None, all.saturating_sub(pinned.len()));
    };
    if pinned.is_empty() {
        let n = (0..CpuSet::count())
            .filter(|&c| set.is_set(c).unwrap_or(false))
            .count();
        return (None, n);
    }
    for &cpu in pinned {
        let _ = set.unset(cpu);
    }
    let free = (0..CpuSet::count())
        .filter(|&c| set.is_set(c).unwrap_or(false))
        .count();
    if free == 0 {
        // Every CPU has a shard: sharing them is all there is.
        return (None, 0);
    }
    (Some(set), free)
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
        let plan = Placement::plan(&db, None, None);
        assert!(
            plan.io_threads >= 1 && plan.blocking_threads >= 1,
            "{plan:?}"
        );
        assert_eq!(Placement::plan(&db, Some(3), Some(5)).io_threads, 3);
        // Entering the plan on a thread (the mask, on Linux) never fails the thread.
        let p = plan.clone();
        std::thread::spawn(move || p.enter()).join().unwrap();
        assert!(default_shards() >= 1);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
