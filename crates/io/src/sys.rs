//! Thread placement: CPU pinning and topology. Lives here because it needs `unsafe` FFI and
//! this is one of the three crates allowed it.
//!
//! ```
//! let cpus = pigeonhole_io::sys::available_cpus();
//! assert!(cpus >= 1);
//! // One shard per physical core (D220), never more than the CPUs.
//! assert!((1..=cpus).contains(&pigeonhole_io::sys::default_shards()));
//! // Pinning may be unsupported (macOS) or refused (restricted containers); both are errors,
//! // never panics.
//! let _ = pigeonhole_io::sys::pin_current_thread(0);
//! ```

use crate::Result;

/// This process's id, as [`std::process::id`] but without a system call after the first:
/// a cached value that a forked child resets (a `pthread_atfork` child handler on Unix), so a
/// child never sees its parent's id. Reader slots compare it with the id recorded at claim,
/// on every pin store, to leave a forked child's copy of a reader's handle inert (#693).
/// Inline: the fork checks on commit and read paths call it per operation (#919's
/// callgrind run measured the out-of-line call).
#[inline]
pub fn process_id() -> u32 {
    crate::os::process_id()
}

/// Number of CPUs available to this process, honoring affinity masks and cgroup CPU quotas.
pub fn available_cpus() -> usize {
    crate::os::available_cpus()
}

/// Number of physical cores this process may run on (SMT siblings count once), or `None`
/// where the OS does not report it. Linux reads the sysfs topology of the CPUs in the
/// affinity mask; macOS asks `hw.physicalcpu`; Windows asks `GetLogicalProcessorInformation`
/// about the CPUs in the process affinity mask.
pub fn physical_cores() -> Option<usize> {
    crate::os::physical_cores()
}

/// The default shard count (D220): one per physical core, within [`available_cpus`] (so a
/// cgroup CPU quota still caps it), or [`available_cpus`] where the OS reports no cores.
pub fn default_shards() -> usize {
    let cpus = available_cpus().max(1);
    physical_cores().map_or(cpus, |cores| cores.clamp(1, cpus))
}

/// Pins the calling thread to `cpu` (an index into the process's allowed CPU set).
/// `Unsupported` on platforms without affinity control (macOS treats it as a hint).
pub fn pin_current_thread(cpu: usize) -> Result<()> {
    crate::os::pin_current_thread(cpu)
}

/// The CPU (an index into the process's allowed CPU set, for [`pin_current_thread`]) for
/// the `i`-th pinned thread: one per physical core first, then their SMT siblings (D220), so
/// [`default_shards`] pinned threads never share a core. `i` modulo the CPUs where the
/// topology is unknown.
pub fn spread_cpu(i: usize) -> usize {
    crate::os::spread_cpu(i)
}

/// NUMA node of `cpu`, or `None` if unknown or not NUMA.
pub fn numa_node_of(cpu: usize) -> Option<u32> {
    crate::os::numa_node_of(cpu)
}

/// Why a [`pin_plan`] leaves threads unpinned (D235).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Unpinned {
    /// The platform has no hard thread affinity. macOS only offers affinity tags, which are
    /// hints the scheduler may ignore, so Pigeonhole does not set them.
    Unsupported,
    /// A CPU quota (a cgroup's `cpu.max`, not a cpuset) caps the process below the CPUs in
    /// its affinity set: pinning there would put every such process on the host's first CPUs.
    CpuQuota,
    /// Fewer CPUs in the process's affinity set than threads, counting those pinned threads
    /// of other plans in this process still hold (another database, say).
    TooFewCpus,
}

impl Unpinned {
    /// A short description, for metrics and logs.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unsupported => "the platform has no hard thread affinity",
            Self::CpuQuota => "a CPU quota is narrower than the affinity set",
            Self::TooFewCpus => "too few free CPUs in the affinity set",
        }
    }
}

/// CPUs held by the live [`PinPlan`]s of this process.
static CLAIMED: std::sync::Mutex<Vec<usize>> = std::sync::Mutex::new(Vec::new());

/// A CPU of its own for each of a set of threads, claimed for this process until the plan is
/// dropped (D235). Made by [`pin_plan`]; each thread pins itself with [`PinPlan::cpu`] and
/// [`pin_current_thread_to`].
#[derive(Debug)]
pub struct PinPlan {
    cpus: Vec<usize>,
    /// The affinity set it was planned from.
    allowed: Vec<usize>,
}

impl PinPlan {
    /// The CPU id for thread `i`, if the plan covers it.
    pub fn cpu(&self, i: usize) -> Option<usize> {
        self.cpus.get(i).copied()
    }

    /// Threads the plan covers.
    pub fn len(&self) -> usize {
        self.cpus.len()
    }

    /// Whether the plan covers no thread.
    pub fn is_empty(&self) -> bool {
        self.cpus.is_empty()
    }

    /// The CPUs for the kernel's I/O workers of the plan's first `drivers` threads (D305):
    /// the affinity set it was planned from, less those threads' own CPUs. Empty when
    /// nothing is left.
    pub fn io_worker_cpus(&self, drivers: usize) -> Vec<usize> {
        let own = &self.cpus[..drivers.min(self.cpus.len())];
        self.allowed
            .iter()
            .copied()
            .filter(|c| !own.contains(c))
            .collect()
    }
}

impl Drop for PinPlan {
    fn drop(&mut self) {
        let mut claimed = CLAIMED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        claimed.retain(|c| !self.cpus.contains(c));
    }
}

/// Plans pinning for `threads` threads (D235): a distinct CPU each from the calling thread's
/// affinity set, one per physical core first and then their SMT siblings (D220), skipping
/// CPUs other live plans in this process hold. Threads are only ever pinned inside the
/// affinity set (taskset, cpusets, containers). It refuses, so the threads run unpinned,
/// where pinning would hurt more than it helps: no hard affinity on this platform, a CPU
/// quota narrower than the affinity set, or fewer free CPUs than threads (never two pinned
/// threads on one CPU).
///
/// ```
/// use pigeonhole_io::sys::{Unpinned, pin_plan};
///
/// match pin_plan(2) {
///     Ok(plan) => assert_eq!(plan.len(), 2),
///     Err(why) => assert!(!why.as_str().is_empty()),
/// }
/// ```
pub fn pin_plan(threads: usize) -> std::result::Result<PinPlan, Unpinned> {
    let order = crate::os::pin_order().ok_or(Unpinned::Unsupported)?;
    plan_from(&order, available_cpus(), threads)
}

/// [`pin_plan`] over `order` (the affinity set in pinning order) with `quota` CPUs available.
fn plan_from(
    order: &[usize],
    quota: usize,
    threads: usize,
) -> std::result::Result<PinPlan, Unpinned> {
    if threads == 0 {
        return Ok(PinPlan {
            cpus: Vec::new(),
            allowed: order.to_vec(),
        });
    }
    if quota < order.len() {
        return Err(Unpinned::CpuQuota);
    }
    let mut claimed = CLAIMED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let cpus: Vec<usize> = order
        .iter()
        .copied()
        .filter(|c| !claimed.contains(c))
        .take(threads)
        .collect();
    if cpus.len() < threads {
        return Err(Unpinned::TooFewCpus);
    }
    claimed.extend_from_slice(&cpus);
    Ok(PinPlan {
        cpus,
        allowed: order.to_vec(),
    })
}

/// Pins the calling thread to CPU id `cpu` (from [`PinPlan::cpu`]). `Unsupported` on
/// platforms without hard affinity.
pub fn pin_current_thread_to(cpu: usize) -> Result<()> {
    crate::os::pin_current_thread_to(cpu)
}

thread_local! {
    static IO_WORKER_CPUS: std::cell::RefCell<Vec<usize>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Names the CPUs (ids, as [`PinPlan::io_worker_cpus`] gives them) the kernel's I/O workers
/// for the calling thread may run on (D305): a pinned shard driver keeps its io_uring
/// workers, which would otherwise preempt it on its own CPU, off the drivers' CPUs. Applied,
/// best effort, when the thread's io_uring ring is made ([`crate::Vfs::attach_thread`]); a
/// ring made before keeps its workers where they are. Empty, the default, leaves them where
/// the kernel puts them. No effect on other backends and platforms.
pub fn set_io_worker_cpus(cpus: Vec<usize>) {
    let _ = IO_WORKER_CPUS.try_with(|c| *c.borrow_mut() = cpus);
}

/// The calling thread's [`set_io_worker_cpus`].
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn io_worker_cpus() -> Vec<usize> {
    IO_WORKER_CPUS
        .try_with(|c| c.borrow().clone())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{Unpinned, plan_from};

    #[test]
    fn plans_take_distinct_free_cpus_and_release_them() {
        // CPU ids far above any test machine's, so other tests' plans never overlap.
        let order = [9000, 9002, 9001, 9003];
        let a = plan_from(&order, 4, 2).unwrap();
        assert_eq!(
            (a.cpu(0), a.cpu(1), a.cpu(2)),
            (Some(9000), Some(9002), None)
        );
        // A second plan in the process takes what is left, in order.
        let b = plan_from(&order, 4, 2).unwrap();
        assert_eq!((b.cpu(0), b.cpu(1)), (Some(9001), Some(9003)));
        // Nothing left: unpinned rather than stacked.
        assert_eq!(plan_from(&order, 4, 1).unwrap_err(), Unpinned::TooFewCpus);
        drop(a);
        let c = plan_from(&order, 4, 2).unwrap();
        assert_eq!((c.cpu(0), c.cpu(1)), (Some(9000), Some(9002)));
    }

    #[test]
    fn more_threads_than_cpus_or_a_quota_stay_unpinned() {
        let order = [9100, 9101];
        assert_eq!(plan_from(&order, 2, 3).unwrap_err(), Unpinned::TooFewCpus);
        assert_eq!(plan_from(&order, 1, 1).unwrap_err(), Unpinned::CpuQuota);
        assert!(plan_from(&order, 2, 0).unwrap().is_empty());
    }

    #[test]
    fn io_workers_go_to_the_affinity_set_less_the_drivers_cpus() {
        let order = [9200, 9202, 9201, 9203];
        let plan = plan_from(&order, 4, 3).unwrap();
        // Two drivers and a compaction thread: the workers may use the compaction thread's
        // CPU and the free one, not the drivers'.
        assert_eq!(plan.io_worker_cpus(2), [9201, 9203]);
        assert_eq!(plan.io_worker_cpus(0), order);
        // Every CPU a driver's: nothing left.
        let all = plan_from(&[9300, 9301], 2, 2).unwrap();
        assert!(all.io_worker_cpus(2).is_empty());
        assert!(all.io_worker_cpus(5).is_empty());
    }
}
