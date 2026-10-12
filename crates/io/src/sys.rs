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
