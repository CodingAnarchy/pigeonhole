//! Thread placement: CPU pinning and topology. Lives here because it needs `unsafe` FFI and
//! this is one of the three crates allowed it.

use crate::Result;

/// Number of CPUs available to this process, honoring affinity masks and cgroup CPU quotas.
/// The default shard count.
pub fn available_cpus() -> usize {
    todo!()
}

/// Pins the calling thread to `cpu` (an index into the process's allowed CPU set).
/// `Unsupported` on platforms without affinity control (macOS treats it as a hint).
pub fn pin_current_thread(cpu: usize) -> Result<()> {
    todo!()
}

/// NUMA node of `cpu`, or `None` if unknown or not NUMA.
pub fn numa_node_of(cpu: usize) -> Option<u32> {
    todo!()
}
