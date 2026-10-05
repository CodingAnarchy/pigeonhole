//! Thread placement: CPU pinning and topology. Lives here because it needs `unsafe` FFI and
//! this is one of the three crates allowed it.
//!
//! ```
//! let cpus = pigeonhole_io::sys::available_cpus();
//! assert!(cpus >= 1);
//! // Pinning may be unsupported (macOS) or refused (restricted containers); both are errors,
//! // never panics.
//! let _ = pigeonhole_io::sys::pin_current_thread(0);
//! ```

use crate::Result;

/// Number of CPUs available to this process, honoring affinity masks and cgroup CPU quotas.
/// The default shard count.
pub fn available_cpus() -> usize {
    crate::os::available_cpus()
}

/// Pins the calling thread to `cpu` (an index into the process's allowed CPU set).
/// `Unsupported` on platforms without affinity control (macOS treats it as a hint).
pub fn pin_current_thread(cpu: usize) -> Result<()> {
    crate::os::pin_current_thread(cpu)
}

/// NUMA node of `cpu`, or `None` if unknown or not NUMA.
pub fn numa_node_of(cpu: usize) -> Option<u32> {
    crate::os::numa_node_of(cpu)
}
