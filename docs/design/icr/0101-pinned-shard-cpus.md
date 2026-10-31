# 0101: `Pigeonhole::pinned_shard_cpus()`

**Status:** Approved (number from the coordinator, 2026-10-31; the owner chose option (A) on #1206: a public accessor rather than reading `/proc`).

## The change
- **pigeonhole:** `Pigeonhole::pinned_shard_cpus(&self) -> Vec<usize>`. These are the CPU ids the engine pinned its shard threads to (D235), one per pinned shard thread, in thread order. It's empty when none is pinned:
  - `pin_threads(false)`;
  - application-owned mode;
  - a platform without hard thread affinity (macOS);
  - a CPU quota, or too few free CPUs;
  - every pin refused.
- **Internal crates** (no promise):
  - **pigeonhole-engine:** `Engine::pinned_shard_cpus(&self) -> Vec<usize>`.
  - **pigeonhole-runtime:** `Runtime::pinned_driver_cpus(&self) -> &[usize]`: the CPUs of the driver threads whose pin succeeded, in driver order.
  - **pigeonhole-io:** `sys::affinity_cpus() -> Option<Vec<usize>>`, the calling thread's affinity set in ascending order (`None` without hard affinity). Also `sys::set_current_thread_cpus(&[usize]) -> Result<()>`, which restricts the calling thread to a set (Linux `sched_setaffinity`, Windows `SetThreadAffinityMask`, `Unsupported` elsewhere).

## Why
#1206: the waits behind sparse-wide's awake inbox p99.9 were pinned shard threads being preempted, mostly by application (bench client) threads the OS placed on their CPUs. The owner decided:
- the bench keeps its client threads off the pinned shard CPUs;
- the guide advises applications to do the same;
- the report notes the placement.

An application needs the CPU ids to do that. `engine_metrics()` only reports counts (`threads_pinned`).

## Callers
- **`pigeonhole-bench`:** `Runner::client_cpus`, which the Pigeonhole runner answers as the affinity set less `pinned_shard_cpus()`. The client threads restrict themselves to it before each pass. `RunDetail::client_cpus` records it, and the markdown report prints a "Client placement" line.
- **The guide's deployment notes** (`getting-started.md`) and the agent reference.
