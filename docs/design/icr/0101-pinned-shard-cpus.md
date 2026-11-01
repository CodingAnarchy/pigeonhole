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

## Why
#1206 traced the waits behind sparse-wide's awake inbox p99.9 to the pinned shard threads being preempted, mostly by application (bench client) threads that the OS placed on their CPUs. An application needs the CPU ids to keep its threads off them, and `engine_metrics()` only reports counts (`threads_pinned`).

**The owner's decision, 2026-11-01 (option 1 on #1244):**
- **Kept:** the accessor.
- **Dropped:** the bench's client placement. Its gate-row A/B on the AX102 showed that at the default topology (one shard per physical core), keeping the clients off the shard CPUs leaves only the shards' SMT siblings. There, `Buffered` put p99 roughly doubled (sparse-wide 24.8 → 48.6 µs).
- **The guide's note:** placement helps only where there are spare physical cores.

## Callers
- The guide's getting-started notes and the agent reference.
