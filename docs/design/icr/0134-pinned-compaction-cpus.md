# 0134: `Options::compaction_cpu_set` and `Pigeonhole::pinned_compaction_cpus()`

**Status:** Approved (number from the coordinator, 2026-11-11, for D317 ticket 8a; both changes additive).
- **Scope after g72:** the coordinator dropped the union mask (#1703 comment 6787495064). ICR 0134 is `compaction_cpu_set`, perf287's A1 on the 8a plan.
- **`pinned_compaction_cpus()`** stays because `compaction_cpu_set` needs it: the set a node confines its later partitions' compaction threads to is the CPUs its earlier partitions' compaction threads pinned, and this is how serve learns them. The pinning check's per-partition counts use it too.
- **The compaction CPUs are returned apart from the shards',** per cli692's review of the 8a plan.

## The change
- **pigeonhole:** `Options::compaction_cpu_set(cpus)` confines the compaction threads ([`Options::compaction_cores`]) to those CPU ids, any of them, instead of pinning each to a CPU of its own.
  - **While it's set,** only the shards are planned CPUs of their own, so it never costs a shard its pin.
  - **No effect** with `pin_threads(false)`, with no compaction threads, in application-owned mode, or with an empty set. Without hard affinity (macOS), the threads run unpinned.
- **pigeonhole:** `EngineMetrics::threads_confined`: the compaction threads confined to the set, neither pinned nor unpinned. A thread the OS won't confine runs unpinned, reported in `unpinned_reason` ("the OS refused some pins").
- **pigeonhole:** `Pigeonhole::pinned_compaction_cpus(&self) -> Vec<usize>`: the CPU ids the engine pinned its compaction threads to, one per pinned thread, in thread order. With `pinned_shard_cpus()` (ICR 0101), they're every CPU the engine pinned. It's empty when no compaction thread is pinned:
  - no compaction threads (`compaction_cores(0)`, or application-owned mode);
  - `pin_threads(false)`, or a platform without hard affinity (macOS);
  - a CPU quota;
  - too few free CPUs (D252: the shards are pinned first and the compaction threads run unpinned);
  - the threads confined to a set;
  - every pin refused.
- **Internal crates** (no promise):
  - **pigeonhole-engine:**
    - `EngineOptions::compaction_cpu_set: Option<Vec<usize>>`;
    - `Engine::pinned_compaction_cpus`;
    - the engine metrics' `confined_threads`.
  - **pigeonhole-runtime:**
    - `RuntimeConfig::compaction_cpu_set: Option<Arc<[usize]>>`;
    - `Runtime::pinned_compaction_cpus(&self) -> &[usize]`;
    - `Pinning::confined`. A refused confine counts in `Pinning::failed`.
  - **pigeonhole-io:** `sys::confine_current_thread_to(&[usize])`: `sched_setaffinity` with the whole set on Linux, `SetThreadAffinityMask` on Windows, `Unsupported` elsewhere.

## Why
- **The cap (perf287's A1 on the 8a plan):** a node with more partitions than its compaction budget would otherwise pin one compaction thread per partition. At 8 partitions on the AX102, that's twice the budget. With the set, the node's first partitions pin the budget's compaction threads, and the rest share those CPUs. So compaction never spreads onto the shards' or the network side's CPUs, and pinned compaction threads never exceed `--compaction-threads`.
- **Not the mask:** `phdb serve`'s network mask leaves out the shards' CPUs only. g72 measured the alternative, leaving out the compaction CPUs too. On a single node it left the network side 2 CPUs where it had 8, with about 3× less throughput and a worse p99. #1703 stays open for 1.3.0.

## Callers
`phdb serve` (D317 ticket 8a):
- the compaction cap for partitions past the node's total;
- the pinning check's per-partition counts and gauges;
- the startup line, which counts the compaction CPUs the mask doesn't leave out.
