### Removed
- **Breaking (before the 1.0 API freeze; ICR 0085):** `pigeonhole`'s test and bench hooks now exist only with its new, non-default `__unstable-hooks` feature, which applications never turn on:
  - `Options::{commit_spin, shard_spin, adaptive_shard_threads}`. The spin windows keep their defaults (50 and 200 µs), and the experimental load controller stays off. Drop these calls.
  - The hidden test hooks (`Options::vfs`, `wal_segment_size`, `shard_stats` and the like).
  - The `PIGEONHOLE_IO` and `PIGEONHOLE_DIRECT` test variables: without the feature, the library ignores them.
- **Breaking:** `Options::allow_unregistered_merge_operators`, a deprecated alias. Use `read_only_fallback`.
- The `PIGEONHOLE_TAIL_INDEX` test variable (an A/B switch from before D199).

### Changed
- Docs: getting started and the agent reference give the memory a writer can use at the defaults: up to about 0.6 GiB at 4 physical cores, 1.3 GiB at 16 and 4.3 GiB at 64, filled only under load. A single memory budget is planned after 1.0 (#1041).
