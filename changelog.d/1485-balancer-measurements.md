### Added
- **Cluster partitions report their load** (D332 PR 1, #1485).
  - `phdb cluster status` shows each partition's `load_cpu`, `load_device` and `load`, averaged over the new `--balance-window` (default 300 s), for the coming balancer.
  - CPU is the partition's shard time plus sampled request CPU, over the node's cores.
  - Device is the partition's bytes at its files over the node's highest one-second rate in the window. The engine estimates it: reads served from the OS page cache count too.
  - Compare partitions with these, not absolute machine use.
