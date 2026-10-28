### Added
- `Options::adaptive_shard_threads(true)`, experimental and off by default: a load controller sizes the shard threads to the load, up to one per shard (D289). Its resize policy is still being tuned across load changes (#1015).
- `Options::cpu_profile(CpuProfile)` (D289, ICR 0081). `CpuProfile::Embedded` (the default) leaves CPU for an application in the same process. `CpuProfile::Server` is for a database whose clients are elsewhere, such as another container, and gives flush and compaction half the cores.

### Changed
- Flush and compaction threads now scale with the physical cores (a quarter of them by default), no longer stopping at four (D289, ICR 0081).
