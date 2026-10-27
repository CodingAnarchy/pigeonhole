### Changed
- Engine-owned shards now run on driver threads sized to the load (D289, #760). Up to one thread per physical core runs the shards. A light load shrinks to a few busy threads, which rarely sleep, so its commits stop waiting for a sleeping thread to be woken (about 400–550 µs at p99 on sparse-wide; 34–38 µs on one or two threads). A heavy load keeps one per core. `Options::shard_threads(k)` fixes the count instead.
- Flush and compaction threads now scale with the physical cores (a quarter of them by default), no longer stopping at four (D289, ICR 0081).

### Added
- `Options::cpu_profile(CpuProfile)` (D289, ICR 0081). `CpuProfile::Embedded` (the default) leaves CPU for an application in the same process. `CpuProfile::Server` is for a database whose clients are elsewhere, such as another container, and gives flush and compaction half the cores.
