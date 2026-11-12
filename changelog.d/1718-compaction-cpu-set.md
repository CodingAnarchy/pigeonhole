### Added
- `Options::compaction_cpu_set` confines the compaction threads to a set of CPUs instead of pinning each, and `Pigeonhole::pinned_compaction_cpus()` lists the CPUs they're pinned to (ICR 0134). `EngineMetrics::threads_confined` counts the confined ones.
