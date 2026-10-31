### Added
- `Pigeonhole::pinned_shard_cpus()` returns the CPUs the engine pinned its shard threads to (ICR 0101), and is empty when none is pinned. Keep your application's busy threads off them: a thread the OS places on a shard's CPU preempts the shard and delays the commits it serves. The guide's deployment notes say how, and the benchmarks now place their client threads this way.
