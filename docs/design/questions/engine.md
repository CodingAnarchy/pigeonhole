# Engine questions

## Proposed decision: shard and compaction threads are not pinned unless asked (#142)
The spec (§ Thread-per-core) says shard threads are "pinned with CPU affinity", and `EngineOptions::pin_threads` defaulted to on with no public way to turn it off. The pin is shard `i` → the `i`-th CPU (wrapping) of the affinity set the new thread inherits from the opener, so it goes wrong whenever the database does not own the whole machine (edge-case review 3-4 §3.2, 8-9 F6):
- two engine-owned databases in one process both put shard 0 on the first CPU;
- containers limited by a CPU quota (not a cpuset) see every host CPU, so each one pins to the host's first CPUs;
- an opener already pinned to one CPU puts every shard, and the I/O pool, on that CPU.

Detecting contention instead ("pin only when the affinity set covers the shards and is not narrowed") still fails the first two cases: neither another database in the process nor a neighbouring container is visible in the affinity set, and a cgroup quota is invisible to it too.

**Interim behavior:** `pin_threads` defaults to **off** in both `EngineOptions` and `pigeonhole::Options`; `Options::pin_threads(true)` opts in, with the same mapping as before (the opener's affinity set, wrapping), documented as "only when this database owns those CPUs". Application-owned mode ignores it (shards run on the caller's threads). Unpinned shard threads keep their one-shard-per-thread ownership, so the lock-free write path is unchanged; only the OS scheduler may migrate them. The spec's "pinned with CPU affinity" becomes "one thread per shard, pinned on request".
