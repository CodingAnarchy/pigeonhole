# Status

Live progress against the [build plan](design/spec.md#build-plan). Updated by the coordinator as work lands.

| Step | State |
|---|---|
| 1. Bootstrap — workspace, CI, contributor docs | in progress |
| 2. Interface freeze — public traits/types for every crate, `FORMAT.md` | not started |
| 3. Foundations — `format`, `io`, `sim` | not started |
| 4. Components — `pager`, `wal`, `memtable`, `cache`, `runtime`, `shm`, then `sst` | not started |
| 5. Assembly — `compaction`, `engine`, `pigeonhole`, `bench`; Phase 1 gate | not started |

## Phases
| Phase | Gate | State |
|---|---|---|
| 1. Core engine | Fault-injection suite green | in progress |
| 2. Wide-column model | Sparse-wide bench beats SQLite EAV and hand-keyed RocksDB | not started |
| 3. Latency engine | Goals-table p50/p99 met; within 1.5× of RocksDB | not started |
| 4. Hardening and 1.0 | File format frozen | not started |
