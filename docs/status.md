# Status

Live progress against the [build plan](design/spec.md#build-plan). Updated by the coordinator as work lands.

| Step | State |
|---|---|
| 1. Bootstrap — workspace, CI, contributor docs | done |
| 2. Interface freeze — public traits/types for every crate, `FORMAT.md` | done ([#1](https://github.com/CodingAnarchy/pigeonhole/pull/1); see [interfaces.md](design/interfaces.md)) |
| 3. Foundations — `format`, `io`, `sim` | done ([#2](https://github.com/CodingAnarchy/pigeonhole/pull/2), [#3](https://github.com/CodingAnarchy/pigeonhole/pull/3), [#4](https://github.com/CodingAnarchy/pigeonhole/pull/4)) |
| 4. Components — `pager`, `wal`, `memtable`, `cache`, `runtime`, `shm`, then `sst` | done — all seven merged, last [#30](https://github.com/CodingAnarchy/pigeonhole/pull/30) (`sst`) |
| 5. Assembly — `compaction`, `engine`, `pigeonhole`, `bench`; Phase 1 gate | `compaction` ([#36](https://github.com/CodingAnarchy/pigeonhole/pull/36)), `engine` Milestone A ([#41](https://github.com/CodingAnarchy/pigeonhole/pull/41)) and the public `pigeonhole` API ([#47](https://github.com/CodingAnarchy/pigeonhole/pull/47)) merged; engine Milestone B ([#37](https://github.com/CodingAnarchy/pigeonhole/issues/37)) in progress; then `bench` and the Phase 1 gate |

## Phases
Every gate also requires the phase's GitHub milestone to have no open issues (D62); check with `scripts/phase-gate.sh <phase>`.

| Phase | Gate | Milestone | State |
|---|---|---|---|
| 1. Core engine | Fault-injection suite green | [Phase 1](https://github.com/CodingAnarchy/pigeonhole/milestone/1) | in progress |
| 2. Wide-column model | Sparse-wide bench beats SQLite EAV and hand-keyed RocksDB | [Phase 2](https://github.com/CodingAnarchy/pigeonhole/milestone/2) | not started |
| 3. Latency engine | Goals-table p50/p99 met; within 1.5× of RocksDB | [Phase 3](https://github.com/CodingAnarchy/pigeonhole/milestone/3) | not started |
| 4. Hardening and 1.0 | File format frozen | [Phase 4](https://github.com/CodingAnarchy/pigeonhole/milestone/4) | not started |

## Tracked follow-ups
Deferred work from the [decisions audit](design/decisions.md), one GitHub issue each (labels: phase, crate; milestone: the phase).

| Issue | Crate | Phase | Decision | Summary |
|---|---|---|---|---|
| [#14](https://github.com/CodingAnarchy/pigeonhole/issues/14) | engine | 1 | D54, ICR 0001 | Map `format::Error::InvalidArgument` to `InvalidArgument` |
| [#15](https://github.com/CodingAnarchy/pigeonhole/issues/15) | engine | 1 | D29 | Benchmark the small-value copy threshold and `arc-swap` gets |
| [#16](https://github.com/CodingAnarchy/pigeonhole/issues/16) | engine | 1 | D49 | Finish or persist flush and manifest work before `Runtime::shutdown` |
| [#20](https://github.com/CodingAnarchy/pigeonhole/issues/20) | engine | 1 | D37 | Writer open order; writer-byte check in last-one-out cleanup |
| [#22](https://github.com/CodingAnarchy/pigeonhole/issues/22) | engine | 1 | D40 | Refuse `compaction_cores` in application-owned mode |
| [#23](https://github.com/CodingAnarchy/pigeonhole/issues/23) | engine | 1 | D57, D58, D60, D61 | Honor the pager's contracts (clean flag, poisoning, shrink, reclaim) |
| [#24](https://github.com/CodingAnarchy/pigeonhole/issues/24) | engine | 1 | D59 | Refuse an interrupted `Pager::create` with `Corruption`; never delete the file |
| [#26](https://github.com/CodingAnarchy/pigeonhole/issues/26) | engine | 1 | D39 | Families in creation or requested order |
| [#21](https://github.com/CodingAnarchy/pigeonhole/issues/21) | compaction | 1 | D41 | `I64Add` fails on a non-`i64` base |
| [#25](https://github.com/CodingAnarchy/pigeonhole/issues/25) | compaction | 1 | D38 | Timestamp-only `CellDelete` in the resolver and GC |
| [#19](https://github.com/CodingAnarchy/pigeonhole/issues/19) | wal | 3 | D30 | Fully off-thread segment rollover (drop the D30 exception) |
| [#17](https://github.com/CodingAnarchy/pigeonhole/issues/17) | memtable | 3 | D52 | 1M-entry lookup above the 300 ns target |
| [#18](https://github.com/CodingAnarchy/pigeonhole/issues/18) | cache | 3 | D50 | 10-thread hit cost |

