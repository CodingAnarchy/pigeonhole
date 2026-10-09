# Status

> **Maturity: experimental 0.x.** [0.1.0](https://github.com/CodingAnarchy/pigeonhole/releases/tag/v0.1.0) is on [crates.io](https://crates.io/crates/pigeonhole). The core engine (Phase 1) is complete and fault-tested in simulation. The wide-column model (Phase 2) is on `main` for 0.2.0; its gate is met as amended by [D193](design/decisions/phase-2.md#d193), with one documented gap ([#387](https://github.com/CodingAnarchy/pigeonhole/issues/387)). The on-disk format and the API may change before 1.0 ([`FORMAT.md`](../FORMAT.md)). The latency work (Phase 3) is still to come. Not recommended for production use yet. Keep this note in sync with each release.

Live progress against the [build plan](design/spec.md#build-plan). Updated by the coordinator as work lands.

The decision log is a [phase file plus an index](design/decisions/README.md) (`decisions/phase-1.md`); open questions from each crate live in [`design/questions/`](design/questions/) until the coordinator folds them in.

| Step | State |
|---|---|
| 1. Bootstrap — workspace, CI, contributor docs | done |
| 2. Interface freeze — public traits/types for every crate, `FORMAT.md` | done ([#1](https://github.com/CodingAnarchy/pigeonhole/pull/1); see [interfaces.md](design/interfaces.md)) |
| 3. Foundations — `format`, `io`, `sim` | done ([#2](https://github.com/CodingAnarchy/pigeonhole/pull/2), [#3](https://github.com/CodingAnarchy/pigeonhole/pull/3), [#4](https://github.com/CodingAnarchy/pigeonhole/pull/4)) |
| 4. Components — `pager`, `wal`, `memtable`, `cache`, `runtime`, `shm`, then `sst` | done — all seven merged, last [#30](https://github.com/CodingAnarchy/pigeonhole/pull/30) (`sst`) |
| 5. Assembly — `compaction`, `engine`, `pigeonhole`, `bench`; Phase 1 gate | done. Every crate merged, tablets on by default ([#168](https://github.com/CodingAnarchy/pigeonhole/pull/168)), the pre-gate edge-case review ([#90](https://github.com/CodingAnarchy/pigeonhole/issues/90)) and all its Phase 1 fixes landed. **Phase 1 gate passed on 2026-10-08** at `3abef59`: the milestone was empty and the 1–300 fault-injection sweeps (engine model/crash with tablets on, the fast balancer and in-flight I/O, engine tablets, public model) and full CI were green. Published as [0.1.0](https://github.com/CodingAnarchy/pigeonhole/releases/tag/v0.1.0) (15 crates). |

## Phases
Every gate also requires the phase's GitHub milestone to have no open issues (D62); check with `scripts/phase-gate.sh <phase>`.

| Phase | Gate | Milestone | State |
|---|---|---|---|
| 1. Core engine | Fault-injection suite green | [Phase 1](https://github.com/CodingAnarchy/pigeonhole/milestone/1) | **done** (gate passed 2026-10-08; [0.1.0](https://github.com/CodingAnarchy/pigeonhole/releases/tag/v0.1.0)) |
| 2. Wide-column model | Sparse-wide bench beats hand-keyed RocksDB, and SQLite EAV on throughput, get/put p99 and p99.9 (amended by [D193](design/decisions/phase-2.md#d193)) | [Phase 2](https://github.com/CodingAnarchy/pigeonhole/milestone/2) | **gate met under D193** (official runs 5–6, main cdfeb56: 28.7–30.4K ops/s vs SQLite 27.1–29.8K and RocksDB 20.4K; p99 389–395 µs vs RocksDB 709–791 µs; get p99 11–98 vs SQLite 152–156 µs; put p99 17 vs 172–178 µs; p99.9 528–541 vs 938–1,040 µs). Documented gap: wide, heavily overwritten row reads and scans have p99 ~2× SQLite EAV (the price of MVCC versions), tracked in [#387](https://github.com/CodingAnarchy/pigeonhole/issues/387) for Phase 3. Closing out: last in-flight PRs, the milestone, then 0.2.0. On `main`: counter families (D179, D186, D187), blob separation with values up to 4 GiB − 2 (D180, D188) and blob GC (D184), per-family compaction styles (D168), zstd (D175), a file at rest near its live size (D183, D185), the FUSE opt-in (D173), flush-time version GC (D191, D192), and the read/write performance work of #287. |
| 3. Latency engine | Goals-table p50/p99 met; within 1.5× of RocksDB | [Phase 3](https://github.com/CodingAnarchy/pigeonhole/milestone/3) | not started |
| 4. Hardening and 1.0 | File format frozen | [Phase 4](https://github.com/CodingAnarchy/pigeonhole/milestone/4) | not started |

## Tracked follow-ups
Deferred work is one GitHub issue each, labeled with its crate and phase and assigned to the phase's milestone (D62). The open list is the milestone itself:
[Phase 2](https://github.com/CodingAnarchy/pigeonhole/milestone/2) ·
[Phase 3](https://github.com/CodingAnarchy/pigeonhole/milestone/3) ·
[Phase 4](https://github.com/CodingAnarchy/pigeonhole/milestone/4).

Known Phase 1 limits carried forward: write throughput does not yet scale with shards ([#154](https://github.com/CodingAnarchy/pigeonhole/issues/154), Phase 3; measurements in [bench.md](bench.md)), open latency is about 14 ms against the 5 ms goal ([#158](https://github.com/CodingAnarchy/pigeonhole/issues/158)). A file at rest used to be 2–4× its live data; after `compact` and `shrink` it is now 1.05–1.16× above 5 MiB ([#185](https://github.com/CodingAnarchy/pigeonhole/issues/185), [D183](design/decisions/phase-2.md#d183)).
