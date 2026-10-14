# Status

> **Maturity: experimental 0.x.** [0.2.2](https://github.com/CodingAnarchy/pigeonhole/releases/tag/v0.2.2) is on [crates.io](https://crates.io/crates/pigeonhole). The core engine (Phase 1) is complete and fault-tested in simulation. The wide-column model (Phase 2) is complete in 0.2.0; its gate is met as amended by [D193](design/decisions/phase-2.md#d193), with one documented gap ([#387](https://github.com/CodingAnarchy/pigeonhole/issues/387)). The on-disk format and the API may change before 1.0 ([`FORMAT.md`](../FORMAT.md)). The latency work (Phase 3) is in progress. Not recommended for production use yet. Keep this note in sync with each release.

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
| 2. Wide-column model | Sparse-wide bench beats hand-keyed RocksDB, and SQLite EAV on throughput, get/put p99 and p99.9 (amended by [D193](design/decisions/phase-2.md#d193)) | [Phase 2](https://github.com/CodingAnarchy/pigeonhole/milestone/2) | **done** (gate met under D193 in official runs 5–6 at main cdfeb56, released as [0.2.0](https://github.com/CodingAnarchy/pigeonhole/releases/tag/v0.2.0): 28.7–30.4K ops/s vs SQLite 27.1–29.8K and RocksDB 20.4K; p99 389–395 µs vs RocksDB 709–791 µs; get p99 11–98 vs SQLite 152–156 µs; put p99 17 vs 172–178 µs; p99.9 528–541 vs 938–1,040 µs). Documented gap: wide, heavily overwritten row reads and scans have p99 ~2× SQLite EAV (the price of MVCC versions), tracked in [#387](https://github.com/CodingAnarchy/pigeonhole/issues/387) for Phase 3 and judged on the reference machine's two baseline gate runs (main f8afbb2d), which replace runs 5–6 as the baseline ([D208](design/decisions/phase-3.md#d208)). In 0.2.0: counter families (D179, D186, D187), blob separation with values up to 4 GiB − 2 (D180, D188) and blob GC (D184), per-family compaction styles (D168), zstd (D175), a file at rest near its live size (D183, D185), the FUSE opt-in (D173), flush-time version GC (D191, D192), and the read/write performance work of #287. |
| 3. Latency engine | Goals-table p50/p99 met; within 1.5× of RocksDB; every roadmap item and Goals-table target in the [#406](https://github.com/CodingAnarchy/pigeonhole/issues/406) checklist done, on the reference hardware ([D197](design/decisions/phase-3.md#d197)) | [Phase 3](https://github.com/CodingAnarchy/pigeonhole/milestone/3) | **in progress; the gate has run twice on the reference machine (2026-10-11, 2026-10-12) and is not yet met.** Every roadmap item is in. Five of the Goals-table targets pass; the rest are being worked on: see [Phase 3 gate: latest state](#phase-3-gate-latest-state) below. The reference machine is a Hetzner AX102 ([#405](https://github.com/CodingAnarchy/pigeonhole/issues/405)), runbook [`crates/bench/baselines/phase3-gate`](../crates/bench/baselines/phase3-gate/README.md), results under [`results/`](../crates/bench/baselines/phase3-gate/results/). Numbers measured elsewhere (macOS, CI runners) are reported in PRs and [bench.md](bench.md) and never gate the phase. |
| 4. Hardening and 1.0 | File format frozen | [Phase 4](https://github.com/CodingAnarchy/pigeonhole/milestone/4) | not started |

## Phase 3 gate: latest state
Measured on the AX102 in the 2026-10-12 window at `a8b167e` ([results](../crates/bench/baselines/phase3-gate/results/2026-10-12/summary.md)) unless the row says otherwise. Changes merged since then are noted; they're judged in the next window.

| #406 item | Latest state |
|---|---|
| io_uring backend, default on Linux | **done.** `IoBackend::Auto` is the default ([D218](design/decisions/phase-3.md#d218)); application-owned mode has a completion fd and no engine threads ([D202](design/decisions/phase-3.md#d202), #408); readahead 16 blocks, no merging ([D222](design/decisions/phase-3.md#d222), #431). |
| Direct I/O | **done** (#403). |
| Owned block cache, tuned | **done** (#18, #57, #372). |
| Row cache | **done:** on by default, 64 MiB ([D216](design/decisions/phase-3.md#d216)). |
| Group commit tuning, group sync depth | **done:** depth 2 on Linux, 1 on macOS ([D221](design/decisions/phase-3.md#d221)), rollover and pin work off the commit path ([D200](design/decisions/phase-3.md#d200), [D206](design/decisions/phase-3.md#d206)); `wal_pin_bytes` stays 2× the budget ([D229](design/decisions/phase-3.md#d229)). The target itself is the group-commit row below (#64). |
| Async API | **done** (#42; [D196](design/decisions/phase-3.md#d196)). |
| Point get in memory: p50 < 2 µs, p99 < 10 µs | **pass:** p50 0.86 µs, p99 7.23 µs. |
| Memtable lookup ≤ 300 ns (#17) | **re-scoped:** judged at 2^18 entries, the largest one memtable reaches under the defaults ([D230](design/decisions/phase-3.md#d230), #613); first measured in the next window. 1M entries reported: 446–479 ns. |
| Point get on cold data: one I/O | **1.106 device reads per get** (target 1). [#611](https://github.com/CodingAnarchy/pigeonhole/pull/611) (ICR 0039) removes the next-block read of a row that ends a block: 1.059 → 0.99 per get on a 4 GiB table (macOS); the 64 GiB box run is pending. |
| Durable group commit: > 1M cells/s, p99 < 200 µs | **throughput passes** (7.00M cells/s); **p99 fails:** 241 µs at 16 shards × 16 in flight ([D226](design/decisions/phase-3.md#d226)), engine side 131 µs. Since then: commits are visible once applied ([D227](design/decisions/phase-3.md#d227)), shard threads are pinned by default ([D235](design/decisions/phase-3.md#d235)), and D236's commit path without engine-wide lines is in review (#626). |
| Ordered scan from cache: > 1 GB/s per core | **pass:** 6.02 GB/s ([D205](design/decisions/phase-3.md#d205)). |
| Open to first read: < 5 ms | **pass:** 2.45–2.68 ms (p99 5.0–5.5 ms). |
| Thread-per-core scaling: ≥ 0.8 × N | **failed in the old form** (N = 8: 0.73; N = 16: 0.54). The gate is split since ([D231](design/decisions/phase-3.md#d231)): engine scaling on a pre-split table with the balancer off, and balancer placement by an open-loop check (busiest share ≤ 1.25/N, [D223](design/decisions/phase-3.md#d223)). The drivers pin their threads ([D235](design/decisions/phase-3.md#d235)) and the bench settles before measuring ([D233](design/decisions/phase-3.md#d233)). The split form is measured in the next window (#154). |
| Within 1.5× of RocksDB on every workload | **pass** (both runs). |
| D193 instruction ceilings; #387 | **ceilings hold** (CI). #387's ratio to SQLite EAV is now within 1.5× (row read 1.33–1.38×, scan 1.18–1.25×); the issue stays open for the owner to close. |
| Run-to-run reproducibility | **fails** (`all-uring-compare.txt`); the bench now settles every engine before measuring and flags pairs whose background work differs ([D233](design/decisions/phase-3.md#d233)). |

## Landed on `main` since 0.2.0 (Phase 3), by topic
- **Visibility and durability:**
  - a commit is visible once applied, before its fsync, and returns once durable at its level ([D227](design/decisions/phase-3.md#d227) part 1, amends D19);
  - durable snapshots and `backup_durable` keep the old view (part 2, ICR 0037; shared-memory layout 2);
  - memtables freeze only at the durable seqno.
- **Failure handling:**
  - `ErrorCode::OutcomeUnknown` (ICR 0038): `Closed` always means not committed;
  - after a shard's death new commits are refused;
  - a failed fsync never counts as durable;
  - the health check `Pigeonhole::is_failed()` / `failure()` and the [recovery guide](guide/recovery.md) (ICR 0040, #628);
  - cross-shard close hangs fixed ([D225](design/decisions/phase-3.md#d225), #591, #597).
- **Placement and threads:**
  - one shard per physical core by default ([D220](design/decisions/phase-3.md#d220));
  - engine-owned shard threads pinned by default ([D235](design/decisions/phase-3.md#d235));
  - balancer packing, damping and closed-loop rules ([D214](design/decisions/phase-3.md#d214), [D217](design/decisions/phase-3.md#d217), [D224](design/decisions/phase-3.md#d224)).
- **Write path:**
  - the L0 write stall starts at 2× `l0_trigger` ([D234](design/decisions/phase-3.md#d234));
  - group syncs shared by concurrent durable commits ([D207](design/decisions/phase-3.md#d207), [D221](design/decisions/phase-3.md#d221));
  - a WAL reopen skips the epochs lost headers would have had ([D209](design/decisions/phase-3.md#d209));
  - the WAL frames records without a staging copy (#608).
- **Read path:**
  - a point or row read whose row ends a block no longer reads the next one (ICR 0039, #611);
  - SST sources skip a finished column from block headers (ICR 0041, #627);
  - stale-tail skips ([D194](design/decisions/phase-3.md#d194), [D199](design/decisions/phase-3.md#d199));
  - the row cache ([D201](design/decisions/phase-3.md#d201), [D216](design/decisions/phase-3.md#d216)).
- **Gate method:**
  - group commit judged under full load ([D213](design/decisions/phase-3.md#d213), [D219](design/decisions/phase-3.md#d219), [D226](design/decisions/phase-3.md#d226));
  - the scaling gate split ([D231](design/decisions/phase-3.md#d231));
  - the bench settles before measuring ([D233](design/decisions/phase-3.md#d233));
  - the memtable target's size ([D230](design/decisions/phase-3.md#d230)).
- **Considered and not adopted:** [D228](design/decisions/phase-3.md#d228), [D232](design/decisions/phase-3.md#d232).
- **Releases from `release/0.2.x`:**
  - [0.2.1](https://github.com/CodingAnarchy/pigeonhole/releases/tag/v0.2.1) (D209);
  - [0.2.2](https://github.com/CodingAnarchy/pigeonhole/releases/tag/v0.2.2): the cross-shard close fixes.

## Tracked follow-ups
Deferred work is one GitHub issue each, labeled with its crate and phase and assigned to the phase's milestone (D62). The open list is the milestone itself:
[Phase 2](https://github.com/CodingAnarchy/pigeonhole/milestone/2) ·
[Phase 3](https://github.com/CodingAnarchy/pigeonhole/milestone/3) ·
[Phase 4](https://github.com/CodingAnarchy/pigeonhole/milestone/4).

Known Phase 1 limits carried forward: write throughput does not yet scale with shards ([#154](https://github.com/CodingAnarchy/pigeonhole/issues/154), Phase 3; the gate now measures application-owned shards writing inline, [D204](design/decisions/phase-3.md#d204); measurements in [bench.md](bench.md)). Open latency ([#158](https://github.com/CodingAnarchy/pigeonhole/issues/158)) is fixed on `main`: a clean reopen waits for no flush, and the first durable commit after it waits for the new WAL files' syncs instead ([D203](design/decisions/phase-3.md#d203)); the 5 ms goal is checked on the reference machine. A file at rest used to be 2–4× its live data; after `compact` and `shrink` it is now 1.05–1.16× above 5 MiB ([#185](https://github.com/CodingAnarchy/pigeonhole/issues/185), [D183](design/decisions/phase-2.md#d183)).
