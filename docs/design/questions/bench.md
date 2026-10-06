# pigeonhole-bench questions

The coordinator's review of PR #55 confirmed each entry below, with the changes noted in each section, ready to fold into `decisions.md`.

## Proposed decision: `Runner` gains two provided methods, `client` and `describe`
The frozen `Runner` trait takes `&mut self` per operation and has no way to hand out per-thread handles, so `WorkloadConfig::threads` could not be honored. The scaling gate needs concurrent writers. The trait also could not say which settings (shards, durability) a number was measured with.

**Interim behavior:** two provided methods with defaults, so existing implementors are unaffected and no frozen signature changes: `fn client(&self) -> Option<Box<dyn Client>>` (default `None`, so `run` uses one thread) and `fn describe(&self) -> String` (default empty). `Client` is a new `Send` trait with `execute(&mut self, &BenchOp)`. All four runners implement both. Also added (new items only): `run_detailed`, `RunRecord`, `Suite`, `Environment`, `Histogram`, `Tolerance`, `compare`, `Scaling`, `WorkloadKind::{ALL, name}`, `WorkloadConfig::{smoke, small}`, `PigeonholeRunner::{shards, memtable_budget, memory, sync}`, `MemoryBudget`, `BLOOM_BITS`, `RunOptions` (warmup), and `memory`/`sync` builders on every comparison runner.

**Status:** confirmed in the coordinator's review of PR #55.

## Q: The scaling gate cannot pass until tablets split or tables spread across shards
The gate wants write throughput at N shards ≥ 0.8 × N × single-shard on "workloads whose rows spread across tablets". Today a table is one tablet, tablets never split, and a tablet's shard is `tablet % shards`. The skewed workload writes one table, so every write lands on one shard whatever N is. Tablet splits are not in engine Milestone B (#37).

**Interim behavior:** `phdb-bench scaling` measures and reports exactly what the engine does (one table, N client threads, `shards(1)` against `shards(N)`), with the efficiency and a pass/fail line. The bench does not spread rows over several tables to fake tablets. Tracked in [#51](https://github.com/CodingAnarchy/pigeonhole/issues/51).

**Status:** confirmed in the coordinator's review of PR #55. The gate waits for tablets (#51). Each `scaling` run evaluates the efficiency half; the "no single-shard p99 regression" half is `compare` against a stored `scaling.json`, which the weekly workflow uploads.

## Q: Sparse-wide: what does "1M rows × 0 to 10K qualifiers, Zipfian" mean?
Read literally, a Zipfian number of qualifiers per row in 0..10K averages about 1,000 cells a row, which means about 10⁹ cells at 1M rows. That is not a sparse workload, and it cannot fit in memory.

**Interim behavior:** the 10K is the qualifier vocabulary, and qualifier popularity is Zipfian (feature-store shape: a few attributes appear in most rows, most are rare). Each row has 0 to 40 cells, uniformly (mean 20); rows with no cells are not written. Operations: 60% point gets of (Zipfian row, Zipfian qualifier), many of which miss as a sparse store should; 20% puts of 1 to 4 cells; 20% scans of 10 rows. The row count is `--records` (1M once #37 lands: [#52](https://github.com/CodingAnarchy/pigeonhole/issues/52)).

**Status:** confirmed in the coordinator's review of PR #55.

## Proposed decision: comparison durability is "written to the OS, not fsynced" unless `--sync`
Engines differ in what "commit" means. To compare like with like, every runner defaults to the level that survives a process crash but not power loss: Pigeonhole `Buffered`, RocksDB WAL without `sync`, SQLite WAL with `synchronous=NORMAL`, fjall `PersistMode::Buffer`. With `--sync`, every engine fsyncs each commit: Pigeonhole `Sync`, RocksDB `sync=true`, SQLite `synchronous=FULL`, fjall `SyncAll`.

**Interim behavior:** as above. The Goals-table "p99 commit < 200 µs with fsync batching" needs concurrent committers under `GroupSync`, which the bench does not measure yet: [#53](https://github.com/CodingAnarchy/pigeonhole/issues/53) (Phase 3).

**Status:** confirmed in the coordinator's review of PR #55. Tuning note from the review, now implemented: no engine is tuned, and none gets more memory than another. Every runner gets the same `MemoryBudget`, a write buffer and a read cache of 256 MiB each by default. That is Pigeonhole's memtable per shard plus its block cache; RocksDB's `write_buffer_size` plus LRU block cache; fjall's `max_memtable_size` plus `cache_size`; and SQLite's page cache of the sum. RocksDB gets a 10-bit bloom filter to match Pigeonhole's default `bloom_bits`; fjall builds filters by default. Every runner's `describe()` prints its budget, filter and durability, and the report shows them in the Settings column. Everything else stays at each engine's defaults.

## Proposed decision: reproducibility tolerance
**Interim behavior:** two runs on one machine agree when throughput and p50 are within ±15% and p99 is within ±30%. p99.9 and max are reported but not checked, because at 2×10⁵ operations p99.9 rests on about 200 samples. `compare` is symmetric: it flags improvements beyond tolerance as well as regressions. `phdb-bench compare --tolerance T` sets T for throughput and p50, and 2T for p99. Comparing runs from different machines or build profiles prints a warning, because the tolerance only applies within one machine. <!-- TOLERANCE -->

**Status:** confirmed in the coordinator's review of PR #55. Re-derived from several runs with the 5% warmup (below).

## Proposed decision: license exceptions for fjall's dependencies
fjall (2.x and 3.x) depends on `varint-rs` (0BSD) and `xxhash-rust` (BSL-1.0). Both licenses are permissive and MIT-compatible but are not in D6's list.

**Interim behavior:** `deny.toml` allows each license for its one crate only (`[[licenses.exceptions]]`), not workspace-wide. They enter only through `pigeonhole-bench`'s off-by-default `fjall` feature, and the bench crate is `publish = false`. If the exceptions are rejected, remove the fjall runner and report fjall numbers from its own benchmarks instead.

**Status:** confirmed in the coordinator's review of PR #55. The exceptions are scoped to pigeonhole-bench only (`publish = false`), and `deny.toml`'s comments say so. A published crate that pulls either crate in needs its own review.

## Proposed decision: CI builds the bench crate without the `rocksdb` feature
`ci.yml` runs `--all-features`, which would build RocksDB from C++ source on Linux, macOS and Windows (several minutes per job, plus libclang for bindgen).

**Interim behavior:** CI runs every `--workspace --all-features` step with `--exclude pigeonhole-bench`, then builds and tests the bench crate with `--features sqlite,fjall`, which needs only a C compiler. `cargo deny` still covers every feature (`[graph] all-features = true`). A separate workflow, `bench-rocksdb.yml`, builds and tests the bench crate with every feature, RocksDB included. It runs weekly, on pushes to `main` that touch `crates/bench/**`, and on demand, outside the PR-gating workflow, so the `rocksdb` feature cannot rot.

**Status:** confirmed in the coordinator's review of PR #55. Includes the build job.

## Q: YCSB fidelity limits imposed by `BenchOp`
`BenchOp::Get` reads one cell and `ReadModifyWrite` has no family or value, so:
- YCSB reads fetch one random field, not all ten (YCSB's `readallfields=true`). Updates write one field, as in YCSB.
- Read-modify-write is a get and then a put, not atomic in any engine, so `ycsb-f` refuses more than one client thread. The value written back is the old value with its first byte incremented (8 zero bytes if the cell is missing), identical across runners.
- YCSB D reads the latest records through a Zipfian over the initial record count. YCSB E picks scan starts from the loaded records, not the growing insert count.

**Interim behavior:** as above, documented in `docs/bench.md`. A family read op (`BenchOp::GetRow`) needs an ICR: [#54](https://github.com/CodingAnarchy/pigeonhole/issues/54).

**Status:** deferred to #54 (coordinator's review of PR #55).

## Q: Time series: TTL never expires during a run
`BenchOp::Put` carries no timestamp, so cells get commit-time timestamps, and a TTL short enough to expire data mid-run would make results depend on wall-clock timing.

**Interim behavior:** the `metric` family has a 1-day TTL, so reads pay the TTL check but nothing expires. Expiry under load (and FIFO-by-time compaction) needs a timestamped put op (ICR) and the engine's Phase 2 TTL compaction: [#54](https://github.com/CodingAnarchy/pigeonhole/issues/54).

**Status:** deferred to #54 (coordinator's review of PR #55).
