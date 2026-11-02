# Benchmarks

`pigeonhole-bench` (`crates/bench`) measures every performance gate in the [roadmap](design/spec.md#roadmap-risks-open-questions). It runs YCSB A–F plus four wide-column workloads against Pigeonhole and, optionally, RocksDB and SQLite. It reports p50/p99/p99.9 latency and throughput as JSON and as a markdown table, and it compares two runs for the reproducibility gate.

## Running

```sh
# Every workload against Pigeonhole, default ("small") size:
cargo run -p pigeonhole-bench --release -- all --json out.json --markdown out.md

# One workload against every engine (comparison engines are cargo features):
cargo run -p pigeonhole-bench --release --features rocksdb,sqlite -- \
    ycsb-a --engine all

# The scaling gate: skewed writes at 1 shard and N shards (default: all cores):
cargo run -p pigeonhole-bench --release -- scaling --shards 8

# Reproducibility: run twice, then compare (exit code 1 if they disagree):
cargo run -p pigeonhole-bench --release -- all --json a.json
cargo run -p pigeonhole-bench --release -- all --json b.json
cargo run -p pigeonhole-bench --release -- compare a.json b.json
```

`phdb-bench --help` lists every option. The main ones:

| Option | Meaning |
|---|---|
| `--engine LIST` | `pigeonhole` (default), `rocksdb`, `sqlite`, or `all` |
| `--scale smoke\|small\|full\|larger-than-ram` | Preset size; `small` is the default, `smoke` is what `cargo test` runs, `full` is the spec's scale, `larger-than-ram` is `full` with a tiny memory budget (below) |
| `--records N`, `--ops N`, `--value-len N`, `--threads N`, `--seed N` | Override the preset |
| `--warmup F` | Unrecorded warmup, as a fraction of `--ops` (default 0.05; `scaling`: 1.0, see below), run on the measured phase's client threads and repeated until the tablet placement settles (D242, below) |
| `--min-secs S` | Measure at least S seconds (default 0: the configured operations once). Inline runs (`scaling`, and `group-commit`'s D213 run) repeat the measured writes (D219); client runs continue with the workload stream's next operations, in passes sized from the rate so far, each generated before its clock starts (D243). A fixed count measures a faster store for less time, too short for its flushes and compactions to show |
| `--max-secs S` | The longest measured phase (D247): past it no client (or inline shard thread) starts another operation, the run reports what it measured, and the record's `capped` says it hit the bound. The warmup is bounded by its own cap. Without it the configured operations, and `--min-secs`, decide |
| `--inline-ops N` | `group-commit`: the D213 run's commits, apart from the client runs' `--ops` (D219; the gate judges it on 2M) |
| `--in-flight N` | Inline runs: commits each shard's thread keeps in flight (default 16, the judged shape). Fewer trade throughput for latency: the gate reports 4 and 8 beside the judged 16 (D226), and a run at another count prints "reported, not judged" |
| `--offered-rate R` (client runs) | Offer the measured operations at about R per second in all (the placement check uses the same flag for its open-loop writes), each client paced to R / clients (the warmup is not paced). Latency is then the store's service time at that offered load rather than at its maximum: #759 reports read p99 both at each store's maximum and with Pigeonhole paced to SQLite's rate. The JSON record carries `detail.offered_rate` |
| `--write-buffer B`, `--cache B` | Every engine's memory budget (default 64 MiB write buffer, 256 MiB read cache; see below) |
| `--cache-model per-store\|total` | How `--cache` maps onto stores whose read memory is not one shared cache (default `per-store`, as before #759; see below) |
| `--shards N` | Pigeonhole shards |
| `--sync` | Fsync every commit on every engine (default: buffered, see below) |
| `--async N` | Pigeonhole: each client thread keeps N operations in flight through the async API (D247, reported): puts as `commit_async`, row reads as `read_async`, gets as `get_async`, polled by the thread itself with no executor; other operations run one at a time in between. Latency is each operation's submission to resolution; the settings read `async=N` |
| `app-owned` (command) | Application-owned mode on the default options (D247, reported): skewed-multi-shard writes inline from each shard's own thread (16 in flight, pinned, as `scaling` does), but with no pre-split, so the balancer and packing run, and every fifth commit of a thread a `WriteBatch` of its write and 1–3 rows picked across the stream, most owned by other shards. Durable levels load buffered; the warmup waits for the placement as client runs do. A commit refused with `Busy` is resubmitted (at most 20 times) and counted in the record's `busy_retries`, and its latency runs from its first submission |
| `--reader-processes N` | Pigeonhole: N reader processes beside the measured phase (#683, D247 item K, reported). Each is `phdb-bench reader`, attached through `Pigeonhole::open_reader`, reading whole `ycsb` rows Zipfian over the loaded records until the phase ends. The record's `reader_processes` has their reads (count, p50, p99, p99.9), reads per second, misses and errors; the writers' own numbers beside it show the readers' effect on them. The gate's `contended` step runs it on ycsb-a at both durabilities |
| `--riders` | Pigeonhole: background work beside the measured phase (D247, reported): a thread scanning 10 rows at a time under a snapshot re-taken every 100 ms, another under a durable snapshot, and a third taking a durable backup 2 s in and then a full compaction. The record's `riders` has their latencies and durations; every record also has `close_ms`, the close after the measured phase |
| `--durability MODE` | `buffered` (the default), `sync` (as `--sync`), `group-sync` or `default`. `group-sync`: Pigeonhole commits `GroupSync`. `default`: Pigeonhole keeps `Options::default()`'s durability, which is `GroupSync`, and the run's settings read `group-sync(default)`. With either, the other stores fsync every write, as in `group-commit`, and every engine loads at buffered durability, then switches after the settle step, so the warmup and the measured phase run at the judged level (settings read `load=buffered`). The bench's own default stays `buffered`, so existing results compare; D247's `contended` step measures `default` |
| `--no-tablet-changes` | Pigeonhole: keep each table one tablet on one shard (`Options::tablet_changes(false)`; by default tablets split, merge and move between shards, so one table's writes spread over every shard) |
| `--row-cache B` | Pigeonhole: a row cache of B bytes (`Options::row_cache`, D201); 0 turns it off. Without it the library default applies (on, D216) and the run's description names no row cache, so results stay comparable with earlier ones. The gate window (#405) runs `ycsb-c` and `ycsb-a` with it off beside the default |
| `--json PATH`, `--markdown PATH` | Write results |
| `--tolerance T` | `compare`: ±T on throughput and p50, ±2T on p99 (default 0.20); a latency also passes within `T × 15 µs` at p50 and `T × 30 µs` at p99 (D259) |

Always use `--release`: a debug build prints a warning and its numbers mean nothing.

### Comparison engines

The RocksDB and SQLite runners sit behind the cargo features `rocksdb` and `sqlite`, both off by default. (A fjall runner was dropped by the owner on 2026-10-29: no gate or D280 bar used it. A pure-Rust comparison with rebuilt fjall and redb runners returns after 1.0 in [#1106](https://github.com/CodingAnarchy/pigeonhole/issues/1106).) `sqlite` compiles the bundled SQLite (needs a C compiler). `rocksdb` compiles RocksDB from C++ source and runs bindgen. On macOS without Xcode, point bindgen at the Command Line Tools' libclang:

```sh
export LIBCLANG_PATH=/Library/Developer/CommandLineTools/usr/lib
export DYLD_FALLBACK_LIBRARY_PATH=$LIBCLANG_PATH
```

**Transactional workloads (D274).** Every store runs transfer's and ycsb-f's atomic operations with its own transaction API; no store is wrapped in a bench-side lock. With `rocksdb`, those two workloads run on RocksDB's `OptimisticTransactionDB` (`RocksDbTxnRunner`), the judged baseline; every other workload runs on its plain `DB`. `--engine pigeonhole-txn` runs ycsb-f's read-modify-write in a Pigeonhole `Transaction`, the row judged against it. `--engine rocksdb-txn` runs them on the pessimistic `TransactionDB` instead (`get_for_update` locks, the default lock timeout, retried on `Busy` or `TimedOut`), reported beside the optimistic row. Each store retries the way it does out of the box: Pigeonhole's transactions run through `transact()` with the default `RetryPolicy`, and RocksDB retries at once. `--txn-backoff US` gives Pigeonhole's transactions `RetryPolicy`'s backoff from `US` µs (0: none) instead; such runs are reported and never judged.

PR-gating CI builds and tests the bench crate with `sqlite` only, so it never needs a C++ toolchain. A separate workflow, `bench-rocksdb.yml`, builds and tests every comparison runner, RocksDB included. It runs weekly, on pushes to `main` that touch `crates/bench/**`, and on demand, so the `rocksdb` feature cannot rot. Locally: `cargo test -p pigeonhole-bench --all-features`.

### Presets

| Preset | Records | Operations | Use |
|---|--:|--:|---|
| `smoke` | 1,000 | 2,000 | What `cargo test` runs; seconds for the whole suite |
| `small` (default) | 50,000 (group-commit: 1,000) | 200,000 (group-commit: 20,000) | A quick check, seconds per workload |
| `full` | 1,000,000 (adjacency: 2,000,000 edges; group-commit: 1,000) | 1,000,000 (skewed-multi-shard: 2,000,000; group-commit: 200,000) | The spec's scale: 1M sparse-wide rows, YCSB with 1M records |
| `larger-than-ram` | same as `full` | same as `full` | The data set is 75–85× the engine's memory budget; gets are split into cold and hot (below) |

Memtables flush to SSTs, so no preset is bounded by memory. At `full` size the data set is several times the 64 MiB write buffer (the runner's default, equal to the engine's), so flushes and compactions run during the load and the measurement, and a run completes without `Busy`. Sparse-wide at `full` loads about 20M cells, YCSB about 10M, and takes minutes per engine. `full` is **not** larger than RAM on a typical workstation, so by itself it does not measure the spec's cold-read target (one I/O per get on data larger than memory). `larger-than-ram` does the next best thing on a laptop.

### The larger-than-RAM preset

`--scale larger-than-ram` runs the `full` sizes with a default memory budget of an **8 MiB write buffer and a 16 MiB read cache** (24 MiB, against 320 MiB by default), unless `--write-buffer` or `--cache` say otherwise. One million YCSB rows are about 1.8–2 GiB on disk, so the data set is 75–85× what the engine may hold in its memtable and block cache, and nearly every first read of a row has to leave the engine's cache. Each result prints its store size on disk beside the budget.

**What this is not.** It bounds the *engine's* memory; it does not bound the machine's. The OS page cache (most of 24 GiB here) still holds the files, so a cold get usually costs a page-cache copy, not a device read. That makes the cold numbers a measure of the engine's miss path (index and filter lookup, block decode, a `pread`), not of NVMe latency. For a true device-bound number, raise `--records` until the store exceeds RAM, or run on reference hardware (D5). The preset exists so the miss path is exercised on any machine, and in CI, without a machine-sized file.

**Device-bound cold gets (#87).** `examples/coldget.rs` and the gate-window step `baselines/phase3-io/cold-get.sh` measure the spec's cold-read target without a store larger than RAM. A 64 GiB table of 1 KiB cells, loaded in random key order so it has a real leveled tree, is read with a block cache of 1/64 of the table (1 GiB; D215: the cache must hold the SST indexes and filters it pins plus the index partitions):

- **with direct I/O** (`PIGEONHOLE_DIRECT=1`), so every block the cache misses is read from the device;
- **buffered, in a cgroup v2 scope capped at 8 GiB** with the page cache emptied first, so most of the table cannot be cached: the default path's cost.

Each run reports cold-get p50/p99/p99.9 for present rows and for absent ones (rows between two present ones, which only a filter rules out), block-cache misses per get, and the device's completed reads per get (`/sys/block/DEV/stat`). Both backends, 1 and 16 threads, three runs each: about 45 minutes on the reference machine (D5), including the load. On macOS, `F_NOCACHE` still serves pages the file system already caches (a just-written table is cached), so a local run checks the miss path, not device latency. The reference results are in [Cold-get results](#cold-get-results-reference-machine-ax102).

**Cold and hot gets.** Whenever a workload has gets, the report adds a second table (and `detail.reads` in the JSON). A get is **cold** when it is the first get of its row in the run (the warmup counts as earlier), **hot** when the row was read before. The classification is by row, decided before the clock starts, and is the same for every engine. It is an approximation: a "hot" row can have been evicted since, a "cold" row can share a block with a row read earlier, and the OS page cache can serve either. Under Zipfian access a minority of gets are cold (16% of gets in the `ycsb-c` run below); use a uniform workload or more records for more.

**Stalls.** The engine stalls writers while a flush frees memtable room and answers `Busy` only when the stall outlasts its timeout (30 s). The Pigeonhole runner retries a write after `Busy` (up to 20 times) instead of failing the run, and the second table reports the retries. A run with zero retries completed without ever refusing a write. Stall time shows up in the latency tail (max, p99.9) and in throughput; the stall table reports the engine's stall counters (`engine_metrics`, ICR 0015).

Tablets do not split yet (#38), so one table's writes use one shard whatever `--shards` is.

## Workloads

Every workload is deterministic for a seed (default `0x5EED`, recorded in every result). After the load, every engine **settles** (D233): it flushes its write buffer (Pigeonhole and RocksDB flush, SQLite checkpoints its WAL), then the run waits until its background work has been quiet for 1 s (no flush or compaction running or completed, and for Pigeonhole no work time on the shard threads), at most 60 s, with a warning and `detail.settle.settled: false` past that. Then a warmup of 5% of `--ops` runs unrecorded on the measured phase's own client threads (D242): these are the first operations of the same seeded stream, and the measured operations follow them. The warmup repeats until the store's tablet placement has gone 2 s without a split, merge or move, at most 60 s; a store without shards warms for 2 s. A pending growth, or a pending first pack from all shards, counts as a change, so a run that packs measures its packed placement (D242's amendments). The JSON records `detail.warmup_secs`, `detail.warmup_settle_secs` (the seconds into the warmup at which the placement settled) and `detail.warmup_settled`. All measured operations are generated before the clock starts, so generation cost is never measured.

**Engine-side commit latency.** A Pigeonhole client run also records `detail.engine_commit`: the measured phase's commits at its most-used durability level as the engine times them, from submission to resolution on the shard (p50/p99/p99.9, half-power-of-two buckets, each value its bucket's lower bound). A put's latency in `by_type` above that is the hand-back to the client thread, so the two together show whether a write tail is inside the engine or in getting the answer back (#760). The inline drivers record the same field.

| Workload | Data | Operation mix |
|---|---|---|
| `ycsb-a` | `records` rows × 10 fields (`field0`..`field9`) of `value-len` bytes, keys `user<fnv(i)>` as in YCSB | 50% read, 50% update; Zipfian (θ 0.99, scrambled) |
| `ycsb-b` | same | 95% read, 5% update |
| `ycsb-c` | same | 100% read |
| `ycsb-d` | same | 95% read-latest, 5% insert |
| `ycsb-e` | same | 95% scan of 1–100 rows (uniform), 5% insert |
| `ycsb-f` | same | 50% read, 50% read-modify-write, atomic across clients in every store, each with its own API (D274): Pigeonhole `commit_if` on the value read, retried on a lost race (counted as a conflict; the idiomatic fast path, reported), or a `Transaction` (`--engine pigeonhole-txn`, the judged row); RocksDB an `OptimisticTransactionDB` transaction (`rocksdb`) or a `TransactionDB` one (`rocksdb-txn`); SQLite an immediate transaction. Runs at any thread count (D247) |
| `sparse-wide` | `records` rows, 0–40 cells each (mean 20), qualifiers from a 10K vocabulary with Zipfian popularity | 40% point get (many miss, as in a sparse store), 20% read of a whole row's family, 20% put of 1–4 cells, 20% scan of 10 rows |
| `time-series-ttl` | `records / 100` entities × 100 points; row `ts:<entity>:<reversed time>`, so a scan from the entity prefix returns the newest point first; family with a 1-day TTL; points carry event timestamps, and a quarter of the loaded ones are already older than the TTL | 40% timestamped append, 40% scan of the newest 10 points, 20% get of a recent point (a miss when expired) |
| `adjacency` | about `records / 40` vertices; row `v:<id>`, qualifiers `edge:<dst>`, out-degree `1 + Zipf(256)` (mean ≈ 40), Zipfian destinations | 80% scan of one vertex's edges, 10% scan of 10 vertices, 10% add an edge |
| `skewed-multi-shard` | `records` rows, one cell each | 100% Zipfian writes from `threads` client threads (default 4) |
| `group-commit` | 1,000 rows (every preset but `smoke`) × 4 fields | 100% durable commits of 4 cells to a uniformly chosen row: `GroupSync` on Pigeonhole, fsync per write elsewhere (below). On Pigeonhole it first runs the D213 shape: one shard per core (`--shards`), the table split over every shard, each shard's thread keeping 16 commits in flight inline. Then, from concurrent clients, at 1, 4 and 16 threads without `--threads`; 20,000 commits at `small`, 200,000 at `full` |
| `cross-shard-contention` | `records` rows dealt over 16 partitions, one cell each; Pigeonhole gives each partition a table of its own (`xs00`–`xs15`), the other stores prefix the row key with it (`xs:<partition>:`) | From 32 client threads (`threads`), 50% one-row commits to a uniformly chosen partition, 50% commits of one row in each of 2–4 consecutive partitions (`cross-shard commit` in the per-type table): on Pigeonhole one `WriteBatch` across the tables, on RocksDB one multi-key `WriteBatch`, on SQLite one transaction. With tablet changes off the partition tables sit one per shard, so every multi-row commit is a two-phase commit; `all` runs Pigeonhole that way too, beside the default (D246) |
| `counters` | `records` rows, each one counter, loaded at 1 | From 16 client threads (`threads`), 90% increments by one of a Zipfian row's counter (`incr`), 10% reads of one (`counter read`). Pigeonhole: a counter family's `incr`, which does not read; RocksDB: a merge with an associative `i64` add; SQLite: an upsert. Not in `all`; the gate's `contended` step runs it (D247) |
| `transfer` | `records` accounts, each an `i64` balance (`bal`) | From 16 client threads, each operation moves one unit between two distinct Zipfian accounts in one transaction that reads both (`transfer`). Pigeonhole: an optimistic transaction through `transact()` and its default `RetryPolicy`, with the conflicts counted (`txn_conflicts` in the record). RocksDB (D274): `rocksdb` is an `OptimisticTransactionDB` transaction reading both with `get_for_update`, retried on `Busy` (the judged baseline); `rocksdb-txn` is a pessimistic `TransactionDB` one locking both in key order (reported). SQLite: an immediate transaction. Every store counts its retries in `txn_conflicts`. Not in `all`; judged in the gate's `contended` step (D274) |
| `sparse-wide-deletes` | as `sparse-wide` | `sparse-wide`'s mix with 5% of the operations deleting, taken from its puts (15% puts): 2.5% delete one column (`column delete`), 2.5% delete a whole row, every family (`row delete`). RocksDB deletes a row with a range delete. Not in `all`; reported in the gate's `contended` step (D247) |
| `adjacency-prefix` | as `adjacency` | `adjacency`'s mix with its one-vertex scans filtered (`filtered scan`): only the edges whose qualifier starts with a prefix fixing every destination digit but the lowest `k`, 10^k about a tenth of the vertices; a vertex with none of them is skipped, as a sparse scan does. Not in `all`; reported in `contended` (D247) |
| `time-series-range` | as `time-series-ttl` | `time-series-ttl`'s mix with its newest-ten scans reading only points 12 to 6 hours old (`filtered scan`), so the scan skips about 25 newer points to reach them. Not in `all`; reported in `contended` (D247) |

**`group-commit`** measures the Goals table's durable-write targets: "> 1M cells/s across cores" and "p99 commit < 200 µs with `fsync` batching" (#53). Each operation is one commit, so its latency is commit latency and cells per second is 4 × the operations per second. The targets are judged on the D213 run ([D213](design/decisions/phase-3.md#d213)), reported as **Durable group commit (D213)** with cells/s in total and per core, commit p50 and p99, and the busiest shard's share. That run uses application-owned shards, one per core. Before the load, the table is split at its rows' quantiles with one piece per shard, so every shard commits from the start. The balancer is off for that run, so the placement stays fixed. A split, merge or move while measuring makes `phdb-bench` exit non-zero. The line also gives the engine's part of the commit latency (submission to resolution on the shard), WAL rollover blocks and inline WAL syncs. Each shard's thread keeps 16 `commit_async` commits in flight on its own rows, as the scaling gate does (D204). The client-thread runs are reported beside it, against RocksDB. With synchronous clients, throughput is at most clients ÷ commit latency, and before D213 every commit landed on one shard. The durability is the workload's, not `--sync`'s: Pigeonhole commits with `Durability::GroupSync`, where a commit returns once it is fsynced and concurrent commits on a shard share one fsync; RocksDB with `sync=true`, where concurrent writers share a write group and its WAL sync (RocksDB's group commit); SQLite with `synchronous=FULL`. The row set is small and overwritten, so the store stays a few MiB and the run measures the commit path rather than flush and compaction. A group-sync commit that arrives while its shard's fsync is in flight waits for the next one, so with several clients p50 sits near two fsync times.

**`cross-shard-contention`** is the contended cross-shard commit ([D246](design/decisions/phase-3.md#d246)): 32 clients over 16 partitions, half the commits across 2–4 of them. On Pigeonhole a multi-row commit is a two-phase commit when its rows sit on different shards, and a participant can refuse a round (it moved, or would stamp below a floor) so the commit retries. The report adds a table for every run with cross-shard commits on a store that counts them: commits, cross-shard commits, the engine's **refused rounds** (ICR 0043, `Metrics::cross_shard`) and their reasons (below a floor, a move's raised floor, a tablet change, no arena room, and since D249 an overlap with another commit's undecided share, retried inside the engine instead of returned as a conflict), the wasted rounds per cross-shard commit, and, as a cross-check, the sequence numbers used while measuring and the wasted rounds they imply (`RunRecord::rounds`). Each commit uses one sequence number and each refused round one more, so the two agree unless a split, merge or move used some too. `all` runs Pigeonhole twice: with the default options, and with tablet changes off, where the partition tables stay one per shard.

**fsync is not the same call on every OS and engine.** On macOS, Pigeonhole's sync is `F_FULLFSYNC` (a drive cache flush, milliseconds on an internal SSD), while SQLite's `synchronous=FULL` issues a plain `fsync`, which macOS does not flush to stable media unless `PRAGMA fullfsync` is on. SQLite's durable commits on macOS are therefore not comparable. The gate is judged on Linux reference hardware (`fdatasync` everywhere) against RocksDB.

Interpretation notes (details and rationale in [`design/questions/bench.md`](design/questions/bench.md)):
- A YCSB read fetches all ten fields of the record (`readallfields=true`, `BenchOp::GetRow`); an update still writes one field, as in YCSB. Results measured before [#54](https://github.com/CodingAnarchy/pigeonhole/issues/54) read one field and are not comparable with newer ones.
- Read-modify-write is a get and then a put in every engine, not an atomic operation. For that reason `ycsb-f` refuses `--threads` > 1: concurrent clients would race and lose updates. Every other workload accepts several threads. With more than one thread, operations are dealt round-robin, so a `ycsb-d` read can reach a key whose insert is still queued on another thread. That read is a miss, in every engine alike.
- Time-series cells expire. `BenchOp::PutAt` writes at an event time; the load places 100 points per entity `TTL/75` apart, ending just before "now" (`WorkloadConfig::epoch_micros`, default the wall clock when the workload is created), so the oldest 25 are older than the 1-day TTL and no read returns them. The 75 live points per entity are at least 6 hours from expiring and the expired ones are at least 14 minutes past it, so engines agree on what is live; a time-series run that lasts more than 6 hours after the workload is created fails instead of reporting numbers. Appends during the run are stamped at "now" and stay live.
- Every engine filters expired cells on read: Pigeonhole in its read path, the others by comparing the cell's timestamp (an 8-byte value prefix in RocksDB, a `ts` column in SQLite) with the clock. Pigeonhole's `metric` family compacts FIFO by time (whole SSTs drop once their newest point expires), and RocksDB's `metric` column family uses FIFO compaction with the same TTL as its counterpart (D163, #236). RocksDB judges a file's age by when it was written, not by the cells' event times, so it drops loaded back-dated points later than Pigeonhole; compare store size with that in mind. SQLite only filters on read. Agreement tests check that every engine reads the same cells, expired ones included.
- Nothing reads or keeps more than the latest version of a cell (`max_versions(1)`). Phase 2's version support is not exercised yet.

### How each store is driven

| Store | Model | Memory (default budget) | Filter | Commit (default / `--sync`) |
|---|---|---|---|---|
| `pigeonhole` | Table `bench`, families `ycsb`, `attr`, `metric` (TTL, `Compaction::FifoByTime`), `edge`, each `max_versions(1)`; public API only | memtable 64 MiB per shard, block cache 256 MiB | bloom, 10 bits/key | `Buffered` / `Sync` |
| `rocksdb` | Hand-written wide-column key: `escape(row) 00 01 <family> <qualifier>` (order-preserving), one key per cell, value = 8-byte timestamp + bytes; `metric` cells in their own column family with FIFO compaction and the 1-day TTL, the rest in the default one; a scan merges both once a `metric` cell exists; no compression codecs compiled in | `write_buffer_size` 64 MiB, LRU block cache 256 MiB | bloom, 10 bits/key | WAL, no fsync / `sync=true` |
| `sqlite-eav` | `cells(row, family, qualifier, ts, value)` `WITHOUT ROWID`, primary key `(row, family, qualifier)`, WAL journal | page cache 320 MiB (write buffer + cache) | none (B-tree) | `synchronous=NORMAL` / `FULL` |

**Memory budget.** Every engine gets the same `MemoryBudget`: a write buffer (`--write-buffer`, 64 MiB by default: Pigeonhole's and RocksDB's default) and a read cache (`--cache`, 256 MiB, Pigeonhole's default block cache). SQLite has no separate write buffer, so its page cache gets the sum. A one-shard table uses one shard's memtable, so the comparison holds whatever `--shards` is. Every engine's other options are its defaults: no tuning on any side. Each result's `Settings` column prints the budget, filter and durability it ran with.

**Cache model (#759).** Two stores' read memory is not one shared cache, so `--cache-model` says how `--cache` maps onto them:

| Store | `per-store` (default) | `total` |
|---|---|---|
| Pigeonhole | block cache = `--cache`; the library's row cache (64 MiB by default) on top | block cache + row cache = `--cache` (the row cache from `--row-cache`, else 64 MiB; the block cache gets the rest) |
| RocksDB | block cache = `--cache` | the same |
| SQLite | **every** connection's page cache = write buffer + cache. The bench opens one connection per client thread, so 16 clients hold 17 private caches: up to 17 × 320 MiB ≈ 5.3 GiB by default | write buffer + cache split evenly across the connections (the clients and the runner's own) |

`per-store` keeps results comparable with every run before #759. `total` holds every store to the same read-cache memory (D274's like for like). SQLite's connections can't share pages: each caches its own copy, which is SQLite's design in a multi-threaded process. Its shared-cache mode would share them, but it changes locking to table-level locks (`SQLITE_LOCKED`) and is discouraged, so the bench doesn't use it. Settings print the model: `cache=1024MiB total (block 960MiB + row 64MiB)` for Pigeonhole, `page_cache=1088MiB total` for SQLite.

**Durability.** The default level, written to the OS but not fsynced, survives a process crash in every engine, so the engines compare like with like. Tests check that every comparison runner reads exactly the same cells and bytes as Pigeonhole, operation by operation, for every workload (`runners::agreement`).

Latency is the wall time of one `execute` call, recorded in an HDR-style log-linear histogram (< 0.8% relative error). Throughput is measured operations divided by the wall time of the measured phase, across all client threads.

### Latency caveats

- **Closed loop, no coordinated-omission correction.** Each client issues its next operation only when the previous one returns. A stall delays every operation behind it, but only the stalled one is recorded as slow. Tail percentiles therefore understate what an open-loop client with a fixed arrival rate would see. Read p99.9 as a lower bound.
- **Timer overhead.** Each operation is bracketed by two `Instant::now()` calls, roughly 20–40 ns on current hardware, and that time is included in its latency. It matters only for sub-microsecond operations (point gets on hot data).
- **Sample counts.** At 200,000 measured operations, p99 rests on 2,000 samples and p99.9 on 200, of which the slowest 200 decide the value. With one thread per workload, a handful of OS scheduling events moves p99.9 a lot; see the calibration below. Raise `--ops` before reading anything into p99.9.
- **Warmup.** 5% of `--ops` run unrecorded first (`--warmup`), on the measured client threads, repeated until the tablet placement settles (D242). Without that, skewed-multi-shard's single-threaded load left D217's packing (13 tablets on shard 0) to unwind at one tablet per second inside the 4-thread measured phase. After a durability switch (`--durability group-sync` or `default`, where every store loads buffered), the quiet window is 5 s and the cap 60 s, longer than packing's move cadence while it re-places the tablets for the new level (D247). Warmup does not remove effects that grow with time, such as memtable size growing during the run.
- **Settling after the load (D233).** Before the warmup every engine flushes and waits for its background work to stop (see above), so the measured phase starts from the same state each time. Without it, how much of the load's flushes and compactions fell into the measured phase was timing luck: on the 2026-10-12 gate, ycsb-b's two identical runs measured 224.5K and 280.6K ops/s, the slower with 7 flushes and 7 compactions in its measured phase and the faster with none. The JSON records the step (`detail.settle`: milliseconds, settled, work done) and the measured phase's own background work (`detail.background_work`, flushes plus compactions, Pigeonhole only). `compare` notes every pair whose measured phases did different background work (one some and the other none, or more than twice as much): reported, not judged. The inline drivers (`scaling`, the D213 group commit) have their own warmup and don't settle.

## Reading results

Each result row gives the workload, store, store settings, size, client threads, throughput, and p50/p99/p99.9 latency in microseconds. The JSON (`Suite`) adds the seed, value size, load time, mean and max latency, and an environment fingerprint: CPU, cores, memory, OS, architecture, filesystem of the benchmark directory, build profile and git revision.

**Reference hardware.** Gates are defined on enterprise NVMe with power-loss protection, Linux 6.x and io_uring (spec, Goals). The reference machine is a Hetzner AX102, rented only for the Phase 3 gate window ([`crates/bench/baselines/phase3-gate`](../crates/bench/baselines/phase3-gate/README.md), #405); until its runs, every result is labeled `non-reference (D5)`. Two official Phase 2 gate runs on it (main f8afbb2d, 2026-10-14) are that gate's baseline, replacing the Mac runs 5–6 (D208). Non-reference numbers are reported in every run and never gate a phase. A run is labeled reference only when an operator sets `PHDB_BENCH_REFERENCE=1` on Linux; the bench never infers it.

## Gates

| Gate | Phase | How to measure | Pass when |
|---|---|---|---|
| RocksDB gap | 1 (reported) | `all --engine pigeonhole,rocksdb` | Reported, not gated |
| Model value | 2 | `sparse-wide --engine pigeonhole,sqlite,rocksdb` | Pigeonhole beats SQLite EAV and RocksDB on throughput and p99 |
| Latency | 3 | `ycsb-c` (point get), `ycsb-a`/`skewed-multi-shard` (writes), `group-commit --engine pigeonhole,rocksdb` (durable commits: the D213 shape, then 1, 4 and 16 threads), `adjacency`/`ycsb-e` (scans) | Goals table: get p50 < 2 µs, p99 < 10 µs; `group-commit` (D213) > 1M cells/s and p99 < 200 µs; within 1.5× of RocksDB |
| Scaling | 1 onward | `scaling --shards N` (N up to the core count), then `compare` against a stored `scaling.json` | Application-owned, each shard thread writing inline (D204): N-shard write throughput ≥ 0.8 × N × single-shard (`efficiency` ≥ 0.8) for N up to one chiplet's physical cores, larger N reported (D238), and single-shard p99 does not regress |
| Scan rate | 3 | `cargo run --release -p pigeonhole-bench --example scanrate -- rate` (one thread; `scan-rate.sh` on the reference machine) | D205: the `narrow` shape (100-byte values, 8-cell rows), compacted and cached, scans > 1 GB/s decoded (row key + qualifier + value) on one core; `small`, `wide`, `large` and the memtable runs are reported |
| Reproducibility | all | Two runs, then `compare a.json b.json` | Every result within tolerance |

**What the scaling gate runs (D204, D231).** The gate measures the spec's thread-per-core embedding, and the engine's throughput only: the balancer's placement is judged by its own check ([placement](#placement), [D231](design/decisions/phase-3.md#d231)).
- Pigeonhole opens **application-owned**, with one thread per shard.
- Each thread drives its own shard and commits the writes to rows its shard owns (`Table::shard_of`, ICR 0022), so its writes run on its own shard with no handoff. A row the balancer moves mid-phase is still committed correctly, routed to its new owner.
- Each thread keeps 16 commits in flight (`commit_async`; `INLINE_IN_FLIGHT`) and gives its shard a turn between polls.
- No client thread competes with the shards for cores, so the gate runs at N up to the core count.
- The load goes through the blocking API first. Then the warm-up (as long as the measured part, unrecorded) and the measured writes run inline.
- A commit's latency is from submission to resolution, so it includes the time it waited behind the other commits in flight.
- **The table is pre-split, with the balancer off** (D231, as D213 does for group commit). Before loading, the table is cut into one piece per shard by the measured writes, each row weighing its writes, so the heaviest piece is as light as it can be. A row hotter than a share (the skewed workload's hottest takes 6.5%) sits alone, cut just before and just after it. No balancer pass falls due, so the placement holds, and the run fails if the split did not leave one tablet per shard.
- **Each shard's thread is pinned** to a CPU of its own, one per physical core first (D220, D235; `pigeonhole_io::sys::pin_plan`), as a thread-per-core application runs. That run is judged. The same pair runs again with the threads unpinned, reported beside it (`scaling_unpinned` in the JSON) and not judged; group commit's D213 run does the same (`group_commit_unpinned`). Where pinning is refused (macOS, a CPU quota, too few CPUs) the judged run goes on unpinned with a warning, and its settings say `unpinned (<why>)`. On the AX102, 8 shards pinned on one CCD ran at 3.24M commits/s, against 2.36–2.42M unpinned across two CCDs.
- The verdict is efficiency alone. The busiest shard's share is printed with it, and each shard's share in every result.

**Judged per chiplet (D238).** The gate window judges ≥ 0.8 × N for N up to the physical cores of one chiplet (CCD), the cores that share one L3 cache. The gate's setup step reads this from `/sys/devices/system/cpu/cpu*/cache/index3/shared_cpu_list`. Larger N cross chiplets and are reported, not judged: on the AX102 (2 CCDs × 8 cores), N = 16 measures about 0.70, which is accepted for two-chiplet parts. `phdb-bench scaling` itself still prints pass or fail against 0.8 at any N; the window's `report.py` applies the per-chiplet bound. The summary also reports efficiency against a fixed single-shard rate recorded on the AX102 (417,922 commits/s, main 9ff02d00), so a faster single shard doesn't hide progress at N.

**Reported beside it, not gating:** the engine-owned shape, with synchronous client threads (four per shard) at half the gate's shards, so the clients have cores of their own. `scaling` prints both, and the JSON carries the second as `scaling_sync`.

Before D204 the gate ran N synchronous clients on N engine-owned shards. That's 2N busy threads, which can't reach 0.8 × N at N = cores (#154). A `scaling.json` from before D204 is not a baseline for the p99 half: the first run after it starts a new one.

**Since D231, `phdb-bench scaling` means something else, run by hand too.** It always pre-splits the table at the writes' quantiles and runs with the balancer off, so it measures the engine's per-shard throughput, not the balancer's placement. Its efficiency is not comparable with a `scaling` result from before D231, where the balancer spread the table during the warm-up and the placement it left capped the result; its busiest share no longer judges placement either. To see how the balancer places a table, run `phdb-bench placement`.

**The scaling gate has two halves, checked differently.** Each `scaling` run evaluates only the efficiency half and prints pass or fail. The other half, "no regression in single-shard p99", needs a baseline: compare this run's `scaling.json` against a stored one with `phdb-bench compare old/scaling.json new/scaling.json`, which checks the single-shard p99 within the p99 tolerance. The weekly `bench.yml` uploads `scaling.json` with every run, so each run leaves the baseline for the next.

**Scaling needs tablet changes** (on by default) for its pre-split. With `--no-tablet-changes` a table stays one tablet on one shard, so the writes all land on one shard whatever N is, and `scaling` fails by construction. `scaling` warms up as long as it measures (`--warmup 1.0`) unless `--warmup` says otherwise, so flushes and compactions reach their steady state first.

<a id="placement"></a>**Balancer placement (`placement`, D231).** The balancer's placement is judged on its own, under a load where a shard's write rate is its share of the work:
- Pigeonhole opens engine-owned at N shards, with tablet changes on and packing off (packing keeps a light load on few shards on purpose). The table starts as one tablet, and the balancer spreads it.
- Client threads (`--threads`, default N) offer the skewed-multi-shard writes **open-loop**, at a fixed total rate (`--offered-rate`, default 30,000 writes/s per shard, above the balancer's per-pass minimum for an average shard). A client behind its schedule catches up instead of slowing the offer. So a shard's write rate is its share of the writes, which the scaling gate's closed-loop driver does not give: there, a shard that finished its work idles, and the busy ones look skewed beside it ([D224](design/decisions/phase-3.md#d224)).
- It waits until every shard owns a tablet and nothing has split, merged or moved for 12 s (longer than the balancer's longest dwell), at most 60 s, then measures a window (`--min-secs`, default 10 s).
- **Pass:** settled within the bound, no split, merge or move in the window, the busiest shard at most 1.25/N of the window's writes (at equal per-shard rates, efficiency can't exceed 1/(N × busiest share), so 1.25/N is the loosest bound that still allows 0.8; [D223](design/decisions/phase-3.md#d223)), and the store completing at least 95% of the offered rate (otherwise the load was not open-loop). The JSON carries it as `placement`, with every shard's share.

**Where the writes went.** For Pigeonhole, each result also reports every shard's share of the measured phase: commits applied, tablets owned at its start and end, and splits, merges and moves completed. JSON has it in `detail.shards`; the markdown prints a `Shard | Commits | Share` table. A scaling run whose N-shard result shows one shard with all the commits measured one tablet, not N shards.

**Scan rate (#29).** `examples/scanrate.rs rate` builds a table per shape, in the memtable or compacted with every block cached, and times whole-table scans of one family on one thread. Decoded bytes are the row key, qualifier and value of every returned cell. Shapes: `narrow` (8 cells of 100 B per row), `wide` (1,000 cells of 100 B), `small` (8 cells of 16 B) and `large` (4 cells of 1 KiB). Rows of small cells are the hardest case, because the cost is mostly per cell rather than per byte. The Goals target is judged on `narrow`, compacted (D205); the other shapes are reported. On the reference machine, `crates/bench/baselines/phase3-io/scan-rate.sh` runs it pinned to one core, three times, and prints the medians. The same binary is a callgrind shape binary too: `scan-narrow`, `scan-small` and `scan-wide` count instructions per returned cell on a compacted table, one source and one version per column, on every PR.

**Visibility waits.** Each Pigeonhole result's stall counters (`detail.stalls`) include the async commits that waited on the global visibility watermark (D19), and how often that waiter list's lock was already held (ICR 0023). The scaling line prints the N-shard run's waits per operation. Near 0 means commits aren't waiting on the watermark; a rising value with N is what would justify reworking D19 (#154).

**On the reference machine (#405).** `crates/bench/baselines/phase3-io/scaling.sh DATA_DIR [OUT_DIR] [RUNS] [SHARDS...]` runs `scaling --scale full` at N = 1, 2, 4, 8 and 16, three runs each. The measured writes grow with N (2,000,000 × N), so each phase lasts seconds. It records the machine and writes one suite JSON per run, then a `summary.md` with each N's median efficiency, busiest share, visibility waits per operation and the sync shape's efficiency. It's a step of the gate-window run, next to `cold-scan.sh` and `open-latency.sh`.

### Reproducibility tolerance

Two runs of the same suite on the same machine agree when, for every result:

| Metric | Tolerance |
|---|---|
| Throughput | ±20% |
| p50 | ±20% or ±3 µs |
| p99 | ±40% or ±6 µs |
| p99.9, max | reported, not checked |

A latency fails only when it is past both its relative tolerance and its absolute floor (D259): at a few microseconds a ±20% move can be under 3 µs, which doesn't mean a different store. The floors scale with `--tolerance` (3 and 6 µs at 0.20).

Only results that measured at least 10 s in both runs are judged, or at least 2M commits for group commit (D243). Shorter ones are listed with no tolerance and named under "Not judged (D243)": at a few seconds, one flush or compaction more or less moves a run past the tolerance. The 2026-10-13 gate's second pair failed only on such runs (the 0.16 s D213 shape, a 5.3 s ycsb-a). The gate window runs `all` with `--min-secs 10` so every result is judged.

How the tolerance was chosen: five runs of `all --engine all` at commit `4b59eac` (`small` preset, 5% warmup, 40 results each) on the machine in the results below, then every pair compared. Runs 2–5 started with a one-minute load of 1.0–1.8, with only the desktop running. Across their 6 pairs (240 comparisons per metric), the worst drift was 10.1% on throughput (95th percentile 5.9%, median 1.4%), 13.4% on p50 (95th percentile 7.5%), and 17.2% on p99 (95th percentile 10.1%). p99.9 drifted up to 202% (95th percentile 25%), which is why it is not checked. The tolerance sits at 1.5–2.3× the worst observed drift. All six pairs pass at the default tolerance.

Run 1 started while the 15-minute load was still 12.3, the tail of another agent's test suite. Its pairs drifted by up to 28% (throughput), 22% (p50) and 38% (p99), and `compare` fails all four of them. That is the intended outcome: a run that starts on a busy machine should not count as a reproduction. Earlier, runs taken while that suite was at 300% CPU disagreed by 20–90% on every engine at once. `compare` warns when a run started with a one-minute load of 2 or more. The load is recorded in every result's environment. On a macOS desktop the one-minute load rarely drops below 1 even when idle, so 2 is the practical bar for "quiet".

`compare` is symmetric: it flags any change beyond tolerance, faster or slower. An unexplained improvement is as suspect as a regression, and an intended one means it is time for a new baseline. It warns when the two runs come from different machines or build profiles, because the tolerance only means something on one machine. Close other heavy work while measuring. Laptops also throttle and switch between performance and efficiency cores.

## Phase 3 baseline (this Mac, non-reference)

**Two runs, commit `105421a`** (main after 0.2.0), measured 2026-10-09 in a quiet window with the other agents paused. Machine: Apple M5, 10 cores, 24 GiB, macOS 26.5.2, APFS. Release build, `--engine pigeonhole,rocksdb --scale small` (50,000 records, 200,000 measured operations, 5% warmup). The 1-minute load at each start was under 1.5. This is the starting point for the Phase 3 work, not the gate: the gate is the #406 checklist on reference hardware (#405, D197).

Each cell gives run 1 / run 2, in µs. The ratios divide Pigeonhole's worse run by RocksDB's better one.

| Workload | Threads | Pigeonhole ops/s | Pigeonhole p50 | Pigeonhole p99 | RocksDB ops/s | RocksDB p50 | RocksDB p99 | p50 ratio | p99 ratio |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | 1 | 124.7K / 135.1K | 6.59 / 6.33 | 19.5 / 16.6 | 198.4K / 207.9K | 3.29 / 3.06 | 15.2 / 14.5 | 2.16 | 1.34 |
| ycsb-b | 1 | 241.8K / 254.8K | 2.72 / 2.64 | 13.4 / 13.4 | 200.6K / 204.0K | 3.34 / 3.26 | 13.4 / 13.3 | 0.83 | 1.01 |
| ycsb-c | 1 | 514.1K / 533.7K | 1.84 / 1.75 | 2.96 / 2.75 | 354.6K / 365.1K | 2.59 / 2.54 | 5.38 / 5.09 | 0.72 | 0.58 |
| ycsb-d | 1 | 456.9K / 472.2K | 1.42 / 1.38 | 8.32 / 7.71 | 309.0K / 343.0K | 2.88 / 2.59 | 7.58 / 6.43 | 0.55 | 1.29 |
| ycsb-e | 1 | 30.5K / 30.7K | 32.3 / 32.1 | 65.0 / 64.5 | 18.1K / 18.9K | 54.0 / 52.2 | 124 / 117 | 0.62 | 0.55 |
| ycsb-f | 1 | 112.5K / 113.0K | 9.09 / 9.02 | 16.9 / 16.9 | 190.9K / 191.3K | 3.63 / 3.58 | 14.1 / 14.1 | 2.54 | 1.20 |
| sparse-wide | 1 | 63.5K / 66.9K | 5.63 / 5.47 | 181 / 177 | 47.6K / 47.5K | 3.42 / 3.42 | 285 / 291 | 1.65 | 0.64 |
| time-series-ttl | 1 | 208.8K / 210.2K | 4.05 / 4.05 | 9.73 / 9.60 | 488.3K / 531.2K | 2.13 / 2.01 | 4.67 / 3.68 | 2.02 | 2.64 |
| adjacency | 1 | 66.0K / 76.6K | 8.51 / 7.71 | 72.2 / 53.2 | 61.0K / 65.0K | 7.42 / 6.97 | 81.9 / 77.8 | 1.22 | 0.93 |
| skewed-multi-shard | 4 | 255.5K / 293.1K | 9.73 / 9.21 | 32.4 / 28.2 | 332.4K / 412.2K | 8.96 / 8.64 | 49.4 / 20.4 | 1.13 | 1.59 |

**Against the 1.5× rule:**
- The point reads are already ahead of RocksDB here:
  - `ycsb-c` reads all ten fields of a record (`readallfields`), so these numbers are not comparable with the single-field reads in the older tables below;
  - so are the read-mostly mixes (`ycsb-b`, `ycsb-d`) and the scans (`ycsb-e`, `adjacency`).
- The gaps are on writes:
  - updates and read-modify-writes (`ycsb-a`, `ycsb-f` p50);
  - timestamped appends (`time-series-ttl`);
  - the sparse-wide median.
- `skewed-multi-shard` p99 crossed 1.5× in run 2 only; RocksDB's p99 moved from 49.4 to 20.4 µs between the runs.

**Durable commits (`group-commit`, one run, load 1.24).**

| Store | Threads | ops/s | p50 µs | p99 µs | p99.9 µs |
|---|--:|--:|--:|--:|--:|
| pigeonhole | 1 | 253 | 3,932 | 6,029 | 16,384 |
| pigeonhole | 4 | 418 | 9,962 | 17,695 | 50,856 |
| pigeonhole | 16 | 937 | 15,794 | 37,224 | 44,564 |
| rocksdb | 1 | 44.3K | 20.4 | 38.1 | 48.6 |
| rocksdb | 4 | 59.3K | 72.2 | 124 | 270 |
| rocksdb | 16 | 121.1K | 122 | 230 | 397 |

- **The stores are not comparable here.** On macOS Pigeonhole syncs with `F_FULLFSYNC`, a drive-cache flush of about 4 ms on this SSD. RocksDB's sync does not flush the cache, as its 20 µs shows.
- **What the table does show is Pigeonhole's own scaling.** Commit latency grows with threads. This workload's 1,000-row table never splits, so every commit lands on one shard. Before D207 that shard started a sync for each small group whatever was in flight, so concurrent commits rarely shared a sync: about 2 per sync at 4 clients and 4.5 at 16 on a Linux runner (#412). D207 bounds the group syncs in flight per stream (1 on macOS, 2 elsewhere, until the gate run decides), and batches the groups that arrive meanwhile behind them.

**Sparse-wide gate runs on this Mac are too noisy to compare single runs.** Three `--scale full` runs of the same binary in the same window ranged:

| Measure | Low | High |
|---|--:|--:|
| ops/s | 22.0K | 31.6K |
| p99 µs | 393 | 532 |
| row-read p99 µs | 461 | 668 |

The noisiest run failed `crates/bench/baselines/phase2-gate/check.py` and the quietest passed. Judge a gate change on several interleaved runs per side, or on #405.

## Scaling gate results (this Mac, non-reference)

**Single runs, n=1, commit `87afe1f` plus this section's bench changes**, measured 2026-10-08: `phdb-bench scaling --shards N` at `--scale small` (50,000 records, 200,000 measured writes) and `--scale full` (1,000,000 records, 2,000,000 measured writes), release, `skewed-multi-shard`, N client threads, buffered commits, 64 MiB write buffer, tablet changes on, warmup equal to the measured ops (single-threaded, unrecorded).

**Environment:** Apple M5 (10 cores, 24 GiB), macOS 26.5.2 aarch64, APFS [non-reference (D5)]. macOS ignores thread pinning. The one-minute load at the start of each run is in the table; the last `full` run started above the quiet bar of 2.

| Scale | N | Load | 1 shard ops/s | N shards ops/s | Efficiency | 1-shard p99 µs | N-shard p99 µs |
|---|--:|--:|--:|--:|--:|--:|--:|
| small | 2 | 1.98 | 172.4K | 197.8K | 0.57 | 14.8 | 14.8 |
| small | 4 | 1.99 | 240.6K | 255.1K | 0.27 | 32.3 | 34.3 |
| small | 8 | 1.91 | 281.3K | 250.6K | 0.11 | 504 | 58.1 |
| full | 2 | 1.76 | 156.2K | 151.9K | 0.49 | 16.8 | 19.2 |
| full | 4 | 1.80 | 193.4K | 185.3K | 0.24 | 49.4 | 155 |
| full | 8 | 3.33 | 237.8K | 191.3K | 0.10 | 524 | 185 |

The gate (efficiency ≥ 0.8) fails at every N. **The writes do spread.** Commits per shard in the N-shard runs, as a share of the measured phase, with tablets owned at its start:

| Scale | N | Commits per shard (%) | Tablets per shard at start | Splits / moves during the phase |
|---|--:|---|---|---|
| small | 2 | 49.8, 50.2 | 1, 1 | 0 / 0 |
| small | 4 | 17.4, 28.6, 27.7, 26.2 | 1, 1, 1, 1 | 0 / 0 |
| small | 8 | 14.4, 13.1, 14.3, 11.1, 16.2, 8.1, 11.1, 11.8 | 1 on each | 3 / 1 |
| full | 2 | 46.0, 54.0 | 2, 2 | 1 / 7 |
| full | 4 | 23.9, 26.9, 24.8, 24.4 | 4, 2, 3, 2 | 0 / 8 |
| full | 8 | 13.2, 12.8, 13.6, 12.5, 11.9, 12.7, 11.3, 11.9 | 6, 4, 3, 3, 5, 4, 2, 2 | 3 / 10 |

With the old 5% warmup, `small` at 8 shards measured one tablet: all 200,000 commits on shard 1, no split completed in the run's ~1 s of writes (0.37 s load, 10,000 warmup ops, 0.62 s measured). A warmup of 50,000 ops put the first split inside the measured phase (53% of commits on one shard); 100,000 to 200,000 spread the table before it. At `full` the load phase alone is long enough.

**Why spreading does not raise throughput.** A `sample` profile (macOS, 10 s each, `skewed-multi-shard --scale full --threads 8 --ops 8000000`) of one shard and of eight:

- **One shard is CPU-bound.** Its thread was parked in about 1% of samples. About 40% of its time went to `resolve_group` waking the 8 client threads (`semaphore_signal_trap` through `ThreadWake`), about 10% to the WAL `pwrite`, and the rest to memtable inserts and the group path. Throughput 269K ops/s.
- **Eight shards are mostly idle.** Each shard thread was parked in the runtime (`IdlePark::park`, `thread::park`) in 64–75% of samples, and spent 7–13% in `pwrite` and 5–8% waking clients. Commits were spread 11–14% per shard. Throughput 194K ops/s.
- **The clients are latency-bound.** Each client thread was blocked in about 91% of samples: about 78% in `PendingCommit::wait` waiting for its shard's reply (`write.rs:254`), and about 14% waiting for the global visibility watermark (`wait_visible`, `write.rs:255`, D19), which waits for every shard's in-flight group below its seqno.
- **Little lock contention.** `__psynch_mutexwait` was under 1.5% of a shard thread's samples. It comes from the global visibility-waiter list, which every shard locks in `publish_watermark` → `wake_visible` after each group and every client locks in `wait_visible`.

With N closed-loop clients, throughput is about N divided by commit latency. At one shard, group commit puts the 8 clients' writes in one batch and one `pwrite`. At eight shards each shard has one client: every commit pays its own `pwrite`, two cross-thread wakes (submit, reply), and the cross-shard visibility wait, while the shard threads idle. With 32 clients at `small` (load 2.66), 8 shards reached 409.6K ops/s against 354.1K on one shard (efficiency 0.14). Changing the workload so it can saturate one shard, and cutting the per-commit handoff, is tracked in [#154](https://github.com/CodingAnarchy/pigeonhole/issues/154) and [#134](https://github.com/CodingAnarchy/pigeonhole/issues/134) (Phase 3).

## Results with disk-backed storage (this Mac, non-reference)

**Single run, n=1, commit `fa606ba`** (the build was made at `4bc304c`, the same code before this branch was rebased onto `main`), measured 2026-10-06: `phdb-bench all` (Pigeonhole only, release, `small` preset: 50,000 records, 200,000 measured operations after a 5% warmup, 100-byte values, seed `0x5EED`, 64 MiB write buffer, buffered commits). Memtables now flush to SSTs and compact during the run.

**Environment:** Apple M5 (10 cores, 24 GiB), macOS 26.5.2 aarch64, APFS, **load 6.14** [non-reference (D5)]. The machine was shared with other agents' test suites, so this is *not* a quiet-machine run: the reproducibility rule above would flag it, and run-to-run drift is likely well beyond ±10%. Read these as order-of-magnitude, not as a regression or improvement against the older table (which ran at load 0.98 on a different engine build).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(10) memtable=64MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 116.8K | 6.43 | 20.7 | 50.4 |
| ycsb-b | pigeonhole | shards=default(10) memtable=64MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 293.1K | 2.46 | 14.6 | 26.4 |
| ycsb-c | pigeonhole | shards=default(10) memtable=64MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 406.3K | 2.13 | 6.05 | 10.3 |
| ycsb-d | pigeonhole | shards=default(10) memtable=64MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 436.5K | 1.09 | 17.5 | 29.8 |
| ycsb-e | pigeonhole | shards=default(10) memtable=64MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 17.7K | 55.3 | 115 | 197 |
| ycsb-f | pigeonhole | shards=default(10) memtable=64MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 124.0K | 6.27 | 23.9 | 36.9 |
| sparse-wide | pigeonhole | shards=default(10) memtable=64MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 62.0K | 2.75 | 261 | 465 |
| time-series-ttl | pigeonhole | shards=default(10) memtable=64MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 172.5K | 3.76 | 21.5 | 30.6 |
| adjacency | pigeonhole | shards=default(10) memtable=64MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 54.5K | 10.8 | 78.8 | 103 |
| skewed-multi-shard | pigeonhole | shards=default(10) memtable=64MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 4 | 234.8K | 14.5 | 34.0 | 532 |

**Not measured:** the `full` preset (1M records) and the comparison engines at it. The machine was too loaded for the numbers to mean anything, so full-scale and multi-engine comparisons are left to the weekly `bench.yml` workflow (`--scale full` is selectable there) and to reference hardware (D5). The only full-scale evidence so far is one run, n=1, of `sparse-wide --scale full` on Pigeonhole alone: 1,000,000 records and 1,000,000 operations completed without `Busy` at 22.4K ops/s (p50 6.17 µs, p99 786 µs), at machine load 8.9. It was built from `a007910` plus this branch's then-uncommitted `full` preset, so it names no branch commit; treat it as evidence that the size runs, not as a measurement.

## Cold-get results (reference machine, AX102)

**Reference hardware (D5, D197):** Hetzner AX102, AMD Ryzen 9 7950X3D (16 cores, 32 threads), 128 GiB, Linux 6.8, XFS on a Samsung PM9A3 NVMe (power-loss protected). `baselines/phase3-io/cold-get.sh`: a 64 GiB table of 1 KiB cells, 1,000,000 cold gets per phase after 100,000 warm-up gets, 3 runs per configuration. **Direct** is `PIGEONHOLE_DIRECT=1`; **buffered** runs in a cgroup capped at 8 GiB with the page cache dropped first. Device reads per get come from `/sys/block/nvme0n1/stat`. io_uring and pread measured the same to within 1% in every row, so one row stands for both.

**Present rows, 2026-10-12** (`main` 8631bab, after D215: block cache 1 GiB = 1/64 of the table, issue #403's rerun):

| Mode | Threads | Device reads/get | Block-cache misses/get | p50 µs | p99 µs | p99.9 µs | Gets/s |
|---|--:|--:|--:|--:|--:|--:|--:|
| direct | 1 | 1.104 | 1.104 | 87.6 | 205 | 301 | 10.1K |
| buffered | 1 | 1.092 | 1.104 | 91.6–92.2 | 264–266 | 375–377 | 10.3K |
| direct | 16 | 1.105 | 1.104 | 105.5–106 | 293–295 | 416–434 | 129.5–130.0K |
| buffered | 16 | 1.075 | 1.104 | 113.2–113.7 | 447–451 | 946–975 | 122.2–122.7K |

**The spec's target is met:** a cold get costs one device read (1.09–1.10 per get; the rest is bloom-filter false positives across levels), at a p50 of 88–92 µs on one thread. That is above the spec's 20–80 µs device estimate by the PM9A3's own random-read latency plus the engine's miss path. With the block cache sized this way, direct I/O has the lower tail (p99 205 against 264 µs on one thread, 293 against 447 on 16) and about 6% more throughput at 16 threads. Buffered reads still win whenever the page cache can hold what the block cache cannot (see `Options::direct_io` and the guide), which is why direct I/O stays off by default (#403).

**Absent rows** (a filter rules them out; 0.02 device reads per get): p50 1.0 µs, p99 88 µs direct and 92 µs buffered on one thread; p99 99 µs direct and 106 µs buffered on 16.

**The first gate run, 2026-10-11** (f333d48, before D215: a 64 MiB block cache) missed the target with **2.09 block-cache misses per get**. The pinned indexes and filters (about 185 MiB) overflowed the cache, so every index partition was read again:

| Mode | Threads | Device reads/get | p50 µs | p99 µs | p99.9 µs | Gets/s |
|---|--:|--:|--:|--:|--:|--:|
| direct | 1 | 2.092 | 172 | 326 | 447 | 5.4K |
| buffered | 1 | 1.087 | 94.2–94.7 | 268–272 | 414–420 | 10.2K |
| direct | 16 | 2.092 | 216–217 | 432–434 | 582–594 | 70.5K |
| buffered | 16 | 1.055 | 115.7–116.2 | 373–375 | 971–1024 | 123.1–123.4K |

Buffered reads hid the second miss there: the index partitions sat in the page cache. D215 sized the gate's block cache to hold them (1/64 of the table), and the engine now reports the pinned metadata and warns when it passes half the block cache.

## IVF index on SIFT1M (reference machine, AX102)
`examples/ivfsift.rs` (D306 PR 3) builds a `pigeonhole-vector` index from the standard SIFT1M files and measures recall@10 and per-query latency, one thread, one query at a time:

```text
IVF_BLOCK_CACHE_MB=1024 IVF_NPROBES=16,20,24,28,32 \
  cargo run --release -p pigeonhole-bench --example ivfsift -- SIFT_DIR DB_DIR bulk all 1000 buffered
```

- **Modes:** `bulk` (`bulk_load_f32`) or `insert` (one `insert_f32` per vector, splits inline).
- **Other arguments:** `N` (`all`, or a prefix of the base, scored by brute force), the target list length, and the durability.
- **Knobs:** `IVF_BLOCK_CACHE_MB`, `IVF_NPROBES`, and `IVF_COMPACT=1` (compact after the build).
- **Output:** JSON lines. Warm rows (sync and `run_async`) come after one untimed pass, then a cold row with direct I/O at the first nprobe reaching 95% recall.

**Comparison:** `crates/bench/baselines/ivf/faiss-ivf.py SIFT_DIR` runs Faiss IVF-Flat (1,000 lists, trained on the first 50,000 vectors) the same way, one thread and one query at a time (`pip install faiss-cpu numpy`).

**Results (2026-11-02, bulk build, target 1,000, 1 GiB block cache):**

| nprobe | recall@10 | p50 | p99 |
|--:|--:|--:|--:|
| 16 | 0.922 | 2.30 ms | 3.94 ms |
| 20 | 0.943 | 2.84 ms | 4.67 ms |
| 24 (the default) | 0.958 | 3.38 ms | 5.39 ms |
| 28 | 0.967 | 3.92 ms | 6.08 ms |
| 32 | 0.974 | 4.47 ms | 6.75 ms |

- **D306's target, 95% recall@10 at ≤ 10 ms,** is met at nprobe 24, with p99 at 5.4 ms.
- **Under the default 256 MiB block cache** (half the 512 MB index cached), nprobe 32 took 10.8 ms. The guide's sizing section says to hold the index.
- **Faiss IVF-Flat:** the same recall within ±0.015 at every nprobe from 4 to 128, 9–10× faster (0.50 ms at nprobe 32). The gap is the engine's per-cell read path (#1283).
- **Builds:** `bulk_load` 138 s. 1M inserts at `Buffered` 67 s (insert p50 56 µs, p99 93 µs, p999 415 µs). Faiss 3.3 s.

## Larger-than-RAM results (this Mac, non-reference)

**Single runs, n=1, commit `ba5e996`**, measured 2026-10-06: `phdb-bench ycsb-c` and `phdb-bench ycsb-a` with `--scale larger-than-ram` (Pigeonhole only, release, 1,000,000 records and 1,000,000 operations, 5% warmup, 100-byte values, seed `0x5EED`, 8 MiB memtable, 16 MiB block cache, buffered commits).

**Environment:** Apple M5 (10 cores, 24 GiB), macOS 26.5.2 aarch64, APFS, **load 6.22 (ycsb-c) and 7.98 (ycsb-a)** at the start of each run [non-reference (D5)]. The machine was shared with other agents' test suites and never dropped below a one-minute load of 6 while I waited, so neither run is a quiet-machine run. Read them as order-of-magnitude. The store was 1,792 MiB (ycsb-c) and 2,048 MiB (ycsb-a) on disk, 75–85× the 24 MiB budget, and fits in the OS page cache, so cold gets did not reach the device (see above).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-c | pigeonhole | shards=default(10) memtable=8MiB cache=16MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 130.9K | 7.10 | 26.5 | 167 |
| ycsb-a | pigeonhole | shards=default(10) memtable=8MiB cache=16MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 13.5K | 14.8 | 1335 | 2900 |

| Workload | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-c | 1792 MiB | 0 | 162686 | 7.68 | 158 | 196 | 837314 | 6.85 | 15.6 | 43.5 |
| ycsb-a | 2048 MiB | 0 | 110500 | 16.6 | 2621 | 3768 | 389773 | 19.6 | 1286 | 2802 |

Both runs completed with no `Busy` retries, so no write was ever refused. Loading took about 230 s each (1M rows × 10 fields under an 8 MiB write buffer). In `ycsb-c`, cold gets have a median close to hot ones (7.7 vs 6.9 µs) and a much worse p99 (158 vs 15.6 µs): the miss path costs a long tail, but a median read is dominated by the same OS-page-cache and engine overhead either way. In `ycsb-a` the tail belongs to the 50% updates running under flush and compaction pressure, which hits cold and hot gets alike (the p99 rows are close to each other). These do not measure the spec's one-I/O-per-get target; that needs a store larger than RAM, on reference hardware.

## Comparison results before Milestone B (this Mac, non-reference)

**Single run, n=1, commit `4b59eac`**, measured 2026-10-06: `phdb-bench all --engine all` (release, `small` preset: 50,000 records, 200,000 measured operations after a 5% warmup, 100-byte values, seed `0x5EED`). This is run 2 of the five calibration runs above, chosen because it started on the quietest machine (load 0.98). Single-run numbers carry the run-to-run drift described above: about ±10% on throughput and p50.

**These numbers predate engine Milestone B** (Pigeonhole ran memory-only with a 256 MiB write buffer; the other engines used the same budget). They are kept as the only multi-engine comparison until it is re-run, and are not comparable with the table above. The run also measured fjall; its rows went with the fjall runner (owner, 2026-10-29). They are **not from reference hardware** (D5): a laptop with an APFS SSD, macOS, and no io_uring. Pigeonhole is memory-bound (#37), so every store's data is hot in memory. Every engine has the same memory budget (256 MiB write buffer, 256 MiB read cache, SQLite 512 MiB page cache) and its default options otherwise. No engine has been tuned. Treat this as a baseline to track, not a verdict.

**Environment:** Apple M5 (10 cores, 24 GiB), macOS 26.5.2 aarch64, APFS, load 0.98 [non-reference (D5)]

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(10) memtable=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 215.5K | 4.38 | 12.0 | 41.0 |
| ycsb-a | rocksdb | write_buffer=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 669.1K | 1.63 | 3.89 | 7.26 |
| ycsb-a | sqlite-eav | page_cache=512MiB buffered | 50000 | 200000 | 1 | 128.7K | 3.84 | 6.46 | 24.6 |
| ycsb-b | pigeonhole | shards=default(10) memtable=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 547.2K | 1.46 | 8.26 | 13.9 |
| ycsb-b | rocksdb | write_buffer=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 1.16M | 0.75 | 2.25 | 5.73 |
| ycsb-b | sqlite-eav | page_cache=512MiB buffered | 50000 | 200000 | 1 | 500.5K | 1.17 | 5.02 | 7.84 |
| ycsb-c | pigeonhole | shards=default(10) memtable=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 821.2K | 1.13 | 2.51 | 3.38 |
| ycsb-c | rocksdb | write_buffer=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 1.27M | 0.75 | 1.50 | 2.10 |
| ycsb-c | sqlite-eav | page_cache=512MiB buffered | 50000 | 200000 | 1 | 858.3K | 1.13 | 1.63 | 1.96 |
| ycsb-d | pigeonhole | shards=default(10) memtable=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 477.1K | 1.46 | 12.5 | 32.0 |
| ycsb-d | rocksdb | write_buffer=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 1.03M | 0.71 | 5.47 | 8.77 |
| ycsb-d | sqlite-eav | page_cache=512MiB buffered | 50000 | 200000 | 1 | 129.1K | 1.29 | 114 | 130 |
| ycsb-e | pigeonhole | shards=default(10) memtable=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 23.6K | 41.5 | 87.6 | 105 |
| ycsb-e | rocksdb | write_buffer=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 24.5K | 40.4 | 85.0 | 98.8 |
| ycsb-e | sqlite-eav | page_cache=512MiB buffered | 50000 | 200000 | 1 | 30.8K | 26.2 | 121 | 138 |
| ycsb-f | pigeonhole | shards=default(10) memtable=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 175.3K | 5.47 | 13.7 | 43.5 |
| ycsb-f | rocksdb | write_buffer=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 537.8K | 2.05 | 4.61 | 8.13 |
| ycsb-f | sqlite-eav | page_cache=512MiB buffered | 50000 | 200000 | 1 | 117.4K | 4.51 | 7.13 | 28.9 |
| sparse-wide | pigeonhole | shards=default(10) memtable=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 66.2K | 2.80 | 270 | 475 |
| sparse-wide | rocksdb | write_buffer=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 101.6K | 1.29 | 170 | 297 |
| sparse-wide | sqlite-eav | page_cache=512MiB buffered | 50000 | 200000 | 1 | 78.5K | 1.92 | 102 | 228 |
| time-series-ttl | pigeonhole | shards=default(10) memtable=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 224.9K | 3.63 | 11.2 | 39.2 |
| time-series-ttl | rocksdb | write_buffer=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 591.6K | 1.75 | 3.71 | 7.39 |
| time-series-ttl | sqlite-eav | page_cache=512MiB buffered | 50000 | 200000 | 1 | 119.9K | 2.22 | 23.3 | 62.0 |
| adjacency | pigeonhole | shards=default(10) memtable=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 54.8K | 9.47 | 84.0 | 103 |
| adjacency | rocksdb | write_buffer=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 1 | 64.0K | 7.10 | 77.8 | 96.8 |
| adjacency | sqlite-eav | page_cache=512MiB buffered | 50000 | 200000 | 1 | 126.7K | 4.89 | 26.5 | 33.8 |
| skewed-multi-shard | pigeonhole | shards=default(10) memtable=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 4 | 354.9K | 9.92 | 29.2 | 86.0 |
| skewed-multi-shard | rocksdb | write_buffer=256MiB cache=256MiB bloom=10 buffered | 50000 | 200000 | 4 | 412.8K | 8.64 | 21.6 | 51.7 |
| skewed-multi-shard | sqlite-eav | page_cache=512MiB buffered | 50000 | 200000 | 4 | 112.2K | 5.02 | 13.3 | 3129 |

**Scaling gate** (single run, commit `4b59eac`; `phdb-bench scaling`, 10 client threads, `shards(1)` against `shards(10)`): 407K and 365K writes/s, efficiency 0.09, so it **fails**. The whole table is one tablet on one shard ([#51](https://github.com/CodingAnarchy/pigeonhole/issues/51)), so this measures no scaling at all, as expected today. Single-shard p99 was 39.7 µs; this run's `scaling.json` is the first baseline for the p99 half.

What stands out (as measured, not explained away):
- **Point reads** (`ycsb-c`): Pigeonhole's p50 is 1.13 µs, against 0.75 µs for RocksDB, with bloom filters and the same cache budget. That already meets the Goals-table p50 < 2 µs and p99 < 10 µs on this machine, but it trails RocksDB by 1.5×.
- **Writes** cost Pigeonhole about 3× RocksDB's latency (`ycsb-a` p50 4.38 µs against 1.63 µs).
- **Scans:** on `ycsb-e`, Pigeonhole is level with RocksDB (23.6K against 24.5K ops/s) and behind SQLite. On `adjacency` it trails every engine, and SQLite is 2× faster than any LSM there.
- **Phase 2 sparse-wide gate** (beat SQLite EAV and RocksDB): not met. Pigeonhole does 66.2K ops/s against 78.5K for SQLite and 101.6K for RocksDB, and its scan tail is the longest (p99 270 µs).
