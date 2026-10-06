# Benchmarks

`pigeonhole-bench` (`crates/bench`) measures every performance gate in the [roadmap](design/spec.md#roadmap-risks-open-questions). It runs YCSB A–F plus four wide-column workloads against Pigeonhole and, optionally, RocksDB, SQLite and fjall. It reports p50/p99/p99.9 latency and throughput as JSON and as a markdown table, and it compares two runs for the reproducibility gate.

## Running

```sh
# Every workload against Pigeonhole, default ("small") size:
cargo run -p pigeonhole-bench --release -- all --json out.json --markdown out.md

# One workload against every engine (comparison engines are cargo features):
cargo run -p pigeonhole-bench --release --features rocksdb,sqlite,fjall -- \
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
| `--engine LIST` | `pigeonhole` (default), `rocksdb`, `sqlite`, `fjall`, or `all` |
| `--scale smoke\|small` | Preset size; `small` is the default, `smoke` is what `cargo test` runs |
| `--records N`, `--ops N`, `--value-len N`, `--threads N`, `--seed N` | Override the preset |
| `--warmup F` | Unrecorded warmup, as a fraction of `--ops` (default 0.05) |
| `--write-buffer B`, `--cache B` | Every engine's memory budget (default 256 MiB each; see below) |
| `--shards N` | Pigeonhole shards |
| `--sync` | Fsync every commit on every engine (default: buffered, see below) |
| `--json PATH`, `--markdown PATH` | Write results |
| `--tolerance T` | `compare`: ±T on throughput and p50, ±2T on p99 (default 0.15) |

Always use `--release`: a debug build prints a warning and its numbers mean nothing.

### Comparison engines

The RocksDB, SQLite and fjall runners sit behind the cargo features `rocksdb`, `sqlite` and `fjall`, all off by default. `sqlite` compiles the bundled SQLite (needs a C compiler). `rocksdb` compiles RocksDB from C++ source and runs bindgen. On macOS without Xcode, point bindgen at the Command Line Tools' libclang:

```sh
export LIBCLANG_PATH=/Library/Developer/CommandLineTools/usr/lib
export DYLD_FALLBACK_LIBRARY_PATH=$LIBCLANG_PATH
```

PR-gating CI builds and tests the bench crate with `sqlite,fjall` only, so it never needs a C++ toolchain. A separate workflow, `bench-rocksdb.yml`, builds and tests every comparison runner, RocksDB included. It runs weekly, on pushes to `main` that touch `crates/bench/**`, and on demand, so the `rocksdb` feature cannot rot. Locally: `cargo test -p pigeonhole-bench --all-features`.

### Size limits today

Until the engine flushes memtables to SSTs ([#37](https://github.com/CodingAnarchy/pigeonhole/issues/37)), all Pigeonhole data stays in memory. A table also lives on one shard until tablets split, so one run's data must fit in one shard's memtable budget. The runner raises that budget to 256 MiB (`--write-buffer`). The `small` preset (50,000 records, 200,000 operations, 100-byte values) fits with room to spare. A run that outgrows the budget fails with `Busy` during load, so it never reports a bogus number. Once #37 lands, grow `--records` and `--ops`. The spec's sizes, such as 1M sparse-wide rows, are flags, not code changes ([#52](https://github.com/CodingAnarchy/pigeonhole/issues/52)).

## Workloads

Every workload is deterministic for a seed (default `0x5EED`, recorded in every result). After the load, a warmup of 5% of `--ops` runs unrecorded on one thread: these are the first operations of the same seeded stream, and the measured operations follow them. All measured operations are generated before the clock starts, so generation cost is never measured.

| Workload | Data | Operation mix |
|---|---|---|
| `ycsb-a` | `records` rows × 10 fields (`field0`..`field9`) of `value-len` bytes, keys `user<fnv(i)>` as in YCSB | 50% read, 50% update; Zipfian (θ 0.99, scrambled) |
| `ycsb-b` | same | 95% read, 5% update |
| `ycsb-c` | same | 100% read |
| `ycsb-d` | same | 95% read-latest, 5% insert |
| `ycsb-e` | same | 95% scan of 1–100 rows (uniform), 5% insert |
| `ycsb-f` | same | 50% read, 50% read-modify-write |
| `sparse-wide` | `records` rows, 0–40 cells each (mean 20), qualifiers from a 10K vocabulary with Zipfian popularity | 60% point get (many miss, as in a sparse store), 20% put of 1–4 cells, 20% scan of 10 rows |
| `time-series-ttl` | `records / 100` entities × 100 points; row `ts:<entity>:<reversed time>`, so a scan from the entity prefix returns the newest point first; family with a 1-day TTL | 40% append, 40% scan of the newest 10 points, 20% get of a recent point |
| `adjacency` | about `records / 40` vertices; row `v:<id>`, qualifiers `edge:<dst>`, out-degree `1 + Zipf(256)` (mean ≈ 40), Zipfian destinations | 80% scan of one vertex's edges, 10% scan of 10 vertices, 10% add an edge |
| `skewed-multi-shard` | `records` rows, one cell each | 100% Zipfian writes from `threads` client threads (default 4) |

Interpretation notes (details and rationale in [`design/questions/bench.md`](design/questions/bench.md)):
- A YCSB read fetches one field, not all ten, because a bench op reads one cell.
- Read-modify-write is a get and then a put in every engine, not an atomic operation. For that reason `ycsb-f` refuses `--threads` > 1: concurrent clients would race and lose updates. Every other workload accepts several threads. With more than one thread, operations are dealt round-robin, so a `ycsb-d` read can reach a key whose insert is still queued on another thread. That read is a miss, in every engine alike.
- TTL never expires during a run (puts carry no timestamp), so reads pay the TTL check and nothing more.

### How each store is driven

| Store | Model | Memory (default budget) | Filter | Commit (default / `--sync`) |
|---|---|---|---|---|
| `pigeonhole` | Table `bench`, families `ycsb`, `attr`, `metric` (TTL), `edge`, each `max_versions(1)`; public API only | memtable 256 MiB per shard, block cache 256 MiB | bloom, 10 bits/key | `Buffered` / `Sync` |
| `rocksdb` | Hand-written wide-column key: `escape(row) 00 01 <family> <qualifier>` (order-preserving), one key per cell; no compression codecs compiled in | `write_buffer_size` 256 MiB, LRU block cache 256 MiB | bloom, 10 bits/key | WAL, no fsync / `sync=true` |
| `sqlite-eav` | `cells(row, family, qualifier, value)` `WITHOUT ROWID`, primary key `(row, family, qualifier)`, WAL journal | page cache 512 MiB (write buffer + cache) | none (B-tree) | `synchronous=NORMAL` / `FULL` |
| `fjall` | Same key encoding as RocksDB, one keyspace | `max_memtable_size` 256 MiB, block cache 256 MiB | fjall's default bloom filters | `PersistMode::Buffer` / `SyncAll` |

**Memory budget.** Every engine gets the same `MemoryBudget`: a write buffer (`--write-buffer`) and a read cache (`--cache`), 256 MiB each by default. The read cache matches Pigeonhole's default block cache. The write buffer is raised from Pigeonhole's 64 MiB default, because all data stays in memtables until #37. SQLite has no separate write buffer, so its page cache gets the sum. A one-shard table uses one shard's memtable, so the comparison holds whatever `--shards` is. Every engine's other options are its defaults: no tuning on any side. Each result's `Settings` column prints the budget, filter and durability it ran with.

**Durability.** The default level, written to the OS but not fsynced, survives a process crash in every engine, so the engines compare like with like. Tests check that every comparison runner reads exactly the same cells and bytes as Pigeonhole, operation by operation, for every workload (`runners::agreement`).

Latency is the wall time of one `execute` call, recorded in an HDR-style log-linear histogram (< 0.8% relative error). Throughput is measured operations divided by the wall time of the measured phase, across all client threads.

### Latency caveats

- **Closed loop, no coordinated-omission correction.** Each client issues its next operation only when the previous one returns. A stall delays every operation behind it, but only the stalled one is recorded as slow. Tail percentiles therefore understate what an open-loop client with a fixed arrival rate would see. Read p99.9 as a lower bound.
- **Timer overhead.** Each operation is bracketed by two `Instant::now()` calls, roughly 20–40 ns on current hardware, and that time is included in its latency. It matters only for sub-microsecond operations (point gets on hot data).
- **Sample counts.** At 200,000 measured operations, p99 rests on 2,000 samples and p99.9 on 200, of which the slowest 200 decide the value. With one thread per workload, a handful of OS scheduling events moves p99.9 a lot; see the calibration below. Raise `--ops` before reading anything into p99.9.
- **Warmup.** 5% of `--ops` run unrecorded first (`--warmup`). Warmup does not remove effects that grow with time, such as memtable size growing during the run.

## Reading results

Each result row gives the workload, store, store settings, size, client threads, throughput, and p50/p99/p99.9 latency in microseconds. The JSON (`Suite`) adds the seed, value size, load time, mean and max latency, and an environment fingerprint: CPU, cores, memory, OS, architecture, filesystem of the benchmark directory, build profile and git revision.

**Reference hardware.** Gates are defined on enterprise NVMe with power-loss protection, Linux 6.x and io_uring (spec, Goals). No such machine is attached yet (D5), so every result is labeled `non-reference (D5)`. Non-reference numbers are reported in every run and never gate a phase. A run is labeled reference only when an operator sets `PHDB_BENCH_REFERENCE=1` on Linux; the bench never infers it.

## Gates

| Gate | Phase | How to measure | Pass when |
|---|---|---|---|
| RocksDB gap | 1 (reported) | `all --engine pigeonhole,rocksdb` | Reported, not gated |
| Model value | 2 | `sparse-wide --engine pigeonhole,sqlite,rocksdb` | Pigeonhole beats SQLite EAV and RocksDB on throughput and p99 |
| Latency | 3 | `ycsb-c` (point get), `ycsb-a`/`skewed-multi-shard` (writes), `adjacency`/`ycsb-e` (scans) | Goals table: get p50 < 2 µs, p99 < 10 µs; within 1.5× of RocksDB |
| Scaling | 1 onward | `scaling --shards N`, then `compare` against a stored `scaling.json` | N-shard write throughput ≥ 0.8 × N × single-shard (`efficiency` ≥ 0.8), and single-shard p99 does not regress |
| Reproducibility | all | Two runs, then `compare a.json b.json` | Every result within tolerance |

**The scaling gate has two halves, checked differently.** Each `scaling` run evaluates only the efficiency half and prints pass or fail. The other half, "no regression in single-shard p99", needs a baseline: compare this run's `scaling.json` against a stored one with `phdb-bench compare old/scaling.json new/scaling.json`, which checks the single-shard p99 within the p99 tolerance. The weekly `bench.yml` uploads `scaling.json` with every run, so each run leaves the baseline for the next.

**Scaling today:** a table is one tablet and tablets do not split yet, so the skewed workload's writes all land on one shard whatever N is. `scaling` reports that faithfully, and it fails. Tracked in [#51](https://github.com/CodingAnarchy/pigeonhole/issues/51).

### Reproducibility tolerance

Two runs of the same suite on the same machine agree when, for every result:

| Metric | Tolerance |
|---|---|
| Throughput | ±15% |
| p50 | ±15% |
| p99 | ±30% |
| p99.9, max | reported, not checked |

How the tolerance was chosen: two back-to-back runs of `all --engine all` (`small` preset, 40 results) on the machine below, with nothing else running. Worst drift between the runs: throughput 8.3% (median 1.1%), p50 8.6% (median 1.1%), p99 12.1% (median 1.3%). p99.9 drifted up to 144% (median 1.9%), which is why it is not checked. The tolerance leaves about 2× headroom over the worst observed drift. Runs made while other heavy work shared the machine (another agent's test suite at 300% CPU) disagreed by 20–90% on every engine at once, and `compare` failed them, as it should. `compare` also warns when a run started with a one-minute load average of 2 or more; the load is recorded in every result's environment.

`compare` is symmetric: it flags any change beyond tolerance, faster or slower. An unexplained improvement is as suspect as a regression, and an intended one means it is time for a new baseline. It warns when the two runs come from different machines or build profiles, because the tolerance only means something on one machine. Close other heavy work while measuring. Laptops also throttle and switch between performance and efficiency cores.

## First results (this Mac, non-reference)

Measured 2026-10-06 at commit `f6f22ab`, `phdb-bench all --engine all` (release, `small` preset: 50,000 records, 200,000 operations, 100-byte values, seed `0x5EED`). These numbers are **not reference hardware** (D5): a laptop with an APFS SSD, macOS, and no io_uring. Pigeonhole is memory-bound (#37), so every store's data is hot in memory. The engine has had no tuning. Treat this as a baseline to track, not a verdict.

**Environment:** Apple M5 (10 cores, 24 GiB), macOS 26.5.2 aarch64, APFS [non-reference (D5)]

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(10) budget=256MiB buffered | 50000 | 200000 | 1 | 229.6K | 4.38 | 10.9 | 13.8 |
| ycsb-a | rocksdb | buffered | 50000 | 200000 | 1 | 634.4K | 1.71 | 4.09 | 7.17 |
| ycsb-a | sqlite-eav | buffered | 50000 | 200000 | 1 | 127.4K | 3.89 | 6.40 | 23.3 |
| ycsb-a | fjall | buffered | 50000 | 200000 | 1 | 742.0K | 1.46 | 3.79 | 6.94 |
| ycsb-b | pigeonhole | shards=default(10) budget=256MiB buffered | 50000 | 200000 | 1 | 573.4K | 1.42 | 6.94 | 11.5 |
| ycsb-b | rocksdb | buffered | 50000 | 200000 | 1 | 902.9K | 0.96 | 3.09 | 5.86 |
| ycsb-b | sqlite-eav | buffered | 50000 | 200000 | 1 | 415.6K | 1.79 | 5.57 | 7.97 |
| ycsb-b | fjall | buffered | 50000 | 200000 | 1 | 1.08M | 0.63 | 3.09 | 6.27 |
| ycsb-c | pigeonhole | shards=default(10) budget=256MiB buffered | 50000 | 200000 | 1 | 813.2K | 1.13 | 2.51 | 3.47 |
| ycsb-c | rocksdb | buffered | 50000 | 200000 | 1 | 840.0K | 1.09 | 2.72 | 5.21 |
| ycsb-c | sqlite-eav | buffered | 50000 | 200000 | 1 | 620.5K | 1.67 | 2.46 | 5.02 |
| ycsb-c | fjall | buffered | 50000 | 200000 | 1 | 1.01M | 0.92 | 2.59 | 5.31 |
| ycsb-d | pigeonhole | shards=default(10) budget=256MiB buffered | 50000 | 200000 | 1 | 537.2K | 1.42 | 10.0 | 15.8 |
| ycsb-d | rocksdb | buffered | 50000 | 200000 | 1 | 905.2K | 0.67 | 5.34 | 8.03 |
| ycsb-d | sqlite-eav | buffered | 50000 | 200000 | 1 | 178.2K | 1.92 | 27.8 | 40.2 |
| ycsb-d | fjall | buffered | 50000 | 200000 | 1 | 1.09M | 0.50 | 6.69 | 9.60 |
| ycsb-e | pigeonhole | shards=default(10) budget=256MiB buffered | 50000 | 200000 | 1 | 23.9K | 41.2 | 86.5 | 93.2 |
| ycsb-e | rocksdb | buffered | 50000 | 200000 | 1 | 18.9K | 52.2 | 117 | 127 |
| ycsb-e | sqlite-eav | buffered | 50000 | 200000 | 1 | 27.5K | 31.2 | 68.1 | 79.9 |
| ycsb-e | fjall | buffered | 50000 | 200000 | 1 | 21.6K | 45.6 | 102 | 113 |
| ycsb-f | pigeonhole | shards=default(10) budget=256MiB buffered | 50000 | 200000 | 1 | 182.7K | 5.44 | 12.3 | 16.3 |
| ycsb-f | rocksdb | buffered | 50000 | 200000 | 1 | 482.4K | 2.22 | 5.57 | 8.51 |
| ycsb-f | sqlite-eav | buffered | 50000 | 200000 | 1 | 115.9K | 4.61 | 7.17 | 23.8 |
| ycsb-f | fjall | buffered | 50000 | 200000 | 1 | 582.4K | 1.75 | 4.96 | 8.06 |
| sparse-wide | pigeonhole | shards=default(10) budget=256MiB buffered | 50000 | 200000 | 1 | 70.9K | 2.72 | 253 | 453 |
| sparse-wide | rocksdb | buffered | 50000 | 200000 | 1 | 76.6K | 2.22 | 213 | 420 |
| sparse-wide | sqlite-eav | buffered | 50000 | 200000 | 1 | 79.5K | 2.80 | 77.3 | 141 |
| sparse-wide | fjall | buffered | 50000 | 200000 | 1 | 98.5K | 0.83 | 177 | 340 |
| time-series-ttl | pigeonhole | shards=default(10) budget=256MiB buffered | 50000 | 200000 | 1 | 222.7K | 3.58 | 11.0 | 41.2 |
| time-series-ttl | rocksdb | buffered | 50000 | 200000 | 1 | 622.8K | 1.71 | 3.55 | 7.07 |
| time-series-ttl | sqlite-eav | buffered | 50000 | 200000 | 1 | 119.9K | 2.33 | 20.6 | 41.7 |
| time-series-ttl | fjall | buffered | 50000 | 200000 | 1 | 897.2K | 1.13 | 2.13 | 5.86 |
| adjacency | pigeonhole | shards=default(10) budget=256MiB buffered | 50000 | 200000 | 1 | 58.2K | 8.77 | 81.4 | 99.8 |
| adjacency | rocksdb | buffered | 50000 | 200000 | 1 | 68.4K | 6.53 | 75.3 | 93.2 |
| adjacency | sqlite-eav | buffered | 50000 | 200000 | 1 | 124.8K | 5.05 | 29.1 | 38.7 |
| adjacency | fjall | buffered | 50000 | 200000 | 1 | 76.8K | 4.80 | 85.5 | 99.8 |
| skewed-multi-shard | pigeonhole | shards=default(10) budget=256MiB buffered | 50000 | 200000 | 4 | 346.9K | 9.79 | 30.0 | 148 |
| skewed-multi-shard | rocksdb | buffered | 50000 | 200000 | 4 | 423.0K | 8.51 | 19.6 | 48.6 |
| skewed-multi-shard | sqlite-eav | buffered | 50000 | 200000 | 4 | 114.6K | 5.21 | 13.6 | 3129 |
| skewed-multi-shard | fjall | buffered | 50000 | 200000 | 4 | 330.8K | 2.85 | 98.3 | 165 |

**Scaling gate** (`phdb-bench scaling`, 10 client threads, `shards(1)` against `shards(10)`): 368K and 366K writes/s, efficiency 0.10, so it **fails**. The whole table is one tablet on one shard ([#51](https://github.com/CodingAnarchy/pigeonhole/issues/51)), so this measures no scaling at all, as expected today.

What stands out (as measured, not explained away):
- Point reads (`ycsb-c`) are close to RocksDB: p50 1.13 µs against 1.09 µs. Writes cost Pigeonhole about 3× RocksDB's latency (`ycsb-a` p50 4.38 µs against 1.71 µs).
- Scans: Pigeonhole leads the KV engines on `ycsb-e` and trails SQLite. It trails on `adjacency` and on the scan tail of `sparse-wide` (p99 253 µs).
- The Phase 2 sparse-wide gate (beat SQLite EAV and RocksDB) is not met today: 70.9K ops/s against 79.5K for SQLite and 76.6K for RocksDB.
