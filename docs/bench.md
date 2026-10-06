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
| `--shards N`, `--memtable-budget B` | Pigeonhole settings |
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

CI builds and tests the bench crate with `sqlite,fjall` only, so it never needs a C++ toolchain. Run the RocksDB runner's tests locally with `cargo test -p pigeonhole-bench --all-features`.

### Size limits today

Until the engine flushes memtables to SSTs ([#37](https://github.com/CodingAnarchy/pigeonhole/issues/37)), all Pigeonhole data stays in memory. A table also lives on one shard until tablets split, so one run's data must fit in one shard's memtable budget. The runner raises that budget to 256 MiB (`--memtable-budget`). The `small` preset (50,000 records, 200,000 operations, 100-byte values) fits with room to spare. A run that outgrows the budget fails with `Busy` during load, so it never reports a bogus number. Once #37 lands, grow `--records` and `--ops`. The spec's sizes, such as 1M sparse-wide rows, are flags, not code changes ([#52](https://github.com/CodingAnarchy/pigeonhole/issues/52)).

## Workloads

Every workload is deterministic for a seed (default `0x5EED`, recorded in every result). All operations are generated before the clock starts, so generation cost is never measured.

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
- Read-modify-write is a get and then a put in every engine, not an atomic operation.
- TTL never expires during a run (puts carry no timestamp), so reads pay the TTL check and nothing more.

### How each store is driven

| Store | Model | Commit (default / `--sync`) |
|---|---|---|
| `pigeonhole` | Table `bench`, families `ycsb`, `attr`, `metric` (TTL), `edge`, each `max_versions(1)`; public API only | `Buffered` / `Sync` |
| `rocksdb` | Hand-written wide-column key: `escape(row) 00 01 <family> <qualifier>` (order-preserving), one key per cell, default options, no compression codecs compiled in | WAL, no fsync / `sync=true` |
| `sqlite-eav` | `cells(row, family, qualifier, value)` `WITHOUT ROWID`, primary key `(row, family, qualifier)`, WAL journal | `synchronous=NORMAL` / `FULL` |
| `fjall` | Same key encoding as RocksDB, one keyspace, default options | `PersistMode::Buffer` / `SyncAll` |

The default level, written to the OS but not fsynced, survives a process crash in every engine, so the engines compare like with like. Tests check that every comparison runner reads exactly the same cells and bytes as Pigeonhole, operation by operation, for every workload (`runners::agreement`).

Latency is the wall time of one `execute` call, recorded in an HDR-style log-linear histogram (< 0.8% relative error). Throughput is measured operations divided by the wall time of the measured phase, across all client threads.

## Reading results

Each result row gives the workload, store, store settings, size, client threads, throughput, and p50/p99/p99.9 latency in microseconds. The JSON (`Suite`) adds the seed, value size, load time, mean and max latency, and an environment fingerprint: CPU, cores, memory, OS, architecture, filesystem of the benchmark directory, build profile and git revision.

**Reference hardware.** Gates are defined on enterprise NVMe with power-loss protection, Linux 6.x and io_uring (spec, Goals). No such machine is attached yet (D5), so every result is labeled `non-reference (D5)`. Non-reference numbers are reported in every run and never gate a phase. A run is labeled reference only when an operator sets `PHDB_BENCH_REFERENCE=1` on Linux; the bench never infers it.

## Gates

| Gate | Phase | How to measure | Pass when |
|---|---|---|---|
| RocksDB gap | 1 (reported) | `all --engine pigeonhole,rocksdb` | Reported, not gated |
| Model value | 2 | `sparse-wide --engine pigeonhole,sqlite,rocksdb` | Pigeonhole beats SQLite EAV and RocksDB on throughput and p99 |
| Latency | 3 | `ycsb-c` (point get), `ycsb-a`/`skewed-multi-shard` (writes), `adjacency`/`ycsb-e` (scans) | Goals table: get p50 < 2 µs, p99 < 10 µs; within 1.5× of RocksDB |
| Scaling | 1 onward | `scaling --shards N` | N-shard write throughput ≥ 0.8 × N × single-shard (`efficiency` ≥ 0.8), and single-shard p99 does not regress against the previous run (`compare`) |
| Reproducibility | all | Two runs, then `compare a.json b.json` | Every result within tolerance |

**Scaling today:** a table is one tablet and tablets do not split yet, so the skewed workload's writes all land on one shard whatever N is. `scaling` reports that faithfully, and it fails. Tracked in [#51](https://github.com/CodingAnarchy/pigeonhole/issues/51).

### Reproducibility tolerance

Two runs of the same suite on the same machine agree when, for every result:

| Metric | Tolerance |
|---|---|
| Throughput | ±15% |
| p50 | ±15% |
| p99 | ±30% |
| p99.9, max | reported, not checked |

<!-- CALIBRATION -->

`compare` warns when the two runs come from different machines or build profiles, because the tolerance only means something on one machine. Close other heavy work while measuring. Laptops also throttle and switch between performance and efficiency cores.

## First results (this Mac, non-reference)

<!-- RESULTS -->
