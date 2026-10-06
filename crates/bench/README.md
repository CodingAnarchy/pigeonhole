# pigeonhole-bench

Benchmark workloads and comparison runners for Pigeonhole: YCSB A–F, sparse-wide, time series with TTL, graph adjacency and skewed multi-shard writes. They run against Pigeonhole and, behind the cargo features `rocksdb`, `sqlite` and `fjall`, against RocksDB, SQLite (EAV) and fjall. Results include p50/p99/p99.9 latency and throughput, as JSON and markdown.

```sh
cargo run -p pigeonhole-bench --release -- all --json out.json
cargo run -p pigeonhole-bench --release -- compare a.json b.json
```

How to run it, how to read results, and what the gates are: [`docs/bench.md`](../../docs/bench.md).

This is an internal crate of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole) and is not published. Most users want the [`pigeonhole`](https://crates.io/crates/pigeonhole) crate.
