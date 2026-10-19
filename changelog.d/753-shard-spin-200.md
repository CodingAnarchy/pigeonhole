### Changed
- `Options::shard_spin` and `EngineOptions::shard_spin_nanos` default to 200 µs instead of 50 µs (D198, amended 2026-10-19): a store whose clients commit across shards no longer settles into a regime where every message wakes a parked shard (the 0.3.0 gate's cross-shard row read 398K ops/s against 590K, p99.9 3–5 ms against 0.5, after D257's packed load regrew). An idle store still never polls; `Duration::ZERO` still turns the window off.
- The Phase 3 gate window's `ycsb-a-shards` step runs the shard spin at 0, 100 and 500 µs beside the default (200), instead of 100, 200 and 500.
