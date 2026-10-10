### Added
- `EngineMetrics` (and the engine's `Metrics`) count async commits that waited on the global visibility watermark, the waiter-list entries they scanned, wake passes, waiters woken and lock contention (ICR 0023). `phdb-bench` reports them with each run's stalls and prints visibility waits per operation on the scaling line.
- `crates/bench/baselines/phase3-io/scaling.sh`: the scaling step of the Phase 3 gate-window run (#405), at N = 1, 2, 4, 8 and 16 shards with a median summary.
