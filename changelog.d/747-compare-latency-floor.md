### Changed
- `phdb-bench compare` fails a latency only when it moved past both its relative tolerance and an absolute floor: p50 ±20% or ±3 µs, p99 ±40% or ±6 µs, with the floors scaling with `--tolerance` (D259). Throughput is unchanged and p99.9 is still reported only. A one-thread p50 that moved 2.5 µs (+25%) had failed the Phase 3 gate's reproducibility check.
