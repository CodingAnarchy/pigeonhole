### Added
- `examples/scanrate.rs`: ordered single-family scan throughput from cache, as decoded GB/s per core (#29). `crates/bench/baselines/phase3-io/scan-rate.sh` runs it in the Phase 3 gate window (#405). The same binary adds callgrind shapes `scan-narrow`, `scan-small` and `scan-wide` (one compacted source, one version per column) to the instructions workflow.
