### Fixed
- The engine's metrics report block-cache hits and misses (#57): `engine_metrics().block_cache` was always `(0, 0)` because the cache kept no counters.

### Added
- `pigeonhole-cache`: `BlockCache::hits_and_misses` (ICR 0024), counted per cache shard with relaxed atomics.
