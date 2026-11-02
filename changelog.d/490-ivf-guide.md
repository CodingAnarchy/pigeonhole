### Changed
- `pigeonhole-vector`: `IndexOptions::nprobe` defaults to 24 lists (was 16): on SIFT1M it gives 95.8% recall@10 at 3.4 ms per query (p99 5.4 ms), single-threaded, with the index in the block cache.

### Added
- A user-guide page on vectors: vector families, exact `nearest_*` search, and the IVF index, with the SIFT1M recall-against-latency table, block-cache sizing, bulk loading and background rebalancing.
