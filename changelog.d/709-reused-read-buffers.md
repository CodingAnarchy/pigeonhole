### Changed
- A read that misses the block cache, sync or async, no longer allocates and zero-fills a new buffer for the block's bytes each time. It reuses one of a few per-thread buffers that the read overwrites entirely, and a block kept by the cache returns its buffer when evicted. Results are unchanged (#709).
