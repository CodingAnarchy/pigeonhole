### Changed
- A read that misses the block cache, sync or async, no longer allocates and zero-fills a new buffer for the block's bytes as read from the file each time. It reuses one of a few per-thread buffers that the read overwrites entirely. What the block cache keeps is charged exactly as before. Results are unchanged (#709).
