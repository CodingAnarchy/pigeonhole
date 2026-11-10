### Changed
- **The block and row caches free evicted entries after their shard lock, not under it** (#1559). A cache shard's write lock no longer waits on the allocator, or on another thread's buffer pool, while it frees what an insert evicted or replaced. Readers queued behind those waits at high reader counts: a 3.4 ms worst-case wait on a cache hit at 32 readers (#1537).
