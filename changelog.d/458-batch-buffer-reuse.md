### Changed
- A commit no longer allocates its batch buffer each time, or frees it on the shard's thread: the shard hands the buffer back with the reply, and the committing thread's next `WriteBatch` reuses it (up to 64 KiB). Allocations per commit drop from 5.25 to 4.25 with one cell, and from 7.25 to 3.25 with 16 (#320).
