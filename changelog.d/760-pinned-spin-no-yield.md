### Changed
- A pinned shard thread's spin before parking runs its first 50 µs without yielding its CPU, then yields every 64 polls as before (D289, #760).
  - Each yield gave a client the core for a whole scheduler slice (about 1 ms on Linux 6.8), and a commit sent meanwhile waited that slice.
  - Not yielding at all moved the wait to clients woken onto the shard's CPU, so the prefix is short.
  - Unpinned threads yield from the start, as do all shard threads when the compaction threads run unpinned for want of CPUs.
  - `Pinning::quiet_spins` counts the shard threads with the prefix.
