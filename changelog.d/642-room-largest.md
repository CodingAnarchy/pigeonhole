### Changed
- A write that waits for memtable room while no flush is in flight now freezes the largest memtables, until they hold about half the arena, instead of every open one (D124). A shard holding many tablets now flushes fewer, larger SSTs and stalls less: locally, a 16-tablet hot shard went from 109–245 background jobs and 1.2–2.9 s of stalls to 31–44 jobs and about 0.1 s.
