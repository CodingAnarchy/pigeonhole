### Changed
- `phdb serve`: a change-feed hub tells its streams about new commits once per batch it reads, not once per commit, which takes fewer wakes under steady writes.
- `phdb serve`: new `--feed-hub-coalesce-ticks N` and `--feed-client-coalesce-ticks N` (0, the default, never waits). After a small batch, a hub or a stream waits N 1 ms ticks before looking again, so the commits arriving meanwhile are read together: fewer wakes per commit for up to a few milliseconds more lag under steady writes. A commit after a quiet gap is never held back (#1901).
