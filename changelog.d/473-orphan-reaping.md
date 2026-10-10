### Fixed
- Application-owned io_uring: a WAL sync could wait for ever for an older sync on the shared ring, which has no reaper there (#408); the WAL's blocking waits now reap it (ICR 0028). And a shard moved to another thread gets an I/O ring on that thread, instead of submitting to a ring no thread reaps. Both hung `phdb-bench scaling` on `PIGEONHOLE_IO=uring`.

### Added
- `pigeonhole-io`: `reap_orphan_io`, for a thread blocked on something chained after I/O no thread reaps (ICR 0028).
