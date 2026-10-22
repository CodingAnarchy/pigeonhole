### Changed
- The writer judges whether a reader process is alive by a byte-range lock the reader holds on the database file for its slot, not by its process id (#693). A reader in another PID namespace (another container sharing the volume and shared memory) is no longer taken for dead, losing its slot and getting `SnapshotExpired` over and over, so the writer and its readers no longer need to share a PID namespace. A reader that dies while claiming a slot no longer leaks it until the next writer generation. The shared-memory layout version is now 3: a writer and readers from earlier releases can't attach to each other's shared memory (`ShmVersionMismatch`).
- A process forked from a reader process can no longer free the parent's reader slot or clear its pins by dropping its copy of the reader; in the child the copy does nothing, and its snapshots fail with `SnapshotExpired` (#693).

### Fixed
- Two processes closing a database at the same moment no longer both decide another is still open and leave the shared memory behind (#150): one that can't upgrade its presence lock releases it and retries.
