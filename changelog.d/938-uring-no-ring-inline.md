### Fixed
- `pigeonhole-io`, `pigeonhole`: in application-owned mode on io_uring, a shard thread whose own ring the kernel refused (`ENOMEM` against the locked-memory limit, or a kernel without the needed support) could leave its WAL syncs unreaped on the shared ring, hanging flushes and commits. Such a thread now runs its I/O inline, as the pread backend does, and retries for a ring later (#938, #929).

### Added
- `pigeonhole`: `IoRings::attach_failures` and `IoRings::inline` count refused ring attaches and the operations run inline meanwhile (ICR 0070, #938). `pigeonhole-io`'s `RingStats` has the same fields.
