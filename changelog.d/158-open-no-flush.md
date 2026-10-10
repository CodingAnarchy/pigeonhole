### Changed
- Opening a database after a clean close no longer waits for any flush (#158, D203): no manifest commit when there is nothing to publish, no length sync when the file is exactly as the clean close left it, and the new WAL stream files' and their directory's syncs are submitted and ordered before the first durable commit instead of waited for. Open to first read on macOS goes from about 17 ms to about 1 ms; a `GroupSync` or `Sync` commit right after the open still waits for those flushes.

### Added
- `pigeonhole-io`: `Vfs::submit_sync_dir` (ICR 0021; defaults to a blocking `sync_dir`, run on the I/O pool by `PreadVfs` and through the ring by `UringVfs`). `SimVfs` defers it like other submitted I/O and adds `SimVfs::hold_dir_syncs` for tests.
