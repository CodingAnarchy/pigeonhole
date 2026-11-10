### Added
- **`Options::wal_preallocate(bool)`** (#1681, #1621, ICR 0130), default off. On, a writable open sizes each shard's write-ahead log for a busy store before it takes writes: every slot it holds when busy, zero-filled and synced, all shards in parallel. So the log never grows while commits wait on it, which otherwise lengthens their tail latency on a busy store. At the defaults that's 5 slots of 64 MiB a shard (about 5 GiB on 16 shards), written at the open.

### Changed
- **`phdb serve` sizes its write-ahead logs at start-up** (#1681, #1621). Before it accepts connections it writes and syncs about 320 MiB a shard at the defaults, plus the change feed's retention. That's a few seconds on NVMe. `--no-wal-preallocate` turns it off.
