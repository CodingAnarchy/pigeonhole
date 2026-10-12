### Fixed
- After a power loss shortly after a database was created, or after its shard count was raised, a later crash could lose acknowledged commits (#520, D211). A shard could commit a WAL checkpoint for a newly created stream file before the file's name was durable. If the power loss then took the file, the reopened database recreated it under the old checkpoint and skipped its first records. A checkpoint now waits until the stream's creation is durable. Released versions (0.2.0 to 0.2.2) are not affected.

### Added
- `pigeonhole-wal`: `Wal::created_durable` and `Wal::notify_created` (defaulted; ICR 0031) report when a new stream's file and directory entry are durable.
