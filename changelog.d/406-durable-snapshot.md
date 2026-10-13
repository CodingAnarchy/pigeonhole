### Added
- **Durable snapshots** (D227, ICR 0037): `durable_snapshot()` on the writer and on reader-process handles sees a commit only once it is durable at its commit's level: of what it reads, `GroupSync` and `Sync` commits survive a power loss, `Buffered` ones a process crash, `None` ones nothing. That is the view every snapshot had before D227. `backup_durable` takes a backup from one, holding only commits durable at their level.

### Changed
- The shared-memory layout is version 2: each shard publishes a `durable` watermark beside `pending` (FORMAT §11.2–11.3). Writer and reader processes must run builds with the same layout version. **Stop reader processes before upgrading the writer:** a writer of the new build fails to open with `ShmVersionMismatch` while readers of an older build (0.2.x included) are still attached, and an old reader can't attach to a new writer.
- A failed WAL fsync now counts as the shard's death (D85 since D227). The failed commits never become durable, so durable snapshots and `backup_durable` never include them. The close is unclean, and the database needs reopening.
- A memtable freezes only once everything in it is durable (D117's rule at the durable seqno), so a flush never collapses a version a durable snapshot reads. A freeze may wait for the fsyncs in flight.
