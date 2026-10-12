### Added
- **Durable snapshots** (D227, ICR 0037): `durable_snapshot()` on the writer and on reader-process handles sees a commit only once it is durable at its level, so everything it reads survives a power loss. That is the view every snapshot had before D227. `backup_durable` takes a backup from one, holding only durable commits.

### Changed
- The shared-memory layout is version 2: each shard publishes a `durable` watermark beside `pending` (FORMAT §11.2–11.3). Writer and reader processes must run builds with the same layout version.
- A memtable freezes only once everything in it is durable (D117's rule at the durable seqno), so a flush never collapses a version a durable snapshot reads. A freeze may wait for the fsyncs in flight.
