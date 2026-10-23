### Added
- `pigeonhole-engine`: `Snapshot::table(id)`, the table as the snapshot's catalog knew it, borrowed (ICR 0068, #927, #892).

### Changed
- `pigeonhole`: reads at a snapshot (row reads, scans, `get_at`, a transaction's reads) resolve and name families from the catalog as of the snapshot (#927, #892).
