### Changed
- **The row cache now serves transactions and snapshot reads** (D290, #708). Point gets in a transaction or at a snapshot are answered from the writer's row cache when the cached row is current at their read point. Before, they always read the memtables and SSTs.
- **A commit that overwrites cells of a cached row keeps it cached** at the new version, instead of leaving it to miss until it is read twice more.
- **Point gets now fill the cache on a row's second miss,** as row reads already did. Families with a merge operator are not filled by gets.

### Added
- `RowCacheStats::write_throughs`: cached rows a commit moved forward.
