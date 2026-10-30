### Changed
- **The row cache now serves transactions and snapshot reads** (D290, #708). Point gets in a transaction or at a snapshot are answered from the writer's row cache when the cached row is current at their read point. Before, they always read the memtables and SSTs.
- **A commit that overwrites cells of a cached row keeps it cached** at the new version, instead of leaving it to miss until it is read twice more.
- **Point gets are served from cached rows but don't fill the cache;** row reads fill it, as before (D290 as amended after the A/B on #1059: storing a whole family row for one cell cost write-heavy point-get workloads more at the tail than it saved: in the box A/B, transfer's p99 fell from 224 to 181 µs without it, ycsb-f gained 3.6% throughput, and sparse-wide get p99 fell from 18 to 10 µs).

### Added
- `RowCacheStats::write_throughs`: cached rows a commit moved forward.
