### Changed
- **A cross-shard transaction no longer fails with `Conflict` just because another cross-shard commit had a share on the same rows still undecided** (D249). Its PREPARE is refused and retried, with the same snapshot and reads, once that share is decided. A commit that only overlaps an earlier commit in the same group waits for the next group. `Conflict` now means a real conflict: a write to a row the transaction read, after its snapshot. A retry that cannot get through before `Options::write_stall_timeout` fails with `Busy`.
- Two cross-shard commits that each meet the other's undecided share resolve in submission order: the older one commits first.

### Added
- `CrossShardMetrics::refused_pending_overlap`: PREPAREs refused because they overlapped another commit's undecided share (ICR 0045).
