### Added
- `EngineMetrics::cross_shard` (`CrossShardMetrics`, ICR 0043): cross-shard commits' two-phase rounds, the rounds a participant refused (wasted rounds per cross-shard commit), and the refusals by reason (timestamp below the participant's floor, below a moved tablet's floor, a tablet change, no arena room).
