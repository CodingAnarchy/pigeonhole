### Fixed
- A shard poisoned while splitting, merging or moving a tablet now records its failure, so `Pigeonhole::is_failed()` and `failure()` report it (cause `Internal`). Before, the instance refused writes but reported itself healthy (#634).
