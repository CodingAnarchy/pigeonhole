### Changed
- The tablet balancer packs a lightly loaded store's written tablets onto fewer shards, as many as its summed shard busy time needs at half load each, and spreads them again as the load grows (D217, ICR 0034). `Options::tablet_packing(false)` keeps today's placement exactly.
