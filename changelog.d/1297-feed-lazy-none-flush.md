### Changed
- With the change feed on (`Options::change_feed`), `Durability::None` commits to fed tables stay lazy. The writer writes them within 10 ms when no stronger commit does, so the feed can deliver them. The shared-memory layout is now version 7, which adds each shard's written and synced watermarks (D310, #1248), so reader processes must run the same release as the writer.
