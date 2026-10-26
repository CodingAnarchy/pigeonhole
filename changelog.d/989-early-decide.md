### Changed
- **A `Buffered` cross-shard commit finishes sooner** (D286, #708). The coordinating shard now tells the other shards to apply as soon as it decides, while it writes its decision, rather than after. A commit still becomes visible and answered only once its decision is written. Other durability levels are unchanged.
