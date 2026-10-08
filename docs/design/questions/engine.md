# Questions: engine

## Proposed decision: arenas are sized for their slots with tablet changes off too (#283)
D136 sized arenas for many slots only with tablet changes on: at least 256 chunks, so 64 `(tablet, family)` slots per shard, and smaller chunks when the tablets placed at open need more. Off, an arena was cut into `budget / 64` chunks (capped at 256 KiB), so a budget under 16 MiB served 16 slots. A shard holding more (4 tables of 6 families on one shard: 24) starved when a flush froze every slot. Writes stalled until `Busy`, or the public harness hung.

**Interim behavior:** both modes use `arena_chunk_size`. With tablet changes off, the slots counted are those `shard_for` places at open (`Catalog::max_slots_per_shard`); tables and families created later fit while a shard stays within 64 slots, as with tablet changes on. At the default 64 MiB budget nothing changes (256 KiB chunks either way). `EngineOptions::arena_chunk_bytes` (hidden) pins a layout for tests that starve an arena on purpose (`milestone_b`). A regression target, `engine/tests/slots.rs`, runs the model check with six families on one tablet-off shard. Seed 5 stalled before.
