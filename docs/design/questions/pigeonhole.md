# Pigeonhole (public crate) questions (Phase 2)

## Proposed decision: lift D95's refusal for Tiered and FifoByTime (#44)
D95 refused `Compaction::Tiered` and `Compaction::FifoByTime` at table creation until their pickers existed. They now do (#31, #32).

**Interim behavior:**
- Both styles are accepted and stored. `Family::zstd` is still refused with `Unsupported` until the codec lands; the rest of #44 stays open for it.
- `FifoByTime` without a TTL is accepted, not refused, although the guide used to say it "needs a TTL". Nothing expires then, and small files still merge, so it is merely pointless. The docs say so.
- The public model test gives family `g` the tiered style and `ttl` the FIFO style, so its sweeps cover both pickers through the public API.
- The engine-wide tuning (`PickerOptions::tiered_*`, `fifo_max_bytes`) is not exposed.
