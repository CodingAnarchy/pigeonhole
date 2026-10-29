### Removed
- **Lower-level crates (#1108):** `pigeonhole-engine` drops items nothing used. `pigeonhole` itself is unchanged.
  - `CrossShardMetrics::{refused_below_floor, refused_move_raise}`, deprecated and always 0 since 0.4.0 (`pigeonhole` removed its copies in #1046).
  - `Engine::try_read_row_latest_into` (call `try_read_row_latest` and drop the miss).
  - `CheckCounts::manifest_extents`, and `EngineOptions::room_recheck_nanos` (fixed at 1 ms).
