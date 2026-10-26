# 0072: a row cache in reader processes (`ReaderOptions::row_cache*`, `PigeonholeReader::row_cache_stats`) and the shared row-epoch table

**Status:** Approved (coordinator, 2026-10-26; D281, #926; design reviewed by perf287). Additive in `pigeonhole`; shared-memory layout 5.

## Change

**`pigeonhole`:**
- **`ReaderOptions::row_cache(bytes)`:** 64 MiB by default (`None` until set), 0 turns it off.
- **`ReaderOptions::row_cache_max_row(bytes)` and `row_cache_family(table, family)`:** as on `Options`.
- **`PigeonholeReader::row_cache_stats() -> RowCacheStats`:** hits, misses and fills since the reader last attached to a writer generation.

**`pigeonhole-engine`:**
- **`Engine::row_cache_stats`:** in a reader process it reports the current attachment's cache.
- **`pub fn row_epoch_slots(row_cache_bytes) -> u32`:** the slot count the writer gives the region, for `pigeonhole`'s shared-memory footprint estimate.
- **Reader processes now use `EngineOptions::row_cache_bytes`, `row_cache_max_row` and `row_cache_families`.**

**`pigeonhole-shm`:**
- **`ShmConfig::row_epoch_slots`:** a power of two, default 2^18, at most `MAX_ROW_EPOCH_SLOTS` (2^24).
- **`ShmRegion::row_epochs() -> RowEpochTable`:** a new type with `note_write(hash, seqno)`, `epoch(hash, seqno)`, `seed()` and `slots()`.

**`pigeonhole-format`:**
- **Header fields:** `shm::header::{ROW_EPOCHS_OFF, ROW_EPOCH_SLOTS, ROW_EPOCH_SEED}` and `ShmHeader::{row_epochs_off, row_epoch_slots, row_epoch_seed}`.
- **`ShmHeader::layout`** takes the slot count (a new parameter before the device).
- **Constants:** `ROW_EPOCH_LEN` and `ROW_EPOCHS_ALIGN`.
- **`hash::SeededBuildHasher`.**
- **`ShmLayoutVersion::CURRENT` = 5.**

## Why

D281 (#926): a reader process read hot rows at 0.09× the writer process with the row cache on, because only the writer had a cache. D201's protocol needs no invalidation messages, only the per-row write watermarks. Moving them into the region lets every reader process run it against a cache of its own.

## Callers

- **`pigeonhole`, `db.rs`:** `ShmFootprint::of` passes `row_epoch_slots` to `ShmHeader::layout`.
- **`pigeonhole-shm`:** `build` and `in_memory` lay out the table and set the seed.
- **`pigeonhole-engine`:**
  - the writer's open sizes the table (`table_slots`) and builds `RowEpochs` on it;
  - shards raise slots through `Shared::row_epochs`;
  - readers build a `RowCaches` per attachment.
- **Tests:** `format` (literals, golden header, round trip, M1 decode cases) and `shm` (versions, M1 at attach).
