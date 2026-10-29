### Removed
- **Lower-level crates, test-only items (#695, ICR 0089):** code under `src/` that only tests called, so tests now cover the paths production runs. `pigeonhole` itself is unchanged.
  - `pigeonhole-cache`: `RowEpochs` (the engine's row epochs live in `pigeonhole-shm`'s `RowEpochTable`), and `BlockCache::erase_file` (call `erase_files(&[file])`).
  - `pigeonhole-shm`: `RowEpochTable::note_write` (call `note_write_prev`, which raises the slot the same way and returns its previous value).
  - `pigeonhole-shm`: `ShmRegion::{claim_reader_slot, reclaim_dead_slots}` (use the `_locked` variants), `reserve_seqnos` (use `advance_next_seqno`) and `bind_arena`; `pigeonhole-io`: `SharedRegion::bind_numa`. NUMA binding was never wired in (#1085).
  - `pigeonhole-engine`: `EngineShard::commit_local` (use `Engine::submit`). `pigeonhole-pager`: `Pager::set_features`.
  - `pigeonhole-format`: `ScanFilter::{admits, next_admissible}` and `CompiledFilter::admits` (use `CompiledFilter::check`), `key::{encode_seek_key, SEEK_KIND, column_prefix_len}`, `Escaped::{eq_raw, is_verbatim}`, `BlockIter::entry_offset`.
