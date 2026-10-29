### Removed
- **Lower-level crates, test-only items (#695):** code under `src/` that only tests called, so tests now cover the paths production runs. `pigeonhole` itself is unchanged.
  - `pigeonhole-cache`: `RowEpochs` (the engine's row epochs live in `pigeonhole-shm`'s `RowEpochTable`), and `BlockCache::erase_file` (call `erase_files(&[file])`).
  - `pigeonhole-shm`: `RowEpochTable::note_write` (call `note_write_prev`, which raises the slot the same way and returns its previous value).
