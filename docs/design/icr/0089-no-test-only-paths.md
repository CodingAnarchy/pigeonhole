# 0089: Lower-level items that only tests called are removed (no test-only production paths)

**Status:** Approved (owner's cleanup via the coordinator, 2026-10-29; #695). Number assigned by the coordinator. Breaking for the lower-level crates only, before the 1.0 freeze. `pigeonhole` itself is unchanged, and no behaviour changes.

## Change
**#1084:**
- **`pigeonhole-cache`:**
  - `RowEpochs` is removed. The engine's row epochs are `pigeonhole-shm`'s `RowEpochTable` (D281), and no crate used this type.
  - `BlockCache::erase_file` is removed: call `erase_files(&[file])`, which is what the engine calls.
- **`pigeonhole-shm`:** `RowEpochTable::note_write` is removed. Call `note_write_prev`, which raises the slot the same way (`fetch_max`, Release) and returns its previous value.

**The second PR** (listed on #695, decided by the coordinator under the owner's rule) changes items under this ICR too:
- shm `claim_reader_slot`, `reclaim_dead_slots` and `reserve_seqnos`;
- engine `Engine::commit_local`;
- shm `bind_arena`;
- pager `set_features` and format helpers that only tests use, which move out of `src/`.

That PR updates this file with its final list.

## Why
The owner's rule, now in AGENTS.md and CONTRIBUTING.md: code under `src/` that tests call must be the code production runs. A method only tests call keeps passing tests while the production path beside it changes untested (#1055 left `note_write` under `cfg(test)` while production called `note_write_prev`).

## Callers changed
Tests only:
- the cache crate's tests and loom models (`erase_files`; the epoch model drives a bare atomic with `RowEpochTable`'s operations);
- the engine's row-cache unit tests (`note_write_prev`);
- shm's doctest.

No production caller existed. `docs/design/interfaces.md`'s cache entry is updated.
