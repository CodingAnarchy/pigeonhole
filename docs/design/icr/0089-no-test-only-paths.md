# 0089: Lower-level items that only tests called are removed (no test-only production paths)

**Status:** Approved (owner's cleanup via the coordinator, 2026-10-29; #695). Number assigned by the coordinator. Breaking for the lower-level crates only, before the 1.0 freeze. `pigeonhole` itself is unchanged, and no behaviour changes.

## Change
**#1084:**
- **`pigeonhole-cache`:**
  - `RowEpochs` is removed. The engine's row epochs are `pigeonhole-shm`'s `RowEpochTable` (D281), and no crate used this type.
  - `BlockCache::erase_file` is removed: call `erase_files(&[file])`, which is what the engine calls.
- **`pigeonhole-shm`:** `RowEpochTable::note_write` is removed. Call `note_write_prev`, which raises the slot the same way (`fetch_max`, Release) and returns its previous value.

**The second PR** (decided by the coordinator under the owner's rule, 2026-10-29):
- **`pigeonhole-shm`:**
  - `ShmRegion::claim_reader_slot` and `reclaim_dead_slots` (liveness by pid) are removed: call `claim_reader_slot_locked(process, file)` and `reclaim_dead_slots_locked(file)`, as the engine does. `ReaderSlot` always holds its `FileRef`.
  - `ShmRegion::reserve_seqnos` is removed: the writer reserves in its own process and raises the mirror with `advance_next_seqno` (ICR 0063).
  - `ShmRegion::bind_arena` is removed (never wired into shard start; NUMA binding is deferred past 1.0 as #1085, and the spec says so).
- **`pigeonhole-io`:** `SharedRegion::bind_numa` (and its `mbind` path) is removed with it.
- **`pigeonhole-engine`:**
  - `EngineShard::commit_local` is removed: call `Engine::submit`.
  - `Engine::set_tail_index` (the `test-hooks` switch `READS_STEP`) is removed: production never set it, and `EngineOptions::memtable_tail_index` chooses the same at open.
- **`pigeonhole-pager`:** `Pager::set_features` is removed (no production caller; tests write superblock pages directly).
- **`pigeonhole-format`:**
  - `ScanFilter::admits`, `ScanFilter::next_admissible` and `CompiledFilter::admits` are removed: `CompiledFilter::check` is the rule every source applies.
  - `key::encode_seek_key`, `key::SEEK_KIND`, `key::column_prefix_len`, `Escaped::eq_raw` and `Escaped::is_verbatim` are removed. FORMAT §2 still defines seek keys, and the golden `keys.bin` still holds one, now built by the tests.
  - `BlockIter::entry_offset` is removed.

## What a re-implementation of `Pager::set_features` must keep (simp724's S1 on #1087)
D269 and FORMAT §13 still describe flagging a feature, and #487 is its first user. The `set_features` that #1087 removes, and its two tests, held four constraints:
1. **Two commits.** Both superblock pages carry the new sets before the first structure that uses the feature.
2. **Each commit names the root current when it starts**, read under the state lock as the commit slot is claimed, so a root committed between the two is never replaced by an older one. That was perf287's S1 on #858; the removed test was `a_feature_change_keeps_a_root_committed_between_its_two_commits`.
3. **The sets become current only once the page is durable.** A failed commit leaves them as they were and poisons the pager, as any failed commit does; the removed test was `a_failed_feature_commit_leaves_the_sets_as_they_were`.
4. **The engine routes the change through its manifest writer as a request**, so it never interleaves with a manifest commit (an overlapping root commit fails "root commits overlap").

The open-time repair of pages whose sets disagree stays in the pager (`OpenedPager::finish`), since another build can leave them.

## Why
The owner's rule, now in AGENTS.md and CONTRIBUTING.md: code under `src/` that tests call must be the code production runs. A method only tests call keeps passing tests while the production path beside it changes untested (#1055 left `note_write` under `cfg(test)` while production called `note_write_prev`).

## Callers changed
Tests, benches and doc examples only, in both PRs:
- the cache crate's tests and loom models (`erase_files`; the epoch model drives a bare atomic with `RowEpochTable`'s operations);
- the engine's row-cache unit tests (`note_write_prev`);
- shm's doctest;
- in the second PR, the shm protocol, simulator and multiprocess tests (the locked and clock paths), the engine's `api`, `writer_fork`, `read_only_fallback` and `tail_index` tests, the pager's feature tests, and the format, sst and cli tests listed above.

No production caller existed. `docs/design/interfaces.md`'s cache entry is updated.
