# 0002: `ShmConfig::first_seqno` and `ReaderSlot::pin` returns the pinned pair

**Status:** Approved (coordinator review of PR #11, 2026-10-05). Implemented in PR #11.

## Change

1. Add `first_seqno: Seqno` to `pigeonhole_shm::ShmConfig` (`#[non_exhaustive]`, D33, so adding a field is not a breaking change; `ShmConfig::new` sets it to 1). A region built by `ShmRegion::open(.., Role::Writer, ..)` or `ShmRegion::in_memory` starts its `next_seqno` counter at `max(first_seqno, 1)`. The writer passes the seqno ceiling it recovered from the WAL and manifest (`Counters.seqno_ceiling` + 1 in the engine's open sequence), so the visible seqno never goes backwards across a writer kill and restart: readers that stayed on the old generation's snapshot see a new generation whose `visible_seqno()` is at least what they had.
2. `ReaderSlot::pin(seqno, view_version)` returns `(Seqno, u64)`, the pair actually pinned, instead of `()`. When the post-store re-check of `view_pointer` finds a newer view, the pin moves to that view **and** to a snapshot seqno taken after it (`max(visible_seqno(), seqno)`), so the slot always holds a consistent pair; the caller must use the returned pair. FORMAT §11.5 is updated accordingly.
3. Additive: `ShmRegion::next_seqno()` (acquire load of `next_seqno`), the lower bound step 1 of FORMAT §11.3 publishes. Also additive: `Error::ViewVersionNotNewer` (a `publish_view` at or below the published version is refused) and `Error::InvalidConfig` with `ShmConfig::validate`.

## Why

Without (1) a writer restart would restart seqnos at 1 while readers still hold snapshots above it, breaking "readers see commits in order" across generations. Without (2) a pin that moved to a newer view would silently keep a seqno chosen before that view, and the caller could not learn the pair it is now protected by.

## Callers

- `ShmConfig`: built by `pigeonhole-engine` from `EngineOptions` (still `todo!()`); it sets `first_seqno` after recovery, before `ShmRegion::open`. Tests in `crates/shm` set it directly.
- `ReaderSlot::pin`: called by the engine's reader-process snapshot path (`docs/design/interfaces.md`, Read path step 1; still `todo!()`). It takes the returned pair as the snapshot instead of the values it passed. No other callers.
- `next_seqno`: new; the engine's group commit (Write path step 3) uses it for the lower bound instead of `visible_seqno() + 1`.
