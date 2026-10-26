# 0074: shm `CROSS_HELD`, the allocator's hold on decided cross-shard seqnos

**Status:** Approved (coordinator, 2026-10-26; D278, perf287's review on #932). `pigeonhole-shm` and `pigeonhole-format` only; the public `pigeonhole` API doesn't change.

## Change

- **`pigeonhole-format`:**
  - `shm::header::CROSS_HELD = 192`: a `u64` atomic in the region header's reserved room, after D281's row-epoch fields (160..188) and 4 bytes of padding, so it's 8-aligned. A test checks that no two header fields overlap.
  - `ShmLayoutVersion::CURRENT` goes up by one: 5 on this branch, 6 once #947 (D281) has taken 5.
- **`pigeonhole-io`:** `SharedRegion::atomic_u64s(offset, n) -> &[AtomicU64]`, a run of words with one bounds and one alignment check. It's additive. Reading `CROSS_HELD` through a second `atomic_u64` call cost ~13 instructions per `visible_seqno` (two checks), and a snapshot calls it twice: get-miss +0.79% against its 0.5% threshold.
- **`pigeonhole-shm`:**
  - `ShmRegion::publish_cross_held(seqno)`: stores the lowest held decided seqno (Release); `u64::MAX` when none.
  - A new region initializes the word to `u64::MAX`.
  - `visible_seqno` and `durable_seqno` take the minimum with it. They load `NEXT_SEQNO` (Acquire), then `CROSS_HELD` (Acquire), then the shards' watermarks (FORMAT §11.3).
- **Docs:** FORMAT §11.1's header table, §11.3's protocol, and the layout history (§11's "Shared-memory layout version 6").

## Why

D278: the participant that applies a cross-shard commit last releases its seqno, from its own thread. A shard's `pending` word has one writer, its own thread, and its value depends on that shard's local state, so the hold can't stay there. The allocator writes `CROSS_HELD` under its lock, which gives the word one writer at a time.

## Callers

- **`pigeonhole-engine`:**
  - `clock.rs`: `Reservation::{reserve_decision, release}` keep the held set and publish the word.
  - `shard.rs`: `Completion`, `decide_share`, `complete`, `on_applied`, `answer_unsettled`.
  - `clock_loom.rs`.
- **`pigeonhole-io`:** `shared.rs` and its unit test (Miri).
- **`pigeonhole-shm`:** `region.rs` (`next_and_held`, through `atomic_u64s`) and its tests; `tests/sim.rs` and `tests/multiprocess.rs` (layout mismatch).
- **`pigeonhole-format`:** `tests/format_literals.rs`, `tests/roundtrip.rs`, `tests/golden/shm_header.bin`.
