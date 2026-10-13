# 0044: `Pigeonhole::engine_metrics` and `EngineMetrics` become documented, semver-covered public API

**Status:** Approved (owner, via the coordinator, 2026-10-13). For 0.3.0. Supersedes ICR 0015's "bench hook" status for the public crate.

## Change

In `pigeonhole`:
- `engine_metrics()` loses `#[doc(hidden)]`.
- `EngineMetrics` is no longer a hidden re-export of `pigeonhole_engine::Metrics`. It is the public crate's own `#[non_exhaustive]` struct with named fields, converted from the engine's (`From<pigeonhole_engine::Metrics>`).
- Two companion types, also `#[non_exhaustive]`:
  - `CommitStats`, returned by `EngineMetrics::commits(Durability)`: `count`, `p50_nanos`, `p99_nanos`, `p999_nanos`;
  - `CrossShardMetrics` (ICR 0043), the public crate's own copy.

`pigeonhole-engine::Metrics` is unchanged, apart from a doc fix (see "A bug" below).

Every field is documented: what it counts, its unit (nanoseconds, bytes), and whether it's a cumulative counter (0 at open, never reset, a reopen starts again) or a gauge (the value now).

**Why a struct of its own rather than the engine's type:**
- The owner asked for tidy names under semver. Renaming the engine's fields would be a breaking change to `pigeonhole-engine`, which is published too.
- With its own struct, the public names are free, and `cargo semver-checks` reports both crates as unchanged or additive. Hidden items aren't public API to it, so the hidden → visible change is an addition.
- It also keeps engine internals (tuples, arrays indexed by `Durability as usize`) out of the API that becomes the C ABI.

**Names**, engine field → public field:

| `pigeonhole_engine::Metrics` | `pigeonhole::EngineMetrics` |
|---|---|
| `commits[d]`, `commit_latency_nanos[d][0..3]` | `commits(d) -> CommitStats { count, p50_nanos, p99_nanos, p999_nanos }` |
| `stalls: (n, nanos)` | `write_stalls`, `write_stall_nanos` |
| `block_cache: (hits, misses)` | `block_cache_hits`, `block_cache_misses` |
| `block_cache_capacity` | `block_cache_capacity_bytes` |
| `block_cache_pinned` | `block_cache_pinned_bytes` |
| `unpin: (passes, flushes)` | `wal_unpin_passes`, `wal_unpin_flushes` |
| `file_growths: (n, nanos)` | `file_growths`, `file_growth_nanos` |
| `file_growths_ahead: (n, nanos)` | `file_growths_ahead`, `file_growth_ahead_nanos` |
| `shard_idle: (parks, wakes)` | `shard_parks`, `shard_wakes` |
| `pinned_threads: (pinned, started)` | `threads_pinned`, `threads_started` |
| `unpinned` | `unpinned_reason` (for people; the wording is not stable) |
| `visibility_waits: (waits, scanned)` | `visibility_waits`, `visibility_waiters_ahead` |
| `visibility_wakes: (passes, woken)` | `visibility_wake_passes`, `visibility_woken` |

Unchanged names: `flushes`, `flush_failures`, `compactions`, `compaction_failures`, `block_cache_pinned_warnings`, `wal_inline_syncs`, `wal_rollover_blocks`, `async_sync_reads`, `shard_busy_nanos`, `commit_parks`, `visibility_contended`, `cross_shard`.

**A bug.** The engine documented `pinned_threads` as `(started, pinned)`, but `Engine::metrics` fills `(pinned, started)`. The engine doc now says `(pinned, started)`, and the public struct's named fields remove the ambiguity. A unit test checks every field's mapping.

## Why

The guide already sends users to `engine_metrics()`: write stalls (errors.md), the block cache's pinned bytes (concepts.md), pinning (getting-started), and now cross-shard refusals (durability.md). blob33's D246/D247 gate rows read `cross_shard`. A hidden hook with tuple fields isn't something users can rely on.

## Callers

- `pigeonhole`:
  - `metrics.rs` (new: the types, the conversion, a mapping test, a doctest);
  - `db.rs` (`engine_metrics`);
  - `lib.rs` (exports);
  - `options.rs` (the `pin_threads` doc);
  - `tests/block_cache_metrics.rs`.
- `pigeonhole-bench`: the Pigeonhole runner and the `coldget` and `coldscan` examples use the new names. The bench's own record fields are unchanged.
- `pigeonhole-engine`: the `Metrics::pinned_threads` doc.
- Docs:
  - the guide's errors, concepts, getting-started and durability pages;
  - the agent reference's `engine_metrics` and `pin_threads` rows;
  - ICR 0015's status;
  - changelog.
