# 0128: `Options::caller_runs` and `EngineOptions::caller_runs` (caller-runs, D321)

**Status:** Approved. The coordinator assigned the number (2026-11-09). The design is #1176's note (rev 3.1), the prototype is #1502, and the owner decided on 2026-11-09 that it ships as an opt-in (D321).

## The change
- **New public names, additive:**
  - `pigeonhole::Options::caller_runs(self, on: bool) -> Self`. Default `false`.
  - `pigeonhole_engine::EngineOptions::caller_runs: bool`, now documented. It was a `#[doc(hidden)]` prototype field; the default is still `false`.
- **Behaviour (Added):** with it on, a thread waiting on its own single-shard commit at `Durability::Buffered` or `Durability::None` (no predicate, transaction validation, retry or `Returning`) may run that shard's turn itself, when no other thread is running the shard and the shard's driver hasn't asked for it.
  - **The turn:** up to 8 queued messages the shard allows, foreground only.
  - **Unchanged:** cross-shard commits, `GroupSync`, flushes and compactions stay on the shard's thread.
  - **No effect:** with application-owned shards, or with the option off. Nothing a caller sees changes except latency: every result, error and ordering guarantee is the same.
- **Internal metrics (unchanged names, not public API):** `caller_turns` and `caller_backstop_wakes`. The second should always be 0; a nonzero value is a lost wake the drivers' 100 ms park backstop recovered.
- **Removed before release, never public:**
  - the `#[doc(hidden)]` `Options::caller_spin_extend` and `EngineOptions::caller_spin_extend_nanos` (#1176's C1 evaluation arm);
  - the `caller-stats` cargo features of `pigeonhole`, `pigeonhole-engine` and the bench;
  - the internal metrics `caller_resolved_{own,other,driver}`;
  - `pigeonhole_runtime::in_caller_turn`.

## Why opt-in, not default
perf287's full-scale gate A/B (D321) found two things. The option cuts buffered put p99.9 where the shard's thread couldn't get its CPU: sparse-wide 473 → 94 µs, and large-value throughput +13%. It also costs hot, short commits: counters −18%, ycsb-a −10%, short-op put p99 +20–40%, and a skewed multi-shard load outside 1.5× of RocksDB. The option's documentation and the guide (`docs/guide/performance.md`) say which writers it's for.

## Off costs nothing
- **The drivers:** with the option off, the runtime's drivers hold their shards as plain `ShardCore`s (a type parameter chosen at start), so they run the loop as before caller-runs.
- **The commit path:** a blocking commit's spinning wait tests the option once, and the caller path is out of line.
- **The submit path and the shard's drain:** they're main's. The caller entries, the work hint and the turn count live in the run token's cell, reached through the engine's `Shared::callers`, which is set only with the option on.
- **Checked by:** x86 callgrind on #1502 against the D193/D195 ceilings.

## Callers changed
None: the option is new and off by default.
