# 0010: hidden `Pigeonhole::shard_stats` bench hook

**Status:** Approved (coordinator task for #51, 2026-10-08: "add a minimal doc(hidden) or
bench-only accessor").

## Change

Additive and hidden from rustdoc. Engine:

```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShardStats {
    pub commits: u64,
    pub tablets: u64,
    pub splits: u64,
    pub merges: u64,
    pub moves: u64,
}

impl Engine {
    #[doc(hidden)]
    pub fn shard_stats(&self) -> Vec<ShardStats>;
}
```

Public crate:

```rust
#[doc(hidden)]
pub use pigeonhole_engine::ShardStats;

impl Pigeonhole {
    #[doc(hidden)]
    pub fn shard_stats(&self) -> Vec<ShardStats>;
}
```

One entry per shard, in shard order. `commits`, `splits`, `merges` and `moves` are cumulative
since open and read from the existing per-shard counters (`ShardMetrics`). `tablets` is the
number of tablets the shard owns in the current view. Nothing new is counted on the write path.

## Why

Issue #51's first criterion is that the scaling gate's writes demonstrably spread over the
shards. `Engine::metrics` sums over shards, and the public crate exposes no metrics at all, so
nothing reachable from `phdb-bench` showed where commits and tablets went. The struct holds
only integers, so a future C ABI can mirror it as a plain struct.

## Callers

- `crates/bench/src/runners/pigeonhole.rs`: `Runner::shard_shares`, read before and after
  the measured phase; the difference is reported per shard (`RunDetail::shards`, the
  markdown "Shard | Commits | Share" table).
- `cargo-semver-checks` ignores `#[doc(hidden)]` items.
