# 0034: tablet packing option (`balance_pack`, `Options::tablet_packing`)

**Status:** Approved (coordinator, 2026-10-11; D217, #478).

## Change

`pigeonhole-engine`, additive field of `EngineOptions` (`#[non_exhaustive]`):

```rust
/// Packing (D217, default on): while writes are light, keep the tablets being written on as
/// few shards as stay about half busy. Only read when `tablet_changes` is on.
pub balance_pack: bool,
```

`pigeonhole`, additive:

```rust
impl Options {
    /// Pack a lightly written store's tablets onto few shards (default on; D217).
    pub fn tablet_packing(self, yes: bool) -> Self;
}
```

`pigeonhole-engine`, a `#[doc(hidden)]` test hook (`test-hooks` feature): `Engine::set_shard_busy(shard, Option<f64>)` makes a shard's balancer measure itself that busy, so packing tests decide on chosen loads under the simulated clock.

## Why

D217: a light writer spread over every shard pays a shard wakeup per commit. On the AX102, time-series-ttl ran at 165K ops/s over 32 shards against 260K on 2 (RocksDB 251K). Packing is on by default (owner decision). The option turns it off: off is exactly the placement before D217.

## Semantics

- **On:** each balancer pass publishes the shard's busy fraction (from `Submitter::busy_nanos`, ICR 0033). Every shard computes the active count `k` from the summed fraction `B`. Shards `k..` hand their written tablets to the active ones. The skew rule and consolidation act only among shards `0..k`. See D217 for the rule and its constants.
- **Never acts** while the store writes fewer than `balance_min_writes` rows per interval over all shards, or while no shard reports busy time: application-owned shards, and the simulated clock.
- **Off, or `tablet_changes` off:** no busy fraction is acted on, and the placement is what it was before D217.

## Callers

- `pigeonhole-engine`: `BalanceConfig::pack` from `balance_pack`; the balancer (`shard/tablets.rs`).
- `pigeonhole`: `Options::tablet_packing` sets `EngineOptions::balance_pack`.
- Tests: `engine/tests/tablets.rs` (packing tests use `set_shard_busy`).
