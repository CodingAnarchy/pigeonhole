# 0009: `pigeonhole::Options::tablet_changes` and the hidden `tablet_balance` hook

**Status:** Approved (owner task for #38 and #51, 2026-10-07).

## Change

Two additive builder methods on `Options`:

```rust
impl Options {
    pub fn tablet_changes(self, yes: bool) -> Self;

    #[doc(hidden)]
    pub fn tablet_balance(self, interval: Duration, min_writes: u64, split_bytes: u64) -> Self;
}
```

`tablet_changes` sets `EngineOptions::tablet_changes` (D129): tablets split, merge and move
between shards, so one table's writes spread over every shard. Default off.

`tablet_balance` sets `EngineOptions::{balance_interval_nanos, balance_min_writes,
tablet_split_bytes}`. Unset keeps the engine defaults (100 ms, 2,000 rows, 256 MiB). Only
read when `tablet_changes` is on.

## Why

The scaling gate (#51) runs through the public API (`phdb-bench scaling`), and nothing there
could turn tablet changes on. The public model suite needs the switch to check tablet changes
end to end, and the hook to make them happen within a short simulated run (the engine's own
suites use the same tiny values, `Config::balance_fast`). Both take only integers and a
`bool`, so they map onto a future C options struct directly.

## Callers

- `crates/bench/src/runners/pigeonhole.rs` (`PigeonholeRunner::tablet_changes`, the
  `--tablet-changes` flag).
- `crates/pigeonhole/tests/model.rs` (`PIGEONHOLE_TABLET_CHANGES`, tablet-change tests).
- The balancer hook is hidden from rustdoc; `cargo-semver-checks` ignores `#[doc(hidden)]`
  items.
