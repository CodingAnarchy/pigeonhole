# 0022: `Table::shard_of(row)`, a routing hint for application-owned mode

**Status:** Approved (coordinator, 2026-10-10; #154, D204).

## Change

`pigeonhole-engine`, additive:

```rust
impl Engine {
    /// The shard owning `row` of `table` in the current tablet map; `None` if no such table.
    pub fn shard_of(&self, table: TableId, row: &[u8]) -> Option<usize>;
}
```

`pigeonhole`, additive:

```rust
impl Table {
    /// The shard owning `row` right now (`Shard::index`); `None` if the table was dropped.
    pub fn shard_of(&self, row: &[u8]) -> Option<usize>;
}
```

## Why

The spec's thread-per-core embedding (application-owned mode) has each core thread drive its shard and write the rows that shard owns inline, with no handoff. An application can only do that if it can ask which shard owns a row. D204's scaling gate drives exactly this: each shard thread commits the rows its shard owns. The routing hint makes that possible through the public API instead of a bench-only hook.

## Semantics

- **A hint, not a lock.** It reads the current tablet map (one lock-free `arc-swap` load and a binary search). A split, merge or move can change the owner at any time after the call.
- **Correctness never depends on it.** A commit from any thread, to rows any shard owns, is routed to the owner and is correct. The hint only decides whether the write runs on the caller's own shard or crosses to another.
- `None` only when the table no longer exists.
- In a C ABI it's a plain function of a table handle and a byte slice returning an integer, so it fits D104.

## Callers

- `pigeonhole-bench`: the scaling gate's inline driver (`runners/inline.rs`, D204) gives each shard thread the writes to rows its shard owns as the phase starts.
