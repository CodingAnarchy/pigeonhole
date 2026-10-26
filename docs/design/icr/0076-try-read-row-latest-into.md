# 0076: `Engine::try_read_row_latest_into`, a latest row read that never blocks

**Status:** Approved (coordinator, 2026-10-26; #709, #971). Additive: a new public method on `pigeonhole_engine::Engine`. Nothing existing changes signature.

## Change

**`pigeonhole-engine`:** a new method on `Engine`:

```rust
pub fn try_read_row_latest_into(
    &self,
    table: TableId,
    row: &[u8],
    families: &[FamilyId],
    spec: &ReadSpec,
    sink: &mut impl RowSink,
) -> Option<Result<bool>>
```

- It is `read_row_latest_into` in cache-only mode, in a writer process: under the view's `arc-swap` guard and through the row cache, as the sync read reads.
- It returns `Some` with the read's result (`Ok(true)` when the row has a visible cell, or an error).
- It returns `None` when the read would need a block the cache does not hold, in a reader process, or when the visible seqno kept moving. `sink` may then hold some of the row's cells; the caller starts `read_row_latest_async` with an empty one.
- A value too large to cache is read synchronously and counted in `async_sync_reads`, as inside the row future.

`GetFuture` gains the same attempt internally (`Engine::get_latest_async` tries it at the call). That's no API change: its fields are private.

## Why

D280's async bar (#709): async reads at 16 clients ran at about 0.4× the blocking ones (gate on main aea4d254), flat across 4 and 16 in flight.

Each row future cloned the engine's `Arc<Inner>`, the current `Arc<View>` (`Guard::into_inner`) and the row's `Arc<TableInfo>` twice. Every client thread writes those reference counts, so their cache lines bounce between cores. One client loses nothing; 16 clients on two CCDs lose most of their throughput.

Reading at the call lets a hit skip the future's state entirely. The future can't do it itself: it needs an empty copy of the caller's sink for its retries, and making one is the `TableInfo` clone. So the caller, which can build a fresh `RowBuf`, makes the attempt, and builds the future only on a miss.

## Callers

- `pigeonhole::RowRead::read_async` (latest reads, no snapshot), the only caller in the workspace.
- `pigeonhole-cli` and the benches call it only through `read_async`.
- `cargo semver-checks` sees an added method (minor).
