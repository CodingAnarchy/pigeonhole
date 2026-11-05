# 0122: `read_within_async`, an async byte-budgeted read (#1286)

**Status:** Approved (number from the coordinator, 2026-11-05; the driver was agreed with simp724 on #1286, issuecomment-6658738349).

## The change
- **pigeonhole** (semver, additive, the `async` feature):
  - `Scan::read_within_async(self, bytes: u64) -> Result<ScanPage>` (`pub async fn`);
  - `KeysRead::read_within_async(self, bytes: u64) -> Result<KeysPage>`.
  
  They return the same page as `read_within` (D308), with the same resume rules, `limit` and table check.
- **pigeonhole-engine** (additive):
  - `Engine::reduce_async(&self, snapshot, table, spec: &ReduceSpec, visitor: V, reducer: R) -> Result<PendingReduce<V, R>>`;
  - `PendingReduce<V, R>: Future<Output = Result<R>>`;
  - `REDUCE_POLL_CELLS` (4,096) and `REDUCE_POLL_BYTES` (1 MiB).
  
  It's a bounded read over one range or a key set, driven on the polling task in one pass in cell order. That's exactly `reduce_into`'s merged result (D296 rev 1.2).

## Semantics
- **The value predicate is tested by the driver, so its misses count toward a poll's bound** (#1237's rule). It tests each column's newest version (D22), and the column's older versions follow it. The column limit leaves the read with the predicate, because the resolver counts only passing columns (simp724's A1).
- **Separated values arrive already read** from their blob files by the cursor, inside its counted region.
- **Rows are positioned from the block cache,** and their missing blocks fetched asynchronously, as `Engine::scan_async` and the multi-get cursor do. A row's cells past its first block that miss the cache, and separated values, are read synchronously and counted in `Metrics::async_sync_reads`.
- **A resume inside a row** uses the new `ScanCursor` step (`resume_in_row`, crate-internal), mirroring the multi-get cursor's.

## Why
D308's `read_within` had no async form (#1286, deferred from 1.1.0). `nearest_async` and `traverse_async` each have their own driver. This one is generic over the visitor and the reducer, so the byte budget's two reducer types (with and without a row limit) share it.

## Callers changed
None: additive. `pigeonhole::Scan::read_within_async` and `KeysRead::read_within_async` call `Engine::reduce_async`, and `read_within` is unchanged.
