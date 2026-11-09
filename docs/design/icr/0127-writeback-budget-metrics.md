# 0127: the background write-back budget's metrics, and `File::submit_writeback`

**Status:** Approved. The coordinator assigned the number (2026-11-09) for D319 Part B (#1621). perf2 asked for the metrics in review of D319 revision 1.

## The change
- **New public fields (`pigeonhole`):** `EngineMetrics::writeback_waits` and `EngineMetrics::writeback_peak_bytes`. Both are additive on a `#[non_exhaustive]` struct.
  - `writeback_waits`: times a flush or compaction found the background write-back budget full and ended its slice to wait for room.
  - `writeback_peak_bytes`: the most write-back bytes in flight at once, at most the 4 MiB budget plus about 1 MiB per job writing at that moment.
- **The engine mirror (`pigeonhole-engine`):** `Metrics::writeback_waits` and `Metrics::writeback_peak_bytes`, filled by `Engine::metrics` from `Shared::writeback`.
- **`phdb serve`'s `/metrics`:** `pigeonhole_writeback_waits_total` (counter) and `pigeonhole_writeback_peak_bytes` (gauge).
- **New trait method (`pigeonhole-io`, internal):** `File::submit_writeback(offset, len) -> Completion<()>`, with a default that completes at once. The io_uring backend submits `IORING_OP_SYNC_FILE_RANGE`, and the pread backend runs `sync_file_range` on its pool (Linux). The simulated disk defers it under deferred I/O. Wrappers that don't forward it get the default.
- **New type (`pigeonhole-sst`, internal):** `Writeback`, the budget, with `SstWriter::with_writeback` and `BlobWriter::with_writeback`. `pigeonhole-compaction` gains `JobContext::writeback` (additive on a `#[non_exhaustive]` struct) and `BlobSink::writing_back`.

## Callers
- `Engine::metrics` and `EngineMetrics::from_engine`; `phdb serve`'s metrics rendering.
- The SST and blob writers inside flush (`SstSink`), compaction (`job.rs`'s `Sink`, `BlobSink`) and blob GC.
- `pigeonhole-otel` doesn't export these yet. Adding them to D316's names is a follow-up if wanted.
