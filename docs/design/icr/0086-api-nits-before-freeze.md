# 0086: Pre-freeze API fixes: `Family::compression`, one `column_limit`, `Row::entry` returns `CellEntry`, no writer `async_sync_reads()`, no `repr(C)`, and a hidden cli library

**Status:** Approved (owner via the coordinator, 2026-10-28; #695, API audit 2c, 2f and 3's cli target, c6480352190, and options inventory R4, c6480419472). Breaking before the 1.0 freeze. No behaviour change.

## Change
| Before | After | Why |
|---|---|---|
| `Family::lz4()`, `Family::zstd(level)`, `Family::uncompressed()` | `Family::compression(Compression)`, with `Compression::{Lz4, Zstd(i8), None}` (the enum `FamilyInfo::compression` already returns) | One setting with one spelling; it reads back as written, and a new codec is a new variant, not a new method (R4). |
| `RowRead::column_limit(n)` and `Scan::columns_per_row(n)` | `column_limit(n)` on both | Both cap columns *per family*, which `columns_per_row` misnamed. The audit proposed `columns_per_row`; on a closer read the limit is per family, so the accurate existing name won. `phdb`'s `columns-per-row` argument is unchanged. |
| `Row::entry(i) -> Option<(&str, &[u8], &Cell)>` | `Option<CellEntry<'_>>` | The same as `RowRef::entry`. |
| `Pigeonhole::async_sync_reads()` | removed; read `engine_metrics().async_sync_reads` | It duplicated the metric. `PigeonholeReader::async_sync_reads()` stays: a reader process has no `engine_metrics()`. |
| `#[repr(C)]` on `Failure` and `Recovery` | none | A leftover from the C ABI, which moved past 1.0 (D250); it implied a layout promise. `#[repr(u8)]`/`#[repr(u32)]` with fixed discriminants stay on `Durability`, `FailureCause` and `ErrorCode`, whose numbers are promised. |
| `CommitTicket::seqno(&mut self)` | unchanged, documented | It polls the commit, like `try_result`. |
| `pigeonhole_cli::{run, ExitStatus}` | `#[doc(hidden)]` | The library exists for in-process tests; `phdb`'s promise covers the binary (concepts.md). |

## Callers changed
- **The cli:** `schema.rs` (`compression`), `shell/mod.rs` (`column_limit`), and the cli tests.
- **pigeonhole tests:** api, corpus, model, row_cache, nonblocking and app_owned_uring.
- **Examples:** `adjacency_scan.rs` and the bench's `coldscan.rs`.
- **Docs:** the agent reference, concepts, getting started, data modeling, scans and filters, async.md and spec.md.
