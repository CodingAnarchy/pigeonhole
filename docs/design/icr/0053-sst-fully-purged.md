# 0053: the SST property `fully_purged`

**Status:** Approved (owner, via the coordinator, 2026-10-21; #820, D268). Additive, within format version 3 (unreleased).

## Change

In `pigeonhole-format`:
- `sst::Properties::fully_purged: Option<u32>`. `Some(max_versions)` when a bottommost compaction wrote the SST with nothing holding its purge back (no live snapshot, nothing above its inputs, and for a counter family every other source listed), under a family keeping `max_versions` (0: every version). `None` means unknown: any other writer (flushes, ingest, backups), or an SST written before the property.
- The properties block gains a purge word after `merge_operator`: `flags` u32 (bit 0: fully purged; the other bits are reserved and ignored on read) and `max_versions` u32. A block that ends before them, which is format 3 as written before this change, decodes as `None`. FORMAT.md §5.2.

In `pigeonhole-sst`:
- `SstWriterOptions::fully_purged: Option<u32>`, default `None` (`for_family`), recorded in the properties.

In `pigeonhole-compaction`:
- `CompactionJob::new` sets it on its outputs' writer options when the job's `GcPolicy` is bottommost, has no snapshot and `min_ts_above == u64::MAX`, and, for a counter family, `other_sources == Some(empty)`.

In `pigeonhole-engine` (internal):
- `compact::may_hold_purgeable` reads the property beside `rows`, through the open-SST cache, only when the manifest's metadata can't settle it. An SST carrying the property under a policy that still holds (a counter family, or the family keeping every version now, or at least as many as then) is not a candidate for its delete markers or its versions. TTL expiry is still checked.
- `compact::plan_full` applies the check to every SST of a last-level run, not only a lone SST (#820).

## Why

A full `compact()` promises that nothing purgeable remains (#805, D268). The format-free checks of #814 can't tell a row of several columns from a column with extra versions: a family keeping `max_versions` with several columns per row always has more entries than rows. Extending the rewrite to last-level runs (#820) would then rewrite such a family's whole last level on every `compact()` call (14–19 ms per MB measured on macOS). The property records that a rewrite would drop nothing, so the rewrite happens once.

## Callers

- `pigeonhole-format`: `sst.rs`; `tests/golden.rs` (the regenerated `sst_properties.bin`, plus `sst_properties_before_d268.bin`, the block as written before the change, decoding as `None`); `tests/roundtrip.rs`.
- `pigeonhole-sst`: `lib.rs` and `writer.rs`.
- `pigeonhole-compaction`: `job.rs`; `tests/compaction.rs` (`outputs_are_marked_fully_purged_only_when_nothing_held_the_purge_back`).
- `pigeonhole-engine`: `compact.rs` (`PurgeProps`, `may_hold_purgeable`, `plan_full`) and `shard.rs` (the `plan_full` call); `tests/full_compaction_purge.rs`.
- Docs: FORMAT.md §5.2, D268, the changelog.
