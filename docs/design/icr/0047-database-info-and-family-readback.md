# 0047: `info()` on both handles, and families' stored options read back

**Status:** Approved (owner, via the coordinator, 2026-10-20; #692). For the `phdb info`, `dump` and `load` commands.

## Change

In `pigeonhole` (all additions):
- **`Pigeonhole::info(&self) -> Result<DatabaseInfo>`** and **`PigeonholeReader::info(&self) -> Result<DatabaseInfo>`**: what the database holds and how it is stored, read from the current catalog. It does no data I/O.
- **`Table::family_info(&self, &str) -> Option<FamilyInfo>`**: a family's stored options, as of when the handle was opened.
- **`ReadTable::families(&self) -> Vec<&str>`** and **`ReadTable::family_info(&self, &str) -> Option<FamilyInfo>`**: a reader had neither. `Table::families` already existed.
- New types, each the public crate's own `#[non_exhaustive]` type:

| Type | Fields |
|---|---|
| `DatabaseInfo` | `db_id: [u8; 16]`, `format_version: u32`, `reader: bool`, `shards: usize`, `file_bytes: u64`, `free_bytes: Option<u64>` (`None` on a reader), `tables: Vec<TableInfo>` (by name) |
| `TableInfo` | `name`, `tablets: usize`, `families: Vec<FamilyInfo>` (creation order), `storage: Vec<FamilyStorage>` (same order) |
| `FamilyInfo` | `name`, `family: Family` (the same options as a builder), `counter`, `max_versions`, `ttl: Option<Duration>`, `bloom_bits`, `blob_threshold`, `compression: Compression`, `block_size`, `merge_operator: Option<String>` (`None` for a counter family), `cache_priority: Priority`, `compaction: Compaction` |
| `Compression` (enum) | `None`, `Lz4`, `Zstd(i8)` |
| `FamilyStorage` | `family` (name), `levels: Vec<LevelInfo>`, `blob_files`, `blob_bytes`, `blob_live_bytes`; methods `ssts()` and `sst_bytes()` |
| `LevelInfo` | `level: u8`, `ssts`, `bytes`, `entries` |

`FamilyInfo::family` is what `phdb load` passes to `TableBuilder::family`: it defines an identical family, in this database or another, with no field-by-field copy that could drift as options are added.

In `pigeonhole-engine` (additions): `Engine::info(&self) -> Result<DbInfo>`, with `DbInfo`, `TableStorage`, `FamilyStorage` and `LevelStorage` (`#[non_exhaustive]`). It reads `Inner::catalog()`, which serves both roles: a reader's catalog follows the manifest version the writer last published.

**Semantics:**
- **SSTs** are counted once per family and level even while several tablets share one after a split.
- **`free_bytes`** is the file's length less the allocated and retired extents (the 64 KiB header included). A reader does not track allocations, so it reports `None`.
- **`shards`** is the instance's own count. Options aren't stored in the file, so a reader reports the writer's count and a writer reports what it opened with. That is why `phdb info` prints shards only in reader mode.
- **`format_version`**: a writable open upgrades older files (ICR 0046), so an open database reports `FormatVersion::CURRENT`.

## Why

#692: `phdb info` needs the format version, sizes, tablets, per-level SSTs and the families' options. `phdb dump` and `load` need the options to recreate a table elsewhere. The public API had family names only, on writers only. The engine's `shard_stats` and the test hooks (`sst_levels`, `blob_files`, `inspect_manifest`) are `#[doc(hidden)]` or behind `test-hooks` and aren't API. Own public structs, rather than re-exports, keep the engine's ids and tuples out of the public crate (ICR 0044's pattern).

## Callers

- `pigeonhole`:
  - `info.rs` (new: the types, the conversions, tests);
  - `db.rs`, `table.rs` and `options.rs` (`Family::from_engine`, crate-private);
  - `lib.rs` (exports);
  - `tests/api.rs` (the reader test reads `families`, `family_info` and `info`).
- `pigeonhole-engine`: `engine/info.rs` (new), `engine.rs` and `lib.rs` (exports).
- `pigeonhole-cli`: `phdb info`, and later `dump` and `load`.
- Docs: the agent reference, the CLI guide page, the changelog.
