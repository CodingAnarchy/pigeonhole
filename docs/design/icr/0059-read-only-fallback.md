# 0059: `Options::read_only_fallback`, a no-write read-only open, and the file's features in `info()`

**Status:** Approved (owner, via the coordinator, 2026-10-21; #496, D269 D3, PR 4; defaults D-a to D-e). Additive, apart from the engine option rename (pre-1.0) and the deprecated alias.

## Change

**`pigeonhole`:**
- `Options::read_only_fallback(bool)` (default off). A writer open of a database this build can't write opens read-only instead of failing. That covers two cases:
  - an unknown `ro_compat` feature bit, which otherwise fails with `UnsupportedFeature`;
  - a family naming a merge operator this process hasn't registered, which otherwise fails with `UnknownMergeOperator`.
- `Options::allow_unregistered_merge_operators(bool)` is `#[deprecated]`; it sets `read_only_fallback`.
- `DatabaseInfo::features: FileFeatures` and `DatabaseInfo::read_only: bool` are new fields. The struct is `#[non_exhaustive]`.
- A new public struct, `FileFeatures { compat, ro_compat, incompat }` (u64 each, `#[non_exhaustive]`). The pigeonhole crate owns it (ICR 0044's rule), and the format crate's type doesn't appear in the API.
- **`DatabaseInfo::format_version` changes meaning:** it's the current superblock's version, not always this build's. A writable open upgrades, so it still reports the current version; a read-only open reports the file's own.
- A read-only handle also refuses `flush`, `compact` and `shrink` with `ReadOnly`. The old unregistered-merge read-only handle allowed them.
- A family naming an unregistered merge operator is refused at creation whatever the options. `allow_unregistered_merge_operators` used to let one be created.

**`pigeonhole-engine`:**
- `EngineOptions::read_only_fallback` replaces `allow_unregistered_merge`.
- New fields: `DbInfo::features: pigeonhole_format::superblock::Features` and `DbInfo::read_only`. `DbInfo::format_version` changes as above.
- `Error::ReadOnlyOpen(String)`: the read-only open can't go on without writing. The public code is `ReadOnly`, and the message says why.

**`pigeonhole-pager`:**
- `Pager::features`, `Pager::set_features` (both pages, before it returns) and `Pager::format_version`.
- `OpenedPager::features` and `OpenedPager::into_read_only`.
- If the two valid pages' feature sets differ, `finish` rewrites both, as for an older version.

**`pigeonhole-wal`:** `Recovery::open_read_only`: replay with the file opened read-only and no syncs. `into_stream` refuses it.

**phdb:** `info` gains `read_only` and `features`. The writer open uses `read_only_fallback(true)`.

## What a read-only open does

It writes nothing to the database's files: the main file and every WAL file are byte for byte as before, through the open, the reads and the close.
- **The pager opens read-only.** There's no length sync and no format upgrade, so an older file stays readable by its own build (perf287's nit 3 on #851). Allocations and root commits are refused.
- **The WAL is replayed into memtables** from read-only files. No stream is created and no new segment is written, and the WAL files stay for the next writable open to replay.
  - Any shard count works (D-a): replay routes each record to the shard owning its rows, and no stream is kept.
  - A replay that doesn't fit `memtable_budget` fails with `ReadOnlyOpen` (D-b), because it would have to flush to SSTs.
- **The shards do nothing that writes:** no flush, compaction, cleanup, blob GC, FIFO expiry, checkpoint commit or tablet change (D-e: the unregistered-merge open used to compact the other families).
- **Close** doesn't flush, checkpoint, reset checkpoints, mark a clean close or remove the WAL files.
- It still takes the writer lock and presence, so no other writer can change the file underneath it.
- **Unsynced records are visible.** Replay reads the WAL without the writable path's `sync_all`, so it sees what the OS holds, including records whose write a crash left unsynced. A later power loss can still remove them, so a read-only open can show commits that the next writable open, after a power loss, won't have. That's D227's "visible once written to the OS", as for a reader process.
- **What `close()` returns.** On such a handle, `Ok(())` means the file is exactly as the open found it. It doesn't mean the data was flushed and the WAL removed, as it does for a writable close: the replayed records are still only in the WAL, for the next writable open to replay. An error means only that the close itself failed, for example a shard died.

## Why

- **D269 D3:** an unknown `ro_compat` bit means "you may read, but not write", and an application should know it can't write, so it's opt-in, not silent.
- **A real no-write open:** if a read-only fallback still flushed, compacted or wrote WAL segments, it would rewrite structures a `ro_compat` feature relies on, which defeats the bit. It would also upgrade an older file's format, locking out the build that wrote it.
- **One option for both cases,** because both mean "this build can't write the file".

## Callers

- `pigeonhole-pager`: `lib.rs`; `tests/format_upgrade.rs`.
- `pigeonhole-wal`: `recovery.rs`.
- `pigeonhole-engine`:
  - `options.rs`, `error.rs`;
  - `engine.rs` (open, maintenance refusals, `check_merge_operator`);
  - `engine/info.rs`;
  - `shard.rs` (the `no_writes` gates, close); `shard/tablets.rs` (the balancer);
  - `tests/read_only_fallback.rs`.
- `pigeonhole`: `options.rs`, `info.rs`, `error.rs`, `lib.rs`; `tests/api.rs`.
- `pigeonhole-cli`: `open.rs`, `cmd/info.rs`, and the info snapshots.
- Docs: the agent reference, cli.md, errors.md, data-modeling.md, the changelog, and FORMAT §8.2.
