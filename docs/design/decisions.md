# Decisions log

Project-level decisions that refine or deviate from [spec.md](spec.md) and [task-briefs.md](task-briefs.md). Newest last. An entry here wins over those files. Agents: when the spec is silent or contradictory, add a question under **Open questions** instead of guessing; the coordinator turns answers into numbered decisions.

## D1 — `pigeonhole-sim` does not depend on `pigeonhole`
The spec lists `pigeonhole` as a dependency of `sim` ("drives the public API from above"). That creates a cycle the moment `engine` or `pigeonhole` use `sim` in tests. Instead `sim` depends only on `io` and `format`; the full-stack simulation suites live in `crates/pigeonhole/tests/` and `crates/engine/tests/`, which take `pigeonhole-sim` as a dev-dependency. Same coverage, strictly downward graph.

## D2 — the simulated VFS lives in `pigeonhole-io`
Per the io brief, `SimVfs` (fault injection, deterministic from a seed) is an `io` backend at `pigeonhole_io::sim`. `pigeonhole-sim` builds the scheduler, crash points and reference model on top of it.

## D3 — writer lock is a byte-range lock on the lock page
"Multi-process readers" mentions `flock`; "Files and locks" specifies byte-range locks on a reserved lock page (OFD on Linux, `fcntl` with a per-process registry on macOS/BSD, `LockFileEx` on Windows). The more specific section wins.

## D4 — interface-freeze gate
The spec gates the interface freeze on owner review. The owner directed autonomous progress, so the coordinator reviews and approves interfaces, records the approval here, and the owner may revisit at any time through an interface-change request.

## D5 — reference hardware
No enterprise-NVMe Linux box with power-loss protection is attached to this project yet. Benchmarks run on available hardware (developer macOS arm64 and GitHub Linux runners), are reported in every run, and are labeled as non-reference. Performance gates are evaluated against those numbers until reference hardware is available.

## D6 — dependency policy
Allowed licenses: MIT, Apache-2.0, BSD-2/3-Clause, ISC, Zlib, Unicode-3.0, CC0-1.0 (enforced by `deny.toml`). Engine crates keep dependencies minimal; each new dependency gets a one-line justification in the PR description.

## D7 — manifest is a copy-on-write chain of snapshot and delta blocks (proposed by interface freeze)
The spec calls the manifest "a small copy-on-write tree"; the format brief calls for "manifest edit records". The simplest design that satisfies both: each manifest commit writes one immutable block of edit records into a fresh extent; a *snapshot* block holds the whole state, a *delta* block one commit's edits; each block links to its predecessor and the superblock points to the newest. A new snapshot is written when the chain exceeds 64 deltas or the deltas outgrow the snapshot. Nothing is overwritten (the superblock flip stays the only in-place write), a commit costs one small block instead of rewriting the manifest, open reads one bounded chain, and reader processes catch up by reading only the new deltas. A B-tree of manifest pages would add node splitting and page-level CoW for no gain at expected manifest sizes. Layout: FORMAT §9.

## D8 — the free-space bitmap is not persisted (proposed by interface freeze)
The pager keeps the bitmap in memory and rebuilds it at open from the live extents the manifest names (the manifest chain, SSTs, blob extents). The engine reads the manifest anyway at open, so this costs nothing extra, a crash can never leak an extent, and no persisted bitmap can disagree with the manifest.

## D9 — family-in-row deletes use a marker key (proposed by interface freeze)
The key layout has no slot for row- or family-level tombstones. A family-in-row delete is the key `[row][00 01][00 00][!ts][!seqno][FamilyDelete]`: `00 00` never appears in an escaped string and sorts before every qualifier, so a reader sees a row's markers before its cells, and the marker deletes every cell of the row in that family with timestamp `<=` its own. Column deletes are `ColumnDelete` at the column's key with the same `<=` rule; cell deletes are `CellDelete` at exactly one version. Filters add a "marker column" key so a column-filter miss never hides a marker (FORMAT §6).

## D10 — a whole-row delete is one family marker per family (proposed by interface freeze)
Each family is its own tree, so there is no single place for a row tombstone. `delete_row` writes a `FamilyDelete` marker into every family of the table in the same atomic commit. Families added later cannot hold older data for that row, so nothing is missed.

## D11 — timestamps are microseconds since the Unix epoch (proposed by interface freeze)
The spec defines timestamps as u64 with a hybrid-logical-clock default but gives no unit; TTL needs one. The default timestamp is `max(now_micros, last_assigned + 1)` per shard. User-supplied timestamps are taken as microseconds for TTL purposes.

## D12 — `WriteBatch::commit()` uses the writer default (proposed by interface freeze)
The API-surface example shows `wb.commit(Durability::GroupSync)`, the Durability section shows `wb.commit()` plus `wb.commit_with(d)`. The more specific Durability section wins: `commit()` uses the writer default, `commit_with(d)` overrides it.

## D13 — family ids are unique per database; SSTs belong to a (tablet, family) (proposed by interface freeze)
A `FamilyId` is never shared across tables, so `(TabletId, FamilyId)` names one LSM tree with one memtable set, one level set and one flushed seqno. After a split both children may reference the parent's SSTs until compaction rewrites them; an extent is retired only when no tablet references it.

## D14 — extra downward dependencies (proposed by interface freeze)
`memtable` depends on `io` (the brief lists only `format`) because its arena is an `io::SharedRegion`; without it, `engine` (which forbids `unsafe`) could not hand shared memory to the memtable. `compaction` depends on `cache` and `io` (the brief lists `sst`, `pager`, `format`) because opening input SSTs takes a `BlockCache` and a `FileRef`. Both point down, so the layer rule holds.

## D15 — shared vocabulary types live in `format` (proposed by interface freeze)
Ids, `Seqno`, `Timestamp`, `Lsn`, `Durability` and the `Cursor` trait are used by nearly every crate; `format` is the only crate all of them depend on, so they live there rather than being duplicated.

## D16 — maximum value length is 2^32 - 1 bytes (proposed by interface freeze)
The spec says "at most 4 GiB"; the blob pointer stores the length as a `u32`, so the limit is one byte short of 4 GiB. Until blob separation lands (Phase 2) a single cell must also fit in its shard's memtable arena; larger cells fail with `ValueTooLarge`.

## D17 — the `async` feature is off by default until Phase 3 (proposed by interface freeze)
The spec makes `async` a default-on feature. In Phase 1 it gates only an empty placeholder module (`pigeonhole::nonblocking`), so it is declared but not default; it becomes default-on when the async API is implemented.

## D18 — filters are cache-line-blocked bloom filters in Phase 1 (proposed by interface freeze)
The spec allows "a ribbon (or blocked bloom) filter". Blocked bloom is simpler and fast enough for Phase 1; the filter block's kind byte leaves room for ribbon later without a format break.

## D19 — commits become visible after their group's WAL write (proposed by interface freeze)
A shard publishes a group's seqnos only after the group's WAL write meets the strongest level any member requested, so a reader never sees a `GroupSync` commit that a crash could still lose. Weaker members of the same group ride along (the stream is ordered). A reader can still observe a `Buffered` or `None` commit that a later power loss removes; that is inherent in those levels.

## D20 — a changed shard count flushes recovered data before dropping streams (proposed by interface freeze)
The spec replays every WAL stream at open regardless of shard count. If streams exist beyond the new shard count, the engine flushes the memtables recovered from them and checkpoints before removing those stream files. Open stays fast in the common case (same shard count).

## D21 — lock page byte assignments (proposed by interface freeze)
Refines D3. On page 2: offset 8192 is the writer byte, 8193 the presence byte, 8194 an shm-init byte held exclusive while a process creates, validates or rebuilds the shared-memory region (so two processes opening at once never both build it). Locks never block; callers retry.

## D22 — filter pushdown semantics (proposed by interface freeze)
"Filters run inside the block decoder" is split by correctness: qualifier selection and time ranges on puts and merge operands run per entry inside `SstIter`; delete entries and markers always pass (hiding them would resurrect older versions). Version count, columns per row and value predicates need snapshot visibility, so the resolver applies them, still before any cell is materialized. A value predicate tests the newest visible value of a column. The sst acceptance test (pushdown equals filter-after) is defined over these semantics.

## D23 — one shared-memory view record carries the tablet map and memtables (proposed by interface freeze)
The spec lists "the current tablet map" and published views separately. A view already includes the tablet map, so the region holds one double-buffered view record (tablet map, memtable roots, manifest version) published by a single pointer swap; a reader can never pair a tablet map with the wrong memtables.

## D24 — WAL checkpoints never strand a prepared commit (proposed by interface freeze)
A participant's PREPARE is applied at recovery only if the coordinator's stream still holds the COMMIT. So a stream's checkpoint may not pass a COMMIT record until every participant's share of that commit is flushed (its `SetFlushed` covers the seqno). The engine computes checkpoints with this rule.

## Open questions
_None yet._
