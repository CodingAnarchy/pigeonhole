# 0048: `pigeonhole::check`, an offline integrity check with findings and a region map

**Status:** Approved (owner, via the coordinator, 2026-10-20; #692). For `phdb check`. Reviewed by perf287, because it reads the on-disk format.

*Amended by ICR 0075: `CheckOptions` has no hidden `vfs`; `check` always reads the real file system.*

## Change

In `pigeonhole` (additions):
- **`pub fn check(path: impl AsRef<Path>, options: CheckOptions) -> Result<CheckReport>`**, a free function: it needs no open database.
- **`CheckOptions`** (a builder): `deep(bool)` and `allow_fuse(bool)`. (A hidden `vfs` for the simulation was removed by ICR 0075.)
- **`CheckReport`**: `findings: Vec<Finding>`, `regions: Vec<Region>`, `wal: Vec<WalFile>`, `counts: CheckCounts`, `file_bytes`, `clean_shutdown`, and `is_ok()`, which is true when no finding is `Corruption`.
- **`Finding`**: `structure: Structure`, `severity: Severity`, `object: Option<u64>`, `offset: Option<u64>`, `message`.
- **`Structure`**: `Superblock`, `Manifest`, `Allocation`, `Sst`, `Blob`, `Wal`. **`Severity`**: `Corruption`, `Warning`, `Info`.
- **`Region`** `{ kind: RegionKind, offset, len }`. `RegionKind` is `Superblock { slot }`, `ManifestSnapshot`, `ManifestLog`, `Sst { sst, table, family, level, used }` or `Blob { blob_file, index, family }`.
- **`WalFile`** `{ stream, path, len, segment_size, segments, records, checkpoint, end, end_slot }`.
- **`CheckCounts`**: superblocks, tables, SSTs, SST entries, blob files, records and pointers, WAL files and records.
- The structs and enums are the public crate's own and `#[non_exhaustive]`, converted from the engine's (ICR 0044's pattern).

In `pigeonhole-engine` (additions): `Engine::check(vfs, path, &CheckOptions) -> Result<CheckReport>` with the engine's own copies of these types (`Extent`, `ExtentKind` for the regions), in a new module, `check.rs`. `check_local` becomes `pub(crate)`. No published crate below the engine changes: everything the check reads with is already public there.

## Semantics

**Offline, and changes nothing:**
- It opens a read-write handle only to take the writer lock, like a writer's open. A running writer makes it fail with `WriterLocked`.
- It reads through a separate read-only handle. It never builds shared memory, never replays the WAL, never calls `OpenedPager::finish`, and writes nothing.
- An error means the check could not run (missing, locked, unreadable). Damage is a finding.
- **A file a newer build wrote** fails with `UnsupportedFormat`. It is not reported as two damaged superblock slots.
- **It needs write access** to the file, for the lock handle, so a copy on read-only media can't be checked in place.

**What it checks:**

| Structure | Checks |
|---|---|
| Superblock | Each slot decoded on its own. `Superblock::choose`'s fallback would hide a damaged slot. An all-zero slot B is a new file's, never written. A damaged slot is `Corruption` if the newest valid root has the clean flag, else `Warning`, because a crash mid-root-commit tears the slot being written. The clean flag means "no root commit since a clean close", not "nothing written since". A clean reopen commits no root, so a session after one can crash during its first root commit while the clean root is still the newest. The rule therefore assumes 512-byte sector atomicity: the 128-byte superblock sits inside one sector, so a sector-atomic tear leaves the old or the new superblock, never an invalid one. On a device that tears inside a sector, that crash could damage the other slot under a clean root, and it would be reported as `Corruption`. The root's `file_pages` must fit the file's length (truncation). |
| Manifest | The snapshot and delta blocks the root names, through `manifest::load`, the same decode as open. A failure is one finding, and the checks that need the catalog are skipped. |
| Allocation | Every extent the root and the manifest reference: a valid extent (past the reserved header, a power of two from 64 KiB to 64 MiB, aligned to its size), inside the file, and overlapping no other. SSTs shared by tablets after a split are counted once. |
| SSTs | Opened with a disabled block cache, so every block is read from disk and its checksum verified. A cached block skips verification. The walk checks: footer, top index, filters and properties at open; then every index partition and data block; keys strictly increasing within the recorded key range; the properties agreeing with the manifest's `SstMeta` (table, family, key range, seqnos, timestamps, entries, deletes); the entry count; the last key; every blob pointer naming a blob file of the same family; and the SST's recorded blob references (#240) equal to the pointers it holds. An SST with pointers but no recorded references is a `Warning`: no release writes one, and the engine treats it as pointing anywhere. |
| Blob files | Each extent's header (file id, position, one size class), then every record from logical offset 0 to `total_bytes`, with its checksum verified. |
| WAL sidecars | Each slot's header: another database's segment, or a wrong stream or segment size, is corruption. Then the log is read from the stream's checkpoint and chained as `wal::Recovery` replays it. Recovery isn't reused because it opens the stream writable and syncs it. |

**How a WAL stop is classified** (amended after perf287's review):
- **Valid fragments of the same epoch after the stop,** in the rest of that frame or at a later frame's start:
  - **`Warning`.** A power loss tears the last unsynced write at sector granularity, and the sectors that survive need not be a prefix. So a stop followed by a later fragment of the same final write is an ordinary crash state: perf287 measured 7 in 400 seeds under `torn_writes`. Offline it can't be told apart from damage, because the WAL doesn't record what was synced.
  - **The root's clean flag doesn't change that** (perf287's re-review). The flag means "no root commit since a clean close". A clean reopen commits no root, so a session that reopened a cleanly closed database, logged, and lost power leaves the flag set over a torn tail: perf287 measured 8 in 600 seeds. A genuinely clean close checkpoints every stream to its end, so the flag is never evidence that a WAL tail was synced.
  - **`Info`** when a recovery already chained a new segment exactly at the stop (`prev_end == stop`) and dropped what follows. Without this, every later check would re-report the same tear until the checkpoint passed it. Records lost to damage before that recovery read the same way, but they were dropped at that open and the check can't tell them apart.
- **Damage inside the WAL is provable.** FORMAT §10.1 rule 1 syncs a full segment before its successor's header is written. So in a segment with a running-time successor, damage shows up as a successor `prev_end` past the stop, which is `Corruption`.
- **A torn last fragment** is `Info`.
- **Unused space or an older epoch** is a clean end.
- **A successor whose `prev_end` disagrees with the stop**, several successors, a missing checkpoint segment with later segments present, or a segment newer than the checkpoint that the chain never reaches: each is `Corruption`.

**With `deep`:**
- Every blob pointer's record is read and verified.
- Each blob file's live bytes are recounted per tablet, as the engine counts them, and compared with the manifest. A mismatch is a `Warning`, because a crash before a large value's commit settled leaves the same picture.

The **regions** are the map `phdb check --map` prints. The CLI's corruption tests use it to find bytes to damage through the public API only.

## Why

#692's "Done when" says `phdb check` detects injected corruption in every structure it covers, and the public API couldn't do that:
- Block checksums are skipped on cache hits.
- A bad WAL checksum reads as a torn tail.
- A damaged superblock slot falls back silently.
- A scan reads only the data blocks a merge reaches.

The engine already has every reader the check needs. What was missing is a pass that forces verification, walks everything, and collects findings rather than stopping at the first.

It touches no commit, cross-shard (#724) or share-reporting (#777) code.

## Tests

`crates/engine/tests/check.rs`, on the simulated filesystem:
- A healthy database (plain and `deep`), and a crashed one with a WAL, check clean.
- After a crashed one checks clean, it opens and recovers every commit, so the check changed nothing.
- A live writer refuses the check, and a newer format version is refused with `UnsupportedFormat`.
- Corruption is found when bytes are flipped in: a superblock slot, the manifest snapshot, an SST data block, an SST footer, a blob record, a blob extent header, and a WAL record in a segment with a successor (the `prev_end` mismatch). A truncated file is found too.
- The same WAL damage at the end of the log is a `Warning`, and unrecorded blob references are a `Warning`.
- **False positives**, without faults: 40 seeded workloads (all three durabilities, background flushes from small memtables, separated values) crash by process kill and by power loss. None leaves anything but `Info`, and each then opens and checks clean.
- **A crash after a clean reopen:** perf287's `a_crash_after_a_clean_reopen_never_looks_like_corruption`. Over 200 seeds, session 1 closes cleanly, session 2 makes WAL-only commits at mixed durabilities, and then a torn power loss hits. The clean flag is set in every seed, and none reports `Corruption`.
- **False positives, with faults:** perf287's `power_losses_that_tear_or_reorder_never_look_like_corruption` runs 100 seeds under each of `torn_writes`, `reorder_unsynced` and both, plus both again followed by a reopen, 50 `Sync` commits and a process crash. None reports `Corruption`, and every database opens. Under the first rule, torn seed 12 failed it.

`crates/pigeonhole/tests/check.rs`: the same on the real filesystem through the public API (a healthy file, a damaged SST, a running writer, a missing file), plus a doctest.

## Callers

- `pigeonhole-engine`: `check.rs` (new), `lib.rs` (exports), `engine.rs` (`check_local` visibility), `tests/check.rs`.
- `pigeonhole`: `check.rs` (new), `lib.rs`, `tests/check.rs`.
- Later: `pigeonhole-cli` (`phdb check`).
- Docs: the agent reference, the changelog.
