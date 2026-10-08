# Backup questions (Phase 2; engine)

## Proposed decision: backup releases its snapshot's memtables before the long merge (#262; review 7 F7-6)
`backup` held one snapshot, memtables included, for its whole run. A snapshot keeps its memtables' arena chunks allocated, so a backup of a large file (minutes) left writers to fill the rest of the arena and fail with `Busy` at the stall timeout (D124, D138). On a simulated clock the hopeless case was refused at once.

The options weighed:
- **Copy the memtables first, then keep only the SST view** (chosen). The snapshot point stays exactly the call time, as documented. The memtable copy is bounded by the arena and runs at memory speed. The long part reads SSTs whose extents the kept view pins.
- **`flush()` first, then back up an SST-only snapshot.** This is simpler, but the snapshot would still include memtables with writes that land between the flush and the snapshot. It also turns every backup into a forced flush of every slot (more L0 files, a compaction burst) and moves the point in time.

**Interim behavior:**
- **Phase 1.** For every slot, the snapshot's memtable entries at or below its seqno go into temporary SSTs in the new file.
- **Release.** `backup` then builds a view with the same tablets, catalog and SST set but no memtables, registered (`ViewPin`) so its SSTs stay unreclaimed, and drops the snapshot. Its memtables' chunks are freed as soon as nothing else holds them. The seqno pin goes too: compactions may garbage-collect past the snapshot meanwhile, which is harmless since the backup reads the pinned old SST files.
- **Phase 2.** Each slot merges its source SSTs with its temporary SSTs and writes the final last-level SSTs. The temporary extents are abandoned in the new file right after (the new file's bitmap is rebuilt from its manifest at open anyway, D8). Temporary SSTs are read through a private block cache, because the new file's SST ids start at 1 and would collide with the engine's cache keys.
- **Cost.** The new file briefly holds up to one arena's worth of temporary SSTs. The old SSTs a compaction replaces during the backup stay allocated until it ends, as they did.
- **Test hook.** `Engine::after_backup_releases_memtables` runs between the phases. No public seam can observe the release, and the test (`backup_releases_the_memtable_arena_before_its_long_merge`) checks that a flush there gives the arena back.
