# 0049: `PigeonholeReader::backup` and `backup_durable`

**Status:** Approved (owner, via the coordinator, 2026-10-20; #692). Design option A of the 2026-10-21 trade-off note: the reader holds its pin for the whole copy. Additive.

## Change

In `pigeonhole` (additions):
- `PigeonholeReader::backup(&self, dest: impl AsRef<Path>) -> Result<()>`
- `PigeonholeReader::backup_durable(&self, dest: impl AsRef<Path>) -> Result<()>`

They do what `Pigeonhole::backup` and `backup_durable` do, from a reader process while the writer runs.

In `pigeonhole-engine`:
- **`Engine::backup` and `backup_durable`** no longer refuse a reader with `ReadOnly`. `shrink` still does.
- **`maintenance::backup`** keeps a clone of a reader snapshot's live count (its shm pin) until the copy is done.
- **Its close check** also stops with `SnapshotExpired` if the writer restarted. A read that failed after such a restart reports `SnapshotExpired` too, not the error the reused bytes caused. The partial copy is removed either way.

There is no new shared state, no protocol change and no FORMAT change.

## The trade-off

The writer's backup works like this:
1. Phase 1 copies the snapshot's memtables into temporary SSTs.
2. It then releases the memtables (#262).
3. It keeps the SSTs alive through a `ViewPin` on `Shared::live_views`, which is visible only in the writer's process.

A reader process can't pin that way. Its only protection is its shm reader slot (a seqno and a view version). The writer frees a view's memtables **and** extents only once no slot pins that view or an older one (FORMAT §11.5).

A reader's slot pin also moves forward to a newer view when that reader takes a new snapshot and none of its snapshots is alive. Without a held clone of the live count, a read in the reader process during the backup would move the pin past the backup's view. The writer could then reclaim SST extents the merge is still reading.

**So a reader backup holds its pin, memtables included, for the whole copy.** The cost:
- For the backup's length, the writer frees none of that view's memtables (arena chunks) or extents.
- On a write-heavy database the writer's arenas can fill, and its commits then wait, up to `write_stall_timeout`, then `Busy`. That is the #262 effect, confined to a backup taken from a reader.
- The rustdoc of `PigeonholeReader::backup` and the CLI guide say so.

**Option B,** an shm pin of a manifest version apart from memtables, would let a reader release memtables after phase 1. It is a FORMAT §11.5 protocol change, and is left for its own issue if this cost matters.

## Tests

`crates/engine/tests/reader_backup.rs`, on the simulated filesystem, with writer and reader as separate processes:
- **The copy is the reader's snapshot.** Mid-copy, the reader reads at a new snapshot, and the writer commits, flushes, compacts and flushes again. The copy checks clean and holds exactly the 1500 rows from before. The SSTs the compaction replaced stay retired, not reclaimed.
- **The reader's backup holds the writer's memtables for the whole copy.** This is the documented cost, the mirror of milestone_b's #262 test for the writer.
- **A writer restart mid-copy** fails the backup with `SnapshotExpired` and removes the partial file.

`crates/pigeonhole/tests/api.rs`: `a_reader_backs_up_while_the_writer_runs`.

**The dropped-pin test** (recov620's review): mid-copy, the steps run in this order.
1. Record the backup's pinned view (`reader_pin_and_view`).
2. The writer commits, flushes and compacts.
3. The reader takes a fresh snapshot.
4. The writer makes a flush the size of the first, whose manifest commit reclaims.

The test then asserts two things: the oldest reader pin is still at or below the backup's view, and `unreferenced_bytes() > 0`.

With the held clone removed, it fails deterministically: "the reader's pin moved past the backup's view 6: Some((1600, 10))". The ordering matters. A fresh snapshot taken before the compaction would move the slot only to a view that still names the old SSTs.

The setup's 1500 rows are below 4096 entries per slot, so a restart is caught by the checks before each slot and the final one, not the in-merge one.

## Callers

- `pigeonhole-engine`: `maintenance.rs`, `engine.rs` (`backup_of` and its docs), `tests/reader_backup.rs`.
- `pigeonhole`: `db.rs`, `tests/api.rs`.
- `pigeonhole-cli`: `open.rs` (`Db::backup`, where the writer or reader choice lives) and `cmd/backup.rs`. `phdb backup` backs up from a reader process while a writer runs.
- Docs: the agent reference, the CLI guide, the changelog.
