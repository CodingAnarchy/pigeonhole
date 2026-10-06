# pigeonhole-engine — open questions (Milestone B, issue #37)

Questions the spec and decisions leave open for flush, checkpoints, compaction scheduling, backup and shrink. Each records the interim behavior the code implements.

## Q: Which WAL streams must a flush sync before its SSTs become visible?
The spec says a flush "must not persist a share of a cross-shard commit until every PREPARE and the COMMIT are durable" but does not say how the flush learns that. Checking per commit (which shards hold shares, whether their records are past each stream's durable LSN) needs cross-shard state the flush task does not have.

**Interim behavior:** before committing its manifest edit, a flush task sends a `SyncBarrier` to its own shard always (so an earlier commit of the same stream is never lost while a later one survives in an SST) and to every shard when any flushed memtable holds a share of a cross-shard commit (`MemEntry::has_shares`). Each barrier is one `submit_sync` of that stream; the task waits for all replies. This over-syncs (every stream, not just the participants') but needs no bookkeeping; a flush is rare compared with commits.

## Q: When may a shard checkpoint a PREPARE or COMMIT record?
D24 says a checkpoint never strands a prepared commit, and D83 that a cross-shard commit is recovered all or nothing. Neither says what a participant needs to know about the coordinator's COMMIT before it moves its checkpoint past its own PREPARE.

**Interim behavior:** each shard keeps a deque of its logged records (`Logged`) and advances its checkpoint to the end of the longest prefix it no longer needs: a single commit while any of its `(tablet, family)` slots is unflushed; a PREPARE until its slots are flushed and either the coordinator reported its COMMIT checkpointed (`CommitCheckpointed`) or the commit was the shard's own and is complete; a COMMIT until every participant reported its share flushed (`ShareFlushed`). Aborted prepares pass at once. The checkpoint is clamped to the stream's written LSN and the manifest edit is committed before `wal.checkpoint` runs, so a crash between the two replays harmlessly.

## Q: Is `SetFlushed` the memtable's max seqno, or the shard's visible seqno?
The manifest brief says `SetFlushed { tablet, family, seqno }` and replay skips mutations at or below it, but a memtable frozen while a group is mid-apply could hold a seqno above the visible watermark while a lower one is still being applied to the active memtable.

**Interim behavior:** a shard freezes only when `active.max_seqno <= visible_seqno`, so every entry up to the memtable's max seqno is in it and `SetFlushed` is exactly that max. The freeze is deferred (`freeze_deferred`) until the condition holds, never skipped.

## Q: How conservative is `GcPolicy` about reader-process snapshots?
D70 narrows purge by `min_ts_above`; D74 purge needs the set of live snapshot seqnos. Reader processes only publish a reader-slot pin (a view version), not their snapshot seqnos.

**Interim behavior:** `gc_policy` takes the writer's live snapshot seqnos (`LiveSeqnos`) plus, for every pinned reader slot, the seqno the pinned view version was published at, treating that as a live snapshot at that seqno and everything above it as reachable. This is conservative (a reader pinned at version v may hold no snapshot at all) and loses only purge work, never visibility. Exact reader snapshot sets are issue #39's territory.

## Q: How should the L0 write stall behave with a frozen or coarse clock?
The spec's token bucket refills with time. Under the simulator the clock advances only when the workload says so, so a stalled shard with nothing else running would wait forever.

**Interim behavior:** the stall engages only while the L0 score is `>= 1.0` and a compaction can run; it arms one `StallTimer` task with a cancel flag, cancelled the moment the score drops (a compaction committed) so a timer never spins on a frozen clock. A commit that cannot get room retries on the next `Kick`. A failed background compaction sets a backoff flag that a stall (score `>= 1.0`) clears, so a device that recovers is retried while a dead one does not loop.

## Q: What does `backup` write for an engine with memtables and many levels?
The spec says a backup is a consistent single-file copy; D60 covers shrink. Copying SST extents verbatim would still need the WAL (unflushed memtables) and the file's free-space layout.

**Interim behavior:** `backup` takes a snapshot and writes a new file: every `(tablet, family)` is merged from the snapshot's memtables and SSTs (raw entries at seqnos `<= snapshot`, no purge) into one SST at the last level, a manifest snapshot names them, and the file is marked clean. The result opens without replay and with no sidecars. Blob extents are not yet copied (filed as a follow-up); data stays inline below the D29 threshold.

## Q: What happens at open when the discovered streams do not match `0..shards`?
D20 says streams beyond a reduced shard count are flushed then removed, but says nothing about the opposite direction (more shards than streams) or whether the flush is synchronous.

**Interim behavior:** when the discovered stream set is not exactly `0..shards`, open replays everything, flushes every recovered memtable synchronously (`flush_recovered`), checkpoints each kept stream to its end, removes the extra streams and creates the missing ones. Open then holds no unflushed WAL data, so the new layout starts clean. This makes a shard-count change an expensive open, which the spec accepts ("a one-time cost").

## Q: Should SST readers open lazily or at manifest apply?
The spec targets `open()` under 5 ms; a manifest can name hundreds of SSTs whose footers would all be read at open.

**Interim behavior:** `OpenSst` holds the metadata and an `OnceLock<Arc<SstReader>>`; the reader (footer, index, filter blocks) is opened on first use by a read or compaction, through the block cache. Open reads only the manifest.

## Q: Does the sim's recovery helper cover a coordinator that is also a participant?
Issue #48 asks the engine suite to adopt `recovered_commits` / `check_acknowledged_survive`. The helper counts one record per stream per commit, so a coordinator's PREPARE and COMMIT on its own stream must be adjacent; the engine interleaves other commits' PREPAREs between them whenever commits overlap.

**Interim behavior:** the harness takes the engine's own append order (the `test-hooks` `AppendedRecord` stream), applies the record-level prefix rule itself, and cross-checks `recovered_commits` only over commits whose records are adjacent per stream and all appended (`sim_helper_recovered`), skipping the check when the streams cannot be represented. `check_acknowledged_survive` and `Model::from_commits` are used as is. A record-level helper in `pigeonhole-sim` would let the check run on every crash; filed as a follow-up on #48.

## Q: How does a group waiting for arena room learn that a flush freed some?
`ShardArena` reports free bytes only through `reserve`; nothing signals the shard when `reclaim` returns memory.

**Interim behavior:** a flush completion (`Flushed`) and every `Maintain` message re-run `reserve_room` for the waiting group (`refresh_free`), and the shard `Kick`s itself. A batch that can never fit in an empty arena fails with `Busy` at once.
