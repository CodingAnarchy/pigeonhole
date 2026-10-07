# Engine questions (issue #38: tablet splits, merges and the balancer)

## Q: Where does the manifest record a tablet's owner?
The spec says a move "records the new owner in the manifest", but `Edit::PutTablet` has no owner field and the `Edit` tags are frozen in `pigeonhole-format`. Adding an edit is a format change outside the engine.

**Interim behavior:** owners live only in the in-memory catalog and the published tablet map. A move, and the placement of a split's children, is still one manifest commit (`ReqKind::Tablets`, which also writes the `Counters` edit), so it is serialized with every other catalog change. At open every owner is re-derived from the tablet id (`tablet % shards`), as before; the balancer moves tablets again if the load calls for it. Correctness does not depend on the owner surviving: replay routes every record through the tablet map at open, and checkpoints compare slots against the catalog's flushed seqnos rather than the shard's own (see the next question). If owners should survive a reopen, add an `Edit::SetTabletOwner` (or an owner field on `PutTablet`) in `format`.

## Proposed decision: checkpoints compare slots against the catalog, not the shard
After a move, or a reopen that re-derives owners, a stream can hold records for slots another shard now owns and flushes. A shard's checkpoint used its own per-slot flushed seqnos, so it never passed such records (the WAL grew and a clean close waited for ever), and a replayed PREPARE applied on another shard was logged with no slots at all (its participant could checkpoint past it before the data reached an SST).

**Interim behavior:** `needed` and `report_shares_flushed` read the flushed seqnos of the current view's catalog. A slot whose tablet no longer exists needs nothing: its table was dropped, or a split or merge retired it after every write to it reached SSTs. Replayed PREPAREs record the slots of every shard they were applied on. Every manifest commit that adds SSTs broadcasts `Maintain`, which now also advances checkpoints and share reports. This replaces the per-shard `dropped` set.

## Proposed decision: a commit routed through an older tablet map
A router can pick a shard just before that shard splits, merges or moves the tablet.

**Interim behavior:**
- **Single-shard commits** touching a tablet being changed are *parked* on the owner before they are logged (they hold no seqno, so the watermark never waits for them) and routed again once the change commits: to the same shard, to the new owner, or through two-phase commit when their rows now span shards. A later commit on any of the same rows parks behind them, so per-row submission order holds on that shard. Commits that reach a shard that no longer owns their rows are forwarded the same way. A client that pipelines two commits on one row from one thread may, during a move, see them applied in the other order (the second can reach the new owner before the first is forwarded); concurrent commits never had an order.
- **PREPAREs** touching a tablet being changed, or routed with a tablet map older than the participant's, are refused with an internal `Moved`. The coordinator aborts the commit everywhere (no COMMIT record, so recovery discards the PREPAREs that were written) and retries it once the tablet map is newer than the one it routed with or a tablet change has finished. The retry keeps its first commit timestamp when that is still at or above every participant's floor, so a refused attempt is invisible to the caller. Each PREPARE carries the whole commit's reads; a participant only checks the reads in tablets it is changing.
- A change waits, before it commits, until no prepared share and no compaction touches its tablets and every memtable of them is in SSTs (shares decided after the freeze are frozen and flushed again).

## Proposed decision: the default-timestamp floor of a moved tablet
D11 wants a per-tablet floor that travels with the tablet; D86 keeps it per shard.

**Interim behavior:** the shard floor stays the only floor kept; it is an upper bound on every default timestamp the shard assigned to any of its tablets. Before a change commits, the shard raises the floor of every shard receiving a tablet to its own (`Shared::ts_raises`, read by the receiver's next default timestamp), so the moved tablet's timestamps never go backwards. A coordinator also picks a cross-shard commit timestamp above every participant's published floor. No per-mutation bookkeeping is added.

## Proposed decision: what the balancer does, and its options
The spec gives the triggers (size, sustained write skew, small and cold) but no policy.

**Interim behavior:** each shard runs its balancer every `EngineOptions::balance_interval_nanos` (default 100 ms; 0 disables it) and changes at most one thing at a time, in this order:
1. **Size:** a tablet whose SSTs hold at least `tablet_split_bytes` splits in two near the middle of its SST boundary rows and recent writes. A tablet that still shares SSTs with a sibling (D13) does not split by size (their bytes would count twice).
2. **Skew:** a shard that wrote at least `balance_min_writes` rows in the interval and more than `balance_skew` (default 1.25) times the mean over shards moves the tablet whose load is closest to half the gap to the coldest shard. When one tablet carries more than the gap, it splits instead, at quantiles of a 64-row sample of its recent writes, into one child per shard below the mean (children go straight to those shards). The same rule applies to memtable bytes, with `memtable_freeze_bytes` as the minimum.
3. **Merge:** two adjacent tablets of one table on the shard, with no writes for two intervals and empty memtables, holding less than a quarter of `tablet_split_bytes` together, merge. A merge is refused while a sibling still has to compact its copy of a shared SST, since the merged tablet would see those rows twice; merges never move tablets to bring neighbours together.

`balance_interval_nanos`, `balance_min_writes` and `balance_skew` are new, additive `EngineOptions` fields. The test hooks `split_tablet_pending`, `merge_tablets_pending`, `move_tablet_pending`, `balance_pending`, `tablet_changes`, `max_ts_floor` and `TabletMap::ranges` sit behind `test-hooks`, like the other hooks.

## Proposed decision: a split's children and the view buffer (D28)
**Interim behavior:** a split whose estimated encoded view would not fit the shared-memory view buffer is refused before it starts (`Unsupported`), so the published view is never refused after the manifest commit (which would poison the pager). Children reference only the parent's SSTs that overlap their own range; scans clamp every tablet's sources to its range, since a shared SST also holds the sibling's rows.

## Proposed decision: a write stall that nothing can end is refused at once
D124 refuses a commit waiting for arena room with `Busy` after `write_stall_timeout_nanos`, or at once when the batch can never fit an empty arena. With many tablets per shard, every `(tablet, family)` slot takes memtable chunks, and memtables pinned by live snapshots stay allocated after their flush. A wait can then reach a state where nothing is frozen or flushing and nothing can freeze: only snapshots being released could make room.

**Interim behavior:** in that state the waiting commits are refused with `Busy` at once instead of being held to the timeout. A wait that a flush, a deferred freeze or the group's own reservations can still end keeps waiting as before. Separately, the balancer and tablet-change validation keep each shard's slots to a quarter of its arena's chunks (`max_slots`), and empty slots release their memtables after a flush. This caps how many tablets of many-family tables a shard holds: with the default 64 MiB budget and 1 MiB chunks, 16 slots per shard. Smaller arena chunks for shards with many tablets are the longer-term fix.
