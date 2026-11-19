# 0141: the engine calls a range move needs: feed-silent batches, `raise_ts_floor`, a feed retention hold (D331 PR 1)

**Status:** Approved.
- **Numbers** are from the coordinator (`.coord/numbers.md`): `incompat` bit 3. The count-bit encoding was chosen over new WAL record types (coordinator, 2026-11-18).
- **The design** is #1499's: cli692's note, with simp724's and perf287's amendments and Amendment 5 (feed-silent cleanup, reviewed by simp724 and cli692).
- **Review:** simp724 (engine), perf2 (WAL format), cli692 (D331).

## Why
A range move (D331) copies a key range from partition S to partition T, follows S's feed for it, hands it over, then deletes S's copy.
- **The copy and the cleanup aren't new history.** If T's feed carried the copy, or S's feed carried the cleanup, a subscriber would see the range's rows written again, or deleted, at the move.
- **A replica must still apply them,** or it stops being a copy of its primary (simp724's M1 on Amendment 5).
- **T's timestamp floor must rise past S's timestamps** before T serves the range. T's clock may be behind S's, and a later write on T would then sort under a copied version (simp724's M1).
- **S's feed must keep its history from the copy's first snapshot** until the catch-up has read it, however long the copy takes (perf287's C1).

## Feed-silent batches
- **Format** (FORMAT §10.3): bit 31 of a batch's `count u32` marks it feed-silent. The mutation count is bits 0–30. Every record that carries a batch can carry the mark: Batch (1), Prepare (2) and External Prepare (5).
- **The bit** (FORMAT §8.2): `incompat` bit 3 is set in both superblock pages before the first silent batch is routed, and never cleared. A build without D331 refuses the file. Without the superblock bit, a marked batch is `Corrupt`: `WalRecord::decode` refuses it, and `decode_with(bytes, silent_ok)` takes it only when `silent_ok`. So a stray bit is never read as the mark.
  - Replay and `phdb check` take `silent_ok` from the superblock.
  - The engine's change feed takes the mark in its cursors and checks it per batch, where it already looks at each batch. A marked batch is `Corruption` unless the database's flag, or failing that the superblocks on disk read again, has the bit (`Pager::features_on_disk`). A reader process's pager reloads only when the manifest version moves, and setting the bit moves none (simp724's S1 on #2055). That check is out of line, so a batch without the mark costs the feed one bit test (#2055's feed shapes).
  - `WalRecord::decode_with` finds the mark at its fixed place in the record (`WalRecord::is_marked`) before decoding, so the decoder is what it was before D331. `FeedCursor::accept_silent` remains for a cursor that gates by itself.
- **Engine:**
  - `WriteBatch::mark_feed_silent()` (doc-hidden). Every commit path that rebuilds a batch keeps the mark: counter preparation, large-value separation, the cross-shard split, and a refused cross-shard round's retry.
  - On a replica database, a silent write of its own is refused, as any write of its own is, and sets no bit. A silent batch reaches a replica only as its primary's logged one.
- **The feed reader** passes over a silent commit as it does one to other tables: nothing delivered, and the position moves past it.
  - `FeedReader::include_silent()` (doc-hidden) delivers them, and `FeedReader::silent()` says whether the last commit was one.
  - Neither the engine's `FeedSpec` nor `pigeonhole`'s `FeedBuilder` has such an option, so no client subscription can ask for them.
- **Replication** (`pigeonhole`, `__unstable-replication`):
  - `Feed::include_silent()` turns the reader's option on, and keeps it across a resubscribe.
  - A silent commit's `ReplicatedEvent` encodes as **version 2**; every other event stays version 1. A stream without silent commits is byte-identical to today's.
  - `ReplicatedEvent::decode` takes 1 and 2. A build before D331 refuses version 2 (`InvalidArgument`), so its follower halts rather than apply a silent commit as an ordinary write.
  - `apply_replicated` splits a run where silence changes, and commits a silent event's part silent. The replica's own feed then skips it as the primary's does, and a promoted replica's feed is its primary's.
  - `phdb serve`'s `Replication.Follow` always includes silent commits, since only a replica's follower calls it.
  - An old follower halting on version 2 is the backstop behind the writer's `range_moves` gate (a move is refused unless every node owning, or holding a replica of, S or T has the feature). It's not a failure mode an operator should meet (cli692).

## `raise_ts_floor(ts)`
- `Engine::raise_ts_floor(ts)`, and `Pigeonhole::raise_ts_floor` under `__unstable-replication`, both doc-hidden.
- They raise D271's `H` to at least `ts`, and commit the manifest's `ts_floor` before returning: replay recovers commit timestamps, not a copy's explicit ones.
- Refused on a replica.

## A feed retention hold
- `Engine::hold_feed_retention(cap_bytes) -> FeedRetentionHold`, and `Pigeonhole::hold_feed_retention` under `__unstable-replication`, both doc-hidden.
- **What it holds:** every stream's history from its current segment on (a stream's head segment is never trimmed). It holds until the guard is dropped, or until the database retains more than `cap_bytes` of history.
- **The cap:** past it, the hold lapses. It's dropped, `lapsed()` turns true, and retention trims again, so a reader it was for finds its position gone and expires. A move then aborts with "the copy outran the source's feed retention".
- **Mechanism:** `FeedMarks::set_hold(epoch)`. A stream's retention pass stops its floor below the held epoch, and never lowers it.
- The engine keeps every hold in one leaf-locked list. Each shard's feed maintenance lapses holds past their cap, at one atomic load while none is held.
- `InvalidArgument` without the change feed, and `Busy` before every stream has opened.
- **Taken before the copy's first snapshot,** so the catch-up's token is always inside it.

## Range drops (D331 PR 4a)
`Engine::drop_range(table, start, end)` and `Pigeonhole::drop_range` (doc-hidden), with `apply_range_drop` on a replica. They remove rows `[start, end)` outright, with no delete markers, so a later write of the range at an older timestamp is visible. The `incompat` bit is D331's bit 3, the same feature and release.
- **The steps:**
  1. Split the table's tablets at the bounds, through the shards' tablet changes, which freeze and flush what they split, so nothing outside the range is lost.
  2. Write a Range Drop (WAL type 7, FORMAT §10.3) synced on **every** stream at one seqno, held from readers until all have it and then released with nothing applied. Tablet owners aren't stored, so every stream carries it.
  3. One manifest commit drops the tablets inside the range and puts one fresh tablet over it, with `SetFlushed` at the seqno in each family. Replay skips a write at or below a slot's flushed seqno, so no earlier write of the range comes back. Then the shards retire the dropped tablets' memtables.
  4. Raise every row-cache slot to the seqno, so a cached row of the range misses in every process.
- **Snapshots:** a live snapshot keeps its view, with the dropped tablets' SSTs and memtables.
- **Gating:** the caller keeps the range unserved while it drops, and stops and joins every writer of the range first: at a source's cleanup its range fence, at a target's abort its copy and catch-up (simp724's M2). A write landing around the seqno would apply live into the fresh tablet but be skipped on replay. So no write of the range is in flight below the seqno. It runs the drop again after a crash until it returns, from its drop-pending row. Every step does nothing once done. Engine replay of a type 7 only counts its seqno, and the record is never needed past a checkpoint: every manifest commit stores a `seqno_ceiling` above it.
- **The feed:** a client subscription passes it, as a seqno with no event. A reader that includes feed-silent records gets `Next::RangeDrop` in seqno order, once, which pigeonhole carries as `ReplicatedEvent` body tag 2. A replica's follower applies it between runs (`apply_range_drop`, then the token). The body is written in version 1 only, and a drop in any other version byte is refused (feed1248's M1).
- **Upgrades:** body tag 2 is new inside version 1, so a replica built before it refuses the event and halts its follower at the first drop. Replicas are upgraded before their primary; a cluster's range moves need every node on 1.3.0 anyway (feed1248's S1).
- **Refusals:**
  - `ReadOnly` on a replica (`drop_range`), and `InvalidArgument` on a primary (`apply_range_drop`);
  - `Busy` while an external share is in doubt (D329; pigeonhole names the xids) or while the balancer keeps changing the bounds' tablets. Read holds (D329 PR 2b's `hold_reads`) refuse it too, added by whichever of #2076 and #2089 lands second (simp724's S1);
  - `Unsupported` with tablet changes off;
  - `InvalidArgument` for an end at or before the start.
- **Leftover tablets:** the balancer merges the small ones left at the bounds.

## Logged batches by name (D331 PR 4a)
`Pigeonhole::logged_batch() -> LoggedBatch` (doc-hidden) and `Engine::commit_logged`. A range move's target writes the source's changes exactly as the source logged them: their kinds, timestamps and stored values (`Change::stored`), named by table and family, since the target's ids aren't the source's.
- It holds logged changes only, so it never mixes with ordinary writes.
- It may be marked feed-silent.
- The timestamp floor isn't raised; the target raises it once, before it serves the range.
- A family the table lacks fails the whole batch, never dropping a change.
- Refused on a replica (`ReadOnly`).

## Callers
D331's range move (serve, later PRs) and D328/D315's replication. Nothing else.

## Tests
- `crates/format/tests/validation.rs`: a silent batch round-trips, and is `Corrupt` to `decode` and to `decode_with(.., false)`. As a build before D331 reads it, the count runs past the batch's bytes.
- `crates/format/tests/golden.rs`: frozen fixtures of a silent Batch, Prepare and External Prepare. `decode` refuses each, and `decode_with(.., true)` takes it (perf2's S1 and S2).
- `crates/engine/tests/it/feed_reader_process.rs`: a reader process subscribed before the writer's first silent commit takes the mark, skips the commit, and sees it with `include_silent` (mutation-checked against the cached features).
- `crates/engine/tests/it/feed_silent.rs`:
  - a subscriber sees the plain commits around single- and cross-shard silent ones, and a reader that includes them sees every commit, marked;
  - bit 3 is set by the first silent commit only, in both pages, and stays across a reopen and a process crash, after which replay applies the batch and a feed from the oldest still skips it;
  - a marked batch with the bit cleared doesn't open;
  - the raised floor survives a crash;
  - a hold keeps a token's history past the retention, a dropped hold doesn't, and a capped hold lapses;
  - a replica refuses its own silent write and sets no bit;
  - counter and large-value batches stay silent.
- `crates/pigeonhole/tests/replication.rs`:
  - a replica applying an interleaved run matches its primary;
  - silent events are version 2 and the rest version 1;
  - neither the primary's client feed nor the replica's own feed sees a silent write;
  - version 3 is refused.
- **PR 4a** (`crates/pigeonhole/tests/replication.rs`, `crates/format/tests/{golden,validation}.rs`):
  - B1: a dropped range takes older writes again, before a crash, after it, after a re-run and after compaction, while a snapshot from before reads it whole;
  - a crash after each of the drop's mutating operations leaves the range whole or gone, and the drop run again finishes it;
  - a feed passes a drop, and tokens before and after it resume after a crash;
  - a lagging replica applies the drop in feed order and matches its primary;
  - a flagged drop event is refused;
  - a logged batch copies another database's changes by name (a delete hiding an older `put_at`, counters), and a missing family fails it whole;
  - type 7's golden files and bound checks.
- **Mutation-checked:**
  - the reader not skipping silent batches;
  - the hold ignored by retention;
  - the cross-shard split dropping the mark;
  - counter and large-value rebuilds dropping it;
  - the replica applying silent events as ordinary writes.
