# 0140: prepared transactions with an external xid (D329 engine PR 1)

**Status:** Approved. Amendment 1 (the coordinator, 2026-11-17, D329 PR 2a; same feature, no new number): `prepare` with reads and no writes logs, and `Transaction::hold_reads` holds a read-only transaction's reads in memory.
- Numbers are from the coordinator (`.coord/numbers.md`): WAL record types 5 and 6, and `incompat` bit 2.
- The D329 design was reviewed by audit120 and cli692, and signed off by the owner.
- perf2 reviewed this ICR's draft as engine reviewer (E1 to E3, Q1 to Q4). Bit 2 is sticky, which supersedes the draft's clearing.
- It amends nothing in D294: bit 2 is sticky like bit 0.

## Why
D329 commits a transaction across partitions on different nodes with two-phase commit. Each partition's branch must prepare durably, hold the rows it read and wrote, survive a crash or a close *in doubt*, and later be committed or aborted by its caller.

The engine's cross-shard 2PC (D263) does all of that inside one commit call, but recovery never leaves a share in doubt: a PREPARE without its decision is dropped. This ICR lets a caller hold the decision.

## API (`pigeonhole`, behind `__unstable-txn-prepare`)
The feature is hidden from the docs and outside the semver promise, as `__unstable-replication` is. `phdb serve` is its one caller.

- **`Transaction::prepare(self, xid, durability) -> Result<CommitInfo>`:** the answer's seqno is the commit's id.
  - It validates the read set and logs the shares, through the PREPARE path even for one shard, and returns once they meet `durability`. Callers ask for GroupSync at least. `Durability::None` is refused (`InvalidArgument`): a `None` decision writes no record, so a crash could bring a decided share back in doubt. A refused round doesn't retry, as a D263 round does: a retry runs as a plain commit, which would commit the share without its decision. So every transient refusal a plain commit would retry (a moved tablet, memtable room, another round's undecided share) answers an external prepare `Busy` at once, and callers retry with backoff. Re-entering the coordination with the external fields kept is a later option. `Buffered` is accepted, with the power-loss caveat of any `Buffered` write: a power loss may take an acknowledged prepare (its xid is then not in doubt, and the caller decides an unknown xid: `decide` answers `None`), or an acknowledged decision (the share comes back in doubt and is decided again). Callers that need the prepare's promise ask for GroupSync.
  - The share then holds its rows, as an undecided D263 share does, until decided.
  - A transaction with no reads and no writes is `InvalidArgument`.
  - **Reads and no writes (amendment 1, D329 PR 2a):** logged like any other prepare, as a type 5 with the read set and an empty batch. It holds its reads until decided and is in doubt across a reopen; a commit decision applies nothing. This is a writing spanning transaction's read-only branch.
- **`Transaction::hold_reads(self) -> Result<ReadHold>` (amendment 1, D329 PR 2a):** a wholly read-only spanning transaction's hold. It validates the transaction's reads and holds them in memory, logging nothing; it is never in doubt. While it holds, a write to a held row is refused at once, as over a share in doubt (a transaction's prepare `Conflict`, any other write `Busy`).
  - `ReadHold::release(self) -> Result<bool>` answers whether the hold was still in place (`true`) or lost to a close or a recovery (`false`): `false` only ever means lost. It waits for the shards' answers, so on a thread that drives a shard (application-owned mode) it's refused (`InvalidArgument`), and the hold is released as it's dropped (perf2's S2 on #2062). Dropping it releases it.
  - Releasing a hold wakes the commits refused over its rows, as a share's decision does (perf2's S1 on #2062).
  - A read whose table was dropped since it was read can't be held: `Conflict`.
  - A transaction with writes, or with no reads, is `InvalidArgument`. A read conflicting since the snapshot is `Conflict`.
  - Live holds count against `in_doubt_limits`' share count: past it, `hold_reads` answers `Busy`. A hold is never evicted.
- **`Pigeonhole::decide(xid, commit) -> Result<Option<Commit>>`:**
  - Commit reserves the apply seqno and timestamp, logs a Decision (type 4) and applies the shares.
  - Abort logs an External Abort (type 6) at a seqno of its own and drops them.
  - `Ok(None)` for an xid that isn't in doubt (already decided, or never prepared): decide is idempotent.
- **`Pigeonhole::in_doubt() -> Vec<InDoubt { xid, prepared_at_ms, wal_bytes }>`:** what a reopen found, or what is still undecided. `wal_bytes` is per share.
- **`Pigeonhole::in_doubt_limits(max_shares, max_wal_bytes)`:** past either (`max_wal_bytes` is the database's total), `prepare` answers `Unavailable` naming the oldest xid.
- **Waits:** a write that waits on an external share waits until its own deadline, or the write-stall timeout without one, then answers `Unavailable` naming the xid. A PREPARE overlapping one is refused at once (`PendingOverlap`), as today.
- **Read-only:** a read-only open, or a reader process, answers `ReadOnly` to `prepare` and `decide`.

## Format (FORMAT.md §10.3, §8.2)
- **Type 5, External Prepare:** the id, the coordinator stream, then the external share: xid, every participant stream, the read set, the batch.
- **Type 6, External Abort:** the id, the abort's own seqno, then the share without its batch.
- **Commits** reuse Decision (type 4).
- **`incompat` bit 2:** set in both pages before the first type 5, and never cleared. Retained feed segments keep such records past every checkpoint, as with bit 1.

## Engine
- **Holding the decision:** `PreparedShare` gains its external fields (xid, reads), and the local coordinator's `Coord` holds its decision for `decide`. A registry, `ExternalRegistry` (`Shared::external`: xid → id, coordinator, prepared-at, batch bytes), serves `decide`, `in_doubt` and the limits. A prepare reserves its xid there, under the lock that checks for the xid and the limits, before it submits; the coordinator replaces the reservation with the entry before it answers. So two prepares of one xid never both log, and concurrent prepares never pass a limit together. A `decide` of a reserved xid answers `Busy`. It's a concurrency.md protocol row, loom-modelled: two prepares of one xid, a `decide` racing a prepare, two `decide`s racing the removal, and the limit under concurrent prepares.
- **Recovery:**
  - Type 5 is stashed with its reads.
  - A Decision with every participant present applies. An Abort drops.
  - Neither, with every participant present: the share is in doubt. Its `prepared` entries, `pending_rows`/`pending_reads`, `Coord` and registry entry are reinstalled before any write is admitted, and `needed()` keeps its records. The reinstalled PREPARE records the slots its batch writes, as an appended one does, so once decided its record stays needed until they're flushed. A reinstalled share's decision is logged at GroupSync whatever it was prepared at: the type 5 doesn't record its level, and GroupSync is the floor callers ask for.
  - A participant missing: dropped as aborted. That is only a torn tail below GroupSync, a disk that lies, which the guide states.
  - `log_replayed` no longer aborts an in-doubt type 5. `flush_recovered` checkpoints only to the first in-doubt record. A layout change with shares in doubt is refused at open.
  - `max_seqno` counts type 5's id and type 6's `abort_seqno` (E2).
  - Tablet owners aren't persisted, so a tablet that moved before a prepare would be placed elsewhere at the reopen than its share's holds (perf2's M1 on #2000). With tablet changes on and bit 2 set, open pre-scans the WAL with the same in-doubt rule and pins each tablet an in-doubt share touches (its writes and its reads) to the shard that logged it, before placing the rest. Two in-doubt shares from different shards on one tablet, which no run of this build leaves, refuse the open.
  - The share's record is built at the member's admission, with the tablet map its group validates by, so the reads it logs are the ones the shard validated.
  - Recovery refuses a type 5 with an empty xid, or whose stream or coordinator isn't among its participants (`Corruption`).
- **Close and moves:**
  - A close doesn't wait for external shares; they stay in the WAL.
  - `nothing_in_flight` ignores them.
  - A tablet change (packing, D217) touching an in-doubt share, a row it writes or reads (#2050), is given up with `Busy` and retried later. A layout change (shard count) or spill at open with shares in doubt is refused with `Unsupported`.
  - A transaction's PREPARE overlapping an in-doubt share fails at once with `Conflict` (any other write, which validates no reads, with `Busy`), rather than waiting on a decision that may take hours.
- **Visibility:** an undecided share holds neither the visible watermark nor the feed's WRITTEN/SYNCED. Its id is reserved, never held, and its apply seqno is reserved at the decision.
- **The abort's seqno (E1),** and its four conditions:
  1. The record is in the WAL before the seqno can be visible.
  2. Feeds and replicas pass over it with no event, while delivered positions and tokens move past.
  3. It is an ordinary empty group for watermarks, with no memtable entry and no counted write.
  4. Recovery counts it and never reapplies it.

## The feed reader (E3)
- A held type 5 is held as a D263 share is, with its filtered changes, and delivered at its Decision's apply seqno.
- It sits outside the read-ahead budget (it counts 0 bytes) and `top_held`, so it never triggers an eviction or a cutoff, however long it waits.
- The idle shortcut treats it as settled, since an abort's own seqno moves the frontier.
- Type 6 drops the hold.
- **Memory:** what a subscriber holds for shares in doubt is bounded only by the writer's in-doubt limits: up to `--txn-max-in-doubt-wal-bytes` (filtered to its tables) per subscriber, so N subscribers hold up to N× that. The guide says so next to the flag. perf2 accepted this for PR 1.
- **Follow-up, #1993 (Post-1.0):** hold the position only and read the record again at its Decision, so a held share costs a few bytes whatever its batch.

## Limits
- `EngineOptions::in_doubt_limits` (`pigeonhole::Options::in_doubt_limits`, hidden behind the feature): past either the count or the bytes, a new prepare answers `Busy`, so a home that stops deciding can't pin unbounded WAL. An open option, so an automatic recovery's new instance keeps it. The engine's default is no limit; `phdb serve` sets it from `--txn-max-in-doubt-wal-bytes` (D329 PR 2).
- **Serve default (the coordinator, 2026-11-16):** 16 MiB of batches in doubt per partition, so 64 feed subscribers hold at most 1 GiB in the worst case (perf2's condition 2). Raising it is a deliberate operator choice. The guide states it, with the per-subscriber bound, next to `--txn-max-in-doubt-wal-bytes` (D329 PR 2, where the flag lands).
- Not supported, and refused with `InvalidArgument`: values large enough to be stored apart (the share's batch must be whole in its record). PR 1 also refused prepares with no writes; amendment 1 (D329 PR 2a) lifts that: see `prepare` and `hold_reads` above.

## Callers
`phdb serve` (D329 PR 2) only. In PR 2a:
- **A writing spanning transaction:**
  - Each branch prepares at GroupSync, with the transaction's id as its xid.
  - The home doesn't prepare: its own share commits at the commit point, together with its decision row.
  - Each branch is then decided.
- **A read-only one:** each partition's reads are held with `hold_reads` and kept for a release round. The hold is taken under the partition gate's lease term, and its release confirms the same term. Single-partition commits are unchanged. The only new branch on a commit path is `external.is_some()` in `on_prepared`.
