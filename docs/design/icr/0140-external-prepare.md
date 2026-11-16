# 0140: prepared transactions with an external xid (D329 engine PR 1)

**Status:** Approved.
- Numbers are from the coordinator (`.coord/numbers.md`): WAL record types 5 and 6, and `incompat` bit 2.
- The D329 design was reviewed by audit120 and cli692, and signed off by the owner.
- perf2 reviewed this ICR's draft as engine reviewer (E1 to E3, Q1 to Q4). Bit 2 is sticky, which supersedes the draft's clearing.
- It amends nothing in D294: bit 2 is sticky like bit 0.

## Why
D329 commits a transaction across partitions on different nodes with two-phase commit. Each partition's branch must prepare durably, hold the rows it read and wrote, survive a crash or a close *in doubt*, and later be committed or aborted by its caller.

The engine's cross-shard 2PC (D263) does all of that inside one commit call, but recovery never leaves a share in doubt: a PREPARE without its decision is dropped. This ICR lets a caller hold the decision.

## API (`pigeonhole`, behind `__unstable-txn-prepare`)
The feature is hidden from the docs and outside the semver promise, as `__unstable-replication` is. `phdb serve` is its one caller.

- **`Transaction::prepare(self, xid, durability) -> Result<Prepared>`:**
  - It validates the read set and logs the shares, through the PREPARE path even for one shard, and returns once they meet `durability`. Callers ask for GroupSync at least.
  - The share then holds its rows, as an undecided D263 share does, until decided.
  - A transaction with no reads and no writes is `InvalidArgument`.
  - A read-only prepare (reads, no writes) validates and holds its reads in memory and logs nothing. `Prepared::release()` answers whether that hold was still in place (`true`) or lost to a reopen, a close or a limit (`false`).
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
- **Holding the decision:** `PreparedShare` gains its external fields (xid, reads), and the local coordinator's `Coord` holds its decision for `decide`. A registry, `Shared::external` (a leaf mutex: xid → id, coordinator, participants, prepared-at, WAL bytes), serves `decide`, `in_doubt` and the limits. It's a concurrency.md row, with loom models of two `decide`s, and of a `decide` racing `prepare`'s insert.
- **Recovery:**
  - Type 5 is stashed with its reads.
  - A Decision with every participant present applies. An Abort drops.
  - Neither, with every participant present: the share is in doubt. Its `prepared` entries, `pending_rows`/`pending_reads`, `Coord` and registry entry are reinstalled before any write is admitted, and `needed()` keeps its records.
  - A participant missing: dropped as aborted. That is only a torn tail below GroupSync, a disk that lies, which the guide states.
  - `log_replayed` no longer aborts an in-doubt type 5. `flush_recovered` checkpoints only to the first in-doubt record. A layout change with shares in doubt is refused at open.
  - `max_seqno` counts type 5's id and type 6's `abort_seqno` (E2).
- **Close and moves:**
  - A close doesn't wait for external shares; they stay in the WAL.
  - `nothing_in_flight` ignores them.
  - Packing (D217) skips tablets an external share touches.
- **Visibility:** an undecided share holds neither the visible watermark nor the feed's WRITTEN/SYNCED. Its id is reserved, never held, and its apply seqno is reserved at the decision.
- **The abort's seqno (E1),** and its four conditions:
  1. The record is in the WAL before the seqno can be visible.
  2. Feeds and replicas pass over it with no event, while delivered positions and tokens move past.
  3. It is an ordinary empty group for watermarks, with no memtable entry and no counted write.
  4. Recovery counts it and never reapplies it.

## The feed reader (E3)
- A held type 5 is kept as its position only, not its batch, and read again when its Decision arrives.
- It sits outside the read-ahead budget and `top_held`, so it never triggers an eviction or a cutoff.
- The idle shortcut treats it as settled, since an abort's own seqno moves the frontier.
- Type 6 drops the hold.

## Callers
`phdb serve` (D329 PR 2) only. Single-partition commits are unchanged. The only new branch on a commit path is `external.is_some()` in `on_prepared`.
