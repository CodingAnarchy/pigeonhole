# 0060: write-free transaction commits; `Pigeonhole::transact` and `RetryPolicy`

**Status:** Number assigned by the coordinator (2026-10-21; #694, [plan](https://github.com/CodingAnarchy/pigeonhole/issues/694#issuecomment-6330551273)). The write-free commit lands in #694 PR 2; `transact` and `RetryPolicy` in PR 3, which completes this file.

## Change, part 1: write-free commits (PR 2)

- **`pigeonhole::Transaction::commit` / `commit_with` / `commit_async`:** a transaction with no writes commits at once. There's no validation, no shard round and no I/O, and it never fails with `Conflict`.
  - Its `CommitInfo` is `{ seqno: <the transaction's snapshot seqno>, durability: Durability::None }`.
  - A closed database still gives `Closed`.
- **`pigeonhole_engine::Txn::commit` / `submit` / `submit_async`:** the same at the engine level; `submit` returns an already-resolved `PendingCommit`.

## Why

A write-free transaction's reads all come from one consistent snapshot, so it's serializable at that snapshot whatever committed since. Validating it could only abort it for nothing.

It writes nothing, so it has no seqno of its own and nothing to make durable. It names the snapshot's seqno, which is the point it serializes at, and `Durability::None`.

**Behaviour change:**
- **Before:** a read-only transaction at `Sync` wrote validation records on every shard it read, and their sync made everything it had read durable. That was a side effect, never documented.
- **Now:** its reads are visible but not necessarily durable, as for any snapshot (D227). An application that needs durable reads uses `durable_snapshot`.

## Callers (part 1)

- `pigeonhole-engine`: `write.rs` (`Txn::write_free`, `resolved`, the commit paths); `engine.rs` (`check_open` is now `pub(crate)`); `tests/api.rs`.
- `pigeonhole`: the `Transaction::commit` docs.
- Docs: getting-started, and the changelog.
