# 0060: write-free transaction commits; `Pigeonhole::transact` and `RetryPolicy`

**Status:** Number assigned by the coordinator (2026-10-21; #694, [plan](https://github.com/CodingAnarchy/pigeonhole/issues/694#issuecomment-6330551273)). The write-free commit lands in #694 PR 2; `transact` and `RetryPolicy` in PR 3, which completes this file.

## Change, part 1: write-free commits (PR 2)

- **`pigeonhole::Transaction::commit` / `commit_with` / `commit_async`:** a transaction with no writes commits at once. There's no validation, no shard round and no I/O, and it never fails with `Conflict`.
  - Its `CommitInfo` is `{ seqno: <the transaction's snapshot seqno>, durability: Durability::None }`.
  - A closed database still gives `Closed`.
- **`pigeonhole_engine::Txn::commit` / `submit` / `submit_async`:** the same at the engine level; `submit` returns an already-resolved `PendingCommit`.

## Why

A write-free transaction's reads all come from one consistent snapshot, so it's serializable at that snapshot whatever committed since. Validating it could only abort it for nothing.

**Why that holds here:** every writing transaction validates its reads (OCC, D249/D263), so writers are serializable in seqno order, and a snapshot is a prefix of that order (D19, D227). The classic read-only anomaly (Fekete et al.) needs snapshot-isolation writers, which allow write skew; validated writers don't. If writers ever ran under plain snapshot isolation, skipping validation for readers would stop being safe.

It writes nothing, so it has no seqno of its own and nothing to make durable. It names the snapshot's seqno, which is the point it serializes at, and `Durability::None`.

**Behaviour changes:**
- **What `Ok` means.** A read-only commit used to validate its reads at commit time, so `Ok` meant they were still current then, and some applications use that as a check ("read X, commit to confirm X still holds"). Now `Ok` means consistent as of the snapshot. To keep the old meaning, write something in the transaction (it's then validated) or use `check_and_mutate` (`RowMutation::commit_if`) on the cell.
- **`CommitInfo.seqno` isn't unique.** A write-free commit's seqno is its snapshot point: a writer's commit seqno, or 0 on an empty store, which other commits may share. It isn't an id of its own. `CommitInfo`'s doc says so.
- **Durability:**
- **Before:** a read-only transaction at `Sync` wrote validation records on every shard it read, and their sync made everything it had read durable. That was a side effect, never documented.
- **Now:** its reads are visible but not necessarily durable, as for any snapshot (D227). An application that needs durable reads uses `durable_snapshot`.

## Callers (part 1)

- `pigeonhole-engine`: `write.rs` (`Txn::write_free`, `resolved`, the commit paths); `engine.rs` (`check_open` is now `pub(crate)`); `tests/api.rs`.
- `pigeonhole`: the `Transaction::commit` docs.
- Docs: getting-started, and the changelog.

## Change, part 2: `transact` and `RetryPolicy` (PR 3)

- **`Pigeonhole::transact<T, E>(&self, policy: &RetryPolicy, body: impl FnMut(&mut Transaction) -> Result<T, E>) -> Result<(T, CommitInfo), E> where E: From<Error>`:** each attempt begins a fresh transaction, runs `body`, and commits, at `policy`'s durability or the writer's default. It retries:
  - a commit that fails with an error the policy retries (`Conflict`, `SnapshotExpired`, and `Busy` if asked);
  - a `body` error, when one of the transaction's own reads failed with such an error, which the transaction records (`Transaction::failed_read`, crate-private).

  Any other `body` error is returned as is, with nothing committed: that's how an application gives up with its own error type. Pauses are exponential with full jitter, cut short at the deadline. Each retry counts in `EngineMetrics::transaction_retries` (ICR 0061).
- **`RetryPolicy`** (`Clone`, `Debug`, `Default`; private fields, builder methods):
  - `max_attempts(n)`, `unlimited_attempts()` (the default);
  - `deadline(d)` (default 30 s, the default write-stall timeout), `no_deadline()`;
  - `backoff(base, cap)` (defaults 100 µs and 100 ms);
  - `retry_busy(bool)` (default off);
  - `durability(d)`;
  - `retries(&Error) -> bool`, `retries_code(ErrorCode) -> bool`, `delay(attempt) -> Duration`, and `next_pause(code, attempt, elapsed) -> Option<Duration>` (the whole decision: `None` to give up, else the pause cut at the deadline).

### Rust-only convenience, C-ABI reachable (coordinator's condition, AGENTS.md)

`transact` takes a closure and a generic error type, so it's a Rust-only convenience and won't cross the C ABI.
- **Everything it decides is reachable without it.** `RetryPolicy` is a plain value (numbers, durations, flags; no closure or trait object). The retryable-error classification is `retries_code(ErrorCode)`. The loop's decision is `next_pause(code, attempt, elapsed)`, and `transact` itself uses exactly these.
- **So a caller can run the same loop by hand.** An async caller with its own timer, or a future C ABI, uses `Pigeonhole::transaction()`, its own reads and writes, `commit`, then `next_pause` with the commit's (or a read's) error code.
- **There's no async `transact`,** because the crate has no timer.

### A transaction refused by the failed engine during a recovery (refines ICR 0050)

ICR 0050 gives a commit refused by the failed engine while a recovery runs `Busy` ("not committed, retry on the recovered one"). **For a transaction's commit it's `SnapshotExpired`** instead, with "not committed" kept in the message (perf287's S1 on #877).
- **The reason:** the transaction's snapshot expired with the swap, exactly as its reads would have. `SnapshotExpired` is what `transact` retries by default, so surviving a recovery doesn't depend on whether the read or the commit met the swap.
- **Unchanged:** a recovery that doesn't finish within `write_stall_timeout` is still `Busy`.
- **Not covered:** the async transaction commit doesn't wait for the swap, as before.
- **Test:** `tests/recovery.rs::transact_survives_a_recovery_with_the_default_policy`.

**Smaller points:**
- Only `Transaction::get` records a failed read for `transact`'s retry decision; `get_async` doesn't. ICR 0062's `get_row` will record it.
- The deadline bounds when attempts start; a running one finishes.

### Why the generic error

`pigeonhole::Error` can't be built by applications. A `body` returning `pigeonhole::Result` couldn't give up with the application's own reason. Generic `E: From<Error>` lets `?` work on pigeonhole calls inside the body and lets the application return its own error. The transaction remembering its failed read keeps "retry this" separate from "the application gave up". A body may need `Ok::<_, pigeonhole::Error>(…)` for inference.

## Callers (part 2)

- `pigeonhole`:
  - `retry.rs` (new);
  - `lib.rs` (`RetryPolicy` exported);
  - `write.rs` (`Transaction::failed_read`, and counting in `finish`);
  - `nonblocking.rs` (the async transaction commit counts);
  - `db.rs` (`txn_counts`, `engine_metrics`);
  - `metrics.rs`;
  - `tests/transact.rs`.
- Docs: getting-started, errors.md, the agent reference, and the changelog.
