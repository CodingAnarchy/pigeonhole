# 0113: `RowMutation::commit_returning`, counters that return their values after the commit

**Status:** Approved (number from the coordinator; the owner put #1440 in 1.1.0 on 2026-11-03; design D314, reviewed by simp724 and perf287 on #1440).

## The change
- **pigeonhole** (semver, additive):
  - `RowMutation::commit_returning(self) -> Result<Incremented>`: commits like `commit`, and returns each counter cell the mutation wrote (`incr`, `incr_at`, `put_i64`, `put_i64_at`, `delete_cell` in a counter family), valued as a get of that version at the commit's seqno.
  - With `async`: `RowMutation::commit_returning_async(self) -> ReturningFuture` (`nonblocking::ReturningFuture`, `Output = Result<Incremented>`).
  - `Incremented` (`#[non_exhaustive]`): `pub info: CommitInfo`.
    - `value(family, qualifier)` and `value_at(family, qualifier, ts)` return `Result<Option<i64>>` per cell. An `Err` there is a read that failed after the commit applied, and the commit succeeded; `InvalidArgument` names a cell the commit didn't write.
    - `iter()` yields `(family, qualifier, ts, Result<Option<i64>>)` in the order the mutation first wrote each cell.
- **pigeonhole-engine** (internal, 2.x line, additive):
  - `Engine::commit_returning(batch, durability) -> Result<(CommitInfo, Vec<Returned>)>`.
  - `Engine::submit_returning(batch, durability) -> Result<PendingReturning>`. `PendingReturning` is a `Future` with `wait()`.
  - `Returned` (`#[non_exhaustive]`: `family`, `qualifier`, `ts`, `value: Result<Option<i64>>`).
  - The hidden `Engine::returning_folds` (statistics, for tests and benchmarks).

## Why
BigTable's `ReadModifyWriteRow` returns the cells after the increment, and applications ported from it depend on that: sequence numbers, ID allocation, workflow step counters. A read after the commit races with concurrent increments. `transact` with `get` + `put_i64` is correct but pays for validation, and turns a contended counter into `Conflict` retries. The row's shard already serializes the row's commits, so it can fold the cell right after the apply and hand the value back (D314).

## Callers changed
None: additive. The Python and Ruby bindings (#696, #697) add their surfaces in a later PR (cli692).
