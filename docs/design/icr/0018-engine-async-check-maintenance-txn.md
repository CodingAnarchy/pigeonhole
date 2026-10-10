# 0018: async conditional commits, maintenance and transaction reads in `pigeonhole-engine`

**Status:** Approved (coordinator, 2026-10-10; #42, D196). Implemented in #42's PR 5.

## Change

`pigeonhole-engine`, all additive:

```rust
impl Engine {
    /// `check_and_mutate` without waiting; never blocks, so a shard's driver may call it.
    pub fn submit_check_and_mutate(
        &self,
        table: TableId,
        row: &[u8],
        predicate: &Predicate,
        batch: WriteBatch,
        durability: Option<Durability>,
    ) -> Result<PendingCheck>;
    /// `flush` without waiting.
    pub fn submit_flush(&self) -> Result<PendingMaintenance>;
    /// `compact` without waiting.
    pub fn submit_compact(&self, table: Option<TableId>) -> Result<PendingMaintenance>;
}

/// A submitted check-and-mutate: `wait()` blocks; `Future<Output = Result<(bool, Option<CommitInfo>)>>`.
pub struct PendingCheck;

/// Already public under `test-hooks` (`flush_pending`, `compact_pending`, tablet ops). Now
/// always exported and documented; its `Future` impl is no longer behind `test-hooks`.
pub struct PendingMaintenance; // Future<Output = Result<()>>

impl Txn {
    /// `get` as a future: records the read at the call, reads at the transaction's snapshot.
    pub fn get_async(&mut self, table: TableId, family: FamilyId, row: &[u8], qualifier: &[u8]) -> GetFuture;
}
```

## Why

D196, as the owner decided: every data operation has an async form, including `commit_if` and `Transaction::get`, and so do `flush` and `compact`. `backup` and `shrink` stay sync-only, and so do open, close and the schema calls. Each blocking call already waits on a handle the engine builds:
- `check_and_mutate` submits a `CommitReq` with a `Reply::Check`, then blocks in `wait_reply` and `wait_visible`.
- `flush` and `compact` build a `PendingMaintenance` and block in `wait`.

The change splits each call at that point, as `Txn::submit` split `Txn::commit` in PR 1.

## Semantics

- **`PendingCheck`** resolves as `check_and_mutate` returns. It polls the shard's reply; then, if the batch applied, it waits for visibility on the global watermark, registering the task's waker as `PendingCommit` does (D19). So no thread spins or parks. `wait()` keeps the blocking path's refusal on a thread that drives a shard (D88), and `Engine::check_and_mutate` still refuses before submitting there.
- **`PendingMaintenance`** resolves once every shard has replied, to every round of a full compaction, with the last failure if any. The future submits each further compaction round when it sees the previous one done, as `wait` does. Its `Future` impl is the one the test harnesses already use, moved from `engine::hooks`.
- **Dropping** any of them does not cancel the operation. A dropped check still lands or fails atomically.
- **`Txn::get_async`** records the `(table, row, family)` read when it's called, then returns the same `GetFuture` as `Engine::get_async` at the transaction's snapshot. A read future that's recorded but dropped unpolled still counts at validation. That's conservative: it can add a conflict, never miss one.
- **Sync paths are unchanged.** `check_and_mutate` is now `submit_check_and_mutate(..)?.wait()`, the same steps in the same order. `flush` and `compact` still call `wait`. None of these is on an instruction-measured shape.

## Callers

- `pigeonhole-engine`: `engine.rs` (the split, `PendingMaintenance`'s `Future`), `write.rs` (`PendingCheck`, `Txn::get_async`), `engine/hooks.rs` (the moved impl), `lib.rs` (exports).
- `pigeonhole`: `RowMutation::commit_if_async` → `nonblocking::CheckFuture`; `Pigeonhole::flush_async` and `compact_async` → `nonblocking::MaintenanceFuture`; `Transaction::get_async` → `nonblocking::GetFuture`.
