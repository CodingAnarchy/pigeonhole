# 0036: an async submission is a pending commit that may still wait for room (`AsyncSubmit` removed)

**Status:** Approved (owner, via the coordinator, 2026-10-12; #523 option 2).

## Change

`pigeonhole-engine`. This breaks callers in this workspace only: none of these items is in a published release (v0.2.1 has none of them).

```rust
// Removed:
pub enum AsyncSubmit<P, B> { Submitted(P), Wait(Box<(B, Admit)>) }

impl Engine {
    // Was: Result<AsyncSubmit<PendingCommit, WriteBatch>>
    pub fn submit_async(&self, batch: WriteBatch, durability: Option<Durability>)
        -> Result<PendingCommit>;
    // Was: Result<AsyncSubmit<PendingCheck, WriteBatch>>
    pub fn submit_check_and_mutate_async(&self, table: TableId, row: &[u8],
        predicate: &Predicate, batch: WriteBatch, durability: Option<Durability>)
        -> Result<PendingCheck>;
}

impl Txn {
    // Was: Result<AsyncSubmit<PendingCommit, Txn>>
    pub fn submit_async(self, durability: Option<Durability>) -> Result<PendingCommit>;
}
```

**The waiting pending commit.** A `PendingCommit` or `PendingCheck` returned while its shard has no room (D210) holds, boxed, its admission and the submission to run. It submits itself once admitted:
- when polled (the async path);
- or in `wait()`, which parks until admitted (out of line; tickets never wait for room).

The admission's charge goes to the submitted commit, as before. Dropped while it still waits, it writes nothing and leaves the queue, as before.

**The blocking path.** Inside the engine, a submitted commit is now a crate-private `Submission`: the reply waiter, the engine, and the spin flag. The engine's own blocking commits wait on it directly: `Engine::commit`, `Txn::commit`, and the inline path that builds one. `PendingCommit` wraps it with the async-only state (the resolved-but-not-yet-visible commit, and the waiting submission). Blocking commits no longer carry, initialize or drop either.

`pigeonhole`: no public change. `CommitFuture` and `CheckFuture` lose their own waiting state (`Waiting`, `from_async`, the boxed submission closure), and `AsyncCommit`/`Deferred` go. Each future is now the engine's pending commit plus its error context. `Transaction::submit`, used only by the old deferred closure, is removed.

## Why

#523: D210's admission cost an uncontended `commit_async` +240 instructions. About 110 of them were plumbing for the rare "wait before submitting" case:
- `Engine::submit_async`'s `Result<AsyncSubmit<..>>` layer;
- the public future's extra waiting state (`from_async`, a three-way `poll`, drop glue).

The submitted commit moved through three enums on its way to the caller.

Two other shapes were measured first and dropped:
- **The waiting state in `PendingCommit`, with the blocking path using it too:** commit-async −50, but every blocking commit +17 (the new field's drop glue, the check in `wait`, and codegen around `Engine::commit`).
- **A separate engine future `AsyncPending<P>` (submitted, or waiting) wrapping an unchanged `PendingCommit`:** blocking commits unchanged, but commit-async +9. It moved the enum match from construction time to every poll and drop.

Option 1 of #523 (a charge carried in the request) stays declined.

## Callers

- `pigeonhole-engine`:
  - `Inner::submit` returns a `Submission`;
  - `Engine::submit` and `Txn::submit` wrap it in a `PendingCommit`;
  - `Engine::commit` and `Txn::commit` wait on the `Submission`;
  - `EngineShard::commit_local` wraps it;
  - `Admit::attach` takes the charge to `pending.sub.waiter`.
- `pigeonhole` write.rs: `Builder::submit_async`, `RowMutation::submit_if_async`, `WriteBatch::submit_async`, `Transaction::submit_async`.
- `pigeonhole` nonblocking.rs: `CommitFuture`, `CheckFuture`.
- `pigeonhole-engine` tests: admission.rs. New: `a_waiting_async_submission_submits_itself_once_admitted` covers a commit, a conditional commit and a transaction that wait, then submit once admitted, and a blocking `wait()` on a waiting commit.

## Cost

Scratch callgrind, 4 runs each, against main a462d5d:

| shape | main | this change |
|---|---|---|
| commit-async | 9890–9892 | 9846–9847 (−43) |
| commit-one | 9767–9771 | 9761–9765 (about −5) |
| commit-at | 10701–10704 | 10699–10700 (−4) |
| commit-overwrite | 11815–11819 | 11811–11812 (−4) |

The blocking shapes gain a little because a blocking commit no longer initializes the async `resolved` field.
