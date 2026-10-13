# 0038: `OutcomeUnknown`, a commit whose outcome the next open decides (`Closed` means not committed)

**Status:** Approved (owner, via the coordinator, 2026-10-12; D227, #601).

## Change

`pigeonhole-engine` and `pigeonhole`. Each gets one new variant. Both enums are `#[non_exhaustive]`, so this is a minor, semver-compatible change.

```rust
// pigeonhole-engine
pub enum Error {
    // ...
    /// A commit's outcome is not known in this process; the next open decides it from
    /// the WAL.
    OutcomeUnknown,
}

// pigeonhole (stable, C-ABI friendly)
#[repr(u32)]
pub enum ErrorCode {
    // ...
    OutcomeUnknown = 28,
}
```

**When a commit fails with it:** a shard died (a panic, or an application-owned shard dropped) or the database closed while the commit was in flight, after its records may have reached the WAL. Exactly these cases:
- a cross-shard commit whose coordinator died;
- a cross-shard commit decided to commit that a participant did not apply (its close gave up, or its apply failed) or never answered;
- a commit in a group whose sync had not resolved when its shard stopped;
- a commit acknowledged by its shard whose wait for visibility ended (the close finished, or a shard died). This was `Closed` before.

**What `Closed` means now:** a commit that fails with `Closed` was not committed: it was never written, or it was decided abort. A cross-shard commit still undecided when a close finds a shard dead is decided abort.

**How the engine answers.** A shard that stops (its close reports, or it dies) answers each unsettled commit before reporting (`answer_unsettled`):
- a written group → `OutcomeUnknown`;
- a coordinated commit decided to commit → `OutcomeUnknown`;
- one not decided to commit → `Closed`.

A commit dropped before it was written still reads as `Closed` through its dropped reply.

## Why

Before this, `Closed` covered both "not committed" and "committed in the WAL, never answered", and a caller could not tell which. ctr274's review of #601 found the two mixed in the dead-shard close. The owner chose a distinct error over documenting the exception. A caller that gets `OutcomeUnknown` reopens and reads; an idempotent write can simply be written again.

## Callers

- `pigeonhole-engine`:
  - `error.rs` (variant, `Display`, `duplicate`);
  - `shard.rs`: `on_applied` (a commit decision with a share not applied keeps its seqno held and answers `OutcomeUnknown`), `on_decide` (a gave-up participant answers not applied), `answer_unsettled` (close report, `abandon`, `report_panicked`);
  - `write.rs`: `wait_visible`, `wait_visible_spinning`, and the `PendingCommit`/`PendingCheck` polls once visibility ended.
- `pigeonhole`: `ErrorCode::OutcomeUnknown = 28` and its mapping. `tests/api.rs` lists the code and the mapping.
- Tests whose waits now end with `OutcomeUnknown`: `blocking_waits` (`a_visibility_wait_ends_when_the_shard_holding_it_dies`, `a_close_ends_when_a_participant_dies_before_it_applied`, `a_decision_after_a_participants_close_reported_is_not_applied`) and `deferred_waits` (`a_visibility_wait_ends_with_outcome_unknown_when_the_shard_holding_it_dies_mid_io`).
- Docs: errors guide (code 28, `Closed`), agent reference, D227, changelog.
