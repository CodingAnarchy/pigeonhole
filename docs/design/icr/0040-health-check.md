# 0040: a health check: `is_failed()` and `failure()` report that an instance failed, which shard, and why

**Status:** Approved (owner, via the coordinator, 2026-10-12).

## Change

Additions only, in `pigeonhole-engine` and `pigeonhole` (re-exported). The types are plain data for the C ABI.

```rust
/// Why the instance failed. The numbers are stable.
#[repr(u8)]
#[non_exhaustive]
pub enum FailureCause { Panic = 1, DriverDropped = 2, Io = 3, Internal = 4 }

/// The first failure of an instance.
#[repr(C)]
pub struct Failure { pub shard: u32, pub cause: FailureCause }
impl Failure { pub const MAIN_FILE: u32 = u32::MAX; }

impl Engine /* and pigeonhole::Pigeonhole */ {
    /// One atomic load.
    pub fn is_failed(&self) -> bool;
    /// The first failure; later ones don't replace it.
    pub fn failure(&self) -> Option<Failure>;
}
```

**What records a failure:**
- **`Panic`:** a shard's thread panicked (`report_panicked`).
- **`DriverDropped`:** an application-owned shard's `EngineShard` was dropped before the close finished (`abandon`).
- **`Io`:** a write, sync or checkpoint to the shard's WAL stream failed. Every such site now poisons through `poison(FailureCause::Io)`. With `shard = Failure::MAIN_FILE`, a manifest commit to the main file failed (the pager is poisoned, D58).
- **`Internal`:** the shard couldn't update its in-memory state consistently (an apply, a view publication, a memtable freeze or a slot retirement failed), so it stopped (`poison(FailureCause::Internal)`).

**Two behaviour fixes this needed** (so the recovery the guide describes works):
- **A poisoned shard's close gives up flushing** and is unclean, as a dead shard's does.
  - Before, a shard whose apply failed after its WAL append kept a logged record that no flush could pass, so its checkpoint, and so `close`, waited forever.
  - The WAL keeps the record, and the next open replays it.
- **`OutcomeUnknown` for commits that may have been written** (following ICR 0038, which already decided that `Closed` means not committed):
  - a single-shard commit whose apply failed after its WAL append (was the apply's own error, e.g. `Busy`): its record replays at the next open;
  - a commit whose group's WAL sync failed (was `Io("WAL sync failed; …")`): its records were written, and applied (D227), so a reopen without a power loss replays them;
  - a commit whose group's `write()`, or the sync submission that writes first, failed (was the poison `Io`): a partial write can leave whole records in the file. A failure while *appending*, before any write, keeps the poison `Io`: nothing of the group reached the file. So `Io` for a commit means "refused before anything was written", as the recovery guide says;
  - a commit whose reply the shard dropped unanswered because the instance failed (was `Closed`).
    - A panicking shard drops the members in flight while unwinding, possibly after writing them.
    - It records its failure only at the end of the unwind, so a blocking wait allows up to 100 ms (yielding) for the record before it decides. That's off the commit path: only when a reply is dropped.
    - A poll checks once.
    - Any recorded failure counts, even another shard's or the main file's. After an unrelated failure, a commit dropped unwritten also reads `OutcomeUnknown` where `Closed` would have been exact. That's safe: reading it back finds it missing.
    - The wait sleeps 1 ms between looks rather than spinning.

## Why

An application learns of a failed shard today only when a commit fails, and then can't tell a failed instance from a closed one. A service wants a health endpoint, and a supervisor wants to close and reopen as soon as a shard fails. Both need a cheap check and the cause: `Panic` and `Internal` are bugs worth reporting, `Io` is usually the disk.

## Callers

- `pigeonhole-engine`:
  - `health.rs` (the types and the packed first-failure record);
  - `shard.rs` (`poison`, `report_panicked`, `abandon`, the close's give-up, the apply-failure outcome);
  - `manifest.rs` (pager poisoning);
  - `engine.rs` (`is_failed`, `failure`);
  - `write.rs` (`dropped_reply`, `dropped_reply_now`);
  - the `fail_next_apply` hook docs.
- `pigeonhole`: `Pigeonhole::is_failed`, `failure`, and the re-exports.
- Tests: `engine/tests/health.rs` (one per cause, plus the main file and "the first failure stays"); `large_values.rs` now expects `OutcomeUnknown`.
- Docs: the user guide's new "Recovering from a failed shard" page (doctested), linked from the errors page, the agent reference and getting-started; changelog.
