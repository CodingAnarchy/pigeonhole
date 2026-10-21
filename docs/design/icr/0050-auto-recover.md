# 0050: automatic recovery in place: `Options::auto_recover`, `Recovery`, `recovery_stats()`, and the engine's writer-lock handover

**Status:** Approved (owner, via the coordinator, 2026-10-21; D265, #620). Lands in four PRs: the engine handover, the `Db` swap plumbing, the option with its supervisor and stats, the model-check arm. This file names the whole delta; each PR's body says which part it brings.

## Change

### `pigeonhole` (additive, semver minor)

```rust
/// Which failures the database recovers from in place (D265). Numbers are stable.
#[repr(u8)]
#[non_exhaustive]
pub enum Recovery {
    /// Never: a failed instance stays failed until it is closed and reopened (the default).
    Off = 0,
    /// I/O failures: a shard whose WAL write or sync failed, or a failed manifest commit
    /// (`Failure::MAIN_FILE`).
    Io = 1,
    /// Also a shard that panicked or stopped after an internal error.
    Any = 2,
}

impl Options {
    /// Default `Recovery::Off`. Refused by `Pigeonhole::open_application_owned` with
    /// `InvalidArgument` unless `Off`.
    pub fn auto_recover(self, recovery: Recovery) -> Self;
}

/// What the handle's recoveries did so far (D265). Plain data for the C ABI.
#[repr(C)]
#[non_exhaustive]
pub struct RecoveryStats {
    /// Opens of the engine behind this handle: 1 at open, +1 per recovery. Lets a monitor see
    /// that `engine_metrics()` restarted from 0.
    pub generation: u64,
    /// Recoveries that swapped a reopened engine in.
    pub recoveries: u64,
    /// Recovery stopped: the budget ran out or a reopen failed for good. The handle stays
    /// failed as without the option.
    pub gave_up: bool,
    /// The last failure recovered from or given up on: the shard (`Failure::MAIN_FILE` for
    /// the main file) and the cause; `last_failure_cause` is 0 while there was none.
    pub last_failure_shard: u32,
    pub last_failure_cause: u8,
    /// How long the last recovery took, from the failure to the swap, in nanoseconds.
    pub last_recovery_nanos: u64,
}

impl Pigeonhole {
    pub fn recovery_stats(&self) -> RecoveryStats;
}
```

Existing codes gain documented situations: `Busy` (an operation waited for a recovery past `write_stall_timeout`), `SnapshotExpired` (a writer-process `Snapshot`, `Transaction`, scan or row iterator from before a recovery), `Closed` (recovery gave up; commits are refused as on a failed instance today), `InvalidArgument` (`auto_recover` other than `Off` with `open_application_owned`). `Snapshot` gains a private generation field.

### `pigeonhole-engine` (additive, `#[doc(hidden)]`: the public crate is a separate crate, so the handover cannot be crate-private; not a stable API)

```rust
/// The writer lock, the presence, the handle they live on and the path they belong to.
/// Dropping it releases the locks but runs no last-one-out cleanup: the WAL files and the
/// shm region stay, as after a crashed writer, and the next open replays and rebuilds.
pub struct Handover { /* private */ }

impl Engine {
    /// Closes as `close` does, keeping the locks: the close's result, and the handover, or
    /// `None` when this call did not start the close (a close was under way: it waits for it,
    /// and that close releases or hands over the locks) or the final close had already released them
    /// (every shard dead); then fall back to `open`. Engine-owned writers only
    /// (`InvalidArgument` otherwise).
    pub fn close_for_reopen(&self) -> (Result<()>, Option<Handover>);
    /// `open` on a handover: skips the local-filesystem check, `WriterLock::acquire` and
    /// `Presence::acquire`; everything else as `open`. `path` must be the handover's file
    /// (`InvalidArgument`). On an error the handover comes back with it, the locks still
    /// held, so a retry (D265's budget) never opens the window.
    pub fn open_with(path: &Path, options: EngineOptions, handover: Handover)
        -> Result<Arc<Engine>, (Error, Handover)>;
    /// Blocks until the instance records a failure (`true`), or until `deadline` or its close
    /// (`false`); on a reader handle, which never fails, until its close.
    pub fn wait_failed(&self, deadline: Instant) -> bool;
    /// Releases what a closed writer still holds that is large (D265's memory release, PR
    /// 2a): the last view (its frozen memtables, open SSTs and the blocks they pin), the
    /// block and row caches, and on Linux the memtable arenas' pages, the latter once the
    /// reopen superseded the region and no in-process view from before the close lives
    /// (then when the last goes). `close_for_reopen` runs it; call it again after `open_with`
    /// succeeded. A no-op on an open instance, a reader or an application-owned engine.
    pub fn release_memory(&self);
}
```

Internal: `Locks` carries the handle (`file`) and moves into the engine as the open's last step, so a failed open leaves them with the caller; `Shared::keep_locks` is set only by the call that starts the close and makes `final_close` leave the locks in place, skip the presence upgrade and remove neither the WAL files nor the shm region (as after a crashed writer); `close_reader` notifies the failure wait. The close waits, after the runtime's threads are joined, for every WAL stream's submitted I/O still in flight on the backend's own threads, and only then releases the writer lock or leaves it for the handover (`Shared::wal_io`, `IoTracker::wait_idle`, `Shared::release_after_io`, `Shared::close_done`; #824): a poisoned shard's final sync waits for nothing, and a spare preparation's zero-fill writes and a held header's chain are never waited for, so without it a reopen in this process could start a segment in a slot such a write then lands on. On a device that never answers the close blocks: the supervisor runs it under the budget's deadline and never opens beside a close that has not returned.

The memory release (PR 2a, for the per-handle cached engine of #796: a stale handle keeps the old engine alive): `Shared::release_memory` runs only once `closed`, publishes an empty view under `view_lock` (the last view's memtable readers, SST set and pinned blocks go with it unless a live view holds them), clears the caches (`BlockCache::clear`, `RowCache::clear`: new in `pigeonhole-cache`, with the index's spare capacity returned) and discards the arenas (`ShmRegion::discard_arenas` → `SharedRegion::discard`: `madvise(MADV_REMOVE)` on Linux, so the pages and their backing store go while the mapping stays valid; an error on macOS and Windows, or from a `shm_dir` file system without hole punching, recorded as refused; the heap stand-in zeroes) under `LiveViews::if_none_live`, and only once `ShmRegion::is_superseded` (the reopen built the next generation, or the region is in-memory): a reader process attached to the superseded generation fails its reads with `SnapshotExpired` after reading (D148), and the memtable reader never faults on zeroed pages. `LiveViews::on_all_released` re-runs the release when the last view goes. `Shared::arena_discard` records the outcome. A stale handle then keeps about 5–10 MiB on Linux: the mapping's header, view-buffer and reader-slot pages, the pager's descriptors and extent maps, the caches' tables, the per-shard metrics and slots, the options and the manifest writer; on macOS the arenas' resident pages too, until the last handle and snapshot drop.

### Not changed
No format, WAL or shm layout change. `pigeonhole-shm` gains `ShmRegion::{discard_arenas, arena_resident_bytes, is_superseded}` (PR 2a; the layout is unchanged). Reader processes keep D148's contract.

## Why

#620: a failed shard ends the instance today, and the application must close and reopen (the recovery guide). The owner wants an opt-in that does it in place, so a service sees a short pause and typed errors for the commits in flight instead of a dead handle. The design and its reasoning are on #620 (fable252, 2026-10-21) and in D265.

The handover is the piece the engine must provide: a second in-process open fails with `WriterLocked` while the old handle holds the writer byte (Linux OFD locks are per open file description; the macOS/BSD registry arbitrates handles of one process), and releasing the lock first opens a window in which another process's writer can take the file and in which a reader's last-one-out close removes the shm region.

## Callers

- `pigeonhole-engine`: `engine.rs` (`open_writer`'s steps 1 and 3 take the handover, `open_with`, `close_for_reopen`, `wait_failed`, `Handover`; PR 2a: `release_memory`, the `on_all_released` hook at open), `shard.rs` (`Locks::file`, `Shared::keep_locks`, `final_close`; PR 2a: `Shared::{release_memory, try_discard_arenas, arena_discard}`), `snapshot.rs` (PR 2a: `LiveViews::{on_all_released, if_none_live}`), `row_cache.rs` (PR 2a: `RowCaches::{clear, usage}`), `engine/hooks.rs` (PR 2a: `memory_release`, `arena_resident_bytes`, `MemoryRelease`, `ArenaDiscard`), `lib.rs` (the hidden exports); `tests/handover.rs`, `tests/handover_real.rs`; `tests/gate` (`Gate::heal`).
- PR 2a's crates below the engine: `pigeonhole-cache` (`BlockCache::clear`, `RowCache::{clear, usage}`, `s3fifo::Shard::clear`), `pigeonhole-io` (`SharedRegion::{discard, resident_bytes}`, `os::{discard, resident_bytes}`), `pigeonhole-shm` (`ShmRegion::{discard_arenas, arena_resident_bytes, is_superseded}`).
- `pigeonhole` (PRs 2–4): `db.rs` (`Db::engine()`, the generation, the supervisor, `recovery_stats`), `options.rs` (`auto_recover`), `lib.rs` (re-exports), `read.rs`/`write.rs`/`table.rs`/`nonblocking.rs` (every `db.engine` use through `Db::engine()`); `tests/recovery.rs`, `tests/model.rs`.
- Docs: `docs/guide/recovery.md` ("Automatic recovery"), `errors.md`, `agent-reference.md`, D265, `concurrency.md` (the `Shared::keep_locks` row, later an appendix for `pigeonhole/src/db.rs`), changelog.
