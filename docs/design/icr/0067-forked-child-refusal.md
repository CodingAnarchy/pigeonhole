# 0067: a database handle used in a forked child refuses every operation

**Status:** Number assigned by the coordinator (2026-10-23; #901, PR #919). A public behaviour change, with no new API and no new error code. It extends to the writer what ICR 0065 (#898, perf287's F1) did for reader processes.

## The rule
**In a forked child, a handle may only be dropped or closed. Everything else fails `Unsupported`, or is inert:** it returns an empty or default value and takes no lock. A mutex that another parent thread held at the moment of the fork stays locked for ever in the child, so a lock taken there could hang.

## Change

**`pigeonhole-engine` (`Engine`, `Snapshot`, `ScanCursor`, `Txn`, the async futures):** in a process other than the one that opened the engine, compared with the cached `pigeonhole_io::sys::process_id` (ICR 0065), every operation fails with `Error::Unsupported("this database was opened in another process (fork); open it again here")`, before it touches a shard, a queue, an io_uring ring or the shared-memory arenas:
- through `check_open`/`check_writable`: commits, submits, transaction commits, flush, compact, backup, shrink, table changes, split;
- `snapshot`, `durable_snapshot`, and the read-latest path (`latest_view`, `get_latest`, `read_row_latest_into`);
- reads through a writer-process snapshot taken before the fork. `Snapshot::check_process` runs once per operation: point and row reads (`get`, `read_row*`, `Txn::read_row_into`), a scan's creation, and each tablet a cursor opens. So a cursor created before the fork fails on its first step. It doesn't run per scan step;
- the async reads (`get_async`, `get_latest_async`, the row futures) fail on their first poll.

**Application-owned shards (`EngineShard`, perf287's M1):**
- `run_once` returns `false` and runs nothing; otherwise the shard state machine would append to the parent's log and apply into the parent's arenas;
- `commit_local` fails `Unsupported` (`check_writable`);
- `wait`, `release` and `set_wakeup` do nothing;
- `next_deadline` and `closed` return `None`;
- `waker` returns a no-op waker.

**Futures and waits created before the fork (M2):** `PendingCommit`, `PendingCheck` and `PendingMaintenance` fail `Unsupported` on their first `poll` or `wait`, as does `Submission::wait` with its spin. Otherwise they would wait for ever for a shard thread that runs only in the parent.

**Accessors are inert (M3):** `metrics()` returns `Metrics::default()`, and each of these returns its empty or zero value:
- `table`, `tables`, `shard_of`;
- `shard_stats`, `row_cache_stats`, `commit_latency_buckets`, `tablet_refusals`, `pack_target`, `admission_inflight`;
- `is_failed` and `failure` (`false`/`None`), and `wait_failed` (`false`, at once);
- `admit` (an `Admit` with no admission);
- `set_default_durability` and `release_memory` do nothing. A release would discard the parent's shared arenas.

`role` and `default_durability` stay: one field and one atomic load. In `pigeonhole`, the supervisor's `stats`, `last_failure`, `gave_up` and `stood_down` are inert too, so `recovery_stats()` returns its default.

**Drops leak, taking no lock (M3).** Each of these registries records the process that created it, and in another process the drop leaves it alone:
- `SeqnoPin` (every writer snapshot's pin; `LiveSeqnos` is also locked by shard threads for GC);
- `ViewPin` (it runs no release callback either);
- an admission `Charge` or a queued `Admit`;
- `LargeValues` (its release would edit the parent's manifest);
- a reader process's `LiveSnapshot` (its `ReaderPins` lock).

**A cursor inside a tablet:** besides the check at each tablet it opens, a scan cursor checks every 256 rows (`FORK_CHECK_ROWS`, out of line). So a cursor that was inside a tablet at the fork stops in the child within 256 rows, before it reads much arena memory the parent may be reusing. A check per row cost the scan shapes 0.5%.

**Close and drop do nothing in a child.**
- `Engine::close` returns `Ok(())`.
- `close_for_reopen` returns `Unsupported`.
- `Drop for Engine` leaks the instance (a clone of its `Arc<Inner>`), and `Drop for EngineShard` leaks its driver: no flush, WAL write, root commit, last-one-out cleanup, lock release or thread join.

**`pigeonhole`:** operations surface `ErrorCode::Unsupported` (already reachable since ICR 0065). The `auto_recover` supervisor (`Supervision`) records its process: in a child, `wait_settled` returns at once and `join` leaks the parent's `JoinHandle` instead of joining it. So `Pigeonhole::close` and drop in a child return at once.


## Why

After `fork(3)` only the forking thread exists. The writer's shard, compaction and I/O threads, and the recovery supervisor, run only in the parent. The child shares the parent's descriptors (on Linux, its open file descriptions) and the shared-memory arenas, which the parent keeps writing and reclaiming. So in the child:
- **any operation** could hang on a thread that isn't there, or read arena memory the parent reuses;
- **a close or drop** ran a full close on the parent's database and joined threads that don't exist.

Python `multiprocessing` with the `fork` start method, and pre-fork servers, do exactly this.

**Why refuse at the entry points:** perf287 asked for it on #901, so that a child fails fast. Making only close a no-op wasn't enough.

**Why leak in close and drop:** that's the only safe option.

**Detection limit:** only `fork(3)` is detected (`pthread_atfork`). `_Fork`, `vfork` and a raw `clone` skip fork handlers; the guide says so.

## Callers

- `pigeonhole-engine`:
  - `engine.rs`: `Inner::opened_in`, `check_process`, `forked`, `FORKED_ENGINE`; `check_open`, `snapshot_of`, `latest_view`, `get_latest`; the read entry points; `close`, `close_for_reopen`; `Drop` for `Engine` and `EngineShard`;
  - `snapshot.rs`: `Snapshot::taken_in`, `check_process`;
  - `read.rs`: `open_next_tablet`;
  - `write.rs`: `Txn::read_row_into`;
  - `nonblocking.rs`: `Reading::poll_read`;
  - `engine.rs` again: `EngineShard::forked` and its driving methods, `Inner::forked` and the inert accessors, `PendingMaintenance::owner`;
  - `shard.rs`: `Shared::opened_in`;
  - `write.rs`: `forked`, used by `PendingCommit`, `PendingCheck` and `Submission`;
  - `snapshot.rs`: the `LiveViews` and `LiveSeqnos` owners, and the `ViewPin`, `SeqnoPin` and `LiveSnapshot` drops;
  - `admission.rs`: `ShardAdmission::owner`, and the `Charge` and `Admit` drops;
  - `large.rs`: `LargeValues::owner`;
  - `read.rs`: `ScanCursor::check_fork`, `FORK_CHECK_ROWS`;
  - `tests/writer_fork.rs` (Unix).
- `pigeonhole`: `recovery.rs` (`Supervision::started_in`, `forked`, `wait_settled`, `join`); `error.rs` (`ErrorCode::Unsupported`'s rustdoc); `tests/fork.rs` (Unix).
- Docs: the concepts guide's platform notes, errors.md, and the changelog.
