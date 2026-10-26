# Recovering from a failed shard

> **Status:** this page describes `main`; `is_failed`, `failure` and `ErrorCode::OutcomeUnknown` are new in 0.3.0.

A Pigeonhole instance runs one thread per shard. A shard can fail on its own:
- its thread panics (a bug);
- in application-owned mode, its `Shard` driver is dropped before `close`;
- a write or sync to its write-ahead log fails (a disk error, a full disk);
- its in-memory state could not be updated consistently, so it stops rather than build on a state it cannot trust.

The whole instance can also fail when a manifest commit to the main file fails.

What a failure refuses depends on what failed:
- **A shard that died** (a panic, a dropped driver, a failed log sync, or a cross-shard commit a shard could not apply): every shard refuses new commits with `Closed`.
- **A shard that stopped for another reason** (a failed log write, an internal error) refuses its own commits.
- **After a failed manifest commit,** flushes stop, so commits fail once they need memtable room.

Reads keep working in every case. Nothing already durable is lost: the log and the main file hold it, and the next open replays what the log holds. Recovery is therefore: notice, close, reopen, and settle the commits that were in flight. With `Options::auto_recover` on, the handle does the close and reopen itself, in place: see [Automatic recovery](#automatic-recovery) below. The manual procedure follows; it is what a handle without the option, or one whose recovery gave up, needs.

## 1. Detect
Either way works; use both in a long-running service.
- **An error.** A commit fails with `OutcomeUnknown`, `Closed` or `Io` (see [Errors](errors.md)).
- **The health check.** `Pigeonhole::is_failed()` is one atomic load, cheap enough for a health endpoint or a loop. `Pigeonhole::failure()` names the first failure:
  - the shard, or `Failure::MAIN_FILE`;
  - the `FailureCause`: `Panic`, `DriverDropped`, `Io` or `Internal`. Log it: `Panic` and `Internal` are bugs worth reporting.
- **Not failures.** `Busy` (a write stall or tablet change outlasted the write-stall timeout), `Conflict` and the other codes in [Errors](errors.md) leave `is_failed()` false: handle them where they occur, without reopening.

## 2. Close the instance
- Call `close()`. On a failed instance it returns an error (the close is unclean): that's expected. The log keeps everything the close could not checkpoint.
- Drop every handle (tables, snapshots, scans) first, or with it.
- In application-owned mode, keep driving the remaining shards until the close finishes, as for any close.

## 3. Reopen
Open the same path with the same options. The open replays the log: every commit that was written and synced comes back, and each cross-shard commit is decided all or nothing.

## 4. Settle the commits that were in flight
| The commit returned | It was | Do |
|---|---|---|
| `OutcomeUnknown` | maybe committed: its records may be in the log | Read it back after the reopen. If it's missing, write it again. Idempotent writes (a `put` of a full value, not an `incr`) make "write it again" always safe. |
| `Closed` | not committed | Retry it on the reopened instance. |
| `Io` | not committed: the shard refused it before writing anything (a commit whose own log write or sync failed gets `OutcomeUnknown` instead) | Retry it on the reopened instance. If the disk keeps failing, the reopen or the retry fails too: fix the disk first. |
| success | committed, durable at its level | Nothing. |

Non-idempotent writes (`incr`, a `commit_if` whose condition depends on the outcome) need care with `OutcomeUnknown`. Read the current value after the reopen and decide from it, or record an operation id in the same commit and check for it before writing again.

## 5. Reader processes
Reader processes (`PigeonholeReader`) don't need to restart. Once the writer reopens, a snapshot taken before fails with `SnapshotExpired`. Take a new snapshot and read again: it sees the reopened instance's state.

## Automatic recovery
`Options::auto_recover(Recovery::Io)` (or `Recovery::Any`) makes the handle recover in place: a supervisor thread waits for a failure and, on one the setting covers, closes the failed engine keeping its writer lock, reopens the same path on it and swaps the new engine in behind every handle. The same `Pigeonhole`, tables and batches keep working; nothing is reopened by hand.

- **Which failures.** `Recovery::Io`: a shard that stopped after a write or sync to its log failed, or a failed manifest commit to the main file. `Recovery::Any`: also a panic or an internal error (bugs the same input may repeat; the budget bounds the loop). The default is `Recovery::Off`: this page's manual procedure.
- **What a call sees during the recovery.** The pause is the failed engine's close (which waits for its log's I/O to land: on a device that never answers, as long as the device takes) plus the backoff and the reopen's replay, usually well under a second. A read without a snapshot is answered from the failed engine's last view as long as that stands (everything committed before the failure; nothing commits during the pause), as reads on a failed instance always were. A commit, a flush, a compact, a catalog change, or a read once the old view is gone, waits for the swap, up to `Options::write_stall_timeout` (30 s by default), then runs on the recovered engine; past the timeout it gets `Busy`: not committed, retry. A commit that was **in flight when the shard failed** comes back as `Busy` too (not committed; the failed engine refused it before writing anything, and the batch went with it): retry it on the recovered engine. One whose log write or sync failed gets `OutcomeUnknown`, as without the option: read it back. Async calls (`submit_async`, the futures) and commit tickets do not wait: they return `Closed` or `Busy` at once and are retried by the caller; that includes a commit the failed engine refused before logging it (not committed).
- **What expires.** A `Snapshot`, a `Transaction`, a scan or a row iterator taken before the recovery fails with `SnapshotExpired` from then on (the reopen frees and reuses the space their old view names): take a new one. Reader processes see the same, as after any writer restart.
- **The budget.** At most three recoveries in ten minutes, with a backoff of 0, 1 and 4 seconds before the reopen; the reopen's own retries after an `Io` count against the same three, so a later recovery in the window gets fewer of them. Past it, or when the reopen fails with `Corruption`, `UnsupportedFormat`, `NoSpace`, `WriterLocked` or `ShmVersionMismatch`, or when the failed engine's close does not return within four write-stall timeouts (two minutes by default), the supervisor gives up: the handle stays failed exactly as with the option off (`is_failed()` true, commits `Closed`, reads still working), and the manual procedure applies.
- **`close()` during a recovery** waits for it to settle (its swap or its give-up), then closes what the handle ends on. It is not bounded by the recovery's deadline: like `close()` without the option, it waits for the failed engine's own close, which a device that never answers can hold for ever. A `close()` that reaches a failure the supervisor has not noticed yet (a race of microseconds after the failure) does not wait for a recovery that has not started: it closes the failed engine as `close()` without the option would (unclean, with the failure's error), and the next open replays the log.
- **Watching it.** `is_failed()` and `failure()` describe the current engine: true and `Some` from the failure until the swap, false and `None` after it. `recovery_stats()` reports the generation (1 at open, +1 per recovery; `engine_metrics()` restarts from zero at each), the recoveries so far, whether the supervisor gave up, the last failure it handled and how long the last recovery took.
- **Not for application-owned mode.** `open_application_owned` refuses the option: the application drives the shards, so it closes and reopens itself.

```rust
use std::time::Duration;
use pigeonhole::{ErrorCode, Family, Options, Pigeonhole, Recovery};

# fn main() -> pigeonhole::Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(
    dir.join("app.phdb"),
    Options::default()
        .shards(1)
        .auto_recover(Recovery::Io)
        .write_stall_timeout(Duration::from_secs(5)),
)?;
let t = db.table("users")?.family("profile", Family::default()).create_if_missing()?;

// A commit during a recovery waits for the swap; past the timeout, or if it was in flight
// at the failure, it comes back as `Busy` and is simply retried.
let mut tries = 0;
loop {
    match t.mutate(b"u1").put("profile", b"name", b"Ada").commit() {
        Ok(_) => break,
        Err(e) if e.code() == ErrorCode::Busy && tries < 3 => tries += 1,
        Err(e) => return Err(e),
    }
}
let stats = db.recovery_stats();
assert_eq!((stats.generation, stats.recoveries, stats.gave_up), (1, 0, false)); // healthy here
# db.close()?;
# Ok(())
# }
```

## Example
A write that recovers the instance once if it failed, then settles the commit. It takes the database by value and returns the one to keep using: the same instance, or the reopened one. It runs here on a healthy database, so it takes the first path.

```rust
use std::path::Path;
use pigeonhole::{ErrorCode, Family, Options, Pigeonhole, Result};

/// Puts `value` at `row`. If the instance failed, closes it, reopens the database at `path`
/// with `options`, and settles the put. The put is idempotent, so writing it again after an
/// unknown outcome is safe.
fn put_recovering(
    db: Pigeonhole,
    path: &Path,
    options: &Options,
    row: &[u8],
    value: &[u8],
) -> Result<Pigeonhole> {
    let first = {
        let t = db.table("users")?.open()?;
        t.mutate(row).put("profile", b"name", value).commit().map(|_| ())
    };
    let err = match first {
        Ok(()) => return Ok(db),
        Err(e) => e,
    };
    if !db.is_failed() {
        return Err(err); // not a failed instance: handle the error as usual
    }
    if let Some(f) = db.failure() {
        eprintln!("shard {} failed ({:?}): reopening", f.shard, f.cause);
    }
    // Close first (unclean on a failed instance: that's expected), then reopen. The close
    // releases the writer lock the reopen needs.
    let _ = db.close();
    let db = Pigeonhole::open(path, options.clone())?;
    {
        let t = db.table("users")?.open()?;
        let landed = err.code() == ErrorCode::OutcomeUnknown
            && t.get(row, "profile", b"name")?.is_some_and(|c| c.value() == value);
        if !landed {
            // `Closed` or `Io`: not committed. `OutcomeUnknown` and missing: not committed.
            t.mutate(row).put("profile", b"name", value).commit()?;
        }
    }
    Ok(db)
}

# fn main() -> Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
let path = dir.join("app.phdb");
let options = Options::default();
let db = Pigeonhole::open(&path, options.clone())?;
db.table("users")?.family("profile", Family::default()).create_if_missing()?;

let db = put_recovering(db, &path, &options, b"user:1", b"Ada")?;
assert!(!db.is_failed());
assert_eq!(db.failure(), None);
# db.close()?;
# Ok(())
# }
```
