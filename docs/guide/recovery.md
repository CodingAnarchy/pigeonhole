# Recovering from a failed shard

> **Status:** this page describes `main` (not in 0.2.2): `is_failed`, `failure` and `ErrorCode::OutcomeUnknown` are new.

A Pigeonhole instance runs one thread per shard. A shard can fail on its own:
- its thread panics (a bug);
- in application-owned mode, its `Shard` driver is dropped before `close`;
- a write or sync to its write-ahead log fails (a disk error, a full disk);
- its in-memory state could not be updated consistently, so it stops rather than build on a state it cannot trust.

The whole instance can also fail when a manifest commit to the main file fails.

A failed shard refuses its commits, and a failed instance refuses all of them. Nothing already durable is lost: the log and the main file hold it, and the next open replays what the log holds. Recovery is therefore: notice, close, reopen, and settle the commits that were in flight. Pigeonhole doesn't recover in place yet. Automatic recovery is a planned option ([#620](https://github.com/CodingAnarchy/pigeonhole/issues/620)).

## 1. Detect
Either way works; use both in a long-running service.
- **An error.** A commit fails with `OutcomeUnknown`, `Closed` or `Io` (see [Errors](errors.md)).
- **The health check.** `Pigeonhole::is_failed()` is one atomic load, cheap enough for a health endpoint or a loop. `Pigeonhole::failure()` names the first failure:
  - the shard, or `Failure::MAIN_FILE`;
  - the `FailureCause`: `Panic`, `DriverDropped`, `Io` or `Internal`. Log it: `Panic` and `Internal` are bugs worth reporting.

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
