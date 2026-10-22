# Errors

> **Status:** this guide describes `main`; the current release on crates.io is 0.3.1, and the [changelog](../../CHANGELOG.md) and [`changelog.d/`](../../changelog.d/README.md) list what `main` adds. Codes are stable. Code samples run as doctests of the `pigeonhole` crate (lines starting with `#` are hidden setup).

Every fallible call returns `pigeonhole::Result<T>` = `Result<T, pigeonhole::Error>`.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta"])?;
use pigeonhole::ErrorCode;

match pages.mutate(b"k").put("nope", b"q", b"v").commit() {
    Ok(info) => { /* info.seqno, info.durability */ }
    Err(e) if e.code() == ErrorCode::FamilyNotFound => { /* e.message() has detail */ }
    Err(e) => return Err(e),
}
# Ok::<(), pigeonhole::Error>(())
```

- `Error::code() -> ErrorCode` is the **stable** part. Branch on it.
- `Error::message() -> &str` is for humans. Do not parse it.
- `Error` implements `Display` and `std::error::Error`.
- `ErrorCode` is `#[non_exhaustive]` and `#[repr(u32)]`: **always include a wildcard arm**. Numeric values never change meaning and are never reused, so they map to a future C enum and to exceptions in other languages.
- Builders (`mutate`, `row`, `scan`, `write_batch`) do not fail while you build. Errors for bad families, keys or values surface at `commit()`, `read()` or `iter()`.

## Code table
| # | Code | Meaning | Typical cause | What to do |
|---|---|---|---|---|
| 1 | `Io` | An I/O failure. | Disk or filesystem error, failed `write` or fsync, permissions. For a commit: refused before anything of it was written (its shard had already stopped after an I/O error). A commit whose own log write or sync failed gets `OutcomeUnknown` instead, since its record may be in the file. | Check `message()`. A commit that got `Io` was **not committed**: retry it once the cause is fixed (after a reopen if the instance failed). |
| 2 | `Corruption` | Stored data failed validation (checksum, structure). | Disk fault, truncated or modified file, bug. Also a crash during the very first `open` that created the file: the message then says it looks like an interrupted create. | Stop writing to this file. Restore from a backup (`Pigeonhole::backup`) and report it. For an interrupted create, nothing was ever committed: delete the file and open again (Pigeonhole never deletes it for you). |
| 3 | `WriterLocked` | Another process holds the writer lock. | A second `open` or `open_application_owned` on the same file. | Use `open_reader` for the second process, or wait and retry after the writer closes. |
| 4 | `ShmVersionMismatch` | A live shared-memory region has a different layout version. | A newer or older Pigeonhole build is attached to the same database. | Run one build version against a database at a time; close all handles and reopen. |
| 5 | `ShmUnavailable` | The shared-memory region could not be created at the configured size. The region is `memtable_budget × shards` plus about 10 MiB for views and reader slots (266 MiB for the defaults on 4 CPUs); opening checks that its filesystem has that much free and fails here if not. The message gives the size and location. **Residual risk:** the check does not hold the space, so if another process fills the same tmpfs after the open, a write that touches new region pages can kill the process with `SIGBUS` instead of returning an error. | `/dev/shm` or the `shm_dir` filesystem is too small (Docker and Kubernetes default `/dev/shm` to 64 MiB) or missing (some minimal images), or `shm_dir` does not exist; the memtable budget times the shard count is too large. | Enlarge `/dev/shm` (`docker run --shm-size`; in Kubernetes an `emptyDir` with `medium: Memory` at `/dev/shm`), point `Options::shm_dir` at a larger tmpfs (ideally one the database does not share), or lower `Options::memtable_budget` or `shards`. |
| 6 | `UnsupportedFormat` | The file's format version is not supported by this build. | The file was written by a newer build, or is not a Pigeonhole file. | Upgrade the library, or open the right file. |
| 7 | `NetworkFilesystem` | The database is on a network or cluster filesystem. | NFS (with or without working locks), SMB/CIFS, 9P, AFS, Ceph, Lustre, GFS2, OCFS2, GPFS, and every FUSE mount (sshfs, s3fs, gcsfuse, JuiceFS, rclone; also local FUSE filesystems such as ntfs-3g) on Linux; any mount macOS does not mark local. | Move it to a local filesystem. For a local FUSE mount you trust, `Options::allow_fuse(true)` / `ReaderOptions::allow_fuse(true)` accepts FUSE (not network filesystems) with its risks; see [Getting started](getting-started.md). |
| 8 | `TableNotFound` | No such table. | `TableBuilder::open` or `drop_table` on a missing name; typo. | Use `create_if_missing`, or check `db.tables()`. |
| 9 | `TableExists` | The table already exists. | `TableBuilder::create` on an existing name. | Use `create_if_missing` or `open`. |
| 10 | `FamilyNotFound` | No such family. | Misspelled family in `put`, `get`, `row` or `scan`; family never declared. | Declare it on the `TableBuilder` (adding a family is cheap), or fix the name. Check `table.families()`. |
| 11 | `FamilyExists` | The family already exists. | Reserved and rare: `TableBuilder::family` on an existing table does not fail (an existing family keeps its stored options). | If you see it, treat the family as present and continue. |
| 12 | `UnknownMergeOperator` | A family names a merge operator this process has not registered. | Opening a database whose family uses a custom operator. | Register it with `Options::merge_operator`. For read-only inspection, `Options::allow_unregistered_merge_operators(true)` (read-only, compaction off for those families; reads of affected cells fail with this code). Creating a family that names an unregistered operator also fails with it. |
| 13 | `MergeFailed` | A merge operator failed. | Bad operand encoding: a custom operator rejected an operand, or in a family from 0.1.0 with the `i64` operator, `put` of arbitrary bytes under an `incr` (counter families refuse such writes at commit instead). | Fix the operands; keep counters in a counter family (`Family::counter()`). |
| 14 | `Conflict` | A transaction conflicted and was aborted. | A concurrent commit touched what the transaction read. | Retry the whole transaction. |
| 15 | `ReadOnly` | The handle or database is read-only. | Writing through a database opened with unregistered merge operators allowed. | Register the operators and reopen as writer. |
| 16 | `KeyTooLarge` | A row key or qualifier exceeds 64 KiB. | An unbounded value used as a key, or a key and qualifier swapped. | Shorten it, or hash it and keep the original in a value. |
| 17 | `ValueTooLarge` | A value exceeds the size limit. | A put value above 4 GiB − 2 bytes, or a merge operand above the inline limit `min(WAL segment payload, 64 MiB, half the shard's memtable arena)` (decision D16). | Split the value across qualifiers, or store it outside the database and keep a reference. |
| 18 | `NoSpace` | The device is full. | Disk full, quota. | Free space and retry. The commit did not apply. For `shrink` (the disk filled while it moved the manifest), nothing was lost; free space and call it again. |
| 19 | `InvalidArgument` | An argument is invalid. | For example a malformed option or table name, or `compaction_cores(k)` with `k > 0` passed to `open_application_owned`, which starts no shard or compaction threads. Counters (D179): `incr` on a family that is not a counter family ("has no merge operator: increments need a counter family"), `incr_at` outside a counter family, a non-`i64` put or untyped `merge` into a counter family ("holds only i64 values"), `incr`/`put_i64` without a timestamp in a counter family with a TTL, or `Family::counter()` with another merge operator. Nothing of the commit is applied. | Check `message()` and fix the call: declare counters with `Family::counter()`, write buckets with `incr_at`/`put_i64_at` where there is a TTL. |
| 20 | `Unsupported` | The feature is not available in this build. | Reserved: the current public API does not return it. | Treat it as a programmer error and report it. |
| 21 | `Closed` | The database is closed. | A table or snapshot handle used after `close()`; a commit still in flight when the database closed; or any new commit after a shard failed (a panic, a dropped application-owned shard, a failed log fsync or a failed apply): the database refuses writes from then on, though reads still work. | Reopen the database. A commit that got `Closed` was **not committed**; one whose outcome is not known gets `OutcomeUnknown` instead. |
| 22 | `NoReaderSlot` | Every reader slot in the shared-memory region is taken. | Too many concurrent reader processes. | Close idle readers, then retry. |
| 23 | `RecordTooLarge` | A commit is too large for one WAL record. | A very large `WriteBatch`. | Split it into smaller batches (each atomic on its own). |
| 24 | `Busy` | A write, a `flush` or a `compact` waited past the write-stall timeout (`Options::write_stall_timeout`, 30 s by default). The timeout is each call's own: a wait for memtable room counts from when that call began waiting, however long the shard has been short of room for the calls ahead of it (D240), and a wait on a tablet change, or a cross-shard commit's retries, counts from when the commit was submitted (D241, D244). In a reader process, `snapshot()` also returns it when the writer has committed a manifest change but has not published it for about a second (a writer that died or stalled mid-publish). | Waiting for memtable room: a commit found the arena full and waited for a flush, or a flush had no room for the fresh memtables it needs, and nothing freed room in time (a slow or full disk, ingest faster than flush and compaction can keep up, or snapshots pinning memtables). Waiting on a tablet change: the commit's rows were being split, merged or moved between shards for the whole timeout, or a cross-shard commit kept being refused by its participants for that reason (it is retried as often as needed until the timeout, never given up after a number of tries). A cross-shard commit is also retried, until the same timeout from its submission, when a participant refuses its timestamp as below writes it already made, with tablet changes on or off (D244): busy shards trading cross-shard commits can do this repeatedly. An L0 write stall (compaction falling behind) slows commits down but never returns `Busy`. | **Transient: back off and retry.** A commit that got `Busy` was **not committed**, so retrying it is safe, conditional and non-idempotent ones included. Back off with a growing, jittered delay: retrying at once adds to the load that caused the stall. A `flush` or `compact` that got `Busy` did not finish; nothing already committed is lost. Drop old snapshots, which pin memtables. To fail faster (latency-sensitive callers) or wait longer (bulk loads), set `Options::write_stall_timeout`. A batch that can never fit gets `BatchTooLarge` instead, never `Busy`. Known limit: a value close to the `ValueTooLarge` limit can find no long enough free run in a fragmented arena even after a flush, and then ends with `Busy` at the timeout rather than being admitted; raise `Options::memtable_budget`. |
| 25 | `SnapshotExpired` | A reader process's snapshot was taken before a writer restart. | The writer process closed or crashed and a new writer opened. The new writer may reuse the space the old snapshot reads, so every read through that snapshot (`get`, `row`, `scan` and each scan step) fails instead of returning wrong data. Snapshots in the writer process never expire. | Drop the snapshot, take a new one and redo the read. |
| 26 | `WouldDeadlock` | A submitted commit's outcome was awaited on a thread that drives a shard (application-owned mode). | `PendingCommit::wait()` called on the thread that runs a `Shard`, where blocking could deadlock (the commit may need that shard, or any shard it drives, to run). | The commit was submitted and **will apply**: do not resubmit it. Poll its future from the event loop, or wait on another thread. Blocking calls that would submit and wait there (`commit()`, `check_and_mutate`, `flush`, `compact`) fail with `InvalidArgument` instead, before submitting anything. |
| 27 | `BatchTooLarge` | A batch can never fit a shard's memtable arena, even an empty one. | One batch whose cells (keys and values, plus per-entry overhead) need more than about half of `Options::memtable_budget` on one shard. A single value within the `ValueTooLarge` limit always fits on its own. | **Not transient: retrying never succeeds.** Split the batch into smaller ones (each atomic on its own), or raise `Options::memtable_budget`. |
| 28 | `OutcomeUnknown` | A commit's outcome is not known in this process. | A shard failed (a panic, an application-owned shard dropped, or a log write or sync that failed) or the database closed while the commit was in flight, after its records may have reached the log. Cases: a commit whose own log write or sync failed, or whose apply failed after it was logged; a multi-shard commit whose coordinating shard failed, or one decided to commit that another shard did not apply; a commit whose log sync had not finished when its shard failed; a commit that was durable but whose wait to become visible ended. Never returned for a commit that was not written. In the last case a `GroupSync` or `Sync` commit is in fact durable (its shard acknowledged it only after its sync), so a reopen shows it; a `Buffered` one survives unless a power loss followed. | Reopen the database and read: the reopen decides the commit from the log, all of it or none of it. If the write is idempotent, writing it again is safe. [Recovering from a failed shard](recovery.md) has the steps and an example. |

## Handling guide
| Situation | Action |
|---|---|
| Retryable | `Busy` (not committed; back off, then retry); `Conflict` (retry the whole transaction); `WriterLocked` (after the other writer exits); `NoSpace` and `Io` once the cause is fixed; `NoReaderSlot` (after a reader process closes); `SnapshotExpired` (take a new snapshot, then redo the read). |
| Programmer error | `FamilyNotFound`, `TableNotFound`, `TableExists`, `FamilyExists`, `KeyTooLarge`, `ValueTooLarge`, `RecordTooLarge` (split the batch), `MergeFailed` (bad operand, or mixed data in a 0.1.0 counter column), `InvalidArgument`, `Unsupported`, `Closed`, `ReadOnly`, `WouldDeadlock` (await from the event loop; the commit still applies). Fix the code or the data model. |
| Configuration | `BatchTooLarge` (split the batch or raise `Options::memtable_budget`), `ShmUnavailable`, `ShmVersionMismatch`, `NetworkFilesystem`, `UnknownMergeOperator`, `UnsupportedFormat`. |
| Outcome unknown | `OutcomeUnknown`: reopen and read to see whether the commit landed (whole or not at all), then write it again if it did not. |
| Failed instance | `Pigeonhole::is_failed()` is true (a shard panicked, lost its driver, or stopped after an I/O or internal error; `failure()` says which and why). Close, reopen, then settle the commits in flight: see [Recovering from a failed shard](recovery.md). |
| Data integrity | `Corruption`. Do not retry; restore from backup. |


## Retrying `Busy`
`Busy` means the call waited the whole write-stall timeout and gave up; a commit that got it was not committed. Retry after a growing delay, and give up (or shed load) after a few tries. The example below retries a commit three times. It runs here on a database with room, so its first try commits.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let pages = pigeonhole::doc_support::table(&db, "pages", &["meta"])?;
use std::time::Duration;
use pigeonhole::ErrorCode;

let mut delay = Duration::from_millis(50);
let mut tries = 0;
let info = loop {
    match pages.mutate(b"k").put("meta", b"q", b"v").commit() {
        // Not committed: nothing to undo. Wait, then try again (add jitter when many
        // writers retry at once).
        Err(e) if e.code() == ErrorCode::Busy && tries < 3 => {
            tries += 1;
            std::thread::sleep(delay);
            delay *= 4;
        }
        other => break other?,
    }
};
assert!(info.seqno > 0);
# Ok::<(), pigeonhole::Error>(())
```

- **What it waited for.** Usually memtable room, because flushes couldn't keep up: a slow or full disk, writes faster than flush and compaction, or old snapshots pinning memtables. `engine_metrics()` counts the stalls (`write_stalls`) and their total time (`write_stall_nanos`). Less often, a tablet split, merge or move kept the commit waiting.
- **The timeout.** It is each call's own (D240, D241): a call that began waiting later is not refused at an earlier call's deadline. A shorter `Options::write_stall_timeout` fails faster; a bulk load can set a longer one instead of retrying.
- **In async code,** `commit_async` fails with the same code. Sleep with your runtime's timer, not `std::thread::sleep`.

If this table disagrees with `crates/pigeonhole/src/error.rs`, the source wins; please report it.
