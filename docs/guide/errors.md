# Errors

> **Status: Phase 1 sync API implemented.** Codes are stable; some can only occur once the feature that raises them lands (noted per row). Code samples run as doctests of the `pigeonhole` crate (lines starting with `#` are hidden setup).

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
| 1 | `Io` | An I/O failure. | Disk or filesystem error, failed `write` or fsync, permissions. | Check `message()`. Treat an in-flight commit as **not durable**. Retry only if the cause is transient. |
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
| 17 | `ValueTooLarge` | A value exceeds the size limit. | Phase 1 limit: `min(WAL segment payload, 64 MiB, half the shard's memtable arena)`. Phase 2 blob separation lifts it. | Split the value across qualifiers, or store it outside the database and keep a reference. |
| 18 | `NoSpace` | The device is full. | Disk full, quota. | Free space and retry. The commit did not apply. For `shrink` (the disk filled while it moved the manifest), nothing was lost; free space and call it again. |
| 19 | `InvalidArgument` | An argument is invalid. | For example a malformed option or table name, or `compaction_cores(k)` with `k > 0` passed to `open_application_owned`, which starts no shard or compaction threads. Counters (D179): `incr` on a family that is not a counter family ("has no merge operator: increments need a counter family"), `incr_at` outside a counter family, a non-`i64` put or untyped `merge` into a counter family ("holds only i64 values"), `incr`/`put_i64` without a timestamp in a counter family with a TTL, or `Family::counter()` with another merge operator. Nothing of the commit is applied. | Check `message()` and fix the call: declare counters with `Family::counter()`, write buckets with `incr_at`/`put_i64_at` where there is a TTL. |
| 20 | `Unsupported` | The feature is not available in this build. | Calling a feature gated off or not yet implemented. | Enable the feature, or use the supported alternative. |
| 21 | `Closed` | The database is closed. | A table or snapshot handle used after `close()`. | Reopen the database. |
| 22 | `NoReaderSlot` | Every reader slot in the shared-memory region is taken. | Too many concurrent reader processes. | Close idle readers, then retry. |
| 23 | `RecordTooLarge` | A commit is too large for one WAL record. | A very large `WriteBatch`. | Split it into smaller batches (each atomic on its own). |
| 24 | `Busy` | Writes, a `flush` or a `compact` are stalled past the write-stall timeout (`Options::write_stall_timeout`, 30 s by default). In a reader process, `snapshot()` also returns it when the writer has committed a manifest change but has not published it for about a second (a writer that died or stalled mid-publish). | A write found the arena full and waited for a flush, or a flush had no room for the fresh memtables it needs, and nothing freed room in time (a slow or full disk, ingest faster than flush and compaction can keep up, or snapshots pinning memtables). | **Transient: back off and retry**, and drop old snapshots. To fail faster (latency-sensitive callers) or wait longer (bulk loads), set `Options::write_stall_timeout`. A batch that can never fit gets `BatchTooLarge` instead, never `Busy`. Known limit: a value close to the `ValueTooLarge` limit can find no long enough free run in a fragmented arena even after a flush, and then ends with `Busy` at the timeout rather than being admitted; raise `Options::memtable_budget`. |
| 25 | `SnapshotExpired` | A reader process's snapshot was taken before a writer restart. | The writer process closed or crashed and a new writer opened. The new writer may reuse the space the old snapshot reads, so every read through that snapshot (`get`, `row`, `scan` and each scan step) fails instead of returning wrong data. Snapshots in the writer process never expire. | Drop the snapshot, take a new one and redo the read. |
| 26 | `WouldDeadlock` | A submitted commit's outcome was awaited on a thread that drives a shard (application-owned mode). | `PendingCommit::wait()` called on the thread that runs a `Shard`, where blocking could deadlock (the commit may need that shard, or any shard it drives, to run). | The commit was submitted and **will apply**: do not resubmit it. Poll its future from the event loop, or wait on another thread. Blocking calls that would submit and wait there (`commit()`, `check_and_mutate`, `flush`, `compact`) fail with `InvalidArgument` instead, before submitting anything. |
| 27 | `BatchTooLarge` | A batch can never fit a shard's memtable arena, even an empty one. | One batch whose cells (keys and values, plus per-entry overhead) need more than about half of `Options::memtable_budget` on one shard. A single value within the `ValueTooLarge` limit always fits on its own. | **Not transient: retrying never succeeds.** Split the batch into smaller ones (each atomic on its own), or raise `Options::memtable_budget`. |

## Handling guide
| Situation | Action |
|---|---|
| Retryable | `Busy` from a write stall (back off); `Conflict` (retry the whole transaction); `WriterLocked` (after the other writer exits); `NoSpace` and `Io` once the cause is fixed; `NoReaderSlot` (after a reader process closes); `SnapshotExpired` (take a new snapshot, then redo the read). |
| Programmer error | `FamilyNotFound`, `TableNotFound`, `TableExists`, `FamilyExists`, `KeyTooLarge`, `ValueTooLarge`, `RecordTooLarge` (split the batch), `MergeFailed` (bad operand, or mixed data in a 0.1.0 counter column), `InvalidArgument`, `Unsupported`, `Closed`, `ReadOnly`, `WouldDeadlock` (await from the event loop; the commit still applies). Fix the code or the data model. |
| Configuration | `BatchTooLarge` (split the batch or raise `Options::memtable_budget`), `ShmUnavailable`, `ShmVersionMismatch`, `NetworkFilesystem`, `UnknownMergeOperator`, `UnsupportedFormat`. |
| Data integrity | `Corruption`. Do not retry; restore from backup. |

If this table disagrees with `crates/pigeonhole/src/error.rs`, the source wins; please report it.
