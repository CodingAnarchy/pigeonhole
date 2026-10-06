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
| 5 | `ShmUnavailable` | The shared-memory region could not be created at the configured size. | `/dev/shm` or the `shm_dir` filesystem is too small; the memtable budget times the shard count is too large. | Lower `Options::memtable_budget` or `shards`, or point `Options::shm_dir` at a larger tmpfs. |
| 6 | `UnsupportedFormat` | The file's format version is not supported by this build. | The file was written by a newer build, or is not a Pigeonhole file. | Upgrade the library, or open the right file. |
| 7 | `NetworkFilesystem` | The database is on a network filesystem. | NFS, SMB and similar. | Move it to a local filesystem. |
| 8 | `TableNotFound` | No such table. | `TableBuilder::open` or `drop_table` on a missing name; typo. | Use `create_if_missing`, or check `db.tables()`. |
| 9 | `TableExists` | The table already exists. | `TableBuilder::create` on an existing name. | Use `create_if_missing` or `open`. |
| 10 | `FamilyNotFound` | No such family. | Misspelled family in `put`, `get`, `row` or `scan`; family never declared. | Declare it on the `TableBuilder` (adding a family is cheap), or fix the name. Check `table.families()`. |
| 11 | `FamilyExists` | The family already exists. | Reserved and rare: `TableBuilder::family` on an existing table does not fail (an existing family keeps its stored options). | If you see it, treat the family as present and continue. |
| 12 | `UnknownMergeOperator` | A family names a merge operator this process has not registered. *(custom operators: Phase 2)* | Opening a database whose family uses a custom operator. | Register it with `Options::merge_operator`. For read-only inspection, `Options::allow_unregistered_merge_operators(true)` (compaction off; reads of affected cells fail with this code). |
| 13 | `MergeFailed` | A merge operator failed. | Bad operand encoding, for example `put` of arbitrary bytes into an `incr` counter column. | Write counter columns only with `incr` and `put_i64`. |
| 14 | `Conflict` | A transaction conflicted and was aborted. | A concurrent commit touched what the transaction read. | Retry the whole transaction. |
| 15 | `ReadOnly` | The handle or database is read-only. | Writing through a database opened with unregistered merge operators allowed. | Register the operators and reopen as writer. |
| 16 | `KeyTooLarge` | A row key or qualifier exceeds 64 KiB. | An unbounded value used as a key, or a key and qualifier swapped. | Shorten it, or hash it and keep the original in a value. |
| 17 | `ValueTooLarge` | A value exceeds the size limit. | Phase 1 limit: `min(WAL segment payload, 64 MiB, half the shard's memtable arena)`. Phase 2 blob separation lifts it. | Split the value across qualifiers, or store it outside the database and keep a reference. |
| 18 | `NoSpace` | The device is full. | Disk full, quota. | Free space and retry. The commit did not apply. |
| 19 | `InvalidArgument` | An argument is invalid. | For example a malformed option or table name, or `compaction_cores(k)` with `k > 0` passed to `open_application_owned`, which starts no threads. | Check `message()` and fix the call. |
| 20 | `Unsupported` | The feature is not available in this build. | Calling a feature gated off or not yet implemented: Phase 2 family settings (`zstd`, `Compaction::Tiered` or `FifoByTime`) at table creation; `compact()` and `backup()` until the engine writes SSTs. | Enable the feature, or use the supported alternative. |
| 21 | `Closed` | The database is closed. | A table or snapshot handle used after `close()`. | Reopen the database. |
| 22 | `NoReaderSlot` | Every reader slot in the shared-memory region is taken. | Too many concurrent reader processes. | Close idle readers, then retry. |
| 23 | `RecordTooLarge` | A commit is too large for one WAL record. | A very large `WriteBatch`. | Split it into smaller batches (each atomic on its own). |
| 24 | `Busy` | Writes are stalled and the call asked not to wait. | The memtable arena is full (until SST flushes land, nothing frees it), or reserved for non-blocking write calls. | Back off and retry; raise `Options::memtable_budget`. |

## Handling guide
| Situation | Action |
|---|---|
| Retryable | `Busy`; `Conflict` (retry the whole transaction); `WriterLocked` (after the other writer exits); `NoSpace` and `Io` once the cause is fixed; `NoReaderSlot` (after a reader process closes). |
| Programmer error | `FamilyNotFound`, `TableNotFound`, `TableExists`, `FamilyExists`, `KeyTooLarge`, `ValueTooLarge`, `RecordTooLarge` (split the batch), `MergeFailed` (bad operand or mixed counter data), `InvalidArgument`, `Unsupported`, `Closed`, `ReadOnly`. Fix the code or the data model. |
| Configuration | `ShmUnavailable`, `ShmVersionMismatch`, `NetworkFilesystem`, `UnknownMergeOperator`, `UnsupportedFormat`. |
| Data integrity | `Corruption`. Do not retry; restore from backup. |

If this table disagrees with `crates/pigeonhole/src/error.rs`, the source wins; please report it.
