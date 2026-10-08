# Engine questions (values above the inline limit, #230)

## Proposed decision (amends D16): puts above the inline limit are separated at commit time
D16 refused any value longer than `min(WAL segment payload, 64 MiB, half the shard's memtable arena)` with `ValueTooLarge`, because every value went through one WAL record and one memtable entry before a flush could separate it.

**Interim behavior:**
- That bound is now the *inline limit*. A put whose stored value is longer is written into a new blob file when its batch is routed (one `BlobSink` per family; blob extents of about 1 MiB, larger for a longer value). The files are committed in the manifest, whose root commit syncs their bytes, before the batch is submitted. The WAL record and the memtable entry hold the 17-byte pointer, and from there the value is handled like any separated value (flush, compaction, blob GC, #240 references, reads, backup).
- A stored value can be up to `2^32 − 1` bytes (the pointer's `len`), so a payload up to 4 GiB − 2. A merge operand above the inline limit is still `ValueTooLarge` (operands are never separated). The family's `blob_threshold` doesn't matter here.
- Same-commit collapse (D34) happens before separation: only the last mutation per (column, timestamp) of a batch is kept, so no value of a mutation the shard would drop goes to a blob file. Two default timestamps compare equal. An explicit timestamp equal to the commit's own cannot be detected at routing (the shard assigns it); the earlier value's bytes would stay counted live with nothing pointing into them, a leak rather than a read error.
- Cost: one manifest commit per batch with such a value, on the submitting thread. `commit_from_thread` drives that commit itself, as `shrink` does, so this works on a shard-driving thread too (application-owned `commit_local` included).

## Proposed decision: releasing a refused commit's blob files
**Interim behavior:** a guard owns the new files from the manifest commit until the commit's outcome is known. It is attached to the reply with the runtime's additive `Notifier::on_resolve`, so it runs even if the caller drops the `PendingCommit`. The files get a `DropBlobFile` (queued through the manifest, never blocking the shard) when:
- the guard is dropped before reaching a shard (an error, or an unwind, between the manifest commit and the submission);
- the outcome proves the batch was never applied: `Conflict`, `Busy`, `BatchTooLarge`, `RecordTooLarge`, `KeyTooLarge`, `ValueTooLarge`, `InvalidArgument`, or a `check_and_mutate` whose predicate is false.

Any other failure keeps the files, since the batch may have been applied (D85: a failed WAL sync after apply leaves the data visible). Those failures poison a shard or close the engine, and the open-time sweep below drops what nothing points into.

## Proposed decision: the open sweep, with no pending marker
The coordinator asked whether the pending marker (a new manifest tag) could be dropped.

**Interim behavior:** no marker, no format change. Flushes, compactions, blob GC, backups and shrink copies always commit a blob file in the same manifest commit as the SSTs that point into it, and those SSTs carry `SstBlobRefs` (#240). So after WAL replay, a blob file that no SST record, no SST of its family without a record, and no recovered memtable entry points into can only come from a commit-time separation whose commit never became durable, or was refused before its release committed. It is dropped at open (`DropBlobFile`, extents retired). Recovery that does not apply a record (an aborted cross-shard share, D83) leaves its file unreferenced, which is right.

## Q: tests and deferred I/O
The engine model harness drives the shards on the thread that commits, and with `PIGEONHOLE_DEFERRED_IO=1` that thread must also complete the in-flight I/O of a background manifest commit. A commit-time separation waiting on the manifest there would never return.

**Interim behavior:** `Store::open_cfg` sets a 120-byte inline limit (values go up to 160 bytes), so about a quarter of the puts take this path under faults, crashes and tablet changes. It doesn't with deferred I/O. The recovery oracle compares put values above 120 bytes by length, since a WAL record's pointer may name a file blob GC has dropped since. `large_values.rs` covers the paths directly, and `huge_value.rs` runs the one real round trip above 64 MiB in its own test binary.
