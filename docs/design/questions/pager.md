# Open questions: pigeonhole-pager

## Q: the clean-close flag outlives the open that read it
Bit 0 of the superblock flags is set by `mark_clean` and cleared only by the next root commit. A writer that opens a cleanly closed file, writes WAL records and crashes before any root commit leaves the flag set.

**Interim behavior:** the pager reports the flag as stored. The engine must not skip WAL replay on it (or must commit a root right after opening as writer if it ever does).

## Q: a failed root commit poisons the pager
After a commit fails (any error once its first sync is issued) the on-disk root is uncertain, and an fsync error may have dropped written pages, so retrying could publish a root over lost bytes.

**Interim behavior:** every later `commit_root`, `submit_commit_root` and `mark_clean` fails. The engine should surface the error and reopen.

## Q: a crash inside `Pager::create` can leave a file with no valid superblock
Nothing was committed to it, so `open` fails with a format error and the caller may remove and recreate it.

**Interim behavior:** as described. The engine's open-or-create path should treat "exists, no valid superblock, length <= 64 KiB" as an interrupted create.

## Q: `shrink_plan` cannot tell published extents from in-flight output
The pager tracks allocated versus retired, not which extents the manifest names.

**Interim behavior:** the plan lists every non-retired extent past the shrink point. The engine relocates only extents its manifest names (or runs shrink with no flush or compaction output in flight). `relocate` fails with `NoSpace` when no free extent of that size lies below the one being moved.

## Q: `reclaim` is clamped to the durable root
`reclaim(oldest_live)` frees nothing newer than the manifest version of the last *completed* root commit, because until the root that drops an extent is durable, a crash recovers to a root that still references it. `truncate_tail` releases nothing while a commit is in flight.

**Interim behavior:** as described. Callers may retire and reclaim right after submitting a commit; the extents are freed by a later `reclaim` once the commit has completed.
